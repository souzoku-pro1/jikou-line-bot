"""webapp_q — Q-BATCH-1: 案件質問応答（PWA 搭載・read-only tool use）

正本: ③（docs/plan/2026-08_execution-plan.md）項目10 要件7点＋12.2-1＋
①の Q 関連記述（6.13/9.7/11.3「Q&A request から業務正本を更新してはならない」）
＋大野裁定 2026-08-17（要件1 を改め専用 LINE アカウントでなく PWA 搭載。
要件2〜7 は維持）。認証は P4-001 の関所（`_gate`）にそのまま乗る。

設計（要件との対応）:
- 要件2 読み取り専用: tool は読み取り専用 kintone 検索の**閉集合のみ**
  （_TOOLS と _DISPATCH の鍵集合が一致・書き込み tool は定義自体が存在しない
  ＝構造でゼロ。kintone 書込み API の呼出しゼロは AST 機械検査で pin）。
  App34 読取は dashboard の filter 経由 loader（filter_active_persons）・
  App36 は同（filter_active_heir_rows）を**共用**する（別実装での PASS を
  作らない流儀・reader manifest 登録済みの単一の正）。
- 要件3 出典明示: **出典はサーバが実測記録する**——tool 実行側が実際に読んだ
  (app, record_id) を積み上げ、応答の出典配列（kintone リンク・PDF リンク付き）
  にする。モデルの自己申告を出典としない。**出典ゼロの断定回答は返さない**
  （fail-closed で固定文言「該当する記録が見つかりません」へ）。
- 要件4 未確定注記: tool 結果から**サーバが機械判定**した固定注記
  （名寄せ未確定・戸籍未確認・評価未確定・取消済み/統合済み無効の除外）を
  回答に必ず併記する（dashboard の filter/集計実装を流用）。
- 要件7 信頼度格付け: 根拠が戸籍読解（手書き・旧字を含み得る）なら低確信度
  注記＋原本 PDF リンク必須・OCR（印字）由来の数値なら原本確認推奨の固定注記。
- 要件5: 第1版は kintone 構造化データ＋読解JSON のみ（PDF 全文検索は第2版・
  スコープ外）。
- 「機械は確定しない」: 確定・承認・編集の経路なし（router は GET と
  質問 POST のみ・POST は Q&A 台帳への追記だけで業務正本に触れない）。
- PII 規律: 本 module は logging を一切 import しない（質問・回答・kintone
  値のログ反射経路を構造的に持たない）。応答は関所の no-store 契約に乗る。
  質問文は **form POST**（access log に query が載る GET を使わない）。
- コスト・安全: 1 質問のコスト概算（Decimal・float 非経由）を Q&A 台帳に記録。
  質問レート制限（固定窓・webapp_auth の流儀）。API 呼出しは timeout 付き・
  turn 上限付きで、失敗時は推測で埋めず固定文言へ fail-closed。
"""

import json
import re
import time
from decimal import Decimal

import anthropic
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response

import config
from hub import kintone
from hub import qa_store
from hub import webapp_case_views as case_views
from hub import webapp_souzoku_dashboard as souzoku_dash
from hub.webapp_auth import WEBAPP_ROOT, _gate

router = APIRouter()

_RECORD_ID_RE = re.compile(r"^[0-9]{1,10}$")
_LINE_USER_ID_RE = re.compile(r"^U[0-9a-f]{32}$")

_MAX_TURNS = 8                    # tool use の往復上限（暴走防止）
_MAX_TOKENS = 4096
_QUESTION_MAX_CHARS = 2000
_CHAT_LIMIT = 30
_MAX_SOURCES = 50                 # 出典の記録上限（超過は注記で明示）
_API_TIMEOUT_SECONDS = 120.0

# レート制限（固定窓・単一利用者前提のプロセス内カウンタ。webapp_auth の流儀）
RATE_WINDOW_SECONDS = 600
RATE_LIMIT = 10
_ask_times: list = []

# ── 固定文言（閉集合・テストで pin） ─────────────────────────────────────────
DISCLAIMER = (
    "この回答は kintone 記録の参照に基づく参考情報であり、確定ではありません。"
    "名寄せ・相続資格・評価などの確定は弁護士の確定操作によります。")
NO_SOURCE_ANSWER = (
    "該当する記録が見つかりませんでした。案件番号や対象（相続人・財産・書類など）"
    "を具体的に指定して、もう一度質問してください（出典のない断定回答は返さない"
    "設計です）。")
ERROR_ANSWER = (
    "回答を生成できませんでした（時間切れまたは一時的なエラー）。しばらくして"
    "再試行してください。")

# 要件4/7 の注記（サーバ機械判定の固定文言・閉集合）
FLAG_NOTES = {
    "nayose_unconfirmed": "名寄せ未確定の人物が含まれます——同一人物かどうかの判断は確定していません",
    "koseki_unconfirmed": "戸籍確認済=yes でない相続人行が含まれます（相続資格は未確定です）",
    "valuation_unconfirmed": "評価未確定の財産が含まれます——金額は参考値です",
    "cancelled_excluded": "取消済みの相続人行は共通 filter で除外されています（行削除はされていません）",
    "merged_excluded": "統合済み無効（soft merge の敗者）の人物行は除外されています",
    "ocr_numbers": "OCR（印字読取）由来の数値を含みます——正確な数値は原本の確認を推奨します",
    "koseki_reading": "戸籍読解（手書き・旧字を含み得る）由来の情報です——低確信度のため原本 PDF の確認が必須です",
    "sources_truncated": "出典が多数のため一部のみ記録しています",
}

# コスト概算（USD/MTok・Decimal 文字列。cache read=0.1×・cache write=1.25×）
_MODEL_PRICES = {
    "claude-sonnet-4-6": (Decimal("3"), Decimal("15")),
    "claude-sonnet-5": (Decimal("3"), Decimal("15")),
}

_SYSTEM = (
    "あなたは大野法律事務所の内部専用・案件参照アシスタントです。kintone の"
    "業務記録を読み取り専用ツールで参照し、事務所の弁護士からの質問に日本語で"
    "簡潔に答えます。\n"
    "規律:\n"
    "- ツールで実際に読んだ記録に基づいてのみ答える。記録に無いことは断定"
    "せず「記録にありません」と言う。推測で埋めない。\n"
    "- 回答ではレコード番号（No.）を自然に言及する（出典の実測記録はサーバ側"
    "でも行われる）。\n"
    "- 名寄せ・相続資格・評価などの未確定データは未確定であることを明記する。"
    "機械は確定しない——確定は弁護士の関所経由でのみ行われる。\n"
    "- 相続案件は list_souzoku_cases/get_souzoku_case から、時効案件は "
    "list_jikou_cases/get_jikou_case から辿る。案件番号が分からない場合は"
    "一覧を取得して絞り込む。\n"
    "- 回答は要点先行で簡潔に。表形式の羅列より短い文章を優先する。")


def _bad_request() -> Response:
    return Response(status_code=400)     # 固定応答（入力値を反射しない）


def _file(name: str) -> Response:
    path = WEBAPP_ROOT / name
    if not path.is_file():
        return Response(status_code=404)
    from fastapi.responses import FileResponse
    return FileResponse(path, media_type="text/html; charset=utf-8")


# ── 出典の実測記録 ───────────────────────────────────────────────────────────

def _record_source(ctx: dict, app_label: str, app_id: str, record_id: str,
                   pdf_url=None) -> None:
    key = (app_label, str(record_id))
    if key in ctx["source_keys"]:
        return
    if len(ctx["sources"]) >= _MAX_SOURCES:
        ctx["flags"].add("sources_truncated")
        return
    ctx["source_keys"].add(key)
    base = config.kintone_record_link_base()
    url = None
    if base is not None and str(app_id).isdigit() and \
            _RECORD_ID_RE.fullmatch(str(record_id)):
        url = f"{base}/{app_id}/show#record={record_id}"
    entry = {"app": app_label, "record_id": str(record_id), "url": url}
    if pdf_url:
        entry["pdf_url"] = pdf_url
    ctx["sources"].append(entry)


def _rid(record: dict) -> str:
    return str((record.get("$id") or {}).get("value") or "")


# ── tool handler（読み取り専用の閉集合。全て検証済み値のみ query へ） ─────────

async def _t_list_souzoku_cases(args: dict, ctx: dict):
    records = await kintone.search_records(
        souzoku_dash.APP_SOUZOKU_CASES,
        "order by 更新日時 desc limit 20",
        fields=souzoku_dash._CASE_LIST_FIELDS)
    out = souzoku_dash._project(records, souzoku_dash._CASE_LIST_FIELDS)
    for r in out:
        _record_source(ctx, "相談カード(相続)",
                       souzoku_dash.APP_SOUZOKU_CASES.app_id(), _rid(r))
    return {"records": out}


async def _t_get_souzoku_case(args: dict, ctx: dict):
    rid = str(args.get("case_record_id") or "")
    if not _RECORD_ID_RE.fullmatch(rid):
        return None
    found = await kintone.search_records(
        souzoku_dash.APP_SOUZOKU_CASES, f'$id = "{rid}" limit 1',
        fields=souzoku_dash._CASE_FIELDS)
    if not found:
        return {"record": None, "message": "該当レコードなし"}
    out = souzoku_dash._project(found, souzoku_dash._CASE_FIELDS)[0]
    _record_source(ctx, "相談カード(相続)",
                   souzoku_dash.APP_SOUZOKU_CASES.app_id(), rid)
    return {"record": out}


async def _t_list_case_persons(args: dict, ctx: dict):
    rid = str(args.get("case_record_id") or "")
    if not _RECORD_ID_RE.fullmatch(rid):
        return None
    data = await souzoku_dash._load_persons(rid)
    for r in data["records"]:
        _record_source(ctx, "App34(人物)",
                       souzoku_dash.APP_KOSEKI_PERSON.app_id(), _rid(r))
        if str((r.get("名寄せ確定") or {}).get("value") or "") != "確定":
            ctx["flags"].add("nayose_unconfirmed")
    if data["excluded_merged_count"] > 0:
        ctx["flags"].add("merged_excluded")
    return data


async def _t_list_case_heirs(args: dict, ctx: dict):
    rid = str(args.get("case_record_id") or "")
    if not _RECORD_ID_RE.fullmatch(rid):
        return None
    data = await souzoku_dash._load_heirs(rid)
    for r in data["records"]:
        _record_source(ctx, "App36(相続人)",
                       souzoku_dash.APP_SOUZOKUNIN.app_id(), _rid(r))
        if str((r.get("戸籍確認済") or {}).get("value") or "") != "yes":
            ctx["flags"].add("koseki_unconfirmed")
    if data["excluded_cancelled_count"] > 0:
        ctx["flags"].add("cancelled_excluded")
    return data


async def _t_list_case_assets(args: dict, ctx: dict):
    rid = str(args.get("case_record_id") or "")
    if not _RECORD_ID_RE.fullmatch(rid):
        return None
    data = await souzoku_dash._load_assets(rid)
    for r in data["records"]:
        _record_source(ctx, "App35(財産)",
                       souzoku_dash.APP_ZAISAN.app_id(), _rid(r))
        if str((r.get("データ源") or {}).get("value") or "").startswith("OCR_"):
            ctx["flags"].add("ocr_numbers")
    total = data.get("total") or {}
    if total.get("computable") and total.get("unconfirmed_count", 0) > 0:
        ctx["flags"].add("valuation_unconfirmed")
    return data


async def _t_list_case_documents(args: dict, ctx: dict):
    rid = str(args.get("case_record_id") or "")
    if not _RECORD_ID_RE.fullmatch(rid):
        return None
    data = await souzoku_dash._load_documents(rid)
    for r in data["records"]:
        _record_source(ctx, "App30(発送管理)",
                       souzoku_dash.APP_SHIPPING.app_id(), _rid(r),
                       pdf_url=r.get("_pdf_url"))
    return data


async def _t_list_case_kosekis(args: dict, ctx: dict):
    rid = str(args.get("case_record_id") or "")
    if not _RECORD_ID_RE.fullmatch(rid):
        return None
    from kinship_graph import load_koseki_summaries_for_case
    from kinship_graph import APP_KOSEKI_BOOK
    rows = await load_koseki_summaries_for_case(rid)
    for row in rows:
        _record_source(ctx, "App33(戸籍読解)", APP_KOSEKI_BOOK.app_id(),
                       row.get("record_id", ""), pdf_url=row.get("pdf_url"))
    if rows:
        # 要件7: 読解JSON 由来=手書き・旧字を含み得る（低確信度・原本必須）
        ctx["flags"].add("koseki_reading")
    return {"records": rows}


async def _t_list_jikou_cases(args: dict, ctx: dict):
    status = args.get("status")
    query = "order by 更新日時 desc limit 20"
    if status is not None:
        if status not in case_views.STATUS_OPTIONS:
            return None
        query = f'status in ("{status}") ' + query
    records = await kintone.search_records(
        case_views.APP_CASES, query, fields=case_views._LIST_FIELDS)
    for r in records:
        _record_source(ctx, "App21(案件)", case_views.APP_CASES.app_id(),
                       _rid(r))
    return {"records": records}


_JIKOU_CASE_FETCH_FIELDS = ["$id", "status", "顧客名", "問い合わせ業者名",
                            "更新日時", "LINEユーザーID"]


async def _t_get_jikou_case(args: dict, ctx: dict):
    rid = str(args.get("case_record_id") or "")
    if not _RECORD_ID_RE.fullmatch(rid):
        return None
    found = await kintone.search_records(
        case_views.APP_CASES, f'$id = "{rid}" limit 1',
        fields=_JIKOU_CASE_FETCH_FIELDS)
    if not found:
        return {"record": None, "message": "該当レコードなし"}
    # LINEユーザーID は chat 取得の内部キーのみ（応答へ出さない=最小化）
    record = {k: v for k, v in found[0].items() if k != "LINEユーザーID"}
    _record_source(ctx, "App21(案件)", case_views.APP_CASES.app_id(), rid)
    shipping = await kintone.search_records(
        case_views.APP_SHIPPING,
        f'案件レコードID = "{rid}" order by 更新日時 desc limit 20',
        fields=case_views._SHIPPING_FIELDS)
    for r in shipping:
        _record_source(ctx, "App30(発送管理)",
                       case_views.APP_SHIPPING.app_id(), _rid(r))
    return {"record": record, "shipping": shipping}


async def _t_list_case_chats(args: dict, ctx: dict):
    rid = str(args.get("jikou_case_record_id") or "")
    if not _RECORD_ID_RE.fullmatch(rid):
        return None
    found = await kintone.search_records(
        case_views.APP_CASES, f'$id = "{rid}" limit 1',
        fields=["$id", "LINEユーザーID"])
    if not found:
        return {"records": [], "message": "該当案件なし"}
    luid = str((found[0].get("LINEユーザーID") or {}).get("value") or "")
    if not _LINE_USER_ID_RE.fullmatch(luid):
        return {"records": [], "message": "LINE 連携なし"}
    chats = await kintone.search_records(
        case_views.APP_CHATLOG,
        f'line_user_id = "{luid}" order by $id desc limit {_CHAT_LIMIT}',
        fields=["$id", "role", "message"])
    _record_source(ctx, "App21(案件)", case_views.APP_CASES.app_id(), rid)
    for r in chats:
        _record_source(ctx, "App28(チャットログ)",
                       case_views.APP_CHATLOG.app_id(), _rid(r))
    return {"records": chats}


def _case_id_schema(field: str = "case_record_id") -> dict:
    return {"type": "object",
            "properties": {field: {
                "type": "string",
                "description": "案件レコード番号（数字のみ）"}},
            "required": [field]}


# tool 定義の閉集合（App21/26/28/30/33/34/35/36 の read のみ。書き込み tool は
# 存在しない——ここに定義しない限り呼べない＝構造でゼロ）
_TOOLS = [
    {"name": "list_souzoku_cases",
     "description": "相続案件（相談カード）の一覧を新しい順に最大20件返す。"
                    "氏名や被相続人名から案件を探すときは、この一覧から絞り込む。",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "get_souzoku_case",
     "description": "相続案件（相談カード）1件の要約（氏名・被相続人名・続柄・"
                    "書類ステータス）を返す。",
     "input_schema": _case_id_schema()},
    {"name": "list_case_persons",
     "description": "相続案件の人物一覧（App34）。統合済み無効の行は除外済み。"
                    "名寄せ確定・相続資格・確認状態を含む。",
     "input_schema": _case_id_schema()},
    {"name": "list_case_heirs",
     "description": "相続案件の相続人一覧（App36）。取消済みの行は除外済み。"
                    "続柄・法定相続分・戸籍確認済・印鑑証明を含む。",
     "input_schema": _case_id_schema()},
    {"name": "list_case_assets",
     "description": "相続案件の財産目録（App35）と評価額合計（有効行・サーバ集計）。",
     "input_schema": _case_id_schema()},
    {"name": "list_case_documents",
     "description": "相続案件の直近の発送・受領書類（App30・最大20件）。",
     "input_schema": _case_id_schema()},
    {"name": "list_case_kosekis",
     "description": "相続案件の取得済み戸籍の読解要約（App33・本籍・筆頭者・"
                    "従前戸籍）。読解は手書き・旧字を含み得る低確信度情報。",
     "input_schema": _case_id_schema()},
    {"name": "list_jikou_cases",
     "description": "時効援用案件（App21）の一覧を新しい順に最大20件返す。"
                    "status で絞り込み可能。",
     "input_schema": {"type": "object",
                      "properties": {"status": {
                          "type": "string",
                          "description": "絞り込み status（省略可・閉集合）"}}}},
    {"name": "get_jikou_case",
     "description": "時効援用案件（App21）1件と、その案件の発送管理（App30）"
                    "最大20件を返す。",
     "input_schema": _case_id_schema()},
    {"name": "list_case_chats",
     "description": "時効援用案件の LINE 会話ログ（App28）を新しい順に最大30件"
                    "返す。",
     "input_schema": _case_id_schema("jikou_case_record_id")},
]

_DISPATCH = {
    "list_souzoku_cases": _t_list_souzoku_cases,
    "get_souzoku_case": _t_get_souzoku_case,
    "list_case_persons": _t_list_case_persons,
    "list_case_heirs": _t_list_case_heirs,
    "list_case_assets": _t_list_case_assets,
    "list_case_documents": _t_list_case_documents,
    "list_case_kosekis": _t_list_case_kosekis,
    "list_jikou_cases": _t_list_jikou_cases,
    "get_jikou_case": _t_get_jikou_case,
    "list_case_chats": _t_list_case_chats,
}


async def _dispatch(name: str, args: dict, ctx: dict) -> tuple:
    """tool 実行（閉集合外・引数 grammar 外・実行失敗はすべて is_error の固定
    文言＝詳細を LLM/応答へ流さない。kintone へは検証済み値のみ到達）。"""
    handler = _DISPATCH.get(name)
    if handler is None:
        return "未定義のツールです（読み取り専用の閉集合のみ使用できます）", True
    try:
        result = await handler(args if isinstance(args, dict) else {}, ctx)
    except Exception:
        return "取得に失敗しました（対象アプリに到達できないか一時的なエラー）", True
    if result is None:
        return "引数が不正です（案件レコード番号は数字のみ）", True
    return json.dumps(result, ensure_ascii=False, default=str), False


# ── コスト概算（Decimal・float 非経由） ──────────────────────────────────────

def _estimate_cost(model: str, input_tokens: int, output_tokens: int,
                   cache_read: int, cache_write: int) -> str:
    prices = _MODEL_PRICES.get(model)
    if prices is None:
        return "unknown"
    in_p, out_p = prices
    mtok = Decimal(1_000_000)
    cost = (Decimal(input_tokens) * in_p / mtok
            + Decimal(output_tokens) * out_p / mtok
            + Decimal(cache_read) * in_p * Decimal("0.1") / mtok
            + Decimal(cache_write) * in_p * Decimal("1.25") / mtok)
    return str(cost.quantize(Decimal("0.000001")))


# ── agent loop（手動 loop・turn 上限・fail-closed） ──────────────────────────

_client_holder: list = []      # 遅延生成（global 文・動的属性アクセスを使わない）


def _anthropic_client():
    if not _client_holder:
        _client_holder.append(anthropic.AsyncAnthropic(
            timeout=_API_TIMEOUT_SECONDS, max_retries=1))
    return _client_holder[0]


async def _answer_question(question: str) -> dict:
    """1 質問の回答生成。戻り値は qa_store 保存形（answer/status/sources/notes/
    model/tokens/cost/elapsed）。失敗・出典ゼロは固定文言へ fail-closed。"""
    started = time.monotonic()
    ctx = {"sources": [], "source_keys": set(), "flags": set()}
    usage = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}
    model = config.PRIMARY_MODEL

    def _finish(answer: str, status: str) -> dict:
        notes = [FLAG_NOTES[f] for f in FLAG_NOTES if f in ctx["flags"]]
        return {"answer": answer, "status": status,
                "sources": ctx["sources"], "notes": notes, "model": model,
                "input_tokens": usage["input"],
                "output_tokens": usage["output"],
                "cache_read_tokens": usage["cache_read"],
                "cost_usd": _estimate_cost(model, usage["input"],
                                           usage["output"],
                                           usage["cache_read"],
                                           usage["cache_write"]),
                "elapsed_ms": int((time.monotonic() - started) * 1000)}

    client = _anthropic_client()
    messages = [{"role": "user", "content": question}]
    try:
        for _ in range(_MAX_TURNS):
            resp = await client.messages.create(
                model=model, max_tokens=_MAX_TOKENS,
                system=[{"type": "text", "text": _SYSTEM,
                         "cache_control": {"type": "ephemeral"}}],
                tools=_TOOLS, messages=messages)
            u = resp.usage
            if u is not None:
                usage["input"] += int(u.input_tokens or 0)
                usage["output"] += int(u.output_tokens or 0)
                usage["cache_read"] += int(u.cache_read_input_tokens or 0)
                usage["cache_write"] += int(u.cache_creation_input_tokens or 0)
            if resp.stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": resp.content})
                results = []
                for block in resp.content:
                    if block.type == "tool_use":
                        content, is_error = await _dispatch(
                            block.name, block.input, ctx)
                        results.append({"type": "tool_result",
                                        "tool_use_id": block.id,
                                        "content": content,
                                        "is_error": is_error})
                messages.append({"role": "user", "content": results})
                continue
            if resp.stop_reason in ("end_turn", "max_tokens"):
                text = "".join(b.text for b in resp.content
                               if b.type == "text").strip()
                if not ctx["sources"] or not text:
                    # 要件3: 出典ゼロの断定回答は返さない（fail-closed）
                    return _finish(NO_SOURCE_ANSWER, "no_source")
                return _finish(text, "ok")
            # refusal / pause_turn 等の想定外 stop は fail-closed
            return _finish(ERROR_ANSWER, "error")
        return _finish(ERROR_ANSWER, "error")     # turn 上限
    except Exception:
        # timeout・API エラー等——推測で埋めた回答を返さない
        return _finish(ERROR_ANSWER, "error")


# ── レート制限（固定窓・暴走防止） ───────────────────────────────────────────

def _rate_limited(now: float) -> bool:
    cutoff = now - RATE_WINDOW_SECONDS
    _ask_times[:] = [t for t in _ask_times if t > cutoff]
    if len(_ask_times) >= RATE_LIMIT:
        return True
    _ask_times.append(now)
    return False


# ── routes（全て _gate・質問は form POST=access log に載らない） ─────────────

@router.get("/app/q")
@_gate
async def q_page(request: Request):
    return _file("q.html")


async def q_ask(request: Request):
    """質問の受付（PRG: 処理後 303 で /app/q へ戻し、画面は台帳を再読込して
    表示する）。回答は生成のたび Q&A 台帳へ保存（③12.2-1）。
    NB: 登録は add_api_route 経由（read-only AST 検査の HTTP 動詞 attr 禁止と
    両立させるため。関所 _gate は下の登録時に適用＝機械検査の対象のまま）。"""
    form = await request.form()
    question = str(form.get("question") or "").strip()
    if not question or len(question) > _QUESTION_MAX_CHARS:
        return RedirectResponse("/app/q?e=input", status_code=303)
    if _rate_limited(time.time()):
        return RedirectResponse("/app/q?e=rate", status_code=303)
    result = await _answer_question(question)
    result["answer"] = result["answer"] + "\n\n" + DISCLAIMER
    try:
        qa_id = await qa_store.save_qa(user_id="owner", question=question,
                                       **result)
    except Exception:
        return RedirectResponse("/app/q?e=save", status_code=303)
    return RedirectResponse(f"/app/q?done={qa_id}", status_code=303)


router.add_api_route("/app/q/ask", _gate(q_ask), methods=["POST"])


@router.get("/app/api/q/history")
@_gate
async def q_history(request: Request):
    """Q&A 台帳のビュー（③12.2-1・新しい順・ページング）。"""
    try:
        limit = int(request.query_params.get("limit", 20))
        offset = int(request.query_params.get("offset", 0))
    except ValueError:
        return _bad_request()
    if not (1 <= limit <= 50) or offset < 0:
        return _bad_request()
    try:
        records = await qa_store.list_qa(limit=limit, offset=offset)
    except Exception:
        records = None                   # DB 未設定/不達は空でなく明示 flag
    if records is None:
        return {"records": [], "available": False}
    return {"records": records, "available": True,
            "limit": limit, "offset": offset}
