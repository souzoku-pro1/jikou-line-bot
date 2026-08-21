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
  質問レート制限（スライディング窓・完了計上＝Q-UX-1(C)）＋single-flight
  （処理中の重複 POST は実行しない＝Q-UX-1(B)）。API 呼出しは timeout 付き・
- Q-CHAT-1 会話化: 直近の会話（前回リセット以降・二重上限つき）を文脈として
  注入。履歴は文脈情報であり出典ではない（出典は当該 turn 内の実測 tool 結果
  のみ＝subset 照合不変）。「新しい話題」で境界を永続化（qa_topic_reset）。
  turn 上限付きで、失敗時は推測で埋めず固定文言へ fail-closed。
"""

import json
import re
import time
from decimal import Decimal

import anthropic
import anyio
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
# Q-01: 消費量の固定上限（いずれも超過は fail-closed・黙って続行しない）
TOTAL_TIMEOUT_SECONDS = 300       # 1 質問全体の wall-clock 上限
_MAX_TOOL_CALLS_TOTAL = 20        # 全 turn 合計の tool 呼び出し数上限
_MAX_TOOL_USE_PER_TURN = 5        # 1 response 内の tool_use 数上限
_MAX_TOOL_RESULT_BYTES = 50_000   # tool 結果（canonical JSON）の byte 上限

# Q-CHAT-1(A): 会話文脈の二重上限。直近 10 往復かつ合計 6000 字（質問+回答の
# 文字数合計）。6000 字 ≒ 日本語でおおむね 6〜9k tokens ≒ 入力 $0.02〜0.03/問
# の上乗せ（Sonnet $3/MTok）で、直近数往復は無切詰めで残る均衡点。1 回答は
# 800 字で末尾切り詰め（要約のための追加 API 呼出しはしない＝票の指定）。
# 超過は古い側から往復ごと丸ごと落とす（新しい側優先）
_HISTORY_MAX_EXCHANGES = 10
_HISTORY_MAX_TOTAL_CHARS = 6000
_HISTORY_ANSWER_MAX_CHARS = 800
_HISTORY_TRUNC_MARK = "…（以下省略）"

# レート制限（スライディング窓・単一利用者前提のプロセス内カウンタ）。
# Q-UX-1(C) 裁定: 「完了した回答生成」（ok/no_source）のみ計上する——error
# （時間切れ・API エラー＝回答生成が完了しなかったもの）と、single-flight で
# 弾いた重複 POST は数えない。制限中も新規計上しない（窓を延長しない）ため、
# 最初の RATE_LIMIT 件が期限切れになれば必ず自然回復する。
RATE_WINDOW_SECONDS = 600
RATE_LIMIT = 10
_ask_times: list = []
# Q-UX-1(B): single-flight marker（処理中は 1 要素・完了/失敗で必ず空へ）。
# 診断 Q-RATE-DIAG で実測された「1 回の質問操作が再送・連打で複数 POST に
# 増幅される」形をサーバ側でも遮断する（増幅分は実行も計上もしない）。
_inflight: list = []

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
TOO_LARGE_RESULT = (
    "対象が大きすぎます。案件や条件を絞って再質問してください（この呼び出しの"
    "結果は破棄され、回答の根拠には採用されません）。")

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
    "koseki_coverage_estimate": "戸籍不足チェックは機械的な参考見立てです——不足の確定は原本・戸籍全体のご確認によります（機械は確定しません）",
}

# Q-QUALITY-1(D): 出典 app ラベルの閉集合——サーバ実測記録（_record_source の
# 呼出しラベル）と submit_answer schema の enum の**共通の正**。乖離はテストで
# pin（AST 走査で _record_source の全ラベルと突合）。モデルは strict schema の
# enum からしか選べないため、ラベル表記ゆれによる照合不一致が構造的にゼロになる
SOURCE_APP_LABELS = (
    "相談カード(相続)",
    "App34(人物)",
    "App36(相続人)",
    "App35(財産)",
    "App30(発送管理)",
    "App33(戸籍読解)",
    "App21(案件)",
    "App28(チャットログ)",
)

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
    "- 回答を終えるときは必ず submit_answer ツールを 1 回呼ぶ。source_refs "
    "には、回答で実際に使用した出典（この会話で読み取りツールが返した "
    "app と record_id）だけを列挙する。読んでいない記録は挙げない。\n"
    "- 出典の書き方: 各ツール結果末尾の _citation_keys にある app ラベルと "
    "record_id を**そのまま**使う（app は submit_answer の選択肢からのみ"
    "選べる）。自分でラベルを言い換えない。\n"
    "- 人名・名称が完全一致で見つからないときは、旧字/新字（例: 澤/沢・"
    "邊/辺・齋/斉）やかな表記のゆれを考慮して、一覧の実データから探し直す。\n"
    "- 完全一致が無くても近い実在レコードがあれば、断定せず"
    "「◯◯さんの案件（No.X）がありますが、こちらのことですか？」の形で候補と"
    "して提示する（候補も必ず出典つき・ツールが返した実在レコードのみ。"
    "記録に無い名前や番号を作らない）。\n"
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
    """Q-02(i): grammar 成立（app_id=数字列・record_id=数字列）のときのみ
    有効出典として記録する。空・不正値は出典に数えない（fail-closed）。

    Q-QUALITY-1-fix1（Q-QUALITY-01）: app_label は SOURCE_APP_LABELS の閉集合を
    **実行時に必須検証**——閉集合外は即時例外。呼出し方（alias・wrapper・動的
    組立て）に依存しない保証で、閉集合外ラベルが ctx["sources"]/_citation_keys
    へ入り enum 不在で提出不能になる系統欠陥の再発を防ぐ（AST 三者一致 pin は
    多層防御として併存）。例外は _dispatch の except で is_error の固定文言に
    落ちる＝当該 tool 結果は採用されず fail-closed のまま。"""
    if app_label not in SOURCE_APP_LABELS:
        raise ValueError("source app label outside closed set")
    app_id_s = str(app_id or "")
    rid_s = str(record_id or "")
    if not app_id_s.isdigit() or not _RECORD_ID_RE.fullmatch(rid_s):
        return
    key = (app_label, rid_s)
    if key in ctx["source_keys"]:
        return
    ctx["source_keys"].add(key)
    base = config.kintone_record_link_base()
    url = f"{base}/{app_id_s}/show#record={rid_s}" if base is not None else None
    entry = {"app": app_label, "record_id": rid_s, "url": url}
    if pdf_url:
        entry["pdf_url"] = pdf_url
    ctx["sources"].append(entry)


def _merge_source(ctx: dict, entry: dict) -> None:
    """採用確定した tool 呼び出しの出典を本 ctx へ統合（上限つき）。"""
    key = (entry["app"], entry["record_id"])
    if key in ctx["source_keys"]:
        return
    if len(ctx["sources"]) >= _MAX_SOURCES:
        ctx["flags"].add("sources_truncated")
        return
    ctx["source_keys"].add(key)
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


async def _t_check_koseki_coverage(args: dict, ctx: dict):
    """KOSEKI-CHECK-1: 戸籍不足チェック（決定的検査は koseki_coverage が実施・
    本 handler は出典の実測記録と注記 flag のみ）。出典=検査に使った実レコード
    （App33 全行＋App36 相続人行）。モデルの役割は結果の平易な説明のみで、
    不足の確定はしない（koseki_coverage_estimate の定型注記が必ず付く）。"""
    rid = str(args.get("case_record_id") or "")
    if not _RECORD_ID_RE.fullmatch(rid):
        return None
    import koseki_coverage
    from kinship_graph import APP_KOSEKI_BOOK
    result = await koseki_coverage.check_coverage(rid)
    for row in result["chain"]["kosekis"]:
        _record_source(ctx, "App33(戸籍読解)", APP_KOSEKI_BOOK.app_id(),
                       row["record_id"],
                       pdf_url=config.drive_pdf_view_url(
                           row.get("drive_file_id") or ""))
    for h in result["heirs"]["rows"]:
        _record_source(ctx, "App36(相続人)",
                       souzoku_dash.APP_SOUZOKUNIN.app_id(), h["record_id"])
    if result["chain"]["kosekis"]:
        ctx["flags"].add("koseki_reading")
    ctx["flags"].add("koseki_coverage_estimate")
    return result


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
    {"name": "check_koseki_coverage",
     "description": "戸籍不足チェック（参考見立て・KOSEKI-CHECK-1）: 被相続人の"
                    "出生〜死亡の戸籍連続性の切れ目と、相続人ごとの現在戸籍の"
                    "有無を機械判定して返す。判定不能は理由つきで区別される"
                    "（不足の確定はしない）。",
     "input_schema": _case_id_schema()},
]

# Q-02(ii): 最終回答は submit_answer の構造化出力のみで受け付ける（本文 text
# での回答は採用しない）。source_refs はサーバ実測の出典集合との subset 照合に
# かける（参照欠落・実測集合外・切捨て領域参照は fail-closed）。
SUBMIT_TOOL_NAME = "submit_answer"
_SUBMIT_TOOL = {
    "name": SUBMIT_TOOL_NAME,
    "description": "最終回答の提出。回答本文と、回答で実際に使用した出典参照"
                   "（この会話で読み取りツールが返したレコードの app と "
                   "record_id のみ）を必ず指定する。回答を終えるときは必ず"
                   "このツールを 1 回呼ぶ。",
    "strict": True,
    "input_schema": {
        "type": "object",
        "properties": {
            "answer": {"type": "string",
                       "description": "日本語の最終回答（簡潔に）"},
            "source_refs": {
                "type": "array",
                "description": "回答で使用した出典参照の閉集合",
                "items": {
                    "type": "object",
                    "properties": {
                        # Q-QUALITY-1(D): app はサーバ既知ラベルの enum 閉集合。
                        # strict schema のためモデルはここからしか選べない
                        # （ラベル表記ゆれによる subset 照合不一致の構造的排除）
                        "app": {"type": "string",
                                "enum": list(SOURCE_APP_LABELS)},
                        "record_id": {"type": "string"},
                    },
                    "required": ["app", "record_id"],
                    "additionalProperties": False,
                }},
        },
        "required": ["answer", "source_refs"],
        "additionalProperties": False,
    },
}

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
    "check_koseki_coverage": _t_check_koseki_coverage,
}


async def _dispatch(name: str, args: dict, ctx: dict) -> tuple:
    """tool 実行（閉集合外・引数 grammar 外・実行失敗はすべて is_error の固定
    文言＝詳細を LLM/応答へ流さない。kintone へは検証済み値のみ到達）。

    Q-01(iv): 結果は canonical JSON の byte 上限で検査し、超過は**呼び出し
    ごと破棄**して固定文言で絞り込み再質問を誘導する（黙って切り捨てない）。
    破棄した呼び出しの出典・flag は本 ctx へ統合しない＝モデルがその領域を
    参照しても実測集合外として fail-closed になる（Q-02(iii)）。"""
    handler = _DISPATCH.get(name)
    if handler is None:
        return "未定義のツールです（読み取り専用の閉集合のみ使用できます）", True
    sub = {"sources": [], "source_keys": set(), "flags": set()}
    try:
        result = await handler(args if isinstance(args, dict) else {}, sub)
    except Exception:
        return "取得に失敗しました（対象アプリに到達できないか一時的なエラー）", True
    if result is None:
        return "引数が不正です（案件レコード番号は数字のみ）", True
    # Q-QUALITY-1(A): この呼び出しで実測記録した引用キー（app ラベル+
    # record_id）を結果に明示して返す——モデルは submit の source_refs に
    # これを**そのまま**使う（照合キーの非開示による不一致を解消）。
    # TOO_LARGE 破棄時は下の上限検査で結果ごと破棄される＝開示もされない
    result["_citation_keys"] = [
        {"app": s["app"], "record_id": s["record_id"]}
        for s in sub["sources"]]
    payload = json.dumps(result, ensure_ascii=False, default=str)
    if len(payload.encode("utf-8")) > _MAX_TOOL_RESULT_BYTES:
        return TOO_LARGE_RESULT, True
    for entry in sub["sources"]:
        _merge_source(ctx, entry)
    ctx["flags"] |= sub["flags"]
    return payload, False


async def _run_tools(blocks: list, ctx: dict) -> list:
    """Q-SPEED-1(b): 同一 turn 内の複数 tool 呼び出しを並列実行する。

    - tool_result の並び・出典（_merge_source）・flag の統合は **block 順**で
      行う（完了順に依存しない＝決定的。_MAX_SOURCES の切捨ても block 順）。
    - 各呼び出しは自前の local ctx で実行し、失敗は _dispatch 内で is_error の
      固定文言に落ちる（1 本の失敗が他を巻き込まない）。
    - 単一呼び出しは従来どおり直列（挙動同一・オーバーヘッドなし）。
    消費量上限（turn 内 5 本・合計 20 本）は呼出し前に検査済みの前提。"""
    if len(blocks) == 1:
        block = blocks[0]
        content, is_error = await _dispatch(block.name, block.input, ctx)
        return [{"type": "tool_result", "tool_use_id": block.id,
                 "content": content, "is_error": is_error}]
    buf: list = [None] * len(blocks)

    async def _one(i: int, block) -> None:
        local = {"sources": [], "source_keys": set(), "flags": set()}
        content, is_error = await _dispatch(block.name, block.input, local)
        buf[i] = (content, is_error, local)

    async with anyio.create_task_group() as tg:
        for i, block in enumerate(blocks):
            tg.start_soon(_one, i, block)
    results = []
    for block, item in zip(blocks, buf):
        content, is_error, local = item
        for entry in local["sources"]:
            _merge_source(ctx, entry)
        ctx["flags"] |= local["flags"]
        results.append({"type": "tool_result", "tool_use_id": block.id,
                        "content": content, "is_error": is_error})
    return results


def _set_message_cache_marker(messages: list) -> None:
    """Q-SPEED-1(a): 会話 prefix の incremental cache。message 側の既存 marker
    を外してから、最後の message の最後の dict block（tool_result）へ付け直す。
    breakpoint は常に system 1 個＋message 1 個の計 2 個（API 上限 4 内）。
    SDK オブジェクト（assistant content）と文字列 content には触れない。"""
    for msg in messages:
        content = msg.get("content") if isinstance(msg, dict) else None
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict):
                    block.pop("cache_control", None)
    last = messages[-1]
    if isinstance(last, dict) and isinstance(last.get("content"), list):
        dict_blocks = [b for b in last["content"] if isinstance(b, dict)]
        if dict_blocks:
            dict_blocks[-1]["cache_control"] = {"type": "ephemeral"}


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


def _validated_submission(args, ctx: dict):
    """Q-02(ii)(iii): submit_answer の subset 照合。回答で使用したと申告された
    source_refs がすべて**サーバ実測の出典集合**に載っているときのみ採用する。
    参照欠落（refs 空）・型不正・実測集合外（未記録・切捨て領域含む）は
    None＝fail-closed（no_source）。返り値は (answer, 採用出典 list)。"""
    if not isinstance(args, dict):
        return None
    answer = args.get("answer")
    refs = args.get("source_refs")
    if not isinstance(answer, str) or not answer.strip():
        return None
    if not isinstance(refs, list) or not refs:
        return None
    by_key = {(s["app"], s["record_id"]): s for s in ctx["sources"]}
    picked = []
    seen = set()
    for ref in refs:
        if not isinstance(ref, dict):
            return None
        key = (str(ref.get("app") or ""), str(ref.get("record_id") or ""))
        if key not in by_key:
            return None                  # 実測集合外＝根拠なし参照
        if key in seen:
            continue
        seen.add(key)
        picked.append(by_key[key])
    if not picked:
        return None
    return answer.strip(), picked


def _strip_disclaimer(answer: str) -> str:
    """履歴注入時のノイズ削減: 保存回答から定型文（DISCLAIMER）を除去。"""
    return answer.replace("\n\n" + DISCLAIMER, "").replace(DISCLAIMER, "")


def _build_history(rows: list) -> list:
    """Q-CHAT-1(A): 会話文脈 message 列（古→新の user/assistant 交互）を組む。

    rows は list_context_qa の戻り（新しい順・リセット境界より後・error 除外
    済み）。二重上限: 直近 _HISTORY_MAX_EXCHANGES 往復かつ合計
    _HISTORY_MAX_TOTAL_CHARS 字。超過は古い側から往復ごと丸ごと落とし、長い
    回答は _HISTORY_ANSWER_MAX_CHARS 字で末尾切り詰め（要約生成のための追加
    API 呼出しはしない）。履歴は**文脈情報であり出典ではない**——出典は当該
    質問の turn 内で実測した tool 結果のみ（subset 照合は不変）。"""
    picked = []
    total = 0
    for row in rows[:_HISTORY_MAX_EXCHANGES]:
        q = str(row.get("question") or "").strip()
        a = _strip_disclaimer(str(row.get("answer") or "")).strip()
        if len(a) > _HISTORY_ANSWER_MAX_CHARS:
            a = a[:_HISTORY_ANSWER_MAX_CHARS] + _HISTORY_TRUNC_MARK
        if not q or not a:
            continue
        if total + len(q) + len(a) > _HISTORY_MAX_TOTAL_CHARS:
            break                        # 新しい側優先・これより古い側は落とす
        total += len(q) + len(a)
        picked.append((q, a))
    messages = []
    for q, a in reversed(picked):        # 古→新
        messages.append({"role": "user", "content": q})
        messages.append({"role": "assistant", "content": a})
    return messages


async def _answer_question(question: str, history: list | None = None) -> dict:
    """1 質問の回答生成。戻り値は qa_store 保存形（answer/status/sources/notes/
    model/tokens/cost/elapsed）。失敗・出典ゼロは固定文言へ fail-closed。
    Q-01: 全体 wall-clock timeout・全 turn 合計/1 turn の tool 数上限つき。
    Q-CHAT-1: history（_build_history の戻り＝上限適用済み message 列）を
    質問の前に注入する。履歴注入分の入力 tokens は usage 経由でコスト概算に
    そのまま反映される。"""
    started = time.monotonic()
    ctx = {"sources": [], "source_keys": set(), "flags": set()}
    usage = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}
    model = config.PRIMARY_MODEL

    def _finish(answer: str, status: str) -> dict:
        notes = [FLAG_NOTES[f] for f in FLAG_NOTES if f in ctx["flags"]]
        # Q-02: 出典は「採用された回答が使用した参照」のみ。fail-closed 応答
        # （no_source/error）に実測出典を添えない（回答と出典の対応を崩さない）
        sources = ctx["sources"] if status == "ok" else []
        return {"answer": answer, "status": status,
                "sources": sources, "notes": notes, "model": model,
                "input_tokens": usage["input"],
                "output_tokens": usage["output"],
                "cache_read_tokens": usage["cache_read"],
                "cost_usd": _estimate_cost(model, usage["input"],
                                           usage["output"],
                                           usage["cache_read"],
                                           usage["cache_write"]),
                "elapsed_ms": int((time.monotonic() - started) * 1000)}

    client = _anthropic_client()
    messages = list(history or []) + [{"role": "user", "content": question}]
    total_calls = 0
    try:
        # Q-01(i): 質問全体の wall-clock 上限（anyio.fail_after=asyncio.timeout
        # 相当。asyncio は read-only checker の禁止 import 集合のため anyio）
        with anyio.fail_after(TOTAL_TIMEOUT_SECONDS):
            for _ in range(_MAX_TURNS):
                resp = await client.messages.create(
                    model=model, max_tokens=_MAX_TOKENS,
                    system=[{"type": "text", "text": _SYSTEM,
                             "cache_control": {"type": "ephemeral"}}],
                    tools=_TOOLS + [_SUBMIT_TOOL], messages=messages)
                u = resp.usage
                if u is not None:
                    usage["input"] += int(u.input_tokens or 0)
                    usage["output"] += int(u.output_tokens or 0)
                    usage["cache_read"] += int(u.cache_read_input_tokens or 0)
                    usage["cache_write"] += int(
                        u.cache_creation_input_tokens or 0)
                if resp.stop_reason == "tool_use":
                    blocks = [b for b in resp.content if b.type == "tool_use"]
                    submits = [b for b in blocks
                               if b.name == SUBMIT_TOOL_NAME]
                    if submits:
                        validated = _validated_submission(submits[0].input,
                                                          ctx)
                        if validated is None:
                            return _finish(NO_SOURCE_ANSWER, "no_source")
                        answer, picked = validated
                        ctx["sources"] = picked   # 出典=回答で使用した閉集合
                        return _finish(answer, "ok")
                    # Q-01(iii): 1 response 内の tool_use 数上限
                    if len(blocks) > _MAX_TOOL_USE_PER_TURN:
                        return _finish(ERROR_ANSWER, "error")
                    # Q-01(ii): 全 turn 合計の tool 呼び出し数上限
                    total_calls += len(blocks)
                    if total_calls > _MAX_TOOL_CALLS_TOTAL:
                        return _finish(ERROR_ANSWER, "error")
                    messages.append({"role": "assistant",
                                     "content": resp.content})
                    # Q-SPEED-1(b): 同一 turn の複数 tool は並列実行（統合は
                    # block 順で決定的）。(a): 会話 prefix の incremental cache
                    results = await _run_tools(blocks, ctx)
                    messages.append({"role": "user", "content": results})
                    _set_message_cache_marker(messages)
                    continue
                if resp.stop_reason in ("end_turn", "max_tokens"):
                    # Q-02: submit_answer を経ない本文回答は採用しない
                    # （参照欠落＝出典ゼロ扱いの fail-closed）
                    return _finish(NO_SOURCE_ANSWER, "no_source")
                # refusal / pause_turn 等の想定外 stop は fail-closed
                return _finish(ERROR_ANSWER, "error")
            return _finish(ERROR_ANSWER, "error")     # turn 上限
    except Exception:
        # wall-clock timeout・API エラー等——推測で埋めた回答を返さない
        return _finish(ERROR_ANSWER, "error")


# ── レート制限（固定窓・暴走防止） ───────────────────────────────────────────

def _rate_status(now: float) -> tuple:
    """(制限中か, 回復までの目安秒) を返す。判定のみで計上はしない（計上は
    q_ask が回答生成の完了後に _count_completed_ask で行う＝Q-UX-1(C)）。"""
    cutoff = now - RATE_WINDOW_SECONDS
    _ask_times[:] = [t for t in _ask_times if t > cutoff]
    if len(_ask_times) >= RATE_LIMIT:
        # 最古の計上が窓から抜けるまでの秒数（切り上げ・最低 1 秒）
        return True, max(int(min(_ask_times) + RATE_WINDOW_SECONDS - now) + 1, 1)
    return False, 0


def _count_completed_ask(now: float) -> None:
    """完了した回答生成（ok/no_source）を 1 件計上する。error は呼ばない。"""
    _ask_times.append(now)


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
    # Q-UX-1(B): single-flight——処理中に来た POST（再送・連打の増幅分）は
    # 実行せず戻す。レート計上もしない。check→append の間に await が無いため
    # 単一 event loop 上で原子的
    if _inflight:
        return RedirectResponse("/app/q?e=busy", status_code=303)
    limited, retry = _rate_status(time.time())
    if limited:
        # Q-UX-1(C): 回復までの目安秒を添える（質問文は含まない・PII 規律維持）
        return RedirectResponse(f"/app/q?e=rate&retry={retry}",
                                status_code=303)
    _inflight.append(1)
    try:
        # Q-CHAT-1(A): 直近の会話（前回リセット以降）を文脈として注入。文脈は
        # 補助情報のため、読めない場合（migration 未適用等）は無文脈で続行
        # （質問機能自体は fail-open・出典規律は turn 内実測のみで不変）
        try:
            context_rows = await qa_store.list_context_qa(
                limit=_HISTORY_MAX_EXCHANGES)
        except Exception:
            context_rows = []
        result = await _answer_question(question,
                                        history=_build_history(context_rows))
        # Q-UX-1(C): 完了した回答生成（ok/no_source）のみ計上。error は数えない
        if result["status"] != "error":
            _count_completed_ask(time.time())
        result["answer"] = result["answer"] + "\n\n" + DISCLAIMER
        try:
            qa_id = await qa_store.save_qa(user_id="owner", question=question,
                                           **result)
        except Exception:
            return RedirectResponse("/app/q?e=save", status_code=303)
        return RedirectResponse(f"/app/q?done={qa_id}", status_code=303)
    finally:
        # Q-UX-1-fix1（R-Q-UX-1 Q-UX-01）: marker は POST 処理全体（レート計上・
        # DISCLAIMER 付加・qa_store.save_qa・303 生成まで）を覆い、保存成功・
        # 保存例外のいずれでも最後に必ず解放する（解放後まで重複 POST は e=busy）。
        # NB: worker 複数化時は in-memory marker では不成立——共有ストア/DB
        # ロック方式への改定が必要（現行は単一 worker 前提・Codex 所見の記録）
        del _inflight[:]


router.add_api_route("/app/q/ask", _gate(q_ask), methods=["POST"])


async def q_reset(request: Request):
    """Q-CHAT-1(B): 話題リセット（「新しい話題」）。以降の質問は過去履歴を
    一切参照しない。質問ではないためレート計上はしない（API コストもゼロ）。
    回答生成中は境界が処理中の保存行とねじれるため busy で弾く。PRG で戻る。"""
    if _inflight:
        return RedirectResponse("/app/q?e=busy", status_code=303)
    # Q-CHAT-1-fix1（R-Q-CHAT-1 Q-CHAT-01）: reset も同じ single-flight 所有権を
    # 取得し、境界保存完了と 303 生成まで保持する（check→append の間に await
    # なし＝原子的）。これで reset 処理中の ask は e=busy で遮断され、ask⇔reset
    # の**双方向**排他が成立——質問の帰属（旧話題/新話題）が DB 到達順でなく
    # 実行順で決定的になる
    _inflight.append(1)
    try:
        try:
            await qa_store.save_topic_reset(user_id="owner")
        except Exception:
            return RedirectResponse("/app/q?e=save", status_code=303)
        return RedirectResponse("/app/q", status_code=303)
    finally:
        del _inflight[:]                 # 保存成功・保存例外いずれも必ず解放


router.add_api_route("/app/q/reset", _gate(q_reset), methods=["POST"])


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
    # Q-CHAT-1(B): リセット境界の可視化。境界が読めない場合（migration 未適用
    # 等）は「リセット無し」へ縮退（台帳表示自体は生かす=fail-open）
    try:
        boundary = await qa_store.latest_reset_boundary()
    except Exception:
        boundary = None
    for rec in records:
        rec["after_reset"] = boundary is not None and rec["id"] > boundary
    return {"records": records, "available": True,
            "limit": limit, "offset": offset,
            "has_reset": boundary is not None}
