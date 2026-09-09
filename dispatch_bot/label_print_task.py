"""Bot語彙: 宛名ラベルの印字（LABEL-PRINT-1 第 2 段・fix1 ジョブ状態機械・隔離 module）

正本: LABEL-PRINT-1 票（裁定 D1〜D9）＋ fix1（Codex LP-01〜06 採用）。
設計サーベイ Desktop\\claude\\ラベル印字_設計.md。

- 入口は LINE 指示ボットのみ（D3）。指示は「ラベル」で始まる決定論の構文で解析する
  （Claude 解析を経ない＝registry.direct_match_fn。モデルの推測で宛先を決めない）。
- 宛先 4 種（D4）: 依頼者（App 21/26/40）・債権者 n（App 40 債権者一覧 n 行目）・
  役所 名（App 31・M1 の解決関数を流用）・発送 No（App 30 の宛先欄）。相手方・裁判所は
  「住所欄がないため未対応」と返す。郵便番号が無い宛先は 〒 なしで印字し復唱に付記。
- 敬称の既定（D6）: 依頼者=様・債権者=御中・役所=御中・発送=宛先名そのまま。指示末尾の
  「様」「御中」で上書き。
- 生成 PDF は案件レコードの FILE 欄「宛名ラベル」へ添付（既存添付は残す・D5）。追記は最新
  $revision の CAS で行い、409 は再取得→既存添付に自分の fileKey をマージして再試行（最大 3 回・
  LP-03）。役所・発送宛は指示に併記した案件番号のレコードへ添付する。
- ジョブ状態機械（hub/label_sheet・LP-01/02/04）: OK 受信時に 1 トランザクションで面を予約
  （event_id 一意＝再配送の無効化・シート行ロック・提案面が埋まっていれば予約せず再指示）→
  トランザクション外で PDF 生成・アップロード・添付 → attached。添付失敗は reserved のまま残し、
  同一宛先・同一面の別イベントで「回収」（再予約しない・アップロード済み fileKey を再利用）。
- 操作コマンド（印刷済／戻す／新しいシート）は event_id 冪等（LP-06）。「残量」は読取のみ。
- **RV-10（D8）/ LP-05**: 復唱・返信・通知・ログに氏名/住所を載せない。構文エラーの返信は固定文言と
  書式案内のみ（入力原文・未解釈トークンを含めない）。ログは長さと分類のみ。
"""

import logging
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from channels.shokumu_seikyu import APP_CITY_MASTER, municipality_office_name
from dispatch_bot.case_search import CaseHit
from dispatch_bot.sortation_assign import _CANCEL_WORDS
from hub import kintone, label_sheet
from hub.address_label import layout_spec, render_label_sheet
from hub.db import DatabaseNotConfigured
from hub.redact import emit

logger = logging.getLogger("dispatch_bot.label_print_task")

TASK_TYPE = "label_print"
LAYOUT = "A4_2x5_aone31514"
ATTACH_FIELD = "宛名ラベル"
ATTACH_CAS_ATTEMPTS = 3
_JST = ZoneInfo("Asia/Tokyo")

APP_JIKOU_CASE = kintone.KintoneApp("App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")
APP_SOUDAN_CARD = kintone.KintoneApp("相談カード (相続)", "SOUZOKU_KINTONE_APP_ID",
                                     "SOUZOKU_KINTONE_API_TOKEN")
APP_HOUKI_CASE = kintone.KintoneApp("App 40 (相続放棄案件)", "APP_HOUKI", "TOKEN_HOUKI")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

# 指示の単位語 → (表示名, 案件アプリ, 氏名欄)。App 21/26/40 とも郵便番号欄は
# 「郵便番号」（無ければ空＝〒 なし）。
UNITS: dict[str, tuple[str, kintone.KintoneApp, str]] = {
    "時効": ("時効援用", APP_JIKOU_CASE, "顧客名"),
    "相続": ("相続一般", APP_SOUDAN_CARD, "氏名"),
    "放棄": ("相続放棄", APP_HOUKI_CASE, "顧客名"),
}
_UNIT_ALIASES = {"時効援用": "時効", "相続一般": "相続", "相続放棄": "放棄",
                 "時効": "時効", "相続": "相続", "放棄": "放棄"}
# App 30 ユニット種別 → 単位語
_UNIT_FROM_SHIPPING = {"時効援用": "時効", "相続一般": "相続", "相続放棄": "放棄"}

ROLES_SUPPORTED = ("依頼者", "債権者", "役所", "発送")
ROLES_UNSUPPORTED = ("相手方", "裁判所")
DEFAULT_HONORIFIC = {"依頼者": "様", "債権者": "御中", "役所": "御中", "発送": ""}

USAGE = ("書式: 「ラベル 時効/相続/放棄 No.17 依頼者」「ラベル 放棄 No.17 債権者 2」"
         "「ラベル 放棄 No.17 役所 川口市」「ラベル 発送 No.120 案件 No.17」"
         "「… 御中」「… 面 7」／「ラベル 印刷済」「ラベル 戻す」「ラベル 新しいシート」「ラベル 残量」")
# 構文エラーの固定文言（LP-05: 入力原文・未解釈トークンを含めない）
MSG_SYNTAX_START = f"「ラベル」で始めてください。{USAGE}"
MSG_SYNTAX_UNKNOWN_WORD = f"解釈できない語があります。{USAGE}"
MSG_SYNTAX_NO_ROLE = f"宛先の種類（依頼者・債権者 n・役所 名・発送 No）が要ります。{USAGE}"
MSG_SYNTAX_NO_CASE = f"案件番号（例: No.17）が要ります。{USAGE}"
MSG_SYNTAX_NO_CASE_FOR_SHIPPING = f"添付先の案件番号を併記してください（例: ラベル 発送 No.120 案件 No.17）。{USAGE}"
MSG_SYNTAX_SHIPPING_NO = f"「発送」の後に App 30 の番号（例: No.120）が要ります。{USAGE}"
MSG_SYNTAX_CASE_NO = f"「案件」の後に案件番号（例: No.17）が要ります。{USAGE}"
MSG_SYNTAX_CREDITOR_NO = f"「債権者」の後に債権者一覧の行番号（例: 債権者 2）が要ります。{USAGE}"
MSG_SYNTAX_MUNI_NAME = f"「役所」の後に市区町村名（例: 役所 川口市）が要ります。{USAGE}"
MSG_SYNTAX_FACE_NO = f"「面」の後に面番号（例: 面 7）が要ります。{USAGE}"
MSG_SYNTAX_PRINTED_ARG = f"「印刷済」の後は面番号だけ指定できます（例: ラベル 印刷済 面 4）。{USAGE}"
MSG_UNSUPPORTED_ROLE = "相手方・裁判所は住所欄がないため未対応です（宛先は 依頼者・債権者・役所・発送 のみ）"
MSG_DB_UNSET = "残量 DB（DATABASE_URL）が未設定のためラベル印字は使えません（管理者に確認してください）"
MSG_SHEET_FULL = "シートが満杯です（提案できる空き面がありません）。「ラベル 新しいシート」を送ってください"
MSG_NO_PENDING_FACE = "印刷待ちの面がありません（添付後に「ラベル 印刷済」を送ってください）"
MSG_NO_UNDO = "取り消せる消費がありません"
MSG_PRINTED_HINT = "印刷したら『ラベル 印刷済』と送ってください"
MSG_DUPLICATE_EVENT = "処理済みです（同じメッセージの再配送）"
MSG_FACE_TAKEN = "面が埋まりました。再指示してください"
MSG_MUNI_NOT_FOUND = "市区町村マスタ（App 31）に該当する有効な市区町村がありません（市区町村名の表記を確認してください）"
MSG_MUNI_NO_ADDRESS = "市区町村マスタ（App 31）の該当レコードに住所が未登録です"
MSG_ATTACH_CONFLICT = "案件レコードの更新が競合し続けたため添付できませんでした。もう一度同じ指示を送ると回収します"
QUESTION_UNIT = "どのアプリの案件ですか？「時効」「相続」「放棄」のいずれかで答えてください"
_STAGE_UNIT = "unit"


class LabelSyntaxError(ValueError):
    """指示の構文誤り（返信文＝固定文言・PII なし）"""


class LabelResolveError(ValueError):
    """宛先の解決失敗（返信文＝レコード番号・役割のみ・PII なし）"""


@dataclass(frozen=True)
class LabelRequest:
    kind: str                     # print | printed | undo | new_sheet | status
    unit: str | None = None       # 時効 | 相続 | 放棄
    case_no: str | None = None    # 案件レコード番号
    role: str | None = None       # 依頼者 | 債権者 | 役所 | 発送 | 相手方 | 裁判所
    role_arg: str | None = None   # 債権者=行番号 / 役所=市区町村名（入力） / 発送=App 30 番号
    face: int | None = None
    honorific: str | None = None  # 末尾の 様/御中（上書き）


@dataclass(frozen=True)
class Target:
    """解決済みの宛先。name/zip_code/address は PDF にだけ載る（復唱・ログへは出さない）。
    role_label/role_key は復唱・冪等・ファイル名用（役所は App 31 の正規名＝入力原文を使わない）"""
    name: str
    zip_code: str
    address: str
    honorific: str
    case_app: kintone.KintoneApp
    case_no: str
    unit_label: str
    role_label: str
    role_key: str


# ── 構文解析（決定論・PII なし） ─────────────────────────────────────────────

_NO_RE = re.compile(r"(?:[Nn][Oo]\.?|№)\s*(\d+)")


def _normalize(text: str) -> str:
    # NFKC: 全角英数字・全角空白・№ を半角へ（日本語はそのまま）
    t = unicodedata.normalize("NFKC", (text or "").strip()).replace("・", " ")
    # 「債権者2」「面4」「発送No.120」「案件No.17」「役所川口市」の連結を分かつ
    t = re.sub(r"(債権者|面|発送|案件|役所)(?=\S)", r"\1 ", t)
    t = re.sub(r"([Nn][Oo]\.?|№)\s+(\d+)", r"No.\2", t)
    return re.sub(r"\s+", " ", t).strip()


def is_label_instruction(text: str) -> bool:
    """「ラベル」で始まる指示のみ本タスク（registry.direct_match_fn）"""
    t = (text or "").strip().replace("　", " ")
    return t == "ラベル" or t.startswith("ラベル ") or t.startswith("ラベル・")


def parse_label_instruction(text: str) -> LabelRequest:
    """指示文 → LabelRequest。構文誤りは LabelSyntaxError（本文は固定文言＋書式案内のみ）。"""
    tokens = _normalize(text).split(" ")
    if not tokens or tokens[0] != "ラベル":
        raise LabelSyntaxError(MSG_SYNTAX_START)
    rest = tokens[1:]
    if not rest:
        raise LabelSyntaxError(USAGE)
    head = rest[0]
    if head in ("印刷済", "印刷済み"):
        return LabelRequest(kind="printed", face=_parse_face(rest[1:]))
    if head in ("戻す", "戻し"):
        return LabelRequest(kind="undo")
    if head in ("新しいシート", "新シート"):
        return LabelRequest(kind="new_sheet")
    if head in ("残量", "残り"):
        return LabelRequest(kind="status")

    unit = case_no = role = role_arg = honorific = None
    face = None
    i = 0
    while i < len(rest):
        t = rest[i]
        m = _NO_RE.fullmatch(t)
        if t in _UNIT_ALIASES:
            unit = _UNIT_ALIASES[t]
        elif t == "発送":
            role = "発送"
            if i + 1 < len(rest) and _NO_RE.fullmatch(rest[i + 1]):
                role_arg = _NO_RE.fullmatch(rest[i + 1]).group(1)
                i += 1
            else:
                raise LabelSyntaxError(MSG_SYNTAX_SHIPPING_NO)
        elif t == "案件":
            if i + 1 < len(rest) and _NO_RE.fullmatch(rest[i + 1]):
                case_no = _NO_RE.fullmatch(rest[i + 1]).group(1)
                i += 1
            else:
                raise LabelSyntaxError(MSG_SYNTAX_CASE_NO)
        elif m:
            case_no = m.group(1)
        elif t in ("依頼者",) + ROLES_UNSUPPORTED:
            role = t
        elif t == "債権者":
            role = t
            if i + 1 < len(rest) and rest[i + 1].isdigit():
                role_arg = str(int(rest[i + 1]))
                i += 1
            else:
                raise LabelSyntaxError(MSG_SYNTAX_CREDITOR_NO)
        elif t == "役所":
            role = t
            if i + 1 < len(rest) and rest[i + 1] not in ("様", "御中", "面"):
                role_arg = rest[i + 1]
                i += 1
            else:
                raise LabelSyntaxError(MSG_SYNTAX_MUNI_NAME)
        elif t == "面":
            if i + 1 < len(rest) and rest[i + 1].isdigit():
                face = int(rest[i + 1])
                i += 1
            else:
                raise LabelSyntaxError(MSG_SYNTAX_FACE_NO)
        elif t in ("様", "御中"):
            honorific = t
        else:
            raise LabelSyntaxError(MSG_SYNTAX_UNKNOWN_WORD)
        i += 1

    if role is None:
        raise LabelSyntaxError(MSG_SYNTAX_NO_ROLE)
    if role in ROLES_UNSUPPORTED:
        raise LabelSyntaxError(MSG_UNSUPPORTED_ROLE)
    if case_no is None:
        raise LabelSyntaxError(MSG_SYNTAX_NO_CASE_FOR_SHIPPING if role == "発送" else MSG_SYNTAX_NO_CASE)
    return LabelRequest(kind="print", unit=unit, case_no=case_no, role=role,
                        role_arg=role_arg, face=face, honorific=honorific)


def _parse_face(tokens: list[str]) -> int | None:
    if not tokens:
        return None
    if tokens[0] == "面" and len(tokens) > 1 and tokens[1].isdigit():
        return int(tokens[1])
    if tokens[0].isdigit():
        return int(tokens[0])
    raise LabelSyntaxError(MSG_SYNTAX_PRINTED_ARG)


# ── 宛先解決（kintone 読取のみ・返す例外文に PII なし） ────────────────────────

def _v(rec: dict, code: str) -> str:
    return str(((rec.get(code) or {}).get("value")) or "").strip()


async def _get_case(unit: str, case_no: str) -> tuple[dict, kintone.KintoneApp, str, str]:
    unit_label, app, name_field = UNITS[unit]
    try:
        rec = await kintone.get_record(app, case_no)
    except kintone.KintoneError as e:
        if e.status == 404 or e.code in ("GAIA_RE01",):
            raise LabelResolveError(f"No.{case_no}（{unit_label}）のレコードが見つかりません")
        raise
    return rec, app, name_field, unit_label


async def resolve_unit(req: LabelRequest) -> str | None:
    """単位語が無い「発送」指示は App 30 のユニット種別から補う。決められなければ None。"""
    if req.unit:
        return req.unit
    if req.role == "発送":
        rec = await kintone.get_record(APP_SHIPPING, req.role_arg)
        return _UNIT_FROM_SHIPPING.get(_v(rec, "ユニット種別"))
    return None


def _escape(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace('"', '\\"')


async def resolve_target(req: LabelRequest) -> Target:
    """宛先 4 種の解決（D4/D6）。失敗は LabelResolveError（レコード番号・役割のみ）。"""
    if req.unit not in UNITS:
        raise LabelResolveError(QUESTION_UNIT)
    rec, app, name_field, unit_label = await _get_case(req.unit, req.case_no)
    honorific = req.honorific if req.honorific is not None else DEFAULT_HONORIFIC[req.role]

    if req.role == "依頼者":
        name, zip_code, addr = _v(rec, name_field), _v(rec, "郵便番号"), _v(rec, "住所")
        if not name or not addr:
            raise LabelResolveError(f"No.{req.case_no}（{unit_label}）の依頼者の氏名または住所が空です")
        return Target(name, zip_code, addr, honorific, app, req.case_no, unit_label, "依頼者", "依頼者")

    if req.role == "債権者":
        if req.unit != "放棄":
            raise LabelResolveError(f"{unit_label}の債権者は住所欄がないため未対応です（債権者は 放棄 のみ）")
        rows = list(((rec.get("債権者一覧") or {}).get("value")) or [])
        idx = int(req.role_arg)
        if not 1 <= idx <= len(rows):
            raise LabelResolveError(f"No.{req.case_no} の債権者一覧に {idx} 行目がありません（{len(rows)} 行）")
        row = rows[idx - 1].get("value") or {}
        name, zip_code, addr = _v(row, "債権者名"), _v(row, "債権者郵便番号"), _v(row, "債権者住所")
        if not name or not addr:
            raise LabelResolveError(f"No.{req.case_no} の債権者 {idx} の名称または住所が空です")
        return Target(name, zip_code, addr, honorific, app, req.case_no, unit_label,
                      f"債権者 {idx}", f"債権者{idx}")

    if req.role == "役所":
        munis = await kintone.search_records(
            APP_CITY_MASTER, f'市区町村名 = "{_escape(req.role_arg)}" and 有効 in ("yes")')
        if not munis:
            raise LabelResolveError(MSG_MUNI_NOT_FOUND)
        muni = munis[0]
        addr = _v(muni, "住所")
        if not addr:
            raise LabelResolveError(MSG_MUNI_NO_ADDRESS)
        canonical = _v(muni, "市区町村名")            # 復唱・キーは App 31 の正規名（入力原文は使わない）
        office = municipality_office_name(canonical)
        dept = _v(muni, "担当部署")
        name = f"{office}　{dept}" if dept else office
        return Target(name, _v(muni, "郵便番号"), addr, honorific, app, req.case_no, unit_label,
                      f"役所 {canonical}", f"役所{canonical}")

    if req.role == "発送":
        ship = await kintone.get_record(APP_SHIPPING, req.role_arg)
        name, zip_code, addr = _v(ship, "宛先名"), _v(ship, "宛先郵便番号"), _v(ship, "宛先住所")
        if not name or not addr:
            raise LabelResolveError(f"発送 No.{req.role_arg} の宛先名または宛先住所が空です")
        return Target(name, zip_code, addr, honorific, app, req.case_no, unit_label,
                      f"発送 No.{req.role_arg}", f"発送No{req.role_arg}")

    raise LabelResolveError(MSG_UNSUPPORTED_ROLE)


# ── フロー（handler の flow_fn / flow_reply_fn フック） ───────────────────────

def _req_from(params: dict) -> LabelRequest:
    keys = LabelRequest.__dataclass_fields__.keys()
    return LabelRequest(**{k: params.get(k) for k in keys if k in params})


def _event_id() -> str:
    from dispatch_bot import handler  # 遅延 import（循環回避）
    return handler.current_event_id.get() or "local:unknown"


async def _sheet_op(req: LabelRequest, user_id: str) -> str:
    """印刷済／戻す／新しいシート／残量（人の操作＝即時・復唱なし・event_id 冪等）"""
    per_page = layout_spec(LAYOUT).per_page
    event_id = _event_id()
    if req.kind == "printed":
        outcome, state, consumed = await label_sheet.consume_printed(event_id, user_id, req.face)
        if outcome == "duplicate_event":
            return MSG_DUPLICATE_EVENT
        if not consumed:
            if req.face is not None and state is not None:
                return f"面 {req.face} は印刷待ちではありません。{label_sheet.status_text(state, per_page)}"
            return MSG_NO_PENDING_FACE
        faces = "・".join(str(f) for f in consumed)
        logger.info("[LABEL] printed faces=%s", emit(len(consumed), "count", "log", "operator"))
        return f"面 {faces} を使用済みにしました。{label_sheet.status_text(state, per_page)}"
    if req.kind == "undo":
        outcome, state, undone = await label_sheet.undo_last(event_id, user_id)
        if outcome == "duplicate_event":
            return MSG_DUPLICATE_EVENT
        if not undone:
            return MSG_NO_UNDO
        faces = "・".join(str(f) for f in undone)
        return f"直前の消費（面 {faces}）を取り消しました。{label_sheet.status_text(state, per_page)}"
    if req.kind == "new_sheet":
        outcome, state = await label_sheet.new_sheet(event_id, user_id)
        if outcome == "duplicate_event":
            return MSG_DUPLICATE_EVENT
        return f"新しいシート {state.sheet_id} を開始しました。{label_sheet.status_text(state, per_page)}"
    state = await label_sheet.ensure_state(user_id)
    return label_sheet.status_text(state, per_page)


def _confirmation_text(target: Target, state: label_sheet.SheetState, face: int, per_page: int,
                       honorific_override: str | None, same_job: label_sheet.Job | None) -> str:
    """復唱（D8: レコード番号・役割・面番号・残量のみ。氏名・住所は載せない）"""
    remaining = state.remaining(per_page) - (1 if face in state.free_faces(per_page) else 0)
    lines = [f"No.{target.case_no}（{target.unit_label}）・{target.role_label}・面 {face}/{per_page} に印字。"
             f"残り {remaining} 面。OK?"]
    notes = []
    if not target.zip_code:
        notes.append("郵便番号なし（〒 なしで印字）")
    if same_job is not None:
        if same_job.status == label_sheet.JOB_ATTACHED:
            notes.append(f"面 {face} は同じ宛先で添付済み（再添付はしません）")
        else:
            notes.append(f"面 {face} は同じ宛先で添付未完了（OK で回収します）")
    elif face in state.used:
        notes.append(f"面 {face} は使用済み（再印字）")
    if honorific_override is not None:
        notes.append(f"敬称: {honorific_override}")
    if notes:
        lines.append("※" + "／".join(notes))
    lines.append("OK / キャンセル（30分有効）")
    return "\n".join(lines)


async def _confirm(user_id: str, parsed: dict, base_text: str, req: LabelRequest) -> str:
    from dispatch_bot import confirm, handler  # 遅延 import（循環回避）
    handler._sessions.pop(user_id, None)
    try:
        target = await resolve_target(req)
        state = await label_sheet.ensure_state(user_id)
    except LabelResolveError as e:
        return str(e)
    except DatabaseNotConfigured:
        return MSG_DB_UNSET
    per_page = layout_spec(LAYOUT).per_page
    target_key = (target.case_app.app_id(), target.case_no, target.role_key)
    face = req.face
    same_job = None
    same_active = [j for j in state.active if j.target == target_key]
    if face is None:
        if same_active:
            # 同じ宛先がこのシートで面を占有中（添付済み or 添付未完）→ その面を提案（回収／再添付なし・LP-02）
            same_job = same_active[0]
            face = same_job.face
        else:
            face = state.next_free(per_page)
            if face is None:
                return MSG_SHEET_FULL
    elif not 1 <= face <= per_page:
        return f"面番号は 1〜{per_page} です"
    else:
        on_face = state.jobs_on(face)
        same = [j for j in on_face if j.target == target_key]
        if same:
            same_job = same[0]
        elif on_face:
            return f"面 {face} は別の宛先で印刷待ちです。別の面を指定するか「ラベル 印刷済」の後に指示してください"
    params = {**asdict(req), "face": face, "sheet_id": state.sheet_id,
              "unit_label": target.unit_label, "role_label": target.role_label,
              "role_key": target.role_key, "explicit": req.face is not None}
    new_parsed = {**parsed, "task_type": TASK_TYPE, "customer_name": None,
                  "task_params": params}
    # CaseHit の氏名は空にする（復唱・エラー通知に氏名を出さない・RV-10）
    hit = CaseHit(record_id=req.case_no, customer_name="", status="", unit=target.unit_label)
    confirm.create(user_id, new_parsed, hit, base_text)
    return _confirmation_text(target, state, face, per_page, req.honorific, same_job)


def _label_line(base_text: str) -> str:
    """聞き返しの結合文（旧指示 + 「（追加回答）…」）でも、最後の「ラベル」行だけを構文解析する"""
    lines = [ln.replace("（追加回答）", "").strip() for ln in (base_text or "").split("\n")]
    return next((ln for ln in reversed(lines) if is_label_instruction(ln)), base_text)


async def flow(user_id: str, parsed: dict, base_text: str, session) -> str:
    """タスク固有フロー本体（handler の flow_fn フック）。構文 → シート操作 or 復唱。"""
    from dispatch_bot import handler, registry  # 遅延 import（循環回避）
    handler._sessions.pop(user_id, None)
    text = _label_line(base_text)
    try:
        req = parse_label_instruction(text)
    except LabelSyntaxError as e:
        # LP-05: ログは長さのみ（入力原文・トークンを出さない）
        logger.info("[LABEL] syntax error len=%s", emit(len(text), "count", "log", "operator"))
        return str(e)
    if req.kind != "print":
        try:
            return await _sheet_op(req, user_id)
        except DatabaseNotConfigured:
            return MSG_DB_UNSET
    try:
        unit = await resolve_unit(req)
    except kintone.KintoneError as e:
        if req.role == "発送" and e.status == 404:
            return f"発送 No.{req.role_arg} のレコードが見つかりません"
        raise
    if unit is None:
        spec = registry.get_task(TASK_TYPE)
        return handler._ask(user_id, base_text, "label_unit", QUESTION_UNIT, session, spec,
                            parsed=parsed, flow=TASK_TYPE,
                            flow_state={"stage": _STAGE_UNIT, "req": asdict(req)})
    if unit != req.unit:
        req = LabelRequest(**{**asdict(req), "unit": unit})
    return await _confirm(user_id, parsed, base_text, req)


async def flow_reply(user_id: str, text: str, session) -> tuple[bool, str]:
    """flow が張ったセッションへの応答（単位語の聞き返し）。"""
    state = session.flow_state or {}
    if state.get("stage") != _STAGE_UNIT:
        return False, ""
    t = (text or "").strip()
    if _CANCEL_WORDS.fullmatch(t):
        return False, ""  # キャンセル語は通常解析（intent=cancel）へ
    unit = _UNIT_ALIASES.get(t)
    if unit is None:
        return True, QUESTION_UNIT
    req = LabelRequest(**{**state["req"], "unit": unit})
    return True, await _confirm(user_id, session.parsed or {}, session.base_text, req)


# ── 実行（handler の execute_fn フック） ─────────────────────────────────────

def build_filename(sheet_id: str, face: int, role_key: str, now: datetime | None = None) -> str:
    day = (now or datetime.now(timezone.utc)).astimezone(_JST).strftime("%Y%m%d")
    return f"宛名ラベル_{day}_{sheet_id}_面{face:02d}_{role_key}.pdf"


class AttachConflict(RuntimeError):
    """$revision CAS が規定回数競合し続けた（添付未完・ジョブは reserved のまま）"""


async def attach_with_cas(app: kintone.KintoneApp, record_id: str, file_key: str) -> None:
    """案件レコードの 宛名ラベル に fileKey を追記（LP-03）: 最新 $revision で CAS PUT。
    409 は再取得して既存添付（同時に増えた分を含む）に自分の fileKey をマージして再試行（最大 3 回）。"""
    for _ in range(ATTACH_CAS_ATTEMPTS):
        rec = await kintone.get_record(app, record_id)
        existing = [{"fileKey": f.get("fileKey")}
                    for f in (((rec.get(ATTACH_FIELD) or {}).get("value")) or [])
                    if f.get("fileKey")]
        try:
            await kintone.update_record(app, record_id,
                                        {ATTACH_FIELD: existing + [{"fileKey": file_key}]},
                                        revision=_v(rec, "$revision") or None)
            return
        except kintone.KintoneConflict:
            continue
    raise AttachConflict()


async def execute(pending) -> tuple[str, str, str]:
    """OK 後（LP-01〜04）: 宛先を再解決 → 1 トランザクションで面を予約（event_id 冪等・行ロック・
    提案面が埋まっていれば再指示）→ PDF → アップロード（fileKey を保存）→ CAS 添付 → attached。
    添付失敗は reserved のまま残し、同一宛先・同一面の別イベントで回収する。全終端で pending invalidate。"""
    from dispatch_bot import confirm  # 遅延 import（循環回避）
    user_id = getattr(pending, "user_id", "")
    try:
        params = (pending.parsed or {}).get("task_params") or {}
        req = _req_from(params)
        face = int(params["face"])
        per_page = layout_spec(LAYOUT).per_page
        try:
            target = await resolve_target(req)
        except LabelResolveError as e:
            return str(e), "", ""
        app_id = target.case_app.app_id()
        try:
            res = await label_sheet.reserve_face(
                _event_id(), app=app_id, record=req.case_no, role=target.role_key,
                sheet_id=str(params.get("sheet_id") or ""), face=face,
                explicit=bool(params.get("explicit")), user_id=user_id)
        except DatabaseNotConfigured:
            return MSG_DB_UNSET, "", ""
        if res.outcome == "duplicate_event":
            return MSG_DUPLICATE_EVENT, req.case_no, ""
        if res.outcome == "sheet_changed":
            return (f"シートが {params.get('sheet_id')} から変わりました。もう一度指示してください", "", "")
        if res.outcome == "face_taken":
            return MSG_FACE_TAKEN, "", ""
        job = res.job
        if res.outcome == "attached":
            return (f"添付済みです（No.{req.case_no}・{target.role_label}・面 {face}・{job.filename}）。"
                    f"{MSG_PRINTED_HINT}", req.case_no, "")

        # ── トランザクション外: PDF → アップロード → CAS 添付（回収時は保存済み fileKey を再利用）──
        recovering = res.outcome == "recover"
        file_key, filename = job.file_key, job.filename
        if not file_key:
            pdf = render_label_sheet(
                [{"宛先名": target.name, "郵便番号": target.zip_code, "住所": target.address,
                  "敬称": target.honorific}],
                layout=LAYOUT, faces=[face])
            filename = build_filename(job.sheet_id, face, target.role_key)
            file_key = await kintone.upload_file(target.case_app, filename, pdf, "application/pdf")
            await label_sheet.set_job_file_key(job.job_id, file_key, filename)
        try:
            await attach_with_cas(target.case_app, req.case_no, file_key)
        except AttachConflict:
            logger.info("[LABEL] attach conflict record=%s face=%s",
                        emit(req.case_no, "record_id", "log", "operator"),
                        emit(face, "count", "log", "operator"))
            return MSG_ATTACH_CONFLICT, "", ""
        except kintone.KintoneError:
            if recovering:
                await label_sheet.clear_job_file_key(job.job_id)   # 期限切れ等: 次の回収で再アップロード
            raise
        await label_sheet.mark_job_attached(job.job_id, file_key, filename)
        state = await label_sheet.ensure_state(user_id)
        logger.info("[LABEL] attached record=%s face=%s",
                    emit(req.case_no, "record_id", "log", "operator"),
                    emit(face, "count", "log", "operator"))
        head = "回収して添付しました" if recovering else "添付しました"
        return (f"{head}（No.{req.case_no}・{target.role_label}・面 {face}/{per_page}・{filename}）。"
                f"{MSG_PRINTED_HINT}\n{label_sheet.status_text(state, per_page)}", req.case_no, "")
    finally:
        confirm.invalidate(user_id)   # 全終端で必ず実行（P3-003-CMD 裁定8 の型）
