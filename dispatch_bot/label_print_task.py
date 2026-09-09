"""Bot語彙: 宛名ラベルの印字（LABEL-PRINT-1 第 2 段・隔離 module）

正本: LABEL-PRINT-1 票（裁定 D1〜D9）。設計サーベイ Desktop\\claude\\ラベル印字_設計.md。

- 入口は LINE 指示ボットのみ（D3）。指示は「ラベル」で始まる決定論の構文で解析する
  （Claude 解析を経ない＝registry.direct_match_fn。モデルの推測で宛先を決めない）。
- 宛先 4 種（D4）: 依頼者（App 21/26/40）・債権者 n（App 40 債権者一覧 n 行目）・
  役所 名（App 31・M1 の解決関数を流用）・発送 No（App 30 の宛先欄）。相手方・裁判所は
  「住所欄がないため未対応」と返す。郵便番号が無い宛先は 〒 なしで印字し復唱に付記。
- 敬称の既定（D6）: 依頼者=様・債権者=御中・役所=御中・発送=宛先名そのまま。指示末尾の
  「様」「御中」で上書き。
- 生成 PDF は案件レコードの FILE 欄「宛名ラベル」へ添付（既存添付は残す・D5）。
  役所・発送宛は指示に併記した案件番号のレコードへ添付する。
- 残量は durable DB（hub/label_sheet・D2）。機械は次の空き面を提案するだけ。面を消費
  するのは人の「ラベル 印刷済」のみ。「戻す」「新しいシート」「残量」も同構文。
- 冪等キー label_print:{app}:{record}:{role}:{sheet}:{face}（同キー再要求は添付しない）。
- **RV-10（D8）**: 復唱・返信・通知・ログに氏名/住所を載せない。載せるのはレコード番号・
  宛先の役割・面番号・シート残量のみ。氏名・住所は PDF にだけ載る。
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
MSG_UNSUPPORTED_ROLE = "相手方・裁判所は住所欄がないため未対応です（宛先は 依頼者・債権者・役所・発送 のみ）"
MSG_DB_UNSET = "残量 DB（DATABASE_URL）が未設定のためラベル印字は使えません（管理者に確認してください）"
MSG_SHEET_FULL = "シートが満杯です（提案できる空き面がありません）。「ラベル 新しいシート」を送ってください"
MSG_NO_PENDING_FACE = "印刷待ちの面がありません（添付後に「ラベル 印刷済」を送ってください）"
MSG_NO_UNDO = "取り消せる消費がありません"
MSG_PRINTED_HINT = "印刷したら『ラベル 印刷済』と送ってください"
QUESTION_UNIT = "どのアプリの案件ですか？「時効」「相続」「放棄」のいずれかで答えてください"
_STAGE_UNIT = "unit"


class LabelSyntaxError(ValueError):
    """指示の構文誤り（返信文＝PII なし）"""


class LabelResolveError(ValueError):
    """宛先の解決失敗（返信文＝レコード番号・役割のみ・PII なし）"""


@dataclass(frozen=True)
class LabelRequest:
    kind: str                     # print | printed | undo | new_sheet | status
    unit: str | None = None       # 時効 | 相続 | 放棄
    case_no: str | None = None    # 案件レコード番号
    role: str | None = None       # 依頼者 | 債権者 | 役所 | 発送 | 相手方 | 裁判所
    role_arg: str | None = None   # 債権者=行番号 / 役所=市区町村名 / 発送=App 30 番号
    face: int | None = None
    honorific: str | None = None  # 末尾の 様/御中（上書き）

    @property
    def role_label(self) -> str:
        if self.role == "債権者":
            return f"債権者 {self.role_arg}"
        if self.role == "役所":
            return f"役所 {self.role_arg}"
        if self.role == "発送":
            return f"発送 No.{self.role_arg}"
        return self.role or ""

    @property
    def role_key(self) -> str:
        """冪等キー・ファイル名用（PII なし）"""
        if self.role == "債権者":
            return f"債権者{self.role_arg}"
        if self.role == "役所":
            return f"役所{self.role_arg}"
        if self.role == "発送":
            return f"発送No{self.role_arg}"
        return self.role or ""


@dataclass(frozen=True)
class Target:
    """解決済みの宛先（PDF にだけ載る。復唱・ログへは出さない）"""
    name: str
    zip_code: str
    address: str
    honorific: str
    case_app: kintone.KintoneApp
    case_no: str
    unit_label: str


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
    """指示文 → LabelRequest。構文誤りは LabelSyntaxError（本文は書式案内のみ）。"""
    tokens = _normalize(text).split(" ")
    if not tokens or tokens[0] != "ラベル":
        raise LabelSyntaxError(f"「ラベル」で始めてください。{USAGE}")
    rest = tokens[1:]
    if not rest:
        raise LabelSyntaxError(USAGE)
    head = rest[0]
    if head in ("印刷済", "印刷済み"):
        face = _parse_face(rest[1:])
        return LabelRequest(kind="printed", face=face)
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
                raise LabelSyntaxError(f"「発送」の後に App 30 の番号（例: No.120）が要ります。{USAGE}")
        elif t == "案件":
            if i + 1 < len(rest) and _NO_RE.fullmatch(rest[i + 1]):
                case_no = _NO_RE.fullmatch(rest[i + 1]).group(1)
                i += 1
            else:
                raise LabelSyntaxError(f"「案件」の後に案件番号（例: No.17）が要ります。{USAGE}")
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
                raise LabelSyntaxError(f"「債権者」の後に債権者一覧の行番号（例: 債権者 2）が要ります。{USAGE}")
        elif t == "役所":
            role = t
            if i + 1 < len(rest) and rest[i + 1] not in ("様", "御中", "面"):
                role_arg = rest[i + 1]
                i += 1
            else:
                raise LabelSyntaxError(f"「役所」の後に市区町村名（例: 役所 川口市）が要ります。{USAGE}")
        elif t == "面":
            if i + 1 < len(rest) and rest[i + 1].isdigit():
                face = int(rest[i + 1])
                i += 1
            else:
                raise LabelSyntaxError(f"「面」の後に面番号（例: 面 7）が要ります。{USAGE}")
        elif t in ("様", "御中"):
            honorific = t
        else:
            raise LabelSyntaxError(f"解釈できない語「{t}」があります。{USAGE}")
        i += 1

    if role is None:
        raise LabelSyntaxError(f"宛先の種類（依頼者・債権者 n・役所 名・発送 No）が要ります。{USAGE}")
    if role in ROLES_UNSUPPORTED:
        raise LabelSyntaxError(MSG_UNSUPPORTED_ROLE)
    if case_no is None:
        if role == "発送":
            raise LabelSyntaxError(f"添付先の案件番号を併記してください（例: ラベル 発送 No.120 案件 No.17）。{USAGE}")
        raise LabelSyntaxError(f"案件番号（例: No.17）が要ります。{USAGE}")
    return LabelRequest(kind="print", unit=unit, case_no=case_no, role=role,
                        role_arg=role_arg, face=face, honorific=honorific)


def _parse_face(tokens: list[str]) -> int | None:
    if not tokens:
        return None
    if tokens[0] == "面" and len(tokens) > 1 and tokens[1].isdigit():
        return int(tokens[1])
    if tokens[0].isdigit():
        return int(tokens[0])
    raise LabelSyntaxError(f"「印刷済」の後は面番号だけ指定できます（例: ラベル 印刷済 面 4）。{USAGE}")


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
        return Target(name, zip_code, addr, honorific, app, req.case_no, unit_label)

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
        return Target(name, zip_code, addr, honorific, app, req.case_no, unit_label)

    if req.role == "役所":
        munis = await kintone.search_records(
            APP_CITY_MASTER, f'市区町村名 = "{req.role_arg}" and 有効 in ("yes")')
        if not munis:
            raise LabelResolveError(f"市区町村マスタ（App 31）に「{req.role_arg}」の有効なレコードがありません")
        muni = munis[0]
        addr = _v(muni, "住所")
        if not addr:
            raise LabelResolveError(f"市区町村マスタ（App 31）の「{req.role_arg}」に住所が未登録です")
        office = municipality_office_name(_v(muni, "市区町村名"))
        dept = _v(muni, "担当部署")
        name = f"{office}　{dept}" if dept else office
        return Target(name, _v(muni, "郵便番号"), addr, honorific, app, req.case_no, unit_label)

    if req.role == "発送":
        ship = await kintone.get_record(APP_SHIPPING, req.role_arg)
        name, zip_code, addr = _v(ship, "宛先名"), _v(ship, "宛先郵便番号"), _v(ship, "宛先住所")
        if not name or not addr:
            raise LabelResolveError(f"発送 No.{req.role_arg} の宛先名または宛先住所が空です")
        return Target(name, zip_code, addr, honorific, app, req.case_no, unit_label)

    raise LabelResolveError(MSG_UNSUPPORTED_ROLE)


# ── フロー（handler の flow_fn / flow_reply_fn フック） ───────────────────────

def _req_from(params: dict) -> LabelRequest:
    keys = LabelRequest.__dataclass_fields__.keys()
    return LabelRequest(**{k: params.get(k) for k in keys if k in params})


async def _sheet_op(req: LabelRequest, user_id: str) -> str:
    """印刷済／戻す／新しいシート／残量（人の操作＝即時・復唱なし）"""
    per_page = layout_spec(LAYOUT).per_page
    if req.kind == "printed":
        state, consumed = await label_sheet.consume_printed(user_id, req.face)
        if not consumed:
            if req.face is not None and state is not None:
                return f"面 {req.face} は印刷待ちではありません。{label_sheet.status_text(state, per_page)}"
            return MSG_NO_PENDING_FACE
        faces = "・".join(str(f) for f in consumed)
        logger.info("[LABEL] printed faces=%s", emit(len(consumed), "count", "log", "operator"))
        return f"面 {faces} を使用済みにしました。{label_sheet.status_text(state, per_page)}"
    if req.kind == "undo":
        state, undone = await label_sheet.undo_last(user_id)
        if not undone:
            return MSG_NO_UNDO
        faces = "・".join(str(f) for f in undone)
        return f"直前の消費（面 {faces}）を取り消しました。{label_sheet.status_text(state, per_page)}"
    if req.kind == "new_sheet":
        state = await label_sheet.new_sheet(user_id)
        return f"新しいシート {state.sheet_id} を開始しました。{label_sheet.status_text(state, per_page)}"
    state = await label_sheet.ensure_state(user_id)
    return label_sheet.status_text(state, per_page)


def _confirmation_text(req: LabelRequest, target: Target, state: label_sheet.SheetState,
                       face: int, per_page: int) -> str:
    """復唱（D8: レコード番号・役割・面番号・残量のみ。氏名・住所は載せない）"""
    remaining = state.remaining(per_page) - (1 if face in state.free_faces(per_page) else 0)
    lines = [f"No.{req.case_no}（{target.unit_label}）・{req.role_label}・面 {face}/{per_page} に印字。"
             f"残り {remaining} 面。OK?"]
    notes = []
    if not target.zip_code:
        notes.append("郵便番号なし（〒 なしで印字）")
    if face in state.used:
        notes.append(f"面 {face} は使用済み（再印字）")
    elif face in state.pending:
        notes.append(f"面 {face} は印刷待ち（再印字）")
    if req.honorific is not None:
        notes.append(f"敬称: {req.honorific}")
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
    face = req.face
    if face is None:
        face = state.next_free(per_page)
        if face is None:
            return MSG_SHEET_FULL
    elif not 1 <= face <= per_page:
        return f"面番号は 1〜{per_page} です"
    params = {**asdict(req), "face": face, "sheet_id": state.sheet_id,
              "unit_label": target.unit_label}
    new_parsed = {**parsed, "task_type": TASK_TYPE, "customer_name": None,
                  "task_params": params}
    # CaseHit の氏名は空にする（復唱・エラー通知に氏名を出さない・RV-10）
    hit = CaseHit(record_id=req.case_no, customer_name="", status="", unit=target.unit_label)
    confirm.create(user_id, new_parsed, hit, base_text)
    return _confirmation_text(req, target, state, face, per_page)


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

def idempotency_key(app_id: str, record_id: str, role_key: str, sheet_id: str, face: int) -> str:
    return f"label_print:{app_id}:{record_id}:{role_key}:{sheet_id}:{face}"


def build_filename(sheet_id: str, face: int, role_key: str, now: datetime | None = None) -> str:
    day = (now or datetime.now(timezone.utc)).astimezone(_JST).strftime("%Y%m%d")
    return f"宛名ラベル_{day}_{sheet_id}_面{face:02d}_{role_key}.pdf"


async def execute(pending) -> tuple[str, str, str]:
    """OK 後: 宛先を再解決 → faces=[面] で 1 頁 PDF → 案件レコード 宛名ラベル へ添付
    （既存添付は残す）→ DB に印刷待ち面と冪等キーを記録。全終端で pending invalidate。"""
    from dispatch_bot import confirm  # 遅延 import（循環回避）
    user_id = getattr(pending, "user_id", "")
    try:
        params = (pending.parsed or {}).get("task_params") or {}
        req = _req_from(params)
        face = int(params["face"])
        try:
            target = await resolve_target(req)
            state = await label_sheet.ensure_state(user_id)
        except LabelResolveError as e:
            return str(e), "", ""
        except DatabaseNotConfigured:
            return MSG_DB_UNSET, "", ""
        if state.sheet_id != params.get("sheet_id"):
            return (f"シートが {params.get('sheet_id')} から {state.sheet_id} に変わりました。"
                    "もう一度指示してください", "", "")
        key = idempotency_key(target.case_app.app_id(), req.case_no, req.role_key,
                              state.sheet_id, face)
        job = await label_sheet.find_job(key)
        if job:
            return (f"添付済みです（No.{req.case_no}・{req.role_label}・面 {face}・"
                    f"{job['filename']}）。{MSG_PRINTED_HINT}", req.case_no, "")
        pdf = render_label_sheet(
            [{"宛先名": target.name, "郵便番号": target.zip_code, "住所": target.address,
              "敬称": target.honorific}],
            layout=LAYOUT, faces=[face])
        filename = build_filename(state.sheet_id, face, req.role_key)
        rec = await kintone.get_record(target.case_app, req.case_no)
        existing = [{"fileKey": f.get("fileKey")}
                    for f in (((rec.get(ATTACH_FIELD) or {}).get("value")) or [])
                    if f.get("fileKey")]
        file_key = await kintone.upload_file(target.case_app, filename, pdf, "application/pdf")
        await kintone.update_record(target.case_app, req.case_no,
                                    {ATTACH_FIELD: existing + [{"fileKey": file_key}]})
        await label_sheet.record_job(key, sheet_id=state.sheet_id, face=face,
                                     filename=filename, file_key=file_key, user_id=user_id)
        state = await label_sheet.mark_pending(face, user_id)
        per_page = layout_spec(LAYOUT).per_page
        logger.info("[LABEL] attached record=%s face=%s",
                    emit(req.case_no, "record_id", "log", "operator"),
                    emit(face, "count", "log", "operator"))
        return (f"添付しました（No.{req.case_no}・{req.role_label}・面 {face}/{per_page}・"
                f"{filename}）。{MSG_PRINTED_HINT}\n"
                f"{label_sheet.status_text(state, per_page)}", req.case_no, "")
    finally:
        confirm.invalidate(user_id)   # 全終端で必ず実行（P3-003-CMD 裁定8 の型）
