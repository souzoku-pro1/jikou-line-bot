"""相続放棄 委任契約書の生成（HOUKI-CONTRACT-GEN・houki 固有部分）

contract_webhook.py（時効版）を ChannelConfig 化した上で、相続放棄（App 40）向けの
差し込み・申述人の束ね・費用計算・署名者決定を本モジュールが担う。状態機械・CAS・
CloudSign 呼出し・凍結検証の枠組みは contract_webhook の共通経路（cfg 経由）。

申述人の束ね（1 レコード=1 申述人）:
  起点レコード（webhook が来たレコード）を代表者とし、被相続人グループID が非空なら
  同じ ID を持つ全レコード（起点を含む）を申述人集合とする。空なら 1 名。
  並び順: 代表者 → レコード番号昇順。集合は送信直前にも再取得し、変化していれば
  要確認（TOCTOU）。

費用（弁護士凍結事項・定数は本モジュールの 1 か所のみ・テストで pin）:
  報酬合計 = FEE_BASE + FEE_ADDITIONAL × (n − 1)
  実費合計 = DEPOSIT_PER_APPLICANT × n
  送付先数 = 全員の 債権者一覧 の正規化後和集合の件数（空行は数えない）
  追加送付件数 = max(0, 送付先数 − INCLUDED_DESTINATIONS)
  追加送付料合計 = 追加送付件数 × EXTRA_SEND_FEE
  支払総額 = 報酬合計 + 追加送付料合計 + 実費合計

特約と契約署名:
  全員: 特約 = 欄そのまま（空なら「特になし」）・署名者 = 全員のメールアドレス
  代表者のみ／空（初期値扱い・空は通知に明記）: 申述人 2 名以上なら
  REPRESENTATIVE_CLAUSE（代表者氏名を差し込み）+ 改行 + 欄（空なら定型文のみ）・
  1 名なら定型文なし（欄が空なら「特になし」）。署名者 = 代表者のみ。
  TOKUYAKU_MAX_CHARS の判定は欄（人が書いた部分）のみ（contract_webhook の
  tokuyaku_problem が担う・定型文は含めない）。

書き込み: 起点レコードのみ（委任契約書・契約書ステータス・cloudsign_document_id）。
他の申述人レコードは読むだけ。値（氏名・住所・メール）はログ・通知に載せない。
"""

import copy
import datetime
import hashlib
import io
import re
import unicodedata
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from docx import Document
from docx.text.paragraph import Paragraph

from hub import kintone
from hub.docx_builder import fill_template
from hub.houki_case_store import APP_HOUKI_CASE, CREDITOR_TABLE

# ── 雛形（人承認済み現物・HOUKI-CONTRACT-FINAL-DOCX の確定 v1） ──────────────
TEMPLATE_PATH = "docx_templates/houki/委任契約書.docx"
TEMPLATE_SHA256 = "519d4bb6765a266514f808e37c8d5f40533d752a694f5989276c3ac4e68d85c9"
# 雛形の全段落テキスト（差し込み記号を含む 62 段落・"\n" 連結）の sha256。
# 雛形の文言が 1 字でも変わればテストが落ちる（凍結条項の段落単位 pin）
TEMPLATE_PARAGRAPHS_SHA256 = (
    "9f15e22b915ed1f845880e76d844d6af0ea339fcfbbc7149c50c49826d25b2c3")
TEMPLATE_PARAGRAPH_COUNT = 62
OUTPUT_FILENAME = "委任契約書_相続放棄.docx"
OUTPUT_PDF_NAME = "委任契約書_相続放棄.pdf"

# 生成物の実行時凍結検証（第2条 見出し+1 項＝報酬条項・雛形逐語）
FROZEN_CLAUSE = (
    "第2条（費用および支払方法）",
    "1　本件の弁護士報酬は、申述人1名の場合は金88,000円とし、2名目以降は1名につき"
    "金33,000円を加算する。受理通知書の写しの送付は送付先3か所までを含み、4か所目以降は"
    "送付先1か所につき金1,100円を加算する。送付先の数は通知を送る相手先ごとに数え、"
    "申述人が複数であっても同じ相手先は1か所と数える。いずれも消費税込みとする。",
)

PLACEHOLDERS = (
    "{{被相続人氏名}}", "{{申述人数}}", "{{報酬合計}}", "{{追加送付件数}}",
    "{{追加送付料合計}}", "{{実費合計}}", "{{支払総額}}", "{{特約}}",
    "{{契約年}}", "{{契約月}}", "{{契約日}}", "{{申述人一覧}}",
)
APPLICANTS_KEY = "{{申述人一覧}}"
TOKUYAKU_KEY = "{{特約}}"

# ── 弁護士凍結事項（費用・定型文） ───────────────────────────────────────────
FEE_BASE = 88_000                 # 申述人 1 名目（税込）
FEE_ADDITIONAL = 33_000           # 2 名目以降 1 名につき（税込）
DEPOSIT_PER_APPLICANT = 5_000     # 実費預託金 1 名につき
EXTRA_SEND_FEE = 1_100            # 受理通知書の追加送付 1 か所につき（税込）
INCLUDED_DESTINATIONS = 3         # 報酬に含む送付先の数
REPRESENTATIVE_CLAUSE = (
    "甲らは、申述人{name}を代表者と定め、代表者が甲ら全員のために本契約に電子署名する。")
TOKUYAKU_NONE = "特になし"

# ── App 40 欄 ────────────────────────────────────────────────────────────────
FIELD_GROUP = "被相続人グループID"
FIELD_SIGN_MODE = "契約署名"
SIGN_ALL = "全員"
SIGN_REPRESENTATIVE = "代表者のみ"
FIELD_NAME = "顧客名"
FIELD_ADDR = "住所"
FIELD_DECEDENT = "被相続人氏名"
FIELD_EMAIL = "メールアドレス"
FIELD_TOKUYAKU = "特約"
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
JST = ZoneInfo("Asia/Tokyo")


class HoukiContractIntegrityError(RuntimeError):
    """雛形/生成物の凍結検証に失敗（生成しない）。"""


def _fv(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _rid(record: dict) -> str:
    return _fv(record, "$id")


# ── 費用 ─────────────────────────────────────────────────────────────────────
def normalize_creditor(name) -> str:
    """送付先名の正規化: 前後空白除去・NFKC（全角/半角の英数記号統一）・
    全角/半角スペース除去。空文字は「空行」= 数えない。"""
    s = unicodedata.normalize("NFKC", str(name or ""))
    return re.sub(r"[\s　]+", "", s).strip()


def destinations(records: list[dict]) -> set[str]:
    """申述人集合全員の 債権者一覧 の正規化後和集合。"""
    out: set[str] = set()
    for rec in records:
        rows = ((rec.get(CREDITOR_TABLE) or {}).get("value") or [])
        for row in rows:
            raw = ((row.get("value") or {}).get("債権者名") or {}).get("value")
            key = normalize_creditor(raw)
            if key:
                out.add(key)
    return out


def compute_fees(n: int, destination_count: int) -> dict:
    """税込・整数円。キーは差し込み名と同じ語。"""
    extra_count = max(0, destination_count - INCLUDED_DESTINATIONS)
    fee = FEE_BASE + FEE_ADDITIONAL * (n - 1)
    deposit = DEPOSIT_PER_APPLICANT * n
    extra_fee = extra_count * EXTRA_SEND_FEE
    return {
        "申述人数": n, "送付先数": destination_count,
        "報酬合計": fee, "実費合計": deposit,
        "追加送付件数": extra_count, "追加送付料合計": extra_fee,
        "支払総額": fee + extra_fee + deposit,
    }


def fmt_yen(n: int) -> str:
    return f"{int(n):,}"


# ── 申述人集合 ────────────────────────────────────────────────────────────────
def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


async def gather_applicants(record: dict) -> tuple[list[dict], str]:
    """起点レコードを代表者として申述人集合を返す（代表者 → レコード番号昇順）。
    被相続人グループID が空なら [起点] のみ。戻り値 (集合, グループID)。"""
    group = _fv(record, FIELD_GROUP)
    rep_id = _rid(record)
    if not group:
        return [record], ""
    rows = await kintone.search_records(
        APP_HOUKI_CASE, f'{FIELD_GROUP} = "{_escape(group)}" order by $id asc limit 500')
    others = sorted((r for r in rows if _rid(r) != rep_id), key=lambda r: int(_rid(r) or 0))
    return [record] + others, group


def applicant_problems(records: list[dict]) -> list[str]:
    """要確認理由（固定語彙+レコード番号のみ・値は載せない）。"""
    problems: list[str] = []
    decedents = {_fv(r, FIELD_DECEDENT) for r in records}
    if len(decedents) != 1 or "" in decedents:
        problems.append("被相続人氏名が申述人集合で一致しない（または空）")
    for r in records:
        if not _fv(r, FIELD_NAME):
            problems.append(f"顧客名 未入力（レコード番号 {_rid(r)}）")
        if not _fv(r, FIELD_ADDR):
            problems.append(f"住所 未入力（レコード番号 {_rid(r)}）")
    return problems


def sign_mode(record: dict) -> tuple[str, bool]:
    """(署名方式, 欄が空だったか)。空は 代表者のみ として扱う。"""
    raw = _fv(record, FIELD_SIGN_MODE)
    if raw == SIGN_ALL:
        return SIGN_ALL, False
    return SIGN_REPRESENTATIVE, raw == ""


def signers(records: list[dict], mode: str) -> list[dict]:
    return list(records) if mode == SIGN_ALL else [records[0]]


def participants(records: list[dict], mode: str) -> tuple[list[tuple[str, str]], list[str]]:
    """CloudSign 宛先 [(email, 顧客名)] と要確認理由。"""
    out: list[tuple[str, str]] = []
    problems: list[str] = []
    for r in signers(records, mode):
        email = _fv(r, FIELD_EMAIL)
        if not email:
            problems.append(f"メールアドレス 未入力（レコード番号 {_rid(r)}）")
        elif not _EMAIL_RE.fullmatch(email):
            problems.append(f"メールアドレス 形式不正（レコード番号 {_rid(r)}）")
        else:
            out.append((email, _fv(r, FIELD_NAME)))
    return out, problems


def compose_tokuyaku(records: list[dict], mode: str) -> str:
    """特約本文（雛形の {{特約}} へ apply_tokuyaku で差し込む文字列・改行=段落）。"""
    rep = records[0]
    user = str((rep.get(FIELD_TOKUYAKU) or {}).get("value") or "").strip()
    if mode == SIGN_ALL or len(records) < 2:
        return user or TOKUYAKU_NONE
    clause = REPRESENTATIVE_CLAUSE.format(name=_fv(rep, FIELD_NAME))
    return clause + ("\n" + user if user else "")


# ── 差し込み ──────────────────────────────────────────────────────────────────
def build_fill_data(records: list[dict], fees: dict,
                    now: datetime.datetime | None = None) -> dict:
    """fill_template 用 10 キー（{{特約}} と {{申述人一覧}} は専用処理のため含まない）。"""
    now = now or datetime.datetime.now(JST)
    rep = records[0]
    return {
        "{{被相続人氏名}}": _fv(rep, FIELD_DECEDENT),
        "{{申述人数}}": str(fees["申述人数"]),
        "{{報酬合計}}": fmt_yen(fees["報酬合計"]),
        "{{追加送付件数}}": str(fees["追加送付件数"]),
        "{{追加送付料合計}}": fmt_yen(fees["追加送付料合計"]),
        "{{実費合計}}": fmt_yen(fees["実費合計"]),
        "{{支払総額}}": fmt_yen(fees["支払総額"]),
        "{{契約年}}": str(now.year), "{{契約月}}": str(now.month), "{{契約日}}": str(now.day),
    }


def _set_text(para, text: str) -> None:
    para.runs[0].text = text
    for r in para.runs[1:]:
        r.text = ""


def expand_applicants(docx_bytes: bytes, records: list[dict]) -> bytes:
    """{{申述人一覧}} の段落を申述人ごとに「住所　…」「氏名　…」の 2 段落へ展開
    （段落書式は元段落を複製・2 名以上は人と人の間に空段落 1 つ・表は使わない）。"""
    doc = Document(io.BytesIO(docx_bytes))
    anchor = next((p for p in doc.paragraphs if p.text == APPLICANTS_KEY), None)
    if anchor is None:
        raise HoukiContractIntegrityError("applicants placeholder missing")
    template_p = copy.deepcopy(anchor._p)
    lines: list[str] = []
    for i, rec in enumerate(records):
        if i:
            lines.append("")
        lines.append(f"住所　{_fv(rec, FIELD_ADDR)}")
        lines.append(f"氏名　{_fv(rec, FIELD_NAME)}")
    _set_text(anchor, lines[0])
    prev = anchor._p
    for line in lines[1:]:
        new_p = copy.deepcopy(template_p)
        prev.addnext(new_p)
        _set_text(Paragraph(new_p, anchor._parent), line)
        prev = new_p
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()


def verify_template_integrity() -> None:
    data = open(TEMPLATE_PATH, "rb").read()
    if hashlib.sha256(data).hexdigest() != TEMPLATE_SHA256:
        raise HoukiContractIntegrityError("template hash mismatch")


def template_paragraphs_sha256(path: str = TEMPLATE_PATH) -> str:
    texts = [p.text for p in Document(path).paragraphs]
    return hashlib.sha256("\n".join(texts).encode("utf-8")).hexdigest()


def render(records: list[dict], mode: str, fees: dict,
           now: datetime.datetime | None = None) -> bytes:
    """雛形の整合検証 → 10 キー差し込み → 特約（定型文込み）→ 申述人一覧の展開 →
    凍結条項検証（第2条）。docx 添付と CloudSign 用 PDF の両方がこの単一経路。"""
    from contract_webhook import apply_tokuyaku, verify_frozen_clause
    verify_template_integrity()
    docx_bytes = fill_template(TEMPLATE_PATH, build_fill_data(records, fees, now))
    docx_bytes = apply_tokuyaku(docx_bytes, compose_tokuyaku(records, mode))
    docx_bytes = expand_applicants(docx_bytes, records)
    verify_frozen_clause(docx_bytes, FROZEN_CLAUSE)
    return docx_bytes


# ── 準備（contract_webhook の共通経路が呼ぶ） ───────────────────────────────
@dataclass
class HoukiPlan:
    records: list[dict]
    group: str
    mode: str
    mode_blank: bool
    fees: dict
    applicant_problems: list[str]
    email_problems: list[str]
    participants: list[tuple[str, str]]

    @property
    def fingerprint(self) -> str:
        parts = [self.group, self.mode]
        for r in self.records:
            parts.append("|".join((_rid(r), _fv(r, FIELD_NAME), _fv(r, FIELD_ADDR),
                                   _fv(r, FIELD_EMAIL), _fv(r, FIELD_DECEDENT))))
        parts.append(",".join(sorted(destinations(self.records))))
        return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

    def summary(self) -> str:
        f = self.fees
        mode = "全員" if self.mode == SIGN_ALL else "代表者のみ"
        if self.mode_blank:
            mode += "（契約署名 が空のため代表者のみとして作成）"
        lines = [f"・申述人数: {f['申述人数']}名",
                 f"・送付先数: {f['送付先数']}か所（追加送付 {f['追加送付件数']}か所）",
                 f"・支払総額: {fmt_yen(f['支払総額'])}円",
                 f"・署名方式: {mode}"]
        if not self.group:
            lines.append("・被相続人グループID 空・1 名として作成")
        return "\n".join(lines)


async def plan(record: dict) -> HoukiPlan:
    records, group = await gather_applicants(record)
    mode, blank = sign_mode(record)
    fees = compute_fees(len(records), len(destinations(records)))
    problems = applicant_problems(records)
    parts, email_problems = participants(records, mode)
    return HoukiPlan(records, group, mode, blank, fees, problems, email_problems, parts)
