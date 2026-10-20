"""souzoku_assets — 財産目録の構造化入力・分割案（割付）・法定相続分との差異（SOUZOKU-4・QUEUE-2026-10-17-H H-5）

正本: docs/souzoku-1-design.md §4。
- 財産は種別ごとの構造（不動産: 所在／地番／家屋番号／持分、預貯金: 金融機関／支店／種別／**口座番号は下 4 桁のみ**、有価証券、その他）。
  口座番号の全桁は保存しない（入力を受けても下 4 桁に落としてから保存＝RV-10）。生年月日の欄は持たない。
- 割付（どの財産を誰が取得・代償金）は人の入力。取得者は souzoku_heirs の最新版（確定済みを優先）の相続人ラベル（配偶者・子1 …）。
- 行は削除せず status=void（RV-08）。
- 差異表示: 各相続人の取得額（財産の評価額＋受取代償金−支払代償金）と 法定相続分×総額 の差（評価未確定があれば参考値）。
"""
import datetime
import json
import logging
import re
import uuid
from fractions import Fraction

import sqlalchemy as sa

from hub import souzoku_heirs as H
from hub.db import session_scope
from hub.redact import emit

logger = logging.getLogger("hub.souzoku_assets")

metadata = sa.MetaData()
TABLE_NAME = "souzoku_assets"
DOC_TABLE_NAME = "souzoku_kyogisho"
TABLE_NAMES = (TABLE_NAME, DOC_TABLE_NAME)
BUSINESS = "souzoku"
KIND_REAL_ESTATE = "real_estate"
KIND_DEPOSIT = "deposit"
KIND_SECURITIES = "securities"
KIND_OTHER = "other"
KINDS = (KIND_REAL_ESTATE, KIND_DEPOSIT, KIND_SECURITIES, KIND_OTHER)
KIND_LABELS = {KIND_REAL_ESTATE: "不動産", KIND_DEPOSIT: "預貯金", KIND_SECURITIES: "有価証券", KIND_OTHER: "その他"}
# 種別ごとの欄（閉集合・生年月日や口座全桁の欄は無い）
FIELDS = {
    KIND_REAL_ESTATE: (("location", "所在"), ("lot_number", "地番"), ("house_number", "家屋番号"), ("share", "持分")),
    KIND_DEPOSIT: (("bank", "金融機関"), ("branch", "支店"), ("account_type", "種別"), ("account_last4", "口座番号（下 4 桁）")),
    KIND_SECURITIES: (("broker", "証券会社"), ("issue", "銘柄"), ("quantity", "数量")),
    KIND_OTHER: (("name", "名称"), ("detail", "内容")),
}
STATUS_ACTIVE = "active"
STATUS_VOID = "void"
STATUSES = (STATUS_ACTIVE, STATUS_VOID)
MAX_FIELD_CHARS = 120
MAX_VALUE_YEN = 10 ** 12
_DIGITS_RE = re.compile(r"\d")
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

souzoku_assets = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("asset_id", sa.Text, primary_key=True),
    sa.Column("intake_id", sa.Text, nullable=False),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("detail_json", sa.Text, nullable=False),          # 種別ごとの欄（預貯金は account_last4 のみ）
    sa.Column("value_yen", _BIG, nullable=True),                # 評価額（未確定は NULL）
    sa.Column("acquirer_label", sa.Text, nullable=True),        # 取得者（相続人ラベル・未割付は NULL）
    sa.Column("compensation_json", sa.Text, nullable=True),     # {"to": label, "yen": int, "due": "YYYY-MM-DD"}（代償金・無ければ NULL）
    sa.Column("status", sa.Text, nullable=False, server_default=STATUS_ACTIVE),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("voided_at", sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("kind IN ('real_estate', 'deposit', 'securities', 'other')", name="ck_souzoku_assets_kind"),
    sa.CheckConstraint("status IN ('active', 'void')", name="ck_souzoku_assets_status"),
    sa.Index("ix_souzoku_assets_intake", "intake_id", "created_at"),
)

souzoku_kyogisho = sa.Table(
    DOC_TABLE_NAME, metadata,
    sa.Column("doc_id", sa.Text, primary_key=True),
    sa.Column("intake_id", sa.Text, nullable=False),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("version", sa.Integer, nullable=False),
    sa.Column("sha256", sa.Text, nullable=False),
    sa.Column("size_bytes", sa.Integer, nullable=False),
    sa.Column("content", sa.LargeBinary, nullable=False),       # 生成した docx（App には添付しない・画面からダウンロード）
    sa.Column("texts_version", sa.Text, nullable=False),        # souzoku_texts の pin
    sa.Column("template_sha256", sa.Text, nullable=False),
    sa.Column("created_by", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint("intake_id", "version", name="uq_souzoku_kyogisho_version"),
    sa.Index("ix_souzoku_kyogisho_intake", "intake_id", "created_at"),
)


class InvalidAsset(ValueError):
    pass


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def account_last4(raw) -> str:
    """口座番号は下 4 桁だけ（数字以外は捨てる・4 桁未満はそのまま・全桁は保存しない）。"""
    digits = "".join(_DIGITS_RE.findall(str(raw or "")))
    return digits[-4:]


def normalize_detail(kind: str, raw: dict) -> dict:
    if kind not in KINDS:
        raise InvalidAsset("kind")
    out = {}
    for key, _label in FIELDS[kind]:
        v = str((raw or {}).get(key) or "").strip()
        if len(v) > MAX_FIELD_CHARS:
            raise InvalidAsset("too_long")
        out[key] = v
    if kind == KIND_DEPOSIT:
        # 全桁が来ても下 4 桁に落とす（account_number という欄名で来ても同じ）
        out["account_last4"] = account_last4((raw or {}).get("account_number") or out.get("account_last4"))
    if not any(out.values()):
        raise InvalidAsset("empty")
    return out


def normalize_value(raw) -> int | None:
    s = str(raw or "").replace(",", "").strip()
    if not s:
        return None
    if not s.isdigit() or int(s) > MAX_VALUE_YEN:
        raise InvalidAsset("value")
    return int(s)


def describe(kind: str, detail: dict) -> str:
    """協議書・画面用の財産の表示（1 行・口座は下 4 桁のみ）。"""
    label = KIND_LABELS.get(kind, kind)
    parts = [f"{flabel} {detail.get(key, '')}" for key, flabel in FIELDS.get(kind, ()) if detail.get(key)]
    if kind == KIND_DEPOSIT and detail.get("account_last4"):
        parts = [p for p in parts if not p.startswith("口座番号")] + [f"口座番号 下4桁 {detail['account_last4']}"]
    return f"{label}　" + "　".join(parts)


# ── CRUD（削除なし） ─────────────────────────────────────────────────────────────
def _row(r) -> dict:
    d = dict(r._mapping)
    d["detail"] = json.loads(d.pop("detail_json") or "{}")
    comp = d.pop("compensation_json", None)
    d["compensation"] = json.loads(comp) if comp else None
    return d


async def add_asset(intake_id: str, kind: str, raw_detail: dict, value_yen=None, *, now=None) -> str:
    detail = normalize_detail(kind, raw_detail)
    value = normalize_value(value_yen)
    now = now or _now()
    aid = uuid.uuid4().hex
    async with session_scope() as s:
        await s.execute(sa.insert(souzoku_assets).values(
            asset_id=aid, intake_id=str(intake_id), business=BUSINESS, kind=kind, detail_json=json.dumps(detail, ensure_ascii=False),
            value_yen=value, status=STATUS_ACTIVE, created_at=now, updated_at=now))
    logger.info("[SOUZOKU_ASSETS] added kind=%s", emit(kind, "freetext", "log", "operator"))
    return aid


async def list_assets(intake_id: str, *, include_void: bool = False) -> list[dict]:
    q = sa.select(souzoku_assets).where(souzoku_assets.c.intake_id == str(intake_id))
    if not include_void:
        q = q.where(souzoku_assets.c.status == STATUS_ACTIVE)
    async with session_scope() as s:
        rows = (await s.execute(q.order_by(souzoku_assets.c.created_at.asc(), souzoku_assets.c.asset_id.asc()))).fetchall()
    return [_row(r) for r in rows]


async def void_asset(intake_id: str, asset_id: str, *, now=None) -> bool:
    now = now or _now()
    async with session_scope() as s:
        r = await s.execute(sa.update(souzoku_assets).where(
            souzoku_assets.c.asset_id == str(asset_id), souzoku_assets.c.intake_id == str(intake_id),
            souzoku_assets.c.status == STATUS_ACTIVE).values(status=STATUS_VOID, voided_at=now, updated_at=now))
        return int(r.rowcount or 0) == 1


async def allocate(intake_id: str, asset_id: str, acquirer_label: str | None, compensation: dict | None, *, valid_labels, now=None) -> bool:
    """割付: 取得者（相続人ラベル）と代償金（{"to", "yen", "due"}・任意）。ラベルは判定結果の閉集合のみ。"""
    now = now or _now()
    acq = (acquirer_label or "").strip() or None
    if acq is not None and acq not in valid_labels:
        raise InvalidAsset("acquirer")
    comp_json = None
    if compensation and (compensation.get("to") or compensation.get("yen")):
        to = str(compensation.get("to") or "").strip()
        yen = normalize_value(compensation.get("yen"))
        if to not in valid_labels or not yen or acq is None or to == acq:
            raise InvalidAsset("compensation")
        due = str(compensation.get("due") or "").strip()
        if due and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", due):
            raise InvalidAsset("due")
        comp_json = json.dumps({"to": to, "yen": yen, "due": due}, ensure_ascii=False)
    async with session_scope() as s:
        r = await s.execute(sa.update(souzoku_assets).where(
            souzoku_assets.c.asset_id == str(asset_id), souzoku_assets.c.intake_id == str(intake_id),
            souzoku_assets.c.status == STATUS_ACTIVE).values(acquirer_label=acq, compensation_json=comp_json, updated_at=now))
        return int(r.rowcount or 0) == 1


# ── 法定相続分との差異（純関数） ──────────────────────────────────────────────────
def heir_labels(heirs_result: dict | None) -> list[str]:
    return [h["label"] for h in (heirs_result or {}).get("heirs", [])]


def diff_table(heirs_result: dict | None, assets: list[dict]) -> dict:
    """{"rows": [{"label","share","expected_yen","acquired_yen","diff_yen"}], "total_yen": int, "unvalued": int, "unallocated": int}"""
    heirs = (heirs_result or {}).get("heirs", [])
    active = [a for a in assets if a.get("status", STATUS_ACTIVE) == STATUS_ACTIVE]
    unvalued = sum(1 for a in active if a.get("value_yen") is None)
    unallocated = sum(1 for a in active if not a.get("acquirer_label"))
    total = sum(int(a["value_yen"]) for a in active if a.get("value_yen") is not None)
    acquired = {h["label"]: 0 for h in heirs}
    for a in active:
        acq = a.get("acquirer_label")
        if acq in acquired and a.get("value_yen") is not None:
            acquired[acq] += int(a["value_yen"])
        comp = a.get("compensation") or {}
        if comp and acq in acquired and comp.get("to") in acquired:
            acquired[acq] -= int(comp["yen"])
            acquired[comp["to"]] += int(comp["yen"])
    rows = []
    for h in heirs:
        share = Fraction(h["share"])
        expected = int(round(total * share))
        rows.append({"label": h["label"], "share": h["share"], "expected_yen": expected,
                     "acquired_yen": acquired[h["label"]], "diff_yen": acquired[h["label"]] - expected})
    return {"rows": rows, "total_yen": total, "unvalued": unvalued, "unallocated": unallocated}
