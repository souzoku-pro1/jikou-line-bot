"""souzoku_heirs — 相続人の確定（第 2 段階）: 構造化した聞取りの保存と法定相続人・法定相続分の判定（SOUZOKU-3・H-4）

正本: docs/souzoku-1-design.md §2。判定表は hub/souzoku_law_constants.py（要大野確認）だけを参照する。
- 入力は **人数と状態の構造**（氏名を持たない＝RV-10。相続人の氏名は通知・ログ・本表に出さない）。
- 判定結果は draft として版を重ね（RV-08: 上書き・削除なし）、承認は人の明示操作（status=confirmed）のみ。
- 未対応の組合せ（数次相続・胎児・相続分の指定 等）は「要弁護士判断」を返す（例外にしない）。
- flag SOUZOKU_INTAKE_ENABLED に従う（画面側で OFF=404）。
"""
import datetime
import json
import logging
import uuid
from fractions import Fraction

import sqlalchemy as sa

from hub import souzoku_law_constants as L
from hub.db import session_scope
from hub.redact import emit

logger = logging.getLogger("hub.souzoku_heirs")

metadata = sa.MetaData()
TABLE_NAME = "souzoku_heirs"
HISTORY_TABLE_NAME = "souzoku_heirs_history"
TABLE_NAMES = (TABLE_NAME, HISTORY_TABLE_NAME)
BUSINESS = "souzoku"
STATUS_DRAFT = "draft"
STATUS_CONFIRMED = "confirmed"
STATUSES = (STATUS_DRAFT, STATUS_CONFIRMED)
RESULT_DETERMINED = "determined"
RESULT_NEEDS_ATTORNEY = "needs_attorney"
RESULT_NO_HEIRS = "no_heirs"
RESULTS = (RESULT_DETERMINED, RESULT_NEEDS_ATTORNEY, RESULT_NO_HEIRS)
ACTION_CREATED = "created"
ACTION_CONFIRMED = "confirmed"
ACTIONS = (ACTION_CREATED, ACTION_CONFIRMED)
ACTOR_HUMAN = "human"

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

souzoku_heirs = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("heirs_id", sa.Text, primary_key=True),
    sa.Column("intake_id", sa.Text, nullable=False),                 # souzoku_intake.intake_id（案件 ID）
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("version", sa.Integer, nullable=False),                # 案件内の版（1, 2, …・上書きしない）
    sa.Column("input_json", sa.Text, nullable=False),                # 構造化した聞取り（氏名なし）
    sa.Column("result_json", sa.Text, nullable=False),               # 判定結果（相続人・分数・根拠条文・注記）
    sa.Column("result", sa.Text, nullable=False),                    # determined / needs_attorney / no_heirs
    sa.Column("status", sa.Text, nullable=False, server_default=STATUS_DRAFT),
    sa.Column("law_table_version", sa.Text, nullable=False),         # 判定表の版（souzoku_law_constants の sha256 先頭 16）
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("confirmed_by", sa.Text, nullable=True),
    sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("status IN ('draft', 'confirmed')", name="ck_souzoku_heirs_status"),
    sa.CheckConstraint("result IN ('determined', 'needs_attorney', 'no_heirs')", name="ck_souzoku_heirs_result"),
    sa.UniqueConstraint("intake_id", "version", name="uq_souzoku_heirs_version"),
    sa.Index("ix_souzoku_heirs_intake", "intake_id", "created_at"),
)

souzoku_heirs_history = sa.Table(
    HISTORY_TABLE_NAME, metadata,
    sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("heirs_id", sa.Text, sa.ForeignKey("souzoku_heirs.heirs_id"), nullable=False),
    sa.Column("action", sa.Text, nullable=False),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("action IN ('created', 'confirmed')", name="ck_souzoku_heirs_history_action"),
    sa.Index("ix_souzoku_heirs_history_h", "heirs_id"),
)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def law_table_version() -> str:
    """判定表の版（constants の数値・条文・閉集合を正規化した sha256 の先頭 16）。"""
    import hashlib
    payload = {
        "shares": {k: [str(a), str(b)] for k, (a, b) in L.SHARES_WITH_SPOUSE.items()},
        "half": str(L.HALF_BLOOD_RATIO), "sib_depth": L.SIBLING_REPRESENTATION_DEPTH, "articles": L.ARTICLES,
        "unsupported": L.UNSUPPORTED, "rank": list(L.RANK_ORDER),
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:16]


# ── 入力の検証（閉集合・人数上限・氏名を持たない） ───────────────────────────────
class InvalidInput(ValueError):
    pass


def _status(v, allowed=L.STATUSES, default=L.STATUS_NONE) -> str:
    v = str(v or default)
    if v not in allowed:
        raise InvalidInput("status")
    return v


def _int(v, lo=0, hi=L.MAX_DESCENDANTS_PER_ROW) -> int:
    try:
        n = int(v or 0)
    except (TypeError, ValueError):
        raise InvalidInput("int") from None
    if n < lo or n > hi:
        raise InvalidInput("range")
    return n


def normalize_input(raw: dict) -> dict:
    """画面／API の生入力を閉集合へ正規化する（不正は InvalidInput）。戻り値の形:
    {"spouse": status, "children": [{"kind", "status", "descendants": int, "descendants_deceased": int}],
     "ascendants": {"father": status, "mother": status, "grandparents_alive": int},
     "siblings": [{"blood", "status", "children": int, "children_deceased": int}],
     "will": will, "special": {"successive", "fetus", "designated_shares", "renunciation_pending": bool}}"""
    raw = raw or {}
    out = {"spouse": _status(raw.get("spouse"), (L.STATUS_ALIVE, L.STATUS_NONE, L.STATUS_DECEASED, L.STATUS_RENOUNCED))}
    children = []
    for c in list(raw.get("children") or [])[:L.MAX_CHILD_ROWS]:
        st = _status(c.get("status"))
        if st == L.STATUS_NONE:
            continue
        kind = str(c.get("kind") or L.KIND_REAL)
        if kind not in L.CHILD_KINDS:
            raise InvalidInput("kind")
        children.append({"kind": kind, "status": st, "descendants": _int(c.get("descendants")),
                         "descendants_deceased": _int(c.get("descendants_deceased"))})
    out["children"] = children
    asc = raw.get("ascendants") or {}
    out["ascendants"] = {"father": _status(asc.get("father")), "mother": _status(asc.get("mother")),
                         "grandparents_alive": _int(asc.get("grandparents_alive"), 0, 4)}
    siblings = []
    for s in list(raw.get("siblings") or [])[:L.MAX_SIBLING_ROWS]:
        st = _status(s.get("status"))
        if st == L.STATUS_NONE:
            continue
        blood = str(s.get("blood") or L.BLOOD_FULL)
        if blood not in L.BLOODS:
            raise InvalidInput("blood")
        siblings.append({"blood": blood, "status": st, "children": _int(s.get("children")),
                         "children_deceased": _int(s.get("children_deceased"))})
    out["siblings"] = siblings
    will = str(raw.get("will") or L.WILL_NONE)
    if will not in L.WILLS:
        raise InvalidInput("will")
    out["will"] = will
    sp = raw.get("special") or {}
    out["special"] = {k: bool(sp.get(k)) for k in ("successive", "fetus", "designated_shares", "renunciation_pending")}
    return out


# ── 判定エンジン（純関数・判定表だけを参照） ──────────────────────────────────────
def _frac(f: Fraction) -> str:
    return f"{f.numerator}/{f.denominator}" if f.denominator != 1 else str(f.numerator)


def _children_line(children: list) -> tuple[list, list]:
    """子の群: (相続人の単位の列 [(label, weight, basis, note)], 注記)。weight は群内の按分の重み（子 1 人＝1・代襲者は按分）。"""
    units, notes = [], []
    for i, c in enumerate(children, start=1):
        label = f"子{i}" + ("（養子）" if c["kind"] == L.KIND_ADOPTED else "")
        basis = [L.ARTICLES["children"]] + ([L.ARTICLES["adopted"]] if c["kind"] == L.KIND_ADOPTED else [])
        if c["status"] == L.STATUS_ALIVE:
            units.append((label, Fraction(1), basis, ""))
        elif c["status"] == L.STATUS_RENOUNCED:
            notes.append(f"{label}: 相続放棄（初めから相続人でなく代襲も生じない・{L.ARTICLES['renounced']}）")
        elif c["status"] in L.REPRESENTATION_CAUSES:
            cause = {L.STATUS_DECEASED: "死亡", L.STATUS_DISQUALIFIED: "欠格", L.STATUS_DISINHERITED: "廃除"}[c["status"]]
            n = c["descendants"]
            if n <= 0:
                notes.append(f"{label}: {cause}・代襲者なし")
                continue
            # 代襲者（孫）の間で子 1 人分を均等に分ける（901 条）。孫がさらに死亡していれば再代襲（887 条 3 項）＝人数不明のため要確認
            per = Fraction(1, n)
            for j in range(1, n + 1):
                units.append((f"代襲（{label}）{j}", per, [L.ARTICLES["representation_children"], L.ARTICLES["per_stirpes"]],
                              f"{label} の{cause}による代襲"))
            if c["descendants_deceased"] > 0:
                notes.append(f"{label}: 代襲者のうち {c['descendants_deceased']} 人が死亡＝再代襲（887 条 3 項）の人数を確認")
    return units, notes


def _ascendants_line(asc: dict) -> tuple[list, list]:
    units, notes = [], []
    parents = [("父", asc["father"]), ("母", asc["mother"])]
    alive = [(lbl, st) for lbl, st in parents if st == L.STATUS_ALIVE]
    for lbl, st in parents:
        if st == L.STATUS_RENOUNCED:
            notes.append(f"{lbl}: 相続放棄（{L.ARTICLES['renounced']}）")
    if alive:
        for lbl, _ in alive:
            units.append((lbl, Fraction(1), [L.ARTICLES["ascendants"]], ""))
        return units, notes
    n = asc["grandparents_alive"]
    for j in range(1, n + 1):
        units.append((f"祖父母{j}", Fraction(1), [L.ARTICLES["ascendants"]], "父母がいないため親等の次に近い直系尊属"))
    return units, notes


def _siblings_line(siblings: list) -> tuple[list, list, bool]:
    """戻り値 (units, notes, needs_attorney)。半血は重み 1/2（900 条 4 号ただし書）。代襲は一代（甥姪）まで。"""
    units, notes, needs = [], [], False
    for i, s in enumerate(siblings, start=1):
        half = s["blood"] == L.BLOOD_HALF
        label = f"兄弟姉妹{i}" + ("（半血）" if half else "")
        w = L.HALF_BLOOD_RATIO if half else Fraction(1)
        basis = [L.ARTICLES["siblings"]] + ([L.ARTICLES["half_blood"]] if half else [])
        if s["status"] == L.STATUS_ALIVE:
            units.append((label, w, basis, ""))
        elif s["status"] == L.STATUS_RENOUNCED:
            notes.append(f"{label}: 相続放棄（代襲も生じない・{L.ARTICLES['renounced']}）")
        elif s["status"] in L.REPRESENTATION_CAUSES:
            n = s["children"]
            if n <= 0:
                notes.append(f"{label}: 代襲者（甥姪）なし")
                continue
            per = w / n
            for j in range(1, n + 1):
                units.append((f"代襲（{label}）{j}", per, [L.ARTICLES["representation_siblings"], L.ARTICLES["per_stirpes"]] + basis[1:],
                              f"{label} の代襲（甥姪・一代限り）"))
            if s["children_deceased"] > 0:
                needs = True
                notes.append(f"{label}: 甥姪のうち {s['children_deceased']} 人が死亡＝{L.UNSUPPORTED['sibling_representation_depth']}")
    return units, notes, needs


def determine(inp: dict) -> dict:
    """法定相続人と法定相続分。戻り値:
    {"result": determined|needs_attorney|no_heirs, "heirs": [{"label","line","share","basis","note"}],
     "notes": [...], "attorney_reasons": [...], "law_table_version": str}"""
    inp = normalize_input(inp)
    reasons = [L.UNSUPPORTED[k] for k in ("successive", "fetus", "designated_shares", "renunciation_pending") if inp["special"].get(k)]
    notes: list = []
    if inp["will"] in (L.WILL_NOTARIAL, L.WILL_HOLOGRAPH):
        notes.append(L.WILL_NOTE)
    elif inp["will"] == L.WILL_UNKNOWN:
        notes.append("遺言の有無が不明（公正証書遺言の検索・自筆証書の保管確認）")
    spouse = inp["spouse"] == L.STATUS_ALIVE
    if inp["spouse"] == L.STATUS_RENOUNCED:
        notes.append(f"配偶者: 相続放棄（{L.ARTICLES['renounced']}）")
    line, units = None, []
    for name, fn in ((L.LINE_CHILDREN, lambda: _children_line(inp["children"])),
                     (L.LINE_ASCENDANTS, lambda: _ascendants_line(inp["ascendants"])),
                     (L.LINE_SIBLINGS, lambda: _siblings_line(inp["siblings"]))):
        res = fn()
        u, n = res[0], res[1]
        notes.extend(n)
        if len(res) > 2 and res[2]:
            reasons.append(L.UNSUPPORTED["sibling_representation_depth"])
        if u:
            line, units = name, u
            break
    heirs: list = []
    if spouse and line:
        sp_share, line_share = L.SHARES_WITH_SPOUSE[line]
    elif spouse:
        sp_share, line_share = L.SHARE_SPOUSE_ONLY, Fraction(0)
    elif line:
        sp_share, line_share = Fraction(0), L.SHARE_LINE_ONLY
    else:
        sp_share, line_share = Fraction(0), Fraction(0)
    if spouse:
        heirs.append({"label": "配偶者", "line": L.LINE_SPOUSE, "share": _frac(sp_share),
                      "basis": [L.ARTICLES["spouse"], L.ARTICLES["shares"]], "note": ""})
    if line:
        total_w = sum((w for _, w, _, _ in units), Fraction(0))
        for label, w, basis, note in units:
            heirs.append({"label": label, "line": line, "share": _frac(line_share * w / total_w),
                          "basis": basis + [L.ARTICLES["shares"]], "note": note})
    if not heirs:
        reasons.append(L.UNSUPPORTED["no_heirs"])
        result = RESULT_NO_HEIRS
    elif reasons:
        result = RESULT_NEEDS_ATTORNEY
    else:
        result = RESULT_DETERMINED
    # 検算: 相続人がいれば分数の合計は 1
    if heirs:
        total = sum((Fraction(h["share"]) for h in heirs), Fraction(0))
        if total != 1:
            reasons.append("分数の合計が 1 にならない（判定表の見直し）")
            result = RESULT_NEEDS_ATTORNEY
    return {"result": result, "heirs": heirs, "notes": notes, "attorney_reasons": sorted(set(reasons)),
            "law_table_version": law_table_version(), "review_mark": L.REVIEW_MARK}


# ── 保存・承認（RV-08: 版を重ねる・削除しない） ─────────────────────────────────
def _row(r) -> dict:
    d = dict(r._mapping)
    d["input"] = json.loads(d.pop("input_json") or "{}")
    d["result_detail"] = json.loads(d.pop("result_json") or "{}")
    return d


async def latest(intake_id: str) -> dict | None:
    async with session_scope() as s:
        r = (await s.execute(sa.select(souzoku_heirs).where(souzoku_heirs.c.intake_id == str(intake_id))
                             .order_by(souzoku_heirs.c.version.desc()).limit(1))).first()
    return _row(r) if r is not None else None


async def versions(intake_id: str) -> list[dict]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(souzoku_heirs).where(souzoku_heirs.c.intake_id == str(intake_id))
                                .order_by(souzoku_heirs.c.version.asc()))).fetchall()
    return [_row(r) for r in rows]


async def save_draft(intake_id: str, raw_input: dict, *, actor: str = ACTOR_HUMAN, now=None) -> dict:
    """聞取りを正規化して判定し、新しい版の draft として保存する（確定済みの版があっても新版は draft）。"""
    inp = normalize_input(raw_input)
    res = determine(inp)
    now = now or _now()
    hid = uuid.uuid4().hex
    async with session_scope() as s:
        cur = (await s.execute(sa.select(sa.func.max(souzoku_heirs.c.version)).where(
            souzoku_heirs.c.intake_id == str(intake_id)))).scalar()
        version = int(cur or 0) + 1
        await s.execute(sa.insert(souzoku_heirs).values(
            heirs_id=hid, intake_id=str(intake_id), business=BUSINESS, version=version,
            input_json=json.dumps(inp, ensure_ascii=False), result_json=json.dumps(res, ensure_ascii=False),
            result=res["result"], status=STATUS_DRAFT, law_table_version=res["law_table_version"], created_at=now))
        await s.execute(sa.insert(souzoku_heirs_history).values(heirs_id=hid, action=ACTION_CREATED, actor=actor, at=now))
    logger.info("[SOUZOKU_HEIRS] draft saved version=%s result=%s heirs=%s", emit(version, "count", "log", "operator"),
                emit(res["result"], "freetext", "log", "operator"), emit(len(res["heirs"]), "count", "log", "operator"))
    return {"heirs_id": hid, "version": version, "result": res}


async def confirm(heirs_id: str, *, actor: str = ACTOR_HUMAN, now=None) -> bool:
    """人の明示操作で draft → confirmed（条件付き UPDATE・二重操作は False・履歴 confirmed）。"""
    now = now or _now()
    async with session_scope() as s:
        r = await s.execute(sa.update(souzoku_heirs).where(souzoku_heirs.c.heirs_id == str(heirs_id),
                                                           souzoku_heirs.c.status == STATUS_DRAFT).values(
            status=STATUS_CONFIRMED, confirmed_by=actor, confirmed_at=now))
        if int(r.rowcount or 0) != 1:
            return False
        await s.execute(sa.insert(souzoku_heirs_history).values(heirs_id=str(heirs_id), action=ACTION_CONFIRMED, actor=actor, at=now))
    logger.info("[SOUZOKU_HEIRS] confirmed count=%s", emit(1, "count", "log", "operator"))
    return True


async def history(heirs_id: str) -> list[dict]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(souzoku_heirs_history).where(souzoku_heirs_history.c.heirs_id == str(heirs_id))
                                .order_by(souzoku_heirs_history.c.history_id))).fetchall()
    return [dict(r._mapping) for r in rows]
