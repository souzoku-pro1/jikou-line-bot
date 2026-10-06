"""brain_migration — BRAIN-ID-1a: 案件脳の移行（M1 → backfill → M2）の共通部品（R28）

正本: Desktop\\claude\\案件脳_設計_v4.3.md §3-1 の移行 (1)〜(5)・§14-2（裁定 R28）。
同期 Connection（SQLAlchemy Core）だけを受ける純関数群。alembic を import しない（D2）・
kintone に触れない・logging 非 import（PII の反射経路を持たない）。

- verify(conn)          検算（backfill --verify と M2 の upgrade() 冒頭が**同じ関数**を使う）:
                        案件キー↔case_id 1:1（両方向・BI-04）・必須表で case_id NULL ゼロ・
                        NULL 可の表は案件キー NULL の行だけが NULL・case_fact の prev 案件キーは
                        prev_case_id と一致（BI-03）・件数一致・再計算 idem_key の一致・running
                        run ゼロ。戻り値 {"ok": bool, "problems": [固定語彙], "counts": {...}}
- plan(conn, ...)       backfill が行う変更の件数（--dry-run の表示。書かない）
- backfill(conn, ...)   冪等・バッチ tx（案件キーごとに case を 1 件・case_identity
                        kintone_record・case_id の埋め込み・idem_key の再計算・App 40 の
                        LINEユーザーID から line_user 識別子）
- downgrade_refusals(conn)  M2 の downgrade 拒否条件（新形式データの検出・固定語彙）
- rewrite_event_keys_legacy(conn)  M2 downgrade が idem_key を A1 形式へ戻す

出力は件数と固定語彙のみ（PII なし）。
"""

import datetime
import re

import sqlalchemy as sa

from hub import brain_ledger as ledger

_LINE_ID_RE = re.compile(r"^U[0-9a-f]{32}$")
_NEW_KEY_RE = re.compile(r"^c[0-9]+\|")
APP40_LINE_ID_ITEM = ledger.APP40_PREFIX + "LINEユーザーID"
BATCH = 500

PROBLEM_KEY_WITHOUT_CASE = "case_key_without_case"          # 案件キーに case が無い
PROBLEM_KEY_AMBIGUOUS = "case_key_ambiguous"                # 同じ案件キーに有効識別子が 2 件以上
PROBLEM_CASE_ID_MISMATCH = "case_id_mismatch"               # 行の case_id が識別子の案件と違う
PROBLEM_REQUIRED_NULL = "required_case_id_null"             # 必須表で case_id NULL
PROBLEM_NULLABLE_INCONSISTENT = "nullable_case_id_inconsistent"  # 案件キーと case_id の NULL が食い違う
PROBLEM_COUNT_MISMATCH = "case_row_count_mismatch"          # 件数不一致
PROBLEM_IDEM_KEY = "event_idem_key_mismatch"                # 再計算 idem_key が保存値と違う
PROBLEM_RUNNING = "running_runs_present"                    # running の sync_run がある
PROBLEM_REGISTERED_WITHOUT_KEY = "registered_case_without_key"  # 登録済み case に識別子が無い
PROBLEM_CASE_SHARED = "case_id_shared_by_keys"              # 1 つの case_id を 2 つ以上の旧案件キーが指す
PREV_CASE = "case_fact.prev_case_id"                        # BI-03: 移動履歴の出所（旧案件）

REFUSE_UNREGISTERED = "unregistered_case_present"
REFUSE_MERGED = "merged_case_present"
REFUSE_IDENTITY_DUP = "duplicate_case_identity_present"
REFUSE_MERGE_HISTORY = "merge_history_present"
REFUSE_SUBJECT_MERGE = "subject_merge_history_present"
REFUSE_FACT_WITHOUT_KEY = "case_fact_without_case_key"
REFUSE_EVENT_WITHOUT_KEY = "case_event_without_case_key"
REFUSE_DERIVATION_WITHOUT_KEY = "case_derivation_without_case_key"

_T = ledger.metadata.tables
_CASE = _T["case"]
_IDENT = _T["case_identity"]
_FACT = _T["case_fact"]
_EVENT = _T["case_event"]
_DERIV = _T["case_derivation"]
_INGEST = _T["source_ingest"]
_LINK = _T["link_history"]
_RUN = _T["sync_run"]
_MERGE = _T["merge_history"]
_SMERGE = _T["subject_merge_history"]
_REQUIRED = (("case_fact", _FACT), ("case_event", _EVENT), ("case_derivation", _DERIV))
_NULLABLE = (("source_ingest", _INGEST),)


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def _count(conn, stmt) -> int:
    return int(conn.execute(stmt).scalar() or 0)


def _kintone_key_of(namespace: str) -> str | None:
    return ledger.app_id_of_namespace(namespace)


# ── 案件キーの収集と識別子の対応 ─────────────────────────────────────────────

def _legacy_keys(conn) -> set:
    """互換の案件キー (アプリ ID, レコード番号) の全集合（案件キー列を持つ全表＋
    link_history の prev/new）。"""
    keys = set()
    for _name, t in _REQUIRED + _NULLABLE:
        rows = conn.execute(sa.select(t.c.case_app_id, t.c.case_record_id).where(
            t.c.case_app_id.isnot(None)).distinct()).fetchall()
        keys.update((str(r[0]), str(r[1])) for r in rows)
    for a, b in ((_LINK.c.prev_case_app_id, _LINK.c.prev_case_record_id),
                 (_LINK.c.new_case_app_id, _LINK.c.new_case_record_id),
                 (_FACT.c.prev_case_app_id, _FACT.c.prev_case_record_id)):     # BI-03
        rows = conn.execute(sa.select(a, b).where(a.isnot(None)).distinct()).fetchall()
        keys.update((str(r[0]), str(r[1])) for r in rows)
    return keys


def _prev_mismatch_count(conn, mapping: dict) -> int:
    """case_fact の prev 案件キーがある行で prev_case_id が対応表と違う件数（BI-03）。"""
    n = 0
    rows = conn.execute(sa.select(_FACT.c.prev_case_app_id, _FACT.c.prev_case_record_id,
                                  _FACT.c.prev_case_id, sa.func.count()).where(
        _FACT.c.prev_case_app_id.isnot(None), _FACT.c.prev_case_id.isnot(None)).group_by(
        _FACT.c.prev_case_app_id, _FACT.c.prev_case_record_id, _FACT.c.prev_case_id)).fetchall()
    for app, rec, cid, cnt in rows:
        if mapping.get((str(app), str(rec))) != int(cid):
            n += int(cnt)
    return n


def _identity_map(conn) -> tuple:
    """有効な kintone_record 識別子 → {(app, rec): [case_id, ...]}（名前空間は
    "kintone:*:{app}" を app で照合＝M2 の検算は env に依らない）。"""
    rows = conn.execute(sa.select(_IDENT.c.namespace, _IDENT.c.value, _IDENT.c.case_id).where(
        _IDENT.c.kind == ledger.IDENTITY_KINTONE_RECORD,
        _IDENT.c.valid_to.is_(None))).fetchall()
    out: dict = {}
    for ns, value, case_id in rows:
        app = _kintone_key_of(ns)
        if app is None:
            continue
        out.setdefault((app, str(value)), []).append(int(case_id))
    return out


def _expected_event_key(row) -> str:
    return ledger.event_idem_key(int(row.case_id), row.source_app_id, row.source_record_id,
                                 ledger.event_revision_part(row.idem_key), row.kind,
                                 row.locator)


# ── 検算（--verify と M2 が共用） ────────────────────────────────────────────

def verify(conn) -> dict:
    problems: list = []
    counts: dict = {}
    keys = _legacy_keys(conn)
    ident = _identity_map(conn)
    mapping: dict = {}
    for k in sorted(keys):
        ids = ident.get(k, [])
        if not ids:
            problems.append(PROBLEM_KEY_WITHOUT_CASE)
        elif len(set(ids)) > 1:
            problems.append(PROBLEM_KEY_AMBIGUOUS)
        else:
            mapping[k] = ids[0]
    counts["case_keys"] = len(keys)
    counts["cases_mapped"] = len(mapping)
    # BI-04: 逆方向の一意性（1 つの case_id を 2 つ以上の旧案件キーが指さない）
    owners: dict = {}
    for k, ids in ident.items():
        for cid in set(ids):
            owners.setdefault(cid, set()).add(k)
    shared = sorted(cid for cid, ks in owners.items() if len(ks) > 1)
    counts["case_id_shared_by_keys"] = len(shared)
    if shared:
        problems.append(PROBLEM_CASE_SHARED)
    # 登録済み case に有効な kintone_record 識別子が無い（1:1 の逆向き）
    registered = conn.execute(sa.select(_CASE.c.case_id).where(
        _CASE.c.registration == ledger.REGISTRATION_REGISTERED)).fetchall()
    with_key = {cid for ids in ident.values() for cid in ids}
    missing = [int(r[0]) for r in registered if int(r[0]) not in with_key]
    counts["registered_cases"] = len(registered)
    if missing:
        problems.append(PROBLEM_REGISTERED_WITHOUT_KEY)
    # 必須表: case_id NULL ゼロ・案件キーのある行は対応が一致・件数一致
    for name, t in _REQUIRED:
        total = _count(conn, sa.select(sa.func.count()).select_from(t))
        nulls = _count(conn, sa.select(sa.func.count()).select_from(t).where(t.c.case_id.is_(None)))
        counts[f"{name}_rows"] = total
        counts[f"{name}_case_id_null"] = nulls
        if nulls:
            problems.append(f"{PROBLEM_REQUIRED_NULL}:{name}")
        if _mismatch_count(conn, t, mapping):
            problems.append(f"{PROBLEM_CASE_ID_MISMATCH}:{name}")
        keyed = _count(conn, sa.select(sa.func.count()).select_from(t).where(
            t.c.case_app_id.isnot(None)))
        keyed_with_id = _count(conn, sa.select(sa.func.count()).select_from(t).where(
            t.c.case_app_id.isnot(None), t.c.case_id.isnot(None)))
        if keyed != keyed_with_id:
            problems.append(f"{PROBLEM_COUNT_MISMATCH}:{name}")
    # NULL 可の表: 案件キー NULL の行だけが case_id NULL（両方向）
    for name, t in _NULLABLE:
        total = _count(conn, sa.select(sa.func.count()).select_from(t))
        counts[f"{name}_rows"] = total
        bad = _count(conn, sa.select(sa.func.count()).select_from(t).where(sa.or_(
            sa.and_(t.c.case_app_id.isnot(None), t.c.case_id.is_(None)),
            sa.and_(t.c.case_app_id.is_(None), t.c.case_id.isnot(None),
                    _registered_case_ids_subq(t.c.case_id)))))
        counts[f"{name}_inconsistent"] = bad
        if bad:
            problems.append(f"{PROBLEM_NULLABLE_INCONSISTENT}:{name}")
        if _mismatch_count(conn, t, mapping):
            problems.append(f"{PROBLEM_CASE_ID_MISMATCH}:{name}")
    # link_history の prev/new
    for prefix in ("prev_", "new_"):
        app = _LINK.c[prefix + "case_app_id"]
        cid = _LINK.c[prefix + "case_id"]
        bad = _count(conn, sa.select(sa.func.count()).select_from(_LINK).where(
            app.isnot(None), cid.is_(None)))
        if bad:
            problems.append(f"{PROBLEM_NULLABLE_INCONSISTENT}:link_history.{prefix}case_id")
        if _link_mismatch_count(conn, prefix, mapping):
            problems.append(f"{PROBLEM_CASE_ID_MISMATCH}:link_history.{prefix}case_id")
    # BI-03: case_fact の prev 案件キーがある行は prev_case_id が非 NULL で対応が一致
    prev_null = _count(conn, sa.select(sa.func.count()).select_from(_FACT).where(
        _FACT.c.prev_case_app_id.isnot(None), _FACT.c.prev_case_id.is_(None)))
    counts["case_fact_prev_case_id_null"] = prev_null
    if prev_null:
        problems.append(f"{PROBLEM_NULLABLE_INCONSISTENT}:{PREV_CASE}")
    if _prev_mismatch_count(conn, mapping):
        problems.append(f"{PROBLEM_CASE_ID_MISMATCH}:{PREV_CASE}")
    # idem_key: 再計算が保存値と一致
    bad_keys = 0
    total_events = 0
    for row in conn.execute(sa.select(_EVENT.c.event_id, _EVENT.c.idem_key, _EVENT.c.case_id,
                                      _EVENT.c.source_app_id, _EVENT.c.source_record_id,
                                      _EVENT.c.kind, _EVENT.c.locator)).fetchall():
        total_events += 1
        if row.case_id is None or _expected_event_key(row) != row.idem_key:
            bad_keys += 1
    counts["case_event_idem_key_mismatch"] = bad_keys
    if bad_keys:
        problems.append(PROBLEM_IDEM_KEY)
    running = _count(conn, sa.select(sa.func.count()).select_from(_RUN).where(
        _RUN.c.status == "running"))
    counts["running_runs"] = running
    if running:
        problems.append(PROBLEM_RUNNING)
    return {"ok": not problems, "problems": problems, "counts": counts}


def _registered_case_ids_subq(col):
    """case_id が登録済み案件を指す（未登録案件への紐付けは案件キー NULL で正しい）。"""
    return col.in_(sa.select(_CASE.c.case_id).where(
        _CASE.c.registration == ledger.REGISTRATION_REGISTERED))


def _mismatch_count(conn, t, mapping: dict) -> int:
    """案件キーのある行で case_id が対応表と違う件数。"""
    n = 0
    rows = conn.execute(sa.select(t.c.case_app_id, t.c.case_record_id, t.c.case_id,
                                  sa.func.count()).where(
        t.c.case_app_id.isnot(None), t.c.case_id.isnot(None)).group_by(
        t.c.case_app_id, t.c.case_record_id, t.c.case_id)).fetchall()
    for app, rec, cid, cnt in rows:
        if mapping.get((str(app), str(rec))) != int(cid):
            n += int(cnt)
    return n


def _link_mismatch_count(conn, prefix: str, mapping: dict) -> int:
    app = _LINK.c[prefix + "case_app_id"]
    rec = _LINK.c[prefix + "case_record_id"]
    cid = _LINK.c[prefix + "case_id"]
    n = 0
    rows = conn.execute(sa.select(app, rec, cid, sa.func.count()).where(
        app.isnot(None), cid.isnot(None)).group_by(app, rec, cid)).fetchall()
    for a, r, c, cnt in rows:
        if mapping.get((str(a), str(r))) != int(c):
            n += int(cnt)
    return n


# ── backfill（冪等・バッチ tx） ──────────────────────────────────────────────

def _namespace_check(namespace_of) -> None:
    ns = namespace_of("40")
    if not ns.startswith(ledger.KINTONE_NAMESPACE_PREFIX) or ns.count(":") != 2 \
            or not ns.split(":")[1]:
        raise ValueError("kintone_namespace_malformed")   # KINTONE_SUBDOMAIN 未設定など


def plan(conn, *, namespace_of, line_namespace: str) -> dict:
    """--dry-run: backfill が行う変更の件数（書かない）。"""
    _namespace_check(namespace_of)
    keys = _legacy_keys(conn)
    ident = _identity_map(conn)
    new_cases = [k for k in keys if not ident.get(k)]
    out = {"case_keys": len(keys), "cases_to_create": len(new_cases),
           "cases_existing": len(keys) - len(new_cases)}
    for name, t in _REQUIRED + _NULLABLE:
        out[f"{name}_to_fill"] = _count(conn, sa.select(sa.func.count()).select_from(t).where(
            t.c.case_app_id.isnot(None), t.c.case_id.is_(None)))
    for prefix in ("prev_", "new_"):
        out[f"link_history_{prefix}to_fill"] = _count(
            conn, sa.select(sa.func.count()).select_from(_LINK).where(
                _LINK.c[prefix + "case_app_id"].isnot(None),
                _LINK.c[prefix + "case_id"].is_(None)))
    out["case_fact_prev_to_fill"] = _count(conn, sa.select(sa.func.count()).select_from(_FACT).where(
        _FACT.c.prev_case_app_id.isnot(None), _FACT.c.prev_case_id.is_(None)))
    out["event_keys_to_rewrite"] = _count(conn, sa.select(sa.func.count()).select_from(_EVENT).where(
        sa.not_(_EVENT.c.idem_key.like("c%|%"))))
    out["line_identities_to_create"] = len(_missing_line_identities(conn, line_namespace, ident, keys))
    return out


def _missing_line_identities(conn, line_namespace: str, ident: dict, keys: set) -> list:
    """App 40 の現在 fact（LINEユーザーID）から作るべき line_user 識別子 (case_id|key, value)。
    case が未作成の案件キーは (key, value) で返す（backfill 後に case_id へ解決）。"""
    rows = conn.execute(sa.select(_FACT.c.case_id, _FACT.c.case_app_id, _FACT.c.case_record_id,
                                  _FACT.c.value_text).where(
        _FACT.c.item_code == APP40_LINE_ID_ITEM, _FACT.c.is_current.is_(True),
        _FACT.c.source_app_id == _FACT.c.case_app_id)).fetchall()
    have = set()
    for r in conn.execute(sa.select(_IDENT.c.case_id, _IDENT.c.value).where(
            _IDENT.c.namespace == line_namespace, _IDENT.c.kind == ledger.IDENTITY_LINE_USER,
            _IDENT.c.valid_to.is_(None))).fetchall():
        have.add((int(r[0]), r[1]))
    wanted = []
    seen = set()
    for r in rows:
        value = str(r.value_text or "").strip()
        if not _LINE_ID_RE.fullmatch(value):
            continue
        cid = int(r.case_id) if r.case_id is not None else None
        if cid is None and r.case_app_id is not None:
            ids = ident.get((str(r.case_app_id), str(r.case_record_id)), [])
            cid = ids[0] if len(ids) == 1 else None
        target = cid if cid is not None else ("key", str(r.case_app_id), str(r.case_record_id))
        if (target, value) in seen or (cid is not None and (cid, value) in have):
            continue
        seen.add((target, value))
        wanted.append((target, value))
    return wanted


def backfill(conn, *, namespace_of, line_namespace: str, now=None, batch: int = BATCH) -> dict:
    """本体（--apply）。各段は冪等（既に case がある案件キーは再作成しない・case_id が
    入っている行は触らない・新形式の idem_key は書き換えない）。バッチごとに commit。"""
    _namespace_check(namespace_of)
    now = now or _now()
    counts = {"cases_created": 0, "identities_created": 0, "rows_filled": {},
              "link_history_filled": 0, "case_fact_prev_filled": 0, "event_keys_rewritten": 0,
              "line_identities_created": 0}
    keys = sorted(_legacy_keys(conn))
    ident = _identity_map(conn)
    mapping: dict = {}
    # (2) 案件キーごとに case を 1 件・kintone_record 識別子
    pending = 0
    for key in keys:
        ids = ident.get(key, [])
        if len(set(ids)) > 1:
            raise ValueError(PROBLEM_KEY_AMBIGUOUS)
        if ids:
            mapping[key] = ids[0]
            continue
        cid = int(conn.execute(sa.insert(_CASE).values(
            kind=ledger.CASE_KIND_HOUKI, registration=ledger.REGISTRATION_REGISTERED,
            status=ledger.CASE_ACTIVE, created_via="backfill", created_at=now,
            version=1)).inserted_primary_key[0])
        conn.execute(sa.insert(_IDENT).values(
            case_id=cid, namespace=namespace_of(key[0]), kind=ledger.IDENTITY_KINTONE_RECORD,
            value=key[1], valid_from=now, valid_to=None,
            reason=ledger.IDENTITY_REASON_BACKFILL, actor="backfill", created_at=now))
        mapping[key] = cid
        counts["cases_created"] += 1
        counts["identities_created"] += 1
        pending += 1
        if pending >= batch:
            conn.commit()
            pending = 0
    conn.commit()
    # (2) 案件キーを持つ行の case_id を埋める
    for name, t in _REQUIRED + _NULLABLE:
        filled = 0
        for i, (key, cid) in enumerate(mapping.items(), 1):
            r = conn.execute(sa.update(t).where(
                t.c.case_app_id == key[0], t.c.case_record_id == key[1],
                t.c.case_id.is_(None)).values(case_id=cid))
            filled += int(r.rowcount or 0)
            if i % batch == 0:
                conn.commit()
        conn.commit()
        counts["rows_filled"][name] = filled
    for prefix in ("prev_", "new_"):
        app = _LINK.c[prefix + "case_app_id"]
        rec = _LINK.c[prefix + "case_record_id"]
        cid_col = _LINK.c[prefix + "case_id"]
        for i, (key, cid) in enumerate(mapping.items(), 1):
            r = conn.execute(sa.update(_LINK).where(
                app == key[0], rec == key[1], cid_col.is_(None)).values({cid_col: cid}))
            counts["link_history_filled"] += int(r.rowcount or 0)
            if i % batch == 0:
                conn.commit()
        conn.commit()
    # BI-03: case_fact の移動履歴（prev 案件キー → prev_case_id）
    for i, (key, cid) in enumerate(mapping.items(), 1):
        r = conn.execute(sa.update(_FACT).where(
            _FACT.c.prev_case_app_id == key[0], _FACT.c.prev_case_record_id == key[1],
            _FACT.c.prev_case_id.is_(None)).values(prev_case_id=cid))
        counts["case_fact_prev_filled"] += int(r.rowcount or 0)
        if i % batch == 0:
            conn.commit()
    conn.commit()
    # (R28) case_event.idem_key を case_id 内包の新形式へ再計算（旧形式は併存させない）
    rows = conn.execute(sa.select(_EVENT.c.event_id, _EVENT.c.idem_key, _EVENT.c.case_id,
                                  _EVENT.c.source_app_id, _EVENT.c.source_record_id,
                                  _EVENT.c.kind, _EVENT.c.locator).where(
        _EVENT.c.case_id.isnot(None),
        sa.not_(_EVENT.c.idem_key.like("c%|%"))).order_by(_EVENT.c.event_id)).fetchall()
    for i, row in enumerate(rows, 1):
        new_key = _expected_event_key(row)
        if new_key != row.idem_key:
            conn.execute(sa.update(_EVENT).where(_EVENT.c.event_id == row.event_id).values(
                idem_key=new_key))
            counts["event_keys_rewritten"] += 1
        if i % batch == 0:
            conn.commit()
    conn.commit()
    # (R29) App 40 の fact に LINE ユーザーID があれば関係者識別子（無ければ次の同期で埋まる）
    ident = _identity_map(conn)
    for i, (target, value) in enumerate(_missing_line_identities(conn, line_namespace, ident,
                                                                 set(keys)), 1):
        if isinstance(target, tuple):
            cid = mapping.get((target[1], target[2]))
            if cid is None:
                continue
        else:
            cid = int(target)
        conn.execute(sa.insert(_IDENT).values(
            case_id=cid, namespace=line_namespace, kind=ledger.IDENTITY_LINE_USER,
            value=value, valid_from=now, valid_to=None,
            reason=ledger.IDENTITY_REASON_BACKFILL, actor="backfill", created_at=now))
        counts["line_identities_created"] += 1
        if i % batch == 0:
            conn.commit()
    conn.commit()
    return counts


# ── M2 downgrade の拒否条件と A1 形への戻し ─────────────────────────────────

def downgrade_refusals(conn) -> list:
    """新形式でしか表せない業務データ（§3-1・R28）: 1 件でもあれば固定語彙で返す。"""
    out = []
    if _count(conn, sa.select(sa.func.count()).select_from(_CASE).where(
            _CASE.c.registration == ledger.REGISTRATION_UNREGISTERED)):
        out.append(REFUSE_UNREGISTERED)
    if _count(conn, sa.select(sa.func.count()).select_from(_CASE).where(
            _CASE.c.status == ledger.CASE_MERGED)):
        out.append(REFUSE_MERGED)
    dup = conn.execute(sa.select(_IDENT.c.namespace, _IDENT.c.kind, _IDENT.c.value,
                                 sa.func.count()).group_by(
        _IDENT.c.namespace, _IDENT.c.kind, _IDENT.c.value).having(sa.func.count() >= 2)).first()
    if dup is not None:
        out.append(REFUSE_IDENTITY_DUP)
    if _count(conn, sa.select(sa.func.count()).select_from(_MERGE)):
        out.append(REFUSE_MERGE_HISTORY)
    if _count(conn, sa.select(sa.func.count()).select_from(_SMERGE)):
        out.append(REFUSE_SUBJECT_MERGE)
    for name, t in ((REFUSE_FACT_WITHOUT_KEY, _FACT), (REFUSE_EVENT_WITHOUT_KEY, _EVENT),
                    (REFUSE_DERIVATION_WITHOUT_KEY, _DERIV)):
        if _count(conn, sa.select(sa.func.count()).select_from(t).where(
                t.c.case_app_id.is_(None), t.c.case_id.isnot(None))):
            out.append(name)
    return out


def rewrite_event_keys_legacy(conn) -> int:
    """M2 downgrade: idem_key を A1 形式（案件キー内包・7 部）へ戻す（行自身の案件キー列から
    決定的に再計算）。"""
    n = 0
    rows = conn.execute(sa.select(_EVENT.c.event_id, _EVENT.c.idem_key, _EVENT.c.case_app_id,
                                  _EVENT.c.case_record_id, _EVENT.c.source_app_id,
                                  _EVENT.c.source_record_id, _EVENT.c.kind,
                                  _EVENT.c.locator).where(
        _EVENT.c.case_app_id.isnot(None), _EVENT.c.idem_key.like("c%|%"))).fetchall()
    for row in rows:
        legacy = ledger.legacy_event_idem_key(
            row.case_app_id, row.case_record_id, row.source_app_id, row.source_record_id,
            ledger.event_revision_part(row.idem_key), row.kind, row.locator)
        conn.execute(sa.update(_EVENT).where(_EVENT.c.event_id == row.event_id).values(
            idem_key=legacy))
        n += 1
    return n


def count_running_runs(conn) -> int:
    return _count(conn, sa.select(sa.func.count()).select_from(_RUN).where(
        _RUN.c.status == "running"))


def stop_stale_runs(conn, *, older_than_minutes: int = ledger.STALE_RUN_MINUTES, now=None) -> int:
    now = now or _now()
    threshold = now - datetime.timedelta(minutes=int(older_than_minutes))
    r = conn.execute(sa.update(_RUN).where(
        _RUN.c.status == "running", _RUN.c.started_at < threshold).values(
        status="stopped_stale", failure="stale_run", finished_at=now))
    conn.commit()
    return int(r.rowcount or 0)
