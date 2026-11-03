"""JIKOU-SEND-PURPOSE-2（QUEUE-2026-10-13-E E-2）: 送信台帳 purpose 正式移行の準備（flag JIKOU_SEND_PURPOSE_V2・既定 OFF）

固定する仕様:
- 凍結の閉集合 send_ledger.PURPOSES（8 件）は不変。V2 集合は別名 PURPOSES_V2（8＋13）。種別→purpose の対応表
  PURPOSE_KINDS は alembic f8b0c2d4e6a8 の KINDS と同一（13 行・名前は接頭辞と同じ）。
- OFF（既定）: begin() は従来どおり other で記録（既存テスト不変）。
- ON かつ migration 未適用（alembic_version に f8b0 以降が無い）: 従来どおり other（CHECK 違反で記録が落ちる事故を防ぐ）。
- ON かつ適用済み: other＋既知接頭辞は種別名で記録。接頭辞の無い other・他の purpose は不変。
- 両方式を同一 key として照合: canonical_purpose／canonical_key／same_key・既存行検索は両表現（OFF→ON、ON→OFF の
  またぎでも DUPLICATE_SENT）。
- migration: 作成のみ（revision f8b0c2d4e6a8・down_revision e6a8c0d2f4b6・up は CHECK 拡張→書換・down は逆変換・
  不明行があれば拒否）。PURPOSE_V2_REVISIONS は chain 上の f8b0 以降の全 revision を含む（pin）。
- pin: sink 違反ゼロ・allowlist 追加ゼロ・呼出側（send_policy 等）は不変（purpose(PURPOSE_OTHER) のまま）。
"""

import ast
import os
import re
import unittest
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa

from test_jikou_reply_q1a import JIKOU, USER, _DbMixin, _FakeClient, _run  # noqa: E402

from hub import db  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
MIG = REPO / "alembic" / "versions" / "20261013_f8b0c2d4e6a8_send_purpose_v2.py"
KEY = "houki_overdue:2026-10-13"
ON = {sl.PURPOSE_V2_ENV: "1"}


def _migration_kinds() -> dict:
    """migration の KINDS を import せずに読む（アプリ本体から alembic を import しない規約・D2）。"""
    tree = ast.parse(MIG.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "KINDS" for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError("KINDS not found")


def _chain_after(rev: str) -> set:
    """alembic/versions の chain で rev 以降（rev を含む）の revision 集合。"""
    down = {}
    for p in (REPO / "alembic" / "versions").glob("*.py"):
        src = p.read_text(encoding="utf-8")
        r = re.search(r"^revision: str = '([0-9a-f]+)'", src, re.M)
        d = re.search(r"^down_revision: .*? = (None|'([0-9a-f]+)')", src, re.M)
        if r and d:
            down[r.group(1)] = d.group(2)
    out = set()
    for r in down:
        cur = r
        while cur is not None:
            if cur == rev:
                out.add(r)
                break
            cur = down.get(cur)
    return out


class TestPure(unittest.TestCase):
    def test_frozen_set_and_v2_table(self):
        self.assertEqual(sl.PURPOSES, ("reply", "first_reply", "urgent", "image_receipt", "image_result",
                                       "follow", "receipt_number", "other"))
        self.assertEqual(len(sl.PURPOSE_KINDS), 13)
        self.assertEqual(sl.PURPOSE_KINDS, _migration_kinds())
        self.assertTrue(all(k == v for k, v in sl.PURPOSE_KINDS.items()))      # 名前は接頭辞と同じ
        self.assertEqual(sl.PURPOSES_V2, sl.PURPOSES + tuple(sl.PURPOSE_KINDS.values()))
        self.assertFalse(set(sl.PURPOSES) & set(sl.PURPOSE_KINDS.values()))
        self.assertEqual(sl.PURPOSE_V2_ENV, "JIKOU_SEND_PURPOSE_V2")
        self.assertFalse(sl.purpose_v2_enabled())                              # 既定 OFF
        self.assertEqual(_chain_after(sl.PURPOSE_V2_REVISION), set(sl.PURPOSE_V2_REVISIONS))

    def test_canonical_and_same_key(self):
        self.assertEqual(sl.kind_of(KEY), "houki_overdue")
        self.assertIsNone(sl.kind_of("ev123"))
        self.assertIsNone(sl.kind_of("unknown:1"))
        self.assertIsNone(sl.kind_of(":x"))
        self.assertIsNone(sl.kind_of(None))
        self.assertEqual(sl.canonical_purpose("other", KEY), "houki_overdue")
        self.assertEqual(sl.canonical_purpose("other", "ev123"), "other")
        self.assertEqual(sl.canonical_purpose("reply", KEY), "reply")
        self.assertEqual(sl.canonical_purpose("houki_overdue", KEY), "houki_overdue")
        a = {"business": "souzoku-houki", "channel": "houki_overdue", "purpose": "other", "inbound_event_id": KEY, "inbound_seq": 1}
        b = dict(a, purpose="houki_overdue")
        c = dict(a, inbound_event_id="houki_overdue:2026-10-14")
        self.assertTrue(sl.same_key(a, b))
        self.assertFalse(sl.same_key(a, c))
        self.assertEqual(sl.canonical_key("j", "c", "other", KEY), sl.canonical_key("j", "c", "houki_overdue", KEY, 1))
        self.assertEqual(sl.equivalent_purposes("other", KEY), ["other", "houki_overdue"])
        self.assertEqual(sl.equivalent_purposes("houki_overdue", KEY), ["houki_overdue", "other"])
        self.assertEqual(sl.equivalent_purposes("other", "ev1"), ["other"])
        self.assertEqual(sl.equivalent_purposes("reply", "ev1"), ["reply"])

    def test_migration_source_pins(self):
        src = MIG.read_text(encoding="utf-8")
        self.assertIn("revision: str = 'f8b0c2d4e6a8'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'e6a8c0d2f4b6'", src)
        self.assertIn("send_operation_purpose_downgrade_refused_unknown_rows_present", src)
        self.assertIn("ck_send_operation_purpose", src)
        self.assertLess(src.index("_replace_check(_legacy_expr(), _v2_expr())"), src.index('_rewrite(lambda k, p: "other", lambda k, p: p)'))
        self.assertIn("docs/send_ledger_purpose_migration.md", src)
        doc = (REPO / "docs" / "send_ledger_purpose_migration.md").read_text(encoding="utf-8")
        for k in sl.PURPOSE_KINDS:
            self.assertIn(f"`{k}`", doc)
        self.assertIn("要 Postgres-ak5B 実走", doc)


def _v2_schema(ck_expr_v2: str):
    """sqlite の試験用: send_operation の CHECK を V2 集合に差し替えて create_all（migration 適用済みの形を模す）。"""
    ck = next(c for c in sl.send_operation.constraints
              if isinstance(c, sa.CheckConstraint) and c.name == "ck_send_operation_purpose")
    original = ck.sqltext
    ck.sqltext = sa.text(ck_expr_v2)
    try:
        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(sl.metadata.create_all)
                await c.execute(sa.text("CREATE TABLE IF NOT EXISTS alembic_version (version_num VARCHAR(32) NOT NULL)"))
        _run(_create())
    finally:
        ck.sqltext = original


class _V2Mixin(_DbMixin):
    """表は V2 CHECK で作る（適用済みの形）。alembic_version は空で始める（未適用）。"""

    def setUp(self):
        super().setUp()
        sl.reset_purpose_v2_cache()

        async def _drop():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(sl.metadata.drop_all)
        _run(_drop())
        expr = "purpose IN (" + ", ".join("'" + p + "'" for p in sl.PURPOSES_V2) + ")"
        _v2_schema(expr)

    def tearDown(self):
        sl.reset_purpose_v2_cache()
        super().tearDown()

    def mark_applied(self, rev: str = sl.PURPOSE_V2_REVISION):
        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.text("DELETE FROM alembic_version"))
                await s.execute(sa.text("INSERT INTO alembic_version (version_num) VALUES (:v)").bindparams(v=rev))
        _run(_ins())
        sl.reset_purpose_v2_cache()

    def send_other(self, key: str = KEY, business: str = "souzoku-houki", channel: str = "houki_overdue"):
        async def _go():
            tok = sl.bind_inbound(key)
            try:
                with sl.purpose(sl.PURPOSE_OTHER):
                    op = await sl.begin(business, channel, USER, "text")
                if isinstance(op, sl.Operation):
                    await sl.finish(op, sl.STATE_SENT)
                return op
            finally:
                sl.unbind(tok)
        return _run(_go())


class TestFlagBehaviour(_V2Mixin):
    def test_off_records_other(self):
        self.assertIsInstance(self.send_other(), sl.Operation)
        self.assertEqual([o["purpose"] for o in self.ops()], ["other"])

    def test_on_but_not_applied_keeps_other(self):
        with patch.dict(os.environ, ON):
            with self.assertLogs("hub.send_ledger", level="INFO") as cm:
                self.assertIsInstance(self.send_other(), sl.Operation)
        self.assertEqual([o["purpose"] for o in self.ops()], ["other"])
        self.assertIn("purpose v2 not ready (legacy purpose kept)", "\n".join(cm.output))

    def test_on_and_applied_records_kind_and_keeps_others(self):
        self.mark_applied()
        with patch.dict(os.environ, ON):
            self.assertIsInstance(self.send_other(), sl.Operation)
            self.assertIsInstance(self.send_other(key="ev-no-prefix", channel="x"), sl.Operation)
            self.assertIsInstance(self.send_other(key="unknown:1", channel="y"), sl.Operation)
            self.assertIs(_run(line_channel.push_text(JIKOU, USER, "hello")), True)       # purpose=reply のまま
        got = {(o["channel"], o["inbound_event_id"]): o["purpose"] for o in self.ops()}
        self.assertEqual(got[("houki_overdue", KEY)], "houki_overdue")
        self.assertEqual(got[("x", "ev-no-prefix")], "other")
        self.assertEqual(got[("y", "unknown:1")], "other")
        self.assertEqual(got[("jikou", None)], "reply")

    def test_duplicate_across_representations(self):
        # OFF で送った（other）→ ON で同じ key → DUPLICATE_SENT（送らない）
        self.assertIsInstance(self.send_other(), sl.Operation)
        self.mark_applied()
        with patch.dict(os.environ, ON):
            self.assertIs(self.send_other(), sl.DUPLICATE_SENT)
            # ON で新 key（新表現）→ OFF に戻しても DUPLICATE_SENT
            k2 = "houki_overdue:2026-10-14"
            self.assertIsInstance(self.send_other(key=k2), sl.Operation)
        sl.reset_purpose_v2_cache()
        self.assertIs(self.send_other(key=k2), sl.DUPLICATE_SENT)
        ops = sorted(self.ops(), key=lambda o: o["inbound_event_id"])
        self.assertEqual([(o["inbound_event_id"], o["purpose"]) for o in ops], [(KEY, "other"), (k2, "houki_overdue")])

    def test_readiness_cache_and_failure(self):
        with patch.dict(os.environ, ON):
            self.assertFalse(_run(sl.purpose_v2_ready()))
            self.mark_applied()
            self.assertTrue(_run(sl.purpose_v2_ready()))
            self.mark_applied("e6a8c0d2f4b6")                       # 前の head に戻ったら未適用扱い
            self.assertFalse(_run(sl.purpose_v2_ready()))
            with patch.object(sl, "session_scope", side_effect=RuntimeError("x")):
                sl.reset_purpose_v2_cache()
                self.assertFalse(_run(sl.purpose_v2_ready()))
        self.assertFalse(_run(sl.purpose_v2_ready()))               # OFF は常に False


class TestPins(unittest.TestCase):
    def test_sink_allowlist_and_callers_unchanged(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        src = (REPO / "hub" / "send_ledger.py").read_text(encoding="utf-8")
        self.assertEqual(scan_source(src, "hub/send_ledger.py"), [])
        self.assertNotIn("send_ledger.py", allow)
        self.assertEqual(src.count("await effective_purpose(purpose_, ctx.inbound_event_id if ctx else None)"), 1)
        self.assertEqual(src.count("send_operation.c.purpose.in_(equivalent_purposes(purpose_, event_id))"), 1)
        for name, call in (("hub/send_policy.py", "send_ledger.purpose(send_ledger.PURPOSE_OTHER)"),
                           ("hub/houki_jukuryo.py", "send_ledger.purpose(LEDGER_PURPOSE)"),
                           ("hub/jikou_reply_metrics.py", "send_policy.send_business_notice(")):
            body = (REPO / name).read_text(encoding="utf-8")
            self.assertIn(call, body, name)                                        # 呼出側は不変（other のまま）
            self.assertNotIn("effective_purpose", body, name)
        self.assertIn("LEDGER_PURPOSE = send_ledger.PURPOSE_OTHER", (REPO / "hub" / "houki_jukuryo.py").read_text(encoding="utf-8"))
        self.assertEqual(_FakeClient.raise_exc, None)


if __name__ == "__main__":
    unittest.main()
