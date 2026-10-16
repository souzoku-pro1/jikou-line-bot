"""JIKOU-SEND-FAILCLOSED-1（QUEUE-2026-10-13-E E-4）: bot 送信経路の「台帳必須」切替（flag JIKOU_SEND_FAILCLOSED・既定 OFF）

固定する仕様:
- OFF（既定）: hub/line_channel の 2 プリミティブは Q1a の fail-open のまま（DB 未設定・DB 例外でも送る）。test_jikou_reply_q1a
  の TestFailOpen が不変のまま通ることが証明（本ファイルでも同じ形で再確認）。
- ON: 台帳 begin() が None（DB 未設定・DB 例外）なら送らない（reply=SEND_FAILED・push=False・LINE HTTP 呼出ゼロ）。
  業務 LINE へ障害通知 1 通（固定文言・PII なし）。1 回性: 台帳 key send_ledger_down:{JST 日付}（台帳が使えれば行で判定・
  使えなければプロセス内メモで同日 2 回目を抑える）。宛先未設定は skipped。
- ON で台帳が使えるときは従来どおり送る（通知なし）。重複判定・unconfirmed の扱いは不変。
- pin: 新 module の sink 違反ゼロ・allowlist 追加ゼロ・line_channel の分岐は 2 か所（flag 判定付き）・既存テスト不変。
"""

import datetime
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1a import JIKOU, USER, _DbMixin, _FakeClient, _reset_fake, _run  # noqa: E402

from hub import db  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import send_failclosed as FC  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
HOUKI = line_channel.HOUKI_CHANNEL
ATTORNEY = "U" + "c" * 32
TODAY = datetime.date(2026, 10, 13)
ON = {FC.FLAG_ENV: "1", FC.RECIPIENT_ENV: ATTORNEY}


class _Base(unittest.TestCase):
    def setUp(self):
        _reset_fake()
        FC.reset_memo()
        self.push = AsyncMock(return_value=True)
        self._p = [patch.object(line_channel.httpx, "AsyncClient", _FakeClient), patch.object(FC, "_push", self.push),
                   patch.object(FC, "_today_jst", lambda: TODAY)]
        for p in self._p:
            p.start()

    def tearDown(self):
        for p in self._p:
            p.stop()
        FC.reset_memo()
        db.reset_for_tests()


def _no_db_env() -> dict:
    return {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}


class TestOffIsUnchanged(_Base):
    def test_off_without_database_url_sends(self):
        with patch.dict(os.environ, _no_db_env(), clear=True):
            db.reset_for_tests()
            self.assertFalse(FC.enabled())
            self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), True)
            self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "t", USER, "y")), sl.SEND_SENT)
        self.assertEqual(len(_FakeClient.calls), 2)
        self.assertEqual(self.push.call_count, 0)

    def test_off_with_database_error_sends(self):
        d = tempfile.mkdtemp(prefix="fc_nodb_")
        try:
            with patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{d}/empty.db"}):
                db.reset_for_tests()
                self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), True)
            self.assertEqual(len(_FakeClient.calls), 1)
            self.assertEqual(self.push.call_count, 0)
        finally:
            shutil.rmtree(d, ignore_errors=True)


class TestOnFailClosed(_Base):
    def test_on_without_database_url_stops_and_notifies_once_per_day(self):
        with patch.dict(os.environ, {**_no_db_env(), **ON}, clear=True):
            db.reset_for_tests()
            self.assertTrue(FC.enabled())
            with self.assertLogs("hub.send_failclosed", level="WARNING") as cm:
                self.assertIs(_run(line_channel.push_text(JIKOU, USER, "secret text")), False)
                self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "t", USER, "secret 2")), sl.SEND_FAILED)
                self.assertIs(_run(line_channel.push_text(JIKOU, USER, "secret 3")), False)
        self.assertEqual(_FakeClient.calls, [])                          # 相談者へは送らない
        self.assertEqual(self.push.call_count, 1)                        # 障害通知は同日 1 通
        to, text = self.push.call_args.args
        self.assertEqual(to, ATTORNEY)
        self.assertTrue(text.startswith(FC.NOTICE_HEADER + "2026-10-13"))
        self.assertIn("jikou", text)
        joined = "\n".join(cm.output)
        self.assertIn("[SEND_FAILCLOSED] send stopped (ledger unavailable)", joined)
        for bad in (USER, "secret"):
            self.assertNotIn(bad, joined)
            self.assertNotIn(bad, text)

    def test_on_with_database_error_stops(self):
        d = tempfile.mkdtemp(prefix="fc_nodb_")
        try:
            with patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{d}/empty.db", **ON}):
                db.reset_for_tests()                                     # 表が無い DB＝begin は None
                self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), False)
                self.assertIs(_run(line_channel.push_text(HOUKI, USER, "x")), False)
            self.assertEqual(_FakeClient.calls, [])
            self.assertEqual(self.push.call_count, 2)                    # チャネルごとに 1 通
            self.assertIn("souzoku-houki", self.push.call_args.args[1])
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_on_other_day_notifies_again_and_recipient_unset_is_skipped(self):
        with patch.dict(os.environ, {**_no_db_env(), **ON}, clear=True):
            db.reset_for_tests()
            self.assertEqual(_run(FC.notify_ledger_down("jikou", today=TODAY)), FC.NOTIFY_SENT)
            self.assertEqual(_run(FC.notify_ledger_down("jikou", today=TODAY)), FC.NOTIFY_THROTTLED)
            self.assertEqual(_run(FC.notify_ledger_down("jikou", today=TODAY + datetime.timedelta(days=1))), FC.NOTIFY_SENT)
            self.push.return_value = False
            self.assertEqual(_run(FC.notify_ledger_down("x", today=TODAY)), FC.NOTIFY_FAILED)
            self.assertEqual(_run(FC.notify_ledger_down("x", today=TODAY)), FC.NOTIFY_FAILED)   # 失敗はメモしない
            with patch.dict(os.environ, {FC.RECIPIENT_ENV: ""}):
                self.assertEqual(_run(FC.notify_ledger_down("jikou", today=TODAY)), FC.NOTIFY_SKIPPED)

    def test_notice_exception_does_not_raise(self):
        with patch.dict(os.environ, {**_no_db_env(), **ON}, clear=True), \
                patch.object(FC, "notify_ledger_down", AsyncMock(side_effect=RuntimeError("x"))):
            db.reset_for_tests()
            self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), False)
        self.assertEqual(_FakeClient.calls, [])


class TestOnWithLedger(_DbMixin):
    def setUp(self):
        super().setUp()
        FC.reset_memo()
        self.push = AsyncMock(return_value=True)
        self._fp = [patch.object(FC, "_push", self.push), patch.object(FC, "_today_jst", lambda: TODAY),
                    patch.dict(os.environ, ON)]
        for p in self._fp:
            p.start()

    def tearDown(self):
        for p in reversed(self._fp):
            p.stop()
        FC.reset_memo()
        super().tearDown()

    def test_on_with_ledger_sends_normally_without_notice(self):
        self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), True)
        self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "t", USER, "y")), sl.SEND_SENT)
        self.assertEqual(len(_FakeClient.calls), 2)
        self.assertEqual(self.push.call_count, 0)
        self.assertEqual(len(self.ops()), 2)

    def test_notice_once_per_day_is_judged_by_ledger_row(self):
        self.assertEqual(_run(FC.notify_ledger_down("jikou", today=TODAY)), FC.NOTIFY_SENT)
        self.assertEqual(_run(FC.notify_ledger_down("jikou", today=TODAY)), FC.NOTIFY_THROTTLED)
        FC.reset_memo()                                                  # メモが無くても台帳の行で抑止
        self.assertEqual(_run(FC.notify_ledger_down("jikou", today=TODAY)), FC.NOTIFY_THROTTLED)
        self.assertEqual(self.push.call_count, 1)
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["business"], ops[0]["channel"], ops[0]["purpose"], ops[0]["inbound_event_id"],
                          ops[0]["state"], ops[0]["actor"]),
                         ("jikou", FC.LEDGER_CHANNEL, sl.PURPOSE_OTHER, "send_ledger_down:2026-10-13", sl.STATE_SENT, sl.ACTOR_BOT))


class TestPins(unittest.TestCase):
    def test_sink_allowlist_and_branch_points(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/send_failclosed.py", "hub/line_channel.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        self.assertNotIn("send_failclosed", allow)
        src = (REPO / "hub" / "line_channel.py").read_text(encoding="utf-8")
        self.assertEqual(src.count("if op is None and send_failclosed.enabled():"), 2)
        self.assertEqual(src.count("await send_failclosed.on_ledger_unavailable(channel.name)"), 2)
        self.assertEqual(FC.FLAG_ENV, "JIKOU_SEND_FAILCLOSED")
        self.assertEqual(FC.ledger_key(TODAY), "send_ledger_down:2026-10-13")
        self.assertFalse(FC.enabled())                                   # 既定 OFF
        t = FC.notice_text("jikou", TODAY)
        self.assertIn(FC.NOTICE_BODY, t)
        self.assertNotIn("%", t)


if __name__ == "__main__":
    unittest.main()
