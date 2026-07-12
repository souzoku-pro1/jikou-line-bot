"""P1-102: S1 4経路の業務チャネル移送 + notify fail-closed + dead-man のテスト

固定する不変条件（DRAFT_RV10 §2/§4/§4.2）:
- S1 の弁護士向け通知は **業務チャネル（DISPATCHBOT）** から送る（顧客Bot 不使用）。
- 本文の氏名・相談本文は emit 経由で redact（既定=完全抑止）。
- business_token_env は fail-closed（顧客Bot へフォールバックしない）。
- notify_business は宛先 allowlist（ATTORNEY / 管理者）以外へ送らない。
- 業務チャネル成功で dead-man heartbeat を記録。daily_healthcheck が鮮度を検証。
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

# import に必要な最小 env のみ（他テストを汚染しないよう業務系 env は各テストで patch）
os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
# cloudsign_webhook の module-level 必須 env（import 時のみ・値は検査対象外）
os.environ.setdefault("CLOUDSIGN_CLIENT_ID", "test_cid")
os.environ.setdefault("CLOUDSIGN_WEBHOOK_SECRET", "test_secret")
os.environ.setdefault("KINTONE_APP_ID", "1")
os.environ.setdefault("KINTONE_API_TOKEN", "test_tok")

from hub import notify  # noqa: E402
from hub import redact  # noqa: E402

# 業務通知系 env（各テストで patch.dict・グローバル汚染しない）
_ENV = {
    "LINE_CHANNEL_ACCESS_TOKEN": "customer_tok",
    "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz_tok",
    "ATTORNEY_LINE_USER_ID": "Uattorney",
    "LINE_ADMIN_USER_ID": "Uadmin",
}


def _run(coro):
    return asyncio.run(coro)


class _EnvMixin(unittest.TestCase):
    def setUp(self):
        super().setUp()
        p = patch.dict(os.environ, _ENV, clear=False)
        p.start()
        self.addCleanup(p.stop)


class _FakePush:
    """push_line_message の HTTP を差し替え（token と本文を記録）"""
    calls: list = []
    status: int = 200

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, headers=None, json=None):
        _FakePush.calls.append({"auth": headers["Authorization"],
                                "to": json["to"],
                                "text": json["messages"][0]["text"]})
        resp = MagicMock()
        resp.is_success = _FakePush.status < 400
        resp.status_code = _FakePush.status
        resp.text = "ok" if resp.is_success else "err-body"
        return resp


class TestBusinessTokenFailClosed(_EnvMixin):
    def test_always_dispatchbot_no_fallback(self):
        self.assertEqual(notify.business_token_env(),
                         "DISPATCHBOT_CHANNEL_ACCESS_TOKEN")
        with patch.dict(os.environ, {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": ""}):
            self.assertEqual(notify.business_token_env(),
                             "DISPATCHBOT_CHANNEL_ACCESS_TOKEN")


class TestNotifyBusinessAllowlist(_EnvMixin):
    def setUp(self):
        super().setUp()
        _FakePush.calls = []
        _FakePush.status = 200

    def test_sends_to_attorney_via_business_channel(self):
        with patch.object(notify.httpx, "AsyncClient", _FakePush), \
             patch("hub.notify_heartbeat.record_success", new_callable=AsyncMock):
            ok = _run(notify.notify_business("Uattorney", "業務連絡"))
        self.assertTrue(ok)
        self.assertEqual(len(_FakePush.calls), 1)
        self.assertEqual(_FakePush.calls[0]["auth"], "Bearer biz_tok")

    def test_rejects_non_allowlisted_recipient(self):
        with patch.object(notify.httpx, "AsyncClient", _FakePush):
            ok = _run(notify.notify_business("Ustranger", "業務連絡"))
        self.assertFalse(ok)
        self.assertEqual(_FakePush.calls, [])  # allowlist 外は送らない

    def test_unset_dispatchbot_sends_nothing(self):
        with patch.dict(os.environ, {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": ""}), \
             patch.object(notify.httpx, "AsyncClient", _FakePush):
            ok = _run(notify.notify_business("Uattorney", "業務連絡"))
        self.assertFalse(ok)
        self.assertEqual(_FakePush.calls, [])  # 顧客Bot へ落とさない


class TestBuildAttorneyNotificationRedacted(_EnvMixin):
    def test_pii_not_present_in_body(self):
        from chat_responder import build_attorney_notification
        body = build_attorney_notification(
            "U1", "田中太郎", "42", "urgent_seizure",
            urgent_kind="差押え切迫", customer_message="差押えが来ました助けて")
        self.assertNotIn("田中太郎", body)
        self.assertNotIn("差押えが来ました", body)
        self.assertIn("42", body)          # record No は残る（参照用）
        self.assertIn("差押え切迫", body)   # urgent_kind は統制値なので残す


class TestChatResponderAttorneyBusinessChannel(_EnvMixin):
    def test_notify_attorney_uses_business_not_customer(self):
        import chat_responder
        with patch.object(chat_responder, "ATTORNEY_LINE_USER_ID", "Uattorney"), \
             patch("hub.notify.notify_business",
                   new_callable=AsyncMock) as biz, \
             patch.object(chat_responder, "send_line_push",
                          new_callable=AsyncMock) as cust:
            _run(chat_responder._notify_attorney(
                "U1", "田中太郎", "42", "urgent", urgent_kind="k",
                customer_message="秘密"))
        biz.assert_awaited_once()
        cust.assert_not_called()
        to, text = biz.await_args.args
        self.assertEqual(to, "Uattorney")
        self.assertNotIn("田中太郎", text)
        self.assertNotIn("秘密", text)


class TestHeartbeatRecordedOnBusinessSuccess(_EnvMixin):
    def setUp(self):
        super().setUp()
        _FakePush.calls = []
        _FakePush.status = 200

    def test_records_heartbeat_on_business_success(self):
        with patch.object(notify.httpx, "AsyncClient", _FakePush), \
             patch("hub.notify_heartbeat.record_success",
                   new_callable=AsyncMock) as rec:
            _run(notify.push_line_message(
                "Uattorney", "x", token_env="DISPATCHBOT_CHANNEL_ACCESS_TOKEN"))
        rec.assert_awaited_once_with("business")

    def test_no_heartbeat_on_customer_channel(self):
        with patch.object(notify.httpx, "AsyncClient", _FakePush), \
             patch("hub.notify_heartbeat.record_success",
                   new_callable=AsyncMock) as rec:
            _run(notify.push_line_message(
                "Uc", "x", token_env="LINE_CHANNEL_ACCESS_TOKEN"))
        rec.assert_not_awaited()


class TestHeartbeatDbBacked(unittest.TestCase):
    """DB(sqlite)での record/get の実動作 + DB 未設定時の no-op"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="hb_test_")
        self._url = f"sqlite+aiosqlite:///{self._dir}/test.db"
        self._env = patch.dict(os.environ, {"DATABASE_URL": self._url})
        self._env.start()
        import hub.db as db
        self.db = db
        db.reset_for_tests()
        import sqlalchemy as sa
        from hub.notify_heartbeat import _metadata

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as conn:
                await conn.run_sync(_metadata.create_all)
        _run(_create())
        db.reset_for_tests()

    def tearDown(self):
        self.db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def test_record_then_get(self):
        from hub.notify_heartbeat import get_last_success, record_success

        async def _flow():
            await record_success("business")
            return await get_last_success("business")
        last = _run(_flow())
        self.db.reset_for_tests()
        self.assertIsNotNone(last)

    def test_no_db_is_noop(self):
        from hub.notify_heartbeat import get_last_success, record_success
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            _run(record_success("business"))  # 例外なし
            self.assertIsNone(_run(get_last_success("business")))

    def test_upsert_conflict_collapses_to_single_row(self):
        """M02: 同一 channel への複数 upsert は ON CONFLICT DO UPDATE で1行に collapse。"""
        import sqlalchemy as sa
        from hub.notify_heartbeat import notify_heartbeat, record_success

        async def _flow():
            await record_success("business")
            await record_success("business")   # PK 衝突 → DO UPDATE（重複行を作らない）
            eng = self.db.get_async_engine()
            async with eng.connect() as conn:
                return (await conn.execute(
                    sa.select(sa.func.count()).select_from(
                        notify_heartbeat).where(
                        notify_heartbeat.c.channel == "business"))).scalar()
        rows = _run(_flow())
        self.db.reset_for_tests()
        self.assertEqual(rows, 1)

    def test_empty_status_before_first_record(self):
        """H01: テーブルはあるが行なし＝('empty', None)（行なし異常の下地）。"""
        from hub.notify_heartbeat import get_heartbeat_status
        status, last = _run(get_heartbeat_status("business"))
        self.db.reset_for_tests()
        self.assertEqual(status, "empty")
        self.assertIsNone(last)


class TestDeadmanLiveness(_EnvMixin):
    """daily_healthcheck 監視項目F: 業務通知経路の鮮度検証"""

    def test_dispatchbot_unset_is_reported(self):
        import daily_healthcheck as hc
        env = {**os.environ, "DATABASE_URL": "sqlite://",
               "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": ""}
        with patch.dict(os.environ, env):
            problems = _run(hc.check_business_notify_liveness())
        self.assertTrue(any("DISPATCHBOT" in p for p in problems))

    def test_no_database_url_skips(self):
        import daily_healthcheck as hc
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(_run(hc.check_business_notify_liveness()), [])

    def test_stale_heartbeat_reported(self):
        import daily_healthcheck as hc
        from datetime import datetime, timezone
        stale = datetime.now(timezone.utc) - timedelta(hours=30)
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite://",
                                     "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "t"}), \
             patch("hub.notify_heartbeat.get_heartbeat_status",
                   new_callable=AsyncMock, return_value=("ok", stale)):
            problems = _run(hc.check_business_notify_liveness())
        self.assertTrue(any("dead-man" in p for p in problems))

    def test_fresh_heartbeat_ok(self):
        import daily_healthcheck as hc
        from datetime import datetime, timezone
        fresh = datetime.now(timezone.utc) - timedelta(hours=1)
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite://",
                                     "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "t"}), \
             patch("hub.notify_heartbeat.get_heartbeat_status",
                   new_callable=AsyncMock, return_value=("ok", fresh)):
            self.assertEqual(_run(hc.check_business_notify_liveness()), [])

    def test_empty_table_is_abnormal(self):
        """H01: テーブルはあるが成功記録が1件も無い＝dead-man 異常として返す。"""
        import daily_healthcheck as hc
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite://",
                                     "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "t"}), \
             patch("hub.notify_heartbeat.get_heartbeat_status",
                   new_callable=AsyncMock, return_value=("empty", None)):
            problems = _run(hc.check_business_notify_liveness())
        self.assertTrue(any("dead-man" in p for p in problems))

    def test_table_missing_skips(self):
        """H01: migration 未適用（table_missing）は許容してスキップ。"""
        import daily_healthcheck as hc
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite://",
                                     "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "t"}), \
             patch("hub.notify_heartbeat.get_heartbeat_status",
                   new_callable=AsyncMock, return_value=("table_missing", None)):
            self.assertEqual(_run(hc.check_business_notify_liveness()), [])


class TestPushLowLevelAllowlist(_EnvMixin):
    """H02: push_line_message は業務チャネル宛先を allowlist で関所化する。"""

    def setUp(self):
        super().setUp()
        _FakePush.calls = []
        _FakePush.status = 200

    def test_business_channel_rejects_non_allowlisted(self):
        with patch.object(notify.httpx, "AsyncClient", _FakePush), \
             patch("hub.notify_heartbeat.record_success", new_callable=AsyncMock):
            ok = _run(notify.push_line_message(
                "Ustranger", "x", token_env="DISPATCHBOT_CHANNEL_ACCESS_TOKEN"))
        self.assertFalse(ok)
        self.assertEqual(_FakePush.calls, [])   # 未許可宛先へは送らない

    def test_business_channel_allows_env_registered(self):
        # SORTATION_ASK_TO 等の正当な直接 caller 宛先は allowlist に含める
        with patch.dict(os.environ, {"SORTATION_ASK_TO": "Usort"}), \
             patch.object(notify.httpx, "AsyncClient", _FakePush), \
             patch("hub.notify_heartbeat.record_success", new_callable=AsyncMock):
            ok = _run(notify.push_line_message(
                "Usort", "x", token_env="DISPATCHBOT_CHANNEL_ACCESS_TOKEN"))
        self.assertTrue(ok)
        self.assertEqual(len(_FakePush.calls), 1)


class TestHeartbeatBestEffort(_EnvMixin):
    """M01: heartbeat 記録失敗（一般DB障害）は HTTP 結果に影響させない。"""

    def setUp(self):
        super().setUp()
        _FakePush.calls = []
        _FakePush.status = 200

    def test_db_failure_keeps_http_success(self):
        with patch.object(notify.httpx, "AsyncClient", _FakePush), \
             patch("hub.notify_heartbeat.record_success",
                   new_callable=AsyncMock, side_effect=RuntimeError("db down")):
            ok = _run(notify.push_line_message(
                "Uattorney", "x", token_env="DISPATCHBOT_CHANNEL_ACCESS_TOKEN"))
        self.assertTrue(ok)                       # DB 障害でも HTTP 成功を返す
        self.assertEqual(len(_FakePush.calls), 1)


class TestNoDbNoToken(unittest.TestCase):
    """DB 未設定 + token 未設定の複合: 例外なく安全にスキップ／no-op。"""

    def test_record_success_sync_noop_without_db(self):
        from hub.notify_heartbeat import record_success_sync
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            record_success_sync("business")       # 例外なし no-op

    def test_push_skips_without_token(self):
        env = {k: v for k, v in os.environ.items()
               if k != "DISPATCHBOT_CHANNEL_ACCESS_TOKEN"}
        with patch.dict(os.environ, env, clear=True), \
             patch.object(notify.httpx, "AsyncClient", _FakePush):
            ok = _run(notify.push_line_message(
                "Uattorney", "x", token_env="DISPATCHBOT_CHANNEL_ACCESS_TOKEN"))
        self.assertFalse(ok)                       # token 無しは送信ゼロ


class TestCloudsignBusinessAdapter(_EnvMixin):
    """M03: cloudsign 業務通知アダプタ（allowlist + 2xx確認 + heartbeat記録）。"""

    @staticmethod
    def _resp(ok=True, status=200, text="ok"):
        r = MagicMock()
        r.ok = ok
        r.status_code = status
        r.text = text
        return r

    def test_records_heartbeat_on_2xx(self):
        import cloudsign_webhook as cw
        with patch.object(cw, "requests") as rq, \
             patch("hub.notify_heartbeat.record_success_sync") as rec:
            rq.post.return_value = self._resp(ok=True)
            cw.notify_business_line("【締結完了】案件レコードNo: 42")
        rq.post.assert_called_once()
        rec.assert_called_once_with("business")   # 送信成功で dead-man 記録

    def test_non_2xx_recognized_no_heartbeat(self):
        import cloudsign_webhook as cw
        with patch.object(cw, "requests") as rq, \
             patch("hub.notify_heartbeat.record_success_sync") as rec:
            rq.post.return_value = self._resp(ok=False, status=500, text="boom")
            cw.notify_business_line("x")
        rq.post.assert_called_once()
        rec.assert_not_called()                    # 非2xx は成功扱いしない

    def test_rejects_when_not_in_allowlist(self):
        import cloudsign_webhook as cw
        with patch("hub.notify.business_channel_allowlist",
                   return_value=frozenset({"Uother"})), \
             patch.object(cw, "requests") as rq, \
             patch("hub.notify_heartbeat.record_success_sync") as rec:
            cw.notify_business_line("x")
        rq.post.assert_not_called()                # 未許可宛先へは送らない
        rec.assert_not_called()


if __name__ == "__main__":
    unittest.main()
