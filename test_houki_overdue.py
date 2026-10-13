"""HOUKI-JUKURYO-3（QUEUE-2026-10-13-D D-7）: 期限超過×未提出の一覧と 1 日 1 回の件数通知

固定する仕様:
- 超過行: 受任後×未提出×起算日_確定あり×残日数 < 0（残日数の小さい順）。起算日未確定・未来・提出済みは含めない。
- 通知: 件数のみ（レコード番号・氏名なし）・送信台帳（channel=houki_overdue・JST 日付キー）で 1 日 1 回・
  2 回目は duplicate_sent で送らない・0 件は送らない・台帳不可は送らない・env で停止。
- 一覧: /app/houki/overdue は _gate・案件番号・期限・残日数のみ（氏名・住所なし）・kintone 書込ゼロ。
- ジョブ登録 1 回（daily 9 時）。sink 違反ゼロ・allowlist 追加ゼロ・main 末尾結線。
"""

import asyncio
import datetime
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_HOUKI": "40", "TOKEN_HOUKI": "t40",
}.items():
    os.environ.setdefault(_k, _v)

import sqlalchemy as sa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from hub import db  # noqa: E402
from hub import houki_jukuryo as hj  # noqa: E402
from hub import houki_overdue as O  # noqa: E402
from hub import scheduler as hub_scheduler  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_houki_overdue_view as V  # noqa: E402
from hub.webapp_auth import MIN_ITERATIONS, hash_password, issue_session  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
TODAY = datetime.date(2026, 10, 13)
ADMIN = "U" + "c" * 32
_ENV = {"WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS), "WEBAPP_SESSION_SECRET": "s" * 32}


def _rec(rid: str, status: str = "受任", start: str = "", submitted: str = "", name: str = "山田 太郎"):
    return {"$id": {"value": rid}, "$revision": {"value": "1"}, "status": {"value": status},
            "申述提出日": {"value": submitted}, "起算日_確定": {"value": start}, "顧客名": {"value": name},
            "通知済み閾値": {"value": []}, "熟慮期間期限": {"value": ""}, "残日数": {"value": ""}}


RECORDS = [
    _rec("11", start="2026-05-01"),                       # 期限 2026-07-31 → 超過
    _rec("12", start="2026-06-20"),                       # 期限 2026-09-19 → 超過
    _rec("13", start="2026-09-01"),                       # 期限 2026-11-30 → 未来
    _rec("14", start=""),                                 # 起算日未確定
    _rec("15", start="2026-05-01", submitted="2026-07-01"),   # 提出済み
    _rec("16", status="問い合わせ", start="2026-05-01"),   # 対象外 status（受任前）
]


def _run(coro):
    try:
        return asyncio.run(coro)
    finally:
        db.reset_for_tests()


def _auth():
    return {"Cookie": f"webapp_session={issue_session()}"}


class TestPure(unittest.TestCase):
    def test_overdue_rows_and_text(self):
        rows = O.overdue_rows(RECORDS, TODAY)
        self.assertEqual([(r.record_id, r.deadline.isoformat(), r.remaining) for r in rows],
                         [("11", "2026-07-31", -74), ("12", "2026-09-19", -24)])
        self.assertEqual(O.overdue_rows(RECORDS, datetime.date(2026, 7, 31)), [])
        t = O.notice_text(TODAY, 2)
        self.assertIn("2 件", t)
        self.assertNotIn("山田", t)
        self.assertNotIn("No.11", t)
        self.assertEqual(O.ledger_key(TODAY), "houki_overdue:2026-10-13")
        with patch.dict(os.environ, {O.DISABLED_ENV: "1"}):
            self.assertTrue(O.disabled())


class TestNotice(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="overdue_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", **_ENV})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(sl.metadata.create_all)
        _run(_create())
        self.push = AsyncMock(return_value=True)
        self.fetch = AsyncMock(return_value=list(RECORDS))
        self._p = [patch.object(hj, "_push_admin_http", self.push), patch.object(hj, "get_admin_line_user_id", lambda: ADMIN),
                   patch.object(hj, "fetch_all_targets", self.fetch), patch.object(hj, "_today_jst", lambda: TODAY)]
        for p in self._p:
            p.start()

    def tearDown(self):
        for p in self._p:
            p.stop()
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _ops(self):
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(sl.send_operation))).fetchall()]
        return _run(_q())

    def test_daily_check_notifies_once_per_day_count_only(self):
        res = _run(O.overdue_daily_check())
        self.assertEqual((res["targets"], res["overdue"], res["notice"]), (6, 2, O.SEND_SENT))
        self.assertEqual(self.push.await_count, 1)
        text = self.push.await_args.args[1]
        self.assertIn("2 件", text)
        self.assertNotIn("山田", text)
        self.assertNotIn("No.1", text)
        ops = self._ops()
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["business"], ops[0]["channel"], ops[0]["purpose"], ops[0]["state"]),
                         ("souzoku-houki", "houki_overdue", "other", "sent"))
        self.assertEqual(ops[0]["inbound_event_id"], "houki_overdue:2026-10-13")
        # 同日 2 回目は送らない
        res2 = _run(O.overdue_daily_check())
        self.assertEqual(res2["notice"], O.SEND_DUPLICATE_SENT)
        self.assertEqual(self.push.await_count, 1)
        # 翌日は送る
        with patch.object(hj, "_today_jst", lambda: TODAY + datetime.timedelta(days=1)):
            self.assertEqual(_run(O.overdue_daily_check())["notice"], O.SEND_SENT)
        self.assertEqual(self.push.await_count, 2)

    def test_zero_disabled_failure_and_ledger_unavailable(self):
        self.fetch.return_value = [RECORDS[2], RECORDS[3]]
        res = _run(O.overdue_daily_check())
        self.assertEqual((res["overdue"], res["notice"]), (0, O.SEND_NONE))
        self.push.assert_not_called()
        with patch.dict(os.environ, {O.DISABLED_ENV: "1"}):
            self.assertEqual(_run(O.overdue_daily_check())["notice"], "disabled")
        self.fetch.side_effect = hj.kintone.KintoneError("boom")
        self.assertEqual(_run(O.overdue_daily_check())["notice"], O.SEND_NONE)
        self.fetch.side_effect = None
        self.fetch.return_value = list(RECORDS)
        self.push.return_value = False
        self.assertEqual(_run(O.overdue_daily_check())["notice"], O.SEND_FAILED)
        self.assertEqual(self._ops()[-1]["state"], "failed")
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            db.reset_for_tests()
            self.assertEqual(_run(O.send_daily_notice(TODAY, 2)), O.SEND_LEDGER_UNAVAILABLE)
        with patch.object(hj, "get_admin_line_user_id", lambda: ""):
            self.assertEqual(_run(O.send_daily_notice(TODAY, 2)), O.SEND_FAILED)

    def test_page_and_api_show_three_columns_only(self):
        self.assertEqual(_client.get(V.PAGE, follow_redirects=False).status_code, 303)
        r = _client.get(V.PAGE, headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertIn("No.11", r.text)
        self.assertIn("2026-07-31", r.text)
        self.assertIn("-74", r.text)
        self.assertNotIn("No.13", r.text)
        self.assertNotIn("山田", r.text)
        api = _client.get("/app/api/houki/overdue", headers=_auth()).json()
        self.assertEqual(api["count"], 2)
        self.assertEqual(set(api["items"][0]), {"record_id", "deadline", "remaining"})
        self.push.assert_not_called()                       # 画面は通知しない
        self.fetch.side_effect = RuntimeError("boom")
        r = _client.get(V.PAGE, headers=_auth())
        self.assertIn("取得できませんでした", r.text)
        self.assertFalse(_client.get("/app/api/houki/overdue", headers=_auth()).json()["ok"])

    def test_job_registered_once(self):
        with patch.object(hub_scheduler, "start_all"):
            O.register_houki_overdue_job()
            O.register_houki_overdue_job()
        self.assertTrue(hub_scheduler.is_registered(O.JOB_NAME))


class TestPins(unittest.TestCase):
    def test_sink_allowlist_and_wiring(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/houki_overdue.py", "hub/webapp_houki_overdue_view.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        self.assertNotIn("houki_overdue", allow)
        msrc = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        tail = "\n".join(msrc[-8:])
        self.assertIn("include_before_catch_all(app, houki_overdue_router)", tail)
        self.assertIn("app.router.on_startup.append(register_houki_overdue_job)", tail)


if __name__ == "__main__":
    unittest.main()
