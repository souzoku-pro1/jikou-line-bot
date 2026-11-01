"""JIKOU-STAGE-APP-1（QUEUE-2026-10-20-HI I-4'）: 時効案件進行アプリ（kintone）連携。

票のテスト 6 件 ＋ 周辺:
  - 署名（secret）不正 → 401・送信ゼロ／env 未設定 → 404・kintone 呼出ゼロ
  - Webhook 重複 10 回 → 送信 1 回（予約 1 つ・送信台帳 op_id で冪等）
  - PDF 生成失敗 → 送信ゼロ＋要対応 1 通（状態は戻さない）
  - 猶予中の取消（回答待ちに戻す）→ 送信ゼロ
  - 固定文が承認台帳に無い → 送信せず要対応
  - 時効確認済: 結果記録=成立・報告書 1 版・R-1 に本人専用 URL（PII なし）・報告済へ・送信日時/版番号/op_id を写す
  - 不成立: R-3・報告済(不成立)。全件報告済 → R-2 を 1 通・完了
  - 写し: 受任→入金確認→発送済→回答待ち を前方へのみ・人の状態には触れない
  - 本人専用 URL: 有効なら PDF・不正/期限切れは 404
"""

import asyncio
import datetime
import os
import unittest
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import _DbMixin, _FakeClient, _client, _run  # noqa: E402

from hub import approved_texts as AT  # noqa: E402
from hub import db  # noqa: E402
from hub import jikou_cert as C  # noqa: E402
from hub import jikou_doc_texts as DT  # noqa: E402
from hub import jikou_followup as F  # noqa: E402
from hub import jikou_receipt as R  # noqa: E402
from hub import jikou_report_link as L  # noqa: E402
from hub import jikou_stage as S  # noqa: E402
from hub import jikou_stage_texts as ST  # noqa: E402
from hub import kintone, line_channel  # noqa: E402

SECRET = "s3cr3t-" + "a" * 20
STAGE_ENV = {S.ENV_APP_ID: "500", S.ENV_SECRET: SECRET, S.ENV_TOKEN: "stage-token", S.ENV_GRACE_MIN: "0",
             "RAILWAY_PUBLIC_DOMAIN": "jikou.example.test",
             "OFFICE_NAME": "大野法律事務所", "OFFICE_ADDRESS": "川口市", "OFFICE_TEL": "048-299-2704",
             "OFFICE_ATTORNEY": "大野 太郎", "OFFICE_BAR": "埼玉弁護士会", "OFFICE_REG": "12345"}
CASE = "321"
LINE_USER = "U" + "d" * 32
NAME, BIRTH, ADDR, PHONE = "山田 太郎", "1980年1月2日", "川口市西青木1-1", "090-1111-2222"


def _app21(paid="入金済み", creditors=("A社", "B社")):
    base = {"顧客名": NAME, "生年月日": BIRTH, "住所": ADDR, "電話番号": PHONE, "LINEユーザーID": LINE_USER, "入金状況": paid,
            "status": "受任", "問い合わせ業者名": creditors[0], "対象債権者2": creditors[1] if len(creditors) > 1 else "",
            "対象債権者3": creditors[2] if len(creditors) > 2 else ""}
    return {"$id": {"value": CASE}, "$revision": {"value": "3"}, **{k: {"value": v} for k, v in base.items()}}


class _FakeKintone:
    """App 21（読むだけ）と 時効案件進行アプリ（読み書き）のインメモリ fake。呼出回数を記録する。"""

    def __init__(self, app21: dict):
        self.app21 = app21
        self.stage: dict[str, dict] = {}
        self.calls: list[tuple] = []
        self._next = 1000

    def _is_stage(self, app) -> bool:
        return app.app_id_env == S.ENV_APP_ID

    def add_stage(self, slot: int, status: str, **over) -> str:
        rid = str(self._next)
        self._next += 1
        vals = {S.F_CASE: CASE, S.F_SLOT: str(slot), S.F_CREDITOR: "A社" if slot == 1 else "B社", S.F_STATUS: status,
                S.F_SHIPPED: "2026-09-01", S.F_ANSWERED: "2026-10-01", S.F_ANSWER_MEMO: "時効完成を認める旨", S.F_OP_ID: "",
                "更新日時": "2026-10-20T00:00:00Z"}
        vals.update(over)
        self.stage[rid] = {"$id": {"value": rid}, "$revision": {"value": "1"}, **{k: {"value": v} for k, v in vals.items()}}
        return rid

    async def get_record(self, app, record_id):
        self.calls.append(("get", app.app_id_env, record_id))
        if self._is_stage(app):
            return self.stage[str(record_id)]
        return self.app21

    async def search_records(self, app, query, fields=None):
        self.calls.append(("search", app.app_id_env, query))
        if self._is_stage(app):
            rows = list(self.stage.values())
            if f'{S.F_CASE} = "' in query:
                case = query.split(f'{S.F_CASE} = "')[1].split('"')[0]
                rows = [r for r in rows if r[S.F_CASE]["value"] == case]
            if f"{S.F_STATUS} in (" in query:
                allowed = [x.strip().strip('"') for x in query.split("in (")[1].split(")")[0].split(",")]
                rows = [r for r in rows if r[S.F_STATUS]["value"] in allowed]
                if f'{S.F_OP_ID} = ""' in query:
                    rows = [r for r in rows if not r[S.F_OP_ID]["value"]]
            return rows
        return [self.app21] if "受任" in query else []

    async def create_record(self, app, fields):
        self.calls.append(("create", app.app_id_env, dict(fields)))
        rid = self.add_stage(int(fields[S.F_SLOT]), S.S_ENGAGED, **{k: v for k, v in fields.items() if k != S.F_SLOT})
        return rid

    async def update_record(self, app, record_id, fields, revision=None):
        self.calls.append(("update", app.app_id_env, record_id, dict(fields)))
        rec = self.stage[str(record_id)]
        for k, v in fields.items():
            rec[k] = {"value": v}
        rec["$revision"] = {"value": str(int(rec["$revision"]["value"]) + 1)}

    async def update_status(self, app, record_id, action, revision=None, assignee=None):
        self.calls.append(("status", app.app_id_env, record_id, action))
        nxt = {S.A_PAID: S.S_PAID, S.A_SHIPPED: S.S_SHIPPED, S.A_WAITING: S.S_WAITING, S.A_REPORTED: S.S_REPORTED,
               S.A_REPORTED_FAILED: S.S_REPORTED_FAILED, S.A_DONE: S.S_DONE}[action]
        self.stage[str(record_id)][S.F_STATUS] = {"value": nxt}

    def status_of(self, rid: str) -> str:
        return self.stage[rid][S.F_STATUS]["value"]

    def stage_writes(self):
        return [c for c in self.calls if c[0] in ("create", "update", "status")]


def _approve_all():
    d = {k: AT.sha256_text(ST.text(k)) for k in ST.TEXT_IDS}
    d.update({k: AT.sha256_text(DT.text(k)) for k in DT.TEXT_IDS})
    return patch.dict(AT.APPROVED_SHA256, d)


def _texts() -> list[str]:
    return [(j or {}).get("messages", [{}])[0].get("text", "") for _u, j in _FakeClient.calls]


class _Base(_DbMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        S.reset_for_tests()
        self.addCleanup(S.reset_for_tests)
        self._env = patch.dict(os.environ, STAGE_ENV)
        self._env.start()
        self.addCleanup(self._env.stop)
        self.k = _FakeKintone(_app21())
        for name in ("get_record", "search_records", "create_record", "update_record", "update_status"):
            p = patch.object(kintone, name, getattr(self.k, name))
            p.start()
            self.addCleanup(p.stop)
        _FakeClient.responses, _FakeClient.calls, _FakeClient.raise_exc = [], [], None
        p = patch.object(line_channel.httpx, "AsyncClient", _FakeClient)
        p.start()
        self.addCleanup(p.stop)
        self.notify = AsyncMock(return_value=True)
        p = patch("hub.notify.notify_admin_line", self.notify)
        p.start()
        self.addCleanup(p.stop)

        async def _create():
            async with db.get_async_engine().begin() as c:
                for md in (F.metadata, C.metadata, R.metadata, L.metadata):
                    await c.run_sync(md.create_all)
        _run(_create())

    def hook(self, rid: str, status: str, secret: str = SECRET, approver: str = "ohno"):
        body = {"type": "UPDATE_STATUS", "app": {"id": "500"},
                "record": {"$id": {"value": rid}, S.F_STATUS: {"value": status}, "更新者": {"value": {"code": approver, "name": "大野"}}}}
        return _client.post(f"{S.PATH}/{secret}", json=body)


class TestWebhookAuth(_Base):
    def test_bad_secret_is_401_and_sends_nothing(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED)
        with _approve_all():
            r = self.hook(rid, S.S_CONFIRMED, secret="wrong")
        self.assertEqual(r.status_code, 401)
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.k.stage_writes(), [])
        self.assertEqual(S.pending_items(), {})

    def test_env_unset_is_404_and_no_kintone_calls(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED)
        with patch.dict(os.environ, {S.ENV_APP_ID: ""}):
            os.environ.pop(S.ENV_APP_ID, None)
            self.assertFalse(S.enabled())
            r = self.hook(rid, S.S_CONFIRMED)
            self.assertEqual(r.status_code, 404)
            self.assertEqual(_run(S.tick()), 0)
            self.assertEqual(_run(S.mirror_scan()), 0)
        self.assertEqual(self.k.calls, [])
        self.assertEqual(_FakeClient.calls, [])

    def test_other_app_or_non_status_event_is_ignored(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED)
        with _approve_all():
            r = _client.post(f"{S.PATH}/{SECRET}", json={"type": "ADD_RECORD", "record": {"$id": {"value": rid}}})
            self.assertEqual(r.json().get("skip"), "not_status_change")
            r = _client.post(f"{S.PATH}/{SECRET}", json={"type": "UPDATE_STATUS", "app": {"id": "999"},
                                                         "record": {"$id": {"value": rid}, S.F_STATUS: {"value": S.S_CONFIRMED}}})
            self.assertEqual(r.json().get("skip"), "other_app")
        self.assertEqual(_FakeClient.calls, [])


class TestConfirmFlow(_Base):
    def test_confirmed_sends_r1_with_link_and_reports(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED)
        self.k.add_stage(2, S.S_WAITING)
        with _approve_all():
            r = self.hook(rid, S.S_CONFIRMED)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(_FakeClient.calls), 1)                       # R-1 のみ（R-2 はまだ）
        text = _texts()[0]
        self.assertIn("A社に対する時効援用", text)
        self.assertIn("https://jikou.example.test/jikou/report/", text)
        for pii in (NAME, BIRTH, ADDR, PHONE, LINE_USER, CASE):
            self.assertNotIn(pii, text)
        # 結果記録=成立・報告書 1 版・写し
        rows = {int(x["slot"]): x for x in _run(F.rows_for_case(CASE))}
        self.assertEqual(rows[1]["result"], F.RESULT_ESTABLISHED)
        self.assertEqual(rows[1]["answered_on"], datetime.date(2026, 10, 1))
        docs = _run(C.list_docs(CASE))
        self.assertEqual([(d["slot"], d["version"]) for d in docs], [(1, 1)])
        self.assertEqual(self.k.status_of(rid), S.S_REPORTED)
        rec = self.k.stage[rid]
        self.assertEqual(rec[S.F_OP_ID]["value"], S.op_id_for(rid, S.S_CONFIRMED))
        self.assertEqual(rec[S.F_REPORT_VERSION]["value"], "1")
        self.assertTrue(rec[S.F_REPORT_SENT_AT]["value"])
        self.notify.assert_not_awaited()
        # 本人専用 URL は PDF を返す・不正 token は 404
        token = text.split("/jikou/report/")[1].split("\n")[0].strip()
        pdf = _client.get(f"{L.PATH}/{token}")
        self.assertEqual(pdf.status_code, 200)
        self.assertTrue(pdf.content.startswith(b"%PDF"))
        self.assertEqual(_client.get(f"{L.PATH}/{'x' * 40}").status_code, 404)

    def test_duplicate_webhooks_send_once(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED)
        self.k.add_stage(2, S.S_WAITING)                                   # 2 社目が未了＝R-2 は送られない（R-1 の 1 回だけを数える）
        with _approve_all():
            for _ in range(10):
                self.assertEqual(self.hook(rid, S.S_CONFIRMED).status_code, 200)
            self.assertEqual(_run(S.process(rid, S.S_CONFIRMED, "ohno")), "already")
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual(len(_run(C.list_docs(CASE))), 1)

    def test_pdf_failure_sends_nothing_and_flags_once(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED)
        with _approve_all(), patch("contract_pdf.docx_to_pdf_bytes", side_effect=RuntimeError("boom")):
            r = self.hook(rid, S.S_CONFIRMED)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.k.status_of(rid), S.S_CONFIRMED)                 # 状態は戻さない・進めない
        rec = self.k.stage[rid]
        self.assertEqual(rec[S.F_ACTION_NEEDED]["value"], [S.ACTION_NEEDED_VALUE])
        self.assertEqual(rec[S.F_ACTION_REASON]["value"], "cert_pdf_failed")
        self.notify.assert_awaited_once()
        self.assertIn("要対応", self.notify.await_args.args[0])
        self.assertIn("cert_pdf_failed", self.notify.await_args.args[0])
        for pii in (NAME, PHONE, LINE_USER):
            self.assertNotIn(pii, self.notify.await_args.args[0])

    def test_unapproved_text_sends_nothing_and_flags(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED)
        with patch.dict(AT.APPROVED_SHA256, {k: AT.sha256_text(DT.text(k)) for k in DT.TEXT_IDS}):   # 報告書は承認・R-1 は未承認
            r = self.hook(rid, S.S_CONFIRMED)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.k.stage[rid][S.F_ACTION_REASON]["value"], "text_unapproved")
        self.notify.assert_awaited_once()

    def test_missing_answered_on_flags(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED, **{S.F_ANSWERED: ""})
        with _approve_all():
            self.hook(rid, S.S_CONFIRMED)
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.k.stage[rid][S.F_ACTION_REASON]["value"], "answered_on_missing")

    def test_grace_cancel_sends_nothing(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED)
        now = datetime.datetime(2026, 10, 21, 3, 0, tzinfo=datetime.timezone.utc)
        with patch.dict(os.environ, {S.ENV_GRACE_MIN: "10"}), _approve_all():
            self.assertTrue(_run(S.on_status_change(rid, S.S_CONFIRMED, "ohno", now=now)))
            self.assertIn((rid, S.S_CONFIRMED), S.pending_items())
            self.assertEqual(_run(S.tick(now + datetime.timedelta(minutes=5))), 0)      # 猶予中
            self.k.stage[rid][S.F_STATUS] = {"value": S.S_WAITING}
            self.assertFalse(_run(S.on_status_change(rid, S.S_WAITING, "ohno", now=now)))  # 取消
            self.assertEqual(S.pending_items(), {})
            self.assertEqual(_run(S.tick(now + datetime.timedelta(minutes=11))), 0)
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(_run(C.list_docs(CASE)), [])

    def test_grace_fires_after_due_and_rechecks_status(self):
        rid = self.k.add_stage(1, S.S_CONFIRMED)
        now = datetime.datetime(2026, 10, 21, 3, 0, tzinfo=datetime.timezone.utc)
        with patch.dict(os.environ, {S.ENV_GRACE_MIN: "10"}), _approve_all():
            _run(S.on_status_change(rid, S.S_CONFIRMED, "ohno", now=now))
            self.assertEqual(_run(S.tick(now + datetime.timedelta(minutes=10))), 1)
        self.assertEqual(len(_FakeClient.calls), 2)                        # 債権者 1 社: R-1 → 全件報告済 → R-2
        self.assertEqual(self.k.status_of(rid), S.S_DONE)

    def test_failed_sends_r3_and_all_done_sends_r2(self):
        r1 = self.k.add_stage(1, S.S_REPORTED, **{S.F_OP_ID: "jikou_stage:x:done"})
        r2 = self.k.add_stage(2, S.S_FAILED)
        with _approve_all():
            self.hook(r2, S.S_FAILED)
        texts = _texts()
        self.assertEqual(len(texts), 2)                                   # R-3 → 全件報告済 → R-2
        self.assertIn("B社に対する時効援用", texts[0])
        self.assertIn("時効の完成を認めない", texts[0])
        self.assertIn("全ての債権者について", texts[1])
        self.assertEqual(self.k.status_of(r2), S.S_DONE)
        self.assertEqual(self.k.status_of(r1), S.S_DONE)
        rows = {int(x["slot"]): x for x in _run(F.rows_for_case(CASE))}
        self.assertEqual(rows[2]["result"], F.RESULT_NOT_ESTABLISHED)
        self.assertEqual(_run(C.list_docs(CASE)), [])                        # 不成立は報告書なし


class TestMirror(_Base):
    def test_creates_records_and_advances_forward_only(self):
        self.assertEqual(_run(S.mirror_case(CASE)), 2)                         # 入金済み → 2 件とも 受任→入金確認
        self.assertEqual(sorted(r[S.F_STATUS]["value"] for r in self.k.stage.values()), [S.S_PAID, S.S_PAID])
        created = [c for c in self.k.calls if c[0] == "create"]
        self.assertEqual(len(created), 2)
        self.assertEqual(created[0][2][S.F_NAME], NAME)                       # 依頼者情報は作成時に転記
        self.assertEqual(created[0][2][S.F_CASE], CASE)
        # 発送日・回答日が記録されたら 発送済→回答待ち。再実行は冪等
        _run(F.ensure_rows(CASE, [(1, "A社"), (2, "B社")]))
        _run(F.set_shipped(CASE, datetime.date(2026, 9, 1), source="human", actor="human"))
        _run(F.record_result(CASE, 1, result=F.RESULT_ESTABLISHED, answered_on=datetime.date(2026, 10, 1), summary=None))
        self.assertEqual(_run(S.mirror_case(CASE)), 2)
        by_slot = {r[S.F_SLOT]["value"]: r for r in self.k.stage.values()}
        self.assertEqual(by_slot["1"][S.F_STATUS]["value"], S.S_WAITING)
        self.assertEqual(by_slot["2"][S.F_STATUS]["value"], S.S_SHIPPED)
        self.assertEqual(by_slot["1"][S.F_ANSWERED]["value"], "2026-10-01")
        self.assertEqual(_run(S.mirror_case(CASE)), 0)
        # 人の状態（時効確認済）には触れない・後退しない
        by_slot["1"][S.F_STATUS] = {"value": S.S_CONFIRMED}
        self.k.app21 = _app21(paid="未入金")
        self.assertEqual(_run(S.mirror_case(CASE)), 0)
        self.assertEqual(by_slot["1"][S.F_STATUS]["value"], S.S_CONFIRMED)
        self.assertEqual(by_slot["2"][S.F_STATUS]["value"], S.S_SHIPPED)

    def test_result_hook_mirrors(self):
        _run(S.on_result_recorded(CASE, _app21()))
        self.assertEqual(len(self.k.stage), 2)

    def test_scan_covers_engaged_cases(self):
        self.assertEqual(_run(S.mirror_scan()), 2)


class TestReportLink(_Base):
    def test_expired_token_is_404(self):
        now = datetime.datetime(2026, 10, 21, tzinfo=datetime.timezone.utc)
        token = _run(L.issue(CASE, "doc-x", now=now))
        self.assertIsNotNone(_run(L.lookup(token, now=now + datetime.timedelta(hours=71))))
        self.assertIsNone(_run(L.lookup(token, now=now + datetime.timedelta(hours=73))))
        self.assertIsNone(_run(L.lookup("bad token!", now=now)))
        self.assertEqual(_client.get(f"{L.PATH}/{token}").status_code, 404)    # doc 不在は 404


if __name__ == "__main__":
    unittest.main()
