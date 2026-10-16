"""JIKOU-FAX-2（QUEUE-2026-10-13-E E-3）: FAX 送信の骨組み（provider 抽象・DryRun・承認必須・台帳が正・実送信なし）

固定する仕様:
- provider は env JIKOU_FAX_PROVIDER（未設定→dryrun・閉集合外→UnknownProvider＝送らない）。実在サービスの実装なし。
- 投入（hub/jikou_fax.enqueue）は署名付き Approval（approve()）以外を NotApproved で拒む（台帳に行を作らない）。
  承認の口は /app/fax/{record_id}/send（明示確認・署名付き ticket）だけ。
- DryRun で台帳 1 行（business=jikou・channel=fax・purpose=other・受信イベント ID=fax:{No}:{slot}:{sha16}・sent）。
  同一案件・同一宛先・同一 PDF の 2 回目は skipped（行は増えない）。PDF が変われば別操作。
- 宛先は業者マスタから（FAX送信可=可・番号検証）。送れない理由は閉集合。番号はログ・画面に出さない（下 4 桁のみ）。
- 台帳が使えなければ送らない（provider を呼ばない）。provider 例外は unconfirmed。
- kintone 刻印は写し（欄があるものだけ・無ければ skipped）。
- DryRun・本 module にファイル I/O なし（PDF を保存しない）。sink 違反ゼロ・allowlist 追加ゼロ。
"""

import asyncio
import hashlib
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
}.items():
    os.environ.setdefault(_k, _v)

import sqlalchemy as sa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import notice_webhook  # noqa: E402
from hub import db  # noqa: E402
from hub import fax_adapter as FA  # noqa: E402
from hub import jikou_fax as FX  # noqa: E402
from hub import kintone  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_fax_view as V  # noqa: E402
from hub.webapp_auth import MIN_ITERATIONS, hash_password, issue_session  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
_ENV = {"WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS), "WEBAPP_SESSION_SECRET": "s" * 32,
        "APP_CREDITOR_MASTER": "50", "TOKEN_CREDITOR_MASTER": "t50"}
PDF = b"%PDF-1.4\n%fake notice\n"
PDF2 = b"%PDF-1.4\n%fake notice v2\n"
SHA = hashlib.sha256(PDF).hexdigest()
NUMBER = "03-1234-5678"
E164 = "+81312345678"
RID = "21"
OP = "a" * 32


def _run(coro):
    try:
        return asyncio.run(coro)
    finally:
        db.reset_for_tests()


def _auth():
    return {"Cookie": f"webapp_session={issue_session()}"}


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


def _master(rid: str = "7", name: str = "アコム", fax: str = NUMBER, ok: str = "可"):
    return {"$id": {"value": rid}, "業者名": {"value": name}, "FAX番号": {"value": fax}, "FAX送信可": {"value": ok}}


def _case(files=None):
    files = files if files is not None else [{"fileKey": "fk1", "name": "時効援用通知書_対象債権者1.pdf",
                                               "contentType": "application/pdf"},
                                              {"fileKey": "fk1d", "name": "時効援用通知書_対象債権者1.docx",
                                               "contentType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}]
    return {"$id": {"value": RID}, "$revision": {"value": "3"}, "顧客名": {"value": "山田 太郎"},
            "問い合わせ業者名": {"value": "アコム"}, "対象債権者2": {"value": ""}, "対象債権者3": {"value": "レイク"},
            FX.FIELD_ATTACHMENT: {"value": files}}


class TestPure(unittest.TestCase):
    def test_number_and_keys(self):
        self.assertEqual(FX.normalize_number("03-1234-5678"), E164)
        self.assertEqual(FX.normalize_number("０３（１２３４）５６７８"), E164)
        self.assertEqual(FX.normalize_number("+81 3 1234 5678"), E164)
        self.assertEqual(FX.normalize_number("09012345678"), "+819012345678")
        for bad in ("", "abc", "1234", "312345678", "+1", "03-1234-5678x"):
            self.assertIsNone(FX.normalize_number(bad), bad)
        self.assertEqual(FA.last4(E164), "5678")
        self.assertEqual(FA.last4(""), "")
        self.assertEqual(FX.ledger_key("21", 1, SHA), f"fax:21:1:{SHA[:16]}")
        rec = _case()
        self.assertEqual([FX.creditor_name(rec, s) for s in (1, 2, 3)], ["アコム", "", "レイク"])
        self.assertEqual(FX.pdf_file_for_slot(rec, 1)["fileKey"], "fk1")           # docx は選ばない
        self.assertIsNone(FX.pdf_file_for_slot(rec, 3))
        self.assertEqual(FX.CREDITOR_FIELDS, notice_webhook._CREDITOR_FIELDS)
        self.assertEqual(FX.FIELD_ATTACHMENT, notice_webhook.FIELD_ATTACHMENT)

    def test_provider_selection_and_dryrun(self):
        with patch.dict(os.environ, {FA.PROVIDER_ENV: ""}):
            p = FA.get_provider()
        self.assertIsInstance(p, FA.DryRunProvider)
        with patch.dict(os.environ, {FA.PROVIDER_ENV: "DryRun"}):
            self.assertIsInstance(FA.get_provider(), FA.DryRunProvider)
        with patch.dict(os.environ, {FA.PROVIDER_ENV: "interfax"}):
            with self.assertRaises(FA.UnknownProvider):
                FA.get_provider()
        with self.assertLogs("hub.fax_adapter", level="INFO") as cm:
            aid = asyncio.run(p.send(PDF, E164, {"record_id": RID, "slot": 1}))
        self.assertEqual(aid, f"dryrun:{SHA[:16]}")
        self.assertEqual(p.accepted, 1)
        joined = "\n".join(cm.output)
        self.assertIn("[FAX_DRYRUN] accepted", joined)
        for bad in (E164, NUMBER, "5678", SHA):                    # 番号は下 4 桁も含めてログに出ない（emit freetext=非表示）
            self.assertNotIn(bad, joined)

    def test_approval_signature(self):
        with patch.dict(os.environ, {"WEBAPP_SESSION_SECRET": ""}):
            self.assertIsNone(FX.approve(RID, 1, SHA, "7", OP))                   # 鍵なし＝承認不能
        with patch.dict(os.environ, {"WEBAPP_SESSION_SECRET": "s" * 32}):
            a = FX.approve(RID, 1, SHA, "7", OP)
            self.assertTrue(FX.is_approved(a))
            self.assertIsNone(FX.approve(RID, 4, SHA, "7", OP))
            self.assertIsNone(FX.approve(RID, 1, "x", "7", OP))
            forged = FX.Approval(RID, 2, SHA, "7", OP, a.signature)                 # 束縛を変えると不一致
            self.assertFalse(FX.is_approved(forged))
            self.assertFalse(FX.is_approved(FX.Approval(RID, 1, SHA, "7", OP, "")))
            self.assertFalse(FX.is_approved(None))
            self.assertFalse(FX.is_approved({"record_id": RID}))

    def test_stamp_values_only_available_fields(self):
        have = frozenset({"FAX送信状態_1", "FAX送信ID_1"})
        v = FX.stamp_values(1, FX.OUTCOME_SENT, "dryrun:abc", have)
        self.assertEqual(v, {"FAX送信状態_1": "送達待ち", "FAX送信ID_1": "dryrun:abc"})
        self.assertEqual(FX.stamp_values(2, FX.OUTCOME_SENT, "x", have), {})
        self.assertEqual(FX.stamp_values(1, FX.OUTCOME_FAILED, "", frozenset({"FAX送信状態_1"})), {"FAX送信状態_1": "失敗"})


class _Mixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="fax_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", **_ENV})
        self._env.start()
        os.environ.pop(FA.PROVIDER_ENV, None)
        db.reset_for_tests()
        FX.clear_cache()

        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(sl.metadata.create_all)
        _run(_create())
        self.search = AsyncMock(return_value=[_master()])
        self.get_record = AsyncMock(return_value=_case())
        self.download = AsyncMock(return_value=PDF)
        self.form = AsyncMock(return_value={"FAX送信状態_1": {"type": "DROP_DOWN"}, "FAX送信日時_1": {"type": "DATETIME"},
                                            "FAX送信ID_1": {"type": "SINGLE_LINE_TEXT"}, "顧客名": {"type": "SINGLE_LINE_TEXT"}})
        self.update = AsyncMock(return_value=None)
        self._p = [patch.object(kintone, "search_records", self.search), patch.object(kintone, "get_record", self.get_record),
                   patch.object(kintone, "download_file", self.download), patch.object(kintone, "get_form_fields", self.form),
                   patch.object(kintone, "update_record", self.update)]
        for p in self._p:
            p.start()

    def tearDown(self):
        for p in self._p:
            p.stop()
        FX.clear_cache()
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def ops(self) -> list:
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(sl.send_operation))).fetchall()]
        return _run(_q())

    def dest(self) -> FX.Destination:
        return _run(FX.resolve_destination("アコム"))

    def approval(self, pdf: bytes = PDF, op: str = OP, master: str = "7") -> FX.Approval:
        return FX.approve(RID, 1, hashlib.sha256(pdf).hexdigest(), master, op)


class TestEnqueue(_Mixin):
    def test_without_approval_raises_and_writes_nothing(self):
        d = self.dest()
        for bad in (None, {"record_id": RID}, FX.Approval(RID, 1, SHA, "7", OP, ""), FX.Approval(RID, 1, SHA, "7", OP, "f" * 64)):
            with self.assertRaises(FX.NotApproved):
                _run(FX.enqueue(bad, PDF, d))
        with self.assertRaises(FX.NotApproved):
            _run(FX.enqueue(self.approval(), PDF2, d))                   # 承認した PDF と違う
        with self.assertRaises(FX.NotApproved):
            _run(FX.enqueue(self.approval(master="8"), PDF, d))          # 承認した宛先と違う
        self.assertEqual(self.ops(), [])

    def test_dryrun_records_one_row_and_second_is_skipped(self):
        d = self.dest()
        self.assertEqual((d.master_record_id, d.number, d.last4), ("7", E164, "5678"))
        with self.assertLogs("hub.jikou_fax", level="INFO") as cm:
            res = _run(FX.enqueue(self.approval(), PDF, d))
        self.assertEqual(res, {"outcome": FX.OUTCOME_SENT, "accept_id": f"dryrun:{SHA[:16]}", "last4": "5678",
                               "op_id": res["op_id"]})
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        o = ops[0]
        self.assertEqual((o["business"], o["channel"], o["purpose"], o["inbound_event_id"], o["state"], o["actor"]),
                         ("jikou", "fax", sl.PURPOSE_OTHER, f"fax:{RID}:1:{SHA[:16]}", sl.STATE_SENT, sl.ACTOR_BOT))
        self.assertEqual(o["text_version"], sl.text_version(SHA))
        self.assertNotIn(E164, str(o))
        self.assertNotIn(NUMBER, str(o))
        joined = "\n".join(cm.output)
        self.assertIn("[JIKOU_FAX] accepted", joined)
        for bad in (E164, NUMBER, "5678"):
            self.assertNotIn(bad, joined)
        # 2 回目（別の op_id でも同一案件・同一宛先・同一 PDF）は skipped・行は増えない
        res2 = _run(FX.enqueue(self.approval(op="b" * 32), PDF, d))
        self.assertEqual(res2["outcome"], FX.OUTCOME_SKIPPED)
        self.assertEqual(len(self.ops()), 1)
        # PDF が変われば別操作
        res3 = _run(FX.enqueue(self.approval(pdf=PDF2), PDF2, d))
        self.assertEqual(res3["outcome"], FX.OUTCOME_SENT)
        self.assertEqual(len(self.ops()), 2)
        self.assertTrue(all(o["inbound_event_id"].startswith(f"fax:{RID}:1:") for o in self.ops()))

    def test_ledger_unavailable_does_not_call_provider(self):
        d = self.dest()
        spy = FA.DryRunProvider()
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True), patch.object(FA, "get_provider", lambda: spy):
            db.reset_for_tests()
            res = _run(FX.enqueue(self.approval(), PDF, d))
        self.assertEqual(res["outcome"], FX.OUTCOME_LEDGER_UNAVAILABLE)
        self.assertEqual(spy.accepted, 0)

    def test_provider_exception_is_unconfirmed_and_unknown_provider_sends_nothing(self):
        d = self.dest()

        class _Boom(FA.DryRunProvider):
            async def send(self, pdf_bytes, to_number, meta):
                raise RuntimeError("x")
        with patch.object(FA, "get_provider", lambda: _Boom()):
            res = _run(FX.enqueue(self.approval(), PDF, d))
        self.assertEqual(res["outcome"], FX.OUTCOME_UNCONFIRMED)
        self.assertEqual(self.ops()[0]["state"], sl.STATE_UNCONFIRMED)
        with patch.dict(os.environ, {FA.PROVIDER_ENV: "efax"}):
            with self.assertRaises(FA.UnknownProvider):
                _run(FX.enqueue(self.approval(pdf=PDF2), PDF2, d))
        self.assertEqual(len(self.ops()), 1)

    def test_destination_fail_closed_reasons(self):
        with patch.dict(os.environ, {"APP_CREDITOR_MASTER": ""}):
            with self.assertRaises(FX.NotSendable) as c:
                self.dest()
            self.assertEqual(c.exception.reason, FX.REASON_MASTER_UNSET)
        cases = [([], FX.REASON_NOT_FOUND), ([_master(), _master("8")], FX.REASON_AMBIGUOUS),
                 ([_master(ok="未確認")], FX.REASON_FAX_NOT_ALLOWED), ([_master(fax="1234")], FX.REASON_INVALID_NUMBER)]
        for rows, reason in cases:
            self.search.return_value = rows
            with self.assertRaises(FX.NotSendable) as c:
                self.dest()
            self.assertEqual(c.exception.reason, reason)
        self.search.side_effect = kintone.KintoneError(500, "x", "y")
        with self.assertRaises(FX.NotSendable) as c:
            self.dest()
        self.assertEqual(c.exception.reason, FX.REASON_KINTONE)
        self.search.side_effect = None
        self.search.return_value = [_master()]
        with self.assertRaises(FX.NotSendable) as c:
            _run(FX.resolve_destination(""))
        self.assertEqual(c.exception.reason, FX.REASON_NOT_FOUND)
        self.assertIn('業者名 = "アコム"', self.search.call_args.args[1])

    def test_fetch_pdf_fail_closed(self):
        self.assertEqual(_run(FX.fetch_pdf(_case(), 1)), PDF)
        with self.assertRaises(FX.NotSendable) as c:
            _run(FX.fetch_pdf(_case(), 3))
        self.assertEqual(c.exception.reason, FX.REASON_PDF_NOT_FOUND)
        self.download.return_value = b"PK\x03\x04docx"
        with self.assertRaises(FX.NotSendable) as c:
            _run(FX.fetch_pdf(_case(), 1))
        self.assertEqual(c.exception.reason, FX.REASON_PDF_INVALID)

    def test_stamp_is_copy_only_available_fields(self):
        self.assertEqual(_run(FX.stamp_kintone(RID, 1, FX.OUTCOME_SENT, "dryrun:abc")), "written")
        fields = self.update.call_args.args[2]
        self.assertEqual(set(fields), {"FAX送信状態_1", "FAX送信日時_1", "FAX送信ID_1"})
        self.assertEqual(fields["FAX送信状態_1"], "送達待ち")
        self.assertEqual(self.update.call_args.args[1], RID)
        self.assertEqual(_run(FX.stamp_kintone(RID, 2, FX.OUTCOME_SENT, "x")), "skipped")      # slot 2 の欄なし
        self.assertEqual(_run(FX.stamp_kintone(RID, 1, FX.OUTCOME_SKIPPED, "x")), "skipped")
        self.assertEqual(self.update.call_count, 1)
        self.assertEqual(self.form.call_count, 1)                                              # キャッシュ


class TestView(_Mixin):
    def test_requires_auth(self):
        self.assertIn(_client.get(f"{V.PAGE}/{RID}", follow_redirects=False).status_code, (302, 303, 401))

    def test_page_shows_last4_only_and_send_flow(self):
        r = _client.get(f"{V.PAGE}/{RID}", headers=_auth())
        self.assertEqual(r.status_code, 200)
        body = r.text
        self.assertIn("アコム", body)
        self.assertIn("****5678", body)
        self.assertIn("レイク", body)
        self.assertIn('通知書 PDF: <span class="bad">なし</span>', body)      # slot 3 は PDF なし（フォームも出ない）
        for bad in (NUMBER, E164, "山田"):
            self.assertNotIn(bad, body)
        import re
        m = re.search(r'name="op_id" value="([0-9a-f]{32})"', body)
        t = re.search(r'name="ticket" value="([^"]+)"', body)
        self.assertTrue(m and t)
        op_id, ticket = m.group(1), t.group(1)
        self.assertEqual(body.count('name="ticket"'), 1)             # 送れるスロットだけにフォーム
        # 確認なし → 送らない
        r = _client.post(f"{V.PAGE}/{RID}/send", data={"slot": "1", "op_id": op_id, "ticket": ticket},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual((r.status_code, r.headers["location"]), (303, f"{V.PAGE}/{RID}?done=confirm_required"))
        self.assertEqual(self.ops(), [])
        # 改竄 ticket → 400
        r = _client.post(f"{V.PAGE}/{RID}/send", data={"slot": "1", "op_id": op_id, "ticket": ticket[:-1] + "0", "confirm": "1"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.ops(), [])
        # 正常
        with self.assertLogs("hub.webapp_fax_view", level="INFO") as cm:
            r = _client.post(f"{V.PAGE}/{RID}/send", data={"slot": "1", "op_id": op_id, "ticket": ticket, "confirm": "1"},
                             headers=_auth(), follow_redirects=False)
        self.assertEqual((r.status_code, r.headers["location"]), (303, f"{V.PAGE}/{RID}?done=sent"))
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["channel"], ops[0]["state"]), ("fax", sl.STATE_SENT))
        self.assertEqual(self.update.call_count, 1)
        self.assertEqual(self.update.call_args.args[2]["FAX送信ID_1"], f"dryrun:{SHA[:16]}")
        self.assertNotIn(E164, "\n".join(cm.output))
        # 同じ ticket で再送 → skipped（行は増えない・刻印しない）
        r = _client.post(f"{V.PAGE}/{RID}/send", data={"slot": "1", "op_id": op_id, "ticket": ticket, "confirm": "1"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{RID}?done=skipped")
        self.assertEqual(len(self.ops()), 1)
        self.assertEqual(self.update.call_count, 1)
        # 宛先がマスタで変わった → stale
        self.search.return_value = [_master(rid="9")]
        r = _client.post(f"{V.PAGE}/{RID}/send", data={"slot": "1", "op_id": op_id, "ticket": ticket, "confirm": "1"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{RID}?done=stale")
        self.assertEqual(len(self.ops()), 1)

    def test_unknown_provider_and_bad_input(self):
        body = _client.get(f"{V.PAGE}/{RID}", headers=_auth()).text
        import re
        op_id = re.search(r'name="op_id" value="([0-9a-f]{32})"', body).group(1)
        ticket = re.search(r'name="ticket" value="([^"]+)"', body).group(1)
        with patch.dict(os.environ, {FA.PROVIDER_ENV: "efax"}):
            r = _client.post(f"{V.PAGE}/{RID}/send", data={"slot": "1", "op_id": op_id, "ticket": ticket, "confirm": "1"},
                             headers=_auth(), follow_redirects=False)
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{RID}?done=provider")
        self.assertEqual(self.ops(), [])
        for data in ({"slot": "4", "op_id": op_id, "ticket": ticket, "confirm": "1"},
                     {"slot": "1", "op_id": "zz", "ticket": ticket, "confirm": "1"}):
            r = _client.post(f"{V.PAGE}/{RID}/send", data=data, headers=_auth(), follow_redirects=False)
            self.assertEqual(r.status_code, 400)
        self.assertEqual(_client.get(f"{V.PAGE}/abc", headers=_auth()).status_code, 400)


class TestPins(unittest.TestCase):
    def test_sink_allowlist_wiring_and_no_file_io(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        srcs = {}
        for name in ("hub/fax_adapter.py", "hub/jikou_fax.py", "hub/webapp_fax_view.py"):
            srcs[name] = (REPO / name).read_text(encoding="utf-8")
            self.assertEqual(scan_source(srcs[name], name), [], name)
            self.assertNotIn(Path(name).stem, allow)
        for name in ("hub/fax_adapter.py", "hub/jikou_fax.py"):
            body = srcs[name].split('"""', 2)[2]
            for forbidden in ("open(", "write_bytes", "write_text", "tempfile", "shutil", "Path("):
                self.assertNotIn(forbidden, body, f"{name}: {forbidden}")
        # 投入の口は承認画面だけ（enqueue 呼出は webapp_fax_view の 1 か所）
        callers = 0
        for p in list((REPO / "hub").glob("*.py")) + [REPO / "main.py", REPO / "chat_responder.py"]:
            callers += p.read_text(encoding="utf-8").count("jikou_fax.enqueue(")
        self.assertEqual(callers, 1)
        self.assertEqual(srcs["hub/webapp_fax_view.py"].count("jikou_fax.enqueue("), 1)
        msrc = (REPO / "main.py").read_text(encoding="utf-8")
        self.assertIn("from hub.webapp_fax_view import router as fax_view_router", msrc)
        self.assertEqual(FA.PROVIDERS, ("dryrun",))
        self.assertEqual(sl.PURPOSES[-1], "other")                 # purpose 閉集合は不変（fax は other+接頭辞）
        self.assertEqual(FX.LEDGER_KIND, "fax")


if __name__ == "__main__":
    unittest.main()
