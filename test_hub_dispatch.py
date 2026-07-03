"""hub/dispatch.py（/hub/dispatch エンドポイント＋ディスパッチャ）のテスト（T1-2）

- token 認証・recordId 抽出・ステータス別振り分け
- 下書き→prepare→承認待ち+弁護士通知 / 承認済→claim→発送処理中→dispatch→発送済(/返送待ち)
- ██ 二重 Webhook で実行1回になる冪等テスト ██
- prepare/dispatch 失敗のエラー遷移＋警報・未対応チャネルの警報のみ
"""

import copy
import os
import unittest
from unittest.mock import AsyncMock, patch

# main import 前に環境変数を差し込む（既存テストと同じ流儀）
# ANTHROPIC_API_KEY は main の import にのみ必要。ダミーを環境に残すと
# test_triage_classification の skipUnless ガードが誤解除されるため、
# import 後に「自分が入れたダミーの場合だけ」取り除く（実キーは温存）。
_DUMMY_ANTHROPIC_KEY = "dummy_key_for_import_only"
os.environ.setdefault("ANTHROPIC_API_KEY", _DUMMY_ANTHROPIC_KEY)
os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "KINTONE_WEBHOOK_TOKEN": "approve_token",
    "DOCUMENT_WEBHOOK_SECRET": "doc_secret",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "APP_APPROVAL": "29",
    "TOKEN_APPROVAL": "dummy",
    "APP_SHIPPING": "30",
    "TOKEN_SHIPPING": "dummy",
    "HUB_WEBHOOK_TOKEN": "hub_token",
    "ATTORNEY_LINE_USER_ID": "U_attorney",
    "HEALTHCHECK_DISABLED": "1",
})

from fastapi.testclient import TestClient  # noqa: E402

import channels  # noqa: E402
import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]  # skip ガードの誤解除防止（上記コメント参照）
from channels.base import Artifact, ChannelAdapter, DispatchResult, PrepareResult  # noqa: E402
from hub import kintone  # noqa: E402

client = TestClient(main.app)
URL = "/hub/dispatch?token=hub_token"


class FakeStore:
    """kintone の代役: レコード保持・revision 楽観ロックを再現"""

    def __init__(self, records):
        self.records = {rid: copy.deepcopy(r) for rid, r in records.items()}
        self.updates = []
        self.uploaded = []

    async def get_record(self, app, record_id):
        if record_id not in self.records:
            raise kintone.KintoneError(404, "GAIA_RE01", "not found")
        return copy.deepcopy(self.records[record_id])

    async def update_record(self, app, record_id, fields, revision=None):
        rec = self.records[record_id]
        cur = int(rec["$revision"]["value"])
        if revision is not None and int(revision) != cur:
            raise kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        for k, v in fields.items():
            rec[k] = {"value": v}
        rec["$revision"] = {"value": str(cur + 1)}
        self.updates.append((record_id, dict(fields)))

    async def upload_file(self, app, filename, content, mime):
        self.uploaded.append(filename)
        return f"fk_{len(self.uploaded)}"


def make_record(status, channel="送付案内", executed="no"):
    return {
        "$id": {"value": "9"}, "$revision": {"value": "1"},
        "発送ステータス": {"value": status},
        "チャネル": {"value": channel},
        "件名": {"value": "テスト件名"},
        "顧客名表示用": {"value": "山田太郎"},
        "実行済み": {"value": executed},
    }


class FakeAdapter(ChannelAdapter):
    channel_name = "送付案内"
    needs_return = False

    def __init__(self, manual=False, needs_return=False,
                 prepare_error=None, dispatch_error=None):
        self.manual = manual
        self.needs_return = needs_return
        self.prepare_error = prepare_error
        self.dispatch_error = dispatch_error
        self.prepare_calls = 0
        self.dispatch_calls = 0

    async def prepare(self, record):
        self.prepare_calls += 1
        if self.prepare_error:
            raise self.prepare_error
        return PrepareResult(artifacts=[Artifact("案内.docx", b"PK\x03\x04data")],
                             fields={"宛先名": "テスト宛先"})

    async def dispatch(self, record):
        self.dispatch_calls += 1
        if self.dispatch_error:
            raise self.dispatch_error
        return DispatchResult(manual_mailing=self.manual,
                              fields={"チャネル固有データ": '{"job_id": "j1"}'})


class DispatchTestBase(unittest.TestCase):
    def run_with(self, store, adapter, body=None):
        """レジストリ・kintone・通知を差し替えて POST し、通知モックを返す"""
        body = body or {"record": {"$id": {"value": "9"}}}
        notify_admin = AsyncMock()
        notify_attorney = AsyncMock()
        registry = {adapter.channel_name: adapter} if adapter else {}
        with patch.dict(channels.CHANNEL_REGISTRY, registry, clear=True), \
             patch("hub.kintone.get_record", new=store.get_record), \
             patch("hub.kintone.update_record", new=store.update_record), \
             patch("hub.kintone.upload_file", new=store.upload_file), \
             patch("hub.notify.notify_admin_line", new=notify_admin), \
             patch("hub.notify.notify_attorney_approval", new=notify_attorney):
            resp = client.post(URL, json=body)
        return resp, notify_admin, notify_attorney


class TestEndpointAuth(DispatchTestBase):
    def test_wrong_token_404(self):
        resp = client.post("/hub/dispatch?token=wrong", json={})
        self.assertEqual(resp.status_code, 404)

    def test_missing_token_404(self):
        resp = client.post("/hub/dispatch", json={})
        self.assertEqual(resp.status_code, 404)

    def test_invalid_json_400(self):
        resp = client.post(URL, content=b"not-json")
        self.assertEqual(resp.status_code, 400)

    def test_no_record_id_skips(self):
        resp = client.post(URL, json={"record": {}})
        self.assertEqual(resp.json(), {"ok": True, "skip": "no_record_id"})


class TestPrepareFlow(DispatchTestBase):
    def test_draft_prepares_attaches_and_requests_approval(self):
        store = FakeStore({"9": make_record("下書き")})
        adapter = FakeAdapter()
        resp, _, notify_attorney = self.run_with(store, adapter)

        self.assertEqual(resp.json(), {"ok": True, "queued": "9"})
        self.assertEqual(adapter.prepare_calls, 1)
        self.assertEqual(store.uploaded, ["案内.docx"])
        rec = store.records["9"]
        self.assertEqual(rec["発送ステータス"]["value"], "承認待ち")
        self.assertEqual(rec["成果物"]["value"], [{"fileKey": "fk_1"}])
        self.assertEqual(rec["宛先名"]["value"], "テスト宛先")
        notify_attorney.assert_awaited_once()

    def test_prepare_failure_goes_to_error_with_alert(self):
        store = FakeStore({"9": make_record("下書き")})
        adapter = FakeAdapter(prepare_error=RuntimeError("template broken"))
        _, notify_admin, notify_attorney = self.run_with(store, adapter)

        rec = store.records["9"]
        self.assertEqual(rec["発送ステータス"]["value"], "エラー")
        self.assertIn("template broken", rec["エラー詳細"]["value"])
        notify_admin.assert_awaited()
        notify_attorney.assert_not_awaited()

    def test_unknown_channel_alerts_without_state_change(self):
        store = FakeStore({"9": make_record("下書き", channel="FAX")})
        adapter = FakeAdapter()  # 登録されるのは 送付案内 のみ
        _, notify_admin, _ = self.run_with(store, adapter)

        self.assertEqual(store.records["9"]["発送ステータス"]["value"], "下書き")
        self.assertEqual(store.updates, [])
        notify_admin.assert_awaited_once()
        self.assertIn("未対応チャネル", notify_admin.await_args.args[0])


class TestDispatchFlow(DispatchTestBase):
    def test_approved_auto_channel_ships(self):
        store = FakeStore({"9": make_record("承認済")})
        adapter = FakeAdapter(manual=False)
        self.run_with(store, adapter)

        rec = store.records["9"]
        self.assertEqual(adapter.dispatch_calls, 1)
        self.assertEqual(rec["実行済み"]["value"], "yes")
        self.assertEqual(rec["発送ステータス"]["value"], "発送済")
        self.assertIn("発送日時", rec)
        self.assertEqual(rec["チャネル固有データ"]["value"], '{"job_id": "j1"}')

    def test_needs_return_goes_to_waiting(self):
        store = FakeStore({"9": make_record("承認済")})
        adapter = FakeAdapter(needs_return=True)
        self.run_with(store, adapter)
        self.assertEqual(store.records["9"]["発送ステータス"]["value"], "返送待ち")

    def test_manual_mailing_stays_processing_and_notifies(self):
        store = FakeStore({"9": make_record("承認済")})
        adapter = FakeAdapter(manual=True)
        _, notify_admin, _ = self.run_with(store, adapter)

        rec = store.records["9"]
        self.assertEqual(rec["発送ステータス"]["value"], "発送処理中")
        texts = [c.args[0] for c in notify_admin.await_args_list]
        self.assertTrue(any("印刷・投函" in t for t in texts))

    def test_dispatch_failure_goes_to_error(self):
        store = FakeStore({"9": make_record("承認済")})
        adapter = FakeAdapter(dispatch_error=RuntimeError("fax api down"))
        _, notify_admin, _ = self.run_with(store, adapter)

        rec = store.records["9"]
        self.assertEqual(rec["発送ステータス"]["value"], "エラー")
        self.assertIn("fax api down", rec["エラー詳細"]["value"])
        notify_admin.assert_awaited()


class TestIdempotency(DispatchTestBase):
    """██ 二重 Webhook で実行が1回になること（T1-2 完了条件）██"""

    def test_double_webhook_executes_once(self):
        store = FakeStore({"9": make_record("承認済")})
        adapter = FakeAdapter()
        self.run_with(store, adapter)   # 1回目: 実行される
        self.run_with(store, adapter)   # 2回目: 最新取得で発送済→skip
        self.assertEqual(adapter.dispatch_calls, 1)

    def test_already_claimed_record_is_skipped(self):
        """状態は承認済のままだが実行済み=yes（クラッシュ後の再送等）→ 実行しない"""
        store = FakeStore({"9": make_record("承認済", executed="yes")})
        adapter = FakeAdapter()
        self.run_with(store, adapter)
        self.assertEqual(adapter.dispatch_calls, 0)
        self.assertEqual(store.records["9"]["発送ステータス"]["value"], "承認済")

    def test_revision_conflict_skips_execution(self):
        """claim の revision 競合（並行プロセスが先取り）→ 実行しない"""
        store = FakeStore({"9": make_record("承認済")})
        adapter = FakeAdapter()

        orig_update = store.update_record

        async def racing_update(app, record_id, fields, revision=None):
            if fields == {"実行済み": "yes"}:
                raise kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
            return await orig_update(app, record_id, fields, revision=revision)

        store.update_record = racing_update
        self.run_with(store, adapter)
        self.assertEqual(adapter.dispatch_calls, 0)


class TestStatusRouting(DispatchTestBase):
    def test_other_statuses_are_skipped(self):
        """サーバー処理対象外の状態（承認待ち・完了等）では何も書かない
        ※発送済は T3-3 で処理対象になったため一覧から除外（返送待ち/完了への自動遷移。
        　T2-2 実装ノートで予告済み・分岐テストは test_shokumu_wiring.py）"""
        for status in ("承認待ち", "発送処理中", "返送待ち", "完了", "エラー", "却下"):
            with self.subTest(status=status):
                store = FakeStore({"9": make_record(status)})
                adapter = FakeAdapter()
                self.run_with(store, adapter)
                self.assertEqual(store.updates, [], f"status={status} で書き込みが発生")
                self.assertEqual(adapter.prepare_calls, 0)
                self.assertEqual(adapter.dispatch_calls, 0)

    def test_record_not_found_is_silent_skip(self):
        store = FakeStore({})
        adapter = FakeAdapter()
        resp, notify_admin, _ = self.run_with(store, adapter)
        self.assertEqual(resp.status_code, 200)

    def test_reprocess_called_for_needs_review(self):
        store = FakeStore({"9": make_record("要確認")})

        class ReprocessAdapter(FakeAdapter):
            def __init__(self):
                super().__init__()
                self.reprocessed = 0

            async def reprocess(self, record):
                self.reprocessed += 1

        adapter = ReprocessAdapter()
        self.run_with(store, adapter)
        self.assertEqual(adapter.reprocessed, 1)


if __name__ == "__main__":
    unittest.main()
