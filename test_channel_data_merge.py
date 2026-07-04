"""チャネル固有データのマージ書き戻し（2026-07-04・作業記録の未処理事項6）

事象: 指示Bot起票時の監査メタ（dispatch_bot キー）が、prepare のチャネル側
書き戻し（全置換）で消える（App 30 No.5 で確認）。

固定内容:
- 共通経路（hub/dispatch のフィールド書き戻し）で shallow merge:
  既存 JSON のトップレベルキー（dispatch_bot 等）を保持し、同名キーは新値優先
- GUI 起票（既存メタなし・既存が空/不正JSON）の従来挙動は不変
- prepare / dispatch 両経路で効く（チャネル個別実装に依存しない）
"""

import copy
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("APP_SHIPPING", "30")
os.environ.setdefault("TOKEN_SHIPPING", "dummy")

import channels  # noqa: E402
from channels.base import Artifact, ChannelAdapter, DispatchResult, PrepareResult  # noqa: E402
from hub import dispatch, kintone  # noqa: E402

DISPATCH_BOT_META = {"dispatch_bot": {
    "指示原文": "テスト太郎に送付案内送って", "userId": "U_owner",
    "解釈日時": "2026-07-04T17:27:18+09:00",
    "pending_command_id": "707c1222-8afe-4883-af69-cb27c12b00fd"}}

CHANNEL_PAYLOAD = {"blocks": ["委任契約書"], "needs_return": True,
                   "ai_note": {"generated": False}}


def record(channel_data=""):
    return {"$id": {"value": "9"}, "$revision": {"value": "1"},
            "発送ステータス": {"value": "下書き"},
            "チャネル": {"value": "テストチャネル"},
            "ユニット種別": {"value": "時効援用"},
            "件名": {"value": "件名"}, "顧客名表示用": {"value": "太郎"},
            "実行済み": {"value": "no"},
            "チャネル固有データ": {"value": channel_data}}


class MergeUnitTest(unittest.TestCase):
    """_merge_channel_data 単体（共通ヘルパー）"""

    def merge(self, existing, new_fields):
        return dispatch._merge_channel_data(record(existing), new_fields)

    def test_dispatch_bot_meta_preserved(self):
        existing = json.dumps(DISPATCH_BOT_META, ensure_ascii=False)
        fields = {"チャネル固有データ": json.dumps(CHANNEL_PAYLOAD, ensure_ascii=False)}
        merged = json.loads(self.merge(existing, fields)["チャネル固有データ"])
        self.assertEqual(merged["dispatch_bot"], DISPATCH_BOT_META["dispatch_bot"],
                         "監査メタが保持される")
        self.assertEqual(merged["blocks"], ["委任契約書"], "チャネル側データも入る")

    def test_same_key_new_value_wins(self):
        existing = json.dumps({"dispatch_bot": {"a": 1}, "kogawase_total": 100})
        fields = {"チャネル固有データ": json.dumps({"kogawase_total": 1200})}
        merged = json.loads(self.merge(existing, fields)["チャネル固有データ"])
        self.assertEqual(merged["kogawase_total"], 1200, "同名キーは新値優先")
        self.assertIn("dispatch_bot", merged)

    def test_empty_existing_returns_channel_payload_unchanged(self):
        """GUI起票（既存メタなし）: 出力はチャネル側の値そのまま（従来挙動）"""
        payload = json.dumps(CHANNEL_PAYLOAD, ensure_ascii=False)
        fields = {"チャネル固有データ": payload}
        self.assertEqual(self.merge("", fields)["チャネル固有データ"], payload)
        self.assertEqual(self.merge("{}", fields)["チャネル固有データ"], payload)

    def test_broken_existing_json_falls_back(self):
        payload = json.dumps(CHANNEL_PAYLOAD, ensure_ascii=False)
        fields = {"チャネル固有データ": payload}
        self.assertEqual(self.merge("{壊れたjson", fields)["チャネル固有データ"], payload)

    def test_no_channel_data_field_untouched(self):
        fields = {"宛先名": "太郎"}
        self.assertEqual(self.merge(json.dumps(DISPATCH_BOT_META), fields), fields)

    def test_other_fields_pass_through(self):
        existing = json.dumps(DISPATCH_BOT_META)
        fields = {"宛先名": "太郎", "チャネル固有データ": json.dumps({"x": 1})}
        out = self.merge(existing, fields)
        self.assertEqual(out["宛先名"], "太郎")


class _PrepareAdapter(ChannelAdapter):
    """M4 と同じ「チャネル固有データ全置換」パターンのアダプタ"""
    channel_name = "テストチャネル"

    async def prepare(self, record):
        return PrepareResult(
            artifacts=[Artifact("成果物.pdf", b"%PDF-test")],
            fields={"チャネル固有データ": json.dumps(CHANNEL_PAYLOAD, ensure_ascii=False)})

    async def dispatch(self, record):
        return DispatchResult(manual_mailing=True,
                              fields={"チャネル固有データ": json.dumps({"job_id": "j1"})})


class DispatcherIntegration(unittest.IsolatedAsyncioTestCase):
    """ディスパッチャ経由（prepare / dispatch 両経路）での実挙動"""

    def run_dispatch(self, rec):
        store = {"9": copy.deepcopy(rec)}
        updates = []

        async def fake_get(app, rid):
            return copy.deepcopy(store[rid])

        async def fake_update(app, rid, fields, revision=None):
            for k, v in fields.items():
                store[rid][k] = {"value": v}
            store[rid]["$revision"] = {"value": str(int(store[rid]["$revision"]["value"]) + 1)}
            updates.append(dict(fields))

        return store, updates, [
            patch.dict(channels.CHANNEL_REGISTRY,
                       {"テストチャネル": _PrepareAdapter()}, clear=True),
            patch("hub.kintone.get_record", new=fake_get),
            patch("hub.kintone.update_record", new=fake_update),
            patch("hub.kintone.upload_file", new=AsyncMock(return_value="fk1")),
            patch("hub.notify.notify_admin_line", new=AsyncMock()),
            patch("hub.notify.notify_attorney_approval", new=AsyncMock()),
        ]

    async def test_bot_meta_survives_prepare(self):
        """指示Bot起票→prepare 後も dispatch_bot メタが残る（不具合の直接再現）"""
        rec = record(json.dumps(DISPATCH_BOT_META, ensure_ascii=False))
        store, updates, ps = self.run_dispatch(rec)
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            await dispatch.process_dispatch("9")
        self.assertEqual(store["9"]["発送ステータス"]["value"], "承認待ち")
        data = json.loads(store["9"]["チャネル固有データ"]["value"])
        self.assertEqual(data["dispatch_bot"]["pending_command_id"],
                         "707c1222-8afe-4883-af69-cb27c12b00fd", "監査メタ保持")
        self.assertEqual(data["blocks"], ["委任契約書"], "チャネル側書き戻しも反映")
        self.assertEqual(data["needs_return"], True)

    async def test_gui_filing_without_meta_unchanged(self):
        """GUI起票（メタなし）: 書き込まれる JSON はチャネル側の値と完全一致（従来挙動）"""
        store, updates, ps = self.run_dispatch(record(""))
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            await dispatch.process_dispatch("9")
        self.assertEqual(store["9"]["チャネル固有データ"]["value"],
                         json.dumps(CHANNEL_PAYLOAD, ensure_ascii=False))

    async def test_meta_survives_dispatch_writeback(self):
        """承認済→dispatch の書き戻し（job_id 等）でもメタが残る"""
        rec = record(json.dumps(DISPATCH_BOT_META, ensure_ascii=False))
        rec["発送ステータス"] = {"value": "承認済"}
        store, updates, ps = self.run_dispatch(rec)
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            await dispatch.process_dispatch("9")
        data = json.loads(store["9"]["チャネル固有データ"]["value"])
        self.assertIn("dispatch_bot", data)
        self.assertEqual(data["job_id"], "j1")


class RealChannelPattern(unittest.TestCase):
    def test_shokumu_seikyu_already_merges(self):
        """職務上請求は既存JSONを読み込んで追記する方式＝メタ保持を確認（影響範囲の記録）"""
        from channels import shokumu_seikyu as sk
        rec = {"チャネル固有データ": {"value": json.dumps(
            {**DISPATCH_BOT_META,
             "request_items": [{"type": "戸籍謄本", "count": 1}]}, ensure_ascii=False)}}
        data = sk.parse_channel_data(rec)
        self.assertIn("dispatch_bot", data, "parse_channel_data が既存キーを保持する")


if __name__ == "__main__":
    unittest.main()
