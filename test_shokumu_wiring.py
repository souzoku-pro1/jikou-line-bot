"""T3-3: 職務上請求チャネルの結線テスト

- CHANNEL_REGISTRY 結線（職務上請求が解決される・既存の送付案内は無傷）
- 一巡: 起票（下書き）→prepare→承認待ち →（人:承認）→発送処理中＋印刷投函LINE
  →（人:発送済）→返送待ち＋返送期限自動設定 → 以降は処理対象外（M5 の消込待ち）
- 発送済→完了（返送想定なしチャネル）と レコード単位 needs_return フラグ（M4方式）
- 返送期限監視（T1-4）が職務上請求の返送待ちレコードを検知すること
"""

import copy
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import channels
from channels.base import ChannelAdapter, DispatchResult, PrepareResult
from channels.shokumu_seikyu import ShokumuSeikyuAdapter
from channels.soufu_annai import SoufuAnnaiAdapter
from hub import dispatch, kintone, return_deadline

OFFICE_ENV = {
    "OFFICE_NAME": "大野法律事務所", "OFFICE_ZIP": "332-0012",
    "OFFICE_ADDRESS": "埼玉県川口市本町4-1-6", "OFFICE_TEL": "048-000-0000",
    "OFFICE_ATTORNEY": "大野太郎",
}


class FakeStore:
    """kintone の代役（test_hub_dispatch と同型・revision 楽観ロック再現）"""

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

    def status(self, rid="9"):
        return self.records[rid]["発送ステータス"]["value"]

    def field(self, name, rid="9"):
        return self.records[rid].get(name, {}).get("value", "")


def muni_record():
    return {"市区町村名": {"value": "川口市"}, "都道府県": {"value": "埼玉県"},
            "担当部署": {"value": "市民課"}, "郵便番号": {"value": "332-8601"},
            "住所": {"value": "埼玉県川口市青木2-1-1"}, "備考": {"value": ""},
            "手数料_戸籍謄本": {"value": "450"}, "手数料_除籍改製原": {"value": "750"},
            "手数料_附票": {"value": "300"}, "手数料_住民票": {"value": "300"}}


def shokumu_record(status="下書き"):
    data = {"request_items": [{"type": "戸籍謄本", "count": 2}],
            "municipality": "川口市",
            "target": {"対象者": "山田花子", "本籍": "埼玉県川口市…",
                       "生年月日": "昭和25年3月15日"},
            "purpose": "受任事件の送付先調査のため"}
    return {"$id": {"value": "9"}, "$revision": {"value": "1"},
            "発送ステータス": {"value": status},
            "チャネル": {"value": "職務上請求"},
            "ユニット種別": {"value": "時効援用"},
            "件名": {"value": "職務上請求（川口市・戸籍）"},
            "顧客名表示用": {"value": "山田太郎"},
            "宛先名": {"value": ""},
            "実行済み": {"value": "no"},
            "チャネル固有データ": {"value": json.dumps(data, ensure_ascii=False)}}


class TestRegistry(unittest.TestCase):
    """①結線: 実レジストリ（パッチなし）で解決されること"""

    def test_shokumu_seikyu_is_registered(self):
        self.assertIsInstance(channels.get_adapter("職務上請求"), ShokumuSeikyuAdapter)

    def test_soufu_annai_still_registered(self):
        self.assertIsInstance(channels.get_adapter("送付案内"), SoufuAnnaiAdapter)

    def test_adapter_declares_needs_return(self):
        self.assertTrue(channels.get_adapter("職務上請求").needs_return)


class TestFullCycle(unittest.IsolatedAsyncioTestCase):
    """②③一巡: 起票→承認→発送済→返送待ち（実レジストリ・実 approval を使用）"""

    def setUp(self):
        self.store = FakeStore({"9": shokumu_record()})
        self.notify_admin = AsyncMock()
        self.notify_attorney = AsyncMock()
        patchers = [
            patch.dict(os.environ, OFFICE_ENV),
            patch("hub.kintone.get_record", new=self.store.get_record),
            patch("hub.kintone.update_record", new=self.store.update_record),
            patch("hub.kintone.upload_file", new=self.store.upload_file),
            patch("hub.kintone.search_records", new=AsyncMock(return_value=[muni_record()])),
            patch("hub.notify.notify_admin_line", new=self.notify_admin),
            patch("hub.notify.notify_attorney_approval", new=self.notify_attorney),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    async def test_full_cycle(self):
        # 1) 起票（下書き保存）→ prepare → 承認待ち＋成果物＋弁護士通知
        await dispatch.process_dispatch("9")
        self.assertEqual(self.store.status(), "承認待ち")
        self.assertEqual(self.store.uploaded,
                         ["発送準備チェックリスト.pdf", "職務上請求書_様式1_戸籍謄本.pdf",
                          "レターパック往復ラベル.pdf"])
        self.notify_attorney.assert_awaited_once()
        # 2026-07-04 修正: 封筒宛先は施設名表記（川口市→川口市役所）
        self.assertEqual(self.store.field("宛先名"), "川口市役所　市民課")

        # 2) 人が承認（承認待ち→承認済は kintone 上の人の操作）→ Webhook 再発火
        self.store.records["9"]["発送ステータス"] = {"value": "承認済"}
        await dispatch.process_dispatch("9")
        self.assertEqual(self.store.status(), "発送処理中",
                         "物理郵送チャネルは発送処理中で停止（印刷・投函は人）")
        self.assertEqual(self.store.field("実行済み"), "yes", "claim 済み（冪等ガード）")
        texts = [c.args[0] for c in self.notify_admin.await_args_list]
        self.assertTrue(any("印刷・投函をお願いします" in t for t in texts),
                        "M4 送付案内と同型の発送指示 LINE")

        # 3) 人が投函し 発送済 に変更 → Webhook 再発火 → 返送待ち＋返送期限
        self.store.records["9"]["発送ステータス"] = {"value": "発送済"}
        await dispatch.process_dispatch("9")
        self.assertEqual(self.store.status(), "返送待ち")
        self.assertEqual(self.store.field("返送期限"),
                         return_deadline.compute_deadline("時効援用"),
                         "発送日（今日）＋ユニット既定日数（時効援用=21日）")

        # 4) 返送待ちの再 Webhook は処理対象外（消込は M5・T4系）
        n_updates = len(self.store.updates)
        await dispatch.process_dispatch("9")
        self.assertEqual(len(self.store.updates), n_updates, "返送待ちでは何も書かない")

    async def test_shipped_twice_is_safe(self):
        """発送済の二重 Webhook: 1回目で返送待ちへ・2回目は処理対象外"""
        self.store.records["9"]["発送ステータス"] = {"value": "発送済"}
        await dispatch.process_dispatch("9")
        await dispatch.process_dispatch("9")
        self.assertEqual(self.store.status(), "返送待ち")
        transitions = [f for _, f in self.store.updates if "発送ステータス" in f]
        self.assertEqual(len(transitions), 1, "返送待ちへの遷移は1回だけ")


class _NoReturnAdapter(ChannelAdapter):
    channel_name = "テストチャネル"
    needs_return = False

    async def prepare(self, record):
        return PrepareResult()

    async def dispatch(self, record):
        return DispatchResult(manual_mailing=True)


class TestShippedBranches(unittest.IsolatedAsyncioTestCase):
    """発送済ハンドラの返送要否分岐（チャネル属性／レコード単位フラグ＝M4方式）"""

    async def _shipped(self, channel_data):
        record = shokumu_record("発送済")
        record["チャネル"] = {"value": "テストチャネル"}
        record["チャネル固有データ"] = {"value": channel_data}
        store = FakeStore({"9": record})
        with patch.dict(channels.CHANNEL_REGISTRY,
                        {"テストチャネル": _NoReturnAdapter()}, clear=True), \
             patch("hub.kintone.get_record", new=store.get_record), \
             patch("hub.kintone.update_record", new=store.update_record), \
             patch("hub.notify.notify_admin_line", new=AsyncMock()):
            await dispatch.process_dispatch("9")
        return store

    async def test_no_return_goes_to_completed(self):
        """返送想定なし → 発送済→完了（SERVER_TRANSITIONS の設計どおり）"""
        store = await self._shipped("{}")
        self.assertEqual(store.status(), "完了")

    async def test_record_level_flag_goes_to_waiting(self):
        """レコード単位の needs_return フラグ（M4 送付案内が prepare 時に記録する方式）"""
        store = await self._shipped(json.dumps({"needs_return": True}))
        self.assertEqual(store.status(), "返送待ち")
        self.assertTrue(store.field("返送期限"))

    async def test_broken_json_treated_as_no_flag(self):
        store = await self._shipped("{壊れたjson")
        self.assertEqual(store.status(), "完了")


class TestDeadlineWatchCoversShokumu(unittest.IsolatedAsyncioTestCase):
    """④返送期限監視（T1-4）が職務上請求の返送待ちを対象に含む"""

    async def test_overdue_shokumu_record_detected(self):
        overdue = {"$id": {"value": "9"}, "件名": {"value": "職務上請求（川口市・戸籍）"},
                   "チャネル": {"value": "職務上請求"},
                   "顧客名表示用": {"value": "山田太郎"},
                   "返送期限": {"value": "2026-06-01"},  # 過去日=超過
                   "追跡番号": {"value": "1234-5678-9012"}}
        notify_admin = AsyncMock()
        with patch("hub.kintone.search_records",
                   new=AsyncMock(return_value=[overdue])) as search, \
             patch("hub.notify.notify_admin_line", new=notify_admin):
            problems = await return_deadline.return_deadline_check()
        self.assertIn('発送ステータス in ("返送待ち")', search.await_args.args[1],
                      "チャネル横断のクエリ（職務上請求も自動的に対象）")
        self.assertEqual(len(problems), 1)
        self.assertIn("職務上請求（川口市・戸籍）", problems[0])
        self.assertIn("1234-5678-9012", problems[0])
        notify_admin.assert_awaited_once()

    async def test_future_deadline_not_reported(self):
        future = {"$id": {"value": "9"}, "件名": {"value": "職務上請求"},
                  "チャネル": {"value": "職務上請求"}, "顧客名表示用": {"value": ""},
                  "返送期限": {"value": "2099-01-01"}, "追跡番号": {"value": ""}}
        notify_admin = AsyncMock()
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=[future])), \
             patch("hub.notify.notify_admin_line", new=notify_admin):
            problems = await return_deadline.return_deadline_check()
        self.assertEqual(problems, [])
        notify_admin.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
