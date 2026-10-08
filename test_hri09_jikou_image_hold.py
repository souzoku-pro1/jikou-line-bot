"""HRI-09｜時効の画像保留（裁定 G-2 の時効への適用）

人対応中に届いた時効の画像は、読解を実行して結果を App 21 に保存し、顧客へは送らない
（受領返信・読解結果とも 0 件）。解析済みの印を付けるので、解除後の次の束で当該画像が
再読解・後出し送信されることは無い。相続放棄側（#260）は変更しない。

main._process_line_image_event を実物で通し、App 21/App 28 と画像はフェイク
（test_image_analysis._FakeStore）、LINE 送信・AI・通知だけ差し替える。
"""
import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from test_image_analysis import (  # noqa: E402
    JPEG, UID, _ENV, _FakeStore, _report, _tool_response)

for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

import chat_responder as cr  # noqa: E402
import main  # noqa: E402
from hub import image_analysis as ia  # noqa: E402
from hub import image_intake as ii  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402

HOLD = "画像人対応保留:jikou"
CLOSED = "画像人対応済:jikou"
RECEIPT = "画像受領済:jikou"


def _run(coro):
    return asyncio.run(coro)


class _Base(unittest.TestCase):
    def setUp(self):
        ii._pending.clear()
        ii._send_claims.clear()
        ia._claims.clear()
        for d in (main.conversation_histories, main.kintone_record_ids):
            d.pop(UID, None)
            self.addCleanup(d.pop, UID, None)
        self.store = _FakeStore()
        self.rid = self.store.seed_case({"LINEユーザーID": UID, "response_mode": "自動"}, ["k1"])
        self.push = AsyncMock(return_value=True)
        self.reply = AsyncMock()
        self.biz = AsyncMock(return_value=True)
        self.ai = AsyncMock(return_value=_tool_response(_report(
            [{"name": "アコム", "role": "原債権者", "confidence": "high"}])))

        async def _get(user_id):                     # main の App 21 照会=フェイクの最新値
            rows = await self.store.search_records(
                None, f'LINEユーザーID = "{user_id}" order by $id desc limit 1')
            return rows[0] if rows else None
        for p in (
                patch.object(hub_kintone, "search_records", self.store.search_records),
                patch.object(hub_kintone, "create_record", self.store.create_record),
                patch.object(hub_kintone, "get_record", self.store.get_record),
                patch.object(hub_kintone, "update_record", self.store.update_record),
                patch.object(hub_kintone, "download_file", self.store.download_file),
                patch.object(ii, "DEBOUNCE_SEC", 0.02),
                patch.dict(os.environ, {"IMAGE_HEAL_DISABLED": "0", "AUTOREPLY_PAUSED": "0",
                                        "ATTORNEY_LINE_USER_ID": "Uattorney"}),
                patch.object(main, "get_app21_record", _get),
                patch.object(main, "_autoreply_paused", lambda: False),
                patch.object(main.autoreply_stoplist, "is_suppressed", AsyncMock(return_value=False)),
                patch.object(main, "_line_reply_with_fallback", self.reply),
                patch.object(main, "ask_claude", AsyncMock(return_value="続きをお伺いします。")),
                patch.object(main, "get_recent_chat_history", AsyncMock(return_value=[])),
                patch.object(main, "save_to_chatlog", AsyncMock()),
                patch.object(main, "save_to_approval_queue", AsyncMock()),
                patch.object(main, "_store_jikou_image", AsyncMock()),
                patch.object(main, "ATTORNEY_LINE_USER_ID", "Uattorney"),
                patch.object(main.human_reply_intake, "run_jikou", AsyncMock(return_value="nothing")),
                patch.object(ii, "push_text", self.push),
                patch.object(ii, "is_suppressed", AsyncMock(return_value=False)),
                patch.object(ia, "push_text", self.push),
                patch.object(ia, "create_message_with_fallback", self.ai),
                patch.object(ia.notify, "notify_admin_line", AsyncMock(return_value=True)),
                patch.object(ia, "is_suppressed", AsyncMock(return_value=False)),
                patch("hub.notify.notify_business", self.biz)):
            p.start()
            self.addCleanup(p.stop)

    def set_mode(self, mode):
        self.store.cases[self.rid]["response_mode"] = {"value": mode}

    def add_photo(self, key):
        self.store.cases[self.rid]["受信書類写真"]["value"].append(
            {"fileKey": key, "name": f"{key}.jpg", "size": "1", "contentType": "image/jpeg"})
        self.store.files[key] = JPEG

    def image(self, event_id):
        _run(main._process_line_image_event("rtok", UID, event_id))

    def text(self, body="こんにちは"):
        _run(main._process_line_event("rtok", UID, body))

    def rows(self, category):
        return [r for r in self.store.chatlog if r.get("category") == category]

    def pushed(self):
        return [c.args[2] for c in self.push.await_args_list]


class TestJikouImageHold(_Base):
    # 受入: 人対応中に画像受信 → 送信 0 件・保留行あり・読解結果は App 21 に保存
    def test_human_mode_image_stores_analysis_without_sending(self):
        self.set_mode("人対応")
        self.image("ev1")
        self.push.assert_not_awaited()
        self.reply.assert_not_awaited()
        self.assertEqual(len(self.rows(HOLD)), 1)
        self.assertEqual(self.rows(RECEIPT), [])
        self.assertEqual(len(self.store.markers("jikou")) if hasattr(self.store, "markers") else
                         len([r for r in self.store.chatlog
                              if str(r.get("category", "")).startswith("画像受領:jikou:")]), 1)
        self.ai.assert_awaited_once()                          # 読解は実行
        self.assertEqual(self.store.field(self.rid, "問い合わせ業者名"), "アコム")   # App 21 に保存
        self.assertEqual(self.store.analyzed_keys(), ["k1"])   # 解析済みの印
        self.assertEqual(self.store.analysis_rows(), [])       # 送信行なし（送っていない）
        self.assertIn("【人対応中】", self.biz.await_args.args[1])

    # 受入: 解除後に当該画像が再読解・後出し送信されない
    def test_release_does_not_reanalyze_or_send_held_photo(self):
        self.set_mode("人対応")
        self.image("ev1")
        self.set_mode("自動")
        self.text()                                            # 解除後の最初の受信: 閉鎖のみ
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows(CLOSED)), 1)
        self.assertEqual(self.ai.await_count, 1)
        self.add_photo("k2")                                   # 解除後の次の束
        self.image("ev2")
        texts = self.pushed()
        self.assertEqual(texts[0], cr.IMAGE_RECEIPT_REPLY)     # 新しい画像は従来どおり
        self.assertEqual(len(texts), 2)
        self.assertEqual(self.ai.await_count, 2)
        self.assertEqual(self.store.downloaded, ["k1", "k2"])  # k1 は再読解されない
        self.assertEqual(sorted(self.store.analyzed_keys()), ["k1", "k2"])
        self.assertEqual(len(self.store.analysis_rows()), 1)   # 送信行は新しい束の分だけ

    def test_switch_during_debounce_also_stores(self):
        async def scenario():
            t = asyncio.ensure_future(main._process_line_image_event("rtok", UID, "ev1"))
            await asyncio.sleep(0.005)
            self.set_mode("人対応")
            await t
        _run(scenario())
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows(HOLD)), 1)
        self.assertEqual(self.store.field(self.rid, "問い合わせ業者名"), "アコム")
        self.assertEqual(self.store.analyzed_keys(), ["k1"])

    def test_analysis_failure_during_hold_is_contained(self):
        self.set_mode("人対応")
        with patch.object(ia, "analyze_and_reply", AsyncMock(side_effect=RuntimeError("boom"))):
            self.image("ev1")                                  # 例外にならない
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows(HOLD)), 1)

    # 回帰: 人対応でない時効の画像経路は不変
    def test_auto_mode_unchanged(self):
        self.image("ev1")
        texts = self.pushed()
        self.assertEqual(texts[0], cr.IMAGE_RECEIPT_REPLY)
        self.assertEqual(len(texts), 2)
        self.assertIn("アコム", texts[1])
        self.assertEqual(len(self.rows(RECEIPT)), 1)
        self.assertEqual(self.rows(HOLD), [])
        self.assertEqual(self.store.field(self.rid, "問い合わせ業者名"), "アコム")
        self.assertEqual(len(self.store.analysis_rows()), 1)

    def test_configs(self):
        self.assertTrue(ia.JIKOU.store_when_human)
        self.assertTrue(ia.HOUKI.store_when_human)             # 相続放棄側は #260 のまま


if __name__ == "__main__":
    unittest.main()
