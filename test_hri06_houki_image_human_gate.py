"""HRI-06｜相続放棄の画像直接経路（image_intake.handle_houki_image）の人対応ゲート

指摘: handle_houki_image に人対応ゲートが無く、人対応中に画像が届くと受領返信が顧客へ
送信されていた（裁定 G「人対応中は顧客へ一切送信しない」に反する）。

実装（裁定 G・G-2）:
- 判定は HRI-01（houki_bot/hearing.py）と同一の houki_case_store.is_human_mode。
- 人対応中: 顧客向け送信 0 件（受領返信・読解結果とも）・App 28 に保留行・読解は実行して
  結果を App 40 へ転記（人対応者が参照できる）・マーカーは消費しない・ヒアリングの状態は
  進めない。
- 解除後: 後出し送信しない。解除後の最初の受信（テキスト／画像）で、保留行のある
  未回収マーカーを人対応済行で閉じる。
- App 40 の照会失敗（判定不能）は送らない側へ倒す（保留行も書かない=未返信のまま残り、
  次の受信で判定し直す）。
- image_analysis の送信直前確認（_blocked）は残す（二重の防御）。時効の画像経路は無変更。
"""
import io
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import test_houki_image_analysis as t_hia     # noqa: E402  App 40/App 28 フェイクと AI の土台

from chat_responder import IMAGE_RECEIPT_REPLY  # noqa: E402
from houki_bot import hearing  # noqa: E402
from hub import houki_case_store  # noqa: E402
from hub import image_analysis as ia  # noqa: E402
from hub import image_intake as ii  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402

UID = t_hia.UID
_run = t_hia._run
REPO = os.path.dirname(os.path.abspath(__file__))


class _GateBase(t_hia._Base):
    def setUp(self):
        super().setUp()
        ii._pending.clear()
        ii._send_claims.clear()
        hearing.conversation_histories.pop(UID, None)
        self.addCleanup(hearing.conversation_histories.pop, UID, None)
        self.reply = AsyncMock()                          # ヒアリングの reply（顧客向け）
        self.intake = AsyncMock(return_value="nothing")
        model = AsyncMock(return_value=SimpleNamespace(content=[SimpleNamespace(
            type="text", text="ありがとうございます。①亡くなった方のお名前とふりがな")]))
        for p in (
                patch.object(ii, "push_text", self.push),             # 受領返信も同じ push で数える
                patch.object(ii, "is_suppressed", AsyncMock(return_value=False)),
                patch.object(ii, "store_houki_image", AsyncMock(return_value="stored")),
                patch.object(ii, "DEBOUNCE_SEC", 0.01),
                patch.object(ii.notify, "notify_admin_line", self.admin),
                patch.dict(os.environ, {"IMAGE_HEAL_DISABLED": "0"}),
                patch.object(hearing, "call_hearing_model", model),
                patch.object(hearing, "reply_with_push_fallback", self.reply),
                patch.object(hearing, "save_to_approval_queue", AsyncMock()),
                patch.object(hearing, "save_to_chatlog", AsyncMock()),
                patch.object(hearing, "get_recent_chat_history", AsyncMock(return_value=[])),
                patch.object(hearing, "is_suppressed", AsyncMock(return_value=False)),
                patch.object(hearing, "autoreply_paused", lambda: False),
                patch.object(hearing.human_reply_intake, "run_houki", self.intake),
                patch.object(hearing.notify, "notify_admin_line", self.admin)):
            p.start()
            self.addCleanup(p.stop)

    def image(self, event_id="IMG-E1"):
        _run(ii.handle_houki_image(UID, event_id, "mid-" + event_id))

    def text(self, body="こんにちは"):
        _run(hearing.handle_houki_hearing("rtok", UID, body, "TXT-E1"))

    def set_mode(self, rid, mode):
        self.store.cases[rid]["response_mode"] = {"value": mode}

    def rows(self, category_prefix):
        return [r for r in self.store.chatlog
                if str(r.get("category", "")).startswith(category_prefix)]

    def pushed_texts(self):
        return [c.args[2] for c in self.push.await_args_list]


class TestHumanModeImage(_GateBase):
    # 受入 1: 人対応中に画像受信 → 送信 0 件・保留行あり・読解結果は App 40 に保存・状態遷移なし
    def test_human_mode_image_sends_nothing_holds_and_stores_result(self):
        rid = self.seed(response_mode="人対応", status="問い合わせ")
        self.image()
        self.push.assert_not_awaited()                     # 受領返信・読解結果とも 0 件
        self.reply.assert_not_awaited()
        self.assertEqual(len(self.rows("画像受領:houki:")), 1)      # マーカーは残存
        self.assertEqual(self.rows("画像受領済:houki"), [])         # 回収していない
        self.assertEqual(self.rows("画像人対応済:houki"), [])
        (hold,) = self.rows("画像人対応保留:houki")               # 抑止した事実の記録
        self.assertEqual(hold["message"], ii.IMAGE_HUMAN_HOLD_MARKER)
        # 読解は実行し、結果は App 40 へ（人対応者が参照できる）
        self.ai.assert_awaited_once()
        self.assertEqual(self.store.creditors(rid), ["アコム"])
        self.assertEqual(self.store.cases[rid]["死亡日_申告"]["value"], "2024-05-01")
        self.assertEqual(self.store.analyzed_keys(), ["k1"])       # 解除後に再解析しない
        self.assertEqual(self.store.analysis_rows(), [])           # 送信行なし（送っていない）
        # ヒアリングの状態は進めない
        self.assertEqual(self.store.cases[rid]["status"]["value"], "問い合わせ")
        self.assertEqual(self.store.cases[rid]["response_mode"]["value"], "人対応")
        self.assertNotIn(UID, hearing.conversation_histories)
        # 弁護士通知（裁判所書類）は送信の抑止と独立に従来どおり
        self.assertIn("houki_image_analysis_court", self.notify_kinds())

    def test_analysis_failure_in_human_mode_is_contained(self):
        self.seed(response_mode="人対応")
        with patch.object(ii.image_analysis, "analyze_and_reply",
                          AsyncMock(side_effect=RuntimeError("boom"))):
            self.image()                                   # 例外にならない
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows("画像人対応保留:houki")), 1)

    def test_no_send_flag_wins_even_if_mode_flipped_during_analysis(self):
        # ゲートで抑止した束は、読解中に自動へ戻っても送らない（G-2: 後出ししない）
        rid = self.seed(response_mode="人対応")

        async def _ai(*a, **k):
            self.set_mode(rid, "自動")
            return t_hia._tool_response(t_hia._hrep([t_hia._hc("アコム")]),
                                        name="report_documents")
        self.ai.side_effect = _ai
        self.image()
        self.push.assert_not_awaited()
        self.assertEqual(self.store.creditors(rid), ["アコム"])


class TestReleaseAfterHumanMode(_GateBase):
    # 受入 2: 解除後の最初の受信 → 送信 0 件・保留行のあるマーカーが人対応済で閉じる
    def test_first_text_after_release_closes_without_sending(self):
        rid = self.seed(response_mode="人対応")
        self.image()
        self.set_mode(rid, "自動")                          # 解除（kintone の手編集）
        self.text()
        self.push.assert_not_awaited()                     # 受領返信・読解結果の後出しなし
        (closed,) = self.rows("画像人対応済:houki")
        self.assertEqual(closed["message"], ii.IMAGE_HUMAN_CLOSED_MARKER)
        self.assertEqual(self.rows("画像受領済:houki"), [])
        self.assertEqual(self.reply.await_count, 1)        # このテキストへの通常応答は従来どおり
        self.text()
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows("画像人対応済:houki")), 1)
        self.assertEqual(self.ai.await_count, 1)           # 解除後に再解析しない

    def test_first_image_after_release_closes_held_then_processes_new_one(self):
        rid = self.seed(response_mode="人対応")
        self.image("IMG-E1")
        self.set_mode(rid, "自動")
        self.store.cases[rid]["受信書類写真"]["value"].append(
            {"fileKey": "k2", "name": "k2.jpg", "size": "1", "contentType": "image/jpeg"})
        self.store.files["k2"] = t_hia.JPEG
        self.image("IMG-E2")                               # 解除後の最初の受信が画像
        closed = self.rows("画像人対応済:houki")
        self.assertEqual(len(closed), 1)
        first, second = self.rows("画像受領:houki:")
        self.assertTrue(int(first["$id"]) < int(closed[0]["$id"]) < int(second["$id"]))
        # 新しい画像は従来どおり: 受領返信 1 通+読解結果 1 通（k2 だけを読む）
        texts = self.pushed_texts()
        self.assertEqual(texts[0], IMAGE_RECEIPT_REPLY)
        self.assertEqual(len(texts), 2)
        self.assertEqual(len(self.rows("画像受領済:houki")), 1)
        self.assertEqual(sorted(self.store.analyzed_keys()), ["k1", "k2"])

    def test_heal_never_sends_for_held_marker(self):
        # 閉鎖を経ずに heal だけが走っても（閉鎖行の保存失敗 等）、送信関門が送らない
        rid = self.seed(response_mode="人対応")
        self.image()
        self.set_mode(rid, "自動")
        self.assertFalse(_run(ii.heal_unreplied("houki", ii.HOUKI_CHANNEL, UID)))
        self.push.assert_not_awaited()


class TestNotHumanModeRegression(_GateBase):
    # 受入 3: 人対応でない場合は従来どおり受領返信と読解結果が出る
    def test_auto_mode_image_sends_receipt_and_analysis(self):
        for mode in ("自動", ""):
            with self.subTest(response_mode=mode):
                self.setUp()
                rid = self.seed(response_mode=mode)
                self.image()
                texts = self.pushed_texts()
                self.assertEqual(texts[0], IMAGE_RECEIPT_REPLY)
                self.assertEqual(len(texts), 2)            # 受領返信+読解結果
                self.assertIn("アコム", texts[1])
                self.assertEqual(len(self.rows("画像受領済:houki")), 1)
                self.assertEqual(self.rows("画像人対応保留:houki"), [])
                self.assertEqual(self.store.creditors(rid), ["アコム"])
                self.assertEqual(len(self.store.analysis_rows()), 1)

    def test_no_case_record_sends_receipt_as_before(self):
        self.image()                                       # App 40 未作成=自動扱い
        self.assertEqual(self.pushed_texts(), [IMAGE_RECEIPT_REPLY])
        self.assertEqual(self.rows("画像人対応保留:houki"), [])


class TestCaseLookupFailure(_GateBase):
    # 受入 4: App 40 照会失敗（判定不能）→ 送信 0 件
    def test_lookup_failure_sends_nothing_and_leaves_marker_unreplied(self):
        self.seed(response_mode="自動")
        with patch.object(houki_case_store, "fetch_case",
                          AsyncMock(side_effect=hub_kintone.KintoneError(520, "X", "down"))):
            self.image()
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows("画像受領:houki:")), 1)      # 未返信のまま
        self.assertEqual(self.rows("画像人対応保留:houki"), [])     # 判定不能=保留も書かない
        self.assertEqual(self.rows("画像受領済:houki"), [])
        self.ai.assert_not_awaited()
        # 照会が回復し、人対応でなければ、次の受信で従来どおり heal が回収する
        self.text()
        self.assertEqual(self.pushed_texts()[0], IMAGE_RECEIPT_REPLY)
        self.assertEqual(len(self.rows("画像受領済:houki")), 1)

    def test_lookup_failure_then_human_mode_holds_on_next_text(self):
        rid = self.seed(response_mode="人対応")
        with patch.object(houki_case_store, "fetch_case",
                          AsyncMock(side_effect=hub_kintone.KintoneError(520, "X", "down"))):
            self.image()
        self.text()                                        # 照会回復・人対応中
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows("画像人対応保留:houki")), 1)
        self.assertEqual(self.store.cases[rid]["response_mode"]["value"], "人対応")


class TestSharedGateAndDefenceInDepth(unittest.TestCase):
    def test_single_gate_function_truth_table(self):
        f = houki_case_store.is_human_mode
        self.assertTrue(f({"response_mode": {"value": "人対応"}}))
        for rec in (None, {}, {"response_mode": {"value": ""}},
                    {"response_mode": {"value": "自動"}}, {"response_mode": None}):
            self.assertFalse(f(rec))

    def test_both_paths_use_the_same_gate_function(self):
        # 別実装の禁止: テキスト経路（HRI-01）と画像直接経路（HRI-06）が同じ関数を呼ぶ
        for rel in ("houki_bot/hearing.py", "hub/image_intake.py"):
            src = io.open(os.path.join(REPO, rel), encoding="utf-8").read()
            self.assertIn("houki_case_store.is_human_mode(", src, rel)
        src = io.open(os.path.join(REPO, "houki_bot/hearing.py"), encoding="utf-8").read()
        self.assertNotIn('== "人対応"', src)                # 直書きの判定が残っていない

    def test_analysis_pre_send_check_is_kept(self):
        # image_analysis 側の送信直前確認（二重の防御）は残す
        self.assertTrue(ia._blocked({"response_mode": {"value": "人対応"}}, UID))
        self.assertFalse(ia._blocked({"response_mode": {"value": "自動"}}, UID))
        self.assertTrue(ia.HOUKI.store_when_human)
        self.assertFalse(ia.JIKOU.store_when_human)        # 時効は従来どおり blocked で終了


class TestJikouUnchanged(t_hia._Base):
    def test_jikou_human_mode_still_blocks_without_store(self):
        # 時効の読解は無変更: 人対応なら blocked（転記も解析済みマーカーも書かない）
        self.store.seed_case({"LINEユーザーID": UID, "response_mode": "人対応"}, ["k1"])
        with patch.object(ia, "_call_ai", AsyncMock(return_value=None)):
            out = _run(ia.analyze_and_reply(UID, "evt-j", ia.JIKOU))
        self.assertEqual(out, "blocked")
        self.push.assert_not_awaited()
        self.assertEqual([r for r in self.store.chatlog
                          if str(r.get("category", "")).startswith(ia.ANALYZED_PREFIX)], [])


if __name__ == "__main__":
    unittest.main()
