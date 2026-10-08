"""HRI-01（Codex R-HUMAN-REPLY-INTAKE・対象 SHA fb771de）: 相続放棄の人対応ゲートの位置。

指摘: houki_bot/hearing.py の人対応判定（App 40 response_mode=人対応）が
image_intake.heal_unreplied の**後**にあり、人対応中でも未返信の画像受領マーカーが
あると受領返信が顧客へ送信されていた（裁定 G: 人対応中は画像受領返信を含め顧客向け
送信を一切発生させない）。

修正: 人対応判定を heal_unreplied より前へ。

裁定 G-2（大野決定・人対応中に抑止した自動送信の解除後の扱い）:
- 人対応中に抑止した顧客向け自動送信（受領返信を含む）は、解除後も顧客へ送らない。
- 抑止した事実は App 28 に「保留」行（画像人対応保留:houki）として記録する。
- 解除後の最初の受信時に、保留行より古い未回収マーカーを「人対応済」行
  （画像人対応済:houki）で閉じる。保留行が存在しないマーカーは閉じない
  （送信失敗等の通常の未返信は従来どおり heal が回収する）。
- App 40 の編集 webhook は使わない（解除の検知=人対応でない受信が来たこと）。

heal は実物（hub.image_intake.heal_unreplied・send_receipt_and_close）を通し、
App 28 / App 40 はフェイク、LINE 送信（push_text）だけを差し替える。
"""
import asyncio
import os
import re
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token",
    "DOCUMENT_WEBHOOK_SECRET": "doc-secret",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
    "APP_HOUKI": "40", "TOKEN_HOUKI": "d",
    "HOUKI_LINE_CHANNEL_SECRET": "houki_secret",
    "HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "houki_token",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from chat_responder import IMAGE_INBOUND_MARKER, IMAGE_RECEIPT_REPLY  # noqa: E402
from houki_bot import hearing  # noqa: E402
from hub import image_intake as ii  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402

USER = "U_hri01"
EVT = "01HRI01EVENT"


def _run(coro):
    return asyncio.run(coro)


class _FakeStore:
    """App 28（チャットログ）+ App 40（相続放棄案件）の最小フェイク。
    App 28 は category 完全一致/部分一致・line_user_id・desc、App 40 は
    LINEユーザーID 検索のみ。"""

    def __init__(self):
        self.chatlog: list[dict] = []
        self.case: dict | None = None
        self._id = 0

    def seed_marker(self, event_id="IMG1"):
        self._id += 1
        self.chatlog.append({"$id": str(self._id), "line_user_id": USER,
                             "role": "user", "message": IMAGE_INBOUND_MARKER,
                             "category": ii.marker_category("houki", event_id),
                             "auto_sent": "no"})

    def set_case(self, response_mode):
        self.case = {"$id": {"value": "50"}, "$revision": {"value": "3"},
                     "LINEユーザーID": {"value": USER},
                     "status": {"value": "問い合わせ"},
                     "顧客名": {"value": "山田太郎"},
                     "response_mode": {"value": response_mode}}

    async def create(self, app, fields):
        assert app.app_id_env == "APP_CHATLOG", app.app_id_env
        self._id += 1
        self.chatlog.append({"$id": str(self._id), **fields})
        return str(self._id)

    async def search(self, app, query, fields=None):
        if app.app_id_env == "APP_HOUKI":
            return [dict(self.case)] if self.case is not None else []
        assert app.app_id_env == "APP_CHATLOG", app.app_id_env
        m_eq = re.search('category = "([^"]+)"', query)
        m_like = re.search('category like "([^"]+)"', query)
        m_uid = re.search('line_user_id = "([^"]+)"', query)
        rows = self.chatlog
        if m_uid:
            rows = [r for r in rows if r.get("line_user_id") == m_uid.group(1)]
        if m_eq:
            rows = [r for r in rows if r.get("category") == m_eq.group(1)]
        elif m_like:
            rows = [r for r in rows
                    if m_like.group(1) in str(r.get("category") or "")]
        rows = sorted(rows, key=lambda r: int(r["$id"]), reverse="desc" in query)
        return [{"$id": {"value": r["$id"]},
                 "category": {"value": r.get("category", "")}} for r in rows[:1]]

    def markers(self):
        return [r for r in self.chatlog
                if str(r.get("category", "")).startswith("画像受領:houki:")]

    def receipts(self):
        return [r for r in self.chatlog if r.get("category") == "画像受領済:houki"]

    def holds(self):
        return [r for r in self.chatlog if r.get("category") == "画像人対応保留:houki"]

    def human_closed(self):
        return [r for r in self.chatlog if r.get("category") == "画像人対応済:houki"]


class TestHumanGateBeforeHeal(unittest.TestCase):
    def setUp(self):
        ii._pending.clear()
        ii._send_claims.clear()
        hearing.conversation_histories.pop(USER, None)
        self.addCleanup(hearing.conversation_histories.pop, USER, None)
        self.store = _FakeStore()
        self.push = AsyncMock(return_value=True)          # 顧客向け push（受領返信）
        self.reply = AsyncMock()                          # 顧客向け reply（ヒアリング）
        self.notify = AsyncMock(return_value=True)        # 管理者通知（顧客向けではない）
        self.intake = AsyncMock(return_value="nothing")
        model = AsyncMock(return_value=SimpleNamespace(content=[SimpleNamespace(
            type="text", text="ありがとうございます。①亡くなった方のお名前とふりがな")]))
        for p in (
                patch.object(hub_kintone, "create_record", self.store.create),
                patch.object(hub_kintone, "search_records", self.store.search),
                patch.dict(os.environ, {"IMAGE_HEAL_DISABLED": "0"}),   # heal は実物
                patch.object(ii, "push_text", self.push),
                patch.object(ii.image_analysis, "analyze_and_reply", AsyncMock()),
                patch.object(hearing, "call_hearing_model", model),
                patch.object(hearing, "reply_with_push_fallback", self.reply),
                patch.object(hearing, "save_to_approval_queue", AsyncMock()),
                patch.object(hearing, "save_to_chatlog", AsyncMock()),
                patch.object(hearing, "get_recent_chat_history",
                             AsyncMock(return_value=[])),
                patch.object(hearing, "is_suppressed", AsyncMock(return_value=False)),
                patch.object(hearing, "autoreply_paused", lambda: False),
                patch.object(hearing.human_reply_intake, "run_houki", self.intake),
                patch.object(hearing.notify, "notify_admin_line", self.notify)):
            p.start()
            self.addCleanup(p.stop)

    def _turn(self, text="こんにちは"):
        _run(hearing.handle_houki_hearing("rtok", USER, text, EVT))

    def customer_sends(self) -> int:
        return self.push.await_count + self.reply.await_count

    # 受入 1: 人対応中+未返信マーカー → 送信 0 件・保留行あり・マーカー残存
    def test_human_mode_with_unreplied_marker_holds_and_sends_nothing(self):
        self.store.set_case("人対応")
        self.store.seed_marker()
        self._turn()
        self.assertEqual(self.customer_sends(), 0)
        self.assertEqual(len(self.store.markers()), 1)     # 消費・削除されていない
        self.assertEqual(self.store.receipts(), [])
        self.assertEqual(self.store.human_closed(), [])    # 人対応中は閉じない
        (hold,) = self.store.holds()                       # 抑止した事実の記録
        self.assertEqual(hold["message"], ii.IMAGE_HUMAN_HOLD_MARKER)
        self.assertEqual((hold["line_user_id"], hold["role"], hold["auto_sent"]),
                         (USER, "user", "no"))
        self.notify.assert_awaited_once()                  # 【人対応中】通知は従来どおり
        self.intake.assert_awaited_once_with(USER, "こんにちは", EVT)   # 取込は常時

    def test_hold_row_is_written_once_per_unreplied_bundle(self):
        self.store.set_case("人対応")
        self.store.seed_marker()
        self._turn()
        self._turn()                                       # 人対応中の 2 通目
        self.assertEqual(len(self.store.holds()), 1)
        self.store.seed_marker("IMG2")                     # 人対応中に新しい未回収
        self._turn()
        self.assertEqual(len(self.store.holds()), 2)
        self.assertEqual(self.customer_sends(), 0)

    def test_human_mode_without_marker_writes_no_hold(self):
        self.store.set_case("人対応")
        self._turn()
        self.assertEqual(self.store.holds(), [])
        self.assertEqual(self.customer_sends(), 0)

    # 受入 2: 解除後の最初のテキスト受信 → 受領返信 0 件・当該マーカーが人対応済で閉じる
    def test_release_closes_held_marker_without_sending_receipt(self):
        self.store.set_case("人対応")
        self.store.seed_marker()
        self._turn()
        self.store.set_case("自動")                        # 人対応の解除（kintone の手編集）
        self._turn()                                       # 解除後の最初の受信
        self.push.assert_not_awaited()                     # 受領返信の後出しなし
        self.assertEqual(self.store.receipts(), [])
        (closed,) = self.store.human_closed()
        self.assertEqual(closed["message"], ii.IMAGE_HUMAN_CLOSED_MARKER)
        self.assertGreater(int(closed["$id"]), int(self.store.markers()[-1]["$id"]))
        self.assertEqual(len(self.store.markers()), 1)     # マーカー自体は残る（追跡可能）
        # このテキストへの通常のヒアリング応答は従来どおり（抑止した送信ではない）
        self.assertEqual(self.reply.await_count, 1)
        self._turn()                                       # 以後の受信でも送らない・二重に閉じない
        self.push.assert_not_awaited()
        self.assertEqual(len(self.store.human_closed()), 1)

    def test_close_failure_still_sends_nothing(self):
        # 人対応済行の保存に失敗しても、保留行のある未回収は送信関門が送らない
        self.store.set_case("人対応")
        self.store.seed_marker()
        self._turn()
        self.store.set_case("自動")
        real_create = self.store.create

        async def _create(app, fields):
            if str(fields.get("category", "")).startswith("画像人対応済:"):
                raise hub_kintone.KintoneError(520, "GAIA_XX", "down")
            return await real_create(app, fields)
        with patch.object(hub_kintone, "create_record", _create):
            self._turn()
        self.push.assert_not_awaited()
        self.assertEqual(self.store.human_closed(), [])
        self._turn()                                       # 次の受信で再び閉鎖を試みる
        self.push.assert_not_awaited()
        self.assertEqual(len(self.store.human_closed()), 1)

    # 受入 3: 保留行の無い未返信マーカー（送信失敗由来）は閉じられず従来どおり扱われる
    def test_unreplied_marker_without_hold_is_healed_as_before(self):
        self.store.set_case("自動")
        self.store.seed_marker()                           # 送信失敗で残った未返信
        self._turn()
        self.assertEqual(self.store.human_closed(), [])
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(self.push.await_args.args[1:], (USER, IMAGE_RECEIPT_REPLY))
        self.assertEqual(len(self.store.receipts()), 1)

    def test_marker_newer_than_hold_is_not_closed(self):
        # 保留行より新しい未返信マーカー（解除後に届いて送信に失敗した画像）は通常の未返信
        self.store.set_case("人対応")
        self.store.seed_marker()
        self._turn()                                       # 保留行
        self.store.set_case("自動")
        self.store.seed_marker("IMG2")                     # 保留行より新しい
        self._turn()
        self.assertEqual(self.store.human_closed(), [])
        self.assertEqual(self.push.await_count, 1)         # 従来どおり heal が回収
        self.assertEqual(len(self.store.receipts()), 1)

    def test_hold_save_failure_is_contained(self):
        self.store.set_case("人対応")
        self.store.seed_marker()
        real_create = self.store.create

        async def _create(app, fields):
            if str(fields.get("category", "")).startswith("画像人対応保留:"):
                raise hub_kintone.KintoneError(520, "GAIA_XX", "down")
            return await real_create(app, fields)
        with patch.object(hub_kintone, "create_record", _create):
            self._turn()                                   # 例外にならない
        self.assertEqual(self.customer_sends(), 0)
        self.notify.assert_awaited_once()

    # 受入 4（回帰）: 人対応でない+未返信マーカー → 従来どおり送信
    def test_auto_mode_with_unreplied_marker_heals_as_before(self):
        for mode in ("", "自動"):
            with self.subTest(response_mode=mode):
                self.store.chatlog.clear()
                self.push.reset_mock()
                self.store.set_case(mode)
                self.store.seed_marker()
                self._turn()
                self.assertEqual(self.push.await_count, 1)
                self.assertEqual(len(self.store.receipts()), 1)
                self.assertEqual(self.store.holds(), [])

    def test_no_case_record_heals_as_before(self):
        self.store.seed_marker()                           # App 40 未作成=自動扱い
        self._turn()
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(len(self.store.receipts()), 1)

    # 順序 pin: 人対応判定（App 40 照会）が heal より前
    def test_gate_lookup_precedes_heal(self):
        self.store.set_case("自動")
        order = []
        real_fetch = hearing.houki_case_store.fetch_case

        async def _fetch(uid):
            order.append("fetch_case")
            return await real_fetch(uid)

        async def _close(*a):
            order.append("close_held")
            return False

        async def _heal(*a):
            order.append("heal")
            return False
        with patch.object(hearing.houki_case_store, "fetch_case", _fetch), \
             patch.object(hearing.image_intake, "close_held_markers", _close), \
             patch.object(hearing.image_intake, "heal_unreplied", _heal):
            self._turn()
        self.assertEqual(order[:3], ["fetch_case", "close_held", "heal"])

    # 判定不能（App 40 照会の失敗）は送らない側へ倒す・取込の finally は維持
    def test_case_lookup_failure_sends_nothing(self):
        self.store.seed_marker()
        with patch.object(hearing.houki_case_store, "fetch_case",
                          AsyncMock(side_effect=RuntimeError("lookup down"))):
            with self.assertRaises(RuntimeError):
                self._turn()
        self.assertEqual(self.customer_sends(), 0)
        self.assertEqual(self.store.receipts(), [])
        self.assertEqual(self.store.holds(), [])           # 判定不能=保留も閉鎖もしない
        self.assertEqual(self.store.human_closed(), [])
        self.intake.assert_awaited_once()


class TestHumanRowsHiddenFromHistory(unittest.TestCase):
    """保留行・人対応済行（固定文言）は会話履歴の復元に含めない。"""

    def test_hold_and_closed_rows_are_excluded(self):
        import chat_responder
        records = [
            {"role": {"value": "user"}, "message": {"value": ii.IMAGE_HUMAN_CLOSED_MARKER}},
            {"role": {"value": "user"}, "message": {"value": ii.IMAGE_HUMAN_HOLD_MARKER}},
            {"role": {"value": "assistant"}, "message": {"value": "返信"}},
            {"role": {"value": "user"}, "message": {"value": IMAGE_INBOUND_MARKER}},
        ]

        class _Resp:
            is_success = True

            def json(self):
                return {"records": records}

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, *a, **k):
                return _Resp()
        with patch.object(chat_responder.httpx, "AsyncClient", lambda *a, **k: _Client()), \
             patch.object(chat_responder, "APP_CHATLOG", "28"), \
             patch.object(chat_responder, "TOKEN_CHATLOG", "d"):
            history = _run(chat_responder.get_recent_chat_history(USER))
        self.assertEqual([h["content"] for h in history], [IMAGE_INBOUND_MARKER, "返信"])


if __name__ == "__main__":
    unittest.main()
