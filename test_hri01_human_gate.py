"""HRI-01（Codex R-HUMAN-REPLY-INTAKE・対象 SHA fb771de）: 相続放棄の人対応ゲートの位置。

指摘: houki_bot/hearing.py の人対応判定（App 40 response_mode=人対応）が
image_intake.heal_unreplied の**後**にあり、人対応中でも未返信の画像受領マーカーが
あると受領返信が顧客へ送信されていた（裁定 G: 人対応中は画像受領返信を含め顧客向け
送信を一切発生させない）。

修正: 人対応判定を heal_unreplied より前へ。ゲートは「送信の抑止」であって
「マーカーの回収」ではない——人対応中は未返信マーカーを消費・削除せず、人対応の
解除後の次のテキスト受信で heal が受領返信を 1 件送って回収する。

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

    # 受入 1: 人対応中+未返信マーカー → 顧客向け送信 0 件・マーカーは残存（未回収）
    def test_human_mode_with_unreplied_marker_sends_nothing_and_keeps_marker(self):
        self.store.set_case("人対応")
        self.store.seed_marker()
        self._turn()
        self.assertEqual(self.customer_sends(), 0)
        self.push.assert_not_awaited()
        self.assertEqual(len(self.store.markers()), 1)     # 削除されていない
        self.assertEqual(self.store.receipts(), [])        # 回収（受領済み行）もされていない
        self.notify.assert_awaited_once()                  # 【人対応中】通知は従来どおり
        self.intake.assert_awaited_once_with(USER, "こんにちは", EVT)   # 取込は常時

    # 受入 2: 人対応を解除して再実行 → 受領返信 1 件・マーカー回収
    def test_release_human_mode_then_heal_sends_once_and_closes(self):
        self.store.set_case("人対応")
        self.store.seed_marker()
        self._turn()
        self.push.assert_not_awaited()
        self.store.set_case("自動")                        # 人対応の解除
        self._turn()
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(self.push.await_args.args[1:], (USER, IMAGE_RECEIPT_REPLY))
        self.assertEqual(len(self.store.receipts()), 1)    # 回収済み
        self._turn()                                       # 以後は再送しない
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(len(self.store.receipts()), 1)

    # 受入 3（回帰）: 人対応でない+未返信マーカー → 従来どおり送信
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

        async def _heal(*a):
            order.append("heal")
            return False
        with patch.object(hearing.houki_case_store, "fetch_case", _fetch), \
             patch.object(hearing.image_intake, "heal_unreplied", _heal):
            self._turn()
        self.assertEqual(order[:2], ["fetch_case", "heal"])

    # 判定不能（App 40 照会の失敗）は送らない側へ倒す・取込の finally は維持
    def test_case_lookup_failure_sends_nothing(self):
        self.store.seed_marker()
        with patch.object(hearing.houki_case_store, "fetch_case",
                          AsyncMock(side_effect=RuntimeError("lookup down"))):
            with self.assertRaises(RuntimeError):
                self._turn()
        self.assertEqual(self.customer_sends(), 0)
        self.assertEqual(self.store.receipts(), [])
        self.intake.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
