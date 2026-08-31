"""IMAGE-INTAKE-1: 画像の複数枚まとめ受領返信（両チャネル・AI読解なし）。

固定する仕様:
- 添付先 FILE 欄は App 21/App 40 とも実測で適切な欄が不在=取得+添付は保留
  （不足フィールド報告・本票は束ね返信のみ）。
- 束ね方式: 受信ごとの App 28 マーカー保存（冪等キー=event_id）は現行維持。
  返信は DEBOUNCE_SEC 待ち→in-memory 予約（H4-fix2 の check-then-act 同型・
  await なし同期区間）で新着に譲り→代表候補は App 28 照会（最新受領行=自分）
  で確定→push 1 通（reply token 不使用）。
- fail-safe: in-memory 状態消失（再起動相当）=個別返信に縮退・照会失敗=
  個別返信に縮退——いずれも**無返信にはならない**。
- 時効側の既存規律（AUTOREPLY_PAUSED・停止リスト・人対応無言・冪等 pre-check・
  マーカー strict 保存 fail-closed）は不変。
- 相続放棄側: 受領返信を新設（凍結文言=時効と同一）・既存の管理者通知
  （300 秒スロットル）維持・マーカー保存不能=返信しない+要確認通知
  （PII は userId 先頭のみ）。
"""

import asyncio
import os
import re
import unittest
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

import chat_responder as cr  # noqa: E402
import main  # noqa: E402
from hub import image_intake as ii  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


class _FakeChatlog28:
    """App 28 フェイク（category 完全一致+マーカー前方一致/最新の両クエリ対応）。"""

    def __init__(self):
        self.rows = []
        self._id = 0

    async def create(self, app, fields):
        self._id += 1
        self.rows.append({"$id": str(self._id), **fields})
        return str(self._id)

    async def search(self, app, query, fields=None):
        m_eq = re.search('category = "([^"]+)"', query)
        m_like = re.search('category like "([^"]+)"', query)
        m_uid = re.search('line_user_id = "([^"]+)"', query)
        rows = self.rows
        if m_uid:
            rows = [r for r in rows if r.get("line_user_id") == m_uid.group(1)]
        if m_eq:
            rows = [r for r in rows if r.get("category") == m_eq.group(1)]
        elif m_like:
            rows = [r for r in rows
                    if m_like.group(1) in str(r.get("category") or "")]
        desc = "desc" in query
        rows = sorted(rows, key=lambda r: int(r["$id"]), reverse=desc)
        return [{"$id": {"value": r["$id"]}} for r in rows[:1]]

    def markers(self):
        return [r for r in self.rows
                if str(r.get("category", "")).startswith("画像受領:")]


class _Base(unittest.TestCase):
    def setUp(self):
        ii._pending.clear()
        self.addCleanup(ii._pending.clear)
        self.store = _FakeChatlog28()
        for target, fn in ((hub_kintone, "create_record"),
                           (hub_kintone, "search_records")):
            pass
        p1 = patch.object(hub_kintone, "create_record", self.store.create)
        p2 = patch.object(hub_kintone, "search_records", self.store.search)
        p3 = patch.object(ii, "DEBOUNCE_SEC", 0.03)
        for p in (p1, p2, p3):
            p.start()
            self.addCleanup(p.stop)


# ── 束ね選出（debounce_and_elect 単体） ─────────────────────────────────────────
class TestDebounceElect(_Base):
    def _latest(self, uid):
        async def q():
            return await ii.latest_marker_row_id(uid)
        return q

    async def _scenario_three(self, uid="U_bundle"):
        # 連続 3 受信（各受信でマーカー保存→登録→待ち）を並行実行
        results = {}

        async def one(i, delay):
            await asyncio.sleep(delay)
            rid = await self.store.create(None, {
                "line_user_id": uid, "category": f"画像受領:e{i}",
                "message": cr.IMAGE_INBOUND_MARKER})
            results[i] = await ii.debounce_and_elect(
                "jikou", uid, rid, self._latest(uid))
        await asyncio.gather(one(1, 0), one(2, 0.005), one(3, 0.01))
        return results

    def test_three_rapid_images_single_representative(self):
        results = _run(self._scenario_three())
        self.assertEqual([results[1], results[2], results[3]],
                         [False, False, True])     # 最後の 1 件だけが代表
        self.assertEqual(len(self.store.markers()), 3)   # マーカーは 3 件

    def test_state_loss_degrades_to_individual_replies(self):
        # 再起動相当: 待機中に in-memory 予約が消える→各自が個別返信に縮退
        # （無返信にはならない）
        async def scenario():
            uid = "U_lost"
            rid1 = await self.store.create(None, {
                "line_user_id": uid, "category": "画像受領:l1"})
            rid2 = await self.store.create(None, {
                "line_user_id": uid, "category": "画像受領:l2"})

            async def one(rid, delay):
                await asyncio.sleep(delay)
                return await ii.debounce_and_elect(
                    "jikou", uid, rid, self._latest(uid))

            async def wipe():
                await asyncio.sleep(0.01)
                ii._pending.clear()          # 状態消失を再現
            r1, r2, _ = await asyncio.gather(one(rid1, 0), one(rid2, 0.005),
                                             wipe())
            return r1, r2
        r1, r2 = _run(scenario())
        self.assertEqual((r1, r2), (True, True))   # 個別返信（無返信ゼロ）

    def test_query_failure_degrades_to_reply(self):
        async def scenario():
            uid = "U_qfail"
            rid = await self.store.create(None, {
                "line_user_id": uid, "category": "画像受領:q1"})

            async def broken():
                raise RuntimeError("down")
            return await ii.debounce_and_elect("jikou", uid, rid, broken)
        self.assertTrue(_run(scenario()))          # fail-safe: 返信する

    def test_parallel_registration_single_winner(self):
        # 並行 2 受信の同時登録でも代表は 1（check-then-act の同期区間）
        results = _run(self._scenario_three())
        self.assertEqual(sum(1 for v in results.values() if v), 1)


# ── 時効側統合（main._process_line_image_event） ────────────────────────────────
class TestJikouBundledFlow(_Base):
    USER = "U_jikou_img"

    def _patches(self):
        return [
            patch.object(main.autoreply_stoplist, "is_suppressed",
                         AsyncMock(return_value=False)),
            patch.object(main, "get_app21_record",
                         AsyncMock(return_value=None)),
            patch.object(main.hub_line_channel, "push_text", AsyncMock()),
            patch.object(main, "save_to_chatlog", AsyncMock()),
            patch("hub.notify.notify_business", AsyncMock(return_value=True)),
            patch.object(main, "ATTORNEY_LINE_USER_ID", "Uattorney"),
        ]

    def test_three_images_one_push_three_markers(self):
        ps = self._patches()
        push = ps[2]

        async def scenario():
            await asyncio.gather(
                main._process_line_image_event("t1", self.USER, "ev1"),
                _delayed(main._process_line_image_event("t2", self.USER,
                                                        "ev2"), 0.005),
                _delayed(main._process_line_image_event("t3", self.USER,
                                                        "ev3"), 0.01))
        with ps[0], ps[1], push as push_m, ps[3] as log, ps[4] as notify, \
                ps[5]:
            _run(scenario())
        push_m.assert_awaited_once()               # push 1 通のみ
        self.assertEqual(push_m.await_args.args[2], cr.IMAGE_RECEIPT_REPLY)
        self.assertEqual(len(self.store.markers()), 3)   # マーカー 3 件
        log.assert_awaited_once()                  # assistant 記録も 1 回
        notify.assert_awaited_once()               # 通知も従来同様 1 回

    def test_existing_gates_kept(self):
        # 停止リスト gate 維持（受信記録+無返信の従来経路）
        ps = self._patches()
        with patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(return_value=True)), \
             patch.object(main, "_handle_suppressed_inbound",
                          AsyncMock()) as sup, \
             ps[1], ps[2] as push_m, ps[3], ps[4], ps[5]:
            _run(main._process_line_image_event("t", self.USER, "ev-s"))
        sup.assert_awaited_once()
        push_m.assert_not_awaited()


def _delayed(coro, delay):
    async def _inner():
        await asyncio.sleep(delay)
        return await coro
    return _inner()


# ── 相続放棄側（handle_houki_image） ──────────────────────────────────────────
class TestHoukiImageFlow(_Base):
    USER = "U_houki_img"

    def _patches(self):
        return [
            patch.object(ii, "is_suppressed",
                         AsyncMock(return_value=False)),
            patch.object(ii, "push_text", AsyncMock()),
            patch.object(ii, "save_to_chatlog", AsyncMock()),
            patch.object(ii.notify, "notify_admin_line",
                         AsyncMock(return_value=True)),
        ]

    def test_receipt_reply_new_behavior(self):
        ps = self._patches()
        with ps[0], ps[1] as push, ps[2] as log, ps[3] as notify:
            _run(ii.handle_houki_image(self.USER, "hv1"))
        push.assert_awaited_once()
        self.assertIs(push.await_args.args[0], ii.HOUKI_CHANNEL)
        self.assertEqual(push.await_args.args[2], cr.IMAGE_RECEIPT_REPLY)
        log.assert_awaited_once()
        notify.assert_not_awaited()                # 通知新設なし（router 側維持）
        self.assertEqual(len(self.store.markers()), 1)

    def test_two_images_bundled_single_push(self):
        ps = self._patches()

        async def scenario():
            await asyncio.gather(
                ii.handle_houki_image(self.USER, "hv1"),
                _delayed(ii.handle_houki_image(self.USER, "hv2"), 0.005))
        with ps[0], ps[1] as push, ps[2], ps[3]:
            _run(scenario())
        push.assert_awaited_once()
        self.assertEqual(len(self.store.markers()), 2)

    def test_redelivery_idempotent(self):
        ps = self._patches()
        with ps[0], ps[1] as push, ps[2], ps[3]:
            _run(ii.handle_houki_image(self.USER, "hv1"))
            _run(ii.handle_houki_image(self.USER, "hv1"))   # 再配送
        push.assert_awaited_once()
        self.assertEqual(len(self.store.markers()), 1)

    def test_marker_save_failure_no_reply_and_alert(self):
        async def broken(app, fields):
            raise RuntimeError("kintone down")
        ps = self._patches()
        with ps[0], ps[1] as push, ps[2], ps[3] as notify, \
                patch.object(hub_kintone, "create_record", broken):
            _run(ii.handle_houki_image(self.USER, "hv9"))
        push.assert_not_awaited()                  # fail-closed: 返信しない
        notify.assert_awaited_once()
        text = notify.await_args.args[0]
        self.assertIn("要確認", text)
        self.assertIn(self.USER[:10], text)
        self.assertNotIn(self.USER, text)          # userId 全文は載せない

    def test_paused_gate_silent(self):
        ps = self._patches()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}), \
                ps[0], ps[1] as push, ps[2], ps[3]:
            _run(ii.handle_houki_image(self.USER, "hv1"))
        push.assert_not_awaited()
        self.assertEqual(len(self.store.markers()), 0)

    def test_stoplist_gate_silent(self):
        ps = self._patches()
        with patch.object(ii, "is_suppressed",
                          AsyncMock(return_value=True)), \
                ps[1] as push, ps[2], ps[3]:
            _run(ii.handle_houki_image(self.USER, "hv1"))
        push.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
