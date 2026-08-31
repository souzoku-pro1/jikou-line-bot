"""IMAGE-INTAKE-1（+fix1）: 画像の複数枚まとめ受領返信（両チャネル・AI読解なし）。

固定する仕様:
- 添付先 FILE 欄は App 21/App 40 とも実測で適切な欄が不在=取得+添付は保留
  （不足フィールド報告・本票は束ね返信のみ）。
- 束ね方式: 受信ごとの App 28 マーカー保存（冪等キー=event_id・fix1[02]:
  category=画像受領:{channel}:{event_id} のチャネル識別込み）は現行維持。
  返信は DEBOUNCE_SEC 待ち→in-memory 予約（H4-fix2 の check-then-act 同型）で
  新着に譲り→代表候補は App 28 照会（**チャネル別**最新受領行=自分）で確定→
  push 1 通。
- fix1[03]: push 成功（True）を確認できたときだけ受領済み行
  （category=画像受領済:{channel}）で冪等を閉じる。非 2xx・通信例外・
  受領済み行の保存失敗=未返信のまま。
- fix1[01]: 未返信（チャネル別最新マーカー行 > 最新受領済み行）は自己修復
  発火 heal_unreplied（次のイベント受信時）が回収——待機タスクの消滅
  （再起動）でも恒久無返信にならない。
- push_text の既定挙動 pin: 非 2xx は例外化せず False を返す・通信例外は
  従来どおり送出（既存 caller への非影響）。
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
from hub import line_channel  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


class _FakeChatlog28:
    """App 28 フェイク（category 完全一致/部分一致・line_user_id・desc 対応）。"""

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
        return [{"$id": {"value": r["$id"]},
                 "category": {"value": r.get("category", "")}}
                for r in rows[:1]]

    def markers(self, channel=None):
        prefix = "画像受領:" + (f"{channel}:" if channel else "")
        return [r for r in self.rows
                if str(r.get("category", "")).startswith(prefix)]

    def receipts(self, channel):
        return [r for r in self.rows
                if r.get("category") == f"画像受領済:{channel}"]


class _Base(unittest.TestCase):
    def setUp(self):
        ii._pending.clear()
        self.addCleanup(ii._pending.clear)
        self.store = _FakeChatlog28()
        for p in (patch.object(hub_kintone, "create_record",
                               self.store.create),
                  patch.object(hub_kintone, "search_records",
                               self.store.search),
                  patch.object(ii, "DEBOUNCE_SEC", 0.03),
                  # conftest の既定無効を本 suite では解除（kintone はフェイク）
                  patch.dict(os.environ, {"IMAGE_HEAL_DISABLED": "0"})):
            p.start()
            self.addCleanup(p.stop)


async def _delayed(coro, delay):
    await asyncio.sleep(delay)
    return await coro


# ── 束ね選出（debounce_and_elect 単体） ─────────────────────────────────────────
class TestDebounceElect(_Base):
    def _latest(self, uid, channel="jikou"):
        async def q():
            return await ii.latest_marker_category(channel, uid)
        return q

    async def _marker(self, uid, channel, evid):
        return await self.store.create(None, {
            "line_user_id": uid,
            "category": ii.marker_category(channel, evid),
            "message": cr.IMAGE_INBOUND_MARKER})

    async def _scenario_three(self, uid="U_bundle"):
        results = {}

        async def one(i, delay):
            await asyncio.sleep(delay)
            await self._marker(uid, "jikou", f"e{i}")
            results[i] = await ii.debounce_and_elect(
                "jikou", uid, ii.marker_category("jikou", f"e{i}"),
                self._latest(uid))
        await asyncio.gather(one(1, 0), one(2, 0.005), one(3, 0.01))
        return results

    def test_three_rapid_images_single_representative(self):
        results = _run(self._scenario_three())
        self.assertEqual([results[1], results[2], results[3]],
                         [False, False, True])
        self.assertEqual(len(self.store.markers("jikou")), 3)

    def test_state_loss_degrades_to_individual_replies(self):
        async def scenario():
            uid = "U_lost"
            await self._marker(uid, "jikou", "l1")
            await self._marker(uid, "jikou", "l2")

            async def one(evid, delay):
                await asyncio.sleep(delay)
                return await ii.debounce_and_elect(
                    "jikou", uid, ii.marker_category("jikou", evid),
                    self._latest(uid))

            async def wipe():
                await asyncio.sleep(0.01)
                ii._pending.clear()
            r1, r2, _ = await asyncio.gather(one("l1", 0),
                                             one("l2", 0.005), wipe())
            return r1, r2
        self.assertEqual(_run(scenario()), (True, True))

    def test_query_failure_degrades_to_reply(self):
        async def scenario():
            uid = "U_qfail"
            await self._marker(uid, "jikou", "q1")

            async def broken():
                raise RuntimeError("down")
            return await ii.debounce_and_elect(
                "jikou", uid, ii.marker_category("jikou", "q1"), broken)
        self.assertTrue(_run(scenario()))

    def test_parallel_registration_single_winner(self):
        results = _run(self._scenario_three())
        self.assertEqual(sum(1 for v in results.values() if v), 1)

    def test_channel_scoped_latest_marker(self):
        # fix1[02]: 両チャネル利用ユーザーでもチャネル別に最新行を判定し、
        # どちらの代表も沈黙しない
        async def scenario():
            uid = "U_both"
            rid_j = await self._marker(uid, "jikou", "j1")
            rid_h = await self._marker(uid, "houki", "h1")   # 全体最新は houki
            self.assertEqual(await ii.latest_marker_row_id("jikou", uid),
                             rid_j)
            self.assertEqual(await ii.latest_marker_row_id("houki", uid),
                             rid_h)
            r_j, r_h = await asyncio.gather(
                ii.debounce_and_elect("jikou", uid,
                                      ii.marker_category("jikou", "j1"),
                                      self._latest(uid, "jikou")),
                ii.debounce_and_elect("houki", uid,
                                      ii.marker_category("houki", "h1"),
                                      self._latest(uid, "houki")))
            return r_j, r_h
        self.assertEqual(_run(scenario()), (True, True))


# ── fix1[03]: push 成功時のみ閉鎖 ───────────────────────────────────────────────
class TestSendReceiptAndClose(_Base):
    UID = "U_close"

    def test_success_saves_receipt(self):
        with patch.object(ii, "push_text", AsyncMock(return_value=True)):
            ok = _run(ii.send_receipt_and_close("houki", ii.HOUKI_CHANNEL,
                                                self.UID))
        self.assertTrue(ok)
        self.assertEqual(len(self.store.receipts("houki")), 1)
        row = self.store.receipts("houki")[0]
        self.assertEqual(row["message"], cr.IMAGE_RECEIPT_REPLY)
        self.assertEqual(row["role"], "assistant")

    def test_non2xx_no_receipt(self):
        with patch.object(ii, "push_text", AsyncMock(return_value=False)):
            ok = _run(ii.send_receipt_and_close("houki", ii.HOUKI_CHANNEL,
                                                self.UID))
        self.assertFalse(ok)
        self.assertEqual(self.store.receipts("houki"), [])

    def test_transport_error_no_receipt(self):
        with patch.object(ii, "push_text",
                          AsyncMock(side_effect=RuntimeError("net"))):
            ok = _run(ii.send_receipt_and_close("houki", ii.HOUKI_CHANNEL,
                                                self.UID))
        self.assertFalse(ok)
        self.assertEqual(self.store.receipts("houki"), [])

    def test_receipt_save_failure_stays_unreplied(self):
        async def broken(app, fields):
            raise RuntimeError("kintone down")
        with patch.object(ii, "push_text", AsyncMock(return_value=True)), \
                patch.object(hub_kintone, "create_record", broken):
            ok = _run(ii.send_receipt_and_close("houki", ii.HOUKI_CHANNEL,
                                                self.UID))
        self.assertFalse(ok)


# ── fix1[01]: 自己修復発火（待機タスク消滅→恒久無返信の遮断） ─────────────────────
class TestHealUnreplied(_Base):
    UID = "U_heal"

    def _make_unreplied(self, channel="houki", evid="hv1"):
        return _run(self.store.create(None, {
            "line_user_id": self.UID,
            "category": ii.marker_category(channel, evid),
            "message": cr.IMAGE_INBOUND_MARKER}))

    def test_cancelled_debounce_task_then_heal_replies(self):
        # 待機タスクの消滅を実際に再現: 画像処理タスクを cancel → マーカーは
        # 残る→次イベント相当の heal で受領返信が送られる
        push = AsyncMock(return_value=True)

        async def scenario():
            with patch.object(ii, "DEBOUNCE_SEC", 5), \
                    patch.object(ii, "push_text", push), \
                    patch.object(ii, "is_suppressed",
                                 AsyncMock(return_value=False)):
                task = asyncio.ensure_future(
                    ii.handle_houki_image(self.UID, "hv1"))
                await asyncio.sleep(0.05)      # マーカー保存+待機に入るまで
                task.cancel()                  # 再起動相当（タスク消滅）
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                ii._pending.clear()            # プロセス消滅なら予約も消える
                self.assertEqual(len(self.store.markers("houki")), 1)
                self.assertEqual(self.store.receipts("houki"), [])
                # 次のイベント受信時の自己修復発火
                healed = await ii.heal_unreplied("houki", ii.HOUKI_CHANNEL,
                                                 self.UID)
                return healed
        self.assertTrue(_run(scenario()))
        push.assert_awaited_once()
        self.assertEqual(len(self.store.receipts("houki")), 1)

    def test_heal_noop_when_replied(self):
        self._make_unreplied()
        with patch.object(ii, "push_text", AsyncMock(return_value=True)):
            self.assertTrue(_run(ii.heal_unreplied(
                "houki", ii.HOUKI_CHANNEL, self.UID)))
            # 2 回目: 受領済み行が最新=再送しない（永続正本で確認）
            push2 = AsyncMock(return_value=True)
            with patch.object(ii, "push_text", push2):
                self.assertFalse(_run(ii.heal_unreplied(
                    "houki", ii.HOUKI_CHANNEL, self.UID)))
            push2.assert_not_awaited()

    def test_heal_noop_without_markers(self):
        push = AsyncMock(return_value=True)
        with patch.object(ii, "push_text", push):
            self.assertFalse(_run(ii.heal_unreplied(
                "houki", ii.HOUKI_CHANNEL, self.UID)))
        push.assert_not_awaited()

    def test_heal_defers_to_live_task(self):
        self._make_unreplied()
        ii._pending[ii._key("houki", self.UID)] = "9"   # 生きた待機タスク相当
        push = AsyncMock(return_value=True)
        with patch.object(ii, "push_text", push):
            self.assertFalse(_run(ii.heal_unreplied(
                "houki", ii.HOUKI_CHANNEL, self.UID)))
        push.assert_not_awaited()

    def test_push_fail_then_next_heal_retries(self):
        # 03×01: push 失敗→未返信のまま→次の heal で再送できる
        self._make_unreplied()
        with patch.object(ii, "push_text", AsyncMock(return_value=False)):
            self.assertFalse(_run(ii.heal_unreplied(
                "houki", ii.HOUKI_CHANNEL, self.UID)))
        self.assertEqual(self.store.receipts("houki"), [])
        with patch.object(ii, "push_text", AsyncMock(return_value=True)):
            self.assertTrue(_run(ii.heal_unreplied(
                "houki", ii.HOUKI_CHANNEL, self.UID)))
        self.assertEqual(len(self.store.receipts("houki")), 1)


# ── push_text の既定挙動 pin（既存 caller への非影響・fix1[03]） ──────────────────
class _FakeResp:
    def __init__(self, status):
        self.status_code = status
        self.text = "resp"

    @property
    def is_success(self):
        return 200 <= self.status_code < 300


class _FakeClient:
    responses: list = []
    raise_exc: Exception | None = None

    def __init__(self, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    async def post(self, url, headers=None, json=None):
        if _FakeClient.raise_exc is not None:
            raise _FakeClient.raise_exc
        return _FakeClient.responses.pop(0)


class TestPushTextDefaultBehaviorPinned(unittest.TestCase):
    def _call(self):
        return _run(line_channel.push_text(
            line_channel.JIKOU_CHANNEL, "Ux", "hello"))

    def test_2xx_returns_true(self):
        _FakeClient.responses = [_FakeResp(200)]
        _FakeClient.raise_exc = None
        with patch.object(line_channel.httpx, "AsyncClient", _FakeClient):
            self.assertIs(self._call(), True)

    def test_non2xx_returns_false_without_raising(self):
        # 既存 caller pin: 非 2xx は従来どおり例外化しない（ログのみ）
        _FakeClient.responses = [_FakeResp(500)]
        _FakeClient.raise_exc = None
        with patch.object(line_channel.httpx, "AsyncClient", _FakeClient):
            self.assertIs(self._call(), False)

    def test_transport_error_still_raises(self):
        # 既存 caller pin: 通信例外は従来どおり送出（挙動不変）
        _FakeClient.raise_exc = RuntimeError("net down")
        with patch.object(line_channel.httpx, "AsyncClient", _FakeClient):
            with self.assertRaises(RuntimeError):
                self._call()
        _FakeClient.raise_exc = None


# ── 時効側統合（main._process_line_image_event） ────────────────────────────────
class TestJikouBundledFlow(_Base):
    USER = "U_jikou_img"

    def _patches(self):
        return [
            patch.object(main.autoreply_stoplist, "is_suppressed",
                         AsyncMock(return_value=False)),
            patch.object(main, "get_app21_record",
                         AsyncMock(return_value=None)),
            patch.object(ii, "push_text", AsyncMock(return_value=True)),
            patch("hub.notify.notify_business", AsyncMock(return_value=True)),
            patch.object(main, "ATTORNEY_LINE_USER_ID", "Uattorney"),
        ]

    def test_three_images_one_push_three_markers(self):
        ps = self._patches()

        async def scenario():
            await asyncio.gather(
                main._process_line_image_event("t1", self.USER, "ev1"),
                _delayed(main._process_line_image_event("t2", self.USER,
                                                        "ev2"), 0.005),
                _delayed(main._process_line_image_event("t3", self.USER,
                                                        "ev3"), 0.01))
        with ps[0], ps[1], ps[2] as push_m, ps[3] as notify, ps[4]:
            _run(scenario())
        push_m.assert_awaited_once()
        self.assertEqual(push_m.await_args.args[2], cr.IMAGE_RECEIPT_REPLY)
        self.assertEqual(len(self.store.markers("jikou")), 3)
        self.assertEqual(len(self.store.receipts("jikou")), 1)   # 閉鎖も 1 回
        notify.assert_awaited_once()

    def test_push_failure_no_close_and_failure_notice(self):
        # fix1[03]: push 失敗=受領済み行なし+失敗通知→次イベントの heal で回収
        ps = self._patches()
        with ps[0], ps[1], \
                patch.object(ii, "push_text",
                             AsyncMock(return_value=False)), \
                ps[3] as notify, ps[4]:
            _run(main._process_line_image_event("t1", self.USER, "ev1"))
        self.assertEqual(self.store.receipts("jikou"), [])
        notify.assert_awaited_once()
        self.assertIn("失敗", notify.await_args.args[1])
        # 次イベント相当の heal で回収
        with patch.object(ii, "push_text", AsyncMock(return_value=True)):
            self.assertTrue(_run(ii.heal_unreplied(
                "jikou", main.hub_line_channel.JIKOU_CHANNEL, self.USER)))
        self.assertEqual(len(self.store.receipts("jikou")), 1)

    def test_existing_gates_kept(self):
        ps = self._patches()
        with patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(return_value=True)), \
             patch.object(main, "_handle_suppressed_inbound",
                          AsyncMock()) as sup, \
             ps[1], ps[2] as push_m, ps[3], ps[4]:
            _run(main._process_line_image_event("t", self.USER, "ev-s"))
        sup.assert_awaited_once()
        push_m.assert_not_awaited()


# ── 相続放棄側（handle_houki_image） ──────────────────────────────────────────
class TestHoukiImageFlow(_Base):
    USER = "U_houki_img"

    def _patches(self):
        return [
            patch.object(ii, "is_suppressed",
                         AsyncMock(return_value=False)),
            patch.object(ii, "push_text", AsyncMock(return_value=True)),
            patch.object(ii.notify, "notify_admin_line",
                         AsyncMock(return_value=True)),
        ]

    def test_receipt_reply_new_behavior(self):
        ps = self._patches()
        with ps[0], ps[1] as push, ps[2] as notify:
            _run(ii.handle_houki_image(self.USER, "hv1"))
        push.assert_awaited_once()
        self.assertIs(push.await_args.args[0], ii.HOUKI_CHANNEL)
        self.assertEqual(push.await_args.args[2], cr.IMAGE_RECEIPT_REPLY)
        notify.assert_not_awaited()
        self.assertEqual(len(self.store.markers("houki")), 1)
        self.assertEqual(len(self.store.receipts("houki")), 1)

    def test_two_images_bundled_single_push(self):
        ps = self._patches()

        async def scenario():
            await asyncio.gather(
                ii.handle_houki_image(self.USER, "hv1"),
                _delayed(ii.handle_houki_image(self.USER, "hv2"), 0.005))
        with ps[0], ps[1] as push, ps[2]:
            _run(scenario())
        push.assert_awaited_once()
        self.assertEqual(len(self.store.markers("houki")), 2)
        self.assertEqual(len(self.store.receipts("houki")), 1)

    def test_redelivery_idempotent(self):
        ps = self._patches()
        with ps[0], ps[1] as push, ps[2]:
            _run(ii.handle_houki_image(self.USER, "hv1"))
            _run(ii.handle_houki_image(self.USER, "hv1"))
        push.assert_awaited_once()
        self.assertEqual(len(self.store.markers("houki")), 1)

    def test_marker_save_failure_no_reply_and_alert(self):
        async def broken(app, fields):
            raise RuntimeError("kintone down")
        ps = self._patches()
        with ps[0], ps[1] as push, ps[2] as notify, \
                patch.object(hub_kintone, "create_record", broken):
            _run(ii.handle_houki_image(self.USER, "hv9"))
        push.assert_not_awaited()
        notify.assert_awaited_once()
        text = notify.await_args.args[0]
        self.assertIn("要確認", text)
        self.assertIn(self.USER[:10], text)
        self.assertNotIn(self.USER, text)

    def test_push_failure_notifies_and_stays_unreplied(self):
        # fix1[03]: 代表の push 失敗=要確認通知+受領済み行なし（heal が回収）
        ps = self._patches()
        with ps[0], patch.object(ii, "push_text",
                                 AsyncMock(return_value=False)), \
                ps[2] as notify:
            _run(ii.handle_houki_image(self.USER, "hv1"))
        notify.assert_awaited_once()
        self.assertIn("受領返信の送信", notify.await_args.args[0])
        self.assertEqual(self.store.receipts("houki"), [])

    def test_paused_gate_silent(self):
        ps = self._patches()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}), \
                ps[0], ps[1] as push, ps[2]:
            _run(ii.handle_houki_image(self.USER, "hv1"))
        push.assert_not_awaited()
        self.assertEqual(len(self.store.markers("houki")), 0)

    def test_stoplist_gate_silent(self):
        ps = self._patches()
        with patch.object(ii, "is_suppressed",
                          AsyncMock(return_value=True)), \
                ps[1] as push, ps[2]:
            _run(ii.handle_houki_image(self.USER, "hv1"))
        push.assert_not_awaited()


# ── 自己修復発火の配線（両チャネルの入口から呼ばれること） ────────────────────────
class TestHealWiring(_Base):
    def test_jikou_text_worker_calls_heal(self):
        spy = AsyncMock(return_value=False)
        stop = AsyncMock(side_effect=RuntimeError("halt after heal"))
        with patch.object(main, "_autoreply_paused", lambda: False),                 patch.object(main.autoreply_stoplist, "is_suppressed",
                             AsyncMock(return_value=False)),                 patch("hub.image_intake.heal_unreplied", spy),                 patch.object(main, "get_app21_record", stop):
            _run(main._process_line_event("t", "U_wire", "こんにちは"))
        spy.assert_awaited_once()
        self.assertEqual(spy.await_args.args[0], "jikou")
        self.assertEqual(spy.await_args.args[2], "U_wire")

    def test_houki_hearing_calls_heal(self):
        from houki_bot import hearing
        spy = AsyncMock(return_value=False)
        with patch.object(hearing, "autoreply_paused", lambda: False),                 patch.object(hearing, "is_suppressed",
                             AsyncMock(return_value=False)),                 patch.object(hearing.image_intake, "heal_unreplied", spy),                 patch.object(hearing, "get_recent_chat_history",
                             AsyncMock(return_value=[])),                 patch.object(hearing, "call_hearing_model",
                             AsyncMock(side_effect=RuntimeError("halt"))),                 patch.object(hearing, "reply_with_push_fallback",
                             AsyncMock()):
            hearing.conversation_histories.pop("U_wire_h", None)
            _run(hearing.handle_houki_hearing("rt", "U_wire_h", "こんにちは"))
        spy.assert_awaited_once()
        self.assertEqual(spy.await_args.args[0], "houki")


if __name__ == "__main__":
    unittest.main()
