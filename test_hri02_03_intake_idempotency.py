"""HRI-02 / HRI-03（Codex R-HUMAN-REPLY-INTAKE・対象 SHA fb771de）

HRI-02｜冪等制御の 2 相化（hub/human_reply_intake.py）
  指摘: 冪等マーカーの保存（_mark）が失敗しても書込へ進んでいた。また、マーカーを
  AI 呼び出しの前に 1 回書くだけだったため、予約後に書込が失敗すると再配送が
  duplicate になり、その受信は永久に再処理できなかった。
  修正: 予約（返答取込:）→完了（返答取込済:）／解放（返答取込解放:）の追記行。
  予約が取れなければ書込へ進まない。予約のリース=行の作成日時から LEASE_SEC。

HRI-03｜最小レコード二重作成の排他（hub/human_reply_intake.py・main.py）
  指摘: 返答取込とヒアリングが、冒頭の検索結果（record=None）を await を跨いで
  持ち越したまま作成しており、同一 LINE ユーザーの案件レコードが二重に作られ得た。
  修正: 共通の排他区間（hub.user_section）の内側で再検索→無ければ作成。最終防衛線は
  ストア側の一意制約——発火したら再検索して既存レコードを採用して継続する。

kintone は test_human_reply_intake の _FakeKintone（作成日時の刻印・時計・障害注入・
一意制約つき）。AI と通知だけを差し替える。
"""
import asyncio
import datetime
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

from test_human_reply_intake import (  # noqa: E402  env 設定と import 順もここで確定
    ADDR, EVT, J11, NAME, USER, _Base, _ai, _run)

import main  # noqa: E402
from hub import houki_case_store as store  # noqa: E402
from hub import human_reply_intake as hri  # noqa: E402
from hub import jikou_case_create  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import user_section as user_section_mod  # noqa: E402

EVT2 = "01M2INTAKEEVENT0000000000002"
RESERVE = f"返答取込:jikou:{EVT}"
DONE = f"返答取込済:jikou:{EVT}"
RELEASE = f"返答取込解放:jikou:{EVT}"
KANA = "やまだたろう"


def _name_ai():
    return _ai({"顧客名": (True, NAME, "high")}, fill=J11)


def _auto_ai(values: dict):
    """要求された欄集合（スキーマの required）に合わせて応答を組む fake モデル。
    レコード未作成=全 12 欄／既存レコード=空欄のみ、のどちらでもキー集合が一致する。"""
    async def _fn(*a, **k):
        codes = k["tools"][0]["input_schema"]["properties"]["items"]["required"]
        return _ai({c: (True, values[c], "high") for c in codes if c in values},
                   fill=codes)
    return _fn


# ── HRI-02: 予約が取れなければ書込へ進まない ────────────────────────────────────
class TestReserveFailureBlocksWrite(_Base):
    def _assert_no_side_effects(self):
        self.ai.assert_not_awaited()
        self.notify.assert_not_awaited()
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual([c for c in self.fake.create_calls if c[0] == "KINTONE_APP_ID"], [])

    def test_reservation_save_failure_stops_before_ai_and_write(self):
        # 修正前: _mark=False のまま継続して書込まで進んでいた（"written"）
        self.fake.add_jikou()
        self.fake.fail_categories = {"返答取込:"}
        self.ai.return_value = _name_ai()
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "reserve_failed")
        self._assert_no_side_effects()
        self.assertEqual(self.fake.val("KINTONE_APP_ID", "10", "顧客名"), "")

    def test_reservation_save_failure_does_not_create_minimal_record(self):
        self.fake.fail_categories = {"返答取込:"}
        self.ai.return_value = _name_ai()
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "reserve_failed")
        self._assert_no_side_effects()
        self.assertEqual(self.fake.rows["KINTONE_APP_ID"], {})

    def test_precheck_scan_failure_stops(self):
        self.fake.add_jikou()
        self.fake.fail_marker_scan = 1
        self.ai.return_value = _name_ai()
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "reserve_failed")
        self._assert_no_side_effects()
        self.assertEqual(self.fake.markers(), [])          # 予約も書いていない

    def test_winner_query_failure_stops_and_releases_own_reservation(self):
        self.fake.add_jikou()
        self.ai.return_value = _name_ai()
        calls = {"n": 0}

        async def _hook(env, query):
            if "category in" in query:
                calls["n"] += 1
                if calls["n"] == 2:                        # 予約保存の後の勝者照会だけ失敗
                    raise hub_kintone.KintoneError(520, "GAIA_XX", "down")
        self.fake.on_search = _hook
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "reserve_failed")
        self._assert_no_side_effects()
        self.assertEqual(self.fake.markers(), [RESERVE, RELEASE])
        # 解放済みなので、再配送は期限を待たずに再処理される
        self.fake.on_search = None
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")

    def test_chatlog_not_configured_means_no_write(self):
        self.fake.add_jikou()
        self.ai.return_value = _name_ai()
        with patch.dict(os.environ, {"APP_CHATLOG": ""}):
            self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "reserve_failed")
        self._assert_no_side_effects()

    def test_houki_reservation_failure_stops(self):
        self.fake.add_houki()
        self.fake.fail_categories = {"返答取込:"}
        self.ai.return_value = _ai({"顧客名": (True, NAME, "high")},
                                   fill=hri.HOUKI.fields)
        self.assertEqual(_run(hri.run_houki(USER, "山田太郎です", EVT)), "reserve_failed")
        self.ai.assert_not_awaited()
        self.assertEqual(self.fake.update_calls, [])


# ── HRI-02: 予約後の失敗は解放 → 再配送で再処理 ─────────────────────────────────
class TestFailureAfterReservationIsRetried(_Base):
    def test_cas_unconverged_then_redelivery_reprocesses(self):
        # 修正前: 1 回目でマーカーが残り、再配送は "duplicate"（永久に未登録）
        self.fake.add_jikou(revision="5")
        self.fake.conflict_next = 2
        self.ai.return_value = _name_ai()
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "no_write")
        self.assertEqual(self.fake.markers(), [RESERVE, RELEASE])     # 完了は入っていない
        self.assertEqual(self.fake.val("KINTONE_APP_ID", "10", "顧客名"), "")
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")
        self.assertEqual(self.fake.val("KINTONE_APP_ID", "10", "顧客名"), NAME)
        self.assertEqual(self.fake.markers(), [RESERVE, RELEASE, RESERVE, DONE])
        self.assertEqual(self.ai.await_count, 2)
        # 完了後の再配送は duplicate
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "duplicate")
        self.assertEqual(self.ai.await_count, 2)

    def test_exception_in_write_releases_then_redelivery_reprocesses(self):
        self.fake.add_jikou()
        self.ai.return_value = _name_ai()
        with patch.object(hri, "_write", AsyncMock(side_effect=RuntimeError("boom"))):
            self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "error")
        self.assertEqual(self.fake.markers(), [RESERVE, RELEASE])
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")
        self.assertEqual(self.fake.val("KINTONE_APP_ID", "10", "顧客名"), NAME)

    def test_ai_failure_releases_then_redelivery_reprocesses(self):
        self.fake.add_jikou()
        self.ai.side_effect = [RuntimeError("api down"), _name_ai()]
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "ai_failed")
        self.assertEqual(self.fake.markers(), [RESERVE, RELEASE])
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")

    def test_terminal_outcomes_are_completed_and_not_reprocessed(self):
        cases = {
            "nothing": _ai({}, fill=J11),
            "mixed_persons": _ai({"顧客名": (True, NAME, "high")}, mixed=True, fill=J11),
            "rejected_only": _ai({"顧客名": (True, NAME, "low")}, fill=J11),
        }
        for outcome, report in cases.items():
            with self.subTest(outcome=outcome):
                self.fake.rows["APP_CHATLOG"].clear()
                self.fake.rows["KINTONE_APP_ID"].clear()
                self.fake.add_jikou()
                self.ai.reset_mock()
                self.ai.return_value = report
                self.assertEqual(_run(hri.run_jikou(USER, "山田です", EVT)), outcome)
                self.assertEqual(self.fake.markers(), [RESERVE, DONE])
                self.assertEqual(_run(hri.run_jikou(USER, "山田です", EVT)), "duplicate")
                self.assertEqual(self.ai.await_count, 1)      # 通知・AI は 1 受信 1 回

    def test_too_long_is_completed(self):
        self.fake.add_jikou()
        text = "あ" * (hri.MAX_TEXT_CHARS + 1)
        self.assertEqual(_run(hri.run_jikou(USER, text, EVT)), "too_long")
        self.assertEqual(self.fake.markers(), [RESERVE, DONE])
        self.assertEqual(_run(hri.run_jikou(USER, text, EVT)), "duplicate")
        self.assertEqual(self.notify.await_count, 1)

    def test_completion_save_failure_falls_back_to_lease(self):
        # 書込は成立・完了行の保存だけ失敗 → 期限内の再配送は duplicate（処理中扱い）、
        # 期限後の再配送は再処理に入るが空欄のみ規則で二重書込にならない
        self.fake.add_jikou(**{c: "済" for c in J11 if c != "顧客名"})
        self.fake.fail_categories = {"返答取込済:"}
        self.ai.side_effect = _auto_ai({"顧客名": NAME})
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")
        self.assertEqual(self.fake.markers(), [RESERVE])
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "all_filled")
        self.assertEqual(len(self.fake.update_calls), 1)
        self.assertEqual(self.ai.await_count, 1)


# ── HRI-02: リース（有効中=処理中／期限切れ=取り直し）・並行配送の勝者決定 ─────────
class TestLeaseAndWinner(_Base):
    def _seed_reservation(self, category=RESERVE):
        return _run(hri._append_marker(USER, category))

    def test_live_reservation_means_duplicate_no_double_write(self):
        self.fake.add_jikou()
        self._seed_reservation()                          # 他の配送が処理中
        self.ai.return_value = _name_ai()
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "duplicate")
        self.ai.assert_not_awaited()
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual(self.fake.markers(), [RESERVE])  # 自分の予約も書いていない

    def test_lease_boundaries(self):
        self.fake.add_jikou()
        self._seed_reservation()
        self.ai.return_value = _name_ai()
        t0 = self.fake.now
        limit = hri.LEASE_SEC + hri.LEASE_CLOCK_MARGIN_SEC
        # 作成日時は分単位へ切り捨て（t0 の :30 → :00）。期限はそこから limit 秒
        created = t0.replace(second=0, microsecond=0)
        self.fake.now = created + datetime.timedelta(seconds=limit - 1)
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "duplicate")
        self.ai.assert_not_awaited()
        self.fake.now = created + datetime.timedelta(seconds=limit)
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")
        self.assertEqual(self.fake.val("KINTONE_APP_ID", "10", "顧客名"), NAME)
        self.assertEqual(self.fake.markers(), [RESERVE, RESERVE, DONE])

    def test_minute_truncation_does_not_expire_early(self):
        # 予約の実時刻は :30。切り捨てで :00 と記録されても、実時刻+LEASE_SEC までは有効
        self.fake.add_jikou()
        self._seed_reservation()
        self.ai.return_value = _name_ai()
        self.fake.now = self.fake.now + datetime.timedelta(seconds=hri.LEASE_SEC - 1)
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "duplicate")

    def test_unreadable_created_time_is_treated_as_live(self):
        self.fake.add_jikou()
        rid = self._seed_reservation()
        self.fake.rows["APP_CHATLOG"][rid]["作成日時"] = {"type": "CREATED_TIME", "value": ""}
        self.fake.now = self.fake.now + datetime.timedelta(days=30)
        self.ai.return_value = _name_ai()
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "duplicate")
        self.ai.assert_not_awaited()

    def test_created_time_found_by_type_not_by_field_code(self):
        self.fake.add_jikou()
        rid = self._seed_reservation()
        row = self.fake.rows["APP_CHATLOG"][rid]
        row["created"] = row.pop("作成日時")               # 欄コードが違っても型で読む
        self.fake.now = self.fake.now + datetime.timedelta(seconds=3600)
        self.ai.return_value = _name_ai()
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")

    def test_other_event_is_independent(self):
        self.fake.add_jikou()
        self._seed_reservation()                          # EVT は処理中
        self.ai.return_value = _name_ai()
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT2)), "written")

    def test_concurrent_delivery_loses_to_smaller_id(self):
        # 他プロセスの配送が、事前照会と自分の予約保存の間に予約を入れた（$id が小さい）
        self.fake.add_jikou()
        self.ai.return_value = _name_ai()
        real_append = hri._append_marker
        state = {"first": True}

        async def _append(user_id, category):
            if state["first"] and category == RESERVE:
                state["first"] = False
                await real_append(user_id, RESERVE)       # 競合相手（先に保存=小さい $id）
            return await real_append(user_id, category)
        with patch.object(hri, "_append_marker", _append):
            self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "duplicate")
        self.ai.assert_not_awaited()
        self.assertEqual(self.fake.update_calls, [])

    def test_concurrent_delivery_wins_with_smaller_id(self):
        self.fake.add_jikou()
        self.ai.return_value = _name_ai()
        calls = {"n": 0}

        async def _hook(env, query):
            if "category in" in query:
                calls["n"] += 1
                if calls["n"] == 2:                        # 自分の保存の後に相手が保存
                    await hri._append_marker(USER, RESERVE)
        self.fake.on_search = _hook
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")
        self.assertEqual(self.ai.await_count, 1)

    def test_same_process_concurrent_same_event_single_write(self):
        self.fake.add_jikou()
        self.ai.return_value = _name_ai()

        async def _both():
            return await asyncio.gather(hri.run_jikou(USER, "山田太郎です", EVT),
                                        hri.run_jikou(USER, "山田太郎です", EVT))
        self.assertEqual(sorted(_run(_both())), ["duplicate", "written"])
        self.assertEqual(len(self.fake.update_calls), 1)
        self.assertEqual(self.ai.await_count, 1)

    def test_marker_rows_are_excluded_from_history(self):
        # 3 種とも message は固定文言=chat_responder の履歴復元から除外される
        for cat in (RESERVE, DONE, RELEASE):
            self._seed_reservation(cat)
        for row in self.fake.rows["APP_CHATLOG"].values():
            self.assertEqual(row["message"]["value"], hri.INTAKE_MARKER)
            self.assertEqual(row["role"]["value"], "user")


# ── HRI-03: 時効の作成区間（取込×取込・取込×ヒアリング・ヒアリング×ヒアリング） ──────
HEARING_REPLY = ("ありがとうございます。[KINTONE_RECORD]"
                 + json.dumps({"問い合わせ業者名": "テスト債権回収株式会社"}, ensure_ascii=False)
                 + "[/KINTONE_RECORD]続きをお伺いします。")


class _CreateBase(_Base):
    def setUp(self):
        super().setUp()
        for d in (main.conversation_histories, main.kintone_record_ids,
                  main.user_business_names):
            d.pop(USER, None)
            self.addCleanup(d.pop, USER, None)
        main.hearing_completed.discard(USER)
        self.addCleanup(main.hearing_completed.discard, USER)
        self.ask = AsyncMock(return_value=HEARING_REPLY)

        async def _post(record):                           # main.post_to_kintone の代役
            return await self.fake.create_record(jikou_case_create.APP_JIKOU_CASE, record)
        for p in (
                patch.object(main.autoreply_stoplist, "is_suppressed",
                             AsyncMock(return_value=False)),
                # ターン冒頭の台帳照会は「未作成」（=持ち越される古い結果）
                patch.object(main, "get_app21_record", AsyncMock(return_value=None)),
                patch.object(main, "get_recent_chat_history", AsyncMock(return_value=[])),
                patch.object(main, "ask_claude", self.ask),
                patch.object(main, "_line_reply_with_fallback", AsyncMock()),
                patch.object(main, "save_to_chatlog", AsyncMock()),
                patch.object(main, "save_to_approval_queue", AsyncMock()),
                patch.object(main, "post_to_kintone", _post),
                patch.object(main.hub_notify, "notify_business", AsyncMock(return_value=True)),
                # main の finally の取込は差し替えない（main.human_reply_intake は hri と同一
                # module=差し替えると直接呼ぶ側まで置き換わる）。非 durable 文脈は event id
                # なし=no_event_id で何もしない
                patch.object(main, "ATTORNEY_LINE_USER_ID", "U_attorney"),
                patch.dict(os.environ, {"AUTOREPLY_PAUSED": "0"})):
            p.start()
            self.addCleanup(p.stop)

    def app21(self) -> dict:
        return self.fake.rows["KINTONE_APP_ID"]

    def app21_creates(self) -> list:
        return [c for c in self.fake.create_calls if c[0] == "KINTONE_APP_ID"]


class TestJikouNoDoubleCreate(_CreateBase):
    def test_intake_sees_none_then_hearing_creates_during_await(self):
        # 受入: 取込 A が record=None を得た後、await 中にヒアリング B が作成 → A は作らない
        async def _ai_then_hearing_creates(*a, **k):
            await main._process_line_event("tok", USER, "はじめまして")     # B（実経路）
            return await _auto_ai({"顧客名": NAME})(*a, **k)
        self.ai.side_effect = _ai_then_hearing_creates
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")
        self.assertEqual(len(self.app21()), 1)
        self.assertEqual(len(self.app21_creates()), 1)     # B の 1 件だけ
        (rid,) = self.app21()
        self.assertEqual(self.fake.val("KINTONE_APP_ID", rid, "顧客名"), NAME)
        self.assertEqual(self.fake.val("KINTONE_APP_ID", rid, "問い合わせ業者名"),
                         "テスト債権回収株式会社")

    def test_hearing_sees_none_then_intake_creates_during_await(self):
        # 逆向き: ヒアリング B が _known_rec=None を得た後、AI の await 中に取込 A が作成
        self.ai.side_effect = _auto_ai({"顧客名": NAME})

        async def _ask_then_intake_creates(*a, **k):
            assert await hri.run_jikou(USER, "山田太郎です", EVT) == "written"   # A（実経路）
            return HEARING_REPLY
        self.ask.side_effect = _ask_then_intake_creates
        _run(main._process_line_event("tok", USER, "山田太郎です"))
        self.assertEqual(len(self.app21()), 1)
        self.assertEqual(len(self.app21_creates()), 1)     # A の最小レコード 1 件だけ
        (rid,) = self.app21()
        self.assertEqual(main.kintone_record_ids[USER], rid)
        # B は create せず統合へ振り替え（空欄のみ）
        self.assertEqual(self.fake.val("KINTONE_APP_ID", rid, "問い合わせ業者名"),
                         "テスト債権回収株式会社")
        self.assertEqual(self.fake.val("KINTONE_APP_ID", rid, "顧客名"), NAME)

    def test_hearing_x_hearing_concurrent_single_create(self):
        gate = asyncio.Event()

        async def _ask(*a, **k):
            await gate.wait()                              # 2 つとも「未作成」を見た後に進む
            return HEARING_REPLY
        self.ask.side_effect = _ask

        async def _both():
            t1 = asyncio.ensure_future(main._process_line_event("t1", USER, "一通目"))
            t2 = asyncio.ensure_future(main._process_line_event("t2", USER, "二通目"))
            await asyncio.sleep(0.05)
            gate.set()
            await asyncio.gather(t1, t2)
        _run(_both())
        self.assertEqual(len(self.app21()), 1)
        self.assertEqual(len(self.app21_creates()), 1)

    def test_intake_x_intake_concurrent_single_create(self):
        self.ai.side_effect = _auto_ai({"顧客名": NAME})

        async def _both():
            return await asyncio.gather(hri.run_jikou(USER, "山田太郎です", EVT),
                                        hri.run_jikou(USER, "山田太郎です", EVT2))
        _run(_both())
        self.assertEqual(len(self.app21()), 1)
        self.assertEqual(len(self.app21_creates()), 1)

    def test_intake_x_intake_without_intake_lock_still_single_create(self):
        # 取込自身のユーザー別 Lock に頼らない（作成区間だけで成立する）ことの確認
        self.ai.side_effect = _auto_ai({"顧客名": NAME, "furigana": KANA})

        class _NoLock:
            async def __aenter__(self):
                return None

            async def __aexit__(self, *a):
                return False
        with patch.object(hri, "_lock", lambda cfg, uid: _NoLock()):
            async def _both():
                return await asyncio.gather(hri.run_jikou(USER, "山田太郎です", EVT),
                                            hri.run_jikou(USER, "山田太郎です", EVT2))
            _run(_both())
        self.assertEqual(len(self.app21()), 1)
        self.assertEqual(len(self.app21_creates()), 1)


# ── HRI-03: ストア側の一意制約が発火した場合の収束（最終防衛線） ────────────────────
class TestUniqueConstraintConvergence(_CreateBase):
    def setUp(self):
        super().setUp()
        self.fake.unique_envs = {"APP_HOUKI", "KINTONE_APP_ID"}   # App 21 も一意制約あり
        real_create = self.fake.create_record
        state = {"raced": False}

        async def _create(app, fields):
            # 別プロセスが「区間内の再検索」と「作成」の間に同じユーザーのレコードを作った
            if app.app_id_env == "KINTONE_APP_ID" and not state["raced"]:
                state["raced"] = True
                self.fake.add_jikou(rid="77")
            return await real_create(app, fields)
        p = patch.object(hub_kintone, "create_record", _create)
        p.start()
        self.addCleanup(p.stop)
        self.fake.create_record = _create                  # main.post_to_kintone の代役も同じ経路

    def test_intake_converges_to_existing_record(self):
        self.ai.side_effect = _auto_ai({"顧客名": NAME})
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "written")
        self.assertEqual(sorted(self.app21()), ["77"])     # 追加作成なし
        self.assertEqual(self.fake.val("KINTONE_APP_ID", "77", "顧客名"), NAME)
        self.assertIn("案件レコードNo.77", self.notice())
        self.assertEqual(self.fake.markers(), [RESERVE, DONE])

    def test_hearing_converges_to_existing_record_and_merges(self):
        _run(main._process_line_event("tok", USER, "はじめまして"))
        self.assertEqual(sorted(self.app21()), ["77"])
        self.assertEqual(main.kintone_record_ids[USER], "77")
        self.assertEqual(self.fake.val("KINTONE_APP_ID", "77", "問い合わせ業者名"),
                         "テスト債権回収株式会社")

    def test_create_or_adopt_vocabulary(self):
        async def _go():
            return await jikou_case_create.create_or_adopt(
                USER, lambda: hub_kintone.create_record(
                    jikou_case_create.APP_JIKOU_CASE, {"LINEユーザーID": USER}))
        self.assertEqual(_run(_go()), ("77", jikou_case_create.CONVERGED, 1))
        self.assertEqual(_run(_go()), ("77", jikou_case_create.ADOPTED, 1))

    def test_create_failure_without_existing_record_still_raises(self):
        async def _boom():
            raise hub_kintone.KintoneError(520, "GAIA_XX", "down")

        async def _go():
            return await jikou_case_create.create_or_adopt("U_other_user", _boom)
        with self.assertRaises(hub_kintone.KintoneError):
            _run(_go())


class TestCreateSectionDetails(_CreateBase):
    def test_created_vocabulary_and_section_released(self):
        async def _go():
            out = await jikou_case_create.create_or_adopt(
                USER, lambda: hub_kintone.create_record(
                    jikou_case_create.APP_JIKOU_CASE, {"LINEユーザーID": USER}))
            return out, user_section_mod.active_keys()
        (rid, how, count), keys = _run(_go())
        self.assertEqual((how, count), (jikou_case_create.CREATED, 0))
        self.assertEqual(sorted(self.app21()), [rid])
        self.assertEqual(keys, [])                         # 区間の解放漏れなし

    def test_section_released_after_exception(self):
        async def _boom():
            raise RuntimeError("x")

        async def _go():
            try:
                await jikou_case_create.create_or_adopt(USER, _boom)
            except RuntimeError:
                pass
            return user_section_mod.active_keys()
        self.assertEqual(_run(_go()), [])

    def test_different_users_are_not_serialized(self):
        order = []

        async def _slow(tag, delay):
            async with user_section_mod.user_section("jikou", tag):
                order.append(f"in:{tag}")
                await asyncio.sleep(delay)
                order.append(f"out:{tag}")

        async def _go():
            await asyncio.gather(_slow("U_a", 0.05), _slow("U_b", 0.0))
        _run(_go())
        self.assertEqual(order[:2], ["in:U_a", "in:U_b"])  # U_b は U_a を待たない

    def test_same_user_is_serialized(self):
        order = []

        async def _slow(tag, delay):
            async with user_section_mod.user_section("jikou", USER):
                order.append(f"in:{tag}")
                await asyncio.sleep(delay)
                order.append(f"out:{tag}")

        async def _go():
            await asyncio.gather(_slow("a", 0.05), _slow("b", 0.0))
        _run(_go())
        self.assertEqual(order, ["in:a", "out:a", "in:b", "out:b"])

    def test_re_search_finds_multiple_records_means_no_write(self):
        # 区間内の再検索で複数件（他プロセスの二重作成の痕跡）→ 書かない・要確認通知
        async def _ai_then_two_records(*a, **k):
            self.fake.add_jikou(rid="80")
            self.fake.add_jikou(rid="81")
            return await _auto_ai({"顧客名": NAME})(*a, **k)
        self.ai.side_effect = _ai_then_two_records
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "ambiguous")
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual(self.app21_creates(), [])
        self.assertIn("案件レコードが複数ある", self.notice())

    def test_re_search_failure_falls_back_to_create(self):
        # 再検索の失敗は従来挙動（作成）を維持=ヒアリング第 1 段階の内容を失わない
        async def _hook(env, query):
            if env == "KINTONE_APP_ID" and "LINEユーザーID" in query:
                raise hub_kintone.KintoneError(520, "GAIA_XX", "search down")
        self.fake.on_search = _hook
        _run(main._process_line_event("tok", USER, "はじめまして"))
        self.assertEqual(len(self.app21_creates()), 1)


# ── HRI-03: 相続放棄（App 40）— ストアの作成区間+一意制約の収束 ─────────────────────
class TestHoukiNoDoubleCreate(_Base):
    FIELDS = {"被相続人氏名": "山田一郎"}

    def houki_creates(self) -> list:
        return [c for c in self.fake.create_calls if c[0] == "APP_HOUKI"]

    def test_stale_none_is_not_carried_over(self):
        # 呼び出し側は existing=None（冒頭の検索結果）だが、実際は既に作成済み
        self.fake.add_houki()
        rid, problems, choice = _run(store.apply_hearing_fields(USER, dict(self.FIELDS), None))
        self.assertEqual((rid, problems, choice), ("50", [], []))
        self.assertEqual(self.houki_creates(), [])         # 修正前: create を試みて制約で失敗
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "被相続人氏名"), "山田一郎")

    def test_three_combinations_concurrent_single_create(self):
        # 取込×取込・ヒアリング×ヒアリング・取込×ヒアリングはいずれもこの単一の入口を通る
        async def _both():
            return await asyncio.gather(
                store.apply_hearing_fields(USER, dict(self.FIELDS), None),
                store.apply_hearing_fields(USER, {"被相続人ふりがな": "やまだいちろう"}, None))
        (rid1, *_), (rid2, *_) = _run(_both())
        self.assertEqual(rid1, rid2)
        self.assertEqual(len(self.fake.rows["APP_HOUKI"]), 1)
        self.assertEqual(len(self.houki_creates()), 1)
        self.assertEqual(self.fake.val("APP_HOUKI", rid1, "被相続人氏名"), "山田一郎")
        self.assertEqual(self.fake.val("APP_HOUKI", rid1, "被相続人ふりがな"), "やまだいちろう")

    def test_intake_and_hearing_paths_share_the_entry(self):
        self.ai.return_value = _ai({"顧客名": (True, NAME, "high")}, fill=hri.HOUKI.fields)

        async def _both():
            return await asyncio.gather(
                hri.run_houki(USER, "山田太郎です", EVT),                       # 取込
                store.apply_hearing_fields(USER, dict(self.FIELDS), None))      # ヒアリング
        _run(_both())
        self.assertEqual(len(self.fake.rows["APP_HOUKI"]), 1)
        self.assertEqual(len(self.houki_creates()), 1)

    def test_unique_constraint_fires_then_converges(self):
        # 別プロセスが「区間内の再検索」と「作成」の間に作成 → 制約発火 → 既存へ収束
        real_create = self.fake.create_record
        state = {"raced": False}

        async def _create(app, fields):
            if app.app_id_env == "APP_HOUKI" and not state["raced"]:
                state["raced"] = True
                self.fake.add_houki(rid="60")
            return await real_create(app, fields)
        with patch.object(hub_kintone, "create_record", _create):
            rid, problems, choice = _run(
                store.apply_hearing_fields(USER, dict(self.FIELDS), None))
        self.assertEqual((rid, problems, choice), ("60", [], []))
        self.assertEqual(sorted(self.fake.rows["APP_HOUKI"]), ["60"])
        self.assertEqual(self.fake.val("APP_HOUKI", "60", "被相続人氏名"), "山田一郎")

    def test_existing_passed_by_caller_is_unchanged_path(self):
        rec = self.fake.add_houki()
        calls_before = len(self.fake.search_calls)
        _run(store.apply_hearing_fields(USER, dict(self.FIELDS), rec))
        self.assertEqual(len(self.fake.search_calls), calls_before)   # 再検索なし=従来どおり
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "被相続人氏名"), "山田一郎")


if __name__ == "__main__":
    unittest.main()
