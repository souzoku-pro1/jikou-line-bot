"""EDGE-1（QUEUE-2026-10-14-G G-2）: 境界値・性質テスト（hypothesis・既存テストは不変）

性質（票の 1)〜6)）:
 1) 料金計算 chat_responder.fee_total_yen: 任意の n>=1 で 44,000(n=1)／88,000(n=2)／88,000+22,000×(n−2)(n>=3)。
    追加依頼は通算社数の差額のみ（fee_total_yen(prev+add) − fee_total_yen(prev)）。負数・0・巨大数・文字列混入は例外。
    費用ガード（apply_server_guards）: 社数の判定不能は必ず承認降格。
 2) 社数読取 chat_responder._creditor_count_mentioned: 算用・全角・漢数字・複合漢数字（二十三 等）を網羅。
    「数社」「複数社」は判定不能（COUNT_UNKNOWN）。判定不能は承認降格。
 3) 熟慮期間 hub.houki_jukuryo.jukuryo_deadline: 全日付で「3 か月後の応当日の前日・応当日なしは月末の前日」。
    月末・うるう年 2/29・年跨ぎの表と、JST/UTC 境界（23:00〜翌 01:00 JST）の「今日」。
 4) 送信台帳: 同一 key の同時 2 呼出で片方のみ True・期限経過（回収後）の finish は late・未開始の finish は skipped・
    heartbeat 停止後の recover_stale_started で回収（時間は timing_config の補正値・sleep は monkeypatch）。
 5) 定型文選択器（Q2）: 任意の入力（ランダム・空・絵文字・制御文字・10 万字）で例外を出さず、定型 ID（閉集合）または
    空（=承認降格）。経路全体（V2 ON の顧客対応）も「閉集合の定型 または 承認降格」。
 6) 承認画面（Q4）: 承認なしの POST・他人の案件 ID・存在しない ID・二重承認で送信ゼロ。

hypothesis は database=None（.hypothesis/ を作らない）・derandomize=True（決定的・CI 向け）・deadline=None。
既存テストの fixture（test_jikou_reply_q1a／q1c／q3b／q4・test_server_guards）を再利用し、新しい送信経路は作らない。
"""

import asyncio
import calendar
import datetime
import os
import types
import unittest
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from test_jikou_reply_q1a import JIKOU, USER, _DbMixin as _LedgerMixin, _FakeClient as _LedgerClient, _run  # noqa: E402
from test_jikou_reply_q1c import _FakeClient as _FlowClient, _compose, _texts  # noqa: E402
from test_jikou_reply_q2 import RENDER_SNAPSHOT  # noqa: E402
from test_jikou_reply_q3b import _Mixin as _FlowMixin  # noqa: E402
from test_jikou_reply_q4 import _DraftMixin, _auth, _client  # noqa: E402
from test_server_guards import _result  # noqa: E402

import chat_responder as cr  # noqa: E402
from hub import db  # noqa: E402
from hub import houki_jukuryo as HJ  # noqa: E402
from hub import houki_phone_triage as HT  # noqa: E402
from hub import jikou_reply_render as R  # noqa: E402
from hub import jikou_reply_select as S  # noqa: E402
from hub import jikou_reply_texts as T  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import reply_draft as rd  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_drafts_view as dv  # noqa: E402

settings.register_profile(
    "edge", database=None, deadline=None, derandomize=True, max_examples=150,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large, HealthCheck.filter_too_much])
settings.load_profile("edge")

ALL_IDS = tuple(RENDER_SNAPSHOT)
_KD = "零一二三四五六七八九"
_WEIRD_TEXT = st.one_of(
    st.text(max_size=120), st.text(alphabet=st.characters(categories=("Cc", "Cf", "So", "Zs")), max_size=40),
    st.just(""), st.just("🙂" * 20), st.just("\x00\x01\x1f\x7f"), st.just("あ" * 100_000),
    st.binary(max_size=40).map(lambda b: b.decode("latin-1")))


def _fee_expected(n: int) -> int:
    return 44_000 if n == 1 else 88_000 if n == 2 else 88_000 + 22_000 * (n - 2)


def _kanji(n: int) -> str:
    """1〜999 を _parse_kanji_number の文法で書く（一十・一百は書かない）。"""
    h, t, d = n // 100, (n // 10) % 10, n % 10
    s = ""
    if h:
        s += (_KD[h] if h > 1 else "") + "百"
    if t:
        s += (_KD[t] if t > 1 else "") + "十"
    if d:
        s += _KD[d]
    return s


def _fullwidth(n: int) -> str:
    return "".join(chr(0xFF10 + int(c)) for c in str(n))


# ── 1) 料金計算 ─────────────────────────────────────────────────────────────────
@given(st.integers(min_value=1, max_value=999))
def test_fee_total_follows_rule(n):
    assert cr.fee_total_yen(n) == _fee_expected(n)


@given(st.integers(min_value=1, max_value=500), st.integers(min_value=1, max_value=500))
def test_fee_additional_request_is_difference_by_cumulative_count(prev, add):
    diff = cr.fee_total_yen(prev + add) - cr.fee_total_yen(prev)
    base_part = max(0, min(prev + add, cr.FEE_BASE_MAX_COUNT) - min(prev, cr.FEE_BASE_MAX_COUNT))
    assert diff == base_part * cr.FEE_BASE_YEN + (add - base_part) * cr.FEE_EXTRA_YEN
    assert diff >= cr.FEE_EXTRA_YEN * add            # 割引なし・差額は常に正


@given(st.one_of(st.integers(max_value=0), st.integers(min_value=1000), st.text(max_size=12), st.floats(),
                 st.none(), st.booleans(), st.lists(st.integers(), max_size=3)))
def test_fee_out_of_domain_raises(x):
    with pytest.raises((ValueError, TypeError)):
        cr.fee_total_yen(x)


def test_fee_guard_demotes_when_count_unknown_and_accepts_known_counts():
    assert all(p in cr.FEE_GUIDE_TEXT for p in cr.FEE_REQUIRED_PHRASES + cr.FEE_MULTI_REQUIRED_PHRASES)
    for tok in ("3", "三", "二十三", "１２", "1", "2"):
        g = cr.apply_server_guards(_result(reply=cr.FEE_GUIDE_TEXT, category="費用の定型案内"), [], f"{tok}社です")
        assert g.can_auto_send, (tok, g.demotion_reasons)
    for tok in ("十十", "二三", "一十", "1十", "十百", "数", "複数"):
        g = cr.apply_server_guards(_result(reply=cr.FEE_GUIDE_TEXT, category="費用の定型案内"), [], f"{tok}社です")
        assert not g.can_auto_send, tok
        assert cr.FEE_COUNT_UNKNOWN_REASON in str(g.demotion_reasons), tok


# ── 2) 社数読取 ─────────────────────────────────────────────────────────────────
@given(st.integers(min_value=1, max_value=99_999))
def test_count_ascii_and_fullwidth_digits(n):
    assert cr._creditor_count_mentioned(f"{n}社") == n
    assert cr._creditor_count_mentioned(f"{_fullwidth(n)}社です") == n
    assert cr._creditor_count_mentioned(f"債権者は {n} 社あります") == n


@given(st.integers(min_value=1, max_value=999))
def test_count_kanji_including_compound(n):
    assert cr._creditor_count_mentioned(f"{_kanji(n)}社") == n


@given(st.lists(st.integers(min_value=1, max_value=999), min_size=1, max_size=4))
def test_count_multiple_mentions_take_max(ns):
    text = "、".join(f"{_kanji(n) if i % 2 else n}社" for i, n in enumerate(ns))
    assert cr._creditor_count_mentioned(text) == max(ns)


def test_count_unparseable_and_vague_expressions_are_unknown():
    for tok in ("十十", "二三", "一十", "1十", "十百", "三3"):
        assert cr._creditor_count_mentioned(f"{tok}社") is cr.COUNT_UNKNOWN, tok
    for text in ("数社あります", "複数社です", "複数の業者から借りています"):
        assert cr._creditor_count_mentioned(text) is cr.COUNT_UNKNOWN, text


@given(st.text(max_size=200))
def test_count_never_raises_and_without_sha_is_zero(text):
    got = cr._creditor_count_mentioned(text)
    assert got is None or (isinstance(got, int) and got >= 0)
    if "社" not in text:
        assert got == 0


# ── 3) 熟慮期間 ─────────────────────────────────────────────────────────────────
def _expected_deadline(start: datetime.date) -> datetime.date:
    m0 = start.month + 3
    y, m = start.year + (m0 - 1) // 12, (m0 - 1) % 12 + 1
    last = calendar.monthrange(y, m)[1]
    anniv = datetime.date(y, m, start.day if start.day <= last else last)
    return anniv - datetime.timedelta(days=1)


@given(st.dates(min_value=datetime.date(1990, 1, 1), max_value=datetime.date(2100, 12, 31)))
def test_jukuryo_deadline_rule_for_all_dates(start):
    got = HJ.jukuryo_deadline(start)
    assert got == _expected_deadline(start)
    assert HT.shanai_deadline(start) == got
    assert 88 <= (got - start).days <= 91


def test_jukuryo_deadline_table_month_end_leap_and_year_crossing():
    table = {
        datetime.date(2026, 1, 31): datetime.date(2026, 4, 29),      # 4/31 なし → 4/30 の前日
        datetime.date(2024, 2, 29): datetime.date(2024, 5, 28),      # うるう日起算
        datetime.date(2023, 11, 29): datetime.date(2024, 2, 28),     # 翌年うるう年の 2/29 の前日
        datetime.date(2023, 11, 30): datetime.date(2024, 2, 28),     # 2/30 なし → 2/29 の前日
        datetime.date(2026, 11, 30): datetime.date(2027, 2, 27),     # 平年: 2/28 の前日
        datetime.date(2026, 10, 31): datetime.date(2027, 1, 30),     # 年跨ぎ
        datetime.date(2026, 12, 31): datetime.date(2027, 3, 30),
        datetime.date(2026, 3, 1): datetime.date(2026, 5, 31),
        datetime.date(2026, 5, 31): datetime.date(2026, 8, 30),
        datetime.date(2026, 8, 31): datetime.date(2026, 11, 29),
        datetime.date(2027, 11, 30): datetime.date(2028, 2, 28),     # 2028 うるう年: 2/29 の前日
    }
    for start, want in table.items():
        assert HJ.jukuryo_deadline(start) == want, start


class _FixedDT(datetime.datetime):
    fixed: datetime.datetime | None = None

    @classmethod
    def now(cls, tz=None):
        return cls.fixed.astimezone(tz) if tz else cls.fixed.replace(tzinfo=None)


@given(st.dates(min_value=datetime.date(2026, 1, 1), max_value=datetime.date(2028, 12, 31)),
       st.integers(min_value=14 * 60, max_value=16 * 60))
def test_today_jst_across_utc_boundary(day, minute):
    utc = datetime.datetime.combine(day, datetime.time(minute // 60, minute % 60), tzinfo=datetime.timezone.utc)
    want = (utc + datetime.timedelta(hours=9)).date()
    _FixedDT.fixed = utc
    ns = types.SimpleNamespace(datetime=_FixedDT, date=datetime.date, timedelta=datetime.timedelta,
                               timezone=datetime.timezone)
    with patch.object(HJ, "datetime", _FixedDT), patch.object(HT, "datetime", ns):
        assert HJ._today_jst() == want
        assert HT._today_jst() == want
        deadline = HJ.jukuryo_deadline(datetime.date(2026, 1, 31))
        assert HJ.remaining_days(deadline, HJ._today_jst()) == (deadline - want).days
    assert want == (utc.date() + datetime.timedelta(days=1) if utc.hour >= 15 else utc.date())


# ── 4) 送信台帳 ─────────────────────────────────────────────────────────────────
_TIMING = {sl.STALE_MINUTES_ENV: "2", sl.DEADLINE_MINUTES_ENV: "1",
           sl.HEARTBEAT_SECONDS_ENV: "10", sl.SEND_TIMEOUT_SECONDS_ENV: "30"}


class TestLedgerProperties(_LedgerMixin):
    def test_same_key_concurrent_two_calls_only_one_true(self):
        gate = asyncio.Event()
        real_post = _LedgerClient.post

        class _Gated(_LedgerClient):
            async def post(self, url, headers=None, json=None):
                await gate.wait()                       # 1 本目の HTTP は 2 本目が台帳を見るまで待つ
                return await real_post(self, url, headers=headers, json=json)

        async def one(first: bool):
            tok = sl.bind_inbound("ev-same")
            try:
                r = await line_channel.push_text(JIKOU, USER, "x")
            finally:
                sl.unbind(tok)
            if not first:
                gate.set()
            return r

        async def both():
            with patch.object(line_channel.httpx, "AsyncClient", _Gated):
                a = asyncio.create_task(one(True))
                await asyncio.sleep(0.05)               # a が begin（started）を終えて HTTP で待つ
                b = asyncio.create_task(one(False))
                return await asyncio.gather(a, b)

        res = _run(both())
        self.assertEqual(res, [True, sl.SEND_UNCONFIRMED])
        self.assertEqual(len(_LedgerClient.calls), 1)
        self.assertEqual([o["state"] for o in self.ops()], ["sent"])

    def test_finish_after_deadline_and_recovery_is_late(self):
        op = _run(sl.begin("jikou", "jikou", USER, "x"))
        self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=sl.stale_started_minutes() + 1))
        self.assertEqual(_run(sl.recover_stale_started()), 1)
        self.assertEqual(_run(sl.finish(op, "sent")), "late")
        self.assertEqual([o["state"] for o in self.ops()], ["unconfirmed"])
        self.assertEqual([h["reason"] for h in self.history(op.op_id)][-2:], ["stale_started", "late_result:sent"])

    def test_finish_after_deadline_without_recovery_is_applied(self):
        """注記: 期限経過だけでは late にならない（所有者一致なら applied）。回収ジョブが先に走ったときだけ late。"""
        op = _run(sl.begin("jikou", "jikou", USER, "x"))
        self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=sl.stale_started_minutes() + 1))
        self.assertEqual(_run(sl.finish(op, "sent")), "applied")

    def test_finish_without_begin_is_skipped(self):
        self.assertEqual(_run(sl.finish(None, "sent")), "skipped")
        self.assertEqual(_run(sl.finish(None, "failed")), "skipped")
        self.assertEqual(self.ops(), [])

    def test_heartbeat_keeps_alive_and_stop_is_recovered(self):
        with patch.dict(os.environ, _TIMING):
            cfg = sl.timing_config()
            self.assertFalse(cfg["defaulted"])
            self.assertEqual((cfg["heartbeat"], cfg["timeout"], cfg["deadline"], cfg["stale"]), (10.0, 30.0, 60.0, 120.0))
            clock = [sl._now()]
            real_sleep = asyncio.sleep
            beats: list = []

            async def fake_sleep(sec):
                clock[0] += datetime.timedelta(seconds=sec)
                beats.append(sec)
                await real_sleep(0)

            with patch.object(sl, "_now", lambda: clock[0]):
                op = _run(sl.begin("jikou", "jikou", USER, "x"))
                t0 = clock[0]

                async def body():
                    with patch.object(sl.asyncio, "sleep", fake_sleep):
                        async with sl.heartbeat_task(op):
                            while (clock[0] - t0).total_seconds() < cfg["stale"] + 30:
                                await real_sleep(0)
                _run(body())
                self.assertTrue(all(b == cfg["heartbeat"] for b in beats) and len(beats) >= 15)
                row = self.ops()[0]
                self.assertEqual(row["state"], "started")
                self.assertGreater(row["last_heartbeat_at"], row["started_at"])
                self.assertEqual(_run(sl.recover_stale_started()), 0)       # 稼働中（heartbeat 継続）は回収しない
                clock[0] += datetime.timedelta(seconds=cfg["stale"] + 1)     # heartbeat 停止 → 閾値超
                self.assertEqual(_run(sl.recover_stale_started()), 1)
                self.assertEqual(self.ops()[0]["state"], "unconfirmed")
                self.assertEqual(_run(sl.recover_stale_started()), 0)       # 冪等
        self.assertEqual(len(_LedgerClient.calls), 0)                       # 自動再送なし


# ── 5) 定型文選択器（Q2） ──────────────────────────────────────────────────────
@given(stage=st.one_of(st.sampled_from(S.STAGES), st.text(max_size=8)),
       intent=st.one_of(st.sampled_from(T.INTENTS), st.text(max_size=12)),
       first=st.booleans(), court=st.booleans(), denied=st.booleans(),
       used=st.lists(st.one_of(st.sampled_from(ALL_IDS), st.text(max_size=10)), max_size=5),
       name=_WEIRD_TEXT, lp=_WEIRD_TEXT, eval_date=st.one_of(st.none(), st.dates()))
def test_selector_never_raises_and_returns_closed_set_or_empty(stage, intent, first, court, denied, used, name, lp, eval_date):
    sel = S.select(S.Inputs(stage=stage, intent=intent, first_reply_sent=first, used_ids=tuple(used),
                            creditor_name=name, last_payment_text=lp, court_claim=court, court_docs_denied=denied,
                            eval_date=eval_date))
    assert isinstance(sel, S.Selection)
    assert all(i in ALL_IDS and T.is_selectable(i) for i in sel.ids)
    assert len(sel.ids) <= S.MAX_IDS + 1
    assert len(set(sel.ids)) == len(sel.ids)
    assert not (set(sel.ids) & set(used))
    if sel.empty:
        return                                           # 承認降格（呼出側が無言+対応待ち）
    try:
        R.render(sel.ids, sel.slots)                     # 描画は成功するか RenderError（=降格）だけ
    except R.RenderError:
        pass


@given(_WEIRD_TEXT)
def test_parse_last_payment_and_rule_lookup_never_raise(text):
    got = R.parse_last_payment(text)
    assert got is None or isinstance(got, datetime.date)
    rule = T.rule_for(text)
    assert rule["status"] in (T.RULE_CONFIRMED, T.RULE_PROVISIONAL)


class TestQ2PathTable(_FlowMixin):
    CASES = ("", "🙂🙂🙂", "\x00\x01\x1f\x7f", "あ" * 100_000, "費用は？", "三社電機の 1 社です", "​‎",
             "' OR 1=1 --", "<script>alert(1)</script>", "𝔘𝔫𝔦𝔠𝔬𝔡𝔢")
    INTENTS = ({"intent": "費用", "creditor_name": ""}, {"intent": "???", "creditor_name": "🙂"},
               {"intent": "", "creditor_name": "あ" * 10_000}, RuntimeError("down"))

    def test_every_input_yields_closed_set_text_or_demotion(self):
        closed = {T.text(i) for i in ALL_IDS}
        for text in self.CASES:
            for intent in self.INTENTS:
                self.on()
                _FlowClient.calls.clear()
                self.approval.reset_mock()
                if isinstance(intent, Exception):
                    self.intent.side_effect = intent
                else:
                    self.intent.side_effect = None
                    self.intent.return_value = intent
                self.customer(_compose("自由文の回答です。", category="費用の定型案内"), text=text)
                sent = _texts()
                self.assertTrue(set(sent) <= closed, (repr(text)[:30], intent, sent))
                if not sent:
                    self.assertTrue(self.approval.called or self.pending(), (repr(text)[:30], intent))


# ── 6) 承認画面（Q4） ──────────────────────────────────────────────────────────
class TestQ4ApprovalProperties(_DraftMixin):
    def _state(self, did: int) -> str:
        return {int(r["draft_id"]): r["state"] for r in self.drafts()}[did]

    def test_post_without_session_sends_nothing(self):
        did = self.make_draft()
        f = self.open_form(did)
        for data in (dict(f, action="send", text=self.gen.return_value, confirm="1"), {}, {"action": "reject"}):
            r = _client.post(f"{dv.PAGE}/{did}/act", data=data, follow_redirects=False)
            self.assertEqual(r.status_code, 303)
            self.assertIn("/app/login", r.headers["location"])
        self.assertEqual(_texts(), [])
        self.assertEqual(self._state(did), rd.STATE_PROPOSED)

    def test_nonexistent_and_foreign_ids_send_nothing(self):
        did = self.make_draft()
        f = self.open_form(did)

        async def _foreign():
            async with db.session_scope() as s:
                now = datetime.datetime.now(datetime.timezone.utc)
                r = await s.execute(sa.insert(rd.reply_draft).values(
                    business="houki", pending_id=None, line_user_hash="h" * 32, category="x",
                    inbound_text_version="v", draft_text="他業務の下書き", guard_ok=True, exemplar_ids="[]",
                    state=rd.STATE_PROPOSED, created_at=now, updated_at=now))
                return int(r.inserted_primary_key[0])
        foreign = _run(_foreign())
        for bad in ("999999", "0", str(foreign)):
            self.assertEqual(_client.get(f"{dv.PAGE}/{bad}", headers=_auth()).status_code, 404, bad)
            r = self.act(bad, dict(f, action="send", text="x", confirm="1"))
            self.assertEqual(r.status_code, 404, bad)
        for bad in ("abc", "1e3", "-1", "１", " 1"):
            r = self.act(bad, dict(f, action="send", text="x", confirm="1"))
            self.assertEqual(r.status_code, 400, bad)
        for bad in ("1/2", ""):                                   # 別 route（GET のみ）に当たる形は 405・送信なし
            r = self.act(bad, dict(f, action="send", text="x", confirm="1"))
            self.assertEqual(r.status_code, 405, bad)
        self.assertEqual(_texts(), [])
        self.assertEqual(self._state(did), rd.STATE_PROPOSED)
        self.assertEqual(self._state(foreign), rd.STATE_PROPOSED)

    @settings(max_examples=80)
    @given(did=st.text(alphabet="0123456789abcdef./ -", max_size=14),
           action=st.one_of(st.sampled_from(["send", "", "SEND", "approve", "reject "]), st.text(max_size=6)),
           op=st.text(alphabet="0123456789abcdef", max_size=32), ticket=st.text(max_size=48),
           text=st.text(max_size=20), confirm=st.sampled_from(["", "1", "0", "true", "１"]))
    def test_random_forms_never_send(self, did, action, op, ticket, text, confirm):
        assume(action != "reject")
        if not hasattr(self, "_did"):
            self._did = self.make_draft()
        r = self.act(did, {"action": action, "op_id": op, "ticket": ticket, "text": text, "confirm": confirm})
        self.assertIn(r.status_code, (303, 400, 404, 405))      # 405=別 route（GET のみ）に当たった POST・送信なし
        if r.status_code == 303:
            self.assertNotIn("done=sent", r.headers["location"])
        self.assertEqual(_texts(), [])
        self.assertEqual(self._state(self._did), rd.STATE_PROPOSED)

    def test_double_approval_and_back_resubmit_send_once(self):
        did = self.make_draft()
        f = self.open_form(did)
        data = dict(f, action="send", text=self.gen.return_value, confirm="1")
        r1 = self.act(did, data)
        r2 = self.act(did, data)                                   # 二重クリック／戻る→再送信（同じ op_id・ticket）
        self.assertIn("done=sent", r1.headers["location"])
        self.assertIn("done=already", r2.headers["location"])
        self.assertEqual(_texts(), [self.gen.return_value])
        ops = [o for o in self.ops() if o["actor"] == sl.ACTOR_APPROVED_DRAFT]
        self.assertEqual([o["state"] for o in ops], ["sent"])
        self.assertEqual(self._state(did), rd.STATE_SENT)
        r3 = self.act(did, dict(data, action="reject"))            # 送信後の却下も副作用なし
        self.assertIn("done=already", r3.headers["location"])
        self.assertEqual(len(_texts()), 1)


if __name__ == "__main__":
    unittest.main()
