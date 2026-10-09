"""HOUKI-JUKURYO-2（大野裁定 2026-10-08・法的判断）/ fix1（Codex BH-01〜04）: 相続放棄 熟慮期間 監視。

CRON-1 のテスト（申告 3 日付の代用順・法定/社内の 2 本立て・ONE_SHOT/DAILY・履歴欄）は
本票で挙動ごと差し替えられたため、同じ性質（計算・対象・冪等・送信→書込の順序・件数が落ちない・
ページング・PII なし・登録）を新裁定の形で検証する。fix1 BH-04 で「期限が変われば通知キーが変わる」
「刻印失敗後の訂正が旧通知に抑止されない」の 2 性質を復元。

- 期限計算（応当日の前日・応当日なし＝その月の末日の前日・閏年・年またぎ・月初）
- 入力は 起算日_確定 のみ（申告 3 日付・起算点確定済・法定満了日・社内締切日 は読まない）
- 対象 status の網羅（受任後 8 値・受任前は対象外・申述提出日ありは対象外）
- 熟慮期間期限／残日数 の保存（差分だけ書く・起算日_確定 変更で上書き・空に戻れば空に戻す）
- 通知の 1 回性の正は send_ledger（1 レコード×1 閾値 = 1 操作 = 1 push・purpose other・
  channel houki_jukuryo・業務キー {種別}:{レコード}:{期限}）。刻印は写し（失敗しても再通知しない・
  次回は刻印だけ再試行）。unconfirmed は自動再送なし。台帳が使えないときだけ写しで抑止
- BH-01: 409 再取得後に対象外なら更新中止。BH-03: 未確定件数の 1 日 1 回も台帳（再起動相当で重複なし）
- PII 非漏洩（本文・ログ）・書く欄は 3 欄の閉集合・ページング・登録（8/13/18）・phone_triage と同式
"""

import asyncio
import logging
import os
import re
import shutil
import tempfile
import time
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

from hub import db
from hub import houki_jukuryo as hj
from hub import houki_phone_triage as tri
from hub import houki_profile
from hub import kintone as hub_kintone
from hub import notify as hub_notify
from hub import scheduler as hub_scheduler
from hub import send_ledger as sl

TODAY = date(2026, 9, 8)
NAME = "山田太郎"
KANA = "やまだたろう"
ADMIN = "Uadmin000000000000000000000000001"

ALL_POST = ("受任", "書類収集中", "申述書作成", "裁判所提出済", "照会書対応", "受理", "債権者通知", "完了")


def _rec(rid="1", status="受任", submitted="", start="", deadline="", remaining="",
         notified=(), knew="2026-05-01", death="2026-04-01", revision="3"):
    return {
        "$id": {"value": rid}, "$revision": {"value": revision},
        "status": {"value": status}, "申述提出日": {"value": submitted},
        "起算日_確定": {"value": start},
        "熟慮期間期限": {"value": deadline}, "残日数": {"value": remaining},
        "通知済み閾値": {"value": list(notified)},
        # 読んではいけない欄（値が入っていても結果に影響しない）
        "相続人と知った日_申告": {"value": knew}, "死亡を知った日_申告": {"value": knew},
        "死亡日_申告": {"value": death}, "起算点確定済": {"value": "no"},
        "法定満了日": {"value": "2026-09-01"}, "社内締切日": {"value": "2026-08-20"},
        "顧客名": {"value": NAME}, "furigana": {"value": KANA},
    }


def _start_for_remaining(remaining: int, today: date = TODAY) -> str:
    """今日の残日数が remaining になる 起算日_確定 を逆算（応当日の前日の式に合わせる）。"""
    target = date.fromordinal(today.toordinal() + remaining)        # 期限日
    for back in range(85, 96):
        cand = date.fromordinal(target.toordinal() - back)
        if hj.jukuryo_deadline(cand) == target:
            return cand.isoformat()
    raise AssertionError("no start found")


def _run(coro):
    return asyncio.run(coro)


# ── 期限計算（裁定 (a)） ─────────────────────────────────────────────────────
class TestDeadlineFormula(unittest.TestCase):
    def test_normal_anniversary_minus_one(self):
        self.assertEqual(hj.jukuryo_deadline(date(2026, 4, 10)), date(2026, 7, 9))      # 票の例
        self.assertEqual(hj.jukuryo_deadline(date(2026, 6, 15)), date(2026, 9, 14))

    def test_no_anniversary_is_month_end_minus_one_non_leap(self):
        self.assertEqual(hj.jukuryo_deadline(date(2026, 11, 30)), date(2027, 2, 27))

    def test_no_anniversary_leap_year(self):
        self.assertEqual(hj.jukuryo_deadline(date(2027, 11, 30)), date(2028, 2, 28))
        self.assertEqual(hj.jukuryo_deadline(date(2027, 11, 29)), date(2028, 2, 28))   # 応当日あり → 前日

    def test_31st_to_30_day_month(self):
        self.assertEqual(hj.jukuryo_deadline(date(2026, 1, 31)), date(2026, 4, 29))
        self.assertEqual(hj.jukuryo_deadline(date(2026, 8, 31)), date(2026, 11, 29))

    def test_year_rollover_and_first_of_month(self):
        self.assertEqual(hj.jukuryo_deadline(date(2026, 12, 20)), date(2027, 3, 19))
        self.assertEqual(hj.jukuryo_deadline(date(2026, 3, 1)), date(2026, 5, 31))      # 前日が前月へ
        self.assertEqual(hj.jukuryo_deadline(date(2026, 10, 1)), date(2026, 12, 31))

    def test_remaining_days_boundaries(self):
        self.assertEqual(hj.remaining_days(date(2026, 9, 22), TODAY), 14)
        self.assertEqual(hj.remaining_days(date(2026, 9, 15), TODAY), 7)
        self.assertEqual(hj.remaining_days(TODAY, TODAY), 0)
        self.assertEqual(hj.remaining_days(date(2026, 9, 7), TODAY), -1)

    def test_phone_triage_uses_the_same_formula(self):
        self.assertIs(tri.jukuryo_deadline, hj.jukuryo_deadline)
        for d in (date(2026, 4, 10), date(2026, 11, 30), date(2026, 1, 31)):
            self.assertEqual(tri.shanai_deadline(d), hj.jukuryo_deadline(d))


# ── 入力は 起算日_確定 のみ ──────────────────────────────────────────────────
class TestResolveStart(unittest.TestCase):
    def test_confirmed_start_only(self):
        self.assertEqual(hj.resolve_start(_rec(start="2026-04-10")), date(2026, 4, 10))

    def test_declared_dates_and_attorney_deadlines_are_ignored(self):
        r = _rec(start="", knew="2026-05-01", death="2026-04-01")
        r["法定満了日"] = {"value": "2026-09-10"}
        r["起算点確定済"] = {"value": "yes"}
        self.assertIsNone(hj.resolve_start(r))
        self.assertIsNone(hj.compute(r, TODAY))

    def test_invalid_date_is_none(self):
        self.assertIsNone(hj.resolve_start(_rec(start="2026/04/10")))

    def test_compute(self):
        c = hj.compute(_rec(start="2026-06-15"), TODAY)
        self.assertEqual((c.start, c.deadline, c.remaining), (date(2026, 6, 15), date(2026, 9, 14), 6))


# ── 対象 ────────────────────────────────────────────────────────────────────
class TestTargets(unittest.TestCase):
    def test_target_statuses_are_the_profile_post_engagement_set(self):
        self.assertEqual(set(hj.TARGET_STATUSES), set(ALL_POST))
        self.assertEqual(set(hj.TARGET_STATUSES), set(houki_profile.HOUKI_PROFILE.post_engagement_statuses))
        self.assertEqual(len(hj.TARGET_STATUSES), 8)

    def test_is_target_all_post_engagement_statuses(self):
        for s in ALL_POST:
            with self.subTest(status=s):
                self.assertTrue(hj.is_target(_rec(status=s)))

    def test_pre_engagement_and_closed_are_not_targets(self):
        for s in ("", "問い合わせ", "電話判断待ち", "電話調整中", "決済待ち", "契約待ち", "不受任", "辞任"):
            with self.subTest(status=s):
                self.assertFalse(hj.is_target(_rec(status=s)))

    def test_submitted_is_not_target(self):
        self.assertFalse(hj.is_target(_rec(submitted="2026-09-01")))

    def test_search_query_pins_statuses_and_empty_submitted(self):
        q = hj.search_query()
        for s in ALL_POST:
            self.assertIn(f'"{s}"', q)
        self.assertNotIn("問い合わせ", q)
        self.assertIn('申述提出日 = ""', q)
        self.assertIn("order by $id asc limit 500", q)
        self.assertIn("$id > 500", hj.search_query("500"))


# ── 通知判定・保存差分・業務キー（pure） ─────────────────────────────────────
class TestAlertsAndUpdates(unittest.TestCase):
    def test_alert_candidates(self):
        self.assertEqual(hj.alert_candidates(15), [])
        self.assertEqual(hj.alert_candidates(14), [14])
        self.assertEqual(hj.alert_candidates(8), [14])
        self.assertEqual(hj.alert_candidates(7), [14, 7])
        self.assertEqual(hj.alert_candidates(0), [14, 7])
        self.assertEqual(hj.alert_candidates(-3), [14, 7])                 # 超過でも候補（1 回性は台帳）

    def test_ledger_business_key_includes_deadline(self):
        c = hj.compute(_rec(start="2026-06-15"), TODAY)
        self.assertEqual(hj.alert_business_key("12", c), "12:2026-09-14")
        self.assertEqual(hj.ledger_key(hj.LEDGER_KIND_14, hj.alert_business_key("12", c)),
                         "houki_jukuryo_14:12:2026-09-14")
        c2 = hj.compute(_rec(start="2026-06-20"), TODAY)
        self.assertNotEqual(hj.alert_business_key("12", c), hj.alert_business_key("12", c2))   # BH-04 (a)
        self.assertEqual(hj.ledger_key(hj.LEDGER_KIND_UNSET, "2026-09-08"), "houki_jukuryo_unset:2026-09-08")

    def test_field_updates_writes_only_changed(self):
        c = hj.compute(_rec(start="2026-06-15"), TODAY)
        self.assertEqual(hj.field_updates(_rec(start="2026-06-15"), c),
                         {"熟慮期間期限": "2026-09-14", "残日数": "6"})
        self.assertEqual(hj.field_updates(_rec(start="2026-06-15", deadline="2026-09-14", remaining="6"), c), {})
        self.assertEqual(hj.field_updates(_rec(start="2026-06-15", deadline="2026-09-14", remaining="6.0"), c), {})
        self.assertEqual(hj.field_updates(_rec(start="2026-06-15", deadline="2026-09-14", remaining="7"), c),
                         {"残日数": "6"})
        self.assertEqual(hj.field_updates(_rec(start="2026-06-15", deadline="2026-09-20", remaining="6"), c),
                         {"熟慮期間期限": "2026-09-14"})

    def test_field_updates_clears_when_start_removed(self):
        self.assertEqual(hj.field_updates(_rec(start="", deadline="2026-09-14", remaining="6"), None),
                         {"熟慮期間期限": "", "残日数": ""})
        self.assertEqual(hj.field_updates(_rec(start=""), None), {})

    def test_record_writes_merges_marks_and_keeps_existing(self):
        r = _rec(start="2026-06-15", deadline="2026-09-14", remaining="6", notified=("30日前",))
        self.assertEqual(hj.record_writes(r, TODAY, [14, 7]),
                         {"通知済み閾値": ["30日前", "14日前", "7日前"]})
        self.assertEqual(hj.record_writes(_rec(start="2026-06-15", deadline="2026-09-14", remaining="6",
                                               notified=("14日前",)), TODAY, [14]), {})
        self.assertEqual(hj.WRITE_FIELDS, frozenset({"熟慮期間期限", "残日数", "通知済み閾値"}))


# ── 本文（PII なし） ────────────────────────────────────────────────────────
class TestNoticeBody(unittest.TestCase):
    def test_alert_text_has_record_deadline_remaining_only(self):
        c = hj.compute(_rec(start="2026-06-15"), TODAY)
        t = hj.alert_text(TODAY, "12", 7, c)
        self.assertEqual(t, "【相続放棄 熟慮期間】 2026-09-08\n・No.12 期限 2026-09-14 残 6 日（7日前の通知）\n"
                            + hj.NOTICE_FOOTER)
        for leak in (NAME, KANA, "2026-05-01", "2026-04-01"):
            self.assertNotIn(leak, t)

    def test_unset_text_has_count_only(self):
        t = hj.unset_text(TODAY, 3)
        self.assertIn("起算日未確定 3 件", t)
        self.assertNotIn("No.", t)


# ── ジョブ（kintone fake・台帳は sqlite 実体・push は fake） ──────────────────
class _Base(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="hj_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/l.db",
            "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz-token", "LINE_ADMIN_USER_ID": ADMIN})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(sl.metadata.create_all)
        _run(_create())

        self.records: dict[str, dict] = {}
        self.updates: list[tuple] = []
        self.gets: list[str] = []
        self.conflict_next: dict[str, int] = {}
        self.fail_on: set[str] = set()
        self.queries: list[str] = []
        self.refetch_mutator = None            # BH-01: 409 の裏でレコードが変わった体

        async def search_records(app, query, fields=None):
            self.queries.append(query)
            m = re.search(r"\$id > (\d+)", query)
            after = int(m.group(1)) if m else 0
            rows = sorted((r for r in self.records.values() if int(r["$id"]["value"]) > after),
                          key=lambda r: int(r["$id"]["value"]))
            return [{k: dict(v) if isinstance(v, dict) else v for k, v in r.items()}
                    for r in rows[:hj.SEARCH_LIMIT]]

        async def get_record(app, rid):
            self.gets.append(rid)
            if self.refetch_mutator:
                self.refetch_mutator(self.records[rid])
            return {k: dict(v) for k, v in self.records[rid].items()}

        async def update_record(app, rid, fields, revision=None):
            self.updates.append((app.app_id_env, rid, dict(fields), revision))
            if self.conflict_next.get(rid, 0) > 0:
                self.conflict_next[rid] -= 1
                raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
            if rid in self.fail_on:
                raise hub_kintone.KintoneError(500, "GAIA_XX", "down")
            rec = self.records[rid]
            self.assertEqual(revision, rec["$revision"]["value"])
            for k, v in fields.items():
                rec[k] = {"value": v}
            rec["$revision"] = {"value": str(int(revision) + 1)}

        self.push = AsyncMock(return_value=True)          # _push_admin_http の代替（True=2xx）
        self.today = TODAY
        for p in (patch.object(hub_kintone, "search_records", search_records),
                  patch.object(hub_kintone, "get_record", get_record),
                  patch.object(hub_kintone, "update_record", update_record),
                  patch.object(hj, "_push_admin_http", self.push),
                  patch.object(hj, "get_admin_line_user_id", lambda: ADMIN),
                  patch.object(hj, "_today_jst", lambda: self.today)):
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def seed(self, *recs):
        for r in recs:
            self.records[r["$id"]["value"]] = r

    def run_job(self, hour=8):
        return _run(hj.jukuryo_daily_check(hour))

    def val(self, rid, code):
        return self.records[rid][code]["value"]

    def sent_texts(self):
        return [c.args[1] for c in self.push.await_args_list]

    def alert_texts(self):
        return [t for t in self.sent_texts() if hj.UNSET_LABEL not in t]

    def ops(self):
        async def q():
            async with db.session_scope() as s:
                rows = (await s.execute(sa.select(sl.send_operation).order_by(
                    sl.send_operation.c.created_at, sl.send_operation.c.op_id))).mappings().all()
            return [dict(r) for r in rows]
        return _run(q())

    def written_fields(self):
        return sorted({k for _, _, f, _ in self.updates for k in f})


class TestDailyJob(_Base):
    def test_writes_deadline_and_remaining_without_alert(self):
        self.seed(_rec(rid="2", start=_start_for_remaining(30), revision="7"))
        res = self.run_job()
        self.assertEqual(self.val("2", "熟慮期間期限"), date.fromordinal(TODAY.toordinal() + 30).isoformat())
        self.assertEqual(self.val("2", "残日数"), "30")
        self.assertEqual(len(self.updates), 1)
        self.assertEqual(self.updates[0][3], "7")                       # $revision CAS
        self.assertEqual(set(self.updates[0][2]), {"熟慮期間期限", "残日数"})
        self.assertEqual((res["written"], res["candidates"]), (1, 0))
        self.assertEqual(self.sent_texts(), [])
        self.assertEqual(self.ops(), [])

    def test_alert_14_sent_once_recorded_in_ledger_and_marked(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        res = self.run_job()
        self.assertEqual(len(self.alert_texts()), 1)
        self.assertIn("・No.1 期限 2026-09-22 残 14 日（14日前の通知）", self.alert_texts()[0])
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])
        self.assertEqual(self.val("1", "残日数"), "14")
        self.assertEqual((res["candidates"], res["sent"], res["written"]), (1, 1, 1))
        self.assertEqual(len([u for u in self.updates if u[1] == "1"]), 1)   # 1 レコード 1 回の書込
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        o = ops[0]
        self.assertEqual((o["business"], o["channel"], o["purpose"], o["actor"], o["state"]),
                         ("souzoku-houki", "houki_jukuryo", "other", "bot", "sent"))
        self.assertEqual(o["inbound_event_id"], "houki_jukuryo_14:1:2026-09-22")
        self.assertIsNone(o["conversation_id"])

    def test_same_day_three_runs_send_once_and_write_once(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.run_job(8)
        r2 = self.run_job(13)
        self.run_job(18)
        self.assertEqual(len(self.alert_texts()), 1)
        self.assertEqual(len(self.updates), 1)
        self.assertEqual(r2["duplicate"], 1)
        self.assertEqual(len(self.ops()), 1)

    def test_alert_7_after_14_marked_and_recorded(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(7), notified=("14日前",),
                       deadline="2026-09-15", remaining="8"))
        self.run_job()                                     # 14 は台帳に無い → 1 回送られる（写しは判定に使わない）
        texts = self.alert_texts()
        self.assertEqual(len(texts), 2)
        self.assertTrue(any("（14日前の通知）" in t for t in texts))
        self.assertTrue(any("（7日前の通知）" in t for t in texts))
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前", "7日前"])
        self.run_job(13)
        self.assertEqual(len(self.alert_texts()), 2)

    def test_missed_14_is_sent_together_with_7_as_two_operations(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(7)))
        res = self.run_job()
        self.assertEqual(len(self.alert_texts()), 2)
        self.assertEqual(res["candidates"], 2)
        self.assertEqual(sorted(o["inbound_event_id"] for o in self.ops()),
                         ["houki_jukuryo_14:1:2026-09-15", "houki_jukuryo_7:1:2026-09-15"])
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前", "7日前"])
        self.assertEqual(len(self.updates), 1)             # 2 操作でも kintone 書込は 1 回

    def test_remaining_15_no_alert_only_fields(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(15)))
        res = self.run_job()
        self.assertEqual(self.alert_texts(), [])
        self.assertEqual(self.val("1", "残日数"), "15")
        self.assertEqual(self.val("1", "通知済み閾値"), [])
        self.assertEqual(res["candidates"], 0)

    def test_overdue_without_ledger_sends_once_no_overdue_notice(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(-3)))
        self.run_job()
        self.run_job(13)
        self.assertEqual(len(self.alert_texts()), 2)       # 14 と 7 の各 1 回
        self.assertTrue(all("残 -3 日" in t for t in self.alert_texts()))
        self.assertFalse(any("超過" in t for t in self.alert_texts()))
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前", "7日前"])

    def test_start_change_recalculates_and_overwrites(self):
        self.seed(_rec(rid="1", start="2026-06-15", deadline="2026-09-14", remaining="6"))
        self.run_job()                                     # 残 6 → 14/7 を送る
        self.assertEqual(len(self.alert_texts()), 2)
        self.records["1"]["起算日_確定"] = {"value": "2026-08-01"}       # 弁護士が起算日を直す（残 53）
        self.run_job(13)
        self.assertEqual(self.val("1", "熟慮期間期限"), "2026-10-31")
        self.assertEqual(self.val("1", "残日数"), "53")
        self.assertEqual(len(self.alert_texts()), 2)       # 新期限は閾値外 → 追加通知なし

    def test_start_cleared_clears_system_fields(self):
        self.seed(_rec(rid="1", start="", deadline="2026-09-14", remaining="6"))
        res = self.run_job()
        self.assertEqual(self.val("1", "熟慮期間期限"), "")
        self.assertEqual(self.val("1", "残日数"), "")
        self.assertEqual(res["unset"], 1)

    def test_all_eight_statuses_are_processed(self):
        for i, s in enumerate(ALL_POST, 1):
            self.seed(_rec(rid=str(i), status=s, start=_start_for_remaining(14)))
        res = self.run_job()
        self.assertEqual(res["targets"], 8)
        self.assertEqual((res["candidates"], res["sent"]), (8, 8))
        for i in range(1, 9):
            self.assertEqual(self.val(str(i), "通知済み閾値"), ["14日前"])
        self.assertEqual(len(self.ops()), 8)

    def test_non_target_records_are_skipped_even_if_returned(self):
        self.seed(_rec(rid="1", status="問い合わせ", start=_start_for_remaining(14)),
                  _rec(rid="2", status="受任", submitted="2026-09-01", start=_start_for_remaining(14)),
                  _rec(rid="3", status="辞任", start=_start_for_remaining(14)))
        res = self.run_job()
        self.assertEqual(self.sent_texts(), [])
        self.assertEqual(self.updates, [])
        self.assertEqual(res["candidates"], 0)

    def test_push_failed_is_failed_in_ledger_no_mark_and_retried_next_run(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.push.return_value = False
        res = self.run_job()
        self.assertEqual(res["failed"], 1)
        self.assertEqual(self.val("1", "通知済み閾値"), [])
        self.assertEqual(self.val("1", "残日数"), "14")                  # 期限欄は書く
        self.assertEqual(self.ops()[0]["state"], "failed")
        self.push.return_value = True
        self.run_job(13)
        self.assertEqual(len(self.alert_texts()), 2)
        o = self.ops()
        self.assertEqual(len(o), 1)                                     # 再試行は同じ操作（attempt_no+1）
        self.assertEqual((o[0]["state"], o[0]["attempt_no"]), ("sent", 2))
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])

    def test_push_exception_is_unconfirmed_no_mark_no_auto_resend(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.push.side_effect = RuntimeError("socket")
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            res = self.run_job()
        self.assertEqual(res["unconfirmed"], 1)
        self.assertEqual(self.val("1", "通知済み閾値"), [])
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")
        self.push.side_effect = None
        res2 = self.run_job(13)
        self.assertEqual(res2["unconfirmed"], 1)                        # 人の確定待ち・自動再送なし
        self.assertEqual(self.push.await_count, 1)
        listed = _run(sl.list_unconfirmed())
        self.assertEqual([(x["purpose"], x["channel"], x["business"]) for x in listed],
                         [("other", "houki_jukuryo", "souzoku-houki")])

    def test_mark_written_only_after_send(self):
        order = []
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        real_update = hub_kintone.update_record

        async def spy_update(app, rid, fields, revision=None):
            order.append(("update", sorted(fields)))
            return await real_update(app, rid, fields, revision)

        async def spy_push(admin, text):
            order.append(("push",))
            return True
        with patch.object(hub_kintone, "update_record", spy_update), \
                patch.object(hj, "_push_admin_http", spy_push):
            self.run_job()
        self.assertEqual(order, [("push",), ("update", ["残日数", "熟慮期間期限", "通知済み閾値"])])

    def test_cas_conflict_refetches_once_and_retries_with_new_values(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14), revision="5"))
        self.conflict_next["1"] = 1

        def mutate(rec):                                   # 409 の裏で弁護士が起算日を変えていた（対象のまま）
            rec["$revision"] = {"value": "6"}
            rec["起算日_確定"] = {"value": _start_for_remaining(40)}
        self.refetch_mutator = mutate
        res = self.run_job()
        self.assertEqual(res["written"], 1)
        self.assertEqual([u[3] for u in self.updates], ["5", "6"])
        self.assertEqual(self.val("1", "残日数"), "40")                  # 再計算した値で保存
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])      # 送った刻印は保持

    def test_cas_conflict_twice_warns_and_does_not_raise(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(30)))
        self.conflict_next["1"] = 2
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING) as cm:
            res = self.run_job()
        self.assertEqual(res["write_failed"], 1)
        self.assertIn("CAS conflict twice", "\n".join(cm.output))
        self.assertEqual(len(self.updates), 2)

    def test_write_error_warns_and_does_not_raise(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(30)))
        self.fail_on.add("1")
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING) as cm:
            res = self.run_job()
        self.assertEqual(res["write_failed"], 1)
        self.assertIn("write failed", "\n".join(cm.output))
        self.assertNotIn(NAME, "\n".join(cm.output))

    def test_fetch_error_sends_and_writes_nothing(self):
        async def boom(app, query, fields=None):
            raise hub_kintone.KintoneError(500, "GAIA_XX", "down")
        with patch.object(hub_kintone, "search_records", boom), \
                self.assertLogs("hub.houki_jukuryo", level=logging.ERROR):
            res = self.run_job()
        self.assertEqual(res["targets"], 0)
        self.assertEqual(self.sent_texts(), [])
        self.assertEqual(self.updates, [])

    def test_candidates_all_become_operations_no_loss(self):
        """旧「分割で件数が落ちない」の継承: 候補 = 操作 = push（60 件）。"""
        for i in range(1, 61):
            self.seed(_rec(rid=str(i), start=_start_for_remaining(14)))
        res = self.run_job()
        self.assertEqual((res["candidates"], res["sent"]), (60, 60))
        self.assertEqual(len(self.ops()), 60)
        self.assertEqual(len(self.alert_texts()), 60)
        for i in range(1, 61):
            self.assertEqual(self.val(str(i), "通知済み閾値"), ["14日前"])

    def test_one_failed_push_leaves_only_its_record_unmarked(self):
        for i in range(1, 6):
            self.seed(_rec(rid=str(i), start=_start_for_remaining(14)))
        self.push.side_effect = [True, True, False, True, True]
        res = self.run_job()
        self.assertEqual((res["sent"], res["failed"]), (4, 1))
        self.assertEqual(self.val("3", "通知済み閾値"), [])
        self.assertEqual(self.val("3", "残日数"), "14")
        for i in (1, 2, 4, 5):
            self.assertEqual(self.val(str(i), "通知済み閾値"), ["14日前"])


# ── BH-01: 409 再取得後に対象外なら更新中止 ───────────────────────────────────
class TestBH01AbortAfterRefetch(_Base):
    def _abort_case(self, mutate):
        self.seed(_rec(rid="1", start=_start_for_remaining(30), revision="5"))
        self.conflict_next["1"] = 1
        self.refetch_mutator = mutate
        with self.assertLogs("hub.houki_jukuryo", level=logging.INFO) as cm:
            res = self.run_job()
        self.assertEqual(res["aborted"], 1)
        self.assertEqual(res["written"], 0)
        self.assertEqual(len(self.updates), 1)             # 2 回目の PUT は出ない
        self.assertEqual(self.gets, ["1"])
        self.assertIn("write aborted after refetch", "\n".join(cm.output))
        return res

    def test_submitted_during_race(self):
        def m(rec):
            rec["$revision"] = {"value": "6"}
            rec["申述提出日"] = {"value": "2026-09-08"}
        self._abort_case(m)
        self.assertEqual(self.val("1", "残日数"), "")       # 書かれていない

    def test_status_closed_during_race(self):
        def m(rec):
            rec["$revision"] = {"value": "6"}
            rec["status"] = {"value": "辞任"}
        self._abort_case(m)

    def test_start_cleared_during_race(self):
        def m(rec):
            rec["$revision"] = {"value": "6"}
            rec["起算日_確定"] = {"value": ""}
        self._abort_case(m)

    def test_still_target_after_refetch_writes(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(30), revision="5"))
        self.conflict_next["1"] = 1

        def m(rec):
            rec["$revision"] = {"value": "6"}
        self.refetch_mutator = m
        res = self.run_job()
        self.assertEqual((res["written"], res["aborted"]), (1, 0))


# ── BH-02: 1 回性の正は台帳・刻印は写し ─────────────────────────────────────────
class TestBH02LedgerIsTruth(_Base):
    def test_mark_failure_does_not_resend_and_mark_is_retried(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.fail_on.add("1")                              # 刻印（kintone 書込）が失敗
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            res = self.run_job()
        self.assertEqual((res["sent"], res["write_failed"]), (1, 1))
        self.assertEqual(self.val("1", "通知済み閾値"), [])
        self.assertEqual(self.ops()[0]["state"], "sent")
        self.fail_on.clear()
        res2 = self.run_job(13)                            # 再通知なし・刻印だけ再試行
        self.assertEqual((res2["sent"], res2["duplicate"], res2["remark"], res2["written"]), (0, 1, 1, 1))
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])

    def test_bh04c_clock_5_hours_later_no_resend(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.fail_on.add("1")
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            self.run_job(8)
        self.fail_on.clear()
        base = time.monotonic()
        with patch.object(hub_notify.time, "monotonic", lambda: base + 5 * 3600), \
                patch.object(sl, "_now", lambda: sl.datetime.datetime.now(sl.datetime.timezone.utc)
                             + sl.datetime.timedelta(hours=5)):
            res = self.run_job(13)
        self.assertEqual((res["sent"], res["duplicate"], res["remark"]), (0, 1, 1))
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])

    def test_unchecking_the_copy_does_not_resend(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.run_job()
        self.records["1"]["通知済み閾値"] = {"value": []}            # 人が写しを外しても台帳が正
        res = self.run_job(13)
        self.assertEqual((res["sent"], res["duplicate"], res["remark"]), (0, 1, 1))
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])    # 写しを復元

    def test_ledger_begin_uses_purpose_other_and_channel_houki_jukuryo(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.run_job()
        o = self.ops()[0]
        self.assertEqual(o["purpose"], "other")                          # DB CHECK 制約の範囲内
        self.assertIn(o["purpose"], sl.PURPOSES)
        self.assertEqual(o["channel"], hj.LEDGER_CHANNEL)
        self.assertTrue(o["inbound_event_id"].startswith("houki_jukuryo_14:"))
        hist = _run(sl.operation_history(o["op_id"]))
        self.assertEqual([h["reason"] for h in hist], ["created", "started", "sent"])

    def test_guarded_http_timeout_is_unconfirmed(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))

        async def slow(admin, text):
            await asyncio.sleep(0.5)
            return True
        with patch.object(hj, "_push_admin_http", slow), \
                patch.dict(os.environ, {sl.SEND_TIMEOUT_SECONDS_ENV: "0.05", sl.HEARTBEAT_SECONDS_ENV: "0.01",
                                        sl.DEADLINE_MINUTES_ENV: "1", sl.STALE_MINUTES_ENV: "2"}), \
                self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            res = self.run_job()
        self.assertEqual(res["unconfirmed"], 1)
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")
        self.assertEqual(self.val("1", "通知済み閾値"), [])


# ── BH-03: 起算日未確定の 1 日 1 回も台帳 ───────────────────────────────────────
class TestBH03UnsetCount(_Base):
    def test_count_only_once_per_day_recorded_in_ledger(self):
        self.seed(_rec(rid="1", start=""), _rec(rid="2", start=""), _rec(rid="3", start=_start_for_remaining(30)))
        res = self.run_job(8)
        self.assertEqual(res["unset_result"], hj.SEND_SENT)
        unset = [t for t in self.sent_texts() if hj.UNSET_LABEL in t]
        self.assertEqual(len(unset), 1)
        self.assertIn("起算日未確定 2 件", unset[0])
        self.assertNotIn("No.", unset[0])
        self.assertNotIn(NAME, unset[0])
        o = [x for x in self.ops() if x["inbound_event_id"].startswith("houki_jukuryo_unset:")]
        self.assertEqual(len(o), 1)
        self.assertEqual(o[0]["inbound_event_id"], "houki_jukuryo_unset:2026-09-08")
        r13 = self.run_job(13)
        self.run_job(18)
        self.assertEqual(r13["unset_result"], hj.SEND_DUPLICATE_SENT)
        self.assertEqual(len([t for t in self.sent_texts() if hj.UNSET_LABEL in t]), 1)

    def test_restart_same_day_does_not_resend(self):
        self.seed(_rec(rid="1", start=""))
        self.run_job(8)
        hub_notify._last_notify_at.clear()                 # 再起動相当（プロセス内状態の初期化・台帳だけが残る）
        res = self.run_job(13)
        self.assertEqual(res["unset_result"], hj.SEND_DUPLICATE_SENT)
        self.assertEqual(len([t for t in self.sent_texts() if hj.UNSET_LABEL in t]), 1)

    def test_new_day_notifies_again(self):
        self.seed(_rec(rid="1", start=""))
        self.run_job()
        self.today = date(2026, 9, 9)
        self.run_job()
        self.assertEqual(len([t for t in self.sent_texts() if hj.UNSET_LABEL in t]), 2)

    def test_zero_unset_no_notice(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(30)))
        res = self.run_job()
        self.assertEqual(res["unset_result"], "")
        self.assertEqual(self.sent_texts(), [])

    def test_failed_unset_notice_is_retried_at_next_run(self):
        self.seed(_rec(rid="1", start=""))
        self.push.return_value = False
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            res = self.run_job(8)
        self.assertEqual(res["unset_result"], hj.SEND_FAILED)
        self.push.return_value = True
        res = self.run_job(13)
        self.assertEqual(res["unset_result"], hj.SEND_SENT)

    def test_unset_records_write_nothing_when_fields_empty(self):
        self.seed(_rec(rid="1", start=""))
        self.run_job()
        self.assertEqual(self.updates, [])


# ── BH-04: 期限変更と通知キーの連動 ──────────────────────────────────────────────
class TestBH04DeadlineChangeRearms(_Base):
    def test_a_same_record_same_threshold_new_deadline_is_a_new_operation(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.run_job()
        self.records["1"]["起算日_確定"] = {"value": _start_for_remaining(10)}   # 期限訂正（まだ閾値内）
        res = self.run_job(13)
        self.assertEqual(res["sent"], 1)                                      # 訂正後に 1 回
        keys = sorted(o["inbound_event_id"] for o in self.ops())
        self.assertEqual(keys, [f"houki_jukuryo_14:1:{date.fromordinal(TODAY.toordinal() + 10).isoformat()}",
                                "houki_jukuryo_14:1:2026-09-22"])
        self.assertEqual(self.val("1", "残日数"), "10")
        self.run_job(18)
        self.assertEqual(self.push.await_count, 2)                             # 以後は増えない

    def test_b_correction_after_mark_failure_is_not_throttled_within_300s(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.fail_on.add("1")
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            self.run_job(8)
        self.fail_on.clear()
        self.records["1"]["起算日_確定"] = {"value": _start_for_remaining(12)}
        base = time.monotonic()
        with patch.object(hub_notify.time, "monotonic", lambda: base + 10):   # 旧通知から 10 秒
            res = self.run_job(13)
        self.assertEqual(res["sent"], 1)
        self.assertEqual(self.push.await_count, 2)
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])


# ── fix2 BH-05: 台帳が使えない（DB 未設定・記録失敗）ときは送らない（fail-closed） ──────────
# fix1 の「台帳なしでも写しで抑止して送る」は司令塔裁定（熟慮期間通知ジョブは台帳必須）により
# 「台帳なしなら送らない・固定理由の件数ログのみ・期限欄の保存は行う」へ仕様変更（緩和ではない）
class TestBH05LedgerUnavailableFailClosed(_Base):
    def setUp(self):
        super().setUp()
        # 親の env patch（DATABASE_URL あり）を外し、DATABASE_URL 無しの patch に差し替える
        # （patch.dict の入れ子で DATABASE_URL が復元され他テストへ漏れないように）
        self._env.stop()
        self._env = patch.dict(os.environ, {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz-token",
                                            "LINE_ADMIN_USER_ID": ADMIN})
        self._env.start()
        os.environ.pop("DATABASE_URL", None)
        db.reset_for_tests()

    def test_no_database_url_sends_nothing_counts_only_and_still_writes_fields(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)),
                  _rec(rid="2", start=_start_for_remaining(14), notified=("14日前",)),
                  _rec(rid="3", start=""))
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING) as cm:
            res = self.run_job()
        self.assertEqual(self.push.await_count, 0)
        self.assertEqual((res["sent"], res["duplicate"], res["ledger_unavailable"]), (0, 0, 2))
        self.assertEqual(res["unset_result"], hj.SEND_LEDGER_UNAVAILABLE)
        self.assertEqual(self.val("1", "通知済み閾値"), [])                 # 刻印しない
        self.assertEqual(self.val("1", "残日数"), "14")                     # 期限欄は書く
        logs = "\n".join(cm.output)
        self.assertEqual(logs.count("ledger_unavailable"), 3)              # 14 日前×2 + 未確定件数
        self.assertNotIn(NAME, logs)

    def test_ledger_record_failure_also_sends_nothing(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))

        async def broken_begin(*a, **k):
            return None                                   # begin の fail-open（記録失敗）と同じ戻り
        with patch.object(sl, "begin", broken_begin), \
                self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            res = self.run_job()
        self.assertEqual(self.push.await_count, 0)
        self.assertEqual(res["ledger_unavailable"], 1)

    def test_recovery_sends_once_without_duplicate(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            self.run_job(8)                               # 台帳なし → 送らない
        # 復旧（台帳が使える状態へ）
        self._env.stop()
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/l.db",
                                            "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz-token",
                                            "LINE_ADMIN_USER_ID": ADMIN})
        self._env.start()
        db.reset_for_tests()
        res = self.run_job(13)
        self.assertEqual((res["sent"], self.push.await_count), (1, 1))
        self.run_job(18)
        self.assertEqual(self.push.await_count, 1)        # 復旧後の再送は 1 回だけ


# ── fix2 BH-06: finish(sent) が applied でなければ写しを作らない ─────────────────────
class TestBH06FinishMustBeApplied(_Base):
    def test_finish_db_failure_leaves_started_counts_unconfirmed_no_mark(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        real_finish = sl.finish

        async def finish_db_down(op, outcome):
            # finish 内の DB 障害を注入: 実装どおり "skipped" を返し、台帳は started のまま
            if outcome == sl.STATE_SENT:
                return "skipped"
            return await real_finish(op, outcome)
        with patch.object(sl, "finish", finish_db_down), \
                self.assertLogs("hub.houki_jukuryo", level=logging.WARNING) as cm:
            res = self.run_job()
        self.assertEqual(self.push.await_count, 1)                           # 送信はした
        self.assertEqual((res["sent"], res["unconfirmed"]), (0, 1))          # 集計は unconfirmed
        self.assertEqual(self.val("1", "通知済み閾値"), [])                   # 刻印なし
        self.assertEqual(self.ops()[0]["state"], "started")                   # 台帳は started
        self.assertIn("finish not applied", "\n".join(cm.output))
        res2 = self.run_job(13)                                               # 自動再送なし（未確定の重複）
        self.assertEqual((res2["unconfirmed"], self.push.await_count), (1, 1))

    def test_late_result_is_unconfirmed(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))

        async def late_finish(op, outcome):
            return "late"
        with patch.object(sl, "finish", late_finish), \
                self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            res = self.run_job()
        self.assertEqual((res["sent"], res["unconfirmed"]), (0, 1))
        self.assertEqual(self.val("1", "通知済み閾値"), [])

    def test_applied_is_sent_and_marked(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        res = self.run_job()
        self.assertEqual((res["sent"], res["unconfirmed"]), (1, 0))
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])


# ── fix2 付随 5: JST 日付境界（23:59 → 0:00） ───────────────────────────────────────
class TestJstDateBoundary(unittest.TestCase):
    def _today_at_utc(self, iso_utc: str) -> date:
        from datetime import datetime as _dt, timezone as _tz

        class _FakeDatetime:
            @staticmethod
            def now(tz=None):
                base = _dt.fromisoformat(iso_utc).replace(tzinfo=_tz.utc)
                return base.astimezone(tz) if tz else base
        with patch.object(hj, "datetime", _FakeDatetime):
            return hj._today_jst()

    def test_2359_jst_and_0000_jst_are_different_days(self):
        self.assertEqual(self._today_at_utc("2026-09-08T14:59:59"), date(2026, 9, 8))   # 23:59:59 JST
        self.assertEqual(self._today_at_utc("2026-09-08T15:00:00"), date(2026, 9, 9))   # 00:00:00 JST
        # UTC の日付（9/8）に引きずられない: 未確定件数の業務キーも JST の日付で切り替わる
        self.assertEqual(hj.ledger_key(hj.LEDGER_KIND_UNSET, self._today_at_utc("2026-09-08T15:00:00").isoformat()),
                         "houki_jukuryo_unset:2026-09-09")


# ── fix2 BH-07: /app/send_ops の表示（閉集合解析・生の業務キーは出さない） ───────────
class TestBH07SendOpsView(_Base):
    def test_list_unconfirmed_includes_inbound_event_id_and_channel(self):
        self.seed(_rec(rid="12", start=_start_for_remaining(14)))
        self.push.side_effect = RuntimeError("socket")
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            self.run_job()
        items = _run(sl.list_unconfirmed())
        self.assertEqual(len(items), 1)
        self.assertEqual((items[0]["channel"], items[0]["inbound_event_id"]),
                         ("houki_jukuryo", "houki_jukuryo_14:12:2026-09-22"))

    def test_jukuryo_label_closed_set(self):
        from hub import webapp_send_ops_view as sov
        self.assertEqual(sov.jukuryo_label({"channel": "houki_jukuryo",
                                            "inbound_event_id": "houki_jukuryo_14:12:2026-09-22"}),
                         "案件番号 12・期限 2026-09-22・14日前")
        self.assertEqual(sov.jukuryo_label({"channel": "houki_jukuryo",
                                            "inbound_event_id": "houki_jukuryo_7:3:2026-09-15"}),
                         "案件番号 3・期限 2026-09-15・7日前")
        self.assertEqual(sov.jukuryo_label({"channel": "houki_jukuryo",
                                            "inbound_event_id": "houki_jukuryo_unset:2026-09-08"}),
                         "起算日未確定の件数通知・対象日 2026-09-08")
        for bad in ("houki_jukuryo_30:1:2026-09-22", "houki_jukuryo_14:abc:2026-09-22",
                    "houki_jukuryo_14:1:2026/09/22", "x", "", None):
            self.assertEqual(sov.jukuryo_label({"channel": "houki_jukuryo", "inbound_event_id": bad}), "")
        # 他チャネルは解析しない（時効側の webhookEventId は表示しない）
        self.assertEqual(sov.jukuryo_label({"channel": "jikou", "inbound_event_id": "houki_jukuryo_14:1:2026-09-22"}), "")

    def test_page_shows_case_deadline_threshold_without_raw_key(self):
        from hub import webapp_send_ops_view as sov
        self.seed(_rec(rid="12", start=_start_for_remaining(14)))
        self.push.side_effect = RuntimeError("socket")
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            self.run_job()
        rows = _run(sl.list_unconfirmed())
        page = sov._page(rows, "")
        self.assertIn("案件番号 12・期限 2026-09-22・14日前", page)
        self.assertIn("相続放棄 熟慮期間（業務 LINE）", page)
        self.assertNotIn("houki_jukuryo_14:12:2026-09-22", page)              # 生の業務キーは出さない
        self.assertNotIn(ADMIN, page)
        self.assertNotIn(NAME, page)
        api_items = [sov.public_row(r) for r in rows]
        self.assertNotIn("inbound_event_id", api_items[0])
        self.assertEqual(api_items[0]["jukuryo"], "案件番号 12・期限 2026-09-22・14日前")

    def test_other_channel_rows_render_as_before(self):
        from hub import webapp_send_ops_view as sov
        row = {"op_id": "op1", "business": "jikou", "channel": "jikou", "purpose": "reply", "actor": "bot",
               "state": "unconfirmed", "stale": False, "started_at": "2026-09-08T00:00:00+00:00",
               "attempt_no": 1, "conversation_ref": "c-1", "inbound_event_id": "wh-xyz"}
        page = sov._page([row], "")
        self.assertNotIn("wh-xyz", page)
        self.assertNotIn("相続放棄 熟慮期間", page)
        self.assertNotIn('class="case"', page)
        self.assertEqual(sov.public_row(row)["jukuryo"], "")
        self.assertNotIn("inbound_event_id", sov.public_row(row))


class TestPushHttp(unittest.TestCase):
    def test_recipient_not_allowlisted_is_refused_without_http(self):
        calls = []

        class _C:
            def __init__(self, **_k): ...

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_a):
                return False

            async def post(self, *a, **k):
                calls.append(a)
        with patch.dict(os.environ, {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "t"}, clear=False), \
                patch.object(hj, "business_channel_allowlist", lambda: frozenset()), \
                patch.object(hj.httpx, "AsyncClient", _C), \
                self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            self.assertFalse(_run(hj._push_admin_http(ADMIN, "x")))
        self.assertEqual(calls, [])

    def test_post_uses_business_token_and_admin(self):
        seen = {}

        class _R:
            is_success = True
            status_code = 200
            text = "{}"

        class _C:
            def __init__(self, **_k): ...

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_a):
                return False

            async def post(self, url, headers=None, json=None):
                seen.update(url=url, headers=headers, json=json)
                return _R()
        with patch.dict(os.environ, {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz"}, clear=False), \
                patch.object(hj, "business_channel_allowlist", lambda: frozenset({ADMIN})), \
                patch.object(hj.httpx, "AsyncClient", _C):
            self.assertTrue(_run(hj._push_admin_http(ADMIN, "hello")))
        self.assertEqual(seen["headers"]["Authorization"], "Bearer biz")
        self.assertEqual(seen["json"]["to"], ADMIN)
        self.assertEqual(seen["json"]["messages"][0]["text"], "hello")


class TestPiiAndClosedSet(_Base):
    def test_no_pii_in_texts_or_logs(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(7)), _rec(rid="2", start=""))
        with self.assertLogs("hub.houki_jukuryo", level=logging.INFO) as cm:
            self.run_job()
        blob = "\n".join(self.sent_texts() + cm.output)
        for leak in (NAME, KANA, "2026-05-01", "2026-04-01", ADMIN):
            self.assertNotIn(leak, blob)
        for o in self.ops():
            self.assertNotIn(NAME, str(o))
            self.assertNotIn(ADMIN, str(o))               # 宛先は hash（RV-10）

    def test_only_three_fields_are_ever_written(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(7)),
                  _rec(rid="2", start="", deadline="2026-01-01", remaining="3"))
        self.run_job()
        self.assertTrue(set(self.written_fields()) <= hj.WRITE_FIELDS, self.written_fields())
        for _app, _rid, fields, _rev in self.updates:
            for banned in ("status", "起算日_確定", "法定満了日", "社内締切日", "熟慮期間通知履歴", "申述提出日"):
                self.assertNotIn(banned, fields)
        self.assertEqual({u[0] for u in self.updates}, {"APP_HOUKI"})


class TestPaging(_Base):
    def _seed_n(self, n, remaining=30):
        for i in range(1, n + 1):
            self.seed(_rec(rid=str(i), start=_start_for_remaining(remaining),
                           deadline=date.fromordinal(TODAY.toordinal() + remaining).isoformat(),
                           remaining=str(remaining)))

    def test_zero_records_one_query(self):
        res = self.run_job()
        self.assertEqual((len(self.queries), res["targets"]), (1, 0))

    def test_499_records_one_query(self):
        self._seed_n(499)
        res = self.run_job()
        self.assertEqual((len(self.queries), res["targets"]), (1, 499))

    def test_exactly_500_fetches_next_page(self):
        self._seed_n(500)
        res = self.run_job()
        self.assertEqual((len(self.queries), res["targets"]), (2, 500))
        self.assertIn("$id > 500", self.queries[1])

    def test_501st_record_due_is_notified(self):
        self._seed_n(500)
        self.seed(_rec(rid="501", start=_start_for_remaining(14)))
        res = self.run_job()
        self.assertEqual(res["targets"], 501)
        self.assertEqual(self.val("501", "通知済み閾値"), ["14日前"])

    def test_over_1000_records_three_pages(self):
        self._seed_n(1001)
        res = self.run_job()
        self.assertEqual((len(self.queries), res["targets"]), (3, 1001))

    def test_fetch_all_targets_stops_when_id_does_not_advance(self):
        async def stuck(app, query, fields=None):
            return [_rec(rid="7", start=_start_for_remaining(30)) for _ in range(hj.SEARCH_LIMIT)]
        with patch.object(hub_kintone, "search_records", stuck), \
                self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            rows = _run(hj.fetch_all_targets())
        self.assertEqual(len(rows), 1000)


class TestKindAndConstants(unittest.TestCase):
    def test_constants_pinned(self):
        # HOUKI-JUKURYO-2（大野裁定 2026-10-08）/ fix1
        self.assertEqual(hj.JUKURYO_MONTHS, 3)
        self.assertEqual(hj.ALERT_DAYS, (14, 7))
        self.assertEqual(hj.NOTIFIED_VALUES, {14: "14日前", 7: "7日前"})
        self.assertEqual(hj.RUN_HOURS_JST, (8, 13, 18))
        self.assertEqual(hj.JOB_NAME, "HOUKI_JUKURYO")
        self.assertEqual((hj.FIELD_STATUS, hj.FIELD_SUBMITTED, hj.FIELD_START),
                         ("status", "申述提出日", "起算日_確定"))
        self.assertEqual((hj.FIELD_DEADLINE, hj.FIELD_REMAINING, hj.FIELD_NOTIFIED),
                         ("熟慮期間期限", "残日数", "通知済み閾値"))
        self.assertEqual((hj.LEDGER_BUSINESS, hj.LEDGER_CHANNEL, hj.LEDGER_PURPOSE),
                         ("souzoku-houki", "houki_jukuryo", "other"))
        self.assertEqual((hj.LEDGER_KIND_14, hj.LEDGER_KIND_7, hj.LEDGER_KIND_UNSET),
                         ("houki_jukuryo_14", "houki_jukuryo_7", "houki_jukuryo_unset"))
        self.assertEqual(hj.LEDGER_KINDS, {14: "houki_jukuryo_14", 7: "houki_jukuryo_7"})
        self.assertEqual(hj.SEARCH_LIMIT, 500)
        self.assertEqual(sorted(hj.SEARCH_FIELDS),
                         sorted(["$id", "$revision", "status", "申述提出日", "起算日_確定",
                                 "熟慮期間期限", "残日数", "通知済み閾値"]))
        for absent in ("法定満了日", "社内締切日", "熟慮期間通知履歴", "相続人と知った日_申告",
                       "死亡を知った日_申告", "死亡日_申告", "起算点確定済", "伸長後満了日"):
            self.assertNotIn(absent, hj.SEARCH_FIELDS)
        self.assertFalse(hasattr(hj, "INTERNAL_MARGIN_DAYS"))             # 2 本立ては廃止
        self.assertFalse(hasattr(hj, "START_DATE_FIELDS"))                 # 申告代用は廃止
        self.assertFalse(hasattr(hj, "NOTIFY_KIND"))                       # notify throttle 不使用
        self.assertFalse(hasattr(hj, "SEND_SENT_UNRECORDED"))              # fix2 BH-05: fail-open 経路の廃止
        self.assertFalse(hasattr(hj, "_unset_notified_on"))
        self.assertEqual(hj.SEND_LEDGER_UNAVAILABLE, "ledger_unavailable")

    def test_send_ledger_closed_sets_untouched(self):
        self.assertEqual(sl.PURPOSES, ("reply", "first_reply", "urgent", "image_receipt",
                                       "image_result", "follow", "receipt_number", "other"))
        self.assertIn(hj.LEDGER_PURPOSE, sl.PURPOSES)

    def test_module_reads_no_new_env_and_no_extension(self):
        src = Path(hj.__file__).read_text(encoding="utf-8")
        body = src.split('"""', 2)[2]
        self.assertEqual(body.count("os.environ"), 1)                       # 既存 env（業務トークン）のみ
        self.assertIn('os.environ.get(business_token_env(), "")', body)
        self.assertNotIn("伸長後満了日", body)                              # 本文コードに伸長の分岐なし
        self.assertNotIn("notify_admin_line_result", body)
        self.assertNotIn("alembic", body)

    def test_main_registers_job(self):
        src = (Path(__file__).parent / "main.py").read_text(encoding="utf-8")
        self.assertIn("register_houki_jukuryo_job()", src)

    def test_notify_truncation_untouched(self):
        src = Path(hub_notify.__file__).read_text(encoding="utf-8")
        self.assertIn("text[:4900]", src)

    def test_config_schema_lists_system_fields(self):
        from config import EXPECTED_KINTONE_SCHEMA
        f = EXPECTED_KINTONE_SCHEMA["App 40 (相続放棄案件)"]["fields"]
        self.assertEqual(f["熟慮期間期限"]["type"], "DATE")
        self.assertEqual(f["残日数"]["type"], "NUMBER")
        self.assertEqual(f["起算日_確定"]["type"], "DATE")
        self.assertEqual(f["通知済み閾値"]["required_options"], ["14日前", "7日前"])


class TestJobRegistration(unittest.TestCase):
    def setUp(self):
        hub_scheduler.stop_all()
        self.addCleanup(hub_scheduler.stop_all)

    def test_registers_three_daily_jobs(self):
        async def run():
            hj.register_houki_jukuryo_job()
            for hour in (8, 13, 18):
                name = hj.job_name(hour)
                self.assertTrue(hub_scheduler.is_registered(name), name)
                job = hub_scheduler._jobs[name]
                self.assertEqual((job.kind, job.hour_jst), ("daily", hour))
                self.assertIs(job.coro_factory.func, hj.jukuryo_daily_check)
                self.assertEqual(job.coro_factory.args, (hour,))
            self.assertEqual(hj.job_name(8), "HOUKI_JUKURYO_08")
        asyncio.run(run())

    def test_double_registration_is_safe(self):
        async def run():
            hj.register_houki_jukuryo_job()
            t1 = hub_scheduler._jobs[hj.job_name(8)].task
            hj.register_houki_jukuryo_job()
            self.assertIs(t1, hub_scheduler._jobs[hj.job_name(8)].task)
            self.assertEqual(len([n for n in hub_scheduler._jobs if n.startswith("HOUKI_JUKURYO")]), 3)
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
