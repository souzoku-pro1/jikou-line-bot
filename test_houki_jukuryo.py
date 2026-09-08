"""HOUKI-JUKURYO-CRON-1: 相続放棄 熟慮期間 日次監視のテスト。

- 期日計算（応当日・月末・閏年・起算日代用順・全空）
- 社内締切 10 日前・マイルストーン判定（7 日前／当日／3 日前／毎日／超過・境界）
- 対象抽出（受任以外・提出日ありは除外）・弁護士設定優先
- 履歴による冪等（同日再実行 0 通）・履歴 CAS 失敗で送らない
- 本文に個人情報なし・kind 登録・定数 pin・ジョブ登録（8:00 JST）・main 結線
"""

import asyncio
import logging
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

from hub import houki_jukuryo as hj
from hub import kintone as hub_kintone
from hub import notify as hub_notify
from hub import scheduler as hub_scheduler

TODAY = date(2026, 9, 8)


def _rec(rid="1", status="受任", submitted="", history="", legal="", internal="",
         knew="", knew_death="", death="", name="山田太郎", revision="3"):
    return {
        "$id": {"value": rid}, "$revision": {"value": revision},
        "status": {"value": status}, "申述提出日": {"value": submitted},
        "熟慮期間通知履歴": {"value": history},
        "法定満了日": {"value": legal}, "社内締切日": {"value": internal},
        "相続人と知った日_申告": {"value": knew}, "死亡を知った日_申告": {"value": knew_death},
        "死亡日_申告": {"value": death}, "氏名": {"value": name},
    }


# ── 期日計算 ────────────────────────────────────────────────────────────────
class TestAddMonths(unittest.TestCase):
    def test_same_day_of_month(self):
        self.assertEqual(hj.add_months(date(2026, 6, 15), 3), date(2026, 9, 15))

    def test_month_end_clamp_non_leap(self):
        self.assertEqual(hj.add_months(date(2026, 11, 30), 3), date(2027, 2, 28))

    def test_month_end_clamp_leap_year(self):
        self.assertEqual(hj.add_months(date(2027, 11, 30), 3), date(2028, 2, 29))

    def test_31st_to_30_day_month(self):
        self.assertEqual(hj.add_months(date(2026, 8, 31), 3), date(2026, 11, 30))

    def test_year_rollover(self):
        self.assertEqual(hj.add_months(date(2026, 12, 20), 3), date(2027, 3, 20))


class TestResolveStart(unittest.TestCase):
    def test_priority_knew_heir_first(self):
        r = _rec(knew="2026-06-15", knew_death="2026-06-01", death="2026-05-01")
        self.assertEqual(hj.resolve_start(r), (date(2026, 6, 15), "相続人と知った日_申告"))

    def test_fallback_to_knew_death(self):
        r = _rec(knew_death="2026-06-01", death="2026-05-01")
        self.assertEqual(hj.resolve_start(r), (date(2026, 6, 1), "死亡を知った日_申告"))

    def test_fallback_to_death(self):
        r = _rec(death="2026-05-01")
        self.assertEqual(hj.resolve_start(r), (date(2026, 5, 1), "死亡日_申告"))

    def test_all_empty_is_unset(self):
        self.assertEqual(hj.resolve_start(_rec()), (None, hj.START_UNSET))

    def test_invalid_date_is_skipped(self):
        r = _rec(knew="not-a-date", death="2026-05-01")
        self.assertEqual(hj.resolve_start(r), (date(2026, 5, 1), "死亡日_申告"))


class TestComputeDeadlines(unittest.TestCase):
    def test_computed_legal_and_internal(self):
        dl = hj.compute_deadlines(_rec(knew="2026-06-15"))
        self.assertEqual((dl.legal, dl.legal_source), (date(2026, 9, 15), hj.SOURCE_COMPUTED))
        self.assertEqual((dl.internal, dl.internal_source), (date(2026, 9, 5), hj.SOURCE_COMPUTED))
        self.assertTrue(dl.determinable)

    def test_attorney_legal_overrides_computed(self):
        dl = hj.compute_deadlines(_rec(knew="2026-06-15", legal="2026-10-01"))
        self.assertEqual((dl.legal, dl.legal_source), (date(2026, 10, 1), hj.SOURCE_ATTORNEY))
        self.assertEqual((dl.internal, dl.internal_source), (date(2026, 9, 21), hj.SOURCE_COMPUTED))

    def test_attorney_internal_overrides_computed(self):
        dl = hj.compute_deadlines(_rec(knew="2026-06-15", internal="2026-09-01"))
        self.assertEqual((dl.legal, dl.legal_source), (date(2026, 9, 15), hj.SOURCE_COMPUTED))
        self.assertEqual((dl.internal, dl.internal_source), (date(2026, 9, 1), hj.SOURCE_ATTORNEY))

    def test_unset_when_no_start_and_no_attorney_legal(self):
        dl = hj.compute_deadlines(_rec())
        self.assertFalse(dl.determinable)
        self.assertEqual(dl.legal_source, hj.START_UNSET)

    def test_attorney_legal_without_start_is_determinable(self):
        dl = hj.compute_deadlines(_rec(legal="2026-10-01"))
        self.assertTrue(dl.determinable)
        self.assertEqual(dl.start_source, hj.START_UNSET)
        self.assertEqual(dl.internal, date(2026, 9, 21))


class TestMilestones(unittest.TestCase):
    DL = hj.compute_deadlines(_rec(knew="2026-06-15"))   # 法定 9/15・社内 9/5

    def ms(self, d):
        return hj.milestones_for(d, self.DL)

    def test_internal_pre_7(self):
        self.assertEqual(self.ms(date(2026, 8, 29)), [hj.MS_INTERNAL_PRE])
        self.assertEqual(self.ms(date(2026, 8, 28)), [])
        self.assertEqual(self.ms(date(2026, 8, 30)), [])

    def test_internal_day(self):
        self.assertEqual(self.ms(date(2026, 9, 5)), [hj.MS_INTERNAL_DAY])
        self.assertEqual(self.ms(date(2026, 9, 4)), [])
        self.assertEqual(self.ms(date(2026, 9, 6)), [])

    def test_legal_pre_3(self):
        self.assertEqual(self.ms(date(2026, 9, 12)), [hj.MS_LEGAL_PRE])
        self.assertEqual(self.ms(date(2026, 9, 11)), [])

    def test_legal_daily_2_1_0(self):
        self.assertEqual(self.ms(date(2026, 9, 13)), [hj.MS_LEGAL_2])
        self.assertEqual(self.ms(date(2026, 9, 14)), [hj.MS_LEGAL_1])
        self.assertEqual(self.ms(date(2026, 9, 15)), [hj.MS_LEGAL_DAY])

    def test_overdue_every_day(self):
        self.assertEqual(self.ms(date(2026, 9, 16)), [hj.MS_OVERDUE])
        self.assertEqual(self.ms(date(2026, 12, 1)), [hj.MS_OVERDUE])

    def test_multiple_when_attorney_dates_coincide(self):
        dl = hj.compute_deadlines(_rec(legal="2026-09-15", internal="2026-09-12"))
        self.assertEqual(hj.milestones_for(date(2026, 9, 12), dl),
                         [hj.MS_INTERNAL_DAY, hj.MS_LEGAL_PRE])

    def test_unset_has_no_milestone(self):
        self.assertEqual(hj.milestones_for(date(2026, 9, 15), hj.compute_deadlines(_rec())), [])


class TestTargetAndHistory(unittest.TestCase):
    def test_is_target(self):
        self.assertTrue(hj.is_target(_rec()))
        self.assertFalse(hj.is_target(_rec(status="書類収集中")))
        self.assertFalse(hj.is_target(_rec(status="問い合わせ")))
        self.assertFalse(hj.is_target(_rec(submitted="2026-09-01")))

    def test_search_query_pins_status_and_empty_submitted(self):
        q = hj.search_query()
        self.assertIn('status in ("受任")', q)
        self.assertIn('申述提出日 = ""', q)

    def test_history_line_and_has(self):
        line = hj.history_line(TODAY, hj.MS_LEGAL_DAY)
        self.assertEqual(line, "2026-09-08 法定満了当日 通知済")
        self.assertTrue(hj.history_has("x\n" + line + "\n", TODAY, hj.MS_LEGAL_DAY))
        self.assertFalse(hj.history_has(line, date(2026, 9, 7), hj.MS_LEGAL_DAY))
        self.assertFalse(hj.history_has(line, TODAY, hj.MS_OVERDUE))
        self.assertFalse(hj.history_has("", TODAY, hj.MS_LEGAL_DAY))

    def test_append_history_text(self):
        self.assertEqual(hj.append_history_text("", "a"), "a")
        self.assertEqual(hj.append_history_text("a", "b"), "a\nb")


class TestNoticeBody(unittest.TestCase):
    def test_body_has_no_pii_and_marks_sources(self):
        rec = _rec(rid="7", knew="2026-06-15", name="山田太郎")
        dl = hj.compute_deadlines(rec)
        body = hj.build_notice(TODAY, [hj.format_item("7", hj.MS_INTERNAL_DAY, dl)], ["9"])
        self.assertNotIn("山田", body)
        self.assertIn("No.7 社内締切当日", body)
        self.assertIn("法定満了 2026-09-15（計算値）", body)
        self.assertIn("社内締切 2026-09-05（計算値）", body)
        self.assertIn("相続人と知った日_申告・申告ベース・要確認", body)
        self.assertIn("起算日未設定: No.9", body)

    def test_attorney_source_marked(self):
        dl = hj.compute_deadlines(_rec(knew="2026-06-15", legal="2026-10-01", internal="2026-09-20"))
        line = hj.format_item("3", hj.MS_LEGAL_PRE, dl)
        self.assertIn("法定満了 2026-10-01（弁護士設定）", line)
        self.assertIn("社内締切 2026-09-20（弁護士設定）", line)


# ── 日次ジョブ（kintone・LINE は mock） ──────────────────────────────────────
class _Base(unittest.TestCase):
    def setUp(self):
        self.records: dict[str, dict] = {}
        self.updates: list[tuple] = []
        self.conflict_on: set[str] = set()
        self.fail_on: set[str] = set()

        async def search_records(app, query, fields=None):
            self.query = query
            return [dict(r) for r in self.records.values()]

        async def update_record(app, rid, fields, revision=None):
            self.updates.append((app.app_id_env, rid, dict(fields), revision))
            if rid in self.conflict_on:
                raise hub_kintone.KintoneConflict("409")
            if rid in self.fail_on:
                raise hub_kintone.KintoneError("500")
            rec = self.records[rid]
            self.assertEqual(revision, rec["$revision"]["value"])
            for k, v in fields.items():
                rec[k] = {"value": v}
            rec["$revision"] = {"value": str(int(revision) + 1)}

        self.admin = AsyncMock(return_value=True)
        for p in (patch.object(hub_kintone, "search_records", search_records),
                  patch.object(hub_kintone, "update_record", update_record),
                  patch.object(hub_notify, "notify_admin_line", self.admin),
                  patch.object(hj, "_today_jst", return_value=TODAY)):
            p.start()
            self.addCleanup(p.stop)

    def seed(self, *recs):
        for r in recs:
            self.records[r["$id"]["value"]] = r

    def run_job(self):
        return asyncio.run(hj.jukuryo_daily_check())

    def history(self, rid):
        return self.records[rid]["熟慮期間通知履歴"]["value"]

    def sent_texts(self):
        return [c.args[0] for c in self.admin.await_args_list]

    def written_fields(self):
        return sorted({k for _, _, f, _ in self.updates for k in f})


class TestDailyJob(_Base):
    def test_internal_day_notifies_once_and_appends_history(self):
        self.seed(_rec(rid="1", knew="2026-06-18"))          # 法定 9/18・社内 9/8=TODAY
        out = self.run_job()
        self.assertEqual(out, {"targets": 1, "items": 1, "unset": 0, "sent": True})
        self.assertEqual(self.history("1"), "2026-09-08 社内締切当日 通知済")
        self.assertEqual(self.written_fields(), ["熟慮期間通知履歴"])   # 履歴欄以外は書かない
        self.assertEqual(self.updates[0][0], "APP_HOUKI")                # App 40 のみ
        self.assertEqual(self.admin.await_count, 1)
        self.assertEqual(self.admin.await_args.kwargs["throttle_key"], "houki_jukuryo_daily:2026-09-08")
        self.assertIn("No.1 社内締切当日", self.sent_texts()[0])
        self.assertNotIn("山田", self.sent_texts()[0])

    def test_same_day_rerun_sends_nothing(self):
        self.seed(_rec(rid="1", knew="2026-06-18"))
        self.run_job()
        self.admin.reset_mock()
        n = len(self.updates)
        out = self.run_job()
        self.assertEqual(out["items"], 0)
        self.assertEqual(self.admin.await_count, 0)
        self.assertEqual(len(self.updates), n)                            # 追記もしない
        self.assertEqual(self.history("1"), "2026-09-08 社内締切当日 通知済")

    def test_history_from_previous_day_does_not_block(self):
        self.seed(_rec(rid="1", knew="2026-06-18", history="2026-09-01 社内締切7日前 通知済"))
        out = self.run_job()
        self.assertEqual(out["items"], 1)
        self.assertEqual(self.history("1"),
                         "2026-09-01 社内締切7日前 通知済\n2026-09-08 社内締切当日 通知済")

    def test_cas_conflict_sends_nothing(self):
        self.seed(_rec(rid="1", knew="2026-06-18"))
        self.conflict_on.add("1")
        out = self.run_job()
        self.assertEqual(out, {"targets": 1, "items": 0, "unset": 0, "sent": False})
        self.assertEqual(self.admin.await_count, 0)
        self.assertEqual(self.history("1"), "")

    def test_write_error_sends_nothing(self):
        self.seed(_rec(rid="1", knew="2026-06-18"))
        self.fail_on.add("1")
        out = self.run_job()
        self.assertEqual(out["items"], 0)
        self.assertEqual(self.admin.await_count, 0)

    def test_non_target_records_are_skipped_even_if_returned(self):
        self.seed(_rec(rid="1", knew="2026-06-18", status="書類収集中"),
                  _rec(rid="2", knew="2026-06-18", submitted="2026-09-01"))
        out = self.run_job()
        self.assertEqual(out["items"], 0)
        self.assertEqual(self.admin.await_count, 0)
        self.assertEqual(self.updates, [])

    def test_no_milestone_today_sends_nothing(self):
        self.seed(_rec(rid="1", knew="2026-07-01"))            # 法定 10/1・社内 9/21
        out = self.run_job()
        self.assertEqual(out["items"], 0)
        self.assertEqual(self.admin.await_count, 0)
        self.assertEqual(self.updates, [])

    def test_unset_start_is_listed_once_per_day(self):
        self.seed(_rec(rid="5"))
        out = self.run_job()
        self.assertEqual(out["unset"], 1)
        self.assertIn("起算日未設定: No.5", self.sent_texts()[0])
        self.assertEqual(self.history("5"), "2026-09-08 起算日未設定 通知済")
        self.admin.reset_mock()
        self.assertEqual(self.run_job()["unset"], 0)
        self.assertEqual(self.admin.await_count, 0)

    def test_multiple_records_one_notice(self):
        self.seed(_rec(rid="1", knew="2026-06-18"),            # 社内締切当日
                  _rec(rid="2", knew="2026-06-25"),            # 法定 9/25・社内 9/15 → 7日前
                  _rec(rid="3", knew="2026-05-01"),            # 法定 8/1 → 超過
                  _rec(rid="4"))                               # 未設定
        out = self.run_job()
        self.assertEqual((out["items"], out["unset"]), (3, 1))
        self.assertEqual(self.admin.await_count, 1)
        body = self.sent_texts()[0]
        for s in ("No.1 社内締切当日", "No.2 社内締切7日前", "No.3 満了超過", "起算日未設定: No.4"):
            self.assertIn(s, body)

    def test_attorney_dates_are_used_and_not_overwritten(self):
        self.seed(_rec(rid="1", knew="2026-06-01", legal="2026-09-18", internal="2026-09-08"))
        out = self.run_job()
        self.assertEqual(out["items"], 1)
        self.assertIn("社内締切 2026-09-08（弁護士設定）", self.sent_texts()[0])
        self.assertIn("法定満了 2026-09-18（弁護士設定）", self.sent_texts()[0])
        self.assertEqual(self.written_fields(), ["熟慮期間通知履歴"])
        self.assertEqual(self.records["1"]["法定満了日"]["value"], "2026-09-18")

    def test_fetch_error_sends_nothing(self):
        with patch.object(hub_kintone, "search_records",
                          AsyncMock(side_effect=hub_kintone.KintoneError("x"))):
            out = self.run_job()
        self.assertEqual(out["sent"], False)
        self.assertEqual(self.admin.await_count, 0)

    def test_notify_failure_is_logged_not_raised(self):
        self.seed(_rec(rid="1", knew="2026-06-18"))
        self.admin.return_value = False
        out = self.run_job()
        self.assertEqual(out["sent"], False)
        self.assertEqual(out["items"], 1)


# ── kind・定数・登録・結線 ────────────────────────────────────────────────────
class TestKindAndConstants(unittest.TestCase):
    def test_kind_registered_in_log_throttled(self):
        with self.assertLogs("hub.notify", level=logging.INFO) as cm:
            hub_notify._log_throttled("houki_jukuryo_daily:2026-09-08")
        out = "\n".join(cm.output)
        self.assertIn("kind=houki_jukuryo_daily", out)
        self.assertNotIn("unknown_kind", out)
        self.assertNotIn("2026-09-08", out)

    def test_constants_pinned(self):
        self.assertEqual(hj.JUKURYO_MONTHS, 3)
        self.assertEqual(hj.INTERNAL_MARGIN_DAYS, 10)
        self.assertEqual(hj.INTERNAL_PRE_DAYS, 7)
        self.assertEqual(hj.LEGAL_PRE_DAYS, 3)
        self.assertEqual(hj.LEGAL_DAILY_FROM_DAYS, 2)
        self.assertEqual(hj.HOUR_JST, 8)
        self.assertEqual(hj.JOB_NAME, "HOUKI_JUKURYO")
        self.assertEqual(hj.NOTIFY_KIND, "houki_jukuryo_daily")
        self.assertEqual(hj.FIELD_STATUS, "status")
        self.assertEqual(hj.STATUS_JUNIN, "受任")
        self.assertEqual(hj.FIELD_SUBMITTED, "申述提出日")
        self.assertEqual(hj.FIELD_HISTORY, "熟慮期間通知履歴")
        self.assertEqual(hj.FIELD_LEGAL_DEADLINE, "法定満了日")
        self.assertEqual(hj.FIELD_INTERNAL_DEADLINE, "社内締切日")
        self.assertEqual(hj.START_DATE_FIELDS,
                         ("相続人と知った日_申告", "死亡を知った日_申告", "死亡日_申告"))
        self.assertEqual(hj.MILESTONES, ("社内締切7日前", "社内締切当日", "法定満了3日前",
                                         "法定満了2日前", "法定満了1日前", "法定満了当日", "満了超過"))

    def test_module_reads_no_env(self):
        src = Path(hj.__file__).read_text(encoding="utf-8")
        self.assertNotIn("os.environ", src)
        self.assertNotIn("import os", src)

    def test_main_registers_job(self):
        src = (Path(__file__).parent / "main.py").read_text(encoding="utf-8")
        self.assertIn("register_houki_jukuryo_job()", src)


class TestJobRegistration(unittest.TestCase):
    def setUp(self):
        hub_scheduler.stop_all()
        self.addCleanup(hub_scheduler.stop_all)

    def test_registers_daily_job_at_8_jst(self):
        async def run():
            hj.register_houki_jukuryo_job()
            self.assertTrue(hub_scheduler.is_registered("HOUKI_JUKURYO"))
            job = hub_scheduler._jobs["HOUKI_JUKURYO"]
            self.assertEqual((job.kind, job.hour_jst), ("daily", 8))
            self.assertIs(job.coro_factory, hj.jukuryo_daily_check)
        asyncio.run(run())

    def test_double_registration_is_safe(self):
        async def run():
            hj.register_houki_jukuryo_job()
            t1 = hub_scheduler._jobs["HOUKI_JUKURYO"].task
            hj.register_houki_jukuryo_job()
            self.assertIs(t1, hub_scheduler._jobs["HOUKI_JUKURYO"].task)
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
