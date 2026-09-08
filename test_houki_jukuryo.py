"""HOUKI-JUKURYO-CRON-1 / fix1 / fix2: 相続放棄 熟慮期間 日次監視のテスト。

- 期日計算（応当日・月末・閏年・起算日代用順・全空）
- 社内締切 10 日前・マイルストーン判定（7 日前／当日／3 日前／毎日／超過・境界）
- 対象抽出（受任以外・提出日ありは除外）・弁護士設定優先
- 履歴による冪等（同日再実行 0 通）・fix1: 送信成功→履歴追記の順序（失敗時は追記しない・
  追記 CAS 失敗は警告のみ・throttled は成功扱い）
- fix2 HJC-01: 分割送信（上限・(n/N)・縮約・失敗した通の案件だけ履歴なし）
- fix2 HJC-02: digest キー（別集合は別キー・同一内容の再実行は throttled=成功）
- fix2 HJC-03: 全件取得（500 ちょうど→次ページ・501 件目・0/499/1000 超）
- fix2 HJC-04: 単発の未送信回収（遅延・本来）・8 日前は回収しない・13:00 の回・起算日未設定は 8:00 のみ
- 本文に個人情報なし・kind 登録・定数 pin・ジョブ登録（8/13/18 JST）・main 結線
"""

import asyncio
import logging
import re
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

from hub import houki_jukuryo as hj
from hub import kintone as hub_kintone
from hub import notify as hub_notify
from hub import scheduler as hub_scheduler

TODAY = date(2026, 9, 8)
_REAL_NOTIFY_RESULT = hub_notify.notify_admin_line_result      # _Base の mock 前に捕捉
KEY_RE = re.compile(r"^houki_jukuryo_daily:2026-09-\d\d:[0-9a-f]{16}$")


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


def _names(ms):
    return [m.name for m in ms]


# 社内締切当日（9/8）の案件。7 日前（9/1）は回収窓（今日−7）内に入るため、送信済みの履歴を
# 持たせて「当日」だけを検証する（回収そのものは TestOneShotRecovery で検証）
H7 = "2026-09-01 社内締切7日前 通知済"


def _due(rid="1", **kw):
    kw.setdefault("knew", "2026-06-18")
    kw.setdefault("history", H7)
    return _rec(rid=rid, **kw)


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

    def test_internal_pre_7_boundary(self):
        self.assertEqual(self.ms(date(2026, 8, 28)), [])                        # 前日は非該当
        on = self.ms(date(2026, 8, 29))
        self.assertEqual(_names(on), [hj.MS_INTERNAL_PRE])
        self.assertFalse(on[0].delayed(date(2026, 8, 29)))                      # 当日は定刻
        late = self.ms(date(2026, 8, 30))                                        # 翌日は遅延回収
        self.assertEqual([(m.name, m.due) for m in late], [(hj.MS_INTERNAL_PRE, date(2026, 8, 29))])
        self.assertTrue(late[0].delayed(date(2026, 8, 30)))

    def test_internal_day_boundary(self):
        self.assertNotIn(hj.MS_INTERNAL_DAY, _names(self.ms(date(2026, 9, 4))))
        self.assertIn(hj.MS_INTERNAL_DAY, _names(self.ms(date(2026, 9, 5))))
        self.assertIn((hj.MS_INTERNAL_DAY, date(2026, 9, 5)),
                      [(m.name, m.due) for m in self.ms(date(2026, 9, 6))])

    def test_legal_pre_3_boundary(self):
        self.assertNotIn(hj.MS_LEGAL_PRE, _names(self.ms(date(2026, 9, 11))))
        self.assertIn(hj.MS_LEGAL_PRE, _names(self.ms(date(2026, 9, 12))))

    def test_one_shot_recovery_window_is_7_days(self):
        # 社内締切当日 9/5 → 9/12 は 7 日後で回収、9/13 は 8 日後で回収しない
        self.assertIn(hj.MS_INTERNAL_DAY, _names(self.ms(date(2026, 9, 12))))
        self.assertNotIn(hj.MS_INTERNAL_DAY, _names(self.ms(date(2026, 9, 13))))
        # 社内締切7日前 8/29 → 9/5 は回収、9/6 は回収しない
        self.assertIn(hj.MS_INTERNAL_PRE, _names(self.ms(date(2026, 9, 5))))
        self.assertNotIn(hj.MS_INTERNAL_PRE, _names(self.ms(date(2026, 9, 6))))

    def test_legal_daily_2_1_0_are_daily_only(self):
        self.assertEqual([(m.name, m.due) for m in self.ms(date(2026, 9, 13))
                          if m.name in hj.DAILY], [(hj.MS_LEGAL_2, date(2026, 9, 13))])
        self.assertIn(hj.MS_LEGAL_1, _names(self.ms(date(2026, 9, 14))))
        self.assertIn(hj.MS_LEGAL_DAY, _names(self.ms(date(2026, 9, 15))))
        self.assertNotIn(hj.MS_LEGAL_2, _names(self.ms(date(2026, 9, 14))))   # DAILY は当日一致のみ

    def test_overdue_every_day(self):
        self.assertIn(hj.MS_OVERDUE, _names(self.ms(date(2026, 9, 16))))
        self.assertEqual(_names(self.ms(date(2026, 12, 1))), [hj.MS_OVERDUE])

    def test_multiple_when_attorney_dates_coincide(self):
        dl = hj.compute_deadlines(_rec(legal="2026-09-15", internal="2026-09-12"))
        self.assertEqual(_names(hj.milestones_for(date(2026, 9, 12), dl)),
                         [hj.MS_INTERNAL_PRE, hj.MS_INTERNAL_DAY, hj.MS_LEGAL_PRE])   # 7日前(9/5)は回収窓内

    def test_unset_has_no_milestone(self):
        self.assertEqual(hj.milestones_for(date(2026, 9, 15), hj.compute_deadlines(_rec())), [])

    def test_kinds_partition(self):
        self.assertEqual(hj.ONE_SHOT, ("社内締切7日前", "社内締切当日", "法定満了3日前"))
        self.assertEqual(hj.DAILY, ("法定満了2日前", "法定満了1日前", "法定満了当日", "満了超過"))
        self.assertEqual(hj.MILESTONES, hj.ONE_SHOT + hj.DAILY)


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
        self.assertIn("order by $id asc limit 500", q)
        self.assertIn("and $id > 500 order by", hj.search_query("500"))

    def test_history_line_and_has(self):
        line = hj.history_line(TODAY, hj.MS_LEGAL_DAY)
        self.assertEqual(line, "2026-09-08 法定満了当日 通知済")
        self.assertTrue(hj.history_has("x\n" + line + "\n", TODAY, hj.MS_LEGAL_DAY))
        self.assertFalse(hj.history_has(line, date(2026, 9, 7), hj.MS_LEGAL_DAY))
        self.assertFalse(hj.history_has(line, TODAY, hj.MS_OVERDUE))
        self.assertFalse(hj.history_has("", TODAY, hj.MS_LEGAL_DAY))

    def test_history_line_delayed_and_has_name(self):
        line = hj.history_line(TODAY, hj.MS_INTERNAL_DAY, date(2026, 9, 7))
        self.assertEqual(line, "2026-09-08 社内締切当日 通知済(本来 2026-09-07)")
        self.assertEqual(hj.history_line(TODAY, hj.MS_INTERNAL_DAY, TODAY), "2026-09-08 社内締切当日 通知済")
        self.assertTrue(hj.history_has_name(line, hj.MS_INTERNAL_DAY))
        self.assertTrue(hj.history_has_name("2026-08-01 社内締切当日 通知済", hj.MS_INTERNAL_DAY))
        self.assertFalse(hj.history_has_name(line, hj.MS_INTERNAL_PRE))
        self.assertTrue(hj.history_has(line, TODAY, hj.MS_INTERNAL_DAY))

    def test_append_history_text(self):
        self.assertEqual(hj.append_history_text("", "a"), "a")
        self.assertEqual(hj.append_history_text("a", "b"), "a\nb")


class TestNoticeBody(unittest.TestCase):
    def test_body_has_no_pii_and_marks_sources(self):
        recs = [_due(rid="7", name="山田太郎"), _rec(rid="9")]
        notices = hj.build_notices(TODAY, hj.plan_today(recs, TODAY))
        self.assertEqual(len(notices), 1)
        body = notices[0].text
        self.assertNotIn("山田", body)
        self.assertIn("(1/1)", body)
        self.assertIn("No.7 社内締切当日 /", body)
        self.assertIn("法定満了 2026-09-18（計算値）", body)
        self.assertIn("社内締切 2026-09-08（計算値）", body)
        self.assertIn("相続人と知った日_申告・申告ベース・要確認", body)
        self.assertIn("起算日未設定: No.9", body)
        self.assertIn(hj.NOTICE_FOOTER, body)

    def test_attorney_source_marked(self):
        dl = hj.compute_deadlines(_rec(knew="2026-06-15", legal="2026-10-01", internal="2026-09-20"))
        line = hj.format_item("3", hj.Milestone(hj.MS_LEGAL_PRE, TODAY), dl, TODAY)
        self.assertIn("法定満了 2026-10-01（弁護士設定）", line)
        self.assertIn("社内締切 2026-09-20（弁護士設定）", line)
        self.assertNotIn("遅延", line)

    def test_delayed_item_marked(self):
        dl = hj.compute_deadlines(_rec(knew="2026-06-17"))                         # 社内 9/7
        line = hj.format_item("3", hj.Milestone(hj.MS_INTERNAL_DAY, date(2026, 9, 7)), dl, TODAY)
        self.assertIn("No.3 社内締切当日（遅延・本来 2026-09-07） /", line)

    def test_split_by_entry_and_numbered(self):
        recs = [_due(rid=str(i)) for i in range(1, 61)]      # 60 案件・全て社内締切当日
        entries = hj.plan_today(recs, TODAY)
        notices = hj.build_notices(TODAY, entries)
        self.assertGreater(len(notices), 1)
        for i, nt in enumerate(notices, 1):
            self.assertLessEqual(len(nt.text), hj.NOTICE_MAX_CHARS)
            self.assertIn(f"({i}/{len(notices)})", nt.text)
        ids = [e.record_id for nt in notices for e in nt.entries]
        self.assertEqual(sorted(ids, key=int), [str(i) for i in range(1, 61)])     # 全案件が丁度 1 通に
        self.assertEqual(len(set(nt.digest for nt in notices)), len(notices))      # 通ごとに別 digest

    def test_entry_lines_stay_together(self):
        rec = _rec(rid="1", legal="2026-09-11", internal="2026-09-08", history=H7)            # 社内当日+法定3日前
        entries = hj.plan_today([rec], TODAY)
        self.assertEqual(len(entries), 1)
        self.assertEqual(len(entries[0].lines), 2)
        notices = hj.build_notices(TODAY, entries)
        self.assertEqual(len(notices), 1)
        self.assertIn("社内締切当日", notices[0].text)
        self.assertIn("法定満了3日前", notices[0].text)

    def test_oversize_entry_is_compacted_not_silently_cut(self):
        rec = _rec(rid="1", legal="2026-09-11", internal="2026-09-08", history=H7)
        entries = hj.plan_today([rec], TODAY)
        frame = len(hj._notice_text(TODAY, [], 99, 99))
        notices = hj.build_notices(TODAY, entries, max_chars=frame + 120)            # 案件 2 行は入らない
        self.assertEqual(len(notices), 1)
        self.assertIn(hj.COMPACT_MARK, notices[0].text)
        self.assertIn("No.1 社内締切当日/法定満了3日前 / 法定満了 2026-09-11 / 社内締切 2026-09-08", notices[0].text)
        self.assertEqual(notices[0].entries[0].history_lines,
                         ["2026-09-08 社内締切当日 通知済", "2026-09-08 法定満了3日前 通知済"])

    def test_digest_depends_on_record_and_milestone_set(self):
        a = hj.plan_today([_due(rid="1")], TODAY)
        b = hj.plan_today([_due(rid="2")], TODAY)
        ab = hj.plan_today([_due(rid="1"), _due(rid="2")], TODAY)
        self.assertNotEqual(hj.digest_of(a), hj.digest_of(b))
        self.assertNotEqual(hj.digest_of(a), hj.digest_of(ab))
        self.assertEqual(hj.digest_of(a), hj.digest_of(hj.plan_today([_due(rid="1")], TODAY)))
        self.assertTrue(KEY_RE.match(hj.notify_key(TODAY, hj.digest_of(a))))


# ── ジョブ（kintone・LINE は mock） ──────────────────────────────────────────
class _Base(unittest.TestCase):
    def setUp(self):
        self.records: dict[str, dict] = {}
        self.updates: list[tuple] = []
        self.conflict_on: set[str] = set()
        self.fail_on: set[str] = set()
        self.queries: list[str] = []
        self.seeded_history: dict[str, str] = {}

        async def search_records(app, query, fields=None):
            self.queries.append(query)
            m = re.search(r"\$id > (\d+)", query)
            after = int(m.group(1)) if m else 0
            rows = sorted((r for r in self.records.values() if int(r["$id"]["value"]) > after),
                          key=lambda r: int(r["$id"]["value"]))
            return [dict(r) for r in rows[:hj.SEARCH_LIMIT]]

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

        self.admin = AsyncMock(return_value="sent")       # 3 値（sent/throttled/failed）
        self.today = TODAY
        for p in (patch.object(hub_kintone, "search_records", search_records),
                  patch.object(hub_kintone, "update_record", update_record),
                  patch.object(hub_notify, "notify_admin_line_result", self.admin),
                  patch.object(hj, "_today_jst", lambda: self.today)):
            p.start()
            self.addCleanup(p.stop)

    def seed(self, *recs):
        for r in recs:
            self.records[r["$id"]["value"]] = r
            self.seeded_history[r["$id"]["value"]] = r["熟慮期間通知履歴"]["value"]

    def run_job(self, hour=8):
        return asyncio.run(hj.jukuryo_daily_check(hour))

    def raw_history(self, rid):
        return self.records[rid]["熟慮期間通知履歴"]["value"]

    def history(self, rid):
        """seed 時の履歴より後に追記された分だけ（追記なしなら ""）。"""
        raw = self.raw_history(rid)
        seeded = self.seeded_history.get(rid, "")
        if not seeded:
            return raw
        self.assertTrue(raw.startswith(seeded), raw)
        return raw[len(seeded):].lstrip("\n")

    def sent_texts(self):
        return [c.args[0] for c in self.admin.await_args_list]

    def sent_keys(self):
        return [c.kwargs["throttle_key"] for c in self.admin.await_args_list]

    def written_fields(self):
        return sorted({k for _, _, f, _ in self.updates for k in f})


class TestDailyJob(_Base):
    def test_internal_day_notifies_once_and_appends_history(self):
        self.seed(_due(rid="1"))          # 法定 9/18・社内 9/8=TODAY
        out = self.run_job()
        self.assertEqual(out, {"targets": 1, "items": 1, "unset": 0, "notices": 1, "sent": 1,
                               "failed": 0, "history_failed": 0})
        self.assertEqual(self.history("1"), "2026-09-08 社内締切当日 通知済")
        self.assertEqual(self.written_fields(), ["熟慮期間通知履歴"])   # 履歴欄以外は書かない
        self.assertEqual(self.updates[0][0], "APP_HOUKI")                # App 40 のみ
        self.assertEqual(self.admin.await_count, 1)
        self.assertTrue(KEY_RE.match(self.sent_keys()[0]))
        self.assertIn("No.1 社内締切当日 /", self.sent_texts()[0])
        self.assertNotIn("山田", self.sent_texts()[0])

    def test_same_day_rerun_sends_nothing(self):
        self.seed(_due(rid="1"))
        self.run_job()
        self.admin.reset_mock()
        n = len(self.updates)
        out = self.run_job()
        self.assertEqual((out["items"], out["notices"]), (0, 0))
        self.assertEqual(self.admin.await_count, 0)
        self.assertEqual(len(self.updates), n)                            # 追記もしない
        self.assertEqual(self.history("1"), "2026-09-08 社内締切当日 通知済")

    def test_history_from_previous_day_does_not_block(self):
        self.seed(_rec(rid="1", knew="2026-06-18", history="2026-09-01 社内締切7日前 通知済"))
        out = self.run_job()
        self.assertEqual(out["items"], 1)                                 # 7日前は同名履歴で除外・当日のみ
        self.assertEqual(self.raw_history("1"),
                         "2026-09-01 社内締切7日前 通知済\n2026-09-08 社内締切当日 通知済")

    def test_internal_day_without_prior_7day_notice_recovers_it_too(self):
        """7 日前の通知が一度も無い当日案件は、当日+遅延の 7 日前（本来 9/1）の 2 行になる。"""
        self.seed(_rec(rid="1", knew="2026-06-18", history=""))
        out = self.run_job()
        self.assertEqual(out["items"], 2)
        body = self.sent_texts()[0]
        self.assertIn("No.1 社内締切7日前（遅延・本来 2026-09-01） /", body)
        self.assertIn("No.1 社内締切当日 /", body)
        self.assertEqual(self.raw_history("1"),
                         "2026-09-08 社内締切7日前 通知済(本来 2026-09-01)\n2026-09-08 社内締切当日 通知済")

    def test_cas_conflict_after_send_logs_warning_and_does_not_raise(self):
        self.seed(_due(rid="1"))
        self.conflict_on.add("1")
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING) as cm:
            out = self.run_job()
        self.assertEqual((out["items"], out["sent"], out["history_failed"]), (1, 1, 1))
        self.assertEqual(self.admin.await_count, 1)                       # 送信は行われる
        self.assertEqual(self.history("1"), "")                           # 履歴は書けていない
        self.assertTrue(any("history append failed after notice" in m for m in cm.output))
        self.assertFalse(any("山田" in m for m in cm.output))

    def test_write_error_after_send_logs_warning_and_does_not_raise(self):
        self.seed(_due(rid="1"))
        self.fail_on.add("1")
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING) as cm:
            out = self.run_job()
        self.assertEqual((out["items"], out["sent"], out["history_failed"]), (1, 1, 1))
        self.assertEqual(self.admin.await_count, 1)
        self.assertTrue(any("history append failed after notice" in m for m in cm.output))

    def test_send_failed_writes_no_history(self):
        self.seed(_due(rid="1"), _rec(rid="5"))
        self.admin.return_value = "failed"
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING) as cm:
            out = self.run_job()
        self.assertEqual((out["items"], out["unset"], out["sent"], out["failed"]), (1, 1, 0, 1))
        self.assertEqual(self.updates, [])                                # 履歴追記 0（未設定分も）
        self.assertEqual(self.history("1"), "")
        self.assertEqual(self.history("5"), "")
        self.assertTrue(any("notice not sent" in m for m in cm.output))
        # 同日の再実行で再び対象になる
        self.admin.return_value = "sent"
        out2 = self.run_job()
        self.assertEqual((out2["items"], out2["unset"]), (1, 1))
        self.assertEqual(self.history("1"), "2026-09-08 社内締切当日 通知済")
        self.assertEqual(self.history("5"), "2026-09-08 起算日未設定 通知済")

    def test_throttled_counts_as_sent_and_writes_history(self):
        self.seed(_due(rid="1"))
        self.admin.return_value = "throttled"
        out = self.run_job()
        self.assertEqual((out["sent"], out["history_failed"]), (1, 0))
        self.assertEqual(self.history("1"), "2026-09-08 社内締切当日 通知済")

    def test_history_written_only_after_send(self):
        """送信呼出しの時点で履歴追記が 0 件であること（順序 pin）。"""
        self.seed(_due(rid="1"))
        seen = []

        async def admin(text, throttle_key=""):
            seen.append(len(self.updates))
            return "sent"
        self.admin.side_effect = admin
        self.run_job()
        self.assertEqual(seen, [0])
        self.assertEqual(len(self.updates), 1)

    def test_multiple_milestones_same_record_one_update(self):
        self.seed(_rec(rid="1", legal="2026-09-11", internal="2026-09-08", history=H7))   # 社内当日+法定3日前
        out = self.run_job()
        self.assertEqual(out["items"], 2)
        self.assertEqual(len(self.updates), 1)                             # 1 レコード 1 回の CAS 更新
        self.assertEqual(self.history("1"),
                         "2026-09-08 社内締切当日 通知済\n2026-09-08 法定満了3日前 通知済")

    def test_non_target_records_are_skipped_even_if_returned(self):
        self.seed(_rec(rid="1", knew="2026-06-18", status="書類収集中"),
                  _rec(rid="2", knew="2026-06-18", submitted="2026-09-01"))
        out = self.run_job()
        self.assertEqual((out["items"], out["notices"]), (0, 0))
        self.assertEqual(self.admin.await_count, 0)
        self.assertEqual(self.updates, [])

    def test_no_milestone_today_sends_nothing(self):
        self.seed(_rec(rid="1", knew="2026-07-01"))            # 法定 10/1・社内 9/21
        out = self.run_job()
        self.assertEqual((out["items"], out["notices"]), (0, 0))
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
        self.seed(_due(rid="1"),            # 社内締切当日
                  _rec(rid="2", knew="2026-06-25"),            # 法定 9/25・社内 9/15 → 7日前
                  _rec(rid="3", knew="2026-05-01"),            # 法定 8/1 → 超過（単発は窓外）
                  _rec(rid="4"))                               # 未設定
        out = self.run_job()
        self.assertEqual((out["items"], out["unset"], out["notices"]), (3, 1, 1))
        self.assertEqual(self.admin.await_count, 1)
        body = self.sent_texts()[0]
        for s in ("No.1 社内締切当日 /", "No.2 社内締切7日前 /", "No.3 満了超過 /", "起算日未設定: No.4"):
            self.assertIn(s, body)
        self.assertNotIn("遅延", body)

    def test_attorney_dates_are_used_and_not_overwritten(self):
        self.seed(_rec(rid="1", knew="2026-06-01", legal="2026-09-18", internal="2026-09-08", history=H7))
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
        self.assertEqual((out["sent"], out["notices"]), (0, 0))
        self.assertEqual(self.admin.await_count, 0)

    def test_notify_failure_is_logged_not_raised(self):
        self.seed(_due(rid="1"))
        self.admin.return_value = "failed"
        out = self.run_job()
        self.assertEqual((out["sent"], out["failed"], out["items"]), (0, 1, 1))
        self.assertEqual(self.history("1"), "")                           # 失敗時は追記しない


class TestSplitSend(_Base):
    """fix2 HJC-01: 分割送信と「送った分だけ履歴」。"""

    def seed60(self):
        self.seed(*[_due(rid=str(i)) for i in range(1, 61)])

    def test_60_records_split_into_multiple_notices_all_included(self):
        self.seed60()
        out = self.run_job()
        self.assertGreater(out["notices"], 1)
        self.assertEqual((out["sent"], out["failed"], out["items"]), (out["notices"], 0, 60))
        texts = self.sent_texts()
        for t in texts:
            self.assertLessEqual(len(t), hj.NOTICE_MAX_CHARS)
        for i in range(1, 61):
            self.assertEqual(sum(1 for t in texts if f"No.{i} 社内締切当日" in t), 1)
            self.assertEqual(self.history(str(i)), "2026-09-08 社内締切当日 通知済")
        self.assertEqual(len(set(self.sent_keys())), out["notices"])

    def test_one_failed_notice_leaves_only_its_records_without_history(self):
        self.seed60()
        calls = []

        async def admin(text, throttle_key=""):
            calls.append(text)
            return "failed" if len(calls) == 2 else "sent"
        self.admin.side_effect = admin
        out = self.run_job()
        self.assertEqual((out["sent"], out["failed"]), (out["notices"] - 1, 1))
        failed_ids = set(re.findall(r"No\.(\d+) 社内締切当日", calls[1]))
        self.assertTrue(failed_ids)
        for i in range(1, 61):
            if str(i) in failed_ids:
                self.assertEqual(self.history(str(i)), "")
            else:
                self.assertEqual(self.history(str(i)), "2026-09-08 社内締切当日 通知済")
        # 13:00 の回: 失敗した通の案件だけが送られる
        self.admin.side_effect = None
        self.admin.return_value = "sent"
        self.admin.reset_mock()
        out2 = self.run_job(hour=13)
        self.assertEqual(out2["items"], len(failed_ids))
        sent_ids = {m for t in self.sent_texts() for m in re.findall(r"No\.(\d+) 社内締切当日", t)}
        self.assertEqual(sent_ids, failed_ids)
        for i in range(1, 61):
            self.assertEqual(self.history(str(i)), "2026-09-08 社内締切当日 通知済")


class TestDigestKeys(_Base):
    """fix2 HJC-02: 別集合は別キー・同一内容は throttled=成功扱い。"""

    def test_new_record_within_window_gets_new_key(self):
        self.seed(_due(rid="1"))
        self.run_job()
        self.seed(_due(rid="2"))
        out = self.run_job()
        self.assertEqual(out["items"], 1)                                  # No.1 は履歴で除外
        keys = self.sent_keys()
        self.assertEqual(len(keys), 2)
        self.assertNotEqual(keys[0], keys[1])
        self.assertIn("No.2 社内締切当日", self.sent_texts()[1])
        self.assertNotIn("No.1 ", self.sent_texts()[1])

    def test_real_notify_same_content_is_throttled_and_treated_as_sent(self):
        """実 notify_admin_line_result（push は mock）: 同一内容の再実行は throttled=成功・
        別集合は sent。"""
        push = AsyncMock(return_value=True)
        for p in (patch.object(hub_notify, "notify_admin_line_result", _REAL_NOTIFY_RESULT),
                  patch.object(hub_notify, "push_line_message", push),
                  patch.object(hub_notify, "get_admin_line_user_id", return_value="Uadmin")):
            p.start()
            self.addCleanup(p.stop)
        hub_notify._last_notify_at.clear()
        hub_notify._notify_in_flight.clear()
        self.addCleanup(hub_notify._last_notify_at.clear)
        self.seed(_due(rid="1"))
        out = self.run_job()
        self.assertEqual((out["sent"], push.await_count), (1, 1))
        # 履歴が書けなかった想定（人為的に空へ戻す）→ 同一内容の再実行は throttled=成功扱いで履歴追記
        self.records["1"]["熟慮期間通知履歴"] = {"value": H7}
        out2 = self.run_job()
        self.assertEqual((out2["sent"], out2["failed"], push.await_count), (1, 0, 1))   # 送信は増えない
        self.assertEqual(self.history("1"), "2026-09-08 社内締切当日 通知済")
        # 別の案件集合は別キーで実送信される（300 秒窓内でも throttled にならない）
        self.seed(_due(rid="2"))
        out3 = self.run_job()
        self.assertEqual((out3["sent"], push.await_count), (1, 2))
        self.assertEqual(self.history("2"), "2026-09-08 社内締切当日 通知済")


class TestPaging(_Base):
    """fix2 HJC-03: 全件取得。"""

    def seed_n(self, n, knew="2026-07-01"):
        self.seed(*[_rec(rid=str(i), knew=knew) for i in range(1, n + 1)])

    def test_zero_records_one_query(self):
        out = self.run_job()
        self.assertEqual((out["targets"], len(self.queries)), (0, 1))

    def test_499_records_one_query(self):
        self.seed_n(499)
        out = self.run_job()
        self.assertEqual((out["targets"], len(self.queries)), (499, 1))

    def test_exactly_500_fetches_next_page(self):
        self.seed_n(500)
        out = self.run_job()
        self.assertEqual((out["targets"], len(self.queries)), (500, 2))
        self.assertIn("$id > 500", self.queries[1])

    def test_501st_record_due_today_is_notified(self):
        self.seed_n(500)                                                     # 500 件は該当なし
        self.seed(_due(rid="501"))                        # 501 件目が社内締切当日
        out = self.run_job()
        self.assertEqual((out["targets"], len(self.queries), out["items"]), (501, 2, 1))
        self.assertIn("No.501 社内締切当日", self.sent_texts()[0])
        self.assertEqual(self.history("501"), "2026-09-08 社内締切当日 通知済")

    def test_over_1000_records_three_pages(self):
        self.seed_n(1001)
        out = self.run_job()
        self.assertEqual((out["targets"], len(self.queries)), (1001, 3))
        self.assertIn("$id > 1000", self.queries[2])

    def test_fetch_all_targets_stops_when_id_does_not_advance(self):
        async def stuck(app, query, fields=None):
            return [_rec(rid="7")] * hj.SEARCH_LIMIT
        with patch.object(hub_kintone, "search_records", stuck), \
                self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            rows = asyncio.run(hj.fetch_all_targets())
        self.assertEqual(len(rows), hj.SEARCH_LIMIT * 2)                       # 2 ページ目で停止


class TestOneShotRecovery(_Base):
    """fix2 HJC-04: 単発の未送信回収と同日再送。"""

    def test_failed_internal_day_is_recovered_next_day_with_delay_mark(self):
        self.seed(_due(rid="1"))                                             # 社内 9/8
        self.admin.return_value = "failed"
        self.run_job()
        self.assertEqual(self.history("1"), "")
        # 翌日: 遅延付きで送信・履歴に本来
        self.today = date(2026, 9, 9)
        self.admin.return_value = "sent"
        self.admin.reset_mock()
        out = self.run_job()
        self.assertEqual(out["items"], 1)
        self.assertIn("No.1 社内締切当日（遅延・本来 2026-09-08） /", self.sent_texts()[0])
        self.assertEqual(self.history("1"), "2026-09-09 社内締切当日 通知済(本来 2026-09-08)")
        # 翌々日: 送らない
        self.today = date(2026, 9, 10)
        self.admin.reset_mock()
        out2 = self.run_job()
        self.assertEqual((out2["items"], self.admin.await_count), (0, 0))

    def test_unsent_8_days_ago_is_not_recovered(self):
        self.seed(_rec(rid="1", internal="2026-08-31", legal="2026-10-31"))   # 社内 8/31 = 今日−8
        out = self.run_job()
        self.assertEqual((out["items"], out["notices"]), (0, 0))
        self.seed(_rec(rid="2", internal="2026-09-01", legal="2026-10-31"))   # 社内 9/1 = 今日−7 → 回収
        out2 = self.run_job()
        self.assertEqual(out2["items"], 1)
        self.assertIn("No.2 社内締切当日（遅延・本来 2026-09-01）", self.sent_texts()[0])

    def test_recovered_one_shot_excluded_by_name_regardless_of_date(self):
        self.seed(_rec(rid="1", knew="2026-06-18", history=H7 + "\n" + "2026-09-02 社内締切当日 通知済(本来 2026-09-01)"))
        out = self.run_job()
        self.assertEqual((out["items"], out["notices"]), (0, 0))

    def test_1300_run_sends_only_what_0800_failed(self):
        self.seed(_due(rid="1"), _rec(rid="2", knew="2026-06-25"))   # 当日 / 7日前
        self.admin.return_value = "failed"
        self.run_job(hour=8)
        self.assertEqual((self.history("1"), self.history("2")), ("", ""))
        self.admin.return_value = "sent"
        self.admin.reset_mock()
        out = self.run_job(hour=13)
        self.assertEqual((out["items"], out["notices"]), (2, 1))
        self.assertEqual(self.history("1"), "2026-09-08 社内締切当日 通知済")
        self.assertEqual(self.history("2"), "2026-09-08 社内締切7日前 通知済")
        self.admin.reset_mock()
        out2 = self.run_job(hour=18)
        self.assertEqual((out2["items"], self.admin.await_count), (0, 0))

    def test_unset_listed_only_at_0800(self):
        self.seed(_rec(rid="5"))
        out13 = self.run_job(hour=13)
        self.assertEqual((out13["unset"], out13["notices"], self.updates), (0, 0, []))
        out18 = self.run_job(hour=18)
        self.assertEqual((out18["unset"], out18["notices"]), (0, 0))
        out8 = self.run_job(hour=8)
        self.assertEqual(out8["unset"], 1)
        self.assertIn("起算日未設定: No.5", self.sent_texts()[0])
        self.assertEqual(self.history("5"), "2026-09-08 起算日未設定 通知済")


# ── kind・定数・登録・結線 ────────────────────────────────────────────────────
class TestKindAndConstants(unittest.TestCase):
    def test_kind_registered_in_log_throttled(self):
        with self.assertLogs("hub.notify", level=logging.INFO) as cm:
            hub_notify._log_throttled("houki_jukuryo_daily:2026-09-08:0123456789abcdef")
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
        self.assertEqual(hj.ONE_SHOT_RECOVERY_DAYS, 7)
        self.assertEqual(hj.RUN_HOURS_JST, (8, 13, 18))
        self.assertEqual(hj.UNSET_LIST_HOUR_JST, 8)
        self.assertEqual(hj.NOTICE_MAX_CHARS, 3800)
        self.assertLess(hj.NOTICE_MAX_CHARS, 4900)
        self.assertEqual(hj.DIGEST_HEX_LEN, 16)
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
        self.assertEqual(hj.SEARCH_LIMIT, 500)

    def test_module_reads_no_env(self):
        src = Path(hj.__file__).read_text(encoding="utf-8")
        self.assertNotIn("os.environ", src)
        self.assertNotIn("import os", src)

    def test_main_registers_job(self):
        src = (Path(__file__).parent / "main.py").read_text(encoding="utf-8")
        self.assertIn("register_houki_jukuryo_job()", src)

    def test_notify_truncation_untouched(self):
        src = Path(hub_notify.__file__).read_text(encoding="utf-8")
        self.assertIn("text[:4900]", src)


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
