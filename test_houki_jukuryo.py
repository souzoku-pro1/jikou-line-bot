"""HOUKI-JUKURYO-2（大野裁定 2026-10-08・法的判断）: 相続放棄 熟慮期間 監視のテスト。

CRON-1 のテスト（申告 3 日付の代用順・法定/社内の 2 本立て・ONE_SHOT/DAILY・履歴欄）は
本票で挙動ごと差し替えられたため、同じ性質（計算・対象・冪等・送信→書込の順序・分割・
ページング・PII なし・登録）を新裁定の形で検証する。削除した性質はない。

- 期限計算（応当日の前日・応当日なし＝その月の末日の前日・閏年・年またぎ・月初）
- 入力は 起算日_確定 のみ（申告 3 日付・起算点確定済・法定満了日・社内締切日 は読まない）
- 対象 status の網羅（受任後 8 値・受任前は対象外・申述提出日ありは対象外）
- 熟慮期間期限／残日数 の保存（差分だけ書く・起算日_確定 変更で上書き・空に戻れば空に戻す）
- 通知: 14/7 日前・各 1 回（同日 3 回走っても 1 通・通知済み閾値 で判定・送信成功後に刻印）
- 送信失敗は刻印しない（期限欄は書く）・throttled は成功扱い・CAS 409 は再取得 1 回
- 起算日未確定: 件数のみ・1 日 1 回・ID なし
- PII 非漏洩（本文・ログ・キー）・書く欄は 3 欄の閉集合・分割送信・ページング・登録（8/13/18）
- phone_triage の式が同じ関数に寄っている（一本化）
"""

import asyncio
import logging
import re
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

from hub import houki_jukuryo as hj
from hub import houki_phone_triage as tri
from hub import houki_profile
from hub import kintone as hub_kintone
from hub import notify as hub_notify
from hub import scheduler as hub_scheduler

TODAY = date(2026, 9, 8)
_REAL_NOTIFY_RESULT = hub_notify.notify_admin_line_result      # _Base の mock 前に捕捉
KEY_RE = re.compile(r"^houki_jukuryo_daily:2026-09-\d\d:[0-9a-f]{16}$")
NAME = "山田太郎"
KANA = "やまだたろう"

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


# ── 期限計算（裁定 (a)） ─────────────────────────────────────────────────────
class TestDeadlineFormula(unittest.TestCase):
    def test_normal_anniversary_minus_one(self):
        self.assertEqual(hj.jukuryo_deadline(date(2026, 4, 10)), date(2026, 7, 9))      # 票の例
        self.assertEqual(hj.jukuryo_deadline(date(2026, 6, 15)), date(2026, 9, 14))

    def test_no_anniversary_is_month_end_minus_one_non_leap(self):
        # 11/30 → 2 月に 30 日なし → 2 月末日（2/28）の前日 = 2/27（翌月末日の前日ではない）
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


# ── 通知判定・保存差分（pure） ───────────────────────────────────────────────
class TestAlertsAndUpdates(unittest.TestCase):
    def test_alerts_due(self):
        self.assertEqual(hj.alerts_due(15, []), [])
        self.assertEqual(hj.alerts_due(14, []), [14])
        self.assertEqual(hj.alerts_due(8, ["14日前"]), [])
        self.assertEqual(hj.alerts_due(7, ["14日前"]), [7])
        self.assertEqual(hj.alerts_due(7, []), [14, 7])                 # 14 を取りこぼしていれば両方
        self.assertEqual(hj.alerts_due(0, ["14日前"]), [7])
        self.assertEqual(hj.alerts_due(-3, []), [14, 7])                # 超過でも未通知分は送る
        self.assertEqual(hj.alerts_due(-3, ["14日前", "7日前"]), [])
        self.assertEqual(hj.alerts_due(7, ["30日前", "14日前", "7日前", "超過"]), [])

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


# ── 本文・digest（PII なし） ─────────────────────────────────────────────────
class TestNoticeBody(unittest.TestCase):
    def test_line_has_record_deadline_remaining_only(self):
        c = hj.compute(_rec(start="2026-06-15"), TODAY)
        line = hj.format_alert_line("12", 7, c)
        self.assertEqual(line, "・No.12 期限 2026-09-14 残 6 日（7日前の通知）")
        self.assertNotIn(NAME, line)

    def test_notice_text_and_key(self):
        entries, unset = hj.plan_alerts([_rec(rid="1", start=_start_for_remaining(14)),
                                         _rec(rid="2", start=_start_for_remaining(7))], TODAY)
        self.assertEqual(unset, 0)
        notices = hj.build_notices(TODAY, entries)
        self.assertEqual(len(notices), 1)
        t = notices[0].text
        self.assertTrue(t.startswith("【相続放棄 熟慮期間】 2026-09-08 (1/1)"))
        self.assertIn("・No.1 期限 2026-09-22 残 14 日（14日前の通知）", t)
        self.assertIn("・No.2 期限 2026-09-15 残 7 日（14日前の通知）", t)
        self.assertIn("・No.2 期限 2026-09-15 残 7 日（7日前の通知）", t)
        self.assertTrue(t.endswith(hj.NOTICE_FOOTER))
        for leak in (NAME, KANA, "2026-05-01", "2026-04-01"):
            self.assertNotIn(leak, t)
        self.assertTrue(KEY_RE.match(hj.notify_key(TODAY, notices[0].digest)))

    def test_digest_is_order_independent_and_content_sensitive(self):
        a = _rec(rid="1", start=_start_for_remaining(14))
        b = _rec(rid="2", start=_start_for_remaining(7))
        e1, _ = hj.plan_alerts([a, b], TODAY)
        e2, _ = hj.plan_alerts([b, a], TODAY)
        self.assertEqual(hj.digest_of(e1), hj.digest_of(e2))
        e3, _ = hj.plan_alerts([a], TODAY)
        self.assertNotEqual(hj.digest_of(e1), hj.digest_of(e3))

    def test_split_by_entry_and_numbered(self):
        recs = [_rec(rid=str(i), start=_start_for_remaining(14)) for i in range(1, 61)]
        entries, _ = hj.plan_alerts(recs, TODAY)
        notices = hj.build_notices(TODAY, entries, max_chars=600)
        self.assertGreater(len(notices), 1)
        ids = [e.record_id for n in notices for e in n.entries]
        self.assertEqual(ids, [str(i) for i in range(1, 61)])
        for i, n in enumerate(notices, 1):
            self.assertIn(f"({i}/{len(notices)})", n.text)
            self.assertLessEqual(len(n.text), 600)

    def test_unset_text_has_count_only(self):
        t = hj.unset_text(TODAY, 3)
        self.assertIn("起算日未確定 3 件", t)
        self.assertNotIn("No.", t)


# ── ジョブ（kintone・notify を fake） ──────────────────────────────────────────
class _Base(unittest.TestCase):
    def setUp(self):
        self.records: dict[str, dict] = {}
        self.updates: list[tuple] = []
        self.gets: list[str] = []
        self.conflict_next: dict[str, int] = {}        # rid → 409 を返す残回数
        self.fail_on: set[str] = set()
        self.queries: list[str] = []

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

        self.admin = AsyncMock(return_value="sent")
        self.today = TODAY
        hj._unset_notified_on = None
        self.addCleanup(setattr, hj, "_unset_notified_on", None)
        for p in (patch.object(hub_kintone, "search_records", search_records),
                  patch.object(hub_kintone, "get_record", get_record),
                  patch.object(hub_kintone, "update_record", update_record),
                  patch.object(hub_notify, "notify_admin_line_result", self.admin),
                  patch.object(hj, "_today_jst", lambda: self.today)):
            p.start()
            self.addCleanup(p.stop)

    def seed(self, *recs):
        for r in recs:
            self.records[r["$id"]["value"]] = r

    def run_job(self, hour=8):
        return asyncio.run(hj.jukuryo_daily_check(hour))

    def val(self, rid, code):
        return self.records[rid][code]["value"]

    def sent_texts(self):
        return [c.args[0] for c in self.admin.await_args_list]

    def sent_keys(self):
        return [c.kwargs["throttle_key"] for c in self.admin.await_args_list]

    def alert_texts(self):
        return [t for t in self.sent_texts() if hj.UNSET_LABEL not in t]

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
        self.assertEqual((res["written"], res["alerts"]), (1, 0))
        self.assertEqual(self.sent_texts(), [])

    def test_alert_14_sent_once_and_marked(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        res = self.run_job()
        self.assertEqual(len(self.alert_texts()), 1)
        self.assertIn("・No.1 期限 2026-09-22 残 14 日（14日前の通知）", self.alert_texts()[0])
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])
        self.assertEqual(self.val("1", "残日数"), "14")
        self.assertEqual((res["alerts"], res["sent"], res["written"]), (1, 1, 1))
        self.assertEqual(len([u for u in self.updates if u[1] == "1"]), 1)   # 1 レコード 1 回の書込

    def test_same_day_three_runs_send_once_and_write_once(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.run_job(8)
        self.run_job(13)
        self.run_job(18)
        self.assertEqual(len(self.alert_texts()), 1)
        self.assertEqual(len(self.updates), 1)
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])

    def test_alert_7_after_14_marked(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(7), notified=("14日前",),
                       deadline="2026-09-15", remaining="8"))
        self.run_job()
        t = self.alert_texts()[0]
        self.assertIn("（7日前の通知）", t)
        self.assertNotIn("（14日前の通知）", t)
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前", "7日前"])
        self.assertEqual(self.val("1", "残日数"), "7")

    def test_missed_14_is_sent_together_with_7(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(7)))
        self.run_job()
        self.assertEqual(len(self.alert_texts()), 1)
        self.assertIn("（14日前の通知）", self.alert_texts()[0])
        self.assertIn("（7日前の通知）", self.alert_texts()[0])
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前", "7日前"])

    def test_remaining_15_no_alert_only_fields(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(15)))
        res = self.run_job()
        self.assertEqual(self.alert_texts(), [])
        self.assertEqual(self.val("1", "残日数"), "15")
        self.assertEqual(self.val("1", "通知済み閾値"), [])
        self.assertEqual(res["alerts"], 0)

    def test_overdue_without_marks_still_sends_once_no_overdue_notice(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(-3)))
        self.run_job()
        self.run_job(13)
        self.assertEqual(len(self.alert_texts()), 1)
        self.assertIn("残 -3 日", self.alert_texts()[0])
        self.assertNotIn("超過", self.alert_texts()[0])
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前", "7日前"])

    def test_both_marked_overdue_sends_nothing(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(-3), notified=("14日前", "7日前")))
        self.run_job()
        self.assertEqual(self.alert_texts(), [])
        self.assertEqual(self.val("1", "残日数"), "-3")

    def test_start_change_recalculates_and_overwrites(self):
        self.seed(_rec(rid="1", start="2026-06-15", deadline="2026-09-14", remaining="6",
                       notified=("14日前", "7日前")))
        self.run_job()
        self.assertEqual(self.updates, [])                                # 変化なし → 書かない
        self.records["1"]["起算日_確定"] = {"value": "2026-08-01"}       # 弁護士が起算日を直す
        self.run_job(13)
        self.assertEqual(self.val("1", "熟慮期間期限"), "2026-10-31")
        self.assertEqual(self.val("1", "残日数"), "53")
        self.assertEqual(len(self.updates), 1)
        self.assertEqual(self.alert_texts(), [])

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
        self.assertEqual(res["alerts"], 8)
        for i in range(1, 9):
            self.assertEqual(self.val(str(i), "通知済み閾値"), ["14日前"])

    def test_non_target_records_are_skipped_even_if_returned(self):
        self.seed(_rec(rid="1", status="問い合わせ", start=_start_for_remaining(14)),
                  _rec(rid="2", status="受任", submitted="2026-09-01", start=_start_for_remaining(14)),
                  _rec(rid="3", status="辞任", start=_start_for_remaining(14)))
        res = self.run_job()
        self.assertEqual(self.sent_texts(), [])
        self.assertEqual(self.updates, [])
        self.assertEqual(res["alerts"], 0)

    def test_send_failed_writes_fields_but_no_mark_and_resends_next_run(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.admin.return_value = "failed"
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            res = self.run_job()
        self.assertEqual(res["failed"], 1)
        self.assertEqual(self.val("1", "通知済み閾値"), [])
        self.assertEqual(self.val("1", "残日数"), "14")                  # 期限欄は書く
        self.admin.return_value = "sent"
        self.run_job(13)
        self.assertEqual(len(self.alert_texts()), 2)
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])

    def test_throttled_counts_as_sent_and_marks(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        self.admin.return_value = "throttled"
        res = self.run_job()
        self.assertEqual(res["sent"], 1)
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])

    def test_mark_written_only_after_send(self):
        order = []
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        real_update = hub_kintone.update_record

        async def spy_update(app, rid, fields, revision=None):
            order.append(("update", sorted(fields)))
            return await real_update(app, rid, fields, revision)

        async def spy_notify(text, throttle_key=None):
            order.append(("notify",))
            return "sent"
        with patch.object(hub_kintone, "update_record", spy_update), \
                patch.object(hub_notify, "notify_admin_line_result", spy_notify):
            self.run_job()
        self.assertEqual(order, [("notify",), ("update", ["残日数", "熟慮期間期限", "通知済み閾値"])])

    def test_cas_conflict_refetches_once_and_retries_with_new_values(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14), revision="5"))
        self.conflict_next["1"] = 1
        orig_get = hub_kintone.get_record

        async def get_changed(app, rid):            # 409 の裏で弁護士が起算日を変えていた
            self.records[rid]["$revision"] = {"value": "6"}
            self.records[rid]["起算日_確定"] = {"value": _start_for_remaining(40)}
            return await orig_get(app, rid)
        with patch.object(hub_kintone, "get_record", get_changed):
            res = self.run_job()
        self.assertEqual(res["written"], 1)
        self.assertEqual([u[3] for u in self.updates], ["5", "6"])
        self.assertEqual(self.val("1", "残日数"), "40")                  # 再計算した値で保存
        self.assertEqual(self.val("1", "通知済み閾値"), ["14日前"])      # 送った刻印は保持

    def test_cas_conflict_twice_warns_and_does_not_raise(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
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

    def test_multiple_records_one_notice(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)),
                  _rec(rid="2", start=_start_for_remaining(7), notified=("14日前",)),
                  _rec(rid="3", start=_start_for_remaining(20)))
        res = self.run_job()
        self.assertEqual(len(self.alert_texts()), 1)
        self.assertIn("No.1", self.alert_texts()[0])
        self.assertIn("No.2", self.alert_texts()[0])
        self.assertNotIn("No.3", self.alert_texts()[0])
        self.assertEqual(res["written"], 3)


class TestUnsetCount(_Base):
    def test_count_only_once_per_day(self):
        self.seed(_rec(rid="1", start=""), _rec(rid="2", start=""), _rec(rid="3", start=_start_for_remaining(30)))
        res = self.run_job(8)
        self.assertTrue(res["unset_notified"])
        unset = [t for t in self.sent_texts() if hj.UNSET_LABEL in t]
        self.assertEqual(len(unset), 1)
        self.assertIn("起算日未確定 2 件", unset[0])
        self.assertNotIn("No.", unset[0])
        self.assertNotIn(NAME, unset[0])
        self.assertEqual(self.sent_keys()[-1], "houki_jukuryo_daily:2026-09-08:unset")
        self.run_job(13)
        self.run_job(18)
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
        self.assertFalse(res["unset_notified"])
        self.assertEqual(self.sent_texts(), [])

    def test_failed_unset_notice_is_retried_at_next_run(self):
        self.seed(_rec(rid="1", start=""))
        self.admin.return_value = "failed"
        with self.assertLogs("hub.houki_jukuryo", level=logging.WARNING):
            res = self.run_job(8)
        self.assertFalse(res["unset_notified"])
        self.admin.return_value = "sent"
        res = self.run_job(13)
        self.assertTrue(res["unset_notified"])

    def test_unset_records_write_nothing_when_fields_empty(self):
        self.seed(_rec(rid="1", start=""))
        self.run_job()
        self.assertEqual(self.updates, [])


class TestPiiAndClosedSet(_Base):
    def test_no_pii_in_texts_keys_or_logs(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(7)), _rec(rid="2", start=""))
        with self.assertLogs("hub.houki_jukuryo", level=logging.INFO) as cm:
            self.run_job()
        blob = "\n".join(self.sent_texts() + self.sent_keys() + cm.output)
        for leak in (NAME, KANA, "2026-05-01", "2026-04-01"):
            self.assertNotIn(leak, blob)

    def test_only_three_fields_are_ever_written(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(7)),
                  _rec(rid="2", start="", deadline="2026-01-01", remaining="3"))
        self.run_job()
        self.assertTrue(set(self.written_fields()) <= hj.WRITE_FIELDS, self.written_fields())
        for _app, _rid, fields, _rev in self.updates:
            for banned in ("status", "起算日_確定", "法定満了日", "社内締切日", "熟慮期間通知履歴", "申述提出日"):
                self.assertNotIn(banned, fields)
        self.assertEqual({u[0] for u in self.updates}, {"APP_HOUKI"})


class TestSplitSend(_Base):
    def test_60_records_split_into_multiple_notices_all_marked(self):
        for i in range(1, 61):
            self.seed(_rec(rid=str(i), start=_start_for_remaining(14)))
        with patch.object(hj, "NOTICE_MAX_CHARS", 600):
            res = self.run_job()
        self.assertGreater(res["notices"], 1)
        self.assertEqual(res["sent"], res["notices"])
        for i in range(1, 61):
            self.assertEqual(self.val(str(i), "通知済み閾値"), ["14日前"])
        for k in self.sent_keys():
            self.assertTrue(KEY_RE.match(k), k)

    def test_one_failed_notice_leaves_only_its_records_unmarked(self):
        for i in range(1, 61):
            self.seed(_rec(rid=str(i), start=_start_for_remaining(14)))
        calls = {"n": 0}

        async def flaky(text, throttle_key=None):
            calls["n"] += 1
            return "failed" if calls["n"] == 2 else "sent"
        with patch.object(hj, "NOTICE_MAX_CHARS", 600), \
                patch.object(hub_notify, "notify_admin_line_result", flaky):
            res = self.run_job()
        self.assertEqual(res["failed"], 1)
        marked = [i for i in range(1, 61) if self.val(str(i), "通知済み閾値") == ["14日前"]]
        unmarked = [i for i in range(1, 61) if self.val(str(i), "通知済み閾値") == []]
        self.assertTrue(marked and unmarked)
        self.assertEqual(len(marked) + len(unmarked), 60)
        for i in unmarked:
            self.assertEqual(self.val(str(i), "残日数"), "14")             # 期限欄は書かれている


class TestDigestKeys(_Base):
    def test_real_notify_same_content_is_throttled_and_treated_as_sent(self):
        self.seed(_rec(rid="1", start=_start_for_remaining(14)))
        pushes = []

        async def fake_push(admin_id, text, token_env=None):
            pushes.append(text)
            return True
        with patch.object(hub_notify, "notify_admin_line_result", _REAL_NOTIFY_RESULT), \
                patch.object(hub_notify, "get_admin_line_user_id", lambda: "Uadmin"), \
                patch.object(hub_notify, "push_line_message", fake_push), \
                patch.dict(hub_notify._last_notify_at, {}, clear=True):
            r1 = self.run_job(8)
            self.records["1"]["通知済み閾値"] = {"value": []}            # 刻印が消えた体で同内容を再送
            r2 = self.run_job(13)
        self.assertEqual((r1["sent"], r2["sent"]), (1, 1))
        self.assertEqual(len([p for p in pushes if "14日前" in p]), 1)      # 実送信は 1 回（throttled）


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
            rows = asyncio.run(hj.fetch_all_targets())
        self.assertEqual(len(rows), 1000)


class TestKindAndConstants(unittest.TestCase):
    def test_kind_registered_in_log_throttled(self):
        with self.assertLogs("hub.notify", level=logging.INFO) as cm:
            hub_notify._log_throttled("houki_jukuryo_daily:2026-09-08:0123456789abcdef")
        out = "\n".join(cm.output)
        self.assertIn("kind=houki_jukuryo_daily", out)
        self.assertNotIn("unknown_kind", out)
        self.assertNotIn("2026-09-08", out)

    def test_constants_pinned(self):
        # HOUKI-JUKURYO-2（大野裁定 2026-10-08）
        self.assertEqual(hj.JUKURYO_MONTHS, 3)
        self.assertEqual(hj.ALERT_DAYS, (14, 7))
        self.assertEqual(hj.NOTIFIED_VALUES, {14: "14日前", 7: "7日前"})
        self.assertEqual(hj.RUN_HOURS_JST, (8, 13, 18))
        self.assertEqual(hj.NOTICE_MAX_CHARS, 3800)
        self.assertLess(hj.NOTICE_MAX_CHARS, 4900)
        self.assertEqual(hj.DIGEST_HEX_LEN, 16)
        self.assertEqual(hj.JOB_NAME, "HOUKI_JUKURYO")
        self.assertEqual(hj.NOTIFY_KIND, "houki_jukuryo_daily")
        self.assertEqual((hj.FIELD_STATUS, hj.FIELD_SUBMITTED, hj.FIELD_START),
                         ("status", "申述提出日", "起算日_確定"))
        self.assertEqual((hj.FIELD_DEADLINE, hj.FIELD_REMAINING, hj.FIELD_NOTIFIED),
                         ("熟慮期間期限", "残日数", "通知済み閾値"))
        self.assertEqual(hj.SEARCH_LIMIT, 500)
        self.assertEqual(sorted(hj.SEARCH_FIELDS),
                         sorted(["$id", "$revision", "status", "申述提出日", "起算日_確定",
                                 "熟慮期間期限", "残日数", "通知済み閾値"]))
        for absent in ("法定満了日", "社内締切日", "熟慮期間通知履歴", "相続人と知った日_申告",
                       "死亡を知った日_申告", "死亡日_申告", "起算点確定済", "伸長後満了日"):
            self.assertNotIn(absent, hj.SEARCH_FIELDS)
        self.assertFalse(hasattr(hj, "INTERNAL_MARGIN_DAYS"))             # 2 本立ては廃止
        self.assertFalse(hasattr(hj, "START_DATE_FIELDS"))                 # 申告代用は廃止

    def test_module_reads_no_env_and_no_extension(self):
        src = Path(hj.__file__).read_text(encoding="utf-8")
        self.assertNotIn("os.environ", src)
        self.assertNotIn("import os", src)
        self.assertNotIn("伸長後満了日", src.split('"""', 2)[2])           # 本文コードに伸長の分岐なし

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
