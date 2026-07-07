"""person_confirm.py（R4-2e T1 確認書き込み中核）のテスト

検証:
書き込み対象のホワイトリスト固定（確認5フィールド＋確認者/確認日時以外への
書き込みゼロ）・確認済への自動付記（確認者=OFFICE_ATTORNEY 共用・確認日時）・
死亡日の形式防御（YYYY-MM-DD以外は ValueError）・死亡日×生存の矛盾拒否・
生死=死亡で死亡日未指定の許容・人物ごと独立実行（1件の失敗が他を止めない）・
フラグ無効/env未設定の完全不発・一覧ビュー（現在値＋死亡記載の推定材料）。
kintone は全てモック。
"""

import asyncio
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import person_confirm  # noqa: E402
from person_confirm import (  # noqa: E402
    ALLOWED_FIELDS, apply_confirmations, build_payload, list_case_persons,
)

_ENV = {"PERSON_MERGE_ENABLED": "1",
        "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34",
        "OFFICE_ATTORNEY": "大野太郎"}


def run(coro):
    return asyncio.run(coro)


def change(rid, name, fields):
    return {"record_id": str(rid), "name": name, "fields": fields}


class TestBuildPayload(unittest.TestCase):
    def test_whitelist_drops_out_of_scope_fields(self):
        """確認5フィールド以外（氏名・生年月日・身分事項等）は黙って書かない"""
        with patch.dict(os.environ, _ENV):
            payload = build_payload({"生死区分": "死亡", "氏名": "書き換え",
                                     "生年月日": "1950-01-01",
                                     "身分事項": [], "相続資格": "法定相続人",
                                     "確認者": "なりすまし"})
        self.assertEqual(payload, {"生死区分": "死亡"})

    def test_confirm_stamps_confirmer_and_datetime(self):
        """確認状態=確認済 は確認者（OFFICE_ATTORNEY 共用）と確認日時を自動付記"""
        with patch.dict(os.environ, _ENV):
            payload = build_payload({"確認状態": "確認済"})
        self.assertEqual(payload["確認状態"], "確認済")
        self.assertEqual(payload["確認者"], "大野太郎")
        self.assertRegex(payload["確認日時"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
        self.assertTrue(set(payload) <= set(ALLOWED_FIELDS))

    def test_confirmer_default_when_env_unset(self):
        with patch.dict(os.environ, {**_ENV, "OFFICE_ATTORNEY": ""}):
            payload = build_payload({"確認状態": "確認済"})
        self.assertEqual(payload["確認者"], "LINE指示Bot")

    def test_no_stamp_for_other_states(self):
        payload = build_payload({"確認状態": "要再確認"})
        self.assertNotIn("確認者", payload)
        self.assertNotIn("確認日時", payload)

    def test_death_date_format_guard(self):
        for bad in ("令和7年4月13日", "2025/04/13", "2025-4-13", "昭和拾參年"):
            with self.assertRaises(ValueError, msg=bad):
                build_payload({"生死区分": "死亡", "死亡日": bad})

    def test_death_without_date_allowed(self):
        """生死=死亡で死亡日未指定は許容（死亡日不明の実務）"""
        self.assertEqual(build_payload({"生死区分": "死亡"}),
                         {"生死区分": "死亡"})

    def test_death_date_with_alive_rejected(self):
        with self.assertRaises(ValueError):
            build_payload({"生死区分": "生存", "死亡日": "2025-04-13"})


class _KT:
    def __init__(self, fail_ids=()):
        self.updated = []
        self.fail_ids = set(fail_ids)

    async def update_record(self, app, record_id, fields, revision=None):
        assert app.app_id_env == "APP_KOSEKI_PERSON"
        if str(record_id) in self.fail_ids:
            raise RuntimeError(f"boom-{record_id}")
        self.updated.append((str(record_id), fields))

    async def search_records(self, app, query, fields=None):
        raise AssertionError("apply では検索しない")


def arm(tc, kt, env=_ENV):
    for p in [patch.dict(os.environ, env),
              patch("hub.kintone.update_record", new=kt.update_record)]:
        p.start()
        tc.addCleanup(p.stop)


class TestApplyConfirmations(unittest.TestCase):
    def test_independent_execution_one_failure_does_not_stop_others(self):
        kt = _KT(fail_ids={"7"})
        arm(self, kt)
        results = run(apply_confirmations([
            change(6, "鈴木誠", {"被相続人フラグ": "yes", "死亡日": "2025-04-13",
                                 "生死区分": "死亡"}),
            change(7, "鈴木香奈", {"生死区分": "生存"}),
            change(8, "香音", {"確認状態": "確認済"})]))
        self.assertEqual([r["status"] for r in results],
                         ["updated", "error", "updated"])
        self.assertIn("boom-7", results[1]["reason"])
        self.assertEqual([rid for rid, _ in kt.updated], ["6", "8"])

    def test_all_payload_keys_within_allowed(self):
        """書き込まれた全 payload のキーが許可7フィールドの範囲内（固定）"""
        kt = _KT()
        arm(self, kt)
        run(apply_confirmations([
            change(6, "誠", {"生死区分": "死亡", "死亡日": "2025-04-13",
                             "被相続人フラグ": "yes", "氏名": "無視される"}),
            change(8, "香音", {"確認状態": "確認済", "名寄せ確定": "確定"})]))
        for rid, fields in kt.updated:
            self.assertTrue(set(fields) <= set(ALLOWED_FIELDS),
                            f"No.{rid} に対象外フィールド: {set(fields)}")

    def test_invalid_change_reported_not_raised(self):
        """形式エラー・矛盾は当該人物の error として報告（他は継続）"""
        kt = _KT()
        arm(self, kt)
        results = run(apply_confirmations([
            change(6, "誠", {"死亡日": "令和7年4月13日", "生死区分": "死亡"}),
            change(7, "香奈", {"生死区分": "生存"})]))
        self.assertEqual(results[0]["status"], "error")
        self.assertIn("YYYY-MM-DD", results[0]["reason"])
        self.assertEqual(results[1]["status"], "updated")

    def test_empty_fields_reported(self):
        kt = _KT()
        arm(self, kt)
        results = run(apply_confirmations([change(6, "誠", {"氏名": "対象外のみ"})]))
        self.assertEqual(results[0]["status"], "error")
        self.assertIn("変更内容が空", results[0]["reason"])
        self.assertEqual(kt.updated, [])

    def test_flag_off_is_inert(self):
        kt = _KT()
        arm(self, kt, env={**_ENV, "PERSON_MERGE_ENABLED": ""})
        results = run(apply_confirmations([change(6, "誠", {"生死区分": "死亡"})]))
        self.assertEqual(results[0]["status"], "unavailable")
        self.assertEqual(kt.updated, [])

    def test_env_unset_is_inert(self):
        kt = _KT()
        arm(self, kt, env={**_ENV, "APP_KOSEKI_PERSON": ""})
        results = run(apply_confirmations([change(6, "誠", {"生死区分": "死亡"})]))
        self.assertEqual(results[0]["status"], "unavailable")
        self.assertEqual(kt.updated, [])


class TestListCasePersons(unittest.TestCase):
    def test_rows_with_death_hint(self):
        record = {
            "$id": {"value": "6"}, "氏名": {"value": "鈴木誠"},
            "名寄せ確定": {"value": "確定"}, "確認状態": {"value": "未確認"},
            "生死区分": {"value": "死亡"}, "死亡日": {"value": ""},
            "被相続人フラグ": {"value": "no"},
            "身分事項": {"value": [
                {"value": {"事項種別": {"value": "死亡"},
                           "年月日": {"value": "令和7年4月13日"}}},
                {"value": {"事項種別": {"value": "死亡"},
                           "年月日": {"value": "令和7年4月13日"}}},  # 重複行
                {"value": {"事項種別": {"value": "婚姻"},
                           "年月日": {"value": "平成11年7月19日"}}}]}}

        async def search_records(app, query, fields=None):
            self.assertIn('案件レコードID = "4"', query)
            return [record]

        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.search_records", new=search_records):
            rows = run(list_case_persons("4"))
        r = rows[0]
        self.assertEqual((r.record_id, r.name, r.alive), ("6", "鈴木誠", "死亡"))
        self.assertEqual(r.hints, ["死亡記載: 令和7年4月13日"],
                         "推定材料は提示のみ・重複は1件に")

    def test_env_unset_returns_empty(self):
        with patch.dict(os.environ, {**_ENV, "APP_KOSEKI_PERSON": ""}):
            self.assertEqual(run(list_case_persons("4")), [])


if __name__ == "__main__":
    unittest.main()
