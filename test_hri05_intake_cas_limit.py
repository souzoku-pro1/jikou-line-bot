"""HRI-05｜取込経路の 409 再取得の上限（裁定 C 要約: 初回+再試行 1 回・再取得 1 回）

- 相続放棄: houki_case_store.apply_hearing_fields に cas_attempts / cas_refetches を注入
  （取込=2/1。既定 None=ヒアリング経路の _CAS_RETRIES=3・409 のたびに再取得=不変）。
- 時効: hearing_update.apply_update は CAS_REFETCH=1（更新 2 回・再取得 1 回）で既に同じ上限。
- 上限到達=write 0 → 取込は再取得の実値で unwritten と判定 → HRI-02 の「解放」を記録
  （完了記録なし=再配送で再処理）。
"""
import unittest
from unittest.mock import patch

from test_human_reply_intake import (  # noqa: E402
    ADDR, EVT, J11, NAME, USER, _Base, _ai, _run)

from hub import hearing_update  # noqa: E402
from hub import houki_case_store as store  # noqa: E402
from hub import human_reply_intake as hri  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402

RESERVE_H = f"返答取込:houki:{EVT}"
RELEASE_H = f"返答取込解放:houki:{EVT}"
DONE_H = f"返答取込済:houki:{EVT}"


class _CountBase(_Base):
    """App 40 の再取得（fetch_case=LINEユーザーID 検索）と更新の回数を数える。"""

    def houki_updates(self):
        return [c for c in self.fake.update_calls if c[0] == "APP_HOUKI"]

    def houki_refetches(self):
        return [q for env, q in self.fake.search_calls
                if env == "APP_HOUKI" and "LINEユーザーID" in q]

    def jikou_gets(self):
        return self._gets

    def setUp(self):
        super().setUp()
        self._gets = []
        real_get = self.fake.get_record

        async def _get(app, rid):
            if app.app_id_env == "KINTONE_APP_ID":
                self._gets.append(str(rid))
            return await real_get(app, rid)
        p = patch.object(hub_kintone, "get_record", _get)
        p.start()
        self.addCleanup(p.stop)


class TestHoukiIntakeCasLimit(_CountBase):
    def test_continuous_409_stops_after_two_updates_and_one_refetch(self):
        self.fake.add_houki()
        self.fake.conflict_next = 10                              # 連続 409
        self.ai.return_value = _ai({"顧客名": (True, NAME, "high")}, fill=hri.HOUKI.fields)
        self.assertEqual(_run(hri.run_houki(USER, "山田太郎です", EVT)), "no_write")
        self.assertEqual(len(self.houki_updates()), 2)            # 初回+再試行 1 回
        # 再取得: 冒頭の _find_record 1 回+409 後の再取得 1 回（+書込後の検証は get_record）
        self.assertEqual(len(self.houki_refetches()), 2)
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "顧客名"), "")
        # 上限到達=完了記録なし（解放）→ 再配送で再処理
        self.assertEqual(self.fake.markers(), [RESERVE_H, RELEASE_H])
        self.fake.conflict_next = 0
        self.assertEqual(_run(hri.run_houki(USER, "山田太郎です", EVT)), "written")
        self.assertEqual(self.fake.markers(), [RESERVE_H, RELEASE_H, RESERVE_H, DONE_H])
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "顧客名"), NAME)

    def test_single_409_then_success_within_limit(self):
        self.fake.add_houki()
        self.fake.conflict_next = 1
        self.ai.return_value = _ai({"顧客名": (True, NAME, "high")}, fill=hri.HOUKI.fields)
        self.assertEqual(_run(hri.run_houki(USER, "山田太郎です", EVT)), "written")
        self.assertEqual(len(self.houki_updates()), 2)
        self.assertEqual(self.fake.markers(), [RESERVE_H, DONE_H])

    def test_intake_constants_pinned(self):
        self.assertEqual((hri.INTAKE_CAS_ATTEMPTS, hri.INTAKE_CAS_REFETCHES), (2, 1))
        self.assertEqual(store._CAS_RETRIES, 3)                   # ヒアリング経路は不変


class TestHearingPathUnchanged(_CountBase):
    def test_default_apply_hearing_fields_keeps_three_attempts_and_refetches(self):
        rec = self.fake.add_houki()
        self.fake.conflict_next = 10
        rid, problems, choice = _run(store.apply_hearing_fields(
            USER, {"被相続人氏名": "山田一郎"}, rec))
        self.assertEqual((rid, problems, choice), ("50", [], []))
        self.assertEqual(len(self.houki_updates()), 3)            # _CAS_RETRIES=3
        self.assertEqual(len(self.houki_refetches()), 3)          # 409 のたびに再取得（従来どおり）

    def test_injected_limits_apply_only_when_passed(self):
        rec = self.fake.add_houki()
        self.fake.conflict_next = 10
        _run(store.apply_hearing_fields(USER, {"被相続人氏名": "山田一郎"}, rec,
                                        cas_attempts=2, cas_refetches=1))
        self.assertEqual(len(self.houki_updates()), 2)
        self.assertEqual(len(self.houki_refetches()), 1)

    def test_injected_limits_with_create_section(self):
        # existing=None（区間内で再検索してから）でも注入した上限が効く
        self.fake.add_houki()
        self.fake.conflict_next = 10
        _run(store.apply_hearing_fields(USER, {"被相続人氏名": "山田一郎"}, None,
                                        cas_attempts=2, cas_refetches=1))
        self.assertEqual(len(self.houki_updates()), 2)
        self.assertEqual(len(self.houki_refetches()), 1 + 1)      # 区間内の再検索 1+再取得 1


class TestJikouIntakeCasLimit(_CountBase):
    def test_apply_update_is_two_updates_one_refetch(self):
        self.fake.add_jikou(revision="5")
        self.fake.conflict_next = 10
        r = _run(hearing_update.apply_update("10", {"顧客名": NAME}))
        self.assertEqual(r["outcome"], hearing_update.OUTCOME_UNCONVERGED)
        self.assertEqual(len([c for c in self.fake.update_calls if c[0] == "KINTONE_APP_ID"]), 2)
        self.assertEqual(len(self.jikou_gets()), 2)              # 初回取得 1+再取得 1
        self.assertEqual(hearing_update.CAS_REFETCH, 1)

    def test_intake_releases_on_limit(self):
        self.fake.add_jikou(revision="5")
        self.fake.conflict_next = 10
        self.ai.return_value = _ai({"顧客名": (True, NAME, "high")}, fill=J11)
        self.assertEqual(_run(hri.run_jikou(USER, "山田太郎です", EVT)), "no_write")
        self.assertEqual(self.fake.markers(), [f"返答取込:jikou:{EVT}", f"返答取込解放:jikou:{EVT}"])


if __name__ == "__main__":
    unittest.main()
