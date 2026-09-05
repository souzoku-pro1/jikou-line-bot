"""SOUZOKU-HOUKI-H4: 電話推奨度判定+通知の固定。

固定する仕様（正本 10-unit-02 §3・票 SOUZOKU-HOUKI-H4）:
- 危険類型 10 種のルール一次判定（機械的ルール列の各形・安全側裁定込み）
- 推奨度の算出（#1〜#6=強推奨 / #7〜#10のみ=推奨 / なし=不要寄り）
- 社内締切日の最小日数計算（初日算入・応当日前日・応当日なし=月末・
  souzoku-houki/03 §3.3。起算点導出不能=#2 該当の fail-closed）
- Claude 補助は**安全側にのみ**働く: 提案は許可閉集合との積→ルール結果への
  合併（追加）のみ。自由記述が全欄空なら呼び出しゼロ。失敗はルールのみで
  確定+固定マーカー（判定は止めない）
- 発火/冪等: 遷移 CAS 勝者のみが判定（promote 経路）+自己修復発火
  （電話判断待ち×推奨度空）。冪等キー=電話推奨度の非空（永続正本）。
  通知→書込の順の at-least-once（書込失敗=次回再発火・重複は人が閉じる）
- 通知は固定文言+案件レコード番号のみ（顧客名・相談内容は非搭載の PII 規律）。
  「不要寄り」でも必ず通知（自動スキップ経路なし）
- 書込は 危険類型フラグ（追記・人の編集保持・CAS 収束）/電話推奨度/
  電話推奨根拠 のみ。弁護士専権欄（電話要否・受任判断・電話予定日時 等）への
  書込は payload 単位で不存在を pin
"""

import asyncio
import datetime
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

# test_houki_hearing が env setdefault と App 40 フェイクを提供する
from test_houki_hearing import (_FakeApp40, _HearingBase, _StoreBase,
                                _text_response, _tool_response)  # noqa: F401
from houki_bot import hearing  # noqa: E402
from hub import houki_case_store as store  # noqa: E402
from hub import houki_phone_triage as tri  # noqa: E402
from hub import notify as notify_mod  # noqa: E402

TODAY = datetime.date(2026, 8, 30)


def _run(coro):
    return asyncio.run(coro)


def _rec(**values) -> dict:
    """App 40 レコード形（{code: {value: ...}}）の組み立てヘルパ。"""
    rec = {"$id": {"value": "1"}, "$revision": {"value": "1"}}
    for code, v in values.items():
        rec[code] = {"value": v}
    return rec


def _clean_values(**over) -> dict:
    """どのルールにも該当しない基準形（不要寄り）。over で上書き。"""
    base = dict(
        死亡日_申告="2026-08-20",
        死亡を知った日_申告="2026-08-21",
        相続人と知った日_申告="2026-08-21",
        財産処分有無="なし", 訴訟督促有無="なし", 未成年後見関与="なし",
        本人区分="本人", 相続順位="子",
    )
    base.update(over)
    return base


def _flags(**over) -> set:
    flags, _ = tri.compute_rule_flags(_rec(**_clean_values(**over)),
                                      today=TODAY)
    return flags


class TestCleanBaseline(unittest.TestCase):
    def test_clean_record_has_no_flags(self):
        self.assertEqual(_flags(), set())


class TestRule1DeadlineNear(unittest.TestCase):
    def test_remaining_30_flags(self):
        # 起算点 2026-06-30 → 社内締切 2026-09-29 → 残 30 日=該当（≤30）
        f = _flags(死亡日_申告="2026-06-30", 死亡を知った日_申告="2026-06-30",
                   相続人と知った日_申告="2026-06-30")
        self.assertIn(tri.FLAG_DEADLINE_NEAR, f)

    def test_remaining_31_no_flag(self):
        f = _flags(死亡日_申告="2026-07-01", 死亡を知った日_申告="2026-07-01",
                   相続人と知った日_申告="2026-07-01")
        self.assertNotIn(tri.FLAG_DEADLINE_NEAR, f)

    def test_confirmed_start_point_wins(self):
        # 起算点確定済=yes は 起算日_確定 を採用（申告値より遅くても）
        f = _flags(起算点確定済="yes", 起算日_確定="2026-08-25",
                   死亡日_申告="2026-05-01", 死亡を知った日_申告="2026-05-01",
                   相続人と知った日_申告="2026-05-01")
        self.assertNotIn(tri.FLAG_DEADLINE_NEAR, f)


class TestRule2DeadlineDoubt(unittest.TestCase):
    def test_missing_one_date_flags(self):
        f = _flags(相続人と知った日_申告="")
        self.assertIn(tri.FLAG_DEADLINE_DOUBT, f)

    def test_all_dates_missing_fail_closed(self):
        # 起算点導出不能=#1 は数値判定せず #2 該当（票 5. の fail-closed）
        f = _flags(死亡日_申告="", 死亡を知った日_申告="",
                   相続人と知った日_申告="")
        self.assertIn(tri.FLAG_DEADLINE_DOUBT, f)
        self.assertNotIn(tri.FLAG_DEADLINE_NEAR, f)

    def test_complete_dates_no_flag(self):
        self.assertNotIn(tri.FLAG_DEADLINE_DOUBT, _flags())


class TestRule3AssetContact(unittest.TestCase):
    def test_ari_fumei_empty_flag(self):
        for v in ("あり", "不明", ""):
            with self.subTest(v=v):
                self.assertIn(tri.FLAG_ASSET_CONTACT, _flags(財産処分有無=v))

    def test_nashi_no_flag(self):
        self.assertNotIn(tri.FLAG_ASSET_CONTACT, _flags(財産処分有無="なし"))


class TestRule4DeathOver3Months(unittest.TestCase):
    def test_over_flags(self):
        f = _flags(死亡日_申告="2026-05-29")
        self.assertIn(tri.FLAG_DEATH_3MONTHS, f)

    def test_exactly_3_months_no_flag(self):
        # 応当日当日は「3ヶ月超」でない
        f = _flags(死亡日_申告="2026-05-30")
        self.assertNotIn(tri.FLAG_DEATH_3MONTHS, f)


class TestRule5DisputeIsClaudeOnly(unittest.TestCase):
    def test_rule_side_never_flags(self):
        f = _flags(他の相続人="兄と揉めています。連絡が取れません")
        self.assertNotIn(tri.FLAG_DISPUTE, f)


class TestRule6Mismatch(unittest.TestCase):
    def test_existing_flag_inherited(self):
        rec = _rec(**_clean_values())
        rec[store.KIKEN_FLAG_FIELD] = {"value": ["申告内容の矛盾"]}
        flags, _ = tri.compute_rule_flags(rec, today=TODAY)
        self.assertIn(tri.FLAG_MISMATCH, flags)


class TestRule7PriorRenunciation(unittest.TestCase):
    def test_sibling_with_houki_text_flags(self):
        f = _flags(相続順位="兄弟姉妹", 先順位相続人の状況="子は全員放棄済み")
        self.assertIn(tri.FLAG_PRIOR_RENUNCIATION, f)

    def test_child_rank_no_flag(self):
        f = _flags(相続順位="子", 先順位相続人の状況="放棄済み")
        self.assertNotIn(tri.FLAG_PRIOR_RENUNCIATION, f)

    def test_sibling_without_houki_no_flag(self):
        f = _flags(相続順位="兄弟姉妹", 先順位相続人の状況="子はいません")
        self.assertNotIn(tri.FLAG_PRIOR_RENUNCIATION, f)

    def test_houki_joukyou_field_also_checked(self):
        f = _flags(相続順位="直系尊属", 先順位者の放棄状況="全員放棄した")
        self.assertIn(tri.FLAG_PRIOR_RENUNCIATION, f)


class TestRule8NotPrincipal(unittest.TestCase):
    def test_relative_flags(self):
        self.assertIn(tri.FLAG_NOT_PRINCIPAL,
                      _flags(本人区分="親族（本人依頼予定）"))

    def test_empty_flags_safe_side(self):
        self.assertIn(tri.FLAG_NOT_PRINCIPAL, _flags(本人区分=""))

    def test_principal_no_flag(self):
        self.assertNotIn(tri.FLAG_NOT_PRINCIPAL, _flags(本人区分="本人"))


class TestRule9MinorGuardianRemoved(unittest.TestCase):
    """HOUKI-HEARING-UX-1（弁護士決定 C）: #9 未成年・後見関与は判定から撤去。
    旧「あり/不明=該当」の pin は本票の弁護士決定を根拠に「影響なし」の検証へ
    書き換え（削除ではない）。欄・選択肢は残置。"""

    def test_any_value_no_flag_and_no_rationale(self):
        for v in ("あり", "不明", "なし", ""):
            with self.subTest(v=v):
                flags, rationale = tri.compute_rule_flags(
                    _rec(**_clean_values(未成年後見関与=v)), today=TODAY)
                self.assertNotIn(tri.FLAG_MINOR_GUARDIAN, flags)
                self.assertEqual(flags, set())                 # 他ルール不変
                self.assertFalse(any("後見" in r for r in rationale))

    def test_recommendation_unaffected(self):
        # 後見あり/不明のみのケースで推奨度が上がらない（不要寄りのまま）
        for v in ("あり", "不明"):
            with self.subTest(v=v):
                flags, _ = tri.compute_rule_flags(
                    _rec(**_clean_values(未成年後見関与=v)), today=TODAY)
                self.assertEqual(tri.derive_recommendation(flags), "不要寄り")
        # 人が手で立てたフラグも推奨度には寄与しない（判定から完全に外す）
        self.assertEqual(tri.derive_recommendation({tri.FLAG_MINOR_GUARDIAN}),
                         "不要寄り")
        self.assertNotIn(tri.FLAG_MINOR_GUARDIAN, tri.MODERATE_FLAGS)
        self.assertNotIn(tri.FLAG_MINOR_GUARDIAN, tri.STRONG_FLAGS)
        self.assertNotIn(tri.FLAG_MINOR_GUARDIAN, tri.CLAUDE_ASSISTABLE_FLAGS)
        # 欄の選択肢（表示順）は残置
        self.assertIn(tri.FLAG_MINOR_GUARDIAN, tri.FLAG_ORDER)

    def test_other_rules_still_moderate(self):
        self.assertEqual(tri.MODERATE_FLAGS, frozenset({
            tri.FLAG_PRIOR_RENUNCIATION, tri.FLAG_NOT_PRINCIPAL,
            tri.FLAG_LITIGATION}))


class TestRule10Litigation(unittest.TestCase):
    def test_ari_flags(self):
        self.assertIn(tri.FLAG_LITIGATION, _flags(訴訟督促有無="あり"))

    def test_fumei_nashi_no_flag(self):
        # 正本の機械的ルールどおり「あり」のみ（不明の解釈は Claude 補助）
        for v in ("不明", "なし"):
            with self.subTest(v=v):
                self.assertNotIn(tri.FLAG_LITIGATION, _flags(訴訟督促有無=v))


class TestRecommendation(unittest.TestCase):
    def test_none_is_low(self):
        self.assertEqual(tri.derive_recommendation(set()), "不要寄り")

    def test_moderate_only_is_moderate(self):
        self.assertEqual(
            tri.derive_recommendation({tri.FLAG_NOT_PRINCIPAL,
                                       tri.FLAG_LITIGATION}), "推奨")

    def test_any_strong_wins(self):
        for f in sorted(tri.STRONG_FLAGS):
            with self.subTest(flag=f):
                self.assertEqual(
                    tri.derive_recommendation({f, tri.FLAG_NOT_PRINCIPAL}),
                    "強推奨")


class TestDeadlineCalc(unittest.TestCase):
    def test_normal_anniversary_minus_one(self):
        self.assertEqual(tri.shanai_deadline(datetime.date(2026, 5, 20)),
                         datetime.date(2026, 8, 19))

    def test_no_anniversary_month_end(self):
        # 11/30 起算 → 2月に応当日 30 なし → 月末（前日は取らない）
        self.assertEqual(tri.shanai_deadline(datetime.date(2026, 11, 30)),
                         datetime.date(2027, 2, 28))
        self.assertEqual(tri.shanai_deadline(datetime.date(2026, 1, 31)),
                         datetime.date(2026, 4, 30))

    def test_month_anniversary(self):
        self.assertEqual(
            tri._month_anniversary(datetime.date(2026, 11, 30), 3),
            datetime.date(2027, 2, 28))
        self.assertEqual(
            tri._month_anniversary(datetime.date(2026, 5, 20), 3),
            datetime.date(2026, 8, 20))


# ── Claude 補助の安全側動作 ─────────────────────────────────────────────────────
def _assist_response(flags, reasons=()):
    return SimpleNamespace(content=[SimpleNamespace(
        type="tool_use", name="set_phone_recommendation", id="tu-a",
        input={"flags": list(flags), "reasons": list(reasons)})])


class TestClaudeAssist(unittest.TestCase):
    def test_no_free_text_no_call(self):
        model = AsyncMock()
        with patch.object(tri, "_call_assist_model", model):
            flags, rationale, failed = _run(
                tri._claude_assist(_rec(**_clean_values())))
        model.assert_not_awaited()
        self.assertEqual((flags, rationale, failed), (set(), [], False))

    def test_proposals_intersected_with_closed_set(self):
        # 許可閉集合外（#8 は補助対象外・未知語）は破棄＝補助は追加方向の
        # 許可類型のみ（安全側にのみ働く構造）
        model = AsyncMock(return_value=_assist_response(
            [tri.FLAG_DISPUTE, tri.FLAG_NOT_PRINCIPAL, "でたらめ類型"],
            ["兄と揉めている", "x", "y"]))
        rec = _rec(**_clean_values(他の相続人="兄と揉めています"))
        with patch.object(tri, "_call_assist_model", model):
            flags, rationale, failed = _run(tri._claude_assist(rec))
        self.assertEqual(flags, {tri.FLAG_DISPUTE})
        self.assertFalse(failed)
        self.assertTrue(any("Claude補助" in r for r in rationale))

    def test_failure_degrades_without_flags(self):
        model = AsyncMock(side_effect=RuntimeError("boom"))
        rec = _rec(**_clean_values(他の相続人="text"))
        with patch.object(tri, "_call_assist_model", model):
            flags, rationale, failed = _run(tri._claude_assist(rec))
        self.assertEqual((flags, rationale), (set(), []))
        self.assertTrue(failed)

    def test_union_only_adds_never_removes(self):
        # ルールが立てた #10 は補助が空を返しても外れない（合併のみ）
        rec = _rec(**_clean_values(訴訟督促有無="あり",
                                   他の相続人="特に問題ありません"))
        rule_flags, _ = tri.compute_rule_flags(rec, today=TODAY)
        self.assertIn(tri.FLAG_LITIGATION, rule_flags)
        model = AsyncMock(return_value=_assist_response([]))
        with patch.object(tri, "_call_assist_model", model):
            assist_flags, _r, _f = _run(tri._claude_assist(rec))
        self.assertEqual(rule_flags | assist_flags, rule_flags)


# ── run_phone_triage（発火・冪等・通知・書込） ───────────────────────────────────
class _TriageBase(_StoreBase):
    UID = "U_houki_triage"
    PATCH_NOTIFY = True     # fix1: 実 notify を使う派生クラスは False にする

    def setUp(self):
        super().setUp()
        if self.PATCH_NOTIFY:
            self.notify = AsyncMock(return_value=True)
            self._np = patch.object(tri.notify, "notify_admin_line",
                                    self.notify)
            self._np.start()
            self.addCleanup(self._np.stop)
        self.assist = AsyncMock(return_value=_assist_response([]))
        self._ap = patch.object(tri, "_call_assist_model", self.assist)
        self._ap.start()
        self.addCleanup(self._ap.stop)

    def make_case(self, status="電話判断待ち", **over):
        values = _clean_values(**over)
        rid = _run(store.kintone.create_record(None, {
            "LINEユーザーID": self.UID,
            "status": status,
            **values}))
        return str(rid)


class TestRunPhoneTriage(_TriageBase):
    def test_low_case_notifies_and_writes_once(self):
        rid = self.make_case()
        self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[0]
        self.assertIn("【電話推奨度】相続放棄", text)
        self.assertIn("不要寄り", text)          # 不要寄りでも必ず通知
        self.assertIn(f"相続放棄案件レコードNo: {rid}", text)
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "不要寄り")
        self.assertTrue(self.fake.field(rid, "電話推奨根拠"))
        # 冪等: 2 回目は作用 0（冪等キー=電話推奨度の非空・永続正本）
        self.assertFalse(_run(tri.run_phone_triage(self.UID)))
        self.notify.assert_awaited_once()

    def test_strong_case_flags_and_reco(self):
        rid = self.make_case(財産処分有無="不明", 訴訟督促有無="あり")
        self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "強推奨")
        flags = self.fake.field(rid, "危険類型フラグ")
        self.assertIn(tri.FLAG_ASSET_CONTACT, flags)
        self.assertIn(tri.FLAG_LITIGATION, flags)
        self.assertIn("財産処分有無", self.fake.field(rid, "電話推奨根拠"))

    def test_notification_carries_no_customer_fields(self):
        # PII 規律: 通知は固定文言+レコード番号のみ（顧客名・相談内容なし）
        self.make_case(顧客名="山田花子", 被相続人氏名="山田太郎",
                       他の相続人="兄の山田次郎と揉めています")
        self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        text = self.notify.await_args.args[0]
        for pii in ("山田花子", "山田太郎", "山田次郎", "揉めて"):
            self.assertNotIn(pii, text)

    def test_status_gate(self):
        for status in ("問い合わせ", "電話調整中", "受任"):
            with self.subTest(status=status):
                self.fake.rows.clear()
                self.make_case(status=status)
                self.assertFalse(_run(tri.run_phone_triage(self.UID)))
        self.notify.assert_not_awaited()

    def test_already_judged_no_action(self):
        self.make_case(電話推奨度="推奨")
        self.assertFalse(_run(tri.run_phone_triage(self.UID)))
        self.notify.assert_not_awaited()

    def test_assist_failure_marks_and_still_completes(self):
        self.assist.side_effect = RuntimeError("boom")
        rid = self.make_case(他の相続人="自由記述あり")
        self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.assertIn(tri.ASSIST_FAILED_NOTE, self.notify.await_args.args[0])
        self.assertIn(tri.ASSIST_FAILED_NOTE,
                      self.fake.field(rid, "電話推奨根拠"))
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "不要寄り")

    def test_claude_flags_raise_recommendation(self):
        # 補助のみ該当（#5 紛争気配）→ 強推奨 へ引き上げ（安全側の追加方向）
        self.assist.return_value = _assist_response(
            [tri.FLAG_DISPUTE], ["連絡が取れない相続人がいる"])
        rid = self.make_case(他の相続人="兄と連絡が取れません")
        self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "強推奨")
        self.assertIn(tri.FLAG_DISPUTE, self.fake.field(rid, "危険類型フラグ"))

    def test_write_failure_notify_first_then_refire(self):
        # 通知先行の at-least-once: 書込失敗=冪等キー空のまま→再発火で
        # 再通知+書込完遂（未通知の沈黙を作らない・重複は人が閉じる）
        rid = self.make_case()
        real_update = self.fake.update_record

        async def _fail(*a, **k):
            raise store.kintone.KintoneError(500, "GAIA_XX", "down")
        with patch.object(store.kintone, "update_record", _fail):
            self.assertFalse(_run(tri.run_phone_triage(self.UID)))
        self.assertEqual(self.notify.await_count, 1)   # 通知は届いている
        self.assertIsNone(self.fake.field(rid, "電話推奨度"))
        with patch.object(store.kintone, "update_record", real_update):
            self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.assertEqual(self.notify.await_count, 2)
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "不要寄り")

    def test_attorney_only_fields_never_in_payload(self):
        # 書込閉集合: 判定の全書込 payload は 3 フィールドのみ（専権欄不書込）
        self.assist.return_value = _assist_response([tri.FLAG_DISPUTE], ["r"])
        self.make_case(財産処分有無="不明", 他の相続人="揉めています")
        seen: set = set()
        real_update = self.fake.update_record

        async def _spy(app, record_id, fields, revision=None):
            seen.update(fields.keys())
            return await real_update(app, record_id, fields,
                                     revision=revision)
        with patch.object(store.kintone, "update_record", _spy):
            self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.assertTrue(seen.issubset(
            {"危険類型フラグ", "電話推奨度", "電話推奨根拠"}), seen)


class TestCasConvergence(_TriageBase):
    def test_add_flags_preserves_human_edits_on_conflict(self):
        rid = self.make_case()
        stale = _run(store.fetch_case(self.UID))
        # 人が別フラグを先に追加（$revision が進む）
        _run(store.kintone.update_record(
            None, rid, {"危険類型フラグ": ["未成年・後見関与"]}))
        added = _run(store.add_kiken_flags(
            rid, stale, [tri.FLAG_ASSET_CONTACT]))
        self.assertEqual(added, 1)
        self.assertEqual(sorted(self.fake.field(rid, "危険類型フラグ")),
                         sorted(["未成年・後見関与", tri.FLAG_ASSET_CONTACT]))

    def test_set_reco_yields_to_other_winner(self):
        rid = self.make_case()
        stale = _run(store.fetch_case(self.UID))
        _run(store.kintone.update_record(
            None, rid, {"電話推奨度": "推奨", "電話推奨根拠": "先勝ち"}))
        self.assertFalse(_run(store.set_phone_recommendation(
            rid, stale, "強推奨", "後発")))
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "推奨")
        self.assertEqual(self.fake.field(rid, "電話推奨根拠"), "先勝ち")


# ── fix1[H4-01]: 通知失敗時に冪等キーを閉じない（通知 True のときだけ書込） ────────
class TestH4Fix1NotifyGate(_TriageBase):
    def test_notify_false_leaves_key_open_then_refire_completes(self):
        rid = self.make_case()
        self.notify.return_value = False
        self.assertFalse(_run(tri.run_phone_triage(self.UID)))
        self.assertIsNone(self.fake.field(rid, "電話推奨度"))
        self.assertTrue(tri.triage_pending(_run(store.fetch_case(self.UID))))
        # 復旧後の再発火（自己修復経路と同じ入口）で通知→書込まで完遂
        self.notify.return_value = True
        self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.assertEqual(self.notify.await_count, 2)
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "不要寄り")


class TestH4Fix1RealNotifyPaths(_TriageBase):
    """実 notify_admin_line を通す negative（管理者未設定/HTTP失敗/スロットル）。"""
    PATCH_NOTIFY = False
    UID = "U_houki_triage_rn"

    def setUp(self):
        super().setUp()
        notify_mod._last_notify_at.clear()
        self.addCleanup(notify_mod._last_notify_at.clear)

    def test_no_admin_id_leaves_key_open(self):
        rid = self.make_case()
        with patch.object(notify_mod, "get_admin_line_user_id", lambda: ""):
            self.assertFalse(_run(tri.run_phone_triage(self.UID)))
        self.assertIsNone(self.fake.field(rid, "電話推奨度"))
        self.assertTrue(tri.triage_pending(_run(store.fetch_case(self.UID))))

    def test_push_failure_no_stamp_immediate_retry_succeeds(self):
        # HTTP 失敗はスロットル刻印されない（成功時のみ刻印の opt-in）＝
        # 直後の再発火が機械的 False にならず、そのまま成功→書込に到達
        rid = self.make_case()
        with patch.object(notify_mod, "get_admin_line_user_id",
                          lambda: "U_admin"):
            with patch.object(notify_mod, "push_line_message",
                              AsyncMock(return_value=False)):
                self.assertFalse(_run(tri.run_phone_triage(self.UID)))
            self.assertIsNone(self.fake.field(rid, "電話推奨度"))
            push = AsyncMock(return_value=True)
            with patch.object(notify_mod, "push_line_message", push):
                self.assertTrue(_run(tri.run_phone_triage(self.UID)))
            push.assert_awaited_once()      # スロットルに阻まれていない
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "不要寄り")

    def test_throttled_rejection_then_clears_and_completes(self):
        rid = self.make_case()
        key = f"houki_phone_triage:{rid}"
        notify_mod._last_notify_at[key] = time.monotonic()   # 直前成功相当
        with patch.object(notify_mod, "get_admin_line_user_id",
                          lambda: "U_admin"), \
             patch.object(notify_mod, "push_line_message",
                          AsyncMock(return_value=True)):
            self.assertFalse(_run(tri.run_phone_triage(self.UID)))
            self.assertIsNone(self.fake.field(rid, "電話推奨度"))
            self.assertTrue(
                tri.triage_pending(_run(store.fetch_case(self.UID))))
            # スロットル解消後の再発火で通知成功→書込まで到達
            notify_mod._last_notify_at[key] = (
                time.monotonic() - notify_mod._NOTIFY_MIN_INTERVAL_SEC - 1)
            self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "不要寄り")

    def test_default_throttle_behavior_unchanged_for_shared_callers(self):
        # 共用先挙動の pin: 既定（opt-in なし）は従来どおり**試行時に刻印**＝
        # 失敗直後の同一キーはスロットルされる（時効側 caller の挙動不変）
        push = AsyncMock(return_value=False)
        with patch.object(notify_mod, "get_admin_line_user_id",
                          lambda: "U_admin"), \
             patch.object(notify_mod, "push_line_message", push):
            self.assertFalse(_run(notify_mod.notify_admin_line(
                "x", throttle_key="shared_k")))
            self.assertFalse(_run(notify_mod.notify_admin_line(
                "x", throttle_key="shared_k")))
        push.assert_awaited_once()          # 2 回目は throttle で送信試行なし


# ── fix2[H4-fix1-01]: opt-in 経路の送信中予約（同一プロセス内排他） ────────────────
class TestH4Fix2InFlightReservation(unittest.TestCase):
    """throttle_on_success_only=True の並行二重送信防止（送信中予約）。"""
    KEY = "houki_phone_triage:fx2"

    def setUp(self):
        notify_mod._last_notify_at.clear()
        getattr(notify_mod, "_notify_in_flight", set()).clear()
        self.addCleanup(notify_mod._last_notify_at.clear)
        self.addCleanup(
            lambda: getattr(notify_mod, "_notify_in_flight", set()).clear())
        self._admin = patch.object(notify_mod, "get_admin_line_user_id",
                                   lambda: "U_admin")
        self._admin.start()
        self.addCleanup(self._admin.stop)

    def _notify(self, text="x"):
        return notify_mod.notify_admin_line(
            text, throttle_key=self.KEY, throttle_on_success_only=True)

    def test_parallel_same_key_sends_once(self):
        # 未刻印から 2 処理が interleave → push は 1 回だけ・後発は False
        calls = []
        gate = asyncio.Event()

        async def _push(to, text, token_env=None):
            calls.append(text)
            await gate.wait()
            return True

        async def scenario():
            with patch.object(notify_mod, "push_line_message", _push):
                t1 = asyncio.ensure_future(self._notify("first"))
                await asyncio.sleep(0)      # t1 が予約して送信待ちに入る
                t2 = asyncio.ensure_future(self._notify("second"))
                await asyncio.sleep(0)      # t2 に進入を試みさせる（予約済み
                                            # なら送信せず即 False で完了する）
                gate.set()
                return await t1, await t2
        r1, r2 = _run(scenario())
        self.assertTrue(r1)
        self.assertFalse(r2)
        self.assertEqual(calls, ["first"])              # 二重送信なし
        self.assertNotIn(self.KEY, notify_mod._notify_in_flight)
        self.assertIn(self.KEY, notify_mod._last_notify_at)   # 成功刻印は確定

    def test_failure_releases_reservation_then_retry_sends(self):
        # 送信失敗 → 自予約を解除・直後の再試行が送信できる（interval 非占有）
        push = AsyncMock(return_value=False)
        with patch.object(notify_mod, "push_line_message", push):
            self.assertFalse(_run(self._notify()))
        self.assertNotIn(self.KEY, notify_mod._notify_in_flight)
        push2 = AsyncMock(return_value=True)
        with patch.object(notify_mod, "push_line_message", push2):
            self.assertTrue(_run(self._notify()))
        push2.assert_awaited_once()

    def test_success_stamps_then_normal_throttle(self):
        push = AsyncMock(return_value=True)
        with patch.object(notify_mod, "push_line_message", push):
            self.assertTrue(_run(self._notify()))
            self.assertFalse(_run(self._notify()))      # 以後は通常スロットル
        push.assert_awaited_once()
        self.assertNotIn(self.KEY, notify_mod._notify_in_flight)

    def test_failure_release_preserves_success_stamp(self):
        # 解除の安全性: 失敗処理の予約解除は成功刻印（_last_notify_at）に
        # 触れない——interval 経過済みの旧刻印がそのまま残る
        old = time.monotonic() - notify_mod._NOTIFY_MIN_INTERVAL_SEC - 10
        notify_mod._last_notify_at[self.KEY] = old
        with patch.object(notify_mod, "push_line_message",
                          AsyncMock(return_value=False)):
            self.assertFalse(_run(self._notify()))
        self.assertEqual(notify_mod._last_notify_at.get(self.KEY), old)
        self.assertNotIn(self.KEY, notify_mod._notify_in_flight)
        # その後の成功で刻印が更新される（別処理の成功を妨げない）
        with patch.object(notify_mod, "push_line_message",
                          AsyncMock(return_value=True)):
            self.assertTrue(_run(self._notify()))
        self.assertGreater(notify_mod._last_notify_at[self.KEY], old)


# ── fix1[H4-02]: フラグ保存失敗と「write 0 正常」の区別・直列化 ───────────────────
class TestH4Fix1FlagGate(_TriageBase):
    def test_flag_unresolved_blocks_notify_and_write(self):
        rid = self.make_case(財産処分有無="不明")
        with patch.object(store, "add_kiken_flags",
                          AsyncMock(return_value=None)):
            self.assertFalse(_run(tri.run_phone_triage(self.UID)))
        self.notify.assert_not_awaited()        # 通知 0 回
        self.assertIsNone(self.fake.field(rid, "電話推奨度"))
        self.assertTrue(tri.triage_pending(_run(store.fetch_case(self.UID))))
        # 復旧後の再発火で保存→通知→書込まで完遂
        self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.notify.assert_awaited_once()
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "強推奨")

    def test_zero_add_normal_proceeds_to_notify_and_write(self):
        # 追加対象なし（既に全フラグが立っている）＝正常 0 → 通知・書込に進む
        rid = self.make_case()
        self.fake.rows[rid]["危険類型フラグ"] = {"value": ["申告内容の矛盾"]}
        self.assertTrue(_run(tri.run_phone_triage(self.UID)))
        self.notify.assert_awaited_once()
        self.assertEqual(self.fake.field(rid, "電話推奨度"), "強推奨")
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["申告内容の矛盾"])


class TestH4Fix1AddFlagsReturn(_TriageBase):
    def test_add_kiken_flags_unresolved_returns_none(self):
        # CAS 収束不能（409 が _CAS_RETRIES 回続く）→ None（要確認通知は維持）
        rid = self.make_case()
        existing = _run(store.fetch_case(self.UID))

        async def _always_conflict(*a, **k):
            raise store.kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        with patch.object(store.kintone, "update_record", _always_conflict):
            result = _run(store.add_kiken_flags(
                rid, existing, [tri.FLAG_ASSET_CONTACT]))
        self.assertIsNone(result)
        self.notify.assert_awaited_once()       # H-3 確立の要確認通知は不変
        self.assertIn("要確認", self.notify.await_args.args[0])

    def test_add_kiken_flags_nothing_to_add_returns_zero(self):
        rid = self.make_case()
        self.fake.rows[rid]["危険類型フラグ"] = \
            {"value": [tri.FLAG_ASSET_CONTACT]}
        existing = _run(store.fetch_case(self.UID))
        self.assertEqual(
            _run(store.add_kiken_flags(rid, existing,
                                       [tri.FLAG_ASSET_CONTACT])), 0)
        self.notify.assert_not_awaited()


# ── 発火点（hearing 配線） ─────────────────────────────────────────────────────
_REQUIRED_FIELDS_INPUT = {
    "被相続人氏名": "山田太郎", "続柄": "子",
    "死亡日_申告": "2026-08-20", "死亡を知った日_申告": "2026-08-21",
    "相続人と知った日_申告": "2026-08-21", "相続順位": "子",
    "顧客名": "山田花子", "住所": "川口市", "生年月日": "1980-01-01",
    "電話番号": "090-0000-0000",
}


class TestHearingTrigger(_HearingBase):
    def test_promote_winner_fires_triage(self):
        fire = AsyncMock(return_value=True)
        with patch.object(tri, "run_phone_triage", fire):
            self.run_turn(
                [_tool_response({"phase": "6_applicant",
                                 "fields": dict(_REQUIRED_FIELDS_INPUT),
                                 "phase_done": True, "hearing_done": True}),
                 _text_response("ありがとうございました。")])
        fire.assert_awaited_once_with(self.uid)
        rid = list(self.fake.rows)[0]
        self.assertEqual(self.fake.field(rid, "status"), "電話判断待ち")

    def test_no_promote_no_fire(self):
        fire = AsyncMock(return_value=True)
        with patch.object(tri, "run_phone_triage", fire):
            self.run_turn(
                [_tool_response({"phase": "1_deceased",
                                 "fields": {"被相続人氏名": "山田太郎"},
                                 "phase_done": True, "hearing_done": False}),
                 _text_response("続けます。")])
        fire.assert_not_awaited()

    def test_repair_path_fires_when_pending(self):
        # 遷移済み×判定未了（取りこぼし）の自己修復発火
        _run(store.kintone.create_record(None, {
            "LINEユーザーID": self.uid,
            "status": "電話判断待ち"}))
        fire = AsyncMock(return_value=True)
        with patch.object(tri, "run_phone_triage", fire):
            self.run_turn([_text_response("補足です。")],
                          text="ひとつ補足があります")
        fire.assert_awaited_once_with(self.uid)

    def test_repair_path_silent_when_judged(self):
        _run(store.kintone.create_record(None, {
            "LINEユーザーID": self.uid,
            "status": "電話判断待ち",
            "電話推奨度": "推奨"}))
        fire = AsyncMock(return_value=True)
        with patch.object(tri, "run_phone_triage", fire):
            self.run_turn([_text_response("補足です。")],
                          text="ひとつ補足があります")
        fire.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
