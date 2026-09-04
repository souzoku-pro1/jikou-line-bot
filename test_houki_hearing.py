"""SOUZOKU-HOUKI-H3: 相続放棄ヒアリング Bot の固定。

固定する仕様（正本 souzoku-houki/02 §1-2〔有効部分〕+ 10-unit-02 §2）:
- record_hearing の逐次 upsert（App 40=APP_HOUKI/TOKEN_HOUKI）: 新規作成
  （受付チャネル=LINE・status=問い合わせ）・既存は空欄のみ更新（非空を
  上書きしない）・許可フィールド閉集合外（弁護士専権/サーバ計算欄）は
  構造的に書けない。
- 日付整合検証: 死亡日と知った日の順序・未来日・形式。矛盾時は日付 3 欄を
  書かず tool_result で聞き直し・2 回失敗で危険類型フラグ「申告内容の矛盾」
  （App 40 の実選択肢値）+承認キュー（App 29 共用）。
- status 遷移の入口: 必須項目充足+hearing_done で 問い合わせ（/空）→
  電話判断待ち の一方向のみ（電話推奨度判定・通知は H-4）。
- 送信ゲート: 第 2 世代ガード機構共用（サニタイズ・300 字/質問数・名乗り/
  記号/無根拠語）+route=houki_hearing（根拠集合空=FAQ 根拠語で降格）。
  違反は App 29+確認中定型。送信は HOUKI_CHANNEL 限定。
- 停止リスト（App 39 共用）・全業務ブレーキ（AUTOREPLY_PAUSED 共用）。
- HOUKI_PROFILE: ヒアリング部分のみ実値・顧客対応部分は fail-closed
  プレースホルダ（auto_send_categories=空集合）。

fix1（R-SOUZOKU-HOUKI-H3・3 件 HIGH）:
- [01] 日付整合の cross-turn 化: 検証は「App 40 既存レコードの日付 3 欄+
  今回入力の postimage 候補」に対して行い、既存×今回の全組合せに 3 順序規則を
  適用。矛盾時は今回の日付 3 欄を write 0。
- [02] status 遷移の CAS 化: $revision 楽観ロック。409=作用 0・自動再遷移
  しない（TOCTOU: 読取後の弁護士変更「受任」を上書きしない）。
- [03] 失敗状態の永続化+承認キュー冪等化: in-memory カウンタ廃止。
  1 回目=日付申告メモの固定マーカー【日付整合エラー検知】（App 40 正本・
  再起動を跨いで持続）／2 回目=承認キュー→危険類型フラグ（queue 先行の
  at-least-once）／フラグ済み=増分 0。

fix2（R-SOUZOKU-HOUKI-H3-2・H3-04/H3-05 HIGH）: App 40 書込の全面 CAS 化。
- [H3-04] upsert=$revision CAS+apply_hearing_fields の 409 収束（最新再取得→
  split_valid_fields(最新) 再実行→再試行≤3。最新合成で矛盾=日付 write 0+
  不一致処理。矛盾 postimage は成立しない）。
- [H3-05] メモ/フラグの read-modify-write=$revision CAS。409 収束: 既存在=
  write 0／未存在=最新値（人の追記・人の別フラグ）を保持して追加／収束不能=
  上書きせず要確認通知（notify_admin_line・固定文言）。

fix3（R-SOUZOKU-HOUKI-H3-3・H3-06/H3-07 HIGH）:
- [H3-06] 二重 create 防止=方式(a): App 40 の LINEユーザーID 欄に kintone の
  「値の重複を禁止する」を有効化（大野の点火作業・fake は一意制約を模す）。
  create 失敗（KintoneConflict 以外の KintoneError）→ 再検索 → 既存レコードへ
  収束（split 再検証込み）。既存なし=重複起因でない障害=従来どおり送出。
- [H3-07] append_creditors=fix2 同型の $revision CAS 収束（再取得・再併合≤3・
  既存行保持+同名スキップ維持・収束不能=上書きせず要確認+0）。
"""

import asyncio
import datetime
import os
import unittest
from types import SimpleNamespace
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
from houki_bot import hearing  # noqa: E402
from hub import houki_case_store as store  # noqa: E402
from hub import houki_profile as hp  # noqa: E402
from hub.line_channel import HOUKI_CHANNEL  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


# ── App 40 の in-memory フェイク（hub.kintone の使用 3 API を模す） ─────────────
class _FakeApp40:
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self._id = 0

    @staticmethod
    def _reject_double_wrap(fields):
        # HOUKI-STORE-FIX1 シーム: hub.kintone.create/update_record の契約は
        # plain 値（kintone 側 _wrap が {code:{"value":v}} へ包む）。呼び出し側
        # が包んだ {"value":…} 形が来たら二重ラップ＝実 kintone は CB_IJ01 で
        # 書込全体を拒否するため、fake でも即 fail させる
        for code, v in (fields or {}).items():
            if isinstance(v, dict) and "value" in v:
                raise AssertionError(
                    f"double-wrapped payload: {code}={v!r}"
                    "（hub.kintone へは plain 値を渡す契約・_wrap が包む）")

    async def search_records(self, app, query, fields=None):
        uid = query.split('"')[1]
        found = [r for r in self.rows.values()
                 if r.get("LINEユーザーID", {}).get("value") == uid]
        found.sort(key=lambda r: -int(r["$id"]["value"]))
        return found[:1]

    async def create_record(self, app, fields):
        self._reject_double_wrap(fields)
        # fix3[H3-06]: App 40 の LINEユーザーID 欄は「値の重複を禁止する」
        # 設定（方式(a)・大野の点火作業）＝一意制約違反を模す
        uid = fields.get("LINEユーザーID")
        if uid and any(r.get("LINEユーザーID", {}).get("value") == uid
                       for r in self.rows.values()):
            raise store.kintone.KintoneError(400, "CB_VA01",
                                             "unique constraint")
        self._id += 1
        rid = str(self._id)
        rec = {k: {"value": v} for k, v in fields.items()}   # 実 API の _wrap
        rec["$id"] = {"value": rid}
        rec["$revision"] = {"value": "1"}
        self.rows[rid] = rec
        return rid

    async def update_record(self, app, record_id, fields, revision=None):
        self._reject_double_wrap(fields)
        rec = self.rows[str(record_id)]
        cur = int(rec["$revision"]["value"])
        if revision is not None and int(revision) != cur:
            raise store.kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        rec.update({k: {"value": v} for k, v in fields.items()})   # _wrap
        rec["$revision"] = {"value": str(cur + 1)}

    async def get_record(self, app, record_id):
        rec = self.rows.get(str(record_id))
        if rec is None:
            raise store.kintone.KintoneError(404, "GAIA_RE01", "not found")
        return rec

    def field(self, rid, code):
        return (self.rows[str(rid)].get(code) or {}).get("value")


def _patch_store(fake):
    return (patch.object(store.kintone, "search_records", fake.search_records),
            patch.object(store.kintone, "create_record", fake.create_record),
            patch.object(store.kintone, "update_record", fake.update_record),
            patch.object(store.kintone, "get_record", fake.get_record))


class _StoreBase(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeApp40()
        self._patches = _patch_store(self.fake)
        for p in self._patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self._patches])


class TestDateValidation(unittest.TestCase):
    TODAY = datetime.date(2026, 8, 26)

    def _v(self, fields):
        return store.validate_hearing_dates(fields, today=self.TODAY)

    def test_ok_and_partial(self):
        self.assertEqual(self._v({}), [])
        self.assertEqual(self._v({"死亡日_申告": "2026-05-01"}), [])
        self.assertEqual(self._v({"死亡日_申告": "2026-05-01",
                                  "死亡を知った日_申告": "2026-05-01",
                                  "相続人と知った日_申告": "2026-06-10"}), [])

    def test_knew_death_before_death(self):
        v = self._v({"死亡日_申告": "2026-05-02",
                     "死亡を知った日_申告": "2026-05-01"})
        self.assertTrue(any("死亡を知った日_申告が死亡日_申告より前" in x
                            for x in v))

    def test_knew_heir_before_death_and_before_knew_death(self):
        v = self._v({"死亡日_申告": "2026-05-02",
                     "相続人と知った日_申告": "2026-05-01"})
        self.assertTrue(any("相続人と知った日_申告が死亡日_申告より前" in x
                            for x in v))
        v = self._v({"死亡を知った日_申告": "2026-06-01",
                     "相続人と知った日_申告": "2026-05-20"})
        self.assertTrue(any("死亡を知った日_申告より前" in x for x in v))

    def test_future_and_format(self):
        v = self._v({"死亡日_申告": "2026-09-01"})
        self.assertTrue(any("未来日" in x for x in v))
        v = self._v({"死亡日_申告": "2026-05頃"})
        self.assertTrue(any("形式不正" in x for x in v))


class TestSplitValidFields(unittest.TestCase):
    def test_closed_set_and_empty_dropped(self):
        fields, problems, _choice = store.split_valid_fields({
            "顧客名": "山田太郎",
            "住所": "  ",                       # 空→落ちる
            "起算日_確定": "2026-05-01",        # 弁護士専権→落ちる
            "status": "受任",                   # サーバ/弁護士管理→落ちる
            "法定満了日": "2026-08-01",         # サーバ計算→落ちる
        })
        self.assertEqual(problems, [])
        self.assertEqual(fields, {"顧客名": "山田太郎"})

    def test_date_trio_dropped_on_problem(self):
        fields, problems, _choice = store.split_valid_fields({
            "死亡日_申告": "2026-05-02",
            "死亡を知った日_申告": "2026-05-01",
            "顧客名": "山田太郎",
        }, today=datetime.date(2026, 8, 26))
        self.assertTrue(problems)
        self.assertEqual(fields, {"顧客名": "山田太郎"})   # 日付は両方落ちる

    def test_attorney_only_fields_not_writable(self):
        # 弁護士専権・サーバ計算欄が閉集合に**含まれない**ことを個別 pin
        for code in ("起算日_確定", "起算点確定済", "起算点メモ", "受任判断",
                     "電話要否", "電話推奨度", "法定満了日", "社内締切日",
                     "提出目標日", "残日数", "熟慮期間期限", "status",
                     "単純承認事由フラグ", "本人確認ステータス"):
            self.assertNotIn(code, store.HEARING_WRITABLE_FIELDS)


class TestChoiceClosedSets(unittest.TestCase):
    """HEARING-FIX1: DROP_DOWN 閉集合の pin（App 40 form fields API 実測の逐語）。"""

    def test_choice_sets_pinned(self):
        self.assertEqual(store.HEARING_CHOICE_FIELDS["続柄"], (
            "子", "孫", "配偶者", "直系尊属（父母・祖父母）", "兄弟姉妹",
            "おいめい", "その他"))
        self.assertEqual(store.HEARING_CHOICE_FIELDS["本人区分"],
                         ("本人", "親族（本人依頼予定）", "その他"))
        self.assertEqual(store.HEARING_CHOICE_FIELDS["相続順位"], (
            "配偶者", "子", "直系尊属", "兄弟姉妹", "甥姪（代襲）", "不明"))
        for code in ("未成年後見関与", "財産処分有無", "訴訟督促有無"):
            self.assertEqual(store.HEARING_CHOICE_FIELDS[code],
                             ("なし", "あり", "不明"))
        self.assertEqual(store.HEARING_CHOICE_FIELDS["同時申述希望"],
                         ("なし", "あり"))
        # 書込対象外だが実測 pin（将来の書込対象化票で使う・ガードは書込面のみ）
        self.assertEqual(store.APP40_CHOICE_REFERENCE["知った日の区分"], (
            "被相続人死亡の当日", "死亡の通知をうけた日",
            "先順位者の相続放棄を知った日", "その他"))
        self.assertEqual(store.APP40_CHOICE_REFERENCE["放棄の理由"], (
            "被相続人から生前に贈与を受けている。", "生活が安定している。",
            "遺産が少ない。", "遺産を分散させたくない。", "債務超過のため。",
            "その他"))

    def test_shokugyou_removed_from_writable(self):
        # 止血: App 40 に「職業」実欄が存在しない（実測）ため書込集合から除外
        self.assertNotIn("職業", store.HEARING_WRITABLE_FIELDS)

    def test_split_drops_out_of_set_choice_without_date_side_effects(self):
        out, problems, choice = store.split_valid_fields(
            {"続柄": "母", "被相続人氏名": "山田太郎",
             "死亡日_申告": "2026-05-01"},
            today=datetime.date(2026, 8, 26))
        self.assertEqual(problems, [])                    # 日付検証は不干渉
        self.assertIn("死亡日_申告", out)                  # 日付は落ちない
        self.assertEqual(out.get("被相続人氏名"), "山田太郎")
        self.assertNotIn("続柄", out)                     # 選択肢外=write 0
        self.assertEqual(len(choice), 1)
        self.assertIn("続柄=選択肢外", choice[0])
        self.assertIn("子/孫/配偶者", choice[0])           # 許容値を固定語彙で提示

    def test_split_keeps_in_set_choice(self):
        out, problems, choice = store.split_valid_fields({"続柄": "子"})
        self.assertEqual((problems, choice), ([], []))
        self.assertEqual(out, {"続柄": "子"})

    def test_tool_description_lists_choice_values(self):
        desc = hp.RECORD_HEARING_TOOL["input_schema"]["properties"][
            "fields"]["description"]
        self.assertIn("続柄", desc)
        self.assertIn("子/孫/配偶者/直系尊属（父母・祖父母）/兄弟姉妹/おいめい/その他",
                      desc)
        self.assertIn("本人/親族（本人依頼予定）/その他", desc)


class TestUpsert(_StoreBase):
    def test_create_sets_channel_and_status(self):
        rid = _run(store.upsert_case_fields("U_h1", {"顧客名": "山田"}, None))
        self.assertEqual(self.fake.field(rid, "受付チャネル"), "LINE")
        self.assertEqual(self.fake.field(rid, "status"), "問い合わせ")
        self.assertEqual(self.fake.field(rid, "LINEユーザーID"), "U_h1")
        self.assertEqual(self.fake.field(rid, "顧客名"), "山田")

    def test_update_only_empty_fields(self):
        rid = _run(store.upsert_case_fields(
            "U_h2", {"顧客名": "山田", "住所": "川口市"}, None))
        existing = self.fake.rows[rid]
        _run(store.upsert_case_fields(
            "U_h2", {"顧客名": "別名で上書きしようとする", "電話番号": "090"},
            existing))
        self.assertEqual(self.fake.field(rid, "顧客名"), "山田")   # 非空は不変
        self.assertEqual(self.fake.field(rid, "電話番号"), "090")  # 空欄は埋まる

    def test_append_creditors_dedup(self):
        rid = _run(store.upsert_case_fields("U_h3", {}, None))
        added = _run(store.append_creditors(rid, self.fake.rows[rid],
                                            ["A社", "B社", "A社", ""]))
        self.assertEqual(added, 2)
        added = _run(store.append_creditors(rid, self.fake.rows[rid],
                                            ["B社", "C社"]))
        self.assertEqual(added, 1)
        rows = self.fake.field(rid, "債権者一覧")
        names = [r["value"]["債権者名"]["value"] for r in rows]
        self.assertEqual(names, ["A社", "B社", "C社"])
        self.assertEqual(rows[0]["value"]["通知要否"]["value"], "未確認")

    def test_mark_date_mismatch_flag_idempotent(self):
        rid = _run(store.upsert_case_fields("U_h4", {}, None))
        self.assertTrue(_run(store.mark_date_mismatch_flag(
            rid, self.fake.rows[rid])))
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["申告内容の矛盾"])
        self.assertFalse(_run(store.mark_date_mismatch_flag(
            rid, self.fake.rows[rid])))
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["申告内容の矛盾"])

    def test_status_promotion_one_way(self):
        rid = _run(store.upsert_case_fields("U_h5", {}, None))
        self.assertTrue(_run(store.promote_status_to_phone_triage(
            rid, self.fake.rows[rid])))
        self.assertEqual(self.fake.field(rid, "status"), "電話判断待ち")
        # 受任 等の他 status からは絶対に動かさない
        self.fake.rows[rid]["status"] = {"value": "受任"}
        self.assertFalse(_run(store.promote_status_to_phone_triage(
            rid, self.fake.rows[rid])))
        self.assertEqual(self.fake.field(rid, "status"), "受任")

    def test_status_promotion_cas_toctou(self):
        # fix1[02]（Codex 指定）: 「問い合わせ」取得後に弁護士が「受任」へ変更
        # → Bot の昇格は 409=作用 0・自動再遷移なし・最終 status は「受任」のまま
        import copy
        rid = _run(store.upsert_case_fields("U_h6", {}, None))
        stale = copy.deepcopy(self.fake.rows[rid])   # Bot が読んだ時点の姿
        self.assertEqual(stale["status"]["value"], "問い合わせ")
        # 弁護士が先に変更（revision が進む）
        _run(self.fake.update_record(None, rid, {"status": "受任"}))
        # Bot の昇格試行（stale で CAS）→ 敗北・作用 0
        self.assertFalse(_run(store.promote_status_to_phone_triage(rid, stale)))
        self.assertEqual(self.fake.field(rid, "status"), "受任")
        # 最新を取り直しても自動で再遷移しない（受任は遷移元でない）
        self.assertFalse(_run(store.promote_status_to_phone_triage(
            rid, self.fake.rows[rid])))
        self.assertEqual(self.fake.field(rid, "status"), "受任")

    def test_mismatch_marker_idempotent(self):
        rid = _run(store.upsert_case_fields("U_h7", {"日付申告メモ": "5月頃"},
                                            None))
        self.assertTrue(_run(store.add_mismatch_marker(
            rid, self.fake.rows[rid])))
        self.assertFalse(_run(store.add_mismatch_marker(
            rid, self.fake.rows[rid])))
        memo = self.fake.field(rid, "日付申告メモ")
        self.assertEqual(memo, "5月頃\n" + store.MISMATCH_MARKER)
        self.assertTrue(store.has_mismatch_marker(self.fake.rows[rid]))


class TestFix2CASConvergence(_StoreBase):
    """fix2[H3-04/05]: App 40 書込の CAS 化と 409 収束の各分岐。"""

    def test_h3_04_concurrent_dates_no_contradictory_postimage(self):
        # Codex 指定形: 両者が日付 3 欄空の同一 revision snapshot を取得
        import copy
        rid = _run(store.upsert_case_fields("U_cas1", {}, None))
        snap_a = copy.deepcopy(self.fake.rows[rid])
        snap_b = copy.deepcopy(self.fake.rows[rid])
        # A: 死亡日を書込（rev が進む）
        rid_a, prob_a, _ca = _run(store.apply_hearing_fields(
            "U_cas1", {"死亡日_申告": "2026-05-02"}, snap_a))
        self.assertEqual(prob_a, [])
        self.assertEqual(self.fake.field(rid, "死亡日_申告"), "2026-05-02")
        # B: 旧 snapshot 前提で「それより前の死亡を知った日」→ CAS 敗北→
        #    再取得・再検証で矛盾検出→日付 write 0+不一致処理（problems 返却）
        rid_b, prob_b, _cb = _run(store.apply_hearing_fields(
            "U_cas1", {"死亡を知った日_申告": "2026-05-01"}, snap_b))
        self.assertTrue(any("死亡を知った日_申告が死亡日_申告より前" in x
                            for x in prob_b), prob_b)
        self.assertIsNone(self.fake.field(rid, "死亡を知った日_申告"))
        self.assertEqual(self.fake.field(rid, "死亡日_申告"), "2026-05-02")
        # 最終状態に矛盾 postimage は成立しない

    def test_h3_04_benign_conflict_converges(self):
        # 競合はあるが矛盾しない書込は収束して両方残る
        import copy
        rid = _run(store.upsert_case_fields("U_cas2", {}, None))
        snap_b = copy.deepcopy(self.fake.rows[rid])
        _run(store.apply_hearing_fields(
            "U_cas2", {"死亡日_申告": "2026-05-02"}, self.fake.rows[rid]))
        _rid, prob, _c = _run(store.apply_hearing_fields(
            "U_cas2", {"顧客名": "山田"}, snap_b))
        self.assertEqual(prob, [])
        self.assertEqual(self.fake.field(rid, "死亡日_申告"), "2026-05-02")
        self.assertEqual(self.fake.field(rid, "顧客名"), "山田")

    def test_h3_05_marker_preserves_human_memo(self):
        # Codex 指定: 取得後に人がメモ追記→旧 snapshot 書き戻しで消えない
        import copy
        rid = _run(store.upsert_case_fields(
            "U_cas3", {"日付申告メモ": "5月頃"}, None))
        stale = copy.deepcopy(self.fake.rows[rid])
        _run(self.fake.update_record(None, rid,
                                     {"日付申告メモ": "5月頃\n人の追記"}))
        self.assertTrue(_run(store.add_mismatch_marker(rid, stale)))
        memo = self.fake.field(rid, "日付申告メモ")
        self.assertEqual(memo, "5月頃\n人の追記\n" + store.MISMATCH_MARKER)

    def test_h3_05_flag_preserves_human_flags(self):
        # Codex 指定: 人が別フラグ追加→保持したうえで「申告内容の矛盾」が加わる
        import copy
        rid = _run(store.upsert_case_fields("U_cas4", {}, None))
        stale = copy.deepcopy(self.fake.rows[rid])
        _run(self.fake.update_record(
            None, rid, {"危険類型フラグ": ["訴訟・督促あり"]}))
        self.assertTrue(_run(store.mark_date_mismatch_flag(rid, stale)))
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["訴訟・督促あり", "申告内容の矛盾"])

    def test_h3_05_already_present_after_conflict_write_zero(self):
        # Codex 指定: 再取得したらマーカー/フラグ既存在= write 0（False）
        import copy
        rid = _run(store.upsert_case_fields("U_cas5", {}, None))
        stale = copy.deepcopy(self.fake.rows[rid])
        _run(self.fake.update_record(
            None, rid,
            {"日付申告メモ": store.MISMATCH_MARKER,
             "危険類型フラグ": ["申告内容の矛盾"]}))
        rev_after = self.fake.rows[rid]["$revision"]["value"]
        self.assertFalse(_run(store.add_mismatch_marker(rid, stale)))
        self.assertFalse(_run(store.mark_date_mismatch_flag(rid, stale)))
        # write 0（revision が進んでいない）
        self.assertEqual(self.fake.rows[rid]["$revision"]["value"], rev_after)

    def test_h3_05_unresolvable_alerts_without_write(self):
        # Codex 指定: 再取得・再照合不能=上書きせず要確認通知
        rid = _run(store.upsert_case_fields("U_cas6", {}, None))
        stale = dict(self.fake.rows[rid])
        alert = AsyncMock(return_value=True)

        async def always_conflict(app, record_id, fields, revision=None):
            raise store.kintone.KintoneConflict(409, "GAIA_CO02", "conflict")

        async def refetch_fail(app, record_id):
            raise store.kintone.KintoneError(520, "X", "down")

        with patch.object(store.kintone, "update_record", always_conflict), \
             patch.object(store.kintone, "get_record", refetch_fail), \
             patch.object(store.notify, "notify_admin_line", alert):
            self.assertFalse(_run(store.add_mismatch_marker(rid, stale)))
        alert.assert_awaited_once()
        self.assertIn("要確認", alert.await_args.args[0])
        self.assertIn(rid, alert.await_args.args[0])
        self.assertIsNone(self.fake.field(rid, "日付申告メモ"))   # 上書きなし
        # フラグ側も同様（収束不能=通知+write 0）
        alert2 = AsyncMock(return_value=True)
        with patch.object(store.kintone, "update_record", always_conflict), \
             patch.object(store.kintone, "get_record", refetch_fail), \
             patch.object(store.notify, "notify_admin_line", alert2):
            self.assertFalse(_run(store.mark_date_mismatch_flag(rid, stale)))
        alert2.assert_awaited_once()
        self.assertEqual(self.fake.field(rid, "危険類型フラグ") or [], [])


class TestFix3DoubleCreateAndCreditors(_StoreBase):
    """fix3[H3-06/07]: 二重 create の収束・債権者一覧 SUBTABLE の CAS 収束。"""

    def test_h3_06_concurrent_creates_converge_to_one_record(self):
        # Codex 指定形: 並行 2 タスクが双方 existing=None を取得した状態から、
        # create 合計 1 件・両タスクの入力が同一レコードへ収束
        rid_a, prob_a, _ca = _run(store.apply_hearing_fields(
            "U_dc1", {"顧客名": "山田"}, None))
        rid_b, prob_b, _cb = _run(store.apply_hearing_fields(
            "U_dc1", {"電話番号": "090"}, None))     # B も existing=None
        self.assertEqual(len(self.fake.rows), 1)     # create は合計 1 件
        self.assertEqual(rid_a, rid_b)
        self.assertEqual(prob_a, [])
        self.assertEqual(prob_b, [])
        self.assertEqual(self.fake.field(rid_a, "顧客名"), "山田")
        self.assertEqual(self.fake.field(rid_a, "電話番号"), "090")
        self.assertEqual(self.fake.field(rid_a, "status"), "問い合わせ")

    def test_h3_06_converged_write_is_revalidated(self):
        # 収束後の書込にも cross-turn 検証が効く（勝者の死亡日と矛盾する
        # 敗者の日付は write 0+problems）
        _run(store.apply_hearing_fields(
            "U_dc2", {"死亡日_申告": "2026-05-02"}, None))
        rid_b, prob_b, _cb = _run(store.apply_hearing_fields(
            "U_dc2", {"死亡を知った日_申告": "2026-05-01"}, None))
        self.assertEqual(len(self.fake.rows), 1)
        self.assertTrue(any("死亡を知った日_申告が死亡日_申告より前" in x
                            for x in prob_b), prob_b)
        self.assertIsNone(self.fake.field(rid_b, "死亡を知った日_申告"))

    def test_h3_06_non_duplicate_create_error_reraised(self):
        # 再検索しても既存なし=重複起因でない create 障害は従来どおり送出
        async def broken_create(app, fields):
            raise store.kintone.KintoneError(500, "GAIA_XX", "down")

        with patch.object(store.kintone, "create_record", broken_create):
            with self.assertRaises(store.kintone.KintoneError):
                _run(store.apply_hearing_fields("U_dc3", {"顧客名": "x"},
                                                None))
        self.assertEqual(len(self.fake.rows), 0)

    def test_h3_07_concurrent_appends_no_lost_update(self):
        # Codex 指定形: 並行 2 処理が別々の債権者 X・Y を追加→最終表に
        # X・Y が各 1 回ずつ残る（lost update なし）
        import copy
        rid = _run(store.upsert_case_fields("U_cr1", {}, None))
        snap_a = copy.deepcopy(self.fake.rows[rid])
        snap_b = copy.deepcopy(self.fake.rows[rid])
        self.assertEqual(_run(store.append_creditors(rid, snap_a, ["X社"])), 1)
        self.assertEqual(_run(store.append_creditors(rid, snap_b, ["Y社"])), 1)
        names = [r["value"]["債権者名"]["value"]
                 for r in self.fake.field(rid, "債権者一覧")]
        self.assertEqual(sorted(names), ["X社", "Y社"])
        self.assertEqual(len(names), 2)     # 各 1 回・重複なし

    def test_h3_07_same_name_after_conflict_write_zero(self):
        # 競合再取得後に同名が既に存在=同名スキップ（write 0・revision 不変）
        import copy
        rid = _run(store.upsert_case_fields("U_cr2", {}, None))
        stale = copy.deepcopy(self.fake.rows[rid])
        _run(store.append_creditors(rid, self.fake.rows[rid], ["X社"]))
        rev_after = self.fake.rows[rid]["$revision"]["value"]
        self.assertEqual(_run(store.append_creditors(rid, stale, ["X社"])), 0)
        self.assertEqual(self.fake.rows[rid]["$revision"]["value"], rev_after)
        names = [r["value"]["債権者名"]["value"]
                 for r in self.fake.field(rid, "債権者一覧")]
        self.assertEqual(names, ["X社"])

    def test_h3_07_unresolvable_alerts_without_write(self):
        # 収束不能=既存表を上書きせず要確認通知+0
        rid = _run(store.upsert_case_fields("U_cr3", {}, None))
        _run(store.append_creditors(rid, self.fake.rows[rid], ["X社"]))
        stale = dict(self.fake.rows[rid])
        alert = AsyncMock(return_value=True)

        async def always_conflict(app, record_id, fields, revision=None):
            raise store.kintone.KintoneConflict(409, "GAIA_CO02", "conflict")

        async def refetch_fail(app, record_id):
            raise store.kintone.KintoneError(520, "X", "down")

        with patch.object(store.kintone, "update_record", always_conflict), \
             patch.object(store.kintone, "get_record", refetch_fail), \
             patch.object(store.notify, "notify_admin_line", alert):
            self.assertEqual(_run(store.append_creditors(rid, stale,
                                                         ["Y社"])), 0)
        alert.assert_awaited_once()
        self.assertIn("債権者一覧", alert.await_args.args[0])
        names = [r["value"]["債権者名"]["value"]
                 for r in self.fake.field(rid, "債権者一覧")]
        self.assertEqual(names, ["X社"])    # 既存表は上書きされていない


class TestPlainPayloadShapes(_StoreBase):
    """HOUKI-STORE-FIX1: hub.kintone へ渡す書込 payload の実形 pin（型別）。

    契約=呼び出し側は plain 値（kintone._wrap が {code:{"value":v}} へ包む）:
    - 文字列/DATE = plain str（DATE は "YYYY-MM-DD"）
    - CHECK_BOX = plain list[str]
    - SUBTABLE = plain の行 list（行内は {"value": {subcode: {"value": v}}} の
      kintone 行構造そのまま・_wrap はフィールド最上位のみ包む）
    """

    def _spy_updates(self):
        calls = []
        real = self.fake.update_record

        async def _spy(app, rid, fields, revision=None):
            calls.append(fields)
            return await real(app, rid, fields, revision=revision)
        return calls, patch.object(store.kintone, "update_record", _spy)

    def test_create_payload_plain_str_and_date(self):
        seen = {}
        real = self.fake.create_record

        async def _spy(app, fields):
            seen.update(fields)
            return await real(app, fields)
        with patch.object(store.kintone, "create_record", _spy):
            _run(store.upsert_case_fields(
                "U_shape1", {"顧客名": "山田", "死亡日_申告": "2026-05-01"},
                None))
        self.assertEqual(seen["顧客名"], "山田")
        self.assertEqual(seen["死亡日_申告"], "2026-05-01")
        self.assertEqual(seen["LINEユーザーID"], "U_shape1")
        self.assertEqual(seen["受付チャネル"], "LINE")
        self.assertEqual(seen["status"], "問い合わせ")

    def test_update_and_status_payload_plain_str(self):
        rid = _run(store.upsert_case_fields("U_shape2", {}, None))
        calls, p = self._spy_updates()
        with p:
            _run(store.upsert_case_fields(
                "U_shape2", {"電話番号": "090-0000-0000"},
                self.fake.rows[rid]))
            _run(store.promote_status_to_phone_triage(
                rid, self.fake.rows[rid]))
            _run(store.set_phone_recommendation(
                rid, self.fake.rows[rid], "強推奨", "根拠"))
        self.assertEqual(calls[0], {"電話番号": "090-0000-0000"})
        self.assertEqual(calls[1], {"status": "電話判断待ち"})
        self.assertEqual(calls[2], {"電話推奨度": "強推奨",
                                    "電話推奨根拠": "根拠"})

    def test_checkbox_payload_plain_list(self):
        rid = _run(store.upsert_case_fields("U_shape3", {}, None))
        calls, p = self._spy_updates()
        with p:
            _run(store.mark_date_mismatch_flag(rid, self.fake.rows[rid]))
            _run(store.add_kiken_flags(rid, self.fake.rows[rid],
                                       ["訴訟・督促あり"]))
        self.assertEqual(calls[0], {"危険類型フラグ": ["申告内容の矛盾"]})
        self.assertEqual(calls[1],
                         {"危険類型フラグ": ["申告内容の矛盾",
                                             "訴訟・督促あり"]})

    def test_subtable_payload_plain_rows(self):
        rid = _run(store.upsert_case_fields("U_shape4", {}, None))
        calls, p = self._spy_updates()
        with p:
            _run(store.append_creditors(rid, self.fake.rows[rid], ["A社"]))
        rows = calls[-1]["債権者一覧"]
        self.assertIsInstance(rows, list)
        self.assertEqual(rows, [{"value": {"債権者名": {"value": "A社"},
                                           "通知要否": {"value": "未確認"}}}])


class TestCrossTurnDateRules(_StoreBase):
    """fix1[01]: 既存レコード×今回入力の cross-turn 矛盾（3 順序規則+逆方向）。"""

    def _existing_with(self, **dates):
        rid = _run(store.upsert_case_fields(
            "U_ct", {k: v for k, v in dates.items()}, None))
        return self.fake.rows[rid]

    def test_rule1_existing_death_incoming_knew_death(self):
        existing = self._existing_with(死亡日_申告="2026-05-02")
        out, problems, _choice = store.split_valid_fields(
            {"死亡を知った日_申告": "2026-05-01"}, existing,
            today=datetime.date(2026, 8, 26))
        self.assertTrue(any("死亡を知った日_申告が死亡日_申告より前" in x
                            for x in problems))
        self.assertEqual(out, {})

    def test_rule2_existing_death_incoming_knew_heir(self):
        existing = self._existing_with(死亡日_申告="2026-05-02")
        out, problems, _choice = store.split_valid_fields(
            {"相続人と知った日_申告": "2026-05-01"}, existing,
            today=datetime.date(2026, 8, 26))
        self.assertTrue(any("相続人と知った日_申告が死亡日_申告より前" in x
                            for x in problems))
        self.assertEqual(out, {})

    def test_rule3_existing_knew_death_incoming_knew_heir(self):
        existing = self._existing_with(死亡を知った日_申告="2026-06-01")
        out, problems, _choice = store.split_valid_fields(
            {"相続人と知った日_申告": "2026-05-20"}, existing,
            today=datetime.date(2026, 8, 26))
        self.assertTrue(any("死亡を知った日_申告より前" in x
                            for x in problems))
        self.assertEqual(out, {})

    def test_reverse_direction_incoming_death_vs_existing_knew(self):
        # 逆方向: 既存=知った日・今回=死亡日 の組合せも検知（write 0）
        existing = self._existing_with(死亡を知った日_申告="2026-05-01")
        out, problems, _choice = store.split_valid_fields(
            {"死亡日_申告": "2026-05-02", "顧客名": "山田"}, existing,
            today=datetime.date(2026, 8, 26))
        self.assertTrue(problems)
        self.assertEqual(out, {"顧客名": "山田"})   # 日付は write 0・他は書く

    def test_no_incoming_dates_no_validation(self):
        existing = self._existing_with(死亡日_申告="2026-05-02")
        out, problems, _choice = store.split_valid_fields(
            {"顧客名": "山田"}, existing, today=datetime.date(2026, 8, 26))
        self.assertEqual(problems, [])
        self.assertEqual(out, {"顧客名": "山田"})


# ── モデル応答のフェイク ────────────────────────────────────────────────────────
def _text_response(text):
    return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])


def _tool_response(tool_input, text=""):
    blocks = []
    if text:
        blocks.append(SimpleNamespace(type="text", text=text))
    blocks.append(SimpleNamespace(type="tool_use", name="record_hearing",
                                  id="tu1", input=tool_input))
    return SimpleNamespace(content=blocks)


class _HearingBase(_StoreBase):
    def setUp(self):
        super().setUp()
        self.uid = f"U_houki_{self.id().rsplit('.', 1)[-1][:20]}"
        hearing.conversation_histories.pop(self.uid, None)
        # fix1[03]: 日付整合の失敗状態は App 40 正本＝in-memory カウンタなし

    def run_turn(self, responses, text="こんにちは", paused=False,
                 suppressed=False):
        """handle_houki_hearing を 1 回実行し (send, queue, chatlog) を返す。"""
        send, queue, chatlog = AsyncMock(), AsyncMock(return_value="q-1"), \
            AsyncMock()
        model = AsyncMock(side_effect=list(responses))
        with patch.object(hearing, "call_hearing_model", model), \
             patch.object(hearing, "reply_with_push_fallback", send), \
             patch.object(hearing, "save_to_approval_queue", queue), \
             patch.object(hearing, "save_to_chatlog", chatlog), \
             patch.object(hearing, "get_recent_chat_history",
                          AsyncMock(return_value=[])), \
             patch.object(hearing, "is_suppressed",
                          AsyncMock(return_value=suppressed)), \
             patch.object(hearing, "autoreply_paused", lambda: paused):
            _run(hearing.handle_houki_hearing("rtok", self.uid, text))
        return send, queue, chatlog


class TestHearingFlow(_HearingBase):
    def test_plain_reply_sent_via_houki_channel(self):
        send, queue, chatlog = self.run_turn(
            [_text_response("お問合せありがとうございます。"
                            "亡くなられた方のお名前を教えていただけますか。")])
        send.assert_awaited_once()
        self.assertIs(send.await_args.args[0], HOUKI_CHANNEL)
        self.assertEqual(send.await_args.args[1], "rtok")
        self.assertEqual(send.await_args.args[2], self.uid)
        queue.assert_not_awaited()
        cats = [c.args[3] for c in chatlog.await_args_list]
        self.assertEqual(cats, ["相続放棄ヒアリング", "相続放棄ヒアリング"])

    def test_tool_use_upserts_and_second_call_replies(self):
        send, _q, _c = self.run_turn(
            [_tool_response({"phase": "1_deceased",
                             "fields": {"被相続人氏名": "山田花子",
                                        "続柄": "子"},
                             "phase_done": True, "hearing_done": False}),
             _text_response("記録しました。次に死亡日を教えて"
                            "いただけますか。")],
            text="母の山田花子が亡くなりました")
        rows = list(self.fake.rows.values())
        self.assertEqual(len(rows), 1)
        rid = rows[0]["$id"]["value"]
        self.assertEqual(self.fake.field(rid, "被相続人氏名"), "山田花子")
        self.assertEqual(self.fake.field(rid, "受付チャネル"), "LINE")
        send.assert_awaited_once()
        self.assertIn("死亡日", send.await_args.args[3])

    def test_date_mismatch_twice_flags_and_queues(self):
        bad = {"phase": "2_dates",
               "fields": {"死亡日_申告": "2026-05-02",
                          "死亡を知った日_申告": "2026-05-01"},
               "phase_done": False, "hearing_done": False}
        # 1 回目: 日付は書かれない・queue なし・**メモに永続マーカー**（fix1[03]）
        _s, queue, _c = self.run_turn(
            [_tool_response(dict(bad)), _text_response("確認させてください。")])
        rid = list(self.fake.rows)[0]
        self.assertIsNone(self.fake.field(rid, "死亡日_申告"))
        queue.assert_not_awaited()
        self.assertIn(store.MISMATCH_MARKER,
                      self.fake.field(rid, "日付申告メモ") or "")
        # 「再起動」相当: in-memory の会話履歴を消しても状態は App 40 が正本
        hearing.conversation_histories.pop(self.uid, None)
        # 2 回目: 危険類型フラグ「申告内容の矛盾」+承認キュー
        _s, queue2, _c = self.run_turn(
            [_tool_response(dict(bad)), _text_response("再度確認します。")],
            text="やはり5月1日に知りました")
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["申告内容の矛盾"])
        queue2.assert_awaited_once()
        self.assertIn("日付整合検証の2回失敗",
                      queue2.await_args.kwargs["reason"])
        # 3 回目（フラグ済み）: 承認キュー増分 0・フラグ不変（fix1[03] 冪等）
        _s, queue3, _c = self.run_turn(
            [_tool_response(dict(bad)), _text_response("承知しました。")],
            text="同じ日付です")
        queue3.assert_not_awaited()
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["申告内容の矛盾"])
        memo = self.fake.field(rid, "日付申告メモ") or ""
        self.assertEqual(memo.count(store.MISMATCH_MARKER), 1)   # 追記も 1 回

    def test_partial_failure_queue_lost_then_refires(self):
        # fix1[03] 部分失敗: 2 回目でキュー作成が失敗（ACK 喪失）→ フラグは
        # **書かれない**（queue 先行）→ 次回の矛盾で再発火してキュー+フラグ完了
        bad = {"phase": "2_dates",
               "fields": {"死亡日_申告": "2026-05-02",
                          "死亡を知った日_申告": "2026-05-01"},
               "phase_done": False, "hearing_done": False}
        self.run_turn([_tool_response(dict(bad)), _text_response("確認します。")])
        rid = list(self.fake.rows)[0]
        # 2 回目: queue が例外 → 全体は確認中定型で縮退・フラグ未書込
        send, queue, chatlog = AsyncMock(), \
            AsyncMock(side_effect=RuntimeError("app29 down")), AsyncMock()
        model = AsyncMock(side_effect=[_tool_response(dict(bad))])
        with patch.object(hearing, "call_hearing_model", model), \
             patch.object(hearing, "reply_with_push_fallback", send), \
             patch.object(hearing, "save_to_approval_queue", queue), \
             patch.object(hearing, "save_to_chatlog", chatlog), \
             patch.object(hearing, "get_recent_chat_history",
                          AsyncMock(return_value=[])), \
             patch.object(hearing, "is_suppressed",
                          AsyncMock(return_value=False)), \
             patch.object(hearing, "autoreply_paused", lambda: False):
            _run(hearing.handle_houki_hearing("rtok", self.uid, "同じです"))
        queue.assert_awaited_once()
        self.assertEqual(self.fake.field(rid, "危険類型フラグ") or [], [])
        send.assert_awaited_once()   # 縮退の確認中定型
        self.assertEqual(send.await_args.args[3],
                         hp.HOUKI_PROFILE.pending_reply)
        # 3 回目（queue 正常）: 再発火してキュー+フラグ完了（at-least-once）
        _s, queue3, _c = self.run_turn(
            [_tool_response(dict(bad)), _text_response("承知しました。")],
            text="やはり同じです")
        queue3.assert_awaited_once()
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["申告内容の矛盾"])

    def test_cross_turn_date_mismatch_not_saved(self):
        # fix1[01]（Codex 指定形）: 第 1 ターンで死亡日保存 → 第 2 ターンで
        # それより前の「死亡を知った日」→ 保存されない
        self.run_turn(
            [_tool_response({"phase": "2_dates",
                             "fields": {"死亡日_申告": "2026-05-02"},
                             "phase_done": False, "hearing_done": False}),
             _text_response("記録しました。")])
        rid = list(self.fake.rows)[0]
        self.assertEqual(self.fake.field(rid, "死亡日_申告"), "2026-05-02")
        self.run_turn(
            [_tool_response({"phase": "2_dates",
                             "fields": {"死亡を知った日_申告": "2026-05-01"},
                             "phase_done": False, "hearing_done": False}),
             _text_response("確認します。")],
            text="知ったのは5月1日です")
        self.assertIsNone(self.fake.field(rid, "死亡を知った日_申告"))
        self.assertEqual(self.fake.field(rid, "死亡日_申告"), "2026-05-02")

    def test_hearing_done_promotes_status(self):
        filled = {c: "x" for c in store.HEARING_REQUIRED_FIELDS}
        filled["続柄"] = "子"          # HEARING-FIX1: 閉集合適合値で充足させる
        filled["相続順位"] = "子"
        filled["死亡日_申告"] = "2026-05-01"
        filled["死亡を知った日_申告"] = "2026-05-01"
        filled["相続人と知った日_申告"] = "2026-05-02"
        _s, _q, _c = self.run_turn(
            [_tool_response({"phase": "7_applicant", "fields": filled,
                             "phase_done": True, "hearing_done": True}),
             _text_response("ありがとうございます。弁護士が確認いたします。")])
        rid = list(self.fake.rows)[0]
        self.assertEqual(self.fake.field(rid, "status"), "電話判断待ち")

    def test_not_done_keeps_status(self):
        _s, _q, _c = self.run_turn(
            [_tool_response({"phase": "1_deceased",
                             "fields": {"被相続人氏名": "山田"},
                             "phase_done": True, "hearing_done": False}),
             _text_response("続いてお伺いします。")])
        rid = list(self.fake.rows)[0]
        self.assertEqual(self.fake.field(rid, "status"), "問い合わせ")


class TestHearingSendGate(_HearingBase):
    def _assert_demoted(self, send, queue, reason_part):
        send.assert_awaited_once()
        self.assertIs(send.await_args.args[0], HOUKI_CHANNEL)
        self.assertEqual(send.await_args.args[3], hp.HOUKI_PROFILE.pending_reply)
        queue.assert_awaited_once()
        self.assertIn(reason_part, queue.await_args.kwargs["reason"])
        self.assertIn("相続放棄ヒアリング送信ゲートで降格",
                      queue.await_args.kwargs["reason"])

    def test_long_reply_demoted(self):
        send, queue, _c = self.run_turn([_text_response("あ" * 301)])
        self._assert_demoted(send, queue, "文字数超過")

    def test_faq_backed_token_demoted(self):
        # route=houki_hearing は根拠集合空＝時効 FAQ 根拠語も降格
        send, queue, _c = self.run_turn(
            [_text_response("信用情報は5年程度で回復します。")])
        self._assert_demoted(send, queue, "経路（houki_hearing）に根拠のない具体値")

    def test_self_intro_demoted(self):
        send, queue, _c = self.run_turn(
            [_text_response("弁護士の大野と申します。ご相談を伺います。")])
        self._assert_demoted(send, queue, "弁護士本人の名乗り検出")

    def test_gates_paused_and_stoplist(self):
        send, queue, chatlog = self.run_turn(
            [_text_response("x")], paused=True)
        send.assert_not_awaited()
        queue.assert_not_awaited()
        chatlog.assert_not_awaited()
        send, queue, chatlog = self.run_turn(
            [_text_response("x")], suppressed=True)
        send.assert_not_awaited()
        chatlog.assert_not_awaited()

    def test_claude_unavailable_fallback(self):
        send, queue, chatlog = self.run_turn(
            [hp.ClaudeUnavailableError("down")])
        send.assert_awaited_once()
        self.assertEqual(send.await_args.args[3],
                         hp.HOUKI_PROFILE.pending_reply)
        queue.assert_awaited_once()
        self.assertIn("Claude応答不能", queue.await_args.kwargs["reason"])
        self.assertEqual(len(chatlog.await_args_list), 2)


class TestHoukiProfileAndPrompt(unittest.TestCase):
    def test_profile_hearing_values(self):
        p = hp.HOUKI_PROFILE
        self.assertEqual(p.name, "souzoku-houki")
        self.assertEqual(p.hearing_style_route, "houki_hearing")
        self.assertEqual(p.customer_style_route, "houki_customer")
        self.assertEqual(p.update_flag_key, "tanjun_shonin_flag")
        self.assertEqual(p.hearing_statuses, frozenset({"", "問い合わせ"}))
        self.assertIn("受任", p.post_engagement_statuses)
        # 障害/降格時の確認中応答は時効と同一の弁護士確定文言を再利用（裁定）
        self.assertEqual(p.pending_reply, cr.PENDING_REPLY)

    def test_profile_customer_side_fail_closed(self):
        # H-5 までの fail-closed プレースホルダ: 顧客対応は全降格・
        # 必須標準回答/第一報バックストップ無効
        p = hp.HOUKI_PROFILE
        self.assertEqual(p.auto_send_categories, frozenset())
        self.assertEqual(p.mandatory_reply_vocab, ())
        self.assertIsNone(p.first_report_detector)
        g = cr.apply_server_guards(
            {"reply": "ご案内します", "category": "挨拶・雑談",
             "auto_send": True}, [], "こんにちは", profile=p)
        self.assertFalse(g.can_auto_send)

    def test_route_basis_houki_hearing_empty(self):
        self.assertEqual(cr.ROUTE_BASIS["houki_hearing"], frozenset())
        v = cr.style_guard_violations("5年程度かかります",
                                      route="houki_hearing")
        self.assertTrue(any("根拠のない具体値" in x for x in v))

    def test_prompt_verbatim_items(self):
        # 質問項目の文言は正本 souzoku-houki/02 §2 の逐語（要語 pin）
        p = hp.HOUKI_HEARING_PROMPT
        for phrase in (
            "亡くなった方の氏名・依頼者との続柄",
            "最後の住所（市区町村まででも可）・本籍（分かれば）",
            "死亡日（分からなければおおよそ）",
            "死亡を知った日・自分が相続人だと知った日（別々に質問）",
            "知った経緯（役所からの通知・債権者からの請求・親族からの連絡 等）",
            "借金・督促の有無、督促状・訴状が届いているか",
            "依頼者は配偶者・子・親・兄弟姉妹のどれか",
            "先順位者（子・親）の有無と、その人達が放棄したか",
            "同順位の相続人（兄弟等）の人数・一緒に放棄したい人がいるか",
            "依頼者本人が相続人か（親族代理の相談か）",
            "未成年・成年後見の関与有無",
            "手元にある戸籍・住民票の有無（自分で取った/これから）",
            "事務所で職務上請求により取得可能",
            # 財産処分の中立質問（民法921条直結・プロンプト固定文言）
            "使ったり、処分したり、解約したり、そこから何かのお支払いをされた"
            "ものはありますか。",
            "YYYY-MM頃",
            "日付の確定は弁護士が行う",
            "熟慮期間の残日数・間に合うかどうかにも言及しない",
        ):
            with self.subTest(phrase=phrase[:20]):
                self.assertIn(phrase, p)
        # 文体（無内容見本・両業務共通の正）を収載
        self.assertIn(cr.HEARING_STYLE_SECTION_BASE, p)
        self.assertEqual(hp.HEARING_TEMPLATE_BLOCKS_HOUKI, ())

    def test_record_hearing_tool_schema(self):
        tool = hp.RECORD_HEARING_TOOL
        self.assertEqual(tool["name"], "record_hearing")
        self.assertEqual(
            tool["input_schema"]["properties"]["phase"]["enum"],
            ["1_deceased", "2_dates", "3_debts", "4_assets",
             "5_others", "6_koseki", "7_applicant"])
        self.assertEqual(sorted(tool["input_schema"]["required"]),
                         ["fields", "hearing_done", "phase", "phase_done"])


class TestChoiceFieldGuardFlow(_HearingBase):
    """HEARING-FIX1: 会話フロー——選択肢外値でも全断せず聞き直しで継続。"""

    def run_turn_with_model(self, responses, text="母が亡くなりました"):
        send, queue, chatlog = AsyncMock(), AsyncMock(return_value="q-1"), \
            AsyncMock()
        model = AsyncMock(side_effect=list(responses))
        with patch.object(hearing, "call_hearing_model", model), \
             patch.object(hearing, "reply_with_push_fallback", send), \
             patch.object(hearing, "save_to_approval_queue", queue), \
             patch.object(hearing, "save_to_chatlog", chatlog), \
             patch.object(hearing, "get_recent_chat_history",
                          AsyncMock(return_value=[])), \
             patch.object(hearing, "is_suppressed",
                          AsyncMock(return_value=False)), \
             patch.object(hearing, "autoreply_paused", lambda: False):
            _run(hearing.handle_houki_hearing("rtok", self.uid, text))
        return send, queue, chatlog, model

    def _tool_result_text(self, model) -> str:
        messages = model.await_args_list[1].args[1]
        return messages[-1]["content"][0]["content"]

    def test_out_of_set_choice_write0_retry_and_continue(self):
        send, queue, _c, model = self.run_turn_with_model(
            [_tool_response({"phase": "1_deceased",
                             "fields": {"被相続人氏名": "山田太郎",
                                        "続柄": "母"},
                             "phase_done": False, "hearing_done": False}),
             _text_response("失礼しました。亡くなられたのはお母様ですね。")])
        rows = list(self.fake.rows.values())
        self.assertEqual(len(rows), 1)                   # 全断せず作成される
        rid = rows[0]["$id"]["value"]
        self.assertEqual(self.fake.field(rid, "被相続人氏名"), "山田太郎")
        self.assertIsNone(self.fake.field(rid, "続柄"))   # 選択肢外は write 0
        tr = self._tool_result_text(model)
        self.assertIn("続柄=選択肢外", tr)
        self.assertIn("子/孫/配偶者", tr)                 # 聞き直しの許容値提示
        send.assert_awaited_once()                        # 会話は継続（定型でない）
        self.assertIn("お母様", send.await_args.args[3])
        queue.assert_not_awaited()
        # 日付矛盾の系（マーカー・危険類型フラグ）には入らない
        self.assertIsNone(self.fake.field(rid, "危険類型フラグ"))
        memo = self.fake.field(rid, "日付申告メモ") or ""
        self.assertNotIn(store.MISMATCH_MARKER, memo)

    def test_in_set_choice_written(self):
        _s, _q, _c, _m = self.run_turn_with_model(
            [_tool_response({"phase": "1_deceased",
                             "fields": {"続柄": "子"},
                             "phase_done": False, "hearing_done": False}),
             _text_response("ありがとうございます。")])
        rid = list(self.fake.rows)[0]
        self.assertEqual(self.fake.field(rid, "続柄"), "子")

    def test_shokugyou_dropped_others_written_no_crash(self):
        send, _q, _c, model = self.run_turn_with_model(
            [_tool_response({"phase": "7_applicant",
                             "fields": {"顧客名": "山田花子",
                                        "職業": "会社員"},
                             "phase_done": False, "hearing_done": False}),
             _text_response("記録しました。続いてご住所を伺えますか。")])
        rid = list(self.fake.rows)[0]
        self.assertEqual(self.fake.field(rid, "顧客名"), "山田花子")
        self.assertIsNone(self.fake.field(rid, "職業"))
        self.assertEqual(self._tool_result_text(model), "記録しました。")
        send.assert_awaited_once()
        self.assertIn("ご住所", send.await_args.args[3])

    def test_kintone_failure_classified_log_and_fallback(self):
        async def _fail(*a, **k):
            raise store.kintone.KintoneError(400, "GAIA_IA02", "token error")
        with patch.object(store.kintone, "create_record", _fail):
            with self.assertLogs("houki_bot.hearing", level="ERROR") as logs:
                send, _q, _c, _m = self.run_turn_with_model(
                    [_tool_response({"phase": "1_deceased",
                                     "fields": {"被相続人氏名": "山田太郎"},
                                     "phase_done": False,
                                     "hearing_done": False}),
                     _text_response("（到達しない）")])
        joined = "\n".join(logs.output)
        self.assertIn("kintone write failed", joined)     # 固定分類
        self.assertIn("GAIA_IA02", joined)                # 固定分類コード
        self.assertNotIn("token error", joined)           # 自由文本文は出さない
        self.assertNotIn("converse failed", joined)       # 汎用分類と区別
        send.assert_awaited_once()
        self.assertEqual(send.await_args.args[3], hp.HOUKI_PROFILE.pending_reply)



if __name__ == "__main__":
    unittest.main()
