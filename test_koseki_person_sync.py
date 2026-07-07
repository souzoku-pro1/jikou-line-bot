"""koseki_person_sync.py（R4-1 人物レコード生成）のテスト

検証:
熊澤5名の起票テーブルテスト（実JSON形フィクスチャ・役割分類・登場戸籍/身分事項
サブテーブル・和暦原文の保持先・案件参照の継承）・親子エッジ候補（父/母人物ID）・
死亡記載の生死区分（記載なしは書かない）・種別の写像（転籍→その他）・
冪等（再実行で重複起票しない・既存レコードを更新しない）・
確認済み系フィールドへの自動遷移なしの固定・env/状態/案件未紐付けの縮退・
R4-0 ハンドラからのフラグ起動（既定無効/有効時のみ）。kintone は全てモック。
"""

import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")

import koseki_person_sync  # noqa: E402
from koseki_person_sync import sync_persons_from_koseki  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == "dummy_key_for_import_only":
    del os.environ["ANTHROPIC_API_KEY"]

_ENV = {"APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33",
        "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34"}

# 実機 App 33 No.1 の読解JSONを模したフィクスチャ（熊澤家5名）
KUMAZAWA = {
    "様式": "現行",
    "戸籍": {"本籍": "埼玉県川口市在家町29番", "筆頭者": "熊澤 秀和",
             "編製日": "平成14年1月1日",
             "従前戸籍": {"本籍": "埼玉県入間市東藤沢七丁目31番", "筆頭者": "熊澤博"}},
    "人物": [
        {"氏名": "熊澤 秀和", "続柄": "三男", "生年月日": "昭和47年11月8日",
         "除籍済み": False,
         "身分事項": [{"種別": "出生", "日付": "昭和47年11月8日"},
                      {"種別": "婚姻", "日付": "平成10年12月25日",
                       "相手方": "山嵜知子"}]},
        {"氏名": "熊澤 知子", "続柄": "二女", "生年月日": "昭和49年5月28日",
         "除籍済み": False,
         "身分事項": [{"種別": "出生", "日付": "昭和49年5月28日"},
                      {"種別": "婚姻", "日付": "平成10年12月25日",
                       "相手方": "熊澤秀和"}]},
        {"氏名": "熊澤 風香", "続柄": "長女", "生年月日": "平成11年6月11日",
         "除籍済み": False, "身分事項": [{"種別": "出生", "日付": "平成11年6月11日"}]},
        {"氏名": "熊澤 舞", "続柄": "二女", "生年月日": "平成17年10月2日",
         "除籍済み": False, "身分事項": [{"種別": "出生", "日付": "平成17年10月2日"}]},
        {"氏名": "熊澤 美咲", "続柄": "三女", "生年月日": "平成23年2月14日",
         "除籍済み": False, "身分事項": [{"種別": "出生", "日付": "平成23年2月14日"}]},
    ],
}


def run(coro):
    return asyncio.run(coro)


def koseki_record(state="AI読解済", case_id="100", reading=KUMAZAWA):
    return {"読解状態": {"value": state},
            "案件アプリID": {"value": "26"},
            "案件レコードID": {"value": case_id},
            "読解JSON": {"value": json.dumps(reading, ensure_ascii=False)}}


class _KT:
    def __init__(self, *, koseki=None, existing_names=()):
        self.koseki = koseki or koseki_record()
        self.existing_names = set(existing_names)  # 冪等ヒットさせる氏名
        self.created = []
        self.updated = []
        self.searches = []

    async def get_record(self, app, record_id):
        assert app.app_id_env == "APP_KOSEKI_BOOK"
        return self.koseki

    async def search_records(self, app, query, fields=None):
        assert app.app_id_env == "APP_KOSEKI_PERSON"
        self.searches.append(query)
        for name in self.existing_names:
            if f'氏名 = "{name}"' in query:
                return [{"$id": {"value": f"既存-{name}"}}]
        return []

    async def create_record(self, app, fields):
        assert app.app_id_env == "APP_KOSEKI_PERSON"
        self.created.append(fields)
        return str(340 + len(self.created))

    async def update_record(self, app, record_id, fields, revision=None):
        self.updated.append((record_id, fields))

    def patches(self):
        return [patch(f"hub.kintone.{n}", new=getattr(self, n))
                for n in ("get_record", "search_records", "create_record",
                          "update_record")]

    def by_name(self, name):
        return next(f for f in self.created if f["氏名"] == name)


def arm(tc, kt, env=_ENV):
    for p in [patch.dict(os.environ, env), *kt.patches()]:
        p.start()
        tc.addCleanup(p.stop)


class TestKumazawaTable(unittest.TestCase):
    """熊澤5名の起票テーブルテスト"""

    def setUp(self):
        self.kt = _KT()
        arm(self, self.kt)
        self.result = run(sync_persons_from_koseki("1"))

    def test_five_persons_created(self):
        self.assertEqual(self.result["status"], "synced")
        self.assertEqual(len(self.kt.created), 5)
        self.assertEqual([c["氏名"] for c in self.result["created"]],
                         ["熊澤 秀和", "熊澤 知子", "熊澤 風香", "熊澤 舞",
                          "熊澤 美咲"], "親（筆頭者・配偶者）が先")

    def test_roles_and_touring_koseki(self):
        expects = {"熊澤 秀和": "筆頭者", "熊澤 知子": "配偶者",
                   "熊澤 風香": "子", "熊澤 舞": "子", "熊澤 美咲": "子"}
        for name, role in expects.items():
            row = self.kt.by_name(name)["登場戸籍"][0]["value"]
            self.assertEqual(row["登場区分"]["value"], role, name)
            self.assertEqual(row["戸籍レコードID"]["value"], "1")

    def test_parent_edge_candidates(self):
        """子3名に 父=秀和(No.341)・母=知子(No.342) の候補エッジ"""
        for name in ("熊澤 風香", "熊澤 舞", "熊澤 美咲"):
            f = self.kt.by_name(name)
            self.assertEqual(f["父人物ID"], "341", name)
            self.assertEqual(f["母人物ID"], "342", name)
        self.assertNotIn("父人物ID", self.kt.by_name("熊澤 秀和"))

    def test_meyose_is_unconfirmed_and_case_inherited(self):
        for f in self.kt.created:
            self.assertEqual(f["名寄せ確定"], "未確定")
            self.assertEqual(f["案件アプリID"], "26")
            self.assertEqual(f["案件レコードID"], "100")
            self.assertEqual(f["ユニット種別"], "相続一般")
            self.assertEqual(f["本籍最新"], "埼玉県川口市在家町29番")

    def test_wareki_kept_in_subtable_not_date_fields(self):
        """和暦原文は身分事項サブテーブルに保持・DATE型（生年月日/死亡日）は書かない"""
        f = self.kt.by_name("熊澤 秀和")
        self.assertNotIn("生年月日", f)
        self.assertNotIn("死亡日", f)
        rows = [r["value"] for r in f["身分事項"]]
        self.assertEqual(rows[0]["事項種別"]["value"], "出生")
        self.assertEqual(rows[0]["年月日"]["value"], "昭和47年11月8日")
        self.assertEqual(rows[1]["事項種別"]["value"], "婚姻")
        self.assertEqual(rows[1]["相手方"]["value"], "山嵜知子")
        self.assertEqual(f["名寄せキー"], "熊澤秀和|昭和47年11月8日")

    def test_no_confirmed_state_fields_written(self):
        """確認済み系フィールドへの自動遷移コード禁止の固定"""
        forbidden = ("確認状態", "確認者", "確認日時", "グラフ確定日時",
                     "相続人候補", "相続資格", "被相続人フラグ", "戸籍確認済")
        for f in self.kt.created:
            for code in forbidden:
                self.assertNotIn(code, f, f["氏名"])

    def test_alive_persons_do_not_get_seishi(self):
        """死亡記載なし＝生存と推定しない（生死区分を書かず初期値=不明のまま）"""
        for f in self.kt.created:
            self.assertNotIn("生死区分", f)


class TestVariants(unittest.TestCase):
    def test_death_record_sets_seishi_dead(self):
        reading = {"戸籍": {"本籍": "x", "筆頭者": "山田太郎"},
                   "人物": [{"氏名": "山田太郎", "続柄": "長男",
                             "除籍済み": True, "除籍事由": "死亡",
                             "身分事項": [{"種別": "死亡",
                                           "日付": "令和2年5月3日"}]}]}
        kt = _KT(koseki=koseki_record(reading=reading))
        arm(self, kt)
        run(sync_persons_from_koseki("1"))
        self.assertEqual(kt.created[0]["生死区分"], "死亡")

    def test_unknown_event_type_maps_to_sonota(self):
        reading = {"戸籍": {"本籍": "x", "筆頭者": "山田太郎"},
                   "人物": [{"氏名": "山田太郎", "続柄": "長男",
                             "身分事項": [{"種別": "転籍",
                                           "日付": "平成5年1月1日"}]}]}
        kt = _KT(koseki=koseki_record(reading=reading))
        arm(self, kt)
        run(sync_persons_from_koseki("1"))
        row = kt.created[0]["身分事項"][0]["value"]
        self.assertEqual(row["事項種別"]["value"], "その他")
        self.assertIn("転籍", row["記載原文"]["value"])

    def test_idempotent_rerun_creates_nothing_and_updates_nothing(self):
        """全員既存 → 再実行で起票ゼロ・更新ゼロ（確定操作なしで App 34 不変の固定）"""
        kt = _KT(existing_names={"熊澤 秀和", "熊澤 知子", "熊澤 風香",
                                 "熊澤 舞", "熊澤 美咲"})
        arm(self, kt)
        result = run(sync_persons_from_koseki("1"))
        self.assertEqual(result["status"], "synced")
        self.assertEqual(len(result["skipped"]), 5)
        self.assertEqual(kt.created, [])
        self.assertEqual(kt.updated, [], "既存レコードの更新はしない（マージはR4-2）")

    def test_partial_existing_uses_existing_parent_id(self):
        """親が既存でも子のエッジは既存IDを使う"""
        kt = _KT(existing_names={"熊澤 秀和"})
        arm(self, kt)
        run(sync_persons_from_koseki("1"))
        child = kt.by_name("熊澤 風香")
        self.assertEqual(child["父人物ID"], "既存-熊澤 秀和")

    def test_env_unset_skips(self):
        kt = _KT()
        arm(self, kt, env={**_ENV, "APP_KOSEKI_PERSON": ""})
        result = run(sync_persons_from_koseki("1"))
        self.assertEqual(result["status"], "skipped")
        self.assertIn("APP_KOSEKI_PERSON", result["reason"])
        self.assertEqual(kt.created, [])

    def test_unread_state_skips(self):
        kt = _KT(koseki=koseki_record(state="未読解"))
        arm(self, kt)
        result = run(sync_persons_from_koseki("1"))
        self.assertEqual(result["status"], "skipped")
        self.assertIn("未読解", result["reason"])

    def test_unlinked_case_skips(self):
        """案件未紐付けの戸籍は人物化しない（宙に浮いた人物を作らない）"""
        kt = _KT(koseki=koseki_record(case_id=""))
        arm(self, kt)
        result = run(sync_persons_from_koseki("1"))
        self.assertEqual(result["status"], "skipped")
        self.assertIn("案件未紐付け", result["reason"])
        self.assertEqual(kt.created, [])


class TestIdempotencyQueryFormat(unittest.TestCase):
    """冪等クエリの実機仕様固定: サブテーブル内フィールド（戸籍レコードID）は
    `=` 不可（GAIA_IQ07・2026-07-07 実機）——`in` を使う。氏名（トップ）は `=`"""

    def test_find_existing_uses_in_operator_for_subtable_field(self):
        kt = _KT()
        arm(self, kt)
        run(sync_persons_from_koseki("1"))
        self.assertTrue(kt.searches, "冪等チェックの検索が実行される")
        for q in kt.searches:
            self.assertIn('戸籍レコードID in ("1")', q,
                          "サブテーブル内フィールドは in 演算子")
            self.assertNotIn("戸籍レコードID =", q, "= は実機で GAIA_IQ07")
        self.assertIn('氏名 = "熊澤 秀和"', kt.searches[0])


class TestSyncMissingPersons(unittest.TestCase):
    """回収関数（案件紐付け済み×人物未生成のみ拾う・失敗は他を止めない）"""

    def _arm(self, *, koseki_ids=("1", "2", "3"), personified=("2",),
             sync_side_effect=None, env=_ENV):
        self.book_query = []
        self.person_query = []

        async def search_records(app, query, fields=None):
            if app.app_id_env == "APP_KOSEKI_BOOK":
                self.book_query.append(query)
                return [{"$id": {"value": i}} for i in koseki_ids]
            self.person_query.append(query)
            for i in personified:
                if f'戸籍レコードID in ("{i}")' in query:
                    return [{"$id": {"value": "既存"}}]
            return []

        self.sync = AsyncMock(side_effect=sync_side_effect,
                              return_value={"status": "synced", "created": [],
                                            "skipped": []})
        patchers = [
            patch("hub.kintone.search_records", new=search_records),
            patch("koseki_person_sync.sync_persons_from_koseki", new=self.sync),
            patch.dict(os.environ, env),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    def test_picks_only_linked_and_unpersonified(self):
        self._arm()
        results = run(koseki_person_sync.sync_missing_persons())
        # 対象抽出の条件: 読解済み×案件紐付け済み
        self.assertIn('読解状態 in ("確認済", "AI読解済")', self.book_query[0])
        self.assertIn('案件レコードID != ""', self.book_query[0])
        # 人物生成済み（id=2）はスキップ・未生成（1,3）のみ人物化
        self.assertEqual([c.args[0] for c in self.sync.await_args_list],
                         ["1", "3"])
        skipped = [r for r in results if r.get("reason") == "人物生成済み"]
        self.assertEqual([r["koseki_record_id"] for r in skipped], ["2"])
        # 人物生成済み判定も in 演算子（GAIA_IQ07 の再発防止）
        for q in self.person_query:
            self.assertNotIn("戸籍レコードID =", q)

    def test_one_failure_does_not_stop_others(self):
        async def side(koseki_id):
            if koseki_id == "1":
                raise RuntimeError("boom")
            return {"status": "synced", "koseki_record_id": koseki_id,
                    "created": [], "skipped": []}
        self._arm(personified=(), sync_side_effect=side)
        results = run(koseki_person_sync.sync_missing_persons())
        self.assertEqual([r["status"] for r in results],
                         ["error", "synced", "synced"])

    def test_env_unset_returns_empty(self):
        self._arm(env={**_ENV, "APP_KOSEKI_PERSON": ""})
        self.assertEqual(run(koseki_person_sync.sync_missing_persons()), [])
        self.sync.assert_not_awaited()


class TestR40Wiring(unittest.TestCase):
    """R4-0 ハンドラからの起動（env フラグ・既定無効）"""

    def _resolve(self, env_flag):
        from review_resolve import ReviewGroup, ReviewItem, resolve_group
        group = ReviewGroup(
            source="koseki_ingest", idempotency_key="drv-1",
            items=[ReviewItem(record_id="9", subject="戸籍読解の案件紐付け",
                              detail={"戸籍レコードID": "1", "冪等キー": "drv-1"})])
        env = {"APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
               "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33",
               "SOUZOKU_KINTONE_APP_ID": "26",
               "KOSEKI_PERSON_SYNC_ENABLED": env_flag}
        sync = AsyncMock(return_value={"status": "synced", "created": [],
                                       "skipped": []})

        async def get_record(app, record_id):
            if app.app_id_env == "APP_SHIPPING":
                return {"発送ステータス": {"value": "要確認"},
                        "実行済み": {"value": "no"}}
            return {"案件レコードID": {"value": ""}}

        with patch.dict(os.environ, env), \
                patch("hub.kintone.get_record", new=get_record), \
                patch("hub.kintone.update_record", new=AsyncMock()), \
                patch("koseki_person_sync.sync_persons_from_koseki", new=sync):
            result = run(resolve_group(group, "100"))
        return result, sync

    def test_flag_off_by_default_does_not_sync(self):
        result, sync = self._resolve(env_flag="")
        self.assertEqual(result["status"], "resolved")
        sync.assert_not_awaited()
        self.assertNotIn("persons", result["items"][0])

    def test_flag_on_runs_sync_after_link(self):
        result, sync = self._resolve(env_flag="1")
        self.assertEqual(result["status"], "resolved")
        sync.assert_awaited_once_with("1")
        self.assertEqual(result["items"][0]["persons"]["status"], "synced")


if __name__ == "__main__":
    unittest.main()
