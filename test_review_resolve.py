"""review_resolve.py（S5-2.5 T1 確定の関所コア）のテスト

検証:
冪等キーグルーピング（同一キー1グループ/異キー別グループ/他ソース分離/壊れJSON）・
ゴールデン比較（App 25 からの再構成財産行 = S5 直行経路と同一入力で一致）・
擬似物件の逆変換（所有者パース・建物の床面積再構成と構造非復元）・
確定の実行（35生成→30クローズ・原本移送・S4由来既存行への追記限定）・
二重確定ガード（要確認以外/実行済み/直前変化→グループ全体中止・書き込みゼロ）・
未知キーの明示応答・env 縮退・案件未指定。kintone は全てモック。
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")

import registry_ingest  # noqa: E402
import review_resolve  # noqa: E402
from review_resolve import (  # noqa: E402
    ReviewGroup,
    ReviewItem,
    _owners_from_mochibun,
    _pseudo_property,
    list_pending_reviews,
    resolve_group,
)

if os.environ.get("ANTHROPIC_API_KEY") == "dummy_key_for_import_only":
    del os.environ["ANTHROPIC_API_KEY"]

_ENV = {"APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
        "APP_ZAISAN": "35", "TOKEN_ZAISAN": "t35",
        "KINTONE_FUDOSAN_APP_ID": "25", "KINTONE_FUDOSAN_API_TOKEN": "t25",
        "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33",
        "SOUZOKU_KINTONE_APP_ID": "26"}


def run(coro):
    return asyncio.run(coro)


def wrap(fields: dict) -> dict:
    return {k: {"value": v} for k, v in fields.items()}


def review_rec(rid, idem="fid-1", source="registry_ingest", fudosan_id="97",
               channel_raw=None):
    import json
    raw = channel_raw if channel_raw is not None else json.dumps(
        {source: {"理由": "案件紐付け不能", "不動産レコードID": fudosan_id,
                  "冪等キー": idem}}, ensure_ascii=False)
    return {"$id": {"value": str(rid)},
            "件名": {"value": f"登記事項証明の読解転記: 案件紐付け不能"},
            "チャネル固有データ": {"value": raw},
            "成果物": {"value": [{"fileKey": f"pdf-{rid}", "name": "touki.pdf"}]}}


def land_prop():
    """S5 直行経路の入力（読解JSON形・土地=App 25 とロスレス往復できる形）"""
    return {"種別": "土地", "所在": "入間市東藤沢七丁目", "地番": "153番26",
            "地目": "宅地", "地積": "123.45㎡",
            "甲区": {"所有者": [
                {"氏名": "山田太郎", "持分": "2分の1"},
                {"氏名": "山田花子", "持分": "2分の1"}]},
            "乙区": {"有効権利あり": False, "内容": ""}}


class _KT:
    """hub.kintone モック（env 名＋クエリで振り分け）"""

    def __init__(self, *, shipping=None, fudosan=None, koseki=None,
                 zaisan_existing=(), pending=()):
        self.shipping = shipping or {}   # id -> {発送ステータス, 実行済み}
        self.fudosan = fudosan or {}     # id -> record(wrapped)
        self.koseki = koseki or {}       # id -> record(wrapped)（App 33・R4-0）
        self.zaisan_existing = list(zaisan_existing)
        self.pending = list(pending)
        self.created, self.updated, self.uploaded, self.downloaded = [], [], [], []

    async def search_records(self, app, query, fields=None):
        if app.app_id_env == "APP_SHIPPING":
            return self.pending
        if app.app_id_env == "APP_ZAISAN":
            return self.zaisan_existing
        return []

    async def get_record(self, app, record_id):
        if app.app_id_env == "APP_SHIPPING":
            return wrap(self.shipping[str(record_id)])
        if app.app_id_env == "APP_KOSEKI_BOOK":
            return self.koseki[str(record_id)]
        return self.fudosan[str(record_id)]

    async def create_record(self, app, fields):
        self.created.append((app.app_id_env, fields))
        return f"{app.app_id_env[-3:]}-{len(self.created)}"

    async def update_record(self, app, record_id, fields, revision=None):
        self.updated.append((app.app_id_env, str(record_id), fields))

    async def upload_file(self, app, filename, content, mime):
        self.uploaded.append((app.app_id_env, filename, content))
        return f"fk-{len(self.uploaded)}"

    async def download_file(self, app, file_key):
        self.downloaded.append(file_key)
        return b"PDFDATA"

    def patches(self):
        return [patch(f"hub.kintone.{n}", new=getattr(self, n))
                for n in ("search_records", "get_record", "create_record",
                          "update_record", "upload_file", "download_file")]

    def by_env(self, seq, env):
        return [x for x in seq if x[0] == env]


def arm(tc, kt, env=_ENV):
    for p in [patch.dict(os.environ, env), *kt.patches()]:
        p.start()
        tc.addCleanup(p.stop)


class TestGrouping(unittest.TestCase):
    def _groups(self, records):
        kt = _KT(pending=records)
        arm(self, kt)
        return run(list_pending_reviews())

    def test_same_idempotency_key_forms_one_group(self):
        groups = self._groups([review_rec(7, idem="fid-1"),
                               review_rec(8, idem="fid-1")])
        self.assertEqual(len(groups), 1)
        self.assertEqual([i.record_id for i in groups[0].items], ["7", "8"])
        self.assertEqual(groups[0].source, "registry_ingest")
        self.assertEqual(groups[0].idempotency_key, "fid-1")

    def test_different_keys_form_separate_groups(self):
        groups = self._groups([review_rec(7, idem="fid-1"),
                               review_rec(9, idem="fid-2")])
        self.assertEqual(len(groups), 2)

    def test_other_source_is_separate_group(self):
        groups = self._groups([review_rec(7, idem="fid-1"),
                               review_rec(10, idem="fid-1", source="zaisan_sync")])
        self.assertEqual(len(groups), 2)
        self.assertEqual({g.source for g in groups},
                         {"registry_ingest", "zaisan_sync"})

    def test_broken_channel_data_becomes_unknown(self):
        groups = self._groups([review_rec(11, channel_raw="not-json")])
        self.assertEqual(groups[0].source, "unknown")
        self.assertEqual(groups[0].idempotency_key, "record:11")

    def test_shipping_env_unset_returns_empty(self):
        kt = _KT(pending=[review_rec(7)])
        arm(self, kt, env={**_ENV, "APP_SHIPPING": ""})
        self.assertEqual(run(list_pending_reviews()), [])


class TestPseudoProperty(unittest.TestCase):
    def test_owners_from_mochibun(self):
        self.assertEqual(_owners_from_mochibun("山田太郎 2分の1・山田花子 2分の1"),
                         [{"氏名": "山田太郎", "持分": "2分の1"},
                          {"氏名": "山田花子", "持分": "2分の1"}])
        self.assertEqual(_owners_from_mochibun("熊澤正広"), [{"氏名": "熊澤正広"}])
        self.assertEqual(_owners_from_mochibun("熊澤 正広 3分の2"),
                         [{"氏名": "熊澤 正広", "持分": "3分の2"}])
        self.assertEqual(_owners_from_mochibun(""), [])

    def test_building_reconstruction_documented_limits(self):
        """建物: 部屋番号→家屋番号・建物名→種類・床面積N階→原文形の再構成。
        構造は App 25 に器が無いため復元されない（ドキュメント済みの制約）"""
        record25 = wrap({"種別": "建物", "所在": "入間市東藤沢七丁目153番地26",
                         "部屋番号": "153番26", "建物名": "居宅",
                         "床面積1階": "58.50", "床面積2階": "62.60",
                         "持分割合": "熊澤正広", "地番": "", "地目": "",
                         "地積": ""})
        pseudo = _pseudo_property(record25)
        self.assertEqual(pseudo["種別"], "建物")
        self.assertEqual(pseudo["家屋番号"], "153番26")
        self.assertEqual(pseudo["種類"], "居宅")
        self.assertEqual(pseudo["床面積"], "1階 58.50㎡ 2階 62.60㎡")
        self.assertNotIn("構造", pseudo)

    def test_mansion_kind_inverse_mapping(self):
        record25 = wrap({"種別": "マンション(区分所有)", "所在": "x",
                         "持分割合": "", "地番": "", "地目": "", "地積": "",
                         "部屋番号": "", "建物名": ""})
        self.assertEqual(_pseudo_property(record25)["種別"], "区分建物")


class TestGoldenReconstruction(unittest.TestCase):
    """再構成財産行 = S5 直行経路と同一入力で一致（同一関数共有＋逆変換の検証）"""

    def _capture_upsert(self, prop):
        kt = _KT()
        arm(self, kt)
        run(registry_ingest._upsert_zaisan(prop, "97", "100", "fid-1",
                                           b"PDFDATA", "touki.pdf"))
        (_, fields), = kt.by_env(kt.created, "APP_ZAISAN")
        return fields

    def test_reconstructed_fields_equal_direct_path(self):
        prop = land_prop()
        direct = self._capture_upsert(prop)
        # S5 直行が App 25 に書いた形 → レコード形 → 擬似物件 → 同一関数
        record25 = wrap({**{"地番": "", "地目": "", "地積": "", "部屋番号": "",
                            "建物名": ""},
                         **registry_ingest._fudosan_fields(prop)})
        reconstructed = self._capture_upsert(_pseudo_property(record25))
        self.assertEqual(reconstructed, direct,
                         "特定情報・名義・財産種別・データ源・冪等キー・元参照・"
                         "評価確定/有効・原本まで完全一致")

    def test_golden_key_fields(self):
        """一致の中身の明示（ゴールデンの読める化）"""
        record25 = wrap({**{"部屋番号": "", "建物名": ""},
                         **registry_ingest._fudosan_fields(land_prop())})
        fields = self._capture_upsert(_pseudo_property(record25))
        self.assertEqual(fields["特定情報"],
                         "所在 入間市東藤沢七丁目 / 地番 153番26 / 地目 宅地 / "
                         "地積 123.45㎡")
        self.assertEqual(fields["名義"], "山田太郎（持分2分の1）外1名")
        self.assertEqual(fields["データ源"], "OCR_登記事項証明")
        self.assertEqual(fields["冪等キー"], "fid-1")
        self.assertEqual(fields["評価確定"], "no")


class TestResolveGroup(unittest.TestCase):
    def group(self, n=2):
        items = [ReviewItem(record_id=str(6 + i), subject="登記",
                            detail={"不動産レコードID": str(96 + i),
                                    "冪等キー": "fid-1"},
                            file_keys=[f"pdf-{6 + i}"], file_name="touki.pdf")
                 for i in range(1, n + 1)]
        return ReviewGroup(source="registry_ingest", idempotency_key="fid-1",
                           items=items)

    def fudosan_records(self):
        base = {"地番": "", "地目": "", "地積": "", "部屋番号": "", "建物名": ""}
        return {
            "97": wrap({**base, **registry_ingest._fudosan_fields(land_prop())}),
            "98": wrap({**base, "種別": "建物", "所在": "入間市東藤沢七丁目153番地26",
                        "部屋番号": "153番26", "建物名": "居宅",
                        "持分割合": "熊澤正広"}),
        }

    def test_happy_path_two_items(self):
        kt = _KT(shipping={"7": {"発送ステータス": "要確認", "実行済み": "no"},
                           "8": {"発送ステータス": "要確認", "実行済み": "no"}},
                 fudosan=self.fudosan_records())
        arm(self, kt)
        result = run(resolve_group(self.group(), "100"))
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(len(result["items"]), 2)
        # App 35 が2行生成（案件・データ源・冪等キー）
        zaisan = kt.by_env(kt.created, "APP_ZAISAN")
        self.assertEqual(len(zaisan), 2)
        for _, fields in zaisan:
            self.assertEqual(fields["案件レコードID"], "100")
            self.assertEqual(fields["データ源"], "OCR_登記事項証明")
            self.assertEqual(fields["冪等キー"], "fid-1")
        # 原本移送: download → App 35 へ upload
        self.assertEqual(kt.downloaded, ["pdf-7", "pdf-8"])
        self.assertEqual([u[0] for u in kt.uploaded],
                         ["APP_ZAISAN", "APP_ZAISAN"])
        self.assertEqual(kt.uploaded[0][2], b"PDFDATA")
        # App 30 クローズ（完了＋実行済み=yes）
        closes = kt.by_env(kt.updated, "APP_SHIPPING")
        self.assertEqual([(rid, f) for _, rid, f in closes],
                         [("7", {"発送ステータス": "完了", "実行済み": "yes"}),
                          ("8", {"発送ステータス": "完了", "実行済み": "yes"})])

    def test_guard_status_changed_aborts_whole_group_without_writes(self):
        """2件目が直前に完了へ変化 → グループ全体中止・書き込みゼロ（部分実行しない）"""
        kt = _KT(shipping={"7": {"発送ステータス": "要確認", "実行済み": "no"},
                           "8": {"発送ステータス": "完了", "実行済み": "yes"}},
                 fudosan=self.fudosan_records())
        arm(self, kt)
        result = run(resolve_group(self.group(), "100"))
        self.assertEqual(result["status"], "aborted")
        self.assertIn("No.8 が要確認ではなくなっています", result["reason"])
        self.assertEqual(kt.created, [])
        self.assertEqual(kt.updated, [])

    def test_guard_executed_flag_aborts(self):
        kt = _KT(shipping={"7": {"発送ステータス": "要確認", "実行済み": "yes"}},
                 fudosan=self.fudosan_records())
        arm(self, kt)
        result = run(resolve_group(self.group(n=1), "100"))
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(kt.created, [])

    def test_missing_fudosan_id_aborts(self):
        kt = _KT(shipping={"7": {"発送ステータス": "要確認", "実行済み": "no"}})
        arm(self, kt)
        group = ReviewGroup(source="registry_ingest", idempotency_key="k",
                            items=[ReviewItem(record_id="7", subject="x",
                                              detail={})])
        result = run(resolve_group(group, "100"))
        self.assertEqual(result["status"], "aborted")
        self.assertIn("不動産レコードID", result["reason"])

    def test_unknown_source_is_explicit(self):
        kt = _KT()
        arm(self, kt)
        group = ReviewGroup(source="zaisan_sync", idempotency_key="k",
                            items=[ReviewItem(record_id="9", subject="x",
                                              detail={})])
        result = run(resolve_group(group, "100"))
        self.assertEqual(result["status"], "unsupported")
        self.assertIn("対応する確定処理がありません", result["reason"])
        self.assertIn("zaisan_sync", result["reason"])
        self.assertEqual(kt.created, [])

    def test_env_unset_is_unavailable(self):
        kt = _KT()
        arm(self, kt, env={**_ENV, "APP_ZAISAN": ""})
        result = run(resolve_group(self.group(n=1), "100"))
        self.assertEqual(result["status"], "unavailable")
        self.assertIn("APP_ZAISAN", result["reason"])

    def test_missing_case_id_aborts(self):
        kt = _KT()
        arm(self, kt)
        result = run(resolve_group(self.group(n=1), ""))
        self.assertEqual(result["status"], "aborted")
        self.assertIn("案件レコードID", result["reason"])

    def test_existing_s4_row_gets_append_only_update(self):
        """S4由来の同一物件行あり → 追記限定（S5 直行と同一の意味論・関数共有）"""
        kt = _KT(shipping={"7": {"発送ステータス": "要確認", "実行済み": "no"}},
                 fudosan=self.fudosan_records(),
                 zaisan_existing=[{"$id": {"value": "88"},
                                   "原本": {"value": [{"fileKey": "old-key"}]}}])
        arm(self, kt)
        result = run(resolve_group(self.group(n=1), "100"))
        self.assertEqual(result["items"][0]["zaisan"], "updated")
        (_, rid, fields), = kt.by_env(kt.updated, "APP_ZAISAN")
        self.assertEqual(rid, "88")
        self.assertEqual(set(fields), {"特定情報", "名義", "原本"},
                         "評価額・評価確定・データ源・有効は不触")
        self.assertEqual(fields["原本"][0], {"fileKey": "old-key"})


class TestResolveKoseki(unittest.TestCase):
    """R4-0: koseki_ingest ハンドラ（戸籍の案件紐付け＋クローズ・App 34 不触）"""

    def group(self, detail=None):
        return ReviewGroup(
            source="koseki_ingest", idempotency_key="drv-1",
            items=[ReviewItem(record_id="9",
                              subject="戸籍読解の案件紐付け: 案件紐付け不能",
                              detail=detail if detail is not None else
                              {"戸籍レコードID": "1", "冪等キー": "drv-1"})])

    def kt(self, *, status="要確認", executed="no", current_case=""):
        return _KT(shipping={"9": {"発送ステータス": status, "実行済み": executed}},
                   koseki={"1": wrap({"案件レコードID": current_case})})

    def test_happy_path_links_case_and_closes(self):
        kt = self.kt()
        arm(self, kt)
        result = run(resolve_group(self.group(), "100"))
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["items"],
                         [{"review_record_id": "9", "koseki_record_id": "1"}])
        koseki_updates = kt.by_env(kt.updated, "APP_KOSEKI_BOOK")
        self.assertEqual(koseki_updates,
                         [("APP_KOSEKI_BOOK", "1",
                           {"案件アプリID": "26", "案件レコードID": "100"})])
        closes = kt.by_env(kt.updated, "APP_SHIPPING")
        self.assertEqual(closes, [("APP_SHIPPING", "9",
                                   {"発送ステータス": "完了", "実行済み": "yes"})])
        self.assertEqual(kt.created, [], "App 34 を含め新規レコードは作らない")

    def test_already_linked_to_other_case_aborts(self):
        kt = self.kt(current_case="55")
        arm(self, kt)
        result = run(resolve_group(self.group(), "100"))
        self.assertEqual(result["status"], "aborted")
        self.assertIn("既に案件 No.55 に紐付いています", result["reason"])
        self.assertEqual(kt.updated, [])

    def test_already_linked_to_same_case_resolves(self):
        kt = self.kt(current_case="100")
        arm(self, kt)
        result = run(resolve_group(self.group(), "100"))
        self.assertEqual(result["status"], "resolved")

    def test_guard_status_changed_aborts(self):
        kt = self.kt(status="完了", executed="yes")
        arm(self, kt)
        result = run(resolve_group(self.group(), "100"))
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(kt.updated, [])

    def test_missing_koseki_id_aborts(self):
        kt = self.kt()
        arm(self, kt)
        result = run(resolve_group(self.group(detail={}), "100"))
        self.assertEqual(result["status"], "aborted")
        self.assertIn("戸籍レコードID", result["reason"])

    def test_koseki_book_env_unset_is_unavailable(self):
        kt = self.kt()
        arm(self, kt, env={**_ENV, "APP_KOSEKI_BOOK": ""})
        result = run(resolve_group(self.group(), "100"))
        self.assertEqual(result["status"], "unavailable")
        self.assertIn("APP_KOSEKI_BOOK", result["reason"])

    def test_zaisan_env_not_required_for_koseki(self):
        """必要 env はハンドラ別（koseki は App 25/35 不要・レジストリ分離の検証）"""
        kt = self.kt()
        arm(self, kt, env={**_ENV, "APP_ZAISAN": "",
                           "KINTONE_FUDOSAN_APP_ID": ""})
        result = run(resolve_group(self.group(), "100"))
        self.assertEqual(result["status"], "resolved")


class TestResolveValuation(unittest.TestCase):
    """S4-M2: valuation_ingest ハンドラ（25から財産行 upsert・S4 資産温存・クローズ）"""

    def group(self):
        return ReviewGroup(
            source="valuation_ingest", idempotency_key="sha256:v1",
            items=[ReviewItem(record_id="11",
                              subject="評価証明・課税明細の読解転記: 案件紐付け不能",
                              detail={"不動産レコードID": "7",
                                      "冪等キー": "sha256:v1"},
                              file_keys=["pdf-11"], file_name="hyoka.pdf")])

    def kt(self):
        return _KT(shipping={"11": {"発送ステータス": "要確認", "実行済み": "no"}},
                   fudosan={"7": wrap({"種別": "土地", "所在": "入間市東藤沢七丁目",
                                       "地番": "153番26", "部屋番号": "",
                                       "固定資産税評価額": "12345678",
                                       "固定資産税評価年度": "2026"})})

    def test_happy_path_upserts_zaisan_and_closes(self):
        kt = self.kt()
        arm(self, kt)
        result = run(resolve_group(self.group(), "3"))
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["items"][0]["zaisan"], "created")
        (_, fields), = kt.by_env(kt.created, "APP_ZAISAN")
        self.assertEqual(fields["データ源"], "OCR_課税明細")
        self.assertEqual(fields["評価額"], "12345678")
        self.assertEqual(fields["評価基準日"], "2026-01-01", "賦課期日温存")
        self.assertEqual(fields["評価確定"], "no")
        self.assertNotIn("名義", fields,
                         "確定ハンドラ経由では名義を書かない（登記由来が正の序列）")
        closes = kt.by_env(kt.updated, "APP_SHIPPING")
        self.assertEqual(closes, [("APP_SHIPPING", "11",
                                   {"発送ステータス": "完了", "実行済み": "yes"})])

    def test_guard_aborts_without_writes(self):
        kt = self.kt()
        kt.shipping["11"]["発送ステータス"] = "完了"
        arm(self, kt)
        result = run(resolve_group(self.group(), "3"))
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(kt.created, [])
        self.assertEqual(kt.updated, [])


if __name__ == "__main__":
    unittest.main()
