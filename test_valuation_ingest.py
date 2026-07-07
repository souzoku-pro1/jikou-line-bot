"""POST /valuation/ingest（S4-M2 評価読解の転記＋入口）のテスト

検証: token 認証（404偽装・探信422回避）・品質ゲート（要確認・トップキー=
valuation_ingest）・複数物件の物件ごと名寄せ→upsert・25の上書きは評価額/年度のみ
（S4温存）・upsert キー（案件・元レコードID・評価基準日=賦課期日）・
評価確定/有効/名義の不触保護（更新時）と新規時のみ設定・冪等キー表記
（sha256:統一・旧素hex互換・同一PDFの原本重複添付防止）・名寄せ分岐
（一致/不一致/曖昧）・案件解決（case_hint/逆引き/不能→要確認）・env 縮退。
既存 /ocr/fixed-asset・zaisan_sync は不変（全suiteの既存回帰で固定）。
OCR / 読解 / kintone は全てモック。
"""

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

_DUMMY_ANTHROPIC_KEY = "dummy_key_for_import_only"
os.environ.setdefault("ANTHROPIC_API_KEY", _DUMMY_ANTHROPIC_KEY)
os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "KINTONE_WEBHOOK_TOKEN": "approve_token",
    "DOCUMENT_WEBHOOK_SECRET": "doc_secret",
    "APP_APPROVAL": "29",
    "TOKEN_APPROVAL": "dummy",
    "GOOGLE_VISION_API_KEY": "dummy_vision",
    "HEALTHCHECK_DISABLED": "1",
})

from fastapi.testclient import TestClient  # noqa: E402

import valuation_ingest  # noqa: E402
from valuation_ingest import same_pdf_key  # noqa: E402
import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]

client = TestClient(main.app)

URL = "/valuation/ingest"
PDF = b"%PDF-1.4 dummy valuation"

_ENV = {"VALUATION_INGEST_TOKEN": "val_token",
        "APP_ZAISAN": "35", "TOKEN_ZAISAN": "t35",
        "KINTONE_FUDOSAN_APP_ID": "25", "KINTONE_FUDOSAN_API_TOKEN": "t25",
        "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30"}


def land(**kw):
    p = {"種別": "土地", "所在": "入間市東藤沢七丁目", "地番": "153番26",
         "評価額": 12345678, "confidence": {"所在": 0.9, "評価額": 0.9}}
    p.update(kw)
    return p


def building(**kw):
    p = {"種別": "家屋", "所在": "入間市東藤沢七丁目153番地26",
         "家屋番号": "153番26", "評価額": 3456789, "confidence": {"所在": 0.9}}
    p.update(kw)
    return p


def reading(props=None, year=2026, owner="熊澤正広"):
    return {"書類種別": "課税明細", "年度": year, "所有者名": owner,
            "物件": props if props is not None else [land()],
            "confidence": {"書類種別": 0.95, "年度": 0.9}}


def frec(rid, shozai, chiban="", heya="", kind="土地"):
    return {"$id": {"value": str(rid)}, "所在": {"value": shozai},
            "地番": {"value": chiban}, "部屋番号": {"value": heya},
            "種別": {"value": kind}}


def wrap(fields):
    return {k: {"value": v} for k, v in fields.items()}


_FUDOSAN_DEFAULT = {"種別": "土地", "所在": "入間市東藤沢七丁目",
                    "地番": "153番26", "部屋番号": "",
                    "固定資産税評価額": "12345678", "固定資産税評価年度": "2026"}


class _KT:
    def __init__(self, *, cands=(), zaisan_rows=(), reverse=(), fudosan=None):
        self.cands = list(cands)
        self.zaisan_rows = list(zaisan_rows)   # upsert 検索の既存行
        self.reverse = list(reverse)           # 逆引き
        self.fudosan = fudosan or {}           # id -> record(wrapped)
        self.created, self.updated, self.uploaded, self.searches = [], [], [], []

    async def search_records(self, app, query, fields=None):
        self.searches.append((app.app_id_env, query))
        if app.app_id_env == "APP_ZAISAN":
            return self.reverse if "元アプリID" in query else self.zaisan_rows
        if app.app_id_env == "KINTONE_FUDOSAN_APP_ID":
            return self.cands
        return []

    async def get_record(self, app, record_id):
        assert app.app_id_env == "KINTONE_FUDOSAN_APP_ID"
        return self.fudosan.get(str(record_id), wrap(_FUDOSAN_DEFAULT))

    async def create_record(self, app, fields):
        self.created.append((app.app_id_env, fields))
        return {"KINTONE_FUDOSAN_APP_ID": "251", "APP_ZAISAN": "351",
                "APP_SHIPPING": "301"}[app.app_id_env]

    async def update_record(self, app, record_id, fields, revision=None):
        self.updated.append((app.app_id_env, str(record_id), fields))

    async def upload_file(self, app, filename, content, mime):
        self.uploaded.append((app.app_id_env, filename))
        return f"fk-{len(self.uploaded)}"

    def patches(self):
        return [patch(f"hub.kintone.{n}", new=getattr(self, n))
                for n in ("search_records", "get_record", "create_record",
                          "update_record", "upload_file")]

    def by_env(self, seq, env):
        return [x for x in seq if x[0] == env]


class _Base(unittest.TestCase):
    def post(self, kt: _KT, env=None, rd=None, token="val_token", data=None):
        patchers = [
            patch("valuation_ingest.read_valuation",
                  new=AsyncMock(return_value=rd if rd is not None else reading())),
            patch.dict("os.environ", env if env is not None else _ENV),
            *kt.patches(),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        # OCR は main._ocr_pdf_bytes（実行時 import）をモック
        po = patch("main._ocr_pdf_bytes", new=MagicMock(return_value="評価OCR"))
        po.start()
        self.addCleanup(po.stop)
        query = f"?token={token}" if token is not None else ""
        return client.post(URL + query,
                           files={"file": ("hyoka.pdf", PDF, "application/pdf")},
                           data=data or {})


class TestAuthAndInput(_Base):
    def test_missing_token_is_404(self):
        self.assertEqual(self.post(_KT(), token=None).status_code, 404)

    def test_wrong_token_is_404(self):
        self.assertEqual(self.post(_KT(), token="wrong").status_code, 404)

    def test_token_env_unset_is_404_deny_all(self):
        resp = self.post(_KT(), env={**_ENV, "VALUATION_INGEST_TOKEN": ""})
        self.assertEqual(resp.status_code, 404)

    def test_probe_without_body_is_404_not_422(self):
        with patch.dict("os.environ", _ENV):
            self.assertEqual(client.post(URL).status_code, 404)

    def test_valid_token_without_file_is_400(self):
        with patch.dict("os.environ", _ENV):
            resp = client.post(URL + "?token=val_token")
        self.assertEqual(resp.status_code, 400)


class TestQualityGate(_Base):
    def test_low_confidence_goes_to_review_with_valuation_envelope(self):
        import json
        kt = _KT()
        resp = self.post(kt, rd={"書類種別": "課税明細",
                                 "物件": [land(confidence={})],
                                 "confidence": {}})
        self.assertEqual(resp.json()["status"], "needs_review")
        (_, fields), = kt.by_env(kt.created, "APP_SHIPPING")
        self.assertEqual(list(json.loads(fields["チャネル固有データ"])),
                         ["valuation_ingest"], "トップキー=valuation_ingest")
        self.assertEqual(kt.by_env(kt.created, "APP_ZAISAN"), [])


class TestMultiPropertyAndMatching(_Base):
    def test_two_properties_matched_and_created(self):
        """複数物件を物件ごとに名寄せ: 土地=一致→25は評価額/年度のみ上書き（S4温存）・
        家屋=不一致→25新規、財産行は2行 upsert"""
        kt = _KT(cands=[frec(7, "入間市東藤沢七丁目", chiban="153番26")],
                 fudosan={"7": wrap(_FUDOSAN_DEFAULT)})
        resp = self.post(kt, rd=reading(props=[land(), building()]),
                         data={"case_hint": "3"})
        body = resp.json()
        self.assertEqual([r["fudosan"] for r in body["results"]],
                         ["updated", "created"])
        env, rid, fields = kt.by_env(kt.updated, "KINTONE_FUDOSAN_APP_ID")[0]
        self.assertEqual(rid, "7")
        self.assertEqual(set(fields), {"固定資産税評価額", "固定資産税評価年度"},
                         "既存25への上書きは S4 と同じ2フィールドのみ")
        self.assertEqual(fields["固定資産税評価額"], "12345678")
        _, cfields = kt.by_env(kt.created, "KINTONE_FUDOSAN_APP_ID")[0]
        self.assertEqual(cfields["種別"], "建物", "家屋→建物（実機選択肢）")
        self.assertEqual(cfields["状況"], "熊澤正広")
        self.assertEqual(len(kt.by_env(kt.created, "APP_ZAISAN")), 2)

    def test_ambiguous_goes_to_review_without_writes(self):
        kt = _KT(cands=[frec(7, "入間市東藤沢七丁目", chiban="153番26"),
                        frec(8, "入間市東藤沢七丁目", chiban="153番26")])
        resp = self.post(kt, data={"case_hint": "3"})
        self.assertEqual(resp.json()["results"][0]["fudosan"], "needs_review")
        self.assertEqual(kt.by_env(kt.updated, "KINTONE_FUDOSAN_APP_ID"), [])
        self.assertEqual(kt.by_env(kt.created, "APP_ZAISAN"), [])


class TestZaisanUpsert(_Base):
    def test_create_sets_initial_and_owner(self):
        """新規作成時のみ 評価確定=no/有効=yes/名義=所有者名・賦課期日・sha256:表記"""
        kt = _KT(cands=[frec(7, "入間市東藤沢七丁目", chiban="153番26")])
        resp = self.post(kt, data={"case_hint": "3"})
        self.assertEqual(resp.json()["results"][0]["zaisan"], "created")
        (_, fields), = kt.by_env(kt.created, "APP_ZAISAN")
        self.assertEqual(fields["評価確定"], "no")
        self.assertEqual(fields["有効"], "yes")
        self.assertEqual(fields["名義"], "熊澤正広")
        self.assertEqual(fields["評価基準日"], "2026-01-01", "賦課期日=年度の1/1")
        self.assertEqual(fields["評価方法"], "固定資産税評価額")
        self.assertEqual(fields["データ源"], "OCR_課税明細")
        self.assertEqual(fields["元レコードID"], "7")
        self.assertTrue(fields["冪等キー"].startswith("sha256:"),
                        "格納表記は sha256: 付きに統一")

    def test_update_protects_confirmed_fields_and_owner(self):
        """既存行（同一 upsert キー）: 評価確定・有効・名義は不触（S4温存＋登記由来が正）"""
        kt = _KT(cands=[frec(7, "入間市東藤沢七丁目", chiban="153番26")],
                 zaisan_rows=[{"$id": {"value": "88"},
                               "冪等キー": {"value": "sha256:other"},
                               "原本": {"value": []}}])
        resp = self.post(kt, data={"case_hint": "3"})
        self.assertEqual(resp.json()["results"][0]["zaisan"], "updated")
        (_, rid, fields), = kt.by_env(kt.updated, "APP_ZAISAN")
        self.assertEqual(rid, "88")
        for forbidden in ("評価確定", "有効", "名義"):
            self.assertNotIn(forbidden, fields)
        # upsert キーに評価基準日（年度別行の分離）が含まれる
        zaisan_queries = [q for e, q in kt.searches if e == "APP_ZAISAN"
                          and "元レコードID" in q and "元アプリID" not in q]
        self.assertIn('評価基準日 = "2026-01-01"', zaisan_queries[0])

    def test_same_pdf_key_compat(self):
        """冪等キー照合の表記互換（sha256: 新表記 vs S4 旧素hex）"""
        self.assertTrue(same_pdf_key("abc123", "sha256:abc123"))
        self.assertTrue(same_pdf_key("sha256:abc123", "abc123"))
        self.assertTrue(same_pdf_key("sha256:abc123", "sha256:abc123"))
        self.assertFalse(same_pdf_key("sha256:zzz", "sha256:abc123"))
        self.assertFalse(same_pdf_key("", "sha256:abc123"))

    def test_same_pdf_does_not_reattach_original(self):
        """既存行の冪等キーが同一PDF（旧素hex表記）なら原本を再添付しない"""
        import hashlib
        bare = hashlib.sha256(PDF).hexdigest()
        kt = _KT(cands=[frec(7, "入間市東藤沢七丁目", chiban="153番26")],
                 zaisan_rows=[{"$id": {"value": "88"},
                               "冪等キー": {"value": bare},  # S4 旧素hex
                               "原本": {"value": [{"fileKey": "old"}]}}])
        self.post(kt, data={"case_hint": "3"})
        (_, _, fields), = kt.by_env(kt.updated, "APP_ZAISAN")
        self.assertNotIn("原本", fields, "同一PDFは原本を重複添付しない")

    def test_different_pdf_appends_original(self):
        kt = _KT(cands=[frec(7, "入間市東藤沢七丁目", chiban="153番26")],
                 zaisan_rows=[{"$id": {"value": "88"},
                               "冪等キー": {"value": "sha256:different"},
                               "原本": {"value": [{"fileKey": "old"}]}}])
        self.post(kt, data={"case_hint": "3"})
        (_, _, fields), = kt.by_env(kt.updated, "APP_ZAISAN")
        self.assertEqual(fields["原本"][0], {"fileKey": "old"})
        self.assertEqual(len(fields["原本"]), 2, "既存保持＋新PDF追加")


class TestCaseResolutionAndEnv(_Base):
    def test_reverse_lookup(self):
        kt = _KT(cands=[frec(7, "入間市東藤沢七丁目", chiban="153番26")],
                 reverse=[{"案件レコードID": {"value": "3"}}])
        resp = self.post(kt)
        self.assertEqual(resp.json()["results"][0]["case_record_id"], "3")

    def test_unresolvable_case_goes_to_review(self):
        kt = _KT(cands=[frec(7, "入間市東藤沢七丁目", chiban="153番26")])
        resp = self.post(kt)
        result = resp.json()["results"][0]
        self.assertEqual(result["zaisan"], "needs_review")
        self.assertEqual(kt.by_env(kt.created, "APP_ZAISAN"), [])

    def test_zaisan_env_unset_skips_zaisan(self):
        kt = _KT(cands=[frec(7, "入間市東藤沢七丁目", chiban="153番26")])
        resp = self.post(kt, env={**_ENV, "APP_ZAISAN": ""},
                         data={"case_hint": "3"})
        result = resp.json()["results"][0]
        self.assertEqual(result["fudosan"], "updated")
        self.assertNotIn("zaisan", result)

    def test_fudosan_env_unset_creates_zaisan_directly(self):
        """25 なし縮退: 読解値から財産行を直接作る（元レコードIDなし）"""
        kt = _KT()
        resp = self.post(kt, env={**_ENV, "KINTONE_FUDOSAN_APP_ID": ""},
                         data={"case_hint": "3"})
        result = resp.json()["results"][0]
        self.assertEqual(result["zaisan"], "created")
        (_, fields), = kt.by_env(kt.created, "APP_ZAISAN")
        self.assertNotIn("元レコードID", fields)
        self.assertEqual(fields["評価額"], "12345678")
        self.assertEqual(fields["名義"], "熊澤正広")


if __name__ == "__main__":
    unittest.main()
