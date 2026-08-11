"""POST /registry/ingest（S5-2 登記読解の転記＋入口）のテスト

検証: token 認証（404偽装・探信422回避）・非PDF 400・Vision env 500・
冪等 skip・品質ゲート（スキーマ逸脱/低確信度→要確認・転記なし）・
正規化（全角半角/ハイフン/番地/丁目漢数字）・名寄せ分岐（完全一致=update／
不一致=create／曖昧・複数=マージも先頭採用もせず要確認）・種別写像（転記層）・
App 25 転記（担保抵当権/担保内容・持分割合・床面積分解）・App 35 upsert
（新規/S4由来行への追記・評価額/評価確定を触らない）・名義表示文字列・
案件紐付け（case_hint/逆引き/不能→要確認）・env 縮退（35/25/30 各スキップ）。
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

import registry_ingest  # noqa: E402
from registry_ingest import normalize_addr, owners_display  # noqa: E402
import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]

client = TestClient(main.app)

URL = "/registry/ingest"
PDF = b"%PDF-1.4 dummy registry"

_ENV = {"REGISTRY_INGEST_TOKEN": "reg_token",
        "APP_ZAISAN": "35", "TOKEN_ZAISAN": "t35",
        "KINTONE_FUDOSAN_APP_ID": "25", "KINTONE_FUDOSAN_API_TOKEN": "t25",
        "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30"}


def prop(kind="土地", shozai="埼玉県川口市青木一丁目", chiban="123番4", **kw):
    """日本語キー登記JSONの物件1件（確信度は品質ゲートを通る値）"""
    p = {"種別": kind, "所在": shozai, "地番": chiban, "地目": "宅地",
         "地積": "123.45㎡",
         "甲区": {"所有者": [
             {"氏名": "山田太郎", "住所": "川口市青木一丁目1番1号",
              "持分": "2分の1", "confidence": {"氏名": 0.95}},
             {"氏名": "山田花子", "持分": "2分の1"}],
             "受付日": "平成14年3月1日", "confidence": {"受付日": 0.9}},
         "乙区": {"有効権利あり": True, "内容": "抵当権（株式会社〇〇銀行）",
                  "confidence": 0.8},
         "confidence": {"所在": 0.9}}
    p.update(kw)
    return p


def frec(rid, shozai, chiban="", heya="", kind="土地"):
    """App 25 の候補レコード"""
    return {"$id": {"value": str(rid)}, "所在": {"value": shozai},
            "地番": {"value": chiban}, "部屋番号": {"value": heya},
            "種別": {"value": kind}}


class _KT:
    """hub.kintone のモック（env 名でアプリを判別・クエリ内容で検索を振り分け）"""

    def __init__(self, *, idem=(), cands=(), zaisan_rows=(), reverse=()):
        self.idem = list(idem)              # App 35 冪等キー検索
        self.cands = list(cands)            # App 25 候補検索
        self.zaisan_rows = list(zaisan_rows)  # App 35 同一物件（S4由来）検索
        self.reverse = list(reverse)        # App 35 案件逆引き
        self.created = []                   # (env, fields)
        self.updated = []                   # (env, record_id, fields)
        self.uploaded = []
        self.searches = []

    async def search_records(self, app, query, fields=None):
        self.searches.append((app.app_id_env, query))
        if app.app_id_env == "APP_ZAISAN":
            if "冪等キー" in query:
                return self.idem
            if "元アプリID" in query:
                return self.reverse
            return self.zaisan_rows
        if app.app_id_env == "KINTONE_FUDOSAN_APP_ID":
            return self.cands
        return []

    async def create_record(self, app, fields):
        self.created.append((app.app_id_env, fields))
        return {"KINTONE_FUDOSAN_APP_ID": "251", "APP_ZAISAN": "351",
                "APP_SHIPPING": "301"}[app.app_id_env]

    async def update_record(self, app, record_id, fields, revision=None):
        self.updated.append((app.app_id_env, record_id, fields))

    async def upload_file(self, app, filename, content, mime):
        self.uploaded.append((app.app_id_env, filename))
        return f"fk-{len(self.uploaded)}"

    def patches(self):
        return [patch(f"hub.kintone.{name}", new=getattr(self, name))
                for name in ("search_records", "create_record",
                             "update_record", "upload_file")]

    def by_env(self, seq, env):
        return [x for x in seq if x[0] == env]


class _Base(unittest.TestCase):
    def post(self, kt: _KT, env=None, reading=None, token="reg_token",
             data=None, ocr_text="登記OCRテキスト"):
        self.ocr = MagicMock(return_value=ocr_text)
        reading = reading if reading is not None else {"物件": [prop()]}
        patchers = [
            patch("registry_ingest._ocr_pdf", new=self.ocr),
            patch("registry_ingest.read_registry",
                  new=AsyncMock(return_value=reading)),
            patch.dict("os.environ", env if env is not None else _ENV),
            *kt.patches(),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        query = f"?token={token}" if token is not None else ""
        return client.post(URL + query,
                           files={"file": ("touki.pdf", PDF, "application/pdf")},
                           data=data or {})


class TestAuthAndInput(_Base):
    def test_missing_token_is_404(self):
        self.assertEqual(self.post(_KT(), token=None).status_code, 404)

    def test_wrong_token_is_404(self):
        self.assertEqual(self.post(_KT(), token="wrong").status_code, 404)

    def test_token_env_unset_is_404_deny_all(self):
        resp = self.post(_KT(), env={**_ENV, "REGISTRY_INGEST_TOKEN": ""})
        self.assertEqual(resp.status_code, 404)

    def test_probe_without_body_is_404_not_422(self):
        with patch.dict("os.environ", _ENV):
            self.assertEqual(client.post(URL).status_code, 404)

    def test_valid_token_without_file_is_400(self):
        with patch.dict("os.environ", _ENV):
            resp = client.post(URL + "?token=reg_token")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("PDF", resp.json()["detail"])

    def test_non_pdf_is_400(self):
        with patch.dict("os.environ", _ENV):
            resp = client.post(URL + "?token=reg_token",
                               files={"file": ("photo.jpg", b"x", "image/jpeg")})
        self.assertEqual(resp.status_code, 400)

    def test_vision_env_unset_is_500(self):
        resp = self.post(_KT(), env={**_ENV, "GOOGLE_VISION_API_KEY": ""})
        self.assertEqual(resp.status_code, 500)


class TestNormalization(unittest.TestCase):
    def test_normalize_addr_variants(self):
        cases = [
            ("１２３番地４", "123番4"),                    # 全角＋番地→番
            ("123 番 4", "123番4"),                        # 空白除去
            # ハイフン類・「の」は「番」へ同値化（S4-M2 で _normalize_shozaichi の
            # 知見を統合・32-6 / 32の6 / 32番地6 が同値になる）
            ("123−4", "123番4"), ("123ー4", "123番4"), ("32の6", "32番6"),
            ("1-2-3", "1番2番3"),                          # 多段も収束
            ("青木2丁目", "青木二丁目"),                    # 丁目の漢数字化
            ("埼玉県川口市青木", "川口市青木"),             # 都道府県削除（S4知見）
            ("川口市 青木１丁目 123番地4", "川口市青木一丁目123番4"),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(normalize_addr(raw), expected)

    def test_owners_display(self):
        self.assertEqual(owners_display([{"氏名": "熊澤正広", "持分": "2分の1"},
                                         {"氏名": "熊澤花子", "持分": "2分の1"}]),
                         "熊澤正広（持分2分の1）外1名")
        self.assertEqual(owners_display([{"氏名": "山田太郎"}]), "山田太郎")
        self.assertEqual(owners_display([]), "")


class TestIdempotencyAndGate(_Base):
    def test_same_idempotency_key_skips(self):
        kt = _KT(idem=[{"$id": {"value": "42"}}])
        resp = self.post(kt, data={"drive_file_id": "drv-1"})
        self.assertEqual(resp.json()["status"], "skip")
        self.ocr.assert_not_called()
        self.assertEqual(kt.created, [])

    def test_low_confidence_goes_to_review_without_writes(self):
        reading = {"物件": [{"種別": "土地", "所在": "x"}]}  # confidence なし=0.0
        kt = _KT()
        resp = self.post(kt, reading=reading)
        body = resp.json()
        self.assertEqual(body["status"], "needs_review")
        self.assertIn("全体確信度", body["reason"])
        env_created = [e for e, _ in kt.created]
        self.assertEqual(env_created, ["APP_SHIPPING"], "要確認キューのみ起票")
        _, fields = kt.created[0]
        self.assertEqual(fields["発送ステータス"], "要確認")
        self.assertEqual(fields["方向"], "受領")

    def test_schema_violation_goes_to_review(self):
        kt = _KT()
        resp = self.post(kt, reading={"物件": []})
        self.assertEqual(resp.json()["status"], "needs_review")
        self.assertIn("スキーマ逸脱", resp.json()["reason"])


class TestFudosanMatching(_Base):
    def test_exact_match_updates_app25(self):
        """正規化完全一致1件 → update（表記揺れ: 全角地番・番地・空白）"""
        kt = _KT(cands=[frec(7, "埼玉県川口市 青木１丁目", chiban="１２３番地４")])
        resp = self.post(kt, data={"case_hint": "100"})
        self.assertEqual(resp.json()["results"][0]["fudosan"], "updated")
        env, rid, fields = kt.by_env(kt.updated, "KINTONE_FUDOSAN_APP_ID")[0]
        self.assertEqual(rid, "7")
        self.assertEqual(fields["担保抵当権"], "有")
        self.assertEqual(fields["担保内容"], "抵当権（株式会社〇〇銀行）")
        self.assertEqual(fields["持分割合"], "山田太郎 2分の1・山田花子 2分の1")
        self.assertEqual(fields["地積"], "123.45")

    def test_no_match_creates_app25(self):
        kt = _KT(cands=[frec(7, "全く別の市", chiban="999番9")])
        resp = self.post(kt, data={"case_hint": "100"})
        self.assertEqual(resp.json()["results"][0]["fudosan"], "created")
        env, fields = kt.by_env(kt.created, "KINTONE_FUDOSAN_APP_ID")[0]
        self.assertEqual(fields["種別"], "土地")
        self.assertEqual(fields["所在"], "埼玉県川口市青木一丁目")

    def test_multiple_exact_matches_go_to_review_without_write(self):
        """完全一致2件 → 先頭採用せず・create もせず要確認キュー（裁定）"""
        kt = _KT(cands=[frec(7, "埼玉県川口市青木一丁目", chiban="123番4"),
                        frec(8, "埼玉県川口市青木一丁目", chiban="123番4")])
        resp = self.post(kt, data={"case_hint": "100"})
        result = resp.json()["results"][0]
        self.assertEqual(result["fudosan"], "needs_review")
        self.assertEqual(kt.by_env(kt.updated, "KINTONE_FUDOSAN_APP_ID"), [])
        self.assertEqual(kt.by_env(kt.created, "KINTONE_FUDOSAN_APP_ID"), [])
        self.assertEqual(kt.by_env(kt.created, "APP_ZAISAN"), [],
                         "曖昧物件は財産行も作らない")
        self.assertEqual(len(kt.by_env(kt.created, "APP_SHIPPING")), 1)

    def test_partial_match_is_ambiguous(self):
        """所在だけ一致（地番不一致）は曖昧一致 → マージせず要確認"""
        kt = _KT(cands=[frec(7, "埼玉県川口市青木一丁目", chiban="999番9")])
        resp = self.post(kt, data={"case_hint": "100"})
        self.assertEqual(resp.json()["results"][0]["fudosan"], "needs_review")

    def test_kind_mismatch_is_not_a_match(self):
        """同一所在・同一番号でも種別が違えば別物件（建物候補に土地は一致しない）"""
        kt = _KT(cands=[frec(7, "埼玉県川口市青木一丁目", heya="123番4", kind="建物")])
        resp = self.post(kt, data={"case_hint": "100"})
        self.assertEqual(resp.json()["results"][0]["fudosan"], "created")


class TestOtsukuTransfer(_Base):
    def test_no_active_mortgage_blanks_tanpo_naiyou(self):
        """乙区 有効権利あり=無 のとき「登記記録の乙区に記録されている事項はない」
        等の原文を担保内容に書かない（空=フィールド自体を送らない・MAINT-4
        台帳§2 の軽微改善）。担保抵当権=無 の転記は従来どおり"""
        p = prop(乙区={"有効権利あり": False,
                       "内容": "登記記録の乙区に記録されている事項はない",
                       "confidence": 0.8})
        kt = _KT()
        self.post(kt, reading={"物件": [p]}, data={"case_hint": "100"})
        env, fields = kt.by_env(kt.created, "KINTONE_FUDOSAN_APP_ID")[0]
        self.assertEqual(fields["担保抵当権"], "無")
        self.assertNotIn("担保内容", fields)

    def test_active_mortgage_keeps_tanpo_naiyou(self):
        """有効権利あり=有 は内容原文をそのまま転記（既存挙動の対照固定）"""
        kt = _KT()
        self.post(kt, data={"case_hint": "100"})
        env, fields = kt.by_env(kt.created, "KINTONE_FUDOSAN_APP_ID")[0]
        self.assertEqual(fields["担保抵当権"], "有")
        self.assertEqual(fields["担保内容"], "抵当権（株式会社〇〇銀行）")


class TestKindMappingAndBuilding(_Base):
    def test_kubun_tatemono_maps_to_mansion(self):
        """種別写像は転記層: 区分建物→マンション(区分所有)・不明→その他"""
        building = prop(kind="区分建物", chiban="", 家屋番号="123番4の501",
                        種類="居宅", 構造="鉄筋コンクリート造5階建",
                        床面積="1階 58.50㎡ 2階 62.60㎡")
        kt = _KT()
        self.post(kt, reading={"物件": [building]}, data={"case_hint": "100"})
        env, fields = kt.by_env(kt.created, "KINTONE_FUDOSAN_APP_ID")[0]
        self.assertEqual(fields["種別"], "マンション(区分所有)")
        self.assertEqual(fields["部屋番号"], "123番4の501")
        self.assertEqual(fields["建物名"], "居宅")
        self.assertEqual(fields["階数"], "5")
        self.assertEqual(fields["床面積1階"], "58.50")
        self.assertEqual(fields["床面積2階"], "62.60")
        _, zfields = kt.by_env(kt.created, "APP_ZAISAN")[0]
        self.assertEqual(zfields["財産種別"], "不動産_区分建物")

    def test_unknown_kind_maps_to_sonota(self):
        kt = _KT()
        self.post(kt, reading={"物件": [prop(kind="不明")]},
                  data={"case_hint": "100"})
        env, fields = kt.by_env(kt.created, "KINTONE_FUDOSAN_APP_ID")[0]
        self.assertEqual(fields["種別"], "その他")


class TestZaisanUpsert(_Base):
    def test_creates_zaisan_row_with_display_owner(self):
        kt = _KT()  # 25 は不一致→create=251
        resp = self.post(kt, data={"case_hint": "100"})
        result = resp.json()["results"][0]
        self.assertEqual(result["zaisan"], "created")
        env, fields = kt.by_env(kt.created, "APP_ZAISAN")[0]
        self.assertEqual(fields["名義"], "山田太郎（持分2分の1）外1名")
        self.assertEqual(fields["データ源"], "OCR_登記事項証明")
        self.assertEqual(fields["財産種別"], "不動産_土地")
        self.assertEqual(fields["元アプリID"], "25")
        self.assertEqual(fields["元レコードID"], "251")
        self.assertEqual(fields["評価確定"], "no")
        self.assertEqual(fields["有効"], "yes")
        self.assertIn("所在 埼玉県川口市青木一丁目", fields["特定情報"])
        self.assertIn("原本", fields)

    def test_existing_s4_row_gets_append_only_update(self):
        """評価証明由来の同一物件行がある → 追記のみ（評価系・データ源を触らない）"""
        kt = _KT(cands=[frec(7, "埼玉県川口市青木一丁目", chiban="123番4")],
                 zaisan_rows=[{"$id": {"value": "88"},
                               "原本": {"value": [{"fileKey": "old-key"}]}}])
        resp = self.post(kt, data={"case_hint": "100"})
        self.assertEqual(resp.json()["results"][0]["zaisan"], "updated")
        env, rid, fields = kt.by_env(kt.updated, "APP_ZAISAN")[0]
        self.assertEqual(rid, "88")
        self.assertEqual(set(fields), {"特定情報", "名義", "原本"},
                         "追記は3項目のみ（評価額・評価確定・データ源・有効は不触）")
        self.assertEqual(fields["原本"][0], {"fileKey": "old-key"},
                         "既存の原本（課税明細）を保持したまま登記PDFを追加")
        self.assertEqual(len(fields["原本"]), 2)

    def test_case_resolution_by_reverse_lookup(self):
        """case_hint 無し → 既存財産行から案件を逆引き"""
        kt = _KT(cands=[frec(7, "埼玉県川口市青木一丁目", chiban="123番4")],
                 reverse=[{"案件レコードID": {"value": "77"}}])
        resp = self.post(kt)
        result = resp.json()["results"][0]
        self.assertEqual(result["case_record_id"], "77")

    def test_unresolvable_case_goes_to_review(self):
        """case_hint 無し・逆引き不能 → 財産行は作らず要確認キュー"""
        kt = _KT()
        resp = self.post(kt)
        result = resp.json()["results"][0]
        self.assertEqual(result["zaisan"], "needs_review")
        self.assertEqual(kt.by_env(kt.created, "APP_ZAISAN"), [])
        self.assertEqual(len(kt.by_env(kt.created, "APP_SHIPPING")), 1)
        self.assertEqual(result["fudosan"], "created",
                         "不動産25の転記は独立に実施される")


class TestEnvDegradation(_Base):
    def test_zaisan_env_unset_skips_zaisan_only(self):
        kt = _KT()
        resp = self.post(kt, env={**_ENV, "APP_ZAISAN": ""},
                         data={"case_hint": "100"})
        self.assertEqual(resp.status_code, 200)
        result = resp.json()["results"][0]
        self.assertEqual(result["fudosan"], "created")
        self.assertNotIn("zaisan", result)
        self.assertEqual(kt.by_env(kt.created, "APP_ZAISAN"), [])

    def test_fudosan_env_unset_skips_fudosan_only(self):
        kt = _KT()
        resp = self.post(kt, env={**_ENV, "KINTONE_FUDOSAN_APP_ID": ""},
                         data={"case_hint": "100"})
        result = resp.json()["results"][0]
        self.assertNotIn("fudosan", result)
        self.assertEqual(result["zaisan"], "created")
        env, fields = kt.by_env(kt.created, "APP_ZAISAN")[0]
        self.assertNotIn("元レコードID", fields, "25なしでは元参照を書かない")

    def test_shipping_env_unset_skips_queue_but_responds(self):
        kt = _KT()
        resp = self.post(kt, env={**_ENV, "APP_SHIPPING": ""})  # 紐付け不能ケース
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["results"][0]["zaisan"], "needs_review")
        self.assertEqual(kt.by_env(kt.created, "APP_SHIPPING"), [])


if __name__ == "__main__":
    unittest.main()
