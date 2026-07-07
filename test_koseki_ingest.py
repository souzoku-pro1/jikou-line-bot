"""POST /koseki/ingest（R2・2026-07-05 裁定版）のテスト

検証: token 認証（不一致/未設定=404 の存在しないフリ）・env 未設定の安全側拒否・
登録フィールド（原本PDF/ページ画像/Drive_fileId/読解JSON/読解状態=未読解/案件ヒント）・
複数ページ画像・Drive_fileId 冪等 skip・既存 /scan（戸籍謄本→App 27）の回帰。
kintone / Vision / ページ画像化は全てモック。
"""

import hashlib
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ── main import 前に環境変数を差し込む（既存テストと同じ流儀） ────────────────
# ANTHROPIC_API_KEY は main の import にのみ必要。本ファイルはアルファベット順で
# test_triage_classification より先に import されるため、ダミーを環境に残すと
# 実 API 前提テストの skip ガードが誤解除される（test_hub_dispatch と同じ対処）
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

import koseki_ingest  # noqa: E402
import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]  # skip ガードの誤解除防止（test_hub_dispatch 参照）

client = TestClient(main.app)

URL = "/koseki/ingest"
PDF = b"%PDF-1.4 dummy koseki"
PDF_SHA = f"sha256:{hashlib.sha256(PDF).hexdigest()}"

# 有効化に必要な環境（token・App 33 env）。
# APP_SHIPPING は既定で空= R4-0 の要確認起票をスキップ（従来テストの挙動を固定。
# 他テストモジュールが process env に APP_SHIPPING を注入する漏れも遮断）。
# 起票の検証は TestCaseLinkReview 側で明示的に設定する
_ENV = {"KOSEKI_INGEST_TOKEN": "koseki_token",
        "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t",
        "APP_SHIPPING": "", "TOKEN_SHIPPING": ""}


class _Kintone:
    """hub.kintone のモック（App 33 のみを想定）"""

    def __init__(self, existing=()):
        self.existing = list(existing)
        self.created = []
        self.reviews = []           # App 30 要確認起票（R4-0）
        self.uploaded = []          # (filename, mime)
        self.search_queries = []

    async def search_records(self, app, query, fields=None):
        assert app.app_id_env == "APP_KOSEKI_BOOK"
        self.search_queries.append(query)
        return self.existing

    async def upload_file(self, app, filename, content, mime):
        self.uploaded.append((filename, mime))
        return f"fk-{len(self.uploaded)}"

    async def create_record(self, app, fields):
        if app.app_id_env == "APP_SHIPPING":  # R4-0 の要確認起票
            if getattr(self, "fail_shipping", False):
                raise RuntimeError("App 30 down")
            self.reviews.append(fields)
            return "30-1"
        assert app.app_id_env == "APP_KOSEKI_BOOK"
        self.created.append(fields)
        return "88"

    def patches(self):
        return [patch(f"hub.kintone.{name}", new=getattr(self, name))
                for name in ("search_records", "upload_file", "create_record")]


class _Base(unittest.TestCase):
    def post(self, kt: _Kintone, env: dict, data: dict | None = None,
             token: str | None = "koseki_token", pages=(b"png-1", b"png-2"),
             ocr_text="戸籍OCRテキスト"):
        self.ocr = MagicMock(return_value=ocr_text)
        patchers = [
            patch("koseki_ingest._ocr_pdf", new=self.ocr),
            patch("koseki_ingest._render_page_images",
                  new=MagicMock(return_value=list(pages))),
            patch.dict("os.environ", env),
            *kt.patches(),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        query = f"?token={token}" if token is not None else ""
        return client.post(
            URL + query,
            files={"file": ("koseki.pdf", PDF, "application/pdf")},
            data=data or {})


class TestAuth(_Base):
    def test_missing_token_is_404(self):
        resp = self.post(_Kintone(), _ENV, token=None)
        self.assertEqual(resp.status_code, 404, "存在しないフリ")

    def test_wrong_token_is_404(self):
        resp = self.post(_Kintone(), _ENV, token="wrong")
        self.assertEqual(resp.status_code, 404)

    def test_token_env_unset_is_404_deny_all(self):
        resp = self.post(_Kintone(), {**_ENV, "KOSEKI_INGEST_TOKEN": ""})
        self.assertEqual(resp.status_code, 404)

    def test_probe_without_body_is_404_not_422(self):
        """token 無し・body 無しの探信にも 404（file 必須の 422 で存在が漏れない・
        2026-07-05 実機で発見した回帰の固定）"""
        with patch.dict("os.environ", _ENV):
            resp = client.post(URL)
        self.assertEqual(resp.status_code, 404)

    def test_valid_token_without_file_is_400(self):
        """認証後のファイル欠落は 400 の明示エラー"""
        with patch.dict("os.environ", _ENV):
            resp = client.post(URL + "?token=koseki_token")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("PDF", resp.json()["detail"])


class TestEnvGuards(_Base):
    def test_app_env_unset_is_503_explicit(self):
        """APP_KOSEKI_BOOK 未設定は安全側の明示拒否（404 ではなく理由を返す）"""
        resp = self.post(_Kintone(), {**_ENV, "APP_KOSEKI_BOOK": ""})
        self.assertEqual(resp.status_code, 503)
        self.assertIn("APP_KOSEKI_BOOK", resp.json()["detail"])

    def test_token_env_of_app_unset_is_503(self):
        resp = self.post(_Kintone(), {**_ENV, "TOKEN_KOSEKI_BOOK": ""})
        self.assertEqual(resp.status_code, 503)

    def test_non_pdf_is_400(self):
        kt = _Kintone()
        for p in [patch.dict("os.environ", _ENV), *kt.patches()]:
            p.start()
            self.addCleanup(p.stop)
        resp = client.post(URL + "?token=koseki_token",
                           files={"file": ("photo.jpg", b"x", "image/jpeg")})
        self.assertEqual(resp.status_code, 400)


class TestRegistration(_Base):
    def test_registers_one_record_with_expected_fields(self):
        kt = _Kintone()
        resp = self.post(kt, _ENV)
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body, {"status": "ok", "kintone_record_id": "88",
                                "page_images": 2, "ocr_chars": len("戸籍OCRテキスト")})

        fields, = kt.created
        self.assertEqual(fields["原本PDF"], [{"fileKey": "fk-1"}])
        self.assertEqual(fields["ページ画像"],
                         [{"fileKey": "fk-2"}, {"fileKey": "fk-3"}],
                         "複数ページ分の画像が全て添付される")
        self.assertEqual(fields["Drive_fileId"], PDF_SHA, "省略時は sha256 由来")
        self.assertEqual(fields["読解状態"], "未読解")
        self.assertIn('"ocr_text"', fields["読解JSON"])
        self.assertIn("戸籍OCRテキスト", fields["読解JSON"])
        self.assertNotIn("案件レコードID", fields, "ヒント無しでは案件参照を書かない")
        self.assertNotIn("編製日", fields, "読解由来フィールドは R3 スコープ")
        # アップロードは PDF 1 + ページ画像 2
        self.assertEqual(kt.uploaded,
                         [("koseki.pdf", "application/pdf"),
                          ("page-001.png", "image/png"),
                          ("page-002.png", "image/png")])

    def test_case_hints_written_when_given(self):
        kt = _Kintone()
        self.post(kt, _ENV, data={"case_hint": "100", "case_app_hint": "26"})
        fields, = kt.created
        self.assertEqual(fields["案件レコードID"], "100")
        self.assertEqual(fields["案件アプリID"], "26")

    def test_explicit_drive_file_id_used_as_is(self):
        kt = _Kintone()
        self.post(kt, _ENV, data={"drive_file_id": "drive-abc"})
        fields, = kt.created
        self.assertEqual(fields["Drive_fileId"], "drive-abc")
        self.assertIn('Drive_fileId = "drive-abc"', kt.search_queries[0])

    def test_no_page_images_still_registers(self):
        """PyMuPDF 不在（画像化不能）でも登録は続行する（安全側）"""
        kt = _Kintone()
        resp = self.post(kt, _ENV, pages=())
        self.assertEqual(resp.json()["page_images"], 0)
        fields, = kt.created
        self.assertNotIn("ページ画像", fields)

    def test_empty_ocr_text_still_registers(self):
        """OCR 空でも登録する（空で返すより人の修正が速い・01 §2）"""
        kt = _Kintone()
        resp = self.post(kt, _ENV, ocr_text="")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(kt.created), 1)


class TestIdempotency(_Base):
    def test_same_drive_file_id_skips_without_ocr(self):
        kt = _Kintone(existing=[{"$id": {"value": "42"}}])
        resp = self.post(kt, _ENV)
        self.assertEqual(resp.json()["status"], "skip")
        self.assertEqual(resp.json()["kintone_record_id"], "42")
        self.assertEqual(kt.created, [], "再登録しない")
        self.ocr.assert_not_called()
        self.assertEqual(kt.uploaded, [], "アップロードもしない")


class TestCaseLinkReview(_Base):
    """R4-0: 案件参照が埋まらなかった戸籍の App 30 要確認起票（S5 と同じ封筒形式）"""

    _LINK_ENV = {**_ENV, "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30"}

    def test_no_case_hint_files_review_with_envelope(self):
        kt = _Kintone()
        resp = self.post(kt, self._LINK_ENV, data={"drive_file_id": "drv-1"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["review_record_id"], "30-1")
        review, = kt.reviews
        self.assertEqual(review["発送ステータス"], "要確認")
        self.assertEqual(review["方向"], "受領")
        self.assertEqual(review["チャネル"], "スキャン受領")
        self.assertEqual(review["実行済み"], "no")
        self.assertIn("案件紐付け不能", review["件名"])
        channel = json.loads(review["チャネル固有データ"])
        self.assertEqual(list(channel), ["koseki_ingest"], "トップキー=koseki_ingest")
        self.assertEqual(channel["koseki_ingest"]["戸籍レコードID"], "88")
        self.assertEqual(channel["koseki_ingest"]["冪等キー"], "drv-1")
        self.assertIn("成果物", review, "原本PDFを添付")

    def test_case_hint_given_does_not_file_review(self):
        kt = _Kintone()
        resp = self.post(kt, self._LINK_ENV, data={"case_hint": "100"})
        self.assertEqual(kt.reviews, [])
        self.assertNotIn("review_record_id", resp.json())

    def test_shipping_env_unset_skips_filing(self):
        kt = _Kintone()
        resp = self.post(kt, _ENV)  # APP_SHIPPING 空
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(kt.reviews, [])
        self.assertNotIn("review_record_id", resp.json())

    def test_filing_failure_does_not_break_ingest(self):
        kt = _Kintone()
        kt.fail_shipping = True
        resp = self.post(kt, self._LINK_ENV)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["kintone_record_id"], "88")
        self.assertNotIn("review_record_id", resp.json())


class TestPersonSyncWiring(_Base):
    """R4-1 結線の補修: 案件付き登録（回送等）でも人物化が起動する
    （KOSEKI_PERSON_SYNC_ENABLED 配下・失敗は登録成功を壊さない）"""

    _SYNC_ENV = {**_ENV, "KOSEKI_PERSON_SYNC_ENABLED": "1"}

    def _post_with_sync(self, env, sync, data=None):
        p = patch("koseki_person_sync.sync_persons_from_koseki", new=sync)
        p.start()
        self.addCleanup(p.stop)
        return self.post(_Kintone(), env, data=data)

    def test_case_hint_with_flag_runs_person_sync(self):
        sync = AsyncMock(return_value={"status": "synced",
                                       "created": [{"氏名": "鈴木太郎"}],
                                       "skipped": []})
        resp = self._post_with_sync(self._SYNC_ENV, sync,
                                    data={"case_hint": "4"})
        sync.assert_awaited_once_with("88")
        self.assertEqual(resp.json()["persons"]["status"], "synced")

    def test_flag_off_does_not_sync(self):
        sync = AsyncMock()
        resp = self._post_with_sync(_ENV, sync, data={"case_hint": "4"})
        sync.assert_not_awaited()
        self.assertNotIn("persons", resp.json())

    def test_no_case_hint_does_not_sync_even_with_flag(self):
        """案件なしは要確認起票経路のみ（人物化は関所の確定後・従来どおり）"""
        sync = AsyncMock()
        resp = self._post_with_sync(self._SYNC_ENV, sync)
        sync.assert_not_awaited()
        self.assertNotIn("persons", resp.json())

    def test_sync_failure_does_not_break_registration(self):
        sync = AsyncMock(side_effect=RuntimeError("boom"))
        resp = self._post_with_sync(self._SYNC_ENV, sync,
                                    data={"case_hint": "4"})
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["kintone_record_id"], "88", "登録成功は不変")
        self.assertEqual(body["persons"]["status"], "error")


class TestExistingScanRegression(unittest.TestCase):
    """既存 /scan（戸籍謄本→App 27）が R2 の追加後も不変であることの固定"""

    def test_unknown_folder_still_400(self):
        resp = client.post("/scan", json={"pdf_base64": "eA==",
                                          "folder_name": "不明フォルダ"})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("未対応のフォルダ名", resp.json()["detail"])

    def test_koseki_folder_still_posts_to_app27(self):
        """戸籍謄本フォルダは従来どおり App 27（KOSEKI_KINTONE_APP_ID）へ登録"""
        extracted = {"氏名": "山田太郎", "本籍": "川口市", "筆頭者": "山田太郎"}
        post_27 = AsyncMock(return_value="27-1")
        patchers = [
            patch("main._ocr_pdf_bytes", new=MagicMock(return_value="OCR")),
            patch("main._extract_by_folder",
                  new=AsyncMock(return_value=dict(extracted))),
            patch("main._post_scan_to_kintone", new=post_27),
            patch.object(main, "GOOGLE_VISION_API_KEY", "dummy_vision"),
            patch.dict("os.environ", {"KOSEKI_KINTONE_APP_ID": "27",
                                      "KOSEKI_KINTONE_API_TOKEN": "t27"}),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        resp = client.post("/scan", json={
            "pdf_base64": "eA==", "folder_name": "戸籍謄本",
            "file_name": "koseki.pdf"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ok")
        app_id, api_token, fields = post_27.await_args.args
        self.assertEqual((app_id, api_token), ("27", "t27"),
                         "登録先が App 27 のまま（App 33 に変わっていない）")
        self.assertEqual(fields["氏名"], "山田太郎")


if __name__ == "__main__":
    unittest.main()
