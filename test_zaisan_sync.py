"""S4 課税明細ライン拡張（/ocr/fixed-asset 追記型・units/souzoku/zaisan_sync）のテスト

検証（05 §3 S4 の完了条件）:
- 既存動作（不動産25上書き）の完全回帰: 追加処理無効時（ZAISAN_SYNC_DISABLED=1 /
  APP_ZAISAN 未設定）はレスポンス・kintone 呼び出しとも現状と完全同一
- case_hint あり → App 財産へ upsert（新規作成・同一キーの再送は上書き更新）
- case_hint なし → 過去の財産行から逆引き、不能なら要確認キュー（App 30）
- 所在検索の複数ヒット → 備考記録＋要確認起票（先頭採用の既存挙動は不変）
- 追加処理の失敗が既存処理の成功応答を壊さない
kintone は全てモック（App 財産は未作成）。
"""

import hashlib
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ── main import 前に環境変数を差し込む（既存テストと同じ流儀） ────────────────
os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "ANTHROPIC_API_KEY": "dummy_key",
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
    "KINTONE_FUDOSAN_APP_ID": "25",
    "KINTONE_FUDOSAN_API_TOKEN": "dummy",
    "HEALTHCHECK_DISABLED": "1",
})

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from units.souzoku import zaisan_sync  # noqa: E402

client = TestClient(main.app)

URL = "/ocr/fixed-asset"
PDF = b"%PDF-1.4 dummy tax statement"
PDF_SHA = hashlib.sha256(PDF).hexdigest()

EXTRACTED = {"評価額": 12345678, "年度": 2025,
             "所在地": "埼玉県川口市朝日1丁目", "地番": "12-3"}

FUDOSAN_RECORD = {
    "$id": {"value": "77"},
    "種別": {"value": "土地"},
    "所在": {"value": "川口市朝日一丁目"},
    "地番": {"value": "12番3"},
    "地目": {"value": "宅地"},
    "地積": {"value": "120.45"},
}

# 追加処理が有効になる環境（App 財産・App 30 の env あり）
_SYNC_ENV = {"APP_ZAISAN": "35", "TOKEN_ZAISAN": "t",
             "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t"}


class _KintoneMocks:
    """hub.kintone のアプリ別モック（App 25 / App 財産 / App 30 を振り分け）"""

    def __init__(self, *, fudosan_hits=1, zaisan_case_rows=(), zaisan_existing=()):
        self.created = []            # (app_id_env, fields)
        self.updated = []            # (app_id_env, record_id, fields)
        self.zaisan_queries = []
        self.fudosan_hits = fudosan_hits
        self.zaisan_case_rows = list(zaisan_case_rows)   # 逆引き結果
        self.zaisan_existing = list(zaisan_existing)     # upsert 既存行

    async def search_records(self, app, query, fields=None):
        if app.app_id_env == "KINTONE_FUDOSAN_APP_ID":
            return [{"$id": {"value": str(70 + i)}} for i in range(self.fudosan_hits)]
        if app.app_id_env == "APP_ZAISAN":
            self.zaisan_queries.append(query)
            if "元アプリID" in query:      # 逆引き
                return self.zaisan_case_rows
            return self.zaisan_existing    # upsert キー検索
        raise AssertionError(f"想定外の検索: {app.label}")

    async def get_record(self, app, record_id):
        assert app.app_id_env == "KINTONE_FUDOSAN_APP_ID"
        assert record_id == "77"
        return FUDOSAN_RECORD

    async def create_record(self, app, fields):
        self.created.append((app.app_id_env, fields))
        return "900" if app.app_id_env == "APP_ZAISAN" else "300"

    async def update_record(self, app, record_id, fields, revision=None):
        self.updated.append((app.app_id_env, record_id, fields))

    async def upload_file(self, app, filename, content, mime):
        return "fk-1"

    def patches(self):
        return [patch(f"hub.kintone.{name}", new=getattr(self, name))
                for name in ("search_records", "get_record", "create_record",
                             "update_record", "upload_file")]


class _Base(unittest.TestCase):
    """既存処理（main 内部）のモックを共通化"""

    def post(self, mocks: _KintoneMocks, env: dict, data: dict | None = None):
        self.update_25 = AsyncMock()
        patchers = [
            patch("main._ocr_pdf_bytes", new=MagicMock(return_value="OCRテキスト")),
            patch("main._extract_fixed_asset",
                  new=AsyncMock(return_value=dict(EXTRACTED))),
            patch("main._search_kintone_record", new=AsyncMock(return_value="77")),
            patch("main._update_kintone_record", new=self.update_25),
            # main のモジュール定数は最初に import したテストの env で固定される
            # ため、全体実行の import 順に依存しないよう直接 patch する
            patch.object(main, "GOOGLE_VISION_API_KEY", "dummy_vision"),
            patch.object(main, "KINTONE_FUDOSAN_DOMAIN", "testsub"),
            patch.object(main, "KINTONE_FUDOSAN_APP_ID_OCR", "25"),
            patch.object(main, "KINTONE_FUDOSAN_API_TOKEN_OCR", "dummy"),
            patch.dict("os.environ", {"KINTONE_FUDOSAN_APP_ID": "25",
                                      "SOUZOKU_KINTONE_APP_ID": "26", **env}),
            *mocks.patches(),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        return client.post(
            URL, files={"file": ("meisai.pdf", PDF, "application/pdf")},
            data=data or {})


class TestRegressionWhenDisabled(_Base):
    """追加処理無効時: レスポンス・kintone 呼び出しとも現状と完全同一"""

    def _assert_legacy_only(self, resp, mocks):
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {           # キー集合ごと完全一致（新キー無し）
            "status": "ok",
            "kintone_record_id": "77",
            "extracted": EXTRACTED,
        })
        self.update_25.assert_awaited_once()       # 不動産25の上書きは従来どおり
        self.assertEqual(mocks.created, [], "App 財産・App 30 への書き込みなし")
        self.assertEqual(mocks.updated, [])

    def test_zaisan_sync_disabled_flag(self):
        mocks = _KintoneMocks()
        resp = self.post(mocks, {**_SYNC_ENV, "ZAISAN_SYNC_DISABLED": "1"})
        self._assert_legacy_only(resp, mocks)

    def test_app_zaisan_env_unset_skips_safely(self):
        mocks = _KintoneMocks()
        resp = self.post(mocks, {"APP_ZAISAN": ""})   # 未設定でも既存動作は完走
        self._assert_legacy_only(resp, mocks)


class TestUpsertWithCaseHint(_Base):
    def test_creates_new_zaisan_row(self):
        mocks = _KintoneMocks()
        resp = self.post(mocks, _SYNC_ENV, data={"case_hint": "100"})
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["kintone_record_id"], "77", "既存レスポンスは不変")
        self.assertEqual(body["zaisan_sync"]["status"], "created")
        self.assertEqual(body["zaisan_sync"]["zaisan_record_id"], "900")
        self.assertEqual(body["zaisan_sync"]["case_via"], "case_hint")
        self.update_25.assert_awaited_once()

        (app_env, fields), = mocks.created
        self.assertEqual(app_env, "APP_ZAISAN")
        self.assertEqual(fields["案件レコードID"], "100")
        self.assertEqual(fields["案件アプリID"], "26")
        self.assertEqual(fields["財産種別"], "不動産_土地")
        self.assertEqual(fields["特定情報"],
                         "所在 川口市朝日一丁目 / 地番 12番3 / 地目 宅地 / 地積 120.45㎡")
        self.assertEqual(fields["評価額"], "12345678")
        self.assertEqual(fields["評価方法"], "固定資産税評価額")
        self.assertEqual(fields["評価基準日"], "2025-01-01", "賦課期日=年度の1月1日")
        self.assertEqual(fields["データ源"], "OCR_課税明細")
        self.assertEqual(fields["元アプリID"], "25")
        self.assertEqual(fields["元レコードID"], "77")
        self.assertEqual(fields["冪等キー"], PDF_SHA)
        self.assertEqual(fields["原本"], [{"fileKey": "fk-1"}])
        self.assertEqual(fields["評価確定"], "no", "新規作成時のみ初期値を設定")
        self.assertEqual(fields["有効"], "yes")
        self.assertNotIn("備考", fields, "単一ヒットでは複数ヒット記録なし")

    def test_updates_existing_row_with_same_key(self):
        """upsert キー (案件, 元レコードID, 評価基準日) 一致 → 上書き更新（再送）"""
        mocks = _KintoneMocks(zaisan_existing=[{"$id": {"value": "555"}}])
        resp = self.post(mocks, _SYNC_ENV, data={"case_hint": "100"})
        self.assertEqual(resp.json()["zaisan_sync"]["status"], "updated")
        self.assertEqual(mocks.created, [], "新規作成しない")
        (app_env, rid, fields), = mocks.updated
        self.assertEqual((app_env, rid), ("APP_ZAISAN", "555"))
        self.assertNotIn("評価確定", fields, "更新時は弁護士の評価確定を触らない")
        self.assertNotIn("有効", fields)
        # upsert キーの検索条件（3要素）
        upsert_query = mocks.zaisan_queries[-1]
        self.assertIn('案件レコードID = "100"', upsert_query)
        self.assertIn('元レコードID = "77"', upsert_query)
        self.assertIn('評価基準日 = "2025-01-01"', upsert_query)

    def test_case_hint_skips_reverse_lookup(self):
        mocks = _KintoneMocks()
        self.post(mocks, _SYNC_ENV, data={"case_hint": "100"})
        self.assertFalse(any("元アプリID" in q for q in mocks.zaisan_queries),
                         "case_hint ありでは逆引き検索しない")


class TestCaseResolutionWithoutHint(_Base):
    def test_reverse_lookup_from_past_zaisan_row(self):
        """case_hint なし → 過去の財産行から案件を逆引き（02 §3 Step 1）"""
        mocks = _KintoneMocks(
            zaisan_case_rows=[{"案件レコードID": {"value": "200"}}])
        resp = self.post(mocks, _SYNC_ENV)
        body = resp.json()["zaisan_sync"]
        self.assertEqual(body["status"], "created")
        self.assertEqual(body["case_record_id"], "200")
        self.assertEqual(body["case_via"], "逆引き")

    def test_unresolvable_goes_to_review_queue(self):
        """紐付け不能 → App 財産に登録せず App 30 要確認へ（02 §3・§5）"""
        mocks = _KintoneMocks()
        resp = self.post(mocks, _SYNC_ENV)
        body = resp.json()
        self.assertEqual(body["kintone_record_id"], "77", "既存処理は完了している")
        self.assertEqual(body["zaisan_sync"]["status"], "needs_review")
        self.assertEqual(body["zaisan_sync"]["review_record_id"], "300")
        (app_env, fields), = mocks.created
        self.assertEqual(app_env, "APP_SHIPPING", "App 財産には登録しない")
        self.assertEqual(fields["発送ステータス"], "要確認")
        self.assertEqual(fields["方向"], "受領")
        self.assertEqual(fields["チャネル"], "スキャン受領")
        self.assertIn("案件紐付け不能", fields["件名"])
        self.assertEqual(fields["成果物"], [{"fileKey": "fk-1"}], "受領PDFを添付")


class TestMultipleHits(_Base):
    def test_multi_hit_recorded_and_review_filed(self):
        """複数ヒット: 先頭採用は不変・備考に記録・要確認にも起票（方針4）"""
        mocks = _KintoneMocks(fudosan_hits=3)
        resp = self.post(mocks, _SYNC_ENV, data={"case_hint": "100"})
        body = resp.json()["zaisan_sync"]
        self.assertEqual(body["status"], "created", "upsert 自体は実行される")
        self.assertEqual(body["hit_count"], 3)
        self.assertEqual(body["review_record_id"], "300")
        self.update_25.assert_awaited_once_with("77", EXTRACTED)  # 先頭採用のまま

        by_app = dict(mocks.created)
        self.assertIn("所在検索3件ヒット・先頭採用", by_app["APP_ZAISAN"]["備考"])
        self.assertEqual(by_app["APP_SHIPPING"]["発送ステータス"], "要確認")
        self.assertIn("複数ヒット", by_app["APP_SHIPPING"]["件名"])


class TestSyncFailureDoesNotBreakLegacy(_Base):
    def test_sync_exception_returns_ok_with_error_detail(self):
        mocks = _KintoneMocks()
        with patch("units.souzoku.zaisan_sync.sync_fixed_asset",
                   new=AsyncMock(side_effect=RuntimeError("boom"))):
            resp = self.post(mocks, _SYNC_ENV, data={"case_hint": "100"})
        body = resp.json()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["kintone_record_id"], "77")
        self.assertEqual(body["zaisan_sync"]["status"], "error")
        self.assertIn("boom", body["zaisan_sync"]["detail"])


class TestHelpers(unittest.TestCase):
    def test_kijunbi_is_january_first_of_nendo(self):
        self.assertEqual(zaisan_sync._kijunbi(2025), "2025-01-01")
        self.assertEqual(zaisan_sync._kijunbi(None), "")

    def test_tokutei_joho_skips_missing_fields(self):
        record = {"所在": {"value": "川口市"}, "地番": {"value": "12番3"},
                  "地目": {"value": ""}, "地積": {"value": ""}}
        self.assertEqual(zaisan_sync._tokutei_joho(record),
                         "所在 川口市 / 地番 12番3")


if __name__ == "__main__":
    unittest.main()
