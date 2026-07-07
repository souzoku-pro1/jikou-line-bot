"""POST /sortation/ingest（書類仕分け第1段 T2+T3）のテスト

検証: token 認証（404 の存在しないフリ・探信 422 回避）・非PDF 400・
auto/ask 分岐としきい値境界（env 上書き含む）・候補0件・リスト外 record_id の棄却・
LINE 照会通知の内容と縮退（宛先未設定・送信失敗）・OCR/Claude 断の安全側縮退・
drive_file_id 重複のログ検知（挙動不変）。OCR / Claude / kintone / LINE は全てモック。
"""

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ── main import 前に環境変数を差し込む（test_koseki_ingest と同じ流儀） ──────
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

import sortation_ingest  # noqa: E402
from customer_directory import Candidate  # noqa: E402
import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]  # skip ガードの誤解除防止

client = TestClient(main.app)

URL = "/sortation/ingest"
PDF = b"%PDF-1.4 dummy sortation"

_ENV = {"SORTATION_INGEST_TOKEN": "sort_token",
        "ATTORNEY_LINE_USER_ID": "U-attorney"}


def cand(rec_id, name, decedent="", status=""):
    return Candidate(record_id=str(rec_id), app_id="26", source="相談カード (相続)",
                     customer_name=name, decedent_name=decedent, status=status)


class _Base(unittest.TestCase):
    """OCR / 候補 / Claude 判定 / LINE / 日付 をモックして POST する共通足場"""

    def post(self, env=None, judged=None, candidates=(), token="sort_token",
             ocr_text="山田太郎 様の書類", data=None, judge_error=None,
             ocr_error=None, push_result=True, push_error=None,
             filename="scan001.pdf", content_type="application/pdf"):
        self.push = AsyncMock(return_value=push_result,
                              side_effect=push_error)
        judge = AsyncMock(return_value=judged, side_effect=judge_error)
        ocr = MagicMock(return_value=ocr_text, side_effect=ocr_error)
        patchers = [
            patch("sortation_ingest._ocr_pdf", new=ocr),
            patch("sortation_ingest.list_candidates",
                  new=AsyncMock(return_value=list(candidates))),
            patch("sortation_ingest._judge_with_claude", new=judge),
            patch("sortation_ingest.push_line_message", new=self.push),
            patch("sortation_ingest._today_jst",
                  new=MagicMock(return_value="20260706")),
            patch.dict("os.environ", env if env is not None else _ENV),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        query = f"?token={token}" if token is not None else ""
        return client.post(URL + query,
                           files={"file": (filename, PDF, content_type)},
                           data=data or {})

    def line_text(self) -> str:
        self.assertEqual(self.push.await_count, 1, "LINE 照会通知が1回送られる")
        return self.push.await_args.args[1]


class TestAuth(_Base):
    def test_missing_token_is_404(self):
        self.assertEqual(self.post(token=None).status_code, 404, "存在しないフリ")

    def test_wrong_token_is_404(self):
        self.assertEqual(self.post(token="wrong").status_code, 404)

    def test_token_env_unset_is_404_deny_all(self):
        resp = self.post(env={**_ENV, "SORTATION_INGEST_TOKEN": ""})
        self.assertEqual(resp.status_code, 404)

    def test_probe_without_body_is_404_not_422(self):
        """探信に file 必須の 422 を返さない（koseki_ingest の実機回帰と同じ固定）"""
        with patch.dict("os.environ", _ENV):
            self.assertEqual(client.post(URL).status_code, 404)

    def test_valid_token_without_file_is_400(self):
        with patch.dict("os.environ", _ENV):
            resp = client.post(URL + "?token=sort_token")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("PDF", resp.json()["detail"])

    def test_non_pdf_is_400(self):
        resp = self.post(filename="photo.jpg", content_type="image/jpeg")
        self.assertEqual(resp.status_code, 400)


class TestAutoBranch(_Base):
    JUDGED = {"doc_type": "評価証明・課税明細", "customer_record_id": "12",
              "confidence": 0.93, "reason": "宛名と被相続人名が一致"}

    def test_auto_response_contract(self):
        resp = self.post(judged=self.JUDGED,
                         candidates=[cand(12, "山田太郎", "山田一郎")])
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {
            "action": "auto",
            "doc_type": "評価証明・課税明細",
            "confidence": 0.93,
            "customer": {"record_id": "12", "name": "山田太郎",
                         "folder_name": "No12_山田太郎"},
            "suggested_filename": "山田太郎_評価証明・課税明細_20260706.pdf",
        })
        self.assertEqual(self.push.await_count, 0, "auto は照会通知しない")

    def test_threshold_boundary_equal_is_auto(self):
        """confidence == しきい値（既定0.85）は auto（>= 判定）"""
        judged = {**self.JUDGED, "confidence": 0.85}
        resp = self.post(judged=judged, candidates=[cand(12, "山田太郎")])
        self.assertEqual(resp.json()["action"], "auto")

    def test_threshold_boundary_just_below_is_ask(self):
        judged = {**self.JUDGED, "confidence": 0.8499}
        resp = self.post(judged=judged, candidates=[cand(12, "山田太郎")])
        self.assertEqual(resp.json()["action"], "ask")
        self.assertEqual(self.push.await_count, 1)

    def test_threshold_env_override(self):
        """SORTATION_AUTO_THRESHOLD の env 上書き（0.95 なら 0.93 は ask）"""
        resp = self.post(env={**_ENV, "SORTATION_AUTO_THRESHOLD": "0.95"},
                         judged=self.JUDGED, candidates=[cand(12, "山田太郎")])
        self.assertEqual(resp.json()["action"], "ask")

    def test_high_confidence_without_customer_is_ask(self):
        """customer 未確定なら confidence が高くても auto にしない"""
        judged = {**self.JUDGED, "customer_record_id": None, "confidence": 0.99}
        resp = self.post(judged=judged, candidates=[cand(12, "山田太郎")])
        self.assertEqual(resp.json()["action"], "ask")
        self.assertIsNone(resp.json()["customer"])
        self.assertIsNone(resp.json()["suggested_filename"])


class TestDocTypeOptions(unittest.TestCase):
    """doc_type 候補の拡充（2026-07-06 誤判定修正）の固定"""

    EXPECTED = ["戸籍", "住民票・戸籍附票", "評価証明・課税明細", "登記事項証明",
                "残高証明", "通帳", "保険", "契約書", "委任状", "印鑑証明書",
                "遺言書", "通知書・連絡文書", "請求書・領収書", "本人確認書類",
                "その他"]

    def test_doc_types_are_expanded_15(self):
        self.assertEqual(sortation_ingest.DOC_TYPES, self.EXPECTED)
        enum = sortation_ingest.JUDGE_TOOL["input_schema"]["properties"][
            "doc_type"]["enum"]
        self.assertEqual(enum, self.EXPECTED, "tool スキーマの enum も同一リスト")

    def test_doc_types_are_filename_safe(self):
        """doc_type は suggested_filename にそのまま入るため、ファイル名に
        使えない文字を含まないこと"""
        for t in sortation_ingest.DOC_TYPES:
            for ch in '/\\:*?"<>|':
                self.assertNotIn(ch, t, t)

    def test_schema_instructs_not_to_force_nearest(self):
        """「近そうな候補に寄せない」の指示が description に入っていること"""
        desc = sortation_ingest.JUDGE_TOOL["input_schema"]["properties"][
            "doc_type"]["description"]
        self.assertIn("その他", desc)
        self.assertIn("近そうな候補に寄せない", desc)


class TestAskBranchAndNotify(_Base):
    def test_ask_notification_content(self):
        """照会通知にファイル名・Driveリンク・理由・候補label・確信度が載る"""
        judged = {"doc_type": "戸籍", "customer_record_id": None,
                  "confidence": 0.4, "reason": "宛名がOCRで読めない"}
        resp = self.post(env={**_ENV, "SORTATION_ASK_TO": "",
                              "DISPATCHBOT_ALLOWED_USER_IDS": ""},
                         judged=judged,
                         candidates=[cand(12, "山田太郎", "山田一郎")],
                         ocr_text="山田太郎 様",
                         data={"drive_file_url": "https://drive.google.com/x"})
        self.assertEqual(resp.json()["action"], "ask")
        text = self.line_text()
        self.assertEqual(self.push.await_args.args[0], "U-attorney")
        for expected in ("scan001.pdf", "https://drive.google.com/x",
                         "戸籍", "0.40", "宛名がOCRで読めない",
                         "山田太郎（被相続人: 山田一郎・No.12・相談カード (相続)）"):
            self.assertIn(expected, text)

    def test_zero_candidates_is_ask_with_no_candidate_line(self):
        """候補0件: 必ず ask・通知は「候補: 該当なし」"""
        judged = {"doc_type": "通帳", "customer_record_id": None,
                  "confidence": 0.2, "reason": "候補リストが空"}
        resp = self.post(judged=judged, candidates=[])
        self.assertEqual(resp.json()["action"], "ask")
        self.assertIn("候補: 該当なし", self.line_text())

    def test_record_id_outside_roster_is_rejected(self):
        """Claude がリスト外の record_id を創作したら棄却（customer=null・ask）"""
        judged = {"doc_type": "戸籍", "customer_record_id": "999",
                  "confidence": 0.95, "reason": "x"}
        resp = self.post(judged=judged, candidates=[cand(12, "山田太郎")])
        body = resp.json()
        self.assertEqual(body["action"], "ask")
        self.assertIsNone(body["customer"])
        self.assertEqual(body["confidence"], 0.0)
        self.assertIn("候補リスト外", self.line_text())

    def test_notify_failure_does_not_break_ask_response(self):
        """LINE 送信の例外は縮退（ask 応答は 200 のまま）"""
        judged = {"doc_type": "戸籍", "customer_record_id": None,
                  "confidence": 0.3, "reason": "x"}
        resp = self.post(judged=judged, push_error=RuntimeError("LINE down"))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["action"], "ask")

    def test_recipient_envs_all_unset_skips_notify_but_responds(self):
        """宛先3系統すべて未設定なら通知スキップ（応答は ask のまま・既存縮退）"""
        judged = {"doc_type": "戸籍", "customer_record_id": None,
                  "confidence": 0.3, "reason": "x"}
        resp = self.post(env={"SORTATION_INGEST_TOKEN": "sort_token",
                              "SORTATION_ASK_TO": "",
                              "DISPATCHBOT_ALLOWED_USER_IDS": "",
                              "ATTORNEY_LINE_USER_ID": ""}, judged=judged)
        self.assertEqual(resp.json()["action"], "ask")
        self.assertEqual(self.push.await_count, 0)


class TestAskChannelAndRecipient(_Base):
    """照会通知のチャネル（業務指示Bot名義）と宛先解決順（2026-07-06 裁定）"""

    JUDGED = {"doc_type": "戸籍", "customer_record_id": None,
              "confidence": 0.3, "reason": "x"}

    def test_sends_via_dispatch_bot_channel(self):
        """token_env=DISPATCHBOT_CHANNEL_ACCESS_TOKEN で送る（顧客Bot名義にしない）"""
        self.post(judged=self.JUDGED)
        self.assertEqual(self.push.await_args.kwargs.get("token_env"),
                         "DISPATCHBOT_CHANNEL_ACCESS_TOKEN")

    def test_recipient_priority_1_explicit_env(self):
        """SORTATION_ASK_TO が最優先"""
        resp = self.post(env={**_ENV, "SORTATION_ASK_TO": "U-explicit",
                              "DISPATCHBOT_ALLOWED_USER_IDS": "U-owner1,U-owner2"},
                         judged=self.JUDGED)
        self.assertEqual(resp.json()["action"], "ask")
        self.assertEqual(self.push.await_args.args[0], "U-explicit")

    def test_recipient_priority_2_allowed_first(self):
        """SORTATION_ASK_TO 空なら DISPATCHBOT_ALLOWED_USER_IDS の先頭"""
        resp = self.post(env={**_ENV, "SORTATION_ASK_TO": "",
                              "DISPATCHBOT_ALLOWED_USER_IDS": " U-owner1 , U-owner2 "},
                         judged=self.JUDGED)
        self.assertEqual(resp.json()["action"], "ask")
        self.assertEqual(self.push.await_args.args[0], "U-owner1")

    def test_recipient_priority_3_attorney_fallback(self):
        """上2つが空なら ATTORNEY_LINE_USER_ID"""
        resp = self.post(env={**_ENV, "SORTATION_ASK_TO": "",
                              "DISPATCHBOT_ALLOWED_USER_IDS": ""},
                         judged=self.JUDGED)
        self.assertEqual(resp.json()["action"], "ask")
        self.assertEqual(self.push.await_args.args[0], "U-attorney")


_LOG_ENV = {**_ENV, "APP_SORTATION_LOG": "38", "TOKEN_SORTATION_LOG": "t38"}


class TestSortationLog(_Base):
    """仕分けログ（App 38）登録（第2段②）: ask 時のみ・env 縮退・失敗縮退"""

    JUDGED = {"doc_type": "戸籍", "customer_record_id": None,
              "confidence": 0.4, "reason": "宛名がOCRで読めない"}

    def post_with_log(self, create=None, env=_LOG_ENV, judged=None, **kw):
        self.create = create if create is not None else AsyncMock(return_value="7")
        p = patch("hub.kintone.create_record", new=self.create)
        p.start()
        self.addCleanup(p.stop)
        return self.post(env=env, judged=judged or self.JUDGED, **kw)

    def test_ask_registers_log_record(self):
        resp = self.post_with_log(
            candidates=[cand(12, "山田太郎", "山田一郎")], ocr_text="山田太郎 様",
            data={"drive_file_id": "drv-9", "drive_file_url": "https://drive/x"})
        self.assertEqual(resp.json()["action"], "ask")
        app, fields = self.create.await_args.args
        self.assertEqual(app.app_id_env, "APP_SORTATION_LOG")
        self.assertEqual(fields, {
            "ファイル名": "scan001.pdf",
            "Drive_fileId": "drv-9",
            "Drive_URL": "https://drive/x",
            "書類種類": "戸籍",
            "確信度": "0.4",
            "判定理由": "宛名がOCRで読めない",
            "候補一覧": "山田太郎（被相続人: 山田一郎・No.12・相談カード (相続)）",
            "状態": "照会中",
        })
        self.assertIn("仕分けログ: https://testsub.cybozu.com/k/38/show#record=7",
                      self.line_text())

    def test_log_env_unset_skips_registration_and_keeps_notify(self):
        """env 未設定は登録スキップ＝従来どおり LINE 通知のみ（回帰維持）"""
        resp = self.post_with_log(env=_ENV)  # APP_SORTATION_LOG なし
        self.assertEqual(resp.json()["action"], "ask")
        self.assertEqual(self.create.await_count, 0)
        self.assertNotIn("仕分けログ:", self.line_text())

    def test_log_failure_keeps_notify_without_url(self):
        """登録失敗でも照会通知は送る（URL 行なし）"""
        from hub.kintone import KintoneError
        resp = self.post_with_log(create=AsyncMock(side_effect=KintoneError(500)))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["action"], "ask")
        self.assertNotIn("仕分けログ:", self.line_text())

    def test_auto_does_not_register_log(self):
        judged = {"doc_type": "戸籍", "customer_record_id": "12",
                  "confidence": 0.95, "reason": "x"}
        resp = self.post_with_log(judged=judged, candidates=[cand(12, "山田太郎")])
        self.assertEqual(resp.json()["action"], "auto")
        self.assertEqual(self.create.await_count, 0)


_FWD_ENV = {**_ENV, "SORTATION_FORWARD_ENABLED": "1",
            "SOUZOKU_KINTONE_APP_ID": "26"}


class TestForwarding(_Base):
    """S5-3 T1: doc_type別の読解ライン回送（auto×種別×doc_type_confidence ゲート）"""

    def judged(self, doc_type="戸籍", dtc=0.95):
        return {"doc_type": doc_type, "doc_type_confidence": dtc,
                "customer_record_id": "12", "confidence": 0.93,
                "reason": "宛名一致"}

    def post_fwd(self, judged, env=_FWD_ENV, koseki=None, registry=None, **kw):
        self.koseki = koseki if koseki is not None else AsyncMock(
            return_value={"status": "ok", "kintone_record_id": "33-9"})
        self.registry = registry if registry is not None else AsyncMock(
            return_value={"status": "ok", "results": [{"zaisan": "created"}]})
        self.valuation = AsyncMock(
            return_value={"status": "ok", "results": [{"zaisan": "updated"}]})
        for p in [patch("koseki_ingest.ingest_koseki_pdf", new=self.koseki),
                  patch("registry_ingest.ingest_registry_pdf", new=self.registry),
                  patch("valuation_ingest.ingest_valuation_pdf",
                        new=self.valuation)]:
            p.start()
            self.addCleanup(p.stop)
        return self.post(env=env, judged=judged,
                         candidates=[cand(12, "山田太郎")], **kw)

    def test_koseki_forward_with_case_hint_and_idempotency_passthrough(self):
        resp = self.post_fwd(self.judged(), data={"drive_file_id": "drv-9"})
        body = resp.json()
        self.assertEqual(body["action"], "auto")
        self.assertEqual(body["forwarded"],
                         {"line": "koseki", "status": "ok",
                          "kintone_record_id": "33-9"})
        (pdf_bytes, fname), kwargs = self.koseki.await_args
        self.assertEqual(pdf_bytes, PDF, "sortation が持つ pdf_bytes を流用")
        self.assertEqual(kwargs["case_hint"], "12", "確定顧客のレコードID")
        self.assertEqual(kwargs["case_app_hint"], "26")
        self.assertEqual(kwargs["drive_file_id"], "drv-9", "冪等キー貫通")

    def test_registry_forward(self):
        resp = self.post_fwd(self.judged(doc_type="登記事項証明"))
        body = resp.json()
        self.assertEqual(body["forwarded"]["line"], "registry")
        _, kwargs = self.registry.await_args
        self.assertEqual(kwargs["case_hint"], "12")
        # drive_file_id 省略時は空のまま渡し、下流の ingest 中核が同一 bytes から
        # sha256 冪等キーを導出する（sortation 単独投入と同値になる）
        self.assertEqual(kwargs["drive_file_id"], "")
        self.koseki.assert_not_awaited()

    def test_flag_off_by_default_no_forward_and_contract_unchanged(self):
        """フラグ既定無効: forwarded キー自体が無い（既存契約そのまま）"""
        resp = self.post_fwd(self.judged(), env={**_ENV,
                                                 "SOUZOKU_KINTONE_APP_ID": "26"})
        self.assertNotIn("forwarded", resp.json())
        self.koseki.assert_not_awaited()

    def test_below_threshold_no_forward(self):
        resp = self.post_fwd(self.judged(dtc=0.84))
        self.assertNotIn("forwarded", resp.json())
        self.koseki.assert_not_awaited()

    def test_threshold_env_override(self):
        resp = self.post_fwd(self.judged(dtc=0.6),
                             env={**_FWD_ENV,
                                  "SORTATION_FORWARD_THRESHOLD": "0.5"})
        self.assertEqual(resp.json()["forwarded"]["line"], "koseki")

    def test_valuation_forward(self):
        """S4-M3: 評価証明・課税明細も回送対象（case_hint/冪等キー貫通は T1 と同じ型）"""
        resp = self.post_fwd(self.judged(doc_type="評価証明・課税明細"),
                             data={"drive_file_id": "drv-9"})
        body = resp.json()
        self.assertEqual(body["action"], "auto")
        self.assertEqual(body["forwarded"],
                         {"line": "valuation", "status": "ok",
                          "results": [{"zaisan": "updated"}]})
        (pdf_bytes, _), kwargs = self.valuation.await_args
        self.assertEqual(pdf_bytes, PDF)
        self.assertEqual(kwargs["case_hint"], "12")
        self.assertEqual(kwargs["case_app_hint"], "26")
        self.assertEqual(kwargs["drive_file_id"], "drv-9")
        self.koseki.assert_not_awaited()
        self.registry.assert_not_awaited()

    def test_valuation_below_threshold_no_forward(self):
        resp = self.post_fwd(self.judged(doc_type="評価証明・課税明細", dtc=0.84))
        self.assertNotIn("forwarded", resp.json())
        self.valuation.assert_not_awaited()

    def test_non_target_doc_type_no_forward(self):
        """対象外種別（通帳等）は高確信度でも回送しない（相談カードも対象外）"""
        for doc_type in ("通帳", "住民票・戸籍附票", "その他"):
            with self.subTest(doc_type=doc_type):
                resp = self.post_fwd(self.judged(doc_type=doc_type))
                self.assertNotIn("forwarded", resp.json())

    def test_ask_route_never_forwards(self):
        """ask（顧客未確定）は種別・確信度を満たしても回送しない"""
        judged = {"doc_type": "戸籍", "doc_type_confidence": 0.99,
                  "customer_record_id": None, "confidence": 0.3, "reason": "x"}
        resp = self.post_fwd(judged)
        self.assertEqual(resp.json()["action"], "ask")
        self.assertNotIn("forwarded", resp.json())
        self.koseki.assert_not_awaited()

    def test_forward_failure_degrades_without_breaking_auto(self):
        resp = self.post_fwd(self.judged(),
                             koseki=AsyncMock(side_effect=RuntimeError("line down")))
        body = resp.json()
        self.assertEqual(body["action"], "auto", "auto 成功は不変")
        self.assertEqual(body["customer"]["folder_name"], "No12_山田太郎")
        self.assertEqual(body["forwarded"]["status"], "error")
        self.assertIn("line down", body["forwarded"]["error"])

    def test_existing_response_keys_unchanged_when_forwarding(self):
        """回送時も auto の既存キーは不変（GAS 無変更の担保・追加は forwarded のみ）"""
        resp = self.post_fwd(self.judged())
        body = resp.json()
        self.assertEqual(
            set(body),
            {"action", "doc_type", "confidence", "customer",
             "suggested_filename", "forwarded"})
        self.assertEqual(body["suggested_filename"], "山田太郎_戸籍_20260706.pdf")


class TestDegradedPaths(_Base):
    def test_claude_failure_degrades_to_ask(self):
        """Claude 全断: action=ask / doc_type=不明 / 判定不能の旨を通知"""
        resp = self.post(judge_error=RuntimeError("all models down"),
                         candidates=[cand(12, "山田太郎")])
        body = resp.json()
        self.assertEqual(body, {"action": "ask", "doc_type": "不明",
                                "confidence": 0.0, "customer": None,
                                "suggested_filename": None})
        self.assertIn("判定処理が実行できませんでした", self.line_text())

    def test_ocr_failure_degrades_to_ask(self):
        resp = self.post(ocr_error=RuntimeError("vision 500"))
        self.assertEqual(resp.json()["action"], "ask")
        self.assertEqual(resp.json()["doc_type"], "不明")
        self.assertEqual(self.push.await_count, 1)

    def test_vision_env_unset_is_500_explicit(self):
        """GOOGLE_VISION_API_KEY 未設定は運用設定ミスの明示エラー（縮退しない）"""
        resp = self.post(env={**_ENV, "GOOGLE_VISION_API_KEY": ""})
        self.assertEqual(resp.status_code, 500)
        self.assertIn("GOOGLE_VISION_API_KEY", resp.json()["detail"])


class TestDuplicateDetection(_Base):
    def test_duplicate_drive_file_id_logs_and_rejudges(self):
        """同一 drive_file_id の再送: ログ検知のみ・応答契約は不変（第1段の冪等）"""
        judged = {"doc_type": "戸籍", "customer_record_id": "12",
                  "confidence": 0.9, "reason": "x"}
        sortation_ingest._seen_drive_file_ids.clear()
        first = self.post(judged=judged, candidates=[cand(12, "山田太郎")],
                          data={"drive_file_id": "dup-1"})
        second = self.post(judged=judged, candidates=[cand(12, "山田太郎")],
                           data={"drive_file_id": "dup-1"})
        self.assertEqual(first.json(), second.json())
        self.assertIn("dup-1", sortation_ingest._seen_drive_file_ids)


if __name__ == "__main__":
    unittest.main()
