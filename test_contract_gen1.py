"""CONTRACT-GEN-1: 時効援用委任契約書の自動生成（第1版）。

固定する仕様:
- テンプレ実在（docx_templates/jikou/委任契約書.docx）と EXPECTED_DOCX_TEMPLATES
  の 7 キー（{{依頼者氏名}} は本文 2 箇所）の実在照合
- トリガ: 契約書ステータス=契約書作成 のみ・作成済はループ防止 skip
- fail-closed: 顧客名/住所/債権者 1 社以上の欠落は生成せず不足フィールド名を
  明示して拒否（値は通知に載せない・空欄契約書を作らない）
- 差し込み: 氏名 2 箇所・住所・債権者 1=問い合わせ業者名／2・3=新設 field・
  空き枠と契約年月日は全角空白・凍結文言（報酬 44,000 円等）不変
- 添付書き戻し: FILE field「委任契約書」＋ステータス=契約書作成済（1 PUT）
"""

import io
import os
import unittest
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
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from docx import Document  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import contract_webhook as cw  # noqa: E402
import main  # noqa: E402
from config import EXPECTED_DOCX_TEMPLATES  # noqa: E402

_client = TestClient(main.app)
_URL = "/contract/doc-secret"


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


def _full_record(**over):
    base = {"契約書ステータス": "契約書作成", "顧客名": "熊澤花子",
            "住所": "埼玉県川口市青木1-1-1",
            "問い合わせ業者名": "株式会社Aファイナンス",
            "対象債権者2": "", "対象債権者3": ""}
    base.update(over)
    return _rec(**base)


def _body(record_id="12", status="契約書作成"):
    return {"record": {"$id": {"value": record_id},
                       "契約書ステータス": {"value": status}}}


def _docx_text(data: bytes) -> str:
    doc = Document(io.BytesIO(data))
    return "\n".join(p.text for p in doc.paragraphs) + "\n".join(
        c.text for t in doc.tables for row in t.rows for c in row.cells)


class TestTemplate(unittest.TestCase):
    def test_template_in_repo_with_all_keys(self):
        keys = EXPECTED_DOCX_TEMPLATES["docx_templates/jikou/委任契約書.docx"]
        self.assertEqual(sorted(keys), sorted([
            "{{依頼者氏名}}", "{{依頼者住所}}",
            "{{対象債権者1}}", "{{対象債権者2}}", "{{対象債権者3}}",
            "{{契約年}}", "{{契約月}}", "{{契約日}}"]))
        text = _docx_text(open(cw.TEMPLATE_PATH, "rb").read())
        for k in keys:
            self.assertIn(k, text)
        # {{依頼者氏名}} は本文 2 箇所（票の指定・実測 pin）
        self.assertEqual(text.count("{{依頼者氏名}}"), 2)
        # 凍結文言（報酬）の存在
        self.assertIn("44,000", text)


class TestWebhook(unittest.TestCase):
    def _post(self, *, record, body=None, url=_URL):
        upload = AsyncMock(return_value="fk-1")
        update = AsyncMock()
        notify = AsyncMock(return_value=True)
        with patch.dict(os.environ, _ENV), \
             patch.object(cw.hub_kintone, "get_record",
                          AsyncMock(return_value=record)), \
             patch.object(cw.hub_kintone, "upload_file", upload), \
             patch.object(cw.hub_kintone, "update_record", update), \
             patch("hub.notify.notify_admin_line", notify):
            r = _client.post(url, json=body or _body())
        return r, upload, update, notify

    def test_wrong_secret_403(self):
        r, *_ = self._post(record=_full_record(), url="/contract/wrong")
        self.assertEqual(r.status_code, 403)

    def test_not_triggered_skip(self):
        r, upload, update, _n = self._post(
            record=_full_record(),
            body=_body(status="契約書作成済"))
        self.assertEqual(r.json().get("skip"), "not_triggered")
        upload.assert_not_awaited()
        update.assert_not_awaited()

    def test_already_done_skip(self):
        r, upload, update, _n = self._post(
            record=_full_record(契約書ステータス="契約書作成済"))
        self.assertEqual(r.json().get("skip"), "already_done")
        upload.assert_not_awaited()

    def test_happy_path_generates_and_attaches(self):
        r, upload, update, notify = self._post(record=_full_record())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json().get("record_id"), "12")
        # 生成物の中身（差し込み結果）
        args = upload.await_args.args
        self.assertEqual(args[1], "委任契約書_時効援用.docx")
        text = _docx_text(args[2])
        self.assertEqual(text.count("熊澤花子"), 2)     # 氏名は 2 箇所
        self.assertIn("埼玉県川口市青木1-1-1", text)
        self.assertIn("株式会社Aファイナンス", text)
        self.assertNotIn("{{", text)                    # キー残存なし
        self.assertIn("44,000", text)                   # 凍結文言不変
        # 添付＋ステータス更新が 1 回の PUT
        update.assert_awaited_once()
        fields = update.await_args.args[2]
        self.assertEqual(fields["委任契約書"], [{"fileKey": "fk-1"}])
        self.assertEqual(fields["契約書ステータス"], "契約書作成済")
        notify.assert_not_awaited()

    def test_creditors_from_dedicated_fields(self):
        record = _full_record(問い合わせ業者名="",
                              対象債権者2="B社", 対象債権者3="C社")
        r, upload, _u, _n = self._post(record=record)
        self.assertEqual(r.status_code, 200)
        text = _docx_text(upload.await_args.args[2])
        self.assertIn("B社", text)
        self.assertIn("C社", text)

    def test_missing_fields_rejected_fail_closed(self):
        cases = {
            "住所欠落": _full_record(住所=""),
            "氏名欠落": _full_record(顧客名="  "),
            "債権者全欠落": _full_record(問い合わせ業者名="",
                                         対象債権者2="", 対象債権者3=""),
        }
        for label, record in cases.items():
            with self.subTest(case=label):
                r, upload, update, notify = self._post(record=record)
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.json().get("skip"), "missing_fields")
                upload.assert_not_awaited()      # 空欄契約書を作らない
                update.assert_not_awaited()      # ステータスも動かさない
                notify.assert_awaited_once()     # 不足を明示
                sent = notify.await_args.args[0]
                self.assertIn("必須項目が未入力", sent)
                # 通知はフィールド名のみ（値=氏名等は載せない）
                self.assertNotIn("熊澤", sent)

    def test_upload_failure_500_without_status_update(self):
        upload = AsyncMock(side_effect=RuntimeError("kintone down"))
        update = AsyncMock()
        with patch.dict(os.environ, _ENV), \
             patch.object(cw.hub_kintone, "get_record",
                          AsyncMock(return_value=_full_record())), \
             patch.object(cw.hub_kintone, "upload_file", upload), \
             patch.object(cw.hub_kintone, "update_record", update):
            r = _client.post(_URL, json=_body())
        self.assertEqual(r.status_code, 500)
        update.assert_not_awaited()


class TestFillData(unittest.TestCase):
    def test_blank_slots_are_fullwidth_space(self):
        data = cw.build_fill_data(_full_record())
        self.assertEqual(data["{{対象債権者2}}"], "　")
        self.assertEqual(data["{{対象債権者3}}"], "　")
        self.assertEqual(data["{{契約年}}"], "　")
        self.assertEqual(data["{{契約月}}"], "　")
        self.assertEqual(data["{{契約日}}"], "　")

    def test_missing_detection_closed(self):
        self.assertEqual(cw._missing_fields(_full_record()), [])
        self.assertEqual(cw._missing_fields(_full_record(顧客名="")),
                         ["顧客名"])
        missing = cw._missing_fields(_rec(**{
            "顧客名": "", "住所": "", "問い合わせ業者名": "",
            "対象債権者2": "", "対象債権者3": ""}))
        self.assertEqual(len(missing), 3)


if __name__ == "__main__":
    unittest.main()
