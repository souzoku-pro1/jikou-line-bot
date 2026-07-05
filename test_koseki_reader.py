"""koseki_reader.py（R3 戸籍構造化読解・2026-07-05 裁定のハイブリッド構成）のテスト

検証: 正常読解（AI読解済・和暦原文保持・ocr_text 温存・確信度書込）・
スキーマ逸脱→要再読解・低確信度→要再読解・OCR空→要再読解・
tool_use 不在は例外（未読解のまま）・未読解以外は skip・無効化フラグ・
一括処理（複数件/0件/1件失敗でも継続）・確認済へ進めない静的検査・
/koseki/ingest の A案結線（読解失敗でも成功応答が不変）。
kintone / Claude は全てモック。
"""

import hashlib
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ── import 前に環境変数を差し込む（既存テストと同じ流儀・triage skip ガード対策込み）──
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

import koseki_reader  # noqa: E402
import main  # noqa: E402
from koseki_reader import (  # noqa: E402
    KosekiReaderError,
    process_record,
    process_unread_records,
    validate_reading,
)

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]  # skip ガードの誤解除防止（test_hub_dispatch 参照）

client = TestClient(main.app)

_ENV = {"KOSEKI_READER_DISABLED": "",   # conftest の既定無効を解除
        "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t"}

OCR_TEXT = "本籍 川口市青木一丁目1番地 筆頭者 山田太郎 …（戸籍OCRテキスト）"


def valid_reading():
    """02 §3 スキーマに適合する読解結果（和暦原文・高確信度）"""
    return {
        "様式": "改製原（昭和）",
        "様式confidence": 0.9,
        "戸籍": {
            "本籍": "川口市青木一丁目1番地",
            "筆頭者": "山田太郎",
            "編製日": "昭和32年4月1日",
            "編製日_西暦": "1957-04-01",
            "消除日": "",
            "消除日_西暦": None,
            "編製事由": "転籍",
            "従前戸籍": {"本籍": "浦和市高砂1番地", "筆頭者": "山田太郎"},
            "confidence": {"本籍": 0.95, "筆頭者": 0.9, "編製日": 0.9},
        },
        "人物": [
            {"氏名": "山田太郎", "続柄": "戸主", "生年月日": "明治40年1月5日",
             "除籍済み": False,
             "身分事項": [{"種別": "出生", "日付": "明治40年1月5日", "confidence": 0.9}],
             "confidence": {"氏名": 0.85, "生年月日": 0.9}},
            {"氏名": "山田花子", "続柄": "妻", "生年月日": "明治45年3月3日",
             "除籍済み": True, "除籍事由": "死亡",
             "身分事項": [{"種別": "死亡", "日付": "昭和60年8月15日", "confidence": 0.9}],
             "confidence": {"氏名": 0.8}},
        ],
    }


def book_record(state="未読解", reading_json=None):
    if reading_json is None:
        reading_json = json.dumps({"ocr_text": OCR_TEXT}, ensure_ascii=False)
    return {"$id": {"value": "88"},
            "読解状態": {"value": state},
            "読解JSON": {"value": reading_json}}


class _Block:
    type = "tool_use"
    name = "save_koseki_reading"

    def __init__(self, input_):
        self.input = input_


class _TextBlock:
    type = "text"
    text = "ツールを使わない応答"


class _Resp:
    stop_reason = "tool_use"

    def __init__(self, blocks):
        self.content = blocks


class _Base(unittest.TestCase):
    def arm(self, *, record=None, reading=None, blocks=None, env=None):
        """kintone / Claude のモック一式を張る"""
        self.updated = []

        async def update_record(app, record_id, fields, revision=None):
            self.updated.append((app.app_id_env, record_id, fields))

        self.get_record = AsyncMock(return_value=record or book_record())
        self.gateway = AsyncMock(return_value=_Resp(
            blocks if blocks is not None else [_Block(reading or valid_reading())]))
        patchers = [
            patch("hub.kintone.get_record", new=self.get_record),
            patch("hub.kintone.update_record", new=update_record),
            patch("koseki_reader.create_message_with_fallback", new=self.gateway),
            patch("koseki_reader._get_client", new=MagicMock()),
            patch.dict("os.environ", {**_ENV, **(env or {})}),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    def run_one(self, record_id="88"):
        import asyncio
        return asyncio.run(process_record(record_id))

    def saved_fields(self):
        (app_env, rid, fields), = self.updated
        self.assertEqual(app_env, "APP_KOSEKI_BOOK")
        return fields


class TestNormalReading(_Base):
    def test_valid_reading_becomes_ai_done(self):
        self.arm()
        result = self.run_one()
        self.assertEqual(result["status"], "ai_done")
        fields = self.saved_fields()
        self.assertEqual(fields["読解状態"], "AI読解済")
        saved = json.loads(fields["読解JSON"])
        self.assertEqual(saved["様式"], "改製原（昭和）")
        self.assertEqual(saved["戸籍"]["編製日"], "昭和32年4月1日",
                         "編製日は和暦原文のまま")
        self.assertEqual(saved["戸籍"]["編製日_西暦"], "1957-04-01",
                         "構造化日付は別キー")
        self.assertEqual(saved["人物"][0]["生年月日"], "明治40年1月5日")
        self.assertEqual(saved["ocr_text"], OCR_TEXT,
                         "OCR原文を温存（再読解の入力を失わない）")

    def test_confidences_written(self):
        self.arm()
        self.run_one()
        fields = self.saved_fields()
        self.assertEqual(fields["様式確信度"], "0.9")
        self.assertAlmostEqual(float(fields["全体確信度"]), 0.888, places=2,
                               msg="全確信度の平均")

    def test_tool_use_is_forced(self):
        """text 応答からの JSON 切り出しをしない（D2 と同流儀）の担保"""
        self.arm()
        self.run_one()
        kwargs = self.gateway.await_args.kwargs
        self.assertEqual(kwargs["tool_choice"],
                         {"type": "tool", "name": "save_koseki_reading"})
        self.assertEqual(kwargs["tools"][0]["name"], "save_koseki_reading")
        self.assertIn(OCR_TEXT, kwargs["messages"][0]["content"])


class TestSafeSideHandling(_Base):
    def test_schema_violation_goes_to_reread(self):
        reading = valid_reading()
        reading["人物"][0]["身分事項"][0]["種別"] = "結婚"  # 許容値外
        self.arm(reading=reading)
        result = self.run_one()
        self.assertEqual(result["status"], "needs_reread")
        fields = self.saved_fields()
        self.assertEqual(fields["読解状態"], "要再読解")
        saved = json.loads(fields["読解JSON"])
        self.assertTrue(any("種別" in e for e in saved["検証エラー"]))
        self.assertEqual(saved["ocr_text"], OCR_TEXT)

    def test_missing_required_key_goes_to_reread(self):
        reading = valid_reading()
        del reading["戸籍"]["本籍"]
        self.arm(reading=reading)
        self.assertEqual(self.run_one()["status"], "needs_reread")

    def test_low_confidence_goes_to_reread(self):
        reading = valid_reading()
        reading["様式confidence"] = 0.2
        reading["戸籍"]["confidence"] = {"本籍": 0.2, "筆頭者": 0.2}
        for person in reading["人物"]:
            person["confidence"] = {"氏名": 0.2}
            for event in person["身分事項"]:
                event["confidence"] = 0.2
        self.arm(reading=reading)
        result = self.run_one()
        self.assertEqual(result["status"], "needs_reread")
        self.assertEqual(self.saved_fields()["読解状態"], "要再読解")

    def test_empty_ocr_goes_to_reread_without_claude(self):
        self.arm(record=book_record(reading_json=json.dumps({"ocr_text": ""})))
        result = self.run_one()
        self.assertEqual(result["status"], "needs_reread")
        self.gateway.assert_not_awaited()
        fields = self.saved_fields()
        self.assertEqual(fields["読解状態"], "要再読解")
        self.assertEqual(fields["全体確信度"], "0.0")

    def test_no_tool_block_raises_and_leaves_record_untouched(self):
        self.arm(blocks=[_TextBlock()])
        with self.assertRaises(KosekiReaderError):
            self.run_one()
        self.assertEqual(self.updated, [], "未読解のまま（後日回収できる）")

    def test_non_unread_state_is_skipped(self):
        self.arm(record=book_record(state="AI読解済"))
        result = self.run_one()
        self.assertEqual(result["status"], "skipped")
        self.gateway.assert_not_awaited()
        self.assertEqual(self.updated, [])

    def test_disabled_flag_skips_without_any_io(self):
        self.arm(env={"KOSEKI_READER_DISABLED": "1"})
        result = self.run_one()
        self.assertEqual(result["status"], "skipped")
        self.get_record.assert_not_awaited()
        self.gateway.assert_not_awaited()


class TestBatchProcessing(unittest.TestCase):
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_processes_all_and_continues_on_failure(self):
        ids = [{"$id": {"value": str(i)}} for i in (1, 2, 3)]
        outcomes = [{"status": "ai_done", "record_id": "1"},
                    KosekiReaderError("boom"),
                    {"status": "needs_reread", "record_id": "3"}]
        with patch("hub.kintone.search_records",
                   new=AsyncMock(return_value=ids)), \
             patch("koseki_reader.process_record",
                   new=AsyncMock(side_effect=outcomes)), \
             patch.dict("os.environ", _ENV):
            results = self._run(process_unread_records(limit=10))
        self.assertEqual([r["status"] for r in results],
                         ["ai_done", "error", "needs_reread"],
                         "1件の失敗が他を止めない")
        self.assertIn("boom", results[1]["detail"])

    def test_zero_records_returns_empty(self):
        search = AsyncMock(return_value=[])
        with patch("hub.kintone.search_records", new=search), \
             patch.dict("os.environ", _ENV):
            self.assertEqual(self._run(process_unread_records()), [])
        query = search.await_args.args[1]
        self.assertIn('読解状態 in ("未読解")', query, "未読解のみ対象")

    def test_disabled_flag_returns_empty_without_search(self):
        search = AsyncMock()
        with patch("hub.kintone.search_records", new=search), \
             patch.dict("os.environ", {**_ENV, "KOSEKI_READER_DISABLED": "1"}):
            self.assertEqual(self._run(process_unread_records()), [])
        search.assert_not_awaited()


class TestStatusStaysAtAiDone(unittest.TestCase):
    """読解状態を AI読解済 より先へ進めない（人手確認フロー=R4 の専権）の静的固定"""

    def test_source_never_mentions_confirmed_status(self):
        source = open("koseki_reader.py", encoding="utf-8").read()
        self.assertNotIn("確認済", source,
                         "koseki_reader は読解状態を AI読解済/要再読解 までしか書かない")

    def test_status_constants_are_the_only_written_states(self):
        self.assertEqual(koseki_reader.STATUS_AI_DONE, "AI読解済")
        self.assertEqual(koseki_reader.STATUS_REREAD, "要再読解")


class TestIngestWiring(unittest.TestCase):
    """A案: /koseki/ingest 登録成功後の同期読解。失敗しても成功応答は不変"""

    PDF = b"%PDF-1.4 dummy koseki"

    def _post(self, reader_mock):
        async def create_record(app, fields):
            return "88"

        async def upload_file(app, filename, content, mime):
            return "fk-1"

        patchers = [
            patch("koseki_ingest._ocr_pdf", new=MagicMock(return_value=OCR_TEXT)),
            patch("koseki_ingest._render_page_images",
                  new=MagicMock(return_value=[])),
            patch("hub.kintone.search_records", new=AsyncMock(return_value=[])),
            patch("hub.kintone.upload_file", new=upload_file),
            patch("hub.kintone.create_record", new=create_record),
            patch("koseki_reader.process_record", new=reader_mock),
            patch.dict("os.environ", {"KOSEKI_INGEST_TOKEN": "koseki_token",
                                      "APP_KOSEKI_BOOK": "33",
                                      "TOKEN_KOSEKI_BOOK": "t"}),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        return client.post(
            "/koseki/ingest?token=koseki_token",
            files={"file": ("koseki.pdf", self.PDF, "application/pdf")})

    def _expected_body(self):
        return {"status": "ok", "kintone_record_id": "88",
                "page_images": 0, "ocr_chars": len(OCR_TEXT)}

    def test_reader_called_synchronously_after_registration(self):
        reader = AsyncMock(return_value={"status": "ai_done", "record_id": "88"})
        resp = self._post(reader)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), self._expected_body())
        reader.assert_awaited_once_with("88")

    def test_reader_failure_does_not_break_ingest_response(self):
        reader = AsyncMock(side_effect=RuntimeError("Claude全断"))
        resp = self._post(reader)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), self._expected_body(),
                         "読解失敗でも成功応答は完全に不変（未読解のまま回収可能）")


if __name__ == "__main__":
    unittest.main()
