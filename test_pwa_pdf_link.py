"""PWA-PDF-LINK: ダッシュボード/関係図からの原本 PDF 直リンクのテスト。

固定する仕様（本票 PWA-PDF-LINK・実測調査 2026-08-17）:
- Drive 参照の所在: App30=Drive_fileId／App33=Drive_fileId（SINGLE_LINE_TEXT）。
  App35（財産）・App28（チャットログ）には Drive 参照 field が存在しない
  （リンク対象外・完了報告に提案記載）。
- URL 構成は config.drive_pdf_view_url の一点（grammar=ASCII 英数・-・_ の
  10〜100 文字のみ・それ以外は None＝リンク化しない・URL インジェクション防止）。
- 応答には grammar 検証済みの導出 URL のみ付与（App30=_pdf_url／App33=pdf_url。
  生 Drive_fileId は VIEW へ出さない）。参照なし/不正の行は key 自体なし
  ＝画面はリンク非表示（壊れたリンクを出さない）。
- サーバは PDF を中継しない（閲覧は開く人の Google ログイン権限・
  「アプリにデータを持たせない」の維持）。kintone 原本リンクは併存。
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "d",
    "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "d",
    "APP_ZAISAN": "35", "TOKEN_ZAISAN": "d",
    "APP_SOUZOKUNIN": "36", "TOKEN_SOUZOKUNIN": "d",
}.items():
    os.environ.setdefault(_k, _v)

import config  # noqa: E402
import hub.kintone as hub_kintone  # noqa: E402
import hub.webapp_souzoku_dashboard as sd  # noqa: E402
from kinship_graph import load_koseki_summaries_for_case  # noqa: E402

_FID_OK = "1AbC-dEfG_hIjKlMnOpQrStUvWxYz0123456789"


def _run(coro):
    return asyncio.run(coro)


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


# ── URL 構成 grammar（単一の正 config.drive_pdf_view_url） ────────────────────
class TestDriveUrlGrammar(unittest.TestCase):
    def test_valid_ids_build_view_url(self):
        for fid in (_FID_OK, "a" * 10, "A0-_" + "b" * 6, "z" * 100):
            with self.subTest(fid=fid):
                self.assertEqual(
                    config.drive_pdf_view_url(fid),
                    f"https://drive.google.com/file/d/{fid}/view")

    def test_grammar_violations_return_none(self):
        bad = ["", "short", "a" * 9, "a" * 101,          # 空・長さ境界外
               "abc/../../evil-idabc", "id/with/slash1",  # path 挿入
               "id?query=1-aaaa", "id#frag-aaaaaa",       # query/fragment
               "id id 0123456", "id\nid0123456",          # 空白・改行
               'id"quote-23456', "id'quote-23456",        # 引用符
               "日本語のIDですよ12", "ｉｄ全角-3456",        # 非 ASCII
               "id&amp=1-23456", "id%2e%2e-3456",         # メタ文字・encoded
               None, 123, ["a" * 12]]                     # 型不正
        for fid in bad:
            with self.subTest(fid=repr(fid)[:30]):
                self.assertIsNone(config.drive_pdf_view_url(fid))


# ── App30（souzoku dashboard・documents section） ────────────────────────────
class TestDocumentsPdfLink(unittest.TestCase):
    def _load(self, rows):
        with patch.dict(os.environ, {"SOUZOKU_KINTONE_APP_ID": "26"}), \
             patch.object(hub_kintone, "search_records",
                          AsyncMock(return_value=rows)):
            return _run(sd._load_documents("12"))

    def test_valid_ref_gets_pdf_url_and_raw_id_not_in_view(self):
        rows = [_rec(**{"$id": "401", "件名": "スキャン受領",
                        "Drive_fileId": _FID_OK})]
        out = self._load(rows)["records"][0]
        self.assertEqual(out["_pdf_url"],
                         f"https://drive.google.com/file/d/{_FID_OK}/view")
        self.assertNotIn("Drive_fileId", out)    # 生 ID は VIEW へ出さない

    def test_missing_or_invalid_ref_has_no_link_key(self):
        rows = [_rec(**{"$id": "402", "件名": "発送分"}),                  # 参照なし
                _rec(**{"$id": "403", "件名": "不正", "Drive_fileId": ""}),
                _rec(**{"$id": "404", "件名": "不正2",
                        "Drive_fileId": "../../etc/passwd"})]
        outs = self._load(rows)["records"]
        for out in outs:
            self.assertNotIn("_pdf_url", out, out.get("$id"))
        # 行自体は維持（リンクだけ非表示）
        self.assertEqual(len(outs), 3)

    def test_fetch_fields_include_ref_and_query_unchanged(self):
        mock = AsyncMock(return_value=[])
        with patch.dict(os.environ, {"SOUZOKU_KINTONE_APP_ID": "26"}), \
             patch.object(hub_kintone, "search_records", mock):
            _run(sd._load_documents("12"))
        self.assertEqual(mock.call_args.kwargs.get("fields"),
                         sd._DOC_FETCH_FIELDS)
        self.assertIn("Drive_fileId", sd._DOC_FETCH_FIELDS)
        self.assertNotIn("Drive_fileId", sd._DOC_VIEW_FIELDS)


# ── App33（kinship・取得済み戸籍一覧） ───────────────────────────────────────
class TestKosekiPdfLink(unittest.TestCase):
    _ENV33 = {"APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33"}

    def _load(self, records):
        with patch.dict(os.environ, self._ENV33), \
             patch("hub.kintone.search_records",
                   new=AsyncMock(return_value=records)) as mock:
            out = _run(load_koseki_summaries_for_case("9"))
        return out, mock

    def test_valid_ref_adds_pdf_url_key(self):
        records = [{"$id": {"value": "70"},
                    "読解JSON": {"value": '{"戸籍": {"本籍": "川口市"}}'},
                    "Drive_fileId": {"value": _FID_OK}}]
        out, mock = self._load(records)
        self.assertEqual(out[0]["pdf_url"],
                         f"https://drive.google.com/file/d/{_FID_OK}/view")
        self.assertEqual(mock.call_args.kwargs.get("fields"),
                         ["$id", "読解JSON", "Drive_fileId"])

    def test_missing_or_invalid_ref_keeps_row_shape_without_key(self):
        # 参照なし/不正は key 自体なし（従来の行 dict と同形＝既存 pin と両立）
        records = [{"$id": {"value": "71"},
                    "読解JSON": {"value": "{}"}},
                   {"$id": {"value": "72"},
                    "読解JSON": {"value": "{}"},
                    "Drive_fileId": {"value": "bad id"}},
                   {"$id": {"value": "73"},
                    "読解JSON": {"value": "{}"},
                    "Drive_fileId": 42}]          # 型崩れも例外を漏らさない
        out, _mock = self._load(records)
        self.assertEqual(len(out), 3)
        for row in out:
            self.assertNotIn("pdf_url", row, row["record_id"])
            self.assertEqual(
                set(row), {"record_id", "honseki", "hittousha",
                           "juzen_honseki"})


# ── 画面側の構造 pin（リンク化はサーバ検証済み URL のみ・rel/target 固定） ────
class TestPagePins(unittest.TestCase):
    def test_souzoku_case_page_renders_pdf_link_guarded(self):
        from hub.webapp_auth import WEBAPP_ROOT
        page = (WEBAPP_ROOT / "souzoku_case.html").read_text(encoding="utf-8")
        self.assertIn('startsWith("https://drive.google.com/file/d/")', page)
        self.assertIn('rel = "noopener noreferrer"', page)
        self.assertIn("pdfLink(rec._pdf_url)", page)
        # UI-POLISH-1 票由来: テーブル列（<th>PDF</th>）→ カード内リンク
        # 「原本PDF」表記へ（リンク化条件・rel/target・pdfLink 経由は不変）
        self.assertIn('a.textContent = "原本PDF";', page)
        self.assertIn("recLink(", page)          # kintone 原本リンクは併存

    def test_kinship_page_renders_pdf_link_guarded(self):
        from hub.webapp_auth import WEBAPP_ROOT
        page = (WEBAPP_ROOT / "kinship.html").read_text(encoding="utf-8")
        self.assertIn('startsWith("https://drive.google.com/file/d/")', page)
        self.assertIn('rel = "noopener noreferrer"', page)
        self.assertIn("原本PDF", page)


if __name__ == "__main__":
    unittest.main()
