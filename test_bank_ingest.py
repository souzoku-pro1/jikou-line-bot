"""bank_reader.py＋/bank/ingest（S6-1 通帳・残高証明）のテスト

検証:
読解部品（スキーマキー・写像往復・validate・確信度・2形態のサンプルOCR回帰）・
入口（404偽装・PDF400・品質ゲート封筒=bank_ingest）・2形態の転記分岐
（残高証明=評価方法「残高証明」／通帳=「その他」＋備考明示）・
upsert（同一口座×案件の再送=残高/基準日の更新のみ・評価確定/有効/名義の不触）・
口座キーの正規化（全半角・ハイフン・空白）・案件解決（case_hint/冪等キー逆引き/
不能→要確認）・読解JSONの原本添付・env縮退・関所ハンドラ。
既存 /scan（通帳→App 27）は不変（全suiteの既存回帰で固定）。全てモック。
"""

import asyncio
import json
import os
import re
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

import bank_ingest  # noqa: E402
import bank_reader  # noqa: E402
from bank_ingest import account_key, normalize_account_part  # noqa: E402
from bank_reader import (  # noqa: E402
    BANK_READING_TOOL,
    BankReaderError,
    read_bank,
    to_japanese_bank,
    validate_reading,
)
import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]

client = TestClient(main.app)

URL = "/bank/ingest"
PDF = b"%PDF-1.4 dummy bank"

_ENV = {"BANK_INGEST_TOKEN": "bank_token",
        "APP_ZAISAN": "35", "TOKEN_ZAISAN": "t35",
        "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30"}


def run(coro):
    return asyncio.run(coro)


def account(**kw):
    a = {"金融機関名": "〇〇銀行", "支店名": "△△支店", "預金種別": "普通",
         "口座番号": "1234567", "名義人": "熊澤宮子", "残高": 1234567,
         "基準日": "令和8年7月1日", "基準日_西暦": "2026-07-01",
         "confidence": {"金融機関名": 0.95, "残高": 0.9}}
    a.update(kw)
    return a


def reading(doc_form="残高証明", accounts=None):
    return {"書類形態": doc_form,
            "口座": accounts if accounts is not None else [account()],
            "confidence": {"書類形態": 0.95}}


def english_reading():
    return {"doc_form": "残高証明",
            "accounts": [{"bank_name": "〇〇銀行", "branch_name": "△△支店",
                          "account_type": "普通", "account_number": "1234567",
                          "holder_name": "熊澤宮子", "balance": 1234567,
                          "basis_date": "令和8年7月1日",
                          "basis_date_seireki": "2026-07-01",
                          "confidence": {"bank_name": 0.95}}],
            "confidence": {"doc_form": 0.95}}


class TestReader(unittest.TestCase):
    def test_schema_keys_are_ascii(self):
        pattern = re.compile(r"^[a-zA-Z0-9_.-]{1,64}$")

        def walk(schema, where="root"):
            for key, sub in schema.get("properties", {}).items():
                self.assertRegex(key, pattern, f"{where}.{key}")
                walk(sub, f"{where}.{key}")
            if isinstance(schema.get("items"), dict):
                walk(schema["items"], f"{where}[]")
        walk(BANK_READING_TOOL["input_schema"])

    def test_mapping_roundtrip(self):
        mapped = to_japanese_bank(english_reading())
        self.assertEqual(mapped["書類形態"], "残高証明")
        acc = mapped["口座"][0]
        self.assertEqual(acc["金融機関名"], "〇〇銀行")
        self.assertEqual(acc["口座番号"], "1234567")
        self.assertEqual(acc["残高"], 1234567)
        self.assertEqual(acc["基準日"], "令和8年7月1日")
        self.assertEqual(acc["基準日_西暦"], "2026-07-01")
        self.assertEqual(acc["confidence"], {"金融機関名": 0.95})
        self.assertEqual(validate_reading(mapped), [])
        self.assertEqual(to_japanese_bank(mapped), mapped, "日本語キーは恒等")

    def test_validate_violations(self):
        cases = [
            ({"書類形態": "領収書", "口座": [account()]}, "書類形態 が許容値外"),
            ({"書類形態": "通帳", "口座": []}, "口座 が空でない配列でない"),
            ({"書類形態": "通帳", "口座": [account(金融機関名="")]},
             "金融機関名 が空でない文字列でない"),
            ({"書類形態": "通帳", "口座": [account(残高="1,234円")]},
             "残高 が整数でも null でもない"),
        ]
        for rd, expected in cases:
            with self.subTest(expected=expected):
                self.assertTrue(any(expected in e for e in validate_reading(rd)))

    def test_sample_ocr_regression_both_forms(self):
        """2形態の読解分岐（tool use 強制・写像適用）"""
        for doc_form in ("残高証明", "通帳"):
            with self.subTest(doc_form=doc_form):
                data = dict(english_reading(), doc_form=doc_form)
                block = MagicMock(type="tool_use", name_=None)
                block.name = "save_bank_reading"
                block.input = data
                response = MagicMock(content=[block])
                with patch.object(bank_reader, "create_message_with_fallback",
                                  new=AsyncMock(return_value=response)), \
                        patch.object(bank_reader, "_get_client",
                                     new=MagicMock()):
                    rd = run(read_bank("残高証明OCR"))
                self.assertEqual(rd["書類形態"], doc_form)
                self.assertEqual(validate_reading(rd), [])

    def test_no_tool_use_raises(self):
        response = MagicMock(content=[], stop_reason="end_turn")
        with patch.object(bank_reader, "create_message_with_fallback",
                          new=AsyncMock(return_value=response)), \
                patch.object(bank_reader, "_get_client", new=MagicMock()):
            with self.assertRaises(BankReaderError):
                run(read_bank("x"))


class TestAccountKey(unittest.TestCase):
    def test_normalization_full_half_hyphen_space(self):
        """口座キーの正規化: 全半角・ハイフン・空白の揺れが同一キーに"""
        a = account(金融機関名="〇〇銀行", 支店名="△△支店", 口座番号="1234567")
        b = account(金融機関名="〇〇 銀行", 支店名="△△支店",
                    口座番号="１２３-４５６７")
        self.assertEqual(account_key(a), account_key(b))
        self.assertTrue(account_key(a).startswith("bank:"))
        self.assertEqual(normalize_account_part("１２３ー４５６・７"), "1234567")


class _KT:
    def __init__(self, *, zaisan_rows=(), reverse=()):
        self.zaisan_rows = list(zaisan_rows)  # upsert 検索の既存行
        self.reverse = list(reverse)          # 冪等キー逆引き
        self.created, self.updated, self.uploaded, self.searches = [], [], [], []

    async def search_records(self, app, query, fields=None):
        self.searches.append((app.app_id_env, query))
        if "案件レコードID =" in query:
            return self.zaisan_rows
        return self.reverse  # 冪等キーのみの逆引き検索

    async def create_record(self, app, fields):
        self.created.append((app.app_id_env, fields))
        return {"APP_ZAISAN": "351", "APP_SHIPPING": "301"}[app.app_id_env]

    async def update_record(self, app, record_id, fields, revision=None):
        self.updated.append((app.app_id_env, str(record_id), fields))

    async def upload_file(self, app, filename, content, mime):
        self.uploaded.append((app.app_id_env, filename, mime))
        return f"fk-{len(self.uploaded)}"

    def patches(self):
        return [patch(f"hub.kintone.{n}", new=getattr(self, n))
                for n in ("search_records", "create_record", "update_record",
                          "upload_file")]

    def by_env(self, seq, env):
        return [x for x in seq if x[0] == env]


class _Base(unittest.TestCase):
    def post(self, kt: _KT, env=None, rd=None, token="bank_token", data=None):
        patchers = [
            patch("bank_ingest.read_bank",
                  new=AsyncMock(return_value=rd if rd is not None else reading())),
            patch("main._ocr_pdf_bytes", new=MagicMock(return_value="銀行OCR")),
            patch.dict("os.environ", env if env is not None else _ENV),
            *kt.patches(),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        query = f"?token={token}" if token is not None else ""
        return client.post(URL + query,
                           files={"file": ("bank.pdf", PDF, "application/pdf")},
                           data=data or {})


class TestAuthAndGate(_Base):
    def test_missing_token_is_404(self):
        self.assertEqual(self.post(_KT(), token=None).status_code, 404)

    def test_wrong_token_is_404(self):
        self.assertEqual(self.post(_KT(), token="wrong").status_code, 404)

    def test_probe_without_body_is_404_not_422(self):
        with patch.dict("os.environ", _ENV):
            self.assertEqual(client.post(URL).status_code, 404)

    def test_valid_token_without_file_is_400(self):
        with patch.dict("os.environ", _ENV):
            self.assertEqual(client.post(URL + "?token=bank_token").status_code,
                             400)

    def test_low_confidence_goes_to_review_with_bank_envelope(self):
        kt = _KT()
        resp = self.post(kt, rd={"書類形態": "通帳",
                                 "口座": [account(confidence={})],
                                 "confidence": {}})
        self.assertEqual(resp.json()["status"], "needs_review")
        (_, fields), = kt.by_env(kt.created, "APP_SHIPPING")
        self.assertEqual(list(json.loads(fields["チャネル固有データ"])),
                         ["bank_ingest"])

    def test_zaisan_env_unset_is_503(self):
        resp = self.post(_KT(), env={**_ENV, "APP_ZAISAN": ""})
        self.assertEqual(resp.status_code, 503)


class TestTranscription(_Base):
    def test_zandaka_shomei_creates_row(self):
        """残高証明: 評価方法=残高証明・推奨書式の特定情報・読解JSON添付"""
        kt = _KT()
        resp = self.post(kt, data={"case_hint": "3"})
        body = resp.json()
        self.assertEqual(body["results"][0]["zaisan"], "created")
        (_, fields), = kt.by_env(kt.created, "APP_ZAISAN")
        self.assertEqual(fields["財産種別"], "預貯金")
        self.assertEqual(fields["特定情報"],
                         "〇〇銀行 △△支店 普通預金 口座番号1234567")
        self.assertEqual(fields["名義"], "熊澤宮子")
        self.assertEqual(fields["評価額"], "1234567")
        self.assertEqual(fields["評価方法"], "残高証明")
        self.assertEqual(fields["評価基準日"], "2026-07-01")
        self.assertEqual(fields["データ源"], "OCR_残高証明")
        self.assertEqual(fields["評価確定"], "no")
        self.assertNotIn("備考", fields)
        # 原本 = PDF + 読解断片.json（明細アプリは作らない・添付で痕跡）
        names = [u[1] for u in kt.by_env(kt.uploaded, "APP_ZAISAN")]
        self.assertEqual(names, ["bank.pdf", "読解断片.json"])
        self.assertEqual(len(fields["原本"]), 2)

    def test_tsucho_uses_sonota_with_biko(self):
        """通帳: 正本選択肢に「通帳」が無いため 評価方法=その他＋備考で明示"""
        kt = _KT()
        resp = self.post(kt, rd=reading(doc_form="通帳"), data={"case_hint": "3"})
        self.assertEqual(resp.json()["results"][0]["zaisan"], "created")
        (_, fields), = kt.by_env(kt.created, "APP_ZAISAN")
        self.assertEqual(fields["評価方法"], "その他")
        self.assertIn("通帳記載の残高による", fields["備考"])

    def test_same_account_resend_updates_balance_only(self):
        """同一口座×案件の再送 = 残高・基準日の更新のみ（不触保護は S5 同一）"""
        kt = _KT(zaisan_rows=[{"$id": {"value": "88"},
                               "原本": {"value": [{"fileKey": "old"}]}}])
        resp = self.post(kt, rd=reading(accounts=[account(残高=2222222)]),
                         data={"case_hint": "3"})
        self.assertEqual(resp.json()["results"][0]["zaisan"], "updated")
        (_, rid, fields), = kt.by_env(kt.updated, "APP_ZAISAN")
        self.assertEqual(rid, "88")
        self.assertEqual(fields["評価額"], "2222222")
        self.assertEqual(fields["評価基準日"], "2026-07-01")
        for forbidden in ("評価確定", "有効", "名義", "評価方法", "特定情報"):
            self.assertNotIn(forbidden, fields)
        self.assertEqual(fields["原本"][0], {"fileKey": "old"},
                         "既存原本を保持して新PDF・読解JSONを追加")
        self.assertEqual(len(fields["原本"]), 3)

    def test_multiple_accounts_one_row_each(self):
        kt = _KT()
        rd = reading(accounts=[account(),
                               account(口座番号="7654321", 残高=500)])
        resp = self.post(kt, rd=rd, data={"case_hint": "3"})
        self.assertEqual(len(kt.by_env(kt.created, "APP_ZAISAN")), 2,
                         "1口座=1行")


class TestCaseResolution(_Base):
    def test_reverse_lookup_by_account_key(self):
        """case_hint 無し → 同一冪等キーの既存財産行から案件を逆引き"""
        kt = _KT(reverse=[{"案件レコードID": {"value": "3"}}])
        resp = self.post(kt)
        result = resp.json()["results"][0]
        self.assertEqual(result["case_record_id"], "3")
        reverse_queries = [q for e, q in kt.searches
                           if "冪等キー" in q and "案件レコードID =" not in q]
        self.assertTrue(reverse_queries)
        self.assertIn("bank:", reverse_queries[0])

    def test_unresolvable_goes_to_review_with_account_detail(self):
        kt = _KT()
        resp = self.post(kt)
        result = resp.json()["results"][0]
        self.assertEqual(result["zaisan"], "needs_review")
        (_, fields), = kt.by_env(kt.created, "APP_SHIPPING")
        detail = json.loads(fields["チャネル固有データ"])["bank_ingest"]
        self.assertEqual(detail["口座"]["金融機関名"], "〇〇銀行",
                         "確定ハンドラが再OCRせず財産行を再構成できる断片")
        self.assertEqual(kt.by_env(kt.created, "APP_ZAISAN"), [])


class TestResolverHandler(unittest.TestCase):
    """関所ハンドラ（RESOLVERS[bank_ingest]）"""

    def _resolve(self, *, status="要確認", executed="no"):
        from review_resolve import ReviewGroup, ReviewItem, resolve_group
        group = ReviewGroup(
            source="bank_ingest", idempotency_key="bank:x",
            items=[ReviewItem(record_id="12", subject="通帳・残高証明の読解転記",
                              detail={"書類形態": "残高証明", "口座": account()},
                              file_keys=["pdf-12"], file_name="bank.pdf")])
        kt = _KT()

        async def get_record(app, record_id):
            return {"発送ステータス": {"value": status},
                    "実行済み": {"value": executed}}

        async def download_file(app, file_key):
            return b"PDFDATA"
        from contextlib import ExitStack
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, _ENV))
            stack.enter_context(patch("hub.kintone.get_record", new=get_record))
            stack.enter_context(
                patch("hub.kintone.download_file", new=download_file))
            for p in kt.patches():
                stack.enter_context(p)
            result = run(resolve_group(group, "3"))
        return result, kt

    def test_happy_path(self):
        result, kt = self._resolve()
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["items"][0]["zaisan"], "created")
        (_, fields), = kt.by_env(kt.created, "APP_ZAISAN")
        self.assertEqual(fields["評価方法"], "残高証明")
        self.assertEqual(fields["案件レコードID"], "3")
        closes = kt.by_env(kt.updated, "APP_SHIPPING")
        self.assertEqual(closes[0][2], {"発送ステータス": "完了", "実行済み": "yes"})

    def test_guard_aborts(self):
        result, kt = self._resolve(status="完了", executed="yes")
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(kt.created, [])


if __name__ == "__main__":
    unittest.main()
