"""hub/redact.py（redaction contract）のテスト（P1-101・DRAFT §1）

固定する不変条件:
- unknown kind / 構造化値 / None / emit 内部例外 のいずれでも **原文が出ない**（fail-closed）。
- §13.1 禁止カテゴリ（contract/fax/qa/vendor_raw）と document_metadata は
  log/exception/business へ完全抑止（line_customer 本人宛のみ素通し）。
- 通常 log の PII は完全抑止（既定）。record_id/count は素通し。
"""

import unittest

from hub import redact
from hub.redact import emit

SECRET_TEXT = "田中太郎の相談本文・希死念慮あり・住所は川口市…"
EMAIL = "taro@example.com"


class TestFailClosed(unittest.TestCase):
    def test_unknown_kind_never_leaks(self):
        out = emit(SECRET_TEXT, "totally_unknown_kind", "log", "operator")
        self.assertNotIn("田中", out)
        self.assertNotIn("希死", out)
        self.assertEqual(out, "（分類不明・非表示）")

    def test_none_suppressed(self):
        self.assertEqual(emit(None, "name", "log", "operator"), "（値なし・非表示）")

    def test_structured_dict_never_leaks(self):
        payload = {"氏名": "田中太郎", "本文": SECRET_TEXT}
        out = emit(payload, "freetext", "log", "operator")
        self.assertNotIn("田中", out)
        self.assertNotIn("希死", out)
        self.assertIn("構造化値", out)

    def test_structured_list_and_bytes_never_leak(self):
        for v in ([SECRET_TEXT, EMAIL], SECRET_TEXT.encode("utf-8")):
            out = emit(v, "freetext", "log", "operator")
            self.assertNotIn("田中", out)
            self.assertNotIn("taro@", out)
            self.assertIn("非表示", out)

    def test_huge_value_not_emitted_to_log(self):
        huge = "秘" * 1_000_000
        out = emit(huge, "freetext", "log", "operator")
        self.assertNotIn("秘", out)
        self.assertTrue(len(out) < 100)

    def test_circular_structure_never_leaks_and_no_crash(self):
        d = {"self": None, "secret": SECRET_TEXT}
        d["self"] = d  # 循環参照
        out = emit(d, "freetext", "log", "operator")
        self.assertNotIn("田中", out)
        self.assertIn("構造化値", out)

    def test_emit_internal_exception_degrades(self):
        class Boom:
            def __str__(self):
                raise RuntimeError("boom")
        # scalar 判定を通す型ではないので構造化扱いになるが、len も __str__ も危険。
        # 念のため _emit 内で例外が出ても emit は固定文言へ縮退することを確認。
        out = emit(Boom(), "freetext", "log", "operator")
        self.assertIn("非表示", out)
        self.assertNotIn("boom", out)

    def test_unknown_sink_and_audience_suppressed(self):
        self.assertEqual(emit(SECRET_TEXT, "name", "weird_sink", "operator"),
                         "（非表示）")
        self.assertEqual(emit(SECRET_TEXT, "name", "log", "weird_audience"),
                         "（非表示）")


# sink → 許可 audience（M04 行列に一致させる）
_SINK_AUD = {
    "log": "operator",
    "http_response": "caller",
    "line_business": "attorney",
    "line_customer": "customer",
    "exception_detail": "caller",
}


class TestPassthrough(unittest.TestCase):
    def test_record_id_and_count_passthrough_all_sinks(self):
        for sink, aud in _SINK_AUD.items():
            self.assertEqual(emit("42", "record_id", sink, aud), "42", sink)
            self.assertEqual(emit(7, "count", sink, aud), "7", sink)

    def test_structured_record_id_still_suppressed(self):
        out = emit(["42", "43"], "record_id", "http_response", "caller")
        self.assertIn("構造化値", out)


class TestPiiSuppression(unittest.TestCase):
    def test_pii_suppressed_in_log(self):
        for kind in ("name", "address", "email", "koseki", "asset", "freetext",
                     "birthdate", "phone"):
            out = emit(SECRET_TEXT, kind, "log", "operator")
            self.assertNotIn("田中", out, kind)
            self.assertNotIn("希死", out, kind)
            self.assertEqual(out, f"（{kind}・非表示）")

    def test_pii_suppressed_in_http_response(self):
        out = emit(SECRET_TEXT, "asset", "http_response", "caller")
        self.assertNotIn("田中", out)

    def test_pii_suppressed_in_exception_detail(self):
        out = emit(SECRET_TEXT, "address", "exception_detail", "caller")
        self.assertNotIn("川口", out)

    def test_pii_suppressed_in_line_business_default(self):
        """出し分け水準 OPEN のため既定＝完全抑止（原文・部分マスクを出さない）"""
        out = emit("田中太郎", "name", "line_business", "attorney")
        self.assertNotIn("田中", out)
        self.assertEqual(out, "（name・非表示）")


class TestForbiddenCategories(unittest.TestCase):
    """§13.1: contract/fax/qa/vendor_raw は log/exception/business へ常に完全抑止"""

    def test_forbidden_suppressed_everywhere_but_customer(self):
        raw = "契約書全文 …甲は乙に…"
        for kind in ("contract", "fax", "qa", "vendor_raw"):
            for sink, aud in (("log", "operator"),
                              ("exception_detail", "caller"),
                              ("line_business", "attorney"),
                              ("http_response", "caller")):
                out = emit(raw, kind, sink, aud)
                self.assertNotIn("甲は乙", out, f"{kind}/{sink}")

    def test_vendor_raw_response_suppressed(self):
        vendor = '{"records":[{"氏名":"田中太郎","住所":"川口市…"}]}'
        out = emit(vendor, "vendor_raw", "exception_detail", "caller")
        self.assertNotIn("田中", out)
        self.assertNotIn("川口", out)


class TestDocumentMetadata(unittest.TestCase):
    """L02: document_metadata は既定＝完全非表示（record ID で参照）"""

    def test_document_metadata_suppressed_in_business(self):
        out = emit("委任契約書（田中太郎）", "document_metadata", "line_business",
                   "attorney")
        self.assertNotIn("田中", out)
        self.assertEqual(out, "（document_metadata・非表示）")


class TestSecretKinds(unittest.TestCase):
    """token/secret は全 sink で常時抑止（本人宛でも出さない）"""

    def test_secret_suppressed_all_sinks(self):
        for kind in ("token", "secret"):
            for sink, aud in (("log", "operator"), ("http_response", "caller"),
                              ("line_business", "attorney"),
                              ("line_customer", "customer"),
                              ("exception_detail", "caller")):
                out = emit("Xw3pKm-supersecret", kind, sink, aud)
                self.assertNotIn("Xw3pKm", out, f"{kind}/{sink}")
                self.assertIn("非表示", out)


class TestExternalRef(unittest.TestCase):
    """external_ref（document ID/LINE user ID/追跡番号）は既定=完全抑止"""

    def test_external_ref_suppressed_including_customer(self):
        for sink, aud in (("log", "operator"), ("line_business", "attorney"),
                          ("line_customer", "customer")):
            out = emit("U1234567890abcdef", "external_ref", sink, aud)
            self.assertNotIn("U1234567890abcdef", out, sink)
            self.assertEqual(out, "（external_ref・非表示）")


class TestValueValidation(unittest.TestCase):
    """record_id=英数字等の値域・count=非負整数。外れる値は抑止側へ"""

    def test_record_id_valid_passthrough(self):
        self.assertEqual(emit("42", "record_id", "log", "operator"), "42")
        self.assertEqual(emit(42, "record_id", "log", "operator"), "42")
        self.assertEqual(emit("rec_A-9", "record_id", "log", "operator"), "rec_A-9")

    def test_record_id_invalid_suppressed(self):
        # 記号混じり・空白・PII 混入・負数・巨大長は record_id として無効→抑止
        for bad in ("田中太郎", "42; DROP", "", "a" * 100, -1, True):
            out = emit(bad, "record_id", "log", "operator")
            self.assertEqual(out, "（record_id・非表示）", repr(bad))

    def test_count_valid_passthrough(self):
        self.assertEqual(emit(0, "count", "log", "operator"), "0")
        self.assertEqual(emit("123", "count", "log", "operator"), "123")

    def test_count_invalid_suppressed(self):
        for bad in (-1, "abc", "3.5", True, "田中"):
            out = emit(bad, "count", "log", "operator")
            self.assertEqual(out, "（count・非表示）", repr(bad))


class TestSinkAudienceMatrix(unittest.TestCase):
    """M04: 許可ペア行列の外は完全抑止"""

    def test_allowed_pairs_only(self):
        allowed = {("log", "operator"), ("http_response", "caller"),
                   ("line_business", "attorney"), ("line_customer", "customer"),
                   ("exception_detail", "caller")}
        for sink in redact.SINKS:
            for aud in redact.AUDIENCES:
                out = emit("42", "record_id", sink, aud)
                if (sink, aud) in allowed:
                    self.assertEqual(out, "42", f"{sink}/{aud}")
                else:
                    self.assertEqual(out, "（非表示）", f"{sink}/{aud} 行列外")

    def test_pii_in_wrong_pair_suppressed(self):
        # line_business × customer は行列外 → 抑止
        out = emit("田中太郎", "name", "line_business", "customer")
        self.assertNotIn("田中", out)
        self.assertEqual(out, "（非表示）")


class TestCustomerOwnInfo(unittest.TestCase):
    """line_customer かつ audience=customer は本人の情報として素通し（正当）"""

    def test_customer_own_info_passthrough(self):
        self.assertEqual(
            emit("あなたのご相談を承りました", "freetext", "line_customer",
                 "customer"),
            "あなたのご相談を承りました")

    def test_customer_sink_wrong_audience_suppressed(self):
        out = emit("秘密", "freetext", "line_customer", "attorney")
        self.assertNotIn("秘密", out)

    def test_customer_structured_still_suppressed(self):
        out = emit({"a": "秘密"}, "freetext", "line_customer", "customer")
        self.assertNotIn("秘密", out)


if __name__ == "__main__":
    unittest.main()
