"""KOSEKI-DATA-1: 読解結果の決定的正規化と App33 構造化 field 書き戻し。

固定する仕様:
- parse_wareki: 和暦→西暦の決定的変換。grammar 不成立・元号範囲外・暦に無い
  日付・余計な文字付き・**元号の実在期間（日単位閉区間）外**（fix1 01）は
  None（誤変換より欠落＝fail-closed）。全角数字吸収。
- normalize_reading: 機械変換で 編製日_西暦/消除日_西暦 を充足・
  人物[].生年月日_西暦 を新設。**モデル申告 ISO の採用は和暦原文が空の場合
  のみ**（fix1 02 裁定）——原文ありで変換不能は null＋「西暦変換不能」キー。
  既存キーは不変（後方互換）・冪等。confidence 判定系は完全不変。
- structured_fields: 厳密検証済みの値のみ（様式∈FORMS・ISO 日付のみ）。
  値の無い field はキーを含めない。
- process_record: 保存 JSON が正規化済み・構造化 field が同一 update に同梱
  （update は従来どおり 1 回＝既存 pin の維持）。
- koseki_backfill: dry-run は書込みゼロ・apply は読解状態に触れない payload。
"""

import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

from test_koseki_reader import _Base, book_record, valid_reading

import koseki_backfill
from koseki_reader import (
    normalize_reading,
    parse_wareki,
    structured_fields,
)


# ── parse_wareki（変換仕様） ─────────────────────────────────────────────────
class TestParseWareki(unittest.TestCase):
    def test_valid_conversions(self):
        cases = {
            "明治40年1月5日": "1907-01-05",
            "明治45年7月29日": "1912-07-29",     # 明治最終年
            "大正元年8月1日": "1912-08-01",      # 元年
            "大正15年12月24日": "1926-12-24",
            "昭和32年4月1日": "1957-04-01",
            "昭和64年1月7日": "1989-01-07",      # 昭和最終日側の年
            "平成元年1月8日": "1989-01-08",
            "平成31年4月30日": "2019-04-30",
            "令和2年2月29日": "2020-02-29",      # 閏日（実在）
            "昭和３２年４月１日": "1957-04-01",   # 全角数字
            " 昭和32年4月1日 ": "1957-04-01",    # 前後空白
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(parse_wareki(text), expected)

    def test_era_boundaries_day_level(self):
        # KOSEKI-DATA-1-fix1（01）: 各改元境界の直前・当日・直後（「暦には
        # あるがその元号には無い日」を null へ）
        positives = {
            "明治元年1月25日": "1868-01-25",     # 明治開始（遡及適用の初日）
            "明治45年7月29日": "1912-07-29",     # 明治最終日
            "大正元年7月30日": "1912-07-30",     # 大正初日
            "大正15年12月24日": "1926-12-24",    # 大正最終日
            "昭和元年12月25日": "1926-12-25",    # 昭和初日
            "昭和64年1月7日": "1989-01-07",      # 昭和最終日
            "平成元年1月8日": "1989-01-08",      # 平成初日
            "平成31年4月30日": "2019-04-30",     # 平成最終日
            "令和元年5月1日": "2019-05-01",      # 令和初日
        }
        for text, expected in positives.items():
            with self.subTest(text=text):
                self.assertEqual(parse_wareki(text), expected)
        negatives = (
            "明治元年1月24日",     # 明治開始前日相当
            "明治45年7月30日",     # 明治には無い日（大正元年7月30日）
            "大正元年7月29日",     # 大正開始前日相当
            "大正15年12月25日",    # 大正には無い日（昭和元年12月25日）
            "昭和元年12月24日",    # 昭和開始前日相当
            "昭和64年1月8日",      # 昭和には無い日（平成元年1月8日）
            "平成元年1月7日",      # 平成開始前日相当
            "平成31年5月1日",      # 平成には無い日（令和元年5月1日）
            "令和元年4月30日",     # 令和開始前日相当
        )
        for text in negatives:
            with self.subTest(text=text):
                self.assertIsNone(parse_wareki(text))

    def test_fail_closed_returns_none(self):
        for text in ("昭和99年1月1日",           # 元号範囲外
                     "平成32年1月1日",           # 平成31まで
                     "昭和60年2月30日",          # 暦に無い日
                     "令和3年2月29日",           # 閏でない年の閏日
                     "昭和32年13月1日",          # 月不正
                     "昭和32年4月1日編製",       # 余計な文字（切り出しはしない）
                     "西暦1957年4月1日",         # 未知の元号
                     "昭和32年4月",              # 日欠落
                     "1957-04-01",               # 和暦でない
                     "", None):
            with self.subTest(text=text):
                self.assertIsNone(parse_wareki(text))


# ── normalize_reading ────────────────────────────────────────────────────────
class TestNormalizeReading(unittest.TestCase):
    def test_deterministic_conversion_wins_over_model_value(self):
        reading = {"戸籍": {"編製日": "昭和32年4月1日",
                            "編製日_西暦": "9999-12-31",   # モデル誤申告
                            "消除日": "", "消除日_西暦": None,
                            "confidence": {}}}
        normalize_reading(reading)
        self.assertEqual(reading["戸籍"]["編製日_西暦"], "1957-04-01")
        self.assertIsNone(reading["戸籍"]["消除日_西暦"])

    def test_model_iso_rejected_when_wareki_present_but_unparseable(self):
        # KOSEKI-DATA-1-fix1（02 裁定・仕様変更＝厳格化）: 原文が存在して機械
        # 変換に失敗した場合はモデル申告 ISO で救済せず null＋「西暦変換不能」
        # （旧仕様「モデル ISO を残す」は本裁定で廃止・緩和ではなく厳格化）
        reading = {"戸籍": {"編製日": "昭和32年4月1日編製",
                            "編製日_西暦": "1957-04-01", "confidence": {}}}
        normalize_reading(reading)
        self.assertIsNone(reading["戸籍"]["編製日_西暦"])
        self.assertEqual(reading["戸籍"]["西暦変換不能"], ["編製日_西暦"])

    def test_model_iso_adopted_only_when_source_empty(self):
        # fix1（02 裁定）: 原文空のときのみモデル申告 ISO を採用・不正 ISO は null
        reading = {"戸籍": {"編製日": "", "編製日_西暦": "1957-04-01",
                            "消除日": "", "消除日_西暦": "not-a-date",
                            "confidence": {}}}
        normalize_reading(reading)
        self.assertEqual(reading["戸籍"]["編製日_西暦"], "1957-04-01")
        self.assertIsNone(reading["戸籍"]["消除日_西暦"])
        self.assertNotIn("西暦変換不能", reading["戸籍"])   # 原文空=マークなし

    def test_unconvertible_with_source_is_null_and_marked(self):
        reading = {"戸籍": {"編製日": "昭和参拾弐年卯月朔日",   # 変換不能な原文
                            "編製日_西暦": "not-a-date",
                            "confidence": {"編製日": 0.8}}}
        normalize_reading(reading)
        self.assertIsNone(reading["戸籍"]["編製日_西暦"])
        self.assertEqual(reading["戸籍"]["西暦変換不能"], ["編製日_西暦"])
        # confidence マップは不変（全体確信度＝要再読解判定を汚染しない）
        self.assertEqual(reading["戸籍"]["confidence"], {"編製日": 0.8})

    def test_empty_source_stays_null_without_mark(self):
        reading = {"戸籍": {"消除日": "", "消除日_西暦": None,
                            "confidence": {}}}
        normalize_reading(reading)
        self.assertIsNone(reading["戸籍"]["消除日_西暦"])
        self.assertNotIn("西暦変換不能", reading["戸籍"])

    def test_person_birth_seireki_added(self):
        reading = valid_reading()
        normalize_reading(reading)
        persons = reading["人物"]
        self.assertEqual(persons[0]["生年月日_西暦"], "1907-01-05")
        self.assertEqual(persons[1]["生年月日_西暦"], "1912-03-03")  # 明治45年
        # 既存キーは不変（後方互換）
        self.assertEqual(persons[0]["生年月日"], "明治40年1月5日")

    def test_idempotent_and_tolerant(self):
        reading = valid_reading()
        once = json.dumps(normalize_reading(json.loads(
            json.dumps(reading, ensure_ascii=False))), ensure_ascii=False,
            sort_keys=True)
        twice_src = json.loads(once)
        twice = json.dumps(normalize_reading(twice_src), ensure_ascii=False,
                           sort_keys=True)
        self.assertEqual(once, twice)                  # 冪等
        self.assertEqual(normalize_reading({}), {})    # 構造欠落は素通し
        self.assertEqual(normalize_reading("x"), "x")  # 非 dict は素通し


# ── structured_fields ────────────────────────────────────────────────────────
class TestStructuredFields(unittest.TestCase):
    def test_full_mapping(self):
        saved = {"様式": "改製原（昭和）",
                 "戸籍": {"編製日_西暦": "1957-04-01",
                          "消除日_西暦": "1996-03-01"}}
        self.assertEqual(structured_fields(saved),
                         {"戸籍種別": "改製原（昭和）",
                          "編製日": "1957-04-01", "消除日": "1996-03-01"})

    def test_only_validated_values_included(self):
        saved = {"様式": "勝手な様式",                  # FORMS 外 → 含めない
                 "戸籍": {"編製日_西暦": "1957/04/01",   # ISO でない → 含めない
                          "消除日_西暦": None}}
        self.assertEqual(structured_fields(saved), {})
        self.assertEqual(structured_fields({}), {})
        self.assertEqual(structured_fields("x"), {})


# ── process_record への統合（mock kintone/Claude・update 1 回の pin 維持） ───
class TestPipelineIntegration(_Base):
    def test_saved_json_normalized_and_structured_fields_included(self):
        self.arm()
        result = self.run_one()
        self.assertEqual(result["status"], "ai_done")
        fields = self.saved_fields()      # 分割代入＝update 1 回のままを暗黙 pin
        self.assertEqual(fields["戸籍種別"], "改製原（昭和）")
        self.assertEqual(fields["編製日"], "1957-04-01")
        self.assertNotIn("消除日", fields)             # 消除なし→キー自体なし
        saved = json.loads(fields["読解JSON"])
        self.assertEqual(saved["戸籍"]["編製日_西暦"], "1957-04-01")
        self.assertEqual(saved["人物"][0]["生年月日_西暦"], "1907-01-05")
        self.assertEqual(saved["人物"][1]["生年月日_西暦"], "1912-03-03")

    def test_empty_ocr_path_has_no_structured_fields(self):
        self.arm(record=book_record(reading_json=json.dumps(
            {"ocr_text": ""}, ensure_ascii=False)))
        result = self.run_one()
        self.assertEqual(result["status"], "needs_reread")
        fields = self.saved_fields()
        for key in ("戸籍種別", "編製日", "消除日"):
            self.assertNotIn(key, fields)


# ── koseki_backfill（移行スクリプト） ────────────────────────────────────────
class TestBackfill(unittest.TestCase):
    def _records(self):
        reading = valid_reading()
        reading["ocr_text"] = "OCR原文"
        return [
            {"$id": {"value": "1"}, "$revision": {"value": "3"},
             "案件レコードID": {"value": "3"},
             "読解状態": {"value": "AI読解済"},
             "読解JSON": {"value": json.dumps(reading, ensure_ascii=False)},
             "戸籍種別": {"value": ""}, "編製日": {"value": ""},
             "消除日": {"value": ""}},
            {"$id": {"value": "9"}, "$revision": {"value": "3"},
             "案件レコードID": {"value": ""},
             "読解状態": {"value": "AI読解済"},
             "読解JSON": {"value": json.dumps(reading, ensure_ascii=False)},
             "戸籍種別": {"value": ""}, "編製日": {"value": ""},
             "消除日": {"value": ""}},
            {"$id": {"value": "10"}, "案件レコードID": {"value": "4"},
             "読解状態": {"value": "AI読解済"},
             "読解JSON": {"value": "{{broken"},
             "戸籍種別": {"value": ""}, "編製日": {"value": ""},
             "消除日": {"value": ""}},
        ]

    def _run(self, apply, records=None, update_error=None):
        import contextlib
        import io
        updates = []

        async def search(app, query, fields=None):
            if app.app_id_env == "APP_KOSEKI_BOOK":
                return records if records is not None else self._records()
            return [{"$id": {"value": "3"}}]          # App26 案件一覧

        async def update(app, rid, fields, revision=None):
            if update_error is not None:
                raise update_error
            updates.append((rid, fields, revision))

        load_persons = AsyncMock(return_value={
            "records": [{"$id": {"value": "1"},
                         "氏名": {"value": "山田太郎"}}],
            "excluded_merged_count": 0})
        out = io.StringIO()
        with patch.dict(os.environ, {"APP_KOSEKI_BOOK": "33",
                                     "TOKEN_KOSEKI_BOOK": "t"}), \
             patch("hub.kintone.search_records", new=search), \
             patch("hub.kintone.update_record", new=update), \
             patch("hub.webapp_souzoku_dashboard._load_persons",
                   new=load_persons), \
             contextlib.redirect_stdout(out):
            rc = asyncio.run(koseki_backfill.run(apply=apply))
        return rc, updates, out.getvalue()

    def _record(self, rid, *, case="4", shubetsu="", hensei="", rev="7"):
        reading = valid_reading()
        reading["ocr_text"] = "OCR原文"
        return {"$id": {"value": rid}, "$revision": {"value": rev},
                "案件レコードID": {"value": case},
                "読解状態": {"value": "AI読解済"},
                "読解JSON": {"value": json.dumps(reading, ensure_ascii=False)},
                "戸籍種別": {"value": shubetsu}, "編製日": {"value": hensei},
                "消除日": {"value": ""}}

    def test_dry_run_writes_nothing(self):
        rc, updates, _out = self._run(apply=False)
        self.assertEqual(rc, 0)
        self.assertEqual(updates, [])

    def test_apply_writes_normalized_payload_without_status(self):
        rc, updates, _out = self._run(apply=True)
        self.assertEqual(rc, 0)
        # 壊れ JSON（$id=10）は書込み対象外・他 2 件が書かれる
        self.assertEqual([rid for rid, _, _r in updates], ["1", "9"])
        for _, fields, _rev in updates:
            self.assertNotIn("読解状態", fields)       # R4 専権に触れない
            self.assertEqual(fields["戸籍種別"], "改製原（昭和）")
            self.assertEqual(fields["編製日"], "1957-04-01")
            self.assertNotIn("消除日", fields)
            saved = json.loads(fields["読解JSON"])
            self.assertEqual(saved["人物"][0]["生年月日_西暦"], "1907-01-05")
            self.assertEqual(saved["ocr_text"], "OCR原文")   # 既存キー温存

    def test_manual_values_never_overwritten(self):
        # fix1（03）: 既存値と生成値の不一致（人手修正の可能性）は --apply でも
        # 上書きされず、手作業送り一覧に載る。一致は write 0（スキップ）
        records = [
            self._record("21", shubetsu="現行"),          # 生成=改製原（昭和）と不一致
            self._record("22", shubetsu="改製原（昭和）",   # 全 field 一致
                         hensei="1957-04-01"),
        ]
        rc, updates, out = self._run(apply=True, records=records)
        self.assertEqual(rc, 0)
        by_rid = {rid: fields for rid, fields, _r in updates}
        # 不一致 field（戸籍種別）は書かれない（編製日は空→充足）
        self.assertNotIn("戸籍種別", by_rid["21"])
        self.assertEqual(by_rid["21"]["編製日"], "1957-04-01")
        self.assertIn("$id=21: 戸籍種別 の既存値と生成値が不一致", out)
        self.assertIn("上書きせず現状維持", out)
        # 一致は write 0: $id=22 は構造化 field を一切書かない（JSON 更新のみ）
        self.assertEqual(set(by_rid["22"]) - {"読解JSON"}, set())

    def test_revision_passed_and_conflict_continues(self):
        # fix1（03）: revision 照合つき update。競合（KintoneError）は当該
        # レコードのみエラー扱いで継続・rc=1
        from hub.kintone import KintoneError
        rc, updates, out = self._run(apply=True)
        self.assertEqual([rev for _, _f, rev in updates], ["3", "3"])
        rc, updates, out = self._run(
            apply=True, update_error=KintoneError(409, "GAIA_CO02", "競合"))
        self.assertEqual(rc, 1)
        self.assertIn("KintoneError status=409 code=GAIA_CO02", out)

    def test_failure_output_is_pii_safe(self):
        # fix1（04）: 例外本文（PII を含み得る）を stdout へ出さない——
        # 分類名のみの閉集合
        rc, _updates, out = self._run(
            apply=True,
            update_error=RuntimeError("SENTINEL-氏名-XY が含まれる例外本文"))
        self.assertEqual(rc, 1)
        self.assertIn("RuntimeError", out)
        self.assertNotIn("SENTINEL", out)
        self.assertNotIn("氏名-XY", out)

    def test_backfill_never_writes_reading_state_statically(self):
        # 静的 pin: 読解状態 は文字として fields へ入り得る箇所が無い
        src = open("koseki_backfill.py", encoding="utf-8").read()
        self.assertNotIn('"読解状態"] =', src)
        self.assertNotIn("fields[\"読解状態\"]", src)


if __name__ == "__main__":
    unittest.main()
