"""P5-001: hub/clause_library（条項ライブラリの器・loader＋検証器）のテスト。

固定する仕様（DRAFT_P5 §1/§2＋裁定 2026-07-27・fix1）:
- repo 内 YAML（clauses/v1）を検証つきで読み込む・違反は ClauseLibraryError
- clause_id grammar（文書種別接頭辞+意味slug+_vN）・ライブラリ内一意
- (doc_type, order) の全 library 一意・doc_type 重複ファイル拒否・
  ファイル名 stem=doc_type（fix1 H01）
- 適用条件は hub/derivation_models の保存語彙（_RELATION_KEYS/_LAWYER_FLAG_KEYS）
  を単一の正として検証・rank は type is int かつ 0-3（bool 遮断）・
  unhashable 要素は固定分類で拒否・requires_human bool（fix1 H02）
- placeholder parser: {{key}}/{{行:key}} の閉集合・対応検査・空キー拒否（fix1 H02）
- 必須 field 完全照合（暗黙補完なし）・str 型強制・version 整合（fix1 M01）
- library_version: 検証合格時のみ hash・version grammar+traversal 遮断・
  境界曖昧性のない hash 材料（fix1 H03）・内容識別能力（fix1 M02）
合成条項のみで検証（実条項は現物到着後の別票）。
"""

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from hub.clause_library import (
    ClauseLibraryError,
    _version_material,
    library_version,
    load_library,
)

_VALID = """\
version: v1
doc_type: test_doc
placeholders:
  - 氏名
  - 財産目録
clauses:
  - clause_id: test_doc_intro_v1
    title: t
    applies:
      rank_in: [1]
      requires_human: false
    body_template: "{{氏名}}は次を取得する。\\n{{行:財産目録}}"
    repeat: per_heir
    order: 10
    since_version: v1
"""


class _TmpLib(unittest.TestCase):
    def setUp(self):
        self._dir = Path(tempfile.mkdtemp(prefix="clauses_"))
        (self._dir / "v1").mkdir()

    def tearDown(self):
        shutil.rmtree(self._dir, ignore_errors=True)

    def _write(self, text: str, name: str = "test_doc.yaml"):
        (self._dir / "v1" / name).write_text(text, encoding="utf-8")

    def _load(self):
        return load_library("v1", base_dir=str(self._dir))


class TestRepoLibraryLoads(unittest.TestCase):
    def test_synthetic_v1_loads_and_validates(self):
        clauses = load_library("v1")            # repo 実体（合成条項）
        ids = [c.clause_id for c in clauses]
        self.assertIn("iso_kyogi_intro_v1", ids)
        self.assertIn("iso_kyogi_acquisition_v1", ids)
        self.assertEqual(len(set(ids)), len(ids))
        per_heir = [c for c in clauses if c.repeat == "per_heir"]
        self.assertTrue(per_heir)
        human = [c for c in clauses if c.applies.get("requires_human")]
        self.assertTrue(human)                  # requires_human 分離の器検証

    def test_library_version_deterministic(self):
        v1, h1 = library_version("v1")
        v2, h2 = library_version("v1")
        self.assertEqual((v1, h1), (v2, h2))
        self.assertEqual(v1, "v1")
        self.assertRegex(h1, r"^[0-9a-f]{64}$")


class TestValidator(_TmpLib):
    def test_valid_file_loads(self):
        self._write(_VALID)
        clauses = self._load()
        self.assertEqual(clauses[0].clause_id, "test_doc_intro_v1")
        self.assertEqual(clauses[0].placeholders_used, ("氏名", "財産目録"))

    def test_bad_clause_id_grammar_rejected(self):
        for bad in ("TestDoc_intro_v1", "test_doc_intro", "test_doc_intro_v0",
                    "test-doc-intro-v1", "intro_v1x"):
            with self.subTest(cid=bad):
                self._write(_VALID.replace("test_doc_intro_v1", bad))
                with self.assertRaises(ClauseLibraryError):
                    self._load()

    def test_doc_type_prefix_enforced(self):
        self._write(_VALID.replace("test_doc_intro_v1", "other_doc_intro_v1"))
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_duplicate_clause_id_rejected(self):
        dup = _VALID + _VALID.split("clauses:\n")[1].replace("order: 10",
                                                             "order: 20")
        self._write(dup)
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_enum_out_of_vocabulary_rejected(self):
        self._write(_VALID.replace("rank_in: [1]",
                                   "rank_in: [1]\n      relation_keys_any: [友人]"))
        with self.assertRaises(ClauseLibraryError):
            self._load()
        self._write(_VALID.replace("rank_in: [1]",
                                   "rank_in: [1]\n      flags_none: [X9]"))
        with self.assertRaises(ClauseLibraryError):
            self._load()
        self._write(_VALID.replace("rank_in: [1]", "rank_in: [4]"))
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_requires_human_must_be_bool(self):
        self._write(_VALID.replace("requires_human: false",
                                   "requires_human: 'yes'"))
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_undefined_placeholder_rejected(self):
        self._write(_VALID.replace("{{氏名}}", "{{未宣言キー}}"))
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_duplicate_order_rejected(self):
        second = _VALID.split("clauses:\n")[1].replace("test_doc_intro_v1",
                                                       "test_doc_second_v1")
        self._write(_VALID + second)            # order 10 が重複
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_repeat_enum_and_unknown_fields_rejected(self):
        self._write(_VALID.replace("repeat: per_heir", "repeat: always"))
        with self.assertRaises(ClauseLibraryError):
            self._load()
        self._write(_VALID.replace("since_version: v1",
                                   "since_version: v1\n    extra_field: 1"))
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_empty_dir_and_broken_yaml_rejected(self):
        with self.assertRaises(ClauseLibraryError):
            self._load()                        # ファイルなし
        self._write("version: v1\n  broken: [unclosed")
        with self.assertRaises(ClauseLibraryError):
            self._load()


_BODY_LINE = 'body_template: "{{氏名}}は次を取得する。\\n{{行:財産目録}}"'


def _with_body(body: str) -> str:
    """_VALID の body_template だけを差し替える（YAML 二重引用は json 準拠で生成）。"""
    return _VALID.replace(_BODY_LINE,
                          f"body_template: {json.dumps(body, ensure_ascii=False)}")


def _second_doc(doc_type: str, order: int = 10) -> str:
    return (_VALID.replace("test_doc", doc_type)
            .replace("order: 10", f"order: {order}"))


class TestLibraryWideUniqueness(_TmpLib):
    """fix1 H01: (doc_type, order) 全 library 一意・doc_type 重複・stem 不一致。"""

    def test_stem_must_equal_doc_type(self):
        self._write(_VALID, name="wrong_name.yaml")
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_duplicate_doc_type_across_files_rejected(self):
        self._write(_VALID)
        # 2ファイル目が同一 doc_type を名乗る（stem 不一致としても検知される）
        self._write(_VALID.replace("test_doc_intro_v1", "test_doc_second_v1")
                    .replace("order: 10", "order: 20"), name="other_doc.yaml")
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_same_order_within_doc_type_rejected_globally(self):
        second = _VALID.split("clauses:\n")[1].replace("test_doc_intro_v1",
                                                       "test_doc_second_v1")
        self._write(_VALID + second)            # 同一 doc_type で order 10 重複
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_same_order_across_doc_types_allowed(self):
        self._write(_VALID)
        self._write(_second_doc("zz_doc", order=10), name="zz_doc.yaml")
        clauses = self._load()                  # doc_type が違えば order 同値は適法
        self.assertEqual(len(clauses), 2)
        self.assertEqual({c.doc_type for c in clauses}, {"test_doc", "zz_doc"})


class TestTypeStrengthening(_TmpLib):
    """fix1 H02: bool rank・unhashable・placeholder parser の負系。"""

    def test_bool_rank_rejected(self):
        self._write(_VALID.replace("rank_in: [1]", "rank_in: [true]"))
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_str_rank_rejected(self):
        self._write(_VALID.replace("rank_in: [1]", "rank_in: ['1']"))
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_unhashable_elements_fixed_classification(self):
        # dict/list 要素でも未処理 TypeError でなく ClauseLibraryError で拒否
        for bad in ("relation_keys_any: [{a: 1}]", "flags_none: [[x]]"):
            with self.subTest(bad=bad):
                self._write(_VALID.replace("rank_in: [1]",
                                           f"rank_in: [1]\n      {bad}"))
                with self.assertRaises(ClauseLibraryError):
                    self._load()

    def test_placeholder_notation_closed_set(self):
        cases = {
            "unclosed": "本文{{氏名",
            "empty": "本文{{}}あり",
            "empty_row_key": "本文{{行:}}あり",
            "stray_close": "本文 } あり{{氏名}}",
            "single_open": "本文 { あり{{氏名}}",
            "nested": "本文{{氏{{名}}}}",
            "padded_key": "本文{{ 氏名 }}",
            "colon_in_key": "本文{{氏:名}}",
        }
        for label, body in cases.items():
            with self.subTest(case=label):
                self._write(_with_body(body))
                with self.assertRaises(ClauseLibraryError):
                    self._load()

    def test_row_marker_notation_accepted(self):
        self._write(_with_body("{{氏名}}\\nと\\n{{行:財産目録}}".replace("\\n", "\n")))
        clauses = self._load()
        self.assertEqual(clauses[0].placeholders_used, ("氏名", "財産目録"))


class TestVersionAndRequiredFields(_TmpLib):
    """fix1 H03（grammar/traversal/検証前置）＋M01（必須 field・str 強制・整合）。"""

    def test_version_grammar_and_traversal_rejected(self):
        self._write(_VALID)
        for bad in ("V1", "v0", "v01", "1", "../v1", "v1/../v1", ""):
            with self.subTest(version=bad):
                with self.assertRaises(ClauseLibraryError):
                    load_library(bad, base_dir=str(self._dir))
                with self.assertRaises(ClauseLibraryError):
                    library_version(bad, base_dir=str(self._dir))

    def test_library_version_refuses_empty_or_invalid(self):
        with self.assertRaises(ClauseLibraryError):
            library_version("v1", base_dir=str(self._dir))   # 空 library に hash なし
        self._write("version: v1\n  broken: [unclosed")
        with self.assertRaises(ClauseLibraryError):
            library_version("v1", base_dir=str(self._dir))   # 不正 library も同様

    def test_file_version_must_match_directory(self):
        self._write(_VALID.replace("version: v1", "version: v2"))
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_since_version_not_newer_than_directory(self):
        self._write(_VALID.replace("since_version: v1", "since_version: v2"))
        with self.assertRaises(ClauseLibraryError):
            self._load()
        self._write(_VALID.replace("since_version: v1", "since_version: '1.0'"))
        with self.assertRaises(ClauseLibraryError):
            self._load()

    def test_missing_required_fields_rejected(self):
        for line in ("    title: t\n", "    repeat: per_heir\n",
                     "    order: 10\n", "    since_version: v1\n"):
            with self.subTest(missing=line.strip()):
                self._write(_VALID.replace(line, ""))
                with self.assertRaises(ClauseLibraryError):
                    self._load()

    def test_string_fields_require_str_type(self):
        self._write(_VALID.replace("title: t", "title: 123"))
        with self.assertRaises(ClauseLibraryError):
            self._load()
        self._write(_VALID.replace("since_version: v1",
                                   "since_version: v1\n    notes: [x]"))
        with self.assertRaises(ClauseLibraryError):
            self._load()


class TestVersionHashCapability(_TmpLib):
    """fix1 M02/H03-iii: 版 hash の内容識別能力と境界曖昧性の排除。"""

    def _hash(self):
        return library_version("v1", base_dir=str(self._dir))[1]

    def test_creation_order_invariant(self):
        self._write(_VALID)
        self._write(_second_doc("zz_doc", order=20), name="zz_doc.yaml")
        h1 = self._hash()
        other = Path(tempfile.mkdtemp(prefix="clauses_"))
        try:
            (other / "v1").mkdir()
            (other / "v1" / "zz_doc.yaml").write_text(
                _second_doc("zz_doc", order=20), encoding="utf-8")
            (other / "v1" / "test_doc.yaml").write_text(_VALID, encoding="utf-8")
            self.assertEqual(h1, library_version("v1", base_dir=str(other))[1])
        finally:
            shutil.rmtree(other, ignore_errors=True)

    def test_one_byte_content_change_changes_hash(self):
        self._write(_VALID)
        h1 = self._hash()
        self._write(_VALID.replace("title: t", "title: u"))
        self.assertNotEqual(h1, self._hash())

    def test_rename_changes_hash(self):
        self._write(_VALID)
        h1 = self._hash()
        (self._dir / "v1" / "test_doc.yaml").unlink()
        self._write(_second_doc("zz_doc", order=10), name="zz_doc.yaml")
        self.assertNotEqual(h1, self._hash())

    def test_add_and_remove_file_changes_hash(self):
        self._write(_VALID)
        h1 = self._hash()
        self._write(_second_doc("zz_doc", order=20), name="zz_doc.yaml")
        h2 = self._hash()
        self.assertNotEqual(h1, h2)
        (self._dir / "v1" / "zz_doc.yaml").unlink()
        self.assertEqual(h1, self._hash())      # 削除で元 hash へ復帰=決定性

    def test_boundary_ambiguity_eliminated_in_material(self):
        # 旧方式（name+content の素連結）では同一バイト列になる境界違いが別材料になる
        self.assertNotEqual(_version_material([("a.yaml", b"XY")]),
                            _version_material([("a.yamlX", b"Y")]))
        self.assertNotEqual(
            _version_material([("a.yaml", b"x"), ("b.yaml", b"y")]),
            _version_material([("a.yaml", b"xb.yamly")]))
        self.assertNotEqual(_version_material([("a.yaml", b"X")]),
                            _version_material([("b.yaml", b"X")]))


if __name__ == "__main__":
    unittest.main()
