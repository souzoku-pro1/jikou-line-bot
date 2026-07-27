"""P5-001: hub/clause_library（条項ライブラリの器・loader＋検証器）のテスト。

固定する仕様（DRAFT_P5 §1/§2＋裁定 2026-07-27）:
- repo 内 YAML（clauses/v1）を検証つきで読み込む・違反は ClauseLibraryError
- clause_id grammar（文書種別接頭辞+意味slug+_vN）・ライブラリ内一意
- 適用条件は hub/derivation_models の保存語彙（_RELATION_KEYS/_LAWYER_FLAG_KEYS）
  を単一の正として検証・rank 0-3・requires_human bool
- placeholder 未定義の拒否・order の文書種別内重複拒否・repeat enum
- library_version = (version, 内容 SHA-256) の決定性
合成条項のみで検証（実条項は現物到着後の別票）。
"""

import shutil
import tempfile
import unittest
from pathlib import Path

from hub.clause_library import (
    ClauseLibraryError,
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


if __name__ == "__main__":
    unittest.main()
