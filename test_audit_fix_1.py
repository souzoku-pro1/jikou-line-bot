"""AUDIT-FIX-1（QUEUE-2026-10-16-R G-1）: AUDIT-1 の「要修正」の修正を固定する

- #1 hub/kintone.delete_record: HTTP DELETE を送らず常に PhysicalDeleteDisabled（RV-08: 論理削除のみ）。
  hub/kintone.py に HTTP 動詞 "DELETE" の文字列が無い（AST で定数を走査）。既存テストが patch.object の対象にするため名前・署名は残す。
- #2 requirements.lock の urllib3 は 2.8.0 以上（PYSEC-2026-4175／4176／4177 の修正版）・SBOM の purl も一致。
"""

import ast
import asyncio
import re
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from hub import kintone

REPO = Path(__file__).parent


class TestDeleteDisabled(unittest.TestCase):
    def test_delete_record_raises_without_http(self):
        app = kintone.KintoneApp("App X", "KINTONE_APP_ID", "KINTONE_API_TOKEN")
        write = AsyncMock()
        with patch.object(kintone, "_write", write):
            with self.assertRaises(kintone.PhysicalDeleteDisabled):
                asyncio.run(kintone.delete_record(app, "1"))
        write.assert_not_called()
        self.assertTrue(issubclass(kintone.PhysicalDeleteDisabled, RuntimeError))

    def test_no_http_delete_verb_in_kintone_module(self):
        tree = ast.parse((REPO / "hub" / "kintone.py").read_text(encoding="utf-8"))
        consts = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        self.assertNotIn("DELETE", consts)
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "_write"]
        verbs = {c.args[0].value for c in calls if c.args and isinstance(c.args[0], ast.Constant)}
        self.assertEqual(verbs, {"POST", "PUT"})


class TestLockUrllib3(unittest.TestCase):
    def _version(self, text: str, pattern: str) -> tuple:
        m = re.search(pattern, text, re.M)
        self.assertIsNotNone(m)
        return tuple(int(x) for x in m.group(1).split("."))

    def test_lock_and_sbom_have_fixed_urllib3(self):
        lock = (REPO / "requirements.lock").read_text(encoding="utf-8")
        v = self._version(lock, r"^urllib3==(\d+\.\d+\.\d+)", )
        self.assertGreaterEqual(v, (2, 8, 0))
        sbom = (REPO / "sbom" / "sbom.cdx.json").read_text(encoding="utf-8")
        sv = self._version(sbom, r"pkg:pypi/urllib3@(\d+\.\d+\.\d+)")
        self.assertEqual(sv, v)


if __name__ == "__main__":
    unittest.main()
