"""P2-CHAIN-011: tools/gas_drift_check.py（INC-0720 §7 規律の機械化・第1段）のテスト。

合成 snapshot／合成 repo（tmp ディレクトリ）のみで検査 — 実 GAS・ネットワーク非依存。
4 系統: 一致（exit 0）／drift（exit 1・行番号のみ）／SIGNED_LANES 行列不一致／
secret 様パターン警告（該当行の内容を出さない）。
"""

import io
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "tools"))
from gas_drift_check import run_check  # noqa: E402

_SIGNING_JS = """// synthetic rv04c_signing copy
var SIGNED_LANES = {
  '/koseki/ingest': true,
  '/registry/ingest': false,
  '/bank/ingest': false,
  '/sortation/ingest': true,
  '/valuation/ingest': false
};
var CHUNK = 8192;
"""


class _Base(unittest.TestCase):
    def setUp(self):
        self._dir = Path(tempfile.mkdtemp(prefix="driftchk_"))
        self.repo = self._dir / "repo"
        self.snap = self._dir / "snap"
        (self.repo / "gas").mkdir(parents=True)
        self.snap.mkdir()
        (self.repo / "gas" / "rv04c_signing.js").write_text(_SIGNING_JS,
                                                            encoding="utf-8")

    def tearDown(self):
        shutil.rmtree(self._dir, ignore_errors=True)

    def _run(self, show_content=False):
        buf = io.StringIO()
        code = run_check(self.snap, self.repo, show_content=show_content, out=buf)
        return code, buf.getvalue()


class TestMatch(_Base):
    def test_identical_snapshot_exit_0(self):
        (self.snap / "rv04c_signing.js").write_text(_SIGNING_JS, encoding="utf-8")
        code, out = self._run()
        self.assertEqual(code, 0, out)
        self.assertIn("[ok]", out)
        self.assertIn("OK", out)            # 行列対比も表示される
        self.assertNotIn("MISMATCH", out)

    def test_empty_snapshot_dir_exit_2(self):
        code, out = self._run()
        self.assertEqual(code, 2, out)


class TestDrift(_Base):
    def test_content_drift_exit_1_line_numbers_only(self):
        drifted = _SIGNING_JS.replace("var CHUNK = 8192;", "var CHUNK = 4096;")
        (self.snap / "rv04c_signing.js").write_text(drifted, encoding="utf-8")
        code, out = self._run()
        self.assertEqual(code, 1, out)
        self.assertIn("[drift]", out)
        self.assertIn("9", out)                    # 不一致行番号（L9）
        self.assertNotIn("4096", out)              # 本文は既定で出さない
        # --show-content 指定時のみ本文を出す
        code2, out2 = self._run(show_content=True)
        self.assertEqual(code2, 1)
        self.assertIn("4096", out2)

    def test_matrix_mismatch_flagged(self):
        # live 側だけ sortation=false（INC-0720 の巻き戻り事故と同型）
        drifted = _SIGNING_JS.replace("'/sortation/ingest': true",
                                      "'/sortation/ingest': false")
        (self.snap / "rv04c_signing.js").write_text(drifted, encoding="utf-8")
        code, out = self._run()
        self.assertEqual(code, 1, out)
        self.assertIn("MISMATCH", out)
        self.assertIn("SIGNED_LANES 行列が期待（repo）と不一致", out)
        self.assertIn("/sortation/ingest: repo(期待)=true live=false", out)


class TestSecretGuard(_Base):
    def test_secret_like_hex_warned_and_never_printed(self):
        secret_hex = "ab" * 32   # 64 桁 hex（HMAC secret 様）
        drifted = _SIGNING_JS + f"var LEAKED = '{secret_hex}';\n"
        (self.snap / "rv04c_signing.js").write_text(drifted, encoding="utf-8")
        code, out = self._run(show_content=True)   # 表示オプション下でも出さない
        self.assertEqual(code, 1)                  # 行追加は drift
        self.assertIn("[warn]", out)
        self.assertIn("secret 様", out)
        self.assertNotIn(secret_hex, out)          # 値は一切出力しない
        self.assertIn("(secret 様のため非表示)", out)


if __name__ == "__main__":
    unittest.main()
