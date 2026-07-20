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


# ── fix1(P2DRIFT-H01/H02/M01) 追加テスト ─────────────────────────────────────
class TestSecretPatternsExpanded(_Base):
    """H01: 非 hex の secret 様 5 種も警告し、--show-content でも値を出さない。"""

    _PAYLOADS = {
        "base64": "var B = 'QWxhZGRpbjpvcGVuIHNlc2FtZUFsYWRkaW46b3BlbnNlc2FtZQ==';",
        "jwt": "var J = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload';",
        "gapi_key": "var G = 'AIzaSyD0123456789abcdefghijklmnopqrstu';",
        "assign": "var KOSEKI_TOKEN = 'supersecretvalue99';",
        "url_token": "var U = RAILWAY_URL + '/koseki/ingest?token=abcdef1234567890';",
    }

    def test_five_nonhex_patterns_warned_and_hidden(self):
        for label, payload in self._PAYLOADS.items():
            with self.subTest(pattern=label):
                (self.snap / "rv04c_signing.js").write_text(
                    _SIGNING_JS + payload + "\n", encoding="utf-8")
                code, out = self._run(show_content=True)
                self.assertEqual(code, 1, (label, out))   # 行追加は drift
                self.assertIn("[warn]", out, label)
                self.assertIn("secret 様", out, label)
                self.assertNotIn(payload, out, label)      # 行本文は恒久非表示
                self.assertIn("(secret 様のため非表示)", out, label)


class TestRequiredFileGate(_Base):
    """H02: rv04c_signing.js 欠落は SIGNED_LANES 比較不能として exit 2（false green 遮断）。"""

    def test_missing_required_file_exit_2(self):
        (self.snap / "other.js").write_text("var X = 1;\n", encoding="utf-8")
        code, out = self._run()
        self.assertEqual(code, 2, out)
        self.assertIn("必須ファイル rv04c_signing.js が snapshot にありません", out)
        self.assertIn("SIGNED_LANES 比較不能", out)


class TestBrokenInputContract(_Base):
    """M01: 壊れた入力・repo-root 不正は traceback でなく固定文言＋exit 2。"""

    def test_non_utf8_snapshot_exit_2(self):
        (self.snap / "rv04c_signing.js").write_bytes(b"\xff\xfe\x00\x81broken")
        code, out = self._run()   # 例外が漏れず戻り値で返ることも同時に検証
        self.assertEqual(code, 2, out)
        self.assertIn("UTF-8 として読めません", out)

    def test_invalid_repo_root_exit_2(self):
        import io
        from gas_drift_check import run_check
        (self.snap / "rv04c_signing.js").write_text(_SIGNING_JS, encoding="utf-8")
        buf = io.StringIO()
        code = run_check(self.snap, self._dir / "no-such-repo", out=buf)
        self.assertEqual(code, 2, buf.getvalue())
        self.assertIn("repo-root 不正", buf.getvalue())


class TestSecretFilenameHidden(_Base):
    """H01: secret 様文字列を含むファイル名は出力しない。"""

    def test_secret_like_filename_not_printed(self):
        (self.snap / "rv04c_signing.js").write_text(_SIGNING_JS, encoding="utf-8")
        bad_name = "AIzaSyD0123456789abcdefghijklmnopqrstu.js"
        (self.snap / bad_name).write_text("var X = 1;\n", encoding="utf-8")
        code, out = self._run()
        self.assertEqual(code, 1, out)             # 余分ファイル＝drift
        self.assertNotIn(bad_name, out)            # ファイル名を出さない
        self.assertNotIn("AIzaSyD", out)
        self.assertIn("ファイル名の snapshot を検出", out)


class TestManifestCheck(_Base):
    """fix1: manifest 照合 — 余分（repo に無い）・不足（snapshot に無い）の検出。"""

    def test_snapshot_extra_file_detected(self):
        (self.snap / "rv04c_signing.js").write_text(_SIGNING_JS, encoding="utf-8")
        (self.snap / "unknown.js").write_text("var X = 1;\n", encoding="utf-8")
        code, out = self._run()
        self.assertEqual(code, 1, out)
        self.assertIn("manifest 余分", out)

    def test_repo_file_missing_from_snapshot_detected(self):
        (self.repo / "gas" / "rv04c_selftest.js").write_text("var T = 1;\n",
                                                             encoding="utf-8")
        (self.snap / "rv04c_signing.js").write_text(_SIGNING_JS, encoding="utf-8")
        code, out = self._run()
        self.assertEqual(code, 1, out)
        self.assertIn("gas/rv04c_selftest.js が snapshot にありません", out)
        self.assertIn("manifest 不足", out)


# ── fix2(P2DRIFT2-H01/H02/M01/M02) 追加テスト ────────────────────────────────
class TestRepoSideSecretMasked(_Base):
    """H01(fix2): repo 側のみ secret の行も --show-content で両側非表示。"""

    def test_repo_only_secret_sentinel_not_exposed(self):
        repo_secret = "cd" * 32
        repo_js = _SIGNING_JS + f"var REPO_ONLY = '{repo_secret}';\n"
        (self.repo / "gas" / "rv04c_signing.js").write_text(repo_js, encoding="utf-8")
        # live 側は同じ行が secret でない値（drift だが live 行自体は無害）
        snap_js = _SIGNING_JS + "var REPO_ONLY = 'x';\n"
        (self.snap / "rv04c_signing.js").write_text(snap_js, encoding="utf-8")
        code, out = self._run(show_content=True)
        self.assertEqual(code, 1, out)
        self.assertNotIn(repo_secret, out)             # repo 側 sentinel を出さない
        self.assertIn("(secret 様のため非表示)", out)   # 両側マスク
        self.assertNotIn("var REPO_ONLY = 'x';", out)  # live 側も出さない（両側非表示）


class TestSecretFilenameErrorPath(_Base):
    """H01/M01(fix2): secret 様ファイル名はエラー経路（UTF-8 失敗等）でも非露出。"""

    def test_error_path_does_not_expose_secret_filename(self):
        (self.snap / "rv04c_signing.js").write_text(_SIGNING_JS, encoding="utf-8")
        bad_name = "AIzaSyD0123456789abcdefghijklmnopqrstu.js"
        (self.snap / bad_name).write_bytes(b"\xff\xfe\x00broken")
        code, out = self._run()
        self.assertEqual(code, 2, out)                 # 壊れ入力は exit 2
        self.assertIn("UTF-8 として読めません", out)
        self.assertNotIn("AIzaSyD", out)               # 名前はエラー文にも出さない
        self.assertIn("(secret 様のためファイル名非表示)", out)


class TestLegacyManifest(_Base):
    """M02(fix2): manifest 正本は gas/ + legacy/gas/。コード.js 欠落は false green にしない。"""

    def test_legacy_code_js_missing_detected(self):
        (self.repo / "legacy" / "gas").mkdir(parents=True)
        (self.repo / "legacy" / "gas" / "コード.js").write_text("var L = 1;\n",
                                                               encoding="utf-8")
        (self.snap / "rv04c_signing.js").write_text(_SIGNING_JS, encoding="utf-8")
        code, out = self._run()
        self.assertEqual(code, 1, out)
        self.assertIn("legacy/gas/コード.js が snapshot にありません", out)
        self.assertIn("manifest 不足", out)


class TestStructuredWriterGuards(_Base):
    """H02(fix2): 構造化 writer は sentinel 混入を構造的に出力しない。"""

    def test_emit_rejects_secret_like_field(self):
        import io
        from gas_drift_check import report
        buf = io.StringIO()
        with self.assertRaises(ValueError):
            report(buf, "ok_line", name="AIzaSyD0123456789abcdefghijklmnopqrstu.js",
                 repo_name="gas/x.js")
        self.assertEqual(buf.getvalue(), "")           # 何も出力されない

    def test_emit_rejects_unknown_field_and_template(self):
        import io
        from gas_drift_check import report
        buf = io.StringIO()
        with self.assertRaises(ValueError):
            report(buf, "ok_line", name="a.js", repo_name="gas/a.js",
                 free_text="任意文字列は受けない")
        with self.assertRaises(KeyError):
            report(buf, "no_such_template")
        self.assertEqual(buf.getvalue(), "")

    def test_content_line_masks_sentinel_even_if_flag_false(self):
        import io
        from gas_drift_check import report_content_line
        sentinel = "ab" * 32
        buf = io.StringIO()
        # 呼び出し側が is_secret=False と誤判定しても内部判定でマスクされる
        report_content_line(buf, 7, "live", f"var X = '{sentinel}';", False)
        self.assertNotIn(sentinel, buf.getvalue())
        self.assertIn("(secret 様のため非表示)", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
