"""HOUKI-JUKURYO-2-fix4（Codex R-HOUKI-JUKURYO-2-fix3 BH-12）: scripts/houki_jukuryo_precount.py の
CLI 入口が、import・取得・集計のどこで失敗しても例外本文・Traceback・依存ロガーの出力を漏らさず、
固定の理由コードと非ゼロ終了で終わること。正常経路は件数行のみ（日付・ID なし）。

subprocess で実行し stdout/stderr を丸ごと検査する。注入点は環境変数 HOUKI_PRECOUNT_FAULT。
"""

import importlib.util
import os
import re
import subprocess
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent
SCRIPT = REPO / "scripts" / "houki_jukuryo_precount.py"
MARKER = "LEAKMARKER_9f3a7c2e"
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_BASE_ENV = {k: v for k, v in os.environ.items()
             if k not in ("HOUKI_PRECOUNT_FAULT", "HOUKI_PRECOUNT_FAULT_MARKER")}
_BASE_ENV.update({"PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1",
                  # hub 側の import に要る最小 env（値はダミー・kintone には触れない）
                  "KINTONE_SUBDOMAIN": "testsub", "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "d",
                  "APP_HOUKI": "40", "TOKEN_HOUKI": "d"})


def _run(fault: str, marker: str = MARKER):
    env = dict(_BASE_ENV)
    if fault:
        env["HOUKI_PRECOUNT_FAULT"] = fault
        env["HOUKI_PRECOUNT_FAULT_MARKER"] = marker
    return subprocess.run([sys.executable, str(SCRIPT)], cwd=str(REPO), env=env,
                          capture_output=True, text=True, encoding="utf-8", timeout=300)


class _Common(unittest.TestCase):
    def assert_no_leak(self, proc, reason: str):
        out, err = proc.stdout, proc.stderr
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(out.strip().splitlines(), [reason])            # 理由コード 1 行のみ
        self.assertEqual(err.strip(), "")                                # stderr に何も出ない
        for leak in (MARKER, "Traceback", "Error", "error", "Exception", "File \"", "httpx", "ModuleNotFound"):
            self.assertNotIn(leak, out)
            self.assertNotIn(leak, err)


class TestFailurePaths(_Common):
    def test_a_fetch_failure_is_reason_code_only(self):
        self.assert_no_leak(_run("fetch"), "PRECOUNT_FAILED:fetch")

    def test_b_import_failure_is_reason_code_only(self):
        self.assert_no_leak(_run("import"), "PRECOUNT_FAILED:import")

    def test_compute_failure_is_reason_code_only(self):
        self.assert_no_leak(_run("compute"), "PRECOUNT_FAILED:compute")

    # fix5 BH-13: SystemExit（依存先の sys.exit 相当）も 3 境界で捕捉し、文字列・コードを出さない
    def test_a_fetch_systemexit_is_reason_code_only(self):
        self.assert_no_leak(_run("fetch_exit"), "PRECOUNT_FAILED:fetch")

    def test_b_import_systemexit_is_reason_code_only(self):
        self.assert_no_leak(_run("import_exit"), "PRECOUNT_FAILED:import")


class TestNormalPath(_Common):
    def test_c_counts_only_without_dates_or_ids(self):
        proc = _run("fake_records")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stderr.strip(), "")
        lines = proc.stdout.strip().splitlines()
        self.assertEqual(lines[0], "集計日は実行時 JST（日付は出力しない）")
        self.assertIn("fetched=3 targets(受任後×未提出)=2", lines[1])      # 問い合わせ は対象外
        self.assertIn("起算日未確定=1", lines[2])
        self.assertTrue(lines[3].startswith("残日数<=14="))
        self.assertIn("通知済み閾値 写し: 14日前=1 7日前=0", lines[4])
        self.assertTrue(lines[5].startswith("初回の最大 push 数（<=14 + <=7 + 未確定件数通知 1）="))
        blob = proc.stdout
        self.assertIsNone(_DATE_RE.search(blob))                        # 日付の個別値なし
        self.assertNotIn("No.", blob)                                    # レコード番号なし
        self.assertNotIn("$id", blob)
        self.assertNotIn("PRECOUNT_FAILED", blob)
        # 最大 push 数 = ≤14 + ≤7 + 1（未確定 1 件）
        m = re.search(r"残日数<=14=(\d+) 残日数<=7=(\d+)", lines[3])
        n = int(re.search(r"=(\d+)$", lines[5]).group(1))
        self.assertEqual(n, int(m.group(1)) + int(m.group(2)) + 1)


class TestEntryHardening(unittest.TestCase):
    def test_excepthook_replaced_and_dependency_loggers_silenced(self):
        import logging
        saved_hook = sys.excepthook
        saved_root = (list(logging.getLogger().handlers), logging.getLogger().level)
        try:
            spec = importlib.util.spec_from_file_location("houki_precount_mod", SCRIPT)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            self.assertIs(sys.excepthook, mod._excepthook)
            root = logging.getLogger()
            self.assertEqual(root.level, logging.WARNING)
            self.assertTrue(all(isinstance(h, logging.NullHandler) for h in root.handlers))
            own = logging.getLogger("scripts.houki_jukuryo_precount")
            self.assertFalse(own.propagate)
            self.assertEqual([type(h).__name__ for h in own.handlers], ["StreamHandler"])
            self.assertIs(own.handlers[0].stream, sys.stdout)
            self.assertEqual((mod.REASON_IMPORT, mod.REASON_FETCH, mod.REASON_COMPUTE, mod.REASON_UNEXPECTED,
                              mod.EXIT_FAILED),
                             ("PRECOUNT_FAILED:import", "PRECOUNT_FAILED:fetch", "PRECOUNT_FAILED:compute",
                              "PRECOUNT_FAILED:unexpected", 2))
            # sink 方針でリテラルにした logger 引数が定数と同値であること
            src = SCRIPT.read_text(encoding="utf-8")
            for reason in (mod.REASON_IMPORT, mod.REASON_FETCH, mod.REASON_COMPUTE, mod.REASON_UNEXPECTED):
                self.assertIn(f'logger.error("{reason}")', src)
        finally:
            sys.excepthook = saved_hook
            root = logging.getLogger()
            for h in list(root.handlers):
                root.removeHandler(h)
            for h in saved_root[0]:
                root.addHandler(h)
            root.setLevel(saved_root[1])

    def test_script_has_no_write_or_send_calls(self):
        impl = REPO / "scripts" / "houki_jukuryo_precount_impl.py"
        for path in (SCRIPT, impl):
            src = path.read_text(encoding="utf-8")
            for banned in ("update_record", "create_record", "delete_record", "upload_file", "push_line_message",
                           "notify_admin", "session_scope", "send_notice", "requests.post", "client.post",
                           "client.put", "sys.stdout.write", "print("):
                self.assertNotIn(banned, src, f"{path.name}: {banned}")
        self.assertIn("fetch_all_targets", impl.read_text(encoding="utf-8"))
        # 入口は hub に依存しない（実装の import 失敗を捕捉できる）
        self.assertNotIn("from hub", SCRIPT.read_text(encoding="utf-8"))
        self.assertIn("from hub.redact import emit", impl.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
