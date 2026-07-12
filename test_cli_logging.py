"""RV-10 PR-4b: standalone CLI の logging 配線を固定する。

print 移送済みの CLI（`if __name__ == "__main__"` を持つ4ファイル）が
hub.logging_setup.configure_app_logging を __main__ で呼び、INFO が stdout に
到達することを検証する。

テスト方式の選択理由:
- 代表 CLI に make_zaisan_mokuroku_template.py を選択。外部通信・kintone/API・
  引数・env を一切要さず（docx を生成するだけ）、temp CWD に隔離して subprocess
  起動できる唯一の CLI のため、「実 CLI を subprocess で起動し正常系 INFO が
  stdout へ出る」ことを実測できる。
- 他3 CLI（registry_to_kintone / import_city_master / channels.shokumu_seikyu）は
  kintone 接続・入力ファイル・引数を必須とし外部依存なしに起動できないため、
  __main__ 内の configure_app_logging 呼び出しの存在を AST で固定する（配線漏れの
  回帰防止）。
"""

import ast
import os
import subprocess
import sys
import tempfile
import unittest

_REPO = os.path.dirname(os.path.abspath(__file__))

_CLI_FILES = [
    "registry_to_kintone.py",
    "import_city_master.py",
    "make_zaisan_mokuroku_template.py",
    "channels/shokumu_seikyu.py",
]


class TestCliLoggingWiring(unittest.TestCase):
    def test_make_zaisan_cli_emits_info_to_stdout(self):
        """代表 CLI を subprocess 起動し、configure_app_logging により INFO が
        新 format（timestamp/level/logger名/message）で stdout に到達することを実証。"""
        with tempfile.TemporaryDirectory() as tmp:
            env = dict(os.environ, PYTHONPATH=_REPO)
            proc = subprocess.run(
                [sys.executable,
                 os.path.join(_REPO, "make_zaisan_mokuroku_template.py")],
                cwd=tmp, env=env, capture_output=True, text=True,
                errors="replace", timeout=120)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("INFO", proc.stdout)                       # level（新 format）
        self.assertIn("generated", proc.stdout)                  # app INFO が stdout へ
        self.assertIn("make_zaisan_mokuroku_template", proc.stdout)  # logger 名

    def test_all_standalone_cli_wire_configure_app_logging(self):
        """print 移送済み standalone CLI 全4件が __main__ 内で
        configure_app_logging を呼ぶことを AST で固定する。"""
        for rel in _CLI_FILES:
            with self.subTest(cli=rel):
                path = os.path.join(_REPO, rel)
                tree = ast.parse(open(path, encoding="utf-8").read(), filename=rel)
                self.assertTrue(self._main_calls_configure(tree),
                                f"{rel}: __main__ が configure_app_logging を呼んでいない")

    @staticmethod
    def _main_calls_configure(tree: ast.AST) -> bool:
        for node in ast.walk(tree):
            if not isinstance(node, ast.If):
                continue
            test = node.test
            if not (isinstance(test, ast.Compare)
                    and isinstance(test.left, ast.Name)
                    and test.left.id == "__name__"):
                continue
            for sub in ast.walk(node):
                if (isinstance(sub, ast.Call)
                        and isinstance(sub.func, ast.Name)
                        and sub.func.id == "configure_app_logging"):
                    return True
        return False


if __name__ == "__main__":
    unittest.main()
