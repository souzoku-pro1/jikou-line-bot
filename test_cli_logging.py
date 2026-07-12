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

    def test_all_standalone_cli_call_configure_first_in_main(self):
        """RP1107B2-L01: print 移送済み standalone CLI 全4件が __main__ body の
        **最初の実行文**で configure_app_logging() を呼ぶこと（配線を他の処理より
        先行させ、起動直後の INFO を取りこぼさない）を AST で固定する。"""
        for rel in _CLI_FILES:
            with self.subTest(cli=rel):
                path = os.path.join(_REPO, rel)
                tree = ast.parse(open(path, encoding="utf-8").read(), filename=rel)
                first = self._main_first_stmt(tree)
                self.assertIsNotNone(first, f"{rel}: __main__ ブロックが無い")
                self.assertTrue(self._is_configure_call(first),
                                f"{rel}: __main__ の最初の実行文が "
                                f"configure_app_logging() でない")

    @staticmethod
    def _main_first_stmt(tree: ast.AST):
        for node in ast.walk(tree):
            if (isinstance(node, ast.If)
                    and isinstance(node.test, ast.Compare)
                    and isinstance(node.test.left, ast.Name)
                    and node.test.left.id == "__name__"):
                return node.body[0] if node.body else None
        return None

    @staticmethod
    def _is_configure_call(stmt) -> bool:
        return (isinstance(stmt, ast.Expr)
                and isinstance(stmt.value, ast.Call)
                and isinstance(stmt.value.func, ast.Name)
                and stmt.value.func.id == "configure_app_logging")


class TestCliRuntimeBehavior(unittest.TestCase):
    """item 11: 実 CLI の起動時挙動（usage INFO + exit 1）と、既存 handler 尊重を
    subprocess（fresh process）で固定する。"""

    def test_registry_cli_no_args_usage_info_and_exit1(self):
        """引数なし起動で usage INFO が stdout に到達し exit 1（挙動不変）。"""
        with tempfile.TemporaryDirectory() as tmp:
            env = dict(os.environ, PYTHONPATH=_REPO)
            proc = subprocess.run(
                [sys.executable, os.path.join(_REPO, "registry_to_kintone.py")],
                cwd=tmp, env=env, capture_output=True, text=True,
                errors="replace", timeout=60)
        self.assertEqual(proc.returncode, 1)                 # 使い方表示 → exit 1
        self.assertIn("INFO", proc.stdout)                   # INFO が stdout へ
        self.assertIn("registry_json_file", proc.stdout)     # usage 本文（ASCII 部）
        self.assertIn("registry_to_kintone", proc.stdout)    # logger 名

    def test_configure_respects_existing_handler_in_subprocess(self):
        """既に root handler がある fresh process で configure_app_logging を呼んでも
        handler 数・level が不変（M01・共有関数の handler/level 尊重）。"""
        code = (
            "import logging\n"
            "logging.basicConfig(level=logging.ERROR)\n"
            "r=logging.getLogger()\n"
            "before=(len(r.handlers), r.level)\n"
            "from hub.logging_setup import configure_app_logging\n"
            "configure_app_logging()\n"
            "after=(len(r.handlers), r.level)\n"
            "print('UNCHANGED' if before==after else 'CHANGED')\n"
        )
        env = dict(os.environ, PYTHONPATH=_REPO)
        proc = subprocess.run([sys.executable, "-c", code], env=env,
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("UNCHANGED", proc.stdout)              # handler/level 不変


if __name__ == "__main__":
    unittest.main()
