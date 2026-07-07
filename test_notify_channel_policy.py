"""LINE 送信チャネル方針の静的検査（2026-07-07 裁定・再発防止）

背景: 業務通知（発送管理の警報等）が顧客Bot（時効援用Bot）チャネルから届く不具合。
原因は push_line_message の token_env 既定値（顧客Bot）に暗黙に乗った呼び出し。

方針の機械強制:
- hub/notify 以外のモジュールから push_line_message を直接呼ぶ場合は
  token_env キーワード引数の明示を必須とする（既定値への暗黙の依存を禁止）
- hub/notify 内部のラッパー（notify_admin_line / notify_attorney_approval）の
  チャネルは test_hub_notify のヘッダピン留めテストで固定

検査方式は tool スキーマキー検査（test_koseki_tool_schema）と同じ AST 走査
（git 管理下の全 *.py・今後の新規呼び出しも自動で網にかかる・空振り防止ガード付き）。
"""

import ast
import subprocess
import unittest
from pathlib import Path


def _tracked_python_files() -> list[Path]:
    out = subprocess.run(["git", "ls-files", "*.py"],
                         capture_output=True, text=True, check=True).stdout
    return [Path(line) for line in out.splitlines() if line]


def _push_calls(tree: ast.AST):
    """push_line_message の直接呼び出しノードを列挙（_push_line_message 等は対象外）"""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else \
            func.attr if isinstance(func, ast.Attribute) else ""
        if name == "push_line_message":
            yield node


class TestPushLineMessageChannelPolicy(unittest.TestCase):
    def test_direct_callers_outside_hub_notify_must_pass_token_env(self):
        """hub/notify 以外の本体モジュールからの直接呼び出しは token_env 明示必須"""
        violations = []
        scanned = 0
        found_calls = 0
        for path in _tracked_python_files():
            posix = path.as_posix()
            if posix == "hub/notify.py" or path.name.startswith("test_"):
                continue  # 実装本体とテストは対象外（ラッパーはヘッダピンで固定）
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            scanned += 1
            for call in _push_calls(tree):
                found_calls += 1
                if not any(kw.arg == "token_env" for kw in call.keywords):
                    violations.append(f"{posix}:{call.lineno}")
        self.assertGreater(scanned, 10, "走査対象が少なすぎる（git ls-files 失敗?）")
        self.assertGreaterEqual(found_calls, 2,
                                "呼び出しが検出できていない（走査壊れ? "
                                "sortation_ingest / main の2箇所以上あるはず）")
        self.assertEqual(violations, [],
                         "push_line_message の直接呼び出しは token_env を明示する"
                         "こと（業務通知=business_token_env()・顧客向けは既定を"
                         "明示的に渡す。2026-07-07 裁定）")


if __name__ == "__main__":
    unittest.main()
