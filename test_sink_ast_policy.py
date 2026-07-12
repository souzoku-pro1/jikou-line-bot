"""sink 出力の AST 方針検査（P1-101・DRAFT §5 の土台）

検査対象（アプリ本体 *.py・test_/legacy/alembic 除外）:
- **print** の直書き（S3 print 全廃の対象）
- print / logger.<level> / HTTPException への **str(...) / .text 直渡し**
  （raw_error＝S2 例外・ログの分類化対象）

移行期は **許可リスト方式**: 現状の違反を redaction_sink_allowlist.json に凍結し、
「**新規追加された違反のみ**」をテスト失敗させる（既存は S1〜S4 で順次解消していく台帳）。
違反を解消したら allowlist から当該行を削除すること（削除漏れもこのテストが検知する）。

この許可リストが S1〜S4 切替の作業台帳そのものである。
"""

import ast
import json
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).parent
ALLOWLIST_PATH = REPO / "redaction_sink_allowlist.json"

EXCLUDE_PREFIXES = ("legacy/", "alembic/")
EXCLUDE_NAMES = {"conftest.py", "redact.py", "test_redact.py",
                 "test_sink_ast_policy.py"}

_LOGGER_ATTRS = {"debug", "info", "warning", "error", "exception", "critical"}


def _tracked_py():
    out = subprocess.run(["git", "ls-files", "*.py"], capture_output=True,
                         text=True, check=True, cwd=REPO).stdout
    for line in out.splitlines():
        if not line:
            continue
        p = Path(line)
        if line.startswith(EXCLUDE_PREFIXES):
            continue
        if p.name.startswith("test_") or p.name in EXCLUDE_NAMES:
            continue
        yield p


def _sink_kind(node: ast.Call):
    f = node.func
    if isinstance(f, ast.Name):
        if f.id == "print":
            return "print"
        if f.id == "HTTPException":
            return "httpexception"
    if isinstance(f, ast.Attribute) and f.attr in _LOGGER_ATTRS:
        return "logger"
    return None


def _has_str_call(node) -> bool:
    return any(isinstance(s, ast.Call) and isinstance(s.func, ast.Name)
               and s.func.id == "str" for s in ast.walk(node))


def _has_dot_text(node) -> bool:
    return any(isinstance(s, ast.Attribute) and s.attr == "text"
               for s in ast.walk(node))


def scan_violations() -> list[str]:
    violations = []
    for path in _tracked_py():
        posix = path.as_posix()
        try:
            tree = ast.parse((REPO / path).read_text(encoding="utf-8"),
                             filename=posix)
        except Exception:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            sink = _sink_kind(node)
            if sink == "print":
                violations.append(f"{posix}:{node.lineno}:print")
                continue
            if sink in ("logger", "httpexception"):
                args = list(node.args) + [kw.value for kw in node.keywords]
                if any(_has_str_call(a) or _has_dot_text(a) for a in args):
                    violations.append(f"{posix}:{node.lineno}:raw_error:{sink}")
    return sorted(set(violations))


class TestSinkAstPolicy(unittest.TestCase):
    def setUp(self):
        self.allow = set(json.loads(ALLOWLIST_PATH.read_text(encoding="utf-8")))
        self.current = set(scan_violations())

    def test_scan_reaches_enough_files(self):
        # 走査が壊れて空にならないことの空振り防止
        self.assertGreater(len(self.current), 10)

    def test_no_new_violations(self):
        """許可リストに無い新規違反はゼロ（新規 print / raw_error 直渡しの追加を阻止）"""
        new = sorted(self.current - self.allow)
        self.assertEqual(new, [],
                         "redaction 方針に反する新規 sink 直書きが追加された。"
                         "emit() 経由にするか、正当なら allowlist に追記して理由を"
                         "レビューで明示すること:\n" + "\n".join(new))

    def test_allowlist_has_no_stale_entries(self):
        """解消済み（=もう存在しない）違反が allowlist に残っていないこと。
        S1〜S4 で違反を消したら allowlist からも削除する運用を強制する"""
        stale = sorted(self.allow - self.current)
        self.assertEqual(stale, [],
                         "解消済みの違反が allowlist に残っている（削除漏れ）。"
                         "S1〜S4 で直したら当該行を allowlist から削除すること:\n"
                         + "\n".join(stale))

    def test_allowlist_entries_are_well_formed(self):
        for e in self.allow:
            parts = e.split(":")
            self.assertGreaterEqual(len(parts), 3, e)
            self.assertIn(parts[2], {"print", "raw_error"}, e)


if __name__ == "__main__":
    unittest.main()
