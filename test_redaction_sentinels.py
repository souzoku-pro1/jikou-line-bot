"""RV-10 PR-5: redaction 恒久保証テスト（sentinel 注入・AST 回帰）。

- 例外 print→logger 転換で生じた sink:logger 25 件（型名可視の受容債務）が、
  例外本文を必ず emit 経由で抑止し、生の非 emit 引数は type(...).__name__ のみで
  あることを AST で全数固定する（item 5）。
- dispatch parser の実 parse 結果に氏名/params sentinel を入れ、captured log に
  出ないことを検証する（item 6・source 検査の補完）。
- ClaudeUnavailableError の message に生の例外本文を新規に書けないこと（型名のみ）を
  AST で回帰固定する（item 7）。
"""

import ast
import json
import os
import pathlib
import types
import unittest
from unittest.mock import patch

_REPO = pathlib.Path(__file__).parent
_ALLOWLIST = json.loads((_REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8"))


def _is_type_name(node: ast.AST) -> bool:
    """`type(x).__name__` 形か（例外クラス名の可視・受容パターン）。"""
    return (isinstance(node, ast.Attribute) and node.attr == "__name__"
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "type")


def _is_emit_call(node: ast.AST) -> bool:
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "emit")


def _is_const_like(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.JoinedStr):
        return all(isinstance(v, ast.Constant) for v in node.values)
    return False


def _arg_ok(node: ast.AST) -> bool:
    """logger 呼び出しの許容引数: 定数 / 定数 f-string / emit(...) / type(x).__name__。"""
    return _is_const_like(node) or _is_emit_call(node) or _is_type_name(node)


class TestConversionExceptionLogsSafePattern(unittest.TestCase):
    """item 5: P1-107b 転換 sink:logger 25 件の logger 呼び出しは、生引数が
    type(...).__name__ のみで、他は定数か emit（本文は emit で抑止）であることを固定。"""

    def _conversion_logger_entries(self):
        man = _ALLOWLIST.get("manifest", {})
        out = []
        for e in _ALLOWLIST["entries"]:
            f = e.split(":")[0]
            rule = ":".join(e.split(":")[2:])
            line = int(e.split(":")[1])
            if rule == "sink:logger" and "P1-107b" in man.get(f + ":sink:logger", ""):
                out.append((f, line))
        return out

    def test_conversion_entries_present(self):
        self.assertGreaterEqual(len(self._conversion_logger_entries()), 20)

    def test_each_conversion_log_exposes_only_safe_args(self):
        for f, line in self._conversion_logger_entries():
            with self.subTest(entry=f"{f}:{line}"):
                tree = ast.parse((_REPO / f).read_text(encoding="utf-8"), filename=f)
                call = self._logger_call_at(tree, line)
                self.assertIsNotNone(call, f"{f}:{line}: logger 呼び出しが見つからない")
                args = list(call.args) + [kw.value for kw in call.keywords]
                bad = [ast.dump(a) for a in args if not _arg_ok(a)]
                self.assertEqual(bad, [], f"{f}:{line}: 生の非 emit 引数がある: {bad}")
                # 例外本文の抑止: emit 引数が少なくとも1つ（本文経路）存在すること
                self.assertTrue(any(_is_emit_call(a) for a in args),
                                f"{f}:{line}: emit 経由の抑止引数が無い")

    @staticmethod
    def _logger_call_at(tree, line):
        best = None
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "logger"
                    and node.lineno == line):
                best = node
        return best

    def test_emit_vendor_raw_suppresses_sentinel(self):
        """本文抑止の実体: emit(str(例外), 'vendor_raw', 'log', 'operator') は原文を返さない。"""
        from hub.redact import emit
        sent = "EXC_BODY_SENTINEL_9Z"
        out = emit(str(RuntimeError(sent)), "vendor_raw", "log", "operator")
        self.assertNotIn(sent, out)


class TestClaudeUnavailableErrorNoRawBody(unittest.TestCase):
    """item 7: ClaudeUnavailableError の message に生の例外本文（primary_exc/
    fallback_exc 等の bare 参照）を新規に書けないことを AST で回帰固定。"""

    def test_message_uses_type_name_not_raw_exception(self):
        tree = ast.parse((_REPO / "claude_gateway.py").read_text(encoding="utf-8"))
        raises = [n for n in ast.walk(tree)
                  if isinstance(n, ast.Raise) and isinstance(n.exc, ast.Call)
                  and isinstance(n.exc.func, ast.Name)
                  and n.exc.func.id == "ClaudeUnavailableError"]
        self.assertTrue(raises, "ClaudeUnavailableError の raise が見つからない")
        banned = {"primary_exc", "fallback_exc", "e", "exc", "err"}
        for r in raises:
            for fv in [n for n in ast.walk(r.exc) if isinstance(n, ast.FormattedValue)]:
                v = fv.value
                is_bare_exc = isinstance(v, ast.Name) and v.id in banned
                self.assertFalse(
                    is_bare_exc,
                    "ClaudeUnavailableError message が生の例外を埋め込んでいる"
                    f"（{getattr(v,'id','?')}）。type(...).__name__ を使うこと")


# item 6 は parser を import するため dummy env を投入（import 時のみ・値は検査対象外）
os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")
_SAVED = os.environ.get("ANTHROPIC_API_KEY")
import dispatch_bot.parser as _parser  # noqa: E402
if _SAVED == "dummy_key_for_import_only":
    os.environ.pop("ANTHROPIC_API_KEY", None)


class TestParserLogRedaction(unittest.IsolatedAsyncioTestCase):
    """item 6: parser の実 parse 結果に氏名/params sentinel を入れ、captured log に
    出ないことを検証（source 検査の補完）。"""

    async def test_parsed_log_omits_name_and_params(self):
        sent_name, sent_param = "NAME_SENTINEL_AA", "PARAM_SENTINEL_BB"
        block = types.SimpleNamespace(
            type="tool_use", name="parse_instruction",
            input={"customer_name": sent_name,
                   "task_params": {"target": sent_param},
                   "intent": "task", "confidence": "high"})
        resp = types.SimpleNamespace(content=[block])

        async def _fake(*a, **k):
            return resp

        with patch.object(_parser, "create_message_with_fallback", _fake):
            with self.assertLogs("dispatch_bot.parser", level="INFO") as cm:
                out = await _parser.parse_instruction("dummy 指示")
        log = "\n".join(cm.output)
        self.assertIn("parsed", log)               # ログ自体は出る
        self.assertNotIn(sent_name, log)           # 氏名は emit(name) で抑止
        self.assertNotIn(sent_param, log)          # params は emit(freetext) で抑止
        self.assertEqual(out["customer_name"], sent_name)  # 戻り値は不変（挙動保存）


if __name__ == "__main__":
    unittest.main()
