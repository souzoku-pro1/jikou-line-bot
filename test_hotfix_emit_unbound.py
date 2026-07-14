"""HOTFIX-01: main.py の _process_line_event 全停止（UnboundLocalError: emit）の回帰封じ。

原因: _process_line_event 内に関数ローカルの `from hub.redact import emit` が残置し、
Python が emit を関数全体でローカル変数扱い→先頭ログ(383行相当)の emit 参照が未束縛で
毎回クラッシュしていた。module-level import(main.py:54) 一本に戻して解消。

本テストは修正前コードで FAIL する（＝1,287件が素通しした穴を塞ぐ）。
"""

import ast
import glob
import os
import unittest
from unittest.mock import MagicMock, patch

# main は import 時に各種 env を要求するため dummy を投入（原状は復元）。
_ENV_OVERRIDES = {
    "ANTHROPIC_API_KEY": "dummy_key_for_import_only",
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "KINTONE_WEBHOOK_TOKEN": "approve_token",
    "DOCUMENT_WEBHOOK_SECRET": "doc_secret",
    "APP_APPROVAL": "29",
    "TOKEN_APPROVAL": "dummy",
    "APP_CHATLOG": "40",
    "TOKEN_CHATLOG": "dummy",
    "GOOGLE_VISION_API_KEY": "dummy_vision",
    "KINTONE_FUDOSAN_APP_ID": "50",
    "KINTONE_FUDOSAN_API_TOKEN": "dummy",
    "STRIPE_WEBHOOK_SECRET": "whsec_dummy",
    "HEALTHCHECK_DISABLED": "1",
}
_ENV_SAVED = {k: os.environ.get(k) for k in _ENV_OVERRIDES}
os.environ.update(_ENV_OVERRIDES)

import main  # noqa: E402

for _k, _orig in _ENV_SAVED.items():
    if _orig is None:
        os.environ.pop(_k, None)
    else:
        os.environ[_k] = _orig


class TestEmitScopeInProcessLineEvent(unittest.TestCase):
    """emit が _process_line_event のローカル変数化していないこと（bytecode 直接検査）。"""

    def test_emit_is_global_not_local(self):
        code = main._process_line_event.__code__
        # 関数ローカル import があると emit は co_varnames に載る（＝バグ）。
        self.assertNotIn("emit", code.co_varnames,
                         "emit が関数ローカル化している（関数内 redact.emit import の残置）")
        # 正しくはグローバル参照（co_names）で解決される。
        self.assertIn("emit", code.co_names, "emit がグローバル参照になっていない")


class TestProcessLineEventHeadLog(unittest.IsolatedAsyncioTestCase):
    """先頭ログ(383行相当)が UnboundLocalError を起こさず実行されること。

    logger.info を sentinel で止め、先頭ログの emit 引数評価まで到達したことを確認する。
    修正前は emit 引数評価で UnboundLocalError が送出され、sentinel に到達しない。"""

    async def test_head_log_executes_without_unbound_local(self):
        sentinel = RuntimeError("STOP_AFTER_HEAD_LOG")
        mock_logger = MagicMock()
        mock_logger.info.side_effect = sentinel
        with patch.object(main, "logger", mock_logger):
            with self.assertRaises(RuntimeError) as ctx:
                await main._process_line_event("reply-tok", "Uuser123", "こんにちは")
        # UnboundLocalError ではなく sentinel が上がる＝emit 評価が成功し先頭ログに到達。
        self.assertIs(ctx.exception, sentinel)
        self.assertTrue(mock_logger.info.called)


class TestNoFunctionLevelRedactImport(unittest.TestCase):
    """module-level import 済み redact.emit を関数内で再 import しないこと（同型残置の走査）。

    main.py 全域 + hub/ 配下を AST 走査し、関数(スコープ)内の
    `from hub.redact import emit` を検出する。1件でもあれば FAIL（残置は削除せず報告）。"""

    @staticmethod
    def _scan(path):
        with open(path, encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=path)
        hits = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for sub in ast.walk(node):
                    if isinstance(sub, ast.ImportFrom) and sub.module == "hub.redact":
                        if any(a.name in ("emit", "*") for a in sub.names):
                            hits.add(f"{os.path.basename(path)}:{sub.lineno}")
        return hits

    def test_no_nested_redact_emit_import(self):
        base = os.path.dirname(os.path.abspath(__file__))
        targets = [os.path.join(base, "main.py")]
        targets += glob.glob(os.path.join(base, "hub", "**", "*.py"), recursive=True)
        hits = set()
        for t in targets:
            hits |= self._scan(t)
        self.assertEqual(sorted(hits), [],
                         f"関数内 redact.emit 再 import 残置（module-level と二重）: {sorted(hits)}")


if __name__ == "__main__":
    unittest.main()
