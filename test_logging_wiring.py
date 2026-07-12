"""RV-10 PR-4a: app ロガーの stdout 出力配線（main._configure_app_logging）の固定。

uvicorn 相当（root に app 用 handler が無い）状態から配線したとき:
  - app モジュールの INFO が stdout に到達する（従来 lastResort で握り潰されていた）
  - 既存の WARNING/error も引き続き出力される（可視性は不変・出力先は stdout に集約）
  - サードパーティ（httpx/httpcore/urllib3）の per-request INFO は WARNING へ抑制
  - root に既に handler がある場合は二重付与しない（INFO 化のみ）
"""

import logging
import os
import sys
import unittest

# main は import 時に多数の env を要求する（cloudsign_webhook 等）。
# 既存テスト（test_bank_ingest 等）と同じダミー env を投入してから import する。
_DUMMY_ANTHROPIC_KEY = "dummy_key_for_import_only"
os.environ.setdefault("ANTHROPIC_API_KEY", _DUMMY_ANTHROPIC_KEY)
os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "GOOGLE_VISION_API_KEY": "dummy_vision",
    "HEALTHCHECK_DISABLED": "1",
})

import main  # noqa: E402

# triage テストの skip ガード（ANTHROPIC_API_KEY 存在）を汚さないよう後始末
if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]


class _RootGuard:
    """root ロガーの handler/level を保存し、クリアして配線を試験、後で復元する。"""

    def __enter__(self):
        self.root = logging.getLogger()
        self._handlers = self.root.handlers[:]
        self._level = self.root.level
        self.root.handlers.clear()
        return self.root

    def __exit__(self, *a):
        self.root.handlers[:] = self._handlers
        self.root.setLevel(self._level)
        return False


class TestLoggingWiring(unittest.TestCase):
    def test_app_info_reaches_stdout(self):
        import io
        buf = io.StringIO()
        with _RootGuard() as root:
            # sys.stdout を差し替えてから配線（handler は呼び出し時の stdout を掴む）
            saved_out = sys.stdout
            sys.stdout = buf
            try:
                main._configure_app_logging()
                logging.getLogger("chat_responder").info("PROBE_INFO_XYZ")
                for h in root.handlers:
                    h.flush()
            finally:
                sys.stdout = saved_out
            out = buf.getvalue()
        self.assertIn("PROBE_INFO_XYZ", out)   # INFO が stdout に到達
        self.assertIn("INFO", out)             # level 表示
        self.assertIn("chat_responder", out)   # logger 名表示

    def test_warning_and_error_still_emitted(self):
        import io
        buf = io.StringIO()
        with _RootGuard() as root:
            saved_out = sys.stdout
            sys.stdout = buf
            try:
                main._configure_app_logging()
                logging.getLogger("main").warning("PROBE_WARN_XYZ")
                logging.getLogger("main").error("PROBE_ERR_XYZ")
                for h in root.handlers:
                    h.flush()
            finally:
                sys.stdout = saved_out
            out = buf.getvalue()
        self.assertIn("PROBE_WARN_XYZ", out)   # WARNING 継続
        self.assertIn("PROBE_ERR_XYZ", out)    # error 継続

    def test_third_party_info_suppressed(self):
        with _RootGuard():
            main._configure_app_logging()
            for noisy in ("httpx", "httpcore", "urllib3"):
                self.assertEqual(logging.getLogger(noisy).level, logging.WARNING,
                                 f"{noisy} should be raised to WARNING")

    def test_no_double_handler_when_already_configured(self):
        with _RootGuard() as root:
            existing = logging.StreamHandler(sys.stderr)
            root.addHandler(existing)
            main._configure_app_logging()
            # 既存 handler があれば新規追加せず（1つのまま）・level のみ INFO 化
            self.assertEqual(root.handlers, [existing])
            self.assertEqual(root.level, logging.INFO)


if __name__ == "__main__":
    unittest.main()
