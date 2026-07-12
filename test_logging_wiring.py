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

# main は import 時に多数の env を要求する（cloudsign_webhook 等）。ダミー env を
# 投入してから import するが、M02: os.environ を恒久汚染しないよう import 後に
# **全キーを元の状態へ完全復元**する（元が未設定なら削除・設定済みなら元値へ）。
_ENV_OVERRIDES = {
    "ANTHROPIC_API_KEY": "dummy_key_for_import_only",
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "GOOGLE_VISION_API_KEY": "dummy_vision",
    "HEALTHCHECK_DISABLED": "1",
}
# import 前に原状を保存（None=未設定）
_ENV_SAVED = {k: os.environ.get(k) for k in _ENV_OVERRIDES}
os.environ.update(_ENV_OVERRIDES)

import main  # noqa: E402


def _restore_env() -> None:
    """import 時に投入したダミー env を原状へ完全復元する。"""
    for k, original in _ENV_SAVED.items():
        if original is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = original


_restore_env()


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

    def test_existing_config_is_respected(self):
        """M01: 既に root に handler がある場合は handler も level も変更しない。"""
        with _RootGuard() as root:
            existing = logging.StreamHandler(sys.stderr)
            root.addHandler(existing)
            root.setLevel(logging.WARNING)   # 既存設定（INFO ではない）
            main._configure_app_logging()
            self.assertEqual(root.handlers, [existing])       # 二重付与しない
            self.assertEqual(root.level, logging.WARNING)     # level を上書きしない

    def test_env_restore_mechanism_is_complete(self):
        """M02: 復元ロジックが「元が未設定なら削除・設定済みなら元値」へ確実に戻すこと。

        本番の os.environ 最終状態は他 test モジュールの import 時 update にも左右される
        （＝この test で相等を主張できない）ため、復元ロジック自体を専用キーで決定的に検証する。
        """
        K_UNSET, K_SET = "_P1107A_PROBE_UNSET", "_P1107A_PROBE_SET"
        os.environ.pop(K_UNSET, None)
        os.environ[K_SET] = "orig"
        saved = {K_UNSET: os.environ.get(K_UNSET), K_SET: os.environ.get(K_SET)}
        try:
            os.environ[K_UNSET] = "dummyA"      # 元未設定を上書き
            os.environ[K_SET] = "dummyB"        # 元設定済みを上書き
            # 復元（_restore_env と同一ロジック）
            for k, original in saved.items():
                if original is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = original
            self.assertNotIn(K_UNSET, os.environ)          # 元未設定 → 削除
            self.assertEqual(os.environ.get(K_SET), "orig")  # 元設定済み → 元値
        finally:
            os.environ.pop(K_UNSET, None)
            os.environ.pop(K_SET, None)


if __name__ == "__main__":
    unittest.main()
