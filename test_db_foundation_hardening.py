"""P1-004a: Codexレビュー所見の固定（H01/M01/M03/L01）

- H01/D4: エンジン・セッション生成 API（create_engine / create_async_engine /
  sessionmaker / async_sessionmaker）を hub/db.py 以外で import・呼び出ししない
  ことを AST 走査で機械強制（alembic/env.py の独自 create_engine 廃止の恒久固定）
- M01/D2: alembic の動的起動（importlib / __import__ / subprocess系 / os.system）
  も検出対象に追加（静的 import 検査 test_db_foundation の強化版）
- M03: credential 入り URL の値がログ・例外・repr に漏れないことを固定。
  ※ SQLAlchemy の仕様上 str(engine.url) は hide_password=False で描画されるため
    「安全な描画」= repr() / render_as_string()（既定 hide_password=True）を正とし、
    本体コードが URL を str 化・出力しないこと（D4 一点集約で URL に触るのは
    hub/db.py のみ）と合わせて担保する
- L01: reset_for_tests()/dispose_all() が async engine も同期的に dispose する

外部通信なし（接続試行は 127.0.0.1:1 への即時拒否のみ・値は dummy）。
"""

import ast
import asyncio
import os
import subprocess
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from sqlalchemy.ext.asyncio import AsyncEngine

import hub.db as db

REPO = Path(__file__).parent
SELF = Path(__file__).name

# エンジン/セッション生成 API（hub/db.py 以外で使用禁止・D4）
_FORBIDDEN_FACTORY_NAMES = {"create_engine", "create_async_engine",
                            "sessionmaker", "async_sessionmaker"}

# alembic の動的起動を検出する呼び出し名（M01）
_DYNAMIC_IMPORT_FUNCS = {"import_module", "__import__"}
_SUBPROCESS_FUNCS = {"run", "Popen", "call", "check_call", "check_output",
                     "system"}


def _tracked_py() -> list[Path]:
    out = subprocess.run(["git", "ls-files", "*.py"], capture_output=True,
                         text=True, check=True, cwd=REPO).stdout
    return [Path(line) for line in out.splitlines() if line]


def _call_name(node: ast.Call) -> str:
    f = node.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return ""


def _string_constants(node: ast.AST):
    for sub in ast.walk(node):
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
            yield sub.value


class TestSinglePointEngineFactory(unittest.TestCase):
    """H01/D4: エンジン生成 API は hub/db.py の外に書かない"""

    EXCLUDED = {"hub/db.py", SELF}

    def test_no_engine_factory_outside_hub_db(self):
        violations = []
        scanned = 0
        for path in _tracked_py():
            posix = path.as_posix()
            if posix in self.EXCLUDED:
                continue
            tree = ast.parse((REPO / path).read_text(encoding="utf-8"),
                             filename=posix)
            scanned += 1
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        if alias.name in _FORBIDDEN_FACTORY_NAMES:
                            violations.append(
                                f"{posix}:{node.lineno} import {alias.name}")
                elif isinstance(node, ast.Call):
                    name = _call_name(node)
                    if name in _FORBIDDEN_FACTORY_NAMES:
                        violations.append(f"{posix}:{node.lineno} {name}()")
        self.assertGreater(scanned, 10, "走査対象が少なすぎる（git ls-files 失敗?）")
        self.assertEqual(violations, [],
                         "エンジン/セッション生成は hub/db.py に一点集約（D4）。"
                         "alembic/env.py も get_engine() を使うこと（H01）")

    def test_env_py_uses_get_engine(self):
        """H01 の直接固定: env.py は get_engine を呼び、create_engine を
        import も呼び出しもしない（コメント中の言及は許容するため AST 検査）"""
        tree = ast.parse((REPO / "alembic" / "env.py").read_text(encoding="utf-8"))
        imported, called = set(), set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                imported.update(a.name for a in node.names)
            elif isinstance(node, ast.Call):
                called.add(_call_name(node))
        self.assertIn("get_engine", imported)
        self.assertIn("get_engine", called)
        self.assertNotIn("create_engine", imported)
        self.assertNotIn("create_engine", called)


class TestNoDynamicAlembicInvocation(unittest.TestCase):
    """M01/D2: alembic の動的起動（importlib/__import__/subprocess/os.system）禁止。

    除外リスト（明示）:
      - alembic/ 配下（alembic 自身）
      - test_db_foundation.py（offline 煙テストが subprocess で alembic CLI を起動
        するのは「明示コマンドのみ」原則のテストであり違反ではない）
      - tools/tracking_pg_harness.py（TRACKING-PREP fix1 H02: 検証済みローカル
        URL を子プロセス env にのみ渡して `python -m alembic upgrade head` を
        起動する migrate ラッパー。人が明示的に打つコマンドの一体化であり
        app runtime からの自動 migration ではない＝D2 の趣旨と整合。
        接続先はローカル限定を機械強制・test_tracking_prep_harness.py で検査）
      - 本テストファイル（検出対象の名前を文字列として含むため）
    """

    EXCLUDED_PREFIXES = ("alembic/",)
    EXCLUDED_FILES = {"test_db_foundation.py", "tracking_pg_harness.py", SELF}

    def test_no_dynamic_alembic_launch(self):
        violations = []
        scanned = 0
        for path in _tracked_py():
            posix = path.as_posix()
            if posix.startswith(self.EXCLUDED_PREFIXES) or \
                    path.name in self.EXCLUDED_FILES:
                continue
            tree = ast.parse((REPO / path).read_text(encoding="utf-8"),
                             filename=posix)
            scanned += 1
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = _call_name(node)
                if name in _DYNAMIC_IMPORT_FUNCS | _SUBPROCESS_FUNCS:
                    if any("alembic" in s for s in _string_constants(node)):
                        violations.append(f"{posix}:{node.lineno} {name}(...)")
        self.assertGreater(scanned, 10, "走査対象が少なすぎる")
        self.assertEqual(violations, [],
                         "alembic はアプリから動的にも起動しない（D2・M01）")


class TestCredentialNonLeak(unittest.TestCase):
    """M03: dummy credential が repr・例外文字列に漏れない"""

    # connect_timeout はテスト時間の上限（環境により拒否が遅い場合の保険）
    DUMMY_URL = ("postgresql://leakuser:dummypass@127.0.0.1:1/leakdb"
                 "?connect_timeout=2")

    def setUp(self):
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()

    def test_engine_repr_masks_password(self):
        with patch.dict(os.environ, {"DATABASE_URL": self.DUMMY_URL}):
            engine = db.get_engine()
        self.assertNotIn("dummypass", repr(engine))
        self.assertNotIn("dummypass", repr(engine.url))
        # 安全な文字列化の正= render_as_string（既定 hide_password=True）
        self.assertNotIn("dummypass", engine.url.render_as_string())

    def test_async_engine_repr_masks_password(self):
        with patch.dict(os.environ, {"DATABASE_URL": self.DUMMY_URL}):
            engine = db.get_async_engine()
        self.assertNotIn("dummypass", repr(engine))
        self.assertNotIn("dummypass", repr(engine.url))

    def test_connect_failure_exception_does_not_leak(self):
        """接続失敗（127.0.0.1:1 への即時拒否・外部通信なし）の例外連鎖に
        password が含まれないこと"""
        with patch.dict(os.environ, {"DATABASE_URL": self.DUMMY_URL}):
            engine = db.get_engine()
            with self.assertRaises(Exception) as ctx:
                with engine.connect():
                    pass  # pragma: no cover（到達しない）
        seen = str(ctx.exception) + repr(ctx.exception)
        cause = ctx.exception.__cause__
        while cause is not None:
            seen += str(cause) + repr(cause)
            cause = cause.__cause__
        self.assertNotIn("dummypass", seen)

    def test_not_configured_error_has_no_url_fragment(self):
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            try:
                db.database_url()
                self.fail("DatabaseNotConfigured が送出されていない")
            except db.DatabaseNotConfigured as e:
                self.assertNotIn("://", str(e))
                self.assertNotIn("dummypass", str(e))


class TestDisposeAll(unittest.TestCase):
    """L01 正規解消（D6・P1-004c）: dispose の実効性と文脈判定の二値性。

    実 async ドライバ不在のため「await AsyncEngine.dispose() が実際に実行される」
    ことは AsyncEngine.dispose への AsyncMock spy（await_count）で検証する。
    実接続込みの dispose 統合テストは P1-005 の実DB使用時に追加する。
    仕様の固定: ループ内の同期呼び出し=明示例外／dispose中の例外=キャッシュは
    必ず空にしたうえで例外を送出（黙った不正 close・黙った握りつぶしは存在しない）
    """

    DUMMY_URL = "postgresql://u:p@127.0.0.1:1/d"

    def tearDown(self):
        db.reset_for_tests()

    def test_sync_reset_awaits_async_dispose(self):
        """2a+2d: 参照破棄だけでは PASS しない——await dispose() の実行を要求"""
        with patch.dict(os.environ, {"DATABASE_URL": self.DUMMY_URL}):
            db.get_async_engine()
            with patch.object(AsyncEngine, "dispose",
                              new_callable=AsyncMock) as spy:
                db.reset_for_tests()
            self.assertEqual(spy.await_count, 1,
                             "AsyncEngine.dispose() が await されていない")
        self.assertIsNone(db._async_engine)
        self.assertIsNone(db._async_session_factory)

    def test_sync_engine_dispose_executed(self):
        """sync engine 側も dispose 実行を spy で要求（参照破棄だけを許さない）"""
        with patch.dict(os.environ, {"DATABASE_URL": self.DUMMY_URL}):
            engine = db.get_engine()
            with patch.object(type(engine), "dispose") as spy:
                db.reset_for_tests()
            spy.assert_called_once()
        self.assertIsNone(db._engine)

    def test_calling_sync_reset_inside_event_loop_raises(self):
        """2b: ループ内からの同期呼び出しは明示例外（案内つき）"""
        async def inner():
            with self.assertRaises(RuntimeError) as ctx:
                db.reset_for_tests()
            self.assertIn("adispose_all", str(ctx.exception))
        asyncio.run(inner())

    def test_cache_cleared_even_if_dispose_raises(self):
        """2c: dispose 中の例外でもキャッシュは残らない（仕様固定）。
        例外自体は握りつぶさず送出される"""
        with patch.dict(os.environ, {"DATABASE_URL": self.DUMMY_URL}):
            db.get_async_engine()
            with patch.object(AsyncEngine, "dispose", new_callable=AsyncMock,
                              side_effect=RuntimeError("boom")):
                with self.assertRaises(RuntimeError):
                    db.reset_for_tests()
        self.assertIsNone(db._async_engine)
        self.assertIsNone(db._async_session_factory)

    def test_adispose_all_is_canonical_async_api(self):
        """D6: async 文脈の正規 API。await adispose_all() が async/sync 両エンジンを
        閉じてキャッシュを空にする（P1-005 の shutdown 経路もこれを使う）"""
        with patch.dict(os.environ, {"DATABASE_URL": self.DUMMY_URL}):
            db.get_engine()
            db.get_async_engine()
            with patch.object(AsyncEngine, "dispose",
                              new_callable=AsyncMock) as aspy:
                asyncio.run(db.adispose_all())
            self.assertEqual(aspy.await_count, 1)
        self.assertIsNone(db._engine)
        self.assertIsNone(db._async_engine)

    def test_dispose_all_idempotent(self):
        db.dispose_all()
        db.dispose_all()  # 何も生成していない状態で複数回呼んでも安全


class TestNoUrlStringificationInAppCode(unittest.TestCase):
    """M02一部（P1-004c）: 接続URLの不用意な文字列化・出力の素朴なAST禁止。

    検出対象（素朴な一致のみ・網羅は求めない）:
      - str(X.url) 形（.url 属性への str() 適用。SQLAlchemy の str(URL) は
        hide_password=False 描画のため）
      - render_as_string(hide_password=False)
      - database_url() の戻り値 または X.url を、print / logger系メソッド
        （debug/info/warning/error/exception/critical）/ 名前が Error/Exception で
        終わる呼び出し（例外constructor）へ**直接**渡す形
    限界（明記）: 変数に代入してから渡す・f-string / % / .format 経由・
    エイリアス関数経由などの間接パターンは検出しない（P1-005 で log capture
    検査を追加予定）。
    除外: hub/db.py（URLを扱う唯一の場所）・alembic/ 配下・本テスト自身。
    """

    EXCLUDED_PREFIXES = ("alembic/",)
    EXCLUDED_POSIX = {"hub/db.py"}

    _LOG_SINKS = {"print", "debug", "info", "warning", "error", "exception",
                  "critical"}

    @staticmethod
    def _is_url_expr(node: ast.AST) -> bool:
        if isinstance(node, ast.Attribute) and node.attr == "url":
            return True
        return isinstance(node, ast.Call) and _call_name(node) == "database_url"

    def test_no_url_stringification_or_direct_logging(self):
        violations = []
        scanned = 0
        for path in _tracked_py():
            posix = path.as_posix()
            if posix.startswith(self.EXCLUDED_PREFIXES) or \
                    posix in self.EXCLUDED_POSIX or path.name == SELF:
                continue
            tree = ast.parse((REPO / path).read_text(encoding="utf-8"),
                             filename=posix)
            scanned += 1
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = _call_name(node)
                if name == "str" and node.args and \
                        self._is_url_expr(node.args[0]):
                    violations.append(f"{posix}:{node.lineno} str(...url)")
                if name == "render_as_string" and any(
                        kw.arg == "hide_password"
                        and isinstance(kw.value, ast.Constant)
                        and kw.value.value is False
                        for kw in node.keywords):
                    violations.append(
                        f"{posix}:{node.lineno} "
                        "render_as_string(hide_password=False)")
                is_sink = name in self._LOG_SINKS or \
                    name.endswith(("Error", "Exception"))
                if is_sink and any(self._is_url_expr(a) for a in node.args):
                    violations.append(f"{posix}:{node.lineno} {name}(...url...)")
        self.assertGreater(scanned, 10, "走査対象が少なすぎる")
        self.assertEqual(violations, [],
                         "接続URL（credential含みうる値）を文字列化・ログ・例外へ"
                         "直接渡さない（M02一部・安全な描画は render_as_string()"
                         "既定 / repr のみ）")


if __name__ == "__main__":
    unittest.main()
