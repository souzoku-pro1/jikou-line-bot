"""P1-004 migration基盤（alembic + hub/db.py）のテスト

固定する設計判断:
  D2: migration は明示コマンドのみ。アプリ本体（alembic/ と本テスト以外の
      追跡 *.py）から alembic を import しない
  D3: DATABASE_URL 未設定でもアプリは正常起動する（hub/db は lazy 初期化・
      main.py は hub.db を import しない）。未設定で DB 機能に到達したときのみ
      DatabaseNotConfigured
  D4: エンジン生成は hub/db.py に一点集約（lazy・キャッシュ）
  D5: 初回 migration は空の baseline 1本のみ

外部通信・実DB接続なし（sqlite URL とオフラインモードのみ使用）。
"""

import ast
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import hub.db as db

REPO = Path(__file__).parent


class TestNormalizeUrl(unittest.TestCase):
    def test_postgres_scheme_is_normalized(self):
        self.assertEqual(db.normalize_url("postgres://u@h:5432/d"),
                         "postgresql+psycopg://u@h:5432/d")

    def test_postgresql_scheme_is_normalized(self):
        self.assertEqual(db.normalize_url("postgresql://u@h:5432/d"),
                         "postgresql+psycopg://u@h:5432/d")

    def test_explicit_driver_passthrough(self):
        self.assertEqual(db.normalize_url("postgresql+psycopg://u@h/d"),
                         "postgresql+psycopg://u@h/d")

    def test_other_scheme_passthrough(self):
        self.assertEqual(db.normalize_url("sqlite:///x.db"), "sqlite:///x.db")


class TestLazyFailClosed(unittest.TestCase):
    """D3: 未設定でも import・起動は成功し、DB到達時のみ明示エラー"""

    def setUp(self):
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()

    def test_database_url_unset_raises_explicitly(self):
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(db.DatabaseNotConfigured):
                db.database_url()

    def test_get_engine_unset_raises_not_hangs(self):
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(db.DatabaseNotConfigured):
                db.get_engine()

    def test_database_url_is_normalized(self):
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://u@h/d"}):
            self.assertEqual(db.database_url(), "postgresql+psycopg://u@h/d")

    def test_error_message_does_not_leak_values(self):
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            try:
                db.database_url()
            except db.DatabaseNotConfigured as e:
                self.assertNotIn("://", str(e))


class TestEngineSinglePoint(unittest.TestCase):
    """D4: lazy 生成・キャッシュ（実接続しない sqlite URL で確認）"""

    def setUp(self):
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()

    def test_engine_is_cached(self):
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite://"}):
            e1 = db.get_engine()
            e2 = db.get_engine()
        self.assertIs(e1, e2)

    def test_reset_clears_cache(self):
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite://"}):
            e1 = db.get_engine()
            db.reset_for_tests()
            e2 = db.get_engine()
        self.assertIsNot(e1, e2)


class TestNoAutoMigrationPolicy(unittest.TestCase):
    """D2/D3 の機械強制（AST走査・notify方針テストと同じ型）"""

    @staticmethod
    def _tracked_py() -> list[Path]:
        out = subprocess.run(["git", "ls-files", "*.py"], capture_output=True,
                             text=True, check=True, cwd=REPO).stdout
        return [Path(line) for line in out.splitlines() if line]

    def test_no_app_module_imports_alembic(self):
        """alembic を import してよいのは alembic/ 配下と本テストのみ"""
        violations = []
        scanned = 0
        for path in self._tracked_py():
            posix = path.as_posix()
            if posix.startswith("alembic/") or path.name == Path(__file__).name:
                continue
            tree = ast.parse((REPO / path).read_text(encoding="utf-8"),
                             filename=posix)
            scanned += 1
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                if any(n == "alembic" or n.startswith("alembic.") for n in names):
                    violations.append(f"{posix}:{node.lineno}")
        self.assertGreater(scanned, 10, "走査対象が少なすぎる（git ls-files 失敗?）")
        self.assertEqual(violations, [],
                         "アプリ本体から alembic を import しない（D2: "
                         "migration は明示コマンドのみ）")

    def test_main_does_not_import_hub_db(self):
        """main.py は hub.db に触れない（D3: 起動経路にDB層を入れない）。
        P1-005 で結線する際はこのテストを設計判断つきで更新すること"""
        src = (REPO / "main.py").read_text(encoding="utf-8")
        self.assertNotIn("hub.db", src)
        self.assertNotIn("from hub import db", src)

    def test_alembic_ini_stays_ascii(self):
        """alembic.ini は locale エンコーディングで読まれる（Windows=cp932）ため
        ASCII 限定（非ASCIIを入れると revision 生成が壊れる回帰の固定）"""
        raw = (REPO / "alembic.ini").read_bytes()
        raw.decode("ascii")  # 失敗すれば UnicodeDecodeError でテスト失敗


class TestAlembicScaffold(unittest.TestCase):
    def test_single_baseline_revision(self):
        """D5: 現時点の revision は空 baseline の1本のみ"""
        files = [p for p in (REPO / "alembic" / "versions").glob("*.py")]
        self.assertEqual(len(files), 1, f"想定外のrevision: {files}")
        self.assertIn("baseline", files[0].name)

    def test_offline_upgrade_generates_sql(self):
        """scaffold 一式（ini→env.py→versions）が実DB無しで通ることの煙テスト。
        offline モード（--sql）は接続せずSQLスクリプトを出力する"""
        env = {**os.environ, "DATABASE_URL": "sqlite:///offline_smoke_dummy.db",
               "PYTHONIOENCODING": "utf-8"}
        proc = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=REPO, env=env, timeout=120)
        self.assertEqual(proc.returncode, 0,
                         f"stderr={proc.stderr[-500:]}")
        self.assertIn("alembic_version", proc.stdout + proc.stderr)

    def test_ini_has_no_url(self):
        """接続URLを ini に書かない（secret を ini に置かない・D4）"""
        text = (REPO / "alembic.ini").read_text(encoding="ascii")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("sqlalchemy.url") and not stripped.startswith("#"):
                self.fail(f"alembic.ini に sqlalchemy.url が定義されている: {line}")


if __name__ == "__main__":
    unittest.main()
