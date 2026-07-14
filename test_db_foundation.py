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
import shutil
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

    def test_main_touches_hub_db_only_in_allowed_forms(self):
        """main.py と hub.db の境界（P1-005a で設計判断つき更新）。

        旧仕様（P1-004）: main.py は hub.db に一切触れない。
        新仕様（P1-005a）: 次の2形のみ許可——
          1. shutdown hook 内の `from hub.db import adispose_all`（P1-004申し送り①）
          2. hub.inbound_event 経由の利用（journal。hub.db を直接名指ししない）
        引き続き禁止: get_engine / get_async_engine / session_scope /
        dispose_all（同期）を main.py が直接使うこと（D3/D4/D6）。
        起動経路（import時・startup）でDBに触れない性質は
        「DATABASE_URL なしで全suiteが通る」ことでも担保される"""
        src = (REPO / "main.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "hub.db":
                names = {a.name for a in node.names}
                self.assertEqual(names, {"adispose_all"},
                                 f"main.py:{node.lineno} hub.db からの import は "
                                 f"adispose_all のみ許可: {names}")
        for banned in ("get_engine", "get_async_engine", "session_scope"):
            self.assertNotIn(banned, src,
                             f"main.py が {banned} を直接使うのは禁止（D4）")
        # 同期 dispose_all の直接呼び出し禁止（adispose_all は許可・D6）
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                f = node.func
                name = f.id if isinstance(f, ast.Name) else \
                    f.attr if isinstance(f, ast.Attribute) else ""
                self.assertNotEqual(name, "dispose_all",
                                    f"main.py:{node.lineno} 同期 dispose_all は"
                                    "禁止（shutdownは await adispose_all・D6）")

    def test_alembic_ini_stays_ascii(self):
        """alembic.ini は locale エンコーディングで読まれる（Windows=cp932）ため
        ASCII 限定（非ASCIIを入れると revision 生成が壊れる回帰の固定）"""
        raw = (REPO / "alembic.ini").read_bytes()
        raw.decode("ascii")  # 失敗すれば UnicodeDecodeError でテスト失敗


class TestAlembicScaffold(unittest.TestCase):
    def test_revisions_form_single_linear_chain_from_baseline(self):
        """migration履歴の健全性（P1-005a で D5 の「1本のみ」pin から更新）:
        root は空 baseline ただ1つ・分岐なしの一直線であること
        （複数head・迷子revisionの混入を検知する）"""
        import re
        revs = {}
        for p in (REPO / "alembic" / "versions").glob("*.py"):
            src = p.read_text(encoding="utf-8")
            rev = re.search(r"^revision: str = '([0-9a-f]+)'", src, re.M)
            down = re.search(
                r"^down_revision: .*? = (None|'([0-9a-f]+)')", src, re.M)
            self.assertIsNotNone(rev, f"{p.name}: revision 不明")
            self.assertIsNotNone(down, f"{p.name}: down_revision 不明")
            revs[rev.group(1)] = (down.group(2), p.name)
        roots = [(r, name) for r, (down, name) in revs.items() if down is None]
        self.assertEqual(len(roots), 1, f"root は1つのみ: {roots}")
        self.assertIn("baseline", roots[0][1])
        # 分岐なし（同じ down_revision を持つ revision が2つ以上ない）
        downs = [down for down, _ in revs.values() if down is not None]
        self.assertEqual(len(downs), len(set(downs)),
                         "migration履歴が分岐している（headが複数）")
        # 全revisionが root から辿れる一直線
        children = {down: r for r, (down, _) in revs.items()}
        chain, cur = 1, roots[0][0]
        while cur in children:
            cur = children[cur]
            chain += 1
        self.assertEqual(chain, len(revs), "rootから辿れない迷子revisionがある")

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

    def test_signature_nonce_migration_round_trip(self):
        """RV-04a: signature_nonce migration が空 DB で up→down 往復できること。
        online モードで実 sqlite ファイルに適用し、テーブルの生成/削除を検証する
        （alembic 起動が許可された本ファイルに置く・D2）。"""
        import sqlite3
        import tempfile
        d = tempfile.mkdtemp(prefix="sig_nonce_mig_")
        dbfile = f"{d}/mig.db"
        env = {**os.environ, "DATABASE_URL": f"sqlite:///{dbfile}",
               "PYTHONIOENCODING": "utf-8"}

        def alembic(*args):
            return subprocess.run(
                [sys.executable, "-m", "alembic", *args],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                cwd=REPO, env=env, timeout=120)

        def tables():
            con = sqlite3.connect(dbfile)
            try:
                return {r[0] for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            finally:
                con.close()

        try:
            up = alembic("upgrade", "head")
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-500:]}")
            self.assertIn("signature_nonce", tables())
            down = alembic("downgrade", "3e59f8270aa8")
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-500:]}")
            self.assertNotIn("signature_nonce", tables())
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_ini_has_no_url(self):
        """接続URLを ini に書かない（secret を ini に置かない・D4）"""
        text = (REPO / "alembic.ini").read_text(encoding="ascii")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("sqlalchemy.url") and not stripped.startswith("#"):
                self.fail(f"alembic.ini に sqlalchemy.url が定義されている: {line}")


if __name__ == "__main__":
    unittest.main()
