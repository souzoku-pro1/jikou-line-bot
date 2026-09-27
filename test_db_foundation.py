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


    def test_shindan_link_migration_round_trip(self):
        """SHINDAN-LINE-LINK-1 T9: shindan_link（a7d3f1c9e2b4・Revises e7a9c4d1f6b3）が
        空 DB で up→down 往復できること（alembic 起動が許可された本ファイルに置く・D2）。"""
        import sqlite3
        import tempfile
        d = tempfile.mkdtemp(prefix="shindan_link_mig_")
        dbfile = f"{d}/mig.db"
        env = {**os.environ, "DATABASE_URL": f"sqlite:///{dbfile}",
               "PYTHONIOENCODING": "utf-8"}

        def alembic(*args):
            return subprocess.run(
                [sys.executable, "-m", "alembic", *args],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                cwd=REPO, env=env, timeout=180)

        def tables():
            con = sqlite3.connect(dbfile)
            try:
                return {r[0] for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            finally:
                con.close()

        try:
            # BRAIN-A1: head が b8c1d4e7f2a5 へ進んだため、本 revision までを明示して往復する
            # （検証内容は不変: shindan_link の生成・列集合・current・downgrade）
            up = alembic("upgrade", "a7d3f1c9e2b4")
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-500:]}")
            self.assertIn("shindan_link", tables())
            con = sqlite3.connect(dbfile)
            try:
                cols = {r[1] for r in con.execute("PRAGMA table_info(shindan_link)")}
            finally:
                con.close()
            # fix2 SLL-02: claimed_at（nullable）を同 revision に追加
            self.assertEqual(cols, {"token", "line_user_id", "created_at",
                                    "expires_at", "used_at", "claimed_at"})
            self.assertIn("a7d3f1c9e2b4", alembic("current").stdout)
            down = alembic("downgrade", "e7a9c4d1f6b3")
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-500:]}")
            self.assertNotIn("shindan_link", tables())
            self.assertIn("e7a9c4d1f6b3", alembic("current").stdout)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    # ── BRAIN-A1 / ID-1a の migration 試験の共通部品（alembic 起動は本ファイルのみ・D2） ──

    BRAIN_M1 = "c9d2e5f8a1b3"
    BRAIN_M2 = "d1e4f7a0b3c6"

    @staticmethod
    def _brain_mig_tools(dbfile):
        import sqlite3

        env = {**os.environ, "DATABASE_URL": f"sqlite:///{dbfile}",
               "PYTHONIOENCODING": "utf-8", "KINTONE_SUBDOMAIN": "testsub"}

        def alembic(*args):
            return subprocess.run(
                [sys.executable, "-m", "alembic", *args],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                cwd=REPO, env=env, timeout=180)

        def tables():
            con = sqlite3.connect(dbfile)
            try:
                return {r[0] for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            finally:
                con.close()

        def unique_cols(con, table):
            out = []
            for r in con.execute(f'PRAGMA index_list("{table}")'):
                if r[2] == 1:
                    out.append(tuple(x[2] for x in con.execute(f"PRAGMA index_info({r[1]})")))
            return out

        def nullable(con, table):
            return {r[1]: (r[3] == 0) for r in con.execute(f'PRAGMA table_info("{table}")')}

        return alembic, tables, unique_cols, nullable

    def test_brain_ledger_migration_round_trip(self):
        """BRAIN-A1-LEDGER-1 → ID-1a: b8c1d4e7f2a5 → M1（c9d2e5f8a1b3）→ M2（d1e4f7a0b3c6）が
        空 DB で up→down 往復し、head で 13 表の列集合が hub/brain_ledger.metadata と一致すること。
        一意制約は sqlite の自動 index 名になるため列集合で検証する（uq_case_fact_key は M2 で
        case_id 基準に置換・A1 形へ戻ると案件キー基準）。"""
        import sqlite3
        import tempfile
        from hub import brain_ledger
        d = tempfile.mkdtemp(prefix="brain_ledger_mig_")
        dbfile = f"{d}/mig.db"
        alembic, tables, unique_cols, nullable = self._brain_mig_tools(dbfile)
        uq_a1 = ("case_app_id", "case_record_id", "subject_id", "item_code",
                 "source_app_id", "source_record_id", "source_revision",
                 "locator", "converter_name", "converter_version")
        uq_m2 = ("case_id", "subject_id", "item_code",
                 "source_app_id", "source_record_id", "source_revision",
                 "locator", "converter_name", "converter_version")
        try:
            # A1 の形（b8c1d4e7f2a5）: 案件キー基準・case 表なし
            up = alembic("upgrade", "b8c1d4e7f2a5")
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertTrue(set(brain_ledger.A1_TABLE_NAMES) <= tables())
            self.assertNotIn("case", tables())
            con = sqlite3.connect(dbfile)
            try:
                self.assertIn(uq_a1, unique_cols(con, "case_fact"))
                self.assertNotIn("case_id", nullable(con, "case_fact"))
            finally:
                con.close()
            # M1: 表と NULL 可の case_id 列（DDL のみ・A1 の一意制約はそのまま）
            up = alembic("upgrade", self.BRAIN_M1)
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertTrue(set(brain_ledger.TABLE_NAMES) <= tables())
            con = sqlite3.connect(dbfile)
            try:
                self.assertIn(uq_a1, unique_cols(con, "case_fact"))
                for name in ("case_fact", "case_event", "case_derivation", "source_ingest"):
                    self.assertTrue(nullable(con, name)["case_id"], name)
                self.assertFalse(nullable(con, "case_fact")["case_app_id"])
                self.assertTrue(nullable(con, "source_ingest")["line_user_id"])
                self.assertTrue(nullable(con, "source_ingest")["pending_reason"])      # R34
                self.assertTrue(nullable(con, "link_history")["prev_case_id"])
                self.assertIn("'stopped_stale'", con.execute(
                    "SELECT sql FROM sqlite_master WHERE name='sync_run'").fetchone()[0])
            finally:
                con.close()
            # M2（空 DB の検算は通る）: NOT NULL・case_id 基準の一意制約・旧列は NULL 許容
            up = alembic("upgrade", "head")
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            con = sqlite3.connect(dbfile)
            try:
                for name in brain_ledger.TABLE_NAMES:
                    cols = {r[1] for r in con.execute(f'PRAGMA table_info("{name}")')}
                    self.assertEqual(
                        cols, {c.name for c in brain_ledger.metadata.tables[name].columns}, name)
                self.assertIn(uq_m2, unique_cols(con, "case_fact"))
                self.assertNotIn(uq_a1, unique_cols(con, "case_fact"))
                self.assertIn(("supersedes_fact_id",), unique_cols(con, "case_fact"))
                self.assertIn(("source_app_id", "source_record_id", "source_revision",
                               "locator", "converter_name", "converter_version"),
                              unique_cols(con, "source_ingest"))
                self.assertIn(("operation_id",), unique_cols(con, "case_confirmation"))
                self.assertIn(("idem_key",), unique_cols(con, "case_event"))
                for name in ("case_fact", "case_event", "case_derivation"):
                    self.assertFalse(nullable(con, name)["case_id"], name)
                    self.assertTrue(nullable(con, name)["case_app_id"], name)
                self.assertTrue(nullable(con, "source_ingest")["case_id"])
                fks = {(r[2], r[3]) for r in con.execute("PRAGMA foreign_key_list(case_fact)")}
                self.assertIn(("case", "case_id"), fks)
            finally:
                con.close()
            self.assertIn(self.BRAIN_M2, alembic("current").stdout)
            # 往復: M2 → M1（新形式データゼロなら A1 形へ）→ b8c1 → a7d3
            down = alembic("downgrade", self.BRAIN_M1)
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-800:]}")
            con = sqlite3.connect(dbfile)
            try:
                self.assertIn(uq_a1, unique_cols(con, "case_fact"))
                self.assertTrue(nullable(con, "case_fact")["case_id"])
                self.assertFalse(nullable(con, "case_fact")["case_app_id"])
            finally:
                con.close()
            down = alembic("downgrade", "b8c1d4e7f2a5")
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-800:]}")
            self.assertNotIn("case", tables())
            con = sqlite3.connect(dbfile)
            try:
                self.assertNotIn("case_id", nullable(con, "case_fact"))
                self.assertNotIn("line_user_id", nullable(con, "source_ingest"))
            finally:
                con.close()
            down = alembic("downgrade", "a7d3f1c9e2b4")
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-800:]}")
            self.assertFalse(set(brain_ledger.TABLE_NAMES) & tables())
            self.assertIn("a7d3f1c9e2b4", alembic("current").stdout)
            self.assertIn("shindan_link", tables())          # 下位 revision の表は残る
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_brain_id1a_m2_downgrade_is_refused_for_new_format_data(self):
        """ID-1a R28: M2 の downgrade は新形式データ（未登録案件・統合済み・識別子の重複・
        merge_history・案件キー NULL の fact）を 1 件でも検出したら固定語彙で拒否する。
        ゼロに戻せば downgrade できる。M2 の upgrade も検算失敗（running run）で中止する。"""
        import sqlite3
        import tempfile
        from hub import brain_migration
        d = tempfile.mkdtemp(prefix="brain_id1a_refuse_")
        dbfile = f"{d}/mig.db"
        alembic, tables, unique_cols, nullable = self._brain_mig_tools(dbfile)
        try:
            self.assertEqual(alembic("upgrade", self.BRAIN_M1).returncode, 0)
            con = sqlite3.connect(dbfile)
            try:
                # 検算失敗（running の run）→ M2 は例外で中止・revision は M1 のまま
                con.execute("INSERT INTO sync_run (target_app, started_at, page_order, status) "
                            "VALUES ('app40', '2026-09-27 00:00:00', 'x', 'running')")
                con.commit()
                up = alembic("upgrade", "head")
                self.assertNotEqual(up.returncode, 0)
                self.assertIn("brain_case_verify_failed", up.stderr)
                self.assertIn(brain_migration.PROBLEM_RUNNING, up.stderr)
                self.assertIn(self.BRAIN_M1, alembic("current").stdout)
                con.execute("UPDATE sync_run SET status='stopped_stale'")
                con.commit()
            finally:
                con.close()
            self.assertEqual(alembic("upgrade", "head").returncode, 0)
            cases = (
                ("INSERT INTO \"case\" (registration, status, created_via) "
                 "VALUES ('unregistered', 'active', 'memo')", brain_migration.REFUSE_UNREGISTERED),
                ("INSERT INTO \"case\" (registration, status, created_via) "
                 "VALUES ('registered', 'merged', 'sync')", brain_migration.REFUSE_MERGED),
                ("INSERT INTO merge_history (from_case_id, into_case_id) VALUES (1, 2)",
                 brain_migration.REFUSE_MERGE_HISTORY),
                ("INSERT INTO subject_merge_history (case_id, from_subject_id, into_subject_id) "
                 "VALUES (1, 'a', 'b')", brain_migration.REFUSE_SUBJECT_MERGE),
            )
            for sql, reason in cases:
                con = sqlite3.connect(dbfile)
                try:
                    con.execute(sql)
                    con.commit()
                    down = alembic("downgrade", self.BRAIN_M1)
                    self.assertNotEqual(down.returncode, 0, reason)
                    self.assertIn("brain_case_downgrade_refused", down.stderr, reason)
                    self.assertIn(reason, down.stderr)
                    self.assertIn(self.BRAIN_M2, alembic("current").stdout, reason)
                    for t in ("subject_merge_history", "merge_history", "case_identity"):
                        con.execute(f"DELETE FROM {t}")
                    con.execute('DELETE FROM "case"')
                    con.commit()
                finally:
                    con.close()
            # 識別子の重複（同一 名前空間・種別・値 が 2 行）も拒否
            con = sqlite3.connect(dbfile)
            try:
                con.execute("INSERT INTO \"case\" (case_id, registration, status, created_via) "
                            "VALUES (1, 'registered', 'active', 'sync')")
                con.execute("INSERT INTO \"case\" (case_id, registration, status, created_via) "
                            "VALUES (2, 'registered', 'active', 'sync')")
                for cid, vt in ((1, "'2026-09-27 01:00:00'"), (2, "NULL")):
                    con.execute("INSERT INTO case_identity (case_id, namespace, kind, value, valid_from, "
                                f"valid_to) VALUES ({cid}, 'kintone:testsub:40', 'kintone_record', '7', "
                                f"'2026-09-27 00:00:00', {vt})")
                con.commit()
                down = alembic("downgrade", self.BRAIN_M1)
                self.assertNotEqual(down.returncode, 0)
                self.assertIn(brain_migration.REFUSE_IDENTITY_DUP, down.stderr)
                con.execute("DELETE FROM case_identity WHERE case_id=1")
                con.execute('DELETE FROM "case"')
                con.commit()
            finally:
                con.close()
            # ゼロに戻せば downgrade できる
            down = alembic("downgrade", self.BRAIN_M1)
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-800:]}")
            self.assertIn(self.BRAIN_M1, alembic("current").stdout)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_brain_id1a_m2_refuses_case_id_shared_by_keys(self):
        """fix1 BI-04: 検算の逆方向の一意性。1 つの case_id を 2 つ以上の旧案件キーが指していたら
        M2 の upgrade() は冒頭の検算で拒否する（DDL 開始前・revision は M1 のまま）。"""
        import sqlite3
        import tempfile
        from hub import brain_migration
        d = tempfile.mkdtemp(prefix="brain_id1a_shared_")
        dbfile = f"{d}/mig.db"
        alembic, tables, unique_cols, nullable = self._brain_mig_tools(dbfile)
        ingest = ("INSERT INTO source_ingest (source_kind, source_app_id, source_record_id, "
                  "source_revision, locator, converter_name, converter_version, state, case_id, "
                  "case_app_id, case_record_id, latest_seen_revision, pending_recheck, last_checked_at) "
                  "VALUES ('kintone', '40', ?, 1, '-', 'app40_record', '1', 'ingested', 1, '40', ?, 1, 0, "
                  "'2026-09-27 00:00:00')")
        try:
            self.assertEqual(alembic("upgrade", self.BRAIN_M1).returncode, 0)
            con = sqlite3.connect(dbfile)
            try:
                con.execute("INSERT INTO \"case\" (case_id, registration, status, created_via) "
                            "VALUES (1, 'registered', 'active', 'backfill')")
                for rec in ("1", "2"):                      # 旧案件キー 40/1 と 40/2 が同じ case 1 を指す
                    con.execute("INSERT INTO case_identity (case_id, namespace, kind, value, valid_from) "
                                "VALUES (1, 'kintone:testsub:40', 'kintone_record', ?, "
                                "'2026-09-27 00:00:00')", (rec,))
                    con.execute(ingest, (rec, rec))
                con.commit()
                before = con.execute("SELECT * FROM source_ingest ORDER BY ingest_id").fetchall()
            finally:
                con.close()
            # 他の検算項目は満たしている（この 1 点だけで拒否されること）
            from hub import db
            with patch.dict(os.environ, {"DATABASE_URL": f"sqlite:///{dbfile}"}):
                db.reset_for_tests()
                with db.get_engine().connect() as conn:
                    result = brain_migration.verify(conn)
                db.reset_for_tests()
            self.assertEqual(result["problems"], [brain_migration.PROBLEM_CASE_SHARED])
            self.assertEqual(brain_migration.PROBLEM_CASE_SHARED, "case_id_shared_by_keys")
            up = alembic("upgrade", "head")
            self.assertNotEqual(up.returncode, 0)
            self.assertIn("brain_case_verify_failed: case_id_shared_by_keys", up.stderr)
            self.assertIn(self.BRAIN_M1, alembic("current").stdout)          # revision は M1 のまま
            con = sqlite3.connect(dbfile)
            try:                                                # DDL は始まっていない
                self.assertTrue(nullable(con, "case_fact")["case_id"])
                self.assertFalse(nullable(con, "case_fact")["case_app_id"])
                self.assertIn(("case_app_id", "case_record_id", "subject_id", "item_code",
                               "source_app_id", "source_record_id", "source_revision",
                               "locator", "converter_name", "converter_version"),
                              unique_cols(con, "case_fact"))
                self.assertEqual(con.execute("PRAGMA foreign_key_list(source_ingest)").fetchall(), [])
                self.assertEqual(con.execute("SELECT * FROM source_ingest ORDER BY ingest_id").fetchall(),
                                 before)
                # 片方の識別子を別の case へ直せば通る
                con.execute("INSERT INTO \"case\" (case_id, registration, status, created_via) "
                            "VALUES (2, 'registered', 'active', 'backfill')")
                con.execute("UPDATE case_identity SET case_id=2 WHERE value='2'")
                con.execute("UPDATE source_ingest SET case_id=2 WHERE case_record_id='2'")
                con.commit()
            finally:
                con.close()
            up = alembic("upgrade", "head")
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertIn(self.BRAIN_M2, alembic("current").stdout)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_brain_id1a_m2_downgrade_refuses_rows_without_case_key_alone(self):
        """fix1 BI-05: downgrade 拒否条件の 5 種目（案件キー NULL かつ case_id ありの fact）を、他の
        拒否条件を満たさない状態で単独に用意し、固定理由での拒否・revision 据置・データ不変を検証する
        （event・derivation も同じ形で単独に検証）。"""
        import sqlite3
        import tempfile
        from hub import brain_migration
        d = tempfile.mkdtemp(prefix="brain_id1a_nokey_")
        dbfile = f"{d}/mig.db"
        alembic, tables, unique_cols, nullable = self._brain_mig_tools(dbfile)
        others = {brain_migration.REFUSE_UNREGISTERED, brain_migration.REFUSE_MERGED,
                  brain_migration.REFUSE_IDENTITY_DUP, brain_migration.REFUSE_MERGE_HISTORY,
                  brain_migration.REFUSE_SUBJECT_MERGE, brain_migration.REFUSE_FACT_WITHOUT_KEY,
                  brain_migration.REFUSE_EVENT_WITHOUT_KEY, brain_migration.REFUSE_DERIVATION_WITHOUT_KEY}
        inserts = (
            ("case_fact", brain_migration.REFUSE_FACT_WITHOUT_KEY,
             "INSERT INTO case_fact (case_id, case_app_id, case_record_id, subject_id, item_code, "
             "value_type, value_text, source_kind, source_app_id, source_record_id, source_revision, "
             "locator, converter_name, converter_version, observation_id, observed_at, confidence, "
             "is_current) VALUES (1, NULL, NULL, 'shipping:5', 'app30.件名', 'text', 'x', 'kintone', "
             "'30', '5', 1, '件名/-/-/-/-', 'app30_record', '1', 'obs', '2026-09-27 00:00:00', 'high', 1)"),
            ("case_event", brain_migration.REFUSE_EVENT_WITHOUT_KEY,
             "INSERT INTO case_event (idem_key, case_id, case_app_id, case_record_id, kind, "
             "source_app_id, source_record_id, source_revision, locator, summary, is_current) "
             "VALUES ('c1|28|100|-|chat_message|message/-/-/-/-', 1, NULL, NULL, 'chat_message', "
             "'28', '100', 1, 'message/-/-/-/-', 'chat:user:hearing', 1)"),
            ("case_derivation", brain_migration.REFUSE_DERIVATION_WITHOUT_KEY,
             "INSERT INTO case_derivation (case_id, case_app_id, case_record_id, kind, "
             "input_fact_versions, calculator_name, calculator_version, computed_at, needs_recalc) "
             "VALUES (1, NULL, NULL, 'deadline', '[]', 'calc', '1', '2026-09-27 00:00:00', 0)"),
        )
        dump_tables = ('"case"', "case_identity", "case_fact", "case_event", "case_derivation",
                       "source_ingest", "link_history", "merge_history", "subject_merge_history")

        def dump(con):
            return {t: con.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall() for t in dump_tables}

        try:
            self.assertEqual(alembic("upgrade", "head").returncode, 0)
            self.assertEqual(brain_migration.REFUSE_FACT_WITHOUT_KEY, "case_fact_without_case_key")
            for table, reason, sql in inserts:
                con = sqlite3.connect(dbfile)
                try:
                    # 登録済み・有効・識別子 1 件の案件（未登録・統合・識別子の重複・統合履歴は無い）
                    con.execute("INSERT INTO \"case\" (case_id, registration, status, created_via) "
                                "VALUES (1, 'registered', 'active', 'sync')")
                    con.execute("INSERT INTO case_identity (case_id, namespace, kind, value, valid_from) "
                                "VALUES (1, 'kintone:testsub:40', 'kintone_record', '1', "
                                "'2026-09-27 00:00:00')")
                    con.execute(sql)
                    con.commit()
                    before = dump(con)
                    self.assertEqual(len(before[table]), 1)
                finally:
                    con.close()
                down = alembic("downgrade", self.BRAIN_M1)
                self.assertNotEqual(down.returncode, 0, reason)
                self.assertIn(f"brain_case_downgrade_refused: {reason}", down.stderr)
                for other in others - {reason}:                 # 単独の理由で拒否されている
                    self.assertNotIn(other, down.stderr, (reason, other))
                self.assertIn(self.BRAIN_M2, alembic("current").stdout, reason)   # revision 据置
                con = sqlite3.connect(dbfile)
                try:
                    self.assertEqual(dump(con), before, reason)                  # データ不変
                    self.assertFalse(nullable(con, table)["case_id"], reason)    # 形も M2 のまま
                    self.assertTrue(nullable(con, table)["case_app_id"], reason)
                    con.execute(f"DELETE FROM {table}")
                    con.execute("DELETE FROM case_identity")
                    con.execute('DELETE FROM "case"')
                    con.commit()
                finally:
                    con.close()
            down = alembic("downgrade", self.BRAIN_M1)
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-800:]}")
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_brain_id1a_m1_only_parity_backfill_and_switch(self):
        """ID-1a の移行 3 段を sqlite で一周する（§14-2・§14-7）:
        (1) M1 だけ適用した DB（新形式データゼロ）で同期・再照合・訂正・確認を回し、head で作った
            DB と同じ観測になる（挙動不変を pin）
        (2) A1 のデータ形（case_id NULL・A1 形式の idem_key・case 表なし）に落としても読み書きが
            同じ（M1 だけ適用した本番の A1 データ＝互換の案件キー一致）
        (3) scripts/brain_case_backfill.py の --dry-run → --apply → --verify（冪等）
        (4) M2（検算つき）→ 観測は同じ・idem_key は case_id 内包・downgrade → upgrade の往復"""
        import asyncio
        import io as _io
        import sqlite3
        import tempfile
        from unittest.mock import patch

        import brain_test_support as bts
        from hub import brain_ledger as ledger
        from hub import brain_migration
        from hub import brain_sync as sync
        from hub import kintone
        from scripts import brain_case_backfill

        d = tempfile.mkdtemp(prefix="brain_id1a_parity_")
        m1_db = f"{d}/m1.db"
        head_db = f"{d}/head.db"
        alembic_m1, _t1, _u1, _n1 = self._brain_mig_tools(m1_db)
        alembic_head, _t2, _u2, _n2 = self._brain_mig_tools(head_db)
        drop = {"case_id", "fact_id", "event_id", "observed_at", "last_checked_at", "created_at",
                "link_id", "confirmation_id", "decided_at", "updated_at", "registered_at",
                "started_at", "finished_at", "last_ok_at", "last_reconcile_at", "last_recheck_at",
                "recheck_started_at", "recheck_completed_at", "run_id", "last_run_id",
                "prev_case_id", "new_case_id", "running_runs"}

        def norm(obj):
            if isinstance(obj, dict):
                return {k: norm(v) for k, v in obj.items() if k not in drop}
            if isinstance(obj, (list, tuple)):
                return [norm(x) for x in obj]
            return obj

        def data():
            return {"40": [bts.app40(1, 1), bts.app40(2, 1, "2026-09-20T02:00:00Z", LINEユーザーID=bts.LINE_B)],
                    "30": [bts.app30(5, 1), bts.app30(61, 1, 案件レコードID="999")],
                    "28": [bts.app28(100, 1), bts.app28(101, 1, line_user_id=bts.LINE_B),
                           bts.app28(102, 1, category="その他判断系")]}

        class Ctx:
            def __init__(self, dbfile):
                self.env = patch.dict(os.environ, {**bts.BRAIN_ENV, "BRAIN_SYNC_ENABLED": "1",
                                                   "DATABASE_URL": f"sqlite+aiosqlite:///{dbfile}"})
                self.fake = bts.FakeKintone(data())
                self.kp = patch.object(kintone, "search_records", self.fake.search_records)

            def __enter__(self):
                from hub import db
                self.env.start()
                db.reset_for_tests()
                self.kp.start()
                return self

            def __exit__(self, *exc):
                from hub import db
                self.kp.stop()
                db.reset_for_tests()
                self.env.stop()

        async def round1():
            for t in (sync.TARGET_APP40, sync.TARGET_APP30, sync.TARGET_APP28):
                assert (await sync.sync_target(t))["status"] == "ok", t
            for t in (sync.TARGET_APP30, sync.TARGET_APP28):
                assert (await sync.recheck_target(t))["status"] == "ok", t
            case2 = await ledger.resolve_case(("40", "2"))
            r = await ledger.relink_source(source_app_id="30", source_record_id="5", new_case=case2,
                                           reason="誤紐付け", operation_id="op-relink-1", actor="owner",
                                           seen_link_version=await ledger.link_version("30", "5"),
                                           seen_source_revision=1)
            assert r["duplicate"] is False
            fact = [f for f in await ledger.list_case_facts(("40", "1")) if f["item_code"] == "app40.status"][0]
            await ledger.record_confirmation(fact_id=fact["fact_id"], seen_version=fact["version"],
                                             decision="confirm", reason="", operation_id="op-confirm-1",
                                             actor="owner")

        async def round2(fake):
            # 新版・対象外 category・LINE ID の変更（識別子の有効終了）・同値新版
            fake.data["40"] = [bts.app40(1, 2, "2026-09-20T03:00:00Z", status="受理"),
                               bts.app40(2, 2, "2026-09-20T03:00:00Z", LINEユーザーID=bts.LINE_C)]
            fake.data["28"][1] = bts.app28(101, 2, "2026-09-21T01:00:00Z", line_user_id=bts.LINE_B,
                                           category="その他判断系")
            for t in (sync.TARGET_APP40, sync.TARGET_APP28, sync.TARGET_APP30):
                assert (await sync.sync_target(t))["status"] == "ok", t
            assert (await sync.recheck_target(sync.TARGET_APP28))["status"] == "ok"

        async def observe():
            out = {}
            for key in (("40", "1"), ("40", "2")):
                out[f"facts{key}"] = await ledger.list_case_facts(key, current_only=False)
                out[f"events{key}"] = await ledger.list_case_events(key, current_only=False)
                out[f"fresh{key}"] = await ledger.case_freshness_detail(key, "app40", sync.source_targets())
            out["holds"] = await ledger.list_holds()
            out["pending"] = await ledger.list_pending_links()
            out["conflicts"] = await ledger.list_conflicts()
            out["recheck"] = await ledger.list_recheck()
            ov = await ledger.sync_overview()
            out["ingest_counts"] = ov["ingest_counts"]
            out["cursors"] = [(c["target_app"], c["kind"], c["state"]) for c in ov["cursors"]]
            for app_id, rid in (("30", "5"), ("30", "61"), ("28", "100"), ("28", "101"), ("28", "102")):
                out[f"link{app_id}/{rid}"] = await ledger.latest_link(app_id, rid)
                cur = await ledger.current_case_of_source(app_id, rid)
                info = await ledger.case_info(cur) if cur is not None else None
                out[f"case{app_id}/{rid}"] = (info["case_app_id"], info["case_record_id"]) if info else None
            return norm(out)

        def legacyize(dbfile):
            """A1 のデータ形へ落とす（case_id NULL・A1 形式の idem_key・case/識別子なし）。"""
            con = sqlite3.connect(dbfile)
            try:
                con.execute("UPDATE case_fact SET case_id=NULL, prev_case_id=NULL")
                con.execute("UPDATE case_event SET case_id=NULL")
                con.execute("UPDATE source_ingest SET case_id=NULL")
                con.execute("UPDATE link_history SET prev_case_id=NULL, new_case_id=NULL")
                rows = con.execute("SELECT event_id, idem_key, case_app_id, case_record_id, source_app_id, "
                                   "source_record_id, kind, locator FROM case_event").fetchall()
                for event_id, key, app, rec, sapp, srec, kind, loc in rows:
                    legacy = ledger.legacy_event_idem_key(app, rec, sapp, srec,
                                                          ledger.event_revision_part(key), kind, loc)
                    con.execute("UPDATE case_event SET idem_key=? WHERE event_id=?", (legacy, event_id))
                con.execute("DELETE FROM case_identity")
                con.execute('DELETE FROM "case"')
                con.commit()
                self.assertEqual(con.execute('SELECT count(*) FROM "case"').fetchone()[0], 0)
            finally:
                con.close()

        def script(*args):
            buf = _io.StringIO()
            with patch.dict(os.environ, {**bts.BRAIN_ENV, "DATABASE_PUBLIC_URL": f"sqlite:///{m1_db}"}):
                from hub import db
                db.reset_for_tests()
                rc = brain_case_backfill.main(list(args), out=buf)
                db.reset_for_tests()
            return rc, buf.getvalue()

        try:
            # head で作った DB（比較の基準）
            self.assertEqual(alembic_head("upgrade", "head").returncode, 0)
            with Ctx(head_db) as c:
                asyncio.run(round1())
                head_obs1 = asyncio.run(observe())
                asyncio.run(round2(c.fake))
                head_obs2 = asyncio.run(observe())
            # (1) M1 だけ適用した DB: 同じ操作 → 同じ観測
            self.assertEqual(alembic_m1("upgrade", self.BRAIN_M1).returncode, 0)
            with Ctx(m1_db):
                asyncio.run(round1())
                self.assertEqual(asyncio.run(observe()), head_obs1)
            # (2) A1 のデータ形に落としても同じ観測（互換の案件キー一致で読む）・次の同期も同じ
            legacyize(m1_db)
            with Ctx(m1_db) as c:
                self.assertEqual(asyncio.run(observe()), head_obs1)
                asyncio.run(round2(c.fake))
                self.assertEqual(asyncio.run(observe()), head_obs2)
            # (3) backfill: --verify は失敗（A1 形のデータあり）→ --dry-run（件数のみ）→ --apply → --verify
            rc, text = script("--verify")
            self.assertEqual(rc, 1)
            self.assertIn("verify: FAILED", text)
            # BI-03: 移動履歴のある A1 データ（prev 案件キーあり・prev_case_id NULL）を検算が検出
            self.assertIn("nullable_case_id_inconsistent:case_fact.prev_case_id", text)
            con = sqlite3.connect(m1_db)
            try:
                prev_before = con.execute(
                    "SELECT fact_id, prev_case_app_id, prev_case_record_id FROM case_fact "
                    "WHERE prev_case_app_id IS NOT NULL ORDER BY fact_id").fetchall()
                self.assertTrue(prev_before)                       # 訂正（1→2）の移動履歴がある
                self.assertEqual(con.execute(
                    "SELECT count(*) FROM case_fact WHERE prev_case_app_id IS NOT NULL "
                    "AND prev_case_id IS NOT NULL").fetchone()[0], 0)
            finally:
                con.close()
            rc, text = script("--dry-run")
            self.assertEqual(rc, 0, text)
            self.assertIn("mode: DRY-RUN", text)
            self.assertIn("cases_to_create=", text)
            self.assertIn("count_running_runs=0", text)
            for pii in (bts.LINE_A, bts.LINE_B, bts.LINE_C, "テスト太郎"):
                self.assertNotIn(pii, text)
            self.assertIn(f"case_fact_prev_to_fill={len(prev_before)}", text)
            rc, text = script("--apply")
            self.assertEqual(rc, 0, text)
            self.assertIn("verify: OK", text)
            self.assertIn(f"case_fact_prev_filled={len(prev_before)}", text)
            # BI-03: 移行前後で対応が一致（NULL が残らない・prev_case_id の指す案件＝prev 案件キー）
            con = sqlite3.connect(m1_db)
            try:
                rows = con.execute(
                    "SELECT f.fact_id, f.prev_case_app_id, f.prev_case_record_id, f.prev_case_id, "
                    "i.namespace, i.value FROM case_fact f LEFT JOIN case_identity i "
                    "ON i.case_id = f.prev_case_id AND i.kind = 'kintone_record' AND i.valid_to IS NULL "
                    "WHERE f.prev_case_app_id IS NOT NULL ORDER BY f.fact_id").fetchall()
                self.assertEqual([(r[0], r[1], r[2]) for r in rows], prev_before)
                for _fid, app, rec, prev_id, ns, value in rows:
                    self.assertIsNotNone(prev_id)
                    self.assertEqual((ns, value), (f"kintone:testsub:{app}", rec))
            finally:
                con.close()
            rc, text2 = script("--apply")                       # 冪等
            self.assertIn("case_fact_prev_filled=0", text2)
            self.assertEqual(rc, 0, text2)
            self.assertIn("cases_created=0", text2)
            self.assertIn("event_keys_rewritten=0", text2)
            rc, text = script("--verify")
            self.assertEqual(rc, 0, text)
            self.assertIn("切替後の再開順序", text)
            # (4) M2: 検算つき切替 → 観測は同じ・idem_key は新形式・登録済み案件に識別子
            up = alembic_m1("upgrade", "head")
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            with Ctx(m1_db):
                self.assertEqual(asyncio.run(observe()), head_obs2)
                cases = {asyncio.run(ledger.resolve_case(("40", "1"))),
                         asyncio.run(ledger.resolve_case(("40", "2")))}
                self.assertEqual(len(cases - {None}), 2)
                self.assertEqual(asyncio.run(ledger.active_cases_for_relation(
                    sync.line_namespace(), ledger.IDENTITY_LINE_USER, bts.LINE_A)),
                    [asyncio.run(ledger.resolve_case(("40", "1")))])
            con = sqlite3.connect(m1_db)
            try:
                keys = [r[0] for r in con.execute("SELECT idem_key FROM case_event")]
                self.assertTrue(keys and all(k.startswith("c") and k.split("|")[0][1:].isdigit() for k in keys))
                self.assertEqual(con.execute("SELECT count(*) FROM case_fact WHERE case_id IS NULL").fetchone()[0], 0)
                self.assertEqual(con.execute("SELECT count(*) FROM source_ingest WHERE line_user_id IS NOT NULL "
                                             "AND source_app_id='28'").fetchone()[0],
                                 con.execute("SELECT count(*) FROM source_ingest WHERE source_app_id='28'").fetchone()[0])
            finally:
                con.close()
            # downgrade（新形式データゼロ）→ A1 形式の idem_key・upgrade で再び検算が通る
            self.assertEqual(alembic_m1("downgrade", self.BRAIN_M1).returncode, 0)
            con = sqlite3.connect(m1_db)
            try:
                keys = [r[0] for r in con.execute("SELECT idem_key FROM case_event")]
                self.assertTrue(keys and all(k.startswith("40|") for k in keys))
            finally:
                con.close()
            rc, text = script("--verify")
            self.assertEqual(rc, 1, text)                      # 旧形式の idem_key を検出
            self.assertIn(brain_migration.PROBLEM_IDEM_KEY, text)
            rc, text = script("--apply")                       # 再計算だけが走る
            self.assertEqual(rc, 0, text)
            self.assertIn("cases_created=0", text)
            self.assertEqual(alembic_m1("upgrade", "head").returncode, 0)
            with Ctx(m1_db):
                self.assertEqual(asyncio.run(observe()), head_obs2)
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
