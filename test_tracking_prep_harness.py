"""TRACKING-PREP fix1: tracking_pg_harness の接続遮断・引数検証の対照テスト。

固定する仕様（R-TRACKING-PREP-1 対応）:
- H01: URL は検証済み要素からの再構築。query/fragment（host/hostaddr/service 等の
  接続先上書きパラメータ）は全面拒否・拒否文言に URL 値非表示・非ローカル host 拒否
- H02: migrate サブコマンド（唯一の alembic 適用経路）——不正 URL では rc 2 で
  alembic 未到達・DATABASE_URL 残置なし
- M01: --rounds は正整数のみ（0/負数は固定文言+rc 2）
- M02: check ごとの単調時計計時（#N elapsed=..s を出力）
"""

import io
import os
import unittest
from unittest.mock import patch

from tools.tracking_pg_harness import (
    HarnessConfigError,
    _validated_local_url,
    main,
)


class TestUrlReconstruction(unittest.TestCase):
    """fix1 H01: 検証済み要素からの再構築・上書きパラメータの迂回拒否。"""

    def test_rebuilt_from_validated_components(self):
        self.assertEqual(
            _validated_local_url("postgresql://postgres:trk@127.0.0.1:5433/tracking_check"),
            "postgresql://postgres:trk@127.0.0.1:5433/tracking_check")
        # driver 明示・IPv6・user のみ・port なしも再構築で正規形へ
        self.assertEqual(
            _validated_local_url("postgresql+psycopg://u@localhost/db"),
            "postgresql://u@localhost/db")
        self.assertEqual(
            _validated_local_url("postgresql://postgres@[::1]:5433/db"),
            "postgresql://postgres@[::1]:5433/db")

    def test_credentials_requoted_not_passed_verbatim(self):
        rebuilt = _validated_local_url("postgresql://a%40b:p%3Aw@localhost/db")
        self.assertEqual(rebuilt, "postgresql://a%40b:p%3Aw@localhost/db")

    def test_override_params_rejected_without_reflection(self):
        cases = (
            "postgresql://u@localhost/db?host=evil.example.com",
            "postgresql://u@localhost/db?hostaddr=203.0.113.9",
            "postgresql://u@localhost/db?service=prod-railway",
            "postgresql://u@localhost/db?options=-csearch_path%3Dpublic",
            "postgresql://u@localhost/db?sslmode=require",   # 閉集合を置かず全面拒否
            "postgresql://u@localhost/db#fragvalue9",
        )
        for url in cases:
            with self.subTest(url=url.split("?")[-1]):
                with self.assertRaises(HarnessConfigError) as ctx:
                    _validated_local_url(url)
                msg = str(ctx.exception)
                for sentinel in ("evil.example.com", "203.0.113.9",
                                 "prod-railway", "search_path", "fragvalue9"):
                    self.assertNotIn(sentinel, msg)          # URL 値の非表示

    def test_non_local_and_malformed_rejected(self):
        for url in ("postgresql://u@db.railway.internal/db",
                    "postgresql://u@10.0.0.5/db",
                    "postgresql://u@localhost:notaport/db",
                    "postgresql://u@localhost/",             # dbname 空
                    "postgresql://u@localhost/db;evil",      # dbname grammar 外
                    "mysql://u@localhost/db",
                    ""):
            with self.subTest(url=url[:30]):
                with self.assertRaises(HarnessConfigError) as ctx:
                    _validated_local_url(url)
                self.assertNotIn("railway.internal", str(ctx.exception))


class TestCliValidation(unittest.TestCase):
    def test_rounds_must_be_positive(self):
        # fix1 M01: 0/負数は固定文言+rc 2（DB・alembic に未到達）
        for rounds in ("0", "-3"):
            with self.subTest(rounds=rounds):
                buf = io.StringIO()
                rc = main(["--rounds", rounds, "--sqlite-selftest"], out=buf)
                self.assertEqual(rc, 2)
                self.assertIn("--rounds は 1 以上", buf.getvalue())

    def test_run_rejects_bad_env_url_with_rc2(self):
        buf = io.StringIO()
        with patch.dict(os.environ, {"TRACKING_PG_URL":
                                     "postgresql://u@db.railway.app/prod"}):
            rc = main([], out=buf)
        self.assertEqual(rc, 2)
        self.assertIn("config error", buf.getvalue())
        self.assertNotIn("railway.app", buf.getvalue())      # 値の非反射

    def test_migrate_rejects_bad_env_url_and_leaves_no_database_url(self):
        # fix1 H02: 不正 URL では alembic 未到達・env 残置なし
        buf = io.StringIO()
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        env["TRACKING_PG_URL"] = "postgresql://u@localhost/db?host=evil"
        with patch.dict(os.environ, env, clear=True):
            rc = main(["migrate"], out=buf)
            self.assertEqual(rc, 2)
            self.assertNotIn("DATABASE_URL", os.environ)     # 残置なし
        self.assertNotIn("evil", buf.getvalue())


class TestSelftestSmoke(unittest.TestCase):
    def test_selftest_outputs_elapsed_per_check(self):
        # fix1 M02: check ごとの単調時計計時が出力に含まれる（selftest で確認）
        buf = io.StringIO()
        rc = main(["--sqlite-selftest", "--rounds", "1"], out=buf)
        text = buf.getvalue()
        self.assertEqual(rc, 0, text)
        self.assertRegex(text, r"#1 elapsed=\d+\.\ds")
        self.assertRegex(text, r"#2 elapsed=\d+\.\ds")
        self.assertIn("SELFTEST", text)


if __name__ == "__main__":
    unittest.main()
