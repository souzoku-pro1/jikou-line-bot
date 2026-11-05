"""REGISTRY-EXPIRY-WARN-1（QUEUE-2026-10-20-J J-1）: scripts/registry_tool.py の契約。

  - check: 未設定/不正/実効鍵 0 → exit 1・有効 → exit 0。出力に秘密値は出ない
  - extend: 空/不正 → 何も書かず exit 2。有効 → %TEMP%/reg.json（Python 側で TEMP を解決）に全 entry の
    expires_at を --until（JST 0 時）で書き換えた JSON。secret は不変
  - rebuild: template（secret なし）+ secrets-dir。無い kid は生成して reg_secret_<kid>.txt に書く／
    --no-generate なら exit 2。template に secret があれば exit 2。書く前に parse_registry 検証
  - いずれも「set」は行わない（出力に railway variables --set を**別コマンド**として案内するだけ）
"""

import contextlib
import datetime
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from scripts import registry_tool as rt  # noqa: E402

SECRET = "cd" * 32
JST = datetime.timezone(datetime.timedelta(hours=9))


def _entry(exp: int, status: str = "active") -> dict:
    return {"secret": SECRET, "caller": "gas-ingest", "allowed_methods": ["POST"],
            "allowed_paths": ["/koseki/ingest"], "status": status, "not_before": 0, "expires_at": exp}


def _future(days: int) -> int:
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp()) + days * 86400


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="registry_tool_")
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))

    def env(self, registry: str | None, **extra):
        base = {"TEMP": self.tmp, "TMP": self.tmp}
        if registry is not None:
            base["SERVICE_HMAC_KEY_REGISTRY"] = registry
        base.update(extra)
        p = patch.dict(os.environ, base)
        p.start()
        self.addCleanup(p.stop)
        if registry is None:
            os.environ.pop("SERVICE_HMAC_KEY_REGISTRY", None)

    def run_tool(self, *argv) -> tuple[int, str]:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = rt.main(list(argv))
        return code, buf.getvalue()

    @property
    def out_file(self) -> Path:
        return Path(self.tmp) / "reg.json"


class TestCheck(_Base):
    def test_empty_and_unset(self):
        for reg in ("", None):
            with self.subTest(reg=reg):
                self.env(reg)
                code, out = self.run_tool("check")
                self.assertEqual(code, rt.EXIT_NOT_EFFECTIVE)
                self.assertIn("empty", out)

    def test_invalid(self):
        self.env("{broken")
        code, out = self.run_tool("check")
        self.assertEqual(code, rt.EXIT_NOT_EFFECTIVE)
        self.assertIn("invalid", out)

    def test_all_expired(self):
        self.env(json.dumps({"k1": _entry(_future(-5))}))
        code, out = self.run_tool("check")
        self.assertEqual(code, rt.EXIT_NOT_EFFECTIVE)
        self.assertIn("EXPIRED", out)
        self.assertIn("effective_keys= 0", out)
        self.assertNotIn(SECRET, out)

    def test_effective(self):
        self.env(json.dumps({"k1": _entry(_future(40)), "k2": _entry(_future(400), "revoked")}))
        code, out = self.run_tool("check")
        self.assertEqual(code, rt.EXIT_OK)
        self.assertIn("k1", out)
        self.assertIn("EFFECTIVE", out)
        self.assertIn("not effective", out)
        self.assertIn("effective_keys= 1", out)
        self.assertNotIn(SECRET, out)


class TestExtend(_Base):
    def test_empty_writes_nothing(self):
        self.env("")
        code, out = self.run_tool("extend", "--until", "2027-07-01")
        self.assertEqual(code, rt.EXIT_ABORT)
        self.assertIn("ABORT", out)
        self.assertFalse(self.out_file.exists())

    def test_invalid_writes_nothing(self):
        self.env(json.dumps({"k1": {"caller": "x"}}))
        code, _ = self.run_tool("extend", "--until", "2027-07-01")
        self.assertEqual(code, rt.EXIT_ABORT)
        self.assertFalse(self.out_file.exists())

    def test_extends_all_entries_in_temp(self):
        self.env(json.dumps({"k1": _entry(_future(-5)), "k2": _entry(_future(3))}))
        code, out = self.run_tool("extend", "--until", "2027-07-01")
        self.assertEqual(code, rt.EXIT_OK)
        self.assertTrue(self.out_file.exists())
        data = json.loads(self.out_file.read_text(encoding="utf-8"))
        expected = int(datetime.datetime(2027, 7, 1, tzinfo=JST).timestamp())
        self.assertEqual({k: v["expires_at"] for k, v in data.items()}, {"k1": expected, "k2": expected})
        self.assertEqual(data["k1"]["secret"], SECRET)       # 秘密値は不変（ファイルにのみ）
        self.assertNotIn(SECRET, out)                         # 画面には出ない
        self.assertIn("railway variables --set", out)         # set は別コマンドの案内のみ
        self.assertNotIn("&&", out)

    def test_string_expires_at_keeps_type(self):
        e = _entry(_future(3))
        e["expires_at"] = str(e["expires_at"])
        self.env(json.dumps({"k1": e}))
        code, _ = self.run_tool("extend", "--until", "2027-01-01")
        self.assertEqual(code, rt.EXIT_OK)
        data = json.loads(self.out_file.read_text(encoding="utf-8"))
        self.assertIsInstance(data["k1"]["expires_at"], str)

    def test_past_until_writes_nothing(self):
        self.env(json.dumps({"k1": _entry(_future(3))}))
        code, out = self.run_tool("extend", "--until", "2020-01-01")
        self.assertEqual(code, rt.EXIT_ABORT)
        self.assertIn("no effective keys", out)
        self.assertFalse(self.out_file.exists())


class TestRebuild(_Base):
    def _template(self, **override) -> str:
        tpl = {"gas-ingest-2026-07a": {"caller": "gas-ingest", "allowed_methods": ["POST"],
                                       "allowed_paths": ["/koseki/ingest"], "status": "active",
                                       "not_before": 0, "expires_at": _future(300)}}
        tpl.update(override)
        p = Path(self.tmp) / "tpl.json"
        p.write_text(json.dumps(tpl), encoding="utf-8")
        return str(p)

    def test_generates_missing_secret_and_validates(self):
        self.env("")
        sd = Path(self.tmp) / "secrets"
        code, out = self.run_tool("rebuild", "--template", self._template(), "--secrets-dir", str(sd))
        self.assertEqual(code, rt.EXIT_OK)
        self.assertIn("GENERATED", out)
        sec = (sd / "reg_secret_gas-ingest-2026-07a.txt").read_text(encoding="utf-8").strip()
        self.assertEqual(len(bytes.fromhex(sec)), 32)
        data = json.loads(self.out_file.read_text(encoding="utf-8"))
        self.assertEqual(data["gas-ingest-2026-07a"]["secret"], sec)
        self.assertNotIn(sec, out)

    def test_uses_existing_secret_file(self):
        self.env("")
        sd = Path(self.tmp) / "secrets"
        sd.mkdir()
        (sd / "reg_secret_gas-ingest-2026-07a.txt").write_text(SECRET + "\n", encoding="utf-8")
        code, out = self.run_tool("rebuild", "--template", self._template(), "--secrets-dir", str(sd),
                                  "--until", "2027-01-01")
        self.assertEqual(code, rt.EXIT_OK)
        self.assertIn("from file", out)
        data = json.loads(self.out_file.read_text(encoding="utf-8"))
        self.assertEqual(data["gas-ingest-2026-07a"]["secret"], SECRET)
        self.assertEqual(data["gas-ingest-2026-07a"]["expires_at"],
                         int(datetime.datetime(2027, 1, 1, tzinfo=JST).timestamp()))

    def test_no_generate_aborts(self):
        self.env("")
        code, out = self.run_tool("rebuild", "--template", self._template(),
                                  "--secrets-dir", str(Path(self.tmp) / "s"), "--no-generate")
        self.assertEqual(code, rt.EXIT_ABORT)
        self.assertFalse(self.out_file.exists())

    def test_template_with_secret_aborts(self):
        self.env("")
        tpl = self._template(**{"gas-ingest-2026-07a": {"secret": "x", "caller": "c"}})
        code, _ = self.run_tool("rebuild", "--template", tpl, "--secrets-dir", str(Path(self.tmp) / "s"))
        self.assertEqual(code, rt.EXIT_ABORT)
        self.assertFalse(self.out_file.exists())

    def test_bad_secret_file_aborts(self):
        self.env("")
        sd = Path(self.tmp) / "secrets"
        sd.mkdir()
        (sd / "reg_secret_gas-ingest-2026-07a.txt").write_text("zz", encoding="utf-8")
        code, _ = self.run_tool("rebuild", "--template", self._template(), "--secrets-dir", str(sd))
        self.assertEqual(code, rt.EXIT_ABORT)
        self.assertFalse(self.out_file.exists())

    def test_example_template_is_valid_shape(self):
        self.env("")
        sd = Path(self.tmp) / "secrets"
        code, _ = self.run_tool("rebuild", "--template", "scripts/registry_template.example.json",
                                "--secrets-dir", str(sd), "--until", "2027-01-01")
        self.assertEqual(code, rt.EXIT_OK)
        data = json.loads(self.out_file.read_text(encoding="utf-8"))
        self.assertEqual(set(data), {"gas-ingest-2026-07a", "pc-watcher-2026-08a"})


if __name__ == "__main__":
    unittest.main()
