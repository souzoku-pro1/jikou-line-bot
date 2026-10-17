"""AUDIT-1（QUEUE-2026-10-16-R F-5）: scripts/audit_static.py の検出すべき最小例／検出してはいけない最小例

固定する仕様:
- (a) PII らしき識別子が print／logger／例外／URL へ流れると検出。emit(...) で包まれた引数・定数のみ・安全語（filename 等）は検出しない。
  redaction_sink_allowlist.json に載る行は known_allowlist=True（仮分類は判断保留）。
- (b) 送信関数（push_text・notify_admin_line・update_record 等）の呼出を列挙し、呼出関数内に承認文脈の印が無いものを区別する。
- (c) sa.delete／delete_record／DELETE FROM／drop_table／os.remove／shutil.rmtree を検出。コメント行は検出しない。
- (d) 鍵の接頭辞・長い英数列・password=直書きを検出。dummy／test／placeholder・短い文字列・os.environ 経由は検出しない。
- (e) 参照 0 の関数・import されない module を候補にする（decorator 付き・main／upgrade／downgrade は除外）。
- スクリプト自体は repo のコードを import しない（AST／正規表現のみ）。print は定数のみ。
"""

import importlib.util
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from test_sink_ast_policy import scan_source

REPO = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("audit_static", REPO / "scripts" / "audit_static.py")
A = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(A)


class _Repo(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="audit_"))
        (self.dir / "hub").mkdir()
        (self.dir / "hub" / "__init__.py").write_text("", encoding="utf-8")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def put(self, rel: str, src: str) -> None:
        p = self.dir / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(src, encoding="utf-8")

    def run_audit(self) -> list:
        return A.run(self.dir)

    def of(self, findings, category):
        return [f for f in findings if f.category == category]


class TestPiiSink(_Repo):
    def test_detects_pii_to_print_logger_exception_url(self):
        self.put("app.py", (
            "import logging\nlogger = logging.getLogger('x')\n"
            "def f(customer_name, phone, user_message):\n"
            "    print(customer_name)\n"
            "    logger.info('user %s', phone)\n"
            "    raise ValueError(f'bad {user_message}')\n"
            "def g(client, line_user_id):\n"
            "    url = f'https://api/{line_user_id}'\n"
            "    return client.get(f'https://x/{line_user_id}')\n"))
        a = self.of(self.run_audit(), A.CATEGORY_A)
        sinks = sorted(f.detail.split(" ")[0] for f in a)
        self.assertEqual(sinks, ["exception:ValueError", "logger.info", "print", "url", "url"])
        by_sink = {f.detail.split(' ')[0]: f.classification for f in a}
        self.assertEqual((by_sink['print'], by_sink['logger.info']), (A.CLASS_FIX, A.CLASS_FIX))
        self.assertEqual(by_sink['url'], A.CLASS_FALSE)                       # API の問い合わせ条件は誤検知扱い
        self.assertEqual(by_sink['exception:ValueError'], A.CLASS_HOLD)
        self.assertEqual({f.file for f in a}, {'app.py'})

    def test_does_not_flag_emit_constants_or_safe_words(self):
        self.put("app.py", (
            "import logging\nfrom hub.redact import emit\nlogger = logging.getLogger('x')\n"
            "def f(customer_name, filename, template_name):\n"
            "    print('fixed text')\n"
            "    logger.info('name=%s', emit(customer_name, 'name', 'log', 'operator'))\n"
            "    logger.info('file=%s', filename)\n"
            "    logger.warning('template=%s', template_name)\n"
            "    raise RuntimeError('fixed reason')\n"))
        self.assertEqual(self.of(self.run_audit(), A.CATEGORY_A), [])

    def test_known_allowlist_rows_are_marked(self):
        self.put("app.py", "def f(phone):\n    print(phone)\n")
        (self.dir / "redaction_sink_allowlist.json").write_text(json.dumps({"entries": ["app.py:2:sink:print"]}), encoding="utf-8")
        a = self.of(self.run_audit(), A.CATEGORY_A)
        self.assertEqual(len(a), 1)
        self.assertTrue(a[0].known_allowlist)
        self.assertEqual(a[0].classification, A.CLASS_HOLD)


class TestSendPaths(_Repo):
    def test_lists_sends_and_distinguishes_approval_marks(self):
        self.put("hub/x.py", (
            "from hub import line_channel, kintone\n"
            "async def auto(user):\n    return await line_channel.push_text(CH, user, 'hi')\n"
            "async def approved(user, ticket, confirm):\n    if confirm:\n        return await line_channel.push_text(CH, user, 'hi')\n"
            "async def write(app, rid):\n    await kintone.update_record(app, rid, {'a': 1})\n"))
        b = self.of(self.run_audit(), A.CATEGORY_B)
        by_func = {f.detail.split(" in ")[1]: f for f in b}
        self.assertEqual(set(by_func), {"auto()", "approved()", "write()"})
        self.assertEqual(by_func["approved()"].classification, A.CLASS_FALSE)
        self.assertEqual(by_func["auto()"].classification, A.CLASS_FIX)        # bot 経路の module 以外で印なし＝要修正
        self.assertEqual(by_func["write()"].classification, A.CLASS_HOLD)
        self.assertIn("ticket", by_func["approved()"].extra["marks_in_function"])

    def test_non_send_calls_are_not_listed(self):
        self.put("hub/x.py", "def f(x):\n    return x.update_state(1) + str(x.get_record())\n")
        self.assertEqual(self.of(self.run_audit(), A.CATEGORY_B), [])


class TestDeleteAndSecrets(_Repo):
    def test_delete_patterns(self):
        self.put("hub/x.py", (
            "import os, shutil, sqlalchemy as sa\n"
            "def f(s, t, p):\n    s.execute(sa.delete(t))\n    os.remove(p)\n    shutil.rmtree(p)\n"
            "    s.execute('DELETE FROM x')\n    # sa.delete(t) in a comment\n"))
        c = self.of(self.run_audit(), A.CATEGORY_C)
        self.assertEqual(sorted(f.line for f in c), [3, 4, 5, 6])
        self.put("hub/y.py", "def g():\n    return 'select'\n")
        self.assertEqual([f.file for f in self.of(self.run_audit(), A.CATEGORY_C) if f.file == "hub/y.py"], [])

    def test_secret_patterns(self):
        self.put("hub/x.py", (
            "KEY = 'sk-abcdefghijklmnopqrstuvwxyz0123456789'\n"
            "AWS = 'AKIAABCDEFGHIJKLMNOP'\n"
            "password = 'hunter2hunter2'\n"
            "SHA = '0' * 64\n"
            "OK1 = 'dummy_token_value_for_tests_only'\n"
            "OK2 = os.environ.get('X', '')\n"
            "import os\n"))
        d = self.of(self.run_audit(), A.CATEGORY_D)
        self.assertEqual(sorted(f.line for f in d), [1, 2, 3])
        self.assertTrue(all(f.classification == A.CLASS_HOLD for f in d))
        self.assertFalse(any("hunter2" in f.detail and "hunter2hunter2" in f.detail for f in d if f.line != 3))


class TestUnused(_Repo):
    def test_unused_function_and_module(self):
        self.put("hub/used.py", "def helper():\n    return 1\n")
        self.put("hub/orphan.py", "def lonely():\n    return 2\n\ndef main():\n    pass\n")
        self.put("main.py", "from hub import used\nfrom fastapi import APIRouter\nrouter = APIRouter()\n"
                           "@router.get('/x')\ndef route():\n    return used.helper()\n")
        e = self.run_audit()
        funcs = {f.detail for f in self.of(e, A.CATEGORY_E_FUNC)}
        mods = {f.file for f in self.of(e, A.CATEGORY_E_MOD)}
        self.assertTrue(any("def lonely" in d for d in funcs))
        self.assertFalse(any("def helper" in d for d in funcs))
        self.assertFalse(any("def route" in d for d in funcs))           # decorator 付きは除外
        self.assertFalse(any("def main" in d for d in funcs))
        self.assertIn("hub/orphan.py", mods)
        self.assertNotIn("hub/used.py", mods)
        self.assertNotIn("main.py", mods)


class TestReportAndPins(unittest.TestCase):
    def test_markdown_and_json_shape(self):
        f = A.Finding(A.CATEGORY_C, "a.py", 3, "sa.delete: x")
        A.classify(f)
        md = A.to_markdown([f], "t")
        self.assertIn("| a_pii_sink | 0 |", md)
        self.assertIn("a.py:3", md)
        self.assertIn(A.CLASS_HOLD, md)
        self.assertEqual(A.summarize([f])[A.CATEGORY_C]["total"], 1)

    def test_script_is_static_and_sink_clean(self):
        src = (REPO / "scripts" / "audit_static.py").read_text(encoding="utf-8")
        self.assertEqual(scan_source(src, "scripts/audit_static.py"), [])
        body = src.split('"""', 2)[2]
        for bad in ("import main", "import hub", "from hub", "httpx", "requests", "subprocess", "importlib"):
            self.assertNotIn(bad, body)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        self.assertNotIn("audit_static", allow)
        tsrc = (REPO / "test_audit_static.py").read_text(encoding="utf-8")
        self.assertIn("検出してはいけない最小例", tsrc)

    def test_runs_on_this_repo_without_error(self):
        # TEST-FLAKY-1（QUEUE-2026-10-17-H H-2）で repo 全体の実走に戻した: R F-5 で「本テストが xdist の worker 割当を変え
        # 既存の log 捕捉系 2 件が落ちる」として scripts/ 配下に縮小したが、落ちた原因は本テストの重さではなく
        # test_houki_jukuryo_precount が残す "hub" logger の level（順序依存）であり、TEST-FLAKY-1 で捕捉を当該 logger
        # 直付けにして解消した（単体実測 約 36 秒・suite 全体では -n auto で吸収）。
        findings = A.run(REPO)
        self.assertTrue(findings)
        self.assertTrue(all(f.classification in (A.CLASS_FIX, A.CLASS_FALSE, A.CLASS_HOLD) for f in findings))
        self.assertTrue(all(f.reason for f in findings))


if __name__ == "__main__":
    unittest.main()
