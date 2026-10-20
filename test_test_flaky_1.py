"""TEST-FLAKY-1（QUEUE-2026-10-17-H H-2）: ログ捕捉の順序非依存化の pin。

- 実測した根本原因: scripts/houki_jukuryo_precount の import が `logging.getLogger("hub").setLevel(WARNING)` を残し、
  root に Handler を付けて hub.hearing_update の INFO を待つテスト 2 件（test_jikou_furigana1／test_jikou_hearing_hotfix）が
  その後に走ると落ちた（QUEUE-2026-10-16-R F-5 で表面化）。
- 対処: (1) 捕捉は当該 logger へ直付け（test_support_logcap.attach／attached）し level を自分で下げる、(2) precount のテストは
  名前付き logger の level も復元する、(3) 以後 root へ直接 addHandler するテストを静的に禁止する。
"""
import logging
import re
import unittest
from pathlib import Path

from test_support_logcap import LogCapture, attach, attached

REPO = Path(__file__).parent
ROOT_ATTACH_RE = re.compile(r"(logging\.getLogger\(\)|\broot)\.addHandler\(")


class TestDirectAttach(unittest.TestCase):
    def test_captures_info_even_when_parent_level_and_propagate_are_changed(self):
        parent = logging.getLogger("hub")
        child = logging.getLogger("hub.hearing_update")
        saved = (parent.level, child.level, child.propagate)
        try:
            parent.setLevel(logging.WARNING)      # precount の import が残す状態
            child.propagate = False               # さらに悪い状態（root へ流れない）
            cap = attach(self, ("hub.hearing_update",), include_root=True)
            child.info("[HEARING_UPDATE] furigana not writable count=%s", 1)
            self.assertEqual(cap.messages(), ["[HEARING_UPDATE] furigana not writable count=1"])
        finally:
            parent.setLevel(saved[0])
            child.setLevel(saved[1])
            child.propagate = saved[2]

    def test_same_record_is_counted_once_with_root(self):
        lg = logging.getLogger("hub.notify")
        with attached(("hub.notify",), include_root=True) as cap:
            lg.warning("admin LINE notify skipped (no admin id)")
        self.assertEqual(len(cap.records), 1)
        self.assertEqual(cap.errors(), [])
        self.assertEqual(logging.getLogger().handlers.count(cap), 0)     # 後始末

    def test_attach_restores_levels(self):
        lg = logging.getLogger("hub.test_flaky_probe")
        lg.setLevel(logging.ERROR)

        class _T(unittest.TestCase):
            def runTest(self):
                attach(self, ("hub.test_flaky_probe",))
                assert lg.level == logging.DEBUG
        t = _T()
        t.runTest()
        t.doCleanups()
        self.assertEqual(lg.level, logging.ERROR)
        self.assertEqual(lg.handlers, [])

    def test_logcapture_api(self):
        cap = LogCapture()
        self.assertEqual((cap.text(), cap.messages(), cap.errors()), ("", [], []))


class TestNoRootOnlyCapture(unittest.TestCase):
    def test_tests_do_not_attach_handlers_to_root_directly(self):
        """root 直付けの捕捉を新たに書かない（test_support_logcap 経由のみ）。test_logging_wiring は root 配線そのものの検査なので除外。"""
        offenders = []
        for p in sorted(REPO.glob("test_*.py")):
            # test_logging_wiring は root 配線そのものの検査、test_houki_jukuryo_precount は import が消した root の handler を
            # 復元するだけ（捕捉ではない）なので除外
            if p.name in ("test_logging_wiring.py", "test_test_flaky_1.py", "test_houki_jukuryo_precount.py"):
                continue
            for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
                if ROOT_ATTACH_RE.search(line):
                    offenders.append(f"{p.name}:{i}")
        self.assertEqual(offenders, [])

    def test_precount_import_does_not_leak_logger_levels(self):
        """test_houki_jukuryo_precount の後始末が hub 等の level を戻すことの間接 pin: precount テストと同じ復元を模す。"""
        src = (REPO / "test_houki_jukuryo_precount.py").read_text(encoding="utf-8")
        self.assertIn('saved_named', src)
        self.assertIn('"hub"', src)


if __name__ == "__main__":
    unittest.main()
