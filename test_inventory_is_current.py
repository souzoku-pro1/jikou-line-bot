"""INVENTORY-1（QUEUE-2026-10-14-G G-6）: docs/inventory.md が scripts/gen_inventory.py の生成結果と一致すること（CI 相当）

- 不一致（画面・ジョブ・env・flag・kintone 欄・文言を足したのに目録を再生成していない）は fail。
  直し方: `python scripts/gen_inventory.py` を実行して docs/inventory.md を commit する。
- 冪等: 2 回生成して同一。値（env の実値・token・URL）・時刻・SHA を含まない。
- pin: 新 script は sink 違反ゼロ・allowlist 追加ゼロ。
"""

import importlib.util
import pathlib
import re
import unittest

from test_sink_ast_policy import scan_source

REPO = pathlib.Path(__file__).parent
_spec = importlib.util.spec_from_file_location("gen_inventory", REPO / "scripts" / "gen_inventory.py")
G = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(G)


class TestInventoryIsCurrent(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.generated = G.build()

    def test_docs_inventory_matches_generated(self):
        current = (REPO / "docs" / "inventory.md").read_text(encoding="utf-8")
        self.assertEqual(current, self.generated,
                         "docs/inventory.md が古い: python scripts/gen_inventory.py で再生成して commit する")

    def test_idempotent(self):
        self.assertEqual(self.generated, G.build())

    def test_sections_and_known_entries(self):
        g = self.generated
        for h in ("## (a) 画面一覧", "## (b) ジョブ一覧", "## (c) 送信台帳", "## (d) env 変数一覧", "## (e) flag 一覧",
                  "## (f) kintone", "## (g) 相談者向け文言"):
            self.assertIn(h, g, h)
        self.assertIn("| /app/login | GET | 公開 |", g)
        self.assertIn("ログイン必須（_gate）", g)
        self.assertIn("SEND_LEDGER_RECOVER", g)
        self.assertIn("HEALTHCHECK", g)
        self.assertIn("| PURPOSE_OTHER | other | はい |", g)
        self.assertIn("| JIKOU_SEND_POLICY_V2 |", g)
        self.assertIn("| INBOUND_EVENT_DURABLE_ENABLED |", g)
        self.assertIn("APP_APPROVAL", g)
        self.assertIn("FROZEN_GREETING", g)

    def test_no_values_secrets_or_timestamps(self):
        g = self.generated
        self.assertNotRegex(g, r"postgres(ql)?://|\.cybozu\.com|rlwy\.net|Bearer |ghp_")
        self.assertNotRegex(g, r"\bU[0-9a-f]{32}\b")
        self.assertNotRegex(g, r"20\d\d-\d\d-\d\d[ T]\d\d:\d\d")            # 生成時刻を書かない
        self.assertNotRegex(g, r"\b[0-9a-f]{40}\b")                            # SHA を書かない

    def test_pins_sink_and_allowlist(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("scripts/gen_inventory.py",):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
            self.assertNotIn(name, allow)
        self.assertTrue((REPO / "docs" / "inventory.md").read_text(encoding="utf-8").startswith("<!-- 自動生成"))
        self.assertFalse(re.search(r"手書き", self.generated.split("\n", 6)[1]))


if __name__ == "__main__":
    unittest.main()
