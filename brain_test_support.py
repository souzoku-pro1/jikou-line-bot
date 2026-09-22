"""BRAIN-A1 テストの共通土台（test_brain_a1_*.py が import する・本番コードではない）

- BrainDbMixin: 1 テスト 1 ファイルの sqlite（aiosqlite）に brain_ledger の表を作る
  （test_q_batch1 の _DbMixin と同じ流儀・reset_for_tests で engine を捨てる）
- FakeKintone: brain_sync が発行する query 文法（更新日時/$id の keyset・
  案件アプリID・$id =/in/>・order by・limit）を解釈する読み取り専用の偽 kintone。
  書込 API は用意しない（呼ばれたら AttributeError＝T-E で書込ゼロを pin）
"""

import asyncio
import os
import re
import shutil
import tempfile
import unittest
from unittest.mock import patch

BRAIN_ENV = {
    "KINTONE_SUBDOMAIN": "testsub", "APP_HOUKI": "40", "TOKEN_HOUKI": "d",
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "d", "APP_CHATLOG": "28",
    "TOKEN_CHATLOG": "d", "SOUZOKU_KINTONE_APP_ID": "26",
}

LINE_A = "U" + "a" * 32
LINE_B = "U" + "b" * 32
LINE_C = "U" + "c" * 32


def run(coro):
    return asyncio.run(coro)


def rec(rid, rev, updated, **fields):
    """kintone records API の形（{"code": {"value": ...}}）を組む。"""
    r = {"$id": {"value": str(rid)}, "$revision": {"value": str(rev)},
         "更新日時": {"value": updated}, "作成日時": {"value": updated}}
    for k, v in fields.items():
        r[k] = {"value": v}
    return r


def subrow(row_id, **cols):
    return {"id": str(row_id), "value": {k: {"value": v} for k, v in cols.items()}}


def app40(rid, rev, updated="2026-09-20T01:00:00Z", **fields):
    base = {"status": "受任", "顧客名": "テスト太郎", "LINEユーザーID": LINE_A,
            "被相続人氏名": "テスト花子", "死亡日": "2026-06-01",
            "債権者一覧": [subrow(11, 債権者名="甲社", 通知要否="要"),
                           subrow(12, 債権者名="乙社", 通知要否="不要")]}
    base.update(fields)
    return rec(rid, rev, updated, **base)


def app30(rid, rev, updated="2026-09-21T00:00:00Z", **fields):
    base = {"案件アプリID": "40", "案件レコードID": "1", "ユニット種別": "相続放棄",
            "発送ステータス": "下書き", "件名": "受理通知送付状", "宛先名": "甲社"}
    base.update(fields)
    return rec(rid, rev, updated, **base)


def app28(rid, rev, updated="2026-09-21T00:00:00Z", **fields):
    base = {"line_user_id": LINE_A, "role": "user", "message": "こんにちは",
            "category": "相続放棄ヒアリング", "auto_sent": "no"}
    base.update(fields)
    return rec(rid, rev, updated, **base)


class FakeKintone:
    """query を解釈する偽 kintone（読取のみ）。fail_on: 呼出し n 回目で例外。"""

    def __init__(self, data: dict | None = None):
        self.data = {"40": [], "30": [], "28": []}
        if data:
            self.data.update(data)
        self.calls: list = []
        self.fail_at: set = set()
        self.raise_all = False

    def _match(self, r: dict, query: str) -> bool:
        ts = r["更新日時"]["value"]
        rid = int(r["$id"]["value"])
        m = re.search(r'更新日時 >= "([^"]+)"', query)
        if m and not ts >= m.group(1):
            return False
        m = re.search(r'更新日時 > "([^"]+)" or \(更新日時 = "([^"]+)" and \$id > (\d+)\)', query)
        if m and not (ts > m.group(1) or (ts == m.group(1) and rid > int(m.group(3)))):
            return False
        m = re.search(r'案件アプリID = "([^"]+)"', query)
        if m and (r.get("案件アプリID") or {}).get("value") != m.group(1):
            return False
        m = re.search(r'\$id = "(\d+)"', query)
        if m and rid != int(m.group(1)):
            return False
        m = re.search(r'\$id in \(([^)]*)\)', query)
        if m and str(rid) not in [x.strip().strip('"') for x in m.group(1).split(",")]:
            return False
        m = re.search(r'\$id > (\d+)', query)
        if m and "更新日時 = " not in query and rid <= int(m.group(1)):
            return False
        return True

    async def search_records(self, app, query, fields=None):
        self.calls.append((app.app_id(), query))
        if self.raise_all or len(self.calls) in self.fail_at:
            raise RuntimeError("fake kintone failure")
        rows = [r for r in self.data.get(app.app_id(), []) if self._match(r, query)]
        if "order by 更新日時" in query:
            rows.sort(key=lambda r: (r["更新日時"]["value"], int(r["$id"]["value"])))
        else:
            rows.sort(key=lambda r: int(r["$id"]["value"]))
        m = re.search(r"limit (\d+)", query)
        return rows[: int(m.group(1))] if m else rows


class BrainDbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="brain_a1_")
        env = dict(BRAIN_ENV)
        env["DATABASE_URL"] = f"sqlite+aiosqlite:///{self._dir}/b.db"
        env.setdefault("BRAIN_SYNC_ENABLED", "1")
        self._env = patch.dict(os.environ, env)
        self._env.start()
        from hub import brain_ledger, db
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(brain_ledger.metadata.create_all)
        asyncio.run(_create())
        db.reset_for_tests()
        self.fake = FakeKintone()
        from hub import kintone
        self._kp = patch.object(kintone, "search_records", self.fake.search_records)
        self._kp.start()

    def tearDown(self):
        from hub import db
        self._kp.stop()
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)
