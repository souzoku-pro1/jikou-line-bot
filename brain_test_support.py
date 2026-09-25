"""BRAIN-A1 テストの共通土台（test_brain_a1_*.py が import する・本番コードではない）

- BrainDbMixin: 1 テスト 1 ファイルの sqlite（aiosqlite）に brain_ledger の表を作る
  （test_q_batch1 の _DbMixin と同じ流儀・reset_for_tests で engine を捨てる）
- FakeKintone: kintone records API の query 文法を**厳格に**解釈する読み取り専用の
  偽 kintone（補足 2）。
  - fields を尊重し、要求された欄だけを返す（未知の欄は例外）
  - 未知の欄・未対応の演算子・型に合わない演算子・不正な構文は例外（実 API の 400）
  - limit（既定 100・1〜500）/ offset（0〜10000）/ order by（複数キー・asc/desc）を
    実 API と同じ意味で扱う。order by 省略時はレコード番号の降順（実 API の既定）
  書込 API は用意しない（呼ばれたら AttributeError＝T-E で書込ゼロを pin）
"""

import asyncio
import os
import re
import shutil
import tempfile
import unittest
from unittest.mock import patch

from hub import brain_ledger as _ledger

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


# ── 偽 kintone の query 文法（実 API の部分集合・それ以外は例外） ─────────────

class FakeQueryError(ValueError):
    """実 API なら 400（GAIA_IQ11 等）になる query／fields。"""


_SYSTEM_FIELDS = {"$id": "number", "$revision": "number"}
_ORDERABLE_TYPES = {"number", "datetime", "date", "text", "choice"}
_TEXT_OPS = {"=", "!=", "in", "not in", "like", "not like"}
_RANGE_OPS = {"=", "!=", ">", "<", ">=", "<="}
_TOKEN_RE = re.compile(r'\s*(?:"((?:[^"\\]|\\.)*)"|(\(|\)|,)|(!=|>=|<=|=|>|<)|([^\s(),"]+))')


def _field_types(app_id: str) -> dict:
    if app_id == "40":
        t = dict(_ledger.app40_fields())
        t[_ledger.APP40_CREDITOR_TABLE] = "subtable"
        t[_ledger.APP40_DOCUMENT_TABLE] = "subtable"
    elif app_id == "30":
        t = dict(_ledger.app30_fields())
    elif app_id == "28":
        t = dict(_ledger.app28_fields())
        t["更新日時"] = "datetime"
    else:
        t = {}
    t.update(_SYSTEM_FIELDS)
    return t


def _tokenize(query: str) -> list:
    tokens = []
    pos = 0
    query = query.strip()
    while pos < len(query):
        m = _TOKEN_RE.match(query, pos)
        if not m or m.end() == pos:
            raise FakeQueryError("query_tokenize_failed")
        if m.group(1) is not None:
            tokens.append(("str", m.group(1)))
        elif m.group(2) is not None:
            tokens.append(("punct", m.group(2)))
        elif m.group(3) is not None:
            tokens.append(("op", m.group(3)))
        else:
            tokens.append(("word", m.group(4)))
        pos = m.end()
    return tokens


class _Parser:
    def __init__(self, tokens: list, types: dict):
        self.t = tokens
        self.i = 0
        self.types = types

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else (None, None)

    def take(self):
        tok = self.peek()
        self.i += 1
        return tok

    def expect_word(self, word: str) -> None:
        tok = self.take()
        if tok != ("word", word):
            raise FakeQueryError("query_syntax")

    def parse(self) -> dict:
        cond = None
        kind, val = self.peek()
        if kind is not None and not (kind == "word" and val in ("order", "limit", "offset")):
            cond = self.parse_or()
        order, limit, offset = [], 100, 0
        seen = set()
        while self.peek()[0] is not None:
            kind, val = self.take()
            if kind != "word" or val in seen:
                raise FakeQueryError("query_syntax")
            seen.add(val)
            if val == "order":
                self.expect_word("by")
                while True:
                    fk, field = self.take()
                    if fk != "word" or field not in self.types:
                        raise FakeQueryError("order_field_unknown")
                    if self.types[field] not in _ORDERABLE_TYPES:
                        raise FakeQueryError("order_field_not_sortable")
                    direction = "asc"
                    if self.peek() in (("word", "asc"), ("word", "desc")):
                        direction = self.take()[1]
                    order.append((field, direction))
                    if self.peek() == ("punct", ","):
                        self.take()
                        continue
                    break
            elif val == "limit":
                nk, n = self.take()
                if nk != "word" or not n.isdigit() or not 1 <= int(n) <= 500:
                    raise FakeQueryError("limit_out_of_range")
                limit = int(n)
            elif val == "offset":
                nk, n = self.take()
                if nk != "word" or not n.isdigit() or int(n) > 10000:
                    raise FakeQueryError("offset_out_of_range")
                offset = int(n)
            else:
                raise FakeQueryError("query_syntax")
        return {"cond": cond, "order": order, "limit": limit, "offset": offset}

    def parse_or(self):
        left = self.parse_and()
        while self.peek() == ("word", "or"):
            self.take()
            left = ("or", left, self.parse_and())
        return left

    def parse_and(self):
        left = self.parse_atom()
        while self.peek() == ("word", "and"):
            self.take()
            left = ("and", left, self.parse_atom())
        return left

    def parse_atom(self):
        kind, val = self.take()
        if (kind, val) == ("punct", "("):
            inner = self.parse_or()
            if self.take() != ("punct", ")"):
                raise FakeQueryError("paren_unbalanced")
            return inner
        if kind != "word" or val not in self.types:
            raise FakeQueryError("field_unknown")
        field = val
        ftype = self.types[field]
        ok, op = self.take()
        if ok == "word" and op == "not":
            ok2, op2 = self.take()
            if ok2 != "word" or op2 not in ("in", "like"):
                raise FakeQueryError("operator_unknown")
            op = "not " + op2
        elif ok == "word" and op in ("in", "like"):
            pass
        elif ok != "op":
            raise FakeQueryError("operator_unknown")
        allowed = _RANGE_OPS | {"in", "not in"} if ftype in ("number", "datetime", "date") \
            else _TEXT_OPS
        if ftype == "subtable" or op not in allowed:
            raise FakeQueryError("operator_not_allowed_for_field")
        if op in ("in", "not in"):
            if self.take() != ("punct", "("):
                raise FakeQueryError("in_list_syntax")
            values = []
            while True:
                vk, v = self.take()
                if vk not in ("str", "word"):
                    raise FakeQueryError("in_list_syntax")
                values.append(v)
                nxt = self.take()
                if nxt == ("punct", ")"):
                    break
                if nxt != ("punct", ","):
                    raise FakeQueryError("in_list_syntax")
            return ("cmp", field, ftype, op, values)
        vk, v = self.take()
        if vk == "str":
            return ("cmp", field, ftype, op, v)
        if vk == "word" and ftype == "number" and v.lstrip("-").isdigit():
            return ("cmp", field, ftype, op, v)
        raise FakeQueryError("value_syntax")


def _record_value(r: dict, field: str):
    return (r.get(field) or {}).get("value")


def _coerce(ftype: str, v):
    if ftype == "number":
        return int(str(v))
    return str(v or "")


def _eval(node, r: dict) -> bool:
    if node[0] == "and":
        return _eval(node[1], r) and _eval(node[2], r)
    if node[0] == "or":
        return _eval(node[1], r) or _eval(node[2], r)
    _tag, field, ftype, op, val = node
    actual = _record_value(r, field)
    if op in ("in", "not in"):
        members = {_coerce(ftype, x) for x in val}
        hit = _coerce(ftype, actual) in members if actual not in (None, "") else False
        return hit if op == "in" else not hit
    if op in ("like", "not like"):
        hit = str(val) in str(actual or "")
        return hit if op == "like" else not hit
    if actual in (None, ""):
        return op == "!="
    a, b = _coerce(ftype, actual), _coerce(ftype, val)
    return {"=": a == b, "!=": a != b, ">": a > b, "<": a < b,
            ">=": a >= b, "<=": a <= b}[op]


class FakeKintone:
    """query を厳格に解釈する偽 kintone（読取のみ）。fail_at: 呼出し n 回目で例外。"""

    def __init__(self, data: dict | None = None):
        self.data = {"40": [], "30": [], "28": []}
        if data:
            self.data.update(data)
        self.calls: list = []
        self.fail_at: set = set()
        self.raise_all = False

    async def search_records(self, app, query, fields=None):
        app_id = app.app_id()
        self.calls.append((app_id, query))
        if self.raise_all or len(self.calls) in self.fail_at:
            raise RuntimeError("fake kintone failure")
        types = _field_types(app_id)
        parsed = _Parser(_tokenize(query), types).parse()
        if fields:
            for f in fields:
                if f not in types:
                    raise FakeQueryError("fields_unknown")
        rows = [r for r in self.data.get(app_id, [])
                if parsed["cond"] is None or _eval(parsed["cond"], r)]
        order = parsed["order"] or [("$id", "desc")]
        for field, direction in reversed(order):
            ftype = types[field]
            rows.sort(key=lambda r, f=field, t=ftype: _coerce(t, _record_value(r, f) or 0)
                      if t == "number" else str(_record_value(r, f) or ""),
                      reverse=(direction == "desc"))
        page = rows[parsed["offset"]: parsed["offset"] + parsed["limit"]]
        if not fields:
            return [dict(r) for r in page]
        return [{f: r[f] for f in fields if f in r} for r in page]


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
