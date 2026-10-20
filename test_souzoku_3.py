"""SOUZOKU-3（QUEUE-2026-10-17-H H-4）: 相続人の確定——判定エンジン・保存／承認・画面・pin。

- 判定は hub/souzoku_law_constants の表（要大野確認）だけを参照。典型 20 ケース以上で分数が一致する。
- 未対応（数次相続・胎児・相続分の指定）は needs_attorney を返し例外にしない。
- 画面 /app/souzoku/{案件ID}/heirs: flag OFF=404・_gate・保存で新版 draft・承認は人の明示操作のみ・「弁護士確認前」の印。
- 通知なし・ログに氏名なし（入力に氏名欄が無い）。migration c1d3（b0c1 の次）・env.py 登録・sink 違反ゼロ・allowlist 追加ゼロ。
"""
import datetime
import json
import os
import shutil
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa

from test_souzoku_2 import GOOD, _run  # noqa: E402  （DB fixture の流儀と受付データを再利用）

REPO = Path(__file__).parent
import main  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from hub import db  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import souzoku_heirs as H  # noqa: E402
from hub import souzoku_intake as S  # noqa: E402
from hub import souzoku_law_constants as L  # noqa: E402
from hub import webapp_souzoku_heirs_view as V  # noqa: E402
from hub.webapp_auth import MIN_ITERATIONS, hash_password, issue_session  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

_client = TestClient(main.app)


def _auth():
    return {"Cookie": f"webapp_session={issue_session()}"}


def D(**kw):
    r = H.determine(kw)
    return r["result"], [(h["label"], h["share"]) for h in r["heirs"]]


def C(status="alive", kind="real", descendants=0, descendants_deceased=0):
    return {"status": status, "kind": kind, "descendants": descendants, "descendants_deceased": descendants_deceased}


def Sb(status="alive", blood="full", children=0, children_deceased=0):
    return {"status": status, "blood": blood, "children": children, "children_deceased": children_deceased}


# ── 1. 判定エンジン（民法 887〜890・900・901 条・表は要大野確認） ─────────────────
class TestDetermine(unittest.TestCase):
    CASES = [
        # (名前, 入力, 期待 (result, [(label, share)]))
        ("配偶者+子2", dict(spouse="alive", children=[C(), C()]), ("determined", [("配偶者", "1/2"), ("子1", "1/4"), ("子2", "1/4")])),
        ("配偶者のみ", dict(spouse="alive"), ("determined", [("配偶者", "1")])),
        ("子のみ 3（養子含む）", dict(spouse="none", children=[C(), C(kind="adopted"), C()]),
         ("determined", [("子1", "1/3"), ("子2（養子）", "1/3"), ("子3", "1/3")])),
        ("配偶者+父母", dict(spouse="alive", ascendants={"father": "alive", "mother": "alive"}),
         ("determined", [("配偶者", "2/3"), ("父", "1/6"), ("母", "1/6")])),
        ("配偶者+母のみ", dict(spouse="alive", ascendants={"father": "deceased", "mother": "alive"}),
         ("determined", [("配偶者", "2/3"), ("母", "1/3")])),
        ("父母のみ", dict(spouse="none", ascendants={"father": "alive", "mother": "alive"}),
         ("determined", [("父", "1/2"), ("母", "1/2")])),
        ("配偶者+祖父母（父母なし）", dict(spouse="alive", ascendants={"father": "deceased", "mother": "deceased", "grandparents_alive": 2}),
         ("determined", [("配偶者", "2/3"), ("祖父母1", "1/6"), ("祖父母2", "1/6")])),
        ("配偶者+兄弟 2（全血）", dict(spouse="alive", siblings=[Sb(), Sb()]),
         ("determined", [("配偶者", "3/4"), ("兄弟姉妹1", "1/8"), ("兄弟姉妹2", "1/8")])),
        ("配偶者+兄弟（全血 1・半血 1）", dict(spouse="alive", siblings=[Sb(), Sb(blood="half")]),
         ("determined", [("配偶者", "3/4"), ("兄弟姉妹1", "1/6"), ("兄弟姉妹2（半血）", "1/12")])),
        ("兄弟のみ（全血 2・半血 1）", dict(spouse="none", siblings=[Sb(), Sb(), Sb(blood="half")]),
         ("determined", [("兄弟姉妹1", "2/5"), ("兄弟姉妹2", "2/5"), ("兄弟姉妹3（半血）", "1/5")])),
        ("配偶者+子1+代襲 2（孫）", dict(spouse="alive", children=[C(), C(status="deceased", descendants=2)]),
         ("determined", [("配偶者", "1/2"), ("子1", "1/4"), ("代襲（子2）1", "1/8"), ("代襲（子2）2", "1/8")])),
        ("代襲のみ（子 1 人が死亡・孫 3）", dict(spouse="none", children=[C(status="deceased", descendants=3)]),
         ("determined", [("代襲（子1）1", "1/3"), ("代襲（子1）2", "1/3"), ("代襲（子1）3", "1/3")])),
        ("欠格の子の代襲", dict(spouse="alive", children=[C(), C(status="disqualified", descendants=1)]),
         ("determined", [("配偶者", "1/2"), ("子1", "1/4"), ("代襲（子2）1", "1/4")])),
        ("廃除の子の代襲", dict(spouse="none", children=[C(status="disinherited", descendants=2), C()]),
         ("determined", [("代襲（子1）1", "1/4"), ("代襲（子1）2", "1/4"), ("子2", "1/2")])),
        ("子が放棄（代襲なし）→ 父母へ", dict(spouse="alive", children=[C(status="renounced")], ascendants={"father": "alive", "mother": "alive"}),
         ("determined", [("配偶者", "2/3"), ("父", "1/6"), ("母", "1/6")])),
        ("子 1 人放棄・1 人生存", dict(spouse="alive", children=[C(status="renounced"), C()]),
         ("determined", [("配偶者", "1/2"), ("子2", "1/2")])),
        ("父放棄・母死亡 → 祖父母", dict(spouse="alive", children=[], ascendants={"father": "renounced", "mother": "deceased", "grandparents_alive": 1}),
         ("determined", [("配偶者", "2/3"), ("祖父母1", "1/3")])),
        ("兄弟の代襲（甥姪 2）", dict(spouse="alive", siblings=[Sb(status="deceased", children=2), Sb()]),
         ("determined", [("配偶者", "3/4"), ("代襲（兄弟姉妹1）1", "1/16"), ("代襲（兄弟姉妹1）2", "1/16"), ("兄弟姉妹2", "1/8")])),
        ("半血兄弟の代襲", dict(spouse="none", siblings=[Sb(), Sb(status="deceased", blood="half", children=1)]),
         ("determined", [("兄弟姉妹1", "2/3"), ("代襲（兄弟姉妹2（半血））1", "1/3")])),
        ("配偶者先死亡・子 2", dict(spouse="deceased", children=[C(), C()]), ("determined", [("子1", "1/2"), ("子2", "1/2")])),
        ("配偶者放棄・子 1", dict(spouse="renounced", children=[C()]), ("determined", [("子1", "1")])),
        ("全員放棄", dict(spouse="renounced", children=[C(status="renounced")], ascendants={"father": "renounced", "mother": "none"},
                     siblings=[Sb(status="renounced")]), ("no_heirs", [])),
        ("相続人なし（入力なし）", dict(spouse="none"), ("no_heirs", [])),
        ("遺言あり（公正証書）は注記のみ", dict(spouse="alive", children=[C()], will="notarial"),
         ("determined", [("配偶者", "1/2"), ("子1", "1/2")])),
    ]

    def test_typical_cases_match_statutory_shares(self):
        self.assertGreaterEqual(len(self.CASES), 20)
        for name, inp, expected in self.CASES:
            with self.subTest(name=name):
                self.assertEqual(D(**inp), expected)
                r = H.determine(inp)
                if r["heirs"]:
                    self.assertEqual(sum((Fraction(h["share"]) for h in r["heirs"]), Fraction(0)), 1)
                for h in r["heirs"]:
                    self.assertTrue(h["basis"] and all(b.startswith("民法") for b in h["basis"]))
                self.assertEqual(r["review_mark"], "弁護士確認前")

    def test_unsupported_returns_needs_attorney_without_exception(self):
        for key, label in (("successive", "数次相続"), ("fetus", "胎児"), ("designated_shares", "相続分の指定"),
                           ("renunciation_pending", "相続放棄の申述")):
            with self.subTest(key=key):
                r = H.determine(dict(spouse="alive", children=[C()], special={key: True}))
                self.assertEqual(r["result"], H.RESULT_NEEDS_ATTORNEY)
                self.assertTrue(any(label in x for x in r["attorney_reasons"]))
                self.assertEqual([(h["label"], h["share"]) for h in r["heirs"]], [("配偶者", "1/2"), ("子1", "1/2")])   # 参考値は出す
        # 甥姪の再代襲（一代限り）は相続人にせず要確認
        r = H.determine(dict(spouse="alive", siblings=[Sb(status="deceased", children=2, children_deceased=1)]))
        self.assertEqual(r["result"], H.RESULT_NEEDS_ATTORNEY)
        self.assertEqual([h["share"] for h in r["heirs"]], ["3/4", "1/8", "1/8"])
        # 遺言ありは注記（法定相続分は参考値）
        r = H.determine(dict(spouse="alive", children=[C()], will="holograph"))
        self.assertIn(L.WILL_NOTE, r["notes"])
        r = H.determine(dict(spouse="alive", children=[C()], will="unknown"))
        self.assertTrue(any("不明" in n for n in r["notes"]))

    def test_invalid_input_is_rejected_and_names_are_not_part_of_the_structure(self):
        for bad in (dict(spouse="x"), dict(children=[{"status": "alive", "kind": "pet"}]), dict(siblings=[{"status": "alive", "blood": "z"}]),
                    dict(will="oral"), dict(children=[{"status": "deceased", "descendants": 99}]), dict(ascendants={"grandparents_alive": 9})):
            with self.subTest(bad=bad):
                with self.assertRaises(H.InvalidInput):
                    H.normalize_input(bad)
        n = H.normalize_input(dict(spouse="alive", children=[C(), {"status": "none"}, C(kind="adopted")], extra_name="山田"))
        self.assertEqual(len(n["children"]), 2)
        self.assertNotIn("山田", json.dumps(n, ensure_ascii=False))
        self.assertEqual(set(n), {"spouse", "children", "ascendants", "siblings", "will", "special"})
        # 行数上限
        n = H.normalize_input(dict(children=[C()] * 30))
        self.assertEqual(len(n["children"]), L.MAX_CHILD_ROWS)

    def test_law_table_constants_are_pinned_and_marked(self):
        self.assertEqual(L.SHARES_WITH_SPOUSE[L.LINE_CHILDREN], (Fraction(1, 2), Fraction(1, 2)))
        self.assertEqual(L.SHARES_WITH_SPOUSE[L.LINE_ASCENDANTS], (Fraction(2, 3), Fraction(1, 3)))
        self.assertEqual(L.SHARES_WITH_SPOUSE[L.LINE_SIBLINGS], (Fraction(3, 4), Fraction(1, 4)))
        self.assertEqual(L.HALF_BLOOD_RATIO, Fraction(1, 2))
        self.assertEqual(L.RANK_ORDER, ("children", "ascendants", "siblings"))
        self.assertEqual(L.REPRESENTATION_CAUSES, ("deceased", "disqualified", "disinherited"))
        src = (REPO / "hub" / "souzoku_law_constants.py").read_text(encoding="utf-8")
        self.assertIn("要大野確認", src)
        self.assertEqual(len(H.law_table_version()), 16)
        # 数値（分数）は定数ファイル以外に直書きしない
        eng = (REPO / "hub" / "souzoku_heirs.py").read_text(encoding="utf-8")
        for lit in ("Fraction(1, 2)", "Fraction(2, 3)", "Fraction(3, 4)"):
            self.assertNotIn(lit, eng)


# ── 2. 保存・承認・画面 ───────────────────────────────────────────────────────
class _Db(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="souzoku3_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", S.FLAG_ENV: "1",
            "WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS), "WEBAPP_SESSION_SECRET": "s" * 32})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            async with db.get_async_engine().begin() as c:
                for md in (sl.metadata, S.metadata, H.metadata):
                    await c.run_sync(md.create_all)
        _run(_create())
        data, errors = S.validate(dict(GOOD, dispute="ない"))
        assert not errors, errors
        self.intake_id = _run(S.save(data))

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def url(self, iid=None):
        return f"/app/souzoku/{iid or self.intake_id}/heirs"

    def rows(self):
        return _run(H.versions(self.intake_id))


class TestStoreAndView(_Db):
    FORM = {"spouse": "alive", "child_1_status": "alive", "child_2_status": "deceased", "child_2_descendants": "2", "will": "none"}

    def test_flag_off_404_and_gate(self):
        with patch.dict(os.environ, {S.FLAG_ENV: ""}):
            self.assertEqual(_client.get(self.url(), headers=_auth()).status_code, 404)
            self.assertEqual(_client.post(self.url(), headers=_auth(), data=self.FORM).status_code, 404)
        self.assertEqual(_client.get(self.url(), follow_redirects=False).status_code, 303)      # 未認証
        for r in V.router.routes:
            self.assertTrue(getattr(r.endpoint, "__webapp_gate__", False), r.path)
        self.assertEqual(_client.get(self.url("f" * 32), headers=_auth()).status_code, 404)       # 存在しない案件
        self.assertEqual(_client.get("/app/souzoku/not-hex/heirs", headers=_auth()).status_code, 404)

    def test_save_creates_versions_and_page_shows_review_mark(self):
        r = _client.get(self.url(), headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertIn("まだ判定がありません", r.text)
        self.assertNotIn("山田", r.text)                                   # 受付の氏名は本画面に出さない
        r = _client.post(self.url(), headers=_auth(), data=self.FORM, follow_redirects=False)
        self.assertEqual((r.status_code, r.headers["location"]), (303, self.url() + "?done=saved"))
        rows = self.rows()
        self.assertEqual([(x["version"], x["status"], x["result"]) for x in rows], [(1, "draft", "determined")])
        self.assertEqual([(h["label"], h["share"]) for h in rows[0]["result_detail"]["heirs"]],
                         [("配偶者", "1/2"), ("子1", "1/4"), ("代襲（子2）1", "1/8"), ("代襲（子2）2", "1/8")])
        page = _client.get(self.url() + "?done=saved", headers=_auth()).text
        self.assertIn("弁護士確認前", page)
        self.assertIn("民法900条", page)
        self.assertIn("1/8", page)
        self.assertIn("この版を確定する", page)
        self.assertIn('name="child_2_descendants" min="0" max="20" value="2"', page)   # 入力の復元
        # 2 回目の保存は新版（上書きしない）
        _client.post(self.url(), headers=_auth(), data=dict(self.FORM, spouse="none"))
        rows = self.rows()
        self.assertEqual([x["version"] for x in rows], [1, 2])
        self.assertEqual([h["share"] for h in rows[1]["result_detail"]["heirs"]], ["1/2", "1/4", "1/4"])
        self.assertEqual(_run(H.latest(self.intake_id))["version"], 2)

    def test_confirm_is_explicit_human_action_on_latest_version_only(self):
        _client.post(self.url(), headers=_auth(), data=self.FORM)
        hid = self.rows()[0]["heirs_id"]
        r = _client.post(self.url() + "/confirm", headers=_auth(), data={"heirs_id": hid}, follow_redirects=False)
        self.assertEqual(r.headers["location"], self.url() + "?done=confirm_required")
        self.assertEqual(self.rows()[0]["status"], "draft")
        r = _client.post(self.url() + "/confirm", headers=_auth(), data={"heirs_id": hid, "confirm": "1"}, follow_redirects=False)
        self.assertEqual(r.headers["location"], self.url() + "?done=confirmed")
        row = self.rows()[0]
        self.assertEqual((row["status"], row["confirmed_by"]), ("confirmed", "human"))
        self.assertIsNotNone(row["confirmed_at"])
        self.assertEqual([h["action"] for h in _run(H.history(hid))], ["created", "confirmed"])
        r = _client.post(self.url() + "/confirm", headers=_auth(), data={"heirs_id": hid, "confirm": "1"}, follow_redirects=False)
        self.assertEqual(r.headers["location"], self.url() + "?done=already")                    # 二重操作は副作用なし
        page = _client.get(self.url(), headers=_auth()).text
        self.assertIn("確定済み", page)
        self.assertNotIn("この版を確定する", page)
        self.assertIn("弁護士確認前", page)                                                   # 印は確定後も残る（判定表が要大野確認）
        # 新版を積むと最新版だけが確定できる（古い版の heirs_id は 400）
        _client.post(self.url(), headers=_auth(), data=self.FORM)
        self.assertEqual(_client.post(self.url() + "/confirm", headers=_auth(), data={"heirs_id": hid, "confirm": "1"}).status_code, 400)
        self.assertEqual(_client.post(self.url() + "/confirm", headers=_auth(), data={"heirs_id": "zz", "confirm": "1"}).status_code, 400)

    def test_needs_attorney_and_bad_input(self):
        r = _client.post(self.url(), headers=_auth(), data=dict(self.FORM, fetus="1"), follow_redirects=False)
        self.assertEqual(r.headers["location"], self.url() + "?done=saved")
        self.assertEqual(self.rows()[0]["result"], "needs_attorney")
        page = _client.get(self.url(), headers=_auth()).text
        self.assertIn("要弁護士判断", page)
        self.assertIn("胎児", page)
        r = _client.post(self.url(), headers=_auth(), data=dict(self.FORM, child_1_status="zzz"), follow_redirects=False)
        self.assertEqual(r.headers["location"], self.url() + "?done=bad_input")
        self.assertEqual(len(self.rows()), 1)

    def test_no_names_in_logs_and_no_sends(self):
        import logging

        class _Cap(logging.Handler):
            def __init__(self):
                super().__init__(level=logging.DEBUG)
                self.msgs = []

            def emit(self, record):
                self.msgs.append(record.getMessage())
        cap = _Cap()
        loggers = [logging.getLogger(n) for n in ("hub.souzoku_heirs", "hub.webapp_souzoku_heirs_view")]
        saved = [(lg, lg.level) for lg in loggers]
        for lg in loggers:                                   # 当該 logger に直付け（TEST-FLAKY-1 の流儀・root 非依存）
            lg.addHandler(cap)
            lg.setLevel(logging.DEBUG)
        try:
            _client.post(self.url(), headers=_auth(), data=self.FORM)
            hid = self.rows()[0]["heirs_id"]
            _client.post(self.url() + "/confirm", headers=_auth(), data={"heirs_id": hid, "confirm": "1"})
        finally:
            for lg, lvl in saved:
                lg.removeHandler(cap)
                lg.setLevel(lvl)
        text = "\n".join(cap.msgs)
        self.assertIn("draft saved", text)
        self.assertIn("confirmed", text)
        for leak in ("山田", "やまだ", self.intake_id, hid):
            self.assertNotIn(leak, text)
        self.assertEqual([o for o in _run(_select(sl.send_operation))], [])                    # 送信なし


async def _select(table):
    async with db.session_scope() as s:
        return [dict(r._mapping) for r in (await s.execute(sa.select(table))).fetchall()]


# ── 3. pin ───────────────────────────────────────────────────────────────────
class TestPins(unittest.TestCase):
    def test_migration_env_wiring_and_sink_policy(self):
        p = REPO / "alembic" / "versions" / "20261017_c1d3e5f7a9b2_souzoku_heirs.py"
        src = p.read_text(encoding="utf-8")
        self.assertIn("revision: str = 'c1d3e5f7a9b2'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'b0c1d2e3f4a5'", src)
        self.assertIn("souzoku_heirs_downgrade_refused_data_present", src)
        for name in H.TABLE_NAMES:
            self.assertIn(f'"{name}"', src)
        self.assertIn("souzoku_heirs_metadata", (REPO / "alembic" / "env.py").read_text(encoding="utf-8"))
        msrc = (REPO / "main.py").read_text(encoding="utf-8").splitlines()
        self.assertEqual(sum("include_before_catch_all(app, souzoku_heirs_router)" in l for l in msrc), 1)
        self.assertIn("include_before_catch_all(app, souzoku_heirs_router)", msrc[-1].strip())      # 末尾追記
        for name in ("hub/souzoku_heirs.py", "hub/souzoku_law_constants.py", "hub/webapp_souzoku_heirs_view.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        self.assertNotIn("souzoku_heirs", allow)
        self.assertEqual(H.STATUSES, ("draft", "confirmed"))
        self.assertEqual(H.RESULTS, ("determined", "needs_attorney", "no_heirs"))
        self.assertEqual({c.name for c in H.souzoku_heirs.columns},
                         {"heirs_id", "intake_id", "business", "version", "input_json", "result_json", "result", "status",
                          "law_table_version", "created_at", "confirmed_by", "confirmed_at"})


if __name__ == "__main__":
    unittest.main()
