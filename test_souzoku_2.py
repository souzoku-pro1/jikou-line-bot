"""SOUZOKU-2（QUEUE-2026-10-16-R G-5）: 相続相談の受付（第 1 段階・受付のみ・既定 OFF）

固定する仕様:
- flag SOUZOKU_INTAKE_ENABLED 未設定（既定 OFF）では /souzoku-intake は全メソッド・別名とも 404（存在しないフリ）。既存フローに触れない
  （main.py 末尾の結線 3 行のみ・既存 module の変更なし）。
- ON: GET は 6 問のフォーム。POST は閉集合で検査（固定文言のみ・入力値を画面に戻さない）。争い=ある は S-2 で終了（status=out_of_scope で記録）。
  それ以外は保存＋業務 LINE 通知 1 通（台帳 key souzoku_intake:{intake_id}・氏名なし）＋受付確認 S-1（画面）。
- 相談者への LINE 送信なし（hub/line_channel を import しない）。レート制限。DB 未設定は固定 500（保存しない）。
- 文言は hub/souzoku_intake_texts（要承認・sha pin）。数値・閉集合は hub/souzoku_constants。
- pin: 新 module の sink 違反ゼロ・allowlist 追加ゼロ・migration は e5f8 の次（本 branch で単一チェーン）・env.py に metadata。
"""

import asyncio
import datetime
import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import souzoku_intake_form as F  # noqa: E402
from hub import db  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import souzoku_constants as C  # noqa: E402
from hub import souzoku_intake as S  # noqa: E402
from hub import souzoku_intake_texts as T  # noqa: E402
from hub import approved_texts as AT  # noqa: E402  TEXT-APPROVAL-GATE-1（H-0）
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
ADMIN = "U" + "c" * 32
_LAST = {"nonce": ""}


def _post(data: dict, nonce: str | None = None, **kw):
    """PRE-REVIEW-1 #307-1: 画面が発行する nonce（冪等キー）を付けて POST。省略時は送信ごとに新しい nonce。"""
    import secrets as _secrets
    n = nonce if nonce is not None else _secrets.token_hex(16)
    _LAST["nonce"] = n
    return _client.post(F.PATH, data={**data, "nonce": n}, **kw)


GOOD = {"deceased_name": "山田 太郎", "deceased_kana": "ヤマダ　タロウ", "death_on": "2026-09-01", "heirs_配偶者": "1", "heirs_子": "2",
        "will_status": "なし", "property_types": ["不動産", "預貯金"], "dispute": "ない"}


def _run(coro):
    try:
        return asyncio.run(coro)
    finally:
        db.reset_for_tests()


class TestOffByDefault(unittest.TestCase):
    def test_all_404_when_flag_off(self):
        os.environ.pop(S.FLAG_ENV, None)
        self.assertFalse(S.enabled())
        for m in ("get", "post", "put", "delete"):
            r = getattr(_client, m)(F.PATH)
            self.assertEqual(r.status_code, 404, m)
        self.assertEqual(_client.get(F.PATH + "/").status_code, 404)
        self.assertEqual(_client.get(F.PATH + "/x").status_code, 404)
        self.assertEqual(_post(GOOD).status_code, 404)


class TestPure(unittest.TestCase):
    def test_validate_and_texts(self):
        data, errors = S.validate(GOOD, today=datetime.date(2026, 10, 16))
        self.assertEqual(errors, [])
        self.assertEqual((data["deceased_kana"], data["death_on"], data["heirs_total"], data["property_types"]),
                         ("やまだ たろう", datetime.date(2026, 9, 1), 3, ["不動産", "預貯金"]))
        self.assertEqual(data["heirs"], [{"relation": "配偶者", "count": 1}, {"relation": "子", "count": 2}])
        self.assertEqual(S.status_for(data), S.STATUS_RECEIVED)
        self.assertEqual(S.status_for(dict(data, dispute="ある")), S.STATUS_OUT_OF_SCOPE)
        bad = dict(GOOD, deceased_kana="Yamada", death_on="2030-01-01", heirs_子="99", will_status="多分", property_types=[], dispute="x")
        _d, errors = S.validate(bad, today=datetime.date(2026, 10, 16))
        self.assertEqual(sorted(errors), sorted(["deceased_kana", "death_on", "heirs", "will_status", "property_types", "dispute"]))
        _d, errors = S.validate(dict(GOOD, **{"heirs_配偶者": "0", "heirs_子": ""}), today=datetime.date(2026, 10, 16))
        self.assertIn("heirs", errors)
        self.assertEqual(S.normalize_kana("ヤマダ　タロウ"), "やまだ たろう")
        self.assertEqual(S.normalize_kana("yamada"), "")
        self.assertTrue(T.verify_pin())
        for i, e in T.TEXTS.items():
            self.assertEqual(e["status"], T.STATUS_NEEDS_APPROVAL, i)
            self.assertFalse(re.search(r"[0-9０-９]", e["text"]), i)
        with patch.object(T, "TABLE_SHA256_PIN", "0" * 64):
            self.assertEqual(T.text("S-1"), "")
        txt = S.notice_text(data, S.STATUS_RECEIVED)
        self.assertIn("相続人 3 名", txt)
        self.assertIn("不動産／預貯金", txt)
        for bad_s in ("山田", "やまだ", "2026-09-01"):
            self.assertNotIn(bad_s, txt)
        self.assertEqual(S.ledger_key("abc"), "souzoku_intake:abc")
        self.assertEqual((C.HEIR_RELATIONS[0], C.DISPUTE_OUT_OF_SCOPE), ("配偶者", "ある"))

    def test_rate_limit(self):
        F.reset_rate_limit_for_tests()
        hits = [F.rate_exceeded("k", now=100.0 + i) for i in range(C.INTAKE_RATE_LIMIT + 1)]
        self.assertEqual(hits.count(True), 1)
        self.assertFalse(F.rate_exceeded("k", now=100.0 + C.INTAKE_RATE_WINDOW_SECONDS + 1))
        F.reset_rate_limit_for_tests()


class _Db(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="souzoku_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", S.FLAG_ENV: "1",
                                            "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz-token", "ATTORNEY_LINE_USER_ID": ADMIN,
                                            "LINE_ADMIN_USER_ID": ADMIN})
        self._env.start()
        db.reset_for_tests()
        F.reset_rate_limit_for_tests()

        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(sl.metadata.create_all)
                await c.run_sync(S.metadata.create_all)
        _run(_create())
        self.push = AsyncMock(return_value=True)
        # PRE-REVIEW-1 #307-2: 文言が要承認のままだと受付画面は 503 になる（test_not_approved_texts_block_intake）。
        # 他のテストは承認済み扱いで本体の挙動を検査する
        # TEXT-APPROVAL-GATE-1（H-0）: 表示は承認台帳（SHA 一致）だけが決める。「承認済み扱い」のテストは台帳にも S-1〜S-3 を
        # 載せる（test_unapproved_texts_render_safe_text_and_notify_once が台帳なし＝安全文の経路を検査する）
        approved = {k: AT.sha256_text(T.text(k)) for k in ("S-1", "S-2", "S-3")}
        self._p = [patch.object(S, "push_line_message", self.push), patch.object(T, "all_approved", lambda: True),
                   patch.dict(AT.APPROVED_SHA256, approved)]
        for p in self._p:
            p.start()
        AT.reset_pending_for_tests()

    def tearDown(self):
        for p in self._p:
            p.stop()
        db.reset_for_tests()
        F.reset_rate_limit_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def rows(self):
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(S.souzoku_intake))).fetchall()]
        return _run(_q())

    def ops(self):
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(sl.send_operation))).fetchall()]
        return _run(_q())


class TestIntakeFlow(_Db):
    def test_get_form_and_submit_receives_once_with_notice(self):
        r = _client.get(F.PATH)
        self.assertEqual(r.status_code, 200)
        for q in ("お名前", "亡くなられた日", "続柄と人数", "遺言の有無", "財産の種類", "争い"):
            self.assertIn(q, r.text)
        r = _post(GOOD)
        self.assertEqual(r.status_code, 200)
        self.assertIn(T.text("S-1"), r.text)
        self.assertNotIn("山田", r.text)                                   # 非反射
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual((row["status"], row["notify_state"], row["heirs_total"], row["property_types"], row["dispute"], row["deceased_kana"]),
                         (S.STATUS_RECEIVED, S.NOTIFY_SENT, 3, "不動産,預貯金", "ない", "やまだ たろう"))
        self.assertEqual(self.push.await_count, 1)
        to, text = self.push.await_args.args[0], self.push.await_args.args[1]
        self.assertEqual(to, ADMIN)
        self.assertTrue(text.startswith(S.NOTICE_HEADER))
        self.assertNotIn("山田", text)
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["business"], ops[0]["channel"], ops[0]["purpose"], ops[0]["state"], ops[0]["inbound_event_id"]),
                         ("souzoku", "souzoku_notice", sl.PURPOSE_OTHER, sl.STATE_SENT, f"souzoku_intake:{_LAST['nonce']}"))
        self.assertEqual(row["inbound_event_id"], f"souzoku_intake:{_LAST['nonce']}")   # PRE-REVIEW-1 #307-1: 冪等キー＝nonce

    def test_same_nonce_resubmit_does_not_duplicate_and_post_redirects(self):
        """PRE-REVIEW-1 #307-1: PRG（303）＋同じ nonce の再送（F5・戻る→再送信・二重クリック）は行も通知も増えない。"""
        r = _post(GOOD, follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], f"{F.PATH}?done={S.STATUS_RECEIVED}&d=n")
        self.assertNotIn("山田", r.headers["location"])
        page = _client.get(r.headers["location"])
        self.assertEqual(page.status_code, 200)
        self.assertIn(T.text("S-1"), page.text)
        nonce = _LAST["nonce"]
        r2 = _post(GOOD, nonce=nonce, follow_redirects=False)
        self.assertEqual((r2.status_code, r2.headers["location"]), (303, r.headers["location"]))
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(len(self.ops()), 1)
        # 入力を変えても同じ nonce なら 2 件目にならない（既存行の結果へ）
        r3 = _post(dict(GOOD, dispute="ある"), nonce=nonce, follow_redirects=False)
        self.assertEqual(r3.status_code, 303)
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.rows()[0]["status"], S.STATUS_RECEIVED)
        # nonce なし／不正は保存しない（固定文）
        r4 = _client.post(F.PATH, data=GOOD)
        self.assertEqual(r4.status_code, 400)
        self.assertIn(F._ERROR_LABELS["nonce"], r4.text)
        self.assertEqual(_post(GOOD, nonce="zz").status_code, 400)
        self.assertEqual(len(self.rows()), 1)
        # 結果画面は status と区分だけ（不正値は素のフォーム）
        self.assertIn("送信する", _client.get(F.PATH + "?done=bogus").text)
        self.assertIn('name="nonce"', _client.get(F.PATH).text)

    def test_not_approved_texts_block_intake(self):
        """PRE-REVIEW-1 #307-2: flag ON でも要承認の文言が残る間は 503 固定文（保存も通知もしない）。"""
        with patch.object(T, "all_approved", lambda: False):
            self.assertEqual(_client.get(F.PATH).status_code, 503)
            r = _post(GOOD)
        self.assertEqual(r.status_code, 503)
        self.assertIn("準備中", r.text)
        self.assertEqual(self.rows(), [])
        self.assertEqual(self.push.await_count, 0)
        # 現状の表（S-1〜S-3 needs_approval）では本物の all_approved は False（setUp の patch 外で評価）
        self.assertFalse(all(e["status"] == T.STATUS_APPROVED for e in T.TEXTS.values()))
        self.assertIn("def all_approved", (REPO / "hub" / "souzoku_intake_texts.py").read_text(encoding="utf-8"))

    def test_dispute_ends_with_out_of_scope_text_and_record(self):
        r = _post(dict(GOOD, dispute="ある"))
        self.assertEqual(r.status_code, 200)
        self.assertIn(T.text("S-2"), r.text)
        self.assertNotIn(T.text("S-1"), r.text)
        row = self.rows()[0]
        self.assertEqual(row["status"], S.STATUS_OUT_OF_SCOPE)
        self.assertIn("対象外（争いあり）", self.push.await_args.args[1])
        r = _post(dict(GOOD, dispute="わからない"))
        self.assertIn(T.text("S-3"), r.text)

    def test_unapproved_texts_render_safe_text_and_notify_once(self):
        # TEXT-APPROVAL-GATE-1（H-0）: 台帳に S-1 が無い（未承認）→ 結果画面は安全文・未承認本文は出ない・
        # 業務 LINE へ「未承認文言の参照 key=S-1」が 1 日 1 回（台帳 key text_unapproved:{JST 日付}・2 回目は DUPLICATE）
        notify = AsyncMock(return_value=True)
        with patch.dict(AT.APPROVED_SHA256, {AT.SAFE_TEXT_KEY: AT.APPROVED_SHA256[AT.SAFE_TEXT_KEY]}, clear=True),                 patch("hub.notify.notify_admin_line", notify):
            r = _client.get(F.PATH + f"?done={S.STATUS_RECEIVED}&d=n")
            self.assertEqual(r.status_code, 200)
            self.assertIn(AT.SAFE_TEXT, r.text)
            self.assertNotIn(T.text("S-1"), r.text)
            r2 = _client.get(F.PATH + f"?done={S.STATUS_RECEIVED}&d=n")
            self.assertIn(AT.SAFE_TEXT, r2.text)
        notify.assert_awaited_once()
        self.assertIn("key=S-1", notify.await_args.args[0])
        self.assertNotIn(T.text("S-1"), notify.await_args.args[0])
        self.assertEqual(len([o for o in self.ops() if o["channel"] == AT.LEDGER_CHANNEL]), 1)

    def test_validation_errors_are_fixed_text_and_not_saved(self):
        r = _post(dict(GOOD, deceased_kana="Yamada<script>", death_on="2030-01-01"))
        self.assertEqual(r.status_code, 400)
        self.assertIn(F._ERROR_LABELS["deceased_kana"], r.text)
        self.assertIn(F._ERROR_LABELS["death_on"], r.text)
        self.assertNotIn("Yamada", r.text)
        self.assertEqual(self.rows(), [])
        self.assertEqual(self.push.await_count, 0)
        r = _client.post(F.PATH, content=b"x" * (C.INTAKE_MAX_BODY_BYTES + 1), headers={"content-type": "application/x-www-form-urlencoded"})
        self.assertEqual(r.status_code, 400)
        r = _client.post(F.PATH, json={"a": 1})
        self.assertEqual(r.status_code, 400)

    def test_rate_limit_and_db_unset(self):
        for _ in range(C.INTAKE_RATE_LIMIT):
            self.assertEqual(_post(GOOD, headers={"X-Real-IP": "9.9.9.9"}).status_code, 200)
        self.assertEqual(_post(GOOD, headers={"X-Real-IP": "9.9.9.9"}).status_code, 429)
        self.assertEqual(len(self.rows()), C.INTAKE_RATE_LIMIT)
        F.reset_rate_limit_for_tests()
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            db.reset_for_tests()
            r = _post(GOOD)
        self.assertEqual(r.status_code, 500)
        self.assertIn("エラーが発生しました", r.text)

    def test_notice_failure_is_recorded_not_raised(self):
        self.push.return_value = False
        r = _post(GOOD)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.rows()[0]["notify_state"], S.NOTIFY_FAILED)
        self.push.return_value = True
        with patch.dict(os.environ, {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": ""}):
            _post(GOOD)
        self.assertEqual(self.rows()[-1]["notify_state"], S.NOTIFY_FAILED)        # token 未設定: 台帳行を作らず failed


class TestPins(unittest.TestCase):
    def test_sink_allowlist_wiring_migration(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/souzoku_intake.py", "hub/souzoku_intake_texts.py", "hub/souzoku_constants.py", "souzoku_intake_form.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
            self.assertNotIn(name.split("/")[-1][:-3], allow)
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        # SOUZOKU-3（H-4）で末尾に heirs 画面の結線が 4 行追記されたため、EOF 窓 3 行→見出し基準（SOUZOKU-2 ブロック以降）に変更
        start = next(i for i, l in enumerate(src) if "SOUZOKU-2（QUEUE-2026-10-16-R G-5）" in l)
        self.assertIn("app.include_router(souzoku_intake_router)", "\n".join(src[start:]))
        for name in ("hub/souzoku_intake.py", "souzoku_intake_form.py"):
            body = (REPO / name).read_text(encoding="utf-8").split('"""', 2)[2]
            for bad in ("line_channel", "push_text(", "reply_with_push_fallback", "kintone"):
                self.assertNotIn(bad, body, name)
        p = REPO / "alembic" / "versions" / "20261016_b0c1d2e3f4a5_souzoku_intake.py"
        msrc = p.read_text(encoding="utf-8")
        self.assertIn("revision: str = 'b0c1d2e3f4a5'", msrc)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'e5f8a1b4c7d9'", msrc)
        self.assertIn("souzoku_intake_downgrade_refused_data_present", msrc)
        self.assertIn("souzoku_intake_metadata", (REPO / "alembic" / "env.py").read_text(encoding="utf-8"))
        self.assertEqual(S.FLAG_ENV, "SOUZOKU_INTAKE_ENABLED")
        self.assertIn("要大野確認", (REPO / "hub" / "souzoku_constants.py").read_text(encoding="utf-8"))
        # 公開 route は /app 配下に無い（webapp の PUBLIC_ROUTES pin を変えない）
        self.assertFalse(F.PATH.startswith("/app"))


if __name__ == "__main__":
    unittest.main()
