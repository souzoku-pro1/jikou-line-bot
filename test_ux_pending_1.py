"""UX-PENDING-1: 対応待ち運用を「通知タップ → 1 画面 → 返信」に一本化 — テスト

設計: docs/ux_pending_1_design.md。票のテスト項目:
  1. 通知 URL の署名・期限・改ざん拒否（種別すり替え・1 文字改変・鍵未設定）
  2. 統合一覧の 3 操作（返信する／対応終了／不要）＋承認キューだけの件（旧）の閉じ・戻す
  3. 送信ボタンの表示（文字「送信」・色の明示・チェック欄なし）
  4. サーバー側自動確認の停止ケース（停止リスト登録あり／照会不能／対応モード照会不能）と通すケース（全体停止・人対応中・対応中）
  5. 1 タップ対応終了の冪等性（GET は副作用なし・2 回目の POST は処理しない）
  追加: ログイン後の着地先 next（閉集合）・経過通知 v2（承認まで v1）・migration approval_close・メニュー・試験アカウント allowlist
規律: 承認なし送信経路ゼロ（署名リンクは「閉じる」だけ）・PII なし・物理削除なし。
ハーネスは test_jikou_reply_q1c の _DbMixin（sqlite 台帳＋LINE/kintone/業務通知のフェイク）を再利用する。
"""

import hashlib
import os
import re
import shutil
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import (USER, USER2, V2_ON, _DbMixin, _ENV, _FakeClient, _auth, _client, _rec,  # noqa: E402
                                  _run)

import main  # noqa: E402
from hub import approval_close as ac  # noqa: E402
from hub import approved_texts as at  # noqa: E402
from hub import autoreply_stoplist  # noqa: E402
from hub import kintone  # noqa: E402
from hub import pending_link as pl  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402
from hub import trial_account  # noqa: E402
from hub import webapp_auth  # noqa: E402
from hub import webapp_inbox_view as iv  # noqa: E402
from hub import webapp_reply_view as rv  # noqa: E402
from hub import db  # noqa: E402

REPO = Path(__file__).parent
DOMAIN = {"RAILWAY_PUBLIC_DOMAIN": "app.example.test"}
REC_ID = "500"
OLD_ID = "777"


def _approval(rec_id=REC_ID, user=USER, **over):
    base = {"$id": rec_id, "line_user_id": user, "顧客名": "試験 太郎", "顧客メッセージ": "督促が来ました。電話は090-1234-5678です",
            "AI下書き": "下書き本文", "カテゴリ": "費用交渉・減額相談", "送信済み": "no", "更新日時": "2026-10-01T01:00:00Z"}
    base.update(over)
    return _rec(**base)


class _InboxMixin(_DbMixin):
    """q1c の DB/フェイクに、approval_close 表・承認キュー（App 29）の検索/取得フェイク・公開ドメインを足す。"""

    def setUp(self):
        super().setUp()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(ac.metadata.create_all)
        _run(_create())
        self.records = {REC_ID: _approval()}
        self.old_records = []

        async def _search(app, query, fields=None):
            if "$id in (" in query:
                ids = re.findall(r'"([0-9]+)"', query)
                return [self.records[i] for i in ids if i in self.records]
            if "送信済み in" in query:
                return list(self.old_records)
            return []

        async def _get(app, record_id):
            if str(record_id) in self.records:
                return self.records[str(record_id)]
            raise kintone.KintoneError("not found")
        self.form_fields = AsyncMock(return_value={"role": {"options": {"user": {}, "assistant": {}, "human": {}}}})
        self._extra_patches = [
            patch.dict(os.environ, {**DOMAIN, "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d"}),
            patch.object(kintone, "search_records", _search),
            patch.object(kintone, "get_record", _get),
            patch.object(kintone, "get_form_fields", self.form_fields),
            patch.object(kintone, "create_record", AsyncMock(return_value="900")),
        ]
        for p in self._extra_patches:
            p.start()

    def _reg(self, user=USER, **kw):
        base = dict(business="jikou", channel="jikou", line_user_hash=sl.line_user_hash("jikou", user),
                    route="customer", reason="demoted", category="費用交渉・減額相談", approval_record_id=REC_ID)
        base.update(kw)
        return _run(rp.register(**base))

    def get(self, path):
        return _client.get(path, headers=_auth(), follow_redirects=False)

    def post(self, path, data):
        return _client.post(path, headers=_auth(), data=data, follow_redirects=False)


# ── 1. 署名付きリンク ──────────────────────────────────────────────────────────
class TestPendingLink(unittest.TestCase):
    def setUp(self):
        self._env = patch.dict(os.environ, {**_ENV, **DOMAIN})
        self._env.start()

    def tearDown(self):
        self._env.stop()

    def test_token_round_trip_and_urls_have_no_pii(self):
        now = 1_800_000_000
        for kind in pl.KINDS:
            tok = pl.make_token(kind, 42, now=now)
            self.assertRegex(tok, r"^[rc]\.42\.[0-9]+\.[0-9a-f]{32}$")
            self.assertEqual(pl.verify(tok, now=now), ("ok", kind, 42))
            self.assertEqual(pl.verify(tok, now=now + pl.TTL_SECONDS - 1), ("ok", kind, 42))
            self.assertEqual(pl.verify(tok, now=now + pl.TTL_SECONDS), ("expired", kind, 42))      # 72 時間で失効
        self.assertEqual(pl.reply_url(42, now=now), f"https://app.example.test/app/go/r/{pl.make_token('r', 42, now=now)}")
        self.assertEqual(pl.close_url(42, now=now), f"https://app.example.test/app/go/c/{pl.make_token('c', 42, now=now)}")
        self.assertEqual(pl.inbox_url(), "https://app.example.test/app/inbox")
        self.assertEqual(pl.TTL_SECONDS, 72 * 3600)
        self.assertNotIn(USER, pl.reply_url(42))

    def test_tamper_and_kind_swap_and_malformed_are_invalid(self):
        now = 1_800_000_000
        tok = pl.make_token("r", 42, now=now)
        head, sig = tok.rsplit(".", 1)
        flipped = ("0" if sig[0] != "0" else "1") + sig[1:]
        self.assertEqual(pl.verify(f"{head}.{flipped}", now=now), ("invalid", None, None))
        self.assertEqual(pl.verify("c" + tok[1:], now=now), ("invalid", None, None))               # 種別すり替え
        kind, pid, exp, _ = tok.split(".")
        self.assertEqual(pl.verify(f"{kind}.43.{exp}.{sig}", now=now), ("invalid", None, None))    # 件のすり替え
        self.assertEqual(pl.verify(f"{kind}.{pid}.{int(exp) + 1}.{sig}", now=now), ("invalid", None, None))   # 期限の改ざん
        for bad in ("", "r.42", "x.42.1.aa", tok + "0", tok.upper(), "r.-1.1." + "a" * 32, "r.42.1." + "g" * 32):
            self.assertEqual(pl.verify(bad, now=now), ("invalid", None, None), bad)
        self.assertIsNone(pl.make_token("x", 42))
        self.assertIsNone(pl.make_token("r", 0))

    def test_no_secret_or_no_domain_yields_no_url(self):
        with patch.dict(os.environ, {"WEBAPP_SESSION_SECRET": "short"}):
            self.assertIsNone(pl.make_token("r", 1))
            self.assertEqual(pl.verify("r.1.9999999999." + "a" * 32), ("invalid", None, None))
            self.assertEqual(pl.reply_url(1), "")
        with patch.dict(os.environ, {"RAILWAY_PUBLIC_DOMAIN": ""}):
            self.assertEqual((pl.reply_url(1), pl.close_url(1), pl.inbox_url()), ("", "", ""))
        with patch.dict(os.environ, {"RAILWAY_PUBLIC_DOMAIN": "evil.test/path"}):
            self.assertEqual(pl.inbox_url(), "")


# ── 通知文（【対応待ち】・経過通知 v2） ─────────────────────────────────────────────
class TestNoticeTexts(unittest.TestCase):
    def setUp(self):
        self._env = patch.dict(os.environ, {**_ENV, **DOMAIN})
        self._env.start()

    def tearDown(self):
        self._env.stop()

    def test_pending_notice_has_two_links_and_no_pii(self):
        text = sp.notice_text(REC_ID, "費用交渉・減額相談", pending_id=7)
        lines = text.split("\n")
        self.assertEqual(lines[0], sp.NOTICE_HEADER)
        self.assertEqual(lines[1], "承認キュー No.500・カテゴリ: 費用交渉・減額相談")
        self.assertTrue(lines[2].startswith(sp.NOTICE_REPLY_LINK + "https://app.example.test/app/go/r/r.7."), lines[2])
        self.assertTrue(lines[3].startswith(sp.NOTICE_CLOSE_LINK + "https://app.example.test/app/go/c/c.7."), lines[3])
        self.assertEqual(lines[4], sp.NOTICE_FOOTER_LINKED)
        self.assertEqual(len(lines), 5)
        self.assertNotIn(USER, text)
        # 承認キューが無い件は返信画面を開けないため一覧へ
        text = sp.notice_text(None, None, pending_id=7)
        self.assertIn(sp.NOTICE_LIST_LINK + "https://app.example.test/app/inbox", text)
        self.assertIn("/app/go/c/c.7.", text)

    def test_pending_notice_falls_back_to_legacy_without_url(self):
        legacy = f"{sp.NOTICE_HEADER}\n承認キュー No.500・カテゴリ: x\n{sp.NOTICE_FOOTER}"
        self.assertEqual(sp.notice_text(REC_ID, "x"), legacy)                     # pending_id 無し（従来の呼び方）
        self.assertEqual(sp.notice_text(REC_ID, "x", pending_id=None), legacy)
        self.assertEqual(sp.notice_text(REC_ID, "x", pending_id="abc"), legacy)
        with patch.dict(os.environ, {"RAILWAY_PUBLIC_DOMAIN": ""}):
            self.assertEqual(sp.notice_text(REC_ID, "x", pending_id=7), legacy)
        with patch.dict(os.environ, {"WEBAPP_SESSION_SECRET": "short"}):
            self.assertEqual(sp.notice_text(REC_ID, "x", pending_id=7), legacy)

    def test_age_notice_uses_v1_until_v2_is_approved(self):
        stats = {"open_count": 3, "over_count": 2, "oldest_minutes": 90}
        v1 = sp.age_notice_text(stats, 1, 60)
        self.assertTrue(v1.endswith("/app/pending からご確認ください。"), v1)     # v2 未承認 → 現行 v1
        self.assertNotIn("ops_age_notice_v2", at.APPROVED_SHA256)                  # 承認は大野（push 前）・本票では台帳に足さない
        sha_v2 = hashlib.sha256(sp.AGE_NOTICE_TEMPLATE_V2.encode("utf-8")).hexdigest()
        self.assertEqual(sha_v2, "b91070d861198b77c39af30b69e38fa8fef46b28c7fd8a1971813a5f8a78d5c1")   # 承認文書と同じ値
        with patch.dict(at.APPROVED_SHA256, {sp.AGE_NOTICE_TEXT_KEY_V2: sha_v2}):
            v2 = sp.age_notice_text(stats, 1, 60)
            self.assertEqual(v2, "【対応待ち・未返信】\n未返信の対応待ち 3 件（60 分超 2 件・最長 90 分経過）・再処理待ち 1 件\n"
                                 "https://app.example.test/app/inbox からご確認ください。")
            self.assertNotIn("{{", v2)
            with patch.dict(os.environ, {"RAILWAY_PUBLIC_DOMAIN": ""}):
                self.assertEqual(sp.age_notice_text(stats, 1, 60), v1)              # 公開ホストが無ければ v1
        # v2 の差分は末尾 1 行だけ（数値の差し込み位置は同じ）
        self.assertEqual(sp.AGE_NOTICE_TEMPLATE.rsplit("\n", 1)[0], sp.AGE_NOTICE_TEMPLATE_V2.rsplit("\n", 1)[0])


# ── 2. 統合一覧（3 操作・旧項目・戻す） ─────────────────────────────────────────
class TestInbox(_InboxMixin):
    def test_unauthenticated_is_redirected_with_next_only_for_allowed_paths(self):
        r = _client.get("/app/inbox", follow_redirects=False)
        self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/login?next=/app/inbox"))
        tok = pl.make_token("c", 1)
        r = _client.get(f"/app/go/c/{tok}", follow_redirects=False)
        self.assertEqual(r.headers["location"], f"/app/login?next=/app/go/c/{tok}")
        r = _client.post("/app/inbox/close", data={"kind": "pending", "id": "1", "outcome": "not_needed"}, follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/login")                      # 操作先は引き継がない
        self.assertEqual(_client.get("/app/approvals", follow_redirects=False).headers["location"], "/app/login")

    def test_login_lands_on_safe_next_only(self):
        tok = pl.make_token("r", 3)
        for nxt, want in (("/app/inbox", "/app/inbox"), (f"/app/reply/{REC_ID}", f"/app/reply/{REC_ID}"),
                          (f"/app/go/r/{tok}", f"/app/go/r/{tok}"),
                          ("//evil.test/app/inbox", "/app"), ("https://evil.test/", "/app"), ("/app/x", "/app"),
                          ("/app/inbox?closed=1", "/app"), ("/app/inbox\n/x", "/app"), ("/app/reply/abc", "/app"), ("", "/app")):
            r = _client.post("/app/login?next=" + nxt.replace("\n", "%0A"), data={"password": "pw"}, follow_redirects=False)
            self.assertEqual((r.status_code, r.headers["location"]), (303, want), nxt)
        self.assertIsNone(webapp_auth.safe_next("/app/inbox/close"))
        self.assertIsNone(webapp_auth.safe_next("/app/go/c/x"))

    def test_list_shows_pending_and_old_items_with_three_buttons_and_no_pii(self):
        pid = self._reg(inbound_event_id="ev1")
        _run(sl.set_attending("jikou", USER))
        self.old_records = [_approval(OLD_ID, USER2, 顧客名="旧 花子", 顧客メッセージ="旧い相談です", 更新日時="2026-09-01T01:00:00Z"),
                            _approval(REC_ID)]     # 対応待ちが参照する No.500 は「旧」として重複表示しない
        r = self.get("/app/inbox")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers["cache-control"], "no-store, private")
        html = r.text
        self.assertIn('<meta name="color-scheme" content="light">', html)
        self.assertIn("試験 太郎", html)
        self.assertIn("督促が来ました", html)
        self.assertIn("bot 停止中（対応中）", html)
        self.assertIn("bot が判断できず下書きだけ作成", html)
        self.assertIn(f'href="/app/reply/{REC_ID}">返信する</a>', html)
        self.assertIn('<span class="badge old">旧</span>', html)
        self.assertIn("旧 花子", html)
        self.assertEqual(html.count('class="badge old"'), 1)
        self.assertEqual(html.count(f'href="/app/reply/{REC_ID}"'), 1)
        self.assertEqual(html.count('value="done_external">対応終了</button>'), 2)
        self.assertEqual(html.count('value="not_needed">不要</button>'), 2)
        self.assertEqual(html.count('<button'), 4)                               # 件ごとに 2（返信する は a 要素）
        self.assertNotIn(USER, html)
        self.assertNotIn(USER2, html)
        self.assertNotIn(sl.line_user_hash("jikou", USER), html)
        self.assertLess(html.index("旧 花子"), html.index("試験 太郎"))             # 古い順（旧項目 9/1 が先）
        self.assertIn('href="/app/inbox?closed=1">閉じた件も表示する', html)
        # 承認キューが無い対応待ちは「返信する」の代わりに案内
        self._reg(user=USER2, inbound_event_id="ev2", approval_record_id=None)
        html = self.get("/app/inbox").text
        self.assertIn("承認キューが無い件です", html)
        self.assertEqual(html.count("返信する</a>"), 2)                       # No.500 と旧 No.777 だけ（承認キュー無しの件には無い）
        self.assertEqual(pid, 1)

    def test_close_done_external_resumes_bot_and_keeps_history(self):
        pid = self._reg(inbound_event_id="ev1")
        _run(sl.set_attending("jikou", USER))
        r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "done_external"})
        self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/inbox?done=closed"))
        row = self.pending()[0]
        self.assertEqual((row["state"], row["resolved_by"]), ("closed_external", "human"))
        self.assertFalse(_run(sl.conversation_state("jikou", USER))["attending"])
        hist = _run(rp.history(pid))
        self.assertEqual([(h["from"], h["to"], h["actor"]) for h in hist][-1], ("open", "closed_external", "human:inbox"))
        self.assertNotIn(f'name="id" value="{pid}"', self.get("/app/inbox").text)   # 未処理から消える
        closed = self.get("/app/inbox?closed=1").text
        self.assertIn("対応終了（別の方法で返信済み）", closed)
        self.assertIn("未処理に戻す", closed)
        # 冪等: もう一度閉じても状態・履歴は変わらない
        r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=already")
        self.assertEqual(len(_run(rp.history(pid))), len(hist))
        # 戻す → open・履歴に残る（物理削除なし）
        r = self.post("/app/inbox/reopen", {"kind": "pending", "id": str(pid)})
        self.assertEqual(r.headers["location"], "/app/inbox?closed=1&done=reopened")
        self.assertEqual(self.pending()[0]["state"], "open")
        self.assertEqual(_run(rp.history(pid))[-1]["to"], "open")
        self.assertEqual(len(_run(rp.history(pid))), len(hist) + 1)
        r = self.post("/app/inbox/reopen", {"kind": "pending", "id": str(pid)})
        self.assertEqual(r.headers["location"], "/app/inbox?closed=1&done=not_closed")
        self.assertEqual(self.post("/app/inbox/reopen", {"kind": "pending", "id": "999"}).headers["location"],
                         "/app/inbox?closed=1&done=not_found")

    def test_not_needed_does_not_resume_bot(self):
        pid = self._reg(inbound_event_id="ev1")
        _run(sl.set_attending("jikou", USER))
        r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "not_needed"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=not_needed")
        self.assertEqual(self.pending()[0]["state"], "not_needed")
        self.assertTrue(_run(sl.conversation_state("jikou", USER))["attending"])
        self.assertEqual(_run(rp.reopen(pid, actor="t")), "ok")
        self.assertEqual(_run(rp.reopen(pid, actor="t")), "not_closed")
        self.assertEqual(_run(rp.mark_closed_external(999, actor="t")), "not_found")

    def test_old_item_close_is_appended_only_and_hides_from_list(self):
        self.old_records = [_approval(OLD_ID, USER2, 顧客名="旧 花子")]
        self.records[OLD_ID] = self.old_records[0]
        _run(sl.set_attending("jikou", USER2))
        r = self.post("/app/inbox/close", {"kind": "old", "id": OLD_ID, "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=closed")
        self.assertNotIn("旧 花子", self.get("/app/inbox").text)
        self.assertFalse(_run(sl.conversation_state("jikou", USER2))["attending"])   # レコードの宛先をサーバー内で解決して bot 再開
        rows = self._rows(ac.approval_close)
        self.assertEqual([(x["approval_record_id"], x["outcome"], x["actor"]) for x in rows], [(OLD_ID, "done_external", "human:inbox")])
        r = self.post("/app/inbox/close", {"kind": "old", "id": OLD_ID, "outcome": "not_needed"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=already")
        self.assertEqual(len(self._rows(ac.approval_close)), 1)
        closed = self.get("/app/inbox?closed=1").text
        self.assertIn("旧 花子", closed)
        self.assertIn('<span class="badge old">旧</span>', closed)
        self.assertIn(f'name="id" value="{OLD_ID}"', closed)
        r = self.post("/app/inbox/reopen", {"kind": "old", "id": OLD_ID})
        self.assertEqual(r.headers["location"], "/app/inbox?closed=1&done=reopened")
        self.assertEqual([x["outcome"] for x in self._rows(ac.approval_close)], ["done_external", "reopened"])   # 追記のみ
        self.assertIn("旧 花子", self.get("/app/inbox").text)
        r = self.post("/app/inbox/close", {"kind": "old", "id": OLD_ID, "outcome": "not_needed"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=not_needed")
        self.assertEqual(len(self._rows(ac.approval_close)), 3)
        # kintone 書込 API は呼ばない（AST 検査）
        src = (REPO / "hub" / "webapp_inbox_view.py").read_text(encoding="utf-8")
        for banned in ("update_record", "create_record", "delete_record", "post_to_kintone"):
            self.assertNotIn(banned, src)
        self.assertNotIn("update_record", (REPO / "hub" / "approval_close.py").read_text(encoding="utf-8"))

    def test_bad_inputs_are_fixed_400(self):
        for data in ({"kind": "pending", "id": "x", "outcome": "not_needed"}, {"kind": "zz", "id": "1", "outcome": "not_needed"},
                     {"kind": "old", "id": "1", "outcome": "delete"}, {"kind": "old", "id": "1"}, {}):
            r = self.post("/app/inbox/close", data)
            self.assertEqual((r.status_code, r.text), (400, ""), data)
        for data in ({"kind": "pending", "id": "x"}, {"kind": "zz", "id": "1"}, {}):
            self.assertEqual(self.post("/app/inbox/reopen", data).status_code, 400, data)

    def test_old_pending_url_redirects_to_inbox(self):
        r = self.get("/app/pending")
        self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/inbox"))
        self.assertEqual(self.post("/app/pending/resolve", {"pending_id": "1", "outcome": "not_needed"}).status_code, 405)

    def test_db_failure_shows_fixed_notice(self):
        with patch.object(iv.reply_pending, "list_open", AsyncMock(side_effect=RuntimeError("db"))):
            r = self.get("/app/inbox")
        self.assertEqual(r.status_code, 200)
        self.assertIn("記録を読み書きできませんでした", r.text)


# ── 5. 署名付きリンクの着地と 1 タップ対応終了（冪等） ─────────────────────────────
class TestGoLinks(_InboxMixin):
    def test_reply_link_lands_on_reply_page_or_inbox(self):
        pid = self._reg(inbound_event_id="ev1")
        r = self.get(f"/app/go/r/{pl.make_token('r', pid)}")
        self.assertEqual((r.status_code, r.headers["location"]), (303, f"/app/reply/{REC_ID}"))
        pid2 = self._reg(user=USER2, inbound_event_id="ev2", approval_record_id=None)
        r = self.get(f"/app/go/r/{pl.make_token('r', pid2)}")
        self.assertEqual(r.headers["location"], "/app/inbox?link=noreply")
        self.assertEqual(self.get(f"/app/go/r/{pl.make_token('r', 999)}").headers["location"], "/app/inbox?done=not_found")
        expired = pl.make_token("r", pid, now=int(time.time()) - pl.TTL_SECONDS - 1)
        r = self.get(f"/app/go/r/{expired}")
        self.assertEqual(r.headers["location"], "/app/inbox?link=expired")       # 失効はエラー画面で終わらせず一覧へ
        self.assertIn("リンクの期限が切れています", self.get("/app/inbox?link=expired").text)
        good = pl.make_token("r", pid)
        tampered = good[:-1] + ("0" if good[-1] != "0" else "1")
        self.assertEqual(self.get(f"/app/go/r/{tampered}").headers["location"], "/app/inbox?link=invalid")
        self.assertEqual(self.get(f"/app/go/r/{pl.make_token('c', pid)}").headers["location"], "/app/inbox?link=invalid")   # 種別違い
        self.assertEqual(self.get("/app/go/r/not-a-token").headers["location"], "/app/inbox?link=invalid")
        self.assertEqual(self.pending()[0]["state"], "open")                      # 開くだけ＝状態は変わらない

    def test_close_link_confirm_then_post_is_idempotent(self):
        pid = self._reg(inbound_event_id="ev1")
        _run(sl.set_attending("jikou", USER))
        tok = pl.make_token("c", pid)
        r = self.get(f"/app/go/c/{tok}")
        self.assertEqual(r.status_code, 200)
        self.assertIn("対応終了にしますか？", r.text)
        self.assertIn("顧客への送信はしません", r.text)
        self.assertIn(f'action="/app/go/c/{tok}"', r.text)
        self.assertIn(">対応終了にする</button>", r.text)
        self.assertIn('href="/app/inbox">やめる', r.text)
        self.assertNotIn(USER, r.text)
        self.assertEqual(self.pending()[0]["state"], "open")                      # GET は副作用なし
        self.assertTrue(_run(sl.conversation_state("jikou", USER))["attending"])
        r = self.post(f"/app/go/c/{tok}", {})
        self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/inbox?done=closed"))
        row = self.pending()[0]
        self.assertEqual(row["state"], "closed_external")
        self.assertFalse(_run(sl.conversation_state("jikou", USER))["attending"])
        hist = _run(rp.history(pid))
        self.assertEqual(hist[-1]["actor"], "human:link")
        # 2 回目（戻る→再送・二重タップ）: 処理せず案内だけ
        r = self.post(f"/app/go/c/{tok}", {})
        self.assertEqual(r.headers["location"], "/app/inbox?done=already")
        self.assertEqual(_run(rp.history(pid)), hist)
        self.assertEqual(self.get(f"/app/go/c/{tok}").headers["location"], "/app/inbox?done=already")
        self.assertEqual(len(_FakeClient.calls), 0)                              # 顧客への LINE 送信は一切無い
        self.biz.assert_not_awaited()

    def test_close_link_rejects_expired_tampered_and_wrong_kind_without_side_effects(self):
        pid = self._reg(inbound_event_id="ev1")
        expired = pl.make_token("c", pid, now=int(time.time()) - pl.TTL_SECONDS - 1)
        self.assertEqual(self.post(f"/app/go/c/{expired}", {}).headers["location"], "/app/inbox?link=expired")
        self.assertEqual(self.get(f"/app/go/c/{expired}").headers["location"], "/app/inbox?link=expired")
        good = pl.make_token("c", pid)
        bad = good[:-1] + ("0" if good[-1] != "0" else "1")
        self.assertEqual(self.post(f"/app/go/c/{bad}", {}).headers["location"], "/app/inbox?link=invalid")
        self.assertEqual(self.post(f"/app/go/c/{pl.make_token('r', pid)}", {}).headers["location"], "/app/inbox?link=invalid")
        self.assertEqual(self.pending()[0]["state"], "open")
        self.assertEqual(len(_run(rp.history(pid))), 1)


# ── 3・4. 返信画面: [送信] の表示とサーバー側自動確認 ────────────────────────────────
class TestReplyAutoCheck(_InboxMixin):
    PAGE = f"/app/reply/{REC_ID}"

    def setUp(self):
        super().setUp()
        sp.clear_condition_cache()

    def _form(self, op="f" * 32, text="お返事します。"):
        return {"op_id": op, "text": text, "ticket": rv.make_ticket(REC_ID, USER, "0", op), "exemplar": "1"}

    def send(self, **kw):
        return self.post(f"{self.PAGE}/send", self._form(**kw))

    def test_send_button_is_labeled_and_styled_and_no_confirm_checkbox(self):
        r = self.get(self.PAGE)
        self.assertEqual(r.status_code, 200)
        html = r.text
        self.assertIn('<button type="submit" class="send">送信</button>', html)
        self.assertEqual(html.count("<button"), 1)
        self.assertNotIn('name="confirm"', html)
        self.assertNotIn("確認していません", html)
        self.assertIn('<meta name="color-scheme" content="light">', html)
        css = html.split("<style>")[1].split("</style>")[0]
        self.assertIn("button.send{display:block;width:100%;background:#1a3c6e;color:#fff;", css)
        self.assertIn("background:#fff;color:#222;min-height:44px;-webkit-appearance:none;appearance:none;font-family:inherit;line-height:1.3}", css)
        self.assertIn('name="exemplar" value="1" checked', html)                 # 手本の保存は残す
        self.assertIn('href="/app/inbox">対応待ちへ戻る</a>', html)
        self.assertIn("サーバーが自動で確認します", html)

    def test_state_badges_reflect_conditions(self):
        _run(sl.set_attending("jikou", USER))
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            self.stop_check.return_value = autoreply_stoplist.OUTCOME_STOPPED
            self.app21.return_value = _rec(status="受任", response_mode="人対応")
            html = self.get(self.PAGE).text
        for label in ("全体停止中（bot）", "停止リスト登録あり", "人対応中", "対応中（bot 停止）"):
            self.assertIn(label, html)
        sp.clear_condition_cache()
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_UNKNOWN
        self.app21.side_effect = RuntimeError("down")
        html = self.get(self.PAGE).text
        self.assertIn("停止リスト照会不能", html)
        self.assertIn("対応モード照会不能", html)
        self.assertNotIn("全体停止中", html)

    def test_stoplisted_or_unknown_conditions_block_without_sending(self):
        cases = ((autoreply_stoplist.OUTCOME_STOPPED, None, "stoplisted"),
                 (autoreply_stoplist.OUTCOME_UNKNOWN, None, "stoplist_unknown"),
                 (autoreply_stoplist.OUTCOME_CLEAR, RuntimeError("down"), "human_mode_unknown"))
        for i, (stop, app21_exc, code) in enumerate(cases):
            sp.clear_condition_cache()
            self.stop_check.return_value = stop
            self.app21.side_effect = app21_exc
            r = self.send(op=("%032x" % (i + 1)))
            self.assertEqual((r.status_code, r.headers["location"]), (303, f"{self.PAGE}?done={code}"), code)
            self.assertEqual(_FakeClient.calls, [], code)                        # LINE write 0
            self.assertEqual(self.ops(), [], code)                                # 台帳の着手もしない
            self.assertEqual(self.pending(), [], code)
            self.assertIn(rv._DONE_NOTICES[code][:12], self.get(f"{self.PAGE}?done={code}").text)
        self.assertIn("停止リストから外してください", rv._DONE_NOTICES["stoplisted"])

    def test_paused_human_mode_and_attending_still_send(self):
        _run(sl.set_attending("jikou", USER))
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            self.app21.return_value = _rec(status="受任", response_mode="人対応")
            r = self.send()
        self.assertTrue(r.headers["location"].startswith(f"{self.PAGE}?done=sent"), r.headers["location"])
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual(self.ops()[0]["actor"], "human")

    def test_decision_table_for_human_auto(self):
        clear = {"paused": sl.CLEAR, "stoplisted": sl.CLEAR, "human_mode": sl.CLEAR}
        self.assertTrue(sl.human_auto_decision(clear).allow)
        self.assertTrue(sl.human_auto_decision({**clear, "paused": sl.STOPPED}).allow)
        self.assertTrue(sl.human_auto_decision({**clear, "human_mode": sl.STOPPED}).allow)
        self.assertEqual(sl.human_auto_decision({**clear, "stoplisted": sl.STOPPED}).reasons,
                         ("stoplisted:stopped:explicit_confirm_required",))
        self.assertEqual(sl.human_auto_decision({**clear, "stoplisted": sl.UNKNOWN}).reasons,
                         ("stoplisted:unknown:explicit_confirm_required",))
        self.assertEqual(sl.human_auto_decision({**clear, "human_mode": sl.UNKNOWN}).reasons,
                         ("human_mode:unknown:explicit_confirm_required",))
        self.assertFalse(sl.human_auto_decision(None).allow)                     # 読めない＝送らない
        self.assertEqual(rv._auto_check_code(sl.human_auto_decision(None)), "stoplist_unknown")
        src = (REPO / "hub" / "webapp_reply_view.py").read_text(encoding="utf-8")
        self.assertNotIn("human_explicit_decision(", src.split('"""', 2)[2])   # 旧入口（チェック依存）は返信画面から消えている
        self.assertNotIn('form.get("confirm"', src)


# ── 6. 試験アカウント allowlist（実装のみ・env 設定と有効化は大野） ─────────────────
class TestTrialAccount(unittest.TestCase):
    def test_env_hash_allowlist_and_routing_override(self):
        h = sl.line_user_hash("jikou", USER)
        with patch.dict(os.environ, {trial_account.ENV: f" {h.upper()} , not-a-hash,"}):
            self.assertTrue(trial_account.is_test_user(USER))
            self.assertFalse(trial_account.is_test_user(USER2))
            self.assertEqual(trial_account.routing_override(USER, "hearing"), "pre_engagement")
            self.assertEqual(trial_account.routing_override(USER, "post_engagement"), "post_engagement")
            self.assertEqual(trial_account.routing_override(USER2, "hearing"), "hearing")
        with patch.dict(os.environ, {trial_account.ENV: ""}):
            self.assertFalse(trial_account.is_test_user(USER))
        os.environ.pop(trial_account.ENV, None)
        self.assertFalse(trial_account.is_test_user(USER))
        self.assertNotIn(USER, repr(trial_account.test_user_hashes()))

    def test_main_routing_uses_override_in_place_without_line_shift(self):
        src = (REPO / "main.py").read_text(encoding="utf-8").splitlines()
        hits = [i for i, l in enumerate(src, 1) if "trial_account." in l]
        self.assertEqual(len(hits), 2 + 0, hits)                                  # 1 行置換 2 箇所（+ 末尾の import は "trial_account  #"）
        self.assertIn("if not in_hearing_session or trial_account.is_test_user(user_id):", src[hits[0] - 1])
        self.assertIn("routing = trial_account.routing_override(user_id, classify_routing(status))", src[hits[1] - 1])
        self.assertIn("from hub import trial_account  # noqa: E402", "\n".join(src))


# ── migration・メニュー ─────────────────────────────────────────────────────────
class TestMigrationAndMenu(unittest.TestCase):
    def test_approval_close_migration_round_trip_and_refuses_downgrade_with_data(self):
        from test_db_foundation import TestAlembicScaffold
        NOTICE, CLOSE = "b7d9f1a3c5e7", "c9e1b3d5f7a9"
        d = tempfile.mkdtemp(prefix="approval_close_mig_")
        dbfile = f"{d}/mig.db"
        alembic, tables, _u, nullable = TestAlembicScaffold._brain_mig_tools(dbfile)
        try:
            self.assertEqual(alembic("upgrade", NOTICE).returncode, 0)
            self.assertNotIn(ac.TABLE_NAME, tables())
            up = alembic("upgrade", "head")
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertIn(CLOSE, alembic("current").stdout)
            self.assertEqual(alembic("heads").stdout.count("(head)"), 1)
            con = sqlite3.connect(dbfile)
            try:
                cols = {r[1] for r in con.execute(f'PRAGMA table_info("{ac.TABLE_NAME}")')}
                self.assertEqual(cols, {c.name for c in ac.approval_close.columns})
                nn = nullable(con, ac.TABLE_NAME)
                self.assertFalse(any(nn[c] for c in ("business", "approval_record_id", "outcome", "actor", "at")))   # すべて NOT NULL
                con.execute(f"INSERT INTO {ac.TABLE_NAME} (business, approval_record_id, outcome, actor, at) "
                            "VALUES ('jikou', '777', 'done_external', 'human:inbox', '2026-11-03 00:00:00')")
                con.commit()
            finally:
                con.close()
            # CHECK 制約: closed_external が入り、未知の値は拒否される（制約名は据え置き）
            con = sqlite3.connect(dbfile)
            try:
                base = ("INSERT INTO reply_pending (business, channel, line_user_hash, route, reason, state, notify_state, customer_send, "
                        "attempts, created_at, updated_at) VALUES ('jikou', 'jikou', 'h', 'customer', 'demoted', '%s', 'none', 'none', 0, "
                        "'2026-11-03 00:00:00', '2026-11-03 00:00:00')")
                con.execute(base % "closed_external")
                con.commit()
                with self.assertRaises(sqlite3.IntegrityError):
                    con.execute(base % "bogus")
                con.rollback()
            finally:
                con.close()
            down = alembic("downgrade", NOTICE)                       # approval_close にデータ → 拒否
            self.assertNotEqual(down.returncode, 0)
            self.assertIn("approval_close_downgrade_refused_data_present", down.stderr)
            con = sqlite3.connect(dbfile)
            try:
                con.execute(f"DELETE FROM {ac.TABLE_NAME}")
                con.commit()
            finally:
                con.close()
            down = alembic("downgrade", NOTICE)                       # closed_external の行 → 拒否
            self.assertNotEqual(down.returncode, 0)
            self.assertIn("reply_pending_closed_external_downgrade_refused_data_present", down.stderr)
            self.assertIn(CLOSE, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                con.execute("DELETE FROM reply_pending")
                con.commit()
            finally:
                con.close()
            self.assertEqual(alembic("downgrade", NOTICE).returncode, 0)
            con = sqlite3.connect(dbfile)
            try:
                with self.assertRaises(sqlite3.IntegrityError):       # 戻した後は 4 値に戻る
                    con.execute(base % "closed_external")
                con.rollback()
            finally:
                con.close()
            self.assertEqual(alembic("upgrade", "head").returncode, 0)
            con = sqlite3.connect(dbfile)
            try:
                con.execute(f"INSERT INTO {ac.TABLE_NAME} (business, approval_record_id, outcome, actor, at) "
                            "VALUES ('jikou', '777', 'done_external', 'human:inbox', '2026-11-03 00:00:00')")
                con.commit()
            finally:
                con.close()
            down = alembic("downgrade", NOTICE)
            self.assertNotEqual(down.returncode, 0)
            self.assertIn("approval_close_downgrade_refused_data_present", down.stderr)
            self.assertNotIn("777", down.stderr.split("approval_close_downgrade_refused_data_present")[0][-200:])
            self.assertIn(CLOSE, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                con.execute(f"DELETE FROM {ac.TABLE_NAME}")
                con.commit()
            finally:
                con.close()
            self.assertEqual(alembic("downgrade", NOTICE).returncode, 0)
            self.assertNotIn(ac.TABLE_NAME, tables())
            self.assertEqual(alembic("upgrade", "head").returncode, 0)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_menu_home_and_login_carry_inbox(self):
        shell = (REPO / "webapp" / "shell.js").read_text(encoding="utf-8")
        self.assertIn('["/app/inbox", "対応待ち"]', shell)
        self.assertLess(shell.index('["/app/approvals", "承認"]'), shell.index('["/app/inbox", "対応待ち"]'))   # 承認の隣
        self.assertNotIn("innerHTML", shell)
        index = (REPO / "webapp" / "index.html").read_text(encoding="utf-8")
        self.assertIn('href="/app/inbox">対応待ち', index)
        self.assertNotIn("承認キュー", index)
        login = (REPO / "webapp" / "login.html").read_text(encoding="utf-8")
        self.assertIn("/app/login?next='+m[1]", login)
        self.assertNotIn("innerHTML", login)
        self.assertEqual(webapp_auth.PUBLIC_ROUTES, {("/app/login", "GET"), ("/app/login", "POST")})   # 公開 route は増えていない

    def test_no_send_path_from_links_or_inbox(self):
        """承認なし送信経路ゼロ: 統合一覧・署名リンクの module は LINE 送信関数を呼ばない。"""
        src = (REPO / "hub" / "webapp_inbox_view.py").read_text(encoding="utf-8")
        for banned in ("push_text_explicit", "reply_with_push_fallback", "push_line_message", "send_business_notice",
                       "httpx", "approved_text("):
            self.assertNotIn(banned, src, banned)
        src = (REPO / "hub" / "pending_link.py").read_text(encoding="utf-8")
        for banned in ("httpx", "kintone", "line_user", "顧客名"):
            self.assertNotIn(banned, src, banned)


if __name__ == "__main__":
    unittest.main()
