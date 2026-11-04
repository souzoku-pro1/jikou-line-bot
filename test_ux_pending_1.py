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
from hub import attending_release as ar  # noqa: E402
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
                await c.run_sync(ar.metadata.create_all)
        _run(_create())
        self.records = {REC_ID: _approval()}
        self.old_records = []
        self.old_queries = []

        async def _search(app, query, fields=None):
            if "$id in (" in query:
                ids = re.findall(r'"([0-9]+)"', query)
                return [self.records[i] for i in ids if i in self.records]
            if "送信済み in" in query:
                self.old_queries.append(query)
                m = re.search(r"limit (\d+)(?: offset (\d+))?", query)
                lim, off = (int(m.group(1)), int(m.group(2) or 0)) if m else (len(self.old_records), 0)
                ex = set(re.findall(r'"([0-9]+)"', query.split("not in (")[1].split(")")[0])) if "not in (" in query else set()
                rows = [r_ for r_ in self.old_records if r_["$id"]["value"] not in ex]   # fix2 BU-09: 問い合わせ条件の除外
                return rows[off:off + lim]
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
        rv._held.clear()

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


# ── fix1（Codex R-UX1 REJECT）BU-01〜05 ───────────────────────────────────────────
class TestFix1ReplyCheck(_InboxMixin):
    PAGE = f"/app/reply/{REC_ID}"

    def setUp(self):
        super().setUp()
        sp.clear_condition_cache()
        rv._held.clear()

    def _form(self, op="f" * 32, text="お返事します。"):
        return {"op_id": op, "text": text, "ticket": rv.make_ticket(REC_ID, USER, "0", op), "exemplar": "1"}

    def send(self, **kw):
        return self.post(f"{self.PAGE}/send", self._form(**kw))

    def test_bu01_send_rechecks_stoplist_after_page_render_bypassing_cache(self):
        """BU-01: 画面表示の直後に停止リストへ登録 → 直後の POST は送らない（照会は GET と POST で 2 回・キャッシュを読まない）。"""
        self.assertEqual(self.get(self.PAGE).status_code, 200)
        self.assertEqual(self.stop_check.await_count, 1)
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_STOPPED        # 表示後に登録された
        r = self.send()
        self.assertEqual((r.status_code, r.headers["location"]), (303, f"{self.PAGE}?done=stoplisted"))
        self.assertEqual(self.stop_check.await_count, 2)                         # 都度照会（30 秒窓の値を使い回さない）
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.ops(), [])
        # bot 側の conditions() は従来どおりキャッシュする（窓内 2 回目は照会しない）
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_CLEAR
        sp.clear_condition_cache()
        _run(sp.conditions(USER)); _run(sp.conditions(USER))
        self.assertEqual(self.stop_check.await_count, 3)
        # 画面の確認は bot のキャッシュに値を残さない（human の照会結果が bot 側に流れない）
        sp.clear_condition_cache()
        _run(sp.human_conditions(USER))
        self.assertEqual(sp._condition_cache, {})
        # 照会の失敗（例外）は不送信
        self.stop_check.side_effect = RuntimeError("down")
        r = self.send(op="1" * 32)
        self.assertEqual(r.headers["location"], f"{self.PAGE}?done=stoplist_unknown")
        self.assertEqual(_FakeClient.calls, [])

    def test_bu02_unconfigured_stoplist_and_missing_app21_config_are_unknown_but_zero_hits_is_clear(self):
        """BU-02: 停止リスト unconfigured・App 21 の設定欠落 → unknown（送らない）。検索 0 件（clear）は送る。"""
        # 停止リスト: 未設定（unconfigured）／照会失敗（unknown）／0 件（clear）
        cases = ((autoreply_stoplist.OUTCOME_UNCONFIGURED, "stoplist_unknown"),
                 (autoreply_stoplist.OUTCOME_UNKNOWN, "stoplist_unknown"),
                 ("bogus", "stoplist_unknown"))
        for i, (outcome, code) in enumerate(cases):
            self.stop_check.return_value = outcome
            r = self.send(op=("%032x" % (i + 10)))
            self.assertEqual(r.headers["location"], f"{self.PAGE}?done={code}", outcome)
            self.assertEqual(_FakeClient.calls, [], outcome)
            self.assertEqual(_run(sp.human_conditions(USER))["stoplisted"], sl.UNKNOWN, outcome)
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_CLEAR               # 正常な 0 件
        self.assertEqual(_run(sp.human_conditions(USER))["stoplisted"], sl.CLEAR)
        # App 21: 設定欠落（env 未設定で None）／照会失敗／設定ありでレコード 0 件（None）
        with patch.dict(os.environ, {"KINTONE_APP_ID": ""}):
            self.app21.return_value = None
            self.assertFalse(sp.app21_configured())
            r = self.send(op="2" * 32)
            self.assertEqual(r.headers["location"], f"{self.PAGE}?done=human_mode_unknown")
            self.assertEqual(_FakeClient.calls, [])
            self.assertIn("対応モード照会不能", self.get(self.PAGE).text)
        self.assertTrue(sp.app21_configured())
        self.app21.side_effect = RuntimeError("down")
        r = self.send(op="3" * 32)
        self.assertEqual(r.headers["location"], f"{self.PAGE}?done=human_mode_unknown")
        self.assertEqual(_FakeClient.calls, [])
        self.app21.side_effect = None
        self.app21.return_value = None                                                 # 設定あり・レコード無し＝対応モード未設定
        self.assertEqual(_run(sp.human_conditions(USER))["human_mode"], sl.CLEAR)
        r = self.send(op="4" * 32)
        self.assertTrue(r.headers["location"].startswith(f"{self.PAGE}?done=sent"), r.headers["location"])
        self.assertEqual(len(_FakeClient.calls), 1)
        # bot 側の判定（conditions）は従来どおり: unconfigured は CLEAR（V2 の既存挙動・本票で変えない）
        sp.clear_condition_cache()
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_UNCONFIGURED
        self.assertEqual(_run(sp.conditions(USER))["stoplisted"], sl.CLEAR)

    def test_bu03_edited_text_survives_auto_check_stop_without_touching_url(self):
        """BU-03: 元の下書きと異なる編集文で停止 → 再表示に編集文が残る（本文は URL・クエリ・ログに出ない）。送れたら消える。"""
        edited = "編集した本文です。090 ではなく事務所へ。"
        self.assertIn("下書き本文", self.get(self.PAGE).text)
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_STOPPED
        with self.assertLogs("hub.webapp_reply_view", level="INFO") as cm:
            r = self.send(text=edited)
        self.assertEqual(r.headers["location"], f"{self.PAGE}?done=stoplisted")       # クエリに本文なし
        self.assertNotIn("編集した本文", r.headers["location"])
        self.assertFalse(any("編集した本文" in m for m in cm.output))
        html = self.get(f"{self.PAGE}?done=stoplisted").text
        self.assertIn(f'required>{edited}</textarea>', html)                           # 編集文が返信欄に戻る
        self.assertNotIn("required>下書き本文", html)
        self.assertIn("編集中の本文を返信欄に戻しました", html)
        # 別のセッション（別ブラウザ）には出ない
        other = _client.get(self.PAGE, headers={"Cookie": f"webapp_session={webapp_auth.issue_session(now=int(time.time()) - 5)}"},
                            follow_redirects=False).text
        self.assertIn("required>下書き本文", other)
        # 期限切れは破棄
        key = next(iter(rv._held))[0]
        self.assertIsNone(rv.held_text(key, REC_ID, now=time.monotonic() + rv.HOLD_TTL_SECONDS + 1))
        self.assertEqual(rv._held, {})
        # 保持 → 送信成功で消える（停止が解けて同じ本文を送る）
        rv.hold_text(key, REC_ID, edited)
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_CLEAR
        r = self.send(op="5" * 32, text=edited)
        self.assertTrue(r.headers["location"].startswith(f"{self.PAGE}?done=sent"))
        self.assertEqual(rv._held, {})
        self.assertIn("required>下書き本文", self.get(self.PAGE).text)
        # 上限: 古いものから消える
        for i in range(rv.HOLD_MAX + 5):
            rv.hold_text("k%d" % i, "1", "x", now=float(i))
        self.assertEqual(len(rv._held), rv.HOLD_MAX)
        self.assertNotIn(("k0", "1"), rv._held)
        rv._held.clear()


class TestFix1Inbox(_InboxMixin):
    def test_bu05_old_items_page_past_closed_records(self):
        """BU-05: 閉じ済み 50 件 + 未処理 1 件（51 件目）で、その 1 件が表示される（除外後の件数でページング）。"""
        closed_ids = [str(1000 + i) for i in range(50)]
        self.old_records = [_approval(i, USER2, 顧客名="旧 %s" % i, 更新日時="2026-09-01T01:%02d:00Z" % (int(i) % 60))
                            for i in closed_ids] + [_approval("1050", USER2, 顧客名="旧 未処理", 更新日時="2026-09-02T01:00:00Z")]
        for i in closed_ids:
            self.assertEqual(_run(ac.close("jikou", i, "done_external", "t")), "ok")
        html = self.get("/app/inbox").text
        self.assertIn("旧 未処理", html)
        self.assertEqual(html.count('class="badge old"'), 1)
        self.assertNotIn("旧 1000", html)
        # fix2 BU-09 追随: 除外 ID が 100 件以下（閉じ済み 50＋参照 1）なら問い合わせ条件で除外 → 1 問い合わせで到達（続きの走査は BU-09 で固定）
        self.assertEqual(len(self.old_queries), 1)
        self.assertIn('and $id not in ("1000","1001",', self.old_queries[0])
        self.assertTrue(self.old_queries[0].endswith("limit 50 offset 0"))
        self.assertNotIn("続きがあります", html)
        # 除外後に上限に達したら続きは読まない／対応待ちが参照する No. も除外して数える
        self.old_queries.clear()
        self.old_records = [_approval(str(2000 + i), USER2, 顧客名="旧 %d" % i) for i in range(60)]
        html = self.get("/app/inbox").text
        self.assertEqual(html.count('class="badge old"'), iv.OLD_LIMIT)
        self.assertEqual(self.old_queries, [self.old_queries[0]])
        # 取得失敗は空（固定理由ログ）・画面は落ちない
        with patch.object(iv.kintone, "search_records", AsyncMock(side_effect=RuntimeError("down"))):
            self.assertEqual(self.get("/app/inbox").status_code, 200)
        # 上限ページ数で止まる（全部除外でも無限に読まない）
        self.old_queries.clear()
        self.old_records = [_approval(str(3000 + i), USER2) for i in range(iv.OLD_PAGE * (iv.OLD_MAX_PAGES + 2))]
        self.assertEqual(_run(iv._old_records({r_["$id"]["value"] for r_ in self.old_records})), [])
        self.assertEqual(len(self.old_queries), iv.OLD_MAX_PAGES)

    def releases(self) -> list:
        return self._rows(ar.attending_release)

    def test_bu04_release_failure_leaves_pending_release_and_recovery_releases(self):
        """BU-04: 解除失敗の注入 → closed だが「解除未完了」が残る → 再操作（already）でも再試行 → 回収で解除される。"""
        pid = self._reg(inbound_event_id="ev1")
        att = _run(sl.set_attending("jikou", USER))
        with patch.object(sl, "release_attending", AsyncMock(side_effect=RuntimeError("db"))):
            r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=closed_release_pending")
        self.assertEqual(self.pending()[0]["state"], "closed_external")                 # 閉じ記録は残る
        self.assertTrue(_run(sl.conversation_state("jikou", USER))["attending"])        # 解除は未完了
        rel = self.releases()
        self.assertEqual(len(rel), 1)
        self.assertEqual((rel[0]["state"], rel[0]["attempts"], rel[0]["source_kind"], rel[0]["source_id"]),
                         ("pending", 1, "pending", str(pid)))
        self.assertEqual((rel[0]["conversation_id"], rel[0]["human_version"]), (att["conversation_id"], att["human_version"]))
        self.assertEqual(rel[0]["line_user_hash"], sl.line_user_hash("jikou", USER))
        self.assertIn("回収ジョブが自動で再試行します", self.get("/app/inbox?done=closed_release_pending").text)
        # 再操作（already）でも解除を再試行する（まだ失敗する → 回数だけ進む）
        with patch.object(sl, "release_attending", AsyncMock(side_effect=RuntimeError("db"))):
            r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=already")
        self.assertEqual((self.releases()[0]["state"], self.releases()[0]["attempts"]), ("pending", 2))
        # 回収ジョブ（send_ledger の hook に登録済み）で解除される
        self.assertIn(iv.run_release_job, sl._recover_hooks)
        _run(sl.run_recover_job())
        self.assertFalse(_run(sl.conversation_state("jikou", USER))["attending"])
        row = self.releases()[0]
        self.assertEqual((row["state"], row["result"], row["attempts"]), ("done", "released", 3))
        self.assertIsNotNone(row["done_at"])
        # 以後は何もしない（冪等）・再操作でも再試行なし
        self.assertEqual(_run(ar.run_release_job()), {"pending": 0, "done": 0, "abandoned": 0})
        r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "done_external"})
        self.assertEqual((r.headers["location"], self.releases()[0]["attempts"]), ("/app/inbox?done=already", 3))
        # 成功経路: 閉じ → 即時解除 → closed（従来の表示）
        pid2 = self._reg(user=USER2, inbound_event_id="ev2", approval_record_id=None)
        _run(sl.set_attending("jikou", USER2))
        r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid2), "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=closed")
        self.assertFalse(_run(sl.conversation_state("jikou", USER2))["attending"])
        self.assertEqual([x["state"] for x in self.releases()], ["done", "done"])

    def test_bu04_release_targets_only_the_fixed_generation(self):
        """BU-04: 閉じた後に大野が再び対応中にした（世代が進んだ）会話は、回収で解除しない（stale・完了扱い）。"""
        pid = self._reg(inbound_event_id="ev1")
        _run(sl.set_attending("jikou", USER))
        with patch.object(sl, "release_attending", AsyncMock(side_effect=RuntimeError("db"))):
            self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "done_external"})
        _run(sl.end_attending("jikou", USER))                                            # 別経路で解除され…
        _run(sl.set_attending("jikou", USER))                                            # …新しい世代で再び対応中
        _run(iv.run_release_job())
        self.assertTrue(_run(sl.conversation_state("jikou", USER))["attending"])         # 新しい世代は触らない
        self.assertEqual((self.releases()[0]["state"], self.releases()[0]["result"]), ("done", "stale"))
        # fix2 BU-08 追随: 会話の記録が無い相談者は対象を固定できない → needs_confirm（回収は触らない・大野の確認で閉じる）
        pid2 = self._reg(user=USER2, inbound_event_id="ev2", approval_record_id=None)
        r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid2), "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=closed_release_confirm")
        _run(iv.run_release_job())
        self.assertEqual((self.releases()[1]["state"], self.releases()[1]["result"], self.releases()[1]["attempts"]),
                         ("needs_confirm", None, 0))
        self.assertEqual(_run(sl.release_attending("jikou", sl.line_user_hash("jikou", USER),
                                                   conversation_id=self.releases()[0]["conversation_id"],
                                                   human_version=self.releases()[0]["human_version"])), "stale")

    def test_bu08_unresolved_target_needs_confirm_and_is_released_only_by_button(self):
        """BU-08（Codex 反例 1）: 閉じた時点で対象を特定できない件は自動解除しない（回収でも確定・解除しない）。
        一覧の「解除の要確認」で現在の状態を見て [対応中を解除する] を押したときだけ、その時点の世代を取得して解除する。
        （fix1 の test_bu04_old_item_with_unresolvable_hash_is_released_by_recovery を BU-08 の裁定に追随して置換）"""
        self.old_records = [_approval(OLD_ID, USER2, 顧客名="旧 花子")]
        r = self.post("/app/inbox/close", {"kind": "old", "id": OLD_ID, "outcome": "done_external"})   # records に無い＝userId を引けない
        self.assertEqual(r.headers["location"], "/app/inbox?done=closed_release_confirm")
        self.assertEqual(_run(ac.closed_ids("jikou")), {OLD_ID})
        row = self.releases()[0]
        self.assertEqual((row["state"], row["line_user_hash"], row["conversation_id"], row["attempts"], row["source_kind"], row["source_id"]),
                         ("needs_confirm", None, None, 0, "old", OLD_ID))
        html = self.get("/app/inbox?done=closed_release_confirm").text
        self.assertIn("解除の要確認", html)
        self.assertIn("相談者を特定できません", html)
        self.assertNotIn("対応中を解除する", html)                                       # 特定できない間は押せない
        # 閉じた後に大野が対応中にした（世代 10）→ 回収は触らない（引けるようになっても確定しない）
        att = _run(sl.set_attending("jikou", USER2))
        _run(iv.run_release_job())
        _run(sl.run_recover_job())
        self.records[OLD_ID] = _approval(OLD_ID, USER2)                                  # 引けるようになった
        _run(iv.run_release_job())
        self.assertTrue(_run(sl.conversation_state("jikou", USER2))["attending"])        # 世代 10 は自動では解除しない
        row = self.releases()[0]
        self.assertEqual((row["state"], row["attempts"], row["conversation_id"], row["line_user_hash"]), ("needs_confirm", 0, None, None))
        html = self.get("/app/inbox").text
        self.assertIn("bot 停止中（対応中）", html)                                       # 押す前に現在の状態
        self.assertIn("対応中を解除する", html)
        self.assertNotIn(USER2, html)
        self.assertNotIn(sl.line_user_hash("jikou", USER2), html)
        # 押したときだけ、その時点の世代を取得して解除
        r = self.post("/app/inbox/release", {"release_id": str(row["release_id"])})
        self.assertEqual(r.headers["location"], "/app/inbox?done=released")
        self.assertFalse(_run(sl.conversation_state("jikou", USER2))["attending"])
        row = self.releases()[0]
        self.assertEqual((row["state"], row["result"], row["line_user_hash"], row["conversation_id"], row["human_version"], row["attempts"]),
                         ("done", "released", sl.line_user_hash("jikou", USER2), att["conversation_id"], att["human_version"], 1))
        html = self.get("/app/inbox?done=released").text
        self.assertIn("対応中を解除しました", html)
        self.assertNotIn("解除の要確認", html)
        # 2 回目は処理しない／不正入力は 400／再操作（already）は closed のまま
        self.assertEqual(self.post("/app/inbox/release", {"release_id": str(row["release_id"])}).headers["location"], "/app/inbox?done=already")
        self.assertEqual(self.post("/app/inbox/release", {"release_id": "x"}).status_code, 400)
        r = self.post("/app/inbox/close", {"kind": "old", "id": OLD_ID, "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=already")
        self.assertEqual(self.releases()[0]["attempts"], 1)

    def test_bu08_fixed_generation_is_persisted_before_release_and_later_generation_not_adopted(self):
        """BU-08（Codex 反例 2）: 押したときの世代を **先に永続化** してから解除を試みる。初回が失敗しても、その後に進んだ世代（20）は
        採用せず、回収は固定した世代（10）に対してのみ再試行する（stale＝完了・世代 20 の対応中はそのまま）。"""
        self.old_records = [_approval(OLD_ID, USER2)]
        self.post("/app/inbox/close", {"kind": "old", "id": OLD_ID, "outcome": "done_external"})
        self.assertEqual(self.releases()[0]["state"], "needs_confirm")
        self.records[OLD_ID] = _approval(OLD_ID, USER2)
        gen10 = _run(sl.set_attending("jikou", USER2))
        with patch.object(sl, "release_attending", AsyncMock(side_effect=RuntimeError("db"))):
            r = self.post("/app/inbox/release", {"release_id": str(self.releases()[0]["release_id"])})
        self.assertEqual(r.headers["location"], "/app/inbox?done=release_pending")
        row = self.releases()[0]
        self.assertEqual((row["state"], row["attempts"], row["conversation_id"], row["human_version"], row["line_user_hash"]),
                         ("pending", 1, gen10["conversation_id"], gen10["human_version"], sl.line_user_hash("jikou", USER2)))
        self.assertTrue(_run(sl.conversation_state("jikou", USER2))["attending"])
        self.assertNotIn("解除の要確認", self.get("/app/inbox").text)                   # 固定済み＝要確認ではない（回収が再試行）
        # 世代が進む（別経路で解除 → 再び対応中＝世代 20）
        _run(sl.end_attending("jikou", USER2))
        gen20 = _run(sl.set_attending("jikou", USER2))
        self.assertNotEqual((gen20["conversation_id"], gen20["human_version"]), (gen10["conversation_id"], gen10["human_version"]))
        _run(sl.run_recover_job())                                                       # 回収は固定した世代 10 にだけ
        self.assertTrue(_run(sl.conversation_state("jikou", USER2))["attending"])        # 世代 20 は触らない
        row = self.releases()[0]
        self.assertEqual((row["state"], row["result"], row["conversation_id"], row["human_version"]),
                         ("done", "stale", gen10["conversation_id"], gen10["human_version"]))
        # 会話の記録が無い相談者 → 押すと「解除不要」で確認済み（no_conversation・done）
        pid = self._reg(user="U" + "3" * 32, inbound_event_id="ev3", approval_record_id=None)
        r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=closed_release_confirm")
        html = self.get("/app/inbox").text
        self.assertIn("会話の記録がありません", html)
        self.assertIn("解除不要として確認済みにする", html)
        r = self.post("/app/inbox/release", {"release_id": str(self.releases()[1]["release_id"])})
        self.assertEqual(r.headers["location"], "/app/inbox?done=release_not_needed")
        self.assertEqual((self.releases()[1]["state"], self.releases()[1]["result"], self.releases()[1]["conversation_id"]),
                         ("done", "no_conversation", None))
        # 対応中でない（会話はある）相談者 → 押すと already_off で確認済み
        _run(sl.end_attending("jikou", USER2))
        self.old_records = [_approval("778", USER2)]
        with patch.object(iv, "_hash_for_record", AsyncMock(return_value=None)):         # 閉じた時点は特定できない
            r = self.post("/app/inbox/close", {"kind": "old", "id": "778", "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=closed_release_confirm")
        self.records["778"] = _approval("778", USER2)
        self.assertIn("対応中ではありません", self.get("/app/inbox").text)
        r = self.post("/app/inbox/release", {"release_id": str(self.releases()[2]["release_id"])})
        self.assertEqual(r.headers["location"], "/app/inbox?done=release_not_needed")
        self.assertEqual((self.releases()[2]["state"], self.releases()[2]["result"]), ("done", "already_off"))

    def test_bu04_close_record_and_release_row_are_one_transaction(self):
        """BU-04: 「解除未完了」を書けなければ閉じ記録も残らない（同一 tx）。不要（not_needed）は従来どおり解除しない。"""
        pid = self._reg(inbound_event_id="ev1")
        _run(sl.set_attending("jikou", USER))
        n0 = len(_run(rp.history(pid)))
        with patch.object(ar, "record_in_tx", AsyncMock(side_effect=RuntimeError("db"))):
            r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=db")
        self.assertEqual(self.pending()[0]["state"], "open")                             # rollback
        self.assertEqual(len(_run(rp.history(pid))), n0)                                 # 履歴も増えない
        self.assertEqual(self.releases(), [])
        self.old_records = [_approval(OLD_ID, USER2)]
        with patch.object(ar, "record_in_tx", AsyncMock(side_effect=RuntimeError("db"))):
            r = self.post("/app/inbox/close", {"kind": "old", "id": OLD_ID, "outcome": "done_external"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=db")
        self.assertEqual(_run(ac.closed_ids("jikou")), set())
        r = self.post("/app/inbox/close", {"kind": "pending", "id": str(pid), "outcome": "not_needed"})
        self.assertEqual(r.headers["location"], "/app/inbox?done=not_needed")
        self.assertEqual(self.releases(), [])
        self.assertTrue(_run(sl.conversation_state("jikou", USER))["attending"])
        # 試行上限で abandoned（運用で確認・自動では再試行しない）
        pid3 = self._reg(user=USER2, inbound_event_id="ev3", approval_record_id=None)
        _run(sl.set_attending("jikou", USER2))
        with patch.object(sl, "release_attending", AsyncMock(side_effect=RuntimeError("db"))):
            self.post("/app/inbox/close", {"kind": "pending", "id": str(pid3), "outcome": "done_external"})
            for _ in range(ar.MAX_ATTEMPTS):
                _run(iv.run_release_job())
        self.assertEqual((self.releases()[0]["state"], self.releases()[0]["attempts"]), ("abandoned", ar.MAX_ATTEMPTS))
        self.assertEqual(_run(ar.run_release_job()), {"pending": 0, "done": 0, "abandoned": 0})


class TestFix2Inbox(_InboxMixin):
    def test_bu09_scan_limit_is_distinguished_from_no_items_and_continues(self):
        """BU-09: 閉じ済み 500 件 + 未処理 1 件（除外が 100 件を超えるため走査で除外）→ 1 走査目は上限到達＝「続きあり」
        （未処理 0 件とは区別）→ 「続きを読み込む」でその 1 件に到達する。"""
        closed_ids = [str(5000 + i) for i in range(500)]
        self.old_records = ([_approval(i, USER2, 顧客名="旧 %s" % i, 更新日時="2026-08-01T00:00:00Z") for i in closed_ids]
                            + [_approval("5600", USER2, 顧客名="旧 未処理", 更新日時="2026-09-02T01:00:00Z")])
        for i in closed_ids:
            self.assertEqual(_run(ac.close("jikou", i, "done_external", "t")), "ok")
        html = self.get("/app/inbox").text
        self.assertNotIn("旧 未処理", html)
        self.assertIn("1〜500 件目まで走査しました（この範囲の未処理 0 件）", html)
        self.assertIn("続きがあります", html)
        self.assertIn('href="/app/inbox?old_from=500"', html)
        self.assertIn("この範囲には対応待ちがありません（続きがあります）", html)
        self.assertNotIn("対応待ちはありません</p>", html)                                   # 上限到達は 0 件と区別
        self.assertEqual(len(self.old_queries), iv.OLD_MAX_PAGES)
        self.assertNotIn("not in (", self.old_queries[0])                                   # 除外 501 件＝問い合わせ条件に入れない
        self.assertEqual(self.old_queries[-1].split("limit ")[1], "50 offset 450")
        # 続きの走査でその 1 件に到達
        self.old_queries.clear()
        html = self.get("/app/inbox?old_from=500").text
        self.assertIn("旧 未処理", html)
        self.assertEqual(html.count('class="badge old"'), 1)
        self.assertIn("501〜501 件目まで走査しました（この範囲の未処理 1 件）", html)
        self.assertIn("続きはありません", html)
        self.assertIn('href="/app/inbox">先頭に戻る', html)
        self.assertEqual([q.split("limit ")[1] for q in self.old_queries], ["50 offset 500"])
        # 不正な old_from は 400／閉じ済みが無ければ「対応待ちはありません」（続きの案内なし）
        self.assertEqual(self.get("/app/inbox?old_from=x").status_code, 400)
        self.old_records = []
        html = self.get("/app/inbox").text
        self.assertIn("対応待ちはありません", html)
        self.assertNotIn("続きがあります", html)
        # 上限 50 件に達して同じページに残りがある → 続きあり・次の offset はページ途中から
        self.old_records = [_approval(str(7000 + i), USER2, 顧客名="旧 %d" % i) for i in range(60)]
        _run(ac.close("jikou", "7000", "done_external", "t"))
        recs, nxt, more = _run(iv._scan_old({"7000"}, 0))
        self.assertEqual((len(recs), nxt, more), (iv.OLD_LIMIT, 50, True))
        # 走査の失敗は「続きなし」で打ち切り（固定理由ログ・画面は落ちない）
        with patch.object(iv.kintone, "search_records", AsyncMock(side_effect=RuntimeError("down"))):
            self.assertEqual(_run(iv._scan_old(set(), 0)), ([], 0, False))
            self.assertEqual(self.get("/app/inbox").status_code, 200)


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
            up = alembic("upgrade", CLOSE)                           # fix1: approval_close の検査は c9e1 で（head は d2f4）
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertIn(CLOSE, alembic("current").stdout)
            self.assertEqual(alembic("heads").stdout.count("(head)"), 1)
            self.assertIn("d2f4a6b8c0e2", alembic("heads").stdout)                 # fix1 BU-04 が head
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
            self.assertEqual(alembic("upgrade", CLOSE).returncode, 0)
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

    def test_attending_release_migration_round_trip_and_refuses_downgrade_with_data(self):
        """fix1 BU-04: d2f4（c9e1 の次・新表のみ）の往復。列集合は hub.attending_release と一致。データがあれば downgrade 拒否（値は出さない）。"""
        from test_db_foundation import TestAlembicScaffold
        CLOSE, REL = "c9e1b3d5f7a9", "d2f4a6b8c0e2"
        d = tempfile.mkdtemp(prefix="attending_release_mig_")
        dbfile = f"{d}/mig.db"
        alembic, tables, _u, nullable = TestAlembicScaffold._brain_mig_tools(dbfile)
        try:
            self.assertEqual(alembic("upgrade", CLOSE).returncode, 0)
            self.assertNotIn(ar.TABLE_NAME, tables())
            up = alembic("upgrade", "head")
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertIn(REL, alembic("current").stdout)
            self.assertEqual(alembic("heads").stdout.count("(head)"), 1)
            con = sqlite3.connect(dbfile)
            try:
                cols = {r[1] for r in con.execute(f'PRAGMA table_info("{ar.TABLE_NAME}")')}
                self.assertEqual(cols, {c.name for c in ar.attending_release.columns})
                nn = nullable(con, ar.TABLE_NAME)
                self.assertFalse(any(nn[c] for c in ("business", "source_kind", "source_id", "state", "attempts", "created_at", "updated_at")))
                self.assertTrue(all(nn[c] for c in ("approval_record_id", "line_user_hash", "conversation_id", "human_version", "result", "done_at")))
                idx = {r[1] for r in con.execute(f'PRAGMA index_list("{ar.TABLE_NAME}")')}
                self.assertEqual({"ix_attending_release_state", "ix_attending_release_source"}, idx)
                con.execute(f"INSERT INTO {ar.TABLE_NAME} (business, source_kind, source_id, state, attempts, created_at, updated_at) "
                            "VALUES ('jikou', 'pending', '1', 'pending', 0, '2026-11-04 00:00:00', '2026-11-04 00:00:00')")
                con.commit()
            finally:
                con.close()
            down = alembic("downgrade", CLOSE)
            self.assertNotEqual(down.returncode, 0)
            self.assertIn("attending_release_downgrade_refused_data_present", down.stderr)
            self.assertIn(REL, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                con.execute(f"DELETE FROM {ar.TABLE_NAME}")
                con.commit()
            finally:
                con.close()
            self.assertEqual(alembic("downgrade", CLOSE).returncode, 0)
            self.assertNotIn(ar.TABLE_NAME, tables())
            self.assertEqual(alembic("upgrade", "head").returncode, 0)
            self.assertIn(ar.TABLE_NAME, tables())
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
