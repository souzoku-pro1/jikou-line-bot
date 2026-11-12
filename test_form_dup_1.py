"""FORM-DUP-1: 診断フォームの二重 POST による App 21 重複の抑止。

実測（2026-11-12 調査・74→75→76 は 1.4 秒／1.8 秒差・77→78 は 20 秒差）: 本人専用
リンク経路の 1 回目 POST が token を使用済みにし、同じブラウザからの 2 回目 POST が
「link invalid」→ k なし経路へ落ちて受付番号付き・LINEユーザーID 空の双子を作っていた。

固定する内容:
 1. 使用済み token の POST は k なし経路へ落とさない。used_at から 10 分以内の再 POST は
    1 回目と同じレコードを送信済みとして完了画面（linked 結果ページ）＝App 21 の create は
    1 回だけ・受付番号は発行しない。内容同一=何もしない／内容相違=上書きせず無視して
    業務 LINE へ 1 日 1 回だけ通知／窓の外・本人レコード不明=固定の送信済みページ。
 2. 送信ボタンは 1 回押したら無効化する JS（フォーム HTML に含む・JS 必須にしない）。
 3. k なし経路の同一入力・同一 IP・60 秒以内の連続 POST は新規作成せず同じ受付番号の
    完了画面（1 回目が処理中なら完了を待つ・失敗した 1 回目は後続を妨げない）。
 4. 既存の linked write（T5/T6/SLL-02）と FORM-2 の try_link のテストは不変。
"""

import asyncio
import datetime
import hashlib
import os
import unittest
from unittest.mock import AsyncMock, patch

from test_shindan_line_link import (  # noqa: E402  共通の fake・DB 基盤・凍結値を再利用
    USER, VALID, _FakeApp21, _FormLinkBase, _run)

import shindan_form as sf  # noqa: E402
from hub import db  # noqa: E402
from hub import shindan_link as sl  # noqa: E402

# 画面文言の sha256 pin（弁護士裁定で差し替え可・意図的変更の可視化が目的）
PIN_RESUBMIT_DONE = "ea57c574548cc7a52038fc6cffbe31c29da5793dd47c029d50b6e7177947a1c7"
PIN_PLAIN_PENDING = "00cdf22f444ee828c54c4425125c363ee18ff7891ceb9d50ba0c9643b9e97c52"


class _Base(_FormLinkBase):
    def setUp(self):
        super().setUp()
        sf._resubmit_notice_day = ""
        self.addCleanup(setattr, sf, "_resubmit_notice_day", "")
        sf._upload_tokens.clear()
        self.addCleanup(sf._upload_tokens.clear)

    def set_used_at(self, token, when: datetime.datetime):
        import sqlalchemy as sa

        async def _u():
            async with db.session_scope() as s:
                await s.execute(sa.update(sl.shindan_link)
                                .where(sl.shindan_link.c.token == token)
                                .values(used_at=when))
        self.q(_u())

    def post_plain(self, ip="203.0.113.10", **over):
        data = dict(VALID)
        data.update(over)
        return self.client.post("/shindan", data=data, headers={"X-Real-IP": ip})


# ── 凍結文言・定数 ───────────────────────────────────────────────────────────────
class TestConstants(unittest.TestCase):
    def test_windows_and_texts(self):
        self.assertEqual(sf.RESUBMIT_WINDOW_SEC, 600)
        self.assertEqual(sf.PLAIN_DEDUPE_WINDOW_SEC, 60)
        self.assertEqual(sf.MAX_RECENT_PLAIN, 5000)
        self.assertEqual(hashlib.sha256(sf.RESUBMIT_DONE_TEXT.encode("utf-8")).hexdigest(),
                         PIN_RESUBMIT_DONE)
        self.assertEqual(hashlib.sha256(sf.PLAIN_PENDING_TEXT.encode("utf-8")).hexdigest(),
                         PIN_PLAIN_PENDING)
        # 文言は固定（入力値を含まない・LINE へ戻す案内）
        self.assertIn("送信済み", sf.RESUBMIT_DONE_TEXT)
        self.assertIn("LINE", sf.RESUBMIT_DONE_TEXT)
        self.assertNotIn("{", sf.RESUBMIT_DONE_TEXT)
        self.assertIn("LINE", sf.PLAIN_PENDING_TEXT)
        self.assertNotIn("{", sf.PLAIN_PENDING_TEXT)

    def test_form_html_has_single_submit_guard_script_and_no_external_assets(self):
        html = sf._form_html()
        self.assertEqual(html.count("<script>"), 1)
        self.assertIn("data-sent", html)
        self.assertIn("b.disabled=true", html)
        self.assertNotIn("src=", html)                     # 外部アセットなし（従来どおり）
        self.assertIn("<button class=\"btn\" type=\"submit\">診断する</button>", html)

    def test_no_search_records_literal_in_module(self):
        # JIKOU-FORM-1 fix2 の pin（照会経路を本モジュールに持たない）は維持
        import inspect
        self.assertNotIn("search_records(", inspect.getsource(sf))


# ── 1. 使用済み token の再 POST ──────────────────────────────────────────────────
class TestUsedTokenResubmit(_Base):
    def test_same_token_twice_creates_once_second_is_done_page(self):
        token = self.issue()
        r1 = self.post_linked(token)
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        rid = self.fake.create_calls and list(self.fake.rows)[-1]
        self.assertIsNotNone(self.used_at(token))
        before_tokens = set(sf._upload_tokens)
        r2 = self.post_linked(token)                        # 2 回目（同じ cookie・同内容）
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)    # create は 1 回のまま
        self.assertEqual(self.fake.update_calls, [])        # 更新もしない
        self.assertNotIn("受付番号", r2.text)                # k なし経路へ落ちていない
        self.assertIn(sf.LINKED_DONE_TEXT, r2.text)          # 完了画面
        self.assertIn(sf.PHOTO_ROUTE, r2.text)               # 写真導線は同じレコードへ
        new = set(sf._upload_tokens) - before_tokens
        self.assertEqual(len(new), 1)
        self.assertEqual(sf._upload_tokens[new.pop()][1], rid)
        self.notify_admin.assert_not_awaited()               # 内容同一=通知なし
        self.assertIn("resubmit same content (no write)", self.cap.text())
        self.assertNotIn(token, self.cap.text())
        self.assertEqual(self.rows()[0]["token"], token)     # 行は 1 本のまま

    def test_query_k_resubmit_also_handled(self):
        token = self.issue()
        self.client.post("/shindan?k=" + token, data=VALID)
        r2 = self.client.post("/shindan?k=" + token, data=VALID)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertNotIn("受付番号", r2.text)

    def test_different_content_is_ignored_and_notified_once_a_day(self):
        token = self.issue()
        self.post_linked(token)
        rid = list(self.fake.rows)[-1]
        r2 = self.post_linked(token, creditor="別の債権者", last_pay="5年以内")   # 内容相違（B 判定）
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(self.fake.update_calls, [])        # 上書きしない
        self.assertEqual(self.fake.val(rid, "問い合わせ業者名"), VALID["creditor"])
        self.assertEqual(self.fake.val(rid, "診断パターン"), "A")
        # 完了画面は登録済みの判定（A）で表示＝新しい回答（B）を反射しない
        self.assertIn(sf.result_text_linked("A").replace("\n", "<br>"), r2.text)
        self.assertNotIn(sf.result_text_linked("B").replace("\n", "<br>"), r2.text)
        self.assertNotIn("別の債権者", r2.text)
        self.notify_admin.assert_awaited_once()
        text = self.notify_admin.await_args.args[0]
        self.assertIn("要確認", text)
        self.assertIn(f"案件レコードNo:{rid}", text)
        self.assertNotIn("別の債権者", text)                 # 回答本文を載せない
        self.assertNotIn(USER, text)
        self.assertEqual(self.notify_admin.await_args.kwargs.get("throttle_key"),
                         sf.RESUBMIT_DIFF_NOTICE_KEY)
        # 同日 2 回目の相違再送は通知しない（1 日 1 回）
        self.post_linked(token, creditor="さらに別")
        self.notify_admin.assert_awaited_once()
        self.assertIn("resubmit notice skipped (already sent today)", self.cap.text())
        self.assertEqual(len(self.fake.create_calls), 1)
        # 日付が変われば再度 1 回
        sf._resubmit_notice_day = "2000-01-01"
        self.post_linked(token, creditor="さらに別")
        self.assertEqual(self.notify_admin.await_count, 2)

    def test_outside_window_is_fixed_done_page_no_write(self):
        token = self.issue()
        self.post_linked(token)
        self.set_used_at(token, sl._now() - datetime.timedelta(seconds=sf.RESUBMIT_WINDOW_SEC + 1))
        r2 = self.post_linked(token)
        self.assertEqual(r2.status_code, 200)
        self.assertIn(sf.RESUBMIT_DONE_TEXT, r2.text)
        self.assertNotIn("受付番号", r2.text)
        self.assertNotIn(sf.PHOTO_ROUTE, r2.text)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(self.fake.update_calls, [])
        self.notify_admin.assert_not_awaited()
        self.assertIn("resubmit outside window (no write)", self.cap.text())

    def test_within_window_boundary_uses_record(self):
        token = self.issue()
        self.post_linked(token)
        self.set_used_at(token, sl._now() - datetime.timedelta(seconds=sf.RESUBMIT_WINDOW_SEC - 5))
        r2 = self.post_linked(token)
        self.assertIn(sf.LINKED_DONE_TEXT, r2.text)
        self.assertEqual(len(self.fake.create_calls), 1)

    def test_used_token_but_record_missing_or_ambiguous_is_done_page(self):
        token = self.issue()
        self.post_linked(token)
        rid = list(self.fake.rows)[-1]
        # 本人レコードが消えた（弁護士が削除）→ 固定ページ・作成しない
        self.fake.rows.pop(rid)
        r2 = self.post_linked(token)
        self.assertIn(sf.RESUBMIT_DONE_TEXT, r2.text)
        self.assertEqual(len(self.fake.create_calls), 1)
        # 同じ uid が 2 件（分裂）→ 書かない・固定ページ
        self.fake.add("300")
        self.fake.add("301")
        r3 = self.post_linked(token)
        self.assertIn(sf.RESUBMIT_DONE_TEXT, r3.text)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(self.fake.update_calls, [])
        self.assertIn("resubmit record not resolved (no write) method=", self.cap.text())

    def test_used_token_with_honeypot_or_invalid_choice_unchanged(self):
        token = self.issue()
        self.post_linked(token)
        self.assertEqual(self.post_linked(token, website="x").status_code, 200)   # honeypot=無言破棄
        self.assertEqual(self.post_linked(token, borrow="?").status_code, 400)
        self.assertEqual(len(self.fake.create_calls), 1)

    def test_lookup_failure_falls_back_to_no_link_but_plain_dedupe_still_applies(self):
        # 使用済み照合が例外 → 「link なし」扱い（従来の縮退）。ただし k なし経路でも
        # 同一内容の連続 POST は 3 で 1 件に抑止される
        token = self.issue()
        self.post_linked(token)
        with patch.object(sl, "lookup_used", AsyncMock(side_effect=RuntimeError("db"))), \
                patch.object(sf, "_draw_number", return_value="012345"):
            r2 = self.client.post("/shindan", data=VALID, cookies={sf.LINK_COOKIE: token},
                                  headers={"X-Real-IP": "198.51.100.1"})
            r3 = self.client.post("/shindan", data=VALID, cookies={sf.LINK_COOKIE: token},
                                  headers={"X-Real-IP": "198.51.100.1"})
        self.assertEqual((r2.status_code, r3.status_code), (200, 200))
        self.assertIn("used link lookup failed", self.cap.text())
        self.assertEqual(len(self.fake.create_calls), 2)        # 縮退時の k なし作成 1 + 1 回目
        self.assertIn("受付番号：012345", r3.text)              # 2 本目は 1 本目と同じ番号

    def test_unused_valid_token_path_unchanged(self):
        # 未使用 token の linked write は従来どおり（T6）＝ lookup_used を経由しない
        token = self.issue()
        with patch.object(sl, "lookup_used", AsyncMock(side_effect=AssertionError("must not be called"))):
            r = self.post_linked(token)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIsNotNone(self.used_at(token))

    def test_unknown_token_cookie_is_plain_as_before(self):
        with patch.object(sf, "_draw_number", return_value="012345"):
            r = self.client.post("/shindan", data=VALID, cookies={sf.LINK_COOKIE: "nosuchtoken"})
        self.assertIn("受付番号：012345", r.text)
        self.assertEqual(self.fake.create_calls[0]["LINEユーザーID"], "")

    def test_get_with_used_token_still_deletes_cookie(self):
        # SLL-03 不変: GET は使用済み token を無効として cookie を消す（別の利用者へ紐付かない）
        token = self.issue()
        self.post_linked(token)
        r = self.client.get("/shindan", params={"k": token})
        self.assertIn("Max-Age=0", r.headers.get("set-cookie", ""))


# ── lookup_used の意味論 ─────────────────────────────────────────────────────────
class TestLookupUsed(_Base):
    def test_semantics(self):
        self.assertIsNone(self.q(sl.lookup_used("")))
        self.assertIsNone(self.q(sl.lookup_used("nosuchtoken")))
        token = self.issue()
        self.assertIsNone(self.q(sl.lookup_used(token)))           # 未使用
        claimed = self.q(sl.claim(token))
        self.assertIsNone(self.q(sl.lookup_used(token)))           # 予約中も未使用
        self.assertTrue(self.q(sl.mark_used(token, claimed)))
        used = self.q(sl.lookup_used(token))
        self.assertEqual((used["token"], used["line_user_id"]), (token, USER))
        self.assertIsNotNone(used["used_at"].tzinfo)
        # 期限切れでも使用済みなら返す（送信済み扱い）
        self.set_used_at(token, sl._now() - datetime.timedelta(days=40))
        self.assertIsNotNone(self.q(sl.lookup_used(token)))
        self.assertIsNone(self.q(sl.lookup(token)))                # 有効 token としては無効


# ── 3. k なし経路の同一内容・同一 IP の連続 POST ───────────────────────────────────
class TestPlainDedupe(_Base):
    def test_same_content_same_ip_twice_creates_once_same_number(self):
        with patch.object(sf, "_draw_number", return_value="012345"):
            r1 = self.post_plain()
            r2 = self.post_plain()
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIn("受付番号：012345", r1.text)
        self.assertIn("受付番号：012345", r2.text)                 # 同じ完了画面（同じ番号）
        self.assertIn(sf.PHOTO_ROUTE, r2.text)
        rid = list(self.fake.rows)[-1]
        self.assertTrue(all(v[1] == rid for v in sf._upload_tokens.values()))
        self.assertEqual(self.notify_biz.await_count, 1)          # 受付通知も 1 回
        self.assertIn("duplicate post (no write) record_id=", self.cap.text())

    def test_different_content_or_ip_creates_again(self):
        self.post_plain()
        self.post_plain(creditor="別の債権者")
        self.post_plain(ip="203.0.113.11")
        self.assertEqual(len(self.fake.create_calls), 3)

    def test_window_expiry_creates_again(self):
        self.post_plain()
        key = next(iter(sf._recent_plain))
        sf._recent_plain[key].ts -= sf.PLAIN_DEDUPE_WINDOW_SEC + 1
        self.post_plain()
        self.assertEqual(len(self.fake.create_calls), 2)

    def test_failed_first_does_not_block_second(self):
        self.fake.create_fail_next = 1
        r1 = self.post_plain()
        self.assertEqual(r1.status_code, 500)
        self.assertEqual(sf._recent_plain, {})                    # 失敗は記憶しない
        r2 = self.post_plain()
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 2)

    def test_concurrent_identical_posts_wait_for_first(self):
        key = sf._plain_dedupe_key("iphash", VALID)
        gate = asyncio.Event()
        real = self.fake.create_record

        async def _slow(app, fields):
            await gate.wait()
            return await real(app, fields)

        async def _two():
            early, pattern, fields = sf._prepare_submission(dict(VALID))
            t1 = asyncio.create_task(sf._handle_submit_plain(pattern, fields, key))
            await asyncio.sleep(0.05)
            t2 = asyncio.create_task(sf._handle_submit_plain(pattern, fields, key))
            await asyncio.sleep(0.05)
            gate.set()
            return await asyncio.gather(t1, t2)
        with patch.object(sf.hub_kintone, "create_record", _slow), \
                patch.object(sf, "_draw_number", return_value="777777"):
            r1, r2 = _run(_two())
        db.reset_for_tests()
        self.assertEqual(len(self.fake.create_calls), 1)
        for r in (r1, r2):
            self.assertEqual(r.status_code, 200)
            self.assertIn("受付番号：777777", r.body.decode("utf-8"))

    def test_pending_timeout_returns_fixed_page_without_create(self):
        key = sf._plain_dedupe_key("iphash", VALID)
        entry = sf._PlainEntry(sf.time.time())
        sf._recent_plain_put(key, entry)                          # 1 回目が処理中のまま
        with patch.object(sf, "_PLAIN_WAIT_SEC", 0.05):
            early, pattern, fields = sf._prepare_submission(dict(VALID))
            r = _run(sf._handle_submit_plain(pattern, fields, key))
        self.assertEqual(r.status_code, 200)
        self.assertIn(sf.PLAIN_PENDING_TEXT, r.body.decode("utf-8"))
        self.assertEqual(self.fake.create_calls, [])

    def test_key_holds_only_hashes_and_is_bounded(self):
        key = sf._plain_dedupe_key("k", {"creditor": " 秘匿 ", "borrow": "b",
                                         "last_pay": "l", "court_doc": "c"})
        self.assertEqual(len(key), 64)
        self.assertNotIn("秘匿", key)
        self.assertEqual(key, sf._plain_dedupe_key("k", {"creditor": "秘匿", "borrow": "b",
                                                          "last_pay": "l", "court_doc": "c"}))
        for i in range(sf.MAX_RECENT_PLAIN + 3):
            sf._recent_plain_put(f"k{i}", sf._PlainEntry(0.0))
        self.assertEqual(len(sf._recent_plain), sf.MAX_RECENT_PLAIN)
        self.assertNotIn("k0", sf._recent_plain)

    def test_direct_call_without_key_is_unchanged(self):
        early, pattern, fields = sf._prepare_submission(dict(VALID))
        with patch.object(sf, "_draw_number", return_value="012345"):
            _run(sf._handle_submit_plain(pattern, fields))
            _run(sf._handle_submit_plain(pattern, fields))
        db.reset_for_tests()
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertEqual(sf._recent_plain, {})


if __name__ == "__main__":
    unittest.main()
