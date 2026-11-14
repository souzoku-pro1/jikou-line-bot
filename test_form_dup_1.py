"""FORM-DUP-1（fix1・Codex R-FD1 BF-01〜04）: 診断フォームの二重 POST による App 21 重複の抑止。

実測（2026-11-12 調査・74→75→76 は 1.4 秒／1.8 秒差・77→78 は 20 秒差）: 本人専用
リンク経路の 1 回目 POST が token を使用済みにし、同じブラウザからの 2 回目 POST が
「link invalid」→ k なし経路へ落ちて受付番号付き・LINEユーザーID 空の双子を作っていた。

固定する内容（fix1 で設計の芯を nonce 方式に変更）:
 - 再送の識別はフォーム表示時に発行する送信操作 ID（nonce: 署名付き・1 回限り・有効 24 時間
   （fix6 BF-09 で 30 分から変更）・hidden 項目）で行う。IP+回答の dedupe・使用済み token からの本人レコード検索はしない。
 - BF-02: k なし経路も本人専用リンク経路も、同じ nonce の再 POST だけを「同一送信」とみなし
   1 回目と同じ完了画面（同じ受付番号／同じレコード）。nonce なし・不正・別 nonce は
   通常どおり新規（同一 IP の別人＝別 nonce は別案件）。24 時間超（署名正）は固定ページ（fix6 BF-09）。
 - BF-03: 使用済み token の再 POST は nonce 一致のときだけ同じ完了画面。不一致・なしは固定の
   「送信済み」ページだけ（診断結果・写真トークン・受付番号なし）。k なし経路にも落とさない。
 - BF-01: token 照合（lookup／lookup_used）の取得不能は不存在と分け、固定ページで終了
   （作成・更新・採番なし）。
 - BF-04: 相違の再送に業務 LINE 通知はしない（件数を INFO ログに出すだけ）。承認台帳を迂回する
   notify 呼出が増えていないことを静的検査で固定する。
 - 既存の linked write（T5/T6/SLL-02）と FORM-2 の try_link のテストは不変。
"""

import ast
import asyncio
import datetime
import hashlib
import inspect
import os
import re
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
_HIDDEN_RE = re.compile(r'<input type="hidden" name="n" value="([^"]*)">')


def _definite_failure():
    """FORM-DUP-1 fix7 BF-11: 取り直しを許す「確定失敗」（kintone の明示的拒否＝4xx）。
    fake の既定（5xx）は「結果不明」になり予約が解放されないので、取り直しの検査はこれを差す。"""
    return sf.hub_kintone.KintoneError(403, "GAIA_AP15", "forbidden")


class _Base(_FormLinkBase):
    def setUp(self):
        super().setUp()
        sf._nonce_results.clear()
        self.addCleanup(sf._nonce_results.clear)
        sf._upload_tokens.clear()
        self.addCleanup(sf._upload_tokens.clear)

    def get_nonce(self, **params) -> str:
        r = self.client.get("/shindan", params=params or None)
        m = _HIDDEN_RE.search(r.text)
        self.assertIsNotNone(m)
        return m.group(1)

    def post_plain(self, ip="203.0.113.10", nonce=None, **over):
        data = dict(VALID)
        data.update(over)
        if nonce:
            data[sf.NONCE_FIELD] = nonce
        return self.client.post("/shindan", data=data, headers={"X-Real-IP": ip})

    def post_linked_n(self, token, nonce=None, **over):
        data = dict(VALID)
        data.update(over)
        if nonce:
            data[sf.NONCE_FIELD] = nonce
        return self.client.post("/shindan", data=data, cookies={sf.LINK_COOKIE: token})

    def upload_rids(self):
        return {v[1] for v in sf._upload_tokens.values()}

    def assert_done_page_only(self, r):
        """BF-03 の固定ページ: 診断結果の再表示・写真トークン・受付番号が応答に含まれない。"""
        self.assertEqual(r.status_code, 200)
        self.assertIn(sf.RESUBMIT_DONE_TEXT, r.text)
        self.assertNotIn("受付番号", r.text)
        self.assertNotIn(sf.PHOTO_ROUTE, r.text)
        self.assertNotIn("診断結果", r.text)
        self.assertNotIn(sf.LINKED_DONE_TEXT, r.text)
        for p in sf.FROZEN_RESULTS:
            self.assertNotIn(sf.result_text_linked(p).replace("\n", "<br>"), r.text)


# ── 凍結文言・定数・静的検査 ────────────────────────────────────────────────────────
class TestConstants(unittest.TestCase):
    def test_windows_and_texts(self):
        self.assertEqual(sf.NONCE_FIELD, "n")
        self.assertEqual(sf.NONCE_TTL_SEC, 24 * 60 * 60)          # fix6 BF-09: 30 分 → 24 時間
        self.assertEqual(sf.MAX_NONCE_RESULTS, 5000)
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

    def test_form_html_carries_nonce_and_single_submit_guard_script(self):
        n = sf.issue_nonce()
        html = sf._form_html(n)
        self.assertEqual(html.count("<input type=\"hidden\""), 1)
        self.assertEqual(_HIDDEN_RE.search(html).group(1), n)
        self.assertEqual(html.count("<script>"), 1)
        self.assertIn("data-sent", html)
        self.assertIn("b.disabled=true", html)
        self.assertNotIn("src=", html)                     # 外部アセットなし（従来どおり）
        self.assertIn("<button class=\"btn\" type=\"submit\">診断する</button>", html)
        self.assertNotIn("<input type=\"hidden\"", sf._form_html())   # nonce なしは hidden なし

    def test_no_search_records_literal_in_module(self):
        # JIKOU-FORM-1 fix2 の pin（照会経路を本モジュールに持たない）は維持
        self.assertNotIn("search_records(", inspect.getsource(sf))

    def test_no_ip_or_answer_based_dedupe_and_no_user_record_lookup_for_resubmit(self):
        src = inspect.getsource(sf)
        for name in ("_recent_plain", "_plain_dedupe_key", "RESUBMIT_WINDOW_SEC",
                     "PLAIN_DEDUPE_WINDOW_SEC", "_notify_resubmit_differs",
                     "RESUBMIT_DIFF_NOTICE_KEY", "_handle_submit_used"):
            self.assertNotIn(name, src, name)
        # 本人レコード検索（find_user_record）は linked write の 1 箇所だけ
        self.assertEqual(src.count("find_user_record("), 1)

    def test_static_no_notify_call_bypassing_approval_baseline(self):
        """承認台帳を迂回する notify 呼出が増えていない（main の基線と同一）: notify_admin_line は
        採番失敗・写真添付失敗の 2 箇所（throttle_key 固定）・notify_business は受付通知 2 箇所。"""
        tree = ast.parse(inspect.getsource(sf))
        admin, business = [], 0
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
            if name == "notify_admin_line":
                keys = [kw.value.value for kw in node.keywords
                        if kw.arg == "throttle_key" and isinstance(kw.value, ast.Constant)]
                admin.append(keys[0] if keys else None)
            elif name == "notify_business":
                business += 1
            elif name in ("notify_admin", "push_text", "send_text", "push_message"):
                self.fail(f"unexpected notify call: {name}")
        self.assertEqual(sorted(admin), ["shindan_numbering", "shindan_photos"])
        self.assertEqual(business, 2)
        self.assertNotIn("notify", inspect.getsource(sl))   # hub.shindan_link は通知を持たない


# ── nonce の発行・検証 ───────────────────────────────────────────────────────────
class TestNonce(unittest.TestCase):
    def test_issue_and_verify(self):
        now = 1_800_000_000.0
        n = sf.issue_nonce(now)
        self.assertRegex(n, r"^[0-9]+\.[0-9a-f]{32}\.[0-9a-f]{32}$")
        self.assertEqual(sf.verify_nonce(n, now), n)
        self.assertEqual(sf.verify_nonce(n, now + sf.NONCE_TTL_SEC - 1), n)
        self.assertEqual(sf.verify_nonce(n, now + sf.NONCE_TTL_SEC), "")        # 期限切れ
        self.assertNotEqual(sf.issue_nonce(now), n)                              # 1 回ごとに別

    def test_invalid_forms_and_tampering(self):
        now = 1_800_000_000.0
        n = sf.issue_nonce(now)
        exp, rnd, sig = n.split(".")
        self.assertEqual(sf.verify_nonce("", now), "")
        self.assertEqual(sf.verify_nonce("x", now), "")
        self.assertEqual(sf.verify_nonce(f"{exp}.{rnd}.{'0' * 32}", now), "")      # 署名違い
        self.assertEqual(sf.verify_nonce(f"{int(exp) + 9999}.{rnd}.{sig}", now), "")  # 期限改ざん
        self.assertEqual(sf.verify_nonce(f"{exp}.{'f' * 32}.{sig}", now), "")      # 乱数改ざん
        self.assertEqual(sf.verify_nonce(n + "x", now), "")
        self.assertEqual(sf.verify_nonce(" " + n + " ", now), n)                  # 前後空白は許容

    def test_key_is_process_local_and_not_in_env(self):
        self.assertIsInstance(sf._NONCE_KEY, bytes)
        self.assertEqual(len(sf._NONCE_KEY), 32)
        self.assertNotIn(sf._NONCE_KEY.hex(), "".join(os.environ.values()))
        self.assertNotIn("NONCE", " ".join(k for k in os.environ))               # 新規 env なし


# ── BF-02: 同じ nonce の再 POST（k なし経路） ─────────────────────────────────────
class TestSameNoncePlain(_Base):
    def test_same_nonce_twice_creates_once_same_completion_page(self):
        n = self.get_nonce()
        with patch.object(sf, "_draw_number", return_value="012345"):
            r1 = self.post_plain(nonce=n)
            r2 = self.post_plain(nonce=n)
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIn("受付番号：012345", r1.text)
        self.assertIn("受付番号：012345", r2.text)                 # 同じ完了画面（同じ番号）
        self.assertIn(sf.PHOTO_ROUTE, r2.text)
        rid = list(self.fake.rows)[-1]
        self.assertEqual(self.upload_rids(), {rid})                # 写真導線は同じレコードへ
        self.assertEqual(self.notify_biz.await_count, 1)          # 受付通知も 1 回
        self.notify_admin.assert_not_awaited()
        self.assertIn("duplicate post (no write) kind=plain record_id=", self.cap.text())
        self.assertIn("differs=0", self.cap.text())

    def test_different_nonce_same_ip_same_content_is_another_case(self):
        # 同一 IP の別人（別 nonce）は別案件（IP+回答の dedupe はしない）
        self.post_plain(nonce=self.get_nonce())
        self.post_plain(nonce=self.get_nonce())
        self.assertEqual(len(self.fake.create_calls), 2)

    def test_no_nonce_or_invalid_is_new_and_stale_is_fixed_page(self):
        # fix6 BF-09（裁定変更）: 24 時間超（署名正）は新規ではなく固定ページ（410・作成 0・予約なし）
        with patch.object(sf, "_draw_number", side_effect=["000001", "000002", "000003", "000004"]):
            self.post_plain()                                      # nonce なし
            self.post_plain()
            self.post_plain(nonce="tampered.value")                # 不正
            stale = sf.issue_nonce(sf.time.time() - sf.NONCE_TTL_SEC - 10)
            r = self.post_plain(nonce=stale)                       # 24 時間超
        self.assertEqual(r.status_code, 410)
        self.assertIn(sf.STALE_TEXT, r.text)
        self.assertNotIn("受付番号", r.text)
        self.assertEqual(len(self.fake.create_calls), 3)          # 24 時間超は作成 0
        self.assertEqual(sf._nonce_results, {})                   # 無効・24 時間超の nonce は記憶しない

    def test_tampered_nonce_is_not_remembered_and_valid_one_is(self):
        n = self.get_nonce()
        self.post_plain(nonce=n)
        self.assertEqual(list(sf._nonce_results), [n])
        exp, rnd, sig = n.split(".")
        self.post_plain(nonce=f"{exp}.{rnd}.{'0' * 32}")
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertEqual(list(sf._nonce_results), [n])

    def test_different_content_same_nonce_shows_first_result_no_notify(self):
        # 戻る→回答を変えて再送信（同じ nonce）: 作成せず 1 回目の完了画面・相違は件数ログのみ
        n = self.get_nonce()
        with patch.object(sf, "_draw_number", return_value="012345"):
            r1 = self.post_plain(nonce=n)
            r2 = self.post_plain(nonce=n, creditor="別の債権者", last_pay="5年以内")   # B 判定の内容
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(self.fake.update_calls, [])
        self.assertIn("受付番号：012345", r2.text)
        self.assertEqual(self.fake.val(list(self.fake.rows)[-1], "問い合わせ業者名"), VALID["creditor"])
        self.assertNotIn("別の債権者", r2.text)
        self.assertEqual(r1.text.split("/shindan/photos/")[0], r2.text.split("/shindan/photos/")[0])
        self.notify_admin.assert_not_awaited()                     # BF-04: 通知なし
        self.assertIn("differs=3", self.cap.text())                # 診断パターン・業者名・最終返済日
        self.assertNotIn("別の債権者", self.cap.text())

    def test_differs_count_includes_pattern(self):
        n = self.get_nonce()
        self.post_plain(nonce=n)
        self.post_plain(nonce=n, last_pay="5年以内")               # 最終返済日 + 診断パターン
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIn("differs=2", self.cap.text())

    def test_failed_first_does_not_block_second(self):
        # fix7 BF-11: 後続を妨げないのは「確定失敗」だけ（結果不明は test_form_dup_1_fix7＝保持）
        n = self.get_nonce()
        self.fake.create_fail_next = 1
        self.fake.create_fail_error = _definite_failure()
        r1 = self.post_plain(nonce=n)
        self.assertEqual(r1.status_code, 500)
        self.assertEqual(sf._nonce_results, {})                   # 確定失敗は記憶しない
        r2 = self.post_plain(nonce=n)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertEqual(list(sf._nonce_results), [n])

    def test_concurrent_same_nonce_posts_wait_for_first(self):
        n = sf.issue_nonce()
        gate = asyncio.Event()
        real = self.fake.create_record

        async def _slow(app, fields):
            await gate.wait()
            return await real(app, fields)

        async def _two():
            t1 = asyncio.create_task(sf._handle_submit(dict(VALID), None, n))
            await asyncio.sleep(0.05)
            t2 = asyncio.create_task(sf._handle_submit(dict(VALID), None, n))
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
        n = sf.issue_nonce()
        sf._nonce_put(n, sf._NonceEntry(sf.time.time(), ()))     # 1 回目が処理中のまま
        with patch.object(sf, "_NONCE_WAIT_SEC", 0.05):
            r = self.post_plain(nonce=n)
        self.assertEqual(r.status_code, 200)
        self.assertIn(sf.PLAIN_PENDING_TEXT, r.text)
        self.assertEqual(self.fake.create_calls, [])
        self.assertIn("duplicate post pending (no write)", self.cap.text())

    def test_store_holds_only_hashes_and_is_bounded(self):
        n = self.get_nonce()
        self.post_plain(nonce=n, creditor="秘匿業者")
        entry = sf._nonce_results[n]
        self.assertEqual(entry.result[0], "plain")
        self.assertEqual(len(entry.sig), len(sf._RESUBMIT_COMPARE_FIELDS))
        self.assertTrue(all(len(h) == 64 for h in entry.sig))
        self.assertNotIn("秘匿業者", repr(entry.sig) + repr(entry.result))
        for i in range(sf.MAX_NONCE_RESULTS + 3):
            sf._nonce_put(f"k{i}", sf._NonceEntry(0.0, ()))
        self.assertEqual(len(sf._nonce_results), sf.MAX_NONCE_RESULTS)
        self.assertNotIn(n, sf._nonce_results)

    def test_honeypot_and_invalid_choice_before_nonce(self):
        n = self.get_nonce()
        self.assertEqual(self.post_plain(nonce=n, website="x").status_code, 200)   # 無言破棄
        self.assertEqual(self.post_plain(nonce=n, borrow="?").status_code, 400)
        self.assertEqual(self.fake.create_calls, [])
        self.assertEqual(sf._nonce_results, {})                   # 記憶もしない

    def test_rate_limit_counts_each_post_as_before(self):
        n = self.get_nonce()
        for _ in range(sf.RATE_LIMIT):
            self.assertEqual(self.post_plain(nonce=n).status_code, 200)
        self.assertEqual(self.post_plain(nonce=n).status_code, 429)
        self.assertEqual(len(self.fake.create_calls), 1)


# ── BF-02／BF-03: 本人専用リンク経路（同じ nonce・使用済み token） ────────────────────
class TestSameNonceLinked(_Base):
    def test_same_nonce_same_token_twice_creates_once_same_linked_page(self):
        token = self.issue()
        n = self.get_nonce(k=token)
        r1 = self.post_linked_n(token, n)
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIsNotNone(self.used_at(token))
        rid = list(self.fake.rows)[-1]
        r2 = self.post_linked_n(token, n)                           # 2 回目（同じ nonce・同じ cookie）
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)            # create は 1 回のまま
        self.assertEqual(self.fake.update_calls, [])
        self.assertNotIn("受付番号", r2.text)                        # k なし経路へ落ちていない
        self.assertIn(sf.LINKED_DONE_TEXT, r2.text)
        self.assertIn(sf.PHOTO_ROUTE, r2.text)
        self.assertEqual(self.upload_rids(), {rid})                 # 写真導線は同じレコードへ
        self.assertEqual(self.notify_biz.await_count, 1)
        self.notify_admin.assert_not_awaited()
        self.assertIn("duplicate post (no write) kind=linked record_id=", self.cap.text())
        self.assertNotIn(token, self.cap.text())
        self.assertEqual(len(self.rows()), 1)

    def test_same_nonce_with_existing_record_updates_once(self):
        self.fake.add("10", revision="5")
        token = self.issue()
        n = self.get_nonce(k=token)
        r1 = self.post_linked_n(token, n)
        r2 = self.post_linked_n(token, n)
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))
        self.assertEqual(self.fake.create_calls, [])
        self.assertEqual(len(self.fake.update_calls), 1)
        self.assertIn(sf.LINKED_DONE_TEXT, r2.text)
        self.assertEqual(self.upload_rids(), {"10"})

    def test_query_k_with_same_nonce_also_handled(self):
        token = self.issue()
        n = self.get_nonce(k=token)
        data = dict(VALID, **{sf.NONCE_FIELD: n})
        self.client.post("/shindan?k=" + token, data=data)
        r2 = self.client.post("/shindan?k=" + token, data=data)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIn(sf.LINKED_DONE_TEXT, r2.text)
        self.assertNotIn("受付番号", r2.text)

    def test_used_token_with_other_nonce_is_done_page_only(self):
        token = self.issue()
        self.post_linked_n(token, self.get_nonce(k=token))
        r = self.post_linked_n(token, self.get_nonce())              # 別 nonce
        self.assert_done_page_only(r)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(self.fake.update_calls, [])
        self.notify_admin.assert_not_awaited()
        self.assertIn("resubmit without matching nonce (no write) head=", self.cap.text())
        self.assertNotIn(token, self.cap.text())
        self.assertEqual(len(self.rows()), 1)                       # 行は 1 本のまま

    def test_used_token_without_nonce_or_invalid_is_done_page_only_and_stale_is_fixed_page(self):
        token = self.issue()
        self.post_linked_n(token, self.get_nonce(k=token))
        before = set(sf._upload_tokens)
        self.assert_done_page_only(self.post_linked_n(token))                        # なし
        self.assert_done_page_only(self.post_linked_n(token, "bad.nonce"))           # 不正
        stale = sf.issue_nonce(sf.time.time() - sf.NONCE_TTL_SEC - 10)
        r = self.post_linked_n(token, stale)                       # 24 時間超: fix6 BF-09 で固定ページ（送信済みページではない）
        self.assertEqual(r.status_code, 410)
        self.assertIn(sf.STALE_TEXT, r.text)
        self.assertNotIn(sf.RESUBMIT_DONE_TEXT, r.text)
        self.assertNotIn("受付番号", r.text)
        self.assertNotIn(sf.PHOTO_ROUTE, r.text)
        self.assert_done_page_only(self.post_linked_n(token, creditor="別の債権者"))  # 内容相違でも同じ
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual(set(sf._upload_tokens), before)             # 写真トークン発行なし

    def test_used_token_same_nonce_after_memory_loss_is_done_page(self):
        # 再起動などで 1 回目の記憶が無い（または別 worker）: nonce 一致を確認できない → 固定ページ
        token = self.issue()
        n = self.get_nonce(k=token)
        self.post_linked_n(token, n)
        sf._nonce_results.clear()
        self.assert_done_page_only(self.post_linked_n(token, n))
        self.assertEqual(len(self.fake.create_calls), 1)

    def test_used_token_first_posted_without_nonce_then_with_nonce_is_done_page(self):
        token = self.issue()
        self.post_linked_n(token)                                    # 1 回目が nonce なし（旧フォーム）
        self.assert_done_page_only(self.post_linked_n(token, self.get_nonce()))
        self.assertEqual(len(self.fake.create_calls), 1)

    def test_used_token_with_honeypot_or_invalid_choice_unchanged(self):
        token = self.issue()
        self.post_linked_n(token, self.get_nonce(k=token))
        self.assertEqual(self.post_linked_n(token, website="x").status_code, 200)   # honeypot=無言破棄
        self.assertEqual(self.post_linked_n(token, borrow="?").status_code, 400)
        self.assertEqual(len(self.fake.create_calls), 1)

    def test_unused_valid_token_path_does_not_consult_lookup_used(self):
        token = self.issue()
        with patch.object(sl, "lookup_used", AsyncMock(side_effect=AssertionError("must not be called"))):
            r = self.post_linked_n(token, self.get_nonce(k=token))
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIsNotNone(self.used_at(token))

    def test_unknown_token_cookie_is_plain_as_before(self):
        with patch.object(sf, "_draw_number", return_value="012345"):
            r = self.client.post("/shindan", data=VALID, cookies={sf.LINK_COOKIE: "nosuchtoken"})
        self.assertIn("受付番号：012345", r.text)
        self.assertEqual(self.fake.create_calls[0]["LINEユーザーID"], "")
        self.assertIn("link invalid head=nosu", self.cap.text())

    def test_get_with_used_token_still_deletes_cookie(self):
        # SLL-03 不変: GET は使用済み token を無効として cookie を消す（別の利用者へ紐付かない）
        token = self.issue()
        self.post_linked_n(token, self.get_nonce(k=token))
        r = self.client.get("/shindan", params={"k": token})
        self.assertIn("Max-Age=0", r.headers.get("set-cookie", ""))


# ── BF-01: 照合の取得不能は不存在と分ける（固定ページ・作成・更新・採番なし） ─────────
class TestLookupFailure(_Base):
    def test_lookup_used_failure_is_fixed_page_no_create(self):
        token = self.issue()
        self.post_linked_n(token, self.get_nonce(k=token))
        with patch.object(sl, "lookup_used", AsyncMock(side_effect=RuntimeError("db"))):
            r2 = self.post_linked_n(token, self.get_nonce())
            r3 = self.post_linked_n(token)
        self.assertEqual((r2.status_code, r3.status_code), (500, 500))
        self.assertNotIn("受付番号", r2.text + r3.text)
        self.assertEqual(len(self.fake.create_calls), 1)            # 障害後に双子を作らない
        self.assertEqual(self.fake.update_calls, [])
        self.assertIn("used link lookup failed (fixed page, no write)", self.cap.text())
        self.assertNotIn(token, self.cap.text())
        self.notify_admin.assert_not_awaited()

    def test_lookup_failure_with_token_is_fixed_page_no_create(self):
        token = self.issue()
        with patch.object(sl, "lookup", AsyncMock(side_effect=RuntimeError("db"))):
            r = self.post_linked_n(token, self.get_nonce())
            r_q = self.client.post("/shindan?k=" + token, data=VALID)
        self.assertEqual((r.status_code, r_q.status_code), (500, 500))
        self.assertEqual(self.fake.create_calls, [])
        self.assertIsNone(self.used_at(token))
        self.assertIn("link lookup failed (fixed page, no write)", self.cap.text())
        # 復旧後は同じ token で通常どおり紐付け
        self.assertEqual(self.post_linked_n(token, self.get_nonce()).status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)

    def test_same_nonce_resubmit_served_from_memory_even_if_lookup_fails(self):
        token = self.issue()
        n = self.get_nonce(k=token)
        self.post_linked_n(token, n)
        with patch.object(sl, "lookup", AsyncMock(side_effect=RuntimeError("db"))), \
                patch.object(sl, "lookup_used", AsyncMock(side_effect=RuntimeError("db"))):
            r2 = self.post_linked_n(token, n)
        self.assertEqual(r2.status_code, 200)
        self.assertIn(sf.LINKED_DONE_TEXT, r2.text)
        self.assertEqual(len(self.fake.create_calls), 1)

    def test_no_token_lookup_failure_irrelevant_plain_as_before(self):
        with patch.object(sl, "lookup", AsyncMock(side_effect=RuntimeError("db"))), \
                patch.object(sf, "_draw_number", return_value="012345"):
            r = self.post_plain(nonce=self.get_nonce())
        self.assertEqual(r.status_code, 200)
        self.assertIn("受付番号：012345", r.text)


# ── lookup_used の意味論 ─────────────────────────────────────────────────────────
class TestLookupUsed(_Base):
    def set_used_at(self, token, when: datetime.datetime):
        import sqlalchemy as sa

        async def _u():
            async with db.session_scope() as s:
                await s.execute(sa.update(sl.shindan_link)
                                .where(sl.shindan_link.c.token == token)
                                .values(used_at=when))
        self.q(_u())

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

    def test_expired_used_token_post_is_done_page_not_plain(self):
        token = self.issue()
        self.post_linked_n(token, self.get_nonce(k=token))
        self.set_used_at(token, sl._now() - datetime.timedelta(days=40))
        self.assert_done_page_only(self.post_linked_n(token))
        self.assertEqual(len(self.fake.create_calls), 1)


if __name__ == "__main__":
    unittest.main()
