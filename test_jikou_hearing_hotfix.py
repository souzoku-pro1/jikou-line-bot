"""JIKOU-HEARING-HOTFIX-1: 時効ヒアリング第 2 段階（KINTONE_UPDATE）の書込先解決と
マーカー除去の固定。

事象（2026-09-13 本番実測）: 書込先が in-memory の kintone_record_ids に依存し、
デプロイで台帳が消えた後の案件・フォーム経由の案件で未書込。マーカー除去も走らず
送信ゲートで降格→App 29 起票。

固定する仕様（票の逐語）:
1. record_id の解決順序: in-memory 台帳に**あれば使う** → 無ければ LINEユーザーID で
   App 21 を検索。ちょうど 1 件=採用。0 件=書かない。複数件=最新更新を採用せず
   書かない（要確認通知）。検索失敗=書かない
2. 書込は空欄のみ・$revision CAS・409 は再取得 1 回。既に値がある欄は上書きしない
3. マーカーは書込の成否にかかわらず送信前に必ず除去。除去できない（閉じタグ不整合
   等）ときは従来どおり送信せず降格（安全側維持）
4. 結果（書いた欄名・書けなかった欄名・解決方法）を業務 LINE に通知。値は載せない
5. 再起動後（台帳空）でも同じ結果
"""

import asyncio
import io
import logging
import os
import re
import unittest
from unittest.mock import AsyncMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token",
    "DOCUMENT_WEBHOOK_SECRET": "doc-secret",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

import main  # noqa: E402
from hub import hearing_update as hu  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import notify as hub_notify  # noqa: E402

USER = "Uhotfix1user00000000000000000001"
NAME = "山田太郎"
ADDR = "埼玉県川口市西青木9-9-9"
BIRTH = "1980-01-01"
PHONE = "090-0000-0000"
MAIL = "taro@example.com"
VALUES = (NAME, ADDR, BIRTH, PHONE, MAIL)

UPDATE_JSON = (
    '{"顧客名": "' + NAME + '", "住所": "' + ADDR + '", "生年月日": "' + BIRTH
    + '", "電話番号": "' + PHONE + '", "メールアドレス": "' + MAIL + '"}')
REPLY_HEAD = "ご回答ありがとうございます。担当弁護士が確認のうえご連絡いたします。"
MARKER_REPLY = REPLY_HEAD + "\n[KINTONE_UPDATE]\n" + UPDATE_JSON + "\n[/KINTONE_UPDATE]"
BROKEN_REPLY = REPLY_HEAD + "\n[KINTONE_UPDATE]\n{not json\n[/KINTONE_UPDATE]"
UNCLOSED_REPLY = REPLY_HEAD + "\n[KINTONE_UPDATE]\n" + UPDATE_JSON
RECORD_BROKEN_REPLY = REPLY_HEAD + "\n[KINTONE_RECORD]\n{not json\n[/KINTONE_RECORD]"


def _run(coro):
    return asyncio.run(coro)


# ── App 21 の in-memory fake（_wrap 境界・$revision CAS・LINEユーザーID 検索を模す） ──
class _FakeApp21:
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self.search_calls: list[str] = []
        self.update_calls: list[tuple] = []
        self.get_calls: list[str] = []
        self.conflict_next = 0     # update を 409 で落とす残回数（行は進めない）
        self.get_fail_next = 0
        self.search_fail = False

    def add(self, rid: str, *, user_id: str = USER, revision: str = "3", **filled):
        rec = {"$id": {"value": rid}, "$revision": {"value": revision},
               "LINEユーザーID": {"value": user_id},
               "status": {"value": "問い合わせ"}}
        for code in hu.UPDATE_FIELDS:
            rec[code] = {"value": ""}
        for k, v in filled.items():
            rec[k] = {"value": v}
        self.rows[rid] = rec
        return rec

    @staticmethod
    def _reject_double_wrap(fields):
        for code, v in (fields or {}).items():
            if isinstance(v, dict) and "value" in v:
                raise AssertionError(f"double-wrapped payload: {code}={v!r}")

    async def search(self, app, query, fields=None):
        self.search_calls.append(query)
        if self.search_fail:
            raise hub_kintone.KintoneError(500, "GAIA_XX", "down")
        m = re.search(r'LINEユーザーID = "([^"]+)"', query)
        assert m, query
        limit = int(re.search(r"limit (\d+)", query).group(1))
        out = [dict(r) for r in self.rows.values()
               if r["LINEユーザーID"]["value"] == m.group(1)]
        out.sort(key=lambda r: int(r["$id"]["value"]))
        out = out[:limit]
        if fields:
            out = [{k: v for k, v in r.items() if k in fields} for r in out]
        return out

    async def get_by_id(self, app, record_id):
        self.get_calls.append(record_id)
        if self.get_fail_next > 0:
            self.get_fail_next -= 1
            raise hub_kintone.KintoneError(500, "GAIA_XX", "down")
        row = self.rows.get(record_id)
        if row is None:
            raise hub_kintone.KintoneError(404, "GAIA_RE01", "not found")
        return {k: dict(v) for k, v in row.items()}

    async def update(self, app, record_id, fields, revision=None):
        self._reject_double_wrap(fields)
        self.update_calls.append((record_id, dict(fields), revision))
        row = self.rows[record_id]
        if self.conflict_next > 0:
            self.conflict_next -= 1
            # 他者が先に更新した体で revision だけ進める
            row["$revision"] = {"value": str(int(row["$revision"]["value"]) + 1)}
            raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        if revision is not None and str(revision) != row["$revision"]["value"]:
            raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        for k, v in fields.items():
            row[k] = {"value": v}
        row["$revision"] = {"value": str(int(row["$revision"]["value"]) + 1)}

    async def get_by_user(self, user_id):
        for r in self.rows.values():
            if r["LINEユーザーID"]["value"] == user_id:
                return dict(r)
        return None

    def values(self, rid, code):
        return self.rows[rid][code]["value"]


class _ModuleBase(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeApp21()
        self.notify = AsyncMock(return_value=True)
        patches = [
            patch.object(hu.hub_kintone, "search_records", self.fake.search),
            patch.object(hu.hub_kintone, "get_record", self.fake.get_by_id),
            patch.object(hu.hub_kintone, "update_record", self.fake.update),
            patch.object(hu.notify, "notify_admin_line", self.notify),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def notice(self) -> str:
        self.assertEqual(self.notify.await_count, 1)
        return self.notify.await_args.args[0]

    def assert_no_values(self, text: str):
        for v in VALUES:
            self.assertNotIn(v, text)


# ── 1. record_id の解決順序 ──────────────────────────────────────────────────────
class TestResolve(_ModuleBase):
    def test_memory_first_no_search(self):
        self.fake.add("10")
        self.assertEqual(_run(hu.resolve_record_id(USER, "77")), ("77", hu.METHOD_MEMORY))
        self.assertEqual(self.fake.search_calls, [])

    def test_search_exactly_one(self):
        self.fake.add("10")
        self.assertEqual(_run(hu.resolve_record_id(USER, None)), ("10", hu.METHOD_SEARCH))
        self.assertEqual(len(self.fake.search_calls), 1)
        self.assertIn('LINEユーザーID = "' + USER + '"', self.fake.search_calls[0])
        self.assertIn("limit 2", self.fake.search_calls[0])   # 複数件検知のため 2 件取る

    def test_zero_rows_none(self):
        self.assertEqual(_run(hu.resolve_record_id(USER, "")), ("", hu.METHOD_NONE))

    def test_two_rows_ambiguous_not_latest(self):
        self.fake.add("10")
        self.fake.add("11")
        self.assertEqual(_run(hu.resolve_record_id(USER, None)), ("", hu.METHOD_AMBIGUOUS))

    def test_search_failure(self):
        self.fake.search_fail = True
        self.assertEqual(_run(hu.resolve_record_id(USER, None)),
                         ("", hu.METHOD_SEARCH_FAILED))

    def test_constants_pinned(self):
        self.assertEqual(hu.CAS_REFETCH, 1)
        self.assertEqual(hu.UPDATE_FIELDS,
                         frozenset({"顧客名", "住所", "生年月日", "電話番号", "メールアドレス"}))


# ── 2. 書込（空欄のみ・CAS・409 再取得 1 回） ────────────────────────────────────
class TestApplyUpdate(_ModuleBase):
    FIELDS = {"顧客名": NAME, "住所": ADDR, "生年月日": BIRTH,
              "電話番号": PHONE, "メールアドレス": MAIL}

    def test_writes_only_empty_fields_with_revision(self):
        self.fake.add("10", revision="5", 電話番号="手入力の番号")
        r = _run(hu.apply_update("10", self.FIELDS))
        self.assertEqual(r["outcome"], hu.OUTCOME_UPDATED)
        self.assertEqual(r["written"], ["メールアドレス", "住所", "生年月日", "顧客名"])
        self.assertEqual(r["preexisting"], ["電話番号"])
        self.assertEqual(len(self.fake.update_calls), 1)
        rid, fields, rev = self.fake.update_calls[0]
        self.assertEqual((rid, rev), ("10", "5"))
        self.assertNotIn("電話番号", fields)
        self.assertEqual(self.fake.values("10", "電話番号"), "手入力の番号")   # 不変
        self.assertEqual(self.fake.values("10", "顧客名"), NAME)

    def test_closed_set_drops_unknown_keys(self):
        self.fake.add("10")
        r = _run(hu.apply_update("10", {"顧客名": NAME, "status": "受任",
                                        "LINEユーザーID": "Uother", "問い合わせ業者名": "X"}))
        self.assertEqual(r["written"], ["顧客名"])
        self.assertEqual(r["dropped"], ["LINEユーザーID", "status", "問い合わせ業者名"])
        self.assertEqual(self.fake.update_calls[0][1], {"顧客名": NAME})
        self.assertEqual(self.fake.values("10", "status"), "問い合わせ")

    def test_all_preexisting_is_noop(self):
        self.fake.add("10", **self.FIELDS)
        r = _run(hu.apply_update("10", self.FIELDS))
        self.assertEqual(r["outcome"], hu.OUTCOME_NOOP)
        self.assertEqual(self.fake.update_calls, [])

    def test_empty_values_not_written(self):
        self.fake.add("10")
        r = _run(hu.apply_update("10", {"顧客名": "  ", "住所": None, "生年月日": BIRTH}))
        self.assertEqual(self.fake.update_calls[0][1], {"生年月日": BIRTH})
        self.assertEqual(r["written"], ["生年月日"])

    def test_cas_409_refetch_once_then_success(self):
        self.fake.add("10", revision="5")
        self.fake.conflict_next = 1
        r = _run(hu.apply_update("10", self.FIELDS))
        self.assertEqual(r["outcome"], hu.OUTCOME_UPDATED)
        self.assertEqual(self.fake.get_calls, ["10", "10"])          # 再取得 1 回
        self.assertEqual([c[2] for c in self.fake.update_calls], ["5", "6"])

    def test_cas_409_twice_unconverged_no_overwrite(self):
        self.fake.add("10", revision="5")
        self.fake.conflict_next = 2
        r = _run(hu.apply_update("10", self.FIELDS))
        self.assertEqual(r["outcome"], hu.OUTCOME_UNCONVERGED)
        self.assertEqual(r["written"], [])
        self.assertEqual(len(self.fake.update_calls), 2)               # 初回+再取得後 1 回
        self.assertEqual(self.fake.values("10", "顧客名"), "")

    def test_refetch_after_409_respects_new_values(self):
        # 409 の間に人が 顧客名 を入れた → 再取得後は 顧客名 を書かない
        self.fake.add("10", revision="5")
        real_update = self.fake.update

        async def _update(app, rid, fields, revision=None):
            if not self.fake.update_calls:
                self.fake.update_calls.append((rid, dict(fields), revision))
                self.fake.rows[rid]["顧客名"] = {"value": "弁護士入力"}
                self.fake.rows[rid]["$revision"] = {"value": "6"}
                raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
            return await real_update(app, rid, fields, revision)
        with patch.object(hu.hub_kintone, "update_record", _update):
            r = _run(hu.apply_update("10", self.FIELDS))
        self.assertEqual(r["outcome"], hu.OUTCOME_UPDATED)
        self.assertNotIn("顧客名", r["written"])
        self.assertEqual(r["preexisting"], ["顧客名"])
        self.assertEqual(self.fake.values("10", "顧客名"), "弁護士入力")

    def test_get_failure_is_failed(self):
        self.fake.add("10")
        self.fake.get_fail_next = 1
        r = _run(hu.apply_update("10", self.FIELDS))
        self.assertEqual(r["outcome"], hu.OUTCOME_FAILED)
        self.assertEqual(self.fake.update_calls, [])

    def test_update_failure_non_409_is_failed(self):
        self.fake.add("10")

        async def _update(app, rid, fields, revision=None):
            raise hub_kintone.KintoneError(400, "CB_VA01", "bad")
        with patch.object(hu.hub_kintone, "update_record", _update):
            r = _run(hu.apply_update("10", self.FIELDS))
        self.assertEqual(r["outcome"], hu.OUTCOME_FAILED)
        self.assertEqual(self.fake.get_calls, ["10"])                # 再取得しない


# ── 4. 通知（欄名と解決方法のみ・値なし） ────────────────────────────────────────
class TestNotice(_ModuleBase):
    def test_notice_has_field_names_not_values(self):
        text = hu.build_notice(USER, "10", hu.METHOD_SEARCH,
                               {"outcome": hu.OUTCOME_UPDATED,
                                "written": ["顧客名", "生年月日"],
                                "preexisting": ["電話番号"], "dropped": ["status"]})
        self.assertIn("案件レコードNo: 10", text)
        self.assertIn("LINEユーザーIDで検索", text)
        self.assertIn("登録した欄: 顧客名, 生年月日", text)
        self.assertIn("既に値があり登録しなかった欄: 電話番号", text)
        self.assertIn("対象外のため登録しなかった欄: status", text)

    def test_notice_methods(self):
        self.assertIn("メモリ上の台帳", hu.build_notice(USER, "10", hu.METHOD_MEMORY, None))
        for method, phrase in ((hu.METHOD_NONE, "0 件"),
                               (hu.METHOD_AMBIGUOUS, "複数"),
                               (hu.METHOD_SEARCH_FAILED, "検索に失敗"),
                               (hu.METHOD_PARSE_FAILED, "解析に失敗"),
                               (hu.METHOD_ERROR, "予期しない")):
            with self.subTest(method=method):
                text = hu.build_notice(USER, "", method, None)
                self.assertIn(phrase, text)
                self.assertNotIn(USER, text)                     # userId は素で出さない
                self.assertIn("匿名ID", text)

    def test_notice_outcomes(self):
        self.assertIn("競合", hu.build_notice(USER, "10", hu.METHOD_MEMORY,
                                            {"outcome": hu.OUTCOME_UNCONVERGED}))
        self.assertIn("失敗", hu.build_notice(USER, "10", hu.METHOD_MEMORY,
                                            {"outcome": hu.OUTCOME_FAILED}))

    def test_throttle_kind_registered(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        logger = logging.getLogger("hub.notify")
        logger.addHandler(handler)
        try:
            hub_notify._log_throttled("hearing_update:10")
        finally:
            logger.removeHandler(handler)
        self.assertNotIn("unknown_kind", stream.getvalue())

    def test_handle_update_end_to_end_and_never_raises(self):
        self.fake.add("10")
        rid, method = _run(hu.handle_update(USER, None, {"顧客名": NAME}))
        self.assertEqual((rid, method), ("10", hu.METHOD_SEARCH))
        text = self.notice()
        self.assert_no_values(text)
        self.assertIn("登録した欄: 顧客名", text)
        self.assertEqual(self.notify.await_args.kwargs["throttle_key"], "hearing_update:10")
        self.assertTrue(self.notify.await_args.kwargs["throttle_on_success_only"])

    def test_handle_update_parse_failed_notifies_only(self):
        self.fake.add("10")
        for bad in (None, {}, ["x"], "str"):
            with self.subTest(bad=bad):
                self.notify.reset_mock()
                self.assertEqual(_run(hu.handle_update(USER, "10", bad)),
                                 ("", hu.METHOD_PARSE_FAILED))
                self.assertIn("解析に失敗", self.notice())
        self.assertEqual(self.fake.update_calls, [])

    def test_handle_update_unexpected_exception_is_contained(self):
        with patch.object(hu, "resolve_record_id", AsyncMock(side_effect=RuntimeError("x"))):
            self.assertEqual(_run(hu.handle_update(USER, None, {"顧客名": NAME})),
                             ("", hu.METHOD_ERROR))
        self.assertIn("予期しない", self.notice())

    def test_notify_failure_is_contained(self):
        self.fake.add("10")
        with patch.object(hu.notify, "notify_admin_line",
                          AsyncMock(side_effect=RuntimeError("line down"))):
            self.assertEqual(_run(hu.handle_update(USER, None, {"顧客名": NAME})),
                             ("10", hu.METHOD_SEARCH))
        self.assertEqual(self.fake.values("10", "顧客名"), NAME)        # 書込は成立


# ── main._process_line_event との結線 ────────────────────────────────────────────
class _FlowBase(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeApp21()
        for d in (main.conversation_histories, main.kintone_record_ids,
                  main.user_business_names):
            d.pop(USER, None)
            self.addCleanup(d.pop, USER, None)
        main.hearing_completed.discard(USER)
        self.addCleanup(main.hearing_completed.discard, USER)
        self.reply = AsyncMock()
        self.log = AsyncMock()
        self.ask = AsyncMock(return_value=MARKER_REPLY)
        self.create = AsyncMock(return_value="900")
        self.legacy_update = AsyncMock()
        self.queue = AsyncMock(return_value="29-1")
        self.notify = AsyncMock(return_value=True)
        self.business = AsyncMock(return_value=True)
        patches = [
            patch.object(main.autoreply_stoplist, "is_suppressed",
                         AsyncMock(return_value=False)),
            patch.object(main, "get_app21_record", self.fake.get_by_user),
            patch.object(main, "get_recent_chat_history", AsyncMock(return_value=[])),
            patch.object(main, "ask_claude", self.ask),
            patch.object(main, "_line_reply_with_fallback", self.reply),
            patch.object(main, "save_to_chatlog", self.log),
            patch.object(main, "save_to_approval_queue", self.queue),
            patch.object(main, "post_to_kintone", self.create),
            patch.object(main, "update_kintone_record", self.legacy_update),
            patch.object(main, "ATTORNEY_LINE_USER_ID", "U_attorney"),
            patch.object(hu.hub_kintone, "search_records", self.fake.search),
            patch.object(hu.hub_kintone, "get_record", self.fake.get_by_id),
            patch.object(hu.hub_kintone, "update_record", self.fake.update),
            patch.object(hu.notify, "notify_admin_line", self.notify),
            patch.object(main.hub_notify, "notify_business", self.business),
            patch.dict(os.environ, {"ATTORNEY_LINE_USER_ID": "U_attorney",
                                    "AUTOREPLY_PAUSED": "0"}),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def run_event(self, text="はい", user=USER):
        _run(main._process_line_event("tok", user, text))

    def sent_text(self) -> str:
        self.assertEqual(self.reply.await_count, 1)
        return self.reply.await_args.args[2]

    def assert_clean_send(self):
        text = self.sent_text()
        self.assertEqual(text, REPLY_HEAD)
        self.assertNotIn("KINTONE", text)
        self.queue.assert_not_awaited()                  # 降格していない
        self.legacy_update.assert_not_awaited()          # 旧・revision なし関数は使わない


class TestFlow(_FlowBase):
    def test_no_memory_one_record_writes_strips_and_sends(self):
        self.fake.add("10", revision="5")
        self.run_event()
        self.assert_clean_send()
        self.assertEqual(len(self.fake.update_calls), 1)
        rid, fields, rev = self.fake.update_calls[0]
        self.assertEqual((rid, rev), ("10", "5"))
        self.assertEqual(fields, {"顧客名": NAME, "住所": ADDR, "生年月日": BIRTH,
                                  "電話番号": PHONE, "メールアドレス": MAIL})
        self.assertEqual(main.kintone_record_ids[USER], "10")
        self.assertIn(USER, main.hearing_completed)
        text = self.notify.await_args.args[0]
        self.assertIn("案件レコードNo: 10", text)
        self.assertIn("LINEユーザーIDで検索", text)
        for v in VALUES:
            self.assertNotIn(v, text)

    def test_no_memory_zero_records_no_write_strips_and_notifies(self):
        self.run_event()
        self.assert_clean_send()
        self.assertEqual(self.fake.update_calls, [])
        self.create.assert_not_awaited()
        self.assertNotIn(USER, main.kintone_record_ids)
        self.assertNotIn(USER, main.hearing_completed)
        self.assertIn("0 件", self.notify.await_args.args[0])

    def test_no_memory_two_records_no_write_needs_review(self):
        self.fake.add("10")
        self.fake.add("11")
        self.run_event()
        self.assert_clean_send()
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual(self.fake.values("10", "顧客名"), "")
        self.assertEqual(self.fake.values("11", "顧客名"), "")
        self.assertIn("複数", self.notify.await_args.args[0])
        self.assertIn("要確認", self.notify.await_args.args[0])
        self.assertNotIn(USER, main.hearing_completed)

    def test_memory_id_used_without_search(self):
        self.fake.add("10")
        self.fake.add("12", revision="7")
        main.kintone_record_ids[USER] = "12"
        self.run_event()
        self.assert_clean_send()
        self.assertEqual(self.fake.search_calls, [])
        self.assertEqual([c[0] for c in self.fake.update_calls], ["12"])
        self.assertEqual(self.fake.values("12", "顧客名"), NAME)
        self.assertEqual(self.fake.values("10", "顧客名"), "")
        self.assertIn("メモリ上の台帳", self.notify.await_args.args[0])

    def test_existing_values_preserved(self):
        self.fake.add("10", 顧客名="弁護士入力", 住所="手入力住所")
        self.run_event()
        self.assert_clean_send()
        self.assertEqual(self.fake.values("10", "顧客名"), "弁護士入力")
        self.assertEqual(self.fake.values("10", "住所"), "手入力住所")
        self.assertEqual(self.fake.update_calls[0][1],
                         {"生年月日": BIRTH, "電話番号": PHONE, "メールアドレス": MAIL})
        text = self.notify.await_args.args[0]
        self.assertIn("既に値があり登録しなかった欄: 住所, 顧客名", text)

    def test_cas_409_refetch_once_in_flow(self):
        self.fake.add("10", revision="5")
        self.fake.conflict_next = 1
        self.run_event()
        self.assert_clean_send()
        self.assertEqual([c[2] for c in self.fake.update_calls], ["5", "6"])
        self.assertEqual(self.fake.values("10", "顧客名"), NAME)

    def test_after_restart_memory_empty_same_result(self):
        # 1 回目: 台帳経由で書込 → 台帳を消す（再起動相当）→ 2 回目も検索で解決
        self.fake.add("10")
        main.kintone_record_ids[USER] = "10"
        self.run_event()
        self.assertEqual(self.fake.search_calls, [])
        for d in (main.conversation_histories, main.kintone_record_ids):
            d.pop(USER, None)
        main.hearing_completed.discard(USER)
        self.fake.rows["10"]["生年月日"] = {"value": ""}     # 1 欄だけ空に戻す
        self.reply.reset_mock()
        self.notify.reset_mock()
        self.run_event()
        self.assert_clean_send()
        self.assertEqual(len(self.fake.search_calls), 1)
        self.assertEqual(self.fake.update_calls[-1][1], {"生年月日": BIRTH})
        self.assertEqual(main.kintone_record_ids[USER], "10")

    def test_broken_json_marker_stripped_and_notified(self):
        self.fake.add("10")
        self.ask.return_value = BROKEN_REPLY
        self.run_event()
        self.assert_clean_send()
        self.assertEqual(self.fake.update_calls, [])
        self.assertIn("解析に失敗", self.notify.await_args.args[0])
        self.assertNotIn(USER, main.hearing_completed)

    def test_unclosed_marker_still_demoted_safe_side(self):
        self.fake.add("10")
        self.ask.return_value = UNCLOSED_REPLY
        self.run_event()
        self.assertEqual(self.fake.update_calls, [])
        self.queue.assert_awaited_once()
        self.assertIn("内部マーカー残存", self.queue.await_args.kwargs["reason"])
        self.assertEqual(self.sent_text(), main.PENDING_REPLY)
        self.notify.assert_not_awaited()

    def test_record_marker_broken_json_stripped_no_create(self):
        self.ask.return_value = RECORD_BROKEN_REPLY
        self.run_event()
        self.assert_clean_send()
        self.create.assert_not_awaited()
        self.assertIn("解析に失敗", self.notify.await_args.args[0])

    def test_no_marker_untouched(self):
        self.ask.return_value = REPLY_HEAD
        self.run_event()
        self.assertEqual(self.sent_text(), REPLY_HEAD)
        self.notify.assert_not_awaited()
        self.assertEqual(self.fake.search_calls, [])
        self.assertNotIn(USER, main.hearing_completed)

    def test_write_failure_does_not_block_send(self):
        self.fake.add("10")
        self.fake.get_fail_next = 1
        self.run_event()
        self.assert_clean_send()
        self.assertIn("失敗", self.notify.await_args.args[0])
        self.assertEqual(main.kintone_record_ids[USER], "10")


if __name__ == "__main__":
    unittest.main()
