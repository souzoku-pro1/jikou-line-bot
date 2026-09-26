"""HRI-07｜App 21「LINEユーザーID」一意制約の前提整備（収束コードの追加）

PR #258（HRI-03）で hub/jikou_case_create.create_or_adopt に「排他区間内で再検索→
無ければ作成→作成失敗は再検索して既存を採用」が入ったが、次の 2 経路は同じ収束を
持たなかった。このまま App 21 に一意制約を有効化すると、制約の発火時に
「保存失敗ページ」「受付番号が確認できません」へ落ちる。

- shindan_form._handle_submit_linked（本人専用リンク経由の作成）→ create_or_adopt を通す
- hub.form_link.bind_record（受付番号による紐付け=更新）→ 個別実装（失敗時のみ再検索）
  ※ create_or_adopt の「実行前の再検索」は R-JIKOU-FORM-2 fix2 の裁定（並行で本人の
    通常レコードができていても紐付けは成立させ、統合先は linked_id・分裂は要確認通知）を
    制約が未設定の現状でも変えてしまうため通さない。

制約違反は fake で注入する（kintone の設定には触れない）。
"""
import asyncio
import io
import os
import re
import subprocess
import unittest
from unittest.mock import AsyncMock, patch

import test_human_reply_intake as t_hri      # noqa: E402  rich fake（App 21/28/40）
import test_jikou_form2 as t_form2           # noqa: E402  紐付けフローの土台
import test_shindan_line_link as t_sll       # noqa: E402  診断フォーム（本人リンク）の土台

import main  # noqa: E402
import shindan_form as sf  # noqa: E402
from hub import form_link as fl  # noqa: E402
from hub import human_reply_intake as hri  # noqa: E402
from hub import jikou_case_create  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import shindan_link as sl  # noqa: E402

REPO = os.path.dirname(os.path.abspath(__file__))


def _unique_violation() -> hub_kintone.KintoneError:
    """kintone の一意制約違反の形（HTTP 400 / CB_VA01 / 欄別 errors）。"""
    return hub_kintone.KintoneError(
        400, "CB_VA01", "入力内容が正しくありません。",
        {"record.LINEユーザーID.value": {"messages": ["値がほかのレコードと重複しています。"]}})


def _run(coro):
    return asyncio.run(coro)


# ── A. shindan_form（本人専用リンク経由の作成）: HTTP 経由 ─────────────────────────
class TestShindanLinkedCreate(t_sll._FormLinkBase):
    USER = t_sll.USER

    def _race_then_violation(self):
        """別プロセスが「区間内の再検索」と「作成」の間に本人のレコードを作った
        → 作成は一意制約違反で失敗する。"""
        real_create = self.fake.create_record

        async def _create(app, fields):
            self.fake.create_calls.append(dict(fields))
            self.fake.add("77", revision="2")
            raise _unique_violation()
        p = patch.object(sf.hub_kintone, "create_record", _create)
        p.start()
        self.addCleanup(p.stop)
        return real_create

    def user_rows(self):
        return [r for r in self.fake.rows.values()
                if r["LINEユーザーID"]["value"] == self.USER]

    # 受入 1: 一意制約違反 → 例外にならず既存レコードを採用して継続
    def test_unique_violation_adopts_existing_record_and_continues(self):
        self._race_then_violation()
        token = self.issue()
        resp = self.post_linked(token)
        self.assertEqual(resp.status_code, 200)
        self.assertIn(sf.LINKED_DONE_TEXT, resp.text)               # 成功ページ（失敗ページでない）
        self.assertIn(sf.PHOTO_ROUTE, resp.text)
        self.assertEqual([r["$id"]["value"] for r in self.user_rows()], ["77"])   # 2 件目なし
        rid, fields, rev = self.fake.update_calls[0]                # 採用先へ空欄のみ・CAS
        self.assertEqual((rid, rev), ("77", "2"))
        self.assertEqual(fields["診断パターン"], "A")
        self.assertEqual(self.fake.val("77", "問い合わせ業者名"), "テスト債権者株式会社")
        self.assertIsNotNone(self.used_at(token))                   # 使用済みが確定
        self.assertIn("案件レコードNo:77", self.notify_biz.await_args.args[1])
        self.assertEqual([r for r in self.cap.records
                          if "shindan_link_write_failed" in r.getMessage()], [])

    def test_adopted_record_keeps_human_values(self):
        real_add = self.fake.add

        async def _create(app, fields):
            real_add("77", revision="4", 問い合わせ業者名="弁護士入力")
            raise _unique_violation()
        with patch.object(sf.hub_kintone, "create_record", _create):
            resp = self.post_linked(self.issue())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.fake.val("77", "問い合わせ業者名"), "弁護士入力")    # 上書きなし

    # 受入 3: 通常系は従来と同一
    def test_normal_create_is_unchanged(self):
        token = self.issue()
        resp = self.post_linked(token)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        payload = self.fake.create_calls[0]
        self.assertEqual(payload["LINEユーザーID"], self.USER)
        self.assertEqual(payload["受付チャネル"], "フォーム")
        self.assertEqual(payload["問い合わせ業者名"], "テスト債権者株式会社")
        self.assertNotIn("受付番号", payload)
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual(len(self.user_rows()), 1)
        self.assertIsNotNone(self.used_at(token))
        self.assertIn(sf.LINKED_DONE_TEXT, resp.text)

    def test_violation_without_existing_record_fails_as_before(self):
        async def _create(app, fields):
            raise _unique_violation()                               # 再検索しても 0 件
        token = self.issue()
        with patch.object(sf.hub_kintone, "create_record", _create):
            resp = self.post_linked(token)
        self.assertEqual(resp.status_code, 200)
        self.assertNotIn(sf.PHOTO_ROUTE, resp.text)                 # 失敗ページ=写真導線なし
        self.assertIsNone(self.used_at(token))
        self.assertIsNone(self.claimed_at(token))                   # 予約は解放（再送可）
        self.assertEqual(self.user_rows(), [])

    def test_record_appearing_after_lookup_is_adopted_without_create(self):
        # 一意制約が未設定でも: 本人レコードの検索（0 件）の後に別経路が作成 → 区間内の
        # 再検索で見つけて採用（create を呼ばない=2 件目を作らない）
        real_search = self.fake.search_records
        calls = {"n": 0}

        async def _search(app, query, fields=None):
            calls["n"] += 1
            if calls["n"] == 2:                                     # create_or_adopt の再検索
                self.fake.add("77", revision="2")
            return await real_search(app, query, fields)
        with patch.object(sf.hub_kintone, "search_records", _search):
            resp = self.post_linked(self.issue())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.fake.create_calls, [])
        self.assertEqual([r["$id"]["value"] for r in self.user_rows()], ["77"])
        self.assertEqual(self.fake.val("77", "診断パターン"), "A")

    def test_multiple_existing_records_are_not_written(self):
        async def _create(app, fields):
            self.fake.add("77")
            self.fake.add("78")
            raise _unique_violation()
        token = self.issue()
        with patch.object(sf.hub_kintone, "create_record", _create):
            resp = self.post_linked(token)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.fake.update_calls, [])                # ambiguous と同じ=書かない
        self.assertIsNone(self.used_at(token))
        self.assertNotIn(sf.PHOTO_ROUTE, resp.text)                 # 失敗ページ=写真導線なし

    def test_unlinked_form_path_is_untouched(self):
        # k なしの経路は LINEユーザーID を空で作る=一意制約の対象外。共通入口を通さない
        with patch.object(jikou_case_create, "create_or_adopt", AsyncMock()) as entry:
            resp = self.client.post("/shindan", data=dict(t_sll.VALID))
        self.assertEqual(resp.status_code, 200)
        entry.assert_not_awaited()
        self.assertEqual(self.fake.create_calls[0]["LINEユーザーID"], "")
        self.assertRegex(self.fake.create_calls[0]["受付番号"], r"^[0-9]{6}$")


# ── B. shindan_form 経路 × 返答取込経路の競合（同一ユーザー・同一 fake） ──────────────
class TestShindanVsIntake(t_sll._DbBase):
    USER = t_hri.USER

    def setUp(self):
        super().setUp()
        self.fake = t_hri._FakeKintone()
        self.ai = AsyncMock()
        hri._locks.clear()
        for p in (
                patch.object(hub_kintone, "search_records", self.fake.search_records),
                patch.object(hub_kintone, "create_record", self.fake.create_record),
                patch.object(hub_kintone, "get_record", self.fake.get_record),
                patch.object(hub_kintone, "update_record", self.fake.update_record),
                patch.object(hri, "create_message_with_fallback", self.ai),
                patch.object(hri, "_now", lambda: self.fake.now),
                patch.object(hri.notify, "notify_admin_line", AsyncMock(return_value=True)),
                patch.object(sf.notify, "notify_business", AsyncMock(return_value=True)),
                patch.dict(os.environ, {"KINTONE_SUBDOMAIN": "testsub"})):
            p.start()
            self.addCleanup(p.stop)

    def app21(self) -> dict:
        return self.fake.rows["KINTONE_APP_ID"]

    def app21_creates(self) -> list:
        return [c for c in self.fake.create_calls if c[0] == "KINTONE_APP_ID"]

    async def _submit(self):
        token = await sl.issue(self.USER)
        return await sf._handle_submit(dict(t_sll.VALID),
                                       {"line_user_id": self.USER, "token": token})

    def _auto_ai(self, values):
        async def _fn(*a, **k):
            codes = k["tools"][0]["input_schema"]["properties"]["items"]["required"]
            return t_hri._ai({c: (True, values[c], "high") for c in codes if c in values},
                             fill=codes)
        return _fn

    def test_intake_sees_none_then_shindan_creates_during_ai(self):
        auto = self._auto_ai({"顧客名": t_hri.NAME})

        async def _ai_then_form(*a, **k):
            resp = await self._submit()                             # 診断フォームが先に作成
            assert resp.status_code == 200
            return await auto(*a, **k)
        self.ai.side_effect = _ai_then_form
        out = self.q(hri.run_jikou(self.USER, "山田太郎です", t_hri.EVT))
        self.assertEqual(out, "written")
        self.assertEqual(len(self.app21()), 1)
        self.assertEqual(len(self.app21_creates()), 1)
        (rid,) = self.app21()
        self.assertEqual(self.fake.val("KINTONE_APP_ID", rid, "顧客名"), t_hri.NAME)
        self.assertEqual(self.fake.val("KINTONE_APP_ID", rid, "受付チャネル"), "フォーム")

    def test_shindan_sees_none_then_intake_creates_before_its_create(self):
        self.ai.side_effect = self._auto_ai({"顧客名": t_hri.NAME})
        real_find = sl.find_user_record

        async def _find_then_intake_creates(app, line_user_id):
            found = await real_find(app, line_user_id)              # 0 件（=持ち越される結果）
            # 排他区間に入る前に、返答取込が最小レコードを作成する
            assert await hri.run_jikou(self.USER, "山田太郎です", t_hri.EVT) == "written"
            return found
        p = patch.object(sl, "find_user_record", _find_then_intake_creates)
        p.start()
        self.addCleanup(p.stop)
        resp = self.q(self._submit())
        self.assertEqual(resp.status_code, 200)
        self.assertIn(sf.PHOTO_ROUTE, resp.body.decode("utf-8"))      # 成功ページ
        self.assertEqual(len(self.app21()), 1)
        self.assertEqual(len(self.app21_creates()), 1)              # 取込の最小レコード 1 件だけ
        (rid,) = self.app21()
        self.assertEqual(self.fake.val("KINTONE_APP_ID", rid, "受付チャネル"), "LINE")
        self.assertEqual(self.fake.val("KINTONE_APP_ID", rid, "問い合わせ業者名"),
                         "テスト債権者株式会社")                      # フォーム回答は空欄へ統合
        self.assertEqual(self.fake.val("KINTONE_APP_ID", rid, "顧客名"), t_hri.NAME)

    def test_concurrent_submit_and_intake_single_record(self):
        self.ai.side_effect = self._auto_ai({"顧客名": t_hri.NAME})

        async def _both():
            return await asyncio.gather(
                self._submit(), hri.run_jikou(self.USER, "山田太郎です", t_hri.EVT))
        resp, out = self.q(_both())
        self.assertEqual(resp.status_code, 200)
        self.assertIn(out, ("written", "no_write"))
        self.assertEqual(len(self.app21()), 1)
        self.assertEqual(len(self.app21_creates()), 1)

    def test_constraint_fires_when_another_process_created(self):
        # 一意制約あり・別プロセス（区間の外）が再検索と作成の間に作成 → 発火 → 収束
        self.fake.unique_envs = {"APP_HOUKI", "KINTONE_APP_ID"}
        real_create = self.fake.create_record
        state = {"raced": False}

        async def _create(app, fields):
            if app.app_id_env == "KINTONE_APP_ID" and not state["raced"]:
                state["raced"] = True
                self.fake.add_jikou(rid="77", user_id=self.USER)
            return await real_create(app, fields)
        with patch.object(hub_kintone, "create_record", _create):
            resp = self.q(self._submit())
        self.assertEqual(resp.status_code, 200)
        self.assertIn(sf.PHOTO_ROUTE, resp.body.decode("utf-8"))      # 成功ページ
        self.assertEqual(sorted(self.app21()), ["77"])
        self.assertEqual(self.fake.val("KINTONE_APP_ID", "77", "問い合わせ業者名"),
                         "テスト債権者株式会社")


# ── C. form_link.bind_record（受付番号による紐付け=更新） ─────────────────────────────
class _BindBase(t_form2._FlowBase):
    USER = t_form2.USER

    def inject_update(self, before=None, error=None, land=False):
        """紐付けの update を差し替える。before=呼び出し前の割り込み（別プロセスの作成）・
        error=送出する例外・land=書込は着いたが応答を失った。"""
        real_update = self.fake.update

        async def _update(app, record_id, fields, revision=None):
            if fl.USER_FIELD not in fields:
                return await real_update(app, record_id, fields, revision)
            self.fake.update_calls.append((record_id, dict(fields), revision))
            if before is not None:
                before()
            if land:
                self.fake.rows[record_id][fl.USER_FIELD] = {"value": fields[fl.USER_FIELD]}
            raise error
        p = patch.object(fl.hub_kintone, "update_record", _update)
        p.start()
        self.addCleanup(p.stop)

    def add_own_record(self, rid="77", **extra):
        return self.fake.add(rid, "", user_id=self.USER, channel="LINE", revision="2",
                             問い合わせ業者名="", 借入時期_テキスト="",
                             最終返済日_テキスト="", 裁判所書類="", **extra)


class TestBindRecordConvergence(_BindBase):
    def bind(self, rec):
        return _run(fl.bind_record(self.USER, rec))

    # 受入 2: 一意制約違反 → 例外にならず既存レコードを採用して継続
    def test_unique_violation_adopts_existing_record(self):
        form = self.fake.add("10", "123456", revision="5")
        self.inject_update(before=self.add_own_record, error=_unique_violation())
        self.assertEqual(self.bind(form), (fl.BIND_ADOPTED, "77"))
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], "")     # 未紐付けのまま
        self.assertEqual(self.fake.rows["10"]["$revision"]["value"], "5")

    # 受入 3: 通常系は従来と同一（事前の再検索なし・CAS 付き更新 1 回）
    def test_normal_bind_is_unchanged(self):
        form = self.fake.add("10", "123456", revision="5")
        self.assertEqual(self.bind(form), (fl.BIND_LINKED, "10"))
        self.assertEqual(self.fake.update_calls, [("10", {"LINEユーザーID": self.USER}, "5")])
        self.assertEqual(self.fake.search_calls, [])
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], self.USER)

    def test_split_ruling_is_unchanged_without_constraint(self):
        # R-JIKOU-FORM-2 fix2: 本人の通常レコードが別に在っても、制約が無ければ紐付けは
        # 成立する（事前の再検索で採用へ倒さない）
        self.add_own_record()
        form = self.fake.add("10", "123456", revision="5")
        self.assertEqual(self.bind(form), (fl.BIND_LINKED, "10"))

    def test_cas_lost_is_unchanged(self):
        form = dict(self.fake.add("10", "123456", revision="5"))    # 照会時点の写し（rev 5）
        self.fake.rows["10"]["$revision"] = {"value": "6"}          # 人が先に更新
        self.assertEqual(self.bind(form), (fl.BIND_FAILED, None))
        self.assertEqual(self.fake.search_calls, [])                # 409 は再検索しない

    def test_other_failure_without_existing_record_is_unchanged(self):
        form = self.fake.add("10", "123456", revision="5")
        self.inject_update(error=hub_kintone.KintoneError(403, "GAIA_NO01", "forbidden"))
        self.assertEqual(self.bind(form), (fl.BIND_FAILED, None))

    def test_lost_response_but_write_landed_converges_to_linked(self):
        form = self.fake.add("10", "123456", revision="5")
        self.inject_update(error=hub_kintone.KintoneError(0, "transport_error", "x"), land=True)
        self.assertEqual(self.bind(form), (fl.BIND_LINKED, "10"))

    def test_re_search_failure_means_failed_not_exception(self):
        form = self.fake.add("10", "123456", revision="5")
        self.inject_update(error=_unique_violation())
        with patch.object(jikou_case_create, "find_existing",
                          AsyncMock(side_effect=hub_kintone.KintoneError(520, "X", "down"))):
            self.assertEqual(self.bind(form), (fl.BIND_FAILED, None))

    def test_try_link_adopted_notifies_record_numbers_only(self):
        self.fake.add("10", "123456", revision="5")
        self.inject_update(before=self.add_own_record, error=_unique_violation())
        out = _run(fl.try_link(self.USER, "123456", now=t_form2.NOW.timestamp()))
        self.assertEqual(out, ("adopted", "77"))
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[1]
        self.assertIn("要確認", text)
        self.assertIn("受付番号:123456", text)
        self.assertIn("レコード番号:10", text)
        self.assertIn("レコード番号:77", text)
        self.assertNotIn(self.USER, text)
        self.assertNotIn("フォーム債権者株式会社", text)


class TestMainAdoptedRouting(_BindBase):
    def _violation(self, **own):
        self.fake.add("10", "123456", revision="5")
        self.inject_update(before=lambda: self.add_own_record(**own),
                           error=_unique_violation())

    def test_adopted_continues_hearing_with_existing_record(self):
        self._violation()
        self.run_event("123456")
        self.ask.assert_awaited_once()                              # 不該当で打ち切らず継続
        self.assertFalse(self.ask.await_args.kwargs.get("form_handover"))   # 引き継ぎ注入なし
        sent = [c.args[2] for c in self.reply.await_args_list]
        self.assertNotIn(fl.REPLY_NOT_MATCHED, sent)
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], "")
        self.assertNotEqual(main.kintone_record_ids.get(self.USER), "10")

    def test_adopted_record_in_human_mode_stays_silent(self):
        self._violation(response_mode="人対応")
        self.run_event("123456")
        self.ask.assert_not_awaited()
        self.reply.assert_not_awaited()

    def test_adopted_refetch_failure_is_fail_closed(self):
        self._violation()
        self.fake.get_fail_next = 1
        self.run_event("123456")
        self.ask.assert_not_awaited()
        self.reply.assert_not_awaited()
        texts = [c.args[1] for c in self.notify.await_args_list]
        self.assertTrue(any("77" in t for t in texts))


# ── D. 網羅確認の機械化: App 21 へ LINEユーザーID を書き得る箇所の閉集合 ─────────────────
class TestLineUserIdWritersClosedSet(unittest.TestCase):
    """「一意制約を有効化しても落ちない」の根拠を固定する。LINEユーザーID を書くには
    payload にこの欄名が要る。欄名の出所は (a) ソース中のリテラルか (b) データ由来の
    キー（モデルの JSON・フォーム入力）で、(b) は閉集合の許可欄でしか書けない
    （hearing_update.UPDATE_FIELDS／form_link.BOT_FILL_FIELDS／
    human_reply_intake.JIKOU_FIELDS／shindan_form._LINKED_UPDATE_FIELDS）。
    よって (a) のリテラルを含むファイルの閉集合を pin し、新しいファイルが現れたら
    「収束を持つか」の確認を強制する。"""

    # file → 役割。write21=App 21 へこの欄を書く（全て収束を持つ）
    EXPECTED = {
        "main.py": "write21",                     # KINTONE_RECORD 作成 → create_or_adopt
        "hub/human_reply_intake.py": "write21",   # 最小レコード作成 → create_or_adopt
        "shindan_form.py": "write21",             # 本人リンク作成 → create_or_adopt／k なしは空値
        "hub/form_link.py": "write21",            # 紐付け（更新）→ 失敗時のみ再検索して採用
        "hub/jikou_case_create.py": "read",
        "hub/hearing_update.py": "read",
        "hub/shindan_link.py": "read",
        "hub/image_analysis.py": "read",
        "hub/image_intake.py": "read",
        "hub/webapp_case_views.py": "read",
        "hub/webapp_q.py": "read",
        "chat_responder.py": "read",
        "config.py": "schema",
        "hub/houki_case_store.py": "app40",
        "hub/houki_card_read.py": "app40",
    }

    def _tracked_sources(self) -> dict:
        out = subprocess.run(["git", "-C", REPO, "ls-files", "*.py"],
                             capture_output=True, text=True, encoding="utf-8").stdout.split()
        srcs = {}
        for f in out:
            base = os.path.basename(f)
            if base.startswith("test_") or base == "conftest.py" or f.startswith("scripts/"):
                continue
            srcs[f] = io.open(os.path.join(REPO, f), encoding="utf-8").read()
        return srcs

    def test_files_naming_the_field_are_a_closed_set(self):
        hits = {f for f, s in self._tracked_sources().items() if "LINEユーザーID" in s}
        self.assertEqual(hits, set(self.EXPECTED),
                         "LINEユーザーID を名指しするファイルが増減した。App 21 へ書く経路なら "
                         "hub.jikou_case_create の収束を通すこと（HRI-07）")

    def test_every_app21_writer_has_convergence(self):
        srcs = self._tracked_sources()
        for f, role in self.EXPECTED.items():
            if role != "write21":
                continue
            with self.subTest(file=f):
                self.assertRegex(srcs[f], r"jikou_case_create\.(create_or_adopt|find_existing)\(")

    def test_data_driven_writes_cannot_carry_the_field(self):
        from hub import hearing_update
        for allowed in (hearing_update.UPDATE_FIELDS, fl.BOT_FILL_FIELDS,
                        hri.JIKOU_FIELDS, sf._LINKED_UPDATE_FIELDS):
            self.assertNotIn("LINEユーザーID", set(allowed))
        # main の統合は LINEユーザーID を明示的に除外している
        src = io.open(os.path.join(REPO, "main.py"), encoding="utf-8").read()
        self.assertIn('if k not in ("LINEユーザーID", "status")', src)

    def test_main_create_goes_through_the_single_entry(self):
        src = io.open(os.path.join(REPO, "main.py"), encoding="utf-8").read()
        calls = re.findall(r"post_to_kintone\(", src)
        self.assertEqual(len(calls), 2)          # 定義 1 + create_or_adopt の lambda 1
        self.assertIn("lambda: post_to_kintone(kintone_record)", src)


if __name__ == "__main__":
    unittest.main()
