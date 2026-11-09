# -*- coding: utf-8 -*-
"""K-1（docs/k_design.md §1〜§2・§5・§6 K-1）: 下書き専用モデルの K 後追い下書き・規則 v1.5・記録表 reply_k_draft。

受入条件（票 K-1）
 (a) K 経路で生成した下書きが承認キュー（返信画面）以外に出る経路が静的検査で 0（AST: 送信関数・kintone 書込・line_channel を参照しない）
 (b) env 閉集合外は off と同一挙動（相談者向け送信 0・K 下書きなし。管理者向け障害通知は無効設定／モデル障害／上限到達の各 1 日 1 回＝設計で許容）＋ fail-closed の記録 1 行
 (c) v1.4 経路の既存テストは不変（env 未設定＝K 無効・基底 prompt は byte 同一・prompt_note は従来どおり）
 (d) 新表 migration の upgrade/downgrade（sqlite）と、本番と同じ e3f5 → head の順
 (e) 第 8 項（相談文中の債権者名・金額・時期を引用して答える）がテストの手本相談文で確認できる
 (f) 秘密値 grep 0（本ファイルに値を置かない）
"""
import ast
import asyncio
import hashlib
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

import chat_responder as cr  # noqa: E402
from hub import approved_texts  # noqa: E402
from hub import db  # noqa: E402
from hub import k_draft as kd  # noqa: E402
from hub import autoreply_stoplist  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import reply_exemplar as rx  # noqa: E402
from hub import reply_followup as rf  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402
from hub import webapp_inbox_view as iv  # noqa: E402
from hub import webapp_reply_view as rv  # noqa: E402
from hub.inbound_event import Base as InboundBase  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
RULES_SHA256 = "a8da31ccba6d25724e2fc215e763ddf76d02bca603fd44565722940c9febdf5a"
MIG = "f6a8c0e2d4b6"
MERGE = "e3f5a7b9c1d3"
MIG_FILE = REPO / "alembic" / "versions" / "20261109_f6a8c0e2d4b6_reply_k_draft.py"

# 手本相談文（受入 (e)）: 債権者名・金額・時期を含む。固有名は業者名のみ（個人名・連絡先なし）
CONSULT = "アコムから2026年10月付で請求書が届きました。金額は35万円で、最後に返済したのは2019年3月です。時効になりますか。"
GOOD_DRAFT = ("山田 太郎様\nアコムからの2026年10月付の請求について、最後のお支払いが2019年3月であれば、"
              "貸金業者の債権の消滅時効は最後の返済から 5 年です（民法 166 条 1 項 1 号）ので、35万円の請求は時効援用できる可能性が高いです。"
              "最終的に時効が成立しているかは、当事務所から業者へ時効援用通知を送り、その後の業者への確認をもって確定します。\n"
              "請求書のほかに、裁判所からの書類（訴状・支払督促）は届いていますか。")


def _run(coro):
    try:
        return asyncio.run(coro)
    finally:
        db.reset_for_tests()


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


APP21 = _rec(status="受付", 顧客名="山田 太郎", 問い合わせ業者名="アコム", 借入時期_テキスト="2015年", 最終返済日_テキスト="2019年3月",
             裁判所書類="なし", 信用情報確認="未")


def _fake_model(text="", in_tokens=10000, out_tokens=400, exc=None):
    calls: list = []

    async def _call(model, system_prompt, messages, tool):
        calls.append({"model": model, "prompt": system_prompt, "messages": messages, "tool": tool})
        if exc is not None:
            raise exc
        return text, in_tokens, out_tokens
    _call.calls = calls
    return _call


# ── (b) env の閉集合（表 1-3） ─────────────────────────────────────────────────────
class TestEnvClosedSet(unittest.TestCase):
    def test_closed_set_and_default_off(self):
        self.assertEqual(kd.ALLOWED_MODELS, ("off", "claude-fable-5-1", "claude-opus-5-5", "claude-sonnet-5-5"))
        self.assertEqual(kd.DEFAULT_WHEN_UNSET, "off")              # 設計 §1-3: 未設定＝K 無効（配備しただけでは挙動が変わらない）
        cases = {None: ("off", False), "": ("off", False), "  ": ("off", False), "off": ("off", False),
                 "claude-fable-5-1": ("claude-fable-5-1", True), "claude-opus-5-5": ("claude-opus-5-5", True),
                 "claude-sonnet-5-5": ("claude-sonnet-5-5", True),
                 "claude-sonnet-4-6": ("invalid", False), "fable": ("invalid", False), "CLAUDE-FABLE-5-1": ("invalid", False),
                 "on": ("invalid", False)}
        for raw, (model, en) in cases.items():
            env = {} if raw is None else {kd.ENV_MODEL: raw}
            with patch.dict(os.environ, env, clear=False):
                if raw is None:
                    os.environ.pop(kd.ENV_MODEL, None)
                self.assertEqual((kd.configured_model(), kd.enabled()), (model, en), raw)

    def test_startup_log_only_for_invalid_and_value_not_logged(self):
        kd._config_logged["done"] = False
        with patch.dict(os.environ, {kd.ENV_MODEL: "sekrit-model-xyz"}):
            with self.assertLogs("hub.k_draft", level="WARNING") as cm:
                self.assertEqual(kd.log_config_once(), "invalid")
            self.assertEqual(len(cm.output), 1)
            self.assertNotIn("sekrit-model-xyz", "\n".join(cm.output))
            self.assertEqual(kd.log_config_once(), "invalid")                # 2 回目は出さない（1 行だけ）
        kd._config_logged["done"] = False
        with patch.dict(os.environ, {kd.ENV_MODEL: "claude-fable-5-1"}):
            with self.assertNoLogs("hub.k_draft", level="WARNING"):
                self.assertEqual(kd.log_config_once(), "claude-fable-5-1")
        kd._config_logged["done"] = False

    def test_off_never_creates_task(self):
        async def _t():
            with patch.dict(os.environ, {"DATABASE_URL": "sqlite+aiosqlite:///:memory:"}):
                os.environ.pop(kd.ENV_MODEL, None)
                self.assertIsNone(kd.after_queue("500", user_id="U1", customer_name="n", customer_message="m", category="挨拶・雑談"))
                os.environ[kd.ENV_MODEL] = "off"
                self.assertIsNone(kd.after_queue("500", user_id="U1", customer_name="n", customer_message="m", category="挨拶・雑談"))
        _run(_t())

    def test_cost_table_pinned(self):
        self.assertEqual(kd.PRICE_USD_PER_MTOK, {"claude-fable-5-1": (10.0, 50.0), "claude-opus-5-5": (4.0, 20.0),
                                                 "claude-sonnet-5-5": (2.0, 10.0)})
        self.assertEqual((kd.YEN_PER_USD, kd.MONTHLY_COST_CAP_YEN, kd.TIMEOUT_SECONDS, kd.MAX_TOKENS), (150.0, 1600.0, 60, 1024))
        self.assertEqual(kd.estimate_cost_yen("claude-fable-5-1", 10_100, 400), 18.15)   # §1-6 の見積（約 18 円）
        self.assertEqual(kd.estimate_cost_yen("claude-opus-5-5", 10_100, 400), 7.26)
        self.assertEqual(kd.estimate_cost_yen("claude-sonnet-5-5", 10_100, 400), 3.63)
        self.assertEqual(kd.estimate_cost_yen("invalid", 10, 10), 0.0)
        self.assertEqual(kd.THINKING_PARAM, {"thinking": {"type": "disabled"}})


# ── pin・静的検査（受入 (a)(c)） ──────────────────────────────────────────────────
class TestPins(unittest.TestCase):
    def test_rules_v15_pinned(self):
        self.assertEqual(hashlib.sha256(kd.RULES_V15_BLOCK.encode("utf-8")).hexdigest(), RULES_SHA256)
        self.assertEqual(kd.rules_sha256(), RULES_SHA256)
        self.assertEqual(kd.RULES_VERSION, "v1.5")
        items = [l for l in kd.RULES_V15_TEXT.split("\n") if l[:1].isdigit()]
        self.assertEqual(len(items), 8)                                              # 8 項（裁定 7）
        self.assertTrue(items[7].startswith("8. 個別に答える"))
        self.assertIn("債権者名・金額・時期", items[7])
        self.assertIn("民法 166 条 1 項 1 号", kd.RULES_V15_TEXT)
        self.assertIn("弁護士の大野", kd.RULES_V15_TEXT)
        self.assertEqual(len(kd.RESERVED_PHRASES), 5)
        self.assertEqual([s for s, _o, _n in kd.RESERVED_PHRASES], ["受任の可否", "費用の交渉・分割", "時効成否の最終判断", "不成立方向の見立て", "推奨時期"])
        self.assertEqual(kd.EVASIVE_PHRASES, ("改めてご案内", "確認のうえご連絡", "申し伝えます", "担当者よりご連絡"))
        self.assertEqual(kd.MAX_CHARS, 600)
        doc = (REPO / "docs" / "jikou_reply_q1c_contract.md").read_text(encoding="utf-8")
        self.assertIn(RULES_SHA256[:16], doc)                                        # 凍結記録 1 行

    def test_k_prompt_is_tail_append_and_base_prompt_unchanged(self):
        base = cr.build_system_prompt(status="受付", customer_name="山田 太郎")
        self.assertNotIn("v1.5", base)
        self.assertNotIn(kd.RULES_V15_HEADER, base)
        self.assertEqual(kd.build_k_prompt(base), base + kd.RULES_V15_BLOCK)
        note = rx.format_note([{"id": 1, "category_id": "挨拶・雑談", "inbound_text": "q", "human_reply": "r", "active": True}])
        self.assertEqual(kd.build_k_prompt(base, note), base + kd.RULES_V15_BLOCK + note)    # 基底 → v1.5 → 参考文例
        self.assertEqual(cr.build_system_prompt(status="受付"), cr.build_system_prompt(status="受付", exemplar_note=""))

    def test_module_has_no_send_or_kintone_path(self):
        src = (REPO / "hub" / "k_draft.py").read_text(encoding="utf-8")
        self.assertEqual(scan_source(src, "hub/k_draft.py"), [])                      # sink 方針（emit のみ）
        self.assertNotIn("k_draft", (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8"))
        tree = ast.parse(src)
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
                imported.update(f"{node.module}.{a.name}" for a in node.names)
            elif isinstance(node, ast.Import):
                imported.update(a.name for a in node.names)
        for banned in ("hub.line_channel", "hub.kintone", "httpx", "hub.notify", "line_channel", "kintone"):
            self.assertFalse(any(m == banned or m.startswith(banned + ".") for m in imported), banned)
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        for banned in ("reply_message", "push_message", "_line_reply_with_fallback", "reply_func", "save_to_approval_queue",
                       "mark_approval_sent", "save_to_chatlog", "notify_admin_line", "notify_business", "_push_business_http",
                       "create_message_with_fallback"):
            self.assertNotIn(banned, names, banned)
        self.assertNotIn("cache_control", src)                                       # §1-6: cache は付けない
        for w in (".post(", ".put(", ".patch(", ".delete("):                              # fix1 BK-01: 履歴の K 専用取得は読取だけ
            self.assertNotIn(w, src, w)
        self.assertNotIn('"publish"', src)

    def test_wiring_single_hook_and_tail_import(self):
        csrc = (REPO / "chat_responder.py").read_text(encoding="utf-8").split("\n")
        self.assertEqual(sum("k_draft.after_queue(record_id" in l for l in csrc), 1)
        self.assertEqual(sum("reply_exemplar.record_draft(record_id" in l for l in csrc), 1)   # Q3a の pin は不変（同じ 1 行）
        hook = next(l for l in csrc if "k_draft.after_queue(record_id" in l)
        self.assertIn("reply_exemplar.record_draft(record_id", hook)                 # 起動点は App 29 起票成功直後の 1 か所
        self.assertIn("from hub import k_draft", "\n".join(csrc[-6:]))              # 末尾追記
        self.assertEqual(sum("k_draft.log_config_once()" in l for l in csrc), 1)
        msrc = (REPO / "main.py").read_text(encoding="utf-8")
        self.assertNotIn("k_draft", msrc)                                            # main.py は触らない（行番号 pin）
        env = (REPO / "alembic" / "env.py").read_text(encoding="utf-8")
        self.assertIn("from hub.k_draft import metadata as k_draft_metadata", env)
        self.assertIn("k_draft_metadata]", env)
        for name in ("hub/webapp_reply_view.py", "hub/webapp_inbox_view.py", "hub/reply_exemplar.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)

    def test_migration_file_pinned(self):
        src = MIG_FILE.read_text(encoding="utf-8")
        self.assertIn(f"revision: str = '{MIG}'", src)
        self.assertIn(f"down_revision: Union[str, Sequence[str], None] = '{MERGE}'", src)
        self.assertIn("reply_k_draft_downgrade_refused_data_present", src)
        self.assertEqual(src.count("op.create_table("), 1)                            # 新表 1 つだけ
        self.assertNotIn("alter_column", src)
        self.assertNotIn("batch_alter_table", src)
        self.assertEqual(kd.TABLE_NAME, "reply_k_draft")
        self.assertEqual(kd.STATES, ("started", "generated", "skipped", "failed", "rejected"))

    def test_ruling3_exemplar_note_goes_to_k_only_when_enabled(self):
        rows = [{"id": 7, "category_id": "挨拶・雑談", "inbound_text": "q", "human_reply": "r", "active": True}]
        with patch.object(rx, "_db_configured", lambda: True), patch.object(rx, "list_exemplars", AsyncMock(return_value=rows)), \
             patch.object(kd, "_db_configured", lambda: True), patch.object(kd, "month_cost_yen", AsyncMock(return_value=0.0)):   # fix1 BK-02: 実効状態＝env ∧ DB ∧ 上限内
            with patch.dict(os.environ, {kd.ENV_MODEL: "claude-fable-5-1"}):
                self.assertEqual(_run(rx.prompt_note("jikou", "受付")), "")                 # 一次生成には付けない
                k_note = _run(rx.k_prompt_note("jikou", "受付"))
                self.assertIn("例1（事務所の返信）: r", k_note)
                self.assertEqual(k_note.ids_by_category, {"挨拶・雑談": [7]})
            with patch.dict(os.environ, {kd.ENV_MODEL: "off"}):
                self.assertIn("例1（事務所の返信）: r", _run(rx.prompt_note("jikou", "受付")))   # K OFF＝Q3a の現状どおり
            with patch.dict(os.environ, {kd.ENV_MODEL: "not-a-model"}):
                self.assertIn("例1（事務所の返信）: r", _run(rx.prompt_note("jikou", "受付")))   # 無効値＝off と同じ


# ── 流れ（sqlite・モデルはモック） ─────────────────────────────────────────────────
class _Db(unittest.TestCase):
    MODEL = "claude-fable-5-1"

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="k1_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/k.db", kd.ENV_MODEL: self.MODEL})
        self._env.start()
        db.reset_for_tests()
        approved_texts.reset_pending_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                for md in (kd.metadata, rx.metadata, sl.metadata):
                    await c.run_sync(md.create_all)
        _run(_create())
        self.notify = AsyncMock(return_value="sent")
        self._patches = [
            patch.object(cr, "get_app21_record", AsyncMock(return_value=APP21)),
            patch.object(cr, "get_recent_chat_history", AsyncMock(return_value=cr.ChatHistory())),
            patch.object(kd, "k_chat_history", AsyncMock(return_value=cr.ChatHistory())),
            patch.object(kd, "notify_once", self.notify),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _rows(self):
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(kd.reply_k_draft))).fetchall()]
        return _run(_q())

    def _go(self, fake, record_id="500", message=CONSULT, name="山田 太郎", category="時効見立て_条件付き"):
        with patch.object(kd, "_call_model", fake):
            return _run(kd.run_after_queue(record_id, user_id="U0123456789abcdef0123456789abcdef", customer_name=name,
                                           customer_message=message, category=category))


class TestFlow(_Db):
    def test_generated_quotes_creditor_amount_and_period(self):
        """受入 (e): 第 8 項が prompt に入り、相談文（債権者名・金額・時期）が messages の末尾に渡り、引用した下書きがそのまま保存される。"""
        fake = _fake_model(GOOD_DRAFT)
        self.assertEqual(self._go(fake), "generated")
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual((r["record_id"], r["business"], r["model"], r["rules_version"], r["rules_sha"], r["state"], r["reason"]),
                         ("500", "jikou", "claude-fable-5-1", "v1.5", RULES_SHA256, "generated", None))
        self.assertEqual(r["text"], GOOD_DRAFT)
        for fact in ("アコム", "35万円", "2019年3月", "2026年10月"):
            self.assertIn(fact, r["text"])                                             # 債権者名・金額・時期の引用
        self.assertEqual(r["text_sha"], hashlib.sha256(GOOD_DRAFT.encode("utf-8")).hexdigest())
        self.assertEqual((r["input_tokens"], r["output_tokens"], r["cost_yen"]), (10000, 400, 18.0))
        self.assertEqual(json.loads(r["exemplar_ids"]), [])                             # 手本の受け口は空集合（K-2 まで）
        self.assertEqual(json.loads(r["flags"]), [])
        self.assertIsNotNone(r["elapsed_ms"])
        self.assertEqual(len(fake.calls), 1)
        c = fake.calls[0]
        self.assertEqual(c["model"], "claude-fable-5-1")
        self.assertTrue(c["prompt"].endswith(kd.RULES_V15_BLOCK))                       # 基底 → v1.5（手本 0 件）
        self.assertIn("8. 個別に答える", c["prompt"])
        self.assertIn("山田 太郎", c["prompt"])                                          # 基底 prompt は同じ引数（案件情報）で組む
        self.assertEqual(c["messages"][-1], {"role": "user", "content": CONSULT})
        self.assertEqual(c["tool"]["name"], "compose_reply")
        self.assertEqual(self.notify.await_count, 0)

    def test_redelivery_same_record_single_row_and_single_call(self):
        fake = _fake_model(GOOD_DRAFT)
        self.assertEqual(self._go(fake), "generated")
        self.assertEqual(self._go(fake), "duplicate")
        self.assertEqual(len(self._rows()), 1)
        self.assertEqual(len(fake.calls), 1)

    def test_invalid_model_is_fail_closed_with_one_skipped_row(self):
        """受入 (b): 閉集合外＝off と同一挙動（モデル呼び出し 0・本文なし）＋ skipped 1 行・通知 1 回（承認済みテキスト経由）。"""
        fake = _fake_model(GOOD_DRAFT)
        with patch.dict(os.environ, {kd.ENV_MODEL: "claude-sonnet-4-6"}):
            self.assertEqual(self._go(fake), "skipped")
            self.assertEqual(self._go(fake), "skipped")                                 # 再配送でも行は増えない
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["state"], rows[0]["reason"], rows[0]["model"], rows[0]["text"]), ("skipped", "model:invalid", "invalid", None))
        self.assertEqual(fake.calls, [])
        self.notify.assert_awaited_once_with(kd.NOTICE_INVALID_KEY)

    def test_model_failure_timeout_and_empty_are_failed_without_fallback(self):
        fake = _fake_model(exc=RuntimeError("boom"))
        self.assertEqual(self._go(fake, record_id="501"), "failed")
        self.assertEqual([c["model"] for c in fake.calls], ["claude-fable-5-1"])          # Opus へ自動フォールバックしない
        empty = _fake_model("   ")
        self.assertEqual(self._go(empty, record_id="502"), "failed")

        async def _slow(model, system_prompt, messages, tool):
            await asyncio.sleep(0.5)
            return GOOD_DRAFT, 1, 1
        with patch.object(kd, "TIMEOUT_SECONDS", 0.01):
            self.assertEqual(self._go(_slow, record_id="503"), "failed")
        by = {r["record_id"]: r for r in self._rows()}
        self.assertEqual((by["501"]["state"], by["501"]["reason"], by["501"]["text"]), ("failed", "model:unavailable", None))
        self.assertEqual((by["502"]["state"], by["502"]["reason"]), ("failed", "model:empty"))
        self.assertEqual((by["503"]["state"], by["503"]["reason"]), ("failed", "model:unavailable"))
        self.assertEqual(self.notify.await_count, 3)
        self.assertEqual({c.args[0] for c in self.notify.await_args_list}, {kd.NOTICE_FAILED_KEY})

    SECRET = "SEKRIT-ERR-77"

    def _assert_input_unavailable(self, rid: str, logs: str, fake):
        r = {x["record_id"]: x for x in self._rows()}[rid]
        self.assertEqual((r["state"], r["reason"], r["text"]), ("failed", "input:unavailable", None))
        self.assertEqual(fake.calls, [])                                                  # モデルは呼ばない
        for col in ("reason", "text", "flags", "model", "category"):
            self.assertNotIn(self.SECRET, str(r.get(col) or ""))
        self.assertNotIn(self.SECRET, logs)                                               # 例外本文はログに出ない
        self.assertNotIn("Traceback", logs)
        self.assertIn("[K_DRAFT] failed (fixed reason)", logs)
        self.assertEqual(self.notify.await_count, 0)                                      # 入力障害は管理者通知の対象外

    def test_bk01_app21_lookup_error_is_input_unavailable(self):
        """fix1 BK-01: App 21 の HTTP 非 2xx（App21LookupError）・取得例外は failed／input:unavailable。レコード無し（None）とは区別。"""
        fake = _fake_model(GOOD_DRAFT)
        for rid, exc in (("504", cr.App21LookupError(f"status=500 {self.SECRET}")), ("505", RuntimeError(f"conn {self.SECRET}"))):
            with patch.object(cr, "get_app21_record", AsyncMock(side_effect=exc)), self.assertLogs("hub.k_draft", level="INFO") as cm:
                self.assertEqual(self._go(fake, record_id=rid), "failed")
            self._assert_input_unavailable(rid, "\n".join(cm.output), fake)

    def test_bk01_chatlog_http_failure_detected_by_k_wrapper(self):
        """fix1 BK-01: 実取得の HTTP 経路。既存 get_recent_chat_history は非成功を [] に丸める（不変）が、K 専用 k_chat_history は
        InputUnavailable を投げ、K は failed／input:unavailable で止まる。識別文字列は記録・ログに出ない。"""
        class _Resp:
            def __init__(self, status, payload=None, text=""):
                self.status_code, self.is_success, self._p, self.text = status, 200 <= status < 300, payload or {}, text

            def json(self):
                return self._p

        class _Client:
            resp = None
            calls: list = []

            def __init__(self, **_kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_a):
                return False

            async def get(self, url, headers=None, params=None):
                _Client.calls.append((url, params))
                return _Client.resp
        fake = _fake_model(GOOD_DRAFT)
        self._patches[-2].stop()                                                            # k_chat_history は本物を使う
        self._patches[1].stop()                                                             # 既存 get_recent_chat_history も本物（不変の確認）
        try:
            with patch.object(cr, "APP_CHATLOG", "28"), patch.object(cr, "TOKEN_CHATLOG", "t"), patch.object(cr.httpx, "AsyncClient", _Client):
                _Client.resp = _Resp(520, text=f"gateway error {self.SECRET}")
                self.assertEqual(_run(cr.get_recent_chat_history("U1")), [])              # 既存関数は [] に丸める（変えない）
                with self.assertRaises(kd.InputUnavailable):
                    _run(kd.k_chat_history("U1"))
                with self.assertLogs("hub.k_draft", level="INFO") as cm:
                    self.assertEqual(self._go(fake, record_id="506"), "failed")
                self._assert_input_unavailable("506", "\n".join(cm.output), fake)
                self.assertTrue(all("records.json" in u for u, _p in _Client.calls))          # 読取だけ
                # 正常: 同じ問い合わせ・同じ整形（古い順・画像の内部行を除く・human 行は assistant）
                from hub.image_intake import IMAGE_HUMAN_HOLD_MARKER
                _Client.resp = _Resp(200, {"records": [
                    {"role": {"value": "assistant"}, "content": {}, "message": {"value": "返信2"}},
                    {"role": {"value": "user"}, "message": {"value": IMAGE_HUMAN_HOLD_MARKER}},
                    {"role": {"value": "user"}, "message": {"value": "質問1"}}]})
                hist = _run(kd.k_chat_history("U1"))
                self.assertEqual(list(hist), list(_run(cr.get_recent_chat_history("U1"))))
                self.assertEqual([m["content"] for m in hist], ["質問1", "返信2"])
                self.assertEqual(self._go(fake, record_id="507"), "generated")
                self.assertEqual(fake.calls[-1]["messages"][:2], list(hist))
        finally:
            self._patches[1].start()
            self._patches[-2].start()

    def test_bk01_normal_absence_proceeds(self):
        """fix1 BK-01: App 21 に該当なし（None）・履歴 0 件・env 未設定は正常な不存在＝空の案件情報で生成に進む。"""
        fake = _fake_model(GOOD_DRAFT)
        with patch.object(cr, "get_app21_record", AsyncMock(return_value=None)), patch.object(cr, "APP_CHATLOG", ""):
            self._patches[-2].stop()
            try:
                self.assertEqual(self._go(fake, record_id="508", name="（ヒアリング中）"), "generated")
            finally:
                self._patches[-2].start()
        self.assertEqual(len(fake.calls), 1)
        self.assertIn("（未登録）", fake.calls[0]["prompt"])
        self.assertEqual(fake.calls[0]["messages"], [{"role": "user", "content": CONSULT}])

    def test_guards_reject_and_assertive_is_flag_only(self):
        cases = {
            "510": ("あ" * 601, "guard:length"),
            "511": ("ご質問の点は確認のうえご連絡します。", "guard:evasive"),
            "512": ("担当の田中からお電話します。番号は 090-1234-5678 です。", "guard:residue"),
            "513": ("{{customer_name}}様、ご連絡ありがとうございます。", "guard:placeholder"),
        }
        for rid, (text, reason) in cases.items():
            self.assertEqual(self._go(_fake_model(text), record_id=rid), "rejected", rid)
        self.assertEqual(self._go(_fake_model("最後の返済から5年を過ぎているので、確実に時効が成立します。業者からの書面は届いていますか。"), record_id="514"), "generated")
        by = {r["record_id"]: r for r in self._rows()}
        for rid, (_t, reason) in cases.items():
            self.assertEqual((by[rid]["state"], by[rid]["reason"], by[rid]["text"]), ("rejected", reason, None), rid)
            self.assertIsNotNone(by[rid]["input_tokens"])                                 # 費用は落とした件でも記録
        self.assertEqual(by["514"]["state"], "generated")
        self.assertEqual(json.loads(by["514"]["flags"]), [kd.FLAG_ASSERTIVE])
        self.assertEqual(self.notify.await_count, 0)

    def test_customer_own_name_is_allowed_in_residue_check(self):
        text = "山田 太郎様\nアコムからの請求について、最後の返済が2019年3月であれば時効援用できる可能性が高いです。裁判所からの書類は届いていますか。"
        out, reason, flags = kd.inspect_text(text, customer_name="山田 太郎")
        self.assertEqual((reason, flags), (None, []))
        self.assertEqual(out, text)

    EXEMPLARS = [{"id": 7, "category_id": "挨拶・雑談", "inbound_text": "こんにちは", "human_reply": "手本の返信", "active": True}]

    def _seed_cost(self, yen: float, record_id: str = "1"):
        async def _seed():
            now = kd._now()
            async with db.session_scope() as s:
                await s.execute(sa.insert(kd.reply_k_draft).values(record_id=record_id, business="jikou", model="claude-fable-5-1",
                                                                     rules_version="v1.5", rules_sha=RULES_SHA256, state="generated",
                                                                     cost_yen=yen, created_at=now, updated_at=now))
        _run(_seed())

    def _request(self, fake, record_id: str):
        """1 リクエスト分（同じ文脈）: 一次生成の前に手本の付加先を決め、起票後に後追いを起動する。戻り値 (note, task_state)。"""
        async def _t():
            note = await rx.prompt_note("jikou", "受付")
            task = kd.after_queue(record_id, user_id="U0123456789abcdef0123456789abcdef", customer_name="山田 太郎",
                                  customer_message=CONSULT, category="挨拶・雑談")
            return str(note), (await task) if task is not None else None
        with patch.object(rx, "_db_configured", lambda: True), patch.object(rx, "list_exemplars", AsyncMock(return_value=self.EXEMPLARS)), \
             patch.object(kd, "_call_model", fake):
            return _run(_t())

    def test_bk02_cap_reached_is_exactly_off(self):
        """fix1 BK-02: 上限到達＋承認済み手本あり → OFF と完全に同じ（手本は一次生成に付く・K 下書きなし・行も作らない）。通知だけ 1 日 1 回。"""
        self._seed_cost(1600.0)
        fake = _fake_model(GOOD_DRAFT)
        note, st = self._request(fake, "520")
        self.assertIn("手本の返信", note)                                                   # 手本は一次生成に付く（→ 承認固定・BQ-01）
        self.assertIsNone(st)                                                              # 後追いは起動しない
        self.assertEqual([r["record_id"] for r in self._rows()], ["1"])                      # 行も作らない
        self.assertEqual(fake.calls, [])
        self.notify.assert_awaited_once_with(kd.NOTICE_COST_CAP_KEY)
        with patch.dict(os.environ, {kd.ENV_MODEL: "off"}):                                  # OFF と同じ値
            note_off, st_off = self._request(fake, "521")
        self.assertEqual((note_off, st_off), (note, st))
        self.assertEqual(len(self._rows()), 1)

    def test_bk02_switches_from_next_item_after_cap_mid_month(self):
        """fix1 BK-02: 月の途中で上限に達した **次の 1 件** から OFF に切り替わる（到達した件自身は生成される）。"""
        self._seed_cost(1590.0)
        fake = _fake_model(GOOD_DRAFT)                                                      # 1 通 18 円 → 1608 円で上限到達
        note1, st1 = self._request(fake, "530")
        self.assertEqual((note1, st1), ("", "generated"))                                  # K ON: 手本は一次生成に付かない・K 下書きあり
        self.assertGreaterEqual(_run(kd.month_cost_yen()), 1600.0)
        note2, st2 = self._request(fake, "531")
        self.assertIn("手本の返信", note2)                                                   # 次の 1 件から OFF と同じ
        self.assertIsNone(st2)
        self.assertEqual(sorted(r["record_id"] for r in self._rows()), ["1", "530"])
        self.assertEqual(len(fake.calls), 1)

    def test_bk02_same_value_within_one_request(self):
        """fix1 BK-02: 同じリクエスト内では手本の付加先と後追い起動が同じ値を見る（一次生成の後に上限へ達しても変わらない）。"""
        fake = _fake_model(GOOD_DRAFT)

        async def _t():
            note = await rx.prompt_note("jikou", "受付")                                   # ここで on と決まる
            self._seed_cost_sync = None
            now = kd._now()
            async with db.session_scope() as s:                                            # 一次生成の間に上限へ
                await s.execute(sa.insert(kd.reply_k_draft).values(record_id="1", business="jikou", model="claude-fable-5-1",
                                                                     rules_version="v1.5", rules_sha=RULES_SHA256, state="generated",
                                                                     cost_yen=1600.0, created_at=now, updated_at=now))
            task = kd.after_queue("540", user_id="U0123456789abcdef0123456789abcdef", customer_name="山田 太郎",
                                  customer_message=CONSULT, category="挨拶・雑談")
            return str(note), await task
        with patch.object(rx, "_db_configured", lambda: True), patch.object(rx, "list_exemplars", AsyncMock(return_value=self.EXEMPLARS)), \
             patch.object(kd, "_call_model", fake):
            note, st = _run(_t())
        self.assertEqual((note, st), ("", "generated"))
        self.assertEqual(sorted(r["record_id"] for r in self._rows()), ["1", "540"])

    def test_hook_from_save_to_approval_queue_runs_after_queue(self):
        """起動点: save_to_approval_queue が No. を返した直後。off なら行も作らない。"""
        class _Resp:
            status_code = 200
            is_success = True
            text = ""

            def json(self):
                return {"id": "600"}

        class _Client:
            def __init__(self, **_kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_a):
                return False

            async def post(self, url, headers=None, json=None):
                return _Resp()
        fake = _fake_model(GOOD_DRAFT)

        async def _t():
            rid = await cr.save_to_approval_queue(user_id="U0123456789abcdef0123456789abcdef", customer_name="山田 太郎",
                                                  customer_message=CONSULT, ai_draft="旧下書き", category="時効見立て_条件付き", reason="r")
            await kd.drain_for_tests()
            return rid
        with patch.object(cr.httpx, "AsyncClient", _Client), patch.object(cr, "APP_APPROVAL", "29"), patch.object(cr, "TOKEN_APPROVAL", "t"), \
             patch.object(kd, "_call_model", fake):
            self.assertEqual(_run(_t()), "600")
            rows = self._rows()
            self.assertEqual([(r["record_id"], r["state"]) for r in rows], [("600", "generated")])
            with patch.dict(os.environ, {kd.ENV_MODEL: "off"}):
                self.assertEqual(_run(_t()), "600")
            self.assertEqual(len(self._rows()), 1)                                       # off: 新しい行なし
            self.assertEqual(len(fake.calls), 1)


class TestNotices(_Db):
    def setUp(self):
        super().setUp()
        self._patches[-1].stop()                                                        # notify_once は本物を使う

    def tearDown(self):
        self._patches[-1].start()
        super().tearDown()

    def test_unapproved_text_is_not_sent_and_key_recorded(self):
        sent = AsyncMock(return_value="sent")
        with patch.object(sp, "send_business_notice", sent), patch.object(approved_texts, "notify_unapproved_once", AsyncMock(return_value=True)):
            self.assertEqual(_run(kd.notify_once(kd.NOTICE_FAILED_KEY)), "needs_approval")
        sent.assert_not_awaited()
        self.assertIn(kd.NOTICE_FAILED_KEY, approved_texts.pending_unapproved_keys())
        for key, text in kd.NOTICE_TEXTS.items():
            self.assertNotIn(key, approved_texts.APPROVED_SHA256)                       # 承認は大野の票で（SHA は報告に記載）
            self.assertTrue(text.startswith("【K 下書き】"))

    def test_approved_text_is_sent_once_a_day_via_ledger_key(self):
        sent = AsyncMock(return_value="sent")
        sha = approved_texts.sha256_text(kd.NOTICE_FAILED_TEXT)
        with patch.dict(approved_texts.APPROVED_SHA256, {kd.NOTICE_FAILED_KEY: sha}), patch.object(sp, "send_business_notice", sent):
            self.assertEqual(_run(kd.notify_once(kd.NOTICE_FAILED_KEY)), "sent")
        sent.assert_awaited_once()
        key, text = sent.await_args.args
        self.assertTrue(key.startswith("k_draft_ops_k_draft_failed_v1:20"))              # 台帳 key＝種別＋JST 日付（1 日 1 通）
        self.assertEqual(text, kd.NOTICE_FAILED_TEXT)
        self.assertNotIn("U0123456789abcdef", text)


# ── fix1 BK-02（統合）: 上限到達＋承認済み手本ありの一次生成が OFF と同じ承認経路になる ─────────
class TestBk02ApprovalRoute(unittest.TestCase):
    USER = "U0123456789abcdef0123456789abcdef"

    class _App29Client:
        calls: list = []

        def __init__(self, **_kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_a):
            return False

        async def post(self, url, headers=None, json=None):
            TestBk02ApprovalRoute._App29Client.calls.append(json)

            class _R:
                status_code, is_success, text = 200, True, ""

                def json(self):
                    return {"id": "700"}
            return _R()

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="k1_route_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/r.db", kd.ENV_MODEL: "claude-fable-5-1",
                                            "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d"})
        self._env.start()
        os.environ.pop(sl.POLICY_V2_ENV, None)
        db.reset_for_tests()
        sp.clear_condition_cache()
        approved_texts.reset_pending_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                for md in (kd.metadata, rx.metadata, sl.metadata, rf.metadata, rp.metadata, InboundBase.metadata):
                    await c.run_sync(md.create_all)
        _run(_create())
        self._App29Client.calls = []
        self._patches = [
            patch.object(line_channel.httpx, "AsyncClient", self._App29Client),
            patch.object(cr.httpx, "AsyncClient", self._App29Client),
            patch.object(cr, "APP_APPROVAL", "29"), patch.object(cr, "TOKEN_APPROVAL", "t"),
            patch.object(sp, "_push_business_http", AsyncMock(return_value=True)),
            patch.object(autoreply_stoplist, "is_suppressed", AsyncMock(return_value=False)),
            patch.object(cr, "save_to_chatlog", AsyncMock(return_value="800")),
            patch.object(cr, "_notify_attorney", AsyncMock()),
            patch.object(cr, "get_recent_chat_history", AsyncMock(return_value=cr.ChatHistory())),
            patch.object(kd, "k_chat_history", AsyncMock(return_value=cr.ChatHistory())),
            patch.object(cr, "get_app21_record", AsyncMock(return_value=APP21)),
            patch.object(kd, "notify_once", AsyncMock(return_value="sent")),
        ]
        for p in self._patches:
            p.start()
        self.assertEqual(_run(rx.save_exemplar(business="jikou", inbound_text="こんにちは", human_reply="手本の返信", category_id="挨拶・雑談")), "saved")
        xid = _run(rx.list_for_view("jikou", include_inactive=True))[0]["id"]
        self.assertTrue(_run(rx.approve_exemplar(int(xid))))

    def tearDown(self):
        for p in self._patches:
            p.stop()
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _k_rows(self):
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(kd.reply_k_draft))).fetchall()]
        return _run(_q())

    def _message(self, fake):
        seen = {}

        async def _compose_spy(system_prompt, messages, tool=None):
            seen["prompt"] = system_prompt
            return {"reply": "ご案内します", "category": "挨拶・雑談", "auto_send": True, "jikou_update_flag": False,
                    "immediate_notice": "none", "reason": "r"}
        sent = AsyncMock(return_value=sl.SEND_SENT)

        async def _t():
            await cr.handle_customer_message(user_id=self.USER, user_message="こんにちは", reply_token="rt", app21_record=APP21, reply_func=sent)
            await kd.drain_for_tests()
        with patch.object(cr, "_call_compose_reply", _compose_spy), patch.object(kd, "_call_model", fake):
            _run(_t())
        return seen["prompt"], [c.args[1] for c in sent.await_args_list]

    def _seed_cost(self, yen):
        async def _seed():
            now = kd._now()
            async with db.session_scope() as s:
                await s.execute(sa.insert(kd.reply_k_draft).values(record_id="1", business="jikou", model="claude-fable-5-1",
                                                                     rules_version="v1.5", rules_sha=RULES_SHA256, state="generated",
                                                                     cost_yen=yen, created_at=now, updated_at=now))
        _run(_seed())

    def test_cap_reached_matches_off_route(self):
        """上限到達: 手本は一次生成に付き（prompt に手本）→ 承認固定（生成本文の顧客送信 0・App 29 起票）→ K 下書きなし（行なし）＝OFF と同じ。"""
        self._seed_cost(1600.0)
        fake = _fake_model(GOOD_DRAFT)
        prompt_cap, texts_cap = self._message(fake)
        self.assertIn("手本の返信", prompt_cap)
        self.assertNotIn("ご案内します", texts_cap)
        self.assertEqual(len(self._App29Client.calls), 1)
        self.assertEqual(self._App29Client.calls[0]["record"]["AI下書き"]["value"], "ご案内します")
        self.assertEqual([r["record_id"] for r in self._k_rows()], ["1"])                    # K 下書きなし（行なし）
        self.assertEqual(fake.calls, [])
        self._App29Client.calls.clear()
        with patch.dict(os.environ, {kd.ENV_MODEL: "off"}):
            prompt_off, texts_off = self._message(fake)
        self.assertEqual(prompt_off, prompt_cap)                                            # OFF と byte 同一の prompt
        self.assertEqual(texts_off, texts_cap)
        self.assertEqual(len(self._App29Client.calls), 1)
        self.assertEqual([r["record_id"] for r in self._k_rows()], ["1"])
        self.assertEqual(fake.calls, [])

    def test_under_cap_k_on_route(self):
        """対照: 上限内なら手本は一次生成に付かず（許可表どおり自動送信）、K 下書きは起票された件にだけ作られる。"""
        fake = _fake_model(GOOD_DRAFT)
        prompt, texts = self._message(fake)
        self.assertNotIn("手本の返信", prompt)                                               # 裁定 3: 手本は K にだけ
        self.assertEqual(texts, ["ご案内します"])                                             # 許可表どおり自動送信（起票なし → K も起動しない）
        self.assertEqual(self._App29Client.calls, [])
        self.assertEqual(self._k_rows(), [])
        self.assertEqual(fake.calls, [])


# ── 返信画面・統合一覧（純関数） ───────────────────────────────────────────────────
class TestViews(unittest.TestCase):
    REC = _rec(line_user_id="U0123456789abcdef0123456789abcdef", 顧客名="山田 太郎", 顧客メッセージ=CONSULT, カテゴリ="時効見立て_条件付き", AI下書き="旧下書き本文")

    def _page(self, k, held=None):
        return rv._page("500", self.REC, [], "op", "ticket", [], (), held=held, k=k)

    def test_generated_text_is_initial_value_with_badge(self):
        html = self._page({"state": "generated", "text": "K本文です。", "flags": [kd.FLAG_ASSERTIVE]})
        self.assertIn('<textarea name="text" maxlength="', html)
        self.assertIn(">K本文です。</textarea>", html)
        self.assertNotIn(">旧下書き本文</textarea>", html)
        self.assertIn(kd.BADGE_GENERATED, html)
        self.assertIn("要確認: 断定表現", html)

    def test_without_k_row_page_is_unchanged(self):
        self.assertEqual(self._page(None), rv._page("500", self.REC, [], "op", "ticket", [], ()))
        self.assertIn(">旧下書き本文</textarea>", self._page(None))
        self.assertNotIn("K 下書き", self._page(None))

    def test_failed_and_rejected_keep_old_draft_with_badge(self):
        for k, badge in (({"state": "failed", "reason": "model:unavailable", "flags": []}, "K 下書きなし（モデル応答なし）"),
                         ({"state": "skipped", "reason": "model:invalid", "flags": []}, "K 下書きなし（設定を確認）"),
                         ({"state": "rejected", "reason": "guard:length", "flags": []}, "K 下書きなし（検査で除外）"),
                         ({"state": "started", "reason": None, "flags": []}, None)):
            html = self._page(k)
            self.assertIn(">旧下書き本文</textarea>", html)
            if badge:
                self.assertIn(badge, html)
            else:
                self.assertNotIn("K 下書き", html)

    def test_held_text_wins_over_k_text(self):
        html = self._page({"state": "generated", "text": "K本文", "flags": []}, held="編集中の本文")
        self.assertIn(">編集中の本文</textarea>", html)

    def test_inbox_badge(self):
        base = {"old": False, "attending": False, "record_id": "5", "reason": "", "route": "", "customer_send": "", "category": ""}
        self.assertIn("K 下書きあり", iv._badges(dict(base, k_draft="generated")))
        self.assertNotIn("K 下書き", iv._badges(dict(base, k_draft="failed")))
        self.assertNotIn("K 下書き", iv._badges(base))

    def test_badges_closed_vocabulary(self):
        self.assertEqual(kd.badges(None), [])
        self.assertEqual(kd.badges({"state": "generated", "flags": []}), [kd.BADGE_GENERATED])
        self.assertEqual(kd.badges({"state": "failed", "reason": "input:unavailable"}), ["K 下書きなし（案件情報を取得できず）"])


# ── (d) migration（sqlite・本番と同じ e3f5 → head） ────────────────────────────────
class TestMigration(unittest.TestCase):
    def _tools(self):
        from test_db_foundation import TestAlembicScaffold
        d = tempfile.mkdtemp(prefix="k1_mig_")
        dbfile = f"{d}/mig.db"
        alembic, tables, unique_cols, nullable = TestAlembicScaffold._brain_mig_tools(dbfile)
        return d, dbfile, alembic, tables, nullable

    def test_upgrade_from_e3f5_to_head_and_downgrade(self):
        import sqlite3
        d, dbfile, alembic, tables, nullable = self._tools()
        try:
            self.assertEqual(alembic("heads").stdout.count("(head)"), 1)
            self.assertIn(MIG, alembic("heads").stdout)
            self.assertEqual(alembic("upgrade", MERGE).returncode, 0)                   # 本番の現在地（e3f5）
            self.assertNotIn(kd.TABLE_NAME, tables())
            up = alembic("upgrade", "head")
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertIn(f"{MIG} (head)", alembic("current").stdout)
            self.assertIn(kd.TABLE_NAME, tables())
            con = sqlite3.connect(dbfile)
            try:
                cols = {r[1] for r in con.execute(f'PRAGMA table_info("{kd.TABLE_NAME}")')}
                self.assertEqual(cols, {c.name for c in kd.reply_k_draft.columns})
                nn = nullable(con, kd.TABLE_NAME)
                self.assertFalse(nn["record_id"]); self.assertFalse(nn["model"]); self.assertFalse(nn["state"]); self.assertFalse(nn["rules_sha"])
                self.assertTrue(nn["text"]); self.assertTrue(nn["pending_id"]); self.assertTrue(nn["exemplar_ids"])
                idx = {r[1]: bool(r[2]) for r in con.execute(f'PRAGMA index_list("{kd.TABLE_NAME}")')}
                self.assertIn("ix_reply_k_draft_state", idx)
                self.assertIn("ix_reply_k_draft_created", idx)
                self.assertTrue(any(u for name, u in idx.items() if "record" in name or u))   # record_id の UNIQUE
            finally:
                con.close()
            down = alembic("downgrade", MERGE)
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-800:]}")
            self.assertNotIn(kd.TABLE_NAME, tables())
            self.assertIn(MERGE, alembic("current").stdout)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_downgrade_refused_when_data_present(self):
        import sqlite3
        d, dbfile, alembic, tables, _n = self._tools()
        try:
            self.assertEqual(alembic("upgrade", "head").returncode, 0)
            con = sqlite3.connect(dbfile)
            try:
                con.execute("INSERT INTO reply_k_draft (record_id, business, model, rules_version, rules_sha, state, created_at, updated_at) "
                            "VALUES ('77', 'jikou', 'claude-fable-5-1', 'v1.5', 'x', 'generated', '2026-11-09 00:00:00', '2026-11-09 00:00:00')")
                con.commit()
                with self.assertRaises(sqlite3.IntegrityError):                            # 1 件 1 行（UNIQUE）
                    con.execute("INSERT INTO reply_k_draft (record_id, business, model, rules_version, rules_sha, state, created_at, updated_at) "
                                "VALUES ('77', 'jikou', 'claude-fable-5-1', 'v1.5', 'x', 'generated', '2026-11-09 00:00:00', '2026-11-09 00:00:00')")
            finally:
                con.close()
            down = alembic("downgrade", MERGE)
            self.assertNotEqual(down.returncode, 0)
            self.assertIn("reply_k_draft_downgrade_refused_data_present", down.stderr)
            self.assertIn(kd.TABLE_NAME, tables())
        finally:
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
