"""AUTOREPLY-STOPLIST-1: 自動返信の個人別停止リスト（方針(C)専用アプリ+B併用）。

固定する契約:
- is_suppressed: env 未設定・userId grammar 外・照会失敗はすべて False
  （fail-open・裁定済み＝顧客対応の穴を作らない）。検証済み userId のみを
  query へ埋める。照会失敗 3 連続で throttle つき管理者警報・成功でリセット
- 受信経路: 停止該当 userId は自動返信 0 件（App21 参照・Claude・LINE 送信・
  承認キュー投入に到達しない）。受信記録＋管理者通知は pause 経路の実装を
  共用（冪等キー prefix=stoplist）。durable 経路は event id つき・二重照会なし
- 承認キュー経路: 停止中 userId への承認済み下書きの自動送信を抑止
  （LINE push 0・送信済み=no のまま・sending marker も取得しない）。
  照会失敗は fail-open（送信継続）
- 追跡: count_suppressed が匿名ID 単位で計数（userId 生値はログへ出さない）
- 書込み経路なし: hub/autoreply_stoplist は kintone 読み取りのみ（AST pin）
"""

import ast
import asyncio
import os
import re
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from ast_policy_helpers import (
    _FORBIDDEN_IMPORTS,
    _binding_violations,
    _readonly_violations,
)

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from config import EXPECTED_KINTONE_SCHEMA  # noqa: E402
from hub import autoreply_stoplist as sl  # noqa: E402

_client = TestClient(main.app)

UID = "U" + "0123456789abcdef" * 2          # grammar 適合の userId
_STOP_ENV = {"APP_AUTOREPLY_STOP": "39", "TOKEN_AUTOREPLY_STOP": "t39"}


def _run(coro):
    return asyncio.run(coro)


class _ResetMixin(unittest.TestCase):
    def setUp(self):
        sl._failure_state["consecutive"] = 0
        sl._failure_state["last_alert_monotonic"] = None
        sl._suppressed_counts.clear()

    tearDown = setUp


# ── is_suppressed（fail-open・grammar・query 形） ────────────────────────────
class TestIsSuppressed(_ResetMixin):
    def test_env_unset_returns_false_without_query(self):
        search = AsyncMock()
        with patch.dict(os.environ, {"APP_AUTOREPLY_STOP": "",
                                     "TOKEN_AUTOREPLY_STOP": ""}), \
             patch.object(sl.kintone, "search_records", search):
            self.assertFalse(_run(sl.is_suppressed(UID)))
        search.assert_not_awaited()

    def test_invalid_user_id_returns_false_without_query(self):
        search = AsyncMock()
        with patch.dict(os.environ, _STOP_ENV), \
             patch.object(sl.kintone, "search_records", search):
            for bad in ("", None, "abc", "U" + "Z" * 32, UID + "x",
                        'U" or 1 limit 1 --'):
                with self.subTest(bad=bad):
                    self.assertFalse(_run(sl.is_suppressed(bad)))
        search.assert_not_awaited()

    def test_hit_true_miss_false_with_exact_query(self):
        search = AsyncMock(return_value=[{"$id": {"value": "1"}}])
        with patch.dict(os.environ, _STOP_ENV), \
             patch.object(sl.kintone, "search_records", search):
            self.assertTrue(_run(sl.is_suppressed(UID)))
        app, query = search.call_args.args[:2]
        self.assertIs(app, sl.APP_AUTOREPLY_STOP)
        self.assertEqual(query, f'LINE_userId = "{UID}" limit 1')
        self.assertEqual(search.call_args.kwargs.get("fields"), ["$id"])
        search = AsyncMock(return_value=[])
        with patch.dict(os.environ, _STOP_ENV), \
             patch.object(sl.kintone, "search_records", search):
            self.assertFalse(_run(sl.is_suppressed(UID)))

    def test_lookup_failure_fails_open(self):
        search = AsyncMock(side_effect=RuntimeError("kintone down"))
        with patch.dict(os.environ, _STOP_ENV), \
             patch.object(sl.kintone, "search_records", search):
            self.assertFalse(_run(sl.is_suppressed(UID)))   # 止めない
        self.assertEqual(sl._failure_state["consecutive"], 1)

    def test_alert_on_third_consecutive_failure_with_throttle(self):
        notify = AsyncMock(return_value=True)
        search = AsyncMock(side_effect=RuntimeError("down"))
        with patch.dict(os.environ, _STOP_ENV), \
             patch.object(sl.kintone, "search_records", search), \
             patch("hub.notify.notify_admin_line", notify):
            _run(sl.is_suppressed(UID))
            _run(sl.is_suppressed(UID))
            notify.assert_not_awaited()          # 2 連続では警報しない
            _run(sl.is_suppressed(UID))
            self.assertEqual(notify.await_count, 1)   # 3 連続で警報
            _run(sl.is_suppressed(UID))
            self.assertEqual(notify.await_count, 1)   # throttle 内は再送しない
            # interval 経過後は再警報
            sl._failure_state["last_alert_monotonic"] -= \
                sl.FAILURE_ALERT_INTERVAL_SEC + 1
            _run(sl.is_suppressed(UID))
            self.assertEqual(notify.await_count, 2)

    def test_success_resets_consecutive_failures(self):
        with patch.dict(os.environ, _STOP_ENV), \
             patch.object(sl.kintone, "search_records",
                          AsyncMock(side_effect=[RuntimeError("x"),
                                                 RuntimeError("x"), []])):
            _run(sl.is_suppressed(UID))
            _run(sl.is_suppressed(UID))
            self.assertEqual(sl._failure_state["consecutive"], 2)
            _run(sl.is_suppressed(UID))
        self.assertEqual(sl._failure_state["consecutive"], 0)

    def test_count_suppressed_tracks_per_anon_user(self):
        n1 = sl.count_suppressed(UID)
        n2 = sl.count_suppressed(UID)
        other = "U" + "f" * 32
        n3 = sl.count_suppressed(other)
        self.assertEqual((n1, n2, n3), (1, 2, 1))
        self.assertNotIn(UID, str(sl._suppressed_counts))   # 生値を保持しない


# ── 受信経路の結線 ───────────────────────────────────────────────────────────
class TestInboundWiring(_ResetMixin):
    def test_suppressed_user_gets_no_autoreply_pipeline(self):
        handled = AsyncMock()
        app21 = AsyncMock()
        with patch.dict(os.environ, {**_ENV, **_STOP_ENV,
                                     "AUTOREPLY_PAUSED": ""}), \
             patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(return_value=True)), \
             patch.object(main, "_handle_paused_inbound", handled), \
             patch.object(main, "get_app21_record", app21):
            _run(main._process_line_event("rt", UID, "こんにちは"))
        # 記録経路は pause 実装の共用・prefix=stoplist
        handled.assert_awaited_once_with(UID, "こんにちは", None,
                                         idem_prefix="stoplist")
        app21.assert_not_awaited()      # App21 参照にも到達しない
        # 計数された（匿名ID 単位）
        self.assertEqual(sum(sl._suppressed_counts.values()), 1)

    def test_not_suppressed_proceeds_to_normal_routing(self):
        app21 = AsyncMock(return_value=None)
        with patch.dict(os.environ, {**_ENV, **_STOP_ENV,
                                     "AUTOREPLY_PAUSED": ""}), \
             patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(return_value=False)), \
             patch.object(main, "get_app21_record", app21):
            try:
                _run(main._process_line_event("rt", UID, "こんにちは"))
            except Exception:
                pass                    # 下流 mock 不足の例外は本テストの対象外
        app21.assert_awaited()          # 通常ルーティングへ進んだ

    def test_process_line_event_signature_unchanged(self):
        # durable テストの patch 契約（3 引数で丸ごと差し替え）を壊さない
        import inspect
        params = list(inspect.signature(
            main._process_line_event).parameters)
        self.assertEqual(params, ["reply_token", "user_id", "user_text"])

    def test_durable_wrapper_checks_with_event_id(self):
        suppressed = AsyncMock(return_value=True)
        handled = AsyncMock()
        completed = AsyncMock()
        with patch.dict(os.environ, {**_ENV, **_STOP_ENV,
                                     "AUTOREPLY_PAUSED": ""}), \
             patch.object(main.autoreply_stoplist, "is_suppressed",
                          suppressed), \
             patch.object(main, "_handle_suppressed_inbound", handled), \
             patch("hub.durable_inbound.mark_line_processing",
                   AsyncMock(return_value=True)), \
             patch("hub.durable_inbound.mark_line_completed", completed):
            _run(main._process_line_event_durable("rt", UID, "x", "evt-1"))
        handled.assert_awaited_once_with(UID, "x", "evt-1")
        completed.assert_awaited_once()

    def test_paused_takes_precedence_over_stoplist(self):
        # 全体停止 ON のとき停止リスト照会にすら行かない（判定順序の pin）
        checker = AsyncMock()
        paused = AsyncMock()
        with patch.dict(os.environ, {**_ENV, "AUTOREPLY_PAUSED": "1"}), \
             patch.object(main.autoreply_stoplist, "is_suppressed", checker), \
             patch.object(main, "_handle_paused_inbound", paused):
            _run(main._process_line_event("rt", UID, "x"))
        paused.assert_awaited_once_with(UID, "x", None)
        checker.assert_not_awaited()


# ── STOPLIST-fix1（STOPLIST-01）: durable 反転窓の event_id 引き継ぎ ─────────
class TestDurableEventIdCarryover(_ResetMixin):
    def test_inner_flip_keeps_event_id_and_retry_is_idempotent(self):
        """外側判定 False →（App39 登録の反転窓）→ 内側判定 True でも、
        ContextVar 経由の event_id で冪等キー stoplist:{event_id} が成立し、
        通知失敗 → durable failed → retry で App28 増分 0・最終該当行 1 件。"""
        rows = []

        async def fake_create(app, fields):
            rows.append(dict(fields))

        async def fake_search(app, query, fields=None):
            # _paused_chatlog_already_saved の完全一致検索を再現
            m = re.match(r'category = "([^"]*)"', query)
            key = m.group(1) if m else None
            return ([{"$id": {"value": "1"}}]
                    if any(r.get("category") == key for r in rows) else [])

        notify = AsyncMock(side_effect=[RuntimeError("line down"), True])
        failed = AsyncMock()
        completed = AsyncMock()
        env = {**_ENV, **_STOP_ENV, "AUTOREPLY_PAUSED": "",
               "APP_CHATLOG": "28", "TOKEN_CHATLOG": "t28"}
        with patch.dict(os.environ, env), \
             patch.object(main, "ATTORNEY_LINE_USER_ID", "U" + "a" * 32), \
             patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(side_effect=[False, True, True])), \
             patch.object(main, "get_app21_record",
                          AsyncMock(return_value=None)), \
             patch.object(main.hub_kintone, "create_record", fake_create), \
             patch.object(main.hub_kintone, "search_records", fake_search), \
             patch("hub.notify.notify_business", notify), \
             patch("hub.durable_inbound.mark_line_processing",
                   AsyncMock(return_value=True)), \
             patch("hub.durable_inbound.mark_line_completed", completed), \
             patch("hub.durable_inbound.mark_line_failed", failed):
            # 1回目: 外側 False → 内側 True（反転窓）。保存成功・通知失敗
            _run(main._process_line_event_durable("rt", UID, "相談です",
                                                  "evt-1"))
            failed.assert_awaited_once()              # durable failed へ
            completed.assert_not_awaited()
            self.assertEqual(len(rows), 1)            # App28 保存は成立
            self.assertEqual(rows[0]["category"], "stoplist:evt-1")  # 完全一致
            # retry（外側 True・already_claimed）: 冪等確認が既存行を発見
            _run(main._process_line_event_durable("rt", UID, "相談です",
                                                  "evt-1",
                                                  already_claimed=True))
            completed.assert_awaited_once()           # 今回は成功
            self.assertEqual(len(rows), 1)            # App28 増分 0・最終 1 件
            self.assertEqual(
                [r["category"] for r in rows], ["stoplist:evt-1"])

    def test_non_durable_context_stays_none(self):
        # 非 durable 文脈では ContextVar は既定 None（従来どおり冪等キーなし）
        handled = AsyncMock()
        with patch.dict(os.environ, {**_ENV, **_STOP_ENV,
                                     "AUTOREPLY_PAUSED": ""}), \
             patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(return_value=True)), \
             patch.object(main, "_handle_suppressed_inbound", handled):
            _run(main._process_line_event("rt", UID, "x"))
        handled.assert_awaited_once_with(UID, "x", None)


# ── 承認キュー経路の抑止 ─────────────────────────────────────────────────────
class TestApprovalSuppression(unittest.TestCase):
    def _post(self, *, suppressed, record):
        push = AsyncMock()
        mark = AsyncMock()
        chatlog = AsyncMock()
        with patch.dict(os.environ, {**_ENV, **_STOP_ENV,
                                     "KINTONE_EVENT_DEDUP_ENABLED": ""}), \
             patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(return_value=suppressed)), \
             patch.object(main.hub_kintone, "get_record",
                          AsyncMock(return_value=record)), \
             patch.object(main, "send_line_push", push), \
             patch.object(main, "mark_approval_sent", mark), \
             patch.object(main, "save_to_chatlog", chatlog):
            r = _client.post(
                "/webhook/kintone/approval?token=kintone-token",
                json={"record": {"$id": {"value": "5"},
                                 "ステータス2": {"value": "承認済"},
                                 "送信済み": {"value": "no"}}})
        return r, push, mark

    def _record(self):
        return {"ステータス2": {"value": "承認済"},
                "送信済み": {"value": "no"},
                "line_user_id": {"value": UID},
                "AI下書き": {"value": "下書き本文"},
                "カテゴリ": {"value": "c"}}

    def test_suppressed_user_blocks_push_and_keeps_unsent(self):
        r, push, mark = self._post(suppressed=True, record=self._record())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json().get("skip"), "autoreply_stopped")
        push.assert_not_awaited()       # LINE 送信 0
        mark.assert_not_awaited()       # 送信済み=no のまま（解除後に再承認可）

    def test_not_suppressed_sends_as_before(self):
        r, push, mark = self._post(suppressed=False, record=self._record())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json().get("record_id"), "5")
        push.assert_awaited_once_with(UID, "下書き本文")
        mark.assert_awaited_once()


# ── 構造 pin ─────────────────────────────────────────────────────────────────
class TestStructuralPins(unittest.TestCase):
    def test_schema_registered_with_optional(self):
        spec = EXPECTED_KINTONE_SCHEMA["App 39 (自動返信停止リスト)"]
        self.assertEqual(spec["app_id_env"], "APP_AUTOREPLY_STOP")
        self.assertEqual(spec["token_env"], "TOKEN_AUTOREPLY_STOP")
        self.assertTrue(spec["optional"])       # env 投入前の healthcheck 警報なし
        self.assertEqual(spec["fields"], {
            "LINE_userId": {"type": "SINGLE_LINE_TEXT"},
            "表示名": {"type": "SINGLE_LINE_TEXT"},
            "停止理由": {"type": "MULTI_LINE_TEXT"},
            "登録日": {"type": "DATE"},
        })

    @staticmethod
    def _imports(tree):
        out = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                out |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom):
                out.add((node.module or "").split(".")[0])
        return out

    def test_stoplist_module_passes_p4_readonly_checker(self):
        # STOPLIST-fix1（STOPLIST-02）: P4 系 read-only checker（許可文脈の
        # 閉集合方式=未知の kintone 属性・private _write・alias/属性直 import・
        # 動的アクセスはすべて fail-closed）＋HTTP/プロセス系 import 禁止を共用
        tree = ast.parse(
            Path("hub/autoreply_stoplist.py").read_text(encoding="utf-8"))
        self.assertEqual(_readonly_violations(tree), [])
        self.assertEqual(_binding_violations(tree), [])
        self.assertFalse(self._imports(tree) & _FORBIDDEN_IMPORTS)
        # 使用する kintone 属性は read の 2 種のみ（許可集合のさらに部分集合）
        attrs = {node.attr for node in ast.walk(tree)
                 if isinstance(node, ast.Attribute)
                 and isinstance(node.value, ast.Name)
                 and node.value.id == "kintone"}
        self.assertEqual(attrs, {"KintoneApp", "search_records"})

    def test_checker_detects_write_and_bypass_forms(self):
        # checker 単体の negative 対照（STOPLIST-02 指定水準）: 既存 write API・
        # private _write・未知 attr（fail-closed）・import alias・属性直 import・
        # HTTP client 直呼び・動的アクセスの各形が違反として検出される
        cases = {
            "既存 write API": 'from hub import kintone\n'
                              'async def f(a):\n'
                              '    await kintone.create_records(a, [])\n',
            "update_record": 'from hub import kintone\n'
                             'async def f(a):\n'
                             '    await kintone.update_record(a, "1", {})\n',
            "private _write": 'from hub import kintone\n'
                              'async def f(a):\n'
                              '    await kintone._write("POST", a, {})\n',
            "未知 attr=fail-closed": 'from hub import kintone\n'
                                     'async def f(a):\n'
                                     '    await kintone.bulk_delete(a)\n',
            "import alias": 'from hub import kintone as kt\n'
                            'async def f(a):\n'
                            '    await kt.create_record(a, {})\n',
            "属性直 import": 'from hub.kintone import create_record\n',
            "HTTP 直呼び": 'from hub import kintone\nimport httpx\n',
            "動的アクセス": 'from hub import kintone\n'
                            'g = getattr(kintone, "create_record")\n',
        }
        for label, src in cases.items():
            with self.subTest(case=label):
                tree = ast.parse(src)
                violations = (_readonly_violations(tree)
                              + _binding_violations(tree))
                forbidden = self._imports(tree) & _FORBIDDEN_IMPORTS
                self.assertTrue(violations or forbidden, label)

    def test_approval_check_precedes_sending_marker(self):
        # 承認経路: 停止判定は sending marker より前（marker を汚さない）
        src = Path("main.py").read_text(encoding="utf-8")
        idx_check = src.index("skip_autoreply_stopped")
        idx_marker = src.index("mark_sending(_ev)")
        self.assertLess(idx_check, idx_marker)


if __name__ == "__main__":
    unittest.main()
