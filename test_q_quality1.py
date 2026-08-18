"""Q-QUALITY-1: 出典照合の構造的不一致の解消（案D enum 閉集合＋案A 引用キー開示）。

背景（Q-QUALITY-DIAG 判定(d)）: サーバ実測出典の app ラベルがモデルに非開示
だったため、モデルが tool description 由来の表記で申告 → subset 照合が機械的に
REJECT → 全質問 no_source の系統欠陥。

固定する仕様:
- SOURCE_APP_LABELS がサーバ実測記録（_record_source の全呼出しラベル）と
  submit_answer schema の enum の共通の正（乖離したら本テストが落ちる）。
- 各 tool 結果に _citation_keys（app ラベル＋record_id）を明示して返す。
- e2e: tool 結果の _citation_keys をそのまま submit した回答が status=ok
  （照合 PASS）になる——系統欠陥の回帰防止。
- 照合の厳密性は不変: enum 外・実測集合外ラベルは従来どおり fail-closed。
"""

import ast
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from test_q_batch1 import (
    _ENV,
    _rec,
    _resp,
    _run,
    _submit,
    _tool_use,
)

import hub.webapp_q as wq

import os


def _ctx():
    return {"sources": [], "source_keys": set(), "flags": set()}


# ── 再発防止 pin: ラベル定義・enum・実測記録の三者一致 ───────────────────────
class TestLabelClosedSet(unittest.TestCase):
    def _record_source_literals(self):
        src = Path("hub/webapp_q.py").read_text(encoding="utf-8")
        labels = set()
        for node in ast.walk(ast.parse(src)):
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "_record_source"):
                self.assertGreaterEqual(len(node.args), 2,
                                        "位置引数でラベルを渡す流儀の維持")
                label = node.args[1]
                self.assertIsInstance(
                    label, ast.Constant,
                    "_record_source のラベルはリテラル固定（動的組立て禁止）")
                labels.add(label.value)
        return labels

    def test_measured_labels_match_closed_set(self):
        # サーバ実測記録の全ラベル == SOURCE_APP_LABELS（乖離したら落ちる）
        self.assertEqual(self._record_source_literals(),
                         set(wq.SOURCE_APP_LABELS))

    def test_submit_schema_enum_matches_closed_set(self):
        app_schema = (wq._SUBMIT_TOOL["input_schema"]["properties"]
                      ["source_refs"]["items"]["properties"]["app"])
        self.assertEqual(app_schema["enum"], list(wq.SOURCE_APP_LABELS))
        self.assertTrue(wq._SUBMIT_TOOL["strict"])   # enum は strict で強制される

    def test_labels_unique_and_nonempty(self):
        labels = wq.SOURCE_APP_LABELS
        self.assertEqual(len(labels), len(set(labels)))
        self.assertEqual(len(labels), 8)
        self.assertTrue(all(isinstance(x, str) and x for x in labels))


# ── 案A: _citation_keys の開示 ───────────────────────────────────────────────
class TestCitationKeysExposed(unittest.TestCase):
    def test_heirs_result_carries_citation_keys(self):
        heirs = {"records": [_rec(**{"$id": "201", "氏名": "山田一郎",
                                     "戸籍確認済": "yes"})],
                 "excluded_cancelled_count": 0}
        ctx = _ctx()
        with patch.dict(os.environ, _ENV), \
             patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(return_value=heirs)):
            content, is_error = _run(wq._dispatch(
                "list_case_heirs", {"case_record_id": "12"}, ctx))
        self.assertFalse(is_error)
        keys = json.loads(content)["_citation_keys"]
        self.assertEqual(keys, [{"app": "App36(相続人)", "record_id": "201"}])
        # 開示キー == サーバ実測キー（同一の正）
        self.assertEqual(keys, [{"app": s["app"], "record_id": s["record_id"]}
                                for s in ctx["sources"]])

    def test_souzoku_list_result_carries_citation_keys(self):
        records = [_rec(**{"$id": "3", "氏名": "熊澤花子",
                           "被相続人名": "熊澤太郎"})]
        ctx = _ctx()
        with patch.dict(os.environ, _ENV), \
             patch.object(wq.kintone, "search_records",
                          AsyncMock(return_value=records)):
            content, is_error = _run(wq._dispatch(
                "list_souzoku_cases", {}, ctx))
        self.assertFalse(is_error)
        keys = json.loads(content)["_citation_keys"]
        self.assertEqual(keys,
                         [{"app": "相談カード(相続)", "record_id": "3"}])

    def test_too_large_result_discards_keys_too(self):
        huge = {"records": [_rec(**{"$id": str(200 + i), "氏名": "山" * 200})
                            for i in range(300)],
                "excluded_cancelled_count": 0}
        ctx = _ctx()
        with patch.dict(os.environ, _ENV), \
             patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(return_value=huge)):
            content, is_error = _run(wq._dispatch(
                "list_case_heirs", {"case_record_id": "12"}, ctx))
        self.assertTrue(is_error)
        self.assertEqual(content, wq.TOO_LARGE_RESULT)   # キーも開示されない
        self.assertEqual(ctx["sources"], [])


# ── e2e: 開示キーをそのまま submit → 照合 PASS → ok（系統欠陥の回帰防止） ──
class TestEndToEndOkWithExposedKeys(unittest.TestCase):
    def test_model_echoing_citation_keys_gets_ok(self):
        heirs = {"records": [_rec(**{"$id": "201", "氏名": "山田一郎",
                                     "続柄": "子", "戸籍確認済": "no"})],
                 "excluded_cancelled_count": 0}
        calls = []

        async def fake_create(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return _resp("tool_use", [_tool_use(
                    "list_case_heirs", {"case_record_id": "12"})])
            # 2 turn 目: 直前の tool_result から _citation_keys を取り出し
            # **そのまま** submit する（実モデルに期待する挙動の再現）
            tool_result = kwargs["messages"][-1]["content"][0]["content"]
            keys = json.loads(tool_result)["_citation_keys"]
            return _resp("tool_use", [_submit(
                "案件 No.12 の戸籍未確認は山田一郎です。", keys)])

        stub = SimpleNamespace(messages=SimpleNamespace(
            create=AsyncMock(side_effect=fake_create)))
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(return_value=heirs)):
            result = _run(wq._answer_question("熊澤の被相続人って誰？"))
        self.assertEqual(result["status"], "ok")         # 照合 PASS
        self.assertEqual(result["sources"][0]["app"], "App36(相続人)")
        self.assertEqual(result["sources"][0]["record_id"], "201")
        # 申告した enum ラベルが閉集合に含まれることの明示確認
        self.assertIn(result["sources"][0]["app"], wq.SOURCE_APP_LABELS)


# ── 照合の厳密性は不変（緩和なし） ───────────────────────────────────────────
class TestStrictnessUnchanged(unittest.TestCase):
    def test_diag_observed_wrong_label_still_rejected(self):
        # Q-QUALITY-DIAG で実測された誤ラベルは今後も REJECT（照合は不変・
        # enum により発生自体が構造的に排除される側で解消）
        ctx = _ctx()
        wq._record_source(ctx, "相談カード(相続)", "26", "3")
        self.assertIsNone(wq._validated_submission(
            {"answer": "熊澤太郎です。",
             "source_refs": [{"app": "相続案件（相談カード）",
                              "record_id": "3"}]}, ctx))
        # 正ラベルなら採用される（対照）
        ok = wq._validated_submission(
            {"answer": "熊澤太郎です。",
             "source_refs": [{"app": "相談カード(相続)",
                              "record_id": "3"}]}, ctx)
        self.assertIsNotNone(ok)

    def test_unmeasured_record_still_rejected_with_valid_enum_label(self):
        # enum に合致するラベルでも、turn 内で実測していない record は従来
        # どおり fail-closed（照合の厳密性の維持）
        ctx = _ctx()
        wq._record_source(ctx, "App36(相続人)", "36", "201")
        self.assertIsNone(wq._validated_submission(
            {"answer": "回答", "source_refs": [
                {"app": "App36(相続人)", "record_id": "999"}]}, ctx))


# ── Q-QUALITY-1-fix1（Q-QUALITY-01 HIGH）: 実行時防壁 ────────────────────────
class TestRuntimeLabelGuard(unittest.TestCase):
    """_record_source は app_label の閉集合を実行時に必須検証する——AST 走査が
    見えない呼出し方（alias・wrapper・動的組立て）でも閉集合外は即時例外。"""

    def test_direct_call_with_unknown_label_raises(self):
        with self.assertRaises(ValueError):
            wq._record_source(_ctx(), "App99(存在しない)", "99", "1")

    def test_alias_call_raises(self):
        # Codex 失敗例1: alias 代入経由は ast.Name 走査に映らないが、
        # 実行時防壁は値で検証するため例外になる
        alias = wq._record_source
        with self.assertRaises(ValueError):
            alias(_ctx(), "相続案件（相談カード）", "26", "3")

    def test_wrapper_call_raises(self):
        # Codex 失敗例2: wrapper 関数経由
        def wrapper(ctx, label):
            return wq._record_source(ctx, label, "36", "201")
        with self.assertRaises(ValueError):
            wrapper(_ctx(), "App36（相続人）")     # 全角括弧の表記ゆれ

    def test_dynamically_built_unknown_label_raises(self):
        # Codex 失敗例3: 動的組立てラベル（リテラル pin に映らない）
        label = "App" + "34" + "（人物）"           # 全角括弧＝閉集合外
        with self.assertRaises(ValueError):
            wq._record_source(_ctx(), label, "34", "5")

    def test_dynamically_built_known_label_passes(self):
        # 対照: 防壁は値基準——動的組立てでも閉集合の値と一致すれば通る
        ctx = _ctx()
        wq._record_source(ctx, "App34" + "(人物)", "34", "5")
        self.assertEqual(len(ctx["sources"]), 1)

    def test_all_eight_labels_pass(self):
        ctx = _ctx()
        for i, label in enumerate(wq.SOURCE_APP_LABELS, start=1):
            wq._record_source(ctx, label, str(20 + i), str(i))
        self.assertEqual(len(ctx["sources"]), len(wq.SOURCE_APP_LABELS))
        self.assertEqual({s["app"] for s in ctx["sources"]},
                         set(wq.SOURCE_APP_LABELS))

    def test_guard_precedes_grammar_check(self):
        # 閉集合外なら id が不正（黙って無視される形）でも例外が優先＝
        # 「静かな取り零し」に化けない
        with self.assertRaises(ValueError):
            wq._record_source(_ctx(), "App99(存在しない)", "", "")


# ── system prompt の追記（案A 指示＋表記ゆれ・候補提示） ─────────────────────
class TestSystemPromptDirectives(unittest.TestCase):
    def test_citation_key_directive(self):
        self.assertIn("_citation_keys", wq._SYSTEM)
        self.assertIn("そのまま", wq._SYSTEM)

    def test_name_variant_and_candidate_directives(self):
        self.assertIn("旧字/新字", wq._SYSTEM)
        self.assertIn("澤/沢", wq._SYSTEM)
        self.assertIn("こちらのことですか", wq._SYSTEM)
        # 出典なし断定をしない設計の不変（候補も実在レコードのみ）
        self.assertIn("記録に無い名前や番号を作らない", wq._SYSTEM)


if __name__ == "__main__":
    unittest.main()
