"""GATE-EXEMPT-FIX-1: 凍結テンプレの正規化一致免除と原文復元の固定。

背景（LINE-QUALITY-DIAG の主因）: モデルが凍結ブロックの改行 2 か所を削除し
1 か所を読点に変えて出力するため（類似度 0.994）、逐語一致免除が効かず自由文として
字数計上され 300 字超で承認降格していた。

裁定（逐語）:
 A. 免除判定は「正規化後の完全一致」。正規化=改行・空白(半角/全角)・読点「、」・
    句点「。」を除去。それ以外の文字差（一字でも）は不一致=従来どおり自由文扱い
 B. 正規化一致した区間は、顧客に届く文面を凍結テンプレの原文（改行込み・逐語）に置換
 C. 対象は既存の凍結テンプレ集合のみ（本文の追加・変更なし）
 D. 字数計上は置換後の文面から凍結区間を除いた自由文で行う
 E. 置換したことを固定語彙 1 行でログ（テンプレ本文・顧客文は載せない）
 F. 類似度しきい値方式は採用しない
"""

import asyncio
import hashlib
import logging
import os
import unittest
from types import SimpleNamespace
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
    "APP_HOUKI": "40", "TOKEN_HOUKI": "d",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

import main  # noqa: E402
from houki_bot import hearing  # noqa: E402
from hub import houki_case_store  # noqa: E402
from hub import houki_profile as hp  # noqa: E402
from hub import reply_sanitizer as rs  # noqa: E402

JIKOU = main.HEARING_TEMPLATE_BLOCKS
HOUKI = hp.HEARING_TEMPLATE_BLOCKS_HOUKI
USER = "Ugateexempt0000000000000000000001"
FREE_150 = "ご回答ありがとうございます。" + "あ" * 136   # 150 字の自由文
NL = "\n"

# 凍結集合の sha256 pin（T7・本票は本文に触れない）
PIN_JIKOU = "ebd2dea63cb3e81b4f4e95235d49c78ac2933ea6f932ffcd19bff632fc78ab2e"
PIN_HOUKI = "e218c0e5c2ab6caeb55ddcdbb88e6e8d5e14cf30374d1e3094e81911008ffc6f"


def _run(coro):
    return asyncio.run(coro)


def mangle_observed(block: str) -> str:
    """診断で観測した差分: 改行 2 か所削除+改行 1 か所→読点（末尾側の 3 改行）。
    罫線行の直前 3 つの改行を対象にする（テンプレ本文は変えない）。"""
    lines = block.split(NL)
    # 最後の罫線行を除いた本文行のうち、末尾 4 行を [a, b, c, d] とする
    body, last_rule = lines[:-1], lines[-1]
    a, b, c, d = body[-4:]
    joined = a + b + "、" + c + d          # a\nb → ab / b\nc → b、c / c\nd → cd
    out = NL.join(body[:-4] + [joined, last_rule])
    assert out != block
    assert rs.normalize_for_match(out) == rs.normalize_for_match(block)
    return out


def mangle_spaces(block: str) -> str:
    """全角空白・半角空白の混入（正規化で除去される差のみ）。"""
    lines = block.split(NL)
    lines[1] = "　" + lines[1] + " "
    lines[2] = lines[2].replace("", " ", 1) if False else lines[2] + "　"
    out = NL.join(lines)
    assert out != block
    assert rs.normalize_for_match(out) == rs.normalize_for_match(block)
    return out


def mangle_one_char(block: str) -> str:
    """テンプレ 1 文字を別の文字に（正規化しても一致しない差）。"""
    i = block.index("が") if "が" in block else 5
    out = block[:i] + "を" + block[i + 1:]
    assert rs.normalize_for_match(out) != rs.normalize_for_match(block)
    return out


class _LogCapture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def gate(self):
        return [r.getMessage() for r in self.records
                if r.name == "hub.reply_sanitizer"]


class _Base(unittest.TestCase):
    def setUp(self):
        self.cap = _LogCapture()
        lg = logging.getLogger("hub.reply_sanitizer")
        self._lvl = lg.level
        lg.setLevel(logging.DEBUG)
        lg.addHandler(self.cap)
        self.addCleanup(lg.removeHandler, self.cap)
        self.addCleanup(lg.setLevel, self._lvl)


# ── T7: 凍結集合は不変 ────────────────────────────────────────────────────────
class TestFrozenSetsPinned(unittest.TestCase):
    def test_T7_pins(self):
        self.assertEqual(hashlib.sha256(NL.join(JIKOU).encode("utf-8")).hexdigest(), PIN_JIKOU)
        self.assertEqual(hashlib.sha256(NL.join(HOUKI).encode("utf-8")).hexdigest(), PIN_HOUKI)
        self.assertEqual((len(JIKOU), len(HOUKI)), (2, 7))

    def test_normalize_definition(self):
        self.assertEqual(rs._NORMALIZE_DROP, frozenset("\n\r\t 　、。"))
        self.assertEqual(rs.normalize_for_match("あ、い。う\n え　お\tか\r"), "あいうえおか")
        self.assertEqual(rs.normalize_for_match("が"), "が")    # 他の文字は落とさない


# ── T1〜T6（module 単体） ───────────────────────────────────────────────────────
class TestNormalizedExemption(_Base):
    def test_T1_verbatim_unchanged_and_passes(self):
        text = JIKOU[0] + NL + FREE_150
        restored, matched = rs.restore_frozen_blocks(text, JIKOU)
        self.assertEqual(restored, text)
        self.assertEqual(matched, [])
        self.assertEqual(rs.structure_violations(text, exempt_blocks=JIKOU), [])
        out, issues, fatal = rs.sanitize_reply(text, exempt_blocks=JIKOU)
        self.assertEqual(out, text)
        self.assertNotIn("凍結テンプレを原文に復元", issues)
        self.assertEqual(self.cap.gate(), [])                  # ログなし

    def test_T2_observed_mangling_restored_and_passes(self):
        mangled = mangle_observed(JIKOU[0])
        text = mangled + NL + FREE_150
        # 従来（正規化なし）なら 300 字超で降格していた形
        self.assertGreater(len(text), 300)
        restored, matched = rs.restore_frozen_blocks(text, JIKOU)
        self.assertEqual(restored, JIKOU[0] + NL + FREE_150)  # 原文（改行込み）へ置換
        self.assertEqual(matched, [1])
        # fix1（A'）: 字数検査は復元後の文面に逐語免除のみ（再照合しない）
        self.assertEqual(rs.structure_violations(restored, exempt_blocks=JIKOU), [])
        self.assertEqual(len(rs.structure_violations(text, exempt_blocks=JIKOU)), 1)
        out, issues, _ = rs.sanitize_reply(text, exempt_blocks=JIKOU)
        self.assertEqual(out, JIKOU[0] + NL + FREE_150)
        self.assertIn("凍結テンプレを原文に復元", issues)

    def test_T3_spaces_restored(self):
        text = mangle_spaces(JIKOU[0]) + NL + FREE_150
        out, issues, _ = rs.sanitize_reply(text, exempt_blocks=JIKOU)
        self.assertEqual(out, JIKOU[0] + NL + FREE_150)
        self.assertEqual(rs.structure_violations(out, exempt_blocks=JIKOU), [])

    def test_T4_one_char_diff_not_exempt(self):
        text = mangle_one_char(JIKOU[0]) + NL + FREE_150
        restored, matched = rs.restore_frozen_blocks(text, JIKOU)
        self.assertEqual(restored, text)                       # 置換されない
        self.assertEqual(matched, [])
        v = rs.structure_violations(text, exempt_blocks=JIKOU)
        self.assertEqual(len(v), 1)
        self.assertIn("文字数超過", v[0])                      # 従来どおり字数計上
        out, issues, _ = rs.sanitize_reply(text, exempt_blocks=JIKOU)
        self.assertEqual(out, text)
        self.assertNotIn("凍結テンプレを原文に復元", issues)
        self.assertEqual(self.cap.gate(), [])

    def test_T5_two_blocks_both_restored(self):
        text = mangle_observed(JIKOU[0]) + NL + mangle_observed(JIKOU[1]) + NL + "承知しました。"
        out, issues, _ = rs.sanitize_reply(text, exempt_blocks=JIKOU)
        self.assertEqual(out, JIKOU[0] + NL + JIKOU[1] + NL + "承知しました。")
        self.assertEqual(self.cap.gate(), ["[GATE] template_normalized_match block=1",
                                           "[GATE] template_normalized_match block=2"])
        # 本番経路どおり復元後の文面を字数検査に渡す=追加ログなし・違反なし
        self.assertEqual(rs.structure_violations(out, exempt_blocks=JIKOU), [])
        self.assertEqual(len(self.cap.gate()), 2)
        # fix1（A'）: structure_violations は再照合しない（照合点は sanitize_reply の 1 か所）
        # =未復元の 2 ブロックは自由文扱いで文字数超過（質問数超過も併発し得る）
        v = rs.structure_violations(text, exempt_blocks=JIKOU)
        self.assertTrue(any("文字数超過" in x for x in v), v)
        self.assertEqual(len(self.cap.gate()), 2)

    def test_T6_log_is_fixed_vocabulary_without_text(self):
        customer = "山田太郎です。至急お願いします"
        text = mangle_observed(JIKOU[0]) + NL + customer
        rs.sanitize_reply(text, exempt_blocks=JIKOU)
        msgs = self.cap.gate()
        self.assertEqual(msgs, ["[GATE] template_normalized_match block=1"])
        for frag in ("債権者名", "アコム", "支払督促", "山田太郎", "至急"):
            self.assertNotIn(frag, msgs[0])

    def test_second_occurrence_still_counted(self):
        # 逐語 1 回目は免除・2 回目（改行落ち）は自由文扱いのまま（各 1 回の規則）
        text = JIKOU[0] + NL + mangle_observed(JIKOU[0])
        v = rs.structure_violations(text, exempt_blocks=JIKOU)
        self.assertEqual(len(v), 1)
        self.assertIn("文字数超過", v[0])

    def test_no_exempt_blocks_unchanged(self):
        # 顧客対応経路（exempt_blocks なし）は従来どおり: 置換もログもしない
        text = mangle_observed(JIKOU[0])
        out, issues, _ = rs.sanitize_reply(text)
        self.assertEqual(out, text)
        self.assertEqual(self.cap.gate(), [])
        self.assertEqual(len(rs.structure_violations(text)), 1)

    def test_houki_block_restored(self):
        mangled = mangle_observed(HOUKI[0])
        out, _, _ = rs.sanitize_reply(mangled + NL + "承知しました。", exempt_blocks=HOUKI)
        self.assertEqual(out, HOUKI[0] + NL + "承知しました。")
        self.assertEqual(rs.structure_violations(out, exempt_blocks=HOUKI), [])


# ── fix1（Codex GEF-01・裁定 A'）: 除去処理の後の照合・照合点は 1 か所 ────────────
EMOJI = "\U0001F60A"


def insert_emoji(block: str) -> str:
    """凍結ブロックの本文中に許可外の絵文字を 1 個挿入（除去処理で消える差）。"""
    i = block.index("①")
    return block[:i + 1] + EMOJI + block[i + 1:]


def insert_markdown(block: str) -> str:
    """Markdown 装飾（**強調**・行頭 ##）を挿入（除去処理で消える差）。"""
    lines = block.split(NL)
    lines[2] = "## " + lines[2]                     # 見出し記号（行頭）
    word = "債権者名"
    assert word in lines[1]
    lines[1] = lines[1].replace(word, "**" + word + "**", 1)
    out = NL.join(lines)
    assert out != block
    return out


def insert_word(block: str) -> str:
    """語を 1 つ追加（除去処理でも 7 種正規化でも消えない差）。2 行目の途中に
    「全く」を挿入する（時効・相続放棄どちらの凍結ブロックにも適用できる形）。"""
    lines = block.split(NL)
    assert len(lines[1]) > 3
    lines[1] = lines[1][:3] + "全く" + lines[1][3:]
    out = NL.join(lines)
    assert rs.normalize_for_match(out) != rs.normalize_for_match(block)
    return out


class TestRulingAPrime(_Base):
    def test_T8_emoji_in_block_is_exempt_and_restored(self):
        text = insert_emoji(mangle_observed(JIKOU[0])) + NL + FREE_150
        out, issues, _ = rs.sanitize_reply(text, exempt_blocks=JIKOU)
        self.assertEqual(out, JIKOU[0] + NL + FREE_150)      # 原文（改行込み・絵文字なし）
        self.assertIn("許可外の絵文字を除去", issues)
        self.assertIn("凍結テンプレを原文に復元", issues)
        self.assertEqual(rs.structure_violations(out, exempt_blocks=JIKOU), [])
        self.assertEqual(self.cap.gate(), ["[GATE] template_normalized_match block=1"])

    def test_T9_markdown_in_block_is_exempt_and_restored(self):
        text = insert_markdown(mangle_observed(JIKOU[0])) + NL + FREE_150
        out, issues, _ = rs.sanitize_reply(text, exempt_blocks=JIKOU)
        self.assertEqual(out, JIKOU[0] + NL + FREE_150)
        self.assertIn("markdown強調記号を除去", issues)
        self.assertIn("markdown見出し記号を除去", issues)
        self.assertIn("凍結テンプレを原文に復元", issues)
        self.assertEqual(rs.structure_violations(out, exempt_blocks=JIKOU), [])

    def test_T10_added_word_not_exempt(self):
        text = insert_word(mangle_observed(JIKOU[0])) + NL + FREE_150
        out, issues, _ = rs.sanitize_reply(text, exempt_blocks=JIKOU)
        self.assertEqual(out, text)                          # 置換されない
        self.assertNotIn("凍結テンプレを原文に復元", issues)
        v = rs.structure_violations(out, exempt_blocks=JIKOU)
        self.assertEqual(len(v), 1)
        self.assertIn("文字数超過", v[0])                     # 自由文として計上
        self.assertEqual(self.cap.gate(), [])

    def test_T11_one_char_diff_plus_emoji_not_exempt(self):
        text = insert_emoji(mangle_one_char(JIKOU[0])) + NL + FREE_150
        out, issues, _ = rs.sanitize_reply(text, exempt_blocks=JIKOU)
        self.assertNotIn(EMOJI, out)                         # 絵文字は消える
        self.assertNotIn(JIKOU[0], out)                      # だが 1 字違いが残り不一致
        self.assertNotIn("凍結テンプレを原文に復元", issues)
        self.assertEqual(len(rs.structure_violations(out, exempt_blocks=JIKOU)), 1)
        self.assertEqual(self.cap.gate(), [])

    def test_T12_pre_match_transforms_pinned_and_single_match_site(self):
        import ast
        import inspect
        self.assertEqual(rs.PRE_MATCH_TRANSFORMS, ("strip_markdown", "strip_emoji"))
        tree = ast.parse(inspect.getsource(rs))
        fns = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}

        def calls(fn):
            names, attrs = set(), set()
            body_nodes = [m for stmt in fns[fn].body for m in ast.walk(stmt)]   # 本体のみ（既定引数は除く）
            for n in body_nodes:
                if isinstance(n, ast.Call):
                    if isinstance(n.func, ast.Name):
                        names.add(n.func.id)
                    elif isinstance(n.func, ast.Attribute):
                        attrs.add(n.func.attr)
            return names, attrs
        # sanitize_reply: 照合前の変換は PRE_MATCH_TRANSFORMS の 2 つだけ・照合は
        # restore_frozen_blocks 1 回・それ以外の関数呼び出しなし
        names, attrs = calls("sanitize_reply")
        self.assertEqual(names, set(rs.PRE_MATCH_TRANSFORMS) | {"restore_frozen_blocks", "bool"})
        self.assertTrue(attrs <= {"search", "append", "extend"}, attrs)
        # 照合順序: strip_markdown → strip_emoji → restore_frozen_blocks（ソース上の出現順）
        src = inspect.getsource(rs.sanitize_reply)
        self.assertLess(src.index("strip_markdown("), src.index("strip_emoji("))
        self.assertLess(src.index("strip_emoji("), src.index("restore_frozen_blocks("))
        # structure_violations は再照合しない
        names, _ = calls("structure_violations")
        for banned in ("normalize_for_match", "restore_frozen_blocks", "_find_normalized_span"):
            self.assertNotIn(banned, names)
        # 呼び出し記録でも 1 か所: sanitize_reply→structure_violations の経路で照合は 1 回
        calls_seen = []
        real = rs.restore_frozen_blocks

        def _spy(text, blocks):
            calls_seen.append(1)
            return real(text, blocks)
        with patch.object(rs, "restore_frozen_blocks", _spy):
            out, _, _ = rs.sanitize_reply(mangle_observed(JIKOU[0]) + NL + FREE_150,
                                          exempt_blocks=JIKOU)
            rs.structure_violations(out, exempt_blocks=JIKOU)
        self.assertEqual(len(calls_seen), 1)
        # strip_markdown 単体は変換分類名を返す（語句の置換はしない）
        s, iss = rs.strip_markdown("**a** b")
        self.assertEqual((s, iss), ("a b", ["markdown強調記号を除去"]))
        self.assertEqual(rs.strip_markdown(JIKOU[0]), (JIKOU[0], []))   # 凍結原文は不変


# ── 配線: 時効ヒアリング（届く文面が原文になる） ────────────────────────────────
class TestJikouFlow(unittest.TestCase):
    def setUp(self):
        for d in (main.conversation_histories, main.kintone_record_ids,
                  main.user_business_names):
            d.pop(USER, None)
            self.addCleanup(d.pop, USER, None)
        main.hearing_completed.discard(USER)
        self.addCleanup(main.hearing_completed.discard, USER)
        self.reply = AsyncMock()
        self.queue = AsyncMock(return_value="29-1")
        self.mangled_reply = "ありがとうございます。" + NL + mangle_observed(JIKOU[0])
        patches = [
            patch.object(main.autoreply_stoplist, "is_suppressed", AsyncMock(return_value=False)),
            patch.object(main, "get_app21_record", AsyncMock(return_value=None)),
            patch.object(main, "get_recent_chat_history", AsyncMock(return_value=[])),
            patch.object(main, "ask_claude", AsyncMock(return_value=self.mangled_reply)),
            patch.object(main, "_line_reply_with_fallback", self.reply),
            patch.object(main, "save_to_chatlog", AsyncMock()),
            patch.object(main, "save_to_approval_queue", self.queue),
            patch.object(main, "ATTORNEY_LINE_USER_ID", "U_attorney"),
            patch.dict(os.environ, {"AUTOREPLY_PAUSED": "0"}),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_sent_text_is_verbatim_template(self):
        _run(main._process_line_event("tok", USER, "相談です"))
        self.queue.assert_not_awaited()                        # 降格しない
        sent = self.reply.await_args.args[2]
        self.assertEqual(sent, "ありがとうございます。" + NL + JIKOU[0])
        self.assertIn(JIKOU[0], sent)
        self.assertNotEqual(sent, self.mangled_reply)

    def test_T13_emoji_exempt_and_added_word_not_exempt_through_flow(self):
        # T8 と同結果: 絵文字入り改行落ちブロック → 原文で送信・降格なし
        main.ask_claude.return_value = "ありがとうございます。" + NL + insert_emoji(
            mangle_observed(JIKOU[0]))
        _run(main._process_line_event("tok", USER, "相談です"))
        self.queue.assert_not_awaited()
        self.assertEqual(self.reply.await_args.args[2], "ありがとうございます。" + NL + JIKOU[0])
        # T10 と同結果: 語を追加 → 免除されず承認降格（確認中定型が届く）
        self.reply.reset_mock()
        main.hearing_completed.discard(USER)
        main.ask_claude.return_value = "ありがとうございます。" + NL + insert_word(
            mangle_observed(JIKOU[0]))
        _run(main._process_line_event("tok", USER, "相談です"))
        self.queue.assert_awaited_once()
        self.assertIn("文字数超過", self.queue.await_args.kwargs["reason"])
        self.assertEqual(self.reply.await_args.args[2], main.PENDING_REPLY)


# ── 配線: 相続放棄ヒアリング ──────────────────────────────────────────────────
class TestHoukiFlow(unittest.TestCase):
    def setUp(self):
        hearing.conversation_histories.pop(USER, None)
        self.addCleanup(hearing.conversation_histories.pop, USER, None)

    def _turn(self, reply_text: str):
        hearing.conversation_histories.pop(USER, None)
        model = AsyncMock(return_value=SimpleNamespace(
            content=[SimpleNamespace(type="text", text=reply_text)]))
        send, queue = AsyncMock(), AsyncMock(return_value="q-1")
        with patch.object(hearing, "call_hearing_model", model), \
             patch.object(hearing, "reply_with_push_fallback", send), \
             patch.object(hearing, "save_to_approval_queue", queue), \
             patch.object(hearing, "save_to_chatlog", AsyncMock()), \
             patch.object(hearing, "get_recent_chat_history", AsyncMock(return_value=[])), \
             patch.object(hearing, "is_suppressed", AsyncMock(return_value=False)), \
             patch.object(hearing, "autoreply_paused", lambda: False), \
             patch.object(houki_case_store, "fetch_case", AsyncMock(return_value=None)):
            _run(hearing.handle_houki_hearing("rtok", USER, "相談です"))
        return send, queue

    def test_sent_text_is_verbatim_template(self):
        send, queue = self._turn("ありがとうございます。" + NL + mangle_observed(HOUKI[0]))
        queue.assert_not_awaited()
        sent = send.await_args.args[3]
        self.assertEqual(sent, "ありがとうございます。" + NL + HOUKI[0])

    def test_T13_emoji_exempt_and_added_word_not_exempt_through_flow(self):
        # 相続放棄の凍結ブロックは 171 字のため、自由文 150 字を添えて「免除されれば
        # 通過・免除されなければ 300 字超で降格」の分岐に到達させる
        send, queue = self._turn(
            "ありがとうございます。" + NL + insert_emoji(mangle_observed(HOUKI[0]))
            + NL + FREE_150)
        queue.assert_not_awaited()
        self.assertEqual(send.await_args.args[3],
                         "ありがとうございます。" + NL + HOUKI[0] + NL + FREE_150)
        send, queue = self._turn(
            "ありがとうございます。" + NL + insert_word(mangle_observed(HOUKI[0]))
            + NL + FREE_150)
        queue.assert_awaited_once()
        self.assertIn("文字数超過", queue.await_args.kwargs["reason"])
        self.assertEqual(send.await_args.args[3], hearing.HOUKI_PROFILE.pending_reply)


if __name__ == "__main__":
    unittest.main()
