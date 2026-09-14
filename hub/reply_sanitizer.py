"""reply_sanitizer — 自動返信の送信直前サニタイズ+構成検証（AUTOREPLY-GEN2）

要件1（出力サニタイズ・サーバ側強制）:
  - markdown 記号の平文化: **強調**/`コード` の記号除去・行頭 #/##（見出し）
    除去・水平線行（---/***/___）削除・行頭箇条書き記号（- * + •）を全角
    中黒「・」へ置換。弁護士確定定型で使う「・」「①②」「━━━」等の
    和文記法は対象外（そのまま維持）
  - 絵文字: 呼び出し側が許可集合（弁護士確定定型に含まれるもの）を渡し、
    それ以外を除去
  - プレースホルダ/内部マーカー残存（<<...>>・{{...}}・[KINTONE...]）は
    サニタイズでは直さず fatal 扱い＝呼び出し側が送信せず承認降格する

要件2（長さ・構成の強制）:
  - 上限文字数（既定 300・env AUTOREPLY_MAX_CHARS で大野調整可）と
    質問数上限（2）の検査。超過は呼び出し側が自動送信せず承認降格
    （切り詰めはしない）。免除はサーバ側保持の確定定型ブロックとの逐語
    一致のみ（fix1[01]・一致部分を除いた自由文へ上限適用）

GATE-EXEMPT-FIX-1（LINE-QUALITY-DIAG の主因対処・裁定 A〜F）:
  - 免除判定は「正規化後の完全一致」。正規化=改行・空白（半角/全角・タブ）・
    読点「、」・句点「。」の除去（_NORMALIZE_DROP）。それ以外の一字の差も不一致
  - 正規化一致した区間は、顧客に届く文面を凍結テンプレの原文（改行込み・逐語）
    に置換する（restore_frozen_blocks・sanitize_reply の exempt_blocks 経由）
  - 置換・免除の対象は呼び出し側が渡す既存の凍結集合のみ（本文の追加・変更なし）
  - 字数計上は置換後の文面から凍結区間（各 1 回）を除いた自由文で行う
  - 置換したことは固定語彙 1 行 "[GATE] template_normalized_match block=N"
    （N=集合内の 1 始まり番号・本文は載せない）。類似度しきい値方式は不採用

GATE-EXEMPT-FIX-1-fix1（Codex GEF-01・裁定 A' 逐語）:
  A'. 凍結ブロックの照合は、既存サニタイザの除去処理（許可外絵文字の除去・Markdown
      装飾記号の除去。いずれも凍結済みの「顧客に届く文面から必ず除去する」処理）を
      通した後の文面に対して行う。その文面に 7 種（LF・CR・TAB・半角空白・全角空白・
      「、」・「。」）の正規化を施して凍結原文と完全一致するときのみ免除し、届く文面は
      その区間を凍結原文（改行込み）に置換する。除去処理と 7 種以外の一字の差は
      不一致。類似度方式は不採用。
  理由: 除去対象文字は顧客に届かないため、除去後に原文と一致する区間は顧客にとって
  凍結原文そのものである。
  - 照合は sanitize_reply の「除去処理の後・字数検査の前」の 1 か所のみ
    （PRE_MATCH_TRANSFORMS に固定した strip_markdown → strip_emoji の直後）。
    structure_violations は再照合せず、受け取った文面の逐語一致区間だけを免除する
"""

import logging
import os
import re
import unicodedata

from hub.redact import emit

logger = logging.getLogger("hub.reply_sanitizer")

# 内部マーカー/プレースホルダの残存（送信禁止＝承認降格）
# fix1[02]: 内容長上限（{1,60}）を撤廃し、開始/終了記号の**存在そのもの**を
# 安全側で検知する（閉じていない開始記号・61 字超・改行入りも漏らさない）
_PLACEHOLDER_RE = re.compile(r"<<|>>|\{\{|\}\}|\[/?KINTONE_")

_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_CODE_RE = re.compile(r"`([^`]*)`")
_HEADING_RE = re.compile(r"^#{1,6}\s*", re.MULTILINE)
_HR_LINE_RE = re.compile(r"^[ \t]*(?:-{3,}|\*{3,}|_{3,})[ \t]*$\n?",
                         re.MULTILINE)
_BULLET_RE = re.compile(r"^[ \t]*[-*+•][ \t]+", re.MULTILINE)
_QUESTION_RE = re.compile(r"[?？]")


_DEFAULT_MAX_CHARS = 300
MAX_QUESTIONS = 2


def max_auto_chars() -> int:
    """1通の上限文字数（既定 300・env AUTOREPLY_MAX_CHARS で調整可）。"""
    raw = os.environ.get("AUTOREPLY_MAX_CHARS", "")
    return int(raw) if raw.isdigit() and int(raw) > 0 else _DEFAULT_MAX_CHARS


def _is_emoji(ch: str) -> bool:
    """絵文字系コードポイントの判定（So/Sk と補助面の絵文字ブロック・
    異体字セレクタ・ZWJ）。和文記号（①・※・〒 等は So でも BMP の
    CJK/囲み英数字ブロック）を巻き込まないよう範囲で限定する。"""
    cp = ord(ch)
    if cp in (0xFE0F, 0x200D):          # 異体字セレクタ・ZWJ
        return True
    if 0x1F000 <= cp <= 0x1FAFF:        # 補助面の絵文字ブロック一帯
        return True
    if 0x2600 <= cp <= 0x27BF:          # Miscellaneous Symbols / Dingbats
        return True
    if cp in range(0x2B00, 0x2C00) and unicodedata.category(ch) == "So":
        return True                     # ⬛⭐ 等
    return False


def strip_emoji(text: str, allowed: frozenset[str] = frozenset()) -> str:
    return "".join(
        ch for ch in text if not _is_emoji(ch) or ch in allowed)


def strip_markdown(text: str) -> tuple[str, list[str]]:
    """Markdown 装飾記号の除去（要件1・凍結済みの除去処理）。
    (除去後の本文, 実施した変換の分類名) を返す。"""
    issues: list[str] = []
    out = text
    if _BOLD_RE.search(out) or _CODE_RE.search(out):
        out = _BOLD_RE.sub(r"\1", out)
        out = _CODE_RE.sub(r"\1", out)
        issues.append("markdown強調記号を除去")
    if _HR_LINE_RE.search(out):
        out = _HR_LINE_RE.sub("", out)
        issues.append("markdown水平線を除去")
    if _HEADING_RE.search(out):
        out = _HEADING_RE.sub("", out)
        issues.append("markdown見出し記号を除去")
    if _BULLET_RE.search(out):
        out = _BULLET_RE.sub("・", out)
        issues.append("markdown箇条書き記号を平文化")
    return out, issues


# GATE-EXEMPT-FIX-1-fix1（裁定 A'）: 凍結ブロックの照合前に走る除去処理の閉集合。
# この順で適用し、この直後に 1 回だけ照合する。語句の置換・文体変更・トリム以外の
# 削除など、これ以外の変換を照合前に入れてはならない（test_gate_exempt_fix が pin）
PRE_MATCH_TRANSFORMS: tuple[str, ...] = ("strip_markdown", "strip_emoji")


def sanitize_reply(text: str,
                   allowed_emoji: frozenset[str] = frozenset(),
                   exempt_blocks: tuple[str, ...] = ()
                   ) -> tuple[str, list[str], bool]:
    """送信直前サニタイズ。

    Returns
    -------
    (sanitized, issues, fatal)
      sanitized : 平文化済みの本文（送信に使う）
      issues    : 実施した変換/検出の分類名リスト（ログ・降格理由用）
      fatal     : True ならプレースホルダ/内部マーカー残存＝送信禁止
                  （サニタイズで直さない・承認降格）

    裁定 A'（GATE-EXEMPT-FIX-1-fix1）: 凍結ブロックの照合は PRE_MATCH_TRANSFORMS
    （strip_markdown → strip_emoji）の**後**の文面に対して 1 回だけ行う。除去対象
    文字は顧客に届かないため、除去後に凍結原文と（7 種正規化のうえで）一致する区間は
    顧客にとって凍結原文そのものである。除去処理と 7 種以外の一字の差は不一致。
    """
    issues: list[str] = []
    fatal = bool(_PLACEHOLDER_RE.search(text))
    if fatal:
        issues.append("プレースホルダ/内部マーカー残存")
    # ── PRE_MATCH_TRANSFORMS（この 2 つ以外の変換を照合前に置かない） ──
    out, md_issues = strip_markdown(text)
    issues.extend(md_issues)
    stripped = strip_emoji(out, allowed_emoji)
    if stripped != out:
        out = stripped
        issues.append("許可外の絵文字を除去")
    # ── 裁定 A'/B: 除去処理の後・字数検査の前の **唯一の照合点**。正規化一致した
    #    区間は凍結原文へ置換（exempt_blocks を渡さない経路=顧客対応 Bot は従来どおり）
    if exempt_blocks:
        restored, matched = restore_frozen_blocks(out, exempt_blocks)
        if matched:
            out = restored
            issues.append("凍結テンプレを原文に復元")
    return out, issues, fatal


# ── GATE-EXEMPT-FIX-1: 凍結テンプレの正規化一致と原文復元 ──────────────────────
# 正規化で除去する文字（裁定 A・閉集合）: 改行・復帰・タブ・半角空白・全角空白・
# 読点・句点。これ以外の差（一字でも）は不一致
# 10=LF 13=CR 9=TAB 32=半角空白 0x3000=全角空白 0x3001=読点「、」 0x3002=句点「。」
_NORMALIZE_DROP = frozenset(chr(c) for c in (10, 13, 9, 32, 0x3000, 0x3001, 0x3002))


def normalize_for_match(text: str) -> str:
    return "".join(ch for ch in text if ch not in _NORMALIZE_DROP)


def _find_normalized_span(text: str, block: str) -> tuple[int, int] | None:
    """text 内で block と正規化一致する最初の区間 (start, end) を返す（無ければ
    None）。区間は元テキスト上の「一致に使った最初の文字〜最後の文字」。"""
    nb = normalize_for_match(block)
    if not nb:
        return None
    kept = [i for i, ch in enumerate(text) if ch not in _NORMALIZE_DROP]
    nt = "".join(text[i] for i in kept)
    p = nt.find(nb)
    if p < 0:
        return None
    return kept[p], kept[p + len(nb) - 1] + 1


def restore_frozen_blocks(text: str, exempt_blocks: tuple[str, ...]
                          ) -> tuple[str, list[int]]:
    """各凍結ブロック（集合の順・各 1 回）について、正規化一致した区間を原文へ
    置換する。戻り値 (置換後テキスト, 原文と差があって置換したブロック番号
    〔1 始まり〕)。逐語一致していた区間は置換も記録もしない。"""
    out = text
    restored: list[int] = []
    for idx, block in enumerate(exempt_blocks, start=1):
        if not block:
            continue
        span = _find_normalized_span(out, block)
        if span is None:
            continue
        start, end = span
        if out[start:end] == block:
            continue
        out = out[:start] + block + out[end:]
        restored.append(idx)
        logger.info("[GATE] template_normalized_match block=%s",
                    emit(idx, "count", "log", "operator"))
    return out, restored


def structure_violations(text: str, *, max_chars: int | None = None,
                         max_questions: int = MAX_QUESTIONS,
                         exempt_blocks: tuple[str, ...] = ()) -> list[str]:
    """長さ・構成の検査（要件2）。違反分類のリストを返す（空=適合）。

    fix1[01]+fix2[01]: 「罫線（━━━━）を含めば長さ免除」を廃止。免除は
    **サーバ側が保持する確定定型ブロック（exempt_blocks）との逐語一致**のみ
    ——一致したブロックを本文から除いた**自由文部分**に通常上限（文字数・
    質問数とも）を適用する。罫線だけ混ぜた自由文は免除されない。
    exempt_blocks を渡さない経路（顧客対応 Bot）は全文に上限適用（従来
    どおり）。

    GATE-EXEMPT-FIX-1-fix1（裁定 A'）: 本関数は**再照合しない**。正規化一致→原文
    復元は sanitize_reply の 1 か所だけで行い、本関数は受け取った文面（復元済み）の
    逐語一致区間だけを免除する（免除対象を広げない）。"""
    limit = max_chars if max_chars is not None else max_auto_chars()
    remainder = text
    for block in exempt_blocks:
        if block:
            # fix2[01]: 各確定ブロックの免除は**最大 1 回**。同一ブロックの
            # 2 回目以降は自由文として検査対象に残す（=実質承認降格）。
            # 2 種類の正規ブロックを各 1 回使う組合せは許可
            remainder = remainder.replace(block, "", 1)
    violations = []
    free_len = len(remainder.strip())   # ブロック除去痕の前後空白は数えない
    if free_len > limit:
        violations.append(
            f"文字数超過（自由文{free_len}字 > 上限{limit}字）")
    questions = len(_QUESTION_RE.findall(remainder))
    if questions > max_questions:
        violations.append(f"質問数超過（{questions} > 上限{max_questions}）")
    return violations
