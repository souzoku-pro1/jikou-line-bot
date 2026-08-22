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
    （切り詰めはしない）。ヒアリング定型テンプレブロック（━━━━ 罫線）を
    含む返信は定型そのものが上限超のため長さ検査を免除する
"""

import os
import re
import unicodedata

# 内部マーカー/プレースホルダの残存（送信禁止＝承認降格）
_PLACEHOLDER_RE = re.compile(
    r"<<[^<>]{1,60}>>|\{\{[^{}]{1,60}\}\}|\[/?KINTONE_[A-Z_]+\]")

_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_CODE_RE = re.compile(r"`([^`]*)`")
_HEADING_RE = re.compile(r"^#{1,6}\s*", re.MULTILINE)
_HR_LINE_RE = re.compile(r"^[ \t]*(?:-{3,}|\*{3,}|_{3,})[ \t]*$\n?",
                         re.MULTILINE)
_BULLET_RE = re.compile(r"^[ \t]*[-*+•][ \t]+", re.MULTILINE)
_QUESTION_RE = re.compile(r"[?？]")

# ヒアリング定型テンプレブロックの罫線（含む返信は長さ検査を免除）
TEMPLATE_BLOCK_MARKER = "━━━━"

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


def sanitize_reply(text: str,
                   allowed_emoji: frozenset[str] = frozenset()
                   ) -> tuple[str, list[str], bool]:
    """送信直前サニタイズ。

    Returns
    -------
    (sanitized, issues, fatal)
      sanitized : 平文化済みの本文（送信に使う）
      issues    : 実施した変換/検出の分類名リスト（ログ・降格理由用）
      fatal     : True ならプレースホルダ/内部マーカー残存＝送信禁止
                  （サニタイズで直さない・承認降格）
    """
    issues: list[str] = []
    fatal = bool(_PLACEHOLDER_RE.search(text))
    if fatal:
        issues.append("プレースホルダ/内部マーカー残存")
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
    stripped = strip_emoji(out, allowed_emoji)
    if stripped != out:
        out = stripped
        issues.append("許可外の絵文字を除去")
    return out, issues, fatal


def structure_violations(text: str, *, max_chars: int | None = None,
                         max_questions: int = MAX_QUESTIONS) -> list[str]:
    """長さ・構成の検査（要件2）。違反分類のリストを返す（空=適合）。
    ヒアリング定型テンプレブロックを含む文は長さ検査を免除する。"""
    limit = max_chars if max_chars is not None else max_auto_chars()
    violations = []
    if TEMPLATE_BLOCK_MARKER not in text and len(text) > limit:
        violations.append(f"文字数超過（{len(text)}字 > 上限{limit}字）")
    questions = len(_QUESTION_RE.findall(text))
    if questions > max_questions:
        violations.append(f"質問数超過（{questions} > 上限{max_questions}）")
    return violations
