"""指示メッセージの応答組み立て（D2: 解析→案件検索→解釈結果の提示まで）

設計: docs/dispatch-bot/03-natural-language-parser.md
- D2 の終点は「解釈結果の提示」。復唱確認・pending・App 30 起票は D3
- 聞き返しは1メッセージ1論点・最大2往復で打ち切り（03 §7）
- 番号選択（複数候補）と聞き返しへの回答は直近セッションへの応答として扱う
- セッションはインメモリ（30分TTL）。再起動で消えても「言い直し」になるだけ（安全側）
"""

import logging
import re
import time
from dataclasses import dataclass, field

from claude_gateway import ClaudeUnavailableError
from dispatch_bot import case_search, parser, registry

logger = logging.getLogger("dispatch_bot.handler")

_SESSION_TTL_SEC = 30 * 60
_MAX_CLARIFY = 2  # 聞き返しは2往復まで（03 §7）

MSG_UNSUPPORTED = "第1弾では送付案内のみ対応しています"
MSG_QUERY_LATER = "照会機能（要対応一覧など）は第2弾で実装されます"
MSG_NO_PENDING = "確認待ちの指示はありません（復唱確認と起票はD3で実装されます）"
MSG_CANCELLED = "取り消しました。言い直してください"
MSG_GIVE_UP = "うまく確定できませんでした。お手数ですが kintone から直接起票してください"
MSG_AI_DOWN = "現在AIが応答できません。復旧後にもう一度指示してください"
MSG_UNKNOWN = "指示を解釈できませんでした。例:「鈴木さんに送付案内を作って」"


@dataclass
class Session:
    """聞き返し・候補選択の対話状態（ユーザーごとに最大1件・03 §7）"""
    base_text: str = ""                 # 元指示（聞き返し回答はここに結合して再解析）
    clarify_count: int = 0              # これまでに聞き返した回数
    candidates: list = field(default_factory=list)   # 複数候補（番号選択待ち）
    parsed: dict | None = None          # 候補選択待ち時の解析結果
    created_at: float = field(default_factory=time.monotonic)

    def expired(self) -> bool:
        return time.monotonic() - self.created_at > _SESSION_TTL_SEC


_sessions: dict[str, Session] = {}


def _get_session(user_id: str) -> Session | None:
    s = _sessions.get(user_id)
    if s and s.expired():
        del _sessions[user_id]
        return None
    return s


def reset_sessions() -> None:
    """テスト用"""
    _sessions.clear()


def _present_interpretation(parsed: dict, hit: case_search.CaseHit) -> str:
    """D2 の終点: 解釈結果の提示（復唱確認・起票は D3 で置き換わる）"""
    spec = registry.get_task(parsed["task_type"])
    lines = ["【解釈結果】（D2段階: 確認と起票はD3で実装されます）",
             f"タスク: {spec.display_name}",
             f"案件: No.{hit.record_id} {hit.customer_name}（{hit.status}・{hit.unit}）"]
    if hit.warn:
        lines.append(f"⚠ この案件は status={hit.status} です")
    return "\n".join(lines)


def _ask(session_user: str, base_text: str, question: str,
         prev: Session | None) -> str:
    """聞き返し（回数管理・2往復で打ち切り・03 §7）"""
    count = (prev.clarify_count if prev else 0) + 1
    if count > _MAX_CLARIFY:
        _sessions.pop(session_user, None)
        return MSG_GIVE_UP
    _sessions[session_user] = Session(base_text=base_text, clarify_count=count)
    return question


async def handle_message(user_id: str, text: str) -> str:
    """許可済みユーザーのメッセージ → 応答テキスト（router から呼ばれる）"""
    try:
        return await _handle(user_id, text)
    except ClaudeUnavailableError:
        return MSG_AI_DOWN


async def _handle(user_id: str, text: str) -> str:
    text = (text or "").strip()
    session = _get_session(user_id)

    # ── 番号選択（複数候補への応答） ─────────────────────────────────────
    if session and session.candidates and re.fullmatch(r"\d{1,2}", text):
        idx = int(text)
        if 1 <= idx <= len(session.candidates):
            hit = session.candidates[idx - 1]
            parsed = session.parsed
            _sessions.pop(user_id, None)
            return _present_interpretation(parsed, hit)
        return f"1〜{len(session.candidates)} の番号で選んでください"

    # ── 解析（聞き返し中なら元指示に回答を結合して再解析・03 §7） ─────────
    base_text = f"{session.base_text}\n（追加回答）{text}" if session and session.base_text \
                else text
    parsed = await parser.parse_instruction(base_text)
    intent = parsed["intent"]

    if intent == "cancel":
        _sessions.pop(user_id, None)
        return MSG_CANCELLED
    if intent == "confirm":
        # D2 に pending はない（復唱・起票は D3）
        return MSG_NO_PENDING
    if intent == "query":
        return MSG_QUERY_LATER
    if intent == "unknown":
        question = parsed["clarification"] or MSG_UNKNOWN
        return _ask(user_id, base_text, question, session)

    # ── intent == task ───────────────────────────────────────────────────
    spec = registry.get_task(parsed["task_type"])
    if spec is None:
        _sessions.pop(user_id, None)
        return MSG_UNSUPPORTED
    if parsed["confidence"] == "low":
        question = parsed["clarification"] or "指示の内容をもう少し具体的に教えてください"
        return _ask(user_id, base_text, question, session)

    # 必須項目の不足 → 聞き返し（1論点ずつ・03 §7）
    if "customer_name" in spec.required_fields and not parsed["customer_name"]:
        return _ask(user_id, base_text, "どの顧客（案件）への指示ですか？氏名を教えてください",
                    session)
    if parsed["missing_fields"]:
        items = "・".join(parsed["missing_fields"][:3])
        return _ask(user_id, base_text, f"次の項目を教えてください: {items}", session)

    # ── 案件検索（03 §4） ────────────────────────────────────────────────
    hits = await case_search.search_cases(parsed["customer_name"])
    if not hits:
        _sessions.pop(user_id, None)
        return case_search.NOT_FOUND_MESSAGE
    if len(hits) > 1:
        _sessions[user_id] = Session(base_text=base_text, candidates=hits, parsed=parsed)
        return case_search.format_choices(hits, parsed["customer_name"])

    _sessions.pop(user_id, None)
    return _present_interpretation(parsed, hits[0])
