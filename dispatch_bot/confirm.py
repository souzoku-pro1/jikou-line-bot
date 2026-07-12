"""復唱確認と pending_command_id（D3）

設計: docs/dispatch-bot/06-confirmation-and-safety.md §2・§3

- LINE上のOKは「解釈確認＋起票承認」のみ（確定判断2）。対外実行の承認ではない
- pending はユーザーごと最大1件・UUID・30分期限・単回消込・割込み無効化
- **インメモリ保持（第1弾の明示仕様）**: Railway再起動で pending は消え、
  その後のOKは「確認待ちなし」→再指示になる（安全側）。永続化は第2弾（D6）
- 復唱の情報密度はリスク比例（低=簡潔版2行／中・高=フルテンプレ・一律フル禁止）
"""

import logging
import time
import uuid
from dataclasses import dataclass, field

from dispatch_bot.case_search import CaseHit
from dispatch_bot.registry import TaskSpec
from hub.redact import emit


logger = logging.getLogger("dispatch_bot.confirm")

PENDING_TTL_SEC = 30 * 60  # 30分（06 §3.1）


@dataclass
class Pending:
    command_id: str
    user_id: str
    parsed: dict
    case: CaseHit
    instruction_text: str          # 指示原文（聞き返し回答の結合済み）
    created_at: float = field(default_factory=time.monotonic)
    executed: bool = False
    record_id: str = ""            # 起票後に記録（二重OK時のリンク再掲用）
    record_url: str = ""

    def expired(self) -> bool:
        return time.monotonic() - self.created_at > PENDING_TTL_SEC


# ユーザーごと最大1件（06 §3.1）
_pending: dict[str, Pending] = {}


def create(user_id: str, parsed: dict, case: CaseHit, instruction_text: str) -> Pending:
    """復唱送信時に発行（既存の pending は上書き=無効化）"""
    p = Pending(command_id=str(uuid.uuid4()), user_id=user_id,
                parsed=parsed, case=case, instruction_text=instruction_text)
    _pending[user_id] = p
    logger.info("[DISPATCHBOT] pending created id=%s user=%s...",
                emit(p.command_id[:8], "record_id", "log", "operator"),
                emit(user_id[:10], "record_id", "log", "operator"))
    return p


def peek(user_id: str) -> tuple[str, Pending | None]:
    """現在の pending 状態: ("none"|"expired"|"executed"|"active", Pending|None)"""
    p = _pending.get(user_id)
    if p is None:
        return "none", None
    if p.expired():
        del _pending[user_id]
        return "expired", None
    if p.executed:
        return "executed", p
    return "active", p


def has_active(user_id: str) -> bool:
    return peek(user_id)[0] == "active"


def invalidate(user_id: str) -> bool:
    """キャンセル・割込みによる無効化。有効な pending があったら True"""
    state, _ = peek(user_id)
    _pending.pop(user_id, None)
    return state == "active"


def mark_executed(p: Pending, record_id: str, record_url: str) -> None:
    """単回消込（06 §3.3 の第1層）。実行済みとして保持し、二重OKにはリンク再掲"""
    p.executed = True
    p.record_id = record_id
    p.record_url = record_url


def reset() -> None:
    """テスト用"""
    _pending.clear()


# ── 復唱テンプレート（06 §2・リスク比例） ───────────────────────────────────

def confirmation_message(spec: TaskSpec, case: CaseHit, parsed: dict) -> str:
    warn = f"⚠ この案件は status={case.status} です\n" if case.warn else ""
    # 同封物（表示名）を復唱に含める（2026-07-04: 例「送付案内（委任契約書）」）
    labels = parsed.get("task_params", {}).get("enclosure_labels") or []
    enc = f"（{'・'.join(labels)}）" if labels else ""
    if spec.risk == "低":
        # 簡潔版（2行程度・06 §2.1）
        noun = spec.display_name.removesuffix("の作成")
        return (f"{warn}{case.customer_name}さん（No.{case.record_id}・{case.status}）に"
                f"{noun}{enc}を起票します。\nOK / キャンセル（30分有効）")
    # 中・高リスク: フルテンプレ（06 §2.2。D4: 職務上請求で使用）
    enc_line = f"同封物: {'・'.join(labels)}\n" if labels else ""
    # タスク固有の明細行（D4: 対象者・種別と通数・宛先自治体・小為替概算等はレジストリの
    # summary_fn から。文言をチャネル知識ごと confirm に持ち込まない）
    detail = ""
    if spec.summary_fn:
        detail = "\n".join(spec.summary_fn(parsed)) + "\n"
    return (f"【確認】以下で起票します\n{warn}"
            f"案件: No.{case.record_id} {case.customer_name}（{case.unit}・{case.status}）\n"
            f"タスク: {spec.display_name}（{'App 30 起票' if spec.destination == 'app30' else '実行キュー起票'}）\n"
            f"{enc_line}{detail}"
            f"実行範囲: {spec.auto_scope}\n"
            f"対外送信: なし（対外実行の承認は従来どおり kintone で行います）\n"
            f"リスク区分: {spec.risk}\n"
            f"有効期限: 30分（このOKは起票の承認です。対外承認ではありません）\n"
            f"OK / キャンセル")
