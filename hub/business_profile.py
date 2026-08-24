"""業務プロファイル（SOUZOKU-HOUKI-H2・G1 一般化の枠）

正本 10-unit-02-souzoku-houki.md §4/§11「chat_responder のサーバー側ガード＝
仕組み共用・定義差し替え（G1 一般化）」の**構造だけ**を定義する module。

- 本 module は純構造（値を持たない）。時効プロファイル JIKOU_PROFILE の実体は
  chat_responder.py が既存 module 定数（逐語の束・hash pin 凍結）から組み立てる。
- 相続放棄プロファイルの実体は本票（H-2）では作らない（中身は H-3/H-5 票）。
- ガード関数（chat_responder）は `profile: BusinessProfile | None = None` を
  受け、None は JIKOU_PROFILE（＝従来挙動と完全一致）に解決する。

境界（何がプロファイル側か）:
  プロファイル側=業務ごとに差し替わる閉集合・文言・検知器
  （カテゴリ・prompt テンプレ・FAQ 由来の必須文言・禁止語・即時定型・PENDING
  文言・文体 route 名・status 語彙・tool schema・許可絵文字・フラグ名・
  第一報検知器）。
  機構側=業務に依存しない検査・流れ
  （サニタイズ・300 字/質問数・名乗り検知・記号残存・無根拠語・承認線引きの
  骨格・承認キュー/チャットログ/通知の配管・PENDING_CONTEXT_ENABLED 等の env
  フラグ解釈）。
  Claude 障害時応答（ClaudeUnavailableError／一般例外の両経路）は
  **pending_reply を流用**する（独立フィールドは持たない・H2-fix1 [01] 裁定。
  送信文言と App28 の assistant 保存文言は同一値）。
"""

from dataclasses import dataclass
from re import Pattern
from typing import Callable, Mapping, Optional


@dataclass(frozen=True)
class BusinessProfile:
    """1 業務種別ぶんのガード定義の束（すべて弁護士確定値への参照）。"""

    name: str                                   # 固定語彙の業務名（例 "jikou"）

    # ── ルーティング（案件 status 語彙） ─────────────────────────────────
    hearing_statuses: frozenset
    post_engagement_statuses: frozenset

    # ── prompt / モデル呼び出し ──────────────────────────────────────────
    system_prompt_template: str                 # named-placeholder 済みテンプレ
    compose_tool: Mapping                       # tool schema（カテゴリ enum 含む）
    update_flag_key: str                        # 例 "jikou_update_flag"

    # ── カテゴリと承認線引き ─────────────────────────────────────────────
    auto_send_categories: frozenset

    # ── 禁止語・許可リスト・許可絵文字 ───────────────────────────────────
    forbidden_patterns: tuple                   # tuple[(label, Pattern), ...]
    allowlisted_phrases: tuple
    allowed_emoji: frozenset

    # ── 文体パック（route 名。閉集合 ROUTE_BASIS は chat_responder 側） ──
    customer_style_route: str
    hearing_style_route: str
    style_section: str

    # ── 費用ガード ───────────────────────────────────────────────────────
    fee_category: str
    fee_required_phrases: tuple
    fee_guide_marker: str

    # ── 条件付き見立てガード（留保文言+更新事由フラグ） ──────────────────
    conditional_category: str
    reservation_general_marker: str
    reservation_individual_markers: tuple

    # ── 必須標準回答の決定的到達（時効=法テラス。vocab 空=ガード無効） ────
    mandatory_reply_vocab: tuple
    mandatory_reply_text: str
    mandatory_reply_notice_key: str
    mandatory_reply_label: str

    # ── 承認送り時の即時応答（Claude 障害時応答にも流用・H2-fix1） ────────
    pending_reply: str
    pending_by_category: Mapping
    immediate_notice_texts: Mapping
    template_dedup_markers: Mapping
    urgent_notice_kinds: Mapping

    # ── 第一報バックストップ（時効=裁判所書類。None=無効） ────────────────
    first_report_detector: Optional[Callable]
    first_report_notice_key: str


__all__ = ["BusinessProfile"]
_ = (Pattern,)   # 型注記用途の明示（forbidden_patterns の中身は re.Pattern）
