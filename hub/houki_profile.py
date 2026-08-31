"""相続放棄の業務プロファイル+ヒアリング定義 — SOUZOKU-HOUKI-H3

- HOUKI_PROFILE: H-2 の BusinessProfile 注入機構への相続放棄側の実体。
  本票（H-3）で実値を持つのは**ヒアリング部分**のみ。顧客対応（トリアージ）
  部分は H-5 まで **fail-closed プレースホルダ**（auto_send_categories=空集合
  ＝全降格・mandatory vocab=空・first_report_detector=None）。
- HOUKI_HEARING_PROMPT: 質問項目は正本 souzoku-houki/02 §2（§1-2 有効・
  10-unit-02 §0.3）の逐語＋10-unit-02 §2.1 の 7 フェーズ構成。文体は
  chat_responder.HEARING_STYLE_SECTION_BASE（無内容見本・両業務共通の正）を
  収載し、具体値の根拠集合は空（ROUTE_BASIS["houki_hearing"]=空集合＝
  FAQ 根拠語を含む出力は承認降格）。
- houki_bot からの chat_responder 直接 import は AST checker が禁止しているため、
  必要な機構関数は本 module が**名前閉集合で re-export** する
  （style_guard_violations / save_to_chatlog / save_to_approval_queue /
  get_recent_chat_history。閉集合は test_houki_bot_policy が pin）。
- 会話ログ=App 28 共用（正本 §11「完全共用」・category で判別・別プロバイダー
  前提で userId 空間は時効側と別）。承認キュー=App 29 共用（正本 02 §5）。
  停止リスト=App 39 共用（userId 基準・チャネル不問の停止意図）。
  AUTOREPLY_PAUSED は全業務ブレーキとして共用。
"""

import os
import re

import anthropic

from chat_responder import (
    HEARING_STYLE_SECTION_BASE,
    PENDING_REPLY,
    ROUTE_BASIS,
    get_recent_chat_history,
    save_to_approval_queue,
    save_to_chatlog,
    style_guard_violations,
)
from claude_gateway import ClaudeUnavailableError, create_message_with_fallback
from hub.business_profile import BusinessProfile
from hub.houki_case_store import HEARING_CHOICE_FIELDS, HEARING_WRITABLE_FIELDS

__all__ = [
    "HOUKI_PROFILE", "HOUKI_HEARING_PROMPT", "RECORD_HEARING_TOOL",
    "HOUKI_HEARING_CATEGORY", "HEARING_TEMPLATE_BLOCKS_HOUKI",
    "call_hearing_model", "autoreply_paused", "ClaudeUnavailableError",
    "style_guard_violations", "save_to_chatlog", "save_to_approval_queue",
    "get_recent_chat_history",
]

# App 28 チャットログの判別カテゴリ（時効=「ヒアリング」と別値で自然分離）
HOUKI_HEARING_CATEGORY = "相続放棄ヒアリング"


def autoreply_paused() -> bool:
    """全業務ブレーキ（AUTOREPLY_PAUSED=1）。時効側と同じ env を共用する
    （緊急停止は業務横断で効く方が安全・H3 設計判断）。"""
    return os.environ.get("AUTOREPLY_PAUSED") == "1"


# ── record_hearing ツール（souzoku-houki/02 §1 の方式・フェーズは正本 §2.1 の
#    7 段構成へ拡張。fields のキーは App 40 の実フィールドコード） ────────────────
RECORD_HEARING_TOOL = {
    "name": "record_hearing",
    "description": (
        "ヒアリングで確認できた項目を相続放棄案件レコードへ逐次記録する。"
        "会話で新しい事実が確認できたターンでは必ず呼ぶこと。"
        "fields のキーは指定のフィールドコードのみ・値が会話で確認できた"
        "ものだけを入れる（推測で埋めない）。"
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "phase": {
                "type": "string",
                "enum": ["1_deceased", "2_dates", "3_debts", "4_assets",
                         "5_others", "6_koseki", "7_applicant"],
                "description": "現在のヒアリングフェーズ",
            },
            "fields": {
                "type": "object",
                "description": (
                    "確認済み項目の部分更新。キーは次のフィールドコードのみ: "
                    + "、".join(sorted(HEARING_WRITABLE_FIELDS))
                    + "。日付（〜_申告）は YYYY-MM-DD 形式が確定した場合のみ"
                      "入れ、曖昧（『2026-05頃』等）は 日付申告メモ に原文の"
                      "まま記録する。"
                    # HEARING-FIX1: 選択式フィールドの許容値を明示（App 40
                    # DROP_DOWN の閉集合。選択肢外はサーバ側で保存されず
                    # 聞き直しになる）。単一の正= HEARING_CHOICE_FIELDS
                    + "次のフィールドは必ず記載の選択肢の値そのままで入れる: "
                    + "、".join(f"{code}（{'/'.join(values)}）"
                                for code, values
                                in sorted(HEARING_CHOICE_FIELDS.items()))
                    + "。会話の表現（『母です』等）は選択肢へ読み替えて記録"
                      "する（例: 亡くなったのが母なら申述人は 子）"
                ),
            },
            "creditor_names": {
                "type": "array",
                "items": {"type": "string"},
                "description": "会話で判明した債権者名（あれば）",
            },
            "phase_done": {
                "type": "boolean",
                "description": "現在フェーズの必須項目が揃ったか",
            },
            "hearing_done": {
                "type": "boolean",
                "description": "全フェーズの必須項目が揃ったか",
            },
        },
        "required": ["phase", "fields", "phase_done", "hearing_done"],
    },
}

# ── ヒアリング用システムプロンプト ──────────────────────────────────────────────
# 質問項目の文言は正本 souzoku-houki/02 §2 の表（有効部分）の逐語。
# フェーズ 6（戸籍収集の要否）・ふりがな/職業（申述書様式の必須欄）は
# 10-unit-02 §2.1 に従い追加。
HOUKI_HEARING_PROMPT = """\
あなたは大野法律事務所（相続放棄専門窓口）のLINEヒアリング担当アシスタントです。
相続放棄のご相談について、以下のフェーズ順に必要事項を伺い、確認できた項目を
record_hearing ツールで案件レコードへ逐次記録します。

【ヒアリングの質問項目（フェーズ順）】
1. 被相続人: 亡くなった方の氏名・依頼者との続柄／最後の住所（市区町村まででも可）・本籍（分かれば）。氏名はふりがなも伺う
2. 日付（最重要）: 死亡日（分からなければおおよそ）／死亡を知った日・自分が相続人だと知った日（別々に質問）／知った経緯（役所からの通知・債権者からの請求・親族からの連絡 等）
3. 債務: 借金・督促の有無、督促状・訴状が届いているか／判明している債権者名
4. 財産: 遺産（預貯金・不動産・車等）の有無。あわせて必ず次の質問をこの文言のまま行う:「亡くなった方の財産（預貯金・不動産・車など）について、使ったり、処分したり、解約したり、そこから何かのお支払いをされたものはありますか。」
5. 他の相続人: 依頼者は配偶者・子・親・兄弟姉妹のどれか／先順位者（子・親）の有無と、その人達が放棄したか／同順位の相続人（兄弟等）の人数・一緒に放棄したい人がいるか
6. 戸籍収集の要否: 手元にある戸籍・住民票の有無（自分で取った/これから）。「事務所で職務上請求により取得可能」の案内まで
7. 依頼者本人: 氏名（ふりがなも）・住所・生年月日・電話・メール・職業、依頼者本人が相続人か（親族代理の相談か）／未成年・成年後見の関与有無

【記録の規則】
- fields のキーは案件レコードのフィールドコードのみ。値は会話で確認できたものだけを入れる（推測・補完はしない）
- 日付の聞き方: 「正確に」を求めない。曖昧なら「YYYY-MM頃」のような答えを許容し、日付申告メモ に原文のまま記録する（〜_申告 の日付欄には YYYY-MM-DD が確定した場合のみ入れる）。日付の確定は弁護士が行う
- 続柄が「その他」のときは 続柄その他 に具体的内容を記録する
- 財産は判明した範囲で 財産_不動産・財産_現金預貯金・財産_有価証券・財産_負債 の各欄に短く記録する

【会話の規則】
- 質問は一度に1つ、答えやすい形で聞く。同じことを二度聞かない（記録済み・会話済みの項目は再質問しない）
- 1メッセージは短く（300字以内）。敬体（です・ます調）
- 金額・期間・法的な見立て・手続きの可否には**答えない**（「弁護士が確認のうえご案内します」に留める）。熟慮期間の残日数・間に合うかどうかにも言及しない
- 財産の使用・処分・解約への許可や黙認と読める表現は使わない
- 記録にない事実・日付・金額の創作は禁止

""" + HEARING_STYLE_SECTION_BASE + """
※ 相続放棄ヒアリング経路の補足: 本プロンプトに FAQ・確定定型の根拠はないため、金額・期間などの具体値は返信に入れない（入れればサーバ側で承認降格）。文体規範は自由文の部分にのみ適用する。顧客名が未回答の間は宛名を省略する。
"""

# ヒアリング prompt 内の定型ブロック（罫線区切り）。現構成では 0 件＝長さ免除
# なし（全文に 300 字上限）。将来テンプレを収載したら時効と同じ抽出規約で拾う
HEARING_TEMPLATE_BLOCKS_HOUKI: tuple = tuple(
    re.findall("━{5,}\n.*?\n━{5,}", HOUKI_HEARING_PROMPT, re.DOTALL))

# ── 文体 route（fix3 の経路別根拠集合）: houki_hearing=空集合 ─────────────────
# （FAQ 根拠語〔5年程度・住宅ローン・1ヶ月程度〕を含む出力は承認降格）。
# route の登録は機構側レジストリ ROUTE_BASIS（chat_responder）に直接定義
# （import 順に依存しない決定的な閉集合・test_autoreply_style1 が pin）
assert "houki_hearing" in ROUTE_BASIS and not ROUTE_BASIS["houki_hearing"]

# ── HOUKI_PROFILE（H-2 注入機構の相続放棄実体・H-3 はヒアリング部分のみ実値） ──
HOUKI_PROFILE = BusinessProfile(
    name="souzoku-houki",
    # status 語彙（H0-APP-2 の 15 値を 3 分類。顧客対応 routing は H-5 で使用）
    hearing_statuses=frozenset({"", "問い合わせ"}),
    post_engagement_statuses=frozenset({
        "受任", "書類収集中", "申述書作成", "裁判所提出済", "照会書対応",
        "受理", "債権者通知", "完了"}),
    # ── ここから顧客対応（H-5）まで fail-closed プレースホルダ ──
    system_prompt_template=HOUKI_HEARING_PROMPT,   # 顧客対応 prompt は H-5
    compose_tool={"name": "compose_reply",
                  "input_schema": {"type": "object"}},   # H-5 で定義
    update_flag_key="tanjun_shonin_flag",   # 正本 §4.3（実配線は H-5）
    auto_send_categories=frozenset(),       # 空集合=顧客対応は全降格（H-5 まで）
    forbidden_patterns=(),
    allowlisted_phrases=(),
    allowed_emoji=frozenset(),
    customer_style_route="houki_customer",  # ROUTE_BASIS 未登録=fail-closed
    hearing_style_route="houki_hearing",
    style_section=HEARING_STYLE_SECTION_BASE,
    fee_category="費用の定型案内",
    fee_required_phrases=(),                # 費用固定文は H-5（正本 §5.2）
    fee_guide_marker="【費用のご案内】",
    conditional_category="相続放棄見立て_条件付き",   # 正本 §4.1（H-5）
    reservation_general_marker="内容が正確であれば",
    reservation_individual_markers=("内容が正確であれば", "審理を経て確定"),
    mandatory_reply_vocab=(),               # 空=必須標準回答ガード無効（H-5）
    mandatory_reply_text="",
    mandatory_reply_notice_key="none",
    mandatory_reply_label="",
    # 障害時・承認送り時の確認中応答: 時効と同一の弁護士確定文言を再利用
    # （業務固有情報を含まない中立文・H2-fix1 の pending_reply 流用裁定と同型）
    pending_reply=PENDING_REPLY,
    pending_by_category={},
    immediate_notice_texts={},
    template_dedup_markers={},
    urgent_notice_kinds={},
    first_report_detector=None,             # 第一報バックストップは H-5
    first_report_notice_key="none",
)


async def call_hearing_model(system_prompt: str,
                             messages: list[dict]) -> object:
    """相続放棄ヒアリングの Claude 呼び出し（claude_gateway 経由=フォール
    バック・残高警報を完全共用〔正本 §11〕。プロンプトキャッシュ有効）。"""
    client = anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
    return await create_message_with_fallback(
        client,
        context="相続放棄ヒアリング record_hearing",
        max_tokens=1024,
        system=[{"type": "text", "text": system_prompt,
                 "cache_control": {"type": "ephemeral"}}],
        tools=[RECORD_HEARING_TOOL],
        tool_choice={"type": "auto"},
        messages=messages,
    )
