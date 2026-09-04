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
from hub.houki_case_store import (
    DEFLECT_REPLY,
    HEARING_CHOICE_FIELDS,
    HEARING_ROUNDS,
    HEARING_WRITABLE_FIELDS,
)

# HOUKI-HEARING-UX-1（弁護士決定 B）: 質問しない・記録しない欄（App 40 の欄と
# 選択肢は残置。tool の説明にも載せない）
NOT_ASKED_FIELDS: frozenset = frozenset({"未成年後見関与"})

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
                # HOUKI-HEARING-UX-1: 7 通構成（HEARING_ROUNDS の順）
                "enum": ["1_deceased", "2_dates", "3_debts_assets",
                         "4_other_heirs", "5_koseki", "6_applicant",
                         "7_principal"],
                "description": "現在のヒアリングの通（第1通〜第7通）",
            },
            "fields": {
                "type": "object",
                "description": (
                    "確認済み項目の部分更新。キーは次のフィールドコードのみ: "
                    + "、".join(sorted(HEARING_WRITABLE_FIELDS - NOT_ASKED_FIELDS))
                    + "。日付（〜_申告）は YYYY-MM-DD 形式が確定した場合のみ"
                      "入れ、曖昧（『2026-05頃』等）は 日付申告メモ に原文の"
                      "まま記録する。"
                    # HEARING-FIX1: 選択式フィールドの許容値を明示（App 40
                    # DROP_DOWN の閉集合。選択肢外はサーバ側で保存されず
                    # 聞き直しになる）。単一の正= HEARING_CHOICE_FIELDS
                    + "次のフィールドは必ず記載の選択肢の値そのままで入れる: "
                    + "、".join(f"{code}（{'/'.join(values)}）"
                                for code, values
                                in sorted(HEARING_CHOICE_FIELDS.items())
                                if code not in NOT_ASKED_FIELDS)
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

# ── ヒアリング用システムプロンプト（HOUKI-HEARING-UX-1・弁護士決定の 7 通構成） ──
# 各通の質問項目は houki_case_store.HEARING_ROUNDS（単一の正・「誰の・何を」を
# 明示）から定型ブロック（罫線区切り・送信ゲートの長さ/質問数免除の対象）を
# 組み立てる。旧正本（souzoku-houki/02 §2・10-unit-02 §2.1）の 1 問ずつ方式は
# 本票の弁護士決定 A で置き換え、未成年・後見の質問は決定 B で撤去。
_RULE = "━━━━━━━━━━━━━━━"
_CIRCLED = "①②③④⑤⑥⑦⑧⑨"


def _round_block(intro: str, items: tuple) -> str:
    lines = [_RULE, intro]
    lines += [f"{_CIRCLED[i]}{label}" for i, (label, _f) in enumerate(items)]
    lines += ["分かる範囲でお答えください。", _RULE]
    return "\n".join(lines)


def _rounds_text() -> str:
    parts = []
    for i, (title, intro, items, note) in enumerate(HEARING_ROUNDS, start=1):
        parts.append(f"第{i}通（{title}）\n{_round_block(intro, items)}\n{note}")
    return "\n\n".join(parts)


HOUKI_HEARING_PROMPT = """\
あなたは大野法律事務所（相続放棄専門窓口）のLINEヒアリング担当アシスタントです。
相続放棄のご相談について、以下の7通の構成で必要事項を伺い、確認できた項目を
record_hearing ツールで案件レコードへ逐次記録します。

【ヒアリングの進め方（7通構成・弁護士決定）】
- 第1通から第7通の順に進める。1通で1テーマ。各通は下記の罫線（━━━）で囲んだ定型ブロックを一字一句そのまま送る（番号・改行・記号を変えない）。ブロックの前に、相手の言葉を引き取る一文や「ありがとうございます」を短く添えてよい
- 各質問は「誰の・何を」を明示している。省略した言い換え（「お名前を教えてください」だけ、等）はしない
- お客様が一部の項目だけ答えたときは、答えてもらえた項目だけを record_hearing で記録し、抜けた項目だけを短く聞き直す（項目名を並べ、疑問符は1つにまとめる。定型ブロックは再送しない）
- 収集済み項目（既知）がある通は、その項目を除いた残りだけを聞く。全項目が既知の通は飛ばす
- その通の項目が全て記録できたら、次の通のブロックを送る

""" + _rounds_text() + """

【ご質問への対応】
- 手続きの要否・法的な見立て・費用・期間・間に合うかどうか等のご質問を受けたときは、内容には答えず、次の文をそのまま返し（変えない）、そのうえで現在の通の未回答項目を改めて示す:
「""" + DEFLECT_REPLY + """」
- 締めの言葉（「よろしくお願い致します。」等）だけの返信は禁止。返信には必ず、上記の受け流し文か、次に答えていただきたい項目のどちらかを含める

【聞かないこと（弁護士決定）】
- ご依頼者ご自身のお子様（年齢・手続きの要否など）や、本人に代わって法律行為を行う代理人・保護者に関することは、ご依頼者についても相続人についても質問しない。お客様から話題に出ても記録せず、上記の受け流し文で返す

【記録の規則】
- fields のキーは案件レコードのフィールドコードのみ。値は会話で確認できたものだけを入れる（推測・補完はしない）
- 日付の聞き方: 「正確に」を求めない。曖昧なら「YYYY-MM頃」のような答えを許容し、日付申告メモ に原文のまま記録する（〜_申告 の日付欄には YYYY-MM-DD が確定した場合のみ入れる）。日付の確定は弁護士が行う
- 続柄が「その他」のときは 続柄その他 に具体的内容を記録する
- 財産は判明した範囲で 財産_不動産・財産_現金預貯金・財産_有価証券・財産_負債 の各欄に短く記録する

【会話の規則】
- 同じことを二度聞かない（記録済み・会話済みの項目は再質問しない）
- 定型ブロック以外の自由文は短く（300字以内）。敬体（です・ます調）
- 金額・期間・法的な見立て・手続きの可否には**答えない**（上記の受け流し文に留める）。熟慮期間の残日数・間に合うかどうかにも言及しない
- 財産の使用・処分・解約への許可や黙認と読める表現は使わない
- 記録にない事実・日付・金額の創作は禁止

""" + HEARING_STYLE_SECTION_BASE + """
※ 相続放棄ヒアリング経路の補足: 本プロンプトに FAQ・確定定型の根拠はないため、金額・期間などの具体値は返信に入れない（入れればサーバ側で承認降格）。文体規範は自由文の部分にのみ適用する（罫線で囲んだ定型ブロックは原文のまま）。顧客名が未回答の間は宛名を省略する。
"""

# ヒアリング prompt 内の定型ブロック（罫線区切り・7 通）。送信ゲートは各ブロックとの
# 逐語一致部分を長さ/質問数の上限から免除する（時効と同じ抽出規約・各 1 回）
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
