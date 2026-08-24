"""
顧客対応 Claude モジュール

ヒアリング完了後（status が「受付中」「問い合わせ」以外）の顧客メッセージに対し、
Claude API (tool use) で返信案を作成し、自動送信または承認キューへの保存を行う。

外部から使うインターフェース:
  - get_app21_record(user_id)          : App21 を LINEユーザーID で検索
  - classify_routing(status)            : ルーティング分類 ("hearing"|"pre"|"post")
  - build_system_prompt(...)            : システムプロンプト組み立て
  - apply_server_guards(...)            : 自動送信前のサーバー側二重チェック
  - handle_customer_message(...)        : 顧客対応Claudeのメインエントリ
  - get_approval_record(record_id)      : 承認キューレコード取得
  - mark_approval_sent(record_id)       : 承認キュー送信済みフラグ更新
  - send_line_push(to, text)            : LINE Push送信
  - save_to_chatlog(...)                : チャットログ保存
"""

import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Callable, Optional

import anthropic
import httpx

from claude_gateway import (
    ClaudeUnavailableError,
    create_message_with_fallback,
)
from config import HEARING_STATUSES, POST_ENGAGEMENT_STATUSES
from hub import line_channel
from hub import reply_sanitizer
from hub.business_profile import BusinessProfile
from hub.redact import emit

logger = logging.getLogger("chat_responder")

# ── 環境変数 ──────────────────────────────────────────────────────────────────
_SUBDOMAIN     = os.environ.get("KINTONE_SUBDOMAIN", "")
_APP21_ID      = os.environ.get("KINTONE_APP_ID", "")
_APP21_TOKEN   = os.environ.get("KINTONE_API_TOKEN", "")
APP_CHATLOG    = os.environ.get("APP_CHATLOG", "")
TOKEN_CHATLOG  = os.environ.get("TOKEN_CHATLOG", "")
APP_APPROVAL   = os.environ.get("APP_APPROVAL", "")
TOKEN_APPROVAL = os.environ.get("TOKEN_APPROVAL", "")
_LINE_TOKEN    = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ATTORNEY_LINE_USER_ID = os.environ.get("ATTORNEY_LINE_USER_ID", "")

# ── ステータス分類 ─────────────────────────────────────────────────────────────
# HEARING_STATUSES / POST_ENGAGEMENT_STATUSES は config.py で一元管理
# 受任前（決済完了・不受任など）→ 顧客対応Claude（受任前モード）
# PRE_ENGAGEMENT: 上記以外の値すべて（安全側フォールバック含む）

# ── カテゴリ定義 ───────────────────────────────────────────────────────────────
AUTO_SEND_CATEGORIES = {
    "挨拶・雑談",
    "手続きの一般的な流れ",
    "必要書類の案内",
    "費用の定型案内",
    "進捗の事実回答",
    "営業案内・アクセス",
    "時効見立て_条件付き",
}

# ── 定型文 ────────────────────────────────────────────────────────────────────
PENDING_REPLY = (
    "ありがとうございます。内容を確認の上、改めてご連絡いたします。\n"
    "少々お時間をいただく場合がございますが、何卒よろしくお願いいたします。"
)

# 「裁判所から書類が来た」系の第一報への資料収集文面。
# カテゴリは承認必須のまま、この定型文のみ即時送信する（弁護士確認済み文言）。
COURT_DOC_REQUEST_REPLY = (
    "裁判所からの書類は放置すると不利益が大きい場合があります。\n"
    "お手元の書類の全ページを写真に撮って、このLINEに送っていただけますか。\n"
    "優先して確認いたします。"
)

# 諦め・離脱兆候への中立引き止め文。実質回答は承認制に回し、これのみ即時送信する。
CHURN_NEUTRAL_REPLY = (
    "ご事情により最適な解決方法は異なります。"
    "よろしければ状況をもう少しお聞かせください。"
)

# 対象外債権（税金・個人からの借入れ）への定型文。実質回答は承認制に回す。
OUT_OF_SCOPE_DEBT_REPLY = (
    "税金や個人の方からの借入れについては、内容により対応が異なりますので、"
    "別途個別にご案内いたします。確認の上、改めてご連絡いたします。"
)

# 希死念慮の表明への専用文面（FAQ第3弾・2026-07-03 弁護士確定。
# 実質回答は承認制のまま、この文面のみ即時送信し【緊急・要即時対応】で通知する）
CRISIS_SUPPORT_REPLY = (
    "お辛い状況の中、正直にお話しくださりありがとうございます。"
    "借金の問題には解決の道があります。"
    "弁護士が優先してご連絡しますので、少しだけお待ちください。"
)

# 差押え等が目前と訴えるパニックへの専用文面（FAQ第3弾・2026-07-03 弁護士確定）
URGENT_SEIZURE_PANIC_REPLY = (
    "ご不安な状況、承知いたしました。"
    "至急、弁護士が内容を確認してご連絡します。"
    "お手元に届いている書類があれば、全ページの写真をこのLINEにお送りください。"
)

# 承認キュー行き時に PENDING_REPLY の代わりに即時送信できる定型文
# （hoterasu は定義順の都合で HOTERASU_STANDARD_REPLY 定義直後に登録——
# AUTOREPLY-GEN2 要件6: 法テラス標準回答のサーバー側決定的到達）
IMMEDIATE_NOTICE_TEXTS = {
    "court_doc_request": COURT_DOC_REQUEST_REPLY,
    "churn_neutral": CHURN_NEUTRAL_REPLY,
    "out_of_scope_debt": OUT_OF_SCOPE_DEBT_REPLY,
    "crisis_support": CRISIS_SUPPORT_REPLY,
    "urgent_seizure_panic": URGENT_SEIZURE_PANIC_REPLY,
}

# 弁護士通知を【緊急・要即時対応】フォーマットにする定型文キー
URGENT_NOTICE_KINDS = {
    "crisis_support": "希死念慮の表明",
    "urgent_seizure_panic": "差押え切迫の訴え",
}

# 同じ定型文を二度送らないための照合マーカー（会話履歴の assistant 発言と照合）。
# 危機対応の2種（crisis_support / urgent_seizure_panic）は意図的に登録しない:
# 繰り返しの訴えにも汎用の「確認中」文ではなく専用文面を返し続けるため。
_TEMPLATE_DEDUP_MARKERS = {
    "court_doc_request": "全ページを写真に撮って",
    "churn_neutral": "最適な解決方法は異なります",
    "out_of_scope_debt": "別途個別にご案内",
}

# ── FAQ第3弾の確定文言（2026-07-03 弁護士確定。言い回しの改変禁止） ─────────────
# 定数化して禁止語混入をテストで固定する（test_server_guards.py）
JIKOU_YEARS_TEXT = (
    "消費者金融やクレジットカード、債権回収会社からの借金は、"
    "基本的に最後の返済から5年で時効援用が可能になります。"
    "ただし、信用金庫や公的機関からの借入れ、判決を取られている場合などは"
    "10年となることがあります。"
)
QUESTIONNAIRE_RETURN_TEXT = (
    "返済しますという趣旨の回答をした場合、時効が更新（リセット）されている"
    "可能性があります。ただしその場合でも時効援用通知を送付の上、交渉はいたします。"
)
HOME_VISIT_TEXT = (
    "返済します・分割で支払います等の発言は時効が更新される可能性があります。"
    "ただし口頭でのやり取りのため、時効が更新しないことも多々あります。"
)
SEIZURE_SCOPE_TEXT = (
    "預貯金も対象になります。ただし給料は職場を知られている場合、"
    "預貯金は銀行名に加えて支店名まで特定されていなければ、"
    "差押えは簡単ではないことが多いです。"
)
MAIL_ADDRESS_TEXT = (
    "無料のメールアドレス（Gmailなど）の取得をお願いしています。"
    "スマートフォンで5分ほどで作成でき、作成方法もご案内します。"
)

# 禁止語テストの対象（新FAQの確定文言＋即時定型文すべて）
FAQ3_CANONICAL_TEXTS = [
    JIKOU_YEARS_TEXT,
    QUESTIONNAIRE_RETURN_TEXT,
    HOME_VISIT_TEXT,
    SEIZURE_SCOPE_TEXT,
    MAIL_ADDRESS_TEXT,
    CRISIS_SUPPORT_REPLY,
    URGENT_SEIZURE_PANIC_REPLY,
]

# ── 費用の定型案内（固定文） ────────────────────────────────────────────────────
# 金額表記は「44,000円（税込）」で統一（2026-07-03 弁護士確定）
FEE_GUIDE_TEXT = (
    "【費用のご案内】\n"
    "・費用: 1社あたり44,000円（税込）。複数社の場合は 44,000円（税込）× 社数\n"
    "・お支払い: 前払いのみ（分割払いは承っておりません）\n"
    "・お支払い方法: 銀行振込またはカード決済（Stripe・デビットカード可）\n"
    "・万一時効が完成していなかった場合も、時効援用通知の送付と業者への確認までの"
    "業務に対する費用は発生いたします。その場合、確認をもって業務は終了となります。"
)

# 会話履歴に固定文を送付済みかを判定するマーカー（費用ガードの会話単位化に使用）
FEE_GUIDE_MARKER = "【費用のご案内】"

# カテゴリ「費用の定型案内」の自動送信文に必須の文言（欠けたら承認制に降格。
# ただし直近の会話履歴で固定文を送付済みなら、続き質問への簡潔な回答を許容する）
FEE_REQUIRED_PHRASES = ["44,000円", "税込", "前払い", "分割払い", "費用は発生"]

# 法テラスの標準回答（弁護士確認済み文言・2026-07-03。数値・条件の改変禁止）
HOTERASU_STANDARD_REPLY = (
    "申し訳ございません。当事務所では法テラス（民事法律扶助）の"
    "ご利用には対応しておりません。"
    "費用は1社あたり44,000円（税込）の前払いとなります。"
)
# AUTOREPLY-GEN2 要件6: 法テラス質問に本標準回答をサーバー側で決定的に
# 到達させる（従来はプロンプト内 FAQ 指示のみ＝モデル依存で不達だった）。
# 承認降格時の即時定型として登録し、二度送り防止のマーカーも持たせる
IMMEDIATE_NOTICE_TEXTS["hoterasu"] = HOTERASU_STANDARD_REPLY
_TEMPLATE_DEDUP_MARKERS["hoterasu"] = "法テラス（民事法律扶助）"

# fix1[04]: 法テラス検知の語彙閉集合（本裁定で確定・返信文言は既存の
# 弁護士確定文のまま不変）。NFKC 正規化+空白除去のうえ部分一致で判定し、
# 検知後は安全側で標準回答へ倒す
_HOTERASU_VOCAB = ("法テラス", "ほうてらす", "民事法律扶助", "法律扶助")


def _mentions_hoterasu(text: str,
                       profile: BusinessProfile | None = None) -> bool:
    """必須標準回答の検知語彙（NFKC+空白除去・部分一致）。H2: 語彙は
    プロファイル側（時効=法テラス閉集合）・正規化照合は機構側。vocab 空の
    プロファイルでは常に False（ガード無効）。"""
    p = profile or JIKOU_PROFILE
    t = unicodedata.normalize("NFKC", text or "")
    t = re.sub(r"\s+", "", t)   # 空白除去（全角空白は NFKC で半角化済み）
    return any(v in t for v in p.mandatory_reply_vocab)

# ── 画像メッセージへの固定受領応答（AUTOREPLY-GEN2 要件4・票由来文言） ─────────
# AI に画像内容の判断はさせない（画像読解は別票）。受領応答+弁護士通知のみ。
# 本文言は merge 前に大野確定を経る
IMAGE_RECEIPT_REPLY = (
    "書類のお写真を受領いたしました。弁護士が確認のうえご連絡いたします。"
)
# App28 に記録する受信側マーカー（画像バイナリは保存しない）
IMAGE_INBOUND_MARKER = "（画像メッセージを受信）"

# ── PENDING_REPLY の文脈化（AUTOREPLY-GEN2 要件5・文言案） ──────────────────────
# カテゴリ名ベースの閉集合文言。**大野の文言確定まで無効**（既定=現行
# PENDING_REPLY）。確定後に env PENDING_CONTEXT_ENABLED=1 で有効化する。
# 所要時間の目安を入れる場合は大野裁定の文言へ差し替える（時間の約束は
# 現案では入れていない）
PENDING_BY_CATEGORY = {
    "費用の定型案内": ("費用についてのご質問ありがとうございます。"
                       "担当弁護士に確認のうえ、改めてご連絡いたします。"),
    "費用交渉・減額相談": ("お支払い方法についてのご相談ありがとうございます。"
                           "担当弁護士が内容を確認のうえ、改めてご連絡いたします。"),
    "法的判断・見通し": ("ご質問ありがとうございます。個別のご事情に関わる"
                         "内容のため、担当弁護士が確認のうえ、改めてご連絡"
                         "いたします。"),
    "手続きの一般的な流れ": ("お手続きについてのご質問ありがとうございます。"
                             "確認のうえ、改めてご連絡いたします。"),
    "必要書類の案内": ("書類についてのご質問ありがとうございます。"
                       "確認のうえ、改めてご連絡いたします。"),
    "進捗の事実回答": ("進捗についてのお問い合わせありがとうございます。"
                       "現在の状況を確認のうえ、改めてご連絡いたします。"),
    "営業案内・アクセス": ("お問い合わせありがとうございます。"
                           "ご案内内容を確認のうえ、改めてご連絡いたします。"),
    "クレーム・不満": ("ご指摘ありがとうございます。責任者が内容を確認の"
                       "うえ、改めてご連絡いたします。"),
    "解約・辞任関係": ("ご連絡ありがとうございます。担当弁護士が内容を"
                       "確認のうえ、改めてご連絡いたします。"),
    "本人確認不能・第三者": ("恐れ入りますが、ご本人さま確認が必要な内容の"
                             "ため、確認のうえ改めてご連絡いたします。"),
    "その他判断系": ("ご質問ありがとうございます。担当者が内容を確認の"
                     "うえ、改めてご連絡いたします。"),
}


def pending_reply_for(category: str,
                      profile: BusinessProfile | None = None) -> str:
    """承認送り時の即時応答（要件5）。大野の文言確定（env
    PENDING_CONTEXT_ENABLED=1）までは現行 PENDING_REPLY を返す。
    H2: 文言はプロファイル側・env フラグの解釈は機構側。"""
    p = profile or JIKOU_PROFILE
    if os.environ.get("PENDING_CONTEXT_ENABLED") == "1":
        return p.pending_by_category.get(category, p.pending_reply)
    return p.pending_reply


# ── 許可絵文字（弁護士確定定型に含まれるもののみ・要件1） ───────────────────────
def _collect_allowed_emoji() -> frozenset:
    from hub.reply_sanitizer import _is_emoji
    sources = FAQ3_CANONICAL_TEXTS + [
        FEE_GUIDE_TEXT, HOTERASU_STANDARD_REPLY, PENDING_REPLY,
        IMAGE_RECEIPT_REPLY, COURT_DOC_REQUEST_REPLY,
    ] + list(PENDING_BY_CATEGORY.values())
    return frozenset(ch for t in sources for ch in t if _is_emoji(ch))


ALLOWED_CANONICAL_EMOJI = _collect_allowed_emoji()


def build_known_items(app21_record: Optional[dict],
                      history: list[dict]) -> dict:
    """AUTOREPLY-GEN2 要件3: 収集済み項目台帳。

    App21 の正（既存ヒアリングフローが機械抽出済み）+会話履歴の画像受領
    マーカーから決定的に構成する。fail-open=取れない項目は載せない
    （未知扱い・抽出失敗で会話を止めない）。"""
    items: dict[str, str] = {}
    try:
        if app21_record:
            for label, code in (("債権者名", "問い合わせ業者名"),
                                ("借入時期", "借入時期_テキスト"),
                                ("最終返済日", "最終返済日_テキスト"),
                                ("裁判所書類の有無", "裁判所書類"),
                                ("信用情報で知ったか", "信用情報確認")):
                v = str((app21_record.get(code) or {}).get("value")
                        or "").strip()
                if v:
                    items[label] = v
        if any(IMAGE_RECEIPT_REPLY in (m.get("content") or "")
               for m in history if m.get("role") == "assistant"):
            items["書類写真"] = "受領済み"
    except Exception:
        return items
    return items

# ── 時効見立て_条件付きの留保文言 ─────────────────────────────────────────────
# 一般論（A型）のただし書き / 個別見立て（B型）の条件+確定留保。
# いずれも無ければ承認制に降格する。
RESERVATION_GENERAL_MARKER = "債務の承認"
RESERVATION_INDIVIDUAL_MARKERS = ("内容が正確であれば", "をもって確定")

# ── 禁止語チェック（サーバー側ガード） ─────────────────────────────────────────
# 受任後顧客への電話対応の定型指示（弁護士確認済み文言。禁止語チェックの許可リスト対象）
APPROVED_PHONE_INSTRUCTION = (
    "手続き中はお電話に出ないでください。"
    "出てしまった場合は「弁護士に依頼しています」とだけ伝えて切ってください。"
)

# 受任後顧客への督促状対応の定型指示（弁護士確認済み文言・2026-07-03 追加）。
# 裁判所書類の但し書きまでの全文のみを許可リストに載せる。但し書きを省略した
# 部分利用は「無視して」が禁止語照合に残るため、自動的に承認制へ降格される。
APPROVED_DUNNING_INSTRUCTION = (
    "手続き中、業者からの督促状は無視していただいて問題ありません。"
    "ただし裁判所から書類が届いた場合は必ずすぐお知らせください。"
)

# 受任前顧客の「督促を無視していいか」系への標準応答型（判断分岐提示型・
# 弁護士確認済みの型。2026-07-03 追加）。行動指示語を含まない文面であること
# （test_server_guards.py で固定）。
BRANCHING_GUIDANCE_EXAMPLE = (
    "督促への対応は、時効が成立している可能性が高いかどうかで変わります。"
    "可能性が高い場合は、お支払いや業者への連絡をせずに時効援用を進めるのが一般的です。"
    "成立していない場合は、和解や分割払いの交渉という選択肢もあります。"
    "どちらに当たるか確認したいので、最後に返済されたのはいつ頃か教えていただけますか。"
)

# ── 大野文体（AUTOREPLY-STYLE-1・両経路共通の文体規範+few-shot 見本） ─────────────
# 真似るのは**文体のみ**。金額・期間・法的見立ての中身は従来どおり弁護士確定
# 定型と承認制に従う（見本中の数値・固有名詞を Bot が引用しない旨を prompt に
# 明記）。見本は大野の実返信を匿名化した弁護士確定の文体見本。
# 見本はサニタイズ・300 字・質問数・禁止語の第 2 世代ガードに適合する
# （test_autoreply_style1.py で pin）。凍結文言（法テラス・PENDING・画像受領
# 文言・定型ブロック等）には触れない（同テストで hash pin）。
STYLE_RULES_TEXT = """\
【文体規範（大野文体・弁護士確定）】
返信文の自由文部分は次の文体で書く。これは文体の規範であり、金額・期間・法的見立ての中身は本プロンプトの弁護士確定定型・FAQ・承認制ルールに従う（文体規範が内容のルールに優先することはない）。
- 結論を最初の1〜2文で言い切り、補足は後に置く（言い切るのは構成の話。断定語の禁止と留保文言の必須は従来どおり）
- 金額・期間・次の行動を具体的に示す。「確認します」で終わる文を避ける（示す金額・期間は弁護士確定定型・FAQ・顧客情報にあるものに限る。記録にない数値の創作は禁止）
- 不利な情報も先回りで正直に開示する姿勢で書く（開示できるのは定型・FAQの範囲。承認必須のカテゴリは従来どおり承認制）
- 記号・箇条書き・罫線・絵文字なしのプレーン文で書く（弁護士確定定型・定型ブロックは原文のまま使い、この規範の対象外）
- 敬語水準は「〜となります」「〜いただけますと」「よろしくお願い致します」
- 相手の言葉を引き取ってから答える（「○○様のおっしゃるように」）
- 宛名で始め「よろしくお願い致します」で結ぶ（顧客名が判明している場合のみ「○○様」で始める。不明・未登録の場合は宛名を省略する。宛名と結びを含めて上限文字数内に収める）\
"""

# 見本（票由来・匿名化済み）。ラベルはプロンプト表示用。
# fix1 [A]（R-AUTOREPLY-STYLE-1 STYLE-01 HIGH）: 見本側の無害化——
#   見本1: 名乗り文「弁護士の大野と申します。」を除去（文体見本に名乗りを置かない）
#   見本2〜4: 案件固有の数値・期間・法的内容（5年程度/1ヶ月程度/住宅ローン/
#   延滞の文字/信用情報機関へのご連絡要求/完全に抹消 等）を、文体（結論先出し・
#   引き取り・正直な開示・敬語水準・「もっとも/そのため」の運び）が保たれる
#   範囲で内容を持たない言い回しへ置換。匿名化記号は ○○=顧客名・△△=相手方/
#   対象 の 2 種に統一（旧「◯社」は廃止）。
# 旧見本の逐語は test_autoreply_style1 に凍結コピーとして保持し、モデル出力が
# 旧見本を引用しても自動送信されないこと（[B] 防壁）を negative で固定する。
# fix2: 見本1〜3 を大野確定の最終文言へ差し替え（逐語・改変禁止）。見本2・3 の
# 具体値（5年程度・住宅ローン）は FAQ 確定文言（信用情報の削除まで 5 年程度／
# 完了後 5 年程度はローンを組めない前提）に根拠がある語＝fix1 防壁の
# 「FAQ 根拠あり・許容」分類と整合する。見本4 は fix1 のまま
STYLE_EXEMPLARS: tuple[tuple[str, str], ...] = (
    ("見立てと提案",
     "○○様 お問合せありがとうございます。"
     "信用情報等ご確認いたしました。△△については○○様のおっしゃられるように"
     "譲渡済となっておりますね。そうすると、当事務所から原債権者に譲渡先を"
     "確認し、その譲渡先に対して時効援用通知をご送付する流れとなりそうです。"
     "当事務所でもお手伝いさせていただくことはできますので、"
     "ご検討のほどよろしくお願いいたします。"),
    ("不利益の正直な開示",
     "信用情報については、お手続きにより解消される部分はあるものの、"
     "時効援用通知の送付から5年程度は住宅ローンを組むことができない"
     "場合もございますので、あらかじめご了承いただきたく存じます。"),
    ("質問への具体的回答",
     "ご質問の点につきましては、当事務所で対応しております。"
     "もっとも、その結果がいつの時点で反映されるかは債権者にも分かりません。"
     "そのため、送付から5年程度は反映されないものとしてご認識いただけると"
     "間違いございません。"),
    ("進捗報告",
     "お世話になっております。本日△△に対して、時効援用通知を送付致します。"
     "今後は結果の確認までお時間をいただき、手続きを進めさせていただきます。"
     "少々お時間をいただきますが、引き続きよろしくお願い致します。"),
)

STYLE_EXEMPLARS_NOTE = (
    "【文体見本（弁護士確定・文体の参照のみ）】\n"
    "以下は文体の見本である。文の運び・語尾・敬語水準・結論先行の構成を真似る。"
    "見本の○○（顧客名）・△△（相手方・対象）は匿名化の空欄であり、返信に"
    "○○・△△の記号を残さない（残せばサーバ側で承認降格。顧客名が不明なら"
    "宛名を省略する）。見本の中の事案の内容・見立ては、本プロンプトの弁護士"
    "確定定型・FAQ・顧客情報に根拠がない限り引用しない（根拠なき引用の禁止）。"
    "回答に金額・期間・見立てを入れるときは、必ず本プロンプトの弁護士確定定型・"
    "FAQ・顧客情報を出典とする。弁護士本人として名乗らない（「大野と申します」"
    "「弁護士の大野」等の名乗りはサーバ側で承認降格。対応者についてはFAQの"
    "回答のとおり）。"
)

# fix3 [A]（R-AUTOREPLY-STYLE-1-2 STYLE-02 HIGH・経路ごとの根拠集合の一致）:
# ヒアリング prompt（_HEARING_PROMPT_FROZEN）には FAQ が無く、顧客対応の見本
# 1〜3 が持つ具体値（5年程度・住宅ローン）の根拠が経路内に存在しない。そのため
# ヒアリング経路は**無内容見本**（文体のみ: 結論先出し・引き取り・正直な開示・
# 敬語水準・宛名/結び）で構成する。見本4 は法的内容を含まないため両経路共通。
# 見本 B・C は fix1 [A] で設計した無内容版（fix2 で顧客対応側のみ大野確定の
# 内容付きへ差し替えられたもの）を再利用。
HEARING_STYLE_EXEMPLARS: tuple[tuple[str, str], ...] = (
    ("引き取りと流れの案内",
     "○○様 お問合せありがとうございます。ご回答内容を確認いたしました。"
     "△△については○○様のおっしゃられるように、まずその点から確認する流れと"
     "なりますね。そうすると、残りの項目を順にお伺いし、確認の結果をご案内する"
     "こととなりそうです。当事務所でもお手伝いさせていただくことはできますので、"
     "ご検討のほどよろしくお願いいたします。"),
    ("不利益の正直な開示",
     "△△については、お手続きにより解消される部分はあるものの、"
     "すぐには解消されない場合もございますので、"
     "あらかじめご了承いただきたく存じます。"),
    ("質問への具体的回答",
     "ご質問の点につきましては、当事務所で対応しております。"
     "もっとも、その結果がいつの時点で反映されるかは相手方にも分かりません。"
     "そのため、当面は反映されないものとしてご認識いただけると"
     "間違いございません。"),
    STYLE_EXEMPLARS[3],     # 進捗報告（両経路共通・法的内容なし）
)

HEARING_STYLE_EXEMPLARS_NOTE = (
    "※ ヒアリング経路の補足: 本プロンプトには FAQ・確定定型の根拠がないため、"
    "金額・期間などの具体値（○年程度・○ヶ月程度・ローン等）は返信に入れない"
    "（入れればサーバ側で承認降格）。"
)


def _exemplars_text(exemplars: tuple[tuple[str, str], ...]) -> str:
    return STYLE_EXEMPLARS_NOTE + "\n" + "\n".join(
        f"見本{i}（{label}）:\n{text}"
        for i, (label, text) in enumerate(exemplars, start=1))


STYLE_EXEMPLARS_TEXT = _exemplars_text(STYLE_EXEMPLARS)
HEARING_STYLE_EXEMPLARS_TEXT = (
    _exemplars_text(HEARING_STYLE_EXEMPLARS) + "\n" + HEARING_STYLE_EXEMPLARS_NOTE)

# 規範（STYLE_RULES_TEXT）と見本注記は両経路で同一の正。見本集合のみ経路別:
#   顧客対応（_SYSTEM_PROMPT_BASE）= STYLE_SECTION（FAQ 根拠つき見本 1〜4）
#   ヒアリング（main.SYSTEM_PROMPT）= HEARING_STYLE_SECTION_BASE（無内容見本）
STYLE_SECTION = STYLE_RULES_TEXT + "\n\n" + STYLE_EXEMPLARS_TEXT
HEARING_STYLE_SECTION_BASE = (
    STYLE_RULES_TEXT + "\n\n" + HEARING_STYLE_EXEMPLARS_TEXT)

# 禁止語照合の前に返信文から除去する許可済みフレーズ
# AUTOREPLY-GEN2 要件4: ヒアリング初回テンプレの写真案内（SYSTEM_PROMPT 内の
# 固定文言）のみ許可。それ以外の自由文での写真依頼は禁止語
# （AI は画像を見られないため、見えないものを求める文面を出させない）
HEARING_PHOTO_GUIDE_PHRASE = "写真を送っていただくと\nより正確に確認できます"
# 写真依頼の許可定型（弁護士確定の資料収集文言のみ。これ以外の自由な
# 言い回しでの写真依頼は「写真依頼」禁止語で承認制に降格される）
APPROVED_PHOTO_REQUEST_PHRASES = [
    "全ページの写真をこのLINEにお送りください",         # 差押え切迫の定型内
    "差押えに関する書類の写真をこのLINEにお送りいただけますか",  # FAQ 指示の定型
]
ALLOWLISTED_PHRASES = [
    APPROVED_PHONE_INSTRUCTION,
    APPROVED_DUNNING_INSTRUCTION,
    "お電話に出ないでください",
    HEARING_PHOTO_GUIDE_PHRASE,
    *APPROVED_PHOTO_REQUEST_PHRASES,
]

# 禁止語（検出したら自動送信を承認制に降格）
# 断定語は、直後に否定（〜とは限らない/保証できない/言い切れない 等）が続く
# 場合を除外する。法律知識ブロックの必須文言「必ず消滅するとは保証できない」や、
# 断定要求への留保付き応答「絶対に大丈夫とは言えません」等が誤検出されるため
# （2026-07-03 実測・弁護士承認済みの緩和）。
_NEGATION_TAIL = r"(?![^。]{0,10}(?:とは(?:限|保証|言|い|断|申)|わけでは))"
_FORBIDDEN_PATTERNS: list[tuple[str, re.Pattern]] = [
    (
        "断定語",
        re.compile(
            rf"(?:確実に|絶対に|間違いなく){_NEGATION_TAIL}"
            rf"|必ず(?:消滅|成立|時効){_NEGATION_TAIL}"
            r"|100[%％]|１００[%％]"
        ),
    ),
    # 行動指示語も打ち消しの形（〜してはいけません/〜してよいわけではありません 等）は
    # 除外する（断定語の否定形除外と同じ原則。「無視してよいわけではありません」が
    # 2026-07-03 実測で誤検出されたため）
    (
        "行動指示語",
        re.compile(
            r"払わないで|連絡しないで|出ないで"
            r"|無視して(?!はいけ|(?:よい|いい)わけで)"
            r"|放置して(?!はいけ|(?:よい|いい)わけで)"
        ),
    ),
    # 「時効間近」は全応答で使用禁止（減額通知等からの安易な示唆を防ぐ。
    # 顧客が使った場合も復唱しない。2026-07-03 FAQ第2弾で弁護士指示）
    ("禁止表現", re.compile(r"時効間近")),
    # AUTOREPLY-GEN2 要件4: AI は画像を見られないため、自由文での写真依頼は
    # 禁止（ヒアリング初回テンプレの固定文言のみ ALLOWLISTED_PHRASES で許可。
    # 画像の受領応答・弁護士確認は _process_line_image_event が担う）
    ("写真依頼",
     re.compile(r"(?:お?写真|画像)を(?:お送り|送っ|撮っ|添付し)")),
]

# ── 裁判所書類の第一報検知（サーバー側バックストップ） ─────────────────────────
_COURT_DOC_HINT = re.compile(r"裁判所|訴状|支払督促|差押|差し押さえ|強制執行|公示送達")
_RECEIPT_HINT = re.compile(r"届|来た|来て|来まし|きた|きまし|受け取|入ってい")
_NEGATION_HINT = re.compile(
    r"届いて(?:い|お)?(?:ない|ません)"
    r"|来て(?:い|お)?(?:ない|ません)"
    r"|(?:届いた|来た)(?:こと|記憶|覚え)?(?:は|も|が)?(?:ない|ありません)"
    r"|何も(?:届|来)(?:いて|て)?(?:い)?(?:ない|ません)"
)

# ── システムプロンプト ─────────────────────────────────────────────────────────
_SYSTEM_PROMPT_BASE = """\
あなたは大野法律事務所（時効援用専門窓口）のLINE応対担当アシスタントです。
顧客からのLINEメッセージへの返信文を作成し、カテゴリと自動送信可否を判定します。
返信文はそのまま自動送信されるか、弁護士（先生）の承認を経て送信されます。

【顧客情報（kintone登録済み・ヒアリング回答）】
- 顧客名: {customer_name}
- 案件ステータス: {status}（{phase}フェーズ）
- 対象業者: {business_name}
- 借入時期: {borrow_period}
- 最終返済日: {last_payment}
- 裁判所からの書類（ヒアリング時の回答）: {court_docs}
- 信用情報で債務を知ったか: {credit_check}

【会話スタイル（最重要）】
- 自然な会話を最優先。テンプレートの棒読み感を出さない
- 同じことを二度言わない。会話履歴や上記の顧客情報で既に確認済みの質問・確認事項は絶対に繰り返さない（特に訴訟・支払督促の有無の確認）
- 1メッセージは短く。長文説明が必要な場合も要点を絞り、詳細は聞かれたら答える
- 顧客への質問は一度に1つ、答えやすい形で聞く（例:「最後に返済されたのはいつ頃でしょうか。だいたいで構いません」）
- 必須文言（費用の注意書き・留保文言等）は該当場面でのみ入れ、無関係な場面に挿入しない
- 敬体（です・ます調）、1メッセージ400字以内を目安とする
- 用語:「時効の更新」を使用（「時効の延長」は禁止）
- 記録にない進捗・日付・金額の創作は禁止
- 書類の写真をお願いするときは定型文言（「お手元の書類の全ページを写真に撮って、このLINEに送っていただけますか。」または「差押えに関する書類の写真をこのLINEにお送りいただけますか」）をそのまま使う。自由な言い回しでの写真依頼はサーバー側で承認制に降格される。収集済み項目に「書類写真: 受領済み」がある場合は再依頼しない
- 断定語（確実に/絶対に/間違いなく/必ず 等）・行動指示語（払わないで/無視して/連絡しないで/放置して 等）は使用禁止。例外は2つのみ: (1)【FAQ】記載の受任後顧客への定型指示（電話対応・督促状対応）をそのまま使う場合 (2)「絶対に大丈夫とは言えません」のような否定の形

<<STYLE_SECTION>>

【カテゴリ選択肢】
自動送信可（auto_send=true にできる）:
  挨拶・雑談 / 手続きの一般的な流れ / 必要書類の案内 / 費用の定型案内 / 進捗の事実回答 / 営業案内・アクセス / 時効見立て_条件付き
承認必須（必ず auto_send=false）:
  法的判断・見通し / 費用交渉・減額相談 / クレーム・不満 / 解約・辞任関係 / 緊急対応 / 本人確認不能・第三者 / その他判断系

【時効見立て_条件付き（自動送信可）】
次のA・Bの場合のみこのカテゴリを使う。
A) 法律の一般論（仮定形の質問）: 正確に断言してよい。
   例:「5年経過・10年内に裁判なし・債務承認なしなら消滅しますか」
   →「その前提がすべて満たされていれば、時効援用により支払義務は消滅します。」と回答し、必ず次のただし書きを添える:
   「なお、債務の承認は本人が気づかず該当していることがあります（電話で支払うと言った、少額を入金した等）。」
B) 個別の見立て: 顧客の申告事実に基づく条件付き表現のみ。
   「お伺いした内容が正確であれば、時効援用できる可能性が高いです。」の形で伝え、必ず次の留保を添える:
   「最終的に時効が成立しているかは、当事務所から業者へ時効援用通知を送り、その後の業者への確認をもって確定します。」
   あわせて、ご依頼への導線を自然な一文で添える（押し付けない）。
   顧客の申告事実が揃っていて上記の条件付き表現+留保で答えられる場合は auto_send=true とする（jikou_update_flag が false である限り、念のための承認回しはしない）。
※ 見立てに必要な事実（最終返済時期など）が足りない場合は、見立てを述べずに質問を1つだけ返す。その場合のカテゴリは「手続きの一般的な流れ」で auto_send=true でよい。

【時効に関する回答で必ず auto_send=false にするケース】
- 時効不成立の方向の見立て（顧客が諦める方向の回答すべて）→ カテゴリ「法的判断・見通し」
- jikou_update_flag が true の場合の、時効に関する回答すべて

【断定・保証を求められた場合（「絶対大丈夫？」「失敗しない？」等）】
断定はしない。「可能性は高いですが、絶対とは言えません」の趣旨の留保付き応答であれば自動送信可
（カテゴリ「時効見立て_条件付き」）。短い留保だけで終えず、必ずB型の留保文言まで含めること。標準の型:
「可能性は高いですが、絶対とは言えません。お伺いした内容が正確であれば、時効援用できる可能性が高いです。最終的に時効が成立しているかは、当事務所から業者へ時効援用通知を送り、その後の業者への確認をもって確定します。」
留保なしで安心させる回答や、断定そのものはしない。
なお、個別の見立てを述べず「失敗する場合の一般的な事例」の説明や事実確認の質問で答える場合は、カテゴリ「手続きの一般的な流れ」でよい（その場合は留保文言不要）。カテゴリ「時効見立て_条件付き」を使うのは見立てを述べるときだけ（必ず留保文言まで含める）。

【jikou_update_flag の判定】
会話履歴・今回のメッセージ・上記顧客情報のいずれかに、次の事実が現れていれば true にする:
- 裁判所書類（訴状・支払督促・判決など）を受け取った
- 差押えを受けた（またはその通知が来た）
- 一部でも弁済した（少額の入金・引き落としを含む）
- 支払う意思を伝えた（業者に「払います」と言った等）
※「届いていない」「支払っていない」などの否定の回答は該当しない（false のまま）。

【法律知識: 裁判所書類】
裁判所書類の話題への応答は以下を正確に踏まえること:
- 訴訟が過去10年以内に確定している場合 → その確定から10年経過するまで時効援用はできない
- 支払督促が過去10年以内に確定している場合 → 時効援用の手続き自体は可能だが、業者により見解が分かれるため、必ず消滅するとは保証できない
- 本人に書類到達の記憶がなくても、公示送達により判決・支払督促が確定している場合がある
この一般論の説明までは自動送信可（カテゴリ「手続きの一般的な流れ」）。そこから先の個別の質問（「私の場合はどうなりますか」「私はもうできないということか」等）が続いたら、回答内容が一般論の言い換えであっても承認制（カテゴリ「法的判断・見通し」、auto_send=false）に回す。

【immediate_notice（承認必須のとき顧客へ即時返す定型文の選択）】
auto_send=false の場合のみ、次から選ぶ。該当なしは "none"。
- court_doc_request:「裁判所から書類が来た」系の第一報のとき（過去の会話で既に書類の写真送付を依頼済みなら選ばない）
- churn_neutral:「じゃあいいです」「もういいです」等の諦め・離脱の兆候のとき
- out_of_scope_debt: 税金・個人からの借入れが対象の相談のとき
- crisis_support: 希死念慮の表明（「自殺も考えている」「死にたい」等）のとき。カテゴリ「緊急対応」・auto_send=false とし、必ずこれを選ぶ
- urgent_seizure_panic: 差押え等が目前と訴えるパニック（「明日差し押さえられるかも」等。書類が届いたかは不明）のとき。カテゴリ「緊急対応」・auto_send=false。※書類が届いた第一報は court_doc_request を優先
auto_send=true のときは常に "none"。
※ 紹介割引の問い合わせは「費用交渉・減額相談」（auto_send=false）で immediate_notice="none"（通常の定型文でよい）。

【費用の定型案内】
費用に関する質問には、次の固定文を一字一句変えずに返信に含めること（前後に会話の流れに合う自然な一文を添えてよい。省略・改変・要約は不可）:
<<FEE_GUIDE>>
例外: この会話で固定文を既に送付済みの場合、続き質問（「3社だといくらか」等）には固定文を繰り返さず簡潔に答えてよい（金額は必ず 44,000円（税込）× 社数 と整合させる）。
費用の値引き・分割回数の相談への応答は「費用交渉・減額相談」（承認必須）。

【FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可）】
- 期間: 受任から完了まで2〜4週間程度。
- 来所: 不要。本人確認書類の写真をLINEで送付いただくだけで完結する。
- 完了報告: LINEまたはメールで連絡する。書面の交付はない（業者から書面が届くことも基本的にない）。
- 信用情報（ブラックリスト）: 時効援用後、削除まで長いと5年程度かかることがある。3ヶ月以内に消える場合もあるが、5年程度見ておくのが安全。削除を早めることは基本的にできない（信用情報機関へ早急に報告するよう業者には伝える）。
- 督促の停止: ご依頼後、基本的に10日以内に止まる。ただし時効が完成していなかった場合は業務終了となるため、その後は再度通知が来る。
- 家族への秘匿: 事務所からの郵送物でご家族に知られることはない。契約は電子契約、決済は振込または電子決済、完了報告もLINE等のため。
- 対象債権: 奨学金・NHK受信料・携帯料金は対象。税金・個人からの借入れは個別案内（受任できない場合もある）→ この2つが出たらカテゴリ「その他判断系」で auto_send=false にし、immediate_notice="out_of_scope_debt" を選ぶ。
- 古い借金（10年・20年前）: 歓迎する方向で回答してよい。ただし過去10年以内に訴訟・支払督促を起こされた記憶がないかを確認する。会話履歴または上記顧客情報で既に確認済みなら絶対に再度聞かない。記憶がなければ受任可能の方向で案内する。
- 業者からの電話:
  * 受任後の顧客 → 次の定型指示をそのまま伝える（弁護士の標準指示のため自動送信可）:
    <<PHONE_INSTRUCTION>>
  * 受任前の顧客 → 一般論「支払う意思と受け取られる発言をすると、時効が更新される場合があります」+「ご依頼いただいた後に、具体的な対応方法をご案内します」に留める（受任前の顧客に具体的な行動指示はしない）
- 業者からの督促状・請求通知（「無視していいか」「放置していいか」等）:
  * 受任後の顧客 → 次の定型指示をそのまま伝える（弁護士の標準指示のため自動送信可。裁判所書類の但し書きを省略・改変しない。省略するとサーバー側で承認制に降格される）:
    <<DUNNING_INSTRUCTION>>
  * 受任前の顧客 → 判断分岐提示型で返す（自動送信可・カテゴリ「手続きの一般的な流れ」）。行動指示はせず、選択肢の分岐を一般論として示し、確認質問を1つだけ添える。標準の型:
    <<BRANCHING_EXAMPLE>>
    会話履歴や顧客情報で最終返済時期が既に確認済みなら、質問を繰り返さず、その事実に基づく条件付き見立て（「時効見立て_条件付き」・留保文言必須）に接続する。
  * 「支払い」の可否に踏み込む質問（「払わなくていいか」「支払うべきか」「もう払わなくていいということか」等）は、督促の話題に見えても督促状の定型指示では答えず、受任後は承認制（カテゴリ「法的判断・見通し」、auto_send=false）。承認済み定型で答えてよいのは「督促状を無視していいか」「電話にどう対応するか」の形の質問のみ
- 家族からの相談（親の借金・配偶者の分）:「ご本人の身分証明書をご提出いただければ手続き可能です」の一般案内までは自動送信可（カテゴリ「手続きの一般的な流れ」）。それ以上の個別対応（案件状況の回答等）は「本人確認不能・第三者」で auto_send=false。
- 法テラス（民事法律扶助）の利用可否: 次の標準回答をそのまま使う（自動送信可・カテゴリ「手続きの一般的な流れ」）:
  「<<HOTERASU_REPLY>>」
- 営業案内・アクセス（以下は登録済みの正確な情報。このまま案内してよい・自動送信可）:
  * やり取りはすべてこのLINEで完結し、メッセージは24時間いつでも送信できる（内容確認の上、順次返信する）
  * 対応時間: 弁護士の対応は10時〜22時頃、LINEでの相談受付は24時間
  * 所在地: 埼玉県川口市西青木2-1-45 レクイアーレ101号（※来所は不要。最寄り駅からの道順など登録にない情報は創作せず、地図アプリでの検索を案内してよい）
  * 電話番号: 048-299-2704。電話番号を聞かれた場合は、番号を案内した上で「LINEでそのままご相談いただくのが最もスムーズです」の趣旨の一文を必ず添える（LINE完結への導線を維持）

【FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可）】
手続き・書類:
- 本人確認書類: 運転免許証・マイナンバーカード・健康保険証・住民票が利用可。その他の顔写真付き証明書は承認制で個別確認（auto_send=false）
- 免許証もマイナンバーカードも無い場合: 手続き可。ただし何らかの本人確認書類は必要
- 督促状が手元に無い: ご依頼可能。業者名がわかれば問題ない
- 業者名がわからない: 当事務所で調べることはできない
- 住所が督促状と違う: 督促状記載の住所で問題ない。あわせて現住所を教えてもらう
- 名字が変わった: 影響なし。通知書記載の名字で手続きする。あわせて現在の姓を教えてもらう
- 通知発送まで: 契約後24時間以内に発送（営業の兼ね合いで48時間程度かかる場合あり）
支払い・契約:
- 振込先: 契約後にご案内する（契約書に記載）
- 領収書: LINEまたはメールで送付
- 家族名義のカード決済: 問題なし（カテゴリは「手続きの一般的な流れ」。金額を答えない支払い実務の質問に費用の固定文は不要）
- キャンセル: 制度の説明「通知発送前であればキャンセル・返金が可能です」までは自動送信可。実際にキャンセルしたい・解約したいという申し出への応答は「解約・辞任関係」（auto_send=false）
時効・法律:
- 保証人になっている借金: 時効援用可
- 保証人への請求を心配された場合、次の整理をそのまま使う:「保証人の方に請求が行く可能性はゼロではありません。ただし保証人の方も同じ借金の時効を主張（援用）できるため、その場合は保証人の方の手続きも別途承ることが可能です」
- 自己破産を検討・相談した過去: 基本的に影響なし
- 業者からの減額通知への応答は次の言い回しを厳守（改変禁止）:「一般的に、減額のご案内は時効にかかっている可能性が高くなる傾向はありますが、時効にかかっていなくても届くことがあります。この通知だけで時効の成否を判断することはできません」
- 「時効間近」という表現は全応答で使用禁止。顧客が使っても復唱しない（サーバー側でも降格される）
- 業者からの反論・裁判の可能性:「反論されることはあります。当方の主張に正当性がある場合は再反論等の対応をします」
- 過払い金調査: 基本的に行っていない
- 複数社のうち1社だけの依頼: 可能。「無理のない範囲でご依頼ください」と添える
- 亡くなった親（親族）の借金: 時効援用は可能と案内してよい（このFAQの範囲では auto_send=true。承認に回さない）。ただし相続放棄と迷っている場合の選択の相談は承認制（カテゴリ「法的判断・見通し」auto_send=false）
- 給料等の差押えを現に受けている状態についての時効可否の質問: 一般論「差押えを受けている場合、時効が更新されているため時効援用はできません」までは自動送信可（カテゴリ「手続きの一般的な流れ」、auto_send=true。下の緊急対応ルールより本項が優先）。同じ返信の中で「状況を確認いたしますので、差押えに関する書類の写真をこのLINEにお送りいただけますか」と資料収集につなげる。jikou_update_flag は必ず true にし、以後の時効に関する回答はすべて承認制。※「差押えの通知・予告や裁判所からの書類が届いた」という第一報は本項ではなく緊急対応（auto_send=false + immediate_notice="court_doc_request"）
- 任意整理を途中でやめた借金: 承認制（個別判断。FAQでは答えない）
状況・属性:
- 生活保護受給中: 依頼可能
- ご本人がLINEを使えない場合の家族からの相談: 相談可能（既存ルールどおり、ご本人の身分証明書の提出が必要）
- 外国籍: 依頼可能
- 海外在住: 手続き可能。契約時に現住所等を教えてもらう
事務所・信頼:
- 対応者について:「弁護士の責任のもとで最初から最後まで対応いたします。ご案内の一部にAIを活用していますが、法的判断と手続きはすべて弁護士が行います」
- 他事務所で断られた:「もちろん確認いたします」と前向きに受ける
- 実績: 時効援用の解決実績は1000件以上。Googleの口コミは「大野法律事務所 川口」で検索いただけると案内してよい
進行中・完了後:
- 通知送付後、結果判明まで: 2〜4週間程度が多い
- 完了後の追加依頼の費用: 同一料金（1社あたり44,000円（税込））。割引制度はない。費用の質問なので【費用の定型案内】の固定文ルールに従う。※「安くなりますか？」と割引の有無を尋ねられただけならこのFAQで自動送信可。値引きを求める交渉（「安くしてほしい」等）は「費用交渉・減額相談」
- 時効成立の証明書: 業者から証明書等は発行されない。完了は既存FAQのとおりLINEまたはメールで報告する

【FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可）】
時効の基本・期間:
- 時効は何年か: 次の文言をそのまま使い、借入先の確認質問を1つ添える:
  「<<JIKOU_YEARS>>」
- 最終返済日の記憶が曖昧（7〜8年前等）: 5年以上経過している認識であれば問題ない旨を案内し、見立てに接続する
- 「借りたのは15年前、5年前まで返済していた」等: 誤解訂正型で答える。時効の起算点は借入日ではなく最終返済日。最終返済から5年以上経過していれば問題ない
- 時効完成までの残り期間を教えて: 当事務所では正確な判断ができないため回答しない。完成後（最終返済から5年経過後）のご依頼を案内する
- 完成まで待つべきか: 待ってからのご依頼を推奨。完成前に援用しても二度手間になることに加え、業者が裁判手続きをとってくることがある旨を説明する
更新事由の細かい判断:
- 電話で「払えません」と言った: まずくない。支払いを拒む発言は債務の承認に当たらない（jikou_update_flag も立てない）
- 電話で「調べて折り返します」と言った: 問題ない（フラグも立てない）
- アンケート様の書類を返送した:「どのような内容で返送されましたか？」と確認質問をした上で、次を添える:
  「<<QUESTIONNAIRE_RETURN>>」
- 10年以上前に裁判された記憶があるが不確実: 経過が不確実な場合は、明らかに10年以上経過していると判断できる状況になってから手続きをとる方がよい旨を案内する
- 差押えは給料以外も対象か: 次の文言をそのまま使う:
  「<<SEIZURE_SCOPE>>」
- 業者が自宅に来て話した:「どのような内容をお話しされましたか？」と確認質問をした上で、次を添える:
  「<<HOME_VISIT>>」
業者の行動:
- 「法的手続きに移行します」の通告が来た: 必ずしも裁判手続きがなされるとは限らない。一方、実際に裁判がいつ行われても不思議でない段階のため、早めの手続きが望ましい（自動送信可。書類の写真送付を勧めてよい）
- 業者側の弁護士事務所から通知が来た: それは裁判ではない。裁判は裁判所からの書類によって行われる（自動送信可）。※「無視」という語は使わず、裁判ではない事実の説明と書類の写真送付の案内に留める
- （受任後の顧客）通知を送ったのにまだ督促が来る: ご依頼から10日前後は行き違いで通知が届くことがある。ご依頼から1ヶ月程度経過後に届いた場合は必ずご連絡ください、と案内する。※この案内で「無視」という語を使わない。督促状への対応に言及する場合は承認済み定型指示の全文をそのまま使う
費用・手続きの深掘り:
- 不成立時にいくら損するか: 返金はなく、手続き費用分の損失になる（カテゴリ「手続きの一般的な流れ」。既存の不成立時費用の説明と整合させる）
- 配偶者に内緒・郵送は本当にないか: 本当にない（既存の家族への秘匿FAQを強調して再掲してよい）
- メールアドレスがない: 次の文言の趣旨で案内する:
  「<<MAIL_ADDRESS>>」
- ケースワーカーに相談してから決めてもいいか: もちろん問題ない
資料・状況:
- 未開封の封筒を全部写真で送りたい: もちろん可能、と歓迎して受け付ける
- 詐欺ではないか・先にお金だけ取られないか（**当事務所への**信頼確認）: そのようなことは一切ない、と明確に否定する（実績1000件以上・Google口コミのFAQに自然に接続してよい）。※契約前のこの種の不安の確認は「クレーム・不満」ではなくFAQで自動送信可（受任後の対応への不満はこれまでどおりクレーム・不満で承認制）
- ※上記と区別: **業者からの請求が詐欺かどうか**の判断（「身に覚えのない請求書が届いた。詐欺か？払う必要はあるか？」等）は個別の法的判断のため承認制（カテゴリ「法的判断・見通し」、auto_send=false）
完了後・その他:
- また借金やクレジットカードは作れるか: 手続きから5年程度はカード作成やローンは組めない前提でいた方がよい。一方、時効援用から数ヶ月以内に作成できる場合もある。※「絶対」という語は使わない
- 家族・職場に知られるか: ない。逆に時効援用手続きをとらないと、差押え等をされた際に職場や家族に知られてしまうことがある

【必ず auto_send=false にするケース（上記に加えて）】
- 不満・不信・強い不安の表明を伴うメッセージ（「本当に進めてくれているんですか」「対応が遅い」等）。進捗確認の形をとっていてもカテゴリ「クレーム・不満」で auto_send=false
- 進捗について、上記顧客情報（ステータス等）にない事実を答える必要がある場合（「進捗の事実回答」は記録にある事実の範囲のみ）
- 裁判所書類・差押えなど緊急連絡の場合（カテゴリ「緊急対応」。第一報なら immediate_notice="court_doc_request"）。※既に差押えを受けている状態についての時効可否の質問は、FAQ第2弾の差押え項目（自動送信可）が優先
- 受任後の顧客から業者への支払いの可否を問われた場合（カテゴリ「法的判断・見通し」。督促状・電話への対応は上記FAQの定型指示で自動送信可）
- 本人以外（家族・第三者）への個別対応の場合（FAQの一般案内を除く）
- 不受任ステータスの顧客から新規受任の可否を問われた場合
- 費用交渉・クレーム・解約の場合
- 判断に迷う場合（迷ったらfalse）\
"""

# 固定文（費用・電話定型指示）をプロンプトに埋め込む。
# {customer_name} 等の named placeholder は build_system_prompt() で埋める。
_SYSTEM_PROMPT_TMPL = (
    _SYSTEM_PROMPT_BASE
    .replace("<<FEE_GUIDE>>", FEE_GUIDE_TEXT)
    .replace("<<PHONE_INSTRUCTION>>", APPROVED_PHONE_INSTRUCTION)
    .replace("<<DUNNING_INSTRUCTION>>", APPROVED_DUNNING_INSTRUCTION)
    .replace("<<BRANCHING_EXAMPLE>>", BRANCHING_GUIDANCE_EXAMPLE)
    .replace("<<HOTERASU_REPLY>>", HOTERASU_STANDARD_REPLY)
    .replace("<<JIKOU_YEARS>>", JIKOU_YEARS_TEXT)
    .replace("<<QUESTIONNAIRE_RETURN>>", QUESTIONNAIRE_RETURN_TEXT)
    .replace("<<HOME_VISIT>>", HOME_VISIT_TEXT)
    .replace("<<SEIZURE_SCOPE>>", SEIZURE_SCOPE_TEXT)
    .replace("<<MAIL_ADDRESS>>", MAIL_ADDRESS_TEXT)
    # AUTOREPLY-STYLE-1: 文体規範+見本（他の置換より後段=見本文中に置換対象
    # マーカーが無いことは test_autoreply_style1 で pin）
    .replace("<<STYLE_SECTION>>", STYLE_SECTION)
)

# ── tool 定義 ─────────────────────────────────────────────────────────────────
_COMPOSE_REPLY_TOOL = {
    "name": "compose_reply",
    "description": "顧客への返信文を作成し、カテゴリと送信可否を判定する",
    "input_schema": {
        "type": "object",
        "properties": {
            "reply": {
                "type": "string",
                "description": "顧客への返信文（400字以内目安）",
            },
            "category": {
                "type": "string",
                "enum": [
                    "挨拶・雑談",
                    "手続きの一般的な流れ",
                    "必要書類の案内",
                    "費用の定型案内",
                    "進捗の事実回答",
                    "営業案内・アクセス",
                    "時効見立て_条件付き",
                    "法的判断・見通し",
                    "費用交渉・減額相談",
                    "クレーム・不満",
                    "解約・辞任関係",
                    "緊急対応",
                    "本人確認不能・第三者",
                    "その他判断系",
                ],
                "description": "メッセージのカテゴリ",
            },
            "auto_send": {
                "type": "boolean",
                "description": "自動送信してよいか。trueでもサーバー側でカテゴリ・禁止語・必須文言を検証する",
            },
            "jikou_update_flag": {
                "type": "boolean",
                "description": (
                    "会話履歴・今回のメッセージ・顧客情報に時効更新事由の疑い"
                    "（裁判所書類の受領・差押え・一部弁済・支払意思の表明）が"
                    "現れているか。否定の回答（届いていない等）は false"
                ),
            },
            "immediate_notice": {
                "type": "string",
                "enum": [
                    "none", "court_doc_request", "churn_neutral",
                    "out_of_scope_debt", "crisis_support", "urgent_seizure_panic",
                ],
                "description": (
                    "auto_send=false のとき顧客へ即時送信する定型文の選択。"
                    "court_doc_request=裁判所書類の第一報 / churn_neutral=諦め・離脱兆候 / "
                    "out_of_scope_debt=税金・個人からの借入れ / "
                    "crisis_support=希死念慮の表明 / "
                    "urgent_seizure_panic=差押え等が目前と訴えるパニック / "
                    "該当なしと auto_send=true は none"
                ),
            },
            "reason": {
                "type": "string",
                "description": "弁護士向けの判断理由（1〜2文）",
            },
        },
        "required": [
            "reply", "category", "auto_send",
            "jikou_update_flag", "immediate_notice", "reason",
        ],
    },
}


# ── ユーティリティ ─────────────────────────────────────────────────────────────

def _kintone_base() -> str:
    sub = _SUBDOMAIN.replace(".cybozu.com", "").strip()
    return f"https://{sub}.cybozu.com"


# ── ルーティング判定 ────────────────────────────────────────────────────────────

def classify_routing(status: str,
                     profile: BusinessProfile | None = None) -> str:
    """
    案件 status 値からルーティング先を返す（H2: status 語彙はプロファイル側・
    3 値分類は機構側）。
      "hearing"        : ヒアリング未完了 → 既存フロー
      "post_engagement": 受任後 → 顧客対応Claude（受任後）
      "pre_engagement" : 受任前 → 顧客対応Claude（受任前）
                         ※ 想定外の値は安全側フォールバックで pre_engagement
    """
    p = profile or JIKOU_PROFILE
    if status in p.hearing_statuses:
        return "hearing"
    if status in p.post_engagement_statuses:
        return "post_engagement"
    return "pre_engagement"


def build_system_prompt(
    *,
    status: str,
    customer_name: str = "（未登録）",
    business_name: str = "（未登録）",
    borrow_period: str = "（未登録）",
    last_payment: str = "（未登録）",
    court_docs: str = "（未登録）",
    credit_check: str = "（未登録）",
    known_items: Optional[dict] = None,
    profile: BusinessProfile | None = None,
) -> str:
    """顧客対応Claudeのシステムプロンプトを組み立てる。

    AUTOREPLY-GEN2 要件3: known_items（収集済み項目台帳・build_known_items）
    を渡すと「既知項目一覧+再質問禁止」の節を追記する。
    H2: テンプレ本文はプロファイル側・phase 分類と既知項目節は機構側。
    ※ 顧客情報 placeholder 集合（business_name 等）は時効テンプレ固有の
    署名のまま（相続放棄の prompt 組み立ては H-3 で設計）。"""
    p = profile or JIKOU_PROFILE
    routing = classify_routing(status, profile=p)
    phase = "受任後" if routing == "post_engagement" else "受任前"
    prompt = p.system_prompt_template.format(
        phase=phase,
        customer_name=customer_name,
        status=status,
        business_name=business_name,
        borrow_period=borrow_period,
        last_payment=last_payment,
        court_docs=court_docs,
        credit_check=credit_check,
    )
    if known_items:
        lines = "\n".join(f"- {k}: {v}" for k, v in known_items.items())
        prompt += (
            "\n\n【収集済み項目（既知・再質問禁止）】\n" + lines +
            "\n上記は既に回答済み・受領済みの項目です。同じ内容を再度"
            "質問・依頼しないでください。"
        )
    return prompt


# ── サーバー側ガード（自動送信前の二重チェック） ─────────────────────────────────

@dataclass
class GuardResult:
    """apply_server_guards() の判定結果"""
    can_auto_send: bool
    demotion_reasons: list[str] = field(default_factory=list)
    immediate_notice: str = "none"  # notice_texts のキー or "none"
    # H2: プロファイルの即時定型集合（None=時効の IMMEDIATE_NOTICE_TEXTS。
    # 既存の生成箇所・テストは省略のままで挙動不変）
    notice_texts: Optional[dict] = None

    @property
    def immediate_notice_text(self) -> Optional[str]:
        """承認キュー行き時に即時送信する定型文（なければ None）"""
        texts = self.notice_texts if self.notice_texts is not None \
            else IMMEDIATE_NOTICE_TEXTS
        return texts.get(self.immediate_notice)


def _strip_allowlisted(text: str,
                       profile: BusinessProfile | None = None) -> str:
    """禁止語照合の前に、弁護士確認済みの許可フレーズを除去する"""
    p = profile or JIKOU_PROFILE
    for phrase in p.allowlisted_phrases:
        text = text.replace(phrase, "")
    return text


def find_forbidden_words(reply: str,
                         profile: BusinessProfile | None = None) -> list[str]:
    """返信文中の禁止語を検出して返す（許可リスト適用後）。
    H2: 禁止語・許可リストはプロファイル側・照合は機構側。"""
    p = profile or JIKOU_PROFILE
    stripped = _strip_allowlisted(reply, profile=p)
    hits = []
    for label, pattern in p.forbidden_patterns:
        for m in pattern.finditer(stripped):
            hits.append(f"{label}「{m.group(0)}」")
    return hits


# ── AUTOREPLY-STYLE-1-fix1 [B]: 文体見本まわりのサーバ側防壁（両経路共通） ────────
# (1) 弁護士本人の名乗り検知: NFKC 正規化+空白除去のうえ、Bot が弁護士本人を
#     自称する形を閉集合で検知→承認降格。「大野法律事務所」（事務所名・凍結
#     テンプレに実在）は名乗りではないため各形で除外される（逐語 pin）。
_SELF_INTRO_PATTERNS: list[tuple[str, re.Pattern]] = [
    # 「弁護士の大野」（直後が「法律事務所」なら事務所名）
    ("弁護士の大野", re.compile(r"弁護士の大野(?!法律事務所)")),
    # 「大野と申します」「大野です」「大野でございます」「大野弁護士です」等
    ("大野と申します/です",
     re.compile(r"大野(?:弁護士)?(?:と申します|と申し上げます|です|でございます)")),
    # 「私は大野」「わたくし、弁護士の大野」「当職大野」等
    ("私は大野",
     re.compile(r"(?:私|わたくし|わたし|当職|小職)(?:は|が|、|,)?(?:弁護士の)?大野")),
    # 「弁護士本人です」「大野本人として」等
    ("弁護士本人です",
     re.compile(r"(?:弁護士|大野)本人(?:です|でございます|として)")),
]

# (2) 見本の匿名化記号の残存: ○○（顧客名）・△△（相手方）・旧「◯社」・□□ が
#     返信に残る＝見本の丸写し/穴埋め失敗。送信せず承認降格
_EXEMPLAR_PLACEHOLDER_RE = re.compile(r"○○|◯◯|△△|□□|◯社")

# (3) 旧見本由来トークンの線引き（fix1 設計）:
#     確定定型・FAQ に同内容の根拠がある語（5年程度・1ヶ月程度・ローン/住宅
#     ローン・信用情報機関への報告）は FAQ ルールの範囲で許容＝新ガードを設けず
#     従来どおり prompt の FAQ 指示に従う。根拠のない旧見本固有の言い回しだけを
#     閉集合で降格する（案件固有の法的内容の無断引用を塞ぐ）
LEGACY_EXEMPLAR_NO_BASIS_PHRASES: tuple[str, ...] = (
    "延滞の文字",       # 旧見本2: 信用情報の「延滞」表記の消去（FAQ は「削除」まで）
    "完全に抹消",       # 旧見本3: 抹消時期の言い切り（FAQ に無い）
    "ご連絡要求",       # 旧見本3: 信用情報機関への「連絡要求」（FAQ は「報告するよう伝える」）
)

# (4) fix3 [B]（STYLE-02）: 経路ごとの根拠集合。「FAQ 根拠あり・許容」は FAQ が
#     prompt に収載される**顧客対応経路に限定**する。ヒアリング prompt
#     （_HEARING_PROMPT_FROZEN）には FAQ が無いため、同語を含む出力は根拠なし＝
#     承認降格（PENDING+承認キュー）。値は顧客対応 prompt の FAQ 文言と対照
#     （test_autoreply_style1 で経路別根拠集合を pin）
FAQ_BACKED_PHRASES: tuple[str, ...] = (
    "5年程度",          # FAQ: 信用情報の削除まで長いと 5 年程度／完了後 5 年程度はローン不可
    "住宅ローン",       # FAQ: カード作成やローンは組めない前提（住宅ローンを含む）
    "1ヶ月程度",        # FAQ: 受任後の督促（ご依頼から 1 ヶ月程度経過後）
)
ROUTE_BASIS: dict[str, frozenset[str]] = {
    "customer": frozenset(FAQ_BACKED_PHRASES),   # FAQ 収載＝根拠あり
    "hearing": frozenset(),                      # FAQ 非収載＝根拠なし
}


def _normalize_for_self_intro(text: str) -> str:
    t = unicodedata.normalize("NFKC", text or "")
    return re.sub(r"\s+", "", t)   # 全角空白は NFKC で半角化済み→除去


def find_attorney_self_intro(reply: str) -> list[str]:
    """弁護士本人の名乗りを検出して返す（空=なし）。"""
    t = _normalize_for_self_intro(reply)
    hits = []
    for label, pattern in _SELF_INTRO_PATTERNS:
        for m in pattern.finditer(t):
            hits.append(f"{label}「{m.group(0)}」")
    return hits


def style_guard_violations(reply: str, *, route: str) -> list[str]:
    """fix1 [B] の降格理由（空=適合）。両経路（顧客対応 apply_server_guards・
    ヒアリング送信ゲート）から同一関数で適用する。

    fix3 [B]: route（"customer" | "hearing"・必須）で根拠集合を切り替える。
    名乗り・記号残存・無根拠語は両経路共通。FAQ 根拠語は route の根拠集合に
    含まれる場合のみ許容（未知の route は fail-closed=根拠なし扱い）。"""
    violations: list[str] = []
    basis = ROUTE_BASIS.get(route, frozenset())
    unbacked = [p for p in FAQ_BACKED_PHRASES
                if p in (reply or "") and p not in basis]
    if unbacked:
        violations.append(
            f"経路（{route}）に根拠のない具体値: " + "、".join(unbacked))
    intro = find_attorney_self_intro(reply)
    if intro:
        violations.append("弁護士本人の名乗り検出: " + "、".join(intro))
    residue = sorted(set(_EXEMPLAR_PLACEHOLDER_RE.findall(reply or "")))
    if residue:
        violations.append("見本の匿名化記号の残存: " + "、".join(residue))
    legacy = [p for p in LEGACY_EXEMPLAR_NO_BASIS_PHRASES if p in (reply or "")]
    if legacy:
        violations.append("旧見本由来の無根拠表現: " + "、".join(legacy))
    return violations


def looks_like_court_doc_report(message: str) -> bool:
    """「裁判所から書類が来た」系の申告かどうか（否定の回答は除外）"""
    return bool(
        _COURT_DOC_HINT.search(message)
        and _RECEIPT_HINT.search(message)
        and not _NEGATION_HINT.search(message)
    )


# ── 時効プロファイル（SOUZOKU-HOUKI-H2・G1）───────────────────────────────────
# 既存 module 定数（弁護士確定・hash pin 凍結済み）への**参照の束**。値の複製は
# しない（単一の正・二重管理なし）。ガード関数は profile=None をこの束に解決
# するため、既存呼び出し・既存テストは無変更で従来挙動と完全一致する。
# 相続放棄プロファイルの実体は H-3/H-5 票で作る（本票は枠のみ）。
JIKOU_PROFILE = BusinessProfile(
    name="jikou",
    hearing_statuses=frozenset(HEARING_STATUSES),
    post_engagement_statuses=frozenset(POST_ENGAGEMENT_STATUSES),
    system_prompt_template=_SYSTEM_PROMPT_TMPL,
    compose_tool=_COMPOSE_REPLY_TOOL,
    update_flag_key="jikou_update_flag",
    auto_send_categories=frozenset(AUTO_SEND_CATEGORIES),
    forbidden_patterns=tuple(_FORBIDDEN_PATTERNS),
    allowlisted_phrases=tuple(ALLOWLISTED_PHRASES),
    allowed_emoji=ALLOWED_CANONICAL_EMOJI,
    customer_style_route="customer",
    hearing_style_route="hearing",
    style_section=STYLE_SECTION,
    fee_category="費用の定型案内",
    fee_required_phrases=tuple(FEE_REQUIRED_PHRASES),
    fee_guide_marker=FEE_GUIDE_MARKER,
    conditional_category="時効見立て_条件付き",
    reservation_general_marker=RESERVATION_GENERAL_MARKER,
    reservation_individual_markers=tuple(RESERVATION_INDIVIDUAL_MARKERS),
    mandatory_reply_vocab=tuple(_HOTERASU_VOCAB),
    mandatory_reply_text=HOTERASU_STANDARD_REPLY,
    mandatory_reply_notice_key="hoterasu",
    mandatory_reply_label="法テラス標準回答の不使用",
    pending_reply=PENDING_REPLY,
    pending_by_category=PENDING_BY_CATEGORY,
    immediate_notice_texts=IMMEDIATE_NOTICE_TEXTS,
    template_dedup_markers=_TEMPLATE_DEDUP_MARKERS,
    urgent_notice_kinds=URGENT_NOTICE_KINDS,
    first_report_detector=looks_like_court_doc_report,
    first_report_notice_key="court_doc_request",
)


def _fee_guide_already_sent(history: list[dict],
                            profile: BusinessProfile | None = None) -> bool:
    """費用の固定文を過去の会話で送付済みか（assistant 発言のマーカー照合）"""
    p = profile or JIKOU_PROFILE
    return any(
        p.fee_guide_marker in m.get("content", "")
        for m in history
        if m.get("role") == "assistant"
    )


def _template_already_sent(notice_key: str, history: list[dict],
                           profile: BusinessProfile | None = None) -> bool:
    """同じ定型文を過去に送信済みか（会話履歴の assistant 発言と照合）"""
    p = profile or JIKOU_PROFILE
    marker = p.template_dedup_markers.get(notice_key, "")
    if not marker:
        return False
    return any(
        marker in m.get("content", "")
        for m in history
        if m.get("role") == "assistant"
    )


def apply_server_guards(
    result: dict, history: list[dict], user_message: str,
    *, sanitize_fatal: bool = False,
    profile: BusinessProfile | None = None,
) -> GuardResult:
    """
    compose_reply の結果にサーバー側の二重チェックを適用する。

    自動送信の条件（すべて満たすこと）:
      - モデルが auto_send=true かつカテゴリが AUTO_SEND_CATEGORIES
      - a) 禁止語（断定語・行動指示語）を含まない（許可リスト適用後）
      - b) カテゴリ「費用の定型案内」なら必須文言をすべて含む
      - c) カテゴリ「時効見立て_条件付き」なら留保文言を含み、
           かつ時効更新事由フラグが立っていない
    違反時は承認制に降格し、降格理由を demotion_reasons に残す。

    承認キュー行きの場合、即時送信する定型文（immediate_notice）を解決する:
      - モデルの選択を採用しつつ、「裁判所から書類が来た」系の第一報は
        サーバー側の検知でも補完する
      - 同じ定型文を過去に送信済みなら通常の定型文（PENDING_REPLY）に戻す
    """
    # H2: 閉集合・文言・フラグ名はプロファイル側（None=時効・挙動不変）。
    # 検査の骨格（線引きの順序・降格の合成）は機構側
    p = profile or JIKOU_PROFILE
    reply = result.get("reply", "") or ""
    category = result.get("category", "")
    auto_send = bool(result.get("auto_send"))
    update_flag = bool(result.get(p.update_flag_key))

    reasons: list[str] = []
    can_auto_send = auto_send and (category in p.auto_send_categories)

    if can_auto_send:
        # a0) AUTOREPLY-GEN2 要件1/2: プレースホルダ残存は送信禁止・
        #     長さ/質問数の超過は自動送信せず承認降格（切り詰めはしない）
        if sanitize_fatal:
            can_auto_send = False
            reasons.append("プレースホルダ/内部マーカー残存")
        structural = reply_sanitizer.structure_violations(reply)
        if structural:
            can_auto_send = False
            reasons.extend(structural)
        # a1) AUTOREPLY-GEN2 要件6: 法テラス質問には標準回答（弁護士確定）を
        #     決定的に到達させる——標準回答を逐語で含まない返信は承認降格し、
        #     即時定型（hoterasu）で標準回答を顧客へ送る
        if p.mandatory_reply_vocab \
                and _mentions_hoterasu(user_message, profile=p) \
                and p.mandatory_reply_text not in reply:
            can_auto_send = False
            reasons.append(p.mandatory_reply_label)
        # a) 禁止語照合
        hits = find_forbidden_words(reply, profile=p)
        if hits:
            can_auto_send = False
            reasons.append("禁止語検出: " + "、".join(hits))
        # a2) AUTOREPLY-STYLE-1-fix1 [B]: 弁護士本人の名乗り・見本の匿名化
        #     記号の残存・旧見本由来の無根拠表現は承認降格
        style_hits = style_guard_violations(reply,
                                            route=p.customer_style_route)
        if style_hits:
            can_auto_send = False
            reasons.extend(style_hits)
        # b) 費用定型の必須文言（会話単位: 固定文を送付済みの顧客への
        #    続き質問には簡潔な回答を許容する。2026-07-03 弁護士承認済みの緩和）
        if category == p.fee_category:
            missing = [ph for ph in p.fee_required_phrases if ph not in reply]
            if missing and not _fee_guide_already_sent(history, profile=p):
                can_auto_send = False
                reasons.append("費用定型の必須文言欠落: " + "、".join(missing))
        # c) 条件付き見立てカテゴリの留保文言・更新事由フラグ
        if category == p.conditional_category:
            has_reservation = (
                p.reservation_general_marker in reply
                or all(m in reply
                       for m in p.reservation_individual_markers)
            )
            if not has_reservation:
                can_auto_send = False
                reasons.append("時効見立ての留保文言なし")
            if update_flag:
                can_auto_send = False
                reasons.append("時効更新事由の疑いフラグあり")

    # 承認キュー行き時の即時定型文を解決
    notice_key = result.get("immediate_notice") or "none"
    if notice_key not in p.immediate_notice_texts:
        notice_key = "none"
    if can_auto_send:
        notice_key = "none"
    else:
        # サーバー側バックストップ: 第一報検知（時効=裁判所書類）で資料収集文面
        if notice_key == "none" and p.first_report_detector is not None \
                and p.first_report_detector(user_message):
            notice_key = p.first_report_notice_key
        # AUTOREPLY-GEN2 要件6: 必須標準回答（時効=法テラス）の承認降格時は
        # 標準回答を即時送信
        if notice_key == "none" and p.mandatory_reply_vocab \
                and _mentions_hoterasu(user_message, profile=p):
            notice_key = p.mandatory_reply_notice_key
        # 同じ定型文の二度送りは通常の定型文に戻す
        if notice_key != "none" \
                and _template_already_sent(notice_key, history, profile=p):
            notice_key = "none"

    return GuardResult(
        can_auto_send=can_auto_send,
        demotion_reasons=reasons,
        immediate_notice=notice_key,
        notice_texts=dict(p.immediate_notice_texts),
    )


# ── kintone App 21 ─────────────────────────────────────────────────────────────

async def get_app21_record(user_id: str) -> Optional[dict]:
    """App 21 から LINEユーザーID でレコードを検索して返す（なければ None）"""
    if not (_SUBDOMAIN and _APP21_TOKEN and _APP21_ID):
        logger.warning("App21 env vars not configured")
        return None
    url = f"{_kintone_base()}/k/v1/records.json"
    params = {
        "app": _APP21_ID,
        "query": f'LINEユーザーID = "{user_id}" order by $id desc limit 1',
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            headers={"X-Cybozu-API-Token": _APP21_TOKEN},
            params=params,
        )
    if not resp.is_success:
        logger.error("App21 search failed: status=%s body=%s",
                     emit(resp.status_code, "count", "log", "operator"),
                     emit(resp.text, "vendor_raw", "log", "operator"))
        return None
    records = resp.json().get("records", [])
    return records[0] if records else None


# ── チャットログ（APP_CHATLOG 未設定時はスキップ） ─────────────────────────────

async def save_to_chatlog(
    user_id: str, role: str, message: str, category: str, auto_sent: str
) -> None:
    """チャットログアプリに1レコード保存。APP_CHATLOG 未設定時はスキップ。"""
    if not (APP_CHATLOG and TOKEN_CHATLOG):
        return
    url = f"{_kintone_base()}/k/v1/record.json"
    body = {
        "app": APP_CHATLOG,
        "record": {
            "line_user_id": {"value": user_id},
            "role":         {"value": role},
            "message":      {"value": message},
            "category":     {"value": category},
            "auto_sent":    {"value": auto_sent},
        },
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            headers={
                "X-Cybozu-API-Token": TOKEN_CHATLOG,
                "Content-Type": "application/json",
            },
            json=body,
        )
    if not resp.is_success:
        logger.warning("chatlog save failed: status=%s body=%s",
                       emit(resp.status_code, "count", "log", "operator"),
                       emit(resp.text, "vendor_raw", "log", "operator"))


async def get_recent_chat_history(user_id: str, limit: int = 10) -> list[dict]:
    """
    チャットログアプリから直近 limit 往復（最大 limit*2 件）のメッセージを取得し、
    Claude messages 形式（role/content）のリストで返す。
    APP_CHATLOG 未設定時は空リスト。
    """
    if not (APP_CHATLOG and TOKEN_CHATLOG):
        return []
    url = f"{_kintone_base()}/k/v1/records.json"
    params = {
        "app": APP_CHATLOG,
        "query": (
            f'line_user_id = "{user_id}" order by $id desc limit {limit * 2}'
        ),
        "fields[0]": "role",
        "fields[1]": "message",
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            headers={"X-Cybozu-API-Token": TOKEN_CHATLOG},
            params=params,
        )
    if not resp.is_success:
        logger.warning("chatlog fetch failed: status=%s body=%s",
                       emit(resp.status_code, "count", "log", "operator"),
                       emit(resp.text, "vendor_raw", "log", "operator"))
        return []
    records = resp.json().get("records", [])
    # desc で取得しているので reversed で古い順に並べ直す
    return [
        {"role": r["role"]["value"], "content": r["message"]["value"]}
        for r in reversed(records)
    ]


# ── 承認キュー（APP_APPROVAL 未設定時はスキップ） ──────────────────────────────

async def save_to_approval_queue(
    user_id: str,
    customer_name: str,
    customer_message: str,
    ai_draft: str,
    category: str,
    reason: str,
) -> Optional[str]:
    """承認キューアプリに下書きを保存し、レコードIDを返す。APP_APPROVAL 未設定時は None。"""
    if not (APP_APPROVAL and TOKEN_APPROVAL):
        logger.warning("APP_APPROVAL not configured, skipping approval queue")
        return None
    url = f"{_kintone_base()}/k/v1/record.json"
    body = {
        "app": APP_APPROVAL,
        "record": {
            "line_user_id":  {"value": user_id},
            "顧客名":        {"value": customer_name},
            "顧客メッセージ": {"value": customer_message},
            "AI下書き":      {"value": ai_draft},
            "カテゴリ":      {"value": category},
            "判断理由":      {"value": reason},
            "ステータス2":   {"value": "承認待ち"},
            "送信済み":      {"value": "no"},
        },
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            headers={
                "X-Cybozu-API-Token": TOKEN_APPROVAL,
                "Content-Type": "application/json",
            },
            json=body,
        )
    if not resp.is_success:
        logger.info("[APP29] save failed: status=%s body=%s",
                    emit(resp.status_code, "count", "log", "operator"),
                    emit(resp.text, "vendor_raw", "log", "operator"))
        return None
    record_id = resp.json().get("id")
    logger.info("[APP29] saved record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    return record_id


async def get_approval_record(record_id: str) -> Optional[dict]:
    """承認キューアプリから指定IDのレコードを取得する"""
    if not (APP_APPROVAL and TOKEN_APPROVAL):
        return None
    url = f"{_kintone_base()}/k/v1/record.json"
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            headers={"X-Cybozu-API-Token": TOKEN_APPROVAL},
            params={"app": APP_APPROVAL, "id": record_id},
        )
    if not resp.is_success:
        logger.error(
            "approval record fetch failed: status=%s body=%s",
            emit(resp.status_code, "count", "log", "operator"),
            emit(resp.text, "vendor_raw", "log", "operator"))
        return None
    return resp.json().get("record")


async def mark_approval_sent(record_id: str) -> None:
    """承認キューの送信済み=yes に更新する（冪等）"""
    if not (APP_APPROVAL and TOKEN_APPROVAL):
        return
    url = f"{_kintone_base()}/k/v1/record.json"
    body = {
        "app": APP_APPROVAL,
        "id": record_id,
        "record": {"送信済み": {"value": "yes"}},
    }
    async with httpx.AsyncClient() as client:
        resp = await client.put(
            url,
            headers={
                "X-Cybozu-API-Token": TOKEN_APPROVAL,
                "Content-Type": "application/json",
            },
            json=body,
        )
    if not resp.is_success:
        logger.error(
            "mark_approval_sent failed: status=%s body=%s",
            emit(resp.status_code, "count", "log", "operator"),
            emit(resp.text, "vendor_raw", "log", "operator"))


# ── LINE 送信 ──────────────────────────────────────────────────────────────────

async def send_line_push(to: str, text: str) -> None:
    """LINE Push API でメッセージを送信する。

    SOUZOKU-HOUKI-H1: 実装は hub/line_channel.push_text へ逐語移設
    （時効チャネル JIKOU_CHANNEL=従来 env・ログ文言不変）。"""
    await line_channel.push_text(line_channel.JIKOU_CHANNEL, to, text)


def build_attorney_notification(
    user_id: str,
    customer_name: str,
    approval_record_id: Optional[str],
    category: str,
    urgent_kind: str = "",
    customer_message: str = "",
) -> str:
    """弁護士向け（業務チャネル）通知文を組み立てる。

    urgent_kind が指定された場合（希死念慮・差押え切迫）は
    【緊急・要即時対応】フォーマット、それ以外は従来の【承認依頼】。

    P1-102（RV-10 S1）: 顧客氏名・相談本文は emit 経由で redact（既定=完全抑止）。
    弁護士は本文中の record No で kintone 承認キューを開いて実体を確認する
    （PII は LINE に載せず record No で参照）。urgent_kind / category は
    統制値なのでそのまま載せる。
    """
    rid = approval_record_id or "（未取得）"
    safe_name = emit(customer_name, "name", "line_business", "attorney")
    safe_msg = emit(customer_message, "freetext", "line_business", "attorney")
    if urgent_kind:
        return (
            f"【緊急・要即時対応】\n"
            f"種別: {urgent_kind}\n"
            f"顧客: {safe_name}\n"
            f"顧客メッセージ: {safe_msg}\n"
            f"承認キューレコードNo: {rid}\n"
            f"至急、kintone 承認キュー No.{rid} を開いて内容を確認しご連絡ください。"
        )
    return (
        f"【承認依頼】\n"
        f"顧客: {safe_name}\n"
        f"カテゴリ: {category}\n"
        f"承認キューレコードNo: {rid}\n"
        f"kintone承認キューを確認し、ステータスを「承認済」に変更してください。"
    )


async def _notify_attorney(
    user_id: str,
    customer_name: str,
    approval_record_id: Optional[str],
    category: str,
    urgent_kind: str = "",
    customer_message: str = "",
) -> None:
    """弁護士に承認依頼（緊急時は【緊急・要即時対応】）を LINE Push で通知する"""
    if not ATTORNEY_LINE_USER_ID:
        logger.info("[ATTORNEY] ATTORNEY_LINE_USER_ID not set, skipping")
        return
    rid = approval_record_id or "（未取得）"
    logger.info(
        "[ATTORNEY] notifying to=%s approval_id=%s",
        emit(ATTORNEY_LINE_USER_ID, "external_ref", "log", "operator"),
        emit(rid, "record_id", "log", "operator"),
    )
    msg = build_attorney_notification(
        user_id, customer_name, approval_record_id, category,
        urgent_kind=urgent_kind, customer_message=customer_message,
    )
    # P1-102（RV-10 S1）: 顧客Bot ではなく業務チャネル（DISPATCHBOT）へ・宛先 allowlist 検証
    from hub.notify import notify_business
    await notify_business(ATTORNEY_LINE_USER_ID, msg)


# ── Claude 呼び出し ────────────────────────────────────────────────────────────

async def _call_compose_reply(system_prompt: str, messages: list[dict],
                              tool: dict | None = None) -> dict:
    """Claude API (tool use / compose_reply 強制) を呼び出し結果 dict を返す

    モデル名は config.py（PRIMARY_MODEL / FALLBACK_MODEL）で管理。
    モデル起因エラー時は claude_gateway が自動フォールバック＋管理者通知する。

    システムプロンプト（約6千トークン）は prompt caching を有効化する。
    キャッシュはプレフィックス一致のため、完全に同一のプロンプト
    （=同一顧客の連続メッセージや、回帰テストの同一status群）で
    2回目以降の入力単価が約1/10になる（2026-07-03 API消費削減・弁護士承認済み）。
    """
    client = anthropic.AsyncAnthropic(api_key=_ANTHROPIC_KEY)
    response = await create_message_with_fallback(
        client,
        context="顧客対応 compose_reply",
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        tools=[tool if tool is not None else _COMPOSE_REPLY_TOOL],
        tool_choice={"type": "tool", "name": "compose_reply"},
        messages=messages,
    )
    block = next((b for b in response.content if b.type == "tool_use"), None)
    if not block:
        raise RuntimeError("compose_reply tool was not called by Claude")
    return block.input  # {"reply": ..., "category": ..., "auto_send": ..., "reason": ...}


# ── Claude 応答不能時の共通処理 ────────────────────────────────────────────────

OUTAGE_DRAFT_PLACEHOLDER = (
    "（AI応答不能のため下書きがありません。顧客メッセージを確認し、"
    "この欄に返信文を記入して承認してください）"
)
OUTAGE_CATEGORY = "AI障害・要対応"


async def handle_claude_outage(
    user_id: str,
    user_message: str,
    reply_token: str,
    reply_func: Callable,
    customer_name: str = "",
    error: str = "",
) -> None:
    """
    PRIMARY / FALLBACK の両方で Claude 応答が得られなかったときの共通処理。

    1. ユーザーには定型の「確認中」応答を返す
    2. 承認キュー（App 29）に要対応レコードを作成する
    3. 弁護士に承認依頼を LINE Push で通知する
    """
    await reply_func(reply_token, PENDING_REPLY)
    approval_id = await save_to_approval_queue(
        user_id=user_id,
        customer_name=customer_name,
        customer_message=user_message,
        ai_draft=OUTAGE_DRAFT_PLACEHOLDER,
        category=OUTAGE_CATEGORY,
        reason=f"Claude応答不能（要手動対応）: {error[:200]}",
    )
    await save_to_chatlog(user_id, "user", user_message, OUTAGE_CATEGORY, "no")
    await save_to_chatlog(user_id, "assistant", PENDING_REPLY, OUTAGE_CATEGORY, "yes")
    await _notify_attorney(user_id, customer_name, approval_id, OUTAGE_CATEGORY)
    logger.info("[OUTAGE] queued user_id=%s approval_id=%s",
                emit(user_id, "external_ref", "log", "operator"),
                emit(approval_id, "record_id", "log", "operator"))


# ── メインハンドラ ─────────────────────────────────────────────────────────────

async def handle_customer_message(
    user_id: str,
    user_message: str,
    reply_token: str,
    app21_record: dict,
    reply_func: Callable,
    profile: BusinessProfile | None = None,
) -> None:
    """
    顧客対応 Claude のメインエントリーポイント。

    Parameters
    ----------
    user_id       : LINE ユーザーID
    user_message  : 顧客のメッセージ本文
    reply_token   : LINE Reply API トークン
    app21_record  : get_app21_record() で取得した kintone App21 レコード dict
    reply_func    : async (reply_token: str, text: str) -> None
                    LINE に返信するための呼び出し元提供の非同期関数
    profile       : 業務プロファイル（H2。None=時効・従来挙動と完全一致）。
                    ※ 直下の案件フィールド抽出（顧客名・借入時期 等）は
                    App21=時効スキーマ固有のまま（相続放棄の entry は H-3）
    """
    p = profile or JIKOU_PROFILE
    # App21 レコードから案件情報を取り出す
    status        = app21_record.get("status", {}).get("value", "")
    customer_name = app21_record.get("顧客名", {}).get("value", "") or "（未登録）"
    business_name = (
        app21_record.get("ルックアップ_0", {}).get("value", "")
        or app21_record.get("問い合わせ業者名", {}).get("value", "")
        or "（未登録）"
    )

    def _field(code: str) -> str:
        return app21_record.get(code, {}).get("value", "") or "（未登録）"

    # チャット履歴（直近10往復）を取得してメッセージに追加
    history = await get_recent_chat_history(user_id)
    messages = history + [{"role": "user", "content": user_message}]

    # AUTOREPLY-GEN2 要件3: 収集済み項目台帳（App21 の正+画像受領マーカー・
    # fail-open）を既知項目一覧としてプロンプトへ注入し再質問を禁止
    known_items = build_known_items(app21_record, history)

    system_prompt = build_system_prompt(
        status=status,
        customer_name=customer_name,
        business_name=business_name,
        borrow_period=_field("借入時期_テキスト"),
        last_payment=_field("最終返済日_テキスト"),
        court_docs=_field("裁判所書類"),
        credit_check=_field("信用情報確認"),
        known_items=known_items,
        profile=p,
    )

    # Claude で返信案を作成
    try:
        result = await _call_compose_reply(system_prompt, messages,
                                           tool=p.compose_tool)
    except ClaudeUnavailableError as e:
        # PRIMARY / FALLBACK 両方失敗 → 確認中応答 + 承認キューに要対応レコード
        logger.exception("compose_reply unavailable for user_id=%s", user_id)
        await handle_claude_outage(
            user_id=user_id,
            user_message=user_message,
            reply_token=reply_token,
            reply_func=reply_func,
            customer_name=customer_name,
            error=str(e),
        )
        return
    except Exception:
        logger.exception("compose_reply failed for user_id=%s", user_id)
        # 一時的なエラー（レート制限等）は定型文を返して終了
        await reply_func(reply_token, PENDING_REPLY)
        return

    # AUTOREPLY-GEN2 要件1: 送信直前サニタイズ（markdown 平文化・許可外
    # 絵文字除去）。fatal（プレースホルダ/内部マーカー残存）はガードで
    # 承認降格＝その文面は送信しない
    reply_text, sanitize_issues, sanitize_fatal = \
        reply_sanitizer.sanitize_reply(
            result["reply"], allowed_emoji=p.allowed_emoji)
    if sanitize_issues:
        logger.info("[SANITIZE] user_id=%s issues=%s",
                    emit(user_id, "external_ref", "log", "operator"),
                    emit(sanitize_issues, "freetext", "log", "operator"))
    result = dict(result, reply=reply_text)
    category   = result["category"]
    auto_send  = result["auto_send"]
    reason     = result.get("reason", "")
    logger.info("[COMPOSE_REPLY] user_id=%s reason=%s",
                emit(user_id, "external_ref", "log", "operator"),
                emit(reason, "freetext", "log", "operator"))

    # サーバー側二重チェック（カテゴリ許可リスト＋禁止語・必須文言・留保文言
    # ＋GEN2: プレースホルダ/長さ/質問数/法テラス標準回答）
    guard = apply_server_guards(result, history, user_message,
                                sanitize_fatal=sanitize_fatal, profile=p)
    if guard.demotion_reasons:
        logger.info("[GUARD] demoted user_id=%s reasons=%s",
                    emit(user_id, "external_ref", "log", "operator"),
                    emit(guard.demotion_reasons, "freetext", "log", "operator"))

    # ユーザーメッセージをチャットログに保存
    await save_to_chatlog(user_id, "user", user_message, category, "no")

    if guard.can_auto_send:
        await reply_func(reply_token, reply_text)
        await save_to_chatlog(user_id, "assistant", reply_text, category, "yes")
        logger.info("[AUTO_SEND] user_id=%s len=%s",
                    emit(user_id, "external_ref", "log", "operator"),
                    emit(len(reply_text), "count", "log", "operator"))
    else:
        # 承認キューに下書きを保存（ガードで降格した場合は理由を併記）
        queue_reason = reason
        if guard.demotion_reasons:
            queue_reason = f"{reason}\n[サーバー側ガードで降格] " + " / ".join(guard.demotion_reasons)
        approval_id = await save_to_approval_queue(
            user_id=user_id,
            customer_name=customer_name,
            customer_message=user_message,
            ai_draft=reply_text,
            category=category,
            reason=queue_reason,
        )
        # 顧客への定型文を返信（裁判所書類の第一報・離脱兆候・対象外債権・
        # 法テラスは専用文面。GEN2 要件5: それ以外はカテゴリ別の文脈化定型
        # ——大野の文言確定=PENDING_CONTEXT_ENABLED までは現行文言）
        ack_text = guard.immediate_notice_text \
            or pending_reply_for(category, profile=p)
        await reply_func(reply_token, ack_text)
        await save_to_chatlog(user_id, "assistant", ack_text, category, "yes")
        # 弁護士へ承認依頼通知（希死念慮・差押え切迫は【緊急・要即時対応】）
        await _notify_attorney(
            user_id, customer_name, approval_id, category,
            urgent_kind=p.urgent_notice_kinds.get(guard.immediate_notice, ""),
            customer_message=user_message,
        )
        logger.info(
            "[APPROVAL] queued user_id=%s approval_id=%s",
            emit(user_id, "external_ref", "log", "operator"),
            emit(approval_id, "record_id", "log", "operator"),
        )
