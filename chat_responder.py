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
from dataclasses import dataclass, field
from typing import Callable, Optional

import anthropic
import httpx

from claude_gateway import (
    ClaudeUnavailableError,
    create_message_with_fallback,
)
from config import HEARING_STATUSES, POST_ENGAGEMENT_STATUSES

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

# 承認キュー行き時に PENDING_REPLY の代わりに即時送信できる定型文
IMMEDIATE_NOTICE_TEXTS = {
    "court_doc_request": COURT_DOC_REQUEST_REPLY,
    "churn_neutral": CHURN_NEUTRAL_REPLY,
    "out_of_scope_debt": OUT_OF_SCOPE_DEBT_REPLY,
}

# 同じ定型文を二度送らないための照合マーカー（会話履歴の assistant 発言と照合）
_TEMPLATE_DEDUP_MARKERS = {
    "court_doc_request": "全ページを写真に撮って",
    "churn_neutral": "最適な解決方法は異なります",
    "out_of_scope_debt": "別途個別にご案内",
}

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

# 禁止語照合の前に返信文から除去する許可済みフレーズ
ALLOWLISTED_PHRASES = [
    APPROVED_PHONE_INSTRUCTION,
    APPROVED_DUNNING_INSTRUCTION,
    "お電話に出ないでください",
]

# 禁止語（検出したら自動送信を承認制に降格）
# 断定語は、直後に否定（〜とは限らない/保証できない/言い切れない 等）が続く
# 場合を除外する。法律知識ブロックの必須文言「必ず消滅するとは保証できない」や、
# 断定要求への留保付き応答「絶対に大丈夫とは言えません」等が誤検出されるため
# （2026-07-03 実測・弁護士承認済みの緩和）。
_NEGATION_TAIL = r"(?![^。]{0,8}とは(?:限|保証|言|い|断|申))"
_FORBIDDEN_PATTERNS: list[tuple[str, re.Pattern]] = [
    (
        "断定語",
        re.compile(
            rf"(?:確実に|絶対に|間違いなく){_NEGATION_TAIL}"
            rf"|必ず(?:消滅|成立|時効){_NEGATION_TAIL}"
            r"|100[%％]|１００[%％]"
        ),
    ),
    ("行動指示語", re.compile(r"払わないで|無視して(?!はいけ)|連絡しないで|放置して(?!はいけ)|出ないで")),
    # 「時効間近」は全応答で使用禁止（減額通知等からの安易な示唆を防ぐ。
    # 顧客が使った場合も復唱しない。2026-07-03 FAQ第2弾で弁護士指示）
    ("禁止表現", re.compile(r"時効間近")),
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
- 断定語（確実に/絶対に/間違いなく/必ず 等）・行動指示語（払わないで/無視して/連絡しないで/放置して 等）は使用禁止。例外は2つのみ: (1)【FAQ】記載の受任後顧客への定型指示（電話対応・督促状対応）をそのまま使う場合 (2)「絶対に大丈夫とは言えません」のような否定の形

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
この一般論の説明までは自動送信可（カテゴリ「手続きの一般的な流れ」）。そこから先の個別の質問（「私の場合はどうなりますか」等）が続いたら承認制（カテゴリ「法的判断・見通し」、auto_send=false）に回す。

【immediate_notice（承認必須のとき顧客へ即時返す定型文の選択）】
auto_send=false の場合のみ、次から選ぶ。該当なしは "none"。
- court_doc_request:「裁判所から書類が来た」系の第一報のとき（過去の会話で既に書類の写真送付を依頼済みなら選ばない）
- churn_neutral:「じゃあいいです」「もういいです」等の諦め・離脱の兆候のとき
- out_of_scope_debt: 税金・個人からの借入れが対象の相談のとき
auto_send=true のときは常に "none"。

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
- 亡くなった親（親族）の借金: 時効援用は可能と案内してよい。ただし相続放棄と迷っている場合の選択の相談は承認制（カテゴリ「法的判断・見通し」auto_send=false）
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
- 完了後の追加依頼の費用: 同一料金（1社あたり44,000円（税込））。割引制度はない。費用の質問なので【費用の定型案内】の固定文ルールに従う
- 時効成立の証明書: 業者から証明書等は発行されない。完了は既存FAQのとおりLINEまたはメールで報告する

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
                "enum": ["none", "court_doc_request", "churn_neutral", "out_of_scope_debt"],
                "description": (
                    "auto_send=false のとき顧客へ即時送信する定型文の選択。"
                    "court_doc_request=裁判所書類の第一報 / churn_neutral=諦め・離脱兆候 / "
                    "out_of_scope_debt=税金・個人からの借入れ / 該当なしと auto_send=true は none"
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

def classify_routing(status: str) -> str:
    """
    App 21 の status 値からルーティング先を返す。
      "hearing"        : ヒアリング未完了 → 既存フロー
      "post_engagement": 受任後 → 顧客対応Claude（受任後）
      "pre_engagement" : 受任前 → 顧客対応Claude（受任前）
                         ※ 想定外の値は安全側フォールバックで pre_engagement
    """
    if status in HEARING_STATUSES:
        return "hearing"
    if status in POST_ENGAGEMENT_STATUSES:
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
) -> str:
    """顧客対応Claudeのシステムプロンプトを組み立てる"""
    routing = classify_routing(status)
    phase = "受任後" if routing == "post_engagement" else "受任前"
    return _SYSTEM_PROMPT_TMPL.format(
        phase=phase,
        customer_name=customer_name,
        status=status,
        business_name=business_name,
        borrow_period=borrow_period,
        last_payment=last_payment,
        court_docs=court_docs,
        credit_check=credit_check,
    )


# ── サーバー側ガード（自動送信前の二重チェック） ─────────────────────────────────

@dataclass
class GuardResult:
    """apply_server_guards() の判定結果"""
    can_auto_send: bool
    demotion_reasons: list[str] = field(default_factory=list)
    immediate_notice: str = "none"  # IMMEDIATE_NOTICE_TEXTS のキー or "none"

    @property
    def immediate_notice_text(self) -> Optional[str]:
        """承認キュー行き時に即時送信する定型文（なければ None）"""
        return IMMEDIATE_NOTICE_TEXTS.get(self.immediate_notice)


def _strip_allowlisted(text: str) -> str:
    """禁止語照合の前に、弁護士確認済みの許可フレーズを除去する"""
    for phrase in ALLOWLISTED_PHRASES:
        text = text.replace(phrase, "")
    return text


def find_forbidden_words(reply: str) -> list[str]:
    """返信文中の禁止語を検出して返す（許可リスト適用後）"""
    stripped = _strip_allowlisted(reply)
    hits = []
    for label, pattern in _FORBIDDEN_PATTERNS:
        for m in pattern.finditer(stripped):
            hits.append(f"{label}「{m.group(0)}」")
    return hits


def looks_like_court_doc_report(message: str) -> bool:
    """「裁判所から書類が来た」系の申告かどうか（否定の回答は除外）"""
    return bool(
        _COURT_DOC_HINT.search(message)
        and _RECEIPT_HINT.search(message)
        and not _NEGATION_HINT.search(message)
    )


def _fee_guide_already_sent(history: list[dict]) -> bool:
    """費用の固定文を過去の会話で送付済みか（assistant 発言のマーカー照合）"""
    return any(
        FEE_GUIDE_MARKER in m.get("content", "")
        for m in history
        if m.get("role") == "assistant"
    )


def _template_already_sent(notice_key: str, history: list[dict]) -> bool:
    """同じ定型文を過去に送信済みか（会話履歴の assistant 発言と照合）"""
    marker = _TEMPLATE_DEDUP_MARKERS.get(notice_key, "")
    if not marker:
        return False
    return any(
        marker in m.get("content", "")
        for m in history
        if m.get("role") == "assistant"
    )


def apply_server_guards(
    result: dict, history: list[dict], user_message: str
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
    reply = result.get("reply", "") or ""
    category = result.get("category", "")
    auto_send = bool(result.get("auto_send"))
    update_flag = bool(result.get("jikou_update_flag"))

    reasons: list[str] = []
    can_auto_send = auto_send and (category in AUTO_SEND_CATEGORIES)

    if can_auto_send:
        # a) 禁止語照合
        hits = find_forbidden_words(reply)
        if hits:
            can_auto_send = False
            reasons.append("禁止語検出: " + "、".join(hits))
        # b) 費用の定型案内の必須文言（会話単位: 固定文を送付済みの顧客への
        #    続き質問には簡潔な回答を許容する。2026-07-03 弁護士承認済みの緩和）
        if category == "費用の定型案内":
            missing = [p for p in FEE_REQUIRED_PHRASES if p not in reply]
            if missing and not _fee_guide_already_sent(history):
                can_auto_send = False
                reasons.append("費用定型の必須文言欠落: " + "、".join(missing))
        # c) 時効見立て_条件付きの留保文言・更新事由フラグ
        if category == "時効見立て_条件付き":
            has_reservation = (
                RESERVATION_GENERAL_MARKER in reply
                or all(m in reply for m in RESERVATION_INDIVIDUAL_MARKERS)
            )
            if not has_reservation:
                can_auto_send = False
                reasons.append("時効見立ての留保文言なし")
            if update_flag:
                can_auto_send = False
                reasons.append("時効更新事由の疑いフラグあり")

    # 承認キュー行き時の即時定型文を解決
    notice_key = result.get("immediate_notice") or "none"
    if notice_key not in IMMEDIATE_NOTICE_TEXTS:
        notice_key = "none"
    if can_auto_send:
        notice_key = "none"
    else:
        # サーバー側バックストップ: 裁判所書類の第一報を検知したら資料収集文面を送る
        if notice_key == "none" and looks_like_court_doc_report(user_message):
            notice_key = "court_doc_request"
        # 同じ定型文の二度送りは通常の定型文に戻す
        if notice_key != "none" and _template_already_sent(notice_key, history):
            notice_key = "none"

    return GuardResult(
        can_auto_send=can_auto_send,
        demotion_reasons=reasons,
        immediate_notice=notice_key,
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
        logger.error("App21 search failed: %s %s", resp.status_code, resp.text)
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
        logger.warning("chatlog save failed: %s %s", resp.status_code, resp.text)


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
        logger.warning("chatlog fetch failed: %s %s", resp.status_code, resp.text)
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
        print(f"[APP29] save failed: {resp.status_code} {resp.text[:300]}")
        return None
    record_id = resp.json().get("id")
    print(f"[APP29] saved record_id={record_id}")
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
            "approval record fetch failed: %s %s", resp.status_code, resp.text
        )
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
            "mark_approval_sent failed: %s %s", resp.status_code, resp.text
        )


# ── LINE 送信 ──────────────────────────────────────────────────────────────────

async def send_line_push(to: str, text: str) -> None:
    """LINE Push API でメッセージを送信する"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {_LINE_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"to": to, "messages": [{"type": "text", "text": text}]},
        )
    print(f"[LINE_PUSH] to={to} status={resp.status_code}")
    if not resp.is_success:
        print(f"[LINE_PUSH] ERROR: {resp.text[:300]}")


async def _notify_attorney(
    user_id: str, customer_name: str, approval_record_id: Optional[str], category: str
) -> None:
    """弁護士に承認依頼を LINE Push で通知する"""
    if not ATTORNEY_LINE_USER_ID:
        print("[ATTORNEY] ATTORNEY_LINE_USER_ID not set, skipping")
        return
    rid = approval_record_id or "（未取得）"
    print(f"[ATTORNEY] notifying to={ATTORNEY_LINE_USER_ID} approval_id={rid} category={category}")
    msg = (
        f"【承認依頼】\n"
        f"顧客: {customer_name or user_id}\n"
        f"カテゴリ: {category}\n"
        f"承認キューレコードNo: {rid}\n"
        f"kintone承認キューを確認し、ステータスを「承認済」に変更してください。"
    )
    await send_line_push(ATTORNEY_LINE_USER_ID, msg)


# ── Claude 呼び出し ────────────────────────────────────────────────────────────

async def _call_compose_reply(system_prompt: str, messages: list[dict]) -> dict:
    """Claude API (tool use / compose_reply 強制) を呼び出し結果 dict を返す

    モデル名は config.py（PRIMARY_MODEL / FALLBACK_MODEL）で管理。
    モデル起因エラー時は claude_gateway が自動フォールバック＋管理者通知する。
    """
    client = anthropic.AsyncAnthropic(api_key=_ANTHROPIC_KEY)
    response = await create_message_with_fallback(
        client,
        context="顧客対応 compose_reply",
        max_tokens=1024,
        system=system_prompt,
        tools=[_COMPOSE_REPLY_TOOL],
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
    print(f"[OUTAGE] queued user_id={user_id} approval_id={approval_id}")


# ── メインハンドラ ─────────────────────────────────────────────────────────────

async def handle_customer_message(
    user_id: str,
    user_message: str,
    reply_token: str,
    app21_record: dict,
    reply_func: Callable,
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
    """
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

    system_prompt = build_system_prompt(
        status=status,
        customer_name=customer_name,
        business_name=business_name,
        borrow_period=_field("借入時期_テキスト"),
        last_payment=_field("最終返済日_テキスト"),
        court_docs=_field("裁判所書類"),
        credit_check=_field("信用情報確認"),
    )

    # チャット履歴（直近10往復）を取得してメッセージに追加
    history = await get_recent_chat_history(user_id)
    messages = history + [{"role": "user", "content": user_message}]

    # Claude で返信案を作成
    try:
        result = await _call_compose_reply(system_prompt, messages)
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

    reply_text = result["reply"]
    category   = result["category"]
    auto_send  = result["auto_send"]
    reason     = result.get("reason", "")
    print(f"[COMPOSE_REPLY] user_id={user_id} category={category!r} auto_send={auto_send} reason={reason!r}")

    # サーバー側二重チェック（カテゴリ許可リスト＋禁止語・必須文言・留保文言）
    guard = apply_server_guards(result, history, user_message)
    if guard.demotion_reasons:
        print(f"[GUARD] demoted user_id={user_id} reasons={guard.demotion_reasons}")

    # ユーザーメッセージをチャットログに保存
    await save_to_chatlog(user_id, "user", user_message, category, "no")

    if guard.can_auto_send:
        await reply_func(reply_token, reply_text)
        await save_to_chatlog(user_id, "assistant", reply_text, category, "yes")
        print(f"[AUTO_SEND] user_id={user_id} category={category} len={len(reply_text)}")
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
        # 顧客への定型文を返信（裁判所書類の第一報・離脱兆候・対象外債権は専用文面）
        ack_text = guard.immediate_notice_text or PENDING_REPLY
        await reply_func(reply_token, ack_text)
        await save_to_chatlog(user_id, "assistant", ack_text, category, "yes")
        # 弁護士へ承認依頼通知
        await _notify_attorney(user_id, customer_name, approval_id, category)
        print(
            f"[APPROVAL] queued user_id={user_id} category={category} "
            f"approval_id={approval_id} notice={guard.immediate_notice}"
        )
