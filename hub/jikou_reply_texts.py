"""jikou_reply_texts — JIKOU-REPLY-Q2 (a)(d): 定型表（J-0〜J-15・J-H1〜H6）・債権者別ルール表・選択器の決定表（データ表+sha pin）

正本: 時効LINEボット_返信規則_v1.4.md §3（定型）・§4（聞き取り項目 J-H）・§5（債権者別ルール表）・§10-4（適用条件表）。
QUEUE-2026-10-13-D D-1。

文言の扱い（連票の共通規律）:
- 相談者向けの定型文言は凍結コードに直書きせず本データ表に置く。**全文言は仮置き（TABLE_STATUS=provisional・
  大野承認待ち）**。v1.4 に大野の原文が確定している定型は status=frozen_v14（v1.4 凍結版が承認の根拠）、
  v1.4 で未決だった J-0（司令塔仮置き文を採用）・J-5b・J-6・J-11・J-13・J-14・J-15・J-H1〜H6 は PC-A 起草の
  status=needs_approval（**要承認**・大野の承認までは選択器が選ばない＝fail-closed）。
- 大野の承認後は値（text / status）だけ差し替えて TABLE_SHA256_PIN を更新する（コードは触らない）。
- 整合: 金額は 44,000/88,000/22,000 規則（J-7 は凍結済み FEE_GUIDE_TEXT と同一・test で pin）。
  「確認します」「申し伝えます」系（hub/empty_reply_guard の語彙）を含めない（J-0 は司令塔仮置き文を
  そのまま採用・送信は Q1c の経路のみ）。
- sha pin: TABLE_SHA256（TEXTS・CREDITOR_RULES・INTENT_RULES の正規化 JSON）を TABLE_SHA256_PIN と照合し、
  不一致なら選択器・描画器は何も返さない（fail-closed・verify_pin）。
- RV-10: 本 module は文言と規則だけを持ち、ログ・通知・例外に相談者情報を載せない。
"""

import hashlib
import json

TABLE_VERSION = "v1.4-q2-1"
TABLE_STATUS = "provisional"            # 仮置き・大野承認待ち（表全体の印）

STATUS_FROZEN_V14 = "frozen_v14"        # v1.4 凍結版に大野の原文がある（自動送信可の根拠=v1.4）
STATUS_NEEDS_APPROVAL = "needs_approval"  # 要承認（大野の承認まで選択器は選ばない）
STATUSES = (STATUS_FROZEN_V14, STATUS_NEEDS_APPROVAL)

STAGE_ANY = "any"                        # 段階非依存
STAGE_ENGAGED = "engaged"                # 受任後のみ
STAGES = (STAGE_ANY, STAGE_ENGAGED)

UNIT_CONVERSATION = ""                   # 使用単位: 1 会話 1 回
UNIT_COMPANY = "company"                 # 使用単位: 会社ごと（J-5）

SLOT_BASE_MONTH = "基準年月"
SLOT_RECOMMENDED_MONTH = "推奨年月"
SLOT_CREDITORS = "社名列挙"
SLOTS = (SLOT_BASE_MONTH, SLOT_RECOMMENDED_MONTH, SLOT_CREDITORS)

# 凍結済み費用定型（chat_responder.FEE_GUIDE_TEXT と同一・test_jikou_reply_q2 が同一性を pin）
_FEE_GUIDE_TEXT = (
    "【費用のご案内】\n"
    "・費用: 1社あたり44,000円（税込）。2社までは 44,000円（税込）× 社数\n"
    "・3社目からは1社あたり22,000円（税込）。例: 3社 110,000円・4社 132,000円\n"
    "・お支払い: 前払いのみ（分割払いは承っておりません）\n"
    "・お支払い方法: 銀行振込またはカード決済（Stripe・デビットカード可）\n"
    "・万一時効が完成していなかった場合も、時効援用通知の送付と業者への確認までの業務に対する費用は発生いたします。その場合、確認をもって業務は終了となります。"
)


def _t(text: str, *, label: str, status: str, stage: str = STAGE_ANY, auto: bool = True,
       slots: tuple = (), unit: str = UNIT_CONVERSATION, length_exempt: bool = False, note: str = "") -> dict:
    return {"text": text, "label": label, "status": status, "stage": stage, "auto": auto,
            "slots": list(slots), "unit": unit, "length_exempt": length_exempt, "note": note}


# ── 定型表（§3・§4）。【 】は差し込み。順序は描画順の既定（選択器が返した順で描画する） ──
TEXTS: dict = {
    "J-0": _t("ご連絡ありがとうございます。内容を確認のうえ、弁護士からこの LINE でご返信いたします。",
              label="初回受付", status=STATUS_NEEDS_APPROVAL,
              note="司令塔仮置き文を採用（C-1 と同一・hub/send_policy.J0_TEXT が本表を参照）。送信は Q1c の経路のみ"),
    "J-1": _t("お問合せありがとうございます。弁護士の大野と申します。",
              label="自己紹介", status=STATUS_FROZEN_V14,
              note="J-0 未送の会話で bot の最初の実質回答の冒頭に付ける"),
    "J-2": _t("債務の原債権者、最終返済日、裁判の有無等については、時効援用手続きを行って初めて判明するものとなります。",
              label="判明時期", status=STATUS_FROZEN_V14),
    "J-3": _t("債権者から住所確認の連絡が来ても、住所等をお知らせしてはいけません。返信もしないでください。",
              label="住所確認への対応", status=STATUS_FROZEN_V14),
    "J-4": _t("最後の返済から 5 年を経過する前に手続きをすると、時効援用は失敗し、成立しなくなります。"
              "過去に数か月早かったことで時効にかからなかった事例が多数ございます。",
              label="早すぎる援用の危険", status=STATUS_FROZEN_V14,
              note="裁判・差押え・判決確定の申告があれば不可（選択器が除外）"),
    "J-5a": _t("最後の返済が【基準年月】より前だと確信があれば現時点で手続きを行うべきですが、"
               "そうでなければ焦って手続きを行ってはいけません。確実な時期としては【推奨年月】頃をおすすめいたします。"
               "時効期間経過後 6 か月については、時効が猶予されてしまう可能性も 0 ではございませんので。",
               label="推奨時期（起算点=最終返済日）", status=STATUS_FROZEN_V14,
               slots=(SLOT_BASE_MONTH, SLOT_RECOMMENDED_MONTH), unit=UNIT_COMPANY,
               note="債権者別ルール表の確定行・最終返済日の申告・業務 LINE への同時通知の記録が条件"),
    "J-5b": _t("お支払いの約束が守られなくなった時点（期限の利益の喪失）から 5 年が経過しているかが目安になります。"
               "確実な時期としては【推奨年月】頃をおすすめいたします。",
               label="推奨時期（起算点=期限の利益喪失時）", status=STATUS_NEEDS_APPROVAL,
               slots=(SLOT_RECOMMENDED_MONTH,), unit=UNIT_COMPANY,
               note="要承認（v1.4: 江口氏会話 10/3 14:29 の大野返信を原文候補）。確定まで選択不可"),
    "J-6": _t("LINE とメールで手続きを進められます。委任契約書は電子でメールにお送りします。ご自宅への電話や郵送は行いません。",
              label="オンライン対応", status=STATUS_NEEDS_APPROVAL, note="要承認（v1.4: 大野の最終確認待ち）"),
    "J-7": _t(_FEE_GUIDE_TEXT, label="費用", status=STATUS_FROZEN_V14, length_exempt=True,
              note="凍結済み FEE_GUIDE_TEXT をそのまま（単独送信・300 字ゲートの免除対象）"),
    "J-8": _t("時期が来ましたら、またいつでもお気軽にご連絡ください。その際は改めて状況を確認させていただき、手続きをご案内いたします。",
              label="待機の締め", status=STATUS_FROZEN_V14),
    "J-9": _t("特に必要書類はございません。【社名列挙】との契約当時のご住所が現在のものと異なる場合は、そちらをご教示ください。",
              label="必要書類", status=STATUS_FROZEN_V14, stage=STAGE_ENGAGED, slots=(SLOT_CREDITORS,)),
    "J-10": _t("電子での委任契約書をメールアドレス宛にお送りいたします。内容をご確認いただき、ご返送をお願いいたします。"
               "特に重要となるのは、契約書最後の特約事項です。",
               label="委任契約の送付", status=STATUS_FROZEN_V14, stage=STAGE_ENGAGED),
    "J-11": _t("弁護士が交渉のうえ、最終的な条件（金額・支払回数等）をご報告し、ご確認・ご承諾いただいてから和解となります。"
               "勝手に話を進めることはございません。",
               label="和解の流れ", status=STATUS_NEEDS_APPROVAL, stage=STAGE_ENGAGED,
               note="要承認（v1.4: 大野の凍結可否待ち）"),
    "J-12": _t("ご自宅に裁判所からのお手紙が到着していないとのことなので、時効にかかっている可能性は高いかとは考えます。"
               "もっとも、訴訟が起こされている可能性もあり、時効援用を行わない限りは判然といたしません。",
               label="時効の見立て", status=STATUS_FROZEN_V14,
               note="既存の限定許可。J-H3 の回答が「届いていない」と明確に否定されている場合のみ・裁判等の申告と併用禁止"),
    "J-13": _t("裁判所で判決が確定している場合は、時効の期間や進め方が変わることがあります。"
               "お勤め先を相手方に知られている可能性があるかをお聞かせください。",
               label="判決確定の分岐", status=STATUS_NEEDS_APPROVAL, stage=STAGE_ENGAGED, auto=False,
               note="下書きのみ（自動送信不可・結論を言わず J-H5 を聞き大野に通知）"),
    "J-14": _t("JICC（CIC）の開示結果の画面をお送りいただけますと、より正確に確認できます。",
               label="開示結果の依頼", status=STATUS_NEEDS_APPROVAL, note="要承認（v1.4: 大野の最終確定待ち）"),
    "J-14b": _t("携帯電話の番号が当時と異なると出てこないことがあります。郵送での開示もご検討ください。",
                label="開示結果（CIC で出ない場合）", status=STATUS_NEEDS_APPROVAL,
                note="要承認（J-14 の CIC 不表示時の文）"),
    "J-15": _t("お電話でのご説明をご希望でしたら、ご都合のよい日時をお知らせください。",
               label="電話への切替", status=STATUS_NEEDS_APPROVAL, stage=STAGE_ENGAGED, auto=False,
               note="下書きのみ（v1.4: 電話提案の可否は大野の未決）"),
    # 聞き取り項目（§4・1 回の返信で 1〜2 問・各問は凍結文）
    "J-H1": _t("最後に返済されたのはいつ頃でしょうか。年と月だけでも構いません。",
               label="聞き取り: 最後に返済した年月", status=STATUS_NEEDS_APPROVAL),
    "J-H2": _t("債権者から書面や SMS は届いていますか。届いている場合は、差出人の名称をお知らせください。",
               label="聞き取り: 債権者からの書面・SMS", status=STATUS_NEEDS_APPROVAL),
    "J-H3": _t("裁判所からの書類が届いたことはありますか。ご家族が受け取った可能性も含めてお聞かせください。",
               label="聞き取り: 裁判所からの書類", status=STATUS_NEEDS_APPROVAL),
    "J-H4": _t("答弁書や支払督促にお心当たりはありますか。",
               label="聞き取り: 答弁書・支払督促", status=STATUS_NEEDS_APPROVAL),
    "J-H5": _t("お勤め先を相手方に知られている可能性はありますか。",
               label="聞き取り: 職場を知られているか", status=STATUS_NEEDS_APPROVAL, stage=STAGE_ENGAGED),
    "J-H6": _t("契約当時のご住所は、現在のご住所と異なりますか。",
               label="聞き取り: 契約当時の住所", status=STATUS_NEEDS_APPROVAL, stage=STAGE_ENGAGED),
}

TEXT_IDS = tuple(TEXTS)
HEARING_IDS = tuple(i for i in TEXT_IDS if i.startswith("J-H"))
J5_IDS = ("J-5a", "J-5b")

# ── 債権者別ルール表（§5・§10-8）。会社の正規 ID → 時効期間・起算点・月数・注意点・J 系列の差し替え ──
BASIS_LAST_PAYMENT = "last_payment"      # 起算点=最終返済日 → J-5a
BASIS_ACCELERATION = "acceleration"      # 起算点=期限の利益喪失時 → J-5b
RULE_CONFIRMED = "confirmed"             # 確定行（J-5 を返してよい）
RULE_PROVISIONAL = "provisional"         # 仮の行（J-5 を返さず J-H と対応待ち）
DEFAULT_COMPANY_ID = "_default"

CREDITOR_RULES: tuple = (
    {"company_id": DEFAULT_COMPANY_ID, "names": [], "version": 1, "status": RULE_PROVISIONAL,
     "basis": BASIS_LAST_PAYMENT, "period_years": 5, "margin_months": 3, "safety_months": 3,
     "j5": "J-5a", "notes": "既定行（表に無い業者）。司令塔裁定: 余裕 3 か月・安全 3 か月。大野が確定するまで J-5 不可"},
    {"company_id": "abilio", "names": ["アビリオ債権回収", "アビリオ"], "version": 1, "status": RULE_PROVISIONAL,
     "basis": BASIS_LAST_PAYMENT, "period_years": 5, "margin_months": 3, "safety_months": 3,
     "j5": "J-5a", "notes": "v1.4 実例（仮）: 起算点=最終返済日。v1.4 の余裕 6 か月／安全 1 か月は司令塔裁定 3 か月に統一（大野確定待ち）"},
    {"company_id": "lake_shinsei", "names": ["レイク", "新生フィナンシャル", "新生パーソナルローン", "SBI新生銀行"],
     "version": 1, "status": RULE_PROVISIONAL, "basis": BASIS_ACCELERATION, "period_years": 5,
     "margin_months": 3, "safety_months": 3, "j5": "J-5b",
     "notes": "v1.4 実例（仮）: 起算点=期限の利益喪失時・催告期間中に訴訟事例あり。J-5b の文言確定まで選択不可"},
)

# ── 選択器の決定表（§10-3・§10-4）: 分類 ID（閉集合）→ 段階ごとの定型 ID 列 ──
# 分類 ID は LLM（tool 強制）が返す閉集合。文面は返さない（R-J3）。
INTENT_GREETING = "挨拶"
INTENT_WHEN_KNOWN = "判明時期"
INTENT_ADDRESS_CHECK = "住所確認の連絡"
INTENT_TIMING = "援用時期"
INTENT_ONLINE = "オンライン対応"
INTENT_FEE = "費用"
INTENT_DOCUMENTS = "必要書類"
INTENT_CONTRACT = "委任契約"
INTENT_SETTLEMENT = "和解の流れ"
INTENT_PROSPECT = "見立て"
INTENT_JUDGMENT = "判決確定"
INTENT_DISCLOSURE = "開示結果"
INTENT_DISCLOSURE_CIC = "開示結果_CIC不表示"
INTENT_PHONE = "電話希望"
INTENT_URGENT = "緊急・裁判所書類"
INTENT_OTHER = "その他"
INTENTS = (INTENT_GREETING, INTENT_WHEN_KNOWN, INTENT_ADDRESS_CHECK, INTENT_TIMING, INTENT_ONLINE, INTENT_FEE,
           INTENT_DOCUMENTS, INTENT_CONTRACT, INTENT_SETTLEMENT, INTENT_PROSPECT, INTENT_JUDGMENT,
           INTENT_DISCLOSURE, INTENT_DISCLOSURE_CIC, INTENT_PHONE, INTENT_URGENT, INTENT_OTHER)

INTENT_DESCRIPTIONS: dict = {
    INTENT_GREETING: "あいさつ・お礼・雑談だけで問いが無い",
    INTENT_WHEN_KNOWN: "原債権者・最終返済日・裁判の有無などが「いつ分かるか」",
    INTENT_ADDRESS_CHECK: "債権者から住所確認の連絡が来た・返事をしてよいか",
    INTENT_TIMING: "いつ手続きをすべきか・今やってよいか・早すぎないか・推奨時期",
    INTENT_ONLINE: "来所せずに LINE やメールだけで進められるか・電話や郵送の有無",
    INTENT_FEE: "費用・料金・支払方法",
    INTENT_DOCUMENTS: "必要書類・用意するもの",
    INTENT_CONTRACT: "委任契約書の送付・返送・記載内容",
    INTENT_SETTLEMENT: "和解の進め方・勝手に話を進められないか",
    INTENT_PROSPECT: "時効になっているか・見立て・可能性",
    INTENT_JUDGMENT: "判決が確定している・確定後の方針",
    INTENT_DISCLOSURE: "信用情報（JICC・CIC）の開示結果を送るべきか",
    INTENT_DISCLOSURE_CIC: "CIC の開示で情報が出てこない",
    INTENT_PHONE: "電話で話したい・電話をかけてほしい",
    INTENT_URGENT: "裁判所からの書類・差押え・支払督促・判決などの緊急連絡や第一報",
    INTENT_OTHER: "上記のいずれにも当てはまらない",
}

# 決定表: 分類 ID → {"any": [...], "engaged": [...]}。"any" は段階非依存（相談段階・受任後・段階不明）。
# 特別語: "J-5?"=推奨時期（算定可能なときだけ・不可なら J-H1）、"J-12?"=見立て（J-H3 否定記録があるときだけ・
# 無ければ J-H3）。これらの条件判定は hub/jikou_reply_select が行う（判定はサーバ側のみ）。
INTENT_RULES: dict = {
    INTENT_GREETING: {"any": [], "engaged": []},
    INTENT_WHEN_KNOWN: {"any": ["J-2"], "engaged": ["J-2"]},
    INTENT_ADDRESS_CHECK: {"any": ["J-3"], "engaged": ["J-3"]},
    INTENT_TIMING: {"any": ["J-4", "J-5?", "J-8"], "engaged": ["J-4", "J-5?", "J-8"]},
    INTENT_ONLINE: {"any": ["J-6"], "engaged": ["J-6"]},
    INTENT_FEE: {"any": ["J-7"], "engaged": ["J-7"]},
    INTENT_DOCUMENTS: {"any": [], "engaged": ["J-9"]},
    INTENT_CONTRACT: {"any": [], "engaged": ["J-10"]},
    INTENT_SETTLEMENT: {"any": [], "engaged": ["J-11"]},
    INTENT_PROSPECT: {"any": ["J-12?"], "engaged": ["J-12?"]},
    INTENT_JUDGMENT: {"any": [], "engaged": []},          # J-13 は下書きのみ（fail-closed）
    INTENT_DISCLOSURE: {"any": ["J-14"], "engaged": ["J-14"]},
    INTENT_DISCLOSURE_CIC: {"any": ["J-14b"], "engaged": ["J-14b"]},
    INTENT_PHONE: {"any": [], "engaged": []},             # J-15 は下書きのみ（fail-closed）
    INTENT_URGENT: {"any": [], "engaged": []},            # 既存の緊急定型・承認経路に任せる
    INTENT_OTHER: {"any": [], "engaged": []},
}

# JIKOU-REPLY-Q5 (b) の置き場: category（compose_reply の分類）→ 昇格した定型 ID の対応表（本票では空）
CATEGORY_TO_TEXT_IDS: dict = {}


def _canonical() -> str:
    return json.dumps({"version": TABLE_VERSION, "texts": TEXTS, "rules": list(CREDITOR_RULES),
                       "intents": INTENT_RULES, "category_map": CATEGORY_TO_TEXT_IDS},
                      ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def table_sha256() -> str:
    return hashlib.sha256(_canonical().encode("utf-8")).hexdigest()


# 大野の承認で値を差し替えたら本 pin を更新する（test_jikou_reply_q2 が照合）
TABLE_SHA256_PIN = "c8c45c0bc8fafb997dc30bd67430d35909194cdd9ff35eef8497aafeb3e3aacf"


def verify_pin() -> bool:
    return table_sha256() == TABLE_SHA256_PIN


def entry(text_id: str) -> dict | None:
    e = TEXTS.get(text_id)
    return dict(e) if e else None


def text(text_id: str) -> str:
    return str((TEXTS.get(text_id) or {}).get("text") or "")


def is_selectable(text_id: str) -> bool:
    """選択器が自動送信に選んでよい定型: 表にあり・status=frozen_v14・auto=True。"""
    e = TEXTS.get(text_id)
    return bool(e and e["status"] == STATUS_FROZEN_V14 and e["auto"])


def needs_approval_ids() -> tuple:
    return tuple(i for i, e in TEXTS.items() if e["status"] == STATUS_NEEDS_APPROVAL)


def rule_for(creditor_name: str | None) -> dict:
    """債権者名（別名を含む・部分一致）→ ルール行。表に無ければ既定行。"""
    name = (creditor_name or "").strip()
    if name:
        for r in CREDITOR_RULES:
            if r["company_id"] == DEFAULT_COMPANY_ID:
                continue
            for alias in r["names"]:
                if alias and alias in name:
                    return dict(r)
    return dict(next(r for r in CREDITOR_RULES if r["company_id"] == DEFAULT_COMPANY_ID))


def approval_table() -> list[dict]:
    """要承認一覧（pgv の別表用・文言と印）。"""
    return [{"id": i, "label": e["label"], "status": e["status"], "stage": e["stage"], "auto": e["auto"],
             "text": e["text"], "note": e["note"]} for i, e in TEXTS.items()]
