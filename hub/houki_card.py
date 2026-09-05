"""相続放棄 相談カード（来所用・紙 A4）の項目定義 — HOUKI-CARD-TEMPLATE（定数のみ）

- 項目は HEARING_ROUNDS（7 通台本）→ App 40 欄の対応（HOUKI-CONTRACT-CARD-SURVEY 6）
  を正とし、各項目に固定の項目番号（1〜N）を振る。スキャン読み取り（次票）は
  この番号で欄を参照する（項目番号→欄コードの対応表が単一の正）。
- 選択肢の文言は houki_case_store.HEARING_CHOICE_FIELDS の逐語。未成年後見関与
  （NOT_ASKED）は載せない。第 5 群（戸籍）は記録欄なし（読み取り時は通知のみ）。
- 雛形（docx/PDF）は scripts/make_houki_card.py が本定義から生成する。
"""

from dataclasses import dataclass

from hub.houki_case_store import HEARING_CHOICE_FIELDS

CARD_VERSION = "v1"
CARD_TITLE = "相続放棄 相談カード"
CARD_FOOTER = f"{CARD_TITLE} {CARD_VERSION}"          # 版判定用（各ページ下部）
PRIVACY_NOTE = "本カードは相続放棄のご相談のためにのみ使用します。"
CHECK_NOTE = "□ の項目は、該当するものに ✓ または ○ を付けてください。"
DATE_BOX = "西暦［　　　　］年［　　］月［　　］日"
CREDITOR_ROWS = 3
CREDITOR_COLUMNS = ("債権者名", "住所または連絡先", "裁判所からの書類")
CREDITOR_DOC_CHOICES = ("あり", "なし")
CREDITOR_OVERFLOW_NOTE = "4 社以上は裏面・別紙に続けてください。"

# 群（7 通台本の順・見出しは HEARING_ROUNDS の見出しと同じ）
GROUPS: tuple = (
    (1, "亡くなった方について"),
    (2, "日付について"),
    (3, "借金と財産について"),
    (4, "他の相続人について"),
    (5, "戸籍について"),
    (6, "ご依頼者ご自身について"),
    (7, "ご相談の区分について"),
)


@dataclass(frozen=True)
class CardItem:
    number: int          # 固定の項目番号（1〜N・一意）
    group: int           # GROUPS の番号
    label: str           # 印字する見出し
    kind: str            # text / kana_text / date / choice / free / creditors / check_only
    fields: tuple        # App 40 欄コード（空=記録欄なし。複数=1 枠を複数欄へ振り分け）
    choices: tuple = ()  # kind=choice/check_only の選択肢（逐語）
    note: str = ""       # 記入欄の補足
    lines: int = 1       # kind=free の行数


CARD_ITEMS: tuple = (
    # 第 1 群 亡くなった方
    CardItem(1, 1, "亡くなった方のお名前（上段にふりがな）", "kana_text",
             ("被相続人氏名", "被相続人ふりがな")),
    CardItem(2, 1, "亡くなった方の最後のお住まい（市区町村まででも構いません）", "text",
             ("被相続人最後の住所",)),
    CardItem(3, 1, "亡くなった方の本籍（分からなければ「不明」）", "text",
             ("被相続人本籍",)),
    CardItem(4, 1, "亡くなった方とあなたとのご関係", "choice", ("続柄", "続柄その他"),
             choices=HEARING_CHOICE_FIELDS["続柄"],
             note="「その他」の場合は（　）に具体的に記入"),
    CardItem(5, 1, "相続順位（事務所記入）", "choice", ("相続順位",),
             choices=HEARING_CHOICE_FIELDS["相続順位"]),
    # 第 2 群 日付
    CardItem(6, 2, "亡くなった方が亡くなった日", "date", ("死亡日_申告",)),
    CardItem(7, 2, "あなたが、亡くなったことを知った日", "date", ("死亡を知った日_申告",)),
    CardItem(8, 2, "あなたが、ご自身は相続人だと知った日", "date", ("相続人と知った日_申告",)),
    CardItem(9, 2, "日付が不確かな場合のメモ（「○年○月頃」など）", "free", ("日付申告メモ",),
             lines=2),
    CardItem(10, 2, "亡くなったこと（相続人であること）を知った経緯"
                    "（役所からの通知・債権者からの請求・親族からの連絡など）", "free",
             ("知った経緯",), lines=2),
    # 第 3 群 借金と財産
    CardItem(11, 3, "亡くなった方の借金や未払い（督促状や裁判所からの書類が届いていれば、"
                    "その内容も）", "free", ("財産_負債",), lines=3),
    CardItem(12, 3, "債権者（借入先・請求元）", "creditors", ("債権者一覧",),
             note=CREDITOR_OVERFLOW_NOTE),
    CardItem(13, 3, "亡くなった方の財産（預貯金・不動産・車・株など。無ければ「なし」）",
             "free", ("財産_現金預貯金", "財産_不動産", "財産_有価証券"), lines=3),
    CardItem(14, 3, "亡くなった方の預貯金を死亡後に出金して使用したり、価値のある財産を"
                    "処分したりしたことはありますか", "choice", ("財産処分有無",),
             choices=HEARING_CHOICE_FIELDS["財産処分有無"]),
    CardItem(15, 3, "督促状・訴状など、裁判所や債権者からの書類は届いていますか", "choice",
             ("訴訟督促有無",), choices=HEARING_CHOICE_FIELDS["訴訟督促有無"]),
    # 第 4 群 他の相続人
    CardItem(16, 4, "あなた以外に相続人にあたる方（亡くなった方の配偶者・お子さん・親・"
                    "兄弟姉妹など。続柄と人数）", "free", ("他の相続人",), lines=2),
    CardItem(17, 4, "先順位の相続人の状況（放棄済み・連絡が取れない など）", "free",
             ("先順位相続人の状況",), lines=2),
    CardItem(18, 4, "先順位の方の相続放棄の状況（分かる範囲で）", "free",
             ("先順位者の放棄状況",), lines=2),
    CardItem(19, 4, "その方々と一緒に相続放棄をしたいご希望はありますか", "choice",
             ("同時申述希望",), choices=HEARING_CHOICE_FIELDS["同時申述希望"]),
    # 第 5 群 戸籍（記録欄なし）
    CardItem(20, 5, "亡くなった方の戸籍謄本・住民票（除票）をお持ちですか", "check_only",
             (), choices=("あり", "なし"),
             note="お手元になくても、事務所で戸籍謄本等の必要書類を取得可能です"),
    # 第 6 群 ご依頼者ご自身
    CardItem(21, 6, "ご依頼者様ご自身のお名前（上段にふりがな）", "kana_text",
             ("顧客名", "furigana")),
    CardItem(22, 6, "ご住所", "text", ("住所",)),
    CardItem(23, 6, "生年月日", "date", ("生年月日",)),
    CardItem(24, 6, "お電話番号", "text", ("電話番号",)),
    CardItem(25, 6, "メールアドレス", "text", ("メールアドレス",)),
    # 第 7 群 区分
    CardItem(26, 7, "今回のご相談は、ご本人としてのご依頼ですか。それとも、ご親族などの"
                    "代理としてのご相談ですか", "choice", ("本人区分",),
             choices=HEARING_CHOICE_FIELDS["本人区分"]),
)

# 対応表（項目番号 → 欄コード）。読み取り prompt・転記はこれを参照する
ITEM_FIELDS: dict = {item.number: item.fields for item in CARD_ITEMS}
