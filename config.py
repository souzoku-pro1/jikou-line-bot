"""
設定の一元管理モジュール

1. Claude モデル名（PRIMARY / FALLBACK の2段構成）
2. kintone フォーム設計の想定値（daily_healthcheck.py が実環境と照合する）
3. 管理者通知先の LINE ユーザーID解決

モデル廃止通知が来たときの手順は README「モデル廃止通知が来たときの運用手順」を参照。
"""

import os

# ══════════════════════════════════════════════════════════════
# Claude モデル設定
# ══════════════════════════════════════════════════════════════

# 通常時に使うモデル
PRIMARY_MODEL = "claude-sonnet-4-6"

# PRIMARY がモデル起因エラー（404 / model_not_found / 廃止に伴う400系）を
# 返したときに1回だけ自動リトライするモデル
FALLBACK_MODEL = "claude-sonnet-5"

# FALLBACK 呼び出し時にだけ追加するパラメータ。
# Sonnet 5 系は thinking がデフォルト有効（レスポンス先頭に thinking ブロックが
# 入り、max_tokens も thinking に消費される）ため明示的に無効化する。
# ※ FALLBACK_MODEL を claude-fable-5 に変える場合は thinking 指定自体が
#   400 になるので、このパラメータを {} にすること。
FALLBACK_EXTRA_PARAMS: dict = {"thinking": {"type": "disabled"}}


# ══════════════════════════════════════════════════════════════
# App 21 status のルーティング分類
#   （kintone App 21 の status DROP_DOWN 実選択肢:
#     問い合わせ / 受付 / 受任 / 手続き中 / 完了 / 不受任 / 決済完了）
# ══════════════════════════════════════════════════════════════

# ヒアリング未完了 → 既存ヒアリングフロー
HEARING_STATUSES = {"", "受付", "問い合わせ"}
# 受任後 → 顧客対応Claude（受任後モード）
POST_ENGAGEMENT_STATUSES = {"受任", "手続き中", "完了"}
# 上記以外（決済完了・不受任など）→ 顧客対応Claude（受任前モード）


# ══════════════════════════════════════════════════════════════
# kintone フォーム設計の想定値
#   daily_healthcheck.py がフォーム設計取得 API の結果と照合する。
#   - fields: コードが読み書きするフィールドコードと型
#   - required_options: コードが依存する選択肢値（実選択肢に「含まれている」こと。
#     kintone 側で選択肢が追加されるのは問題ないが、削除・改名されたら検知する）
# ══════════════════════════════════════════════════════════════

EXPECTED_KINTONE_SCHEMA = {
    "App 21 (案件)": {
        "app_id_env": "KINTONE_APP_ID",
        "token_env": "KINTONE_API_TOKEN",
        "fields": {
            "status": {
                "type": "DROP_DOWN",
                # 読み: classify_routing / 書き: "問い合わせ"(main.py),
                # "受任"(cloudsign_webhook.py), "決済完了"は /webhook/stripe が参照
                "required_options": sorted(
                    HEARING_STATUSES - {""}
                    | POST_ENGAGEMENT_STATUSES
                    | {"不受任", "決済完了"}
                ),
            },
            "LINEユーザーID": {"type": "SINGLE_LINE_TEXT"},
            "問い合わせ業者名": {"type": "SINGLE_LINE_TEXT"},
            "借入時期_テキスト": {"type": "SINGLE_LINE_TEXT"},
            "最終返済日_テキスト": {"type": "SINGLE_LINE_TEXT"},
            "裁判所書類": {"type": "SINGLE_LINE_TEXT"},
            "信用情報確認": {"type": "SINGLE_LINE_TEXT"},
            "顧客名": {"type": "SINGLE_LINE_TEXT"},
            "住所": {"type": "SINGLE_LINE_TEXT"},
            "生年月日": {"type": "SINGLE_LINE_TEXT"},
            "電話番号": {"type": "SINGLE_LINE_TEXT"},
            "メールアドレス": {"type": "SINGLE_LINE_TEXT"},
            "cloudsign_document_id": {"type": "SINGLE_LINE_TEXT"},
        },
    },
    "App 28 (チャットログ)": {
        "app_id_env": "APP_CHATLOG",
        "token_env": "TOKEN_CHATLOG",
        "fields": {
            "line_user_id": {"type": "SINGLE_LINE_TEXT"},
            "role": {"type": "RADIO_BUTTON", "required_options": ["assistant", "user"]},
            "message": {"type": "MULTI_LINE_TEXT"},
            "category": {"type": "SINGLE_LINE_TEXT"},
            "auto_sent": {"type": "RADIO_BUTTON", "required_options": ["no", "yes"]},
        },
    },
    "App 29 (承認キュー)": {
        "app_id_env": "APP_APPROVAL",
        "token_env": "TOKEN_APPROVAL",
        "fields": {
            "line_user_id": {"type": "SINGLE_LINE_TEXT"},
            "顧客名": {"type": "SINGLE_LINE_TEXT"},
            "顧客メッセージ": {"type": "MULTI_LINE_TEXT"},
            "AI下書き": {"type": "MULTI_LINE_TEXT"},
            "カテゴリ": {"type": "SINGLE_LINE_TEXT"},
            "判断理由": {"type": "MULTI_LINE_TEXT"},
            "ステータス2": {
                "type": "DROP_DOWN",
                # 書き: "承認待ち" / 読み比較: "承認済"
                "required_options": ["承認待ち", "承認済"],
            },
            "送信済み": {"type": "RADIO_BUTTON", "required_options": ["no", "yes"]},
        },
    },
    # ── 相続系4アプリ（/scan・/document・/ocr/fixed-asset・registry_to_kintone が参照）──
    # 型・選択肢は 2026-07-03 にフォーム設計取得 API の実値で登録
    "相談カード (相続)": {
        "app_id_env": "SOUZOKU_KINTONE_APP_ID",
        "token_env": "SOUZOKU_KINTONE_API_TOKEN",
        "fields": {
            # 書き: /scan（相談カード）
            "氏名": {"type": "SINGLE_LINE_TEXT"},
            "生年月日": {"type": "DATE"},
            "住所": {"type": "SINGLE_LINE_TEXT"},
            "電話番号": {"type": "SINGLE_LINE_TEXT"},
            "メールアドレス": {"type": "SINGLE_LINE_TEXT"},
            "被相続人名": {"type": "SINGLE_LINE_TEXT"},
            "続柄": {"type": "SINGLE_LINE_TEXT"},
            "被相続人生年月日": {"type": "DATE"},
            "被相続人死亡日": {"type": "DATE"},
            "被相続人住所": {"type": "SINGLE_LINE_TEXT"},
            "被相続人本籍": {"type": "SINGLE_LINE_TEXT"},
            "ファイル名": {"type": "SINGLE_LINE_TEXT"},
            "登録日時": {"type": "DATETIME"},
            # 読み書き: document_webhook（送付状生成トリガーと添付書き戻し）
            "書類ステータス": {
                "type": "DROP_DOWN",
                "required_options": ["送付状作成", "送付状作成済"],
            },
            "送付状": {"type": "FILE"},
        },
    },
    "戸籍謄本 (相続)": {
        "app_id_env": "KOSEKI_KINTONE_APP_ID",
        "token_env": "KOSEKI_KINTONE_API_TOKEN",
        "fields": {
            "氏名": {"type": "SINGLE_LINE_TEXT"},
            "生年月日": {"type": "DATE"},
            "死亡日": {"type": "DATE"},
            "続柄": {"type": "SINGLE_LINE_TEXT"},
            "婚姻関係": {"type": "SINGLE_LINE_TEXT"},
            # 実体は DROP_DOWN(あり/なし)。一方 /scan のプロンプトは「人名を列挙」させる
            # ため選択肢外の値になり得る（潜在バグ・要修正）。監視は実体に合わせる
            "養子縁組": {"type": "DROP_DOWN", "required_options": ["あり", "なし"]},
            "本籍": {"type": "SINGLE_LINE_TEXT"},
            "筆頭者": {"type": "SINGLE_LINE_TEXT"},
            "ファイル名": {"type": "SINGLE_LINE_TEXT"},
            "登録日時": {"type": "DATETIME"},
        },
    },
    "通帳 (相続)": {
        "app_id_env": "KINTONE_SCAN_APP_ID_TSUCHOU",
        "token_env": "KINTONE_SCAN_API_TOKEN_TSUCHOU",
        # 2026-07-03 時点で Railway に環境変数が未設定（/scan の通帳経路は休眠状態）。
        # env が設定されるまで監視をスキップし、設定されたら自動的に監視対象になる
        "optional": True,
        "fields": {
            "金融機関名": {"type": "SINGLE_LINE_TEXT"},
            "口座番号": {"type": "SINGLE_LINE_TEXT"},
            "名義人": {"type": "SINGLE_LINE_TEXT"},
            "残高": {"type": "NUMBER"},
        },
    },
    "不動産 (相続)": {
        "app_id_env": "KINTONE_FUDOSAN_APP_ID",
        "token_env": "KINTONE_FUDOSAN_API_TOKEN",
        "fields": {
            # 読み書き: /ocr/fixed-asset（所在で検索し評価額・年度を上書き）
            "所在": {"type": "SINGLE_LINE_TEXT"},
            "固定資産税評価額": {"type": "NUMBER"},
            "固定資産税評価年度": {"type": "NUMBER"},
            # 書き: registry_to_kintone.py（手動CLI）
            # 注意: 同スクリプトが書く「担保内容」はフォームに存在せず、「種別」の
            # 選択肢に「区分建物」が無い（2026-07-03 確認・スクリプト側の要修正事項）。
            # 存在しないフィールドは監視に登録しない
            "地番": {"type": "SINGLE_LINE_TEXT"},
            "種別": {"type": "DROP_DOWN", "required_options": ["土地", "建物"]},
            "地目": {"type": "SINGLE_LINE_TEXT"},
            "地積": {"type": "NUMBER"},
            "床面積1階": {"type": "SINGLE_LINE_TEXT"},
            "床面積2階": {"type": "SINGLE_LINE_TEXT"},
            "床面積3階": {"type": "SINGLE_LINE_TEXT"},
            "建物名": {"type": "SINGLE_LINE_TEXT"},
            "部屋番号": {"type": "SINGLE_LINE_TEXT"},
            "専有面積": {"type": "NUMBER"},
            "階数": {"type": "SINGLE_LINE_TEXT"},
            "持分割合": {"type": "SINGLE_LINE_TEXT"},
            "担保抵当権": {"type": "DROP_DOWN", "required_options": ["有", "無"]},
            "備考": {"type": "MULTI_LINE_TEXT"},
            "状況": {"type": "SINGLE_LINE_TEXT"},
        },
    },
    # ── 発送/受領ハブ（docs/architecture/02 §2）──
    # 2026-07-03 作成・フォーム設計APIで27フィールドの全一致を確認済み（T1-1）
    "App 30 (発送管理)": {
        "app_id_env": "APP_SHIPPING",
        "token_env": "TOKEN_SHIPPING",
        "fields": {
            # 共通・案件参照
            "ユニット種別": {"type": "DROP_DOWN", "required_options": ["時効援用"]},
            "チャネル": {
                "type": "DROP_DOWN",
                "required_options": ["職務上請求", "e内容証明", "FAX", "送付案内", "スキャン受領"],
            },
            "方向": {"type": "DROP_DOWN", "required_options": ["発送", "受領"]},
            "案件アプリID": {"type": "SINGLE_LINE_TEXT"},
            "案件レコードID": {"type": "SINGLE_LINE_TEXT"},
            "顧客名表示用": {"type": "SINGLE_LINE_TEXT"},
            "件名": {"type": "SINGLE_LINE_TEXT"},
            # 状態機械（01 §4。承認済への遷移は人のみ・T1-2）
            "発送ステータス": {
                "type": "DROP_DOWN",
                "required_options": ["下書き", "承認待ち", "承認済", "発送処理中", "発送済",
                                     "返送待ち", "完了", "エラー", "却下", "要確認"],
            },
            "実行済み": {"type": "RADIO_BUTTON", "required_options": ["no", "yes"]},
            "承認者コメント": {"type": "MULTI_LINE_TEXT"},
            "却下理由": {"type": "MULTI_LINE_TEXT"},
            "エラー詳細": {"type": "MULTI_LINE_TEXT"},
            "リトライ回数": {"type": "NUMBER"},
            # 宛先
            "宛先名": {"type": "SINGLE_LINE_TEXT"},
            "宛先郵便番号": {"type": "SINGLE_LINE_TEXT"},
            "宛先住所": {"type": "SINGLE_LINE_TEXT"},
            "宛先FAX番号": {"type": "SINGLE_LINE_TEXT"},
            # 成果物・本文
            "成果物": {"type": "FILE"},
            "本文_特記事項": {"type": "MULTI_LINE_TEXT"},
            # 同封物選択の選択肢は仮値「（未設定）」のため required_options を置かない
            # （T2-1 で App 32 と同期する実選択肢に差し替える）
            "同封物選択": {"type": "CHECK_BOX"},
            "チャネル固有データ": {"type": "MULTI_LINE_TEXT"},
            # 発送・追跡・受領
            "発送日時": {"type": "DATETIME"},
            "追跡番号": {"type": "SINGLE_LINE_TEXT"},
            "返送期限": {"type": "DATE"},
            "送達結果": {
                "type": "DROP_DOWN",
                "required_options": ["未確認", "送達済", "不達", "返送受領"],
            },
            "受領ファイル": {"type": "FILE"},
            "Drive_fileId": {"type": "SINGLE_LINE_TEXT"},
        },
    },
    # 2026-07-03 作成・フォーム設計APIで14フィールドの全一致を確認済み
    # （M1 職務上請求の宛先マスタ。初期データ投入は T3-1 で実施）
    "App 31 (市区町村マスタ)": {
        "app_id_env": "APP_CITY_MASTER",
        "token_env": "TOKEN_CITY_MASTER",
        "fields": {
            "団体コード": {"type": "SINGLE_LINE_TEXT"},
            "都道府県": {"type": "SINGLE_LINE_TEXT"},
            "市区町村名": {"type": "SINGLE_LINE_TEXT"},
            "担当部署": {"type": "SINGLE_LINE_TEXT"},
            "郵便番号": {"type": "SINGLE_LINE_TEXT"},
            "住所": {"type": "SINGLE_LINE_TEXT"},
            "電話番号": {"type": "SINGLE_LINE_TEXT"},
            "FAX番号": {"type": "SINGLE_LINE_TEXT"},
            "手数料_戸籍謄本": {"type": "NUMBER"},
            "手数料_除籍改製原": {"type": "NUMBER"},
            "手数料_附票": {"type": "NUMBER"},
            "手数料_住民票": {"type": "NUMBER"},
            "備考": {"type": "MULTI_LINE_TEXT"},
            "有効": {"type": "RADIO_BUTTON", "required_options": ["yes", "no"]},
        },
    },
    # 2026-07-03 作成・フォーム設計APIで7フィールドの全一致を確認済み
    # （M4 送付案内の文章ブロックマスタ。App 30 同封物選択との同期検査は T2-1 で追加）
    "App 32 (同封物ブロックマスタ)": {
        "app_id_env": "APP_ENCLOSURE",
        "token_env": "TOKEN_ENCLOSURE",
        "fields": {
            "ブロックキー": {"type": "SINGLE_LINE_TEXT"},
            "表示名": {"type": "SINGLE_LINE_TEXT"},
            "案内文": {"type": "MULTI_LINE_TEXT"},
            "対象ユニット": {"type": "CHECK_BOX", "required_options": ["時効援用"]},
            "返送要否": {"type": "RADIO_BUTTON", "required_options": ["要", "不要"]},
            "表示順": {"type": "NUMBER"},
            "有効": {"type": "RADIO_BUTTON", "required_options": ["yes", "no"]},
        },
    },
}


# ══════════════════════════════════════════════════════════════
# ユニット設定（発送/受領ハブ・docs/architecture/03 §10）
#   新ユニット（相続放棄・相続一般・補助金）はエントリ追加のみでハブに乗る。
#   T0-3 時点では時効援用のみ登録。
# ══════════════════════════════════════════════════════════════

UNIT_CONFIG = {
    "時効援用": {
        "case_app_env": ("KINTONE_APP_ID", "KINTONE_API_TOKEN"),  # App 21
        "customer_name_field": "顧客名",
        "customer_addr_field": "住所",
        "channels": ["職務上請求", "e内容証明", "FAX", "送付案内", "スキャン受領"],
        "template_dir": "jikou",          # docx_templates/jikou/<種別>.docx（規約配置）
        "return_deadline_days": 21,
    },
}


# ══════════════════════════════════════════════════════════════
# docx テンプレート検査の想定値（daily_healthcheck が照合する）
#   テンプレートを人が編集して差込プレースホルダを消した事故を翌朝までに検知する。
#   キー: リポジトリ相対パス / 値: コードが差し込むプレースホルダ
# ══════════════════════════════════════════════════════════════

EXPECTED_DOCX_TEMPLATES = {
    # document_webhook.py（送付状生成）が差し込む4キー（2026-07-03 実テンプレートで確認）
    "docx_templates/送付状_委任契約書.docx": [
        "{{日付}}", "{{依頼者住所}}", "{{依頼者氏名}}", "{{被相続人名}}",
    ],
}


# ══════════════════════════════════════════════════════════════
# 事務所固定情報（返信用ラベル・帳票の差出人表示に使用）
#   移転対応のため環境変数で注入する（docs/souzoku-houki/04 §5 の OFFICE_INFO 方針）
# ══════════════════════════════════════════════════════════════

def get_office_info() -> dict:
    """事務所の固定情報（未設定の項目は空文字）"""
    return {
        "名称": os.environ.get("OFFICE_NAME", ""),
        "郵便番号": os.environ.get("OFFICE_ZIP", ""),
        "住所": os.environ.get("OFFICE_ADDRESS", ""),
        "電話": os.environ.get("OFFICE_TEL", ""),
    }


# ══════════════════════════════════════════════════════════════
# 管理者通知先
# ══════════════════════════════════════════════════════════════

def get_admin_line_user_id() -> str:
    """管理者通知先の LINE ユーザーID（未設定なら空文字）"""
    return (
        os.environ.get("LINE_ADMIN_USER_ID", "")
        or os.environ.get("ATTORNEY_LINE_USER_ID", "")
    )
