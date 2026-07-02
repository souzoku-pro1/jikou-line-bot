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
