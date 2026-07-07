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
    # ── 戸籍読解（R系列・docs/koseki-ocr/02 §1）──
    # 2026-07-05 フォーム設計取得APIで実機22フィールドの全一致を確認して登録。
    # 編製日・消除日は和暦原文保持のため SINGLE_LINE_TEXT（DATE型にしない・
    # 2026-07-05 検収裁定）。App 34（人物）は同日、引き継ぎブリーフの完全形35
    # （当初21＋追加14）との実機全一致を確認して下記に登録済み（02 §2 改訂版参照）
    "App 33 (戸籍読解)": {
        "app_id_env": "APP_KOSEKI_BOOK",
        "token_env": "TOKEN_KOSEKI_BOOK",
        # env 未設定の環境では監視をスキップ（設定されたら自動的に監視対象になる）
        "optional": True,
        "fields": {
            # 案件参照4点（ハブ共通方式）
            "ユニット種別": {
                "type": "DROP_DOWN",
                "required_options": ["時効援用", "相続放棄", "相続一般", "補助金"],
            },
            "案件アプリID": {"type": "SINGLE_LINE_TEXT"},
            "案件レコードID": {"type": "SINGLE_LINE_TEXT"},
            "被相続人名表示用": {"type": "SINGLE_LINE_TEXT"},
            # 戸籍の識別子（原文表記）
            "本籍": {"type": "SINGLE_LINE_TEXT"},
            "筆頭者": {"type": "SINGLE_LINE_TEXT"},
            "戸籍種別": {
                "type": "DROP_DOWN",
                "required_options": ["現行", "改製原（平成）", "改製原（昭和）",
                                     "除籍", "不明"],
            },
            # 連続性の判定材料（和暦原文のまま保持）
            "編製日": {"type": "SINGLE_LINE_TEXT"},
            "消除日": {"type": "SINGLE_LINE_TEXT"},
            "編製事由": {"type": "SINGLE_LINE_TEXT"},
            "従前戸籍_本籍": {"type": "SINGLE_LINE_TEXT"},
            "従前戸籍_筆頭者": {"type": "SINGLE_LINE_TEXT"},
            "新戸籍_本籍": {"type": "SINGLE_LINE_TEXT"},
            # 原本・読解結果（/koseki/ingest・koseki_reader が読み書き）
            "原本PDF": {"type": "FILE"},
            "ページ画像": {"type": "FILE"},
            "Drive_fileId": {"type": "SINGLE_LINE_TEXT"},
            "読解JSON": {"type": "MULTI_LINE_TEXT"},
            "読解状態": {
                "type": "DROP_DOWN",
                # 書き: "未読解"(koseki_ingest) / "AI読解済"・"要再読解"(koseki_reader)
                # "確認済" は人手確認フロー（R4）が使う
                "required_options": ["未読解", "AI読解済", "確認済", "要再読解"],
            },
            # 監査痕跡・品質管理
            "確認者": {"type": "SINGLE_LINE_TEXT"},
            "確認日時": {"type": "DATETIME"},
            "様式確信度": {"type": "NUMBER"},
            "全体確信度": {"type": "NUMBER"},
        },
    },
    # ── 人物（R系列・docs/koseki-ocr/02 §2 改訂版）──
    # 2026-07-05 実機突合で完全形35（当初21＋追加14・トップ34＋サブテーブル2）の
    # 全一致を確認して登録。SUBTABLE の内部列は healthcheck の検査対象外
    # （型一致のみ検査）。選択肢はフォーム設計取得APIの実機実出力どおり
    "App 34 (人物)": {
        "app_id_env": "APP_KOSEKI_PERSON",
        "token_env": "TOKEN_KOSEKI_PERSON",
        # env 未設定の環境では監視をスキップ（設定されたら自動的に監視対象になる）
        "optional": True,
        "fields": {
            # 案件参照（ハブ共通方式）
            "ユニット種別": {
                "type": "DROP_DOWN",
                "required_options": ["時効援用", "相続放棄", "相続一般", "補助金"],
            },
            "案件アプリID": {"type": "SINGLE_LINE_TEXT"},
            "案件レコードID": {"type": "SINGLE_LINE_TEXT"},
            "被相続人名表示用": {"type": "SINGLE_LINE_TEXT"},
            # 氏名系（表示名・原文・名寄せ用正字）
            "氏名": {"type": "SINGLE_LINE_TEXT"},
            "氏名フリガナ": {"type": "SINGLE_LINE_TEXT"},
            "旧姓別名": {"type": "SINGLE_LINE_TEXT"},
            "氏名_原文": {"type": "SINGLE_LINE_TEXT"},
            "氏名_正字": {"type": "SINGLE_LINE_TEXT"},
            # 確定値の日付（和暦原文は身分事項側）
            "生年月日": {"type": "DATE"},
            "死亡日": {"type": "DATE"},
            "性別": {
                "type": "DROP_DOWN",
                "required_options": ["男", "女", "不明"],
            },
            # 親子エッジ（関係図・相続人導出の骨格）
            "父人物ID": {"type": "SINGLE_LINE_TEXT"},
            "母人物ID": {"type": "SINGLE_LINE_TEXT"},
            "養父人物ID": {"type": "SINGLE_LINE_TEXT"},
            "養母人物ID": {"type": "SINGLE_LINE_TEXT"},
            "被相続人フラグ": {
                "type": "RADIO_BUTTON",
                "required_options": ["no", "yes"],
            },
            "生死区分": {
                "type": "DROP_DOWN",
                "required_options": ["生存", "死亡", "不明"],
            },
            "続柄メモ": {"type": "SINGLE_LINE_TEXT"},
            "本籍最新": {"type": "SINGLE_LINE_TEXT"},
            "住所最新": {"type": "SINGLE_LINE_TEXT"},
            # 名寄せ（候補提示=機械・確定=人）
            "名寄せキー": {"type": "SINGLE_LINE_TEXT"},
            "名寄せ確定": {
                "type": "DROP_DOWN",
                "required_options": ["未確定", "自動候補", "確定"],
            },
            # 相続人導出（候補=機械・資格確定=弁護士）
            "相続人候補": {
                "type": "DROP_DOWN",
                "required_options": ["候補", "非該当", "未判定"],
            },
            "相続資格": {
                "type": "DROP_DOWN",
                "required_options": ["未判定", "法定相続人", "代襲相続人",
                                     "数次相続人", "相続放棄済", "資格なし"],
            },
            # 読解トレーサビリティ・監査痕跡
            "読解由来": {
                "type": "RADIO_BUTTON",
                "required_options": ["AI読解", "手入力"],
            },
            "読解JSON断片": {"type": "MULTI_LINE_TEXT"},
            "グラフ確定日時": {"type": "DATETIME"},
            "確認状態": {
                "type": "DROP_DOWN",
                "required_options": ["未確認", "確認済", "要再確認"],
            },
            "確認者": {"type": "SINGLE_LINE_TEXT"},
            "確認日時": {"type": "DATETIME"},
            "備考": {"type": "MULTI_LINE_TEXT"},
            # サブテーブル（内部列は型検査の対象外）
            "身分事項": {"type": "SUBTABLE"},
            "登場戸籍": {"type": "SUBTABLE"},
        },
    },
    # ── 財産（S系列・docs/souzoku-shorui/01 §1.1）──
    # 2026-07-06 フォーム設計取得APIで実機19フィールドの全一致（型・選択肢順序・
    # 初期値・defaultNowなし）を確認して登録。zaisan_mokuroku / zaisan_sync が読み書き
    "App 35 (財産)": {
        "app_id_env": "APP_ZAISAN",
        "token_env": "TOKEN_ZAISAN",
        # env 未設定の環境では監視をスキップ（設定されたら自動的に監視対象になる）
        "optional": True,
        "fields": {
            # 案件参照（ハブ共通方式）
            "ユニット種別": {
                "type": "DROP_DOWN",
                "required_options": ["時効援用", "相続放棄", "相続一般", "補助金"],
            },
            "案件アプリID": {"type": "SINGLE_LINE_TEXT"},
            "案件レコードID": {"type": "SINGLE_LINE_TEXT"},
            "被相続人名表示用": {"type": "SINGLE_LINE_TEXT"},
            # 財産の実体（1レコード=1財産または1債務）
            "財産種別": {
                "type": "DROP_DOWN",
                "required_options": ["不動産_土地", "不動産_建物", "不動産_区分建物",
                                     "預貯金", "有価証券", "生命保険", "出資金",
                                     "自動車", "動産", "債権", "債務", "葬儀費用",
                                     "その他"],
            },
            "特定情報": {"type": "MULTI_LINE_TEXT"},
            "名義": {"type": "SINGLE_LINE_TEXT"},
            # 評価（確定は弁護士のみ yes・書類生成の前提条件）
            "評価額": {"type": "NUMBER"},
            "評価方法": {
                "type": "DROP_DOWN",
                "required_options": ["固定資産税評価額", "相続税評価額", "残高証明",
                                     "解約返戻金相当額", "時価査定", "額面", "その他"],
            },
            "評価基準日": {"type": "DATE"},
            "評価確定": {
                "type": "RADIO_BUTTON",
                "required_options": ["no", "yes"],
            },
            "備考": {"type": "MULTI_LINE_TEXT"},
            # データ源・トレーサビリティ（OCR経路の確信度・原本必須）
            "データ源": {
                "type": "DROP_DOWN",
                "required_options": ["OCR_課税明細", "OCR_残高証明", "OCR_登記事項証明",
                                     "手入力", "ヒアリング"],
            },
            "確信度": {"type": "NUMBER"},
            "元アプリID": {"type": "SINGLE_LINE_TEXT"},
            "元レコードID": {"type": "SINGLE_LINE_TEXT"},
            "原本": {"type": "FILE"},
            "冪等キー": {"type": "SINGLE_LINE_TEXT"},
            "有効": {"type": "RADIO_BUTTON", "required_options": ["yes", "no"]},
        },
    },
    # ── 相続人（S系列・docs/souzoku-shorui/01 §2）──
    # 2026-07-06 フォーム設計取得APIで実機16フィールドの全一致を確認して登録。
    # 生年月日は SINGLE_LINE_TEXT（正本設計の型指定。協議書の当事者表示に和暦等を
    # そのまま差し込むため DATE 型にしない）
    "App 36 (相続人)": {
        "app_id_env": "APP_SOUZOKUNIN",
        "token_env": "TOKEN_SOUZOKUNIN",
        # env 未設定の環境では監視をスキップ（設定されたら自動的に監視対象になる）
        "optional": True,
        "fields": {
            # 案件参照（ハブ共通方式）
            "ユニット種別": {
                "type": "DROP_DOWN",
                "required_options": ["時効援用", "相続放棄", "相続一般", "補助金"],
            },
            "案件アプリID": {"type": "SINGLE_LINE_TEXT"},
            "案件レコードID": {"type": "SINGLE_LINE_TEXT"},
            "被相続人名表示用": {"type": "SINGLE_LINE_TEXT"},
            # 当事者表示（協議書の署名欄等に差込）
            "氏名": {"type": "SINGLE_LINE_TEXT"},
            "フリガナ": {"type": "SINGLE_LINE_TEXT"},
            "続柄": {
                "type": "DROP_DOWN",
                "required_options": ["配偶者", "子", "直系尊属", "兄弟姉妹",
                                     "甥姪（代襲）", "受遺者（相続人外）", "その他"],
            },
            "法定相続分": {"type": "SINGLE_LINE_TEXT"},
            "住所": {"type": "SINGLE_LINE_TEXT"},
            "生年月日": {"type": "SINGLE_LINE_TEXT"},
            "本籍": {"type": "SINGLE_LINE_TEXT"},
            "連絡先": {"type": "SINGLE_LINE_TEXT"},
            "状態": {
                "type": "DROP_DOWN",
                "required_options": ["通常", "放棄済み", "代襲", "相続分譲渡",
                                     "未成年（特別代理人要）", "成年被後見人"],
            },
            # 書類生成の前提（戸籍確認済=yes は弁護士のみ）・添付書類管理
            "戸籍確認済": {
                "type": "RADIO_BUTTON",
                "required_options": ["no", "yes"],
            },
            "印鑑証明": {
                "type": "DROP_DOWN",
                "required_options": ["未", "依頼中", "受領"],
            },
            "データ源": {
                "type": "DROP_DOWN",
                "required_options": ["ヒアリング", "戸籍読解", "手入力"],
            },
        },
    },
    # ── 割付（S系列・docs/souzoku-shorui/01 §3〔設計上は App 38 表記・実機は 37〕）──
    # 2026-07-06 フォーム設計取得APIで実機11フィールドの全一致（型・選択肢順序・
    # 初期値）を確認して登録。遺産分割協議書の「誰が何を取得するか」の1行=1割付
    "App 37 (割付)": {
        "app_id_env": "APP_WARITSUKE",
        "token_env": "TOKEN_WARITSUKE",
        # env 未設定の環境では監視をスキップ（設定されたら自動的に監視対象になる）
        "optional": True,
        "fields": {
            # 案件参照（ハブ共通方式）
            "ユニット種別": {
                "type": "DROP_DOWN",
                "required_options": ["時効援用", "相続放棄", "相続一般", "補助金"],
            },
            "案件アプリID": {"type": "SINGLE_LINE_TEXT"},
            "案件レコードID": {"type": "SINGLE_LINE_TEXT"},
            "被相続人名表示用": {"type": "SINGLE_LINE_TEXT"},
            # 割付の実体（財産×相続人の対応・両端は App 35/36 のレコードID参照）
            "財産レコードID": {"type": "SINGLE_LINE_TEXT"},
            "相続人レコードID": {"type": "SINGLE_LINE_TEXT"},
            "取得区分": {
                "type": "DROP_DOWN",
                "required_options": ["単独取得", "共有取得", "換価分割", "代償取得",
                                     "債務引受", "保険金受取（みなし）"],
            },
            "持分": {"type": "SINGLE_LINE_TEXT"},
            "代償金額": {"type": "NUMBER"},
            "条件メモ": {"type": "MULTI_LINE_TEXT"},
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
    "相続一般": {
        # S3（財産目録）時点では docx テンプレート規約のみ。
        # 案件アプリ（App 26 昇格）・channels 等は S1 以降で追加する（souzoku-shorui/05）
        "template_dir": "souzoku",        # docx_templates/souzoku/<種別>.docx（規約配置）
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
    # channels/soufu_annai.py（M4 送付案内）が差し込むキー（事務所正式書式・2026-07-03 差替）
    "docx_templates/jikou/送付案内.docx": [
        "{{日付}}", "{{依頼者住所}}", "{{依頼者氏名}}", "{{事務所署名ブロック}}",
        "{{本文}}", "{{特記事項}}",
        "{{行:No}}", "{{行:書類名}}", "{{行:部数}}", "{{行:備考}}",
    ],
    # units/souzoku/zaisan_mokuroku.py（S3 財産目録）が差し込むキー
    # （標準形テンプレート。オーナー書式への差し替え時もこのキーを残すこと）
    "docx_templates/souzoku/財産目録.docx": [
        "{{被相続人名}}", "{{作成日}}", "{{作成者}}",
        "{{積極財産合計}}", "{{消極財産合計}}", "{{純資産額}}",
        "{{評価基準日}}", "{{出典資料}}",
        "{{行:不動産}}", "{{行:所在}}", "{{行:地番家屋番号}}", "{{行:地目種別}}",
        "{{行:地積床面積}}", "{{行:持分}}",
        "{{行:預貯金}}", "{{行:金融機関}}", "{{行:支店}}", "{{行:種別}}",
        "{{行:口座番号}}", "{{行:死亡日残高}}",
        "{{行:有価証券}}", "{{行:銘柄内容}}", "{{行:数量}}", "{{行:評価額}}",
        "{{行:債務}}", "{{行:内容}}", "{{行:金額}}",
    ],
}


# ══════════════════════════════════════════════════════════════
# 戸籍読解プロンプト（R3・koseki-ocr 01 §2: プロンプトは config のデータとして持つ）
#   キーは様式。"共通" は様式判定込みの単段読解（R3 v1）。様式別の最適化は
#   キー追加で行い、コード変更なしで差し替えられるようにする
# ══════════════════════════════════════════════════════════════

KOSEKI_READER_PROMPTS = {
    "共通": (
        "以下は戸籍謄本（現行戸籍・改製原戸籍・除籍謄本のいずれか）のOCRテキストです。\n"
        "縦書きのレイアウト崩れがあり得るため、記載順に依存せず人物単位で再構成してください。\n"
        "次の規則に従い save_koseki_reading ツールで構造化してください。\n"
        "- 日付（編製日・消除日・生年月日・身分事項の日付）は和暦の原文表記のまま出力する"
        "（例: 昭和32年4月1日）。西暦は hensei_date_seireki・shojo_date_seireki にのみ"
        " YYYY-MM-DD で出力し、変換に自信がなければ null にする\n"
        "- 氏名・本籍等は旧字体・異体字を含め原文どおりに出力する（正規化しない）\n"
        "- 名欄に×や斜線がある者、除籍・死亡等の記載がある者は removed=true とする\n"
        "- 各フィールドの確信度（0〜1）を confidence に出力する。"
        "読み取れない項目は空文字にし、確信度を低くする\n"
        "- 複数の戸籍が合綴されている場合は先頭の戸籍のみを読み取り、form_confidence を下げる\n"
        "\n=== OCRテキスト ===\n{ocr_text}\n=== END ==="
    ),
}


# 書類分割の区間判定（D1-1・document_splitter.py）
DOCUMENT_SPLIT_PROMPTS = {
    "共通": (
        "以下は1つのPDFに含まれる複数ページのOCRテキストです（ページ区切りつき）。\n"
        "このPDFに**何種類の書類が何ページ目から何ページ目まで**入っているかを、\n"
        "save_document_segments ツールで区間判定してください。\n"
        "- 区間は必ず 1ページ目から最終ページまでを連続・重複なしで覆うこと\n"
        "- 書類の切れ目が判別できない場合は無理に分割せず、1つの区間にまとめて\n"
        "  confidence を低くする（誤った切れ目の方が害が大きい）\n"
        "- 各区間の doc_type は選択肢から選ぶ。どれにも明確に該当しなければ「その他」\n"
        "- confidence はその区間の**切れ目と種別の両方**への自信（0〜1）\n"
        "\n{pages_block}"
    ),
}


# 通帳・残高証明読解（S6-1・bank_reader.py）。既存 /scan（通帳）の抽出知見
# （残高=ページ末尾の最新残高・円整数）を継承し tool use 化・複数口座対応
BANK_READER_PROMPTS = {
    "共通": (
        "以下は銀行の残高証明書または通帳（見開き）のOCRテキストです。\n"
        "save_bank_reading ツールで構造化してください。\n"
        "- 書類形態（doc_form）: 残高証明書なら「残高証明」・通帳なら「通帳」・"
        "判別できなければ「不明」\n"
        "- 残高証明書に複数の口座が載る場合は accounts に**すべての口座**を抽出する\n"
        "- 各口座: 金融機関名・支店名・預金種別（普通/定期/当座/貯蓄/その他）・"
        "口座番号・名義人（いずれも原文のまま）\n"
        "- 残高（balance）: 円単位の整数（カンマ・「円」は除去）。通帳は"
        "**ページ末尾の最新残高**。読み取れなければ null\n"
        "- 基準日（basis_date）: 残高証明の証明基準日・通帳は最終記帳日。"
        "和暦の原文表記のまま。西暦は basis_date_seireki にのみ YYYY-MM-DD で出力し、"
        "変換に自信がなければ null にする\n"
        "- 各フィールドの確信度（0〜1）を confidence に出力する。"
        "読み取れない項目は空文字/nullにし、確信度を低くする\n"
        "\n=== OCRテキスト ===\n{ocr_text}\n=== END ==="
    ),
}


# 評価証明読解（S4-M1・valuation_reader.py）。既存 /ocr/fixed-asset の抽出知見
# （評価額=円整数・年度=西暦4桁）を継承し tool use 化・複数物件対応
VALUATION_READER_PROMPTS = {
    "共通": (
        "以下は固定資産評価証明書または課税明細書（納税通知書の明細含む）の"
        "OCRテキストです。\n"
        "save_valuation_reading ツールで構造化してください。\n"
        "- 書類種別（doc_kind）: 評価証明書なら「評価証明」・課税明細書/納税通知書なら"
        "「課税明細」・判別できなければ「不明」\n"
        "- 年度（year）: 西暦4桁の整数（例: 令和6年度→2024、令和7年度→2025）。"
        "不明なら null\n"
        "- 所有者・納税義務者（owner_name）: 原文のまま\n"
        "- 1枚に複数の土地・家屋が載る様式では properties に**すべての物件**を抽出する"
        "（先頭だけにしない）\n"
        "- 各物件: 土地/家屋の別（kind）・所在（原文表記のまま全体・切り詰めない）・"
        "地番（土地）・家屋番号（家屋）・評価額\n"
        "- 評価額（assessed_value）: 円単位の整数（カンマ・「円」は除去）。"
        "課税標準額ではなく**評価額（価格）**の欄を使う。読み取れなければ null\n"
        "- 各フィールドの確信度（0〜1）を confidence に出力する。"
        "読み取れない項目は空文字/nullにし、確信度を低くする\n"
        "\n=== OCRテキスト ===\n{ocr_text}\n=== END ==="
    ),
}


# 登記読解（S5-1・registry_reader.py）。抽出項目・注意事項は参考実装
# ocr_to_claude.py の項目表を継承（方式は tool use 強制に置換）
REGISTRY_READER_PROMPTS = {
    "共通": (
        "以下は不動産登記事項証明書（登記簿謄本）のOCRテキストです。\n"
        "あなたは日本の不動産登記の読解者として、"
        "save_registry_reading ツールで構造化してください。\n"
        "- 表題部（不動産の表示）・権利部甲区（所有権）・権利部乙区（所有権以外の権利）を読む\n"
        "- OCRの誤認識（記号の混入・文字化け）があっても最善の解釈を行う\n"
        "- 複数の不動産が含まれる場合は properties にすべて抽出する\n"
        "- 甲区は**現在有効な最新の所有権登記**のみ。共有なら owners に全員を入れ、"
        "持分（share）は原文のまま（例: 2分の1）。下線等で抹消された過去の所有者は入れない\n"
        "- 乙区は抹消済み（抹消登記・下線）を除いた**有効な権利の有無と内容の要約**のみ\n"
        "- 日付（受付日・原因日付）は和暦の原文表記のまま出力する（例: 平成14年3月1日）。"
        "西暦は receipt_date_seireki・cause_date_seireki にのみ YYYY-MM-DD で出力し、"
        "変換に自信がなければ null にする\n"
        "- 所在・氏名・地積・床面積等は原文どおりに出力する（正規化しない）\n"
        "- 各フィールドの確信度（0〜1）を confidence に出力する。"
        "読み取れない項目は空文字にし、確信度を低くする\n"
        "\n=== OCRテキスト ===\n{ocr_text}\n=== END ==="
    ),
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
        "FAX": os.environ.get("OFFICE_FAX", ""),
        "弁護士名": os.environ.get("OFFICE_ATTORNEY", ""),
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
