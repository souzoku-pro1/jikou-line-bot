"""jikou_constants — 時効援用（jikou）側の運用定数（数値は本ファイル 1 か所・QUEUE-2026-10-16-R 共通規律）

**全項目 要大野確認**（PC-A 仮置き）。文言は hub/jikou_reply_texts.py、金額は chat_responder.FEE_GUIDE_TEXT（凍結）が正。
本ファイルは日数・間隔・上限だけを置く。値を変えるときはここだけを変える（参照側に数値を直書きしない）。
"""

# JIKOU-FOLLOWUP-1 (1): 発送日からの経過日数で業務 LINE に未記録の案件を通知する節目（要大野確認）
FOLLOWUP_DAYS = (14, 28)

# JIKOU-PROGRESS-1: 進捗連絡の下書きを積む対象期間（この日数より前のイベントは拾わない＝配備時の過去分一括起票を防ぐ・要大野確認）
PROGRESS_LOOKBACK_DAYS = 14

# JIKOU-PROGRESS-1: イベント走査（入金確認・発送完了）の間隔（分・要大野確認）
PROGRESS_SCAN_INTERVAL_MINUTES = 30
