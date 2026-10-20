"""souzoku_constants — 相続一般（souzoku）側の運用定数（数値・閉集合は本ファイル 1 か所・QUEUE-2026-10-16-R 共通規律）

**全項目 要大野確認**（PC-A 仮置き）。文言は hub/souzoku_intake_texts.py が正。
正本: docs/souzoku-1-design.md §1（第 1 段階・SOUZOKU-2）。
"""

# 受付フォーム（SOUZOKU-2）の入力上限
INTAKE_MAX_NAME_CHARS = 60            # 被相続人の氏名・ふりがな
INTAKE_MAX_HEIRS_PER_RELATION = 20    # 続柄ごとの人数の上限（これを超える入力は不正として聞き直し）
INTAKE_MAX_BODY_BYTES = 16 * 1024     # POST 本文の上限（本文を読む前に Content-Length で判定）
INTAKE_RATE_LIMIT = 10                # 同一送信元からの回数／窓
INTAKE_RATE_WINDOW_SECONDS = 600
INTAKE_RATE_MAX_BUCKETS = 5000

# 質問票の閉集合（docs/souzoku-1-design.md §1）
HEIR_RELATIONS = ("配偶者", "子", "父母", "兄弟姉妹", "その他")
WILL_OPTIONS = ("あり", "なし", "不明")
PROPERTY_TYPES = ("不動産", "預貯金", "有価証券", "その他")
DISPUTE_OPTIONS = ("ない", "ある", "わからない")
DISPUTE_OUT_OF_SCOPE = "ある"         # 「当事務所では扱っていない」で終了する値
