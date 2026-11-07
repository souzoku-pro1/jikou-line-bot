"""trial_account — UX-PENDING-1 (6): 大野の試験用 LINE アカウントを「試験アカウント扱い」にする env allowlist

- env `JIKOU_TEST_USER_HASHES`: カンマ区切りの sha256（hub.send_ledger.line_user_hash("jikou", userId) と同じ値＝
  `sha256("jikou:" + userId)` の hex 64 桁）。値は大野が手元で計算して設定する（LINE userId そのものは置かない）。
- 効果は **受信の経路選択だけ**: App 21 の status がヒアリング中でも顧客対応経路（分類 → guard → 許可表 → apply_v2）へ
  入れる（classify_routing の結果 "hearing" を "pre_engagement" に読み替える・App 21 は読むだけで書かない）。
  人対応ゲート・停止リスト・許可表・台帳・承認は本物のまま通る（試験でも承認なし送信経路は作らない）。
- 空・未設定・形式不正は無効（誰も試験扱いにならない）。
"""

import os
import re

from hub import send_ledger

ENV = "JIKOU_TEST_USER_HASHES"
BUSINESS = "jikou"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def test_user_hashes() -> frozenset:
    raw = os.environ.get(ENV, "")
    return frozenset(h for h in (p.strip().lower() for p in raw.split(",")) if _HASH_RE.fullmatch(h))


def is_test_user(user_id: str) -> bool:
    hashes = test_user_hashes()
    if not hashes or not user_id:
        return False
    return send_ledger.line_user_hash(BUSINESS, str(user_id)) in hashes


def routing_override(user_id: str, routing: str) -> str:
    """試験アカウントの "hearing" だけを "pre_engagement" に読み替える（他は不変）。"""
    if routing == "hearing" and is_test_user(user_id):
        return "pre_engagement"
    return routing
