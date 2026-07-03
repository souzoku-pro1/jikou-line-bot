"""kintone Webhook 受信の共通型

設計: docs/architecture/03-common-components.md §4
App 29 承認 Webhook・document_webhook・cloudsign_webhook で確立した
「合言葉 → recordId 抽出 → 最新レコード再取得で再判定」パターンの関数化。
"""

import hmac
import os

from hub import kintone


def verify_token(supplied: str, expected_env: str) -> bool:
    """合言葉トークンの検証。環境変数が未設定なら常に False（deny-all）。
    不一致時に 404 / 403 のどちらを返すかは呼び出し元の責務（既存挙動を維持）。"""
    expected = os.environ.get(expected_env, "")
    if not expected:
        return False
    return hmac.compare_digest(supplied or "", expected)


def extract_record_id(body) -> str | None:
    """kintone Webhook ボディからレコード ID を取り出す。
    body["record"]["$id"]["value"] → 無ければ body["recordId"]（既存2実装と同一）。"""
    try:
        record_id = body["record"]["$id"]["value"]
    except (KeyError, TypeError):
        record_id = body.get("recordId") if isinstance(body, dict) else None
    return str(record_id) if record_id else None


async def refetch_and_check(app: kintone.KintoneApp, record_id: str,
                            expects: dict[str, str]) -> dict | None:
    """最新レコードを取得し、expects（例 {"発送ステータス": "承認済", "実行済み": "no"}）
    を満たさなければ None（= skip）。満たせばレコードを返す。
    取得失敗（KintoneError）も None（呼び出し元は skip 扱い）。"""
    try:
        record = await kintone.get_record(app, record_id)
    except kintone.KintoneError:
        return None
    for code, expected_value in expects.items():
        if record.get(code, {}).get("value", "") != expected_value:
            return None
    return record
