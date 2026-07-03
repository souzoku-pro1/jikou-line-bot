"""発送管理（App 30）の状態機械と冪等ガード（hub/approval）

設計: docs/architecture/03-common-components.md §5.1-5.2、01-overview.md §4

██ 絶対制約 ██████████████████████████████████████████████████████
「承認待ち → 承認済」への遷移を行うコードパスをサーバー側に作らない。
承認は弁護士の kintone 操作のみ。SERVER_TRANSITIONS に遷移先が「承認済」の
組が存在しないことを test_hub_approval.py が恒久的に担保する。
████████████████████████████████████████████████████████████████
"""

import logging

from hub import kintone, notify

logger = logging.getLogger("hub.approval")

# App 30 発送ステータスの全状態（02 §2.1）
STATUSES = (
    "下書き", "承認待ち", "承認済", "発送処理中", "発送済",
    "返送待ち", "完了", "エラー", "却下", "要確認",
)

# サーバー（Railway）が実行してよい遷移。これ以外は TransitionError + LINE 警報。
# ※「承認待ち→承認済」「承認待ち→却下」は弁護士の kintone 操作のみ（絶対制約）。
SERVER_TRANSITIONS = frozenset({
    ("下書き", "承認待ち"),      # prepare 成功（成果物添付済み）
    ("下書き", "エラー"),        # prepare 失敗
    ("承認済", "発送処理中"),    # dispatch 開始（claim 通過後）
    ("発送処理中", "発送済"),    # dispatch 成功（自動送信チャネル）
    ("発送処理中", "エラー"),    # dispatch 失敗（リトライ超過含む）
    ("発送済", "返送待ち"),      # 返送想定あり
    ("発送済", "完了"),          # 返送想定なし
    ("返送待ち", "完了"),        # M5 返送消込・送達確認
})

# 人（弁護士・事務員）が kintone 上で行う遷移（ドキュメント用。サーバーは実行しない）
HUMAN_TRANSITIONS = frozenset({
    ("承認待ち", "承認済"),      # 弁護士の承認
    ("承認待ち", "却下"),        # 弁護士の却下
    ("発送処理中", "発送済"),    # 物理郵送チャネル: 事務員の投函・追跡番号入力
    ("エラー", "下書き"), ("エラー", "完了"),
    ("要確認", "下書き"), ("要確認", "完了"),
})


class TransitionError(Exception):
    """状態遷移表にないサーバー遷移が要求された"""


async def transition(app: kintone.KintoneApp, record_id: str,
                     from_status: str, to_status: str,
                     extra_fields: dict | None = None) -> None:
    """発送ステータスを遷移させる（サーバー側で 発送ステータス を書く唯一の経路）。

    遷移表（SERVER_TRANSITIONS）にない組は実行せず、LINE 警報の上 TransitionError。
    """
    if (from_status, to_status) not in SERVER_TRANSITIONS:
        await notify.notify_admin_line(
            "【発送管理: 不正な状態遷移を拒否】\n"
            f"レコードNo: {record_id}\n"
            f"要求された遷移: {from_status} → {to_status}\n"
            "サーバーはこの遷移を実行できません（承認は kintone 上の操作のみ）。",
            throttle_key=f"invalid_transition:{from_status}:{to_status}",
        )
        raise TransitionError(f"forbidden transition: {from_status} -> {to_status}")

    fields = {"発送ステータス": to_status}
    if extra_fields:
        fields.update(extra_fields)
    await kintone.update_record(app, record_id, fields)
    logger.info("transition record=%s %s -> %s", record_id, from_status, to_status)


async def claim_execution(app: kintone.KintoneApp, record: dict) -> bool:
    """冪等ガード（claim パターン・03 §5.2）。

    実行済み=no のレコードに対し revision 指定で 実行済み=yes に更新する。
    - 既に yes（二重 Webhook・再送）→ False
    - revision 競合（他プロセスが先に claim）→ False
    True を返した呼び出し元だけが発送処理を実行してよい。
    """
    if record.get("実行済み", {}).get("value", "") != "no":
        return False
    record_id = str(record["$id"]["value"])
    revision = record.get("$revision", {}).get("value")
    try:
        await kintone.update_record(app, record_id, {"実行済み": "yes"}, revision=revision)
    except kintone.KintoneConflict:
        logger.info("claim conflict record=%s (already claimed by another process)", record_id)
        return False
    return True
