"""JIKOU-SEND-PURPOSE-2: 送信台帳 purpose の正式移行（CHECK の集合拡張＋既存行の purpose 書換・可逆）

正本: docs/send_ledger_purpose_migration.md（QUEUE-2026-10-13-E E-2）。作成のみ・本番適用は大野（要 Postgres-ak5B 実走）。
- upgrade: ck_send_operation_purpose を 旧 8 件＋新 13 件 に広げ、purpose=other かつ inbound_event_id が既知接頭辞
  `{種別}:` の行の purpose を種別名へ書き換える（inbound_event_id は不変）。
- downgrade: 逆変換（種別名 → other）してから CHECK を旧集合へ戻す（行は削除しない・RV-08）。
- 方言: Postgres は ALTER TABLE DROP/ADD CONSTRAINT、sqlite は batch モード（表の作り直し・既存 CHECK を table_args で明示）。
- 表定義は hub/send_ledger.py の metadata と対応（PURPOSE_KINDS と本 file の KINDS はテストで同一性を pin）。

Revision ID: f8b0c2d4e6a8
Revises: e6a8c0d2f4b6
Create Date: 2026-10-13
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'f8b0c2d4e6a8'
down_revision: Union[str, Sequence[str], None] = 'e6a8c0d2f4b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "send_operation"
CK_NAME = "ck_send_operation_purpose"
LEGACY_PURPOSES = ('reply', 'first_reply', 'urgent', 'image_receipt', 'image_result',
                   'follow', 'receipt_number', 'other')
# 種別（inbound_event_id の接頭辞） → 新 purpose 名（hub/send_ledger.PURPOSE_KINDS と同一）
KINDS = {
    "pending_notice": "pending_notice",
    "draft_notice": "draft_notice",
    "approval_blocked": "approval_blocked",
    "exemplar_promotion": "exemplar_promotion",
    "j5_notice": "j5_notice",
    "jikou_metrics_weekly": "jikou_metrics_weekly",
    "houki_jukuryo_14": "houki_jukuryo_14",
    "houki_jukuryo_7": "houki_jukuryo_7",
    "houki_jukuryo_unset": "houki_jukuryo_unset",
    "houki_overdue": "houki_overdue",
    "houki_reply": "houki_reply",
    "fax": "fax",
    "send_ledger_down": "send_ledger_down",
}
V2_PURPOSES = LEGACY_PURPOSES + tuple(KINDS.values())


def _in_list(values) -> str:
    return ", ".join("'" + v + "'" for v in values)


def _legacy_expr() -> str:
    return f"purpose IN ({_in_list(LEGACY_PURPOSES)})"


def _v2_expr() -> str:
    return f"purpose IN ({_in_list(V2_PURPOSES)})"


def _replace_check(old_expr: str, new_expr: str) -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(sa.text(f"ALTER TABLE {TABLE} DROP CONSTRAINT {CK_NAME}"))
        op.execute(sa.text(f"ALTER TABLE {TABLE} ADD CONSTRAINT {CK_NAME} CHECK ({new_expr})"))
        return
    # sqlite: batch（表の作り直し＝move and copy）。表定義は hub/send_ledger.metadata を copy_from に渡す（反映不要・--sql でも可）。
    # copy_from の CHECK（コード側の凍結集合）を名前で落として new_expr を付け直す（旧集合→新集合・新集合→旧集合の両方向で同じ手順）
    from hub.send_ledger import metadata as send_ledger_metadata
    with op.batch_alter_table(TABLE, copy_from=send_ledger_metadata.tables[TABLE]) as b:
        b.drop_constraint(CK_NAME, type_="check")
        b.create_check_constraint(CK_NAME, new_expr)


def _rewrite(from_purpose_of_kind, to_purpose_of_kind) -> None:
    for kind, purpose in KINDS.items():
        to_p, from_p = to_purpose_of_kind(kind, purpose), from_purpose_of_kind(kind, purpose)
        assert all(ch.isalnum() or ch == "_" for ch in to_p + from_p + kind)       # 定数のみ（リテラル SQL・--sql モードでも出せる）
        op.execute(sa.text(f"UPDATE {TABLE} SET purpose = '{to_p}' WHERE purpose = '{from_p}' AND inbound_event_id LIKE '{kind}:%'"))


def upgrade() -> None:
    _replace_check(_legacy_expr(), _v2_expr())                       # 先に集合を広げる
    _rewrite(lambda k, p: "other", lambda k, p: p)                   # other → 種別名（inbound_event_id は不変）


def downgrade() -> None:
    _rewrite(lambda k, p: p, lambda k, p: "other")                   # 種別名 → other（行は消さない）
    n = op.get_bind().execute(sa.text(
        f"SELECT COUNT(*) FROM {TABLE} WHERE purpose NOT IN ({_in_list(LEGACY_PURPOSES)})")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("send_operation_purpose_downgrade_refused_unknown_rows_present")
    _replace_check(_v2_expr(), _legacy_expr())
