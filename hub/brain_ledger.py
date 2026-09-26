"""brain_ledger — BRAIN-A1-LEDGER-1: 案件脳の台帳（Postgres・独立 metadata）

正本: Desktop\\claude\\案件脳_設計_v3.md §4-1〜4-6（＋司令塔裁定 v3.1 R1〜R7・
v3.2 R8〜R10）。kintone が正本・台帳は索引と派生（§1）。本 module は kintone を
import せず、DB 以外へ何も出さない（logging 非 import＝PII の反射経路を構造的に
持たない）。

表（§4）:
- case_fact          事実（出典・版・locator・変換器・確からしさ・supersedes）
- case_confirmation  確認（fact の版に固定・操作 ID 冪等・撤回は履歴）
- case_event         出来事（冪等キー一意・R9: App 28 の会話はここ）
- case_derivation    派生（A1 では空でよい）
- case_usage         利用記録（A1 では空でよい）
- source_ingest      取込の冪等キーと状態・案件紐付けの現在値・latest_seen_revision（R8）
- link_history       出典→案件キーの紐付け履歴（候補一覧を含む）
- sync_run / sync_cursor  同期の実行と確認済み範囲（kind=sync/recheck・BA-05/09）

制約（§4-1・票の指定）:
- fact 一意キー = (案件キー, subject_id, 項目コード, 出典アプリ, 出典レコード,
  revision, locator, 変換器名, 変換器版)
- 取込の冪等キー（source_ingest）= (出典アプリ, 出典レコード, revision, locator,
  変換器名, 変換器版)
- locator は NOT NULL（不明部分は "-"）・空文字禁止
- supersedes は同表参照・自己参照禁止（CHECK）・分岐禁止（UNIQUE）・循環は
  アプリ側検査（_assert_no_cycle）
- case_event.idem_key 一意・case_confirmation.operation_id 一意

subject_id の規則（§4-1・R1/R2/R9）: 案件キーの内側でのみ意味を持つ固定 ID。
  case / applicant（申述人・案件キー由来の固定 ID）/ decedent（被相続人）/
  creditor:{行ID} / document:{行ID}（サブテーブル行 ID・順番では識別しない）/
  shipping:{App 30 レコード番号}（R9: 発送ごとに別 subject）。

現在値の規則（R8）: 出典系列（出典アプリ・レコード・locator・変換器）ごとに
  latest_seen_revision を source_ingest に持ち、現在値 = 系列内で revision 最大の
  fact。同値の新版は fact を増やさず latest_seen_revision だけ進める。
  latest_seen_revision より小さい revision の入力は保存するが現在値に昇格させない
  （invalid_reason=stale_revision）。

関連喪失の共通処理（R10・_detach_source_tx）: 自動採用可の関連が失われた／移動した
  ときは link_history に理由付きで記録 → 旧案件の当該出典由来 fact/event を
  is_current=False（link_lost / link_moved）→ 依存する確認は再確認一覧に出る →
  移動先が自動採用可なら新案件へ積む。通常同期・追跡再照合・手動訂正が同じ関数を
  通る（削除しない）。
"""

import datetime
import hashlib
import json

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from hub.db import session_scope

metadata = sa.MetaData()

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

# ── 閉集合の語彙（テストで pin） ─────────────────────────────────────────────
CONFIDENCE_VALUES = ("high", "medium", "low")
DECISION_VALUES = ("confirm", "reject", "revoke")
INGEST_STATES = ("ingested", "mismatch_hold", "unavailable", "held", "detached")
TRUST_LEVELS = ("auto", "candidate", "hold")
RUN_STATES = ("running", "ok", "failed", "stopped", "partial")   # partial: 未解決あり（R12/BA-17）
CURSOR_STATES = ("synced", "incomplete", "error", "stopped")
CURSOR_KIND_SYNC = "sync"
CURSOR_KIND_RECHECK = "recheck"
CURSOR_KINDS = (CURSOR_KIND_SYNC, CURSOR_KIND_RECHECK)
LOCATOR_UNKNOWN = "-"
LOCATOR_PARTS = 5          # 欄コード/行ID/添付識別子/添付の版/資料内位置
SOURCE_KIND_KINTONE = "kintone"
INVALID_LINK_MOVED = "link_moved"
INVALID_LINK_LOST = "link_lost"
INVALID_SUPERSEDED = "superseded"
INVALID_ROW_REMOVED = "row_removed"
INVALID_STALE = "stale_revision"
# 関連喪失で「現在値でなくなった」理由（この理由で外れた fact は復帰時の supersedes 元）
RETIRED_REASONS = (INVALID_LINK_MOVED, INVALID_LINK_LOST, INVALID_ROW_REMOVED)
HOLD_REASONS = ("ref_missing", "ref_not_digits", "unit_mismatch",
                "ref_other_app", "multiple_match", "conflict", "mismatch")
# R5 の不成立（保留にもしない＝detached）。brain_link がこの語彙を使う
DETACH_REASONS = ("category_out_of_set", "line_id_not_unique")
FLAG_SOURCE_UNAVAILABLE = "source_unavailable"
FLAG_PENDING_RECHECK = "pending_recheck"       # R12: 判定不能（正本の検索/実在確認の失敗）
REASON_RELINK_PENDING = "relink_pending"       # BA-11: 訂正先へ積み直せていない
FRESHNESS_STATES = ("synced", "partial", "incomplete", "error", "stopped")
# 確認/却下を拒む理由（409 に添える固定語彙・BA-13）
CONFLICT_VERSION = "version_mismatch"
CONFLICT_NOT_CURRENT = "fact_not_current"
CONFLICT_SOURCE_DETACHED = "source_detached"
STALE_STATE = "stale"                          # R11: 遅着旧版は履歴として保存するだけ
# R15/BA-25: 紐付け訂正の対象閉集合（App 28 チャットログ・App 30 発送管理）。API・台帳関数が参照
RELINKABLE_APPS = frozenset({"28", "30"})

SUBJECT_CASE = "case"
SUBJECT_APPLICANT = "applicant"
SUBJECT_DECEDENT = "decedent"
SUBJECT_CREDITOR_PREFIX = "creditor:"
SUBJECT_DOCUMENT_PREFIX = "document:"
SUBJECT_SHIPPING_PREFIX = "shipping:"

# ── 項目コードの閉集合（前提確認 §2 の表 + R2/R4。値は型） ──────────────────
# value_type: text / choice / multi / date / datetime / number / file
_APP40_FIELDS = {
    # (A) 案件の識別・状態
    "レコード番号": "text", "status": "choice", "response_mode": "choice",
    "受付チャネル": "choice", "相談経路": "choice", "相談経路その他": "text",
    "相談カード読取": "choice", "受任判断": "choice", "来所日": "date",
    "作成日時": "datetime", "更新日時": "datetime",
    # (B) 申述人
    "顧客名": "text", "furigana": "text", "住所": "text", "郵便番号": "text",
    "電話番号": "text", "メールアドレス": "text", "生年月日": "text",
    "LINEユーザーID": "text", "本人区分": "choice", "続柄": "choice",
    "続柄その他": "text", "相続順位": "choice", "本人確認ステータス": "choice",
    "本人確認書類": "file", "未成年後見関与": "choice",
    # (C) 被相続人
    "被相続人氏名": "text", "被相続人ふりがな": "text", "被相続人生年月日": "text",
    "被相続人本籍": "text", "被相続人最後の住所": "text", "死亡日": "date",
    "死亡日_申告": "date", "被相続人グループID": "text",
    # (D) 熟慮期間・期日
    "死亡を知った日_申告": "date", "相続人と知った日_申告": "date",
    "相続の開始を知った日": "date", "知った日の区分": "choice",
    "知った日の区分その他": "text", "知った経緯": "text", "起算日_確定": "date",
    "起算点確定済": "choice", "起算点メモ": "text", "法定満了日": "date",
    "社内締切日": "date", "熟慮期間期限": "date", "伸長後満了日": "date",
    "期間伸長申立": "choice", "日付申告メモ": "text", "提出目標日": "date",
    # (E) 電話トリアージ
    "電話要否": "choice", "電話推奨度": "choice", "電話推奨根拠": "text",
    "危険類型フラグ": "multi", "電話予定日時": "datetime", "電話実施記録": "text",
    # (F) 契約・決済
    "契約書ステータス": "choice", "契約署名": "choice", "契約書回収メモ": "text",
    "入金状況": "choice", "特約": "text", "委任契約書": "file", "委任状": "file",
    # (G) 申述手続
    "管轄家庭裁判所": "text", "事件番号": "text", "申述提出日": "date",
    "受理日": "date", "受理通知受領日": "date", "照会書受領日": "date",
    "放棄の理由": "choice", "放棄の理由その他": "text", "同時申述希望": "choice",
    "申述書": "file", "事情説明書": "file", "代理人目録": "file",
    "受理通知書": "file", "相談カード": "file", "受信書類写真": "file",
    # (H) 財産・相続人状況（自由記述は R2: subject=case の文字列事実）
    "財産_不動産": "text", "財産_有価証券": "text", "財産_現金預貯金": "text",
    "財産_負債": "text", "財産処分有無": "choice", "訴訟督促有無": "choice",
    "単純承認事由フラグ": "choice", "単純承認メモ": "text", "他の相続人": "text",
    "先順位相続人の状況": "text", "先順位者の放棄状況": "text",
    "連続戸籍充足": "choice",
}
_APP40_APPLICANT_FIELDS = frozenset({
    "顧客名", "furigana", "住所", "郵便番号", "電話番号", "メールアドレス",
    "生年月日", "LINEユーザーID", "本人区分", "続柄", "続柄その他", "相続順位",
    "本人確認ステータス", "本人確認書類", "未成年後見関与"})
_APP40_DECEDENT_FIELDS = frozenset({
    "被相続人氏名", "被相続人ふりがな", "被相続人生年月日", "被相続人本籍",
    "被相続人最後の住所", "死亡日", "死亡日_申告", "被相続人グループID"})
_APP40_FREETEXT_FIELDS = frozenset({
    "相談経路その他", "続柄その他", "知った日の区分その他", "知った経緯",
    "起算点メモ", "日付申告メモ", "電話推奨根拠", "電話実施記録",
    "契約書回収メモ", "特約", "放棄の理由その他", "財産_不動産", "財産_有価証券",
    "財産_現金預貯金", "財産_負債", "単純承認メモ", "他の相続人",
    "先順位相続人の状況", "先順位者の放棄状況"})
APP40_CREDITOR_TABLE = "債権者一覧"
APP40_CREDITOR_COLUMNS = {
    "債権者名": "text", "債権者郵便番号": "text", "債権者住所": "text",
    "通知要否": "choice", "送付状態": "choice", "送付発送管理No": "text",
    "追加料金対象": "choice"}
APP40_DOCUMENT_TABLE = "書類チェック"
APP40_DOCUMENT_COLUMNS = {
    "書類名": "choice", "対象者": "text", "取得方法": "choice", "書類状態": "choice",
    "同封対象": "multi", "発送管理No": "text"}
_APP30_FIELDS = {
    "レコード番号": "text", "案件アプリID": "text", "案件レコードID": "text",
    "ユニット種別": "choice", "チャネル": "choice", "方向": "choice",
    "発送ステータス": "choice", "実行済み": "choice", "件名": "text",
    "宛先名": "text", "宛先郵便番号": "text", "宛先住所": "text",
    "同封物選択": "multi", "発送日時": "datetime", "追跡番号": "text",
    "送達結果": "choice", "返送期限": "date", "承認者コメント": "text",
    "却下理由": "text", "本文_特記事項": "text", "チャネル固有データ": "text",
    "成果物": "file", "受領ファイル": "file", "作成日時": "datetime",
    "更新日時": "datetime",
}
# R9: App 28 の行は case_event（会話）として登録する。項目コードは閉集合の記録
# として残す（fact としては積まない）
_APP28_FIELDS = {"role": "choice", "message": "text", "category": "text",
                 "auto_sent": "choice", "line_user_id": "text",
                 "作成日時": "datetime"}

APP40_PREFIX = "app40."
APP30_PREFIX = "app30."
APP28_PREFIX = "app28."


def _codes() -> dict:
    out = {}
    for f, t in _APP40_FIELDS.items():
        out[APP40_PREFIX + f] = t
    for c, t in APP40_CREDITOR_COLUMNS.items():
        out[f"{APP40_PREFIX}{APP40_CREDITOR_TABLE}.{c}"] = t
    for c, t in APP40_DOCUMENT_COLUMNS.items():
        out[f"{APP40_PREFIX}{APP40_DOCUMENT_TABLE}.{c}"] = t
    for f, t in _APP30_FIELDS.items():
        out[APP30_PREFIX + f] = t
    for f, t in _APP28_FIELDS.items():
        out[APP28_PREFIX + f] = t
    return out


ITEM_CODES = _codes()                    # 項目コード → value_type（閉集合）
ITEM_CODE_SET = frozenset(ITEM_CODES)


def app40_field_subject(field: str) -> str:
    if field in _APP40_FREETEXT_FIELDS:
        return SUBJECT_CASE                  # R2: 自由記述は subject=案件の文字列事実
    if field in _APP40_APPLICANT_FIELDS:
        return SUBJECT_APPLICANT
    if field in _APP40_DECEDENT_FIELDS:
        return SUBJECT_DECEDENT
    return SUBJECT_CASE


def app40_field_confidence(field: str) -> str:
    return "medium" if field in _APP40_FREETEXT_FIELDS else "high"


def app40_fields() -> dict:
    return dict(_APP40_FIELDS)


def app30_fields() -> dict:
    return dict(_APP30_FIELDS)


def app28_fields() -> dict:
    return dict(_APP28_FIELDS)


# ── 表 ───────────────────────────────────────────────────────────────────────
case_fact = sa.Table(
    "case_fact", metadata,
    sa.Column("fact_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("case_app_id", sa.Text, nullable=False),
    sa.Column("case_record_id", sa.Text, nullable=False),
    sa.Column("subject_id", sa.Text, nullable=False),
    sa.Column("item_code", sa.Text, nullable=False),
    sa.Column("value_type", sa.Text, nullable=False),
    sa.Column("value_text", sa.Text, nullable=False, server_default=""),
    sa.Column("value_json", _JSON, nullable=True),
    sa.Column("source_kind", sa.Text, nullable=False),
    sa.Column("source_app_id", sa.Text, nullable=False),
    sa.Column("source_record_id", sa.Text, nullable=False),
    sa.Column("source_revision", _BIG, nullable=False),
    sa.Column("locator", sa.Text, nullable=False, server_default=LOCATOR_UNKNOWN),
    sa.Column("converter_name", sa.Text, nullable=False),
    sa.Column("converter_version", sa.Text, nullable=False),
    sa.Column("observation_id", sa.Text, nullable=False),
    sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("confidence", sa.Text, nullable=False),
    sa.Column("is_current", sa.Boolean, nullable=False, server_default=sa.true()),
    sa.Column("supersedes_fact_id", _BIG, sa.ForeignKey("case_fact.fact_id"),
              nullable=True, unique=True),
    sa.Column("invalid_reason", sa.Text, nullable=True),
    sa.Column("invalidated_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("prev_case_app_id", sa.Text, nullable=True),
    sa.Column("prev_case_record_id", sa.Text, nullable=True),
    sa.UniqueConstraint("case_app_id", "case_record_id", "subject_id", "item_code",
                        "source_app_id", "source_record_id", "source_revision",
                        "locator", "converter_name", "converter_version",
                        name="uq_case_fact_key"),
    sa.CheckConstraint("supersedes_fact_id IS NULL OR supersedes_fact_id <> fact_id",
                       name="ck_case_fact_no_self_supersede"),
    sa.CheckConstraint("confidence IN ('high', 'medium', 'low')",
                       name="ck_case_fact_confidence"),
    sa.CheckConstraint("locator <> ''", name="ck_case_fact_locator_nonempty"),
    sa.Index("ix_case_fact_case", "case_app_id", "case_record_id"),
    sa.Index("ix_case_fact_source", "source_app_id", "source_record_id"),
    sa.Index("ix_case_fact_item_value", "item_code", "value_text"),
)

case_confirmation = sa.Table(
    "case_confirmation", metadata,
    sa.Column("confirmation_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("fact_id", _BIG, sa.ForeignKey("case_fact.fact_id"), nullable=False),
    sa.Column("fact_version", _BIG, nullable=False),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("decision", sa.Text, nullable=False),
    sa.Column("reason", sa.Text, nullable=False, server_default=""),
    sa.Column("revoked_of", _BIG,
              sa.ForeignKey("case_confirmation.confirmation_id"), nullable=True),
    sa.Column("operation_id", sa.Text, nullable=False, unique=True),
    sa.Column("seen_version", _BIG, nullable=False),
    sa.CheckConstraint("decision IN ('confirm', 'reject', 'revoke')",
                       name="ck_case_confirmation_decision"),
    sa.Index("ix_case_confirmation_fact", "fact_id"),
)

case_event = sa.Table(
    "case_event", metadata,
    sa.Column("event_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("idem_key", sa.Text, nullable=False, unique=True),
    sa.Column("case_app_id", sa.Text, nullable=False),
    sa.Column("case_record_id", sa.Text, nullable=False),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("source_app_id", sa.Text, nullable=False),
    sa.Column("source_record_id", sa.Text, nullable=False),
    sa.Column("source_revision", _BIG, nullable=False),
    sa.Column("locator", sa.Text, nullable=False, server_default=LOCATOR_UNKNOWN),
    sa.Column("summary", sa.Text, nullable=False),
    sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    # R10: 関連喪失で旧案件の現在ビューから外す（削除しない）
    sa.Column("is_current", sa.Boolean, nullable=False, server_default=sa.true()),
    sa.Column("invalid_reason", sa.Text, nullable=True),
    sa.Column("invalidated_at", sa.DateTime(timezone=True), nullable=True),
    sa.Index("ix_case_event_case", "case_app_id", "case_record_id"),
    sa.Index("ix_case_event_source", "source_app_id", "source_record_id"),
)

case_derivation = sa.Table(
    "case_derivation", metadata,
    sa.Column("derivation_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("case_app_id", sa.Text, nullable=False),
    sa.Column("case_record_id", sa.Text, nullable=False),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("input_fact_versions", _JSON, nullable=False),
    sa.Column("calculator_name", sa.Text, nullable=False),
    sa.Column("calculator_version", sa.Text, nullable=False),
    sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("result", _JSON, nullable=True),
    sa.Column("needs_recalc", sa.Boolean, nullable=False, server_default=sa.false()),
)

case_usage = sa.Table(
    "case_usage", metadata,
    sa.Column("usage_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("usage_kind", sa.Text, nullable=False),
    sa.Column("usage_ref", sa.Text, nullable=False),
    sa.Column("fact_id", _BIG, sa.ForeignKey("case_fact.fact_id"), nullable=False),
    sa.Column("fact_version", _BIG, nullable=False),
    sa.Column("used_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
)

source_ingest = sa.Table(
    "source_ingest", metadata,
    sa.Column("ingest_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("source_kind", sa.Text, nullable=False),
    sa.Column("source_app_id", sa.Text, nullable=False),
    sa.Column("source_record_id", sa.Text, nullable=False),
    sa.Column("source_revision", _BIG, nullable=False),
    sa.Column("locator", sa.Text, nullable=False, server_default=LOCATOR_UNKNOWN),
    sa.Column("converter_name", sa.Text, nullable=False),
    sa.Column("converter_version", sa.Text, nullable=False),
    sa.Column("state", sa.Text, nullable=False),
    sa.Column("case_app_id", sa.Text, nullable=True),
    sa.Column("case_record_id", sa.Text, nullable=True),
    sa.Column("hold_reason", sa.Text, nullable=True),
    sa.Column("source_updated_at", sa.Text, nullable=True),
    # R8: 出典系列（アプリ・レコード・locator・変換器）で見た最大 revision
    sa.Column("latest_seen_revision", _BIG, nullable=True),
    # R12: 判定不能（正本の検索失敗・実在確認失敗）＝次回の再照合対象・鮮度は partial
    sa.Column("pending_recheck", sa.Boolean, nullable=False, server_default=sa.false()),
    sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.UniqueConstraint("source_app_id", "source_record_id", "source_revision",
                        "locator", "converter_name", "converter_version",
                        name="uq_source_ingest_key"),
    sa.CheckConstraint(
        "state IN ('ingested', 'mismatch_hold', 'unavailable', 'held', 'detached')",
        name="ck_source_ingest_state"),
    sa.CheckConstraint("locator <> ''", name="ck_source_ingest_locator_nonempty"),
    sa.Index("ix_source_ingest_source", "source_app_id", "source_record_id"),
    sa.Index("ix_source_ingest_case", "case_app_id", "case_record_id"),
)

link_history = sa.Table(
    "link_history", metadata,
    sa.Column("link_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("source_app_id", sa.Text, nullable=False),
    sa.Column("source_record_id", sa.Text, nullable=False),
    sa.Column("prev_case_app_id", sa.Text, nullable=True),
    sa.Column("prev_case_record_id", sa.Text, nullable=True),
    sa.Column("new_case_app_id", sa.Text, nullable=True),
    sa.Column("new_case_record_id", sa.Text, nullable=True),
    sa.Column("trust_level", sa.Text, nullable=False),
    sa.Column("reason", sa.Text, nullable=False),
    sa.Column("candidates", _JSON, nullable=True),
    sa.Column("operation_id", sa.Text, nullable=True, unique=True),
    sa.Column("actor", sa.Text, nullable=False, server_default="system"),
    sa.Column("source_revision", _BIG, nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.CheckConstraint("trust_level IN ('auto', 'candidate', 'hold')",
                       name="ck_link_history_trust"),
    sa.Index("ix_link_history_source", "source_app_id", "source_record_id"),
)

sync_run = sa.Table(
    "sync_run", metadata,
    sa.Column("run_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("target_app", sa.Text, nullable=False),
    sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("scan_upper_bound", sa.Text, nullable=True),
    sa.Column("page_order", sa.Text, nullable=False),
    sa.Column("incomplete_page", _JSON, nullable=True),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("failure", sa.Text, nullable=True),
    sa.Column("pages_done", sa.Integer, nullable=False, server_default="0"),
    sa.Column("records_seen", sa.Integer, nullable=False, server_default="0"),
    sa.Column("confirmed_range", _JSON, nullable=True),
    # BA-17: 初見の出典で判定不能になった件数（出典行が無いため run に永続化）
    sa.Column("pending_unregistered", sa.Integer, nullable=False, server_default="0"),
    # BA-20: 最初の未解決位置（更新日時・$id）。ページ確定ごとに永続化
    sa.Column("first_unresolved_position", _JSON, nullable=True),
    sa.CheckConstraint("status IN ('running', 'ok', 'failed', 'stopped', 'partial')",
                       name="ck_sync_run_status"),
)

sync_cursor = sa.Table(
    "sync_cursor", metadata,
    sa.Column("target_app", sa.Text, primary_key=True),
    # kind=sync: 窓走査のカーソル / kind=recheck: 追跡再照合の継続位置（BA-05）
    sa.Column("kind", sa.Text, primary_key=True, server_default=CURSOR_KIND_SYNC),
    sa.Column("cursor_updated_at", sa.Text, nullable=True),
    sa.Column("cursor_record_id", sa.Text, nullable=True),
    sa.Column("confirmed_until", sa.Text, nullable=True),
    # BA-09: 未完了走査の上限と再開位置（走査完了で page_position=NULL）
    sa.Column("scan_upper_bound", sa.Text, nullable=True),
    sa.Column("page_position", _JSON, nullable=True),
    sa.Column("last_run_id", _BIG, nullable=True),
    sa.Column("last_ok_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_reconcile_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_recheck_at", sa.DateTime(timezone=True), nullable=True),
    # BA-05: 追跡再照合の一巡（開始・完了）。「全件照合済み」は完了時刻でのみ判定
    sa.Column("recheck_started_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("recheck_completed_at", sa.DateTime(timezone=True), nullable=True),
    # BA-17: 直近の走査で未登録のまま判定不能だった件数（0 になるまで synced にしない）
    sa.Column("pending_unregistered", sa.Integer, nullable=False, server_default="0"),
    # BA-20: 最初の未解決位置。確認済み範囲（再開位置）はこれを越えて進めない
    sa.Column("first_unresolved_position", _JSON, nullable=True),
    sa.Column("state", sa.Text, nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("state IN ('synced', 'incomplete', 'error', 'stopped')",
                       name="ck_sync_cursor_state"),
    sa.CheckConstraint("kind IN ('sync', 'recheck')", name="ck_sync_cursor_kind"),
)

TABLE_NAMES = ("case_fact", "case_confirmation", "case_event", "case_derivation",
               "case_usage", "source_ingest", "link_history", "sync_run",
               "sync_cursor")


# ── 値・locator・観測 ID ──────────────────────────────────────────────────────

class LedgerError(Exception):
    """台帳の規則違反（閉集合外・循環・版不一致など）。本文に PII を含めない。"""


class MismatchError(LedgerError):
    """同じ一意キーで値が違う（抽出結果の不一致）。"""


class VersionConflict(LedgerError):
    """操作時に画面が見ていた版と現在の版が違う／対象が生きていない（reason 固定語彙）。"""

    def __init__(self, current: dict, reason: str = CONFLICT_VERSION):
        super().__init__("version_conflict")
        self.current = current
        self.reason = reason


class SourceUnavailable(LedgerError):
    """出典確認不能の fact は確認・採用の対象にしない（BA-07）。"""


class NotRelinkable(LedgerError):
    """R15: 紐付け訂正の対象は App 28・App 30 のみ。案件本体（案件アプリの出典）は拒否。"""

    def __init__(self):
        super().__init__("app_not_relinkable")
        self.reason = "app_not_relinkable"


def make_locator(field: str = LOCATOR_UNKNOWN, row_id: str = LOCATOR_UNKNOWN,
                 attachment: str = LOCATOR_UNKNOWN,
                 attachment_version: str = LOCATOR_UNKNOWN,
                 position: str = LOCATOR_UNKNOWN) -> str:
    """locator の正規化（固定順 5 部・不明は "-"・NULL/空は作らない）。"""
    parts = [field, row_id, attachment, attachment_version, position]
    norm = []
    for p in parts:
        s = str(p or "").strip().replace("/", "_")
        norm.append(s or LOCATOR_UNKNOWN)
    return "/".join(norm)


def observation_id(source_app_id: str, source_record_id: str, locator: str,
                   converter_name: str, converter_version: str) -> str:
    raw = "|".join([source_app_id, source_record_id, locator, converter_name,
                    converter_version])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def canonical_value(value_type: str, value) -> tuple:
    """(value_text, value_json) の正規形。比較は value_text で行う。"""
    if value_type in ("multi", "file"):
        if value_type == "multi":
            items = sorted(str(v) for v in (value or []))
            return "|".join(items), items
        files = []
        for f in (value or []):
            if isinstance(f, dict):
                files.append({"fileKey": str(f.get("fileKey") or ""),
                              "name": str(f.get("name") or ""),
                              "size": str(f.get("size") or ""),
                              "contentType": str(f.get("contentType") or "")})
        return "|".join(x["fileKey"] for x in files), files
    if value is None:
        return "", None
    return str(value).strip(), None


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _iso(dt) -> str:
    return dt.isoformat() if dt else ""


def _int_id(col):
    """Text のレコード番号を数値順で扱う（$id は数字のみ・_DIGITS_RE 済み）。"""
    return sa.cast(col, sa.BigInteger)


# ── 取込（§4-1・§4-6） ──────────────────────────────────────────────────────

class FactIn:
    """変換器が出す 1 事実（型付き値・出典位置・確からしさ）。"""

    __slots__ = ("subject_id", "item_code", "value_type", "value", "locator",
                 "confidence", "occurred_at")

    def __init__(self, subject_id: str, item_code: str, value_type: str, value,
                 locator: str, confidence: str = "high", occurred_at=None):
        if item_code not in ITEM_CODE_SET:
            raise LedgerError("item_code_not_in_closed_set")
        if confidence not in CONFIDENCE_VALUES:
            raise LedgerError("confidence_not_in_closed_set")
        if not locator or locator.count("/") != LOCATOR_PARTS - 1:
            raise LedgerError("locator_malformed")
        self.subject_id = subject_id
        self.item_code = item_code
        self.value_type = value_type
        self.value = value
        self.locator = locator
        self.confidence = confidence
        self.occurred_at = occurred_at


class EventIn:
    """出来事。per_record=True は冪等キーから revision を外す（R9: 会話は
    App 28 レコード番号で 1 件・行の版更新で増やさない）。"""

    __slots__ = ("kind", "locator", "summary", "occurred_at", "per_record")

    def __init__(self, kind: str, summary: str, locator: str = LOCATOR_UNKNOWN,
                 occurred_at=None, per_record: bool = False):
        self.kind = kind
        self.locator = locator
        self.summary = summary
        self.occurred_at = occurred_at
        self.per_record = per_record


class SourceRef:
    __slots__ = ("app_id", "record_id", "revision", "converter_name",
                 "converter_version", "updated_at")

    def __init__(self, app_id: str, record_id: str, revision: int,
                 converter_name: str, converter_version: str,
                 updated_at: str = ""):
        self.app_id = str(app_id)
        self.record_id = str(record_id)
        self.revision = int(revision)
        self.converter_name = converter_name
        self.converter_version = converter_version
        self.updated_at = updated_at or ""


def _ingest_key_where(src: SourceRef):
    return sa.and_(source_ingest.c.source_app_id == src.app_id,
                   source_ingest.c.source_record_id == src.record_id,
                   source_ingest.c.source_revision == src.revision,
                   source_ingest.c.locator == LOCATOR_UNKNOWN,
                   source_ingest.c.converter_name == src.converter_name,
                   source_ingest.c.converter_version == src.converter_version)


def _series_where(src: SourceRef):
    """出典系列（アプリ・レコード・locator・変換器名/版）の source_ingest 行（R8）。"""
    return sa.and_(source_ingest.c.source_app_id == src.app_id,
                   source_ingest.c.source_record_id == src.record_id,
                   source_ingest.c.locator == LOCATOR_UNKNOWN,
                   source_ingest.c.converter_name == src.converter_name,
                   source_ingest.c.converter_version == src.converter_version)


def _source_where(table, source_app_id: str, source_record_id: str):
    return sa.and_(table.c.source_app_id == str(source_app_id),
                   table.c.source_record_id == str(source_record_id))


async def known_revision(src: SourceRef) -> bool:
    """取込の冪等キーが既知か（revision 既知なら再処理しない・§5）。"""
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(source_ingest.c.ingest_id).where(_ingest_key_where(src))
        )).first()
        return row is not None


async def _has_current(session, source_app_id: str, source_record_id: str) -> bool:
    fact = (await session.execute(sa.select(case_fact.c.fact_id).where(
        _source_where(case_fact, source_app_id, source_record_id),
        case_fact.c.is_current.is_(True)).limit(1))).first()
    if fact is not None:
        return True
    ev = (await session.execute(sa.select(case_event.c.event_id).where(
        _source_where(case_event, source_app_id, source_record_id),
        case_event.c.is_current.is_(True)).limit(1))).first()
    return ev is not None


async def ingest_status(src: SourceRef, case_key: tuple | None) -> dict:
    """取込の要否判定に使う状態: known（冪等キー既知）・same_case（現在の紐付けが
    case_key と一致）・has_current_facts（その出典の現在 fact/event が案件側にある）。
    既知でも案件が違う／現在 fact が無い（紐付け訂正後）なら再処理する。"""
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(source_ingest).where(_ingest_key_where(src)))).first()
        if row is None:
            return {"known": False, "same_case": False, "has_current_facts": False,
                    "state": None}
        same = (case_key is not None and row.case_app_id == str(case_key[0])
                and row.case_record_id == str(case_key[1]))
        if case_key is None:
            same = row.state in ("held", "detached")
        has = await _has_current(session, src.app_id, src.record_id)
        return {"known": True, "same_case": same, "has_current_facts": has,
                "state": row.state}


async def _latest_revision(session, source_app_id: str, source_record_id: str) -> int | None:
    row = (await session.execute(
        sa.select(sa.func.max(source_ingest.c.source_revision),
                  sa.func.max(source_ingest.c.latest_seen_revision)).where(
            _source_where(source_ingest, source_app_id, source_record_id)))).first()
    vals = [int(v) for v in (row or ()) if v is not None]
    return max(vals) if vals else None


async def latest_known_revision(source_app_id: str, source_record_id: str) -> int | None:
    """出典レコードで見た最大 revision（取込行と latest_seen_revision の大きい方）。"""
    async with session_scope() as session:
        return await _latest_revision(session, source_app_id, source_record_id)


async def _series_latest_seen(session, src: SourceRef) -> int | None:
    row = (await session.execute(
        sa.select(sa.func.max(source_ingest.c.latest_seen_revision),
                  sa.func.max(source_ingest.c.source_revision))
        .where(_series_where(src)))).first()
    vals = [int(v) for v in (row or ()) if v is not None]
    return max(vals) if vals else None


async def _upsert_ingest(session, src: SourceRef, *, state: str, case_key,
                         hold_reason: str | None, now) -> None:
    if state not in INGEST_STATES:
        raise LedgerError("ingest_state_not_in_closed_set")
    case_app, case_rec = (case_key if case_key else (None, None))
    existing = (await session.execute(
        sa.select(source_ingest.c.ingest_id).where(_ingest_key_where(src)))).first()
    values = dict(state=state, case_app_id=case_app, case_record_id=case_rec,
                  hold_reason=hold_reason, source_updated_at=src.updated_at,
                  last_checked_at=now)
    if existing:
        await session.execute(sa.update(source_ingest).where(
            source_ingest.c.ingest_id == existing[0]).values(**values))
    else:
        await session.execute(sa.insert(source_ingest).values(
            source_kind=SOURCE_KIND_KINTONE, source_app_id=src.app_id,
            source_record_id=src.record_id, source_revision=src.revision,
            locator=LOCATOR_UNKNOWN, converter_name=src.converter_name,
            converter_version=src.converter_version, **values))
    # R8: 系列の latest_seen_revision を進める（下がることはない）
    seen = await _series_latest_seen(session, src)
    latest = max(src.revision, seen if seen is not None else src.revision)
    await session.execute(sa.update(source_ingest).where(_series_where(src)).values(
        latest_seen_revision=latest))


async def _assert_no_cycle(session, from_fact_id: int | None, to_fact_id: int) -> None:
    """supersedes の循環・自己参照をアプリ側で拒否（from が None なら新規行）。"""
    if from_fact_id is not None and from_fact_id == to_fact_id:
        raise LedgerError("supersedes_self_reference")
    seen = set()
    cur = to_fact_id
    for _ in range(10_000):
        if cur in seen:
            raise LedgerError("supersedes_cycle")
        seen.add(cur)
        if from_fact_id is not None and cur == from_fact_id:
            raise LedgerError("supersedes_cycle")
        row = (await session.execute(sa.select(case_fact.c.supersedes_fact_id)
                                     .where(case_fact.c.fact_id == cur))).first()
        if row is None or row[0] is None:
            return
        cur = int(row[0])
    raise LedgerError("supersedes_chain_too_long")


def _new_summary() -> dict:
    return {"inserted": 0, "skipped": 0, "events": 0, "moved": 0, "retired": 0,
            "detached": 0, "stale": 0}


async def ingest_source(src: SourceRef, case_key: tuple | None, facts: list,
                        events: list | None = None, *, extras: dict | None = None,
                        now=None) -> dict:
    """1 出典（レコード 1 revision）を台帳へ取り込む（1 トランザクション）。

    - case_key None（紐付け待ち）: 業務台帳へは入れず source_ingest=held のみ
      （extras["ingest_state"]="detached" なら R5 不成立＝保留にもしない）。
    - 同じ一意キーで値が違う → MismatchError を送出し、別トランザクションで
      source_ingest=mismatch_hold を記録（黙って捨てない）。
    - 出典系列の版更新は supersedes で繋ぎ、latest_seen_revision より古い入力は
      現在値にしない（R8）。extras["link_change"] は R10 の共通処理を同一
      トランザクションで先に適用する。extras["subjects_seen"] は BA-03。
    戻り値: {"inserted": n, "skipped": n, "events": n, "moved": n, "retired": n,
             "detached": n, "state": ...}
    """
    now = now or _now()
    summary = _new_summary()
    try:
        async with session_scope() as session:
            await _ingest_record_tx(session, src, case_key, facts, events or [],
                                    extras, now, summary)
    except MismatchError:
        async with session_scope() as session:
            await _upsert_ingest(session, src, state="mismatch_hold",
                                 case_key=case_key, hold_reason="mismatch", now=now)
        summary["state"] = "mismatch_hold"
        return summary
    return summary


async def _latest_ingest_row(session, source_app_id: str, source_record_id: str):
    return (await session.execute(sa.select(source_ingest).where(
        _source_where(source_ingest, source_app_id, source_record_id))
        .order_by(source_ingest.c.source_revision.desc()).limit(1))).first()


async def _ingest_stale_tx(session, src: SourceRef, facts: list, now, s: dict) -> None:
    """R11: 系列の latest_seen_revision より小さい入力は「履歴として保存するだけ」。
    現在の紐付け・fact/event の有効性・取込状態・link_history を一切変えない。
    fact は現在の紐付け先の案件に is_current=False（stale_revision）で残す。
    出来事は積まない。取込行は現在の行の状態・案件をそのまま写す。"""
    cur = await _latest_ingest_row(session, src.app_id, src.record_id)
    case = (cur.case_app_id, cur.case_record_id) if cur is not None and cur.case_app_id else None
    if case is not None:
        for f in facts:
            await _ingest_one(session, src, case[0], case[1], f, now, s, False)
    # mismatch_hold は revision 単位の保留なので旧版の行には写さない（他は出典単位の状態）
    state = cur.state if cur is not None else "held"
    if state == "mismatch_hold":
        state = "ingested"
    await _upsert_ingest(session, src, state=state, case_key=case,
                         hold_reason=cur.hold_reason if cur is not None else "pending_link",
                         now=now)
    if cur is not None and cur.pending_recheck:
        await session.execute(sa.update(source_ingest).where(
            _ingest_key_where(src)).values(pending_recheck=True))
    s["state"] = STALE_STATE
    s["stale"] = 1


async def _ingest_record_tx(session, src: SourceRef, case_key, facts: list,
                            events: list, extras: dict | None, now, s: dict) -> None:
    """1 出典の取込本体（呼び出し側のトランザクション内・ページ経路と単発経路で共通）。
    R11: revision の新旧判定を関連解除より前に同一トランザクションで行う。"""
    extras = extras or {}
    seen = await _series_latest_seen(session, src)
    if extras.get("stale") or (seen is not None and src.revision < seen):
        await _ingest_stale_tx(session, src, facts, now, s)
        return
    change = extras.get("link_change")
    if change is not None:
        await _detach_source_tx(session, src.app_id, src.record_id, now=now,
                                source_revision=src.revision, **change)
        s["detached"] += 1
    # R13/BA-15: ここに来た＝有効な最新版の判定に成功（採用・不採用を問わず）。
    # 当該出典の全取込行の pending_recheck を同一トランザクションで解除する
    await session.execute(sa.update(source_ingest).where(
        _source_where(source_ingest, src.app_id, src.record_id),
        source_ingest.c.pending_recheck.is_(True)).values(pending_recheck=False))
    if case_key is None:
        state = extras.get("ingest_state") or "held"
        if state not in ("held", "detached"):
            raise LedgerError("ingest_state_not_in_closed_set")
        hold_reason = extras.get("hold_reason") or "pending_link"
        await _upsert_ingest(session, src, state=state, case_key=None,
                             hold_reason=hold_reason, now=now)
        # R13: 有効な最新版の判定に成功した＝unavailable だった行も同じ状態へ復旧
        await session.execute(sa.update(source_ingest).where(
            _source_where(source_ingest, src.app_id, src.record_id),
            source_ingest.c.state == "unavailable").values(
            state=state, hold_reason=hold_reason, last_checked_at=now))
        s["state"] = state
        return
    case_app, case_rec = str(case_key[0]), str(case_key[1])
    for f in facts:
        await _ingest_one(session, src, case_app, case_rec, f, now, s, True)
    await _retire_removed_rows(session, src, case_app, case_rec,
                               extras.get("subjects_seen"), now, s)
    for ev in events:
        await _ingest_event(session, src, case_app, case_rec, ev, s)
    await _upsert_ingest(session, src, state="ingested", case_key=(case_app, case_rec),
                         hold_reason=None, now=now)
    # 案件紐付けの現在値は出典単位: 以前 held/detached だった旧 revision の行も追随。
    # R13/BA-14: 有効な最新版の取込に成功した＝unavailable もここで ingested へ復旧する
    await session.execute(sa.update(source_ingest).where(
        _source_where(source_ingest, src.app_id, src.record_id),
        source_ingest.c.state.in_(("held", "detached", "unavailable"))).values(
        state="ingested", case_app_id=case_app, case_record_id=case_rec,
        hold_reason=None, last_checked_at=now))
    s["state"] = "ingested"


def _event_idem(case_app: str, case_rec: str, src: SourceRef, ev: EventIn) -> str:
    rev = LOCATOR_UNKNOWN if ev.per_record else str(src.revision)
    return "|".join([case_app, case_rec, src.app_id, src.record_id, rev, ev.kind,
                     ev.locator])


async def _ingest_event(session, src: SourceRef, case_app: str, case_rec: str,
                        ev: EventIn, s: dict) -> None:
    idem = _event_idem(case_app, case_rec, src, ev)
    exists = (await session.execute(sa.select(case_event).where(
        case_event.c.idem_key == idem))).first()
    if exists is not None:
        if not exists.is_current:
            # 関連喪失で外れた同じ出来事が同じ案件へ戻った（復帰＝状態変更・削除なし）
            await session.execute(sa.update(case_event).where(
                case_event.c.event_id == exists.event_id).values(
                is_current=True, invalid_reason=None, invalidated_at=None))
            s["events"] += 1
        return
    await session.execute(sa.insert(case_event).values(
        idem_key=idem, case_app_id=case_app, case_record_id=case_rec,
        kind=ev.kind, occurred_at=ev.occurred_at,
        source_app_id=src.app_id, source_record_id=src.record_id,
        source_revision=src.revision, locator=ev.locator, summary=ev.summary,
        is_current=True))
    s["events"] += 1


def _series_fact_where(src: SourceRef, f: FactIn):
    return sa.and_(case_fact.c.source_app_id == src.app_id,
                   case_fact.c.source_record_id == src.record_id,
                   case_fact.c.locator == f.locator,
                   case_fact.c.converter_name == src.converter_name,
                   case_fact.c.converter_version == src.converter_version,
                   case_fact.c.subject_id == f.subject_id,
                   case_fact.c.item_code == f.item_code)


async def _retired_predecessor(session, src: SourceRef, f: FactIn, case_app: str,
                               case_rec: str) -> tuple:
    """関連喪失・行消去で外れた系列の最後の fact。戻り値 (same_case_pred, other_case_pred)。
    supersedes は**同じ案件**の系列にだけ張る（BA-16: 案件間の往復で UNIQUE(supersedes)
    に触れない・履歴は案件ごとに一直線）。別案件の直前行は prev_case の情報にだけ使う。"""
    rows = (await session.execute(sa.select(case_fact).where(
        _series_fact_where(src, f), case_fact.c.is_current.is_(False),
        case_fact.c.invalid_reason.in_(RETIRED_REASONS))
        .order_by(case_fact.c.fact_id.desc()))).fetchall()
    same = other = None
    for row in rows:
        if (row.case_app_id, row.case_record_id) == (case_app, case_rec):
            if same is None:
                has_successor = (await session.execute(sa.select(case_fact.c.fact_id).where(
                    case_fact.c.supersedes_fact_id == row.fact_id))).first()
                if has_successor is None:
                    same = row
        elif other is None:
            other = row
    return same, other


async def _ingest_one(session, src: SourceRef, case_app: str, case_rec: str,
                      f: FactIn, now, summary: dict, newest: bool) -> None:
    vtext, vjson = canonical_value(f.value_type, f.value)
    # 同一一意キーの既存行
    same_key = (await session.execute(sa.select(case_fact).where(
        case_fact.c.case_app_id == case_app, case_fact.c.case_record_id == case_rec,
        _series_fact_where(src, f), case_fact.c.source_revision == src.revision))).first()
    if same_key is not None:
        if same_key.value_text != vtext:
            raise MismatchError("fact_value_mismatch")
        if not same_key.is_current and newest:
            head_now = (await session.execute(sa.select(case_fact.c.fact_id).where(
                _series_fact_where(src, f), case_fact.c.is_current.is_(True)))).first()
            if head_now is None:
                # 紐付け訂正で外した行を同じ案件へ戻す（復帰＝状態変更・削除なし）
                await session.execute(sa.update(case_fact).where(
                    case_fact.c.fact_id == same_key.fact_id).values(
                    is_current=True, invalid_reason=None, invalidated_at=None))
                summary["inserted"] += 1
                return
        summary["skipped"] += 1
        return
    # 出典系列の現在値（案件を問わず: 紐付け移動の検出のため）
    head = (await session.execute(sa.select(case_fact).where(
        _series_fact_where(src, f), case_fact.c.is_current.is_(True)))).first()
    obs = observation_id(src.app_id, src.record_id, f.locator, src.converter_name,
                         src.converter_version)
    base = dict(subject_id=f.subject_id, item_code=f.item_code,
                value_type=f.value_type, value_text=vtext, value_json=vjson,
                source_kind=SOURCE_KIND_KINTONE, source_app_id=src.app_id,
                source_record_id=src.record_id, source_revision=src.revision,
                locator=f.locator, converter_name=src.converter_name,
                converter_version=src.converter_version, observation_id=obs,
                occurred_at=f.occurred_at, observed_at=now,
                confidence=f.confidence)
    empty = vtext == "" and vjson in (None, [])
    if head is None:
        if not newest:
            # R8: latest_seen_revision より古い入力は保存するが現在値にしない
            if empty:
                summary["skipped"] += 1
                return
            await session.execute(sa.insert(case_fact).values(
                case_app_id=case_app, case_record_id=case_rec, is_current=False,
                invalid_reason=INVALID_STALE, **base))
            summary["inserted"] += 1
            return
        same, other = await _retired_predecessor(session, src, f, case_app, case_rec)
        if same is None and other is None:
            if empty:
                summary["skipped"] += 1          # 値なし・履歴も無し＝積まない
                return
            await session.execute(sa.insert(case_fact).values(
                case_app_id=case_app, case_record_id=case_rec, is_current=True, **base))
            summary["inserted"] += 1
            return
        # 関連喪失・行消去の後の復帰: 同じ案件の直前行にだけ supersedes を張る（一直線）。
        # 別案件から戻ってきた場合は prev_case を残す（supersedes は張らない・BA-16）
        if same is not None:
            await _assert_no_cycle(session, None, int(same.fact_id))
        await session.execute(sa.insert(case_fact).values(
            case_app_id=case_app, case_record_id=case_rec, is_current=True,
            supersedes_fact_id=int(same.fact_id) if same is not None else None,
            prev_case_app_id=other.case_app_id if (same is None and other is not None) else None,
            prev_case_record_id=other.case_record_id if (same is None and other is not None) else None,
            **base))
        summary["inserted"] += 1
        if same is None:
            summary["moved"] += 1
        return
    moved = (head.case_app_id, head.case_record_id) != (case_app, case_rec)
    changed = head.value_text != vtext or moved
    if newest and src.revision > head.source_revision:
        if not changed:
            summary["skipped"] += 1
            return
        if not moved:
            await _assert_no_cycle(session, None, int(head.fact_id))
        await session.execute(sa.update(case_fact).where(
            case_fact.c.fact_id == head.fact_id).values(
            is_current=False, invalidated_at=now,
            invalid_reason=(INVALID_LINK_MOVED if moved else INVALID_SUPERSEDED)))
        # 案件をまたぐ移動は supersedes を張らない（案件ごとの履歴を一直線に保つ・BA-16）
        await session.execute(sa.insert(case_fact).values(
            case_app_id=case_app, case_record_id=case_rec, is_current=True,
            supersedes_fact_id=None if moved else int(head.fact_id),
            prev_case_app_id=head.case_app_id if moved else None,
            prev_case_record_id=head.case_record_id if moved else None, **base))
        summary["inserted"] += 1
        if moved:
            summary["moved"] += 1
        return
    # 旧 revision の遅着（現在値にしない・独立した観測として残す・R8）
    if not changed or empty:
        summary["skipped"] += 1
        return
    await session.execute(sa.insert(case_fact).values(
        case_app_id=case_app, case_record_id=case_rec, is_current=False,
        invalid_reason=INVALID_STALE, **base))
    summary["inserted"] += 1


async def _retire_removed_rows(session, src: SourceRef, case_app: str, case_rec: str,
                               subjects_seen: dict | None, now, s: dict) -> None:
    """BA-03: 同一レコードの新版を完全に取得できたときだけ、前版にあって新版に無い
    サブテーブル行（subject）の現在 fact を row_removed で利用停止（履歴・削除なし）。
    subjects_seen は {subject接頭辞: {行ID,...}}。None／接頭辞なし＝判断しない。"""
    if not subjects_seen:
        return
    for prefix, ids in subjects_seen.items():
        if ids is None:
            continue                                   # 取得できていない表は判断しない
        rows = (await session.execute(sa.select(case_fact).where(
            case_fact.c.case_app_id == case_app, case_fact.c.case_record_id == case_rec,
            case_fact.c.source_app_id == src.app_id,
            case_fact.c.source_record_id == src.record_id,
            case_fact.c.converter_name == src.converter_name,
            case_fact.c.converter_version == src.converter_version,
            case_fact.c.is_current.is_(True),
            case_fact.c.subject_id.like(prefix + "%")))).fetchall()
        gone = [r.fact_id for r in rows if r.subject_id[len(prefix):] not in ids]
        if not gone:
            continue
        await session.execute(sa.update(case_fact).where(
            case_fact.c.fact_id.in_(gone)).values(
            is_current=False, invalid_reason=INVALID_ROW_REMOVED, invalidated_at=now))
        s["retired"] += len(gone)


# ── 関連喪失の共通処理（R10） ───────────────────────────────────────────────

async def _current_case_tx(session, source_app_id: str, source_record_id: str):
    row = (await session.execute(
        sa.select(source_ingest.c.case_app_id, source_ingest.c.case_record_id)
        .where(_source_where(source_ingest, source_app_id, source_record_id))
        .order_by(source_ingest.c.source_revision.desc()).limit(1))).first()
    if row is None or row[0] is None:
        return None
    return (row[0], row[1])


async def _detach_source_tx(session, source_app_id: str, source_record_id: str, *,
                            new_case: tuple | None, reason: str, trust: str,
                            candidates=None, actor: str = "system",
                            operation_id: str | None = None, now,
                            source_revision: int | None = None,
                            ingest_state: str = "held") -> dict:
    """R10: 自動採用可の関連が失われた／移動したときの共通処理（同一トランザクション）。
    1) link_history に理由付きで記録 2) 旧案件の当該出典由来 fact/event を
    is_current=False（link_moved／link_lost）3) 依存する確認は再確認一覧に出る
    （list_recheck が invalid_reason から導く）4) source_ingest の現在値を移動先
    （auto なら ingested）／なし（held＝紐付け待ち／detached＝R5 不成立）へ。
    削除はしない。移動先へ積む処理は呼び出し側（取込）が同じトランザクションで行う。"""
    if trust not in TRUST_LEVELS:
        raise LedgerError("trust_level_not_in_closed_set")
    if ingest_state not in ("held", "detached"):
        raise LedgerError("ingest_state_not_in_closed_set")
    source_app_id, source_record_id = str(source_app_id), str(source_record_id)
    prev = await _current_case_tx(session, source_app_id, source_record_id)
    new_case = (str(new_case[0]), str(new_case[1])) if new_case else None
    reason_code = INVALID_LINK_MOVED if new_case else INVALID_LINK_LOST
    link = await session.execute(sa.insert(link_history).values(
        source_app_id=source_app_id, source_record_id=source_record_id,
        prev_case_app_id=prev[0] if prev else None,
        prev_case_record_id=prev[1] if prev else None,
        new_case_app_id=new_case[0] if new_case else None,
        new_case_record_id=new_case[1] if new_case else None,
        trust_level=trust, reason=reason, candidates=candidates or None,
        operation_id=operation_id, actor=actor, source_revision=source_revision))
    facts = await session.execute(sa.update(case_fact).where(
        _source_where(case_fact, source_app_id, source_record_id),
        case_fact.c.is_current.is_(True)).values(
        is_current=False, invalid_reason=reason_code, invalidated_at=now))
    events = await session.execute(sa.update(case_event).where(
        _source_where(case_event, source_app_id, source_record_id),
        case_event.c.is_current.is_(True)).values(
        is_current=False, invalid_reason=reason_code, invalidated_at=now))
    # 取込行の現在値を移す。mismatch_hold（revision 単位の保留）と unavailable（復旧は
    # 有効な最新版の照合成功時だけ・R13）は状態を保ったまま案件だけ移す
    await session.execute(sa.update(source_ingest).where(
        _source_where(source_ingest, source_app_id, source_record_id)).values(
        case_app_id=new_case[0] if new_case else None,
        case_record_id=new_case[1] if new_case else None,
        state=sa.case((source_ingest.c.state.in_(("mismatch_hold", "unavailable")),
                       source_ingest.c.state),
                      else_="ingested" if new_case else ingest_state),
        hold_reason=None if new_case else reason, last_checked_at=now))
    return {"link_id": int(link.inserted_primary_key[0]), "prev": prev,
            "moved_facts": int(facts.rowcount or 0),
            "moved_events": int(events.rowcount or 0)}


async def mark_source_unavailable(source_app_id: str, source_record_id: str,
                                  *, now=None) -> int:
    """取得不能（権限・404）: 削除と推定せず「出典確認不能」として利用停止。
    fact は消さない・値なしにしない（source_ingest の状態だけを変える）。"""
    now = now or _now()
    async with session_scope() as session:
        result = await session.execute(sa.update(source_ingest).where(
            _source_where(source_ingest, source_app_id, source_record_id),
            source_ingest.c.state != "unavailable").values(
            state="unavailable", last_checked_at=now))
        return int(result.rowcount or 0)


async def _restore_unavailable_tx(session, source_app_id: str, source_record_id: str, now) -> int:
    """unavailable の解除。復帰先は案件付きなら ingested、案件なしは held（R5 不成立の
    理由なら detached）。"""
    rows = (await session.execute(sa.select(source_ingest).where(
        _source_where(source_ingest, source_app_id, source_record_id),
        source_ingest.c.state == "unavailable"))).fetchall()
    for r in rows:
        if r.case_app_id is not None:
            state = "ingested"
        elif r.hold_reason in DETACH_REASONS:
            state = "detached"
        else:
            state = "held"
        await session.execute(sa.update(source_ingest).where(
            source_ingest.c.ingest_id == r.ingest_id).values(
            state=state, last_checked_at=now))
    return len(rows)


async def mark_source_verified(source_app_id: str, source_record_id: str,
                               *, revision: int | None = None, now=None) -> dict:
    """R13/BA-19: 有効な最新版の照合が成功した（取込は不要）ときの復旧を**1 トランザクション**で:
    pending_recheck の解除と unavailable の解除（復帰先は _restore_unavailable_tx）。
    BA-24: revision を渡すと、照合した最新 revision へ latest_seen_revision を同じ
    トランザクションで前進させる（下がることはない・関連解除の状態は変えない＝復活させない）。
    途中で失敗すればすべて元のまま（一部だけ変わらない）。"""
    now = now or _now()
    async with session_scope() as session:
        cleared = await session.execute(sa.update(source_ingest).where(
            _source_where(source_ingest, source_app_id, source_record_id),
            source_ingest.c.pending_recheck.is_(True)).values(
            pending_recheck=False, last_checked_at=now))
        restored = await _restore_unavailable_tx(session, source_app_id, source_record_id, now)
        advanced = 0
        if revision is not None:
            adv = await session.execute(sa.update(source_ingest).where(
                _source_where(source_ingest, source_app_id, source_record_id),
                sa.or_(source_ingest.c.latest_seen_revision.is_(None),
                       source_ingest.c.latest_seen_revision < int(revision))).values(
                latest_seen_revision=int(revision), last_checked_at=now))
            advanced = int(adv.rowcount or 0)
        return {"pending_cleared": int(cleared.rowcount or 0), "restored": restored,
                "advanced": advanced}


async def mark_source_checked(source_app_id: str, source_record_id: str,
                              *, now=None) -> int:
    """再照合で出典が健在だったときの最終確認日時の更新（unavailable の解除）。
    BA-19 以降は mark_source_verified が正（本関数は unavailable の解除だけ）。"""
    now = now or _now()
    async with session_scope() as session:
        return await _restore_unavailable_tx(session, source_app_id, source_record_id, now)


async def list_sources(source_app_id: str, *, after: str = "0",
                       limit: int | None = None, pending_only: bool = False) -> list[dict]:
    """出典レコードごとの最新取込（追跡再照合・定期照合の対象一覧）。
    レコード番号の数値順・after より大きいものから limit 件（BA-05 の継続位置）。
    pending_only: 判定不能（pending_recheck）の出典だけ（R12・毎回先に再照合する）。"""
    async with session_scope() as session:
        q = (sa.select(source_ingest.c.source_record_id,
                       sa.func.max(source_ingest.c.source_revision).label("rev"))
             .where(source_ingest.c.source_app_id == str(source_app_id),
                    _int_id(source_ingest.c.source_record_id) > int(after or "0"))
             .group_by(source_ingest.c.source_record_id)
             .order_by(_int_id(source_ingest.c.source_record_id)))
        if pending_only:
            q = q.where(source_ingest.c.pending_recheck.is_(True))
        if limit is not None:
            q = q.limit(int(limit))
        rows = (await session.execute(q)).fetchall()
        return [{"record_id": r.source_record_id, "revision": int(r.rev)} for r in rows]


async def set_pending_recheck(source_app_id: str, source_record_id: str, flag: bool,
                              *, now=None) -> int:
    """R12: 判定不能を pending_recheck として記録（関連・状態は保持）／判定成功で解除。
    出典の行が無い（未取込）ときは何も記録しない（窓の再取得・定期照合で再試行）。"""
    now = now or _now()
    async with session_scope() as session:
        result = await session.execute(sa.update(source_ingest).where(
            _source_where(source_ingest, source_app_id, source_record_id),
            source_ingest.c.pending_recheck.is_(not flag)).values(
            pending_recheck=flag, last_checked_at=now))
        return int(result.rowcount or 0)


async def count_pending_recheck(source_app_id: str) -> int:
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(sa.func.count(sa.distinct(source_ingest.c.source_record_id)))
            .where(source_ingest.c.source_app_id == str(source_app_id),
                   source_ingest.c.pending_recheck.is_(True)))).first()
        return int(row[0] or 0)


async def current_case_of_source(source_app_id: str, source_record_id: str) -> tuple | None:
    async with session_scope() as session:
        return await _current_case_tx(session, source_app_id, source_record_id)


async def find_case_by_item_value(item_code: str, value_text: str) -> list[tuple]:
    """現在値の一致で案件キーを引く（例: app40.LINEユーザーID）。重複なし。
    R5 の一意判定は正本（kintone）で行う（BA-04）。本関数は台帳の参照用。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(case_fact.c.case_app_id, case_fact.c.case_record_id)
            .where(case_fact.c.item_code == item_code,
                   case_fact.c.value_text == value_text,
                   case_fact.c.is_current.is_(True)).distinct())).fetchall()
        return [(r[0], r[1]) for r in rows]


async def case_exists(case_app_id: str, case_record_id: str) -> bool:
    """案件キーが台帳に存在するか（自身の取込行 or 案件キーを持つ fact）。"""
    async with session_scope() as session:
        return await _case_exists_tx(session, case_app_id, case_record_id)


async def _case_exists_tx(session, case_app_id: str, case_record_id: str) -> bool:
    own = (await session.execute(sa.select(source_ingest.c.ingest_id).where(
        _source_where(source_ingest, case_app_id, case_record_id)).limit(1))).first()
    if own is not None:
        return True
    fact = (await session.execute(sa.select(case_fact.c.fact_id).where(
        case_fact.c.case_app_id == str(case_app_id),
        case_fact.c.case_record_id == str(case_record_id)).limit(1))).first()
    return fact is not None


# ── 紐付け履歴（§3・§4-6） ──────────────────────────────────────────────────

async def add_link_history(*, source_app_id: str, source_record_id: str,
                           prev_case: tuple | None, new_case: tuple | None,
                           trust_level: str, reason: str, candidates=None,
                           operation_id: str | None = None,
                           actor: str = "system") -> int:
    if trust_level not in TRUST_LEVELS:
        raise LedgerError("trust_level_not_in_closed_set")
    async with session_scope() as session:
        if operation_id:
            dup = (await session.execute(sa.select(link_history.c.link_id).where(
                link_history.c.operation_id == operation_id))).first()
            if dup:
                return int(dup[0])
        result = await session.execute(sa.insert(link_history).values(
            source_app_id=str(source_app_id), source_record_id=str(source_record_id),
            prev_case_app_id=prev_case[0] if prev_case else None,
            prev_case_record_id=prev_case[1] if prev_case else None,
            new_case_app_id=new_case[0] if new_case else None,
            new_case_record_id=new_case[1] if new_case else None,
            trust_level=trust_level, reason=reason, candidates=candidates,
            operation_id=operation_id, actor=actor))
        return int(result.inserted_primary_key[0])


def _link_view(row) -> dict:
    return {"link_id": int(row.link_id), "trust_level": row.trust_level,
            "reason": row.reason, "actor": row.actor,
            "source_revision": (int(row.source_revision)
                                if row.source_revision is not None else None),
            "new_case": ((row.new_case_app_id, row.new_case_record_id)
                         if row.new_case_app_id else None),
            "prev_case": ((row.prev_case_app_id, row.prev_case_record_id)
                          if row.prev_case_app_id else None),
            "candidates": row.candidates, "created_at": _iso(row.created_at)}


async def _latest_link_tx(session, source_app_id: str, source_record_id: str):
    return (await session.execute(sa.select(link_history).where(
        _source_where(link_history, source_app_id, source_record_id))
        .order_by(link_history.c.link_id.desc()).limit(1))).first()


async def latest_link(source_app_id: str, source_record_id: str) -> dict | None:
    async with session_scope() as session:
        row = await _latest_link_tx(session, source_app_id, source_record_id)
        return _link_view(row) if row is not None else None


async def link_version(source_app_id: str, source_record_id: str) -> int:
    """紐付け訂正の版（画面が見る値）: 最新 link_history の link_id（無ければ 0）。"""
    async with session_scope() as session:
        row = await _latest_link_tx(session, source_app_id, source_record_id)
        return int(row.link_id) if row is not None else 0


async def relink_source(*, source_app_id: str, source_record_id: str,
                        new_case: tuple, reason: str, operation_id: str,
                        actor: str, seen_link_version: int,
                        seen_source_revision: int, now=None) -> dict:
    """紐付け訂正（人の操作・BA-06）: R10 の共通処理を通す履歴追加のみ。
    受付条件（同一トランザクションで照合）: 操作 ID 未使用、画面が見ていた
    紐付け版（最新 link_id）と出典 revision が現在値と一致（不一致は
    VersionConflict に最新を添える）、訂正先が台帳に存在する案件キー
    （LedgerError: relink_target_unknown）。次回同期で新案件側の fact が積まれる。"""
    now = now or _now()
    new_case = (str(new_case[0]), str(new_case[1]))
    source_app_id, source_record_id = str(source_app_id), str(source_record_id)
    # R15/BA-25: 訂正対象は許可集合 RELINKABLE_APPS（App 28・App 30）だけ。許可集合外は
    # ここで拒否。案件アプリ（訂正先と同じアプリ）・台帳に案件アプリとして現れているアプリの
    # 判定は二重の安全として残す
    if source_app_id not in RELINKABLE_APPS:
        raise NotRelinkable()
    if source_app_id == new_case[0]:
        raise NotRelinkable()
    async with session_scope() as session:
        is_case_app = (await session.execute(sa.select(source_ingest.c.ingest_id).where(
            source_ingest.c.case_app_id == source_app_id).limit(1))).first()
        if is_case_app is not None:
            raise NotRelinkable()
        dup = (await session.execute(sa.select(link_history.c.link_id).where(
            link_history.c.operation_id == operation_id))).first()
        if dup:
            return {"duplicate": True, "link_id": int(dup[0])}
        last = await _latest_link_tx(session, source_app_id, source_record_id)
        cur_link = int(last.link_id) if last is not None else 0
        cur_rev = await _latest_revision(session, source_app_id, source_record_id)
        current = {"link_version": cur_link, "source_revision": cur_rev}
        if int(seen_link_version) != cur_link or cur_rev is None \
                or int(seen_source_revision) != int(cur_rev):
            raise VersionConflict(current)
        if not await _case_exists_tx(session, new_case[0], new_case[1]):
            raise LedgerError("relink_target_unknown")
        # BA-11: 旧案件側の現在 fact/event を先に取り、無効化と同じトランザクションで
        # 移動先へ積み直す（履歴を繋ぐ・削除しない）
        live_facts = (await session.execute(sa.select(case_fact).where(
            _source_where(case_fact, source_app_id, source_record_id),
            case_fact.c.is_current.is_(True)))).fetchall()
        live_events = (await session.execute(sa.select(case_event).where(
            _source_where(case_event, source_app_id, source_record_id),
            case_event.c.is_current.is_(True)))).fetchall()
        out = await _detach_source_tx(
            session, source_app_id, source_record_id, new_case=new_case,
            reason=reason, trust="auto", actor=actor, operation_id=operation_id,
            now=now, source_revision=cur_rev)
        re_facts, re_events = await _reproject_tx(session, live_facts, live_events,
                                                  new_case, now)
        return {"duplicate": False, "link_id": out["link_id"],
                "moved_facts": out["moved_facts"], "prev": out["prev"],
                "reprojected_facts": re_facts, "reprojected_events": re_events}


async def _reproject_tx(session, live_facts: list, live_events: list, new_case: tuple,
                        now) -> tuple:
    """訂正先へ fact/event を積み直す（BA-11/BA-16）。案件をまたぐ移動は supersedes を
    張らず prev_case で出所を残す（案件ごとの供述は一直線・UNIQUE(supersedes) に触れない）。
    同じ一意キー（同じ観測×同じ案件×同じ revision）の行が移動先に既にあるときは、
    uq_case_fact_key により新行を作れないため、その行を現在値へ戻す（供述の重複は作らない）。"""
    case_app, case_rec = new_case
    n_facts = 0
    for f in live_facts:
        existing = (await session.execute(sa.select(case_fact).where(
            case_fact.c.case_app_id == case_app, case_fact.c.case_record_id == case_rec,
            case_fact.c.subject_id == f.subject_id, case_fact.c.item_code == f.item_code,
            case_fact.c.source_app_id == f.source_app_id,
            case_fact.c.source_record_id == f.source_record_id,
            case_fact.c.source_revision == f.source_revision,
            case_fact.c.locator == f.locator,
            case_fact.c.converter_name == f.converter_name,
            case_fact.c.converter_version == f.converter_version))).first()
        moved = (f.case_app_id, f.case_record_id) != (case_app, case_rec)
        if existing is not None:
            if not existing.is_current:
                await session.execute(sa.update(case_fact).where(
                    case_fact.c.fact_id == existing.fact_id).values(
                    is_current=True, invalid_reason=None, invalidated_at=None,
                    prev_case_app_id=f.case_app_id if moved else existing.prev_case_app_id,
                    prev_case_record_id=(f.case_record_id if moved
                                         else existing.prev_case_record_id)))
                n_facts += 1
            continue
        await session.execute(sa.insert(case_fact).values(
            case_app_id=case_app, case_record_id=case_rec, subject_id=f.subject_id,
            item_code=f.item_code, value_type=f.value_type, value_text=f.value_text,
            value_json=f.value_json, source_kind=f.source_kind,
            source_app_id=f.source_app_id, source_record_id=f.source_record_id,
            source_revision=f.source_revision, locator=f.locator,
            converter_name=f.converter_name, converter_version=f.converter_version,
            observation_id=f.observation_id, occurred_at=f.occurred_at,
            observed_at=now, confidence=f.confidence, is_current=True,
            supersedes_fact_id=None,
            prev_case_app_id=f.case_app_id if moved else None,
            prev_case_record_id=f.case_record_id if moved else None))
        n_facts += 1
    n_events = 0
    for e in live_events:
        parts = e.idem_key.split("|")
        idem = "|".join([case_app, case_rec] + parts[2:]) if len(parts) == 7 else \
            "|".join([case_app, case_rec, e.idem_key])
        existing = (await session.execute(sa.select(case_event).where(
            case_event.c.idem_key == idem))).first()
        if existing is not None:
            if not existing.is_current:
                await session.execute(sa.update(case_event).where(
                    case_event.c.event_id == existing.event_id).values(
                    is_current=True, invalid_reason=None, invalidated_at=None))
                n_events += 1
            continue
        await session.execute(sa.insert(case_event).values(
            idem_key=idem, case_app_id=case_app, case_record_id=case_rec, kind=e.kind,
            occurred_at=e.occurred_at, source_app_id=e.source_app_id,
            source_record_id=e.source_record_id, source_revision=e.source_revision,
            locator=e.locator, summary=e.summary, is_current=True))
        n_events += 1
    return n_facts, n_events


# ── 確認（§4-2） ────────────────────────────────────────────────────────────

async def _series_head(session, fact_row) -> dict:
    head = (await session.execute(sa.select(case_fact).where(
        case_fact.c.source_app_id == fact_row.source_app_id,
        case_fact.c.source_record_id == fact_row.source_record_id,
        case_fact.c.locator == fact_row.locator,
        case_fact.c.converter_name == fact_row.converter_name,
        case_fact.c.converter_version == fact_row.converter_version,
        case_fact.c.subject_id == fact_row.subject_id,
        case_fact.c.item_code == fact_row.item_code,
        case_fact.c.is_current.is_(True)))).first()
    if head is None:
        return {"fact_id": int(fact_row.fact_id), "version": int(fact_row.source_revision),
                "is_current": False, "invalid_reason": fact_row.invalid_reason}
    return {"fact_id": int(head.fact_id), "version": int(head.source_revision),
            "is_current": True, "invalid_reason": None}


async def current_version(fact_id: int) -> dict | None:
    async with session_scope() as session:
        row = (await session.execute(sa.select(case_fact).where(
            case_fact.c.fact_id == int(fact_id)))).first()
        if row is None:
            return None
        return await _series_head(session, row)


async def _source_unavailable_tx(session, source_app_id: str, source_record_id: str) -> bool:
    row = (await session.execute(sa.select(source_ingest.c.ingest_id).where(
        _source_where(source_ingest, source_app_id, source_record_id),
        source_ingest.c.state == "unavailable").limit(1))).first()
    return row is not None


async def record_confirmation(*, fact_id: int, seen_version: int, decision: str,
                              reason: str, operation_id: str, actor: str,
                              revoked_of: int | None = None, now=None) -> dict:
    """確認/却下/撤回の記録（操作 ID 冪等・対象版一致・削除なし）。
    受付条件（§4-2）: 操作 ID 未使用、画面が見ていた版 = 現在の版。不一致は
    VersionConflict（最新を添える）。確認は当該 fact の版に固定（新版へ継承しない）。
    出典確認不能の fact への確認/却下は SourceUnavailable（BA-07・撤回は可）。"""
    if decision not in DECISION_VALUES:
        raise LedgerError("decision_not_in_closed_set")
    now = now or _now()
    async with session_scope() as session:
        dup = (await session.execute(sa.select(case_confirmation).where(
            case_confirmation.c.operation_id == operation_id))).first()
        if dup:
            return {"duplicate": True, "confirmation_id": int(dup.confirmation_id)}
        fact = (await session.execute(sa.select(case_fact).where(
            case_fact.c.fact_id == int(fact_id)))).first()
        if fact is None:
            raise LedgerError("fact_not_found")
        head = await _series_head(session, fact)
        # 画面が見ていた版 = 現在の版。confirm/reject は現在 fact にのみ付けられる。
        # revoke は旧 fact の確認にも付けられる（再確認一覧からの撤回）が、版は現在の版
        if int(seen_version) != head["version"]:
            raise VersionConflict(head, CONFLICT_VERSION)
        if decision != "revoke":
            # BA-13: 系列 head が生きていて、対象 fact 自身が有効であること
            if not head["is_current"] or not fact.is_current or fact.invalid_reason:
                raise VersionConflict(head, CONFLICT_NOT_CURRENT)
            if head["fact_id"] != int(fact_id):
                raise VersionConflict(head, CONFLICT_VERSION)
            src_row = await _latest_ingest_row(session, fact.source_app_id,
                                               fact.source_record_id)
            if src_row is not None and src_row.state == "detached":
                raise VersionConflict(head, CONFLICT_SOURCE_DETACHED)
            if await _source_unavailable_tx(session, fact.source_app_id,
                                            fact.source_record_id):
                raise SourceUnavailable(FLAG_SOURCE_UNAVAILABLE)
        if decision == "revoke":
            if revoked_of is None:
                raise LedgerError("revoke_requires_target")
            target = (await session.execute(sa.select(case_confirmation).where(
                case_confirmation.c.confirmation_id == int(revoked_of)))).first()
            if target is None or int(target.fact_id) != int(fact_id):
                raise LedgerError("revoke_target_mismatch")
        result = await session.execute(sa.insert(case_confirmation).values(
            fact_id=int(fact_id), fact_version=int(fact.source_revision),
            actor=actor, decided_at=now, decision=decision, reason=reason or "",
            revoked_of=int(revoked_of) if revoked_of is not None else None,
            operation_id=operation_id, seen_version=int(seen_version)))
        return {"duplicate": False,
                "confirmation_id": int(result.inserted_primary_key[0])}


# ── ビュー（§9: 紐付け待ち・競合・再確認・同期状態） ─────────────────────────

def _fact_view(row, flag: str | None = None) -> dict:
    return {"fact_id": int(row.fact_id), "case_app_id": row.case_app_id,
            "case_record_id": row.case_record_id, "subject_id": row.subject_id,
            "item_code": row.item_code, "value_type": row.value_type,
            "value_text": row.value_text, "value_json": row.value_json,
            "source_app_id": row.source_app_id,
            "source_record_id": row.source_record_id,
            "version": int(row.source_revision), "locator": row.locator,
            "confidence": row.confidence, "is_current": bool(row.is_current),
            "invalid_reason": row.invalid_reason,
            "observed_at": _iso(row.observed_at), "flag": flag}


async def list_pending_links(limit: int = 100) -> list[dict]:
    """紐付け待ち（案件キーの無い出典）。出典レコードごとに 1 行（最新 revision）。
    候補は link_history の最新行。訂正フォーム用に紐付け版と出典 revision を添える。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(source_ingest).where(source_ingest.c.state == "held")
            .order_by(source_ingest.c.source_revision.desc(),
                      source_ingest.c.ingest_id.desc()))).fetchall()
        out = []
        seen = set()
        for r in rows:
            key = (r.source_app_id, r.source_record_id)
            if key in seen:
                continue
            seen.add(key)
            hist = await _latest_link_tx(session, r.source_app_id, r.source_record_id)
            rev = await _latest_revision(session, r.source_app_id, r.source_record_id)
            out.append({"source_app_id": r.source_app_id,
                        "source_record_id": r.source_record_id,
                        "revision": int(r.source_revision),
                        "source_revision": int(rev) if rev is not None else int(r.source_revision),
                        "link_version": int(hist.link_id) if hist is not None else 0,
                        "hold_reason": (hist.reason if hist else r.hold_reason) or "",
                        "candidates": (hist.candidates if hist else None) or [],
                        "last_checked_at": _iso(r.last_checked_at)})
            if len(out) >= limit:
                break
        return out


async def list_holds(limit: int = 100) -> list[dict]:
    """抽出結果の不一致・出典確認不能・関連喪失（利用停止中の出典）。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(source_ingest).where(sa.or_(
                source_ingest.c.state.in_(("mismatch_hold", "unavailable", "detached")),
                source_ingest.c.pending_recheck.is_(True)))
            .order_by(source_ingest.c.source_revision.desc(),
                      source_ingest.c.ingest_id.desc()))).fetchall()
        out = []
        seen = set()
        for r in rows:                       # 出典レコードごとに 1 行（最新 revision）
            key = (r.source_app_id, r.source_record_id)
            if key in seen:
                continue
            seen.add(key)
            out.append({"source_app_id": r.source_app_id,
                        "source_record_id": r.source_record_id,
                        "revision": int(r.source_revision), "state": r.state,
                        "case_app_id": r.case_app_id, "case_record_id": r.case_record_id,
                        "hold_reason": r.hold_reason or "",
                        "pending_recheck": bool(r.pending_recheck),
                        "last_checked_at": _iso(r.last_checked_at)})
            if len(out) >= limit:
                break
        return out


async def list_conflicts(limit: int = 100) -> list[dict]:
    """競合（§4-1・R9）: 有効な別観測（出典アプリ・レコード・locator・変換器の
    いずれかが異なる）間で、同じ案件・subject・項目の現在値が異なる。
    会話は case_event なので対象外。発送は subject=shipping:{No} で発送ごとに分かれる。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(case_fact).where(case_fact.c.is_current.is_(True),
                                       case_fact.c.invalid_reason.is_(None))
            .order_by(case_fact.c.case_app_id, case_fact.c.case_record_id,
                      case_fact.c.subject_id, case_fact.c.item_code,
                      case_fact.c.fact_id))).fetchall()
        unavailable = await _unavailable_sources(session)
        groups: dict = {}
        for r in rows:
            if (r.source_app_id, r.source_record_id) in unavailable:
                continue
            key = (r.case_app_id, r.case_record_id, r.subject_id, r.item_code)
            groups.setdefault(key, []).append(r)
        out = []
        for key, facts in groups.items():
            if len({f.value_text for f in facts}) < 2:
                continue
            observations = {(f.source_app_id, f.source_record_id, f.locator,
                             f.converter_name, f.converter_version) for f in facts}
            if len(observations) < 2:
                continue
            out.append({"case_app_id": key[0], "case_record_id": key[1],
                        "subject_id": key[2], "item_code": key[3],
                        "facts": [_fact_view(f) for f in facts]})
            if len(out) >= limit:
                break
        return out


async def _unavailable_sources(session) -> set:
    rows = (await session.execute(
        sa.select(source_ingest.c.source_app_id, source_ingest.c.source_record_id)
        .where(source_ingest.c.state == "unavailable").distinct())).fetchall()
    return {(r[0], r[1]) for r in rows}


async def list_recheck(limit: int = 100) -> list[dict]:
    """再確認が必要（§4-2）: 有効な確認（撤回されていない confirm/reject）が付いた
    fact が、現在値でなくなった（superseded／link_moved／link_lost／row_removed）／
    出典確認不能になったもの。"""
    async with session_scope() as session:
        confs = (await session.execute(
            sa.select(case_confirmation).order_by(case_confirmation.c.confirmation_id)
        )).fetchall()
        revoked = {int(c.revoked_of) for c in confs if c.revoked_of is not None}
        active = [c for c in confs if c.decision in ("confirm", "reject")
                  and int(c.confirmation_id) not in revoked]
        unavailable = await _unavailable_sources(session)
        out = []
        for c in active:
            fact = (await session.execute(sa.select(case_fact).where(
                case_fact.c.fact_id == c.fact_id))).first()
            if fact is None:
                continue
            reasons = []
            if not fact.is_current:
                reasons.append(fact.invalid_reason or INVALID_SUPERSEDED)
            if (fact.source_app_id, fact.source_record_id) in unavailable:
                reasons.append(FLAG_SOURCE_UNAVAILABLE)
            if not reasons:
                continue
            head = await _series_head(session, fact)
            out.append({"confirmation_id": int(c.confirmation_id),
                        "decision": c.decision, "decided_at": _iso(c.decided_at),
                        "fact": _fact_view(fact), "reasons": reasons,
                        "current": head})
            if len(out) >= limit:
                break
        return out


async def list_confirmations(fact_id: int) -> list[dict]:
    async with session_scope() as session:
        rows = (await session.execute(sa.select(case_confirmation).where(
            case_confirmation.c.fact_id == int(fact_id))
            .order_by(case_confirmation.c.confirmation_id))).fetchall()
        return [{"confirmation_id": int(r.confirmation_id), "fact_id": int(r.fact_id),
                 "fact_version": int(r.fact_version), "actor": r.actor,
                 "decided_at": _iso(r.decided_at), "decision": r.decision,
                 "reason": r.reason,
                 "revoked_of": int(r.revoked_of) if r.revoked_of is not None else None,
                 "operation_id": r.operation_id, "seen_version": int(r.seen_version)}
                for r in rows]


async def list_case_facts(case_app_id: str, case_record_id: str,
                          current_only: bool = True) -> list[dict]:
    """案件の事実。出典確認不能の出典に由来する fact は flag=source_unavailable で
    区別して返す（BA-07・確認/採用の対象外）。"""
    async with session_scope() as session:
        q = sa.select(case_fact).where(case_fact.c.case_app_id == str(case_app_id),
                                       case_fact.c.case_record_id == str(case_record_id))
        if current_only:
            q = q.where(case_fact.c.is_current.is_(True))
        rows = (await session.execute(q.order_by(case_fact.c.subject_id,
                                                 case_fact.c.item_code,
                                                 case_fact.c.fact_id))).fetchall()
        unavailable = await _unavailable_sources(session)
        return [_fact_view(r, FLAG_SOURCE_UNAVAILABLE
                           if (r.source_app_id, r.source_record_id) in unavailable
                           else None) for r in rows]


async def list_case_events(case_app_id: str, case_record_id: str,
                           current_only: bool = True) -> list[dict]:
    async with session_scope() as session:
        q = sa.select(case_event).where(
            case_event.c.case_app_id == str(case_app_id),
            case_event.c.case_record_id == str(case_record_id))
        if current_only:
            q = q.where(case_event.c.is_current.is_(True))
        rows = (await session.execute(q.order_by(case_event.c.event_id))).fetchall()
        return [{"event_id": int(r.event_id), "kind": r.kind, "summary": r.summary,
                 "occurred_at": _iso(r.occurred_at),
                 "source_app_id": r.source_app_id,
                 "source_record_id": r.source_record_id,
                 "version": int(r.source_revision), "is_current": bool(r.is_current),
                 "invalid_reason": r.invalid_reason} for r in rows]


# ── 同期の実行管理（§5） ────────────────────────────────────────────────────

def _cursor_view(row) -> dict:
    return {"target_app": row.target_app, "kind": row.kind,
            "cursor_updated_at": row.cursor_updated_at or "",
            "cursor_record_id": row.cursor_record_id or "",
            "confirmed_until": row.confirmed_until or "",
            "scan_upper_bound": row.scan_upper_bound or "",
            "page_position": row.page_position,
            "last_run_id": row.last_run_id, "state": row.state,
            "last_ok_at": _iso(row.last_ok_at),
            "last_reconcile_at": _iso(row.last_reconcile_at),
            "last_recheck_at": _iso(row.last_recheck_at),
            "recheck_started_at": _iso(row.recheck_started_at),
            "recheck_completed_at": _iso(row.recheck_completed_at),
            "pending_unregistered": int(row.pending_unregistered or 0),
            "first_unresolved_position": row.first_unresolved_position,
            "updated_at": _iso(row.updated_at)}


async def get_cursor(target_app: str, kind: str = CURSOR_KIND_SYNC) -> dict | None:
    if kind not in CURSOR_KINDS:
        raise LedgerError("cursor_kind_not_in_closed_set")
    async with session_scope() as session:
        row = (await session.execute(sa.select(sync_cursor).where(
            sync_cursor.c.target_app == target_app, sync_cursor.c.kind == kind))).first()
        if row is None:
            return None
        return _cursor_view(row)


async def set_cursor_state(target_app: str, state: str, *, kind: str = CURSOR_KIND_SYNC,
                           now=None, **extra) -> None:
    if state not in CURSOR_STATES:
        raise LedgerError("cursor_state_not_in_closed_set")
    if kind not in CURSOR_KINDS:
        raise LedgerError("cursor_kind_not_in_closed_set")
    now = now or _now()
    async with session_scope() as session:
        await _set_cursor(session, target_app, kind=kind, state=state, now=now, **extra)


async def _set_cursor(session, target_app: str, *, now, kind: str = CURSOR_KIND_SYNC,
                      **values) -> None:
    exists = (await session.execute(sa.select(sync_cursor.c.target_app).where(
        sync_cursor.c.target_app == target_app, sync_cursor.c.kind == kind))).first()
    values = dict(values, updated_at=now)
    if exists:
        await session.execute(sa.update(sync_cursor).where(
            sync_cursor.c.target_app == target_app, sync_cursor.c.kind == kind)
            .values(**values))
    else:
        values.setdefault("state", "incomplete")
        await session.execute(sa.insert(sync_cursor).values(
            target_app=target_app, kind=kind, **values))


async def start_run(target_app: str, scan_upper_bound: str, *, now=None) -> int:
    now = now or _now()
    async with session_scope() as session:
        result = await session.execute(sa.insert(sync_run).values(
            target_app=target_app, started_at=now, scan_upper_bound=scan_upper_bound,
            page_order="更新日時 asc, $id asc", status="running"))
        return int(result.inserted_primary_key[0])


async def finish_run(run_id: int, status: str, *, failure: str | None = None,
                     incomplete_page=None, pages_done: int = 0,
                     records_seen: int = 0, confirmed_range=None,
                     pending_unregistered: int = 0, first_unresolved=None,
                     now=None) -> None:
    if status not in RUN_STATES:
        raise LedgerError("run_state_not_in_closed_set")
    now = now or _now()
    async with session_scope() as session:
        await session.execute(sa.update(sync_run).where(sync_run.c.run_id == run_id)
                              .values(status=status, failure=failure,
                                      incomplete_page=incomplete_page,
                                      pages_done=pages_done, records_seen=records_seen,
                                      confirmed_range=confirmed_range,
                                      pending_unregistered=int(pending_unregistered),
                                      first_unresolved_position=first_unresolved,
                                      finished_at=now))


async def advance_cursor_with_page(target_app: str, run_id: int, *,
                                   ingests: list, cursor_updated_at: str,
                                   cursor_record_id: str, pages_done: int,
                                   records_seen: int, scan_upper_bound: str | None = None,
                                   pending_unregistered: int = 0, first_unresolved=None,
                                   now=None) -> list:
    """ページ単位の台帳書込とカーソル前進を**同一トランザクション**で行う。
    ingests は (SourceRef, case_key|None, facts, events[, extras]) のタプル列。
    失敗時はページ全体が巻き戻り、カーソルも進まない。page_position（再開位置・
    BA-09）と scan_upper_bound を同時に保持する。BA-20: 未解決件数と最初の未解決位置
    （{"updated_at","record_id"}）もページ確定時に cursor と run へ永続化する。
    戻り値は各取込の summary。"""
    now = now or _now()
    summaries = []
    try:
        async with session_scope() as session:
            for item in ingests:
                src, case_key, facts, events = item[0], item[1], item[2], item[3]
                extras = item[4] if len(item) > 4 else None
                s = _new_summary()
                await _ingest_record_tx(session, src, case_key, facts, events or [],
                                        extras, now, s)
                summaries.append(s)
            values = dict(cursor_updated_at=cursor_updated_at,
                          cursor_record_id=cursor_record_id, last_run_id=run_id,
                          state="incomplete",
                          page_position={"after_updated_at": cursor_updated_at,
                                         "after_record_id": cursor_record_id},
                          pending_unregistered=int(pending_unregistered),
                          first_unresolved_position=first_unresolved)
            if scan_upper_bound is not None:
                values["scan_upper_bound"] = scan_upper_bound
            await _set_cursor(session, target_app, now=now, **values)
            await session.execute(sa.update(sync_run).where(sync_run.c.run_id == run_id)
                                  .values(pages_done=pages_done, records_seen=records_seen,
                                          pending_unregistered=int(pending_unregistered),
                                          first_unresolved_position=first_unresolved))
    except MismatchError:
        # ページ全体を巻き戻したうえで、不一致の出典だけ保留として記録する
        # （どの出典かは summaries の長さで特定＝黙って捨てない）
        idx = len(summaries)
        src = ingests[idx][0] if idx < len(ingests) else None
        if src is not None:
            case_key = ingests[idx][1]
            async with session_scope() as session:
                await _upsert_ingest(session, src, state="mismatch_hold",
                                     case_key=case_key, hold_reason="mismatch", now=now)
        raise
    return summaries


async def sync_overview() -> dict:
    """同期状態の一覧（対象アプリ・kind ごとの cursor と直近 run）。"""
    async with session_scope() as session:
        cursors = (await session.execute(sa.select(sync_cursor).order_by(
            sync_cursor.c.target_app, sync_cursor.c.kind))).fetchall()
        runs = (await session.execute(sa.select(sync_run)
                                      .order_by(sync_run.c.run_id.desc()).limit(20))).fetchall()
        counts = (await session.execute(
            sa.select(source_ingest.c.state, sa.func.count())
            .group_by(source_ingest.c.state))).fetchall()
        pending = (await session.execute(
            sa.select(sa.func.count(sa.distinct(
                source_ingest.c.source_app_id + ":" + source_ingest.c.source_record_id)))
            .where(source_ingest.c.pending_recheck.is_(True)))).first()
        return {
            "pending_recheck": int(pending[0] or 0),
            # BA-17: 出典行の無い未解決（初見で判定不能）の件数＝同期カーソルの合計
            "pending_unregistered": sum(int(c.pending_unregistered or 0) for c in cursors
                                        if c.kind == CURSOR_KIND_SYNC),
            "cursors": [_cursor_view(c) for c in cursors],
            "runs": [{"run_id": int(r.run_id), "target_app": r.target_app,
                      "status": r.status, "failure": r.failure,
                      "started_at": _iso(r.started_at), "finished_at": _iso(r.finished_at),
                      "pages_done": int(r.pages_done or 0),
                      "records_seen": int(r.records_seen or 0),
                      "pending_unregistered": int(r.pending_unregistered or 0),
                      "first_unresolved_position": r.first_unresolved_position,
                      "incomplete_page": r.incomplete_page} for r in runs],
            "ingest_counts": {str(k): int(v) for k, v in counts},
        }


def _source_state_eval(ingest_row, cursor, *, check_cursor: bool,
                       has_current: bool = True) -> tuple:
    """1 出典の鮮度: (state, reason)。state は synced/partial/incomplete/error/stopped。"""
    if ingest_row.pending_recheck:
        return "partial", FLAG_PENDING_RECHECK              # R12: 関連は保持・鮮度は partial
    if ingest_row.state in ("unavailable", "mismatch_hold"):
        return "error", "source_" + ingest_row.state
    if ingest_row.state == "ingested" and not has_current:
        return "partial", REASON_RELINK_PENDING             # BA-11: 積み直し待ち
    if not check_cursor:
        return "synced", None
    if cursor is None:
        return "incomplete", "cursor_missing"
    if cursor.state == "stopped":
        return "stopped", "sync_stopped"
    if cursor.state == "error":
        return "incomplete", "cursor_error"        # 旧 confirmed_until で synced にしない
    if (cursor.confirmed_until and ingest_row.source_updated_at
            and ingest_row.source_updated_at <= cursor.confirmed_until):
        return "synced", None
    return "incomplete", "not_confirmed"


async def case_freshness_detail(case_app_id: str, case_record_id: str,
                                target_app: str, source_targets: dict | None = None) -> dict:
    """案件ごとの鮮度（§5・BA-07）: 案件に紐づく全出典（案件自身の出典＋紐付いた
    App 30/28 の出典）の状態を集約する。
    - 案件自身: stopped / error（出典確認不能・不一致）/ incomplete（未確認範囲・
      カーソル error）/ synced
    - 自身が synced でも、紐付いた出典のいずれかが error/unavailable なら partial
      （理由付き）、未確認なら incomplete
    source_targets: 出典アプリ ID → カーソル対象名。未指定は案件自身のみカーソル検査。
    戻り値: {"state": ..., "reasons": ["app30:5:source_unavailable", ...]}"""
    case_app_id, case_record_id = str(case_app_id), str(case_record_id)
    targets = dict(source_targets or {})
    targets.setdefault(case_app_id, target_app)
    async with session_scope() as session:
        cursor_rows = (await session.execute(sa.select(sync_cursor).where(
            sync_cursor.c.kind == CURSOR_KIND_SYNC))).fetchall()
        cursors = {c.target_app: c for c in cursor_rows}
        own = (await session.execute(sa.select(source_ingest).where(
            _source_where(source_ingest, case_app_id, case_record_id))
            .order_by(source_ingest.c.source_revision.desc()).limit(1))).first()
        own_cursor = cursors.get(target_app)
        if own_cursor is not None and own_cursor.state == "stopped":
            return {"state": "stopped", "reasons": ["sync_stopped"]}
        if own is None:
            own_state, own_reasons = "incomplete", ["no_source"]
        else:
            state, reason = _source_state_eval(own, own_cursor, check_cursor=True)
            own_state, own_reasons = state, ([reason] if reason else [])
        # 紐付いた出典の理由は案件自身の状態に関わらず併記する（何が未解決かを隠さない）
        linked = (await session.execute(sa.select(source_ingest).where(
            source_ingest.c.case_app_id == case_app_id,
            source_ingest.c.case_record_id == case_record_id)
            .order_by(source_ingest.c.source_app_id, source_ingest.c.source_record_id,
                      source_ingest.c.source_revision.desc()))).fetchall()
        reasons = []
        worst = "synced"
        seen = set()
        for r in linked:
            key = (r.source_app_id, r.source_record_id)
            if key in seen or key == (case_app_id, case_record_id):
                continue
            seen.add(key)
            tgt = targets.get(r.source_app_id)
            has_current = await _has_current(session, r.source_app_id, r.source_record_id)
            st, why = _source_state_eval(r, cursors.get(tgt) if tgt else None,
                                         check_cursor=tgt is not None,
                                         has_current=has_current)
            if st == "synced":
                continue
            tag = f"app{r.source_app_id}:{r.source_record_id}:{why}"
            reasons.append(tag)
            if st in ("error", "stopped", "partial"):
                worst = "partial"
            elif worst != "partial":
                worst = "incomplete"
        if own_state != "synced":
            return {"state": own_state, "reasons": own_reasons + reasons}
        return {"state": worst, "reasons": reasons}


async def case_freshness(case_app_id: str, case_record_id: str,
                         target_app: str, source_targets: dict | None = None) -> str:
    """案件ごとの鮮度（§5）: synced / partial / incomplete / error / stopped。"""
    return (await case_freshness_detail(case_app_id, case_record_id, target_app,
                                        source_targets))["state"]


def dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
