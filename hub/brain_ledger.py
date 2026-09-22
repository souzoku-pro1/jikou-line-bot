"""brain_ledger — BRAIN-A1-LEDGER-1: 案件脳の台帳（Postgres・独立 metadata）

正本: Desktop\\claude\\案件脳_設計_v3.md §4-1〜4-6（＋司令塔裁定 v3.1 R1〜R7）。
kintone が正本・台帳は索引と派生（§1）。本 module は kintone を import せず、
DB 以外へ何も出さない（logging 非 import＝PII の反射経路を構造的に持たない）。

表（§4）:
- case_fact          事実（出典・版・locator・変換器・確からしさ・supersedes）
- case_confirmation  確認（fact の版に固定・操作 ID 冪等・撤回は履歴）
- case_event         出来事（冪等キー一意）
- case_derivation    派生（A1 では空でよい）
- case_usage         利用記録（A1 では空でよい）
- source_ingest      取込の冪等キーと状態・案件紐付けの現在値
- link_history       出典→案件キーの紐付け履歴（候補一覧を含む）
- sync_run / sync_cursor  同期の実行と確認済み範囲

制約（§4-1・票の指定）:
- fact 一意キー = (案件キー, subject_id, 項目コード, 出典アプリ, 出典レコード,
  revision, locator, 変換器名, 変換器版)
- 取込の冪等キー（source_ingest）= (出典アプリ, 出典レコード, revision, locator,
  変換器名, 変換器版)
- locator は NOT NULL（不明部分は "-"）・空文字禁止
- supersedes は同表参照・自己参照禁止（CHECK）・分岐禁止（UNIQUE）・循環は
  アプリ側検査（_assert_no_cycle）
- case_event.idem_key 一意・case_confirmation.operation_id 一意

subject_id の規則（§4-1・R1/R2）: 案件キーの内側でのみ意味を持つ固定 ID。
  case / applicant（申述人・案件キー由来の固定 ID）/ decedent（被相続人）/
  creditor:{行ID} / document:{行ID}（サブテーブル行 ID・順番では識別しない）。
  行 ID の無い出典由来の subject は A1 では発生しない（自由記述は R2 で
  subject=case の文字列事実）。

現在値の規則: 同じ出典系列（出典アプリ・レコード・locator・変換器）の版更新
のみ supersedes で繋ぎ、現在値 = is_current（revision 順で決める・遅れて届いた
旧 revision は現在値にならない）。案件紐付けが変わったときは旧案件側の fact を
invalid_reason=link_moved で現在ビューから外し、新案件側へ新 fact を積む。
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
INGEST_STATES = ("ingested", "mismatch_hold", "unavailable", "held")
TRUST_LEVELS = ("auto", "candidate", "hold")
RUN_STATES = ("running", "ok", "failed", "stopped")
CURSOR_STATES = ("synced", "incomplete", "error", "stopped")
LOCATOR_UNKNOWN = "-"
LOCATOR_PARTS = 5          # 欄コード/行ID/添付識別子/添付の版/資料内位置
SOURCE_KIND_KINTONE = "kintone"
INVALID_LINK_MOVED = "link_moved"
INVALID_SUPERSEDED = "superseded"
HOLD_REASONS = ("ref_missing", "ref_not_digits", "unit_mismatch",
                "ref_other_app", "multiple_match", "conflict", "mismatch")

SUBJECT_CASE = "case"
SUBJECT_APPLICANT = "applicant"
SUBJECT_DECEDENT = "decedent"
SUBJECT_CREDITOR_PREFIX = "creditor:"
SUBJECT_DOCUMENT_PREFIX = "document:"

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
    sa.Index("ix_case_event_case", "case_app_id", "case_record_id"),
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
    sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.UniqueConstraint("source_app_id", "source_record_id", "source_revision",
                        "locator", "converter_name", "converter_version",
                        name="uq_source_ingest_key"),
    sa.CheckConstraint(
        "state IN ('ingested', 'mismatch_hold', 'unavailable', 'held')",
        name="ck_source_ingest_state"),
    sa.CheckConstraint("locator <> ''", name="ck_source_ingest_locator_nonempty"),
    sa.Index("ix_source_ingest_source", "source_app_id", "source_record_id"),
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
    sa.CheckConstraint("status IN ('running', 'ok', 'failed', 'stopped')",
                       name="ck_sync_run_status"),
)

sync_cursor = sa.Table(
    "sync_cursor", metadata,
    sa.Column("target_app", sa.Text, primary_key=True),
    sa.Column("cursor_updated_at", sa.Text, nullable=True),
    sa.Column("cursor_record_id", sa.Text, nullable=True),
    sa.Column("confirmed_until", sa.Text, nullable=True),
    sa.Column("last_run_id", _BIG, nullable=True),
    sa.Column("last_ok_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_reconcile_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_recheck_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("state", sa.Text, nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("state IN ('synced', 'incomplete', 'error', 'stopped')",
                       name="ck_sync_cursor_state"),
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
    """操作時に画面が見ていた版と現在の版が違う。"""

    def __init__(self, current: dict):
        super().__init__("version_conflict")
        self.current = current


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
    __slots__ = ("kind", "locator", "summary", "occurred_at")

    def __init__(self, kind: str, summary: str, locator: str = LOCATOR_UNKNOWN,
                 occurred_at=None):
        self.kind = kind
        self.locator = locator
        self.summary = summary
        self.occurred_at = occurred_at


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


async def known_revision(src: SourceRef) -> bool:
    """取込の冪等キーが既知か（revision 既知なら再処理しない・§5）。"""
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(source_ingest.c.ingest_id).where(_ingest_key_where(src))
        )).first()
        return row is not None


async def ingest_status(src: SourceRef, case_key: tuple | None) -> dict:
    """取込の要否判定に使う状態: known（冪等キー既知）・same_case（現在の紐付けが
    case_key と一致）・has_current_facts（その出典の現在 fact が案件側にある）。
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
            same = row.state == "held"
        has = (await session.execute(sa.select(case_fact.c.fact_id).where(
            case_fact.c.source_app_id == src.app_id,
            case_fact.c.source_record_id == src.record_id,
            case_fact.c.is_current.is_(True)).limit(1))).first() is not None
        return {"known": True, "same_case": same, "has_current_facts": has,
                "state": row.state}


async def latest_known_revision(source_app_id: str, source_record_id: str) -> int | None:
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(sa.func.max(source_ingest.c.source_revision)).where(
                source_ingest.c.source_app_id == str(source_app_id),
                source_ingest.c.source_record_id == str(source_record_id)))).first()
        return int(row[0]) if row and row[0] is not None else None


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


async def ingest_source(src: SourceRef, case_key: tuple | None, facts: list,
                        events: list | None = None, *, now=None) -> dict:
    """1 出典（レコード 1 revision）を台帳へ取り込む（1 トランザクション）。

    - case_key None（紐付け待ち）: 業務台帳へは入れず source_ingest=held のみ。
    - 同じ一意キーで値が違う → MismatchError を送出し、別トランザクションで
      source_ingest=mismatch_hold を記録（黙って捨てない）。
    - 出典系列の版更新は supersedes で繋ぎ、旧 revision の遅着は現在値にしない。
    - 案件紐付けの変化は旧 fact を link_moved で現在ビューから外し、新案件へ積む。
    戻り値: {"inserted": n, "skipped": n, "events": n, "state": ...}
    """
    now = now or _now()
    events = events or []
    summary = {"inserted": 0, "skipped": 0, "events": 0, "moved": 0}
    if case_key is None:
        async with session_scope() as session:
            await _upsert_ingest(session, src, state="held", case_key=None,
                                 hold_reason="pending_link", now=now)
        summary["state"] = "held"
        return summary
    case_app, case_rec = str(case_key[0]), str(case_key[1])
    try:
        async with session_scope() as session:
            for f in facts:
                await _ingest_one(session, src, case_app, case_rec, f, now, summary)
            for ev in events:
                idem = "|".join([src.app_id, src.record_id, str(src.revision),
                                 ev.kind, ev.locator])
                exists = (await session.execute(sa.select(case_event.c.event_id)
                                                .where(case_event.c.idem_key == idem))).first()
                if exists:
                    continue
                await session.execute(sa.insert(case_event).values(
                    idem_key=idem, case_app_id=case_app, case_record_id=case_rec,
                    kind=ev.kind, occurred_at=ev.occurred_at,
                    source_app_id=src.app_id, source_record_id=src.record_id,
                    source_revision=src.revision, locator=ev.locator,
                    summary=ev.summary))
                summary["events"] += 1
            await _upsert_ingest(session, src, state="ingested",
                                 case_key=(case_app, case_rec), hold_reason=None,
                                 now=now)
    except MismatchError:
        async with session_scope() as session:
            await _upsert_ingest(session, src, state="mismatch_hold",
                                 case_key=(case_app, case_rec),
                                 hold_reason="mismatch", now=now)
        summary["state"] = "mismatch_hold"
        return summary
    summary["state"] = "ingested"
    return summary


async def _ingest_one(session, src: SourceRef, case_app: str, case_rec: str,
                      f: FactIn, now, summary: dict) -> None:
    vtext, vjson = canonical_value(f.value_type, f.value)
    # 同一一意キーの既存行
    same_key = (await session.execute(sa.select(case_fact).where(
        case_fact.c.case_app_id == case_app, case_fact.c.case_record_id == case_rec,
        case_fact.c.subject_id == f.subject_id, case_fact.c.item_code == f.item_code,
        case_fact.c.source_app_id == src.app_id,
        case_fact.c.source_record_id == src.record_id,
        case_fact.c.source_revision == src.revision, case_fact.c.locator == f.locator,
        case_fact.c.converter_name == src.converter_name,
        case_fact.c.converter_version == src.converter_version))).first()
    if same_key is not None:
        if same_key.value_text != vtext:
            raise MismatchError("fact_value_mismatch")
        if not same_key.is_current:
            head_now = (await session.execute(sa.select(case_fact.c.fact_id).where(
                case_fact.c.source_app_id == src.app_id,
                case_fact.c.source_record_id == src.record_id,
                case_fact.c.locator == f.locator,
                case_fact.c.converter_name == src.converter_name,
                case_fact.c.converter_version == src.converter_version,
                case_fact.c.subject_id == f.subject_id,
                case_fact.c.item_code == f.item_code,
                case_fact.c.is_current.is_(True)))).first()
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
        case_fact.c.source_app_id == src.app_id,
        case_fact.c.source_record_id == src.record_id,
        case_fact.c.locator == f.locator,
        case_fact.c.converter_name == src.converter_name,
        case_fact.c.converter_version == src.converter_version,
        case_fact.c.subject_id == f.subject_id, case_fact.c.item_code == f.item_code,
        case_fact.c.is_current.is_(True)))).first()
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
    if head is None:
        if vtext == "" and vjson in (None, []):
            summary["skipped"] += 1          # 値なし・履歴も無し＝積まない
            return
        await session.execute(sa.insert(case_fact).values(
            case_app_id=case_app, case_record_id=case_rec, is_current=True, **base))
        summary["inserted"] += 1
        return
    moved = (head.case_app_id, head.case_record_id) != (case_app, case_rec)
    changed = head.value_text != vtext or moved
    if src.revision > head.source_revision:
        if not changed:
            summary["skipped"] += 1
            return
        await _assert_no_cycle(session, None, int(head.fact_id))
        await session.execute(sa.update(case_fact).where(
            case_fact.c.fact_id == head.fact_id).values(
            is_current=False, invalidated_at=now,
            invalid_reason=(INVALID_LINK_MOVED if moved else INVALID_SUPERSEDED)))
        await session.execute(sa.insert(case_fact).values(
            case_app_id=case_app, case_record_id=case_rec, is_current=True,
            supersedes_fact_id=int(head.fact_id),
            prev_case_app_id=head.case_app_id if moved else None,
            prev_case_record_id=head.case_record_id if moved else None, **base))
        summary["inserted"] += 1
        if moved:
            summary["moved"] += 1
        return
    # 旧 revision の遅着（現在値にしない・独立した観測として残す）
    if not changed:
        summary["skipped"] += 1
        return
    await session.execute(sa.insert(case_fact).values(
        case_app_id=case_app, case_record_id=case_rec, is_current=False, **base))
    summary["inserted"] += 1


async def mark_source_unavailable(source_app_id: str, source_record_id: str,
                                  *, now=None) -> int:
    """取得不能（権限・404）: 削除と推定せず「出典確認不能」として利用停止。
    fact は消さない・値なしにしない（source_ingest の状態だけを変える）。"""
    now = now or _now()
    async with session_scope() as session:
        result = await session.execute(sa.update(source_ingest).where(
            source_ingest.c.source_app_id == str(source_app_id),
            source_ingest.c.source_record_id == str(source_record_id)).values(
            state="unavailable", last_checked_at=now))
        return int(result.rowcount or 0)


async def mark_source_checked(source_app_id: str, source_record_id: str,
                              *, now=None) -> int:
    """再照合で出典が健在だったときの最終確認日時の更新（unavailable の解除）。"""
    now = now or _now()
    async with session_scope() as session:
        result = await session.execute(sa.update(source_ingest).where(
            source_ingest.c.source_app_id == str(source_app_id),
            source_ingest.c.source_record_id == str(source_record_id),
            source_ingest.c.state == "unavailable").values(
            state="ingested", last_checked_at=now))
        return int(result.rowcount or 0)


async def list_sources(source_app_id: str) -> list[dict]:
    """出典レコードごとの最新取込（追跡再照合・定期照合の対象一覧）。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(source_ingest.c.source_record_id,
                      sa.func.max(source_ingest.c.source_revision).label("rev"))
            .where(source_ingest.c.source_app_id == str(source_app_id))
            .group_by(source_ingest.c.source_record_id))).fetchall()
        return [{"record_id": r.source_record_id, "revision": int(r.rev)} for r in rows]


async def current_case_of_source(source_app_id: str, source_record_id: str) -> tuple | None:
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(source_ingest.c.case_app_id, source_ingest.c.case_record_id,
                      source_ingest.c.state)
            .where(source_ingest.c.source_app_id == str(source_app_id),
                   source_ingest.c.source_record_id == str(source_record_id))
            .order_by(source_ingest.c.source_revision.desc()).limit(1))).first()
        if row is None or row[0] is None:
            return None
        return (row[0], row[1])


async def find_case_by_item_value(item_code: str, value_text: str) -> list[tuple]:
    """現在値の一致で案件キーを引く（例: app40.LINEユーザーID）。重複なし。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(case_fact.c.case_app_id, case_fact.c.case_record_id)
            .where(case_fact.c.item_code == item_code,
                   case_fact.c.value_text == value_text,
                   case_fact.c.is_current.is_(True)).distinct())).fetchall()
        return [(r[0], r[1]) for r in rows]


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


async def latest_link(source_app_id: str, source_record_id: str) -> dict | None:
    async with session_scope() as session:
        row = (await session.execute(sa.select(link_history).where(
            link_history.c.source_app_id == str(source_app_id),
            link_history.c.source_record_id == str(source_record_id))
            .order_by(link_history.c.link_id.desc()).limit(1))).first()
        if row is None:
            return None
        return {"link_id": int(row.link_id), "trust_level": row.trust_level,
                "reason": row.reason, "actor": row.actor,
                "source_revision": (int(row.source_revision)
                                    if row.source_revision is not None else None),
                "new_case": ((row.new_case_app_id, row.new_case_record_id)
                             if row.new_case_app_id else None),
                "prev_case": ((row.prev_case_app_id, row.prev_case_record_id)
                              if row.prev_case_app_id else None),
                "candidates": row.candidates}


async def relink_source(*, source_app_id: str, source_record_id: str,
                        new_case: tuple, reason: str, operation_id: str,
                        actor: str, now=None) -> dict:
    """紐付け訂正（人の操作）: 履歴追加のみ。旧案件側の現在 fact は link_moved で
    現在ビューから外し（削除しない）、source_ingest の現在値を新案件へ。次回
    同期で新案件側の fact が積まれる（revision 既知でも案件が違えば再取込）。"""
    now = now or _now()
    prev = await current_case_of_source(source_app_id, source_record_id)
    pinned_rev = await latest_known_revision(source_app_id, source_record_id)
    new_case = (str(new_case[0]), str(new_case[1]))
    async with session_scope() as session:
        dup = (await session.execute(sa.select(link_history.c.link_id).where(
            link_history.c.operation_id == operation_id))).first()
        if dup:
            return {"duplicate": True, "link_id": int(dup[0])}
        moved = await session.execute(sa.update(case_fact).where(
            case_fact.c.source_app_id == str(source_app_id),
            case_fact.c.source_record_id == str(source_record_id),
            case_fact.c.is_current.is_(True)).values(
            is_current=False, invalid_reason=INVALID_LINK_MOVED, invalidated_at=now))
        await session.execute(sa.update(source_ingest).where(
            source_ingest.c.source_app_id == str(source_app_id),
            source_ingest.c.source_record_id == str(source_record_id)).values(
            case_app_id=new_case[0], case_record_id=new_case[1],
            state="ingested", hold_reason=None, last_checked_at=now))
        # 再取込を許すため、最新 revision の取込行を消さずに「案件違い」として扱う:
        # 次回同期は revision 既知でも案件キー不一致なら再処理する（brain_sync 側）
        result = await session.execute(sa.insert(link_history).values(
            source_app_id=str(source_app_id), source_record_id=str(source_record_id),
            prev_case_app_id=prev[0] if prev else None,
            prev_case_record_id=prev[1] if prev else None,
            new_case_app_id=new_case[0], new_case_record_id=new_case[1],
            trust_level="auto", reason=reason, operation_id=operation_id,
            actor=actor, source_revision=pinned_rev))
        return {"duplicate": False, "link_id": int(result.inserted_primary_key[0]),
                "moved_facts": int(moved.rowcount or 0), "prev": prev}


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


async def record_confirmation(*, fact_id: int, seen_version: int, decision: str,
                              reason: str, operation_id: str, actor: str,
                              revoked_of: int | None = None, now=None) -> dict:
    """確認/却下/撤回の記録（操作 ID 冪等・対象版一致・削除なし）。
    受付条件（§4-2）: 操作 ID 未使用、画面が見ていた版 = 現在の版。不一致は
    VersionConflict（最新を添える）。確認は当該 fact の版に固定（新版へ継承しない）。"""
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
            raise VersionConflict(head)
        if decision != "revoke" and head["fact_id"] != int(fact_id):
            raise VersionConflict(head)
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

def _fact_view(row) -> dict:
    return {"fact_id": int(row.fact_id), "case_app_id": row.case_app_id,
            "case_record_id": row.case_record_id, "subject_id": row.subject_id,
            "item_code": row.item_code, "value_type": row.value_type,
            "value_text": row.value_text, "value_json": row.value_json,
            "source_app_id": row.source_app_id,
            "source_record_id": row.source_record_id,
            "version": int(row.source_revision), "locator": row.locator,
            "confidence": row.confidence, "is_current": bool(row.is_current),
            "invalid_reason": row.invalid_reason,
            "observed_at": _iso(row.observed_at)}


async def list_pending_links(limit: int = 100) -> list[dict]:
    """紐付け待ち（案件キーの無い出典）。候補は link_history の最新行。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(source_ingest).where(source_ingest.c.state == "held")
            .order_by(source_ingest.c.ingest_id.desc()).limit(limit))).fetchall()
        out = []
        for r in rows:
            hist = (await session.execute(
                sa.select(link_history).where(
                    link_history.c.source_app_id == r.source_app_id,
                    link_history.c.source_record_id == r.source_record_id)
                .order_by(link_history.c.link_id.desc()).limit(1))).first()
            out.append({"source_app_id": r.source_app_id,
                        "source_record_id": r.source_record_id,
                        "revision": int(r.source_revision),
                        "hold_reason": (hist.reason if hist else r.hold_reason) or "",
                        "candidates": (hist.candidates if hist else None) or [],
                        "last_checked_at": _iso(r.last_checked_at)})
        return out


async def list_holds(limit: int = 100) -> list[dict]:
    """抽出結果の不一致・出典確認不能（利用停止中の出典）。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(source_ingest).where(
                source_ingest.c.state.in_(("mismatch_hold", "unavailable")))
            .order_by(source_ingest.c.ingest_id.desc()).limit(limit))).fetchall()
        return [{"source_app_id": r.source_app_id,
                 "source_record_id": r.source_record_id,
                 "revision": int(r.source_revision), "state": r.state,
                 "case_app_id": r.case_app_id, "case_record_id": r.case_record_id,
                 "last_checked_at": _iso(r.last_checked_at)} for r in rows]


async def list_conflicts(limit: int = 100) -> list[dict]:
    """競合（§4-1）: 有効な別出典間で同じ案件・subject・項目の現在値が異なる。"""
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
            sources = {(f.source_app_id, f.source_record_id, f.locator) for f in facts}
            if len(sources) < 2:
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
    fact が、現在値でなくなった／案件移動した／出典確認不能になったもの。"""
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
                reasons.append("source_unavailable")
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
    async with session_scope() as session:
        q = sa.select(case_fact).where(case_fact.c.case_app_id == str(case_app_id),
                                       case_fact.c.case_record_id == str(case_record_id))
        if current_only:
            q = q.where(case_fact.c.is_current.is_(True))
        rows = (await session.execute(q.order_by(case_fact.c.subject_id,
                                                 case_fact.c.item_code,
                                                 case_fact.c.fact_id))).fetchall()
        return [_fact_view(r) for r in rows]


async def list_case_events(case_app_id: str, case_record_id: str) -> list[dict]:
    async with session_scope() as session:
        rows = (await session.execute(sa.select(case_event).where(
            case_event.c.case_app_id == str(case_app_id),
            case_event.c.case_record_id == str(case_record_id))
            .order_by(case_event.c.event_id))).fetchall()
        return [{"event_id": int(r.event_id), "kind": r.kind, "summary": r.summary,
                 "occurred_at": _iso(r.occurred_at),
                 "source_app_id": r.source_app_id,
                 "source_record_id": r.source_record_id,
                 "version": int(r.source_revision)} for r in rows]


# ── 同期の実行管理（§5） ────────────────────────────────────────────────────

async def get_cursor(target_app: str) -> dict | None:
    async with session_scope() as session:
        row = (await session.execute(sa.select(sync_cursor).where(
            sync_cursor.c.target_app == target_app))).first()
        if row is None:
            return None
        return {"target_app": row.target_app,
                "cursor_updated_at": row.cursor_updated_at or "",
                "cursor_record_id": row.cursor_record_id or "",
                "confirmed_until": row.confirmed_until or "",
                "last_run_id": row.last_run_id, "state": row.state,
                "last_ok_at": _iso(row.last_ok_at),
                "last_reconcile_at": _iso(row.last_reconcile_at),
                "last_recheck_at": _iso(row.last_recheck_at),
                "updated_at": _iso(row.updated_at)}


async def set_cursor_state(target_app: str, state: str, *, now=None, **extra) -> None:
    if state not in CURSOR_STATES:
        raise LedgerError("cursor_state_not_in_closed_set")
    now = now or _now()
    async with session_scope() as session:
        await _set_cursor(session, target_app, state=state, now=now, **extra)


async def _set_cursor(session, target_app: str, *, now, **values) -> None:
    exists = (await session.execute(sa.select(sync_cursor.c.target_app).where(
        sync_cursor.c.target_app == target_app))).first()
    values = dict(values, updated_at=now)
    if exists:
        await session.execute(sa.update(sync_cursor).where(
            sync_cursor.c.target_app == target_app).values(**values))
    else:
        values.setdefault("state", "incomplete")
        await session.execute(sa.insert(sync_cursor).values(
            target_app=target_app, **values))


async def start_run(target_app: str, scan_upper_bound: str, *, now=None) -> int:
    now = now or _now()
    async with session_scope() as session:
        result = await session.execute(sa.insert(sync_run).values(
            target_app=target_app, started_at=now, scan_upper_bound=scan_upper_bound,
            page_order="更新日時 asc, $id asc", status="running"))
        return int(result.inserted_primary_key[0])


async def finish_run(run_id: int, status: str, *, failure: str | None = None,
                     incomplete_page=None, pages_done: int = 0,
                     records_seen: int = 0, confirmed_range=None, now=None) -> None:
    if status not in RUN_STATES:
        raise LedgerError("run_state_not_in_closed_set")
    now = now or _now()
    async with session_scope() as session:
        await session.execute(sa.update(sync_run).where(sync_run.c.run_id == run_id)
                              .values(status=status, failure=failure,
                                      incomplete_page=incomplete_page,
                                      pages_done=pages_done, records_seen=records_seen,
                                      confirmed_range=confirmed_range, finished_at=now))


async def advance_cursor_with_page(target_app: str, run_id: int, *,
                                   ingests: list, cursor_updated_at: str,
                                   cursor_record_id: str, pages_done: int,
                                   records_seen: int, now=None) -> list:
    """ページ単位の台帳書込とカーソル前進を**同一トランザクション**で行う。
    ingests は (SourceRef, case_key|None, facts, events) のタプル列。失敗時は
    ページ全体が巻き戻り、カーソルも進まない。戻り値は各取込の summary。"""
    now = now or _now()
    summaries = []
    try:
        async with session_scope() as session:
            for src, case_key, facts, events in ingests:
                s = {"inserted": 0, "skipped": 0, "events": 0, "moved": 0}
                if case_key is None:
                    await _upsert_ingest(session, src, state="held", case_key=None,
                                         hold_reason="pending_link", now=now)
                    s["state"] = "held"
                else:
                    case_app, case_rec = str(case_key[0]), str(case_key[1])
                    for f in facts:
                        await _ingest_one(session, src, case_app, case_rec, f, now, s)
                    for ev in (events or []):
                        idem = "|".join([src.app_id, src.record_id, str(src.revision),
                                         ev.kind, ev.locator])
                        exists = (await session.execute(
                            sa.select(case_event.c.event_id)
                            .where(case_event.c.idem_key == idem))).first()
                        if not exists:
                            await session.execute(sa.insert(case_event).values(
                                idem_key=idem, case_app_id=case_app,
                                case_record_id=case_rec, kind=ev.kind,
                                occurred_at=ev.occurred_at, source_app_id=src.app_id,
                                source_record_id=src.record_id,
                                source_revision=src.revision, locator=ev.locator,
                                summary=ev.summary))
                            s["events"] += 1
                    await _upsert_ingest(session, src, state="ingested",
                                         case_key=(case_app, case_rec),
                                         hold_reason=None, now=now)
                    s["state"] = "ingested"
                summaries.append(s)
            await _set_cursor(session, target_app, now=now,
                              cursor_updated_at=cursor_updated_at,
                              cursor_record_id=cursor_record_id,
                              last_run_id=run_id, state="incomplete")
            await session.execute(sa.update(sync_run).where(sync_run.c.run_id == run_id)
                                  .values(pages_done=pages_done, records_seen=records_seen))
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
    """同期状態の一覧（対象アプリごとの cursor と直近 run）。"""
    async with session_scope() as session:
        cursors = (await session.execute(sa.select(sync_cursor))).fetchall()
        runs = (await session.execute(sa.select(sync_run)
                                      .order_by(sync_run.c.run_id.desc()).limit(20))).fetchall()
        counts = (await session.execute(
            sa.select(source_ingest.c.state, sa.func.count())
            .group_by(source_ingest.c.state))).fetchall()
        return {
            "cursors": [{"target_app": c.target_app, "state": c.state,
                         "cursor_updated_at": c.cursor_updated_at or "",
                         "confirmed_until": c.confirmed_until or "",
                         "last_ok_at": _iso(c.last_ok_at),
                         "last_reconcile_at": _iso(c.last_reconcile_at),
                         "last_recheck_at": _iso(c.last_recheck_at)} for c in cursors],
            "runs": [{"run_id": int(r.run_id), "target_app": r.target_app,
                      "status": r.status, "failure": r.failure,
                      "started_at": _iso(r.started_at), "finished_at": _iso(r.finished_at),
                      "pages_done": int(r.pages_done or 0),
                      "records_seen": int(r.records_seen or 0),
                      "incomplete_page": r.incomplete_page} for r in runs],
            "ingest_counts": {str(k): int(v) for k, v in counts},
        }


async def case_freshness(case_app_id: str, case_record_id: str,
                         target_app: str) -> str:
    """案件ごとの鮮度（§5）: synced / incomplete / error / stopped。"""
    async with session_scope() as session:
        cur = (await session.execute(sa.select(sync_cursor).where(
            sync_cursor.c.target_app == target_app))).first()
        if cur is None:
            return "incomplete"
        if cur.state == "stopped":
            return "stopped"
        rows = (await session.execute(sa.select(source_ingest).where(
            source_ingest.c.source_app_id == str(case_app_id),
            source_ingest.c.source_record_id == str(case_record_id))
            .order_by(source_ingest.c.source_revision.desc()).limit(1))).first()
        if rows is None:
            return "incomplete"
        if rows.state in ("unavailable", "mismatch_hold"):
            return "error"                     # 出典側の障害（run の失敗は cursor に持つ）
        if (cur.confirmed_until and rows.source_updated_at
                and rows.source_updated_at <= cur.confirmed_until):
            return "synced"
        return "incomplete"


def dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
