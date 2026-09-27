"""brain_ledger — BRAIN-A1 / BRAIN-ID-1a: 案件脳の台帳（Postgres・独立 metadata）

正本: Desktop\\claude\\案件脳_設計_v3.md §4-1〜4-6（＋司令塔裁定 v3.1 R1〜R7・v3.2 R8〜R10・
v3.3 R11〜R12・v3.4 R13・v3.5 R14〜R15）と 案件脳_設計_v4.3.md §3-1・§3-2・§14
（R27〜R33: case 表・case_identity・case_id 基準）。kintone が正本・台帳は索引と派生
（§1）。本 module は kintone を import せず、DB 以外へ何も出さない（logging 非 import＝
PII の反射経路を構造的に持たない）。

表（§4・v4.3 §3-1）:
- case               案件本体（case_id・種別・登録状態・状態・統合先・作成経路・版）
- case_identity      識別子。案件識別子（kintone_record / receipt_number / drive_folder）は
                     同時に 1 案件だけ有効、関係者識別子（line_user）は複数案件可。
                     名前空間つき・有効期間（valid_from/valid_to）・削除しない
- merge_history / subject_merge_history  統合と subject 対応の履歴（1b で使う・M1 で作る）
- case_fact          事実（保存先 case_id・出典・版・locator・変換器・supersedes）
- case_confirmation  確認（fact の版に固定・操作 ID 冪等・撤回は履歴）
- case_event         出来事（冪等キー一意・case_id 内包・R9: App 28 の会話はここ）
- case_derivation    派生（A1 では空でよい）
- case_usage         利用記録（A1 では空でよい）
- source_ingest      取込の冪等キーと状態・案件紐付けの現在値（case_id・NULL 可）・
                     latest_seen_revision（R8）・line_user_id（R30・DB 内のみ）
- link_history       出典→案件の紐付け履歴（prev/new_case_id・候補一覧を含む）
- sync_run / sync_cursor  同期の実行と確認済み範囲（kind=sync/recheck・BA-05/09）

案件の識別（v4.3 §3-1・§14-5）: 主キー体系は case_id（脳固有の連番）。A1 の案件キー列
（case_app_id, case_record_id）は互換・履歴用に残す（未登録案件では NULL）。公開 API の
案件引数 `case` は case_id（int）・KintoneCaseKey・互換の (アプリ ID, レコード番号) の
いずれも受け、後者は kintone_record 識別子の検索として解決する。戻り値の案件は case_id。
M1 だけ適用した DB（case_id 列が NULL の A1 データ）では、案件キー一致の行も同じ案件
として読む（_case_match）＝挙動は A1 と同じ。

制約（§4-1・v4.3 §3-1）:
- fact 一意キー = (case_id, subject_id, 項目コード, 出典アプリ, 出典レコード,
  revision, locator, 変換器名, 変換器版)
- 取込の冪等キー（source_ingest）= (出典アプリ, 出典レコード, revision, locator,
  変換器名, 変換器版)
- 案件識別子の重なり禁止 = Postgres の EXCLUDE 制約（migration・dialect 条件付き）＋
  アプリ側検査（_assert_case_identity_free・同一 tx・FOR UPDATE）の 2 段（R29）
- locator は NOT NULL（不明部分は "-"）・空文字禁止
- supersedes は同表参照・自己参照禁止（CHECK）・分岐禁止（UNIQUE）・循環は
  アプリ側検査（_assert_no_cycle）
- case_event.idem_key 一意（case_id 内包＝event_idem_key）・case_confirmation.operation_id 一意

subject_id の規則（§4-1・R1/R2/R9）: 案件の内側でのみ意味を持つ固定 ID。
  case / applicant / decedent / creditor:{行ID} / document:{行ID} /
  shipping:{App 30 レコード番号}（R9: 発送ごとに別 subject）。

現在値の規則（R8）: 出典系列（出典アプリ・レコード・locator・変換器）ごとに
  latest_seen_revision を source_ingest に持ち、現在値 = 系列内で revision 最大の
  fact。同値の新版は fact を増やさず latest_seen_revision だけ進める。
  latest_seen_revision より小さい revision の入力は保存するが現在値に昇格させない。

関連喪失の共通処理（R10・_detach_source_tx）と訂正先への再投影（BA-11/16・_reproject_tx）
  は A1 のまま（v4.3 §14-1: R22 の規則は A1 の機構で物化する）。
"""

import datetime
import hashlib
import json
import os

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
# partial: 未解決あり（R12/BA-17）・stopped_stale: 回収された実行中 run（R28）
RUN_STATES = ("running", "ok", "failed", "stopped", "partial", "stopped_stale")
STALE_RUN_MINUTES = 15
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
# unregistered: kintone 未登録の案件（R31）
FRESHNESS_STATES = ("synced", "partial", "incomplete", "error", "stopped", "unregistered")
# 確認/却下を拒む理由（409 に添える固定語彙・BA-13）
CONFLICT_VERSION = "version_mismatch"
CONFLICT_NOT_CURRENT = "fact_not_current"
CONFLICT_SOURCE_DETACHED = "source_detached"
STALE_STATE = "stale"                          # R11: 遅着旧版は履歴として保存するだけ
# R15/BA-25: 紐付け訂正の対象閉集合（App 28 チャットログ・App 30 発送管理）。API・台帳関数が参照
RELINKABLE_APPS = frozenset({"28", "30"})

# ── 案件本体と識別子の語彙（v4.3 §3-1・§3-2） ──────────────────────────────
CASE_KIND_HOUKI = "souzoku_houki"              # 種別（相続放棄）
CASE_KINDS = (CASE_KIND_HOUKI,)
REGISTRATION_REGISTERED = "registered"
REGISTRATION_UNREGISTERED = "unregistered"
REGISTRATION_STATES = (REGISTRATION_REGISTERED, REGISTRATION_UNREGISTERED)
CASE_ACTIVE = "active"
CASE_MERGED = "merged"
CASE_STATES = (CASE_ACTIVE, CASE_MERGED)
CASE_CREATED_VIA = ("sync", "memo", "manual", "backfill")
IDENTITY_KINTONE_RECORD = "kintone_record"
IDENTITY_RECEIPT_NUMBER = "receipt_number"
IDENTITY_DRIVE_FOLDER = "drive_folder"
IDENTITY_LINE_USER = "line_user"
CASE_IDENTITY_KINDS = (IDENTITY_KINTONE_RECORD, IDENTITY_RECEIPT_NUMBER, IDENTITY_DRIVE_FOLDER)
RELATION_IDENTITY_KINDS = (IDENTITY_LINE_USER,)
IDENTITY_KINDS = CASE_IDENTITY_KINDS + RELATION_IDENTITY_KINDS
MERGE_STATES = ("active", "reverted")
IDENTITY_REASON_SYNC = "app40_sync"
IDENTITY_REASON_ENDED = "app40_field_changed"
IDENTITY_REASON_BACKFILL = "backfill"
IDENTITY_REASON_CREATED = "case_created"
KINTONE_NAMESPACE_PREFIX = "kintone:"
LINE_NAMESPACE_PREFIX = "line:"

SUBJECT_CASE = "case"
SUBJECT_APPLICANT = "applicant"
SUBJECT_DECEDENT = "decedent"
SUBJECT_CREDITOR_PREFIX = "creditor:"
SUBJECT_DOCUMENT_PREFIX = "document:"
SUBJECT_SHIPPING_PREFIX = "shipping:"


def kintone_namespace(subdomain: str, app_id: str) -> str:
    """案件識別子 kintone_record の名前空間（R29: "kintone:{subdomain}:{app_id}"）。
    文字列の生成責務は brain_sync／backfill にあり、本関数は形式の単一の正。"""
    return f"{KINTONE_NAMESPACE_PREFIX}{str(subdomain or '').strip()}:{str(app_id).strip()}"


def line_namespace(channel_id: str) -> str:
    """関係者識別子 line_user の名前空間（"line:{channel_id}"）。"""
    return f"{LINE_NAMESPACE_PREFIX}{str(channel_id or '').strip()}"


def app_id_of_namespace(namespace: str) -> str | None:
    """"kintone:{subdomain}:{app_id}" → app_id（互換の案件キー列の書込用）。"""
    parts = str(namespace or "").split(":")
    if len(parts) == 3 and parts[0] == "kintone" and parts[2]:
        return parts[2]
    return None


def _default_kintone_namespace(app_id: str) -> str:
    """互換の (アプリ ID, レコード番号) 入力に使う名前空間（KINTONE_SUBDOMAIN は env）。"""
    return kintone_namespace(os.environ.get("KINTONE_SUBDOMAIN", ""), app_id)


def event_idem_key(case_id: int, source_app_id: str, source_record_id: str,
                   revision_part: str, kind: str, locator: str) -> str:
    """case_event の冪等キー（R28: case_id 内包・6 部）。backfill が既存行を同じ関数で
    再計算する（決定的・一意性維持）。revision_part は revision の文字列か "-"
    （per_record＝会話は revision を含めない）。"""
    return "|".join([f"c{int(case_id)}", str(source_app_id), str(source_record_id),
                     str(revision_part), kind, locator])


def legacy_event_idem_key(case_app_id: str, case_record_id: str, source_app_id: str,
                          source_record_id: str, revision_part: str, kind: str,
                          locator: str) -> str:
    """A1 形式の冪等キー（7 部・案件キー内包）。M1 だけ適用した DB の既存行の照合と
    M2 の downgrade（A1 形へ戻す）にだけ使う。"""
    return "|".join([str(case_app_id), str(case_record_id), str(source_app_id),
                     str(source_record_id), str(revision_part), kind, locator])


def event_revision_part(idem_key: str) -> str:
    """冪等キー（新旧いずれの形式でも）から revision 部を取り出す（末尾 3 部は
    revision・kind・locator。kind・locator は "|" を含まない閉集合）。"""
    parts = str(idem_key).rsplit("|", 3)
    return parts[1] if len(parts) == 4 else LOCATOR_UNKNOWN


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
case = sa.Table(
    "case", metadata,
    sa.Column("case_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("kind", sa.Text, nullable=False, server_default=CASE_KIND_HOUKI),
    sa.Column("registration", sa.Text, nullable=False),
    sa.Column("status", sa.Text, nullable=False, server_default=CASE_ACTIVE),
    sa.Column("merged_into_case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=True),
    sa.Column("created_via", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("version", _BIG, nullable=False, server_default="1"),
    sa.CheckConstraint("registration IN ('registered', 'unregistered')",
                       name="ck_case_registration"),
    sa.CheckConstraint("status IN ('active', 'merged')", name="ck_case_status"),
    sa.CheckConstraint("created_via IN ('sync', 'memo', 'manual', 'backfill')",
                       name="ck_case_created_via"),
)

case_identity = sa.Table(
    "case_identity", metadata,
    sa.Column("identity_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
    sa.Column("namespace", sa.Text, nullable=False),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("value", sa.Text, nullable=False),
    sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
    sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
    sa.Column("reason", sa.Text, nullable=False, server_default=""),
    sa.Column("actor", sa.Text, nullable=False, server_default="system"),
    sa.Column("operation_id", sa.Text, nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.CheckConstraint(
        "kind IN ('kintone_record', 'receipt_number', 'drive_folder', 'line_user')",
        name="ck_case_identity_kind"),
    sa.CheckConstraint("value <> ''", name="ck_case_identity_value_nonempty"),
    sa.CheckConstraint("valid_to IS NULL OR valid_to >= valid_from",
                       name="ck_case_identity_period"),
    sa.Index("ix_case_identity_key", "namespace", "kind", "value"),
    sa.Index("ix_case_identity_case", "case_id"),
    # 案件識別子の有効期間の重なり禁止は Postgres の EXCLUDE 制約
    # ex_case_identity_active（migration M1・dialect 条件付き）＋アプリ側検査
    # _assert_case_identity_free（同一 tx・FOR UPDATE）の 2 段（R29）
)

merge_history = sa.Table(
    "merge_history", metadata,
    sa.Column("merge_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("from_case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
    sa.Column("into_case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
    sa.Column("status", sa.Text, nullable=False, server_default="active"),
    sa.Column("actor", sa.Text, nullable=False, server_default="system"),
    sa.Column("reason", sa.Text, nullable=False, server_default=""),
    sa.Column("operation_id", sa.Text, nullable=True, unique=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("reverted_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("revert_operation_id", sa.Text, nullable=True, unique=True),
    sa.CheckConstraint("status IN ('active', 'reverted')", name="ck_merge_history_status"),
    sa.CheckConstraint("from_case_id <> into_case_id", name="ck_merge_history_distinct"),
)

subject_merge_history = sa.Table(
    "subject_merge_history", metadata,
    sa.Column("subject_merge_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
    sa.Column("from_subject_id", sa.Text, nullable=False),
    sa.Column("into_subject_id", sa.Text, nullable=False),
    sa.Column("status", sa.Text, nullable=False, server_default="active"),
    sa.Column("actor", sa.Text, nullable=False, server_default="system"),
    sa.Column("reason", sa.Text, nullable=False, server_default=""),
    sa.Column("operation_id", sa.Text, nullable=True, unique=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.CheckConstraint("status IN ('active', 'reverted')",
                       name="ck_subject_merge_history_status"),
)

case_fact = sa.Table(
    "case_fact", metadata,
    sa.Column("fact_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
    sa.Column("case_app_id", sa.Text, nullable=True),        # 互換・履歴用（未登録は NULL）
    sa.Column("case_record_id", sa.Text, nullable=True),
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
    sa.Column("prev_case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=True),
    sa.UniqueConstraint("case_id", "subject_id", "item_code",
                        "source_app_id", "source_record_id", "source_revision",
                        "locator", "converter_name", "converter_version",
                        name="uq_case_fact_key"),
    sa.CheckConstraint("supersedes_fact_id IS NULL OR supersedes_fact_id <> fact_id",
                       name="ck_case_fact_no_self_supersede"),
    sa.CheckConstraint("confidence IN ('high', 'medium', 'low')",
                       name="ck_case_fact_confidence"),
    sa.CheckConstraint("locator <> ''", name="ck_case_fact_locator_nonempty"),
    sa.Index("ix_case_fact_case", "case_app_id", "case_record_id"),
    sa.Index("ix_case_fact_case_id", "case_id"),
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
    sa.Column("case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
    sa.Column("case_app_id", sa.Text, nullable=True),
    sa.Column("case_record_id", sa.Text, nullable=True),
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
    sa.Index("ix_case_event_case_id", "case_id"),
    sa.Index("ix_case_event_source", "source_app_id", "source_record_id"),
)

case_derivation = sa.Table(
    "case_derivation", metadata,
    sa.Column("derivation_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
    sa.Column("case_app_id", sa.Text, nullable=True),
    sa.Column("case_record_id", sa.Text, nullable=True),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("input_fact_versions", _JSON, nullable=False),
    sa.Column("calculator_name", sa.Text, nullable=False),
    sa.Column("calculator_version", sa.Text, nullable=False),
    sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("result", _JSON, nullable=True),
    sa.Column("needs_recalc", sa.Boolean, nullable=False, server_default=sa.false()),
    sa.Index("ix_case_derivation_case_id", "case_id"),
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
    sa.Column("case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=True),  # 紐付けの現在値
    sa.Column("case_app_id", sa.Text, nullable=True),
    sa.Column("case_record_id", sa.Text, nullable=True),
    sa.Column("hold_reason", sa.Text, nullable=True),
    # R30: App 28 出典の関係者識別子（DB 内のみ・ログ/通知/復唱に出さない）
    sa.Column("line_user_id", sa.Text, nullable=True),
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
    sa.Index("ix_source_ingest_case_id", "case_id"),
    sa.Index("ix_source_ingest_line_user", "line_user_id"),
)

link_history = sa.Table(
    "link_history", metadata,
    sa.Column("link_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("source_app_id", sa.Text, nullable=False),
    sa.Column("source_record_id", sa.Text, nullable=False),
    sa.Column("prev_case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=True),
    sa.Column("new_case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=True),
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
    sa.CheckConstraint(
        "status IN ('running', 'ok', 'failed', 'stopped', 'partial', 'stopped_stale')",
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

TABLE_NAMES = ("case", "case_identity", "merge_history", "subject_merge_history",
               "case_fact", "case_confirmation", "case_event", "case_derivation",
               "case_usage", "source_ingest", "link_history", "sync_run",
               "sync_cursor")
A1_TABLE_NAMES = ("case_fact", "case_confirmation", "case_event", "case_derivation",
                  "case_usage", "source_ingest", "link_history", "sync_run",
                  "sync_cursor")
# 案件キー列（互換）を持つ表と、その case_id の必須性（True = M2 で NOT NULL）
CASE_KEYED_TABLES = (("case_fact", True), ("case_event", True), ("case_derivation", True),
                     ("source_ingest", False))


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


class IdentityOverlap(LedgerError):
    """R29: 同じ (名前空間, 種別, 値) の案件識別子が既に別案件で有効（アプリ側検査）。"""

    def __init__(self):
        super().__init__("case_identity_overlap")
        self.reason = "case_identity_overlap"


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


# ── 案件の参照（case_id 基準・互換の案件キーを解決） ─────────────────────────

class KintoneCaseKey:
    """kintone レコードによる案件の指定（名前空間つき・R29: 文字列は呼び出し側が生成）。"""

    __slots__ = ("namespace", "app_id", "record_id")

    def __init__(self, namespace: str, app_id: str, record_id: str):
        self.namespace = str(namespace)
        self.app_id = str(app_id)
        self.record_id = str(record_id)


class CaseRef:
    """解決済みの案件参照。case_id（未作成の互換キーだけの案件は None）と互換の案件キー。"""

    __slots__ = ("case_id", "app", "rec", "registration", "status")

    def __init__(self, case_id: int | None, app: str | None = None, rec: str | None = None,
                 registration: str | None = None, status: str | None = None):
        self.case_id = int(case_id) if case_id is not None else None
        self.app = str(app) if app is not None else None
        self.rec = str(rec) if rec is not None else None
        self.registration = registration
        self.status = status

    @property
    def key(self) -> tuple | None:
        return (self.app, self.rec) if self.app is not None else None


def _case_input(case) -> tuple:
    """案件引数の正規化 → (case_id|None, KintoneCaseKey|None)。"""
    if case is None:
        return None, None
    if isinstance(case, CaseRef):
        if case.case_id is not None:
            return case.case_id, None
        if case.app is None:
            return None, None
        return None, KintoneCaseKey(_default_kintone_namespace(case.app), case.app, case.rec)
    if isinstance(case, bool):
        raise LedgerError("case_ref_malformed")
    if isinstance(case, int):
        return int(case), None
    if isinstance(case, KintoneCaseKey):
        return None, case
    if isinstance(case, (tuple, list)) and len(case) == 2:
        app, rec = str(case[0]), str(case[1])
        if not app or not rec:
            raise LedgerError("case_ref_malformed")
        return None, KintoneCaseKey(_default_kintone_namespace(app), app, rec)
    if isinstance(case, str) and case.isdigit():
        return int(case), None
    raise LedgerError("case_ref_malformed")


def _active_identity_where(namespace: str, kind: str, value: str):
    return sa.and_(case_identity.c.namespace == str(namespace),
                   case_identity.c.kind == kind,
                   case_identity.c.value == str(value),
                   case_identity.c.valid_to.is_(None))


async def _kintone_key_of_case(session, case_id: int) -> tuple | None:
    """案件の有効な kintone_record 識別子 → 互換の (アプリ ID, レコード番号)。"""
    row = (await session.execute(sa.select(case_identity).where(
        case_identity.c.case_id == int(case_id),
        case_identity.c.kind == IDENTITY_KINTONE_RECORD,
        case_identity.c.valid_to.is_(None)).order_by(case_identity.c.identity_id))).first()
    if row is None:
        return None
    app = app_id_of_namespace(row.namespace)
    return (app, row.value) if app else None


async def _case_row(session, case_id: int):
    return (await session.execute(sa.select(case).where(case.c.case_id == int(case_id)))).first()


async def _new_case_tx(session, *, registration: str, created_via: str, now,
                       kind: str = CASE_KIND_HOUKI) -> int:
    if registration not in REGISTRATION_STATES:
        raise LedgerError("registration_not_in_closed_set")
    if created_via not in CASE_CREATED_VIA:
        raise LedgerError("created_via_not_in_closed_set")
    if kind not in CASE_KINDS:
        raise LedgerError("case_kind_not_in_closed_set")
    result = await session.execute(sa.insert(case).values(
        kind=kind, registration=registration, status=CASE_ACTIVE, created_via=created_via,
        created_at=now, version=1))
    return int(result.inserted_primary_key[0])


async def _assert_case_identity_free(session, namespace: str, kind: str, value: str) -> None:
    """R29 のアプリ側検査: 同一 (名前空間, 種別, 値) の有効行を FOR UPDATE で取り、
    1 件でもあれば重なりとして拒否（同一 tx・Postgres の EXCLUDE 制約と 2 段）。"""
    rows = (await session.execute(sa.select(case_identity.c.identity_id).where(
        _active_identity_where(namespace, kind, value)).with_for_update())).fetchall()
    if rows:
        raise IdentityOverlap()


async def _add_identity_tx(session, case_id: int, namespace: str, kind: str, value: str,
                           *, now, reason: str, actor: str = "system",
                           operation_id: str | None = None) -> int:
    if kind not in IDENTITY_KINDS:
        raise LedgerError("identity_kind_not_in_closed_set")
    namespace, value = str(namespace or "").strip(), str(value or "").strip()
    if not namespace or not value:
        raise LedgerError("identity_malformed")
    if kind in CASE_IDENTITY_KINDS:
        await _assert_case_identity_free(session, namespace, kind, value)
    result = await session.execute(sa.insert(case_identity).values(
        case_id=int(case_id), namespace=namespace, kind=kind, value=value,
        valid_from=now, valid_to=None, reason=reason, actor=actor,
        operation_id=operation_id, created_at=now))
    return int(result.inserted_primary_key[0])


async def _ref_of_case_id(session, case_id: int, *, create: bool = False) -> CaseRef | None:
    """case_id → CaseRef。不明な case_id は読取（create=False）では None、書込（create=True）
    では LedgerError（存在しない案件へは何も積まない）。"""
    row = await _case_row(session, case_id)
    if row is None:
        if create:
            raise LedgerError("case_unknown")
        return None
    key = await _kintone_key_of_case(session, case_id)
    return CaseRef(int(row.case_id), key[0] if key else None, key[1] if key else None,
                   row.registration, row.status)


async def _resolve_case_tx(session, case, *, create: bool = False, now=None,
                           created_via: str = "sync") -> CaseRef | None:
    """案件引数 → CaseRef。create=True のとき kintone_record 識別子が無ければ登録済み
    案件として case と識別子を作る（同期は App 40 の新レコードを見つけたら必ず新しい
    case を起こす・v4.3 §3-3）。create=False で識別子が無い互換キーは case_id=None の
    CaseRef（M1 だけ適用した DB の A1 データを案件キーで読むため）。"""
    case_id, kkey = _case_input(case)
    if case_id is None and kkey is None:
        return None
    if case_id is not None:
        return await _ref_of_case_id(session, case_id, create=create)
    row = (await session.execute(sa.select(case_identity.c.case_id).where(
        _active_identity_where(kkey.namespace, IDENTITY_KINTONE_RECORD, kkey.record_id)))).first()
    if row is not None:
        crow = await _case_row(session, int(row[0]))
        return CaseRef(int(row[0]), kkey.app_id, kkey.record_id,
                       crow.registration if crow else None, crow.status if crow else None)
    if not create:
        return CaseRef(None, kkey.app_id, kkey.record_id)
    now = now or _now()
    new_id = await _new_case_tx(session, registration=REGISTRATION_REGISTERED,
                                created_via=created_via, now=now)
    await _add_identity_tx(session, new_id, kkey.namespace, IDENTITY_KINTONE_RECORD,
                           kkey.record_id, now=now, reason=IDENTITY_REASON_CREATED)
    return CaseRef(new_id, kkey.app_id, kkey.record_id, REGISTRATION_REGISTERED, CASE_ACTIVE)


async def _ref_from_row(session, row, *, create: bool = False, now=None) -> CaseRef | None:
    """行（case_id と互換の案件キー列を持つ）から CaseRef。"""
    if row is None:
        return None
    if row.case_id is not None:
        return await _ref_of_case_id(session, int(row.case_id), create=True)
    if row.case_app_id is None:
        return None
    return await _resolve_case_tx(session, (row.case_app_id, row.case_record_id),
                                  create=create, now=now)


def _case_match(table, ref: CaseRef | None):
    """表の行が案件 ref に属する条件。case_id 一致、または（M1 だけ適用した DB の
    A1 データ）case_id が NULL で互換の案件キーが一致。"""
    if ref is None:
        return sa.false()
    conds = []
    if ref.case_id is not None:
        conds.append(table.c.case_id == ref.case_id)
    if ref.app is not None:
        conds.append(sa.and_(table.c.case_id.is_(None),
                             table.c.case_app_id == ref.app,
                             table.c.case_record_id == ref.rec))
    return sa.or_(*conds) if conds else sa.false()


def _row_same_case(row, ref: CaseRef | None) -> bool:
    if ref is None or row is None:
        return False
    if row.case_id is not None and ref.case_id is not None:
        return int(row.case_id) == ref.case_id
    if row.case_id is None and ref.app is not None and row.case_app_id is not None:
        return (row.case_app_id, row.case_record_id) == (ref.app, ref.rec)
    return False


def _case_cols(ref: CaseRef | None, prefix: str = "") -> dict:
    """行へ書く案件列（case_id と互換の案件キー列）。"""
    if ref is None:
        return {prefix + "case_id": None, prefix + "case_app_id": None,
                prefix + "case_record_id": None}
    return {prefix + "case_id": ref.case_id, prefix + "case_app_id": ref.app,
            prefix + "case_record_id": ref.rec}


def _case_view(ref: CaseRef | None) -> dict:
    return {"case_id": ref.case_id if ref else None,
            "case_app_id": ref.app if ref else None,
            "case_record_id": ref.rec if ref else None}


# ── 案件・識別子の公開 API（v4.3 §3-1・§3-2・§14-3） ───────────────────────

async def ensure_case_for_record(namespace: str, app_id: str, record_id: str, *,
                                 created_via: str = "sync", now=None) -> int:
    """kintone レコードの案件を返す（無ければ登録済み案件として作る・冪等）。"""
    async with session_scope() as session:
        ref = await _resolve_case_tx(session, KintoneCaseKey(namespace, app_id, record_id),
                                     create=True, now=now, created_via=created_via)
        return ref.case_id


async def resolve_case(case) -> int | None:
    """識別子検索: case_id／KintoneCaseKey／互換の (アプリ ID, レコード番号) → case_id。
    無ければ None（作らない）。"""
    async with session_scope() as session:
        ref = await _resolve_case_tx(session, case, create=False)
        return ref.case_id if ref else None


async def case_info(case_id: int) -> dict | None:
    async with session_scope() as session:
        row = await _case_row(session, int(case_id))
        if row is None:
            return None
        key = await _kintone_key_of_case(session, int(case_id))
        return {"case_id": int(row.case_id), "kind": row.kind,
                "registration": row.registration, "status": row.status,
                "merged_into_case_id": (int(row.merged_into_case_id)
                                        if row.merged_into_case_id is not None else None),
                "created_via": row.created_via, "created_at": _iso(row.created_at),
                "version": int(row.version),
                "case_app_id": key[0] if key else None,
                "case_record_id": key[1] if key else None}


async def list_case_identities(case_id: int) -> list[dict]:
    """案件の識別子（案件識別子は値を返す。関係者識別子の値は返さない＝RV-10:
    line_user_id は DB 内のみ）。"""
    async with session_scope() as session:
        rows = (await session.execute(sa.select(case_identity).where(
            case_identity.c.case_id == int(case_id))
            .order_by(case_identity.c.identity_id))).fetchall()
        return [{"identity_id": int(r.identity_id), "namespace": r.namespace,
                 "kind": r.kind,
                 "value": r.value if r.kind in CASE_IDENTITY_KINDS else None,
                 "valid_from": _iso(r.valid_from), "valid_to": _iso(r.valid_to),
                 "reason": r.reason, "actor": r.actor} for r in rows]


async def active_cases_for_relation(namespace: str, kind: str, value: str) -> list[int]:
    """関係者識別子（例 line_user）が有効な案件（status=active）の case_id 一覧（昇順）。
    R29: 「有効な案件」は case_identity で決まる（R5 の identity 化）。"""
    if kind not in IDENTITY_KINDS:
        raise LedgerError("identity_kind_not_in_closed_set")
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(case_identity.c.case_id).join(
                case, case.c.case_id == case_identity.c.case_id)
            .where(_active_identity_where(namespace, kind, value),
                   case.c.status == CASE_ACTIVE)
            .distinct().order_by(case_identity.c.case_id))).fetchall()
        return [int(r[0]) for r in rows]


async def _sync_relation_identities_tx(session, case_id: int, namespace: str, kind: str,
                                       values: list, *, now, reason: str) -> dict:
    """App 40 の同期が関係者識別子を作成・更新する（R29）: 案件×名前空間×種別の
    有効行を values に揃える（無いものを追加・無くなったものは有効終了・削除しない）。"""
    if kind not in RELATION_IDENTITY_KINDS:
        raise LedgerError("identity_kind_not_in_closed_set")
    wanted = {str(v).strip() for v in values if str(v or "").strip()}
    rows = (await session.execute(sa.select(case_identity).where(
        case_identity.c.case_id == int(case_id),
        case_identity.c.namespace == str(namespace), case_identity.c.kind == kind,
        case_identity.c.valid_to.is_(None)))).fetchall()
    have = {r.value for r in rows}
    added = ended = 0
    for r in rows:
        if r.value not in wanted:
            await session.execute(sa.update(case_identity).where(
                case_identity.c.identity_id == r.identity_id).values(
                valid_to=now, reason=IDENTITY_REASON_ENDED))
            ended += 1
    for v in sorted(wanted - have):
        await _add_identity_tx(session, case_id, namespace, kind, v, now=now, reason=reason)
        added += 1
    return {"added": added, "ended": ended}


async def set_relation_identities(case_id: int, namespace: str, kind: str, values: list, *,
                                  reason: str = IDENTITY_REASON_SYNC, now=None) -> dict:
    now = now or _now()
    async with session_scope() as session:
        if await _case_row(session, int(case_id)) is None:
            raise LedgerError("case_unknown")
        return await _sync_relation_identities_tx(session, int(case_id), namespace, kind,
                                                  values, now=now, reason=reason)


async def add_case_identity(case_id: int, namespace: str, kind: str, value: str, *,
                            reason: str = "manual", actor: str = "system",
                            operation_id: str | None = None, now=None) -> int:
    """識別子の追加（案件識別子は重なり検査つき・R29）。1a では backfill とテストが使う。"""
    now = now or _now()
    async with session_scope() as session:
        if await _case_row(session, int(case_id)) is None:
            raise LedgerError("case_unknown")
        return await _add_identity_tx(session, int(case_id), namespace, kind, value,
                                      now=now, reason=reason, actor=actor,
                                      operation_id=operation_id)


async def count_running_runs() -> int:
    """状態 running の sync_run 件数（移行前提「実行中 run ゼロ」の確認・R28）。"""
    async with session_scope() as session:
        row = (await session.execute(sa.select(sa.func.count()).select_from(sync_run)
                                     .where(sync_run.c.status == "running"))).first()
        return int(row[0] or 0)


async def stop_stale_runs(older_than_minutes: int = STALE_RUN_MINUTES, *, now=None) -> int:
    """開始から older_than_minutes 以上経った running の run を stopped_stale で回収する
    （R28: BRAIN_SYNC_ENABLED=0 → 15 分待つ → stop_stale_runs → count=0 の順）。"""
    now = now or _now()
    threshold = now - datetime.timedelta(minutes=int(older_than_minutes))
    async with session_scope() as session:
        result = await session.execute(sa.update(sync_run).where(
            sync_run.c.status == "running", sync_run.c.started_at < threshold).values(
            status="stopped_stale", failure="stale_run", finished_at=now))
        return int(result.rowcount or 0)


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


async def ingest_status(src: SourceRef, case_key) -> dict:
    """取込の要否判定に使う状態: known（冪等キー既知）・same_case（現在の紐付けが
    case_key と一致）・has_current_facts（その出典の現在 fact/event が案件側にある）。
    既知でも案件が違う／現在 fact が無い（紐付け訂正後）なら再処理する。"""
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(source_ingest).where(_ingest_key_where(src)))).first()
        if row is None:
            return {"known": False, "same_case": False, "has_current_facts": False,
                    "state": None}
        ref = await _resolve_case_tx(session, case_key, create=False)
        if ref is None:
            same = row.state in ("held", "detached")
        else:
            same = _row_same_case(row, ref)
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


async def _upsert_ingest(session, src: SourceRef, *, state: str, ref: CaseRef | None,
                         hold_reason: str | None, now,
                         line_user_id: str | None = None) -> None:
    if state not in INGEST_STATES:
        raise LedgerError("ingest_state_not_in_closed_set")
    existing = (await session.execute(
        sa.select(source_ingest.c.ingest_id).where(_ingest_key_where(src)))).first()
    values = dict(state=state, hold_reason=hold_reason, source_updated_at=src.updated_at,
                  last_checked_at=now, **_case_cols(ref))
    if line_user_id:
        values["line_user_id"] = line_user_id     # R30: DB 内のみ
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


async def ingest_source(src: SourceRef, case_key, facts: list,
                        events: list | None = None, *, extras: dict | None = None,
                        now=None) -> dict:
    """1 出典（レコード 1 revision）を台帳へ取り込む（1 トランザクション）。

    - case_key None（紐付け待ち）: 業務台帳へは入れず source_ingest=held のみ
      （extras["ingest_state"]="detached" なら R5 不成立＝保留にもしない）。
    - case_key は case_id／KintoneCaseKey／互換の (アプリ ID, レコード番号)。識別子の
      無い kintone レコードは登録済み案件として case を起こす（同一トランザクション）。
    - 同じ一意キーで値が違う → MismatchError を送出し、別トランザクションで
      source_ingest=mismatch_hold を記録（黙って捨てない）。
    - 出典系列の版更新は supersedes で繋ぎ、latest_seen_revision より古い入力は
      現在値にしない（R8）。extras["link_change"] は R10 の共通処理を同一
      トランザクションで先に適用する。extras["subjects_seen"] は BA-03。
      extras["relation_identities"]={"namespace","kind","values"} は案件自身の出典
      （App 40）の関係者識別子の作成・更新（R29）。extras["line_user_id"] は R30。
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
            ref = await _resolve_case_tx(session, case_key, create=True, now=now)
            await _upsert_ingest(session, src, state="mismatch_hold",
                                 ref=ref, hold_reason="mismatch", now=now)
        summary["state"] = "mismatch_hold"
        return summary
    return summary


async def _latest_ingest_row(session, source_app_id: str, source_record_id: str):
    return (await session.execute(sa.select(source_ingest).where(
        _source_where(source_ingest, source_app_id, source_record_id))
        .order_by(source_ingest.c.source_revision.desc()).limit(1))).first()


async def _ingest_stale_tx(session, src: SourceRef, facts: list, now, s: dict,
                           extras: dict) -> None:
    """R11: 系列の latest_seen_revision より小さい入力は「履歴として保存するだけ」。
    現在の紐付け・fact/event の有効性・取込状態・link_history を一切変えない。
    fact は現在の紐付け先の案件に is_current=False（stale_revision）で残す。
    出来事は積まない。取込行は現在の行の状態・案件をそのまま写す。"""
    cur = await _latest_ingest_row(session, src.app_id, src.record_id)
    ref = await _ref_from_row(session, cur, create=True, now=now)
    if ref is not None:
        for f in facts:
            await _ingest_one(session, src, ref, f, now, s, False)
    # mismatch_hold は revision 単位の保留なので旧版の行には写さない（他は出典単位の状態）
    state = cur.state if cur is not None else "held"
    if state == "mismatch_hold":
        state = "ingested"
    await _upsert_ingest(session, src, state=state, ref=ref,
                         hold_reason=cur.hold_reason if cur is not None else "pending_link",
                         now=now, line_user_id=(extras or {}).get("line_user_id"))
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
        await _ingest_stale_tx(session, src, facts, now, s, extras)
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
    line_user_id = extras.get("line_user_id")
    ref = await _resolve_case_tx(session, case_key, create=True, now=now)
    if ref is None:
        state = extras.get("ingest_state") or "held"
        if state not in ("held", "detached"):
            raise LedgerError("ingest_state_not_in_closed_set")
        hold_reason = extras.get("hold_reason") or "pending_link"
        await _upsert_ingest(session, src, state=state, ref=None,
                             hold_reason=hold_reason, now=now, line_user_id=line_user_id)
        # R13: 有効な最新版の判定に成功した＝unavailable だった行も同じ状態へ復旧
        await session.execute(sa.update(source_ingest).where(
            _source_where(source_ingest, src.app_id, src.record_id),
            source_ingest.c.state == "unavailable").values(
            state=state, hold_reason=hold_reason, last_checked_at=now))
        s["state"] = state
        return
    for f in facts:
        await _ingest_one(session, src, ref, f, now, s, True)
    await _retire_removed_rows(session, src, ref, extras.get("subjects_seen"), now, s)
    for ev in events:
        await _ingest_event(session, src, ref, ev, s)
    rel = extras.get("relation_identities")
    if rel and ref.case_id is not None and (src.app_id, src.record_id) == (ref.app, ref.rec):
        # R29: 案件自身の出典（App 40）の同期が関係者識別子を作成・更新する
        await _sync_relation_identities_tx(session, ref.case_id, rel["namespace"],
                                           rel["kind"], rel.get("values") or [],
                                           now=now, reason=IDENTITY_REASON_SYNC)
    await _upsert_ingest(session, src, state="ingested", ref=ref,
                         hold_reason=None, now=now, line_user_id=line_user_id)
    # 案件紐付けの現在値は出典単位: 以前 held/detached だった旧 revision の行も追随。
    # R13/BA-14: 有効な最新版の取込に成功した＝unavailable もここで ingested へ復旧する
    await session.execute(sa.update(source_ingest).where(
        _source_where(source_ingest, src.app_id, src.record_id),
        source_ingest.c.state.in_(("held", "detached", "unavailable"))).values(
        state="ingested", hold_reason=None, last_checked_at=now, **_case_cols(ref)))
    s["state"] = "ingested"


def _event_idem(ref: CaseRef, src: SourceRef, ev: EventIn) -> str:
    rev = LOCATOR_UNKNOWN if ev.per_record else str(src.revision)
    return event_idem_key(ref.case_id, src.app_id, src.record_id, rev, ev.kind, ev.locator)


def _legacy_event_idem(ref: CaseRef, src: SourceRef, ev: EventIn) -> str | None:
    if ref.app is None:
        return None
    rev = LOCATOR_UNKNOWN if ev.per_record else str(src.revision)
    return legacy_event_idem_key(ref.app, ref.rec, src.app_id, src.record_id, rev,
                                 ev.kind, ev.locator)


async def _ingest_event(session, src: SourceRef, ref: CaseRef, ev: EventIn, s: dict) -> None:
    idem = _event_idem(ref, src, ev)
    exists = (await session.execute(sa.select(case_event).where(
        case_event.c.idem_key == idem))).first()
    if exists is None:
        # M1 だけ適用した DB: A1 形式の冪等キー（案件キー内包）の同じ出来事があれば
        # それを同一とみなす（backfill が新形式へ書き換えるまで重複を作らない）
        legacy = _legacy_event_idem(ref, src, ev)
        if legacy is not None:
            exists = (await session.execute(sa.select(case_event).where(
                case_event.c.idem_key == legacy, case_event.c.case_id.is_(None)))).first()
    if exists is not None:
        if not exists.is_current:
            # 関連喪失で外れた同じ出来事が同じ案件へ戻った（復帰＝状態変更・削除なし）
            await session.execute(sa.update(case_event).where(
                case_event.c.event_id == exists.event_id).values(
                is_current=True, invalid_reason=None, invalidated_at=None))
            s["events"] += 1
        return
    await session.execute(sa.insert(case_event).values(
        idem_key=idem, kind=ev.kind, occurred_at=ev.occurred_at,
        source_app_id=src.app_id, source_record_id=src.record_id,
        source_revision=src.revision, locator=ev.locator, summary=ev.summary,
        is_current=True, **_case_cols(ref)))
    s["events"] += 1


def _series_fact_where(src: SourceRef, f: FactIn):
    return sa.and_(case_fact.c.source_app_id == src.app_id,
                   case_fact.c.source_record_id == src.record_id,
                   case_fact.c.locator == f.locator,
                   case_fact.c.converter_name == src.converter_name,
                   case_fact.c.converter_version == src.converter_version,
                   case_fact.c.subject_id == f.subject_id,
                   case_fact.c.item_code == f.item_code)


async def _retired_predecessor(session, src: SourceRef, f: FactIn, ref: CaseRef) -> tuple:
    """関連喪失・行消去で外れた系列の最後の fact。戻り値 (same_case_pred, other_case_pred)。
    supersedes は**同じ案件**の系列にだけ張る（BA-16: 案件間の往復で UNIQUE(supersedes)
    に触れない・履歴は案件ごとに一直線）。別案件の直前行は prev_case の情報にだけ使う。"""
    rows = (await session.execute(sa.select(case_fact).where(
        _series_fact_where(src, f), case_fact.c.is_current.is_(False),
        case_fact.c.invalid_reason.in_(RETIRED_REASONS))
        .order_by(case_fact.c.fact_id.desc()))).fetchall()
    same = other = None
    for row in rows:
        if _row_same_case(row, ref):
            if same is None:
                has_successor = (await session.execute(sa.select(case_fact.c.fact_id).where(
                    case_fact.c.supersedes_fact_id == row.fact_id))).first()
                if has_successor is None:
                    same = row
        elif other is None:
            other = row
    return same, other


def _prev_cols(row) -> dict:
    """旧案件の情報（案件間の移動の出所・BA-16）。"""
    if row is None:
        return {"prev_case_id": None, "prev_case_app_id": None, "prev_case_record_id": None}
    return {"prev_case_id": int(row.case_id) if row.case_id is not None else None,
            "prev_case_app_id": row.case_app_id, "prev_case_record_id": row.case_record_id}


async def _ingest_one(session, src: SourceRef, ref: CaseRef, f: FactIn, now,
                      summary: dict, newest: bool) -> None:
    vtext, vjson = canonical_value(f.value_type, f.value)
    # 同一一意キーの既存行
    same_key = (await session.execute(sa.select(case_fact).where(
        _case_match(case_fact, ref), _series_fact_where(src, f),
        case_fact.c.source_revision == src.revision))).first()
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
                confidence=f.confidence, **_case_cols(ref))
    empty = vtext == "" and vjson in (None, [])
    if head is None:
        if not newest:
            # R8: latest_seen_revision より古い入力は保存するが現在値にしない
            if empty:
                summary["skipped"] += 1
                return
            await session.execute(sa.insert(case_fact).values(
                is_current=False, invalid_reason=INVALID_STALE, **base))
            summary["inserted"] += 1
            return
        same, other = await _retired_predecessor(session, src, f, ref)
        if same is None and other is None:
            if empty:
                summary["skipped"] += 1          # 値なし・履歴も無し＝積まない
                return
            await session.execute(sa.insert(case_fact).values(is_current=True, **base))
            summary["inserted"] += 1
            return
        # 関連喪失・行消去の後の復帰: 同じ案件の直前行にだけ supersedes を張る（一直線）。
        # 別案件から戻ってきた場合は prev_case を残す（supersedes は張らない・BA-16）
        if same is not None:
            await _assert_no_cycle(session, None, int(same.fact_id))
        await session.execute(sa.insert(case_fact).values(
            is_current=True,
            supersedes_fact_id=int(same.fact_id) if same is not None else None,
            **_prev_cols(other if same is None else None), **base))
        summary["inserted"] += 1
        if same is None:
            summary["moved"] += 1
        return
    moved = not _row_same_case(head, ref)
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
            is_current=True, supersedes_fact_id=None if moved else int(head.fact_id),
            **_prev_cols(head if moved else None), **base))
        summary["inserted"] += 1
        if moved:
            summary["moved"] += 1
        return
    # 旧 revision の遅着（現在値にしない・独立した観測として残す・R8）
    if not changed or empty:
        summary["skipped"] += 1
        return
    await session.execute(sa.insert(case_fact).values(
        is_current=False, invalid_reason=INVALID_STALE, **base))
    summary["inserted"] += 1


async def _retire_removed_rows(session, src: SourceRef, ref: CaseRef,
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
            _case_match(case_fact, ref),
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

async def _current_case_ref_tx(session, source_app_id: str, source_record_id: str,
                               *, create: bool = False, now=None) -> CaseRef | None:
    row = (await session.execute(
        sa.select(source_ingest.c.case_id, source_ingest.c.case_app_id,
                  source_ingest.c.case_record_id)
        .where(_source_where(source_ingest, source_app_id, source_record_id))
        .order_by(source_ingest.c.source_revision.desc()).limit(1))).first()
    return await _ref_from_row(session, row, create=create, now=now)


async def _detach_source_tx(session, source_app_id: str, source_record_id: str, *,
                            new_case, reason: str, trust: str,
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
    prev = await _current_case_ref_tx(session, source_app_id, source_record_id,
                                      create=True, now=now)
    new_ref = await _resolve_case_tx(session, new_case, create=True, now=now)
    reason_code = INVALID_LINK_MOVED if new_ref is not None else INVALID_LINK_LOST
    link = await session.execute(sa.insert(link_history).values(
        source_app_id=source_app_id, source_record_id=source_record_id,
        trust_level=trust, reason=reason, candidates=candidates or None,
        operation_id=operation_id, actor=actor, source_revision=source_revision,
        **_case_cols(prev, "prev_"), **_case_cols(new_ref, "new_")))
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
        state=sa.case((source_ingest.c.state.in_(("mismatch_hold", "unavailable")),
                       source_ingest.c.state),
                      else_="ingested" if new_ref is not None else ingest_state),
        hold_reason=None if new_ref is not None else reason, last_checked_at=now,
        **_case_cols(new_ref)))
    return {"link_id": int(link.inserted_primary_key[0]),
            "prev": prev.case_id if prev else None,
            "prev_case": prev.key if prev else None,
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
        if r.case_id is not None or r.case_app_id is not None:
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
                               *, revision: int | None = None, now=None,
                               line_user_id: str | None = None) -> dict:
    """R13/BA-19: 有効な最新版の照合が成功した（取込は不要）ときの復旧を**1 トランザクション**で:
    pending_recheck の解除と unavailable の解除（復帰先は _restore_unavailable_tx）。
    BA-24: revision を渡すと、照合した最新 revision へ latest_seen_revision を同じ
    トランザクションで前進させる（下がることはない・関連解除の状態は変えない＝復活させない）。
    R30: line_user_id を渡すと App 28 出典の取込行に埋める（DB 内のみ）。
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
        if line_user_id:
            await session.execute(sa.update(source_ingest).where(
                _source_where(source_ingest, source_app_id, source_record_id),
                sa.or_(source_ingest.c.line_user_id.is_(None),
                       source_ingest.c.line_user_id != line_user_id)).values(
                line_user_id=line_user_id))
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


async def current_case_of_source(source_app_id: str, source_record_id: str) -> int | None:
    """出典の現在の紐付け先（case_id）。未紐付け・未取込は None。
    M1 だけ適用した DB の A1 データ（案件キーだけで case が無い）は、登録済み案件として
    case と kintone_record 識別子を起こして返す（backfill と同じ結果・冪等。同期の移動判定が
    案件キーの紐付けを見落とさないため）。"""
    async with session_scope() as session:
        ref = await _current_case_ref_tx(session, source_app_id, source_record_id, create=True)
        return ref.case_id if ref else None


async def current_case_key_of_source(source_app_id: str, source_record_id: str) -> tuple | None:
    """互換: 出典の現在の紐付け先の案件キー（登録済み案件のみ・未登録は None）。"""
    async with session_scope() as session:
        ref = await _current_case_ref_tx(session, source_app_id, source_record_id)
        return ref.key if ref else None


async def find_case_by_item_value(item_code: str, value_text: str) -> list[int]:
    """現在値の一致で案件（case_id）を引く（例 app40.LINEユーザーID）。重複なし・昇順。
    R5 の判定は case_identity（active_cases_for_relation）で行う（R29）。本関数は参照用。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(case_fact.c.case_id, case_fact.c.case_app_id,
                      case_fact.c.case_record_id)
            .where(case_fact.c.item_code == item_code,
                   case_fact.c.value_text == value_text,
                   case_fact.c.is_current.is_(True)).distinct())).fetchall()
        out = set()
        for r in rows:
            ref = await _ref_from_row(session, r, create=True)
            if ref is not None and ref.case_id is not None:
                out.add(ref.case_id)
        return sorted(out)


async def case_exists(case) -> bool:
    """案件が台帳に存在するか（case_id は case 表・互換の案件キーは識別子または
    A1 データ〔自身の取込行 or 案件キーを持つ fact〕）。"""
    async with session_scope() as session:
        return await _case_exists_tx(session, case)


async def _legacy_case_rows_exist(session, app: str, rec: str) -> bool:
    own = (await session.execute(sa.select(source_ingest.c.ingest_id).where(
        _source_where(source_ingest, app, rec)).limit(1))).first()
    if own is not None:
        return True
    fact = (await session.execute(sa.select(case_fact.c.fact_id).where(
        case_fact.c.case_app_id == str(app),
        case_fact.c.case_record_id == str(rec)).limit(1))).first()
    return fact is not None


async def _case_exists_tx(session, case) -> bool:
    case_id, kkey = _case_input(case)
    if case_id is not None:
        return await _case_row(session, case_id) is not None
    if kkey is None:
        return False
    ref = await _resolve_case_tx(session, kkey, create=False)
    if ref is not None and ref.case_id is not None:
        return True
    return await _legacy_case_rows_exist(session, kkey.app_id, kkey.record_id)


# ── 紐付け履歴（§3・§4-6） ──────────────────────────────────────────────────

async def add_link_history(*, source_app_id: str, source_record_id: str,
                           prev_case, new_case, trust_level: str, reason: str,
                           candidates=None, operation_id: str | None = None,
                           actor: str = "system") -> int:
    if trust_level not in TRUST_LEVELS:
        raise LedgerError("trust_level_not_in_closed_set")
    async with session_scope() as session:
        if operation_id:
            dup = (await session.execute(sa.select(link_history.c.link_id).where(
                link_history.c.operation_id == operation_id))).first()
            if dup:
                return int(dup[0])
        prev_ref = await _resolve_case_tx(session, prev_case, create=False)
        new_ref = await _resolve_case_tx(session, new_case, create=False)
        result = await session.execute(sa.insert(link_history).values(
            source_app_id=str(source_app_id), source_record_id=str(source_record_id),
            trust_level=trust_level, reason=reason, candidates=candidates,
            operation_id=operation_id, actor=actor,
            **_case_cols(prev_ref, "prev_"), **_case_cols(new_ref, "new_")))
        return int(result.inserted_primary_key[0])


def _link_view(row) -> dict:
    return {"link_id": int(row.link_id), "trust_level": row.trust_level,
            "reason": row.reason, "actor": row.actor,
            "source_revision": (int(row.source_revision)
                                if row.source_revision is not None else None),
            "new_case_id": int(row.new_case_id) if row.new_case_id is not None else None,
            "prev_case_id": int(row.prev_case_id) if row.prev_case_id is not None else None,
            "new_case": ((row.new_case_app_id, row.new_case_record_id)
                         if row.new_case_app_id else None),
            "prev_case": ((row.prev_case_app_id, row.prev_case_record_id)
                          if row.prev_case_app_id else None),
            "candidates": row.candidates, "created_at": _iso(row.created_at)}


async def _latest_link_tx(session, source_app_id: str, source_record_id: str):
    return (await session.execute(sa.select(link_history).where(
        _source_where(link_history, source_app_id, source_record_id))
        .order_by(link_history.c.link_id.desc()).limit(1))).first()


async def _link_view_tx(session, row) -> dict:
    """_link_view に加えて、case_id 列が NULL で互換の案件キーだけの行（M1 だけ適用した DB の
    A1 データ）は登録済み案件として解決した case_id を添える（手動訂正の優先判定・判定の
    繰り返し検出が案件キーの紐付けを見落とさないため）。"""
    view = _link_view(row)
    for prefix in ("prev_", "new_"):
        if view[prefix + "case_id"] is None and view[prefix + "case"] is not None:
            ref = await _resolve_case_tx(session, view[prefix + "case"], create=True)
            view[prefix + "case_id"] = ref.case_id if ref else None
    return view


async def latest_link(source_app_id: str, source_record_id: str) -> dict | None:
    async with session_scope() as session:
        row = await _latest_link_tx(session, source_app_id, source_record_id)
        return await _link_view_tx(session, row) if row is not None else None


async def link_version(source_app_id: str, source_record_id: str) -> int:
    """紐付け訂正の版（画面が見る値）: 最新 link_history の link_id（無ければ 0）。"""
    async with session_scope() as session:
        row = await _latest_link_tx(session, source_app_id, source_record_id)
        return int(row.link_id) if row is not None else 0


async def relink_source(*, source_app_id: str, source_record_id: str,
                        new_case, reason: str, operation_id: str,
                        actor: str, seen_link_version: int,
                        seen_source_revision: int, now=None) -> dict:
    """紐付け訂正（人の操作・BA-06）: R10 の共通処理を通す履歴追加のみ。
    受付条件（同一トランザクションで照合）: 操作 ID 未使用、画面が見ていた
    紐付け版（最新 link_id）と出典 revision が現在値と一致（不一致は
    VersionConflict に最新を添える）、訂正先が台帳に存在する案件（case_id・R31:
    未登録案件でもよい。互換の案件キーは識別子または A1 データで解決。
    LedgerError: relink_target_unknown／relink_target_not_active）。"""
    now = now or _now()
    source_app_id, source_record_id = str(source_app_id), str(source_record_id)
    # R15/BA-25: 訂正対象は許可集合 RELINKABLE_APPS（App 28・App 30）だけ。許可集合外は
    # ここで拒否。案件アプリ（訂正先と同じアプリ）・台帳に案件アプリとして現れているアプリの
    # 判定は二重の安全として残す
    if source_app_id not in RELINKABLE_APPS:
        raise NotRelinkable()
    async with session_scope() as session:
        target = await _resolve_case_tx(session, new_case, create=False)
        if target is None:
            raise LedgerError("relink_target_unknown")
        if target.case_id is None:
            # 互換: 識別子の無い A1 データの案件キー → 台帳に痕跡があれば案件を起こす
            if not await _legacy_case_rows_exist(session, target.app, target.rec):
                raise LedgerError("relink_target_unknown")
            target = await _resolve_case_tx(session, new_case, create=True, now=now)
        if target.status is not None and target.status != CASE_ACTIVE:
            raise LedgerError("relink_target_not_active")
        if target.app is not None and source_app_id == target.app:
            raise NotRelinkable()
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
        # BA-11: 旧案件側の現在 fact/event を先に取り、無効化と同じトランザクションで
        # 移動先へ積み直す（履歴を繋ぐ・削除しない）
        live_facts = (await session.execute(sa.select(case_fact).where(
            _source_where(case_fact, source_app_id, source_record_id),
            case_fact.c.is_current.is_(True)))).fetchall()
        live_events = (await session.execute(sa.select(case_event).where(
            _source_where(case_event, source_app_id, source_record_id),
            case_event.c.is_current.is_(True)))).fetchall()
        out = await _detach_source_tx(
            session, source_app_id, source_record_id, new_case=target.case_id,
            reason=reason, trust="auto", actor=actor, operation_id=operation_id,
            now=now, source_revision=cur_rev)
        re_facts, re_events = await _reproject_tx(session, live_facts, live_events,
                                                  target, now)
        return {"duplicate": False, "link_id": out["link_id"],
                "moved_facts": out["moved_facts"], "prev": out["prev"],
                "prev_case": out["prev_case"], "new_case_id": target.case_id,
                "reprojected_facts": re_facts, "reprojected_events": re_events}


async def _reproject_tx(session, live_facts: list, live_events: list, new_ref: CaseRef,
                        now) -> tuple:
    """訂正先へ fact/event を積み直す（BA-11/BA-16）。案件をまたぐ移動は supersedes を
    張らず prev_case で出所を残す（案件ごとの供述は一直線・UNIQUE(supersedes) に触れない）。
    同じ一意キー（同じ観測×同じ案件×同じ revision）の行が移動先に既にあるときは、
    uq_case_fact_key により新行を作れないため、その行を現在値へ戻す（供述の重複は作らない）。"""
    n_facts = 0
    for f in live_facts:
        existing = (await session.execute(sa.select(case_fact).where(
            _case_match(case_fact, new_ref),
            case_fact.c.subject_id == f.subject_id, case_fact.c.item_code == f.item_code,
            case_fact.c.source_app_id == f.source_app_id,
            case_fact.c.source_record_id == f.source_record_id,
            case_fact.c.source_revision == f.source_revision,
            case_fact.c.locator == f.locator,
            case_fact.c.converter_name == f.converter_name,
            case_fact.c.converter_version == f.converter_version))).first()
        moved = not _row_same_case(f, new_ref)
        if existing is not None:
            if not existing.is_current:
                prev = _prev_cols(f) if moved else {
                    "prev_case_id": existing.prev_case_id,
                    "prev_case_app_id": existing.prev_case_app_id,
                    "prev_case_record_id": existing.prev_case_record_id}
                await session.execute(sa.update(case_fact).where(
                    case_fact.c.fact_id == existing.fact_id).values(
                    is_current=True, invalid_reason=None, invalidated_at=None, **prev))
                n_facts += 1
            continue
        await session.execute(sa.insert(case_fact).values(
            subject_id=f.subject_id,
            item_code=f.item_code, value_type=f.value_type, value_text=f.value_text,
            value_json=f.value_json, source_kind=f.source_kind,
            source_app_id=f.source_app_id, source_record_id=f.source_record_id,
            source_revision=f.source_revision, locator=f.locator,
            converter_name=f.converter_name, converter_version=f.converter_version,
            observation_id=f.observation_id, occurred_at=f.occurred_at,
            observed_at=now, confidence=f.confidence, is_current=True,
            supersedes_fact_id=None, **_prev_cols(f if moved else None),
            **_case_cols(new_ref)))
        n_facts += 1
    n_events = 0
    for e in live_events:
        idem = event_idem_key(new_ref.case_id, e.source_app_id, e.source_record_id,
                              event_revision_part(e.idem_key), e.kind, e.locator)
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
            idem_key=idem, kind=e.kind,
            occurred_at=e.occurred_at, source_app_id=e.source_app_id,
            source_record_id=e.source_record_id, source_revision=e.source_revision,
            locator=e.locator, summary=e.summary, is_current=True, **_case_cols(new_ref)))
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
    return {"fact_id": int(row.fact_id),
            "case_id": int(row.case_id) if row.case_id is not None else None,
            "case_app_id": row.case_app_id,
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
    """紐付け待ち（案件の無い出典）。出典レコードごとに 1 行（最新 revision）。
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
                        "case_id": int(r.case_id) if r.case_id is not None else None,
                        "case_app_id": r.case_app_id, "case_record_id": r.case_record_id,
                        "hold_reason": r.hold_reason or "",
                        "pending_recheck": bool(r.pending_recheck),
                        "last_checked_at": _iso(r.last_checked_at)})
            if len(out) >= limit:
                break
        return out


def _row_case_key(row) -> tuple:
    """行の案件（case_id があればそれ、無ければ互換の案件キー）でまとめるためのキー。"""
    if row.case_id is not None:
        return ("id", int(row.case_id))
    return ("key", row.case_app_id, row.case_record_id)


async def list_conflicts(limit: int = 100) -> list[dict]:
    """競合（§4-1・R9）: 有効な別観測（出典アプリ・レコード・locator・変換器の
    いずれかが異なる）間で、同じ案件・subject・項目の現在値が異なる。
    会話は case_event なので対象外。発送は subject=shipping:{No} で発送ごとに分かれる。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(case_fact).where(case_fact.c.is_current.is_(True),
                                       case_fact.c.invalid_reason.is_(None))
            .order_by(case_fact.c.case_id, case_fact.c.case_app_id,
                      case_fact.c.case_record_id, case_fact.c.subject_id,
                      case_fact.c.item_code, case_fact.c.fact_id))).fetchall()
        unavailable = await _unavailable_sources(session)
        groups: dict = {}
        for r in rows:
            if (r.source_app_id, r.source_record_id) in unavailable:
                continue
            key = (_row_case_key(r), r.subject_id, r.item_code)
            groups.setdefault(key, []).append(r)
        out = []
        for key, facts in groups.items():
            if len({f.value_text for f in facts}) < 2:
                continue
            observations = {(f.source_app_id, f.source_record_id, f.locator,
                             f.converter_name, f.converter_version) for f in facts}
            if len(observations) < 2:
                continue
            first = facts[0]
            out.append({"case_id": int(first.case_id) if first.case_id is not None else None,
                        "case_app_id": first.case_app_id,
                        "case_record_id": first.case_record_id,
                        "subject_id": key[1], "item_code": key[2],
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


async def list_case_facts(case, current_only: bool = True) -> list[dict]:
    """案件の事実（case は case_id／互換の案件キー）。出典確認不能の出典に由来する fact は
    flag=source_unavailable で区別して返す（BA-07・確認/採用の対象外）。"""
    async with session_scope() as session:
        ref = await _resolve_case_tx(session, case, create=False)
        q = sa.select(case_fact).where(_case_match(case_fact, ref))
        if current_only:
            q = q.where(case_fact.c.is_current.is_(True))
        rows = (await session.execute(q.order_by(case_fact.c.subject_id,
                                                 case_fact.c.item_code,
                                                 case_fact.c.fact_id))).fetchall()
        unavailable = await _unavailable_sources(session)
        return [_fact_view(r, FLAG_SOURCE_UNAVAILABLE
                           if (r.source_app_id, r.source_record_id) in unavailable
                           else None) for r in rows]


async def list_case_events(case, current_only: bool = True) -> list[dict]:
    async with session_scope() as session:
        ref = await _resolve_case_tx(session, case, create=False)
        q = sa.select(case_event).where(_case_match(case_event, ref))
        if current_only:
            q = q.where(case_event.c.is_current.is_(True))
        rows = (await session.execute(q.order_by(case_event.c.event_id))).fetchall()
        return [{"event_id": int(r.event_id),
                 "case_id": int(r.case_id) if r.case_id is not None else None,
                 "kind": r.kind, "summary": r.summary,
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
    ingests は (SourceRef, case|None, facts, events[, extras]) のタプル列。
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
                ref = await _resolve_case_tx(session, case_key, create=True, now=now)
                await _upsert_ingest(session, src, state="mismatch_hold",
                                     ref=ref, hold_reason="mismatch", now=now)
        raise
    return summaries


async def sync_overview() -> dict:
    """同期状態の一覧（対象アプリ・kind ごとの cursor と直近 run・案件数）。"""
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
        cases = (await session.execute(
            sa.select(case.c.registration, sa.func.count()).where(
                case.c.status == CASE_ACTIVE).group_by(case.c.registration))).fetchall()
        running = (await session.execute(sa.select(sa.func.count()).select_from(sync_run)
                                         .where(sync_run.c.status == "running"))).first()
        return {
            "pending_recheck": int(pending[0] or 0),
            # BA-17: 出典行の無い未解決（初見で判定不能）の件数＝同期カーソルの合計
            "pending_unregistered": sum(int(c.pending_unregistered or 0) for c in cursors
                                        if c.kind == CURSOR_KIND_SYNC),
            "running_runs": int(running[0] or 0),
            "cases": {str(k): int(v) for k, v in cases},
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


async def case_freshness_detail(case, target_app: str,
                                source_targets: dict | None = None) -> dict:
    """案件ごとの鮮度（§5・BA-07・R31）: 案件に紐づく全出典（案件自身の出典＋紐付いた
    App 30/28 の出典）の状態を集約する。
    - 案件自身: stopped / error（出典確認不能・不一致）/ incomplete（未確認範囲・
      カーソル error）/ synced
    - 自身が synced でも、紐付いた出典のいずれかが error/unavailable なら partial
      （理由付き）、未確認なら incomplete
    - 未登録案件（R31）: 案件自身の出典は無くてよい。紐付く出典がすべて synced なら
      synced、紐付く出典が無ければ unregistered。no_source は登録済みのはずの出典が
      取れていないときに限る
    source_targets: 出典アプリ ID → カーソル対象名。未指定は案件自身のみカーソル検査。
    戻り値: {"state": ..., "reasons": ["app30:5:source_unavailable", ...]}"""
    targets = dict(source_targets or {})
    async with session_scope() as session:
        ref = await _resolve_case_tx(session, case, create=False)
        if ref is None:
            return {"state": "incomplete", "reasons": ["case_unknown"]}
        if ref.app is not None:
            targets.setdefault(ref.app, target_app)
        cursor_rows = (await session.execute(sa.select(sync_cursor).where(
            sync_cursor.c.kind == CURSOR_KIND_SYNC))).fetchall()
        cursors = {c.target_app: c for c in cursor_rows}
        own = None
        if ref.app is not None:
            own = (await session.execute(sa.select(source_ingest).where(
                _source_where(source_ingest, ref.app, ref.rec))
                .order_by(source_ingest.c.source_revision.desc()).limit(1))).first()
        own_cursor = cursors.get(target_app)
        if own_cursor is not None and own_cursor.state == "stopped":
            return {"state": "stopped", "reasons": ["sync_stopped"]}
        unregistered = ref.registration == REGISTRATION_UNREGISTERED
        if own is None:
            if unregistered:
                own_state, own_reasons = "synced", []
            else:
                own_state, own_reasons = "incomplete", ["no_source"]
        else:
            state, reason = _source_state_eval(own, own_cursor, check_cursor=True)
            own_state, own_reasons = state, ([reason] if reason else [])
        # 紐付いた出典の理由は案件自身の状態に関わらず併記する（何が未解決かを隠さない）
        linked = (await session.execute(sa.select(source_ingest).where(
            _case_match(source_ingest, ref))
            .order_by(source_ingest.c.source_app_id, source_ingest.c.source_record_id,
                      source_ingest.c.source_revision.desc()))).fetchall()
        reasons = []
        worst = "synced"
        seen = set()
        for r in linked:
            key = (r.source_app_id, r.source_record_id)
            if key in seen or key == (ref.app, ref.rec):
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
        if own is None and unregistered and not seen:
            return {"state": "unregistered", "reasons": ["no_linked_source"]}
        if own_state != "synced":
            return {"state": own_state, "reasons": own_reasons + reasons}
        return {"state": worst, "reasons": reasons}


async def case_freshness(case, target_app: str, source_targets: dict | None = None) -> str:
    """案件ごとの鮮度（§5）: synced / partial / incomplete / error / stopped / unregistered。"""
    return (await case_freshness_detail(case, target_app, source_targets))["state"]


def dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
