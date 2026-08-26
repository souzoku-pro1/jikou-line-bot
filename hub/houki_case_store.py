"""相続放棄案件（App 40）アクセス層 — SOUZOKU-HOUKI-H3

houki_bot（AST checker で外部作用を閉集合化）から kintone を直接触らせず、
App 40 への読み書きを本 module の高位関数に集約する（許可名の閉集合を
test_houki_bot_policy が pin する）。

設計（正本 10-unit-02 §2.1・§6.1/6.2、souzoku-houki/02 §1-2〔有効部分〕）:
- record_hearing の fields を**逐次 upsert**（途中離脱してもデータが残る・
  in-memory 消失の影響を受けない）。
- upsert は「空値で非空を上書きしない」（聞き直し・言い直しで確定値を消さない）。
- 弁護士専権フィールド（起算日_確定・起算点確定済・受任判断・電話要否 等）と
  サーバ計算フィールド（法定満了日・社内締切日 等）は**書き込み許可集合に
  含めない**（構造的に書けない・test で pin）。
- 日付整合検証（正本 §2.1・02 §6）: 矛盾した日付フィールドは書かずに理由を
  返す（Bot が聞き直す）。2 回失敗で危険類型フラグ「申告内容の矛盾」
  （App 40 の CHECK_BOX 実選択肢値・H0-APP-2）を立てる。
- status 遷移の入口（正本 §1 のB案・票指定）: ヒアリング必須項目の充足時に
  「問い合わせ」（または空）→「電話判断待ち」への**一方向遷移のみ**。
  電話推奨度判定・通知は H-4 スコープ。
"""

import datetime
import logging
import re

from hub import kintone
from hub.redact import emit

logger = logging.getLogger("hub.houki_case_store")

APP_HOUKI_CASE = kintone.KintoneApp(
    "App 40 (相続放棄案件)", "APP_HOUKI", "TOKEN_HOUKI")

# ── record_hearing が書き込める App 40 フィールドの閉集合 ──────────────────────
# （H0-APP-2 の実フィールドコード。弁護士専権・サーバ計算欄は含めない）
HEARING_WRITABLE_FIELDS: frozenset = frozenset({
    # 申述人（正本 §2.1 phase7 + 様式必須のふりがな・職業）
    "顧客名", "furigana", "生年月日", "住所", "電話番号", "メールアドレス",
    "職業", "本人区分", "未成年後見関与",
    # 被相続人（phase1）
    "被相続人氏名", "被相続人ふりがな", "被相続人本籍", "被相続人最後の住所",
    "続柄", "続柄その他",
    # 日付（phase2・申告値のみ。確定は弁護士）
    "死亡日_申告", "死亡を知った日_申告", "相続人と知った日_申告",
    "日付申告メモ", "知った経緯",
    # 債務・財産（phase3/4。App 40 は財産 4 欄形＝H0-APP-2 採用裁定）
    "財産_不動産", "財産_現金預貯金", "財産_有価証券", "財産_負債",
    "財産処分有無", "訴訟督促有無",
    # 相続関係（phase5）
    "相続順位", "先順位相続人の状況", "他の相続人", "同時申述希望",
    "先順位者の放棄状況",
})

# 日付フィールド（YYYY-MM-DD のみ upsert・曖昧値は 日付申告メモ に残す運用）
_DATE_FIELDS = ("死亡日_申告", "死亡を知った日_申告", "相続人と知った日_申告")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# ヒアリング必須項目（正本 §2.1 の各フェーズ最重要項目。充足+hearing_done で
# status を 電話判断待ち へ進める）
HEARING_REQUIRED_FIELDS: tuple = (
    "被相続人氏名", "続柄",
    "死亡日_申告", "死亡を知った日_申告", "相続人と知った日_申告",
    "相続順位",
    "顧客名", "住所", "生年月日", "電話番号",
)

STATUS_FIELD = "status"
STATUS_INQUIRY = "問い合わせ"
STATUS_PHONE_TRIAGE = "電話判断待ち"
# 危険類型フラグ（CHECK_BOX）の実選択肢値（H0-APP-2・正本 §3.1 逐語）。
# 旧 02 §6 の「日付不整合」・正本 §2.1 の「申告矛盾」は本値に正規化される
KIKEN_FLAG_FIELD = "危険類型フラグ"
KIKEN_FLAG_DATE_MISMATCH = "申告内容の矛盾"

CREDITOR_TABLE = "債権者一覧"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


async def fetch_case(user_id: str) -> dict | None:
    """LINEユーザーID で App 40 の案件を検索（なければ None・最新 1 件）。"""
    rows = await kintone.search_records(
        APP_HOUKI_CASE,
        f'LINEユーザーID = "{user_id}" order by $id desc limit 1')
    return rows[0] if rows else None


def validate_hearing_dates(fields: dict,
                           today: datetime.date | None = None) -> list[str]:
    """日付整合検証（正本 §2.1・02 §6「知った日 < 死亡日 等」）。

    返り値=矛盾理由の一覧（固定語彙・空=適合）。検証は与えられた値のみで行う
    （欠けている側は判定しない・fail-open で会話を止めない）:
      (i)   死亡を知った日_申告 < 死亡日_申告
      (ii)  相続人と知った日_申告 < 死亡日_申告
      (iii) 相続人と知った日_申告 < 死亡を知った日_申告
      (iv)  いずれかが未来日
      (v)   YYYY-MM-DD 形式でない（曖昧値は 日付申告メモ へ・_申告欄には
            確定形式のみ書く）
    """
    problems: list[str] = []
    parsed: dict[str, datetime.date] = {}
    for code in _DATE_FIELDS:
        raw = str(fields.get(code) or "").strip()
        if not raw:
            continue
        if not _DATE_RE.fullmatch(raw):
            problems.append(f"{code}=形式不正")
            continue
        try:
            parsed[code] = datetime.date.fromisoformat(raw)
        except ValueError:
            problems.append(f"{code}=形式不正")
    today = today or datetime.date.today()
    for code, d in parsed.items():
        if d > today:
            problems.append(f"{code}=未来日")
    death = parsed.get("死亡日_申告")
    knew_death = parsed.get("死亡を知った日_申告")
    knew_heir = parsed.get("相続人と知った日_申告")
    if death and knew_death and knew_death < death:
        problems.append("死亡を知った日_申告が死亡日_申告より前")
    if death and knew_heir and knew_heir < death:
        problems.append("相続人と知った日_申告が死亡日_申告より前")
    if knew_death and knew_heir and knew_heir < knew_death:
        problems.append("相続人と知った日_申告が死亡を知った日_申告より前")
    return problems


def split_valid_fields(fields: dict,
                       today: datetime.date | None = None
                       ) -> tuple[dict, list[str]]:
    """tool の fields を（書き込み可能な適合分, 矛盾理由）へ分ける。

    - 許可集合外のフィールド名は黙って落とす（弁護士専権・サーバ計算欄の防壁）
    - 空値は落とす（非空を空で上書きしない）
    - 日付矛盾があれば**日付 3 欄をすべて**書き込み対象から外す（部分書込で
      矛盾ペアの片側だけ残る事故を防ぐ）。他フィールドは書く
    """
    problems = validate_hearing_dates(fields, today=today)
    out: dict = {}
    for code, value in (fields or {}).items():
        if code not in HEARING_WRITABLE_FIELDS:
            continue
        sval = str(value or "").strip()
        if not sval:
            continue
        if problems and code in _DATE_FIELDS:
            continue
        out[code] = sval
    return out, problems


async def upsert_case_fields(user_id: str, fields: dict,
                             existing: dict | None) -> str:
    """適合済み fields を App 40 へ upsert し、レコード ID を返す。

    - 新規: 受付チャネル=LINE・status=問い合わせ で作成
    - 既存: **空でない現値は上書きしない**（record_hearing は追記専用）
    """
    if existing is None:
        payload = {code: {"value": v} for code, v in fields.items()}
        payload["LINEユーザーID"] = {"value": user_id}
        payload["受付チャネル"] = {"value": "LINE"}
        payload[STATUS_FIELD] = {"value": STATUS_INQUIRY}
        rid = await kintone.create_record(APP_HOUKI_CASE, payload)
        logger.info("[HOUKI_CASE] created record_id=%s",
                    emit(rid, "record_id", "log", "operator"))
        return str(rid)
    rid = _v(existing, "$id")
    update = {code: {"value": v} for code, v in fields.items()
              if not _v(existing, code)}
    if update:
        await kintone.update_record(APP_HOUKI_CASE, rid, update)
        logger.info("[HOUKI_CASE] updated record_id=%s fields=%s",
                    emit(rid, "record_id", "log", "operator"),
                    emit(len(update), "count", "log", "operator"))
    return rid


async def append_creditors(record_id: str, existing: dict | None,
                           names: list[str]) -> int:
    """債権者一覧 SUBTABLE へ債権者名を追記（既存行保持・同名スキップ・
    新規行は 通知要否=未確認）。追加行数を返す。"""
    clean = [str(n or "").strip() for n in (names or [])]
    clean = [n for n in clean if n]
    if not clean:
        return 0
    rows = list(((existing or {}).get(CREDITOR_TABLE) or {}).get("value") or [])
    known = {str(((r.get("value") or {}).get("債権者名") or {})
                 .get("value") or "").strip() for r in rows}
    added = 0
    for name in clean:
        if name in known:
            continue
        rows.append({"value": {"債権者名": {"value": name},
                               "通知要否": {"value": "未確認"}}})
        known.add(name)
        added += 1
    if added:
        await kintone.update_record(APP_HOUKI_CASE, record_id,
                                    {CREDITOR_TABLE: {"value": rows}})
        logger.info("[HOUKI_CASE] creditors appended record_id=%s rows=%s",
                    emit(record_id, "record_id", "log", "operator"),
                    emit(added, "count", "log", "operator"))
    return added


async def mark_date_mismatch_flag(record_id: str, existing: dict) -> bool:
    """日付整合の 2 回失敗（正本 §2.1）: 危険類型フラグへ
    「申告内容の矛盾」を追記する（既存チェックは保持・冪等）。"""
    current = list(((existing.get(KIKEN_FLAG_FIELD) or {}).get("value")) or [])
    if KIKEN_FLAG_DATE_MISMATCH in current:
        return False
    current.append(KIKEN_FLAG_DATE_MISMATCH)
    await kintone.update_record(APP_HOUKI_CASE, record_id,
                                {KIKEN_FLAG_FIELD: {"value": current}})
    logger.info("[HOUKI_CASE] kiken flag set record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    return True


def hearing_required_satisfied(record: dict, pending: dict) -> bool:
    """必須項目（HEARING_REQUIRED_FIELDS）が record+今回書込分で全て非空か。"""
    for code in HEARING_REQUIRED_FIELDS:
        if not (_v(record or {}, code) or str(pending.get(code) or "").strip()):
            return False
    return True


async def promote_status_to_phone_triage(record_id: str,
                                         existing: dict) -> bool:
    """status を 問い合わせ（または空）→ 電話判断待ち へ一方向遷移させる。
    それ以外の現値（受任 等）からは**絶対に動かさない**。遷移したら True。"""
    current = _v(existing, STATUS_FIELD)
    if current not in ("", STATUS_INQUIRY):
        return False
    await kintone.update_record(
        APP_HOUKI_CASE, record_id,
        {STATUS_FIELD: {"value": STATUS_PHONE_TRIAGE}})
    logger.info("[HOUKI_CASE] status -> phone triage record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    return True
