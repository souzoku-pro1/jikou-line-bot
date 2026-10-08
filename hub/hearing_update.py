"""時効ヒアリング第 2 段階（KINTONE_UPDATE）の書込 — JIKOU-HEARING-HOTFIX-1

事象（2026-09-13 本番実測）: main._process_line_event の第 2 段階は書込先を
in-memory の kintone_record_ids に依存しており、デプロイ（2026-09-08）で台帳が
消えた後は「それ以前の案件・フォーム経由の案件」で record_id が引けず未書込。
さらにマーカー除去が書込成功時にしか行われず、送信ゲート（内部マーカー残存）
で降格→承認キュー起票（App 29 No.31〜33）。

本 module の契約（票の逐語）:
1. record_id の解決順序: in-memory の台帳に**あれば使う**（格下げ）→ 無ければ
   LINEユーザーID で App 21 を検索（limit 2）。ちょうど 1 件=その $id。
   0 件=書かない（none）。複数件=最新更新を採用せず書かない（ambiguous・要確認）。
   検索失敗=書かない（search_failed）。
2. 書込は **空欄のみ・$revision CAS**。409 は再取得 **1 回**（CAS_REFETCH）。
   既に値がある欄は上書きしない（手入力を守る）。許可集合は UPDATE_FIELD_LABELS
   （SYSTEM_PROMPT の KINTONE_UPDATE 5 項目→App 21 欄コード→表示名）の閉集合。
3. マーカー除去は呼び出し側（main）が書込の成否にかかわらず行う（本 module は
   書込と通知のみ）。
4. 結果（書いた欄名・書けなかった欄名・record_id の解決方法）を業務 LINE
   （notify_admin_line_result=指示Bot チャネル）へ通知。**値は載せない**。

fix1（Codex R-JIKOU-HEARING-HOTFIX-1）:
- JHH-01: 許可集合に無いキー（モデルが JSON に混ぜた項目名）は「対象外」。
  キー名も値も通知・ログ・戻り値のどこにも載せない——件数（dropped_count）のみ。
  通知に載せる欄名は許可集合の表示名（氏名・住所・生年月日・電話番号・メールアドレス）だけ。
- JHH-02: 通知は notify_admin_line_result の 3 値をそのまま扱う
  （sent=ok / throttled=skipped / failed）。failed は NOTIFY_RETRY_MAX 回まで
  NOTIFY_RETRY_SLEEP_SEC 間隔で再送し、全失敗は ERROR 1 行（レコード番号と固定理由
  hearing_notify_failed のみ）。skipped は再送せず INFO 1 行。例外は上位へ伝播させず、
  書込済みの CAS 結果は取り消さない（書込→通知の順は維持）。
  冪等キーは成功通知 hearing_update_ok:{record_id}:{digest} と要確認通知
  hearing_update_review:{record_id}:{digest} の 2 種（digest=値を含まない
  canonical 文字列の sha256 先頭 8 桁・_anon と同じ計算）。
5. 例外は外へ出さない（handle_update が握る=顧客への返信を道連れにしない）。
"""

import asyncio
import hashlib
import logging

from hub import kintone as hub_kintone
from hub import notify
from hub.redact import emit

logger = logging.getLogger("hub.hearing_update")

APP_JIKOU_CASE = hub_kintone.KintoneApp(
    "App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")

USER_FIELD = "LINEユーザーID"

# JHH-01: 台帳の項目（KINTONE_UPDATE の JSON キー=App 21 欄コード）→ 通知の表示名。
# この対応表が許可集合の単一の正。表示順は通知の並び順
UPDATE_FIELD_LABELS: dict = {
    "顧客名": "氏名",
    "住所": "住所",
    "生年月日": "生年月日",
    "電話番号": "電話番号",
    "メールアドレス": "メールアドレス",
}
UPDATE_FIELDS: frozenset = frozenset(UPDATE_FIELD_LABELS)

# 409（KintoneConflict）後の再取得回数（票: 再取得 1 回）
CAS_REFETCH = 1

# JHH-02: 通知の再送（failed のみ・有限回）
NOTIFY_RETRY_MAX = 2
NOTIFY_RETRY_SLEEP_SEC = 1.0
NOTIFY_FAILED_REASON = "hearing_notify_failed"
KIND_OK = "hearing_update_ok"
KIND_REVIEW = "hearing_update_review"

# record_id の解決方法（固定語彙・通知/ログにこのまま載せる）
METHOD_MEMORY = "memory"            # in-memory 台帳にあった
METHOD_SEARCH = "search"            # App 21 を LINEユーザーID で検索・1 件
METHOD_NONE = "none"                # 検索 0 件（書かない）
METHOD_AMBIGUOUS = "ambiguous"      # 検索 2 件以上（書かない・要確認）
METHOD_SEARCH_FAILED = "search_failed"   # 検索の失敗（書かない）
METHOD_PARSE_FAILED = "parse_failed"     # マーカーはあったが JSON として読めない
METHOD_ERROR = "error"              # 予期しない例外（書込の成否不明）

_METHOD_LABELS = {
    METHOD_MEMORY: "メモリ上の台帳",
    METHOD_SEARCH: "LINEユーザーIDで検索",
}

# 書込結果（固定語彙）
OUTCOME_UPDATED = "updated"         # 書込成立
OUTCOME_NOOP = "noop"               # 埋める欄なし（全欄に既に値がある）
OUTCOME_UNCONVERGED = "unconverged" # 409 が再取得後も続いた（上書きせず中止）
OUTCOME_FAILED = "failed"           # 取得/書込の確定失敗


def _value(record: dict, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "").strip()


def _sha8(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def _anon(user_id: str) -> str:
    return _sha8(user_id)


def _labels(codes) -> str:
    """許可集合の欄コード列を表示名（固定順）へ。許可集合外は表示しない。"""
    return ", ".join(UPDATE_FIELD_LABELS[c] for c in UPDATE_FIELD_LABELS
                     if c in set(codes))


async def resolve_record_id(user_id: str, memory_id) -> tuple[str, str]:
    """(record_id, method) を返す。record_id 空=書かない。"""
    if memory_id:
        return str(memory_id), METHOD_MEMORY
    try:
        rows = await hub_kintone.search_records(
            APP_JIKOU_CASE,
            f'{USER_FIELD} = "{user_id}" order by $id asc limit 2',
            fields=["$id"])
    except hub_kintone.KintoneError as e:
        logger.warning("[HEARING_UPDATE] record search failed code=%s",
                       emit(e.code, "vendor_raw", "log", "operator"))
        return "", METHOD_SEARCH_FAILED
    if not rows:
        return "", METHOD_NONE
    if len(rows) >= 2:
        return "", METHOD_AMBIGUOUS
    rid = _value(rows[0], "$id")
    if not rid:
        return "", METHOD_SEARCH_FAILED
    return rid, METHOD_SEARCH


def split_fields(fields: dict,
                 allowed: frozenset = UPDATE_FIELDS) -> tuple[dict, int]:
    """JHH-01: (許可集合内で非空の候補 {欄コード: 値}, 対象外キーの件数)。
    対象外キーの名前・値はここで捨てる（以後どこにも渡らない）。
    allowed（HUMAN-REPLY-INTAKE-1）: 許可集合の差し替え。既定=UPDATE_FIELDS。"""
    candidate = {k: str(v).strip() for k, v in (fields or {}).items()
                 if k in allowed and str(v or "").strip()}
    dropped_count = sum(1 for k in (fields or {}) if k not in allowed)
    return candidate, dropped_count


async def apply_update(record_id: str, fields: dict,
                       allowed: frozenset = UPDATE_FIELDS) -> dict:
    """許可集合内・非空の値を、最新レコードで空欄の欄にだけ $revision CAS で書く。
    戻り値: {"outcome", "written", "preexisting", "dropped_count"}
    （欄コードは許可集合のもののみ・値なし・対象外キーは件数のみ）。
    allowed（HUMAN-REPLY-INTAKE-1）: 許可集合の差し替え。既定=UPDATE_FIELDS
    （KINTONE_UPDATE 経路の挙動不変）。"""
    candidate, dropped_count = split_fields(fields, allowed)
    if dropped_count:
        logger.info("[HEARING_UPDATE] foreign keys dropped count=%s",
                    emit(dropped_count, "count", "log", "operator"))
    written: list[str] = []
    preexisting: list[str] = []
    outcome = OUTCOME_UNCONVERGED
    for _attempt in range(CAS_REFETCH + 1):
        try:
            latest = await hub_kintone.get_record(APP_JIKOU_CASE, record_id)
        except hub_kintone.KintoneError as e:
            logger.warning("[HEARING_UPDATE] refetch failed code=%s",
                           emit(e.code, "vendor_raw", "log", "operator"))
            outcome = OUTCOME_FAILED
            break
        preexisting = sorted(k for k in candidate if _value(latest, k))
        to_write = {k: v for k, v in candidate.items() if not _value(latest, k)}
        if not to_write:
            outcome = OUTCOME_NOOP
            break
        try:
            await hub_kintone.update_record(
                APP_JIKOU_CASE, record_id, to_write,
                revision=_value(latest, "$revision") or None)
        except hub_kintone.KintoneConflict:
            logger.info("[HEARING_UPDATE] cas conflict (refetch)")
            continue
        except hub_kintone.KintoneError as e:
            logger.warning("[HEARING_UPDATE] update failed code=%s",
                           emit(e.code, "vendor_raw", "log", "operator"))
            outcome = OUTCOME_FAILED
            break
        written = sorted(to_write)
        outcome = OUTCOME_UPDATED
        break
    logger.info("[HEARING_UPDATE] record_id=%s written=%s preexisting=%s",
                emit(record_id, "record_id", "log", "operator"),
                emit(len(written), "count", "log", "operator"),
                emit(len(preexisting), "count", "log", "operator"))
    return {"outcome": outcome, "written": written,
            "preexisting": preexisting, "dropped_count": dropped_count}


def build_notice(user_id: str, record_id: str, method: str,
                 result: dict | None) -> str:
    """業務 LINE 向け通知文（許可集合の表示名・レコード No・件数のみ。値と
    対象外キーの原文は載せない）。"""
    lines = ["【ヒアリング登録】お名前等の登録結果"]
    if record_id:
        lines.append(f"案件レコードNo: {record_id}"
                     f"（解決方法: {_METHOD_LABELS.get(method, method)}）")
    else:
        lines.append(f"対象ユーザー: 匿名ID {_anon(user_id)}（App 28 で実体を確認）")
    if method == METHOD_NONE:
        lines.append("・該当する案件レコードがありません（LINEユーザーIDで検索・0 件）。"
                     "登録していません")
    elif method == METHOD_AMBIGUOUS:
        lines.append("・同一 LINE ユーザーの案件レコードが複数あります。登録して"
                     "いません（要確認: App 21 で重複を整理してください）")
    elif method == METHOD_SEARCH_FAILED:
        lines.append("・案件レコードの検索に失敗しました。登録していません（要確認）")
    elif method == METHOD_PARSE_FAILED:
        lines.append("・登録データの解析に失敗しました。登録していません"
                     "（App 28 の会話から手入力してください）")
    elif method == METHOD_ERROR:
        lines.append("・予期しない失敗が起きました。登録の成否は App 21 で確認してください")
    if result:
        if result.get("written"):
            lines.append("・登録した欄: " + _labels(result["written"]))
        if result.get("preexisting"):
            lines.append("・既に値があり登録しなかった欄: " + _labels(result["preexisting"]))
        n = int(result.get("dropped_count") or 0)
        if n > 0:
            lines.append(f"・対象外の項目が {n} 件あります（登録していません）")
        if result.get("outcome") == OUTCOME_UNCONVERGED:
            lines.append("・更新が競合し収束できませんでした（上書きせず中止・要確認）")
        elif result.get("outcome") == OUTCOME_FAILED:
            lines.append("・レコードの取得または更新に失敗しました（要確認）")
    return "\n".join(lines)


def notice_kind(method: str, result: dict | None) -> str:
    """JHH-02(4): 成功通知（ok）と要確認通知（review）を分ける。
    ok=書込成立かつ既存値との衝突なしかつ対象外 0 件。それ以外は review。"""
    if method not in (METHOD_MEMORY, METHOD_SEARCH) or not result:
        return KIND_REVIEW
    if (result.get("outcome") != OUTCOME_UPDATED or result.get("preexisting")
            or int(result.get("dropped_count") or 0) > 0):
        return KIND_REVIEW
    return KIND_OK


def notice_digest(method: str, result: dict | None) -> str:
    """冪等キーの digest（値を含まない canonical 文字列の sha256 先頭 8 桁）。"""
    r = result or {}
    canonical = "|".join([
        method, str(r.get("outcome") or ""),
        ",".join(sorted(r.get("written") or [])),
        ",".join(sorted(r.get("preexisting") or [])),
        str(int(r.get("dropped_count") or 0)),
    ])
    return _sha8(canonical)


def notice_key(user_id: str, record_id: str, method: str,
               result: dict | None) -> str:
    return (f"{notice_kind(method, result)}:{record_id or _anon(user_id)}:"
            f"{notice_digest(method, result)}")


async def send_notice(text: str, key: str, record_id: str) -> str:
    """JHH-02: notify_admin_line_result の 3 値をそのまま扱う。
    戻り値 ok / skipped / failed。例外は外へ出さない。"""
    rid = record_id or "none"
    for attempt in range(NOTIFY_RETRY_MAX + 1):
        try:
            outcome = await notify.notify_admin_line_result(text, throttle_key=key)
        except Exception:
            outcome = "failed"
        if outcome == "sent":
            return "ok"
        if outcome == "throttled":
            logger.info("[HEARING_UPDATE] notify skipped record_id=%s",
                        emit(rid, "record_id", "log", "operator"))
            return "skipped"
        if attempt < NOTIFY_RETRY_MAX:
            await asyncio.sleep(NOTIFY_RETRY_SLEEP_SEC)
    logger.error("[HEARING_UPDATE] hearing_notify_failed record_id=%s",
                 emit(rid, "record_id", "log", "operator"))
    return "failed"


async def _notify(user_id: str, record_id: str, method: str,
                  result: dict | None) -> str:
    return await send_notice(build_notice(user_id, record_id, method, result),
                             notice_key(user_id, record_id, method, result),
                             record_id)


async def handle_update(user_id: str, memory_id, fields) -> tuple[str, str]:
    """第 2 段階の一括処理: 解決→書込→通知。(record_id, method) を返す。
    record_id 非空=書込を試みた（結果は通知済み）。例外は外へ出さない。
    fields が dict でない/空（マーカーはあったが JSON として読めない）は
    parse_failed として通知のみ。"""
    try:
        if not isinstance(fields, dict) or not fields:
            await _notify(user_id, "", METHOD_PARSE_FAILED, None)
            return "", METHOD_PARSE_FAILED
        record_id, method = await resolve_record_id(user_id, memory_id)
        result = None
        if record_id:
            result = await apply_update(record_id, fields)
        else:
            logger.info("[HEARING_UPDATE] no record to update (write 0)")
        await _notify(user_id, record_id, method, result)
        return record_id, method
    except Exception:
        logger.error("[HEARING_UPDATE] unexpected failure (fixed reason)")
        await _notify(user_id, "", METHOD_ERROR, None)
        return "", METHOD_ERROR
