"""確定の関所（S5-2.5 T1）: App 30 要確認 → 案件確定 → App 35 生成＋クローズ

設計: 2026-07-07 設計調査＋裁定
- 粒度は**冪等キー単位の一括**（同一PDF由来の土地＋建物をまとめて確定。1件も同機構）。
  案件の指定（顧客名突合・No.直指定）は上位=T2 の責務で、本モジュールは
  「確定対象グループ＋案件レコードID」を受けて実行する純部品
- 実行は Railway 直（kintone 内部のみ: App 35 生成＋App 30 クローズ。
  Drive・LINE顧客側・対外送信なし）。クローズ値 = 発送ステータス:完了＋実行済み:yes
- 汎用化は**チャネル固有データのトップキー→確定ハンドラの登録辞書**（RESOLVERS）のみ。
  第1版は registry_ingest（S5 案件紐付け不能）専用。未知キーは
  「対応する確定処理がありません」の明示応答（黙って無視しない）
- 財産行の再構成: App 25 のレコードから**擬似物件（読解JSON形）を組み立て、
  S5 直行経路と同一の registry_ingest._upsert_zaisan を呼ぶ**——書式同一
  （特定情報の推奨書式・名義表示文字列・データ源=OCR_登記事項証明・冪等キー・
  評価確定=no/有効=yes・S4由来既存行への追記限定〔評価額/評価確定不触〕）を
  関数共有で構造的に保証する。
  ⚠ App 25 に器が無い項目（建物の構造・床面積/地積の原文単位表記）は
  特定情報に完全には戻らない（土地はロスレス・原文は原本PDFと App 25 で保持）
- 原本PDFの移送: 要確認レコードの成果物を download → App 35 原本へ再添付
- 二重確定ガード（仕分け第2段と同じ意味論）: 発送ステータス=要確認・実行済み=no の
  もののみ対象。**書き込み直前にグループ全件を再読**し、1件でも変化していれば
  グループ全体を中止して報告（部分実行しない）
- 既存アプリ・既存エンドポイント・GAS は無変更（registry_ingest は import 共有のみ）
"""

import json
import logging
import os
import re
from dataclasses import dataclass, field

from hub import kintone
from hub.redact import emit
from registry_ingest import (
    APP_FUDOSAN,
    APP_SHIPPING,
    APP_ZAISAN,
    _upsert_zaisan,
)

logger = logging.getLogger("review_resolve")

APP_KOSEKI_BOOK = kintone.KintoneApp(
    "App 33 (戸籍読解)", "APP_KOSEKI_BOOK", "TOKEN_KOSEKI_BOOK")

STATUS_PENDING = "要確認"
STATUS_DONE = "完了"

MSG_UNSUPPORTED_SOURCE = "対応する確定処理がありません"

# 実機 App 25「種別」→ 読解部品の種別（registry_ingest.KIND_TO_APP25 の逆写像）
_APP25_TO_KIND = {"土地": "土地", "建物": "建物",
                  "マンション(区分所有)": "区分建物", "その他": "不明"}


@dataclass(frozen=True)
class ReviewItem:
    """要確認レコード1件（確定に必要な最小属性）"""
    record_id: str
    subject: str
    detail: dict            # チャネル固有データのトップキー配下の JSON
    file_keys: list[str] = field(default_factory=list)  # 成果物（原本PDF）
    file_name: str = ""


@dataclass(frozen=True)
class ReviewGroup:
    """冪等キー単位の確定グループ（同一PDF由来の物件をまとめる）"""
    source: str             # チャネル固有データのトップキー（"registry_ingest" 等）
    idempotency_key: str
    items: list[ReviewItem] = field(default_factory=list)

    def label(self) -> str:
        subjects = "・".join(f"No.{i.record_id}" for i in self.items)
        return f"{subjects}（{len(self.items)}件・{self.source}）"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _parse_channel_data(raw: str) -> tuple[str, dict]:
    """チャネル固有データ → (トップキー, detail)。壊れていれば ("unknown", {})"""
    try:
        payload = json.loads(raw or "")
    except json.JSONDecodeError:
        return "unknown", {}
    if not isinstance(payload, dict) or len(payload) != 1:
        return "unknown", {}
    key = next(iter(payload))
    detail = payload[key]
    return key, detail if isinstance(detail, dict) else {}


async def list_pending_reviews() -> list[ReviewGroup]:
    """App 30 の要確認（未実行）を取得し、トップキー＋冪等キーでグルーピングする。

    env（APP_SHIPPING）未設定は空リスト（縮退・上位が明示メッセージを出す前提で
    ここでは検索不能として空を返すのみ）。
    """
    if not (APP_SHIPPING.app_id() and APP_SHIPPING.token()):
        logger.info("[REVIEW_RESOLVE] APP_SHIPPING 未設定のため要確認を取得できません")
        return []
    records = await kintone.search_records(
        APP_SHIPPING,
        '発送ステータス in ("要確認") and 実行済み in ("no")'
        ' order by $id asc limit 100',
        fields=["$id", "件名", "チャネル固有データ", "成果物"])

    groups: dict[tuple[str, str], list[ReviewItem]] = {}
    for r in records:
        source, detail = _parse_channel_data(_v(r, "チャネル固有データ"))
        files = (r.get("成果物") or {}).get("value") or []
        item = ReviewItem(
            record_id=_v(r, "$id"),
            subject=_v(r, "件名"),
            detail=detail,
            file_keys=[f.get("fileKey") for f in files if f.get("fileKey")],
            file_name=str(files[0].get("name")) if files else "")
        idem = str(detail.get("冪等キー") or f"record:{item.record_id}")
        groups.setdefault((source, idem), []).append(item)
    return [ReviewGroup(source=s, idempotency_key=k, items=v)
            for (s, k), v in groups.items()]


# ── registry_ingest（S5 案件紐付け不能）の確定ハンドラ ──────────────────────

def _owners_from_mochibun(raw: str) -> list[dict]:
    """App 25 持分割合（例「山田太郎 2分の1・山田花子 2分の1」「熊澤正広」）→
    所有者リスト。末尾トークンが持分表記（〜分の〜）のときだけ持分として分離"""
    owners = []
    for part in (raw or "").split("・"):
        part = part.strip()
        if not part:
            continue
        name, _, share = part.rpartition(" ")
        if name and re.fullmatch(r"\d+分の\d+", share):
            owners.append({"氏名": name, "持分": share})
        else:
            owners.append({"氏名": part})
    return owners


def _pseudo_property(fudosan: dict) -> dict:
    """App 25 レコード → 擬似物件（読解JSON形）。S5 直行と同一の財産行生成関数に
    渡すための逆変換（App 25 に器が無い項目=構造・原文単位は復元しない）"""
    kind = _APP25_TO_KIND.get(_v(fudosan, "種別"), "不明")
    chiseki = _v(fudosan, "地積")
    floors = []
    for n in ("1", "2", "3"):
        area = _v(fudosan, f"床面積{n}階")
        if area:
            floors.append(f"{n}階 {area}㎡")
    return {
        "種別": kind,
        "所在": _v(fudosan, "所在"),
        "地番": _v(fudosan, "地番"),
        "地目": _v(fudosan, "地目"),
        "地積": f"{chiseki}㎡" if chiseki else "",
        "家屋番号": _v(fudosan, "部屋番号"),
        "種類": _v(fudosan, "建物名"),
        "床面積": " ".join(floors),
        "甲区": {"所有者": _owners_from_mochibun(_v(fudosan, "持分割合"))},
    }


async def _resolve_registry(group: ReviewGroup, case_record_id: str) -> dict:
    """S5「案件紐付け不能」グループの確定:
    App 25 から財産行を再構成（S5 直行と同一関数）→ App 35 生成 → App 30 クローズ"""
    # ── phase 1: 書き込み直前の全件再読（二重確定ガード・1件でも変化なら全体中止）──
    verified = []
    for item in group.items:
        record = await kintone.get_record(APP_SHIPPING, item.record_id)
        status, executed = _v(record, "発送ステータス"), _v(record, "実行済み")
        if status != STATUS_PENDING or executed != "no":
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} が要確認ではなくなっています"
                              f"（発送ステータス={status}・実行済み={executed}）。"
                              "グループ全体を中止しました（書き込みなし）"}
        fudosan_id = str(item.detail.get("不動産レコードID") or "")
        if not fudosan_id:
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} に不動産レコードIDがありません"
                              "（再構成不能・書き込みなし）"}
        fudosan = await kintone.get_record(APP_FUDOSAN, fudosan_id)
        verified.append((item, fudosan_id, fudosan))

    # ── phase 2: 財産行生成 → クローズ ──────────────────────────────────────
    results = []
    for item, fudosan_id, fudosan in verified:
        pdf_bytes = b""
        if item.file_keys:
            try:
                pdf_bytes = await kintone.download_file(
                    APP_SHIPPING, item.file_keys[0])
            except Exception as e:
                logger.info("[REVIEW_RESOLVE] 原本の取得に失敗（添付なしで続行）: %s: %s",
                            type(e).__name__,
                            emit(str(e), "vendor_raw", "log", "operator"))
        outcome = await _upsert_zaisan(
            _pseudo_property(fudosan), fudosan_id, case_record_id,
            str(item.detail.get("冪等キー") or ""), pdf_bytes,
            item.file_name or "登記事項証明.pdf")
        await kintone.update_record(APP_SHIPPING, item.record_id, {
            "発送ステータス": STATUS_DONE,
            "実行済み": "yes",
        })
        results.append({"review_record_id": item.record_id,
                        "fudosan_record_id": fudosan_id, **outcome})
        logger.info("[REVIEW_RESOLVE] resolved review=No.%s fudosan=%s zaisan=%s "
                    "case=%s",
                    emit(item.record_id, "record_id", "log", "operator"),
                    emit(fudosan_id, "record_id", "log", "operator"),
                    emit(outcome.get("zaisan_record_id"), "record_id", "log", "operator"),
                    emit(case_record_id, "record_id", "log", "operator"))
    return {"status": "resolved", "case_record_id": case_record_id,
            "items": results}


# ── koseki_ingest（R4-0 戸籍の案件紐付け不能）の確定ハンドラ ─────────────────

async def _resolve_koseki(group: ReviewGroup, case_record_id: str) -> dict:
    """戸籍の案件紐付けグループの確定:
    App 33 の案件アプリID/案件レコードIDを埋め、App 30 をクローズする。
    App 34（人物）には触れない（人物化は R4-1 の仕事・裁定）"""
    # ── phase 1: 書き込み直前の全件再読（T1 と同じ意味論・1件でも変化なら全体中止）──
    verified = []
    for item in group.items:
        record = await kintone.get_record(APP_SHIPPING, item.record_id)
        status, executed = _v(record, "発送ステータス"), _v(record, "実行済み")
        if status != STATUS_PENDING or executed != "no":
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} が要確認ではなくなっています"
                              f"（発送ステータス={status}・実行済み={executed}）。"
                              "グループ全体を中止しました（書き込みなし）"}
        koseki_id = str(item.detail.get("戸籍レコードID") or "")
        if not koseki_id:
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} に戸籍レコードIDがありません"
                              "（書き込みなし）"}
        koseki = await kintone.get_record(APP_KOSEKI_BOOK, koseki_id)
        current_case = _v(koseki, "案件レコードID")
        if current_case and current_case != case_record_id:
            return {"status": "aborted",
                    "reason": f"戸籍 No.{koseki_id} は既に案件 No.{current_case} に"
                              "紐付いています。付け替えは kintone で直接行ってください"
                              "（書き込みなし）"}
        verified.append((item, koseki_id))

    # ── phase 2: 案件紐付け → クローズ ─────────────────────────────────────
    results = []
    for item, koseki_id in verified:
        await kintone.update_record(APP_KOSEKI_BOOK, koseki_id, {
            "案件アプリID": os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
            "案件レコードID": case_record_id,
        })
        await kintone.update_record(APP_SHIPPING, item.record_id, {
            "発送ステータス": STATUS_DONE,
            "実行済み": "yes",
        })
        result = {"review_record_id": item.record_id,
                  "koseki_record_id": koseki_id}
        # R4-1: 案件が付いた戸籍の人物化（env KOSEKI_PERSON_SYNC_ENABLED=1 のとき
        # のみ・既定無効）。失敗しても紐付け・クローズの成功は壊さない
        try:
            from koseki_person_sync import sync_enabled, sync_persons_from_koseki
            if sync_enabled():
                result["persons"] = await sync_persons_from_koseki(koseki_id)
        except Exception as e:
            logger.info("[REVIEW_RESOLVE] 人物化に失敗（紐付けは完了済み） koseki=%s: "
                        "%s: %s",
                        emit(koseki_id, "record_id", "log", "operator"),
                        type(e).__name__,
                        emit(str(e), "vendor_raw", "log", "operator"))
            result["persons"] = {"status": "error", "reason": str(e)[:200]}
        results.append(result)
        logger.info("[REVIEW_RESOLVE] koseki linked review=No.%s koseki=%s case=%s",
                    emit(item.record_id, "record_id", "log", "operator"),
                    emit(koseki_id, "record_id", "log", "operator"),
                    emit(case_record_id, "record_id", "log", "operator"))
    return {"status": "resolved", "case_record_id": case_record_id,
            "items": results}


# ── valuation_ingest（S4-M2 評価証明・課税明細の案件紐付け不能）の確定ハンドラ ──

async def _resolve_valuation(group: ReviewGroup, case_record_id: str) -> dict:
    """評価読解グループの確定: App 25 から財産行を upsert（S4 の資産温存・
    valuation_ingest.upsert_zaisan_from_fudosan を共用）→ App 30 クローズ"""
    from valuation_ingest import upsert_zaisan_from_fudosan  # 遅延 import

    # ── phase 1: 書き込み直前の全件再読（T1 と同じ意味論・1件でも変化なら全体中止）──
    verified = []
    for item in group.items:
        record = await kintone.get_record(APP_SHIPPING, item.record_id)
        status, executed = _v(record, "発送ステータス"), _v(record, "実行済み")
        if status != STATUS_PENDING or executed != "no":
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} が要確認ではなくなっています"
                              f"（発送ステータス={status}・実行済み={executed}）。"
                              "グループ全体を中止しました（書き込みなし）"}
        fudosan_id = str(item.detail.get("不動産レコードID") or "")
        if not fudosan_id:
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} に不動産レコードIDがありません"
                              "（再構成不能・書き込みなし）"}
        verified.append((item, fudosan_id))

    # ── phase 2: 財産行 upsert → クローズ ──────────────────────────────────
    results = []
    for item, fudosan_id in verified:
        pdf_bytes = b""
        if item.file_keys:
            try:
                pdf_bytes = await kintone.download_file(
                    APP_SHIPPING, item.file_keys[0])
            except Exception as e:
                logger.info("[REVIEW_RESOLVE] 原本の取得に失敗（添付なしで続行）: %s: %s",
                            type(e).__name__,
                            emit(str(e), "vendor_raw", "log", "operator"))
        outcome = await upsert_zaisan_from_fudosan(
            fudosan_id, case_record_id,
            str(item.detail.get("冪等キー") or ""),
            pdf_bytes=pdf_bytes,
            filename=item.file_name or "課税明細.pdf")
        await kintone.update_record(APP_SHIPPING, item.record_id, {
            "発送ステータス": STATUS_DONE,
            "実行済み": "yes",
        })
        results.append({"review_record_id": item.record_id,
                        "fudosan_record_id": fudosan_id, **outcome})
        logger.info("[REVIEW_RESOLVE] valuation resolved review=No.%s fudosan=%s "
                    "zaisan=%s case=%s",
                    emit(item.record_id, "record_id", "log", "operator"),
                    emit(fudosan_id, "record_id", "log", "operator"),
                    emit(outcome.get("zaisan_record_id"), "record_id", "log", "operator"),
                    emit(case_record_id, "record_id", "log", "operator"))
    return {"status": "resolved", "case_record_id": case_record_id,
            "items": results}


# ── bank_ingest（S6-1 通帳・残高証明の案件紐付け不能）の確定ハンドラ ─────────

async def _resolve_bank(group: ReviewGroup, case_record_id: str) -> dict:
    """口座グループの確定: 要確認 detail の口座断片から財産行を upsert
    （bank_ingest.upsert_account_row を共用・再OCRしない）→ App 30 クローズ"""
    from bank_ingest import upsert_account_row  # 遅延 import

    verified = []
    for item in group.items:
        record = await kintone.get_record(APP_SHIPPING, item.record_id)
        status, executed = _v(record, "発送ステータス"), _v(record, "実行済み")
        if status != STATUS_PENDING or executed != "no":
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} が要確認ではなくなっています"
                              f"（発送ステータス={status}・実行済み={executed}）。"
                              "グループ全体を中止しました（書き込みなし）"}
        account = item.detail.get("口座")
        if not isinstance(account, dict) or not account.get("金融機関名"):
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} に口座情報がありません"
                              "（再構成不能・書き込みなし）"}
        verified.append((item, account))

    results = []
    for item, account in verified:
        pdf_bytes = b""
        if item.file_keys:
            try:
                pdf_bytes = await kintone.download_file(
                    APP_SHIPPING, item.file_keys[0])
            except Exception as e:
                logger.info("[REVIEW_RESOLVE] 原本の取得に失敗（添付なしで続行）: %s: %s",
                            type(e).__name__,
                            emit(str(e), "vendor_raw", "log", "operator"))
        outcome = await upsert_account_row(
            account, str(item.detail.get("書類形態") or "不明"), case_record_id,
            pdf_bytes=pdf_bytes, filename=item.file_name or "残高証明.pdf")
        await kintone.update_record(APP_SHIPPING, item.record_id, {
            "発送ステータス": STATUS_DONE,
            "実行済み": "yes",
        })
        results.append({"review_record_id": item.record_id, **outcome})
        logger.info("[REVIEW_RESOLVE] bank resolved review=No.%s zaisan=%s case=%s",
                    emit(item.record_id, "record_id", "log", "operator"),
                    emit(outcome.get("zaisan_record_id"), "record_id", "log", "operator"),
                    emit(case_record_id, "record_id", "log", "operator"))
    return {"status": "resolved", "case_record_id": case_record_id,
            "items": results}


# ハンドラ登録辞書: トップキー → (確定ハンドラ, 必要な kintone env)。
# 将来の zaisan_sync はキー追加で載せる
# P3-003b: heir_derivation（相続人導出封筒の確定関所）を追加（ENVELOPE_FLOW §3.1。
# ハンドラ本体は hub/heir_projection に隔離＝App36 write の一本経路をそこへ閉じる）
from hub.heir_projection import (  # noqa: E402（登録用 import・循環なし）
    APP_SOUZOKUNIN,
    _resolve_heir_derivation,
)

RESOLVERS = {
    "registry_ingest": (_resolve_registry, (APP_SHIPPING, APP_FUDOSAN, APP_ZAISAN)),
    "koseki_ingest": (_resolve_koseki, (APP_SHIPPING, APP_KOSEKI_BOOK)),
    "valuation_ingest": (_resolve_valuation,
                         (APP_SHIPPING, APP_FUDOSAN, APP_ZAISAN)),
    "bank_ingest": (_resolve_bank, (APP_SHIPPING, APP_ZAISAN)),
    "heir_derivation": (_resolve_heir_derivation, (APP_SHIPPING, APP_SOUZOKUNIN)),
}


async def resolve_group(group: ReviewGroup, case_record_id: str,
                        decided_by: str = "") -> dict:
    """確定グループを案件へ確定する（上位=T2 から呼ばれる入口）。

    Returns: {"status": "resolved"|"aborted"|"unsupported"|"unavailable", ...}
    - unsupported: RESOLVERS に無いトップキー（明示応答・黙って無視しない）
    - unavailable: そのハンドラが必要とする env の未設定
    - aborted: 二重確定ガード発動（書き込みゼロで中止・理由つき）
    - decided_by: 確定者の識別（P3-003b・heir_derivation の ATTORNEY_ALLOWLIST
      検証に使用）。**ハンドラ固有分岐は作らない**——signature に decided_by を
      持つハンドラへだけ渡す（能力ベース・既存4ハンドラは無変更で互換）
    """
    entry = RESOLVERS.get(group.source)
    if entry is None:
        return {"status": "unsupported",
                "reason": f"{MSG_UNSUPPORTED_SOURCE}"
                          f"（チャネル固有データのキー={group.source}）"}
    handler, required_apps = entry
    for app in required_apps:
        if not (app.app_id() and app.token()):
            return {"status": "unavailable",
                    "reason": f"{app.label} の env（{app.app_id_env}）が未設定です"}
    if not case_record_id:
        return {"status": "aborted", "reason": "案件レコードIDが指定されていません"}
    import inspect
    if "decided_by" in inspect.signature(handler).parameters:
        return await handler(group, case_record_id, decided_by=decided_by)
    return await handler(group, case_record_id)
