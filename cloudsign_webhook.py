"""
クラウドサイン Webhook 受け取りモジュール（Railway 用）

役割:
  1. クラウドサインから「締結完了」の通知（Webhook）を受け取る
  2. なりすまし防止のため URL に埋めた合言葉を検証
  3. 書類詳細 API を叩いて通知の真正性を確認（締結情報も取得）。
     確認できない場合は fail-closed（P0B-002 / R0A-B03）: 受任遷移も
     顧客向け通知も行わず、業務指示Botチャネルで要人手確認を警報する
  4. documentID で kintone の案件レコードを検索し、ステータスを「締結済み」に更新
  5. LINE で管理者に通知

既存の LINE Bot アプリ（Railway 上）に組み込む前提。
フレームワークに依存しないコア関数 + FastAPI / Flask アダプタの2構成。
"""

import os
import time
import hmac
import json
import logging

import requests

from hub.redact import emit

logger = logging.getLogger("cloudsign")

# ============================================================
# 環境変数（Railway の Variables に設定する）
# ============================================================
CLOUDSIGN_CLIENT_ID = os.environ["CLOUDSIGN_CLIENT_ID"]
# 本番: https://api.cloudsign.jp / サンドボックス: 別ホスト（検証時はこちらを指定）
CLOUDSIGN_API_BASE = os.environ.get("CLOUDSIGN_API_BASE", "https://api.cloudsign.jp")
# Webhook URL に埋め込む合言葉（推測されない長いランダム文字列にする）
WEBHOOK_SECRET = os.environ["CLOUDSIGN_WEBHOOK_SECRET"]

# kintone（既存の Railway 環境変数を流用）
#   KINTONE_SUBDOMAIN はサブドメインのみ（例: xxxx）でも、xxxx.cybozu.com でもOK
_sub = os.environ["KINTONE_SUBDOMAIN"]
KINTONE_BASE = _sub if _sub.startswith("http") else f"https://{_sub.replace('.cybozu.com', '')}.cybozu.com"
# 時効援用の案件が入っているアプリのトークン/ID。
#   ※もし時効援用案件が SOUZOKU_* などの別アプリなら、そちらの変数名に差し替える
KINTONE_APP_ID = os.environ["KINTONE_APP_ID"]
KINTONE_API_TOKEN = os.environ["KINTONE_API_TOKEN"]

# kintone のフィールドコード（App 21 の実フィールドコードに合わせる）
FIELD_DOCUMENT_ID = "cloudsign_document_id"  # 送信時に documentID を保存しておくフィールド
FIELD_STATUS = "status"                      # 案件ステータス（DROP_DOWN）のフィールドコード
# HOUKI-CONTRACT-GEN: App 40（相続放棄案件）も同じ cloudsign_document_id / status（受任）
# を持つ。検索は App 21 → App 40 の順。両方に該当があれば fail-closed（処理せず通知）。
# env は houki_card_webhook / houki_case_store と同じ既存のもの（新規 env なし）
HOUKI_APP_ID_ENV = "APP_HOUKI"
HOUKI_TOKEN_ENV = "TOKEN_HOUKI"

# LINE 通知（任意。設定が無ければスキップ）
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
LINE_ADMIN_USER_ID = os.environ.get("LINE_ADMIN_USER_ID")  # 通知先（自分/管理者のuserId）

# クラウドサインの status 値
#   例の JSON では COMPLETED = 2。status は3種の整数を取るので、
#   却下・取消などの他の値は実環境（サンドボックス）で必ず確認すること。
STATUS_COMPLETED = 2


# ============================================================
# クラウドサイン アクセストークン管理
#   有効期限はクラウドサイン側で管理される。300秒バッファで自動更新し、
#   401（失効）が出たら1回だけ取り直す。
# ============================================================
class _CloudSignToken:
    def __init__(self):
        self._token = None
        self._expires_at = 0.0

    def get(self) -> str:
        if self._token and time.time() < self._expires_at - 300:
            return self._token
        resp = requests.post(
            f"{CLOUDSIGN_API_BASE}/token",
            params={"client_id": CLOUDSIGN_CLIENT_ID},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        # レスポンスのフィールド名は仕様書で確認（access_token / expires_in 想定）
        self._token = data["access_token"]
        self._expires_at = time.time() + int(data.get("expires_in", 3600))
        return self._token

    def invalidate(self):
        self._expires_at = 0.0


_token = _CloudSignToken()


def fetch_document(document_id: str) -> dict:
    """書類詳細を取得。Webhook の真正性確認＆締結情報の取得に使う。"""
    url = f"{CLOUDSIGN_API_BASE}/documents/{document_id}"
    headers = {"Authorization": f"Bearer {_token.get()}"}
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code == 401:  # トークン失効 → 取り直して1回だけ再試行
        _token.invalidate()
        headers = {"Authorization": f"Bearer {_token.get()}"}
        resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


# 失敗分類は閉集合の固定文字列のみ（RCF-M05: vendor由来の生値・本文・PII・token を
# 分類文字列に埋め込まない。HTTP コードも下記の既知集合から選ぶ）
_HTTP_CODE_CLASSES = {401: "api_http_401", 403: "api_http_403", 404: "api_http_404"}


def _classify_fetch_error(exc: Exception) -> str:
    """安全ログ用の失敗分類（閉集合の固定文字列のみを返す）"""
    if isinstance(exc, requests.HTTPError):
        code = getattr(getattr(exc, "response", None), "status_code", None)
        return _HTTP_CODE_CLASSES.get(code, "api_http_error")
    if isinstance(exc, requests.Timeout):
        return "api_timeout"
    if isinstance(exc, requests.RequestException):
        return "api_request_error"
    return "unexpected_error"


def verify_completed_document(document_id: str) -> tuple[dict | None, str]:
    """書類詳細 API で webhook の真正性を確認する（fail-closed 化・P0B-002/003）。

    戻り値: (書類dict, "") 成功 ／ (None, 失敗分類) 失敗。
    失敗と判定する条件（いずれも受任遷移させない）:
      - fetch_document の例外（HTTPエラー・タイムアウト・接続失敗ほか）
      - レスポンスが dict でない（想定外レスポンス）
      - レスポンスに id が無い（RCF-H01: id を照合できないものは安全側で失敗。
        CloudSign API が id を返さない仕様だと実機確認できた場合のみ、
        その根拠と実レスポンス fixture をテストに固定した上で別途緩める）
      - レスポンスの id が webhook の documentID と一致しない
      - レスポンスの status が STATUS_COMPLETED(2) でない
        ※書類詳細 API の status 値は webhook と同体系（:50-53 の注のとおり
          実環境/サンドボックスで要確認）。キー欠落も安全側で失敗扱いにする
    失敗分類は固定文字列のみ（RCF-M05）。
    """
    try:
        doc = fetch_document(document_id)
    except Exception as exc:
        return None, _classify_fetch_error(exc)
    if not isinstance(doc, dict):
        return None, "unexpected_response_type"
    doc_id = doc.get("id")
    if doc_id is None:
        return None, "document_id_missing"
    if str(doc_id) != str(document_id):
        return None, "document_id_mismatch"
    if doc.get("status") != STATUS_COMPLETED:
        return None, "status_mismatch"
    return doc, ""


# ============================================================
# kintone 更新
# ============================================================
class AmbiguousDocumentMatch(RuntimeError):
    """HOUKI-CONTRACT-GEN: documentID が App 21 と App 40 の両方に該当（fail-closed）。"""


def _search_by_document_id(app_id: str, token: str, document_id: str) -> list:
    r = requests.get(
        f"{KINTONE_BASE}/k/v1/records.json",
        headers={"X-Cybozu-API-Token": token},
        params={
            "app": app_id,
            "query": f'{FIELD_DOCUMENT_ID} = "{document_id}"',
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json().get("records", [])


def _houki_app() -> tuple[str, str]:
    """App 40 の接続情報（既存 env）。未設定なら ("", "")=検索しない。"""
    return os.environ.get(HOUKI_APP_ID_ENV, ""), os.environ.get(HOUKI_TOKEN_ENV, "")


def update_kintone_status(document_id: str, new_status: str):
    """documentID で案件を探し、ステータスフィールドを更新する。
    成功で kintone レコード No（str）・該当なし/未更新で None を返す（M06: 通知の書類特定用）。

    HOUKI-CONTRACT-GEN: App 21 → App 40 の順で検索。両方に該当があれば
    AmbiguousDocumentMatch（処理せず通知へ）。どちらにも無ければ従来どおり None。
    App 40 の完了時後続処理（status=受任）は時効版と同じ。"""
    # 1. documentID で検索（App 21 → App 40）
    records = _search_by_document_id(KINTONE_APP_ID, KINTONE_API_TOKEN, document_id)
    app_id, token = KINTONE_APP_ID, KINTONE_API_TOKEN
    houki_app, houki_token = _houki_app()
    if houki_app and houki_token and houki_app != KINTONE_APP_ID:
        houki_records = _search_by_document_id(houki_app, houki_token, document_id)
        if records and houki_records:
            logger.warning("kintone両App該当のため処理せず document_id=%s",
                           emit(document_id, "external_ref", "log", "operator"))
            raise AmbiguousDocumentMatch("document id matched in both apps")
        if houki_records:
            records, app_id, token = houki_records, houki_app, houki_token
    if not records:
        logger.warning("kintoneに該当案件なし document_id=%s",
                       emit(document_id, "external_ref", "log", "operator"))
        return None

    record_id = records[0]["$id"]["value"]

    # 2. ステータス更新
    u = requests.put(
        f"{KINTONE_BASE}/k/v1/record.json",
        headers={
            "X-Cybozu-API-Token": token,
            "Content-Type": "application/json",
        },
        json={
            "app": app_id,
            "id": record_id,
            "record": {FIELD_STATUS: {"value": new_status}},
        },
        timeout=10,
    )
    u.raise_for_status()
    logger.info("kintone更新完了 record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    return record_id


# ============================================================
# App 30（発送管理）— 要確認封筒（P1-102b/c・M06 App30封筒方式・司令塔裁定）
#   照合失敗・kintone未更新の失敗経路で documentID を業務チャネルに載せずに
#   人手確認のハンドルを復元する。人は App 30 の「要確認」封筒を kintone 画面で
#   確認しクローズする運用（RESOLVERS ハンドラは作らない＝OUT_OF_SCOPE）。
#   起票は person_merge._file_candidate と同型（単票 API・トップキー付き
#   チャネル固有データ）。cloudsign は sync 経路なので requests で起票する。
# ============================================================
FIELD_CHANNEL_DATA = "チャネル固有データ"


def _mismatch_key(document_id: str) -> str:
    """要確認封筒の冪等キー。"""
    return f"cloudsign_mismatch:{document_id}"


def _escape_kintone_query_value(value: str) -> str:
    """kintone クエリ文字列リテラル用のエスケープ（M07: documentID の特殊文字）。
    二重引用符・バックスラッシュを無害化し、like 構文の破壊・誤マッチを防ぐ。"""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _find_mismatch_envelopes(app_id: str, token: str,
                             key: str) -> list[tuple[int, str, str]]:
    """冪等キー key の要確認封筒 (record No int, 発送ステータス, $revision) を No 昇順で返す（M07）。

    like で絞り込んだ後、各レコードのチャネル固有データ JSON をコード側でパースし、
    トップキー cloudsign_mismatch が存在し「冪等キー」が **完全一致** するものだけ採用する
    （部分文字列 like の誤マッチ＝別 documentID の封筒を確実に排除する）。発送ステータスは
    収束（H03）で「要確認」の敗者だけを閉じるため、$revision は却下 PUT の楽観ロック
    （H03 TOCTOU: 検索から PUT までの間の人更新を弾く）のために持ち回る。
    """
    esc = _escape_kintone_query_value(key)
    r = requests.get(
        f"{KINTONE_BASE}/k/v1/records.json",
        headers={"X-Cybozu-API-Token": token},
        params={"app": app_id,
                "query": f'{FIELD_CHANNEL_DATA} like "{esc}"',
                "fields[0]": "$id",
                "fields[1]": FIELD_CHANNEL_DATA,
                "fields[2]": "発送ステータス",
                "fields[3]": "$revision"},
        timeout=10,
    )
    r.raise_for_status()
    out: list[tuple[int, str, str]] = []
    for rec in r.json().get("records", []):
        raw = rec.get(FIELD_CHANNEL_DATA, {}).get("value", "")
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            continue
        mm = data.get("cloudsign_mismatch") if isinstance(data, dict) else None
        if isinstance(mm, dict) and mm.get("冪等キー") == key:
            rid = rec.get("$id", {}).get("value")
            status = rec.get("発送ステータス", {}).get("value", "")
            revision = rec.get("$revision", {}).get("value", "")
            if rid is not None:
                try:
                    out.append((int(rid), status, revision))
                except (ValueError, TypeError):
                    continue
    return sorted(out, key=lambda t: t[0])


def _close_envelope(app_id: str, token: str, record_id: int,
                    revision: str) -> bool:
    """要確認封筒を revision 楽観ロック付きで「却下」クローズする（H03 TOCTOU）。

    revision は **必須**。取得できていない（None/""）ときは楽観ロックが張れないため
    PUT を送らず False を返す（revision 無指定の無条件 PUT フォールバックはしない＝
    更新しない側へ縮退し、人確認に委ねる）。
    検索時に取得した revision を PUT に指定し、検索〜PUT の間に人が更新していれば
    kintone は 409（GAIA_CO02）を返す。その場合も **人更新優先** として却下せず
    False を返す（再試行しない）。True=却下成功。409 以外の失敗は例外を送出。
    """
    if revision in (None, ""):
        return False              # revision 欠落＝楽観ロック不可 → 却下しない
    u = requests.put(
        f"{KINTONE_BASE}/k/v1/record.json",
        headers={"X-Cybozu-API-Token": token, "Content-Type": "application/json"},
        json={"app": app_id, "id": str(record_id), "revision": str(revision),
              "record": {"発送ステータス": {"value": "却下"}}},
        timeout=10,
    )
    if u.status_code == 409:      # revision 競合＝人が先に更新（楽観ロック衝突）
        return False
    u.raise_for_status()
    return True


def _converge_envelopes(app_id: str, token: str,
                        matches: list[tuple[int, str, str]]) -> str:
    """完全一致封筒群を最小 No（winner）1 件へ収束させ、winner の No（str）を返す（H03）。

    遵守事項（R-P1-102 3〜5巡目レビューの安全条件）:
      - winner（最小 No）は絶対に閉じない・更新しない。
      - 敗者クローズ対象は「要確認」状態のもののみ（検索時 status で保護）。
      - 却下 PUT は検索時 revision の楽観ロック付き（revision 必須）。revision が
        取得できない敗者は却下せず人確認へ委ねる（無条件 PUT はしない）。
      - 検索〜PUT の間に人が更新した敗者は revision 競合で却下せず人更新優先（再試行なし）。
      - クローズの一部が失敗しても winner No を返し通知を維持する（warning のみ）。
    """
    matches = sorted(matches, key=lambda t: t[0])
    winner = matches[0][0]
    close_failed = False
    for rid, status, revision in matches:
        if rid == winner:
            continue                 # winner は絶対に閉じない
        if status != "要確認":
            continue                 # 人処理済み（完了・却下・実行済み等）は触らない
        if revision in (None, ""):
            # revision 欠落＝楽観ロック不可 → 却下せず人確認に委ねる
            logger.warning("要確認封筒: revision欠落・却下スキップ・人確認へ")
            continue
        try:
            if not _close_envelope(app_id, token, rid, revision):
                # 検索後に人が更新（revision 競合）→ 人更新優先で却下しない
                logger.warning("要確認封筒の却下を人更新優先でスキップ（revision競合）")
        except Exception:
            close_failed = True
    if close_failed:
        logger.warning("要確認封筒の収束クローズに一部失敗（winner No で通知継続）")
    return str(winner)


def file_mismatch_envelope(document_id: str, failure_reason: str):
    """失敗経路で App 30 へ「要確認」封筒を起票し、通知に使う record No（str）を返す。

    並行時も**後続再送を含め最終的に winner（最小 No）1 件へ収束する**（compensation・H03）:
    冪等キー完全一致（M07）で封筒を検索し、**複数一致した全ての時点**（既存検索でも
    create 後の再検索でも）で最小 No を winner とし、winner 以外の「要確認」封筒を却下で
    クローズする。相互不可視な race（A/B が互いの封筒を見ずに create）でも、次の再送の
    既存検索で両方が見えた時点で winner 1 件へ収束する。完全一意（重複を作らせない）は
    RCF-M07（App 30 専用フィールド＋重複禁止設定・CU 作業）で DEFER。

    **人処理済みの保護は二段で保証する**: (1) 検索時の発送ステータスで「要確認」の敗者
    だけを対象にし、(2) 却下 PUT に検索時 revision を指定する楽観ロックで、検索〜PUT の
    間に人が更新した敗者（TOCTOU）は revision 競合として却下せず人更新を優先する。
    楽観ロックは必須で、revision が取得できない場合は却下せず人確認に委ねる（欠落時は
    更新しない側へ縮退する）。

    App 30 未設定・起票/検索失敗は None を返す（呼び出し側で縮退通知へフォールバック）。
    """
    app_id = os.environ.get("APP_SHIPPING", "")
    token = os.environ.get("TOKEN_SHIPPING", "")
    if not (app_id and token):
        logger.warning("App30(発送管理)未設定のため要確認封筒を起票できず縮退します")
        return None
    key = _mismatch_key(document_id)
    try:
        # M07/H03: 冪等キー完全一致で既存封筒を検索。複数あれば（後続再送の収束点）
        # winner へ収束、1 件ならそれを再利用（CloudSign 再送で封筒が増えない）。
        existing = _find_mismatch_envelopes(app_id, token, key)
        if existing:
            rid = (_converge_envelopes(app_id, token, existing)
                   if len(existing) > 1 else str(existing[0][0]))
            logger.info("要確認封筒は起票済み No=%s doc=%s",
                        emit(rid, "record_id", "log", "operator"),
                        emit(document_id, "external_ref", "log", "operator"))
            return rid

        detail = {
            "冪等キー": key,
            "documentID": document_id,
            "失敗理由": failure_reason,
        }
        record = {
            "発送ステータス": {"value": "要確認"},
            "方向": {"value": "受領"},
            "チャネル": {"value": "スキャン受領"},
            "ユニット種別": {"value": "時効援用"},
            "件名": {"value": "CloudSign照合失敗・要確認（自動起票）"},
            "エラー詳細": {"value": json.dumps(detail, ensure_ascii=False)[:500]},
            FIELD_CHANNEL_DATA: {"value": json.dumps(
                {"cloudsign_mismatch": detail}, ensure_ascii=False)},
            "実行済み": {"value": "no"},
        }
        # 単票 API（record.json）で起票（一括 API は Webhook 非送信・app30_filer 準拠）
        c = requests.post(
            f"{KINTONE_BASE}/k/v1/record.json",
            headers={"X-Cybozu-API-Token": token,
                     "Content-Type": "application/json"},
            json={"app": app_id, "record": record},
            timeout=10,
        )
        c.raise_for_status()
        created = c.json()
        my_id = int(created["id"])
        my_rev = created.get("revision", "")   # 自レコードの却下クローズ用 revision

        # H03（compensation）: create 後に再検索して見えた同キー封筒で収束する。
        # 読み後れで自レコードが検索に出ない場合に備え、自 No（要確認・自 revision）を含める。
        # 再検索が失敗した場合のみ自 No で通知する（個々のクローズ失敗は _converge_envelopes
        # 内で warning 記録し winner No を維持する＝通知は必ず出る）。
        winner = str(my_id)
        try:
            matches = _find_mismatch_envelopes(app_id, token, key)
            if my_id not in [m[0] for m in matches]:
                matches.append((my_id, "要確認", my_rev))
            if len(matches) > 1:
                winner = _converge_envelopes(app_id, token, matches)
        except Exception:
            logger.warning("要確認封筒の収束処理に失敗、自Noで通知します")
            winner = str(my_id)

        logger.info("要確認封筒を起票 No=%s doc=%s",
                    emit(winner, "record_id", "log", "operator"),
                    emit(document_id, "external_ref", "log", "operator"))
        return winner
    except Exception:
        # 起票・検索の失敗は致命ではない（通知は必ず出す・縮退へフォールバック）
        logger.error("要確認封筒の起票に失敗 (request failed)")  # 固定分類・L01
        return None


# ============================================================
# LINE 通知
# ============================================================
def notify_line(message: str) -> None:
    if not (LINE_CHANNEL_ACCESS_TOKEN and LINE_ADMIN_USER_ID):
        return
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "to": LINE_ADMIN_USER_ID,
                "messages": [{"type": "text", "text": message}],
            },
            timeout=10,
        )
    except Exception:
        logger.error("LINE通知失敗 (request failed)")  # 固定分類・L01


def notify_business_line(message: str) -> None:
    """業務通知（締結完了・照合失敗の要人手確認）を業務指示Botチャネルで送る。

    M03: 共通アダプタ相当の防御を sync 経路にも適用する:
      - 宛先 allowlist（hub.notify.business_channel_allowlist・迂回防止）
      - 非2xx の確認（従来は status を見ていなかった）＋vendor 生値の emit 抑止
      - 送信成功で dead-man heartbeat を記録（sync 版・best-effort）
    DISPATCHBOT 未設定/宛先未許可なら送信しない（顧客Bot へのフォールバックはしない）。
    """
    from hub.notify import business_channel_allowlist

    token = os.environ.get("DISPATCHBOT_CHANNEL_ACCESS_TOKEN", "")
    to = (os.environ.get("LINE_ADMIN_USER_ID", "")
          or os.environ.get("ATTORNEY_LINE_USER_ID", ""))
    if not (token and to):
        logger.warning(
            "business LINE notify skipped (DISPATCHBOT token or admin id unset)")
        return
    if to not in business_channel_allowlist():
        logger.warning("business LINE notify skipped (recipient not allowlisted)")
        return
    try:
        resp = requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"to": to, "messages": [{"type": "text", "text": message[:4900]}]},
            timeout=10,
        )
    except Exception:
        logger.error("business LINE通知失敗 (request failed)")  # 固定分類・L01
        return
    if not resp.ok:
        logger.error("business LINE通知失敗 status=%s body=%s",
                     emit(resp.status_code, "count", "log", "operator"),
                     emit(resp.text, "vendor_raw", "log", "operator"))
        return
    try:
        from hub.notify_heartbeat import record_success_sync
        record_success_sync("business")
    except Exception:
        logger.warning("business heartbeat record failed (non-fatal)")


# ============================================================
# コア処理（フレームワーク非依存）
#   返り値: (HTTPステータスコード, レスポンスdict)
# ============================================================
def handle_webhook(secret: str, payload: dict) -> tuple[int, dict]:
    # 合言葉チェック（一致しなければ存在しないフリ＝404）
    if not hmac.compare_digest(secret or "", WEBHOOK_SECRET):
        return 404, {"error": "not found"}

    document_id = payload.get("documentID")
    status = payload.get("status")
    # H01: documentID は external_ref のため抑止（INFO 配線後は log が可視のため）
    logger.info("CloudSign webhook受信 doc=%s status=%s",
                emit(document_id, "external_ref", "log", "operator"),
                emit(status, "count", "log", "operator"))

    if status == STATUS_COMPLETED and document_id:
        doc, failure = verify_completed_document(document_id)
        if doc is None:
            # fail-closed（P0B-002 / R0A-B03）: 真正性を確認できないイベントでは
            # 業務 state（受任）を進めない・顧客チャネルの通知も出さない。
            # H01 裁定: documentID は external_ref のため抑止（この時点で封筒 record No は
            # 未取得＝抑止のみ。取得後は file_mismatch_envelope 側が record No を emit で残す）。
            # ログは失敗分類のみ可視（本文・PII・token・ベンダー生レスポンスは出さない）。
            logger.warning("CloudSign照合失敗のため受任へ遷移せず doc=%s reason=%s",
                           emit(document_id, "external_ref", "log", "operator"), failure)
            # P1-102b（M06 App30封筒方式・司令塔裁定）: documentID は業務チャネルに
            # 載せず、App 30 へ「要確認」封筒を起票して record No でハンドルを復元する。
            # 起票失敗時は通知を必ず出す縮退動作へフォールバック（内容は抑止・fail-closed）。
            env_no = file_mismatch_envelope(document_id, failure)
            if env_no:
                notify_business_line(
                    "【CloudSign照合失敗・要確認】\n"
                    f"要確認封筒 record No: {env_no}\n"
                    f"失敗分類: {failure}\n"
                    "受任への自動遷移は行っていません。"
                    "App 30（発送管理）の当該封筒を確認してください。")
            else:
                notify_business_line(
                    "【CloudSign照合失敗・要人手確認】\n"
                    f"参照ID: {emit(document_id, 'external_ref', 'line_business', 'attorney')}\n"
                    f"失敗分類: {failure}\n"
                    "受任への自動遷移は行っていません。"
                    "CloudSign管理画面で締結状況を確認してください。")
            # 再送方針: CloudSign が非2xx応答時に再送するか・何回で打ち切るか・
            # webhook を自動無効化するかは、リポジトリ内資料からは確定できない
            # （BLOCKED_NEEDS_HUMAN）。不用意に非2xxを返すと再送ループや配信停止を
            # 招き得るため、受理応答は従来どおり 200 を維持する。
            # 「業務 state を進めていない」ことは state=verification_failed と
            # 上記ログで判別できる（イベントの永続 journal 化は Phase 1）。
            return 200, {"ok": True, "state": "verification_failed"}

        try:
            record_no = update_kintone_status(document_id, "受任")
        except AmbiguousDocumentMatch:
            # HOUKI-CONTRACT-GEN: App 21 と App 40 の両方に該当＝どちらの案件か
            # 機械判定できないため処理せず（受任へ進めない）要確認封筒+通知
            env_no = file_mismatch_envelope(document_id, "kintone_ambiguous_match")
            if env_no:
                notify_business_line(
                    "【CloudSign締結・App二重該当・要確認】\n"
                    f"要確認封筒 record No: {env_no}\n"
                    "失敗分類: kintone_ambiguous_match\n"
                    "documentID が時効援用案件と相続放棄案件の両方に一致したため、"
                    "受任への自動遷移は行っていません。"
                    "App 30（発送管理）の当該封筒を確認してください。")
            else:
                notify_business_line(
                    "【CloudSign締結・App二重該当・要人手確認】\n"
                    f"参照ID: {emit(document_id, 'external_ref', 'line_business', 'attorney')}\n"
                    "失敗分類: kintone_ambiguous_match\n"
                    "documentID が時効援用案件と相続放棄案件の両方に一致しました。"
                    "cloudsign_document_id を突合してください。")
            return 200, {"ok": True, "state": "ambiguous_match"}
        if not record_no:
            # RCF-M04: 照合は成功したが kintone に一致案件がない（未更新）。
            # 受任は成立していないので「締結完了」通知は出さず、要人手確認を
            # 業務チャネルで警報する（顧客チャネルへは出さない）。
            # M06: この経路は kintone レコード No が引けない＝documentID を抑止すると
            # 相関手段が無くなる（司令塔裁定待ち・COMPLETION_REPORT に BLOCKED 記載）。
            # P1-102b（M06 App30封筒方式・司令塔裁定）: record No が引けない失敗経路も
            # documentID を業務チャネルに載せず、App 30 の「要確認」封筒でハンドルを復元。
            # 起票失敗時は通知を必ず出す縮退動作へフォールバック（内容は抑止・fail-closed）。
            logger.warning("CloudSign照合成功だがkintone未更新 doc=%s",
                           emit(document_id, "external_ref", "log", "operator"))
            env_no = file_mismatch_envelope(document_id, "kintone_no_match")
            if env_no:
                notify_business_line(
                    "【CloudSign締結・kintone未更新・要確認】\n"
                    f"要確認封筒 record No: {env_no}\n"
                    "失敗分類: kintone_no_match\n"
                    "documentID に一致する案件レコードが見つかりませんでした。"
                    "App 30（発送管理）の当該封筒を確認してください。")
            else:
                notify_business_line(
                    "【CloudSign照合成功・kintone未更新・要人手確認】\n"
                    f"参照ID: {emit(document_id, 'external_ref', 'line_business', 'attorney')}\n"
                    "documentID に一致する案件レコードが見つからず、受任へ更新できて"
                    "いません。CloudSign の直近の締結完了と kintone の "
                    "cloudsign_document_id を突合してください。")
            return 200, {"ok": True, "state": "kintone_update_failed"}
        # P1-102/102a（RV-10 S1・M06）: 業務チャネルへ。documentID は external_ref
        # で抑止し、書類特定は kintone レコード No（内部参照）で行う。
        notify_business_line(
            f"【締結完了】案件レコードNo: {record_no}\n"
            f"参照ID: {emit(document_id, 'external_ref', 'line_business', 'attorney')}\n"
            "kintone の該当案件（受任へ更新済）を確認してください。")
        return 200, {"ok": True, "state": "processed"}

    # 締結完了以外のイベント（却下・取消等）は従来どおり何もしない
    # クラウドサインには常に200を返す（再送ループを防ぐ）
    return 200, {"ok": True, "state": "skipped"}


# ============================================================
# FastAPI アダプタ
#   既存アプリで:  app.include_router(router)
#   登録URL:      https://<app>.up.railway.app/cloudsign/webhook/<SECRET>
# ============================================================
try:
    from fastapi import APIRouter, Request
    from fastapi.responses import JSONResponse

    router = APIRouter()

    @router.post("/cloudsign/webhook/{secret}")
    async def cloudsign_webhook(secret: str, request: Request):
        payload = await request.json()
        code, body = handle_webhook(secret, payload)
        return JSONResponse(status_code=code, content=body)

except ImportError:
    router = None


# ============================================================
# Flask アダプタ（Flask を使っている場合はこちらを利用）
# ------------------------------------------------------------
# from flask import Flask, request, jsonify
#
# @app.route("/cloudsign/webhook/<secret>", methods=["POST"])
# def cloudsign_webhook(secret):
#     code, body = handle_webhook(secret, request.get_json(force=True))
#     return jsonify(body), code
# ============================================================
