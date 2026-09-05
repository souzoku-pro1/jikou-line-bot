"""時効診断フォーム（公開ページ）— JIKOU-FORM-1（fix1 反映）

HP 設置の簡易診断: 4 問（①債権者名・②借入時期・③最終返済・④裁判所書類）
→ サーバ側ルール判定（A〜D）→ 弁護士凍結の結果文言+受付番号を表示し、
LINE 友だち追加へ誘導する。回答は App 21 へ保存（LINE 紐付けは FORM-2・
写真アップロードは FORM-3 の別票）。

設計（票の逐語+SURVEY 実測）:
- prefix /shindan（/app 配下に置かない=test_p4_001 の PUBLIC_ROUTES pin 維持）
- 公開条件 fail-closed: env JIKOU_LINE_ADD_URL（時効 LINE 友だち追加 URL）が
  未設定/空白なら全メソッド・全別名で 404（存在しないフリ・shinjutsu_webhook 同型）
- 判定はサーバ側のみ（優先順 C→B→D→A・②は判定に使わず保存のみ・
  「その他の督促通知」は裁判所手続でないため C 非該当=司令塔裁定）
- 凍結文言（FROZEN_RESULTS/FROZEN_NOTE）は test_jikou_form1 が sha256 pin。
  {受付番号} プレースホルダのみ実値置換（改変禁止）
- 非反射: 入力値をエラー応答・結果画面のどこにも出さない（固定文言のみ）。
  結果画面に出る可変値は**サーバ生成の受付番号のみ**
- App 21 保存は hub.kintone（plain 値契約・HOUKI-STORE-FIX1 の教訓）。
  受付番号=secrets 乱数 6 桁ゼロ埋め・「値の重複を禁止する」（CU 済み実測）。
  レコード番号の流用は禁止（連番=推測可能で FORM-2 の紐付け乗っ取りリスク）
- 必須 RADIO 4 種（既定値「あり」の誤登録防止・form fields API 実測）:
  １０年以内の訴訟の有無←④写像／他 3 種（住民票相違・業者電話・アンケート
  送付）は④から写像できないため「不明」を明示指定（既定値任せにしない）
- 弁護士通知は notify_business 流儀・固定文言+受付番号+診断パターンのみ
  （債権者名・回答本文は載せない=PII 規律）。通知は best-effort
  （App 21 レコードが正本・失敗しても受付は成立）

fix1（R-JIKOU-FORM-1 01〜04）:
- 01 二重作成防止: 再採番は「kintone の一意制約違反」と確認できた閉集合
  （HTTP 400 / code CB_VA01 / errors["record.受付番号.value"] 在）のみ。
  結果不明（transport 例外=status 0・5xx）は**即 unknown**=固定 500+要確認通知
  （通知本文に区分と受付番号を含め弁護士が突合できるようにする・PII 非搭載）。
  403・スキーマ不整合（CB_VA01 でも他欄）等の確定失敗は再採番せず即 500+要確認
  fix2（fix1-01）: 結果不明時の「受付番号で照会→在れば成功扱い」と「同番号で
  create 再試行」は撤去した。受付番号は 6 桁乱数で他申込と衝突し得るため、
  番号一致だけでは今回の書込か別申込の既存レコードかを識別できず、成功扱いに
  すると今回の回答を保存していないのに番号を表示・通知し FORM-2 で別顧客の
  案件へ紐付く経路が生じる（同番号再試行の一意違反も同様に識別不能）。
  要確認通知には「同番号のレコードがあっても今回の申込とは限らない」旨を明記。
  将来 transport 障害が実運用で頻発する場合の改善案: App 21 に申込 ID 欄
  （本モジュール生成の UUID・UNIQUE）を CU で追加し、申込 ID 一致で所有元を
  確定できる照会に置き換える（Codex 第 1 案・CU を伴うため今回は不採用）
- 02 クライアント IP: 信頼済み proxy の契約に基づくヘッダのみ採用
  （env SHINDAN_CLIENT_IP_HEADER・既定 X-Real-IP）。X-Forwarded-For は採用しない
  （env で指定されても既定へ倒す）。ヘッダ欠落時は request.client.host
- 03 有界バケット: OrderedDict の LRU・MAX_BUCKETS を厳密上限とし、上限到達時は
  最古（最終 touch が最も古い）を退避。期限切れ掃除は先頭から定数ステップ
  （償却 O(1)）で、リクエストごとの全件走査をしない
- 04 ゲート順序: 素の Request を受ける単一入口（Form パラメータ依存なし）で
  ①env→②メソッド/別名→③Content-Type/Content-Length（MAX_BODY_BYTES）→④レート
  を通過した後にのみ body を読む。body は urlencoded のみ自前解析
  （starlette の FormParser/MultiPartParser を呼ばない=一時ファイル化なし）。
  別名（末尾スラッシュ・%2F・配下パス）は明示 catch-all route で 404（307 なし）
"""

import hashlib
import html
import logging
import os
import re
import secrets
import time
from collections import OrderedDict
from urllib.parse import parse_qsl

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from hub import image_store
from hub import kintone as hub_kintone
from hub import notify
from hub.redact import emit

logger = logging.getLogger("shindan")

router = APIRouter()

APP_JIKOU_CASE = hub_kintone.KintoneApp(
    "App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")

_LINE_URL_ENV = "JIKOU_LINE_ADD_URL"

# ── 弁護士凍結事項（逐語・改変禁止・sha256 pin は test_jikou_form1） ─────────────
FROZEN_RESULTS = {
    "A": ("ご回答の内容からは、消滅時効を援用できる可能性が高いと考えられます。\n"
          "時効の成立が認められれば、債務が消滅し、支払う必要がなくなります。"
          "ただし、最終的な判断には弁護士による確認が必要です。\n"
          "LINEの無料相談へお進みください。\n"
          "受付番号：{受付番号}"),
    "B": ("ご回答の内容からは、現時点で時効が成立している可能性は低いと"
          "考えられます。\n"
          "ただし、返済の時期や経緯によっては、判断が変わることがあります。\n"
          "詳しくは、LINEの無料相談で弁護士にご確認ください。\n"
          "受付番号：{受付番号}"),
    "C": ("裁判所から訴状や支払督促などが届いている場合、通常とは時効の期間が"
          "異なる可能性があります。\n"
          "正確に判断するためには、裁判所から届いた書類の確認が必要です。\n"
          "お手元の書類を撮影して、LINEの無料相談へお進みください。\n"
          "受付番号：{受付番号}"),
    "D": ("ご回答の内容だけでは、時効が成立しているか判断できませんでした。\n"
          "不明な点があっても、時効を援用できる可能性はあります。わかる範囲で"
          "結構ですので、LINEの無料相談で弁護士にご相談ください。\n"
          "受付番号：{受付番号}"),
}
FROZEN_NOTE = ("本診断は簡易的な目安であり、法的判断を確定するものではありません。"
               "正式な判断は弁護士がご相談内容を確認のうえ行います。")

# ── 選択肢の閉集合（サーバ側検証・選択肢外=固定 400） ────────────────────────────
CHOICES_BORROW = ("5年以上前", "5年以内", "不明")                     # ②
CHOICES_LAST_PAY = ("5年以上前", "5年以内", "不明")                   # ③
CHOICES_COURT_DOC = ("訴状が届いた", "支払督促が届いた",
                     "その他の督促通知が届いた", "何も届いていない", "不明")  # ④

CREDITOR_MAX_CHARS = 100

# ④ → App 21 必須 RADIO「１０年以内の訴訟の有無」（あり/なし/不明）の写像。
# 訴状・支払督促=裁判所手続あり／その他督促・何も届いていない=なし／不明=不明
_COURT_DOC_TO_SOSHO = {
    "訴状が届いた": "あり",
    "支払督促が届いた": "あり",
    "その他の督促通知が届いた": "なし",
    "何も届いていない": "なし",
    "不明": "不明",
}

# ── 受付番号（6 桁ゼロ埋め乱数・一意制約違反の確定時のみ再採番） ─────────────────
_NUMBER_ATTEMPTS = 5      # 再採番の上限（一意制約違反が続いた場合）
_NUMBER_FIELD = "受付番号"

# 要確認通知の固定注意文（fix2）: 受付番号は乱数で他申込と衝突し得るため、
# 同番号のレコードの存在は今回の申込の保存を意味しない
ALERT_OTHER_APPLICANT_NOTE = (
    "App 21 に同じ受付番号のレコードがあっても、今回の申込のものとは限りません"
    "（別申込の可能性あり）。")

# 一意制約違反の閉集合（kintone REST API の検証エラー形: HTTP 400 / code CB_VA01 /
# errors={"record.<fieldcode>.value": {"messages": [...]}}。hub.kintone._raise_error
# が errors 詳細を KintoneError.errors に保持する）。**受付番号の欄に対する
# CB_VA01 のみ**を一意制約違反とみなす（受付番号は本モジュールが生成する 6 桁
# 数字なので、この欄で起き得る検証エラーは重複のみ）。他欄の CB_VA01 はスキーマ
# 不整合=確定失敗。code/欄が特定できない場合は再採番しない（fail-closed）
UNIQUE_VIOLATION_STATUS = 400
UNIQUE_VIOLATION_CODES = frozenset({"CB_VA01"})
UNIQUE_VIOLATION_ERROR_KEY = f"record.{_NUMBER_FIELD}.value"


def _draw_number() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def _is_unique_violation(e: hub_kintone.KintoneError) -> bool:
    return (e.status == UNIQUE_VIOLATION_STATUS
            and e.code in UNIQUE_VIOLATION_CODES
            and UNIQUE_VIOLATION_ERROR_KEY in (getattr(e, "errors", None) or {}))


def _classify_create_error(e: hub_kintone.KintoneError) -> str:
    """create 失敗の三分類:
    duplicate=一意制約違反（確定・再採番可）／
    unknown=結果不明（transport 例外=status 0・5xx=書込が着いた可能性あり）／
    failed=確定失敗（上記以外の 4xx: 403・401・404・他欄の CB_VA01 等）"""
    if _is_unique_violation(e):
        return "duplicate"
    if e.status == 0 or e.status >= 500:
        return "unknown"
    return "failed"


async def _persist_with_number(fields_base: dict, number: str) -> tuple[str, str]:
    """受付番号を付けて 1 回だけ create を送る。戻り値 (outcome, record_id):
    created=保存確定／duplicate=一意制約違反（確定・再採番可）／
    failed=確定失敗／unknown=結果不明（レコードの有無を確定できない）。

    fix2: unknown に対する番号照会・同番号再試行は行わない（番号一致では
    今回の書込か別申込の既存レコードかを識別できないため=fail-closed）。"""
    fields = {**fields_base, _NUMBER_FIELD: number}
    try:
        return "created", await hub_kintone.create_record(APP_JIKOU_CASE, fields)
    except hub_kintone.KintoneError as e:
        kind = _classify_create_error(e)
        logger.warning("[SHINDAN] create failed kind=%s code=%s",
                       emit(kind, "freetext", "log", "operator"),
                       emit(e.code, "vendor_raw", "log", "operator"))
        return kind, ""


# ── クライアント IP の導出（02: 信頼済み proxy ヘッダのみ） ───────────────────────
# Railway の公開仕様（docs.railway.com → Public Networking → Specs & Limits）は
# edge proxy が付加するヘッダとして「X-Real-IP for identifying client's remote IP」
# を明記している。本モジュールはこの契約に依存し、既定で X-Real-IP のみを採用する。
# X-Forwarded-For は多段 proxy でクライアント側の付加値と proxy 付加値が混在し、
# どの要素を信頼できるかがデプロイ環境に依存するため採用しない（env で指定されても
# 既定へ倒す）。proxy 構成が変わった場合は env SHINDAN_CLIENT_IP_HEADER で切り替える。
CLIENT_IP_HEADER_ENV = "SHINDAN_CLIENT_IP_HEADER"
DEFAULT_CLIENT_IP_HEADER = "X-Real-IP"
_REJECTED_IP_HEADERS = frozenset({"x-forwarded-for"})


def _client_ip_header() -> str:
    name = os.environ.get(CLIENT_IP_HEADER_ENV, "").strip()
    if not name or name.lower() in _REJECTED_IP_HEADERS:
        return DEFAULT_CLIENT_IP_HEADER
    return name


def _rate_key(request: Request) -> str:
    """信頼済み proxy ヘッダ（既定 X-Real-IP）の値を SHA-256 のみで保持する
    （生 IP を状態・ログに残さない）。ヘッダ欠落時（直接接続・テスト）は
    request.client.host に fallback。"""
    ip = (request.headers.get(_client_ip_header().lower(), "") or "").strip()
    if not ip:
        client = getattr(request, "client", None)
        ip = getattr(client, "host", "") if client else ""
    return hashlib.sha256((ip or "").encode("utf-8")).hexdigest()


# ── レート制限（03: 固定窓・OrderedDict LRU の厳密上限・単一 worker 前提） ─────────
RATE_WINDOW_SECONDS = 600
RATE_LIMIT = 10
MAX_BUCKETS = 5000
_PRUNE_STEPS = 2          # リクエストごとに先頭（最古 touch）から見る定数ステップ
_attempts: "OrderedDict[str, tuple[float, int]]" = OrderedDict()


def _bump_fixed_window(bucket: "OrderedDict[str, tuple[float, int]]",
                       key: str, now: float, limit: int, window: float,
                       max_buckets: int) -> bool:
    """固定窓カウンタを 1 進め、上限超過なら True。窓満了で自然解除。
    - 期限切れ掃除: 先頭（最終 touch が最も古い）から _PRUNE_STEPS 件だけ見て
      期限切れなら捨てる（償却 O(1)・全件走査なし）
    - 上限: 新規キーで len が max_buckets に達していれば最古を 1 件退避してから
      挿入する（辞書サイズは max_buckets を超えない）。退避=429 ではなく受入
      （新規利用者の可用性優先。退避で制限を外すには max_buckets 個の別 IP から
      の送信が要り、それ自体が上限回数×max_buckets を超えるコストになる）
    FORM-3: 本申込（_attempts）と写真アップロード（_photo_attempts）の専用
    バケットで共用する汎用形（本申込側の挙動は不変）。"""
    for _ in range(_PRUNE_STEPS):
        if not bucket:
            break
        oldest_key = next(iter(bucket))
        if now - bucket[oldest_key][0] >= window:
            del bucket[oldest_key]
        else:
            break
    if key in bucket:
        start, count = bucket[key]
        if now - start >= window:
            start, count = now, 0
        bucket.move_to_end(key)
    else:
        start, count = now, 0
        while len(bucket) >= max_buckets:
            bucket.popitem(last=False)           # 最古（LRU）を退避
    bucket[key] = (start, count + 1)
    return count + 1 > limit


def _rate_exceeded(key: str, now: float) -> bool:
    """本申込（/shindan）のレート制限（FORM-1 のまま・専用バケット _attempts）。"""
    return _bump_fixed_window(_attempts, key, now, RATE_LIMIT,
                              RATE_WINDOW_SECONDS, MAX_BUCKETS)


# ── FORM-3 Part B: 写真アップロード（第 2 段）の上限・トークン・専用バケット ──────
# 画面文言（お客様向け・弁護士裁定で差し替え可・凍結後は test_jikou_form3 が pin）
PHOTO_PROMPT_TEXT = (
    "督促状・請求書・訴状などのお写真をお持ちの場合は、こちらから送信できます"
    "（任意・5枚まで）。後からLINEでお送りいただくこともできます。")
PHOTO_DONE_TEXT = "お写真を受け付けました。LINEの無料相談へお進みください。"

# 上限（実装判断・票報告に明記）
PHOTO_MAX_PARTS = 5                                  # パート数（枚数）上限
PHOTO_MAX_PART_BYTES = image_store.MAX_IMAGE_BYTES   # 1 ファイル 10MB（Part A と同値）
PHOTO_MAX_TOTAL_BYTES = 30 * 1024 * 1024             # Content-Length 上限（body 読取前）
PHOTO_PART_NAME = "photo"                            # 受け取る項目名（これ以外は拒否）
_PHOTO_HEADER_MAX_BYTES = 8 * 1024                   # 1 パートのヘッダ上限
_PHOTO_RATE_LIMIT = 5                                # 写真 POST の専用レート（回/窓）
_photo_attempts: "OrderedDict[str, tuple[float, int]]" = OrderedDict()

# 使い捨てアップロードトークン: token → (発行時刻, record_id, 受付番号)。
# secrets 生成・受付番号（レコード）に紐付け・TTL 15 分・1 回限り（消費で削除）。
# 【単一 worker 前提】in-memory の有界 LRU（MAX_UPLOAD_TOKENS 厳密上限・最古退避）。
# uvicorn workers=1（Procfile 実測・test_image_intake が pin）が前提であり、
# worker 複数化票では永続ストアへ置換すること（_attempts と同じ既知の制約）
UPLOAD_TOKEN_TTL_SECONDS = 15 * 60
MAX_UPLOAD_TOKENS = 5000
# token → (発行時刻, record_id, 受付番号, claimed)
# fix1（H3-01）: 状態は 3 つ——未使用（claimed=False）／予約中（claimed=True・
# 解析〜検査の間だけ）／使用済み（辞書から削除=consumed）。解析・容量・形式の
# 失敗は release で未使用へ戻し（TTL 内なら同じ URL で再送可）、添付呼び出しに
# 進む時点で consume（以後は添付結果に関わらず 404=再試行しない規律）。
# TTL は発行時刻基準のまま（claim/release で延長しない）
_upload_tokens: "OrderedDict[str, tuple[float, str, str, bool]]" = OrderedDict()


def issue_upload_token(record_id: str, number: str, now: float) -> str:
    """結果画面に埋め込む使い捨てトークンを発行する（生成=secrets）。"""
    token = secrets.token_urlsafe(32)
    while len(_upload_tokens) >= MAX_UPLOAD_TOKENS:
        _upload_tokens.popitem(last=False)           # 最古（LRU）を退避
    _upload_tokens[token] = (now, record_id, number, False)
    return token


def _upload_token_entry(token: str, now: float) -> tuple[str, str] | None:
    """有効（存在・未期限・未使用かつ予約中でない）なら (record_id, 受付番号)。
    期限切れは削除。予約中（他の POST が解析中）は無効扱い（404）。"""
    if not token:
        return None
    entry = _upload_tokens.get(token)
    if entry is None:
        return None
    issued, record_id, number, claimed = entry
    if now - issued > UPLOAD_TOKEN_TTL_SECONDS:
        _upload_tokens.pop(token, None)
        return None
    if claimed:
        return None
    return record_id, number


def claim_upload_token(token: str, now: float) -> tuple[str, str] | None:
    """トークンを予約する（check-then-act は await を挟まない同期区間＝
    H4-fix2 / IMG-1 _send_claims と同型・単一 worker 前提）。未使用かつ TTL 内
    なら予約成功・予約中/使用済み/期限切れは None（404）。"""
    entry = _upload_token_entry(token, now)
    if entry is None:
        return None
    issued, record_id, number, _claimed = _upload_tokens[token]
    _upload_tokens[token] = (issued, record_id, number, True)
    return entry


def release_upload_token(token: str) -> None:
    """予約を解除して未使用へ戻す（解析・容量・形式の失敗・予期しない例外）。
    使用済み（削除済み）なら何もしない。"""
    entry = _upload_tokens.get(token)
    if entry is None:
        return
    issued, record_id, number, _claimed = entry
    _upload_tokens[token] = (issued, record_id, number, False)


def consume_upload_token(token: str, now: float | None = None) -> None:
    """使用済みに確定する（辞書から削除・以後は 404）。添付呼び出しに進む時点で
    呼ぶ。添付の結果（成功・失敗・unconverged）に関わらず戻さない。"""
    _upload_tokens.pop(token, None)


def _photo_rate_exceeded(key: str, now: float) -> bool:
    return _bump_fixed_window(_photo_attempts, key, now, _PHOTO_RATE_LIMIT,
                              RATE_WINDOW_SECONDS, MAX_BUCKETS)


# ── ゲート（04: body 読取前に通す関門） ────────────────────────────────────────────
MAX_BODY_BYTES = 64 * 1024        # FORM-1 は写真なし（回答 4 問+100 字）=64KB で十分
_FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"
_MAX_FORM_FIELDS = 16
_ENTRY_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE",
                  "OPTIONS", "TRACE"]


def _disabled() -> bool:
    """公開条件 fail-closed: LINE 友だち追加 URL 未設定なら受け口ごと無効。"""
    return not os.environ.get(_LINE_URL_ENV, "").strip()


def _not_found() -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "not found"})


def _content_length_ok(request: Request) -> bool:
    """Content-Length が数値で MAX_BODY_BYTES 以下（欠落=chunked 等は fail-closed）。"""
    raw = (request.headers.get("content-length", "") or "").strip()
    if not raw.isdigit():
        return False
    return int(raw) <= MAX_BODY_BYTES


def _content_type_ok(request: Request) -> bool:
    """urlencoded のみ受理（multipart は解析器に渡さない=一時ファイル化なし）。"""
    ct = (request.headers.get("content-type", "") or "").split(";", 1)[0]
    return ct.strip().lower() == _FORM_CONTENT_TYPE


def _parse_form(body: bytes) -> dict[str, str] | None:
    """urlencoded を自前解析（starlette の FormParser を呼ばない）。
    復号不能・項目数超過は None（固定 400 へ）。"""
    try:
        pairs = parse_qsl(body.decode("utf-8"), keep_blank_values=True,
                          max_num_fields=_MAX_FORM_FIELDS)
    except (UnicodeDecodeError, ValueError):
        return None
    return {k: v for k, v in pairs}


def judge(last_pay: str, court_doc: str) -> str:
    """ルール判定（優先順 C→B→D→A・②借入時期は判定に使わない=保存のみ）。"""
    if court_doc in ("訴状が届いた", "支払督促が届いた"):
        return "C"
    if last_pay == "5年以内":
        return "B"
    if last_pay == "不明" or court_doc == "不明":
        return "D"
    return "A"


def result_text(pattern: str, number: str) -> str:
    """凍結文言の {受付番号} のみを実値置換する（他の改変は不可）。"""
    return FROZEN_RESULTS[pattern].replace("{受付番号}", number)


# ── HTML（外部アセットなし・JS 必須にしない・モバイルファースト） ─────────────────
_PAGE_STYLE = """
  body{margin:0;padding:16px;background:#f7f5f0;color:#333;
       font-family:'Hiragino Sans','Yu Gothic',Meiryo,sans-serif;
       font-size:16px;line-height:1.7}
  .card{max-width:560px;margin:0 auto;background:#fff;border-radius:12px;
        padding:20px 18px;box-shadow:0 1px 4px rgba(0,0,0,.08)}
  h1{font-size:20px;margin:0 0 14px}
  fieldset{border:none;margin:0 0 18px;padding:0}
  legend{font-weight:bold;margin-bottom:6px}
  label.opt{display:block;padding:6px 2px}
  input[type=text]{width:100%;box-sizing:border-box;padding:10px;
       font-size:16px;border:1px solid #ccc;border-radius:8px}
  .hp{display:none}
  .btn{display:block;width:100%;box-sizing:border-box;text-align:center;
       background:#06c755;color:#fff;font-size:17px;font-weight:bold;
       padding:14px;border:none;border-radius:10px;text-decoration:none}
  .note{font-size:12px;color:#777;margin-top:16px}
  .num{font-size:18px;font-weight:bold}
  h2{font-size:17px;margin:22px 0 8px}
  input[type=file]{display:block;width:100%;box-sizing:border-box;
       margin:8px 0 12px;font-size:15px}
  .btn2{display:block;width:100%;box-sizing:border-box;text-align:center;
       background:#fff;color:#06c755;font-size:16px;font-weight:bold;
       padding:12px;border:2px solid #06c755;border-radius:10px}
"""


def _page(title: str, body_html: str) -> str:
    return (
        "<!doctype html><html lang=\"ja\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        "<meta name=\"robots\" content=\"noindex\">"
        f"<title>{title}</title><style>{_PAGE_STYLE}</style></head>"
        f"<body><div class=\"card\">{body_html}</div></body></html>"
    )


def _radio_group(name: str, legend: str, choices: tuple) -> str:
    inputs = "".join(
        f"<label class=\"opt\"><input type=\"radio\" name=\"{name}\" "
        f"value=\"{html.escape(c, quote=True)}\" required> {html.escape(c)}"
        "</label>"
        for c in choices)
    return f"<fieldset><legend>{legend}</legend>{inputs}</fieldset>"


def _form_html() -> str:
    body = (
        "<h1>消滅時効 かんたん診断（無料）</h1>"
        "<p>4つの質問に答えるだけで、時効援用できる可能性の目安がわかります。</p>"
        "<form method=\"post\" action=\"/shindan\">"
        "<fieldset><legend>① 債権者名（お金を借りた業者・請求してきている業者）"
        "</legend>"
        f"<input type=\"text\" name=\"creditor\" maxlength=\"{CREDITOR_MAX_CHARS}\""
        " placeholder=\"例: ○○ファイナンス（複数社は「、」で区切って記入）\">"
        "</fieldset>"
        + _radio_group("borrow", "② おおよその借入時期", CHOICES_BORROW)
        + _radio_group("last_pay", "③ 最後に返済したのはいつ頃ですか",
                       CHOICES_LAST_PAY)
        + _radio_group("court_doc",
                       "④ 10年以内に裁判所から届いた書類はありますか",
                       CHOICES_COURT_DOC)
        + "<div class=\"hp\" aria-hidden=\"true\">"
          "<label>このらんにはにゅうりょくしないでください"
          "<input type=\"text\" name=\"website\" tabindex=\"-1\""
          " autocomplete=\"off\"></label></div>"
          "<button class=\"btn\" type=\"submit\">診断する</button>"
          "</form>"
          f"<p class=\"note\">{FROZEN_NOTE}</p>"
    )
    return _page("消滅時効かんたん診断", body)


def _photo_section_html(upload_token: str) -> str:
    """FORM-3 Part B: 結果画面の任意の第 2 段（写真送信）。使い捨てトークンは
    専用ルートのパスに載せる（本申込 /shindan の公開面は不変）。"""
    if not upload_token:
        return ""
    return (
        "<h2>お写真の送信（任意）</h2>"
        f"<p>{PHOTO_PROMPT_TEXT}</p>"
        "<form method=\"post\" enctype=\"multipart/form-data\" "
        f"action=\"{PHOTO_ROUTE}/{html.escape(upload_token, quote=True)}\">"
        f"<input type=\"file\" name=\"{PHOTO_PART_NAME}\" "
        "accept=\"image/jpeg,image/png,image/heic,application/pdf\" multiple>"
        "<button class=\"btn2\" type=\"submit\">お写真を送信する</button>"
        "</form>"
    )


def _result_html(pattern: str, number: str, line_url: str,
                 upload_token: str = "") -> str:
    text_html = html.escape(result_text(pattern, number)).replace("\n", "<br>")
    body = (
        "<h1>診断結果</h1>"
        f"<p class=\"num\">{text_html}</p>"
        f"<a class=\"btn\" href=\"{html.escape(line_url, quote=True)}\">"
        "LINEで無料相談する（友だち追加）</a>"
        "<p>LINEで上記の受付番号をお送りいただくと、ご回答内容を引き継いで"
        "スムーズにご案内できます。</p>"
        + _photo_section_html(upload_token)
        + f"<p class=\"note\">{FROZEN_NOTE}</p>"
    )
    return _page("診断結果", body)


def _photo_done_html(count: int, line_url: str) -> str:
    """完了画面（固定文言+枚数のみ・入力値は反射しない）。"""
    body = (
        "<h1>お写真の送信</h1>"
        f"<p class=\"num\">{PHOTO_DONE_TEXT}</p>"
        f"<p>受け付けた枚数: {int(count)}枚</p>"
        f"<a class=\"btn\" href=\"{html.escape(line_url, quote=True)}\">"
        "LINEで無料相談する（友だち追加）</a>"
        f"<p class=\"note\">{FROZEN_NOTE}</p>"
    )
    return _page("お写真の送信", body)


def _fixed_page(title: str, message: str, status: int) -> HTMLResponse:
    """固定文言のみの応答（入力値を一切反射しない）。"""
    return HTMLResponse(_page(title, f"<h1>{title}</h1><p>{message}</p>"),
                        status_code=status)


def _error_500() -> HTMLResponse:
    return _fixed_page(
        "エラーが発生しました",
        "申し訳ありません。システムエラーが発生しました。お手数ですが、"
        "時間をおいてもう一度お試しください。", 500)


# ── 入口（04: 単一 route・素の Request・ゲート通過後にのみ body 読取） ──────────────
async def shindan_entry(request: Request):
    # ① env 未設定 → 全メソッド 404（存在しないフリ）
    if _disabled():
        return _not_found()
    # ② メソッド: GET/HEAD=フォーム表示・POST=送信・他=404
    method = request.method.upper()
    if method in ("GET", "HEAD"):
        return HTMLResponse(_form_html())
    if method != "POST":
        return _not_found()
    # ③ Content-Type/Content-Length（urlencoded のみ・上限超過/欠落は 404）
    if not _content_type_ok(request) or not _content_length_ok(request):
        logger.info("[SHINDAN] body gate rejected (type/length)")
        return _not_found()
    # ④ レート制限（固定窓・キーは SHA-256 のみ保持・超過は固定応答）
    if _rate_exceeded(_rate_key(request), time.time()):
        logger.info("[SHINDAN] rate limited")
        return _fixed_page(
            "アクセスが集中しています",
            "アクセスが集中しています。しばらく時間をおいてから"
            "もう一度お試しください。", 429)
    # ── ここで初めて body を読む（上限を再確認・urlencoded 自前解析） ──
    body = await request.body()
    if len(body) > MAX_BODY_BYTES:
        return _not_found()
    form = _parse_form(body)
    if form is None:
        return _fixed_page(
            "入力内容をご確認ください",
            "入力内容を確認して、もう一度お試しください。", 400)
    return await _handle_submit(form)


async def shindan_alias_not_found(request: Request, _rest: str = ""):
    """末尾スラッシュ・%2F・配下パス等の別名は 307 でなく明示 404。"""
    return _not_found()


# ── FORM-3 Part B: 写真アップロード（専用ルート・multipart はここでのみ受ける） ────
PHOTO_ROUTE = "/shindan/photos"
_MULTIPART_CONTENT_TYPE = "multipart/form-data"
_BOUNDARY_RE = re.compile(r"^[A-Za-z0-9'()+_,\-./:=? ]{1,70}$")
_DISPOSITION_NAME_RE = re.compile(r';\s*name="([^"]*)"')
_DISPOSITION_FILENAME_RE = re.compile(r';\s*filename="([^"]*)"')


class _MultipartLimit(Exception):
    """上限超過（パート数・1 ファイル・合計）＝即中断（残りは読まない）。"""


class _MultipartBad(Exception):
    """multipart の形式不正・許可外の項目名。"""


def _multipart_boundary(request: Request) -> bytes | None:
    """Content-Type が multipart/form-data かつ boundary が妥当なら boundary。"""
    ct = (request.headers.get("content-type", "") or "")
    main_type, _, rest = ct.partition(";")
    if main_type.strip().lower() != _MULTIPART_CONTENT_TYPE:
        return None
    boundary = ""
    for param in rest.split(";"):
        k, _, v = param.strip().partition("=")
        if k.strip().lower() == "boundary":
            boundary = v.strip().strip('"')
    if not boundary or not _BOUNDARY_RE.match(boundary):
        return None
    return boundary.encode("ascii")


def _content_length_within(request: Request, limit: int) -> bool:
    raw = (request.headers.get("content-length", "") or "").strip()
    return raw.isdigit() and int(raw) <= limit


def _parse_part_headers(raw: bytes) -> tuple[str, str | None]:
    """パートヘッダから (name, filename|None) を取り出す。"""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise _MultipartBad()
    name, filename = "", None
    for line in text.split("\r\n"):
        key, _, value = line.partition(":")
        if key.strip().lower() != "content-disposition":
            continue
        m = _DISPOSITION_NAME_RE.search(value)
        name = m.group(1) if m else ""
        m = _DISPOSITION_FILENAME_RE.search(value)
        filename = m.group(1) if m else None
    return name, filename


async def _parse_multipart_stream(stream, boundary: bytes) -> list[bytes]:
    """有界ストリーミング解析（starlette の MultiPartParser を使わない・一時
    ファイルなし）。項目名 PHOTO_PART_NAME のファイルパート本文だけを返す
    （filename 空・本文空=未選択の入力は読み飛ばし）。
    上限: パート数 PHOTO_MAX_PARTS・1 ファイル PHOTO_MAX_PART_BYTES・合計
    PHOTO_MAX_TOTAL_BYTES（Content-Length が偽でも実読込で検査）。超過は
    _MultipartLimit で即中断。形式不正・許可外の項目名は _MultipartBad。"""
    delim = b"--" + boundary
    buf = bytearray()
    body = bytearray()
    files: list[bytes] = []
    state = "preamble"
    part_count = 0
    total = 0
    cur_filename: str | None = None
    keep = len(delim) + 4                     # 区切り検出のために残す末尾長

    async for chunk in stream:
        total += len(chunk)
        if total > PHOTO_MAX_TOTAL_BYTES:
            raise _MultipartLimit()
        buf += chunk
        while True:
            if state == "preamble":
                i = buf.find(delim)
                if i < 0:
                    if len(buf) > keep:
                        del buf[:-keep]
                    break
                del buf[:i + len(delim)]
                state = "after_delim"
            if state == "after_delim":
                if len(buf) < 2:
                    break
                if buf[:2] == b"--":
                    state = "done"
                    break
                if buf[:2] != b"\r\n":
                    raise _MultipartBad()
                del buf[:2]
                state = "headers"
            if state == "headers":
                j = buf.find(b"\r\n\r\n")
                if j < 0:
                    if len(buf) > _PHOTO_HEADER_MAX_BYTES:
                        raise _MultipartBad()
                    break
                part_count += 1
                if part_count > PHOTO_MAX_PARTS:
                    raise _MultipartLimit()
                name, cur_filename = _parse_part_headers(bytes(buf[:j]))
                if name != PHOTO_PART_NAME:
                    raise _MultipartBad()      # 項目名以外の入力は受け取らない
                del buf[:j + 4]
                body = bytearray()
                state = "body"
            if state == "body":
                k = buf.find(b"\r\n" + delim)
                if k < 0:
                    if len(buf) > keep:
                        body += buf[:-keep]
                        del buf[:-keep]
                    if len(body) > PHOTO_MAX_PART_BYTES:
                        raise _MultipartLimit()
                    break
                body += buf[:k]
                if len(body) > PHOTO_MAX_PART_BYTES:
                    raise _MultipartLimit()
                del buf[:k + 2 + len(delim)]
                if cur_filename and len(body) > 0:
                    files.append(bytes(body))
                body = bytearray()
                state = "after_delim"
        if state == "done":
            break
    if state != "done":
        raise _MultipartBad()
    return files


def _photo_limit_page() -> HTMLResponse:
    return _fixed_page(
        "お写真をご確認ください",
        "お写真のサイズまたは枚数が上限（1枚10MB・5枚まで）を超えています。"
        "枚数やサイズを減らしてもう一度お試しいただくか、後からLINEでお送り"
        "ください。", 413)


def _photo_bad_page() -> HTMLResponse:
    return _fixed_page(
        "お写真をご確認ください",
        "お写真を受け付けられませんでした。JPEG・PNG・HEIC・PDF の形式で、"
        "もう一度お試しいただくか、後からLINEでお送りください。", 400)


async def shindan_photos_entry(request: Request, token: str = ""):
    """写真アップロードの単一入口（素の Request）。ゲート順（body 読取前）:
    ①env → ②メソッド（POST 以外 404）→ ③トークン有効性（無し/期限切れ/使用済み
    =404・存在しないフリ）→ ④Content-Type（multipart+boundary）/Content-Length
    （PHOTO_MAX_TOTAL_BYTES・欠落=404）→ ⑤レート（専用バケット）→ ⑥トークン消費
    （1 回限り）→ ここで初めて body をストリーミング解析。"""
    if _disabled():
        return _not_found()
    if request.method.upper() != "POST":
        return _not_found()
    now = time.time()
    if _upload_token_entry(token, now) is None:
        return _not_found()
    boundary = _multipart_boundary(request)
    if boundary is None or not _content_length_within(
            request, PHOTO_MAX_TOTAL_BYTES):
        logger.info("[SHINDAN_PHOTOS] body gate rejected (type/length)")
        return _not_found()
    if _photo_rate_exceeded(_rate_key(request), now):
        logger.info("[SHINDAN_PHOTOS] rate limited")
        return _fixed_page(
            "アクセスが集中しています",
            "アクセスが集中しています。しばらく時間をおいてから"
            "もう一度お試しください。", 429)
    # fix1（H3-01）: ⑥予約（claim・同期区間）。解析〜検査の失敗は finally で
    # release（未使用へ戻す=TTL 内なら同じ URL で再送可）。添付呼び出しに進む
    # 時点で consume に確定し、以後は添付結果に関わらず戻さない
    entry = claim_upload_token(token, now)
    if entry is None:
        return _not_found()
    record_id, number = entry
    consumed = False
    try:
        # ── ここで初めて body を読む（有界ストリーミング・一時ファイルなし） ──
        try:
            contents = await _parse_multipart_stream(request.stream(), boundary)
        except _MultipartLimit:
            logger.info("[SHINDAN_PHOTOS] limit exceeded (aborted)")
            return _photo_limit_page()
        except _MultipartBad:
            logger.info("[SHINDAN_PHOTOS] malformed multipart")
            return _photo_bad_page()
        if not contents:
            return _photo_bad_page()
        files: list[tuple[str, bytes, str]] = []
        for i, data in enumerate(contents, start=1):
            fmt = image_store.detect_format(data)
            if fmt is None:
                logger.info("[SHINDAN_PHOTOS] unknown format (no attach)")
                return _photo_bad_page()
            ext, mime = fmt
            files.append((f"form_{number}_{i}.{ext}", data, mime))
        consume_upload_token(token)
        consumed = True
    finally:
        if not consumed:
            release_upload_token(token)          # 成功確定前の離脱は必ず解除
    outcome = await image_store.attach_files(
        APP_JIKOU_CASE, record_id, files)
    if outcome != "attached":
        logger.error("[SHINDAN_PHOTOS] attach not confirmed record_id=%s",
                     emit(record_id, "record_id", "log", "operator"))
        await notify.notify_admin_line(
            "【時効診断フォーム・要確認】お写真の添付を確定できませんでした"
            f"（区分:{outcome} 受付番号:{number}）。上書きせず中止しています。"
            "kintone と Railway ログを確認してください。",
            throttle_key="shindan_photos",
        )
        return _fixed_page(
            "エラーが発生しました",
            "申し訳ありません。お写真を受け付けられませんでした。お手数ですが、"
            "後からLINEでお送りください。", 500)
    logger.info("[SHINDAN_PHOTOS] attached record_id=%s count=%s",
                emit(record_id, "record_id", "log", "operator"),
                emit(len(files), "count", "log", "operator"))
    return HTMLResponse(_photo_done_html(
        len(files), os.environ.get(_LINE_URL_ENV, "").strip()))


router.add_api_route("/shindan", shindan_entry, methods=_ENTRY_METHODS,
                     include_in_schema=False)
# FORM-3: 写真ルートは別名 catch-all より先に登録（トークン無しの
# /shindan/photos や配下パスは従来どおり catch-all の 404）
router.add_api_route(PHOTO_ROUTE + "/{token}", shindan_photos_entry,
                     methods=_ENTRY_METHODS, include_in_schema=False)
router.add_api_route("/shindan/{_rest:path}", shindan_alias_not_found,
                     methods=_ENTRY_METHODS, include_in_schema=False)


async def _handle_submit(form: dict[str, str]):
    creditor = str(form.get("creditor", "") or "").strip()
    borrow = form.get("borrow", "")
    last_pay = form.get("last_pay", "")
    court_doc = form.get("court_doc", "")
    website = form.get("website", "")

    # honeypot: 値があれば無言破棄（保存も通知もしない・番号も発行しない。
    # bot に検知させないため固定の受付風ページを 200 で返す）
    if website.strip():
        logger.info("[SHINDAN] honeypot discard (no side effects)")
        return _fixed_page(
            "送信を受け付けました",
            "送信を受け付けました。LINEの無料相談へお進みください。", 200)

    # サーバ側検証（閉集合・上限。失敗は固定文言のみ=非反射）
    if (len(creditor) > CREDITOR_MAX_CHARS
            or borrow not in CHOICES_BORROW
            or last_pay not in CHOICES_LAST_PAY
            or court_doc not in CHOICES_COURT_DOC):
        return _fixed_page(
            "入力内容をご確認ください",
            "入力内容を確認して、もう一度お試しください。", 400)

    pattern = judge(last_pay, court_doc)

    # App 21 保存（plain 値契約）
    fields_base = {
        "受付チャネル": "フォーム",
        "診断パターン": pattern,
        "status": "問い合わせ",
        "LINEユーザーID": "",                       # 紐付けは FORM-2
        "問い合わせ業者名": creditor,
        "借入時期_テキスト": borrow,
        "最終返済日_テキスト": last_pay,
        "裁判所書類": court_doc,
        # 必須 RADIO 4 種（既定値「あり」任せにしない・明示指定）
        "ラジオボタン": _COURT_DOC_TO_SOSHO[court_doc],   # １０年以内の訴訟の有無
        "ラジオボタン_2": "不明",                          # 住民票と居住地の相違
        "ラジオボタン_3": "不明",                          # 業者への電話有無
        "ラジオボタン_4": "不明",                          # アンケート・書面送付有無
    }
    number = ""
    record_id = ""
    outcome = "duplicate"
    for _attempt in range(_NUMBER_ATTEMPTS):
        number = _draw_number()
        outcome, record_id = await _persist_with_number(fields_base, number)
        if outcome != "duplicate":
            break                      # created / failed / unknown は再採番しない
    if outcome != "created":
        # 保存できていない（failed）／有無不明（unknown）／再採番上限（duplicate）
        # → 固定 500+要確認通知（受付番号を含め弁護士が App 21 と突合できる）
        logger.error("[SHINDAN] record not confirmed outcome=%s",
                     emit(outcome, "freetext", "log", "operator"))
        await notify.notify_admin_line(
            "【時効診断フォーム・要確認】レコード作成を確定できませんでした"
            f"（区分:{outcome} 受付番号:{number}）。"
            + ALERT_OTHER_APPLICANT_NOTE
            + "kintone と Railway ログを確認してください。",
            throttle_key="shindan_numbering",
        )
        return _error_500()

    logger.info("[SHINDAN] record created record_id=%s pattern=%s",
                emit(record_id, "record_id", "log", "operator"),
                emit(pattern, "freetext", "log", "operator"))

    # 弁護士通知（best-effort・固定文言+受付番号+パターンのみ=PII 非搭載）
    attorney_id = os.environ.get("ATTORNEY_LINE_USER_ID", "")
    if attorney_id:
        sent = await notify.notify_business(
            attorney_id,
            f"【時効診断フォーム受付】受付番号:{number} 診断パターン:{pattern}")
        if not sent:
            logger.warning("[SHINDAN] attorney notify failed (record saved)")
    else:
        logger.info("[SHINDAN] ATTORNEY_LINE_USER_ID not set, notify skipped")

    # FORM-3 Part B: 結果画面に写真送信（任意・第 2 段）の使い捨てトークンを
    # 埋め込む（受付番号のレコードに紐付け・TTL 15 分・1 回限り）
    upload_token = issue_upload_token(record_id, number, time.time())
    return HTMLResponse(_result_html(
        pattern, number, os.environ.get(_LINE_URL_ENV, "").strip(),
        upload_token))
