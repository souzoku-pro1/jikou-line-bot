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
import secrets
import time
from collections import OrderedDict
from urllib.parse import parse_qsl

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

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


def _rate_exceeded(key: str, now: float) -> bool:
    """固定窓カウンタを 1 進め、上限超過なら True。窓満了で自然解除。
    - 期限切れ掃除: 先頭（最終 touch が最も古い）から _PRUNE_STEPS 件だけ見て
      期限切れなら捨てる（償却 O(1)・全件走査なし）
    - 上限: 新規キーで len が MAX_BUCKETS に達していれば最古を 1 件退避してから
      挿入する（辞書サイズは MAX_BUCKETS を超えない）。退避=429 ではなく受入
      （新規利用者の可用性優先。退避で制限を外すには MAX_BUCKETS 個の別 IP から
      の送信が要り、それ自体が上限回数×MAX_BUCKETS を超えるコストになる）"""
    for _ in range(_PRUNE_STEPS):
        if not _attempts:
            break
        oldest_key = next(iter(_attempts))
        if now - _attempts[oldest_key][0] >= RATE_WINDOW_SECONDS:
            del _attempts[oldest_key]
        else:
            break
    if key in _attempts:
        start, count = _attempts[key]
        if now - start >= RATE_WINDOW_SECONDS:
            start, count = now, 0
        _attempts.move_to_end(key)
    else:
        start, count = now, 0
        while len(_attempts) >= MAX_BUCKETS:
            _attempts.popitem(last=False)        # 最古（LRU）を退避
    _attempts[key] = (start, count + 1)
    return count + 1 > RATE_LIMIT


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


def _result_html(pattern: str, number: str, line_url: str) -> str:
    text_html = html.escape(result_text(pattern, number)).replace("\n", "<br>")
    body = (
        "<h1>診断結果</h1>"
        f"<p class=\"num\">{text_html}</p>"
        f"<a class=\"btn\" href=\"{html.escape(line_url, quote=True)}\">"
        "LINEで無料相談する（友だち追加）</a>"
        "<p>LINEで上記の受付番号をお送りいただくと、ご回答内容を引き継いで"
        "スムーズにご案内できます。</p>"
        f"<p class=\"note\">{FROZEN_NOTE}</p>"
    )
    return _page("診断結果", body)


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


router.add_api_route("/shindan", shindan_entry, methods=_ENTRY_METHODS,
                     include_in_schema=False)
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

    return HTMLResponse(_result_html(
        pattern, number, os.environ.get(_LINE_URL_ENV, "").strip()))
