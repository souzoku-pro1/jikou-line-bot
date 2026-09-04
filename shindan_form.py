"""時効診断フォーム（公開ページ）— JIKOU-FORM-1

HP 設置の簡易診断: 4 問（①債権者名・②借入時期・③最終返済・④裁判所書類）
→ サーバ側ルール判定（A〜D）→ 弁護士凍結の結果文言+受付番号を表示し、
LINE 友だち追加へ誘導する。回答は App 21 へ保存（LINE 紐付けは FORM-2・
写真アップロードは FORM-3 の別票）。

設計（票の逐語+SURVEY 実測）:
- prefix /shindan（/app 配下に置かない=test_p4_001 の PUBLIC_ROUTES pin 維持。
  route 内 gate 方式のため encoded alias も starlette の decode 後に本 route へ
  落ちて gate を通る=service_auth normalize_path と同じ入口遮断の思想）
- 公開条件 fail-closed: env JIKOU_LINE_ADD_URL（時効 LINE 友だち追加 URL）が
  未設定/空白なら GET/POST とも 404（存在しないフリ・shinjutsu_webhook 同型）
- 判定はサーバ側のみ（優先順 C→B→D→A・②は判定に使わず保存のみ・
  「その他の督促通知」は裁判所手続でないため C 非該当=司令塔裁定）
- 凍結文言（FROZEN_RESULTS/FROZEN_NOTE）は test_jikou_form1 が sha256 pin。
  {受付番号} プレースホルダのみ実値置換（改変禁止）
- スパム対策: honeypot（非表示 website 欄・値あり=無言破棄=保存も通知もしない）
  + X-Forwarded-For 最終要素の SHA-256 キー（webapp_auth._rate_key 流儀・
  生 IP を保持しない）による固定窓レート制限（RATE_LIMIT 回/RATE_WINDOW 秒）
- 非反射: 入力値をエラー応答・結果画面のどこにも出さない（固定文言のみ）。
  結果画面に出る可変値は**サーバ生成の受付番号のみ**
- App 21 保存は hub.kintone（plain 値契約・HOUKI-STORE-FIX1 の教訓）。
  受付番号=secrets 乱数 6 桁ゼロ埋め・「値の重複を禁止する」（CU 済み実測）
  による create 失敗→再採番リトライ（上限 _NUMBER_ATTEMPTS 回・全失敗=
  固定文言 500+要確認通知）。レコード番号の流用は禁止（連番=推測可能で
  FORM-2 の紐付け乗っ取りリスク）
- 必須 RADIO 4 種（既定値「あり」の誤登録防止・form fields API 実測）:
  １０年以内の訴訟の有無←④写像／他 3 種（住民票相違・業者電話・アンケート
  送付）は④から写像できないため「不明」を明示指定（既定値任せにしない）
- 弁護士通知は notify_business 流儀・固定文言+受付番号+診断パターンのみ
  （債権者名・回答本文は載せない=PII 規律）。通知は best-effort
  （App 21 レコードが正本・失敗しても受付は成立）
"""

import hashlib
import html
import logging
import os
import secrets
import time

from fastapi import APIRouter, Form, Request
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

# ── 受付番号（6 桁ゼロ埋め乱数・重複は再採番） ──────────────────────────────────
_NUMBER_ATTEMPTS = 5


def _draw_number() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


# ── レート制限（webapp_auth._rate_key 流儀の固定窓・単一 worker 前提） ────────────
RATE_WINDOW_SECONDS = 600
RATE_LIMIT = 10
MAX_BUCKETS = 5000
_attempts: dict[str, tuple[float, int]] = {}


def _rate_key(request: Request) -> str:
    """X-Forwarded-For の最終要素（単一信頼 proxy=Railway が付加した実クライアント）
    を SHA-256 のみで保持する（生 IP/生 XFF を状態・ログに残さない）。
    XFF 不在時（直接接続・テスト）は client.host に fallback。"""
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        ip = xff.split(",")[-1].strip()
    else:
        ip = request.client.host if request.client else ""
    return hashlib.sha256(ip.encode("utf-8")).hexdigest()


def _rate_exceeded(key: str, now: float) -> bool:
    """固定窓カウンタを 1 進め、上限超過なら True。窓満了で自然解除。
    バケット暴走時は期限切れのみ掃除（ロック中は保全）。"""
    if len(_attempts) > MAX_BUCKETS:
        for k in [k for k, (start, _c) in _attempts.items()
                  if now - start >= RATE_WINDOW_SECONDS]:
            del _attempts[k]
    start, count = _attempts.get(key, (now, 0))
    if now - start >= RATE_WINDOW_SECONDS:
        start, count = now, 0
    _attempts[key] = (start, count + 1)
    return count + 1 > RATE_LIMIT


def _disabled() -> bool:
    """公開条件 fail-closed: LINE 友だち追加 URL 未設定なら受け口ごと無効。"""
    return not os.environ.get(_LINE_URL_ENV, "").strip()


def _not_found() -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "not found"})


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


@router.get("/shindan")
async def shindan_form_page():
    if _disabled():
        return _not_found()
    return HTMLResponse(_form_html())


@router.post("/shindan")
async def shindan_submit(request: Request,
                         creditor: str = Form(default=""),
                         borrow: str = Form(default=""),
                         last_pay: str = Form(default=""),
                         court_doc: str = Form(default=""),
                         website: str = Form(default="")):
    if _disabled():
        return _not_found()

    # honeypot: 値があれば無言破棄（保存も通知もしない・番号も発行しない。
    # bot に検知させないため固定の受付風ページを 200 で返す）
    if website.strip():
        logger.info("[SHINDAN] honeypot discard (no side effects)")
        return _fixed_page(
            "送信を受け付けました",
            "送信を受け付けました。LINEの無料相談へお進みください。", 200)

    # レート制限（固定窓・キーは SHA-256 のみ保持・超過は固定応答）
    if _rate_exceeded(_rate_key(request), time.time()):
        logger.info("[SHINDAN] rate limited")
        return _fixed_page(
            "アクセスが集中しています",
            "アクセスが集中しています。しばらく時間をおいてから"
            "もう一度お試しください。", 429)

    # サーバ側検証（閉集合・上限。失敗は固定文言のみ=非反射）
    creditor = str(creditor or "").strip()
    if (len(creditor) > CREDITOR_MAX_CHARS
            or borrow not in CHOICES_BORROW
            or last_pay not in CHOICES_LAST_PAY
            or court_doc not in CHOICES_COURT_DOC):
        return _fixed_page(
            "入力内容をご確認ください",
            "入力内容を確認して、もう一度お試しください。", 400)

    pattern = judge(last_pay, court_doc)

    # App 21 保存（plain 値契約）。受付番号の一意制約 create 失敗は再採番
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
    for _attempt in range(_NUMBER_ATTEMPTS):
        number = _draw_number()
        try:
            record_id = await hub_kintone.create_record(
                APP_JIKOU_CASE, {**fields_base, "受付番号": number})
            break
        except hub_kintone.KintoneError as e:
            # 一意制約（重複）想定の再採番。他要因でも上限までで打ち切り=固定 500
            logger.warning("[SHINDAN] create failed (redraw) code=%s",
                           emit(e.code, "vendor_raw", "log", "operator"))
    else:
        logger.error("[SHINDAN] numbering exhausted (no record)")
        await notify.notify_admin_line(
            "【時効診断フォーム・要確認】受付番号の採番・レコード作成に"
            "失敗しました（保存できていません）。kintone と Railway ログを"
            "確認してください。",
            throttle_key="shindan_numbering",
        )
        return _fixed_page(
            "エラーが発生しました",
            "申し訳ありません。システムエラーが発生しました。お手数ですが、"
            "時間をおいてもう一度お試しください。", 500)

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
