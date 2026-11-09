"""k_draft — K 後追い下書き（K-1: 下書き専用モデルの経路・規則 v1.5・記録表 reply_k_draft）

正本: docs/k_design.md §1〜§2・§5・§6 K-1。裁定（2026-11-04・§7）反映済み。

何をするか（§1-2）
- App 29（承認キュー）への起票が成功した直後（chat_responder.save_to_approval_queue が No. を返した 1 か所）に **後追い** で
  1 回だけ起動し、同じ案件情報・履歴・相談者の今回の文から **大野向けの下書き** を下書き専用モデルで作り直して
  DB の新表 reply_k_draft に 1 行置く。返信画面（/app/reply/{No.}）はこの本文を返信欄の初期値にし、統合一覧は札を付ける。
- 相談者へ自動で届く文は 1 文字も変えない。本モジュールは LINE へ送る関数・kintone へ書く関数を **持たない**（§5・AST 検査）。
  K 下書きは返信画面の [送信]（大野の承認）を通らない限りどこにも出ない。

env ``JIKOU_DRAFT_MODEL``（§1-3・閉集合）
- 未設定／空／``off`` … K 無効（既定。配備しただけでは挙動が 1 つも変わらない＝設計 §1-3・受入 1）。
- ``claude-fable-5-1``（有効化時の既定・裁定 4）／``claude-opus-5-5``（予備）／``claude-sonnet-5-5``（安価側）。
- それ以外 … 無効値＝fail-closed（§1-4）。K 下書きは作らず reply_k_draft に state=skipped（reason model:invalid）を 1 行、
  起動ログに WARNING 1 行（値は出さない）、管理者 LINE へ 1 日 1 回（承認済みテキスト）。既定へ黙って倒さない。
- FALLBACK なし・再試行なし・thinking 無効・timeout 60 秒・1 件 1 回（§1-4）。

費用（§1-6）: 1 通ごとに入出力トークン数と概算費用（円）を表に記録する。月の概算が上限（``MONTHLY_COST_CAP_YEN``・§1-6 の
「Fable で月 1,600 円以内」）に達したら off と同じ挙動（state=skipped・reason cost:cap）・業務 LINE へ 1 日 1 回。

規則 v1.5（§2）: v1.4（自動送信の規則・凍結）は触らない。v1.5 は **K 下書きの prompt にだけ** 実行時付加する層
（``RULES_V15_BLOCK``・sha pin）。組み立て順は固定: 基底 prompt → 収集済み項目 → human 注記 → v1.5 ブロック → 参考文例ブロック。
手本（参考文例）の付け先は裁定 3: K が ON のときは K 下書きにだけ付け、一次生成には付けない（reply_exemplar.prompt_note が本
モジュールの enabled() を見る）。K-1 では手本は空集合（投入は K-2）。

RV-08: 行は削除しない（state の追加だけ）。RV-10: 本文・氏名・LINE userId・例外本文をログ・通知・URL に出さない。
"""
from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import logging
import os
import re
import time

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as _pg
from sqlalchemy.dialects import sqlite as _sqlite

from hub import approved_texts, db, pii_scan
from hub.db import DatabaseNotConfigured, session_scope
from hub.redact import emit

logger = logging.getLogger("hub.k_draft")

BUSINESS = "jikou"
_JST = datetime.timezone(datetime.timedelta(hours=9))

# ── env JIKOU_DRAFT_MODEL（§1-3・閉集合） ─────────────────────────────────────────
ENV_MODEL = "JIKOU_DRAFT_MODEL"
MODEL_OFF = "off"
MODEL_FABLE = "claude-fable-5-1"
MODEL_OPUS = "claude-opus-5-5"
MODEL_SONNET = "claude-sonnet-5-5"
ALLOWED_MODELS = (MODEL_OFF, MODEL_FABLE, MODEL_OPUS, MODEL_SONNET)      # 4 値（裁定 4）。追加・変更は票で
DEFAULT_WHEN_UNSET = MODEL_OFF        # 設計 §1-3「未設定／空／off＝K 無効（既定）」。有効化時の既定モデルは MODEL_FABLE（大野が env に書く）
MODEL_INVALID = "invalid"             # 閉集合外（fail-closed・内部印・env の値そのものは保持しない）
TIMEOUT_SECONDS = 60
MAX_TOKENS = 1024
THINKING_PARAM = {"thinking": {"type": "disabled"}}        # FALLBACK_EXTRA_PARAMS と同じ（§1-3）


def configured_model() -> str:
    """env を閉集合に写す。未設定／空 → DEFAULT_WHEN_UNSET、許可値はそのまま、それ以外 → MODEL_INVALID。"""
    raw = str(os.environ.get(ENV_MODEL) or "").strip()
    if raw == "":
        return DEFAULT_WHEN_UNSET
    if raw in ALLOWED_MODELS:
        return raw
    return MODEL_INVALID


def enabled() -> bool:
    """K 後追い下書きを起動するか（許可値のモデル名のときだけ True。off・無効値は False）。"""
    return configured_model() not in (MODEL_OFF, MODEL_INVALID)


_config_logged = {"done": False}


def log_config_once() -> str:
    """起動時に 1 回: 無効値なら WARNING 1 行（値は出さない）。戻り値は閉集合の状態。"""
    m = configured_model()
    if not _config_logged["done"]:
        _config_logged["done"] = True
        if m == MODEL_INVALID:
            logger.warning("[K_DRAFT] JIKOU_DRAFT_MODEL is not in the allowed set; K drafts are disabled (fail-closed)")
    return m


# ── 費用（§1-6・公式料金ページ 2026-11-04 確認・$/MTok） ───────────────────────────
PRICE_USD_PER_MTOK = {MODEL_FABLE: (10.0, 50.0), MODEL_OPUS: (4.0, 20.0), MODEL_SONNET: (2.0, 10.0)}
YEN_PER_USD = 150.0
MONTHLY_COST_CAP_YEN = 1600.0         # §1-6「月 45〜90 通の幅で見て Fable で月 1,600 円以内」を上限とする（超えたら off と同じ挙動）


def estimate_cost_yen(model: str, input_tokens: int, output_tokens: int) -> float:
    pi, po = PRICE_USD_PER_MTOK.get(model, (0.0, 0.0))
    usd = (max(0, int(input_tokens or 0)) * pi + max(0, int(output_tokens or 0)) * po) / 1_000_000.0
    return round(usd * YEN_PER_USD, 2)


# ── 規則 v1.5（§2-1・8 項・K 下書きだけ・sha pin） ────────────────────────────────
RULES_VERSION = "v1.5"
RULES_V15_TEXT = "\n".join([
    "1. 逃げない: 「確認のうえ改めてご案内します」「担当者に申し伝えます」で終わらない。答えられる範囲を今答え、確認が要る点は「何を・なぜ確認するか」を 1 文で示す。",
    "2. 言い切る（事実と一般論）: 年数・条文・手続名を具体に書く。条文番号は書く（例: 貸金業者の債権の消滅時効は、最後の返済から 5 年です（民法 166 条 1 項 1 号）／判決が確定している場合は、確定から 10 年です（民法 169 条 1 項）／一部でも支払うと債務の承認（民法 152 条）に当たり、時効が更新されます／裁判所からの書類（訴状・支払督促）が届いた場合は、期限内の対応で時効を主張できます／手続は、当事務所から業者へ内容証明で時効援用通知を送り、その後の業者への確認で確定します）。用語は「時効の更新」（「延長」は使わない）。",
    "3. 次に聞くべきことを 1 つ添える: 返信の最後に、見立てや手続を進めるために次に要る事実を 1 問だけ（順: 最後の返済年月 → 業者からの書面・SMS → 裁判所からの書類 → 答弁書・支払督促の心当たり）。既知項目（収集済み一覧）にあるものは聞かない。",
    "4. 断定してはいけない場面の書き方: 受任の可否・費用の交渉・時効成否の最終判断は、言い切らず定型の留保で結ぶ（下記の言い回し表）。",
    "5. 長さ: 下書きは 600 字以内・3 段落まで（結論 → 根拠 → 次の 1 問）。LINE で崩れる記号（表・見出し記号・絵文字）は使わない。箇条書きは「・」まで。",
    "6. 敬体の統一: です・ます調。一人称は「当事務所」または「弁護士の大野」。相談者は登録名＋「様」（本人への返信のみ・未登録なら呼びかけなし）。「ございます」は定型の引用に限り、地の文は「です・ます」で揃える。「〜させていただく」は 1 通に 1 回まで。",
    "7. 参考文例の使い方: 文体と構成だけを参照し、個別の事情・数値・固有名は写さない。",
    "8. 個別に答える: 相談者の文に債権者名・金額・時期が書かれていれば、下書きはそれを引用して個別に答える（一般論で終わらない）。例: 「〇〇（業者名）からの〇年〇月付の請求について、最後のお支払いが〇年〇月であれば…」。引用してよいのは相談者の文と収集済み項目にある事実だけで、記録にない事実は書かない。",
])
# §2-2 断定してはいけない場面の言い回し（閉集合・pin）: (場面, 書いてよい形, 書いてはいけない形)
RESERVED_PHRASES = (
    ("受任の可否", "ご依頼いただける場合は、委任契約書を電子でお送りして手続に入ります", "「お受けできます」「受任いたします」（可否の確定）"),
    ("費用の交渉・分割", "費用は 44,000 円（税込）で一律です。お支払い方法のご相談は弁護士が直接お伺いします", "値引き・分割回数・支払期日の提示"),
    ("時効成否の最終判断", "お伺いした内容が正確であれば、時効援用できる可能性が高いです。最終的に時効が成立しているかは、当事務所から業者へ時効援用通知を送り、その後の業者への確認をもって確定します", "「時効です」「成立しています」「大丈夫です」"),
    ("不成立方向の見立て", "時効が更新されている可能性があります。〇〇を確認させてください（＋次の 1 問）", "「時効は使えません」「諦めてください」"),
    ("推奨時期", "債権者別ルール表の確定行がある会社だけ既存の文。無ければ「会社名と最後の返済年月を伺ってから、時期をご案内します」", "表に無い会社の年月の提示"),
)
RULES_V15_HEADER = "\n\n【返信規則 v1.5（この下書きは弁護士の大野が手直しして送ります。自動送信ではありません）】"
RULES_V15_TABLE_HEADER = "■ 断定してはいけない場面の言い回し（場面／書いてよい形／書いてはいけない形）"
RULES_V15_BLOCK = "\n".join(
    [RULES_V15_HEADER, RULES_V15_TEXT, RULES_V15_TABLE_HEADER]
    + [f"・{scene}／{ok}／{ng}" for scene, ok, ng in RESERVED_PHRASES])


def rules_sha256() -> str:
    return hashlib.sha256(RULES_V15_BLOCK.encode("utf-8")).hexdigest()


def build_k_prompt(base_prompt: str, exemplar_note: str = "") -> str:
    """K 下書きの system prompt＝基底 prompt（収集済み項目・human 注記込み・byte 同一）＋ v1.5 ブロック ＋ 参考文例ブロック（末尾付加のみ）。"""
    return str(base_prompt) + RULES_V15_BLOCK + str(exemplar_note or "")


# ── 保存前の検査（§1-5・すべて機械・落ちたら rejected で保存しない） ─────────────────
MAX_CHARS = 600
EVASIVE_PHRASES = ("改めてご案内", "確認のうえご連絡", "申し伝えます", "担当者よりご連絡")   # 閉集合・pin
REASON_GUARD_PLACEHOLDER = "guard:placeholder"
REASON_GUARD_EMPTY = "guard:empty"
REASON_GUARD_LENGTH = "guard:length"
REASON_GUARD_EVASIVE = "guard:evasive"
REASON_GUARD_RESIDUE = "guard:residue"
FLAG_ASSERTIVE = "要確認: 断定表現"           # 落とさず札だけ（§1-5 の 6）
_NAME_PLACEHOLDERS = ("", "（未登録）", "（ヒアリング中）")
RESIDUE_SKIP_LABEL = "name_candidate"      # 発見的な氏名候補だけ外す（context_name・phone・address 等は見る）


def inspect_text(text: str, *, customer_name: str = "", allowed_emoji=None) -> tuple[str, str | None, list[str]]:
    """(検査後の本文, 落とした理由 or None, 札)。理由があれば保存しない。"""
    import chat_responder as cr      # 遅延 import（chat_responder が本モジュールを末尾で import するため）
    from hub import empty_reply_guard
    kwargs = {"allowed_emoji": allowed_emoji} if allowed_emoji is not None else {}
    out, _issues, fatal = cr.sanitize_reply_without_human_marker(str(text or ""), **kwargs)
    if fatal:
        return out, REASON_GUARD_PLACEHOLDER, []
    if any(p in out for p in EVASIVE_PHRASES):                 # K 固有の理由を先に（空返事検査と重なる語彙がある）
        return out, REASON_GUARD_EVASIVE, []
    if not out.strip() or empty_reply_guard.empty_reply_labels(out):
        return out, REASON_GUARD_EMPTY, []
    if len(out) > MAX_CHARS:
        return out, REASON_GUARD_LENGTH, []
    probe = out
    name = str(customer_name or "").strip()
    if name not in _NAME_PLACEHOLDERS:
        probe = probe.replace(name, "")                      # 相談者本人の登録名は可（R-J7）
    # 残留検査: 確定パターン（メール・電話・郵便番号・LINE userId・数字列・住所・続柄+氏名）＋ BQ-03 の文脈つき氏名（context_name）。
    # 発見的な氏名候補（name_candidate）は匿名化後の自由文向けで、通常の返信文（業者名・手続名）に誤反応するため K の検査では使わない。
    if [h for h in pii_scan.residue_hits(probe) if h != RESIDUE_SKIP_LABEL]:
        return out, REASON_GUARD_RESIDUE, []
    flags = [FLAG_ASSERTIVE] if any(rx.search(out) for _label, rx in cr._FORBIDDEN_PATTERNS) else []
    return out, None, flags


# ── 表 reply_k_draft（新表のみ・既存表の列変更なし） ────────────────────────────────
metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
TABLE_NAME = "reply_k_draft"
STATE_STARTED = "started"       # 1 件 1 回の確定（起動直後）
STATE_GENERATED = "generated"   # 本文あり（返信画面の初期値になる）
STATE_SKIPPED = "skipped"       # 設定不正・費用上限（本文なし）
STATE_FAILED = "failed"         # モデル障害・入力取得不能（本文なし）
STATE_REJECTED = "rejected"     # 検査落ち（本文なし）
STATES = (STATE_STARTED, STATE_GENERATED, STATE_SKIPPED, STATE_FAILED, STATE_REJECTED)
REASON_MODEL_INVALID = "model:invalid"
REASON_MODEL_UNAVAILABLE = "model:unavailable"
REASON_MODEL_EMPTY = "model:empty"
REASON_COST_CAP = "cost:cap"
REASON_INPUT_UNAVAILABLE = "input:unavailable"

reply_k_draft = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("id", _BIG, primary_key=True, autoincrement=True),              # 下書き ID
    sa.Column("record_id", sa.Text, nullable=False),                            # 元の App 29 No.（1 件 1 行）
    sa.Column("pending_id", _BIG, nullable=True),                               # 元の対応待ち pending_id（結線時に分かれば）
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("category", sa.Text, nullable=True),                              # 一次判定のカテゴリ
    sa.Column("model", sa.Text, nullable=False),                                # 閉集合のモデル名（無効値は 'invalid'）
    sa.Column("rules_version", sa.Text, nullable=False),                        # 'v1.5'
    sa.Column("rules_sha", sa.Text, nullable=False),                            # RULES_V15_BLOCK の sha256
    sa.Column("state", sa.Text, nullable=False),
    sa.Column("reason", sa.Text, nullable=True),                                # 固定語彙のみ
    sa.Column("text", sa.Text, nullable=True),                                  # generated のときだけ本文
    sa.Column("text_sha", sa.Text, nullable=True),
    sa.Column("flags", sa.Text, nullable=True),                                 # 札（固定語彙・JSON 配列）
    sa.Column("exemplar_ids", sa.Text, nullable=True),                          # 手本参照の ID（JSON 配列・K-2 まで空）
    sa.Column("input_tokens", sa.Integer, nullable=True),
    sa.Column("output_tokens", sa.Integer, nullable=True),
    sa.Column("cost_yen", sa.Float, nullable=True),                             # 概算費用（円）
    sa.Column("elapsed_ms", sa.Integer, nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint("record_id", name="uq_reply_k_draft_record"),
    sa.Index("ix_reply_k_draft_state", "business", "state"),
    sa.Index("ix_reply_k_draft_created", "business", "created_at"),
)


def _db_configured() -> bool:
    try:
        db.database_url()
    except DatabaseNotConfigured:
        return False
    return True


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _sha(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


async def _claim(record_id: str, *, category: str | None, model: str, state: str, reason: str | None = None) -> bool:
    """record_id の行を INSERT … ON CONFLICT DO NOTHING … RETURNING で 1 行だけ確定（再配送で 2 行にならない・psycopg は RETURNING で判定）。"""
    now = _now()
    values = dict(record_id=str(record_id), business=BUSINESS, category=category, model=model,
                  rules_version=RULES_VERSION, rules_sha=rules_sha256(), state=state, reason=reason,
                  created_at=now, updated_at=now)
    async with session_scope() as s:
        dialect = (await s.connection()).dialect.name
        if dialect == "postgresql":
            stmt = _pg.insert(reply_k_draft).values(**values).on_conflict_do_nothing(index_elements=["record_id"])
        elif dialect == "sqlite":
            stmt = _sqlite.insert(reply_k_draft).values(**values).on_conflict_do_nothing(index_elements=["record_id"])
        else:
            cur = (await s.execute(sa.select(reply_k_draft.c.id).where(reply_k_draft.c.record_id == str(record_id)))).first()
            if cur is not None:
                return False
            stmt = sa.insert(reply_k_draft).values(**values)
        res = await s.execute(stmt.returning(reply_k_draft.c.id))
        return res.first() is not None


async def _finish(record_id: str, **fields) -> None:
    fields["updated_at"] = _now()
    async with session_scope() as s:
        await s.execute(sa.update(reply_k_draft).where(reply_k_draft.c.record_id == str(record_id)).values(**fields))


async def month_cost_yen(now: datetime.datetime | None = None) -> float:
    """今月（JST）の概算費用の合計（円）。"""
    now = (now or _now()).astimezone(_JST)
    start = datetime.datetime(now.year, now.month, 1, tzinfo=_JST).astimezone(datetime.timezone.utc)
    async with session_scope() as s:
        v = (await s.execute(sa.select(sa.func.coalesce(sa.func.sum(reply_k_draft.c.cost_yen), 0.0)).where(
            reply_k_draft.c.business == BUSINESS, reply_k_draft.c.created_at >= start))).scalar()
    return float(v or 0.0)


# ── 通知（承認済みテキストのみ・1 日 1 回・本文なし） ───────────────────────────────
NOTICE_INVALID_KEY = "ops_k_draft_invalid_model_v1"
NOTICE_INVALID_TEXT = ("【K 下書き】下書き専用モデルの設定値（JIKOU_DRAFT_MODEL）が許可外のため、K 下書きを作っていません。"
                       "Railway の変数を確認してください（許可値: off / claude-fable-5-1 / claude-opus-5-5 / claude-sonnet-5-5）。"
                       "対応待ちと従来の下書きはそのままです。")
NOTICE_FAILED_KEY = "ops_k_draft_failed_v1"
NOTICE_FAILED_TEXT = ("【K 下書き】下書き専用モデルから応答が得られず、K 下書きなしの件があります。対応待ちと従来の下書きは"
                      "そのままです。続く場合は JIKOU_DRAFT_MODEL の見直しをご検討ください。")
NOTICE_COST_CAP_KEY = "ops_k_draft_cost_cap_v1"
NOTICE_COST_CAP_TEXT = ("【K 下書き】今月の K 下書きの概算費用が上限に達したため、月末まで K 下書きを止めています"
                        "（対応待ちと従来の下書きはそのままです）。")
NOTICE_TEXTS = {NOTICE_INVALID_KEY: NOTICE_INVALID_TEXT, NOTICE_FAILED_KEY: NOTICE_FAILED_TEXT,
                NOTICE_COST_CAP_KEY: NOTICE_COST_CAP_TEXT}


def notice_ledger_key(kind: str, now: datetime.datetime | None = None) -> str:
    d = (now or _now()).astimezone(_JST).strftime("%Y-%m-%d")
    return f"k_draft_{kind}:{d}"


async def notify_once(kind_key: str) -> str:
    """業務 LINE へ 1 日 1 回（台帳 key）。文言は承認台帳に載った定型だけ＝未承認なら送らず key を記録（1 日 1 回の未承認通知に乗る）。"""
    text = approved_texts.approved_text(kind_key, NOTICE_TEXTS[kind_key])
    if text is None:
        approved_texts.approved_or_safe(kind_key, NOTICE_TEXTS[kind_key])
        try:
            await approved_texts.notify_unapproved_once()
        except Exception:
            logger.error("[K_DRAFT] unapproved text notice failed (fixed reason)")
        return "needs_approval"
    from hub import send_policy
    try:
        return await send_policy.send_business_notice(notice_ledger_key(kind_key), text)
    except Exception:
        logger.error("[K_DRAFT] notice failed (fixed reason)")
        return "failed"


# ── モデル呼び出し（1 回・FALLBACK なし・thinking 無効・timeout 60 秒） ───────────────
async def _call_model(model: str, system_prompt: str, messages: list[dict], tool: dict) -> tuple[str, int, int]:
    """下書き専用モデルを 1 回だけ呼び、(本文, 入力トークン, 出力トークン) を返す。tool の category／auto_send は読まない。"""
    import anthropic
    import chat_responder as cr
    client = anthropic.AsyncAnthropic(api_key=cr._ANTHROPIC_KEY, timeout=float(TIMEOUT_SECONDS), max_retries=0)
    resp = await client.messages.create(
        model=model, max_tokens=MAX_TOKENS,
        system=[{"type": "text", "text": system_prompt}],                 # prompt cache は付けない（§1-6・K は 1 日 1〜3 件）
        tools=[tool], tool_choice={"type": "tool", "name": "compose_reply"},
        messages=messages, **THINKING_PARAM)
    block = next((b for b in resp.content if getattr(b, "type", "") == "tool_use"), None)
    text = str(((getattr(block, "input", None) or {}).get("reply") or "")) if block is not None else ""
    usage = getattr(resp, "usage", None)
    return text, int(getattr(usage, "input_tokens", 0) or 0), int(getattr(usage, "output_tokens", 0) or 0)


async def _inputs(user_id: str, customer_name: str, customer_message: str) -> tuple[str, list[dict], str, dict]:
    """既存の取得関数で App 21 の案件情報と直近履歴を取り、基底 prompt（v1.5 なし）と messages を組む。"""
    import chat_responder as cr
    from hub import reply_exemplar
    p = cr.JIKOU_PROFILE
    try:
        rec = await cr.get_app21_record(user_id)
    except Exception:
        rec = None
    rec = rec or {}

    def _field(code: str) -> str:
        return rec.get(code, {}).get("value", "") or "（未登録）"

    status = rec.get("status", {}).get("value", "") or ""
    name = rec.get("顧客名", {}).get("value", "") or (customer_name if customer_name not in _NAME_PLACEHOLDERS else "（未登録）")
    business_name = (rec.get("ルックアップ_0", {}).get("value", "") or rec.get("問い合わせ業者名", {}).get("value", "") or "（未登録）")
    history = await cr.get_recent_chat_history(user_id)
    messages = list(history) + [{"role": "user", "content": str(customer_message or "")}]
    known_items = cr.build_known_items(rec or None, history)
    base = cr.build_system_prompt(status=status, customer_name=name, business_name=business_name,
                                  borrow_period=_field("借入時期_テキスト"), last_payment=_field("最終返済日_テキスト"),
                                  court_docs=_field("裁判所書類"), credit_check=_field("信用情報確認"),
                                  known_items=known_items, profile=p, human_note=cr.human_history_note(history), exemplar_note="")
    note = await reply_exemplar.k_prompt_note(p.name, status)           # 手本の受け口（K-1 は空集合でも動く・裁定 3）
    return base, messages, name, {"note": note, "tool": p.compose_tool, "allowed_emoji": p.allowed_emoji}


def _flatten_ids(note) -> list[int]:
    out: list[int] = []
    for ids in (getattr(note, "ids_by_category", None) or {}).values():
        out.extend(int(x) for x in ids)
    return out


async def run_after_queue(record_id: str, *, user_id: str, customer_name: str, customer_message: str,
                          category: str | None) -> str:
    """後追い本体。戻り値は最終 state（テスト用）。例外は外に出さない。"""
    rid = str(record_id)
    model = configured_model()
    cat = category if category else None
    try:
        if model == MODEL_INVALID:
            if await _claim(rid, category=cat, model=MODEL_INVALID, state=STATE_SKIPPED, reason=REASON_MODEL_INVALID):
                logger.warning("[K_DRAFT] skipped (invalid model)")
                await notify_once(NOTICE_INVALID_KEY)
            return STATE_SKIPPED
        if not await _claim(rid, category=cat, model=model, state=STATE_STARTED):
            return "duplicate"
    except Exception:
        logger.warning("[K_DRAFT] store failed (fixed reason)")
        return "store_failed"
    try:
        if await month_cost_yen() >= MONTHLY_COST_CAP_YEN:
            await _finish(rid, state=STATE_SKIPPED, reason=REASON_COST_CAP)
            logger.warning("[K_DRAFT] skipped (monthly cost cap)")
            await notify_once(NOTICE_COST_CAP_KEY)
            return STATE_SKIPPED
    except Exception:
        logger.warning("[K_DRAFT] store failed (fixed reason)")
        return "store_failed"
    t0 = time.monotonic()
    try:
        base, messages, name, extra = await _inputs(user_id, customer_name, customer_message)
    except Exception:
        logger.error("[K_DRAFT] failed (fixed reason)")
        await _safe_finish(rid, state=STATE_FAILED, reason=REASON_INPUT_UNAVAILABLE)
        return STATE_FAILED
    prompt = build_k_prompt(base, extra["note"])
    try:
        text, in_t, out_t = await asyncio.wait_for(_call_model(model, prompt, messages, extra["tool"]), TIMEOUT_SECONDS)
    except Exception:
        logger.error("[K_DRAFT] failed (fixed reason)")
        await _safe_finish(rid, state=STATE_FAILED, reason=REASON_MODEL_UNAVAILABLE)
        await notify_once(NOTICE_FAILED_KEY)
        return STATE_FAILED
    elapsed = int((time.monotonic() - t0) * 1000)
    cost = estimate_cost_yen(model, in_t, out_t)
    usage = dict(input_tokens=in_t, output_tokens=out_t, cost_yen=cost, elapsed_ms=elapsed,
                 exemplar_ids=json.dumps(_flatten_ids(extra["note"])))
    if not str(text or "").strip():
        logger.error("[K_DRAFT] failed (fixed reason)")
        await _safe_finish(rid, state=STATE_FAILED, reason=REASON_MODEL_EMPTY, **usage)
        await notify_once(NOTICE_FAILED_KEY)
        return STATE_FAILED
    out, reason, flags = inspect_text(text, customer_name=name, allowed_emoji=extra["allowed_emoji"])
    if reason:
        logger.warning("[K_DRAFT] rejected (fixed reason)")
        await _safe_finish(rid, state=STATE_REJECTED, reason=reason, **usage)
        return STATE_REJECTED
    await _safe_finish(rid, state=STATE_GENERATED, reason=None, text=out, text_sha=_sha(out),
                       flags=json.dumps(flags, ensure_ascii=False), **usage)
    logger.info("[K_DRAFT] generated tokens_in=%s tokens_out=%s", emit(in_t, "count", "log", "operator"),
                emit(out_t, "count", "log", "operator"))
    return STATE_GENERATED


async def _safe_finish(rid: str, **fields) -> None:
    try:
        await _finish(rid, **fields)
    except Exception:
        logger.warning("[K_DRAFT] store failed (fixed reason)")


_tasks: set = set()


def after_queue(record_id, *, user_id: str, customer_name: str, customer_message: str, category: str | None):
    """起動点（save_to_approval_queue が No. を返した直後・1 か所）。off なら何もしない（行も作らない）。
    後追い＝応答・通知を待たせない（asyncio.Task）。戻り値は Task か None。"""
    if not record_id or configured_model() == MODEL_OFF or not _db_configured():
        return None
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return None
    task = loop.create_task(run_after_queue(str(record_id), user_id=str(user_id or ""), customer_name=str(customer_name or ""),
                                            customer_message=str(customer_message or ""), category=category))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return task


async def drain_for_tests() -> None:
    while _tasks:
        await asyncio.gather(*list(_tasks), return_exceptions=True)


# ── 読取口（返信画面・統合一覧） ───────────────────────────────────────────────────
def _row(r) -> dict:
    m = dict(r._mapping)
    try:
        m["flags"] = json.loads(m.get("flags") or "[]")
    except Exception:
        m["flags"] = []
    return m


async def view_state(record_id: str) -> dict | None:
    """返信画面用: {state, reason, text, flags, model} か None（行なし・DB 未設定・例外）。"""
    if not record_id or not _db_configured():
        return None
    try:
        async with session_scope() as s:
            r = (await s.execute(sa.select(reply_k_draft).where(reply_k_draft.c.record_id == str(record_id)))).first()
    except Exception:
        logger.warning("[K_DRAFT] read failed (fixed reason)")
        return None
    return _row(r) if r is not None else None


async def states_for(record_ids: list[str]) -> dict:
    """統合一覧用: record_id → state（行がある件だけ）。DB 未設定・例外は {}。"""
    ids = [str(x) for x in record_ids if x]
    if not ids or not _db_configured():
        return {}
    try:
        async with session_scope() as s:
            rows = (await s.execute(sa.select(reply_k_draft.c.record_id, reply_k_draft.c.state).where(
                reply_k_draft.c.record_id.in_(ids)))).fetchall()
    except Exception:
        return {}
    return {str(r[0]): str(r[1]) for r in rows}


# 返信画面の札（state／reason → 文言・固定語彙）
BADGE_GENERATED = "K 下書き（下書き専用モデル・規則 v1.5）を初期表示"
BADGE_NONE = {REASON_MODEL_INVALID: "K 下書きなし（設定を確認）", REASON_MODEL_UNAVAILABLE: "K 下書きなし（モデル応答なし）",
              REASON_MODEL_EMPTY: "K 下書きなし（モデル応答なし）", REASON_INPUT_UNAVAILABLE: "K 下書きなし（案件情報を取得できず）",
              REASON_COST_CAP: "K 下書きなし（今月の費用上限）"}
BADGE_REJECTED = "K 下書きなし（検査で除外）"


def badges(k: dict | None) -> list[str]:
    if not k:
        return []
    st = k.get("state")
    if st == STATE_GENERATED:
        return [BADGE_GENERATED] + [str(f) for f in (k.get("flags") or [])]
    if st == STATE_REJECTED:
        return [BADGE_REJECTED]
    if st in (STATE_SKIPPED, STATE_FAILED):
        return [BADGE_NONE.get(str(k.get("reason") or ""), "K 下書きなし")]
    return []
