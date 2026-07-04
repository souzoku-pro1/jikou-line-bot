"""指示メッセージの応答組み立て（D3: 解析→案件検索→復唱確認→OK→App 30 起票）

設計: docs/dispatch-bot/03-natural-language-parser.md・06-confirmation-and-safety.md
- 解釈確定 → 復唱（リスク別テンプレ）＋pending 発行 → OK で App 30「下書き」起票。
  以降の prepare・承認は既存の状態機械（LINE の OK は対外承認ではない・確定判断2）
- 聞き返しは1メッセージ1論点・最大2往復で打ち切り（03 §7）
- 番号選択・聞き返し回答は現在の対話への応答。別指示は pending を割込み無効化
- セッション・pending はインメモリ（30分TTL）。再起動で消えても
  「もう一度確認される」方向に倒れる（安全側・06 §3.2）
"""

import re
import time
from dataclasses import dataclass, field

from claude_gateway import ClaudeUnavailableError
from dispatch_bot import app30_filer, case_search, confirm, enclosures, parser, registry
from hub import kintone, notify


_SESSION_TTL_SEC = 30 * 60
_MAX_CLARIFY = 2  # 聞き返しは2往復まで（03 §7）

MSG_UNSUPPORTED = ("未対応のタスクです。現在対応: "
                   + "・".join(s.display_name for s in registry.TASK_REGISTRY.values()
                               if not s.answer_only))
MSG_QUERY_LATER = "照会機能（要対応一覧など）は第2弾で実装されます"
MSG_NO_PENDING = ("確認待ちの指示はありません（期限切れの可能性があります）。"
                  "もう一度指示してください")
MSG_EXPIRED = "期限切れです。もう一度指示してください"
MSG_CANCELLED = "取り消しました。言い直してください"
MSG_CANCELLED_PENDING = "キャンセルしました。もう一度指示し直してください"
MSG_INTERRUPTED = "先ほどの確認は取り消しました。"
MSG_GIVE_UP = "うまく確定できませんでした。お手数ですが kintone から直接起票してください"
MSG_AI_DOWN = "現在AIが応答できません。復旧後にもう一度指示してください"
MSG_UNKNOWN = "指示を解釈できませんでした。例:「鈴木さんに送付案内を作って」"
MSG_FILE_FAILED = ("起票に失敗しました。時間をおいてもう一度指示してください"
                   "（管理者に通知済みです）")


@dataclass
class Session:
    """聞き返し・候補選択の対話状態（ユーザーごとに最大1件・03 §7）"""
    base_text: str = ""                 # 元指示（聞き返し回答はここに結合して再解析）
    clarify_count: int = 0              # これまでに聞き返した回数
    candidates: list = field(default_factory=list)   # 案件複数候補（番号選択待ち）
    enclosure_options: list = field(default_factory=list)  # 同封物選択肢（番号選択待ち）
    parsed: dict | None = None          # 番号選択・選択肢待ち時の解析結果
    topic: str = ""                     # 直前に聞いた論点（同一論点の再質問は1回まで・D4）
    topic_repeat: int = 0               # 同一論点の再質問回数
    choice: str = ""                    # pre_confirm 選択肢待ち（task_type を格納）
    case_hit: object = None             # 選択肢待ち時の確定済み案件
    created_at: float = field(default_factory=time.monotonic)

    def expired(self) -> bool:
        return time.monotonic() - self.created_at > _SESSION_TTL_SEC


_sessions: dict[str, Session] = {}


def _get_session(user_id: str) -> Session | None:
    s = _sessions.get(user_id)
    if s and s.expired():
        del _sessions[user_id]
        return None
    return s


def reset_sessions() -> None:
    """テスト用（聞き返しセッションと pending の両方をクリア）"""
    _sessions.clear()
    confirm.reset()


def _start_confirmation(user_id: str, parsed: dict, hit: case_search.CaseHit,
                        instruction_text: str) -> str:
    """D3: 解釈確定 → pending 発行＋復唱確認（リスク別テンプレ・06 §2）"""
    confirm.create(user_id, parsed, hit, instruction_text)
    spec = registry.get_task(parsed["task_type"])
    return confirm.confirmation_message(spec, hit, parsed)


async def _execute_confirmed(user_id: str) -> str:
    """OK 受領 → pending 消込 → App 30 起票（06 §3.3 の多層防止）"""
    state, pending = confirm.peek(user_id)
    if state == "none":
        return MSG_NO_PENDING
    if state == "expired":
        return MSG_EXPIRED
    if state == "executed":
        return f"実行済みです（App 30 No.{pending.record_id}）\n{pending.record_url}"

    try:
        rid, url, already = await app30_filer.file_from_pending(pending)
    except kintone.KintoneError as e:
        # 起票失敗: ユーザーに通知＋管理者警報。pending は消込済みでよい（再指示でやり直し）
        confirm.invalidate(user_id)
        print(f"[DISPATCHBOT] ERROR: filing failed cmd={pending.command_id[:8]}: {e}")
        await notify.notify_admin_line(
            "【指示Bot: 起票失敗】\n"
            f"タスク: {pending.parsed.get('task_type')} / 案件No.{pending.case.record_id}\n"
            f"指示原文: {pending.instruction_text[:100]}\n"
            f"エラー: {str(e)[:300]}",
            throttle_key="dispatchbot_filing_error",
        )
        return MSG_FILE_FAILED

    confirm.mark_executed(pending, rid, url)
    if already:
        return f"起票済みです（App 30 No.{rid}・二重実行を防止しました）\n{url}"
    return (f"起票しました。App 30 No.{rid}。"
            f"この後の生成・承認はkintone側で行われます\n{url}")


# 選択肢をマスタから動的取得して番号選択式で聞く項目（field_questions では扱わない）
_DYNAMIC_FIELDS = {"enclosures"}


def _first_missing_question(spec, parsed: dict) -> str | None:
    """レジストリの必須入力項目のうち**最初の不足1つだけ**の質問文を返す（1論点・03 §7）。
    不足がなければ None。モデル出力の missing_fields は判定に使わない。
    動的フィールド（同封物）は専用フロー（_handle 内）で扱う"""
    for f in spec.required_fields:
        if f in _DYNAMIC_FIELDS:
            continue
        value = parsed["customer_name"] if f == "customer_name" \
                else parsed["task_params"].get(f)
        if not value:
            return spec.field_questions.get(f, f"「{f}」を教えてください")
    return None


def _ask(session_user: str, base_text: str, topic: str, question: str,
         prev: Session | None, spec=None, **extra) -> str:
    """聞き返し（03 §7・D4改訂）:
    - 1論点1往復×必要項目数まで許容（総往復上限は spec.max_clarify・既定2）
    - 同一論点の再質問は1回まで（2回聞いても埋まらなければ打ち切り）
    - 上限超過は打ち切り→kintone直接起票を案内"""
    count = (prev.clarify_count if prev else 0) + 1
    max_rounds = spec.max_clarify if spec else _MAX_CLARIFY
    if count > max_rounds:
        _sessions.pop(session_user, None)
        return MSG_GIVE_UP
    repeat = 0
    if prev and prev.topic == topic:
        repeat = prev.topic_repeat + 1
        if repeat > 1:  # 質問1回＋再質問1回で埋まらない論点は打ち切り
            _sessions.pop(session_user, None)
            return MSG_GIVE_UP
    _sessions[session_user] = Session(base_text=base_text, clarify_count=count,
                                      topic=topic, topic_repeat=repeat, **extra)
    return question


async def handle_message(user_id: str, text: str) -> str:
    """許可済みユーザーのメッセージ → 応答テキスト（router から呼ばれる）"""
    try:
        return await _handle(user_id, text)
    except ClaudeUnavailableError:
        return MSG_AI_DOWN


async def _handle(user_id: str, text: str) -> str:
    text = (text or "").strip()
    session = _get_session(user_id)

    # ── 選択肢応答（pre_confirm の 1/2。App 31 未登録時等・D4） ──────────────
    if session and session.choice and re.fullmatch(r"[12]", text):
        spec = registry.get_task(session.choice)
        action, msg = spec.choice_fn(session.parsed, int(text))
        if action == "invalid":
            return msg
        parsed = session.parsed
        hit = session.case_hit
        base_text = session.base_text
        _sessions.pop(user_id, None)
        if action == "abort":
            return msg
        return _start_confirmation(user_id, parsed, hit, base_text)
    if session and session.choice and re.fullmatch(r"\d{1,2}", text):
        return "1 か 2 の番号で選んでください"

    # ── 番号選択①: 同封物（複数可・カンマ区切り。現対話への応答・06 §3.1） ──
    if session and session.enclosure_options and             re.fullmatch(r"\d{1,2}(\s*[,、，]\s*\d{1,2})*", text):
        opts = session.enclosure_options
        nums = [int(n) for n in re.split(r"[,、，]", text.replace(" ", "").replace("　", ""))]
        if all(1 <= n <= len(opts) for n in nums):
            chosen, seen = [], set()
            for n in nums:
                o = opts[n - 1]
                if o.key not in seen:
                    chosen.append(o)
                    seen.add(o.key)
            parsed = session.parsed
            parsed["task_params"]["enclosures"] = [o.key for o in chosen]
            parsed["task_params"]["enclosure_labels"] = [o.label for o in chosen]
            base_text = session.base_text
            _sessions.pop(user_id, None)
            return await _resolve_case_and_confirm(user_id, parsed, base_text)
        return f"1〜{len(opts)} の番号で選んでください（複数はカンマ区切り）"

    # ── 番号選択②: 案件複数候補への応答（06 §3.1） ─────────────────────────
    if session and session.candidates and re.fullmatch(r"\d{1,2}", text):
        idx = int(text)
        if 1 <= idx <= len(session.candidates):
            hit = session.candidates[idx - 1]
            parsed = session.parsed
            base_text = session.base_text
            _sessions.pop(user_id, None)
            return await _finalize(user_id, parsed, hit, base_text)
        return f"1〜{len(session.candidates)} の番号で選んでください"

    # ── 解析（聞き返し中なら元指示に回答を結合して再解析・03 §7） ─────────
    base_text = f"{session.base_text}\n（追加回答）{text}" if session and session.base_text \
                else text
    parsed = await parser.parse_instruction(base_text)
    intent = parsed["intent"]

    if intent == "confirm":
        return await _execute_confirmed(user_id)
    if intent == "cancel":
        _sessions.pop(user_id, None)
        if confirm.invalidate(user_id):
            return MSG_CANCELLED_PENDING
        return MSG_CANCELLED

    # ── 割込み無効化: pending 有効中の別指示は旧 pending を無効化（06 §3.1）──
    # （OK・キャンセル・番号選択・聞き返し回答だけが現 pending への応答。
    # 　聞き返し回答は session 経由=ここに来る時点で pending は存在しない）
    prefix = ""
    if intent in ("task", "query", "unknown") and confirm.invalidate(user_id):
        print(f"[DISPATCHBOT] pending interrupted by new instruction user={user_id[:10]}...")
        prefix = MSG_INTERRUPTED + "\n"

    if intent == "query":
        return prefix + MSG_QUERY_LATER
    if intent == "unknown":
        question = parsed["clarification"] or MSG_UNKNOWN
        return prefix + _ask(user_id, base_text, "unknown", question, session)

    # ── intent == task ───────────────────────────────────────────────────
    spec = registry.get_task(parsed["task_type"])
    if spec is None:
        _sessions.pop(user_id, None)
        if parsed["task_type"] is None:
            # task_type 未特定（低確信度分岐はここに限定・2026-07-04 実機修正:
            # task_type が特定できていれば low でもレジストリ駆動の不足聞き返しに
            # 進む。モデルは氏名の帰属曖昧さ等で low を返し得るが、不足・曖昧は
            # コード側の個別質問が自然に解消する）
            return prefix + _ask(user_id, base_text, "low_confidence",
                                 "指示の内容をもう少し具体的に教えてください"
                                 "（例:「鈴木さんに送付案内を作って」）", session)
        return prefix + MSG_UNSUPPORTED

    # 必須項目の不足 → 聞き返し（1論点ずつ・03 §7）。
    # ★質問文は必ずレジストリの定義（required_fields / field_questions）から組み立てる。
    #   モデルの missing_fields / clarification をそのまま出さない（2026-07-04 不具合修正:
    #   「書類名」「送付日」等、レジストリに存在しない項目を創作して3項目同時に
    #   要求する事象が実機で発生したため）
    if spec.param_normalizer:
        # モデル抽出値の検証・正規化（不正種別・通数は落として聞き返しに乗せる・D4）
        parsed["task_params"] = spec.param_normalizer(parsed["task_params"])
        # ユニット種別を注入（第1弾は検索対象が App 21=時効援用のみ。purpose の
        # ユニット別既定文言の解決に使う。App 33 実装時は案件由来に切り替える）
        parsed["task_params"].setdefault("unit", _DEFAULT_UNIT)

    question = _first_missing_question(spec, parsed)
    if question:
        return prefix + _ask(user_id, base_text, "customer_name", question, session, spec)

    # ── 同封物の解決（必須・動的選択肢。2026-07-04 実機エラー対応） ────────
    # 選択肢は App 32 の有効ブロックから動的取得（ハードコードしない）。
    # 指示文由来の書類名（モデルが task_params.enclosures に抽出）は
    # App 32 の表示名/ブロックキーと照合できたもののみ採用し、聞き返しをスキップ
    if "enclosures" in spec.required_fields and             not parsed["task_params"].get("enclosure_labels"):
        options = await enclosures.list_options(_DEFAULT_UNIT)
        if not options:
            _sessions.pop(user_id, None)
            return prefix + enclosures.MSG_NO_OPTIONS
        matched = enclosures.match_names(parsed["task_params"].get("enclosures"), options)
        if matched:
            parsed["task_params"]["enclosures"] = [o.key for o in matched]
            parsed["task_params"]["enclosure_labels"] = [o.label for o in matched]
        else:
            return prefix + _ask(user_id, base_text, "enclosures",
                                 enclosures.format_question(options), session, spec,
                                 enclosure_options=options, parsed=parsed)

    # ── タスク固有の動的必須項目（D4: 職務上請求は種別通数→自治体→対象者→
    #    生年月日〔様式1のみ〕の順に1論点ずつ。質問文はレジストリ定義から） ──────
    if spec.missing_param_fn:
        field_key = spec.missing_param_fn(parsed)
        if field_key:
            q = spec.field_questions.get(field_key, f"「{field_key}」を教えてください")
            return prefix + _ask(user_id, base_text, field_key, q, session, spec)

    return prefix + await _resolve_case_and_confirm(user_id, parsed, base_text)


# App 21（時効援用）のみが検索対象の第1弾定数（App 33 実装後にユニット判定を拡張）
_DEFAULT_UNIT = "時効援用"


async def _resolve_case_and_confirm(user_id: str, parsed: dict, base_text: str) -> str:
    """案件検索 → 候補選択 or 復唱確認（同封物回答後の再入口でも使う）"""
    hits = await case_search.search_cases(parsed["customer_name"])
    if not hits:
        _sessions.pop(user_id, None)
        return case_search.NOT_FOUND_MESSAGE
    if len(hits) > 1:
        _sessions[user_id] = Session(base_text=base_text, candidates=hits, parsed=parsed)
        return case_search.format_choices(hits, parsed["customer_name"])

    return await _finalize(user_id, parsed, hits[0], base_text)


async def _finalize(user_id: str, parsed: dict, hit: case_search.CaseHit,
                    base_text: str) -> str:
    """案件確定後の最終処理: pre_confirm フック（App 31照合等・D4）→ 復唱確認"""
    spec = registry.get_task(parsed["task_type"])
    if spec and spec.pre_confirm_fn:
        action, payload = await spec.pre_confirm_fn(parsed)
        if action == "choice":
            # 選択肢（1/2）待ち。応答は choice_fn で処理（現対話への応答扱い）
            _sessions[user_id] = Session(base_text=base_text, parsed=parsed,
                                         case_hit=hit, choice=parsed["task_type"])
            return payload

    # ── 解釈確定 → 復唱確認＋pending 発行（D3・06 §2-3） ────────────────
    _sessions.pop(user_id, None)
    return _start_confirmation(user_id, parsed, hit, base_text)
