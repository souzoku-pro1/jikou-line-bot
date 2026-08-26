"""相続放棄ヒアリング会話（SOUZOKU-HOUKI-H3・正本 souzoku-houki/02 §1-2 +
10-unit-02 §2）

流れ（テキスト受信 1 件あたり）:
  1. 全業務ブレーキ（AUTOREPLY_PAUSED）→ 記録のみ・無応答
  2. 停止リスト（App 39 共用・userId 基準）→ 記録のみ・無応答
  3. 会話履歴: in-memory（単一 worker 前提）+ 空なら App 28 から復元
  4. App 40 の途中レコードがあれば既知項目を prompt へ注入（再質問禁止）
  5. Claude 呼び出し（record_hearing ツール・tool_choice=auto）
     - tool 呼び出しがあれば: 日付整合検証 → 適合分を App 40 へ逐次 upsert →
       tool_result（矛盾は固定語彙で返しモデルが聞き直す）→ 2 回目の呼び出しで
       最終返信を得る（2 回失敗で危険類型フラグ「申告内容の矛盾」+承認キュー）
     - 必須項目充足 + hearing_done → status 問い合わせ→電話判断待ち（一方向。
       電話推奨度判定・通知は H-4）
  6. 送信ゲート（第 2 世代ガード機構を共用）: サニタイズ・300 字/質問数・
     名乗り/記号/無根拠語+route=houki_hearing（根拠集合空=具体値は降格）。
     違反は承認キュー（App 29 共用）+確認中定型で応答
  7. 送信は HOUKI_CHANNEL のみ（時効チャネルの資格情報には触れない・
     AST checker が構造固定）。App 28 へ会話を保存（category=相続放棄ヒアリング）

Claude 全断（ClaudeUnavailableError）は確認中定型+承認キュー+ログ
（souzoku-houki/02 §6・handle_claude_outage と同型の縮退）。
"""

import logging

from hub import houki_case_store
from hub import reply_sanitizer
from hub.houki_profile import (
    HEARING_TEMPLATE_BLOCKS_HOUKI,
    HOUKI_HEARING_CATEGORY,
    HOUKI_HEARING_PROMPT,
    HOUKI_PROFILE,
    ClaudeUnavailableError,
    autoreply_paused,
    call_hearing_model,
    get_recent_chat_history,
    save_to_approval_queue,
    save_to_chatlog,
    style_guard_violations,
)
from hub.autoreply_stoplist import is_suppressed
from hub.line_channel import HOUKI_CHANNEL, reply_with_push_fallback
from hub.redact import emit

logger = logging.getLogger("houki_bot.hearing")

# ユーザーごとの会話履歴（in-memory・単一 worker 前提。履歴は App 28 から
# 復元できるため消失しても会話は継続する）。日付整合の失敗状態は fix1[03] で
# App 40 が正本（メモの固定マーカー+危険類型フラグ）＝in-memory を持たない
conversation_histories: dict[str, list] = {}

_MAX_HISTORY_TURNS = 10


def _known_items_note(record: dict | None) -> str:
    """App 40 の途中レコードから既知項目一覧を組み立てる（再質問禁止・
    正本 02 §1「途中レコードがあれば続きから」）。"""
    if not record:
        return ""
    lines = []
    for code in sorted(houki_case_store.HEARING_WRITABLE_FIELDS):
        value = str((record.get(code) or {}).get("value") or "").strip()
        if value:
            lines.append(f"- {code}: {value}")
    if not lines:
        return ""
    return ("\n\n【収集済み項目（既知・再質問禁止）】\n" + "\n".join(lines)
            + "\n上記は既に回答済みの項目です。同じ内容を再度質問しないで"
              "ください。未収集の項目から続けてください。")


def _extract_text(response) -> str:
    # 動的アクセス（getattr 等）は houki_bot ポリシーで禁止のため直接属性参照。
    # content block は常に .type を持つ（anthropic SDK の契約）
    parts = [b.text for b in response.content if b.type == "text"]
    return "".join(parts).strip()


def _extract_tool_use(response):
    for b in response.content:
        if b.type == "tool_use" and b.name == "record_hearing":
            return b
    return None


async def _apply_record_hearing(user_id: str, record: dict | None,
                                tool_input: dict) -> tuple[str, dict | None]:
    """record_hearing の入力を検証・upsert し、(tool_result 文字列,
    最新レコード) を返す。矛盾は固定語彙で返す（モデルが聞き直す）。"""
    # fix1[01]: 既存レコードとの合成（postimage 候補）で cross-turn の
    # 日付矛盾も検証する
    fields, problems = houki_case_store.split_valid_fields(
        tool_input.get("fields") or {}, record)
    record_id = await houki_case_store.upsert_case_fields(user_id, fields, record)
    creditors = tool_input.get("creditor_names") or []
    if creditors:
        latest = await houki_case_store.fetch_case(user_id)
        await houki_case_store.append_creditors(record_id, latest, creditors)

    if problems:
        # fix1[03]: 失敗状態は App 40 が正本（再起動を跨いで持続）。
        #   1 回目=メモへ固定マーカー / 2 回目=承認キュー→危険類型フラグ
        #   （queue 先行の at-least-once: queue 喪失時はフラグ未書込のまま
        #   次回再発火＝未通知の沈黙を作らない。重複起票は人が閉じる） /
        #   フラグ済み=増分 0（承認キューの再作成を抑止）
        latest = await houki_case_store.fetch_case(user_id)
        rid = str(((latest or {}).get("$id") or {}).get("value") or record_id)
        if latest is not None and not houki_case_store.has_mismatch_marker(latest):
            await houki_case_store.add_mismatch_marker(rid, latest)
            logger.info("[HOUKI_HEARING] date mismatch first userId=%s...",
                        emit(user_id[:10], "record_id", "log", "operator"))
        elif latest is not None \
                and not houki_case_store.has_mismatch_flag(latest):
            await save_to_approval_queue(
                user_id=user_id,
                customer_name="（相続放棄ヒアリング中）",
                customer_message="（日付整合の検証が2回失敗）",
                ai_draft="（日付申告の矛盾。会話履歴と案件レコードを確認して"
                         "ください）",
                category="相続放棄ヒアリング・要確認",
                reason="[相続放棄ヒアリング] 日付整合検証の2回失敗: "
                       + " / ".join(problems),
            )
            await houki_case_store.mark_date_mismatch_flag(rid, latest)
        result = ("記録しましたが、日付に矛盾があるため日付欄は保存して"
                  "いません: " + " / ".join(problems)
                  + "。丁寧に確認し直してください。")
    else:
        result = "記録しました。"

    latest = await houki_case_store.fetch_case(user_id)
    # 必須充足 + hearing_done → status 遷移（問い合わせ→電話判断待ち・一方向）
    if latest and bool(tool_input.get("hearing_done")) \
            and houki_case_store.hearing_required_satisfied(latest, {}):
        promoted = await houki_case_store.promote_status_to_phone_triage(
            str((latest.get("$id") or {}).get("value") or ""), latest)
        if promoted:
            result += " 必須項目が揃ったため案件を弁護士の判断待ちに進めました。"
    return result, latest


async def _converse(user_id: str, record: dict | None,
                    history: list[dict]) -> str:
    """Claude と最大 2 往復（tool 実行 1 回まで）して最終返信文を得る。"""
    system = HOUKI_HEARING_PROMPT + _known_items_note(record)
    messages = list(history)
    response = await call_hearing_model(system, messages)
    tool_use = _extract_tool_use(response)
    if tool_use is None:
        return _extract_text(response)

    tool_result, _latest = await _apply_record_hearing(
        user_id, record, tool_use.input or {})
    messages = messages + [
        {"role": "assistant", "content": response.content},
        {"role": "user", "content": [{
            "type": "tool_result",
            "tool_use_id": tool_use.id,
            "content": tool_result,
        }]},
    ]
    followup = await call_hearing_model(system, messages)
    text = _extract_text(followup)
    if text:
        return text
    return _extract_text(response)


async def handle_houki_hearing(reply_token: str, user_id: str,
                               user_text: str) -> None:
    """相続放棄ヒアリングのメインエントリ（router から BackgroundTasks で実行）。"""
    if autoreply_paused():
        logger.info("[HOUKI_HEARING] paused (global brake) userId=%s...",
                    emit(user_id[:10], "record_id", "log", "operator"))
        return
    if await is_suppressed(user_id):
        logger.info("[HOUKI_HEARING] suppressed (stoplist) userId=%s...",
                    emit(user_id[:10], "record_id", "log", "operator"))
        return

    history = conversation_histories.setdefault(user_id, [])
    if not history:
        history.extend(await get_recent_chat_history(user_id))
    history.append({"role": "user", "content": user_text})
    del history[:-_MAX_HISTORY_TURNS * 2]

    record = await houki_case_store.fetch_case(user_id)

    try:
        reply_text = await _converse(user_id, record, history)
    except ClaudeUnavailableError:
        # souzoku-houki/02 §6: 全断は確認中定型+承認キュー（同型の縮退）
        logger.error("[HOUKI_HEARING] claude unavailable")
        await save_to_approval_queue(
            user_id=user_id,
            customer_name="（相続放棄ヒアリング中）",
            customer_message=user_text,
            ai_draft="（AI応答不能のため下書きがありません。顧客メッセージを"
                     "確認し、この欄に返信文を記入して承認してください）",
            category="相続放棄ヒアリング・要確認",
            reason="[相続放棄ヒアリング] Claude応答不能（要手動対応）",
        )
        await reply_with_push_fallback(HOUKI_CHANNEL, reply_token, user_id,
                                       HOUKI_PROFILE.pending_reply)
        await save_to_chatlog(user_id, "user", user_text,
                              HOUKI_HEARING_CATEGORY, "no")
        await save_to_chatlog(user_id, "assistant",
                              HOUKI_PROFILE.pending_reply,
                              HOUKI_HEARING_CATEGORY, "yes")
        history.pop()   # 応答なしの user メッセージを履歴に残さない
        return
    except Exception:
        logger.error("[HOUKI_HEARING] converse failed (fixed reason)")
        await reply_with_push_fallback(HOUKI_CHANNEL, reply_token, user_id,
                                       HOUKI_PROFILE.pending_reply)
        history.pop()
        return

    # ── 送信ゲート（第 2 世代ガード機構の共用・route=houki_hearing） ──────────
    cleaned, _issues, fatal = reply_sanitizer.sanitize_reply(
        reply_text, allowed_emoji=HOUKI_PROFILE.allowed_emoji)
    violations = ((["プレースホルダ/内部マーカー残存"] if fatal else [])
                  + reply_sanitizer.structure_violations(
                      cleaned, exempt_blocks=HEARING_TEMPLATE_BLOCKS_HOUKI)
                  + style_guard_violations(
                      cleaned, route=HOUKI_PROFILE.hearing_style_route))
    if violations:
        await save_to_approval_queue(
            user_id=user_id,
            customer_name="（相続放棄ヒアリング中）",
            customer_message=user_text,
            ai_draft=cleaned,
            category="相続放棄ヒアリング・要確認",
            reason="[相続放棄ヒアリング送信ゲートで降格] "
                   + " / ".join(violations),
        )
        await reply_with_push_fallback(HOUKI_CHANNEL, reply_token, user_id,
                                       HOUKI_PROFILE.pending_reply)
        await save_to_chatlog(user_id, "user", user_text,
                              HOUKI_HEARING_CATEGORY, "no")
        await save_to_chatlog(user_id, "assistant",
                              HOUKI_PROFILE.pending_reply,
                              HOUKI_HEARING_CATEGORY, "yes")
        history.append({"role": "assistant",
                        "content": HOUKI_PROFILE.pending_reply})
        return

    await reply_with_push_fallback(HOUKI_CHANNEL, reply_token, user_id,
                                   cleaned)
    history.append({"role": "assistant", "content": cleaned})
    await save_to_chatlog(user_id, "user", user_text,
                          HOUKI_HEARING_CATEGORY, "no")
    await save_to_chatlog(user_id, "assistant", cleaned,
                          HOUKI_HEARING_CATEGORY, "yes")
