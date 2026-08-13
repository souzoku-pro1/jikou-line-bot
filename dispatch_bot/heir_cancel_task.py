"""Bot語彙: 相続人確定の取消（P3-003C-CANCEL 実装票・隔離 module）

正本: DRAFT_P3_003C_CANCEL.md（FROZEN）。R1: 取消の開始は弁護士の明示操作のみ・
機械は提案も実行もしない。影響範囲の収集（対象 run・App36 行の列挙）は**人が
案件を指定した後**の読取専用支援（plan_cancel）で行い、結果を復唱に載せる。

- 復唱対象（R1(iv)）: 案件レコードID・対象 run id・巻き戻し対象の App36
  record ID 集合・巻き戻し内容の要約（PII 非搭載＝record ID・件数のみ）。
- flag ゲート: HEIR_CANCEL_ENABLED（既定 OFF・語彙一覧への掲載も flag 連動＝
  registry.visible_fn・P3-003-CMD の型。flow/execute 冒頭でも辞退＝I/O ゼロ）。
- 案件解決は review_resolve_task と同型（顧客名突合・No.直指定・番号選択）。
- pending invalidate は execute 内 finally（P3-003-CMD 裁定8 の型）。
"""

import logging
import re
from datetime import datetime, timezone

from customer_directory import list_candidates
from dispatch_bot.case_search import CaseHit
from dispatch_bot.sortation_assign import _CANCEL_WORDS, _match_customers
from hub.heir_cancel import (MSG_CANCEL_DISABLED, execute_cancel,
                             heir_cancel_enabled, plan_cancel)

logger = logging.getLogger("dispatch_bot.heir_cancel_task")

TASK_TYPE = "heir_cancel"

MSG_NO_CUSTOMER_MATCH = ("候補顧客に該当がありません。氏名の表記を変えて教えて"
                         "ください（中止するときは「キャンセル」）")
QUESTION_CUSTOMER = ("どの案件（顧客）の相続人確定を取り消しますか？"
                     "氏名または案件No（例: No.12）を教えてください")


def _summary_lines(plan: dict) -> list[str]:
    """復唱の巻き戻し要約（R1(iv)・record ID と件数のみ・PII 非搭載）。"""
    if plan["mode"] == "legacy":
        return ["巻き戻し: なし（write-set の無い旧確定＝取消台帳のみ追記・"
                "App36 は変更しません・実機修正は人手）"]
    ins = [e for e in plan["candidates"] if e["op"] == "insert"]
    upd = [e for e in plan["candidates"] if e["op"] == "update"]
    lines = [f"巻き戻し対象 App36: "
             f"{'・'.join('No.' + r for r in plan['record_ids']) or 'なし'}"]
    if ins:
        lines.append(f"　無効化（戸籍確認済=no＋取消済み=yes）: "
                     f"{'・'.join('No.' + e['app36_record_id'] for e in ins)}")
    if upd:
        lines.append(f"　反映前の値へ復元: "
                     f"{'・'.join('No.' + e['app36_record_id'] for e in upd)}")
    if plan["mismatched"]:
        lines.append(f"　要確認（現在値が反映結果と不一致・書き換えません）: "
                     f"{'・'.join('No.' + r for r in sorted(plan['mismatched']))}")
    if plan["mode"] == "resumed":
        lines.append("　※取消記録は保存済みです（未了の巻き戻しのみ再実行）")
    return lines


async def _confirm(user_id: str, parsed: dict, base_text: str, cand) -> str:
    """plan_cancel（読取専用）→ 復唱＋pending 発行（R1(iv) の復唱対象を明記）。"""
    from dispatch_bot import confirm, handler  # 遅延 import（循環回避）
    handler._sessions.pop(user_id, None)
    plan = await plan_cancel(cand.record_id, user_id)
    if plan["status"] != "plan":
        return plan["reason"]
    parsed = {**parsed, "task_type": TASK_TYPE,
              "task_params": {**(parsed.get("task_params") or {}),
                              "case_record_id": cand.record_id,
                              "case_name": cand.customer_name}}
    hit = CaseHit(record_id=cand.record_id, customer_name=cand.customer_name,
                  status=cand.status or "相談カード", unit="相続")
    confirm.create(user_id, parsed, hit, base_text)
    lines = [f"案件 No.{cand.record_id} の相続人確定（run #{plan['run_id']}）を"
             "取り消します。"]
    lines += _summary_lines(plan)
    lines.append("取消後の正しい確定は再導出→確定です（この操作は元に戻せません・"
                 "台帳には追記のみ）。")
    lines.append("OK / キャンセル（30分有効）")
    return "\n".join(lines)


async def _case_step(user_id: str, parsed: dict, base_text: str,
                     session) -> str:
    """案件指定: No.直指定 → 顧客名突合 → 不足は聞き返し（review_resolve 同型）"""
    from dispatch_bot import handler, registry  # 遅延 import（循環回避）
    spec = registry.get_task(TASK_TYPE)

    params = parsed.get("task_params") or {}
    direct = str(params.get("case_record_id") or "").strip()
    name = parsed.get("customer_name")

    candidates = await list_candidates()
    if direct:
        m = re.search(r"\d+", direct)
        cand = next((c for c in candidates
                     if m and c.record_id == m.group(0)), None)
        if cand is None:
            return handler._ask(user_id, base_text, "heir_cancel_case",
                                f"No.{direct} は候補顧客に見つかりません。"
                                "氏名または正しい案件Noを教えてください",
                                session, spec, parsed=parsed, flow=TASK_TYPE,
                                flow_state={"stage": "case_name"})
        return await _confirm(user_id, parsed, base_text, cand)

    if not name:
        return handler._ask(user_id, base_text, "heir_cancel_case",
                            QUESTION_CUSTOMER, session, spec,
                            parsed=parsed, flow=TASK_TYPE,
                            flow_state={"stage": "case_name"})

    cands = _match_customers(candidates, name)
    if not cands:
        return handler._ask(user_id, base_text, "heir_cancel_case",
                            MSG_NO_CUSTOMER_MATCH, session, spec,
                            parsed=parsed, flow=TASK_TYPE,
                            flow_state={"stage": "case_name"})
    if len(cands) > 1:
        handler._sessions[user_id] = handler.Session(
            base_text=base_text, parsed=parsed, flow=TASK_TYPE,
            flow_state={"stage": "customer", "cands": cands})
        return "\n".join(
            [f"「{name}」に複数の候補があります。番号で選んでください:"] +
            [f"{i}. No.{c.record_id} {c.customer_name}"
             for i, c in enumerate(cands, 1)])
    return await _confirm(user_id, parsed, base_text, cands[0])


async def flow(user_id: str, parsed: dict, base_text: str, session) -> str:
    """タスク固有フロー本体（handler の flow_fn フックから呼ばれる）。"""
    from dispatch_bot import handler  # 遅延 import（循環回避）
    if not heir_cancel_enabled():
        # flag ゲート（防御の二重化: 語彙非公開に加え flow 冒頭でも辞退・I/O ゼロ）
        handler._sessions.pop(user_id, None)
        return MSG_CANCEL_DISABLED
    return await _case_step(user_id, parsed, base_text, session)


async def flow_reply(user_id: str, text: str, session) -> tuple[bool, str]:
    """flow が張ったセッションへの応答（番号選択・案件再指定）。"""
    state = session.flow_state or {}
    stage = state.get("stage")

    if stage == "customer":
        if not re.fullmatch(r"\d{1,2}", text):
            return False, ""
        cands = state["cands"]
        idx = int(text)
        if not (1 <= idx <= len(cands)):
            return True, f"1〜{len(cands)} の番号で選んでください"
        return True, await _confirm(user_id, session.parsed,
                                    session.base_text, cands[idx - 1])

    if stage == "case_name":
        if _CANCEL_WORDS.fullmatch(text):
            return False, ""  # キャンセル語は通常解析（intent=cancel）へ
        parsed = dict(session.parsed or {})
        m = re.fullmatch(r"[Nn][Oo]\.?\s*(\d+)|(\d{1,5})", text.strip())
        params = dict(parsed.get("task_params") or {})
        if m:
            params["case_record_id"] = m.group(1) or m.group(2)
            parsed["customer_name"] = None
        else:
            parsed["customer_name"] = text.strip()
            params.pop("case_record_id", None)
        parsed["task_params"] = params
        return True, await _case_step(user_id, parsed, session.base_text,
                                      session)

    return False, ""


async def execute(pending) -> tuple[str, str, str]:
    """OK 後の実行（handler の execute_fn フック）。全終端で pending invalidate
    （P3-003-CMD 裁定8 の型）。実行前にも flag/条件を再検証（execute_cancel が
    phase 1 を毎回再実施＝盲目適用しない）。"""
    from dispatch_bot import confirm  # 遅延 import（循環回避）
    user_id = getattr(pending, "user_id", "")
    try:
        if not heir_cancel_enabled():
            return MSG_CANCEL_DISABLED, "", ""
        params = (pending.parsed or {}).get("task_params") or {}
        case_id = str(params.get("case_record_id") or "")
        result = await execute_cancel(case_id, user_id,
                                      datetime.now(timezone.utc))
        if result["status"] != "cancelled":
            return result.get("reason", "取消を中止しました（書き込みなし）"), "", ""
        if result["mode"] == "legacy":
            msg = (f"取消を台帳へ記録しました（run #{result['run_id']}・"
                   "legacy 確定のため App36 は変更していません。実機の修正は"
                   "人手調査で行ってください）\n"
                   f"取消封筒 No.{result['envelope_id']}")
        elif result.get("envelope_closed"):
            msg = (f"取消を完了しました（run #{result['run_id']}・"
                   f"巻き戻し {result['rolled_back']} 件）\n"
                   f"取消封筒 No.{result['envelope_id']}（クローズ済み）\n"
                   "正しい確定が必要な場合は再導出→確定してください")
        else:
            msg = (f"取消を記録しました（run #{result['run_id']}・"
                   f"巻き戻し {result['rolled_back']} 件・"
                   f"要確認 {result['held']} 件）\n"
                   f"取消封筒 No.{result['envelope_id']} は要確認のまま残して"
                   "います。現物確認のうえ再指示で残りを回収できます")
        return msg, str(result["envelope_id"]), ""
    finally:
        confirm.invalidate(user_id)   # 裁定8: 全終端で必ず実行・handler 無改変
