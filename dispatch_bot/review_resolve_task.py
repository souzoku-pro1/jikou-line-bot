"""Bot語彙: 要確認の確定（S5-2.5 T2・確定の関所の dispatch_bot 結線）

設計: 2026-07-07 設計調査＋裁定
- 「〇〇さんの要確認を確定して」「要確認を処理して」等を認識し、
  review_resolve（T1 コア）の resolve_group を LINE から起動する
- 要確認 0件=即答／1グループ=対象確定／複数グループ=番号付き一覧から選択
- 案件指定: 顧客名→customer_directory 突合（同姓複数は番号選択）を主経路に、
  「No.○」のレコードID直指定（task_params.case_record_id）も受ける
- 復唱→OK→resolve_group 実行。実行結果（成功／ガード中止／env縮退／未知キー）は
  **そのまま LINE で報告**（結果の意訳・隠蔽をしない）
- kintone 内部のみの操作（Drive・LINE顧客側・対外送信なし）。handler への
  固有分岐は足さない: 仕分け第2段と同じ TaskSpec 汎用フック
  （flow_fn / flow_reply_fn / execute_fn）で結線する
"""

import re
from dataclasses import asdict

from customer_directory import Candidate, list_candidates
from dispatch_bot.case_search import CaseHit
from dispatch_bot.sortation_assign import _CANCEL_WORDS, _match_customers
from hub import kintone
from review_resolve import (
    APP_FUDOSAN,
    ReviewGroup,
    ReviewItem,
    list_pending_reviews,
    resolve_group,
)

TASK_TYPE = "review_resolve"

MSG_NO_PENDING_REVIEWS = "現在、要確認はありません"
MSG_NO_CUSTOMER_MATCH = ("候補顧客に該当がありません。氏名の表記を変えて教えてください"
                         "（中止するときは「キャンセル」）")
QUESTION_CUSTOMER = ("どの案件（顧客）に確定しますか？氏名または案件No"
                     "（例: No.12）を教えてください")


def _format_group_choices(groups: list[ReviewGroup]) -> str:
    lines = [f"要確認が{len(groups)}グループあります。番号で選んでください:"]
    for i, g in enumerate(groups, 1):
        nos = "・".join(f"No.{item.record_id}" for item in g.items)
        subject = g.items[0].subject if g.items else g.source
        lines.append(f"{i}. {subject}（{nos}）")
    return "\n".join(lines)


def _format_customer_choices(cands: list[Candidate], name: str) -> str:
    lines = [f"「{name}」の候補顧客が{len(cands)}件あります。番号で選んでください:"]
    lines += [f"{i}. {c.label()}" for i, c in enumerate(cands, 1)]
    return "\n".join(lines)


async def _group_kinds(group: ReviewGroup) -> list[str]:
    """復唱用の物件種別（App 25 の 種別）を引く。env 未設定・取得失敗は空（省略表記）"""
    if not (APP_FUDOSAN.app_id() and APP_FUDOSAN.token()):
        return []
    kinds = []
    for item in group.items:
        fudosan_id = str(item.detail.get("不動産レコードID") or "")
        if not fudosan_id:
            continue
        try:
            record = await kintone.get_record(APP_FUDOSAN, fudosan_id)
            kind = str((record.get("種別") or {}).get("value") or "")
            if kind:
                kinds.append(kind)
        except Exception as e:
            print(f"[REVIEW_RESOLVE_TASK] 種別取得に失敗（省略表記で続行）: {e}")
    return kinds


async def _confirm(user_id: str, parsed: dict, base_text: str,
                   group: ReviewGroup, cand: Candidate) -> str:
    """復唱→pending 発行（既存 confirm 機構・30分単回。OK で execute が走る）"""
    from dispatch_bot import confirm, handler  # 遅延 import（循環回避）
    handler._sessions.pop(user_id, None)
    folder = f"No{cand.record_id}_{cand.customer_name}"
    kinds = await _group_kinds(group)
    kinds_note = f"（{'・'.join(kinds)}）" if kinds else ""
    parsed = {**parsed, "task_type": TASK_TYPE,
              "task_params": {**(parsed.get("task_params") or {}),
                              "group_source": group.source,
                              "group_idem": group.idempotency_key,
                              "group_items": [asdict(i) for i in group.items],
                              "case_record_id": cand.record_id,
                              "case_name": cand.customer_name,
                              "folder_name": folder}}
    hit = CaseHit(record_id=cand.record_id, customer_name=cand.customer_name,
                  status=cand.status or "相談カード", unit="相続")
    confirm.create(user_id, parsed, hit, base_text)
    return (f"要確認{len(group.items)}件{kinds_note}を {folder} の案件に確定します。\n"
            f"OK / キャンセル（30分有効）")


async def _case_step(user_id: str, parsed: dict, base_text: str,
                     group: ReviewGroup, session) -> str:
    """案件指定: No.直指定 → 顧客名突合 → 不足は聞き返し"""
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
            return handler._ask(user_id, base_text, "review_case",
                                f"No.{direct} は候補顧客に見つかりません。"
                                "氏名または正しい案件Noを教えてください",
                                session, spec, parsed=parsed, flow=TASK_TYPE,
                                flow_state={"stage": "case_name", "group": group})
        return await _confirm(user_id, parsed, base_text, group, cand)

    if not name:
        return handler._ask(user_id, base_text, "review_case",
                            QUESTION_CUSTOMER, session, spec,
                            parsed=parsed, flow=TASK_TYPE,
                            flow_state={"stage": "case_name", "group": group})

    cands = _match_customers(candidates, name)
    if not cands:
        return handler._ask(user_id, base_text, "review_case",
                            MSG_NO_CUSTOMER_MATCH, session, spec,
                            parsed=parsed, flow=TASK_TYPE,
                            flow_state={"stage": "case_name", "group": group})
    if len(cands) > 1:
        handler._sessions[user_id] = handler.Session(
            base_text=base_text, parsed=parsed, flow=TASK_TYPE,
            flow_state={"stage": "customer", "group": group, "cands": cands})
        return _format_customer_choices(cands, name)
    return await _confirm(user_id, parsed, base_text, group, cands[0])


async def flow(user_id: str, parsed: dict, base_text: str, session) -> str:
    """タスク固有フロー本体（handler の flow_fn フックから呼ばれる）"""
    from dispatch_bot import handler  # 遅延 import（循環回避）
    groups = await list_pending_reviews()
    if not groups:
        handler._sessions.pop(user_id, None)
        return MSG_NO_PENDING_REVIEWS

    if len(groups) == 1:
        return await _case_step(user_id, parsed, base_text, groups[0], session)

    handler._sessions[user_id] = handler.Session(
        base_text=base_text, parsed=parsed, flow=TASK_TYPE,
        flow_state={"stage": "group", "groups": groups})
    return _format_group_choices(groups)


async def flow_reply(user_id: str, text: str, session) -> tuple[bool, str]:
    """flow が張ったセッションへの応答。消費しない入力は (False, "") で
    handler の通常解析（キャンセル・別指示）に落とす"""
    from dispatch_bot import handler  # 遅延 import（循環回避）
    state = session.flow_state or {}
    stage = state.get("stage")

    if stage == "group":
        if not re.fullmatch(r"\d{1,2}", text):
            return False, ""
        groups = state["groups"]
        idx = int(text)
        if not (1 <= idx <= len(groups)):
            return True, f"1〜{len(groups)} の番号で選んでください"
        parsed, base_text = session.parsed, session.base_text
        handler._sessions.pop(user_id, None)
        return True, await _case_step(user_id, parsed, base_text,
                                      groups[idx - 1], None)

    if stage == "customer":
        if not re.fullmatch(r"\d{1,2}", text):
            return False, ""
        cands = state["cands"]
        idx = int(text)
        if not (1 <= idx <= len(cands)):
            return True, f"1〜{len(cands)} の番号で選んでください"
        return True, await _confirm(user_id, session.parsed, session.base_text,
                                    state["group"], cands[idx - 1])

    if stage == "case_name":
        if _CANCEL_WORDS.fullmatch(text):
            return False, ""  # キャンセル語は通常解析（intent=cancel）へ
        parsed = dict(session.parsed or {})
        m = re.fullmatch(r"[Nn][Oo]\.?\s*(\d+)|(\d{1,5})", text.strip())
        if m:
            params = dict(parsed.get("task_params") or {})
            params["case_record_id"] = m.group(1) or m.group(2)
            parsed["task_params"] = params
            parsed["customer_name"] = None
        else:
            parsed["customer_name"] = text.strip()
            params = dict(parsed.get("task_params") or {})
            params.pop("case_record_id", None)
            parsed["task_params"] = params
        return True, await _case_step(user_id, parsed, session.base_text,
                                      state["group"], session)

    return False, ""


async def execute(pending) -> tuple[str, str, str]:
    """OK 後の実行（handler の execute_fn フック）: T1 の resolve_group を起動し、
    結果（成功／ガード中止／env縮退／未知キー）をそのまま報告する"""
    params = (pending.parsed or {}).get("task_params") or {}
    group = ReviewGroup(
        source=str(params.get("group_source") or ""),
        idempotency_key=str(params.get("group_idem") or ""),
        items=[ReviewItem(**i) for i in (params.get("group_items") or [])])
    case_id = str(params.get("case_record_id") or "")
    folder = str(params.get("folder_name") or f"No{case_id}")

    result = await resolve_group(group, case_id)
    status = result.get("status")

    if status == "resolved":
        lines = [f"要確認{len(result.get('items') or [])}件を {folder} の案件に"
                 "確定しました"]
        first_id, first_app = "", None
        from review_resolve import APP_KOSEKI_BOOK, APP_ZAISAN
        for item in result.get("items") or []:
            rid = item.get("review_record_id")
            if item.get("koseki_record_id"):  # R4-0: 戸籍の案件紐付け
                kid = str(item.get("koseki_record_id"))
                lines.append(f"・要確認 No.{rid} → 戸籍 No.{kid} に案件を紐付け")
                if not first_id:
                    first_id, first_app = kid, APP_KOSEKI_BOOK
            else:  # S5-2.5: 財産行の生成/追記
                zid = str(item.get("zaisan_record_id") or "")
                action = "追記" if item.get("zaisan") == "updated" else "新規"
                lines.append(f"・要確認 No.{rid} → 財産行 No.{zid}（{action}）")
                if not first_id:
                    first_id, first_app = zid, APP_ZAISAN
        lines.append("（kintone内部のみ・対外送信なし）")
        url = ""
        if first_id and first_app and first_app.app_id():
            url = (f"{kintone._base_url()}/k/{first_app.app_id()}"
                   f"/show#record={first_id}")
            lines.append(url)
        return "\n".join(lines), first_id, url

    # ガード中止・env縮退・未知キー: 理由をそのまま報告（意訳しない）
    prefix = {"aborted": "確定を中止しました",
              "unavailable": "確定できません（環境未設定）",
              "unsupported": "確定できません"}.get(status, "確定できませんでした")
    return f"{prefix}: {result.get('reason')}", "", ""
