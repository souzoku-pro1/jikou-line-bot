"""Bot仕分け指示（書類仕分け第2段②: App 38 照会中 → 確定）

設計: 2026-07-06 実装裁定（案A）
- 「〇〇のフォルダに入れて」「〇〇さんの書類」等の仕分け指示を受け、
  仕分けログ（App 38）の 状態=照会中 レコードを 確定 に更新する
- **対外効果ゼロの原則**: この操作で Drive も LINE 顧客側も一切動かない。
  Drive のフォルダ移動・リネームは GAS（第2段③）が 状態=確定 を拾って実行する
- 処理順: a) 照会中取得（0件は即答）→ b) 対象書類の確定（1件=即・複数=番号選択）
  → c) 顧客突合（customer_directory・同名複数=番号選択）→ d) 復唱→OKで確定更新
- handler への個別分岐は置かない: TaskSpec の flow_fn / flow_reply_fn / execute_fn
  フック経由で結線する（registry.py の登録エントリ参照）。handler のセッション・
  聞き返し上限・キャンセル語・pending（30分単回）の既存機構をそのまま使う
"""

import re
from dataclasses import dataclass
from datetime import datetime, timezone

from customer_directory import Candidate, list_candidates
from dispatch_bot.case_search import CaseHit
from hub import kintone

APP_SORTATION_LOG = kintone.KintoneApp(
    "App 38 (仕分けログ)", "APP_SORTATION_LOG", "TOKEN_SORTATION_LOG")

TASK_TYPE = "sortation_assign"

MSG_NO_PENDING_DOCS = "現在、照会中の書類はありません"
MSG_APP_UNSET = ("仕分けログ（App 38）が未設定のため、仕分け指示は使えません"
                 "（APP_SORTATION_LOG / TOKEN_SORTATION_LOG の登録が必要です）")
MSG_NO_CUSTOMER_MATCH = ("候補顧客に該当がありません。氏名の表記を変えて教えてください"
                         "（中止するときは「キャンセル」）")
QUESTION_CUSTOMER = "どの顧客のフォルダに仕分けしますか？氏名を教えてください"

# キャンセル語はフローで消費せず handler の解析（intent=cancel）に落とす
_CANCEL_WORDS = re.compile(r"^(キャンセル|やめて|やめる|中止|取り消し|取消)$")


@dataclass(frozen=True)
class LogDoc:
    """照会中の仕分けログ1件（選択・復唱・確定更新に使う最小属性）"""
    record_id: str
    file_name: str
    doc_type: str


def _log_url(record_id: str) -> str:
    return (f"{kintone._base_url()}/k/{APP_SORTATION_LOG.app_id()}"
            f"/show#record={record_id}")


async def _fetch_pending() -> list[LogDoc]:
    records = await kintone.search_records(
        APP_SORTATION_LOG,
        '状態 in ("照会中") order by $id asc limit 100',
        fields=["$id", "ファイル名", "書類種類"])
    return [LogDoc(
        record_id=str((r.get("$id") or {}).get("value") or ""),
        file_name=str((r.get("ファイル名") or {}).get("value") or ""),
        doc_type=str((r.get("書類種類") or {}).get("value") or ""),
    ) for r in records]


def _match_customers(candidates: list[Candidate], name: str) -> list[Candidate]:
    """氏名の部分一致（「山田」→山田太郎・山田花子の同姓複数を許容。D4方式の
    番号選択に載せる）。空白（全角含む）は除去して比較"""
    q = (name or "").replace(" ", "").replace("　", "")
    if not q:
        return []
    hits = []
    for c in candidates:
        cn = c.customer_name.replace(" ", "").replace("　", "")
        if q == cn or q in cn or cn in q:
            hits.append(c)
    return hits


def _format_doc_choices(docs: list[LogDoc]) -> str:
    lines = [f"照会中の書類が{len(docs)}件あります。番号で選んでください:"]
    lines += [f"{i}. {d.file_name}（{d.doc_type}）" for i, d in enumerate(docs, 1)]
    return "\n".join(lines)


def _format_customer_choices(cands: list[Candidate], name: str) -> str:
    lines = [f"「{name}」の候補顧客が{len(cands)}件あります。番号で選んでください:"]
    lines += [f"{i}. {c.label()}" for i, c in enumerate(cands, 1)]
    return "\n".join(lines)


def _confirm(user_id: str, parsed: dict, base_text: str,
             doc: LogDoc, cand: Candidate) -> str:
    """d) 復唱→pending 発行（既存 confirm 機構・30分単回。OK で execute が走る）"""
    from dispatch_bot import confirm, handler  # 遅延 import（循環回避）
    handler._sessions.pop(user_id, None)
    folder = f"No{cand.record_id}_{cand.customer_name}"
    parsed = {**parsed, "task_type": TASK_TYPE,
              "task_params": {**(parsed.get("task_params") or {}),
                              "log_record_id": doc.record_id,
                              "file_name": doc.file_name,
                              "customer_record_id": cand.record_id,
                              "customer_name": cand.customer_name,
                              "folder_name": folder}}
    # confirm 機構の契約上 CaseHit を渡す（仕分けの「案件」＝相談カードの顧客）
    hit = CaseHit(record_id=cand.record_id, customer_name=cand.customer_name,
                  status=cand.status or "相談カード", unit="相続")
    confirm.create(user_id, parsed, hit, base_text)
    return (f"{doc.file_name}を {folder} のフォルダに仕分けします。\n"
            f"OK / キャンセル（30分有効）")


async def _customer_step(user_id: str, parsed: dict, base_text: str,
                         doc: LogDoc, session) -> str:
    """c) 顧客突合。氏名不足は聞き返し（既存 _ask の上限機構を使う）"""
    from dispatch_bot import handler, registry  # 遅延 import（循環回避）
    name = parsed.get("customer_name")
    if not name:
        spec = registry.get_task(TASK_TYPE)
        return handler._ask(user_id, base_text, "sortation_customer",
                            QUESTION_CUSTOMER, session, spec,
                            parsed=parsed, flow=TASK_TYPE,
                            flow_state={"stage": "customer_name", "doc": doc})
    cands = _match_customers(await list_candidates(), name)
    if not cands:
        spec = registry.get_task(TASK_TYPE)
        return handler._ask(user_id, base_text, "sortation_customer",
                            MSG_NO_CUSTOMER_MATCH, session, spec,
                            parsed=parsed, flow=TASK_TYPE,
                            flow_state={"stage": "customer_name", "doc": doc})
    if len(cands) > 1:
        handler._sessions[user_id] = handler.Session(
            base_text=base_text, parsed=parsed, flow=TASK_TYPE,
            flow_state={"stage": "customer", "doc": doc, "cands": cands})
        return _format_customer_choices(cands, name)
    return _confirm(user_id, parsed, base_text, doc, cands[0])


async def flow(user_id: str, parsed: dict, base_text: str, session) -> str:
    """タスク固有フロー本体（handler の flow_fn フックから呼ばれる）"""
    from dispatch_bot import handler  # 遅延 import（循環回避）
    if not (APP_SORTATION_LOG.app_id() and APP_SORTATION_LOG.token()):
        handler._sessions.pop(user_id, None)
        return MSG_APP_UNSET

    docs = await _fetch_pending()
    if not docs:  # a) 0件は即答
        handler._sessions.pop(user_id, None)
        return MSG_NO_PENDING_DOCS

    if len(docs) == 1:  # b) 1件なら対象確定
        return await _customer_step(user_id, parsed, base_text, docs[0], session)

    # b) 複数件は番号付き一覧で聞き返し（ファイル名・書類種類）
    handler._sessions[user_id] = handler.Session(
        base_text=base_text, parsed=parsed, flow=TASK_TYPE,
        flow_state={"stage": "doc", "docs": docs})
    return _format_doc_choices(docs)


async def flow_reply(user_id: str, text: str, session) -> tuple[bool, str]:
    """flow が張ったセッションへの応答（handler の flow_reply_fn フック）。
    消費しない入力は (False, "") を返し、handler の通常解析
    （キャンセル・別指示の割込み）にそのまま落とす"""
    from dispatch_bot import handler  # 遅延 import（循環回避）
    state = session.flow_state or {}
    stage = state.get("stage")

    if stage == "doc":
        if not re.fullmatch(r"\d{1,2}", text):
            return False, ""  # 番号以外は通常解析へ（キャンセル・別指示）
        docs = state["docs"]
        idx = int(text)
        if not (1 <= idx <= len(docs)):
            return True, f"1〜{len(docs)} の番号で選んでください"
        parsed, base_text = session.parsed, session.base_text
        handler._sessions.pop(user_id, None)
        return True, await _customer_step(user_id, parsed, base_text,
                                          docs[idx - 1], None)

    if stage == "customer":
        if not re.fullmatch(r"\d{1,2}", text):
            return False, ""
        cands = state["cands"]
        idx = int(text)
        if not (1 <= idx <= len(cands)):
            return True, f"1〜{len(cands)} の番号で選んでください"
        parsed, base_text = session.parsed, session.base_text
        return True, _confirm(user_id, parsed, base_text,
                              state["doc"], cands[idx - 1])

    if stage == "customer_name":
        if _CANCEL_WORDS.fullmatch(text):
            return False, ""  # キャンセル語は通常解析（intent=cancel）へ
        if re.fullmatch(r"\d{1,2}", text):
            return True, "氏名で答えてください（番号ではありません）"
        parsed = dict(session.parsed or {})
        parsed["customer_name"] = text.strip()
        return True, await _customer_step(user_id, parsed, session.base_text,
                                          state["doc"], session)

    return False, ""


async def execute(pending) -> tuple[str, str, str]:
    """OK 後の実行（handler の execute_fn フック）: App 38 を 照会中→確定 に更新。
    Drive・LINE顧客側は一切動かさない（対外効果ゼロ・実行は GAS=③の責務）"""
    params = (pending.parsed or {}).get("task_params") or {}
    log_id = str(params.get("log_record_id") or "")
    url = _log_url(log_id)

    record = await kintone.get_record(APP_SORTATION_LOG, log_id)
    state = str((record.get("状態") or {}).get("value") or "")
    if state != "照会中":
        # 選択後に他経路（kintone 直接操作等）で処理済みになった場合の安全側
        return (f"この書類は既に処理済みです（状態={state}・更新していません）\n{url}",
                log_id, url)

    await kintone.update_record(APP_SORTATION_LOG, log_id, {
        "状態": "確定",
        "仕分け先レコードID": str(params.get("customer_record_id") or ""),
        "仕分け先氏名": str(params.get("customer_name") or ""),
        "仕分け先フォルダ名": str(params.get("folder_name") or ""),
        "確定日時": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    message = (f"仕分けを確定しました: {params.get('file_name')} → "
               f"{params.get('folder_name')}\n"
               f"Drive のフォルダ移動は次回のフォルダ整理（GAS）実行時に行われます"
               f"（対外送信なし）\n{url}")
    return message, log_id, url
