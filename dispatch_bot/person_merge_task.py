"""Bot語彙: 名寄せ候補の提示・確定（R4-2b T2）

設計: 2026-07-07 R4-2b 裁定
- レジストリ1エントリ＋本モジュールへの隔離（確定の関所T2と同型・handler への
  固有分岐ゼロ: flow_fn / flow_reply_fn / execute_fn の汎用フックのみ）
- 語彙の意味論は関所（案件確定）と**別系統**。既存関所語彙への変更ゼロ
- **一覧提示を基本形とする**（渋滞対策・裁定）:
  「名寄せ候補を見せて」→ 未処理の person_merge 封筒を**1メッセージに集約**
  （番号・勝者/敗者の氏名とレコード番号・成立シグナル・保留フラグの別）。
  1件ずつの吹き出し分割はしない
- 確定操作は番号指定: 「1と3を統合して」（複数一括）／「2は別人」（棄却）／
  「全部統合して」（保留フラグなしの候補全件——保留つきは対象外と復唱）
- 復唱→OK の二段確認は関所と同じ型（confirm 機構・30分単回）。復唱には
  統合される人物の氏名・レコード番号・敗者側の無効化されるレコード番号を明示する
  （RV-08 soft merge: 敗者は物理削除せず「統合済み無効」で残置）
- env フラグは PERSON_MERGE_ENABLED を共用（無効時は一覧提示も不発）
- 実行は Railway 直（kintone 内部のみ・Drive・LINE顧客側・対外送信なし）
"""

import re
from dataclasses import asdict

from dispatch_bot.case_search import CaseHit
from hub import kintone
from person_merge import APP_KOSEKI_PERSON, merge_enabled
from person_merge_exec import (
    MergeCandidate,
    execute_merge,
    list_merge_candidates,
    reject_pair,
)

TASK_TYPE = "person_merge"

MSG_DISABLED = "名寄せ機能は現在無効です（PERSON_MERGE_ENABLED 未設定）"
MSG_NO_CANDIDATES = "現在、名寄せ候補はありません"
MSG_NO_MERGEABLE = ("保留（案件相違）以外の候補がありません。"
                    "保留つきは「1と2を統合して」の番号指定で個別に確定してください")

# 「全部統合して」「すべて統合」等（渋滞対策の一括確定）
_ALL_MERGE = re.compile(r"(全部|全て|すべて)\s*(を)?\s*統合")
# 「1と3を統合して」「1、3を統合」「2を統合して」等
_NUM_MERGE = re.compile(r"^[\d\s,、，と]+(を|も)?\s*統合")
# 「2は別人」「2番は別人」
_REJECT = re.compile(r"^(\d{1,2})\s*(番)?\s*(は|が)?\s*別人")


def _format_candidates(cands: list[MergeCandidate]) -> str:
    """未処理候補の一覧を**1メッセージに集約**する（吹き出し分割しない・裁定）"""
    lines = [f"名寄せ候補が{len(cands)}件あります:"]
    for i, c in enumerate(cands, 1):
        hold = "【保留: 案件相違】" if c.pending_case else ""
        lines.append(f"{i}. {c.winner_label()} ⇔ {c.loser_label()}{hold}")
        lines.append(f"   シグナル: {'・'.join(c.signals) or '（記録なし）'}")
    lines.append("操作: 「1と3を統合して」／「2は別人」／「全部統合して」"
                 "（保留つきは「全部」の対象外）")
    return "\n".join(lines)


def _confirm(user_id: str, session, action: str,
             targets: list[MergeCandidate], held_count: int = 0) -> str:
    """復唱→pending 発行（関所と同じ confirm 機構・30分単回。OK で execute）"""
    from dispatch_bot import confirm, handler  # 遅延 import（循環回避）
    handler._sessions.pop(user_id, None)
    parsed = {**(session.parsed or {}), "task_type": TASK_TYPE,
              "task_params": {"action": action,
                              "targets": [asdict(c) for c in targets]}}
    hit = CaseHit(record_id=targets[0].winner_id,
                  customer_name="名寄せ", status="人物", unit="相続")
    confirm.create(user_id, parsed, hit, session.base_text)

    if action == "reject":
        c = targets[0]
        return (f"候補（{c.winner_label()} ⇔ {c.loser_label()}）を"
                "【別人】として棄却します。\n"
                "封筒をクローズし、このペアは今後自動起票されません。\n"
                "OK / キャンセル（30分有効）")

    lines = [f"以下の{len(targets)}件を統合します:"]
    for c in targets:
        lines.append(f"・{c.loser_label()} を {c.winner_label()} に統合"
                     f"（No.{c.loser_id} のレコードは無効化されます）")
        if c.pending_case:
            lines.append(f"  ⚠ 保留つき: {c.pending_reason or '案件参照が相違'}"
                         "（勝者側の案件参照が残ります）")
    if held_count:
        lines.append(f"※ 保留（案件相違）つきの{held_count}件は対象外です")
    lines.append("敗者レコードは削除せず「統合済み無効」で残置します"
                 "（RV-08 soft merge・監査JSONを封筒に添付）。")
    lines.append("OK / キャンセル（30分有効）")
    return "\n".join(lines)


async def flow(user_id: str, parsed: dict, base_text: str, session) -> str:
    """タスク固有フロー本体（handler の flow_fn フック）。
    どんな指示でも**まず一覧提示**（渋滞対策の基本形）——操作は一覧への番号指定で行う"""
    from dispatch_bot import handler  # 遅延 import（循環回避）
    if not merge_enabled():
        handler._sessions.pop(user_id, None)
        return MSG_DISABLED
    cands = await list_merge_candidates()
    if not cands:
        handler._sessions.pop(user_id, None)
        return MSG_NO_CANDIDATES
    handler._sessions[user_id] = handler.Session(
        base_text=base_text, parsed=parsed, flow=TASK_TYPE,
        flow_state={"stage": "list", "cands": cands})
    return _format_candidates(cands)


async def flow_reply(user_id: str, text: str, session) -> tuple[bool, str]:
    """一覧セッションへの応答（統合・別人・全部）。消費しない入力は (False, "")
    で handler の通常解析（キャンセル・別指示）に落とす"""
    state = session.flow_state or {}
    if state.get("stage") != "list":
        return False, ""
    cands: list[MergeCandidate] = state["cands"]
    t = (text or "").strip()

    if _ALL_MERGE.search(t):
        targets = [c for c in cands if not c.pending_case]
        held = len(cands) - len(targets)
        if not targets:
            return True, MSG_NO_MERGEABLE
        return True, _confirm(user_id, session, "merge", targets, held_count=held)

    m = _REJECT.match(t)
    if m:
        idx = int(m.group(1))
        if not (1 <= idx <= len(cands)):
            return True, f"1〜{len(cands)} の番号で選んでください"
        return True, _confirm(user_id, session, "reject", [cands[idx - 1]])

    if _NUM_MERGE.match(t):
        nums = [int(n) for n in re.findall(r"\d+", t.split("統合")[0])]
        if not nums:
            return True, f"1〜{len(cands)} の番号で選んでください"
        if any(not (1 <= n <= len(cands)) for n in nums):
            return True, f"1〜{len(cands)} の番号で選んでください"
        targets = []
        for n in dict.fromkeys(nums):  # 重複番号は1回
            targets.append(cands[n - 1])
        return True, _confirm(user_id, session, "merge", targets)

    return False, ""


def _record_url(record_id: str) -> str:
    if not (record_id and APP_KOSEKI_PERSON.app_id()):
        return ""
    return (f"{kintone._base_url()}/k/{APP_KOSEKI_PERSON.app_id()}"
            f"/show#record={record_id}")


async def execute(pending) -> tuple[str, str, str]:
    """OK 後の実行（handler の execute_fn フック）: 候補ごとに独立して実行し、
    結果（統合／棄却／ガード中止／env縮退）を**そのまま報告**する（意訳しない）"""
    params = (pending.parsed or {}).get("task_params") or {}
    action = str(params.get("action") or "")
    targets = [MergeCandidate(**t) for t in (params.get("targets") or [])]
    if not targets:
        return "対象の候補がありません", "", ""

    lines = []
    first_id = ""
    for cand in targets:
        result = await (reject_pair(cand) if action == "reject"
                        else execute_merge(cand))
        status = result.get("status")
        if status == "merged":
            note = ""
            if result.get("repointed"):
                nos = "・".join(f"No.{p['person_record_id']}"
                                for p in result["repointed"])
                note = f"（親エッジ付け替え: {nos}）"
            lines.append(f"・{cand.loser_label()} を {cand.winner_label()} に"
                         f"統合しました。No.{cand.loser_id} は無効化（統合済み"
                         f"無効・残置）・監査JSONを"
                         f"封筒 No.{cand.review_record_id} に添付{note}")
            first_id = first_id or cand.winner_id
        elif status == "rejected":
            lines.append(f"・{cand.winner_label()} ⇔ {cand.loser_label()} を"
                         "別人として棄却しました（このペアは今後起票されません）")
        else:
            lines.append(f"・{cand.winner_label()} ⇔ {cand.loser_label()}: "
                         f"{result.get('reason')}")
    lines.append("（kintone内部のみ・対外送信なし）")
    url = _record_url(first_id)
    if url:
        lines.append(url)
    return "\n".join(lines), first_id, url
