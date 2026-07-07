"""Bot語彙: 人物の確認（R4-2e T2・確認5フィールドの一覧提示＋一括書き込み）

設計: 2026-07-07 R4-2e 裁定
- レジストリ1エントリ＋本モジュールへの隔離（R4-2b T2 と同型・handler への
  固有分岐ゼロ・既存語彙無改変）
- 提示: 「〔顧客名/案件〕の人物を確認して」→ 案件の全人物を**1メッセージに集約**
  （番号・レコードNo・氏名・現在値5種＋読解データからの推定材料。推定は提示のみで
  機械は値を決めない）
- 指定: 番号ベースの一括指定。複数行・複数指定の1メッセージ一括を許容（渋滞対策）:
  「1は死亡2025-04-13」「2と3は生存」「4を被相続人に」「全部確認済みに」
  「1と2の名寄せを確定」
- 復唱→OK の二段確認（関所と同じ confirm 機構）。復唱には レコードNo・氏名・
  変更フィールド・変更前→後 を全件明示
- 死亡日は YYYY-MM-DD のみ受理（大字・和暦はエラーではなく再入力案内）。
  死亡日指定と生存の矛盾は拒否。生死=死亡で死亡日未指定は許容
- 書き込みは person_confirm.apply_confirmations（人物ごと独立実行）のみ・
  env フラグは PERSON_MERGE_ENABLED を共用（無効時は一覧提示も不発）
"""

import re

from customer_directory import Candidate, list_candidates
from dispatch_bot.case_search import CaseHit
from dispatch_bot.sortation_assign import _CANCEL_WORDS, _match_customers
from hub import kintone
from person_confirm import (
    PersonRow,
    apply_confirmations,
    list_case_persons,
)
from person_merge import APP_KOSEKI_PERSON, merge_enabled

TASK_TYPE = "person_confirm"

MSG_DISABLED = "人物確認機能は現在無効です（PERSON_MERGE_ENABLED 未設定）"
MSG_NO_PERSONS = "この案件に人物レコードがありません"
MSG_NO_CUSTOMER_MATCH = ("候補顧客に該当がありません。氏名の表記を変えて教えてください"
                         "（中止するときは「キャンセル」）")
QUESTION_CUSTOMER = ("どの案件（顧客）の人物を確認しますか？氏名または案件No"
                     "（例: No.4）を教えてください")
MSG_DATE_FORMAT = ("死亡日は YYYY-MM-DD 形式で指定してください"
                   "（例: 1は死亡2025-04-13）")

USAGE = ("受理形式: 「1は死亡2025-04-13」「2と3は生存」「4を被相続人に」"
         "「1と2の名寄せを確定」「1を確認済みに」「全部確認済みに」"
         "（複数行でまとめて指定可）")

_NUMS = r"\d+(?:\s*[と,、，]\s*\d+)*"
_RE_ALL_CONFIRM = re.compile(r"^(全部|全員|全て|すべて)\s*(を)?\s*確認済み?(に|にして)?$")
_RE_DEAD = re.compile(rf"^({_NUMS})\s*は\s*死亡\s*(.*)$")
_RE_ALIVE = re.compile(rf"^({_NUMS})\s*は\s*生存(に)?(して)?$")
_RE_DECEDENT = re.compile(rf"^({_NUMS})\s*を?\s*被相続人(に|とする)?$")
_RE_MEYOSE = re.compile(rf"^({_NUMS})\s*の?\s*名寄せ(を)?\s*確定(して)?$")
_RE_CONFIRM = re.compile(rf"^({_NUMS})\s*(は|を)?\s*確認済み?(に|にして)?$")
_RE_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")

# 復唱で使う現在値のフィールド名（PersonRow 属性との対応）
_FIELD_ATTR = {"名寄せ確定": "meyose", "確認状態": "kakunin",
               "生死区分": "alive", "死亡日": "death_date",
               "被相続人フラグ": "decedent"}


def _nums(token: str) -> list[int]:
    return [int(n) for n in re.findall(r"\d+", token)]


def parse_directives(text: str, count: int) -> dict:
    """指定メッセージの解釈（純関数）。

    Returns:
      {"ok": True, "changes": {index(1始まり): {フィールド: 値}}} ／
      {"ok": False, "reason": "unmatched"|"error", "message": 再入力案内}
    行単位で解釈し、**1行も解釈できなければ unmatched**（handler の通常解析へ
    フォールスルー）。一部の行だけ解釈不能・番号範囲外・矛盾は error（案内を返す）
    """
    changes: dict[int, dict] = {}
    matched_any = False

    def add(indices: list[int], fields: dict) -> str | None:
        for i in indices:
            if not (1 <= i <= count):
                return f"1〜{count} の番号で選んでください"
            merged = changes.setdefault(i, {})
            if "生死区分" in fields and "生死区分" in merged \
                    and merged["生死区分"] != fields["生死区分"]:
                return f"番号{i} への生存/死亡の指定が矛盾しています"
            if fields.get("生死区分") == "生存" and merged.get("死亡日"):
                return f"番号{i}: 死亡日の指定と生存は矛盾しています"
            merged.update(fields)
        return None

    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        if _RE_ALL_CONFIRM.match(line):
            error = add(list(range(1, count + 1)), {"確認状態": "確認済"})
            matched_any = True
        elif m := _RE_DEAD.match(line):
            rest = m.group(2).strip()
            fields = {"生死区分": "死亡"}
            if rest:
                if not _RE_DATE.fullmatch(rest):
                    return {"ok": False, "reason": "error",
                            "message": MSG_DATE_FORMAT}
                fields["死亡日"] = rest
            error = add(_nums(m.group(1)), fields)
            matched_any = True
        elif m := _RE_ALIVE.match(line):
            error = add(_nums(m.group(1)), {"生死区分": "生存"})
            matched_any = True
        elif m := _RE_DECEDENT.match(line):
            error = add(_nums(m.group(1)), {"被相続人フラグ": "yes"})
            matched_any = True
        elif m := _RE_MEYOSE.match(line):
            error = add(_nums(m.group(1)), {"名寄せ確定": "確定"})
            matched_any = True
        elif m := _RE_CONFIRM.match(line):
            error = add(_nums(m.group(1)), {"確認状態": "確認済"})
            matched_any = True
        else:
            if matched_any:
                return {"ok": False, "reason": "error",
                        "message": f"解釈できない行があります: 「{line}」\n{USAGE}"}
            return {"ok": False, "reason": "unmatched", "message": ""}
        if error:
            return {"ok": False, "reason": "error", "message": error}
    if not matched_any:
        return {"ok": False, "reason": "unmatched", "message": ""}
    # 一括後の全体矛盾（例: 死亡日つき死亡→同じ行群で生存に上書き、は add で拒否済み。
    # ここでは 死亡日あり×生存 の最終形を防御的に再検査）
    for i, fields in changes.items():
        if fields.get("死亡日") and fields.get("生死区分") == "生存":
            return {"ok": False, "reason": "error",
                    "message": f"番号{i}: 死亡日の指定と生存は矛盾しています"}
    return {"ok": True, "changes": changes}


def _format_rows(rows: list[PersonRow], case_label: str) -> str:
    """案件の全人物を**1メッセージに集約**（現在値5種＋推定材料）"""
    lines = [f"{case_label} の人物 {len(rows)}名:"]
    for i, r in enumerate(rows, 1):
        lines.append(f"{i}. No.{r.record_id} {r.name}")
        lines.append(f"   名寄せ={r.meyose or '未設定'}／確認={r.kakunin or '未設定'}"
                     f"／生死={r.alive or '未設定'}／死亡日={r.death_date or 'なし'}"
                     f"／被相続人={r.decedent or 'no'}")
        for hint in r.hints:
            lines.append(f"   💡{hint}")
    lines.append(USAGE)
    return "\n".join(lines)


def _confirm_message(changes_list: list[dict]) -> str:
    """復唱: レコードNo・氏名・変更フィールド・変更前→後を全件明示"""
    lines = [f"以下の{len(changes_list)}名の確認内容を書き込みます:"]
    for c in changes_list:
        parts = [f"{code}「{c['before'].get(code) or '未設定'}」→「{v}」"
                 for code, v in c["fields"].items()]
        lines.append(f"・No.{c['record_id']} {c['name']}: {'、'.join(parts)}")
    lines.append("（kintone内部のみ・確認済への変更には確認者・確認日時を自動付記）")
    lines.append("OK / キャンセル（30分有効）")
    return "\n".join(lines)


async def _person_step(user_id: str, parsed: dict, base_text: str,
                       cand: Candidate) -> str:
    """案件確定 → 人物一覧の提示（stage=list のセッションを張る）"""
    from dispatch_bot import handler  # 遅延 import（循環回避）
    rows = await list_case_persons(cand.record_id)
    if not rows:
        handler._sessions.pop(user_id, None)
        return MSG_NO_PERSONS
    label = f"No.{cand.record_id} {cand.customer_name}"
    handler._sessions[user_id] = handler.Session(
        base_text=base_text, parsed=parsed, flow=TASK_TYPE,
        flow_state={"stage": "list", "case": cand, "rows": rows})
    return _format_rows(rows, label)


async def _case_step(user_id: str, parsed: dict, base_text: str,
                     session) -> str:
    """案件指定: No.直指定 → 顧客名突合 → 不足は聞き返し（関所T2と同型）"""
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
            return handler._ask(user_id, base_text, "confirm_case",
                                f"No.{direct} は候補顧客に見つかりません。"
                                "氏名または正しい案件Noを教えてください",
                                session, spec, parsed=parsed, flow=TASK_TYPE,
                                flow_state={"stage": "case_name"})
        return await _person_step(user_id, parsed, base_text, cand)

    if not name:
        return handler._ask(user_id, base_text, "confirm_case",
                            QUESTION_CUSTOMER, session, spec,
                            parsed=parsed, flow=TASK_TYPE,
                            flow_state={"stage": "case_name"})

    cands = _match_customers(candidates, name)
    if not cands:
        return handler._ask(user_id, base_text, "confirm_case",
                            MSG_NO_CUSTOMER_MATCH, session, spec,
                            parsed=parsed, flow=TASK_TYPE,
                            flow_state={"stage": "case_name"})
    if len(cands) > 1:
        handler._sessions[user_id] = handler.Session(
            base_text=base_text, parsed=parsed, flow=TASK_TYPE,
            flow_state={"stage": "customer", "cands": cands})
        lines = [f"「{name}」の候補顧客が{len(cands)}件あります。番号で選んでください:"]
        lines += [f"{i}. {c.label()}" for i, c in enumerate(cands, 1)]
        return "\n".join(lines)
    return await _person_step(user_id, parsed, base_text, cands[0])


async def flow(user_id: str, parsed: dict, base_text: str, session) -> str:
    """タスク固有フロー本体（handler の flow_fn フック）"""
    from dispatch_bot import handler  # 遅延 import（循環回避）
    if not merge_enabled():
        handler._sessions.pop(user_id, None)
        return MSG_DISABLED
    return await _case_step(user_id, parsed, base_text, session)


def _build_confirmation(user_id: str, session, rows: list[PersonRow],
                        cand, changes: dict) -> str:
    """解釈結果 → pending 発行＋復唱（関所と同じ confirm 機構・30分単回）"""
    from dispatch_bot import confirm, handler  # 遅延 import（循環回避）
    handler._sessions.pop(user_id, None)
    changes_list = []
    for idx in sorted(changes):
        row = rows[idx - 1]
        changes_list.append({
            "record_id": row.record_id, "name": row.name,
            "fields": changes[idx],
            "before": {code: getattr(row, attr)
                       for code, attr in _FIELD_ATTR.items()
                       if code in changes[idx]}})
    parsed = {**(session.parsed or {}), "task_type": TASK_TYPE,
              "task_params": {"changes": changes_list}}
    hit = CaseHit(record_id=cand.record_id, customer_name=cand.customer_name,
                  status=cand.status or "相談カード", unit="相続")
    confirm.create(user_id, parsed, hit, session.base_text)
    return _confirm_message(changes_list)


async def flow_reply(user_id: str, text: str, session) -> tuple[bool, str]:
    """flow が張ったセッションへの応答。消費しない入力は (False, "") で
    handler の通常解析（キャンセル・別指示）に落とす"""
    state = session.flow_state or {}
    stage = state.get("stage")

    if stage == "customer":
        if not re.fullmatch(r"\d{1,2}", text):
            return False, ""
        cands = state["cands"]
        idx = int(text)
        if not (1 <= idx <= len(cands)):
            return True, f"1〜{len(cands)} の番号で選んでください"
        return True, await _person_step(user_id, session.parsed,
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

    if stage == "list":
        result = parse_directives(text, len(state["rows"]))
        if not result["ok"]:
            if result["reason"] == "unmatched":
                return False, ""
            return True, result["message"]
        return True, _build_confirmation(user_id, session, state["rows"],
                                         state["case"], result["changes"])

    return False, ""


async def execute(pending) -> tuple[str, str, str]:
    """OK 後の実行（handler の execute_fn フック）: 人物ごと独立に書き込み、
    結果（成功／失敗理由）を人物ごとそのまま報告する"""
    params = (pending.parsed or {}).get("task_params") or {}
    changes_list = list(params.get("changes") or [])
    if not changes_list:
        return "対象の変更がありません", "", ""
    results = await apply_confirmations(changes_list)
    lines = []
    first_id = ""
    for r in results:
        if r["status"] == "updated":
            fields = "、".join(f"{k}={v}" for k, v in r["fields"].items()
                               if k not in ("確認者", "確認日時"))
            lines.append(f"・No.{r['record_id']} {r['name']}: {fields} を"
                         "書き込みました")
            first_id = first_id or r["record_id"]
        else:
            lines.append(f"・No.{r['record_id']} {r['name']}: "
                         f"{r.get('reason')}")
    lines.append("（kintone内部のみ・対外送信なし）")
    url = ""
    if first_id and APP_KOSEKI_PERSON.app_id():
        url = (f"{kintone._base_url()}/k/{APP_KOSEKI_PERSON.app_id()}"
               f"/show#record={first_id}")
        lines.append(url)
    return "\n".join(lines), first_id, url
