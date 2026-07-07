"""人物確認の書き込み中核（R4-2e T1）: 確認5フィールドの一括更新

設計: 2026-07-07 R4-2e 裁定
- 人の確認操作（名寄せ確定・確認状態・生死区分・死亡日・被相続人フラグ）を
  LINE 語彙（dispatch_bot/person_confirm_task.py）から起動する受け皿。
  **書き込みは全て人の LINE 指示＋二段確認の結果としてのみ起動**（関所・
  名寄せ統合と同じ意味論。「機械は確定しない」原則に抵触しない）
- 書き込み対象は上記5フィールド＋自動付記の 確認者/確認日時 の**7つのみ**
  （build_payload がホワイトリストで濾過・テスト固定）。氏名・生年月日・
  身分事項等のデータ修正はスコープ外（kintone 直編集のまま）
- 確認状態=確認済 の書き込みには 確認者・確認日時 を自動付記。
  確認者の出所: env OFFICE_ATTORNEY（既存の弁護士名設定を共用）・
  未設定時は "LINE指示Bot"
- 死亡日は DATE 型・YYYY-MM-DD のみ（形式検証は語彙側・ここでも防御）
- 人物ごと独立実行（1件の失敗が他を止めない・結果を人物ごと返す）
- env フラグは PERSON_MERGE_ENABLED を共用（名寄せ系と同じ有効化単位・既定無効）
"""

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from hub import kintone
from person_merge import APP_KOSEKI_PERSON, merge_enabled, _v

# 語彙から書き込める確認フィールド（裁定1のスコープ）
CONFIRM_FIELDS = ("名寄せ確定", "確認状態", "生死区分", "死亡日", "被相続人フラグ")
# 確認済 遷移時に自動付記するフィールド
STAMP_FIELDS = ("確認者", "確認日時")
ALLOWED_FIELDS = CONFIRM_FIELDS + STAMP_FIELDS

_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def confirmer_name() -> str:
    """確認者の既定値（出所: 既存の弁護士名 env OFFICE_ATTORNEY を共用）"""
    return os.environ.get("OFFICE_ATTORNEY") or "LINE指示Bot"


@dataclass(frozen=True)
class PersonRow:
    """一覧提示用の人物ビュー（現在値＋読解データからの推定材料）"""
    record_id: str
    name: str
    meyose: str = ""
    kakunin: str = ""
    alive: str = ""
    death_date: str = ""
    decedent: str = ""
    hints: list[str] = field(default_factory=list)  # 例「死亡記載: 令和7年4月13日」


def _hints(record: dict) -> list[str]:
    """身分事項からの推定材料（**提示のみ**・機械は値を決めない）"""
    hints = []
    for row in (record.get("身分事項") or {}).get("value") or []:
        v = {c: (x or {}).get("value") for c, x in (row.get("value") or {}).items()}
        if v.get("事項種別") == "死亡":
            date = str(v.get("年月日") or "").strip()
            hint = f"死亡記載: {date}" if date else "死亡記載あり"
            if hint not in hints:
                hints.append(hint)
    return hints


async def list_case_persons(case_record_id: str) -> list[PersonRow]:
    """案件の人物一覧（読み取りのみ）。env 未設定は空リスト"""
    if not (APP_KOSEKI_PERSON.app_id() and APP_KOSEKI_PERSON.token()):
        print("[PERSON_CONFIRM] APP_KOSEKI_PERSON 未設定のため人物を取得できません")
        return []
    records = await kintone.search_records(
        APP_KOSEKI_PERSON,
        f'案件レコードID = "{case_record_id}" order by $id asc limit 100',
        fields=["$id", "氏名", "名寄せ確定", "確認状態", "生死区分", "死亡日",
                "被相続人フラグ", "身分事項"])
    return [PersonRow(
        record_id=_v(r, "$id"), name=_v(r, "氏名"),
        meyose=_v(r, "名寄せ確定"), kakunin=_v(r, "確認状態"),
        alive=_v(r, "生死区分"), death_date=_v(r, "死亡日"),
        decedent=_v(r, "被相続人フラグ"), hints=_hints(r)) for r in records]


def build_payload(changes: dict) -> dict:
    """書き込み payload の組み立て（ホワイトリスト濾過＋自動付記＋防御検証）。

    - CONFIRM_FIELDS 以外のキーは黙って書かず落とす（対象外への書き込みゼロ）
    - 確認状態=確認済 なら 確認者・確認日時 を自動付記
    - 死亡日は YYYY-MM-DD 以外を ValueError（語彙側の形式ガードの二重防御）
    - 死亡日指定と 生死区分=生存 の同時指定は矛盾として ValueError
    """
    payload = {k: v for k, v in changes.items() if k in CONFIRM_FIELDS}
    death = str(payload.get("死亡日") or "")
    if death and not _DATE.fullmatch(death):
        raise ValueError(f"死亡日は YYYY-MM-DD 形式のみ受理します: {death!r}")
    if death and payload.get("生死区分") == "生存":
        raise ValueError("死亡日の指定と 生死区分=生存 は矛盾しています")
    if payload.get("確認状態") == "確認済":
        payload["確認者"] = confirmer_name()
        payload["確認日時"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
    return payload


async def apply_confirmations(changes_list: list[dict]) -> list[dict]:
    """人物ごとの確認書き込み（独立実行・1件の失敗が他を止めない）。

    changes_list: [{"record_id", "name", "fields": {確認フィールド: 値}}]
    Returns: [{"record_id", "name", "status": "updated"|"error", ...}]
    """
    if not merge_enabled():
        return [{"record_id": c.get("record_id"), "name": c.get("name"),
                 "status": "unavailable",
                 "reason": "PERSON_MERGE_ENABLED が未設定です"}
                for c in changes_list]
    if not (APP_KOSEKI_PERSON.app_id() and APP_KOSEKI_PERSON.token()):
        return [{"record_id": c.get("record_id"), "name": c.get("name"),
                 "status": "unavailable",
                 "reason": f"{APP_KOSEKI_PERSON.label} の env が未設定です"}
                for c in changes_list]
    results = []
    for change in changes_list:
        rid = str(change.get("record_id") or "")
        name = str(change.get("name") or "")
        try:
            payload = build_payload(change.get("fields") or {})
            if not payload:
                results.append({"record_id": rid, "name": name,
                                "status": "error", "reason": "変更内容が空です"})
                continue
            await kintone.update_record(APP_KOSEKI_PERSON, rid, payload)
            results.append({"record_id": rid, "name": name,
                            "status": "updated", "fields": payload})
            print(f"[PERSON_CONFIRM] updated No.{rid} {name} "
                  f"fields={sorted(payload)}")
        except Exception as e:
            results.append({"record_id": rid, "name": name, "status": "error",
                            "reason": str(e)[:200]})
            print(f"[PERSON_CONFIRM] 更新失敗（他の人物は継続） No.{rid}: {e}")
    return results
