"""顧客名寄せディレクトリ（書類仕分け第1段 T1）: 仕分け判定用の候補顧客リスト

設計: 2026-07-06 調査報告 §3 T1
- 仕分け判定（T2 予定）の Claude プロンプトに注入する「候補顧客リスト」を
  kintone から引く読み取り専用の部品。書き込み・Drive 操作は持たない
- 第1段のスコープは相続系のみ: App 26（相談カード (相続)）が唯一のソース。
  App 21（時効援用案件）等の将来追加は _SOURCES への1エントリ追加で行う
  （dispatch_bot/case_search.py は「名前→案件」の逆方向検索であり別物。変更しない）
- 完了・処理済みの案件も除外しない（過去案件の書類が届くため）。状態は
  Candidate.status に持たせ、除外ではなく判定側の参考情報とする
- env 未設定のソースはスキップ（全ソース未設定なら空リスト・例外にしない縮退）。
  kintone API エラーは KintoneError のまま送出する（呼び出し側が縮退可否を決める）
"""

import logging
from dataclasses import dataclass
from typing import Callable

from hub import kintone
from hub.redact import emit

logger = logging.getLogger("customer_directory")

# kintone records.json の1リクエスト上限（これ未満の件数が返ったら最終ページ）
_PAGE_SIZE = 500
# 暴走・無限ループ保険（kintone の offset 上限と同じ）
_MAX_OFFSET = 10000


@dataclass(frozen=True)
class Candidate:
    """仕分け判定プロンプトに注入する最小属性（読み取り専用）"""

    record_id: str      # ソースアプリ内のレコード番号
    app_id: str         # 実アプリID（env 解決値。仕分けログの参照先に使う）
    source: str         # ソース名（例: "相談カード (相続)"）
    customer_name: str  # 顧客名（氏名）
    decedent_name: str = ""  # 被相続人名（ソースにあれば）
    status: str = ""    # 状態（App 26 は書類ステータス。参考情報・除外には使わない）

    def label(self) -> str:
        """プロンプト注入用の1行表記（例: 山田太郎（被相続人: 山田一郎・No.12・相談カード (相続)））"""
        parts = [f"被相続人: {self.decedent_name}"] if self.decedent_name else []
        parts += [f"No.{self.record_id}", self.source]
        if self.status:
            parts.append(self.status)
        return f"{self.customer_name}（{'・'.join(parts)}）"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "")


def _from_soudan_card(record: dict, app_id: str) -> Candidate | None:
    """App 26 の1レコード → Candidate。氏名が空なら名寄せに使えないため None"""
    name = _v(record, "氏名").strip()
    if not name:
        return None
    return Candidate(
        record_id=_v(record, "$id"),
        app_id=app_id,
        source="相談カード (相続)",
        customer_name=name,
        decedent_name=_v(record, "被相続人名").strip(),
        # App 26 に案件ステータスは無く、状態らしい項目は書類ステータスのみ
        # （送付状作成/送付状作成済）。参考情報としてそのまま持たせる
        status=_v(record, "書類ステータス"),
    )


@dataclass(frozen=True)
class _Source:
    """候補顧客のソース定義。将来の App 21 等はここに1エントリ追加する"""

    app: kintone.KintoneApp
    fields: tuple[str, ...]                              # 取得フィールド（$id 含む）
    to_candidate: Callable[[dict, str], Candidate | None]  # (record, app_id) → Candidate


_SOURCES: tuple[_Source, ...] = (
    _Source(
        app=kintone.KintoneApp(
            "相談カード (相続)", "SOUZOKU_KINTONE_APP_ID", "SOUZOKU_KINTONE_API_TOKEN"),
        fields=("$id", "氏名", "被相続人名", "書類ステータス"),
        to_candidate=_from_soudan_card,
    ),
    # 将来追加例（第1段スコープ外・調査報告 §1-④）:
    # _Source(app=kintone.KintoneApp("App 21 (案件)", "KINTONE_APP_ID",
    #                                "KINTONE_API_TOKEN"),
    #         fields=("$id", "顧客名", "status"), to_candidate=_from_jikou_case)
)


async def _fetch_all(source: _Source) -> list[dict]:
    """ソースの全レコードを取得する（search_records はページングを持たないため
    limit/offset で全ページ回す。$id 昇順で安定化）"""
    records: list[dict] = []
    offset = 0
    while offset < _MAX_OFFSET:
        page = await kintone.search_records(
            source.app,
            f"order by $id asc limit {_PAGE_SIZE} offset {offset}",
            fields=list(source.fields))
        records += page
        if len(page) < _PAGE_SIZE:
            break
        offset += _PAGE_SIZE
    return records


async def list_candidates() -> list[Candidate]:
    """全ソースから仕分け判定用の候補顧客リストを引く。

    Returns: Candidate のリスト（ソース定義順→レコード番号昇順）。
    - 完了・処理済みも除外しない（status を持たせ判定側の参考とする）
    - 氏名が空のレコードは名寄せに使えないため含めない
    - env（アプリID/トークン）未設定のソースはスキップ。全ソース未設定なら
      空リストを返し例外にしない（縮退）
    - kintone API エラー（KintoneError）は送出する（呼び出し側の責務）
    """
    candidates: list[Candidate] = []
    for source in _SOURCES:
        app_id = source.app.app_id()
        if not (app_id and source.app.token()):
            logger.info("[CUSTOMER_DIRECTORY] %s は env 未設定のためスキップ",
                        emit(source.app.label, "freetext", "log", "operator"))
            continue
        for record in await _fetch_all(source):
            candidate = source.to_candidate(record, app_id)
            if candidate is not None:
                candidates.append(candidate)
    return candidates
