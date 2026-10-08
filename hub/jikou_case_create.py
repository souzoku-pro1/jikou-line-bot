"""時効（App 21）の「検索→なければ新規作成」— HRI-03（Codex R-HUMAN-REPLY-INTAKE・対象 SHA fb771de）

指摘: 返答取込（最小レコード作成）とヒアリング（KINTONE_RECORD の作成）が、それぞれ
「ターン冒頭の検索結果（record=None）」を await を跨いで持ち越したまま作成しており、
同一 LINE ユーザーの案件レコードが二重に作られ得た。

create_or_adopt が両経路の単一の入口:
1. 共通の排他区間（hub.user_section・in-process）に入る。
2. 区間の**内側で必ず再検索**する（呼び出し側の検索結果は持ち越さない）。
   既存があれば作成せず採用（ADOPTED）。
3. 無ければ create_fn() で作成（CREATED）。
4. 作成が失敗したら再検索し、既存が見つかればそれを採用して継続（CONVERGED）。
   ——ストア側の一意制約（App 21「LINEユーザーID」の「値の重複を禁止する」）が
   発火した場合の収束。見つからなければ重複起因でない障害=従来どおり送出。
   相続放棄（App 40）の houki_case_store.apply_hearing_fields（fix3[H3-06]
   方式(a)）と同型。

**最終防衛線はストア側の一意制約**であり、(1) の排他は単一プロセス内の事前解消に
すぎない（worker／レプリカ複数化で破れる）。App 21 の当該制約は kintone 側の設定
（弁護士の点火作業）で、未設定の間は (4) が発火しない=複数プロセス間の二重作成は
防げない。設定後はコード変更なしで (4) が働く。

再検索の失敗は「作成へ進む」（従来挙動の維持: ヒアリング第 1 段階の内容を失う方が
重い。重複は一意制約と既存の複数件検知〔ambiguous／split detected〕が拾う）。
"""

import logging
from typing import Awaitable, Callable

from hub import kintone as hub_kintone
from hub.redact import emit
from hub.user_section import user_section

logger = logging.getLogger("hub.jikou_case_create")

APP_JIKOU_CASE = hub_kintone.KintoneApp(
    "App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")
USER_FIELD = "LINEユーザーID"
CHANNEL = "jikou"

# 結果（固定語彙）
CREATED = "created"          # 新規作成した
ADOPTED = "adopted"          # 区間内の再検索で既存を見つけた（作成していない）
CONVERGED = "converged"      # 作成が失敗し、再検索で見つけた既存を採用（制約発火の収束）


async def find_existing(user_id: str) -> tuple[str, int]:
    """(最新レコードの $id, 見つかった件数〔最大 2〕)。0 件は ("", 0)。失敗は送出。"""
    rows = await hub_kintone.search_records(
        APP_JIKOU_CASE,
        f'{USER_FIELD} = "{user_id}" order by $id desc limit 2',
        fields=["$id"])
    if not rows:
        return "", 0
    rid = str(((rows[0].get("$id") or {}).get("value")) or "").strip()
    return rid, len(rows)


async def create_or_adopt(user_id: str,
                          create_fn: Callable[[], Awaitable]) -> tuple[str, str, int]:
    """(record_id, 結果, 既存件数)。結果=CREATED/ADOPTED/CONVERGED。
    既存件数は ADOPTED/CONVERGED のとき 1 以上（2=同一ユーザーに複数件）。"""
    async with user_section(CHANNEL, user_id):
        try:
            rid, count = await find_existing(user_id)      # 区間内の再検索（持ち越し禁止）
        except Exception:
            logger.warning("[CASE_CREATE] re-search failed (proceed to create)")
            rid, count = "", 0
        if rid:
            logger.info("[CASE_CREATE] existing record adopted record_id=%s",
                        emit(rid, "record_id", "log", "operator"))
            return rid, ADOPTED, count
        try:
            created = await create_fn()
        except Exception:
            # ストア側の一意制約の発火（または他の作成失敗）。既存が見つかれば採用
            try:
                rid, count = await find_existing(user_id)
            except Exception:
                rid, count = "", 0
            if not rid:
                raise
            logger.info("[CASE_CREATE] duplicate create converged record_id=%s",
                        emit(rid, "record_id", "log", "operator"))
            return rid, CONVERGED, count
        return str(created), CREATED, 0
