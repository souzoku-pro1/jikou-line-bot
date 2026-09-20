"""同一ユーザーの排他区間（in-process） — HRI-03（Codex R-HUMAN-REPLY-INTAKE・対象 SHA fb771de）

「検索→なければ新規作成」を、返答取込とヒアリングの両経路で**同じ鍵**の区間に
入れるための最小部品。区間は短く保つ（区間内で AI 呼び出しをしない=顧客への返信を
取込の処理時間で待たせない）。

**この排他は正当性の保証ではない。** in-process の asyncio.Lock は worker／レプリカが
複数になった時点で破れる。二重作成の最終防衛線はストア側の一意制約
（kintone「値の重複を禁止する」）と、制約が発火したときの収束（作成失敗→再検索→
既存レコードの採用）が担う。本区間は単一プロセス内の競合を**事前に**解消して、
制約発火（=例外経路）の頻度を下げる役割だけを持つ。

実装メモ:
- asyncio.Lock は最初の競合待ちでイベントループに束縛されるため、ループごとに
  台帳を分ける（WeakKeyDictionary・ループの寿命に連動）。
- 利用者がいなくなった鍵は台帳から外す（ユーザー数に比例して増え続けない）。
"""

import asyncio
import weakref
from contextlib import asynccontextmanager

# loop → {key: [Lock, 利用者数]}
_sections: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()


@asynccontextmanager
async def user_section(channel: str, user_id: str):
    """`async with user_section("jikou", user_id):` — 同一 (channel, user_id) を直列化。"""
    loop = asyncio.get_running_loop()
    table = _sections.get(loop)
    if table is None:
        table = _sections[loop] = {}
    key = f"{channel}:{user_id}"
    entry = table.get(key)
    if entry is None:
        entry = table[key] = [asyncio.Lock(), 0]
    entry[1] += 1                                # 登録（同期区間・await なし）
    try:
        async with entry[0]:
            yield
    finally:
        entry[1] -= 1
        if entry[1] == 0 and table.get(key) is entry:
            del table[key]


def active_keys() -> list[str]:
    """テスト・診断用: 現在のループで利用中の鍵（区間の解放漏れ検査）。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return []
    return sorted(_sections.get(loop) or {})
