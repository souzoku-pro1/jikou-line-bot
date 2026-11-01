"""webapp_houki_overdue_view — HOUKI-JUKURYO-3: /app/houki/overdue（期限超過×未提出の一覧・認証 _gate）

案件番号・期限・残日数のみ（氏名・住所・LINE userId は出さない＝RV-10）。kintone から都度取得（読取のみ・書込ゼロ）。
取得失敗は固定文言。参照 API: /app/api/houki/overdue（同じ 3 列）。
"""

import html
import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from hub import houki_overdue as O
from hub.webapp_auth import _gate

logger = logging.getLogger("hub.webapp_houki_overdue_view")

router = APIRouter()

PAGE = "/app/houki/overdue"
_NOTICE_DB = "案件を取得できませんでした（kintone）。時間をおいて再表示してください。"


def _page(rows: list, notice: str, today) -> str:
    e = html.escape
    parts = [
        '<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>期限超過 - 案件管理（仮）</title>",
        "<style>body{font-family:sans-serif;max-width:720px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
        "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem;margin:0 0 .8rem}"
        "table{width:100%;border-collapse:collapse;background:#fff;border-radius:14px;overflow:hidden}"
        "th,td{padding:.6rem .8rem;border-bottom:1px solid #e2e6ec;text-align:left;font-size:.95rem}"
        "th{background:#f2f4f7;color:#556}.neg{color:#8a1f1f;font-weight:bold}.empty{color:#667;text-align:center;margin:1.2rem 0}"
        ".notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0}</style></head><body>",
        "<h1>相続放棄 期限超過（未提出）</h1>",
        f'<p class="sub">社内期限（起算日_確定の 3 か月後の応当日の前日）を超過し、申述提出日が空の案件です（{e(str(today))} 時点）。'
        '案件番号・期限・残日数のみ表示します。<a href="/app/souzoku">相続放棄の一覧へ戻る</a>｜<a href="/app/send_ops">送信確認待ち</a></p>',
    ]
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    if not rows:
        parts.append('<p class="empty">期限超過の案件はありません</p>')
    else:
        parts.append("<table><tr><th>案件番号</th><th>期限</th><th>残日数</th></tr>")
        for r in rows:
            parts.append(f'<tr><td>No.{e(r.record_id)}</td><td>{e(r.deadline.isoformat())}</td>'
                         f'<td class="neg">{int(r.remaining)}</td></tr>')
        parts.append("</table>")
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def overdue_page(request: Request):
    today = O.hj._today_jst()
    try:
        rows = await O.list_overdue(today=today)
        notice = ""
    except Exception:
        rows, notice = [], _NOTICE_DB
    return HTMLResponse(_page(rows, notice, today))


async def api_overdue(request: Request):
    today = O.hj._today_jst()
    try:
        rows = await O.list_overdue(today=today)
    except Exception:
        return {"ok": False, "reason": "kintone", "count": 0, "items": []}
    return {"ok": True, "count": len(rows), "as_of": today.isoformat(),
            "items": [{"record_id": r.record_id, "deadline": r.deadline.isoformat(), "remaining": r.remaining} for r in rows]}


router.add_api_route(PAGE, _gate(overdue_page), methods=["GET"])
router.add_api_route("/app/api/houki/overdue", _gate(api_overdue), methods=["GET"])
