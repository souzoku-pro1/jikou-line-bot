"""webapp_reply_metrics_view — JIKOU-REPLY-Q6: /app/reply/metrics（返信品質の運用計測・読取専用）

認証は _gate（PWA の人＝大野のみ・bot／署名サービスには開かない）。表示は件数・率・時間・定型 ID・社名・日付のみ。
相談者の氏名・LINE userId・line_user_hash・本文・電話番号は集計元から読まず、画面にも API にも出さない（RV-10）。
DB 書込ゼロ・kintone 呼出ゼロ。参照 API: /app/api/reply/metrics（同じ数値）。
"""

import html
import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from hub import jikou_reply_metrics as M
from hub.webapp_auth import _gate

logger = logging.getLogger("hub.webapp_reply_metrics_view")

router = APIRouter()

PAGE = "/app/reply/metrics"
API = "/app/api/reply/metrics"
_NOTICE_DB = "集計を読み取れませんでした（DB）。時間をおいて再表示してください。"
_STATUS_LABELS = {"active": "有効", "retired": "退役"}


def _page(m, notice: str) -> str:
    e = html.escape
    parts = [
        '<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>返信計測 - 案件管理（仮）</title>",
        "<style>body{font-family:sans-serif;max-width:760px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
        "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}h2{font-size:1.02rem;margin:1.1rem 0 .3rem}"
        ".sub{color:#667;font-size:.85rem;margin:0 0 .8rem}table{width:100%;border-collapse:collapse;background:#fff;"
        "border-radius:14px;overflow:hidden;margin:.3rem 0}th,td{padding:.5rem .7rem;border-bottom:1px solid #e2e6ec;"
        "text-align:left;font-size:.93rem}th{background:#f2f4f7;color:#556}.num{text-align:right}"
        ".empty{color:#667;margin:.4rem 0}.notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0}"
        ".kpi{display:flex;gap:.6rem;flex-wrap:wrap}.kpi div{background:#fff;border-radius:12px;padding:.6rem .9rem;"
        "min-width:9rem}.kpi b{display:block;font-size:1.2rem}</style></head><body>",
        "<h1>時効 返信計測</h1>",
        '<p class="sub">reply_pending・reply_draft_approval・reply_exemplar・jikou_reply_text・送信台帳からの読取専用集計です。'
        '相談者の氏名・LINE ID・本文は表示しません。<a href="/app/pending">対応待ち</a>｜<a href="/app/drafts">下書き</a>｜'
        '<a href="/app/exemplars">手本</a></p>',
    ]
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    if m is None:
        parts.append('<script src="/app/shell.js"></script></body></html>')
        return "".join(parts)
    as_of = m.as_of.astimezone(M._JST).strftime("%Y-%m-%d %H:%M")
    parts.append(f'<p class="sub">{e(as_of)} JST 時点</p>')
    # 5) 未返信
    parts.append("<h2>未返信（対応待ち残）</h2><div class=\"kpi\">"
                 f"<div>件数<b>{int(m.open_count)}</b></div>"
                 f"<div>最古の経過<b>{e(M.fmt_minutes(m.oldest_open_min))}</b></div></div>")
    # 1) 遅延
    parts.append("<h2>受信→弁護士返信の遅延</h2><table><tr><th>期間</th><th class=\"num\">件数</th><th class=\"num\">中央値</th>"
                 "<th class=\"num\">90%</th><th class=\"num\">最長</th></tr>")
    for lx in m.latency:
        parts.append(f'<tr><td>直近 {int(lx.days)} 日</td><td class="num">{int(lx.count)}</td>'
                     f'<td class="num">{e(M.fmt_minutes(lx.median_min))}</td><td class="num">{e(M.fmt_minutes(lx.p90_min))}</td>'
                     f'<td class="num">{e(M.fmt_minutes(lx.max_min))}</td></tr>')
    parts.append("</table>")
    # 2) 下書き
    parts.append("<h2>AI 下書きの結果</h2><table><tr><th>期間</th><th class=\"num\">件数</th><th class=\"num\">無修正承認</th>"
                 "<th class=\"num\">修正承認</th><th class=\"num\">却下</th></tr>")
    for d in m.drafts:
        label = f"直近 {int(d.days)} 日" if d.days else "全期間"
        parts.append(f'<tr><td>{e(label)}</td><td class="num">{int(d.total)}</td>'
                     f'<td class="num">{int(d.asis)}（{e(M.fmt_rate(d.asis_rate))}）</td>'
                     f'<td class="num">{int(d.edited)}（{e(M.fmt_rate(d.edited_rate))}）</td>'
                     f'<td class="num">{int(d.rejected)}（{e(M.fmt_rate(d.rejected_rate))}）</td></tr>')
    parts.append("</table>")
    # 3) 昇格定型
    parts.append(f"<h2>凍結昇格した定型（{len(m.promoted)} 件）</h2>")
    if not m.promoted:
        parts.append('<p class="empty">昇格した定型はありません</p>')
    else:
        parts.append("<table><tr><th>定型 ID</th><th>分類</th><th>昇格日</th><th>状態</th></tr>")
        for p in m.promoted:
            parts.append(f"<tr><td>{e(p.text_id)}</td><td>{e(p.category)}</td><td>{e(p.promoted_on)}</td>"
                         f"<td>{e(_STATUS_LABELS.get(p.status, p.status))}</td></tr>")
        parts.append("</table>")
    # 4) 債権者別
    parts.append("<h2>債権者別（AI 下書きの件数と承認率）</h2>")
    if not m.creditors:
        parts.append('<p class="empty">集計対象の下書きはありません</p>')
    else:
        parts.append("<table><tr><th>債権者</th><th class=\"num\">下書き</th><th class=\"num\">結果あり</th>"
                     "<th class=\"num\">承認率</th></tr>")
        for c in m.creditors:
            parts.append(f'<tr><td>{e(c.creditor)}</td><td class="num">{int(c.drafts)}</td><td class="num">{int(c.decided)}</td>'
                         f'<td class="num">{e(M.fmt_rate(c.approval_rate))}</td></tr>')
        parts.append("</table>")
    # 6) 案件結果（JIKOU-FOLLOWUP-1 (3)）
    parts.append("<h2>案件結果（債権者別・時効援用通知の発送後）</h2>"
                 f'<p class="sub">発送済みで結果未記録の枠: {int(m.case_unanswered)}。記録は /app/jikou/result/（案件 No.）から。</p>')
    if "case_results_unavailable" in m.notes:
        parts.append('<p class="empty">案件結果の表を読めませんでした（migration 未適用の可能性）</p>')
    elif not m.case_results:
        parts.append('<p class="empty">記録された案件結果はありません</p>')
    else:
        parts.append("<table><tr><th>債権者</th><th class=\"num\">結果あり</th><th class=\"num\">成立</th>"
                     "<th class=\"num\">一部成立</th><th class=\"num\">不成立</th><th class=\"num\">回答なし</th>"
                     "<th class=\"num\">成立率</th><th class=\"num\">回答まで（中央値）</th></tr>")
        for c in m.case_results:
            md = "—" if c.median_days is None else f"{int(round(c.median_days))}日"
            parts.append(f'<tr><td>{e(c.creditor)}</td><td class="num">{int(c.cases)}</td><td class="num">{int(c.established)}</td>'
                         f'<td class="num">{int(c.partial)}</td><td class="num">{int(c.not_established)}</td>'
                         f'<td class="num">{int(c.no_answer)}</td><td class="num">{e(M.fmt_rate(c.established_rate))}</td>'
                         f'<td class="num">{e(md)}</td></tr>')
        parts.append("</table>")
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


def _json(m) -> dict:
    return {
        "ok": True, "as_of": m.as_of.isoformat(),
        "open": {"count": int(m.open_count), "oldest_minutes": m.oldest_open_min},
        "latency": [{"days": x.days, "count": x.count, "median_minutes": x.median_min, "p90_minutes": x.p90_min,
                     "max_minutes": x.max_min} for x in m.latency],
        "drafts": [{"days": d.days, "total": d.total, "approved_asis": d.asis, "approved_edited": d.edited,
                    "rejected": d.rejected, "asis_rate": d.asis_rate, "edited_rate": d.edited_rate,
                    "rejected_rate": d.rejected_rate} for d in m.drafts],
        "promoted": [{"text_id": p.text_id, "category": p.category, "promoted_on": p.promoted_on, "status": p.status}
                     for p in m.promoted],
        "creditors": [{"creditor": c.creditor, "drafts": c.drafts, "decided": c.decided, "approved": c.approved,
                       "approval_rate": c.approval_rate} for c in m.creditors],
        "case_results": [{"creditor": c.creditor, "cases": c.cases, "established": c.established, "partial": c.partial,
                          "not_established": c.not_established, "no_answer": c.no_answer,
                          "established_rate": c.established_rate, "median_days": c.median_days} for c in m.case_results],
        "case_unanswered": int(m.case_unanswered),
        "notes": list(m.notes),
    }


async def metrics_page(request: Request):
    try:
        m = await M.collect()
        notice = ""
    except Exception:
        logger.warning("[JIKOU_METRICS] page read failed (fixed reason)")
        m, notice = None, _NOTICE_DB
    return HTMLResponse(_page(m, notice))


async def api_metrics(request: Request):
    try:
        m = await M.collect()
    except Exception:
        return {"ok": False, "reason": "db"}
    return _json(m)


router.add_api_route(PAGE, _gate(metrics_page), methods=["GET"])
router.add_api_route(API, _gate(api_metrics), methods=["GET"])

REPLY_RECORD_PATH = "/app/reply/{record_id}"      # webapp_reply_view（Q1b）の動的 route。固定 path はこの前に置く


def _paths_of(entry) -> list:
    """app.router.routes の要素（_IncludedRouter / Route / APIRoute）の path 一覧（webapp_brain_view と同じ判定）。"""
    try:
        return [r.path for r in entry.original_router.routes]
    except AttributeError:
        pass
    try:
        return [entry.path]
    except AttributeError:
        return []


def wire(app) -> None:
    """main.py 末尾から呼ぶ: 本 router を結線し、/app/reply/{record_id}（動的・webapp_reply_view）を含む
    エントリより前へ並べ替える（FastAPI は登録順にマッチ＝後置きのままだと "metrics" が record_id として
    400 になる）。include_router が 1 エントリ（_IncludedRouter）でも個別 route でも同じに扱う。"""
    routes = app.router.routes
    before = len(routes)
    app.include_router(router)
    added = len(routes) - before
    mine = routes[-added:] if added > 0 else []
    if added > 0:
        del routes[-added:]
    idx = next((i for i, entry in enumerate(routes) if REPLY_RECORD_PATH in _paths_of(entry)), len(routes))
    for k, entry in enumerate(mine):
        routes.insert(idx + k, entry)


def wire_all(app) -> None:
    """main.py 末尾の 1 行: 画面の結線（wire）＋週次要約ジョブの startup 登録（HOUKI 系と同方式）。"""
    wire(app)
    app.router.on_startup.append(M.register_jikou_metrics_job)
