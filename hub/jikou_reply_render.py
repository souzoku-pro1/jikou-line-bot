"""jikou_reply_render — JIKOU-REPLY-Q2 (c): 描画器（定型 ID+差し込み値 → 本文・LLM なし）と J-5 の 2 算式

正本: 時効LINEボット_返信規則_v1.4.md §10-3（描画器）・§10-8（J-5 の計算契約）。QUEUE-2026-10-13-D D-1 (c)。

- 描画器は hub/jikou_reply_texts の定型 ID と差し込み値（SLOTS の閉集合）だけから本文を組む。差し込み値以外の
  自由生成は無い（文字列の連結のみ）。差し込み値は型検査（年月表示の形式・社名列挙の文字種）に落ちれば
  RenderError（fail-closed）。表の pin 不一致も RenderError。
- J-5 の 2 算式（司令塔裁定: 余裕 3 か月・安全 3 か月。表の月数を使う）:
    推奨（順算）= 入力の上限日 + 時効期間（5 年）の応当日 + 余裕月数 → 表示「YYYY年M月末」（上限日を含む月の月末
                  ＝月初からの実施を許す読みにならない）
    基準（逆算）= 評価日 − 時効期間（5 年）− 安全月数 → 表示「YYYY年M月」（その月より前＝安全側に丸める）
  応当日が無い月（2 月 29 日→平年）は月末に寄せる。月のみの申告は月末扱い・範囲の申告は最も遅い月（parse_last_payment）。
- RV-10: 例外・ログに相談者の申告値を載せない（RenderError の文言は固定分類のみ）。
"""

import calendar
import datetime
import re

from hub import jikou_reply_texts as T

SEPARATOR = "\n\n"
MONTH_END_SUFFIX = "末"

_RE_SLOT = re.compile(r"【([^【】]+)】")
_RE_YM_DISPLAY = re.compile(r"^\d{4}年(?:1[0-2]|[1-9])月(?:末)?$")
_RE_CREDITORS = re.compile(r"^[^\n【】{}<>]{1,120}$")
_ERA = {"令和": 2018, "平成": 1988, "昭和": 1925, "R": 2018, "H": 1988, "S": 1925}
_RE_DATE = re.compile(
    r"(?:(?P<era>令和|平成|昭和|[RHS])\s*(?P<ey>元|\d{1,2})|(?P<y>(?:19|20)\d{2}))\s*[年/.\-]\s*"
    r"(?P<m>1[0-2]|0?[1-9])(?![0-9])\s*(?:月|/|\.|-)?\s*(?:(?P<d>3[01]|[12]\d|0?[1-9])(?![0-9])\s*日?)?")
_RE_RANGE_TAIL = re.compile(r"(?:〜|~|～|から|ないし)\s*(1[0-2]|0?[1-9])\s*月(?!\s*\d)")


class RenderError(ValueError):
    """描画の失敗（固定分類のみ・相談者の値を含めない）。"""


def _month_end(y: int, m: int) -> datetime.date:
    return datetime.date(y, m, calendar.monthrange(y, m)[1])


def add_months(d: datetime.date, months: int) -> datetime.date:
    """月数の加減算（応当日が無ければ月末に寄せる）。"""
    total = d.year * 12 + (d.month - 1) + int(months)
    y, m = divmod(total, 12)
    m += 1
    return datetime.date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def add_years(d: datetime.date, years: int) -> datetime.date:
    return add_months(d, 12 * int(years))


def parse_last_payment(text: str | None) -> datetime.date | None:
    """申告の最終返済日（App 21 最終返済日_テキスト／J-H1 回答）→ 上限日。
    月のみ→月末・日あり→その日・複数（範囲）→最も遅い日。解釈できなければ None（J-5 不送信）。"""
    s = (text or "").strip()
    if not s:
        return None
    s = s.translate(str.maketrans("０１２３４５６７８９／．－", "0123456789/.-"))
    found: list[datetime.date] = []
    for m in _RE_DATE.finditer(s):
        if m.group("era"):
            ey = 1 if m.group("ey") == "元" else int(m.group("ey"))
            y = _ERA[m.group("era")] + ey
        else:
            y = int(m.group("y"))
        mo = int(m.group("m"))
        try:
            if m.group("d"):
                found.append(datetime.date(y, mo, int(m.group("d"))))
            else:
                found.append(_month_end(y, mo))
        except ValueError:
            return None
    if not found:
        return None
    # 範囲の申告（「2021年10月〜12月」）: 年を省いた後半の月は直前の年で補う → 最も遅い月
    last_year = found[-1].year
    for m in _RE_RANGE_TAIL.finditer(s):
        try:
            found.append(_month_end(last_year, int(m.group(1))))
        except ValueError:
            return None
    return max(found)


def j5_dates(last_payment_upper: datetime.date, eval_date: datetime.date, *,
             period_years: int = 5, margin_months: int = 3, safety_months: int = 3) -> dict:
    """J-5 の 2 算式。戻り値: recommended（date）・base（date）・表示 2 種。"""
    rec = add_months(add_years(last_payment_upper, period_years), margin_months)
    base = add_months(add_years(eval_date, -period_years), -safety_months)
    return {"recommended": rec, "base": base,
            "recommended_display": f"{rec.year}年{rec.month}月{MONTH_END_SUFFIX}",
            "base_display": f"{base.year}年{base.month}月"}


def _check_slot(name: str, value: str) -> str:
    v = str(value or "").strip()
    if name in (T.SLOT_BASE_MONTH, T.SLOT_RECOMMENDED_MONTH):
        if not _RE_YM_DISPLAY.fullmatch(v):
            raise RenderError("slot_type:month")
        return v
    if name == T.SLOT_CREDITORS:
        if not _RE_CREDITORS.fullmatch(v):
            raise RenderError("slot_type:creditors")
        return v
    raise RenderError("slot_unknown")


def render_one(text_id: str, slots: dict | None = None) -> str:
    """定型 1 件の描画。表に無い ID・pin 不一致・差し込み不足・型不一致は RenderError。"""
    if not T.verify_pin():
        raise RenderError("table_pin_mismatch")
    e = T.entry(text_id)
    if e is None:
        raise RenderError("unknown_id")
    slots = dict(slots or {})
    out = e["text"]
    # 【 】のうち差し込み名（T.SLOTS）だけを置換する（「【費用のご案内】」のような見出しは本文の一部）
    for name in _RE_SLOT.findall(out):
        if name not in T.SLOTS:
            continue
        if name not in e["slots"]:
            raise RenderError("slot_unknown")
        if name not in slots:
            raise RenderError("slot_missing")
        out = out.replace(f"【{name}】", _check_slot(name, slots[name]))
    if any(n in T.SLOTS for n in _RE_SLOT.findall(out)):
        raise RenderError("slot_unresolved")
    return out


def render(text_ids, slots: dict | None = None) -> str:
    """定型 ID 列の描画（順序どおり・空行で連結）。1 件でも落ちれば RenderError（全体を送らない）。"""
    ids = [str(i) for i in (text_ids or ())]
    if not ids:
        raise RenderError("empty")
    return SEPARATOR.join(render_one(i, slots) for i in ids)


def length_exempt_blocks(text_ids) -> tuple:
    """300 字ゲートの免除ブロック（J-7 の費用定型など length_exempt=True の本文）。"""
    return tuple(T.text(i) for i in (text_ids or ()) if (T.entry(i) or {}).get("length_exempt"))
