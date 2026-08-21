"""UI-POLISH-1: PWA 全画面のデザイン・文言改修の pin（表示層のみの変更）。

固定する仕様:
- 内部用語の全排除: P4-xxx・scan・「承認キュー」・「offset」表示等が画面に出ない
- 日時: fmtDate（日本時間 2026/7/7 15:30 形式・当日は「今日 15:30」）を各画面が持つ
- カード型: 一覧・詳細系ページから <table> を全廃（kinship は現状維持で対象外）
- 空欄: 「未登録」の淡色表示ヘルパを各画面が持つ
- ホーム: サマリー（相続/時効/承認待ち件数）＋主要導線（既存 href pin は
  既存テストが担保）
- 挙動不変: fetch 規律・DOM 構築規律・form 不在等は既存 pin が無修正で担保
"""

import os
import re
import unittest
from datetime import datetime, timezone
from pathlib import Path

_WEBAPP = Path("webapp")
_COMMENT_RE = re.compile(r"<!--.*?-->", re.S)

# 改修対象ページ（kinship・q・login は本票の対象外）
_PAGES = ("index.html", "cases.html", "case.html", "approvals.html",
          "souzoku.html", "souzoku_case.html")


def _src(name):
    return (_WEBAPP / name).read_text(encoding="utf-8")


class TestNoInternalJargon(unittest.TestCase):
    def test_no_internal_codes_or_raw_terms_on_screen(self):
        # HTML コメント（画面に表示されない設計注記・票番号参照）は走査対象外
        for name in _PAGES:
            src = _COMMENT_RE.sub("", _src(name))
            for banned in ("P4-0", "scan 到着", "承認キュー",
                           "送信済み: ", "offset ${offset}",
                           "（App33・", "DerivationRun"):
                with self.subTest(page=name, banned=banned):
                    self.assertNotIn(banned, src)

    def test_no_raw_confirmation_values_in_souzoku_case(self):
        src = _src("souzoku_case.html")
        # 「yes（確定）」「no【未確定】」の生値表示はバッジへ置換済み
        for banned in ("yes（確定）", "no【未確定】", "yes" + "（"):
            self.assertNotIn(banned, src)
        self.assertIn("confirmBadge(", src)


_DATE_PAGES = ("cases.html", "case.html", "approvals.html",
               "souzoku.html", "souzoku_case.html")


def _fmtdate_block(name):
    src = _src(name)
    start = src.index("// UI-POLISH-1-fix1（UI-POLISH-02）")
    body = src[start:]
    end = body.index("\n}", body.index("function fmtDate")) + 2
    return body[:end]


class TestDateFormatting(unittest.TestCase):
    """UI-POLISH-1-fix1（UI-POLISH-02）: JST（UTC+9 固定＝Asia/Tokyo と恒等・
    DST なし）を単一の正とする。JS 実行環境（node）がローカルに無いため
    （maint3 fix2 L01 と同じ確立制約）、挙動検査は「全画面の逐語同一 pin＋
    非 UTC getter 不在の構造 pin＋同一アルゴリズムの Python port 実行」で行う。
    実行水準の最終確認は実機スモーク（[人]）。"""

    def test_single_verbatim_implementation_across_pages(self):
        # 単一の正: 5 画面の fmtDate/jstParts ブロックが byte 同一
        canonical = _fmtdate_block("cases.html")
        self.assertIn("function jstParts", canonical)
        self.assertIn("9 * 3600 * 1000", canonical)     # JST=UTC+9 固定
        self.assertIn('"今日 " + p.hm', canonical)
        for name in _DATE_PAGES:
            with self.subTest(page=name):
                self.assertEqual(_fmtdate_block(name), canonical)

    def test_no_local_timezone_getters(self):
        # 端末タイムゾーン非依存の構造 pin: 非 UTC getter を一切使わない
        # （使うのは getTime と getUTC* のみ）
        canonical = _fmtdate_block("cases.html")
        self.assertNotRegex(
            canonical,
            r"\.get(FullYear|Month|Date|Day|Hours|Minutes|Seconds)\(")
        self.assertIn(".getUTCFullYear()", canonical)
        self.assertNotIn("toLocaleString", canonical)

    # ── Python port（JS と同一アルゴリズム・上の逐語 pin が同一性を担保） ──
    @staticmethod
    def _jst(epoch_ms):
        from datetime import datetime, timedelta, timezone
        t = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc) \
            + timedelta(hours=9)
        return t.year, t.month, t.day, f"{t.hour:02d}:{t.minute:02d}"

    @classmethod
    def _fmt(cls, value, now_epoch_ms):
        from datetime import datetime
        s = str(value or "")
        if not s:
            return ""
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
        if m:
            def strip0(x):
                return x[1:] if x.startswith("0") else x
            return f"{m.group(1)}/{strip0(m.group(2))}/{strip0(m.group(3))}"
        try:
            d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return s
        epoch = d.timestamp() * 1000
        y, mo, da, hm = cls._jst(epoch)
        ny, nmo, nda, _ = cls._jst(now_epoch_ms)
        if (y, mo, da) == (ny, nmo, nda):
            return f"今日 {hm}"
        return f"{y}/{mo}/{da} {hm}"

    # 2026-08-18T15:00:00Z = JST 2026-08-19 00:00（日付境界ちょうど）
    _NOW = int(datetime(2026, 8, 18, 15, 0,
                        tzinfo=timezone.utc).timestamp() * 1000)

    def test_utc_boundary_rolls_jst_date(self):
        # UTC 15:00 で JST の日付が跨ぐ（UTC→JST 変換の境界固定）
        self.assertEqual(self._fmt("2026-08-18T14:59:00Z", self._NOW),
                         "2026/8/18 23:59")
        self.assertEqual(self._fmt("2026-08-18T15:00:00Z", self._NOW),
                         "今日 00:00")

    def test_today_is_judged_on_jst_calendar_day(self):
        # 「今日」判定は JST 日付境界（now=JST 8/19 00:00 に対し JST 同日のみ今日）
        self.assertEqual(self._fmt("2026-08-19T00:30:00+09:00", self._NOW),
                         "今日 00:30")
        self.assertEqual(self._fmt("2026-08-19T15:00:00Z", self._NOW),
                         "2026/8/20 00:00")     # JST では翌日
        self.assertEqual(self._fmt("2026-07-07T06:30:00Z", self._NOW),
                         "2026/7/7 15:30")      # 票の例示形式

    def test_offset_input_and_degenerate_values(self):
        self.assertEqual(self._fmt("2026-07-07", self._NOW), "2026/7/7")
        self.assertEqual(self._fmt("", self._NOW), "")
        self.assertEqual(self._fmt("不正な値", self._NOW), "不正な値")

    def test_port_is_timezone_independent(self):
        # 端末（プロセス）タイムゾーンを変えても出力不変（UTC 演算のみの実証）
        import time as _time
        results = []
        for tz in ("UTC", "America/New_York", "Asia/Tokyo"):
            old = os.environ.get("TZ")
            os.environ["TZ"] = tz
            if hasattr(_time, "tzset"):
                _time.tzset()          # Windows では no-op 相当（属性なし）
            try:
                results.append(self._fmt("2026-08-18T15:00:00Z", self._NOW))
            finally:
                if old is None:
                    os.environ.pop("TZ", None)
                else:
                    os.environ["TZ"] = old
                if hasattr(_time, "tzset"):
                    _time.tzset()
        self.assertEqual(set(results), {"今日 00:00"})


class TestHomeCountFailClosed(unittest.TestCase):
    """UI-POLISH-1-fix1（UI-POLISH-01）: HTTP 4xx/5xx の JSON 応答が「0件」に
    化けない——!resp.ok は「−」へ縮退。判定は純関数 countText（同一
    アルゴリズムの Python port で挙動検査・JS 実行環境なしの確立制約は
    TestDateFormatting と同じ）。"""

    @staticmethod
    def _count_text(ok, records, cap):
        if not ok or not isinstance(records, list):
            return "−"
        n = len(records)
        return f"{cap}+" if n >= cap else str(n)

    def test_http_error_shows_dash_not_zero_for_each_api(self):
        # 3 API それぞれ: HTTP 500（ok=False）の JSON 応答でも「0」ではなく「−」
        for cap in (50, 20, 20):
            with self.subTest(cap=cap):
                self.assertEqual(self._count_text(False, [], cap), "−")
                self.assertNotEqual(self._count_text(False, [], cap), "0")
                self.assertEqual(self._count_text(False, None, cap), "−")

    def test_ok_counts_and_cap(self):
        self.assertEqual(self._count_text(True, [], 20), "0")
        self.assertEqual(self._count_text(True, [1, 2, 3], 20), "3")
        self.assertEqual(self._count_text(True, [0] * 20, 20), "20+")
        self.assertEqual(self._count_text(True, None, 20), "−")  # 形不正も縮退

    def test_page_source_guards_and_isolation(self):
        src = _src("index.html")
        # !resp.ok ガード（ok のときだけ json 化・countText へ ok を渡す）
        self.assertIn("const data = resp.ok ? await resp.json() : null;", src)
        self.assertIn("countText(resp.ok, data && data.records, cap)", src)
        self.assertIn("function countText(ok, records, cap)", src)
        self.assertIn('if (!ok || !Array.isArray(records)) { return "−"; }',
                      src)
        # 1 API の失敗が他を妨げない: 3 呼出しが独立（各 fillCount が自前の
        # try/catch で「−」へ縮退）
        self.assertEqual(src.count("fillCount(app_fetch("), 3)
        self.assertIn('node.textContent = "−";', src)


class TestCardLayout(unittest.TestCase):
    def test_tables_removed_from_list_and_detail_pages(self):
        for name in ("index.html", "cases.html", "case.html",
                     "approvals.html", "souzoku.html", "souzoku_case.html"):
            with self.subTest(page=name):
                self.assertNotIn("<table", _src(name))

    def test_badges_present(self):
        # バッジは CSS（.badge）＋DOM 構築（el("span", "badge…")）の組で存在
        for name in ("cases.html", "approvals.html", "souzoku.html",
                     "souzoku_case.html"):
            src = _src(name)
            with self.subTest(page=name):
                self.assertIn(".badge{", src)
                self.assertIn('"badge', src)

    def test_cards_link_whole_area(self):
        # 一覧はカード全体タップで詳細へ（ID リンクの廃止）
        for name, href in (("cases.html", '"/app/case?id="'),
                           ("souzoku.html", '"/app/souzoku/case?id="')):
            src = _src(name)
            with self.subTest(page=name):
                self.assertIn('el("a", "case-card")', src)
                self.assertIn(href, src)


class TestEmptyValues(unittest.TestCase):
    def test_empty_fields_show_mitoroku(self):
        for name in ("cases.html", "case.html", "souzoku.html",
                     "souzoku_case.html"):
            with self.subTest(page=name):
                self.assertIn('"未登録"', _src(name))


class TestHomeDashboard(unittest.TestCase):
    def test_summary_counts_and_big_nav(self):
        src = _src("index.html")
        self.assertIn('id="n-souzoku"', src)
        self.assertIn('id="n-jikou"', src)
        self.assertIn('id="n-approvals"', src)
        self.assertIn("返信の承認待ち", src)
        self.assertIn('class="navcard"', src)
        # app_fetch の第1引数は各呼出しリテラル固定（ラッパー規律の流儀）
        self.assertIn('app_fetch("/app/api/souzoku/cases?limit=50")', src)
        self.assertIn('app_fetch("/app/api/cases?limit=20")', src)
        self.assertIn('app_fetch("/app/api/approvals?limit=20")', src)
        # 「書類の到着確認」への改称（準備中カード）
        self.assertIn("書類の到着確認", src)
        self.assertIn("ダッシュボード", src)     # 既存 pin（p4_001）の維持


class TestShellNav(unittest.TestCase):
    def test_nav_labels_and_structure_kept(self):
        src = (_WEBAPP / "shell.js").read_text(encoding="utf-8")
        self.assertIn("/app/logout", src)
        self.assertIn('"/app/q"', src)
        self.assertIn("時効案件", src)           # 「案件一覧」→ 平易な名称へ
        self.assertNotIn("innerHTML", src)


if __name__ == "__main__":
    unittest.main()
