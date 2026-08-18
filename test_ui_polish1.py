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

import re
import unittest
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


class TestDateFormatting(unittest.TestCase):
    def test_fmtdate_present_with_today_and_slash_format(self):
        for name in ("cases.html", "case.html", "approvals.html",
                     "souzoku.html", "souzoku_case.html"):
            src = _src(name)
            with self.subTest(page=name):
                self.assertIn("function fmtDate", src)
                self.assertIn('"今日 " + hm', src)
                self.assertIn('getFullYear() + "/" + (d.getMonth() + 1)', src)


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
