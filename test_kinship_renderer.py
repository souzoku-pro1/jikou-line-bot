"""kinship_renderer.py（Z2 関係図レンダラ・graphviz）のテスト

検証:
定型5ケースの dot スナップショット（核家族=全文一致・他=描画規則の行固定）・
拒否時の非描画（validate 非空→dot 不実行）・dot 特殊文字のエスケープ・
graphviz バイナリ不在時の縮退・render 成功経路（-Tsvg/-Tpdf）・
案件添付関数（App 26・FILE フィールド指定）・/health の graphviz 表示。
graphviz 実行・kintone は全てモック（バイナリ実行なしで完結）。
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

_DUMMY_ANTHROPIC_KEY = "dummy_key_for_import_only"
os.environ.setdefault("ANTHROPIC_API_KEY", _DUMMY_ANTHROPIC_KEY)
os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "KINTONE_WEBHOOK_TOKEN": "approve_token",
    "DOCUMENT_WEBHOOK_SECRET": "doc_secret",
    "APP_APPROVAL": "29",
    "TOKEN_APPROVAL": "dummy",
    "GOOGLE_VISION_API_KEY": "dummy_vision",
    "HEALTHCHECK_DISABLED": "1",
})

import main  # noqa: E402
from kinship_graph import Edge, KinshipGraph, PersonNode  # noqa: E402
from kinship_renderer import (  # noqa: E402
    GraphvizUnavailable,
    KinshipValidationRejected,
    attach_kinship_to_case,
    render_kinship,
    to_dot,
)

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]


def run(coro):
    return asyncio.run(coro)


def node(rid, name, **kw):
    base = dict(record_id=str(rid), name=name, meyose="確定", kakunin="確認済",
                alive="生存")
    base.update(kw)
    return PersonNode(**base)


def nuclear_family() -> KinshipGraph:
    return KinshipGraph(
        nodes=[node(1, "山田太郎", gender="男", birth_wareki="昭和10年1月2日"),
               node(2, "山田花子", gender="女"),
               node(3, "山田一郎"), node(4, "山田二郎")],
        edges=[Edge("婚姻", "1", "2"),
               Edge("親子", "1", "3"), Edge("親子", "2", "3"),
               Edge("親子", "1", "4"), Edge("親子", "2", "4")])


class TestDotSnapshots(unittest.TestCase):
    maxDiff = None

    def test_case1_nuclear_family_full_snapshot(self):
        """核家族: dot 全文スナップショット（夫婦=不可視点ノード・子は夫婦間から）"""
        expected = (
            'digraph kinship {\n'
            '  rankdir=TB;\n'
            '  node [shape=box fontname="IPAexGothic"];\n'
            '  edge [fontname="IPAexGothic"];\n'
            '  "p1" [label="山田太郎\\n昭和10年1月2日生"];\n'
            '  "p2" [label="山田花子"];\n'
            '  "p3" [label="山田一郎"];\n'
            '  "p4" [label="山田二郎"];\n'
            '  "m1_2" [shape=point width=0.02 label=""];\n'
            '  { rank=same; "p1"; "m1_2"; "p2"; }\n'
            '  "p1" -> "m1_2" [dir=none];\n'
            '  "m1_2" -> "p2" [dir=none];\n'
            '  "m1_2" -> "p3";\n'
            '  "m1_2" -> "p4";\n'
            '}\n'
        )
        self.assertEqual(to_dot(nuclear_family()), expected)

    def test_case2_remarriage_two_couples(self):
        g = KinshipGraph(
            nodes=[node(1, "山田太郎"), node(2, "佐藤良子"), node(3, "鈴木春子")],
            edges=[Edge("婚姻", "1", "2"), Edge("婚姻", "1", "3")])
        dot = to_dot(g)
        self.assertIn('"m1_2" [shape=point', dot)
        self.assertIn('"m1_3" [shape=point', dot)

    def test_case3_adoption_dashed(self):
        g = KinshipGraph(
            nodes=[node(1, "山田太郎"), node(3, "山田養男")],
            edges=[Edge("養親子", "1", "3")])
        dot = to_dot(g)
        self.assertIn('"p1" -> "p3" [style=dashed];', dot)

    def test_case4_substitution_attrs(self):
        """代襲: 死亡=グレー＋没年・被相続人=二重枠・代襲=注記"""
        g = KinshipGraph(
            nodes=[node(1, "山田太郎", is_decedent=True, alive="死亡",
                        death_wareki="令和8年1月15日", death_date="2026-01-15"),
                   node(2, "山田一郎", alive="死亡"),
                   node(3, "山田孫子", daishu_candidate=True)],
            edges=[Edge("親子", "1", "2"), Edge("親子", "2", "3")])
        dot = to_dot(g)
        self.assertIn('"p1" [label="山田太郎\\n（令和8年1月15日没）" '
                      'peripheries=2 style=filled fillcolor=gray80];', dot)
        self.assertIn('"p2" [label="山田一郎\\n（死亡）" '
                      'style=filled fillcolor=gray80];', dot)
        self.assertIn('"p3" [label="山田孫子\\n（代襲）"];', dot)

    def test_case5_three_generations_direct_edges(self):
        """3世代: 夫婦不在の親は直接エッジ（片親から子へ）"""
        g = KinshipGraph(
            nodes=[node(1, "山田祖父"), node(2, "山田父"), node(3, "山田孫")],
            edges=[Edge("親子", "1", "2"), Edge("親子", "2", "3")])
        dot = to_dot(g)
        self.assertIn('"p1" -> "p2";', dot)
        self.assertIn('"p2" -> "p3";', dot)
        self.assertNotIn("rank=same", dot, "夫婦がいなければ不可視点ノードなし")

    def test_single_parent_couple_not_used(self):
        """両親のうち夫婦関係が無い組は親から個別に垂線"""
        g = KinshipGraph(
            nodes=[node(1, "山田太郎"), node(2, "内縁花子"), node(3, "山田一郎")],
            edges=[Edge("親子", "1", "3"), Edge("親子", "2", "3")])
        dot = to_dot(g)
        self.assertIn('"p1" -> "p3";', dot)
        self.assertIn('"p2" -> "p3";', dot)


class TestEscaping(unittest.TestCase):
    def test_quotes_and_backslash_in_name(self):
        g = KinshipGraph(nodes=[node(1, '山田"太郎\\')])
        dot = to_dot(g)
        self.assertIn('label="山田\\"太郎\\\\"', dot)


class TestRenderGuards(unittest.TestCase):
    def test_rejection_does_not_render(self):
        """検証非空 → 列挙付きで拒否・dot 実行にも到達しない"""
        g = KinshipGraph(nodes=[node(1, "山田太郎", meyose="未確定")])
        boom = MagicMock(side_effect=AssertionError("dot が実行された"))
        with patch("kinship_renderer.subprocess.run", new=boom):
            with self.assertRaises(KinshipValidationRejected) as ctx:
                render_kinship(g)
        problems = ctx.exception.problems
        self.assertTrue(any("No.1 山田太郎: 名寄せ確定が「未確定」" in p
                            for p in problems))
        self.assertIn("被相続人が特定されていません"
                      "（被相続人フラグ=yes の人物がいません）", problems)
        boom.assert_not_called()

    def _valid_graph(self):
        return KinshipGraph(nodes=[node(1, "山田太郎", is_decedent=True,
                                        alive="死亡", death_date="2026-01-15")])

    def test_graphviz_missing_is_explicit_degradation(self):
        with patch("kinship_renderer.shutil.which", new=MagicMock(return_value=None)):
            with self.assertRaises(GraphvizUnavailable) as ctx:
                render_kinship(self._valid_graph())
        self.assertIn("他機能は正常", str(ctx.exception))

    def test_render_success_invokes_dot_with_format(self):
        proc = MagicMock(returncode=0, stdout=b"<svg/>", stderr=b"")
        runner = MagicMock(return_value=proc)
        with patch("kinship_renderer.shutil.which",
                   new=MagicMock(return_value="/usr/bin/dot")), \
                patch("kinship_renderer.subprocess.run", new=runner):
            out = render_kinship(self._valid_graph(), fmt="svg")
        self.assertEqual(out, b"<svg/>")
        self.assertEqual(runner.call_args.args[0], ["/usr/bin/dot", "-Tsvg"])
        self.assertIn(b'"p1"', runner.call_args.kwargs["input"])


class TestAttach(unittest.TestCase):
    def test_attach_uploads_svg_and_pdf_to_field(self):
        upload = AsyncMock(side_effect=["fk-svg", "fk-pdf"])
        update = AsyncMock()
        proc = MagicMock(returncode=0, stdout=b"bin", stderr=b"")
        with patch("kinship_renderer.shutil.which",
                   new=MagicMock(return_value="/usr/bin/dot")), \
                patch("kinship_renderer.subprocess.run",
                      new=MagicMock(return_value=proc)), \
                patch("hub.kintone.upload_file", new=upload), \
                patch("hub.kintone.update_record", new=update):
            g = KinshipGraph(nodes=[node(1, "山田太郎", is_decedent=True,
                                         alive="死亡", death_date="2026-01-15")])
            result = run(attach_kinship_to_case("3", g))
        self.assertEqual(result["status"], "attached")
        names = [c.args[1] for c in upload.await_args_list]
        self.assertEqual(names, ["相続関係図.svg", "相続関係図.pdf"])
        app, rid, fields = update.await_args.args
        self.assertEqual(rid, "3")
        self.assertEqual(fields["関係図"],
                         [{"fileKey": "fk-svg"}, {"fileKey": "fk-pdf"}])


class TestHealthGraphviz(unittest.TestCase):
    def test_health_reports_graphviz_presence(self):
        with patch("shutil.which", new=MagicMock(return_value="/usr/bin/dot")):
            deps = run(main.health())["deps"]
        self.assertTrue(deps["graphviz"].startswith("ok ("))

    def test_health_reports_graphviz_absence_without_failing(self):
        with patch("shutil.which", new=MagicMock(return_value=None)):
            body = run(main.health())
        self.assertEqual(body["status"], "ok", "graphviz 不在でも health 自体は ok")
        self.assertIn("関係図の描画のみ不可", body["deps"]["graphviz"])


if __name__ == "__main__":
    unittest.main()
