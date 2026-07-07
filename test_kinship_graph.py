"""kinship_graph.py（Z1 相続関係図のグラフ構造体）のテスト

検証:
熊澤5名の実データ形フィクスチャでの構造体化と生成拒否の列挙（名寄せ未確定・
確認状態・生死不明・被相続人不在＝どの人物のどの項目かが分かる形）・
定型5ケース（核家族・再婚・養子・代襲・3世代）の構造体テーブルテスト・
夫婦照合の分岐（相手方不在・同姓同名は保留・相互記載の1本化）・
親IDの参照切れ警告・案件ローダの読み取り専用性。kintone はモック。
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from kinship_graph import (  # noqa: E402
    Edge,
    build_graph,
    load_graph_for_case,
    validate_for_rendering,
)


def run(coro):
    return asyncio.run(coro)


def person(rid, name, *, gender="", zokugara="", alive="", meyose="未確定",
           kakunin="未確認", decedent="no", death_date="", shikaku="未判定",
           father="", mother="", adoptive_father="", adoptive_mother="",
           events=()):
    """App 34 レコード形（実機の kintone record 形・R4-1 が書く形と同一）"""
    rows = [{"value": {"事項種別": {"value": e[0]}, "年月日": {"value": e[1]},
                       "相手方": {"value": e[2] if len(e) > 2 else ""}}}
            for e in events]
    return {"$id": {"value": str(rid)},
            "氏名": {"value": name},
            "性別": {"value": gender},
            "続柄メモ": {"value": zokugara},
            "生死区分": {"value": alive},
            "死亡日": {"value": death_date},
            "被相続人フラグ": {"value": decedent},
            "相続資格": {"value": shikaku},
            "名寄せ確定": {"value": meyose},
            "確認状態": {"value": kakunin},
            "父人物ID": {"value": father},
            "母人物ID": {"value": mother},
            "養父人物ID": {"value": adoptive_father},
            "養母人物ID": {"value": adoptive_mother},
            "身分事項": {"value": rows}}


def confirmed(**kw):
    """検証を通る既定値（名寄せ確定・確認済・生存）"""
    base = {"meyose": "確定", "kakunin": "確認済", "alive": "生存"}
    base.update(kw)
    return base


def kumazawa_five():
    """熊澤5名の実データ形（R4-1 実機起票と同じ値）"""
    return [
        person(1, "熊澤 秀和", gender="男", zokugara="三男",
               events=[("出生", "昭和47年11月8日"),
                       ("婚姻", "平成10年12月25日", "山嵜知子")]),
        person(2, "熊澤 知子", gender="女", zokugara="二女",
               events=[("出生", "昭和49年5月28日"),
                       ("婚姻", "平成10年12月25日", "熊澤秀和")]),
        person(3, "熊澤 風香", gender="女", zokugara="長女", father="1", mother="2",
               events=[("出生", "平成11年6月11日")]),
        person(4, "熊澤 舞", gender="女", zokugara="二女", father="1", mother="2",
               events=[("出生", "平成17年10月2日")]),
        person(5, "熊澤 美咲", gender="女", zokugara="三女", father="1", mother="2",
               events=[("出生", "平成23年2月14日")]),
    ]


class TestKumazawaFixture(unittest.TestCase):
    def setUp(self):
        self.graph = build_graph(kumazawa_five())

    def test_structure(self):
        self.assertEqual(len(self.graph.nodes), 5)
        marriages = [e for e in self.graph.edges if e.kind == "婚姻"]
        self.assertEqual(marriages, [Edge("婚姻", "1", "2")],
                         "相互記載でも夫婦エッジは1本（氏名照合・空白正規化）")
        children = sorted((e.a, e.b) for e in self.graph.edges if e.kind == "親子")
        self.assertEqual(children, [("1", "3"), ("1", "4"), ("1", "5"),
                                    ("2", "3"), ("2", "4"), ("2", "5")])
        # 秀和側の相手方は旧姓「山嵜知子」で照合不能だが、知子側の相互記載
        # （婚姻後の氏）で連結済みのため警告は抑止される（2パス方式）
        self.assertEqual(self.graph.warnings, [])
        node = self.graph.node("1")
        self.assertEqual(node.birth_wareki, "昭和47年11月8日")
        self.assertEqual(node.gender, "男")

    def test_rejection_enumerates_person_and_item(self):
        """実データ（未確定・未確認・生死不明・被相続人不在）の拒否列挙"""
        problems = validate_for_rendering(self.graph)
        self.assertIn("被相続人が特定されていません"
                      "（被相続人フラグ=yes の人物がいません）", problems)
        # 5名 × 3項目（名寄せ・確認状態・生死）＋被相続人不在 = 16件
        self.assertEqual(len(problems), 16)
        self.assertTrue(any(p.startswith("No.1 熊澤 秀和: 名寄せ確定が「未確定」")
                            for p in problems))
        self.assertTrue(any("No.3 熊澤 風香: 確認状態が「未確認」" in p
                            for p in problems))
        self.assertTrue(any("No.5 熊澤 美咲: 生死区分が不明" in p
                            for p in problems))

    def test_decedent_without_death_date_is_rejected(self):
        records = kumazawa_five()
        records[0] = person(1, "熊澤 秀和", decedent="yes", death_date="",
                            **confirmed(alive="死亡"))
        problems = validate_for_rendering(build_graph(records))
        self.assertTrue(any("No.1 熊澤 秀和: 被相続人の死亡日（DATE）が未確定"
                            in p for p in problems))


class TestTypicalCases(unittest.TestCase):
    """定型5ケースの構造体テーブルテスト"""

    def test_case1_nuclear_family(self):
        records = [
            person(1, "山田太郎", **confirmed(),
                   events=[("婚姻", "S50", "山田花子")]),
            person(2, "山田花子", **confirmed()),
            person(3, "山田一郎", father="1", mother="2", **confirmed()),
            person(4, "山田二郎", father="1", mother="2", **confirmed()),
        ]
        g = build_graph(records)
        self.assertEqual(len([e for e in g.edges if e.kind == "婚姻"]), 1)
        self.assertEqual(len([e for e in g.edges if e.kind == "親子"]), 4)

    def test_case2_remarriage(self):
        """再婚: 婚姻エッジ2本（前妻・後妻）"""
        records = [
            person(1, "山田太郎", **confirmed(),
                   events=[("婚姻", "S50", "佐藤良子"), ("離婚", "S60"),
                           ("婚姻", "H2", "鈴木春子")]),
            person(2, "佐藤良子", **confirmed()),
            person(3, "鈴木春子", **confirmed()),
        ]
        g = build_graph(records)
        marriages = sorted((e.a, e.b) for e in g.edges if e.kind == "婚姻")
        self.assertEqual(marriages, [("1", "2"), ("1", "3")])

    def test_case3_adoption(self):
        """養子: 養親子エッジは kind で実親子と区別される"""
        records = [
            person(1, "山田太郎", **confirmed()),
            person(2, "山田花子", **confirmed()),
            person(3, "山田養男", adoptive_father="1", adoptive_mother="2",
                   father="9", **confirmed()),
        ]
        g = build_graph(records)
        adoptive = sorted((e.a, e.b) for e in g.edges if e.kind == "養親子")
        self.assertEqual(adoptive, [("1", "3"), ("2", "3")])
        # 実父 No.9 はレコード不在 → エッジ未作成＋警告（参照切れの可視化）
        self.assertEqual([e for e in g.edges if e.kind == "親子"], [])
        self.assertTrue(any("父人物ID=9 の人物レコードが見つかりません" in w
                            for w in g.warnings))

    def test_case4_substitution(self):
        """代襲: 先死した子＋代襲候補の孫（属性で表現）"""
        records = [
            person(1, "山田太郎", decedent="yes", death_date="2026-01-15",
                   **confirmed(alive="死亡"),
                   events=[("死亡", "令和8年1月15日")]),
            person(2, "山田一郎", father="1", **confirmed(alive="死亡"),
                   events=[("死亡", "令和5年3月1日")]),
            person(3, "山田孫子", father="2", shikaku="代襲相続人", **confirmed()),
        ]
        g = build_graph(records)
        self.assertTrue(g.node("1").is_decedent)
        self.assertEqual(g.node("1").death_wareki, "令和8年1月15日")
        self.assertEqual(g.node("2").alive, "死亡")
        self.assertTrue(g.node("3").daishu_candidate)
        self.assertEqual(validate_for_rendering(g), [], "確定済データは生成可")

    def test_case5_three_generations(self):
        records = [
            person(1, "山田祖父", **confirmed(alive="死亡")),
            person(2, "山田父", father="1", **confirmed()),
            person(3, "山田孫", father="2", **confirmed()),
        ]
        g = build_graph(records)
        chain = sorted((e.a, e.b) for e in g.edges if e.kind == "親子")
        self.assertEqual(chain, [("1", "2"), ("2", "3")])


class TestMarriageMatching(unittest.TestCase):
    def test_partner_not_found_is_warning_without_edge(self):
        records = [person(1, "山田太郎", **confirmed(),
                          events=[("婚姻", "S50", "行方不明子")])]
        g = build_graph(records)
        self.assertEqual(g.edges, [])
        self.assertTrue(any("「行方不明子」に一致する人物がいません" in w
                            for w in g.warnings))

    def test_duplicate_name_partners_are_held(self):
        """同姓同名の候補複数 → エッジを張らず保留（警告に候補を列挙）"""
        records = [
            person(1, "山田太郎", **confirmed(),
                   events=[("婚姻", "S50", "山田花子")]),
            person(2, "山田花子", **confirmed()),
            person(3, "山田 花子", **confirmed()),  # 空白差＝正規化後同名
        ]
        g = build_graph(records)
        self.assertEqual([e for e in g.edges if e.kind == "婚姻"], [])
        warning = next(w for w in g.warnings if "候補が複数" in w)
        self.assertIn("No.2", warning)
        self.assertIn("No.3", warning)
        self.assertIn("保留", warning)

    def test_unresolved_second_marriage_still_warns(self):
        """再婚の片方だけ照合不能なら警告は残る（連結済み抑止は行数を満たす場合のみ）"""
        records = [
            person(1, "山田太郎", **confirmed(),
                   events=[("婚姻", "S50", "山田花子"),
                           ("婚姻", "H2", "旧姓不明子")]),
            person(2, "山田花子", **confirmed(),
                   events=[("婚姻", "S50", "山田太郎")]),
        ]
        g = build_graph(records)
        self.assertEqual(len([e for e in g.edges if e.kind == "婚姻"]), 1)
        self.assertTrue(any("「旧姓不明子」に一致する人物がいません" in w
                            for w in g.warnings))

    def test_empty_partner_is_ignored(self):
        records = [person(1, "山田太郎", **confirmed(),
                          events=[("婚姻", "S50")])]
        g = build_graph(records)
        self.assertEqual(g.edges, [])
        self.assertEqual(g.warnings, [])


class TestLoader(unittest.TestCase):
    def test_load_is_read_only_and_scoped_to_case(self):
        search = AsyncMock(return_value=kumazawa_five())
        with patch("hub.kintone.search_records", new=search), \
                patch.dict(os.environ, {"APP_KOSEKI_PERSON": "34",
                                        "TOKEN_KOSEKI_PERSON": "t34"}):
            g = run(load_graph_for_case("3"))
        self.assertEqual(len(g.nodes), 5)
        _, query = search.await_args.args[0], search.await_args.args[1]
        self.assertIn('案件レコードID = "3"', query)


if __name__ == "__main__":
    unittest.main()
