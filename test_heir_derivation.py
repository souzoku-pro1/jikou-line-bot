"""heir_derivation.py（R4-3 相続順位エンジン）のテスト

正解仕様: docs/koseki-ocr/09-heir-test-cases.md（確定版 v0.1・大野弁護士承認済み・
凍結）。テスト名にケース番号を含める（test_A1_... 形式）。表の実行数は47行
（A13+B8+C5+D5+E6+F6+G4）。相続分は fractions.Fraction（浮動小数不使用）。

追加検証: 被相続人0名/複数名の拒否・被相続人死亡日未確定の保留・
required_persons の絞り込み（G1で尊属兄弟が含まれない）・
validate_for_rendering の None 時現行互換＋絞り込み時の挙動。
"""

import os
import unittest
from fractions import Fraction as F
from unittest.mock import patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from heir_derivation import (  # noqa: E402
    Declarations, HeirPerson, LifeEvent, derive_heirs, persons_from_records,
    required_persons,
)
from kinship_graph import Edge, KinshipGraph, PersonNode, validate_for_rendering  # noqa: E402

D_DATE = "2025-04-13"   # 被相続人の死亡日（基準日）
BEFORE = "2020-01-01"   # 先死亡（死*）
AFTER = "2025-06-01"    # 後死亡（死†・数次）


def p(pid, **kw):
    kw.setdefault("name", pid)
    return HeirPerson(record_id=pid, **kw)


def dec(pid="被", **kw):
    return p(pid, alive="死亡", death_date=D_DATE, is_decedent=True, **kw)


def dead(pid, date=BEFORE, **kw):
    return p(pid, alive="死亡", death_date=date, **kw)


def marry(a, b, divorced=False):
    a.events.append(LifeEvent("婚姻", "平成11年7月19日", b.name))
    b.events.append(LifeEvent("婚姻", "平成11年7月19日", a.name))
    if divorced:
        a.events.append(LifeEvent("離婚", "令和3年8月31日", b.name))
        b.events.append(LifeEvent("離婚", "令和3年8月31日", a.name))


class _Base(unittest.TestCase):
    def derive(self, *persons, decl=None, kosekis=None):
        d = derive_heirs(list(persons), decl, kosekis=kosekis)
        self.assertEqual(d.status, "derived", d.hold_reasons)
        return d

    def held(self, *persons, decl=None, kosekis=None):
        d = derive_heirs(list(persons), decl, kosekis=kosekis)
        self.assertEqual(d.status, "held", getattr(d, "shares", None))
        return d

    def flag_ids(self, d):
        return [f["flag"] for f in d.flags]


class TestA_BasicRanks(_Base):
    def test_A1_spouse_and_two_children(self):
        P, S = dec("P"), p("S")
        marry(P, S)
        d = self.derive(P, S, p("C1", father_id="P"), p("C2", father_id="P"))
        self.assertEqual(d.shares, {"S": F(1, 2), "C1": F(1, 4), "C2": F(1, 4)})
        self.assertEqual(d.rank, 1)
        spouse = next(h for h in d.heirs if h.person_id == "S")
        self.assertIn("民法890条", spouse.basis)
        self.assertIn("民法900条1号", spouse.basis)

    def test_A2_spouse_only(self):
        P, S = dec("P"), p("S")
        marry(P, S)
        d = self.derive(P, S)
        self.assertEqual(d.shares, {"S": F(1)})

    def test_A3_children_only(self):
        d = self.derive(dec("P"), p("C1", father_id="P"), p("C2", father_id="P"))
        self.assertEqual(d.shares, {"C1": F(1, 2), "C2": F(1, 2)})

    def test_A4_spouse_and_parents(self):
        P, S = dec("P", father_id="F", mother_id="M"), p("S")
        marry(P, S)
        d = self.derive(P, S, p("F"), p("M"))
        self.assertEqual(d.shares, {"S": F(2, 3), "F": F(1, 6), "M": F(1, 6)})
        self.assertEqual(d.rank, 2)

    def test_A5_spouse_and_mother_only(self):
        P, S = dec("P", father_id="F", mother_id="M"), p("S")
        marry(P, S)
        d = self.derive(P, S, dead("F"), p("M"))
        self.assertEqual(d.shares, {"S": F(2, 3), "M": F(1, 3)})

    def test_A6_spouse_and_two_siblings(self):
        P, S = dec("P", father_id="F", mother_id="M"), p("S")
        marry(P, S)
        d = self.derive(P, S, dead("F"), dead("M"),
                        p("B1", father_id="F", mother_id="M"),
                        p("B2", father_id="F", mother_id="M"))
        self.assertEqual(d.shares, {"S": F(3, 4), "B1": F(1, 8), "B2": F(1, 8)})
        self.assertEqual(d.rank, 3)

    def test_A7_parents_only(self):
        d = self.derive(dec("P", father_id="F", mother_id="M"), p("F"), p("M"))
        self.assertEqual(d.shares, {"F": F(1, 2), "M": F(1, 2)})

    def test_A8_three_siblings_only(self):
        d = self.derive(dec("P", father_id="F", mother_id="M"),
                        dead("F"), dead("M"),
                        p("B1", father_id="F", mother_id="M"),
                        p("B2", father_id="F", mother_id="M"),
                        p("B3", father_id="F", mother_id="M"))
        self.assertEqual(d.shares,
                         {"B1": F(1, 3), "B2": F(1, 3), "B3": F(1, 3)})

    def test_A9_children_exclude_parents(self):
        P, S = dec("P", father_id="F", mother_id="M"), p("S")
        marry(P, S)
        d = self.derive(P, S, p("C", father_id="P"), p("F"), p("M"))
        self.assertEqual(d.shares, {"S": F(1, 2), "C": F(1, 2)})
        self.assertNotIn("F", d.shares, "887条1項が889条に優先")

    def test_A10_nearest_degree_ascendant_only(self):
        d = self.derive(dec("P", father_id="F", mother_id="M"),
                        dead("F", father_id="PGF"), p("M"), p("PGF"))
        self.assertEqual(d.shares, {"M": F(1)},
                         "親等の近い母が優先・祖父は入らない（889①一但書）")

    def test_A11_grandparent_generation(self):
        d = self.derive(dec("P", father_id="F", mother_id="M"),
                        dead("F", father_id="PGF"), dead("M", mother_id="MGM"),
                        dead("PGF"), p("MGM"))
        self.assertEqual(d.shares, {"MGM": F(1)},
                         "尊属内に代襲の概念はなく親等で決まる")

    def test_A12_divorced_spouse_excluded(self):
        P, S = dec("P"), p("S")
        marry(P, S, divorced=True)
        d = self.derive(P, S, p("C", father_id="P"))
        self.assertEqual(d.shares, {"C": F(1)}, "元配は相続人でない（890条）")

    def test_A13_predeceased_spouse_excluded(self):
        P, S = dec("P"), dead("S")
        marry(P, S)
        d = self.derive(P, S, p("C", father_id="P"))
        self.assertEqual(d.shares, {"C": F(1)}, "死別した配偶者は相続人でない")


class TestB_Substitution(_Base):
    def test_B1_grandchild_substitutes(self):
        P, S = dec("P"), p("S")
        marry(P, S)
        d = self.derive(P, S, p("A", father_id="P"), dead("B", father_id="P"),
                        p("b1", father_id="B"))
        self.assertEqual(d.shares, {"S": F(1, 2), "A": F(1, 4), "b1": F(1, 4)})
        b1 = next(h for h in d.heirs if h.person_id == "b1")
        self.assertEqual(b1.via, "B")
        self.assertIn("民法887条2項", b1.basis)

    def test_B2_re_substitution_great_grandchild(self):
        d = self.derive(dec("P"), dead("A", father_id="P"),
                        dead("a1", father_id="A"), p("a1x", father_id="a1"))
        self.assertEqual(d.shares, {"a1x": F(1)}, "再代襲（887条3項）")
        a1x = next(h for h in d.heirs if h.person_id == "a1x")
        self.assertIn("民法887条3項", a1x.basis)

    def test_B3_stirpes_split(self):
        d = self.derive(dec("P"), p("A", father_id="P"),
                        dead("B", father_id="P"),
                        p("b1", father_id="B"), p("b2", father_id="B"))
        self.assertEqual(d.shares,
                         {"A": F(1, 2), "b1": F(1, 4), "b2": F(1, 4)},
                         "株分け（901条）: 代襲者はBの分1/2を等分")

    def test_B4_nephew_substitutes_sibling(self):
        d = self.derive(dec("P", father_id="F", mother_id="M"),
                        dead("F"), dead("M"),
                        p("A", father_id="F", mother_id="M"),
                        dead("B", father_id="F", mother_id="M"),
                        p("b1", father_id="B"))
        self.assertEqual(d.shares, {"A": F(1, 2), "b1": F(1, 2)})
        b1 = next(h for h in d.heirs if h.person_id == "b1")
        self.assertEqual(b1.zokugara, "甥姪（代襲）")
        self.assertIn("民法889条2項", b1.basis)

    def test_B5_no_re_substitution_for_siblings(self):
        d = self.derive(dec("P", father_id="F", mother_id="M"),
                        dead("F"), dead("M"),
                        p("A", father_id="F", mother_id="M"),
                        dead("B", father_id="F", mother_id="M"),
                        dead("b1", father_id="B"), p("b1x", father_id="b1"))
        self.assertEqual(d.shares, {"A": F(1)},
                         "兄弟系の再代襲なし（889条2項は887条3項を準用しない）")
        self.assertNotIn("b1x", d.shares)

    def test_B6_renunciation_does_not_substitute(self):
        d = self.derive(dec("P"), p("A", father_id="P"),
                        p("B", father_id="P"), p("b1", father_id="B"),
                        decl=Declarations(renounced={"B"}))
        self.assertEqual(d.shares, {"A": F(1)},
                         "放棄は代襲原因でない（939条・887条2項）")
        self.assertIn("F1", self.flag_ids(d))

    def test_B7_disqualification_substitutes(self):
        d = self.derive(dec("P"), p("A", father_id="P"),
                        p("B", father_id="P"), p("b1", father_id="B"),
                        decl=Declarations(disqualified={"B"}))
        self.assertEqual(d.shares, {"A": F(1, 2), "b1": F(1, 2)},
                         "欠格は代襲原因（887条2項・891条）")
        self.assertIn("F2", self.flag_ids(d))

    def test_B8_rank_moves_when_line_empty_including_substitutes(self):
        d = self.derive(dec("P", father_id="F"), dead("A", father_id="P"),
                        dead("a1", father_id="A"), p("F"))
        self.assertEqual(d.shares, {"F": F(1)},
                         "第1順位が代襲込みで空 → 尊属へ移行")
        self.assertEqual(d.rank, 2)


class TestC_SuccessiveInheritance(_Base):
    def test_C1_suji_spouse_of_heir_participates(self):
        P = dec("P")
        A = dead("A", date=AFTER, father_id="P")
        a_sp = p("a配")
        marry(A, a_sp)
        d = self.derive(P, A, a_sp, p("a1", father_id="A"))
        self.assertEqual(d.shares, {"a配": F(1, 2), "a1": F(1, 2)},
                         "Aの地位を数次で承継（896条）・子の配偶者が入る")
        self.assertIn("数次相続", self.flag_ids(d))
        suji = next(h for h in d.heirs if h.person_id == "a配")
        self.assertIn("数次承継", suji.zokugara)
        self.assertIn("民法896条", suji.basis)

    def test_C2_predecease_flips_to_substitution(self):
        P = dec("P")
        A = dead("A", date=BEFORE, father_id="P")
        a_sp = p("a配")
        marry(A, a_sp)
        d = self.derive(P, A, a_sp, p("a1", father_id="A"))
        self.assertEqual(d.shares, {"a1": F(1)},
                         "先死亡なら代襲（887条2項）・配偶者a配は入らない")
        self.assertNotIn("a配", d.shares)

    def test_C3_suji_via_spouse(self):
        P = dec("P")
        Q = dead("Q", date=AFTER)
        marry(P, Q)
        d = self.derive(P, Q, p("A", father_id="P", mother_id="Q"))
        self.assertEqual(d.shares, {"A": F(1)},
                         "自己の1/2＋Qの地位1/2の承継で結果全部")

    def test_C4_simultaneous_death_presumption(self):
        d = self.derive(dec("P"), dead("A", date=D_DATE, father_id="P"),
                        p("a1", father_id="A"), p("B", father_id="P"))
        self.assertEqual(d.shares, {"B": F(1, 2), "a1": F(1, 2)},
                         "同時死亡推定→相互に相続しない→代襲は生じる")
        self.assertIn("同時死亡推定", self.flag_ids(d))

    def test_C5_undated_death_holds(self):
        d = self.held(dec("P"),
                      p("A", alive="死亡", death_wareki="令和6年頃",
                        father_id="P"))
        self.assertIn("死亡日が未確定", d.hold_reasons[0])
        self.assertIn("参考: 死亡記載 令和6年頃", d.hold_reasons[0])


class TestD_HalfBlood(_Base):
    def _rank3(self, *extra):
        return [dec("P", father_id="F", mother_id="M"), dead("F"), dead("M"),
                *extra]

    def test_D1_half_blood_is_half_share(self):
        d = self.derive(*self._rank3(
            p("A", father_id="F", mother_id="M"),
            p("B", father_id="F", mother_id="M2"), p("M2")))
        self.assertEqual(d.shares, {"A": F(2, 3), "B": F(1, 3)})
        b = next(h for h in d.heirs if h.person_id == "B")
        self.assertIn("民法900条4号但書", b.basis)

    def test_D2_half_blood_with_spouse(self):
        persons = self._rank3(
            p("A", father_id="F", mother_id="M"),
            p("B", father_id="F", mother_id="M2"), p("M2"))
        S = p("S")
        marry(persons[0], S)
        d = self.derive(*persons, S)
        self.assertEqual(d.shares,
                         {"S": F(3, 4), "A": F(1, 6), "B": F(1, 12)})

    def test_D3_half_blood_only_equal(self):
        d = self.derive(*self._rank3(
            p("B", father_id="F", mother_id="M2"), p("M2"),
            p("C", father_id="F", mother_id="M3"), p("M3")))
        self.assertEqual(d.shares, {"B": F(1, 2), "C": F(1, 2)},
                         "半血同士は等分")

    def test_D4_no_half_blood_for_children(self):
        d = self.derive(dec("P"),
                        p("C1", father_id="P", mother_id="W1"),
                        p("C2", father_id="P", mother_id="W2"))
        self.assertEqual(d.shares, {"C1": F(1, 2), "C2": F(1, 2)},
                         "子の相続に半血の概念なし（900条4号本文）")

    def test_D5_missing_parent_edge_holds(self):
        d = self.held(*self._rank3(
            p("A", father_id="F", mother_id="M"),
            p("B", father_id="F", mother_id="")))
        self.assertIn("全血/半血が判定できません", d.hold_reasons[0])
        self.assertIn("親エッジ", d.hold_reasons[0])


class TestE_Adoption(_Base):
    def test_E1_ordinary_adoptee_equals_biological_child(self):
        d = self.derive(dec("P"), p("C", father_id="P"),
                        p("AD", adoptive_father_id="P"),
                        decl=Declarations(adoption_kinds={"AD": "普通養子"}))
        self.assertEqual(d.shares, {"C": F(1, 2), "AD": F(1, 2)},
                         "実子と同順位・同分（809条）")

    def test_E2_ordinary_adoptee_inherits_from_biological_parent(self):
        d = self.derive(dec("P"),
                        p("C", father_id="P", adoptive_father_id="X"),
                        decl=Declarations(adoption_kinds={"C": "普通養子"}))
        self.assertEqual(d.shares, {"C": F(1)},
                         "普通養子は実方との親族関係が存続（二重資格）")

    def test_E3_special_adoptee_severed_from_biological_parent(self):
        d = self.derive(dec("P", father_id="F"),
                        p("C", father_id="P", adoptive_father_id="X"),
                        p("F"),
                        decl=Declarations(adoption_kinds={"C": "特別養子"}))
        self.assertEqual(d.shares, {"F": F(1)},
                         "特別養子は実方断絶（817条の9）→ 尊属へ")
        self.assertNotIn("C", d.shares)

    def test_E4_undetermined_adoption_kind_holds(self):
        d = self.held(dec("P"), p("AD", adoptive_father_id="P"))
        self.assertIn("養子区分", d.hold_reasons[0])
        self.assertIn("未判定", d.hold_reasons[0])

    def test_E5_child_born_before_adoption_does_not_substitute(self):
        d = self.derive(dec("P", father_id="F"),
                        dead("A", adoptive_father_id="P"),
                        p("a1", father_id="A",
                          born_before_parents_adoption=True),
                        p("F"),
                        decl=Declarations(adoption_kinds={"A": "普通養子"}))
        self.assertEqual(d.shares, {"F": F(1)},
                         "縁組前出生の子は養親を代襲しない（直系卑属要件）")
        self.assertIn("E5", self.flag_ids(d))

    def test_E6_child_born_after_adoption_substitutes(self):
        d = self.derive(dec("P"),
                        dead("A", adoptive_father_id="P"),
                        p("a1", father_id="A"),
                        decl=Declarations(adoption_kinds={"A": "普通養子"}))
        self.assertEqual(d.shares, {"a1": F(1)}, "縁組後出生の子は代襲（727条）")


class TestF_LawyerFlags(_Base):
    def test_F1_renunciation_flag(self):
        P, S = dec("P"), p("S")
        marry(P, S)
        d = self.derive(P, S, p("A", father_id="P"), p("B", father_id="P"),
                        decl=Declarations(renounced={"B"}))
        self.assertEqual(d.shares, {"S": F(1, 2), "A": F(1, 2)},
                         "放棄者を除外して再計算")
        f1 = next(f for f in d.flags if f["flag"] == "F1")
        self.assertIn("確認は人", f1["内容"])
        self.assertTrue(d.provisional, "フラグありは参考値（弁護士確認必須）")

    def test_F2_disqualification_flag(self):
        d = self.derive(dec("P"), p("A", father_id="P"),
                        p("B", father_id="P"), p("b1", father_id="B"),
                        decl=Declarations(disqualified={"B"}))
        f2 = next(f for f in d.flags if f["flag"] == "F2")
        self.assertIn("認定は人", f2["内容"])
        self.assertEqual(d.shares["b1"], F(1, 2), "欠格は代襲あり")

    def test_F3_fetus_presented(self):
        P, S = dec("P"), p("S")
        marry(P, S)
        d = self.derive(P, S, p("C", father_id="P"),
                        decl=Declarations(fetuses=["妻"]))
        self.assertEqual(d.shares,
                         {"S": F(1, 2), "C": F(1, 4), "胎児:妻": F(1, 4)},
                         "胎児は既に生まれたものとみなして提示（886条）")
        f3 = next(f for f in d.flags if f["flag"] == "F3")
        self.assertIn("出生後", f3["内容"])

    def test_F4_unknown_life_status_holds(self):
        d = self.held(dec("P"), p("A", father_id="P", alive="不明"))
        self.assertIn("生死が不明", d.hold_reasons[0])
        self.assertIn("F4", self.flag_ids(d))

    def test_F5_insufficient_koseki_collection_holds(self):
        kosekis = [{"戸籍": {"本籍": "東京都足立区鹿浜三丁目1261番地",
                             "筆頭者": "鈴木 金次",
                             "従前戸籍": {"本籍": "東京都足立区北鹿浜町1261番地",
                                          "筆頭者": "鈴木金太郎"}}}]
        d = self.held(dec("P", father_id="F", mother_id="M"),
                      dead("F"), dead("M"),
                      p("B", father_id="F", mother_id="M"),
                      kosekis=kosekis)
        self.assertIn("兄弟姉妹相続には父母の出生までの戸籍が必要",
                      d.hold_reasons[0])
        self.assertIn("北鹿浜町1261番地", d.hold_reasons[0], "不足戸籍を列挙")
        self.assertIn("F5", self.flag_ids(d))

    def test_F6_ambiguous_spouse_name_holds(self):
        P = dec("P")
        P.events.append(LifeEvent("婚姻", "平成11年", "花子"))
        d = self.held(P, p("H1", name="花子"), p("H2", name="花子"))
        self.assertIn("同姓同名", d.hold_reasons[0])
        self.assertIn("F6", self.flag_ids(d))


def _record(rid, name, *, alive="", death="", decedent="no",
            father="", identity=()):
    return {"$id": {"value": rid}, "氏名": {"value": name},
            "生死区分": {"value": alive}, "死亡日": {"value": death},
            "被相続人フラグ": {"value": decedent},
            "父人物ID": {"value": father}, "母人物ID": {"value": ""},
            "養父人物ID": {"value": ""}, "養母人物ID": {"value": ""},
            "身分事項": {"value": [
                {"value": {"事項種別": {"value": k},
                           "年月日": {"value": d},
                           "相手方": {"value": a}}}
                for k, d, a in identity]}}


class TestG_Combined(_Base):
    def test_G1_suzuki_makoto_real_data(self):
        """実データ形（App 34 レコード→persons_from_records）での一気通貫"""
        records = [
            _record("6", "鈴木誠", alive="死亡", death="2025-04-13",
                    decedent="yes",
                    identity=[("婚姻", "平成11年7月19日", "長谷川香奈"),
                              ("離婚", "令和3年8月31日", ""),
                              ("死亡", "令和7年4月13日", "")]),
            _record("7", "鈴木香奈", alive="生存",
                    identity=[("婚姻", "平成11年7月19日", "鈴木誠"),
                              ("離婚", "令和3年8月31日", "")]),
            _record("8", "香音", alive="生存", father="6"),
            _record("14", "鈴木金次", alive="死亡", death="2022-10-27"),
            _record("13", "鈴木チヨ子", alive="死亡", death="2022-07-10"),
            _record("20", "鈴木 美佳", alive="不明"),
            _record("21", "鈴木 雅寿", alive="不明"),
        ]
        d = derive_heirs(persons_from_records(records))
        self.assertEqual(d.status, "derived", d.hold_reasons)
        self.assertEqual(d.shares, {"8": F(1)}, "香音が全部")
        self.assertNotIn("7", d.shares, "香奈は離婚により相続人でない")
        unused = "\n".join(d.unused_facts)
        self.assertIn("鈴木 美佳", unused, "兄弟の生死は判定に不要（未使用列挙）")
        self.assertIn("鈴木 雅寿", unused)

    def test_G2_substitution_and_renunciation_combined(self):
        P, S = dec("P"), p("S")
        marry(P, S)
        d = self.derive(P, S, p("A", father_id="P"),
                        dead("B", father_id="P"), p("b1", father_id="B"),
                        p("C", father_id="P"), p("c1", father_id="C"),
                        decl=Declarations(renounced={"C"}))
        self.assertEqual(d.shares,
                         {"S": F(1, 2), "A": F(1, 4), "b1": F(1, 4)})
        self.assertNotIn("C", d.shares)
        self.assertNotIn("c1", d.shares, "放棄は代襲せず")

    def test_G3_sibling_substitution_half_blood_combined(self):
        d = self.derive(dec("P", father_id="F", mother_id="M"),
                        dead("F"), dead("M"),
                        p("A", father_id="F", mother_id="M"),
                        dead("B", father_id="F", mother_id="M"),
                        p("b1", father_id="B"),
                        p("C", father_id="F", mother_id="M2"), p("M2"))
        self.assertEqual(d.shares,
                         {"A": F(2, 5), "b1": F(2, 5), "C": F(1, 5)},
                         "全血2:2:1・b1はBの株を承継")

    def test_G4_special_adoptive_parent_as_ascendant(self):
        d = self.derive(dec("P", father_id="F", mother_id="M",
                            adoptive_mother_id="Y"),
                        p("F"), p("M"), p("Y"),
                        decl=Declarations(adoption_kinds={"P": "特別養子"}))
        self.assertEqual(d.shares, {"Y": F(1)},
                         "特別養子の養親が直系尊属（809条）・実方は断絶")


class TestNegatives(_Base):
    def test_no_decedent_rejected(self):
        d = derive_heirs([p("A"), p("B")])
        self.assertEqual(d.status, "error")
        self.assertIn("0 名", d.hold_reasons[0])

    def test_multiple_decedents_rejected(self):
        d = derive_heirs([dec("P1"), dec("P2")])
        self.assertEqual(d.status, "error")
        self.assertIn("2 名", d.hold_reasons[0])

    def test_decedent_without_death_date_holds(self):
        d = derive_heirs([p("P", alive="死亡", is_decedent=True),
                          p("C", father_id="P")])
        self.assertEqual(d.status, "held")
        self.assertIn("死亡日", d.hold_reasons[0])


def _node(rid, name=None, *, alive="生存", meyose="確定", kakunin="確認済",
          death_date="", is_decedent=False):
    return PersonNode(record_id=rid, name=name or rid, alive=alive,
                      meyose=meyose, kakunin=kakunin, death_date=death_date,
                      is_decedent=is_decedent)


class TestZ1GateScope(unittest.TestCase):
    """required_persons の絞り込みと validate_for_rendering の互換"""

    def _graph(self):
        nodes = [
            _node("6", "鈴木誠", alive="死亡", death_date="2025-04-13",
                  is_decedent=True),
            _node("7", "鈴木香奈"),
            _node("8", "香音"),
            # 下位順位（未確認のまま）
            _node("14", "鈴木金次", alive="死亡", meyose="未確定",
                  kakunin="未確認"),
            _node("13", "鈴木チヨ子", alive="死亡", meyose="未確定",
                  kakunin="未確認"),
            _node("20", "鈴木 美佳", alive="不明", meyose="未確定",
                  kakunin="未確認"),
        ]
        edges = [Edge(kind="婚姻", a="6", b="7"),
                 Edge(kind="親子", a="6", b="8"),
                 Edge(kind="親子", a="14", b="6"),
                 Edge(kind="親子", a="13", b="6"),
                 Edge(kind="親子", a="14", b="20"),
                 Edge(kind="親子", a="13", b="20")]
        return KinshipGraph(nodes=nodes, edges=edges)

    def test_G1_required_persons_excludes_lower_ranks(self):
        graph = self._graph()
        required = required_persons(graph, graph.node("6"))
        self.assertEqual(required, {"6", "7", "8"},
                         "第1順位確定 → 尊属（14/13）・兄弟（20）は要求しない")

    def test_validate_none_keeps_current_behavior(self):
        graph = self._graph()
        problems = validate_for_rendering(graph)
        self.assertTrue(any("鈴木 美佳" in x for x in problems),
                        "None 時は全ノード要求（現行互換）")
        self.assertEqual(problems, validate_for_rendering(graph, None))

    def test_validate_scoped_passes_when_required_are_ready(self):
        graph = self._graph()
        required = required_persons(graph, graph.node("6"))
        self.assertEqual(validate_for_rendering(graph, required), [],
                         "必要人物（6/7/8）が充足していれば下位順位の未確認は"
                         "拒否理由にならない")

    def test_renderer_heir_scope_wiring(self):
        """render_kinship(heir_scope=True) がエンジン→必要人物→ゲートを結線"""
        from kinship_renderer import (
            GraphvizUnavailable, KinshipValidationRejected, render_kinship,
        )
        graph = self._graph()
        with self.assertRaises(KinshipValidationRejected):
            render_kinship(graph, fmt="svg")  # 従来: 全員要求 → 拒否
        with patch("kinship_renderer.shutil.which", return_value=None), \
                self.assertRaises(GraphvizUnavailable):
            # 絞り込み: 検証は通過し dot 不在まで到達（=ゲート通過の証明）
            render_kinship(graph, fmt="svg", heir_scope=True)


if __name__ == "__main__":
    unittest.main()
