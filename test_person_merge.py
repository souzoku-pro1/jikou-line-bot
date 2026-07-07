"""person_merge.py（R4-2a 名寄せ候補検出スコアラー＋候補起票）のテスト

検証:
シグナル成立条件（①+③/②+③/④・単独一致は候補にしない）・実機観測ケース
（誠3重の①+③・金次の空白差・香音の②氏補完・長谷川香奈の④婚姻相互リンク・
鈴木子型の氏名欠損を誤候補化しない）・同一戸籍内ペア除外・配偶者ペアを
同一人としない・⑤従前戸籍チェーンは注記のみ・案件相違は保留起票・
チェーン縮約（勝者=最小番号への集約）・封筒定型（トップキー person_merge）・
ペアキー冪等（未処理同ペアはスキップ）・機械遷移は「未確定→自動候補」のみ
（update payload の完全固定＋「確定」への機械遷移が存在しないことの AST 検査）・
PERSON_MERGE_ENABLED 既定無効（無効時は検出も起票もしない）。kintone は全てモック。
"""

import ast
import asyncio
import json
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")

import person_merge  # noqa: E402
from person_merge import (  # noqa: E402
    PersonView, build_views, detect_merge_candidates, reduce_chain_pairs,
    score_pair, _date_compatible,
)

if os.environ.get("ANTHROPIC_API_KEY") == "dummy_key_for_import_only":
    del os.environ["ANTHROPIC_API_KEY"]

_ENV = {"PERSON_MERGE_ENABLED": "1",
        "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34",
        "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33",
        "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30"}


def run(coro):
    return asyncio.run(coro)


def _sub(rows):
    return {"value": [{"value": {k: {"value": v} for k, v in row.items()}}
                      for row in rows]}


def person_record(rid, name, birth="", case="100", meyose="未確定",
                  koseki=("1",), marriages=()):
    """App 34 レコード形のフィクスチャ。marriages は (相手方, 日付) の列"""
    identity = []
    if birth:
        identity.append({"事項種別": "出生", "年月日": birth, "相手方": ""})
    for partner, date in marriages:
        identity.append({"事項種別": "婚姻", "年月日": date, "相手方": partner})
    return {"$id": {"value": str(rid)},
            "氏名": {"value": name},
            "案件レコードID": {"value": case},
            "名寄せ確定": {"value": meyose},
            "身分事項": _sub(identity),
            "登場戸籍": _sub([{"戸籍レコードID": str(k)} for k in koseki])}


def koseki_record(rid, hittousha, honseki="", juzen_honseki=""):
    reading = {"戸籍": {"筆頭者": hittousha, "本籍": honseki,
                        "従前戸籍": {"本籍": juzen_honseki, "筆頭者": ""}}}
    return {"$id": {"value": str(rid)},
            "読解JSON": {"value": json.dumps(reading, ensure_ascii=False)}}


# 実機 App 34 の観測に寄せた既定フィクスチャ:
# 戸籍1(鈴木 誠)・戸籍2(鈴木 誠)・戸籍3(鈴木 香音)・戸籍4(長谷川 慎太郎)
DEFAULT_KOSEKI = [
    koseki_record(1, "鈴木 誠", honseki="埼玉県川口市A町1番"),
    koseki_record(2, "鈴木 誠", honseki="埼玉県川口市B町2番",
                  juzen_honseki="埼玉県川口市A町1番"),
    koseki_record(3, "鈴木 香音", honseki="埼玉県川口市C町3番"),
    koseki_record(4, "長谷川 慎太郎", honseki="東京都大田区D町4番"),
]


def views_of(persons, koseki=DEFAULT_KOSEKI):
    return build_views(persons, koseki), person_merge._koseki_info(koseki)


def score_of(persons, koseki=DEFAULT_KOSEKI):
    views, info = views_of(persons, koseki)
    assert len(views) == 2, "スコアリング対象は2名"
    return score_pair(views[0], views[1], info)


class _KT:
    """kintone モック（App 34/33/30 の検索・封筒起票・人物更新を捕捉）"""

    def __init__(self, persons, koseki=DEFAULT_KOSEKI, filed_keys=()):
        self.persons = persons
        self.koseki = koseki
        self.filed_keys = set(filed_keys)  # 既存の未処理封筒があるペアキー
        self.created = []
        self.updated = []
        self.shipping_queries = []

    async def search_records(self, app, query, fields=None):
        if app.app_id_env == "APP_KOSEKI_PERSON":
            return self.persons
        if app.app_id_env == "APP_KOSEKI_BOOK":
            return self.koseki
        assert app.app_id_env == "APP_SHIPPING"
        self.shipping_queries.append(query)
        for key in self.filed_keys:
            if f'"{key}"' in query:
                return [{"$id": {"value": "既存封筒"}}]
        return []

    async def create_record(self, app, fields):
        assert app.app_id_env == "APP_SHIPPING", "起票先は App 30 のみ"
        self.created.append(fields)
        return str(900 + len(self.created))

    async def update_record(self, app, record_id, fields, revision=None):
        assert app.app_id_env == "APP_KOSEKI_PERSON", "更新先は App 34 のみ"
        self.updated.append((record_id, fields))

    def patches(self):
        return [patch(f"hub.kintone.{n}", new=getattr(self, n))
                for n in ("search_records", "create_record", "update_record")]

    def envelope(self, i=0):
        return json.loads(self.created[i]["チャネル固有データ"])


def arm(tc, kt, env=_ENV):
    for p in [patch.dict(os.environ, env), *kt.patches()]:
        p.start()
        tc.addCleanup(p.stop)


class TestSignals(unittest.TestCase):
    """シグナル成立条件（純関数 score_pair・実機観測ケース）"""

    def test_name_and_birth_qualifies(self):
        """①+③: 誠3重型（同名・同生年月日・別戸籍）"""
        s = score_of([person_record(6, "鈴木 誠", birth="昭和20年3月5日",
                                    koseki=("1",)),
                      person_record(9, "鈴木 誠", birth="昭和20年3月5日",
                                    koseki=("2",))])
        self.assertIn(person_merge.SIGNAL_NAME, s["signals"])
        self.assertIn(person_merge.SIGNAL_BIRTH, s["signals"])
        self.assertTrue(s["qualified"])
        self.assertFalse(s["pending"])

    def test_space_variant_matches_as_signal1(self):
        """金次型: 「鈴木 金次」⇔「鈴木金次」の空白差は正規化で①一致"""
        s = score_of([person_record(14, "鈴木 金次", birth="大正10年1月2日",
                                    koseki=("1",)),
                      person_record(17, "鈴木金次", birth="大正10年1月2日",
                                    koseki=("2",))])
        self.assertIn(person_merge.SIGNAL_NAME, s["signals"])
        self.assertTrue(s["qualified"])

    def test_family_name_completion_qualifies(self):
        """②+③: 香音型（名のみ「香音」＋在籍戸籍の筆頭者「鈴木 香音」→氏=鈴木）"""
        s = score_of([person_record(8, "香音", birth="平成11年6月1日",
                                    koseki=("1",)),
                      person_record(11, "鈴木 香音", birth="平成11年6月1日",
                                    koseki=("3",))])
        self.assertIn(person_merge.SIGNAL_NAME_COMPLETED, s["signals"])
        self.assertNotIn(person_merge.SIGNAL_NAME, s["signals"])
        self.assertTrue(s["qualified"])

    def test_marriage_mutual_link_qualifies_alone(self):
        """④単独成立: 長谷川香奈型（同一の相手方＋互換日付〔prefix許容〕）"""
        s = score_of([person_record(7, "香奈", koseki=("1",),
                                    marriages=[("長谷川慎太郎", "平成11年7月")]),
                      person_record(10, "長谷川 香奈", koseki=("4",),
                                    marriages=[("長谷川慎太郎", "平成11年7月19日")])])
        self.assertEqual(s["signals"], [person_merge.SIGNAL_MARRIAGE])
        self.assertTrue(s["qualified"])

    def test_name_alone_does_not_qualify(self):
        """①単独（生年月日欠落）は候補にしない（戸籍3/4=チェーンなしの組）"""
        s = score_of([person_record(1, "鈴木 誠", koseki=("3",)),
                      person_record(2, "鈴木 誠", koseki=("4",))])
        self.assertEqual(s["signals"], [person_merge.SIGNAL_NAME])
        self.assertFalse(s["qualified"])

    def test_birth_alone_does_not_qualify(self):
        """③単独（別名・同生年月日）は候補にしない"""
        s = score_of([person_record(1, "鈴木 誠", birth="昭和20年3月5日",
                                    koseki=("3",)),
                      person_record(2, "鈴木 太郎", birth="昭和20年3月5日",
                                    koseki=("4",))])
        self.assertEqual(s["signals"], [person_merge.SIGNAL_BIRTH])
        self.assertFalse(s["qualified"])

    def test_truncated_name_is_not_candidate(self):
        """鈴木子型（氏名欠損疑い）: 「鈴木子」⇔「鈴木チヨ子」を候補化しない
        （部分一致・包含では①を成立させない）"""
        s = score_of([person_record(15, "鈴木子", birth="大正14年8月8日",
                                    koseki=("1",)),
                      person_record(13, "鈴木 チヨ子", birth="大正14年8月8日",
                                    koseki=("2",))])
        self.assertNotIn(person_merge.SIGNAL_NAME, s["signals"])
        self.assertNotIn(person_merge.SIGNAL_NAME_COMPLETED, s["signals"])
        self.assertFalse(s["qualified"])

    def test_same_koseki_pair_excluded(self):
        """同一戸籍に共起する2レコードは別人（同名同生年月日でも候補化しない）"""
        s = score_of([person_record(1, "鈴木 誠", birth="昭和20年3月5日",
                                    koseki=("1",)),
                      person_record(2, "鈴木 誠", birth="昭和20年3月5日",
                                    koseki=("1", "2"))])
        self.assertEqual(s["signals"], [])
        self.assertFalse(s["qualified"])

    def test_spouse_pair_is_not_same_person(self):
        """互いを相手方とする婚姻（配偶者ペア）は④を成立させない"""
        s = score_of([person_record(6, "鈴木 誠", koseki=("1",),
                                    marriages=[("鈴木チヨ子", "昭和40年4月1日")]),
                      person_record(13, "鈴木 チヨ子", koseki=("2",),
                                    marriages=[("鈴木誠", "昭和40年4月1日")])])
        self.assertNotIn(person_merge.SIGNAL_MARRIAGE, s["signals"])
        self.assertFalse(s["qualified"])

    def test_chain_is_annotation_only(self):
        """⑤従前戸籍チェーン: 注記には載るが成立条件に入らない"""
        # 戸籍2の従前本籍 = 戸籍1の本籍（チェーン成立）・氏名一致のみ（③なし）
        s = score_of([person_record(1, "鈴木 誠", koseki=("1",)),
                      person_record(2, "鈴木 誠", koseki=("2",))],
                     koseki=DEFAULT_KOSEKI)
        self.assertIn(person_merge.SIGNAL_CHAIN, s["signals"])
        self.assertFalse(s["qualified"], "⑤は成立条件に入らない")
        # 成立ペア（①+③）では注記として同時に載る
        s2 = score_of([person_record(1, "鈴木 誠", birth="昭和20年3月5日",
                                     koseki=("1",)),
                       person_record(2, "鈴木 誠", birth="昭和20年3月5日",
                                     koseki=("2",))])
        self.assertIn(person_merge.SIGNAL_CHAIN, s2["signals"])
        self.assertTrue(s2["qualified"])

    def test_cross_case_pair_is_pending(self):
        """案件参照が異なるペアは保留（qualified のままだが pending=True）"""
        s = score_of([person_record(1, "鈴木 誠", birth="昭和20年3月5日",
                                    case="100", koseki=("1",)),
                      person_record(2, "鈴木 誠", birth="昭和20年3月5日",
                                    case="200", koseki=("2",))])
        self.assertTrue(s["qualified"])
        self.assertTrue(s["pending"])

    def test_date_compatibility_rules(self):
        """④の日付互換: 月まで⇔日ありの prefix は互換・別日同士は非互換"""
        self.assertTrue(_date_compatible("平成11年7月", "平成11年7月19日"))
        self.assertTrue(_date_compatible("平成11年7月19日", "平成11年7月19日"))
        self.assertFalse(_date_compatible("平成11年7月1日", "平成11年7月19日"),
                         "日を含む同士の前方一致は別日（互換にしない）")
        self.assertFalse(_date_compatible("平成11年1月", "平成11年11月"))
        self.assertFalse(_date_compatible("", "平成11年7月19日"))

    def test_nameless_record_not_scored(self):
        """氏名なしレコードはスコアリング対象外（build_views で除外）"""
        views, _ = views_of([person_record(1, ""),
                             person_record(2, "鈴木 誠")])
        self.assertEqual([v.record_id for v in views], ["2"])


class TestCanonScoring(unittest.TestCase):
    """R4-2d: 大字・漢数字→算用のカノニカライズ比較（③④のみ・保存値不変）"""

    def test_daiji_birth_matches_arabic_kaneji_case(self):
        """No.25-14 型: 大字出生 ⇔ 算用出生が③一致（①+③で候補化）"""
        s = score_of([person_record(25, "鈴木金次",
                                    birth="昭和拾參年參月弐拾弐日",
                                    koseki=("3",)),
                      person_record(14, "鈴木 金次", birth="昭和13年3月22日",
                                    koseki=("4",))])
        self.assertIn(person_merge.SIGNAL_NAME, s["signals"])
        self.assertIn(person_merge.SIGNAL_BIRTH, s["signals"], "大字⇔算用の③一致")
        self.assertTrue(s["qualified"])

    def test_nameonly_daiji_chiyoko_case(self):
        """No.26-13 型: 名のみ+大字出生 ⇔ 氏名フル+算用（②+③で候補化）"""
        koseki = DEFAULT_KOSEKI + [koseki_record(7, "鈴木",
                                                 honseki="東京都足立区鹿浜三丁目")]
        s = score_of([person_record(26, "チヨ子", birth="昭和拾參年四月參日",
                                    koseki=("7",)),
                      person_record(13, "鈴木チヨ子", birth="昭和13年4月3日",
                                    koseki=("4",))],
                     koseki=koseki)
        self.assertIn(person_merge.SIGNAL_NAME_COMPLETED, s["signals"])
        self.assertIn(person_merge.SIGNAL_BIRTH, s["signals"])
        self.assertTrue(s["qualified"])

    def test_daiji_marriage_date_compatible(self):
        """④の互換日付が大字表記に対応（同一相手方+大字⇔算用日付）"""
        s = score_of([person_record(12, "鈴木縫次郎", koseki=("3",),
                                    marriages=[("内田チョ子",
                                                "昭和参拾五年拾壱月弐拾弐日")]),
                      person_record(14, "鈴木金次", koseki=("4",),
                                    marriages=[("内田チョ子",
                                                "昭和35年11月22日")])])
        self.assertIn(person_merge.SIGNAL_MARRIAGE, s["signals"])
        self.assertTrue(s["qualified"])

    def test_nuijiro_real_data_not_caught_report(self):
        """No.12-14 型（実データ）: ③は大字⇔算用で一致するが、①②不成立・
        ④は相手方の小書き仮名差（内田チヨ子/内田チョ子）で不成立 →
        単独③のため候補化されない（手動統合裁定へ・完了報告事項）"""
        s = score_of([person_record(12, "鈴木縫次郎",
                                    birth="昭和拾參年參月弐拾弐日",
                                    koseki=("3",),
                                    marriages=[("内田チヨ子",
                                                "昭和参拾五年拾壱月弐拾弐日")]),
                      person_record(14, "鈴木金次", birth="昭和13年3月22日",
                                    koseki=("4",),
                                    marriages=[("内田チョ子",
                                                "昭和35年11月22日")])])
        self.assertIn(person_merge.SIGNAL_BIRTH, s["signals"], "③自体は一致する")
        self.assertNotIn(person_merge.SIGNAL_MARRIAGE, s["signals"],
                         "④は相手方のョ/ヨ差で不成立")
        self.assertFalse(s["qualified"], "単独③は候補にしない（凍結裁定のまま）")

    def test_small_kana_names_still_distinct(self):
        """①は現行挙動のまま: 小書き仮名（ョ/ヨ）を同一視しない（氏名の
        丸め込みはしない・R5-1 判断4と同じ原則）"""
        s = score_of([person_record(13, "鈴木チヨ子", birth="昭和拾參年四月參日",
                                    koseki=("3",)),
                      person_record(30, "鈴木チョ子", birth="昭和13年4月3日",
                                    koseki=("4",))])
        self.assertNotIn(person_merge.SIGNAL_NAME, s["signals"])
        self.assertIn(person_merge.SIGNAL_BIRTH, s["signals"])
        self.assertFalse(s["qualified"])

    def test_saved_values_keep_original_notation(self):
        """保存値不変: 封筒の生年月日実値は大字の原文正規化のまま
        （カノニカライズは比較時のみ・App 34 も不変=update payload固定は既存）"""
        kt = _KT([person_record(25, "鈴木金次", birth="昭和拾參年參月弐拾弐日",
                                koseki=("3",)),
                  person_record(14, "鈴木 金次", birth="昭和13年3月22日",
                                koseki=("4",))])
        arm(self, kt)
        run(detect_merge_candidates())
        detail = kt.envelope()["person_merge"]
        self.assertEqual(detail["根拠"]["生年月日実値"]["25"],
                         "昭和拾參年參月弐拾弐日", "大字の原文を保持")
        self.assertEqual(detail["根拠"]["生年月日実値"]["14"], "昭和13年3月22日")

    def test_date_compatible_daiji_rules(self):
        from person_merge import _date_compatible
        self.assertTrue(_date_compatible("昭和参拾五年拾壱月弐拾弐日",
                                         "昭和35年11月22日"))
        self.assertFalse(_date_compatible("平成11年1月", "平成11年11月"),
                         "既存の安全側規則は維持")
        self.assertFalse(_date_compatible("昭和拾參年參月弐拾弐日",
                                          "昭和16年12月4日"))


class TestChainReduction(unittest.TestCase):
    """3名以上の連鎖の縮約（勝者=最小番号への集約）"""

    def test_triangle_reduces_to_minimum_winner(self):
        """誠3重型: (6,9)(6,19)(9,19) → (6,9)(6,19)（9-19 は 6 に集約）"""
        self.assertEqual(reduce_chain_pairs([("6", "9"), ("6", "19"),
                                             ("9", "19")]),
                         [("6", "9"), ("6", "19")])

    def test_open_chain_keeps_both(self):
        """共通ノードのない (6,9)(9,19) はどちらも残す（(6,19)未成立のため）"""
        self.assertEqual(reduce_chain_pairs([("6", "9"), ("9", "19")]),
                         [("6", "9"), ("9", "19")])


class TestDetectFlow(unittest.TestCase):
    """detect_merge_candidates の統合フロー（封筒起票・自動候補マーク・冪等）"""

    MAKOTO3 = [
        person_record(6, "鈴木 誠", birth="昭和20年3月5日", koseki=("1",)),
        person_record(9, "鈴木 誠", birth="昭和20年3月5日", koseki=("2",)),
        person_record(19, "鈴木誠", birth="昭和20年3月5日", koseki=("3",)),
    ]

    def test_envelope_format_and_pair_key(self):
        kt = _KT([person_record(6, "鈴木 誠", birth="昭和20年3月5日",
                                koseki=("1",)),
                  person_record(9, "鈴木 誠", birth="昭和20年3月5日",
                                koseki=("2",))])
        arm(self, kt)
        result = run(detect_merge_candidates())
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["filed"], 1)
        f = kt.created[0]
        self.assertEqual(f["発送ステータス"], "要確認")
        self.assertEqual(f["方向"], "受領")
        self.assertEqual(f["チャネル"], "スキャン受領")
        self.assertEqual(f["ユニット種別"], "相続一般")
        self.assertEqual(f["実行済み"], "no")
        self.assertIn("人物の名寄せ候補", f["件名"])
        detail = kt.envelope()["person_merge"]  # トップキー = person_merge
        self.assertEqual(detail["ペアキー"], "person_merge:6-9")
        self.assertEqual(detail["勝者候補"], "6", "勝者=番号小")
        self.assertEqual(detail["敗者候補"], "9")
        self.assertIn(person_merge.SIGNAL_NAME, detail["シグナル"])
        self.assertFalse(detail["保留"])
        self.assertIn("氏名", detail["根拠"])

    def test_chain_files_two_envelopes_and_marks_three(self):
        """誠3重 → 封筒2通（6-9・6-19）・自動候補マーク3名（各1回のみ）"""
        kt = _KT(self.MAKOTO3)
        arm(self, kt)
        result = run(detect_merge_candidates())
        self.assertEqual(result["filed"], 2)
        keys = [json.loads(c["チャネル固有データ"])["person_merge"]["ペアキー"]
                for c in kt.created]
        self.assertEqual(keys, ["person_merge:6-9", "person_merge:6-19"])
        self.assertEqual(kt.updated,
                         [("6", {"名寄せ確定": "自動候補"}),
                          ("9", {"名寄せ確定": "自動候補"}),
                          ("19", {"名寄せ確定": "自動候補"})],
                         "書き込みは 名寄せ確定=自動候補 のみ・同一人物は1回")

    def test_pending_pair_filed_but_not_marked(self):
        """案件相違ペア: 保留フラグつきで起票・自動候補マークはしない"""
        kt = _KT([person_record(6, "鈴木 誠", birth="昭和20年3月5日",
                                case="100", koseki=("1",)),
                  person_record(9, "鈴木 誠", birth="昭和20年3月5日",
                                case="200", koseki=("2",))])
        arm(self, kt)
        result = run(detect_merge_candidates())
        self.assertEqual(result["filed"], 1)
        detail = kt.envelope()["person_merge"]
        self.assertTrue(detail["保留"])
        self.assertIn("案件参照が相違", detail["保留理由"])
        self.assertEqual(kt.updated, [], "保留ペアは自動候補にしない")

    def test_duplicate_pair_skipped(self):
        """同ペアの封筒あり（未処理・裁定済みを問わず）→ 起票もマークもスキップ"""
        kt = _KT(self.MAKOTO3[:2], filed_keys={"person_merge:6-9"})
        arm(self, kt)
        result = run(detect_merge_candidates())
        self.assertEqual(result["filed"], 0)
        self.assertEqual(result["skipped_duplicates"], 1)
        self.assertEqual(kt.created, [])
        self.assertEqual(kt.updated, [])
        self.assertIn('like "person_merge:6-9"', kt.shipping_queries[0])
        # R4-2b: 状態を問わず照合（「別人」裁定でクローズ済みの封筒も再起票を
        # 恒久抑止する）。ステータス条件があるとクローズ済みが漏れる
        self.assertNotIn("発送ステータス", kt.shipping_queries[0])
        self.assertNotIn("実行済み", kt.shipping_queries[0])

    def test_already_confirmed_person_not_touched(self):
        """名寄せ確定が未確定以外（確定/自動候補）の人物には書かない"""
        kt = _KT([person_record(6, "鈴木 誠", birth="昭和20年3月5日",
                                meyose="確定", koseki=("1",)),
                  person_record(9, "鈴木 誠", birth="昭和20年3月5日",
                                meyose="未確定", koseki=("2",))])
        arm(self, kt)
        run(detect_merge_candidates())
        self.assertEqual(kt.updated, [("9", {"名寄せ確定": "自動候補"})],
                         "未確定→自動候補 以外の遷移は存在しない")

    def test_flag_off_by_default_does_nothing(self):
        """PERSON_MERGE_ENABLED 未設定: 検索・起票・更新の全てが不発"""
        kt = _KT(self.MAKOTO3)
        calls = []

        async def search_records(app, query, fields=None):
            calls.append(query)
            return []

        with patch.dict(os.environ, {**_ENV, "PERSON_MERGE_ENABLED": ""}), \
                patch("hub.kintone.search_records", new=search_records):
            result = run(detect_merge_candidates())
        self.assertEqual(result["status"], "disabled")
        self.assertEqual(calls, [], "無効時は読み取りもしない")
        self.assertEqual(kt.created, [])
        self.assertEqual(kt.updated, [])

    def test_env_unset_skips(self):
        kt = _KT(self.MAKOTO3)
        arm(self, kt, env={**_ENV, "APP_SHIPPING": ""})
        result = run(detect_merge_candidates())
        self.assertEqual(result["status"], "skipped")
        self.assertIn("APP_SHIPPING", result["reason"])
        self.assertEqual(kt.created, [])


class TestNoConfirmTransitionInSource(unittest.TestCase):
    """「確定」への機械遷移が存在しないことのコード検査レベルの保証（裁定1）"""

    def _tree(self):
        path = os.path.join(os.path.dirname(person_merge.__file__),
                            "person_merge.py")
        with open(path, encoding="utf-8") as f:
            return ast.parse(f.read())

    def _calls(self, tree, name):
        return [node for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == name]

    def test_update_record_payload_is_fixed_literal(self):
        """全 update_record 呼び出しの payload が {"名寄せ確定": "自動候補"} の
        リテラルであること（他フィールド・他値への書き込みコードが存在しない）"""
        calls = self._calls(self._tree(), "update_record")
        self.assertTrue(calls, "自動候補マークの update_record が存在する")
        for call in calls:
            payload = call.args[2]
            self.assertIsInstance(payload, ast.Dict)
            self.assertEqual(len(payload.keys), 1)
            self.assertEqual(payload.keys[0].value, "名寄せ確定")
            self.assertEqual(payload.values[0].value, "自動候補",
                             "「確定」への機械遷移は存在してはならない")

    def test_create_record_targets_shipping_only(self):
        """create_record の宛先は App 30（封筒）のみ（App 34/36 への起票なし）"""
        for call in self._calls(self._tree(), "create_record"):
            self.assertIsInstance(call.args[0], ast.Name)
            self.assertEqual(call.args[0].id, "APP_SHIPPING")

    def test_no_forbidden_field_literals(self):
        """確認済み系フィールドへの言及がソースに存在しない"""
        path = os.path.join(os.path.dirname(person_merge.__file__),
                            "person_merge.py")
        with open(path, encoding="utf-8") as f:
            source = f.read()
        for code in ("確認状態", "確認者", "確認日時", "グラフ確定日時",
                     "相続人候補", "相続資格", "被相続人フラグ"):
            self.assertNotIn(f'"{code}"', source, code)


if __name__ == "__main__":
    unittest.main()
