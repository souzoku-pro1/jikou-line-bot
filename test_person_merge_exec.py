"""person_merge_exec.py（R4-2b T1 統合実行）のテスト

検証:
転記規則（勝者既存値の不上書き・空フィールドのみ転記・サブテーブル和集合と
重複行排除・追加行なしは不書込）・確認済み系不触・名寄せ確定=確定 は人の確定
操作経路のみ・監査JSONラウンドトリップ（敗者の全フィールド＋全サブテーブル行が
欠落なく復元可能）・順序固定（監査添付の成功が削除の前提: 添付失敗→削除・更新・
クローズの全不発）・親エッジ参照の付け替え（削除前）・二重実行ガード（封筒再読）・
「別人」クローズ＋裁定記録（App 34 不触）・棄却済みペアの再起票恒久抑止
（スコアラー連携）・フラグ無効/env未設定の完全不発・封筒一覧の解釈。
kintone は全てモック。
"""

import asyncio
import json
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")

import person_merge  # noqa: E402
import person_merge_exec  # noqa: E402
from hub import kintone  # noqa: E402
from person_merge_exec import (  # noqa: E402
    MergeCandidate, build_merge_payload, execute_merge, list_merge_candidates,
    reject_pair, restore_payload_from_audit,
)

if os.environ.get("ANTHROPIC_API_KEY") == "dummy_key_for_import_only":
    del os.environ["ANTHROPIC_API_KEY"]

_ENV = {"PERSON_MERGE_ENABLED": "1",
        "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34",
        "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30"}


def run(coro):
    return asyncio.run(coro)


def _cell(ftype, value):
    return {"type": ftype, "value": value}


def _sub_row(cols: dict, row_id="1"):
    return {"id": row_id,
            "value": {c: {"type": "SINGLE_LINE_TEXT", "value": v}
                      for c, v in cols.items()}}


def person_record(rid, name, *, biko="", furigana="", meyose="自動候補",
                  case="100", father="", kakunin="未確認",
                  identity=(), koseki=()):
    """App 34 GET 形のフィクスチャ（システムフィールド込み）"""
    return {
        "$id": _cell("__ID__", str(rid)),
        "$revision": _cell("__REVISION__", "3"),
        "レコード番号": _cell("RECORD_NUMBER", str(rid)),
        "作成者": _cell("CREATOR", {"code": "gas"}),
        "作成日時": _cell("CREATED_TIME", "2026-07-07T00:00:00Z"),
        "氏名": _cell("SINGLE_LINE_TEXT", name),
        "フリガナ": _cell("SINGLE_LINE_TEXT", furigana),
        "案件レコードID": _cell("SINGLE_LINE_TEXT", case),
        "名寄せ確定": _cell("DROP_DOWN", meyose),
        "備考": _cell("MULTI_LINE_TEXT", biko),
        "父人物ID": _cell("SINGLE_LINE_TEXT", father),
        "確認状態": _cell("DROP_DOWN", kakunin),
        "確認者": _cell("SINGLE_LINE_TEXT", ""),
        "身分事項": {"type": "SUBTABLE",
                     "value": [_sub_row(c, str(i + 1))
                               for i, c in enumerate(identity)]},
        "登場戸籍": {"type": "SUBTABLE",
                     "value": [_sub_row(c, str(i + 1))
                               for i, c in enumerate(koseki)]},
    }


def envelope_record(rid="90", pair="person_merge:6-9", winner="6", loser="9",
                    status="要確認", executed="no", pending=False, files=()):
    detail = {"ペアキー": pair, "勝者候補": winner, "敗者候補": loser,
              "シグナル": ["①正規化氏名一致", "③生年月日一致"],
              "保留": pending,
              "保留理由": "案件参照が相違" if pending else "",
              "根拠": {"氏名": [f"No.{winner} 鈴木 誠", f"No.{loser} 鈴木 誠"]}}
    return {"$id": _cell("__ID__", str(rid)),
            "発送ステータス": {"value": status},
            "実行済み": {"value": executed},
            "件名": {"value": "人物の名寄せ候補: ..."},
            "成果物": {"value": [{"fileKey": k, "name": f"{k}.json"}
                                 for k in files]},
            "チャネル固有データ": {"value": json.dumps(
                {"person_merge": detail}, ensure_ascii=False)}}


def candidate(review="90", winner="6", loser="9", pending=False):
    return MergeCandidate(
        review_record_id=review, pair_key=f"person_merge:{winner}-{loser}",
        winner_id=winner, loser_id=loser,
        winner_name="鈴木 誠", loser_name="鈴木 誠",
        signals=["①正規化氏名一致", "③生年月日一致"], pending_case=pending)


class _KT:
    """kintone モック（呼び出し順序を sequence に記録する）"""

    def __init__(self, *, envelope=None, persons=None, referrers=(),
                 upload_fail=False, shipping_list=()):
        self.envelope = envelope or envelope_record()
        self.persons = persons or {}
        self.referrers = list(referrers)
        self.upload_fail = upload_fail
        self.shipping_list = list(shipping_list)
        self.sequence = []          # ("upload"|"update"|"delete", 詳細)
        self.updated = []
        self.deleted = []
        self.uploads = []

    async def get_record(self, app, record_id):
        if app.app_id_env == "APP_SHIPPING":
            return self.envelope
        rec = self.persons.get(str(record_id))
        if rec is None:
            raise kintone.KintoneError(404, "GAIA_RE01", "not found")
        return rec

    async def search_records(self, app, query, fields=None):
        if app.app_id_env == "APP_SHIPPING":
            return self.shipping_list
        assert "父人物ID" in query, "参照洗い出しは親エッジ4フィールドで検索"
        return self.referrers

    async def create_record(self, app, fields):
        raise AssertionError("統合実行で create_record は使わない")

    async def update_record(self, app, record_id, fields, revision=None):
        self.sequence.append(("update", app.app_id_env, str(record_id),
                              fields))
        self.updated.append((app.app_id_env, str(record_id), fields))

    async def delete_record(self, app, record_id):
        assert app.app_id_env == "APP_KOSEKI_PERSON", "削除は App 34 のみ"
        self.sequence.append(("delete", str(record_id)))
        self.deleted.append(str(record_id))

    async def upload_file(self, app, filename, content, mime):
        if self.upload_fail:
            raise RuntimeError("upload boom")
        self.sequence.append(("upload", filename))
        self.uploads.append((filename, content))
        return f"fk-{len(self.uploads)}"

    def patches(self):
        return [patch(f"hub.kintone.{n}", new=getattr(self, n))
                for n in ("get_record", "search_records", "create_record",
                          "update_record", "delete_record", "upload_file")]

    def person_updates(self, rid=None):
        return [(r, f) for a, r, f in self.updated
                if a == "APP_KOSEKI_PERSON" and (rid is None or r == rid)]

    def shipping_updates(self):
        return [(r, f) for a, r, f in self.updated if a == "APP_SHIPPING"]

    def audit(self):
        return json.loads(self.uploads[0][1].decode("utf-8"))


def arm(tc, kt, env=_ENV):
    for p in [patch.dict(os.environ, env), *kt.patches()]:
        p.start()
        tc.addCleanup(p.stop)


WINNER = dict(name="鈴木 誠", furigana="スズキマコト", biko="",
              identity=({"事項種別": "出生", "年月日": "昭和20年3月5日",
                         "相手方": "", "記載原文": ""},),
              koseki=({"戸籍レコードID": "1", "登場区分": "筆頭者",
                       "続柄原文": "", "在籍期間メモ": ""},))
LOSER = dict(name="鈴木 誠", furigana="", biko="旧戸籍由来",
             identity=({"事項種別": "出生", "年月日": "昭和20年3月5日",
                        "相手方": "", "記載原文": ""},
                       {"事項種別": "死亡", "年月日": "令和7年4月13日",
                        "相手方": "", "記載原文": "死亡"}),
             koseki=({"戸籍レコードID": "2", "登場区分": "筆頭者",
                      "続柄原文": "", "在籍期間メモ": ""},))


def _default_kt(**kw):
    return _KT(persons={"6": person_record(6, **WINNER),
                        "9": person_record(9, **LOSER)}, **kw)


class TestMergeRules(unittest.TestCase):
    """転記規則（純関数 build_merge_payload）"""

    def test_fill_only_empty_fields(self):
        """勝者側が空のフィールドのみ転記・勝者の既存値は上書きしない"""
        payload = build_merge_payload(person_record(6, **WINNER),
                                      person_record(9, **LOSER))
        self.assertEqual(payload["備考"], "旧戸籍由来", "勝者空→敗者値を転記")
        self.assertNotIn("フリガナ", payload, "敗者側が空は転記しない")
        self.assertNotIn("氏名", payload, "勝者に値あり→不上書き")
        self.assertNotIn("案件レコードID", payload, "勝者に値あり→不上書き")

    def test_subtable_union_with_dedup(self):
        """登場戸籍・身分事項は和集合（重複行=出生は1行に・敗者の死亡行が加わる）"""
        payload = build_merge_payload(person_record(6, **WINNER),
                                      person_record(9, **LOSER))
        identity = [r["value"]["事項種別"]["value"] for r in payload["身分事項"]]
        self.assertEqual(identity, ["出生", "死亡"], "重複の出生は1行・死亡が追加")
        koseki = [r["value"]["戸籍レコードID"]["value"] for r in payload["登場戸籍"]]
        self.assertEqual(koseki, ["1", "2"], "登場戸籍の和集合")

    def test_no_new_rows_skips_subtable(self):
        """敗者由来の追加行が無いサブテーブルは書かない"""
        loser = dict(LOSER, identity=WINNER["identity"], koseki=WINNER["koseki"])
        payload = build_merge_payload(person_record(6, **WINNER),
                                      person_record(9, **loser))
        self.assertNotIn("身分事項", payload)
        self.assertNotIn("登場戸籍", payload)

    def test_forbidden_fields_never_in_payload(self):
        """確認済み系は敗者に値があり勝者が空でも転記しない（統合しても未確認）"""
        loser = person_record(9, **LOSER, )
        loser["確認状態"] = _cell("DROP_DOWN", "確認済")
        loser["確認者"] = _cell("SINGLE_LINE_TEXT", "大野")
        loser["相続資格"] = _cell("DROP_DOWN", "法定相続人")
        winner = person_record(6, **WINNER, )
        winner["確認状態"] = _cell("DROP_DOWN", "")
        payload = build_merge_payload(winner, loser)
        for code in person_merge_exec.FORBIDDEN_FIELDS:
            self.assertNotIn(code, payload, code)
        self.assertNotIn("名寄せ確定", payload, "遷移は呼び出し元で明示付与")

    def test_system_fields_never_in_payload(self):
        payload = build_merge_payload(person_record(6, **WINNER),
                                      person_record(9, **LOSER))
        for code in ("$id", "$revision", "レコード番号", "作成者", "作成日時"):
            self.assertNotIn(code, payload, code)


class TestExecuteMerge(unittest.TestCase):
    """統合実行の順序固定・監査・参照付け替え・ガード"""

    def test_full_flow_order_and_close(self):
        """順序: 監査添付 → 勝者更新 → 敗者削除 → 封筒クローズ"""
        kt = _default_kt()
        arm(self, kt)
        result = run(execute_merge(candidate()))
        self.assertEqual(result["status"], "merged")
        kinds = [s[0] for s in kt.sequence]
        self.assertEqual(kinds, ["upload", "update", "update", "delete",
                                 "update"],
                         "添付→(成果物更新)→勝者更新→削除→クローズ")
        self.assertEqual(kt.sequence[1][1], "APP_SHIPPING", "監査添付が先頭")
        self.assertEqual(kt.sequence[2][1], "APP_KOSEKI_PERSON")
        winner_update = kt.sequence[2][3]
        self.assertEqual(winner_update["名寄せ確定"], "確定",
                         "人の確定操作の結果として確定へ遷移")
        self.assertEqual(winner_update["備考"], "旧戸籍由来")
        self.assertEqual(kt.deleted, ["9"], "敗者の物理削除")
        close = kt.shipping_updates()[-1][1]
        self.assertEqual(close["発送ステータス"], "完了")
        self.assertEqual(close["実行済み"], "yes")

    def test_audit_attach_failure_blocks_all_writes(self):
        """監査保存失敗 → 削除・勝者更新・付け替え・クローズの全不発（順序固定）"""
        kt = _default_kt(upload_fail=True,
                         referrers=[{"$id": {"value": "12"},
                                     "父人物ID": {"value": "9"},
                                     "母人物ID": {"value": ""},
                                     "養父人物ID": {"value": ""},
                                     "養母人物ID": {"value": ""}}])
        arm(self, kt)
        result = run(execute_merge(candidate()))
        self.assertEqual(result["status"], "aborted")
        self.assertIn("監査JSONの保存に失敗", result["reason"])
        self.assertEqual(kt.deleted, [], "監査なしで削除しない")
        self.assertEqual(kt.updated, [], "App 34 への書き込みもゼロ")

    def test_audit_roundtrip_completeness(self):
        """監査JSONから敗者の全フィールド・全サブテーブル行が欠落なく復元可能"""
        loser = person_record(9, **LOSER)
        kt = _KT(persons={"6": person_record(6, **WINNER), "9": loser})
        arm(self, kt)
        run(execute_merge(candidate()))
        audit = kt.audit()
        # 取得→JSON化→復元→同値比較（レコード verbatim 保持）
        self.assertEqual(audit["敗者レコード"], loser)
        restored = restore_payload_from_audit(audit)
        for code, cell in loser.items():
            if cell.get("type") in person_merge_exec.SYSTEM_TYPES:
                self.assertNotIn(code, restored, "システム項目は復元対象外")
                continue
            self.assertIn(code, restored, f"{code} が復元payloadに欠落")
        self.assertEqual(restored["備考"], "旧戸籍由来")
        self.assertEqual(len(restored["身分事項"]), 2, "サブテーブル全行")
        self.assertEqual(
            restored["身分事項"][1]["value"]["年月日"]["value"],
            "令和7年4月13日")
        self.assertEqual(audit["統合先レコードID"], "6")
        self.assertEqual(audit["削除レコードID"], "9")
        self.assertEqual(audit["封筒レコードID"], "90")
        self.assertTrue(audit["成立シグナル"])

    def test_reference_repointing_before_delete(self):
        """敗者を親エッジで参照する人物は削除前に勝者へ付け替える"""
        kt = _default_kt(referrers=[{"$id": {"value": "12"},
                                     "父人物ID": {"value": "9"},
                                     "母人物ID": {"value": "7"},
                                     "養父人物ID": {"value": ""},
                                     "養母人物ID": {"value": ""}}])
        arm(self, kt)
        result = run(execute_merge(candidate()))
        repoint = [(r, f) for r, f in kt.person_updates("12")]
        self.assertEqual(repoint, [("12", {"父人物ID": "6"})],
                         "一致したフィールドのみ・母人物ID(別人)は触らない")
        delete_pos = kt.sequence.index(("delete", "9"))
        repoint_pos = next(i for i, s in enumerate(kt.sequence)
                           if s[0] == "update" and s[2] == "12")
        self.assertLess(repoint_pos, delete_pos, "付け替えは削除前")
        self.assertEqual(result["repointed"],
                         [{"person_record_id": "12", "fields": ["父人物ID"]}])
        self.assertEqual(kt.audit()["参照付け替え"], result["repointed"],
                         "監査JSONにも付け替えを記録")

    def test_guard_envelope_no_longer_pending(self):
        """封筒が要確認でなくなっていたら書き込みゼロで中止（二重実行ガード）"""
        kt = _default_kt(envelope=envelope_record(status="完了",
                                                  executed="yes"))
        arm(self, kt)
        result = run(execute_merge(candidate()))
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(kt.updated, [])
        self.assertEqual(kt.deleted, [])
        self.assertEqual(kt.uploads, [])

    def test_missing_person_aborts(self):
        """勝者/敗者レコードが見つからない（既に削除等）→ 書き込みゼロで中止"""
        kt = _KT(persons={"6": person_record(6, **WINNER)})  # 9 が不在
        arm(self, kt)
        result = run(execute_merge(candidate()))
        self.assertEqual(result["status"], "aborted")
        self.assertIn("取得に失敗", result["reason"])
        self.assertEqual(kt.updated, [])
        self.assertEqual(kt.deleted, [])

    def test_flag_off_or_env_unset_does_nothing(self):
        kt = _default_kt()
        arm(self, kt, env={**_ENV, "PERSON_MERGE_ENABLED": ""})
        result = run(execute_merge(candidate()))
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(kt.updated, [])
        kt2 = _default_kt()
        arm(self, kt2, env={**_ENV, "APP_KOSEKI_PERSON": ""})
        result2 = run(execute_merge(candidate()))
        self.assertEqual(result2["status"], "unavailable")
        self.assertIn("APP_KOSEKI_PERSON", result2["reason"])


class TestRejectPair(unittest.TestCase):
    """「別人」裁定: クローズ＋裁定記録・App 34 不触・再起票の恒久抑止"""

    def test_reject_closes_with_ruling(self):
        kt = _default_kt()
        arm(self, kt)
        result = run(reject_pair(candidate()))
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(kt.person_updates(), [], "App 34 には一切書かない")
        self.assertEqual(kt.deleted, [], "削除もしない")
        rid, fields = kt.shipping_updates()[0]
        self.assertEqual(rid, "90")
        self.assertEqual(fields["発送ステータス"], "完了")
        self.assertEqual(fields["実行済み"], "yes")
        detail = json.loads(fields["チャネル固有データ"])["person_merge"]
        self.assertEqual(detail["裁定"], "別人")
        self.assertEqual(detail["ペアキー"], "person_merge:6-9",
                         "ペアキーは保持（恒久抑止の like 照合が拾う）")

    def test_rejected_pair_suppresses_refiling(self):
        """棄却済み（クローズ済み）封筒でもスコアラーの再起票が抑止される
        （person_merge._already_filed は状態を問わず照合する）"""
        closed = envelope_record(status="完了", executed="yes")

        async def search_records(app, query, fields=None):
            self.assertNotIn("発送ステータス", query, "状態条件なし（恒久抑止）")
            if 'like "person_merge:6-9"' in query:
                return [closed]
            return []

        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.search_records", new=search_records):
            self.assertTrue(run(person_merge._already_filed("person_merge:6-9")))

    def test_guard_and_flag(self):
        kt = _default_kt(envelope=envelope_record(executed="yes"))
        arm(self, kt)
        self.assertEqual(run(reject_pair(candidate()))["status"], "aborted")
        self.assertEqual(kt.updated, [])
        kt2 = _default_kt()
        arm(self, kt2, env={**_ENV, "PERSON_MERGE_ENABLED": ""})
        self.assertEqual(run(reject_pair(candidate()))["status"], "unavailable")
        self.assertEqual(kt2.updated, [])


class TestListCandidates(unittest.TestCase):
    """未処理封筒の一覧化（勝者/敗者・氏名・シグナル・保留の解釈）"""

    def test_parses_envelopes(self):
        kt = _KT(shipping_list=[
            envelope_record(rid="90"),
            envelope_record(rid="91", pair="person_merge:7-10",
                            winner="7", loser="10", pending=True)])
        arm(self, kt)
        cands = run(list_merge_candidates())
        self.assertEqual(len(cands), 2)
        c = cands[0]
        self.assertEqual((c.review_record_id, c.winner_id, c.loser_id),
                         ("90", "6", "9"))
        self.assertEqual(c.winner_name, "鈴木 誠")
        self.assertIn("①正規化氏名一致", c.signals)
        self.assertFalse(c.pending_case)
        self.assertTrue(cands[1].pending_case)
        self.assertEqual(cands[1].pair_key, "person_merge:7-10")

    def test_broken_envelope_skipped(self):
        broken = envelope_record(rid="92")
        broken["チャネル固有データ"] = {"value": "not-json"}
        kt = _KT(shipping_list=[broken, envelope_record(rid="93")])
        arm(self, kt)
        cands = run(list_merge_candidates())
        self.assertEqual([c.review_record_id for c in cands], ["93"])

    def test_env_unset_returns_empty(self):
        kt = _KT()
        arm(self, kt, env={**_ENV, "APP_SHIPPING": ""})
        self.assertEqual(run(list_merge_candidates()), [])


if __name__ == "__main__":
    unittest.main()
