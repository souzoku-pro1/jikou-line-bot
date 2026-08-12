"""RV-08 soft merge / unmerge 移行のテスト（DRAFT_RV08_SOFT_MERGE 凍結票 §4）

カバレッジ（実装票 RV08-IMPL の要求）:
- flag 全象限（ON=soft merge・OFF=不発・全象限で物理削除への経路なし＝AST pin）
- 有効行定義の閉集合 pin（MERGE_STATE_VALUES ⇔ config 監視エントリの一字一句一致）
- 無効化行の下流除外（候補検出／グラフ／人物確認一覧／shokumu _load_persons／
  導出 projection の直接 get＝要確認）
- 新 consumer 機械検査（APP_KOSEKI_PERSON を参照する module は person_validity
  を通すか、理由つき allowlist に載ること）
- 操作台帳（preimage/postimage 記録・immutable・DB 不在は書き込みゼロで中止）
- 部分失敗→再実行の照合（適用済み=skip・未適用=続行・不一致=write 0 要確認）
- 過去物理削除分の復元 CLI（dry-run 無書込・実行・soft merge 監査の拒否）
- koseki_person_sync 冪等ヒット＝再生成抑止の現行維持（§10.2(iii)・filter 非適用）
"""

import ast
import asyncio
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")

import hub.db as db  # noqa: E402
import kinship_graph  # noqa: E402
import person_confirm  # noqa: E402
import person_merge  # noqa: E402
import person_restore_cli  # noqa: E402
from config import EXPECTED_KINTONE_SCHEMA  # noqa: E402
from hub import heir_projection as hp  # noqa: E402
from hub import kintone  # noqa: E402
from hub import shokumu_plan as sp  # noqa: E402
from hub.person_merge_journal import (  # noqa: E402
    STAGE_POSTIMAGE, STAGE_PREIMAGE, STAGE_RESTORE, PersonMergeJournalBase,
    PersonMergeOperation, record_fingerprint, record_stage)
from hub.person_validity import (  # noqa: E402
    MERGE_STATE_FIELD, MERGE_STATE_VALUES, filter_active_persons,
    is_active_person)
from person_merge_exec import MergeCandidate, execute_merge  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == "dummy_key_for_import_only":
    del os.environ["ANTHROPIC_API_KEY"]

REPO = Path(__file__).resolve().parent

_ENV = {"PERSON_MERGE_ENABLED": "1",
        "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34",
        "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33",
        "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30"}


def run(coro):
    return asyncio.run(coro)


def _cell(ftype, value):
    return {"type": ftype, "value": value}


def person(rid, name, *, state=None, case="100", birth="昭和20年3月5日"):
    rec = {
        "$id": _cell("__ID__", str(rid)),
        "$revision": _cell("__REVISION__", "3"),
        "氏名": _cell("SINGLE_LINE_TEXT", name),
        "案件レコードID": _cell("SINGLE_LINE_TEXT", case),
        "名寄せ確定": _cell("DROP_DOWN", "未確定"),
        "備考": _cell("MULTI_LINE_TEXT", ""),
        "父人物ID": _cell("SINGLE_LINE_TEXT", ""),
        "母人物ID": _cell("SINGLE_LINE_TEXT", ""),
        "養父人物ID": _cell("SINGLE_LINE_TEXT", ""),
        "養母人物ID": _cell("SINGLE_LINE_TEXT", ""),
        "身分事項": {"type": "SUBTABLE", "value": [
            {"id": "1", "value": {
                "事項種別": {"type": "SINGLE_LINE_TEXT", "value": "出生"},
                "年月日": {"type": "SINGLE_LINE_TEXT", "value": birth},
                "相手方": {"type": "SINGLE_LINE_TEXT", "value": ""},
                "記載原文": {"type": "SINGLE_LINE_TEXT", "value": ""}}}]},
        "登場戸籍": {"type": "SUBTABLE", "value": []},
    }
    if state is not None:
        rec[MERGE_STATE_FIELD] = _cell("DROP_DOWN", state)
    return rec


def arm_db(tc):
    d = tempfile.mkdtemp(prefix="rv08_")
    p = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{d}/j.db"})
    p.start()
    tc.addCleanup(p.stop)
    db.reset_for_tests()

    async def _create():
        eng = db.get_async_engine()
        async with eng.begin() as c:
            await c.run_sync(PersonMergeJournalBase.metadata.create_all)
    run(_create())
    db.reset_for_tests()
    tc.addCleanup(lambda: (db.reset_for_tests(),
                           shutil.rmtree(d, ignore_errors=True)))


# ══════════════════════════════════════════════════════════════
# 有効行定義（閉集合 pin・helper 単体）
# ══════════════════════════════════════════════════════════════

class TestValidityDefinition(unittest.TestCase):
    def test_closed_set_pin(self):
        """閉集合の増減は DRAFT_RV08 改定と同時のみ（RV08-03）"""
        self.assertEqual(MERGE_STATE_VALUES, ("有効", "統合済み無効"))

    def test_config_matches_closed_set(self):
        """config の App34 監視エントリ（統合状態）と一字一句一致"""
        f = EXPECTED_KINTONE_SCHEMA["App 34 (人物)"]["fields"]
        self.assertEqual(f["統合状態"]["required_options"],
                         list(MERGE_STATE_VALUES))
        self.assertEqual(f["統合状態"]["type"], "DROP_DOWN")
        self.assertEqual(f["統合先人物ID"]["type"], "SINGLE_LINE_TEXT")
        self.assertEqual(f["統合日時"]["type"], "DATETIME")

    def test_is_active_person(self):
        self.assertTrue(is_active_person(person(1, "甲")),
                        "フィールド不在=有効（CU 適用前互換）")
        self.assertTrue(is_active_person(person(1, "甲", state="")))
        self.assertTrue(is_active_person(person(1, "甲", state="有効")))
        self.assertFalse(is_active_person(person(1, "甲", state="統合済み無効")))
        self.assertFalse(is_active_person(person(1, "甲", state="謎の値")),
                         "閉集合外の未知値は無効扱い（安全側・拾わない）")

    def test_filter_preserves_order(self):
        a, b, c = person(1, "甲"), person(2, "乙", state="統合済み無効"), \
            person(3, "丙", state="有効")
        self.assertEqual(filter_active_persons([a, b, c]), [a, c])


# ══════════════════════════════════════════════════════════════
# AST pin（R3: App34 向け delete の不在）＋新 consumer 機械検査（RV08-03）
# ══════════════════════════════════════════════════════════════

# APP_KOSEKI_PERSON を参照するが有効行 filter を通さない module の閉集合
# （理由の無い追加は本テストが FAIL させる＝無検査 read の混入を CI で検出）
_READER_ALLOWLIST = {
    "hub/person_validity.py": "定義サイト（単一の正そのもの）",
    "config.py": "監視 env 名の定義のみ（App34 read なし）",
    "koseki_person_sync.py": "§10.2(iii) 冪等ヒット維持（意図的に filter なし）",
    "person_merge_exec.py": "勝者/敗者の直接 get＝execute_merge 内の専用状態ガード",
    "person_restore_cli.py": "監査JSONからの人手復元 CLI（裁定④・書込み側）",
    "review_resolve.py": "env 存在チェックのみ（App34 read なし）",
    "dispatch_bot/person_merge_task.py": "レコード URL 生成のみ（App34 read なし）",
    "dispatch_bot/person_confirm_task.py": "レコード URL 生成のみ（App34 read なし）",
}


def _module_files():
    for base in ("", "hub", "dispatch_bot"):
        for f in sorted((REPO / base if base else REPO).glob("*.py")):
            rel = f"{base}/{f.name}" if base else f.name
            if f.name.startswith("test_") or f.name == "conftest.py":
                continue
            yield rel, f


class TestStructuralPins(unittest.TestCase):
    def test_no_delete_record_in_person_merge_exec(self):
        """R3: person_merge_exec に delete_record 呼出し・参照が存在しない
        （flag×コード状態の全象限で物理削除への経路なし・§4 全象限表）"""
        tree = ast.parse((REPO / "person_merge_exec.py")
                         .read_text(encoding="utf-8"))
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        names |= {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        names |= {a.name for n in ast.walk(tree)
                  if isinstance(n, ast.ImportFrom) for a in n.names}
        self.assertNotIn("delete_record", names)

    def test_app34_readers_pass_validity_filter(self):
        """新 consumer 検査: APP_KOSEKI_PERSON を参照する module は
        hub.person_validity を通す（または閉集合 allowlist に理由つきで載る）"""
        hits = []
        for rel, f in _module_files():
            src = f.read_text(encoding="utf-8")
            if "APP_KOSEKI_PERSON" not in src:
                continue
            hits.append(rel)
            if rel in _READER_ALLOWLIST:
                continue
            self.assertIn(
                "person_validity", src,
                f"{rel}: App34 を参照するのに有効行 filter（hub.person_validity）"
                "を通していません（RV08-03 新 consumer 検査。正当な理由があれば"
                "_READER_ALLOWLIST へ理由つきで追加＝レビュー経由）")
        # 検査自体の生存確認（既知 consumer が 1 つも見つからないのは検査の破損）
        self.assertIn("kinship_graph.py", hits)
        self.assertIn("hub/shokumu_plan.py", hits)

    def test_koseki_sync_intentionally_unfiltered(self):
        """§10.2(iii): koseki_person_sync は filter を通さない（冪等ヒット維持）"""
        src = (REPO / "koseki_person_sync.py").read_text(encoding="utf-8")
        self.assertNotIn("filter_active_persons", src)
        self.assertIn("§10.2(iii)", src, "意図の明文化（規約コメント）")


# ══════════════════════════════════════════════════════════════
# 無効化行の下流除外（consumer 面）
# ══════════════════════════════════════════════════════════════

class TestConsumerExclusion(unittest.TestCase):
    def _search_two(self, active_second=True):
        p1 = person(6, "鈴木 誠")
        p2 = person(9, "鈴木 誠",
                    state=None if active_second else "統合済み無効")

        async def fake_search(app, query, fields=None):
            if app.app_id_env == "APP_KOSEKI_PERSON":
                return [p1, p2]
            return []
        return fake_search

    def test_candidate_detection_excludes_inactive(self):
        """検出 logic 不変・入力集合の絞りのみ（§3.1）"""
        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.search_records",
                      new=self._search_two(active_second=True)), \
                patch("hub.kintone.update_record", new=AsyncMock()), \
                patch("hub.kintone.create_record",
                      new=AsyncMock(return_value="90")):
            both = run(person_merge.detect_merge_candidates())
        self.assertTrue(both["candidates"], "対照: 両方有効なら候補が立つ")
        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.search_records",
                      new=self._search_two(active_second=False)), \
                patch("hub.kintone.update_record", new=AsyncMock()), \
                patch("hub.kintone.create_record", new=AsyncMock()):
            result = run(person_merge.detect_merge_candidates())
        self.assertEqual(result["candidates"], [],
                         "無効化行は候補ペアの入力に載らない")

    def test_kinship_graph_excludes_inactive(self):
        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.search_records",
                      new=self._search_two(active_second=False)):
            graph = run(kinship_graph.load_graph_for_case("100"))
        self.assertEqual([n.record_id for n in graph.nodes], ["6"])

    def test_person_confirm_list_excludes_inactive(self):
        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.search_records",
                      new=self._search_two(active_second=False)):
            rows = run(person_confirm.list_case_persons("100"))
        self.assertEqual([r.record_id for r in rows], ["6"])

    def test_shokumu_load_persons_excludes_inactive(self):
        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.search_records",
                      new=self._search_two(active_second=False)):
            rows = run(sp._load_persons("100"))
        self.assertEqual([(r.get("$id") or {}).get("value") for r in rows],
                         ["6"])


class TestProjectionDirectGet(unittest.TestCase):
    """RV-08 §10.2(ii)＋RV08-03「直接 get の状態確認」（heir_projection）"""

    def test_env_unset_skips_check(self):
        with patch.dict(os.environ, {"APP_KOSEKI_PERSON": "",
                                     "TOKEN_KOSEKI_PERSON": ""}):
            self.assertFalse(run(hp._source_person_inactive("12")))

    def test_inactive_and_error_fail_closed(self):
        env = {"APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34"}
        with patch.dict(os.environ, env), \
                patch("hub.kintone.get_record",
                      new=AsyncMock(return_value=person(12, "甲"))):
            self.assertFalse(run(hp._source_person_inactive("12")))
        with patch.dict(os.environ, env), \
                patch("hub.kintone.get_record",
                      new=AsyncMock(return_value=person(
                          12, "甲", state="統合済み無効"))):
            self.assertTrue(run(hp._source_person_inactive("12")))
        with patch.dict(os.environ, env), \
                patch("hub.kintone.get_record",
                      new=AsyncMock(side_effect=kintone.KintoneError(
                          404, "GAIA_RE01", "not found"))):
            self.assertTrue(run(hp._source_person_inactive("12")),
                            "取得不能も要確認へ倒す（fail-closed）")

    def test_project_row_holds_inactive_person(self):
        """無効化行の person は当該行 held（write 0・App36 検索にも到達しない）"""
        env = {"APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34",
               "APP_SOUZOKUNIN": "36", "TOKEN_SOUZOKUNIN": "t36"}
        run_obj = SimpleNamespace(id=1, case_app_id="26")
        search = AsyncMock(side_effect=AssertionError(
            "held 判定後に App36 検索へ到達してはならない"))
        with patch.dict(os.environ, env), \
                patch("hub.kintone.get_record",
                      new=AsyncMock(return_value=person(
                          12, "甲", state="統合済み無効"))), \
                patch("hub.kintone.search_records", new=search), \
                patch.object(hp, "_alert_business",
                             new=AsyncMock()) as alert:
            outcome = run(hp._project_row(run_obj, "9", "相続放棄", "12",
                                          "子", "1/2", set()))
        self.assertEqual(outcome, "held")
        alert.assert_awaited_once()
        self.assertNotIn("甲", alert.await_args.args[0], "PII 非搭載")


# ══════════════════════════════════════════════════════════════
# 操作台帳＋部分失敗の回収（§3.2a）
# ══════════════════════════════════════════════════════════════

class _StatefulKT:
    """状態を持つ kintone モック（複数回実行の照合を実測するため）"""

    def __init__(self, winner, loser, envelope_detail=None):
        self.persons = {"6": winner, "9": loser}
        detail = envelope_detail or {
            "ペアキー": "person_merge:6-9", "勝者候補": "6", "敗者候補": "9",
            "シグナル": ["①正規化氏名一致"], "保留": False,
            "根拠": {"氏名": ["No.6 鈴木 誠", "No.9 鈴木 誠"]}}
        self.envelope = {
            "$id": _cell("__ID__", "90"),
            "発送ステータス": {"value": "要確認"},
            "実行済み": {"value": "no"},
            "成果物": {"value": []},
            "チャネル固有データ": {"value": json.dumps(
                {"person_merge": detail}, ensure_ascii=False)},
        }
        self.filenames = {}     # fileKey -> name
        self.fail_next_update_on = None   # record_id（App34）で 1 回だけ失敗
        self.person_update_log = []       # (record_id, fields)

    def _apply(self, rec, fields):
        for k, v in fields.items():
            if isinstance(v, list) and isinstance(
                    (rec.get(k) or {}).get("value"), list) \
                    and (rec.get(k) or {}).get("type") == "SUBTABLE":
                rec[k] = {"type": "SUBTABLE", "value": v}
            elif isinstance(v, list):
                rec[k] = {"type": "SUBTABLE", "value": v}
            else:
                base = rec.get(k) or {}
                rec[k] = {"type": base.get("type") or "SINGLE_LINE_TEXT",
                          "value": v}

    async def get_record(self, app, rid):
        if app.app_id_env == "APP_SHIPPING":
            return self.envelope
        rec = self.persons.get(str(rid))
        if rec is None:
            raise kintone.KintoneError(404, "GAIA_RE01", "not found")
        return rec

    async def search_records(self, app, query, fields=None):
        return []      # 参照付け替え対象なし

    async def create_record(self, app, fields):
        raise AssertionError("統合実行で create_record は使わない")

    async def update_record(self, app, rid, fields, revision=None):
        rid = str(rid)
        if app.app_id_env == "APP_SHIPPING":
            if "成果物" in fields:
                self.envelope["成果物"] = {"value": [
                    {"fileKey": e["fileKey"],
                     "name": self.filenames.get(e["fileKey"], "")}
                    for e in fields["成果物"]]}
            for k in ("発送ステータス", "実行済み", "チャネル固有データ"):
                if k in fields:
                    self.envelope[k] = {"value": fields[k]}
            return
        if self.fail_next_update_on == rid:
            self.fail_next_update_on = None
            raise RuntimeError("update boom")
        self.person_update_log.append((rid, dict(fields)))
        self._apply(self.persons[rid], fields)

    async def delete_record(self, app, record_id):
        raise AssertionError("RV-08: delete_record は呼ばれてはならない")

    async def upload_file(self, app, filename, content, mime):
        key = f"fk{len(self.filenames) + 1}"
        self.filenames[key] = filename
        return key

    def patches(self):
        return [patch(f"hub.kintone.{n}", new=getattr(self, n))
                for n in ("get_record", "search_records", "create_record",
                          "update_record", "delete_record", "upload_file")]


def _cand():
    return MergeCandidate(
        review_record_id="90", pair_key="person_merge:6-9",
        winner_id="6", loser_id="9", winner_name="鈴木 誠",
        loser_name="鈴木 誠", signals=["①正規化氏名一致"])


def _journal_rows():
    async def _q():
        async with db.session_scope() as s:
            import sqlalchemy as sa
            t = PersonMergeOperation.__table__
            return (await s.execute(
                sa.select(t.c.operation_id, t.c.stage, t.c.payload)
                .order_by(t.c.id.asc()))).all()
    return run(_q())


class TestJournalAndRecovery(unittest.TestCase):
    def _arm(self, kt):
        for p in [patch.dict(os.environ, _ENV), *kt.patches()]:
            p.start()
            self.addCleanup(p.stop)
        arm_db(self)

    def test_success_writes_pre_and_postimage(self):
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="有効"))
        self._arm(kt)
        result = run(execute_merge(_cand()))
        self.assertEqual(result["status"], "merged")
        rows = _journal_rows()
        self.assertEqual([r.stage for r in rows],
                         [STAGE_PREIMAGE, STAGE_POSTIMAGE])
        self.assertEqual(rows[0].operation_id, result["operation_id"])
        pre = rows[0].payload
        self.assertEqual(pre["winner"]["id"], "6")
        self.assertEqual(pre["loser"]["id"], "9")
        for side in ("winner", "loser"):
            for k in ("pre", "post"):
                self.assertRegex(pre[side][k], r"^[0-9a-f]{64}$",
                                 "台帳は fingerprint のみ（PII 非保持）")
        blob = json.dumps(pre, ensure_ascii=False) \
            + json.dumps(rows[1].payload, ensure_ascii=False)
        self.assertNotIn("鈴木", blob, "台帳 payload に氏名を持ち込まない")

    def test_journal_immutable(self):
        kt = _StatefulKT(person(6, "鈴木 誠"), person(9, "鈴木 誠"))
        self._arm(kt)
        run(execute_merge(_cand()))

        async def _mutate():
            import sqlalchemy as sa
            async with db.session_scope() as s:
                t = PersonMergeOperation.__table__
                await s.execute(sa.update(t).values(stage="restore"))
        with self.assertRaises(Exception):
            run(_mutate())

    def test_db_unavailable_writes_nothing(self):
        """台帳へ記録できなければ kintone へ一切書かない（fail-closed）"""
        kt = _StatefulKT(person(6, "鈴木 誠"), person(9, "鈴木 誠"))
        for p in [patch.dict(os.environ, {**_ENV, "DATABASE_URL": ""}),
                  *kt.patches()]:
            p.start()
            self.addCleanup(p.stop)
        db.reset_for_tests()
        result = run(execute_merge(_cand()))
        self.assertEqual(result["status"], "aborted")
        self.assertIn("操作台帳", result["reason"])
        self.assertEqual(kt.person_update_log, [])
        self.assertEqual(kt.filenames, {}, "監査添付にも到達しない")

    def test_partial_failure_then_resume_skips_applied(self):
        """部分失敗（敗者無効化で失敗）→ 封筒 open 維持＋detail 追記 →
        再実行は勝者=適用済み skip・敗者=未適用 続行で完走（§3.2a）"""
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="有効"))
        kt.fail_next_update_on = "9"
        self._arm(kt)
        r1 = run(execute_merge(_cand()))
        self.assertEqual(r1["status"], "partial")
        self.assertEqual(self._envelope_status(kt), "要確認",
                         "部分失敗では封筒をクローズしない")
        detail = json.loads(kt.envelope["チャネル固有データ"]["value"])[
            "person_merge"]
        self.assertEqual(detail["operation_id"], r1["operation_id"])
        self.assertEqual(detail["到達段"], "敗者無効化")
        winner_updates_1 = [u for u in kt.person_update_log if u[0] == "6"]
        self.assertEqual(len(winner_updates_1), 1, "勝者更新は 1 回適用済み")

        r2 = run(execute_merge(_cand()))
        self.assertEqual(r2["status"], "merged")
        self.assertEqual(r2["operation_id"], r1["operation_id"],
                         "同一 operation の続行（新規発番しない）")
        winner_updates_2 = [u for u in kt.person_update_log if u[0] == "6"]
        self.assertEqual(len(winner_updates_2), 1,
                         "適用済みの勝者更新は再適用しない（skip）")
        loser_updates = [u for u in kt.person_update_log if u[0] == "9"]
        self.assertEqual(len(loser_updates), 1)
        self.assertEqual(loser_updates[0][1]["統合状態"], "統合済み無効")
        self.assertEqual(self._envelope_status(kt), "完了")
        stages = [r.stage for r in _journal_rows()]
        self.assertEqual(stages, [STAGE_PREIMAGE, STAGE_POSTIMAGE])

    def test_third_party_change_aborts_with_zero_writes(self):
        """再実行時に preimage と不一致（第三者変更）→ write 0 で要確認"""
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="有効"))
        kt.fail_next_update_on = "9"
        self._arm(kt)
        r1 = run(execute_merge(_cand()))
        self.assertEqual(r1["status"], "partial")
        writes_before = list(kt.person_update_log)
        # 第三者が敗者を編集
        kt.persons["9"]["備考"] = _cell("MULTI_LINE_TEXT", "第三者の編集")
        r2 = run(execute_merge(_cand()))
        self.assertEqual(r2["status"], "aborted")
        self.assertIn("盲目再適用しません", r2["reason"])
        self.assertEqual(kt.person_update_log, writes_before,
                         "App34 への追加書き込みゼロ")
        self.assertEqual(self._envelope_status(kt), "要確認")

    def test_loser_merged_to_other_winner_aborts(self):
        """敗者が別の勝者へ無効化済み → 要確認・書き込みなし（直接 get 規約）"""
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="統合済み無効"))
        kt.persons["9"]["統合先人物ID"] = _cell("SINGLE_LINE_TEXT", "77")
        self._arm(kt)
        result = run(execute_merge(_cand()))
        self.assertEqual(result["status"], "aborted")
        self.assertIn("別の統合先", result["reason"])
        self.assertEqual(kt.person_update_log, [])

    @staticmethod
    def _envelope_status(kt):
        return kt.envelope["発送ステータス"]["value"]


# ══════════════════════════════════════════════════════════════
# 復元 CLI（裁定④・過去物理削除分）
# ══════════════════════════════════════════════════════════════

def _legacy_audit():
    loser = person(9, "鈴木 誠")
    return {
        "監査種別": "person_merge",
        "ペアキー": "person_merge:6-9",
        "封筒レコードID": "90",
        "統合先レコードID": "6",
        "削除レコードID": "9",
        "参照付け替え": [{"person_record_id": "12", "fields": ["父人物ID"]}],
        "敗者レコード": loser,
    }


class TestRestoreCli(unittest.TestCase):
    def _write_audit(self, audit):
        d = tempfile.mkdtemp(prefix="rv08cli_")
        self.addCleanup(shutil.rmtree, d, True)
        path = os.path.join(d, "audit.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(audit, f, ensure_ascii=False)
        return path

    def test_dry_run_writes_nothing(self):
        path = self._write_audit(_legacy_audit())
        create = AsyncMock()
        update = AsyncMock()
        row12 = person(12, "丙")
        row12["父人物ID"] = _cell("SINGLE_LINE_TEXT", "6")
        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.create_record", new=create), \
                patch("hub.kintone.update_record", new=update), \
                patch("hub.kintone.get_record",
                      new=AsyncMock(return_value=row12)):
            person_restore_cli.main([path])
        create.assert_not_awaited()
        update.assert_not_awaited()

    def test_execute_restores_and_relinks_and_journals(self):
        arm_db(self)
        path = self._write_audit(_legacy_audit())
        create = AsyncMock(return_value="901")
        update = AsyncMock()
        row12 = person(12, "丙")
        row12["父人物ID"] = _cell("SINGLE_LINE_TEXT", "6")
        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.create_record", new=create), \
                patch("hub.kintone.update_record", new=update), \
                patch("hub.kintone.get_record",
                      new=AsyncMock(return_value=row12)):
            person_restore_cli.main([path, "--execute"])
        create.assert_awaited_once()
        payload = create.await_args.args[1]
        self.assertEqual(payload["氏名"], "鈴木 誠")
        for admin in ("統合状態", "統合先人物ID", "統合日時"):
            self.assertNotIn(admin, payload, "復元行は有効行として作る")
        update.assert_awaited_once()
        self.assertEqual(update.await_args.args[1], "12")
        self.assertEqual(update.await_args.args[2], {"父人物ID": "901"},
                         "親エッジの逆適用は新 ID へ")
        rows = _journal_rows()
        self.assertEqual([r.stage for r in rows], [STAGE_RESTORE])
        self.assertEqual(rows[0].payload["restored_new_id"], "901")

    def test_execute_skips_third_party_changed_edge(self):
        arm_db(self)
        path = self._write_audit(_legacy_audit())
        create = AsyncMock(return_value="901")
        update = AsyncMock()
        row12 = person(12, "丙")
        row12["父人物ID"] = _cell("SINGLE_LINE_TEXT", "55")   # 第三者変更
        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.create_record", new=create), \
                patch("hub.kintone.update_record", new=update), \
                patch("hub.kintone.get_record",
                      new=AsyncMock(return_value=row12)):
            person_restore_cli.main([path, "--execute"])
        update.assert_not_awaited()

    def test_soft_merge_audit_rejected(self):
        audit = _legacy_audit()
        audit["統合方式"] = "soft_merge"
        path = self._write_audit(audit)
        with patch.dict(os.environ, _ENV):
            with self.assertRaises(SystemExit):
                person_restore_cli.main([path, "--execute"])


# ══════════════════════════════════════════════════════════════
# koseki_person_sync 冪等ヒット（§10.2(iii)・現行維持の pin）
# ══════════════════════════════════════════════════════════════

class TestKosekiSyncIdempotency(unittest.TestCase):
    def test_disabled_row_still_hits_idempotency(self):
        """無効化行も冪等ヒット＝再生成を抑止（重複人物の再出現を防ぐ）"""
        import koseki_person_sync as kps
        # _find_existing は $id のみ取得＝統合状態で絞らない検索（filter 非適用）
        with patch.dict(os.environ, _ENV), \
                patch("hub.kintone.search_records",
                      new=AsyncMock(return_value=[{"$id": {"value": "5"}}])) \
                as search:
            self.assertEqual(run(kps._find_existing("2", "山田")), "5")
        query = search.await_args.args[1]
        self.assertNotIn("統合状態", query, "検索条件でも無効化行を除外しない")


# ══════════════════════════════════════════════════════════════
# fingerprint（照合の正規形）
# ══════════════════════════════════════════════════════════════

class TestFingerprint(unittest.TestCase):
    def test_stable_under_revision_and_datetime(self):
        a = person(6, "鈴木 誠")
        b = person(6, "鈴木 誠")
        b["$revision"] = _cell("__REVISION__", "99")
        b["統合日時"] = _cell("DATETIME", "2026-08-12T15:00:00+09:00")
        self.assertEqual(record_fingerprint(a), record_fingerprint(b),
                         "$revision・統合日時は照合対象外")

    def test_sensitive_to_field_change(self):
        a = person(6, "鈴木 誠")
        b = person(6, "鈴木 誠")
        b["備考"] = _cell("MULTI_LINE_TEXT", "x")
        self.assertNotEqual(record_fingerprint(a), record_fingerprint(b))


if __name__ == "__main__":
    unittest.main()
