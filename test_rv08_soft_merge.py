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
# AST 構造検査（RV08-IMPL-03/留保1）: delete pin 全 module 拡張＋
# consumer 検査の AST 化（文字列包含検査は廃止）
# ══════════════════════════════════════════════════════════════

# ── RV08-IMPL-06（採用方式=レビュー提示の代替: reader 関数名の閉集合 manifest＋
#    各関数の AST 形検査＋negative 直接固定）──────────────────────────────────
# App34 を read する**関数の全数**（(module, 関数名) → 規律）。読取を含む関数が
# ここに無ければ検査が FAIL する（新 reader の追加＝manifest 登録＝レビュー経由）。
# 規律: search="filter"（(a) filter Call＋戻り値使用を関数内 AST で検証）／
#       get="guard"（(b) is_active_person への流入＋停止分岐到達を検証）／
#       "exempt"（理由必須・fingerprint 照合等の別防御を明記）
_APP34_READER_MANIFEST = {
    ("person_merge.py", "detect_merge_candidates"): {"search": "filter"},
    ("kinship_graph.py", "load_graph_for_case"): {"search": "filter"},
    ("person_confirm.py", "list_case_persons"): {"search": "filter"},
    ("dispatch_bot/heir_derive_task.py", "_pipeline"): {"search": "filter"},
    ("hub/shokumu_plan.py", "_load_persons"): {"search": "filter"},
    # PWA-BATCH-1: 相続案件ダッシュボード（read-only 表示・filter 経由で
    # 統合済み無効を除外し、除外件数のみ注記用に返す）
    ("hub/webapp_souzoku_dashboard.py", "_load_persons"): {"search": "filter"},
    ("koseki_person_sync.py", "_find_existing"): {
        "search": "exempt",
        "reason": "§10.2(iii) 冪等ヒット維持（無効化行もヒット＝再生成抑止・確定事項）"},
    ("koseki_person_sync.py", "sync_missing_persons"): {
        "search": "exempt",
        "reason": "戸籍単位の人物生成済み判定（存在確認のみ・§10.2(iii) と同根）"},
    ("koseki_person_sync.py", "sync_persons_from_koseki"): {
        "get": "exempt",
        "reason": "over-approx 検出（RV08-IMPL-09）: APP_KOSEKI_PERSON 参照は"
                  "env 存在チェックのみ・get は App33（戸籍）で App34 read なし"},
    ("person_merge_exec.py", "_find_referrers"): {
        "search": "exempt",
        "reason": "無効化行の親エッジも付け替え/監査対象（preimage 保全）"},
    ("person_merge_exec.py", "execute_merge"): {"get": "guard"},
    ("person_merge_exec.py", "reconcile_merge_operations"): {
        "get": "exempt",
        "reason": "postimage fingerprint の閉集合検証（読み取り照合のみ・"
                  "不一致は警報＋追記なし・App34 書込みゼロ）"},
    ("hub/heir_projection.py", "_source_person_inactive"): {"get": "guard"},
    ("person_restore_cli.py", "_find_restored_candidate"): {
        "search": "exempt", "get": "exempt",
        "reason": "復元操作ID の決定的同定（三値 fail-closed）＋payload "
                  "fingerprint 完全一致のみ採用"},
    ("person_restore_cli.py", "_classify_relink"): {
        "get": "exempt",
        "reason": "per-edge 三値照合（勝者=適用/新ID=skip/第三者変更=write 0）"},
    ("person_restore_cli.py", "_converge_existing"): {
        "get": "exempt",
        "reason": "done-path の fingerprint 現況照合（不一致=write 0 要確認・"
                  "親エッジは per-edge 三値照合の冪等追適用のみ）"},
}

# get 結果の許可された流入先（状態確認・照合系の閉集合）
_GET_FLOW_SINKS = ("is_active_person", "record_fingerprint", "_edge_view",
                   "_subset_fp")


def _tracked_modules():
    import subprocess
    out = subprocess.run(["git", "ls-files", "*.py"], capture_output=True,
                         text=True, check=True, cwd=REPO).stdout
    for line in out.splitlines():
        if not line or line.startswith(("legacy/", "alembic/")):
            continue
        p = Path(line)
        if p.name.startswith("test_") or p.name == "conftest.py":
            continue
        yield line, REPO / p


def _call_name(node) -> str:
    f = node.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return ""


def _is_app34_arg(node) -> bool:
    return (isinstance(node, ast.Name) and node.id == "APP_KOSEKI_PERSON") or \
        (isinstance(node, ast.Attribute) and node.attr == "APP_KOSEKI_PERSON")


def _walk_local(node):
    """node 配下を、ネストした関数/クラス定義の内側へ入らずに走査する
    （call を最内包の関数へ帰属させるための境界付き walk）"""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                              ast.ClassDef)):
            continue
        yield child
        yield from _walk_local(child)


def _refs_app34(local_nodes) -> tuple[bool, set]:
    """RV08-IMPL-09: APP_KOSEKI_PERSON の参照検出（直接＋**単純 alias 1 hop**）。
    Returns: (参照の有無, 参照名の集合={APP_KOSEKI_PERSON}∪alias 名)"""
    names = {"APP_KOSEKI_PERSON"}
    direct = False
    for n in local_nodes:
        if isinstance(n, ast.Name) and n.id == "APP_KOSEKI_PERSON":
            direct = True
        elif isinstance(n, ast.Attribute) and n.attr == "APP_KOSEKI_PERSON":
            direct = True
        if isinstance(n, ast.Assign) and len(n.targets) == 1 \
                and isinstance(n.targets[0], ast.Name):
            for c in ast.walk(n.value):
                if (isinstance(c, ast.Name) and c.id == "APP_KOSEKI_PERSON") \
                        or (isinstance(c, ast.Attribute)
                            and c.attr == "APP_KOSEKI_PERSON"):
                    names.add(n.targets[0].id)
                    direct = True
                    break
    return direct, names


def _read_calls(local_nodes):
    return [n for n in local_nodes if isinstance(n, ast.Call)
            and _call_name(n) in ("search_records", "get_record")]


def _app34_calls(local_nodes):
    """精密側: 引数（**位置・keyword とも**）が APP_KOSEKI_PERSON
    （alias 1 hop 込み）を参照する search/get 呼出し（規律検査の対象）"""
    _direct, names = _refs_app34(local_nodes)
    searches, gets = [], []
    for n in _read_calls(local_nodes):
        linked = False
        for sub in list(n.args) + [kw.value for kw in n.keywords]:
            for c in ast.walk(sub):
                if (isinstance(c, ast.Name) and c.id in names) or \
                        (isinstance(c, ast.Attribute)
                         and c.attr == "APP_KOSEKI_PERSON"):
                    linked = True
                    break
        if linked:
            (searches if _call_name(n) == "search_records"
             else gets).append(n)
    return searches, gets


def _reader_functions(tree) -> dict:
    """module 内の App34 reader を関数単位で列挙する（RV08-IMPL-09・
    **取りこぼし方向を塞ぐ過大近似**: 本体が APP_KOSEKI_PERSON を直接または
    alias 1 hop で参照し、かつ search_records/get_record を呼ぶ関数はすべて
    reader＝manifest 照合対象）。
    Returns: {関数名（module 直下は "<module>"）:
              (fn_node, 精密searches, 精密gets, 全read呼出し)}"""
    out = {}

    def probe(name, node):
        local = list(_walk_local(node))
        direct, _names = _refs_app34(local)
        reads = _read_calls(local)
        if direct and reads:
            s, g = _app34_calls(local)
            out[name] = (node, s, g, reads)

    probe("<module>", tree)
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            probe(fn.name, fn)
    return out


def _parent_map(root) -> dict:
    parents = {}
    for node in ast.walk(root):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _unwrap_await(node, parents):
    p = parents.get(node)
    while isinstance(p, ast.Await):
        p = parents.get(p)
    return p


def check_filter_discipline(fn) -> list[str]:
    """(a) 関数単位: App34 search 結果が filter_active_persons へ渡り、
    filter 戻り値が後続処理/return に使用されることを検証（RV08-IMPL-06）"""
    local = list(_walk_local(fn))
    parents = _parent_map(fn)
    searches, _gets = _app34_calls(local)
    problems: list[str] = []
    filter_calls = [n for n in local if isinstance(n, ast.Call)
                    and _call_name(n) == "filter_active_persons"]
    for s in searches:
        p = _unwrap_await(s, parents)
        ok = isinstance(p, ast.Call) \
            and _call_name(p) == "filter_active_persons"
        if not ok and isinstance(p, ast.Assign) and len(p.targets) == 1 \
                and isinstance(p.targets[0], ast.Name):
            var = p.targets[0].id
            ok = any(any(isinstance(a, ast.Name) and a.id == var
                         for a in fc.args) for fc in filter_calls)
        if not ok:
            problems.append(
                f"行{s.lineno}: search 結果が filter_active_persons の引数に"
                "渡っていない")
    for fc in filter_calls:
        p = parents.get(fc)
        while isinstance(p, ast.Await):
            p = parents.get(p)
        if isinstance(p, ast.Expr):
            problems.append(f"行{fc.lineno}: filter 戻り値が捨て置かれている")
            continue
        if isinstance(p, ast.Assign) and len(p.targets) == 1 \
                and isinstance(p.targets[0], ast.Name):
            var = p.targets[0].id
            used = any(isinstance(n, ast.Name) and n.id == var
                       and isinstance(n.ctx, ast.Load)
                       and getattr(n, "lineno", 0) > fc.lineno
                       for n in local)
            if not used:
                problems.append(
                    f"行{fc.lineno}: filter 結果 {var} が後続処理/return に"
                    "使用されていない")
        # Return / 呼出し引数などの式位置は「使用」
    if not searches:
        problems.append("manifest は search=filter だが App34 search が無い（stale）")
    return problems


def check_guard_discipline(fn, module_tree) -> list[str]:
    """(b) 関数単位: App34 get 結果が is_active_person（または照合閉集合）へ
    流入し、不成立分岐が Return/Raise/Continue へ到達することを検証。
    helper 形（return 式に is_active_person）は caller の If を 1 hop 解決"""
    local = list(_walk_local(fn))
    parents = _parent_map(fn)
    _searches, gets = _app34_calls(local)
    problems: list[str] = []
    if not gets:
        problems.append("manifest は get=guard だが App34 get が無い（stale）")
    for g in gets:
        p = _unwrap_await(g, parents)
        var = None
        direct = isinstance(p, ast.Call) \
            and _call_name(p) == "is_active_person"
        if not direct and isinstance(p, ast.Assign) and len(p.targets) == 1 \
                and isinstance(p.targets[0], ast.Name):
            var = p.targets[0].id
        elif not direct:
            problems.append(f"行{g.lineno}: get 結果の束縛形を判定できない")
            continue
        flows = direct or any(
            isinstance(n, ast.Call) and _call_name(n) in _GET_FLOW_SINKS
            and any(isinstance(a, ast.Name) and a.id == var for a in n.args)
            for n in local)
        if not flows:
            problems.append(
                f"行{g.lineno}: get 結果 {var} が状態確認/照合"
                f"（{'/'.join(_GET_FLOW_SINKS)}）へ流れていない")
    # 停止分岐: is_active_person 直接 or guard 変数（is_active_person を含む
    # 式の代入先）を test に持つ If の body が Return/Raise/Continue を含む
    guard_vars = set()
    for n in local:
        if isinstance(n, ast.Assign) and len(n.targets) == 1 \
                and isinstance(n.targets[0], ast.Name) \
                and any(isinstance(c, ast.Call)
                        and _call_name(c) == "is_active_person"
                        for c in ast.walk(n.value)):
            guard_vars.add(n.targets[0].id)

    def _test_hits(test) -> bool:
        for c in ast.walk(test):
            if isinstance(c, ast.Call) \
                    and _call_name(c) == "is_active_person":
                return True
            if isinstance(c, ast.Name) and c.id in guard_vars:
                return True
        return False

    def _terminating(if_node) -> bool:
        return any(isinstance(s, (ast.Return, ast.Raise, ast.Continue))
                   for stmt in if_node.body for s in ast.walk(stmt))

    guarded = any(isinstance(n, ast.If) and _test_hits(n.test)
                  and _terminating(n) for n in local)
    if not guarded:
        # helper 形: return 式に is_active_person → caller の If を 1 hop 解決
        helper_return = any(
            isinstance(n, ast.Return) and n.value is not None
            and any(isinstance(c, ast.Call)
                    and _call_name(c) == "is_active_person"
                    for c in ast.walk(n.value))
            for n in local)
        if helper_return:
            for n in ast.walk(module_tree):
                if isinstance(n, ast.If) and any(
                        isinstance(c, ast.Call) and _call_name(c) == fn.name
                        for c in ast.walk(n.test)) and _terminating(n):
                    guarded = True
                    break
    if not guarded:
        problems.append("不成立時の停止分岐（Return/Raise/Continue）が確認"
                        "できない")
    return problems


def _delete_refs(tree) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "delete_record":
            return True
        if isinstance(node, ast.Attribute) and node.attr == "delete_record":
            return True
        if isinstance(node, ast.ImportFrom) \
                and any(a.name == "delete_record" for a in node.names):
            return True
    return False


class TestStructuralPins(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.trees = {}
        cls.readers = {}       # (module, func) -> (fn_node, searches, gets)
        for rel, path in _tracked_modules():
            tree = ast.parse(path.read_text(encoding="utf-8"))
            cls.trees[rel] = tree
            for fname, entry in _reader_functions(tree).items():
                cls.readers[(rel, fname)] = entry

    def test_no_delete_record_anywhere(self):
        """留保1: delete_record の参照（Name/Attribute/import）は非テスト全 module
        で禁止——定義サイト hub/kintone.py のみ例外（物理削除経路の repo 全域 pin）"""
        offenders = sorted(rel for rel, tree in self.trees.items()
                           if _delete_refs(tree) and rel != "hub/kintone.py")
        self.assertEqual(offenders, [], "delete_record を参照する module:\n"
                         + "\n".join(offenders))

    def test_reader_manifest_is_exact_closure(self):
        """RV08-IMPL-06: App34 を read する関数の全数が manifest と完全一致する
        （未登録の新 reader 関数＝FAIL・実体の無い manifest エントリ＝FAIL。
        「同一 module 内の別の無防備 get 素通り」を関数単位の閉包で遮断）"""
        found = set(self.readers)
        listed = set(_APP34_READER_MANIFEST)
        self.assertEqual(
            sorted(found - listed), [],
            "manifest 未登録の App34 reader 関数（規律を決めて登録すること）:\n"
            + "\n".join(f"{m}:{f}" for m, f in sorted(found - listed)))
        self.assertEqual(
            sorted(listed - found), [],
            "実体の無い manifest エントリ（削除漏れ）:\n"
            + "\n".join(f"{m}:{f}" for m, f in sorted(listed - found)))

    def test_manifest_kinds_match_reads(self):
        """manifest の search/get 宣言が実際の読取種別と一致（宣言漏れ=FAIL。
        RV08-IMPL-09: 種別判定は over-approx 側=当該関数の全 read 呼出し）"""
        for key, spec in _APP34_READER_MANIFEST.items():
            _fn, _s, _g, reads = self.readers[key]
            has_search = any(_call_name(c) == "search_records" for c in reads)
            has_get = any(_call_name(c) == "get_record" for c in reads)
            if has_search:
                self.assertIn("search", spec,
                              f"{key}: search があるのに規律宣言なし")
            if has_get:
                self.assertIn("get", spec, f"{key}: get があるのに規律宣言なし")
            if "search" in spec:
                self.assertTrue(has_search, f"{key}: search 宣言が stale")
            if "get" in spec:
                self.assertTrue(has_get, f"{key}: get 宣言が stale")

    def test_filter_disciplines_hold(self):
        """(a) search=filter の各関数: filter Call＋戻り値使用（関数内 AST）"""
        for (rel, fname), spec in _APP34_READER_MANIFEST.items():
            if spec.get("search") != "filter":
                continue
            fn, _s, _g, _r = self.readers[(rel, fname)]
            self.assertEqual(check_filter_discipline(fn), [],
                             f"{rel}:{fname} の filter 規律違反")

    def test_guard_disciplines_hold(self):
        """(b) get=guard の各関数: is_active_person への流入＋停止分岐到達"""
        for (rel, fname), spec in _APP34_READER_MANIFEST.items():
            if spec.get("get") != "guard":
                continue
            fn, _s, _g, _r = self.readers[(rel, fname)]
            self.assertEqual(check_guard_discipline(fn, self.trees[rel]), [],
                             f"{rel}:{fname} の guard 規律違反")

    def test_exempt_entries_have_reasons(self):
        """exempt は理由必須（無根拠の免除を許さない）"""
        for key, spec in _APP34_READER_MANIFEST.items():
            if "exempt" in (spec.get("search"), spec.get("get")):
                self.assertTrue(str(spec.get("reason") or "").strip(),
                                f"{key}: exempt に理由がない")

    def test_koseki_sync_intentionally_unfiltered(self):
        """§10.2(iii): koseki_person_sync は filter を通さない（manifest で
        exempt 宣言済み）＋意図の規約コメントが残っていること"""
        self.assertEqual(
            _APP34_READER_MANIFEST[("koseki_person_sync.py",
                                    "_find_existing")]["search"], "exempt")
        self.assertIn("§10.2(iii)",
                      (REPO / "koseki_person_sync.py").read_text(
                          encoding="utf-8"),
                      "意図の明文化（規約コメント）")


class TestReaderCheckerNegatives(unittest.TestCase):
    """RV08-IMPL-06 の 2 穴を checker 単体の negative fixture で直接固定"""

    @staticmethod
    def _fn(src):
        tree = ast.parse(src)
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)))
        return fn, tree

    def test_hole1_filter_result_discarded_fails(self):
        """穴1a: filter を呼ぶが戻り値を捨て置く（式文）→ FAIL すること"""
        fn, _ = self._fn(
            "async def f():\n"
            "    records = await kintone.search_records(APP_KOSEKI_PERSON, q)\n"
            "    filter_active_persons(records)\n"
            "    return records\n")
        problems = check_filter_discipline(fn)
        self.assertTrue(any("捨て置かれている" in p for p in problems))

    def test_hole1_assigned_but_unused_fails(self):
        """穴1b: filter 結果を代入するが未使用 → FAIL すること"""
        fn, _ = self._fn(
            "async def f():\n"
            "    records = await kintone.search_records(APP_KOSEKI_PERSON, q)\n"
            "    filtered = filter_active_persons(records)\n"
            "    return records\n")
        problems = check_filter_discipline(fn)
        self.assertTrue(any("使用されていない" in p for p in problems))

    def test_hole1_search_not_reaching_filter_fails(self):
        """search 結果変数が filter の引数に渡らない → FAIL すること"""
        fn, _ = self._fn(
            "async def f():\n"
            "    records = await kintone.search_records(APP_KOSEKI_PERSON, q)\n"
            "    other = filter_active_persons(something_else)\n"
            "    return other + records\n")
        problems = check_filter_discipline(fn)
        self.assertTrue(any("渡っていない" in p for p in problems))

    def test_hole2_unlisted_reader_function_is_detected(self):
        """穴2a: 同一 module 内に無防備 get の別関数を追加 → reader 列挙が
        その関数を独立に検出する（manifest 閉包検査で FAIL になる形）"""
        tree = ast.parse(
            "async def guarded(pid):\n"
            "    rec = await kintone.get_record(APP_KOSEKI_PERSON, pid)\n"
            "    if not is_active_person(rec):\n"
            "        return None\n"
            "    return rec\n"
            "async def sneaky(pid):\n"
            "    return await kintone.get_record(APP_KOSEKI_PERSON, pid)\n")
        readers = _reader_functions(tree)
        self.assertIn("sneaky", readers, "無防備 get の別関数を独立検出")
        self.assertIn("guarded", readers)

    def test_hole2_extra_unguarded_get_in_guard_function_fails(self):
        """穴2b: guard 関数の内部に照合へ流れない get を追加 → FAIL すること"""
        fn, tree = self._fn(
            "async def f(pid, other):\n"
            "    rec = await kintone.get_record(APP_KOSEKI_PERSON, pid)\n"
            "    if not is_active_person(rec):\n"
            "        return None\n"
            "    extra = await kintone.get_record(APP_KOSEKI_PERSON, other)\n"
            "    return extra\n")
        problems = check_guard_discipline(fn, tree)
        self.assertTrue(any("流れていない" in p for p in problems))

    def test_guard_helper_one_hop_passes(self):
        """helper 形（return 式の is_active_person）＋caller の停止 If → PASS"""
        src = (
            "async def helper(pid):\n"
            "    rec = await kintone.get_record(APP_KOSEKI_PERSON, pid)\n"
            "    return not is_active_person(rec)\n"
            "async def caller(pid):\n"
            "    if await helper(pid):\n"
            "        return 'held'\n"
            "    return 'ok'\n")
        tree = ast.parse(src)
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.AsyncFunctionDef) and n.name == "helper")
        self.assertEqual(check_guard_discipline(fn, tree), [])

    def test_guard_without_terminating_branch_fails(self):
        """is_active_person は呼ぶが停止分岐が無い → FAIL すること"""
        fn, tree = self._fn(
            "async def f(pid):\n"
            "    rec = await kintone.get_record(APP_KOSEKI_PERSON, pid)\n"
            "    flag = is_active_person(rec)\n"
            "    print(flag)\n"
            "    return rec\n")
        problems = check_guard_discipline(fn, tree)
        self.assertTrue(any("停止分岐" in p for p in problems))

    def test_keyword_arg_form_is_detected(self):
        """RV08-IMPL-09 negative: keyword 引数（app=APP_KOSEKI_PERSON）でも
        reader として検出され、精密側の search にも計上されること"""
        tree = ast.parse(
            "async def f(q):\n"
            "    return await kintone.search_records(\n"
            "        app=APP_KOSEKI_PERSON, query=q)\n")
        readers = _reader_functions(tree)
        self.assertIn("f", readers)
        _fn, searches, _gets, _reads = readers["f"]
        self.assertEqual(len(searches), 1, "keyword 形も精密検査の対象")

    def test_alias_form_is_detected(self):
        """RV08-IMPL-09 negative: 単純 alias（変数代入 1 hop）経由でも
        reader として検出され、精密側の get にも計上されること"""
        tree = ast.parse(
            "async def f(rid):\n"
            "    target = APP_KOSEKI_PERSON\n"
            "    return await kintone.get_record(target, rid)\n")
        readers = _reader_functions(tree)
        self.assertIn("f", readers)
        _fn, _s, gets, _reads = readers["f"]
        self.assertEqual(len(gets), 1, "alias 1 hop も精密検査の対象")

    def test_overapprox_env_check_plus_other_app_read_is_reader(self):
        """RV08-IMPL-09: APP_KOSEKI_PERSON を env チェックでのみ参照し
        別 app を read する関数も（過大近似で）reader として検出＝manifest
        照合対象になること（取りこぼし方向を塞ぐ）"""
        tree = ast.parse(
            "async def f(rid):\n"
            "    if not APP_KOSEKI_PERSON.app_id():\n"
            "        return None\n"
            "    return await kintone.get_record(APP_KOSEKI_BOOK, rid)\n")
        readers = _reader_functions(tree)
        self.assertIn("f", readers)
        _fn, _s, gets, reads = readers["f"]
        self.assertEqual(len(gets), 0, "精密側は App34 read なし")
        self.assertEqual(len(reads), 1, "over-approx 側で read を計上")


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


class TestHeirDeriveInputExclusion(unittest.TestCase):
    """RV08-IMPL-03 併設 negative: 導出入力（heir_derive_task._pipeline）に
    無効化行が載らないことを挙動で実測（唯一の被相続人を無効化 → 導出は
    「被相続人 0 名」の非保存エラー経路へ落ちる＝入力から除外された証跡）"""

    def test_pipeline_excludes_inactive_from_inputs(self):
        import dispatch_bot.heir_derive_task as hd

        def rec(rid, name, decedent, state=None):
            r = person(rid, name, case="9")
            r["案件アプリID"] = _cell("SINGLE_LINE_TEXT", "26")
            r["生死区分"] = _cell("DROP_DOWN", "死亡" if decedent else "生存")
            r["死亡日"] = _cell("DATE", "2026-01-01" if decedent else "")
            r["被相続人フラグ"] = _cell("RADIO_BUTTON",
                                        "yes" if decedent else "no")
            if state is not None:
                r[MERGE_STATE_FIELD] = _cell("DROP_DOWN", state)
            return r

        records = [rec(7, "甲", False),
                   rec(8, "乙", True, state="統合済み無効")]   # 唯一の被相続人
        captured = {}
        orig = hd.persons_from_records

        def spy(recs):
            captured["ids"] = [str((r.get("$id") or {}).get("value") or "")
                               for r in recs]
            return orig(recs)

        env = {"APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34"}
        with patch.dict(os.environ, env), \
                patch("hub.kintone.search_records",
                      new=AsyncMock(return_value=records)), \
                patch.object(hd, "persons_from_records", new=spy):
            state = {"run": "failed:unexpected", "env": "skipped",
                     "run_id": None, "env_no": None}
            msg = run(hd._pipeline(state, "26", "9"))
        self.assertEqual(captured["ids"], ["7"],
                         "無効化行は導出入力（revisions/persons）に載らない")
        self.assertIn("導出エラー", msg, "被相続人が除外され 0 名＝非保存エラー")


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
        self.fail_next_upload_containing = None   # filename 部分一致で 1 回失敗
        self.fail_next_close = False              # 封筒クローズで 1 回失敗
        self.envelopes = {}     # 追加封筒（record_id -> 封筒 dict・reconcile 用）
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
            return self.envelopes.get(str(rid), self.envelope)
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
            if self.fail_next_close and fields.get("発送ステータス") == "完了":
                self.fail_next_close = False
                raise RuntimeError("close boom")
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
        if self.fail_next_upload_containing \
                and self.fail_next_upload_containing in filename:
            self.fail_next_upload_containing = None
            raise RuntimeError("upload boom")
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

    def test_postimage_attach_failure_then_resume(self):
        """RV08-IMPL-04: postimage 添付で失敗 → 封筒 open・preimage のみの
        open operation → 再実行が同一 operation_id で回収（App34 再書込みなし）"""
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="有効"))
        kt.fail_next_upload_containing = "_postimage"
        self._arm(kt)
        r1 = run(execute_merge(_cand()))
        self.assertEqual(r1["status"], "partial")
        self.assertIn("postimage添付", r1["reason"])
        self.assertEqual(self._envelope_status(kt), "要確認")
        self.assertEqual([r.stage for r in _journal_rows()], [STAGE_PREIMAGE])
        writes_before = list(kt.person_update_log)
        r2 = run(execute_merge(_cand()))
        self.assertEqual(r2["status"], "merged")
        self.assertEqual(r2["operation_id"], r1["operation_id"])
        self.assertEqual(kt.person_update_log, writes_before,
                         "全適用済み＝App34 への追加書き込みなし（skip）")
        post_names = [n for n in kt.filenames.values() if "_postimage" in n]
        self.assertEqual(len(post_names), 1, "postimage 添付は 1 回のみ")
        self.assertEqual(self._envelope_status(kt), "完了")
        self.assertEqual([r.stage for r in _journal_rows()],
                         [STAGE_PREIMAGE, STAGE_POSTIMAGE])

    def test_close_failure_then_resume(self):
        """RV08-IMPL-04: 封筒クローズで失敗 → 再実行は添付を二重化せず
        （封筒 成果物 の filename 判定）クローズと完了マークのみ実施"""
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="有効"))
        kt.fail_next_close = True
        self._arm(kt)
        r1 = run(execute_merge(_cand()))
        self.assertEqual(r1["status"], "partial")
        self.assertIn("封筒クローズ", r1["reason"])
        self.assertEqual([r.stage for r in _journal_rows()], [STAGE_PREIMAGE])
        r2 = run(execute_merge(_cand()))
        self.assertEqual(r2["status"], "merged")
        audit_names = [n for n in kt.filenames.values()
                       if n.endswith(".json") and "_postimage" not in n]
        post_names = [n for n in kt.filenames.values() if "_postimage" in n]
        self.assertEqual((len(audit_names), len(post_names)), (1, 1),
                         "監査/postimage とも添付は各 1 回のみ（二重防止）")
        self.assertEqual(self._envelope_status(kt), "完了")
        self.assertEqual([r.stage for r in _journal_rows()],
                         [STAGE_PREIMAGE, STAGE_POSTIMAGE])

    def _merge_with_flaky_postimage_row(self, kt):
        """postimage 台帳記録（完了マーク・最後尾）だけを 1 回失敗させて実行"""
        import person_merge_exec as pme
        from hub.person_merge_journal import MergeJournalError
        real = pme.record_stage
        state = {"failed": False}

        async def flaky(*a, **kw):
            if kw.get("stage") == STAGE_POSTIMAGE and not state["failed"]:
                state["failed"] = True
                raise MergeJournalError("boom")
            return await real(*a, **kw)

        p = patch("person_merge_exec.record_stage", new=flaky)
        p.start()
        self.addCleanup(p.stop)
        return run(execute_merge(_cand()))

    def test_postimage_row_failure_then_reconcile_completes(self):
        """RV08-IMPL-07: 完了マーク失敗（封筒 closed×台帳 open）→ reconcile が
        閉集合検証の成立を確認して同一 operation_id へ postimage を追記・完了化
        （App34/封筒への二重書込みゼロ・検知でなく回収）"""
        from person_merge_exec import reconcile_merge_operations
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="有効"))
        self._arm(kt)
        r1 = self._merge_with_flaky_postimage_row(kt)
        self.assertEqual(r1["status"], "merged")
        self.assertNotIn("warning", r1, "warning 方式は撤去（reconcile へ置換）")
        self.assertEqual(self._envelope_status(kt), "完了")
        self.assertEqual([r.stage for r in _journal_rows()], [STAGE_PREIMAGE])
        writes_before = list(kt.person_update_log)
        uploads_before = dict(kt.filenames)
        stats = run(reconcile_merge_operations())
        self.assertEqual((stats["walked"], stats["reconciled"],
                          stats["alerted"]), (1, 1, 0))
        rows = _journal_rows()
        self.assertEqual([r.stage for r in rows],
                         [STAGE_PREIMAGE, STAGE_POSTIMAGE])
        self.assertIs(rows[1].payload.get("reconciled"), True)
        self.assertEqual(rows[1].operation_id, r1["operation_id"],
                         "同一 operation_id への追記（新規発番しない）")
        self.assertEqual(kt.person_update_log, writes_before,
                         "App34 への書き込みゼロ")
        self.assertEqual(kt.filenames, uploads_before, "添付の二重化なし")
        stats2 = run(reconcile_merge_operations())
        self.assertEqual(stats2["walked"], 0, "回収済み＝open operation なし")

    def test_reconcile_mismatch_alerts_without_completion(self):
        """RV08-IMPL-07: 閉集合検証の不一致（第三者変更）→ 追記せず警報
        （要確認・沈黙しない・open のまま残置）"""
        import person_merge_exec as pme
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="有効"))
        self._arm(kt)
        r1 = self._merge_with_flaky_postimage_row(kt)
        self.assertEqual(r1["status"], "merged")
        kt.persons["6"]["備考"] = _cell("MULTI_LINE_TEXT", "クローズ後の編集")
        alert = AsyncMock()
        with patch.object(pme, "_alert_reconcile", new=alert):
            stats = run(pme.reconcile_merge_operations())
        self.assertEqual((stats["reconciled"], stats["alerted"]), (0, 1))
        alert.assert_awaited_once()
        self.assertEqual([r.stage for r in _journal_rows()], [STAGE_PREIMAGE],
                         "検証不一致では追記しない（open のまま）")

    def test_reconcile_skips_open_envelope(self):
        """封筒が open のままの部分失敗は通常 resume の管轄（reconcile 不干渉）"""
        from person_merge_exec import reconcile_merge_operations
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="有効"))
        kt.fail_next_update_on = "9"
        self._arm(kt)
        r1 = run(execute_merge(_cand()))
        self.assertEqual(r1["status"], "partial")
        stats = run(reconcile_merge_operations())
        self.assertEqual((stats["still_open"], stats["reconciled"],
                          stats["alerted"]), (1, 0, 0))
        self.assertEqual([r.stage for r in _journal_rows()], [STAGE_PREIMAGE])

    @staticmethod
    def _open_envelope_stub():
        return {"発送ステータス": {"value": "要確認"},
                "実行済み": {"value": "no"},
                "成果物": {"value": []},
                "チャネル固有データ": {"value": ""}}

    def test_paging_reaches_trailing_closed_operation(self):
        """RV08-IMPL-10 negative: open 封筒の operation が多数滞留していても、
        カーソルページング（縮小ページで複数回）により**後方**の
        「封筒 closed×台帳 open」へ必ず到達して回収されること＋滞留閾値超で
        業務警報が 1 回発されること"""
        import person_merge_exec as pme
        kt = _StatefulKT(person(6, "鈴木 誠", state="有効"),
                         person(9, "鈴木 誠", state="有効"))
        self._arm(kt)
        # 前方に open 封筒の未完了 operation を 5 件滞留させる
        for i in range(5):
            rid = str(801 + i)
            kt.envelopes[rid] = self._open_envelope_stub()
            run(record_stage(
                operation_id=f"bulk{i}", pair_key=f"person_merge:bulk{i}",
                envelope_record_id=rid, winner_id="6", loser_id="9",
                stage=STAGE_PREIMAGE,
                payload={"winner": {"id": "6", "pre": "0" * 64,
                                    "post": "0" * 64},
                         "loser": {"id": "9", "pre": "0" * 64,
                                   "post": "0" * 64},
                         "repoint": []}))
        # 後方に closed×open（完了マークだけ失敗した merge）を 1 件
        r1 = self._merge_with_flaky_postimage_row(kt)
        self.assertEqual(r1["status"], "merged")
        alert = AsyncMock(return_value=True)
        with patch.object(pme, "_RECONCILE_PAGE", 2), \
                patch.dict(os.environ,
                           {"MERGE_OPEN_OPS_ALERT_THRESHOLD": "3"}), \
                patch("hub.notify.notify_admin_line", new=alert):
            stats = run(pme.reconcile_merge_operations())
        self.assertEqual(stats["walked"], 6, "全未完了を有限回で走査完了")
        self.assertEqual(stats["still_open"], 5)
        self.assertEqual(stats["reconciled"], 1,
                         "後方の closed×open が回収される（打ち切りなし）")
        self.assertEqual(stats["alerted"], 0)
        alert.assert_awaited_once()   # 滞留閾値超の業務警報は 1 回（沈黙しない）
        self.assertIn("滞留", alert.await_args.args[0])
        post_rows = [r for r in _journal_rows() if r.stage == STAGE_POSTIMAGE]
        self.assertEqual(len(post_rows), 1)
        self.assertEqual(post_rows[0].operation_id, r1["operation_id"])

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


class _CliKT:
    """復元 CLI 用の stateful kintone モック（再実行の収束を実測する）"""

    def __init__(self, rows=None):
        self.records = {rid: rec for rid, rec in (rows or {}).items()}
        self.created = []
        self.updates = []
        self.fail_next_update_on = None
        self.next_id = 900

    async def get_record(self, app, rid):
        rec = self.records.get(str(rid))
        if rec is None:
            raise kintone.KintoneError(404, "GAIA_RE01", "not found")
        return rec

    async def search_records(self, app, query, fields=None):
        import re as _re
        m = _re.search(r'復元操作ID = "(.*?)"', query)
        if not m:
            return []
        op_id = m.group(1)
        return [{"$id": {"value": rid}}
                for rid, rec in sorted(self.records.items(),
                                       key=lambda kv: int(kv[0]))
                if str((rec.get("復元操作ID") or {})
                       .get("value") or "") == op_id]

    async def create_record(self, app, fields):
        self.next_id += 1
        rid = str(self.next_id)
        rec = {"$id": {"type": "__ID__", "value": rid}}
        for k, v in fields.items():
            rec[k] = {"type": "SUBTABLE" if isinstance(v, list)
                      else "SINGLE_LINE_TEXT", "value": v}
        self.records[rid] = rec
        self.created.append((rid, dict(fields)))
        return rid

    async def update_record(self, app, rid, fields, revision=None):
        rid = str(rid)
        if self.fail_next_update_on == rid:
            self.fail_next_update_on = None
            raise kintone.KintoneError(500, "boom", "update boom")
        self.updates.append((rid, dict(fields)))
        rec = self.records[rid]
        for k, v in fields.items():
            base = rec.get(k) or {}
            rec[k] = {"type": base.get("type") or "SINGLE_LINE_TEXT",
                      "value": v}

    def patches(self):
        return [patch(f"hub.kintone.{n}", new=getattr(self, n))
                for n in ("get_record", "search_records", "create_record",
                          "update_record")]


def _edge_row(rid, name, father="6"):
    row = person(rid, name)
    row["父人物ID"] = _cell("SINGLE_LINE_TEXT", father)
    return row


class TestRestoreCli(unittest.TestCase):
    def _write_audit(self, audit):
        d = tempfile.mkdtemp(prefix="rv08cli_")
        self.addCleanup(shutil.rmtree, d, True)
        path = os.path.join(d, "audit.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(audit, f, ensure_ascii=False)
        return path

    def _arm(self, kt):
        for p in [patch.dict(os.environ, _ENV), *kt.patches()]:
            p.start()
            self.addCleanup(p.stop)

    def test_operation_id_is_deterministic(self):
        """RV08-IMPL-01: 材料閉集合4値からの決定的構成（uuid 不使用）"""
        a = _legacy_audit()
        self.assertEqual(person_restore_cli.restore_operation_id(a),
                         person_restore_cli.restore_operation_id(
                             json.loads(json.dumps(a))))
        b = _legacy_audit()
        b["封筒レコードID"] = "91"
        self.assertNotEqual(person_restore_cli.restore_operation_id(a),
                            person_restore_cli.restore_operation_id(b))
        tree = ast.parse((REPO / "person_restore_cli.py")
                         .read_text(encoding="utf-8"))
        imported = {a.name.split(".")[0] for n in ast.walk(tree)
                    if isinstance(n, (ast.Import, ast.ImportFrom))
                    for a in n.names} | \
            {(n.module or "").split(".")[0] for n in ast.walk(tree)
             if isinstance(n, ast.ImportFrom)}
        self.assertNotIn("uuid", imported, "uuid 廃止（決定的構成のみ）")

    def test_dry_run_writes_nothing(self):
        kt = _CliKT({"12": _edge_row(12, "丙")})
        self._arm(kt)
        path = self._write_audit(_legacy_audit())
        person_restore_cli.main([path])
        self.assertEqual(kt.created, [])
        self.assertEqual(kt.updates, [])

    def test_execute_restores_and_relinks_and_journals(self):
        arm_db(self)
        kt = _CliKT({"12": _edge_row(12, "丙")})
        self._arm(kt)
        path = self._write_audit(_legacy_audit())
        person_restore_cli.main([path, "--execute"])
        self.assertEqual(len(kt.created), 1)
        new_id, payload = kt.created[0]
        self.assertEqual(payload["氏名"], "鈴木 誠")
        for admin in ("統合状態", "統合先人物ID", "統合日時"):
            self.assertNotIn(admin, payload, "復元行は有効行として作る")
        self.assertEqual(kt.updates, [("12", {"父人物ID": new_id})],
                         "親エッジの逆適用は新 ID へ")
        rows = _journal_rows()
        self.assertEqual([r.stage for r in rows],
                         [STAGE_PREIMAGE, STAGE_RESTORE],
                         "create 前の preimage 先行保存＋完了記録（RV08-IMPL-02）")
        self.assertEqual(rows[1].payload["restored_new_id"], new_id)

    def test_repeat_execute_creates_once(self):
        """RV08-IMPL-01: 同一監査JSONの連続 --execute で create 合計 1 回"""
        arm_db(self)
        kt = _CliKT({"12": _edge_row(12, "丙")})
        self._arm(kt)
        path = self._write_audit(_legacy_audit())
        person_restore_cli.main([path, "--execute"])
        person_restore_cli.main([path, "--execute"])
        self.assertEqual(len(kt.created), 1, "2回目は create 0（既存新IDへ収束）")
        self.assertEqual(len(kt.updates), 1, "再結線も再適用しない（skip）")

    def test_ack_loss_after_create_converges(self):
        """RV08-IMPL-02/05: create 後に完了記録が失敗（ACK 喪失）→ 再実行は
        復元操作ID の完全一致検索で作成済みを再利用（create 増分 0）"""
        arm_db(self)
        kt = _CliKT({"12": _edge_row(12, "丙")})
        self._arm(kt)
        path = self._write_audit(_legacy_audit())
        from hub.person_merge_journal import MergeJournalError
        real = person_restore_cli.insert_stage_in_session

        async def flaky(session, **kw):
            if kw.get("stage") == STAGE_RESTORE:
                raise MergeJournalError("boom")
            return await real(session, **kw)

        with patch("person_restore_cli.insert_stage_in_session", new=flaky):
            with self.assertRaises(SystemExit) as ctx:
                person_restore_cli.main([path, "--execute"])
        self.assertIn("既存復元へ収束", str(ctx.exception))
        self.assertEqual(len(kt.created), 1)
        person_restore_cli.main([path, "--execute"])   # 再実行（台帳は健全）
        self.assertEqual(len(kt.created), 1, "create 増分 0")
        self.assertEqual([r.stage for r in _journal_rows()],
                         [STAGE_PREIMAGE, STAGE_RESTORE], "完了記録へ収束")

    def test_ack_loss_never_adopts_same_name_same_content_record(self):
        """RV08-IMPL-05 negative（レビュー失敗例 No.700/No.901 の再現）:
        復元前から**同氏名・同内容**の人物 No.700 が存在する状態で create
        ACK 喪失しても、回収は復元操作ID の一致する No.901 のみを採用し
        No.700 へ誤って相乗りしない（create 合計 1 回・No.700 不触）"""
        arm_db(self)
        audit = _legacy_audit()
        payload = person_restore_cli.restore_payload_from_audit(audit)
        # No.700 = 同氏名・同内容（payload と fingerprint まで一致する複製）
        twin = {"$id": _cell("__ID__", "700")}
        for code, v in payload.items():
            twin[code] = {"type": "SUBTABLE" if isinstance(v, list)
                          else "SINGLE_LINE_TEXT", "value": v}
        kt = _CliKT({"700": twin, "12": _edge_row(12, "丙")})
        kt.next_id = 900   # create されるのは No.901
        self._arm(kt)
        path = self._write_audit(audit)
        from hub.person_merge_journal import MergeJournalError
        real = person_restore_cli.insert_stage_in_session

        async def flaky(session, **kw):
            if kw.get("stage") == STAGE_RESTORE:
                raise MergeJournalError("boom")
            return await real(session, **kw)

        with patch("person_restore_cli.insert_stage_in_session", new=flaky):
            with self.assertRaises(SystemExit):
                person_restore_cli.main([path, "--execute"])
        self.assertEqual([rid for rid, _f in kt.created], ["901"])
        person_restore_cli.main([path, "--execute"])   # 回収
        self.assertEqual(len(kt.created), 1, "create 増分 0（No.901 を再利用）")
        rows = _journal_rows()
        self.assertEqual(rows[-1].stage, STAGE_RESTORE)
        self.assertEqual(rows[-1].payload["restored_new_id"], "901",
                         "採用は復元操作ID 一致の No.901（No.700 ではない）")
        self.assertNotIn("700", [rid for rid, _f in kt.updates],
                         "No.700 には一切書かない")

    def test_concurrent_execute_serializes_to_single_create(self):
        """RV08-IMPL-08 negative（interleave 再現）: 並行 2 実行でも DB 直列化
        （sqlite=RESERVED ロック）により App34 create は合計 1 回。ロック保持側の
        完了後、待機側はロック下の再読で restore 行を観測し create 0 で収束"""
        arm_db(self)
        kt = _CliKT({"12": _edge_row(12, "丙")})
        self._arm(kt)
        audit = _legacy_audit()
        entered = asyncio.Event()
        gate = asyncio.Event()
        orig_create = kt.create_record
        creates = {"n": 0}

        async def slow_create(app, fields):
            creates["n"] += 1
            entered.set()          # A が臨界区間内（ロック保持中）に到達
            await gate.wait()      # B の開始を待ってから完了させる
            return await orig_create(app, fields)

        async def scenario():
            with patch("hub.kintone.create_record", new=slow_create):
                task_a = asyncio.create_task(
                    person_restore_cli._restore(
                        json.loads(json.dumps(audit)), True))
                await asyncio.wait_for(entered.wait(), timeout=10)
                task_b = asyncio.create_task(
                    person_restore_cli._restore(
                        json.loads(json.dumps(audit)), True))
                await asyncio.sleep(0.3)   # B を pending 観測→ロック待ちへ進める
                gate.set()
                await asyncio.wait_for(asyncio.gather(task_a, task_b),
                                       timeout=30)
        run(scenario())
        self.assertEqual(creates["n"], 1, "並行 2 実行でも create は合計 1 回")
        self.assertEqual(len(kt.created), 1)
        stages = [r.stage for r in _journal_rows()]
        self.assertEqual(stages, [STAGE_PREIMAGE, STAGE_RESTORE],
                         "restore 完了行は 1 行のみ（二重完了なし）")

    def test_partial_relink_failure_then_resume(self):
        """RV08-IMPL-02: 親エッジ途中失敗 → 再実行は適用済み skip・未適用のみ続行"""
        arm_db(self)
        kt = _CliKT({"12": _edge_row(12, "丙"), "13": _edge_row(13, "丁")})
        kt.fail_next_update_on = "13"
        self._arm(kt)
        audit = _legacy_audit()
        audit["参照付け替え"] = [
            {"person_record_id": "12", "fields": ["父人物ID"]},
            {"person_record_id": "13", "fields": ["父人物ID"]}]
        path = self._write_audit(audit)
        with self.assertRaises(SystemExit) as ctx:
            person_restore_cli.main([path, "--execute"])
        self.assertIn("再実行で回収", str(ctx.exception))
        person_restore_cli.main([path, "--execute"])
        self.assertEqual(len(kt.created), 1, "create 合計 1 回")
        rids = [rid for rid, _f in kt.updates]
        self.assertEqual(sorted(rids), ["12", "13"],
                         "12 は初回適用済み skip・13 のみ再実行で適用")
        self.assertEqual([r.stage for r in _journal_rows()],
                         [STAGE_PREIMAGE, STAGE_RESTORE])

    def test_db_unavailable_no_create(self):
        """RV08-IMPL-02: DB 利用不能 → App34 create/update 0（fail-closed）"""
        kt = _CliKT({"12": _edge_row(12, "丙")})
        self._arm(kt)
        p = patch.dict(os.environ, {"DATABASE_URL": ""})
        p.start()
        self.addCleanup(p.stop)
        db.reset_for_tests()
        path = self._write_audit(_legacy_audit())
        with self.assertRaises(SystemExit) as ctx:
            person_restore_cli.main([path, "--execute"])
        self.assertIn("操作台帳", str(ctx.exception))
        self.assertEqual(kt.created, [])
        self.assertEqual(kt.updates, [])

    def test_migration_missing_no_create(self):
        """RV08-IMPL-02: migration 未適用（テーブル不在）→ create/update 0"""
        kt = _CliKT({"12": _edge_row(12, "丙")})
        self._arm(kt)
        d = tempfile.mkdtemp(prefix="rv08nomig_")
        self.addCleanup(shutil.rmtree, d, True)
        p = patch.dict(os.environ,
                       {"DATABASE_URL": f"sqlite+aiosqlite:///{d}/empty.db"})
        p.start()
        self.addCleanup(p.stop)
        db.reset_for_tests()
        self.addCleanup(db.reset_for_tests)
        path = self._write_audit(_legacy_audit())
        with self.assertRaises(SystemExit) as ctx:
            person_restore_cli.main([path, "--execute"])
        self.assertIn("操作台帳", str(ctx.exception))
        self.assertEqual(kt.created, [])
        self.assertEqual(kt.updates, [])

    def test_pending_with_changed_audit_writes_nothing(self):
        """RV08-IMPL-01: pending と監査JSON内容の不一致 → write 0 要確認"""
        arm_db(self)
        kt = _CliKT({"12": _edge_row(12, "丙")})
        self._arm(kt)
        audit = _legacy_audit()
        op_id = person_restore_cli.restore_operation_id(audit)
        run(record_stage(operation_id=op_id, pair_key="person_merge:6-9",
                         envelope_record_id="90", winner_id="6", loser_id="9",
                         stage=STAGE_PREIMAGE,
                         payload={"payload_fp": "0" * 64, "old_id": "9",
                                  "relink_plan": []}))
        path = self._write_audit(audit)
        with self.assertRaises(SystemExit) as ctx:
            person_restore_cli.main([path, "--execute"])
        self.assertIn("一致しません", str(ctx.exception))
        self.assertEqual(kt.created, [])
        self.assertEqual(kt.updates, [])

    def test_execute_skips_third_party_changed_edge(self):
        arm_db(self)
        kt = _CliKT({"12": _edge_row(12, "丙", father="55")})   # 第三者変更
        self._arm(kt)
        path = self._write_audit(_legacy_audit())
        person_restore_cli.main([path, "--execute"])
        self.assertEqual(kt.updates, [], "第三者変更エッジは write 0")
        self.assertEqual(len(kt.created), 1)

    def test_soft_merge_audit_rejected(self):
        audit = _legacy_audit()
        audit["統合方式"] = "soft_merge"
        path = self._write_audit(audit)
        with patch.dict(os.environ, _ENV):
            with self.assertRaises(SystemExit):
                person_restore_cli.main([path, "--execute"])

    def test_stdout_never_contains_pii(self):
        """留保2: 氏名等を混入させた fixture で captured stdout に PII が
        出ないことを直接固定（dry-run／--execute の両経路）"""
        import contextlib
        import io
        sentinel = "機密復元PII太郎SENTINEL"
        audit = _legacy_audit()
        audit["敗者レコード"]["氏名"] = _cell("SINGLE_LINE_TEXT", sentinel)
        audit["敗者レコード"]["備考"] = _cell("MULTI_LINE_TEXT",
                                              f"{sentinel}の旧戸籍")
        arm_db(self)
        kt = _CliKT({"12": _edge_row(12, "丙")})
        self._arm(kt)
        path = self._write_audit(audit)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            person_restore_cli.main([path])                 # dry-run
            person_restore_cli.main([path, "--execute"])    # 実書き込み
        text = out.getvalue()
        self.assertNotIn(sentinel, text, "stdout に PII を出さない")
        self.assertIn("RESTORED", text)
        self.assertEqual(len(kt.created), 1)


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
