"""TEXT-APPROVAL-GATE-1（QUEUE-2026-10-20-HI H-0）: 要承認文言の直接参照を AST で禁止する機械検査。

規則（本ファイルの checker が単一の正・scripts/audit_static.py (f) は同じ規則の自己完結コピー＝merge 後に共通化）:
  1. 文言表 = `hub/*_texts.py`（および repo 直下の `*_texts.py`）。status が needs_approval の ID を抽出する
     （dict 直書き `{"status": STATUS_NEEDS_APPROVAL|"needs_approval"}` と `_t(..., status=STATUS_NEEDS_APPROVAL)` の両形）。
  2. 文言表を import する非テスト module の **関数本体**で、その表の `text(<ID>)`／`entry(<ID>)`／`TEXTS[<ID>]`／
     `TEXTS.get(<ID>)` を **ID 定数**で呼ぶのは「直接参照」＝違反。
  3. 例外: 呼出が `approved_or_safe(...)`／`approved_text(...)`／`gate_and_notify(...)`（hub/approved_texts の表示ゲート）の
     引数の内側にある／module が Q4 経路（人による承認）に属する／module が文言表自身・approved_texts・scripts・tests。
  4. 変数経由（`text(i)`）・module レベルの代入・`is_selectable` 等の判定関数は対象外（選択器は status で fail-closed）。

期待: feature/souzoku-2 の修正前（souzoku_intake_form が T.text("S-1") を直接描画）は不合格、修正後は合格。
既存の Q4 経由（P-1〜3 は hub/jikou_progress＝Q4 経路 module・Q2 定型は選択器が status で選ぶ）は合格。
"""

import ast
import os
import shutil
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent

NEEDS_APPROVAL = "needs_approval"
TEXT_ACCESSORS = frozenset({"text", "entry", "get"})
GATE_FUNCS = frozenset({"approved_or_safe", "approved_text", "gate_and_notify"})
Q4_PATH_MODULES = frozenset({
    "hub/reply_draft.py", "hub/reply_draft_approval.py", "hub/jikou_progress.py", "hub/webapp_drafts_view.py",
})
EXEMPT_MODULES = frozenset({"hub/approved_texts.py"})
SCAN_DIRS = ("hub", "houki_bot", "channels", ".")


def _texts_modules(repo: Path) -> list[Path]:
    out = sorted((repo / "hub").glob("*_texts.py")) if (repo / "hub").exists() else []
    out += sorted(p for p in repo.glob("*_texts.py"))
    return out


def _const_str(node) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _is_needs_approval(node) -> bool:
    if isinstance(node, ast.Constant):
        return node.value == NEEDS_APPROVAL
    if isinstance(node, ast.Name):
        return node.id == "STATUS_NEEDS_APPROVAL"
    if isinstance(node, ast.Attribute):
        return node.attr == "STATUS_NEEDS_APPROVAL"
    return False


def needs_approval_ids(src: str) -> set[str]:
    """文言表 module のソースから status=needs_approval の ID を抽出する。"""
    tree = ast.parse(src)
    ids: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for k, v in zip(node.keys, node.values):
            tid = _const_str(k)
            if tid is None:
                continue
            if isinstance(v, ast.Dict):
                for kk, vv in zip(v.keys, v.values):
                    if _const_str(kk) == "status" and _is_needs_approval(vv):
                        ids.add(tid)
            elif isinstance(v, ast.Call):
                for kw in v.keywords:
                    if kw.arg == "status" and _is_needs_approval(kw.value):
                        ids.add(tid)
    return ids


def _texts_bindings(tree: ast.AST, module_names: set[str]) -> tuple[set[str], set[str]]:
    """module 内で文言表に束縛された名前（alias）と、表から直接 import された accessor 名を返す。"""
    aliases: set[str] = set()
    direct: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                base = a.name.split(".")[-1]
                if base in module_names:
                    aliases.add(a.asname or a.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            mod = (node.module or "").split(".")
            if mod and mod[-1] in module_names:
                for a in node.names:
                    if a.name in TEXT_ACCESSORS or a.name == "TEXTS":
                        direct.add(a.asname or a.name)
            elif mod == ["hub"] or node.module == "hub":
                for a in node.names:
                    if a.name in module_names:
                        aliases.add(a.asname or a.name)
    return aliases, direct


def _accessor_call_id(node: ast.AST, aliases: set[str], direct: set[str]) -> str | None:
    """`T.text("S-1")`／`text("S-1")`／`T.TEXTS["S-1"]`／`T.TEXTS.get("S-1")` なら ID を返す。"""
    if isinstance(node, ast.Call) and node.args:
        f = node.func
        if isinstance(f, ast.Attribute) and f.attr in TEXT_ACCESSORS:
            owner = f.value
            if isinstance(owner, ast.Name) and owner.id in aliases:
                return _const_str(node.args[0])
            if isinstance(owner, ast.Attribute) and owner.attr == "TEXTS" and \
                    isinstance(owner.value, ast.Name) and owner.value.id in aliases:
                return _const_str(node.args[0])
            if isinstance(owner, ast.Name) and owner.id == "TEXTS" and "TEXTS" in direct:
                return _const_str(node.args[0])
        if isinstance(f, ast.Name) and f.id in direct and f.id in TEXT_ACCESSORS:
            return _const_str(node.args[0])
    if isinstance(node, ast.Subscript):
        v = node.value
        if (isinstance(v, ast.Attribute) and v.attr == "TEXTS" and isinstance(v.value, ast.Name)
                and v.value.id in aliases) or (isinstance(v, ast.Name) and v.id == "TEXTS" and "TEXTS" in direct):
            return _const_str(node.slice)
    return None


def _is_gate_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    f = node.func
    name = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
    return name in GATE_FUNCS


def find_violations_in_source(rel: str, src: str, ids_by_module: dict[str, set[str]]) -> list[tuple[str, int, str]]:
    """1 ファイルの違反 [(rel, lineno, text_id)]。ids_by_module: 文言表 module 名（hub を除く basename）→ ID 集合。"""
    tree = ast.parse(src)
    module_names = set(ids_by_module)
    aliases, direct = _texts_bindings(tree, module_names)
    if not aliases and not direct:
        return []
    all_ids: set[str] = set().union(*ids_by_module.values()) if ids_by_module else set()
    out: list[tuple[str, int, str]] = []

    def visit(node: ast.AST, in_func: bool, under_gate: bool):
        is_func = isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        tid = _accessor_call_id(node, aliases, direct) if in_func else None
        if tid is not None and tid in all_ids and not under_gate:
            out.append((rel, node.lineno, tid))
        gate = under_gate or _is_gate_call(node)
        for child in ast.iter_child_nodes(node):
            visit(child, in_func or is_func, gate)

    visit(tree, False, False)
    return out


def scan_repo(repo: Path) -> list[tuple[str, int, str]]:
    repo = repo.resolve()
    ids_by_module: dict[str, set[str]] = {}
    texts_rel: set[str] = set()
    for p in _texts_modules(repo):
        rel = p.relative_to(repo).as_posix()
        texts_rel.add(rel)
        ids = needs_approval_ids(p.read_text(encoding="utf-8", errors="replace"))
        if ids:
            ids_by_module[p.stem] = ids
    if not ids_by_module:
        return []
    violations: list[tuple[str, int, str]] = []
    for d in SCAN_DIRS:
        base = repo / d
        if not base.is_dir():
            continue
        for p in sorted(base.glob("*.py")):
            rel = p.relative_to(repo).as_posix()
            if (rel.startswith("test_") or rel.startswith("scripts/") or rel in texts_rel
                    or rel in EXEMPT_MODULES or rel in Q4_PATH_MODULES):
                continue
            src = p.read_text(encoding="utf-8", errors="replace")
            try:
                violations += find_violations_in_source(rel, src, ids_by_module)
            except SyntaxError:
                continue
    return violations


# ══════════════════════════════════════════════════════════════
# テスト
# ══════════════════════════════════════════════════════════════

TEXTS_SRC = '''
STATUS_NEEDS_APPROVAL = "needs_approval"
STATUS_APPROVED = "approved"
TEXTS = {
    "S-1": {"label": "a", "status": STATUS_NEEDS_APPROVAL, "text": "未承認の文"},
    "S-9": {"label": "b", "status": STATUS_APPROVED, "text": "承認済みの文"},
}
def text(text_id):
    return TEXTS[text_id]["text"]
def entry(text_id):
    return TEXTS.get(text_id)
'''

TEXTS_T_SRC = '''
STATUS_NEEDS_APPROVAL = "needs_approval"
STATUS_FROZEN_V14 = "frozen_v14"
def _t(text, *, label, status):
    return {"text": text, "label": label, "status": status}
TEXTS = {
    "J-0": _t("x", label="a", status=STATUS_NEEDS_APPROVAL),
    "J-1": _t("y", label="b", status=STATUS_FROZEN_V14),
}
def text(text_id):
    return TEXTS[text_id]["text"]
'''


class _Repo(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="tag_"))
        (self.dir / "hub").mkdir()
        (self.dir / "hub" / "__init__.py").write_text("", encoding="utf-8")
        self.addCleanup(lambda: shutil.rmtree(self.dir, ignore_errors=True))

    def put(self, rel: str, src: str) -> None:
        p = self.dir / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(src, encoding="utf-8")


class TestNeedsApprovalIds(_Repo):
    def test_dict_form_and_t_form(self):
        self.assertEqual(needs_approval_ids(TEXTS_SRC), {"S-1"})
        self.assertEqual(needs_approval_ids(TEXTS_T_SRC), {"J-0"})


class TestDirectReferenceIsViolation(_Repo):
    def setUp(self):
        super().setUp()
        self.put("hub/souzoku_intake_texts.py", TEXTS_SRC)

    def test_render_function_referencing_unapproved_id_fails(self):
        # souzoku-2 修正前の形（T.text("S-1") を描画関数で直接参照）
        self.put("souzoku_intake_form.py",
                 "from hub import souzoku_intake_texts as T\n"
                 "def _result_html():\n    return '<p>' + T.text('S-1') + '</p>'\n")
        v = scan_repo(self.dir)
        self.assertEqual([(f, i) for f, _, i in v], [("souzoku_intake_form.py", "S-1")])

    def test_subscript_and_get_forms_fail(self):
        self.put("hub/render.py",
                 "from hub import souzoku_intake_texts as T\n"
                 "def a():\n    return T.TEXTS['S-1']['text']\n"
                 "def b():\n    return T.TEXTS.get('S-1')\n"
                 "def c():\n    return T.entry('S-1')\n")
        self.assertEqual(sorted(i for _, _, i in scan_repo(self.dir)), ["S-1", "S-1", "S-1"])

    def test_direct_import_of_accessor_fails(self):
        self.put("hub/render.py",
                 "from hub.souzoku_intake_texts import text\n"
                 "def a():\n    return text('S-1')\n")
        self.assertEqual(len(scan_repo(self.dir)), 1)


class TestAllowedForms(_Repo):
    def setUp(self):
        super().setUp()
        self.put("hub/souzoku_intake_texts.py", TEXTS_SRC)

    def test_gate_wrapped_passes(self):
        # souzoku-2 修正後の形（表示ゲート経由）
        self.put("souzoku_intake_form.py",
                 "from hub import souzoku_intake_texts as T\n"
                 "from hub import approved_texts\n"
                 "def _result_html():\n    return approved_texts.approved_or_safe('S-1', T.text('S-1'))\n"
                 "async def h():\n    return await approved_texts.gate_and_notify('S-1', T.text('S-1'))\n")
        self.assertEqual(scan_repo(self.dir), [])

    def test_approved_id_is_not_flagged(self):
        self.put("souzoku_intake_form.py",
                 "from hub import souzoku_intake_texts as T\n"
                 "def _result_html():\n    return T.text('S-9')\n")
        self.assertEqual(scan_repo(self.dir), [])

    def test_q4_path_module_passes(self):
        self.put("hub/jikou_progress.py",
                 "from hub import souzoku_intake_texts as T\n"
                 "def build():\n    return T.text('S-1')\n")
        self.assertEqual(scan_repo(self.dir), [])

    def test_variable_and_module_level_pass(self):
        # 選択器のように変数経由で渡す形・module レベルの配線は対象外（status で fail-closed）
        self.put("hub/select.py",
                 "from hub import souzoku_intake_texts as T\n"
                 "ID = 'S-1'\nX = T.text('S-1')\n"
                 "def pick(i):\n    return T.text(i)\n")
        self.assertEqual(scan_repo(self.dir), [])

    def test_module_without_texts_import_is_ignored(self):
        self.put("hub/other.py", "def a():\n    return 'S-1'\n")
        self.assertEqual(scan_repo(self.dir), [])

    def test_tests_and_scripts_are_ignored(self):
        self.put("test_x.py", "from hub import souzoku_intake_texts as T\ndef t():\n    return T.text('S-1')\n")
        self.put("scripts/make.py", "from hub import souzoku_intake_texts as T\ndef t():\n    return T.text('S-1')\n")
        self.assertEqual(scan_repo(self.dir), [])


class TestThisRepo(unittest.TestCase):
    def test_repo_has_no_direct_reference_to_unapproved_texts(self):
        v = scan_repo(REPO)
        self.assertEqual(v, [], "未承認文言の直接参照（approved_texts の表示ゲートを通すこと）: "
                               + "; ".join(f"{f}:{n} {i}" for f, n, i in v))


if __name__ == "__main__":
    unittest.main()
