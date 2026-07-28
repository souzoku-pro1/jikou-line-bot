"""P4-002: hub/webapp_case_views（案件一覧＋詳細・read-only proxy）のテスト。

固定する仕様（DRAFT_P4 §2＋裁定/司令塔既定 2026-07-28）:
- 全 route が P4-001 の関所（_gate）必須・公開例外なし（機械検査）
- read-only: kintone 書込み API の呼出しゼロ（AST 機械検査）
- 一覧=App21 検索（status 絞込は schema 実選択肢の閉集合・更新順・
  ページング上限50/既定20・不正は固定 400 非反射）
- 詳細=App21 単票＋App30 案件絞込＋App28 件数のみ（本文非取得・fields=$id 限定・
  line_user_id は grammar 検証済みのみ query へ埋める）
- catch-all（webapp_auth）より先の結線（authed でページが 200 になることで pin）
"""

import ast
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "d",
    "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import hub.kintone as hub_kintone  # noqa: E402
import hub.webapp_case_views as cv  # noqa: E402
from hub.webapp_auth import (  # noqa: E402
    MIN_ITERATIONS,
    PUBLIC_ROUTES,
    hash_password,
    issue_session,
)

_client = TestClient(main.app)
_ENV = {
    "WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS),
    "WEBAPP_SESSION_SECRET": "s" * 32,
}
_LUID = "U" + "a" * 32

_ROUTES = ("/app/api/cases", "/app/api/cases/1", "/app/cases", "/app/case")


def _auth_headers():
    return {"Cookie": f"webapp_session={issue_session()}"}


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


class TestAuthBoundary(unittest.TestCase):
    def test_unauthenticated_all_rejected(self):
        with patch.dict(os.environ, _ENV):
            for path in _ROUTES:
                with self.subTest(path=path):
                    r = _client.get(path, follow_redirects=False)
                    self.assertEqual(r.status_code, 303)
                    self.assertEqual(r.headers["location"], "/app/login")

    def test_all_routes_gated_no_public_exception(self):
        routes = [r for r in cv.router.routes if hasattr(r, "endpoint")]
        self.assertGreaterEqual(len(routes), 4)
        for route in routes:
            for method in route.methods:
                with self.subTest(path=route.path, method=method):
                    self.assertNotIn((route.path, method), PUBLIC_ROUTES)
                    self.assertTrue(
                        getattr(route.endpoint, "__webapp_gate__", False),
                        f"{method} {route.path} に認証関所（_gate）がない")


# ── read-only AST 検査（fix2 H01 で関数化・alias 完全追跡）─────────────────────

_ALLOWED_KINTONE_ATTRS = {"KintoneApp", "search_records", "get_record"}
_BANNED_DYNAMIC = {"getattr", "setattr", "__import__", "eval", "exec",
                   "import_module"}
# 直接 HTTP client に加え subprocess/os も遮断（curl 等の外部プロセス経由 HTTP を
# 「対象外」にせず、プロセス起動の import 自体を禁止して塞ぐ・fix2 H01-4）。
# fix3 H01-4: operator（attrgetter=動的属性アクセス）・asyncio
# （create_subprocess_exec 系=プロセス起動）も「対象外」とせず入口遮断へ追加
# （本 module は async def のみで asyncio API を必要としない）。
_FORBIDDEN_IMPORTS = {"httpx", "requests", "urllib", "aiohttp", "http",
                      "importlib", "subprocess", "os", "operator", "asyncio"}
_WRITE_VERBS = {"post", "put", "delete", "patch", "request"}


def _names_in(target) -> list[str]:
    return [n.id for n in ast.walk(target) if isinstance(n, ast.Name)]


def _assign_pairs(target, value) -> list[tuple[str, ast.AST]]:
    """代入 target と value の (名前, 対応式) 対。tuple/list unpack は要素対応・
    形が合わない unpack は保守的に「全 target 名 × value 全体」。"""
    if isinstance(target, ast.Name):
        return [(target.id, value)]
    if isinstance(target, ast.Starred):
        return _assign_pairs(target.value, value)
    if isinstance(target, (ast.Tuple, ast.List)):
        if isinstance(value, (ast.Tuple, ast.List)) \
                and len(value.elts) == len(target.elts):
            out = []
            for t, v in zip(target.elts, value.elts):
                out += _assign_pairs(t, v)
            return out
        return [(name, value) for t in target.elts for name in _names_in(t)]
    return []


def _is_alias_of(value, names: set) -> bool:
    """value が names の別名になる式か（Name そのもの／Name 起点の Attribute 連鎖。
    Call の戻り値は別名ではない=KintoneApp(...) 等を誤検出しない）。"""
    node = value
    while isinstance(node, ast.Attribute):
        node = node.value
    return isinstance(node, ast.Name) and node.id in names


def _contains_alias_of(value, names: set) -> bool:
    return any(_is_alias_of(sub, names)
               for sub in ast.walk(value) if isinstance(sub, (ast.Name,
                                                              ast.Attribute)))


def _readonly_violations_legacy(tree) -> list[str]:
    """fix1 時点の検査（**対照用の凍結コピー・変更しない**）。
    迂回 fixture が旧 PASS/新 FAIL となる三段対照（lineq G0 型）の基準。"""
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            mod = getattr(node, "module", "") or ""
            for a in node.names:
                if ("kintone" in (a.name or "") or "kintone" in mod) \
                        and a.asname is not None:
                    violations.append(f"import alias: {a.asname}")
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
            value = node.value
            if isinstance(value, ast.Name) and value.id == "kintone":
                violations.append("module alias 代入")
            if isinstance(value, ast.Attribute) \
                    and isinstance(value.value, ast.Name) \
                    and value.value.id == "kintone":
                violations.append(f"属性 alias 代入: {value.attr}")
        if isinstance(node, ast.Call):
            f = node.func
            name = f.id if isinstance(f, ast.Name) else (
                f.attr if isinstance(f, ast.Attribute) else "")
            if name in _BANNED_DYNAMIC:
                violations.append(f"動的呼出し: {name}")
    return violations


def _readonly_violations_fix2(tree) -> list[str]:
    """fix2 H01 時点の検査（**対照用の凍結コピー・変更しない**）。

    fix3 メタテストの基準: Attribute/Subscript 代入先への持ち出し・
    builtins.getattr 型の Attribute 連鎖 alias を検出できない版。
    """
    violations = []
    poison = {"kintone"}
    banned = set(_BANNED_DYNAMIC)

    for node in ast.walk(tree):          # import 系（1 パスで確定）
        if isinstance(node, ast.Import):
            for a in node.names:
                top = a.name.split(".")[0]
                if top in _FORBIDDEN_IMPORTS:
                    violations.append(f"禁止 import: {a.name}")
                if "kintone" in a.name:
                    bound = a.asname or top
                    poison.add(bound)
                    if a.asname is not None:
                        violations.append(f"kintone import alias: {a.asname}")
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod.split(".")[0] in _FORBIDDEN_IMPORTS:
                violations.append(f"禁止 module からの import: {mod}")
            if mod.endswith("kintone"):
                violations.append(f"kintone の属性直接 import: {mod}")
            for a in node.names:
                if a.name in _BANNED_DYNAMIC:
                    violations.append(f"禁止関数 import: {a.name}")
                    banned.add(a.asname or a.name)
                if a.name == "kintone":
                    poison.add(a.asname or a.name)
                    if a.asname is not None:
                        violations.append(f"kintone import alias: {a.asname}")

    changed = True                       # 代入 alias の固定点反復
    while changed:
        changed = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                pairs = []
                for tgt in node.targets:
                    pairs += _assign_pairs(tgt, node.value)
            elif isinstance(node, (ast.AnnAssign, ast.NamedExpr)):
                if getattr(node, "value", None) is None:
                    continue
                pairs = _assign_pairs(node.target, node.value)
            else:
                continue
            for name, value in pairs:
                poisoned = (_is_alias_of(value, poison)
                            or (not isinstance(value, (ast.Name, ast.Attribute))
                                and _contains_alias_of(value, poison)
                                and isinstance(value, (ast.Tuple, ast.List))))
                if poisoned and name not in poison:
                    poison.add(name)
                    violations.append(f"kintone alias 代入: {name}")
                    changed = True
                if isinstance(value, ast.Name) and value.id in banned \
                        and name not in banned:
                    banned.add(name)
                    violations.append(f"禁止関数 alias 代入: {name}")
                    changed = True

    for node in ast.walk(tree):          # 使用箇所（poison/banned 確定後）
        if isinstance(node, ast.Call):
            f = node.func
            name = f.id if isinstance(f, ast.Name) else (
                f.attr if isinstance(f, ast.Attribute) else "")
            if name in banned:
                violations.append(f"動的呼出し: {name}")
            if isinstance(f, ast.Attribute) and f.attr in _WRITE_VERBS:
                violations.append(f"HTTP 書込み動詞呼出し: {f.attr}")
        if isinstance(node, ast.Attribute) \
                and isinstance(node.value, ast.Name) \
                and node.value.id in poison \
                and node.attr not in _ALLOWED_KINTONE_ATTRS:
            violations.append(f"poison 属性参照: {node.attr}")
    return violations


def _extra_guard_violations(tree) -> list[str]:
    """fix3 H01 の追加規則（fix2 検査への上乗せ・代入先種類非依存の遮断）。

    - R1: RHS が poison/禁止関数の別名式（直接 Name・Attribute 連鎖）である代入は、
      **代入先の種類にかかわらず違反**——Attribute（box.kt = kintone）・Subscript
      （holder["kt"] = kintone）は追跡せず、その代入自体を禁止（安全側）。
    - R2: RHS 内の Call の**引数**に poison/禁止関数の別名式が含まれる代入も違反
      （呼出し戻り値経由の持ち出し禁止。kintone.KintoneApp("...") のように
      引数が非 hazard の呼出しは対象外=本体 module の正当パターンを誤検出しない）。
    - R3: **Attribute 連鎖の末尾が禁止関数名**（builtins.getattr 等）の式は
      alias 生成（どの代入先でも）・呼出しとも違反。Name target への束縛は
      固定点で追跡し、その別名の呼出しも違反化。
    """
    violations = []
    poison = {"kintone"}
    extra_banned: set[str] = set()

    def _is_banned_expr(v) -> bool:
        if isinstance(v, ast.Name) and (v.id in _BANNED_DYNAMIC
                                        or v.id in extra_banned):
            return True
        return isinstance(v, ast.Attribute) and v.attr in _BANNED_DYNAMIC

    def _contains_hazard(v) -> bool:
        for sub in ast.walk(v):
            if isinstance(sub, ast.Name) and sub.id in poison:
                return True
            if _is_banned_expr(sub):
                return True
        return False

    def _assign_nodes():
        for node in ast.walk(tree):
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr,
                                 ast.AugAssign)):
                value = getattr(node, "value", None)
                if value is None:
                    continue
                targets = node.targets if isinstance(node, ast.Assign) \
                    else [node.target]
                yield node, targets, value

    changed = True                       # R3: banned 別名の固定点追跡
    while changed:
        changed = False
        for _, targets, value in _assign_nodes():
            if not _is_banned_expr(value):
                continue
            for t in targets:
                if isinstance(t, ast.Name) and t.id not in extra_banned:
                    extra_banned.add(t.id)
                    changed = True

    for _, targets, value in _assign_nodes():
        if _is_banned_expr(value):       # R3: alias 生成（代入先の種類を問わない）
            violations.append("禁止関数の alias 生成（Attribute 連鎖含む）")
        non_trackable = [t for t in targets
                         if not isinstance(t, (ast.Name, ast.Tuple, ast.List,
                                               ast.Starred))]
        if non_trackable and _contains_hazard(value):   # R1
            violations.append("Attribute/Subscript 代入先への hazard 持ち出し")
        for call in (s for s in ast.walk(value) if isinstance(s, ast.Call)):
            if any(_contains_hazard(a) for a in
                   list(call.args) + [k.value for k in call.keywords]):   # R2
                violations.append("hazard を引数に渡す呼出し戻り値の代入")
                break

    for node in ast.walk(tree):          # R3: banned 別名の呼出し
        if isinstance(node, ast.Call):
            f = node.func
            if isinstance(f, ast.Name) and f.id in extra_banned:
                violations.append(f"banned 別名の呼出し: {f.id}")
    return violations


def _readonly_violations_fix3(tree) -> list[str]:
    """fix3 H01 時点の検査（**対照用の凍結コピー・変更しない**）。
    禁止形の列挙方式の最終版=条件式（IfExp/BoolOp）・コンテナ（Dict 等）・
    関数引数経由の静的迂回を検出できない版（fix4 メタテストの基準）。"""
    return _readonly_violations_fix2(tree) + _extra_guard_violations(tree)


# fix4 H01-2: 禁止関数名（許可文脈ゼロ）。attrgetter も名前レベルで遮断
_BANNED_NAMES = _BANNED_DYNAMIC | {"attrgetter"}


def _context_allowlist_violations(tree) -> list[str]:
    """fix4 H01: **許可文脈の閉集合方式**（禁止形の列挙からの反転）。

    - kintone（poison 名）の出現許可文脈は2つのみ:
      (a) import 文そのもの（`from hub import kintone`。import の module 名は
      Name node を生成しないため走査上も自然に除外される）
      (b) `kintone.<許可3 API>` を **Call の func とする Attribute 連鎖の起点**。
      それ以外の**あらゆる出現**（関数引数・IfExp/BoolOp・Dict/List/Tuple/
      Subscript 内・代入 RHS・return・比較…）は構文文脈を問わず違反。
      名前の出現自体を縛るため alias の伝播追跡は不要（歴代検査は重畳として維持）。
    - 禁止関数名（getattr/setattr/__import__/eval/exec/import_module/attrgetter）は
      **許可文脈ゼロ**——Name としての出現・Attribute 連鎖末尾としての出現を
      module 全域で違反化（本 module はこれらを一切使わない前提。正当な必要が
      生じたらテスト改定=レビュー経由）。
    """
    violations = []
    parents: dict = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            if node.id == "kintone":
                parent = parents.get(node)
                gp = parents.get(parent)
                allowed = (isinstance(parent, ast.Attribute)
                           and parent.attr in _ALLOWED_KINTONE_ATTRS
                           and isinstance(gp, ast.Call) and gp.func is parent)
                if not allowed:
                    violations.append(
                        f"kintone の許可外文脈での出現（行 {node.lineno}）")
            if node.id in _BANNED_NAMES:
                violations.append(f"禁止名の出現: {node.id}")
        elif isinstance(node, ast.Attribute) and node.attr in _BANNED_NAMES:
            violations.append(f"禁止名の Attribute 出現: {node.attr}")
    return violations


def _readonly_violations_fix4(tree) -> list[str]:
    """fix4 H01 時点の検査（**対照用の凍結コピー・変更しない**）。
    出現文脈の閉集合まで=「kintone という名前への**束縛元**」は検証しないため、
    同名 shadow 束縛（import alias・仮引数・except as・match capture 等の
    Name node を生成しない束縛構文）を検出できない版（fix5 メタテストの基準）。"""
    return (_context_allowlist_violations(tree)
            + _readonly_violations_fix2(tree)
            + _extra_guard_violations(tree))


def _binding_violations(tree) -> list[str]:
    """fix5 H01: 束縛元の検証（最終ピース）。

    - 正規束縛=「module 直下の `from hub import kintone`（alias なし）・
      ちょうど 1 回」のみ許可。
    - "kintone" という識別子への**他の全束縛**を違反化。

    列挙の完全性（根拠）: Python 3.12 の AST（ASDL）で新しい識別子を束縛できる
    構文要素は次の閉集合であり、本関数はその全てを検査する——
    ① `Name(ctx=Store|Del)`（代入 target 全形式・for/with as・comprehension
    target・walrus はすべてこの形に脱糖される） ② `alias.asname`／`alias.name`
    （import 系） ③ `arg.arg`（posonly/args/kwonly/vararg/kwarg・lambda 含む——
    仮引数は arguments 配下の arg node に一元化されている） ④
    `ExceptHandler.name` ⑤ `FunctionDef/AsyncFunctionDef/ClassDef.name` ⑥
    match 系 capture（`MatchAs.name`／`MatchStar.name`／`MatchMapping.rest`）
    ⑦ `Global/Nonlocal.names`（宣言=束縛スコープの変更）。
    これら以外に識別子を導入する AST node は存在しない（ASDL の identifier
    フィールドを持つ node の全数）。
    """
    violations = []
    canonical = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.asname == "kintone":
                    violations.append("import alias による kintone 束縛")
                elif a.name == "kintone" and a.asname is None:
                    if node.module == "hub" and node in tree.body:
                        canonical += 1
                    else:
                        violations.append("正規以外の from-import kintone 束縛")
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.asname == "kintone" or (a.asname is None
                                             and a.name == "kintone"):
                    violations.append("import による kintone 束縛")
        elif isinstance(node, ast.arg) and node.arg == "kintone":
            violations.append("仮引数 kintone（shadow 束縛）")
        elif isinstance(node, ast.ExceptHandler) and node.name == "kintone":
            violations.append("except as kintone（shadow 束縛）")
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                               ast.ClassDef)) and node.name == "kintone":
            violations.append("def/class 名 kintone（shadow 束縛）")
        elif isinstance(node, (ast.MatchAs, ast.MatchStar)) \
                and getattr(node, "name", None) == "kintone":
            violations.append("match capture kintone（shadow 束縛）")
        elif isinstance(node, ast.MatchMapping) and node.rest == "kintone":
            violations.append("match mapping rest kintone（shadow 束縛）")
        elif isinstance(node, (ast.Global, ast.Nonlocal)) \
                and "kintone" in node.names:
            violations.append("global/nonlocal kintone 宣言")
        elif isinstance(node, ast.Name) and node.id == "kintone" \
                and isinstance(node.ctx, (ast.Store, ast.Del)):
            violations.append("代入系 target への kintone 束縛")
    if canonical != 1:
        violations.append(
            f"正規束縛（module 直下 from hub import kintone）が {canonical} 回"
            "（ちょうど 1 回であること）")
    return violations


def _readonly_violations(tree) -> list[str]:
    """fix5 H01 の現行検査 = 許可文脈の閉集合＋**束縛元の検証**（正規束縛
    ちょうど1回・他の全束縛構文を違反化）＋歴代検査（防御の重畳）。

    残余の限界（fix5 で再訂正）: 束縛検証の追加により同名 shadow 束縛も閉じた。
    **本当に残るのは実行時文字列からの名前解決（globals()/vars() の辞書アクセス
    等・文字列 "kintone" は Name/identifier node でない）と C 拡張内部の呼出し
    のみ**。この残余は sink AST policy・関所テスト・レビューで重畳防御する。
    """
    return _binding_violations(tree) + _readonly_violations_fix4(tree)


class TestReadOnlyMachineCheck(unittest.TestCase):
    """read-only の AST 機械検査。

    fix4 H01 で**許可文脈の閉集合方式へ反転**（禁止形の列挙を追加し続ける方式を
    終端）: kintone の出現は「import 文」「許可3 API の Call func 起点」のみ許可・
    禁止関数名は許可文脈ゼロ。歴代検査（fix1/fix2/fix3 の凍結コピー）は
    メタテストの基準および防御の重畳として並置維持。

    残余の限界（fix4 で限定）: **実行時文字列からの名前解決**（globals()/vars()
    の辞書アクセス等——文字列 "kintone" は Name node でないため出現検査の対象外）
    と **C 拡張内部の呼出し**のみ。subprocess/os・operator/asyncio・HTTP client は
    禁止 import 集合による入口遮断で対応済み。残余は sink AST policy・関所テスト・
    レビューで重畳防御。
    """

    def setUp(self):
        self.src = Path(cv.__file__).read_text(encoding="utf-8")
        self.tree = ast.parse(self.src)

    def test_only_read_apis_of_kintone_used(self):
        used = {n.attr for n in ast.walk(self.tree)
                if isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name) and n.value.id == "kintone"}
        self.assertTrue(used)
        self.assertLessEqual(used, _ALLOWED_KINTONE_ATTRS,
                             f"書込み系 API の使用: {used - _ALLOWED_KINTONE_ATTRS}")
        for banned in ("create_record", "update_record", "delete_record",
                       "upload_file", "_write"):
            self.assertNotIn(banned, self.src)

    def test_module_passes_strengthened_checker(self):
        self.assertEqual(_readonly_violations(self.tree), [])

    def test_meta_bypass_fixtures_old_pass_new_fail(self):
        """fix2 H01-3: Codex 提示の迂回 fixture 3種が「旧検査 PASS・新検査 FAIL」
        となる三段対照（lineq G0 の型）。旧検査は凍結コピー=基準の固定。"""
        fixtures = {
            "builtins_alias_getattr_concat": (
                "from builtins import getattr as ga\n"
                "from hub import kintone\n"
                "fn = ga(kintone, 'create_' + 'record')\n"),
            "builtins_alias_dunder_import": (
                "from builtins import __import__ as imp\n"
                "k = imp('hub.kintone')\n"),
            "tuple_unpack_alias": (
                "from hub import kintone\n"
                "kt, = (kintone,)\n"
                "fn = kt.update_record\n"),
        }
        for label, src in fixtures.items():
            with self.subTest(fixture=label):
                tree = ast.parse(src)
                self.assertEqual(_readonly_violations_legacy(tree), [],
                                 "旧検査は素通り（迂回が実在した証明）")
                self.assertNotEqual(_readonly_violations(tree), [],
                                    "新検査は検出すること")

    def test_meta_fix3_bypass_fixtures_fix2_pass_new_fail(self):
        """fix3 H01-3: Attribute/Subscript 代入系の迂回 3種が
        「旧（fix2 凍結コピー）検査 PASS・新検査 FAIL」となる三段対照。"""
        fixtures = {
            "compound_attr_box_and_builtins_getattr": (
                "from hub import kintone\n"
                "import builtins\n"
                "class B:\n    pass\n"
                "box = B()\n"
                "box.kt = kintone\n"
                "ga = builtins.getattr\n"
                "fn = ga(box.kt, 'create_' + 'record')\n"),
            "subscript_assignment": (
                "from hub import kintone\n"
                "holder = {}\n"
                "holder['kt'] = kintone\n"),
            "builtins_getattr_alias": (
                "import builtins\n"
                "ga = builtins.getattr\n"
                "x = ga(object, 'attr')\n"),
        }
        for label, src in fixtures.items():
            with self.subTest(fixture=label):
                tree = ast.parse(src)
                self.assertEqual(_readonly_violations_fix2(tree), [],
                                 "fix2 検査は素通り（迂回が実在した証明）")
                self.assertNotEqual(_readonly_violations(tree), [],
                                    "新検査は検出すること")

    def test_meta_fix4_bypass_fixtures_fix3_pass_new_fail(self):
        """fix4 H01-3: Codex 実測4経路が「旧（fix3 凍結コピー）検査 PASS・
        新（許可文脈閉集合）検査 FAIL」となる三段対照（歴代凍結コピーは並置維持）。"""
        fixtures = {
            "func_arg_sink": (
                "from hub import kintone\n"
                "def sink(m):\n    return m\n"
                "sink(kintone)\n"),
            "ifexp_alias": (
                "from hub import kintone\n"
                "k = kintone if True else None\n"),
            "dict_container": (
                "from hub import kintone\n"
                "d = {'k': kintone}\n"
                "fn = d['k'].update_record\n"),
            "conditional_builtins_getattr": (
                "import builtins\n"
                "ga = builtins.getattr if True else None\n"
                "x = ga(object, 'a')\n"),
        }
        for label, src in fixtures.items():
            with self.subTest(fixture=label):
                tree = ast.parse(src)
                self.assertEqual(_readonly_violations_fix3(tree), [],
                                 "fix3 検査は素通り（迂回が実在した証明）")
                self.assertNotEqual(_readonly_violations(tree), [],
                                    "新検査（許可文脈閉集合）は検出すること")

    def test_meta_fix5_shadow_binding_fixtures_fix4_pass_new_fail(self):
        """fix5 H01-3: Codex 実測4形の同名 shadow 束縛（Name node を生成しない
        束縛構文）が「fix4 検査 PASS・新（束縛検証）検査 FAIL」の三段対照。
        各 fixture の使用箇所は許可文脈（許可 API の Call）なので、検出は
        束縛元検証によることが分離して証明される。"""
        fixtures = {
            "import_alias_shadow": (
                "import evil_module as kintone\n"
                "kintone.get_record(1, '2')\n"),
            "function_parameter_shadow": (
                "def f(kintone):\n"
                "    return kintone.get_record(1, '2')\n"),
            "except_as_shadow": (
                "try:\n    pass\n"
                "except Exception as kintone:\n"
                "    kintone.get_record(1, '2')\n"),
            "match_capture_shadow": (
                "def f(v):\n"
                "    match v:\n"
                "        case kintone:\n"
                "            return kintone.get_record(1, '2')\n"),
        }
        for label, src in fixtures.items():
            with self.subTest(fixture=label):
                tree = ast.parse(src)
                self.assertEqual(_readonly_violations_fix4(tree), [],
                                 "fix4 検査は素通り（迂回が実在した証明）")
                self.assertNotEqual(_readonly_violations(tree), [],
                                    "新検査（束縛元検証）は検出すること")

    def test_canonical_binding_exactly_once(self):
        # fix5 H01-1: 本体 module の正規束縛がちょうど 1 回・shadow 束縛ゼロ
        src = Path(cv.__file__).read_text(encoding="utf-8")
        self.assertEqual(_binding_violations(ast.parse(src)), [])

    def test_no_direct_http_or_process_launch_imports(self):
        imported = set()
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        self.assertFalse(imported & _FORBIDDEN_IMPORTS, imported)


class TestCasesApi(unittest.TestCase):
    def _get(self, url, mock_search):
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", mock_search):
            return _client.get(url, headers=_auth_headers(),
                               follow_redirects=False)

    def test_default_query_and_passthrough(self):
        mock = AsyncMock(return_value=[_rec(**{"$id": "1", "顧客名": "山田太郎",
                                               "status": "受任"})])
        r = self._get("/app/api/cases", mock)
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["records"][0]["顧客名"]["value"], "山田太郎")
        self.assertEqual(body["limit"], 20)
        app, query = mock.call_args.args[:2]
        self.assertIs(app, cv.APP_CASES)
        self.assertEqual(query, "order by 更新日時 desc limit 20 offset 0")

    def test_status_filter_closed_set(self):
        mock = AsyncMock(return_value=[])
        r = self._get("/app/api/cases?status=受任&limit=50&offset=20", mock)
        self.assertEqual(r.status_code, 200)
        query = mock.call_args.args[1]
        self.assertEqual(query,
                         'status in ("受任") order by 更新日時 desc '
                         "limit 50 offset 20")
        self.assertEqual(r.json()["status_options"], list(cv.STATUS_OPTIONS))

    def test_invalid_inputs_fixed_400_no_reflection_no_call(self):
        cases = ("status=怪しい値", "limit=51", "limit=0", "limit=abc",
                 "offset=-1", "offset=x")
        for qs in cases:
            with self.subTest(qs=qs):
                mock = AsyncMock(return_value=[])
                r = self._get(f"/app/api/cases?{qs}", mock)
                self.assertEqual(r.status_code, 400)
                self.assertEqual(r.content, b"")     # 固定・非反射
                mock.assert_not_called()             # kintone 未到達


class TestCaseDetailApi(unittest.TestCase):
    def _detail(self, record_id, case_rec, search_results):
        get_mock = AsyncMock(return_value=case_rec)
        search_mock = AsyncMock(side_effect=search_results)
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "get_record", get_mock), \
             patch.object(hub_kintone, "search_records", search_mock):
            r = _client.get(f"/app/api/cases/{record_id}",
                            headers=_auth_headers(), follow_redirects=False)
        return r, get_mock, search_mock

    def test_detail_with_chat_count_only(self):
        case = _rec(**{"顧客名": "山田太郎", "LINEユーザーID": _LUID})
        shipping = [_rec(**{"$id": "9", "件名": "封筒"})]
        chats = [_rec(**{"$id": str(i)}) for i in range(3)]
        r, get_mock, search_mock = self._detail("12", case, [shipping, chats])
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["case"]["顧客名"]["value"], "山田太郎")
        self.assertEqual(body["shipping"][0]["件名"]["value"], "封筒")
        self.assertEqual(body["chat_count"], 3)
        self.assertFalse(body["chat_count_capped"])
        ship_call, chat_call = search_mock.call_args_list
        self.assertIn('案件レコードID = "12"', ship_call.args[1])
        self.assertIn(f'line_user_id = "{_LUID}"', chat_call.args[1])
        self.assertEqual(chat_call.kwargs.get("fields"), ["$id"])  # 本文非取得

    def test_invalid_line_user_id_skips_chat_query(self):
        bad = 'x" or role = "user'
        case = _rec(**{"顧客名": "山田太郎", "LINEユーザーID": bad})
        r, _, search_mock = self._detail("12", case, [[]])
        self.assertEqual(r.status_code, 200)
        self.assertIsNone(r.json()["chat_count"])
        self.assertEqual(search_mock.call_count, 1)      # App30 のみ・注入経路なし
        self.assertNotIn(bad, search_mock.call_args.args[1])

    def test_bad_record_id_fixed_404_no_kintone_call(self):
        for bad in ("abc", "1e3", "12345678901", "1;drop"):
            with self.subTest(rid=bad):
                r, get_mock, search_mock = self._detail(bad, {}, [])
                self.assertEqual(r.status_code, 404)
                self.assertEqual(r.content, b"")
                get_mock.assert_not_called()
                search_mock.assert_not_called()


class TestBoundaryPins(unittest.TestCase):
    """fix1 L01: 境界・fields 集合の完全一致 pin。"""

    def test_chat_count_exactly_at_cap_sets_capped(self):
        case = _rec(**{"顧客名": "山田太郎", "LINEユーザーID": _LUID})
        chats = [_rec(**{"$id": str(i)}) for i in range(500)]     # ちょうど 500
        get_mock = AsyncMock(return_value=case)
        search_mock = AsyncMock(side_effect=[[], chats])
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "get_record", get_mock), \
             patch.object(hub_kintone, "search_records", search_mock):
            r = _client.get("/app/api/cases/12", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.json()["chat_count"], 500)
        self.assertTrue(r.json()["chat_count_capped"])

    def test_fields_sets_pinned_exactly(self):
        # App21 一覧 / App30 絞込の fields 集合の完全一致（黙った拡張の防波堤）
        self.assertEqual(cv._LIST_FIELDS,
                         ["$id", "status", "顧客名", "問い合わせ業者名", "更新日時"])
        self.assertEqual(cv._SHIPPING_FIELDS,
                         ["$id", "件名", "チャネル", "方向", "発送ステータス",
                          "発送日時", "追跡番号", "送達結果", "更新日時"])
        mock = AsyncMock(return_value=[])
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", mock):
            _client.get("/app/api/cases", headers=_auth_headers(),
                        follow_redirects=False)
        self.assertEqual(mock.call_args.kwargs.get("fields"), cv._LIST_FIELDS)
        get_mock = AsyncMock(return_value=_rec(**{"顧客名": "x"}))
        ship_mock = AsyncMock(side_effect=[[]])
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "get_record", get_mock), \
             patch.object(hub_kintone, "search_records", ship_mock):
            _client.get("/app/api/cases/12", headers=_auth_headers(),
                        follow_redirects=False)
        self.assertEqual(ship_mock.call_args.kwargs.get("fields"),
                         cv._SHIPPING_FIELDS)


class TestWebappDomSafety(unittest.TestCase):
    """fix1 H01: HTML 文字列補間 API の全画面不在（将来画面の防波堤）。"""

    _FORBIDDEN = ("innerHTML", "insertAdjacentHTML", "document.write")

    def test_no_html_string_apis_in_any_webapp_page(self):
        pages = sorted((Path(cv.__file__).resolve().parent.parent / "webapp")
                       .glob("*.html"))
        self.assertGreaterEqual(len(pages), 4)           # 走査対象が空でないこと
        for page in pages:
            src = page.read_text(encoding="utf-8")
            for banned in self._FORBIDDEN:
                with self.subTest(page=page.name, banned=banned):
                    self.assertNotIn(banned, src)

    def test_cases_status_options_built_as_dom_strings(self):
        # H01 pin: option は createElement+textContent/value 代入で構築され、
        # HTML 特殊文字を含む選択肢値も DOM 文字列として扱われる（補間文字列なし）
        src = (Path(cv.__file__).resolve().parent.parent
               / "webapp" / "cases.html").read_text(encoding="utf-8")
        self.assertIn('document.createElement("option")', src)
        self.assertIn("opt.textContent = o", src)
        self.assertIn("opt.value = o", src)
        self.assertNotIn("<option", src.split("<script>")[1])   # JS 内に option HTML なし


class TestPages(unittest.TestCase):
    def test_pages_served_when_authed(self):
        # catch-all（webapp_auth）より先の結線 pin: catch-all に食われると 404 になる
        with patch.dict(os.environ, _ENV):
            for path, needle in (("/app/cases", "案件一覧"),
                                 ("/app/case", "案件詳細")):
                with self.subTest(path=path):
                    r = _client.get(path, headers=_auth_headers(),
                                    follow_redirects=False)
                    self.assertEqual(r.status_code, 200)
                    self.assertIn(needle, r.text)

    def test_dashboard_links_to_cases(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app", headers=_auth_headers(),
                            follow_redirects=False)
            self.assertIn('href="/app/cases"', r.text)


if __name__ == "__main__":
    unittest.main()
