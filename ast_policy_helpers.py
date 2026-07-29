"""ast_policy_helpers — read-only／起動遮断系 AST 機械検査の共通ヘルパ（移設のみ）

AST-CONSOL 票（2026-07-30・小粒キュー）: p4-002／p4-004 の 2 テストファイルに
byte 同一で並置されていた kintone read-only 検査群（許可文脈閉集合＋束縛検証:
ASDL 7分類＋PEP 695 type parameter 3種＋star import 禁止・歴代凍結コピー含む
19 定義）と、tracking-prep の subprocess/os 束縛検証 2 関数を、**挙動同一のまま**
本 module へ移設した（検査の意味・強度の変更なし・禁止形の追加/緩和/拡張なし・
ソースは移設元と逐語一致）。

- 凍結コピー（*_legacy／*_fix2／*_fix3／*_fix4／*_fix5／*_prev）の「対照用・
  変更しない」契約は移設後も不変（各テストの旧 PASS/新 FAIL メタ対照の基準のまま）。
- kintone 系（_readonly_violations ほか）と subprocess/os 系
  （_proc_binding_violations ほか）は検査対象・正規束縛形が設計上異なる別実装で
  あり、本票では**統合していない**（統合の要否は司令塔裁定事項）。
"""

import ast


# ══════════════════════════════════════════════════════════════
# p4-002／p4-004 由来（byte 同一並置の解消・test_p4_002_case_views.py L86-514 逐語）
# ══════════════════════════════════════════════════════════════

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


def _binding_violations_fix5(tree) -> list[str]:
    """fix5 時点の束縛検証（**対照用の凍結コピー・変更しない**）。
    star import・PEP 695 type parameter を束縛構文として扱わない版
    （fix6 メタテストの基準）。"""
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
        violations.append("正規束縛がちょうど 1 回でない")
    return violations


def _readonly_violations_fix5(tree) -> list[str]:
    """fix5 時点の現行検査（**対照用の凍結コピー**）= 束縛検証(fix5)＋fix4。
    star import・type parameter 束縛を見逃す版（fix6 メタの基準）。"""
    return _binding_violations_fix5(tree) + _readonly_violations_fix4(tree)


def _binding_violations(tree) -> list[str]:
    """fix5 H01→fix6 で star import 全面違反・PEP 695 type parameter を追加。

    - 正規束縛=「module 直下の `from hub import kintone`（alias なし）・
      ちょうど 1 回」のみ許可。
    - "kintone" という識別子への**他の全束縛**を違反化。

    列挙の完全性（根拠・fix6 で type parameter/star import を追加し最終確定）:
    Python 3.12 の AST（ASDL）で新しい識別子を束縛できる構文要素は次の閉集合であり、
    本関数はその全てを検査する——
    ① `Name(ctx=Store|Del)`（代入 target 全形式・for/with as・comprehension
    target・walrus はすべてこの形に脱糖される） ② `alias.asname`／`alias.name`
    （import 系） ③ `arg.arg`（posonly/args/kwonly/vararg/kwarg・lambda 含む——
    仮引数は arguments 配下の arg node に一元化されている） ④
    `ExceptHandler.name` ⑤ `FunctionDef/AsyncFunctionDef/ClassDef.name` ⑥
    match 系 capture（`MatchAs.name`／`MatchStar.name`／`MatchMapping.rest`）
    ⑦ `Global/Nonlocal.names`（宣言=束縛スコープの変更）
    ⑧ **PEP 695 type parameter**（`TypeVar`／`ParamSpec`／`TypeVarTuple` の name。
    `def f[kintone]()`／`class C[kintone]`／`type X[kintone] = ...`）。
    加えて ⑨ **star import**（`from <any> import *`）は束縛される名前が静的に
    確定できず kintone を暗黙導入し得るため、module を問わず一律違反とする
    （本 module に star import を使う正当理由はない）。
    これら以外に識別子を導入する AST node は存在しない（ASDL の identifier
    フィールドを持つ node の全数）。
    """
    violations = []
    canonical = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name == "*":            # ⑨ star import は一律違反（fix6 H01）
                    violations.append(f"star import（from {node.module} import *）")
                elif a.asname == "kintone":
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
        elif isinstance(node, (ast.TypeVar, ast.ParamSpec, ast.TypeVarTuple)) \
                and node.name == "kintone":     # ⑧ PEP 695 type parameter
            violations.append("type parameter kintone（shadow 束縛）")
        elif isinstance(node, ast.Name) and node.id == "kintone" \
                and isinstance(node.ctx, (ast.Store, ast.Del)):
            violations.append("代入系 target への kintone 束縛")
    if canonical != 1:
        violations.append(
            f"正規束縛（module 直下 from hub import kintone）が {canonical} 回"
            "（ちょうど 1 回であること）")
    return violations


def _readonly_violations(tree) -> list[str]:
    """fix6 H01/M01 の現行検査 = 許可文脈の閉集合＋**束縛元の検証**（正規束縛
    ちょうど1回・全束縛構文〔7分類＋type parameter 3種〕を違反化・star import
    一律違反）＋歴代検査（防御の重畳）。

    残余の限界（fix6 で最終確定）: 束縛検証（7分類＋PEP 695 type parameter＋
    star import 禁止）と許可文脈閉集合により、静的に kintone を導入・別名化・
    持ち出しする経路は網羅的に閉じた。**本当に残るのは実行時文字列からの
    名前解決（globals()/vars() の辞書アクセス等・文字列 "kintone" は
    Name/identifier node でない）と C 拡張内部の呼出しのみ**。この残余は
    sink AST policy・関所テスト・レビューで重畳防御する。
    """
    return _binding_violations(tree) + _readonly_violations_fix4(tree)


# ══════════════════════════════════════════════════════════════
# tracking-prep 由来（subprocess/os 束縛検証・test_tracking_prep_harness.py
# L498-608 逐語）
# ══════════════════════════════════════════════════════════════

def _proc_binding_violations_prev(tree) -> list[str]:
    """fix6 M02 初版の束縛検証（**対照用の凍結コピー・変更しない**）。
    全面 star import・PEP 695 type parameter を束縛構文として扱わない版
    （p4-002 fix6 と同型の追補メタテストの基準）。"""
    targets = {"subprocess", "os"}
    canon = {"subprocess": 0, "os": 0}
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.asname in targets:
                    violations.append(f"import alias による {a.asname} 束縛")
                elif a.name in targets and a.asname is None:
                    canon[a.name] += 1
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.asname in targets or a.name in targets:
                    violations.append(
                        f"from-import による {a.asname or a.name} 束縛")
        elif isinstance(node, ast.arg) and node.arg in targets:
            violations.append(f"仮引数 {node.arg}（shadow 束縛）")
        elif isinstance(node, ast.ExceptHandler) and node.name in targets:
            violations.append(f"except as {node.name}（shadow 束縛）")
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                               ast.ClassDef)) and node.name in targets:
            violations.append(f"def/class 名 {node.name}（shadow 束縛）")
        elif isinstance(node, (ast.MatchAs, ast.MatchStar)) \
                and getattr(node, "name", None) in targets:
            violations.append(f"match capture {node.name}（shadow 束縛）")
        elif isinstance(node, ast.MatchMapping) and node.rest in targets:
            violations.append(f"match mapping rest {node.rest}（shadow 束縛）")
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            for n in node.names:
                if n in targets:
                    violations.append(f"global/nonlocal {n} 宣言")
        elif isinstance(node, ast.Name) and node.id in targets \
                and isinstance(node.ctx, (ast.Store, ast.Del)):
            violations.append(f"代入系 target への {node.id} 束縛")
    for name in targets:
        if canon[name] != 1:
            violations.append(
                f"正規束縛（alias なし import {name}）が {canon[name]} 回"
                "（ちょうど 1 回であること）")
    return violations


def _proc_binding_violations(tree) -> list[str]:
    """fix6 M02: subprocess/os の束縛元の検証（p4-002 fix5/fix6 と同型）。

    正規束縛=「alias なし `import subprocess`／`import os` が各ちょうど 1 回」。
    それ以外の "subprocess"/"os" への全束縛を違反化。

    列挙の完全性（根拠・p4-002 fix6 に合わせて最終確定）: Python 3.12 AST（ASDL）
    で識別子を束縛する node は ①Name(ctx=Store|Del)（代入 target 全形式・
    for/with as・comprehension target・walrus は本形へ脱糖） ②alias.asname/
    alias.name（import 系） ③arg.arg（posonly/args/kwonly/vararg/kwarg・lambda
    含む＝arguments 配下 arg） ④ExceptHandler.name ⑤FunctionDef/
    AsyncFunctionDef/ClassDef.name ⑥match capture（MatchAs/MatchStar.name・
    MatchMapping.rest） ⑦Global/Nonlocal.names ⑧**PEP 695 type parameter**
    （TypeVar/ParamSpec/TypeVarTuple の name）、の閉集合であり、本関数はその全てを
    検査する。加えて ⑨ **star import**（`from <any> import *`）は束縛名が静的に
    確定できず subprocess/os を暗黙導入し得るため module を問わず一律違反。

    残余（p4-002 と共通で最終確定）: 実行時文字列からの名前解決（globals()/vars()
    の辞書アクセス・文字列は identifier node でない）と C 拡張内部の呼出しのみ。
    """
    targets = {"subprocess", "os"}
    canon = {"subprocess": 0, "os": 0}
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.asname in targets:
                    violations.append(f"import alias による {a.asname} 束縛")
                elif a.name in targets and a.asname is None:
                    canon[a.name] += 1
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name == "*":            # ⑨ star import は module を問わず違反
                    violations.append(f"star import（from {node.module} import *）")
                elif a.asname in targets or a.name in targets:
                    violations.append(
                        f"from-import による {a.asname or a.name} 束縛")
        elif isinstance(node, ast.arg) and node.arg in targets:
            violations.append(f"仮引数 {node.arg}（shadow 束縛）")
        elif isinstance(node, ast.ExceptHandler) and node.name in targets:
            violations.append(f"except as {node.name}（shadow 束縛）")
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                               ast.ClassDef)) and node.name in targets:
            violations.append(f"def/class 名 {node.name}（shadow 束縛）")
        elif isinstance(node, (ast.MatchAs, ast.MatchStar)) \
                and getattr(node, "name", None) in targets:
            violations.append(f"match capture {node.name}（shadow 束縛）")
        elif isinstance(node, ast.MatchMapping) and node.rest in targets:
            violations.append(f"match mapping rest {node.rest}（shadow 束縛）")
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            for n in node.names:
                if n in targets:
                    violations.append(f"global/nonlocal {n} 宣言")
        elif isinstance(node, (ast.TypeVar, ast.ParamSpec, ast.TypeVarTuple)) \
                and node.name in targets:       # ⑧ PEP 695 type parameter
            violations.append(f"type parameter {node.name}（shadow 束縛）")
        elif isinstance(node, ast.Name) and node.id in targets \
                and isinstance(node.ctx, (ast.Store, ast.Del)):
            violations.append(f"代入系 target への {node.id} 束縛")
    for name in targets:
        if canon[name] != 1:
            violations.append(
                f"正規束縛（alias なし import {name}）が {canon[name]} 回"
                "（ちょうど 1 回であること）")
    return violations
