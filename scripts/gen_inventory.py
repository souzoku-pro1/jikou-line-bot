# -*- coding: utf-8 -*-
"""gen_inventory — INVENTORY-1（QUEUE-2026-10-14-G G-6）: コードから全体目録 docs/inventory.md を自動生成（手書きゼロ・冪等）

(a) 画面一覧（/app 配下の URL・必要ロール・担当モジュール）: FastAPI の route 表（main.app.router.routes・include_router の
    エントリは original_router を展開）から。必要ロール＝ hub.webapp_auth.PUBLIC_ROUTES なら「公開」、endpoint が _gate を
    経由（__webapp_gate__）なら「ログイン必須」、それ以外は「関所なし（要確認）」。
(b) ジョブ一覧（名前・起動時刻・台帳 key 形式・通知先）: register_daily／register_interval の呼出を AST で読む（実行しない）。
(c) 送信台帳の purpose と受信イベント ID（key）形式: hub/send_ledger.py の定数と bind_inbound(...) の引数・*ledger_key の戻り値。
(d) env 変数一覧（名前と用途のみ・値は出さない）: os.environ.get／os.environ[]／os.getenv／*_ENV 定数（AST）。
(e) flag 一覧（名前・既定値・ON にすると何が変わるか）: (d) のうち flag 名（…ENABLED／DISABLED／PAUSED 等）または
    "1"/"true" と比較される読取。説明はコード中の近傍コメント／docstring から取る（手書きなし）。
(f) kintone アプリ・欄の参照一覧（App・欄名・読取/書込）: KintoneApp(...) 定義と FIELD_* 定数。書込＝hub.kintone の
    update_record／create_record／add_record の引数 dict に現れる欄。
(g) 相談者向け文言の一覧（要承認／承認済／凍結）: scripts/check_reply_wording.py の抽出器を再利用（AST）。
値（env の実値・token・URL）・時刻・SHA は出力しない（再実行で差分が出ない）。

usage: python scripts/gen_inventory.py [--out docs/inventory.md] [--check]
"""
import argparse
import ast
import importlib.util
import os
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

# main を import するための仮の env（値は出力しない・既存テストと同じ鍵集合・未設定のときだけ入れる）
_DUMMY_ENV = {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret", "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "ANTHROPIC_API_KEY": "dummy_key", "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy", "CLOUDSIGN_CLIENT_ID": "c",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs", "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1", "STRIPE_WEBHOOK_SECRET": "w",
    "GOOGLE_VISION_API_KEY": "dummy_vision", "IMAGE_HEAL_DISABLED": "1",
}
SCAN_FILES = ("main.py", "chat_responder.py", "daily_healthcheck.py", "claude_gateway.py")
SCAN_DIRS = ("hub", "channels", "units")
ENV_CONST_RE = re.compile(r"^_?[A-Z0-9_]+_ENV$")
FLAG_NAME_RE = re.compile(r"(ENABLED|DISABLED|PAUSED|REQUIRED|_FLAG|_V2|DRY_RUN|DRYRUN|_MODE)$")
FLAG_VALUES = {"1", "true", "on", "yes", "0", "false", "off", "no"}
KINTONE_WRITE_FUNCS = ("update_record", "create_record", "add_record", "update_records", "create_records")
NOTIFY_MARKERS = (
    ("send_business_notice", "業務 LINE（ATTORNEY_LINE_USER_ID・send_policy）"),
    ("_push_business_http", "業務 LINE（ATTORNEY_LINE_USER_ID・send_policy）"),
    ("_push_admin_http", "管理者 LINE（LINE_ADMIN_USER_ID・業務トークン）"),
    ("notify_admin_line", "管理者 LINE（notify_admin_line）"),
    ("notify_business", "業務 LINE（notify_business）"),
    ("get_admin_line_user_id", "管理者 LINE（LINE_ADMIN_USER_ID）"),
)
STATUS_LABELS = {"frozen_v14": "凍結（v1.4）", "needs_approval": "要承認", "frozen": "凍結", "prompt": "凍結（prompt・FAQ）",
                 "promoted": "承認済（昇格）"}


# ── 共通: ファイル集合・AST ──────────────────────────────────────────────────────
def py_files() -> list[pathlib.Path]:
    out = [REPO / f for f in SCAN_FILES if (REPO / f).exists()]
    for d in SCAN_DIRS:
        p = REPO / d
        if p.is_dir():
            out += sorted(x for x in p.rglob("*.py") if "__pycache__" not in x.parts)
    return sorted(set(out))


def rel(p: pathlib.Path) -> str:
    return p.relative_to(REPO).as_posix()


def _first_line(doc) -> str:
    return (doc or "").strip().split("\n")[0].strip()


class _Mod:
    """1 ファイルの AST・module 定数・alias → module 解決・近傍コメント。"""
    cache: dict = {}

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.src = path.read_text(encoding="utf-8")
        self.lines = self.src.split("\n")
        self.tree = ast.parse(self.src)
        self.consts: dict = {}
        self.const_nodes: dict = {}
        self.aliases: dict = {}
        self.funcs: dict = {}
        for node in self.tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                self.const_nodes[node.targets[0].id] = node.value
                try:
                    self.consts[node.targets[0].id] = ast.literal_eval(node.value)
                except (ValueError, SyntaxError, TypeError):
                    pass
            elif isinstance(node, ast.ImportFrom) and node.module:
                for a in node.names:
                    self.aliases[a.asname or a.name] = f"{node.module}.{a.name}"
            elif isinstance(node, ast.Import):
                for a in node.names:
                    self.aliases[a.asname or a.name] = a.name
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.funcs[node.name] = node
        self.parents: dict = {}
        for p in ast.walk(self.tree):
            for c in ast.iter_child_nodes(p):
                self.parents[c] = p

    @classmethod
    def get(cls, path: pathlib.Path) -> "_Mod":
        key = str(path)
        if key not in cls.cache:
            cls.cache[key] = cls(path)
        return cls.cache[key]

    def module_of(self, alias: str):
        """alias（from hub import x as y／import hub.x as y）→ _Mod。"""
        target = self.aliases.get(alias)
        if not target:
            return None
        p = REPO / (target.replace(".", "/") + ".py")
        return self.get(p) if p.exists() else None

    def is_kintone_call(self, call: ast.Call) -> bool:
        f = call.func
        if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name):
            return (self.aliases.get(f.value.id) or "").split(".")[-1] == "kintone"
        if isinstance(f, ast.Name):
            return (self.aliases.get(f.id) or "").startswith("hub.kintone.")
        return False

    def enclosing_fn(self, node):
        cur = self.parents.get(node)
        while cur is not None and not isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
            cur = self.parents.get(cur)
        return cur

    def enclosing_ifs(self, node) -> list[str]:
        out, cur = [], self.parents.get(node)
        while cur is not None and not isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if isinstance(cur, ast.If):
                out.append(ast.unparse(cur.test))
            cur = self.parents.get(cur)
        return [c for c in out if "is_registered" not in c]

    def comment_near(self, node) -> str:
        """同じ行の # コメント → 直前の連続コメント行（最大 3 行・順序どおり）→ 囲む関数の docstring 1 行目 →
        module docstring 1 行目。"""
        lineno = getattr(node, "lineno", 0)
        line = self.lines[lineno - 1] if 0 < lineno <= len(self.lines) else ""
        if "#" in line and not line.strip().startswith("#"):
            return line.split("#", 1)[1].strip()
        block = []
        for i in range(lineno - 2, max(-1, lineno - 5), -1):
            s = self.lines[i].strip()
            if s.startswith("#"):
                block.insert(0, s.lstrip("#").strip())
            else:
                break
        if block:
            return " ".join(block)
        fn = self.enclosing_fn(node)
        if fn is not None and _first_line(ast.get_docstring(fn)):
            return _first_line(ast.get_docstring(fn))
        return _first_line(ast.get_docstring(self.tree))


def _const_str(v) -> str:
    return "／".join(str(x) for x in v) if isinstance(v, (tuple, list)) else str(v)


def _resolved(node, mod: _Mod, local: dict):
    """定数に確定できる式はその値（文字列）を、できなければ None を返す。"""
    if isinstance(node, ast.Constant):
        return str(node.value)
    if isinstance(node, ast.Name) and node.id not in local and node.id in mod.consts:
        return _const_str(mod.consts[node.id])
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        m = mod.module_of(node.value.id)
        if m is not None and node.attr in m.consts:
            return _const_str(m.consts[node.attr])
    return None


def render(node, mod: _Mod, local: dict | None = None, depth: int = 0) -> str:
    """式を目録用の文字列に（定数は値・f-string の未確定部分は {式}・ループ変数は {名∈候補}・同 module の単純関数は展開）。"""
    local = local or {}
    if node is None or depth > 6:
        return ""
    if isinstance(node, ast.Constant):
        return str(node.value)
    if isinstance(node, ast.JoinedStr):
        out = ""
        for v in node.values:
            if isinstance(v, ast.Constant):
                out += str(v.value)
            elif isinstance(v, ast.FormattedValue):
                spec = (":" + render(v.format_spec, mod, local, depth + 1)) if v.format_spec else ""
                fixed = _resolved(v.value, mod, local)
                if fixed is not None and not spec:
                    out += fixed
                    continue
                inner = render(v.value, mod, local, depth + 1)
                if inner.startswith("{") and inner.endswith("}"):
                    out += inner[:-1] + spec + "}"
                else:
                    out += "{" + inner + spec + "}"
        return out
    if isinstance(node, ast.Name):
        if node.id in local:
            return local[node.id]
        if node.id in mod.consts:
            return _const_str(mod.consts[node.id])
        if node.id in mod.const_nodes:
            return render(mod.const_nodes[node.id], mod, {}, depth + 1)
        return "{" + node.id + "}"
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        m = mod.module_of(node.value.id)
        if m is not None and node.attr in m.consts:
            return _const_str(m.consts[node.attr])
        return "{" + node.value.id + "." + node.attr + "}"
    if isinstance(node, ast.Call):
        src = ast.unparse(node.func)
        if src in ("os.environ.get", "os.getenv") and node.args and isinstance(node.args[0], ast.Constant):
            default = node.args[1] if len(node.args) > 1 else None
            d = f"（既定 {render(default, mod, local, depth + 1)}）" if isinstance(default, ast.Constant) else ""
            return "env:" + str(node.args[0].value) + d
        if src == "int" and node.args:
            return render(node.args[0], mod, local, depth + 1)
        fn = mod.funcs.get(src)
        if fn is not None and len(fn.body) == 1 or (fn is not None and len(fn.body) == 2
                                                    and isinstance(fn.body[0], ast.Expr)):
            ret = fn.body[-1]
            if isinstance(ret, ast.Return) and ret.value is not None:
                params = [a.arg for a in fn.args.args]
                bound = dict(zip(params, [render(a, mod, local, depth + 1) for a in node.args]))
                return render(ret.value, mod, bound, depth + 1)
        return "{" + src + "(...)}"
    if isinstance(node, (ast.Tuple, ast.List)):
        return "／".join(render(e, mod, local, depth + 1) for e in node.elts)
    return "{" + ast.unparse(node) + "}"


def _locals_of(fn: ast.AST, mod: _Mod) -> dict:
    """関数内の単純代入（x = <式>）と for ループ変数（x in <定数列>）の表示用束縛。"""
    local: dict = {}
    for node in ast.walk(fn):
        if isinstance(node, ast.For) and isinstance(node.target, ast.Name):
            local[node.target.id] = "{" + node.target.id + "∈" + render(node.iter, mod) + "}"
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            local[node.targets[0].id] = render(node.value, mod, local)
    return local


# ── (a) 画面一覧 ──────────────────────────────────────────────────────────────
def _flatten_routes(routes, prefix: str = "") -> list:
    out = []
    for r in routes:
        inner = getattr(r, "original_router", None)
        if inner is not None:
            out += _flatten_routes(inner.routes, prefix + (getattr(r, "prefix", "") or ""))
        else:
            out.append((prefix + getattr(r, "path", ""), r))
    return out


def screens() -> list[dict]:
    for k, v in _DUMMY_ENV.items():
        os.environ.setdefault(k, v)
    import main
    from hub.webapp_auth import PUBLIC_ROUTES
    rows = []
    for path, r in _flatten_routes(main.app.router.routes):
        if not path.startswith("/app"):
            continue
        ep = getattr(r, "endpoint", None)
        if ep is None:
            rows.append({"path": path, "methods": "MOUNT", "role": "公開（静的）", "module": type(r).__name__})
            continue
        for m in sorted(getattr(r, "methods", None) or ["*"]):
            if (path, m) in PUBLIC_ROUTES:
                role = "公開"
            elif getattr(ep, "__webapp_gate__", False):
                role = "ログイン必須（_gate）"
            else:
                role = "関所なし（要確認）"
            rows.append({"path": path, "methods": m, "role": role, "module": getattr(ep, "__module__", "")})
    return sorted(rows, key=lambda x: (x["path"], x["methods"]))


# ── (b) ジョブ一覧 ────────────────────────────────────────────────────────────
def ledger_keys_in(mod: _Mod) -> list[str]:
    keys = []
    for node in ast.walk(mod.tree):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", getattr(node.func, "id", "")) == "bind_inbound" \
                and node.args:
            fn = mod.enclosing_fn(node)
            keys.append(render(node.args[0], mod, _locals_of(fn, mod) if fn else {}))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and (node.name.endswith("ledger_key")
                                                                           or node.name.endswith("_day_key")):
            local = _locals_of(node, mod)
            for r in ast.walk(node):
                if isinstance(r, ast.Return) and r.value is not None:
                    keys.append(render(r.value, mod, local))
    return sorted(set(k for k in keys if k))


def notify_target(mod: _Mod) -> str:
    found = [label for marker, label in NOTIFY_MARKERS if marker in mod.src]
    return "／".join(dict.fromkeys(found)) if found else "通知なし（DB／kintone のみ）"


def jobs() -> list[dict]:
    rows = []
    for p in py_files():
        if rel(p) == "hub/scheduler.py":
            continue
        mod = _Mod.get(p)
        for node in ast.walk(mod.tree):
            fname = getattr(node.func, "attr", getattr(node.func, "id", "")) if isinstance(node, ast.Call) else ""
            if fname not in ("register_daily", "register_interval") or len(node.args) < 3:
                continue
            kind = "daily" if fname == "register_daily" else "interval"
            fn = mod.enclosing_fn(node)
            local = _locals_of(fn, mod) if fn else {}
            when = render(node.args[1], mod, local)
            rows.append({"name": render(node.args[0], mod, local), "kind": kind,
                         "when": (f"毎日 {when} 時 JST" if kind == "daily" else f"{when} 分ごと"),
                         "func": ast.unparse(node.args[2]), "module": rel(p), "cond": "／".join(mod.enclosing_ifs(node)),
                         "keys": "、".join(ledger_keys_in(mod)) or "—", "notify": notify_target(mod)})
    return sorted(rows, key=lambda x: (x["name"], x["module"]))


# ── (c) 送信台帳 purpose／key ───────────────────────────────────────────────────
def purposes() -> dict:
    mod = _Mod.get(REPO / "hub" / "send_ledger.py")
    ps = {k: v for k, v in mod.consts.items() if k.startswith("PURPOSE_") and isinstance(v, str)}
    closed = set()
    node = mod.const_nodes.get("PURPOSES")
    if isinstance(node, (ast.Tuple, ast.List)):
        closed = {render(e, mod) for e in node.elts}
    kinds = mod.consts.get("PURPOSE_KINDS")
    keys = []
    for p in py_files():
        for k in ledger_keys_in(_Mod.get(p)):
            keys.append((k, rel(p)))
    return {"purposes": sorted(ps.items()), "closed": closed, "kinds": kinds, "keys": sorted(set(keys))}


# ── (d)(e) env・flag ───────────────────────────────────────────────────────────
def _env_name(node: ast.Call, mod: _Mod):
    f = node.func
    is_get = isinstance(f, ast.Attribute) and f.attr in ("get", "getenv", "setdefault", "pop") and \
        (ast.unparse(f.value) in ("os.environ", "os") or (isinstance(f.value, ast.Name) and f.value.id == "environ"))
    if not is_get or not node.args:
        return None
    a = node.args[0]
    if isinstance(a, ast.Constant) and isinstance(a.value, str):
        return a.value
    if isinstance(a, ast.Name) and isinstance(mod.consts.get(a.id), str):
        return mod.consts[a.id]
    if isinstance(a, ast.Attribute) and isinstance(a.value, ast.Name):
        m = mod.module_of(a.value.id)
        if m is not None and isinstance(m.consts.get(a.attr), str):
            return m.consts[a.attr]
    return None


def env_refs() -> dict:
    """env 名 → {"refs": {module: n}, "note": str, "flag": bool}。"""
    out: dict = {}

    def add(name, mod, node, flag=False):
        if not name or not re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
            return
        e = out.setdefault(name, {"refs": {}, "note": "", "flag": False})
        e["refs"][rel(mod.path)] = e["refs"].get(rel(mod.path), 0) + 1
        e["flag"] = e["flag"] or flag
        if not e["note"]:
            e["note"] = mod.comment_near(node)
    for p in py_files():
        mod = _Mod.get(p)
        for node in mod.tree.body:
            if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name) \
                    and ENV_CONST_RE.match(node.targets[0].id) and isinstance(mod.consts.get(node.targets[0].id), str):
                add(mod.consts[node.targets[0].id], mod, node, bool(FLAG_NAME_RE.search(mod.consts[node.targets[0].id])))
        for node in ast.walk(mod.tree):
            if isinstance(node, ast.Subscript) and ast.unparse(node.value) == "os.environ" \
                    and isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                add(node.slice.value, mod, node)
            if isinstance(node, ast.Call):
                name = _env_name(node, mod)
                if not name:
                    continue
                flag = bool(FLAG_NAME_RE.search(name))
                cur, hops = mod.parents.get(node), 0
                while cur is not None and hops < 4 and not flag:          # .get(X).strip().lower() in ("1", ...)
                    if isinstance(cur, ast.Compare):
                        for cmp in cur.comparators:
                            vals = [cmp.value] if isinstance(cmp, ast.Constant) else \
                                [e.value for e in getattr(cmp, "elts", []) if isinstance(e, ast.Constant)]
                            if any(str(v).lower() in FLAG_VALUES for v in vals):
                                flag = True
                    cur, hops = mod.parents.get(cur), hops + 1
                add(name, mod, node, flag)
    return out


def flag_default(name: str) -> str:
    if "DISABLED" in name:
        return "未設定＝有効（1 で停止）"
    if "PAUSED" in name:
        return "未設定＝停止しない（1 で全停止）"
    return "未設定＝OFF（1／true で ON）"


# ── (f) kintone ───────────────────────────────────────────────────────────────
def kintone_refs() -> dict:
    apps, fields = [], []
    for p in py_files():
        mod = _Mod.get(p)
        write_names: set = set()
        write_codes: set = set()
        for node in ast.walk(mod.tree):
            if not isinstance(node, ast.Call):
                continue
            fname = getattr(node.func, "attr", getattr(node.func, "id", ""))
            if fname == "KintoneApp" and len(node.args) >= 3 and all(isinstance(a, ast.Constant) for a in node.args[:3]):
                apps.append({"label": node.args[0].value, "app_env": node.args[1].value,
                             "token_env": node.args[2].value, "module": rel(p)})
            if fname in KINTONE_WRITE_FUNCS and mod.is_kintone_call(node):
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Dict):
                        for k in sub.keys:
                            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                                write_codes.add(k.value)
                            elif isinstance(k, ast.Name):
                                write_names.add(k.id)
        for name, val in mod.consts.items():
            if name.startswith("FIELD_") and isinstance(val, str):
                rw = "書込" if (name in write_names or val in write_codes) else "読取"
                fields.append({"module": rel(p), "const": name, "code": val, "rw": rw})
        for code in sorted(write_codes):
            if not any(f["module"] == rel(p) and f["code"] == code for f in fields):
                fields.append({"module": rel(p), "const": "（直書き）", "code": code, "rw": "書込"})
    return {"apps": sorted(apps, key=lambda a: (a["app_env"], a["module"])),
            "fields": sorted(fields, key=lambda f: (f["module"], f["code"], f["const"]))}


# ── (g) 文言 ──────────────────────────────────────────────────────────────────
def texts() -> list[dict]:
    path = HERE / "check_reply_wording.py"
    if not path.exists():
        return []
    spec = importlib.util.spec_from_file_location("check_reply_wording", path)
    C = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(C)
    rows = []
    for group, items in C.collect_sources(REPO).items():
        for i, status, text in items:
            rows.append({"group": group, "id": i, "status": STATUS_LABELS.get(status, status), "chars": len(text)})
    return rows


# ── 出力 ──────────────────────────────────────────────────────────────────────
def _table(headers: list[str], rows: list[list]) -> list[str]:
    def esc(s) -> str:
        return str(s).replace("|", "｜").replace("\n", " ")
    return ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)] + \
           ["| " + " | ".join(esc(c) for c in r) + " |" for r in rows] + [""]


def build() -> str:
    L = ["<!-- 自動生成: scripts/gen_inventory.py（INVENTORY-1）。手で編集しない。再生成: python scripts/gen_inventory.py -->",
         "# 全体目録（自動生成）", "",
         "コードから抽出した目録。env の値・token・URL・時刻・SHA は含まない。test_inventory_is_current.py が",
         "生成結果と本ファイルの一致を検査する（差分があれば再生成して commit する）。", ""]
    L += ["## (a) 画面一覧（/app 配下）", ""]
    L += _table(["URL", "method", "必要ロール", "担当モジュール"],
                [[r["path"], r["methods"], r["role"], r["module"]] for r in screens()])
    L += ["## (b) ジョブ一覧", ""]
    L += _table(["名前", "種別", "起動", "本体", "登録モジュール", "登録条件", "台帳 key 形式（モジュール内）", "通知先"],
                [[j["name"], j["kind"], j["when"], j["func"], j["module"], j["cond"] or "—", j["keys"], j["notify"]]
                 for j in jobs()])
    pu = purposes()
    L += ["## (c) 送信台帳 purpose と受信イベント ID（key）形式", "", "### purpose（閉集合）", ""]
    L += _table(["定数", "値", "閉集合 PURPOSES に含む"],
                [[k, v, "はい" if v in pu["closed"] else "いいえ"] for k, v in pu["purposes"]])
    if pu["kinds"]:
        L += ["### purpose V2 の種別（PURPOSE_KINDS）", ""]
        L += _table(["kind", "定義"], [[k, str(v)] for k, v in sorted(dict(pu["kinds"]).items())])
    L += ["### 受信イベント ID（key）の形式（bind_inbound の引数・*ledger_key の戻り値。{…} は実行時の値）", ""]
    L += _table(["形式", "モジュール"], [[k, m] for k, m in pu["keys"]])
    env = env_refs()
    L += ["## (d) env 変数一覧（名前と用途のみ・値は出さない）", ""]
    L += _table(["名前", "参照モジュール（件数）", "用途（コード中の近傍コメント／docstring）"],
                [[n, "、".join(f"{m}({c})" for m, c in sorted(e["refs"].items())), e["note"] or "—"]
                 for n, e in sorted(env.items())])
    L += ["## (e) flag 一覧", ""]
    L += _table(["名前", "既定値（名前規約から）", "ON にすると何が変わるか（コード中の近傍コメント／docstring）", "参照モジュール"],
                [[n, flag_default(n), e["note"] or "—", "、".join(sorted(e["refs"]))]
                 for n, e in sorted(env.items()) if e["flag"]])
    kt = kintone_refs()
    L += ["## (f) kintone アプリ・欄の参照一覧", "", "### アプリ（KintoneApp 定義）", ""]
    L += _table(["表示名", "App ID の env", "token の env", "定義モジュール"],
                [[a["label"], a["app_env"], a["token_env"], a["module"]] for a in kt["apps"]])
    L += ["### 欄（FIELD_* 定数と hub.kintone の update／create の引数）", ""]
    L += _table(["モジュール", "定数", "欄名（フィールドコード）", "読取/書込"],
                [[f["module"], f["const"], f["code"], f["rw"]] for f in kt["fields"]])
    L += ["## (g) 相談者向け文言の一覧", ""]
    L += _table(["対象", "文言 ID", "区分", "文字数"], [[t["group"], t["id"], t["status"], t["chars"]] for t in texts()])
    L += ["承認済の手本・昇格定型は DB 表（reply_exemplar・jikou_reply_text）にありコード外（目録の対象外）。", ""]
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(REPO / "docs" / "inventory.md"))
    ap.add_argument("--check", action="store_true", help="生成結果と --out の一致を検査（不一致は 1）")
    a = ap.parse_args(argv)
    text = build()
    out = pathlib.Path(a.out)
    if a.check:
        current = out.read_text(encoding="utf-8") if out.exists() else ""
        if current == text:
            print("inventory: up to date")
            return 0
        print("inventory: STALE (regenerate)")      # sink は定数のみ（sink 方針）
        return 1
    out.write_text(text, encoding="utf-8", newline="\n")
    print("inventory: written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
