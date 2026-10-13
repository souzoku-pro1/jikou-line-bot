# -*- coding: utf-8 -*-
"""check_reply_wording — WORDING-1（QUEUE-2026-10-14-G G-4）: 相談者向け文言の安全点検（報告のみ・文言は変更しない）

対象（すべてソースの AST から文字列定数を取り出す＝アプリの import・env・外部 I/O なし）:
 1) hub/jikou_reply_texts.py の定型表 TEXTS（J 定型・status 付き。ファイルが無ければ「対象外」と記す）
 2) chat_responder.py の固定文・即時定型（弁護士確定の凍結文: *_REPLY／*_TEXT の文字列定数）
 3) chat_responder._SYSTEM_PROMPT_BASE の【FAQ…】【費用の定型案内】節の各項目（「- 」で始まる行・継続行を連結）
 4) あいさつ文 hub/shindan_link.FROZEN_GREETING
 5) 凍結ヒアリング台本 main._HEARING_PROMPT_FROZEN（友達追加の自動返信を含む）
語リストは hub/wording_rules.py（要大野確認・仮置き）。検出は報告に載せるだけで、修正はしない（法的判断は大野専権）。
出力は決定的（同じソースなら同じ報告）。報告には文言の抜粋（最大 36 字）と件数のみを載せ、相談者情報は扱わない。

usage: python scripts/check_reply_wording.py --out <report.md> [--repo <repo root>]
"""
import argparse
import ast
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
DEFAULT_REPO = HERE.parent
if str(DEFAULT_REPO) not in sys.path:
    sys.path.insert(0, str(DEFAULT_REPO))

from hub import wording_rules as W  # noqa: E402

GROUP_Q2 = "1) hub/jikou_reply_texts.py（J 定型）"
GROUP_FIXED = "2) chat_responder.py 固定文・即時定型（凍結文）"
GROUP_FAQ = "3) 顧客対応 prompt の FAQ・費用の定型案内"
GROUP_GREETING = "4) あいさつ文（hub/shindan_link.FROZEN_GREETING）"
GROUP_HEARING = "5) 凍結ヒアリング台本（main._HEARING_PROMPT_FROZEN）"
GROUPS = (GROUP_Q2, GROUP_FIXED, GROUP_FAQ, GROUP_GREETING, GROUP_HEARING)

FIXED_NAME_SUFFIXES = ("_REPLY", "_TEXT")
FIXED_NAME_EXCLUDE = ("STYLE_RULES_TEXT", "STYLE_EXEMPLARS_TEXT", "HEARING_STYLE_EXEMPLARS_TEXT", "FEE_GUIDE_MARKER")
FAQ_HEADER_KEYS = ("FAQ", "費用の定型案内")
SNIPPET_MAX = 36


def _module_constants(src: str) -> dict:
    """module top-level の `NAME = <文字列リテラル>`（暗黙連結含む）を集める。"""
    tree = ast.parse(src)
    out: dict = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            try:
                v = ast.literal_eval(node.value)
            except (ValueError, SyntaxError):
                continue
            if isinstance(v, str):
                out[node.targets[0].id] = v
    return out


def _resolve(node, consts: dict):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return consts.get(node.id)
    return None


def collect_q2(repo: pathlib.Path) -> list[tuple[str, str, str]]:
    """(id, status, text)。TEXTS の各要素は `_t(<text>, label=..., status=<NAME>)`。"""
    p = repo / "hub" / "jikou_reply_texts.py"
    if not p.exists():
        return []
    src = p.read_text(encoding="utf-8")
    consts = _module_constants(src)
    tree = ast.parse(src)
    rows = []
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            target = node.targets[0] if isinstance(node, ast.Assign) else node.target
            if not (isinstance(target, ast.Name) and target.id == "TEXTS" and isinstance(node.value, ast.Dict)):
                continue
            for k, v in zip(node.value.keys, node.value.values):
                if not (isinstance(k, ast.Constant) and isinstance(v, ast.Call) and v.args):
                    continue
                text = _resolve(v.args[0], consts)
                status = next((_resolve(kw.value, consts) or getattr(kw.value, "id", "") for kw in v.keywords
                               if kw.arg == "status"), "")
                if text is not None:
                    rows.append((str(k.value), str(status), text))
    return rows


def collect_fixed(repo: pathlib.Path) -> list[tuple[str, str, str]]:
    src = (repo / "chat_responder.py").read_text(encoding="utf-8")
    consts = _module_constants(src)
    rows = []
    for name in sorted(consts):
        if name.startswith("_") or name in FIXED_NAME_EXCLUDE or not name.endswith(FIXED_NAME_SUFFIXES):
            continue
        rows.append((name, "frozen", consts[name]))
    return rows


def collect_faq(repo: pathlib.Path) -> list[tuple[str, str, str]]:
    src = (repo / "chat_responder.py").read_text(encoding="utf-8")
    prompt = _module_constants(src).get("_SYSTEM_PROMPT_BASE", "")
    rows = []
    header, item, idx = "", [], 0

    def flush():
        if item:
            rows.append((f"{header} #{idx}", "prompt", "\n".join(item)))
    for line in prompt.split("\n"):
        if line.startswith("【") and "】" in line:
            flush()
            item, idx = [], 0
            header = line[1:line.index("】")]
            continue
        if not any(key in header for key in FAQ_HEADER_KEYS):
            continue
        if line.startswith("- "):
            flush()
            idx += 1
            item = [line[2:]]
        elif item and line.strip():
            item.append(line.strip())
    flush()
    return rows


def collect_greeting(repo: pathlib.Path) -> list[tuple[str, str, str]]:
    p = repo / "hub" / "shindan_link.py"
    if not p.exists():
        return []
    consts = _module_constants(p.read_text(encoding="utf-8"))
    return [("FROZEN_GREETING", "frozen", consts["FROZEN_GREETING"])] if "FROZEN_GREETING" in consts else []


def collect_hearing(repo: pathlib.Path) -> list[tuple[str, str, str]]:
    consts = _module_constants((repo / "main.py").read_text(encoding="utf-8"))
    text = consts.get("_HEARING_PROMPT_FROZEN")
    return [("_HEARING_PROMPT_FROZEN", "frozen", text)] if text else []


def collect_sources(repo: pathlib.Path) -> dict:
    return {GROUP_Q2: collect_q2(repo), GROUP_FIXED: collect_fixed(repo), GROUP_FAQ: collect_faq(repo),
            GROUP_GREETING: collect_greeting(repo), GROUP_HEARING: collect_hearing(repo)}


def run(repo: pathlib.Path) -> dict:
    """{"groups": {group: [(id, status, text, [hits])]}, "counts": {group: {rule: n}}}"""
    groups, counts = {}, {}
    for g, rows in collect_sources(repo).items():
        groups[g] = [(i, s, t, W.check_text(t)) for i, s, t in rows]
        counts[g] = {r: sum(1 for _i, _s, _t, hits in groups[g] for h in hits if h["rule"] == r) for r in W.RULES}
    return {"groups": groups, "counts": counts}


def render(result: dict) -> str:
    lines = ["# WORDING-1 相談者向け文言の安全点検（報告のみ・文言は未修正）", "",
             "- 語リスト・正規表現は hub/wording_rules.py の仮置き（**要大野確認**）。検出＝要確認であって誤りの断定ではない。",
             "- 対象はソースの文字列定数（AST 抽出）。相談者情報は扱わない。抜粋は最大 36 字・電話番号様の数字列は伏せる。",
             "- 文言の修正はしない（法的判断は大野専権）。凍結文・sha pin は不変。", ""]
    lines += ["## 件数", "", "| 対象 | 文言数 | " + " | ".join(W.RULE_LABELS[r] for r in W.RULES) + " |",
              "|---|---|" + "---|" * len(W.RULES)]
    for g in GROUPS:
        rows = result["groups"].get(g, [])
        c = result["counts"].get(g, {})
        n = len(rows) if rows else "対象外（ファイルなし）" if g == GROUP_Q2 else 0
        lines.append(f"| {g} | {n} | " + " | ".join(str(c.get(r, 0)) for r in W.RULES) + " |")
    lines.append("")
    for g in GROUPS:
        rows = result["groups"].get(g, [])
        lines += [f"## {g}", ""]
        if not rows:
            lines += ["（対象なし）", ""]
            continue
        hit_rows = [(i, s, t, hits) for i, s, t, hits in rows if hits]
        if not hit_rows:
            lines += ["検出なし。", ""]
            continue
        lines += ["| 文言 ID | 状態 | 観点 | 語 | 抜粋 |", "|---|---|---|---|---|"]
        for i, s, _t, hits in hit_rows:
            for h in hits:
                snip = h["snippet"][:SNIPPET_MAX].replace("|", "｜")
                lines.append(f"| {i} | {s} | {W.RULE_LABELS[h['rule']]} | {h['match']} | …{snip}… |")
        lines.append("")
    lines += ["## 検出ゼロの文言（観点 (a)〜(e) のいずれにも当たらない）", ""]
    for g in GROUPS:
        clean = [i for i, _s, _t, hits in result["groups"].get(g, []) if not hits]
        if clean:
            lines.append(f"- {g}: " + "、".join(clean))
    lines.append("")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--repo", default=str(DEFAULT_REPO))
    a = ap.parse_args(argv)
    report = render(run(pathlib.Path(a.repo)))
    pathlib.Path(a.out).write_text(report, encoding="utf-8", newline="\n")
    print("wording check: report written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
