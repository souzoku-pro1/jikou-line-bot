"""相続関係図のレンダラ（Z2・graphviz dot 生成→SVG/PDF）

設計: docs/koseki-ocr/05 §1・2026-07-07 Z2 裁定
- 入力は Z1（kinship_graph）の中間表現のみ。**生成前提は必ず
  validate_for_rendering を通し、拒否時は列挙を返して描画しない**
- 描画規則（正本＋Z1属性）:
  世代=rank 段組（dot の階層レイアウトに委譲）・**夫婦は不可視ノードで連結し
  子への垂線は夫婦間から**・死亡=グレー塗り＋没年・被相続人=二重枠
  （peripheries=2）・代襲=注記・養親子=破線で実親子と区別・氏名は原文表記
- graphviz バイナリ（dot）は nixpacks.toml で追加（インフラ変更・最小限）。
  不在時は GraphvizUnavailable の明示縮退（他機能は落とさない・/health に表示）
- SVG/PDF の案件アプリ（App 26）添付は関数として実装のみ
  （**本番実行はしない**・実機は明示指示待ち。添付先の FILE フィールドは
  既定コード「関係図」= App 26 側への追加が必要な人作業）
"""

import shutil
import subprocess

from hub import kintone
from kinship_graph import Edge, KinshipGraph, PersonNode, validate_for_rendering

APP_CASE = kintone.KintoneApp(
    "相談カード (相続)", "SOUZOKU_KINTONE_APP_ID", "SOUZOKU_KINTONE_API_TOKEN")

_FONT = "IPAexGothic"  # 実機コンテナのフォント解決は Z2 実機検収の確認点


class KinshipRenderError(Exception):
    """描画の失敗（dot 実行エラー等）"""


class KinshipValidationRejected(KinshipRenderError):
    """生成前提の未充足（problems に人物×項目の列挙・Z1 の検証結果そのまま）"""

    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("生成前提を満たしていません: " + " / ".join(problems))


class GraphvizUnavailable(KinshipRenderError):
    """dot バイナリ不在（関係図の描画のみ不可・他機能は正常の縮退）"""


def _esc(text: str) -> str:
    """dot の二重引用符文字列のエスケープ（氏名に引用符・バックスラッシュ等）"""
    return (text or "").replace("\\", "\\\\").replace('"', '\\"')


def _label(n: PersonNode) -> str:
    parts = [n.name]
    if n.birth_wareki:
        parts.append(f"{n.birth_wareki}生")
    if n.alive == "死亡":
        parts.append(f"（{n.death_wareki}没）" if n.death_wareki else "（死亡）")
    if n.daishu_candidate:
        parts.append("（代襲）")
    return "\\n".join(_esc(p) for p in parts)


def _node_attrs(n: PersonNode) -> str:
    attrs = [f'label="{_label(n)}"']
    if n.is_decedent:
        attrs.append("peripheries=2")  # 被相続人=二重枠
    if n.alive == "死亡":
        attrs.append('style=filled fillcolor=gray80')  # 死亡=グレー
    return " ".join(attrs)


def to_dot(graph: KinshipGraph) -> str:
    """中間表現 → dot（純関数・検証はしない。検証込みは render_kinship）"""
    lines = [
        "digraph kinship {",
        "  rankdir=TB;",
        f'  node [shape=box fontname="{_FONT}"];',
        f'  edge [fontname="{_FONT}"];',
    ]
    for n in graph.nodes:
        lines.append(f'  "p{n.record_id}" [{_node_attrs(n)}];')

    marriages = [e for e in graph.edges if e.kind == "婚姻"]
    couples: dict[frozenset, str] = {}
    for e in marriages:
        mid = f"m{e.a}_{e.b}"
        couples[frozenset((e.a, e.b))] = mid
        # 夫婦は不可視ノードで連結（子への垂線は夫婦間の点から出す・正本の定石）
        lines.append(f'  "{mid}" [shape=point width=0.02 label=""];')
        lines.append(f'  {{ rank=same; "p{e.a}"; "{mid}"; "p{e.b}"; }}')
        lines.append(f'  "p{e.a}" -> "{mid}" [dir=none];')
        lines.append(f'  "{mid}" -> "p{e.b}" [dir=none];')

    # 実親子: 両親が夫婦なら夫婦点から・それ以外は親から直接
    parents_of: dict[str, list[str]] = {}
    for e in graph.edges:
        if e.kind == "親子":
            parents_of.setdefault(e.b, []).append(e.a)
    for child, parents in parents_of.items():
        couple = couples.get(frozenset(parents)) if len(parents) == 2 else None
        if couple:
            lines.append(f'  "{couple}" -> "p{child}";')
        else:
            for parent in parents:
                lines.append(f'  "p{parent}" -> "p{child}";')

    # 養親子: 破線で実親子と区別
    for e in graph.edges:
        if e.kind == "養親子":
            lines.append(f'  "p{e.a}" -> "p{e.b}" [style=dashed];')

    lines.append("}")
    return "\n".join(lines) + "\n"


def render_kinship(graph: KinshipGraph, fmt: str = "svg",
                   heir_scope: bool = False) -> bytes:
    """検証 → dot 生成 → graphviz 実行。SVG/PDF のバイト列を返す。

    - 生成前提の未充足は KinshipValidationRejected（problems=Z1 の列挙）で
      **描画しない**（dot も実行しない）
    - dot バイナリ不在は GraphvizUnavailable（縮退・他機能に影響させない）
    - heir_scope=True（R4-3・D-5 裁定）: 検証要求を「相続人確定に必要な人物」
      （heir_derivation.required_persons）に絞る。被相続人が特定できない場合は
      従来どおり全ノード検証に縮退。既定 False=従来挙動（後方互換）
    """
    required_ids = None
    if heir_scope:
        from heir_derivation import required_persons  # 遅延 import（循環回避）
        decedents = graph.decedents()
        if len(decedents) == 1:
            required_ids = required_persons(graph, decedents[0])
    problems = validate_for_rendering(graph, required_ids)
    if problems:
        raise KinshipValidationRejected(problems)
    if fmt not in ("svg", "pdf"):
        raise KinshipRenderError(f"未対応の出力形式: {fmt}")
    dot_bin = shutil.which("dot")
    if not dot_bin:
        raise GraphvizUnavailable(
            "graphviz（dot）バイナリがありません。nixpacks.toml の graphviz 追加を"
            "含むデプロイが必要です（関係図の描画のみ不可・他機能は正常）")
    result = subprocess.run([dot_bin, f"-T{fmt}"],
                            input=to_dot(graph).encode("utf-8"),
                            capture_output=True)
    if result.returncode != 0:
        raise KinshipRenderError(
            f"dot 実行エラー: {result.stderr.decode('utf-8', errors='replace')[:300]}")
    return result.stdout


async def attach_kinship_to_case(case_record_id: str, graph: KinshipGraph,
                                 field_code: str = "関係図") -> dict:
    """SVG＋PDF を生成して案件アプリ（App 26）の FILE フィールドへ添付する。

    ⚠ 本番実行は明示指示待ち（Z2 は関数実装まで）。field_code のフィールドが
    App 26 に存在しない場合は kintone が 400 を返す（フィールド追加は人作業）。
    """
    files = []
    for fmt, mime in (("svg", "image/svg+xml"), ("pdf", "application/pdf")):
        content = render_kinship(graph, fmt)
        key = await kintone.upload_file(
            APP_CASE, f"相続関係図.{fmt}", content, mime)
        files.append({"fileKey": key})
    await kintone.update_record(APP_CASE, case_record_id, {field_code: files})
    return {"status": "attached", "case_record_id": case_record_id,
            "files": len(files)}
