"""clause_library — 条項ライブラリの器（P5-001・loader＋検証器）

正本: `docs/design-drafts/DRAFT_P5_CLAUSE_LIBRARY.md` §1（schema 案）§2（placeholder
3種別）。裁定（2026-07-27・[人]）:
- **保管形式=repo 内 YAML を正式採用**（`clauses/<version>/*.yaml`・1ファイル=1文書種別。
  git diff で弁護士レビュー可能・`clause_library_version` は directory 名＋内容 hash）。
- **clause_id 命名規約**=英小文字スネークで「文書種別接頭辞＋意味 slug＋版番号」
  （例: `iso_kyogi_intro_v1`）。一意性・grammar は本検証器が機械強制。
- **初期条項は合成のみ**（書式現物到着前・実条項の投入は現物到着後の別票）。

検証器（hub/docx_builder.validate_template の「人が編集して壊す事故を検知する」型）:
- clause_id grammar・ライブラリ内一意
- 適用条件の enum 整合（relation_keys_any ⊆ _RELATION_KEYS／flags_none ⊆
  _LAWYER_FLAG_KEYS＝**hub/derivation_models の保存語彙を単一の正として import**・
  rank_in ⊆ {0,1,2,3}・requires_human は bool）
- placeholder 未定義の拒否（body_template 中の `{{key}}`／`{{行:key}}` は、ファイル
  先頭の `placeholders:` 宣言リストに含まれること）
- order の重複拒否（同一ファイル=同一文書種別内で一意）
- repeat ∈ {none, per_heir}（§2 の3種別: 条件付き条項は applies が担い docx 側に
  条件記法を持たない）

違反は ClauseLibraryError（読み込ませない=fail-closed）。
"""

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from hub.derivation_models import _LAWYER_FLAG_KEYS, _RELATION_KEYS

CLAUSES_ROOT = "clauses"

# clause_id grammar（裁定②: 文書種別接頭辞+意味slug+版番号・英小文字スネーク）
_CLAUSE_ID_RE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)+_v[1-9][0-9]*$")
_PLACEHOLDER_RE = re.compile(r"\{\{(行:)?([^{}]+)\}\}")
_RANKS = {0, 1, 2, 3}
_REPEATS = {"none", "per_heir"}
_APPLIES_KEYS = {"relation_keys_any", "rank_in", "flags_none", "requires_human"}
_CLAUSE_KEYS = {"clause_id", "title", "applies", "body_template",
                "repeat", "order", "since_version", "notes"}
_FILE_KEYS = {"version", "doc_type", "placeholders", "clauses"}


class ClauseLibraryError(ValueError):
    """条項ライブラリの検証違反（grammar・enum・placeholder 未定義・order 重複等）。
    人が YAML を編集して壊した事故を読み込み時に検知する（fail-closed）。"""


@dataclass(frozen=True)
class Clause:
    clause_id: str
    title: str
    applies: dict
    body_template: str
    repeat: str
    order: int
    since_version: str
    notes: str = ""
    doc_type: str = ""
    placeholders_used: tuple = field(default_factory=tuple)


def _validate_applies(clause_id: str, applies) -> None:
    if not isinstance(applies, dict) or set(applies) - _APPLIES_KEYS:
        raise ClauseLibraryError(
            f"{clause_id}: applies は {sorted(_APPLIES_KEYS)} のみ許可")
    rel = applies.get("relation_keys_any", [])
    if not isinstance(rel, list) or not set(rel) <= _RELATION_KEYS:
        raise ClauseLibraryError(
            f"{clause_id}: relation_keys_any が保存語彙（_RELATION_KEYS）外")
    flags = applies.get("flags_none", [])
    if not isinstance(flags, list) or not set(flags) <= _LAWYER_FLAG_KEYS:
        raise ClauseLibraryError(
            f"{clause_id}: flags_none が保存語彙（_LAWYER_FLAG_KEYS）外")
    ranks = applies.get("rank_in", [])
    if not isinstance(ranks, list) or not set(ranks) <= _RANKS:
        raise ClauseLibraryError(f"{clause_id}: rank_in は 0-3 のみ")
    rh = applies.get("requires_human", False)
    if not isinstance(rh, bool):
        raise ClauseLibraryError(f"{clause_id}: requires_human は bool")


def _extract_placeholders(body: str) -> list[str]:
    return [m.group(2).strip() for m in _PLACEHOLDER_RE.finditer(body)]


def _load_file(path: Path) -> list[Clause]:
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ClauseLibraryError(f"{path.name}: YAML 解析失敗（{type(e).__name__}）")
    if not isinstance(doc, dict) or set(doc) != _FILE_KEYS:
        raise ClauseLibraryError(
            f"{path.name}: ファイル最上位は {sorted(_FILE_KEYS)} と完全一致すること")
    declared = doc["placeholders"]
    if not isinstance(declared, list) or not all(isinstance(x, str) for x in declared):
        raise ClauseLibraryError(f"{path.name}: placeholders は文字列リストであること")
    declared_set = set(declared)
    doc_type = str(doc["doc_type"])

    clauses: list[Clause] = []
    seen_orders: set[int] = set()
    for raw in doc["clauses"]:
        if not isinstance(raw, dict) or not _CLAUSE_KEYS >= set(raw):
            raise ClauseLibraryError(
                f"{path.name}: clause フィールドは {sorted(_CLAUSE_KEYS)} のみ許可")
        cid = str(raw.get("clause_id", ""))
        if not _CLAUSE_ID_RE.fullmatch(cid):
            raise ClauseLibraryError(
                "clause_id が命名規約（英小文字スネーク・文書種別接頭辞+意味slug+"
                f"版番号 _vN）に不一致: {cid!r}")
        if not cid.startswith(doc_type + "_"):
            raise ClauseLibraryError(
                f"{cid}: clause_id は doc_type（{doc_type}）接頭辞で始まること")
        _validate_applies(cid, raw.get("applies", {}))
        body = raw.get("body_template", "")
        if not isinstance(body, str) or not body.strip():
            raise ClauseLibraryError(f"{cid}: body_template が空")
        used = _extract_placeholders(body)
        undefined = [p for p in used if p not in declared_set]
        if undefined:
            raise ClauseLibraryError(
                f"{cid}: 未定義 placeholder {undefined}（ファイルの placeholders "
                "宣言に追加してから使用すること）")
        repeat = raw.get("repeat", "none")
        if repeat not in _REPEATS:
            raise ClauseLibraryError(f"{cid}: repeat は {sorted(_REPEATS)} のみ")
        order = raw.get("order")
        if not isinstance(order, int) or isinstance(order, bool):
            raise ClauseLibraryError(f"{cid}: order は整数であること")
        if order in seen_orders:
            raise ClauseLibraryError(
                f"{cid}: order {order} が同一文書種別内で重複")
        seen_orders.add(order)
        clauses.append(Clause(
            clause_id=cid, title=str(raw.get("title", "")),
            applies=raw.get("applies", {}), body_template=body,
            repeat=repeat, order=order,
            since_version=str(raw.get("since_version", "")),
            notes=str(raw.get("notes", "")), doc_type=doc_type,
            placeholders_used=tuple(used)))
    return clauses


def load_library(version: str = "v1",
                 base_dir: str = CLAUSES_ROOT) -> list[Clause]:
    """clauses/<version>/*.yaml を全件検証つきで読み込む（違反は fail-closed）。"""
    root = Path(base_dir) / version
    files = sorted(root.glob("*.yaml"))
    if not files:
        raise ClauseLibraryError(f"条項ファイルがありません: {root.as_posix()}")
    clauses: list[Clause] = []
    seen_ids: set[str] = set()
    for f in files:
        for c in _load_file(f):
            if c.clause_id in seen_ids:
                raise ClauseLibraryError(f"clause_id 重複: {c.clause_id}")
            seen_ids.add(c.clause_id)
            clauses.append(c)
    return clauses


def library_version(version: str = "v1",
                    base_dir: str = CLAUSES_ROOT) -> tuple[str, str]:
    """clause_library_version の実体（DRAFT_P5 §1.1: directory 名＋内容 hash）。

    hash はファイル名昇順に (ファイル名, UTF-8 bytes) を連結した SHA-256。
    TemplateVersion.clause_library_version へは "v1:<hex64>" 形式で記録する想定。"""
    root = Path(base_dir) / version
    h = hashlib.sha256()
    for f in sorted(root.glob("*.yaml")):
        h.update(f.name.encode("utf-8"))
        h.update(f.read_bytes())
    return version, h.hexdigest()
