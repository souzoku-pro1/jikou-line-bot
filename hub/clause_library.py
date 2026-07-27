"""clause_library — 条項ライブラリの器（P5-001・loader＋検証器）

正本: `docs/design-drafts/DRAFT_P5_CLAUSE_LIBRARY.md` §1（schema 案）§2（placeholder
3種別）。裁定（2026-07-27・[人]）:
- **保管形式=repo 内 YAML を正式採用**（`clauses/<version>/*.yaml`・1ファイル=1文書種別。
  git diff で弁護士レビュー可能・`clause_library_version` は directory 名＋内容 hash）。
- **clause_id 命名規約**=英小文字スネークで「文書種別接頭辞＋意味 slug＋版番号」
  （例: `iso_kyogi_intro_v1`）。一意性・grammar は本検証器が機械強制。
- **初期条項は合成のみ**（書式現物到着前・実条項の投入は現物到着後の別票）。

検証器（hub/docx_builder.validate_template の「人が編集して壊す事故を検知する」型）:
- clause_id grammar・ライブラリ内一意／(doc_type, order) の**全 library 一意**・
  doc_type の重複ファイル拒否・**ファイル名 stem = doc_type**（「1ファイル=1文書種別」
  の機械強制・fix1 H01）
- 適用条件の enum 整合（relation_keys_any ⊆ _RELATION_KEYS／flags_none ⊆
  _LAWYER_FLAG_KEYS＝**hub/derivation_models の保存語彙を単一の正として import**・
  rank_in は各要素 `type(x) is int` かつ 0〜3〔bool 遮断〕・requires_human は bool。
  非 str 要素（dict/list 等 unhashable 含む）は固定分類で拒否＝未処理 TypeError を
  出さない・fix1 H02）
- placeholder は**専用 parser**（fix1 H02）: `{{key}}`／`{{行:key}}` の閉集合のみ許可・
  `{{ }}` の対応（未閉鎖・単独 brace・入れ子）検査・空キー拒否・宣言済みキーのみ
- **必須 field の完全照合**（fix1 M01）: clause は必須 {clause_id, title, applies,
  body_template, repeat, order, since_version}＋任意 {notes} のみ。欠落の暗黙補完を
  しない・文字列 field は YAML 入力時点で str 型を強制（暗黙 str() 変換なし）・
  ファイル内 version=directory version・since_version ≤ directory version を検証
- repeat ∈ {none, per_heir}

library_version()（fix1 H03）: hash 算出前に load_library と同一の検証器を通す
（空・不正 library では hash を返さない）。version は grammar ^v[1-9][0-9]*$ で検証し
resolve 後に base_dir 直下であることを確認（path traversal 遮断）。hash 材料は
`filename + NUL + sha256(content) + NUL` の連結（ファイル境界の曖昧性を排除）。

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
_VERSION_RE = re.compile(r"^v[1-9][0-9]*$")
_ROW_MARKER = "行:"
_RANKS = {0, 1, 2, 3}
_REPEATS = {"none", "per_heir"}
_APPLIES_KEYS = {"relation_keys_any", "rank_in", "flags_none", "requires_human"}
_REQUIRED_CLAUSE_KEYS = {"clause_id", "title", "applies", "body_template",
                         "repeat", "order", "since_version"}
_OPTIONAL_CLAUSE_KEYS = {"notes"}                    # 任意はこの明示リストのみ
_CLAUSE_KEYS = _REQUIRED_CLAUSE_KEYS | _OPTIONAL_CLAUSE_KEYS
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


def _resolve_root(version: str, base_dir: str) -> Path:
    """version grammar 検証＋resolve 後の base_dir 直下確認（fix1 H03-ii）。"""
    if not isinstance(version, str) or not _VERSION_RE.fullmatch(version):
        raise ClauseLibraryError(
            "version は grammar ^v[1-9][0-9]*$ に従うこと")
    base = Path(base_dir).resolve()
    root = (base / version).resolve()
    if root.parent != base:
        raise ClauseLibraryError("version が base_dir 直下を指していない")
    return root


def _require_str(name: str, value, where: str) -> str:
    """YAML 入力時点の文字列型強制（暗黙 str() 変換の廃止・fix1 M01）。"""
    if type(value) is not str:
        raise ClauseLibraryError(f"{where}: {name} は文字列であること")
    return value


def _validate_str_subset(cid: str, name: str, values, vocab: set) -> None:
    """全要素 str 型を検証してから集合比較（unhashable の未処理 TypeError 遮断・
    fix1 H02）。"""
    if not isinstance(values, list):
        raise ClauseLibraryError(f"{cid}: {name} はリストであること")
    if not all(type(x) is str for x in values):
        raise ClauseLibraryError(f"{cid}: {name} の要素は文字列のみ許可")
    if not set(values) <= vocab:
        raise ClauseLibraryError(f"{cid}: {name} が保存語彙外")


def _validate_applies(clause_id: str, applies) -> None:
    if not isinstance(applies, dict) or set(applies) - _APPLIES_KEYS:
        raise ClauseLibraryError(
            f"{clause_id}: applies は {sorted(_APPLIES_KEYS)} のみ許可")
    _validate_str_subset(clause_id, "relation_keys_any",
                         applies.get("relation_keys_any", []), _RELATION_KEYS)
    _validate_str_subset(clause_id, "flags_none",
                         applies.get("flags_none", []), _LAWYER_FLAG_KEYS)
    ranks = applies.get("rank_in", [])
    if not isinstance(ranks, list) or not all(
            type(x) is int and x in _RANKS for x in ranks):
        raise ClauseLibraryError(
            f"{clause_id}: rank_in は整数 0-3 のみ（bool・文字列は不可）")
    rh = applies.get("requires_human", False)
    if not isinstance(rh, bool):
        raise ClauseLibraryError(f"{clause_id}: requires_human は bool")


def _parse_placeholders(cid: str, body: str) -> list[str]:
    """placeholder parser（fix1 H02）。許可記法は `{{key}}`／`{{行:key}}` の閉集合。
    単独 brace・未閉鎖・入れ子・空キー・前後空白・キー内の ':' は拒否。"""
    used: list[str] = []
    i, n = 0, len(body)
    while i < n:
        ch = body[i]
        if ch == "{":
            if body[i:i + 2] != "{{":
                raise ClauseLibraryError(
                    f"{cid}: 単独の '{{' は placeholder 記法違反")
            end = body.find("}}", i + 2)
            if end == -1:
                raise ClauseLibraryError(f"{cid}: placeholder が閉じていない")
            inner = body[i + 2:end]
            if "{" in inner or "}" in inner or "\n" in inner:
                raise ClauseLibraryError(
                    f"{cid}: placeholder の入れ子・改行は記法違反")
            key = inner[len(_ROW_MARKER):] if inner.startswith(_ROW_MARKER) \
                else inner
            if not key or key != key.strip() or ":" in key:
                raise ClauseLibraryError(
                    f"{cid}: placeholder キーが空・空白付き・記法外")
            used.append(key)
            i = end + 2
        elif ch == "}":
            raise ClauseLibraryError(f"{cid}: 対応しない '}}' がある")
        else:
            i += 1
    return used


def _since_version_ok(since: str, version: str) -> bool:
    return int(since[1:]) <= int(version[1:])


def _load_file(path: Path, version: str) -> tuple[str, list[Clause]]:
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ClauseLibraryError(f"{path.name}: YAML 解析失敗（{type(e).__name__}）")
    if not isinstance(doc, dict) or set(doc) != _FILE_KEYS:
        raise ClauseLibraryError(
            f"{path.name}: ファイル最上位は {sorted(_FILE_KEYS)} と完全一致すること")
    file_version = _require_str("version", doc["version"], path.name)
    if file_version != version:
        raise ClauseLibraryError(
            f"{path.name}: ファイル内 version（{file_version}）が directory "
            f"version（{version}）と不一致")
    doc_type = _require_str("doc_type", doc["doc_type"], path.name)
    if path.stem != doc_type:
        raise ClauseLibraryError(
            f"{path.name}: ファイル名 stem と doc_type（{doc_type}）が不一致"
            "（1ファイル=1文書種別の機械強制）")
    declared = doc["placeholders"]
    if not isinstance(declared, list) or not all(
            type(x) is str for x in declared):
        raise ClauseLibraryError(f"{path.name}: placeholders は文字列リストであること")
    declared_set = set(declared)

    if not isinstance(doc["clauses"], list):
        raise ClauseLibraryError(f"{path.name}: clauses はリストであること")
    clauses: list[Clause] = []
    for raw in doc["clauses"]:
        if not isinstance(raw, dict):
            raise ClauseLibraryError(f"{path.name}: clause は mapping であること")
        missing = _REQUIRED_CLAUSE_KEYS - set(raw)
        if missing:                       # 欠落の暗黙補完はしない（fix1 M01）
            raise ClauseLibraryError(
                f"{path.name}: 必須 field 欠落 {sorted(missing)}")
        unknown = set(raw) - _CLAUSE_KEYS
        if unknown:
            raise ClauseLibraryError(
                f"{path.name}: 未知 field {sorted(unknown)}（任意は "
                f"{sorted(_OPTIONAL_CLAUSE_KEYS)} のみ）")
        cid = _require_str("clause_id", raw["clause_id"], path.name)
        if not _CLAUSE_ID_RE.fullmatch(cid):
            raise ClauseLibraryError(
                "clause_id が命名規約（英小文字スネーク・文書種別接頭辞+意味slug+"
                f"版番号 _vN）に不一致: {cid!r}")
        if not cid.startswith(doc_type + "_"):
            raise ClauseLibraryError(
                f"{cid}: clause_id は doc_type（{doc_type}）接頭辞で始まること")
        title = _require_str("title", raw["title"], cid)
        _validate_applies(cid, raw["applies"])
        body = _require_str("body_template", raw["body_template"], cid)
        if not body.strip():
            raise ClauseLibraryError(f"{cid}: body_template が空")
        used = _parse_placeholders(cid, body)
        undefined = [p for p in used if p not in declared_set]
        if undefined:
            raise ClauseLibraryError(
                f"{cid}: 未定義 placeholder {undefined}（ファイルの placeholders "
                "宣言に追加してから使用すること）")
        repeat = _require_str("repeat", raw["repeat"], cid)
        if repeat not in _REPEATS:
            raise ClauseLibraryError(f"{cid}: repeat は {sorted(_REPEATS)} のみ")
        order = raw["order"]
        if type(order) is not int:        # bool も遮断（type is int）
            raise ClauseLibraryError(f"{cid}: order は整数であること")
        since = _require_str("since_version", raw["since_version"], cid)
        if not _VERSION_RE.fullmatch(since):
            raise ClauseLibraryError(
                f"{cid}: since_version は grammar ^v[1-9][0-9]*$ に従うこと")
        if not _since_version_ok(since, version):
            raise ClauseLibraryError(
                f"{cid}: since_version（{since}）が directory version"
                f"（{version}）より新しい")
        notes = raw.get("notes", "")
        if "notes" in raw:
            notes = _require_str("notes", raw["notes"], cid)
        clauses.append(Clause(
            clause_id=cid, title=title, applies=raw["applies"],
            body_template=body, repeat=repeat, order=order,
            since_version=since, notes=notes, doc_type=doc_type,
            placeholders_used=tuple(used)))
    return doc_type, clauses


def load_library(version: str = "v1",
                 base_dir: str = CLAUSES_ROOT) -> list[Clause]:
    """clauses/<version>/*.yaml を全件検証つきで読み込む（違反は fail-closed）。"""
    root = _resolve_root(version, base_dir)
    files = sorted(root.glob("*.yaml"))
    if not files:
        raise ClauseLibraryError(f"条項ファイルがありません: {root.as_posix()}")
    clauses: list[Clause] = []
    seen_ids: set[str] = set()
    seen_doc_types: set[str] = set()
    seen_orders: set[tuple[str, int]] = set()
    for f in files:
        doc_type, file_clauses = _load_file(f, version)
        if doc_type in seen_doc_types:    # fix1 H01: doc_type の重複ファイル拒否
            raise ClauseLibraryError(
                f"{f.name}: doc_type（{doc_type}）が複数ファイルで重複")
        seen_doc_types.add(doc_type)
        for c in file_clauses:
            if c.clause_id in seen_ids:
                raise ClauseLibraryError(f"clause_id 重複: {c.clause_id}")
            seen_ids.add(c.clause_id)
            key = (c.doc_type, c.order)   # fix1 H01: 全 library で一意
            if key in seen_orders:
                raise ClauseLibraryError(
                    f"{c.clause_id}: (doc_type, order)={key} が library 内で重複")
            seen_orders.add(key)
            clauses.append(c)
    return clauses


def _version_material(entries: list[tuple[str, bytes]]) -> bytes:
    """hash 材料の固定構造（fix1 H03-iii）: ファイルごとに
    `filename + NUL + sha256(content).hexdigest + NUL` を連結。ファイル名と内容・
    ファイル同士の境界が NUL と固定長 digest で一意に定まり、連結バイト列が同じで
    境界だけ異なる 2 library が同一 hash になる曖昧性を排除する。"""
    parts = []
    for name, data in entries:
        parts.append(name.encode("utf-8") + b"\x00"
                     + hashlib.sha256(data).hexdigest().encode("ascii") + b"\x00")
    return b"".join(parts)


def library_version(version: str = "v1",
                    base_dir: str = CLAUSES_ROOT) -> tuple[str, str]:
    """clause_library_version の実体（DRAFT_P5 §1.1: directory 名＋内容 hash）。

    fix1 H03: 算出前に load_library と同一の検証器を全ファイルへ通す＝空・検証不合格の
    library では hash を返さない（不正内容に版識別子を与えない）。
    TemplateVersion.clause_library_version へは "v1:<hex64>" 形式で記録する想定。"""
    root = _resolve_root(version, base_dir)
    load_library(version, base_dir)       # 検証（違反は ClauseLibraryError で中止）
    entries = [(f.name, f.read_bytes()) for f in sorted(root.glob("*.yaml"))]
    return version, hashlib.sha256(_version_material(entries)).hexdigest()
