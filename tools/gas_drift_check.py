"""gas_drift_check — repo⇔live GAS の drift 検知（INC-0720 §7 規律の機械化・第1段）

運用:
- [人] が live GAS エディタから各ファイル全文をコピーし `tools/gas_live_snapshot/` に
  同名で貼る（例: `rv04c_signing.js`）。snapshot 実体は .gitignore で除外され
  repo には残らない（live コードを恒久保存しない）。
- PC-A が本ツールを実行し、repo 側（`gas/`・`legacy/gas/`）との差分を機械検査する。

機能:
- (a) SIGNED_LANES 行列の抽出と対比表示（全 5 lane・repo 側=期待行列との一致判定。
      INC-0720 §7(ii) の repo=live 同期規律により repo 値が期待値）
- (b) ファイル単位の一致/不一致と不一致行番号（本文は `--show-content` 指定時のみ）
- (c) manifest 照合（fix2・M02: **正本＝repo の `gas/*.js` と `legacy/gas/*.js` の両方**）:
      snapshot 側の余分（repo に対応なし）と不足（repo 側にあるが snapshot に無い・
      `legacy/gas/コード.js` の欠落も false green として遮断）を検出。

## 終了コード契約（M01 完全化）
- **0 = 一致**（全ファイル一致・SIGNED_LANES 行列一致・manifest 過不足なし）
- **1 = drift 検出**（内容不一致／行列不一致／manifest 過不足）
- **2 = 入力・実行エラー**（snapshot 無し／必須 `rv04c_signing.js` 欠落＝SIGNED_LANES
  比較不能（H02 false green 遮断）／UTF-8 decode 失敗・読取不能／repo-root 不正。
  いずれも traceback ではなく固定文言。**エラー文に生パスは埋めない**（fix2・M01））

## secret 防御（H01）
- 検出パターン: 32+ hex 連続／base64 長文字列／JWT 形式（eyJ 始まり）／Google API key
  （AIza 始まり）／token・secret・key へのリテラル代入／URL query 内の token 値。
- **表示判定は repo 行・live 行の両側**で行い、どちらか該当なら両方を非表示（fix2・H01）。
- secret 様の**ファイル名**は名前自体を出力しない（エラー経路含む）。
- **制御文字（CR/LF 等）を含む表示名**は writer が拒否し、呼び出し側でマスクする
  （fix3・P2DRIFT3-M01: 出力行の偽装・ログ注入の遮断）。

## repo 正本の一意性（fix3・P2DRIFT3-L01）
gas/ と legacy/gas/ に**同名ファイルが両方存在**する場合、照合先が不定になり
false green の温床となるため、drift ではなく**入力エラー（exit 2）**として中断する。

## sink 政策との関係（fix2・H02）
出力は print 直書きではなく**構造化 writer**（`report`／`report_content_line`）に一本化する。
これは sink AST 検査（test_sink_ast_policy）の**回避ではなく構造的制約による担保**:
- `report(out, template_id, **fields)` は **module 定数辞書 `_TEMPLATES` の固定文言**にしか
  展開できず、fields は**型・値域を検証した安全値**（行番号 int・lane 名 enum・件数 int・
  secret 判定済み表示名）のみ受け付ける（自由文字列の合成経路が存在しない）。
- 本文表示が必要な `--show-content` のみ `report_content_line` を使い、**secret 判定を
  関数内部で強制**（呼び出し側が判定を忘れても secret 様本文は出力されない）。

ネットワーク・GAS 実機に依存しない（ローカルファイル比較のみ）。
"""

import argparse
import re
import sys
from pathlib import Path

_LANES = ["/koseki/ingest", "/registry/ingest", "/bank/ingest",
          "/sortation/ingest", "/valuation/ingest"]
_REQUIRED_SNAPSHOT = "rv04c_signing.js"   # H02: SIGNED_LANES 比較の必須ファイル
_REPO_SUBDIRS = ("gas", "legacy/gas")     # M02: manifest 正本（両方を照合対象にする）

# ── secret 様パターン（H01・いずれか該当で当該行/名前を恒久非表示） ──────────
_SECRET_RES = [
    re.compile(r"[0-9a-fA-F]{32,}"),                       # 長 hex（HMAC secret 等）
    re.compile(r"[A-Za-z0-9+/]{40,}={0,2}"),               # base64 長文字列
    re.compile(r"eyJ[A-Za-z0-9_-]{10,}"),                  # JWT 形式
    re.compile(r"AIza[0-9A-Za-z_-]{20,}"),                 # Google API key
    re.compile(r"(?i)(token|secret|key)\w*\s*[:=]\s*['\"][^'\"]{8,}['\"]"),  # リテラル代入
    re.compile(r"[?&]token=[A-Za-z0-9%_\-]{8,}"),          # URL query 内 token 値
]
_SIGNED_LANES_RE = re.compile(r"var SIGNED_LANES = \{(.*?)\};", re.S)
_LANE_ENTRY_RE = re.compile(r"'(/[a-z/]+/ingest)':\s*(true|false)")

_MASK_NAME = "(secret 様のためファイル名非表示).js"
_MASK_LINE = "(secret 様のため非表示)"

# M01(fix3): 制御文字（C0 領域＋DEL）。表示名に混入すると出力行の偽装が可能になる
_CTRL_RE = re.compile(r"[\x00-\x1f\x7f]")


def _is_secret_like(text: str) -> bool:
    return any(rx.search(text) for rx in _SECRET_RES)


def _has_ctrl(text: str) -> bool:
    return bool(_CTRL_RE.search(text))


def _mask_needed(name: str) -> bool:
    """表示名をマスクすべきか（secret 様 or 制御文字混入・fix3 M01）。"""
    return _is_secret_like(name) or _has_ctrl(name)


# ══════════════════════════════════════════════════════════════
# 構造化 writer（fix2・H02）: 固定テンプレート＋検証済み安全値のみ
# ══════════════════════════════════════════════════════════════

_TEMPLATES = {
    "bad_repo_root": "[error] repo-root 不正: 指定パスに gas/ がありません"
                     "（--repo-root を確認してください）",
    "snapshot_unreadable": "[error] snapshot ディレクトリを読み取れません"
                           "（--snapshot-dir を確認してください）",
    "no_snapshot": "[drift-check] snapshot がありません"
                   "（snapshot ディレクトリへ live GAS の .js を貼ってください）",
    "required_missing": "[error] 必須ファイル rv04c_signing.js が snapshot にありません"
                        "（SIGNED_LANES 比較不能のため検査を中断します）",
    "warn_secret_filename": "[warn] secret 様の文字列を含むファイル名の snapshot を検出"
                            "（名前は表示しません）。改名のうえ再実行を推奨します。",
    "warn_ctrl_filename": "[warn] 制御文字を含むファイル名の snapshot を検出"
                          "（名前は表示しません）。改名のうえ再実行を推奨します。",
    "duplicate_repo_name": "[error] {name}: gas/ と legacy/gas/ の両方に存在します"
                           "（repo 正本の一意性エラー・どちらかへ集約してから"
                           "再実行してください）",
    "not_utf8": "[error] {name}: UTF-8 として読めません（貼付内容を確認してください）",
    "file_unreadable": "[error] {name}: 読み取りに失敗しました",
    "warn_secret_lines": "[warn] {name}: secret 様パターンを検出（行 {lines}）。"
                         "該当行の内容は表示しません。誤貼付でないか確認し、"
                         "secret は snapshot に含めないでください。",
    "manifest_extra": "[drift] {name}: repo 側（gas/・legacy/gas/）に対応ファイルなし"
                      "（manifest 余分）",
    "repo_unreadable": "[error] repo 側 {repo_name} を読み取れません",
    "drift_lines": "[drift] {name}: 不一致 {count} 行 (repo={repo_name}) 行番号: {lines}",
    "ok_line": "[ok]    {name}: repo と一致 (repo={repo_name})",
    "matrix_head": "[matrix] SIGNED_LANES（期待=repo・INC-0720 §7(ii)）:",
    "matrix_row": "  {lane}: repo(期待)={rv} live={sv} {mark}",
    "matrix_mismatch": "[drift] SIGNED_LANES 行列が期待（repo）と不一致",
    "manifest_missing": "[drift] {repo_name} が snapshot にありません"
                        "（manifest 不足・live との照合が未完）",
}

# field 検証: 名前ごとに許容型/値域を固定（安全値以外は ValueError）
_BOOLS = ("true", "false", "-")


def _ok_display_name(v) -> bool:
    """表示名: str かつ secret 様でも制御文字混入でもない（マスク済み定数は常に可）。
    制御文字（CR/LF 等）は出力行の偽装が可能なため writer 側で拒否（fix3・M01）。"""
    if v in (_MASK_NAME, _MASK_LINE):
        return True
    return (isinstance(v, str) and len(v) <= 200
            and not _is_secret_like(v) and not _has_ctrl(v))


_FIELD_OK = {
    "name": _ok_display_name,
    "repo_name": _ok_display_name,
    "count": lambda v: isinstance(v, int),
    "lineno": lambda v: isinstance(v, int),
    "lines": lambda v: (isinstance(v, (list, tuple))
                        and all(isinstance(i, int) for i in v)),
    "lane": lambda v: v in _LANES,
    "rv": lambda v: v in _BOOLS,
    "sv": lambda v: v in _BOOLS,
    "mark": lambda v: v in ("OK", "**MISMATCH**"),
}


def report(out, template_id: str, **fields) -> None:
    """固定テンプレート＋検証済み安全値のみを出力する（自由文字列の合成不可）。
    未知の template_id／field 名・検証不合格の値は ValueError（何も出力しない）。"""
    tmpl = _TEMPLATES[template_id]          # 未知 ID は KeyError＝実装バグとして顕在化
    for k, v in fields.items():
        ok = _FIELD_OK.get(k)
        if ok is None or not ok(v):
            raise ValueError(f"report: unsafe field {k!r} for template {template_id!r}")
    out.write(tmpl.format(**fields) + "\n")


def report_content_line(out, lineno: int, side: str, text: str, is_secret: bool) -> None:
    """--show-content の本文 1 行出力。**secret 判定は本関数内部で強制**:
    is_secret=True または text 自体が secret 様なら本文を出力しない
    （呼び出し側が判定を誤っても secret 様本文は構造的に出ない）。"""
    if not isinstance(lineno, int) or side not in ("repo", "live"):
        raise ValueError("report_content_line: invalid lineno/side")
    if is_secret or _is_secret_like(text):
        out.write(f"  L{lineno}: {_MASK_LINE}\n")
        return
    out.write(f"  L{lineno}: {side}: {text}\n")


# ══════════════════════════════════════════════════════════════
# 検査本体
# ══════════════════════════════════════════════════════════════

def extract_signed_lanes(text: str) -> dict | None:
    """SIGNED_LANES ブロックから {path: 'true'|'false'} を抽出。無ければ None。"""
    m = _SIGNED_LANES_RE.search(text)
    if not m:
        return None
    return dict(_LANE_ENTRY_RE.findall(m.group(1)))


def find_secret_lines(text: str) -> list[int]:
    """secret 様パターンを含む行番号（1-indexed）。"""
    return [i for i, line in enumerate(text.splitlines(), 1)
            if _is_secret_like(line)]


def _diff_line_numbers(repo_text: str, snap_text: str) -> list[int]:
    """単純行対比の不一致行番号（1-indexed・行数差も不一致として数える）。"""
    a, b = repo_text.splitlines(), snap_text.splitlines()
    n = max(len(a), len(b))
    return [i + 1 for i in range(n)
            if (a[i] if i < len(a) else None) != (b[i] if i < len(b) else None)]


def _find_repo_file(repo_root: Path, name: str) -> Path | None:
    for sub in _REPO_SUBDIRS:
        p = repo_root / sub / name
        if p.is_file():
            return p
    return None


def run_check(snapshot_dir: Path, repo_root: Path, show_content: bool = False,
              out=sys.stdout) -> int:
    """検査本体。戻り値=終了コード（0 一致／1 drift／2 入力・実行エラー）。"""
    repo_root = Path(repo_root)
    if not (repo_root / "gas").is_dir():
        report(out, "bad_repo_root")
        return 2

    # L01(fix3): gas/ と legacy/gas/ の同名衝突は照合先が不定＝入力エラー（exit 2）
    names_per_sub = []
    for sub in _REPO_SUBDIRS:
        d = repo_root / sub
        names_per_sub.append({p.name for p in d.glob("*.js")} if d.is_dir() else set())
    duplicates = sorted(set.intersection(*names_per_sub))
    if duplicates:
        for dup in duplicates:
            report(out, "duplicate_repo_name",
                   name=(dup if not _mask_needed(dup) else _MASK_NAME))
        return 2

    try:
        snaps = sorted(p for p in Path(snapshot_dir).glob("*.js") if p.is_file())
    except OSError:
        report(out, "snapshot_unreadable")
        return 2
    if not snaps:
        report(out, "no_snapshot")
        return 2

    # H02: 必須ファイル（SIGNED_LANES 比較不能の false green を遮断）
    if _REQUIRED_SNAPSHOT not in {p.name for p in snaps}:
        report(out, "required_missing")
        return 2

    drift = False
    seen_names: set[str] = set()
    for snap in snaps:
        seen_names.add(snap.name)
        # H01: ファイル名自体が secret 様なら名前を出力しない（エラー経路含む）
        # M01(fix3): 制御文字混入のファイル名も同様にマスク（出力行の偽装遮断）
        name_secret = _is_secret_like(snap.name)
        name_masked = name_secret or _has_ctrl(snap.name)
        disp = _MASK_NAME if name_masked else snap.name
        if name_secret:
            report(out, "warn_secret_filename")
        elif name_masked:
            report(out, "warn_ctrl_filename")

        # M01: 読取不能・decode 失敗は固定文言＋exit 2（traceback にしない）
        try:
            snap_text = snap.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            report(out, "not_utf8", name=disp)
            return 2
        except OSError:
            report(out, "file_unreadable", name=disp)
            return 2

        snap_secret = set(find_secret_lines(snap_text))
        if snap_secret:
            report(out, "warn_secret_lines", name=disp, lines=sorted(snap_secret))

        repo_file = _find_repo_file(repo_root, snap.name)
        if repo_file is None:
            report(out, "manifest_extra", name=disp)
            drift = True
            continue
        rel = repo_file.relative_to(repo_root).as_posix()
        repo_disp = _MASK_LINE if name_masked else rel

        try:
            repo_text = repo_file.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            report(out, "repo_unreadable", repo_name=repo_disp)
            return 2

        # H01(fix2): 表示判定は repo/live 両側の secret 行の和集合
        secret_union = snap_secret | set(find_secret_lines(repo_text))

        mismatched = _diff_line_numbers(repo_text, snap_text)
        if mismatched:
            drift = True
            report(out, "drift_lines", name=disp, count=len(mismatched),
                 repo_name=repo_disp, lines=mismatched[:50])
            if show_content:
                a, b = repo_text.splitlines(), snap_text.splitlines()
                for i in mismatched[:50]:
                    sec = i in secret_union
                    if sec:
                        report_content_line(out, i, "repo", "", True)
                        continue
                    ra = a[i - 1] if i - 1 < len(a) else "(repo: 行なし)"
                    rb = b[i - 1] if i - 1 < len(b) else "(live: 行なし)"
                    report_content_line(out, i, "repo", ra, sec)
                    report_content_line(out, i, "live", rb, sec)
        else:
            report(out, "ok_line", name=disp, repo_name=repo_disp)

        # SIGNED_LANES 行列の対比（rv04c_signing.js のみ該当）
        snap_m = extract_signed_lanes(snap_text)
        if snap_m is not None:
            repo_m = extract_signed_lanes(repo_text) or {}
            report(out, "matrix_head")
            matrix_ok = True
            for lane in _LANES:
                rv, sv = repo_m.get(lane, "-"), snap_m.get(lane, "-")
                mark = "OK" if rv == sv else "**MISMATCH**"
                if rv != sv:
                    matrix_ok = False
                report(out, "matrix_row", lane=lane, rv=rv, sv=sv, mark=mark)
            if not matrix_ok:
                drift = True
                report(out, "matrix_mismatch")

    # M02: manifest 照合（不足）— 正本は gas/ と legacy/gas/ の両方
    for sub in _REPO_SUBDIRS:
        d = repo_root / sub
        if not d.is_dir():
            continue
        for repo_js in sorted(d.glob("*.js")):
            if repo_js.name not in seen_names:
                rel = f"{sub}/{repo_js.name}"
                report(out, "manifest_missing",
                     repo_name=(rel if not _mask_needed(rel) else _MASK_LINE))
                drift = True

    return 1 if drift else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="repo⇔live GAS drift 検知（INC-0720 §7）")
    ap.add_argument("--snapshot-dir", default="tools/gas_live_snapshot")
    ap.add_argument("--repo-root", default=".")
    ap.add_argument("--show-content", action="store_true",
                    help="不一致行の本文も表示（secret 様行は常に非表示）")
    args = ap.parse_args(argv)
    return run_check(Path(args.snapshot_dir), Path(args.repo_root),
                     show_content=args.show_content)


if __name__ == "__main__":
    sys.exit(main())
