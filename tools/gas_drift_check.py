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
- (c) manifest 照合: repo 側 `gas/*.js` の不足（snapshot に無い）・snapshot 側の余分
      （repo に対応なし）を検出（fix1・照合不足を silent pass にしない）

## 終了コード契約（fix1・M01 完全化）
- **0 = 一致**（全ファイル一致・SIGNED_LANES 行列一致・manifest 過不足なし）
- **1 = drift 検出**（内容不一致／行列不一致／manifest 過不足）
- **2 = 入力・実行エラー**（snapshot 無し／必須 `rv04c_signing.js` 欠落＝SIGNED_LANES
  比較不能（fix1・H02 false green 遮断）／UTF-8 decode 失敗・読取不能／repo-root 不正。
  いずれも traceback ではなく固定文言）

## secret 防御（fix1・H01 拡張）
snapshot 内に secret 様パターンを検出したら警告し、該当行の内容は
`--show-content` 指定時でも恒久非表示。対象: 32+ hex 連続／base64 長文字列／
JWT 形式（eyJ 始まり）／Google API key（AIza 始まり）／token・secret・key への
リテラル代入／URL query 内の token 値。**ファイル名**に secret 様文字列が含まれる
場合はファイル名自体も出力しない。

ネットワーク・GAS 実機に依存しない（ローカルファイル比較のみ）。
"""

import argparse
import re
import sys
from pathlib import Path

_LANES = ["/koseki/ingest", "/registry/ingest", "/bank/ingest",
          "/sortation/ingest", "/valuation/ingest"]
_REQUIRED_SNAPSHOT = "rv04c_signing.js"   # H02: SIGNED_LANES 比較の必須ファイル

# ── secret 様パターン（H01 拡張・いずれか該当で当該行を恒久非表示） ──────────
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


def _w(out, msg: str) -> None:
    """レポート出力（CLI レポート専用 writer）。print 直書きにしない:
    repo の sink AST 方針（test_sink_ast_policy）は print を redaction sink として
    検査するため、出力は本 writer に一本化する。secret 非表示は本ツール自身が
    _is_secret_like / find_secret_lines で担保する（出力内容は行番号・lane 名・
    固定文言のみ）。"""
    out.write(msg + "\n")


def _is_secret_like(text: str) -> bool:
    return any(rx.search(text) for rx in _SECRET_RES)


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
    for rel in (Path("gas") / name, Path("legacy") / "gas" / name):
        p = repo_root / rel
        if p.is_file():
            return p
    return None


def run_check(snapshot_dir: Path, repo_root: Path, show_content: bool = False,
              out=sys.stdout) -> int:
    """検査本体。戻り値=終了コード（0 一致／1 drift／2 入力・実行エラー）。"""
    repo_root = Path(repo_root)
    if not (repo_root / "gas").is_dir():
        _w(out, f"[error] repo-root 不正: {repo_root} に gas/ がありません"
              "（--repo-root を確認してください）")
        return 2

    try:
        snaps = sorted(p for p in Path(snapshot_dir).glob("*.js") if p.is_file())
    except OSError:
        _w(out, f"[error] snapshot ディレクトリを読み取れません: {snapshot_dir}")
        return 2
    if not snaps:
        _w(out, f"[drift-check] snapshot が {snapshot_dir} にありません"
              "（live GAS から .js を貼ってください）")
        return 2

    # H02: 必須ファイル（SIGNED_LANES 比較不能の false green を遮断）
    if _REQUIRED_SNAPSHOT not in {p.name for p in snaps}:
        _w(out, f"[error] 必須ファイル {_REQUIRED_SNAPSHOT} が snapshot にありません"
              "（SIGNED_LANES 比較不能のため検査を中断します）")
        return 2

    drift = False
    seen_names: set[str] = set()
    for snap in snaps:
        seen_names.add(snap.name)
        # H01: ファイル名自体が secret 様なら名前を出力しない
        name_secret = _is_secret_like(snap.name)
        disp = "(secret 様のためファイル名非表示).js" if name_secret else snap.name
        if name_secret:
            _w(out, "[warn] secret 様の文字列を含むファイル名の snapshot を検出"
                  "（名前は表示しません）。改名のうえ再実行を推奨します。")

        # M01: 読取不能・decode 失敗は固定文言＋exit 2（traceback にしない）
        try:
            snap_text = snap.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            _w(out, f"[error] {disp}: UTF-8 として読めません（貼付内容を確認してください）")
            return 2
        except OSError:
            _w(out, f"[error] {disp}: 読み取りに失敗しました")
            return 2

        # secret 様パターンの防御（該当行の内容は一切出さない）
        secret_lines = find_secret_lines(snap_text)
        if secret_lines:
            _w(out, f"[warn] {disp}: secret 様パターンを検出（行 {secret_lines}）。"
                  "該当行の内容は表示しません。誤貼付でないか確認し、"
                  "secret は snapshot に含めないでください。")

        repo_file = _find_repo_file(repo_root, snap.name)
        if repo_file is None:
            _w(out, f"[drift] {disp}: repo 側（gas/・legacy/gas/）に対応ファイルなし"
                  "（manifest 余分）")
            drift = True
            continue
        repo_disp = "(secret 様のため非表示)" if name_secret else repo_file.as_posix()

        try:
            repo_text = repo_file.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            _w(out, f"[error] repo 側 {repo_disp} を読み取れません")
            return 2

        mismatched = _diff_line_numbers(repo_text, snap_text)
        if mismatched:
            drift = True
            _w(out, f"[drift] {disp}: 不一致 {len(mismatched)} 行 "
                  f"(repo={repo_disp}) 行番号: {mismatched[:50]}")
            if show_content:
                secret_set = set(secret_lines)
                a, b = repo_text.splitlines(), snap_text.splitlines()
                for i in mismatched[:50]:
                    if i in secret_set:
                        _w(out, f"  L{i}: (secret 様のため非表示)")
                        continue
                    ra = a[i - 1] if i - 1 < len(a) else "(repo: 行なし)"
                    rb = b[i - 1] if i - 1 < len(b) else "(live: 行なし)"
                    _w(out, f"  L{i}: repo: {ra}")
                    _w(out, f"  L{i}: live: {rb}")
        else:
            _w(out, f"[ok]    {disp}: repo と一致 (repo={repo_disp})")

        # SIGNED_LANES 行列の対比（rv04c_signing.js のみ該当）
        snap_m = extract_signed_lanes(snap_text)
        if snap_m is not None:
            repo_m = extract_signed_lanes(repo_text) or {}
            _w(out, "[matrix] SIGNED_LANES（期待=repo・INC-0720 §7(ii)）:")
            matrix_ok = True
            for lane in _LANES:
                rv, sv = repo_m.get(lane, "-"), snap_m.get(lane, "-")
                mark = "OK" if rv == sv else "**MISMATCH**"
                if rv != sv:
                    matrix_ok = False
                _w(out, f"  {lane}: repo(期待)={rv} live={sv} {mark}")
            if not matrix_ok:
                drift = True
                _w(out, "[drift] SIGNED_LANES 行列が期待（repo）と不一致")

    # manifest 照合（不足）: repo 側 gas/*.js のうち snapshot に無いもの
    for repo_js in sorted((repo_root / "gas").glob("*.js")):
        if repo_js.name not in seen_names:
            _w(out, f"[drift] gas/{repo_js.name} が snapshot にありません"
                  "（manifest 不足・live との照合が未完）")
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
