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
- (c) 終了コード: 一致=0／drift 検出=1／snapshot 無し=2（自動チェック組込み用）
- secret 様パターン（長い hex 文字列等）を snapshot 内に検出したら警告し、
  該当行の内容は表示しない（誤貼付の防御・--show-content でも出さない）

ネットワーク・GAS 実機に依存しない（ローカルファイル比較のみ）。
"""

import argparse
import re
import sys
from pathlib import Path

_LANES = ["/koseki/ingest", "/registry/ingest", "/bank/ingest",
          "/sortation/ingest", "/valuation/ingest"]
# secret 様パターン: 32 文字以上の hex 連続（HMAC secret 等の誤貼付検知）
_SECRET_RE = re.compile(r"[0-9a-fA-F]{32,}")
_SIGNED_LANES_RE = re.compile(r"var SIGNED_LANES = \{(.*?)\};", re.S)
_LANE_ENTRY_RE = re.compile(r"'(/[a-z/]+/ingest)':\s*(true|false)")


def extract_signed_lanes(text: str) -> dict | None:
    """SIGNED_LANES ブロックから {path: 'true'|'false'} を抽出。無ければ None。"""
    m = _SIGNED_LANES_RE.search(text)
    if not m:
        return None
    return dict(_LANE_ENTRY_RE.findall(m.group(1)))


def find_secret_lines(text: str) -> list[int]:
    """secret 様（長 hex）を含む行番号（1-indexed）。"""
    return [i for i, line in enumerate(text.splitlines(), 1)
            if _SECRET_RE.search(line)]


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
    """検査本体。戻り値=終了コード（0 一致／1 drift／2 snapshot 無し）。"""
    snaps = sorted(p for p in Path(snapshot_dir).glob("*.js") if p.is_file())
    if not snaps:
        print(f"[drift-check] snapshot が {snapshot_dir} にありません"
              "（live GAS から .js を貼ってください）", file=out)
        return 2

    drift = False
    for snap in snaps:
        snap_text = snap.read_text(encoding="utf-8")

        # secret 様パターンの防御（該当行の内容は一切出さない）
        secret_lines = find_secret_lines(snap_text)
        if secret_lines:
            print(f"[warn] {snap.name}: secret 様の長 hex を検出"
                  f"（行 {secret_lines}）。該当行の内容は表示しません。"
                  "誤貼付でないか確認し、secret は snapshot に含めないでください。",
                  file=out)

        repo_file = _find_repo_file(repo_root, snap.name)
        if repo_file is None:
            print(f"[drift] {snap.name}: repo 側（gas/・legacy/gas/）に対応ファイルなし",
                  file=out)
            drift = True
            continue

        repo_text = repo_file.read_text(encoding="utf-8")
        mismatched = _diff_line_numbers(repo_text, snap_text)
        if mismatched:
            drift = True
            print(f"[drift] {snap.name}: 不一致 {len(mismatched)} 行 "
                  f"(repo={repo_file.as_posix()}) 行番号: {mismatched[:50]}", file=out)
            if show_content:
                secret_set = set(secret_lines)
                a, b = repo_text.splitlines(), snap_text.splitlines()
                for i in mismatched[:50]:
                    if i in secret_set:
                        print(f"  L{i}: (secret 様のため非表示)", file=out)
                        continue
                    ra = a[i - 1] if i - 1 < len(a) else "(repo: 行なし)"
                    rb = b[i - 1] if i - 1 < len(b) else "(live: 行なし)"
                    print(f"  L{i}: repo: {ra}", file=out)
                    print(f"  L{i}: live: {rb}", file=out)
        else:
            print(f"[ok]    {snap.name}: repo と一致 (repo={repo_file.as_posix()})",
                  file=out)

        # SIGNED_LANES 行列の対比（rv04c_signing.js のみ該当）
        snap_m = extract_signed_lanes(snap_text)
        if snap_m is not None:
            repo_m = extract_signed_lanes(repo_text) or {}
            print("[matrix] SIGNED_LANES（期待=repo・INC-0720 §7(ii)）:", file=out)
            matrix_ok = True
            for lane in _LANES:
                rv, sv = repo_m.get(lane, "-"), snap_m.get(lane, "-")
                mark = "OK" if rv == sv else "**MISMATCH**"
                if rv != sv:
                    matrix_ok = False
                print(f"  {lane}: repo(期待)={rv} live={sv} {mark}", file=out)
            if not matrix_ok:
                drift = True
                print("[drift] SIGNED_LANES 行列が期待（repo）と不一致", file=out)

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
