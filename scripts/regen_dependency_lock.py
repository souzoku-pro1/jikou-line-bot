"""RV-12: 依存 lockfile（requirements.lock）と SBOM（sbom/sbom.cdx.json）の再生成。

1 コマンド:  python scripts/regen_dependency_lock.py

- 入力は requirements.txt（宣言）・出力は requirements.lock（全推移固定・universal）と
  sbom/sbom.cdx.json（CycloneDX 1.6）。手順の正本は docs/runbooks/dependency-lock-sbom.md。
- 前提ツールは requirements-dev.txt（uv / cyclonedx-bom）。
- --universal: 環境マーカー付きの platform 非依存 lock（Windows 開発機と Railway Linux の
  双方で同一 lock を使える。例: uvloop は sys_platform マーカー付きで含まれる）。
- 社内 PC の TLS 失効チェック問題対策として既定で --native-tls（OS 証明書ストア）を使う。
  無効化は UV_NATIVE_TLS=0。
"""

import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_VERSION = "3.12"   # 本番想定の下限（変更時は runbook も更新）


def _run(args: list) -> None:
    subprocess.run(args, check=True, cwd=ROOT)


def main() -> int:
    native_tls = ["--native-tls"] if os.environ.get("UV_NATIVE_TLS", "1") == "1" else []
    print("[1/2] uv pip compile requirements.txt --universal -> requirements.lock")
    _run([sys.executable, "-m", "uv", "pip", "compile", "requirements.txt",
          "--universal", "--python-version", PYTHON_VERSION, *native_tls,
          "-o", "requirements.lock"])
    print("[2/2] cyclonedx-py requirements requirements.lock -> sbom/sbom.cdx.json")
    os.makedirs(os.path.join(ROOT, "sbom"), exist_ok=True)
    _run([sys.executable, "-m", "cyclonedx_py", "requirements", "requirements.lock",
          "-o", os.path.join("sbom", "sbom.cdx.json")])
    print("done: requirements.lock / sbom/sbom.cdx.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
