# Runbook: 依存 lockfile と SBOM（RV-12）

- 正本ファイル: `requirements.lock`（全推移依存の固定）・`sbom/sbom.cdx.json`（CycloneDX 1.6）
- 宣言（人が編集する入力）: `requirements.txt`。**lock と SBOM は手編集しない**（常に再生成）。
- 再生成ツール: `requirements-dev.txt` の `uv` / `cyclonedx-bom`（バージョン固定）。

## 1. 再生成（1 コマンド）

```
pip install -r requirements-dev.txt   # 初回のみ（uv / cyclonedx-bom）
python scripts/regen_dependency_lock.py
```

- 出力1: `requirements.lock` — `uv pip compile requirements.txt --universal --python-version 3.12`。
  **universal 解決**のため環境マーカー付きで platform 非依存（Windows 開発機と Railway Linux で
  同一 lock。例: `uvloop … sys_platform != 'win32'` / `colorama … sys_platform == 'win32'`）。
- 出力2: `sbom/sbom.cdx.json` — lock 全 pin の CycloneDX 1.6 SBOM（`cyclonedx-py requirements`）。
- このPC固有: TLS 失効チェック問題のためスクリプトは既定 `--native-tls`（OS 証明書ストア）。
  他環境で不要なら `UV_NATIVE_TLS=0` で無効化。

## 2. 更新フロー（依存を足す/上げるとき）

1. `requirements.txt` を編集（宣言のみ・可能なら `==` を書かず宣言は緩く保つ。固定は lock の役割）
2. `python scripts/regen_dependency_lock.py` で lock＋SBOM を再生成
3. `requirements.lock` の diff をレビュー（意図しない major bump が混ざっていないか）
4. lock からの再現ビルド確認（§3）→ PR（lock・SBOM・requirements* を同一 commit で）

## 3. lock からの再現ビルド確認（受入手順）

```
uv venv <tmpdir>/venv --python 3.12 --native-tls
uv pip install -r requirements.lock --python <tmpdir>/venv/Scripts/python.exe --native-tls
<tmpdir>/venv/Scripts/python.exe -c "import fastapi, uvicorn, httpx, anthropic, docx, requests, stripe, reportlab, fitz, sqlalchemy, alembic, psycopg, multipart"
```

（Linux/CI では `Scripts` → `bin`。import 一式が通れば本番依存の再現ビルド成立。
2026-07-15 に Windows 3.12 で実測済み＝work-log 参照。）

## 4. 位置づけ・既知の限界

- **Railway の本番ビルドは従来どおり `requirements.txt` を使う**（本タスクではビルド入力を
  変えない＝デプロイ挙動不変）。ビルドを `requirements.lock` へ切り替えるかは司令塔裁定の
  別票（マージ＝デプロイのため）。切替時は nixpacks/Railpack の install コマンドを
  `pip install -r requirements.lock` にする。
- lock は「再生成した時点の PyPI」で解決される（再生成のたびに新しいバージョンへ動き得る）。
  固定したいのは **lock ファイルそのもの**であり、意図しない更新は §2-3 の diff レビューで止める。
- hash pinning（`--generate-hashes`）は未採用（v1 は版固定のみ）。採用する場合は
  `scripts/regen_dependency_lock.py` の compile 引数に追加する（全推移依存に hash が付く）。
- SBOM は lock と同時再生成が原則（乖離させない）。監査時は `sbom/sbom.cdx.json` の
  `components[].purl` を参照。
