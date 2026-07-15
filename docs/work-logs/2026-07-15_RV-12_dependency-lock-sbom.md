# 作業記録 2026-07-15: RV-12（dependency lock＋SBOM）

- 票: Phase 1 小粒バッチ PR-C（tooling 系）
- BASE: origin/main b726171 ／ BRANCH: feat/rv12-dependency-lock
- 本番ビルド入力（requirements.txt / Procfile / Railway 設定）は**不変**＝デプロイ挙動不変。

## 1. 整備物

| ファイル | 内容 |
|---|---|
| `requirements.lock` | 全推移依存 **46 パッケージを `==` 固定**。`uv pip compile requirements.txt --universal --python-version 3.12` で生成した **universal lock**（環境マーカー付き・platform 非依存。例: `uvloop==0.22.1 ; … sys_platform != 'win32'`〔Railway Linux 用〕・`colorama ; sys_platform == 'win32'`〔開発機用〕） |
| `sbom/sbom.cdx.json` | lock 全 pin の SBOM（**CycloneDX 1.6・components 46 件**・`cyclonedx-py requirements` で生成） |
| `scripts/regen_dependency_lock.py` | **再生成 1 コマンド**（`python scripts/regen_dependency_lock.py`）。compile→SBOM を直列実行。この PC の TLS 失効チェック問題対策で既定 `--native-tls`（`UV_NATIVE_TLS=0` で無効化可） |
| `requirements-dev.txt` | 再生成ツールを固定追加: `uv==0.11.28`・`cyclonedx-bom==7.3.0` |
| `docs/runbooks/dependency-lock-sbom.md` | 手順の正本（再生成・更新フロー・再現ビルド確認手順・既知の限界） |

## 2. 受入条件との対応（実測）

### 2.1 lock から再現ビルド可能（クリーン venv で実測）
```
$ uv venv <scratch>/rv12venv --python 3.12 --native-tls
$ uv pip install -r requirements.lock --python <scratch>/rv12venv/Scripts/python.exe --native-tls
 + watchfiles==1.2.0
 + websockets==16.1        （…全依存のインストール成功）
$ <venv>/python -c "import fastapi, uvicorn, httpx, anthropic, docx, requests, stripe,
                    reportlab, fitz, sqlalchemy, alembic, psycopg, multipart; ..."
fastapi 0.139.0
anthropic 0.116.0
sqlalchemy 2.0.51
alembic 1.18.5
pymupdf 1.28.0
stripe 15.3.0
IMPORT SMOKE: OK
```
requirements.txt の既存 pin（sqlalchemy 2.0.51／alembic 1.18.5／psycopg 3.3.4）は lock でも
同値を維持（宣言 pin が lock を拘束することを確認）。

### 2.2 SBOM 生成手順を docs に固定
`docs/runbooks/dependency-lock-sbom.md`（§1 再生成・§2 更新フロー・§3 再現ビルド確認・§4 限界）。

### 2.3 CI またはローカル 1 コマンドで再生成可能
`python scripts/regen_dependency_lock.py`（ローカル 1 コマンド・実行実測で本 lock/SBOM を生成）。
CI（GitHub Actions ubuntu）での定期再生成/乖離検査は任意の別票（universal lock のため
Linux でも同一 lock を消費可能・runbook §4）。

## 3. 判断メモ

- **universal 解決（uv）を採用**: pip-tools の platform 依存 compile では Windows 開発機で
  生成した lock に Linux 専用依存（uvloop 等)が入らず Railway で再現しない。uv の
  `--universal` は環境マーカー付きで単一 lock に全 platform 分を畳み込む。
- **Railway ビルドの lock 切替は本票のスコープ外**（マージ＝デプロイのため司令塔裁定の別票。
  切替コマンドは runbook §4 に記載済み）。
- **hash pinning は v1 未採用**（版固定のみ・追加方法を runbook §4 に記載）。
- 逸脱なし。テストコード・本番コードは非接触（新規ファイル＋requirements-dev.txt 追記のみ）。

## 4. 枠消化の日次一行
- 2026-07-15: RV-12（universal lock 46 pin・CycloneDX 1.6 SBOM・regen 1コマンド・runbook）。
  開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
