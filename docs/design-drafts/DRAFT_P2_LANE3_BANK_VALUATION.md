# DRAFT: P2 lane3 開通準備調査 — bank／valuation（新規開通・dual 併存不要案）

- TASK_ID: P2-CHAIN-005（READ_ONLY 調査＋DRAFT 起票・PC-A）／記録日 2026-07-20
- 調査 BASE: origin/main `231f71d`（読取のみ・コード/テスト/env/GAS/本番 非接触）
- 状態: **DRAFT**（開通の実施可否・順序・GAS watcher 新設は司令塔/[人]裁定）

## 1. サーバ側受入の現状（逐語）

両エンドポイントとも **koseki と同型**（file は探信 404 偽装のため意図的 optional・
他 3 フィールドは `Form(default=None)`＝全て任意）。

`bank_ingest.py:290-297`:
```python
@router.post("/bank/ingest")
async def bank_ingest(_auth: None = Depends(ingest_guard("BANK_INGEST_TOKEN")),
                      # file は意図的に optional: File(...) だと探信に 422 が
                      # 返り 404 偽装より先に存在が漏れる（koseki_ingest と同じ）
                      file: UploadFile | None = File(default=None),
                      case_hint: str | None = Form(default=None),
                      case_app_hint: str | None = Form(default=None),
                      drive_file_id: str | None = Form(default=None)):
```

`valuation_ingest.py:384-391`:
```python
@router.post("/valuation/ingest")
async def valuation_ingest(_auth: None = Depends(ingest_guard("VALUATION_INGEST_TOKEN")),
                           # file は意図的に optional: File(...) だと探信に 422 が
                           # 返り 404 偽装より先に存在が漏れる（koseki_ingest と同じ）
                           file: UploadFile | None = File(default=None),
                           case_hint: str | None = Form(default=None),
                           case_app_hint: str | None = Form(default=None),
                           drive_file_id: str | None = Form(default=None)):
```

**LANE_FIELDS との対応**（`gas/rv04c_signing.js`・現 main）:
`'/bank/ingest': ['file', 'case_hint', 'case_app_hint', 'drive_file_id']`／
`'/valuation/ingest': ['file', 'case_hint', 'case_app_hint', 'drive_file_id']`
＝**両 lane とも 4 キーでサーバ Form と 1:1**（koseki のような 2 キー縮小は未適用。
§6 OPEN-2: 送信契約を 2 キーへ縮小するかは裁定事項）。

## 2. 認証経路の現状

- **hub.service_auth 結線済み**（RV-04b dual-accept）: 両ファイルとも
  `from hub.service_auth import BodyCachingRoute, ingest_guard`（bank:42／valuation:31）・
  `router = APIRouter(route_class=BodyCachingRoute)`（bank:46／valuation:42）・
  `Depends(ingest_guard("BANK_INGEST_TOKEN" / "VALUATION_INGEST_TOKEN"))`。
  ＝**署名経路はサーバ側で開通済み**（registry env の allowed_paths 5 入口に両 path 含有・
  checklist 52-53 行）。
- **legacy token 経路あり**: env 名 `BANK_INGEST_TOKEN`／`VALUATION_INGEST_TOKEN`
  （定義参照箇所: bank_ingest.py 7,291 行・valuation_ingest.py 4,385 行。**値は非出力**）。
  `SERVICE_AUTH_LEGACY_DISABLED_PATHS` の現運用値は
  `/sortation/ingest,/koseki/ingest`（work-log 2026-07-20 §6.1）＝**bank/valuation は
  非該当で legacy 受理可能な状態**。ただし §3 のとおり legacy 送信元の GAS 現物が無い。

## 3. 「GAS 現物不在」の repo 側証跡（新規開通の裏取り）

`legacy/gas/コード.js` への grep 実出力:
```
$ grep -n "bank/ingest\|valuation/ingest\|registry/ingest" legacy/gas/コード.js
192:        RAILWAY_URL + '/registry/ingest?token=' + encodeURIComponent(REGISTRY_TOKEN), {
```
- `/bank/ingest`・`/valuation/ingest` の送信箇所は **repo 写し上では未検出**（ingest 系の
  legacy 送信は registry の 1 箇所のみ）。なお 5 行目の `'通帳'` フォルダは既存 `/scan` 行き
  （別経路・不変）。
- **P2L3P-H01: repo grep は live 不在の証明ではない**（repo/live drift は INC-0720 で実証済み）。
  ＝「新規開通・dual-accept 併存期間が不要」は**条件付き候補**であり、確定には
  **G-L3-0（§6 冒頭・[人] live 実見ゲート）の 3 点充足が必要**。
  **対照: registry（lane4 扱い）は repo 写しにも live 送信元があるため koseki 型の
  dual 併存→切替が必要**。混同注意。
- 補足: sortation の自動回送（`sortation_ingest.py:226-245`）は `ingest_valuation_pdf` 等の
  **in-process 直接呼出**（HTTP でない・認証非経由）。`_FORWARD_LINES`（119 行）は
  戸籍/登記事項証明/評価証明・課税明細の 3 種で **bank は回送対象外**。
  ＝ 現状、両 lane への **HTTP 呼出元はゼロ**。

## 4. 呼出予定元（設計文書上の位置づけ・引用)

- 正本 `DRAFT_RV04C_CALLER_MIGRATION.md` **rev D5**（commit `c32c45d`・**main 外**・
  S5 close log が SHA 参照で FROZEN 扱い）26 行:
  > GAS watcher系（1 スクリプト「相続書類自動化」）… 対象 fetch は ingest 系（/koseki・/sortation。**/registry・/bank・/valuation は結線済み入口として同じヘルパで開通**）
- `docs/runbooks/2026-07_S4-S5_cutover-checklist.md` 100-101 行:
  > `SIGNED_LANES.<lane>=true` を **1 lane ずつ**（順序: sortation → koseki → **registry/bank/valuation**）
- `docs/reports/2026-07_phase1-close-report_DRAFT.md` 74 行:
  > (iv) **残 lane 切替（koseki/registry/bank/valuation）は Phase 2**（材料: S4-5-PREP-LANE23）
- ＝ 設計上は**同一 GAS スクリプトの新設 watcher ブロック**が呼出元になる想定だが、
  対象フォルダ ID・トリガー設計は文書化されていない（§6 OPEN-1・[人]裁定）。

## 5. テスト資産の棚卸し

| 資産 | 内容 |
|---|---|
| `test_bank_ingest.py`（20 tests） | 読解・転記の機能系＋token 404 系 |
| `test_valuation_ingest.py`（17 tests） | 同上 |
| `test_valuation_reader.py`（14 tests） | 読解器単体 |
| `test_rv04b_dual_accept.py` | 5 入口 parametrize（署名受理・nonce replay 409）＋downgrade③が valuation 使用 |
| `test_rv04c_gas_builder.py` | LANE_FIELDS の GAS/表一致・pipeline vector は valuation lane で実行（P2K-H01 後） |
| `test_p2_koseki_signed_lane.py` | SIGNED_LANES 行列 pin（bank/valuation=false を固定中→開通時に要改定） |

**koseki 型 negative 群の横展開見積り**: 各 lane 約 10〜12
（negative 6・legacy/新規開通系 2〜3・byte パリティ 2・契約構造 1）＝ **2 lane 合計 20〜24**。
共通ヘルパ化（koseki 版の parametrize 化）で 15 前後まで圧縮可。別途**行列 pin テスト改定 1**。

## 6. 開通手順草案（koseki 手順との差分形式・dual 不要は G-L3-0 充足が条件）

### 実施前提ゲート G-L3-0（[人]・live 実見・fix1/P2L3P-H01 で新設）

- (a) **live GAS プロジェクト全文**で `/bank/ingest`・`/valuation/ingest` の endpoint 検索
  （エディタの検索機能）→ **ヒット 0 を実見**。
- (b) **トリガー一覧**（時計アイコン）で bank/valuation 系の実行主体が**ないことを実見**。
- (c) 可能な観測窓で **Railway HTTP Logs** の `/bank/ingest`・`/valuation/ingest` への
  **legacy 到達 0 を実見**。
- → **3 点充足で初めて「新規開通・dual 不要」が確定**。**未充足（いずれかで live caller の
  痕跡あり）なら registry 型（dual 併存→切替）へ計画を切替える**（下表の「dual-accept 併存」
  「D-5 相当」行は registry 型に読み替え）。

| 工程 | koseki（lane2・実績） | bank/valuation（lane3・草案） |
|---|---|---|
| repo: SIGNED_LANES | true 化＋行列 pin テスト | **同じ**（2 lane 分・pin 改定込み） |
| repo: 送信契約 | 2 キー縮小（P2K-H01） | **OPEN-2**: 4 キーのまま or 2 キー縮小（裁定） |
| repo: テスト | negative 横展開 12 件 | **同じ型**を 2 lane へ（§5 見積り） |
| dual-accept 併存 | 必要（live 送信元あり） | **不要（G-L3-0 充足が前提条件）**。充足時は**開通と同時に `SERVICE_AUTH_LEGACY_DISABLED_PATHS` へ両 path 追記可**（legacy を最初から閉じる）。未充足なら registry 型へ切替 |
| GAS 反映 | 既存ブロック②を dispatcher 置換 | **watcher ブロック新設**（OPEN-1: 要否・フォルダ ID・トリガー・[済] 規約は[人]裁定）。**全置換禁止・期待行列読み合わせ**（INC-0720 §7 規律） |
| D-3/D-4 相当 | 200 実測 2〜3 件・?token= 0 | **同じ**（新規開通のため legacy 0 は自明・署名 200 のみ確認） |
| D-5 相当 | 事後に PATHS 追記 | **開通時に前倒し済み**（上記）→ 事後工程なし |
| D-7 相当 | 能動 404 実測 | **同じ**（旧 token が存在しないため「未定義 env=deny-all 404」の確認に読み替え） |

## 7. リスクと停止条件の候補

- **リスク**: (i) 新設 watcher のフォルダ誤指定・アカウント取り違え（7/18-1 前例）
  (ii) registry（live 送信元あり・dual 必要）との手順混同 (iii) LANE_FIELDS を 2 キー化する場合の
  gas_builder ⊆ 契約・fixture vector 実行 lane（現在 valuation 使用）への波及
  (iv) legacy 即時閉鎖により障害切り分け経路が 1 本になる（一時 dual を選ぶ選択肢はある）
  (v) **P2L3P-H01: repo grep のみを根拠に dual 省略した場合、未知の live caller が存在すると
  当該経路が即 404**（INC-0720 と同型の巻き戻り事故）。**G-L3-0 の 3 点実見で事前に遮断**し、
  痕跡発見時は registry 型へ切替える。
- **停止条件候補**: 期待行列の読み合わせ不一致／D-3 相当で 401・404 連続／
  repo・live drift の検出／fixture 変更が必要になった場合。

## 8. 次票（実装票）のスコープ案

- 対象: `gas/rv04c_signing.js`（SIGNED_LANES 2 lane true 化＋OPEN-2 の契約反映）・
  `test_p2_koseki_signed_lane.py`（行列 pin 改定）・新規 `test_p2_lane3_signed.py`
  （negative 横展開・§5 見積り）・（OPEN-2 が 2 キー縮小の場合のみ）`test_rv04c_gas_builder.py`
  の承認箇所改定。
- 別票（[人]）: GAS watcher 新設・env（`SERVICE_AUTH_LEGACY_DISABLED_PATHS` 追記）・点火。
- OPEN-1: GAS 側新設 watcher の要否・設計（[人]裁定）／OPEN-2: 送信契約 4 キー vs 2 キー（裁定）。
