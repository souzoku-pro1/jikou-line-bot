# DRAFT: Phase 4（PWA・Release A）準備調査 — 既存 Web 資産の棚卸しと実装票分解

- TASK_ID: P2-BATCH-06 / TASK 15（READ_ONLY 調査＋DRAFT 起票・PC-A）／記録日 2026-07-27
- 調査 BASE: origin/main `d87b3d6`（読取のみ）
- **前提の制約**: Phase 4 要件の正本（v2.4）は repo 外。repo 内に PWA/Release A の
  記述は存在しない（grep 0 件）。本書の要件分解は票の指定（画面候補5種・read-only）
  に基づく案であり、**v2.4 逐語との突合は司令塔提供／[人]確認が必要**。

## 1. 既存 Web 資産の棚卸し（実出力）

### 1.1 FastAPI route 一覧（main.py＋include_router 全 16 endpoint）

| endpoint | method | 認証方式（実物） | 分類 |
|---|---|---|---|
| `/health` | GET | なし（固定応答） | 公開 |
| `/health/deps` | GET | なし（直近 probe 結果の参照のみ・外部呼出しゼロ） | 公開（内部情報の要素あり） |
| `/webhook` | POST | LINE `X-Line-Signature`（HMAC-SHA256） | 署名 |
| `/webhook/kintone/approval` | POST | query `?token=`（`KINTONE_WEBHOOK_TOKEN`・NEXT dual） | token |
| `/webhook/stripe` | POST | `stripe-signature`（STRIPE_WEBHOOK_SECRET） | 署名 |
| `/webhook/dispatch-bot` | POST | LINE 署名（業務チャネル） | 署名 |
| `/cloudsign/webhook/{secret}` | POST | URL path secret | secret |
| `/document/{secret}` | POST | URL path secret | secret |
| `/hub/dispatch` | POST | query token（kintone webhook） | token |
| `/koseki/ingest`・`/registry/ingest`・`/bank/ingest`・`/sortation/ingest`・`/valuation/ingest` | POST | `ingest_guard`（RV-04b dual-accept: NM01 HMAC 署名 or legacy token） | HMAC |
| `/scan` | POST | **認証パラメータなし**（本文形式のみ・legacy GAS lane） | ⚠️要確認 |
| `/ocr/fixed-asset` | POST | **認証パラメータなし**（multipart upload） | ⚠️要確認 |

- **管理系（ブラウザ向け）エンドポイントは存在しない**。全 route が webhook/機械連携。
- ⚠️ `/scan`・`/ocr/fixed-asset` の無認証は P4 と独立の既知論点として記録
  （P2 koseki cutover で signed lane へ移行済みの区間があるため、残 lane の扱いは
  別票・[人]裁定。本票のスコープ外）。

### 1.2 静的配信・テンプレート

- `app.mount`／`StaticFiles`／HTML テンプレートの使用は**ゼロ**（grep 0 件）。
  PWA の静的配信（shell/manifest/Service Worker）は**新設**になる。
- `assets/fonts` は kinship_renderer（関係図画像）用フォントのみ。
- 参考: `kinship_renderer.py` が関係図の**画像レンダリング資産**として既存
  （PWA 関係図画面の再利用候補・§2）。

## 2. Release A（read-only PWA）の要件分解 — 画面候補×データ源

| 画面候補 | データ源 | kintone/DB アクセス形 | scan 20件（※）への依存 |
|---|---|---|---|
| 案件一覧 | App21（案件） | **複数レコード検索**（status 別・更新順） | 不要（先行可） |
| 案件詳細 | App21 単票＋App30（発送管理・案件絞込）＋App28（チャットログ・件数程度） | 単票＋絞込検索 | 不要（先行可） |
| 相続人関係図 | App34（人物）＋App33（戸籍）＋（P3 merge 後）derivation_run／heir_confirmation_decision の projection | 検索＋DB 読取。描画は kinship_graph→kinship_renderer 流用（サーバ側で画像生成→PWA は img 表示が最小構成） | 不要（先行可） |
| 書類到着状況 | **ingestion_receipt**（file ingest の可視化台帳・last_outcome が state 正本）＋inbound_event（webhook 系） | **DB 読取のみ**（kintone 不要） | **要**（書類ビュー・仕分け結果確認の UX が実物書類の見え方に依存） |
| 承認キュー参照 | App29（承認キュー） | 複数レコード検索（未送信/承認待ちの絞込) | 不要（先行可） |

- （※）**「scan 20件」の定義（司令塔裁定・2026-07-27 差し替え）**:
  **大野提供の相続書類サンプルスキャン 20 件**（戸籍・登記・通帳・評価証明等）＝
  **UX 設計素材**（v2.4 P8）。実物書類の見え方（レイアウト・画質・ページ構成）に
  UX が依存する画面—**書類ビュー・仕分け結果確認**—は、サンプル到着を設計着手の
  ゲートとする。一覧・関係図・承認キュー参照はサンプル不要で先行できる。
  ※旧解釈（「直近スキャン受領 20 件の一覧参照」）は**撤回**。
- read-only 徹底: Release A は**書込み API を 1 本も持たない**（承認操作・状態変更は
  従来どおり kintone/LINE。PWA は参照専用＝App30 絶対制約と非干渉）。

## 3. 認証設計の選択肢（大野単独利用前提）

| 案 | 概要 | 強度 | 運用負担 | 備考 |
|---|---|---|---|---|
| (a) 固定 Bearer token | env の長寿命 token を PWA が保存し全 API に付与 | 中 | 最小 | 漏洩時は全面露出・失効=env 変更（deploy 要） |
| (b) パスワード login＋期限付き session cookie | `/app/login` で照合→HttpOnly cookie（署名付き・期限 7-30 日） | 中〜高 | 小 | **推奨**。失効・再login が env 変更なしで可能。CSRF は read-only＋SameSite=Strict で最小化 |
| (c) NM01 HMAC（service_auth 流用） | 既存 registry で署名検証 | 高 | 大 | **不採用推奨**: ブラウザ JS に鍵を置く＝露出と同義。NM01 は server-to-server（GAS）用であり用途が異なる |
| (d) Basic 認証 | リバプロ不要の最小実装 | 低〜中 | 最小 | iOS PWA（standalone）でダイアログ UX が悪い・失効概念なし |

- **推奨 = (b)**。理由: 閲覧専用でも表示内容は顧客 PII そのものであり、失効可能な
  session が必要十分。大野単独前提なのでアカウント管理は不要（パスワード 1 本＋
  ハッシュを env）。
- **既存 ingest_guard/HMAC 基盤との関係**: 流用しない（上表 (c)）。ただし
  「秘密の env 保持・rotation 運用（NEXT dual token の型）」の運用規律は踏襲する。
- **[人]裁定事項**: ①認証方式の採否（推奨 (b)） ②session 期限 ③PWA を公開 URL の
  どのパス（`/app` 等）に置くか ④`/health/deps` の内部情報露出の扱い（P4 と同時に
  認証下へ移すか）。

## 4. PWA 技術構成案（iPhone 前提）

- **配信**: FastAPI に `StaticFiles`（`/app` mount）を新設し、単一 HTML＋JS
  （ビルドツールなし・vanilla or 最小ライブラリ）＋`manifest.json`＋`sw.js` を配信。
  Node/ビルド環境を要求しない（このPCに npm が無い制約とも整合）。
- **API**: `/app/api/*` に read-only JSON endpoint 群（§2 のデータ源を集約・
  kintone へのアクセスはサーバ側 proxy＝API token をブラウザに出さない）。
- **Service Worker / オフライン方針**: app shell（HTML/JS/CSS/フォント）のみ
  precache。**データは network-first・キャッシュ不可**（顧客 PII を端末キャッシュに
  残さない方針を既定とする。オフライン時は「オフラインです」表示のみ）。
  ※「PII を SW キャッシュに置かない」は RV-10 の趣旨からの提案であり[人]裁定事項。
- **iPhone（iOS Safari）の制約**: ①ホーム画面追加で standalone 化（Release A は
  これで十分） ②Web Push は iOS 16.4+ で可能だが Release A（read-only）では不使用
  （通知は既存 LINE のまま） ③SW キャッシュは OS により追い出されうる→shell 再取得で
  復帰する設計（データ非依存なので影響軽微） ④cookie session は standalone でも保持
  される（案 (b) と両立）。

## 5. 実装票スコープ案（P4-001〜）

| 票案 | スコープ | scan 20件（サンプル） | 依存 |
|---|---|---|---|
| P4-001: 認証＋PWA shell | login（案(b)）・session・`/app` 静的配信・manifest/SW（shell only）・空ダッシュボード | 不要 | なし（P3 非依存・先行着手可） |
| P4-002: 案件一覧＋案件詳細 | App21/App30/App28 の read-only proxy API＋2 画面 | 不要 | P4-001 |
| P4-003: 書類到着状況（書類ビュー・仕分け結果確認を含む） | ingestion_receipt/inbound_event の受領状況 API＋画面。DB 読取のみ。**書類ビュー・仕分け結果確認の UX 設計はサンプルスキャンを素材に行う** | **要（設計着手ゲート）** | P4-001＋**scan 20件到着（[人]・大野提供）** |
| P4-004: 承認キュー参照 | App29 絞込の read-only API＋画面 | 不要 | P4-001 |
| P4-005: 相続人関係図 | kinship_renderer 流用のサーバ側画像生成 API＋画面。導出結果（App36/DerivationRun projection）の重畳表示は **P3 merge 後**に追加 | 不要 | P4-001＋（重畳表示のみ）p3-001/002/003 merge |
- 依存整理（司令塔裁定反映）: **実物書類の見え方に依存する画面（書類ビュー・
  仕分け結果確認＝P4-003）のみ scan 20件到着がゲート**。一覧・関係図・
  承認キュー参照（P4-002/004/005）は scan 不要で先行できる。P3 merge を待つのは
  P4-005 の導出結果重畳のみ。
- 順序案: P4-001 → 002/004 並行 → 005 →（サンプル到着後）003。
