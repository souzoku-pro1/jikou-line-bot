# DRAFT: RV-04c caller 移行（GAS 署名付与＋kintone App 29 レーン＋旧方式停止・rotation）

- 発注: 司令塔 2026-07-15夜（Phase 1 最終）／起草: PC-A 2026-07-16（S1）
- 正本の上位: `DRAFT_RV04_HMAC_MIGRATION.md`（v2・NM01 v1 FROZEN）。**本 DRAFT はサーバ側
  contract（canonical・検証順・§6.1/6.2 reason 表）を一切変更しない**（FROZEN 非抵触が受入条件）。
- 前提（票で確定済み）: K1=kintone webhook 冪等キーは top-level `id`（公式仕様）／K2=kintone 側
  IP 制限不可・公開 CIDR `103.79.14.0/24` のみ素材／K3=GAS UrlFetchApp カスタムヘッダ送出 OK
  （httpbin 実測 200）／K4=LINE redelivery 両チャネル OFF（本票対象外）／RV-04b dual-accept
  実装済み（flag 未設定=OFF）／P1-114 registry fail-fast 4象限マージ済み／承認キュー token は
  露出済み扱い→rotation 必須。

## §0 スコープ

| 対象 | 内容 |
|---|---|
| GAS watcher系（1 スクリプト「相続書類自動化」） | multipart 手組み＋NM01 v1 署名付与。対象 fetch は ingest 系（/koseki・/sortation。/registry・/bank・/valuation は結線済み入口として同じヘルパで開通） |
| kintone **App 29（承認キュー）** webhook | token 継続（rotation 必須）＋防御束（`id` 冪等・XFF 補助検証・既存 refetch/再照合） |
| 旧 query 方式の停止 | ingest 5 入口の query token 受理停止（入口ごと段階停止・§6） |

**非対象**: 顧客Bot `/webhook`・`/webhook/dispatch-bot`（LINE 署名は既存のまま）・Stripe（不変）・
`/scan`（base64 JSON 経路・別票）・kintone App 30 `/hub/dispatch`・App 26 `/document/{secret}`
（webhook 代替は用途別選択の OPEN＝RV-04 §3・**本票では触れない**。App 29 レーンの防御束が
そのまま横展開のテンプレになる）。

## §1 GAS 側 HMAC 実装設計（NM01 v1 の GAS 再現）【票論点1】

### 1.1 byte 列の扱い（規約・本 DRAFT で凍結する実装規約）

NM01 v1 FROZEN の「content 対象＝**送出最終バイト列全体の SHA-256**」を GAS で成立させるため、
以下を GAS 実装規約とする:

- **R1: multipart は手組み**（FROZEN ④の成立条件 (i)）。`payload: {file: blob}` の自動組立は
  **使用禁止**（自動採番 boundary を送出前に読めず hash 不能）。boundary はリクエスト毎に
  `'RV04C' + nonce`（nonce は §1.2 の 32 hex・boundary 衝突は本文 hash 基準のため安全性に
  影響しないが一意にする）。`contentType: 'multipart/form-data; boundary=' + BND` を明示 set し、
  `payload` には**組み立てた byte 配列そのもの**（`Utilities.newBlob(bytes)` ではなく byte[] を
  直接渡す。GAS の UrlFetchApp は byte[] payload を無変換で送出する）を渡す。
  ※実測確認事項（S2 受入）: byte[] 直渡しで Content-Length が bytes.length に一致すること。
- **R2: 文字列→byte は UTF-8 に統一**。変換は
  `Utilities.newBlob(str).getBytes()`（GAS Blob の既定 charset=UTF-8）**のみ**を使う。
  `str.charCodeAt` ベースの手変換は禁止（サロゲート/非 ASCII で壊れる）。
- **R3: GAS の byte は signed（-128..127）**。hex 化・比較の前に必ず `(b + 256) % 256`
  （または `b & 0xFF`）で 0..255 へ正規化する共通関数 `toHex_(bytes)` を通す。
- **R4: multipart のパート構成は現行サーバ受理形と同一**:
  `--BND\r\n Content-Disposition: form-data; name="<n>"[; filename="<f>"]\r\n
  [Content-Type: <mime>\r\n] \r\n <value bytes> \r\n … --BND--\r\n`。
  テキストフィールド（drive_file_id 等）は UTF-8 bytes・ファイルは `blob.getBytes()` を
  そのまま連結（再エンコードしない）。連結は JS の配列 concat（`Array.prototype.push.apply`）。
- **R5: content_sha256 = `Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes)`**
  を R3 で hex 化。**digest 対象は R1 で payload に渡す同一の byte 配列**（コピーや再組み立てを
  挟まない＝「同一バイト列」保証を構造で担保）。

### 1.2 canonical / 署名の GAS 実装

- canonical は §2.1 FROZEN のとおり length-prefix。**len は UTF-8 バイト長**
  （`Utilities.newBlob(f).getBytes().length`。JS の `str.length` は不可＝日本語で不一致）。
  組み立ては byte 配列で行う: `ascii(len)` と `":"`・`"\n"` は ASCII bytes・field は R2 の
  UTF-8 bytes。ORDER = `["v1", key_id, caller_id, method_upper, normalized_path,
  timestamp_str, nonce_hex, content_sha256_hex]`（8 要素・サーバ `canonical_v1` と同一）。
- 署名 = `Utilities.computeHmacSha256Signature(canonicalBytes, keyBytes)` → `toHex_`。
  **byte[] 引数版を使う**（文字列引数版は charset 事故の温床のため禁止）。
  keyBytes は Script Properties の hex 文字列（§3）を `hexToBytes_` で復元。
- nonce = `Utilities.getUuid().replace(/-/g, '')`（32 hex・`[0-9a-fA-F]{32}` に適合。
  一意性目的であり秘匿性は不要＝UUID で十分）。timestamp = `Math.floor(Date.now()/1000)`。
- normalized_path = 入口ごとの固定文字列（`/koseki/ingest` 等・query を含めない。サーバは
  ASGI raw_path 基準・末尾 slash なしの固定値なら正規化差異は発生しない）。
- 送信ヘッダは §2.2 の 7 本。**query token は付けない**（署名 lane に切り替えた watcher からは
  `?token=` を外す＝ヘッダ在時はサーバが署名経路のみで判定するため併記は無意味かつ紛らわしい）。
- ヘルパは **1 関数に集約**（RV-04 §5 の裁定どおり）:
  `rv04cSignedFetch_(path, parts)` — 手組み multipart・署名・fetch までを一括で行い、
  watcher 側は `UrlFetchApp.fetch(...)` 呼出しをこの1行に置き換えるだけにする。
  **lane 単位の切替定数 `SIGNED_LANES = {koseki: true, sortation: false, ...}`** を持ち、
  false の lane は現行コード（旧 token）をそのまま通す（＝GAS 側 rollback は定数 1 箇所）。

### 1.3 既知の GAS 制約と対処

- UrlFetchApp はヘッダ名を送出時にそのまま維持する（K3 実測で X-Sig-* の到達確認済み）。
- 6 分実行制限・URL Fetch 日次 quota は現行 watcher と同等（署名計算の追加コストは µs〜ms
  オーダーで無視できる）。
- GAS からは**送出後の実 wire bytes を検分できない**ため、「payload=byte[] 直渡しが無変換で
  ある」ことは S4 の[人]実機検分（サーバ側 content_sha256 一致＝reason=ok）で最終確認する
  （FROZEN ④ (ii) の要求どおり）。

## §2 golden 突合方法（S2 受入条件の実測手順）【票論点2】

- 正本 fixture: `docs/design-drafts/rv04_hmac_golden_vectors.v1.json`
  （**5 vectors**: ascii_filename_multipart／japanese_filename_multipart／empty_body／
  long_boundary／multi_field。各 vector = body_b64・content_sha256・canonical_b64・
  signature・key_id/caller/method/normalized_path/timestamp/nonce・secret_hex_test_only）。
- GAS 側テスト関数 `rv04c_goldenSelfTest()` を **K3-test プロジェクト（流用可・票指定）**に置く:
  1. fixture の 5 vectors を GAS 定数として転記（テスト専用 secret のみ・本番 secret 不使用）。
  2. 各 vector で `Utilities.base64Decode(body_b64)` → §1 の `toHex_(computeDigest(...))` が
     `content_sha256` に一致すること（= byte 処理・hash の検証）。
  3. §1.2 の canonical 組み立て → base64 化が `canonical_b64` に一致すること
     （= length-prefix・UTF-8 バイト長の検証。日本語 vector が効く）。
  4. HMAC hex が `signature` に一致すること（= 鍵復元・署名の検証）。
  5. `Logger.log` に vector 別 PASS/FAIL と総合判定を出す。
- **受入**: 5/5 PASS のログを大野がスクショ→PC-A が実出力を S2 work-log（.md）へ全文保存。
  1 本でも FAIL なら S2 を停止して報告（FROZEN 再実装の検証ゲート）。
- path 異常形拒否ベクトル（%2F・`..`・`//`・非 ASCII）は**サーバ側検証の契約**であり GAS は
  生成しないため、GAS 突合の対象外（既存 `test_service_auth.py` が担保・変更しない）。

## §3 registry 更新（GAS caller 用 key の発行・保存）【票論点3】

- **key_id 設計**: `gas-ingest-2026-07a`（発行年月＋連番。rotation で `-07b` 等の新 ID を発番・
  **ID 再利用禁止**＝RV-04 §2.5）。
- **caller / allowed_paths**: caller は **`gas-ingest` の 1 本**とし、allowed_paths に
  ingest 5 入口全部を列挙する。
  - 根拠: 実体が単一 GAS プロジェクト（単一実行主体・単一 Script Properties）であり、
    per-lane に鍵を分けても**同一プロジェクト内で全鍵が同居**するため侵害時の blast radius は
    変わらない。運用（発行・rotation・registry 管理）は 1/5 になる。
  - 代替案（不採用・記録）: lane 別 key_id（gas-koseki 等×5）。将来 watcher を別プロジェクトへ
    分離する場合に再検討（その時は新 key_id 発番で自然に分離できる）。
- **サーバ側保存**: 既存 env `SERVICE_HMAC_KEY_REGISTRY`（JSON）に entry 追加（大野・値非表示）。
  形式は §2.5 のとおり（secret=hex 64桁以上・status=active・not_before=投入時刻・
  expires_at=次回 rotation 予定＋余裕。P1-114 の起動時 4象限検証が構文/実効性を担保する）。
- **GAS 側保存**: **Script Properties を採用**（`PropertiesService.getScriptProperties()`・
  キー `RV04C_KEY_ID` / `RV04C_SECRET_HEX`）。
  - 採否根拠: コード非埋め込み（**平文コード埋め込み禁止**を充足）・clasp pull の取得対象外
    （repo 写しに混入しない）・編集権限は所有アカウント（t-ohno@…）に限定。
  - 併せて**既存の旧 token 定数（KOSEKI_TOKEN 等・現在コード内平文）も §7 の retirement で
    Script Properties へ移すのではなく削除する**（旧方式ごと廃止するため移設不要）。
- **secret 生成**: 大野が `python -c "import secrets; print(secrets.token_hex(32))"` を
  ローカル実行（PC-A は値を見ない・扱わない）。Script Properties と registry env に同値を投入。

## §4 kintone App 29 レーン設計（token 継続＋防御束）【票論点4】

App 29（承認キュー）webhook はヘッダ付与不可のため署名移行対象外。**案K1（URL secret 強化＋
rotation）を 5 点セットで成立**させる（RV-04 §3 の採用条件）:

| # | 防御 | 実装 |
|---|---|---|
| ① | イベント dedup | **新規実装（S3）**: webhook body top-level `id`（K1 確定・通知ごと一意）を `inbound_event` へ記録（provider="kintone"・external_event_id=`id`・dedup_key=`build_idempotency_key("kintone", id)`・payload_hash）。**UNIQUE 衝突＝重複配信→skip 応答（200・処理登録なし）**。flag `KINTONE_EVENT_DEDUP_ENABLED`（既定 OFF＝現行挙動 byte 同一・M-06 流儀の env 直読みゲート） |
| ② | 条件付き状態遷移 | **既存**: `/webhook/kintone/approval` は受信後に record を refetch し「承認済かつ送信済み=no」を再判定（main.py 実装済み・変更しない） |
| ③ | kintone 再照合 | **既存**: ②の refetch がそのまま該当（webhook body の値を信用せず最新を取り直す） |
| ④ | source restriction | **XFF 補助検証 middleware（観測モードで導入・§4.1）** |
| ⑤ | 詳細ログ抑止 | **既存**: redaction 規律（emit 契約・reason code のみ）を継続 |

- ①の DB 到達不能時: **dedup は fail-open（記録失敗を握って処理継続・警告ログ）**とする。
  根拠: kintone webhook は再送しないため 5xx=イベント喪失（承認送信が止まる実害）。一方
  dedup が抜けても②③の状態再判定が二重送信を止める（dedup は防御束の一層であり単独の砦では
  ない）。この裁定は work-log に明記する。
- **K1 実測（S3/S4）**: 同一レコード 2 回更新で `id` が相違することを、(a) テスト（fixture
  body 2 通で dedup が別扱い）＋(b) 本番ログ実測（S4 で大野が App 29 レコードを 2 回更新→
  inbound_event に別 `id` の 2 行）で確認し、K1 記録欄を実測で確定する。

### 4.1 XFF 補助検証の採否（正直な限界評価つき）

- **限界（明記）**: `X-Forwarded-For` は**多段 proxy でクライアントが先頭に任意値を注入できる**。
  信頼できるのは「Railway の edge が**最後に append した hop**（実 peer IP）」のみで、それが
  kintone のアウトバウンド帯 `103.79.14.0/24` に入るかを見る。ただし
  (a) Railway edge の XFF 付与仕様（append か上書きか・複数 hop の並び）は**実測で確定が必要**、
  (b) `103.79.14.0/24` は cybozu.com 全体の帯であり webhook 専用でない（帯変更は
  「サイボウズからのお知らせ」通知のみ＝**enforce だと帯変更で全断リスク**）、
  (c) IP は「kintone のどこかから来た」以上を証明しない（テナント識別不可）。
  → **XFF 単独は防御にならない。①②③の後ろの補助層**という位置づけを固定する。
- **採用形（提案）**: kintone 入口（`/webhook/kintone/approval` のみ）限定の**観測モード**で導入
  —— rightmost hop を CIDR 照合し、**不一致は警告ログ（emit 契約・IP は external_ref 扱い）
  のみで reject しない**。env `KINTONE_XFF_OBSERVE_ENABLED`（既定 OFF）。
  **enforce（reject）への昇格は観測実績を見て別裁定**（S4 で Railway の実 XFF 形状を採取して
  から判断。enforce 時も帯変更に備え env で CIDR を差し替え可能にする）。
- 代替案（不採用・記録): 導入見送り（①②③＋rotation で十分とする）。観測モードはコスト極小で
  「Railway XFF 形状の実測」という将来資産が得られるため導入を提案する。

## §5 移行順序と各段の rollback【票論点5】

順序は RV-04 §5 を踏襲（sortation → koseki → 未稼働 lane → kintone レーンは並行トラック）。
**各段は独立に rollback 可能**で、rollback はすべて「env / GAS 定数を戻す」のみ（コード revert 不要）。

| 段 | 操作（[人]=大野） | 確認（PC-A=READ_ONLY 実測） | rollback |
|---|---|---|---|
| 0 準備 | [人] secret 生成→GAS Script Properties＋`SERVICE_HMAC_KEY_REGISTRY` env 投入（値非表示） | 起動ログで P1-114 起動時検証が通ること（flag OFF のうちは非参照＝挙動不変） | env/Properties を削除 |
| 1 点火 | [人] `SERVICE_AUTH_DUAL_ACCEPT_ENABLED=1`（深夜・問い合わせ少時間帯） | /health 200・起動成功（=registry 4象限 PASS）・**旧 token 経路が従来どおり 200**（Phase A 併存・全 lane 不変） | env を外す（即時・全 lane 旧挙動へ） |
| 2 GAS 署名切替 | [人] ヘルパ＋`SIGNED_LANES.sortation=true` をデプロイ（1 lane ずつ・sortation→koseki→…） | decision ログ `reason=ok`・対象 lane の 200 継続・golden 済み前提 | GAS 定数 `SIGNED_LANES.<lane>=false` に戻す（1 箇所・即時） |
| 3 並行観測 | [人] なし（数日） | 全 lane reason=ok 継続・401/403/409 ゼロ・旧 token 到達が署名済み lane で 0 になること | — |
| 4 旧 query 停止 | [人] `SERVICE_AUTH_LEGACY_DISABLED_PATHS` に lane を追加（段階・§6） | 停止 lane への token アクセスが 404・署名経路は 200 のまま | env から当該 path を外す |
| 5 rotation | [人] **承認キュー token rotation（露出済み・必須）**: 新 `KINTONE_WEBHOOK_TOKEN` 投入→kintone Webhook URL 更新。ingest 旧 `*_INGEST_TOKEN` を env から削除・GAS 内の旧 token 定数を削除 | App 29 webhook が新 token で 200・旧 token で 404。K1 実測（§4） | 旧 token を一時再投入（App29 のみ・露出済みのため短期限定） |
| 6 retirement | [人] なし | **旧 query token での認証成功 0 件**をログ期間実測→ retirement evidence を .md 固定（§7） | — |

- 段 1→2 の間、および段 2 の lane 間は任意に停止・滞留できる（Phase A は無期限併存可能）。
- kintone レーン（①dedup flag ON・④XFF 観測 ON）は段 4 と独立に点火できる（別 env）。

## §6 旧方式停止の実装形【票論点6】

- **提案: 入口ごとの段階停止（env list 方式）**。
  `SERVICE_AUTH_LEGACY_DISABLED_PATHS="/sortation/ingest,/koseki/ingest,…"`（カンマ区切り・
  既定 未設定＝**どこも停止しない（現行不変）**）。`authorize_ingest` の旧 token 分岐の手前で
  「当該 path が停止 list に含まれる場合、query token を検証せず 404」（存在しないフリの
  既存流儀を維持。署名ヘッダ在なら従来どおり署名経路のみ）。
  - 根拠: §5 の caller 別切替順序と 1:1 に対応し、事故時の blast radius が lane 単位で最小。
    「一括 flag」は全 lane 同時停止しかできず、切替順序の思想と合わない。全停止は全列挙で表現できる。
  - 実装は薄い（env parse＋set 照合）。テスト: 停止 lane=404（token 有効でも）／未停止 lane=
    従来どおり／署名経路は停止 list の影響を受けない／flag OFF（dual-accept 自体 OFF）時は
    list を参照しない（現行 byte 同一）——の 4 点を固定。
- 恒久形: 全 lane 停止が一定期間安定したら、Phase C として旧 token コード自体の削除を別票で
  実施（本票では削除しない＝rollback 経路を残す）。

## §7 retirement evidence（S5・受入条件の実測方法）

- 証跡1: decision ログの期間集計で「旧 query token 経路の認証成功（＝token 経路 200）」が
  観測窓（提案: 停止後 7 日）で **0 件**であることを `railway logs` 実測（UTC→JST 正規化）。
- 証跡2: 旧 credential の失効状態一覧（`*_INGEST_TOKEN` env 削除済み・GAS 旧定数削除済み・
  承認キュー旧 token が kintone 側 URL からも消えたこと［人］確認）。
- 証跡3: 露出済み承認キュー旧 token での手動アクセスが 404 になる実測 1 回（大野 or PC-A・
  値は表示しない）。
- 以上を S5 work-log（.md）へ実出力つきで固定。

## §8 テスト戦略（S2/S3 の受入・全体受入条件との対応）

| 受入条件（票） | 担保 |
|---|---|
| golden 5 本の GAS 実装突合 PASS | §2（GAS selfTest 5/5 PASS の実出力を .md 保存） |
| FROZEN 範囲非抵触（サーバ contract 変更ゼロ） | canonical/verify_* は非接触。追加はすべて「authorize_ingest の legacy 停止分岐」「kintone dedup（新 flag 配下）」「XFF 観測（新 flag 配下）」＝既存テスト（golden・§6.1 reason 表・rv04b dual-accept）が全 GREEN のままであることで機械担保 |
| 全 suite 既知 1 FAIL 以外 GREEN／台帳 61 不変 | 各 S の worktree 実測・新規 sink なし（既存 decision sink 再利用・XFF 警告ログは emit 契約で追加が必要なら台帳変更を司令塔へ事前相談） |
| 全段 rollback 文書化 | §5 の表（env/GAS 定数のみで戻る） |
| retirement evidence | §7 |

- S3 の修正前 FAIL 実測（規律）: kintone dedup は「同一 `id` の 2 回配信で処理が 2 回走る」を
  旧コードで実測（FAIL する形）→ 実装後 skip を確認。legacy 停止は「停止 list 記載 lane に
  旧 token で 200 が返る」を旧コードで実測→実装後 404。

## §9 OPEN（S1 時点・Codex レビューへの論点提示）

1. §3 の「caller 1 本・allowed_paths 5 入口」vs lane 別 key——blast radius の評価が分かれ得る。
2. §4 dedup の fail-open 裁定（可用性優先）——fail-closed（5xx）にすべきか。kintone 非再送の
   制約下では 5xx=喪失のため fail-open を提案するが、承認フローの重要度評価は司令塔判断。
3. §4.1 XFF 観測モードの採否そのもの（見送り代替あり）。
4. §5 段 3 の観測期間の長さ（提案: lane あたり 2〜3 日・全体 1 週間）。
5. 停止 list（§6）の env 名・404 という応答選択（403 でなく既存の「存在しないフリ」踏襲）の確認。
