# DRAFT: RV-04c caller 移行（GAS 署名付与＋kintone App 29 レーン＋旧方式停止・rotation）

- 発注: 司令塔 2026-07-15夜（Phase 1 最終）／起草: PC-A 2026-07-16（S1）
- **rev D2（2026-07-16）**: R-RV-04C-D 所見 12 件を司令塔裁定どおり反映（H01〜H08・M01/M03/M04・
  I03・§9 裁定確定）。RCF-M13（kintone revision 原子 claim・Phase 2 候補）は司令塔台帳へ起票済み。
- **rev D3（2026-07-16）**: R-RV-04C-D2 残所見 6 件を反映（H05残=LINE送信後 crash 行＋sending
  marker・D2-H01=stale received 監視・H06残=工程4の3小工程化・D2-M01=NEXT 期限 env・
  D2-M02/M03=§8 実測系追補）。
- **rev D4（2026-07-17）**: R-RV-04C-D3 所見 4 件を反映（H05残=phase 別失敗遷移表〔迷えば
  sending・failed 上書き禁止〕・D3-H01=marker rowcount=1 を送信前提条件に契約化・
  D2-M02残=実測 2 系統・D3-M01=検知遅延 ≈25h の明記）。
- **rev D5（2026-07-17）**: R-RV-04C-D4 所見 1 件を反映（D4-M01=正常 no-op の done terminal 化
  〔固定 enum 理由コード〕・terminal 3 値の意味一覧・偽警報系の修正前 FAIL 実測追加）。
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
  `'RV04C' + nonce`（nonce は §1.2 の 32 hex）。
  **boundary の安全性根拠（M01 訂正）**: 本文と delimiter が衝突しないことの根拠は
  **128bit nonce 由来のランダム性**（既知・未知の本文に同一 37 文字列が現れる確率が実務上
  無視できる）である。**content hash は根拠にならない**——本文に delimiter 同形列が現れれば、
  hash が正しくても multipart の parse 自体が壊れる（hash は改変検知であって構文安全の
  担保ではない）。この検証として delimiter 類似列内包 vector を §2 に置く。
  `contentType: 'multipart/form-data; boundary=' + BND` を明示 set し、
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
  そのまま連結（再エンコードしない）。
  **連結は固定サイズ chunk append 方式（H02）**: 出力配列へソース配列を **8192 bytes 等の
  固定 chunk に区切った for ループ**で追記する共通関数 `appendBytes_(dst, src)` に一本化。
  **`Array.prototype.push.apply(dst, src)` は使用禁止**（apply は src 全要素を引数展開する
  ため、実運用サイズの PDF（数 MB）で JS エンジンの引数上限を超え RangeError で落ちる。
  小さい入力では通ってしまい大きい入力だけ壊れる時限式のため、禁止を規約に明記する）。
  **S2 受入条件**: 実運用上限相当サイズの PDF、および chunk 境界の前後（chunk±1 byte 等）の
  入力で GAS 実機テストを行い、生成 body の SHA-256 がローカル計算（Python）と一致すること。
- **R5: content_sha256 = `Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes)`**
  を R3 で hex 化。**digest 対象は R1 で payload に渡す同一の byte 配列**（コピーや再組み立てを
  挟まない＝「同一バイト列」保証を構造で担保）。

### 1.1b multipart 構文規則（H01・builder が受け付ける入力の凍結）

builder（§1.1 R4）が生成する構文要素は以下に**固定**する（自由文字列を wire に載せない）:

- **field 名 = 固定 allowlist のみ**。lane 別に helper 内の定数表で凍結する
  （例: koseki=`{file, drive_file_id}`・sortation=`{file, drive_file_id, drive_file_url}`。
  S2 実装時に各入口の Form 定義から全 lane 分を確定して定数表に固定）。allowlist 外の
  field 名が渡されたら**送出前に例外**（サイレント除去はしない）。
- **filename 規則（採る方式を 1 つに固定）**:
  1. CR（0x0D）・LF（0x0A）・NUL（0x00）を含む filename は**拒否**（送出前に例外・
     ヘッダインジェクションの根絶）。`"` は `\"` にエスケープせず**拒否**（単純化）。
  2. 非 ASCII（コードポイント > 127）を 1 文字でも含む場合は **ASCII fallback に固定置換**:
     `doc-<driveFileId>.<ext>`（`<ext>` は原名末尾の ASCII 英数字拡張子・取れなければ `bin`）。
     原名は `drive_file_id` から Drive 側で常に復元できるため情報は失わない。
  3. **RFC 5987 `filename*` は使わない**（単純化・server parser の実装差依存を作らない）。
- **S2 受入条件（server parser 通し vector）**: builder 生成 body を**実サーバの multipart
  parser（FastAPI/python-multipart・TestClient 経由）に通し**、field 値・ファイル内容 bytes・
  fallback 後 filename が期待どおり復元されることを vector で固定する（日本語原名→fallback の
  ケースを必ず含む・§2 の新 fixture に収録）。

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

- 正本 fixture（既存・**不変=FROZEN**）: `docs/design-drafts/rv04_hmac_golden_vectors.v1.json`
  （**5 vectors**: ascii_filename_multipart／japanese_filename_multipart／empty_body／
  long_boundary／multi_field。各 vector = body_b64・content_sha256・canonical_b64・
  signature・key_id/caller/method/normalized_path/timestamp/nonce・secret_hex_test_only）。
- 追加 fixture（**新設**・builder 検証用）: `rv04c_gas_builder_vectors.v1.json`——
  既存 5 本に **parts 分解**（boundary・field 名/値・filename・mime・content bytes の入力仕様）を
  付した写し＋追加 vector: **delimiter 類似列内包**（content に `--<boundary>` 前方一致列を
  含む・M01）・**非 ASCII 原名→ASCII fallback**（§1.1b の filename 規則検証・H01）。
  既存 v1 と新 fixture の整合は body_b64 一致で機械確認（v1 側は一切変更しない）。
- GAS 側テスト関数 `rv04c_goldenSelfTest()` を **K3-test プロジェクト（流用可・票指定）**に置く。
  **構造要件（H03）: self-test は本番 helper と同一の builder 関数**（§1.1 R1〜R5・§1.1b を
  実装した `buildMultipart_(boundary, parts)`）**を呼ぶ**（テスト用に別実装した builder の
  PASS は受入と認めない＝検証対象の同一性をコード構造で担保）:
  0. **第0段（H03）**: 各 vector の parts を**本番 builder** に入力し、生成 body が
     `body_b64` と **byte 完全一致**すること（base64 同値比較。ここが崩れると以降の hash/
     署名一致は「たまたま同じ入力を写した」だけの検証になるため最初に置く）。
  1. `Utilities.base64Decode(body_b64)` → §1 の `toHex_(computeDigest(...))` が
     `content_sha256` に一致すること（= byte 処理・hash の検証）。
  2. §1.2 の canonical 組み立て → base64 化が `canonical_b64` に一致すること
     （= length-prefix・UTF-8 バイト長の検証。日本語 vector が効く）。
  3. HMAC hex が `signature` に一致すること（= 鍵復元・署名の検証）。
  4. `Logger.log` に vector 別 PASS/FAIL と総合判定を出す。
- **受入**: 全 vector PASS（第0段含む）のログを大野がスクショ→PC-A が実出力を S2 work-log
  （.md）へ全文保存。1 本でも FAIL なら S2 を停止して報告（FROZEN 再実装の検証ゲート）。
  加えて §1.1 R4 の chunk 境界/実運用上限 PDF の GAS 実機テスト・§1.1b の server parser 通し
  vector（サーバ側 TestClient テスト）も S2 受入に含める。
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
  - **採用条件 5 点（M03・固定。1 つでも崩れたら Script Properties の前提を失う）**:
    1. **単独所有**: プロジェクト所有者は t-ohno@… の 1 アカウントのみ。
    2. **共同編集者ゼロ**: 編集共有を一切付けない（editor が居れば Properties を読める）。
    3. **editor 定期監査**: 共有設定（編集者一覧）を定期確認する運用項目に追加
       （提案: 月次・rotation 時は必須）。
    4. **secret/log 禁止テスト**: GAS コードに `RV04C_SECRET_HEX` を `Logger.log`/`console.*` へ
       渡す箇所がないことを、repo 写し（clasp pull 後）への静的検査（grep）で S2 受入に含める。
    5. **権限変更時 rotation**: 共有設定を変更した（editor を追加した等）場合は、その時点で
       §2.5 lifecycle により **即 rotation**（旧 key_id を retiring→revoked）。
  - **[人] S4 前チェックリスト（M03）**: dual-accept 点火の前に大野が
    **「共同編集者ゼロ」を GAS の共有ダイアログで実見**し、スクショを記録（S4 手順書に組込み）。
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

- ①の DB 到達不能時（**H04 裁定・fail-open 案は廃棄**）: **dedup 不能時（DB 障害等）は
  処理せず 5xx を返す（fail-closed）。喪失は観測で検知し・人手リカバリ**（App 29 レコードの
  再操作＝新 `id` の再配信）とする。
  - **比較裁定（明文化）**: fail-open の最悪様態＝**顧客への二重送信**（重複配信＋refetch の
    レース窓で②③をすり抜けた場合。顧客に見える実害で取り消せない）。fail-closed の最悪様態＝
    **検知可能な 1 件喪失**（5xx はログ・観測（§4.2）に残り、承認は kintone 上に留まるため
    人手で再送できる）。**後者が安全側**——「取り消せない顧客影響 > 検知して回復できる遅延」。
  - kintone record の **revision を使った原子 claim**（refetch レース窓自体の閉塞）は
    **Phase 2 候補として RCF-M13 起票済み（司令塔台帳が正本・本 DRAFT は参照のみ）**。
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
  →（D2 裁定確定）**observe-only で採用**。enforce 昇格は観測実績後の別裁定（§9-3）。

### 4.2 kintone 専用 state 遷移（H05・inbound_event 同居の整合設計）

**遷移図（provider="kintone"・同期処理・自動 replay なし）**:

```
（webhook 受信・dedup flag ON）
  INSERT inbound_event(state='received', attempts=1)   ← 挿入時 state = received
    ├─ UNIQUE(dedup_key) 衝突 → 重複配信 = skip 応答（200・処理登録なし・行は不変）
    └─ INSERT 成功 = claim 成立 → 同一 request 内で処理続行
         ②③の再照合が**正常に不成立**（業務的 no-op・D4-M01）
           → UPDATE state='done', processed_at=now(), 理由=固定分類コード
             （LINE write 0・下記「正常 no-op の terminal 遷移」）
         ②③の再判定 PASS・LINE 送信を行うと確定した時点
           → UPDATE state='sending' WHERE state='received'   ← 送信着手 marker（H05残・D3）
             ＝ LINE 送信「直前」に同一 DB 内で原子的に記録（done 遷移とは別の遷移）
             **marker 成功（rowcount=1）が LINE 送信の前提条件（D3-H01・下記契約）**
         LINE 送信 → App 29 送信済み=yes 更新（既存 handler の副作用順）
         処理成功（全副作用完了）
           → UPDATE state='done', processed_at=now()     ← terminal 化は全副作用の後
         処理失敗 → **phase 別失敗遷移表（D4・下記）に従う**（marker 後は failed へ
             上書きしない）。5xx は返さない（副作用途中の再配信は無いため応答は結果に
             影響しない・失敗は観測対象）
  DB 到達不能（INSERT 自体が不能）→ §4 H04 のとおり 5xx（fail-closed・行なし）
```

- **marker 契約（D3-H01）**: `state='sending'` への UPDATE の**成功（rowcount=1）を LINE 送信の
  前提条件**とする。UPDATE の失敗・例外・rowcount≠1（行が想定 state でない＝並行操作等）の
  いずれの場合も **LINE write 0 で終了**（fail-closed・行は現状のまま・滞留として §4.2b が
  検知）。根拠: marker が書けないまま送信すると「送信したのに判別材料が無い」領域が生まれ、
  sending marker の存在意義（crash 後の人手判別）が崩れる。送らなければ最悪でも
  「検知可能な未送信滞留」に収まる。

- **phase 別失敗遷移表（D4・H05残）**——**原則: 「送信済みでないと確実に言える場合のみ
  `failed`。迷えば `sending`」**:

  | 失敗 phase | 遷移 | 理由 |
  |---|---|---|
  | LINE 呼出し前に確定失敗（marker 前の検証エラー・②③再判定 NG の例外系 等） | `failed` **可**（last_error=分類コード） | LINE write が発生していないことが構造上確実（marker 前=未送信確定） |
  | marker UPDATE 自体の失敗・例外・rowcount≠1 | **遷移なし**（行は `received` のまま・LINE write 0 で終了） | D3-H01 契約。滞留として検知 |
  | **marker 成功〜LINE 呼出し以後の一切**（LINE API 例外・timeout・レスポンス不明・成功後の App 29 更新失敗 を含む） | **`sending` 維持**（**`failed` への上書き禁止**・last_error も書かない＝state を動かさない） | timeout/例外は「送信されなかった」を意味しない（サーバ側で送信完了済みの可能性が常にある）。**不明は不明のまま**人手 runbook（§4.2 の判別手順）へ渡す。failed に上書きすると「未送信確定」の意味を偽り、人手再操作の安全判定を壊す |

- **正常 no-op の terminal 遷移（D4-M01）**: INSERT 後の②③再照合が**正常に不成立**となる経路
  （承認済みでない・既に 送信済み=yes・record 不在 等の**業務的 no-op**）は、例外系ではないので
  `failed` にも滞留にもせず **`done` へ terminal 遷移**する（`processed_at=now()`・**LINE write
  0**）。理由は**固定分類コード**を `last_error` 列へ記録する（enum 固定・**自由文字列禁止**・
  例: `skip_not_approved`／`skip_already_sent`／`skip_record_not_found`／`skip_missing_fields`。
  列名は error だが provider=kintone では「terminal 理由コード」として流用＝**ALTER 0 維持**。
  S3 実装時に既存 handler の skip 分岐（`not_triggered`/`already_sent_or_not_approved` 等）と
  1:1 対応の enum を確定する）。received のまま放置すると §4.2b の 1h 監視が**正常動作を
  偽警報**として拾ってしまう——これを塞ぐのが本遷移の目的。

- **主要 state 3 値の意味一覧（D4-M01・整合の単一の正・D5-L01 表題訂正）**:

  | state | 意味 | LINE write | 人手対応 |
  |---|---|---|---|
  | `failed` | **未送信確定**の失敗（marker 前の**例外系**のみ） | 0（確定） | 安全に再操作可 |
  | `done` | **業務完了 または 正常 no-op**（`last_error` の理由コードで判別: NULL/空=送信完了・`skip_*`=no-op） | 完了時 1／no-op 時 0 | 不要 |
  | `sending` | **不明**（送信済みの可能性あり・非 terminal） | 不明 | §4.2 runbook で判別→最終判断は人 |

  （`received` は過渡状態: 未処理 claim 済み or marker 失敗終了。滞留すれば §4.2b が検知。）

- **sending marker（H05残・緩和策）**: `state='sending'` は**既存 state 列の新しい値**であり
  列追加なし＝**ALTER 0 維持**（state は Text・provider 別語彙は §5.1〔RV-05〕の流儀）。
  marker の目的は「LINE 送信に着手した事実」を副作用の**前**に永続化し、crash 後の人手判別を
  可能にすること（送信自体の原子化ではない——それは RCF-M13 の領分）。

- **claim 主体 = INSERT の UNIQUE 勝者**（LINE の「排他 claim UPDATE」に相当する原子性を、
  kintone では**初回 INSERT そのもの**が担う。再配送が存在しないため re-claim 経路は不要）。
- **terminal 化順序**: `done` は**全副作用（LINE push・kintone 更新）完了後**に書く。
  ⇒ crash 各点の挙動:
  | crash 点 | 行の状態 | 帰結 |
  |---|---|---|
  | INSERT 前 | 行なし | イベント喪失（kintone 非再送）。検知=業務側（承認したのに送信されない）＋日次健診。回復=人手（レコード再操作で新 `id`） |
  | INSERT 後〜sending marker 前 | `received` 滞留 | 送信は**確実に**起きていない。滞留として観測（§4.2b）→人手リカバリ（安全に再操作できる）。**再操作しても旧行は残る**（新 `id` で新行）＝旧行は人手 reset（failed へ）または滞留観測のまま終息 |
  | **sending marker 後〜LINE 送信完了前**（D3） | `sending` 滞留 | 送信は**未遂の可能性**。marker だけでは送信有無を確定できない＝下記 runbook の「送信済みの可能性あり」扱い（LINE 側の実受信確認まで再操作を保留） |
  | **LINE 送信成功後〜App 29 送信済み=yes 更新前**（H05残・独立行） | `sending` 滞留（実は LINE 送信済み・kintone 側は 送信済み=no のまま） | **裁定=二重通知許容**。この状態でレコードを人手再操作すると②③の再判定（送信済み=no）を**正しく通過**して再送され得る＝顧客に同内容が 2 回届く。**比較裁定: 検知可能な 2 回 > 沈黙の 0 回**（RV-05 fix4 §3.3 と同思想——2 回は顧客・事務所に見えて謝れる・0 回は静かに約束を破る）。緩和は sending marker による下記 runbook 判別 |
  | 全副作用後〜terminal 書込前 | `sending` 滞留（実は全て完了） | 二重送信は起きない（重複配信は dedup が skip・再操作時は②③の 送信済み=yes 再判定が止める）。滞留観測→人手が kintone 側 送信済み=yes を確認して行を手動 terminal 化 |

- **人手再操作 runbook（H05残・S3 で runbook 節として固定）**:
  1. 滞留行の `state` を確認——`received`=送信未着手（安全に再操作可。marker 失敗終了も
     ここに含まれる）／`failed`=**未送信確定**の失敗（phase 別遷移表により marker 前の確定
     失敗のみがこの値を取る＝安全に再操作可）／`sending`=**「送信済みの可能性あり」**
     （原則どおり、迷った失敗はすべてここに集まる）。
  2. `sending` の場合: App 29 の 送信済み フィールド・LINE の実受信（弁護士 or 管理画面で顧客
     トーク確認）・Railway ログ（[LINE] reply/push OK 行）を突き合わせて送信有無を判別する。
  3. **最終判断は人**——判別がつかない場合に再操作するか（二重通知リスクを取るか）
     放置するか（未達リスクを取るか）は、内容の性質（承認済み回答の重要度）で大野が判断する。
     機械は marker の提示まで（自動再送はしない）。
- **既存 Stripe/LINE の reclaim・backlog 監視からの分離条件**:
  1. **reclaim 非適用**: LINE の stale processing 再 claim（RV-05-13 fix4）は `dedup_key`
     prefix=`line:` の claim 経路内でのみ発動・Stripe の stale 再 claim も Stripe 経路内。
     kintone 行はどちらの claim 経路にも入らない（`kintone:` prefix・再配送も存在しない）
     ことを実装で確認し、テストで固定（S3）。
  2. **backlog 監視の分離**: `check_journal_backlog`（監視項目E）は現在 provider 無差別に
     processing/failed を数えるため、kintone 行が混入すると **Stripe runbook を指す誤警報**に
     なる。S3 で集計に provider 次元を追加し、kintone 分は**専用文言＋専用 runbook 参照**で
     別警報にする（既存 Stripe/LINE の閾値・文言は不変）。
- **RV-05/13 不変条件との整合証明**:
  - **epoch**: inbound_event に epoch 列は無い（epoch fencing は ingestion_receipt=sortation
    専用・DRAFT_RV05 §H-01）。kintone レーンも epoch を持たない＝**不変条件維持**
    （併走の可能性は「再配送が存在しない」ため LINE より狭く、②③が受容域を担保）。
  - **state 正本**: state の正本は inbound_event 行（LINE/Stripe と同型）。kintone は
    `received/sending/done/failed` の **4 値**のみ使用（D3 で `sending` 追加。
    `processing`・`failed_exhausted` は使わない＝同期処理で claim=INSERT のため LINE 型の
    中間 state が不要・attempts は 1 固定で加算経路なし。`sending` は送信着手 marker であり
    再 claim 対象にしない——kintone に再配送は存在しない）。
  - **migration 要否（確定）**: **不要（ALTER 0）**。使用列は provider／external_event_id／
    caller_id（=app id）／dedup_key／payload_hash／event_type（=body type）／state／
    received_at／processed_at／last_error／attempts——すべて既存列（`sending` は state 列の
    値追加のみ）。

### 4.2b kintone provider の滞留監視（D2-H01・既存 backlog 監視への統合）

- **対象**: provider="kintone" の `received`・`sending` 滞留（=crash 表の中間 3 行の検知装置）。
- **年齢閾値**: `KINTONE_STALE_EVENT_HOURS`（env・**既定 1 時間**）。根拠: kintone レーンは
  同期処理（秒オーダー）であり、1 時間残留は確実に crash/障害。Stripe の 24h（再送待ち前提）
  とは意味が違うため**閾値を分離**する。
  **注意（D3-M01・runbook にも明記）**: 1h は**抽出閾値**であって検知速度ではない。駆動は
  **日次健診（7:00 JST）のため最悪検知遅延 ≈25h**（7:00 直後に発生した滞留は翌朝の健診まで
  検知されない）。より速い検知が必要になったら健診とは別の tick を別票で裁定する。
- **件数・表示**: 件数＋PK 上位 10 件のみ（event id・record 内容は出さない＝D17 流儀踏襲）。
- **統合方法**: `check_journal_backlog`（daily_healthcheck 監視項目E）の集計へ **provider
  次元を追加**し、kintone 分は**専用文言**（`kintone滞留: received/sending が1時間超 N件
  (PK=[…]) — runbook: <§4.2 の人手再操作 runbook>`）で別警報にする。既存 Stripe/LINE の
  閾値（24h）・文言・runbook 参照は**不変**。ゲートは既存 `STRIPE_EVENT_JOURNAL_ENABLED` に
  依存させず `KINTONE_EVENT_DEDUP_ENABLED`（§4-①）で判定（flag OFF なら kintone 行が
  存在しないため検査もスキップ）。
- **実測テスト（S3）**: received/sending の閾値超え fixture で専用警報文言が出ること・
  閾値内は出ないこと・Stripe/LINE の既存警報に影響しないこと（§8 D2-M03 の provider 不変
  assert と対）。

## §5 移行順序と各段の rollback【票論点5】

順序は RV-04 §5 を踏襲（sortation → koseki → 未稼働 lane → kintone レーンは並行トラック）。
各段は独立に rollback 可能。ただし（**M04・「1 箇所戻すだけ」表現は撤回**）**段 4 以降の
rollback は複数箇所の状態が絡むため、§5.1 の順序付き複合手順に従う**（順序を誤ると
「legacy を再許可したのに credential が失効していて全断」等の窓が開く）。段 1〜3 の rollback は
従来どおり env / GAS 定数の単一操作で戻る。

| 段 | 操作（[人]=大野） | 確認（PC-A=READ_ONLY 実測） | rollback |
|---|---|---|---|
| 0 準備 | [人] secret 生成→GAS Script Properties＋`SERVICE_HMAC_KEY_REGISTRY` env 投入（値非表示） | 起動ログで P1-114 起動時検証が通ること（flag OFF のうちは非参照＝挙動不変） | env/Properties を削除 |
| 1 点火 | [人] `SERVICE_AUTH_DUAL_ACCEPT_ENABLED=1`（深夜・問い合わせ少時間帯） | /health 200・起動成功（=registry 4象限 PASS）・**旧 token 経路が従来どおり 200**（Phase A 併存・全 lane 不変） | env を外す（即時・全 lane 旧挙動へ） |
| 2 GAS 署名切替 | [人] ヘルパ＋`SIGNED_LANES.sortation=true` をデプロイ（1 lane ずつ・sortation→koseki→…） | decision ログ `reason=ok`・対象 lane の 200 継続・golden 済み前提 | GAS 定数 `SIGNED_LANES.<lane>=false` に戻す（1 箇所・即時） |
| 3 並行観測 | [人] なし（数日） | 全 lane reason=ok 継続・401/403/409 ゼロ・旧 token 到達が署名済み lane で 0 になること | — |
| 4 旧 query 停止 | [人] `SERVICE_AUTH_LEGACY_DISABLED_PATHS` に lane を追加（段階・§6） | 停止 lane への token アクセスが 404・署名経路は 200 のまま | **§5.1 の複合手順**（M04） |
| 5 rotation | **H06: 下記 5.2 の 4 工程に分解**（承認キュー token・露出済み・必須） | 各工程の確認列を 5.2 に記載 | 各工程の rollback 列を 5.2 に記載 |
| 6 retirement | [人] なし | §7 の再設計手順（lane 別収束観測・試行計数・窓） | — |

- 段 1→2 の間、および段 2 の lane 間は任意に停止・滞留できる（Phase A は無期限併存可能）。
- kintone レーン（①dedup flag ON・④XFF 観測 ON）は段 4 と独立に点火できる（別 env）。

### 5.1 段 4 以降の rollback（M04・順序付き複合手順）

段 4 で問題が出た（署名 lane が不調のまま legacy も止まっている等）場合、**必ずこの順序**:

1. **server: legacy 再許可** — `SERVICE_AUTH_LEGACY_DISABLED_PATHS` から当該 path を外す
   （まず受け口を開ける。これをしないと以降の手順が無意味）。
2. **旧 credential の有効性確認** — 当該 lane の `*_INGEST_TOKEN` が env に**残存しているか確認**
   （段 5 で削除済みなら再投入が必要。承認キュー旧 token は露出済みのため再投入は
   **短期・司令塔承認つき**に限定）。verify: token 経路の実送 1 回が 200。
3. **GAS: 旧 lane 戻し** — `SIGNED_LANES.<lane>=false` へ（最後に caller を旧経路へ戻す。
   1→2 より先にやると「token を送るのに server が 404」の全断窓が開く）。

### 5.2 段 5 rotation の 4 工程分解（H06・承認キュー token）

| 工程 | 操作（[人]=大野） | 確認 | rollback |
|---|---|---|---|
| 5-1 期限付き dual-accept | server が**新旧 2 token を期限付きで併存受理**する状態にする（S3 実装: `KINTONE_WEBHOOK_TOKEN`＋`KINTONE_WEBHOOK_TOKEN_NEXT` の 2 env 受理・期限は運用で管理）→ [人] `_NEXT` に新 token 投入 | 旧 token で 200 継続（無停止）・起動ログ正常 | `_NEXT` を外す（旧のみ受理へ即復帰） |
| 5-2 kintone URL 更新 | [人] kintone App 29 Webhook 設定の URL を新 token 付きへ更新 | — | URL を旧 token 付きへ戻す（5-1 併存中は無停止で戻せる） |
| 5-3 新 token 実着確認 | [人] App 29 でテスト遷移を 1 回発火 | PC-A: ログで**新 token での認証成功**を実測（旧 token 到達が 0 になったことも併せて確認） | （確認のみ・戻す対象なし） |
| 5-4 旧 token revoke | **H06残（D3）: 下記 3 小工程に分解**（Railway の env 変更は**都度再デプロイ**を伴う前提で、どの再デプロイ時点でも新 token が受理される**原子性非依存の順序**にする） | 各小工程に記載 | 各小工程に記載 |

**5-4 の 3 小工程（D3）**:

| 小工程 | 操作（[人]） | この時点の受理状態 | 確認 | rollback |
|---|---|---|---|---|
| 5-4a primary 差替え | `KINTONE_WEBHOOK_TOKEN` を**新 token 値**へ差替え（`_NEXT`=新のまま・再デプロイ発生） | primary=新・NEXT=新（同値 2 本＝再デプロイ跨ぎでも新 token は必ず受理・**旧 token はこの再デプロイ完了時点で死ぬ**） | 起動正常・webhook 200 継続 | primary を旧値へ戻す…は**不可**（露出済み）。不調なら 5-1 の形で更に次の新値へ前進 |
| 5-4b 新 token 成功ログ実測 | App 29 でテスト遷移 1 回発火 | 同上 | PC-A: **新 primary での認証成功**をログ実測（5-3 と別に、primary 差替え後の実測として独立に取る） | （確認のみ） |
| 5-4c NEXT 削除 | `KINTONE_WEBHOOK_TOKEN_NEXT`（＋`_NEXT_EXPIRES`）を削除（再デプロイ発生） | primary=新のみ（定常形） | 起動正常・webhook 200 継続・NEXT 残置検査（下記 D2-M01）が消えること | `_NEXT` に新値を再投入（5-4a 直後の形へ戻る・無害） |

- **NEXT の期限管理（D2-M01）**: `KINTONE_WEBHOOK_TOKEN_NEXT` を置く際は
  **`KINTONE_WEBHOOK_TOKEN_NEXT_EXPIRES`（日付 env・例 `2026-07-31`）を併設**する。
  - **期限超過時**: 起動ログに**固定文言の警告**（値・token は出さない）＋
    `daily_healthcheck` に **NEXT 残置検査を 1 項目追加**——`_NEXT` が設定済みかつ
    `_NEXT_EXPIRES` 超過（または `_EXPIRES` 未設定）なら**通知本文に notice として 1 行**
    載せる（**警報ではない**＝異常検知扱いにしない。dual-accept 併存の消し忘れを
    可視化するだけで可用性影響はないため）。
  - **期限の owner = 大野**（runbook に明記・S3 で runbook 節として固定）: 期限の設定・延長・
    5-4c での削除はすべて大野の操作。PC-A は notice の検知報告まで。

（ingest 側の旧 `*_INGEST_TOKEN` 削除・GAS 旧 token 定数削除は段 5 と同時でよいが、
**§5.1 rollback の手順 2 が参照するため、削除は当該 lane の段 4 安定確認後**とする。）

## §6 旧方式停止の実装形【票論点6】

- **提案: 入口ごとの段階停止（env list 方式）**。
  `SERVICE_AUTH_LEGACY_DISABLED_PATHS="/sortation/ingest,/koseki/ingest,…"`（カンマ区切り・
  既定 未設定＝**どこも停止しない（現行不変）**）。`authorize_ingest` の旧 token 分岐の手前で
  「当該 path が停止 list に含まれる場合、query token を検証せず 404」（存在しないフリの
  既存流儀を維持。署名ヘッダ在なら従来どおり署名経路のみ）。
  - 根拠: §5 の caller 別切替順序と 1:1 に対応し、事故時の blast radius が lane 単位で最小。
    「一括 flag」は全 lane 同時停止しかできず、切替順序の思想と合わない。全停止は全列挙で表現できる。
  - **起動時 strict 検証（H07・要件化）**: `SERVICE_AUTH_LEGACY_DISABLED_PATHS` は起動時に
    P1-114 と同じ fail-fast 境界で検証する——
    (a) **既知 5 path の厳格集合**（`/koseki/ingest`・`/registry/ingest`・`/bank/ingest`・
    `/sortation/ingest`・`/valuation/ingest`）に対する完全一致のみ受理。
    (b) **未知値・重複・末尾 slash・空要素・全角文字**はいずれも**固定文言の設定例外で起動停止**
    （P1-114 の固定文言方式に合流・値の実体は例外/ログに出さない）。
    (c) 実行時の照合は**実 routing raw path と同一の正規化**（＝正規化しない完全一致。署名経路の
    normalize_path とは別物であることを明記——停止 list は「設定として正しい形しか受け付けない」
    ため実行時の正規化余地を作らない）。
  - 実装は薄い（起動時 strict parse＋set 照合）。テスト: 停止 lane=404（token 有効でも）／
    未停止 lane=従来どおり／署名経路は停止 list の影響を受けない／flag OFF（dual-accept 自体
    OFF）時は list を参照しない（現行 byte 同一）／**strict 検証 5 異常形（未知値・重複・
    末尾 slash・空要素・全角）で起動停止**——を固定。
- 恒久形: 全 lane 停止が一定期間安定したら、Phase C として旧 token コード自体の削除を別票で
  実施（本票では削除しない＝rollback 経路を残す）。

## §7 retirement evidence（S5・H08+I03 再設計）

**設計原則**: 「観測ゼロ」は「窓が lane の実行 cadence より短ければ無意味」なので、
**窓を lane の実測 cadence から導出**し、かつ**成功と試行を別 reason で数える**（停止後の
「試行が来ているが遮断されている」と「そもそも来ていない」を区別する）。

1. **停止前（lane 別の legacy 成功 0 収束観測）**: decision ログを lane 別に集計し、
   「**署名経路成功が継続**しつつ **legacy 成功（token 経路 200）が 0 に収束**」を確認して
   から当該 lane を停止 list に入れる（＝停止は観測で裏づけられた lane からのみ）。
2. **停止後（legacy 試行の計数・reason 分離）**: 停止 lane への query token 試行は
   **専用 reason（例 `legacy_blocked`）で decision ログに計数**する（S3 実装・応答は 404 の
   まま・**成功 reason と別コード**にして「試行あり/なし」と「成功 0」を独立に観測可能にする。
   固定 reason のみ＝新規 sink なし・既存 decision sink 再利用）。
3. **観測窓（I03）**: lane ごとに
   **窓 ≥ 当該 lane の最大実行間隔（実測）× 2**、かつ
   **窓内に署名経路の成功が最低 N 回（提案 N=3・cadence の低い lane は司令塔と個別合意）**
   あること。GAS watcher はトリガー周期が lane で異なるため、一律日数（旧 7 日提案）は
   **廃止**し cadence 基準へ（§9-4 裁定）。
4. **証跡の組合せ（3 点セット）**:
   - **署名成功の実送**: 各 lane で署名経路 200 の実ログ（窓内 N 回）。
   - **能動 404 試験**: 旧 query token での手動アクセス 1 回が 404 になる実測
     （大野 or PC-A・値は表示しない・`legacy_blocked` reason がログに出ること）。
   - **credential 削除**: `*_INGEST_TOKEN` env 削除済み・GAS 旧 token 定数削除済み・
     承認キュー旧 token が kintone URL からも消えたこと（[人] 実見）＋§5.2 5-4 完了。
- 以上（lane 別収束グラフ相当の集計・試行計数・3 点セット）を S5 work-log（.md）へ
  実出力つきで固定。

## §8 テスト戦略（S2/S3 の受入・全体受入条件との対応）

| 受入条件（票） | 担保 |
|---|---|
| golden 突合 PASS（**第0段=builder byte 一致を含む**・H03） | §2（本番 builder 共用の selfTest 全 PASS 実出力を .md 保存。既存 5 本＋delimiter 類似列内包＋ASCII fallback の追加 vector） |
| GAS 実機の大サイズ/chunk 境界（H02） | §1.1 R4（実運用上限相当 PDF・chunk±境界の実機テスト・SHA-256 を Python と突合） |
| server parser 通し（H01） | §1.1b（builder 生成 body を TestClient で実 parser に通す vector・新 fixture） |
| FROZEN 範囲非抵触（サーバ contract 変更ゼロ） | canonical/verify_* は非接触。追加はすべて「authorize_ingest の legacy 停止分岐（H07 strict 検証つき）」「kintone dedup＋state 遷移（新 flag 配下・§4.2）」「XFF 観測（新 flag 配下）」「KINTONE_WEBHOOK_TOKEN_NEXT 併存受理（§5.2 5-1）」「legacy_blocked reason 計数（§7-2）」＝既存テスト（golden・§6.1 reason 表・rv04b dual-accept）が全 GREEN のままであることで機械担保 |
| 全 suite 既知 1 FAIL 以外 GREEN／台帳 61 不変 | 各 S の worktree 実測・新規 sink なし（既存 decision sink 再利用・XFF 警告/legacy_blocked は固定 reason コード。台帳変更が必要になった場合は司令塔へ事前相談） |
| 全段 rollback 文書化 | §5（段 1〜3=単一操作・段 4 以降=§5.1 順序付き複合手順・rotation=§5.2 の工程別） |
| retirement evidence | §7（lane 別収束・試行計数・cadence 窓・3 点セット） |

- S3 の修正前 FAIL 実測（規律）:
  - kintone dedup:「同一 `id` の 2 回配信で処理が 2 回走る」を旧コードで実測→実装後 skip。
  - dedup fail-closed:「DB 到達不能でも処理が走る（沈黙で dedup 抜け）」を旧設計相当で実測→
    実装後 5xx（H04）。
  - legacy 停止:「停止 list 記載 lane に旧 token で 200」を旧コードで実測→実装後 404
    ＋`legacy_blocked` 計数。
  - H07 strict 検証: 異常形 5 種で「起動成功してしまう」を旧コードで実測→実装後 起動停止。
  - H05 state/監視分離: kintone `failed` 行が既存 backlog 警報（Stripe 文言）に混入することを
    旧コードで実測→実装後 provider 別警報。
  - **crash 境界（D2-M02・marker 含む）**: LINE 送信直前 crash（marker のみ）／LINE 送信後・
    App 29 更新前 crash（marker＋送信済み）の各 fixture で、旧コード=「行が `received` の
    まま区別不能」を実測→実装後 `sending` marker により判別可能（§4.2 crash 表の全行を
    テストで固定）。
  - **marker 後例外の failed 非上書き（D2-M02残・D4）**: marker 成功後に LINE API 例外/
    timeout を注入し、「素朴実装＝`failed` へ上書き」が起きる形を旧設計相当で実測→
    実装後 **最終 state=`sending` のまま**を assert（**最終 state と LINE 送信回数の両方を
    assert する**）。
  - **marker 失敗時 LINE write 0（D2-M02残・D3-H01）**: marker UPDATE を失敗/例外/rowcount=0 に
    細工し、旧設計相当=「送信が走ってしまう」を実測→実装後 **LINE 送信回数 0・行は
    `received` のまま**を assert（同じく最終 state＋送信回数の両 assert）。
  - **正常 no-op の terminal 化（D4-M01）**: no-op 各分類（not_approved／already_sent／
    record_not_found／missing_fields）の fixture で、旧設計相当=「行が `received` のまま
    残留→1h 後に §4.2b が**偽警報**」を実測→実装後 **最終 state=`done`・LINE 送信 0・
    理由コード一致**（分類ごとに個別 assert・enum 外の自由文字列が書かれないことも assert）。
  - **stale received/sending（D2-M02・§4.2b）**: 閾値超え fixture で旧コード=「警報なし
    （検知空白）or Stripe 文言へ混入」を実測→実装後 kintone 専用警報。
  - **rotation 4 状態 table test（D2-M02）**: {old-only／dual（primary=旧+NEXT=新）／
    new-primary+NEXT（5-4a 後）／NEXT 削除後} の 4 状態 × {旧 token・新 token} の受理/拒否を
    table test で固定（旧コード=NEXT 概念なしで dual 状態が FAIL する形）。
- **provider 不変 assert（D2-M03）**: Stripe/LINE/kintone の**混在 fixture**（3 provider の
  滞留行を同一 DB に共存させる）で、監視集計の **provider 別件数・provider 別最古時刻・
  既存閾値（Stripe/LINE=24h が kintone=1h に引きずられないこと）・警報文面（Stripe 文言に
  kintone が混ざらない/逆も）**を個別に assert するテストを S3 受入に含める（§4.2b の
  分離条件の機械担保・既存 Stripe/LINE 監視の回帰防止）。

## §9 OPEN → 裁定確定（R-RV-04C-D・D2 反映済み）

| # | 論点 | 裁定（確定） |
|---|---|---|
| 1 | caller 1 本 vs lane 別 key | **条件付き採用**（caller 1 本。条件=§3 M03 の Script Properties 採用条件 5 点が維持されること。条件が崩れたら即 rotation・lane 分離は将来のプロジェクト分離時に再検討） |
| 2 | dedup fail-open | **不採用（H04 裁定）**。fail-closed（5xx・検知可能な 1 件喪失を安全側とする比較裁定・§4）。revision 原子 claim は RCF-M13（Phase 2 候補・司令塔台帳） |
| 3 | XFF 補助検証 | **observe-only で採用**（§4.1。enforce 昇格は観測実績後の別裁定） |
| 4 | 観測期間 | **cadence 基準**（一律日数は廃止。lane 別最大実行間隔×2＋最低成功回数・§7-3） |
| 5 | 停止 list の形 | **strict 検証条件付き採用**（H07。既知 5 path 厳格集合・異常形は固定文言で起動停止・404=存在しないフリ踏襲を確認） |
