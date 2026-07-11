# DRAFT: RV-04 query token → header HMAC 移行設計

> **status: DRAFT（司令塔裁定待ち）・実装開始根拠にしない。**
> 対象SHA 7b03069 の実物調査に基づく叩き台。確定は次セッションの司令塔裁定。

前提資料: docs/evidence/ENDPOINT_TRUST_BOUNDARY_INVENTORY.md（境界B）・製品設計完全版v2.4 §12.10。

## 0. 問題
境界B の8本は共有 token を **URL query/path** に載せる（`?token=`・`/{secret}`）。
アクセスログ・リファラ・プロキシ・GASエディタ履歴で漏れうる。署名（HMAC）＋
timestamp＋nonce へ移行し、replay・path転用・body改変・期限外を防ぐ。

## 1. ★最重要の分岐: 呼出し元がヘッダを付けられるか

実物調査の結論（対象8本を2群に分ける）:

| endpoint | 呼出し元 | カスタムヘッダ | HMAC移行 |
|---|---|---|---|
| /koseki/ingest | GAS UrlFetchApp | **可**（legacy/gas/コード.js:124/154 で `headers:` を実使用） | ○ v1署名 |
| /registry/ingest | GAS | 可 | ○ |
| /valuation/ingest | GAS(予定) | 可 | ○ |
| /bank/ingest | GAS(予定) | 可 | ○ |
| /sortation/ingest | GAS | 可 | ○ |
| /webhook/kintone/approval | **kintone webhook** | **不可** | △ 代替（後述） |
| /hub/dispatch | **kintone webhook** | **不可** | △ 代替 |
| /document/{secret} | **kintone webhook** | **不可** | △ 代替 |

**kintone のアプリWebhook はカスタムヘッダを付けられない**（固定POSTのみ）。
→ HMAC-in-header は kintone webhook 由来の3本には**適用不能**。分岐して代替設計にする。

## 2. GAS群（5本）: header HMAC v1

### 2.1 canonical encoding（署名対象文字列 v1）
改行 `\n` 連結・順序固定:
```
v1
<key_id>
<caller_id>
<method(大文字)>
<normalized_path>        # クエリ除去・末尾スラッシュ正規化。例 /koseki/ingest
<timestamp(UNIX秒)>
<nonce(128bit hex)>
<content_sha256(hex)>    # リクエストbody生バイトのSHA-256
```
署名 = `hex(HMAC_SHA256(key=<caller別secret>, msg=<上記canonical>))`。

### 2.2 送信ヘッダ
```
X-Sig-Version: v1
X-Sig-Key-Id: <key_id>          # secretのローテーション識別（例 koseki-2026-07）
X-Sig-Caller: <caller_id>       # gas-koseki 等
X-Sig-Timestamp: <unix秒>
X-Sig-Nonce: <128bit hex>
X-Sig-Content-SHA256: <hex>
X-Sig-Signature: <hex>
```
※ multipart（file+drive_file_id）でも content_sha256 は**生bodyバイト全体**を対象にする
（GAS側は `blob.getBytes()` を含む最終payloadのhashを取る必要＝GAS実装で要検証）。

### 2.3 サーバ検証順（fail-closed）
1. version==v1 / 必須ヘッダ全存在（欠落=401）
2. key_id→secret解決（unknown key=401・**keyストアはenv `SIG_KEY_<KEY_ID>` 案**）
3. `abs(now - timestamp) <= SKEW`（既定300秒・env `SIG_MAX_SKEW_SEC`）超過=401
4. content_sha256 が実body hashと一致（body改変検知・不一致=401）
5. 署名再計算＝`compare_digest`（不一致=401）
6. **nonce一回性**（後述）を満たす（再利用=409 or 401）
すべて通れば処理。既存の verify_token（query）は §4 の dual-accept 期間のみ併存。

### 2.4 nonce 一回使用のサーバ側実装（inbound_event 流用の可否）
- **案A（流用）**: `inbound_event` に `provider="sig:<caller>"` / `dedup_key="nonce:<nonce>"`
  で INSERT、UNIQUE衝突=replay。既存の journal 基盤（P1-005a）をそのまま使え、
  滞留監視・TTLも共通化できる。ただし ingest は現状 journal を通していない（Stripeのみ）。
- **案B（専用）**: `signature_nonce(nonce PK, caller, seen_at)` を新テーブルにし、
  `seen_at < now-SKEW` の行を定期削除。責務が明確・inbound_eventの意味が濁らない。
- **叩き台の推奨**: 案B（nonceは署名検証の関心事で、業務イベントjournalとは別レイヤ。
  ただしテーブル追加コスト）。timestamp SKEW窓（5分）内のnonceだけ保持すれば良いので小さい。
- 【論点1】案A/B の選択（司令塔裁定）。

## 3. kintone webhook群（3本）: 代替設計

ヘッダ不可のため HMAC不可。3案:

- **案K1（URL secret強化＋rotation運用）**: 現状の path/query secret を「長いランダム＋
  key_id埋め込み（/document/{key_id}.{secret}）」にし、定期rotationを runbook 化。
  署名ではないので replay/body改変は防げないが、kintone→自サーバのTLS内でIP的にも
  限定され、**現実的な最小改善**。
- **案K2（中継GAS化）**: kintone webhook を直接受けず、GAS Web App or 中継を挟んで
  署名を付け直す。到達性は増えるが構成が複雑・障害点増。
- **案K3（ポーリング化）**: webhook廃止し、GAS/schedulerがkintoneをポーリングして
  自サーバの署名付きエンドポイントを叩く（sortation第2段の App38 ポーリングが実例・
  legacy/gas/コード.js:120-）。webhookのリアルタイム性は落ちるが署名境界に統一できる。
- **叩き台の推奨**: 3本それぞれ用途で分ける — approval/hub-dispatch は案K1（rotation）、
  document は生成トリガーなので案K3（ポーリング）に寄せられる可能性。
- 【論点2】kintone webhook 3本の代替方式（K1/K2/K3）の選択。

## 4. dual-accept 期間の設計（GAS群）
1. **Phase A（併存）**: サーバは「新署名OK **or** 旧query token OK」を受理。
   GAS未改修でも動く。新ロジックを本番投入しても既存を壊さない。
2. **Phase B（GAS切替）**: GASエディタで各フォルダの fetch に署名ヘッダ付与（大野）。
   Railwayログで「署名経路で来ているか」を観測（signature_result列で判別）。
3. **Phase C（旧廃止）**: 全caller が署名経路に移ったことを確認後、query token 受理を停止。
   旧 `*_INGEST_TOKEN` env を revoke。
4. **rotation**: 以後は key_id 単位で secret を回す（新key_id併存→旧key_id廃止）。

## 5. caller別 切替順序（制約つき）
1. **/sortation/ingest**（最初）: 業務影響が観測しやすく、回送先はin-process＝この1本の
   切替で下流も守れる。
2. koseki/registry（戸籍・登記ライン・実機実績あり）
3. valuation/bank（token未投入＝未稼働のうちに新方式で開通できる＝dual不要の好機）
4. kintone webhook 3本（代替方式・別トラック）
- GASは1ファイル（legacy/gas/コード.js＝正本はGASエディタ側）に全fetchが集約されるため、
  **署名付与ヘルパをGAS内に1つ作り各fetchで共用**する実装が現実的（大野作業・BLOCKED）。

## 6. テスト戦略
- **replay**: 同一nonce再送→409/401（案A/Bどちらでも）
- **path転用**: /koseki の署名を /bank へ→normalized_path不一致で401
- **body改変**: content_sha256改変・body差し替え→401
- **期限外**: timestamp を SKEW超過→401
- **unknown key**: 未登録key_id→401
- **skew境界**: ちょうどSKEW内/外の分岐
- **dual-accept**: 新署名OK・旧token OK・両方無し（401）の3系
- いずれも mock（実HTTP不要）。canonical encoding の固定は golden 文字列で pin。
- kintone webhook代替は方式確定後に別途。

## 7. 論点・BLOCKED
- 【論点1】nonceストア（inbound_event流用=案A / 専用テーブル=案B）
- 【論点2】kintone webhook 3本の代替（K1/K2/K3）
- 【論点3】key_idの保管（env `SIG_KEY_*` / 将来のsecret manager）
- 【論点4】content_sha256 の対象（multipart生body全体で確定してよいか・GAS実装可否）
- BLOCKED_NEEDS_HUMAN: GASエディタでの署名ヘッダ実装（UrlFetchAppでの生bodyhash計算の
  実現性検証）・kintone webhook設定のヘッダ/URL自由度の実確認・watcher（/ocrは境界Cで別だが
  同じ事務所PC由来で将来署名化するなら実装場所の確認）
