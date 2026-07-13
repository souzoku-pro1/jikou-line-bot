# DRAFT: RV-04 query token → header HMAC 移行設計（v2・Codexレビュー反映）

> **status: DRAFT（司令塔裁定待ち）・実装開始根拠にしない。**
> 対象SHA 7b03069 の実物調査に基づく叩き台。R-P1-007-drafts-v2 の所見（BLOCKER2/HIGH11/
> MEDIUM12/LOW3 全ACCEPT・REJECT0）を反映した改訂版。確定は次セッションの司令塔裁定。
> **OPEN 項目は仮決めせず OPEN ラベル＋owner を明記する。**

前提資料: docs/evidence/ENDPOINT_TRUST_BOUNDARY_INVENTORY.md（境界B）・製品設計完全版v2.4 §12.4/§12.10。

---

## ★共有節: 実装順序骨子（M11・3 DRAFT 共通）

RV04/RV10/App36 の重量タスクは相互依存する。M11 として全体を8段階で並べる（3 DRAFT に
同一掲載）。各段の前提を跨がない順にする:

1. **redaction contract 確定**（RV10 §1・sink/audience policy と禁止カテゴリの確定。
   OPEN の伏字水準は大野裁定で埋める）— 以降のログ設計の土台。
2. **RV10 S1 切替＋notify fail-closed**（顧客Bot への機微 PII 漏れを最優先で停止）。
3. **RV04 multipart body-hash PoC**（v1 contract 成立条件・別票）。GAS の UrlFetchApp で
   生body の SHA-256 が再現できることを実証してから canonical を凍結。
4. **RV04 GAS群 header HMAC 実装＋dual-accept Phase A**（署名存在時 fallback 禁止＝downgrade防止）。
5. **RV10 S2/S3/S4 段階解消＋AST 機械強制**（body最小化・print全廃・例外分類化）。
6. **App36 DerivationRun（immutable）＋App36 projection 起票（R4-3b）**。
7. **App37 割付＋TemplateVersion registry**（成果物生成 Phase 5 の前提）。
8. **dead-man 監視＋RV04 dual-accept 廃止（Phase C）＋kintone webhook 代替（K選択後）**。

OPEN（各段の着手前に大野裁定が要る）: K選択（RV04 §3）・PII 出し分け水準（RV10 §1）・
凍結表追補（App36 放棄写像）・過去ログ裁定（RV10）。

---

## 0. 問題
境界B の8本は共有 token を **URL query/path** に載せる（`?token=`・`/{secret}`）。
アクセスログ・リファラ・プロキシ・GASエディタ履歴で漏れうる。署名（HMAC）＋timestamp＋
nonce へ移行し、replay・path転用・body改変・期限外・downgrade を防ぐ。

## 1. ★最重要の分岐: 呼出し元がヘッダを付けられるか

| endpoint | 呼出し元 | カスタムヘッダ | HMAC移行 |
|---|---|---|---|
| /koseki/ingest | GAS UrlFetchApp | **可**（legacy/gas/コード.js:124/154 で `headers:` を実使用） | ○ v1署名 |
| /registry/ingest | GAS | 可 | ○ |
| /valuation/ingest | GAS(予定) | 可 | ○ |
| /bank/ingest | GAS(予定) | 可 | ○ |
| /sortation/ingest | GAS | 可 | ○ |
| /webhook/kintone/approval | **kintone webhook** | **不可** | △ 代替（§3） |
| /hub/dispatch | **kintone webhook** | **不可** | △ 代替 |
| /document/{secret} | **kintone webhook** | **不可** | △ 代替 |

**kintone のアプリWebhook はカスタムヘッダを付けられない**（固定POSTのみ）→ HMAC-in-header は
kintone webhook 由来の3本には**適用不能**。§3 の代替に分岐。

## 2. GAS群（5本）: header HMAC v1

### 2.1 canonical encoding（BLOCKER: 曖昧さの排除／NM01: v1 は単一方式）
**NM01（検証裁定）**: **length-prefix 方式を唯一の v1 とする**。厳格文字集合方式は「代替」
ではなく**別 version（v2）**として version 識別子で分離する（**同一 version 内に二方式を
併存させない**＝検証側が version から一意に方式を決められる）。v1 の凍結は §7 の
multipart PoC 完了後（PoC で content 対象が確定してから golden を固定）。

> **NM01 v1 = FROZEN（2026-07-13・司令塔裁定・PoC=P1-103）**
> **凍結範囲は「① content 対象の定義」と「② canonical 形式」に限定する**（③ §2.3 検証順の
> 実装詳細と ④ GAS 適用性は凍結対象外＝下記のとおり P1-103-fix で更に硬化／条件付き解決）。
> - **① content 対象を「送出最終バイト列全体の SHA-256」と確定（FROZEN）**（＝クライアントが
>   実際に wire へ載せる body 全バイトの SHA-256。multipart の場合は boundary を含む body 全体）。
>   PoC（`test_hmac_multipart_poc.py`）で hash 一致・改変検知(401)・boundary 吸収・
>   `body()`先読みと`form()`受理の共存を実証。§7 が警戒した「フィールド別 hash への再設計」は不要。
> - **② canonical 形式（§2.1 length-prefix・ORDER）を確定（FROZEN）**。version は v1 単一方式。
> - **③ §2.3 検証順（実装詳細）は凍結対象外・P1-103-fix で硬化**: 署名対象 path は
>   **実 routing path**で再計算し client 指定の path 系ヘッダは非信用（H01）／key lifecycle の
>   reason を分離（`key_unknown`/`key_revoked`/`key_not_yet_valid`/`key_expired`・retiring は
>   受理+警告〔H02〕）。§6 の 403/401 ケース分けを参照。
> - **④ GAS 適用性 = CONDITIONALLY_RESOLVED**（§7）。条件 = (i) GAS caller が multipart を
>   **手組み**すること（固定 boundary を自ら選定・`Content-Type: multipart/form-data; boundary=…`
>   を明示 set・content_sha256 はその**同一バイト列**を対象に計算。UrlFetchApp の payload(Blob)
>   自動組立に依存しない＝自動採番 boundary を送出前に読めず hash 不能になるリスクを排除）／
>   (ii) **caller 移行段階の[人]実機検分**で GAS 実体の payload hash 一致を確認（本 PoC は
>   サーバ契約と wire 形式を実証・GAS 実体は OUT_OF_SCOPE のため未実行）。

v1 canonical（length-prefix）:
```
canonical = concat_for each field f in ORDER:
              ascii(len(utf8(f))) || ":" || utf8(f) || "\n"
ORDER = [ "v1", key_id, caller_id, method_upper, normalized_path,
          timestamp_str, nonce_hex, content_sha256_hex ]
```
- 参考（将来 v2 候補・v1 では使わない）: 厳格文字集合限定＋`\n` 連結。採用時は
  `X-Sig-Version: v2` として別扱いにし、サーバは version ごとに検証器を分ける。
- **normalized_path（H02: 規則を確定）**: **decode 前の raw path** を対象にする（%エンコード
  デコードで path を再解釈しない）。正規化は **末尾 slash 除去のみ**。以下は**拒否（400）**:
  `%2F`（エンコード slash）・dot segment（`.`/`..`）・連続 slash（`//`）・非 ASCII 生バイト。
  → 「正規化で意味が変わる余地」をゼロにし、署名対象 path と実ルーティング path のズレを排除。
- 署名 = `hex(HMAC_SHA256(key=<key_id が解決する secret>, msg=utf8(canonical)))`。
- **cross-language テストベクトル節（HIGH）**: server(Python)・client(GAS/JS)双方で同一
  canonical→同一署名になることを、固定入力→固定署名の golden ベクトル（最低5本・
  ASCII/日本語ファイル名/空body/multipart/境界長）で相互検証する。**加えて path 異常形の
  拒否ケース（%2F・`..`・`//`・非ASCII）を testベクトルに追加**（H02）。これを v1 contract の一部にする。
  - **golden 固定時は P1-103 PoC の生 body 入力を流用する**（`test_hmac_multipart_poc.py` の
    日本語ファイル名 multipart・手組み固定 boundary body・空 body 等を固定入力ベクトルとして
    server↔GAS で同一署名を確認）。PoC で content 対象＝送出生 body 全体と確定済みのため、
    golden はこの固定バイト列の SHA-256 を content_sha256 として据える。

### 2.2 送信ヘッダ
```
X-Sig-Version: v1
X-Sig-Key-Id: <key_id>
X-Sig-Caller: <caller_id>
X-Sig-Timestamp: <unix秒>
X-Sig-Nonce: <128bit hex>
X-Sig-Content-SHA256: <hex>
X-Sig-Signature: <hex>
```

### 2.3 サーバ検証順（fail-closed・downgrade防止）
1. **署名ヘッダ（X-Sig-*）が1つでも存在すれば署名経路として扱い、query token への
   fallback を禁止**（HIGH: downgrade 攻撃防止）。version!=v1 や欠落は 401（旧経路に落ちない）。
2. key_id を key registry（§2.5）で解決（unknown/expired/revoked=401）。
3. caller_id と key registry の `caller` 一致（不一致=401）。
4. method / normalized_path が registry の `allowed_methods`/`allowed_paths` に含まれる（外=403）。
5. `now - SKEW <= timestamp <= now + SKEW`（既定SKEW=300秒・env `SIG_MAX_SKEW_SEC`）超過=401。
6. content_sha256 が実body hash と一致（body改変検知・不一致=401）。
7. 署名再計算＝`compare_digest`（不一致=401）。
8. **nonce 一回性**（§2.4）。再利用=409。
全通過で処理。query token 受理は §4 dual-accept 期間中、かつ**署名ヘッダ不在時のみ**併存。

### 2.4 nonce 一回使用（HIGH: 保持期限の明確化）
- **保持期限 = 署名 timestamp + SKEW**（受理し得る最遅時刻）まで。この時刻を過ぎた nonce は
  そもそも §2.3-5 で timestamp 超過 401 になるため、期限切れ nonce 行は安全に削除できる。
  → nonce 行に `expires_at = timestamp + SKEW` を持たせ、`expires_at < now` を定期削除。
- **案A（inbound_event 流用）** / **案B（専用 signature_nonce テーブル）**。
  - 叩き台推奨: **案B**（nonce は署名検証レイヤの関心事・inbound_event の業務意味を濁さない。
    保持窓が SKEW=5分と小さくテーブルも小さい）。schema 案:
    `signature_nonce(nonce TEXT PK, key_id TEXT, caller TEXT, seen_at ts, expires_at ts)`。
    INSERT の UNIQUE 衝突＝replay→409。
- 【OPEN・owner=大野/司令塔】案A/B（判断材料: inbound_event の運用一体化 vs レイヤ分離）。

### 2.5 key registry モデル（BLOCKER: 鍵管理の構造化）
key_id 単位で以下を持つ（保管先は env or 将来の secret manager・§7）:
```
key_id        : 一意・再利用禁止（rotation で新IDを発番）
secret        : HMAC 鍵（値はログ/例外に出さない）
caller        : この鍵を使える caller_id（例 gas-koseki）
allowed_methods: {POST}
allowed_paths : {/koseki/ingest} 等（caller が叩いてよい path 集合）
not_before    : 有効化時刻
expires_at    : 失効時刻
status        : active / retiring / revoked
```
- **rotation lifecycle（HIGH）**: (1) 新 key_id を `active`・not_before 設定で追加 →
  (2) GAS を新 key_id に切替 → (3) 旧 key_id を `retiring`（受理はするが警告ログ）→
  (4) 一定期間後 `revoked`（受理停止）。**key_id は失効後も再利用しない**（過去署名の
  取り違え防止）。rollback は「新 key_id を revoked にし旧を active に戻す」で対応。
- **status / 時刻に対する (status, reason) の期待値は §6.2 を単一の契約とする**（本節に
  reason を二重定義しない＝期待値の分岐を防ぐ）。要点のみ: unknown=`key_unknown` /
  revoked=`key_revoked` / not_before 前=`key_not_yet_valid` / expired=`key_expired`（各 401）、
  `retiring`=受理 + **warning ログ 1 回**（key_id・caller_id のみ可視・secret/署名/nonce/body
  非混入）。詳細と網羅表は §6.2 を参照。

## 3. kintone webhook群（3本）: 代替設計（採用条件つき）

ヘッダ不可のため HMAC 不可。各案は**採用条件を満たす場合のみ**可:

- **案K1（URL secret 強化＋rotation）**: 署名ではないので単独では replay/body改変を防げない。
  **§12.4 の代替防御を一括で束ねた場合のみ採用可**（MEDIUM）:
  ①イベント dedup（inbound_event journal）②条件付き状態遷移（refetch_and_check で最新状態
  再判定）③kintone 側の再照合（受信 recordId を kintone から取り直して検証）④source
  restriction（可能なら kintone/CloudSign の送信元 IP レンジ制限）⑤詳細ログ抑止（reason code のみ）。
  この5点セットが揃わない K1 は不可。
- **案K2（中継GAS化）**: 中継を挟んで署名を付け直す。**採用時の成立条件（H05・全て満たすこと）**:
  ①中継入口自体の認証方式（中継が誰でも叩ける口にならない・中継→本サーバは §2 の署名を付ける）
  ②kintone 再照合（受信 recordId を kintone から取り直して検証）③イベント dedup（inbound_event）
  ④payload allowlist（中継が転送する payload のキーを許可制にし、想定外フィールドを落とす）。
- **案K3（ポーリング化）**: webhook 廃止し GAS/scheduler が kintone をポーリング→署名付き
  エンドポイントへ。**M04（条件付き）**: リアルタイム性低下の許容・ポーリング間隔・
  取りこぼし防止（カーソル/更新時刻）・kintone API レート・二重処理防止（dedup）を
  満たす場合のみ。sortation 第2段の App38 ポーリング（legacy/gas/コード.js:120-）が実例。
- 【OPEN・owner=大野/司令塔】kintone webhook 3本の代替（K1/K2/K3）。判断材料:
  approval/hub-dispatch は状態遷移トリガー（K1 の②③が効く）・document は生成トリガー
  （K3 に寄せやすい）。3本一律でなく用途別選択でよい。

## 4. dual-accept 期間（GAS群・downgrade 防止込み）
1. **Phase A（併存）**: **署名ヘッダ不在時のみ** 旧 query token を受理。署名ヘッダがあれば
   署名検証のみ（失敗しても token に落ちない＝downgrade 防止）。
2. **Phase B（GAS切替）**: GAS に署名付与（大野）。signature_result 列で署名経路到達を観測。
3. **Phase C（旧廃止）**: 全 caller 署名移行を確認後、query token 受理を停止・旧 `*_INGEST_TOKEN`
   revoke。
4. **rotation**: 以後 §2.5 の lifecycle で key_id 単位に回す。

## 5. caller別 切替順序
1. /sortation/ingest（業務影響が観測しやすい・回送先 in-process）
2. koseki/registry（実機実績あり）
3. valuation/bank（token 未投入＝未稼働のうちに新方式で開通＝dual 不要の好機）
4. kintone webhook 3本（代替・別トラック）
- GAS は1ファイルに全 fetch 集約 → **署名付与ヘルパを GAS 内に1つ**作り共用（大野・BLOCKED）。

## 6. テスト戦略

### 6.1 403 と 401 のケース分け（M03: status を混同しない）
検証は §2.3 の**段順**で最初に不成立になった段の status/reason を返す（fail-closed）。
特に **path 不許可（4段）と署名不一致（7段）を status で分離**する:

| 契機 | 段 | status | reason | 備考 |
|---|---|---|---|---|
| method 非許可 | 4 | **403** | `method_denied` | registry の `allowed_methods` 外 |
| **実 path が allowlist 外** | 4 | **403** | `path_denied` | 実 routing path が `allowed_paths` に無い。**署名検証に到達しない** |
| path 正規化違反（%2F・`..`・`//`・非ASCII） | 4 | **400** | `bad_path` | §2.1 H02 |
| **署名不一致（実 path で再計算）** | 7 | **401** | `bad_sig` | 実 path が allowlist 内でも、署名 path≠実 path なら 7 段で露見 |
| content_sha256 不一致 | 6 | 401 | `body_mismatch` | body 改変 |
| timestamp SKEW 超過 | 5 | 401 | `skew` | ±SKEW 境界 |
| replay（nonce 再利用） | 8 | 409 | `replay` | |

**H01 の要点**: 署名対象 path は **ASGI `scope["raw_path"]`（decode 前生バイト）**で再計算し、
client 指定の path 系ヘッダ（旧 X-Sig-Path 相当）も **decode 済み path** も検証の真実源にしない。
decode 済み path を使うと `%2F` が `/` に化けて separator を smuggling され path 拘束が破れる
（例: 実 raw `/koseki%2Fingest` が decode 後 `/koseki/ingest` として許可されてしまう）。
→ allowlist 外の実 path へ正規署名を転用しても**4 段 path deny=403**、allowlist 内の別実 path へ
転用しても**7 段署名不一致=401 bad_sig**、`%2F`/`%2e`/`%252F`/`//`/非 ASCII 生バイトは**400 bad_path**、
`raw_path` 欠落は**fail-closed（受理しない）**。

### 6.2 key lifecycle の reason 分離（H02・**reason contract の単一の正**）
§2.5 はここを参照する（reason を二重定義しない）。段は §2.3 の検証段番号。

| status/時刻 | 段 | status | reason |
|---|---|---|---|
| 未登録 key_id | 2 | 401 | `key_unknown` |
| `revoked` | 2 | 401 | `key_revoked` |
| `now < not_before` | 2 | 401 | `key_not_yet_valid` |
| `now > expires_at` | 2 | 401 | `key_expired` |
| `retiring` | 2→8 全通過 | **200** | `ok_retiring`（**受理+警告**） |
| `active` | 2→8 全通過 | 200 | `ok` |

- **retiring の warning ログ（M01）**: 受理時に warning を**1 回**出力。可視は **key_id・caller_id のみ**。
  secret・署名・nonce・body（およびその hash）は**混入させない**（§7 認証ログ方針と統合）。
- 判定順の帰結: revoked は時刻窓より先に判定（revoked 鍵は期限内でも `key_revoked`）。
  not_before / expires_at 違反は status に依らず時刻 reason を返す。

### 6.3 本体実装（RV-04 body）で必須のテスト一覧（M01: Codex 提案の不足分を全数反映）
PoC で実証済みの項目に加え、本体実装は下記を**全数**テスト化すること:
- **署名検証段（§2.3 各段）**: caller 不一致=401 `caller_mismatch`／version 欠落・不正（`v1`以外）=401 `bad_version`／
  timestamp 形式不正（非数値）=401 `bad_ts`／future skew（`now+SKEW` 超過）=401 `skew`／
  **skew ±300 境界**（`now±300` は受理・`now±301` は拒否の両側）／required header 欠落
  （各 `X-Sig-*` 欠落で個別に拒否・欠落 header 名で挙動が変わらないこと）。
- **downgrade 防止**: 署名ヘッダ在＋query token 併記時に token へ落ちない（署名不正なら 401・
  token では受理しない）／dual-accept 3系（署名OK・token OK・両方無=401）。
- **status–reason table**: §6.1/§6.2 の (status, reason) 表を 1 対 1 で網羅する parametrized テスト。
- **canonical / cross-language**: canonical golden ベクトル固定（§2.1・最低5本＝ASCII/日本語
  ファイル名/空 body/multipart/境界長）／server(Python)↔client(GAS/JS) の cross-language 一致。
- **multipart 生 body 形状**: boundary 衝突（body 内に boundary 類似列）／filename の quote/escape／
  CRLF 正規化（`\r\n` 固定）／空 body／複数 field（file+meta）で content hash が生 body 全体基準で
  一致すること。
- **GAS 実機**: GAS UrlFetchApp が送出する最終 payload の SHA-256 が server 受信生 body の hash と
  一致することを実機で比較（手組み multipart 前提・§2.1 実装制約／§7・caller 移行段階の[人]検分）。
- **path 拘束（H01）**: 署名対象 path は **ASGI `scope["raw_path"]`（decode 前生バイト）**で
  再計算（decode 済み path は %2F separator smuggling を許すため不可）。allowlist 外実 path=403
  `path_denied`／allowlist 内別実 path 転用=401 `bad_sig`／`%2F`・`%2e`・`%252F`・`//`・非 ASCII
  生バイト=400 `bad_path`／`raw_path` 欠落=fail-closed（受理しない）。
- **key lifecycle（H02）**: unknown/revoked/expired/not_before の reason 分離・retiring 受理+警告。

> **PoC 実証済み（P1-103 / -fix / -fix2・`test_hmac_multipart_poc.py`・21 tests）**:
> content hash 一致(a)／boundary 非依存(b)／1byte 改変拒否(c)／replay・skew・unknown key(d)／
> GAS 相当 httpx multipart(e)／raw body 取得／body()×form() 共存／H01 path 拘束（foreign=403・
> 別実 path=bad_sig・raw_path %2F/%2e/%252F/// 拒否・raw_path 欠落 fail-closed・normalize 単体）／
> H02 lifecycle 5 系／M01 retiring warning（1 回・機微ゼロ）／§6.1・§6.2 status–reason table。
> 本体実装は上記 6.3 の残り（downgrade・cross-language golden・GAS 実機 等）を本番経路で全数化する。

## 7. 論点・OPEN・BLOCKED
- 【OPEN・owner=大野/司令塔】nonce ストア（案A/B）・kintone webhook 代替（K1/K2/K3）。
- 【OPEN・owner=大野/司令塔】key_id 保管方式（Railway env `SIG_KEY_*` / 将来の secret manager）。
  判断材料: Railway env の管理容易さ vs secret manager の監査/rotation 機能・rotation 運用コスト
  （§2.5 lifecycle を env 手運用で回すか managed で回すか）。
- **multipart body-hash PoC（M11 段3）→ CONDITIONALLY_RESOLVED（P1-103／P1-103-fix・2026-07-13）**:
  サーバ受信生 body の SHA-256 が送出最終バイト列の SHA-256 と一致することを実証
  （`test_hmac_multipart_poc.py`・15 tests）。content 対象＝送出生 body 全体で確定・
  フィールド別 hash への再設計は**不要**（§2.1 ①②を FROZEN）。**条件**（未充足なら freeze の
  GAS 適用は保留）: (i) GAS caller が multipart を手組み（固定 boundary 自選・Content-Type 明示 set・
  hash は同一バイト列）／(ii) **caller 移行段階の[人]実機検分**で GAS 実体 payload の hash 一致を
  確認。サーバ契約・wire 形式・path 拘束(H01)・key lifecycle(H02) は PoC で実証済み。
- **認証ログ方針（HIGH）**: 認証失敗ログは **固定 reason code（`bad_sig`/`skew`/`key_revoked`/
  `path_denied`/`replay` 等）＋caller_id＋key_id＋相関ID(nonce or request id)** に限定。
  secret・body・PII・vendor 生値を出さない（RV10 §禁止と統合）。
- BLOCKED_NEEDS_HUMAN: GAS 署名ヘッダ実装（生body hash 計算の実現性＝PoC）・kintone webhook
  設定のヘッダ/URL/送信元 IP 自由度の実確認・watcher（/ocr は境界C で別だが将来署名化の実装場所）。
