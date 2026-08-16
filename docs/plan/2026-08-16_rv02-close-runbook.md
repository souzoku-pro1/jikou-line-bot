# RV-02 CLOSE 点火手順書 — /scan・/ocr/fixed-asset の署名強制化（起草）

- 票: RV02-CLOSE-PLAN（2026-08-16・READ_ONLY+docs・PC-A）
- 位置づけ: 監査（`docs/plan/2026-08-15_audit.md` §5）で RV-02=Phase 0 BLOCKER 未達を確認。
  司令塔裁定=最優先で封鎖。RV-0102-PREP（PR #200・署名 opt-in 二重受理）を土台に、
  非署名リクエストの**拒否（強制化）**までの段取りを定める。
- 本書の実測はすべて 2026-08-16 時点（repo=plan-audit `f50947c` 起点・本番 env は
  railway CLI READ_ONLY 実測）。**コード変更は含まない**（必要な変更は §3 の小票提案）。
- 様式: ブロックA点火の §8 方式（`docs/design-drafts/DRAFT_P2_DURABLE_IGNITION.md` §8 /
  `docs/work-logs/2026-08-11_block-a-ignition.md`）に倣う——(a)事前 read-only 機械検査→
  段階ゲート→観測→各段階 rollback 可。**secret 値は表示しない**。

---

## §1 呼び出し元の全数実査

### 1.1 repo 実物から確認できた呼び出し元（全数）

| # | 入口 | 呼び出し元 | 根拠（repo 実物） |
|---|---|---|---|
| 1 | `POST /scan` | GAS プロジェクト「相続書類自動化」`onFileAdded()` の 3 フォルダループ（相談カード/戸籍謄本/通帳）。**fetch 呼び出し点は 1 箇所**（ループ内） | `legacy/gas/コード.js:29`（読み取りコピー・取得 2026-07-03/手動同期 2026-07-06。**正本は GAS 側＝stale の可能性あり**） |
| 2 | `POST /ocr/fixed-asset` | **事務所 PC の `ocr_watcher.py`**（watchdog で「デスクトップ/OCR_inbox」を監視し multipart POST。タスクスケジューラでログオン時自動起動）。**GAS ではない** | `docs/current-state-report.md` §1.2・§2.2、`docs/evidence/ENDPOINT_TRUST_BOUNDARY_INVENTORY.md` #5。スクリプト実物は **repo 未追跡かつ PC-A 作業コピーに不在**（`ls ocr_watcher.py` → 不存在を実測）＝事務所 PC 上にのみ存在 |

- repo 内の in-process／サーバ内呼び出し: **なし**（`*.py` 全数 grep。ヒットは main.py の
  route 定義・test の TestClient・コメント参照のみ。sortation の回送は in-process 関数
  呼び出しで HTTP `/scan` へは行かない）。
- docs/config 上の別経路の手がかり: なし（`ENDPOINT_TRUST_BOUNDARY_INVENTORY.md` でも
  caller は上記 2 系統のみ）。

### 1.2 [人]確認が必要な項目（repo 外・確認手順つき）

| # | 確認事項 | 手順 |
|---|---|---|
| H-1 | **GAS 実機の現行 `onFileAdded()`**: `/scan` fetch が今も 1 箇所か・rv04c 署名ヘルパ（S4 反映済みのはず）が同居しているか・トリガー種別と周期 | GAS エディタ（scriptId `1N-GZ0lciPrU…`・t-ohno@sozoku-law.com）で目視、または PC-A で `NODE_OPTIONS=--use-system-ca` を設定し `legacy/gas/` で `clasp pull`（要 clasp 再ログイン・2026-07-06 以降未同期）。トリガーは GAS UI「トリガー」画面で確認 |
| H-2 | **`ocr_watcher.py` の送信実装**: HTTP ライブラリ（requests か）・multipart の組み方・リトライ/失敗時挙動・Python バージョン | 事務所 PC で実物を開く（タスクスケジューラの登録から実体パスを辿れる。`setup_ocr_watcher.bat` が登録元）。§2.5 の変更を入れる前に送信部の写しを PC-A へ共有 |
| H-3 | **未知の呼び出し元の有無**: 上記 2 系統以外に `/scan`・`/ocr/fixed-asset` を叩くものが居ないか | §4(d) の到達実測と同じ方法で検証できる——署名切替完了後に unsigned 到達が 0 にならなければ未知 caller が存在する。事前には railway logs の `POST /scan`・`POST /ocr/fixed-asset` 行の時刻と業務実態（スキャン作業時刻）の突合でも当たりが付く |

---

## §2 現行配線の実測

### 2.1 サーバ側ゲート（コード実測）

- 両入口は `signed_optin_router`（`BodyCachingRoute`）配下で
  `Depends(optional_signature_guard())` → `authorize_optionally_signed`
  （`main.py:1187`・`main.py:1402`・`main.py:1474`、`hub/service_auth.py:575-591`）。
- `authorize_optionally_signed` の分岐（`hub/service_auth.py:587-591`）:
  - flag OFF → **無条件受理**（署名ヘッダが付いていても無視）
  - flag ON・署名ヘッダ在 → `_enforce_signed_request`（§2.3 全 8 段・token/無認証へ
    fallback しない）
  - flag ON・署名ヘッダ皆無 → **受理**（現行挙動不変）… **← RV-02 の残穴はここ**
- **非署名受理時はログが一切出ない**（`_log_ingest_decision` は署名経路でのみ呼ばれる）
  ——到達率実測（§4(d)）はこの前提で設計する。

### 2.2 flag・本番 env の実測（railway variables・2026-08-16・secret 非表示）

| env | 本番実測値 | 含意 |
|---|---|---|
| `SERVICE_AUTH_DUAL_ACCEPT_ENABLED` | **`1`（既に ON）** | **工程「flag ON」は完了済み**。opt-in 2 入口は今も「署名が付けば検証・無ければ受理」で稼働中 |
| `SERVICE_HMAC_KEY_REGISTRY` | kid=`gas-ingest-2026-07a`・caller=`gas-ingest`・methods=POST・status=active・not_before=2026-07-16・**expires_at=2026-10-15T00:00Z**・allowed_paths=`/koseki/ingest, /registry/ingest, /bank/ingest, /sortation/ingest, /valuation/ingest` | **`/scan`・`/ocr/fixed-asset` は allowed_paths に不在**。registry 更新（§4 c-0）前に送信側が署名を付け始めると `path_not_allowed` 403 で**業務停止**する——順序厳守。鍵の失効 2026-10-15 が強制化スケジュールの外側の締切 |
| `SERVICE_AUTH_LEGACY_DISABLED_PATHS` | `/sortation/ingest,/koseki/ingest` | 2 lane は既に new-only。署名基盤（registry・nonce DB・skew）は**本番で実証済み** |
| `SIG_MAX_SKEW_SEC` | 未設定 → 既定 300 秒（`service_auth.py:44`） | — |
| （参考）`VALUATION_INGEST_TOKEN`・`BANK_INGEST_TOKEN`・`SORTATION_FORWARD_ENABLED=1` | 投入済み | 台帳 §3 #7 の env 3 種は投入済み（RV-03 側の受け皿前進。実機検証状況は本書の対象外） |

### 2.3 署名方式（NM01 v1・変更なし・受信側は実装済み）

`X-Sig-Version/Key-Id/Caller/Timestamp/Nonce/Content-SHA256/Signature` の 7 ヘッダ。
canonical は length-prefix（`v1, key_id, caller, METHOD, path, ts, nonce, content_sha256`
各 `len:val\n`・`service_auth.py:284-293`）を HMAC-SHA256。nonce は 128bit hex・DB
（`signature_nonce` 表）で一回性担保。**content_sha256 は生 body バイト列の digest**
——multipart でも JSON でも同じ（受信側は `request.body()` に対して検証・
`service_auth.py:513`）。

### 2.4 GAS 側（/scan）に必要な変更 — 断片レベル

GAS には S4 反映済みの `rv04c_signing.js` があるが、`SIGNED_LANES`/`LANE_FIELDS`/
`rv04cIngestFetch_` は **multipart 5 lane 専用**で `/scan`（JSON body）を扱えない。
JSON lane 用の薄い追加が要る（正本は GAS 側・repo `gas/rv04c_signing.js` にも同文を
反映して写しを維持）:

```js
// ── /scan 用: JSON body の署名付き送信（rv04c ヘルパを共用） ──
var SIGNED_SCAN = false;   // rollback は この 1 箇所（false=現行無署名送信と同一）

function scanFetch_(jsonObj) {
  var bodyBytes = utf8Bytes_(JSON.stringify(jsonObj));   // 送信 byte と digest 対象を同一に
  if (SIGNED_SCAN !== true) {
    return UrlFetchApp.fetch(RAILWAY_URL + '/scan', {
      method: 'post', contentType: 'application/json',
      payload: bodyBytes, muteHttpExceptions: true });   // 現行送信と実質同一
  }
  var props = PropertiesService.getScriptProperties();
  var keyId = props.getProperty('RV04C_KEY_ID');
  var secretHex = props.getProperty('RV04C_SECRET_HEX');
  var csha = sha256Hex_(bodyBytes);
  var ts = Math.floor(Date.now() / 1000);
  var nonce = Utilities.getUuid().replace(/-/g, '');
  var canon = canonicalV1_(keyId, 'gas-ingest', 'POST', '/scan', ts, nonce, csha);
  var sig = hmacHex_(canon, hexToBytes_(secretHex));
  return UrlFetchApp.fetch(RAILWAY_URL + '/scan', {
    method: 'post', contentType: 'application/json',
    payload: bodyBytes,
    headers: { 'X-Sig-Version': 'v1', 'X-Sig-Key-Id': keyId,
               'X-Sig-Caller': 'gas-ingest', 'X-Sig-Timestamp': String(ts),
               'X-Sig-Nonce': nonce, 'X-Sig-Content-SHA256': csha,
               'X-Sig-Signature': sig },
    muteHttpExceptions: true });
}
```

`onFileAdded()` 側は既存の `UrlFetchApp.fetch(RAILWAY_URL + '/scan', {...})` 1 箇所を
`scanFetch_({fileData: base64Data, fileName: file.getName(), folderName: folderName})`
に置換するのみ。鍵は既存 Script Properties（`RV04C_KEY_ID`/`RV04C_SECRET_HEX`・
caller=`gas-ingest`）を共用できる（registry 側 allowed_paths への `/scan` 追加が前提）。

- 注意: 現行コードは `payload: JSON.stringify(...)`（文字列）だが、digest と送信 byte の
  同一性を機械的に保証するため **byte[] 直渡し**へ揃える（rv04cSignedFetch_ と同流儀）。
- フォルダ別の段階切替が要る場合は `SIGNED_SCAN` を
  `var SIGNED_SCAN_FOLDERS = {'通帳': true, '相談カード': false, '戸籍謄本': false}` の
  形に置き換えれば可能（fetch 点が 1 箇所なので判定を folderName で行うだけ）。
  ただし受信側は二重受理のため**一括切替でも他 lane を道連れにしない**——分割は
  「切替直後の初回スモークを書類種ごとに刻みたい」場合の選択肢。

### 2.5 watcher 側（/ocr/fixed-asset）に必要な変更 — 断片レベル

事務所 PC の `ocr_watcher.py`（Python・実物は H-2 で確認）。multipart は**手組みで
body バイト列を確定させてから** digest する（requests に boundary を任せると送信 byte
が確定しない）:

```python
import hashlib, hmac, os, secrets, time, requests

KEY_ID = os.environ["OCR_WATCHER_SIG_KEY_ID"]          # 平文でコードに書かない
SECRET = bytes.fromhex(os.environ["OCR_WATCHER_SIG_SECRET_HEX"])
URL = "https://jikou-line-bot-production.up.railway.app"

def _canonical_v1(key_id, caller, method, path, ts, nonce, csha):
    out = b""
    for f in ("v1", key_id, caller, method, path, ts, nonce, csha):
        u = f.encode("utf-8")
        out += str(len(u)).encode("ascii") + b":" + u + b"\n"
    return out

def post_fixed_asset_signed(pdf_path):
    boundary = "WATCHER" + secrets.token_hex(16)
    with open(pdf_path, "rb") as f:
        pdf = f.read()
    body = (f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="scan.pdf"\r\n'
            f"Content-Type: application/pdf\r\n\r\n").encode() + pdf + \
           f"\r\n--{boundary}--\r\n".encode()
    ts, nonce = str(int(time.time())), secrets.token_hex(16)
    csha = hashlib.sha256(body).hexdigest()
    sig = hmac.new(SECRET, _canonical_v1(KEY_ID, "ocr-watcher", "POST",
                   "/ocr/fixed-asset", ts, nonce, csha), hashlib.sha256).hexdigest()
    return requests.post(URL + "/ocr/fixed-asset", data=body, headers={
        "Content-Type": f"multipart/form-data; boundary={boundary}",
        "X-Sig-Version": "v1", "X-Sig-Key-Id": KEY_ID, "X-Sig-Caller": "ocr-watcher",
        "X-Sig-Timestamp": ts, "X-Sig-Nonce": nonce,
        "X-Sig-Content-SHA256": csha, "X-Sig-Signature": sig}, timeout=120)
```

- filename は現行どおり任意（サーバは `.pdf` 拡張子のみ検査）。`canonical_v1` は
  `hub/service_auth.py:284` と逐語同型（watcher は repo 外スタンドアロンのため自前実装。
  検証はサーバ側が正）。
- 鍵は **GAS と共用しない別 kid の新設を推奨**（例: kid=`pc-watcher-2026-08a`・
  caller=`ocr-watcher`・allowed_paths=`["/ocr/fixed-asset"]` のみ）。事務所 PC への
  secret 配置と GAS の秘匿を分離し、漏えい時に片方だけ revoke できる。
- rollback: 署名ヘッダ組立を環境変数（例 `OCR_WATCHER_SIGNED=0`）で無効化する分岐を
  同時に入れる（受信側は二重受理のため無署名へ戻しても即時受理される）。

---

## §3 強制化の実装要否 — **要（コード変更・小票提案）**

実測どおり、flag ON でも署名ヘッダ皆無なら受理する（`service_auth.py:589-590` の
early return）。**「署名なしを拒否」へ進めるにはコード変更が必要**。以下を小票として
提案する（本票では実装しない）:

### 小票案: RV02-ENFORCE（/scan・/ocr/fixed-asset の非署名遮断・path 単位）

1. **env `SERVICE_AUTH_SIGNED_REQUIRED_PATHS`**（comma 区切り path 集合・既定空=挙動不変）。
   parse/検証は `SERVICE_AUTH_LEGACY_DISABLED_PATHS` の厳格 parser
   （`_parse_legacy_disabled_strict`・同一ファイル内）を共用し、起動時 strict 検証
   （`validate_*_startup`）も同流儀で追加。
2. `authorize_optionally_signed` の「署名ヘッダ皆無」分岐に挿入:
   `effective_signed_path(request.scope)` が集合に在れば
   `_log_ingest_decision(headers, "unsigned_blocked")` → **404**（`legacy_blocked` と同じ
   「存在しないフリ」の既存流儀）。集合に無ければ従来どおり受理。
3. **観測ログ（到達率自動化・任意だが推奨）**: 集合に無い path の非署名受理時に
   `_log_ingest_decision(headers, "unsigned_accepted")` を emit（固定 reason コードのみ・
   顧客情報なし。/scan・/ocr は低頻度イベントでログ量の懸念なし）。これで §4(d) の
   実測が「access log との突合」から「decision ログの直読」に簡素化される。
4. 判定の置き場は **dual-accept flag ON 分岐の内側のまま**とする（flag OFF が全段の
   master rollback として機能し続ける。強制化中に flag を OFF へ戻す事故は
   「遮断も解除される」だけで受理側に倒れる=業務停止しない）。
5. テスト: `test_rv0102_prep_signed_optin.py` へ追補——(i) flag OFF 不変（既存 pin 維持）
   (ii) flag ON・集合外=受理不変 (iii) flag ON・集合内・非署名=404 (iv) flag ON・集合内・
   正署名=通過 (v) 片 path のみ指定時に他 path が不変（/scan と /ocr の独立性）。
   検証器・registry は rv04b と同一物を共用（別実装 PASS を作らない流儀を維持）。

規模感: 実装+テストで小粒（既存 parser・ログ・raise の再利用のみ・新規 sink なし）。

---

## §4 点火手順書（ブロックA・§8 方式）

前提: 全段階で**受信側は二重受理**（署名あり=検証・なし=受理）を維持したまま送信側を
先に署名化し、全 caller の署名到達を実測してから遮断する。**GAS 全ライン道連れ停止は
構造的に起きない**——(i) 既存 5 lane と opt-in 2 入口は registry の allowed_paths 追加
（加算のみ）以外に共有設定を変えない、(ii) `/scan`（GAS）と `/ocr/fixed-asset`
（事務所 PC watcher）は**呼び出し元が別マシン・別コード**で独立に切替・独立に rollback
できる、(iii) 強制化（e）も `SERVICE_AUTH_SIGNED_REQUIRED_PATHS` の path 単位で片方ずつ
進められる。

### (a) 事前検査（[人]+PC-A・read-only・不足があれば中止）

1. flag/env 実測の再確認（`!` railway variables・secret 値非表示）:
   `SERVICE_AUTH_DUAL_ACCEPT_ENABLED=1`・registry parse 可・
   `SERVICE_AUTH_LEGACY_DISABLED_PATHS` 現状維持——§2.2 と相違があれば本書を改版。
2. `signature_nonce` 表の存在（ブロックA §8.1(a) 検査 2 の SELECT を流用。
   既に署名 2 lane が本番稼働中のため通常は充足済み）。
3. 全 suite green（`python -m pytest -q --ignore=test_triage_classification.py`）。
4. baseline 採取: railway logs から `POST /scan`・`POST /ocr/fixed-asset` の直近件数・
   時刻を記録（uvicorn access log 形式 `"POST /scan HTTP/1.1" <status>` を実測確認済み）。
   **旧デプロイのログは世代交代で消える**——採取はデプロイをまたぐ前に行う。
5. H-1/H-2（§1.2）の実物確認が完了していること。

### (b) flag ON（二重受理・挙動不変）— **完了済み（2026-08-16 実測）**

`SERVICE_AUTH_DUAL_ACCEPT_ENABLED=1` は本番投入済み（S5 cutover 以降）。追加作業なし。
(a)-1 の再確認のみ行う。

### (c) 送信側の署名付与（[人]・ライン別に段階可能）

- **c-0（必須先行・[人] env）**: registry 更新。
  - 既存 kid `gas-ingest-2026-07a` の `allowed_paths` に `/scan` を**追加**
    （既存 5 lane への影響なし・加算のみ）。
  - watcher 用に新 kid を**新設**（例 kid=`pc-watcher-2026-08a`・caller=`ocr-watcher`・
    `allowed_paths=["/ocr/fixed-asset"]`・secret は大野がローカル生成
    `python -c "import secrets; print(secrets.token_hex(32))"`）。
  - 投入は `!` railway variables（値の生成・投入とも PC-A は行わない）。
  - 検証: 投入直後に (a)-1 の parse 確認を再実行（壊れ registry は既存署名 2 lane を
    503 に落とすため、**投入は業務時間外**・投入直後に既存 lane のスモーク 1 件）。
- **c-1（/scan・[人]=GAS エディタ）**: §2.4 の `scanFetch_` を追加し fetch 1 箇所を置換、
  `SIGNED_SCAN=true`。切替直後に相談カード/戸籍謄本/通帳の 3 書類種を 1 件ずつ実投入し、
  kintone 登録+decision ログ `reason=ok` を確認（(d) の初回実測を兼ねる）。
  rollback=`SIGNED_SCAN=false`（受信側は無署名でも受理継続中）。
- **c-2（/ocr/fixed-asset・[人]=事務所 PC）**: §2.5 を watcher へ反映し env で ON。
  OCR_inbox へテスト PDF 1 件投入→kintone 更新+LINE 通知+decision ログ確認。
  rollback=watcher の署名 OFF。
- c-1 と c-2 は**順不同・独立**。片方の不具合はもう片方に波及しない。

### (d) 署名到達率の実測（全受信が署名付きになったことの確認）

- 方法（現行コードのまま・追加実装なしで可能）: 同一時間窓の railway logs で
  1. `"POST /scan HTTP/1.1"` の access log 行数 = 総到達数
  2. `service-auth ingest decision ... reason=ok` 行数 = 署名到達数（key_id/caller/
     reason のみの固定コードログ・§2.1 実測どおり非署名受理はログを出さない）
  3. **差分 0 が「全受信=署名付き」**。差分>0 なら非署名 caller が残存（H-3 の検証を兼ねる）。
  `/ocr/fixed-asset` も同様。RV02-ENFORCE の観測ログ（§3-3）が入れば
  `unsigned_accepted` の直読で代替できる。
- 採取の罠: ログは**デプロイ世代交代で消える**——観測期間中にデプロイがあるたび、
  デプロイ前に窓を採取して積算する。
- 判定基準（両入口それぞれ）: 切替後、**実業務由来の到達が各書類種で 1 件以上を含む
  連続 5 営業日で 非署名到達=0**（/scan・/ocr はイベント駆動で低頻度のため、
  「日数」だけでなく「実件数>0」を必須条件にする）。
- この期間はそのまま**二重受理期間**（旧経路へいつでも戻せる状態を維持する期間）。

### (e) 強制化（RV02-ENFORCE merge+デプロイ後・[人] env・path 単位で段階可）

1. 前提: RV02-ENFORCE がレビュー→merge→デプロイ済み（env 未設定なら挙動不変のため
   デプロイ自体は (d) と並行可）。
2. `SERVICE_AUTH_SIGNED_REQUIRED_PATHS=/ocr/fixed-asset` を投入（caller が 1 台で
   検証しやすい watcher 側から）→ 直後スモーク: 署名付き 1 件=通過・
   （可能なら）無署名 1 件=404 を実測。
3. 24h 観測後、`SERVICE_AUTH_SIGNED_REQUIRED_PATHS=/ocr/fixed-asset,/scan` へ拡大 →
   3 書類種スモーク。
4. これで RV-02 の「外部到達性を止める」（無認証受理の廃止）が閉じる。
   監査 §5 の残論点（endpoint 自体の disable・safe-ingest への統合=RV-03 側）は別票。

### (f) 観測・rollback（各段階で戻せること）

| 段階 | 観測 | rollback（すべて env/定数 1 箇所・コード変更なし） |
|---|---|---|
| c-0 registry | 既存 2 署名 lane のスモーク・decision ログに 503/`registry_config_error` が出ないこと | registry を前値へ戻す（`!` railway variables） |
| c-1 /scan 署名化 | kintone 登録成否・decision `reason=ok`・GAS 実行ログ（4xx/5xx は fetch 例外で実行停止=現行既知挙動のまま） | `SIGNED_SCAN=false` |
| c-2 watcher 署名化 | kintone 更新・LINE 通知・エラーサブフォルダ | watcher の署名 OFF |
| e 強制化 | access log の 404 急増有無・decision `unsigned_blocked`・業務の書類登録が滞らないこと | `SERVICE_AUTH_SIGNED_REQUIRED_PATHS` から当該 path を除去（=二重受理へ即復帰）。最終手段: `SERVICE_AUTH_DUAL_ACCEPT_ENABLED=0`（ただし**既存署名 5 lane も旧 token へ戻る**ため単独判断で使わない） |
| 常時 | `X-Sig` 鍵 `gas-ingest-2026-07a` の **expires_at=2026-10-15**。失効前 rotation（RV-04 の rotation 手順を流用）を強制化完了より先に期日管理へ載せる | — |

---

## §5 暫定緩和策の検討（強制化完了までの即効手段・実測ベース)

- **サーバ側でコード変更なしに非署名を遮断する手段は無い**（実測: 遮断分岐が存在しない
  =§2.1。`SERVICE_AUTH_LEGACY_DISABLED_PATHS` は dual-accept の token lane 専用で
  opt-in 入口には効かない=`service_auth.py:552-557` は `authorize_ingest` 側のみ）。
  Railway 側の path 単位 ACL は repo/CLI の範囲に手段が見当たらない。
- **今日からできる実効策は「検知」**: §4(d) の access log 突合は署名化前でも成立する
  （現状は全到達が非署名なので、`POST /scan`・`POST /ocr/fixed-asset` の access log 行数
  そのもの=非署名到達数。業務時刻と突合し、**業務由来でない到達が観測されたら**
  司令塔へ即報告→(e) の前倒し裁定）。ログ世代交代の罠があるため、観測は「デプロイ前
  採取」をルール化する。
- **期間短縮こそが実質の緩和**: c-0（registry 追加）と RV02-ENFORCE の小票を先行着手
  すれば、(c)〜(e) のクリティカルパスは [人] の GAS/watcher 反映と観測日数のみになる。
- 推測ベースの安心材料（「URL が知られていない」等）は**本書に採用しない**。

---

## 付録: 本書の実測根拠一覧

- コード: `hub/service_auth.py:462-601`（flag/ゲート/遮断分岐の全実物）・
  `main.py:16,1187,1402,1472-1474`・`gas/rv04c_signing.js`（SIGNED_LANES/署名組立）・
  `legacy/gas/コード.js:29`（/scan caller 写し）
- 本番 env: railway variables 実測 2026-08-16（§2.2・secret 非表示）
- ログ形式: railway logs 実測 2026-08-16（uvicorn access log 行の実在確認）
- 呼び出し元台帳: `docs/evidence/ENDPOINT_TRUST_BOUNDARY_INVENTORY.md` #5/#6・
  `docs/current-state-report.md` §1.2/§2.2
- 監査: `docs/plan/2026-08-15_audit.md` §1.0/§5
