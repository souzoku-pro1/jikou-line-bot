# 03. 共通コンポーネント：`hub/` 共通ライブラリ

宛名ラベル生成・docx 生成・承認キュー連携・LINE 警報は全モジュール共用とする。
既存コードに散在する同等実装を `hub/` に集約し、5モジュールはこれだけを使う。

## 1. 方針

1. **「動いているものを動いたまま」移設する**。抽出リファクタは外部挙動を一切変えない
   （エンドポイントの URL・レスポンス・kintone への書き込み内容が不変であることをテストで担保）
2. 移設元モジュール（`document_webhook.py` 等）には **re-export を残す**
   （`from hub.docx_builder import fill_template` を旧名でも import 可能に）。
   既存テスト・既存コードの import を壊さない
3. チャネル固有の知識（座標表・CSV 列・プロンプト）は `hub/` に置かない。
   `channels/` 配下の各アダプタが持つ

## 2. ディレクトリ構成

```
（リポジトリルート）
├─ main.py                    # ルータ登録のみ追加。既存エンドポイントは不変
├─ config.py                  # UNIT_CONFIG / CHANNEL 定義 / スキーマ想定値を追加
├─ claude_gateway.py          # 既存のまま（全モジュールがこれ経由で Claude を呼ぶ）
│
├─ hub/                       # ★共通ライブラリ（チャネル非依存・ユニット非依存）
│   ├─ __init__.py
│   ├─ kintone.py             # kintone REST API 共通クライアント
│   ├─ webhook_auth.py        # 合言葉検証・recordId 抽出・トリガー再判定
│   ├─ approval.py            # 状態機械・冪等ガード・承認依頼通知
│   ├─ dispatch.py            # /hub/dispatch エンドポイント＋チャネルレジストリ
│   ├─ docx_builder.py        # fill_template / to_wareki / テンプレート解決
│   ├─ address_label.py       # reportlab 座標印字エンジン（ラベル・重ね打ち）
│   ├─ notify.py              # 管理者警報・弁護士承認依頼の LINE Push
│   └─ scheduler.py           # 日次/定期ジョブのレジストリ
│
├─ channels/                  # ★チャネルアダプタ（1モジュール = 1ファイル）
│   ├─ __init__.py            # CHANNEL_REGISTRY の組み立て
│   ├─ base.py                # ChannelAdapter インターフェース定義
│   ├─ shokumu_seikyu.py      # M1（04 参照）
│   ├─ enaishomei.py          # M2（05 参照）
│   ├─ fax.py                 # M3（06 参照）
│   ├─ soufu_annai.py         # M4（07 参照）
│   └─ scan_intake.py         # M5 受領アダプタ（08 参照）
│
└─ docx_templates/
    ├─ 送付状_委任契約書.docx   # 既存（互換のため据え置き）
    └─ jikou/                  # ユニット別テンプレート（規約ベース配置）
        ├─ 送付案内.docx
        ├─ 内容証明_時効援用通知.docx
        └─ ...
```

## 3. `hub/kintone.py` — kintone 共通クライアント

現在 kintone I/O は main.py / chat_responder.py / document_webhook.py /
cloudsign_webhook.py / daily_healthcheck.py に**5重に**実装されている。これを集約する。

```python
@dataclass(frozen=True)
class KintoneApp:
    """アプリへの接続情報。環境変数名を保持し、値はリクエスト時に解決する"""
    label: str          # ログ・警報表示用（例: "App 30 発送管理"）
    app_id_env: str     # 例: "APP_SHIPPING"
    token_env: str      # 例: "TOKEN_SHIPPING"

# 提供する非同期関数（すべて httpx.AsyncClient 使用・既存実装の一般化）
async def get_record(app, record_id) -> dict
async def search_records(app, query, fields=None) -> list[dict]
async def create_record(app, fields: dict) -> str            # record_id を返す
async def create_records(app, records: list[dict]) -> list[str]  # 100件チャンク一括
async def update_record(app, record_id, fields: dict, revision=None) -> None
async def upload_file(app, filename, content: bytes, mime) -> str  # fileKey
async def download_file(app, file_key) -> bytes
async def get_form_fields(app) -> dict                        # 死活監視用
```

設計上の決めごと:

- `fields` は `{"コード": 値}` のフラット dict を受け、`{"value": ...}` への包みは内部で行う
  （既存 `post_to_kintone` と同じ流儀）
- **書き込みはリトライしない**。二重実行防止は上位（`実行済み` フラグ + revision）で担保。
  読み込み（GET）のみ 1 回リトライ
- `update_record(revision=...)` は kintone の楽観ロックを透過させる
  （revision 不一致 = 他プロセスが先に更新 → `KintoneConflict` 例外。冪等ガードで使用）
- 失敗は `KintoneError(status, code, message)` に正規化。呼び出し元が
  「エラー状態への遷移 + 警報」を行う（クライアント自身は警報を出さない）

## 4. `hub/webhook_auth.py` — Webhook 受信の共通型

App 29 承認 Webhook・document_webhook・cloudsign_webhook で確立した
「合言葉 → recordId 抽出 → ボディで高速判定 → 最新レコード再取得で再判定」を関数化する。

```python
def verify_token(supplied: str, expected_env: str) -> bool
    # hmac.compare_digest。不一致は呼び出し元が 404 を返す（存在しないフリ）

def extract_record_id(body: dict) -> str | None
    # body["record"]["$id"]["value"] → 無ければ body["recordId"]（既存2実装と同一）

async def refetch_and_check(app, record_id, expects: dict[str, str]) -> dict | None
    # 最新レコードを取得し、expects（例 {"発送ステータス": "承認済", "実行済み": "no"}）
    # を満たさなければ None（= skip）。満たせばレコードを返す
```

## 5. `hub/approval.py` + `hub/dispatch.py` — 状態機械・承認・ディスパッチ

### 5.1 状態遷移表（コードで強制する）

| 現在 | 遷移先 | 誰が/何が |
|---|---|---|
| 下書き | 承認待ち | Railway（prepare 成功時に自動） |
| 下書き | エラー | Railway（prepare 失敗） |
| 承認待ち | 承認済 / 却下 | **弁護士のみ**（kintone 上の手動変更） |
| 承認済 | 発送処理中 | Railway（dispatch 開始・冪等ガード通過時） |
| 発送処理中 | 発送済 / エラー | Railway（dispatch 結果） |
| 発送済 | 返送待ち | Railway（返送想定ありの場合に自動） |
| 発送済 / 返送待ち | 完了 | Railway（M5 消込・送達確認） |
| エラー / 要確認 | 下書き / 完了 | 人（対応後に手動で戻す） |

- 表にない遷移を検知したら（人が手でステータスを飛ばした等）: 処理は行わず LINE 警報
  「不正な状態遷移: 発送済→承認待ち record=123」。**壊れた状態で発信しないことを最優先**
- `承認待ち → 承認済` は kintone 上の人の操作のみ。**Railway 側に承認済へ遷移させる
  コードパスを作らない**（制約「弁護士名義の対外発信は必ず承認を挟む」の実装保証）

### 5.2 冪等ガード（claim パターン）

```
async def claim_execution(record) -> bool:
    # 実行済み=no のレコードに対し revision 指定で 実行済み=yes に更新。
    # KintoneConflict（=他プロセスが先に claim）なら False を返して skip。
    # App 29 の「送信済み」パターン + revision 楽観ロックで二重配信・再送・
    # 多重デプロイ時の重複実行を1回に抑える。
```

### 5.3 `/hub/dispatch` エンドポイント（App 30 Webhook の受け口・1本のみ）

```
POST /hub/dispatch?token=<HUB_WEBHOOK_TOKEN>
  1. verify_token → NG なら 404
  2. extract_record_id → refetch（最新レコード）
  3. 発送ステータスで分岐:
     - 下書き:   adapter.prepare(record)  → 成果物添付 → 承認待ちへ + 弁護士LINE通知
     - 承認済:   claim_execution → adapter.dispatch(record) → 発送済/エラーへ
     - 要確認:   （M5）人が種別・案件を補記して保存 → adapter.reprocess(record)
     - その他:   skip（200 を返す。kintone Webhook はリトライしないため常に 200）
  4. adapter が例外を投げたら: 発送ステータス=エラー + エラー詳細書き込み + LINE 警報
```

- 重い処理（OCR・外部 API）は既存 `/webhook` と同じく **BackgroundTasks** で実行し即 200 を返す

### 5.4 `channels/base.py` — アダプタインターフェース

```python
class ChannelAdapter(Protocol):
    channel_name: str                      # App 30 チャネル欄の値と一致
    needs_return: bool                     # 発送済後に返送待ちへ遷移するか（既定）

    async def prepare(self, record: dict) -> PrepareResult:
        """下書き→承認待ちの間に成果物（docx/PDF/CSV）を生成して添付する"""

    async def dispatch(self, record: dict) -> DispatchResult:
        """承認済後の発送実行。外部APIなしのチャネルは印刷指示のみで
        DispatchResult(manual_mailing=True) を返す"""
```

- アダプタは**発送管理レコードだけ**を入力とする（ユニット非依存の実装保証。
  案件アプリを直接読む必要がある場合も `案件アプリID`+`案件レコードID` 経由）
- アダプタから LINE を直接呼ばない。通知は `hub/notify.py` 経由のみ

## 6. `hub/docx_builder.py` — docx 生成

- `fill_template(template_path, data) -> bytes` / `to_wareki(date) -> str` を
  `document_webhook.py` から**そのまま移設**（実装変更なし・re-export 維持）
- 追加: `resolve_template(unit, doc_type) -> Path`
  — `docx_templates/<unit>/<doc_type>.docx` の規約解決。無ければ `TemplateNotFound`
- 追加: `validate_template(path, required_keys) -> list[str]`
  — テンプレート内に必要プレースホルダ（`{{...}}`）が揃っているか検査。
  日次死活監視に登録し、**テンプレートを人が編集して差込キーを消した事故**を検知する

## 7. `hub/address_label.py` — 宛名ラベル・重ね打ち PDF（reportlab 新規採用）

座標指定の印字が必要なため PDF を採用（python-docx では座標印字不可）。
このモジュールは**座標印字エンジンのみ**を持ち、具体的な座標表はチャネル側が持つ。

```python
@dataclass
class TextAt:
    x_mm: float; y_mm: float; text: str
    font_size: float = 10.5; max_width_mm: float | None = None  # はみ出し時は縮小

def render_overlay(page_size, items: list[TextAt], *, grid=False) -> bytes
    # 白紙PDFに座標印字。grid=True で 5mm 方眼＋座標値を重ねる（キャリブレーション用）

def render_letterpack_label(to_addr, from_addr) -> bytes
    # レターパックの「お届け先/ご依頼主」欄への重ね打ち（内部で render_overlay）

def render_label_sheet(addresses: list, layout="A4_2x6") -> bytes
    # ラベルシール用の面付け印字（送付案内 M4 の宛名ラベル）
```

- 日本語フォント: IPAexゴシック等を `assets/fonts/` に同梱し reportlab に登録
  （Railway コンテナにインストール済みフォントを期待しない）
- **キャリブレーション**: プリンタ個体差を環境変数 `PRINT_OFFSET_X_MM` / `PRINT_OFFSET_Y_MM`
  で全体オフセット。`grid=True` の試し刷りで合わせる（運用手順は 04 §3.3）

## 8. `hub/notify.py` — LINE 通知の一本化

- `notify_admin_line(text, throttle_key)` を `claude_gateway.py` から移設
  （claude_gateway には re-export を残す。スロットル実装・警報文言は不変）
- 追加: `notify_attorney_approval(record)` — 発送管理の承認依頼通知。
  既存 App 29 の「【承認依頼】」と同型:

```
【承認依頼】発送
件名: 受任通知FAX（アコム）
チャネル: FAX / 顧客: 山田太郎
発送管理レコードNo: 45
kintone で成果物を確認し、発送ステータスを「承認済」に変更してください。
```

- LINE Push の実体も1実装に集約（現在 main.py / chat_responder.py /
  cloudsign_webhook.py / claude_gateway.py に4重実装）

## 9. `hub/scheduler.py` — 定期ジョブレジストリ

`daily_healthcheck.py` のスケジューラループを一般化する。

```python
def register_daily(name: str, hour_jst: int, coro_factory) -> None
def register_interval(name: str, minutes: int, coro_factory) -> None
def start_all() -> None   # main.py の startup で1回呼ぶ
```

- 各ジョブは try/except で隔離（1ジョブの失敗が他ジョブを止めない・既存ループと同じ）
- ジョブ自身が冪等であること（再デプロイの重なりで同日2回走っても安全）を実装規約とする
- 登録されるジョブ（最終形）:

| ジョブ | 周期 | 内容 | 由来 |
|---|---|---|---|
| daily_healthcheck | 毎日 7:00 | モデル・kintone スキーマ検証（既存） + 新設アプリ + テンプレート検査 + App30/32 同期検査 | 既存を移行 |
| return_deadline_check | 毎日 8:00 | `返送待ち` の `返送期限` 超過 → LINE 警報 | M1 |
| fax_status_poll | 15分 | `発送処理中` の FAX の送達結果取得・書き戻し | M3 |

## 10. `config.py` 拡張 — UNIT_CONFIG

```python
UNIT_CONFIG = {
    "時効援用": {
        "case_app_env": ("KINTONE_APP_ID", "KINTONE_API_TOKEN"),
        "customer_name_field": "顧客名",       # 案件アプリの氏名フィールドコード
        "customer_addr_field": "住所",
        "channels": ["職務上請求", "e内容証明", "FAX", "送付案内", "スキャン受領"],
        "template_dir": "jikou",
        "return_deadline_days": 21,
    },
    # 相続放棄 / 相続一般 / 補助金 はエントリ追加のみでハブに乗る（01 §5.3）
}
```

## 11. テスト方針（既存流儀の踏襲）

- unittest + `unittest.mock`（httpx / requests をモック）。`conftest.py` の既存設定を利用
- 実 API を叩くテストは `railway run` 前提で `skipUnless(os.environ.get(...))` ガード
- `hub/` の各モジュールに対応する `test_hub_*.py` を置く。抽出リファクタのタスクは
  「移設前後で既存テストが無変更で PASS」を完了条件に含める（09 参照）
- 状態機械は**遷移表を data として持ち**、全禁止遷移を総当たりで検査するテストを書く

## 12. `hub/durable_inbound.py` — durable inbound の確定仕様（2026-07-21 昇格）

> 転記元: `docs/design-drafts/DRAFT_P2_DURABLE_IGNITION.md`（fix1〜fix4・R-P2-DURABLE-PREP-5
> PASS・PR #154）。DRAFT は経緯込みの正本として残し、ここには**確定部分のみ**を仕様として置く。

### 12.1 点火ゲート（INBOUND_EVENT_DURABLE_ENABLED）

- 点火の**唯一の前提**＝LINE 滞留監視（received／processing の両 state・durable flag 配下・
  `STRIPE_EVENT_JOURNAL_ENABLED` 非依存・閾値超過で LINE 警報）の**検査関数と
  daily_healthcheck 結線の両方が merge 済み**であること。分票時は両票完了まで点火不可。
- K4（LINE 再配送設定確認）は**補助**であり点火条件ではない（K4 は非 2xx への再配送のため、
  200 ACK 後の BackgroundTask crash による滞留を回収できない）。
- rollback は env OFF 1 本で即時に現行挙動と byte 同一（M-06: flag OFF は import 不発）。

### 12.2 状態語彙（inbound_event.state・LINE Phase A）

- `done` = 処理が正常終端した記録。**照合源による根拠がある場合のみ**手動遷移可。
- `failed_exhausted` = **再試行を行わないことが確定した打切り**。
  自動（`attempts >= max`・retry_exhausted 系）と手動（再配送終了済みで再処理見込みなしの
  [人] 判断・runbook 2026-07-15_RV-05-13-fix5 §4.4(b)）の両方を含む。
  手動遷移時は `last_error` を固定分類（例: `manual_closed`）とし自動上限と識別する（運用）。
- **「処理済み」の行を `failed_exhausted` に入れることは禁止**（done と混同しない）。
- 管理終端のための新 state は**作らない**（migration 回避・裁定）。

### 12.3 照合源（received 行の閉鎖判断）

- Phase A は **raw payload を保存しない**（保存列は payload_hash 等のみ・PII/本文非搭載）。
- 照合源は**実在するもののみ**: `external_event_id`（LINE webhookEventId）・既存の構造化ログ・
  LINE 側の配信記録等。**照合源が無い行は「確認不能として残置」が唯一の扱い**
  （根拠なき done 更新＝未処理の処理済み偽装は禁止）。
