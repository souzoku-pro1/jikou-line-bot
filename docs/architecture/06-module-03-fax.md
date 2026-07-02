# 06. モジュール3: FAX 自動送信

クラウド FAX API による受任通知の自動発射と、送達結果の App 30 書き戻し。
5モジュール中唯一「承認後に完全自動で外へ出る」チャネル。

## 0. プロバイダ選定（T6-1 スパイク）

要件:

| # | 要件 | 理由 |
|---|---|---|
| 1 | REST API で PDF を指定番号へ送信できる | Railway からの自動送信 |
| 2 | 送達結果を API で取得できる（Webhook があればなお良い） | 書き戻し・リトライ判断 |
| 3 | 日本国内の固定電話番号帯（0AB〜J）へ送信可能 | 業者・官公庁宛て |
| 4 | 従量課金・月額が事務所規模に見合う | |

**T6-1 選定スパイク実施済み（2026-07-02）**: 比較表・選定理由は
[06a-fax-provider.md](06a-fax-provider.md) 参照。推奨は **InterFAX**
（REST + ジョブID ポーリングが本設計の FaxProvider に1対1で写像・テスト用FAX番号あり・
月額固定費最小）、次点 NetFax（月170枚超で再評価）。
**設計はプロバイダ非依存**とし、差し替え可能にする:

```python
# channels/fax.py 内
class FaxProvider(Protocol):
    async def send(self, to_number: str, pdf: bytes, *, subject: str) -> str   # job_id
    async def get_status(self, job_id: str) -> FaxJobStatus   # QUEUED/SENDING/DELIVERED/FAILED

# 環境変数: FAX_PROVIDER=interfax 等、FAX_API_KEY / FAX_API_SECRET / FAX_FROM_NUMBER
```

- プロバイダ契約はリードタイムがあるため、**T6-1 は他フェーズと並行して早期に着手**する
  （09 §2 の注記参照）

## 1. 受任通知の自動発射フロー

```
[起票トリガー] CloudSign 締結 Webhook（既存）に増設:
   App 21 status=受任 への更新と同時に、App 30 へ「FAX 受任通知」下書きを自動起票
   ├─ 宛先FAX番号: 業者マスタを 問い合わせ業者名 で検索して取得
   └─ FAX番号なし/業者未登録 → 要確認 遷移 + LINE 警報
      （警報文言に「FAX番号を登録して再保存、または送付案内チャネル（郵送）へ振替」を明記）
[prepare]（Railway 自動）
   1. 受任通知 PDF を生成（§2）
   2. FAX番号の形式検証（§3）
   3. 成果物に添付 → 承認待ち + 弁護士 LINE 通知
[承認] 弁護士が PDF・宛先番号を確認 → 承認済
[dispatch]（Railway 自動・冪等ガード通過後）
   provider.send() → job_id を チャネル固有データ に保存 → 発送処理中
[送達確認] scheduler.register_interval("fax_status_poll", 15分):
   発送処理中 かつ チャネル=FAX の全レコードについて provider.get_status(job_id)
   ├─ DELIVERED → 送達結果=送達済・発送日時 記録 → 完了（返送想定なし）
   ├─ FAILED    → リトライ回数+1 して再送（最大3回・話中/一時エラーのみ）
   │              超過 → 送達結果=不達・エラー遷移 + LINE 警報
   └─ QUEUED/SENDING → 次回ポーリングへ（24時間経過しても確定しない → 警報）
```

- **「自動発射」でも承認は省略しない**（制約 5.2）。締結から FAX 送信までの人の操作は
  弁護士の「承認済に変更」1回のみ

## 2. 受任通知 PDF の生成方式

- **reportlab で直接 PDF 生成**する（docx を経由しない）
  - 理由: Railway 上での docx→PDF 変換は LibreOffice 常駐が必要で重く、障害点が増える。
    受任通知は定型1〜2枚のレイアウトで、`hub/address_label` と同じ
    reportlab 基盤で実装できる
  - 文面テンプレートは Python 側の構造化テキスト（config でユニット別に差し替え可能）
  - 弁護士が文面を変えたいときはコード変更になる点はトレードオフとして許容
    （変更頻度が低い定型文書のため。頻繁に変わる文書は M4 の docx 系を使う）
- 1ページ目に FAX 送信票（宛先・発信元・枚数・`本文_特記事項`）を自動付与

## 3. バリデーション

| 検査 | NG 時 |
|---|---|
| FAX 番号形式（0 始まり 10〜11 桁・ハイフン除去後） | prepare でエラー遷移 + 警報 |
| 番号が業者マスタの値と一致（手で書き換えられた場合も許容するが、マスタ不一致は承認依頼文言に明記） | 警報ではなく承認者への注意喚起 |
| PDF 生成失敗（フォント欠損等） | エラー遷移 + 警報 |

## 4. チャネル固有データ（JSON スキーマ）

```json
{
  "provider": "interfax",
  "job_id": "123456789",
  "attempts": [
    {"at": "2026-07-02T10:00:00+09:00", "job_id": "123456789", "result": "FAILED", "detail": "busy"}
  ],
  "業者マスタレコードID": "12"
}
```

## 5. 異常系一覧

| 事象 | 検知 | 自動対応 | 警報 |
|---|---|---|---|
| 業者マスタに FAX 番号なし | 起票時 | 要確認遷移 | ○（郵送振替の選択肢を提示） |
| 話中・一時送信失敗 | ポーリング | 最大3回自動再送 | 超過時のみ ○ |
| 恒久エラー（番号不存在等） | ポーリング | 再送せずエラー遷移 | ○ |
| プロバイダ API ダウン | send/ポーリング例外 | 状態維持・次回ポーリングで回復 | 連続失敗時 ○（スロットル付き） |
| 24時間ステータス未確定 | ポーリング | 状態維持 | ○ |

## 6. テスト観点

- FaxProvider をモックした dispatch の冪等性（二重 Webhook で send が1回）
- ポーリングの状態遷移全分岐（DELIVERED / FAILED×リトライ / 超過 / 未確定24h）
- 番号バリデーション（形式・ハイフン混在・国際番号拒否）
- CloudSign 締結 → FAX 下書き自動起票（`test_cloudsign_webhook.py` 拡張）
- 実プロバイダ疎通は `railway run` + `skipUnless` ガード（送信先は自事務所 FAX）
