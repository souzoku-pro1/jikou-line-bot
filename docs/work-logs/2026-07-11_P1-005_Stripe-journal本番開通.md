# 作業記録 2026-07-11: P1-005系 — Stripe InboundEvent journal 本番開通

- Phase/Gate: Phase 1（基盤hardening）・P1-005-survey〜005f
- 記録日: 2026-07-11 ／ 実施: Claude Code（PC-A）＋大野（マージ・env投入）
- 出典: 各COMPLETION_REPORT・Codexレビュー・git/PR/Railway実出力（推測補完なし）

## 1. 経緯（P1-005系の全体）

| 票 | 内容 |
|---|---|
| survey | 全inbound入口14本の棚卸し（ACK方式/dedup/喪失リスク）・process memory全列挙（**ヒアリング会話state 4つの発見**含む）・InboundEvent/IngestionReceipt設計案・着手順の推奨（Stripe→LINE指示Bot→顧客Bot→ingest→CloudSign） |
| 005a | InboundEventテーブル（第2migration）＋Stripe dedup。D7（DB不達=5xx・fallback禁止）/D8（PII非保存）/D9（入口の関所のみ）/D10（flag既定OFF）。shutdown hook=await adispose_all() |
| 005b | D11（kintone非2xx検知・M02）/D12（claimed_at=第3migration＋stale再claim・RCF-M06）/D13（未処理の闇損失＞まれな二重処理の裁定） |
| 005c | D14（実行中重複=503でStripe再送維持・H01）/D15（再処理経路のApp 21照合reconciliation・H02）/D16（journal行消失=fail closed・M01）/D17（ログはPKのみ） |
| 005d | 日次死活監視に**監視項目E**（journal 24h滞留・業務LINE警報・件数とPKのみ）＋復旧runbook（docs/runbooks/stripe-journal-recovery.md） |
| 005e | PR #103（3 commits）マージ（`885eb43`）→第3migration本番適用（current=`f8ef81de70a5 (head)`・claimed_at列確認済み） |
| 005f | `STRIPE_EVENT_JOURNAL_ENABLED=1` 本番投入（大野・2026-07-11深夜）→**本番開通** |

## 2. Codexレビュー3巡の裁定記録

| rev | 対象 | 判定 |
|---|---|---|
| R-P1-005a | `21b5ea3` | PASS_WITH_FINDINGS（**flag ON保留**・M01/M02を開通前提に） |
| R-P1-005b | `99cc5e0` | CHANGES_REQUIRED（**開通NO判定**・H01: 200で飲むと永久未処理経路/H02: 無照合再POSTの二重起票） |
| R-P1-005c | `ec94218` | **PASS_WITH_FINDINGS・開通判定YES（条件付き）** — 4条件=①H01解消（503+stale再claim） ②H02解消（reconciliation） ③M01解消（fail closed） ④滞留監視+runbook（005dで充足）→**全充足** |

（005dは低リスク裁定でCodex省略・司令塔検収のみ）

## 3. 本番状態（2026-07-11深夜時点）

- main = `885eb43`（PR #103マージ）・Railwayデプロイ済み・サービスOnline
- migrations 3本適用済み: baseline → inbound_event → claimed_at（head=`f8ef81de70a5`）
- **`STRIPE_EVENT_JOURNAL_ENABLED=1` 投入済み（本番開通）**
- 監視項目E（journal滞留）は翌朝7:00 JSTの日次死活監視から有効
- 15分窓: env `STALE_PROCESSING_MINUTES`（既定15・未投入=既定）

## 4. 本番挙動変更の記録（重要）

**D11はflagと無関係に有効**: kintone非2xx時に /webhook/stripe が500を返し
Stripeの自動再送が始まる（従来は「黙って成功扱い」で決済レコードが闇損失）。
kintone障害中はStripe Dashboardにwebhook失敗が積まれるが、これは仕様
（復旧後の再送で自己回復・journalが二重起票を防ぐ）。

## 5. テスト推移

**1,102 → 1,113（005a）→ 1,126（005b）→ 1,135（005c）→ 1,144（005d）**・
全区間 FAIL 0・skip 0・既存テストの削除/緩和なし（設計変更に伴う期待値更新は
理由コメント付きで3件のみ）。

## 6. DEFER台帳の更新

- **RCF-M06（stale processing）: 解消済み**（D12再claim＋D14の503でStripe再送が
  回収を起動する構造・E2Eテスト固定）
- RCF-L03（async dispose例外時のbest-effort化）: 未消化のまま維持（P1-004記録どおり。
  dispose系はD6実装で運用上の支障なし）
- **RP1005C-L01（新規追記）**: App 21のStripe決済ID一意性はkintone側で強制されて
  いないため、reconciliation照合(GET)とPOSTの間の狭い窓で理論上二重起票が残る。
  D13裁定（検索で人が検知可能）の範囲内としてPhase 1では受容。恒久策は
  kintone側の重複禁止設定 or Outbox worker（Phase 6）で検討
- 既存継続: RCF-M01（**CloudSign**成功側idempotency・Stripe側は本系で解消）/
  RCF-M02（LINE警報非2xx検知）/RCF-L01（IDログ台帳化）/RCF-L02（path secret HMAC化）/
  offline URL分離/lock・SBOM

## 7. 次の一手（次セッション向け・surveyの推奨着手順の続き）

1. **LINE指示Bot**: webhookEventId取得（現状は捨てている・2行）＋journal化。
   **BackgroundTasks喪失対策の型**をここで確立（利用者が身内で再操作容易）
2. 顧客Bot＋ConversationSession（ヒアリング会話state 4つの永続化・App21重複作成穴）
3. ingest系 IngestionReceipt（挙動変更なし・観測性先行）
4. CloudSign成功側idempotency（RCF-M01消化）

## 8. 実機確認の宿題[人]

- [ ] 次回実決済: App 21に**単一**起票＋journal行が`done`（psql or 管理画面）
- [ ] 翌朝7:00: 日次死活監視LINEが正常受信（滞留0なら従来どおり静か・
      異常時のみ警報が来る）
- [ ] BLOCKED残: watcher（/ocr/fixed-asset）の再送・リネーム挙動（事務所PC）／
      kintone webhook payloadのevent id有無（実サンプル1件）
