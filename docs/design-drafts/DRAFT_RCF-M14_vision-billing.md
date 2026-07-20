# DRAFT: RCF-M14 — Vision API billing 403（司令塔台帳 転記用）

- 状態: **起票 DRAFT**（PC-A 整形・正本は司令塔 RCF 台帳）。台帳へ転記のうえ裁定。
- 分類: RCF-M（恒久策候補）。検知: 2026-07-18 S4/S5 実機移行中。

## 1. 検知経緯

- S4/S5 の sortation 実機移行中、Google Vision API（`files:annotate`）呼び出しが **billing 403**
  を返す状態を検知（billing 未有効化 or 課金アカウント不整合）。

## 2. 影響

- **仕分け判定（sortation_ingest）が全件「不明（doc_type=不明）」フォールバック**へ縮退する。
  設計上、OCR/判定不能は**安全側で ask（人手照会）へ縮退**する（沈黙処理にしない）ため、
  **顧客への誤送・沈黙喪失は発生しない**（`[照会中]` リネーム＝人手確認へ回る）。
- ただし **自動仕分け（auto）が成立せず**、事務所の手作業が増える（機能低下・データ損失ではない）。
- 署名移行そのもの（HMAC 経路・200 応答）には非依存（認証は成立・Vision 段でのみ縮退）。

## 3. 対処案

- **一次対処（[人]・必須）**: Google Cloud の Vision API **billing 有効化**（課金アカウント紐付け・
  対象プロジェクト確認）。有効化後、sortation の auto 判定が復帰することを実測確認。
- **監視拡張（Phase 2 候補）**: daily_healthcheck の依存 API 監視を **Vision API 到達性/billing 状態**
  へ拡張（現在は Anthropic Models・kintone schema・docx template・journal・業務通知 heartbeat を監視）。
  Vision 403/課金エラーを **dead-man 同様に検知**し、全件「不明」縮退の長期化を早期可視化する。
  - 実装形の候補: (a) 起動時/日次に Vision の軽量 probe（1x1 ダミー annotate or models/health 相当）
    を 1 回叩き 403/課金エラーを problems へ。(b) sortation の縮退率（doc_type=不明の比率）を
    観測し閾値超で警報。いずれも別票・Phase 2 裁定。

## 3.1 監視拡張の実装（2026-07-20・P2-CHAIN-008）

- §3 実装形 (a) を実装済み: **`/health/deps`**（`hub/health_deps.py`・既存 `/health` は無変更）。
  Vision へ 1x1 ダミー annotate の軽量 probe（タイムアウト短め・既定 5 秒・
  env `HEALTH_DEPS_TIMEOUT_SECONDS` で調整可）を行い、403/到達不能/タイムアウトを
  **HTTP 200 のまま `status: degraded`** で返す（healthcheck 自体を落とさない・本教訓の反映）。
  応答に secret・内部 URL・vendor 本文は含めない（H02 流儀）。
  daily_healthcheck からの定期呼出し・警報結線は別票（Phase 2 裁定のまま）。

## 4. 位置づけ・未決

- 本件は **billing 運用事象**であり、コード欠陥ではない（縮退は設計どおり安全側）。恒久策の主体は
  (i) billing 有効化（[人]・即時）と (ii) 監視拡張（Phase 2・コード）。優先度・着手は司令塔裁定。
- S5 work-log（2026-07-18）の残置事項 (iii) と対応。
