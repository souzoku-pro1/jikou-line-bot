# DRAFT: Phase 3 control plane 準備調査 — repo 実物との gap 分析

- TASK_ID: P2-CHAIN-010（READ_ONLY 調査＋DRAFT 起票・PC-A）／記録日 2026-07-21
- 調査 BASE: origin/main `7ff7775`（読取のみ）
- **前提の制約**: 「製品設計完全版/master **v2.4**」正本は **repo 外**（repo からは
  §番号引用のみ到達可能: 例 DRAFT_APP36 冒頭「v2.4 §8.11/§9.21/§9.23」・DRAFT_RV05
  「§9.17/§9.17.1/§8.8」）。**Phase 3 要件の逐語・effect level E0–E3 の定義は司令塔
  提供／[人]確認が必要**（P2L3P-H01 型の断定はしない）。本書の gap は repo 内引用と
  実装物の対比に基づく。

## 1. 既存資産の棚卸し（実出力）

### 1.1 DB テーブル（alembic 実物・migration 資産）
```
20260711_a3ea96f2e1a8  inbound_event（＋ f8ef81de70a5 claimed_at 列）
20260712_3e59f8270aa8  notify_heartbeat
20260714_b7d3e1a9c2f4  signature_nonce
20260714_c4f1a2b7d8e9  ingestion_receipt ＋ processing_attempt（create_table×2）
```
＝ RV-05/13 系（inbound_event／ingestion_receipt／processing_attempt）は**実装・migration 済み**。

### 1.2 対象モデルの現存状況（grep 実出力: 本番 .py に該当なし）
- **DerivationRun／HeirConfirmationDecision**: コード・migration とも**未実装**。
  設計は `DRAFT_APP36_DERIVATION_APP37_TEMPLATE_REGISTRY.md` §2 で確定済み
  （NH01: **DerivationRun は純粋 immutable**・人の確定は HeirConfirmationDecision へ分離／
  **2026-07-12 司令塔裁定済み**: app-state DB 置き）。
- **Outbox（OutboxJob／worker）**: **意図的に未実装**。`DRAFT_RV05_DURABLE_INBOUND.md`:
  > §8.8 は Phase 6 Outbox worker 契約。本票は…**継続 worker/OutboxJob/poll consumer は作らない**＝§4
- **ApprovalSnapshot**: この名称の実装・設計は repo に無い。近縁は App37 の
  **template snapshot 凍結**（DRAFT_APP36 §4「生成時は App37 snapshot を凍結」）。
  v2.4 上の「ApprovalSnapshot」該当概念の有無は**[人]確認**。

### 1.3 承認系の稼働資産
- `hub/approval.py`: **App 30（発送管理）の状態機械**・冪等ガード。
  **絶対制約**=「承認待ち→承認済」遷移をサーバーに作らない（弁護士 kintone 操作のみ・
  `test_hub_approval.py` が恒久担保）。SERVER_TRANSITIONS で許可遷移を frozen 管理。
- App 29（承認キュー）: `main.py:67` で hub 経由結線
  （`KintoneApp("App 29 (承認キュー)", "APP_APPROVAL", "TOKEN_APPROVAL")`）・
  `/webhook/kintone/approval`（承認済→LINE Push）稼働中。障害時は「承認キューに
  要対応レコード」起票の縮退（main.py:488）。

## 2. v2.4 Phase 3 要件との対応表（repo 引用ベース）

| 要件（票の指定） | repo 現状 | 充足 |
|---|---|---|
| control plane（承認を経ない対外効果を作らない） | App30 状態機械＋絶対制約＋App29 webhook（§1.3） | **土台あり**（時効援用ユニットで稼働） |
| 承認キュー | App 29 結線・「【承認依頼】」型（03-common §5）稼働 | **あり** |
| effect level E0–E3 | **repo 内に定義・実装なし**（grep 0 件） | **不足**（定義正本は v2.4・[人]確認） |
| DerivationRun（immutable 導出台帳） | 設計・裁定済み／未実装（§1.2） | **不足（設計済）** |
| 人の確定（HeirConfirmationDecision） | 同上 | **不足（設計済）** |
| Outbox worker（§8.8 Phase 6 契約） | 原則のみ流用・本体は作らない裁定 | **不足（意図的 DEFER）** |
| durable inbound（受理台帳） | 実装済み・flag OFF 待機（P2-CHAIN-007 で点火材料済み） | **あり（点火待ち）** |

## 3. App29（承認キュー）結線の現状と Phase 3 拡張点

- 現状: (i) 承認済 webhook→LINE Push (ii) Claude 全滅時の要対応レコード起票
  (iii) rotation 済み token（D-6b・4 工程完了）。
- Phase 3 で必要になる拡張点（候補・裁定用）:
  - **effect level のフィールド化**（E0–E3 を App29/App30 レコードに持たせ、レベル別に
    承認要否・承認者を分岐）— E 定義の正本確認が前提（[人]）。
  - **承認 snapshot**（承認時点の入力・テンプレ版数の凍結参照）— App37 snapshot 凍結
    （DRAFT_APP36 §4）を承認レコードへ拡張する形が自然か、v2.4 の規定に従うか裁定。
  - 相続放棄ユニット（10-unit-02）への**同型横展開**（G1 一般化の枠組みで共用）。

## 4. 不足分の実装順序案（依存関係）

```
[裁定] E0–E3 定義の正本確認（[人]・v2.4 逐語）
   │
   ▼
① DerivationRun ＋ HeirConfirmationDecision（app-state DB・migration 要=新規2表）
   │  （NH01 分離・immutable 制約・裁定 2026-07-12 済みのため先行着手可能）
   ▼
② TemplateVersion の DB 化【fix1・P3PREP-H01: 正本 DRAFT_APP36 §2（89-91 行）裁定どおり
   │  **app-state DB の別 metadata**（DerivationRun/HCD と同じ専用モジュール群・
   │  inbound_event.Base 相乗りせず・L03 準拠）。**migration 要**。
   ▼
③ 封筒フロー結線【fix1・P3PREP-H02: 正本 §3.1-3.2 の方向へ整合】
   │  **DerivationRun → App30 要確認封筒の起票（機械・detail=DerivationRun.id・単票 API）
   │  → [人] の関所（承認・review_resolve）→ HCD 1 行追記＋App36 projection 更新**
   │  （旧記述「承認前 App36 を入力に App30 起票」の逆向きは撤回）
   ▼
④ effect level の control plane 組込み（E 定義確定後・App29/30 拡張）
   ⑤ Outbox worker（§8.8 Phase 6 契約・K4/RV-06 と同一群で裁定）→ Phase 3 では DEFER 継続が既定
```
- migration 要否: ①=**要**（2 表新規）・②=**要**（TemplateVersion 表・fix1）・
  ③④=既存表/kintone 拡張中心・⑤=要（将来）。

## 5. Phase 3 最初の実装票 3 本分のスコープ案（司令塔裁定用）

（fix1: Codex 推奨の依存関係 P3-001 → P3-002 → P3-003 へ再構成・E0–E3 は正本確認後）

| 票案 | スコープ | 前提 |
|---|---|---|
| P3-001: DerivationRun/HCD 実装 | モデル＋migration 2 表（app-state DB・別 metadata）＋immutable 制約（UPDATE/DELETE 拒否）＋分離契約テスト | 裁定済み・着手可 |
| P3-002: TemplateVersion の DB 化 | **DB model＋migration**（正本 §2 裁定・app-state DB 別 metadata）。**immutable 版管理・部分 unique・bytes 再現 contract を DB 制約で担保**＋生成時 snapshot 凍結・provisional=True 生成拒否 | P3-001（同 metadata 群） |
| P3-003: 封筒フロー結線 | **DerivationRun→App30 要確認封筒起票（機械）→[人]関所（review_resolve）→HCD 追記＋App36 projection 更新**（正本 §3.1-3.2 の向き・冪等キー＝`heir_derivation:{case}:{input_hash}`）＋状態機械整合テスト | P3-001・P3-002・App30 稼働資産 |
- E0–E3 組込み（④）は**定義正本の確認後に別途起票**（本 3 本と並行裁定可）。
