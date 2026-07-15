# 作業記録 2026-07-14: RV-05-13-fix2（R-RV-05-13-2 所見の修正）

- 対象: R-RV-05-13-2（fix1 = 587b1fc への再レビュー）の必須修正
- BASE: 587b1fc / BRANCH: feat/rv05-13-durable-inbound（継続）
- 正本: DRAFT_RV05_DURABLE_INBOUND.md rev5（本文契約は不変）
- DO_NOT_CHANGE 遵守: fencing core・§H-06 応答表・顧客Bot本体・Stripe は未接触

## 0. 結論サマリ

2 件の欠陥を是正:
- **H-NEW-01**（永久滞留）: fix1 の「重複配送は一律 skip」は、部分 insert 失敗
  （同一バッチの他 event が 503）で **INSERT 済みだが未処理**の event を再配送でも
  skip し続け、**永久に処理されない**経路を作っていた。terminal 到達済みのみ skip し、
  未終端（received/failed・上限内）は re-attempt へ変更。
- **M-NEW-01**（lease 過小＋Vision 無 timeout）: lease 600s は Claude の SDK retry 乗数
  （×3）を数えておらず過小。1800s へ。Vision `urlopen` の無 timeout（socket ハングで
  無限滞留）に明示 timeout（120s）を付与。

- 対象 suite `test_rv05_13_durable.py`＋`test_ingestion_receipt.py`: **31 passed**（fix1 26 ＋5）。
- 全 suite: §6（base 比較つき）。

## 1. 所見 → 修正 対応表

| 所見 | 修正 | 実装位置 | 担保テスト |
|---|---|---|---|
| **H-NEW-01** | record_line_event の重複分岐を DB 最新 state で 3 分岐に: `done`（terminal）→ "duplicate"（skip・二重返信遮断）／`received`・`failed`（attempt 上限内）→ "reattempt"（条件付き atomic UPDATE で attempts++・再処理登録）／`processing`・上限超 → "duplicate"（skip）。main は "duplicate" のみ登録 skip | `hub/durable_inbound.py` record_line_event・`main.py` webhook | test_hnew01_partial_insert_failure_reattempts_once（①）・test_hnew01_terminal_duplicate_skips（②） |
| **M-NEW-01a** | lease 600→1800s（既定 `_DEFAULT_STALE_SECONDS`）。根拠は §5 に完全列挙 | `hub/durable_inbound.py` | §5 |
| **M-NEW-01b** | Vision `urlopen` に明示 timeout（env `VISION_ANNOTATE_TIMEOUT_SECONDS`・既定120s）。根拠は §5 | `main.py` `_vision_timeout_seconds`/`_vision_annotate` | §5（全 suite 回帰なし） |
| **M-NEW-02** | mark_phase=None／mark_terminal=False 誘発時に OCR/ask/forward 非実行かつ非200 を HTTP 統合層で固定 | test 追補 | test_mnew02_fence_lost_vendor_pre_no_side_effects・test_mnew02_fence_lost_terminal_non_200 |
| **M-NEW-03** | claim 不可→state 別応答が §H-06 状態表内に収まる契約テスト（processing→409・pending_retry→再処理200 等 8 state） | test 追補 | test_mnew03_claim_unavailable_response_contract（8 subtests） |

## 2. H-NEW-01 の設計（永久滞留を断つ）

### 2.1 fix1 の欠陥（再現シナリオ）
1 webhook バッチに event A・B。A の `record_line_event` は "new"（INSERT 成功・
BackgroundTasks 登録）。B の insert が DB 一時障害で失敗→ webhook は 503 送出。
FastAPI は **例外送出時に BackgroundTasks を実行しない**ため、**A の処理タスクは走らない**。
A 行は `received` のまま。GAS が同バッチを再配送→ fix1 は A を「重複」で一律 skip→
**A は永久に処理されない**（＝ INSERT 後クラッシュ／部分失敗の永久滞留）。

これは Stripe 経路が D14 で既に塞いだのと同型の穴（「INSERT後クラッシュ→再送停止→
永久未処理」）を LINE Phase A で再発させていた。

### 2.2 修正
`record_line_event` の IntegrityError（重複）分岐を **DB 最新 state** で判定:
- `done`（terminal 到達済み）→ 二重返信を遮断（"duplicate"・skip）。
- `received`／`failed`（attempts < 上限）→ 条件付き atomic UPDATE
  （`state IN ('received','failed') AND attempts < max` → `state='received', attempts++`
  RETURNING id）で再処理権を取り "reattempt"。**未終端の滞留を断つ**。
- `processing`（実行中）・attempts ≥ 上限（poison）→ "duplicate"（skip）。

main は従来どおり `outcome == "duplicate"` のみ登録 skip（"new"/"reattempt" は登録）。
上限は env `INBOUND_LINE_MAX_ATTEMPTS`（既定5）。

### 2.3 「一回だけ」の担保（二重返信なし・test ①）
再配送で A を re-attempt するが、二重処理は起きない:
- 例外送出で bg task が走らない場合 → A は request2 で初めて処理（1回）。
- 仮に走った場合 → request2 では A は `done` → skip（1回）。
どちらでも **A は正確に1回**（test ① が msgA の呼出回数＝1 を固定・旧コードでは 0＝滞留で FAIL）。

### 2.4 既知の残（OPEN・RV-06）
`processing` のまま hard-kill された行（mark_line_failed も走らなかった）は再配送でも
skip され滞留しうる。Phase A は fencing/stale-reclaim を持たない観測専用のため本 fix の
範囲外。RV-06 で LINE 側にも stale-reclaim を導入して解消する（`_process_line_event_durable`
の except は mark_line_failed を呼ぶため、処理中 crash の大半は `failed`→re-attempt で回収される）。

## 3. M-NEW-02（HTTP 統合層での中断担保）
- `mark_phase`=None（vendor_pre で fence 喪失）→ OCR/ask/forward いずれも未実行・**409**。
  （mock で mark_phase を None にして誘発。OCR は vendor_pre marker の後にしか走らないため
  非実行を検証できる。）
- `mark_terminal`=False（commit で fence 喪失）→ **409**（成功 ACK にしない）・receipt は
  completed にならない。

## 4. M-NEW-03（claim 不可→state 別応答契約）
claim 不可時の応答が §H-06 状態表内に収まることを 8 state で固定:

| state | claim | 応答 |
|---|---|---|
| processing / vendor_pre / sending / duplicate_suspect | 不可 | 409（別 request 実行中・人手待ち） |
| completed / failed / unknown | 不可 | 200 `action=skip`（冪等・人手） |
| pending_retry | **可**（guard 内） | 200（再 claim して再処理＝状態表内の正規経路） |

## 5. 【M-NEW-01】lease 1800s・Vision timeout の再定量化

### 5.1 単一 judge 呼出の最悪時間（SDK retry 乗数を含む）
| 要素 | 値 | 出典 |
|---|---|---|
| Claude read timeout | 600s | anthropic SDK 既定 `Timeout(read=600)` |
| SDK max_retries | 2（＝最大 3 attempt） | anthropic SDK 既定 `DEFAULT_MAX_RETRIES=2` |
| gateway FALLBACK_MODEL | ＋1 モデル（それ自体も SDK retry を持つ） | `claude_gateway.create_message_with_fallback` |

fix1 の 600s は「単一 attempt の read timeout」だけを数え、**SDK retry 乗数（×3）を無視**して
いた。単一 judge 呼出の最悪は少なくとも **600 × (1+2) = 1800s**（PRIMARY の 3 attempt）。
FALLBACK まで含めると理論上さらに +1800s（＝3600s）だが、下記 5.3 の通り lease は相互排他の
正しさ保証ではなく、marker 間区間に対する誤 reclaim 回避の下限であるため、**支配的な単一
call の retry 展開（1800s）**を lease 値に採る。

### 5.2 Vision の明示 timeout（120s）
Vision `files:annotate`（同期・5ページ/req）は `urllib.request.urlopen` に **timeout 未指定**で、
socket ハング時に**無限滞留**しうる（無 timeout）。5ページ同期 annotate は実測で数秒オーダー。
120s は通常の ~20-40× 余裕で遅い大判スキャンも吸収しつつ、無限滞留経路を断つ。env
`VISION_ANNOTATE_TIMEOUT_SECONDS` で調整可。timeout 超過は例外送出→ sortation の
PENDING_RETRY→5xx（fail-closed・GAS 再送）に倒れる。

### 5.3 lease=1800 の選択と正しさ非依存
- 各 mark_phase/claim/mark_terminal は `last_heartbeat_at=now()` を書く（fence 更新）。lease が
  超えるべきは request 総和ではなく**連続する 2 つの fence 更新書込の最長間隔**。
  支配区間 `claim→vendor_pre` ＝ Vision（≤120s×バッチ数・多くは 1〜2 バッチ）＋ judge
  （≤1800s・retry 込み）。
- lease=1800 は「支配的単一 judge 呼出（retry 込み）」に一致。fix1 の 600 からの 3× 引き上げは、
  無視していた SDK retry 乗数の是正が主因。
- **正しさは lease に依存しない**: 誤 reclaim（epoch++）が起きても in-flight の terminal/
  heartbeat は 0 行で abort（RV-05-13-fix work-log §4.6・test_m04）。lease は「健全な処理中を
  誤って reclaim しない」ための liveness 下限。
- 残リスク（judge full-3-retry ＋ 大判 Vision が同一区間・または FALLBACK まで発火して 1800 超）は
  fencing 正しさで無害化される。judge の 3 連続フル timeout は指数 backoff＋健全 call は数秒完了
  のため実務上極めて稀。

（fix1 work-log の §5 は lease=600 前提の記述。本 fix2 §5 が最新の正本。）

## 6. テスト・全 suite 実出力（base 比較）

### 6.1 対象 suite
```
$ PYTHONUTF8=1 python -m pytest test_rv05_13_durable.py test_ingestion_receipt.py -q
31 passed, 5 warnings, 10 subtests passed in 5.89s
```
fix1 の 26 ＋5（H-NEW-01 ×2・M-NEW-02 ×2・M-NEW-03 ×1〔8 subtests〕）。

### 6.2 sink 方針・sentinel（台帳 resync 後）
```
$ PYTHONUTF8=1 python -m pytest test_sink_ast_policy.py test_redaction_sentinels.py -q
17 passed, 89 subtests passed in 7.02s
```
resync は main.py の `sink:logger` 2 件の行移動のみ（total 61 不変・baseline 211 単調減少維持・
manifest 不変・新規違反ゼロ）。

### 6.3 全 suite（base 587b1fc 比較）
```
$ PYTHONUTF8=1 python -m pytest -q
# fix2（本ブランチ HEAD）
1 failed, 1364 passed, 5 warnings, 447 subtests passed in 44.11s
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy

# base 587b1fc（fix1）
1 failed, 1359 passed, 5 warnings, 439 subtests passed
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy
```
- Δ = 1,359 → **1,364 passed（＋5＝本 fix の新規テスト）・回帰ゼロ・subtests 439→447（＋8＝M-NEW-03）**。
- 唯一の FAIL は fix1 で実証済みの **既存アーティファクト**（`@skipUnless(ANTHROPIC_API_KEY)` の
  実 Claude API テストが full suite の env 漏れで dummy キー実行され落ちる）。base でも同一に発生し、
  本 fix と無関係。real key の `railway run` 実行では pass。
