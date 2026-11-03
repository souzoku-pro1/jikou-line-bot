# 時効 LINE bot 返信規則（JIKOU-REPLY Q1a〜Q6）引継ぎ書

- 作成: 2026-10-13（HOUKI-E2E-1・QUEUE-2026-10-13-E E-5）。対象: feature/jikou-reply-q6 までの積み上げ（Q1a は main 済み・
  Q1b〜Q6 は未 merge。merge 順は pr_bodies/_merge_order.md）。
- 正本: Desktop/claude/時効LINEボット_返信規則_v1.4.md（リポジトリ外）。docs/ 配下に JIKOU-REPLY の設計 doc は無い（本書が
  コード側の入口一覧）。
- 共通規律: RV-08／RV-10・承認なしの送信経路を作らない・purpose は閉集合（send_ledger.PURPOSES・CHECK 制約）・新表は alembic CLI
  のみ（起動時に走らせない）。
- 総括スイッチ: `JIKOU_SEND_POLICY_V2`（既定 OFF）。OFF のとき Q1c〜Q5 の分岐には入らず従来経路と同一。ON のとき: 許可表 V2・
  空返事の無言化・J-0 初回 1 通・対応待ち・**閉集合定型のみ自動送信（AI 自由文は下書き）**・AI 下書き（手本あり）・昇格定型。

## 段階ごとの入口

| 段階 | module | 入口（route／ジョブ／結線） | 要点 |
|---|---|---|---|
| Q1a 送信台帳 | hub/send_ledger・hub/line_channel・hub/webapp_send_ops_view | `GET /app/send_ops`・`POST /app/send_ops/confirm`・`GET /app/api/send_ops/unconfirmed`・job `SEND_LEDGER_RECOVER`（5 分）・main.py 末尾（register_recover_job・check_dialect_at_startup・check_timing_config） | 全送信経路に記録と排他（begin→push→finish）。再配送は同一受信・用途・連番で 1 操作。fail-open（DB 未設定・例外は素通り）。unconfirmed は人が確定（自動再送なし） |
| Q1b 返信欄 | hub/webapp_reply_view・line_channel.push_text_explicit・hub/reply_exemplar・hub/reply_followup・hub/pii_scan | `GET /app/reply/{App29 No.}`・`POST …/send` | 人の明示送信（actor=human・台帳必須＝fail-closed）・署名付き ticket・明示確認・App 28 human 行（role に human が無ければ skip→回収）・手本集 reply_exemplar（匿名化） |
| Q1c 許可表 V2 | hub/send_policy・hub/reply_pending・hub/webapp_pending_view・hub/empty_reply_guard・hub/autoreply_stoplist | `GET /app/pending`・`POST /app/pending/resolve`・`POST /app/pending/end_attending`・`GET /app/api/pending/open`・main `_inbound_stop_kind`／`_handle_stopped_inbound`・承認 webhook の policy_stopped | decide_send（paused／stoplisted／human_mode／attending）・空返事は無言＋対応待ち・J-0（purpose=first_reply・会話 1 回）・【対応待ち】通知 1 会話 1 日 1 回 |
| Q3a 手本画面 | hub/webapp_exemplar_view・reply_exemplar（prompt_note／record_draft／record_sent） | `GET /app/exemplars`・`POST /app/exemplars/update`・`GET /app/api/exemplars` | 承認／退役・参考文例を prompt 末尾に（手本 0 件は従来と同一）・無修正承認の計数（reply_draft_approval）・閾値 3 で【凍結定型の候補】 |
| Q2 定型表 | hub/jikou_reply_texts・jikou_reply_select・jikou_reply_render・jikou_reply_flow | chat_responder の `try_closed_set`（V2 ON のみ） | 定型 24 件（frozen_v14 10 件選択可・needs_approval 14 件）・`TABLE_SHA256_PIN=c8c45c0b…`・分類 ID 16（tool select_intent）・J-5 の 2 算式（推奨＝最終返済日+5 年+3 か月末／基準＝評価日−5 年−3 か月）・債権者ルール全行仮＝J-5 は現状送らない・300 字ゲート（J-8→J-1 を外す）・該当なしは無言＋対応待ち |
| Q3b AI 下書き | hub/reply_draft・webapp_pending_view（印） | chat_responder の `reply_draft.maybe_generate`（1 行） | 同 category の承認済み手本 ≤3 があるときだけ生成（Claude）・サーバーガード・保存のみ（自動送信なし）・【下書きあり】1 会話 1 日 1 回 |
| Q4 下書き承認 | hub/webapp_drafts_view | `GET /app/drafts`・`GET /app/drafts/{id}`・`POST …/act`・`GET /app/api/drafts` | このまま送る／直して送る／送らない・actor=approved_draft（op_id `draft:{id}:{op}`）・CAS・outcome 記録・修正承認は手本候補（active=False） |
| Q5 昇格 | hub/jikou_reply_promote・webapp_exemplar_view（追加）・jikou_reply_flow._try_promoted | `POST /app/exemplars/promote`・`POST /app/exemplars/text` | 無修正承認 3 回の category の手本を J-U 連番でデータ表へ（コードの pin 不変）・退役／再承認・Q2 該当なしのとき category 対応の昇格定型を送る（DRAFT_ONLY_CATEGORIES は送らない） |
| Q6 計測 | hub/jikou_reply_metrics・webapp_reply_metrics_view | `GET /app/reply/metrics`・`GET /app/api/reply/metrics`・job `JIKOU_REPLY_METRICS_WEEKLY`（daily 9 時・月曜のみ送る） | 遅延（7／30 日の中央値・90%・最長）・下書き結果率・昇格定型・債権者別・未返信。件数と率のみ。週次 key `jikou_metrics_weekly:{週開始日}` |

## env／flag

| env | 既定 | 用途 |
|---|---|---|
| JIKOU_SEND_POLICY_V2 | OFF | 上記の総括スイッチ（ON は大野） |
| SEND_LEDGER_STALE_MINUTES／DEADLINE_MINUTES／HEARTBEAT_SECONDS／SEND_TIMEOUT_SECONDS | 10／5／60／240 | 台帳の回収・期限・heartbeat・送信 timeout（順序不整合は警告＋既定値） |
| JIKOU_REPLY_METRICS_DISABLED | 未設定（有効） | 週次要約の停止 |
| AUTOREPLY_PAUSED | 未設定 | 全体停止（許可表 paused） |
| ATTORNEY_LINE_USER_ID＋業務トークン（notify.business_token_env） | — | 業務 LINE 通知の宛先 |
| APP_APPROVAL／TOKEN_APPROVAL（App 29）・APP_CHATLOG／TOKEN_CHATLOG（App 28）・APP_AUTOREPLY_STOP／TOKEN_AUTOREPLY_STOP（App 39）・KINTONE_APP_ID／KINTONE_API_TOKEN（App 21） | — | kintone |
| DATABASE_URL | — | 未設定: bot 経路は素通り（Q1a fail-open）・人の明示送信は fail-closed（Q1b）。JIKOU-SEND-FAILCLOSED-1（別 branch・flag 既定 OFF）で bot 経路も台帳必須に切替可 |

## DB 表と migration（適用順・本番は未適用のものあり）

| revision | 表 | 票 |
|---|---|---|
| e5f8a1b4c7d9 | conversation・send_operation・send_operation_history・inbound_touch | Q1a（fix1/2 は同 revision を更新） |
| f7b2c9d4e1a6 | reply_exemplar・reply_exemplar_history・reply_chatlog_followup | Q1b（fix1 同 revision） |
| a1c3e5f7b9d2 | reply_pending・reply_pending_history | Q1c |
| b2d4f6a8c0e1 | reply_draft_approval | Q3a |
| c3e5a7b9d1f3 | reply_draft・reply_draft_history | Q3b |
| d4f6b8c0e2a4 | reply_draft_approval.outcome | Q4 |
| e6a8c0d2f4b6 | jikou_reply_text・jikou_reply_text_history | Q5 |
| f8b0c2d4e6a8 | send_operation.purpose の CHECK 拡張＋既存行書換 | JIKOU-SEND-PURPOSE-2（別 branch・作成のみ） |

downgrade はデータがあれば拒否（RV-08）。本番適用前に `alembic current` を確認（同 revision 更新の票あり）。

## 送信台帳の key（purpose／channel／受信イベント ID）

| 送信 | actor／purpose | channel | key |
|---|---|---|---|
| bot 返信（既定） | bot／reply | jikou | webhookEventId＋inbound_seq |
| follow／受付番号／画像受領／画像読取／緊急即時定型／J-0 | bot／follow・receipt_number・image_receipt・image_result・urgent・first_reply | jikou | 受信イベント ID |
| 承認 webhook | approved_draft／reply | jikou | op_id `approval:{App29 No.}:{revision}` |
| 返信欄（Q1b） | human／reply | jikou | op_id uuid4 |
| 下書き承認（Q4） | approved_draft／reply | jikou | op_id `draft:{draft_id}:{op}` |
| 業務通知 | bot／other | jikou_notice | `pending_notice:{会話ref}:{日付}`・`draft_notice:…`・`approval_blocked:{No}:{rev}`・`exemplar_promotion:{category}:{n}`・`j5_notice:{会話ref}:{company}`・`jikou_metrics_weekly:{週開始日}` |

通知ヘッダ: 【対応待ち】【承認済み下書き・未送信】【凍結定型の候補】【推奨時期の自動案内】【下書きあり】【時効 返信計測 週次】（既存: 【人対応中】）。

## 凍結 pin・閉集合

- `jikou_reply_texts.TABLE_SHA256_PIN=c8c45c0bc8fafb997dc30bd67430d35909194cdd9ff35eef8497aafeb3e3aacf`（TEXTS・CREDITOR_RULES・INTENT_RULES・CATEGORY_TO_TEXT_IDS の正規化 JSON）。不一致は選択器・描画器が何も返さない。承認後は text／status と pin だけ差し替える（コードは触らない）。
- `send_policy.J0_TEXT_SHA256=0f1c3ca29956f4000aface287c8dc9aa7c6907e93c9b7f1267858a6c2212b03d`（J-0＝司令塔仮置き・要承認）。J-7 は凍結 FEE_GUIDE_TEXT と同一。
- 分類 14（reply_exemplar.CATEGORIES＝compose_reply の enum）・意図 16（jikou_reply_texts.INTENTS）・purpose 8・actor 3・state 5・pending state 4／route 3／reason 4・draft state 5／kind 2・outcome 3・昇格 status 2・DRAFT_ONLY_CATEGORIES 7。
- 要承認文言 24 件の一覧: pgv/q2_texts_for_approval.md（リポジトリ外・大野承認待ち）。

## 運用の注意（docstring 由来）

1. V2 ON は配備後に env で切り替える。ON にすると AI 自由文の自動送信が無くなり、該当なしは無言＋対応待ち通知になる（意図した挙動変更）。
2. 承認済み下書き（App 29）は会話版の一致を取れないため `versions_match=False`（人対応中・対応中は送らず【承認済み下書き・未送信】）。
3. 対応待ちの保存に失敗したら顧客へ送らない（J-0 も送らない）。
4. 手本の残留検査（pii_scan）に落ちた文は保存しない（送信は成功扱い）。
5. 会話履歴の反映は main.conversation_histories（プロセス内・単一 worker）。
6. App 28 の role に human が無ければ記録を skip し回収ジョブが再試行（選択肢追加は大野）。
7. J-5 は債権者ルール表の確定行が無い間は送らない（現状全行 provisional）。
8. 週次要約は V2 OFF の間は全件数 0 で送られない。
9. Q3a／Q4 の main.py 末尾窓 pin（exemplar 結線が末尾 10 行・drafts 結線が末尾 6 行）は後続の末尾追記で落ちる（merge 時に大野判断）。
