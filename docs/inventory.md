<!-- 自動生成: scripts/gen_inventory.py（INVENTORY-1）。手で編集しない。再生成: python scripts/gen_inventory.py -->
# 全体目録（自動生成）

コードから抽出した目録。env の値・token・URL・時刻・SHA は含まない。test_inventory_is_current.py が
生成結果と本ファイルの一致を検査する（差分があれば再生成して commit する）。

## (a) 画面一覧（/app 配下）

| URL | method | 必要ロール | 担当モジュール |
|---|---|---|---|
| /app | GET | ログイン必須（_gate） | hub.webapp_auth |
| /app/api/approvals | GET | ログイン必須（_gate） | hub.webapp_approval_view |
| /app/api/brain/case | GET | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/api/brain/conflicts | GET | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/api/brain/enabled | GET | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/api/brain/facts | GET | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/api/brain/holds | GET | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/api/brain/overview | GET | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/api/brain/pending | GET | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/api/brain/recheck | GET | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/api/cases | GET | ログイン必須（_gate） | hub.webapp_case_views |
| /app/api/cases/{record_id} | GET | ログイン必須（_gate） | hub.webapp_case_views |
| /app/api/drafts | GET | ログイン必須（_gate） | hub.webapp_drafts_view |
| /app/api/exemplars | GET | ログイン必須（_gate） | hub.webapp_exemplar_view |
| /app/api/houki/overdue | GET | ログイン必須（_gate） | hub.webapp_houki_overdue_view |
| /app/api/kinship | GET | ログイン必須（_gate） | hub.webapp_kinship_view |
| /app/api/pending/open | GET | ログイン必須（_gate） | hub.webapp_pending_view |
| /app/api/q/history | GET | ログイン必須（_gate） | hub.webapp_q |
| /app/api/reply/metrics | GET | ログイン必須（_gate） | hub.webapp_reply_metrics_view |
| /app/api/send_ops/unconfirmed | GET | ログイン必須（_gate） | hub.webapp_send_ops_view |
| /app/api/souzoku/cases | GET | ログイン必須（_gate） | hub.webapp_souzoku_dashboard |
| /app/api/souzoku/cases/{record_id} | GET | ログイン必須（_gate） | hub.webapp_souzoku_dashboard |
| /app/app.js | GET | ログイン必須（_gate） | hub.webapp_auth |
| /app/approvals | GET | ログイン必須（_gate） | hub.webapp_approval_view |
| /app/brain | GET | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/brain/confirm | POST | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/brain/relink | POST | ログイン必須（_gate） | hub.webapp_brain_view |
| /app/case | GET | ログイン必須（_gate） | hub.webapp_case_views |
| /app/cases | GET | ログイン必須（_gate） | hub.webapp_case_views |
| /app/drafts | GET | ログイン必須（_gate） | hub.webapp_drafts_view |
| /app/drafts/{draft_id} | GET | ログイン必須（_gate） | hub.webapp_drafts_view |
| /app/drafts/{draft_id}/act | POST | ログイン必須（_gate） | hub.webapp_drafts_view |
| /app/exemplars | GET | ログイン必須（_gate） | hub.webapp_exemplar_view |
| /app/exemplars/promote | POST | ログイン必須（_gate） | hub.webapp_exemplar_view |
| /app/exemplars/text | POST | ログイン必須（_gate） | hub.webapp_exemplar_view |
| /app/exemplars/update | POST | ログイン必須（_gate） | hub.webapp_exemplar_view |
| /app/houki/jijou/{record_id} | GET | ログイン必須（_gate） | hub.webapp_houki_jijou_view |
| /app/houki/jijou/{record_id}/generate | POST | ログイン必須（_gate） | hub.webapp_houki_jijou_view |
| /app/houki/overdue | GET | ログイン必須（_gate） | hub.webapp_houki_overdue_view |
| /app/icons/icon-192.png | GET | ログイン必須（_gate） | hub.webapp_auth |
| /app/icons/icon-512.png | GET | ログイン必須（_gate） | hub.webapp_auth |
| /app/kinship | GET | ログイン必須（_gate） | hub.webapp_kinship_view |
| /app/login | GET | 公開 | hub.webapp_auth |
| /app/login | POST | 公開 | hub.webapp_auth |
| /app/logout | POST | ログイン必須（_gate） | hub.webapp_auth |
| /app/manifest.json | GET | ログイン必須（_gate） | hub.webapp_auth |
| /app/pending | GET | ログイン必須（_gate） | hub.webapp_pending_view |
| /app/pending/end_attending | POST | ログイン必須（_gate） | hub.webapp_pending_view |
| /app/pending/resolve | POST | ログイン必須（_gate） | hub.webapp_pending_view |
| /app/q | GET | ログイン必須（_gate） | hub.webapp_q |
| /app/q/ask | POST | ログイン必須（_gate） | hub.webapp_q |
| /app/q/reset | POST | ログイン必須（_gate） | hub.webapp_q |
| /app/reply/metrics | GET | ログイン必須（_gate） | hub.webapp_reply_metrics_view |
| /app/reply/{record_id} | GET | ログイン必須（_gate） | hub.webapp_reply_view |
| /app/reply/{record_id}/send | POST | ログイン必須（_gate） | hub.webapp_reply_view |
| /app/send_ops | GET | ログイン必須（_gate） | hub.webapp_send_ops_view |
| /app/send_ops/confirm | POST | ログイン必須（_gate） | hub.webapp_send_ops_view |
| /app/shell.js | GET | ログイン必須（_gate） | hub.webapp_auth |
| /app/souzoku | GET | ログイン必須（_gate） | hub.webapp_souzoku_dashboard |
| /app/souzoku/case | GET | ログイン必須（_gate） | hub.webapp_souzoku_dashboard |
| /app/sw.js | GET | ログイン必須（_gate） | hub.webapp_auth |
| /app/{_rest:path} | GET | ログイン必須（_gate） | hub.webapp_auth |

## (b) ジョブ一覧

| 名前 | 種別 | 起動 | 本体 | 登録モジュール | 登録条件 | 台帳 key 形式（モジュール内） | 通知先 |
|---|---|---|---|---|---|---|---|
| BRAIN_SYNC | interval | 5.0 分ごと | run_brain_sync_job | hub/brain_sync.py | — | — | 通知なし（DB／kintone のみ） |
| HEALTHCHECK | daily | 毎日 env:HEALTHCHECK_HOUR_JST（既定 7） 時 JST | run_healthcheck | daily_healthcheck.py | — | — | 管理者 LINE（notify_admin_line）／業務 LINE（notify_business） |
| HOUKI_JUKURYO_{hour∈8／13／18:02d} | daily | 毎日 {hour∈8／13／18} 時 JST | functools.partial(jukuryo_daily_check, hour) | hub/houki_jukuryo.py | — | {kind}:{business_key} | 管理者 LINE（LINE_ADMIN_USER_ID・業務トークン）／管理者 LINE（LINE_ADMIN_USER_ID） |
| HOUKI_KOSEKI_CHECKLIST | interval | 30.0 分ごと | functools.partial(scan_once) | hub/houki_koseki_checklist.py | — | {key} | 管理者 LINE（LINE_ADMIN_USER_ID・業務トークン）／管理者 LINE（notify_admin_line）／管理者 LINE（LINE_ADMIN_USER_ID） |
| HOUKI_OVERDUE | daily | 毎日 9 時 JST | overdue_daily_check | hub/houki_overdue.py | — | houki_overdue:{today.isoformat(...)} | 管理者 LINE（LINE_ADMIN_USER_ID・業務トークン）／管理者 LINE（LINE_ADMIN_USER_ID） |
| JIKOU_REPLY_METRICS_WEEKLY | daily | 毎日 9 時 JST | weekly_check | hub/jikou_reply_metrics.py | — | jikou_metrics_weekly:{week_start.isoformat(...)} | 業務 LINE（ATTORNEY_LINE_USER_ID・send_policy） |
| JIKOU_REPLY_PENDING_AGE_{hour∈9／15:02d} | daily | 毎日 {hour∈9／15} 時 JST | functools.partial(run_age_notice_job, hour) | hub/send_policy.py | — | {key} | 業務 LINE（ATTORNEY_LINE_USER_ID・send_policy） |
| RETURN_DEADLINE | daily | 毎日 env:RETURN_DEADLINE_HOUR_JST 時 JST | return_deadline_check | hub/return_deadline.py | — | — | 管理者 LINE（notify_admin_line） |
| SEND_LEDGER_RECOVER | interval | 5.0 分ごと | run_recover_job | hub/send_ledger.py | — | — | 通知なし（DB／kintone のみ） |

## (c) 送信台帳 purpose と受信イベント ID（key）形式

### purpose（閉集合）

| 定数 | 値 | 閉集合 PURPOSES に含む |
|---|---|---|
| PURPOSE_FIRST_REPLY | first_reply | はい |
| PURPOSE_FOLLOW | follow | はい |
| PURPOSE_IMAGE_RECEIPT | image_receipt | はい |
| PURPOSE_IMAGE_RESULT | image_result | はい |
| PURPOSE_OTHER | other | はい |
| PURPOSE_RECEIPT_NUMBER | receipt_number | はい |
| PURPOSE_REPLY | reply | はい |
| PURPOSE_URGENT | urgent | はい |

### 受信イベント ID（key）の形式（bind_inbound の引数・*ledger_key の戻り値。{…} は実行時の値）

| 形式 | モジュール |
|---|---|
| houki_overdue:{today.isoformat(...)} | hub/houki_overdue.py |
| jikou_metrics_weekly:{week_start.isoformat(...)} | hub/jikou_reply_metrics.py |
| {'reply_followup_hold:' + now.astimezone(_JST).strftime('%Y-%m-%d')} | hub/reply_followup.py |
| {_day_key(...)} | hub/reply_followup.py |
| {_marker_cat.rsplit(':', 1)[-1] if _marker_cat else ''} | hub/image_intake.py |
| {event_id} | hub/image_analysis.py |
| {event_id} | main.py |
| {key} | hub/houki_koseki_checklist.py |
| {key} | hub/send_policy.py |
| {kind}:{business_key} | hub/houki_jukuryo.py |
| {webhook_event_id} | main.py |

## (d) env 変数一覧（名前と用途のみ・値は出さない）

| 名前 | 参照モジュール（件数） | 用途（コード中の近傍コメント／docstring） |
|---|---|---|
| ANTHROPIC_API_KEY | channels/soufu_annai.py(1)、chat_responder.py(1)、hub/houki_card_read.py(1)、hub/houki_phone_triage.py(1)、hub/houki_profile.py(1)、hub/image_analysis.py(1)、main.py(1) | 特記事項の一文を Claude で生成する。**失敗時は空欄を返して続行** |
| APP_APPROVAL | chat_responder.py(1) | 顧客対応 Claude モジュール |
| APP_CHATLOG | chat_responder.py(1) | 顧客対応 Claude モジュール |
| APP_ENCLOSURE | channels/soufu_annai.py(1) | App 32 の有効ブロックキー ⊆ App 30『同封物選択』の選択肢 を検証する。 |
| APP_HOUKI | hub/heir_envelope.py(1) | HOUKI-SHOKUMU-LINK-1: App 40（相続放棄案件） |
| APP_SHIPPING | channels/soufu_annai.py(1)、hub/houki_soufu.py(1) | App 32 の有効ブロックキー ⊆ App 30『同封物選択』の選択肢 を検証する。 |
| APP_SOUZOKUNIN | daily_healthcheck.py(1) | 正本 DRAFT_APP36 §3.4 H11 検知側: 「対応する decision（decided_by あり・ |
| ATTORNEY_ALLOWLIST | hub/heir_projection.py(1) | ATTORNEY_ALLOWLIST（env・カンマ区切りの弁護士識別集合）。 |
| ATTORNEY_LINE_USER_ID | chat_responder.py(1)、daily_healthcheck.py(1)、hub/form_link.py(1)、hub/image_intake.py(1)、hub/image_store.py(1)、hub/notify.py(2)、hub/send_policy.py(1) | 顧客対応 Claude モジュール |
| AUTOREPLY_MAX_CHARS | hub/reply_sanitizer.py(1) | 1通の上限文字数（既定 300・env AUTOREPLY_MAX_CHARS で調整可）。 |
| AUTOREPLY_PAUSED | hub/houki_profile.py(1)、hub/image_analysis.py(2)、hub/image_intake.py(1)、hub/send_policy.py(1)、main.py(1) | 全業務ブレーキ（AUTOREPLY_PAUSED=1）。時効側と同じ env を共用する |
| BRAIN_SYNC_ENABLED | hub/brain_sync.py(1) | flag BRAIN_SYNC_ENABLED（既定 OFF・値集合は既存 flag 群と同一・R7）。 |
| BUSINESS_NOTIFY_STALE_HOURS | daily_healthcheck.py(1) | 業務通知（DISPATCHBOT）チャネルの死活を heartbeat の鮮度で検証する。 |
| DATABASE_URL | daily_healthcheck.py(3)、hub/db.py(1)、hub/durable_inbound.py(2)、hub/ingestion_receipt.py(1)、hub/notify_heartbeat.py(2) | journal（inbound_event）の滞留を検知し、問題のリストを返す。 |
| DISPATCHBOT_ALLOWED_USER_IDS | hub/notify.py(1) | 業務チャネル（DISPATCHBOT）で送信を許す全宛先（H02・push_line_message の関所）。 |
| DISPATCHBOT_CHANNEL_ACCESS_TOKEN | daily_healthcheck.py(1)、hub/notify.py(1) | M04: token 未設定は DATABASE_URL より先に検知（fail-closed で全業務通知が無音） |
| GOOGLE_VISION_API_KEY | hub/health_deps.py(1)、main.py(1) | Vision API の到達可否・billing/権限エラーの検知（403 を早期可視化）。 |
| HEALTHCHECK_DISABLED | daily_healthcheck.py(1) | FastAPI startup から呼ぶ。HEALTHCHECK_DISABLED=1 で無効化できる。 |
| HEALTHCHECK_HOUR_JST | daily_healthcheck.py(1) | FastAPI startup から呼ぶ。HEALTHCHECK_DISABLED=1 で無効化できる。 |
| HEALTH_DEPS_TIMEOUT_SECONDS | hub/health_deps.py(2) | health_deps — 依存サービス到達性の軽量確認（RCF-M14 監視拡張・Phase 2） |
| HEIR_CANCEL_ENABLED | hub/heir_cancel.py(1) | flag HEIR_CANCEL_ENABLED（既定 OFF・値集合は durable 系と同一流儀）。 |
| HEIR_DERIVATION_ENABLED | hub/heir_envelope.py(1) | flag HEIR_DERIVATION_ENABLED（既定 OFF・値集合は durable 系と同一流儀）。 |
| HOUKI_KOSEKI_CHECKLIST_DISABLED | hub/houki_koseki_checklist.py(2) | houki_koseki_checklist — HOUKI-KOSEKI-CHECKLIST-1: 受任時の必要戸籍チェックリスト初期行と連続戸籍充足の候補通知 |
| HOUKI_OVERDUE_DISABLED | hub/houki_overdue.py(2) | houki_overdue — HOUKI-JUKURYO-3: 期限超過×未提出の一覧と 1 日 1 回の件数通知 |
| IMAGE_HEAL_DISABLED | hub/image_intake.py(3) | setdefault・KOSEKI_READER_DISABLED と同区分）: 本発火は全テキスト受信の 入口に配線されるため、これを知らない既存テストから実 kintone へ到達 し得る。本番は env 未設定＝有効 |
| INBOUND_EVENT_DURABLE_ENABLED | daily_healthcheck.py(1)、hub/durable_inbound.py(1)、main.py(2) | 監視項目G（P2-CHAIN-012）: LINE durable 滞留（received/processing）。 M-06: flag OFF は hub.durable_inbound を import せず（env 直読み）現行 byte 同一。 |
| INBOUND_LINE_MAX_ATTEMPTS | hub/durable_inbound.py(2) | durable_inbound — RV-05-13 の flag・観測性（3系列カウンタ）・LINE Phase A 記録 |
| INBOUND_LINE_STALE_PROCESSING_SECONDS | hub/durable_inbound.py(2) | durable_inbound — RV-05-13 の flag・観測性（3系列カウンタ）・LINE Phase A 記録 |
| INBOUND_LINE_STALE_RECEIVED_SECONDS | hub/durable_inbound.py(2) | DRAFT_P2_DURABLE_IGNITION §3/§7: received（200 ACK 後 crash・batch 途中 503 等で 誰にも claim されない滞留）と processing（stale）を能動警報する。 `STRIPE_EVENT_JOURNAL_ENABLED` に依存しない（check_journal_backlog とは独立の専用検査）。 |
| INBOUND_RECONCILE_STALE_SECONDS | hub/durable_inbound.py(2) | durable_inbound — RV-05-13 の flag・観測性（3系列カウンタ）・LINE Phase A 記録 |
| JIKOU_REPLY_METRICS_DISABLED | hub/jikou_reply_metrics.py(2) | jikou_reply_metrics — JIKOU-REPLY-Q6: 返信品質の運用計測（読取専用）と週次要約（業務 LINE・件数と率のみ） |
| JIKOU_SEND_POLICY_V2 | hub/send_ledger.py(2) | send_ledger — JIKOU-REPLY-Q1a-SEND-BASE: 送信操作記録・会話・共通排他（§10-5・§12 Q1a） |
| KINTONE_API_TOKEN | chat_responder.py(1)、main.py(3) | 顧客対応 Claude モジュール |
| KINTONE_APP_ID | chat_responder.py(1)、hub/heir_envelope.py(1)、main.py(1) | 顧客対応 Claude モジュール |
| KINTONE_DOMAIN | main.py(1) | — |
| KINTONE_EVENT_DEDUP_ENABLED | daily_healthcheck.py(1)、hub/kintone_lane.py(1)、main.py(1) | journal（inbound_event）の滞留を検知し、問題のリストを返す。 |
| KINTONE_FUDOSAN_API_TOKEN | main.py(1) | — |
| KINTONE_FUDOSAN_APP_ID | main.py(1) | — |
| KINTONE_STALE_EVENT_HOURS | hub/kintone_lane.py(2) | kintone_lane — RV-04c S3: kintone webhook の id 冪等・state 遷移・XFF 観測・stale 監視補助。 |
| KINTONE_SUBDOMAIN | chat_responder.py(1)、daily_healthcheck.py(1)、hub/brain_ledger.py(1)、hub/brain_sync.py(1)、hub/kintone.py(1)、main.py(4) | ── 環境変数 ────────────────────────────────────────────────────────────────── |
| KINTONE_WEBHOOK_TOKEN | main.py(1) | — |
| KINTONE_WEBHOOK_TOKEN_NEXT | daily_healthcheck.py(1)、main.py(1) | RV-04c D2-M01: KINTONE_WEBHOOK_TOKEN_NEXT の残置検査（notice・警報ではない）。 |
| KINTONE_WEBHOOK_TOKEN_NEXT_EXPIRES | daily_healthcheck.py(1) | RV-04c D2-M01: KINTONE_WEBHOOK_TOKEN_NEXT の残置検査（notice・警報ではない）。 |
| KINTONE_XFF_OBSERVE_ENABLED | hub/kintone_lane.py(2) | ── §4.1 XFF 観測モード（observe-only・reject しない） ─────────────────────── |
| LINE_CHANNEL_ACCESS_TOKEN | chat_responder.py(1)、main.py(1) | 顧客対応 Claude モジュール |
| LINE_CHANNEL_SECRET | main.py(1) | 必須 env の import 時 fail-fast は従来どおり維持（値の実使用は hub/line_channel.JIKOU_CHANNEL が呼び出し時に env から解決する・H1） |
| LINE_USER_ID | main.py(1) | — |
| OFFICE_ATTORNEY_REG | channels/shokumu_seikyu.py(1) | 請求者欄（弁護士会・事務所・氏名・登録番号・電話）。印字フラグOFFなら空 |
| OFFICE_BAR_ASSOCIATION | channels/shokumu_seikyu.py(1) | 請求者欄（弁護士会・事務所・氏名・登録番号・電話）。印字フラグOFFなら空 |
| OFFICE_ZIP | hub/houki_dairinin.py(1) | env から事務所情報（未設定は ""）。住所は OFFICE_ZIP があれば「〒zip 住所」。 |
| PENDING_CONTEXT_ENABLED | chat_responder.py(1) | 承認送り時の即時応答（要件5）。大野の文言確定（env |
| PERSON_MERGE_ENABLED | main.py(1) | RV08-IMPL-07: 「封筒 closed×台帳 open」operation の回収（閉集合検証つき 完了化・検証不一致は警報）。flag OFF は import せず一切実行しない（M-06 流儀）。 |
| PRINT_OFFSET_X_MM | hub/address_label.py(1) | 印字オフセット（プリンタ個体差のキャリブレーション・mm単位） |
| PRINT_OFFSET_Y_MM | hub/address_label.py(1) | 印字オフセット（プリンタ個体差のキャリブレーション・mm単位） |
| RAILWAY_PUBLIC_DOMAIN | hub/shindan_link.py(2) | Railway 注入の既存 platform 変数 |
| REPLY_PENDING_AGE_MIN | hub/send_policy.py(2) | send_policy — JIKOU-REPLY-Q1c: 許可表 V2（bot／承認済み下書き列）の適用・空返事の無言化・J-0・対応待ちの結線 |
| RETURN_DEADLINE_DISABLED | hub/return_deadline.py(1) | FastAPI startup から呼ぶ。RETURN_DEADLINE_DISABLED=1 で無効化できる。 |
| RETURN_DEADLINE_HOUR_JST | hub/return_deadline.py(1) | FastAPI startup から呼ぶ。RETURN_DEADLINE_DISABLED=1 で無効化できる。 |
| SEND_LEDGER_DEADLINE_MINUTES | hub/send_ledger.py(1) | send_ledger — JIKOU-REPLY-Q1a-SEND-BASE: 送信操作記録・会話・共通排他（§10-5・§12 Q1a） |
| SEND_LEDGER_HEARTBEAT_SECONDS | hub/send_ledger.py(1) | fix3 BQ-09: 送信経路の稼働中の印と実効的な全体期限（heartbeat 間隔 < 送信 timeout < deadline < 回収閾値） |
| SEND_LEDGER_SEND_TIMEOUT_SECONDS | hub/send_ledger.py(1) | send_ledger — JIKOU-REPLY-Q1a-SEND-BASE: 送信操作記録・会話・共通排他（§10-5・§12 Q1a） |
| SEND_LEDGER_STALE_MINUTES | hub/send_ledger.py(1) | send_ledger — JIKOU-REPLY-Q1a-SEND-BASE: 送信操作記録・会話・共通排他（§10-5・§12 Q1a） |
| SERVICE_AUTH_DUAL_ACCEPT_ENABLED | hub/service_auth.py(2) | RV-04b dual-accept feature flag（既定 OFF＝完全に旧挙動。ON で署名経路併存） |
| SERVICE_AUTH_LEGACY_DISABLED_PATHS | hub/service_auth.py(3) | RV-04c §6/H07: 旧 query token を段階停止する path list（既定 未設定＝どこも停止しない）。 |
| SERVICE_AUTH_SIGNED_REQUIRED_PATHS | hub/service_auth.py(3) | RV02-ENFORCE: 署名 opt-in 入口の非署名遮断 path 集合（既定 未設定＝どこも遮断しない ＝二重受理のまま）。列挙できるのは opt-in 2 入口のみ（closed set）。列挙 path は dual-accept flag の状態に関わらず署名必須（強い方が勝つ・runbook §3-3/§4(e)）。 |
| SERVICE_HMAC_KEY_REGISTRY | hub/service_auth.py(1) | service_auth — NM01 v1 HMAC 署名検証コア（本番モジュール・RV-04a） |
| SHOKUMU_PLAN_ENABLED | hub/shokumu_plan.py(1) | flag SHOKUMU_PLAN_ENABLED（既定 OFF・値集合は既存 flag 群と同一流儀）。 |
| SIG_MAX_SKEW_SEC | hub/service_auth.py(2) | service_auth — NM01 v1 HMAC 署名検証コア（本番モジュール・RV-04a） |
| SOUZOKU_KINTONE_APP_ID | hub/heir_envelope.py(1)、hub/shokumu_plan.py(4)、units/souzoku/zaisan_sync.py(1) | ユニット種別の案件由来解決（fix1 H03・凍結 §2.1）。 |
| STALE_PROCESSING_MINUTES | hub/inbound_event.py(1) | processing を stale とみなす閾値（env STALE_PROCESSING_MINUTES・既定15分） |
| STRIPE_EVENT_JOURNAL_ENABLED | daily_healthcheck.py(1)、main.py(1) | 24h/1h の二重計上と閾値矛盾を解消）。kintone は §4.2b の専用検査 （KINTONE_EVENT_DEDUP_ENABLED ゲート・既定1h・専用 runbook）として別警報。 どちらの flag も OFF なら何も見ない。 |
| STRIPE_WEBHOOK_SECRET | main.py(1) | — |
| TOKEN_APPROVAL | chat_responder.py(1) | 顧客対応 Claude モジュール |
| TOKEN_CHATLOG | chat_responder.py(1) | 顧客対応 Claude モジュール |
| TOKEN_SHIPPING | hub/houki_soufu.py(1) | 相続放棄 受理通知書写しの発送 起票（hub/houki_soufu・HOUKI-SOUFU-1） |
| TOKEN_SOUZOKUNIN | daily_healthcheck.py(1) | 正本 DRAFT_APP36 §3.4 H11 検知側: 「対応する decision（decided_by あり・ |
| VISION_ANNOTATE_TIMEOUT_SECONDS | main.py(1) | Vision files:annotate の per-request timeout（env VISION_ANNOTATE_TIMEOUT_SECONDS・ |
| WEBAPP_PASSWORD_HASH | hub/webapp_auth.py(3) | webapp_auth — P4-001: PWA（仮名称「案件管理」）の認証＋shell 配信 |
| WEBAPP_SESSION_SECRET | hub/webapp_auth.py(3) | webapp_auth — P4-001: PWA（仮名称「案件管理」）の認証＋shell 配信 |
| ZAISAN_SYNC_DISABLED | units/souzoku/zaisan_sync.py(1) | 課税明細ライン S4: /ocr/fixed-asset の追記型拡張（App 財産への財産行 upsert） |

## (e) flag 一覧

| 名前 | 既定値（名前規約から） | ON にすると何が変わるか（コード中の近傍コメント／docstring） | 参照モジュール |
|---|---|---|---|
| AUTOREPLY_PAUSED | 未設定＝停止しない（1 で全停止） | 全業務ブレーキ（AUTOREPLY_PAUSED=1）。時効側と同じ env を共用する | hub/houki_profile.py、hub/image_analysis.py、hub/image_intake.py、hub/send_policy.py、main.py |
| BRAIN_SYNC_ENABLED | 未設定＝OFF（1／true で ON） | flag BRAIN_SYNC_ENABLED（既定 OFF・値集合は既存 flag 群と同一・R7）。 | hub/brain_sync.py |
| HEALTHCHECK_DISABLED | 未設定＝有効（1 で停止） | FastAPI startup から呼ぶ。HEALTHCHECK_DISABLED=1 で無効化できる。 | daily_healthcheck.py |
| HEIR_CANCEL_ENABLED | 未設定＝OFF（1／true で ON） | flag HEIR_CANCEL_ENABLED（既定 OFF・値集合は durable 系と同一流儀）。 | hub/heir_cancel.py |
| HEIR_DERIVATION_ENABLED | 未設定＝OFF（1／true で ON） | flag HEIR_DERIVATION_ENABLED（既定 OFF・値集合は durable 系と同一流儀）。 | hub/heir_envelope.py |
| HOUKI_KOSEKI_CHECKLIST_DISABLED | 未設定＝有効（1 で停止） | houki_koseki_checklist — HOUKI-KOSEKI-CHECKLIST-1: 受任時の必要戸籍チェックリスト初期行と連続戸籍充足の候補通知 | hub/houki_koseki_checklist.py |
| HOUKI_OVERDUE_DISABLED | 未設定＝有効（1 で停止） | houki_overdue — HOUKI-JUKURYO-3: 期限超過×未提出の一覧と 1 日 1 回の件数通知 | hub/houki_overdue.py |
| IMAGE_HEAL_DISABLED | 未設定＝有効（1 で停止） | setdefault・KOSEKI_READER_DISABLED と同区分）: 本発火は全テキスト受信の 入口に配線されるため、これを知らない既存テストから実 kintone へ到達 し得る。本番は env 未設定＝有効 | hub/image_intake.py |
| INBOUND_EVENT_DURABLE_ENABLED | 未設定＝OFF（1／true で ON） | 監視項目G（P2-CHAIN-012）: LINE durable 滞留（received/processing）。 M-06: flag OFF は hub.durable_inbound を import せず（env 直読み）現行 byte 同一。 | daily_healthcheck.py、hub/durable_inbound.py、main.py |
| JIKOU_REPLY_METRICS_DISABLED | 未設定＝有効（1 で停止） | jikou_reply_metrics — JIKOU-REPLY-Q6: 返信品質の運用計測（読取専用）と週次要約（業務 LINE・件数と率のみ） | hub/jikou_reply_metrics.py |
| JIKOU_SEND_POLICY_V2 | 未設定＝OFF（1／true で ON） | send_ledger — JIKOU-REPLY-Q1a-SEND-BASE: 送信操作記録・会話・共通排他（§10-5・§12 Q1a） | hub/send_ledger.py |
| KINTONE_EVENT_DEDUP_ENABLED | 未設定＝OFF（1／true で ON） | journal（inbound_event）の滞留を検知し、問題のリストを返す。 | daily_healthcheck.py、hub/kintone_lane.py、main.py |
| KINTONE_XFF_OBSERVE_ENABLED | 未設定＝OFF（1／true で ON） | ── §4.1 XFF 観測モード（observe-only・reject しない） ─────────────────────── | hub/kintone_lane.py |
| PENDING_CONTEXT_ENABLED | 未設定＝OFF（1／true で ON） | 承認送り時の即時応答（要件5）。大野の文言確定（env | chat_responder.py |
| PERSON_MERGE_ENABLED | 未設定＝OFF（1／true で ON） | RV08-IMPL-07: 「封筒 closed×台帳 open」operation の回収（閉集合検証つき 完了化・検証不一致は警報）。flag OFF は import せず一切実行しない（M-06 流儀）。 | main.py |
| RETURN_DEADLINE_DISABLED | 未設定＝有効（1 で停止） | FastAPI startup から呼ぶ。RETURN_DEADLINE_DISABLED=1 で無効化できる。 | hub/return_deadline.py |
| SERVICE_AUTH_DUAL_ACCEPT_ENABLED | 未設定＝OFF（1／true で ON） | RV-04b dual-accept feature flag（既定 OFF＝完全に旧挙動。ON で署名経路併存） | hub/service_auth.py |
| SHOKUMU_PLAN_ENABLED | 未設定＝OFF（1／true で ON） | flag SHOKUMU_PLAN_ENABLED（既定 OFF・値集合は既存 flag 群と同一流儀）。 | hub/shokumu_plan.py |
| STRIPE_EVENT_JOURNAL_ENABLED | 未設定＝OFF（1／true で ON） | 24h/1h の二重計上と閾値矛盾を解消）。kintone は §4.2b の専用検査 （KINTONE_EVENT_DEDUP_ENABLED ゲート・既定1h・専用 runbook）として別警報。 どちらの flag も OFF なら何も見ない。 | daily_healthcheck.py、main.py |
| ZAISAN_SYNC_DISABLED | 未設定＝有効（1 で停止） | 課税明細ライン S4: /ocr/fixed-asset の追記型拡張（App 財産への財産行 upsert） | units/souzoku/zaisan_sync.py |

## (f) kintone アプリ・欄の参照一覧

### アプリ（KintoneApp 定義）

| 表示名 | App ID の env | token の env | 定義モジュール |
|---|---|---|---|
| App 29 (承認キュー) | APP_APPROVAL | TOKEN_APPROVAL | hub/send_policy.py |
| App 29 (承認キュー) | APP_APPROVAL | TOKEN_APPROVAL | hub/webapp_approval_view.py |
| App 29 (承認キュー) | APP_APPROVAL | TOKEN_APPROVAL | hub/webapp_reply_view.py |
| App 29 (承認キュー) | APP_APPROVAL | TOKEN_APPROVAL | main.py |
| App 39 (自動返信停止リスト) | APP_AUTOREPLY_STOP | TOKEN_AUTOREPLY_STOP | hub/autoreply_stoplist.py |
| App 28 (チャットログ) | APP_CHATLOG | TOKEN_CHATLOG | hub/brain_sync.py |
| App 28 (チャットログ) | APP_CHATLOG | TOKEN_CHATLOG | hub/image_analysis.py |
| App 28 (チャットログ) | APP_CHATLOG | TOKEN_CHATLOG | hub/image_intake.py |
| App 28 (チャットログ) | APP_CHATLOG | TOKEN_CHATLOG | hub/webapp_case_views.py |
| App 28 (チャットログ) | APP_CHATLOG | TOKEN_CHATLOG | hub/webapp_reply_view.py |
| App 28 (チャットログ) | APP_CHATLOG | TOKEN_CHATLOG | main.py |
| App 31 (市区町村マスタ) | APP_CITY_MASTER | TOKEN_CITY_MASTER | channels/shokumu_seikyu.py |
| App 32 (同封物ブロックマスタ) | APP_ENCLOSURE | TOKEN_ENCLOSURE | channels/soufu_annai.py |
| App 32 (同封物ブロックマスタ) | APP_ENCLOSURE | TOKEN_ENCLOSURE | hub/houki_soufu.py |
| App 40 (相続放棄案件) | APP_HOUKI | TOKEN_HOUKI | hub/brain_sync.py |
| App 40 (相続放棄案件) | APP_HOUKI | TOKEN_HOUKI | hub/houki_case_store.py |
| App 33 (戸籍読解) | APP_KOSEKI_BOOK | TOKEN_KOSEKI_BOOK | hub/shokumu_plan.py |
| App 34 (人物) | APP_KOSEKI_PERSON | TOKEN_KOSEKI_PERSON | hub/person_validity.py |
| App 34 (人物) | APP_KOSEKI_PERSON | TOKEN_KOSEKI_PERSON | hub/shokumu_plan.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | channels/soufu_annai.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | hub/brain_sync.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | hub/dispatch.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | hub/heir_envelope.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | hub/houki_soufu.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | hub/return_deadline.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | hub/shokumu_plan.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | hub/webapp_case_views.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | hub/webapp_souzoku_dashboard.py |
| App 30 (発送管理) | APP_SHIPPING | TOKEN_SHIPPING | units/souzoku/zaisan_sync.py |
| App 36 (相続人) | APP_SOUZOKUNIN | TOKEN_SOUZOKUNIN | hub/heir_projection.py |
| App 36 (相続人) | APP_SOUZOKUNIN | TOKEN_SOUZOKUNIN | hub/shokumu_plan.py |
| App 36 (相続人) | APP_SOUZOKUNIN | TOKEN_SOUZOKUNIN | hub/shokumu_plan.py |
| App 36 (相続人) | APP_SOUZOKUNIN | TOKEN_SOUZOKUNIN | hub/webapp_souzoku_dashboard.py |
| App 35 (財産) | APP_ZAISAN | TOKEN_ZAISAN | hub/webapp_souzoku_dashboard.py |
| App 財産 | APP_ZAISAN | TOKEN_ZAISAN | units/souzoku/zaisan_mokuroku.py |
| App 財産 | APP_ZAISAN | TOKEN_ZAISAN | units/souzoku/zaisan_sync.py |
| App 21 (案件) | KINTONE_APP_ID | KINTONE_API_TOKEN | hub/form_link.py |
| App 21 (案件) | KINTONE_APP_ID | KINTONE_API_TOKEN | hub/hearing_update.py |
| App 21 (案件) | KINTONE_APP_ID | KINTONE_API_TOKEN | hub/image_store.py |
| App 21 (案件) | KINTONE_APP_ID | KINTONE_API_TOKEN | hub/webapp_case_views.py |
| App 25 (不動産) | KINTONE_FUDOSAN_APP_ID | KINTONE_FUDOSAN_API_TOKEN | units/souzoku/zaisan_sync.py |
| 相談カード (相続) | SOUZOKU_KINTONE_APP_ID | SOUZOKU_KINTONE_API_TOKEN | hub/shokumu_plan.py |
| 相談カード (相続) | SOUZOKU_KINTONE_APP_ID | SOUZOKU_KINTONE_API_TOKEN | hub/webapp_souzoku_dashboard.py |

### 欄（FIELD_* 定数と hub.kintone の update／create の引数）

| モジュール | 定数 | 欄名（フィールドコード） | 読取/書込 |
|---|---|---|---|
| hub/approval.py | （直書き） | 実行済み | 書込 |
| hub/brain_link.py | FIELD_UNIT | ユニット種別 | 読取 |
| hub/brain_link.py | FIELD_CASE_APP | 案件アプリID | 読取 |
| hub/brain_link.py | FIELD_CASE_RECORD | 案件レコードID | 読取 |
| hub/heir_cancel.py | （直書き） | case_record_id | 書込 |
| hub/heir_cancel.py | （直書き） | mode | 書込 |
| hub/heir_cancel.py | （直書き） | run_id | 書込 |
| hub/heir_cancel.py | （直書き） | チャネル | 書込 |
| hub/heir_cancel.py | （直書き） | チャネル固有データ | 書込 |
| hub/heir_cancel.py | （直書き） | 件名 | 書込 |
| hub/heir_cancel.py | （直書き） | 冪等キー | 書込 |
| hub/heir_cancel.py | （直書き） | 実行済み | 書込 |
| hub/heir_cancel.py | （直書き） | 方向 | 書込 |
| hub/heir_cancel.py | （直書き） | 案件レコードID | 書込 |
| hub/heir_cancel.py | （直書き） | 注記 | 書込 |
| hub/heir_cancel.py | （直書き） | 発送ステータス | 書込 |
| hub/heir_cancel.py | （直書き） | 要確認行 | 書込 |
| hub/heir_projection.py | （直書き） | heir_derivation | 書込 |
| hub/heir_projection.py | （直書き） | チャネル固有データ | 書込 |
| hub/houki_card_read.py | FIELD_UPDATED | 更新日時 | 読取 |
| hub/houki_card_read.py | FIELD_CARD | 相談カード | 読取 |
| hub/houki_card_read.py | FIELD_STATUS | 相談カード読取 | 書込 |
| hub/houki_contract.py | FIELD_CS_DOC_ID | cloudsign_document_id | 読取 |
| hub/houki_contract.py | FIELD_EMAIL | メールアドレス | 読取 |
| hub/houki_contract.py | FIELD_ADDR | 住所 | 読取 |
| hub/houki_contract.py | FIELD_CONTRACT_STATUS | 契約書ステータス | 読取 |
| hub/houki_contract.py | FIELD_RECOVERY_MEMO | 契約書回収メモ | 読取 |
| hub/houki_contract.py | FIELD_SIGN_MODE | 契約署名 | 読取 |
| hub/houki_contract.py | FIELD_TOKUYAKU | 特約 | 読取 |
| hub/houki_contract.py | FIELD_GROUP | 被相続人グループID | 読取 |
| hub/houki_contract.py | FIELD_DECEDENT | 被相続人氏名 | 読取 |
| hub/houki_contract.py | FIELD_NAME | 顧客名 | 読取 |
| hub/houki_jijou.py | FIELD_ATTACHMENT | 事情説明書 | 読取 |
| hub/houki_jukuryo.py | FIELD_STATUS | status | 読取 |
| hub/houki_jukuryo.py | FIELD_REMAINING | 残日数 | 読取 |
| hub/houki_jukuryo.py | FIELD_DEADLINE | 熟慮期間期限 | 読取 |
| hub/houki_jukuryo.py | FIELD_SUBMITTED | 申述提出日 | 読取 |
| hub/houki_jukuryo.py | FIELD_START | 起算日_確定 | 読取 |
| hub/houki_jukuryo.py | FIELD_NOTIFIED | 通知済み閾値 | 読取 |
| hub/houki_koseki_checklist.py | FIELD_STATUS | status | 読取 |
| hub/houki_koseki_checklist.py | FIELD_KANKEI | 続柄 | 読取 |
| hub/houki_koseki_checklist.py | FIELD_COMPLEX | 複雑性フラグ | 読取 |
| hub/houki_koseki_checklist.py | FIELD_CONTINUOUS | 連続戸籍充足 | 読取 |
| hub/houki_shokumu_link.py | FIELD_STATUS | status | 読取 |
| hub/houki_shokumu_link.py | （直書き） | チャネル | 書込 |
| hub/houki_shokumu_link.py | （直書き） | チャネル固有データ | 書込 |
| hub/houki_shokumu_link.py | （直書き） | ユニット種別 | 書込 |
| hub/houki_shokumu_link.py | （直書き） | 件名 | 書込 |
| hub/houki_shokumu_link.py | （直書き） | 実行済み | 書込 |
| hub/houki_shokumu_link.py | （直書き） | 方向 | 書込 |
| hub/houki_shokumu_link.py | （直書き） | 案件アプリID | 書込 |
| hub/houki_shokumu_link.py | （直書き） | 案件レコードID | 書込 |
| hub/houki_shokumu_link.py | （直書き） | 発送ステータス | 書込 |
| hub/houki_shokumu_link.py | FIELD_LAST_ADDRESS | 被相続人最後の住所 | 読取 |
| hub/houki_shokumu_link.py | FIELD_DECEDENT | 被相続人氏名 | 読取 |
| hub/houki_soufu.py | FIELD_STATUS | status | 書込 |
| hub/houki_soufu.py | （直書き） | エラー詳細 | 書込 |
| hub/houki_soufu.py | （直書き） | チャネル固有データ | 書込 |
| hub/houki_soufu.py | FIELD_ACCEPT_RECEIVED | 受理通知受領日 | 読取 |
| hub/houki_soufu.py | FIELD_ACCEPT_FILE | 受理通知書 | 読取 |
| hub/houki_soufu.py | （直書き） | 発送ステータス | 書込 |
| hub/houki_soufu.py | FIELD_GROUP | 被相続人グループID | 読取 |
| hub/houki_soufu.py | FIELD_CUSTOMER | 顧客名 | 読取 |
| hub/image_analysis.py | （直書き） | auto_sent | 書込 |
| hub/image_analysis.py | （直書き） | category | 書込 |
| hub/image_analysis.py | （直書き） | line_user_id | 書込 |
| hub/image_analysis.py | （直書き） | message | 書込 |
| hub/image_analysis.py | （直書き） | role | 書込 |
| hub/image_analysis.py | （直書き） | 問い合わせ業者名 | 書込 |
| hub/image_intake.py | （直書き） | auto_sent | 書込 |
| hub/image_intake.py | （直書き） | category | 書込 |
| hub/image_intake.py | （直書き） | line_user_id | 書込 |
| hub/image_intake.py | （直書き） | message | 書込 |
| hub/image_intake.py | （直書き） | role | 書込 |
| hub/shokumu_plan.py | （直書き） | チャネル | 書込 |
| hub/shokumu_plan.py | （直書き） | チャネル固有データ | 書込 |
| hub/shokumu_plan.py | （直書き） | ユニット種別 | 書込 |
| hub/shokumu_plan.py | （直書き） | 件名 | 書込 |
| hub/shokumu_plan.py | （直書き） | 宛先住所 | 書込 |
| hub/shokumu_plan.py | （直書き） | 宛先名 | 書込 |
| hub/shokumu_plan.py | （直書き） | 宛先郵便番号 | 書込 |
| hub/shokumu_plan.py | （直書き） | 実行済み | 書込 |
| hub/shokumu_plan.py | （直書き） | 方向 | 書込 |
| hub/shokumu_plan.py | （直書き） | 案件アプリID | 書込 |
| hub/shokumu_plan.py | （直書き） | 案件レコードID | 書込 |
| hub/shokumu_plan.py | （直書き） | 発送ステータス | 書込 |
| hub/shokumu_plan.py | （直書き） | 顧客名表示用 | 書込 |
| hub/webapp_houki_jijou_view.py | （直書き） | fileKey | 書込 |
| hub/webapp_reply_view.py | （直書き） | auto_sent | 書込 |
| hub/webapp_reply_view.py | （直書き） | category | 書込 |
| hub/webapp_reply_view.py | （直書き） | line_user_id | 書込 |
| hub/webapp_reply_view.py | （直書き） | message | 書込 |
| hub/webapp_reply_view.py | （直書き） | role | 書込 |
| main.py | （直書き） | auto_sent | 書込 |
| main.py | （直書き） | category | 書込 |
| main.py | （直書き） | line_user_id | 書込 |
| main.py | （直書き） | message | 書込 |
| main.py | （直書き） | role | 書込 |

## (g) 相談者向け文言の一覧

| 対象 | 文言 ID | 区分 | 文字数 |
|---|---|---|---|
| 1) hub/jikou_reply_texts.py（J 定型） | J-0 | 要承認 | 46 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-1 | 凍結（v1.4） | 27 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-2 | 凍結（v1.4） | 52 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-3 | 凍結（v1.4） | 46 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-4 | 凍結（v1.4） | 79 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-5a | 凍結（v1.4） | 137 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-5b | 要承認 | 78 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-6 | 要承認 | 58 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-7 | 凍結（v1.4） | 248 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-8 | 凍結（v1.4） | 60 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-9 | 凍結（v1.4） | 56 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-10 | 凍結（v1.4） | 75 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-11 | 要承認 | 72 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-12 | 凍結（v1.4） | 96 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-13 | 要承認 | 70 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-14 | 要承認 | 41 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-14b | 要承認 | 43 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-15 | 要承認 | 35 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-H1 | 要承認 | 32 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-H2 | 要承認 | 47 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-H3 | 要承認 | 46 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-H4 | 要承認 | 21 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-H5 | 要承認 | 25 |
| 1) hub/jikou_reply_texts.py（J 定型） | J-H6 | 要承認 | 24 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | CHURN_NEUTRAL_REPLY | 凍結 | 42 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | COURT_DOC_REQUEST_REPLY | 凍結 | 80 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | CRISIS_SUPPORT_REPLY | 凍結 | 75 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | FEE_GUIDE_TEXT | 凍結 | 248 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | HOME_VISIT_TEXT | 凍結 | 69 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | HOTERASU_STANDARD_REPLY | 凍結 | 96 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | IMAGE_RECEIPT_REPLY | 凍結 | 34 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | JIKOU_YEARS_TEXT | 凍結 | 103 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | MAIL_ADDRESS_TEXT | 凍結 | 62 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | OUT_OF_SCOPE_DEBT_REPLY | 凍結 | 66 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | PENDING_REPLY | 凍結 | 67 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | QUESTIONNAIRE_RETURN_TEXT | 凍結 | 74 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | SEIZURE_SCOPE_TEXT | 凍結 | 75 |
| 2) chat_responder.py 固定文・即時定型（凍結文） | URGENT_SEIZURE_PANIC_REPLY | 凍結 | 76 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #1 | 凍結（prompt・FAQ） | 20 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #2 | 凍結（prompt・FAQ） | 36 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #3 | 凍結（prompt・FAQ） | 51 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #4 | 凍結（prompt・FAQ） | 113 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #5 | 凍結（prompt・FAQ） | 66 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #6 | 凍結（prompt・FAQ） | 67 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #7 | 凍結（prompt・FAQ） | 138 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #8 | 凍結（prompt・FAQ） | 118 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #9 | 凍結（prompt・FAQ） | 180 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #10 | 凍結（prompt・FAQ） | 502 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #11 | 凍結（prompt・FAQ） | 130 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #12 | 凍結（prompt・FAQ） | 78 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ（弁護士確認済みの標準回答。数値・条件の改変禁止。自動送信可） #13 | 凍結（prompt・FAQ） | 320 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #1 | 凍結（prompt・FAQ） | 76 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #2 | 凍結（prompt・FAQ） | 41 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #3 | 凍結（prompt・FAQ） | 29 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #4 | 凍結（prompt・FAQ） | 26 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #5 | 凍結（prompt・FAQ） | 39 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #6 | 凍結（prompt・FAQ） | 44 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #7 | 凍結（prompt・FAQ） | 51 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #8 | 凍結（prompt・FAQ） | 22 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #9 | 凍結（prompt・FAQ） | 18 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #10 | 凍結（prompt・FAQ） | 61 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #11 | 凍結（prompt・FAQ） | 106 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #12 | 凍結（prompt・FAQ） | 18 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #13 | 凍結（prompt・FAQ） | 110 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #14 | 凍結（prompt・FAQ） | 24 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #15 | 凍結（prompt・FAQ） | 117 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #16 | 凍結（prompt・FAQ） | 47 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #17 | 凍結（prompt・FAQ） | 56 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #18 | 凍結（prompt・FAQ） | 18 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #19 | 凍結（prompt・FAQ） | 39 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #20 | 凍結（prompt・FAQ） | 121 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #21 | 凍結（prompt・FAQ） | 339 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #22 | 凍結（prompt・FAQ） | 41 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #23 | 凍結（prompt・FAQ） | 13 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #24 | 凍結（prompt・FAQ） | 55 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #25 | 凍結（prompt・FAQ） | 9 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #26 | 凍結（prompt・FAQ） | 35 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #27 | 凍結（prompt・FAQ） | 75 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #28 | 凍結（prompt・FAQ） | 31 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #29 | 凍結（prompt・FAQ） | 69 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #30 | 凍結（prompt・FAQ） | 24 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #31 | 凍結（prompt・FAQ） | 318 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第2弾（弁護士確定済み・2026-07-03追加。数値・条件の改変禁止。特記なき項目は自動送信可） #32 | 凍結（prompt・FAQ） | 53 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #1 | 凍結（prompt・FAQ） | 53 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #2 | 凍結（prompt・FAQ） | 55 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #3 | 凍結（prompt・FAQ） | 77 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #4 | 凍結（prompt・FAQ） | 66 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #5 | 凍結（prompt・FAQ） | 83 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #6 | 凍結（prompt・FAQ） | 67 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #7 | 凍結（prompt・FAQ） | 34 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #8 | 凍結（prompt・FAQ） | 78 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #9 | 凍結（prompt・FAQ） | 79 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #10 | 凍結（prompt・FAQ） | 46 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #11 | 凍結（prompt・FAQ） | 73 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #12 | 凍結（prompt・FAQ） | 102 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #13 | 凍結（prompt・FAQ） | 96 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #14 | 凍結（prompt・FAQ） | 153 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #15 | 凍結（prompt・FAQ） | 69 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #16 | 凍結（prompt・FAQ） | 48 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #17 | 凍結（prompt・FAQ） | 44 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #18 | 凍結（prompt・FAQ） | 38 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #19 | 凍結（prompt・FAQ） | 35 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #20 | 凍結（prompt・FAQ） | 163 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #21 | 凍結（prompt・FAQ） | 117 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #22 | 凍結（prompt・FAQ） | 94 |
| 3) 顧客対応 prompt の FAQ・費用の定型案内 | FAQ第3弾（弁護士確定済み・2026-07-03追加。数値・条件・言い回しの改変禁止。特記なき項目は自動送信可） #23 | 凍結（prompt・FAQ） | 60 |
| 4) あいさつ文（hub/shindan_link.FROZEN_GREETING） | FROZEN_GREETING | 凍結 | 86 |
| 5) 凍結ヒアリング台本（main._HEARING_PROMPT_FROZEN） | _HEARING_PROMPT_FROZEN | 凍結 | 1560 |

承認済の手本・昇格定型は DB 表（reply_exemplar・jikou_reply_text）にありコード外（目録の対象外）。
