# JIKOU-REPLY-Q1c 契約（許可表 V2・無言化・J-0・対応待ち）

対象: `hub/send_policy.py`（許可表 V2 の適用・J-0・対応待ちの結線・回収ジョブ）、`hub/reply_pending.py`（対応待ち・永続キュー）、
`hub/empty_reply_guard.py`（空返事の検出）、`hub/webapp_pending_view.py`（/app/pending）、`hub/line_channel.py`（送信前フック）、
`hub/send_ledger.py`（begin() 内の許可表評価・J-0 の在庫検査）。
正本は 時効LINEボット_返信規則_v1.4.md §1 R-J1／R-J2／R-J10／R-J11／R-J14・§10-2・§10-5・§10-6・§12 Q1c。本書は Codex R-Q1c
（BC-01〜05）の指摘で明文化した **実装契約** で、正本と矛盾する場合は正本が勝つ。flag `JIKOU_SEND_POLICY_V2`（既定 OFF）が
ON のときだけ有効な事項は「V2 ON」と明記する。

## 1. fail-closed の範囲（fix1 BC-01）

- **V2 ON** の顧客向け bot 経路（返信・画像受領・J-0）と承認済み下書き経路は、**会話状態（attending）または送信台帳が確認できない
  ときは送らない**。
  - `send_ledger.begin()` が None（DB 未設定・記録失敗・会話行の読取失敗）を返したとき、`hub/line_channel._begin_with_policy` は
    `PolicyStopped(("ledger:unavailable",))` に倒し、2 プリミティブは `SEND_POLICY_STOPPED` を返す（LINE write 0・台帳行なし）。
  - 受信入口の `send_policy.bot_stop_reasons()` は会話状態の取得失敗を `attending:unknown`（停止）として扱う＝顧客へは無言・
    App 28 の受信記録のみ・対応待ちは登録できれば登録（できなければ §3 の永続キューへ・それも書けなければ固定語彙ログ）。
  - 業務 LINE へ **障害通知 1 日 1 回**（台帳 key `policy_outage:{JST 日付}`・`send_policy.outage_stop`）。台帳が使えないときは
    固定語彙ログのみ（通知は復旧後の最初の停止で 1 回）。
- **Q1a の fail-open**（begin() が None でも送る）は **V2 OFF の経路・時効以外の業務・業務通知チャネル** に限る。V2 ON でも
  大野の明示送信（actor=human）は Q1b の fail-closed（`push_text_explicit`）のまま。
- 承認済み下書きが台帳不可で止まったときは送信済み=no のまま（解除後の再承認で送れる）・kintone lane は sending→done
  （`skip_policy_v2`）・業務 LINE へ固定文言（1 承認版 1 通・台帳不可なら固定ログのみ）。

## 2. 停止条件の 3 値と対応中（Q1c 本体・変更なし）

- bot: AUTOREPLY_PAUSED・App 39 停止リスト（照会失敗＝unknown＝停止）・App 21 response_mode（照会失敗＝unknown）・対応中
  （attending・会話行から排他の中で読む）。承認済み下書きは会話版・承認版の一致を App 29 で持てないため `versions_match=False`
  （人対応中・対応中は送らず通知し、再承認待ち）。
- **LINE アプリ（LINE Official Account Manager）から弁護士が直接返信した場合は attending を検出できない**（本サーバを経由しない）。
  これは仕様。対応中にしたいときは返信欄（/app/reply）から送るか、将来の検出手段（手動返信を webhook で拾えるか）は別票で調査。

## 3. 無言にした受信の回収（fix1 BC-03）

- 対応待ち（`reply_pending`）の保存（`register`）に失敗した受信は、**受信イベント ID を鍵に永続キュー `reply_pending_retry`**
  （migration `d4f6a8b0c2e4`・a1c3 の次）へ閉集合値（route／reason／宛先ハッシュ／カテゴリ／App 29 No.・本文なし）だけ置く。
  回収ジョブ（`send_ledger` の recover hook・`send_policy.run_pending_retry_job`・SEND_LEDGER_RECOVER の間隔）が次回以降に
  register → 業務 LINE 通知 を再処理する（再配送は冪等・上限 20 回で abandoned・行は残す＝RV-08）。
  受信イベント ID が無い経路（非 durable）と DB 全停止中は永続化できない（固定語彙ログ＋§1 の障害通知・App 28 の受信記録で人が回収）。
- 通知の失敗（notify_state=failed／unavailable）は同じジョブが **台帳 key（登録日の `pending_notice:{会話 ref}:{JST 日付}`）** で
  再試行する＝送れても 1 通（DUPLICATE_SENT → throttled）。上限 `NOTIFY_RETRY_MAX`（3）回。
- **未返信の経過通知**: open のまま `REPLY_PENDING_AGE_MIN` 分（既定 60・env で上書き可・**要大野確認**）を超えた対応待ちを、
  業務 LINE へ **1 日 2 回**（JST `AGE_NOTICE_HOURS_JST`＝9 時・15 時・**要大野確認**・hub/scheduler の daily・HOUKI 系ジョブと同じ流儀）
  まとめて通知する。件数と経過分・再処理待ち件数のみ（PII なし）。台帳 key `pending_age:{JST 日付}:{時}` で 1 枠 1 通。
  対象 0 件（再処理待ちも 0）なら送らない。
- **V2 ON の条件**: 本節の結線（永続キューの migration 適用・回収ジョブの登録・経過通知のジョブ登録）が完了していること。
  未完了のまま ON にすると、保存失敗した受信が回収されず無言のまま残る。

## 4. 解除の単位（fix1 BC-04）

- 承認済み下書き・大野の明示送信とも、**承認・送信着手の時点で返信対象の pending_id 集合を固定**し（`send_policy.pending_targets`／
  `fixed_targets`）、送信成功（台帳 finish=applied）のときに **その集合だけ** を human_replied にする（`reply_pending.resolve_ids`・
  open のものだけ・同じ相談者の行に限る）。送信中に届いた受信は open のまま（次の返信で閉じる）。
- bot の自動送信は対応待ちを閉じない（R-J14・不変）。画面の「不要にする」は 1 件ずつ（不変）。

## 5. J-0（初回 1 通）の記録（fix1 BC-05）

- `customer_send` の閉集合: none／**j0_sent**（台帳 finish(sent)=applied＝送信フックが `SEND_SENT` を返したときだけ）／
  **j0_failed**（LINE 非 2xx）／**j0_unconfirmed**（通信例外・late／skipped・戻り値なし）／**j0_stopped**（許可表・在庫検査・台帳不可で
  送らなかった）。送信前に j0_sent を立てない。
- App 28（チャットログ）への assistant 行の写しは **確定した文（sent）だけ**（`send_policy.ack_recorded`）。失敗・結果不明・停止では
  写さない（顧客に届いていない文を記録しない）。
- main の送信ラッパー `_line_reply_with_fallback` は `reply_with_push_fallback` の 3 値（＋policy_stopped）を呼出元へ返す。
- 1 回性の正は送信台帳（purpose=first_reply の在庫検査・排他の中）であり、customer_send は表示用の状態。

## 6. 凍結の扱い（司令塔裁定）

- 顧客対応 prompt（`chat_responder._SYSTEM_PROMPT_BASE`【会話スタイル】）への「答えられない問いは reply を空文字にする」の **1 行追加と
  その SHA pin 更新**（test_autoreply_style1 `customer_prompt_base`・test_business_profile の集約 sha）は、大野裁定（Q1c: 確認します系の廃止・
  無言）に基づく **限定的な凍結解除** として承認済み。FROZEN_SHA の他の項目（ヒアリング prompt・PENDING_REPLY・即時定型・法テラス・
  費用固定文）は不変。
- J-0 文言（`send_policy.J0_TEXT`・sha pin）は司令塔仮置き・**大野承認待ち**。

## 7. migration（fix1 BC-02）

- 鎖は 1 本: e5f8（送信台帳）→ f7b2（手本）→ a7c9（create_state）→ c8e0（row_version）→ **a1c3**（対応待ち・Q1b-fix5／fix6 で
  down_revision を c8e0 に付け替え）→ **d4f6**（永続キュー・fix1）。`alembic heads` は d4f6 のみ。
- downgrade は a1c3（対応待ち・履歴）・d4f6（永続キュー）とも **データがあれば拒否**（RV-08・値は出さない）。
- 本番適用は merge 前に `alembic upgrade head`（大野・保留一覧）。Q3a 以降の migration（b2d4 …）は前方 merge 時に d4f6 の後ろへ付け替える。
