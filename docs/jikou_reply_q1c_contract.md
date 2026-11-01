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

## 2b. 空返事の分類（fix2 BC-10）

- `hub/empty_reply_guard.empty_reply_labels` は、空文字・空白（半角・全角 U+3000・改行）だけの返事を「空の返信」、**基底文字（Letter／Number）
  が 1 つも無い返事**（句読点 。、…！？ の全半角・記号・区切り・制御／書式文字・結合記号だけの組合せ。例 `。。。`／`！？`／`…`）を「記号・空白のみ」
  （`LABEL_SYMBOLS_ONLY`）に分類する（純関数・本文は返さない）。
  - **fix3 BC-16**: 内容ありの根拠は基底文字の有無だけ。結合記号（Unicode Mark・異体字セレクタ U+FE0F・単独の結合濁点・ZWJ）は単独では
    内容としない＝`☀`／`☀️`（☀ + U+FE0F）／単独 U+FE0F は記号・空白のみ、結合記号を伴う通常の日本語は内容あり（4 対を `test_jikou_reply_q1c_fix3` が固定）。
- **V2 ON**: 顧客対応（`apply_v2`）・ヒアリング（`empty_reply_violations`）の双方で降格＝顧客送信ゼロ・対応待ち（reason=empty_reply／
  ヒアリングは降格理由に空返事）。J-0 の条件を満たせば J-0 だけを送る（記号の返事は送らない）。
- **V2 OFF**: 従来どおり（空文字だけ降格・記号のみは語彙表と同じく適用しない＝既存経路の挙動不変）。

## 3. 無言にした受信の回収（fix1 BC-03）

- 対応待ち（`reply_pending`）の保存（`register`）に失敗した受信は、**受信イベント ID を鍵に永続キュー `reply_pending_retry`**
  （migration `d4f6a8b0c2e4`・a1c3 の次）へ閉集合値（route／reason／宛先ハッシュ／カテゴリ／App 29 No.・本文なし）だけ置く。
  回収ジョブ（`send_ledger` の recover hook・`send_policy.run_pending_retry_job`・SEND_LEDGER_RECOVER の間隔）が次回以降に
  register → 業務 LINE 通知 を再処理する（再配送は冪等・上限 20 回で abandoned・行は残す＝RV-08）。
  受信イベント ID が無い経路（非 durable）と DB 全停止中は永続化できない（固定語彙ログ＋§1 の障害通知・App 28 の受信記録で人が回収）。
- 通知の失敗（notify_state=failed／unavailable）は同じジョブが **台帳 key（登録日の `pending_notice:{会話 ref}:{JST 日付}`）** で
  再試行する＝送れても 1 通（DUPLICATE_SENT → throttled）。上限 `NOTIFY_RETRY_MAX`（3）回。
- **fix2 BC-07（通知キーの永続化と claim・migration `b7d9f1a3c5e7`）**: 通知キーは **登録時に 1 度だけ算出**して
  `reply_pending.notice_key` に保存する（register に失敗したときは永続キュー `reply_pending_retry.notice_key` に置き、回収ジョブの
  register がそのまま引き継ぐ）。以後の通知（再試行・再配送・キュー経由）はすべて保存値で行う＝会話参照の取得障害でハッシュ形へ
  落ちる／キュー作成日と登録日が JST の日付をまたぐ、でも key は変わらず 2 通目は出ない。通知処理は
  `reply_pending.claim_notify`（notify_owner／notify_claimed_at の条件付き更新・リース 300 秒）を取れた 1 者だけが進み、
  `set_notify_state` が解放する（通知済み sent／throttled／unconfirmed の行は claim できない＝再通知しない）。
  列追加前の旧行（notice_key NULL）だけ登録日の key を算出して保存してから使う。
- **fix3 BC-13**: 送信に使う通知キーは **claim と同一トランザクションで読んだ永続キーだけ**（`reply_pending.claim_notify` が
  (result, key) を返す）。旧行の NULL 埋めも同一 tx（既に値があれば上書きしない）。読取・更新の例外は "" に潰さず送出し、rollback で
  claim も残らない＝送らずに回収へ戻す（`notice_key_of` も例外を送出・送信経路では使わない）。計算値が送信に渡ることはない。
- **fix3 BC-14**: 結果（notify_state）・試行数（attempts）・claim 解放は **pending_id AND notify_owner = 自分** の条件付き更新
  （`set_notify_state(owner=)`／`bump_attempts(owner=)`）。リース切れ後に他者が claim していれば 0 件＝遅着として無変更（履歴も書かない）。
  - **fix4 BC-17**: 両関数は **owner 必須**（省略・None は TypeError）。未所有行（notify_owner IS NULL）への管理操作は
    `set_notify_state_unowned`／`bump_attempts_unowned`（所有中の行は触らない）に分け、通知経路では使わない。
- **fix3 BC-15（未通知行の回収）**: 回収ジョブ（`run_pending_retry_job`）の通知対象に **open かつ notify_state=none で未所有またはリース切れ**
  の行を加える（`reply_pending.list_notify_retry`）＝キューからの登録（retry_done）直後の停止や初回 claim の DB エラーで通知に至らなかった
  対応待ちを次回以降に拾う。送信は保存キーで送信台帳と照合（begin の重複判定）してから行い、送信済みなら 2 通目を出さず throttled を記録する。
  **司令塔裁定: この回収は業務 LINE 通知だけ**で、顧客向け送信・J-0（customer_send・台帳の first_reply 在庫）には一切触れない
  （`test_jikou_reply_q1c_fix3.TestRecoverUnnotifiedPending` が固定）。
- **未返信の経過通知**: open のまま `REPLY_PENDING_AGE_MIN` 分（既定 60・env で上書き可・**要大野確認**）を超えた対応待ちを、
  業務 LINE へ **1 日 2 回**（JST `AGE_NOTICE_HOURS_JST`＝9 時・15 時・**要大野確認**・hub/scheduler の daily・HOUKI 系ジョブと同じ流儀）
  まとめて通知する。件数と経過分・再処理待ち件数のみ（PII なし）。台帳 key `pending_age:{JST 日付}:{時}` で 1 枠 1 通。
  対象 0 件（再処理待ちも 0）なら送らない。
  - **fix2 BC-08（司令塔裁定: 業務 LINE 向け経過通知も approved_texts 経由・例外なし）**: 本文は承認台帳
    `hub/approved_texts.APPROVED_SHA256` に載った定型 **`ops_age_notice_v1`**（`send_policy.AGE_NOTICE_TEMPLATE`）だけ。文字列組立は
    せず、数値スロット（{{open}}／{{threshold}}／{{over}}／{{oldest}}／{{queued}}）だけ差し込む。**未承認の間は送らない**
    （`run_age_notice_job` の notice=`needs_approval`・台帳行なし・未承認 key は approved_texts の仕組みで 1 日 1 回だけ知らせる）。
    文面と SHA は `pgv/q1c_fix2_texts_for_approval.md`（大野承認待ち）。`hub/approved_texts.py` は TEXT-APPROVAL-GATE-1（`0e80ea6`）
    と同一内容を q1c に置いた（同じ blob＝別 PR の merge で衝突しない・台帳の追加は司令塔の票でのみ）。
- **V2 ON の条件**: 本節の結線（永続キューの migration 適用・回収ジョブの登録・経過通知のジョブ登録）が完了していること。
  未完了のまま ON にすると、保存失敗した受信が回収されず無言のまま残る。

## 4. 解除の単位（fix1 BC-04）

- 承認済み下書き・大野の明示送信とも、**承認・送信着手の時点で返信対象の pending_id 集合を固定**し（`send_policy.pending_targets`／
  `fixed_targets`）、送信成功（台帳 finish=applied）のときに **その集合だけ** を human_replied にする（`reply_pending.resolve_ids`・
  open のものだけ・同じ相談者の行に限る）。送信中に届いた受信は open のまま（次の返信で閉じる）。
- bot の自動送信は対応待ちを閉じない（R-J14・不変）。画面の「不要にする」は 1 件ずつ（不変）。
- **fix2 BC-09（0 件と読取失敗の区別）**: `send_policy.pending_targets` は 0 件を `()`、**DB 読取失敗を None** で返す（送信着手前に
  1 回だけ即時再試行し、それでも読めなければ None）。None のときは **送信を進めない**（解除対象が分からないまま送ると既存の対応待ちが
  未解決のまま残るため）。返信欄（/app/reply）は `pending_unavailable` で戻し回収行は prepared のまま（同じ操作で再開可）。承認 webhook
  （V2 ON）は `fixed_targets` が印を立て、`hub/line_channel._begin_with_policy` が `PolicyStopped(("pending_targets:unavailable",))`
  に倒す（LINE write 0・台帳行なし・承認は送信済み=no のまま・業務 LINE へ固定文言）。再試行で読めれば、着手時点の集合だけを閉じ、
  送信中に届いた受信は open のまま（BC-04 と同じ）。

## 5. J-0（初回 1 通）の記録（fix1 BC-05）

- `customer_send` の閉集合: none／**j0_sent**（台帳 finish(sent)=applied＝送信フックが `SEND_SENT` を返したときだけ）／
  **j0_failed**（LINE 非 2xx）／**j0_unconfirmed**（通信例外・late／skipped・戻り値なし）／**j0_stopped**（許可表・在庫検査・台帳不可で
  送らなかった）。送信前に j0_sent を立てない。
- App 28（チャットログ）への assistant 行の写しは **確定した文（sent）だけ**（`send_policy.ack_recorded`）。失敗・結果不明・停止では
  写さない（顧客に届いていない文を記録しない）。
- main の送信ラッパー `_line_reply_with_fallback` は `reply_with_push_fallback` の 3 値（＋policy_stopped）を呼出元へ返す。
- 1 回性の正は送信台帳（purpose=first_reply の在庫検査・排他の中）であり、customer_send は表示用の状態。
- **fix2 BC-11（司令塔裁定・単位の訂正）**: J-0 は **「業務 × LINE 利用者」単位で 1 通**。会話単位ではない。送信台帳の在庫検査
  （`send_ledger._first_reply_count`・送信前の `first_reply_due` と begin() の排他の中の両方）は `conversation` を join して
  **同じ利用者（line_user_hash）の全会話**の「初回返信判定の対象」操作を数える＝**30 日無受信後の新会話でも J-0 は再送しない**
  （`test_jikou_reply_q1c_fix2.TestJ0PerUser` が固定）。表・列の変更は無く migration 不要（§7）。会話を持たない操作（受信前の送信）は
  利用者に結び付けられないため数えない。従来の記述「その会話に…」「1 会話 1 回」は本訂正に読み替える。
- **fix2 BC-06**: `reply_with_push_fallback` が `SEND_SENT` を返すのは **台帳 finish() が applied**（または台帳で sent 確定済みの重複
  `DUPLICATE_SENT`）のときだけ。HTTP 2xx でも finish が late／skipped（遅着・確定失敗）なら `SEND_UNCONFIRMED` → `j0_unconfirmed`・
  App 28 へ写さない（台帳は started のまま＝回収ジョブが unconfirmed に移し、人が確定する）。記録なし（op None＝V2 OFF の fail-open・
  他業務）は従来どおり `SEND_SENT`。main の実コールバック（顧客対応・障害応答の `_reply_func`）は戻り値を返す
  （`test_jikou_reply_q1c_fix2` が `main._process_line_event` 経由で固定）。

## 6. 凍結の扱い（司令塔裁定）

- 顧客対応 prompt（`chat_responder._SYSTEM_PROMPT_BASE`【会話スタイル】）への「答えられない問いは reply を空文字にする」の **1 行追加と
  その SHA pin 更新**（test_autoreply_style1 `customer_prompt_base`・test_business_profile の集約 sha）は、大野裁定（Q1c: 確認します系の廃止・
  無言）に基づく **限定的な凍結解除** として承認済み。FROZEN_SHA の他の項目（ヒアリング prompt・PENDING_REPLY・即時定型・法テラス・
  費用固定文）は不変。
- main の JIKOU-FAQ-ADDON-PROMPT-1（`67f992e`・QUEUE-2026-10-11-B Q-4・司令塔票で承認）による FAQ「完了後の追加依頼の費用」行への「固定文を必ず含める」1 文と、上記の 1 行追加は、main `b8ab474` 同期 merge（JIKOU-REPLY-Q1c-sync-main・2026-10-31）で同じ prompt 本文に共存する。**両方とも承認済みの凍結解除**であり、pin は両方を含む本文で再計算（customer_prompt_base `908f9335…`・集約 sha `ce329d3c…`）。他の凍結項目は不変。
- J-0 文言（`send_policy.J0_TEXT`・sha pin）は司令塔仮置き・**大野承認待ち**（fix2 時点でも未承認のまま）。
  **fix2 BC-11（凍結記録）: J-0 文言の大野承認は V2 有効化前の条件**（`JIKOU_SEND_POLICY_V2` を ON にする前に承認を得る）とする。承認時は
  `J0_TEXT` と test の sha pin を同時に更新する（変更が無ければ pin はそのまま）。

## 7. migration（fix1 BC-02）

- 鎖は 1 本: e5f8（送信台帳）→ f7b2（手本）→ a7c9（create_state）→ c8e0（row_version）→ **a1c3**（対応待ち・Q1b-fix5／fix6 で
  down_revision を c8e0 に付け替え）→ **d4f6**（永続キュー・fix1）→ **b7d9**（fix2 BC-07: `reply_pending.notice_key`／
  `notify_owner`／`notify_claimed_at`・`reply_pending_retry.notice_key` の列追加）。`alembic heads` は b7d9 のみ。
  票の「c8e0 の次」は main の head（c8e0）に対する q1c の鎖の意で、q1c 内では d4f6 の後ろに置く（鎖を 1 本に保つ）。
- downgrade は a1c3（対応待ち・履歴）・d4f6（永続キュー）・b7d9（notice_key に値がある行）とも **データがあれば拒否**（RV-08・値は出さない）。
- 本番適用は merge 前に `alembic upgrade head`（大野・保留一覧）。Q3a 以降の migration（b2d4 …）は前方 merge 時に b7d9 の後ろへ付け替える。
- fix2 BC-11（J-0 の 利用者単位）は送信台帳の **読取条件の変更だけ**（conversation を join して line_user_hash で数える）で、
  表・列の変更は無く migration は不要。
