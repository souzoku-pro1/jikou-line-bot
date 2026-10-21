# JIKOU-REPLY-Q1b 契約（明示送信・台帳・後処理）

対象: `hub/webapp_reply_view.py`（/app/reply）、`hub/send_ledger.py`（台帳）、`hub/reply_followup.py`（回収表）、
`hub/reply_exemplar.py`（手本）、`hub/pii_scan.py`（匿名化・残留検査）。
正本は 時効LINEボット_返信規則_v1.4.md §10・§12 Q1b。本書は Codex R-JIKOU-REPLY-Q1b-1／R-Q1b-fix1 の指摘で
明文化した **実装契約** で、正本と矛盾する場合は正本が勝つ。

## 1. 台帳（send_operation）における人の返信の識別

- 人の明示送信は send_ledger に **purpose=reply・actor=human** の送信操作記録を作る（bot の送信と列で区別。許可表 human 列は
  `confirm` があるときだけ allow）。
- 冪等性の単位は **操作（op_id）** である（司令塔裁定・fix2）。op_id は画面を開いたときに発行され署名付き ticket に束縛される。
  同じ op_id の再 POST（二重クリック・戻る→再送・並行）は 1 送信（台帳の op_id 一意＋条件付き UPDATE）。
- **同一 inbound_event_id に対する人の別操作（別 op_id）は別送信として扱う**（人の返信の inbound_event_id は NULL。受信 1 件に
  人が 2 回返信することは正当な業務で、台帳は止めない）。
- 送信の宛先・本文は画面表示時のものに束縛する（ticket の宛先ハッシュ・revision、finish 時の本文ハッシュ照合）。ずれたら送らない。
- 台帳が使えない（DB 未設定・begin 失敗・重複照会失敗）ときは **送らない**（fail-closed・bot 経路の fail-open は不変）。

## 2. 送信後の後処理と回収表（reply_chatlog_followup）

- 回収表は送信操作記録の付帯情報（op_id 主キー）。fix2 BR-12 で **送信の前に prepared** として置く: 本文・本文ハッシュ
  （text_version）・宛先ハッシュ（line_user_hash）・op_id・後処理状態。prepared を置けないときは台帳と同じく送らない。
- 状態（閉集合）: prepared → pending（送信済み・App 28 未記録）／unconfigured（App 28 未設定）→ in_progress（回収中）→ done。
  closed（未送信で終端: LINE 失敗・台帳 failed・台帳行なし）。exhausted（失敗が上限回数）。終端行は消さない（RV-08）。
- App 28（チャットログ）の human 行は本文欄の末尾にマーカー `op_{op_id}` を 1 行で含めて書く（BR-07 のマーカー方式の流用・新欄なし。
  fix4 で旧 `[op:{op_id}]` から改めた: kintone のクエリ `like` は単語検索で `[ ] : %` 等の記号はキーワードから除かれ、`_` は単語の
  一部とみなされるため、英数字と `_` だけの 1 単語にした）。画面の履歴表示とモデル入力（`chat_responder.model_content`）では除去する。
- 回収（fix2 BR-11）: App 28 書込の前に **条件付き UPDATE で in_progress を取る**（owner_token・deadline_at）。取れなければ
  何もしない。deadline を過ぎた in_progress は再取得できる。作成の前に op_id マーカーで App 28 を検索し、既存があれば作成せず
  done（成功応答喪失の修復）。
- 照合の 4 値（fix4 BR-13）: `find_human_chatlog` は found／absent／unavailable／unconfigured を返す。like の結果は本文末尾の等価照合
  （`marker_matches`）まで行って found とする。**unavailable（検索失敗）は不存在ではない**: 作成せず pending（last_result=
  lookup_unavailable）に残して次回へ。保留（lookup_unavailable／unknown）がある日は業務 LINE へ 1 日 1 回通知する（台帳 key
  `reply_followup_hold:{JST 日付}`・件数のみ）。
- 所有権の維持と再確認（fix4 BR-14）: 回収中は heartbeat（HEARTBEAT_SECONDS ごと・所有者のみ）で deadline を延ばす。外部作成の
  **直前**に同一 tx の条件付き UPDATE で「まだ owner か」を再確認し（`confirm_owner_before_create`・last_result=creating を置く）、
  失われていれば作成しない。作成呼出後に応答を失った行は last_result=**unknown** として pending に残し、**期限切れだけを根拠に
  再作成しない**（op_id 照合で既存が見つかるまで作成しない。creating のまま期限切れになった行も同じ）。
- 再 POST と prepare（fix4 BR-15）: 同 op_id の既存行がある prepare は 業務・宛先ハッシュ・本文ハッシュ・状態 を照合し、
  未送信を確実に確認できる（台帳に行なし・回収行 prepared・ハッシュ一致）ときだけ同一 tx で再準備して送信可（reprepared）。
  終端（done／closed／exhausted）・送信済み・不一致・台帳行ありは rejected＝送らず「新しい操作でやり直し」。台帳 begin が失敗した
  経路では回収行を閉じず prepared のまま残す（本文保持）。
- 突合（fix2 BR-12）: 回収ジョブ（SEND_LEDGER_RECOVER に相乗り）は prepared／pending／unconfigured／期限切れ in_progress を
  台帳の状態と突合する——台帳 sent → 記録、台帳 unconfirmed／started → 人の確定（confirm_by_human）を待つ、台帳 failed または
  台帳行なし → closed。自動再送はしない。
- 同じ op_id の再 POST は「送信済み・記録のみ再試行」（LINE には触れない・同じ所有権規則）。
- fix3（PRE-REVIEW-1 #272-1）: 送信直後の主経路の App 28 書込も **同じ関門**（claim → op_id 照合 → 書込 → done）を通す。
  回収ジョブと主経路が同じ op を同時に扱っても所有権を取れた側だけが作成する。prepared 行は作成から 600 秒は台帳行が無くても
  閉じない（prepare → begin の窓）。exhausted でも本文は保持しない。

## 3. 手本（reply_exemplar）

- 保存は匿名化（pii_scan）＋残留検査に通ったものだけ。残留検査は fix2 BR-10 で 住所（都道府県・市区町村・丁目／番／号・ハイフン
  数字）・電話・メール・LINE ID 様・6 桁以上の数字列・続柄＋氏名・敬称なし氏名候補 を含む（誤検知許容・安全側）。
- 保存時の状態は **pending_review**（approved_by／approved_at は NULL）。自動マスクだけで承認済みにしない。
- 下書き参照（list_exemplars＝Q3a 以降の参考文例添付・Q2 選択器・Q3b 下書き）は **status=approved のものだけ**。
- approved への遷移は人の明示操作（approve_exemplar）のみ。Q1b 単体には遷移の画面が無い（Q3a の承認画面が接続する）。

## 4. migration

- Q1b の migration は **e5f8a1b4c7d9 → f7b2c9d4e1a6 の 1 段** だけ（reply_exemplar・reply_exemplar_history・
  reply_chatlog_followup）。f7b2 は本番未適用のため fix1・fix2 とも同一 revision を更新している（merge 前に本番 current が
  f7b2 でないことを確認）。
- a1c3・b2d4 は後続票（Q1c 以降）の revision であり、本票の範囲外。
- downgrade はデータがあれば拒否（RV-08）。

## 5. 不変条件（RV）

- RV-08: 物理削除なし（手本は retired、回収行は終端状態で残す）。
- RV-10: ログは固定語彙＋件数。LINE userId・本文・氏名をログ・URL・通知に出さない。
- allowlist（redaction_sink_allowlist.json）追加ゼロ。承認なし送信経路なし。
