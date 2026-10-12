# JIKOU-FAX-1 設計書: 時効援用通知書の FAX 自動送信（設計のみ・コード不変）

- 票: JIKOU-FAX-1-DESIGN（QUEUE-2026-10-13-D D-9）。本書は設計の正本案であり、実装は別票（JIKOU-FAX-1-IMPL）で行う。
- 前提: 時効援用通知書 docx は既存の `notice_webhook`（JIKOU-NOTICE-1）が App 21 の FILE 欄に生成・添付している。送信は
  現状すべて人（FAX 機・複合機）で行っている。
- 凍結事項: 承認なしの送信経路を作らない（送信は PWA の明示承認ごと）・RV-08（削除せず状態・履歴の追加）・RV-10（ログ・
  通知・画面に債権者の FAX 番号・相談者の氏名を出さない）・送信台帳（hub/send_ledger）の purpose は閉集合（CHECK 制約）。
- 「要確認」印: 外部 API の仕様・料金は本書作成時点で未確認。実装前に大野が各社の最新資料で確認する。

## 1. 目的と範囲

- 目的: 弁護士が PWA で承認した時効援用通知書（PDF）を、債権者（業者）の FAX 番号へインターネット FAX API で送信し、送達結果を
  App 21 と送信台帳に記録する。
- 範囲: 時効（App 21）のみ。相続放棄側は対象外。送信先は kintone の業者マスタに登録された FAX 番号のみ（手入力の番号へは送らない）。
- 範囲外: 受信 FAX の取込、送付状の自由文、複数宛先の一括送信（1 通知書 = 1 宛先 = 1 送信操作）。

## 2. インターネット FAX API 候補の比較（要確認）

| 候補 | API の有無 | 料金体系（要確認） | 送達通知の取得方法（要確認） | 備考 |
|---|---|---|---|---|
| 秒速FAX Plus（Toones） | REST API あり（送信・送信結果照会） | 月額基本料+送信 1 枚ごとの従量。要確認 | 送信結果の照会 API（ポーリング）。Webhook の有無は要確認 | 国内事業者・法人向け実績。PDF 送信可 |
| MOVFAX（日本テレネット） | API あり（送信）。要確認 | 月額+従量。要確認 | ステータス照会 API／メール通知。要確認 | 国内・医療機関等の実績 |
| InterFAX（Upland） | REST API あり（送信・ステータス照会・Webhook） | 従量（ページ単価・国別）。要確認 | ステータス照会 API+Webhook（送達・失敗） | 海外事業者・日本番号可。英語ドキュメント |
| eFax（j2 Global） | Developer API あり（要申請）。要確認 | 月額+枚数制限超過分従量。要確認 | API ステータス照会／メール通知。要確認 | 個人・SOHO 向けの色が強い。法人 API は要相談 |

- 比較観点（実装票で確定）: ①送信 API が PDF を直接受けるか ②送達結果が「成功／失敗／結果不明」の 3 値で機械取得できるか
  ③結果取得が Webhook かポーリングか（ポーリングなら回収ジョブに組み込む）④送信 ID（外部キー）が返るか ⑤同一送信の再送を
  API 側で冪等にできるか（できなければ台帳側で抑止）⑥番号の E.164 表記 ⑦認証（API キー）の保管（Railway env）⑧料金。
- 司令塔仮置き: **送達結果を API で取得でき、送信 ID が返る事業者を優先**（送信確認待ちの回収が自動化できるため）。
  候補は 秒速FAX Plus（国内・API）または InterFAX（Webhook あり）。最終選定は大野。

## 3. kintone

### 3.1 業者マスタ（既存アプリ・App ID/フィールドコードは実機確認＝要確認）
- 既存の `FAX番号` 欄を送信先の正とする（docs/architecture/02-kintone-design.md §業者マスタ・M3 と同じ前提）。
- 追加案: `FAX送信可`（DROP_DOWN: 可／不可／未確認・既定 未確認）。**未確認・不可には送らない**（fail-closed）。
- 番号の検証: サーバで数字・ハイフンのみ・E.164 へ正規化（国内 0 始まり → +81）。検証に落ちた番号は送らず通知。

### 3.2 App 21（時効案件）
- 既存: 通知書 FILE 欄（notice_webhook が添付）・`問い合わせ業者名`・`対象債権者2/3`（最大 3 社）。
- 追加案（債権者スロットごと 3 組）: `FAX送信状態_{1,2,3}`（DROP_DOWN: 未送信／送信待ち／送信中／送達／失敗／結果不明）・
  `FAX送信日時_{1,2,3}`（DATETIME）・`FAX送信ID_{1,2,3}`（SINGLE_LINE_TEXT・外部送信 ID）。
- 書込は「送信着手の確定」と「結果確定」の 2 回のみ（$revision CAS・409 は再取得して状態だけ再評価）。

## 4. 流れ

1. 通知書 PDF の生成: 既存 notice_webhook の docx 生成（凍結検証済み）→ PDF 変換（LibreOffice headless または既存の PDF 生成部品〔CONTRACT-GEN-2 の方式〕）。PDF にも凍結検証（docx の期待段落との一致は docx 側で済・PDF はページ数と文字列抽出でプレースホルダ残存なしを検査）。
2. PWA 承認（/app/fax・新画面・認証 _gate）: 案件 No.・債権者スロット・宛先（**業者名と番号の末尾 4 桁のみ表示**）・PDF プレビュー・
   明示確認チェック → 「FAX を送信する」。操作 ID・宛先ハッシュ・App 21 revision を署名付き ticket で束縛（Q1b の返信欄と同型）。
3. 送信着手: 送信台帳 `begin(business="jikou", channel="fax", to=<業者マスタ record_id>)`（主体 human・用途 `fax`〔§6 の
   migration 要〕・受信イベント ID = `fax:{App 21 No.}:{slot}:{PDF sha256 先頭 16}`）。重複（sent／unconfirmed）は送らない。
   着手確定後に App 21 を `送信中` に CAS 更新。
4. API 送信: guarded_http（heartbeat・timeout）で API を 1 回だけ呼ぶ。送信 ID を App 21 と台帳（text_version 相当の欄）に記録。
   通信例外・timeout は `unconfirmed`（自動再送なし・/app/send_ops に出る）。
5. 送達結果: Webhook（あれば）またはポーリング回収ジョブ（10 分ごと・最長 24 時間）で 成功／失敗／結果不明 を取得し、
   台帳 `finish(sent|failed)`（applied のときだけ App 21 を `送達`／`失敗` に更新）。24 時間を超えて不明なら `結果不明` として
   人の確認（/app/send_ops の確認操作）に回す。
6. 記録: App 28 には書かない（相談者宛ではない）。業務 LINE へ「送達／失敗」の固定文言（案件 No.・スロット・結果のみ）。

## 5. 失敗時の扱い

| 事象 | 扱い |
|---|---|
| 番号未登録・検証落ち・FAX送信可≠可 | 送らず画面に固定文言。台帳に行を作らない |
| 台帳 begin が None（DB 不可） | 送らない（fail-closed・Q1b の明示送信と同じ） |
| API が 4xx（番号不正・認証） | `failed`・App 21 失敗・業務 LINE 固定文言。再送は人が画面から新しい操作として行う |
| API が 5xx／timeout | `unconfirmed`・自動再送なし・回収ジョブは結果照会のみ |
| 送達結果が「話中・応答なし」 | API 側の自動リトライに任せるか（要確認）、失敗として人が再送 |
| 同じ PDF・同じ宛先の二重操作 | 受信イベント ID（PDF sha256 を含む）で台帳が重複検出 → 送らない |
| 通知書 PDF が変わった（再生成） | sha が変わるので別操作として送れる（前回の送達記録は残る＝RV-08） |

## 6. 送信台帳の変更（migration 要）

- 新チャネル `fax`（channel 列は自由文字列・変更不要）。
- purpose の閉集合に `fax` を追加（migration: `ck_send_operation_purpose` の CHECK を更新・既存行は不変・downgrade は fax 行が
  あれば拒否）。それまでは purpose=other+接頭辞 `fax:` で代替可能（HOUKI-JUKURYO-2 と同じ暫定方式）だが、/app/send_ops の
  表示と回収ジョブの分岐のために正式追加を推奨。
- `send_operation.text_version` には PDF の sha256 を入れる（本文ではない）。

## 7. RV-10（秘匿）

- ログ: 案件 No.・スロット番号・固定語彙・件数のみ。FAX 番号・業者名・相談者氏名を載せない。
- 画面: 番号は末尾 4 桁のみ。PDF プレビューは認証後のみ（キャッシュ no-store）。
- 通知（業務 LINE）: 案件 No.・スロット・結果のみ。
- 外部 API への送信内容: PDF と番号のみ（相談者の氏名は PDF 内に含まれる＝送信先は債権者であり業務上必要）。API キーは env。
- 台帳: 宛先は業者マスタの record_id（番号そのものを置かない）。

## 8. Codex 審査観点（実装票の PR 本文に転記）

1. 承認なしの送信経路が無いこと（API 呼出は PWA の POST ハンドラからのみ・ジョブは結果照会のみ）。
2. 二重送信なし（操作 ID・受信イベント ID・台帳の重複検出・CAS）。
3. fail-closed の各経路（番号・台帳・API 4xx/5xx・結果不明）で送信ゼロまたは unconfirmed に倒れること。
4. RV-10（番号・氏名がログ・通知・画面に出ない）。sink 検査ゼロ・allowlist 差分ゼロ。
5. migration の線形性と downgrade 拒否。
6. 既存 notice_webhook の生成・凍結検証が不変（PDF 変換は後段で分離）。
7. 外部 API の障害時に時効側の他経路（LINE 返信・承認 webhook）へ影響しないこと（timeout・別タスク）。

## 9. 保留（大野）

- 事業者の選定と契約（§2）。API キーの Railway env 名（例 `FAX_API_KEY`・`FAX_API_BASE`）。
- 業者マスタの App ID・`FAX番号` の実フィールドコード・`FAX送信可` 欄の追加。App 21 の 9 欄追加。
- purpose `fax` の migration 方針（正式追加か暫定 other）。
- PDF 変換部品の選定（既存 CONTRACT-GEN-2 の方式を流用するか）。
