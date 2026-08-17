<!-- ============================================================
     収載注記（repo収載時に付加した部分。ここから下のマーカー行までは
     原文ではない。原文はマーカー行の直後から末尾までである）
     ============================================================ -->

> **【収載注記】**
> 本文書は repo 外にあった正本の写しである。
>
> - 原本ファイル名: 202608_longterm_PWA_product_system_design_master_v2_4.md
> - 原本の表題: 2026年8月以降 長期運用PWA・業務基盤 製品設計完全版
> - 作成/改訂日: 作成日 2026-07-10（document_id: PWA-PRODUCT-SYSTEM-MASTER-2026-08 / version: 2.4-part-time-operations）
> - 原本 SHA-256: `ce8dea9d1a8266562c24c49f440c8dca0f898a5d63e0c8695200054ef47cb9f8`
> - repo 収載日: 2026-08-13（第5セッション）
> - 以後の変更は司令塔の裁定として下記「改定記録」節へ追記し、**原文は改変しない**。
>
> **改定記録**
>
> | 日付 | 裁定者 | 変更内容 |
> |---|---|---|
> | 2026-08-13 | — | 初版収載（原文無改変） |

<!-- ORIGINAL-TEXT-BELOW / 以下、原文を一字一句そのまま収載 -->
# 2026年8月以降 長期運用PWA・業務基盤 製品設計完全版

- 作成日: 2026-07-10
- document_id: PWA-PRODUCT-SYSTEM-MASTER-2026-08
- version: 2.4-part-time-operations
- status: READY_FOR_HUMAN_REVIEW（大野承認後にAPPROVED）
- owner: Claude Fable司令塔
- approver: 大野
- 対象: 大野法律事務所の相続・時効援用等の共通業務基盤
- 主対象: iPhone／PCで使用する内部PWA
- 読者: Claude Fable司令塔、PC-A Claude Code実装セッション、Codex独立レビュー、大野
- 目標工数: 基準210時間。G0後220〜230時間。G8.5 remediation reserve別枠8〜20時間、Release D productionまで最大228〜250時間想定
- repo spot-audit基準: GitHub main `2822f898d1d67f7de0f282b8e61c5edbb0919ae2`（2026-07-10確認。production deploy・全test PASSは未確認）
- 実装方式: Fable司令塔＋PC-A単線実装＋Codex読取専用レビュー
- 運用規模: owner 1人を基準とするlean governance。文書量ではなく安全不変条件と実証を優先
- 位置づけ: 数年間育てて使う製品の設計正本。工程・工数・Gateは本書第17章を正本とし、別紙「Fable単線実装＋Codex独立レビュー設計書」は本書と矛盾しない範囲で役割分担・レビュー様式だけを参照する

---

# 0. 読み方・状態・正本順位

## 0.1 本書の目的

本書は、単に機能を列挙する計画書ではない。次を一つの設計体系として固定する。

- 誰が、どの状況で、何を確認し、何を操作するか
- どの画面をどの順序で使うか
- 何を速くし、何に意図的な摩擦を残すか
- kintone、Drive、FastAPI、LINE、外部サービスの責任境界
- 認証、権限、承認、監査、障害復旧
- 正常時だけでなく、empty、loading、stale、conflict、失敗、結果不明の挙動
- 将来の社員追加・業務ユニット追加に耐える構造
- 実装・テスト・実機検収・release・運用の完了条件

## 0.2 要件状態

Claudeは各記載を次の状態で解釈する。状態を無視して全てを確定仕様として実装してはならない。

| 状態 | 意味 | 実装上の扱い |
|---|---|---|
| FIXED | 大野が既に裁定し、変更禁止 | 明示的な再裁定なしに変更しない |
| PROVISIONAL | 現時点の推奨。repo・実機突合後に確定 | Gateで検証し、ACCEPT／CHANGEを記録 |
| OPEN | 人または実物確認が必要 | 想像で実装せずBLOCKEDへ |
| DEFERRED | 第1版には入れない将来項目 | data設計上の余地だけ残し、UI・機能を作らない |

## 0.2.1 証拠状態

本書は「報告書に書いてある」と「実物で確認した」を混同しない。重要な事実・Gate証拠へ次の状態を付ける。

| 証拠状態 | 意味 | 利用可能範囲 |
|---|---|---|
| REPORT_ONLY | report・設計書だけで把握 | 調査仮説。実装開始根拠にしない |
| REPO_VERIFIED | 指定SHAのsource／commitで確認 | code上のAS-IS根拠。env・実機は未証明 |
| STAGING_VERIFIED | stagingとtest dataで実証 | 許可されたpilot前まで |
| PRODUCTION_VERIFIED | production設定・実data・実機で証拠化 | release受入根拠 |
| STALE | その後の変更により失効 | 使用禁止。再検証する |

証拠状態は、RV群、BLOCKER／HIGH、E2／E3、安全不変条件、Gate、production release claimにだけ必須とする。全要件・全UI・全testへ人が手入力しない。通常taskはwork-logのSHA、test、実機結果で足りる。CI／scriptで導出できる状態は自動生成し、同じ証拠をADR・MigrationItem・matrixへ重複転記しない。

大野向けの日常表示は「未確認／確認済み／要対応」の3語で足りる。5状態はClaudeとrelease evidenceが証拠の強さを誤認しないための内部metadataであり、大野が日常的に台帳更新する対象ではない。

## 0.2.2 risk remediation状態

| 状態 | 合格条件 |
|---|---|
| OPEN | riskが有効。BLOCKER／HIGHならGate不合格 |
| CONTAINED | production到達性または権限が遮断され、危険credentialが失効／最小化され、traffic・caller確認、manual fallback、owner、恒久修正期限、monitor、rollback、PRODUCTION evidenceがある |
| FIXED | 目標controlへ移行し、旧route／token／permission／triggerが無効、回帰・retirement evidenceあり |
| STALE | 再調査により当該AS-ISが現行に存在しない。反証evidenceあり |

移行計画、停止予定日、監視の追加だけではCONTAINEDにしない。CONTAINEDにはexpires_atを必須とし、期限超過はOPENへ戻す。

## 0.3 正本順位

「現在どう動くか」と「今後どうあるべきか」の正本を分ける。

### AS-ISの証拠順位

1. production実機・deploy済みSHA・外部service実状態
2. 現在main、最新work-log、全test実出力
3. kintone／Drive／GAS／Railway／watcherの取得時刻付きsnapshot
4. 2026年7月完全レポート等の統合報告

### TO-BEの規範順位

1. 日付の新しい大野の明示裁定と弁護士承認済み凍結仕様
2. 本書のFIXED安全不変条件、APPROVED ADR、MigrationItem
3. docs配下の機能別正本
4. 本書第17章の工程・Gate
5. 別紙の役割分担・review様式

危険なAS-ISが存在しても、それがTO-BE安全要件を上書きしない。反対に、TO-BEを理由にAS-ISの挙動を黙って変更しない。旧新併存・caller切替・backfill・credential rotation・data照合・legacy retirementを要する差分だけMigrationItemとし、それ以外はtask work-logで管理する。

本書第17章は210時間統合品質版の工程・工数・Gateの正本であり、96時間版・160時間版より優先する。法務文言、料金、期限、戸籍判定の凍結正本は上書きしない。

## 0.4 Claudeへの絶対指示

- repoに存在しないfield名、endpoint、table名を本書から推測して作らない。
- 本書の論理名を、Gate 0で現在の実装へmappingしてから実装する。
- OPENを仮決めしない。
- DEFERREDを「将来必要だから」と先行実装しない。
- FIXEDと実機が矛盾した場合は、実機に合わせて勝手に変更せず停止する。
- 完了報告は、動作証拠、test、外部効果、未解決を伴う。

## 0.5 2026-07-10 repo spot-auditで確認した現在地

この節は指定SHAのsourceで確認したREPO_VERIFIEDなAS-ISであり、production設定・deploy・実機成功を意味しない。Phase 0で再取得し、解消済みなら証拠を添えてSTALEへ移す。未確認の事項を「たぶん直っている」と扱わない。

REPO_VERIFIEDは「指定SHAの該当sourceにその挙動が読めた」という限定claimである。現在mainが一commitでも異なれば、そのまま実装根拠にせずG0で再検証する。spot-auditの所見も、7月reportや引継ぎbriefと同じく地図であって領土ではない。

| ID | REPO_VERIFIEDな現在地 | risk | TO-BE／Phase |
|---|---|---|---|
| RV-01 | GitHub mainは報告書のf7e9b17から進み、2822f898…であった。current test PASS数とproduction deploy SHAは未確認 | HIGH | G0でSHA・deploy・全test実出力を固定 |
| RV-02 | legacy /scanは認証、durable idempotency、PDF上限がなく、AIのfree-form JSONを使い、抽出dataとkintone requestをlogへ出す | BLOCKER | Phase 0で外部到達性を止め、Phase 1で安全なingestへ統合 |
| RV-03 | legacy /ocr/fixed-assetは認証がなく、抽出した地番を検索に使わず、所在部分一致の先頭recordを更新し得る | BLOCKER | watcherを/valuation/ingestへ移行し、legacy endpointを無効化 |
| RV-04 | 新ingest群もsecretをURL queryへ載せる。proxy／access log等への露出余地がある | BLOCKER | header署名方式へdual-accept移行後、query方式停止・rotation |
| RV-05 | LINE・App 30 webhook後の処理にprocess-local BackgroundTasksがあり、再起動時に受理済み処理を失い得る | BLOCKER | InboundEvent＋durable workerへ置換 |
| RV-06 | 顧客Botの会話履歴・record ID・hearing stateにprocess memoryが残る | HIGH | durable conversation stateまたは明示的再構成へ移行 |
| RV-07 | Stripe webhookはPII log、hard-coded App 21、event idempotency・kintone結果確認不足がある | BLOCKER | 署名・event journal・read-after-write・reconciliationを実装 |
| RV-08 | R4-2名寄せは敗者recordを物理削除し、複数system writeをdurable journalなしで進める | BLOCKER | candidate logicは維持し、soft merge／unmerge可能なlineageへ段階移行 |
| RV-09 | 業務通知token未設定時に顧客Bot tokenへfallbackする経路がある | BLOCKER | fail closed。channel roleを必須にし、default tokenを廃止 |
| RV-10 | log・LINE通知に氏名、住所、財産、Drive URL、質問／本文等が含まれ得る | HIGH | notification最小化、redaction、既存logの露出調査・retention裁定 |
| RV-11 | CloudSign webhookはURL path secretで、真正性確認失敗後もpayloadを採用して更新し得る | BLOCKER | fail closed、署名／event journal、非同期照合、secret移行 |
| RV-12 | dependencyがversion固定されず、current mainのGitHub Actions実行証拠を取得できなかった | HIGH | lock／hash、SBOM、CI、dependency scanをrelease gate化 |
| RV-13 | sortationの重複防止はprocess memory中心で、ask task保存失敗やforward失敗を成功response内へ飲み込む経路がある | BLOCKER | IngestionReceipt、TaskBinding、retry／quarantine、read-after-write |
| RV-14 | /healthはdependency NGでもstatus okを返し得て、daily schedulerはprocess内実行で多instance重複・再起動欠落があり得る | HIGH | liveness／readiness分離、scheduler lease、dead-man monitor |

上表は「全部作り直す」という意味ではない。既存R／S／Z／D／M／H系列のdomain logicを保存し、その入口・identity・journal・privacy・復旧をcontrol planeで包み直すためのmigration backlogである。

RV-09と7月PR #75は直ちに矛盾とは判定しない。PR #75の「business_token_env一点集約」と、監査SHAで読めた「その関数内部のfallback」は同時に成立し得る。G0では現mainの関数本体、全call site、設定欠落時test、production env keyの有無を別々に確認し、実物がfail closedならRV-09をSTALEへ移す。

### spot-audit evidence index

| 対象                                  | 指定SHAで確認した主source                                                      | Phase 0で再確認する観点                                       |
| ----------------------------------- | ---------------------------------------------------------------------- | ----------------------------------------------------- |
| legacy scan／fixed asset／LINE／Stripe | main.py                                                                | auth、PII log、event journal、App ID、response確認          |
| sortation                           | sortation_ingest.py                                                    | durable idempotency、threshold、ask task、forward retry  |
| 戸籍／登記／評価／預貯金ingest                  | koseki_ingest.py、registry_ingest.py、valuation_ingest.py、bank_ingest.py | query token、source hash、safe-side route               |
| 名寄せ                                 | person_merge_exec.py                                                   | physical delete、revision CAS、復元、journal               |
| dispatch                            | dispatch_bot/router.py、hub/dispatch.py                                 | BackgroundTasks、pending command、App 30 state          |
| kintone adapter                     | hub/kintone.py                                                         | retry、partial success、unknown effect、read-after-write |
| webhook auth／通知                     | hub/webhook_auth.py、hub/notify.py                                      | signature、channel fail-closed、PII                     |
| CloudSign                           | cloudsign_webhook.py、document_webhook.py                               | URL secret、真正性失敗時、blocking I/O、template data log      |
| health／scheduler                    | daily_healthcheck.py、main.py                                           | readiness、leader lease、missed／duplicate run           |
| build／dependency                    | requirements.txt、railpack.json                                         | lock、hash、runtime、SBOM、CI                             |

これは全repository監査ではない。G0のPC-Aは現mainをlocalで取得し、rg、OpenAPI、call graph、test、production config snapshotで到達経路を全量確認する。spot-audit結果だけを根拠にcodeを直接変更しない。

## 0.6 v2で旧計画から変えた中心判断

1. 96時間／160時間計画を、既存資産統合型210時間へ置換した。
2. PWAを新しい業務台帳ではなく、kintone／Drive／既存engineのcontrol planeとした。
3. R／S／Z／D／M／H系列は原則保存し、入口・journal・訂正・privacy・復旧だけを重点再構築する。
4. 高確信度自動仕分けはE1の仮binding／可逆routingとして残し、人確認済みと分離した。
5. provisional bindingはR3読解までとし、R4-1人物化以降はHUMAN_CONFIRMEDまで止める。
6. R4-2候補logicは保存するが、App 34人物recordの物理削除を恒久禁止しsoft mergeへ移行する。
7. App 29／30／38を既存正本として保ち、PWA taskをsource projection／assignment overlay／native taskに分けた。
8. App 36をDerivationRun projection、App 37を遺産分割入力正本候補として結線した。
9. LINEは通知・軽量fallback、PWAはE2／E3の主作業面、GASはDrive mutation executorとした。
10. Drive物理再編を必須から外し、KEEP_CURRENT／PILOT／DEFERREDを正規結果にした。
11. legacy unauthenticated endpoint、query secret、memory queue、PII log、Stripe／CloudSign webhookをPWA前の必須hardeningへ移した。
12. Release A〜Cと外部connectorを分離し、外部契約待ちでもread／workflow／draftを安全に段階releaseできるようにした。
13. Phase 1をbase 20hではなく、G0後30〜40hへ膨らむことを既定riskとして総計220〜230hへ織り込んだ。
14. v2.2でcombined 70万円soft budget／75万円absolute ceilingを置いたが、v2.3で資金制約解除によりSUPERSEDED。
15. 資金制約を解除し、Console上限を150万円相当の暴走・事故検知器へ変更。品質を落とす節約を禁止した。
16. stagingをSTAGING_READY_COREとconnector別STAGING_READY_CONNECTOR_Xに分け、契約待ちでCoreをblockしない構造にした。
17. passkey recoveryをhardware security key 2本＋予備端末でCLOSEDへ移した。
18. Cloudflare有料planとRailway外部の有料uptime監視を前提化した。
19. Release D前の専門業者penetration test GateとO-36を追加した。
20. 大野の関与を「朝15〜30分・夕30〜60分の2窓＋提示済み事項の随時LINE承認」に固定し、通常日1〜2時間／検収集中日3〜4時間を計画前提とした。
21. 内容を確認できない場合の「本日停止」を正規裁定とし、形式承認を禁止して「大野裁定待ち」として安全に翌窓へ持ち越す規律を追加した。
22. Fable起動指示へ、要求の2窓集約、[人]taskの完成形提示、「今日は止めて」を計画遅延・検収失敗として扱わない規律を組み込んだ。

## 0.7 一人運用のlean governance【FIXED】

安全controlは残すが、同じ情報を複数文書へ手で転記しない。維持できない規律を増やすこと自体をriskとみなす。

### 人が維持する正本

1. 本master: 方針・FIXED仕様・Gate。日々の作業結果は書き足さない。
2. work-log: 一task／一PhaseのSHA、test、実機、未解決、次の一指示。
3. ADR: 下記triggerに該当する判断だけ。
4. 弁護士凍結正本・実物template: 既存どおり。

release evidence、SBOM、schema snapshot、OpenAPI、test index、dependency list、evidence index、traceability viewは可能な限りCI／scriptで自動生成する。自動生成物を人が別の台帳へ再入力しない。

### ADRが必須になるtrigger

- 法務凍結事項、料金、期限、権限留保の変更
- source of truth、App／DBのwrite owner、data retentionの変更
- auth、role、credential、network、public／worker boundaryの変更
- E2／E3、承認、外部効果、UNKNOWN policyの変更
- data削除、merge、backfill、Drive移行等の不可逆／広範migration
- RPO／RTO、backup、重大なvendor依存の変更

UI文言、CSS、既定の並び順、小さなrefactor、test追加、APPROVED挙動へ戻すbug fixにはADRを作らずwork-logで足りる。

### MigrationItemが必要になる範囲

旧新が一定期間併存し、caller切替、backfill、credential rotation、data照合、rollback、legacy retirementのどれかを要する変更だけに作る。単一transaction内の通常code変更や一回で戻せるfeature flag変更には作らない。active MigrationItemは一つの表に集約し、完了後はrelease evidenceへ閉じる。

### traceabilityの軽量化

詳細なREQ→screen→API→testのchainは、E2／E3、auth／PII、cross-system write、法務成果物、安全不変条件だけに必須とする。E0 read viewや低risk UIは task ID → changed files → tests → human result の四点で足りる。

### 文書化budget

手作業の文書化・転記はactive project時間の10％を目安、15％を上限とする。超過予測時は安全証拠を削らず、文書を統合・自動生成する。節約した時間は新機能ではなく、実機test、訂正、復旧、UX改善へ戻す。210時間の総枠はG0再見積りまで変更しない。

---

# 1. 製品ビジョン

## 1.1 一文での定義

このPWAは、事務所内のデータを新たに保管する倉庫ではなく、kintoneとDriveにある案件情報を、必要な時に安全に確認・処理するための「業務司令盤」である。【FIXED】

## 1.2 解決する問題

- LINE文字応答だけでは、仕分け・名寄せ・承認が増えるほど操作が煩雑になる。
- 案件の現在地を確認するために、複数AppとDriveを横断する必要がある。
- 承認待ち、期限接近、処理失敗、結果不明が各所に散らばる。
- 人が確認すべきものと、機械が処理できるものの境界が画面上で不明瞭になり得る。
- 将来の社員追加時に、誰が何を見て何を確定したかを説明できる必要がある。
- 新しい業務ユニットを追加するたびに、別アプリを作り直す構造は避けたい。

## 1.3 製品としての成功

成功は機能数ではなく、次で測る。

| 指標 | 初期目標 | 状態 |
|---|---:|---|
| 通知から対象作業画面へ到達 | 10秒以内 | PROVISIONAL |
| 通常の仕分け確定 | 原本表示後3tap以内 | PROVISIONAL |
| 案件の現在地把握 | 10秒以内 | FIXEDの方向性 |
| 承認対象の宛先・本文・添付・版確認 | 1画面で完結 | FIXED |
| E2／E3の自動確定・承認なし送信 | 0件 | FIXED |
| 誤った案件へのdata表示・回送 | 0件 | FIXED |
| 操作主体を説明できない確定操作 | 0件 | FIXED |
| 新任事務員の基本操作習得 | 30〜60分以内 | PROVISIONAL |
| 重大障害の検知 | 5分以内 | PROVISIONAL |

## 1.4 非目標

- PWAをkintoneやDriveの代替DBにしない。【FIXED】
- 第1版でnative iOSアプリを作らない。【FIXED】
- 顧客dataをoffline利用できるようにしない。【FIXED】
- AIが名寄せ、法務判断、対外送信を確定しない。【FIXED】
- 全業務を一画面へ詰め込まない。【FIXED】
- 将来機能のために過剰なmicroservice化をしない。【PROVISIONAL】
- 一般顧客向け公開アプリにしない。【FIXED】

---

# 2. 製品原則

## 2.1 通知はLINE、作業はPWA

- LINEは「何が起きたか」「何件溜まっているか」を知らせる。
- 通知から対象taskまたはcaseへdeep linkする。
- 機微な回答全文、戸籍画像、財産情報をLINEへ残さない。
- 例外的な一件処理経路として既存LINE語彙を残すが、主要作業UIはPWAとする。

## 2.2 速くする場所

- case選択後のcurrent state表示
- 高確信度候補の並び順
- 通常書類の仕分け
- Q&Aから出典への移動
- dashboardから最新書類への移動
- 一度確認済みの非重要な表示設定

## 2.3 意図的に遅くする場所

- 名寄せ確定
- 低確信度のdoc_type確定
- 対外送信承認
- 宛先変更
- 添付差替え
- 期限・料金・法務文言に関する判断
- 結果不明処理の再送
- Drive移行・大規模回送変更

## 2.4 安全な摩擦

- 原本を一度表示するまで確定buttonを有効化しない。
- 承認時は宛先、本文、添付、送信手段、版を同一画面に表示する。
- 外部効果のある操作は直前にWebAuthn再認証する。
- 低確信度は「確定」ではなく「確認が必要」と表示する。
- 結果不明は成功／失敗のどちらにも自動分類しない。

## 2.5 一貫した状態語彙

画面・API・logで、同じ概念に異なる語を使わない。最低限、次を統一する。

- 未処理
- 候補あり
- 人確認待ち
- 人確認済み（binding／人物等）
- 下書き
- 承認待ち
- 弁護士承認済み（外部実行前）
- 実行中
- 完了
- 失敗
- 結果不明
- 保留

実際のenumと日本語表示のmappingはGate 0で固定する。

## 2.6 effect levelと人の関所【FIXED】

「機械は確定しない」を、内部の可逆なroutingまで一律禁止する意味に拡張しない。操作をeffect levelで分類し、入口がLINEでもPWAでも同じruleを適用する。

| level | 性質 | 例 | 必要control |
|---|---|---|---|
| E0 | read only | Q&A、dashboard、出典表示 | login、role、case access、audit |
| E1 | 内部・可逆 | machineの仮binding／routing、task担当、保留、同一binding内の再試行 | durable journal、revision、訂正可能性。machineまたは通常session |
| E2 | 人の業務・法務確定 | HUMAN_CONFIRMED case binding、誤仕分け訂正、名寄せ、人物確認、相続人確定、成果物承認申請 | PWA target-bound step-up、原本表示、actor、source revision |
| E3 | 対外・法的効果 | CloudSign、FAX、内容証明、顧客送信、課金 | owner-lawyer、ApprovalSnapshot、target-bound step-up、Outbox、照合 |

LINEに残る既存E2語彙は移行期間だけfeature flagで維持する。PWAのstep-upをLINEで迂回できないよう、E2はPWA deep linkへ段階移行する。E3をLINEのOKだけで実行する経路は作らない。

## 2.7 自動routingと人の確定を分ける【FIXED】

7月実装の高確信度自動仕分けは、法務判断や人確認済みへの遷移ではなく、訂正可能な内部routingである。長期版では次の状態を区別する。

- AUTO_BOUND_UNREVIEWED: 機械がprovisionalなcase bindingを作った。人確認済みではない。
- AUTO_ROUTED_UNREVIEWED: 上記bindingに基づくDrive routingも完了した画面用projection。bindingの人確認済みを意味しない。
- HUMAN_REVIEW_REQUIRED: low confidence、候補衝突、unknown、high-risk document。
- HUMAN_CONFIRMED: 原本を見た人がdoc type／case bindingを確定した。
- CORRECTION_REQUIRED: 誤routingまたはsource変更により訂正待ち。

全doc type共通の0.85だけを恒久ruleにしない。document class、誤routingの影響、候補差、OCR品質ごとにthresholdを設定し、匿名化corpusのconfusion matrixと実運用誤り率で校正する。AIだけのcase bindingにはsource、model、prompt、confidence、candidate setを残す。

AUTO_BOUND／AUTO_ROUTED_UNREVIEWEDのfragmentはR3読解まで進めてよいが、R4-1人物化は人のHUMAN_CONFIRMED bindingまで停止する。【FIXED】誤仕分け訂正時は下流依存を自動列挙し、黙って別caseへ付け替えない。暫定的に現行R4-1自動結線を維持する場合は全人物にprovisional_binding_idを持たせ、R4-2、R4-3、App 36、artifact、approvalから除外する独立ADRとE2E証拠が必要であり、defaultにはしない。

---

# 3. 利用者・ロール・権限

## 3.1 想定利用者

| 利用者 | 第1版 | 主な利用 | 備考 |
|---|---|---|---|
| 大野 | 使用 | 全閲覧、仕分け、法務確認、対外承認、管理 | 唯一の対外送信承認者 |
| 事務員 | 将来利用を前提に器だけ作る | 仕分け、内部task、書類確認 | 対外承認不可 |
| 雇用弁護士 | DEFERREDだがrole余地を残す | 法務確認、担当case承認 | 承認範囲は将来裁定 |
| system service | 使用 | 通知、read model、integration | 人間roleと分離 |
| Codex reviewer | 開発時のみ | source・testの読取専用review | 本番data・PWA利用者ではない |

## 3.2 role

| role | 閲覧 | 仕分け | 名寄せ確定 | 内部承認 | 対外送信承認 | 管理 |
|---|---:|---:|---:|---:|---:|---:|
| OWNER_LAWYER | 可 | 可 | 可 | 可 | 可 | 可 |
| LAWYER | 可 | 可 | PROVISIONAL | 可 | PROVISIONAL | 不可 |
| STAFF | 可 | 可 | PROVISIONAL | 内部のみ | 不可 | 不可 |
| VIEWER | 可 | 不可 | 不可 | 不可 | 不可 | 不可 |
| SERVICE_READ | API読取のみ | 不可 | 不可 | 不可 | 不可 | 不可 |
| SERVICE_WRITE_SCOPED | 対象限定 | 対象処理のみ | 不可 | 不可 | 不可 | 不可 |

role名は論理名である。現在の実装名はGate 0でmappingする。

## 3.3 case単位access

- 第1版は大野1人利用のため、caseごとの細かいaccess制御UIは作らない。【FIXED】
- data modelにはuser×case permissionを表現できる余地を残す。【FIXED】
- 将来社員追加時、全案件一律閲覧を初期値にしない。【PROVISIONAL】
- APIは将来のcase access検査を追加できるservice境界を通す。

## 3.4 操作log

全ての確定操作は最低限次を記録する。

- user_id
- credential_idまたは認証手段
- role
- case_id
- action
- target type／target id
- before version／after version
- timestamp
- correlation_id
- success／failure／unknown

logへ本文全文・戸籍data・tokenを保存しない。

## 3.5 user lifecycle【FIXED】

1. ownerが招待対象とrole・case scopeを承認
2. 期限付き登録セッションで本人がpasskeyを登録
3. 初回利用で禁止事項・操作記録・緊急連絡を確認
4. 追加端末は既存の強いsession＋owner policyで登録
5. role変更はstep-up、before／after、実行者をaudit
6. 休職・停止は新規login拒否、active session失効、未完了task回収
7. 退職はcredential・session・API権限失効、担当task再割当、実施証跡保存

停止userの過去auditは消去せず、表示名変更で操作主体が不明にならないようuser_idを維持する。

---

# 4. 情報設計とnavigation

## 4.1 第1版navigation

| 主navigation | 目的 | 第1版 |
|---|---|---|
| 今日 | 優先task・期限・失敗・結果不明を一望 | 必須 |
| 処理待ち | 仕分け、名寄せ、人物確認、訂正、障害のqueue | 必須 |
| 案件 | case検索と案件dashboard | 必須 |
| 質問 | Q&A検索・新規質問・出典 | 必須 |
| 承認 | 下書き、承認待ち、結果不明 | 必須 |
| 管理 | account、passkey、session、role、system state | ownerのみ |

mobileはbottom navigationまたは同等の5項目以内を基本とする。【PROVISIONAL】

「管理」はprofile/menu内へ格納し、主要navigationを圧迫しない。

## 4.1.1 「今日」・「処理待ち」・「承認」の責任分離

- 「今日」は優先順付きsummaryであり、独自dataを持たない。
- 「処理待ち」は既存queueをTaskBindingで束ねたcanonical projection list。業務状態の正本はApp 29／30／38等に残し、仕分け・名寄せ・訂正・障害を横断する。
- 「承認」はApprovalRequest・ArtifactVersion・ExternalDeliveryを一続きで扱う専用workspace。
- 同じapprovalをWorkTaskとApprovalRequestの両方に二重計上しない。summaryはcanonical IDと同一query ruleから算出し、card件数と過渡先list件数をtestで一致させる。

## 4.2 ホームを機能一覧にしない

「今日」画面の中心はtile一覧ではなく、処理すべきwork queueとする。

優先表示例:

1. 結果不明
2. 期限超過・期限接近
3. 対外承認待ち
4. 名寄せ・人確認待ち
5. 仕分け待ち
6. 新着書類
7. 通常の内部task

priority計算は法務判断を自動化しない。期限、状態、失敗、未処理時間等の機械的dataだけで並べる。

## 4.3 URL設計

論理URLは次の形を想定する。【PROVISIONAL】

- /today
- /tasks
- /tasks/{task_id}
- /corrections/{correction_id}
- /cases
- /cases/{case_id}
- /cases/{case_id}/people
- /cases/{case_id}/assets
- /cases/{case_id}/documents
- /cases/{case_id}/timeline
- /questions
- /questions/{qa_id}
- /cases/{case_id}/artifacts
- /artifacts/{artifact_id}/versions/{version}
- /approvals
- /approvals/{approval_id}
- /admin/security

実際のprefix・router構成はrepoを見て確定する。case_id、task_idをURLだけで信用せず、server側でaccessを検査する。

---

# 5. 主要user journey

## 5.1 LINE通知から仕分け

1. LINEに「仕分け待ち3件」と通知
2. deep linkからPWAの対象queueを開く
3. session有効なら表示、失効ならpasskey login
4. PDF thumbnailと高確信度候補を表示
5. 原本を一度表示
6. doc_typeとcase候補を確認
7. HUMAN_CONFIRMED case bindingの直前にtask ID／version／decision hash結合の再認証
8. 人確認済みとして確定。既にauto-routed済みなら再routingしない
9. success結果と次の1件を表示
10. audit logとidempotency keyを記録

## 5.2 案件の現在地確認

1. 「案件」から氏名・case番号等で検索
2. 候補からcaseを選択
3. dashboardに相続人、財産、進行、期限、次action、直近書類を表示
4. 未確定dataは明示
5. 元record・原本へ移動可能
6. 10秒以内に現在地を理解できる

## 5.3 Q&A

1. 質問画面でcaseを先に固定
2. 自然文で質問
3. serverが許可されたread toolだけで検索
4. 回答、出典record、未確定注記、信頼度を表示
5. 低信頼・手書き・旧字は原本確認を要求
6. 回答をQ&A履歴へ保存
7. 書込み・確定操作は行わない

## 5.4 成果物の生成・修正・承認申請

1. case dashboardの「成果物」から種類を選ぶ
2. 使用するsource record・人入力・template versionを確認
3. serverがGENERATINGを作り、正本templateと既存engineで生成
4. validation errorがあればVALIDATION_BLOCKEDと不足fieldを表示
5. DRAFT_READYでpreview、source snapshot、差分、version履歴を確認
6. 修正は許可fieldだけを編集し、保存するたびに新ArtifactVersionを作る
7. source revisionが進んでいればSOURCE_OUTDATEDとし、再生成または人確認
8. workflow authorityを確認する。LEGACY_APP_AUTHORITATIVEなら新ApprovalRequestを作らずApp 29／30の既存承認へdeep linkする。PWA_SHADOW_READ_ONLYなら差分表示だけで承認不可。PWA_AUTHORITATIVEの時だけApprovalSnapshotを固定し、step-up後にPENDING_APPROVALへ進む

生成中・draft編集中は次を扱う。

- DIRTY: server未保存の編集あり。離脱警告あり
- SAVE_IN_PROGRESS: 重複保存を防止
- SAVE_FAILED: 未保存範囲と再試行可否を表示
- DRAFT_CONFLICT: 他versionとの差分を表示し、自動上書きしない
- SESSION_EXPIRED: 未保存本文を闇で外部保存せず、再ログイン後の安全な復帰方針をGate 2で確定

自動保存は長文の全上書きにせず、optimistic lockと新version作成を守れる場合のみ採用する。不完全な自動保存で「保存済み」と表示しない。

## 5.5 対外承認

このjourneyはPWA_AUTHORITATIVE workflowにだけ適用する。App 29顧客replyとApp 30物理発送がLEGACY_APP_AUTHORITATIVEの間は、PWAにsource stateとdeep linkを表示するが、新ApprovalRequestを作らずPWAで承認しない。

1. 承認待ちqueueから対象を開く
2. case、宛先、本文、添付、送信手段、版を一画面で表示
3. 添付を実際に開いて確認
4. 修正が必要なら「下書きを修正」へ戻り、新ArtifactVersionを作成
5. 旧ApprovalRequestをSUPERSEDEDにし、新ApprovalSnapshotで再申請
6. WebAuthn再認証をapproval ID／version／snapshot hashに結合
7. immutable snapshotを承認
8. 承認と同時にOutboxJobを作成。workerが承認済みbytesだけを実行
9. QUEUED／EXECUTING／ACCEPTED／DELIVERED_OR_COMPLETED／失敗／結果不明を区別
10. 結果不明は人の調査taskを作り、自動再送しない

## 5.6 Drive architecture decision／optional pilot

まずPWAのvirtual viewで現行folderを使い、具体的不便を再計測する。KEEP_CURRENTを選んだ場合は以下を実行しない。PILOTが承認された場合だけ進む。

1. 対象caseをpilotとして明示選択
2. dry-runで移動予定、旧新folder、権限、件数を表示
3. 大野が確認
4. 人操作でpilot開始を許可
5. file単位manifestを記録
6. 移動後に件数・ID・回送・読解を検証
7. 問題時は旧routingへ戻す
8. 全面移行へ自動拡張しない

## 5.7 誤仕分け・誤名寄せの訂正

1. 元の確定履歴から「訂正を開始」
2. 現在の結果と新候補を並べる
3. Drive、読解、人物graph、順位、帳票、承認への影響listを表示
4. 訂正理由と原本を確認し、step-up
5. 新decisionが旧decisionをsupersede
6. 実行前派生物は失効、実行済み外部効果は補償taskへ
7. 再回送・再読解・再生成の個別結果を照合

## 5.8 LINE通知とdeep linkの寿命

- 同一task・同一状態の通知は重複排除し、多数発生時は件数で集約する。
- 期限・結果不明の再通知間隔は通知policyで固定する。
- LINE送信失敗でWorkTaskを消さない。「今日」が常に正本の入口である。
- COMPLETED／CANCELLED／SUPERSEDEDのdeep linkは再実行画面ではなく、現在stateと履歴へ開く。
- access失効後のdeep linkは内容を漏らさずFORBIDDENを返す。
- LINE劣化時はPWA上にdegraded indicatorを表示する。

## 5.9 PWA追加・更新

1. Safariで正規domainを開き、「ホーム画面に追加」手順を画面内で案内
2. standalone起動後、ログイン前のdeep linkをserver側の安全なreturn targetとして保持
3. 新version検出時はUPDATE_AVAILABLEを表示し、DIRTY編集中・承認中に強制reloadしない
4. minimum client version未満の旧clientはread-onlyまたは静的更新画面でwriteを停止
5. 更新後はaccess再評価の上、元のdeep linkへ戻る

---

# 6. 画面別詳細仕様

全画面は、正常状態だけでなく次の共通状態を持つ。

| 状態 | 必須表示・挙動 |
|---|---|
| LOADING | skeleton表示。重複tapを防ぐ。一定時間超過で「時間がかかっています」 |
| EMPTY | 「0件」だけでなく、0件の意味と次にできることを表示 |
| ERROR | 人が理解できる要約、correlation ID、再試行可否。内部stackを表示しない |
| PARTIAL | どのdataが表示でき、どれが欠落したかを表示。欠落が確定に影響するならwrite不可 |
| STALE | data取得時刻と更新button。古いdataで確定操作をさせない |
| CONFLICT | 他操作でversionが進んだことを表示し、最新版を再取得。自動上書きしない |
| OFFLINE | 顧客dataを表示せず、通信回復を要求。offline操作queueを作らない |
| FORBIDDEN | 権限不足を明示。dataの存在自体を不要に漏らさない |
| UNKNOWN | 結果不明を成功・失敗へ寄せない。人確認taskへ誘導 |
| DIRTY／SAVE_FAILED | 未保存を明示。離脱警告と安全な再試行 |
| SESSION_EXPIRED／STEP_UP_EXPIRED | 必要な認証だけをやり直し、対象version／hashを再確認 |
| DELETED／MERGED／OBSOLETE | 再実行を禁止し、後継対象と履歴へ案内 |
| UPDATE_AVAILABLE／CLIENT_INCOMPATIBLE | 編集中は強制reloadせず、非互換clientのwriteはserverも拒否 |

### 主要画面×状態の操作可否

| 画面 | STALE／PARTIAL | DIRTY／SAVE_FAILED | SESSION／STEP-UP失効 | DELETED／MERGED | CLIENT_INCOMPATIBLE |
|---|---|---|---|---|---|
| 今日・一覧 | 表示可、確定不可 | 対象外 | 再login | 履歴・後継へ | write不可 |
| 仕分け・名寄せ | 原本／候補不足なら確定不可 | 選択状態を明示 | 対象再照合後、E1はsession再確認・E2はstep-up | 訂正履歴へ | 確定不可 |
| case dashboard | 欠落sectionを明示 | 対象外 | 再login | merged caseへredirect | write不可 |
| artifact editor | source staleは承認申請不可 | 離脱警告・再保存 | 未保存範囲を明示 | 後継versionへ | 保存・申請不可 |
| approval | stale・snapshot不一致は承認不可 | in-place編集不可 | 対象hash再表示後step-up | SUPERSEDED履歴 | 承認不可 |
| Q&A | STALE注記、旧回答を現行扱いしない | 対象外 | 再login | 回答本文を非表示にする場合あり | 新規質問不可 |

## 6.1 Login・passkey

### 目的

passwordを主認証にせず、固定domain上でpasskeyにより本人確認する。

### 表示

- 事務所名・内部systemであること
- 「passkeyでlogin」
- login先domain
- session失効理由（期限、管理者失効、credential失効等）
- support／緊急停止の案内

### 操作

- passkey login
- 初回credential登録（ownerが許可したsessionだけ）
- credential名の設定
- login cancel

### 禁止

- query parameterへtokenを入れる
- password fallbackを自動表示する
- login errorにuser存在情報を出す
- 別origin・仮domainで本番credentialを登録する

### 受入条件

- userVerification必須
- challenge一回限り・短時間
- replay拒否
- session fixation防止
- credential失効後の即時拒否
- iPhoneのPWA modeとSafari双方で確認

## 6.2 「今日」画面

### 目的

今処理すべきこと、止まっていること、期限・失敗を10秒以内に把握する。

### 表示data

- 結果不明
- 期限超過・期限接近
- 承認待ち
- 人確認待ち
- 仕分け待ち
- 新着書類
- system警報
- 最終更新時刻

### card

各cardは最低限次を表示する。

- task種別
- case表示名
- priority理由
- 待機時間または期限
- 未確定・低信頼のbadge
- 主action
- 詳細へのdeep link

### 操作

- cardを開く
- 種別filter
- case filter
- 完了済みを一時表示
- stale dataを更新

### 禁止

- priorityだけで法務判断済みと誤認させる
- 期限不明を低priorityへ自動配置する
- cardから即時に対外送信する

### 受入条件

- 結果不明は常に通常taskより上
- ownerとstaffでactionが変わる
- 0件時にsystem停止と正常な0件を区別
- card countと詳細list件数が一致

## 6.3 処理待ち一覧

### queue種別

- SORTATION
- PERSON_MATCH
- INTERNAL_REVIEW
- INTEGRATION_FAILURE
- UNKNOWN_RESULT

ApprovalRequestはWorkTaskへ複製しない。「今日」の承認cardはApprovalRequestから直接作るprojectionであり、「承認」workspaceへ遷移する。WorkTask件数には含めず、approval_idをtask_idとして扱わない。【FIXED】

### filter・sort

- case
- task種別
- 期限
- 作成時刻
- confidence
- 担当者（将来）
- 状態

### batch

- 第1版では不可逆操作のbatch確定を作らない。【FIXED】
- 既読化、表示filter等の非本質操作だけbatch可。【PROVISIONAL】
- 将来batchを入れる場合も、対象一覧と件数を固定し、途中失敗を個別表示する。

### 担当・claim

- 担当なし、claim中、指名割当、保留、完了をbadgeで区別
- claim時はlease期限と操作者を表示し、他userの有効claimを黙って奪わない
- ownerの再割当は理由を必須とし、旧・新担当へ通知
- cancel・supersede後のcardは履歴と後継taskを示し、実行buttonを出さない

### task詳細

- task ID、type、case／source document、state、version
- 起票理由、期限、priority理由、source ref
- 現担当、claim lease期限、過去の割当履歴
- 完了条件と必要evidence
- claim／作業開始／保留／再開／完了
- ownerだけの再割当／取消／supersede。理由必須
- COMPLETED後に誤りが判明した場合は旧taskを再OPENせず、訂正taskを作って相互link
- version conflict時は最新担当・状態・入力差分を表示し、自動上書きしない

## 6.4 仕分け画面

### layout

mobile:

1. 上部: case候補と現在のtask状態
2. 中央: PDF／画像原本viewer
3. 下部: doc_type候補、case候補、確定button

desktop:

- 左: 原本
- 右: 候補・metadata・確定
- 原本と候補を同時に見られる

### 表示data

- 原本thumbnail／page count
- file名・受領時刻
- OCR抽出の最小限のpreview
- doc_type候補とconfidence
- case候補と根拠
- 低信頼理由
- 重複疑い
- 回送予定先
- current binding state（AUTO_BOUND_UNREVIEWED／HUMAN_REVIEW_REQUIRED／HUMAN_CONFIRMED）とrouting stateを別表示

### 操作

- page移動・拡大
- doc_type選択
- case選択
- 候補にない値を検索
- 保留
- 誤投入として隔離
- 確定

### 安全要件

- 原本viewerが一度表示完了するまで確定不可
- 低信頼時は追加確認表示
- file version、task versionによるoptimistic lock
- 確定endpointはidempotent
- 二重tap、reload、通信timeoutに耐える
- 別case候補を選んだ時は回送予定を再表示
- 確定後の自動取消は行わない。訂正flowを別にする

### 受入条件

- 実scan20件
- 通常caseは原本表示後3tap以内を目標
- HUMAN_CONFIRMEDへの自動遷移0件。既にE1 auto routing済みなら未確認badgeを表示
- 二重起票0件
- 低信頼が通常確信として表示されない
- user_idと対象versionがlogへ残る

## 6.5 名寄せ・人物確認

### 表示

- 左右または上下に候補人物
- 氏名、旧字、別表記
- 生年月日
- 続柄
- 本籍・住所の一致要素
- 出典書類とpage
- engineの一致根拠
- 不一致・不足要素

### 操作

- 同一人物
- 別人
- 判断保留
- 原本を開く
- 関連人物・関係図を見る

### 安全要件

- default選択を置かない
- keyboard shortcutやsingle swipeだけで確定させない
- engine confidenceだけで確定しない
- 一度の操作で複数pairを連鎖確定しない
- 訂正時は影響する関係図・順位・帳票を表示

## 6.5.1 訂正詳細

### 必須表示

- correction ID、対象decision ID／version、確定者・確定時刻
- 旧decisionと提案する新decision
- 訂正理由、原本、source revisions
- 影響snapshot: Drive file／routing、OCR／読解、人物graph／順位、ArtifactVersion、ApprovalRequest、OutboxJob／ExternalDelivery
- 「自動失効できるもの」「人確認が必要なもの」「既に外部効果があり戻せないもの」の区分

### 操作

- 訂正案の編集
- source最新版の再取得
- 影響再計算
- 保留／取消
- step-up後の訂正確定

### concurrency

影響snapshot作成時の全source revision、対象decision version、依存artifact／approval／delivery versionからimpact hashを作る。step-up challengeをcorrection ID・expected version・impact hashへ結合する。再認証後に一つでもversionが進めばCONFLICTとして影響を再計算し、旧hashで確定しない。

### 受入条件

- 旧decision・旧auditが残る
- 依存成果物・承認の失効漏れ0
- 実行済み外部効果を「取消済み」と誤表示しない
- compensation taskと担当・期限が作られる
- crash後もcorrection operation_idから再開・照合できる

## 6.6 案件一覧

### 検索

- case ID
- 顧客氏名
- 被相続人氏名
- 業務類型
- 電話下4桁等の許可された補助key

検索logへ全文queryを無制限保存しない。検索結果はcase access検査後に返す。

### list表示

- case表示名
- 業務類型
- 現在state
- 期限・警告
- 次action
- 未確定badge
- 最終更新

## 6.7 案件dashboard

### header

- case表示名
- case ID
- 業務類型
- 担当
- 現在state
- 主要期限
- 未確定・警報

### section

1. 次に行うこと
2. 相続人・関係者summary
3. 財産summary
4. 書類・不足書類
5. 直近activity
6. 承認待ち・結果不明
7. 外部service状態

### data源

- App 34
- App 35
- 進行台帳
- Drive document metadata
- 読解JSON
- approval／integration state

dataをPWA独自の正本として複製しない。必要なread modelを作る場合も、source record ID・version・取得時刻を保持する。

### 受入条件

- 実caseを10秒以内に説明できる
- 未確定dataを確定dataと同じ表示にしない
- summaryからsource record・原本へ到達
- source更新後のstale表示
- 権限不足sectionをAPIから返さない

### case lifecycle

- OPEN: 通常業務可
- CLOSED: 新規対外処理は原則停止。残task・approvalを0または明示的に取消してからclose
- ARCHIVED: read-only。履歴と出典は維持
- MERGED: 旧URLは後継caseへ安全にredirectし、旧IDの履歴を残す
- REOPENED: ownerのstep-upと理由、正本revision再取得後に業務再開

close・merge・reopen時は、未完了task、approval、outbox、外部効果を先に棚卸し、実行中のものを黙って取り消さない。同姓同名のcase候補は、マスクした生年月日・住所・case ID等で識別する。表示範囲はGate 2で実機確定する。

## 6.8 人物・相続関係

### 表示

- 人物list
- status（生死・未確認）
- 続柄
- 名寄せ状態
- 出典
- 法定順位engineの導出結果
- 不足情報

### 将来表示

- 関係図visual
- node選択から原本

visualは正確性を優先し、装飾目的の自動配置を正本としない。

## 6.9 財産

### 表示

- 種別
- 名義
- 概要
- 金額・評価
- 評価時点
- 共有持分
- 未確定
- 出典
- 目録4用途への反映状態

### 安全要件

- 合計と内訳のcurrency・桁
- 未確定金額を確定合計へ混ぜた場合は明示
- 原本と台帳の差異を警告
- PWAから元dataを書き換える機能は第1版DEFERRED

## 6.10 書類

### 表示

- 書類種別
- 原本／fragment
- 受領日
- 読解状態
- confidence
- version
- Drive link
- 関連人物・財産

### 操作

- preview
- sourceへ移動
- Q&Aへ引用
- 誤分類の訂正taskを作る

原本の無分割性を維持し、fragmentだけを原本と誤表示しない。

## 6.11 timeline

### event

- 書類受領
- 読解完了
- 人確認
- 帳票生成
- 承認
- 外部送信
- integration結果
- 訂正
- system警報

timelineはaudit logの生raw表示ではなく、人が理解できるread viewとする。監査用raw eventは別に保持する。

## 6.12 成果物workspace

### 一覧

- artifact type・用途
- state（GENERATING／DRAFT_READY／VALIDATION_BLOCKED／GENERATION_FAILED／SOURCE_OUTDATED／SUPERSEDED）
- current version・template version
- source data取得時刻・revision
- 作成者・最終保存者
- approvalの有無・現在state

### 生成画面

- 成果物種別と用途
- 入力source一覧、record ID、revision、未確定注記
- 人が入力する分割内容・類型等
- 使用template・条項libraryのversion
- 不足・矛盾・unknownのvalidation
- 「下書き生成」のみ。送信・draftからの自動承認なし

### preview・editor

- 文書previewと入力snapshotを並列表示
- 正本固定field、編集可field、人承認fieldを視覚的に区別
- 改頁、長住所、旧字、金額、日付、空欄を検査
- version履歴、差分、supersedes関係
- 保存後は新ArtifactVersion。旧versionは閲覧のみ
- 承認申請前にsource revisionとbytes hashを再検査

### 失敗時

- GENERATION_FAILEDはerror classとcorrelation ID、再生成可否を表示
- VALIDATION_BLOCKEDは不足fieldごとに正本sourceへ案内
- SOURCE_OUTDATEDは旧版を消さず、影響差分を確認して再生成
- 生成途中のrequest retryはidempotency keyで二重artifactを防ぐ

## 6.13 Q&A一覧

### 入口とdata境界【FIXED】

- 業務指示Botと分離した専用LINEアカウントを使う。
- 業務正本へのread-only credentialと、QARecordだけへappendできるwriterを物理的に分ける。
- 第1版のkintone構造化data＋読解JSONを対象とし、PDF全文検索は第2版とする。
- 第1版の仕分け時に、D1で既に取得した全page OCR textを書類単位で安全に保存し、将来の全文検索の下地とする。過去分はPhase 4で対象件数・費用・再開性を測る小規模pilotを行い、全件backfillは別batchのGate・予算を裁定する。対象数・成功・失敗・再開位置はjournal化する。PDF全文検索UI自体は第2版DEFERRED。【FIXED】
- 書類種別のconfidenceを次の表示に写像する: 活字＝通常回答、印字数値＝数値は原本確認推奨、手書き・旧字＝低確信注記＋原本link必須。

### 表示

- 質問
- 回答summary
- case
- 出典数
- confidence
- 未確定注記
- 作成時刻
- 質問者

### 新規質問

- caseを必須固定
- caseなしの横断質問はowner限定または第1版DEFERRED
- 回答生成中state
- timeout時に結果不明ではなく失敗・再試行可否を表示
- 出典0件なら断定回答しない

### 回答詳細

- 回答
- 出典record ID
- document type
- original link
- data取得時刻
- 未確定・低信頼
- 「原本確認済み」ではなく「原本を開く」
- source record／documentの現行revisionと回答時revisionが異なればSTALE banner
- sourceが統合・削除された場合は後継sourceへ案内し、根拠を再検証できない回答を現行と表示しない
- userがcase accessを失った場合、過去回答の本文・snippet・原本linkを再表示しない

## 6.14 承認一覧

一覧はauthorityをbadge表示する。

- PWA承認: ApprovalRequestを正本とし、このworkspaceで決裁可能。
- Legacy承認: App 29／30を正本とし、件数・状態・source deep linkだけを表示。
- Shadow: 新旧差分検証中で、PWAの承認buttonを出さない。

### category

- DRAFT
- PENDING_APPROVAL
- APPROVED／QUEUED
- EXECUTING
- ACCEPTED
- DELIVERED_OR_COMPLETED
- FAILED_RETRYABLE／FAILED_FINAL
- UNKNOWN／RECONCILING
- PREPARED_FOR_HUMAN

### sort

結果不明、期限、外部効果、作成時刻の順。承認済みと送信済みを混同しない。

## 6.15 承認詳細

### 必須表示

- case
- action種別
- 宛先
- sender
- subject
- 本文
- 添付名・版・hash
- 送信手段
- template版
- 作成者
- 最終編集者
- 変更履歴
- 料金等の外部画面確認項目

### 操作

- 「下書きを修正」でartifact editorへ移動し、新version・新snapshot・新approvalを作る
- 添付preview
- reject／差戻し
- WebAuthn再認証
- 承認
- 承認取消（実行前のみ、audit必須）

### 禁止

- listから内容を見ずにapprove
- 承認後のin-place編集
- 旧approvalの再利用
- staff roleの対外承認
- 結果不明から再送buttonを直接出す

## 6.16 結果不明・障害詳細

### 表示

- 何の結果が不明か
- 最後に確認できたstate
- correlation ID
- idempotency keyの末尾等、安全な識別子
- 外部service受付番号
- 確認済みの履歴
- 次に人が確認する場所

### 操作

- 外部履歴確認済みを記録
- 証拠を添付し、ACCEPTED／DELIVERED_OR_COMPLETED／FAILED_FINAL等へ人が解決eventをappend
- 補償・訂正taskを作る

自動再送buttonを置かない。

## 6.17 管理・security

### owner向け

- user list
- role
- passkey credential
- active session
- session一括失効
- 最終login
- system health
- version・deployed SHA
- feature flagのread-only表示

### user管理操作【第1版必須】

- 招待作成・取消。期限、予定role、招待者を表示
- passkey登録完了の確認。credentialの秘密情報は表示しない
- role変更。before／afterと影響権限をpreviewし、step-up
- user suspend／reactivate。active sessionを同時失効
- credential個別revoke／全credential revoke
- session個別revoke／全session revoke
- 退職処理。未完了task・approval関与・active session・credentialの棚卸しを表示し、task回収後にREVOKED

user recordを物理削除するbuttonは作らない。role変更・suspend・退職は確認画面、対象結合step-up、監査、結果summaryを必須とする。第1版で社員を実際に追加しない場合も、大野自身の予備credential登録、session失効、復旧runbookで同じ境界を検収する。

本番env値、token値を画面へ表示しない。feature flag変更は第1版では既存の安全な管理経路を使う。

---

# 7. UX・design system

## 7.1 mobile first

- iPhone縦持ちを第1基準とする。
- PCでは情報密度を上げるが、操作語彙を変えない。
- hoverだけに依存しない。
- safe area、keyboard表示、PWA standalone modeを確認する。
- 主要tap targetは44px相当以上を基準とする。【PROVISIONAL】

## 7.2 visual hierarchy

優先順位:

1. 危険・結果不明
2. 人確認・承認
3. 通常task
4. 補助情報

色だけで状態を伝えない。icon、label、説明を併用する。

## 7.3 color token【PROVISIONAL】

- neutral: 通常背景・text
- info: 説明・進行中
- success: 完了
- warning: 期限接近・低信頼・人確認
- danger: 失敗・結果不明・外部効果
- accent: 主action

warningとdangerを同じ赤へ統合しない。結果不明は独立labelを持つ。

## 7.4 typography

- 日本語本文の可読性を優先
- 数字、金額、期限、case IDは桁が読みやすいfont
- 最小文字sizeを安易に下げない
- 法務文面previewは本文とmetadataを視覚的に分離

## 7.5 共通component

- AppShell
- BottomNav／SideNav
- TaskCard
- StatusBadge
- ConfidenceBadge
- SourceLink
- OriginalPreview
- CaseHeader
- EmptyState
- ErrorPanel
- StaleBanner
- ConflictDialog
- SafeConfirmPanel
- ApprovalSnapshot
- AuditSummary
- Skeleton
- Toast（完了等の補助。重大結果はtoastだけにしない）

各componentはloading、disabled、error、permission stateを持つ。

## 7.6 form

- labelをplaceholderだけで代用しない
- validationはfield近傍とsummary双方
- 入力途中でdataを失う可能性を表示
- irreversible action前にserver側最新版を再取得
- 法務文面の自由編集領域と固定領域を区別

## 7.7 animation

- 状態理解を助ける最小限
- 承認・送信を軽く見せる派手なanimationを使わない
- reduced motionへ対応

## 7.8 accessibility

目標はWCAG 2.2 AA相当とする。【PROVISIONAL】

- keyboard navigation
- focus表示
- screen reader label
- contrast
- errorのtext説明
- landmark
- form association
- zoom 200％

内部1人利用でも、将来社員と加齢・疲労時の誤操作防止のため省略しない。

## 7.9 wording

- system用語をそのまま画面へ出さない。
- 「成功」ではなく何が完了したかを書く。
- 「エラー」だけでなく、人が次に何をすべきかを書く。
- 「承認」と「送信」を明確に分ける。
- 「未確定」と「情報なし」を分ける。
- AI confidenceを過度に精密な％だけで見せない。

---

# 8. 論理architecture

## 8.1 方針

- 既存FastAPIの上にPWAを載せる。【FIXED】
- LINE BotとPWAは別の入口だが、同じapplication serviceを呼ぶ。【FIXED】
- browserからkintone、Drive、CloudSign、InterFAXへ直接接続しない。【FIXED】
- modular monolithを基本とし、独立運用の必要が生じるまでmicroserviceへ分割しない。【PROVISIONAL】
- 外部serviceはadapterで隔離する。
- readとwrite、draftとexecution、approvalとsendを分離する。

## 8.2 論理構成

~~~mermaid
flowchart TD
    U["iPhone／PC PWA"] --> E["Access・FastAPI入口"]
    L["LINE通知・軽量入口"] --> E
    E --> A["Application Services"]
    A --> K["kintone・app-state DB"]
    A --> G["App 38／Journal→GAS→Drive"]
    A --> X["Approval Outbox→Worker→外部"]
~~~

## 8.3 layer

| layer | 責任 | 禁止 |
|---|---|---|
| Presentation | PWA、LINE message、validation表示 | 法務・業務ruleの独自実装 |
| API／Auth | 認証、session、role、case access、request validation | browser credentialで外部serviceへ直結 |
| Application | use case、state transition、idempotency、audit | template正本文言の創作 |
| Domain | case、task、approval、artifact等のrule | infrastructure SDKへの依存 |
| Adapter | kintone、Drive、LINE、CloudSign、InterFAX、日本郵便 | 独自の承認・retry policy |
| Persistence | user、session、audit、approval等 | kintone／Drive正本の無断複製 |

実際のdirectory構成はGate 0でrepoに合わせる。新architectureを理由に全面refactorしない。

## 8.4 source of truth

| data | 正本 | PWA側 |
|---|---|---|
| 顧客・case・進行 | kintoneの該当App | read view |
| 相続人・人物 | App 34等 | source ref付き表示 |
| 財産 | App 35 | source ref付き表示 |
| 原本書類 | Drive | metadata・期限付きpreview導線 |
| 読解結果 | 読解JSON／既存保存先 | confidence・source ref付き表示 |
| R4-3導出履歴 | app-state DBのimmutable DerivationRun | App 36はcurrent projection。App 36だけから旧runを再構成しない |
| user・role・passkey | PWA auth store | 正本 |
| session | server session store | 正本 |
| Q&A履歴 | 専用record store | 正本 |
| approval・artifact version | approval store | 正本 |
| 外部送信attempt | integration／outbox store | 正本 |
| audit event | append-oriented audit store | 正本 |

## 8.5 read model・cache

- PWA表示用read modelは許可するが、元record ID、version、取得時刻を持つ。
- durableな二重正本を作らない。
- server cacheは必要性を測定してから導入する。
- 顧客dataのbrowser cacheは禁止。
- stale許容時間は画面別に定義する。
- 確定・承認前は必ずserverの最新版とversionを再確認する。

## 8.6 data分類・暗号化

| 分類 | 例 | 取扱い |
|---|---|---|
| Secret | API key、cookie secret、private key | secret manager。DB・log・chat・Git禁止 |
| Highly sensitive | 戸籍、住所、送信本文、契約書、approval snapshot | 最小化、TLS、at-rest保護、厳格権限、no-cache、監査 |
| Sensitive metadata | case ref、document ref、recipient hash、操作履歴 | opaque ID、権限制御、retention、redaction |
| Public／static | icon、version付きJS／CSS | integrity確認後cache可 |

approval snapshotの保管方式（app DBへの暗号化保存、またはDrive等のimmutable version＋bytes hash）はOPENとする。ただし、承認時の実bytesを後から一意に取得でき、送信時に可変な正本を再読込みしないことはFIXEDとする。application-level encryptionを採用する場合は、鍵を同じDBへ置かず、key version、rotation、旧data復号、backup restoreまで設計する。

## 8.7 複数正本を跨ぐ整合性

kintone、Drive、app-state DBを一つのtransactionにはできない。cross-system writeを「順にAPIを呼び、例外がなければ成功」と実装しない。

実table形式はGate 1で固定するが、次の不変条件は【FIXED】とする。

- 全cross-system writeにdurableなoperation_idとcommand journalを持つ。
- 外部write後のACK不明は再実行より先にreconciliationする。
- 過去のauditは通常の業務権限で書き換えない。
- 高リスクwriteはauditとjournalの永続化に失敗したらfail closedとする。

各人操作へoperation_idを付け、command journal／sagaとして次を記録する。

1. intent、actor、対象revision、期待結果
2. 正本revisionの再確認
3. adapterへのoperation_id／既存冪等key
4. 新record ID／revision／file version
5. read-after-writeによる期待結果照合
6. audit確定とCOMPLETED

途中停止はPENDING_RECONCILIATIONとし、安全な再開または人照合を可能にする。外部write後にlocal ACKだけ失敗した疑いがある場合、新規writeを繰り返さず、まず正本を検索・照合する。Driveはfile名ではなくfile IDと期待parentで成功判定する。

## 8.8 durable worker運用契約【FIXED】

- 外部実行をFastAPIのrequest内処理、BackgroundTasks、process-memory queue、browser requestの寿命へ結び付けない。
- durable DBのOutboxJobを読む継続workerとして起動し、deploy／process再起動後もQUEUED jobを自動的に再開する。
- startup時と定期的にexpired leaseを回収する。vendor call開始前を証明できるjobだけ再queueし、開始後または不明はUNKNOWNへ送る。
- claimはDBのatomic update／lockとleaseで行い、worker concurrencyを設定値で上限化する。同じidempotency keyを二workerが実行しない。
- connector別kill switchがOFFなら新規jobをleaseせず、実行中jobは安全な境界で停止・照合する。kill switch変更はowner step-upとauditを要する。
- graceful shutdown時は新規leaseを止め、実行中attemptのdurable markerを確定してから終了する。強制終了後もstartup reconciliationで回復する。
- healthはprocess aliveだけでなく、最終poll、最終成功、queue lag、expired lease、UNKNOWN件数、connector別停止状態を返す。
- queue最古滞留、poll停止、lease異常、UNKNOWNをalertし、監視不能時に外部効果を楽観的に継続しない。
- worker credentialはPWA sessionと分離し、vendor実行と必要snapshot読取だけの最小権限にする。

## 8.9 既存資産統合契約【FIXED】

PWAは7月資産を置き換える新systemではなく、既存の業務data planeを安全に束ねるcontrol planeである。

| 層 | 正本・既存資産 | PWAの責任 | 禁止 |
|---|---|---|---|
| data plane | kintone App 21〜38、Drive原本、R／S／Z／D／M／H系列 | source revision付きread、既存use case呼出し | 同じ業務状態を別DBへ複製して二重正本化 |
| control plane | User、Passkey、Session、Audit、Task overlay、Journal、Artifact、Approval、Outbox、Q&A履歴 | 認証、権限、横断表示、訂正、版、承認、外部効果制御 | domain正本の無断上書き |
| execution plane | GAS Drive executor、watcher、durable worker、外部adapter | operation status表示、kill switch、reconciliation | browserまたはLINE request寿命に実行を依存 |

既存LINEとPWAはPresentationだけが異なり、同じApplication Service、revision guard、operation_id、idempotency keyを使う。PWA用に既存ruleを再実装しない。全面refactorではなく、characterization testを置いてuse case単位に境界を抽出する。

## 8.10 trust boundaryとdeployment boundary【FIXED方針】

単一repository／modular monolithは維持できるが、次の実行境界とcredentialは分離する。

1. Public ingress: LINE、Stripe、CloudSign等のwebhook。vendor署名、event idempotency、body limit、最小応答だけを担う。
2. Internal PWA: Access前段＋passkey session。browserからkintone／Drive／vendorへ直結しない。
3. Worker: durable queueをclaimし、必要なadapter writeだけを行う。PWA cookieを持たない。
4. Legacy internal caller: GAS／watcher。header署名、timestamp、nonce、caller IDで認証し、query secretは廃止する。

Public ingressとInternal PWAを同一Railway serviceへ置くかはADRで裁定できるが、外部送信workerは別process／service・別credential environmentとする。【FIXED】public ingressとPWA環境にCloudSign／FAX等のexecute credentialを置かず、workerはpublic listener、PWA cookie secret、不要なkintone write tokenを持たない。同一codebaseでもroute、middleware、credential、rate limit、log policyを境界別に分ける。public routeがinternal application serviceを任意payloadで呼べないことをcontract testで固定する。

## 8.11 App別Source of Truth・write owner

Gate 0で実schemaへmappingするまで、次を論理契約とする。

| domain | AS-IS正本 | primary writer | PWA |
|---|---|---|---|
| 時効援用case | App 21 | 顧客Bot／既存service | read view |
| 相続case・顧客folder番号 | App 26 | 既存case workflow | CaseRefの起点 |
| chat log／顧客reply承認 | App 28／29 | 顧客Bot既存service | 集約表示。第1版で移行しない |
| 発送state・既存封筒 | App 30 | M1／M4／既存関所 | source task表示 |
| 市区町村・同封物master | App 31／32 | 人／管理workflow | read only |
| 戸籍fragment・読解JSON | App 33 | R2／R3 | source表示 |
| 人物・親子edge | App 34 | R4-1＋人の確認service | read／E2 command |
| 財産正規化 | App 35 | S4／S5／S6＋人 | read／訂正入口 |
| 相続人導出projection | App 36 | R4-3b＋人の関所 | DerivationRun表示 |
| 分割割付 | App 37 | 人の入力service | 協議書入力の正本候補 |
| 仕分け状態 | App 38 | sortation＋GAS＋人 | read／E1 command |
| 不動産詳細 | App 25 | 既存S系 | App 35のsource |
| 原scan bundle | Drive | 人＋GAS | metadata／auth proxy preview |
| PWA auth・audit・artifact・approval・outbox | app-state DB | PWA／worker | 正本 |

旧App 23／24／25／27と新App 33〜38は、Gate 0でrecord単位・field単位のowner matrixを作る。便利だからという理由で統合・移送しない。CaseRefは少なくとも unit_type、source_app、source_record_id を組にし、単なるrecord番号をglobal IDとして扱わない。

## 8.12 既存queueとPWA taskのmapping【FIXED方針】

WorkTaskは総称であり、実体を三つに分ける。

- SourceTaskProjection: App 29／30／38等の状態から導出する。破棄・再構築可能。
- TaskAssignmentOverlay: 担当、claim、UI上の保留memoだけを持つ。業務完了stateを持たない。
- PwaNativeTask: 訂正、結果不明、security／integration failure等、app-state DBが正本の新task。

既存queueのcomplete／cancel／supersedeは既存write serviceを呼び、sourceのread-after-write後にprojectionを再取得する。projectionだけを完了させない。

| task type | 業務状態の正本 | PWA projection | write service |
|---|---|---|---|
| SORTATION | App 38 | WorkTask＋TaskBinding | 既存sortation確定service |
| PERSON_MERGE | App 30封筒＋App 34 | WorkTask＋TaskBinding | R4-2共通service |
| PERSON_CONFIRM | App 30封筒＋App 34 | WorkTask＋TaskBinding | R4-2e共通service |
| HEIR_CONFIRM | App 30封筒＋App 36 | WorkTask＋TaskBinding | R4-3b |
| PHYSICAL_SHIPPING | App 30 | read／task view | M1／M4既存service |
| CUSTOMER_REPLY_APPROVAL | App 29 | 第1版は別queue表示 | 顧客Bot既存service |
| EXTERNAL_SEND | 新Approval store | Approval workspace | Outbox worker |

同一source_kind＋source_id＋source_revisionにactive projectionを二つ作らない。source更新でprojectionをSTALEにし、PWAだけを進めない。claim／担当はoverlayに持てるが、業務状態と混同しない。

## 8.13 LINE・PWA・GASのcommand arbitration【FIXED】

- LINE user IDとPWA UserをExternalIdentityLinkで結ぶ。未結合actorをE2／E3の確定者にしない。
- LINEとPWAが同じtaskを処理した場合、source revisionとoperation_idで先着一件だけを成立させ、後着を409／CONFLICTにする。
- LINEは通知とdeep linkを原則とし、E2語彙は段階廃止、E3実行は禁止する。
- PWAはDriveを直接変更しない。App 38等へintentをdurableに記録し、GASがfile IDとexpected parentで移動する。
- Railwayが判定し、GASがDriveを動かす既存境界を変更する場合は独立ADR、credential threat model、二重移動test、rollbackが必須。
- GAS結果が戻らない状態は成功にせずPENDING_RECONCILIATIONとする。

## 8.14 end-to-end lineageと訂正波及【FIXED】

次は書類取込から人物・相続人導出までの共通spineであり、全成果物が全nodeを通る直列pipelineではない。

Drive原本file ID・sha256 → scan bundle／page範囲 → fragment ID・sha256 → SortDecision／App 38 → Drive routing結果 → App 33／25／35 record revision → OCR／読解version → App 34 field source → MergeDecision／人物確認 → DerivationRun／App 36。

ArtifactDependencyはtyped DAGとし、vertical capability manifestごとに必須SourceRefを固定する。

| artifact | 必須branch |
|---|---|
| 職務上請求 | DerivationRun／App 36＋必要戸籍plan＋App 30起票＋App 31自治体＋M1 template |
| 相続放棄 | H系列version＋DerivationRun／App 36＋申述人＋期限＋必要戸籍plan |
| 財産目録 | App 35 records＋AssetSourceLink＋用途mapping＋S3／TemplateVersion |
| 遺産分割協議書 | App 34 confirmed people＋App 35＋App 37 allocation＋条項library／TemplateVersion |
| 委任契約書 | unit type＋case／顧客＋契約類型＋TemplateVersion |
| 請求書／領収書 | 契約・報酬正本＋Stripe InboundEvent＋入金照合＋採番rule＋TemplateVersion |

各branchはArtifactVersion → 必要時ApprovalSnapshot → OutboxJob → ExternalDeliveryへ進む。App 30／31／32、Stripe event、templateもsource revision／hashで固定し、不要なApp 36／37を形式的に依存させない。

訂正時の最低失効ruleは次とする。

| upstream変更 | stale／補償対象 |
|---|---|
| 誤仕分け・case binding | routing、read record、人物、Q&A、artifactを影響解析 |
| App 33読解訂正 | App 34候補、merge、DerivationRun、関係図、関連artifact |
| App 34人物／merge訂正 | App 36、関係図、職務上請求、放棄書類、協議書 |
| App 35財産変更 | 目録4種、協議書、顧客説明資料 |
| App 36相続人変更 | 必要戸籍plan、相続放棄、協議書 |
| App 37割付変更 | 遺産分割協議書 |
| template変更 | 既存artifactを保存し、新規生成だけ新version |
| 外部送信後のsource変更 | 送信済みを消さず、訂正／補償taskを起票 |

失効は可視化だけでなくtest fixture化する。旧artifact・旧decisionを消さず、current projectionだけを更新する。

## 8.15 vertical capability registry

新しい業務ユニットを追加する時は、独自画面を先に作らず、次をmanifestとして登録する。

- unit_type、case source、required role
- available read models
- task typesとeffect level
- template versionsと弁護士承認
- domain engine／required gates
- allowed connectors
- retention／processor
- feature flag、pilot evidence、rollback

相続放棄、相続一般、時効援用を同じ器へ載せる一方、法務rule・料金・purpose文言・templateを混在させない。

---

# 9. 論理data model

以下は論理modelであり、実table名・field名ではない。既存schemaへのmappingを先に作る。

## 9.1 User

| field | 意味 | 制約 |
|---|---|---|
| user_id | 内部一意ID | immutable |
| display_name | 表示名 | 必須 |
| status | ACTIVE／SUSPENDED／REVOKED | REVOKEDはlogin不可 |
| primary_role | 主role | role master参照 |
| created_at | 作成 | audit |
| updated_at | 更新 | version管理 |

## 9.1.1 UserInvitation

| field | 意味 | 制約 |
|---|---|---|
| invitation_id | 一意 | immutable |
| token_hash | 招待tokenのhash | 平文token保存禁止・unique |
| intended_identity_ref | 予定本人の最小識別情報 | logへ平文を出さない |
| intended_role／case_scope | 付与予定権限 | 招待後に勝手に拡張しない |
| state | PENDING／USED／EXPIRED／CANCELLED | state machine |
| expires_at | 期限 | 必須 |
| invited_by／created_at | 招待者 | audit |
| used_by／used_at | 使用結果 | one-time |
| cancelled_by／at | 取消 | audit |
| version | concurrency | 必須 |

招待作成・取消はownerの対象結合step-upを要する。tokenは十分なentropyを持ち、表示・送信後にserverへ平文保存せず、URL・access log・analyticsへの漏えいを最小化する。使用時はPENDING、期限内、未使用、未取消、予定identity／roleを同一transactionで検査し、User／初回登録状態を作ってUSEDへ一回だけ遷移する。

## 9.2 PasskeyCredential

| field | 意味 | 制約 |
|---|---|---|
| credential_id | credential識別 | unique |
| user_id | 所有user | 必須 |
| public_key | 公開鍵 | server保存 |
| sign_count等 | replay補助 | library仕様に従う |
| transports | 利用transport | optional |
| label | user向け端末名 | 機微情報を入れない |
| status | ACTIVE／REVOKED | revoke即時 |
| created_at／last_used_at | 監査 | 必須 |

秘密鍵をserverへ保存しない。同期passkey等の実際の性質を誤って説明しない。

## 9.3 Session

| field | 意味 |
|---|---|
| session_id | server側一意ID |
| user_id | 利用user |
| created_at | 作成 |
| expires_at | absolute期限 |
| last_seen_at | idle期限判定 |
| last_step_up_at | 一般表示用。個別承認の根拠には使わない |
| revoked_at | 失効 |
| device_label | 安全な表示用 |

browser tokenはHttpOnly、Secure、SameSite等を前提に設計し、localStorageへ保存しない。【PROVISIONAL】

sessionが直前再認証済みであることだけを、特定の承認・確定操作へ流用してはならない。高リスク操作はStepUpChallengeを対象ID・version・hashへ個別に結合する。【FIXED】

## 9.4 CaseRef

| field | 意味 |
|---|---|
| case_id | PWAで使う安定ID |
| source_app | source App |
| source_record_id | 元record |
| source_revision | 元version |
| display_name | 表示用 |
| unit_type | 相続放棄等 |
| state | 進行state |
| updated_at | source更新 |

## 9.4.1 共通data表現

- 金額はfloatを使わず、整数円または精度を固定したDecimalで扱う。
- 日付と日時を分ける。法定期限等の「日」はdateとして保持し、表示・判定のtimezoneはAsia/Tokyoを正本とする。
- server内部timestampは一貫した基準で保存し、UIでJST表示する。
- 氏名、住所、旧字、戸籍記載は原文を保存・表示し、比較用normalizationを別field／関数にする。
- normalization結果で原文を上書きしない。
- 電話番号、郵便番号、case ID等は数値計算対象にしない。

## 9.5 CasePermission【DEFERRED UI／FIXED余地】

| field | 意味 |
|---|---|
| user_id | user |
| case_id | case |
| permission | VIEW／WORK／LEGAL_APPROVE等 |
| granted_by | 付与者 |
| effective_from／to | 期間 |

第1版でUIを作らなくても、serviceが全件accessをhard-codeしない。

## 9.6 WorkTask

| field | 意味 | 制約 |
|---|---|---|
| task_id | 一意 | immutable |
| storage_kind | SOURCE_PROJECTION／ASSIGNMENT_OVERLAY／PWA_NATIVE | authorityを決定 |
| case_id | 確定した対象case | 未仕分け時nullable |
| source_document_ref | 起票元書類 | 仕分けtaskでは必須 |
| task_type | SORTATION等 | enum |
| state | task state | state machine |
| priority_reason | 期限・失敗等 | 法務判断を入れない |
| due_at | 期限 | 不明可 |
| source_ref | 起票元 | trace可能 |
| assigned_user_id | 将来担当 | optional |
| claim_expires_at | claim lease | optional |
| superseded_by | 訂正・不要化後の後継 | optional |
| version | concurrency | 必須 |
| created_at／updated_at | 時刻 | 必須 |

machineが出したcase候補はSortProposal／CaseBindingDecisionへ根拠付きで保存する。E1のAUTO_BOUND_UNREVIEWEDではprovisional_case_refとしてroutingに使えるが、人のHUMAN_CONFIRMED case_idと同じ表示・権限・成果物gateにしない。【FIXED】

## 9.6.1 CorrectionRequest

| field | 意味 |
|---|---|
| correction_id | 一意 |
| case_id | 対象case。未仕分け由来ならnullable |
| decision_type／decision_id／decision_version | 旧確定 |
| proposed_decision_payload／hash | 新案 |
| reason | 人の訂正理由 |
| source_revisions | 影響計算時の正本revision |
| impact_snapshot_ref／hash | 依存対象と処理区分 |
| state | DRAFT／IMPACT_READY／PENDING_STEP_UP／APPLYING／COMPLETED／CONFLICT／CANCELLED／FAILED_RECONCILIATION／RECONCILING |
| requested_by／decided_by | actor |
| operation_id | saga |
| version | concurrency |
| superseding_decision_id | 成功後の新decision |

impact snapshotはstep-up後に再計算せず、version一致を検査して同じhashの内容を適用する。訂正適用とlocalな依存失効eventは可能な範囲で同一transaction、外部正本を跨ぐ処理はcommand journalで照合する。

## 9.7 QARecord

| field | 意味 |
|---|---|
| qa_id | 一意 |
| case_id | 必須。横断は別権限 |
| question | 質問 |
| answer | 回答 |
| source_refs | record・document・page・取得時revision |
| confidence_class | 通常／原本推奨／低信頼 |
| uncertainty_notes | 未確定 |
| freshness_state | CURRENT／STALE／ACCESS_REVOKED |
| model／prompt_version | 再現用metadata |
| asked_by | user |
| created_at | 時刻 |

token、system prompt、顧客dataを不要に保存しない。

「Q系read-only」は、相続人・財産・case・書類等の業務正本を変更できないことを意味する。Q&A履歴を保存する場合は、検索processのread-only connectorと、QARecordへappendする専用writerを資格情報・interfaceとも分離する。専用writerはQARecord以外を変更できず、LINE語彙から任意writeを呼べない。分離できない間は、Q系へ業務write tokenを与えない。

source revisionが更新・統合・削除された場合、過去回答をSTALEにする。case accessが失効したuserへ回答本文を再表示しない。検索結果summaryでもSTALEを現行回答のように表示しない。

## 9.8 ArtifactVersion

| field | 意味 | 制約 |
|---|---|---|
| artifact_id | 論理成果物 | versionと複合unique |
| version | immutable版 | 単調増加 |
| case_id | case | 必須 |
| artifact_type | 契約書等 | enum |
| storage_ref | 安全な保存先 | immutable bytesを一意取得 |
| sha256 | bytes hash | 必須 |
| template_id／version | template | 必須 |
| source_revisions | 入力した全record・documentのID／revision | immutable |
| input_snapshot_ref／hash | 人入力を含む生成入力 | immutable |
| validation_result_ref／hash | 生成時validation結果 | immutable |
| created_by／at | 作成 | audit |
| supersedes | 旧版 | optional・同一artifact |

既存versionのbytes、input snapshot、source revisions、template version、validation resultをin-place更新しない。ArtifactVersion自体には可変stateを持たせない。

SOURCE_OUTDATEDは、ArtifactVersionのsource_revisionsと各正本の現行revisionをread時または承認申請前に比較して導出し、状態eventとして記録する。元sourceが変わっても旧versionの入力snapshot・bytes・validation結果を上書きしない。

## 9.8.1 ArtifactGenerationJob

生成開始からArtifactVersion成立までのprocessを表す。GENERATING／GENERATION_FAILEDはArtifactVersionへ無理に保存しない。

| field | 意味 |
|---|---|
| generation_id | 一意 |
| case_id／artifact_type | 対象 |
| requested_by／at | 起票 |
| input_snapshot_ref／hash | 固定した入力 |
| source_revisions | 固定したsource |
| template_id／version | 使用template |
| state | QUEUED／GENERATING／SUCCEEDED／VALIDATION_BLOCKED／GENERATION_FAILED |
| result_artifact_id／version | 成功時の成果物 |
| validation_result_ref | 検証結果 |
| idempotency_key | 二重生成防止 |
| error_class／correlation_id | 安全な障害識別 |

SUCCEEDED時にArtifactVersionを作成する。VALIDATION_BLOCKEDで一部bytesを作るかどうかはtemplate別契約に従うが、「使用可能なDRAFT_READY」と誤表示しない。

## 9.8.2 ArtifactLifecycleEvent・current projection

ArtifactVersionの利用状態はappend-only eventから導出し、immutable recordを更新しない。

| field | 意味 |
|---|---|
| event_id | 一意 |
| artifact_id／version | 対象version |
| event_type | DRAFT_READY／SOURCE_OUTDATED／SUPERSEDED／APPROVAL_REQUESTED |
| caused_by | user／source change／new version |
| source_comparison_ref／hash | outdated判定根拠 |
| related_version／approval_id | 後継・申請 |
| occurred_at | 時刻 |

画面用current stateはこのevent列から再構築できるprojectionとする。projectionが壊れてもArtifactVersionやeventを上書きせず再生成する。SOURCE_OUTDATEDは保存済みsource_revisionsと現行revisionの比較でeventをappendし、後に正本が偶然同じ値へ戻っても旧承認を自動復活させない。

## 9.9 ApprovalSnapshot

承認される「宛先・本文・添付・方法」の不変スナップショットである。申請後は上書きしない。【FIXED】

| field | 意味 | 制約 |
|---|---|---|
| snapshot_id | 一意ID | immutable |
| case_id | case | 必須 |
| action_type | CLOUDSIGN_SEND、FAX_SEND等 | enum |
| payload_schema_version | canonical schema | 必須 |
| canonical_payload | 宛先・sender・subject・body・送信手段等 | immutable・暗号化又は不変参照 |
| canonical_payload_hash | canonical bytesのsha256 | 必須 |
| artifact_refs | ArtifactVersion ID・version・bytes hash | 必須 |
| template_refs | template ID・version | 必須 |
| source_revisions | kintone・Drive等の参照revision | 必須 |
| created_by／at | 作成 | audit |

canonicalizationはencoding、key order、array order、null／省略、改行、正規化規則をschema versionごとに固定する。添付hashは実際に外部へ渡すbytesに対して計算する。実行workerは承認後の可変なkintone・Drive・下書きを再読込みしない。

## 9.10 ApprovalRequest

| field | 意味 |
|---|---|
| approval_id | 一意 |
| snapshot_id | 承認対象ApprovalSnapshot |
| state | DRAFT／PENDING_APPROVAL／APPROVED／REJECTED／EXPIRED／SUPERSEDED／CANCELLED |
| requested_by／at | 申請 |
| decided_by／at | 承認／否認 |
| decision_credential_id | WebAuthn credential |
| decision_assertion_ref | 安全な検証証跡 |
| expires_at | 有効期限 |
| superseded_by | 修正後の新approval |
| version | concurrency |

画面で修正する場合はApprovalRequestをin-place編集しない。新ArtifactVersionと新ApprovalSnapshotを作り、旧ApprovalRequestをSUPERSEDEDにして新規申請する。

## 9.11 StepUpChallenge

| field | 意味 |
|---|---|
| challenge_id | 一意 |
| user_id | 操作者 |
| purpose | SORT_CONFIRM／PERSON_CONFIRM／APPROVE_EXTERNAL等 |
| target_id | task、decision、approval等 |
| target_version | optimistic concurrency用 |
| target_hash | snapshot／decision payload hash |
| random_challenge | WebAuthn challenge |
| expires_at | 短い期限 |
| used_at | one-time消費 |

serverはchallenge、RP ID、origin、signature、user verification、credential state、期限、未使用、target ID／version／hashを一体で検証する。一般sessionの「最近Face ID済み」だけで個別操作を許可しない。

## 9.12 OutboxJob

ApprovalRequestのAPPROVED遷移とOutboxJob作成は同一DB transactionでコミットする。browserはvendorを呼ばず、workerだけが実行する。【FIXED】

| field | 意味 |
|---|---|
| job_id | 一意 |
| approval_id／snapshot_id | 承認と不変対象 |
| connector | CloudSign／FAX／E-CONTENT等 |
| capability | EXECUTE／PREPARE_ONLY |
| idempotency_key | logical delivery単位でunique |
| state | QUEUED／LEASED／SUCCEEDED／FAILED／UNKNOWN／CANCELLED |
| available_at | 実行可能時刻 |
| lease_owner／lease_expires_at | worker crash回復 |
| dispatch_started_at | vendor call直前にdurable記録。nullなら未開始 |
| attempt_count | 回数 |
| created_at／updated_at | 時刻 |

## 9.13 IntegrationAttempt

一回のvendor呼出し証跡。過去attemptは上書きせず、retryは新行とする。

| field | 意味 |
|---|---|
| attempt_id | 一意 |
| job_id／approval_id／snapshot_id | trace |
| attempt_no | 通番 |
| request_fingerprint | 機微情報を除く識別 |
| started_at／finished_at | 時刻 |
| transport_outcome | ACKNOWLEDGED／REJECTED／TIMEOUT／CONNECTION_LOST／UNKNOWN |
| external_receipt_id | 受付番号 |
| error_class | 安定分類 |
| correlation_id | trace |

本文全文、token、vendor raw responseを通常errorへ保存しない。

## 9.14 ExternalDelivery

logicalな一送信の現在地を表すaggregateとし、raw attemptと分離する。

| field | 意味 |
|---|---|
| delivery_id | 一意 |
| job_id | outbox |
| state | QUEUED／EXECUTING／ACCEPTED／DELIVERED_OR_COMPLETED／FAILED_RETRYABLE／FAILED_FINAL／UNKNOWN／RECONCILING／PREPARED_FOR_HUMAN |
| external_receipt_id | vendor識別 |
| resolved_by／at | 人照合 |
| resolution_evidence_ref | 証跡 |
| version | concurrency |

人がUNKNOWNを解決する時もattemptを書き換えず、resolution eventと証拠をappendする。vendorが「受付」したことと「相手に到達／処理完了」を同一のSENTで潰さない。

## 9.15 AuditEvent

append-orientedとし、通常の業務update権限で過去eventを書き換えられない構造を必須とする。高リスクwriteはaudit永続化と同時に成功しなければfail closedとする。【FIXED】

## 9.16 ExternalIdentityLink

| field | 意味 |
|---|---|
| provider／external_subject | LINE等の外部主体 |
| user_id | PWA User |
| assurance_state | UNVERIFIED／VERIFIED／REVOKED |
| verified_by／at | ownerによる結合証跡 |
| last_seen_at | 監査用 |

外部subjectだけでE2／E3 actorを推定しない。account変更・退職・端末紛失時はlinkをrevokeし、既存pending commandを無効化する。

## 9.17 InboundEvent・IngestionReceipt

public webhook、GAS、watcherから受けたrequestを、ACK後のprocess memoryへ預けない。

| model | 必須field |
|---|---|
| InboundEvent | provider、external_event_id、caller_id、payload_ref／hash、received_at、signature_result、state、attempt_count |
| IngestionReceipt | source_file_id、source_sha256、ingest_type、case_hint、first_seen_at、last_outcome、downstream_refs、idempotency_key |

external event IDまたはcaller＋source ID＋hashへunique制約を置く。payloadに顧客dataがある場合は暗号化／不変参照とretentionを定義し、通常logへ複製しない。

## 9.17.1 ConversationSession・PendingCommand

顧客Bot／業務Botの会話と関所をprocess memoryへ持たない。

| model | 必須field |
|---|---|
| ConversationSession | provider subject、channel role、case ref、flow type、state、version、last event ID、expires_at |
| PendingCommand | command ID、actor link、target refs／revisions、parsed payload／hash、effect level、state、expires_at、used_at |

同じexternal event replayでstate遷移は一回だけとする。期限切れ・使用済みcommand、別user／channel／caseへの転用を拒否する。process restart後はdurable stateから再構成し、memory fallbackで成功ACKしない。

## 9.18 SourceRef・DocumentEnvelope・DocumentSegment

| model | 主なfield |
|---|---|
| SourceRef | source_system、app_id、record_id、revision、file_id、sha256、page_from／to、field_path |
| DocumentEnvelope | original_file_id、original_sha256、page_count、received_at、case_hint、retention |
| DocumentSegment | envelope_id、page range、fragment_sha256、doc_type proposal、storage_ref、derivation_version |

原本は無分割の正本、segmentは再生成可能な派生物である。segmentの削除・再生成で原本を変更しない。OCR text、thumbnail、読解JSONはsegmentとprocessing versionへ結ぶ。

## 9.19 CaseBindingDecision

| field | 意味 |
|---|---|
| binding_id／version | append型decision |
| document_ref | envelopeまたはsegment |
| proposed_case_refs | 候補集合 |
| selected_case_ref | provisionalまたはhuman confirmed |
| decision_kind | AUTO_BOUND_UNREVIEWED／HUMAN_CONFIRMED／CORRECTED |
| evidence | model、prompt、confidence、candidate set、actor |
| source_hash／revision | stale検出 |
| supersedes | 訂正前decision |

## 9.20 TaskBinding

| field | 意味 |
|---|---|
| work_task_id | PWA projection |
| source_kind／source_id／source_revision | App 29／30／38等の正本 |
| source_state | 取得時state |
| projection_state | 表示用mapping |
| overlay_owner／claim | PWA固有の担当 |
| freshness | CURRENT／STALE／MISSING |

source taskとprojectionの件数・state差分を定期照合し、projection破損は再構築する。

## 9.21 DerivationRun

R4-3の一回の導出をimmutable runとして保存し、App 36はcurrent projectionとする。

| field | 意味 |
|---|---|
| derivation_run_id | 一意 |
| decedent_person_id | 被相続人 |
| engine_version／frozen_case_version | rule再現 |
| input_person_revisions | App 34入力 |
| result_payload／hash | 相続人・required_persons・根拠 |
| lawyer_flags | 要弁護士flag |
| human_state／decided_by | 人の関所 |
| supersedes_run_id | 後継 |

## 9.22 MergeDecision・PersonAlias

候補score logicは維持するが、長期正本では敗者人物を物理削除しない。

| model | 主なfield |
|---|---|
| MergeDecision | decision_id、winner、aliases、evidence、source revisions、actor、state、supersedes |
| PersonAlias | person_id、canonical_person_id、state、merge decision、pre-merge snapshot |

MERGED recordは通常viewから除外するが、source・添付・旧revisionを保持する。unmergeは旧decisionを書き換えず、CorrectionRequestから後継decisionを作る。業務runtimeからApp 34人物recordの物理削除を恒久禁止し、通常tokenから削除権限を除く。復元toolは過去に削除済みのrecord回収専用であり、新規削除の許可条件ではない。

## 9.23 TemplateVersion

| field | 意味 |
|---|---|
| template_id／version | immutable識別 |
| artifact_type／unit_type／purpose | 適用範囲 |
| storage_ref／sha256 | 正本bytes |
| mapping_version／clause_library_version | 生成rule |
| approved_by／at | 弁護士・owner承認 |
| effective_from／retired_at | 適用期間 |

旧templateを削除せず、過去ArtifactVersionを同じbytesとmappingで再現可能にする。templateの作業copyとAPPROVED正本を混同しない。

## 9.24 ArtifactDependency

| field | 意味 |
|---|---|
| artifact_id／version | 派生成果物 |
| upstream_ref | SourceRef、Decision、DerivationRun、App 37等 |
| dependency_type | required／informational |
| upstream_hash／revision | 生成時固定値 |
| invalidation_policy | stale／regenerate／compensation |

## 9.25 AssetSourceLink

App 35の各財産fieldが、登記、評価証明、残高証明、通帳、人入力のどれに由来するかをfield単位で結ぶ。複数sourceが衝突した時は値を黙って上書きせず、current choiceと両sourceを残す。

## 9.26 OperationJournal

| field | 意味 |
|---|---|
| operation_id | 人操作／system処理の一意ID |
| operation_type／effect_level | policy |
| actor／source | 実行主体 |
| expected_revisions | CAS |
| steps | intent、adapter call、read-after-write、compensation |
| state | PLANNED／RUNNING／COMPLETED／PENDING_RECONCILIATION／COMPENSATING／FAILED |
| correlation_id | log・alert結合 |

App 30、App 38、kintone、GAS、Driveを跨ぐ一連の処理に同じoperation_idを通す。

## 9.27 ScheduledRun

| field | 意味 |
|---|---|
| schedule_key／intended_run_at | uniqueな予定実行 |
| state | DUE／LEASED／COMPLETED／FAILED／MISSED／RECONCILING |
| lease_owner／expires_at | 多instance重複防止 |
| attempt／completed_at | 実行証跡 |
| catch_up_policy | 再起動後に一回だけ補完するか |

process内timerだけを正本にしない。法定／社内期限監視はmissed runをdead-man alertし、復旧後のcatch-upで同一予定時刻を二重実行しない。

---

# 10. state machine

## 10.0 UserInvitation

~~~mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> USED
    PENDING --> EXPIRED
    PENDING --> CANCELLED
~~~

USED／EXPIRED／CANCELLEDはterminalとする。同じtokenの二重使用、期限切れ使用、取消と使用の競合はDB transactionとversion／unique制約で一件だけ成立させる。

## 10.1 WorkTask

~~~mermaid
stateDiagram-v2
    [*] --> UNASSIGNED
    UNASSIGNED --> CLAIMED
    UNASSIGNED --> ASSIGNED
    CLAIMED --> UNASSIGNED
    CLAIMED --> IN_PROGRESS
    ASSIGNED --> IN_PROGRESS
    ASSIGNED --> UNASSIGNED
    IN_PROGRESS --> COMPLETED
    IN_PROGRESS --> ON_HOLD
    ON_HOLD --> IN_PROGRESS
    ON_HOLD --> UNASSIGNED
    ON_HOLD --> ASSIGNED
    UNASSIGNED --> CANCELLED
    CLAIMED --> CANCELLED
    ASSIGNED --> CANCELLED
    IN_PROGRESS --> CANCELLED
    ON_HOLD --> CANCELLED
    UNASSIGNED --> SUPERSEDED
    CLAIMED --> SUPERSEDED
    ASSIGNED --> SUPERSEDED
    IN_PROGRESS --> SUPERSEDED
    ON_HOLD --> SUPERSEDED
~~~

このstate machineはPwaNativeTaskに適用する。SourceTaskProjectionのstateはsource Appから導出し、TaskAssignmentOverlayは担当／claimだけを遷移させる。legacy sourceのCOMPLETED／CANCELLED／SUPERSEDEDは既存write service、task typeごとの権限・evidenceを要求し、sourceのread-after-write後にprojectionへ反映する。claimはleaseとversionを持ち、vendor call等の外部効果を伴わない通常taskのCLAIMED lease失効だけUNASSIGNEDへ戻せる。IN_PROGRESS中の利用者離脱・端末故障・user停止時は、ownerが理由付き・step-up・version CASで強制ON_HOLDにした後、ON_HOLD→UNASSIGNEDまたはASSIGNEDへ回収する。再割当はASSIGNED／CLAIMED／ON_HOLD等からownerがversion CASで行い、権限検査、旧・新担当への通知、auditを要する。PwaNativeTaskのCANCELLED／SUPERSEDEDは理由と後継IDを必須とする。COMPLETEDは上書きで戻さず、誤りは新CorrectionRequest／訂正taskから参照する。

## 10.2 ArtifactGenerationJob・ArtifactVersion

~~~mermaid
stateDiagram-v2
    [*] --> QUEUED
    QUEUED --> GENERATING
    GENERATING --> SUCCEEDED
    GENERATING --> VALIDATION_BLOCKED
    GENERATING --> GENERATION_FAILED
    VALIDATION_BLOCKED --> QUEUED
~~~

生成processの状態はArtifactGenerationJobに保存する。成立したArtifactVersionには状態遷移を持たせず、DRAFT_READY／SOURCE_OUTDATED／SUPERSEDED等の画面状態はArtifactLifecycleEventから導出する。画面上の「修正」「再生成」は新しいgeneration jobと別versionを作り、旧版へSUPERSEDED eventをappendする。

## 10.3 ApprovalRequest

~~~mermaid
stateDiagram-v2
    [*] --> DRAFT
    DRAFT --> PENDING_APPROVAL
    PENDING_APPROVAL --> APPROVED
    PENDING_APPROVAL --> REJECTED
    PENDING_APPROVAL --> EXPIRED
    PENDING_APPROVAL --> SUPERSEDED
    PENDING_APPROVAL --> CANCELLED
    APPROVED --> CANCELLED
~~~

- payload変更は旧RequestをSUPERSEDEDにし、新snapshot・新Requestを作る。
- APPROVED遷移とQUEUED OutboxJob作成は同一DB transactionである。
- APPROVEDからの取消は、対応jobがQUEUEDでworker未leaseの場合だけ、ApprovalRequest→CANCELLEDとOutboxJob→CANCELLEDを同一transaction・version CASで行う。
- jobがLEASED、またはdeliveryがEXECUTING以降なら取消を拒否し、必要なら別の訂正・補償taskを作る。

## 10.4 OutboxJob

~~~mermaid
stateDiagram-v2
    [*] --> QUEUED
    QUEUED --> LEASED
    QUEUED --> CANCELLED
    LEASED --> SUCCEEDED
    LEASED --> FAILED
    LEASED --> UNKNOWN
    LEASED --> QUEUED
~~~

LEASED→QUEUEDは、lease失効かつvendor call開始前であることをdurable markerで証明できる時だけ許可する。vendor call開始後または開始有無を証明できない時はUNKNOWNへ進め、reconciliationより先にretryしない。

## 10.5 ExternalDelivery

~~~mermaid
stateDiagram-v2
    [*] --> QUEUED
    QUEUED --> EXECUTING
    EXECUTING --> ACCEPTED
    EXECUTING --> PREPARED_FOR_HUMAN
    EXECUTING --> FAILED_RETRYABLE
    EXECUTING --> FAILED_FINAL
    EXECUTING --> UNKNOWN
    ACCEPTED --> DELIVERED_OR_COMPLETED
    UNKNOWN --> RECONCILING
    RECONCILING --> ACCEPTED
    RECONCILING --> DELIVERED_OR_COMPLETED
    RECONCILING --> FAILED_FINAL
~~~

- workerはApprovalSnapshotだけを読み、可変な正本を再読込みしない。
- UNKNOWNから自動でEXECUTINGへ戻さない。
- ACCEPTED／DELIVERED_OR_COMPLETED後の取消・訂正は別task。
- PREPARE_ONLY connectorはPREPARED_FOR_HUMANで止まり、人の送信を自動完了と記録しない。

## 10.6 SortDecision

case bindingとDrive routing executionを一つのstateで表さない。

binding:

~~~mermaid
stateDiagram-v2
    [*] --> DETECTED
    DETECTED --> CANDIDATES_READY
    CANDIDATES_READY --> AUTO_BOUND_UNREVIEWED
    CANDIDATES_READY --> HUMAN_REVIEW_REQUIRED
    AUTO_BOUND_UNREVIEWED --> HUMAN_CONFIRMED
    HUMAN_REVIEW_REQUIRED --> HUMAN_CONFIRMED
    AUTO_BOUND_UNREVIEWED --> CORRECTION_REQUIRED
    HUMAN_CONFIRMED --> CORRECTION_REQUIRED
~~~

routing execution:

~~~mermaid
stateDiagram-v2
    [*] --> NOT_REQUESTED
    NOT_REQUESTED --> INTENT_RECORDED
    INTENT_RECORDED --> EXECUTING
    EXECUTING --> EXECUTED
    EXECUTING --> ROUTING_FAILED
    EXECUTED --> CORRECTION_INTENT
    CORRECTION_INTENT --> EXECUTING
~~~

machineはAUTO_BOUND_UNREVIEWEDへ進められるが、HUMAN_CONFIRMEDへ進めない。auto routingはE1かつ訂正可能な内部効果に限定する。高リスクdoc type、候補差が小さい、case候補が複数、OCR低品質、model／schema不整合では必ずHUMAN_REVIEW_REQUIREDへ送る。

人が既にauto-routed済みのbindingを確認しても、新しいrouting intentを自動生成しない。同じfile ID・同じexpected parent・同じbinding versionにactive routing operationは一件だけとする。訂正時だけ新しいoperation_idと旧／新parentを持つCORRECTION_INTENTを作る。

## 10.7 訂正と波及失効

誤仕分け・誤名寄せを上書きで消さない。訂正開始時に次の影響を表示し、新しいdecisionが旧decisionをsupersedeする。

~~~mermaid
stateDiagram-v2
    [*] --> DRAFT
    DRAFT --> IMPACT_READY
    IMPACT_READY --> PENDING_STEP_UP
    PENDING_STEP_UP --> APPLYING
    PENDING_STEP_UP --> CONFLICT
    APPLYING --> COMPLETED
    APPLYING --> FAILED_RECONCILIATION
    FAILED_RECONCILIATION --> RECONCILING
    RECONCILING --> COMPLETED
    RECONCILING --> CONFLICT
    DRAFT --> CANCELLED
    IMPACT_READY --> CANCELLED
    CONFLICT --> IMPACT_READY
~~~

1. Drive回送先・ファイルmanifest
2. OCR・読解結果
3. 人物グラフ・順位導出
4. 生成済みArtifactVersion
5. 承認待ち・承認済みApprovalRequest
6. 実行前OutboxJobと実行済み外部効果

依存成果物はSOURCE_OUTDATED、SUPERSEDEDまたはREVIEW_REQUIREDへ遷移し、実行前承認は失効する。既に生じた外部効果は巻き戻ったと表示せず、補償・訂正taskとauditを作る。訂正は必ずstep-upと影響確認を要する。

APPLYING中のtimeout・crashで新しいCorrectionRequestを作ったり全工程を先頭から盲目的に再実行したりしない。同じoperation_idのcommand journalから各stepの実行済み証拠を照合し、未実行stepだけを冪等再開する。外部writeの結果が不明ならFAILED_RECONCILIATION→RECONCILINGで人照合し、完了または新しいCONFLICTへ進める。

## 10.8 legacy承認からPWA承認への移行

同じworkflowでApp 30とApprovalRequestを同時に承認正本にしない。

~~~mermaid
stateDiagram-v2
    [*] --> LEGACY_APP_AUTHORITATIVE
    LEGACY_APP_AUTHORITATIVE --> PWA_SHADOW_READ_ONLY
    PWA_SHADOW_READ_ONLY --> PWA_AUTHORITATIVE
    PWA_AUTHORITATIVE --> LEGACY_APPROVAL_DISABLED
    PWA_SHADOW_READ_ONLY --> LEGACY_APP_AUTHORITATIVE
~~~

- M1／M4の物理発送は、独立migrationが承認されるまでApp 30を正本とする。
- CloudSign／FAX等の新connectorは新ApprovalRequestを正本とする。
- e内容証明はApprovalRequestで文面・添付を固定し、CU支援後の最終送信は大野が行う。
- migrationごとにdual writeを避け、shadow差分0、rollback、禁止遷移test、feature flagを要求する。
- App 30の「承認待ち→承認済み」をserverが自動遷移しない既存不変条件を、PWA導入を理由に黙って削除しない。

---

# 11. 論理API契約

実path・schemaはGate 0でOpenAPI等へ落とす。全write endpointはauth、role、case access、version、idempotencyを検査する。

## 11.1 共通response

成功:

~~~json
{
  "data": {},
  "meta": {
    "correlation_id": "opaque-id",
    "source_revision": "opaque-version",
    "generated_at": "ISO-8601"
  }
}
~~~

失敗:

~~~json
{
  "error": {
    "code": "STABLE_MACHINE_CODE",
    "message": "人が理解できる説明",
    "retryable": false,
    "correlation_id": "opaque-id"
  }
}
~~~

stack、token、外部response全文を返さない。

## 11.2 read

- GET today summary
- GET tasks
- GET task detail
- GET cases
- GET case dashboard
- GET case people／assets／documents／timeline
- GET questions／question detail
- GET approvals／approval detail
- GET artifacts／artifact version／preview
- GET external deliveries／attempt history

listはpagination、filter、stable sortを持つ。大量取得を前提にしない。

## 11.3 write

- POST sort confirmation
- POST sort correction request／correction decision
- POST person decision
- POST Q&A request（read use caseだがrecord作成）
- POST artifact generation
- POST artifact draft save（常に新version。旧versionを上書きしない）
- POST approval request
- POST approval decision
- POST approval cancel（実行前のみ）
- POST task claim／assign／reassign（overlay）。PwaNativeTaskだけstart／hold／resume／complete／cancel／supersedeを直接遷移
- POST legacy task actionはsource_kind別の既存write serviceを呼び、source revisionを再取得
- POST integration outcome resolution（ownerのみ）
- POST user invitation／invitation cancel（ownerのみ）
- POST user role change／suspend／reactivate／revoke（ownerのみ）
- POST credential registration begin／finish／revoke
- POST session revoke／revoke-all
- POST user task recovery（ownerのみ）

Q&A requestから業務正本を更新してはならない。履歴appendは前項の専用writerに限定する。

browser向けの「送信実行」endpointは作らない。approval decisionのAPPROVED遷移時に、同一DB transactionでOutboxJobを一つ作る。vendor呼出しはworker専用とし、worker認証境界からのみ実行可能とする。【FIXED】

## 11.3.1 step-up必要操作【FIXED】

| 操作 | 通常session | 個別step-up | 結合対象 |
|---|---:|---:|---|
| 一覧・詳細・原本表示 | 必要 | 不要 | case access |
| filter・検索・一時入力 | 必要 | 不要 | session |
| 下書き保存 | 必要 | 不要 | artifact ID／expected version |
| machine auto binding／routing | 対象外 | 不要 | policy version／source hash／candidate evidence |
| E1の保留・同じbindingのrouting再試行 | 必要 | 不要 | task ID／version／operation ID |
| HUMAN_CONFIRMED case binding | 必要 | 必要 | task ID／version／decision hash |
| 誤仕分け訂正 | 必要 | 必要 | old／new binding・impact hash |
| 名寄せ・人物確定 | 必要 | 必要 | decision ID／version／payload hash |
| その他の誤確定訂正 | 必要 | 必要 | old/new decision・impact hash |
| 承認申請 | 必要 | 必要 | snapshot ID／hash |
| 対外承認・取消 | 必要 | 必要 | approval ID／version／snapshot hash |
| UNKNOWNの人照合解決 | 必要 | 必要 | delivery ID／version／evidence hash |
| 招待・role・user停止・credential／session・kill switch変更 | 必要 | 必要 | 対象user／設定のversion／change hash |

step-up完了後に対象のversionまたはhashが変わった場合は再認証する。画面操作の手間を理由に個別結合を省略しない。

## 11.4 concurrency

write requestは次を含む。

- target ID
- expected version
- idempotency key
- action-specific payload

serverがversion不一致を検知した場合409相当でCONFLICTを返し、自動mergeしない。

## 11.5 deep link

- LINEからtask_idまたはcase_idのopaque IDだけを渡す。
- deep linkに顧客氏名・token・法務情報を入れない。
- login後にserver側でaccessを再評価する。
- link期限が必要な場合も、固定session tokenをURLへ載せない。

---

# 12. security・privacy設計

## 12.1 保護対象

- 顧客識別情報
- 戸籍・住民票・財産・取引data
- 法務判断・相談内容
- passkey credential metadata
- session
- API token・secret
- 承認artifact
- audit・外部送信証跡

## 12.2 trust boundary

1. iPhone／PC browser
2. Access前段
3. FastAPI
4. auth／approval store
5. kintone／Drive
6. LINE
7. 外部送信service
8. 開発・review環境

境界を跨ぐたび、認証、最小権限、入力検証、log redactionを定義する。

## 12.3 threatとcontrol

| threat | 例 | 主control |
|---|---|---|
| 認証迂回 | Railway直URL | Access前段＋origin側制限＋server auth |
| IDOR | URLのcase_id差替え | server case access検査 |
| session窃取 | browser data流出 | Secure HttpOnly cookie、短期限、失効 |
| CSRF | write誘導 | SameSite、CSRF対策、origin検査 |
| XSS | OCR・書類text | escape、CSP、dangerous HTML禁止 |
| prompt injection | OCR内の命令 | documentをdata扱い、tool allowlist |
| 誤承認 | 承認後本文差替え | immutable artifact＋hash＋再認証 |
| 二重送信 | timeout retry | idempotency、UNKNOWN、人確認 |
| secret露出 | log／commit | secret manager、redaction、scan |
| 端末残存 | Service Worker cache | static shellのみcache、API no-store |
| 案件混線 | 検索・候補誤り | case拘束、source ref、確認UI |
| Webhook偽装 | 外部callback | vendor実機能に合わせたsignature又はendpoint secret・再照合 |
| 過剰権限 | 共通token | service・環境・action別credential |

## 12.4 network前段

- 推測不能URLをsecurity controlとして扱わない。
- Cloudflare有料planを前提とし、Access前段を必須とする。【FIXED】具体的plan／機能組合せはO-02で、Access、WAF、rate limit、Bot対策、log retention、origin保護を比較する
- 原則としてoriginをAccess edge以外から到達不可にする。基盤上それができない場合は、保護routeごとにAccess JWTのsignature、issuer、audience、expiryをserverが検証する。単なる`X-*`ヘッダを信頼しない。【FIXED】
- Webhook公開pathは通常PWA入口と分離し、service別検証を行う。
- rate limitはauth前・auth後・高危険endpointで別設定を検討する。

Webhookはsignature機能がvendorに実在する場合のみsignature検証を要件化する。signatureがない場合は接続先ごとのendpoint secret、可能なsource restriction、replay対策を行い、callbackだけで成功確定せずvendor API・管理画面と再照合する。信頼性はO-11で実機確認する。

## 12.5 browser

- Service Workerはbuild時に固定したversioned static assetのpositive allowlistだけをcacheする。same-originという理由でruntime cacheしない。
- 認証後HTML、全API JSON、PDF、thumbnail、戸籍data、Q&A、approvalは`Cache-Control: private, no-store`とし、Service Workerもcacheしない。
- app shellにuser固有HTML・顧客dataを埋め込まない。offline時はstaticな障害画面だけを表示し、過去dataで業務を続行させない。
- localStorage／IndexedDBへ顧客data・session tokenを保存しない。
- clipboardへcopyしたdataを自動管理できると誤認しない。copy機能は最小化。
- CSP、frame-ancestors、content type、referrer policyを設定する。

## 12.6 passkey運用

- userVerification required
- 固定RP ID
- credential登録は既存の強いsession＋owner許可
- primary iPhoneのplatform passkeyに加え、FIDO2／passkey対応hardware security key 2本と予備端末1台を購入・登録する。【CLOSED-04／FIXED】
- security key 2本は同じ場所に保管せず、PIN／復旧情報をkeyと一緒に置かない。具体的保管場所は大野だけが非公開runbookへ記録する
- 予備端末は最新OS、画面lock、Find My／remote wipe相当、顧客data offline保存なしとし、通常業務には使わない
- recoveryは登録済みbackup credentialでlogin→紛失credentialと全sessionをrevoke→新primary credential登録→audit／alert確認の順とし、password bypassを作らない
- security key／予備端末の登録、recovery、revokeをstagingで実演し、productionでは半年ごと・端末変更時にread-only recovery drillを行う
- 端末紛失時の全session・credential失効
- 退職・role変更時の即時失効
- credential一覧とlast usedをownerが確認

## 12.7 data最小化

- PWAは画面に必要なfieldだけ取得
- logは識別用ID中心
- Q&Aへ不要な全案件dataを渡さない
- Codex reviewには匿名fixture
- error reportへ原文を貼らない

## 12.8 retention【OPEN】

次は大野の方針と既存業務要件を確認して確定する。

- Q&A履歴の保存期間
- audit eventの保存期間
- external attempt log
- revoked session
- PWA用thumbnail
- test artifact

未確定のまま無期限保存をdefaultにしない。

## 12.9 legacy immediate containment【FIXED優先順】

PWA画面開発より先に、次をproduction exposureと実callerを確認して封じる。

1. /scan と /ocr/fixed-assetをAccess／service authなしで到達不能にする。正規ingestへの移行計画ができるまでfail closed。
2. ingest query tokenをredactし、header署名方式を追加する。GAS／watcherを一つずつ移行し、旧query方式停止後にtoken rotation。
3. business notification token未設定時のcustomer token fallbackを削除し、設定不備では送信せずalert。
4. Stripe／CloudSign webhookをevent journal、真正性検証、idempotency、read-after-write、reconciliationへ移す。検証不能payloadで業務stateを進めない。
5. process memoryのBackgroundTasks、会話state、重複setをdurable InboundEvent／Receiptへ移す。
6. PIIを含むlog、LINE本文、Drive URLをredactし、過去logの保存先・閲覧者・削除可能性を調査する。
7. R4-2の新規物理削除をfeature flagと権限の両方で恒久停止し、soft mergeへ移行する。過去削除recordの復元性は別に実証する。
8. dependencyをlockし、known vulnerability、license、SBOMをrelease evidenceへ入れる。

containmentにより現行業務が止まる場合、危険経路を開けたまま進むのではなく、ownerに停止範囲・手動fallback・復旧条件を提示して裁定を得る。

## 12.10 PDF・OCR・AI入力防御

- MIMEとmagic bytesを一致検査し、暗号化PDF、壊れたxref、巨大page、画像展開比を安全に拒否する。
- file size、page数、解像度、OCR時間、model token、case当たり日次costに上限を持つ。
- uploadされたfilenameをpath、header、logへ無加工で使わない。
- PDF parser／image converterをresource limit付きの隔離processで実行し、temporary fileをfinallyで削除する。
- OCR内命令をuntrusted dataとして区切り、modelへcredential、任意network、write toolを与えない。
- AI outputはversion付きschema、enum allowlist、candidate allowlistで検証し、free-form JSONを業務writeへ直接渡さない。
- low confidence、schema violation、candidate外ID、page欠落はsafe-side queueへ送る。

### service request署名contract【FIXED】

GAS／watcherのHMACは key ID、caller ID、HTTP method、normalized path、timestamp、nonce、content SHA-256 をversion付きcanonical encodingで署名する。serverはbounded clock skew、nonce一回使用＋TTL、content hash、constant-time比較、caller別scopeを検査する。key rotation中だけ期限付きdual keyを許し、query secretとのdual accept期間を最短化する。replay、別path転用、body 1byte変更、期限外、unknown key IDを全て拒否する。

## 12.11 privacy・processor register

- Claude、Google Vision、kintone、Drive、LINE、CloudSign、Stripe、FAX等について、送信data、目的、保存期間、region、再委託、学習利用、契約、削除手順、ownerを台帳化する。
- production dataをCI、staging、Codex fixture、screenshot共有へ持ち込まない。匿名化corpusを別に作る。
- access log、APM、error tracker、Railway logがquery、header、request bodyを採取していないことを実出力で検査する。
- document previewはserver auth proxyまたは短命opaque referenceを使い、Drive credentialやsecretをURLへ出さない。
- PDF／thumbnailへno-store、nosniff、no-referrerを適用し、download filenameへ氏名を入れない。
- bfcache復帰時はsession、case access、source revisionを再検査する。

---

# 13. Reliability・observability・復旧

## 13.1 構造化log

通常logに含める候補:

- timestamp、level、environment、release SHA
- request／correlation ID
- opaque user ID、case ID、task／approval／attempt ID
- operation、outcome、duration
- dependency、attempt、safe error code

通常logに含めない:

- token、cookie、API key
- WebAuthn raw data
- 氏名、住所、戸籍本文、相談本文
- 契約書・FAX・Q&Aの本文全文
- vendor raw request／response

## 13.2 metrics

- API count、error rate、p50／p95 latency
- dependency別error・latency
- login／step-up失敗、rate limit
- queue件数、最古滞留時間
- approval pending／expired
- outbox lag、retry、UNKNOWN
- audit write failure
- 仕分け時間、tap数、候補的中率
- 承認所要時間、差戻し率
- Service Worker version分布（個人追跡しない）

## 13.3 alert

即時:

- 未承認execute試行
- approval hash不一致
- UNKNOWN delivery
- audit永続化失敗
- 外部送信kill switch作動
- Access迂回疑い
- backup／restore検証失敗

営業時間内:

- queue滞留
- dependency error増加
- read view failure
- passkey／session失敗増加

## 13.4 SLO候補【PROVISIONAL】

実traffic・plan・vendor能力を測定して確定する。

| 指標 | 初期候補 |
|---|---:|
| PWA／API月間可用性 | 99.5％ |
| read API p95 | 2秒以内 |
| internal write p95 | 3秒以内 |
| 承認後queue可視化 | 99％が10秒以内 |
| LINE通知 | 95％が5分以内 |
| app-state RPO | 15分以内 |
| app-state RTO | 4時間以内 |

安全KPIを性能より優先する。

- 無承認送信0
- wrong recipient 0
- E2／E3のmachine自動確定0（E1のAUTO_ROUTED_UNREVIEWEDは別計測）
- UNKNOWN無照合再送0
- client cacheへの守秘data残存0

## 13.5 dependency劣化

| 障害 | 許可 | 禁止 |
|---|---|---|
| kintone read不可 | shell、障害表示 | 古いdataで確定・承認 |
| kintone write不可 | 入力保持方針に従う | 成功表示、無限retry |
| Drive read不可 | metadata、障害表示 | 原本未確認の確定 |
| LINE障害 | PWA queue継続 | task消失扱い |
| vendor障害 | approved job待機／停止 | 無制限retry |
| WebAuthn不可 | policyで許可したread | passwordだけで承認 |
| audit store不可 | read-only劣化 | 高危険write継続 |

## 13.6 backup

- app-state DB: encrypted backup、可能ならpoint-in-time recovery
- migration: Git正本
- code・設定schema: Git正本。secret値なし
- secret inventory: 種別、owner、rotate方法
- kintone／Drive: vendor version／export能力を確認
- audit: app-state以上の復旧性を検討

backup成功logだけで完了にしない。隔離環境でrestoreし、schema、件数、参照整合性、起動を確認する。初期候補は四半期ごとのrestore演習、重要migration前の個別snapshotとする。【PROVISIONAL】

## 13.7 rollback

- release SHAを表示し、直前の既知良好版へ戻せる
- DBはexpand→backfill→switch→contract
- destructive migrationを同一releaseで行わない
- connector別kill switch
- code rollbackと既発生の外部効果のreconciliationを分ける
- Service Workerの新旧client併存を考慮する

## 13.7.1 manual fallback

PWA停止が法定期限・顧客対応を止めないよう、既存のkintone／Drive／手動送信経路を緊急fallbackとして文書化する。fallback中の操作も後でaudit・台帳へreconcileする。障害中にPWAだけへ入力を溜め込み、復旧時に無条件一括送信しない。

## 13.8 incident

| severity | 例 | 初動 |
|---|---|---|
| SEV-1 | 誤送信、漏えい、auth迂回、data破壊 | 外部送信停止、session／secret失効、証拠保全、大野報告 |
| SEV-2 | UNKNOWN複数、権限不具合、重要機能停止 | 関連feature停止、影響特定 |
| SEV-3 | 一部画面障害、遅延 | release停止、通常修正 |

初動:

1. connector kill switch
2. incident ID・時刻・発見者
3. deploy SHA・audit・outbox・vendor状態保全
4. 推測retry・削除・status手修正禁止
5. 影響case・宛先・artifact版・actor特定
6. secret露出ならrotate・session失効
7. 顧客対応・法的判断は大野
8. staging再現、test追加、Codex review
9. localとvendorの全件reconcile
10. postmortem

## 13.9 既存component監視

PWA DBだけを監視して「全体正常」としない。

| component | heartbeat／metric | alert条件例 |
|---|---|---|
| GAS 4本 | deployed source hash、trigger最終実行、処理件数、queue age、quarantine | trigger欠落、hash drift、最古滞留、連続失敗 |
| ocr_watcher | version、最終scan、spool、retry、host disk | heartbeat途絶、spool増加、credential error |
| kintone | App schema hash、read／write smoke、revision conflict | schema drift、permission変化 |
| Drive | root／folder access、共有権限、expected parent不一致 | public link、権限拡大、移動不整合 |
| Railway API | liveness、readiness、release SHA、inbound lag | dependency NG、旧SHA、受理済み未処理 |
| 外部uptime monitor | Railway外の有料providerからliveness／readiness／Access edgeを複数regionでsynthetic check | Railway全体停止、DNS／TLS／Cloudflare／origin経路異常 |
| worker／scheduler | poll、lease、job lag、leader lease | duplicate leader、expired lease、poll停止 |
| LINE通知 | push outcome＋独立dead-man | token混線、通知経路自体の停止 |

Railway自身のhealth／logだけを監視正本にしない。外部有料uptime監視は顧客dataや本番sessionを持たず、public liveness、dependencyを反映するreadiness、保護routeが期待どおりAccess challenge／拒否になることだけを確認する。LINE障害や誤tokenでも警報を受け取れる独立した第二経路を用意する。monitoring credentialと実行credentialを分ける。

## 13.10 backup setとrestore順序【FIXED範囲】

backup setには次を含める。

- app-state DB、AuditEvent、OperationJournal、ArtifactVersion、ApprovalSnapshot、Outbox／Delivery
- kintone App 21〜38の重要dataとschema snapshot
- Drive原本、file ID、sha256、parent、permission、segment manifest
- GAS source hash、deployment ID、trigger、timezone、property key schema
- watcher version、config schema、spool manifest
- Railway release SHA、migration、feature flag schema
- account owner、MFA、break-glass、rotation手順

restoreは、identity／config → app-state → kintone／Drive参照整合 → GAS／watcher → read only smoke → reconciliation → connector単位のworker解除、の順に行う。復旧直後に外部workerを自動再開せず、UNKNOWN、vendor receipt、queue leaseを照合する。件数だけでなくhash、revision、dangling ref、permissionを検査する。

---

# 14. 環境・release・migration

## 14.1 environment

| 項目 | local | staging | production |
|---|---|---|---|
| data | synthetic fixture | synthetic／許可済みtest | 実data |
| Railway | local process | productionと分離した第2環境・専用DB／secret | production環境 |
| kintone／Drive | mock | test用space＋schema clone App／専用test folder | production |
| LINE | mock | 顧客Bot／業務Bot／Q系のtest accountまたは正式test channel | production Bot |
| 外部送信 | mock | 正式test契約＋許可済みtest宛先 | feature flag OFFから |
| domain／RP ID | local規則 | staging固有 | 固定本番 |
| DB | disposable | staging専用 | production専用 |
| network前段 | local | Cloudflare有料planのstaging policy | Cloudflare有料planのproduction policy |

production dataをstagingへcopyしない。必要時は大野裁定、最小化、匿名化、期限、削除確認を要する。

STAGING_READY_CORE条件:

- Railway第2環境がproductionと別DB、別secret、別domain、別connector credentialを持つ。
- kintone test用space／AppとDrive test folderのschema・permissionが固定され、production tokenを使わない。
- test LINE accountのchannel roleが明示され、production recipientへ送れない。
- synthetic／匿名fixture、reset手順、seed／cleanup、staging release SHA表示、監視がある。
- production data、production LINE user、production external recipientをallowlistへ混ぜない。

STAGING_READY_CONNECTOR_X条件:

- connectorごとに正式test契約、sandbox／test account、許可宛先、別credential、rate／cost capを持つ。
- vendor webhook／poll、receipt、timeout、replay、UNKNOWNをtest環境で再現できる。
- 未契約connectorは当該G6-XだけBLOCKED_EXTERNALとし、STAGING_READY_CORE、G1〜G4、Release A〜Cをblockしない。

認証cutoverとservice HMACはSTAGING_READY_COREで、vendor webhookと外部送信は該当STAGING_READY_CONNECTOR_Xで、同一release candidateを一度全通し、rollback／replay／timeout／signature failureを確認してからproductionへ進む。資金不足を理由にstagingを省略しない。【FIXED】

## 14.2 configuration

- config schemaを起動時検証
- test credentialとproduction宛先の混在を拒否
- secretをsource、chat、docs、screenshot、logへ置かない
- credentialをservice・環境・権限別に分離
- Q系read-onlyとwrite tokenを物理分離
- envがPID 1へ反映されたことを実機確認
- dependency lockfileを維持し、package追加・major updateは別taskでreview
- production buildで既知脆弱性・license・supply-chainの最低限の検査

## 14.3 feature flag

最低限の候補:

- PWA read views
- sort write
- person decision
- CloudSign execution
- FAX execution
- e内容証明prepare
- Drive新routing

外部効果flagはproduction deploy時OFF。大野が1connectorずつONにする。flag変更もaudit対象。

## 14.4 migration

- schema、backfill、traffic切替を分離
- backfillは件数、成功、失敗、再開位置をjournal化
- kintone／Driveはdry-run→pilot→照合→rollback
- 新旧ID mappingを保持
- migration中の新規dataの扱いを先に決める
- dual-writeを恒久化しない

## 14.5 release gate

- base／target SHA
- migration version
- 対象REQ・正本・ADR
- test実出力
- transitive lock／hash、reproducible install、SBOM、vulnerability／license scan、runtime version
- CI run ID／resultとdeploy artifactの対応
- GAS deployed source hash、trigger、timezone、property key schema
- Codex reviewとFable裁定
- scopeに応じたSTAGING_READY_CORE／CONNECTOR_X evidenceと、同一release candidateのstaging全通・rollback実機
- feature flag初期値
- rollback・kill switch
- 大野のpush／本番承認

---

# 15. Test strategy

## 15.1 test layer

| layer | 対象 |
|---|---|
| Unit | hash、role、state transition、normalization |
| Property／model | idempotency、任意操作列の不変条件 |
| Contract | kintone／Drive／vendor schema |
| Integration | FastAPI＋test DB＋mock adapter |
| E2E | PWA＋staging API |
| Security | auth、IDOR、CSRF、XSS、Webhook、cache |
| Visual／device | iPhone、PC、長文、PDF |
| Recovery | backup／restore、worker crash、rollback |

## 15.2 必須不変条件test

1. machine actorからE2のHUMAN_CONFIRMED／APPROVEDへ到達しない。E1のAUTO_ROUTED_UNREVIEWEDは別state
2. STAFF／VIEWERは対外承認できない
3. step-upなしでapproveできない
4. challenge replay・期限切れ・別approval転用を拒否
5. 宛先・本文・添付の1byte変更で旧承認を拒否
6. crashしても未承認送信・承認消失が起きない
7. 二workerでもlogical executionは一つ
8. UNKNOWNを自動再送しない
9. 偽造・重複・古いWebhookを拒否／冪等処理
10. 他case IDへaccessできない
11. stale version writeを拒否
12. Service WorkerへAPI・PDF・thumbnail・Q&Aをcacheしない
13. log／errorへsecret・PII markerを残さない
14. origin直アクセスで保護routeへ到達できない
15. audit write失敗時、高リスク操作はfail closed
16. APPROVED遷移とOutboxJob作成が同一transactionで、片方だけ残らない
17. workerはApprovalSnapshotだけを読み、承認後のkintone／Drive変更を送信内容へ混ぜない
18. WorkTaskの同時claim、lease失効、再割当、停止user回収が正しい
19. 誤仕分け／誤名寄せ訂正で旧decisionが残り、依存artifact／approvalが失効する
20. Q&A source revision更新でSTALE、case access失効で本文・出典が非表示になる
21. UPDATE_AVAILABLEでDIRTY編集を強制reloadせず、CLIENT_INCOMPATIBLEのwriteをserverが拒否
22. completed／cancelled／superseded deep linkから再実行できない
23. vendor callbackは実機が提供する真正性検証とstatus再照合なしに完了確定しない
24. STAFF／VIEWERは招待・role変更・suspend・credential／session revokeを実行できない
25. user suspend／revokeとactive session失効・未完了task回収が部分成功で放置されず、journalから照合できる
26. ApprovalRequest取消とworker lease取得の競合で、LEASED／EXECUTING後のjobをCANCELLEDにしない
27. generation job失敗時に使用可能なArtifactVersionが成立せず、成功時はinput／source／template／bytes hashを固定する
28. 招待tokenの平文がDB／logへ残らず、二重使用・期限切れ・取消後使用・予定外role／identityを拒否する
29. 招待作成／取消はownerの対象結合step-upなしに実行できない

## 15.3 UX受入

### journey

- LINE→task
- 仕分け20件
- 案件current state
- Q&A→source
- approval→send→status
- session revoke
- 統合pilot→rollback。DriveはADRがPILOTを選んだ場合だけ追加

### state coverage

各主要画面でnormal、empty、loading、error、partial、stale、conflict、offline、forbidden、unknown、dirty／save failed、session／step-up expired、deleted／merged／obsolete、update available／client incompatibleを第6章の適用matrixに従って確認する。

## 15.4 実機

- iPhone PWA standalone
- Safari
- PC主要browser
- back／reload
- network遅延・切断
- 二重tap
- long document
- 同姓同名
- page数の多いPDF
- session期限途中

## 15.5 external

- local／CIはnetwork禁止・mock
- stagingは許可済みtest宛先
- production smokeは人承認、1件、監視
- UNKNOWNで以後停止
- Codexは送信testを実行しない

## 15.6 Constitutional Regression Suite【FIXED】

test総数ではなく、7月に事故を防いだ次の不変条件を独立suiteとしてG0、関連Gate、G8で実行する。

1. R4-3の弁護士承認済み47caseを変更・skipせず全PASS
2. App 30の禁止承認遷移をserver codeが作らない
3. machineが名寄せ確定、人物確認済み、法務確定を書かない
4. 顧客chat_responderと業務系をimport／credentialで混線させない
5. business notificationは明示channelのみで、fallbackしない
6. D1の混在PDF、6page以上、原本無変更、fragment page範囲
7. safe-side routeでcandidate外case IDを棄却
8. 名寄せApplication Service、通常runtime credential、PWA、LINEからApp 34物理削除へ到達できず、MergeDecision＋PersonAlias＋inactive化だけで統合する
9. kintone単票／一括APIのWebhook差をcharacterization testで固定
10. Q系connectorは業務正本へwriteできない
11. App 29／30／38とPWA projectionの二重完了・state driftがない
12. E1 auto routingをE2人確認済みとして表示・利用しない

既存testを削除、skip、assertion弱化してPASS数を合わせない。仕様変更が必要なら、大野裁定、ADR、旧testの意図、代替control、回帰証拠を一組で残す。

## 15.7 cross-system failure injection

各stepで成功前、外部write直後、ACK受領前後、local commit前後、process crash、timeout、duplicate、順序逆転を注入し、lost effectとduplicate effectが0であることを確認する。

対象:

- inbound webhook → InboundEvent
- sortation → App 38 → GAS → Drive
- D1 → 各ingest → App 33／25／35
- R4-2 merge／unmerge → App 34
- R4-3 → DerivationRun → App 36
- ArtifactVersion → ApprovalSnapshot
- approval → OutboxJob → vendor → webhook／poll

PENDING_RECONCILIATIONとUNKNOWNを意図的に作り、runbookどおりに証拠付きで解決する。mockだけでなくkintone query仕様、token権限、GAS trigger、vendor test accountを使う実機testをGate別に一件以上持つ。

---

# 16. Figma・製品検証

Figmaは初回の高risk journeyを実装前に検証する道具であり、運用後にcodeと常時同期する第二正本ではない。大野が承認したflowをscreen仕様へ反映した後は、minor UI変更のたびにFigmaを更新しない。Figmaとapproved文書が食い違う場合は文書正本を優先する。

## 16.1 実装前に作るprototype

interactive prototypeを必須にするのは次の6本だけとする。

1. LINE通知→「今日」→仕分けqueue→1件人確認
2. 案件検索→dashboard→原本
3. Q&A→出典→原本
4. 成果物生成→validation block→preview→新version→申請
5. 承認一覧→再認証→結果不明／照合
6. 誤仕分け／誤名寄せ→影響確認→訂正→依存失効

session失効、権限不足、task claim、update available等はannotated wireframe／state tableで検証できる。大野が迷う、誤操作riskが高い、または新しいinteraction patternを導入する場合だけinteractive prototypeへ格上げする。

## 16.2 必須state coverage

normal、empty、loading、error、stale、conflict、offline、low confidence、source outdated、unknown resultをscreen state matrixで網羅する。Figma frameを全組合せ分作る義務はなく、interactionまたはlayoutが変わる代表frameだけを作る。

## 16.3 検証台本

大野がprototypeを操作し、次を記録する。

- 完了時間
- tap数
- 迷った場所
- 誤って押しそうな場所
- 欲しい情報が不足した場所
- 情報が多すぎる場所
- 確認摩擦が必要／不要
- iPhoneとPCの使い分け

素材:

- 通常scan
- 候補0
- 候補複数
- 同姓同名
- 低confidence
- 長い住所・文面
- timeout・結果不明

## 16.4 Gate

コード実装前に、第16.1の6 journeyをprototypeで完走し、work-logのUX節へPASS／CHANGE／BLOCKED、所要時間、tap数、未解決を記録する。独立したUX_ACCEPTANCE_MATRIX fileは作らない。blockerとなる迷い、戻れない導線、対象不明の承認を0にする。

prototypeへ顧客実dataを不要に複製しない。必要な場合は匿名化する。

## 16.5 製品analytics

操作改善用analyticsと法的auditを分離する。

analytics候補:

- screen到達
- task完了時間
- tap数
- filter利用
- error class
- abandon

氏名、本文、質問全文、PDFをanalyticsへ送らない。少人数利用でも、改善目的・保存期間・閲覧者を明示する。

---

# 17. 基準210時間の既存資産統合roadmap

210時間はFable司令塔＋PC-A単線のactive engineering／design／test／裁定時間である。Codexの独立review実行時間、大野／CUのactive acceptance時間、外部待ちelapsed timeは別ledgerで計測する。ただしCodex所見の裁定、PC-A修正、再検査は210時間へ含む。素材・外部審査待ちは工数へ含めず、該当項目をBLOCKED_EXTERNALとする。

G0前のbase planは210時間だが、Phase 1が30〜40時間へ膨らむ220〜230時間をrisk-adjustedな既定想定とする。190〜210時間は、legacy hardeningの相当部分が現在main／productionで既に解消済みとG0で実証された場合だけである。G8.5 remediationは別枠8〜20時間、全3connectorをproduction品質にする場合はさらに20〜40時間のrisk reserveを別裁定する。

日次では PC-A／Fable active、Codex review、Ono／CU acceptance、external wait の四区分を記録し、待ち時間を実装工数へ混ぜない。

| Phase | 工数 | 目的 | Gate |
|---|---:|---|---|
| 0. 現況固定・containment | 10h | main、実機、P0〜P16、endpoint、secret／PII、baselineを固定 | G0 |
| 1. 統合設計・既存基盤hardening | base 20h／G0後想定30〜40h | queue、lineage、legacy入口、GAS、merge、App 36を補強 | G1 |
| 2. UX・Figma prototype | 18h | 実装前に主要journey・例外状態を潰す | G2 |
| 3. PWA control plane | 34h | durable DB、auth、role、audit、journal、task overlay、前段 | G3 |
| 4. PWA中核業務 | 30h | 今日、dashboard、仕分け、Q&A、状態画面 | G4 |
| 5. 成果物生成 | 28h | DerivationRun、App 37、template版を項目1〜6へ結線 | G5 |
| 6. 承認・対外送信 | 34h | snapshot、outbox、worker、UNKNOWN、項目7〜9 | G6 |
| 7. 有機的統合pilot | 8h | 新規1件・既存1〜2件、任意のDrive pilot、rollback | G7 |
| 8. 安定化・訂正・復旧 | 20h | E2E、soft merge訂正、monitor、restore、incident | G8 |
| 9. 段階release・引継ぎ | 8h | Release A〜D、運用訓練、証拠index、9月brief | G9 |
| **合計** | **base 210h／risk-adjusted 220〜230h** |  |  |
| G8.5 remediation reserve | 別枠8〜20h | pentest支援、修正、G8再実行、R8-delta、retest | G8.5-CORE／D |
| **Release D productionまで** | **最大228〜250h想定** | 業者実施・待ち時間は別 |  |

各Phaseに列挙する大文字名の「成果物」は、原則として独立file名ではなく、一つのPhase work-log／evidence bundle内のsection名である。独立fileにするのはAPPROVED ADR、弁護士正本、再利用template、長期runbookだけ。schema、SBOM、OpenAPI、test一覧、diff、traceabilityは自動生成物へのlinkで足りる。

## 17.0 元13項目との対応

| 元項目 | 主Phase |
|---|---|
| 1 職務上請求書 | Phase 5 |
| 2 相続放棄 | Phase 5 |
| 3 遺産分割協議書 | Phase 5 |
| 4 財産目録4種 | Phase 5 |
| 5 委任契約書 | Phase 5 |
| 6 請求書・領収書 | Phase 5 |
| 7 CloudSign | Phase 6 |
| 8 e内容証明 | Phase 6 |
| 9 FAX | Phase 6 |
| 10 Q系Bot | Phase 4 |
| 11 Drive二階層 | Phase 7でADR。KEEP_CURRENTを正規合格とし、必要時だけpilot |
| 12 PWA | Phase 1〜4・6 |
| 13 ラベル実機 | Phase 8 |

7月残taskはPhase 0で「各Phaseの前提」「独立小粒」「正当放置」に再分類する。前提だけを依存Phaseの前に回収し、小粒を中核Phaseへ混ぜない。

## 17.0.1 P0〜P16準備Gate

| ID | 必要な実物／状態 | 用途 | 未充足時 |
|---|---|---|---|
| P0 | 現在main SHA、git status、最新work-log、baseline test | Phase 0でPC-Aが収集 | 未収集でもPhase 0開始可、G0通過不可 |
| P1 | 職務上請求書の現行template実物 | 項目1 | 該当帳票BLOCKED |
| P2 | 類型別委任契約書の現行template | 項目5 | 該当帳票BLOCKED |
| P3 | 請求書・領収書の現行書式と採番rule | 項目6 | 該当帳票BLOCKED |
| P4 | 財産目録の現行Word実物1枚以上 | 項目3・4 | 該当帳票BLOCKED |
| P5 | CloudSign契約planのAPI利用可否・公式仕様 | 項目7 | PREPARE_ONLYへ格下げ |
| P6 | 業務類型×顧客の分類表 | Drive ADR／任意pilot | 未充足なら物理再編だけDEFERRED |
| P7 | 熊澤案件等の許可済み実機素材 | 項目1・2検収 | syntheticで作れても実案件検収BLOCKED |
| P8 | LINE仕分けの不満点メモ＋scan20件 | UX・仕分け | G2／G4 BLOCKED |
| P9 | InterFAX正式test契約、API資料、sandbox／test account、許可済みtest宛先 | 項目9 | connectorをBLOCKED_EXTERNAL |
| P10 | e内容証明公式雛形ZIP・現行実務書面 | 項目8 | 実文面生成・実機BLOCKED |
| P11A | Railway staging第2環境、staging DB／domain／secret、kintone test space、Drive test folder、test LINE、synthetic fixture | Core staging infrastructure | G1 BLOCKED |
| P11B | production固定domain／RP ID、HTTPS、Cloudflare有料plan、iPhone、security key 2本、予備端末 | PWA auth・recovery実機 | G3 BLOCKED |
| P12 | CloudSign／FAX／e内容証明の正式test契約、sandbox／test account、許可済みtest宛先 | 対外検収 | G6 BLOCKED_EXTERNAL |
| P13 | kintone関連Appの最新schema export、status、token権限表 | Phase 0で人／PC-Aが収集 | 未収集でもPhase 0開始可、G0通過不可 |
| P14 | GAS 4本、watcher、Railway deploy／configのfingerprint | Phase 0で人／PC-Aが収集 | 未収集でもPhase 0開始可、G0通過不可 |
| P15 | 匿名化した代表PDF／戸籍／財産／期待結果corpus | regression、AI calibration、UX | G1／G2／G4 BLOCKED |
| P16 | Q&A、audit、thumbnail、外部証跡、logのretention方針 | privacy、DB容量、削除 | G1でclass／legal holdを仮固定、G3前に期間確定 |

READY／TO_COLLECT／IN_PROGRESS／BLOCKED／NOT_APPLICABLE／STALEをPhase 0で実物により判定する。「あるはず」をREADYとしない。P0／P13／P14はTO_COLLECTから始まり、Phase 0の調査task自体をblockしない。人から受領すべきP1〜P12／P15／P16と、Phase 0で採取する証拠を区別する。

### 7月残taskの依存判定

| 残task | 関連Phase | 扱い |
|---|---|---|
| R4-3b 導出結果→関所→App 36 | Phase 5項目2 | 前提。Phase 5前に完了 |
| R4-3c App 34／36 fieldと本番裁定 | Phase 4・5 | 前提。人／CU結果を実物確認 |
| No.20／21親people edge | Phase 4・5 | 半血判定等に使う前に回収 |
| /scan堅牢化・氏名待ちfall-through | Phase 1 safe-ingest migrationへSUPERSEDED | legacy /scanを延命せずcaller移行後disable。Phase 4は新ingest回帰だけ |
| 現行戸籍在籍者の生存自動設定 | Phase 5項目2 | 判定入力に使う前に完了 |
| S6実機・通帳2件 | Phase 5項目4 | 実財産data検収の前提かをPhase 0で裁定 |
| log・文言・表示の小粒 | Phase 8 | 依存性なしなら安定化期のみ |
| R5・Z3 | 9月以降 | DEFERRED。器以上を作らない |

## 17.0.2 項目1〜6の既存資産との結線

| 項目 | 入力・判定正本 | 生成・配置の境界 | 停止条件 |
|---|---|---|---|
| 1 職務上請求 | koseki_chain／F5→App 36等 | 起票後は既存M1の宛先引当て・小為替・様式生成を再利用 | 不足戸籍・自治体・請求理由がunknown |
| 2 相続放棄 | H系列正本＋順位engine＋続柄別戸籍matrix | R4-3b／cとApp 36結線後に申述書・代理人目録・必要戸籍請求までdraft | 期限・続柄・法務正本が不一致 |
| 3 遺産分割協議書 | App 34×App 35×App 37の人入力＋条項library | App 37 schema／validationを固定し、特殊条項は作らず8割draft | 人入力なし・未確定相続人・割付不整合・特殊条項 |
| 4 財産目録 | App 35正規化data | S3を再利用し、協議書／家裁／税理士／顧客の4写像 | P4なし・用途mapping不明 |
| 5 委任契約 | 業務類型分岐表＋P2 | UNIT_CONFIG等の既存template切替を再利用しdraft | 類型unknownは推測選択しない |
| 6 請求書・領収書 | P3＋報酬・入金正本＋Stripe event | 採番、event ID、金額、税、入金状態を照合し、顧客folderの下書きへ配置 | event replay・金額不一致・採番衝突 |

## 17.1 Phase 0: 現況固定・immediate containment（10h）

### Phase 0A read-only evidence

PC-Aがsource、test、schema、production configの読み取り証拠を取得し、workspace変更0で報告する。Fableが検収し、Codex R0-Aが現在地とrisk分類をreviewする。

### Phase 0B owner承認済みcontainment

R0-A後、大野が明示承認したkill switch、route遮断、credential rotation、feature flag、log停止等だけを一指示ずつ実施する。差分をCodex R0-BがreviewしてからG0判定する。

### 成果物

- CURRENT_STATE_REPORT
- P0_P16_GATE_TABLE
- BASELINE_TEST_REPORT
- DESIGN_DOC_INDEX
- 本書の論理名とrepo実名のmapping
- ENDPOINT_TRUST_BOUNDARY_INVENTORY
- KINTONE_SCHEMA_AND_TOKEN_MATRIX
- GAS_WATCHER_RAILWAY_FINGERPRINT
- SECRET_PII_EXPOSURE_REPORT
- LEGACY_MIGRATION_LEDGER
- 210時間再見積り

### G0

- main SHA、dirty、production deploy SHA、全test実出力が固定
- 0.5のRV-01〜RV-14を再検査し、各項目がOPEN／CONTAINED／FIXED／STALE
- public endpoint、webhook署名、query token caller、PII logの影響範囲を実物固定
- kintone App 21〜38 schema、token権限、GAS deploy hash／trigger、watcher version／heartbeatを取得
- /scan、/ocr/fixed-asset、channel token fallback、Stripe／CloudSignの危険経路が停止または隔離
- P1〜P4の有無
- 警報・既知障害・secret／PII露出なし、またはrotation・containment済み
- 大野が150万円相当を目安とするConsole暴走検知上限、auto reload、残高／異常消費警報、Codex課金方式、上限到達runbookを確認
- 文書矛盾一覧
- 最初のtaskを一つだけ発行可能
- Codex R0-A／R0-B完了、Fable裁定記録済み、未containmentのBLOCKER／HIGH 0
- CONTAINED項目はproduction遮断証拠、rotation、manual fallback、owner、Phase 1 migration ID、expires_atを持つ
- OPENのBLOCKER／HIGHはG0不合格

## 17.2 Phase 1: 統合設計・既存基盤hardening（base 20h／G0後想定30〜40h）

Phase 1はminimal durable ingress／conversationの唯一の実装ownerである。既存durable DBがなければ、最小app-state DB、migration、InboundEvent、IngestionReceipt、ConversationSession、PendingCommandだけをbootstrapする。auth、passkey、AuditEvent、TaskBinding、Approval、Outbox、汎用OperationJournalへの拡張はPhase 3／6がownerであり、暫定memory queueや別kintone queueを作らない。

このPhaseは計画上最大の工数riskである。HMAC本番cutover、全caller移行、旧token／route retirement、durable ingress、soft merge＋過去復元tool、CloudSign webhook、GAS per-file isolation、初回dependency lock／SBOMを同時に含むため、G0で未解消と判明した項目が多ければ30〜40時間を通常予測とする。20時間へ収めるためにcutover、retirement evidence、failure test、復元性を省略しない。40時間超予測ならPhase 1着手前に全体を再裁定する。

### 成果物

- PRODUCT_BRIEF
- USER_ROLE_PERMISSION_MATRIX
- DOMAIN_WORKFLOW_MAP
- SOURCE_OF_TRUTH_MATRIX
- NON_FUNCTIONAL_REQUIREMENTS
- ERROR_TAXONOMY
- REQUIREMENT_TRACEABILITY_MATRIX初版
- APP_SOURCE_OF_TRUTH_AND_WRITE_OWNER_MATRIX
- TASK_BINDING_AND_STATE_MAPPING
- DOCUMENT_LINEAGE_AND_INVALIDATION_MATRIX
- EFFECT_LEVEL_AND_COMMAND_ARBITRATION
- PRESERVE_HARDEN_REPLACE_DEFER_LEDGER
- query token→HMAC header署名migration
- InboundEvent／IngestionReceipt、ConversationSession／PendingCommandとper-file retry／quarantine
- /scan／fixed-asset caller移行、GAS毒饅頭解消、heartbeat
- notification fail-closed、PII redaction
- R4-2 soft merge／unmerge migration設計
- App 36 DerivationRun、App 37割付、template registry設計
- case_hintへCaseBindingDecisionを結び、AUTO_BOUND_UNREVIEWEDはR3で停止、HUMAN_CONFIRMEDだけR4-1人物化
- ADR確定

### G1

- 大野が「毎日最初に見るもの」「事務員へ任せる範囲」「弁護士留保」を承認
- PWAが二重台帳になっていない
- App 29／30／38とWorkTask、App 30とApprovalRequestの責任分界がworkflow別に固定
- LINE／PWA／GAS共通use case contractとcharacterization testを固定。PWA実動結線はG4
- STAGING_READY_COREでHMAC dual-accept→new-only、GAS／watcher caller切替を全通・rollbackしてからproduction cutover
- CloudSignはSTAGING_READY_CONNECTOR_CLOUDSIGNなら新webhookを全通し、未契約なら旧危険routeをDISABLED／CONTAINEDとしてG6-CLOUDSIGNをBLOCKED_EXTERNALへ送る
- productionの全GAS／watcher callerが新認証へ移行し、旧query token requestを拒否、旧token rotation、legacy traffic 0、retirement evidenceを保存
- HMAC replay、別path転用、body改変、期限外、unknown key IDを拒否し、nonce一回使用を実証
- /scanと/ocr/fixed-assetは全callerをsafe ingestへ移して無効化。fixed-asset相当の更新は地番等の完全識別条件を使い、0件／複数件でwrite 0
- 類似所在record 2件で先頭誤更新0、MIME／magic／size／page／schema／candidate allowlist違反でwrite 0
- process-memoryのcritical queue／idempotencyを廃止し、durable commit前に成功ACKを返さない
- 顧客Bot／業務Botの会話・hearing・pending command・record bindingをdurable化し、restart／event replay／期限切れ／複数user混線testをPASS
- CloudSign旧webhook routeはtraffic 0、旧secret失効、retirement evidence。新routeは真正性検証失敗でwrite 0
- 過去PII logの保存先、期間、閲覧者、削除可否、incident該当性を大野が裁定
- direct／transitive dependencyとruntimeをversion／hash固定し、reproducible installと初回SBOM／scanを実証
- 30fileの先頭・中間・末尾へ毒fileを入れても正常file欠落0、quarantine／再開可能
- R4-2 soft merge／unmergeを実装し、旧削除route無効、通常tokenのApp 34削除権限除去、過去削除recordの復元toolを実証
- App 36をDerivationRun projection、App 37を分割入力正本候補として実schemaで裁定
- 誤auto bindingが別caseのApp 34人物を作らず、R4-2／R4-3／artifactへ到達しない
- IN／OUTが明確
- NFRに測定方法がある
- OPENがowner・期限付き
- Codex R1完了、Fable裁定記録済み、未解決BLOCKER／HIGH 0

## 17.3 Phase 2: UX・prototype（18h）

### 成果物

- INFORMATION_ARCHITECTURE
- NAVIGATION_AND_DEEP_LINK_SPEC
- SCREEN_STATE_SPEC
- Figma操作prototype 6 journey
- work-log内UX acceptance節
- 20件のUX_TEST_RECORD
- design token／component inventory

### G2

- 大野が第16.1の6 journeyを完走し、PASS／CHANGE／BLOCKEDをwork-logへ記録
- 対象不明の承認、戻れない導線、誤case処理のblocker 0
- iPhone／PC差を明文化
- 例外状態を定義
- PC-Aが画面仕様を推測する必要なし
- Codex R2完了、Fable裁定記録済み、未解決BLOCKER／HIGH 0

## 17.4 Phase 3: PWA control plane（34h）

### 実装

- passkey、session、step-up、revoke
- 招待、role変更、suspend／revoke、credential／session失効、task回収のowner管理境界
- server role／将来case access境界
- Access前段とorigin迂回防止
- security headers、CSRF、rate limit、correlation ID
- audit
- durable DB migration・backup／restore
- InboundEvent、OperationJournal、TaskBinding、projection rebuild
- Phase 1のConversationSession／PendingCommandをPWA identity、audit、monitorへ結線
- optimistic concurrency
- manifest、Service Worker
- responsive shell、共通state component
- environments、feature flag、rollback
- structured log、health、monitor hook
- ScheduledRun（schedule key、intended_run_at、unique、state、lease、attempt、completed_at、catch-up policy）

### G3

- STAGING_READY_CORE環境でpasskey登録／login／step-up／revoke／origin遮断を全通してからproduction RPへ進む
- iPhone staging login
- hardware security key 2本＋予備端末を登録し、片方のbackup credentialだけでrecovery→primary revoke→再登録をstaging実証
- server role test
- step-upが対象操作に結合
- session／credential失効
- owner以外のuser管理拒否、user suspendとtask回収の全通
- origin迂回不可
- browser cacheに顧客dataなし
- stale write拒否
- auditに必要field、本文・tokenなし
- feature flag OFFで既存LINE無変更
- durable commit前にwebhookへ成功ACKを返さない。event store停止時はvendor retry可能responseまたは独立durable ingressを使い、process-memory fallbackは禁止
- app-state DB停止時、既存の安全なread／replyは可能な範囲で継続してよいが、journal必須のE1〜E3 writeは503／手動fallback。誤成功・破損を起こさない
- App 29／30／38 projectionを破棄・再構築して件数・state一致
- InboundEvent受理後のprocess crashでlost event 0
- hearing途中crash後のcase混線0、LINE event replayで遷移一回、期限切れPendingCommandのwrite 0、複数user／channel state混線0
- scheduled時刻を跨ぐ停止後に一回だけcatch-upし、二instanceでも同一run一件。liveness／readinessを分離
- backup→restoreでidentity、journal、task bindingの参照整合PASS
- Codex R3完了、Fable裁定記録済み、未解決BLOCKER／HIGH 0

## 17.5 Phase 4: PWA中核・既存adapter統合（30h）

### 実装

- 今日／work queue
- case list／dashboard
- people／assets／documents／timeline
- 仕分け
- 名寄せ確認
- Q&A／source
- 新規書類のOCR全文保存。過去分は件数・費用・再開性を測るbackfill設計＋小規模pilot（PDF全文検索UIは作らない）
- 結果不明・failure
- LINE deep link
- ExternalIdentityLink
- ownerによるLINE identity bootstrap、既存pending E2 taskのactor migration／失効
- Q専用LINE account、read-only connector、QA専用append writer
- App 29／30／38の集約readとsource deep link
- 既存LINE／PWA共通Application Service

### G4

- 実data readのsource／revision追跡
- 仕分け実機20件
- 通常3tap目標を測定
- E2 machine自動確定0。E1 auto routingはAUTO_ROUTED_UNREVIEWEDとして別表示・別計測
- App 29／30／38とPWA task count・state一致、二重表示0
- LINEとPWAの同時操作は一件だけ成立し、後着409／CONFLICT
- 未結合／revoke済みLINE identityはE2 write 0。Q専用accountから業務正本write 0
- duplicate投入とprocess restart後もIngestionReceipt、ask task、forwardは各一件
- ask task保存失敗を成功にせず、forward失敗はPENDING_RETRY／PENDING_RECONCILIATIONで可視化
- downstream成功確認前にDrive処理済みとせず、retry後の重複App record／重複Drive移動0
- 誤auto bindingがR4-1人物化、R4-2、R4-3、App 36、artifact、approvalへ到達しないE2E
- stale、conflict、offline、low confidence
- iPhone／PC
- OCR backfill pilotの対象件数・欠落・重複・再開試験を照合し、全件実行のPhase／費用を裁定
- Codex R4通過後に実data pilot
- Fable裁定記録済み、未解決BLOCKER／HIGH 0

## 17.6 Phase 5: 成果物生成（28h）

対象:

1. 職務上請求書
2. 相続放棄
3. 財産目録4種
4. 遺産分割協議書
5. 委任契約書
6. 請求書・領収書

### 規律

- P1〜P4実物
- 既存engine・台帳を再実装しない
- 特殊条項を創作しない
- unknown類型は停止
- immutable version
- ArtifactVersion・source revision・template version・bytes hashをPhase 5で実装する
- R4-3一回ごとのDerivationRunを固定し、App 36をcurrent projectionとして結線する
- App 37を分割内容の正本とし、財産ID、取得者人物ID、割合、代償金、確定stateをvalidationする
- TemplateVersion registryでapproved正本、mapping、条項library、適用期間を固定する
- App 34／35／36／37の訂正でArtifactDependencyから旧成果物をSOURCE_OUTDATEDにする
- 現行Drive構造内の下書き配置先を固定し、物理再編を待たない
- draft-only
- golden／snapshot、改頁・旧字・長住所

### G5

- 実案件で軽微修正以内
- 元dataと帳票一致
- R4-3 run→App 36→必要戸籍plan／成果物まで一つのlineageで追跡可能
- App 37の未割付、重複割付、合計不整合、人未確定をblock
- template bytes、mapping、input snapshot、output bytesを旧版含め再現可能
- upstream訂正で旧版を消さずstale表示し、承認申請を拒否
- 既存M1／S3と同じ入力の同値比較を記録
- 項目6はStripe raw body署名検証、event ID unique、replay時二重発行0、App ID config化、case／顧客／金額／通貨／支払state照合をPASS
- Stripe→kintone writeをread-after-writeし、timeout／ACK不明はPENDING_RECONCILIATION。payload、氏名、住所の通常log出力0
- 上記Stripe条件未充足なら項目6だけBLOCKEDとし、推測で請求書／領収書をCOMPLETEにしない
- 自動送信なし
- 二重発行なし
- 大野の実物判定
- Codex R5完了、Fable裁定記録済み、未解決BLOCKER／HIGH 0

## 17.7 Phase 6: 承認・対外送信（34h）

### 実装

- approval center
- ApprovalSnapshot／ApprovalRequest（ArtifactVersionはPhase 5の実装を利用）
- canonical hash
- 対象結合StepUpChallenge
- OutboxJob／IntegrationAttempt／ExternalDelivery
- durable worker process、startup lease回収、継続poll、concurrency上限、graceful shutdown
- connector別kill switch、worker health、queue-lag／expired-lease alert
- CloudSign
- FAX
- e内容証明
- webhook、idempotency、UNKNOWN
- public webhook InboundEvent、signature／endpoint secret、event replay防止
- App 30 legacy承認と新ApprovalRequestのworkflow別migration state
- connector account／credential／許可宛先の環境分離
- vendor receipt polling／管理画面照合

connectorを同時に開通しない。

1. mock contract
2. staging
3. Codex review
4. 許可宛先test
5. 大野承認
6. PILOT_CANDIDATEとしてflag OFFで固定
7. Phase 7のstaging／正式test宛先pilot 1件
8. 観察・reconciliation

### G6-CORE

- STAGING_READY_CORE＋deterministic mock connectorでApprovalSnapshot→Outbox→worker→成功／timeout／UNKNOWN／reconciliationを全通し、実vendor契約なしでもcoreを検証可能
- 未承認send不可
- 1byte変更で承認失効
- role＋step-up
- duplicate／timeout／UNKNOWN
- webhook真正性検証失敗ではstateを進めず、callbackとvendor status不一致をRECONCILINGへ
- approvalとOutbox作成、worker lease、vendor call、callbackの各境界でfailure injection PASS
- connectorごとにkill switch、allowlisted recipient、rate／cost cap
- credential inventoryでpublic ingress／Internal PWAにvendor execute secret 0、workerにpublic listener／PWA cookie secret 0を実出力確認
- worker credentialは承認snapshot read＋対象connector writeだけ
- deploy／worker crash後のdurable再開と、call開始後lease失効のUNKNOWN化
- e内容証明最終送信は人
- Codex R6後にmock allowlist／誤宛先拒否test。実vendorの許可宛先testは各G6-Xで実施
- Fable裁定記録済み、未解決BLOCKER／HIGH 0

### connector sub-gate

CloudSign、FAX、e内容証明を別々のG6-Xで判定する。各G6-Xは該当STAGING_READY_CONNECTOR_Xでapproval→Outbox→worker→test vendor→webhook／poll→reconciliationを全通し、production credential／recipient混入0を確認する。MOCK_READY／STAGING_VERIFIED／TEST_RECIPIENT_VERIFIED／PILOT_CANDIDATE／PILOT_VERIFIED／BLOCKED_EXTERNAL／PREPARE_ONLY／DEFERREDを記録する。G6-COREは全connectorがBLOCKED_EXTERNALでも通過でき、Release A〜Cをblockしない。Release Dの該当connectorだけがそのsub-gateを要求する。

210時間の範囲は共通framework＋利用可能なconnector 1本のstaging／正式test宛先pilotを基準とする。production pilotはG8.5-D後のRelease Dで行う。残るconnectorのproduction品質化はRelease D-2／D-3とし、必要なら20〜40時間のrisk reserveを別裁定する。

e内容証明の自動化境界は「公式雛形に基づくWord／CSV生成→人承認→CUによるupload／入力支援→最終確認・送信は大野」でFIXED。Web APIがない前提で設計し、日本郵便の公式雛形ZIPを必須とする。OPENはCU操作の安定範囲・保守方法だけである。【FIXED】

## 17.8 Phase 7: 有機的統合pilot（8h）

新規案件1件と既存案件1〜2件で、LINE／PWA → 仕分け → GAS routing → 読解 → 人物 → 名寄せ／確認 → DerivationRun → draft成果物 → 承認前を全通する。各stepのsource revision、operation_id、actor、artifact hashを一本のevidence indexにする。PILOT_CANDIDATEのconnectorがあればR7後にstaging／正式test宛先で1件を実行し、receiptまで追跡する。G8.5-D前にproduction connector送信を行わない。全connectorがBLOCKED_EXTERNALならG7-COREをblockしない。

Drive二階層再編はこのPhaseの必須成果ではない。PWAのcase／doc type／stateによるvirtual viewを使った後、大野がなお具体的不便を示した場合だけADRでPILOTを選ぶ。

Drive ADRの正規結果:

- KEEP_CURRENT: 現行「誰の／何の」構造を維持し、PWA virtual viewで解決。これでG7をPASSできる。
- PILOT: dry-run、file ID manifest、新規1件、既存1〜2件、rollbackを実施。
- DEFERRED: 判断材料不足。全体releaseをblockせず、物理再編だけ後送り。

### G7

- 新規・既存案件で、仕分けから成果物draftまで全lineageを追跡
- PWA feature flag OFFで既存LINE／data planeの回帰PASS
- 誤仕分けまたは誤名寄せを一件訂正し、依存失効・再生成・rollbackを実証
- 利用可能なconnectorがある場合はstaging／正式test宛先で未承認send 0、receiptまで照合。ない場合はmock／PREPARE_ONLY、kill switch、BLOCKED_EXTERNAL証拠
- DriveはKEEP_CURRENT＋現行回帰PASS、PILOT＋欠落／重複0＋rollback PASS、またはDEFERRED＋現行回帰PASS＋owner／判断期限
- 旧新の権限が意図なく変わっていない
- Codex R7をpilot前と人実機後の差分に実施し、Fable裁定・未解決BLOCKER／HIGH 0

## 17.9 Phase 8: 安定化・訂正・復旧（20h）

- 全E2E
- invariant security test
- performance実測
- alert
- backup／restore
- rollback
- incident drill
- session失効
- label実機
- 7月残taskのうち必要範囲
- Constitutional Regression Suite
- cross-system failure injection
- merge→訂正→unmerge
- GAS毒file、watcher停止、scheduler二重leader
- processor／log／cache privacy検査

### G8

- 主要journeyのE2E・全回帰がbaseline以上でPASSし、test削除・skip・緩和0
- auth、role、case境界、step-up、approval snapshot、outbox、UNKNOWN、cacheの不変条件がPASS
- backup→restore、release rollback、session／credential失効を隔離環境と実機で証明
- app-state DBは隔離環境へ実restore、kintoneはtest Appまたはexport→別App、Driveはtest folder、GASはtest deployment／triggerで復元し、dangling ref、hash、revision、permission不整合0
- productionはread-only backup evidenceとrollback rehearsalだけを行い、復旧試験で本番data／triggerを破壊しない
- GASの先頭／中間／末尾毒fileでも正常file欠落0、quarantineから再開
- MergeDecisionの訂正／unmergeとApp 36旧run再現を実証
- worker crash、scheduler重複、watcher停止、LINE警報停止を検知・復旧
- Railway／DNS／Cloudflare／originの各停止を外部有料uptime監視が検知し、LINE以外の第二経路へalert
- source・fixture・log・artifactにsecret・不要PII混入0
- transitive dependencyまでversion／hash固定、reproducible install、SBOM、vulnerability／license scan、runtime version、CI実行証拠をrelease recordへ保存
- HIGH／CRITICAL dependency例外はowner、理由、代替control、expires_atなしに許可しない
- GAS deployed source hashとrepo release manifest一致。trigger、timezone、property key schemaを記録
- monitor・alert・kill switch・incident・manual fallback runbookを受入
- Codex R8完了、Fable裁定記録済み、未解決BLOCKER／HIGH 0

## 17.9.1 G8.5: 専門業者penetration test

独立した専門業者による人間penetration testを二つのsub-gateに分ける。【FIXED】

- G8.5-CORE: 認証、session、WebAuthn／step-up、role／IDOR、PII、Cloudflare Access／WAF／rate limit／origin迂回。Release A〜Cのproduction有効化前に必須。
- G8.5-D: ApprovalSnapshot、Outbox、worker credential境界、webhook、vendor callback、外部送信、UNKNOWN。Release Dのproduction pilot前に必須。

一般的な業務機能全体へscopeを肥大させない。O-36で業者、NDA、各sub-gateのscope、test期間、連絡先、停止条件、再test条件を大野が確定する。

実施条件:

- G8.5-COREはSTAGING_READY_CORE、G8.5-Dは該当STAGING_READY_CONNECTOR_Xを主対象とし、synthetic data／test accountだけを使う。
- stagingとproductionのCloudflare rule、route topology、security header、credential class、release SHA、config fingerprintの差分表を作る。
- stagingで再現できないproduction perimeter差分だけ、大野の時間帯・IP／account・rate制限付き明示承認で限定testする。顧客dataの閲覧・破壊・実送信は禁止。
- test account、connector kill switch、backup、incident連絡、証拠保存を事前確認する。

各sub-gateの合格条件:

- penetration test報告書と対象release SHA／staging・production config fingerprintが対応
- BLOCKER／HIGHを全件修正し、影響範囲のG8 testを再実行、Codex R8-delta、業者retestを全てclosed
- 修正がcode、Cloudflare policy、route、worker config、credential scopeへ触れた場合、修正前のG8／R8 evidenceをSTALEにし、最終candidateで再生成
- MEDIUM以下はowner、影響、代替control、期限、該当release可否を記録
- credential／test dataを撤去し、想定外の外部効果・data残存0

G8.5-CORE未通過ではRelease A〜Cをproduction有効化しない。G8.5-D未通過ではRelease Dのproduction pilot／connector flagを有効化しない。

業者の実施・待ち時間は210／230h外で別計測する。staging seed／cleanup、test支援、修正、影響G8再実行、R8-delta、retest対応のPC-A／Fable active hoursとして8〜20hのG8.5 engineering reserveを別枠で置く。したがってRelease D productionまでの総active見込みは228〜250hとなり得る。230h到達時は17.11どおり新featureを止め、scopeを増やさず人の再見積りを経てreserveを開放する。

## 17.10 Phase 9: 段階release・本番受入（8h）

- final release SHA
- migration／flag／rollback記録
- iPhone／PC全通
- 実帳票
- 許可済み外部送信
- Drive ADR結果
- 未完分類
- runbook
- 9月brief

releaseは一括切替しない。

- G4後: Release A／B candidateをstaging固定
- G5後: Release C candidateをstaging固定
- G6-X後: Release D connector candidateをflag OFFで固定
- R7後のG7: 許可済みconnectorをstaging／正式test宛先でpilot
- R8／G8＋G8.5-CORE合格後: productionへA→B→Cをfeature flagで順次有効化
- G8.5-D合格後: Release Dで許可済みproduction宛先1件のcontrolled pilot→観察／reconciliation→connector flagを順次有効化

Release Aは「業務正本へのwriteなし」であり、QARecordへの専用appendは許す。

### G9

- 大野のiPhone PWA／Safari／PCでlogin→「今日」→仕分け→案件→成果物→承認の許可範囲を完走
- release SHA、config schema、migration、feature flag初期値、rollback point、人pushの証跡を一つのrelease recordに固定
- 実物template帳票、許可宛先の対外経路、Drive ADR結果を各完了基準で照合
- Release A〜CはG8.5-CORE、Release DはG8.5-Dについて、O-36、最終SHA／config fingerprint、BLOCKER／HIGH 0、G8再実行、R8-delta、業者retest closedを照合
- 未完をCOMPLETEに丸めず、PARTIAL／DEFERRED／BLOCKED_EXTERNALと残条件・ownerを記録
- 大野が通常操作、緊急停止、session失効、UNKNOWN照合、手動fallback、復旧依頼の手順を実演
- FableがG0〜G9の証拠indexと9月引継ぎbriefを交付し、大野が受入を明示

## 17.11 時間管理

- Phaseが125％超、または累積予測230h超なら停止して再見積り。
- 230h超では新featureを停止するが、read-only調査、kill switch、危険route停止、credential rotation等のactive exposure containmentは大野承認の一指示として継続できる。同時に再見積りを報告する。
- 早く終わってもfeature追加せず、例外・実機・test・復旧へ使う。
- 190h未満で完了できるのはG0〜G9、未解決BLOCKER／HIGH 0、検収削除なし、既存hardeningが実装済みと実証できた場合だけ。
- codeがあるだけの外部項目はCOMPLETEにしない。

## 17.11.1 予算・credit管理

大野の裁定により本projectの資金制約を解除する。【FIXED】これはscope制約の解除ではない。17.11の時間停止規則、17.13の後送り候補、DEFERRED、Gate、1返信1指示、必要範囲の原則を変更しない。

品質を落とすcost削減を禁止する。

- 日次消化額を理由に翌日の冒頭context、正本、test inputを削らない。
- Codex reviewの対象SHA、security検査、negative test、再reviewの粒度を費用理由で下げない。
- 価格を理由にmodelを格下げしない。
- staging、外部test契約、security key、monitoring、penetration test、backup／restoreを省かない。
- prompt cacheは応答性能・同一contextの再利用効率のために維持し、品質を下げるためのcontext切捨てには使わない。
- 0.7の文書化budget 10〜15％は維持する。資金制約解除を文書量の肥大化に使わない。

Railway staging第2環境、kintone test space、test LINE、Cloudflare有料plan、外部uptime監視、InterFAX／CloudSign等の正式test契約、hardware security key 2本、予備端末、専門業者penetration testを品質確保費として正式に予算対象へ含める。購入・契約・支払は大野だけが実行し、機能scopeを増やす根拠にはしない。

Console月間上限は予算規律ではなく、loop、credential漏えい、誤routing、異常なtoken消費を検知する安全装置である。目安は150万円相当とし、実際のUSD設定値は設定日の為替・Console仕様を見て大野が決める。到達／到達予測時は「予算を使い切った」と処理せず、次を行う。

1. 新規model requestを安全な境界で一時保留し、進行中の外部効果を増やさない。
2. token／request数、caller、session、model、loop、retry、credential使用を調査する。
3. 正常な高品質作業による消化と確認できれば、大野が上限を引き上げて再開する。
4. 原因不明、loop、漏えい疑いならincidentとしてcredential rotation、kill switch、影響調査を行う。

上限到達はscope削減、review省略、test省略、安価modelへの切替を自動発動しない。Consoleの仕様上hard stopになる場合も、原因確認後の上限変更は大野だけが行う。

日次または各12時間session終了時に、Fable／Codexの実支出、active hours、request count、token異常、Console上限比率をwork-logへ一行記録する。目的は節約ではなく、暴走・漏えい・retry storm・billing anomalyの早期検知である。金額設定、credit購入、上限変更は大野だけが実行する。

G0完了条件へ、150万円相当のConsole上限、auto reload、残高／異常消費警報、Codex課金方式、上限到達runbookの人確認を含める。

## 17.12 後送り禁止

- legacy unauthenticated入口・query secret・PII logのcontainment
- durable InboundEvent／journal、GAS per-file isolation
- queue／SoT／effect level／lineageの統合契約
- R4-2訂正可能性、App 36 DerivationRun
- prototype検収
- source of truth・role
- auth・session・step-up・audit
- no sensitive cache
- 人確定
- Q系read-only・case境界
- 正本template・draft-only
- approval hash・変更失効
- idempotency・UNKNOWN
- e内容証明人送信
- feature flag・rollback
- E2E・実機・Codex high-risk review

## 17.13 後送り候補

1. PDF全文検索UI。新規OCR text保存は後送り不可。過去backfillは件数を測って別batch可
2. 主張書面差分・時系列自動整理
3. Drive物理再編・全面移行
4. 高度なcase access／利益相反wall
5. 生産性dashboard
6. offline、native push、native app
7. bulk操作・高度shortcut
8. Q&A高度filter
9. API不可のCloudSign自動化
10. FAX本番送信（adapter・mock・承認は残す）

---

# 18. 設計文書・ADR・traceability

## 18.0 最小file構成

owner一人の通常運用で人が更新するのは、次だけとする。

- 本master
- docs/work-logs/ の当日／Phase log
- docs/adrs/ のtrigger該当判断
- docs/runbooks/ の停止・復旧手順
- 弁護士凍結仕様とapproved template

PHASE_REPORT、SOURCE_OF_TRUTH_MATRIX、TRACEABILITY_MATRIX、UX_ACCEPTANCE_MATRIX、SECURITY_REPORT等を同内容の別fileとして量産しない。必要な表はmaster／work-logのsection、またはrelease evidence generatorの出力にする。

## 18.1 文書状態

| status | 意味 | 実装 |
|---|---|---|
| DRAFT | 作成中 | 不可 |
| READY_FOR_REVIEW | review可能 | 不可 |
| APPROVED | 実装可 | 可 |
| FROZEN | 法務・料金・凍結case | 完全一致 |
| BLOCKED | 素材・判断待ち | 不可 |
| SUPERSEDED | 新docに置換 | 不可 |
| DEPRECATED | 移行中 | 新規不可 |

## 18.2 front matter

長期正本、ADR、runbook、独立したapproved仕様だけに付ける。日次work-logと自動生成evidenceへ重いfront matterを要求しない。

~~~yaml
---
doc_id: SCREEN-SORTING-001
title: 仕分け画面仕様
status: DRAFT
owner: Fable司令塔
approver: 大野
last_verified_at: null
effective_from_sha: null
supersedes: null
related_requirements: [REQ-SORT-001]
related_adrs: [ADR-0001]
---
~~~

## 18.3 traceability

REQ-ID → workflow → screen → API → source of truth → permission → test → human acceptance → release status

この完全chainを要求するのはE2／E3、auth／PII、cross-system write、法務成果物、安全不変条件だけである。E0／低risk E1はtask ID、changed files、test、human resultで足りる。traceability viewはsource annotation、test marker、release manifestから自動生成し、人が別sheetへ転記しない。高risk chainが途中で切れている場合だけGate未通過とする。

## 18.4 ADR対象

ADR triggerは0.7に従う。典型例はpasskey／session、Access前段、role／case access、retention、source of truth、approval／UNKNOWN、Drive新旧、data削除、backup／RPO／RTOである。UI文言、CSS、小refactor、test追加、approved挙動へ戻すbug fixにはADRを作らない。

## 18.5 ADR template

~~~markdown
# ADR-XXXX: 判断名

- Status: PROPOSED / ACCEPTED / REJECTED / SUPERSEDED
- Date:
- Deciders: 大野 / Fable司令塔
- Related requirements:

## Context
## Decision drivers
## Options considered
## Decision
## Consequences
## Verification
## Rollback / supersession
~~~

ACCEPTEDを静かに書き換えない。判断変更は新ADRでsupersedeする。

---

# 19. Codex独立review

Codexは第2実装者ではない。指定SHA・設計書・test結果をread-onlyで検査する。

| Review | 時期 | 主対象 | 必須gate |
|---|---|---|---|
| R0-A | Phase 0A後 | 現況・baseline・risk分類 | containment前 |
| R0-B | G0前 | containment差分・retirement計画 | Phase 1前 |
| R1 | G1前 | product・role・NFR・正本＋ingress／GAS／token／notification／PII差分 | 設計承認前 |
| R2 | G2前 | prototype・誤操作・状態誤認 | 実装前 |
| R3 | G3前 | auth・session・cache・audit | 公開・実data前 |
| R4 | G4前 | queue・dashboard・仕分け・Q&A | 実data pilot前 |
| R5 | G5前 | 帳票・正本・draft-only | 実案件利用前 |
| R6 | G6前 | approval・send・UNKNOWN | 外部test前 |
| R7 | G7前 | 全lineage・訂正・任意Drive・rollback | pilot前 |
| R8 | G8前 | 全diff・回帰・secret・運用 | controlled pilot後・一般production rollout前 |

所見はFableがACCEPT／REJECT／DEFER／NEEDS_HUMANに裁定する。Codexはcode・test・docsを編集しない。

Codex security reviewはG8.5の専門業者penetration testを代替しない。penetration test修正後は影響G8再実行とCodex R8-deltaを必須とし、G8.5-CORE／Dそれぞれに業者retestまたは契約上同等の独立確認を要する。

---

# 20. Claude Fableの実装統制

## 20.1 PC-A task票

Fableは同時に一つだけ発行する。

~~~text
[PC-A]
TASK_ID:
PHASE:
TITLE:
TIMEBOX:
TASK_MODE: READ_ONLY / IMPLEMENT / MIGRATION / REAL_DEVICE_SUPPORT

USER_OUTCOME:
このtaskで大野が何を安全にできるようになるか。

BASE_SHA:
BRANCH:
AUTHORITATIVE_DOCS:
RELATED_REQ_IDS:
RELATED_ADRS:

IN_SCOPE:
OUT_OF_SCOPE:
ALLOWED_FILES_OR_MODULES:
DO_NOT_CHANGE:

DEPENDENCIES_AND_GATE:
INPUT_FIXTURES_OR_TEMPLATES:
DATA_AND_MIGRATION_IMPACT:
EXTERNAL_EFFECT:
FEATURE_FLAG_AND_DEFAULT:

ACCEPTANCE_CRITERIA:
- Given / When / Then

REQUIRED_TESTS:
- unit
- contract／integration
- permission／negative
- regression
- human／real device

OBSERVABILITY:
ROLLBACK:
CODEX_REVIEW_TRIGGER:
HUMAN_ACTION_REQUIRED:

STOP_CONDITIONS:
- 正本不足
- 実装と実機の矛盾
- 顧客data・secret露出
- 範囲外変更が必要
- 本番事故・対外効果の可能性

COMPLETION_REPORT:
- COMPLETE / PARTIAL / BLOCKED
- BASE_SHA / HEAD / commits / git status
- 変更fileと理由
- test commandと実出力
- migration / env / 外部効果
- acceptance結果
- known issue
- 人操作
- Codex対象SHA
~~~

READ_ONLY taskはbranch／commitを要求せず、workspace変更0をDoDとする。task外の発見はDISCOVERYとして報告し、現在taskを広げない。

## 20.2 Fable司令塔起動指示

~~~text
あなたは「既存資産統合型・長期運用品質版・基準210時間」の唯一の司令塔です。

目的は機能数や速度の最大化ではありません。
数年間使い、社員追加と新業務unitに耐える内部PWAと共通業務基盤を、安全・使いやすい・変更可能・復旧可能な状態で完成させることです。

実装はPC-A／Claude Code Fableの単線だけで行います。
Codexは指定SHAと設計書の読取専用reviewだけを行い、code・test・docsを編集しません。
push、本番変更、資格情報投入、実機送信、法務・UX最終承認は大野だけが行います。

司令塔自身がrepoを直接確認できない前提を守り、最初はcode変更を指示しません。

第一回答:
1. 「司令塔を引き継ぎます」
2. reportだけで把握した現在地を3行以内。REPORT_ONLYと明示
3. 次の一指示として[PC-A-READONLY] Phase 0A現況取得taskを全文発行

[PC-A-READONLY]は次を実出力で取得します。
1. 現在のmain、git status、最新work-log、既存test実出力
2. 2026年7月完全レポート
3. 2026年8月実行計画完全レポート
4. Fable単線実装＋Codex独立review設計書
5. 本製品設計完全版
6. docs配下の機能別正本、凍結case、ADR
7. P0〜P16、実機、外部service状態

PC-Aはsource、local test、許可済みread-only CLIで取得できる範囲だけを報告する。secret値を要求・表示せず、authenticated GUI操作、production変更、external writeを行わない。PC-Aから確認できない実機・外部service状態はBLOCKED_NEEDS_HUMANとする。

PC-A報告を機械検収した次の回答:
1. REPO_VERIFIED／PRODUCTION_VERIFIEDな現在地
2. main SHA、dirty、baseline test
3. P0〜P16のREADY / TO_COLLECT / IN_PROGRESS / BLOCKED / NOT_APPLICABLE / STALE
4. 文書矛盾と正本順位による裁定候補
5. Phase 0〜9のREADY / BLOCKEDと再見積り
6. 次の一指示だけ。人の実機証拠が最優先なら[人]task、そうでなければ次の[PC-A-READONLY]または承認済みcontainment

運用:
- 1返信=1指示。[PC-A] / [CODEX-REVIEW] / [CU] / [人]を付ける
- 大野への要求は朝・夕の2窓に集約し、窓の外での新規の問いかけ・催促は緊急時に限る
- 大野に渡す全ての[人]taskは、判断材料、選択肢、推奨、完成形の実行手順を添え、そのまま判断・実行できる状態にする
- 大野の「今日は止めて」は正規の裁定であり、計画遅延・検収失敗として扱わない。「大野裁定待ち」と記録して翌窓へ持ち越す
- DRAFT、BLOCKED、SUPERSEDEDを実装しない
- Gate前に次Phaseの実装を積まない
- 実機中は当該経路の修正と再検査だけ
- 問題時は停止し、推測補完しない
- 法務文言、料金、期限、凍結case、実物templateを創作・改変しない
- machineはE2の人確認済み、名寄せ確定、承認済みへ自動遷移しない。E1のauto routingはAUTO_ROUTED_UNREVIEWEDとして別扱い
- 対外効果は生成、承認、実行を分離
- push、本番、secret、外部送信を人へ返す
- Codex所見はACCEPT / REJECT / DEFER / NEEDS_HUMANに裁定
- 早く終われば例外、実機、test、復旧へ時間を使う
- 230時間超予測なら品質を削らず、残工数・原因・選択肢を大野へ返す

完了はcode量でなくG0〜G9の証拠付き通過で判定してください。
~~~

## 20.3 大野の片手間運用モード【FIXED】

大野の通常関与は「1日2窓＋提示済み事項の随時LINE承認」とする。PC-AとCodexは日中に自走するが、人の承認・push・本番操作・実機検収が必要な地点を越えてはならない。承認待ちは安全な停止状態であり、未処理を理由に承認を推定したり別経路で進めたりしない。

本節は既存の承認権限・承認channelを変更しない。LINE上のOKは従来どおり解釈確認・起票承認の範囲に限り、対外送信、法務判断、E3承認をLINE承認へ拡張しない。各操作は本書で定めたPWA再認証、kintone操作、実機確認等の正規channelを使う。

### 朝の窓（15〜30分）

司令塔は次の順序で、一つの朝次packetとして提示する。このpacketは状況・裁定・当日候補をまとめるbriefingであって、複数taskの同時実行指示や一括承認ではない。裁定後の実行は20.2に従って次の一指示ずつ発行し、各taskの停止条件を維持する。

1. 前日work-logの要約を3行以内で提示する。完了、停止地点、未解決だけを記載する。
2. 当日の裁定事項を、相互排他的な選択肢、各選択肢の影響、司令塔の推奨付きで提示する。判断材料の原典・実出力を参照できない裁定は求めない。
3. 大野だけが行うpushを、対象branch／SHA／事前確認／完成形command／成功確認／停止条件まで揃えて渡す。
4. その日の[人]taskを受け渡す。書類収集、契約、GUI、実機操作は手順分解し、準備物、入力値の出所、期待結果、失敗時の停止地点を明記する。

司令塔は大野へ渡す前に、全てを「判断するだけ／実行するだけ」の完成形まで準備する。未確定値やsecretを推測してcommandへ埋めず、人が手元で入力する位置と確認方法を安全に示す。

### 夕の窓（30〜60分）

夕の窓は実機検収専用とする。司令塔は対象Gate、開始状態、操作手順、期待結果、確認するaudit／receipt／log、異常時の停止条件、所要時間見込みを事前に一つの検収票へまとめる。新しい実装や別featureの裁定を同じ窓へ積まず、異常を見つけた場合は当該経路を停止して証拠を保存し、次のtaskへ進まない。

### 日中の自走と随時LINE承認

- PC-Aは承認済みtaskの範囲だけを実装し、Codexは指定SHAを非同期reviewする。
- 大野の承認、push、本番変更、実機操作が必要になった時点で安全に停止し、次の窓まで待機する。
- BLOCKER、本番事故、security／privacy警報、active exposure、対外効果の誤実行または切迫だけを緊急とし、LINEで即時通知する。それ以外の質問、DISCOVERY、選択、完了報告は次の窓へ集約する。
- 随時LINE承認は、窓内で既に提示された事項または既存の承認キューに積まれた事項を、大野が都合のよい時に確認して処理できる補助経路である。司令塔が窓外に新規質問や催促を送る根拠にはしない。
- LINE未応答を承認と解釈しない。期限までに確認されなければ停止を維持し、翌窓へ持ち越す。

### 検収集中日

次は検収集中日として、司令塔が前日までに対象、準備物、開始可能条件、所要時間見込みを予告する。

- G2の主要journey走破
- 仕分け実機20件
- 帳票と実物templateの突合
- CloudSign、FAX、e内容証明等の正式test宛先送信
- Phase 0Bの封じ込め承認

active exposureまたはSEV-1に対する緊急封じ込めは前日予告を待たず、日中の緊急LINE通知規律を使う。

計画上の大野の想定拘束は、通常日1〜2時間、検収集中日3〜4時間とする。通常日の値は朝夕2窓と、大野が自発的に処理する提示済みLINE／正本画面の承認を合わせた想定である。所要時間が上限を超える見込みなら前日予告時に分割案と推奨を示し、無断で一つの窓へ詰め込まない。

### 見ていない承認の禁止

大野が対象内容、差分、実物、または判断材料を確認できない場合、形式的に承認せず「本日停止」を選ぶことを正規の運用とする。司令塔はこれを計画遅延、検収失敗、協力不足またはactive engineering工数の超過として扱わず、work-logへ「大野裁定待ち」、確認できなかった対象、現在のSHA／Gate／feature flag、外部効果なしの状態、次回提示に必要な材料を記録して翌窓へ持ち越す。calendar上の経過とrelease予測日は事実どおり更新し、待機時間をFable／PC-A／Codexのactive工数へ混ぜない。

再開時は対象SHA、source revision、承認snapshot、期限を再検査し、変更または失効した確認結果を承認根拠へ流用しない。

これは2026-07-07の司令塔バイパス事件に対する恒久策である。多忙時の直接承認によって指揮系統を迂回した再発を防ぐため、見ていない承認、承認の推定、停止中taskの迂回実行を禁止する。

## 20.4 日次・Phase報告

~~~text
DATE / ELAPSED_HOURS:
PHASE / GATE:
TASK_ID:
RESULT: COMPLETE / PARTIAL / BLOCKED

BASE_SHA / HEAD / COMMITS:
DESIGN_DOC_STATUS_CHANGES:
REQ_IDS_COMPLETED:
TEST_BASELINE / CURRENT:
REAL_DEVICE_OR_HUMAN_RESULT:
EXTERNAL_EFFECT:
CODEX_REVIEW_ID / RESULT / DISPOSITION:
OPEN_BLOCKER_HIGH_MEDIUM:
P0_P16_CHANGES:
TIME_BUDGET_USED / FORECAST_TO_G9:
ROLLBACK_POINT:
NEXT_SINGLE_INSTRUCTION:
~~~

230時間超予測時は新featureのNEXT_SINGLE_INSTRUCTIONを発行せず、原因・残工数・削れない品質・後送り候補を大野へ返す。active exposureのread-only調査／containmentだけは例外として人承認後に発行できる。

---

# 21. OPEN裁定一覧

Claudeは次を推測しない。

| ID | OPEN | owner | 決定期限 | 実証Gate |
|---|---|---|---|---|
| O-01 | 現在のdurable DB・migration基盤 | 大野＋Fable | G0 | G0 |
| O-02 | Cloudflare有料planで採用するAccess、WAF、rate limit、Bot対策、origin遮断、JWT検証の構成 | 大野＋Fable | G1 | G3 |
| O-03 | production domain／RP ID | 大野 | Phase 3開始前 | G3 |
| O-05 | 名寄せをstaffへ許す範囲 | 大野 | G1 | G4／社員追加時 |
| O-06 | audit・Q&A・snapshot retention | 大野 | G1 | G8 |
| O-07 | RPO／RTOとbackup plan | 大野＋Fable | G1 | G8 |
| O-08 | PDF previewのserver proxy／短期URL | 大野＋Fable | Phase 3開始前 | G3 |
| O-09 | Q&A正本保存先 | 大野＋Fable | G1 | G4 |
| O-10 | kintone／Drive revision固定方法 | Fable | G0 | G3 |
| O-11 | vendorごとのidempotency・timeout・status・Webhook真正性 | Fable＋人確認 | Phase 6開始前 | G6 |
| O-12 | e内容証明CU操作の安定範囲・保守方法 | 大野＋Fable | Phase 6開始前 | G6 |
| O-13 | approval有効期限 | 大野 | Phase 6開始前 | G6 |
| O-14 | idle／absolute session timeout | 大野 | Phase 3開始前 | G3 |
| O-15A | PWA staging domain・account・test device | 大野 | Phase 3開始前 | G3 |
| O-15B | 外部connectorごとのtest宛先・account | 大野 | Phase 6開始前 | G6 |
| O-16 | 暫定alert先／SEV-1責任者はG0、本番routingはG3、connector escalationはG6前 | 大野 | G0から段階確定 | G0／G3／G6／G8 |
| O-17 | incident時の証拠保全・連絡責任 | 大野 | 暫定G0、正式G3前 | G8 drill |
| O-18 | home queue priority rule | 大野 | G1 | G2 |
| O-19 | mobile bottom navの最終構成 | 大野 | G2 | G2 |
| O-20 | analyticsの有無・保存期間 | 大野 | G2 | G8 |
| O-21 | case close／archive／merge／reopen policy | 大野 | G1 | G4 |
| O-22 | 同姓同名識別で表示するマスク情報 | 大野 | G2 | G4 |
| O-23 | artifact editorのautosave・SESSION_EXPIRED復帰方針 | 大野＋Fable | G2 | G5 |
| O-24A | ArtifactVersion bytes storage／encryption／backup | 大野＋Fable | Phase 5開始前 | G5／G8 |
| O-24B | ApprovalSnapshot canonical payload／attachment storage | 大野＋Fable | Phase 6開始前 | G6／G8 |
| O-25 | public ingressとinternal PWAを同一deployか分離するか。worker別process／credentialはFIXED | 大野＋Fable | G1 | G3 |
| O-26 | App 29／30／38とWorkTask／ApprovalRequestのworkflow別authority | 大野＋Fable | G1 | G4／G6 |
| O-27 | doc type別auto routing threshold・禁止class | 大野＋Fable | G1 | G4 |
| O-28 | App 37を分割入力正本として使える実schema・不足field | 大野＋Fable | G1 | G2／G5 |
| O-29 | R4-2 soft merge移行、既存record復元、unmerge policy | 大野＋Fable | G1 | G4／G8 |
| O-30 | GAS／watcher service認証方式とquery token停止日 | 大野＋Fable | G1 | G1／G8 |
| O-31 | LINE通知に許す最小metadataと独立alert先 | 大野 | G1 | G4／G8 |
| O-32 | external processor registerと事務所のretention根拠 | 大野 | Phase 3前 | G3／G8 |
| O-33 | Drive physical reorganizationをKEEP_CURRENT／PILOT／DEFERREDのどれにするか | 大野 | G4終了時 | G7 |
| O-34 | LINE owner identity bootstrap、既存E2 task actor migration、解除手順 | 大野＋Fable | Phase 4前 | G4 |
| O-35 | Q専用LINE account、read-only token、QA専用writer、test user | 大野＋Fable | Phase 4前 | G4 |
| O-36 | 専門penetration test業者の選定、NDA、CORE／D scope、日程、再test条件 | 大野 | Release A production candidate前 | G8.5-CORE／D |

未決の間に閉じるfeature flag、owner、期限を記録する。
O-24A未決ならArtifactVersionのapproved storage実装を、O-24B未決なら外部connector実行をBLOCKEDとし、可変なkintone／Driveを再読込みする暫定実装を作らない。

## 21.1 CLOSED裁定

| ID | 裁定 | 決定 | 実証 |
|---|---|---|---|
| O-04／CLOSED-04 | passkey recovery | hardware security key 2本＋予備端末1台を購入・登録。password bypassなし | G3、半年ごとのdrill |

---

# 22. Definition of Done

## 22.1 製品設計DoD

- status、owner、approver、REQ-ID、ADR
- 正常・停止・例外・権限・正本
- 未決と非目標
- test・人検収・rollback
- 関連docとの矛盾なし
- 大野承認

## 22.2 task DoD

- scope内だけ変更
- acceptance PASS
- unit、negative、permission、regression
- test削除・skip・緩和なし
- flag default safe
- log・error・rollback・migration説明
- secret・実data混入なし
- local commit・完了報告
- 外部不足をPARTIAL／BLOCKEDと記録

## 22.3 feature DoD

- end-to-end
- role、case境界、低confidence、conflict、timeout、二重操作
- iPhone／PCまたは実帳票／許可宛先
- audit・障害log・monitor
- runbook
- traceability
- Codex未解決BLOCKER／HIGH 0

## 22.4 Slice Release DoD

- Release A／B／C／Dの当該scopeと依存Gateだけを対象にする
- release SHA、config、migration、flag、rollback
- scope内の全回帰、role、audit、monitor、manual fallback
- production Release A〜CはG8.5-CORE、connectorを含むRelease DはG8.5-D合格を要求
- Release Dだけ許可済み外部送信またはBLOCKED_EXTERNAL証拠を要求
- 未完を次sliceのCOMPLETEに混ぜない

## 22.5 Program Completion DoD

- G0〜G9
- release SHA、config、migration、flag、rollback
- baselineから全回帰PASS
- iPhone、実帳票、当該scopeの許可済み外部送信またはBLOCKED_EXTERNAL、Drive ADR結果
- 承認なし送信、E2／E3自動確定、Q系write、secret／不要PII混入なし
- backup／restore、session revoke、rollback、UNKNOWN runbook
- 大野が操作・停止・復旧を理解
- COMPLETE／PARTIAL／DEFERRED／BLOCKED_EXTERNALを正直に分類
- RV-01〜RV-14がFIXED／STALE、または該当capability DISABLED＋retirement evidence。CONTAINED期限超過、旧route／token／credential／trigger、critical process-memory state、App 34物理削除可能なruntime pathを残さない
- production release対象のevidence stateはPRODUCTION_VERIFIED
- dependency lock／SBOM／scan／runtime／CIとGAS deploy manifestをrelease recordへ保存
- 9月brief

---

# 23. 既存資産の統合裁定

## 23.1 PRESERVE／HARDEN／REPLACE／DEFER

| component・機能 | 裁定 | 理由・変更境界 |
|---|---|---|
| R4-3順位engine＋凍結47case | PRESERVE | 法務判断の中核。engine versionとDerivationRunだけ追加 |
| R3読解／R5-1 second opinion | PRESERVE＋HARDEN | schema・model・page・source revisionを残し、確定者へ格上げしない |
| R4-1人物化 | PRESERVE＋HARDEN | 根拠付き候補と人確認を分離 |
| R4-2候補signal | PRESERVE | 高価値のdomain asset |
| R4-2敗者物理削除 | RETIRE | 新規物理削除を恒久禁止。soft merge、alias、supersede、unmergeへ |
| D1混在PDF分割・原本無変更 | PRESERVE＋HARDEN | manifest、page hash、失敗隔離、6page超回帰 |
| sortation candidate logic | PRESERVE＋HARDEN | E1 auto routingとE2人確認を分離、threshold校正 |
| /scan／/ocr/fixed-asset legacy入口 | REPLACE／DISABLE | 無認証・誤更新・PII logを安全なingestへ移行 |
| S3／S4／S5／S6-1・App 35思想 | PRESERVE＋HARDEN | input snapshot、AssetSourceLink、失効ruleを追加 |
| Z1／Z2 required_persons | PRESERVE | 必要範囲の原則を守る |
| App 29／30／38 queue | PRESERVE | PWAでprojection。storage統合しない |
| App 30 state machine／M1／M4 | PRESERVE＋HARDEN | legacy承認正本を明示し、ArtifactVersionへ接続 |
| App 36 | HARDEN | current上書きではなくDerivationRun projection |
| App 37 | HARDEN | 人の分割入力正本としてschema／validationを確定 |
| 顧客Bot安全guard | PRESERVE | chat_responder境界を崩さない |
| 業務指示LINE | PRESERVE＋段階移行 | 通知・fallbackを維持、E2はPWA deep linkへ |
| FastAPI modular monolith | PRESERVE＋HARDEN | 全面rewriteせず、public／internal／worker境界を抽出 |
| BackgroundTasks／memory queue | REPLACE | durable InboundEvent／workerへ |
| GAS Drive executor | PRESERVE＋HARDEN | per-file isolation、journal、heartbeat、source hash |
| GAS editor正本 | 段階REPLACE | まずdrift検知。repo正本化はADRと人release |
| ocr_watcher | PRESERVE＋HARDEN | safe ingest、spool、retry、heartbeat |
| query string secret | REPLACE | header署名、nonce、rotation |
| Drive現行folder | PRESERVE default | PWA virtual view後も必要ならpilot |
| Drive全面再編 | DEFER | 独立migrationとして扱う |
| PWA control plane | NEW | auth、role、audit、task overlay、artifact、approval、outbox |
| Q系 | NEW | read-only connector＋QA専用append writer |
| CloudSign／FAX | NEW・段階開通 | connectorごとのpilot、UNKNOWN、kill switch |
| e内容証明 | NEW・PREPARE_ONLY | 人の最終送信を維持 |
| B系主張書面起案 | DEFER | 取り下げ裁定を維持 |
| R5束内検証、S6第2版、Z3 | DEFER | 本統合品質版へ混ぜない |

## 23.2 MigrationItem必須field

各REPLACE／段階移行は次を一件の台帳行として持つ。

| field | 内容 |
|---|---|
| migration_id／owner | 一意IDと責任者 |
| as_is_evidence | SHA、schema、実機、log、証拠状態 |
| risk／affected callers | 放置risk、GAS／watcher／LINE等 |
| target contract | TO-BE、effect level、state、credential |
| compatibility period | dual read／dual acceptの期限。dual writeは原則避ける |
| feature flag／kill switch | safe default |
| data backfill／reconciliation | 件数、hash、revision、欠落／重複 |
| cutover evidence | staging、pilot、current traffic 0 |
| rollback | code、config、data、外部効果の戻し方 |
| retirement proof | legacy route／token／permission／triggerが無効 |

## 23.3 最初の実装順

1. G0証拠固定と危険入口のcontainment
2. query token、channel fallback、PII log、webhookの封じ込み
3. durable inbound／journal、GAS per-file isolation
4. SoT、queue、effect level、lineageの統合契約
5. R4-2 soft merge、App 36 DerivationRun、App 37裁定
6. PWA read-only Release A
7. 仕分け・人物系Release B
8. 成果物Release C
9. connectorを一本ずつRelease D
10. 訂正・restore・incident drill

この順序を逆にして先に華やかなPWA画面を作ると、古い危険経路と二重正本を見栄えよく覆うだけになる。

---

# 24. 最終裁定

本計画の中心は画面数ではない。

- 日々の未処理が見える
- 対象caseと根拠が分かる
- 人が判断すべき場所で必ず止まる
- 承認内容と送信内容が同一
- 誰がいつ何を確定したか追える
- 失敗、結果不明、端末紛失、外部障害時に止めて戻せる
- 新しい業務unitを安全に追加できる

この状態まで証拠付きで仕上げて初めて「長く使える良いsystem」とする。210時間は締切ではなく、この品質へ到達するための基準工数である。既存の相続判断engineを捨てず、危険な入口・訂正不能・二重正本・結果不明だけを先に補強することが、最短ではなく最も長持ちする道である。
