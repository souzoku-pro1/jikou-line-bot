# PC-A の push 運用 — feature/* と docs/* だけ・main は触らない

状態: **運用**（大野承認 2026-11-04・票 PUSH-POLICY-1）。ただし **有効化は、大野が GitHub で main のブランチ保護（Restrict deletions・Block force pushes・Require a pull request）を設定し「保護できた」と伝えてから**。それまでは現行どおり PC-A は push せず、push 行を大野に渡して `!` で実行していただきます（§8）。

## 1. 何が変わるか（1 枚で）

| 項目 | 現行（有効化まで） | 運用（有効化後） |
|---|---|---|
| ブランチの push | 大野が `!` で実行（PC-A は push 行を渡すだけ） | **PC-A が `feature/*`・`docs/*` に限り push する** |
| main への push | 誰もしない（merge は PR から） | 変わらず **禁止**（`main`・`master`。PC-A の許可設定でも、GitHub 側の保護でも止める） |
| force push | しない | **禁止**（`--force`・`--force-with-lease` とも。履歴を直したいときは新しい commit を積む） |
| PR の作成・本文の更新 | PC-A（GitHub API・票で明示されたとき） | 変わらず PC-A（push の直後に作成・追加 push のたびに本文 PATCH・head 照合まで） |
| merge（＝本番 auto-deploy） | **大野の専権** | 変わらず **大野の専権**。PC-A は merge ボタンも API の merge も使わない |
| migration 適用・env・kintone の設定 | 大野 | 変わらず **大野の専権** |
| docs だけの自己 merge | 票で明示されたときのみ | 変わらず |

## 2. PC-A が push してよい条件（すべて満たすこと）

1. 票で push が成果物に含まれている（「push 行を渡す」としか書かれていない票では、従来どおり大野に渡す）。
2. 対象は `feature/*` または `docs/*` のブランチだけ。`main`・`master`・`release/*`・タグ・他人のブランチは対象外。
3. **後周を終えている**: 票の指定どおり (a) `pytest -n 2`（triage・brain matrix を除く）と (b) brain matrix を回し、許容外の失敗が 0（許容は既知の時限 7 件＋順序依存 1 件）。結果はブランチ名・tip・件数を報告に書く。
4. 秘密値の検査: push 前に差分に接続文字列・トークン・LINE ID・メールが無いことを機械で確認（既存の sink 検査＋ grep）。1 件でも疑いがあれば push しない。
5. 凍結事項・テスト削除／skip／緩和が無いことを報告に明記している。
6. ローカル tip と push 後の `origin/<branch>` が同じ SHA であることを確認し、報告に書く（`git ls-remote` で照合）。
7. 1 票 1 ブランチ（前方 merge のように複数本を扱う票は、票に列挙された本だけ）。

## 3. push の後に PC-A がすること

1. 新規ブランチなら **PR を作成**（base は票の指定・既定 main）。本文は `pgv/pr_bodies/<branch>.md` をそのまま載せる（HEAD の SHA・後周の結果・凍結不変・新設書込の列挙・規律の確認を含む）。
2. 追加 push のたびに PR 本文を **PATCH** し、HEAD・後周・変更点を更新する。**PR の head と origin とローカル tip が同じ SHA** であることを照合して報告に書く。
3. `pgv/progress_log.md` に「push: ブランチ・tip・PR 番号・時刻」を 1 行。
4. merge はしない。merge の可否・時刻（＝本番 deploy）は大野が決める。配備後の `alembic upgrade head` の要否は PR 本文に書く。

## 4. 許可設定（PC-A 側・有効化のときに変える）

- 現在: `git push` は許可設定で遮断（deny）。
- 運用: `git push origin feature/*`・`git push origin docs/*` の形だけ許可し、`git push origin main`・`master`・`--force`・`--force-with-lease`・`--delete`・`refs/tags` を含む形は deny のまま。
- GitHub の認証は現行どおり `git credential fill` の値をその場で使い、値を表示・保存しない。
- 失敗した push は PC-A が別の手段（API 複製など）で代替しない。原因と再実行案を報告して待つ（既存規律）。

## 5. GitHub 側の main 保護（大野向け・5 行）

1. GitHub の `souzoku-pro1/jikou-line-bot` → **Settings → Rules → Rulesets → New branch ruleset**（名前「main-protect」・Enforcement は Active）。
2. Target branches に **Include default branch**（main）を追加。
3. Rules で **Restrict deletions**・**Block force pushes**・**Require a pull request before merging** をオン（Required approvals は 0 のままでよい＝大野ご自身が merge するため）。
4. **Bypass list は空のまま**（管理者も直接 push しない。大野の merge は PR からなので影響なし）。
5. Create で保存。確認は、PC-A が `git push origin main` の乾式（`--dry-run`）を試して拒否されることを報告します（実 push はしません）。

## 6. 変えないこと

大野の merge 専権／マージ＝本番 auto-deploy の時刻判断は大野／`!` 失敗時の代替実行なし／凍結事項・RV-08・RV-10・承認なし送信経路ゼロ・kintone 承認なし書込ゼロ／秘密値をチャット・文書・ログに載せない。

## 7. PC-A 自身の規律ファイルに足す文面（有効化のときに追記・それまでは「push しない」のまま）

PC-A の規律は PC-A のメモリ（`memory/` の規律ファイル。CLAUDE.md を置く場合も同文）に置く。追記する文面は次のとおり（そのまま貼る）:

```
push は feature/* と docs/* のみ。main・master への push 禁止。--force・--force-with-lease 禁止。
push 前に後周条件（docs/ops/pc_a_push_policy.md §2）を満たすこと。
push 後は PR 作成/PATCH と head 照合（PR head＝origin＝ローカル tip）まで行う。
merge・migration 適用・env・kintone は大野専権のまま。失敗した push は代替実行しない。
```

## 8. 有効化の手順（大野の「保護できた」の後）

1. 大野が §5 を設定し、「保護できた」と伝える。
2. PC-A が **GitHub の API（読取のみ）** で main に有効な rule を列挙し、`deletion`・`non_fast_forward`（force push 禁止）・`pull_request` の 3 つが揃っていることを OK/NG で報告する（実 push はしない）。
   - 取得先: `GET /repos/souzoku-pro1/jikou-line-bot/rules/branches/main`（rulesets）。classic 保護の場合は `GET …/branches/main/protection` の allow_deletions=false・allow_force_pushes=false・required_pull_request_reviews あり。token は `git credential fill` の値をその場で使い、表示・保存しない。道具: `pgv/_branch_rules_check.py`。UI（Settings → Rules／Branches）での確認でも可。
   - `git push --dry-run` は確認手段にならない（2026-11-06 に判明）: dry-run は参照の照合とローカル検査だけで、サーバ側の保護判定（pre-receive）を通らない。ローカルが遅れていれば「fetch first」で拒否されるだけで保護の有無は分からず、最新なら保護があっても成功扱いになる。
   - 3 つ揃わなければ有効化しない。確認記録: 2026-11-06 API 確認＝deletion OK・non_fast_forward OK・pull_request OK（Repository ruleset・classic 保護なし）。
3. 大野が PC-A の許可設定を §4 のとおりに変え、PC-A は §7 の文面を規律ファイルに追記する。
4. **最初の push は 1 本だけ**: 本書のブランチ `docs/push-policy-1` を PC-A が push し、`git ls-remote` の SHA・PR の head・ローカル tip の 3 つが一致することを報告してから、通常運用（§2・§3）に入る。
5. 有効化前に push 待ちだったブランチ（大野に push 行を渡したもの）は、大野の `!` か、有効化後の PC-A の push か、どちらか一方で行う（二重に打たない）。

## 9. 大野向け: PC-A が push したことをどこで確認できるか（3 行）

1. **PR の head**: GitHub の PR ページ上部「〈branch〉 wants to merge N commits」の最新 commit SHA が、PC-A の報告の tip と同じ（Commits タブで時刻も見える）。
2. **progress_log**: `pgv/progress_log.md` の末尾に「push: ブランチ・tip・PR 番号・時刻」の 1 行が増える。
3. **PC-A の報告**: 票ごとの報告（`pgv/*_report.md`）と会話の末尾に「push 済み・origin＝local＝PR head＝〈SHA〉」の照合表が出る。一致していなければ PC-A は merge 可と書かない。
