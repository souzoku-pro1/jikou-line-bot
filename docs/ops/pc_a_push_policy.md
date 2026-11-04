# PC-A の push 運用（案）— feature/* と docs/* だけ・main は触らない

状態: **案**（K-DESIGN-1・2026-11-03）。大野の承認があるまで、現行どおり PC-A は push せず、push 行を大野に渡して `!` で実行していただきます。承認後に、本書を「運用」に改めて PC-A の許可設定を変えます。

## 1. 何が変わるか（1 枚で）

| 項目 | 現行 | 案 |
|---|---|---|
| ブランチの push | 大野が `!` で実行（PC-A は push 行を渡すだけ） | **PC-A が `feature/*`・`docs/*` に限り push する** |
| main への push | 誰もしない（merge は PR から） | 変わらず **禁止**（PC-A の許可設定でも、GitHub 側の保護でも止める） |
| force push | しない | **禁止**（`--force`・`--force-with-lease` とも。履歴を直したいときは新しい commit を積む） |
| PR の作成・本文の更新 | PC-A（GitHub API・票で明示されたとき） | 変わらず PC-A（push の直後に作成・追加 push のたびに本文 PATCH） |
| merge（＝本番 auto-deploy） | **大野の専権** | 変わらず **大野の専権**。PC-A は merge ボタンも API の merge も使わない |
| docs だけの自己 merge | 票で明示されたときのみ | 変わらず |

## 2. PC-A が push してよい条件（すべて満たすこと）

1. 票で push が成果物に含まれている（「push 行を渡す」としか書かれていない票では、従来どおり大野に渡す）。
2. 対象は `feature/*` または `docs/*` のブランチだけ。`main`・`release/*`・タグ・他人のブランチは対象外。
3. **後周を終えている**: 票の指定どおり (a) `pytest -n 2`（triage・brain matrix を除く）と (b) brain matrix を回し、許容外の失敗が 0（許容は既知の時限 7 件＋順序依存 1 件）。結果はブランチ名・tip・件数を報告に書く。
4. 秘密値の検査: push 前に差分に接続文字列・トークン・LINE ID・メールが無いことを機械で確認（既存の sink 検査＋ grep）。1 件でも疑いがあれば push しない。
5. 凍結事項・テスト削除／skip／緩和が無いことを報告に明記している。
6. ローカル tip と push 後の `origin/<branch>` が同じ SHA であることを確認し、報告に書く（`git ls-remote` で照合）。
7. 1 票 1 ブランチ（前方 merge のように複数本を扱う票は、票に列挙された本だけ）。

## 3. push の後に PC-A がすること

1. 新規ブランチなら **PR を作成**（base は票の指定・既定 main）。本文は `pgv/pr_bodies/<branch>.md` をそのまま載せる（HEAD の SHA・後周の結果・凍結不変・新設書込の列挙・規律の確認を含む）。
2. 追加 push のたびに PR 本文を **PATCH** し、HEAD・後周・変更点を更新する。
3. `progress_log` に「push: ブランチ・tip・PR 番号・時刻」を 1 行。
4. merge はしない。merge の可否・時刻（＝本番 deploy）は大野が決める。配備後の `alembic upgrade head` の要否は PR 本文に書く。

## 4. 許可設定（PC-A 側・大野の承認後に変える）

- 現在: `git push` は許可設定で遮断（deny）。
- 案: `git push origin feature/*`・`git push origin docs/*` の形だけ許可し、`git push origin main`・`--force`・`--delete`・`refs/tags` を含む形は deny のまま。
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
