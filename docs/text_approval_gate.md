# 相談者向け文言の承認台帳と表示ゲート（TEXT-APPROVAL-GATE-1・QUEUE-2026-10-20-HI H-0）

相談者向けの文言は、文言表（`hub/*_texts.py`）の `status=needs_approval` という**印**だけでは止まらない。描画側が
`T.text("S-1")` と直接参照すれば未承認文がそのまま相談者に出る（#307 feature/souzoku-2 の S-1〜S-3 で実際にそうなっていた）。
そこで `hub/approved_texts.py` に **承認台帳**（key → 承認済み本文の SHA256）を置き、Q4（人による承認）を通らない描画・送信経路は
`approved_texts.approved_or_safe(key, text)`（async なら `gate_and_notify`）**経由でしか文言を取得できない**形にした。
台帳に無い・本文が変わって SHA が一致しない文言は安全文「現在準備中です。弁護士より改めてご連絡いたします。」に置き換わり、
業務 LINE に「【未承認文言の参照】key=…」が 1 日 1 回届く（台帳 key `text_unapproved:{JST 日付}`）。
直接参照は機械検査で禁止する（`test_text_approval_gate.py`・`scripts/audit_static.py` (f)）。

**承認の運用**: 大野が文言を裁定したら、司令塔の票で `hub/approved_texts.APPROVED_SHA256` に `key: sha256(本文)` を 1 行追加する
（SHA は `python -c "import hashlib;print(hashlib.sha256('本文'.encode()).hexdigest())"`、または文言表の本文から PC-A が算出して票に書く）。
文言表側の本文を後で変えれば SHA が変わり、再承認まで自動的に安全文へ戻る。文言表の `status` 印は一覧・承認依頼のためのもので、
表示の可否は台帳だけが決める（印と台帳は独立）。初期の台帳は安全文 1 件のみ。
