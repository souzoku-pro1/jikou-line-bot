"""lineq — LINE 応答品質改善トラック 相1 の評価ハーネス（LINE-Q-001）。

正本: docs/design-drafts/DRAFT_LINE_QUALITY_IMPROVEMENT.md §3.2（相1・ログ非依存）。
- 顧客データ・実ログに依存しない（合成スレッドのみ・§2.1 の変更対象外 8 項目に
  一切触れない＝chat_responder 等の本番モジュールを import しない）。
- judge（別系統 LLM）の **API 呼出しは含まない**（モデル選定=[人]ゲート。
  本パッケージはプロンプト生成・係留例・機械計数までを提供する）。
"""
