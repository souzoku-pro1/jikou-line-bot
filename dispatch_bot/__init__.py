"""LINE指示Bot（dispatch入口）パッケージ

設計: docs/dispatch-bot/（D系列）。既存 hub/dispatch.py（App 30 チャネル
ディスパッチャ）とは別物（名称の混同注意・設計 01 冒頭）。

顧客Bot（main.py /webhook・chat_responder）とは完全分離:
- チャネル・secret・トークン・プロンプト・ログすべて独立（設計 02 §5）
- chat_responder は一切 import しない（顧客向けガード混入の防止・確定判断9）
"""
