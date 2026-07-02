"""
pytest 共通設定

Windows ローカル環境では Python 標準の証明書ストアが OS の証明書を
参照できず SSL エラーになることがあるため、truststore があれば
OS 証明書ストアを注入する（Railway/Linux 上では実質 no-op）。
"""

try:
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass
