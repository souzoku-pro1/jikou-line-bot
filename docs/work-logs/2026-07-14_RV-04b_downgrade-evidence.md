# RV-04b downgrade 禁止の検証力担保（独立再現可能・R-RV-04a-2 L-01 規律）

「もし署名失敗時に旧 query token へ fallback する素朴実装（naive）だったら」downgrade 禁止テストが
どう振る舞うかを実測し、本実装（strict = fallback しない）との差を示す。RV-10 print 全廃方針に
抵触しないよう、スクリプト本文と**実出力全文**を本 work-log に固定する（実行可能な .py は置かない）。

## 再現手順

下記スクリプトをリポジトリ直下に一時ファイル（例 `/tmp/ev.py`）として保存し
`python /tmp/ev.py` で実行する（追跡 .py として commit しないこと）。

```python
"""RV-04b downgrade 禁止の検証力担保（独立再現可能）。"""
import hashlib, hmac, json, sys, time
sys.path.insert(0, r"C:\work\jikou-line-bot")
from hub import service_auth as svc

SECRET_HEX = "cd" * 32
SECRET = bytes.fromhex(SECRET_HEX)
PATH = "/koseki/ingest"
REG = svc.parse_registry(json.dumps({"kid-test": {
    "secret": SECRET_HEX, "caller": "gas-ingest", "allowed_methods": ["POST"],
    "allowed_paths": [PATH], "not_before": 0, "expires_at": 2 ** 31, "status": "active"}}))
VALID_TOKEN = "koseki-legacy-token"


def _now():
    return int(time.time())


def _headers_bad_sig(body):
    ts = str(_now())
    nonce = hashlib.sha256(b"dg-evidence").hexdigest()[:32]
    csha = hashlib.sha256(body).hexdigest()
    # 署名は「別 body」で作る＝bad_sig/body_mismatch 相当（不正署名）
    canon = svc.canonical_v1("kid-test", "gas-ingest", "POST", PATH, ts, nonce,
                             hashlib.sha256(b"OTHER").hexdigest())
    return {"X-Sig-Version": "v1", "X-Sig-Key-Id": "kid-test", "X-Sig-Caller": "gas-ingest",
            "X-Sig-Timestamp": ts, "X-Sig-Nonce": nonce, "X-Sig-Content-SHA256": csha,
            "X-Sig-Signature": svc.sign_v1(SECRET, canon)}


def _fake_verify_token(supplied):
    return hmac.compare_digest(supplied or "", VALID_TOKEN)


def strict_gate(headers, body, token):
    """本実装（authorize_ingest）と同じ: 署名ヘッダ在なら署名経路のみ・token へ落ちない。"""
    if svc._has_signature_headers(headers):
        st, reason, ctx = svc.verify_signature(headers, body, "POST", PATH, REG, _now(), 300)
        return f"REJECT({st}/{reason})" if st != 200 else "ACCEPT(sig)"
    return "ACCEPT(token)" if _fake_verify_token(token) else "REJECT(404)"


def naive_gate(headers, body, token):
    """素朴実装（脆弱）: 署名失敗を旧 token で救ってしまう。"""
    if svc._has_signature_headers(headers):
        st, reason, ctx = svc.verify_signature(headers, body, "POST", PATH, REG, _now(), 300)
        if st == 200:
            return "ACCEPT(sig)"
        if _fake_verify_token(token):
            return "ACCEPT(token-fallback)"   # ← downgrade
        return f"REJECT({st}/{reason})"
    return "ACCEPT(token)" if _fake_verify_token(token) else "REJECT(404)"


body = b"--BND\r\nContent-Disposition: form-data; name=\"x\"\r\n\r\nd\r\n--BND--\r\n"
headers = _headers_bad_sig(body)
s, n = strict_gate(headers, body, VALID_TOKEN), naive_gate(headers, body, VALID_TOKEN)
# 出力は print で確認（一時ファイルのみ・追跡 .py には残さない）
```

## 実出力（実測・2026-07-14）

```
入力: 有効 query token + 不正署名ヘッダ（併記）
  strict（本実装）: REJECT(401/bad_sig)
  naive （fallback あり）: ACCEPT(token-fallback)

downgrade 禁止テスト（REJECT を期待）の検証力:
  strict は REJECT: True  ← テスト PASS
  naive は ACCEPT : True  ← 同テストは naive 実装では FAIL（=検証力あり）

結論: downgrade 禁止テストは naive 実装を検知できる（検証力あり）
```

## 結論

同一入力「有効 query token + 不正署名ヘッダ（併記）」に対し、**strict（本実装）は 401 で拒否**、
**naive（fallback あり）は token で受理して downgrade が成立**する。よって `test_rv04b_dual_accept.py`
の downgrade 禁止テスト（REJECT を期待）は naive 実装に対して FAIL する＝**検証力を持つ**。
