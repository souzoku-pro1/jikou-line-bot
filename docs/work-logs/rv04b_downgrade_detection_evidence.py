"""RV-04b downgrade 禁止の検証力担保（独立再現可能・R-RV-04a-2 L-01 規律）。

「もし署名失敗時に旧 token へ fallback する素朴実装（naive）だったら」downgrade テストが
どう振る舞うかを実測し、本実装（strict = fallback しない）との差を示す。

実行: python docs/work-logs/rv04b_downgrade_detection_evidence.py
期待: 同一の「有効 token + 不正署名」入力に対し
  - naive（fallback あり）→ ACCEPT（downgrade 成立＝脆弱）
  - strict（本実装）      → REJECT（401）
よって downgrade 禁止テスト（REJECT を期待）は naive 実装では FAIL する。
"""
import hashlib
import hmac
import json
import sys

sys.path.insert(0, r"C:\work\jikou-line-bot")
from hub import service_auth as svc

SECRET_HEX = "cd" * 32
SECRET = bytes.fromhex(SECRET_HEX)
PATH = "/koseki/ingest"
TS = None  # 署名時に現在時刻を使う（strict 側の skew を通すため）

REG = svc.parse_registry(json.dumps({"kid-test": {
    "secret": SECRET_HEX, "caller": "gas-ingest", "allowed_methods": ["POST"],
    "allowed_paths": [PATH], "not_before": 0, "expires_at": 2 ** 31, "status": "active"}}))

VALID_TOKEN = "koseki-legacy-token"


def _headers_bad_sig(body):
    import time
    ts = str(int(time.time()))
    nonce = hashlib.sha256(b"dg-evidence").hexdigest()[:32]
    csha = hashlib.sha256(body).hexdigest()
    # 署名は「別 body」で作る＝body_mismatch/bad_sig 相当（不正署名）
    canon = svc.canonical_v1("kid-test", "gas-ingest", "POST", PATH, ts, nonce,
                             hashlib.sha256(b"OTHER").hexdigest())
    return {"X-Sig-Version": "v1", "X-Sig-Key-Id": "kid-test", "X-Sig-Caller": "gas-ingest",
            "X-Sig-Timestamp": ts, "X-Sig-Nonce": nonce, "X-Sig-Content-SHA256": csha,
            "X-Sig-Signature": svc.sign_v1(SECRET, canon)}


def _fake_verify_token(supplied):
    return hmac.compare_digest(supplied or "", VALID_TOKEN)


def strict_gate(headers, body, token):
    """本実装（authorize_ingest）と同じ判定: 署名ヘッダ在なら署名経路のみ。"""
    if svc._has_signature_headers(headers):
        st, reason, ctx = svc.verify_signature(headers, body, "POST", PATH, REG,
                                               now=_now(), skew=300)
        if st != 200:
            return f"REJECT({st}/{reason})"   # token へ fallback しない
        return "ACCEPT(sig)"
    return "ACCEPT(token)" if _fake_verify_token(token) else "REJECT(404)"


def naive_gate(headers, body, token):
    """素朴実装（脆弱）: 署名が失敗したら旧 token へ fallback してしまう。"""
    if svc._has_signature_headers(headers):
        st, reason, ctx = svc.verify_signature(headers, body, "POST", PATH, REG,
                                               now=_now(), skew=300)
        if st == 200:
            return "ACCEPT(sig)"
        # ← ここが downgrade。署名失敗を token で救ってしまう
        if _fake_verify_token(token):
            return "ACCEPT(token-fallback)"
        return f"REJECT({st}/{reason})"
    return "ACCEPT(token)" if _fake_verify_token(token) else "REJECT(404)"


def _now():
    import time
    return int(time.time())


def main():
    body = b"--BND\r\nContent-Disposition: form-data; name=\"x\"\r\n\r\nd\r\n--BND--\r\n"
    headers = _headers_bad_sig(body)   # 不正署名
    s = strict_gate(headers, body, VALID_TOKEN)
    n = naive_gate(headers, body, VALID_TOKEN)
    print("入力: 有効 query token + 不正署名ヘッダ（併記）")
    print(f"  strict（本実装）: {s}")
    print(f"  naive （fallback あり）: {n}")
    strict_rejects = s.startswith("REJECT")
    naive_accepts = n.startswith("ACCEPT")
    print()
    print("downgrade 禁止テスト（REJECT を期待）の検証力:")
    print(f"  strict は REJECT: {strict_rejects}  ← テスト PASS")
    print(f"  naive は ACCEPT : {naive_accepts}  ← 同テストは naive 実装では FAIL（=検証力あり）")
    ok = strict_rejects and naive_accepts
    print()
    print("結論:", "downgrade 禁止テストは naive 実装を検知できる（検証力あり）" if ok
          else "検証力不足（要見直し）")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
