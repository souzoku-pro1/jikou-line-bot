"""RV02-ENFORCE: /scan・/ocr/fixed-asset の署名必須化の器（SERVICE_AUTH_SIGNED_REQUIRED_PATHS）。

契約（docs/plan/2026-08-16_rv02-close-runbook.md §3・票 RV02-ENFORCE-IMPL）:
- 既定（env 未設定/空）= 完全な挙動不変（RV-0102-PREP の二重受理のまま・回帰 pin）
- 列挙 path・非署名 = 404（legacy_blocked と同じ「存在しないフリ」・
  reason=signed_required_blocked で path 別計数）
- 列挙 path・署名有効 = 受理（§2.3 全8段・不正署名は従来どおり 401/403/400/409）
- 非列挙 path = 従来どおり（flag ON は unsigned_accepted で path 別計数・挙動不変）
- flag 優先順位: 列挙 path は SERVICE_AUTH_DUAL_ACCEPT_ENABLED に関わらず署名必須
  （強い方が勝つ）。両 env の組合せ全象限を固定する。
- 新 env の parser は legacy 停止 list の strict 実装を共用（closed set は opt-in 2 path
  のみ＝ingest 5 lane は列挙不可）。

検証器・registry・署名ヘッダ組立・DB mixin・ゲート通過の判定信号は
test_rv04b_dual_accept / test_rv0102_prep_signed_optin と同一物を共用する
（別実装での PASS を作らない流儀）。判定信号: /scan は未対応フォルダ名 400・
/ocr/fixed-asset は kintone env 未設定 500（いずれも認証前段を通過した後の応答）。
"""

import unittest
from unittest.mock import patch

import os  # noqa: F401  (patch.dict 対象)

from hub import service_auth as svc  # noqa: E402
from hub.service_auth import ServiceAuthConfigError  # noqa: E402

from test_rv04b_dual_accept import (  # noqa: E402
    _DbMixin, _FLAG, _REGENV, _client, _nofile_multipart, _nonce, _sig_headers)
from test_rv0102_prep_signed_optin import (  # noqa: E402
    REG_OPTIN_JSON, _ENV_ON, _OCR, _SCAN, _pdf_multipart, _scan_body)

_REQ = "SERVICE_AUTH_SIGNED_REQUIRED_PATHS"
_LOGGER = "hub.service_auth"


def _post_scan_unsigned():
    return _client.post(_SCAN, content=_scan_body(),
                        headers={"Content-Type": "application/json"})


def _post_ocr_unsigned():
    ct, body = _pdf_multipart()
    return _client.post(_OCR, content=body, headers={"Content-Type": ct})


# ── 既定（env 未設定/空文字）: 完全な挙動不変（回帰 pin） ─────────────────────
class TestDefaultEmptyUnchanged(unittest.TestCase):
    def test_flag_off_req_unset_scan_unchanged(self):
        with patch.dict(os.environ, {"GOOGLE_VISION_API_KEY": "dummy_vision"}):
            os.environ.pop(_FLAG, None)
            os.environ.pop(_REQ, None)
            # flag OFF はログも含め完全不変（unsigned_accepted も出ない）
            with self.assertNoLogs(_LOGGER, level="INFO"):
                r = _post_scan_unsigned()
            self.assertEqual(r.status_code, 400)
            self.assertIn("未対応のフォルダ名", r.json()["detail"])

    def test_flag_off_req_unset_ocr_unchanged(self):
        with patch.dict(os.environ, {"GOOGLE_VISION_API_KEY": "dummy_vision"}):
            os.environ.pop(_FLAG, None)
            os.environ.pop(_REQ, None)
            with self.assertNoLogs(_LOGGER, level="INFO"):
                r = _post_ocr_unsigned()
            self.assertEqual(r.status_code, 500)

    def test_flag_on_req_unset_http_unchanged(self):
        with patch.dict(os.environ, _ENV_ON):
            os.environ.pop(_REQ, None)
            self.assertEqual(_post_scan_unsigned().status_code, 400)
            self.assertEqual(_post_ocr_unsigned().status_code, 500)

    def test_flag_on_req_empty_string_http_unchanged(self):
        with patch.dict(os.environ, {**_ENV_ON, _REQ: ""}):
            self.assertEqual(_post_scan_unsigned().status_code, 400)
            self.assertEqual(_post_ocr_unsigned().status_code, 500)


# ── 列挙 path・非署名 = 404（signed_required_blocked・path 別計数） ──────────
class TestRequiredBlocksUnsigned(unittest.TestCase):
    def setUp(self):
        self._p = patch.dict(os.environ,
                             {**_ENV_ON, _REQ: "/scan,/ocr/fixed-asset"})
        self._p.start()

    def tearDown(self):
        self._p.stop()

    def test_scan_unsigned_blocked_404(self):
        with self.assertLogs(_LOGGER, level="INFO") as cm:
            r = _post_scan_unsigned()
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.json()["detail"], "Not Found")
        self.assertTrue(any(
            "reason=signed_required_blocked path=scan" in m for m in cm.output),
            cm.output)

    def test_ocr_unsigned_blocked_404(self):
        with self.assertLogs(_LOGGER, level="INFO") as cm:
            r = _post_ocr_unsigned()
        self.assertEqual(r.status_code, 404)
        self.assertTrue(any(
            "reason=signed_required_blocked path=ocr_fixed-asset" in m
            for m in cm.output), cm.output)

    def test_garbage_sig_headers_not_accepted(self):
        # 署名ヘッダ在＝署名経路で判定（404 の「存在しないフリ」ではなく §6.1 の拒否）。
        # 非署名遮断が「ヘッダを付けるだけ」で迂回されないことの固定。
        h = {"Content-Type": "application/json", "X-Sig-Version": "v1",
             "X-Sig-Signature": "00" * 32}
        r = _client.post(_SCAN, content=_scan_body(), headers=h)
        self.assertEqual(r.status_code, 401)   # missing_header（§2.3 第1段）


# ── 列挙 path・署名有効 = 受理（不正署名は従来どおり拒否） ────────────────────
class TestRequiredSignedAccepted(_DbMixin):
    def setUp(self):
        super().setUp()
        self._p = patch.dict(os.environ,
                             {**_ENV_ON, _REQ: "/scan,/ocr/fixed-asset"})
        self._p.start()

    def tearDown(self):
        self._p.stop()
        super().tearDown()

    def test_scan_signed_valid_passes(self):
        body = _scan_body()
        h = _sig_headers(_SCAN, body, _nonce("rv02e-ok"))
        h["Content-Type"] = "application/json"
        r = _client.post(_SCAN, content=body, headers=h)
        self.assertEqual(r.status_code, 400, r.text)   # ゲート通過→未対応フォルダ名
        self.assertIn("未対応のフォルダ名", r.json()["detail"])

    def test_ocr_signed_valid_passes(self):
        ct, body = _pdf_multipart()
        h = _sig_headers(_OCR, body, _nonce("rv02e-ok-ocr"))
        h["Content-Type"] = ct
        r = _client.post(_OCR, content=body, headers=h)
        self.assertEqual(r.status_code, 500, r.text)   # ゲート通過→kintone env 未設定

    def test_scan_bad_signature_rejected(self):
        body = _scan_body()
        h = _sig_headers(_SCAN, body, _nonce("rv02e-bad"))
        h["X-Sig-Signature"] = "00" * 32
        h["Content-Type"] = "application/json"
        self.assertEqual(
            _client.post(_SCAN, content=body, headers=h).status_code, 401)


# ── 片 path のみ列挙: 他 path は従来どおり（/scan と /ocr の独立性） ──────────
class TestPartialListIndependence(unittest.TestCase):
    def test_only_ocr_listed(self):
        with patch.dict(os.environ, {**_ENV_ON, _REQ: "/ocr/fixed-asset"}):
            self.assertEqual(_post_scan_unsigned().status_code, 400)   # 不変
            self.assertEqual(_post_ocr_unsigned().status_code, 404)    # 遮断

    def test_only_scan_listed(self):
        with patch.dict(os.environ, {**_ENV_ON, _REQ: "/scan"}):
            self.assertEqual(_post_scan_unsigned().status_code, 404)   # 遮断
            self.assertEqual(_post_ocr_unsigned().status_code, 500)    # 不変


# ── flag 優先順位の全象限（DUAL_ACCEPT × REQUIRED・強い方が勝つ） ─────────────
class TestQuadrants(_DbMixin):
    """象限: (a) flag OFF×列挙 (b) flag OFF×非列挙 (c) flag ON×列挙 (d) flag ON×非列挙。
    (c)(d) は上の class 群で固定済みのため、ここでは flag OFF 側 2 象限と
    (c)(d) の代表 1 点ずつを合わせて全象限を1か所で読める形にする。"""

    def _envq(self, extra):
        base = {_REGENV: REG_OPTIN_JSON, "GOOGLE_VISION_API_KEY": "dummy_vision"}
        return patch.dict(os.environ, {**base, **extra})

    def test_a_flag_off_listed_unsigned_blocked(self):
        with self._envq({_REQ: "/scan"}):
            os.environ.pop(_FLAG, None)
            r = _post_scan_unsigned()
            self.assertEqual(r.status_code, 404)   # flag OFF でも署名必須＝強い方が勝つ

    def test_a_flag_off_listed_signed_valid_passes(self):
        with self._envq({_REQ: "/scan"}):
            os.environ.pop(_FLAG, None)
            body = _scan_body()
            h = _sig_headers(_SCAN, body, _nonce("rv02e-qa"))
            h["Content-Type"] = "application/json"
            r = _client.post(_SCAN, content=body, headers=h)
            self.assertEqual(r.status_code, 400, r.text)   # 検証通過→未対応フォルダ名

    def test_a_flag_off_listed_signed_multipart_passes(self):
        # flag OFF×列挙×multipart: BodyCachingRoute の必須集合合流で
        # 署名検証（生 body）と UploadFile 受理が同一 body で共存すること
        with self._envq({_REQ: "/ocr/fixed-asset"}):
            os.environ.pop(_FLAG, None)
            ct, body = _pdf_multipart()
            h = _sig_headers(_OCR, body, _nonce("rv02e-qa-ocr"))
            h["Content-Type"] = ct
            r = _client.post(_OCR, content=body, headers=h)
            self.assertEqual(r.status_code, 500, r.text)   # 検証+form 双方通過

    def test_a_flag_off_listed_bad_signature_rejected(self):
        with self._envq({_REQ: "/scan"}):
            os.environ.pop(_FLAG, None)
            body = _scan_body()
            h = _sig_headers(_SCAN, body, _nonce("rv02e-qbad"))
            h["X-Sig-Signature"] = "00" * 32
            h["Content-Type"] = "application/json"
            self.assertEqual(
                _client.post(_SCAN, content=body, headers=h).status_code, 401)

    def test_b_flag_off_unlisted_unsigned_accepted(self):
        with self._envq({_REQ: "/scan"}):
            os.environ.pop(_FLAG, None)
            self.assertEqual(_post_ocr_unsigned().status_code, 500)   # 従来どおり受理

    def test_b_flag_off_unlisted_sig_headers_ignored(self):
        # flag OFF×非列挙は署名ヘッダが付いていても無視＝RV-0102 の回帰 pin 維持
        with self._envq({_REQ: "/scan"}):
            os.environ.pop(_FLAG, None)
            ct, body = _pdf_multipart()
            h = {"Content-Type": ct, "X-Sig-Version": "v1",
                 "X-Sig-Signature": "00" * 32}
            self.assertEqual(
                _client.post(_OCR, content=body, headers=h).status_code, 500)

    def test_c_flag_on_listed_unsigned_blocked(self):
        with self._envq({_FLAG: "1", _REQ: "/scan"}):
            self.assertEqual(_post_scan_unsigned().status_code, 404)

    def test_d_flag_on_unlisted_unsigned_accepted(self):
        with self._envq({_FLAG: "1", _REQ: "/scan"}):
            self.assertEqual(_post_ocr_unsigned().status_code, 500)


# ── unsigned_accepted 計数（二重受理中の可視化・path 別・挙動不変） ───────────
class TestUnsignedAcceptedCounting(unittest.TestCase):
    def test_flag_on_scan_counted(self):
        with patch.dict(os.environ, _ENV_ON):
            os.environ.pop(_REQ, None)
            with self.assertLogs(_LOGGER, level="INFO") as cm:
                r = _post_scan_unsigned()
            self.assertEqual(r.status_code, 400)   # 挙動は不変（ログのみ）
            self.assertTrue(any(
                "reason=unsigned_accepted path=scan" in m for m in cm.output),
                cm.output)

    def test_flag_on_ocr_counted(self):
        with patch.dict(os.environ, _ENV_ON):
            os.environ.pop(_REQ, None)
            with self.assertLogs(_LOGGER, level="INFO") as cm:
                r = _post_ocr_unsigned()
            self.assertEqual(r.status_code, 500)
            self.assertTrue(any(
                "reason=unsigned_accepted path=ocr_fixed-asset" in m
                for m in cm.output), cm.output)

    def test_flag_on_partial_list_unlisted_still_counted(self):
        with patch.dict(os.environ, {**_ENV_ON, _REQ: "/ocr/fixed-asset"}):
            with self.assertLogs(_LOGGER, level="INFO") as cm:
                self.assertEqual(_post_scan_unsigned().status_code, 400)
            self.assertTrue(any(
                "reason=unsigned_accepted path=scan" in m for m in cm.output),
                cm.output)

    def test_flag_off_not_counted(self):
        with patch.dict(os.environ, {"GOOGLE_VISION_API_KEY": "dummy_vision"}):
            os.environ.pop(_FLAG, None)
            os.environ.pop(_REQ, None)
            with self.assertNoLogs(_LOGGER, level="INFO"):
                self.assertEqual(_post_scan_unsigned().status_code, 400)


# ── RV02-ENFORCE-01: encoded alias（percent-encoding 迂回）の negative 固定 ───
_SCAN_ALIAS = "/s%63an"            # routing 層が /scan へ decode する encoded alias
_OCR_ALIAS = "/ocr/fixed-asse%74"  # 同 /ocr/fixed-asset


def _post_scan_alias_unsigned():
    return _client.post(_SCAN_ALIAS, content=_scan_body(),
                        headers={"Content-Type": "application/json"})


class TestEncodedAliasBlocked(unittest.TestCase):
    """routing 層は /s%63an を /scan へ decode して handler へ届けるため、raw path の
    集合照合だけでは署名必須遮断も unsigned_accepted 計数も迂回される（fix1 前の実測:
    400＝handler 到達）。正規化後照合＋正規化不能 fail-closed（reason=bad_path_blocked・
    404 の存在しないフリ）による入口遮断を固定する。"""

    def test_i_flag_off_required_scan_alias_no_handler_effect(self):
        # (i) DUAL_ACCEPT OFF・required=/scan・署名なし POST /s%63an → handler 作用 0
        with patch.dict(os.environ, {"GOOGLE_VISION_API_KEY": "dummy_vision",
                                     _REQ: "/scan"}):
            os.environ.pop(_FLAG, None)
            with self.assertLogs(_LOGGER, level="INFO") as cm:
                r = _post_scan_alias_unsigned()
            self.assertEqual(r.status_code, 404)
            self.assertEqual(r.json()["detail"], "Not Found")
            self.assertNotIn("未対応のフォルダ名", r.text)   # handler 非到達
            self.assertTrue(any(
                "reason=bad_path_blocked path=invalid_path" in m
                for m in cm.output), cm.output)

    def test_ii_flag_on_alias_not_counted_unsigned_accepted(self):
        # (ii) DUAL_ACCEPT ON でも同要求が unsigned_accepted に混入しない
        with patch.dict(os.environ, {**_ENV_ON, _REQ: "/scan"}):
            with self.assertLogs(_LOGGER, level="INFO") as cm:
                r = _post_scan_alias_unsigned()
            self.assertEqual(r.status_code, 404)
            self.assertFalse(any("unsigned_accepted" in m for m in cm.output),
                             cm.output)

    def test_ii_flag_on_required_unset_alias_not_counted(self):
        # (ii) 補: required 未設定×DUAL ON でも alias は受理計数に混入しない
        # （fix1 前は unsigned_accepted path=invalid_path で handler 到達していた）
        with patch.dict(os.environ, _ENV_ON):
            os.environ.pop(_REQ, None)
            with self.assertLogs(_LOGGER, level="INFO") as cm:
                r = _post_scan_alias_unsigned()
            self.assertEqual(r.status_code, 404)
            self.assertNotIn("未対応のフォルダ名", r.text)
            self.assertFalse(any("unsigned_accepted" in m for m in cm.output),
                             cm.output)
            self.assertTrue(any("reason=bad_path_blocked" in m for m in cm.output),
                            cm.output)

    def test_iii_signed_alias_does_not_bypass_verification(self):
        # (iii) /scan 宛の正しい署名を alias に載せても署名検証を迂回して handler へ
        # 到達しない（required 列挙時・非列挙時とも入口 404 で遮断）
        body = _scan_body()
        for tag, extra in (("required", {_REQ: "/scan"}), ("unset", {})):
            with self.subTest(quadrant=tag):
                with patch.dict(os.environ, {**_ENV_ON, **extra}):
                    if not extra:
                        os.environ.pop(_REQ, None)
                    h = _sig_headers(_SCAN, body, _nonce("rv02f1-alias-" + tag))
                    h["Content-Type"] = "application/json"
                    r = _client.post(_SCAN_ALIAS, content=body, headers=h)
                    self.assertEqual(r.status_code, 404, r.text)
                    self.assertNotIn("未対応のフォルダ名", r.text)

    def test_iv_ocr_alias_blocked(self):
        # (iv) /ocr/fixed-asset の encoded alias も同様（fix1 前は 500=handler 到達）
        with patch.dict(os.environ, {"GOOGLE_VISION_API_KEY": "dummy_vision",
                                     _REQ: "/ocr/fixed-asset"}):
            os.environ.pop(_FLAG, None)
            ct, body = _pdf_multipart()
            r = _client.post(_OCR_ALIAS, content=body,
                             headers={"Content-Type": ct})
            self.assertEqual(r.status_code, 404, r.text)

    def test_v_trailing_slash_redirect_no_handler_effect(self):
        # (v) /scan/ は redirect_slashes の 307 → /scan 再要求。追随後もゲートが
        # 遮断し handler に到達しない（redirect が遮断を跳び越える経路がない）
        with patch.dict(os.environ, {"GOOGLE_VISION_API_KEY": "dummy_vision",
                                     _REQ: "/scan"}):
            os.environ.pop(_FLAG, None)
            r = _client.post("/scan/", content=_scan_body(),
                             headers={"Content-Type": "application/json"},
                             follow_redirects=True)
            self.assertEqual(r.status_code, 404)
            self.assertNotIn("未対応のフォルダ名", r.text)

    def test_vi_partial_list_only_canonical_unlisted_accepted(self):
        # (vi) required 部分列挙時: 非列挙の**正規** path のみ従来どおり受理。
        # 非列挙の encoded alias は受理されず unsigned_accepted にも混入しない
        with patch.dict(os.environ, {**_ENV_ON, _REQ: "/scan"}):
            self.assertEqual(_post_ocr_unsigned().status_code, 500)   # 正規は従来どおり
            ct, body = _pdf_multipart()
            with self.assertLogs(_LOGGER, level="INFO") as cm:
                r = _client.post(_OCR_ALIAS, content=body,
                                 headers={"Content-Type": ct})
            self.assertEqual(r.status_code, 404)
            self.assertFalse(any("unsigned_accepted" in m for m in cm.output),
                             cm.output)

    def test_vii_non_post_method_not_reaching_handler(self):
        # (vii) method 境界: /scan の routing 許可は POST のみ＝POST 以外は 405 で
        # 業務 handler に到達しない（既定・required 列挙の双方で明文化）
        with patch.dict(os.environ, {"GOOGLE_VISION_API_KEY": "dummy_vision"}):
            os.environ.pop(_FLAG, None)
            os.environ.pop(_REQ, None)
            self.assertEqual(_client.get(_SCAN).status_code, 405)
        with patch.dict(os.environ, {"GOOGLE_VISION_API_KEY": "dummy_vision",
                                     _REQ: "/scan"}):
            os.environ.pop(_FLAG, None)
            r = _client.get(_SCAN)
            self.assertEqual(r.status_code, 405)
            self.assertNotIn("未対応のフォルダ名", r.text)


class TestLegacyDisabledAliasBlocked(unittest.TestCase):
    """票 3（対象範囲の確認）: legacy 停止 list（SERVICE_AUTH_LEGACY_DISABLED_PATHS）にも
    同種の encoded alias 迂回が実測で存在した（fix1 前: /koseki/inges%74?token=有効 が
    停止 lane の handler へ 400 到達）ため、同修正（正規化後照合＋正規化不能
    fail-closed）を適用して固定する。"""

    _ENV = {**_ENV_ON, "SERVICE_AUTH_LEGACY_DISABLED_PATHS": "/koseki/ingest",
            "KOSEKI_INGEST_TOKEN": "koseki-legacy-token",
            "BANK_INGEST_TOKEN": "bank-legacy-token"}

    def test_disabled_lane_alias_with_valid_token_blocked(self):
        with patch.dict(os.environ, self._ENV):
            ct, body = _nofile_multipart()
            with self.assertLogs(_LOGGER, level="INFO") as cm:
                r = _client.post("/koseki/inges%74?token=koseki-legacy-token",
                                 content=body, headers={"Content-Type": ct})
            self.assertEqual(r.status_code, 404, r.text)
            self.assertNotIn("PDF", r.text)   # handler 非到達
            # legacy_blocked（停止 lane への正規試行）の計数にも混入しない
            self.assertFalse(any("reason=legacy_blocked" in m for m in cm.output),
                             cm.output)
            self.assertTrue(any("reason=bad_path_blocked" in m for m in cm.output),
                            cm.output)

    def test_disabled_list_canonical_behavior_unchanged(self):
        # 正規 path の挙動は不変: 停止 lane=404（legacy_blocked）・非停止 lane=通過
        with patch.dict(os.environ, self._ENV):
            ct, body = _nofile_multipart()
            r_blocked = _client.post("/koseki/ingest?token=koseki-legacy-token",
                                     content=body, headers={"Content-Type": ct})
            self.assertEqual(r_blocked.status_code, 404)
            r_pass = _client.post("/bank/ingest?token=bank-legacy-token",
                                  content=body, headers={"Content-Type": ct})
            self.assertEqual(r_pass.status_code, 400)   # ゲート通過→file 無し 400


# ── strict parser（既存実装共用・closed set 差し替え）と起動時検証 ────────────
class TestStrictParserAndStartup(unittest.TestCase):
    def _validate(self, raw):
        with patch.dict(os.environ, {_REQ: raw}):
            return svc.validate_signed_required_paths_startup()

    def test_unset_and_empty_are_empty_set(self):
        with patch.dict(os.environ):
            os.environ.pop(_REQ, None)
            self.assertEqual(svc.validate_signed_required_paths_startup(),
                             frozenset())
        self.assertEqual(self._validate(""), frozenset())

    def test_valid_values(self):
        self.assertEqual(self._validate("/scan"), frozenset({"/scan"}))
        self.assertEqual(self._validate("/scan,/ocr/fixed-asset"),
                         frozenset({"/scan", "/ocr/fixed-asset"}))

    def test_invalid_values_rejected_with_fixed_msg(self):
        bad = ["/koseki/ingest",            # ingest lane は列挙不可（closed set 外）
               "/scan/",                    # 末尾 slash
               "/scan,/scan",               # 重複
               " /scan",                    # 前後空白
               "/scan,,/ocr/fixed-asset",   # 空要素
               "／scan"]                    # 全角
        for raw in bad:
            with self.assertRaises(ServiceAuthConfigError) as ctx:
                self._validate(raw)
            self.assertEqual(str(ctx.exception),
                             "signed required paths configuration invalid")

    def test_startup_validation_not_gated_by_dual_accept_flag(self):
        # legacy 側（H03: flag OFF は inert＝検証しない）との対比を固定:
        # 本 env は flag OFF でも実効のため、flag OFF でも異常形は起動停止。
        with patch.dict(os.environ, {_REQ: "/koseki/ingest"}):
            os.environ.pop(_FLAG, None)
            with self.assertRaises(ServiceAuthConfigError):
                svc.validate_signed_required_paths_startup()
        with patch.dict(os.environ,
                        {"SERVICE_AUTH_LEGACY_DISABLED_PATHS": "garbage//"}):
            os.environ.pop(_FLAG, None)
            self.assertEqual(svc.validate_legacy_disabled_paths_startup(),
                             frozenset())   # legacy は従来どおり flag OFF で inert

    def test_runtime_accessor_degrades_to_empty_on_parse_error(self):
        with patch.dict(os.environ, {_REQ: "/scan/"}):
            self.assertEqual(svc.signed_required_paths(), frozenset())

    def test_legacy_parser_default_unchanged(self):
        # 既定引数（legacy 用途）は従来どおり: ingest 5 lane のみ許可・opt-in path は未知値
        self.assertEqual(svc._parse_legacy_disabled_strict("/koseki/ingest"),
                         frozenset({"/koseki/ingest"}))
        with self.assertRaises(ServiceAuthConfigError) as ctx:
            svc._parse_legacy_disabled_strict("/scan")
        self.assertEqual(str(ctx.exception),
                         "legacy disabled paths configuration invalid")

    def test_registry_startup_validated_when_required_set_nonempty(self):
        # flag OFF でも REQ 非空なら壊れ registry は起動停止（fail-fast の象限拡張）
        with patch.dict(os.environ, {_REQ: "/scan", _REGENV: "not-json"}):
            os.environ.pop(_FLAG, None)
            with self.assertRaises(ServiceAuthConfigError):
                svc.validate_registry_startup()
        # 両 env 未設定なら従来どおり何もしない（壊れ registry でも 0）
        with patch.dict(os.environ, {_REGENV: "not-json"}):
            os.environ.pop(_FLAG, None)
            os.environ.pop(_REQ, None)
            self.assertEqual(svc.validate_registry_startup(), 0)


if __name__ == "__main__":
    unittest.main()
