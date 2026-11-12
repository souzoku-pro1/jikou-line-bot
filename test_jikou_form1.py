"""JIKOU-FORM-1: 時効診断フォーム（公開ページ+ルール判定+App 21 保存+受付番号+
弁護士通知）の固定。

固定する仕様（票の逐語）:
- 凍結文言 sha256 pin（4 パターン+共通注記・{受付番号} のみ置換可）
- 判定はサーバ側のみ・優先順 C→B→D→A・②借入時期は判定に使わず保存のみ・
  全 15 組合せ（③3 値×④5 値）を列挙 pin
- 公開条件 fail-closed: env JIKOU_LINE_ADD_URL 未設定なら /shindan 系は
  GET/POST とも 404・設定時のみ公開
- honeypot（非表示 website 欄）: 値があれば無言破棄（保存も通知もしない）
- レート制限: 信頼済み proxy ヘッダ（env SHINDAN_CLIENT_IP_HEADER・既定
  X-Real-IP）の SHA-256 キーの固定窓（10 回/600 秒）・超過は固定応答（非反射）
  （fix1 02: X-Forwarded-For は採用しない・ヘッダ欠落は client.host）
- App 21 保存は plain 値（hub.kintone の _wrap 契約・STORE-FIX1 の教訓どおり
  fake は _wrap 境界を模し {"value":…} 形は二重ラップとして fail）
- 受付番号: secrets 乱数 6 桁ゼロ埋め・**一意制約違反と確認できた閉集合**
  （400/CB_VA01/errors["record.受付番号.value"]）のみ再採番（上限 5 回）・
  結果不明（transport/5xx）は**即 unknown**=500+要確認（番号照会・同番号
  再試行はしない=fix2 fix1-01: 別申込の既存レコードを今回の成功と誤認しない）・
  403 等の確定失敗は即 500+要確認・レコード番号流用禁止（fix1 01）
- ゲート順序（fix1 04）: env→メソッド/別名→Content-Length（64KB）→レート
  →**その後にのみ body 読取**・別名は 307 でなく 404・multipart は解析しない
- バケット有界（fix1 03）: OrderedDict LRU・MAX_BUCKETS 厳密上限・最古退避
- 必須 RADIO 4 種は既定値任せにしない（④→訴訟有無の写像+他 3 種は「不明」
  明示指定）・status=問い合わせ・LINEユーザーID=""（空）を明示
- 弁護士通知は「【時効診断フォーム受付】受付番号:xxxxxx 診断パターン:X」のみ
  （債権者名・回答本文は載せない）
- 非反射: 入力値をエラー応答・結果画面に反射しない
- /app 配下に置かない（test_p4_001 の PUBLIC_ROUTES pin 維持）
"""

import hashlib
import os
import unittest
from unittest.mock import AsyncMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token",
    "DOCUMENT_WEBHOOK_SECRET": "doc-secret",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)
# 公開条件 fail-closed の既定検証のため、未設定を基本とする（テスト内で付与）
os.environ.pop("JIKOU_LINE_ADD_URL", None)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import shindan_form as sf  # noqa: E402

_LINE_URL = "https://line.me/R/ti/p/@test_jikou"


# ── 凍結文言 sha256 pin（弁護士凍結・{受付番号} プレースホルダ込みの逐語） ─────────
FROZEN_PINS = {
    "A": "5a16f85624e1f11d252d75d16cfeca05ae74efd190fe6c5394d6805282c89279",
    "B": "461bf86fdd92a301c784954ea53444d565d216b8923fe15ac48ea317616475c5",
    "C": "b152b6751d9ed9bedf0d56142c16de3c7d6675b39676e18bd04d4bd430776733",
    "D": "9858d1db287a526b7503ecd85eeba0e6194a74d109881da7148e6ec2375f957a",
}
NOTE_PIN = "1bdb98b09b3cc62bacecc3e47cbafa675b9cd1c6b0a1e1e49a272675cbd92d43"


class TestFrozenTexts(unittest.TestCase):
    def test_result_templates_sha256(self):
        self.assertEqual(set(sf.FROZEN_RESULTS), {"A", "B", "C", "D"})
        for pattern, template in sf.FROZEN_RESULTS.items():
            self.assertEqual(
                hashlib.sha256(template.encode("utf-8")).hexdigest(),
                FROZEN_PINS[pattern], f"パターン{pattern}の凍結文言が改変されている")
            self.assertIn("{受付番号}", template)

    def test_common_note_sha256(self):
        self.assertEqual(
            hashlib.sha256(sf.FROZEN_NOTE.encode("utf-8")).hexdigest(),
            NOTE_PIN, "共通注記の凍結文言が改変されている")

    def test_result_text_substitutes_number_only(self):
        text = sf.result_text("A", "012345")
        self.assertEqual(text,
                         sf.FROZEN_RESULTS["A"].replace("{受付番号}", "012345"))
        self.assertNotIn("{受付番号}", text)


# ── 判定（サーバ側・優先順 C→B→D→A・全 15 組合せの列挙 pin） ─────────────────────
JUDGE_ALL_15 = {
    ("5年以上前", "訴状が届いた"): "C",
    ("5年以上前", "支払督促が届いた"): "C",
    ("5年以上前", "その他の督促通知が届いた"): "A",
    ("5年以上前", "何も届いていない"): "A",
    ("5年以上前", "不明"): "D",
    ("5年以内", "訴状が届いた"): "C",
    ("5年以内", "支払督促が届いた"): "C",
    ("5年以内", "その他の督促通知が届いた"): "B",
    ("5年以内", "何も届いていない"): "B",
    ("5年以内", "不明"): "B",
    ("不明", "訴状が届いた"): "C",
    ("不明", "支払督促が届いた"): "C",
    ("不明", "その他の督促通知が届いた"): "D",
    ("不明", "何も届いていない"): "D",
    ("不明", "不明"): "D",
}


class TestJudge(unittest.TestCase):
    def test_all_15_combinations(self):
        combos = [(lp, cd) for lp in sf.CHOICES_LAST_PAY
                  for cd in sf.CHOICES_COURT_DOC]
        self.assertEqual(len(combos), 15)
        self.assertEqual(set(JUDGE_ALL_15), set(combos))
        for (last_pay, court_doc), expected in JUDGE_ALL_15.items():
            with self.subTest(last_pay=last_pay, court_doc=court_doc):
                self.assertEqual(sf.judge(last_pay, court_doc), expected)

    def test_borrow_not_used_in_judgement(self):
        # ②借入時期は判定に使わない（引数にすら取らない=構造的に不使用）
        import inspect
        params = list(inspect.signature(sf.judge).parameters)
        self.assertEqual(params, ["last_pay", "court_doc"])


# ── App 21 の in-memory フェイク（_wrap 境界を模す・STORE-FIX1 の教訓） ───────────
def _unique_violation() -> "sf.hub_kintone.KintoneError":
    """実 kintone の「値の重複を禁止する」違反（HTTP 400 / CB_VA01 /
    errors["record.受付番号.value"]）と同形の例外。"""
    return sf.hub_kintone.KintoneError(
        400, "CB_VA01", "入力内容が正しくありません。",
        errors={"record.受付番号.value":
                {"messages": ["値がほかのレコードと重複しています。"]}})


def _transport_error() -> "sf.hub_kintone.KintoneError":
    return sf.hub_kintone.KintoneError(0, "transport_error", "ReadTimeout")


class _FakeApp21:
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self._id = 0
        self.fail_next = 0          # 次の create を何回失敗させるか（一意制約模擬）
        self.transport_fail_next = 0   # 保存せず transport 例外（結果不明・未着）
        self.landed_fail_next = 0      # 保存した上で transport 例外（結果不明・着）
        self.search_fail_next = 0      # 照会（search_records）を失敗させる回数
        self.create_calls: list[dict] = []
        self.search_calls: list[str] = []

    async def search_records(self, app, query, fields=None):
        self.search_calls.append(query)
        if self.search_fail_next > 0:
            self.search_fail_next -= 1
            raise _transport_error()
        # query は sf 側が組む '受付番号 = "NNNNNN"' 形のみ受け付ける
        prefix = '受付番号 = "'
        assert query.startswith(prefix) and query.endswith('"'), query
        num = query[len(prefix):-1]
        return [{"$id": r["$id"]} for r in self.rows.values()
                if (r.get("受付番号") or {}).get("value") == num]

    @staticmethod
    def _reject_double_wrap(fields):
        # hub.kintone.create_record の契約は plain 値（kintone 側 _wrap が包む）。
        # {"value":…} 形が来たら二重ラップ＝実 kintone では CB_IJ01 拒否
        for code, v in (fields or {}).items():
            if isinstance(v, dict) and "value" in v:
                raise AssertionError(
                    f"double-wrapped payload: {code}={v!r}"
                    "（hub.kintone へは plain 値を渡す契約・_wrap が包む）")

    async def create_record(self, app, fields):
        self._reject_double_wrap(fields)
        self.create_calls.append(dict(fields))
        if self.fail_next > 0:
            self.fail_next -= 1
            raise _unique_violation()
        if self.transport_fail_next > 0:
            self.transport_fail_next -= 1
            raise _transport_error()
        num = fields.get("受付番号")
        if num and any((r.get("受付番号") or {}).get("value") == num
                       for r in self.rows.values()):
            raise _unique_violation()
        self._id += 1
        rid = str(self._id)
        rec = {k: {"value": v} for k, v in fields.items()}   # 実 API の _wrap
        rec["$id"] = {"value": rid}
        self.rows[rid] = rec
        if self.landed_fail_next > 0:
            self.landed_fail_next -= 1
            raise _transport_error()        # 保存は成立したが応答が届かない
        return rid

    def last_fields(self) -> dict:
        rid = str(self._id)
        return {k: v["value"] for k, v in self.rows[rid].items()
                if k != "$id"}


class _FormBase(unittest.TestCase):
    VALID = {
        "creditor": "テスト債権者株式会社",
        "borrow": "5年以上前",
        "last_pay": "5年以上前",
        "court_doc": "何も届いていない",
        "website": "",
    }

    def setUp(self):
        self.client = TestClient(main.app)
        self.fake = _FakeApp21()
        self.notify_biz = AsyncMock(return_value=True)
        self.notify_admin = AsyncMock(return_value=True)
        patches = [
            patch.dict(os.environ, {"JIKOU_LINE_ADD_URL": _LINE_URL,
                                    "ATTORNEY_LINE_USER_ID": "U_attorney"}),
            patch.object(sf.hub_kintone, "create_record",
                         self.fake.create_record),
            patch.object(sf.hub_kintone, "search_records",
                         self.fake.search_records),
            patch.object(sf.notify, "notify_business", self.notify_biz),
            patch.object(sf.notify, "notify_admin_line", self.notify_admin),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        sf._attempts.clear()
        self.addCleanup(sf._attempts.clear)

    def post(self, **over):
        data = dict(self.VALID)
        data.update(over)
        return self.client.post("/shindan", data=data)


# ── 公開条件 fail-closed（env 未設定=404） ───────────────────────────────────────
class TestEnvGate(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        os.environ.pop("JIKOU_LINE_ADD_URL", None)
        sf._attempts.clear()

    def test_unset_env_get_and_post_404(self):
        self.assertEqual(self.client.get("/shindan").status_code, 404)
        self.assertEqual(
            self.client.post("/shindan", data=_FormBase.VALID).status_code,
            404)

    def test_blank_env_still_404(self):
        with patch.dict(os.environ, {"JIKOU_LINE_ADD_URL": "   "}):
            self.assertEqual(self.client.get("/shindan").status_code, 404)

    def test_set_env_serves_form(self):
        with patch.dict(os.environ, {"JIKOU_LINE_ADD_URL": _LINE_URL}):
            resp = self.client.get("/shindan")
        self.assertEqual(resp.status_code, 200)
        for label in ("債権者名", "借入時期", "裁判所", "診断"):
            self.assertIn(label, resp.text)
        # honeypot 欄はフォームに存在する（非表示）
        self.assertIn('name="website"', resp.text)


# ── 正常系（保存 payload pin・結果画面・弁護士通知） ─────────────────────────────
class TestSubmitFlow(_FormBase):
    def test_payload_pin_plain_values(self):
        with patch.object(sf, "_draw_number", return_value="012345"):
            resp = self.post()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.fake.last_fields(), {
            "受付番号": "012345",
            "受付チャネル": "フォーム",
            "診断パターン": "A",
            "status": "問い合わせ",
            "LINEユーザーID": "",
            "問い合わせ業者名": "テスト債権者株式会社",
            "借入時期_テキスト": "5年以上前",
            "最終返済日_テキスト": "5年以上前",
            "裁判所書類": "何も届いていない",
            # 必須 RADIO 4 種は既定値（あり）任せにしない・明示指定
            "ラジオボタン": "なし",          # １０年以内の訴訟の有無 ← ④写像
            "ラジオボタン_2": "不明",        # 住民票と居住地の相違（④から写像不能）
            "ラジオボタン_3": "不明",        # 業者への電話有無（同上）
            "ラジオボタン_4": "不明",        # アンケート・書面送付有無（同上）
        })

    def test_sosho_radio_mapping_all_5(self):
        expected = {
            "訴状が届いた": "あり",
            "支払督促が届いた": "あり",
            "その他の督促通知が届いた": "なし",
            "何も届いていない": "なし",
            "不明": "不明",
        }
        for court_doc, radio in expected.items():
            with self.subTest(court_doc=court_doc):
                self.fake.rows.clear()
                sf._attempts.clear()
                self.assertEqual(self.post(court_doc=court_doc).status_code,
                                 200)
                self.assertEqual(self.fake.last_fields()["ラジオボタン"],
                                 radio)

    def test_result_page_frozen_text_number_note_and_line_link(self):
        with patch.object(sf, "_draw_number", return_value="654321"):
            resp = self.post(last_pay="5年以内")   # → B
        body = resp.text
        expected_text = sf.result_text("B", "654321")
        self.assertIn(expected_text.replace("\n", "<br>"), body)
        self.assertIn("受付番号：654321", body)
        self.assertIn(sf.FROZEN_NOTE, body)
        self.assertIn(_LINE_URL, body)
        # 受付番号は画面表示のみ（URL に載せない・リンク href に混入しない）
        self.assertNotIn("654321", _LINE_URL)

    def test_pattern_saved_matches_judgement(self):
        self.post(last_pay="不明", court_doc="不明")   # → D
        self.assertEqual(self.fake.last_fields()["診断パターン"], "D")

    def test_attorney_notify_fixed_text_no_pii(self):
        with patch.object(sf, "_draw_number", return_value="222333"):
            self.post(creditor="秘匿すべき債権者名")
        self.notify_biz.assert_awaited_once()
        to, text = self.notify_biz.await_args.args
        self.assertEqual(to, "U_attorney")
        self.assertEqual(text,
                         "【時効診断フォーム受付】受付番号:222333 診断パターン:A")
        self.assertNotIn("秘匿すべき債権者名", text)

    def test_notify_failure_does_not_break_result(self):
        # 通知は best-effort（App 21 レコードが正本・失敗しても結果画面は返す）
        self.notify_biz.return_value = False
        self.assertEqual(self.post().status_code, 200)
        self.assertEqual(len(self.fake.rows), 1)


# ── 受付番号（6 桁ゼロ埋め・重複再採番≤5・全失敗=500+要確認通知） ────────────────
class TestReceiptNumber(_FormBase):
    def test_number_is_6_digit_zero_padded(self):
        with patch.object(sf.secrets, "randbelow", return_value=7):
            self.post()
        self.assertEqual(self.fake.last_fields()["受付番号"], "000007")

    def test_unique_conflict_redraws_and_succeeds(self):
        self.fake.fail_next = 2
        draws = iter(["111111", "111111", "999999"])
        with patch.object(sf, "_draw_number", side_effect=draws):
            resp = self.post()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(self.fake.last_fields()["受付番号"], "999999")
        self.notify_admin.assert_not_awaited()

    def test_all_5_attempts_fail_fixed_500_and_admin_alert(self):
        self.fake.fail_next = 5
        resp = self.post()
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(len(self.fake.rows), 0)
        self.notify_admin.assert_awaited_once()
        self.assertIn("要確認", self.notify_admin.await_args.args[0])
        self.assertIn("時効診断フォーム", self.notify_admin.await_args.args[0])
        self.notify_biz.assert_not_awaited()      # 保存失敗時は受付通知しない
        # 応答は固定文言のみ（入力値・番号を反射しない）
        self.assertNotIn("テスト債権者株式会社", resp.text)

    def test_create_called_at_most_5_times(self):
        calls = []
        real = self.fake.create_record

        async def _spy(app, fields):
            calls.append(fields)
            return await real(app, fields)
        self.fake.fail_next = 99
        with patch.object(sf.hub_kintone, "create_record", _spy):
            self.assertEqual(self.post().status_code, 500)
        self.assertEqual(len(calls), 5)


# ── honeypot・レート制限・入力検証（非反射） ─────────────────────────────────────
class TestSpamGuards(_FormBase):
    def test_honeypot_silently_discards(self):
        resp = self.post(website="http://spam.example")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(self.fake.rows), 0)      # 保存しない
        self.notify_biz.assert_not_awaited()          # 通知もしない
        self.notify_admin.assert_not_awaited()
        self.assertNotIn("受付番号：", resp.text)      # 番号は発行しない

    def test_rate_limit_fixed_window(self):
        for _ in range(sf.RATE_LIMIT):
            self.assertEqual(self.post().status_code, 200)
        resp = self.post()
        self.assertEqual(resp.status_code, 429)
        self.assertEqual(len(self.fake.rows), sf.RATE_LIMIT)
        # 固定応答（入力値の反射なし）
        self.assertNotIn("テスト債権者株式会社", resp.text)

    def test_rate_limit_values_pinned(self):
        self.assertEqual((sf.RATE_LIMIT, sf.RATE_WINDOW_SECONDS), (10, 600))

    def test_invalid_choice_fixed_400_no_reflection(self):
        payload = "<script>alert(1)</script>"
        for field in ("borrow", "last_pay", "court_doc"):
            with self.subTest(field=field):
                resp = self.post(**{field: payload})
                self.assertEqual(resp.status_code, 400)
                self.assertNotIn(payload, resp.text)
                self.assertNotIn("alert(1)", resp.text)
        self.assertEqual(len(self.fake.rows), 0)

    def test_missing_choice_fixed_400(self):
        resp = self.client.post("/shindan", data={"creditor": "x"})
        self.assertEqual(resp.status_code, 400)

    def test_creditor_over_100_chars_fixed_400(self):
        resp = self.post(creditor="あ" * 101)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(len(self.fake.rows), 0)

    def test_creditor_100_chars_ok_and_optional(self):
        self.assertEqual(self.post(creditor="あ" * 100).status_code, 200)
        self.fake.rows.clear()
        sf._attempts.clear()
        self.assertEqual(self.post(creditor="").status_code, 200)
        self.assertEqual(self.fake.last_fields()["問い合わせ業者名"], "")


# ── 配線（/app 配下に置かない・main へ登録済み） ─────────────────────────────────
class TestWiring(unittest.TestCase):
    def test_router_paths_all_under_shindan_not_app(self):
        paths = [r.path for r in sf.router.routes]
        self.assertTrue(paths)
        for path in paths:
            self.assertTrue(path.startswith("/shindan"), path)
            self.assertFalse(path.startswith("/app"), path)

    def test_registered_in_main_app(self):
        # 本環境の FastAPI は include_router を _IncludedRouter として保持する
        # （app.routes に flat な path が出ない）ため再帰で列挙する
        def _paths(routes):
            out = []
            for r in routes:
                p = getattr(r, "path", None)
                if isinstance(p, str):
                    out.append(p)
                inner = (getattr(r, "original_router", None)
                         or getattr(r, "router", None))
                if inner is not None and hasattr(inner, "routes"):
                    out.extend(_paths(inner.routes))
            return out
        self.assertIn("/shindan", _paths(main.app.routes))


# ══════════════════════════════════════════════════════════════════════════════
# fix1（R-JIKOU-FORM-1 の 01〜04・全て HIGH）
# ══════════════════════════════════════════════════════════════════════════════

# ── 01: 書込結果不明時の再採番で二重作成 ─────────────────────────────────────────
class TestUniqueViolationClosedSet(unittest.TestCase):
    """再採番は「kintone の一意制約違反」と確認できた閉集合のみ（fail-closed）。"""

    def test_closed_set_pinned(self):
        self.assertEqual(sf.UNIQUE_VIOLATION_STATUS, 400)
        self.assertEqual(sf.UNIQUE_VIOLATION_CODES, frozenset({"CB_VA01"}))
        self.assertEqual(sf.UNIQUE_VIOLATION_ERROR_KEY, "record.受付番号.value")

    def test_hub_kintone_normalization_keeps_errors_detail(self):
        # hub.kintone._raise_error の分類を実測: errors 詳細が KintoneError に残る
        class _Resp:
            status_code = 400
            text = "x"

            @staticmethod
            def json():
                return {"code": "CB_VA01", "id": "x",
                        "message": "入力内容が正しくありません。",
                        "errors": {"record.受付番号.value":
                                   {"messages": ["値がほかのレコードと重複しています。"]}}}
        with self.assertRaises(sf.hub_kintone.KintoneError) as ctx:
            sf.hub_kintone._raise_error(_Resp())
        e = ctx.exception
        self.assertEqual((e.status, e.code), (400, "CB_VA01"))
        self.assertIn("record.受付番号.value", e.errors)
        self.assertTrue(sf._is_unique_violation(e))

    def test_classification_table(self):
        K = sf.hub_kintone.KintoneError
        cases = {
            "unique": (_unique_violation(), "duplicate"),
            # CB_VA01 でも受付番号以外の欄（スキーマ不整合）は再採番しない
            "cb_va01_other_field": (
                K(400, "CB_VA01", "x",
                  errors={"record.診断パターン.value": {"messages": ["x"]}}),
                "failed"),
            "cb_va01_no_detail": (K(400, "CB_VA01", "x"), "failed"),
            "forbidden_403": (K(403, "GAIA_AP15", "x"), "failed"),
            "unauth_401": (K(401, "GAIA_IA02", "x"), "failed"),
            "not_found_404": (K(404, "GAIA_RE01", "x"), "failed"),
            "transport": (_transport_error(), "unknown"),
            "server_500": (K(500, "GAIA_UN01", "x"), "unknown"),
            "bad_gateway_502": (K(502, "", ""), "unknown"),
            "unavailable_503": (K(503, "", ""), "unknown"),
        }
        for name, (err, want) in cases.items():
            with self.subTest(name=name):
                self.assertEqual(sf._classify_create_error(err), want)


class TestWriteConvergence(_FormBase):
    """fix2（fix1-01）: 結果不明（transport/5xx）は**即 unknown**。番号照会で
    既存レコードを今回の成功と誤認しない・同番号の create 再試行もしない
    （再試行の一意違反が今回の書込か別申込かを識別できないため）。"""

    def _assert_unknown_500(self, resp, number: str, creditor: str):
        self.assertEqual(resp.status_code, 500)
        self.assertNotIn("受付番号：", resp.text)          # 表示番号なし
        self.assertNotIn(number, resp.text)
        self.assertNotIn(creditor, resp.text)
        self.assertEqual(self.fake.search_calls, [])       # 番号照会をしない
        self.assertEqual(len(self.fake.create_calls), 1)   # 同番号再試行もしない
        self.notify_biz.assert_not_awaited()               # 受付通知しない
        self.notify_admin.assert_awaited_once()
        text = self.notify_admin.await_args.args[0]
        self.assertIn("要確認", text)
        self.assertIn(number, text)                        # 弁護士が突合できる番号
        self.assertIn("unknown", text)                     # 区分
        self.assertIn(sf.ALERT_OTHER_APPLICANT_NOTE, text)  # 別申込の可能性の注意
        self.assertIn("別申込", sf.ALERT_OTHER_APPLICANT_NOTE)
        self.assertNotIn(creditor, text)                   # PII 非搭載

    def test_preexisting_other_record_and_unlanded_transport_is_unknown(self):
        # 同番号の別顧客レコードが事前存在 + 今回の create は未着 transport 例外
        # → 既存レコードを今回の成功と誤認しない（unknown → 500+要確認）
        other = {**_FormBase.VALID, "受付番号": "424242",
                 "問い合わせ業者名": "別申込の債権者", "website": ""}
        other.pop("website")
        self.fake.rows["100"] = {k: {"value": v} for k, v in other.items()}
        self.fake.rows["100"]["$id"] = {"value": "100"}
        self.fake.transport_fail_next = 1
        with patch.object(sf, "_draw_number", return_value="424242"):
            resp = self.post(creditor="今回の債権者")
        self._assert_unknown_500(resp, "424242", "今回の債権者")
        self.assertEqual(len(self.fake.rows), 1)           # 別申込の行のみ
        self.assertEqual(self.fake.rows["100"]["問い合わせ業者名"]["value"],
                         "別申込の債権者")                 # 上書きもしない

    def test_other_process_wins_same_number_after_unknown(self):
        # 結果不明の直後に別処理が同番号で先勝ち → 照会も再試行もせず unknown
        real_create = _FakeApp21.create_record.__get__(self.fake)

        async def _create(app, fields):
            self.fake.create_calls.append(dict(fields))
            if len(self.fake.create_calls) == 1:
                # 未着のまま、別処理が同番号の行を作る
                self.fake.rows["200"] = {
                    "受付番号": {"value": fields["受付番号"]},
                    "問い合わせ業者名": {"value": "別申込"},
                    "$id": {"value": "200"}}
                raise _transport_error()
            return await real_create(app, fields)
        with patch.object(sf.hub_kintone, "create_record", _create), \
                patch.object(sf, "_draw_number", return_value="313131"):
            resp = self.post(creditor="今回の債権者")
        self._assert_unknown_500(resp, "313131", "今回の債権者")
        self.assertEqual(list(self.fake.rows), ["200"])

    def test_landed_transport_error_is_unknown_not_created(self):
        # 保存は成立したが応答が届かない → 有無を確定できないので unknown
        # （レコードは残るため、通知の番号で弁護士が App 21 と突合する）
        self.fake.landed_fail_next = 1
        with patch.object(sf, "_draw_number", return_value="515151"):
            resp = self.post(creditor="今回の債権者")
        self._assert_unknown_500(resp, "515151", "今回の債権者")
        self.assertEqual(len(self.fake.rows), 1)

    def test_server_5xx_is_unknown_no_retry(self):
        err = sf.hub_kintone.KintoneError(503, "GAIA_XX02", "x")
        with patch.object(sf.hub_kintone, "create_record",
                          AsyncMock(side_effect=err)) as create, \
                patch.object(sf, "_draw_number", return_value="616161"):
            resp = self.post(creditor="秘匿すべき債権者名")
        self.assertEqual(create.await_count, 1)
        self.fake.create_calls.append({})                  # helper 用の件数合わせ
        self._assert_unknown_500(resp, "616161", "秘匿すべき債権者名")

    def test_lookup_and_retry_paths_removed(self):
        self.assertFalse(hasattr(sf, "_lookup_by_number"))
        self.assertFalse(hasattr(sf, "_UNKNOWN_RETRIES"))
        import inspect
        src = inspect.getsource(sf)
        self.assertNotIn("search_records(", src)           # 照会経路が存在しない

    def test_unique_violation_only_redraws(self):
        self.fake.fail_next = 2
        draws = iter(["111111", "111111", "999999"])
        with patch.object(sf, "_draw_number", side_effect=draws):
            resp = self.post()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.fake.last_fields()["受付番号"], "999999")
        self.assertEqual(self.fake.search_calls, [])   # 一意違反は照会不要

    def test_403_no_redraw_immediate_500(self):
        err = sf.hub_kintone.KintoneError(403, "GAIA_AP15", "forbidden")
        with patch.object(sf.hub_kintone, "create_record",
                          AsyncMock(side_effect=err)) as create:
            resp = self.post()
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(create.await_count, 1)        # 再採番なし・再試行なし
        self.assertEqual(self.fake.search_calls, [])
        self.notify_admin.assert_awaited_once()
        self.assertIn("要確認", self.notify_admin.await_args.args[0])
        self.notify_biz.assert_not_awaited()

    def test_cb_va01_other_field_no_redraw(self):
        err = sf.hub_kintone.KintoneError(
            400, "CB_VA01", "x",
            errors={"record.診断パターン.value": {"messages": ["x"]}})
        with patch.object(sf.hub_kintone, "create_record",
                          AsyncMock(side_effect=err)) as create:
            resp = self.post()
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(create.await_count, 1)
        self.notify_admin.assert_awaited_once()


# ── 02: クライアント IP の導出（信頼済み proxy ヘッダのみ・XFF 不採用） ─────────
class TestClientIpDerivation(_FormBase):
    def test_default_header_and_env_pinned(self):
        self.assertEqual(sf.CLIENT_IP_HEADER_ENV, "SHINDAN_CLIENT_IP_HEADER")
        self.assertEqual(sf.DEFAULT_CLIENT_IP_HEADER, "X-Real-IP")

    def test_variable_xff_same_bucket(self):
        # 固定クライアント（TestClient の client.host）+ 可変 XFF → 同一 bucket
        for i in range(sf.RATE_LIMIT):
            r = self.client.post("/shindan", data=self.VALID,
                                 headers={"X-Forwarded-For": f"10.0.{i}.1"})
            self.assertEqual(r.status_code, 200)
        r = self.client.post("/shindan", data=self.VALID,
                             headers={"X-Forwarded-For": "10.9.9.9"})
        self.assertEqual(r.status_code, 429)
        self.assertEqual(len(sf._attempts), 1)

    def test_trusted_header_splits_buckets(self):
        for _ in range(sf.RATE_LIMIT):
            self.assertEqual(self.client.post(
                "/shindan", data=self.VALID,
                headers={"X-Real-IP": "203.0.113.1"}).status_code, 200)
        self.assertEqual(self.client.post(
            "/shindan", data=self.VALID,
            headers={"X-Real-IP": "203.0.113.1"}).status_code, 429)
        self.assertEqual(self.client.post(
            "/shindan", data=self.VALID,
            headers={"X-Real-IP": "203.0.113.2"}).status_code, 200)
        self.assertEqual(len(sf._attempts), 2)

    def test_env_header_override(self):
        with patch.dict(os.environ, {sf.CLIENT_IP_HEADER_ENV: "CF-Connecting-IP"}):
            for _ in range(sf.RATE_LIMIT):
                self.assertEqual(self.client.post(
                    "/shindan", data=self.VALID,
                    headers={"CF-Connecting-IP": "198.51.100.1",
                             "X-Real-IP": "203.0.113.7"}).status_code, 200)
            self.assertEqual(self.client.post(
                "/shindan", data=self.VALID,
                headers={"CF-Connecting-IP": "198.51.100.1",
                         "X-Real-IP": "203.0.113.8"}).status_code, 429)
            self.assertEqual(self.client.post(
                "/shindan", data=self.VALID,
                headers={"CF-Connecting-IP": "198.51.100.2",
                         "X-Real-IP": "203.0.113.7"}).status_code, 200)

    def test_env_cannot_select_xff(self):
        # X-Forwarded-For は env で指定しても採用しない（既定へ fail-closed）
        with patch.dict(os.environ, {sf.CLIENT_IP_HEADER_ENV: "x-forwarded-for"}):
            self.assertEqual(sf._client_ip_header(), sf.DEFAULT_CLIENT_IP_HEADER)

    def test_missing_header_falls_back_to_client_host(self):
        class _Req:
            headers = {}

            class client:
                host = "192.0.2.10"
        self.assertEqual(sf._rate_key(_Req()),
                         hashlib.sha256(b"192.0.2.10").hexdigest())
        # ヘッダ値は SHA-256 のみ保持（生 IP を鍵に残さない）
        class _Req2:
            headers = {"x-real-ip": "203.0.113.5"}
            client = None
        self.assertEqual(sf._rate_key(_Req2()),
                         hashlib.sha256(b"203.0.113.5").hexdigest())


# ── 03: MAX_BUCKETS の厳密上限（OrderedDict LRU・最古退避） ──────────────────────
class TestBoundedBuckets(unittest.TestCase):
    def setUp(self):
        sf._attempts.clear()
        self.addCleanup(sf._attempts.clear)

    def test_structure_is_ordered_dict(self):
        from collections import OrderedDict
        self.assertIsInstance(sf._attempts, OrderedDict)
        self.assertEqual(sf.MAX_BUCKETS, 5000)

    def test_size_never_exceeds_max(self):
        now = 1_000_000.0
        with patch.object(sf, "MAX_BUCKETS", 5):
            for i in range(5 + 1):
                sf._rate_exceeded(f"k{i}", now)
                self.assertLessEqual(len(sf._attempts), 5)
            self.assertEqual(len(sf._attempts), 5)
            self.assertNotIn("k0", sf._attempts)      # 最古（LRU）を退避
            self.assertIn("k5", sf._attempts)

    def test_surviving_bucket_keeps_limit_after_eviction(self):
        now = 1_000_000.0
        with patch.object(sf, "MAX_BUCKETS", 5):
            for i in range(4):
                sf._rate_exceeded(f"k{i}", now)
            for _ in range(sf.RATE_LIMIT):            # A を上限まで消費（最終 touch）
                self.assertFalse(sf._rate_exceeded("A", now))
            self.assertEqual(len(sf._attempts), 5)
            self.assertFalse(sf._rate_exceeded("new1", now))   # 新規は退避で受入
            self.assertEqual(len(sf._attempts), 5)     # 上限超えなし
            self.assertNotIn("k0", sf._attempts)        # 最古（k0）が退避された
            self.assertIn("A", sf._attempts)            # 直近 touch は生存
            self.assertTrue(sf._rate_exceeded("A", now))   # 制限は効いたまま

    def test_expired_front_pruned_amortized(self):
        now = 1_000_000.0
        with patch.object(sf, "MAX_BUCKETS", 5):
            sf._rate_exceeded("old", now - sf.RATE_WINDOW_SECONDS - 1)
            for i in range(4):
                sf._rate_exceeded(f"k{i}", now)
            sf._rate_exceeded("fresh", now)
            self.assertNotIn("old", sf._attempts)      # 期限切れは先頭から掃除
            self.assertEqual(len(sf._attempts), 5)

    def test_no_full_scan_on_request(self):
        # リクエストごとの全件走査をしない（先頭から定数ステップのみ）
        self.assertLessEqual(sf._PRUNE_STEPS, 4)


# ── 04: env ゲート・レート制限が Form 解析より前 ─────────────────────────────────
class _NoParse:
    """body 解析器に触れた時点で失敗させる（ゲート前解析の検出）。"""

    def __enter__(self):
        import starlette.requests as sr
        import starlette.formparsers as fp

        def _boom(*a, **k):
            raise AssertionError("body parser reached before gate")

        async def _aboom(*a, **k):
            raise AssertionError("request.form() reached before gate")
        self._p = [patch.object(sr.Request, "form", _aboom),
                   patch.object(fp.MultiPartParser, "__init__", _boom),
                   patch.object(fp.FormParser, "__init__", _boom)]
        for p in self._p:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self._p:
            p.stop()
        return False


def _multipart_body() -> tuple[str, bytes]:
    boundary = "----boundary123"
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"f\";"
            " filename=\"a.bin\"\r\nContent-Type: application/octet-stream"
            "\r\n\r\n" + "A" * 2000 + "\r\n--" + boundary + "--\r\n")
    return f"multipart/form-data; boundary={boundary}", body.encode()


class TestGateBeforeBody(unittest.TestCase):
    ALIASES = ("/shindan/", "/shindan%2F", "/shindan/x", "/shindan%2Fx")

    def setUp(self):
        self.client = TestClient(main.app)
        os.environ.pop("JIKOU_LINE_ADD_URL", None)
        sf._attempts.clear()
        self.addCleanup(sf._attempts.clear)

    def test_body_limit_pinned(self):
        self.assertEqual(sf.MAX_BODY_BYTES, 64 * 1024)

    def test_env_unset_all_methods_and_aliases_404_without_parse(self):
        with _NoParse():
            self.assertEqual(self.client.get("/shindan").status_code, 404)
            self.assertEqual(self.client.head("/shindan").status_code, 404)
            for method in ("put", "patch", "delete", "options"):
                with self.subTest(method=method):
                    r = getattr(self.client, method)("/shindan")
                    self.assertEqual(r.status_code, 404)
            r = self.client.post("/shindan", data=_FormBase.VALID)
            self.assertEqual(r.status_code, 404)
            for alias in self.ALIASES:
                for method in ("get", "post"):
                    with self.subTest(alias=alias, method=method):
                        r = getattr(self.client, method)(
                            alias, follow_redirects=False)
                        self.assertEqual(r.status_code, 404)   # 307 でない
                        self.assertNotIn("location", r.headers)

    def test_env_unset_huge_and_malformed_bodies_404_without_parse(self):
        with _NoParse():
            big = "creditor=" + "a" * (sf.MAX_BODY_BYTES + 1)
            r = self.client.post("/shindan", content=big.encode(), headers={
                "Content-Type": "application/x-www-form-urlencoded"})
            self.assertEqual(r.status_code, 404)
            ct, body = _multipart_body()
            r = self.client.post("/shindan", content=body,
                                 headers={"Content-Type": ct})
            self.assertEqual(r.status_code, 404)
            r = self.client.post("/shindan", content=b"--garbage", headers={
                "Content-Type": "multipart/form-data; boundary=zzz"})
            self.assertEqual(r.status_code, 404)      # parser 由来 400 が出ない

    def test_env_set_aliases_and_other_methods_still_404(self):
        with patch.dict(os.environ, {"JIKOU_LINE_ADD_URL": _LINE_URL}):
            for alias in self.ALIASES:
                with self.subTest(alias=alias):
                    r = self.client.post(alias, data=_FormBase.VALID,
                                         follow_redirects=False)
                    self.assertEqual(r.status_code, 404)
            for method in ("put", "patch", "delete"):
                self.assertEqual(
                    getattr(self.client, method)("/shindan").status_code, 404)
            self.assertEqual(self.client.head("/shindan").status_code, 200)

    def test_env_set_oversize_and_multipart_rejected_before_parse(self):
        with patch.dict(os.environ, {"JIKOU_LINE_ADD_URL": _LINE_URL}), \
                _NoParse():
            big = "creditor=" + "a" * (sf.MAX_BODY_BYTES + 1)
            r = self.client.post("/shindan", content=big.encode(), headers={
                "Content-Type": "application/x-www-form-urlencoded"})
            self.assertEqual(r.status_code, 404)
            ct, body = _multipart_body()
            r = self.client.post("/shindan", content=body,
                                 headers={"Content-Type": ct})
            self.assertEqual(r.status_code, 404)        # multipart は受けない
            self.assertEqual(len(sf._attempts), 0)      # レート計上より前に遮断

    def test_env_set_rate_limit_stops_before_parse(self):
        fake = _FakeApp21()
        with patch.dict(os.environ, {"JIKOU_LINE_ADD_URL": _LINE_URL}), \
                patch.object(sf.hub_kintone, "create_record", fake.create_record), \
                patch.object(sf.hub_kintone, "search_records", fake.search_records), \
                patch.object(sf.notify, "notify_business", AsyncMock(return_value=True)):
            for _ in range(sf.RATE_LIMIT):
                self.assertEqual(self.client.post(
                    "/shindan", data=_FormBase.VALID).status_code, 200)
            with _NoParse():
                r = self.client.post("/shindan", data=_FormBase.VALID)
                self.assertEqual(r.status_code, 429)
                # honeypot 値があっても解析前に止まる（bot の連射も計上）
                r = self.client.post("/shindan", data={**_FormBase.VALID,
                                                       "website": "x"})
                self.assertEqual(r.status_code, 429)

    def test_gate_order_is_documented_in_entry(self):
        # POST 入口は Form パラメータ依存を持たない（素の Request のみ）
        import inspect
        params = list(inspect.signature(sf.shindan_entry).parameters)
        self.assertEqual(params, ["request"])


if __name__ == "__main__":
    unittest.main()
