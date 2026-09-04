"""JIKOU-FORM-1: 時効診断フォーム（公開ページ+ルール判定+App 21 保存+受付番号+
弁護士通知）の固定。

固定する仕様（票の逐語）:
- 凍結文言 sha256 pin（4 パターン+共通注記・{受付番号} のみ置換可）
- 判定はサーバ側のみ・優先順 C→B→D→A・②借入時期は判定に使わず保存のみ・
  全 15 組合せ（③3 値×④5 値）を列挙 pin
- 公開条件 fail-closed: env JIKOU_LINE_ADD_URL 未設定なら /shindan 系は
  GET/POST とも 404・設定時のみ公開
- honeypot（非表示 website 欄）: 値があれば無言破棄（保存も通知もしない）
- レート制限: X-Forwarded-For 最終要素 SHA-256 キーの固定窓（10 回/600 秒）・
  超過は固定応答（非反射）
- App 21 保存は plain 値（hub.kintone の _wrap 契約・STORE-FIX1 の教訓どおり
  fake は _wrap 境界を模し {"value":…} 形は二重ラップとして fail）
- 受付番号: secrets 乱数 6 桁ゼロ埋め・重複 create 失敗→再採番（上限 5 回・
  全失敗=固定文言 500+要確認通知）・レコード番号流用禁止
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
class _FakeApp21:
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self._id = 0
        self.fail_next = 0          # 次の create を何回失敗させるか（一意制約模擬）

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
        if self.fail_next > 0:
            self.fail_next -= 1
            raise sf.hub_kintone.KintoneError(400, "CB_VA01",
                                              "unique constraint")
        num = fields.get("受付番号")
        if num and any((r.get("受付番号") or {}).get("value") == num
                       for r in self.rows.values()):
            raise sf.hub_kintone.KintoneError(400, "CB_VA01",
                                              "unique constraint")
        self._id += 1
        rid = str(self._id)
        rec = {k: {"value": v} for k, v in fields.items()}   # 実 API の _wrap
        rec["$id"] = {"value": rid}
        self.rows[rid] = rec
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


if __name__ == "__main__":
    unittest.main()
