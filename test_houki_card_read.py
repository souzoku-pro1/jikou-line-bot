"""HOUKI-CARD-READ: 相談カード（紙）のスキャン読取→App 40 転記。

固定する仕様:
- 受け口 POST /souzoku-houki/card/{token} は申述書受け口と同じ token ゲート
  （未設定/同値=404・不一致=403）・app.id 完全一致・record id・状態ゲート
  （読取依頼 かつ 相談カード 非空）・claim CAS（敗者 0 作用）
- 状態遷移 読取依頼→読取中→読取済/要確認（成功確定前の離脱は必ず 要確認）
- 転記は confidence=high かつ検証通過の値のみ・空欄のみ・既存関数経由
  （apply_hearing_fields / append_creditors）・書かない欄の閉集合
- tool 出力は閉集合スキーマ（キー集合の完全一致・逸脱=ai_failed）
- 通知は欄コードのみ（カードの値は載せない）・kind 2 種を登録
"""

import asyncio
import hashlib
import os
import unittest
from unittest.mock import AsyncMock, patch

from test_image_analysis import _ENV, _FakeStore, _tool_response  # noqa: F401,E402

for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)
os.environ.setdefault("HOUKI_WEBHOOK_TOKEN", "houki-hook")

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from hub import houki_card as hc  # noqa: E402
from hub import houki_card_read as r  # noqa: E402
from hub import houki_case_store as store  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import notify as hub_notify  # noqa: E402

_client = TestClient(main.app)
_URL = "/souzoku-houki/card/houki-hook"
UID = "U_card_read"
JPEG = b"\xff\xd8\xff\xe0" + b"J" * 32
PNG = b"\x89PNG\r\n\x1a\n" + b"P" * 32
HEIC = b"\x00\x00\x00\x18ftypheic" + b"H" * 32
PROMPT_SHA256 = "bfd462383c45689b01434f5ca3383d808012a7a8add13015b2b41e875feac76e"

# カード上の「値」（通知・ログに出てはいけない語）
V_NAME = "山田太郎"
V_KANA = "ヤマダタロウ"
V_ADDR = "長野県木曽郡上松町本町通り4-43"
V_ME = "山田花子"
V_PHONE = "090-1234-5678"
V_MAIL = "hanako@example.com"
V_CRED = "アコム"
SECRETS = (V_NAME, V_KANA, V_ADDR, V_ME, V_PHONE, V_MAIL, V_CRED)


def _run(coro):
    return asyncio.run(coro)


def _e(value=None, confidence="high"):
    return {"value": value, "confidence": confidence}


def _report(**over):
    """全 27 キー（1〜26 + 4_other）を持つ空カード（null/high）を基に上書き。"""
    items = {str(it.number): _e() for it in hc.CARD_ITEMS}
    items["12"] = _e([])
    items["4_other"] = _e()
    version = over.pop("version", "v1")
    legible = over.pop("legible", True)
    for k, v in over.items():
        items[k.lstrip("_")] = v
    return {"version": version, "legible": legible, "items": items}


def _filled(**over):
    """主要欄が high で埋まったカード。"""
    base = dict(
        _1=_e({"name": V_NAME, "kana": V_KANA}), _2=_e(V_ADDR), _3=_e("不明"),
        _4=_e("子"), _5=_e("子"), _6=_e("2024-05-01"), _7=_e("2024-05-03"),
        _8=_e("2024-05-03"), _10=_e("親族からの連絡"), _11=_e("消費者金融の借入"),
        _12=_e([{"name": V_CRED, "contact": "東京都", "court_document": "あり"},
                {"name": None, "contact": None, "court_document": "不明"}]),
        _13=_e({"cash_deposit": "少額", "real_estate": None, "securities": None}),
        _14=_e("なし"), _15=_e("あり"), _16=_e("母と姉"), _19=_e("あり"), _20=_e("なし"),
        _21=_e({"name": V_ME, "kana": "ヤマダハナコ"}), _22=_e(V_ADDR),
        _23=_e("1976-03-13"), _24=_e(V_PHONE), _25=_e(V_MAIL), _26=_e("本人"))
    base.update(over)
    return _report(**base)


class _FakeApp40(_FakeStore):
    def seed_card(self, status=r.STATUS_WORKING, keys=("c1",), contents=None,
                  **fields) -> str:
        base = {"LINEユーザーID": UID, "response_mode": "自動", "status": "問い合わせ"}
        for code in store.HEARING_WRITABLE_FIELDS:
            base.setdefault(code, "")
        for code in ("死亡日", "起算日_確定", "相続の開始を知った日"):
            base.setdefault(code, "")
        base[r.FIELD_STATUS] = status
        base.update(fields)
        rid = super().seed_case(base, [], None)
        rec = self.cases[rid]
        rec[r.FIELD_CARD] = {"value": [
            {"fileKey": k, "name": f"{k}.jpg", "size": "1", "contentType": "image/jpeg"}
            for k in keys]}
        rec["債権者一覧"] = {"value": []}
        for k in keys:
            self.files[k] = (contents or {}).get(k, JPEG)
        return rid

    def creditors(self, rid):
        return [row["value"]["債権者名"]["value"]
                for row in self.cases[str(rid)]["債権者一覧"]["value"]]

    def status(self, rid):
        return self.field(rid, r.FIELD_STATUS)


class _Base(unittest.TestCase):
    def setUp(self):
        self.store = _FakeApp40()
        self.admin = AsyncMock(return_value=True)
        self.ai = AsyncMock(return_value=_tool_response(_filled(), name="read_card"))
        for p in (patch.object(hub_kintone, "search_records", self.store.search_records),
                  patch.object(hub_kintone, "create_record", self.store.create_record),
                  patch.object(hub_kintone, "get_record", self.store.get_record),
                  patch.object(hub_kintone, "update_record", self.store.update_record),
                  patch.object(hub_kintone, "download_file", self.store.download_file),
                  patch.object(r, "create_message_with_fallback", self.ai),
                  patch.object(hub_notify, "notify_admin_line", self.admin)):
            p.start()
            self.addCleanup(p.stop)

    def set_ai(self, report):
        self.ai.return_value = _tool_response(report, name="read_card")

    def read(self, rid):
        rec = _run(self.store.get_record(None, rid))
        return _run(r.run_card_read(rec))

    def notices(self):
        return [(c.kwargs["throttle_key"].split(":", 1)[0], c.args[0])
                for c in self.admin.await_args_list]

    def kinds(self):
        return [k for k, _t in self.notices()]

    def assert_no_values(self, text):
        for s in SECRETS:
            self.assertNotIn(s, text)


# ── 1. 受け口ゲート ─────────────────────────────────────────────────────────────
class TestWebhookGates(_Base):
    def setUp(self):
        super().setUp()
        self.runner = AsyncMock(return_value="done")
        p = patch.object(r, "run_card_read", self.runner)
        p.start()
        self.addCleanup(p.stop)

    def post(self, url=_URL, rid="1", app="40", body=None):
        if body is None:
            body = {"app": {"id": app}, "record": {"$id": {"value": rid}}}
        return _client.post(url, json=body)

    def test_token_unset_and_misconfig_404(self):
        with patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": ""}):
            self.assertEqual(self.post().status_code, 404)
        for env in ("DOCUMENT_WEBHOOK_SECRET", "KINTONE_WEBHOOK_TOKEN"):
            with self.subTest(env=env), \
                    patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": os.environ[env]}):
                resp = self.post(f"/souzoku-houki/card/{os.environ[env]}")
                self.assertEqual(resp.status_code, 404)
        self.assertEqual(self.store.cases, {})
        self.runner.assert_not_awaited()

    def test_wrong_secret_403_and_bad_json_400(self):
        self.assertEqual(self.post("/souzoku-houki/card/wrong").status_code, 403)
        resp = _client.post(_URL, content=b"nope",
                            headers={"content-type": "application/json"})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(self.post(body=[1]).status_code, 400)
        self.runner.assert_not_awaited()

    def test_app_id_must_match_exactly(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        for body in ({"record_id": rid},                                  # app 不在
                     {"app": {"id": "21"}, "record": {"$id": {"value": rid}}},
                     {"app": {"id": "400"}, "record": {"$id": {"value": rid}}}):
            with self.subTest(body=body):
                resp = self.post(body=body)
                self.assertEqual(resp.json().get("skip"), "app_mismatch")
        self.assertEqual(self.post(body={"app": {"id": "40"}}).json().get("skip"),
                         "no_record_id")
        self.assertEqual(self.store.status(rid), r.STATUS_REQUESTED)
        self.runner.assert_not_awaited()

    def test_state_gate_requested_and_card_present(self):
        for status in (r.STATUS_UNREAD, r.STATUS_WORKING, r.STATUS_DONE, r.STATUS_REVIEW):
            with self.subTest(status=status):
                rid = self.store.seed_card(status=status)
                self.assertEqual(self.post(rid=rid).json().get("skip"),
                                 "status_not_requested")
                self.assertEqual(self.store.status(rid), status)
        rid = self.store.seed_card(status=r.STATUS_REQUESTED, keys=())
        self.assertEqual(self.post(rid=rid).json().get("skip"), "no_card")
        self.assertEqual(self.store.status(rid), r.STATUS_REQUESTED)
        self.assertEqual(self.post(rid="999").status_code, 500)
        self.runner.assert_not_awaited()

    def test_claim_cas_then_background_read(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        resp = self.post(rid=rid)
        self.assertEqual(resp.json(), {"ok": True, "record_id": rid, "claimed": True})
        self.assertEqual(self.store.status(rid), r.STATUS_WORKING)
        self.runner.assert_awaited_once()
        passed = self.runner.await_args.args[0]
        self.assertEqual(passed["$revision"]["value"], "2")      # claim 後の正本
        self.assertEqual(passed[r.FIELD_STATUS]["value"], r.STATUS_WORKING)
        # 二重配信の敗者（CAS 409）は 0 作用
        rid2 = self.store.seed_card(status=r.STATUS_REQUESTED)
        self.store.conflicts_left = 1
        self.assertEqual(self.post(rid=rid2).json().get("skip"), "cas_lost")
        self.assertEqual(self.store.status(rid2), r.STATUS_REQUESTED)
        self.assertEqual(self.runner.await_count, 1)


# ── 2. 読取→転記（正常系） ──────────────────────────────────────────────────────
class TestHappyPath(_Base):
    def test_writes_high_valid_fields_only_and_marks_done(self):
        rid = self.store.seed_card()
        self.assertEqual(self.read(rid), "done")
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        f = self.store.field
        self.assertEqual(f(rid, "被相続人氏名"), V_NAME)
        self.assertEqual(f(rid, "被相続人ふりがな"), V_KANA)
        self.assertEqual(f(rid, "続柄"), "子")
        self.assertEqual(f(rid, "続柄その他"), "")
        self.assertEqual(f(rid, "死亡日_申告"), "2024-05-01")
        self.assertEqual(f(rid, "死亡を知った日_申告"), "2024-05-03")
        self.assertEqual(f(rid, "財産_現金預貯金"), "少額")
        self.assertEqual(f(rid, "財産_不動産"), "")
        self.assertEqual(f(rid, "顧客名"), V_ME)
        self.assertEqual(f(rid, "電話番号"), V_PHONE)
        self.assertEqual(f(rid, "メールアドレス"), V_MAIL)
        self.assertEqual(f(rid, "本人区分"), "本人")
        self.assertEqual(self.store.creditors(rid), [V_CRED])
        # 書かない欄
        for code in ("未成年後見関与", "死亡日", "起算日_確定", "相続の開始を知った日"):
            self.assertEqual(f(rid, code), "")
        self.assertEqual(f(rid, "response_mode"), "自動")
        self.assertEqual(f(rid, "status"), "問い合わせ")
        # AI 呼出の形（tool 強制・凍結 prompt・画像 1 枚）
        kw = self.ai.await_args.kwargs
        self.assertEqual(kw["tool_choice"], {"type": "tool", "name": "read_card"})
        self.assertEqual(kw["system"], r.SYSTEM_PROMPT)
        self.assertEqual(kw["messages"][0]["content"][0]["type"], "image")
        # 通知: kind=houki_card_read・欄コードのみ・値なし
        self.assertEqual(self.kinds(), ["houki_card_read"])
        text = self.notices()[0][1]
        self.assert_no_values(text)
        self.assertIn(f"案件レコードNo.{rid}（結果: 読取済）", text)
        self.assertIn("・転記した欄: ", text)
        self.assertIn("被相続人氏名", text)
        self.assertIn("・債権者: 転記 1 件（裁判所書類あり 1 件）", text)
        self.assertIn("・戸籍謄本・住民票: なし", text)
        self.assertEqual(self.admin.await_args.kwargs["throttle_on_success_only"], True)

    def test_empty_only_preexisting_kept_and_apply_called_once_with_existing(self):
        rid = self.store.seed_card(被相続人氏名="既存太郎", 電話番号="0300000000")
        with patch.object(store, "apply_hearing_fields",
                          wraps=store.apply_hearing_fields) as spy:
            self.assertEqual(self.read(rid), "done")
        spy.assert_awaited_once()
        self.assertIs(spy.await_args.args[2] is not None, True)
        self.assertNotIn("被相続人氏名", spy.await_args.args[1])
        self.assertEqual(self.store.field(rid, "被相続人氏名"), "既存太郎")
        self.assertEqual(self.store.field(rid, "電話番号"), "0300000000")
        text = self.notices()[0][1]
        self.assertIn("・既に値があり転記しなかった欄: ", text)
        self.assertIn("被相続人氏名, 電話番号", text)
        self.assert_no_values(text)

    def test_tsuzukigara_sonota_only_when_sonota(self):
        rid = self.store.seed_card()
        self.set_ai(_filled(_4=_e("その他"), _4_other=_e("内縁の妻")))
        self.read(rid)
        self.assertEqual(self.store.field(rid, "続柄"), "その他")
        self.assertEqual(self.store.field(rid, "続柄その他"), "内縁の妻")
        rid2 = self.store.seed_card()
        self.set_ai(_filled(_4=_e("子"), _4_other=_e("内縁の妻")))
        self.read(rid2)
        self.assertEqual(self.store.field(rid2, "続柄"), "子")
        self.assertEqual(self.store.field(rid2, "続柄その他"), "")

    def test_pdf_and_png_and_multiple_files(self):
        pdf = _tiny_pdf()
        rid = self.store.seed_card(keys=("a", "b"), contents={"a": pdf, "b": PNG})
        self.assertEqual(self.read(rid), "done")
        content = self.ai.await_args.kwargs["messages"][0]["content"]
        self.assertEqual([c["type"] for c in content], ["document", "image", "text"])
        self.assertEqual(content[1]["source"]["media_type"], "image/png")


# ── 3. 要確認（low/検証落ち/書けなかった） ────────────────────────────────────────
class TestReviewOutcomes(_Base):
    def test_low_and_medium_not_written_and_review(self):
        rid = self.store.seed_card()
        self.set_ai(_filled(_2=_e(V_ADDR, "low"), _24=_e(V_PHONE, "medium")))
        self.assertEqual(self.read(rid), "review")
        self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)
        self.assertEqual(self.store.field(rid, "被相続人最後の住所"), "")
        self.assertEqual(self.store.field(rid, "電話番号"), "")
        self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)   # 他は転記
        text = self.notices()[0][1]
        self.assertIn("（結果: 要確認）", text)
        self.assertIn("・読めなかった/自信の低い欄: ", text)
        self.assertIn("被相続人最後の住所", text)
        self.assertIn("電話番号", text)
        self.assert_no_values(text)

    def test_invalid_values_rejected_server_side(self):
        cases = {
            "_6": (_e("2024-02-30"), "死亡日_申告"),       # 実在しない
            "_7": (_e("2999-01-01"), "死亡を知った日_申告"),  # 未来
            "_23": (_e("1976/03/13"), "生年月日"),          # 形式
            "_24": (_e("090-1234-56789012"), "電話番号"),   # 長さ
            "_25": (_e("hanako.example.com"), "メールアドレス"),  # @ なし
            "_2": (_e("x" * 101), "被相続人最後の住所"),      # 文字数
            "_16": (_e("http://evil.example"), "他の相続人"),  # URL
        }
        for key, (entry, code) in cases.items():
            with self.subTest(code=code):
                self.setUp()
                rid = self.store.seed_card()
                self.set_ai(_filled(**{key: entry}))
                self.assertEqual(self.read(rid), "review")
                self.assertEqual(self.store.field(rid, code), "")
                text = self.notices()[0][1]
                self.assertIn("・検証に落ちた欄: " , text)
                self.assertIn(code, text)
                self.assert_no_values(text)

    def test_choice_outside_set_is_schema_violation(self):
        rid = self.store.seed_card()
        self.set_ai(_filled(_4=_e("長男")))
        self.assertEqual(self.read(rid), "ai_failed")
        self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)
        self.assertEqual(self.store.field(rid, "続柄"), "")
        self.assertEqual(self.store.field(rid, "被相続人氏名"), "")      # 転記 0
        self.assertEqual(self.kinds(), ["houki_card_read_failure"])

    def test_date_inconsistency_leaves_dates_unwritten_and_review(self):
        rid = self.store.seed_card()
        self.set_ai(_filled(_6=_e("2024-05-10"), _7=_e("2024-05-03")))  # 知った日<死亡日
        self.assertEqual(self.read(rid), "review")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "")
        self.assertEqual(self.store.field(rid, "死亡を知った日_申告"), "")
        self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)
        text = self.notices()[0][1]
        self.assertIn("日付の整合検証", text)
        self.assertIn("・書けなかった欄: ", text)

    def test_write_not_converged_is_review(self):
        rid = self.store.seed_card()
        self.store.conflicts_left = store._CAS_RETRIES
        self.assertEqual(self.read(rid), "review")
        self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)
        self.assertEqual(self.store.field(rid, "被相続人氏名"), "")
        self.assertIn("・書けなかった欄: ", self.notices()[0][1])

    def test_illegible_or_wrong_version_no_write(self):
        for kw, outcome in (({"legible": False}, "review"),
                            ({"version": "other"}, "version_mismatch"),
                            ({"version": "unknown"}, "version_mismatch")):
            with self.subTest(kw=kw):
                self.setUp()
                rid = self.store.seed_card()
                self.set_ai(_filled(**kw))
                self.assertEqual(self.read(rid), outcome)
                self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)
                self.assertEqual(self.store.field(rid, "被相続人氏名"), "")
                self.assertEqual(self.store.creditors(rid), [])
                self.assertEqual(len(self.kinds()), 1)


# ── 4. 失敗系（try/finally で必ず 要確認） ────────────────────────────────────────
class TestFailures(_Base):
    def test_ai_exception_and_schema_deviation(self):
        rid = self.store.seed_card()
        self.ai.side_effect = RuntimeError("boom")
        self.assertEqual(self.read(rid), "ai_failed")
        self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)
        self.assertEqual(self.kinds(), ["houki_card_read_failure"])
        self.ai.side_effect = None
        bad = _filled()
        bad["items"]["27"] = _e("x")                                    # 余分なキー
        rid2 = self.store.seed_card()
        self.set_ai(bad)
        self.assertEqual(self.read(rid2), "ai_failed")
        self.assertEqual(self.store.field(rid2, "被相続人氏名"), "")

    def test_download_failure_and_unreadable_attachments(self):
        rid = self.store.seed_card()
        self.store.download_error = hub_kintone.KintoneError(500, "x", "y")
        self.assertEqual(self.read(rid), "download_failed")
        self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)
        self.store.download_error = None
        for label, blob in (("heic", HEIC), ("unknown", b"\x00" * 40),
                            ("oversize", JPEG + b"\x00" * r.MAX_AI_IMAGE_BYTES)):
            with self.subTest(label=label):
                rid2 = self.store.seed_card(keys=("z",), contents={"z": blob})
                self.assertEqual(self.read(rid2), "unreadable_attachment")
                self.assertEqual(self.store.status(rid2), r.STATUS_REVIEW)
        self.ai.assert_not_awaited()
        self.assertTrue(all(k == "houki_card_read_failure" for k in self.kinds()))

    def test_pdf_page_limit_and_max_files(self):
        with patch.object(r, "_pdf_page_count", return_value=r.MAX_PDF_PAGES + 1):
            rid = self.store.seed_card(keys=("p",), contents={"p": _tiny_pdf()})
            self.assertEqual(self.read(rid), "unreadable_attachment")
        self.store.downloaded.clear()
        rid2 = self.store.seed_card(keys=tuple(f"k{i}" for i in range(r.MAX_FILES + 1)))
        self.assertEqual(self.read(rid2), "unreadable_attachment")      # 上限超=要確認
        self.assertEqual(self.store.downloaded, [])
        rid3 = self.store.seed_card(keys=tuple(f"m{i}" for i in range(r.MAX_FILES)))
        self.assertEqual(self.read(rid3), "done")
        self.assertEqual(len(self.store.downloaded), r.MAX_FILES)

    def test_unexpected_exception_finalizes_review(self):
        rid = self.store.seed_card()
        with patch.object(r, "extract_fields", side_effect=KeyError("x")):
            self.assertEqual(self.read(rid), "failed")
        self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)
        self.assertEqual(self.kinds(), ["houki_card_read_failure"])

    def test_finalize_failure_notifies(self):
        rid = self.store.seed_card()
        real = self.store.update_record

        async def flaky(app, record_id, fields, revision=None):
            if r.FIELD_STATUS in fields:
                raise hub_kintone.KintoneError(500, "x", "y")
            return await real(app, record_id, fields, revision)
        with patch.object(hub_kintone, "update_record", flaky):
            self.assertEqual(self.read(rid), "done")
        self.assertEqual(self.store.status(rid), r.STATUS_WORKING)      # 更新できず
        self.assertEqual(self.kinds(), ["houki_card_read_failure", "houki_card_read"])
        self.assertIn("ステータス更新（読取済）に失敗", self.notices()[0][1])


# ── 5. スキーマ・pin・sink ────────────────────────────────────────────────────────
class TestSchemaAndPins(_Base):
    def test_prompt_sha_and_tool_closed_schema(self):
        self.assertEqual(hashlib.sha256(r.SYSTEM_PROMPT.encode("utf-8")).hexdigest(),
                         PROMPT_SHA256)
        schema = r.READ_CARD_TOOL["input_schema"]
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(schema["properties"]["version"]["enum"], ["v1", "other", "unknown"])
        items = schema["properties"]["items"]
        self.assertFalse(items["additionalProperties"])
        self.assertEqual(set(items["properties"]), {str(n) for n in range(1, 27)} | {"4_other"})
        self.assertEqual(set(items["required"]), set(items["properties"]))
        for key, spec in items["properties"].items():
            self.assertFalse(spec["additionalProperties"], key)
            self.assertEqual(spec["properties"]["confidence"]["enum"],
                             ["high", "medium", "low"])
        self.assertEqual(items["properties"]["12"]["properties"]["value"]["maxItems"], 3)
        self.assertEqual(items["properties"]["20"]["properties"]["value"]["enum"],
                         ["あり", "なし", None])
        for it in hc.CARD_ITEMS:
            if it.kind == "choice":
                self.assertEqual(items["properties"][str(it.number)]["properties"]
                                 ["value"]["enum"], list(it.choices) + [None])

    def test_parse_rejects_deviations(self):
        self.assertIsNotNone(r.parse_card_report(_filled()))
        bad = []
        x = _filled(); del x["items"]["26"]; bad.append(x)
        x = _filled(); x["items"]["1"]["confidence"] = "sure"; bad.append(x)
        x = _filled(); x["items"]["12"]["value"] = [{"name": "a", "contact": None}]; bad.append(x)
        x = _filled(); x["items"]["12"]["value"] = [_e()] * 4; bad.append(x)
        x = _filled(); x["items"]["13"]["value"] = {"cash_deposit": "a"}; bad.append(x)
        x = _filled(); x["version"] = "v2"; bad.append(x)
        x = _filled(); x["extra"] = 1; bad.append(x)
        x = _filled(); x["items"]["1"]["value"] = "文字列"; bad.append(x)
        for i, b in enumerate(bad):
            with self.subTest(i=i):
                self.assertIsNone(r.parse_card_report(b))

    def test_never_write_and_notify_kinds_registered(self):
        self.assertEqual(r.NEVER_WRITE, {"未成年後見関与", "死亡日", "起算日_確定",
                                         "相続の開始を知った日", "response_mode", "status"})
        for kind in ("houki_card_read", "houki_card_read_failure"):
            with self.subTest(kind=kind), \
                    self.assertLogs(hub_notify.logger, level="INFO") as cm:
                hub_notify._log_throttled(f"{kind}:12")
                out = "\n".join(cm.output)
                self.assertIn(f"kind={kind}", out)
                self.assertNotIn("unknown_kind", out)

    def test_logs_carry_no_card_values(self):
        rid = self.store.seed_card()
        with self.assertLogs(r.logger, level="INFO") as cm:
            self.read(rid)
        self.assert_no_values("\n".join(cm.output))


def _tiny_pdf() -> bytes:
    import fitz
    d = fitz.open()
    d.new_page()
    return d.tobytes()


if __name__ == "__main__":
    unittest.main()
