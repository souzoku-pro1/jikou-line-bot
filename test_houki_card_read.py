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
- fix1 HCR-01: 読取中 の取り残し（更新日時 が STALE_MINUTES より古い）は再配送で
  CAS claim し直して再実行（新しい 読取中 は in_flight・作用 0）。claim 直後の
  正本取得失敗も finally で 要確認
- fix1 HCR-02: 終端遷移は最新の 相談カード読取 が 読取中 のときだけ（人の変更を
  検知したら作用 0・通知 1 行）
- fix2 HCRF1-01: claim 世代フェンス。reclaim で失効した旧処理は転記・終端・通知を
  行わない（in-memory・単一 worker 前提）
- fix3 HCRF2-01: フェンスは houki_case_store の CAS 再試行中（各試行の前・409 後の
  再取得の前）にも効く（fence 引数・既定 None は従来挙動）。HCRF2-02: 採番は
  プロセス単一の itertools.count・所有権は終端成功/preempted で自世代のときだけ削除
"""

import asyncio
import datetime
import hashlib
import itertools
import os
import unittest
from unittest.mock import AsyncMock, patch

from test_image_analysis import _ENV, _FakeStore, _tool_response  # noqa: F401,E402

for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)
os.environ.setdefault("HOUKI_WEBHOOK_TOKEN", "houki-hook")

import httpx  # noqa: E402
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
NOW = datetime.datetime(2026, 9, 6, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _ts(minutes_ago: int) -> str:
    return (NOW - datetime.timedelta(minutes=minutes_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")

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
        base.setdefault(r.FIELD_UPDATED, _ts(0))
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

    async def update_record(self, app, record_id, fields, revision=None):
        await super().update_record(app, record_id, fields, revision)
        self.cases[str(record_id)][r.FIELD_UPDATED] = {"value": _ts(0)}   # kintone 同様

    def status_puts(self):
        """終端（読取済/要確認）の PUT 回数（claim の 読取中 PUT は数えない）。"""
        return self.status_put_count

    def __init__(self):
        super().__init__()
        self.status_put_count = 0


class _Base(unittest.TestCase):
    def setUp(self):
        r._generations.clear()
        self.addCleanup(r._generations.clear)
        self.store = _FakeApp40()
        self.admin = AsyncMock(return_value=True)
        self.ai = AsyncMock(return_value=_tool_response(_filled(), name="read_card"))
        for p in (patch.object(hub_kintone, "search_records", self.store.search_records),
                  patch.object(hub_kintone, "create_record", self.store.create_record),
                  patch.object(hub_kintone, "get_record", self.store.get_record),
                  patch.object(hub_kintone, "update_record", self._update_record),
                  patch.object(hub_kintone, "download_file", self.store.download_file),
                  patch.object(r, "create_message_with_fallback", self.ai),
                  patch.object(r, "_now", lambda: NOW),
                  patch.object(hub_notify, "notify_admin_line", self.admin)):
            p.start()
            self.addCleanup(p.stop)

    async def _update_record(self, app, record_id, fields, revision=None):
        if fields.get(r.FIELD_STATUS) in (r.STATUS_DONE, r.STATUS_REVIEW):   # 終端 PUT のみ
            self.store.status_put_count += 1
        return await self.store.update_record(app, record_id, fields, revision)

    def set_ai(self, report):
        self.ai.return_value = _tool_response(report, name="read_card")

    def human_change(self, rid, status):
        """人が kintone 画面で 相談カード読取 を変えた（revision が進む）。"""
        rec = self.store.cases[str(rid)]
        rec[r.FIELD_STATUS] = {"value": status}
        rec["$revision"] = {"value": str(int(rec["$revision"]["value"]) + 1)}

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
        p = patch.object(r, "run_card_read_by_id", self.runner)
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
        for status in (r.STATUS_UNREAD, r.STATUS_DONE, r.STATUS_REVIEW):
            with self.subTest(status=status):
                rid = self.store.seed_card(status=status)
                self.assertEqual(self.post(rid=rid).json().get("skip"),
                                 "status_not_requested")
                self.assertEqual(self.store.status(rid), status)
        rid = self.store.seed_card(status=r.STATUS_WORKING)                # 新しい 読取中
        self.assertEqual(self.post(rid=rid).json().get("skip"), "in_flight")
        self.assertEqual(self.store.field(rid, "$revision"), "1")
        rid = self.store.seed_card(status=r.STATUS_REQUESTED, keys=())
        self.assertEqual(self.post(rid=rid).json().get("skip"), "no_card")
        self.assertEqual(self.store.status(rid), r.STATUS_REQUESTED)
        self.assertEqual(self.post(rid="999").status_code, 500)
        self.runner.assert_not_awaited()

    def test_claim_cas_then_background_read(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        resp = self.post(rid=rid)
        self.assertEqual(resp.json(), {"ok": True, "record_id": rid, "claimed": True,
                                       "reconciled": False})
        self.assertEqual(self.store.status(rid), r.STATUS_WORKING)
        self.assertEqual(self.store.field(rid, "$revision"), "2")   # claim で revision が進む
        self.runner.assert_awaited_once()
        gen = self.runner.await_args.args[2]
        self.assertEqual(self.runner.await_args.args, (rid, False, gen))  # 世代を所有
        self.assertIsInstance(gen, int)
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


# ── 6. fix1 HCR-01: 読取中 の取り残し回収（reconcile） ───────────────────────────
class TestReconcile(_Base):
    def post(self, rid):
        return _client.post(_URL, json={"app": {"id": "40"},
                                        "record": {"$id": {"value": rid}}})

    def test_hcr01_1_get_record_failure_after_claim_ends_in_review(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        real = self.store.get_record
        calls = {"n": 0}

        async def flaky(app, record_id):
            calls["n"] += 1
            if calls["n"] == 2:                       # claim 直後の正本取得だけ失敗
                raise hub_kintone.KintoneError(500, "x", "y")
            return await real(app, record_id)
        with patch.object(hub_kintone, "get_record", flaky):
            resp = self.post(rid)
        self.assertEqual(resp.json().get("claimed"), True)
        self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)   # 読取中 を残さない
        self.ai.assert_not_awaited()
        self.assertEqual(self.kinds(), ["houki_card_read_failure"])
        self.assertIn("claim 後のレコード取得に失敗", self.notices()[0][1])

    def test_hcr01_2_stale_working_is_rerun_without_double_write(self):
        rid = self.store.seed_card(status=r.STATUS_WORKING, 被相続人氏名=V_NAME,
                                   **{r.FIELD_UPDATED: _ts(r.STALE_MINUTES + 1)})
        _run(store.append_creditors(rid, _run(self.store.get_record(None, rid)), [V_CRED]))
        self.store.cases[rid][r.FIELD_UPDATED] = {"value": _ts(r.STALE_MINUTES + 1)}
        rev_before = int(self.store.field(rid, "$revision"))
        resp = self.post(rid)
        self.assertEqual(resp.json(), {"ok": True, "record_id": rid, "claimed": True,
                                       "reconciled": True})
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)          # 収束
        self.assertGreater(int(self.store.field(rid, "$revision")), rev_before)
        self.assertEqual(self.store.creditors(rid), [V_CRED])            # 二重なし
        self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)   # 上書きなし
        self.assertEqual(self.store.field(rid, "顧客名"), V_ME)          # 空欄は転記
        text = self.notices()[-1][1]
        self.assertIn("・取り残しを再実行しました", text)
        self.assertIn("・既に値があり転記しなかった欄: ", text)
        self.assert_no_values(text)
        # 要確認 に倒れる再実行にも 1 行が付く
        rid2 = self.store.seed_card(status=r.STATUS_WORKING,
                                    **{r.FIELD_UPDATED: _ts(r.STALE_MINUTES + 1)})
        self.set_ai(_filled(_2=_e(V_ADDR, "low")))
        self.post(rid2)
        self.assertEqual(self.store.status(rid2), r.STATUS_REVIEW)
        self.assertIn("・取り残しを再実行しました", self.notices()[-1][1])

    def test_hcr01_3_fresh_working_is_in_flight(self):
        for age in (0, 1, r.STALE_MINUTES):                 # ちょうど 10 分は処理中扱い
            with self.subTest(age=age):
                rid = self.store.seed_card(status=r.STATUS_WORKING,
                                           **{r.FIELD_UPDATED: _ts(age)})
                self.assertEqual(self.post(rid).json().get("skip"), "in_flight")
                self.assertEqual(self.store.field(rid, "$revision"), "1")
        rid = self.store.seed_card(status=r.STATUS_WORKING, **{r.FIELD_UPDATED: ""})
        self.assertEqual(self.post(rid).json().get("skip"), "in_flight")   # 不明=処理中扱い
        self.ai.assert_not_awaited()
        self.assertEqual(self.kinds(), [])
        self.assertEqual(r.STALE_MINUTES, 10)
        self.assertEqual(r.FIELD_UPDATED, "更新日時")

    def test_hcr01_4_concurrent_reconcile_runs_once(self):
        rid = self.store.seed_card(status=r.STATUS_WORKING,
                                   **{r.FIELD_UPDATED: _ts(r.STALE_MINUTES + 1)})
        with patch.object(r, "run_card_read_by_id", AsyncMock(return_value="done")) as run:
            self.assertEqual(self.post(rid).json().get("reconciled"), True)
            self.assertEqual(self.post(rid).json().get("skip"), "in_flight")  # 更新日時 が進んだ
            self.assertEqual(run.await_count, 1)
        rid2 = self.store.seed_card(status=r.STATUS_WORKING,
                                    **{r.FIELD_UPDATED: _ts(r.STALE_MINUTES + 1)})
        self.store.conflicts_left = 1                                       # 同時配送の敗者
        with patch.object(r, "run_card_read_by_id", AsyncMock(return_value="done")) as run:
            self.assertEqual(self.post(rid2).json().get("skip"), "cas_lost")
            self.assertEqual(run.await_count, 0)


# ── 7. fix1 HCR-02: 終端遷移は最新が 読取中 のときだけ ──────────────────────────
class TestFinalizePreempted(_Base):
    def test_hcr02_1_human_change_during_ai_keeps_it(self):
        rid = self.store.seed_card()

        async def ai_then_human(*a, **k):
            self.human_change(rid, r.STATUS_REVIEW)
            return _tool_response(_filled(), name="read_card")
        self.ai.side_effect = ai_then_human
        self.read(rid)
        self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)
        self.assertEqual(self.store.status_puts(), 0)                     # PUT 0
        self.assertEqual(self.notices(), [("houki_card_read",
                                           f"【相談カード読取】案件レコードNo.{rid}: 担当者の"
                                           "変更を検知したため終端遷移を行いませんでした。")])
        self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)    # 転記は独立に安全

    def test_hcr02_2_interleave_after_refetch_is_zero_effect(self):
        rid = self.store.seed_card()
        real = self.store.update_record
        state = {"fired": False}

        async def interleave(app, record_id, fields, revision=None):
            if r.FIELD_STATUS in fields and not state["fired"]:
                state["fired"] = True
                self.human_change(rid, r.STATUS_UNREAD)      # 再取得後・PUT 直前に人が変更
                raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "c")
            if r.FIELD_STATUS in fields:
                self.store.status_put_count += 1
            return await real(app, record_id, fields, revision)
        with patch.object(hub_kintone, "update_record", interleave):
            self.assertEqual(self.read(rid), "done")
        self.assertEqual(self.store.status(rid), r.STATUS_UNREAD)
        self.assertEqual(self.store.status_puts(), 0)
        self.assertEqual(self.kinds(), ["houki_card_read"])
        self.assertIn("担当者の変更を検知したため終端遷移を行いませんでした",
                      self.notices()[0][1])

    def test_hcr02_3_normal_path_still_finalizes(self):
        rid = self.store.seed_card()
        self.assertEqual(self.read(rid), "done")
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        self.assertEqual(self.store.status_puts(), 1)
        self.assertEqual(self.kinds(), ["houki_card_read"])

    def test_hcr02_4_reverted_to_requested_then_redelivery_no_double_write(self):
        rid = self.store.seed_card()

        async def ai_then_revert(*a, **k):
            self.human_change(rid, r.STATUS_REQUESTED)
            return _tool_response(_filled(), name="read_card")
        self.ai.side_effect = ai_then_revert
        self.read(rid)
        self.assertEqual(self.store.status(rid), r.STATUS_REQUESTED)
        self.assertEqual(self.store.creditors(rid), [V_CRED])
        self.ai.side_effect = None
        resp = _client.post(_URL, json={"app": {"id": "40"},
                                        "record": {"$id": {"value": rid}}})
        self.assertEqual(resp.json().get("claimed"), True)
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        self.assertEqual(self.store.creditors(rid), [V_CRED])              # 二重なし
        self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)
        self.assertIn("・既に値があり転記しなかった欄: ", self.notices()[-1][1])


# ── 8. fix2 HCRF1-01: claim 世代フェンス（実経路の interleave） ───────────────────
class TestGenerationFence(_Base):
    """A（旧処理）を AI 待ち等で停止させ、その間に B が stale reclaim → A 復帰。
    HTTP は httpx.ASGITransport で同一イベントループ上を通す（BackgroundTasks 込み）。"""

    def _body(self, rid):
        return {"app": {"id": "40"}, "record": {"$id": {"value": rid}}}

    async def _post(self, client, rid):
        resp = await client.post(_URL, json=self._body(rid))
        return resp.json()

    def _make_stale(self, rid):
        self.store.cases[rid][r.FIELD_UPDATED] = {"value": _ts(r.STALE_MINUTES + 1)}

    async def _spin(self, ready: asyncio.Event):
        await asyncio.wait_for(ready.wait(), timeout=5)

    def test_fence_1_old_process_resuming_after_reclaim_has_no_effect(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        gate, at_gate = asyncio.Event(), asyncio.Event()
        calls = {"n": 0}

        async def ai(*a, **k):
            calls["n"] += 1
            if calls["n"] == 1:                       # A だけ AI 待ちで停止
                at_gate.set()
                await gate.wait()
            return _tool_response(_filled(), name="read_card")
        self.ai.side_effect = ai

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                         base_url="http://t") as client:
                task_a = asyncio.create_task(self._post(client, rid))
                await self._spin(at_gate)
                gen_a = r._generations[rid]           # A が所有
                self._make_stale(rid)                 # A が取り残しに見える
                res_b = await self._post(client, rid)  # B: reclaim（世代 2）→ 完走
                self.assertEqual(res_b.get("reconciled"), True)
                self.assertEqual(self.store.status(rid), r.STATUS_DONE)
                self.assertEqual(self.store.status_puts(), 1)
                self.assertNotIn(rid, r._generations)  # B の終端で所有権が消える
                self.assertNotEqual(r._next_generation("x"), gen_a)   # 採番は使い回さない
                r._generations.pop("x")
                gate.set()                             # A 復帰
                res_a = await task_a
                self.assertEqual(res_a.get("claimed"), True)
        _run(scenario())
        # A は転記も終端も通知もしていない（B の 1 通のみ・二重なし）
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        self.assertEqual(self.store.status_puts(), 1)
        self.assertEqual(self.store.creditors(rid), [V_CRED])
        self.assertEqual(self.kinds(), ["houki_card_read"])
        self.assertIn("・取り残しを再実行しました", self.notices()[0][1])

    def test_fence_2_a_done_b_review_final_is_review_single_notice(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        gate, at_gate = asyncio.Event(), asyncio.Event()
        calls = {"n": 0}

        async def ai(*a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                at_gate.set()
                await gate.wait()
                return _tool_response(_filled(), name="read_card")            # A=done
            return _tool_response(_filled(_2=_e(V_ADDR, "low")), name="read_card")  # B=review
        self.ai.side_effect = ai

        async def scenario():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                         base_url="http://t") as client:
                task_a = asyncio.create_task(self._post(client, rid))
                await self._spin(at_gate)
                self._make_stale(rid)
                await self._post(client, rid)
                gate.set()
                await task_a
        _run(scenario())
        self.assertEqual(self.store.status(rid), r.STATUS_REVIEW)
        self.assertEqual(self.store.field(rid, "被相続人最後の住所"), "")  # A の値は書かれない
        self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)
        self.assertEqual(self.store.status_puts(), 1)
        self.assertEqual(len(self.notices()), 1)
        self.assertIn("（結果: 要確認）", self.notices()[0][1])

    def test_fence_3_reclaim_after_transcription_before_finalize(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        gate, at_gate = asyncio.Event(), asyncio.Event()
        real_get = self.store.get_record
        calls = {"n": 0}

        async def get_record(app, record_id):
            calls["n"] += 1
            if calls["n"] == 4:                       # A の「4. 実値で判定」= 転記後・終端前
                at_gate.set()
                await gate.wait()
            return await real_get(app, record_id)

        async def scenario():
            with patch.object(hub_kintone, "get_record", get_record):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                             base_url="http://t") as client:
                    task_a = asyncio.create_task(self._post(client, rid))
                    await self._spin(at_gate)
                    self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)  # A 転記済み
                    self.assertEqual(self.store.creditors(rid), [V_CRED])
                    self._make_stale(rid)
                    res_b = await self._post(client, rid)  # B: reclaim → 空欄のみ → done
                    self.assertEqual(res_b.get("reconciled"), True)
                    gate.set()
                    await task_a
        _run(scenario())
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        self.assertEqual(self.store.status_puts(), 1)                    # A の終端は作用 0
        self.assertEqual(self.store.creditors(rid), [V_CRED])           # 二重なし
        self.assertEqual(len(self.notices()), 1)
        text = self.notices()[0][1]
        self.assertIn("・取り残しを再実行しました", text)
        self.assertIn("・既に値があり転記しなかった欄: ", text)

    def test_fence_4_normal_path_unchanged(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        resp = _client.post(_URL, json=self._body(rid))
        self.assertEqual(resp.json().get("claimed"), True)
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        self.assertEqual(self.store.status_puts(), 1)
        self.assertEqual(self.kinds(), ["houki_card_read"])
        self.assertNotIn(rid, r._generations)

    def test_fence_5_late_old_process_after_terminal_has_no_effect(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        _client.post(_URL, json=self._body(rid))
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        self.assertNotIn(rid, r._generations)
        rev = self.store.field(rid, "$revision")
        self.store.cases[rid]["被相続人最後の住所"] = {"value": ""}       # 空欄を用意
        self.ai.reset_mock()
        self.admin.reset_mock()
        with self.assertLogs(r.logger, level="INFO") as cm:
            self.assertEqual(_run(r.run_card_read_by_id(rid, False, 1)), "fenced")
        self.assertIn("fenced", "\n".join(cm.output))
        self.assertEqual(self.store.field(rid, "$revision"), rev)          # 作用 0
        self.assertEqual(self.store.field(rid, "被相続人最後の住所"), "")
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        self.admin.assert_not_awaited()
        # 次の claim は旧世代と衝突しない（採番はプロセス単一・単調増加）
        self.assertGreater(r._next_generation(rid), 1)

    def test_fence_6_finalize_rechecks_generation_after_409(self):
        rid = self.store.seed_card()
        gen = r._next_generation(rid)
        real = self.store.update_record
        state = {"fired": False}

        async def interleave(app, record_id, fields, revision=None):
            if r.FIELD_STATUS in fields and not state["fired"]:
                state["fired"] = True
                r._next_generation(rid)               # PUT 直前に reclaim された
                raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "c")
            if r.FIELD_STATUS in fields:
                self.store.status_put_count += 1
            return await real(app, record_id, fields, revision)
        with patch.object(hub_kintone, "update_record", interleave):
            rec = _run(self.store.get_record(None, rid))
            self.assertEqual(_run(r.run_card_read(rec, False, gen)), "done")
        self.assertEqual(self.store.status(rid), r.STATUS_WORKING)
        self.assertEqual(self.store.status_puts(), 0)
        self.assertEqual(self.kinds(), [])


# ── 9. fix3 HCRF2-01: CAS 再試行中の所有権検査（実経路 interleave） ─────────────
class TestFenceInCasRetry(_Base):
    """A が転記直前の世代検査を通過 → A の第 1 PUT の直前に B が reclaim →
    A の PUT は 409 → A は再取得・再試行せず write 0。B は空欄のみ転記。"""

    def _body(self, rid):
        return {"app": {"id": "40"}, "record": {"$id": {"value": rid}}}

    async def _post(self, client, rid):
        return (await client.post(_URL, json=self._body(rid))).json()

    def _make_stale(self, rid):
        self.store.cases[rid][r.FIELD_UPDATED] = {"value": _ts(r.STALE_MINUTES + 1)}

    def _gate_first_field_put(self):
        """最初の非ステータス PUT（A の転記）を gate で止める update_record ラッパ。"""
        real = self.store.update_record
        gate, at_gate = asyncio.Event(), asyncio.Event()
        state = {"fired": False, "field_puts": 0}

        async def update(app, record_id, fields, revision=None):
            if r.FIELD_STATUS not in fields:
                state["field_puts"] += 1
                if not state["fired"]:
                    state["fired"] = True
                    at_gate.set()
                    await gate.wait()
            elif fields.get(r.FIELD_STATUS) in (r.STATUS_DONE, r.STATUS_REVIEW):
                self.store.status_put_count += 1
            return await real(app, record_id, fields, revision)
        return update, gate, at_gate, state

    def _ai_by_call(self, first, rest):
        calls = {"n": 0}

        async def ai(*a, **k):
            calls["n"] += 1
            return _tool_response(first if calls["n"] == 1 else rest, name="read_card")
        self.ai.side_effect = ai

    def _scenario(self, rid, update):
        gate, at_gate = update[1], update[2]

        async def run():
            with patch.object(hub_kintone, "update_record", update[0]):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                             base_url="http://t") as client:
                    task_a = asyncio.create_task(self._post(client, rid))
                    await asyncio.wait_for(at_gate.wait(), timeout=5)
                    self._make_stale(rid)
                    res_b = await self._post(client, rid)   # B: reclaim → 完走
                    gate.set()                               # A の PUT（旧 revision）
                    res_a = await task_a
                    return res_a, res_b
        return _run(run())

    def test_hcrf2_01_fields_first_put_409_then_no_retry(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        self._ai_by_call(_filled(), _filled(_2=_e(None)))     # A は住所あり・B は住所なし
        update = self._gate_first_field_put()
        res_a, res_b = self._scenario(rid, update)
        self.assertEqual(res_b.get("reconciled"), True)
        self.assertEqual(res_a.get("claimed"), True)
        self.assertEqual(self.store.field(rid, "被相続人最後の住所"), "")   # A は write 0
        self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)      # B が転記
        self.assertEqual(self.store.case_queries, 0)                        # A は再取得しない
        self.assertEqual(update[3]["field_puts"], 3)     # A 1 回（再試行なし）+ B 2 回（欄・債権者）
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        self.assertEqual(self.store.status_puts(), 1)
        self.assertEqual(self.kinds(), ["houki_card_read"])                 # B の 1 通のみ
        self.assertEqual(self.store.creditors(rid), [V_CRED])

    def test_hcrf2_01_creditors_first_put_409_then_no_retry(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        a_card = _report(_12=_e([{"name": V_CRED, "contact": None, "court_document": "なし"}]))
        b_card = _report(_12=_e([{"name": "プロミス", "contact": None, "court_document": "なし"}]))
        self._ai_by_call(a_card, b_card)
        update = self._gate_first_field_put()
        res_a, res_b = self._scenario(rid, update)
        self.assertEqual(res_b.get("reconciled"), True)
        self.assertEqual(self.store.creditors(rid), ["プロミス"])           # A の行は入らない
        self.assertEqual(update[3]["field_puts"], 2)                        # A 1 回 + B 1 回
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)
        self.assertEqual(self.kinds(), ["houki_card_read"])                 # 収束不能通知なし

    def test_hcrf2_01_a_put_first_then_b_reclaim_loses(self):
        rid = self.store.seed_card(status=r.STATUS_REQUESTED)
        real = self.store.update_record
        gate_a, at_gate_a, a_put_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
        state = {"claims": 0, "fired": False}

        async def update(app, record_id, fields, revision=None):
            if fields.get(r.FIELD_STATUS) == r.STATUS_WORKING:
                state["claims"] += 1
                if state["claims"] == 2:                 # B の reclaim: A の PUT を先行させる
                    gate_a.set()
                    await asyncio.wait_for(a_put_done.wait(), timeout=5)
            elif r.FIELD_STATUS not in fields and not state["fired"]:
                state["fired"] = True
                at_gate_a.set()
                await gate_a.wait()
                out = await real(app, record_id, fields, revision)
                a_put_done.set()
                return out
            elif fields.get(r.FIELD_STATUS) in (r.STATUS_DONE, r.STATUS_REVIEW):
                self.store.status_put_count += 1
            return await real(app, record_id, fields, revision)

        async def run():
            with patch.object(hub_kintone, "update_record", update):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                             base_url="http://t") as client:
                    task_a = asyncio.create_task(self._post(client, rid))
                    await asyncio.wait_for(at_gate_a.wait(), timeout=5)
                    self._make_stale(rid)
                    res_b = await self._post(client, rid)
                    res_a = await task_a
                    return res_a, res_b
        res_a, res_b = _run(run())
        self.assertEqual(res_b.get("skip"), "cas_lost")                   # B は敗北
        self.assertEqual(res_a.get("claimed"), True)
        self.assertEqual(self.store.status(rid), r.STATUS_DONE)           # A が終端まで所有
        self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)
        self.assertEqual(self.store.creditors(rid), [V_CRED])
        self.assertEqual(self.store.status_puts(), 1)
        self.assertEqual(self.kinds(), ["houki_card_read"])
        self.assertNotIn(rid, r._generations)

    def test_hcrf2_01_fence_none_keeps_default_behaviour(self):
        """fence=None（既存呼び出し）は 409 で再取得・再試行する従来挙動。"""
        rid = self.store.seed_card()
        rec = _run(self.store.get_record(None, rid))
        self.store.conflicts_left = 1
        _rid, problems, _c = _run(store.apply_hearing_fields(UID, {"被相続人氏名": V_NAME}, rec))
        self.assertEqual(problems, [])
        self.assertEqual(self.store.field(rid, "被相続人氏名"), V_NAME)    # 再試行して書けた
        self.assertEqual(self.store.case_queries, 1)
        rec = _run(self.store.get_record(None, rid))
        self.store.conflicts_left = 1
        self.assertEqual(_run(store.append_creditors(rid, rec, [V_CRED])), 1)
        self.assertEqual(self.store.creditors(rid), [V_CRED])
        # fence が False を返すときは初回から write 0
        rec = _run(self.store.get_record(None, rid))
        _rid, problems, _c = _run(store.apply_hearing_fields(
            UID, {"被相続人最後の住所": V_ADDR}, rec, fence=lambda: False))
        self.assertEqual(problems, ["fenced"])
        self.assertEqual(self.store.field(rid, "被相続人最後の住所"), "")
        self.assertEqual(_run(store.append_creditors(rid, rec, ["プロミス"],
                                                     fence=lambda: False)), 0)
        self.assertEqual(self.store.creditors(rid), [V_CRED])
        self.assertEqual(self.kinds(), [])                                 # 収束不能通知なし


# ── 10. fix3 HCRF2-02: 世代テーブルの肥大防止 ───────────────────────────────────
class TestGenerationTableCleanup(_Base):
    def test_single_process_counter_pinned(self):
        self.assertFalse(hasattr(r, "_generation_counter"))
        self.assertIsInstance(r._generation_seq, itertools.count)
        a = r._next_generation("1")
        b = r._next_generation("2")
        self.assertGreater(b, a)                                            # 単調増加
        self.assertEqual(r._generations, {"1": a, "2": b})

    def test_entry_removed_on_normal_terminal(self):
        rid = self.store.seed_card()
        gen = r._next_generation(rid)
        rec = _run(self.store.get_record(None, rid))
        self.assertEqual(_run(r.run_card_read(rec, False, gen)), "done")
        self.assertNotIn(rid, r._generations)

    def test_entry_removed_on_preempted_when_owner(self):
        rid = self.store.seed_card()
        gen = r._next_generation(rid)
        other = r._next_generation("other")                                 # 他レコードの所有権

        async def ai_then_human(*a, **k):
            self.human_change(rid, r.STATUS_REVIEW)
            return _tool_response(_filled(), name="read_card")
        self.ai.side_effect = ai_then_human
        rec = _run(self.store.get_record(None, rid))
        _run(r.run_card_read(rec, False, gen))
        self.assertNotIn(rid, r._generations)                              # 自世代なので削除
        self.assertEqual(r._generations, {"other": other})                 # 他は消さない
        self.assertIn("担当者の変更を検知", self.notices()[0][1])

    def test_entry_kept_on_finalize_failure(self):
        rid = self.store.seed_card()
        gen = r._next_generation(rid)
        real = self.store.update_record

        async def flaky(app, record_id, fields, revision=None):
            if r.FIELD_STATUS in fields:
                raise hub_kintone.KintoneError(500, "x", "y")
            return await real(app, record_id, fields, revision)
        rec = _run(self.store.get_record(None, rid))
        with patch.object(hub_kintone, "update_record", flaky):
            _run(r.run_card_read(rec, False, gen))
        self.assertEqual(self.store.status(rid), r.STATUS_WORKING)
        self.assertEqual(r._generations.get(rid), gen)                     # 残す（次の reclaim で上書き）
        self.assertEqual(self.kinds(), ["houki_card_read_failure", "houki_card_read"])

    def test_entry_kept_for_owner_when_old_process_is_fenced(self):
        rid = self.store.seed_card()
        old = r._next_generation(rid)
        new = r._next_generation(rid)                                       # B が reclaim 済み
        rec = _run(self.store.get_record(None, rid))
        self.assertEqual(_run(r.run_card_read(rec, False, old)), "fenced")
        self.assertEqual(r._generations.get(rid), new)                     # 他世代の所有権は消さない
        self.assertEqual(self.kinds(), [])
        self.assertEqual(self.store.status(rid), r.STATUS_WORKING)


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
