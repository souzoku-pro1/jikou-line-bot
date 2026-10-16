"""HOUKI-SHOKUMU-LINK-1（QUEUE-2026-10-13-D D-6）: App 40 → 職務上請求の請求案（提案封筒）の写像・起票・flag OFF

固定する仕様:
- 写像: hub/heir_envelope._unit_for_case(APP_HOUKI) == "相続放棄"（既存の App 21=時効援用・App 26=相続一般は不変・未知は例外）。
- 起票: 受任系 status（受任／書類収集中／申述書作成）で被相続人の住民票除票の提案封筒を App 30 に 1 件
  （ユニット種別=相続放棄・案件アプリID=App 40・発送ステータス=要確認・実行済み=no・チャネル固有データ=houki_shokumu_plan）。
  同一冪等キーの open 封筒があれば already_filed（作成しない）。住所解決不能は input_required で起票。被相続人氏名なしは起票しない。
- flag OFF（既定）: process_record は disabled（kintone I/O ゼロ）・受け口は skip=disabled。
- 受け口: token 未設定 404・不一致 403・app 不一致 skip。sink 違反ゼロ・allowlist 追加ゼロ。
"""

import asyncio
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_HOUKI": "40", "TOKEN_HOUKI": "t40",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from hub import heir_envelope  # noqa: E402
from hub import houki_shokumu_link as L  # noqa: E402
from hub import kintone  # noqa: E402
from hub import shokumu_plan  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
TOKEN = "houki-secret-token"


def _run(coro):
    return asyncio.run(coro)


def _rec(**fields):
    out = {k: {"value": v} for k, v in fields.items()}
    out.setdefault("$id", {"value": "40"})
    out.setdefault("$revision", {"value": "2"})
    return out


class TestMapping(unittest.TestCase):
    def test_unit_for_case_maps_app40_and_keeps_existing(self):
        with patch.dict(os.environ, {"KINTONE_APP_ID": "21", "SOUZOKU_KINTONE_APP_ID": "26", "APP_HOUKI": "40"}):
            self.assertEqual(heir_envelope._unit_for_case("40"), "相続放棄")
            self.assertEqual(heir_envelope._unit_for_case("26"), "相続一般")
            self.assertEqual(heir_envelope._unit_for_case("21"), "時効援用")
            with self.assertRaises(heir_envelope.EnvelopePolicyError):
                heir_envelope._unit_for_case("99")
        with patch.dict(os.environ, {"APP_HOUKI": ""}):
            with self.assertRaises(heir_envelope.EnvelopePolicyError):
                heir_envelope._unit_for_case("40")
        self.assertEqual(L.UNIT, "相続放棄")
        self.assertEqual(L.ENGAGED_STATUSES, frozenset({"受任", "書類収集中", "申述書作成"}))


class TestFiling(unittest.TestCase):
    def setUp(self):
        self.created: list = []
        self.search = AsyncMock(return_value=[])

        async def _create(app, fields):
            self.created.append((app, fields))
            return str(300 + len(self.created))
        self.create = AsyncMock(side_effect=_create)
        self.app31 = AsyncMock(return_value={"municipality": "さいたま市大宮区", "app31_record_id": "7", "有効": "yes", "fallback": ""})
        self._p = [patch.object(kintone, "search_records", self.search), patch.object(kintone, "create_record", self.create),
                   patch.object(shokumu_plan, "resolve_app31", self.app31),
                   patch.dict(os.environ, {"SHOKUMU_PLAN_ENABLED": "1", "APP_HOUKI": "40", "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30"})]
        for p in self._p:
            p.start()

    def tearDown(self):
        for p in reversed(self._p):
            p.stop()

    def test_flag_off_is_noop(self):
        with patch.dict(os.environ, {"SHOKUMU_PLAN_ENABLED": ""}):
            res = _run(L.process_record(_rec(status="受任", 被相続人氏名="山田 太郎", 被相続人最後の住所="埼玉県さいたま市大宮区1-1")))
            self.assertEqual(res["status"], L.RESULT_DISABLED)
            self.assertEqual(_run(L.process_record_id("40"))["status"], L.RESULT_DISABLED)
        self.search.assert_not_called()
        self.create.assert_not_called()
        self.app31.assert_not_called()

    def test_not_engaged_or_missing_decedent_does_not_file(self):
        self.assertEqual(_run(L.process_record(_rec(status="問い合わせ", 被相続人氏名="山田 太郎")))["status"], L.RESULT_NOT_ENGAGED)
        res = _run(L.process_record(_rec(status="受任", 被相続人氏名="")))
        self.assertEqual(res["status"], L.RESULT_PROBLEMS)
        self.assertIn("被相続人氏名が未入力です", res["problems"])
        self.create.assert_not_called()

    def test_files_once_and_is_idempotent(self):
        rec = _rec(status="受任", 被相続人氏名="山田 太郎", 被相続人最後の住所="埼玉県さいたま市大宮区1-1")
        res = _run(L.process_record(rec))
        self.assertEqual((res["status"], res["envelope_id"], res["record_id"]), (L.RESULT_FILED, "301", "40"))
        app, fields = self.created[0]
        self.assertEqual(app, shokumu_plan.APP_SHIPPING)
        self.assertEqual((fields["ユニット種別"], fields["案件アプリID"], fields["案件レコードID"], fields["発送ステータス"], fields["実行済み"]),
                         ("相続放棄", "40", "40", shokumu_plan.STATUS_PENDING, "no"))
        self.assertNotIn("山田", fields["件名"])
        import json
        detail = json.loads(fields["チャネル固有データ"])[L.TOP_KEY]
        self.assertEqual((detail["line_type"], detail["request_type"], detail["municipality"], detail["status"]),
                         ("joh_removed", "住民票の除票", "さいたま市大宮区", "propose"))
        self.assertTrue(detail["冪等キー"].startswith("houki_shokumu_plan:40:"))
        self.assertNotIn("shokumu_plan", json.loads(fields["チャネル固有データ"]).keys() - {L.TOP_KEY})
        # 同一キーの open 封筒がある → already_filed（作成しない）
        self.search.return_value = [{"$id": {"value": "301"}, "チャネル固有データ": {"value": fields["チャネル固有データ"]},
                                     "発送ステータス": {"value": shokumu_plan.STATUS_PENDING}, "実行済み": {"value": "no"}}]
        res2 = _run(L.process_record(rec))
        self.assertEqual((res2["status"], res2["envelope_id"]), (L.RESULT_ALREADY, "301"))
        self.assertEqual(len(self.created), 1)
        # terminal（実行済み=yes）だけなら新規起票
        self.search.return_value[0]["実行済み"] = {"value": "yes"}
        self.assertEqual(_run(L.process_record(rec))["status"], L.RESULT_FILED)
        self.assertEqual(len(self.created), 2)

    def test_unresolved_address_is_input_required(self):
        self.app31.return_value = {"municipality": shokumu_plan.INPUT_REQUIRED, "app31_record_id": "", "有効": "", "fallback": ""}
        res = _run(L.process_record(_rec(status="書類収集中", 被相続人氏名="山田 太郎", 被相続人最後の住所="不明")))
        self.assertEqual(res["status"], L.RESULT_FILED)
        import json
        detail = json.loads(self.created[0][1]["チャネル固有データ"])[L.TOP_KEY]
        self.assertEqual(detail["status"], "input_required")
        res2 = _run(L.process_record(_rec(status="申述書作成", 被相続人氏名="山田 太郎")))
        self.assertEqual(res2["status"], L.RESULT_FILED)
        self.assertEqual(self.app31.await_count, 1)                      # 住所なしは App 31 を引かない

    def test_validate_detail_closed_set_and_failure_is_reported(self):
        detail, _ = _run(L.build_detail(_rec(status="受任", 被相続人氏名="山田 太郎", 被相続人最後の住所="x")))
        L.validate_detail(detail)
        for bad in (dict(detail, unit="相続一般"), dict(detail, status="done"), dict(detail, 冪等キー="x"),
                    {k: v for k, v in detail.items() if k != "plan_hash"},
                    dict(detail, municipality=shokumu_plan.INPUT_REQUIRED, status="propose")):
            with self.assertRaises(L.HoukiPlanError):
                L.validate_detail(bad)
        self.create.side_effect = RuntimeError("boom")
        res = _run(L.process_record(_rec(status="受任", 被相続人氏名="山田 太郎", 被相続人最後の住所="x")))
        self.assertEqual(res["status"], L.RESULT_FAILED)


class TestWebhook(unittest.TestCase):
    def test_auth_and_dispatch(self):
        with patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": ""}):
            self.assertEqual(_client.post(f"/souzoku-houki/shokumu-link/{TOKEN}", json={"record_id": "40"}).status_code, 404)
        with patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": TOKEN}):
            self.assertEqual(_client.post("/souzoku-houki/shokumu-link/wrong", json={"record_id": "40"}).status_code, 403)
            r = _client.post(f"/souzoku-houki/shokumu-link/{TOKEN}", json={"app": {"id": "99"}, "record": {"$id": {"value": "40"}}})
            self.assertEqual(r.json().get("skip"), "app_mismatch")
            r = _client.post(f"/souzoku-houki/shokumu-link/{TOKEN}", json={"record_id": "40"})
            self.assertEqual(r.json().get("skip"), "disabled")            # flag OFF は I/O ゼロ
            with patch.object(L, "process_record_id", AsyncMock(return_value={"status": L.RESULT_FILED, "envelope_id": "301"})):
                r = _client.post(f"/souzoku-houki/shokumu-link/{TOKEN}", json={"record": {"$id": {"value": "40"}}})
                self.assertEqual((r.status_code, r.json()["status"], r.json()["envelope_id"]), (200, "filed", "301"))
            with patch.object(L, "process_record_id", AsyncMock(return_value={"status": L.RESULT_FAILED})):
                self.assertEqual(_client.post(f"/souzoku-houki/shokumu-link/{TOKEN}", json={"record_id": "40"}).status_code, 500)
            self.assertEqual(_client.post(f"/souzoku-houki/shokumu-link/{TOKEN}", json={"record_id": "x"}).json().get("skip"), "no_record_id")


class TestPins(unittest.TestCase):
    def test_sink_allowlist_and_wiring(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/houki_shokumu_link.py", "houki_shokumu_webhook.py", "hub/heir_envelope.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        self.assertNotIn("houki_shokumu", allow)
        msrc = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        self.assertIn("app.include_router(houki_shokumu_router)", "\n".join(msrc[-6:]))
        self.assertEqual(shokumu_plan.PLAN_UNIT, "相続放棄")


if __name__ == "__main__":
    unittest.main()
