"""HOUKI-SOUFU-1: 相続放棄 受理通知書写しの発送起票のテスト。

- 起点条件（status/受領日/添付の各欠落で not triggered・本文 status gate）
- 必須値欠落で起票 0+要確認（欄名のみ・値なし）
- 対象行の絞り込み（要/未/住所）・行ごと起票と行更新の CAS（他行・他列保全）
- 冪等（再配送で 2 件目なし・既存検索で行を揃える・送付状態≠未 はスキップ）
- status 一方向遷移（受理→債権者通知 を 1 回・以降は触らない・409）
- 追加料金対象の出現順（グループ和集合・管理レコード先頭・4 番目以降・既存 yes 保全）
- ブロック未登録（App 32 / App 30 選択肢）で要確認
- 発送済書き戻し（App 40 のみ・時効側不変）
- 送付状の差し込み 15 個と和暦（昭和/平成/令和・非 ISO はそのまま）・凍結 pin・残存プレースホルダ拒否
- M4 prepare の相続放棄分岐（成果物 3 点・ラベル 1 面・時効側は従来どおり）
- 通知本文に個人情報なし・kind 登録・config 登録
"""

import asyncio
import copy
import hashlib
import io
import json
import os
import unittest
from datetime import date
from pathlib import Path
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
    "APP_HOUKI": "40", "TOKEN_HOUKI": "d",
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "d",
    "APP_ENCLOSURE": "32", "TOKEN_ENCLOSURE": "d",
    "HOUKI_LINE_CHANNEL_SECRET": "houki_secret",
    "HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "houki_token",
    "HOUKI_WEBHOOK_TOKEN": "houki-hook",
    "OFFICE_NAME": "テスト法律事務所", "OFFICE_ZIP": "332-0000",
    "OFFICE_ADDRESS": "埼玉県テスト市1-2-3", "OFFICE_TEL": "048-000-0000",
    "OFFICE_FAX": "048-000-0001", "OFFICE_ATTORNEY": "テスト　太郎",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from docx import Document  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import config  # noqa: E402
import main  # noqa: E402
from channels import soufu_annai  # noqa: E402
from hub import dispatch as hub_dispatch  # noqa: E402
from hub import houki_soufu as hs  # noqa: E402
from hub import houki_soufu_letter as L  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import notify as hub_notify  # noqa: E402

_client = TestClient(main.app)
_URL = "/souzoku-houki/soufu/houki-hook"
TODAY = date(2026, 9, 8)
REPO = Path(__file__).parent

CASE_VALUES = {
    "顧客名": "申述太郎", "住所": "埼玉県テスト市1-1", "生年月日": "1980-02-03",
    "被相続人氏名": "被相続花子", "被相続人最後の住所": "東京都テスト区2-2",
    "被相続人生年月日": "昭和25年1月1日", "死亡日": "2026-05-01",
    "管轄家庭裁判所": "さいたま家庭裁判所", "事件番号": "令和8年（家）第123号", "受理日": "2026-09-01",
}


def _row(rid, name, addr="東京都千代田区1-1", zip_="100-0001", notify="要", state="未", extra="no", ship_no=""):
    return {"id": rid, "value": {
        "債権者名": {"value": name}, "債権者郵便番号": {"value": zip_}, "債権者住所": {"value": addr},
        "通知要否": {"value": notify}, "送付状態": {"value": state},
        "追加料金対象": {"value": extra}, "送付発送管理No": {"value": ship_no}}}


def _case(rid="1", status="受理", received="2026-09-05", files=True, rows=None, group="", revision="3", **over):
    rec = {"$id": {"value": rid}, "$revision": {"value": revision},
           "status": {"value": status}, "受理通知受領日": {"value": received},
           "受理通知書": {"value": [{"fileKey": "fk-notice", "name": "受理通知書.pdf",
                                     "contentType": "application/pdf"}] if files else []},
           "被相続人グループID": {"value": group},
           "債権者一覧": {"value": rows if rows is not None else [_row("11", "甲社")]}}
    for k, v in {**CASE_VALUES, **over}.items():
        rec[k] = {"value": v}
    return rec


def _body(rid="1", status="受理", app="40"):
    return {"app": {"id": app}, "record": {"$id": {"value": rid}, "status": {"value": status}}}


class _Base(unittest.TestCase):
    """kintone / notify / App 32 / App 30 form を fake にする共通土台。"""

    def setUp(self):
        self.cases: dict[str, dict] = {}
        self.shipping: dict[str, dict] = {}
        self.updates: list[tuple] = []
        self.next_ship = 100
        self.blocks = [{"ブロックキー": {"value": "受理通知書写し"}, "対象ユニット": {"value": ["相続放棄"]}}]
        self.options = {"（未設定）", "委任契約書", "返信用封筒", "受理通知書写し"}
        self.conflict_once: set[str] = set()

        async def get_record(app, rid):
            store = self.cases if app.app_id_env == "APP_HOUKI" else self.shipping
            if str(rid) not in store:
                raise hub_kintone.KintoneError(404, "GAIA_RE01", "not found")
            return copy.deepcopy(store[str(rid)])

        async def update_record(app, rid, fields, revision=None):
            store = self.cases if app.app_id_env == "APP_HOUKI" else self.shipping
            rec = store[str(rid)]
            self.updates.append((app.app_id_env, str(rid), copy.deepcopy(fields), revision))
            if str(rid) in self.conflict_once:
                self.conflict_once.discard(str(rid))
                raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
            cur = int(rec["$revision"]["value"])
            if revision is not None and int(revision) != cur:
                raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
            for k, v in fields.items():
                rec[k] = {"value": copy.deepcopy(v)}
            rec["$revision"] = {"value": str(cur + 1)}

        async def create_record(app, fields):
            self.assertEqual(app.app_id_env, "APP_SHIPPING")
            rid = str(self.next_ship)
            self.next_ship += 1
            rec = {"$id": {"value": rid}, "$revision": {"value": "1"}}
            for k, v in fields.items():
                rec[k] = {"value": copy.deepcopy(v)}
            self.shipping[rid] = rec
            return rid

        async def search_records(app, query, fields=None):
            if app.app_id_env == "APP_ENCLOSURE":
                return copy.deepcopy(self.blocks)
            if app.app_id_env == "APP_SHIPPING":
                key = query.split('like "')[1].split('"')[0]
                return [{"$id": {"value": r["$id"]["value"]}} for r in self.shipping.values()
                        if key in (r.get("チャネル固有データ", {}).get("value") or "")]
            if app.app_id_env == "APP_HOUKI":
                gid = query.split('= "')[1].split('"')[0]
                return [copy.deepcopy(r) for r in sorted(self.cases.values(), key=lambda r: int(r["$id"]["value"]))
                        if r.get("被相続人グループID", {}).get("value") == gid]
            return []

        async def get_form_fields(app):
            return {"同封物選択": {"type": "CHECK_BOX", "options": {o: {"index": str(i)} for i, o in enumerate(self.options)}}}

        async def download_file(app, key):
            return b"%PDF-notice-" + key.encode()

        self.admin = AsyncMock(return_value=True)
        for p in (patch.object(hub_kintone, "get_record", get_record),
                  patch.object(hub_kintone, "update_record", update_record),
                  patch.object(hub_kintone, "create_record", create_record),
                  patch.object(hub_kintone, "search_records", search_records),
                  patch.object(hub_kintone, "get_form_fields", get_form_fields),
                  patch.object(hub_kintone, "download_file", download_file),
                  patch.object(hub_notify, "notify_admin_line", self.admin),
                  patch("hub.notify.notify_admin_line", self.admin)):
            p.start()
            self.addCleanup(p.stop)
        hub_notify._last_notify_at.clear()
        self.addCleanup(hub_notify._last_notify_at.clear)

    def seed(self, *recs):
        for r in recs:
            self.cases[r["$id"]["value"]] = r

    def run_soufu(self, rid="1"):
        return asyncio.run(hs.process_soufu(rid))

    def rows(self, rid="1"):
        return self.cases[rid]["債権者一覧"]["value"]

    def texts(self):
        return [c.args[0] for c in self.admin.await_args_list]

    def kinds(self):
        return [c.kwargs.get("throttle_key", "").split(":", 1)[0] for c in self.admin.await_args_list]


# ── webhook gate ─────────────────────────────────────────────────────────────
class TestWebhook(_Base):
    def post(self, url=_URL, body=None):
        return _client.post(url, json=body if body is not None else _body())

    def test_token_unset_404_and_misconfig_404(self):
        with patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": ""}):
            self.assertEqual(self.post().status_code, 404)
        with patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": os.environ["KINTONE_WEBHOOK_TOKEN"]}):
            self.assertEqual(self.post("/souzoku-houki/soufu/kintone-token").status_code, 404)

    def test_wrong_secret_403_and_bad_json_400(self):
        self.assertEqual(self.post("/souzoku-houki/soufu/wrong").status_code, 403)
        resp = _client.post(_URL, content=b"{bad", headers={"content-type": "application/json"})
        self.assertEqual(resp.status_code, 400)

    def test_app_mismatch_and_no_record_id_skip(self):
        self.assertEqual(self.post(body=_body(app="21")).json()["skip"], "app_mismatch")
        self.assertEqual(self.post(body={"app": {"id": "40"}, "record": {}}).json()["skip"], "no_record_id")

    def test_status_gate_not_triggered_for_other_statuses(self):
        for st in ("受任", "債権者通知", "完了", ""):
            with self.subTest(status=st):
                resp = self.post(body=_body(status=st))
                self.assertEqual(resp.json()["skip"], "not_triggered")
        self.assertEqual(self.shipping, {})

    def test_accepted_status_queues_and_files(self):
        self.seed(_case())
        resp = self.post()
        self.assertEqual(resp.json(), {"ok": True, "record_id": "1", "queued": True})
        self.assertEqual(len(self.shipping), 1)                     # BackgroundTask 実行済み（TestClient）
        self.assertEqual(self.rows()[0]["value"]["送付状態"]["value"], "起票済")


# ── 起点条件・必須値・対象行 ─────────────────────────────────────────────────
class TestTriggerAndTargets(_Base):
    def test_not_triggered_when_status_received_or_file_missing(self):
        for kw in ({"status": "受任"}, {"received": ""}, {"files": False}):
            with self.subTest(kw=kw):
                self.cases.clear()
                self.seed(_case(**kw))
                out = self.run_soufu()
                self.assertEqual(out["skip"], "not_triggered")
        self.assertEqual(self.shipping, {})
        self.assertEqual(self.admin.await_count, 0)

    def test_missing_required_fields_files_nothing_and_lists_field_names_only(self):
        self.seed(_case(**{"事件番号": "", "受理日": ""}))
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["review"]), (0, 1))
        self.assertEqual(self.shipping, {})
        self.assertEqual(self.updates, [])
        text = self.texts()[0]
        self.assertIn("送付状の必須欄が未入力: 事件番号・受理日", text)
        self.assertEqual(self.kinds(), ["houki_soufu_needs_review"])
        for pii in ("申述太郎", "被相続花子", "甲社", "テスト市"):
            self.assertNotIn(pii, text)

    def test_target_rows_filtering(self):
        rows = [_row("11", "甲社"), _row("12", "乙社", notify="不要"), _row("13", "丙社", notify="未確認"),
                _row("14", "丁社", state="起票済"), _row("15", "戊社", addr=""), _row("16", "己社", zip_="")]
        self.seed(_case(rows=rows))
        out = self.run_soufu()
        self.assertEqual(out["filed"], 2)                           # 甲社・己社（郵便番号空でも起票）
        self.assertEqual({r["宛先名"]["value"] for r in self.shipping.values()}, {"甲社", "己社"})
        states = {r["value"]["債権者名"]["value"]: r["value"]["送付状態"]["value"] for r in self.rows()}
        self.assertEqual(states, {"甲社": "起票済", "乙社": "未", "丙社": "未", "丁社": "起票済", "戊社": "未", "己社": "起票済"})
        review = [t for t in self.texts() if t.startswith(hs.NOTICE_HEAD_REVIEW)]
        self.assertEqual(len(review), 1)
        self.assertIn("債権者住所が未入力の行（通知要否=要）: 5 行目", review[0])
        self.assertNotIn("戊社", review[0])

    def test_no_target_rows_skips(self):
        self.seed(_case(rows=[_row("11", "甲社", state="送付済")]))
        out = self.run_soufu()
        self.assertEqual((out["skip"], out["filed"]), ("no_target_rows", 0))
        self.assertEqual(self.admin.await_count, 0)


# ── 起票内容・行更新 CAS・冪等 ────────────────────────────────────────────────
class TestFiling(_Base):
    def test_shipping_fields_and_row_update(self):
        self.seed(_case())
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["promoted"]), (1, True))
        ship = self.shipping["100"]
        expect = {"発送ステータス": "下書き", "ユニット種別": "相続放棄", "チャネル": "送付案内", "方向": "発送",
                  "案件アプリID": "40", "案件レコードID": "1", "顧客名表示用": "申述太郎",
                  "件名": "受理通知送付（甲社）", "宛先名": "甲社", "宛先郵便番号": "100-0001",
                  "宛先住所": "東京都千代田区1-1", "実行済み": "no", "同封物選択": ["受理通知書写し"]}
        for k, v in expect.items():
            self.assertEqual(ship[k]["value"], v, k)
        meta = json.loads(ship["チャネル固有データ"]["value"])
        self.assertEqual(meta, {"houki_soufu_key": "houki_soufu:1:11", "row_id": "11", "case_record_id": "1"})
        row = self.rows()[0]
        self.assertEqual((row["value"]["送付状態"]["value"], row["value"]["送付発送管理No"]["value"],
                          row["value"]["追加料金対象"]["value"]), ("起票済", "100", "no"))
        self.assertEqual(row["value"]["債権者住所"]["value"], "東京都千代田区1-1")   # 他列保全
        houki_updates = [u for u in self.updates if u[0] == "APP_HOUKI"]
        self.assertEqual([sorted(u[2]) for u in houki_updates], [["債権者一覧"], ["status"]])
        self.assertEqual(houki_updates[0][3], "3")                  # CAS: 取得時 revision

    def test_row_update_cas_conflict_refetches_once(self):
        self.seed(_case())
        self.conflict_once.add("1")
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["review"]), (1, 0))
        self.assertEqual(self.rows()[0]["value"]["送付状態"]["value"], "起票済")
        self.assertEqual(len([u for u in self.updates if u[0] == "APP_HOUKI" and "債権者一覧" in u[2]]), 2)

    def test_redelivery_does_not_create_second_record(self):
        self.seed(_case())
        self.run_soufu()
        self.assertEqual(len(self.shipping), 1)
        self.admin.reset_mock()
        out = self.run_soufu()                                       # 再配送: status は 債権者通知 → not triggered
        self.assertEqual(out["skip"], "not_triggered")
        self.assertEqual(len(self.shipping), 1)

    def test_existing_shipping_aligns_row_without_second_record(self):
        # 起票済みの App 30 があるが行更新が未反映（前回途中で落ちた想定）
        self.seed(_case())
        self.shipping["100"] = {"$id": {"value": "100"}, "$revision": {"value": "1"},
                                "チャネル固有データ": {"value": json.dumps({"houki_soufu_key": "houki_soufu:1:11"})}}
        self.next_ship = 101
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["aligned"]), (0, 1))
        self.assertEqual(len(self.shipping), 1)
        self.assertEqual(self.rows()[0]["value"]["送付発送管理No"]["value"], "100")
        self.assertIn("既存起票に揃えた行 1 件", self.texts()[-1])

    def test_uses_single_record_create_api(self):
        self.seed(_case(rows=[_row("11", "甲社"), _row("12", "乙社")]))
        with patch.object(hub_kintone, "create_records", AsyncMock()) as bulk:
            out = self.run_soufu()
        self.assertEqual(out["filed"], 2)
        bulk.assert_not_awaited()


class TestStatusTransition(_Base):
    def test_promotes_once_and_never_beyond(self):
        self.seed(_case())
        self.run_soufu()
        self.assertEqual(self.cases["1"]["status"]["value"], "債権者通知")
        for st in ("債権者通知", "完了", "受任"):
            self.cases["1"]["status"] = {"value": st}
            self.assertFalse(asyncio.run(hs.promote_status("1")))
            self.assertEqual(self.cases["1"]["status"]["value"], st)

    def test_no_promotion_when_nothing_filed(self):
        self.seed(_case(rows=[_row("11", "甲社", addr="")]))
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["promoted"]), (0, False))
        self.assertEqual(self.cases["1"]["status"]["value"], "受理")

    def test_status_cas_conflict_gives_up_without_error(self):
        self.seed(_case())
        self.conflict_once.add("1")                                 # 行更新の初回を 409 に
        out = self.run_soufu()
        self.assertTrue(out["promoted"])                             # 行更新の再取得後に遷移


class TestExtraFee(_Base):
    def test_fourth_distinct_creditor_in_group_order_is_extra(self):
        # 管理レコード 2（グループ ID=2）先頭 → 3 の順。正規化和集合の出現順で 4 番目以降=yes
        self.seed(_case(rid="2", group="2", rows=[_row("21", "Ａ社"), _row("22", "B社"), _row("23", "Ｂ社")]),
                  _case(rid="3", group="2", rows=[_row("31", "C社"), _row("32", "A社"), _row("33", "D社")]))
        out = self.run_soufu("3")
        self.assertEqual(out["filed"], 3)
        extra = {r["value"]["債権者名"]["value"]: r["value"]["追加料金対象"]["value"] for r in self.rows("3")}
        self.assertEqual(extra, {"C社": "no", "A社": "no", "D社": "yes"})   # 和集合順 A,B,C,D → D が 4 番目
        self.assertEqual(hs.extra_fee_names(hs.order_group(self.cases["3"], list(self.cases.values()))), {"D社"})

    def test_existing_yes_is_preserved(self):
        self.seed(_case(rows=[_row("11", "甲社", extra="yes")]))
        self.run_soufu()
        self.assertEqual(self.rows()[0]["value"]["追加料金対象"]["value"], "yes")

    def test_order_group_manager_first(self):
        recs = [_case(rid="5", group="7"), _case(rid="7", group="7"), _case(rid="6", group="7")]
        ordered = hs.order_group(recs[0], recs)
        self.assertEqual([r["$id"]["value"] for r in ordered], ["7", "5", "6"])


class TestEnclosureBlock(_Base):
    def test_block_missing_in_app32_needs_review(self):
        self.blocks = []
        self.seed(_case())
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["review"]), (0, 1))
        self.assertEqual(self.shipping, {})
        self.assertIn("同封物ブロック「受理通知書写し」が App 32 に未登録", self.texts()[0])

    def test_option_missing_in_app30_needs_review(self):
        self.options.discard("受理通知書写し")
        self.seed(_case())
        out = self.run_soufu()
        self.assertEqual(out["filed"], 0)
        self.assertIn("App 30 同封物選択 の選択肢に「受理通知書写し」がありません", self.texts()[0])

    def test_block_unit_mismatch_needs_review(self):
        self.blocks[0]["対象ユニット"] = {"value": ["時効援用"]}
        self.seed(_case())
        self.assertEqual(self.run_soufu()["filed"], 0)


# ── 発送済の書き戻し ─────────────────────────────────────────────────────────
class TestWriteBack(_Base):
    def _ship(self, app_id="40", meta=None):
        return {"$id": {"value": "100"}, "$revision": {"value": "2"},
                "発送ステータス": {"value": "発送済"}, "チャネル": {"value": "送付案内"},
                "件名": {"value": "受理通知送付（甲社）"}, "顧客名表示用": {"value": "申述太郎"},
                "実行済み": {"value": "yes"}, "案件アプリID": {"value": app_id}, "案件レコードID": {"value": "1"},
                "チャネル固有データ": {"value": json.dumps(meta if meta is not None else
                                                         {"houki_soufu_key": "houki_soufu:1:11", "row_id": "11",
                                                          "case_record_id": "1", "needs_return": False})}}

    def test_shipped_houki_record_marks_row_sent_and_completes(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        self.shipping["100"] = self._ship()
        with patch.object(hub_notify, "notify_attorney_approval", AsyncMock()):
            asyncio.run(hub_dispatch.process_dispatch("100"))
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "完了")
        self.assertEqual(self.rows()[0]["value"]["送付状態"]["value"], "送付済")
        self.assertEqual(self.rows()[0]["value"]["送付発送管理No"]["value"], "100")

    def test_jikou_record_is_untouched(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        self.shipping["100"] = self._ship(app_id="21", meta={"needs_return": False})
        with patch.object(hub_notify, "notify_attorney_approval", AsyncMock()):
            asyncio.run(hub_dispatch.process_dispatch("100"))
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "完了")
        self.assertEqual(self.rows()[0]["value"]["送付状態"]["value"], "起票済")
        self.assertEqual([u for u in self.updates if u[0] == "APP_HOUKI"], [])

    def test_write_back_failure_notifies_and_keeps_completion(self):
        self.seed(_case(rows=[_row("99", "甲社", state="起票済")]))    # row_id 11 は存在しない
        self.shipping["100"] = self._ship()
        with patch.object(hub_notify, "notify_attorney_approval", AsyncMock()):
            asyncio.run(hub_dispatch.process_dispatch("100"))
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "完了")
        self.assertTrue(any("書き戻せませんでした" in t for t in self.texts()))
        self.assertIn("houki_soufu_needs_review", self.kinds())

    def test_mark_row_sent_cas_conflict_refetches_once(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済")]))
        self.conflict_once.add("1")
        self.assertTrue(asyncio.run(hs.mark_row_sent(self._ship())))
        self.assertEqual(self.rows()[0]["value"]["送付状態"]["value"], "送付済")


# ── M4 prepare の相続放棄分岐 ────────────────────────────────────────────────
class TestPrepareHouki(_Base):
    def _shipping(self, unit="相続放棄"):
        return {"$id": {"value": "100"}, "ユニット種別": {"value": unit}, "チャネル": {"value": "送付案内"},
                "宛先名": {"value": "甲社"}, "宛先郵便番号": {"value": "100-0001"},
                "宛先住所": {"value": "東京都千代田区1-1"}, "顧客名表示用": {"value": "申述太郎"},
                "件名": {"value": "受理通知送付（甲社）"}, "同封物選択": {"value": ["受理通知書写し"]},
                "本文_特記事項": {"value": ""},
                "チャネル固有データ": {"value": json.dumps({"houki_soufu_key": "houki_soufu:1:11", "row_id": "11",
                                                        "case_record_id": "1"})}}

    def test_prepare_produces_letter_copy_and_single_face_label(self):
        self.seed(_case())
        blocks = [{"ブロックキー": {"value": "受理通知書写し"}, "表示名": {"value": "受理通知書写し"},
                   "案内文": {"value": ""}, "対象ユニット": {"value": ["相続放棄"]},
                   "返送要否": {"value": "不要"}, "表示順": {"value": "1"}, "有効": {"value": "yes"}}]
        labels = []

        def fake_labels(addresses, layout="A4_2x6", **kw):
            labels.append(addresses)
            return b"%PDF-labels"
        ai = AsyncMock(side_effect=AssertionError("AI must not be called for houki"))
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=blocks)), \
                patch.object(soufu_annai, "render_label_sheet", fake_labels), \
                patch.object(soufu_annai, "create_message_with_fallback", ai):
            result = asyncio.run(soufu_annai.SoufuAnnaiAdapter().prepare(self._shipping()))
        names = [a.filename for a in result.artifacts]
        self.assertEqual(names, ["受理通知送付状.docx", "受理通知書写し_受理通知書.pdf", "宛名ラベル.pdf"])
        self.assertEqual(result.artifacts[1].content, b"%PDF-notice-fk-notice")
        self.assertEqual(labels, [[{"宛先名": "甲社", "郵便番号": "100-0001", "住所": "東京都千代田区1-1", "敬称": "御中"}]])
        doc = Document(io.BytesIO(result.artifacts[0].content))
        text = "\n".join(p.text for p in doc.paragraphs)
        self.assertIn("甲社　御中", text)
        self.assertIn("申述太郎 氏の代理人として、被相続人 被相続花子 氏", text)
        self.assertIn("さいたま家庭裁判所（事件番号：令和8年（家）第123号）において、令和8年9月1日 付で受理", text)
        self.assertNotIn("{{", text)
        meta = json.loads(result.fields["チャネル固有データ"])
        self.assertEqual(meta["needs_return"], False)
        self.assertNotIn("本文_特記事項", result.fields)

    def test_prepare_missing_case_fields_raises(self):
        self.seed(_case(**{"管轄家庭裁判所": ""}))
        blocks = [{"ブロックキー": {"value": "受理通知書写し"}, "表示名": {"value": "x"}, "案内文": {"value": ""},
                   "対象ユニット": {"value": ["相続放棄"]}, "返送要否": {"value": "不要"}, "表示順": {"value": "1"}}]
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=blocks)):
            with self.assertRaises(soufu_annai.SoufuAnnaiError) as cm:
                asyncio.run(soufu_annai.SoufuAnnaiAdapter().prepare(self._shipping()))
        self.assertIn("管轄家庭裁判所", str(cm.exception))
        self.assertNotIn("申述太郎", str(cm.exception))

    def test_jikou_unit_still_uses_legacy_path(self):
        rec = self._shipping(unit="時効援用")
        rec["同封物選択"] = {"value": ["返信用封筒"]}
        rec["チャネル固有データ"] = {"value": ""}
        blocks = [{"ブロックキー": {"value": "返信用封筒"}, "表示名": {"value": "返信用封筒"}, "案内文": {"value": ""},
                   "対象ユニット": {"value": ["時効援用"]}, "返送要否": {"value": "不要"}, "表示順": {"value": "1"}}]
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=blocks)), \
                patch.object(soufu_annai, "generate_tokki_note", AsyncMock(return_value="")):
            result = asyncio.run(soufu_annai.SoufuAnnaiAdapter().prepare(rec))
        self.assertEqual([a.filename for a in result.artifacts], ["送付案内.docx", "宛名ラベル.pdf"])


# ── 送付状（差し込み・和暦・凍結） ──────────────────────────────────────────
class TestLetter(unittest.TestCase):
    def test_wareki_eras_and_passthrough(self):
        self.assertEqual(L.wareki(date(1980, 2, 3)), "昭和55年2月3日")
        self.assertEqual(L.wareki(date(1926, 12, 25)), "昭和元年12月25日")
        self.assertEqual(L.wareki(date(1989, 1, 7)), "昭和64年1月7日")
        self.assertEqual(L.wareki(date(1989, 1, 8)), "平成元年1月8日")
        self.assertEqual(L.wareki(date(2019, 5, 1)), "令和元年5月1日")
        self.assertEqual(L.wareki_text("2026-09-01"), "令和8年9月1日")
        self.assertEqual(L.wareki_text("昭和25年1月1日"), "昭和25年1月1日")   # 非 ISO はそのまま
        self.assertEqual(L.wareki_text(""), "")

    def test_placeholders_15_and_data_keys(self):
        self.assertEqual(len(L.PLACEHOLDERS), 15)
        self.assertEqual(set(L.PLACEHOLDERS), set(config.EXPECTED_DOCX_TEMPLATES[L.TEMPLATE_PATH]))
        data = L.build_letter_data(_case(), "甲社", "100-0001", "東京都千代田区1-1", "署名", today=TODAY)
        self.assertEqual(set(data), set(L.PLACEHOLDERS))
        self.assertEqual(data["{{日付}}"], "令和8年9月8日")
        self.assertEqual(data["{{宛先郵便番号}}"], "〒100-0001")
        self.assertEqual(data["{{宛先名}}"], "甲社　御中")
        self.assertEqual(data["{{受理日}}"], "令和8年9月1日")
        self.assertEqual(data["{{死亡日}}"], "令和8年5月1日")
        self.assertEqual(data["{{申述人生年月日}}"], "昭和55年2月3日")
        self.assertEqual(data["{{被相続人生年月日}}"], "昭和25年1月1日")
        self.assertEqual(L.build_letter_data(_case(), "甲社", "", "住所", "署名", today=TODAY)["{{宛先郵便番号}}"], "")

    def test_template_sha_and_frozen_body_pinned(self):
        raw = (REPO / L.TEMPLATE_PATH).read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), L.TEMPLATE_SHA256)
        self.assertEqual(L.body_sha256(L.BODY_PARAGRAPHS), L.BODY_SHA256)
        self.assertEqual(L.template_body(raw), list(L.BODY_PARAGRAPHS))
        doc = Document(io.BytesIO(raw))
        self.assertEqual(doc.tables, [])
        for key in L.PLACEHOLDERS:                                    # 各キーは単一 run
            self.assertTrue(any(key in r.text for p in doc.paragraphs for r in p.runs), key)

    def test_frozen_body_change_is_rejected(self):
        with patch.object(L, "BODY_PARAGRAPHS", L.BODY_PARAGRAPHS[:-1] + ("以上（改変）",)):
            with self.assertRaises(L.LetterIntegrityError):
                L.render_letter(L.build_letter_data(_case(), "甲社", "", "住所", "署名", today=TODAY))
        with patch.object(L, "TEMPLATE_SHA256", "0" * 64):
            with self.assertRaises(L.LetterIntegrityError):
                L.render_letter(L.build_letter_data(_case(), "甲社", "", "住所", "署名", today=TODAY))

    def test_render_replaces_all_and_keeps_signature_lines(self):
        data = L.build_letter_data(_case(), "甲社", "100-0001", "東京都千代田区1-1", "〒332\n事務所\n弁護士", today=TODAY)
        doc = Document(io.BytesIO(L.render_letter(data)))
        paras = [p.text for p in doc.paragraphs]
        self.assertNotIn("{{", "".join(paras))
        self.assertEqual(paras[0], "令和8年9月8日")
        self.assertEqual(paras[1:4], ["〒100-0001", "東京都千代田区1-1", "　　　甲社　御中"])
        self.assertEqual(paras[5], "〒332\n事務所\n弁護士")
        self.assertEqual(paras[7], L.TITLE)
        self.assertIn("死亡年月日　令和8年5月1日", paras)
        self.assertEqual(paras[-1], "以上")
        with self.assertRaises(L.LetterIntegrityError):
            L.render_letter({k: v for k, v in data.items() if k != "{{事件番号}}"})


# ── kind・config 登録 ───────────────────────────────────────────────────────
class TestRegistry(unittest.TestCase):
    def test_notify_kinds_registered(self):
        import logging
        for kind in ("houki_soufu_filed", "houki_soufu_needs_review"):
            with self.assertLogs("hub.notify", level=logging.INFO) as cm:
                hub_notify._log_throttled(f"{kind}:1")
            self.assertIn(f"kind={kind}", "\n".join(cm.output))
            self.assertNotIn("unknown_kind", "\n".join(cm.output))

    def test_config_registrations(self):
        self.assertEqual(config.UNIT_CONFIG["相続放棄"],
                         {"case_app_env": ("APP_HOUKI", "TOKEN_HOUKI"), "customer_name_field": "顧客名",
                          "customer_addr_field": "住所", "channels": ["送付案内"], "template_dir": "houki"})
        self.assertIn("相続放棄", config.EXPECTED_KINTONE_SCHEMA["App 30 (発送管理)"]["fields"]["ユニット種別"]["required_options"])
        app40 = config.EXPECTED_KINTONE_SCHEMA["App 40 (相続放棄案件)"]
        self.assertEqual((app40["app_id_env"], app40["token_env"]), ("APP_HOUKI", "TOKEN_HOUKI"))
        for code, t in (("受理通知書", "FILE"), ("受理日", "DATE"), ("債権者一覧", "SUBTABLE"),
                        ("被相続人生年月日", "SINGLE_LINE_TEXT"), ("死亡日", "DATE")):
            self.assertEqual(app40["fields"][code]["type"], t, code)
        self.assertLessEqual({"受理", "債権者通知", "完了"}, set(app40["fields"]["status"]["required_options"]))
        self.assertEqual(hs.INCLUDED_DESTINATIONS, 3)
        self.assertEqual(hs.ENCLOSURE_BLOCK_KEY, "受理通知書写し")
        self.assertEqual(hs.soufu_key("1", "11"), "houki_soufu:1:11")

    def test_constants_pinned(self):
        self.assertEqual((hs.STATUS_ACCEPTED, hs.STATUS_NOTIFYING, hs.STATUS_COMPLETED), ("受理", "債権者通知", "完了"))
        self.assertEqual((hs.NOTIFY_REQUIRED, hs.STATE_TODO, hs.STATE_FILED, hs.STATE_SENT), ("要", "未", "起票済", "送付済"))
        self.assertEqual(L.REQUIRED_CASE_FIELDS, ("顧客名", "住所", "生年月日", "被相続人氏名", "被相続人最後の住所",
                                                  "被相続人生年月日", "死亡日", "管轄家庭裁判所", "事件番号", "受理日"))
        self.assertNotIn("死亡日_申告", L.REQUIRED_CASE_FIELDS)


if __name__ == "__main__":
    unittest.main()
