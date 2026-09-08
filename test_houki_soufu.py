"""HOUKI-SOUFU-1: 相続放棄 受理通知書写しの発送起票のテスト。

- 起点条件（status/受領日/添付の各欠落で not triggered・本文 status gate）
- 必須値欠落で起票 0+要確認（欄名のみ・値なし）
- 対象行の絞り込み（要/未/住所）・行ごと起票と行更新の CAS（他行・他列保全）
- 冪等（再配送で 2 件目なし・既存検索で行を揃える・送付状態≠未 はスキップ）
- status 一方向遷移（受理→債権者通知 を 1 回・以降は触らない・409）
- 追加料金対象の出現順（グループ和集合・管理レコード先頭・4 番目以降・既存 yes 保全）
- ブロック未登録（App 32 / App 30 選択肢）で要確認
- 発送済書き戻し（App 40 のみ・時効側不変）・fix1 D: 取得/更新の例外は要確認通知 1 件・例外を伝播しない
- fix1 A: claim 先行（未→起票済・起票中）→ App 30 作成 → 番号書込・並行 2 実行で重複なし・stale snapshot の
  claim は 409・作成失敗は 起票中 のまま次回回収・Lock
- fix1 B: status=債権者通知 でも未 行の起票と回収・受理→債権者通知 は現在が 受理 のときだけ
- fix1 C: 宛先は App 30 から・App 40 の行と不一致なら成果物 0＋要確認（PrepareDeferred＝下書きのまま）
- fix2 A: claim は 起票中:{token}:{期限}（TTL 600 秒）・期限内は他者が回収しない・期限切れは自 token で claim し直し・
  作成直前の所有権確認・作成後の重複検出（番号最小を正・自分の 下書き は 下書き→エラー で無効化〔物理削除は RV-08 pin で不可〕／
  下書き でなければ要確認）・番号書込は再取得 revision で 1 回
- fix2 B: 回収の新規作成前に 通知要否=要 ∧ 住所非空 を再確認・満たさなければ 未 に戻して要確認
- fix2 C: 書き戻し失敗通知は notify_admin_line_result・failed は ERROR ログ（番号入り）・throttled は成功
- fix3 A: 起票は pending_dedupe=true で作成・prepare は pending の間 PrepareDeferred(silent)・resolve で正だけ pending 解除、
  正以外は 下書き∧pending のみ エラー 化（再取得 revision で CAS）・既に 承認待ち は触らず要確認
- fix3 B: 番号復旧でも全件検索→resolve を必ず通す・未解決は duplicates_unresolved（起票件数に数えない）・
  作成後の応答喪失は期限切れ後の回収で 1 件・pending 解除・番号確定
- fix4 A: 作成後の検索に無ければ番号で実物を取得（代替データを組み立てない）・取得失敗は claim/pending 維持・書込 0・通知なし・
  pending 解除は実物へのマージ＋revision（AST pin）
- fix4 B: 解除失敗は pending_clear_failed（番号を書かない・該当番号のみ通知）・番号あり pending 残りは実行のたびに再解除・
  通知内訳「起票 / 重複未解決 / 解除再試行待ち」
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
from datetime import date, datetime, timedelta, timezone
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
_REAL_NOTIFY_RESULT = hub_notify.notify_admin_line_result      # _Base の mock 前に捕捉

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
        self.conflict_on_nth: set[int] = set()        # App 40 の N 回目の update を 409 に（1 始まり）
        self.houki_update_calls = 0
        self.deleted: list[str] = []
        self.ship_conflict_on_nth: set[int] = set()    # App 30 の N 回目の update を 409 に（1 始まり）
        self.ship_update_calls = 0
        self.create_status = "下書き"                   # fake: 作成直後の 発送ステータス（重複テスト用）
        self.now = datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc)

        async def get_record(app, rid):
            await asyncio.sleep(0)                      # 並行テストの yield 点
            store = self.cases if app.app_id_env == "APP_HOUKI" else self.shipping
            if str(rid) not in store:
                raise hub_kintone.KintoneError(404, "GAIA_RE01", "not found")
            return copy.deepcopy(store[str(rid)])

        async def update_record(app, rid, fields, revision=None):
            store = self.cases if app.app_id_env == "APP_HOUKI" else self.shipping
            rec = store[str(rid)]
            self.updates.append((app.app_id_env, str(rid), copy.deepcopy(fields), revision))
            if app.app_id_env == "APP_HOUKI":
                self.houki_update_calls += 1
                if self.houki_update_calls in self.conflict_on_nth:
                    raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
            if app.app_id_env == "APP_SHIPPING":
                self.ship_update_calls += 1
                if self.ship_update_calls in self.ship_conflict_on_nth:
                    raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
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
            await asyncio.sleep(0)
            self.assertEqual(app.app_id_env, "APP_SHIPPING")
            rid = str(self.next_ship)
            self.next_ship += 1
            rec = {"$id": {"value": rid}, "$revision": {"value": "1"}}
            for k, v in fields.items():
                rec[k] = {"value": copy.deepcopy(v)}
            rec["発送ステータス"] = {"value": self.create_status}
            self.shipping[rid] = rec
            return rid

        async def delete_record(app, rid):
            self.assertEqual(app.app_id_env, "APP_SHIPPING")
            self.deleted.append(str(rid))
            self.shipping.pop(str(rid), None)

        async def search_records(app, query, fields=None):
            if app.app_id_env == "APP_ENCLOSURE":
                return copy.deepcopy(self.blocks)
            if app.app_id_env == "APP_SHIPPING":
                key = query.split('like "')[1].split('"')[0]
                return [copy.deepcopy(r) for r in sorted(self.shipping.values(), key=lambda r: int(r["$id"]["value"]))
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
        self.admin_result = AsyncMock(return_value="sent")
        for p in (patch.object(hub_kintone, "get_record", get_record),
                  patch.object(hub_kintone, "update_record", update_record),
                  patch.object(hub_kintone, "create_record", create_record),
                  patch.object(hub_kintone, "delete_record", delete_record),
                  patch.object(hub_kintone, "search_records", search_records),
                  patch.object(hub_kintone, "get_form_fields", get_form_fields),
                  patch.object(hub_kintone, "download_file", download_file),
                  patch.object(hub_notify, "notify_admin_line", self.admin),
                  patch("hub.notify.notify_admin_line", self.admin),
                  patch.object(hub_notify, "notify_admin_line_result", self.admin_result),
                  patch.object(hs, "_now_utc", lambda: self.now)):
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

    def result_texts(self):
        return [c.args[0] for c in self.admin_result.await_args_list]

    def advance(self, seconds):
        self.now = self.now + timedelta(seconds=seconds)

    def ship_no(self, rid="1", idx=0):
        return self.rows(rid)[idx]["value"]["送付発送管理No"]["value"]

    def _dup(self, sid, status="下書き", pending=True, key="houki_soufu:1:11"):
        return {"$id": {"value": sid}, "$revision": {"value": "1"}, "発送ステータス": {"value": status},
                "チャネル固有データ": {"value": json.dumps({"houki_soufu_key": key, "row_id": "11",
                                                        "case_record_id": "1", "pending_dedupe": pending})}}

    def _race_first_search_empty(self):
        """両方が作成に至った競合の再現: 作成前の全件検索だけを「なし」に強制（作成後の検索は実物）。"""
        real = hs.find_all_shipping
        state = {"first": True}

        async def once_empty(key):
            if state["first"]:
                state["first"] = False
                return []
            return await real(key)
        return patch.object(hs, "find_all_shipping", once_empty)

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
        for st in ("受任", "完了", "裁判所提出済", ""):        # fix1 B-1: 債権者通知 は通る（別テスト）
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
                _row("14", "丁社", state="起票済", ship_no="200"), _row("15", "戊社", addr=""), _row("16", "己社", zip_="")]
        self.seed(_case(rows=rows))
        out = self.run_soufu()
        self.assertEqual(out["filed"], 2)                           # 甲社・己社（郵便番号空でも起票）
        self.assertEqual({r["宛先名"]["value"] for r in self.shipping.values()}, {"甲社", "己社"})
        states = {r["value"]["債権者名"]["value"]: r["value"]["送付状態"]["value"] for r in self.rows()}
        self.assertEqual(states, {"甲社": "起票済", "乙社": "未", "丙社": "未", "丁社": "起票済", "戊社": "未", "己社": "起票済"})
        review = [t for t in self.texts() if t.startswith(hs.NOTICE_HEAD_REVIEW)]
        self.assertEqual(len(review), 1)
        self.assertIn("債権者住所が未入力の行（通知要否=要）: 5 行目", review[0])
        self.assertIn("住所を入力して保存すれば起票されます（status は変更不要）", review[0])
        self.assertNotIn("受理のまま", review[0])
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
        self.assertEqual(meta, {"houki_soufu_key": "houki_soufu:1:11", "row_id": "11", "case_record_id": "1",
                                "pending_dedupe": False})                  # fix3: 作成時 true → 重複確認後 false
        row = self.rows()[0]
        self.assertEqual((row["value"]["送付状態"]["value"], row["value"]["送付発送管理No"]["value"],
                          row["value"]["追加料金対象"]["value"]), ("起票済", "100", "no"))
        self.assertEqual(row["value"]["債権者住所"]["value"], "東京都千代田区1-1")   # 他列保全
        houki_updates = [u for u in self.updates if u[0] == "APP_HOUKI"]
        # fix1 A: claim（未→起票済・起票中）→ 番号書込 → status の順
        self.assertEqual([sorted(u[2]) for u in houki_updates], [["債権者一覧"], ["債権者一覧"], ["status"]])
        self.assertEqual(houki_updates[0][3], "3")                  # CAS: 取得時 revision
        claimed_row = houki_updates[0][2]["債権者一覧"][0]["value"]
        self.assertEqual(claimed_row["送付状態"]["value"], "起票済")
        self.assertTrue(claimed_row["送付発送管理No"]["value"].startswith("起票中:"))   # fix2: 起票中:{token}:{期限}
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "下書き")   # 起票は claim の後

    def test_claim_cas_conflict_skips_row_and_next_run_files(self):
        """fix1 A-1: claim の 409 は「他者が処理中」としてその行をスキップ（App 30 を作らない）。"""
        self.seed(_case())
        self.conflict_on_nth.add(1)
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["pending"]), (0, 0))
        self.assertEqual(self.shipping, {})
        self.assertEqual(self.rows()[0]["value"]["送付状態"]["value"], "未")
        self.assertEqual(self.cases["1"]["status"]["value"], "受理")
        out2 = self.run_soufu()
        self.assertEqual((out2["filed"], out2["promoted"]), (1, True))

    def test_number_write_cas_conflict_leaves_claiming_and_redelivery_aligns(self):
        """fix1 A-3/A-4・B: 番号書込の CAS が失敗 → 行は 起票中 のまま → 再配送（status=債権者通知）で
        既存起票を検索して番号を揃える（2 件目を作らない）。"""
        self.seed(_case())
        self.conflict_on_nth.update({2, 3})                          # 番号書込（初回+再取得後）を 409 に
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["pending"], out["promoted"]), (1, 1, True))
        row = self.rows()[0]["value"]
        self.assertEqual(row["送付状態"]["value"], "起票済")
        self.assertTrue(row["送付発送管理No"]["value"].startswith("起票中:"))
        self.assertTrue(any("回収" in t for t in self.texts()))
        self.assertEqual(self.cases["1"]["status"]["value"], "債権者通知")
        self.admin.reset_mock()
        self.assertEqual(self.run_soufu()["recovered"], 0)          # fix2 A-2: 期限内の 起票中 は触らない
        self.advance(hs.CLAIM_TTL_SEC + 1)
        out2 = self.run_soufu()
        self.assertEqual((out2["filed"], out2["aligned"], out2["recovered"], out2["promoted"]), (0, 1, 1, False))
        self.assertEqual(len(self.shipping), 1)
        self.assertEqual(self.rows()[0]["value"]["送付発送管理No"]["value"], "100")

    def test_redelivery_does_not_create_second_record(self):
        self.seed(_case())
        self.run_soufu()
        self.assertEqual(len(self.shipping), 1)
        self.admin.reset_mock()
        out = self.run_soufu()                                       # 再配送: 債権者通知 でも入るが対象行なし
        self.assertEqual(out["skip"], "no_target_rows")
        self.assertEqual(len(self.shipping), 1)
        self.assertEqual(self.admin.await_count, 0)

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


class TestClaimAndRecovery(_Base):
    """fix1 A: claim 先行・回収・Lock。"""

    def test_concurrent_runs_create_exactly_one_record(self):
        self.seed(_case(rows=[_row("11", "甲社"), _row("12", "乙社")]))

        async def both():
            return await asyncio.gather(hs.process_soufu("1"), hs.process_soufu("1"))
        r1, r2 = asyncio.run(both())
        self.assertEqual(r1["filed"] + r2["filed"], 2)               # 行 2 つに対し合計 2 件・重複なし
        self.assertEqual(len(self.shipping), 2)
        self.assertEqual({r["宛先名"]["value"] for r in self.shipping.values()}, {"甲社", "乙社"})
        self.assertEqual({r["value"]["送付発送管理No"]["value"] for r in self.rows()}, {"100", "101"})

    def test_claim_cas_rejects_stale_snapshot_even_if_search_found_nothing(self):
        """既存検索が両方「なし」でも、同じ revision の snapshot で 2 回目の claim は 409 で弾かれる。"""
        self.seed(_case())
        snapshot = copy.deepcopy(self.cases["1"])
        self.assertEqual(asyncio.run(hs.find_all_shipping("houki_soufu:1:11")), [])
        self.assertTrue(asyncio.run(hs.claim_row("1", "11", copy.deepcopy(snapshot), "tokA")))
        self.assertEqual(asyncio.run(hs.find_all_shipping("houki_soufu:1:11")), [])
        self.assertFalse(hasattr(hs, "find_existing_shipping"))                    # 先頭 1 件だけ見る経路は無い
        self.assertFalse(asyncio.run(hs.claim_row("1", "11", copy.deepcopy(snapshot), "tokB")))   # revision 3 は古い
        self.assertFalse(asyncio.run(hs.claim_row("1", "11", None, "tokB")))                      # 最新でも 未 でないので不可
        row = self.rows()[0]["value"]
        self.assertEqual(row["送付状態"]["value"], "起票済")
        self.assertEqual(hs.claim_owner(self.rows()[0]), "tokA")
        self.assertEqual(self.shipping, {})

    def test_create_failure_leaves_row_claiming_and_next_run_recovers(self):
        self.seed(_case())
        real_create = hub_kintone.create_record
        calls = {"n": 0}

        async def flaky_create(app, fields):
            calls["n"] += 1
            if calls["n"] == 1:
                raise hub_kintone.KintoneError(500, "GAIA_XX", "boom")
            return await real_create(app, fields)
        with patch.object(hub_kintone, "create_record", flaky_create):
            out = self.run_soufu()
            self.assertEqual((out["filed"], out["pending"], out["promoted"]), (0, 1, False))
            row = self.rows()[0]["value"]
            self.assertEqual(row["送付状態"]["value"], "起票済")
            self.assertTrue(row["送付発送管理No"]["value"].startswith("起票中:"))
            self.assertEqual(self.shipping, {})
            self.assertEqual(self.cases["1"]["status"]["value"], "受理")
            self.assertTrue(any("次回の保存時に自動で回収" in t for t in self.texts()))
            self.admin.reset_mock()
            self.assertEqual(self.run_soufu()["recovered"], 0)      # 期限内は回収しない
            self.advance(hs.CLAIM_TTL_SEC + 1)
            out2 = self.run_soufu()
        self.assertEqual((out2["filed"], out2["recovered"], out2["promoted"]), (1, 1, True))
        self.assertEqual(len(self.shipping), 1)
        self.assertEqual(self.rows()[0]["value"]["送付発送管理No"]["value"], "100")
        self.assertEqual(self.cases["1"]["status"]["value"], "債権者通知")

    def test_recovery_row_with_existing_shipping_gets_number_without_create(self):
        self.seed(_case(status="債権者通知", rows=[_row("11", "甲社", state="起票済", ship_no="")]))
        self.shipping["100"] = self._dup("100")
        self.next_ship = 101
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["aligned"], out["recovered"]), (0, 1, 1))
        self.assertEqual(len(self.shipping), 1)
        self.assertEqual(self.rows()[0]["value"]["送付発送管理No"]["value"], "100")
        self.assertFalse(hs.is_pending_dedupe(self.shipping["100"]))         # 番号復旧でも pending 解除

    def test_recovery_claim_cas_conflict_skips(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="")]))
        self.conflict_on_nth.add(1)                                  # 回収 claim を 409 に
        out = self.run_soufu()
        self.assertEqual((out["recovered"], out["filed"]), (0, 0))
        self.assertEqual(self.shipping, {})


class TestOwnerClaim(_Base):
    """fix2 A: 所有者付き claim・期限・所有権確認・重複検出。"""

    def test_claim_value_format_and_expiry(self):
        v = hs.claim_value("abc123def456", self.now)
        self.assertEqual(v, "起票中:abc123def456:2026-09-08T00:10:00Z")
        self.assertEqual(hs.parse_claim(v), ("abc123def456", datetime(2026, 9, 8, 0, 10, tzinfo=timezone.utc)))
        self.assertEqual(hs.parse_claim("起票中"), ("", None))                 # 旧形式=期限切れ扱い
        self.assertIsNone(hs.parse_claim("100"))
        self.assertIsNone(hs.parse_claim(""))
        row = _row("11", "甲社", state="起票済", ship_no=v)
        self.assertFalse(hs.claim_expired(row, self.now + timedelta(seconds=599)))
        self.assertTrue(hs.claim_expired(row, self.now + timedelta(seconds=600)))
        self.assertTrue(hs.claim_expired(_row("11", "甲社", state="起票済", ship_no="起票中"), self.now))
        self.assertTrue(hs.is_recovery_row(_row("11", "甲社", state="起票済", ship_no=""), self.now))
        self.assertFalse(hs.is_recovery_row(row, self.now))
        self.assertTrue(hs.is_active_claim(row, self.now))

    def test_other_instance_does_not_recover_active_claim(self):
        """A が claim 後に待機 → B（別 Lock）が最新取得 → 期限内の 起票中 は回収しない（作成 0）。"""
        self.seed(_case())
        self.assertTrue(asyncio.run(hs.claim_row("1", "11", None, "tokA")))
        out_b = asyncio.run(hs._process_soufu_locked("1", "tokB"))
        self.assertEqual((out_b["filed"], out_b["recovered"], out_b["skip"]), (0, 0, "no_target_rows"))
        self.assertEqual(self.shipping, {})
        self.assertEqual(hs.claim_owner(self.rows()[0]), "tokA")

    def test_expired_claim_recovered_by_b_then_a_loses_ownership(self):
        self.seed(_case())
        self.assertTrue(asyncio.run(hs.claim_row("1", "11", None, "tokA")))
        self.advance(hs.CLAIM_TTL_SEC + 1)
        out_b = asyncio.run(hs._process_soufu_locked("1", "tokB"))
        self.assertEqual((out_b["filed"], out_b["recovered"]), (1, 1))
        self.assertEqual(self.ship_no(), "100")
        # A が再開: 作成直前の所有権確認で止まる（作成 0）
        result = {"filed": 0, "aligned": 0, "review": 0, "pending": 0, "recovered": 0,
                  "lost": 0, "reverted": 0, "promoted": False, "skip": ""}
        filed = []
        asyncio.run(hs._file_claimed_row("1", self.cases["1"], "11", "tokA", result, filed))
        self.assertEqual((result["lost"], filed, len(self.shipping)), (1, [], 1))

    def test_recovery_rewrites_claim_with_own_token_before_proceeding(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="起票中")]))     # 旧形式
        claims = []
        real_update = hub_kintone.update_record

        async def spy(app, rid, fields, revision=None):
            if app.app_id_env == "APP_HOUKI" and "債権者一覧" in fields:
                claims.append(fields["債権者一覧"][0]["value"]["送付発送管理No"]["value"])
            return await real_update(app, rid, fields, revision)
        with patch.object(hub_kintone, "update_record", spy):
            out = asyncio.run(hs._process_soufu_locked("1", "tokB"))
        self.assertEqual(out["recovered"], 1)
        self.assertTrue(claims[0].startswith("起票中:tokB:"))          # 自分の token・新しい期限
        self.assertEqual(claims[1], "100")

    def test_both_created_min_id_wins_and_own_draft_is_voided(self):
        """両方が作成に至った場合（既存検索を fake で「なし」に強制）→ 番号最小を正・自分の 下書き を無効化
        （下書き→エラー。物理削除は RV-08 の repo 全域 pin により行わない）。"""
        self.seed(_case())
        self.shipping["50"] = self._dup("50")
        with self._race_first_search_empty():
            out = self.run_soufu()
        self.assertEqual((out["filed"], out["aligned"], out["duplicates_unresolved"]), (0, 1, 0))
        self.assertEqual(self.deleted, [])                                   # 物理削除なし
        self.assertEqual(set(self.shipping), {"50", "100"})
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "エラー")
        self.assertEqual(self.shipping["100"]["エラー詳細"]["value"], "二重起票（正: 発送管理 No.50）。本レコードは使用しません。")
        self.assertFalse(hs.is_pending_dedupe(self.shipping["100"]))
        self.assertEqual(self.shipping["50"]["発送ステータス"]["value"], "下書き")
        self.assertFalse(hs.is_pending_dedupe(self.shipping["50"]))          # 正の pending は外れる
        self.assertEqual(self.ship_no(), "50")
        self.assertEqual([t for t in self.texts() if "二重起票" in t], [])

    def test_both_created_own_not_draft_is_kept_and_reviewed(self):
        self.seed(_case())
        self.shipping["50"] = self._dup("50")
        self.create_status = "承認待ち"                                 # 自分の分が 下書き でない
        with self._race_first_search_empty():
            out = self.run_soufu()
        self.assertEqual(self.deleted, [])
        self.assertEqual(set(self.shipping), {"50", "100"})
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "承認待ち")   # 触らない
        self.assertEqual((out["filed"], out["duplicates_unresolved"]), (0, 1))
        self.assertEqual(self.ship_no(), "50")
        review = [t for t in self.texts() if "二重起票の疑い" in t]
        self.assertEqual(len(review), 1)
        self.assertIn("No.50", review[0])
        self.assertIn("No.100", review[0])
        self.assertNotIn("甲社", review[0])
        filed = [t for t in self.texts() if t.startswith(hs.NOTICE_HEAD_FILED)]
        self.assertIn("起票 0 件 / 重複未解決 1 件 / 解除再試行待ち 0 件", filed[0])

    def test_own_created_is_min_keeps_it(self):
        self.seed(_case())
        self.shipping["200"] = self._dup("200")
        with self._race_first_search_empty():
            out = self.run_soufu()
        self.assertEqual((out["filed"], self.deleted, self.ship_no()), (1, [], "100"))
        self.assertEqual(self.shipping["200"]["発送ステータス"]["value"], "エラー")

    def test_number_write_uses_refetched_revision_and_409_leaves_claiming(self):
        self.seed(_case())
        self.conflict_on_nth.add(2)                                  # 番号書込（1 回のみ・再取得なし）
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["pending"]), (1, 1))
        self.assertTrue(self.ship_no().startswith("起票中:"))
        self.assertEqual(len([u for u in self.updates if u[0] == "APP_HOUKI" and "債権者一覧" in u[2]]), 2)


class TestPendingDedupe(_Base):
    """fix3 A: 重複確認が済むまで prepare に入らない。"""

    def _blocks(self):
        return [{"ブロックキー": {"value": "受理通知書写し"}, "表示名": {"value": "x"}, "案内文": {"value": ""},
                 "対象ユニット": {"value": ["相続放棄"]}, "返送要否": {"value": "不要"}, "表示順": {"value": "1"}}]

    def _shipping_record(self, sid="100", pending=True, status="下書き"):
        return {"$id": {"value": sid}, "$revision": {"value": "1"}, "発送ステータス": {"value": status},
                "ユニット種別": {"value": "相続放棄"}, "チャネル": {"value": "送付案内"}, "実行済み": {"value": "no"},
                "宛先名": {"value": "甲社"}, "宛先郵便番号": {"value": "100-0001"},
                "宛先住所": {"value": "東京都千代田区1-1"}, "顧客名表示用": {"value": "申述太郎"},
                "件名": {"value": "受理通知送付（甲社）"}, "同封物選択": {"value": ["受理通知書写し"]},
                "本文_特記事項": {"value": ""}, "案件アプリID": {"value": "40"}, "案件レコードID": {"value": "1"},
                "チャネル固有データ": {"value": json.dumps({"houki_soufu_key": "houki_soufu:1:11", "row_id": "11",
                                                        "case_record_id": "1", "pending_dedupe": pending})}}

    def test_filed_record_carries_pending_flag(self):
        self.seed(_case())
        out = self.run_soufu()
        self.assertEqual(out["filed"], 1)
        self.assertFalse(hs.is_pending_dedupe(self.shipping["100"]))        # 単独なら即解除
        houki_writes = [u for u in self.updates if u[0] == "APP_SHIPPING"]
        self.assertEqual(len(houki_writes), 1)                               # pending 解除の CAS 1 回
        self.assertEqual(houki_writes[0][3], "1")

    def test_prepare_deferred_silently_while_pending(self):
        """作成直後に dispatcher の prepare が走っても pending_dedupe で PrepareDeferred（承認待ちに進まない・通知なし）。"""
        import logging
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        self.shipping["100"] = self._shipping_record(pending=True)
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=self._blocks())), \
                patch.object(hub_kintone, "upload_file", AsyncMock(return_value="fk")), \
                patch.object(hub_notify, "notify_attorney_approval", AsyncMock()) as approval, \
                self.assertLogs("channels.soufu_annai", level=logging.INFO) as cm:
            asyncio.run(hub_dispatch.process_dispatch("100"))
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "下書き")
        approval.assert_not_awaited()
        self.assertEqual(self.admin.await_count, 0)                          # 通知なし
        self.assertEqual(self.admin_result.await_count, 0)
        self.assertEqual(len([m for m in cm.output if "pending dedupe" in m]), 1)
        self.assertNotIn("成果物", self.shipping["100"])

    def test_prepare_runs_after_pending_cleared(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        self.shipping["100"] = self._shipping_record(pending=False)
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=self._blocks())), \
                patch.object(hub_kintone, "upload_file", AsyncMock(return_value="fk")), \
                patch.object(soufu_annai, "render_label_sheet", lambda *a, **k: b"%PDF"), \
                patch.object(hub_notify, "notify_attorney_approval", AsyncMock()) as approval:
            asyncio.run(hub_dispatch.process_dispatch("100"))
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "承認待ち")
        approval.assert_awaited_once()

    def test_voided_duplicate_never_enters_prepare(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="50")]))
        self.shipping["100"] = self._shipping_record(pending=False, status="エラー")
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=self._blocks())), \
                patch.object(hub_notify, "notify_attorney_approval", AsyncMock()) as approval:
            asyncio.run(hub_dispatch.process_dispatch("100"))
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "エラー")
        approval.assert_not_awaited()

    def test_resolve_clears_only_canonical_and_voids_pending_drafts(self):
        found = [self._dup("50"), self._dup("60"), self._dup("70", status="承認待ち", pending=False)]
        for r in found:
            self.shipping[r["$id"]["value"]] = copy.deepcopy(r)
        canonical, unresolved, cleared = asyncio.run(hs.resolve_duplicates(
            "1", "houki_soufu:1:11", asyncio.run(hs.find_all_shipping("houki_soufu:1:11"))))
        self.assertEqual((canonical, unresolved, cleared), ("50", True, True))
        self.assertFalse(hs.is_pending_dedupe(self.shipping["50"]))
        self.assertEqual(self.shipping["50"]["発送ステータス"]["value"], "下書き")
        self.assertEqual(self.shipping["60"]["発送ステータス"]["value"], "エラー")
        self.assertEqual(self.shipping["70"]["発送ステータス"]["value"], "承認待ち")   # 既に 承認待ち は エラー にしない
        review = [t for t in self.texts() if "二重起票の疑い" in t]
        self.assertEqual(len(review), 1)
        self.assertIn("正: 発送管理 No.50", review[0])
        self.assertIn("未解決: No.70", review[0])
        self.assertNotIn("No.60", review[0].split("未解決")[1])

    def test_void_cas_conflict_refetches_once(self):
        self.shipping["50"] = self._dup("50")
        self.shipping["60"] = self._dup("60")
        self.shipping["60"]["$revision"] = {"value": "9"}                     # 検索結果が古い revision
        found = [copy.deepcopy(self.shipping["50"]), copy.deepcopy(self.shipping["60"])]
        found[1]["$revision"] = {"value": "1"}
        canonical, unresolved, cleared = asyncio.run(hs.resolve_duplicates("1", "houki_soufu:1:11", found))
        self.assertEqual((canonical, unresolved, cleared), ("50", False, True))
        self.assertEqual(self.shipping["60"]["発送ステータス"]["value"], "エラー")

    def test_serverside_status_write_is_in_transition_table(self):
        from hub import approval
        self.assertIn(("下書き", "エラー"), approval.SERVER_TRANSITIONS)


class TestRealRecordAndPendingClear(_Base):
    """fix4 A: 代替レコードの組み立て禁止 / B: pending 解除失敗と重複未解決の区別 / B-3 保険。"""

    def _blocks(self):
        return [{"ブロックキー": {"value": "受理通知書写し"}, "表示名": {"value": "x"}, "案内文": {"value": ""},
                 "対象ユニット": {"value": ["相続放棄"]}, "返送要否": {"value": "不要"}, "表示順": {"value": "1"}}]

    def _run_prepare(self, sid="100"):
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=self._blocks())), \
                patch.object(hub_kintone, "upload_file", AsyncMock(return_value="fk")), \
                patch.object(soufu_annai, "render_label_sheet", lambda *a, **k: b"%PDF"), \
                patch.object(hub_notify, "notify_attorney_approval", AsyncMock()) as approval:
            asyncio.run(hub_dispatch.process_dispatch(sid))
        return approval

    def test_search_miss_after_create_fetches_real_record_and_keeps_metadata(self):
        """作成後の検索 0 件 → 番号で再取得 → pending 解除後もメタデータ保持・prepare が通る。"""
        self.seed(_case())
        with patch.object(hs, "find_all_shipping", AsyncMock(return_value=[])):
            out = self.run_soufu()
        self.assertEqual((out["filed"], out["deferred"], out["pending_clear_failed"]), (1, 0, 0))
        meta = json.loads(self.shipping["100"]["チャネル固有データ"]["value"])
        self.assertEqual(meta, {"houki_soufu_key": "houki_soufu:1:11", "row_id": "11", "case_record_id": "1",
                                "pending_dedupe": False})
        self.assertEqual(self.ship_no(), "100")
        ship_updates = [u for u in self.updates if u[0] == "APP_SHIPPING"]
        self.assertEqual(len(ship_updates), 1)
        self.assertEqual(ship_updates[0][3], "1")                                 # revision つき
        self.shipping["100"].update({"ユニット種別": {"value": "相続放棄"}, "チャネル": {"value": "送付案内"},
                                     "実行済み": {"value": "no"}, "本文_特記事項": {"value": ""}})
        approval = self._run_prepare()
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "承認待ち")
        approval.assert_awaited_once()

    def test_created_not_readable_keeps_claim_and_pending_then_recovers_after_ttl(self):
        self.seed(_case())
        real_get = hub_kintone.get_record
        fail = {"on": True}

        async def get(app, rid):
            if app.app_id_env == "APP_SHIPPING" and fail["on"]:
                raise hub_kintone.KintoneError(503, "GAIA_XX", "unavailable")
            return await real_get(app, rid)
        with patch.object(hub_kintone, "get_record", get), \
                patch.object(hs, "find_all_shipping", AsyncMock(return_value=[])):
            out = self.run_soufu()
        self.assertEqual((out["filed"], out["deferred"], out["pending_clear_failed"]), (0, 1, 0))
        self.assertTrue(self.ship_no().startswith("起票中:"))                     # claim 維持
        self.assertTrue(hs.is_pending_dedupe(self.shipping["100"]))                # pending 維持
        self.assertEqual([u for u in self.updates if u[0] == "APP_SHIPPING"], [])   # App 30 書込 0
        self.assertEqual([t for t in self.texts() if t.startswith(hs.NOTICE_HEAD_REVIEW)], [])   # 通知なし
        fail["on"] = False
        self.advance(hs.CLAIM_TTL_SEC + 1)
        out2 = self.run_soufu()
        self.assertEqual((out2["aligned"], out2["recovered"]), (1, 1))
        self.assertFalse(hs.is_pending_dedupe(self.shipping["100"]))
        self.assertEqual(self.ship_no(), "100")
        self.assertEqual(len(self.shipping), 1)

    def test_clear_pending_merges_real_data_with_revision(self):
        self.shipping["100"] = self._dup("100")
        meta = json.loads(self.shipping["100"]["チャネル固有データ"]["value"])
        meta["extra"] = {"kept": True}
        self.shipping["100"]["チャネル固有データ"] = {"value": json.dumps(meta, ensure_ascii=False)}
        stale = copy.deepcopy(self.shipping["100"])
        stale["チャネル固有データ"] = {"value": json.dumps({"houki_soufu_key": "houki_soufu:1:11", "pending_dedupe": True})}
        self.assertTrue(asyncio.run(hs._clear_pending_cas(stale)))                # 検索結果が古くても実物から読む
        after = json.loads(self.shipping["100"]["チャネル固有データ"]["value"])
        self.assertEqual(after, {**meta, "pending_dedupe": False})
        self.assertEqual(self.updates[-1][3], "1")

    def test_module_never_writes_app30_without_revision(self):
        """全置換・revision なしの App 30 書込経路が無いことを AST で pin。"""
        import ast as _ast
        tree = _ast.parse(Path(hs.__file__).read_text(encoding="utf-8"))
        calls = [n for n in _ast.walk(tree) if isinstance(n, _ast.Call)
                 and isinstance(n.func, _ast.Attribute) and n.func.attr == "update_record"
                 and n.args and isinstance(n.args[0], _ast.Name) and n.args[0].id == "APP_SHIPPING"]
        self.assertTrue(calls)
        for c in calls:
            self.assertIn("revision", [k.arg for k in c.keywords])

    def test_pending_clear_cas_fails_twice_keeps_claim_no_number_then_succeeds(self):
        """解除 CAS が 2 回失敗 → 番号を書かない・行は 起票中・pending_clear_failed・通知文の区別 →
        競合解消後の再実行で解除・番号確定・prepare が通る。"""
        self.seed(_case())
        self.ship_conflict_on_nth.update({1, 2})
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["pending_clear_failed"], out["duplicates_unresolved"]), (0, 1, 0))
        self.assertTrue(self.ship_no().startswith("起票中:"))
        self.assertTrue(hs.is_pending_dedupe(self.shipping["100"]))
        review = [t for t in self.texts() if t.startswith(hs.NOTICE_HEAD_REVIEW)]
        self.assertEqual(len(review), 1)
        self.assertIn("発送管理 No.100 の準備待ち解除に失敗しました。自動で再試行します", review[0])
        self.assertNotIn("二重起票", review[0])
        filed = [t for t in self.texts() if t.startswith(hs.NOTICE_HEAD_FILED)]
        self.assertIn("起票 0 件 / 重複未解決 0 件 / 解除再試行待ち 1 件", filed[0])
        self.assertEqual(self.cases["1"]["status"]["value"], "受理")            # 起票 0 のため遷移しない
        # 競合解消後（TTL 経過で回収）
        self.advance(hs.CLAIM_TTL_SEC + 1)
        self.admin.reset_mock()
        out2 = self.run_soufu()
        self.assertEqual((out2["aligned"], out2["recovered"], out2["pending_clear_failed"]), (1, 1, 0))
        self.assertFalse(hs.is_pending_dedupe(self.shipping["100"]))
        self.assertEqual(self.ship_no(), "100")
        self.assertEqual(len(self.shipping), 1)
        self.shipping["100"].update({"ユニット種別": {"value": "相続放棄"}, "チャネル": {"value": "送付案内"},
                                     "実行済み": {"value": "no"}, "本文_特記事項": {"value": ""}})
        approval = self._run_prepare()
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "承認待ち")
        approval.assert_awaited_once()

    def test_numbered_row_with_pending_is_recleared_next_run(self):
        """B-3 保険: 番号ありで pending が残った行 → 次回実行で解除される。"""
        self.seed(_case(status="債権者通知", rows=[_row("11", "甲社", state="起票済", ship_no="100"),
                                                  _row("12", "乙社", state="送付済", ship_no="101")]))
        self.shipping["100"] = self._dup("100")
        self.shipping["101"] = self._dup("101", pending=True, key="houki_soufu:1:12")
        out = self.run_soufu()
        self.assertEqual((out["pending_recleared"], out["filed"]), (1, 0))
        self.assertFalse(hs.is_pending_dedupe(self.shipping["100"]))
        self.assertTrue(hs.is_pending_dedupe(self.shipping["101"]))               # 送付済 は対象外
        self.assertEqual(self.ship_no(), "100")

    def test_numbered_row_without_pending_costs_one_get_and_no_write(self):
        self.seed(_case(status="債権者通知", rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        self.shipping["100"] = self._dup("100", pending=False)
        out = self.run_soufu()
        self.assertEqual((out["pending_recleared"], out["skip"]), (0, "no_target_rows"))
        self.assertEqual([u for u in self.updates if u[0] == "APP_SHIPPING"], [])


class TestRecoveryDedupe(_Base):
    """fix3 B: 番号復旧でも重複確認。"""

    def _stale_row(self, **kw):
        kw.setdefault("state", "起票済")
        kw.setdefault("ship_no", hs.claim_value("dead0000beef", self.now - timedelta(seconds=hs.CLAIM_TTL_SEC + 5)))
        return _row("11", "甲社", **kw)

    def test_two_pending_drafts_recovered_min_is_canonical(self):
        self.seed(_case(status="債権者通知", rows=[self._stale_row()]))
        self.shipping["50"] = self._dup("50")
        self.shipping["60"] = self._dup("60")
        self.next_ship = 200
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["aligned"], out["recovered"], out["duplicates_unresolved"]), (0, 1, 1, 0))
        self.assertEqual(set(self.shipping), {"50", "60"})                    # 新規作成なし
        self.assertEqual(self.shipping["50"]["発送ステータス"]["value"], "下書き")
        self.assertFalse(hs.is_pending_dedupe(self.shipping["50"]))
        self.assertEqual(self.shipping["60"]["発送ステータス"]["value"], "エラー")
        self.assertEqual(self.ship_no(), "50")
        self.assertEqual([t for t in self.texts() if "二重起票" in t], [])    # 解消済み＝通知なし

    def test_recovery_with_approved_duplicate_is_unresolved(self):
        self.seed(_case(status="債権者通知", rows=[self._stale_row()]))
        self.shipping["50"] = self._dup("50")
        self.shipping["60"] = self._dup("60", status="承認待ち", pending=False)
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["aligned"], out["duplicates_unresolved"]), (0, 0, 1))
        self.assertEqual(self.shipping["60"]["発送ステータス"]["value"], "承認待ち")
        self.assertEqual(self.ship_no(), "50")                               # 番号は書く
        review = [t for t in self.texts() if "二重起票の疑い" in t]
        self.assertEqual(len(review), 1)
        self.assertIn("No.50", review[0])
        self.assertIn("No.60", review[0])
        filed = [t for t in self.texts() if t.startswith(hs.NOTICE_HEAD_FILED)]
        self.assertIn("起票 0 件 / 重複未解決 1 件 / 解除再試行待ち 0 件", filed[0])

    def test_create_then_lost_response_recovered_once_with_pending_cleared(self):
        """作成成功・応答喪失 → 起票中 残留 → 期限切れ後の回収で 1 件のみ・pending 解除・番号確定。"""
        self.seed(_case())
        real_create = hub_kintone.create_record

        async def create_then_lose(app, fields):
            await real_create(app, fields)
            raise asyncio.TimeoutError()                                     # 応答喪失（レコードは出来ている）
        with patch.object(hub_kintone, "create_record", create_then_lose):
            out = self.run_soufu()
        self.assertEqual((out["filed"], out["pending"]), (0, 1))
        self.assertTrue(self.ship_no().startswith("起票中:"))
        self.assertTrue(hs.is_pending_dedupe(self.shipping["100"]))
        self.advance(hs.CLAIM_TTL_SEC + 1)
        self.admin.reset_mock()
        out2 = self.run_soufu()
        self.assertEqual((out2["filed"], out2["aligned"], out2["recovered"]), (0, 1, 1))
        self.assertEqual(set(self.shipping), {"100"})
        self.assertFalse(hs.is_pending_dedupe(self.shipping["100"]))
        self.assertEqual(self.ship_no(), "100")
        self.assertEqual(self.cases["1"]["status"]["value"], "債権者通知")


class TestRecoveryEligibility(_Base):
    """fix2 B: 回収時の新規作成条件。"""

    def _stale(self, **kw):
        kw.setdefault("state", "起票済")
        kw.setdefault("ship_no", hs.claim_value("dead0000beef", self.now - timedelta(seconds=hs.CLAIM_TTL_SEC + 5)))
        return _row("11", "甲社", **kw)

    def test_notify_changed_to_not_required_reverts_row(self):
        self.seed(_case(rows=[self._stale(notify="不要")]))
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["reverted"]), (0, 1))
        self.assertEqual(self.shipping, {})
        row = self.rows()[0]["value"]
        self.assertEqual((row["送付状態"]["value"], row["送付発送管理No"]["value"]), ("未", ""))
        review = [t for t in self.texts() if "取り消しました" in t]
        self.assertEqual(len(review), 1)
        self.assertIn("案件レコードNo.1 行 1: 起票中でしたが通知要否/住所の条件を満たさないため取り消しました", review[0])
        self.assertNotIn("甲社", review[0])

    def test_address_cleared_reverts_row(self):
        self.seed(_case(rows=[self._stale(addr="")]))
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["reverted"]), (0, 1))
        self.assertEqual((self.rows()[0]["value"]["送付状態"]["value"], self.ship_no()), ("未", ""))

    def test_eligible_stale_row_is_created(self):
        self.seed(_case(rows=[self._stale()]))
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["recovered"], out["reverted"]), (1, 1, 0))
        self.assertEqual(self.ship_no(), "100")

    def test_number_recovery_does_not_check_eligibility(self):
        self.seed(_case(rows=[self._stale(notify="不要")]))
        self.shipping["100"] = {"$id": {"value": "100"}, "$revision": {"value": "1"},
                                "チャネル固有データ": {"value": json.dumps({"houki_soufu_key": "houki_soufu:1:11"})}}
        out = self.run_soufu()
        self.assertEqual((out["aligned"], out["reverted"]), (1, 0))
        self.assertEqual(self.ship_no(), "100")


class TestNotifyingStatusEntry(_Base):
    """fix1 B: 債権者通知 でも未 行の起票と回収を行う。"""

    def test_webhook_gate_accepts_notifying_status(self):
        self.seed(_case(status="債権者通知", rows=[_row("11", "甲社")]))
        resp = _client.post(_URL, json=_body(status="債権者通知"))
        self.assertEqual(resp.json()["queued"], True)
        self.assertEqual(len(self.shipping), 1)

    def test_address_added_later_is_filed_under_notifying_status(self):
        self.seed(_case(rows=[_row("11", "甲社"), _row("12", "乙社", addr="")]))
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["promoted"]), (1, True))
        self.assertEqual(self.cases["1"]["status"]["value"], "債権者通知")
        self.assertEqual(self.rows()[1]["value"]["送付状態"]["value"], "未")
        # 2 行目の住所を補って保存 → 債権者通知 のまま起票される
        self.rows()[1]["value"]["債権者住所"] = {"value": "大阪府テスト市2-2"}
        self.admin.reset_mock()
        out2 = self.run_soufu()
        self.assertEqual((out2["filed"], out2["promoted"]), (1, False))
        self.assertEqual(self.cases["1"]["status"]["value"], "債権者通知")
        self.assertEqual({r["宛先名"]["value"] for r in self.shipping.values()}, {"甲社", "乙社"})
        self.assertEqual(self.rows()[1]["value"]["送付発送管理No"]["value"], "101")


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

    def test_status_promote_cas_conflict_retries_once(self):
        self.seed(_case())
        self.conflict_on_nth.add(3)                                  # status 遷移の初回を 409 に
        out = self.run_soufu()
        self.assertTrue(out["promoted"])                             # 再取得後に遷移
        self.assertEqual(self.cases["1"]["status"]["value"], "債権者通知")

    def test_promote_only_when_currently_accepted(self):
        """fix1 B-3: 債権者通知 のときに起票が増えても status は触らない。"""
        self.seed(_case(status="債権者通知", rows=[_row("11", "甲社")]))
        out = self.run_soufu()
        self.assertEqual((out["filed"], out["promoted"]), (1, False))
        self.assertEqual(self.cases["1"]["status"]["value"], "債権者通知")
        self.assertEqual([u for u in self.updates if "status" in u[2]], [])


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

    def test_write_back_get_timeout_notifies_once_and_does_not_raise(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        self.shipping["100"] = self._ship()
        real_get = hub_kintone.get_record

        async def get(app, rid):
            if app.app_id_env == "APP_HOUKI":
                raise asyncio.TimeoutError()
            return await real_get(app, rid)
        with patch.object(hub_kintone, "get_record", get), \
                patch.object(hub_notify, "notify_attorney_approval", AsyncMock()):
            asyncio.run(hub_dispatch.process_dispatch("100"))          # 例外は伝播しない
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "完了")
        review = [t for t in self.result_texts() if t.startswith(hs.NOTICE_HEAD_REVIEW)]
        self.assertEqual(len(review), 1)
        self.assertIn("案件レコードNo.1", review[0])
        self.assertIn("発送管理 No.100", review[0])
        self.assertIn("送付状態 を手で 送付済 にしてください", review[0])
        self.assertNotIn("甲社", review[0])
        self.assertEqual(self.rows()[0]["value"]["送付状態"]["value"], "起票済")

    def test_write_back_update_exception_notifies_once(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        self.shipping["100"] = self._ship()
        real_update = hub_kintone.update_record

        async def update(app, rid, fields, revision=None):
            if app.app_id_env == "APP_HOUKI":
                raise ConnectionError("reset")
            return await real_update(app, rid, fields, revision)
        with patch.object(hub_kintone, "update_record", update), \
                patch.object(hub_notify, "notify_attorney_approval", AsyncMock()):
            asyncio.run(hub_dispatch.process_dispatch("100"))
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "完了")
        self.assertEqual(len([t for t in self.result_texts() if t.startswith(hs.NOTICE_HEAD_REVIEW)]), 1)

    def test_write_back_notify_exception_logs_error_without_raise(self):
        import logging
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        self.shipping["100"] = self._ship()

        async def get(app, rid):
            raise asyncio.TimeoutError()
        self.admin_result.side_effect = RuntimeError("line down")
        with patch.object(hub_kintone, "get_record", get), \
                self.assertLogs("hub.houki_soufu", level=logging.ERROR) as cm:
            self.assertFalse(asyncio.run(hs.write_back_safely(self._ship())))
        self.assertTrue(any("write-back notify failed" in m for m in cm.output))
        self.assertFalse(any("甲社" in m for m in cm.output))

    def _real_result_notify(self, push_ok):
        push = AsyncMock(return_value=push_ok)
        for p in (patch.object(hub_notify, "notify_admin_line_result", _REAL_NOTIFY_RESULT),
                  patch.object(hub_notify, "push_line_message", push),
                  patch.object(hub_notify, "get_admin_line_user_id", return_value="Uadmin")):
            p.start()
            self.addCleanup(p.stop)
        hub_notify._last_notify_at.clear()
        hub_notify._notify_in_flight.clear()
        self.addCleanup(hub_notify._last_notify_at.clear)
        return push

    def test_write_back_notice_failed_result_logs_error_with_numbers(self):
        """fix2 C: 実 notify_admin_line_result・push が False → ERROR ログ 1 行（案件番号+発送管理番号）・例外なし。"""
        import logging
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        push = self._real_result_notify(False)

        async def get(app, rid):
            raise asyncio.TimeoutError()
        with patch.object(hub_kintone, "get_record", get), \
                self.assertLogs("hub.houki_soufu", level=logging.ERROR) as cm:
            self.assertFalse(asyncio.run(hs.write_back_safely(self._ship())))
        lines = [m for m in cm.output if "write-back notice not delivered" in m]
        self.assertEqual(len(lines), 1)
        self.assertIn("case=1", lines[0])
        self.assertIn("shipping=100", lines[0])
        self.assertEqual(push.await_count, 1)

    def test_write_back_notice_sent_and_throttled_have_no_error(self):
        import logging
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        push = self._real_result_notify(True)

        async def get(app, rid):
            raise asyncio.TimeoutError()
        with patch.object(hub_kintone, "get_record", get), \
                self.assertLogs("hub.houki_soufu", level=logging.ERROR) as cm:
            asyncio.run(hs.write_back_safely(self._ship()))            # sent
            asyncio.run(hs.write_back_safely(self._ship()))            # throttled（同キー・窓内）
        self.assertEqual(push.await_count, 1)
        self.assertFalse(any("not delivered" in m or "notify failed" in m for m in cm.output))
        self.assertEqual(len([m for m in cm.output if "write-back exception" in m]), 2)

    def test_jikou_shipping_never_touches_app40_even_on_exception_paths(self):
        self.seed(_case(rows=[_row("11", "甲社", state="起票済", ship_no="100")]))
        self.shipping["100"] = self._ship(app_id="21", meta={"needs_return": False})

        async def get(app, rid):
            if app.app_id_env == "APP_HOUKI":
                self.fail("App 40 must not be read for jikou shipping")
            return copy.deepcopy(self.shipping[str(rid)])
        with patch.object(hub_kintone, "get_record", get), \
                patch.object(hub_notify, "notify_attorney_approval", AsyncMock()):
            asyncio.run(hub_dispatch.process_dispatch("100"))
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "完了")
        self.assertEqual(self.admin.await_count, 0)

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

    def test_prepare_recipient_mismatch_defers_with_review_and_no_artifacts(self):
        """fix1 C-2: 起票後に App 40 の行の住所を訂正 → prepare は成果物 0・要確認通知・PrepareDeferred。"""
        from channels.base import PrepareDeferred
        self.seed(_case(rows=[_row("11", "甲社", addr="東京都千代田区9-9")]))     # App 30 は 1-1 のまま
        blocks = [{"ブロックキー": {"value": "受理通知書写し"}, "表示名": {"value": "x"}, "案内文": {"value": ""},
                   "対象ユニット": {"value": ["相続放棄"]}, "返送要否": {"value": "不要"}, "表示順": {"value": "1"}}]
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=blocks)), \
                patch.object(soufu_annai, "render_label_sheet", lambda *a, **k: self.fail("label must not render")):
            with self.assertRaises(PrepareDeferred):
                asyncio.run(soufu_annai.SoufuAnnaiAdapter().prepare(self._shipping()))
        self.assertEqual(self.kinds(), ["houki_soufu_needs_review"])
        text = self.texts()[0]
        self.assertIn("案件レコードNo.1 行 1: 起票後に宛先が変更されています。App 30 の宛先を直すか、行を 未 に戻して再起票してください。", text)
        for pii in ("千代田", "甲社", "申述太郎"):
            self.assertNotIn(pii, text)

    def test_prepare_mismatch_via_dispatcher_keeps_draft(self):
        self.seed(_case(rows=[_row("11", "甲社", zip_="999-9999")]))
        ship = self._shipping()
        ship.update({"$revision": {"value": "1"}, "発送ステータス": {"value": "下書き"}, "実行済み": {"value": "no"}})
        self.shipping["100"] = ship
        blocks = [{"ブロックキー": {"value": "受理通知書写し"}, "表示名": {"value": "x"}, "案内文": {"value": ""},
                   "対象ユニット": {"value": ["相続放棄"]}, "返送要否": {"value": "不要"}, "表示順": {"value": "1"}}]
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=blocks)), \
                patch.object(hub_kintone, "upload_file", AsyncMock(return_value="fk")), \
                patch.object(hub_notify, "notify_attorney_approval", AsyncMock()) as approval:
            asyncio.run(hub_dispatch.process_dispatch("100"))
        self.assertEqual(self.shipping["100"]["発送ステータス"]["value"], "下書き")
        approval.assert_not_awaited()
        self.assertNotIn("成果物", self.shipping["100"])

    def test_prepare_recipient_strings_identical_in_letter_and_label(self):
        self.seed(_case())
        blocks = [{"ブロックキー": {"value": "受理通知書写し"}, "表示名": {"value": "x"}, "案内文": {"value": ""},
                   "対象ユニット": {"value": ["相続放棄"]}, "返送要否": {"value": "不要"}, "表示順": {"value": "1"}}]
        labels = []

        def fake_labels(addresses, layout="A4_2x6", **kw):
            labels.append(addresses)
            return b"%PDF-labels"
        ship = self._shipping()
        with patch.object(hub_kintone, "search_records", AsyncMock(return_value=blocks)), \
                patch.object(soufu_annai, "render_label_sheet", fake_labels):
            result = asyncio.run(soufu_annai.SoufuAnnaiAdapter().prepare(ship))
        doc = Document(io.BytesIO(result.artifacts[0].content))
        paras = [p.text for p in doc.paragraphs]
        self.assertEqual(paras[1:4], [f"〒{ship['宛先郵便番号']['value']}", ship["宛先住所"]["value"],
                                      f"　　　{ship['宛先名']['value']}　御中"])
        self.assertEqual(labels[0][0]["住所"], ship["宛先住所"]["value"])
        self.assertEqual(labels[0][0]["郵便番号"], ship["宛先郵便番号"]["value"])
        self.assertEqual(labels[0][0]["宛先名"], ship["宛先名"]["value"])
        self.assertEqual(self.admin.await_count, 0)

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
        self.assertEqual(hs.CLAIM_TTL_SEC, 600)
        self.assertEqual(hs.SHIP_NO_CLAIMING, "起票中")

    def test_constants_pinned(self):
        self.assertEqual((hs.STATUS_ACCEPTED, hs.STATUS_NOTIFYING, hs.STATUS_COMPLETED), ("受理", "債権者通知", "完了"))
        self.assertEqual((hs.NOTIFY_REQUIRED, hs.STATE_TODO, hs.STATE_FILED, hs.STATE_SENT), ("要", "未", "起票済", "送付済"))
        self.assertEqual(L.REQUIRED_CASE_FIELDS, ("顧客名", "住所", "生年月日", "被相続人氏名", "被相続人最後の住所",
                                                  "被相続人生年月日", "死亡日", "管轄家庭裁判所", "事件番号", "受理日"))
        self.assertNotIn("死亡日_申告", L.REQUIRED_CASE_FIELDS)


if __name__ == "__main__":
    unittest.main()
