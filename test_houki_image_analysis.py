"""HOUKI-IMG-2: 書類写真の AI 読解を相続放棄チャネルへ展開（hub/image_analysis の
ChannelConfig 化・houki cfg）。

固定する仕様:
- 起点=image_intake.send_receipt_and_close の分岐（houki は HOUKI cfg・時効は既定 cfg）
- houki 用 tool スキーマ（閉集合・additionalProperties false・サーバ側でキー集合の
  完全一致を検証）・凍結 system prompt（sha256 pin）
- 2 通目=弁護士文言（空行を含め逐語・sha256 pin）。表示規則: 債権者名は high+名称
  検証（代理人・回収受託者は除く・「A」と「B」・最大 3）／裁判所書類は 6 種の固定語
  のみ／死亡日は YYYY-MM-DD 完全一致・実在・未来日でない・high のみ「YYYY年M月D日」／
  固定順・0 件は送らない・原本保管文は裁判所書類の行があるときだけ・質問なし
- App 40 転記: 債権者名→append_creditors（CAS）・死亡日_申告→空欄のみ CAS。
  訴訟督促有無・財産処分有無・財産_負債 は書かない
- 弁護士通知 3 種（court/notice/disposition）は送信の成否・抑止と独立
- 送信直前の再取得（fail-closed）・人対応/pause/停止リスト・マーカーは時効と同型
- 時効側の既存テスト（pin 含む）は無変更で green
"""

import asyncio
import datetime
import hashlib
import os
import unittest
from unittest.mock import AsyncMock, patch

from test_image_analysis import _ENV, _FakeStore, _tool_response  # noqa: F401,E402

for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from hub import houki_case_store  # noqa: E402
from hub import image_analysis as ia  # noqa: E402
from hub import image_intake as ii  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import notify as hub_notify  # noqa: E402
from hub import reply_sanitizer  # noqa: E402
from hub.line_channel import HOUKI_CHANNEL, JIKOU_CHANNEL  # noqa: E402

JPEG = b"\xff\xd8\xff\xe0" + b"J" * 32
UID = "U_houki_img2"
EVT = "evt-h2"
TODAY = datetime.date(2026, 9, 5)


def _run(coro):
    return asyncio.run(coro)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _hrep(creditors=(), court="なし", death=None, death_conf="low",
          inh_kind="なし", inh_date=None, timing="不明", disp="なし",
          legible=True):
    return {"creditors": [dict(c) for c in creditors],
            "court_document": court, "death_date": death,
            "death_date_confidence": death_conf,
            "inheritance_document": {"kind": inh_kind, "document_date": inh_date,
                                     "knowledge_timing": timing},
            "possible_disposition_document": disp, "legible": legible}


def _hc(name, role="原債権者", kind="民間債権", confidence="high"):
    return {"name": name, "role": role, "kind": kind, "confidence": confidence}


class _FakeApp40(_FakeStore):
    """App 40 相当: 債権者一覧 SUBTABLE・死亡日_申告・response_mode を持つ。"""

    def seed_case(self, fields: dict, file_keys: list[str], contents=None) -> str:
        rid = super().seed_case(fields, file_keys, contents)
        rec = self.cases[rid]
        for code in ("死亡日_申告", "訴訟督促有無", "財産処分有無", "財産_負債",
                     "死亡日", "起算日_確定", "死亡を知った日_申告",
                     "相続人と知った日_申告", "相続の開始を知った日"):
            rec.setdefault(code, {"value": ""})
        rec.setdefault("債権者一覧", {"value": []})
        return rid

    def creditors(self, rid):
        return [r["value"]["債権者名"]["value"]
                for r in self.cases[str(rid)]["債権者一覧"]["value"]]

    def analyzed_keys(self):
        p = ia.ANALYZED_PREFIX + "houki:"
        return [r["category"][len(p):] for r in self.chatlog
                if str(r.get("category", "")).startswith(p)]


class _Base(unittest.TestCase):
    def setUp(self):
        ia._claims.clear()
        self.addCleanup(ia._claims.clear)
        self.store = _FakeApp40()
        self.push = AsyncMock(return_value=True)
        self.admin = AsyncMock(return_value=True)
        self.ai = AsyncMock(return_value=_tool_response(_hrep(
            [_hc("アコム")], court="支払督促", death="2024-05-01",
            death_conf="high"), name="report_documents"))
        for p in (patch.object(hub_kintone, "search_records", self.store.search_records),
                  patch.object(hub_kintone, "create_record", self.store.create_record),
                  patch.object(hub_kintone, "get_record", self.store.get_record),
                  patch.object(hub_kintone, "update_record", self.store.update_record),
                  patch.object(hub_kintone, "download_file", self.store.download_file),
                  patch.object(ia, "push_text", self.push),
                  patch.object(ia, "create_message_with_fallback", self.ai),
                  patch.object(ia.notify, "notify_admin_line", self.admin),
                  patch.object(hub_notify, "notify_admin_line", self.admin),
                  patch.object(ia, "is_suppressed", AsyncMock(return_value=False)),
                  patch.object(ia.datetime, "date", _FixedDate),
                  patch.dict(os.environ, {"AUTOREPLY_PAUSED": "0"})):
            p.start()
            self.addCleanup(p.stop)

    def seed(self, keys=("k1",), **fields):
        base = {"LINEユーザーID": UID, "response_mode": "自動"}
        base.update(fields)
        return self.store.seed_case(base, list(keys))

    def set_ai(self, **kw):
        self.ai.return_value = _tool_response(_hrep(**kw), name="report_documents")

    def go(self, event_id=EVT):
        return _run(ia.analyze_and_reply(UID, event_id, ia.HOUKI))

    def sent_text(self) -> str:
        self.push.assert_awaited_once()
        self.assertIs(self.push.await_args.args[0], HOUKI_CHANNEL)
        self.assertEqual(self.push.await_args.args[1], UID)
        return self.push.await_args.args[2]

    def notify_kinds(self):
        return [c.kwargs["throttle_key"].split(":", 1)[0]
                for c in self.admin.await_args_list]


class _FixedDate(datetime.date):
    @classmethod
    def today(cls):
        return TODAY


LAWYER_TEXT = (
    "お写真をありがとうございます。\n"
    "お写真からは、次の内容が読み取れました。\n"
    "\n"
    "・債権者名：「〇〇」\n"
    "・裁判所から届いた書類：「支払督促」\n"
    "・亡くなられた方の死亡日：「2024年5月1日」\n"
    "\n"
    "読み取りに誤りがある場合は、正しい内容をお知らせください。\n"
    "裁判所から届いた書類の原本は、そのまま保管してください。")


# ── 1〜2: 文言の逐語と 3 形 ──────────────────────────────────────────────────────
class TestReplyText(_Base):
    def test_1_all_three_items_verbatim(self):
        rid = self.seed()
        self.set_ai(creditors=[_hc("テスト")], court="支払督促",
                    death="2024-05-01", death_conf="high")
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.sent_text(), LAWYER_TEXT.replace("〇〇", "テスト"))
        # 組立関数でも同一
        items = {"creditor_names": ["〇〇"], "court": "支払督促",
                 "death_date": datetime.date(2024, 5, 1)}
        self.assertEqual(ia.compose_houki_reply(items), LAWYER_TEXT)
        self.assertEqual(self.store.creditors(rid), ["テスト"])
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "2024-05-01")
        self.assertEqual(self.notify_kinds(), ["houki_image_analysis_court"])

    def test_2_single_item_forms(self):
        # 債権者名のみ
        self.seed()
        self.set_ai(creditors=[_hc("アコム")])
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.sent_text(),
                         "お写真をありがとうございます。\n"
                         "お写真からは、次の内容が読み取れました。\n\n"
                         "・債権者名：「アコム」\n\n"
                         "読み取りに誤りがある場合は、正しい内容をお知らせください。")
        # 裁判所書類のみ（原本保管文あり）
        self.setUp()
        self.seed()
        self.set_ai(court="訴状")
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.sent_text(),
                         "お写真をありがとうございます。\n"
                         "お写真からは、次の内容が読み取れました。\n\n"
                         "・裁判所から届いた書類：「訴状」\n\n"
                         "読み取りに誤りがある場合は、正しい内容をお知らせください。\n"
                         "裁判所から届いた書類の原本は、そのまま保管してください。")
        # 死亡日のみ（原本保管文なし）
        self.setUp()
        self.seed()
        self.set_ai(death="2023-12-31", death_conf="high")
        self.assertEqual(self.go(), "sent")
        text = self.sent_text()
        self.assertEqual(text,
                         "お写真をありがとうございます。\n"
                         "お写真からは、次の内容が読み取れました。\n\n"
                         "・亡くなられた方の死亡日：「2023年12月31日」\n\n"
                         "読み取りに誤りがある場合は、正しい内容をお知らせください。")
        self.assertNotIn("原本", text)
        self.assertNotIn("？", text)                    # 質問は付けない


# ── 3〜4: 債権者・裁判所書類の規則 ────────────────────────────────────────────────
class TestCreditorAndCourtRules(_Base):
    def test_3_multiple_agent_and_tax(self):
        rid = self.seed()
        self.set_ai(creditors=[_hc("アコム"),
                               _hc("川口市", kind="公租公課"),
                               _hc("山田法律事務所", role="代理人・回収受託者")])
        self.assertEqual(self.go(), "sent")
        text = self.sent_text()
        self.assertIn("・債権者名：「アコム」と「川口市」", text)    # 公租公課も名称表示
        self.assertNotIn("山田法律事務所", text)                   # 代理人は表示しない
        self.assertEqual(self.store.creditors(rid), ["アコム", "川口市"])   # 一覧にも書かない
        # 重複除去・譲受人も債権者として表示（4 件は parse で拒否=別テスト）
        self.setUp()
        rid = self.seed()
        self.set_ai(creditors=[_hc("A"), _hc("B", role="譲受人"), _hc("A")])
        self.assertEqual(self.go(), "sent")
        self.assertIn("・債権者名：「A」と「B」", self.sent_text())
        self.assertEqual(self.store.creditors(rid), ["A", "B"])
        self.setUp()
        rid = self.seed()
        self.set_ai(creditors=[_hc("http://x"), _hc("D", confidence="medium")])
        self.assertEqual(self.go(), "nothing_to_send")
        self.push.assert_not_awaited()
        self.assertEqual(self.store.creditors(rid), [])

    def test_4_court_other_notifies_without_display(self):
        self.seed()
        self.set_ai(court="その他")
        self.assertEqual(self.go(), "nothing_to_send")
        self.push.assert_not_awaited()
        self.assertEqual(self.notify_kinds(), ["houki_image_analysis_court"])
        self.assertIn("「その他」", self.admin.await_args.args[0])
        for court in ("なし", "不明"):
            with self.subTest(court=court):
                self.setUp()
                self.seed()
                self.set_ai(court=court)
                self.assertEqual(self.go(), "nothing_to_send")
                self.push.assert_not_awaited()
                self.admin.assert_not_awaited()
        for court in ia.HOUKI_COURT_DISPLAY:
            with self.subTest(court=court):
                self.setUp()
                self.seed()
                self.set_ai(court=court)
                self.assertEqual(self.go(), "sent")
                self.assertIn(f"・裁判所から届いた書類：「{court}」", self.sent_text())
                self.assertEqual(self.notify_kinds(), ["houki_image_analysis_court"])
                self.assertEqual(self.store.field("1", "訴訟督促有無"), "")   # 書かない


# ── 5: 死亡日 ───────────────────────────────────────────────────────────────────
class TestDeathDate(_Base):
    def test_5_conditions(self):
        cases = {
            "medium": dict(death="2024-05-01", death_conf="medium"),
            "invalid_day": dict(death="2024-02-30", death_conf="high"),
            "slash": dict(death="2024/05/01", death_conf="high"),
            "future": dict(death="2026-09-06", death_conf="high"),
            "null": dict(death=None, death_conf="high"),
        }
        for label, kw in cases.items():
            with self.subTest(case=label):
                self.setUp()
                rid = self.seed()
                self.set_ai(**kw)
                self.assertEqual(self.go(), "nothing_to_send")
                self.push.assert_not_awaited()
                self.assertEqual(self.store.field(rid, "死亡日_申告"), "")
        # 今日は未来日でない
        self.setUp()
        rid = self.seed()
        self.set_ai(death=TODAY.isoformat(), death_conf="high")
        self.assertEqual(self.go(), "sent")
        self.assertIn("「2026年9月5日」", self.sent_text())
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "2026-09-05")

    def test_5b_existing_value_not_overwritten_and_document_date_not_used(self):
        rid = self.seed(**{"死亡日_申告": "2020-01-01"})
        self.set_ai(death="2024-05-01", death_conf="high")
        self.assertEqual(self.go(), "sent")
        self.assertIn("「2024年5月1日」", self.sent_text())              # 表示はする
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "2020-01-01")  # 転記しない
        self.assertEqual(self.store.field(rid, "$revision"), "1")
        # 書面日付は死亡日に流用しない
        self.setUp()
        rid = self.seed()
        self.set_ai(inh_kind="相続放棄申述受理通知書", inh_date="2024-05-01",
                    death=None, death_conf="high")
        self.assertEqual(self.go(), "nothing_to_send")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "")
        self.assertEqual(self.notify_kinds(), ["houki_image_analysis_notice"])
        self.assertIn("書面日付 2024年5月1日", self.admin.await_args.args[0])
        # 死亡日（確定）・起算日_確定・知った日 3 欄には書かない
        self.setUp()
        rid = self.seed()
        self.set_ai(death="2024-05-01", death_conf="high")
        self.go()
        for code in ("死亡日", "起算日_確定", "死亡を知った日_申告",
                     "相続人と知った日_申告", "相続の開始を知った日"):
            self.assertEqual(self.store.field(rid, code), "", code)


# ── 6: 通知のみの項目・書かない欄 ────────────────────────────────────────────────
class TestNoticeOnlyItems(_Base):
    def test_6_inheritance_and_disposition_notify_only(self):
        rid = self.seed()
        self.set_ai(creditors=[_hc("アコム")], inh_kind="相続関係についての通知書",
                    inh_date=None, timing="該当", disp="あり")
        self.assertEqual(self.go(), "sent")
        text = self.sent_text()
        self.assertNotIn("相続関係", text)
        self.assertNotIn("処分", text)
        self.assertEqual(sorted(self.notify_kinds()),
                         ["houki_image_analysis_disposition",
                          "houki_image_analysis_notice"])
        notice = [c for c in self.admin.await_args_list
                  if "notice" in c.kwargs["throttle_key"]][0]
        self.assertIn("「相続関係についての通知書」", notice.args[0])
        self.assertIn("書面日付 不明", notice.args[0])
        self.assertIn(f"レコード番号 {rid}", notice.args[0])
        self.assertTrue(notice.kwargs["throttle_on_success_only"])
        self.assertNotIn("アコム", notice.args[0])
        for code in ("財産処分有無", "訴訟督促有無", "財産_負債"):
            self.assertEqual(self.store.field(rid, code), "", code)
        # HI2-02（票の訂正）: knowledge_timing 単独では発火しない（kind のみ）。
        # 旧 pin「kind=不明でも該当なら通知」は訂正により置換（緩和ではない）
        for kind in ("不明", "なし"):
            with self.subTest(kind=kind):
                self.setUp()
                self.seed()
                self.set_ai(inh_kind=kind, timing="該当")
                self.go()
                self.admin.assert_not_awaited()
        self.setUp()
        self.seed()
        self.set_ai(inh_kind="相続放棄申述受理通知書", timing="不明")
        self.go()
        self.assertEqual(self.notify_kinds(), ["houki_image_analysis_notice"])
        # 不明 は通知しない
        self.setUp()
        self.seed()
        self.set_ai(inh_kind="不明", timing="不明", disp="不明")
        self.go()
        self.admin.assert_not_awaited()


# ── 7: parse の閉集合検証 ───────────────────────────────────────────────────────
class TestHoukiParse(_Base):
    def test_7_negatives(self):
        ok = _hrep([_hc("アコム")], court="訴状")
        bad = {
            "unknown_top": {**ok, "debtor_name": "山田"},
            "missing_top": {k: v for k, v in ok.items() if k != "death_date"},
            "four": _hrep([_hc("A"), _hc("B"), _hc("C"), _hc("D")]),
            "unknown_elem": _hrep([{**_hc("A"), "amount": "1"}]),
            "missing_elem": _hrep([{"name": "A", "role": "原債権者",
                                    "confidence": "high"}]),
            "role_out": _hrep([_hc("A", role="サービサー")]),
            "kind_out": _hrep([_hc("A", kind="税")]),
            "court_out": _hrep(court="仮執行"),
            "death_type": {**ok, "death_date": 20240501},
            "death_conf_out": {**ok, "death_date_confidence": "sure"},
            "inh_unknown_key": {**ok, "inheritance_document":
                                {"kind": "なし", "document_date": None,
                                 "knowledge_timing": "不明", "x": 1}},
            "inh_kind_out": _hrep(inh_kind="通知"),
            "timing_out": _hrep(timing="はい"),
            "disp_bool": {**ok, "possible_disposition_document": True},
            "legible_str": {**ok, "legible": "true"},
            "not_dict": ["x"],
        }
        for label, inp in bad.items():
            with self.subTest(case=label):
                self.assertIsNone(ia.parse_houki_report(inp))
                self.setUp()
                rid = self.seed()
                self.ai.return_value = _tool_response(inp, name="report_documents")
                self.assertEqual(self.go(), "nothing_to_send")
                self.push.assert_not_awaited()
                self.admin.assert_not_awaited()
                self.assertEqual(self.store.creditors(rid), [])
        self.assertIsNotNone(ia.parse_houki_report(ok))
        self.assertIsNotNone(ia.parse_houki_report(_hrep()))
        schema = ia.HOUKI_REPORT_TOOL["input_schema"]
        self.assertIs(schema["additionalProperties"], False)
        self.assertIs(schema["properties"]["creditors"]["items"]["additionalProperties"],
                      False)
        self.assertIs(schema["properties"]["inheritance_document"]["additionalProperties"],
                      False)
        self.assertEqual(sorted(schema["properties"]),
                         ["court_document", "creditors", "death_date",
                          "death_date_confidence", "inheritance_document",
                          "legible", "possible_disposition_document"])


# ── 8〜9: 送信直前の再取得・通知のみ ─────────────────────────────────────────────
class TestGates(_Base):
    def test_8_human_switch_during_ai_blocks_but_notifies(self):
        rid = self.seed()

        async def ai(*_a, **_k):
            self.store.cases[rid]["response_mode"] = {"value": "人対応"}
            return _tool_response(_hrep([_hc("アコム")], court="訴状"),
                                  name="report_documents")
        self.ai.side_effect = ai
        self.assertEqual(self.go(), "blocked")
        self.push.assert_not_awaited()
        self.assertEqual(self.store.analysis_rows(), [])
        self.assertEqual(self.store.analyzed_keys(), [])
        self.assertEqual(self.notify_kinds(), ["houki_image_analysis_court"])
        self.assertEqual(self.store.creditors(rid), [])              # 転記も送信後のみ
        # 再取得失敗 → push 0・マーカー 0
        self.setUp()
        self.seed()
        with patch.object(ia, "_refetch_record", AsyncMock(return_value=None)):
            self.assertEqual(self.go(), "recheck_failed")
        self.push.assert_not_awaited()
        self.assertEqual(self.store.analysis_rows(), [])
        # pause・停止リスト
        self.setUp()
        self.seed()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            self.assertEqual(self.go(), "blocked")
        self.setUp()
        self.seed()
        with patch.object(ia, "is_suppressed", AsyncMock(return_value=True)):
            self.assertEqual(self.go(), "blocked")
        self.push.assert_not_awaited()

    def test_9_notice_only_no_send_no_marker(self):
        self.seed()
        self.set_ai(disp="あり")
        self.assertEqual(self.go(), "nothing_to_send")
        self.push.assert_not_awaited()
        self.assertEqual(self.store.analysis_rows(), [])
        self.assertEqual(self.store.analyzed_keys(), [])
        self.admin.assert_awaited_once()
        self.assertEqual(self.notify_kinds(), ["houki_image_analysis_disposition"])

    def test_send_failure_and_markers(self):
        self.seed(keys=["k1", "k2"])
        self.push.return_value = False
        self.assertEqual(self.go(), "send_failed")
        self.assertEqual(self.store.analysis_rows(), [])
        self.assertEqual([k for k in self.notify_kinds() if "send_failure" in k],
                         ["houki_image_analysis_send_failure"])
        self.setUp()
        self.seed(keys=["k1", "k2"])
        self.assertEqual(self.go(), "sent")
        rows = self.store.analysis_rows()
        self.assertEqual(rows[0]["category"], f"画像解析:houki:{EVT}")
        self.assertEqual(rows[0]["message"], self.push.await_args.args[2])
        self.assertEqual(sorted(self.store.analyzed_keys()), ["k1", "k2"])
        self.assertTrue(all(c.startswith("画像解析済:houki:")
                            for c in [r["category"] for r in self.store.chatlog
                                      if "画像解析済" in r["category"]]))
        ia._claims.clear()
        self.push.reset_mock()
        self.assertEqual(self.go("evt-next"), "no_files")           # 再解析されない

    def test_store_failure_notifies(self):
        rid = self.seed()
        self.store.update_error = hub_kintone.KintoneError(403, "GAIA_NO01", "x")
        self.assertEqual(self.go(), "sent")
        self.assertIn("houki_image_analysis_store", self.notify_kinds())
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "")


# ── 10〜12: cfg 分離・台本整合・pin ───────────────────────────────────────────────
class TestConfigSeparationAndPins(_Base):
    HOUKI_SYSTEM_SHA256 = "fcdd9aaea1b5cd395c56fbfd61c497f8ce70c3e38b3e734291d6a15164231ed2"
    HOUKI_TEMPLATE_SHA256 = "26e1d1081a42e8cff547dfd6d442c52434a9d2d3c6a9e17abe084f46dc2e8ffa"
    # 時効側 4 種（不変・test_image_analysis と同値）
    JIKOU_SYSTEM_SHA256 = "538e10dd53dbfa93680d1287f00df51fd08e8e213a7431e14fafc824f13ad935"
    JIKOU_TEMPLATE_SHA256 = "1944ddd9d8e9ad57c1b124ff5e45847c03589a07bf77e8272df2b4f5704adcd5"
    JIKOU_LINES_SHA256 = "430b0cb50ab864520f28770bc235c264efbb011a440a07828291233690043410"
    JIKOU_QUESTIONS_SHA256 = "bb13f98727cb7172a2ecccfca7ea0b4a92714dfdde1f81b48d3a8dfee8c1f106"

    def test_10_houki_uses_houki_cfg_and_jikou_default(self):
        self.seed()
        self.go()
        kw = self.ai.await_args.kwargs
        self.assertEqual(kw["system"], ia.HOUKI_SYSTEM_PROMPT)
        self.assertEqual(kw["tool_choice"], {"type": "tool", "name": "report_documents"})
        self.assertEqual(kw["messages"][0]["content"][-1]["text"], ia.HOUKI_USER_TEXT)
        self.assertIs(self.push.await_args.args[0], HOUKI_CHANNEL)
        self.assertIs(ia.HOUKI.app, houki_case_store.APP_HOUKI_CASE)
        self.assertIs(ia.JIKOU.app, ia.APP_JIKOU_CASE)
        self.assertIs(ia.JIKOU.line_channel, JIKOU_CHANNEL)
        self.assertEqual(ia.JIKOU.report_tool["name"], "report_creditors")
        self.assertEqual(ia.JIKOU.notify_timing, "after_send")
        self.assertEqual(ia.HOUKI.notify_timing, "after_ai")
        # claim key はチャネル別
        self.assertEqual(ia._claims, set())

    def test_10b_hook_routes_by_channel(self):
        analyze = AsyncMock(return_value="sent")

        async def search(app, query, fields=None):
            if "画像受領:" in query:
                ch = "houki" if "houki" in query else "jikou"
                return [{"$id": {"value": "5"},
                         "category": {"value": f"画像受領:{ch}:evt-9"}}]
            return []
        with patch.object(hub_kintone, "search_records", search), \
             patch.object(hub_kintone, "create_record", AsyncMock(return_value="9")), \
             patch.object(ii, "push_text", AsyncMock(return_value=True)), \
             patch.object(ia, "analyze_and_reply", analyze), \
             patch.dict(os.environ, {"IMAGE_HEAL_DISABLED": "0"}):
            ii._send_claims.clear()
            self.assertTrue(_run(ii.send_receipt_and_close("houki", HOUKI_CHANNEL, UID)))
            self.assertTrue(_run(ii.send_receipt_and_close("jikou", JIKOU_CHANNEL, UID)))
        self.assertEqual(analyze.await_args_list[0].args, (UID, "evt-9", ia.HOUKI))
        self.assertEqual(analyze.await_args_list[1].args, (UID, "evt-9"))

    def test_11_script_treats_photo_filled_death_date_as_answered(self):
        rid = self.seed()
        self.set_ai(death="2024-05-01", death_conf="high")
        self.assertEqual(self.go(), "sent")
        rec = _run(self.store.get_record(None, rid))
        self.assertEqual(rec["死亡日_申告"]["value"], "2024-05-01")
        # 第 1 通が済んだ状態にすると、第 2 通の「亡くなった方が亡くなった日」は
        # 回答済み扱い（写真由来の欄実値で判定・他の項目は未回答のまま）
        round2_labels = [l for l, f in houki_case_store.HEARING_ROUNDS[1][2]]
        rec2 = dict(rec)
        for code in ("被相続人氏名", "続柄", "被相続人最後の住所", "被相続人本籍"):
            rec2[code] = {"value": "x"}
        n, title, missing = houki_case_store.unanswered_items(rec2, [])
        self.assertEqual(n, 2)
        self.assertNotIn("亡くなった方が亡くなった日", missing)
        self.assertEqual(len(missing), len(round2_labels) - 1)

    def test_12_pins_and_kinds(self):
        self.assertEqual(_sha(ia.HOUKI_SYSTEM_PROMPT), self.HOUKI_SYSTEM_SHA256)
        self.assertEqual(_sha("|".join((ia.HOUKI_REPLY_TEMPLATE, ia.HOUKI_KEEP_ORIGINAL_LINE,
                                        ia.HOUKI_ITEM_CREDITOR, ia.HOUKI_ITEM_COURT,
                                        ia.HOUKI_ITEM_DEATH))),
                         self.HOUKI_TEMPLATE_SHA256)
        self.assertEqual(_sha(ia.SYSTEM_PROMPT), self.JIKOU_SYSTEM_SHA256)
        self.assertEqual(_sha(ia.IMG2_REPLY_TEMPLATE + "|" + ia.IMG2_REPLY_TEMPLATE_NO_CREDITOR),
                         self.JIKOU_TEMPLATE_SHA256)
        self.assertEqual(_sha("|".join((ia.CREDITOR_LINE_SINGLE, ia.CREDITOR_LINE_MULTI,
                                        ia.CREDITOR_LINE_ASSIGNED, ia.CREDITOR_LINE_AGENT))),
                         self.JIKOU_LINES_SHA256)
        self.assertEqual(_sha("|".join((ia.QUESTION_1, ia.QUESTION_2, ia.QUESTION_3,
                                        ia.QUESTION_4))), self.JIKOU_QUESTIONS_SHA256)
        for phrase in ("初めて届いた通知かどうか", "財産処分に当たるかどうか", "熟慮期間"):
            self.assertIn(phrase, ia.HOUKI_SYSTEM_PROMPT)
        for kind in ("houki_image_analysis_court", "houki_image_analysis_notice",
                     "houki_image_analysis_disposition",
                     "houki_image_analysis_send_failure", "houki_image_analysis_store"):
            with self.subTest(kind=kind), \
                    self.assertLogs(hub_notify.logger, level="INFO") as cm:
                hub_notify._log_throttled(f"{kind}:U_secret")
                out = "\n".join(cm.output)
                self.assertIn(f"kind={kind}", out)
                self.assertNotIn("unknown_kind", out)
        # 長文ゲート不使用・上限 600
        self.seed()
        boom = patch.object(reply_sanitizer, "structure_violations",
                            side_effect=AssertionError("gate reached"))
        with boom:
            self.assertEqual(self.go(), "sent")
        self.assertEqual(ia.REPLY_MAX_CHARS, 600)


# ── fix1: HI2-01（死亡日_申告 の整合検証経由）・HI2-02（notice 条件） ──────────────
class TestFix1DeathDateValidation(_Base):
    def test_hi2_01_inconsistent_with_known_date_write0(self):
        # (1) 死亡を知った日_申告=2024-05-01 保存済み・death_date=2024-06-01 →
        #     死亡日 > 知った日 の矛盾 → write 0・既存欄不変・通知なし
        rid = self.seed(**{"死亡を知った日_申告": "2024-05-01"})
        self.set_ai(death="2024-06-01", death_conf="high")
        with self.assertLogs(ia.logger, level="INFO") as cm:
            self.assertEqual(self.go(), "sent")
        self.assertIn("date_inconsistent", "\n".join(cm.output))
        self.assertIn("「2024年6月1日」", self.sent_text())        # 表示はする
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "")
        self.assertEqual(self.store.field(rid, "死亡を知った日_申告"), "2024-05-01")
        self.assertEqual(self.store.field(rid, "$revision"), "1")
        self.assertNotIn("houki_image_analysis_store", self.notify_kinds())

    def test_hi2_01_conflict_then_inconsistent_after_refetch(self):
        # (2) CAS 競合中に 死亡を知った日_申告 が書き換えられ矛盾 → 再取得後 write 0
        rid = self.seed()
        self.set_ai(death="2024-06-01", death_conf="high")
        orig = self.store.update_record
        state = {"n": 0}

        async def racing(app, record_id, fields, revision=None):
            if "死亡日_申告" in fields and state["n"] == 0:
                state["n"] += 1
                rec = self.store.cases[str(record_id)]
                rec["死亡を知った日_申告"] = {"value": "2024-05-01"}
                rec["$revision"] = {"value": str(int(rec["$revision"]["value"]) + 1)}
                raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "c")
            return await orig(app, record_id, fields, revision)
        with patch.object(hub_kintone, "update_record", racing):
            self.assertEqual(self.go(), "sent")
        self.assertEqual(state["n"], 1)
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "")
        self.assertEqual(self.store.field(rid, "死亡を知った日_申告"), "2024-05-01")

    def test_hi2_01_consistent_stored_and_existing_not_overwritten(self):
        # (3) 整合（死亡日 ≤ 知った日）→ 従来どおり転記
        rid = self.seed(**{"死亡を知った日_申告": "2024-06-10",
                           "相続人と知った日_申告": "2024-06-12"})
        self.set_ai(death="2024-06-01", death_conf="high")
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "2024-06-01")
        self.assertEqual(self.store.field(rid, "死亡を知った日_申告"), "2024-06-10")
        # CAS 競合 1 回（矛盾なし）でも収束して転記
        self.setUp()
        rid = self.seed()
        self.set_ai(death="2024-06-01", death_conf="high")
        self.store.conflicts_left = 1
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "2024-06-01")
        # (4) 死亡日_申告 が非空なら転記しない（検証にも入らない）
        self.setUp()
        rid = self.seed(**{"死亡日_申告": "2020-01-01"})
        self.set_ai(death="2024-06-01", death_conf="high")
        with patch.object(houki_case_store, "apply_hearing_fields",
                          AsyncMock(side_effect=AssertionError("must not be called"))):
            self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "2020-01-01")

    def test_hi2_01_goes_through_houki_case_store(self):
        # 独自 CAS 直書きではなく apply_hearing_fields（検証+収束）を経由する
        rid = self.seed()
        self.set_ai(death="2024-06-01", death_conf="high")
        with patch.object(houki_case_store, "apply_hearing_fields",
                          AsyncMock(return_value=(rid, [], []))) as apply:
            self.assertEqual(self.go(), "sent")
        apply.assert_awaited_once()
        args = apply.await_args.args
        self.assertEqual(args[0], UID)
        self.assertEqual(args[1], {"死亡日_申告": "2024-06-01"})
        self.assertEqual(args[2]["$id"]["value"], rid)
        src = open("hub/image_analysis.py", encoding="utf-8").read()
        body = src[src.index("async def _houki_store_death_date("):
                   src.index("async def _houki_store(")]
        self.assertNotIn("update_record", body)
        self.assertIn("apply_hearing_fields", body)

    def test_hi2_02_notice_condition(self):
        cases = {("不明", "該当"): 0, ("なし", "該当"): 0,
                 ("相続放棄申述受理通知書", "不明"): 1,
                 ("その他", "非該当"): 1, ("なし", "不明"): 0}
        for (kind, timing), expected in cases.items():
            with self.subTest(kind=kind, timing=timing):
                self.setUp()
                self.seed()
                self.set_ai(inh_kind=kind, timing=timing)
                self.go()
                self.assertEqual(self.notify_kinds().count("houki_image_analysis_notice"),
                                 expected)
        # parse 段階では組合せを拒否しない（他の有効情報を失わない）
        rep = ia.parse_houki_report(_hrep([_hc("アコム")], inh_kind="不明", timing="該当"))
        self.assertIsNotNone(rep)


# ── fix2: HI2F1-01（転記結果を App 40 実値で判定） ──────────────────────────────
class TestFix2DeathDateOutcome(_Base):
    DEATH = "2024-06-01"

    def _seed_and_ai(self):
        rid = self.seed()
        self.set_ai(death=self.DEATH, death_conf="high")
        return rid

    def test_1_cas_exhausted_write0_notifies(self):
        rid = self._seed_and_ai()
        self.store.conflicts_left = houki_case_store._CAS_RETRIES + 1
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "")     # write 0
        self.assertEqual(self.notify_kinds().count("houki_image_analysis_store"), 1)
        store_call = [c for c in self.admin.await_args_list
                      if c.kwargs["throttle_key"].startswith("houki_image_analysis_store")][0]
        self.assertIn(f"レコード番号 {rid}", store_call.args[0])

    def test_2_rival_stored_same_value_is_stored_no_notify(self):
        rid = self._seed_and_ai()
        orig = self.store.update_record

        async def racing(app, record_id, fields, revision=None):
            if "死亡日_申告" in fields:
                rec = self.store.cases[str(record_id)]
                if not rec["死亡日_申告"]["value"]:
                    rec["死亡日_申告"] = {"value": self.DEATH}   # 競合相手が同値を先に保存
                    rec["$revision"] = {"value": str(int(rec["$revision"]["value"]) + 1)}
                    raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "c")
            return await orig(app, record_id, fields, revision)
        with patch.object(hub_kintone, "update_record", racing):
            self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), self.DEATH)
        self.assertNotIn("houki_image_analysis_store", self.notify_kinds())

    def test_3_rival_stored_other_value_is_skipped_no_notify(self):
        rid = self._seed_and_ai()
        orig = self.store.update_record

        async def racing(app, record_id, fields, revision=None):
            if "死亡日_申告" in fields:
                rec = self.store.cases[str(record_id)]
                if not rec["死亡日_申告"]["value"]:
                    rec["死亡日_申告"] = {"value": "2024-05-20"}   # 競合相手が別値を先に保存
                    rec["$revision"] = {"value": str(int(rec["$revision"]["value"]) + 1)}
                    raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "c")
            return await orig(app, record_id, fields, revision)
        with patch.object(hub_kintone, "update_record", racing), \
                self.assertLogs(ia.logger, level="INFO") as cm:
            self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "2024-05-20")   # 既存値を尊重
        self.assertNotIn("houki_image_analysis_store", self.notify_kinds())
        self.assertIn("date_preempted", "\n".join(cm.output))

    def test_4_refetch_failure_after_store_is_failed(self):
        rid = self._seed_and_ai()
        orig = self.store.get_record
        calls = {"n": 0}

        async def flaky(app, record_id):
            # 転記後の再取得（apply の後）だけ失敗させる
            if self.store.cases[str(record_id)]["死亡日_申告"]["value"] == self.DEATH:
                raise hub_kintone.KintoneError(500, "x", "y")
            return await orig(app, record_id)
        with patch.object(hub_kintone, "get_record", flaky):
            self.assertEqual(self.go(), "sent")
        self.assertEqual(self.notify_kinds().count("houki_image_analysis_store"), 1)
        # 0 件（レコードが消えた）
        self.setUp()
        rid = self._seed_and_ai()

        async def gone(app, record_id):
            if self.store.cases[str(record_id)]["死亡日_申告"]["value"] == self.DEATH:
                raise hub_kintone.KintoneError(404, "GAIA_RE01", "nf")
            return await orig(app, record_id)
        with patch.object(hub_kintone, "get_record", gone):
            self.assertEqual(self.go(), "sent")
        self.assertEqual(self.notify_kinds().count("houki_image_analysis_store"), 1)

    def test_5_inconsistent_write0_still_no_notify(self):
        rid = self.seed(**{"死亡を知った日_申告": "2024-05-01"})
        self.set_ai(death=self.DEATH, death_conf="high")
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), "")
        self.assertNotIn("houki_image_analysis_store", self.notify_kinds())

    def test_6_success_path_refetches_once_and_stored(self):
        rid = self._seed_and_ai()
        orig = self.store.get_record
        seen = []

        async def counting(app, record_id):
            rec = await orig(app, record_id)
            seen.append(rec["死亡日_申告"]["value"])
            return rec
        with patch.object(hub_kintone, "get_record", counting):
            self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "死亡日_申告"), self.DEATH)
        self.assertEqual(seen.count(self.DEATH), 1)         # 転記後の再取得 1 回
        self.assertNotIn("houki_image_analysis_store", self.notify_kinds())


if __name__ == "__main__":
    unittest.main()
