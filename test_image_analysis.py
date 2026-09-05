"""JIKOU-IMG-2: 時効 LINE の書類写真の AI 読解→「債権者は◯◯とお見受けします」+
未回答質問の 1 通自動送信（hub/image_analysis）。

固定する仕様:
- 起点=時効チャネルの受領返信成功直後（image_intake.send_receipt_and_close の
  wrapper・IMG-1 の構造不変・相続放棄は対象外）
- 対象=App 21 の 受信書類写真 fileKey のうち未解析（App 28 画像解析済 行なし）を
  新しい順に最大 5・download_file 経由・jpeg/png=image・pdf=document・heic/5MB
  超は AI に送らない
- AI は tool_choice 強制の閉集合スキーマ・凍結 system prompt（sha256 pin）
- 返信は凍結テンプレ+検証済み債権者名の差し込みのみ（AI 自由文なし・長文
  ゲート structure_violations は通さない・全文 600 字上限）
- 失敗の区別: ai_failed/illegible/low_confidence=質問のみ版／download 失敗・
  レコード不在・fileKey 0 件=送らない／送信失敗=マーカーなし+要確認通知／
  人対応・pause・停止リスト=送らない・マーカーなし
- 冪等: in-memory claim・解析マーカー（画像解析:jikou:{event_id}・message=本文）
  +fileKey ごとの 画像解析済:jikou:{fileKey} 行は送信成功後にのみ書く
"""

import asyncio
import hashlib
import os
import re
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

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
    "HOUKI_LINE_CHANNEL_SECRET": "houki_secret",
    "HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "houki_token",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

import main  # noqa: E402
from hub import image_analysis as ia  # noqa: E402
from hub import image_intake as ii  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import notify as hub_notify  # noqa: E402
from hub import reply_sanitizer  # noqa: E402

JPEG = b"\xff\xd8\xff\xe0" + b"J" * 32
PNG = b"\x89PNG\r\n\x1a\n" + b"P" * 32
PDF = b"%PDF-1.4\n" + b"D" * 32
HEIC = b"\x00\x00\x00\x18ftypheic" + b"H" * 32
BIG = b"\xff\xd8\xff" + b"x" * ia.MAX_AI_IMAGE_BYTES
UID = "U_img2_user"
EVT = "evt-img2"


def _run(coro):
    return asyncio.run(coro)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _report(creditors=(), court="なし", legible=True):
    return {"creditors": [dict(c) for c in creditors],
            "court_document": court, "legible": legible}


def _tool_response(tool_input, name="report_creditors"):
    return SimpleNamespace(content=[SimpleNamespace(
        type="tool_use", name=name, id="tu1", input=tool_input)])


class _FakeStore:
    """App 21（LINEユーザーID 検索・get/update CAS・download_file）と App 28
    （line_user_id + category 検索・create）の最小フェイク。"""

    def __init__(self):
        self.cases: dict[str, dict] = {}
        self.chatlog: list[dict] = []
        self.files: dict[str, bytes] = {}
        self._id = 0
        self.downloaded: list[str] = []
        self.conflicts_left = 0
        self.update_error: Exception | None = None
        self.download_error: Exception | None = None

    @staticmethod
    def _reject_double_wrap(fields):
        for code, v in (fields or {}).items():
            if isinstance(v, dict) and "value" in v:
                raise AssertionError(f"double-wrapped payload: {code}")

    def seed_case(self, fields: dict, file_keys: list[str],
                  contents: dict[str, bytes] | None = None) -> str:
        self._id += 1
        rid = str(self._id)
        rec = {k: {"value": v} for k, v in fields.items()}
        rec["$id"] = {"value": rid}
        rec["$revision"] = {"value": "1"}
        rec.setdefault("問い合わせ業者名", {"value": ""})
        rec.setdefault("裁判所書類", {"value": ""})
        rec["受信書類写真"] = {"value": [
            {"fileKey": k, "name": f"{k}.jpg", "size": "1",
             "contentType": "image/jpeg"} for k in file_keys]}
        self.cases[rid] = rec
        for k in file_keys:
            self.files[k] = (contents or {}).get(k, JPEG)
        return rid

    def seed_analyzed(self, user_id: str, file_key: str):
        _run(self.create_record(None, {
            "line_user_id": user_id, "role": "assistant",
            "message": ia.ANALYZED_MARKER_TEXT,
            "category": ia.analyzed_category(file_key), "auto_sent": "no"}))

    async def search_records(self, app, query, fields=None):
        m = re.search('LINEユーザーID = "([^"]+)"', query)
        if m:
            rows = [r for r in self.cases.values()
                    if r.get("LINEユーザーID", {}).get("value") == m.group(1)]
            rows.sort(key=lambda r: -int(r["$id"]["value"]))
            return rows[:1]
        m_uid = re.search('line_user_id = "([^"]+)"', query)
        m_eq = re.search('category = "([^"]+)"', query)
        m_like = re.search('category like "([^"]+)"', query)
        rows = self.chatlog
        if m_uid:
            rows = [r for r in rows if r.get("line_user_id") == m_uid.group(1)]
        if m_eq:
            rows = [r for r in rows if r.get("category") == m_eq.group(1)]
        elif m_like:
            rows = [r for r in rows
                    if str(r.get("category") or "").startswith(m_like.group(1))]
        rows = sorted(rows, key=lambda r: int(r["$id"]), reverse="desc" in query)
        m_lim = re.search(r"limit (\d+)", query)
        lim = int(m_lim.group(1)) if m_lim else len(rows)
        return [{"$id": {"value": r["$id"]},
                 "category": {"value": r.get("category", "")}}
                for r in rows[:lim]]

    async def create_record(self, app, fields):
        self._reject_double_wrap(fields)
        self._id += 1
        self.chatlog.append({"$id": str(self._id), **fields})
        return str(self._id)

    async def get_record(self, app, record_id):
        rec = self.cases.get(str(record_id))
        if rec is None:
            raise hub_kintone.KintoneError(404, "GAIA_RE01", "nf")
        return {k: (dict(v) if isinstance(v, dict) else v)
                for k, v in rec.items()}

    async def update_record(self, app, record_id, fields, revision=None):
        self._reject_double_wrap(fields)
        if self.conflicts_left > 0:
            self.conflicts_left -= 1
            raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "c")
        if self.update_error is not None:
            raise self.update_error
        rec = self.cases[str(record_id)]
        cur = int(rec["$revision"]["value"])
        if revision is not None and int(revision) != cur:
            raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "c")
        rec.update({k: {"value": v} for k, v in fields.items()})
        rec["$revision"] = {"value": str(cur + 1)}

    async def download_file(self, app, file_key):
        if self.download_error is not None:
            raise self.download_error
        self.downloaded.append(file_key)
        return self.files[file_key]

    # helpers
    def analysis_rows(self):
        return [r for r in self.chatlog
                if str(r.get("category", "")).startswith(ia.ANALYSIS_PREFIX)]

    def analyzed_keys(self):
        p = ia.ANALYZED_PREFIX + "jikou:"
        return [r["category"][len(p):] for r in self.chatlog
                if str(r.get("category", "")).startswith(p)]

    def field(self, rid, code):
        return self.cases[str(rid)][code]["value"]


class _Base(unittest.TestCase):
    def setUp(self):
        ia._claims.clear()
        self.addCleanup(ia._claims.clear)
        self.store = _FakeStore()
        self.push = AsyncMock(return_value=True)
        self.admin = AsyncMock(return_value=True)
        self.ai = AsyncMock(return_value=_tool_response(_report(
            [{"name": "アコム", "role": "原債権者", "confidence": "high"}])))
        for p in (patch.object(hub_kintone, "search_records", self.store.search_records),
                  patch.object(hub_kintone, "create_record", self.store.create_record),
                  patch.object(hub_kintone, "get_record", self.store.get_record),
                  patch.object(hub_kintone, "update_record", self.store.update_record),
                  patch.object(hub_kintone, "download_file", self.store.download_file),
                  patch.object(ia, "push_text", self.push),
                  patch.object(ia, "create_message_with_fallback", self.ai),
                  patch.object(ia.notify, "notify_admin_line", self.admin),
                  patch.object(ia, "is_suppressed", AsyncMock(return_value=False)),
                  patch.dict(os.environ, {"AUTOREPLY_PAUSED": "0"})):
            p.start()
            self.addCleanup(p.stop)

    def seed(self, keys=("k1",), known=None, contents=None):
        fields = {"LINEユーザーID": UID, "response_mode": "自動",
                  **(known or {})}
        return self.store.seed_case(fields, list(keys), contents)

    def go(self, event_id=EVT):
        return _run(ia.analyze_and_reply(UID, event_id))

    def sent_text(self) -> str:
        self.push.assert_awaited_once()
        self.assertIs(self.push.await_args.args[0], ia.JIKOU_CHANNEL)
        self.assertEqual(self.push.await_args.args[1], UID)
        return self.push.await_args.args[2]

    def set_ai(self, creditors, court="なし", legible=True):
        self.ai.return_value = _tool_response(_report(creditors, court, legible))


# ── 1〜3: 債権者行の型と未回答質問 ─────────────────────────────────────────────
class TestCreditorLineAndQuestions(_Base):
    def test_1_single_high_with_pending_questions_only(self):
        self.seed(known={"借入時期_テキスト": "2015年頃"})
        self.assertEqual(self.go(), "sent")
        text = self.sent_text()
        self.assertTrue(text.startswith("お写真をありがとうございます。\n"))
        self.assertIn("お写真から、債権者はアコムとお見受けします。"
                      "違っている場合は教えてください。", text)
        self.assertNotIn(ia.QUESTION_1, text)          # 債権者行があるので①省略
        self.assertNotIn(ia.QUESTION_2, text)          # 既知（借入時期）は省く
        self.assertIn(ia.QUESTION_3, text)
        self.assertIn(ia.QUESTION_4, text)
        self.assertIn("あわせて、次の点を教えてください。わかる範囲で結構です。", text)
        self.assertNotIn("⑤", text)
        self.assertLessEqual(len(text), ia.REPLY_MAX_CHARS)

    def test_2_two_and_three_high(self):
        self.seed()
        self.set_ai([{"name": "アコム", "role": "原債権者", "confidence": "high"},
                     {"name": "レイク", "role": "不明", "confidence": "high"}])
        self.go()
        self.assertIn("債権者はアコムおよびレイクとお見受けします", self.sent_text())
        self.push.reset_mock()
        ia._claims.clear()
        self.store.chatlog.clear()
        self.set_ai([{"name": "アコム", "role": "原債権者", "confidence": "high"},
                     {"name": "レイク", "role": "サービサー", "confidence": "high"},
                     {"name": "プロミス", "role": "不明", "confidence": "high"}])
        self.go("evt-2")
        self.assertIn("債権者はアコム、レイクおよびプロミスとお見受けします",
                      self.sent_text())

    def test_3_assigned_and_agent(self):
        self.seed()
        self.set_ai([{"name": "日本債権回収", "role": "譲受人", "confidence": "high"},
                     {"name": "アコム", "role": "原債権者", "confidence": "high"}])
        self.go()
        self.assertIn("債権者は日本債権回収（アコムから債権譲渡を受けたもの）"
                      "とお見受けします", self.sent_text())
        # 譲渡型は譲受人・原債権者の両方を 問い合わせ業者名 へ（「、」区切り）
        self.assertEqual(self.store.field("1", "問い合わせ業者名"), "日本債権回収、アコム")
        self.setUp()
        self.seed()
        self.set_ai([{"name": "アコム", "role": "原債権者", "confidence": "high"},
                     {"name": "山田法律事務所", "role": "代理人", "confidence": "high"}])
        self.go("evt-2")
        self.assertIn("債権者はアコム（ご連絡元の山田法律事務所はその代理人）"
                      "とお見受けします", self.sent_text())
        # 問い合わせ業者名 には代理人を書かない
        self.assertEqual(self.store.field("1", "問い合わせ業者名"), "アコム")

    def test_questions_only_with_no_pending_questions_omits_tail(self):
        self.seed(known={"借入時期_テキスト": "2015年頃",
                         "最終返済日_テキスト": "2018年",
                         "裁判所書類": "何も届いていない"})
        self.assertEqual(self.go(), "sent")
        text = self.sent_text()
        self.assertEqual(text, "お写真をありがとうございます。\n"
                         + ia.CREDITOR_LINE_SINGLE.replace("{A}", "アコム"))
        self.assertNotIn("あわせて", text)


# ── 4〜5: 失敗時は質問のみ版・差し込み値検証 ───────────────────────────────────
class TestQuestionsOnlyFallbacks(_Base):
    def _assert_questions_only(self, outcome_log=None):
        text = self.sent_text()
        self.assertNotIn("お見受けします", text)
        self.assertIn(ia.QUESTION_1, text)
        self.assertIn(ia.QUESTION_2, text)
        self.assertIn(ia.QUESTION_3, text)
        self.assertIn(ia.QUESTION_4, text)
        self.assertEqual(self.store.field("1", "問い合わせ業者名"), "")

    def test_4_no_high_illegible_exception_timeout_invalid(self):
        cases = {
            "medium_only": lambda: self.set_ai(
                [{"name": "アコム", "role": "原債権者", "confidence": "medium"}]),
            "illegible": lambda: self.set_ai(
                [{"name": "アコム", "role": "原債権者", "confidence": "high"}],
                legible=False),
            "exception": lambda: setattr(self.ai, "side_effect",
                                         RuntimeError("boom")),
            "timeout": lambda: setattr(self.ai, "side_effect",
                                       asyncio.TimeoutError()),
            "invalid_role": lambda: setattr(self.ai, "return_value", _tool_response(
                {"creditors": [{"name": "アコム", "role": "債権者",
                                "confidence": "high"}],
                 "court_document": "なし", "legible": True})),
            "wrong_tool": lambda: setattr(self.ai, "return_value",
                                          _tool_response({}, name="other")),
            "agent_only": lambda: self.set_ai(
                [{"name": "山田法律事務所", "role": "代理人", "confidence": "high"}]),
        }
        for name, setup in cases.items():
            with self.subTest(case=name):
                self.setUp()
                self.seed()
                setup()
                self.assertEqual(self.go(), "sent")
                self._assert_questions_only()
                self.assertEqual(len(self.store.analysis_rows()), 1)

    def test_5_invalid_creditor_names_drop_line(self):
        for bad in ("あ" * 41, "アコム\nレイク", "http://example.com",
                    "www.example.com", "123456", "１２３", "アコム;DROP", ""):
            with self.subTest(bad=bad[:12]):
                self.setUp()
                self.seed()
                self.set_ai([{"name": bad, "role": "原債権者", "confidence": "high"},
                             {"name": "レイク", "role": "不明", "confidence": "high"}])
                self.assertEqual(self.go(), "sent")
                self._assert_questions_only()
        for ok in ("アコム株式会社", "エー・シー・エス（株）", "SMBC&Co", "ＡＣＯＭ"):
            with self.subTest(ok=ok):
                self.assertTrue(ia.valid_creditor_name(ok))


# ── 6〜8: 上限・抑止・送信失敗・マーカー ─────────────────────────────────────────
class TestGatesAndMarkers(_Base):
    def test_6_too_long_not_sent_and_notified(self):
        self.seed()
        with patch.object(ia, "REPLY_MAX_CHARS", 100):
            self.assertEqual(self.go(), "too_long")
        self.push.assert_not_awaited()
        self.admin.assert_awaited_once()
        self.assertEqual(self.admin.await_args.kwargs["throttle_key"],
                         f"image_analysis_send_failure:{UID}")
        self.assertEqual(self.store.analysis_rows(), [])
        self.assertEqual(self.store.analyzed_keys(), [])
        self.assertEqual(ia.REPLY_MAX_CHARS, 600)

    def test_7_human_pause_stoplist_not_sent_no_marker(self):
        # 人対応
        self.store.seed_case({"LINEユーザーID": UID, "response_mode": "人対応"}, ["k1"])
        self.assertEqual(self.go(), "blocked")
        # pause
        self.store.cases.clear()
        self.seed()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            ia._claims.clear()
            self.assertEqual(self.go(), "blocked")
        # 停止リスト
        with patch.object(ia, "is_suppressed", AsyncMock(return_value=True)):
            ia._claims.clear()
            self.assertEqual(self.go(), "blocked")
        self.push.assert_not_awaited()
        self.assertEqual(self.store.analysis_rows(), [])
        self.assertEqual(self.store.analyzed_keys(), [])
        self.assertEqual(self.store.field("2", "問い合わせ業者名"), "")

    def test_8_send_failure_no_marker_notify_and_success_marker(self):
        self.seed(keys=["k1", "k2"])
        self.push.return_value = False
        self.assertEqual(self.go(), "send_failed")
        self.assertEqual(self.store.analysis_rows(), [])
        self.assertEqual(self.store.analyzed_keys(), [])
        self.admin.assert_awaited_once()
        self.assertEqual(self.admin.await_args.kwargs["throttle_key"],
                         f"image_analysis_send_failure:{UID}")
        self.assertNotIn("アコム", self.admin.await_args.args[0])
        # push 例外も同じ
        ia._claims.clear()
        self.push.side_effect = RuntimeError("net")
        self.assertEqual(self.go(), "send_failed")
        self.assertEqual(self.store.analysis_rows(), [])
        # 成功 → マーカー（message=送った本文）+ fileKey ごとの解析済み行
        ia._claims.clear()
        self.push.side_effect = None
        self.push.return_value = True
        self.assertEqual(self.go(), "sent")
        rows = self.store.analysis_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["category"], f"画像解析:jikou:{EVT}")
        self.assertEqual(rows[0]["message"], self.push.await_args.args[2])
        self.assertEqual(rows[0]["role"], "assistant")
        self.assertEqual(rows[0]["auto_sent"], "yes")
        self.assertEqual(sorted(self.store.analyzed_keys()), ["k1", "k2"])
        # 再解析されない（未解析 0 件）
        ia._claims.clear()
        self.push.reset_mock()
        self.assertEqual(self.go("evt-next"), "no_files")
        self.push.assert_not_awaited()


# ── 9〜10: 対象写真の選定と振り分け ─────────────────────────────────────────────
class TestTargetsAndBlocks(_Base):
    def test_9_only_unanalyzed_newest_five(self):
        keys = [f"k{i}" for i in range(1, 8)]             # k1 が最古・k7 が最新
        self.seed(keys=keys)
        self.store.seed_analyzed(UID, "k7")                # 最新は解析済み
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.downloaded, ["k6", "k5", "k4", "k3", "k2"])
        self.assertEqual(sorted(self.store.analyzed_keys()),
                         ["k2", "k3", "k4", "k5", "k6", "k7"])
        blocks = self.ai.await_args.kwargs["messages"][0]["content"]
        self.assertEqual(len([b for b in blocks if b["type"] == "image"]), 5)
        self.assertEqual(blocks[-1]["type"], "text")

    def test_9b_no_files_or_no_record_not_sent(self):
        self.seed(keys=[])
        self.assertEqual(self.go(), "no_files")
        self.store.cases.clear()
        ia._claims.clear()
        self.assertEqual(self.go(), "no_record")
        self.push.assert_not_awaited()
        self.admin.assert_not_awaited()
        self.assertEqual(self.store.downloaded, [])

    def test_9c_download_failure_not_sent_no_notify(self):
        self.seed()
        self.store.download_error = hub_kintone.KintoneError(500, "x", "y")
        self.assertEqual(self.go(), "download_failed")
        self.push.assert_not_awaited()
        self.admin.assert_not_awaited()
        self.ai.assert_not_awaited()

    def test_10_pdf_document_heic_and_oversize_skipped(self):
        self.seed(keys=["k1", "k2", "k3", "k4"],
                  contents={"k1": PDF, "k2": HEIC, "k3": BIG, "k4": PNG})
        self.assertEqual(self.go(), "sent")
        blocks = self.ai.await_args.kwargs["messages"][0]["content"]
        types = [(b["type"], b.get("source", {}).get("media_type"))
                 for b in blocks[:-1]]
        self.assertEqual(types, [("image", "image/png"),
                                 ("document", "application/pdf")])
        self.assertEqual(sorted(self.store.analyzed_keys()),
                         ["k1", "k2", "k3", "k4"])    # 読めない写真も解析済み扱い
        kw = self.ai.await_args.kwargs
        self.assertEqual(kw["tool_choice"], {"type": "tool",
                                             "name": "report_creditors"})
        self.assertEqual(kw["system"], ia.SYSTEM_PROMPT)
        # 全て読めない → AI を呼ばず質問のみ版
        self.setUp()
        self.seed(keys=["h1"], contents={"h1": HEIC})
        self.assertEqual(self.go(), "sent")
        self.ai.assert_not_awaited()
        self.assertIn(ia.QUESTION_1, self.push.await_args.args[2])

    def test_ai_client_timeout_and_retries_pinned(self):
        self.seed()
        with patch.object(ia.anthropic, "AsyncAnthropic") as client_cls:
            self.go()
        self.assertEqual(client_cls.call_args.kwargs["timeout"], 60.0)
        self.assertEqual(client_cls.call_args.kwargs["max_retries"], 1)
        self.assertEqual(ia.MAX_FILES, 5)
        self.assertEqual(ia.MAX_AI_IMAGE_BYTES, 5 * 1024 * 1024)


# ── 11〜12: kintone 書込と弁護士通知 ─────────────────────────────────────────────
class TestStoreAndCourt(_Base):
    def test_11_creditor_store_only_when_empty(self):
        rid = self.seed()
        self.set_ai([{"name": "アコム", "role": "原債権者", "confidence": "high"},
                     {"name": "レイク", "role": "不明", "confidence": "high"}])
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "問い合わせ業者名"), "アコム、レイク")
        self.assertEqual(self.store.field(rid, "$revision"), "2")
        # 非空なら書かない
        rid2 = self.store.seed_case({"LINEユーザーID": UID, "問い合わせ業者名": "既存"},
                                    ["z1"])
        ia._claims.clear()
        self.assertEqual(self.go("evt-2"), "sent")
        self.assertEqual(self.store.field(rid2, "問い合わせ業者名"), "既存")
        self.admin.assert_not_awaited()

    def test_11b_conflict_retry_and_failure_notify(self):
        rid = self.seed()
        self.store.conflicts_left = 1
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "問い合わせ業者名"), "アコム")
        self.admin.assert_not_awaited()
        # 確定失敗 → 要確認通知（kind image_analysis_store）・上書きなし
        self.setUp()
        rid = self.seed()
        self.store.update_error = hub_kintone.KintoneError(403, "GAIA_NO01", "x")
        self.assertEqual(self.go(), "sent")
        self.assertEqual(self.store.field(rid, "問い合わせ業者名"), "")
        self.admin.assert_awaited_once()
        self.assertEqual(self.admin.await_args.kwargs["throttle_key"],
                         f"image_analysis_store:{rid}")
        self.assertNotIn("アコム", self.admin.await_args.args[0])

    def test_12_court_document_notifies_and_field_unchanged(self):
        rid = self.seed()
        self.set_ai([{"name": "アコム", "role": "原債権者", "confidence": "high"}],
                    court="訴状")
        self.assertEqual(self.go(), "sent")
        calls = [c for c in self.admin.await_args_list
                 if c.kwargs["throttle_key"] == f"image_analysis_court:{rid}"]
        self.assertEqual(len(calls), 1)
        text = calls[0].args[0]
        self.assertIn("種別: 訴状", text)
        self.assertIn(f"レコード番号: {rid}", text)
        self.assertNotIn("アコム", text)
        self.assertEqual(self.store.field(rid, "裁判所書類"), "")
        # なし/不明 は通知しない
        self.setUp()
        rid = self.seed()
        self.set_ai([{"name": "アコム", "role": "原債権者", "confidence": "high"}],
                    court="不明")
        self.go()
        self.admin.assert_not_awaited()

    def test_throttle_kinds_registered(self):
        for kind in ("image_analysis_send_failure", "image_analysis_store",
                     "image_analysis_court", "houki_image_send_failure",
                     "houki_image_failure", "houki_image_attach",
                     "shindan_photos", "form_link_merge"):
            with self.subTest(kind=kind), \
                    self.assertLogs(hub_notify.logger, level="INFO") as cm:
                hub_notify._log_throttled(f"{kind}:U_secret_user")
                out = "\n".join(cm.output)
                self.assertIn(f"kind={kind}", out)
                self.assertNotIn("unknown_kind", out)
                self.assertNotIn("U_secret_user", out)


# ── 13〜14: claim・長文ゲート不使用・凍結 pin ──────────────────────────────────
class TestClaimAndPins(_Base):
    SYSTEM_SHA256 = "538e10dd53dbfa93680d1287f00df51fd08e8e213a7431e14fafc824f13ad935"
    TEMPLATE_SHA256 = "b97d20bbfe56f4bc4e177b7d4ece623620f1d0cd94ae9705f02e3fedf76f9087"
    LINES_SHA256 = "0bf3d53d51882ad3eedbc6cb1d3f62a41c8b4699eecfb4e49289929d09fef763"

    def test_13_concurrent_same_event_sends_once(self):
        self.seed()

        async def slow(*_a, **_k):
            await asyncio.sleep(0.02)
            return _tool_response(_report(
                [{"name": "アコム", "role": "原債権者", "confidence": "high"}]))
        self.ai.side_effect = slow

        async def scenario():
            return await asyncio.gather(ia.analyze_and_reply(UID, EVT),
                                        ia.analyze_and_reply(UID, EVT))
        outcomes = _run(scenario())
        self.assertEqual(sorted(outcomes), ["claimed", "sent"])
        self.push.assert_awaited_once()
        self.assertEqual(len(self.store.analysis_rows()), 1)
        self.assertEqual(ia._claims, set())

    def test_14_structure_gate_not_used_and_pins(self):
        self.seed()
        boom = MagicMock(side_effect=AssertionError("structure gate reached"))
        with patch.object(reply_sanitizer, "structure_violations", boom):
            self.assertEqual(self.go(), "sent")
        boom.assert_not_called()
        src = open("hub/image_analysis.py", encoding="utf-8").read()
        self.assertNotIn("structure_violations(", src.split('"""', 2)[2])
        self.assertEqual(_sha(ia.SYSTEM_PROMPT), self.SYSTEM_SHA256)
        self.assertEqual(_sha(ia.IMG2_REPLY_TEMPLATE), self.TEMPLATE_SHA256)
        self.assertEqual(_sha("|".join((ia.CREDITOR_LINE_SINGLE, ia.CREDITOR_LINE_MULTI,
                                        ia.CREDITOR_LINE_ASSIGNED,
                                        ia.CREDITOR_LINE_AGENT))),
                         self.LINES_SHA256)
        # 質問文は _HEARING_PROMPT_FROZEN の逐語（単一の正）・⑤は流用しない
        for q in (ia.QUESTION_1, ia.QUESTION_2, ia.QUESTION_3, ia.QUESTION_4):
            self.assertIn(q, main._HEARING_PROMPT_FROZEN)
        self.assertFalse(any(q.startswith("⑤") for _k, q in ia.QUESTIONS))
        self.assertEqual([k for k, _q in ia.QUESTIONS],
                         ["債権者名", "借入時期", "最終返済日", "裁判所書類の有無"])
        # tool スキーマの閉集合
        props = ia.REPORT_TOOL["input_schema"]["properties"]
        self.assertEqual(sorted(props), ["court_document", "creditors", "legible"])
        self.assertEqual(sorted(props["creditors"]["items"]["properties"]),
                         ["confidence", "name", "role"])


# ── 15: 起点（image_intake の wrapper）と既存構造の不変 ─────────────────────────
class TestHookFromReceipt(unittest.TestCase):
    def setUp(self):
        ii._pending.clear()
        ii._send_claims.clear()
        self.rows = []

        async def search(app, query, fields=None):
            if "画像受領:" in query:
                return [{"$id": {"value": "5"},
                         "category": {"value": "画像受領:jikou:evt-77"}}]
            return []

        async def create(app, fields):
            self.rows.append(fields)
            return "9"
        self.analyze = AsyncMock(return_value="sent")
        for p in (patch.object(hub_kintone, "search_records", search),
                  patch.object(hub_kintone, "create_record", create),
                  patch.object(ii, "push_text", AsyncMock(return_value=True)),
                  patch.object(ia, "analyze_and_reply", self.analyze),
                  patch.dict(os.environ, {"IMAGE_HEAL_DISABLED": "0"})):
            p.start()
            self.addCleanup(p.stop)

    def test_jikou_success_triggers_analysis_with_event_id(self):
        result = _run(ii.send_receipt_and_close("jikou", main.hub_line_channel.JIKOU_CHANNEL,
                                                UID))
        self.assertTrue(result)
        self.analyze.assert_awaited_once_with(UID, "evt-77")
        self.assertEqual(ii._send_claims, set())        # claim 解放後に呼ばれる

    def test_houki_not_triggered(self):
        result = _run(ii.send_receipt_and_close("houki", main.hub_line_channel.HOUKI_CHANNEL,
                                                UID))
        self.assertTrue(result)
        self.analyze.assert_not_awaited()

    def test_receipt_failure_no_analysis_and_hook_error_contained(self):
        with patch.object(ii, "push_text", AsyncMock(return_value=False)), \
             patch("hub.notify.notify_business", AsyncMock(return_value=True)):
            result = _run(ii.send_receipt_and_close(
                "jikou", main.hub_line_channel.JIKOU_CHANNEL, UID))
        self.assertFalse(result)
        self.analyze.assert_not_awaited()
        self.analyze.side_effect = RuntimeError("boom")
        result = _run(ii.send_receipt_and_close("jikou", main.hub_line_channel.JIKOU_CHANNEL,
                                                UID))
        self.assertTrue(result)                         # 受領返信の結果は不変


if __name__ == "__main__":
    unittest.main()
