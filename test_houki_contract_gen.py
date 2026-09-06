"""HOUKI-CONTRACT-GEN: 相続放棄 委任契約書の自動生成（App 40・ChannelConfig 化）。

固定する仕様:
- 雛形 docx_templates/houki/委任契約書.docx は人承認済み現物（SHA256 pin）・全 62 段落の
  テキスト sha256 pin・12 個の差し込み記号が各 1 回・表なし
- 費用定数（88,000 / 33,000 / 5,000 / 1,100 / 3 か所）と代表者定型文は弁護士凍結事項
- 申述人集合（被相続人グループID）・費用計算・申述人一覧の展開・契約署名 3 通り
- 状態機械・CAS・_regeneration_guard 3 経路は時効版と共通（cfg 経由）
- cloudsign_webhook は App 21 → App 40 の順に検索し、両方該当は fail-closed
- 時効側の既存テスト（test_contract_gen1/gen2/tokuyaku・test_cloudsign_*）は無変更で green
"""

import asyncio
import datetime
import hashlib
import io
import os
import re
import unittest
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
    "HOUKI_WEBHOOK_TOKEN": "houki-hook",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from docx import Document  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import contract_pdf  # noqa: E402
import contract_webhook as cw  # noqa: E402
import main  # noqa: E402
from config import EXPECTED_DOCX_TEMPLATES  # noqa: E402
from hub import houki_contract as hc  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import notify as hub_notify  # noqa: E402
from hub.kintone import KintoneError  # noqa: E402

_client = TestClient(main.app)
_URL = "/souzoku-houki/contract/houki-hook"
NOW = datetime.datetime(2026, 9, 6, 15, 0, tzinfo=hc.JST)
EMAIL_A, EMAIL_B, EMAIL_C = "a@example.com", "b@example.com", "c@example.com"


def _run(coro):
    return asyncio.run(coro)


def _docx_paras(data: bytes) -> list[str]:
    return [p.text for p in Document(io.BytesIO(data)).paragraphs]


def _creditors(*names):
    return [{"value": {"債権者名": {"value": n}, "通知要否": {"value": "未確認"}}} for n in names]


def _rec(rid, name, addr="架空県架空市1-1", email=EMAIL_A, decedent="架空太郎",
         group="", sign="", tokuyaku="", creditors=(), status="契約書作成",
         revision="5", attachment=None, cs_doc_id=""):
    base = {
        "$id": rid, "$revision": revision, "契約書ステータス": status,
        "顧客名": name, "住所": addr, "メールアドレス": email, "被相続人氏名": decedent,
        "被相続人グループID": group, "契約署名": sign, "特約": tokuyaku,
        "cloudsign_document_id": cs_doc_id, "委任契約書": attachment or [],
    }
    rec = {k: {"value": v} for k, v in base.items()}
    rec["債権者一覧"] = {"value": _creditors(*creditors)}
    return rec


def _body(rid="1", status="契約書作成", app="40"):
    return {"app": {"id": app}, "record": {"$id": {"value": rid},
                                           "契約書ステータス": {"value": status}}}


class _Base(unittest.TestCase):
    """App 40 の最小フェイク: get_record / search_records（被相続人グループID）/
    update_record（CAS）/ upload_file。"""

    def setUp(self):
        self.records: dict[str, dict] = {}
        self.uploads: list[tuple] = []
        self.updates: list[tuple] = []
        self.admin = AsyncMock(return_value=True)
        self.cs_create = MagicMock(return_value="doc-h1")
        self.cs_attach = MagicMock()
        self.cs_participant = MagicMock()
        self.cs_delete = MagicMock(return_value=True)

        async def get_record(app, rid):
            rec = self.records.get(str(rid))
            if rec is None:
                raise KintoneError(404, "GAIA_RE01", "nf")
            return {k: dict(v) for k, v in rec.items()}

        async def search_records(app, query, fields=None):
            m = re.search(r'被相続人グループID = "([^"]+)"', query)
            rows = [r for r in self.records.values()
                    if m and r["被相続人グループID"]["value"] == m.group(1)]
            rows.sort(key=lambda r: int(r["$id"]["value"]))
            return [{k: dict(v) for k, v in r.items()} for r in rows]

        async def update_record(app, rid, fields, revision=None):
            rec = self.records[str(rid)]
            cur = int(rec["$revision"]["value"])
            if revision is not None and int(revision) != cur:
                raise KintoneError(409, "GAIA_CO02", "c")
            self.updates.append((str(rid), dict(fields)))
            for k, v in fields.items():
                rec[k] = {"value": v}
            rec["$revision"] = {"value": str(cur + 1)}

        async def upload_file(app, filename, content, mime):
            self.uploads.append((filename, content, mime))
            return f"fk-{len(self.uploads)}"

        for p in (patch.dict(os.environ, _ENV),
                  patch.object(cw.hub_kintone, "get_record", get_record),
                  patch.object(cw.hub_kintone, "update_record", update_record),
                  patch.object(cw.hub_kintone, "upload_file", upload_file),
                  patch.object(hub_kintone, "search_records", search_records),
                  patch.object(cw, "_cs_create_document", self.cs_create),
                  patch.object(cw, "_cs_attach_pdf", self.cs_attach),
                  patch.object(cw, "_cs_add_participant", self.cs_participant),
                  patch.object(cw, "_cs_delete_draft", self.cs_delete),
                  patch.object(hub_notify, "notify_admin_line", self.admin),
                  patch("hub.notify.notify_admin_line", self.admin)):
            p.start()
            self.addCleanup(p.stop)

    def seed(self, *recs):
        for r in recs:
            self.records[r["$id"]["value"]] = r
        return recs[0]["$id"]["value"]

    def post(self, rid="1", status="契約書作成", url=_URL, app="40"):
        return _client.post(url, json=_body(rid, status, app))

    def field(self, rid, code):
        return self.records[str(rid)][code]["value"]

    def kinds(self):
        return [c.kwargs.get("throttle_key", "").split(":", 1)[0]
                for c in self.admin.await_args_list]

    def texts(self):
        return [c.args[0] for c in self.admin.await_args_list]

    def last_docx(self) -> bytes:
        return self.uploads[-1][1]


# ── 1. 雛形と凍結 pin ─────────────────────────────────────────────────────────
class TestTemplatePins(unittest.TestCase):
    def test_template_sha_and_paragraph_hash(self):
        data = open(hc.TEMPLATE_PATH, "rb").read()
        self.assertEqual(hashlib.sha256(data).hexdigest(), hc.TEMPLATE_SHA256)
        self.assertEqual(hc.TEMPLATE_SHA256[:8], "519d4bb6")
        doc = Document(hc.TEMPLATE_PATH)
        texts = [p.text for p in doc.paragraphs]
        self.assertEqual(len(texts), hc.TEMPLATE_PARAGRAPH_COUNT)
        self.assertEqual(hc.template_paragraphs_sha256(), hc.TEMPLATE_PARAGRAPHS_SHA256)
        self.assertEqual(len(doc.tables), 0)
        full = "\n".join(texts)
        for ph in hc.PLACEHOLDERS:
            self.assertEqual(full.count(ph), 1, ph)
            runs = [r for p in doc.paragraphs for r in p.runs if ph in r.text]
            self.assertEqual(len(runs), 1, ph)
        self.assertEqual(set(re.findall(r"\{\{[^{}]*\}\}", full)), set(hc.PLACEHOLDERS))
        self.assertEqual(len(hc.PLACEHOLDERS), 12)
        self.assertEqual(EXPECTED_DOCX_TEMPLATES[hc.TEMPLATE_PATH], list(hc.PLACEHOLDERS))
        # 凍結条項は雛形の段落と逐語一致
        self.assertEqual(hc.FROZEN_CLAUSE, tuple(texts[9:11]))

    def test_tampered_template_detected(self):
        with patch.object(hc, "TEMPLATE_SHA256", "0" * 64):
            with self.assertRaises(hc.HoukiContractIntegrityError):
                hc.verify_template_integrity()
        with patch.object(hc, "TEMPLATE_PARAGRAPHS_SHA256", "0" * 64):
            self.assertNotEqual(hc.template_paragraphs_sha256(), hc.TEMPLATE_PARAGRAPHS_SHA256)

    def test_frozen_constants_pinned(self):
        self.assertEqual((hc.FEE_BASE, hc.FEE_ADDITIONAL, hc.DEPOSIT_PER_APPLICANT,
                          hc.EXTRA_SEND_FEE, hc.INCLUDED_DESTINATIONS),
                         (88000, 33000, 5000, 1100, 3))
        self.assertEqual(hc.REPRESENTATIVE_CLAUSE,
                         "甲らは、申述人{name}を代表者と定め、代表者が甲ら全員のために本契約に電子署名する。")
        self.assertEqual(hc.TOKUYAKU_NONE, "特になし")
        self.assertIn("88,000", hc.FROZEN_CLAUSE[1])
        self.assertIn("33,000", hc.FROZEN_CLAUSE[1])
        self.assertIn("1,100", hc.FROZEN_CLAUSE[1])
        self.assertEqual(cw.TOKUYAKU_MAX_CHARS, 600)


# ── 2. 費用計算 ───────────────────────────────────────────────────────────────
class TestFees(unittest.TestCase):
    def test_fees_by_applicants_and_destinations(self):
        cases = {
            (1, 0): (88000, 5000, 0, 0, 93000),
            (1, 3): (88000, 5000, 0, 0, 93000),
            (1, 4): (88000, 5000, 1, 1100, 94100),
            (2, 0): (121000, 10000, 0, 0, 131000),
            (3, 5): (154000, 15000, 2, 2200, 171200),
        }
        for (n, d), (fee, dep, cnt, extra, total) in cases.items():
            with self.subTest(n=n, d=d):
                f = hc.compute_fees(n, d)
                self.assertEqual((f["報酬合計"], f["実費合計"], f["追加送付件数"],
                                  f["追加送付料合計"], f["支払総額"]), (fee, dep, cnt, extra, total))
        self.assertEqual(hc.fmt_yen(154000), "154,000")

    def test_destinations_union_normalized(self):
        a = _rec("1", "甲", creditors=("株式会社ＡＢＣ", "ｱｺﾑ", " アコム ", "", "アコム　"))
        b = _rec("2", "乙", creditors=("株式会社ABC", "プロミス", "プロ　ミス"))
        d = hc.destinations([a, b])
        self.assertEqual(d, {"株式会社ABC", "アコム", "プロミス"})
        self.assertEqual(hc.normalize_creditor("  "), "")


# ── 3. 申述人一覧の展開・特約・署名 ───────────────────────────────────────────
class TestRender(unittest.TestCase):
    def _render(self, records, mode):
        fees = hc.compute_fees(len(records), len(hc.destinations(records)))
        return hc.render(records, mode, fees, NOW)

    def test_one_applicant_expansion_and_fill(self):
        r = _rec("1", "甲野一郎", addr="架空県架空市1-1", creditors=("アコム",))
        paras = _docx_paras(self._render([r], hc.SIGN_ALL))
        i = paras.index("甲（申述人）")
        self.assertEqual(paras[i + 1:i + 3], ["住所　架空県架空市1-1", "氏名　甲野一郎"])
        self.assertEqual(paras[i + 3], "乙（受任者）")
        self.assertIn("弁護士報酬（申述人1名分）　金88,000円", paras)
        self.assertIn("受理通知書の追加送付料（送付先0か所分）　金0円", paras)
        self.assertIn("実費預託金（申述人1名につき5,000円）　金5,000円", paras)
        self.assertIn("支払総額　金93,000円", paras)
        self.assertIn("〔契約日〕　2026年9月6日", paras)
        self.assertTrue(any("被相続人架空太郎（以下" in p for p in paras))
        self.assertEqual(paras[paras.index("特約事項") + 1], "特になし")
        self.assertFalse(any("{{" in p for p in paras))
        self.assertEqual(len(Document(io.BytesIO(self._render([r], hc.SIGN_ALL))).tables), 0)

    def test_three_applicants_expansion_blank_between(self):
        recs = [_rec("3", "甲野一郎", addr="住所A"), _rec("1", "甲野二郎", addr="住所B"),
                _rec("2", "甲野三郎", addr="住所C")]
        paras = _docx_paras(self._render(recs, hc.SIGN_ALL))
        i = paras.index("甲（申述人）")
        self.assertEqual(paras[i + 1:i + 9],
                         ["住所　住所A", "氏名　甲野一郎", "", "住所　住所B", "氏名　甲野二郎",
                          "", "住所　住所C", "氏名　甲野三郎"])
        self.assertIn("弁護士報酬（申述人3名分）　金154,000円", paras)
        self.assertIn("支払総額　金169,000円", paras)

    def test_tokuyaku_by_sign_mode(self):
        rep = _rec("1", "甲野一郎", tokuyaku="分割払いを認める")
        other = _rec("2", "甲野二郎")
        # 全員: 欄そのまま
        self.assertEqual(hc.compose_tokuyaku([rep, other], hc.SIGN_ALL), "分割払いを認める")
        # 代表者のみ（2 名以上）: 定型文 + 改行 + 欄
        self.assertEqual(hc.compose_tokuyaku([rep, other], hc.SIGN_REPRESENTATIVE),
                         "甲らは、申述人甲野一郎を代表者と定め、代表者が甲ら全員のために本契約に"
                         "電子署名する。\n分割払いを認める")
        # 代表者のみ・欄が空: 定型文のみ
        rep2 = _rec("1", "甲野一郎")
        self.assertEqual(hc.compose_tokuyaku([rep2, other], hc.SIGN_REPRESENTATIVE),
                         "甲らは、申述人甲野一郎を代表者と定め、代表者が甲ら全員のために本契約に"
                         "電子署名する。")
        # 1 名: 定型文なし（空なら 特になし）
        self.assertEqual(hc.compose_tokuyaku([rep2], hc.SIGN_REPRESENTATIVE), "特になし")
        self.assertEqual(hc.compose_tokuyaku([rep], hc.SIGN_REPRESENTATIVE), "分割払いを認める")
        # docx では 2 段落（定型文・欄）に分かれる
        paras = _docx_paras(self._render([rep, other], hc.SIGN_REPRESENTATIVE))
        i = paras.index("特約事項")
        self.assertTrue(paras[i + 1].startswith("甲らは、申述人甲野一郎を代表者と定め"))
        self.assertEqual(paras[i + 2], "分割払いを認める")

    def test_sign_mode_and_participants(self):
        rep = _rec("1", "甲野一郎", email=EMAIL_A)
        other = _rec("2", "甲野二郎", email=EMAIL_B)
        self.assertEqual(hc.sign_mode(_rec("1", "x", sign="全員")), ("全員", False))
        self.assertEqual(hc.sign_mode(_rec("1", "x", sign="代表者のみ")), ("代表者のみ", False))
        self.assertEqual(hc.sign_mode(_rec("1", "x", sign="")), ("代表者のみ", True))
        self.assertEqual(hc.participants([rep, other], hc.SIGN_ALL),
                         ([(EMAIL_A, "甲野一郎"), (EMAIL_B, "甲野二郎")], []))
        self.assertEqual(hc.participants([rep, other], hc.SIGN_REPRESENTATIVE),
                         ([(EMAIL_A, "甲野一郎")], []))
        parts, problems = hc.participants([rep, _rec("2", "甲野二郎", email="")], hc.SIGN_ALL)
        self.assertEqual(parts, [(EMAIL_A, "甲野一郎")])
        self.assertEqual(problems, ["メールアドレス 未入力（レコード番号 2）"])
        _p, problems = hc.participants([_rec("1", "x", email="bad")], hc.SIGN_ALL)
        self.assertEqual(problems, ["メールアドレス 形式不正（レコード番号 1）"])

    def test_frozen_clause_verified_at_render(self):
        r = _rec("1", "甲野一郎")
        fees = hc.compute_fees(1, 0)
        with patch.object(hc, "FROZEN_CLAUSE", (hc.FROZEN_CLAUSE[0], "改変")):
            with self.assertRaises(cw.ContractIntegrityError):
                hc.render([r], hc.SIGN_ALL, fees, NOW)
        pdf = contract_pdf.docx_to_pdf_bytes(hc.render([r], hc.SIGN_ALL, fees, NOW))
        cw.verify_frozen_pdf(pdf, cw._houki_cfg())
        with self.assertRaises(cw.ContractIntegrityError):
            cw.verify_frozen_pdf(pdf)                       # 時効版の第2条は含まれない


# ── 4. 受け口・状態機械（生成） ──────────────────────────────────────────────
class TestGenerateFlow(_Base):
    def test_token_gates(self):
        self.seed(_rec("1", "甲野一郎"))
        with patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": ""}):
            self.assertEqual(self.post().status_code, 404)
        with patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": os.environ["DOCUMENT_WEBHOOK_SECRET"]}):
            self.assertEqual(self.post(url="/souzoku-houki/contract/doc-secret").status_code, 404)
        self.assertEqual(self.post(url="/souzoku-houki/contract/wrong").status_code, 403)
        self.assertEqual(self.post(app="21").json().get("skip"), "app_mismatch")
        self.assertEqual(self.post(status="契約書作成中").json().get("skip"), "not_triggered")
        self.assertEqual(self.uploads, [])
        self.assertEqual(self.updates, [])

    def test_single_applicant_group_blank(self):
        self.seed(_rec("1", "甲野一郎", creditors=("アコム", "プロミス", "レイク", "アイフル")))
        r = self.post()
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(self.field("1", "契約書ステータス"), "契約書作成済")
        self.assertEqual(self.field("1", "委任契約書"), [{"fileKey": "fk-1"}])
        self.assertEqual(self.uploads[0][0], "委任契約書_相続放棄.docx")
        paras = _docx_paras(self.last_docx())
        self.assertIn("弁護士報酬（申述人1名分）　金88,000円", paras)
        self.assertIn("受理通知書の追加送付料（送付先1か所分）　金1,100円", paras)
        self.assertIn("支払総額　金94,100円", paras)
        self.assertEqual(self.kinds(), ["houki_contract_created"])
        text = self.texts()[0]
        self.assertIn("・申述人数: 1名", text)
        self.assertIn("・送付先数: 4か所（追加送付 1か所）", text)
        self.assertIn("・支払総額: 94,100円", text)
        self.assertIn("・署名方式: 代表者のみ（契約署名 が空のため代表者のみとして作成）", text)
        self.assertIn("・被相続人グループID 空・1 名として作成", text)
        self.assertNotIn("甲野一郎", text)
        # CAS 遷移: 作成→作成中→作成済（起点レコードのみ更新）
        self.assertEqual([u[1].get("契約書ステータス") for u in self.updates],
                         ["契約書作成中", "契約書作成済"])

    def test_group_of_three_representative_first_then_id_order(self):
        self.seed(_rec("5", "代表花子", addr="住所5", group="G1", sign="全員",
                       creditors=("アコム",)),
                  _rec("2", "二郎", addr="住所2", group="G1", email=EMAIL_B,
                       creditors=("ｱｺﾑ", "プロミス")),
                  _rec("9", "九郎", addr="住所9", group="G1", email=EMAIL_C),
                  _rec("7", "無関係", group="G2"))
        r = self.post(rid="5")
        self.assertEqual(r.status_code, 200, r.text)
        paras = _docx_paras(self.last_docx())
        i = paras.index("甲（申述人）")
        self.assertEqual(paras[i + 1:i + 9],
                         ["住所　住所5", "氏名　代表花子", "", "住所　住所2", "氏名　二郎", "",
                          "住所　住所9", "氏名　九郎"])
        self.assertIn("弁護士報酬（申述人3名分）　金154,000円", paras)
        self.assertIn("受理通知書の追加送付料（送付先0か所分）　金0円", paras)   # 2 か所 ≤ 3
        self.assertIn("支払総額　金169,000円", paras)
        self.assertEqual(paras[paras.index("特約事項") + 1], "特になし")
        # 他の申述人レコードは更新しない
        self.assertTrue(all(rid == "5" for rid, _f in self.updates))
        self.assertEqual(self.field("2", "契約書ステータス"), "契約書作成")

    def test_review_when_decedent_mismatch_or_missing_fields(self):
        for label, recs in {
            "decedent": [_rec("1", "甲", group="G", decedent="A"),
                         _rec("2", "乙", group="G", decedent="B")],
            "name": [_rec("1", "甲", group="G"), _rec("2", "", group="G")],
            "addr": [_rec("1", "甲", group="G"), _rec("2", "乙", group="G", addr="")],
        }.items():
            with self.subTest(label=label):
                self.setUp()
                self.seed(*recs)
                r = self.post()
                self.assertEqual(r.json().get("skip"), "houki_preconditions")
                self.assertEqual(self.field("1", "契約書ステータス"), "要確認")
                self.assertEqual(self.uploads, [])
                self.assertEqual(self.kinds(), ["houki_contract_needs_review"])
                self.assertIn("レコード番号 2" if label != "decedent" else "被相続人氏名",
                              self.texts()[0])

    def test_regeneration_guard_three_paths(self):
        # ① 契約書作成トリガ: 特約に {{ }} → 要確認（生成 0）
        self.seed(_rec("1", "甲", tokuyaku="{{x}}"))
        self.assertEqual(self.post().json().get("skip"), "tokuyaku_invalid")
        self.assertEqual(self.field("1", "契約書ステータス"), "要確認")
        # ② 作成中 reconcile（添付なし）: 特約 601 字 → 要確認
        self.setUp()
        self.seed(_rec("1", "甲", status="契約書作成中", tokuyaku="あ" * 601))
        self.assertEqual(self.post().json().get("skip"), "tokuyaku_too_long")
        self.assertEqual(self.field("1", "契約書ステータス"), "要確認")
        # ③ クラウドサイン登録: cloudsign_document_id 非空 → 要確認・API 0 回
        self.setUp()
        self.seed(_rec("1", "甲", status="クラウドサイン登録", cs_doc_id="doc-old",
                       attachment=[{"fileKey": "k"}]))
        self.assertEqual(self.post(status="クラウドサイン登録").json().get("skip"), "cs_registered")
        self.assertEqual(self.field("1", "契約書ステータス"), "要確認")
        self.cs_create.assert_not_called()
        self.assertEqual(self.kinds(), ["houki_contract_needs_review"])
        # 定型文は上限に含めない（欄 600 字ちょうど+代表者定型文でも生成される）
        self.setUp()
        self.seed(_rec("1", "甲", group="G", tokuyaku="あ" * 600),
                  _rec("2", "乙", group="G", email=EMAIL_B))
        self.assertEqual(self.post().status_code, 200)
        self.assertEqual(self.field("1", "契約書ステータス"), "契約書作成済")

    def test_cas_lost_and_already_done(self):
        self.seed(_rec("1", "甲", revision="5"))
        real_update = cw.hub_kintone.update_record

        async def conflict(app, rid, fields, revision=None):
            raise KintoneError(409, "GAIA_CO02", "c")
        with patch.object(cw.hub_kintone, "update_record", conflict):
            self.assertEqual(self.post().json().get("skip"), "cas_lost")
        self.assertEqual(self.uploads, [])
        self.records["1"]["契約書ステータス"] = {"value": "契約書作成済"}
        self.assertEqual(self.post().json().get("skip"), "already_done")

    def test_internal_error_notifies_failed(self):
        self.seed(_rec("1", "甲"))
        with patch.object(hc, "render", side_effect=RuntimeError("boom")):
            r = self.post()
        self.assertEqual(r.status_code, 500)
        self.assertEqual(self.kinds(), ["houki_contract_failed"])
        self.assertEqual(self.field("1", "契約書ステータス"), "契約書作成中")   # reconcile 対象


# ── 5. CloudSign 登録 ─────────────────────────────────────────────────────────
class TestCloudSignFlow(_Base):
    def _seed_registered(self, sign="全員", emails=(EMAIL_A, EMAIL_B)):
        return self.seed(
            _rec("1", "代表花子", group="G", sign=sign, email=emails[0],
                 status="クラウドサイン登録", attachment=[{"fileKey": "k"}]),
            _rec("2", "二郎", group="G", email=emails[1]))

    def test_all_signers(self):
        self._seed_registered("全員")
        r = self.post(status="クラウドサイン登録")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json().get("cloudsign"))
        self.cs_create.assert_called_once_with("1", "相続放棄_委任契約書_案件No.1")
        self.assertEqual(self.cs_attach.call_args.args[0], "doc-h1")
        self.assertEqual(self.cs_attach.call_args.args[2], "委任契約書_相続放棄.pdf")
        self.assertEqual([c.args for c in self.cs_participant.call_args_list],
                         [("doc-h1", EMAIL_A, "代表花子"), ("doc-h1", EMAIL_B, "二郎")])
        self.assertEqual(self.field("1", "cloudsign_document_id"), "doc-h1")
        self.assertEqual(self.field("1", "契約書ステータス"), "クラウドサイン登録済")
        self.assertEqual(self.kinds(), ["houki_contract_created"])
        self.assertIn("・署名方式: 全員", self.texts()[0])
        # PDF の凍結検証（第2条）と全文一致は共通経路で実行済み＝添付 PDF に {{ なし
        pdf = self.cs_attach.call_args.args[1]
        self.assertNotIn("{{", contract_pdf.pdf_text(pdf))

    def test_representative_only_and_blank(self):
        for sign in ("代表者のみ", ""):
            with self.subTest(sign=sign):
                self.setUp()
                self._seed_registered(sign, emails=(EMAIL_A, ""))     # 2 名目のメール空でも可
                r = self.post(status="クラウドサイン登録")
                self.assertEqual(r.status_code, 200, r.text)
                self.assertEqual([c.args for c in self.cs_participant.call_args_list],
                                 [("doc-h1", EMAIL_A, "代表花子")])
                text = self.texts()[0]
                self.assertIn("・署名方式: 代表者のみ", text)
                self.assertEqual("契約署名 が空のため" in text, sign == "")
                pdf_text = contract_pdf.pdf_text(self.cs_attach.call_args.args[1])
                self.assertIn("代表者と定め", "".join(pdf_text.split()))

    def test_email_missing_is_review(self):
        self._seed_registered("全員", emails=(EMAIL_A, ""))
        self.assertEqual(self.post(status="クラウドサイン登録").json().get("skip"),
                         "houki_preconditions")
        self.assertEqual(self.field("1", "契約書ステータス"), "要確認")
        self.cs_create.assert_not_called()
        self.assertIn("メールアドレス 未入力（レコード番号 2）", self.texts()[0])
        self.setUp()
        self._seed_registered("代表者のみ", emails=("", EMAIL_B))
        self.assertEqual(self.post(status="クラウドサイン登録").json().get("skip"),
                         "houki_preconditions")
        self.assertIn("レコード番号 1", self.texts()[0])

    def test_toctou_applicant_set_changed_before_send(self):
        self._seed_registered("全員")
        real_plan = hc.plan
        calls = {"n": 0}

        async def plan_then_change(record):
            calls["n"] += 1
            p = await real_plan(record)
            if calls["n"] == 1:                       # 1 回目の後に 3 人目が追加される
                self.records["3"] = _rec("3", "三郎", group="G", email=EMAIL_C)
            return p
        with patch.object(hc, "plan", plan_then_change):
            r = self.post(status="クラウドサイン登録")
        self.assertEqual(r.json().get("skip"), "applicants_changed")
        self.assertEqual(self.field("1", "契約書ステータス"), "要確認")
        self.cs_create.assert_not_called()
        self.assertEqual(self.kinds(), ["houki_contract_needs_review"])

    def test_docx_missing_is_precondition_notice_only(self):
        self.seed(_rec("1", "代表花子", status="クラウドサイン登録"))
        self.assertEqual(self.post(status="クラウドサイン登録").json().get("skip"),
                         "cs_preconditions")
        self.assertEqual(self.field("1", "契約書ステータス"), "クラウドサイン登録")
        self.assertEqual(self.kinds(), ["houki_contract_needs_review"])


# ── 6. cloudsign_webhook の App 40 対応 ───────────────────────────────────────
class TestCloudSignWebhookApp40(unittest.TestCase):
    def setUp(self):
        import cloudsign_webhook
        self.mod = cloudsign_webhook

    def _resp(self, records):
        m = MagicMock()
        m.raise_for_status = MagicMock()
        m.json.return_value = {"records": records}
        return m

    def test_found_in_app40_only(self):
        put = MagicMock()
        put.raise_for_status = MagicMock()
        with patch.dict(os.environ, {"APP_HOUKI": "40", "TOKEN_HOUKI": "t40"}), \
                patch.object(self.mod.requests, "get",
                             side_effect=[self._resp([]), self._resp([{"$id": {"value": "8"}}])]) as g, \
                patch.object(self.mod.requests, "put", return_value=put) as p:
            self.assertEqual(self.mod.update_kintone_status("doc-h", "受任"), "8")
        self.assertEqual([c.kwargs["params"]["app"] for c in g.call_args_list],
                         [self.mod.KINTONE_APP_ID, "40"])
        self.assertEqual(p.call_args.kwargs["json"]["app"], "40")
        self.assertEqual(p.call_args.kwargs["headers"]["X-Cybozu-API-Token"], "t40")
        self.assertEqual(p.call_args.kwargs["json"]["record"], {"status": {"value": "受任"}})

    def test_both_apps_match_is_fail_closed(self):
        with patch.dict(os.environ, {"APP_HOUKI": "40", "TOKEN_HOUKI": "t40"}), \
                patch.object(self.mod.requests, "get",
                             side_effect=[self._resp([{"$id": {"value": "1"}}]),
                                          self._resp([{"$id": {"value": "8"}}])]), \
                patch.object(self.mod.requests, "put") as p:
            with self.assertRaises(self.mod.AmbiguousDocumentMatch):
                self.mod.update_kintone_status("doc-x", "受任")
        p.assert_not_called()
        # handle_webhook: 処理せず封筒+通知・200
        with patch.object(self.mod, "verify_completed_document",
                          return_value=({"id": "doc-x", "status": 2}, "")), \
                patch.object(self.mod, "update_kintone_status",
                             side_effect=self.mod.AmbiguousDocumentMatch("x")), \
                patch.object(self.mod, "file_mismatch_envelope", return_value="77") as env, \
                patch.object(self.mod, "notify_business_line") as nb:
            code, body = self.mod.handle_webhook(
                self.mod.WEBHOOK_SECRET, {"documentID": "doc-x", "status": 2})
        self.assertEqual((code, body["state"]), (200, "ambiguous_match"))
        env.assert_called_once_with("doc-x", "kintone_ambiguous_match")
        self.assertIn("kintone_ambiguous_match", nb.call_args.args[0])
        self.assertNotIn("doc-x", nb.call_args.args[0])

    def test_neither_app_is_none(self):
        with patch.dict(os.environ, {"APP_HOUKI": "40", "TOKEN_HOUKI": "t40"}), \
                patch.object(self.mod.requests, "get", return_value=self._resp([])), \
                patch.object(self.mod.requests, "put") as p:
            self.assertIsNone(self.mod.update_kintone_status("doc-none", "受任"))
        p.assert_not_called()


# ── 7. 通知 kind の登録・時効側の cfg 不変 ───────────────────────────────────
class TestConfigAndKinds(unittest.TestCase):
    def test_notify_kinds_registered(self):
        for kind in ("houki_contract_created", "houki_contract_needs_review",
                     "houki_contract_failed"):
            with self.subTest(kind=kind), \
                    self.assertLogs(hub_notify.logger, level="INFO") as cm:
                hub_notify._log_throttled(f"{kind}:12")
                self.assertIn(f"kind={kind}", "\n".join(cm.output))
                self.assertNotIn("unknown_kind", "\n".join(cm.output))

    def test_channel_configs(self):
        j, h = cw._jikou_cfg(), cw._houki_cfg()
        self.assertEqual((j.name, j.template_path, j.frozen_clause, j.output_pdf_name),
                         ("jikou", cw.TEMPLATE_PATH, cw.FROZEN_CLAUSE, cw.OUTPUT_PDF_NAME))
        self.assertEqual((h.name, h.template_path, h.template_sha256, h.output_filename,
                          h.output_pdf_name),
                         ("houki", hc.TEMPLATE_PATH, hc.TEMPLATE_SHA256,
                          "委任契約書_相続放棄.docx", "委任契約書_相続放棄.pdf"))
        self.assertEqual(h.app.app_id(), "40")
        self.assertEqual(h.cs_title("7"), "相続放棄_委任契約書_案件No.7")
        self.assertEqual(j.cs_title("7"), "委任契約書_案件No.7")
        # 時効側の定数を patch すると jikou cfg は追随する（既存テスト互換）
        with patch.object(cw, "TEMPLATE_SHA256", "0" * 64):
            self.assertEqual(cw._jikou_cfg().template_sha256, "0" * 64)


if __name__ == "__main__":
    unittest.main()
