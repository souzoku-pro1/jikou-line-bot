"""CONTRACT-GEN-2: 委任契約書の PDF 化+CloudSign 自動登録（送信はしない）。

固定する仕様:
- 本文 gate はトリガ 2 値（契約書作成/クラウドサイン登録）のみ通過。
  dispatch は正本ステータスに対して行う（登録済=already_done・
  登録中=自動再実行せず常に「要確認」+通知・二重下書き防止）。
- fail-closed 前提: 委任契約書 docx 添付あり（第1版実行済み）・メール
  アドレス存在+簡易 grammar 適合・v1 必須（氏名/住所/債権者）充足。
  未充足は状態不変・不足フィールド名のみ通知（値は非搭載）。
- CAS: 登録→登録中 勝者のみ CloudSign 作成（並行 2 本でも作成 1 回）。
  完了 PUT は revision=claim+1 で cloudsign_document_id+登録済。
- PDF 凍結検証: テンプレ docx=単一の正（fill_template 済み docx から描画・
  本文をコードに持たない）。抽出テキストの全空白除去列に対する第2条の
  逐語連続一致+差し込みキー残存なし（不一致は登録せず 500）。
- CloudSign 送信 API（PUT /documents/{id}）は呼ばない（source 走査で pin・
  送信操作は大野が CloudSign 画面で行う対外効果の一線）。
- 途中失敗: 下書き削除成功→「クラウドサイン登録」へ巻き戻し→500（再配送で
  自動再試行）。削除失敗→巻き戻さず 500（reconcile が「要確認」へ）。
  書き戻し PUT 失敗→下書きは削除しない（完成下書きを人が回収）。
"""

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
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import cloudsign_webhook  # noqa: E402
import contract_pdf  # noqa: E402
import contract_webhook as cw  # noqa: E402
import main  # noqa: E402
from config import EXPECTED_KINTONE_SCHEMA  # noqa: E402
from hub.kintone import KintoneError  # noqa: E402
from test_contract_gen1 import _rec, _tampered_docx  # noqa: E402

_client = TestClient(main.app)
_URL = "/contract/doc-secret"
_EMAIL = "hanako@example.com"


def _cs_record(**over):
    base = {"$revision": "5", "契約書ステータス": "クラウドサイン登録",
            "顧客名": "熊澤花子", "住所": "埼玉県川口市青木1-1-1",
            "問い合わせ業者名": "株式会社Aファイナンス",
            "対象債権者2": "", "対象債権者3": "",
            "メールアドレス": _EMAIL,
            "委任契約書": [{"fileKey": "f0"}]}
    base.update(over)
    return _rec(**base)


def _body(record_id="12", status="クラウドサイン登録"):
    return {"app": {"id": "21"},
            "record": {"$id": {"value": record_id},
                       "契約書ステータス": {"value": status}}}


def _filled_pdf() -> bytes:
    docx = cw.fill_template(cw.TEMPLATE_PATH,
                            cw.build_fill_data(_cs_record()))
    return contract_pdf.docx_to_pdf_bytes(docx)


class _CsBase(unittest.TestCase):
    def _post(self, *, record, body=None, update=None, create=None,
              attach=None, participant=None, delete=None):
        create = create or MagicMock(return_value="doc-1")
        attach = attach or MagicMock()
        participant = participant or MagicMock()
        delete = delete or MagicMock(return_value=True)
        update = update or AsyncMock()
        notify = AsyncMock(return_value=True)
        get = AsyncMock(return_value=record)
        with patch.dict(os.environ, _ENV), \
             patch.object(cw.hub_kintone, "get_record", get), \
             patch.object(cw.hub_kintone, "update_record", update), \
             patch.object(cw, "_cs_create_document", create), \
             patch.object(cw, "_cs_attach_pdf", attach), \
             patch.object(cw, "_cs_add_participant", participant), \
             patch.object(cw, "_cs_delete_draft", delete), \
             patch("hub.notify.notify_admin_line", notify):
            r = _client.post(_URL, json=body or _body())
        return r, create, attach, participant, delete, update, notify, get


class TestPdfGeneration(unittest.TestCase):
    """PDF 化: テンプレ=単一の正・凍結検証は第1版同水準。"""

    @classmethod
    def setUpClass(cls):
        cls.pdf = _filled_pdf()
        cls.text = contract_pdf.pdf_text(cls.pdf)

    def test_pdf_header_and_content(self):
        self.assertTrue(self.pdf.startswith(b"%PDF"))
        flat = "".join(self.text.split())
        self.assertEqual(flat.count("熊澤花子"), 2)   # 差し込み 2 箇所
        self.assertIn("株式会社Aファイナンス", flat)

    def test_no_unfilled_keys_in_pdf(self):
        self.assertNotIn("{{", self.text)
        self.assertNotIn("}}", self.text)

    def test_frozen_clause_verbatim_in_pdf(self):
        # 全空白除去後の連結文字列に第2条（見出し+3 項）が連続部分列として
        # 文字単位で逐語一致（PDF の折返し・改行差を吸収する正規化）
        flat = "".join(self.text.split())
        frozen = "".join("".join(p.split()) for p in cw.FROZEN_CLAUSE)
        self.assertIn(frozen, flat)
        cw.verify_frozen_pdf(self.pdf)               # 実行時検証も同判定

    def test_tampered_pdf_rejected(self):
        # 報酬 44,000→40,000 改変（弁護士凍結事項）は PDF 段でも検知
        pdf = contract_pdf.docx_to_pdf_bytes(_tampered_docx())
        with self.assertRaises(cw.ContractIntegrityError):
            cw.verify_frozen_pdf(pdf)

    def test_unfilled_template_pdf_rejected(self):
        # 未差し込みテンプレ（{{キー}} 残存）は登録しない
        pdf = contract_pdf.docx_to_pdf_bytes(
            open(cw.TEMPLATE_PATH, "rb").read())
        with self.assertRaises(cw.ContractIntegrityError):
            cw.verify_frozen_pdf(pdf)


class TestEmailGrammar(unittest.TestCase):
    def test_closed_grammar(self):
        # 簡易 grammar: ASCII local@domain.TLD のみ（fail-closed）
        for ok in ("a@example.com", "a.b+c-d_e@ex-ample.co.jp"):
            self.assertIsNotNone(cw._EMAIL_RE.fullmatch(ok), ok)
        for ng in ("", "はなこ@example.com", "a b@example.com",
                   "a@example", "a@@example.com", "a@例.jp",
                   "a@example.com "):
            self.assertIsNone(cw._EMAIL_RE.fullmatch(ng), repr(ng))


class TestCsEntry(_CsBase):
    def test_self_update_echo_not_triggered(self):
        # 自 update の echo（登録中/登録済）は本文 gate で作用 0
        for status in ("クラウドサイン登録中", "クラウドサイン登録済"):
            with self.subTest(status=status):
                r, create, _a, _p, _d, update, _n, get = self._post(
                    record=_cs_record(), body=_body(status=status))
                self.assertEqual(r.json().get("skip"), "not_triggered")
                get.assert_not_awaited()
                create.assert_not_called()

    def test_redelivery_after_done_zero_effects(self):
        r, create, _a, _p, _d, update, _n, _g = self._post(
            record=_cs_record(契約書ステータス="クラウドサイン登録済"))
        self.assertEqual(r.json().get("skip"), "already_done")
        create.assert_not_called()
        update.assert_not_awaited()

    def test_dispatch_follows_authoritative_status(self):
        # stale な v1 本文（契約書作成）でも正本が「クラウドサイン登録」なら
        # v2 を実行（dispatch は正本が正）
        r, create, _a, _p, _d, _u, _n, _g = self._post(
            record=_cs_record(), body=_body(status="契約書作成"))
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json().get("cloudsign"))
        create.assert_called_once()


class TestCsFailClosed(_CsBase):
    def test_preconditions_rejected_no_state_change(self):
        cases = {
            "メール欠落": (_cs_record(メールアドレス=""), "メールアドレス"),
            "メール形式不正": (_cs_record(メールアドレス="はなこ@例.jp"),
                               "形式不正"),
            "docx未添付": (_cs_record(委任契約書=[]), "docx 未添付"),
            "住所欠落": (_cs_record(住所=""), "住所"),
        }
        for label, (record, needle) in cases.items():
            with self.subTest(case=label):
                r, create, _a, _p, _d, update, notify, _g = self._post(
                    record=record)
                self.assertEqual(r.json().get("skip"), "cs_preconditions")
                create.assert_not_called()
                update.assert_not_awaited()      # 状態も動かさない
                notify.assert_awaited_once()
                sent = notify.await_args.args[0]
                self.assertIn(needle, sent)
                self.assertNotIn("hanako", sent)  # 値は通知に載せない
                self.assertNotIn("熊澤", sent)

    def test_tampered_output_rejected_before_cloudsign(self):
        with patch.object(cw, "fill_template",
                          lambda *_a, **_k: _tampered_docx()):
            r, create, _a, _p, _d, update, _n, _g = self._post(
                record=_cs_record())
        self.assertEqual(r.status_code, 500)
        create.assert_not_called()               # CloudSign には触れない
        self.assertEqual(update.await_count, 1)  # CAS のみ（登録中のまま）


class TestCsStateMachine(_CsBase):
    def test_happy_path(self):
        r, create, attach, participant, delete, update, notify, _g = \
            self._post(record=_cs_record())
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json().get("cloudsign"))
        # CAS（登録→登録中）→ 完了 PUT（doc id+登録済・revision=claim+1）
        self.assertEqual(update.await_count, 2)
        cas_call, final_call = update.await_args_list
        self.assertEqual(cas_call.args[2],
                         {"契約書ステータス": "クラウドサイン登録中"})
        self.assertEqual(cas_call.kwargs.get("revision"), "5")
        self.assertEqual(final_call.args[2], {
            "cloudsign_document_id": "doc-1",
            "契約書ステータス": "クラウドサイン登録済"})
        self.assertEqual(final_call.kwargs.get("revision"), "6")
        # CloudSign: 作成→PDF 添付→宛先（氏名+メール）・削除なし・通知なし
        create.assert_called_once_with("12")
        doc_id, pdf_bytes = attach.call_args.args
        self.assertEqual(doc_id, "doc-1")
        self.assertTrue(pdf_bytes.startswith(b"%PDF"))
        cw.verify_frozen_pdf(pdf_bytes)          # 添付物は凍結検証済み実体
        participant.assert_called_once_with("doc-1", _EMAIL, "熊澤花子")
        delete.assert_not_called()
        notify.assert_not_awaited()

    def test_concurrent_second_request_loses_cas(self):
        loser = AsyncMock(side_effect=KintoneError(409, "GAIA_CO02"))
        r, create, _a, _p, _d, update, _n, _g = self._post(
            record=_cs_record(), update=loser)
        self.assertEqual(r.json().get("skip"), "cas_lost")
        create.assert_not_called()               # 敗者は CloudSign 作成 0 回
        self.assertEqual(update.await_count, 1)

    def test_claim_failures_not_silenced(self):
        # fix2 規則を継承: 409 のみ cas_lost・障害系は 500 で再配送へ
        for err, want in ((KintoneError(500, "GAIA_XX01", "x"), 500),
                          (KintoneError(0, "transport_error", "x"), 500)):
            with self.subTest(status=err.status):
                update = AsyncMock(side_effect=err)
                r, create, _a, _p, _d, _u, _n, _g = self._post(
                    record=_cs_record(), update=update)
                self.assertEqual(r.status_code, want)
                create.assert_not_called()

    def test_participant_failure_cleans_draft_and_rolls_back(self):
        # 途中失敗: 下書き削除（部分状態を残さない）+ 掃除成功時は
        # 登録へ巻き戻し → 500（再配送で自動再試行）
        participant = MagicMock(side_effect=RuntimeError("cs down"))
        r, _c, _a, _p, delete, update, _n, _g = self._post(
            record=_cs_record(), participant=participant)
        self.assertEqual(r.status_code, 500)
        delete.assert_called_once_with("doc-1")
        self.assertEqual(update.await_count, 2)
        rollback = update.await_args_list[1]
        self.assertEqual(rollback.args[2],
                         {"契約書ステータス": "クラウドサイン登録"})
        self.assertEqual(rollback.kwargs.get("revision"), "6")

    def test_cleanup_failure_keeps_working_state(self):
        # 掃除失敗 → 巻き戻さない（登録中のまま→reconcile が要確認へ）
        participant = MagicMock(side_effect=RuntimeError("cs down"))
        delete = MagicMock(return_value=False)
        r, _c, _a, _p, _d, update, _n, _g = self._post(
            record=_cs_record(), participant=participant, delete=delete)
        self.assertEqual(r.status_code, 500)
        self.assertEqual(update.await_count, 1)  # CAS のみ・巻き戻しなし

    def test_create_failure_rolls_back_without_delete(self):
        create = MagicMock(side_effect=RuntimeError("cs down"))
        r, _c, attach, _p, delete, update, _n, _g = self._post(
            record=_cs_record(), create=create)
        self.assertEqual(r.status_code, 500)
        delete.assert_not_called()               # 下書き未作成=掃除対象なし
        attach.assert_not_called()
        self.assertEqual(update.await_count, 2)  # CAS+巻き戻し
        self.assertEqual(update.await_args_list[1].args[2],
                         {"契約書ステータス": "クラウドサイン登録"})

    def test_rollback_failure_still_reports_original_500(self):
        participant = MagicMock(side_effect=RuntimeError("cs down"))
        update = AsyncMock(side_effect=[None,
                                        KintoneError(409, "GAIA_CO02")])
        r, _c, _a, _p, _d, _u, _n, _g = self._post(
            record=_cs_record(), participant=participant, update=update)
        self.assertEqual(r.status_code, 500)     # 巻き戻し失敗は握って元の 500

    def test_writeback_failure_keeps_completed_draft(self):
        # 書き戻し PUT 失敗: 完成下書きは削除しない（人が CloudSign 画面で
        # 回収）。登録中のまま → 再配送は reconcile で要確認へ
        update = AsyncMock(side_effect=[None,
                                        KintoneError(500, "GAIA_XX01", "x")])
        r, _c, _a, _p, delete, _u, _n, _g = self._post(
            record=_cs_record(), update=update)
        self.assertEqual(r.status_code, 500)
        delete.assert_not_called()

    def test_reconcile_working_always_review_no_rerun(self):
        # 登録中の reconcile: 自動再実行せず常に CAS で要確認+通知
        # （CloudSign 側の外部状態=下書き重複を機械確認できないため）
        record = _cs_record(契約書ステータス="クラウドサイン登録中")
        r, create, _a, _p, _d, update, notify, _g = self._post(record=record)
        self.assertEqual(r.json().get("skip"), "cs_needs_review")
        create.assert_not_called()
        self.assertEqual(update.await_count, 1)
        self.assertEqual(update.await_args.args[2],
                         {"契約書ステータス": "要確認"})
        self.assertEqual(update.await_args.kwargs.get("revision"), "5")
        notify.assert_awaited_once()
        self.assertIn("下書き", notify.await_args.args[0])

    def test_reconcile_cas_lost_no_notify(self):
        record = _cs_record(契約書ステータス="クラウドサイン登録中")
        loser = AsyncMock(side_effect=KintoneError(409, "GAIA_CO02"))
        r, _c, _a, _p, _d, _u, notify, _g = self._post(
            record=record, update=loser)
        self.assertEqual(r.json().get("skip"), "cas_lost")
        notify.assert_not_awaited()


class TestSendApiNeverCalled(unittest.TestCase):
    def test_no_put_in_source(self):
        # 裁定済み方針の pin: 送信 API（PUT /documents/{id}）は呼ばない。
        # _cs_request の method は POST/DELETE の閉集合
        src = open("contract_webhook.py", encoding="utf-8").read()
        methods = set(re.findall(r'_cs_request\(\s*"([A-Z]+)"', src))
        self.assertTrue(methods)
        self.assertLessEqual(methods, {"POST", "DELETE"})
        self.assertNotIn('"PUT"', src)
        self.assertNotIn("'PUT'", src)


class TestCloudSignHttp(unittest.TestCase):
    def test_create_document_no_pii_in_title(self):
        resp = MagicMock()
        resp.json.return_value = {"id": "d-9"}
        with patch.object(cw, "_cs_request",
                          MagicMock(return_value=resp)) as req:
            self.assertEqual(cw._cs_create_document("12"), "d-9")
        method, path = req.call_args.args
        self.assertEqual((method, path), ("POST", "/documents"))
        title = req.call_args.kwargs["data"]["title"]
        self.assertIn("12", title)
        self.assertNotIn("熊澤", title)          # タイトルは案件 No のみ

    def test_create_document_missing_id_rejected(self):
        resp = MagicMock()
        resp.json.return_value = {}
        with patch.object(cw, "_cs_request", MagicMock(return_value=resp)):
            with self.assertRaises(cw.ContractIntegrityError):
                cw._cs_create_document("12")

    def test_attach_pdf_multipart_shape(self):
        with patch.object(cw, "_cs_request", MagicMock()) as req:
            cw._cs_attach_pdf("d-9", b"%PDF-x")
        self.assertEqual(req.call_args.args,
                         ("POST", "/documents/d-9/files"))
        name, payload, mime = req.call_args.kwargs["files"]["uploadfile"]
        self.assertEqual(name, cw.OUTPUT_PDF_NAME)
        self.assertEqual(payload, b"%PDF-x")
        self.assertEqual(mime, "application/pdf")

    def test_add_participant_shape(self):
        with patch.object(cw, "_cs_request", MagicMock()) as req:
            cw._cs_add_participant("d-9", _EMAIL, "熊澤花子")
        self.assertEqual(req.call_args.args,
                         ("POST", "/documents/d-9/participants"))
        self.assertEqual(req.call_args.kwargs["data"],
                         {"email": _EMAIL, "name": "熊澤花子"})

    def test_token_401_retried_once(self):
        class _FakeToken:
            def __init__(self):
                self.invalidated = False

            def get(self):
                return "t2" if self.invalidated else "t1"

            def invalidate(self):
                self.invalidated = True

        fake = _FakeToken()
        r401 = MagicMock(status_code=401)
        r200 = MagicMock(status_code=200)
        with patch("requests.request",
                   MagicMock(side_effect=[r401, r200])) as req, \
             patch.object(cloudsign_webhook, "_token", fake):
            resp = cw._cs_request("POST", "/documents", data={"title": "t"})
        self.assertIs(resp, r200)
        self.assertEqual(req.call_count, 2)
        self.assertTrue(fake.invalidated)
        headers = [c.kwargs["headers"]["Authorization"]
                   for c in req.call_args_list]
        self.assertEqual(headers, ["Bearer t1", "Bearer t2"])


class TestSchemaPin(unittest.TestCase):
    def test_status_options_closed_seven(self):
        opts = (EXPECTED_KINTONE_SCHEMA["App 21 (案件)"]["fields"]
                ["契約書ステータス"]["required_options"])
        self.assertEqual(opts, [
            "契約書作成", "契約書作成中", "契約書作成済",
            "クラウドサイン登録", "クラウドサイン登録中",
            "クラウドサイン登録済", "要確認"])


if __name__ == "__main__":
    unittest.main()
