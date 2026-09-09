"""LABEL-PRINT-1: 宛名ラベル印字（A-one 31514・使いかけシートの指定面）のテスト

検証（票 第 2 段 5）:
- レイアウト寸法 pin（共有線の格子 縦 [12,106,200]／横 [293,236,179,122,65,8]・既定オフセット 0/0・
  全 10 面の領域）・faces 指定（指定面以外は空白・文字位置は面の左下から x+6/y+31/y+13）・
  既存 A4_2x6 不変（仕様値・面原点・13 件 2 頁・PRINT_OFFSET 系）
- 残量の状態遷移（印刷済／戻す／新しいシート／満杯時の提案なし）＝sqlite の実 DB
- 指示の構文（各書式・未対応宛先の返答・敬称上書き・全角）
- 宛先解決（依頼者・債権者 n・役所・発送の 4 種・郵便番号なし・未対応/不足の返答）
- 添付先の FILE 欄「宛名ラベル」（既存添付を残す・ファイル名に日付と面番号）・冪等（同キー再要求は添付しない）
- RV-10（復唱・返信に氏名/住所が出ない）・直接一致（Claude 解析を経ない）・既存語彙の非干渉
kintone / parser は全てモック。DB は file sqlite（rv04b の流儀）。
"""

import asyncio
import os
import re
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")

from reportlab.lib.units import mm  # noqa: E402

from dispatch_bot import handler, label_print_task as lp, parser, registry  # noqa: E402
from hub import address_label as al, db, kintone, label_sheet  # noqa: E402

LAYOUT = "A4_2x5_aone31514"
_ENV = {"APP_HOUKI": "40", "TOKEN_HOUKI": "t40", "SOUZOKU_KINTONE_APP_ID": "26",
        "SOUZOKU_KINTONE_API_TOKEN": "t26", "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
        "APP_CITY_MASTER": "31", "TOKEN_CITY_MASTER": "t31"}

NAME40, ADDR40 = "放棄太郎", "埼玉県川口市西青木9-9-9"
NAME21, ADDR21 = "時効花子", "東京都新宿区3-3-3"
CRED1, CRED1_ZIP, CRED1_ADDR = "甲社", "100-0001", "東京都千代田区1-1-1"
CRED2, CRED2_ADDR = "乙社", "大阪府大阪市2-2-2"
SHIP_NAME, SHIP_ZIP, SHIP_ADDR = "丙運送株式会社", "333-0001", "埼玉県川口市4-4-4"
MUNI_ZIP, MUNI_ADDR = "332-8601", "埼玉県川口市青木2-1-1"
PII = (NAME40, ADDR40, NAME21, ADDR21, CRED1, CRED1_ADDR, CRED2, CRED2_ADDR,
       SHIP_NAME, SHIP_ADDR, MUNI_ADDR, "9-9-9", "3-3-3")

PARSE_CONFIRM = {"intent": "confirm", "task_type": None, "customer_name": None,
                 "task_params": {}, "confidence": "high", "missing_fields": [],
                 "clarification": None}
PARSE_CANCEL = {**PARSE_CONFIRM, "intent": "cancel"}
PARSE_SOUFU = {"intent": "task", "task_type": "soufu_annai", "customer_name": "鈴木",
               "task_params": {}, "confidence": "high", "missing_fields": [],
               "clarification": None}


def rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


def row(rid, name, zip_code, addr):
    return {"id": rid, "value": rec(債権者名=name, 債権者郵便番号=zip_code, 債権者住所=addr)}


def case40(files=None, rows=None):
    return rec(**{"$id": "17", "顧客名": NAME40, "住所": ADDR40,
                  "宛名ラベル": files if files is not None else [{"fileKey": "old-1", "name": "x.pdf"}],
                  "債権者一覧": rows if rows is not None else [
                      row("1", CRED1, CRED1_ZIP, CRED1_ADDR), row("2", CRED2, "", CRED2_ADDR)]})


def case21():
    return rec(**{"$id": "5", "顧客名": NAME21, "住所": ADDR21, "宛名ラベル": []})


def ship120(unit="相続放棄"):
    return rec(**{"$id": "120", "宛先名": SHIP_NAME, "宛先郵便番号": SHIP_ZIP,
                  "宛先住所": SHIP_ADDR, "ユニット種別": unit})


def muni():
    return rec(市区町村名="川口市", 担当部署="市民課", 郵便番号=MUNI_ZIP, 住所=MUNI_ADDR)


def run(coro):
    return asyncio.run(coro)


def draw_calls(fn):
    """render 中の drawString 呼び出しを (x_mm, y_mm, text) で捕捉（用紙左下原点）"""
    with patch.object(al.canvas.Canvas, "drawString", autospec=True) as ds:
        fn()
    return [(round(c.args[1] / mm, 1), round(c.args[2] / mm, 1), c.args[3])
            for c in ds.call_args_list]


# ══════════════════════════════════════════════════════════════
# 1. レイアウト寸法 pin・faces・A4_2x6 不変
# ══════════════════════════════════════════════════════════════

class TestLayoutPin(unittest.TestCase):
    def setUp(self):
        self._env = patch.dict(os.environ, {"LABEL_PRINT_OFFSET_X_MM": "", "LABEL_PRINT_OFFSET_Y_MM": "",
                                            "PRINT_OFFSET_X_MM": "", "PRINT_OFFSET_Y_MM": ""})
        self._env.start()
        self.addCleanup(self._env.stop)

    def test_31514_grid_is_frozen(self):
        """大野の校正（2026-09-09・13 回目で確定）の格子を定数として pin"""
        spec = al.layout_spec(LAYOUT)
        self.assertEqual(spec.grid_x_mm, (12.0, 106.0, 200.0))
        self.assertEqual(spec.grid_y_mm, (293.0, 236.0, 179.0, 122.0, 65.0, 8.0))
        self.assertEqual((spec.cols, spec.rows, spec.per_page), (2, 5, 10))
        self.assertEqual(spec.default_offset_mm, (0.0, 0.0))
        self.assertEqual(al._spec_offsets(spec), (0.0, 0.0))
        self.assertEqual(spec.offset_env, "label")
        self.assertTrue(spec.omit_empty_zip)
        self.assertEqual(spec.text_h_mm, 55.0)

    def test_31514_all_ten_face_boxes(self):
        spec = al.layout_spec(LAYOUT)
        expected = {1: (12, 236), 2: (106, 236), 3: (12, 179), 4: (106, 179), 5: (12, 122),
                    6: (106, 122), 7: (12, 65), 8: (106, 65), 9: (12, 8), 10: (106, 8)}
        for face, (x, y) in expected.items():
            self.assertEqual(spec.face_box(face), (x, y, 94.0, 57.0), face)
            self.assertEqual(spec.face_origin(face), (x, y))
        with self.assertRaises(ValueError):
            spec.face_box(11)
        with self.assertRaises(ValueError):
            spec.face_box(0)

    def test_label_offset_env_is_separate_from_print_offset(self):
        spec = al.layout_spec(LAYOUT)
        with patch.dict(os.environ, {"PRINT_OFFSET_X_MM": "9", "PRINT_OFFSET_Y_MM": "9"}):
            self.assertEqual(al._spec_offsets(spec), (0.0, 0.0))
        with patch.dict(os.environ, {"LABEL_PRINT_OFFSET_X_MM": "1.5", "LABEL_PRINT_OFFSET_Y_MM": "-2"}):
            self.assertEqual(al._spec_offsets(spec), (1.5, -2.0))

    def test_faces_draws_only_specified_face_at_grid_origin(self):
        """面 4（原点 106,179）: 文字は左下から x+6・住所 y+31・宛名 y+13。〒 は空なら省略"""
        addr = {"宛先名": "テスト太郎", "郵便番号": "", "住所": "埼玉県川口市西青木9-9-9", "敬称": "様"}
        calls = draw_calls(lambda: al.render_label_sheet([addr], layout=LAYOUT, faces=[4]))
        self.assertEqual(calls, [(112.0, 210.0, "埼玉県川口市西青木9-9-9"),
                                 (112.0, 192.0, "テスト太郎　様")])

    def test_faces_with_zip_and_two_faces(self):
        a = {"宛先名": "甲", "郵便番号": "100-0001", "住所": "住所A"}
        b = {"宛先名": "乙", "郵便番号": "", "住所": "住所B", "敬称": "御中"}
        calls = draw_calls(lambda: al.render_label_sheet([a, b], layout=LAYOUT, faces=[1, 10]))
        self.assertEqual(calls, [(18.0, 279.0, "〒100-0001"), (18.0, 267.0, "住所A"),
                                 (18.0, 249.0, "甲　様"),
                                 (112.0, 39.0, "住所B"), (112.0, 21.0, "乙　御中")])

    def test_faces_validation(self):
        a = {"宛先名": "甲", "郵便番号": "", "住所": "x"}
        with self.assertRaises(ValueError):
            al.render_label_sheet([a, a], layout=LAYOUT, faces=[1])
        with self.assertRaises(ValueError):
            al.render_label_sheet([a, a], layout=LAYOUT, faces=[2, 2])
        with self.assertRaises(ValueError):
            al.render_label_sheet([a], layout=LAYOUT, faces=[11])

    def test_faces_output_is_single_page_and_deterministic(self):
        a = {"宛先名": "甲", "郵便番号": "", "住所": "x"}
        pdf1 = al.render_label_sheet([a], layout=LAYOUT, faces=[4])
        pdf2 = al.render_label_sheet([a], layout=LAYOUT, faces=[4])
        self.assertEqual(pdf1, pdf2)
        self.assertEqual(pdf1.count(b"/Type /Page\n"), 1)
        self.assertNotEqual(pdf1, al.render_label_sheet([a], layout=LAYOUT, faces=[5]))

    def test_calibration_page_draws_shared_grid_lines_only(self):
        with patch.object(al.canvas.Canvas, "line", autospec=True) as ln, \
                patch.object(al.canvas.Canvas, "rect", autospec=True) as rc:
            al.render_label_calibration(LAYOUT)
        self.assertEqual(rc.call_count, 0)
        lines = sorted({tuple(round(v / mm, 1) for v in c.args[1:]) for c in ln.call_args_list})
        self.assertEqual(len(lines), 9)
        self.assertIn((12.0, 8.0, 12.0, 293.0), lines)
        self.assertIn((12.0, 293.0, 200.0, 293.0), lines)

    def test_a4_2x6_unchanged(self):
        """既存 A4_2x6（T1-3 の等分レイアウト）は仕様値・面原点・頁分割とも従来どおり"""
        spec = al.layout_spec("A4_2x6")
        self.assertEqual((spec.cols, spec.rows, spec.label_w_mm, spec.label_h_mm), (2, 6, 105.0, 49.5))
        self.assertEqual((spec.top_mm, spec.left_mm, spec.pitch_x_mm, spec.pitch_y_mm), (0.0, 0.0, 105.0, 49.5))
        self.assertEqual(spec.offset_env, "print")
        self.assertFalse(spec.omit_empty_zip)
        self.assertFalse(spec.is_grid)
        self.assertEqual(spec.face_origin(1), (0.0, 247.5))
        self.assertEqual(spec.face_origin(2), (105.0, 247.5))
        self.assertEqual(spec.face_origin(12), (105.0, 0.0))
        a = {"宛先名": "乙", "郵便番号": "", "住所": "住所B", "敬称": "行"}
        calls = draw_calls(lambda: al.render_label_sheet([a]))
        self.assertEqual(calls, [(6.0, 285.0, "〒"), (6.0, 273.0, "住所B"), (6.0, 255.0, "乙　行")])
        many = [{"宛先名": f"n{i}", "郵便番号": "1", "住所": "a"} for i in range(13)]
        self.assertEqual(al.render_label_sheet(many).count(b"/Type /Page\n"), 2)
        with patch.dict(os.environ, {"PRINT_OFFSET_X_MM": "2", "PRINT_OFFSET_Y_MM": "-1"}):
            calls = draw_calls(lambda: al.render_label_sheet([a]))
        self.assertEqual(calls[1], (8.0, 272.0, "住所B"))


# ══════════════════════════════════════════════════════════════
# 2. 残量の状態遷移（sqlite 実 DB）
# ══════════════════════════════════════════════════════════════

class _DbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="lbl_")
        self._envp = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/label.db", **_ENV})
        self._envp.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(label_sheet.metadata.create_all)
        asyncio.run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._envp.stop()
        shutil.rmtree(self._dir, ignore_errors=True)


class TestSheetState(_DbMixin):
    def test_initial_sheet_and_next_free(self):
        self.assertIsNone(run(label_sheet.get_state()))
        st = run(label_sheet.ensure_state("U1"))
        self.assertEqual((st.sheet_id, st.used, st.pending, st.history), ("S-1", (), (), ()))
        self.assertEqual(st.next_free(10), 1)
        self.assertEqual(st.remaining(10), 10)
        self.assertEqual(run(label_sheet.ensure_state("U1")).sheet_id, "S-1")  # 二重作成しない

    def test_pending_reserves_face_but_does_not_consume(self):
        run(label_sheet.ensure_state("U1"))
        st = run(label_sheet.mark_pending(1, "U1"))
        self.assertEqual((st.used, st.pending), ((), (1,)))
        self.assertEqual(st.next_free(10), 2)
        self.assertEqual(st.remaining(10), 9)
        self.assertEqual(run(label_sheet.mark_pending(1, "U1")).pending, (1,))  # 冪等

    def test_printed_consumes_pending_only_human_action(self):
        run(label_sheet.mark_pending(1, "U1"))
        run(label_sheet.mark_pending(2, "U1"))
        st, consumed = run(label_sheet.consume_printed("U1"))
        self.assertEqual(consumed, [1, 2])
        self.assertEqual((st.used, st.pending, st.history), ((1, 2), (), ((1, 2),)))
        st, consumed = run(label_sheet.consume_printed("U1"))
        self.assertEqual(consumed, [])
        self.assertEqual(st.used, (1, 2))

    def test_printed_specific_face(self):
        run(label_sheet.mark_pending(3, "U1"))
        run(label_sheet.mark_pending(4, "U1"))
        st, consumed = run(label_sheet.consume_printed("U1", 4))
        self.assertEqual((consumed, st.used, st.pending), ([4], (4,), (3,)))
        st, consumed = run(label_sheet.consume_printed("U1", 9))
        self.assertEqual((consumed, st.used, st.pending), ([], (4,), (3,)))

    def test_undo_reverts_last_batch_only(self):
        run(label_sheet.mark_pending(1, "U1"))
        run(label_sheet.consume_printed("U1"))
        run(label_sheet.mark_pending(2, "U1"))
        run(label_sheet.consume_printed("U1"))
        st, undone = run(label_sheet.undo_last("U1"))
        self.assertEqual((undone, st.used, st.history), ([2], (1,), ((1,),)))
        self.assertEqual(st.next_free(10), 2)
        st, undone = run(label_sheet.undo_last("U1"))
        self.assertEqual((undone, st.used, st.history), ([1], (), ()))
        st, undone = run(label_sheet.undo_last("U1"))
        self.assertEqual(undone, [])

    def test_new_sheet_starts_empty_and_keeps_old_row(self):
        run(label_sheet.mark_pending(1, "U1"))
        run(label_sheet.consume_printed("U1"))
        st = run(label_sheet.new_sheet("U1"))
        self.assertEqual((st.sheet_id, st.used, st.pending), ("S-2", (), ()))
        self.assertEqual(run(label_sheet.get_state()).sheet_id, "S-2")

    def test_full_sheet_proposes_nothing(self):
        for f in range(1, 11):
            run(label_sheet.mark_pending(f, "U1"))
        run(label_sheet.consume_printed("U1"))
        st = run(label_sheet.get_state())
        self.assertEqual(len(st.used), 10)
        self.assertIsNone(st.next_free(10))
        self.assertEqual(st.remaining(10), 0)

    def test_job_idempotency_key(self):
        self.assertIsNone(run(label_sheet.find_job("k")))
        run(label_sheet.record_job("k", sheet_id="S-1", face=4, filename="f.pdf", file_key="fk", user_id="U1"))
        self.assertEqual(run(label_sheet.find_job("k")),
                         {"sheet_id": "S-1", "face": 4, "filename": "f.pdf", "file_key": "fk"})
        with self.assertRaises(Exception):   # 一意制約
            run(label_sheet.record_job("k", sheet_id="S-1", face=4, filename="g.pdf", file_key="fk2", user_id="U1"))

    def test_status_text_has_no_pii_fields(self):
        run(label_sheet.mark_pending(2, "U1"))
        st = run(label_sheet.get_state())
        self.assertEqual(label_sheet.status_text(st, 10),
                         "シート S-1（10 面）: 使用済み 0 面（なし）・印刷待ち 1 面（2）・残り 9 面")


# ══════════════════════════════════════════════════════════════
# 3. 指示の構文
# ══════════════════════════════════════════════════════════════

class TestSyntax(unittest.TestCase):
    def p(self, text):
        return lp.parse_label_instruction(text)

    def test_formats_from_ticket(self):
        self.assertEqual(self.p("ラベル No.17 依頼者"),
                         lp.LabelRequest(kind="print", case_no="17", role="依頼者"))
        self.assertEqual(self.p("ラベル No.17 債権者 2"),
                         lp.LabelRequest(kind="print", case_no="17", role="債権者", role_arg="2"))
        self.assertEqual(self.p("ラベル No.17 役所 川口市"),
                         lp.LabelRequest(kind="print", case_no="17", role="役所", role_arg="川口市"))
        self.assertEqual(self.p("ラベル 発送 No.120 案件 No.17"),
                         lp.LabelRequest(kind="print", case_no="17", role="発送", role_arg="120"))
        self.assertEqual(self.p("ラベル No.17 依頼者 御中"),
                         lp.LabelRequest(kind="print", case_no="17", role="依頼者", honorific="御中"))
        self.assertEqual(self.p("ラベル No.17 依頼者 面 7"),
                         lp.LabelRequest(kind="print", case_no="17", role="依頼者", face=7))
        self.assertEqual(self.p("ラベル 放棄 No.17 依頼者"),
                         lp.LabelRequest(kind="print", unit="放棄", case_no="17", role="依頼者"))
        self.assertEqual(self.p("ラベル 時効 No.5 依頼者 様"),
                         lp.LabelRequest(kind="print", unit="時効", case_no="5", role="依頼者", honorific="様"))
        self.assertEqual(self.p("ラベル 相続 No.3 依頼者"),
                         lp.LabelRequest(kind="print", unit="相続", case_no="3", role="依頼者"))

    def test_sheet_ops(self):
        self.assertEqual(self.p("ラベル 印刷済"), lp.LabelRequest(kind="printed"))
        self.assertEqual(self.p("ラベル 印刷済 面 4"), lp.LabelRequest(kind="printed", face=4))
        self.assertEqual(self.p("ラベル 戻す"), lp.LabelRequest(kind="undo"))
        self.assertEqual(self.p("ラベル 新しいシート"), lp.LabelRequest(kind="new_sheet"))
        self.assertEqual(self.p("ラベル 残量"), lp.LabelRequest(kind="status"))

    def test_fullwidth_and_glued_tokens(self):
        self.assertEqual(self.p("ラベル　放棄　Ｎｏ．１７　債権者２　面４"),
                         lp.LabelRequest(kind="print", unit="放棄", case_no="17", role="債権者", role_arg="2", face=4))
        self.assertEqual(self.p("ラベル 発送No.120 案件No.17 御中"),
                         lp.LabelRequest(kind="print", case_no="17", role="発送", role_arg="120", honorific="御中"))
        self.assertEqual(self.p("ラベル・放棄・№17・役所川口市"),
                         lp.LabelRequest(kind="print", unit="放棄", case_no="17", role="役所", role_arg="川口市"))

    def test_unsupported_roles(self):
        for t in ("ラベル No.17 相手方", "ラベル 放棄 No.17 裁判所"):
            with self.assertRaises(lp.LabelSyntaxError) as cm:
                self.p(t)
            self.assertEqual(str(cm.exception), lp.MSG_UNSUPPORTED_ROLE)

    def test_syntax_errors_carry_usage_only(self):
        for t in ("ラベル", "ラベル No.17", "ラベル 発送 No.120", "ラベル No.17 依頼者 xyz",
                  "ラベル 依頼者", "ラベル No.17 債権者", "ラベル No.17 役所", "ラベル 印刷済 x",
                  "ラベル No.17 依頼者 面"):
            with self.assertRaises(lp.LabelSyntaxError, msg=t):
                self.p(t)
        with self.assertRaises(lp.LabelSyntaxError):
            self.p("鈴木さんにラベル")

    def test_direct_match_predicate(self):
        self.assertTrue(lp.is_label_instruction("ラベル 残量"))
        self.assertTrue(lp.is_label_instruction("ラベル　放棄　No.17 依頼者"))
        self.assertTrue(lp.is_label_instruction("ラベル"))
        self.assertFalse(lp.is_label_instruction("ラベルを作って"))
        self.assertFalse(lp.is_label_instruction("鈴木さんに送付案内"))

    def test_role_key_and_filename(self):
        req = lp.LabelRequest(kind="print", case_no="17", role="債権者", role_arg="2")
        self.assertEqual(req.role_key, "債権者2")
        self.assertEqual(lp.idempotency_key("40", "17", req.role_key, "S-1", 4),
                         "label_print:40:17:債権者2:S-1:4")
        from datetime import datetime, timezone
        self.assertEqual(lp.build_filename("S-1", 4, "依頼者", datetime(2026, 9, 9, 23, 30, tzinfo=timezone.utc)),
                         "宛名ラベル_20260910_S-1_面04_依頼者.pdf")   # JST で日付


# ══════════════════════════════════════════════════════════════
# 4. 宛先解決（kintone モック）
# ══════════════════════════════════════════════════════════════

class _KintoneMixin:
    def arm_kintone(self, *, cases40=None, cases21=None, ships=None, munis=None):
        self.cases40 = cases40 if cases40 is not None else {"17": case40()}
        self.cases21 = cases21 if cases21 is not None else {"5": case21()}
        self.ships = ships if ships is not None else {"120": ship120()}
        self.munis = munis if munis is not None else [muni()]
        self.updates: list[tuple] = []
        self.uploads: list[tuple] = []

        async def get_record(app, rid):
            table = {"KINTONE_APP_ID": self.cases21, "APP_HOUKI": self.cases40,
                     "APP_SHIPPING": self.ships}.get(app.app_id_env, {})
            if rid not in table:
                raise kintone.KintoneError(404, "GAIA_RE01", "not found")
            return table[rid]

        async def search_records(app, query, fields=None):
            if app.app_id_env != "APP_CITY_MASTER":
                return []
            self.assertIn('有効 in ("yes")', query)
            name = re.search(r'市区町村名 = "([^"]+)"', query).group(1)
            return [m for m in self.munis if m["市区町村名"]["value"] == name]

        async def upload_file(app, filename, content, mime):
            self.uploads.append((app.app_id_env, filename, content, mime))
            return f"fk-new-{len(self.uploads)}"

        async def update_record(app, rid, fields, revision=None):
            self.updates.append((app.app_id_env, rid, fields, revision))

        for name, fn in (("get_record", get_record), ("search_records", search_records),
                         ("upload_file", upload_file), ("update_record", update_record)):
            p = patch.object(kintone, name, fn)
            p.start()
            self.addCleanup(p.stop)


class TestResolve(_KintoneMixin, unittest.TestCase):
    def setUp(self):
        self._envp = patch.dict(os.environ, _ENV)
        self._envp.start()
        self.addCleanup(self._envp.stop)
        self.arm_kintone()

    def r(self, **kw):
        return run(lp.resolve_target(lp.LabelRequest(kind="print", **kw)))

    def test_client_app40_without_zip(self):
        t = self.r(unit="放棄", case_no="17", role="依頼者")
        self.assertEqual((t.name, t.zip_code, t.address, t.honorific), (NAME40, "", ADDR40, "様"))
        self.assertEqual((t.case_app.app_id_env, t.case_no, t.unit_label), ("APP_HOUKI", "17", "相続放棄"))

    def test_client_app21_and_honorific_override(self):
        t = self.r(unit="時効", case_no="5", role="依頼者", honorific="御中")
        self.assertEqual((t.name, t.zip_code, t.address, t.honorific), (NAME21, "", ADDR21, "御中"))
        self.assertEqual(t.case_app.app_id_env, "KINTONE_APP_ID")

    def test_creditor_rows(self):
        t = self.r(unit="放棄", case_no="17", role="債権者", role_arg="1")
        self.assertEqual((t.name, t.zip_code, t.address, t.honorific), (CRED1, CRED1_ZIP, CRED1_ADDR, "御中"))
        t = self.r(unit="放棄", case_no="17", role="債権者", role_arg="2")
        self.assertEqual((t.name, t.zip_code, t.address), (CRED2, "", CRED2_ADDR))
        with self.assertRaises(lp.LabelResolveError) as cm:
            self.r(unit="放棄", case_no="17", role="債権者", role_arg="3")
        self.assertEqual(str(cm.exception), "No.17 の債権者一覧に 3 行目がありません（2 行）")

    def test_creditor_outside_houki_is_unsupported(self):
        with self.assertRaises(lp.LabelResolveError) as cm:
            self.r(unit="時効", case_no="5", role="債権者", role_arg="1")
        self.assertIn("住所欄がないため未対応", str(cm.exception))

    def test_municipality_via_app31(self):
        t = self.r(unit="放棄", case_no="17", role="役所", role_arg="川口市")
        self.assertEqual((t.name, t.zip_code, t.address, t.honorific),
                         ("川口市役所　市民課", MUNI_ZIP, MUNI_ADDR, "御中"))
        self.assertEqual(t.case_no, "17")
        with self.assertRaises(lp.LabelResolveError) as cm:
            self.r(unit="放棄", case_no="17", role="役所", role_arg="蕨市")
        self.assertEqual(str(cm.exception), "市区町村マスタ（App 31）に「蕨市」の有効なレコードがありません")

    def test_shipping_record_as_is(self):
        t = self.r(unit="放棄", case_no="17", role="発送", role_arg="120")
        self.assertEqual((t.name, t.zip_code, t.address, t.honorific), (SHIP_NAME, SHIP_ZIP, SHIP_ADDR, ""))
        self.assertEqual(run(lp.resolve_unit(lp.LabelRequest(kind="print", case_no="17", role="発送", role_arg="120"))), "放棄")
        self.ships["120"] = ship120(unit="")
        self.assertIsNone(run(lp.resolve_unit(lp.LabelRequest(kind="print", case_no="17", role="発送", role_arg="120"))))

    def test_missing_case_or_empty_address(self):
        with self.assertRaises(lp.LabelResolveError) as cm:
            self.r(unit="放棄", case_no="99", role="依頼者")
        self.assertEqual(str(cm.exception), "No.99（相続放棄）のレコードが見つかりません")
        self.cases40["17"]["住所"]["value"] = ""
        with self.assertRaises(lp.LabelResolveError) as cm:
            self.r(unit="放棄", case_no="17", role="依頼者")
        self.assertEqual(str(cm.exception), "No.17（相続放棄）の依頼者の氏名または住所が空です")

    def test_resolve_errors_carry_no_pii(self):
        for kw in ({"unit": "放棄", "case_no": "17", "role": "債権者", "role_arg": "9"},
                   {"unit": "時効", "case_no": "5", "role": "債権者", "role_arg": "1"}):
            with self.assertRaises(lp.LabelResolveError) as cm:
                self.r(**kw)
            for p in PII:
                self.assertNotIn(p, str(cm.exception))


# ══════════════════════════════════════════════════════════════
# 5. フロー（handler 経由・復唱・OK→添付・冪等・残量操作・RV-10）
# ══════════════════════════════════════════════════════════════

class TestFlow(_KintoneMixin, _DbMixin):
    def setUp(self):
        super().setUp()
        handler.reset_sessions()
        self.arm_kintone()
        self.parse = AsyncMock(side_effect=self._parse)
        p = patch.object(parser, "parse_instruction", new=self.parse)
        p.start()
        self.addCleanup(p.stop)

    async def _parse(self, text):
        last = text.split("\n")[-1].replace("（追加回答）", "")
        if last == "OK":
            return dict(PARSE_CONFIRM)
        if last == "キャンセル":
            return dict(PARSE_CANCEL)
        if text.startswith("ラベル"):
            raise AssertionError("ラベル指示は Claude 解析を経ない")
        return dict(PARSE_SOUFU)

    def msg(self, text, user="U1"):
        return run(handler.handle_message(user, text))

    def assert_no_pii(self, reply):
        for p in PII:
            self.assertNotIn(p, reply)

    def test_registry_entry(self):
        spec = registry.get_task("label_print")
        self.assertIs(spec.flow_fn, lp.flow)
        self.assertIs(spec.flow_reply_fn, lp.flow_reply)
        self.assertIs(spec.execute_fn, lp.execute)
        self.assertIs(spec.direct_match_fn, lp.is_label_instruction)
        self.assertIn("label_print", registry.catalog_for_prompt())

    def test_confirmation_text_and_no_parser_call(self):
        reply = self.msg("ラベル 放棄 No.17 依頼者")
        self.assertEqual(reply, "No.17（相続放棄）・依頼者・面 1/10 に印字。残り 9 面。OK?\n"
                                "※郵便番号なし（〒 なしで印字）\nOK / キャンセル（30分有効）")
        self.parse.assert_not_called()
        self.assert_no_pii(reply)

    def test_confirmation_with_zip_creditor_explicit_face_and_honorific(self):
        reply = self.msg("ラベル 放棄 No.17 債権者 1 面 7 様")
        self.assertEqual(reply, "No.17（相続放棄）・債権者 1・面 7/10 に印字。残り 9 面。OK?\n"
                                "※敬称: 様\nOK / キャンセル（30分有効）")

    def test_unit_question_then_answer(self):
        reply = self.msg("ラベル No.17 依頼者")
        self.assertEqual(reply, lp.QUESTION_UNIT)
        self.assertEqual(self.msg("たぶん"), lp.QUESTION_UNIT)
        reply = self.msg("放棄")
        self.assertTrue(reply.startswith("No.17（相続放棄）・依頼者・面 1/10 に印字。"))
        self.assert_no_pii(reply)

    def test_unit_question_cancel_falls_through(self):
        self.msg("ラベル No.17 依頼者")
        self.assertEqual(self.msg("キャンセル"), handler.MSG_CANCELLED)

    def test_shipping_unit_from_app30(self):
        reply = self.msg("ラベル 発送 No.120 案件 No.17")
        self.assertEqual(reply, "No.17（相続放棄）・発送 No.120・面 1/10 に印字。残り 9 面。OK?\n"
                                "OK / キャンセル（30分有効）")
        self.ships["120"] = ship120(unit="")
        handler.reset_sessions()
        self.assertEqual(self.msg("ラベル 発送 No.120 案件 No.17"), lp.QUESTION_UNIT)
        handler.reset_sessions()
        self.assertEqual(self.msg("ラベル 発送 No.999 案件 No.17"), "発送 No.999 のレコードが見つかりません")

    def test_unsupported_and_syntax_replies(self):
        self.assertEqual(self.msg("ラベル No.17 相手方"), lp.MSG_UNSUPPORTED_ROLE)
        self.assertEqual(self.msg("ラベル 放棄 No.17 裁判所"), lp.MSG_UNSUPPORTED_ROLE)
        self.assertIn("書式:", self.msg("ラベル No.17"))
        self.assertEqual(self.msg("ラベル 時効 No.5 債権者 1"),
                         "時効援用の債権者は住所欄がないため未対応です（債権者は 放棄 のみ）")
        self.assertEqual(self.msg("ラベル 放棄 No.99 依頼者"), "No.99（相続放棄）のレコードが見つかりません")

    def test_ok_attaches_pdf_keeps_existing_files_and_marks_pending(self):
        self.msg("ラベル 放棄 No.17 依頼者")
        reply = self.msg("OK")
        self.assertEqual(len(self.uploads), 1)
        app_env, filename, content, mime = self.uploads[0]
        self.assertEqual((app_env, mime), ("APP_HOUKI", "application/pdf"))
        self.assertRegex(filename, r"^宛名ラベル_\d{8}_S-1_面01_依頼者\.pdf$")
        self.assertTrue(content.startswith(b"%PDF"))
        # 添付は 宛名ラベル 欄のみ・既存添付を残す・revision 指定なし
        self.assertEqual(self.updates, [("APP_HOUKI", "17",
                                         {"宛名ラベル": [{"fileKey": "old-1"}, {"fileKey": "fk-new-1"}]}, None)])
        self.assertIn("添付しました（No.17・依頼者・面 1/10・", reply)
        self.assertIn(lp.MSG_PRINTED_HINT, reply)
        self.assertIn("印刷待ち 1 面（1）・残り 9 面", reply)
        self.assert_no_pii(reply)
        st = run(label_sheet.get_state())
        self.assertEqual((st.used, st.pending), ((), (1,)))
        job = run(label_sheet.find_job("label_print:40:17:依頼者:S-1:1"))
        self.assertEqual((job["face"], job["file_key"]), (1, "fk-new-1"))
        # 二重 OK: 全終端で pending を invalidate する型（P3-003-CMD 裁定8）→ 確認待ちなし・再添付しない
        self.assertEqual(self.msg("OK"), handler.MSG_NO_PENDING)
        self.assertEqual(len(self.uploads), 1)

    def test_pdf_content_is_the_target_on_the_face(self):
        self.msg("ラベル 放棄 No.17 債権者 1 面 4")
        with patch.object(al.canvas.Canvas, "drawString", autospec=True) as ds:
            self.msg("OK")
        calls = [(round(c.args[1] / mm, 1), round(c.args[2] / mm, 1), c.args[3]) for c in ds.call_args_list]
        self.assertEqual(calls, [(112.0, 222.0, f"〒{CRED1_ZIP}"), (112.0, 210.0, CRED1_ADDR),
                                 (112.0, 192.0, f"{CRED1}　御中")])

    def test_next_face_skips_pending_and_used(self):
        self.msg("ラベル 放棄 No.17 依頼者")
        self.msg("OK")
        reply = self.msg("ラベル 放棄 No.17 債権者 1")
        self.assertTrue(reply.startswith("No.17（相続放棄）・債権者 1・面 2/10 に印字。残り 8 面。OK?"))
        self.msg("OK")
        self.assertEqual(self.msg("ラベル 印刷済"),
                         "面 1・2 を使用済みにしました。シート S-1（10 面）: 使用済み 2 面（1・2）・印刷待ち 0 面（なし）・残り 8 面")
        reply = self.msg("ラベル 放棄 No.17 依頼者")
        self.assertTrue(reply.startswith("No.17（相続放棄）・依頼者・面 3/10 に印字。残り 7 面。OK?"))

    def test_idempotent_same_key_does_not_reattach(self):
        self.msg("ラベル 放棄 No.17 依頼者 面 4")
        self.msg("OK")
        self.assertEqual(len(self.uploads), 1)
        reply = self.msg("ラベル 放棄 No.17 依頼者 面 4")
        self.assertIn("面 4 は印刷待ち（再印字）", reply)
        reply = self.msg("OK")
        self.assertTrue(reply.startswith("添付済みです（No.17・依頼者・面 4・"))
        self.assertIn(lp.MSG_PRINTED_HINT, reply)
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(len(self.updates), 1)
        # 別の面・別の役割は別キー
        self.msg("ラベル 放棄 No.17 依頼者 面 5")
        self.msg("OK")
        self.assertEqual(len(self.uploads), 2)

    def test_reprint_on_used_face_is_allowed_with_note(self):
        self.msg("ラベル 放棄 No.17 依頼者 面 4")
        self.msg("OK")
        self.msg("ラベル 印刷済")
        reply = self.msg("ラベル 放棄 No.17 依頼者 面 4")
        self.assertIn("面 4 は使用済み（再印字）", reply)
        reply = self.msg("ラベル 放棄 No.17 依頼者 面 11")
        self.assertEqual(reply, handler.MSG_INTERRUPTED + "\n面番号は 1〜10 です")

    def test_sheet_ops_via_line(self):
        self.assertEqual(self.msg("ラベル 残量"),
                         "シート S-1（10 面）: 使用済み 0 面（なし）・印刷待ち 0 面（なし）・残り 10 面")
        self.assertEqual(self.msg("ラベル 印刷済"), lp.MSG_NO_PENDING_FACE)
        self.assertEqual(self.msg("ラベル 戻す"), lp.MSG_NO_UNDO)
        self.msg("ラベル 放棄 No.17 依頼者")
        self.msg("OK")
        self.assertEqual(self.msg("ラベル 印刷済 面 9"),
                         "面 9 は印刷待ちではありません。シート S-1（10 面）: 使用済み 0 面（なし）・印刷待ち 1 面（1）・残り 9 面")
        self.assertEqual(self.msg("ラベル 印刷済 面 1"),
                         "面 1 を使用済みにしました。シート S-1（10 面）: 使用済み 1 面（1）・印刷待ち 0 面（なし）・残り 9 面")
        self.assertEqual(self.msg("ラベル 戻す"),
                         "直前の消費（面 1）を取り消しました。シート S-1（10 面）: 使用済み 0 面（なし）・印刷待ち 0 面（なし）・残り 10 面")
        self.assertEqual(self.msg("ラベル 新しいシート"),
                         "新しいシート S-2 を開始しました。シート S-2（10 面）: 使用済み 0 面（なし）・印刷待ち 0 面（なし）・残り 10 面")
        # parser が呼ばれたのは「OK」の 1 回だけ（ラベル指示は Claude 解析を経ない）
        self.assertEqual([c.args[0] for c in self.parse.call_args_list], ["OK"])

    def test_full_sheet_has_no_proposal(self):
        for f in range(1, 11):
            run(label_sheet.mark_pending(f, "U1"))
        run(label_sheet.consume_printed("U1"))
        self.assertEqual(self.msg("ラベル 放棄 No.17 依頼者"), lp.MSG_SHEET_FULL)
        reply = self.msg("ラベル 放棄 No.17 依頼者 面 3")   # 明示指定は再印字として通す
        self.assertIn("面 3/10 に印字。残り 0 面。OK?", reply)

    def test_sheet_changed_between_confirm_and_ok(self):
        self.msg("ラベル 放棄 No.17 依頼者")
        run(label_sheet.new_sheet("U1"))
        reply = self.msg("OK")
        self.assertEqual(reply, "シートが S-1 から S-2 に変わりました。もう一度指示してください")
        self.assertEqual(self.uploads, [])
        self.assertEqual(self.msg("OK"), handler.MSG_NO_PENDING)   # pending は invalidate 済み

    def test_kintone_failure_on_ok_reports_without_pii(self):
        self.msg("ラベル 放棄 No.17 依頼者")

        async def boom(app, filename, content, mime):
            raise kintone.KintoneError(500, "GAIA_XX", "boom")
        with patch.object(kintone, "upload_file", boom), \
                patch("dispatch_bot.handler.notify.notify_admin_line", new=AsyncMock()) as nl:
            reply = self.msg("OK")
        self.assertEqual(reply, handler.MSG_FILE_FAILED)
        self.assert_no_pii(nl.call_args.args[0])
        self.assertIn("案件No.17", nl.call_args.args[0])
        self.assertEqual(self.updates, [])

    def test_other_instructions_still_go_through_parser(self):
        reply = self.msg("鈴木さんに送付案内を作って")
        self.parse.assert_called_once()
        self.assertNotIn("ラベル", reply.split("\n")[0])

    def test_all_replies_are_pii_free(self):
        for t in ("ラベル 放棄 No.17 依頼者", "OK", "ラベル 残量", "ラベル 印刷済",
                  "ラベル 放棄 No.17 債権者 2", "OK", "ラベル 放棄 No.17 役所 川口市", "OK",
                  "ラベル 発送 No.120 案件 No.17", "OK", "ラベル 戻す", "ラベル 新しいシート"):
            self.assert_no_pii(self.msg(t))


class TestFlowWithoutDb(_KintoneMixin, unittest.TestCase):
    def setUp(self):
        handler.reset_sessions()
        self._envp = patch.dict(os.environ, {**_ENV, "DATABASE_URL": ""})
        self._envp.start()
        self.addCleanup(self._envp.stop)
        db.reset_for_tests()
        self.addCleanup(db.reset_for_tests)
        self.arm_kintone()

    def test_db_unset_is_fail_closed_message(self):
        self.assertEqual(run(handler.handle_message("U1", "ラベル 残量")), lp.MSG_DB_UNSET)
        self.assertEqual(run(handler.handle_message("U1", "ラベル 放棄 No.17 依頼者")), lp.MSG_DB_UNSET)
        self.assertEqual(self.uploads, [])


if __name__ == "__main__":
    unittest.main()
