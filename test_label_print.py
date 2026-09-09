"""LABEL-PRINT-1（＋fix1 ジョブ状態機械）: 宛名ラベル印字（A-one 31514・使いかけシートの指定面）のテスト

検証（票 第 2 段 5・fix1 D）:
- レイアウト寸法 pin（共有線の格子 縦 [12,106,200]／横 [293,236,179,122,65,8]・既定オフセット 0/0・
  全 10 面の領域）・faces 指定（指定面以外は空白・文字位置は面の左下から x+6/y+31/y+13）・
  既存 A4_2x6 不変（仕様値・面原点・13 件 2 頁・PRINT_OFFSET 系）
- ジョブ状態機械（sqlite 実 DB）: 予約（event_id 冪等・面の占有・回収・添付済み）・印刷済／戻す／
  新しいシート（event_id 冪等・状態更新と同一トランザクション）・満杯時の提案なし
- 指示の構文（各書式・未対応宛先の返答・敬称上書き・全角・固定文言のみ＝LP-05）
- 宛先解決（依頼者・債権者 n・役所・発送の 4 種・郵便番号なし・未対応/不足の返答）
- 添付先の FILE 欄「宛名ラベル」（既存添付を残す・$revision CAS・409 再取得マージ＝LP-03）
- LP-01（提案面の競合・シート切替）・LP-02（添付失敗→回収）・LP-04（印刷済→戻す→再予約）・
  LP-05（返信・ログに入力原文なし）・LP-06（操作コマンドの再配送）
- RV-10（復唱・返信に氏名/住所が出ない）・直接一致（Claude 解析を経ない）・既存語彙の非干渉
kintone / parser は全てモック。DB は file sqlite（rv04b の流儀）。
"""

import asyncio
import inspect
import logging
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
NAME18, ADDR18 = "放棄次郎", "埼玉県川口市東青木8-8-8"
NAME21, ADDR21 = "時効花子", "東京都新宿区3-3-3"
CRED1, CRED1_ZIP, CRED1_ADDR = "甲社", "100-0001", "東京都千代田区1-1-1"
CRED2, CRED2_ADDR = "乙社", "大阪府大阪市2-2-2"
SHIP_NAME, SHIP_ZIP, SHIP_ADDR = "丙運送株式会社", "333-0001", "埼玉県川口市4-4-4"
MUNI_ZIP, MUNI_ADDR = "332-8601", "埼玉県川口市青木2-1-1"
PII = (NAME40, ADDR40, NAME18, ADDR18, NAME21, ADDR21, CRED1, CRED1_ADDR, CRED2, CRED2_ADDR,
       SHIP_NAME, SHIP_ADDR, MUNI_ADDR, "9-9-9", "8-8-8", "3-3-3")

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


def case40(rid="17", name=NAME40, addr=ADDR40, files=None, rows=None):
    return rec(**{"$id": rid, "顧客名": name, "住所": addr,
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
# 2. ジョブ状態機械（sqlite 実 DB）
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


def reserve(event, face, *, record="17", role="依頼者", app="40", sheet="S-1", explicit=False, user="U1"):
    return run(label_sheet.reserve_face(event, app=app, record=record, role=role, sheet_id=sheet,
                                        face=face, explicit=explicit, user_id=user))


def jobs_all():
    import sqlalchemy as sa

    async def _q():
        async with label_sheet.session_scope() as s:
            rows = (await s.execute(sa.select(label_sheet.label_print_job)
                                    .order_by(label_sheet.label_print_job.c.id))).fetchall()
            return [(r.id, r.event_id, r.face, r.status, r.file_key) for r in rows]
    return run(_q())


def sheets_all():
    import sqlalchemy as sa

    async def _q():
        async with label_sheet.session_scope() as s:
            return [r.sheet_id for r in (await s.execute(
                sa.select(label_sheet.label_sheet_state).order_by(label_sheet.label_sheet_state.c.id))).fetchall()]
    return run(_q())


class TestSheetState(_DbMixin):
    def test_initial_sheet_and_next_free(self):
        self.assertIsNone(run(label_sheet.get_state()))
        st = run(label_sheet.ensure_state("U1"))
        self.assertEqual((st.sheet_id, st.used, st.pending, st.history, st.active), ("S-1", (), (), (), ()))
        self.assertEqual(st.next_free(10), 1)
        self.assertEqual(st.remaining(10), 10)
        self.assertEqual(run(label_sheet.ensure_state("U1")).sheet_id, "S-1")  # 二重作成しない

    def test_reserve_is_the_job_row_and_occupies_face(self):
        """LP-01/02: 予約＝ジョブ行（別の予約表を持たない）。reserved が面を占有し提案候補から外れる"""
        run(label_sheet.ensure_state("U1"))
        self.assertNotIn("pending_faces", label_sheet.label_sheet_state.c)
        res = reserve("ev-1", 1)
        self.assertEqual(res.outcome, "reserved")
        self.assertEqual((res.job.face, res.job.status, res.job.event_id), (1, "reserved", "ev-1"))
        st = run(label_sheet.get_state())
        self.assertEqual((st.used, st.pending), ((), (1,)))
        self.assertEqual(st.next_free(10), 2)
        self.assertEqual(st.remaining(10), 9)

    def test_reserve_duplicate_event_is_noop(self):
        run(label_sheet.ensure_state("U1"))
        reserve("ev-1", 1)
        res = reserve("ev-1", 2)
        self.assertEqual((res.outcome, res.job.face), ("duplicate_event", 1))
        self.assertEqual(len(jobs_all()), 1)

    def test_reserve_face_taken_by_other_target(self):
        """LP-01: 他宛先の reserved/attached がある面は予約しない"""
        run(label_sheet.ensure_state("U1"))
        reserve("ev-1", 1)
        res = reserve("ev-2", 1, record="18")
        self.assertEqual(res.outcome, "face_taken")
        self.assertEqual(len(jobs_all()), 1)
        run(label_sheet.mark_job_attached(1, "fk", "f.pdf"))
        self.assertEqual(reserve("ev-3", 1, record="18").outcome, "face_taken")

    def test_reserve_same_target_recover_and_attached(self):
        """LP-02/04: 同一宛先・同一面の reserved は回収（再予約せず event_id を更新）、attached は添付済み"""
        run(label_sheet.ensure_state("U1"))
        reserve("ev-1", 1)
        res = reserve("ev-2", 1)
        self.assertEqual((res.outcome, res.job.job_id, res.job.event_id), ("recover", 1, "ev-2"))
        self.assertEqual(len(jobs_all()), 1)
        run(label_sheet.mark_job_attached(1, "fk", "f.pdf"))
        res = reserve("ev-3", 1)
        self.assertEqual((res.outcome, res.job.status, res.job.file_key), ("attached", "attached", "fk"))
        self.assertEqual(len(jobs_all()), 1)

    def test_reserve_sheet_changed_and_used_face(self):
        run(label_sheet.ensure_state("U1"))
        self.assertEqual(reserve("ev-1", 1, sheet="S-9").outcome, "sheet_changed")
        self.assertEqual(jobs_all(), [])
        reserve("ev-2", 1)
        run(label_sheet.mark_job_attached(1, "fk", "f.pdf"))
        run(label_sheet.consume_printed("op-1", "U1"))
        self.assertEqual(reserve("ev-3", 1).outcome, "face_taken")             # 提案面が used → 予約しない
        self.assertEqual(reserve("ev-4", 1, explicit=True).outcome, "reserved")  # 明示のみ再印字
        self.assertEqual(reserve("ev-5", 1, record="18", explicit=True).outcome, "face_taken")

    def test_reserve_uses_one_session_and_rolls_back_on_error(self):
        """LP-02 構造 pin: 予約（ジョブ挿入）は 1 つの session_scope 内で完結し、途中失敗で行が残らない"""
        run(label_sheet.ensure_state("U1"))
        src = inspect.getsource(label_sheet.reserve_face)
        self.assertEqual(src.count("session_scope()"), 1)
        opened = []
        real_scope = label_sheet.session_scope

        def counting_scope():
            opened.append(1)
            return real_scope()
        with patch.object(label_sheet, "session_scope", counting_scope):
            self.assertEqual(reserve("ev-1", 1).outcome, "reserved")
        self.assertEqual(len(opened), 1)

        async def boom(*a, **k):
            raise RuntimeError("simulated failure inside the transaction")
        # recover 経路（event_id 更新→同一 txn）で例外 → 何も変わらない（rollback）
        with patch.object(label_sheet, "_update_job", boom):
            with self.assertRaises(RuntimeError):
                reserve("ev-2", 1)
        self.assertEqual([j[1] for j in jobs_all()], ["ev-1"])

    def test_printed_consumes_attached_only_and_is_idempotent(self):
        """LP-06: 印刷済は attached の面だけ消費・ジョブは printed・同 event_id 再配送は無変化"""
        run(label_sheet.ensure_state("U1"))
        reserve("ev-1", 1)
        reserve("ev-2", 2, record="18")
        run(label_sheet.mark_job_attached(1, "fk1", "a.pdf"))
        outcome, st, consumed = run(label_sheet.consume_printed("op-1", "U1"))
        self.assertEqual((outcome, consumed, st.used, st.pending, st.history), ("applied", [1], (1,), (2,), ((1,),)))
        self.assertEqual([j[3] for j in jobs_all()], ["printed", "reserved"])
        outcome, st, consumed = run(label_sheet.consume_printed("op-1", "U1"))
        self.assertEqual((outcome, consumed, st.used, st.history), ("duplicate_event", [1], (1,), ((1,),)))
        outcome, st, consumed = run(label_sheet.consume_printed("op-2", "U1"))
        self.assertEqual((outcome, consumed), ("nothing", []))

    def test_printed_specific_face(self):
        run(label_sheet.ensure_state("U1"))
        reserve("ev-1", 3)
        reserve("ev-2", 4, record="18")
        run(label_sheet.mark_job_attached(1, "fk", "a.pdf"))
        run(label_sheet.mark_job_attached(2, "fk", "b.pdf"))
        outcome, st, consumed = run(label_sheet.consume_printed("op-1", "U1", 4))
        self.assertEqual((consumed, st.used, st.pending), ([4], (4,), (3,)))
        outcome, st, consumed = run(label_sheet.consume_printed("op-2", "U1", 9))
        self.assertEqual((outcome, consumed, st.used, st.pending), ("nothing", [], (4,), (3,)))

    def test_undo_reverts_last_batch_only_and_is_idempotent(self):
        """LP-06: 消費履歴 2 バッチで「戻す」の同 event_id 再配送 → 1 バッチだけ戻る"""
        run(label_sheet.ensure_state("U1"))
        reserve("ev-1", 1)
        run(label_sheet.mark_job_attached(1, "fk", "a.pdf"))
        run(label_sheet.consume_printed("op-1", "U1"))
        reserve("ev-2", 2, record="18")
        run(label_sheet.mark_job_attached(2, "fk", "b.pdf"))
        run(label_sheet.consume_printed("op-2", "U1"))
        outcome, st, undone = run(label_sheet.undo_last("op-u", "U1"))
        self.assertEqual((outcome, undone, st.used, st.history), ("applied", [2], (1,), ((1,),)))
        self.assertEqual([j[3] for j in jobs_all()], ["printed", "undone"])
        self.assertEqual(st.next_free(10), 2)
        outcome, st, undone = run(label_sheet.undo_last("op-u", "U1"))
        self.assertEqual((outcome, undone, st.used, st.history), ("duplicate_event", [2], (1,), ((1,),)))
        outcome, st, undone = run(label_sheet.undo_last("op-u2", "U1"))
        self.assertEqual((outcome, undone, st.used, st.history), ("applied", [1], (), ()))
        self.assertEqual(run(label_sheet.undo_last("op-u3", "U1"))[0], "nothing")

    def test_new_sheet_starts_empty_keeps_old_row_and_is_idempotent(self):
        """LP-06: 「新しいシート」再配送でシートが増えない"""
        run(label_sheet.ensure_state("U1"))
        reserve("ev-1", 1)
        run(label_sheet.mark_job_attached(1, "fk", "a.pdf"))
        run(label_sheet.consume_printed("op-1", "U1"))
        outcome, st = run(label_sheet.new_sheet("op-n", "U1"))
        self.assertEqual((outcome, st.sheet_id, st.used, st.pending), ("applied", "S-2", (), ()))
        outcome, st = run(label_sheet.new_sheet("op-n", "U1"))
        self.assertEqual((outcome, st.sheet_id), ("duplicate_event", "S-2"))
        self.assertEqual(sheets_all(), ["S-1", "S-2"])
        self.assertEqual(run(label_sheet.get_state()).sheet_id, "S-2")

    def test_full_sheet_proposes_nothing(self):
        run(label_sheet.ensure_state("U1"))
        for f in range(1, 11):
            reserve(f"ev-{f}", f, record=str(f))
            run(label_sheet.mark_job_attached(f, "fk", "a.pdf"))
        run(label_sheet.consume_printed("op-1", "U1"))
        st = run(label_sheet.get_state())
        self.assertEqual(len(st.used), 10)
        self.assertIsNone(st.next_free(10))
        self.assertEqual(st.remaining(10), 0)

    def test_file_key_roundtrip(self):
        run(label_sheet.ensure_state("U1"))
        reserve("ev-1", 1)
        run(label_sheet.set_job_file_key(1, "fk-up", "f.pdf"))
        job = run(label_sheet.get_job(1))
        self.assertEqual((job.file_key, job.status), ("fk-up", "reserved"))
        run(label_sheet.clear_job_file_key(1))
        self.assertEqual(run(label_sheet.get_job(1)).file_key, "")
        self.assertIsNone(run(label_sheet.get_job(99)))

    def test_status_text_has_no_pii_fields(self):
        run(label_sheet.ensure_state("U1"))
        reserve("ev-1", 2)
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

    def test_syntax_errors_are_fixed_messages_without_input_echo(self):
        """LP-05: 構文エラーは固定文言＋書式案内のみ。入力原文・未解釈トークンを含めない"""
        cases = {
            "ラベル": lp.USAGE,
            "ラベル No.17": lp.MSG_SYNTAX_NO_ROLE,
            "ラベル 発送 No.120": lp.MSG_SYNTAX_NO_CASE_FOR_SHIPPING,
            "ラベル No.17 依頼者 山田太郎": lp.MSG_SYNTAX_UNKNOWN_WORD,
            "ラベル No.17 依頼者 埼玉県川口市1-1": lp.MSG_SYNTAX_UNKNOWN_WORD,
            "ラベル 依頼者": lp.MSG_SYNTAX_NO_CASE,
            "ラベル No.17 債権者": lp.MSG_SYNTAX_CREDITOR_NO,
            "ラベル No.17 役所": lp.MSG_SYNTAX_MUNI_NAME,
            "ラベル 印刷済 x": lp.MSG_SYNTAX_PRINTED_ARG,
            "ラベル No.17 依頼者 面": lp.MSG_SYNTAX_FACE_NO,
            "ラベル 発送": lp.MSG_SYNTAX_SHIPPING_NO,
            "ラベル 案件": lp.MSG_SYNTAX_CASE_NO,
            "鈴木さんにラベル": lp.MSG_SYNTAX_START,
        }
        for text, expected in cases.items():
            with self.assertRaises(lp.LabelSyntaxError, msg=text) as cm:
                self.p(text)
            self.assertEqual(str(cm.exception), expected, text)
            for tok in ("山田太郎", "埼玉県", "1-1", "鈴木"):
                self.assertNotIn(tok, str(cm.exception), text)

    def test_direct_match_predicate(self):
        self.assertTrue(lp.is_label_instruction("ラベル 残量"))
        self.assertTrue(lp.is_label_instruction("ラベル　放棄　No.17 依頼者"))
        self.assertTrue(lp.is_label_instruction("ラベル"))
        self.assertFalse(lp.is_label_instruction("ラベルを作って"))
        self.assertFalse(lp.is_label_instruction("鈴木さんに送付案内"))

    def test_filename(self):
        from datetime import datetime, timezone
        self.assertEqual(lp.build_filename("S-1", 4, "依頼者", datetime(2026, 9, 9, 23, 30, tzinfo=timezone.utc)),
                         "宛名ラベル_20260910_S-1_面04_依頼者.pdf")   # JST で日付


# ══════════════════════════════════════════════════════════════
# 4. 宛先解決（kintone モック・$revision CAS つき）
# ══════════════════════════════════════════════════════════════

class _KintoneMixin:
    def arm_kintone(self, *, cases40=None, cases21=None, ships=None, munis=None):
        self.cases40 = cases40 if cases40 is not None else {"17": case40()}
        self.cases21 = cases21 if cases21 is not None else {"5": case21()}
        self.ships = ships if ships is not None else {"120": ship120()}
        self.munis = munis if munis is not None else [muni()]
        self.revisions: dict[tuple, int] = {}
        self.updates: list[tuple] = []
        self.uploads: list[tuple] = []
        self.conflict_inject: list = []     # LP-03: 次の update で 409 にし、外部の添付を割り込ませる

        def table(app):
            return {"KINTONE_APP_ID": self.cases21, "APP_HOUKI": self.cases40,
                    "APP_SHIPPING": self.ships}.get(app.app_id_env, {})

        async def get_record(app, rid):
            if rid not in table(app):
                raise kintone.KintoneError(404, "GAIA_RE01", "not found")
            r = table(app)[rid]
            r["$revision"] = {"value": str(self.revisions.get((app.app_id_env, rid), 1))}
            return r

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
            key = (app.app_id_env, rid)
            cur = self.revisions.get(key, 1)
            r = table(app)[rid]
            if self.conflict_inject:
                external = self.conflict_inject.pop(0)
                r["宛名ラベル"]["value"] = list(r["宛名ラベル"]["value"]) + [{"fileKey": external}]
                self.revisions[key] = cur + 1
                raise kintone.KintoneConflict(409, "GAIA_CO02", "revision conflict")
            if revision is not None and str(revision) != str(cur):
                raise kintone.KintoneConflict(409, "GAIA_CO02", "revision conflict")
            self.updates.append((app.app_id_env, rid, fields, revision))
            if "宛名ラベル" in fields:
                r["宛名ラベル"]["value"] = [{"fileKey": f["fileKey"]} for f in fields["宛名ラベル"]]
            self.revisions[key] = cur + 1

        for name, fn in (("get_record", get_record), ("search_records", search_records),
                         ("upload_file", upload_file), ("update_record", update_record)):
            p = patch.object(kintone, name, fn)
            p.start()
            self.addCleanup(p.stop)

    def attached_keys(self, rid="17"):
        return [f["fileKey"] for f in self.cases40[rid]["宛名ラベル"]["value"]]


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
        self.assertEqual((t.role_label, t.role_key), ("依頼者", "依頼者"))

    def test_client_app21_and_honorific_override(self):
        t = self.r(unit="時効", case_no="5", role="依頼者", honorific="御中")
        self.assertEqual((t.name, t.zip_code, t.address, t.honorific), (NAME21, "", ADDR21, "御中"))
        self.assertEqual(t.case_app.app_id_env, "KINTONE_APP_ID")

    def test_creditor_rows(self):
        t = self.r(unit="放棄", case_no="17", role="債権者", role_arg="1")
        self.assertEqual((t.name, t.zip_code, t.address, t.honorific), (CRED1, CRED1_ZIP, CRED1_ADDR, "御中"))
        self.assertEqual((t.role_label, t.role_key), ("債権者 1", "債権者1"))
        t = self.r(unit="放棄", case_no="17", role="債権者", role_arg="2")
        self.assertEqual((t.name, t.zip_code, t.address), (CRED2, "", CRED2_ADDR))
        with self.assertRaises(lp.LabelResolveError) as cm:
            self.r(unit="放棄", case_no="17", role="債権者", role_arg="3")
        self.assertEqual(str(cm.exception), "No.17 の債権者一覧に 3 行目がありません（2 行）")

    def test_creditor_outside_houki_is_unsupported(self):
        with self.assertRaises(lp.LabelResolveError) as cm:
            self.r(unit="時効", case_no="5", role="債権者", role_arg="1")
        self.assertIn("住所欄がないため未対応", str(cm.exception))

    def test_municipality_via_app31_uses_canonical_name(self):
        t = self.r(unit="放棄", case_no="17", role="役所", role_arg="川口市")
        self.assertEqual((t.name, t.zip_code, t.address, t.honorific),
                         ("川口市役所　市民課", MUNI_ZIP, MUNI_ADDR, "御中"))
        self.assertEqual((t.case_no, t.role_label, t.role_key), ("17", "役所 川口市", "役所川口市"))
        with self.assertRaises(lp.LabelResolveError) as cm:
            self.r(unit="放棄", case_no="17", role="役所", role_arg="山田太郎")
        self.assertEqual(str(cm.exception), lp.MSG_MUNI_NOT_FOUND)   # 入力原文を返さない（LP-05）

    def test_shipping_record_as_is(self):
        t = self.r(unit="放棄", case_no="17", role="発送", role_arg="120")
        self.assertEqual((t.name, t.zip_code, t.address, t.honorific), (SHIP_NAME, SHIP_ZIP, SHIP_ADDR, ""))
        self.assertEqual((t.role_label, t.role_key), ("発送 No.120", "発送No120"))
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
                   {"unit": "時効", "case_no": "5", "role": "債権者", "role_arg": "1"},
                   {"unit": "放棄", "case_no": "17", "role": "役所", "role_arg": NAME40}):
            with self.assertRaises(lp.LabelResolveError) as cm:
                self.r(**kw)
            for p in PII:
                self.assertNotIn(p, str(cm.exception))

    def test_attach_with_cas_merges_on_conflict_and_gives_up_after_3(self):
        """LP-03: 409 → 再取得して既存添付（割り込み分を含む）に自分の fileKey をマージして再試行"""
        self.conflict_inject = ["ext-1"]
        run(lp.attach_with_cas(lp.APP_HOUKI_CASE, "17", "fk-mine"))
        self.assertEqual(self.attached_keys(), ["old-1", "ext-1", "fk-mine"])
        self.assertEqual(len(self.updates), 1)
        self.assertEqual(self.updates[0][3], "2")            # 再取得後の最新 $revision で PUT
        self.conflict_inject = ["ext-2", "ext-3", "ext-4"]
        with self.assertRaises(lp.AttachConflict):
            run(lp.attach_with_cas(lp.APP_HOUKI_CASE, "17", "fk-late"))
        self.assertNotIn("fk-late", self.attached_keys())


# ══════════════════════════════════════════════════════════════
# 5. フロー（handler 経由・復唱・OK→予約→添付・回収・操作コマンド・RV-10）
# ══════════════════════════════════════════════════════════════

class TestFlow(_KintoneMixin, _DbMixin):
    def setUp(self):
        super().setUp()
        handler.reset_sessions()
        self.arm_kintone(cases40={"17": case40(), "18": case40("18", NAME18, ADDR18, files=[])})
        self.parse = AsyncMock(side_effect=self._parse)
        p = patch.object(parser, "parse_instruction", new=self.parse)
        p.start()
        self.addCleanup(p.stop)
        self.notify = AsyncMock()
        p = patch("dispatch_bot.handler.notify.notify_admin_line", new=self.notify)
        p.start()
        self.addCleanup(p.stop)
        self._ev = 0

    async def _parse(self, text):
        last = text.split("\n")[-1].replace("（追加回答）", "")
        if last == "OK":
            return dict(PARSE_CONFIRM)
        if last == "キャンセル":
            return dict(PARSE_CANCEL)
        if text.startswith("ラベル"):
            raise AssertionError("ラベル指示は Claude 解析を経ない")
        return dict(PARSE_SOUFU)

    def msg(self, text, user="U1", event=None):
        if event is None:
            self._ev += 1
            event = f"ev-{self._ev}"
        return run(handler.handle_message(user, text, event_id=event))

    def assert_no_pii(self, reply):
        for p in PII:
            self.assertNotIn(p, reply)

    def fill_used(self, faces):
        run(label_sheet.ensure_state("U1"))
        for f in faces:
            reserve(f"fill-{f}", f, record=f"fill{f}", explicit=True)
            run(label_sheet.mark_job_attached(len(jobs_all()), "fk-fill", "fill.pdf"))
        run(label_sheet.consume_printed("fill-op", "U1"))

    # ── 登録・復唱 ─────────────────────────────────────────────
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

    def test_municipality_confirmation_uses_canonical_name(self):
        reply = self.msg("ラベル 放棄 No.17 役所 川口市")
        self.assertTrue(reply.startswith("No.17（相続放棄）・役所 川口市・面 1/10 に印字。残り 9 面。OK?"))
        self.assertEqual(self.msg("ラベル 放棄 No.17 役所 蕨市"),
                         handler.MSG_INTERRUPTED + "\n" + lp.MSG_MUNI_NOT_FOUND)

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
        self.assertEqual(self.msg("ラベル No.17"), lp.MSG_SYNTAX_NO_ROLE)
        self.assertEqual(self.msg("ラベル 時効 No.5 債権者 1"),
                         "時効援用の債権者は住所欄がないため未対応です（債権者は 放棄 のみ）")
        self.assertEqual(self.msg("ラベル 放棄 No.99 依頼者"), "No.99（相続放棄）のレコードが見つかりません")

    # ── OK → 予約 → 添付 ───────────────────────────────────────
    def test_ok_reserves_attaches_keeps_existing_files_and_marks_attached(self):
        self.msg("ラベル 放棄 No.17 依頼者")
        reply = self.msg("OK", event="ok-1")
        self.assertEqual(len(self.uploads), 1)
        app_env, filename, content, mime = self.uploads[0]
        self.assertEqual((app_env, mime), ("APP_HOUKI", "application/pdf"))
        self.assertRegex(filename, r"^宛名ラベル_\d{8}_S-1_面01_依頼者\.pdf$")
        self.assertTrue(content.startswith(b"%PDF"))
        # 添付は 宛名ラベル 欄のみ・既存添付を残す・最新 $revision の CAS
        self.assertEqual(self.updates, [("APP_HOUKI", "17",
                                         {"宛名ラベル": [{"fileKey": "old-1"}, {"fileKey": "fk-new-1"}]}, "1")])
        self.assertIn("添付しました（No.17・依頼者・面 1/10・", reply)
        self.assertIn(lp.MSG_PRINTED_HINT, reply)
        self.assertIn("印刷待ち 1 面（1）・残り 9 面", reply)
        self.assert_no_pii(reply)
        self.assertEqual(jobs_all(), [(1, "ok-1", 1, "attached", "fk-new-1")])
        st = run(label_sheet.get_state())
        self.assertEqual((st.used, st.pending), ((), (1,)))
        # 二重 OK: 全終端で pending を invalidate する型（P3-003-CMD 裁定8）→ 確認待ちなし・再添付しない
        self.assertEqual(self.msg("OK"), handler.MSG_NO_PENDING)
        self.assertEqual(len(self.uploads), 1)

    def test_ok_event_redelivery_is_noop(self):
        """同じ OK イベントの再配送（pending が残っている想定）→ 予約も添付もしない"""
        self.msg("ラベル 放棄 No.17 依頼者")
        self.msg("OK", event="ok-1")
        from dispatch_bot import confirm
        self.msg("ラベル 放棄 No.17 依頼者 面 2")          # 別の pending を張る
        state, pending = confirm.peek("U1")
        self.assertEqual(state, "active")
        reply = self.msg("OK", event="ok-1")                  # 同じ event_id の再配送
        self.assertEqual(reply, lp.MSG_DUPLICATE_EVENT)
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(len(jobs_all()), 1)

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

    # ── LP-01 ────────────────────────────────────────────────
    def test_lp01_two_confirmations_same_proposed_face_second_ok_is_refused(self):
        """案件 17・18 の復唱を続けて作り両方に面 1 が提案 → 順に OK → 2 件目は予約されず再指示・添付 0"""
        r1 = self.msg("ラベル 放棄 No.17 依頼者", user="U1")
        r2 = self.msg("ラベル 放棄 No.18 依頼者", user="U2")
        self.assertIn("面 1/10 に印字", r1)
        self.assertIn("面 1/10 に印字", r2)
        self.assertIn("添付しました（No.17・依頼者・面 1/10", self.msg("OK", user="U1"))
        reply = self.msg("OK", user="U2")
        self.assertEqual(reply, lp.MSG_FACE_TAKEN)
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual([j[3] for j in jobs_all()], ["attached"])
        self.assertEqual(self.attached_keys("18"), [])
        # 再指示 → 面 2 が提案される
        self.assertIn("面 2/10 に印字", self.msg("ラベル 放棄 No.18 依頼者", user="U2"))

    def test_lp01_sheet_switched_after_confirmation(self):
        """復唱後にシート切替 → OK は再指示・予約 0"""
        self.msg("ラベル 放棄 No.17 依頼者")
        run(label_sheet.new_sheet("op-n", "U9"))
        reply = self.msg("OK")
        self.assertEqual(reply, "シートが S-1 から変わりました。もう一度指示してください")
        self.assertEqual((self.uploads, jobs_all()), ([], []))
        self.assertEqual(self.msg("OK"), handler.MSG_NO_PENDING)   # pending は invalidate 済み

    # ── LP-02 ────────────────────────────────────────────────
    def test_lp02_attach_failure_leaves_reserved_then_recovers_without_new_reservation(self):
        """添付失敗 → reserved 残留 → 別イベントの同一指示で回収して添付・attached。回収中は再予約しない"""
        self.msg("ラベル 放棄 No.17 依頼者")

        async def boom(app, rid, fields, revision=None):
            raise kintone.KintoneError(500, "GAIA_XX", "boom")
        with patch.object(kintone, "update_record", boom):
            reply = self.msg("OK", event="ok-1")
        self.assertEqual(reply, handler.MSG_FILE_FAILED)
        self.assert_no_pii(self.notify.call_args.args[0])
        self.assertEqual(jobs_all(), [(1, "ok-1", 1, "reserved", "fk-new-1")])   # fileKey は保存済み
        # 同一指示（別 event_id）: 同じ宛先の占有面が提案され、OK で回収
        reply = self.msg("ラベル 放棄 No.17 依頼者")
        self.assertEqual(reply, "No.17（相続放棄）・依頼者・面 1/10 に印字。残り 9 面。OK?\n"
                                "※郵便番号なし（〒 なしで印字）／面 1 は同じ宛先で添付未完了（OK で回収します）\n"
                                "OK / キャンセル（30分有効）")
        reply = self.msg("OK", event="ok-2")
        self.assertIn("回収して添付しました（No.17・依頼者・面 1/10・", reply)
        self.assertEqual(len(self.uploads), 1)                                    # fileKey 再利用
        self.assertEqual(self.attached_keys(), ["old-1", "fk-new-1"])
        self.assertEqual(jobs_all(), [(1, "ok-2", 1, "attached", "fk-new-1")])   # 再予約なし・event_id 更新
        # attached 後の同一指示 → 添付済み（再添付しない）
        reply = self.msg("ラベル 放棄 No.17 依頼者")
        self.assertIn("面 1 は同じ宛先で添付済み（再添付はしません）", reply)
        self.assertTrue(self.msg("OK").startswith("添付済みです（No.17・依頼者・面 1・"))
        self.assertEqual(len(self.updates), 1)

    def test_lp02_recovery_with_expired_file_key_reuploads_next_time(self):
        self.msg("ラベル 放棄 No.17 依頼者")

        async def boom(app, rid, fields, revision=None):
            raise kintone.KintoneError(500, "GAIA_XX", "boom")
        with patch.object(kintone, "update_record", boom):
            self.msg("OK", event="ok-1")
            self.msg("ラベル 放棄 No.17 依頼者")
            self.msg("OK", event="ok-2")                       # 回収でも失敗（保存済み fileKey は破棄）
        self.assertEqual(jobs_all(), [(1, "ok-2", 1, "reserved", "")])
        self.msg("ラベル 放棄 No.17 依頼者")
        self.assertIn("回収して添付しました", self.msg("OK", event="ok-3"))
        self.assertEqual(len(self.uploads), 2)
        self.assertEqual(jobs_all(), [(1, "ok-3", 1, "attached", "fk-new-2")])

    # ── LP-03 ────────────────────────────────────────────────
    def test_lp03_concurrent_attach_merges_both_pdfs(self):
        """同一案件に異なる面を並行添付（既存 FILE 一覧を同時取得）→ 409 再取得マージで両方残る"""
        self.msg("ラベル 放棄 No.17 依頼者", user="U1")
        self.msg("ラベル 放棄 No.17 債権者 1 面 2", user="U2")
        self.conflict_inject = ["fk-u2-parallel"]          # U1 の PUT 前に U2 の添付が割り込む
        reply = self.msg("OK", user="U1")
        self.assertIn("添付しました", reply)
        self.assertEqual(self.attached_keys(), ["old-1", "fk-u2-parallel", "fk-new-1"])
        self.assertEqual(len(self.uploads), 1)              # 409 の再試行でアップロードし直さない
        self.assertEqual(self.updates[-1][3], "2")          # 再取得後の $revision
        self.assertIn("添付しました", self.msg("OK", user="U2"))
        self.assertEqual(self.attached_keys(), ["old-1", "fk-u2-parallel", "fk-new-1", "fk-new-2"])

    def test_lp03_persistent_conflict_leaves_reserved_for_recovery(self):
        self.msg("ラベル 放棄 No.17 依頼者")
        self.conflict_inject = ["e1", "e2", "e3"]
        self.assertEqual(self.msg("OK", event="ok-1"), lp.MSG_ATTACH_CONFLICT)
        self.assertEqual(jobs_all(), [(1, "ok-1", 1, "reserved", "fk-new-1")])
        self.msg("ラベル 放棄 No.17 依頼者")
        self.assertIn("回収して添付しました", self.msg("OK", event="ok-2"))
        self.assertEqual(len(self.uploads), 1)

    # ── LP-04 ────────────────────────────────────────────────
    def test_lp04_printed_undo_then_same_instruction_creates_new_job(self):
        """面 1 添付 → 印刷済 → 戻す → 同一指示（別 event_id）→ 新規ジョブで面 1 を予約・添付 → 印刷済で消費"""
        self.msg("ラベル 放棄 No.17 依頼者")
        self.msg("OK", event="ok-1")
        self.assertIn("面 1 を使用済みにしました", self.msg("ラベル 印刷済", event="op-1"))
        self.assertIn("直前の消費（面 1）を取り消しました", self.msg("ラベル 戻す", event="op-2"))
        self.assertEqual([j[3] for j in jobs_all()], ["undone"])
        reply = self.msg("ラベル 放棄 No.17 依頼者")
        self.assertEqual(reply, "No.17（相続放棄）・依頼者・面 1/10 に印字。残り 9 面。OK?\n"
                                "※郵便番号なし（〒 なしで印字）\nOK / キャンセル（30分有効）")
        self.assertIn("添付しました（No.17・依頼者・面 1/10", self.msg("OK", event="ok-2"))
        self.assertEqual([(j[1], j[3]) for j in jobs_all()], [("ok-1", "undone"), ("ok-2", "attached")])
        self.assertEqual(len(self.uploads), 2)
        self.assertIn("面 1 を使用済みにしました", self.msg("ラベル 印刷済", event="op-3"))
        self.assertEqual([j[3] for j in jobs_all()], ["undone", "printed"])

    def test_explicit_used_face_reprint_and_other_target_pending_face(self):
        self.msg("ラベル 放棄 No.17 依頼者 面 4")
        self.msg("OK")
        self.msg("ラベル 印刷済")
        reply = self.msg("ラベル 放棄 No.17 依頼者 面 4")
        self.assertIn("面 4 は使用済み（再印字）", reply)
        self.assertIn("添付しました（No.17・依頼者・面 4/10", self.msg("OK"))
        self.assertEqual(len(self.uploads), 2)
        # 他宛先が占有中の面を明示 → 復唱せず案内
        reply = self.msg("ラベル 放棄 No.18 依頼者 面 4")
        self.assertIn("面 4 は別の宛先で印刷待ちです", reply)
        self.assertEqual(self.msg("OK"), handler.MSG_NO_PENDING)
        reply = self.msg("ラベル 放棄 No.17 依頼者 面 11")
        self.assertTrue(reply.endswith("面番号は 1〜10 です"))

    # ── LP-05 ────────────────────────────────────────────────
    def test_lp05_reply_and_logs_do_not_echo_input(self):
        """「ラベル 放棄 No.17 依頼者 山田太郎」→ 返信・ログに「山田太郎」が含まれない。住所を含む誤入力も同様"""
        records: list[logging.LogRecord] = []

        class _Cap(logging.Handler):
            def emit(self, record):
                records.append(record)
        cap = _Cap(level=logging.DEBUG)
        root = logging.getLogger()
        old_level = root.level
        root.addHandler(cap)
        root.setLevel(logging.DEBUG)
        try:
            for text in ("ラベル 放棄 No.17 依頼者 山田太郎", "ラベル 放棄 No.17 依頼者 埼玉県川口市西青木9-9-9",
                         "ラベル 放棄 No.17 役所 山田太郎"):
                reply = self.msg(text)
                self.assertIn(reply, (lp.MSG_SYNTAX_UNKNOWN_WORD, lp.MSG_MUNI_NOT_FOUND), text)
        finally:
            root.removeHandler(cap)
            root.setLevel(old_level)
        logged = "\n".join(r.getMessage() for r in records)
        for tok in ("山田太郎", "埼玉県", "9-9-9"):
            self.assertNotIn(tok, logged)
        self.assertIn("[LABEL] syntax error len=", logged)

    # ── LP-06 ────────────────────────────────────────────────
    def test_lp06_undo_and_new_sheet_redelivery(self):
        """消費履歴 2 バッチで「戻す」の同一 event_id 再配送 → 1 バッチだけ戻る。「新しいシート」再配送でシートが増えない"""
        self.msg("ラベル 放棄 No.17 依頼者")
        self.msg("OK")
        self.msg("ラベル 印刷済", event="op-1")
        self.msg("ラベル 放棄 No.18 依頼者")
        self.msg("OK")
        self.msg("ラベル 印刷済", event="op-2")
        self.assertEqual(run(label_sheet.get_state()).used, (1, 2))
        self.assertIn("直前の消費（面 2）を取り消しました", self.msg("ラベル 戻す", event="op-u"))
        self.assertEqual(self.msg("ラベル 戻す", event="op-u"), lp.MSG_DUPLICATE_EVENT)
        self.assertEqual(run(label_sheet.get_state()).used, (1,))
        self.assertIn("新しいシート S-2 を開始しました", self.msg("ラベル 新しいシート", event="op-n"))
        self.assertEqual(self.msg("ラベル 新しいシート", event="op-n"), lp.MSG_DUPLICATE_EVENT)
        self.assertEqual(sheets_all(), ["S-1", "S-2"])
        self.assertEqual(self.msg("ラベル 印刷済", event="op-1"), lp.MSG_DUPLICATE_EVENT)
        # 意図的な繰り返し（別イベント）は通る
        self.assertIn("新しいシート S-3 を開始しました", self.msg("ラベル 新しいシート", event="op-n2"))

    # ── 操作コマンド・その他 ────────────────────────────────────
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
        self.fill_used(range(1, 11))
        self.assertEqual(self.msg("ラベル 放棄 No.17 依頼者"), lp.MSG_SHEET_FULL)
        reply = self.msg("ラベル 放棄 No.17 依頼者 面 3")   # 明示指定は再印字として通す
        self.assertIn("面 3/10 に印字。残り 0 面。OK?", reply)
        self.assertIn("添付しました", self.msg("OK"))

    def test_kintone_upload_failure_reports_without_pii_and_keeps_reserved(self):
        self.msg("ラベル 放棄 No.17 依頼者")

        async def boom(app, filename, content, mime):
            raise kintone.KintoneError(500, "GAIA_XX", "boom")
        with patch.object(kintone, "upload_file", boom):
            reply = self.msg("OK", event="ok-1")
        self.assertEqual(reply, handler.MSG_FILE_FAILED)
        self.assert_no_pii(self.notify.call_args.args[0])
        self.assertIn("案件No.17", self.notify.call_args.args[0])
        self.assertEqual(self.updates, [])
        self.assertEqual(jobs_all(), [(1, "ok-1", 1, "reserved", "")])

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


class TestRouterEventId(unittest.TestCase):
    def setUp(self):
        handler.reset_sessions()

    def test_webhook_passes_event_id_to_handler(self):
        from dispatch_bot import router
        seen = {}

        async def fake_handle(user_id, text, event_id=""):
            seen.update(user_id=user_id, text=text, event_id=event_id)
            return ""
        with patch("dispatch_bot.handler.handle_message", fake_handle), \
                patch.object(router, "is_allowed", lambda u: True):
            run(router.process_dispatch_bot_event("rt", "U1", "ラベル 残量", "01234567-89ab"))
        self.assertEqual(seen, {"user_id": "U1", "text": "ラベル 残量", "event_id": "01234567-89ab"})

    def test_handler_defaults_to_local_unique_event(self):
        seen = []

        async def fake_op(req, user_id):
            seen.append(handler.current_event_id.get())
            return "x"
        with patch.object(lp, "_sheet_op", fake_op):
            run(handler.handle_message("U1", "ラベル 残量"))
            run(handler.handle_message("U1", "ラベル 残量"))
            run(handler.handle_message("U1", "ラベル 残量", event_id="line-ev-1"))
        self.assertEqual(len(seen), 3)
        self.assertTrue(all(s.startswith("local:") for s in seen[:2]))
        self.assertNotEqual(seen[0], seen[1])
        self.assertEqual(seen[2], "line-ev-1")
        self.assertEqual(handler.current_event_id.get(), "")


if __name__ == "__main__":
    unittest.main()
