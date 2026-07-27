"""P3-003a: hub/heir_envelope（DerivationRun→App30 要確認封筒）のテスト。

正本 DRAFT_P3_003_ENVELOPE_FLOW §2 の契約を pin する:
- flag HEIR_DERIVATION_ENABLED 既定 OFF＝一切起票しない（kintone 呼出しゼロ）
- 対象は status derived/held のみ（error は not_target・起票しない）
- 起票 fields: 発送ステータス=要確認・実行済み=no・単票 API（create_record）
- 冪等: 同一冪等キーの既存封筒があれば新規起票しない（already_filed）
- PII 非混入: detail は _DETAIL_KEYS 閉集合のみ・result_payload/氏名を封筒へ複製しない
kintone は全て mock（実機・ネットワーク非依存）。
"""

import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from hub import heir_envelope as he  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


def _mk_run(**over):
    base = dict(id=31, case_app_id="26", case_record_id="R-9",
                input_hash="ih" * 8, result_hash="rh" * 8,
                status="derived", provisional=False,
                lawyer_flags={"flags": ["F3"]},
                # result_payload は file_heir_envelope が「読まない」ことの検証用に
                # あえて sentinel を持たせる（封筒 JSON に漏れないことを assert）
                result_payload={"heirs": [{"person_id": "胎児:F1"}]})
    base.update(over)
    return SimpleNamespace(**base)


_ON = {"HEIR_DERIVATION_ENABLED": "1"}


class TestFlagGate(unittest.TestCase):
    def test_flag_off_files_nothing(self):
        search = AsyncMock()
        create = AsyncMock()
        env = {k: v for k, v in os.environ.items()
               if k != "HEIR_DERIVATION_ENABLED"}
        with patch.dict(os.environ, env, clear=True), \
             patch.object(he.kintone, "search_records", new=search), \
             patch.object(he.kintone, "create_record", new=create):
            r = _run(he.file_heir_envelope(_mk_run()))
        self.assertEqual(r, {"status": "disabled", "record_id": None})
        search.assert_not_awaited()          # kintone 呼出しゼロ
        create.assert_not_awaited()

    def test_error_run_is_not_target(self):
        search = AsyncMock()
        create = AsyncMock()
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search), \
             patch.object(he.kintone, "create_record", new=create):
            r = _run(he.file_heir_envelope(_mk_run(status="error")))
        self.assertEqual(r["status"], "not_target")
        search.assert_not_awaited()
        create.assert_not_awaited()


class TestFiling(unittest.TestCase):
    def _file(self, run, existing=None):
        search = AsyncMock(return_value=(
            [{"$id": {"value": existing}}] if existing else []))
        create = AsyncMock(return_value="77")
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search), \
             patch.object(he.kintone, "create_record", new=create):
            r = _run(he.file_heir_envelope(run))
        return r, search, create

    def test_files_kakunin_envelope_with_expected_fields(self):
        r, search, create = self._file(_mk_run())
        self.assertEqual(r, {"status": "filed", "record_id": "77"})
        # 単票 API（create_record）1回のみ
        create.assert_awaited_once()
        app, fields = create.await_args.args
        self.assertIs(app, he.APP_SHIPPING)
        self.assertEqual(fields["発送ステータス"], "要確認")
        self.assertEqual(fields["実行済み"], "no")
        self.assertEqual(fields["ユニット種別"], "相続一般")
        self.assertEqual(fields["案件レコードID"], "R-9")
        self.assertIn("run #31", fields["件名"])
        # チャネル固有データ: トップキー方式＋detail 閉集合＋冪等キー平文
        data = json.loads(fields["チャネル固有データ"])
        self.assertEqual(set(data), {"heir_derivation"})
        detail = data["heir_derivation"]
        self.assertEqual(set(detail), set(he._DETAIL_KEYS))
        self.assertEqual(detail["derivation_run_id"], 31)
        self.assertEqual(detail["冪等キー"],
                         f"heir_derivation:R-9:{'ih' * 8}")

    def test_idempotent_existing_envelope_reused(self):
        r, search, create = self._file(_mk_run(), existing="55")
        self.assertEqual(r, {"status": "already_filed", "record_id": "55"})
        create.assert_not_awaited()          # 新規起票しない（§2.2）
        # like 検索は冪等キーで行われる
        q = search.await_args.args[1]
        self.assertIn(f'heir_derivation:R-9:{"ih" * 8}', q)

    def test_held_run_is_target(self):
        r, _s, create = self._file(_mk_run(status="held"))
        self.assertEqual(r["status"], "filed")
        create.assert_awaited_once()

    def test_no_pii_and_no_payload_copy_in_envelope(self):
        # result_payload（sentinel）・氏名様の値が封筒 JSON/件名へ漏れない
        r, _s, create = self._file(_mk_run())
        _app, fields = create.await_args.args
        blob = json.dumps(fields, ensure_ascii=False)
        self.assertNotIn("heirs", blob)          # payload 本体を複製しない
        self.assertNotIn("胎児:F1", blob)
        self.assertNotIn("result_payload", blob)
        # 顧客名系フィールドを封筒に設定しない（App30 の顧客名表示用も未使用）
        self.assertNotIn("顧客名表示用", fields)
        self.assertNotIn("宛先名", fields)


class TestFailureBehaviorContract(unittest.TestCase):
    """契約 (a) の失敗時挙動: kintone I/O の失敗は握らず送出（部分状態なし・
    再実行は冪等キーで安全＝リトライ判断は呼出し元の責務）。"""

    def test_kintone_errors_propagate_unhandled(self):
        from hub.kintone import KintoneError
        # create 失敗 → 送出（検索は通過済み・App30 書込みは単票1回のみ＝部分状態なし）
        search = AsyncMock(return_value=[])
        create = AsyncMock(side_effect=KintoneError(500, "x", "boom"))
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search), \
             patch.object(he.kintone, "create_record", new=create):
            with self.assertRaises(KintoneError):
                _run(he.file_heir_envelope(_mk_run()))
        # 検索失敗 → 送出（未起票のまま・create には到達しない）
        search2 = AsyncMock(side_effect=KintoneError(503, "y", "down"))
        create2 = AsyncMock()
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search2), \
             patch.object(he.kintone, "create_record", new=create2):
            with self.assertRaises(KintoneError):
                _run(he.file_heir_envelope(_mk_run()))
        create2.assert_not_awaited()


class TestDetailClosedSet(unittest.TestCase):
    def test_detail_builder_pins_closed_set(self):
        d = he._build_detail(_mk_run())
        self.assertEqual(set(d), set(he._DETAIL_KEYS))

    def test_idempotency_key_format(self):
        self.assertEqual(he.idempotency_key("R-1", "abc"),
                         "heir_derivation:R-1:abc")


if __name__ == "__main__":
    unittest.main()
