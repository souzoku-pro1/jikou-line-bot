"""P3-003a: hub/heir_envelope（DerivationRun→App30 要確認封筒）のテスト（fix1）。

正本 DRAFT_P3_003_ENVELOPE_FLOW §2＋§6 の契約を pin する:
- flag HEIR_DERIVATION_ENABLED 既定 OFF＝一切起票しない（kintone 呼出しゼロ）
- 対象は status derived/held のみ（error は not_target・起票しない）
- 起票 fields: 発送ステータス=要確認・実行済み=no・単票 API（create_record）・
  ユニット種別は案件由来（fix1 H03・解決不能は起票せず異常）
- 冪等: escape 済み like＋JSON 完全一致（fix1 H01・部分一致/別トップキー/壊れ JSON は
  再利用しない）
- 起票境界の検証: 型・grammar（数字列/hex64・曖昧値拒否=fix1 M01）＋lawyer_flags
  allowlist（PII sentinel 保存前拒否=fix1 H02）。policy 失敗は kintone I/O ゼロ
- 失敗時挙動（fix1 M02 契約 pin）: search/create/policy 失敗は例外伝播・
  新規起票を成功扱いにしない・例外時の追加 write ゼロ
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
from hub.derivation_models import PayloadPolicyError  # noqa: E402

IH = "a1" * 32   # 正規化 SHA-256 相当（小文字 hex 64）
RH = "b2" * 32


def _run(coro):
    return asyncio.run(coro)


def _mk_run(**over):
    base = dict(id=31, case_app_id="26", case_record_id="9",
                input_hash=IH, result_hash=RH,
                status="derived", provisional=False,
                lawyer_flags={"flags": ["F3"]},
                # result_payload は「読まない」ことの検証用 sentinel（封筒へ漏れない）
                result_payload={"heirs": [{"person_id": "胎児:F1"}]})
    base.update(over)
    return SimpleNamespace(**base)


_KEY = f"heir_derivation:9:{IH}"
_ON = {"HEIR_DERIVATION_ENABLED": "1",
       "SOUZOKU_KINTONE_APP_ID": "26", "KINTONE_APP_ID": "21"}


def _envelope_record(rid: str, key: str = _KEY, top: str = "heir_derivation",
                     raw: str | None = None):
    if raw is None:
        raw = json.dumps({top: {"冪等キー": key}}, ensure_ascii=False)
    return {"$id": {"value": rid}, "チャネル固有データ": {"value": raw}}


class _MockIo(unittest.TestCase):
    def _file(self, run, records=None, env=_ON):
        search = AsyncMock(return_value=records or [])
        create = AsyncMock(return_value="77")
        with patch.dict(os.environ, env), \
             patch.object(he.kintone, "search_records", new=search), \
             patch.object(he.kintone, "create_record", new=create):
            r = _run(he.file_heir_envelope(run))
        return r, search, create


class TestFlagGate(_MockIo):
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
        r, search, create = self._file(_mk_run(status="error"))
        self.assertEqual(r["status"], "not_target")
        search.assert_not_awaited()
        create.assert_not_awaited()


class TestFiling(_MockIo):
    def test_files_kakunin_envelope_with_expected_fields(self):
        r, search, create = self._file(_mk_run())
        self.assertEqual(r, {"status": "filed", "record_id": "77"})
        create.assert_awaited_once()         # 単票 API 1回のみ
        app, fields = create.await_args.args
        self.assertIs(app, he.APP_SHIPPING)
        self.assertEqual(fields["発送ステータス"], "要確認")
        self.assertEqual(fields["実行済み"], "no")
        self.assertEqual(fields["ユニット種別"], "相続一般")   # 案件(App26)由来・H03
        self.assertEqual(fields["案件レコードID"], "9")
        self.assertIn("run #31", fields["件名"])
        data = json.loads(fields["チャネル固有データ"])
        self.assertEqual(set(data), {"heir_derivation"})
        detail = data["heir_derivation"]
        self.assertEqual(set(detail), set(he._DETAIL_KEYS))
        self.assertEqual(detail["derivation_run_id"], 31)
        self.assertEqual(detail["冪等キー"], _KEY)

    def test_unit_resolved_from_jikou_case(self):
        r, _s, create = self._file(_mk_run(case_app_id="21"))
        _app, fields = create.await_args.args
        self.assertEqual(fields["ユニット種別"], "時効援用")   # 案件(App21)由来

    def test_unknown_case_app_raises_without_io(self):
        # H03: 案件から解決不能 → 起票せず異常扱い（kintone I/O ゼロ）
        search = AsyncMock()
        create = AsyncMock()
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search), \
             patch.object(he.kintone, "create_record", new=create):
            with self.assertRaises(he.EnvelopePolicyError):
                _run(he.file_heir_envelope(_mk_run(case_app_id="99")))
        search.assert_not_awaited()
        create.assert_not_awaited()

    def test_held_run_is_target(self):
        r, _s, create = self._file(_mk_run(status="held"))
        self.assertEqual(r["status"], "filed")
        create.assert_awaited_once()

    def test_no_pii_and_no_payload_copy_in_envelope(self):
        r, _s, create = self._file(_mk_run())
        _app, fields = create.await_args.args
        blob = json.dumps(fields, ensure_ascii=False)
        self.assertNotIn("heirs", blob)          # payload 本体を複製しない
        self.assertNotIn("胎児:F1", blob)
        self.assertNotIn("result_payload", blob)
        self.assertNotIn("顧客名表示用", fields)
        self.assertNotIn("宛先名", fields)


class TestIdempotentExactMatch(_MockIo):
    """fix1 H01: escape 済み like＋JSON 完全一致の照合（CloudSign 封筒経路の型）。"""

    def test_exact_match_reused(self):
        rec = _envelope_record("55")
        r, search, create = self._file(_mk_run(), records=[rec])
        self.assertEqual(r, {"status": "already_filed", "record_id": "55"})
        create.assert_not_awaited()              # 新規起票しない（§2.2）
        q = search.await_args.args[1]
        self.assertIn(_KEY, q)                   # like は冪等キーで発行

    def test_partial_match_not_reused(self):
        # 冪等キーが前方部分一致（別 input_hash の封筒）→ 再利用しない
        rec = _envelope_record("55", key=_KEY + "ff")
        r, _s, create = self._file(_mk_run(), records=[rec])
        self.assertEqual(r["status"], "filed")
        create.assert_awaited_once()

    def test_other_topkey_not_reused(self):
        # 同一キー文字列でもトップキーが heir_derivation でない封筒は再利用しない
        rec = _envelope_record("55", top="person_merge")
        r, _s, create = self._file(_mk_run(), records=[rec])
        self.assertEqual(r["status"], "filed")
        create.assert_awaited_once()

    def test_broken_json_not_reused(self):
        rec = _envelope_record("55", raw="{broken json!!")
        r, _s, create = self._file(_mk_run(), records=[rec])
        self.assertEqual(r["status"], "filed")
        create.assert_awaited_once()

    def test_query_value_is_escaped(self):
        # find_existing 単体: 引用符/バックスラッシュを含む値でも query が壊れない
        search = AsyncMock(return_value=[])
        with patch.object(he.kintone, "search_records", new=search):
            r = _run(he.find_existing('9" or 1', 'ab\\cd'))
        self.assertIsNone(r)
        q = search.await_args.args[1]
        self.assertIn('9\\" or 1', q)            # `"` → `\"`
        self.assertIn("ab\\\\cd", q)             # `\` → `\\`


class TestBoundaryValidation(_MockIo):
    """fix1 H02/M01: 起票境界の型・grammar 検証と PII 保存前拒否（I/O ゼロ）。"""

    def _reject(self, run, exc=he.EnvelopePolicyError):
        search = AsyncMock()
        create = AsyncMock()
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search), \
             patch.object(he.kintone, "create_record", new=create):
            with self.assertRaises(exc):
                _run(he.file_heir_envelope(run))
        search.assert_not_awaited()              # policy 失敗は kintone I/O ゼロ
        create.assert_not_awaited()

    def test_ambiguous_key_components_rejected(self):
        # M01: 冪等キー構成要素の曖昧値（`:`・記号・引用符・空）を拒否
        for bad in ("9:9", "R-9", '9"x', "", "9 9"):
            with self.subTest(case_record_id=bad):
                self._reject(_mk_run(case_record_id=bad))
        for bad in ("ih" * 8, "A1" * 32, "xyz", ""):
            with self.subTest(input_hash=bad):
                self._reject(_mk_run(input_hash=bad))

    def test_result_hash_and_ids_validated(self):
        self._reject(_mk_run(result_hash="not-hex"))
        self._reject(_mk_run(case_app_id="app-26"))
        self._reject(_mk_run(id="31"))           # 型違い（str）も拒否

    def test_bool_run_id_rejected(self):
        # fix3 M01: bool は int の subclass — isinstance 判定では True(=1>0) が
        # 素通りするため type() is int で拒否することを対照で固定
        self._reject(_mk_run(id=True))
        self._reject(_mk_run(id=False))

    def test_pii_sentinel_in_allowed_key_rejected_before_write(self):
        # H02: 許可キー lawyer_flags 内の PII 様値（enum 外）を保存前に拒否
        self._reject(_mk_run(lawyer_flags={"flags": ["山田太郎"]}),
                     exc=PayloadPolicyError)
        self._reject(_mk_run(lawyer_flags={"メモ": "090-1234-5678"}),
                     exc=PayloadPolicyError)


class TestFailureBehaviorContract(_MockIo):
    """契約 pin（fix1 M02 → P3-003-CMD §3B 改定〔[人]承認済み・裁定7/9〕で同時更新）:
    失敗は段階別固定例外3種（policy/search/create）で閉じる・握り潰し禁止・
    新規起票を成功扱いにしない・例外時の追加 write ゼロ・vendor 本文非保持・
    except 外 raise（__context__ is None）。"""

    def test_stage_exceptions_replace_raw_propagation(self):
        from hub.kintone import KintoneError
        # create 失敗 → EnvelopeCreateUnknownError（結果不明）。
        # App30 への write は当該単票 create 1回のみ（追加 write ゼロ）
        search = AsyncMock(return_value=[])
        create = AsyncMock(side_effect=KintoneError(500, "x", "boom"))
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search), \
             patch.object(he.kintone, "create_record", new=create):
            with self.assertRaises(he.EnvelopeCreateUnknownError):
                _run(he.file_heir_envelope(_mk_run()))
        self.assertEqual(search.await_count, 1)
        self.assertEqual(create.await_count, 1)  # 成功扱いの戻り値は返さない=例外のみ
        # 検索失敗 → EnvelopeSearchError（未起票のまま・create 未到達=write 0）
        search2 = AsyncMock(side_effect=KintoneError(503, "y", "down"))
        create2 = AsyncMock()
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search2), \
             patch.object(he.kintone, "create_record", new=create2):
            with self.assertRaises(he.EnvelopeSearchError):
                _run(he.file_heir_envelope(_mk_run()))
        create2.assert_not_awaited()

    def test_ack_lost_create_reconciled_on_retry(self):
        """fix2 H02→§3B: create の通信失敗=結果不明（EnvelopeCreateUnknownError）。
        kintone 側では封筒が作成済みだった場合、再実行は H01 の完全一致照合で
        回収し二重起票しない。"""
        from hub.kintone import KintoneError
        # 1回目: create が通信例外（実際には kintone 側で封筒 No.88 が作成済み）
        search1 = AsyncMock(return_value=[])
        create1 = AsyncMock(side_effect=KintoneError(0, "", "timeout"))
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search1), \
             patch.object(he.kintone, "create_record", new=create1):
            with self.assertRaises(he.EnvelopeCreateUnknownError):
                _run(he.file_heir_envelope(_mk_run()))
        # 2回目（再実行）: 検索が「1回目で実は作成されていた封筒」を返す
        search2 = AsyncMock(return_value=[_envelope_record("88")])
        create2 = AsyncMock()
        with patch.dict(os.environ, _ON), \
             patch.object(he.kintone, "search_records", new=search2), \
             patch.object(he.kintone, "create_record", new=create2):
            r = _run(he.file_heir_envelope(_mk_run()))
        self.assertEqual(r, {"status": "already_filed", "record_id": "88"})
        create2.assert_not_awaited()             # 二重起票しない（完全一致照合で回収）

    def test_stage_closed_set_and_wrapper_hygiene(self):
        """§3B/§7-18: stage 値域 {policy, search, create} の閉集合 pin＋
        sentinel 入り vendor 例外が wrapper の str/repr/args へ非残存・
        __context__ is None・__cause__ is None（except 外 raise の実証）。"""
        self.assertEqual(
            {he.EnvelopePolicyError.stage, he.EnvelopeSearchError.stage,
             he.EnvelopeCreateUnknownError.stage},
            {"policy", "search", "create"})

        sentinel = "VENDOR-SENTINEL-山田太郎-090-1234"
        for target, exc_type in (
                ("search_records", he.EnvelopeSearchError),
                ("create_record", he.EnvelopeCreateUnknownError)):
            mocks = {"search_records": AsyncMock(return_value=[]),
                     "create_record": AsyncMock(return_value="77")}
            mocks[target] = AsyncMock(side_effect=RuntimeError(sentinel))
            with patch.dict(os.environ, _ON), \
                 patch.object(he.kintone, "search_records",
                              new=mocks["search_records"]), \
                 patch.object(he.kintone, "create_record",
                              new=mocks["create_record"]):
                with self.assertRaises(exc_type) as ctx:
                    _run(he.file_heir_envelope(_mk_run()))
            e = ctx.exception
            for surface in (str(e), repr(e), repr(e.args)):
                self.assertNotIn(sentinel, surface, (target, surface))
            self.assertIsNone(e.__context__, target)   # except 外 raise（裁定9）
            self.assertIsNone(e.__cause__, target)
            self.assertFalse(vars(e), "vendor 例外を属性へ保存しない（§3B）")

        # policy 段: 固定例外そのもの（vendor 例外の介在なし・I/O 0）
        with self.assertRaises(he.EnvelopePolicyError) as ctx:
            he._validated_snapshot(_mk_run(input_hash="ZZZ"))
        self.assertIsNone(ctx.exception.__context__)
        self.assertEqual(he.EnvelopePolicyError.stage, "policy")


class TestDetailClosedSet(unittest.TestCase):
    def test_detail_builder_pins_closed_set(self):
        snap = he._validated_snapshot(_mk_run())
        d = he._build_detail(snap)
        self.assertEqual(set(d), set(he._DETAIL_KEYS))

    def test_idempotency_key_format(self):
        self.assertEqual(he.idempotency_key("9", "abc"),
                         "heir_derivation:9:abc")


if __name__ == "__main__":
    unittest.main()
