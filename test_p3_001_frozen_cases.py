"""P3-001 fix3 接続テスト: 凍結 47 ケース全数を「導出→payload 変換→validate→保存」まで通す。

- 凍結 fixture は test_heir_derivation.py（正解仕様 09-heir-test-cases.md v0.1・
  弁護士承認・凍結・47 ケース）を**そのまま実行**し、derive_heirs の全戻り値を spy で
  捕捉する（fixture の複製を作らない＝凍結の単一ソース維持）。
- 捕捉した各 Derivation を build_run_payload → validate → create_derivation_run で
  sqlite へ実保存する（胎児ケース含む）。
- flag 写像の全数一致: heir_derivation ソースから flag リテラルを機械抽出し、
  flag_key が全種を写像できることを検査（情報を provisional へ潰さない・M01）。
"""

import asyncio
import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import hub.db as db
from hub.derivation_models import (DerivationBase, _LAWYER_FLAG_KEYS,
                                   build_run_payload, create_derivation_run,
                                   flag_key, validate_lawyer_flags,
                                   validate_result_payload)


def _run(coro):
    return asyncio.run(coro)


class TestFrozenCasesEndToEnd(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p3frozen_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/a.db"})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(DerivationBase.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def test_all_frozen_cases_convert_validate_and_persist(self):
        import heir_derivation
        import test_heir_derivation as frozen

        captured = []
        real = heir_derivation.derive_heirs

        def spy(*a, **k):
            r = real(*a, **k)
            captured.append(r)
            return r

        suite = unittest.TestLoader().loadTestsFromModule(frozen)
        result = unittest.TestResult()
        with patch.object(heir_derivation, "derive_heirs", spy), \
             patch.object(frozen, "derive_heirs", spy):
            suite.run(result)
        self.assertTrue(result.wasSuccessful(),
                        f"凍結ケース自体が FAIL: {result.failures[:1]}"
                        f"{result.errors[:1]}")
        self.assertGreaterEqual(len(captured), 47)   # 47 ケース（＋数次の内部導出）

        # 凍結 fixture の person id（"M"/"S" 等の記号）は App34 `$id` の stand-in。
        # grammar は本番形式（数字列/胎児:）を守るため、テスト側で記号→数字の写像を
        # 掛けてから validate/保存する（胎児合成 ID は実形式のためそのまま通す）。
        id_map: dict[str, str] = {}

        def _mapped(pid: str) -> str:
            if pid.startswith("胎児:") or pid.isdigit():
                return pid
            return id_map.setdefault(pid, str(len(id_map) + 1))

        fetus_seen = False
        saved = 0
        for i, deriv in enumerate(captured):
            payload, flags = build_run_payload(deriv)          # 変換（氏名はここで落ちる）
            for h in payload["heirs"]:
                h["person_id"] = _mapped(h["person_id"])
            validate_result_payload(payload)                   # validate
            validate_lawyer_flags(flags)
            pk = _run(create_derivation_run(                   # 保存（正規経路）
                case_app_id="26", case_record_id=f"R-frozen-{i}",
                decedent_person_id="0", at_date="2026-01-01",
                frozen_case_version="v0.1",
                input_person_revisions={}, input_person_ids=[],
                input_hash=f"frozen-{i}", status=deriv.status,
                rank=deriv.rank, result_payload=payload,
                result_hash=f"rh-{i}", lawyer_flags=flags,
                provisional=deriv.provisional, engine_version="hd-frozen"))
            db.reset_for_tests()
            self.assertIsInstance(pk, int)
            saved += 1
            if any(h["person_id"].startswith("胎児:") for h in payload["heirs"]):
                fetus_seen = True
        self.assertGreaterEqual(saved, 47)
        self.assertTrue(fetus_seen, "胎児ケース（F3）が接続経路を通っていない")

    def test_flag_mapping_covers_all_engine_flags(self):
        # heir_derivation ソースから flag リテラル全数を機械抽出（ctx flags＋_Hold flag）
        src = Path("heir_derivation.py").read_text(encoding="utf-8")
        literals = set(re.findall(r'"flag": "([^"]+)"', src))
        literals |= set(re.findall(r',\s*"([A-Z][0-9])"\)', src))
        literals |= {"数次"}   # line 610: suji_hold.flag or "数次" の fallback
        self.assertGreaterEqual(len(literals), 10, literals)
        mapped = {flag_key(fl) for fl in literals}   # 未写像なら PayloadPolicyError
        self.assertLessEqual(mapped, _LAWYER_FLAG_KEYS,
                             "写像結果が enum 外（enum と変換関数がずれている）")


if __name__ == "__main__":
    unittest.main()
