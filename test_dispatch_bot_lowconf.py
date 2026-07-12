"""実機不具合の回帰（2026-07-04）: 低確信度分岐の task_type 未特定への限定

事象: 「テスト太郎の戸籍謄本２通と附票１通を川口市に職務上請求」に対し
parsed intent=task task=shokumu_seikyu conf=low（Railwayログ実出力）となり、
handler の低確信度分岐が定型聞き返し（送付案内の例文付き）を返して
抽出結果を全破棄した。

固定内容:
- task_type 特定済みなら conf=low でもレジストリ駆動フローへ進む
  （不足はコード側の個別質問が解消・氏名の帰属曖昧さは customer 質問で自然解決）
- 定型聞き返しは task_type 未特定（null）のときのみ
- 未登録の task_type 名（fax_send 等）は従来どおり未対応案内
- プロンプトに confidence の定義（task_type特定への自信・写像や全角数字は
  low の理由にしない）が明記されていること
"""

import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")
os.environ.setdefault("APP_SHIPPING", "30")
os.environ.setdefault("TOKEN_SHIPPING", "dummy")

from dispatch_bot import case_search, handler, parser  # noqa: E402

MUNI_RECORD = {"市区町村名": {"value": "川口市"}, "担当部署": {"value": "市民課"},
               "郵便番号": {"value": "332-8601"}, "住所": {"value": "埼玉県川口市青木2-1-1"},
               "手数料_戸籍謄本": {"value": "450"}, "手数料_除籍改製原": {"value": "750"},
               "手数料_附票": {"value": "300"}, "手数料_住民票": {"value": "300"},
               "備考": {"value": ""}}

# 実機で観測された解析結果の再現（conf=low・種別/通数/自治体は抽出済みの想定）
OBSERVED_PARAMS = {"request_items": [{"type": "戸籍謄本", "count": 2},
                                     {"type": "戸籍の附票", "count": 1}],
                   "municipality": "川口市",
                   "target": {"対象者": "テスト太郎"}}


def parsed(**over):
    base = {"intent": "task", "task_type": "shokumu_seikyu",
            "customer_name": None, "task_params": dict(OBSERVED_PARAMS),
            "confidence": "low", "missing_fields": [], "clarification": None}
    base.update(over)
    return base


def hit(rid="45", name="テスト太郎", status="受任"):
    return case_search.CaseHit(record_id=rid, customer_name=name, status=status)


class Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    def patches(self, parse_results):
        seq = parse_results if isinstance(parse_results, list) else [parse_results]

        async def fake_search(app, query, fields=None):
            return [MUNI_RECORD] if "市区町村名" in query else []

        return [
            patch.object(parser, "parse_instruction", new=AsyncMock(side_effect=seq)),
            patch.object(case_search, "search_cases",
                         new=AsyncMock(return_value=[hit()])),
            patch("hub.kintone.search_records", new=AsyncMock(side_effect=fake_search)),
            patch("hub.kintone.get_record",
                  new=AsyncMock(return_value={"顧客名": {"value": "テスト太郎"},
                                              "住所": {"value": "X"},
                                              "郵便番号": {"value": ""}})),
            patch("hub.kintone.create_record", new=AsyncMock(return_value="301")),
        ]


class TestLowConfWithResolvedTask(Base):
    async def test_observed_case_customer_missing_asks_registry_question(self):
        """実機再現: conf=low・params抽出済み・顧客名の帰属曖昧（未設定）
        → 定型聞き返しではなく、レジストリの顧客名質問に進む"""
        ps = self.patches([parsed()])
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            reply = await handler.handle_message(
                "U1", "テスト太郎の戸籍謄本２通と附票１通を川口市に職務上請求")
        self.assertEqual(reply, "どの顧客（案件）への指示ですか？氏名を教えてください")
        self.assertNotIn("もう少し具体的に", reply, "定型聞き返しに落ちない")
        self.assertNotIn("送付案内を作って", reply, "無関係な例文を出さない")

    async def test_low_conf_with_full_params_goes_straight_to_confirmation(self):
        """conf=low でも全項目そろっていれば復唱直行（抽出結果を破棄しない）"""
        seq = [parsed(customer_name="テスト太郎",
                      task_params={**OBSERVED_PARAMS,
                                   "target": {"対象者": "テスト太郎",
                                              "生年月日": "昭和25年3月15日"}})]
        ps = self.patches(seq)
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            reply = await handler.handle_message(
                "U1", "テスト太郎の戸籍謄本２通と附票１通を川口市に職務上請求")
        self.assertIn("【確認】以下で起票します", reply)
        self.assertIn("請求: 戸籍謄本2通・戸籍の附票1通", reply)
        self.assertIn("小為替概算: 1,200円", reply)

    async def test_unresolved_task_type_still_asks_generic(self):
        """task_type 未特定（null）の低確信度のみ定型聞き返し"""
        ps = self.patches([parsed(task_type=None, task_params={})])
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            reply = await handler.handle_message("U1", "あれやっといて")
        self.assertIn("もう少し具体的に", reply)

    async def test_unregistered_named_task_still_unsupported(self):
        """未登録の task_type 名は従来どおり未対応案内（低確信度でも）"""
        ps = self.patches([parsed(task_type="fax_send", task_params={})])
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            reply = await handler.handle_message("U1", "FAXして")
        self.assertEqual(reply, handler.MSG_UNSUPPORTED)


class TestPromptAndLogging(unittest.TestCase):
    def test_prompt_defines_confidence(self):
        """修正2: confidence 定義（task_type特定への自信・写像等はlowの理由にしない）"""
        prompt = parser.build_system_prompt()
        self.assertIn("confidence は「task_type を正しく特定できたか」への自信", prompt)
        self.assertIn("附票→戸籍の附票", prompt)
        self.assertIn("全角数字", prompt)
        self.assertIn("low の理由にしない", prompt)
        self.assertIn("task_type が特定できないときのみ confidence=low", prompt)

    def test_parsed_log_redacts_params_and_drops_raw_conf(self):
        """P1-107b（仕様変更・RV-10 redaction）: print→logger 移送に伴い、parsed ログの
        task_params と customer_name は emit 契約経由で抑止し、raw confidence（controlled
        status・素通し kind なし）は出力から drop する。2026-07-04 の生値ログ
        （raw_conf=/params= の生値埋め込み）は PII 露出のため廃止した（緩和ではなく仕様変更）。"""
        import pathlib
        src = pathlib.Path(parser.__file__).read_text(encoding="utf-8")
        self.assertNotIn("raw_conf=", src)                    # 生 confidence は drop
        self.assertIn('emit(parsed["customer_name"], "name"', src)  # 氏名は emit 抑止
        self.assertIn("emit(params,", src)                    # params も emit 経由


if __name__ == "__main__":
    unittest.main()
