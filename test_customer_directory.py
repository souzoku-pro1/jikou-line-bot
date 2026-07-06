"""customer_directory.py（書類仕分け第1段 T1・名寄せ部品）のテスト

検証:
候補リストの写像（record_id/app_id/source/氏名/被相続人名/書類ステータス）・
完了扱いレコードを除外しないこと・氏名空レコードの除外・0件時の空リスト・
env 欠落時の縮退（空リスト・API 非呼び出し）・ページング（500件境界）・
label() のプロンプト注入表記・第1段のソースが App 26 のみであること。
kintone 接続はすべてモック。
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import customer_directory
from customer_directory import Candidate, list_candidates

_ENV = {
    "KINTONE_SUBDOMAIN": "testsub",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "SOUZOKU_KINTONE_API_TOKEN": "token26",
}


def run(coro):
    return asyncio.run(coro)


def card(rec_id, name, decedent="", doc_status=""):
    """App 26（相談カード (相続)）のレコード（本部品が読む4フィールド分）"""
    return {
        "$id": {"value": str(rec_id)},
        "氏名": {"value": name},
        "被相続人名": {"value": decedent},
        "書類ステータス": {"value": doc_status},
    }


class TestListCandidates(unittest.TestCase):
    def _run_with(self, records_pages, env=_ENV):
        """search_records をページ列でモックして list_candidates を実行する"""
        mock = AsyncMock(side_effect=records_pages)
        with patch.dict(os.environ, env, clear=True), \
                patch.object(customer_directory.kintone, "search_records", mock):
            return run(list_candidates()), mock

    def test_mapping(self):
        """App 26 レコード → Candidate の写像（全属性）"""
        hits, _ = self._run_with([[card(12, "山田太郎", "山田一郎", "送付状作成済")]])
        self.assertEqual(hits, [Candidate(
            record_id="12", app_id="26", source="相談カード (相続)",
            customer_name="山田太郎", decedent_name="山田一郎",
            status="送付状作成済")])

    def test_processed_records_are_included(self):
        """処理済み（書類ステータスあり）も除外しない（過去案件の書類が届くため）。
        状態は status に持たせ判定側の参考とする"""
        hits, _ = self._run_with([[
            card(1, "佐藤花子", doc_status="送付状作成済"),
            card(2, "鈴木次郎", doc_status=""),
        ]])
        self.assertEqual([h.customer_name for h in hits], ["佐藤花子", "鈴木次郎"])
        self.assertEqual(hits[0].status, "送付状作成済")

    def test_blank_name_is_skipped(self):
        """氏名が空・空白のみのレコードは名寄せに使えないため含めない"""
        hits, _ = self._run_with([[card(1, ""), card(2, "　"), card(3, "田中三郎")]])
        self.assertEqual([h.customer_name for h in hits], ["田中三郎"])

    def test_zero_records(self):
        hits, mock = self._run_with([[]])
        self.assertEqual(hits, [])
        self.assertEqual(mock.await_count, 1)

    def test_env_unset_returns_empty_without_api_call(self):
        """env 未設定は縮退: 空リスト・kintone API を一切呼ばない"""
        for env in ({"KINTONE_SUBDOMAIN": "testsub"},  # 両方欠落
                    {"KINTONE_SUBDOMAIN": "testsub",
                     "SOUZOKU_KINTONE_APP_ID": "26"}):  # トークンのみ欠落
            with self.subTest(env=env):
                hits, mock = self._run_with([[]], env=env)
                self.assertEqual(hits, [])
                self.assertEqual(mock.await_count, 0)

    def test_pagination_over_500(self):
        """500件ちょうどのページが返ったら次ページを取りに行く（100件上限の回避）"""
        page1 = [card(i, f"顧客{i}") for i in range(1, 501)]
        page2 = [card(501, "顧客501")]
        hits, mock = self._run_with([page1, page2])
        self.assertEqual(len(hits), 501)
        self.assertEqual(mock.await_count, 2)
        queries = [c.args[1] for c in mock.await_args_list]
        self.assertIn("offset 0", queries[0])
        self.assertIn("offset 500", queries[1])

    def test_under_page_size_stops_after_one_call(self):
        hits, mock = self._run_with([[card(1, "山田太郎")]])
        self.assertEqual(len(hits), 1)
        self.assertEqual(mock.await_count, 1)

    def test_stage1_source_is_app26_only(self):
        """第1段のソースは App 26 のみ（App 21 追加は _SOURCES 拡張で行う将来スコープ）"""
        self.assertEqual(
            [s.app.app_id_env for s in customer_directory._SOURCES],
            ["SOUZOKU_KINTONE_APP_ID"])


class TestCandidateLabel(unittest.TestCase):
    def test_label_full(self):
        c = Candidate(record_id="12", app_id="26", source="相談カード (相続)",
                      customer_name="山田太郎", decedent_name="山田一郎",
                      status="送付状作成済")
        self.assertEqual(
            c.label(),
            "山田太郎（被相続人: 山田一郎・No.12・相談カード (相続)・送付状作成済）")

    def test_label_minimal(self):
        c = Candidate(record_id="3", app_id="26", source="相談カード (相続)",
                      customer_name="佐藤花子")
        self.assertEqual(c.label(), "佐藤花子（No.3・相談カード (相続)）")


if __name__ == "__main__":
    unittest.main()
