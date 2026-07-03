"""channels/shokumu_seikyu.py + import_city_master.py のテスト（T3-1）

- 手数料・小為替計算（種別別・複数通・除籍/改製原の同一料金・欠損→登録依頼）
- 宛先引き当て（未登録/住所なし → PrepareDeferred＝エラーにしない）
- prepare 成果物（チェックリストPDF・宛先書き戻し・kogawase_total）
- ディスパッチャ結線: PrepareDeferred で状態変更なし＋登録依頼警報
- 投入スクリプトの dry-run 検証（パース・件数検証・重複スキップ）
"""

import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import import_city_master as icm
from channels import shokumu_seikyu as sk
from channels.base import PrepareDeferred
from channels.shokumu_seikyu import (
    ShokumuSeikyuAdapter,
    ShokumuSeikyuError,
    compute_kogawase,
    find_municipality,
    parse_channel_data,
)


def run(coro):
    return asyncio.run(coro)


def muni(name="川口市", address="埼玉県川口市青木2-1-1", zip_="332-8601",
         dept="市民課", **fees):
    base = {"手数料_戸籍謄本": "450", "手数料_除籍改製原": "750",
            "手数料_附票": "300", "手数料_住民票": "300"}
    base.update(fees)
    rec = {"市区町村名": {"value": name}, "都道府県": {"value": "埼玉県"},
           "担当部署": {"value": dept}, "郵便番号": {"value": zip_},
           "住所": {"value": address}, "備考": {"value": ""}}
    for k, v in base.items():
        rec[k] = {"value": v}
    return rec


def shipping_record(items=None, municipality=None, **over):
    # 生年月日は T3-2 追加要件（様式1で必須）に伴いフィクスチャへ追加（2026-07-03）
    data = {"request_items": items or [{"type": "戸籍謄本", "count": 1}],
            "target": {"対象者": "山田太郎", "本籍": "埼玉県川口市…",
                       "生年月日": "昭和25年3月15日"},
            "purpose": "消滅時効援用通知書の送付先調査のため"}
    if municipality:
        data["municipality"] = municipality
    rec = {
        "$id": {"value": "9"}, "$revision": {"value": "1"},
        "発送ステータス": {"value": "下書き"},
        "チャネル": {"value": "職務上請求"},
        "ユニット種別": {"value": "時効援用"},
        "件名": {"value": "職務上請求（川口市・戸籍）"},
        "顧客名表示用": {"value": "山田太郎"},
        "宛先名": {"value": "川口市"},
        "実行済み": {"value": "no"},
        "チャネル固有データ": {"value": json.dumps(data, ensure_ascii=False)},
    }
    rec.update(over)
    return rec


class TestParseChannelData(unittest.TestCase):
    def test_valid(self):
        data = parse_channel_data(shipping_record())
        self.assertEqual(data["request_items"][0]["type"], "戸籍謄本")

    def test_invalid_json_raises(self):
        rec = shipping_record(チャネル固有データ={"value": "{壊れたjson"})
        with self.assertRaises(ShokumuSeikyuError):
            parse_channel_data(rec)

    def test_missing_items_raises(self):
        rec = shipping_record(チャネル固有データ={"value": "{}"})
        with self.assertRaises(ShokumuSeikyuError):
            parse_channel_data(rec)

    def test_unknown_type_raises(self):
        rec = shipping_record(items=[{"type": "パスポート", "count": 1}])
        with self.assertRaises(ShokumuSeikyuError) as ctx:
            parse_channel_data(rec)
        self.assertIn("パスポート", str(ctx.exception))

    def test_invalid_count_raises(self):
        for count in (0, -1, "2", None):
            rec = shipping_record(items=[{"type": "戸籍謄本", "count": count}])
            with self.assertRaises(ShokumuSeikyuError):
                parse_channel_data(rec)


class TestKogawase(unittest.TestCase):
    """手数料・小為替計算（T3-1 完了条件の中核）"""

    def test_single_type(self):
        total, lines = compute_kogawase([{"type": "戸籍謄本", "count": 1}], muni())
        self.assertEqual(total, 450)
        self.assertIn("戸籍謄本 1通 × 450円 = 450円", lines[0])

    def test_multiple_types_and_counts(self):
        items = [{"type": "戸籍謄本", "count": 2},      # 900
                 {"type": "除籍謄本", "count": 1},      # 750
                 {"type": "戸籍の附票", "count": 3}]    # 900
        total, lines = compute_kogawase(items, muni())
        self.assertEqual(total, 2550)
        self.assertEqual(len(lines), 3)

    def test_joseki_and_kaiseigen_share_fee_field(self):
        """除籍謄本と改製原戸籍は同一フィールド（手数料_除籍改製原）"""
        m = muni(手数料_除籍改製原="800")
        t1, _ = compute_kogawase([{"type": "除籍謄本", "count": 1}], m)
        t2, _ = compute_kogawase([{"type": "改製原戸籍", "count": 1}], m)
        self.assertEqual((t1, t2), (800, 800))

    def test_missing_fee_defers_with_request(self):
        """手数料欠損 → エラーではなく登録依頼（PrepareDeferred）"""
        m = muni(手数料_附票="")
        with self.assertRaises(PrepareDeferred) as ctx:
            compute_kogawase([{"type": "戸籍の附票", "count": 1}], m)
        msg = str(ctx.exception)
        self.assertIn("手数料が未登録", msg)
        self.assertIn("手数料_附票", msg)
        self.assertIn("川口市", msg)

    def test_missing_fee_lists_all_missing(self):
        m = muni(手数料_戸籍謄本="", 手数料_住民票=None)
        with self.assertRaises(PrepareDeferred) as ctx:
            compute_kogawase([{"type": "戸籍謄本", "count": 1},
                              {"type": "住民票", "count": 1}], m)
        self.assertIn("手数料_戸籍謄本", str(ctx.exception))
        self.assertIn("手数料_住民票", str(ctx.exception))


class TestFindMunicipality(unittest.IsolatedAsyncioTestCase):
    async def test_found_by_recipient_name(self):
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=[muni()])) as s:
            got = await find_municipality(shipping_record(), {"request_items": []})
        self.assertEqual(got["市区町村名"]["value"], "川口市")
        self.assertIn('市区町村名 = "川口市"', s.await_args.args[1])

    async def test_municipality_key_takes_precedence(self):
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=[muni("蕨市")])) as s:
            await find_municipality(shipping_record(), {"municipality": "蕨市"})
        self.assertIn('市区町村名 = "蕨市"', s.await_args.args[1])

    async def test_not_found_defers(self):
        """未登録の自治体 → エラーではなく登録依頼"""
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=[])):
            with self.assertRaises(PrepareDeferred) as ctx:
                await find_municipality(shipping_record(), {})
        self.assertIn("川口市", str(ctx.exception))
        self.assertIn("レコードがありません", str(ctx.exception))

    async def test_missing_address_defers(self):
        """住所未登録 → エラーではなく住所登録依頼（T3-1 指示の中核）"""
        with patch("hub.kintone.search_records",
                   new=AsyncMock(return_value=[muni(address="")])):
            with self.assertRaises(PrepareDeferred) as ctx:
                await find_municipality(shipping_record(), {})
        self.assertIn("住所が未登録", str(ctx.exception))

    async def test_no_recipient_raises_error(self):
        rec = shipping_record(宛先名={"value": ""})
        with self.assertRaises(ShokumuSeikyuError):
            await find_municipality(rec, {})


class TestPrepare(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_outputs_checklist_and_fields(self):
        items = [{"type": "戸籍謄本", "count": 2}, {"type": "住民票の除票", "count": 1}]
        rec = shipping_record(items=items, 宛先名={"value": ""},
                              チャネル固有データ={"value": json.dumps(
                                  {"request_items": items, "municipality": "川口市",
                                   "target": {"対象者": "山田太郎",
                                              "生年月日": "昭和25年3月15日"},
                                   "purpose": "調査"},
                                  ensure_ascii=False)})
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=[muni()])):
            result = await ShokumuSeikyuAdapter().prepare(rec)

        self.assertEqual(result.artifacts[0].filename, "発送準備チェックリスト.pdf")
        self.assertTrue(result.artifacts[0].content.startswith(b"%PDF"))
        self.assertEqual(result.fields["宛先名"], "川口市　市民課")
        self.assertEqual(result.fields["宛先郵便番号"], "332-8601")
        self.assertEqual(result.fields["宛先住所"], "埼玉県川口市青木2-1-1")
        meta = json.loads(result.fields["チャネル固有データ"])
        self.assertEqual(meta["kogawase_total"], 450 * 2 + 300)

    async def test_existing_recipient_name_is_kept(self):
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=[muni()])):
            result = await ShokumuSeikyuAdapter().prepare(
                shipping_record(宛先名={"value": "川口市"}))
        self.assertEqual(result.fields["宛先名"], "川口市")

    async def test_dispatch_is_manual_and_needs_return(self):
        adapter = ShokumuSeikyuAdapter()
        self.assertTrue(adapter.needs_return)
        result = await adapter.dispatch(shipping_record())
        self.assertTrue(result.manual_mailing)

    async def test_adapter_not_registered_yet(self):
        """T3-3 まで CHANNEL_REGISTRY に登録されない"""
        import channels
        self.assertIsNone(channels.get_adapter("職務上請求"))


class TestDispatcherDeferred(unittest.IsolatedAsyncioTestCase):
    """PrepareDeferred: 状態変更なし＋登録依頼警報（エラー遷移しない）"""

    async def test_deferred_keeps_draft_and_alerts(self):
        import copy

        from channels.base import ChannelAdapter, PrepareResult
        from hub import dispatch

        class DeferAdapter(ChannelAdapter):
            channel_name = "職務上請求"

            async def prepare(self, record):
                raise PrepareDeferred("市区町村マスタ（App 31）に住所が未登録です")

        rec = shipping_record()
        records = {"9": copy.deepcopy(rec)}
        updates = []

        async def fake_get(app, rid):
            return copy.deepcopy(records[rid])

        async def fake_update(app, rid, fields, revision=None):
            updates.append(fields)

        import channels
        notify_admin = AsyncMock()
        with patch.dict(channels.CHANNEL_REGISTRY, {"職務上請求": DeferAdapter()}, clear=True), \
             patch("hub.kintone.get_record", new=fake_get), \
             patch("hub.kintone.update_record", new=fake_update), \
             patch("hub.notify.notify_admin_line", new=notify_admin):
            await dispatch.process_dispatch("9")

        self.assertEqual(updates, [], "状態変更・書き込みが発生しない（下書き維持）")
        notify_admin.assert_awaited_once()
        text = notify_admin.await_args.args[0]
        self.assertIn("エラーではありません", text)
        self.assertIn("住所が未登録", text)
        self.assertIn("再保存すると自動で再処理", text)


SAMPLE_CSV = """団体コード,都道府県名（漢字）,市区町村名（漢字）,都道府県名（カナ）,市区町村名（カナ）
010006,北海道,,ホッカイドウ,
011002,北海道,札幌市,ホッカイドウ,サッポロシ
112143,埼玉県,川口市,サイタマケン,カワグチシ
112232,埼玉県,蕨市,サイタマケン,ワラビシ
"""


class TestImportScript(unittest.TestCase):
    def test_parse_skips_prefecture_rows(self):
        rows = icm.parse_rows(SAMPLE_CSV)
        self.assertEqual(len(rows), 3, "都道府県行（市区町村名なし）は除外")
        self.assertEqual(rows[1], {"団体コード": "112143", "都道府県": "埼玉県",
                                   "市区町村名": "川口市"})

    def test_validate_detects_problems(self):
        rows = icm.parse_rows(SAMPLE_CSV)
        problems = icm.validate_rows(rows)
        self.assertTrue(any("件数が想定範囲外" in p for p in problems),
                        "サンプル3件は 1,741 の想定範囲外として検出される")
        rows_bad = rows + [{"団体コード": "112143", "都道府県": "埼玉県",
                            "市区町村名": "川口市（重複）"},
                           {"団体コード": "12ab", "都道府県": "", "市区町村名": "壊れ市"}]
        problems = icm.validate_rows(rows_bad)
        self.assertTrue(any("重複" in p for p in problems))
        self.assertTrue(any("6桁数字でない" in p for p in problems))
        self.assertTrue(any("都道府県が空" in p for p in problems))

    def test_validate_ok_for_full_size(self):
        rows = [{"団体コード": f"{100000+i:06d}", "都道府県": "X県", "市区町村名": f"市{i}"}
                for i in range(1741)]
        self.assertEqual(icm.validate_rows(rows), [])

    def test_plan_insert_skips_existing(self):
        rows = icm.parse_rows(SAMPLE_CSV)
        to_insert, skipped = icm.plan_insert(rows, existing_codes={"112143"})
        self.assertEqual(skipped, 1)
        self.assertEqual([r["市区町村名"] for r in to_insert], ["札幌市", "蕨市"])

    def test_cp932_csv_is_readable(self):
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            f.write(SAMPLE_CSV.encode("cp932"))
            path = f.name
        try:
            rows = icm.parse_rows(icm.read_csv_text(path))
            self.assertEqual(len(rows), 3)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
