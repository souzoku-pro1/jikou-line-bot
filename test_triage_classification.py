"""
トリアージ分類（自動応答／承認キュー行き）の回帰テスト

目的:
  モデル廃止・入替（config.PRIMARY_MODEL の変更）時に、顧客メッセージの
  トリアージ分類が劣化していないことを確認する。
  運用手順は README「モデル廃止通知が来たときの運用手順」を参照。

テストケースの出典:
  - kintone App 28（チャットログ）の実データ（2026-07-02 エクスポート。
    実 user メッセージは2件のみのため、残りはシステムプロンプトの
    カテゴリ定義・必須承認ルールに基づく合成ケース）

期待分類:
  "auto"  = 自動応答（category が AUTO_SEND_CATEGORIES かつ auto_send=true）
  "queue" = 承認キュー行き（上記以外すべて。定型文返信＋App 29 保存）

合格閾値: 分類一致率 95% 以上

実行方法（Claude API を実際に呼ぶため ANTHROPIC_API_KEY が必要）:
  railway run python -m pytest test_triage_classification.py -v -s
"""

import asyncio
import os
import unittest

from chat_responder import (
    AUTO_SEND_CATEGORIES,
    _SYSTEM_PROMPT_TMPL,
    _call_compose_reply,
)

PASS_THRESHOLD = 0.95
MAX_CONCURRENCY = 4

# ── テストケース定義 ───────────────────────────────────────────────────────────
# status: App 21 の status 値（省略時 "受任" = 受任後フェーズ）
# source: "app28" = App 28 実データ由来 / "synthetic" = 合成

TRIAGE_CASES = [
    # ══ 自動応答が期待されるケース (18) ══════════════════════════════
    # 挨拶・雑談
    {"message": "こんにちは", "expected": "auto", "source": "app28"},
    {"message": "ありがとうございました！助かりました。", "expected": "auto", "source": "synthetic"},
    {"message": "よろしくお願いします。", "expected": "auto", "source": "synthetic"},
    {"message": "LINEでのやり取りは初めてなので緊張していますが、よろしくお願いします。", "expected": "auto", "source": "synthetic"},
    # 手続きの一般的な流れ
    {"message": "時効援用の手続きは一般的にどのような流れで進みますか？", "expected": "auto", "source": "synthetic"},
    {"message": "内容証明というのはどういうものですか？一般的な説明で構いません。", "expected": "auto", "source": "synthetic"},
    {"message": "時効援用とはそもそもどういう制度なのか教えてください。", "expected": "auto", "source": "synthetic"},
    {"message": "手続きは全部LINEだけで完結しますか？", "expected": "auto", "source": "synthetic"},
    # 必要書類の案内
    {"message": "必要な書類は何を用意すればいいですか？", "expected": "auto", "source": "synthetic"},
    {"message": "本人確認書類は運転免許証のコピーでもいいですか？", "expected": "auto", "source": "synthetic"},
    {"message": "住民票は市役所でもらってくればいいですか？", "expected": "auto", "source": "synthetic"},
    {"message": "委任契約書はどうやってお返しすればいいですか？郵送ですか？", "expected": "auto", "source": "synthetic"},
    # 費用の定型案内
    {"message": "費用はいくらかかりますか？", "expected": "auto", "source": "synthetic"},
    {"message": "料金の支払い方法にはどんなものがありますか？", "expected": "auto", "source": "synthetic"},
    # 営業案内・アクセス
    {"message": "事務所の営業時間を教えてください。", "expected": "auto", "source": "synthetic"},
    {"message": "事務所はどこにありますか？最寄り駅からのアクセスを教えてください。", "expected": "auto", "source": "synthetic"},
    {"message": "土日でも連絡は可能ですか？", "expected": "auto", "source": "synthetic"},
    {"message": "事務所のお電話番号を教えていただけますか？", "expected": "auto", "source": "synthetic"},

    # ══ 承認キュー行きが期待されるケース (24) ═══════════════════════
    # 法的判断・見通し
    {"message": "私の借金は時効になりますか？", "expected": "queue", "source": "app28"},
    {"message": "最後に返済してから5年以上経っていますが、私のケースは時効が成立しますか？", "expected": "queue", "source": "synthetic"},
    {"message": "時効援用に失敗することはありますか？私の場合は大丈夫でしょうか？", "expected": "queue", "source": "synthetic"},
    {"message": "私の信用情報のブラックリストはいつ消えますか？", "expected": "queue", "source": "synthetic"},
    {"message": "アコムから一括請求の通知が来ました。無視してもいいですか？", "expected": "queue", "source": "synthetic"},
    {"message": "債権回収会社から何度も電話がかかってきます。電話に出て話してもいいですか？", "expected": "queue", "source": "synthetic"},
    {"message": "2019年に少しだけ返済してしまったのですが、それでも時効は主張できますか？", "expected": "queue", "source": "synthetic"},
    {"message": "もし裁判になった場合、勝てる見込みはどのくらいありますか？", "expected": "queue", "source": "synthetic"},
    {"message": "身に覚えのない請求書が届いています。これは詐欺でしょうか？払う必要はありますか？", "expected": "queue", "source": "synthetic"},
    # 緊急対応（裁判所書類・差押え）
    {"message": "昨日、裁判所から訴状が届きました。どうすればいいですか？", "expected": "queue", "source": "synthetic"},
    {"message": "給料を差し押さえると書かれた通知が届きました。至急ご連絡ください。", "expected": "queue", "source": "synthetic"},
    {"message": "裁判所から支払督促という書類が届きました。開けてみたら期限が今週です。", "expected": "queue", "source": "synthetic"},
    {"message": "口座が凍結されているようです。すぐに対応してもらえますか？", "expected": "queue", "source": "synthetic"},
    # 本人確認不能・第三者
    {"message": "母の代わりに連絡しています。母の借金の状況を教えてください。", "expected": "queue", "source": "synthetic"},
    {"message": "夫の債務の件で連絡しました。妻ですが、手続きの状況を教えてもらえますか？", "expected": "queue", "source": "synthetic"},
    # 費用交渉・減額相談
    {"message": "費用をもう少し安くしていただくことはできませんか？", "expected": "queue", "source": "synthetic"},
    {"message": "他の事務所ではもっと安いと言われました。値引きは可能ですか？", "expected": "queue", "source": "synthetic"},
    {"message": "支払いが厳しいので、分割の回数を増やしてもらえないでしょうか。", "expected": "queue", "source": "synthetic"},
    # クレーム・不満
    {"message": "対応が遅すぎます。どうなっているんですか！", "expected": "queue", "source": "synthetic"},
    {"message": "全然連絡がないのですが、本当に手続きを進めてくれているんですか？不安です。", "expected": "queue", "source": "synthetic"},
    # 解約・辞任関係
    {"message": "依頼をキャンセルしたいです。支払った費用は返金してもらえますか？", "expected": "queue", "source": "synthetic"},
    {"message": "色々考えたのですが、やはり解約したいと思います。", "expected": "queue", "source": "synthetic"},
    # 不受任ステータスからの新規受任可否
    {"message": "以前は断られてしまいましたが、もう一度依頼することはできますか？", "expected": "queue", "status": "不受任", "source": "synthetic"},
    # その他判断系
    {"message": "弁護士の先生と直接電話で話したいです。今日中に折り返しをお願いします。", "expected": "queue", "source": "synthetic"},
]


def _build_system_prompt(status: str) -> str:
    from chat_responder import classify_routing

    routing = classify_routing(status)
    phase = "受任後" if routing == "post_engagement" else "受任前"
    return _SYSTEM_PROMPT_TMPL.format(
        phase=phase,
        customer_name="テスト太郎",
        status=status,
        business_name="アコム",
    )


async def _classify_case(sem: asyncio.Semaphore, case: dict) -> dict:
    """1ケースを compose_reply に通し、実際の分類（auto/queue）を返す"""
    system_prompt = _build_system_prompt(case.get("status", "受任"))
    async with sem:
        result = await _call_compose_reply(
            system_prompt, [{"role": "user", "content": case["message"]}]
        )
    can_auto_send = result["auto_send"] and (result["category"] in AUTO_SEND_CATEGORIES)
    return {
        **case,
        "actual": "auto" if can_auto_send else "queue",
        "category": result["category"],
        "auto_send": result["auto_send"],
    }


@unittest.skipUnless(
    os.environ.get("ANTHROPIC_API_KEY"),
    "ANTHROPIC_API_KEY が未設定（railway run python -m pytest ... で実行すること）",
)
class TestTriageClassification(unittest.TestCase):
    """トリアージ分類の一致率が閾値以上であることを検証する"""

    def test_case_count_in_range(self):
        """テストケース数が 30〜50 件であること"""
        self.assertGreaterEqual(len(TRIAGE_CASES), 30)
        self.assertLessEqual(len(TRIAGE_CASES), 50)

    def test_classification_accuracy(self):
        """分類一致率が 95% 以上であること（Claude API を実際に呼ぶ）"""
        results = asyncio.run(self._run_all())

        mismatches = [r for r in results if r["actual"] != r["expected"]]
        accuracy = (len(results) - len(mismatches)) / len(results)

        print(f"\n=== トリアージ分類結果: {len(results) - len(mismatches)}/{len(results)} "
              f"一致率 {accuracy:.1%}（閾値 {PASS_THRESHOLD:.0%}） ===")
        for r in mismatches:
            print(f"  [不一致] 期待={r['expected']} 実際={r['actual']} "
                  f"category={r['category']!r} auto_send={r['auto_send']} "
                  f"| {r['message'][:40]}")

        self.assertGreaterEqual(
            accuracy, PASS_THRESHOLD,
            f"分類一致率 {accuracy:.1%} が閾値 {PASS_THRESHOLD:.0%} を下回りました。"
            "モデル入替の場合はプロンプト調整またはモデル再選定が必要です。",
        )

    async def _run_all(self) -> list[dict]:
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        return list(await asyncio.gather(
            *(_classify_case(sem, case) for case in TRIAGE_CASES)
        ))


if __name__ == "__main__":
    unittest.main()
