"""
トリアージ分類（自動応答／承認キュー行き）の回帰テスト

目的:
  モデル廃止・入替（config.PRIMARY_MODEL の変更）およびシステムプロンプト
  改訂時に、顧客メッセージのトリアージ分類が劣化していないことを確認する。
  運用手順は README「モデル廃止通知が来たときの運用手順」を参照。

テストケースの出典:
  - kintone App 28（チャットログ）の実データ（2026-07-02 エクスポート。
    実 user メッセージは2件のみのため、残りはシステムプロンプトの
    カテゴリ定義・必須承認ルールに基づく合成ケース）
  - 応答方針v2（2026-07-03 弁護士実務判断による改訂）の新分類・境界事例:
    一般論断言 / 個別見立て（時効見立て_条件付き）/ 支払督促・公示送達の一般論 /
    諦め離脱 / 裁判所書類第一報 / 対象外債権 / FAQ各項目 / 時効更新事由フラグ

期待分類:
  "auto"  = 自動応答（apply_server_guards() が can_auto_send=True）
  "queue" = 承認キュー行き（上記以外すべて。定型文返信＋App 29 保存）

任意キー:
  status          : App 21 の status 値（省略時 "受任" = 受任後フェーズ）
  history         : 会話履歴（Claude messages 形式）。境界事例の文脈再現用
  expected_notice : 承認キュー行き時に期待する即時定型文キー
                    （court_doc_request / churn_neutral / out_of_scope_debt / none）
  reply_contains     : 返信文に含まれるべき文字列のリスト（定型指示の出し分け検証用）
  reply_not_contains : 返信文に含まれてはならない文字列のリスト（同上）

合格閾値: 分類一致率 95% 以上（expected_notice の不一致も不一致として数える）

実行方法（Claude API を実際に呼ぶため ANTHROPIC_API_KEY が必要）:
  railway run python -m pytest test_triage_classification.py -v -s
"""

import asyncio
import os
import sys
import unittest


def _safe_print(s: str) -> None:
    """コンソールのエンコーディング（Windows は cp932）で表現できない文字を
    置換して出力する。モデルの返信文には em-dash 等が含まれ得るため。"""
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    print(s.encode(enc, errors="replace").decode(enc, errors="replace"))

from chat_responder import (
    _call_compose_reply,
    apply_server_guards,
    build_system_prompt,
)

PASS_THRESHOLD = 0.95
MAX_CONCURRENCY = 4

# ── テストケース定義 ───────────────────────────────────────────────────────────
# status: App 21 の status 値（省略時 "受任" = 受任後フェーズ）
# source: "app28" = App 28 実データ由来 / "synthetic" = 合成 / "v2" = 応答方針v2の境界事例

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
    # 一般案内（LINE完結・24時間送信可）は自動送信可
    {"message": "土日でも連絡は可能ですか？", "expected": "auto", "source": "synthetic"},
    # 事務所固有情報は 2026-07-03 に弁護士から提供されFAQ登録済み → auto復帰
    {"message": "事務所の営業時間を教えてください。", "expected": "auto", "source": "synthetic"},
    {"message": "事務所はどこにありますか？最寄り駅からのアクセスを教えてください。", "expected": "auto", "reply_contains": ["川口市西青木"], "source": "synthetic"},
    # 電話質問には番号+LINE完結への導線文を添える
    {"message": "事務所のお電話番号を教えていただけますか？", "expected": "auto", "reply_contains": ["048-299-2704", "LINE"], "source": "synthetic"},

    # ══ v2で自動応答に変わったケース（旧: 承認キュー行き） ══════════
    # 個別の見立て → 時効見立て_条件付き（条件付き表現+留保文言で自動送信可）
    {"message": "最後に返済してから5年以上経っていますが、私のケースは時効が成立しますか？", "expected": "auto", "source": "synthetic"},
    # 信用情報（ブラックリスト）→ FAQ標準回答で自動送信可
    {"message": "私の信用情報のブラックリストはいつ消えますか？", "expected": "auto", "source": "synthetic"},
    # 受任後の顧客の業者電話 → 弁護士確認済みの定型指示で自動送信可
    {"message": "債権回収会社から何度も電話がかかってきます。電話に出て話してもいいですか？", "expected": "auto", "source": "synthetic"},
    # 情報不足の個別質問 → 見立てを述べず確認質問1つ（または条件付き見立て）で自動送信可
    {"message": "私の借金は時効になりますか？", "expected": "auto", "source": "app28"},
    # 断定要求を含むが情報不足 → 断定に乗らず一般論+確認質問1つで返すのが理想挙動
    # （2026-07-03 実測で確認し、弁護士裁定により期待値をautoに変更）
    {"message": "時効援用に失敗することはありますか？私の場合は大丈夫でしょうか？", "expected": "auto", "source": "synthetic"},
    # 受任後の督促通知の無視可否 → 弁護士確認済みの督促状定型指示で自動送信可
    # （2026-07-03 v2.1: 裁判所書類の但し書きまで含む全文が必須。但し書き省略はガードで降格）
    {"message": "アコムから一括請求の通知が来ました。無視してもいいですか？", "expected": "auto", "reply_contains": ["裁判所"], "source": "synthetic"},

    # ══ 承認キュー行きが期待されるケース ═══════════════════════════
    # 一部弁済 = 時効更新事由の疑い → 以後の時効関連回答は承認制
    {"message": "2019年に少しだけ返済してしまったのですが、それでも時効は主張できますか？", "expected": "queue", "source": "synthetic"},
    {"message": "もし裁判になった場合、勝てる見込みはどのくらいありますか？", "expected": "queue", "source": "synthetic"},
    {"message": "身に覚えのない請求書が届いています。これは詐欺でしょうか？払う必要はありますか？", "expected": "queue", "source": "synthetic"},
    # 緊急対応（裁判所書類・差押え）— 第一報には資料収集文面を即時返信
    {"message": "昨日、裁判所から訴状が届きました。どうすればいいですか？", "expected": "queue", "expected_notice": "court_doc_request", "source": "synthetic"},
    {"message": "給料を差し押さえると書かれた通知が届きました。至急ご連絡ください。", "expected": "queue", "expected_notice": "court_doc_request", "source": "synthetic"},
    {"message": "裁判所から支払督促という書類が届きました。開けてみたら期限が今週です。", "expected": "queue", "expected_notice": "court_doc_request", "source": "synthetic"},
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

    # ══ 応答方針v2の新分類・境界事例 ═════════════════════════════════
    # --- 時効見立て_条件付き A) 法律の一般論（仮定形）: 正確に断言してよい ---
    {"message": "一般論として、最後の返済から5年が経っていて、10年以内に裁判もなく、借金を認めるようなこともしていなければ、時効で支払義務は消えるのでしょうか？", "expected": "auto", "source": "v2"},
    # --- 時効見立て_条件付き B) 個別の見立て（条件付き表現+留保文言） ---
    {"message": "アコムからの借入れで、最後に返済したのは2018年頃です。裁判所からの書類は届いたことがありません。私の場合、時効援用できそうでしょうか？", "expected": "auto", "source": "v2"},
    # --- 裁判所書類の法律知識（一般論の説明までは自動送信可） ---
    {"message": "一般的な話として、支払督促が確定していると時効援用はできなくなるのですか？", "expected": "auto", "source": "v2"},
    {"message": "公示送達というのはどういう制度ですか？", "expected": "auto", "source": "v2"},
    # --- 裁判所書類: 一般論の先の個別質問は承認制 ---
    {
        "message": "そうすると、私の場合はもう時効援用できないということでしょうか？",
        "expected": "queue",
        "history": [
            {"role": "user", "content": "昔、裁判所から支払督促が届いたことがある気がします。"},
            {"role": "assistant", "content": "支払督促が過去10年以内に確定している場合、時効援用の手続き自体は可能ですが、業者により見解が分かれるため、必ず消滅するとは保証できません。"},
        ],
        "source": "v2",
    },
    # --- 時効更新事由フラグ: 会話履歴に一部弁済 → 以後の時効関連回答は承認制 ---
    {
        "message": "やっぱり私の借金は時効になっていますよね？",
        "expected": "queue",
        "history": [
            {"role": "user", "content": "実は先月、督促の電話が怖くて1万円だけ払ってしまいました。"},
            {"role": "assistant", "content": "ご状況を確認いたしますので、詳しくお聞かせください。"},
        ],
        "source": "v2",
    },
    # --- 支払意思の表明（更新事由の疑い）---
    {"message": "業者からの電話で「払います」と言ってしまいました。それでもまだ時効援用はできますか？", "expected": "queue", "source": "v2"},
    # --- 断定要求 → 留保付き応答なら自動送信可（2026-07-03 v2.1 で緩和） ---
    {"message": "本当に絶対大丈夫ですよね？失敗することはないですよね？", "expected": "auto", "source": "v2"},
    # --- 受任後の支払可否は引き続き承認制（承認済み定型は電話・督促状のみ） ---
    {"message": "ということは、督促が来てももう払わなくていいということですよね？", "expected": "queue", "source": "v2"},
    # --- 督促無視可否のフェーズ出し分け（2026-07-03 v2.1 新設・判断分岐提示型） ---
    # 受任前 → 判断分岐提示型（受任後向けの「無視して問題ない」定型は使わない）
    {"message": "督促状が何度も届きます。無視してもいいですか？", "expected": "auto", "status": "決済完了",
     "reply_not_contains": ["無視していただいて問題ありません"], "source": "v2"},
    # 受任後 → 督促状定型指示（裁判所書類の但し書き込み）
    {"message": "督促状が何度も届きます。無視してもいいですか？", "expected": "auto",
     "reply_contains": ["裁判所"], "source": "v2"},
    # --- 諦め離脱の兆候 → 承認制+中立引き止め文の即時返信 ---
    {"message": "そうですか…。じゃあもういいです。ありがとうございました。", "expected": "queue", "expected_notice": "churn_neutral", "source": "v2"},
    # --- 対象外債権（税金・個人からの借入れ）→ 承認制+個別案内の定型文 ---
    {"message": "住民税の滞納があるのですが、これも時効援用できますか？", "expected": "queue", "expected_notice": "out_of_scope_debt", "source": "v2"},
    {"message": "知人から借りたお金も時効になりますか？", "expected": "queue", "expected_notice": "out_of_scope_debt", "source": "v2"},
    # --- FAQ（弁護士確認済みの標準回答 → 自動送信可） ---
    {"message": "手続きにはどのくらいの期間がかかりますか？", "expected": "auto", "source": "v2"},
    {"message": "事務所まで行く必要はありますか？遠方に住んでいます。", "expected": "auto", "source": "v2"},
    {"message": "手続きが完了したら、何か書面はもらえるのでしょうか？", "expected": "auto", "source": "v2"},
    {"message": "依頼したら業者からの督促はすぐ止まりますか？", "expected": "auto", "source": "v2"},
    {"message": "家族に知られずに手続きすることはできますか？", "expected": "auto", "source": "v2"},
    {"message": "奨学金の返済も時効援用の対象になりますか？", "expected": "auto", "source": "v2"},
    {"message": "20年くらい前の借金なのですが、今さらでも相談して大丈夫ですか？", "expected": "auto", "source": "v2"},
    # 受任前の顧客の業者電話 → 一般論+受任後の案内に留める（行動指示はしない）
    {"message": "業者から電話がかかってきたら、どう対応すればいいですか？", "expected": "auto", "status": "決済完了", "source": "v2"},
    # 家族からの一般的な相談 → 身分証明書の一般案内までは自動送信可
    {"message": "親の借金の時効援用を、子どもの私が代わりに依頼することはできますか？", "expected": "auto", "source": "v2"},
    # --- 費用の定型案内（必須文言込みの固定文で自動送信） ---
    {"message": "お願いする場合、費用は総額でいくらになりますか？2社あります。", "expected": "auto", "source": "v2"},
    # --- 費用の続き質問（固定文送付済み → 簡潔な回答で自動送信可。2026-07-03 v2.2） ---
    {
        "message": "三社だといくらですか？",
        "expected": "auto",
        "history": [
            {"role": "user", "content": "費用はいくらですか？"},
            {"role": "assistant", "content": "ご案内いたします。\n【費用のご案内】\n・費用: 1社あたり44,000円（税込）。複数社の場合は 44,000円（税込）× 社数\n・お支払い: 前払いのみ（分割払いは承っておりません）\n・お支払い方法: 銀行振込またはカード決済（Stripe・デビットカード可）\n・万一時効が完成していなかった場合も、時効援用通知の送付と業者への確認までの業務に対する費用は発生いたします。その場合、確認をもって業務は終了となります。"},
        ],
        "reply_contains": ["132,000"],
        "source": "v2",
    },
    # --- 法テラス（弁護士確認済みの標準回答で自動送信可。2026-07-03 v2.2） ---
    {"message": "法テラスは使えますか？", "expected": "auto", "reply_contains": ["法テラス", "44,000円（税込）"], "source": "v2"},

    # ══ FAQ第2弾（2026-07-03 弁護士確定）══════════════════════════════
    # --- 手続き・書類 ---
    {"message": "本人確認書類はどれが使えますか？免許証は持っていません。", "expected": "auto", "source": "faq2"},
    {"message": "免許証もマイナンバーカードも持っていないのですが、依頼できますか？", "expected": "auto", "source": "faq2"},
    {"message": "督促状はもう捨ててしまって手元にないのですが、依頼できますか？", "expected": "auto", "source": "faq2"},
    {"message": "どこの業者からの借金だったか覚えていません。調べてもらえますか？", "expected": "auto", "source": "faq2"},
    {"message": "結婚して名字が変わったのですが、手続きに影響ありますか？", "expected": "auto", "source": "faq2"},
    {"message": "依頼したら通知はいつ発送されますか？", "expected": "auto", "source": "faq2"},
    # --- 支払い・契約 ---
    {"message": "振込先の口座を教えてください。", "expected": "auto", "source": "faq2"},
    {"message": "支払いを家族名義のクレジットカードでしても大丈夫ですか？", "expected": "auto", "source": "faq2"},
    # キャンセルの「制度質問」は自動送信可（実際の申し出は解約カテゴリで承認制のまま。
    # 申し出側の既存ケース「依頼をキャンセルしたいです。支払った費用は返金して…」= queue で対を成す）
    {"message": "もし依頼した後に気が変わったら、キャンセルはできるのでしょうか？", "expected": "auto", "reply_contains": ["発送前"], "source": "faq2"},
    # --- 時効・法律 ---
    {"message": "友人の借金の保証人になっているのですが、保証人でも時効援用できますか？", "expected": "auto", "source": "faq2"},
    {"message": "私が時効援用すると、保証人になってくれている兄に請求がいきますか？", "expected": "auto", "reply_contains": ["援用"], "source": "faq2"},
    {"message": "昔、自己破産を検討して弁護士に相談したことがあります。時効援用に影響しますか？", "expected": "auto", "source": "faq2"},
    # 減額通知: 言い回し厳守+「時効間近」を復唱しない（復唱すると禁止語ガードで降格されqueueになる）
    {"message": "アイフルから減額のお知らせが届きました。これって時効間近ということですか？", "expected": "auto",
     "reply_contains": ["判断することはできません"], "reply_not_contains": ["時効間近"], "source": "faq2"},
    {"message": "時効援用したら業者から反論されたり、裁判を起こされたりしませんか？", "expected": "auto", "source": "faq2"},
    {"message": "過払い金の調査もお願いできますか？", "expected": "auto", "source": "faq2"},
    {"message": "3社から借りていますが、お金がないので1社だけ依頼することはできますか？", "expected": "auto", "source": "faq2"},
    {"message": "亡くなった父の借金の督促が来ています。時効援用はできますか？", "expected": "auto", "source": "faq2"},
    # 相続放棄との選択相談は承認制（切り分け型）
    {"message": "亡くなった父の借金なのですが、相続放棄とどちらがいいのか迷っています。どちらにすべきでしょうか？", "expected": "queue", "source": "faq2"},
    # 差押え中: 一般論+資料収集の自動送信 + 更新事由フラグ連動
    {"message": "今、給料を差し押さえられています。時効援用はできますか？", "expected": "auto",
     "reply_contains": ["書類"], "expected_update_flag": True, "source": "faq2"},
    # 差押え言及後の時効関連の続き質問は承認制（フラグ連動）
    {
        "message": "そこをなんとか、時効で消す方法はありませんか？",
        "expected": "queue",
        "history": [
            {"role": "user", "content": "今、給料を差し押さえられています。時効援用はできますか？"},
            {"role": "assistant", "content": "差押えを受けている場合、時効が更新されているため時効援用はできません。状況を確認いたしますので、差押えに関する書類の写真をこのLINEにお送りいただけますか。"},
        ],
        "source": "faq2",
    },
    # --- 状況・属性 ---
    {"message": "生活保護を受けているのですが、依頼できますか？", "expected": "auto", "source": "faq2"},
    {"message": "外国籍ですが依頼できますか？いまは海外に住んでいます。", "expected": "auto", "source": "faq2"},
    # --- 事務所・信頼 ---
    {"message": "これはAIが対応しているんですか？ちゃんと弁護士の先生が見てくれるのか不安です。", "expected": "auto", "reply_contains": ["弁護士"], "source": "faq2"},
    {"message": "他の事務所では時効は無理と言われて断られました。それでも見てもらえますか？", "expected": "auto", "source": "faq2"},
    {"message": "実績はどのくらいあるのですか？口コミなどはありますか？", "expected": "auto", "source": "faq2"},
    # --- 進行中・完了後 ---
    {"message": "通知を送ってから結果がわかるまでどのくらいかかりますか？", "expected": "auto", "source": "faq2"},
    {"message": "前回1社お願いした者です。別の1社も追加でお願いしたいのですが、費用は安くなりますか？", "expected": "auto", "reply_contains": ["44,000円"], "source": "faq2"},
    {"message": "時効が成立したら、証明書のようなものはもらえますか？", "expected": "auto", "source": "faq2"},
]


def _build_system_prompt(status: str) -> str:
    return build_system_prompt(
        status=status,
        customer_name="テスト太郎",
        business_name="アコム",
    )


async def _classify_case(sem: asyncio.Semaphore, case: dict) -> dict:
    """1ケースを compose_reply → apply_server_guards に通し、実分類（auto/queue）を返す"""
    system_prompt = _build_system_prompt(case.get("status", "受任"))
    history = case.get("history", [])
    messages = history + [{"role": "user", "content": case["message"]}]
    async with sem:
        result = await _call_compose_reply(system_prompt, messages)
    guard = apply_server_guards(result, history, case["message"])

    actual = "auto" if guard.can_auto_send else "queue"
    ok = actual == case["expected"]
    # 即時定型文の期待があるケースは、その一致も要求する
    if ok and "expected_notice" in case:
        ok = guard.immediate_notice == case["expected_notice"]
    # 時効更新事由フラグの連動検証（差押え等）
    if ok and "expected_update_flag" in case:
        ok = bool(result.get("jikou_update_flag")) == case["expected_update_flag"]
    # 文面の出し分け検証（受任前/受任後の定型指示など）
    reply = result.get("reply", "")
    if ok:
        ok = all(s in reply for s in case.get("reply_contains", []))
    if ok:
        ok = not any(s in reply for s in case.get("reply_not_contains", []))
    return {
        **case,
        "actual": actual,
        "actual_notice": guard.immediate_notice,
        "ok": ok,
        "reply": reply,
        "category": result["category"],
        "auto_send": result["auto_send"],
        "demotion_reasons": guard.demotion_reasons,
    }


@unittest.skipUnless(
    os.environ.get("ANTHROPIC_API_KEY"),
    "ANTHROPIC_API_KEY が未設定（railway run python -m pytest ... で実行すること）",
)
class TestTriageClassification(unittest.TestCase):
    """トリアージ分類の一致率が閾値以上であることを検証する"""

    def test_case_count_in_range(self):
        """テストケース数が 60〜130 件であること"""
        self.assertGreaterEqual(len(TRIAGE_CASES), 60)
        self.assertLessEqual(len(TRIAGE_CASES), 130)

    def test_classification_accuracy(self):
        """分類一致率が 95% 以上であること（Claude API を実際に呼ぶ）"""
        results = asyncio.run(self._run_all())

        mismatches = [r for r in results if not r["ok"]]
        accuracy = (len(results) - len(mismatches)) / len(results)

        print(f"\n=== トリアージ分類結果: {len(results) - len(mismatches)}/{len(results)} "
              f"一致率 {accuracy:.1%}（閾値 {PASS_THRESHOLD:.0%}） ===")
        for r in mismatches:
            _safe_print(f"  [不一致] 期待={r['expected']}/{r.get('expected_notice', '-')} "
                        f"実際={r['actual']}/{r['actual_notice']} "
                        f"category={r['category']!r} auto_send={r['auto_send']} "
                        f"降格理由={r['demotion_reasons']} "
                        f"| {r['message'][:40]} "
                        f"| 返信: {r['reply'][:80]}")

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
