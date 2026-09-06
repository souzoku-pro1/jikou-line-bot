"""相続放棄案件（App 40）アクセス層 — SOUZOKU-HOUKI-H3

houki_bot（AST checker で外部作用を閉集合化）から kintone を直接触らせず、
App 40 への読み書きを本 module の高位関数に集約する（許可名の閉集合を
test_houki_bot_policy が pin する）。

設計（正本 10-unit-02 §2.1・§6.1/6.2、souzoku-houki/02 §1-2〔有効部分〕）:
- record_hearing の fields を**逐次 upsert**（途中離脱してもデータが残る・
  in-memory 消失の影響を受けない）。
- upsert は「空値で非空を上書きしない」（聞き直し・言い直しで確定値を消さない）。
- 弁護士専権フィールド（起算日_確定・起算点確定済・受任判断・電話要否 等）と
  サーバ計算フィールド（法定満了日・社内締切日 等）は**書き込み許可集合に
  含めない**（構造的に書けない・test で pin）。
- 日付整合検証（正本 §2.1・02 §6）: 矛盾した日付フィールドは書かずに理由を
  返す（Bot が聞き直す）。2 回失敗で危険類型フラグ「申告内容の矛盾」
  （App 40 の CHECK_BOX 実選択肢値・H0-APP-2）を立てる。
- status 遷移の入口（正本 §1 のB案・票指定）: ヒアリング必須項目の充足時に
  「問い合わせ」（または空）→「電話判断待ち」への**一方向遷移のみ**。
  電話推奨度判定・通知は H-4 スコープ。
"""

import datetime
import logging
import re
from typing import Callable

from hub import kintone
from hub import notify
from hub.redact import emit

logger = logging.getLogger("hub.houki_case_store")

# houki_bot（AST checker で hub.kintone 直 import 禁止）向けの例外 re-export
KintoneError = kintone.KintoneError

# fix2: read-modify-write の CAS 収束再試行の上限（超過=収束不能・上書きしない）
_CAS_RETRIES = 3

APP_HOUKI_CASE = kintone.KintoneApp(
    "App 40 (相続放棄案件)", "APP_HOUKI", "TOKEN_HOUKI")

# ── record_hearing が書き込める App 40 フィールドの閉集合 ──────────────────────
# （H0-APP-2 の実フィールドコード。弁護士専権・サーバ計算欄は含めない）
HEARING_WRITABLE_FIELDS: frozenset = frozenset({
    # 申述人（正本 §2.1 phase7 + 様式必須のふりがな）
    # HEARING-FIX1 止血: 「職業」は App 40 に実欄が存在しない（form fields
    # API 実測 87 欄に不在）ため除外——含めたまま書くと create/update 全体が
    # kintone に拒否され会話が全断していた。復帰条件: 大野が CU で職業欄を
    # 追加したら小票で本集合へ復帰し、H7C 申述書の {{職業欄}} マッピングにも
    # 接続する（H7C 完了報告の不足フィールド一覧参照）
    "顧客名", "furigana", "生年月日", "住所", "電話番号", "メールアドレス",
    "本人区分", "未成年後見関与",
    # 被相続人（phase1）
    "被相続人氏名", "被相続人ふりがな", "被相続人本籍", "被相続人最後の住所",
    "続柄", "続柄その他",
    # 日付（phase2・申告値のみ。確定は弁護士）
    "死亡日_申告", "死亡を知った日_申告", "相続人と知った日_申告",
    "日付申告メモ", "知った経緯",
    # 債務・財産（phase3/4。App 40 は財産 4 欄形＝H0-APP-2 採用裁定）
    "財産_不動産", "財産_現金預貯金", "財産_有価証券", "財産_負債",
    "財産処分有無", "訴訟督促有無",
    # 相続関係（phase5）
    "相続順位", "先順位相続人の状況", "他の相続人", "同時申述希望",
    "先順位者の放棄状況",
})

# ── HEARING-FIX1: DROP_DOWN 閉集合（App 40 form fields API 実測の逐語 pin） ──
# 書込対象の選択式フィールド。選択肢外値は kintone が update/create ごと
# 拒否する（実障害: 続柄=自由値で会話全断・2026-08-31）ため、サーバ側で
# 事前検証し write 0+聞き直し（日付整合検証と同型・fail-open）にする
HEARING_CHOICE_FIELDS: dict = {
    "続柄": ("子", "孫", "配偶者", "直系尊属（父母・祖父母）", "兄弟姉妹",
             "おいめい", "その他"),
    "本人区分": ("本人", "親族（本人依頼予定）", "その他"),
    "未成年後見関与": ("なし", "あり", "不明"),
    "財産処分有無": ("なし", "あり", "不明"),
    "訴訟督促有無": ("なし", "あり", "不明"),
    "相続順位": ("配偶者", "子", "直系尊属", "兄弟姉妹", "甥姪（代襲）",
                 "不明"),
    "同時申述希望": ("なし", "あり"),
}

# 書込対象外だが App 40 実測を pin（H7C 申述書マッピングの前提値・将来
# ヒアリング書込対象化する票はここから HEARING_CHOICE_FIELDS へ昇格させる）
APP40_CHOICE_REFERENCE: dict = {
    "知った日の区分": ("被相続人死亡の当日", "死亡の通知をうけた日",
                       "先順位者の相続放棄を知った日", "その他"),
    "放棄の理由": ("被相続人から生前に贈与を受けている。",
                   "生活が安定している。", "遺産が少ない。",
                   "遺産を分散させたくない。", "債務超過のため。", "その他"),
}

# 日付フィールド（YYYY-MM-DD のみ upsert・曖昧値は 日付申告メモ に残す運用）
_DATE_FIELDS = ("死亡日_申告", "死亡を知った日_申告", "相続人と知った日_申告")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# ヒアリング必須項目（正本 §2.1 の各フェーズ最重要項目。充足+hearing_done で
# status を 電話判断待ち へ進める）
HEARING_REQUIRED_FIELDS: tuple = (
    "被相続人氏名", "続柄",
    "死亡日_申告", "死亡を知った日_申告", "相続人と知った日_申告",
    "相続順位",
    "顧客名", "住所", "生年月日", "電話番号",
)

# ── HOUKI-HEARING-UX-1: ヒアリング台本の構造（7 通・弁護士決定・凍結） ──────────
# 各通 = (見出し, 導入文, 項目タプル, 記録メモ)。項目 = (質問ラベル, 対応欄)。
# 質問ラベルは「誰の・何を」を必ず明示する。対応欄は「その項目が回答済みか」を
# レコード実値で判定するための欄（いずれか非空=回答済み）。空タプル=記録欄なし
# （第 5 通・戸籍。会話で確認するのみ）。項目の追加・削減は本票の凍結範囲外
HEARING_ROUNDS: tuple = (
    ("亡くなった方について", "亡くなった方についてお伺いします。", (
        ("亡くなった方のお名前とふりがな", ("被相続人氏名", "被相続人ふりがな")),
        ("亡くなった方とあなたとのご関係（母・父・兄・叔父など）", ("続柄",)),
        ("亡くなった方の最後のお住まい（市区町村まででも構いません）",
         ("被相続人最後の住所",)),
        ("亡くなった方の本籍（分からなければ「不明」で構いません）",
         ("被相続人本籍",)),
    ), "記録: 被相続人氏名／被相続人ふりがな／続柄（選択肢へ読み替え・その他は 続柄その他 に"
       "具体的内容）／被相続人最後の住所／被相続人本籍。続柄から 相続順位 も記録する"
       "（子→子・配偶者→配偶者・父母や祖父母→直系尊属・兄弟姉妹→兄弟姉妹・"
       "おいめい→甥姪（代襲）・判断できなければ 不明）"),
    ("日付について", "日付についてお伺いします。正確でなくても「◯年◯月頃」で構いません。", (
        ("亡くなった方が亡くなった日", ("死亡日_申告",)),
        ("あなたが、亡くなったことを知った日", ("死亡を知った日_申告",)),
        ("あなたが、ご自身は相続人だと知った日", ("相続人と知った日_申告",)),
        ("亡くなったこと（相続人であること）を知った経緯"
         "（役所からの通知・債権者からの請求・親族からの連絡など）", ("知った経緯",)),
    ), "記録: 死亡日_申告／死亡を知った日_申告／相続人と知った日_申告（YYYY-MM-DD が"
       "確定した場合のみ。曖昧な答えは 日付申告メモ に原文のまま）／知った経緯"),
    ("借金と財産について", "亡くなった方の借金と財産についてお伺いします。", (
        ("亡くなった方に借金や未払いはありますか。督促状や裁判所からの書類が"
         "届いていれば、その内容も教えてください", ("財産_負債", "訴訟督促有無")),
        ("亡くなった方の不動産以外の財産（預貯金・車・株など）で、分かっている"
         "ものはありますか（無ければ「なし」で構いません）",
         ("財産_現金預貯金", "財産_有価証券")),
        # HOUKI-HEARING-UX-1-fix1: 弁護士確認による文言差し替え（記録先は不変）
        ("亡くなった方の預貯金を死亡後に出金して使用したり、価値のある財産を"
         "処分したりしたことはありますか",
         ("財産処分有無",)),
    ), "記録: 財産_負債（借金・未払いの概要。無ければ なし）／訴訟督促有無（督促状・"
       "訴状などが届いていれば あり・無ければ なし・曖昧なら 不明）／債権者名は "
       "creditor_names／財産_現金預貯金・財産_有価証券（判明した範囲で短く。無ければ "
       "なし）／財産処分有無（なし/あり/不明）"),
    ("他の相続人について", "他の相続人についてお伺いします。", (
        ("あなた以外に相続人にあたる方（亡くなった方の配偶者・お子さん・親・"
         "兄弟姉妹など）はいらっしゃいますか。いれば続柄と人数を教えてください",
         ("他の相続人",)),
        ("その方々と一緒に相続放棄をしたいご希望はありますか", ("同時申述希望",)),
    ), "記録: 他の相続人（いなければ なし）／同時申述希望（なし/あり）。先順位の方が"
       "放棄済みと分かれば 先順位相続人の状況・先順位者の放棄状況 にも記録"),
    ("戸籍について", "戸籍についてお伺いします。", (
        # HOUKI-HEARING-UX-1-fix1: 弁護士確認による文言差し替え（「職務上請求」の
        # 語は顧客向け文面から除去）
        ("亡くなった方の戸籍謄本や住民票（除票）を、すでに取得されていますか。"
         "それともこれから取得のご予定ですか（お手元になくても、事務所で戸籍"
         "謄本等の必要書類を取得可能です）", ()),
    ), "記録欄なし（会話で確認するのみ。回答後は次の通へ進む）"),
    ("ご依頼者ご自身について", "ご依頼者様ご自身についてお伺いします。", (
        ("ご依頼者様ご自身のお名前とふりがな", ("顧客名", "furigana")),
        ("ご依頼者様ご自身のご住所", ("住所",)),
        ("ご依頼者様ご自身の生年月日", ("生年月日",)),
        ("ご依頼者様ご自身のお電話番号", ("電話番号",)),
        ("ご依頼者様ご自身のメールアドレス", ("メールアドレス",)),
    ), "記録: 顧客名／furigana／住所／生年月日／電話番号／メールアドレス"),
    ("ご相談の区分について", "最後に確認です。", (
        ("今回は、ご依頼者様ご本人としてのご依頼でしょうか。それとも、ご親族などの"
         "代理としてのご相談でしょうか", ("本人区分",)),
    ), "記録: 本人区分（本人／親族（本人依頼予定）／その他）"),
)

# 法律質問等への固定の受け流し文（弁護士決定・凍結。法的説明は送らない）
DEFLECT_REPLY = ("その点は弁護士が確認のうえご案内します。ヒアリング終了後に"
                 "お伝えできますので、引き続きよろしくお願いいたします。")

# 空応答の判定: 締め・相づちの定型句だけで構成された返信（閉集合）
HOLLOW_FORMULAS: tuple = (
    "よろしくお願い致します", "よろしくお願いいたします",
    "ありがとうございます", "ありがとうございました",
    "承知いたしました", "承知しました", "かしこまりました", "了解しました",
    "記録しました", "はい",
)
_HOLLOW_STRIP = "。、．，！!？?・…　 \n\t\r"
_CIRCLED = "①②③④⑤⑥⑦⑧⑨"


def is_hollow_reply(text: str) -> bool:
    """返信が空、または HOLLOW_FORMULAS の定型句と句読点だけなら True。"""
    t = str(text or "")
    for phrase in HOLLOW_FORMULAS:
        t = t.replace(phrase, "")
    return not t.strip(_HOLLOW_STRIP)


def round_body(intro: str, items: tuple) -> str:
    """定型ブロックの本文（導入文+番号付き項目ラベル）。houki_profile の罫線
    ブロックはこの本文を罫線と定型末尾で挟むだけ（項目行の逐語は単一の正）。"""
    return "\n".join([intro] + [f"{_CIRCLED[i]}{label}"
                                for i, (label, _f) in enumerate(items)])


def _asked_and_answered(label: str, history: list | None) -> bool:
    """HOUKI-HEARING-UX-1-fix2（UX1-01）: 記録欄のない項目の完了判定。
    「その項目ラベルを逐語で含む assistant 発話（定型ブロック／fallback の
    再提示。閉集合=HEARING_ROUNDS のラベル）が送信済み、かつ その後に
    お客様（user）の発話がある」とき True。正本は会話履歴（in-memory と
    App 28 復元で同じ role/content 文字列の形）＝再起動後も同じ結果。
    tool の phase/phase_done は使わない（安全側=未実施扱いが優先）。"""
    asked = False
    for msg in history or ():
        content = msg.get("content")
        if not isinstance(content, str):
            continue        # tool_use/tool_result 等の block 列は判定に使わない
        role = msg.get("role")
        if role == "assistant" and label in content:
            asked = True
        elif role == "user" and asked:
            return True
    return False


def unanswered_items(record: dict | None,
                     history: list | None = None) -> tuple[int, str, list[str]]:
    """最初の未完了の通を返す: (通番号 1-7, 見出し, 未回答項目ラベル)。
    記録欄のある項目=欄の充足（現行どおり）。記録欄のない項目（第 5 通・将来
    同型の項目も同じ規則）=会話履歴で「提示済み かつ その後にお客様の発話あり」
    （_asked_and_answered）。全通完了なら (0, "", [])。
    fallback_reply（再提示）と進行状況の注入（houki_bot.hearing）が共にこの
    関数を使う（完了判定の二重管理をしない）。"""
    for i, (title, _intro, items, _note) in enumerate(HEARING_ROUNDS, start=1):
        missing = [
            label for label, fields in items
            if (not any(_v(record or {}, f) for f in fields) if fields
                else not _asked_and_answered(label, history))]
        if missing:
            return i, title, missing
    return 0, "", []


def fallback_reply(record: dict | None, all_done_text: str,
                   history: list | None = None) -> str:
    """空応答の差し替え文: 受け流し文+現在の通の未回答項目の再提示（疑問符なし・
    定型ブロックは再送しない）。全通完了なら all_done_text（確認中定型）。"""
    n, title, missing = unanswered_items(record, history)
    if not missing:
        return all_done_text
    lines = [f"{_CIRCLED[i]}{label}" for i, label in enumerate(missing)]
    return (DEFLECT_REPLY + "\n\n改めて、" + title + "、次の点をお伺いします。\n"
            + "\n".join(lines) + "\n分かる範囲でお答えください。")


def progress_note(record: dict | None, history: list | None = None) -> str:
    """進行状況の注入文（system prompt の末尾に動的付加。凍結 prompt 本文
    HOUKI_HEARING_PROMPT には含めない）。次に進める通と未回答項目をサーバ
    判定（unanswered_items）から示し、モデル側の進行判定を同じ正に揃える。"""
    n, title, missing = unanswered_items(record, history)
    if not missing:
        return ("\n\n【進行状況（サーバ判定）】\n全7通の項目が揃っています。"
                "新たな質問は不要です。")
    lines = [f"{_CIRCLED[i]}{label}" for i, label in enumerate(missing)]
    return ("\n\n【進行状況（サーバ判定）】\n次に進める通: 第" + str(n) + "通（"
            + title + "）。この通の未回答項目:\n" + "\n".join(lines)
            + "\n上記より先の通には進まないでください。")


STATUS_FIELD = "status"
STATUS_INQUIRY = "問い合わせ"
STATUS_PHONE_TRIAGE = "電話判断待ち"
# 危険類型フラグ（CHECK_BOX）の実選択肢値（H0-APP-2・正本 §3.1 逐語）。
# 旧 02 §6 の「日付不整合」・正本 §2.1 の「申告矛盾」は本値に正規化される
KIKEN_FLAG_FIELD = "危険類型フラグ"
KIKEN_FLAG_DATE_MISMATCH = "申告内容の矛盾"

CREDITOR_TABLE = "債権者一覧"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


async def fetch_case(user_id: str) -> dict | None:
    """LINEユーザーID で App 40 の案件を検索（なければ None・最新 1 件）。"""
    rows = await kintone.search_records(
        APP_HOUKI_CASE,
        f'LINEユーザーID = "{user_id}" order by $id desc limit 1')
    return rows[0] if rows else None


def validate_hearing_dates(fields: dict,
                           today: datetime.date | None = None) -> list[str]:
    """日付整合検証（正本 §2.1・02 §6「知った日 < 死亡日 等」）。

    返り値=矛盾理由の一覧（固定語彙・空=適合）。検証は与えられた値のみで行う
    （欠けている側は判定しない・fail-open で会話を止めない）:
      (i)   死亡を知った日_申告 < 死亡日_申告
      (ii)  相続人と知った日_申告 < 死亡日_申告
      (iii) 相続人と知った日_申告 < 死亡を知った日_申告
      (iv)  いずれかが未来日
      (v)   YYYY-MM-DD 形式でない（曖昧値は 日付申告メモ へ・_申告欄には
            確定形式のみ書く）
    """
    problems: list[str] = []
    parsed: dict[str, datetime.date] = {}
    for code in _DATE_FIELDS:
        raw = str(fields.get(code) or "").strip()
        if not raw:
            continue
        if not _DATE_RE.fullmatch(raw):
            problems.append(f"{code}=形式不正")
            continue
        try:
            parsed[code] = datetime.date.fromisoformat(raw)
        except ValueError:
            problems.append(f"{code}=形式不正")
    today = today or datetime.date.today()
    for code, d in parsed.items():
        if d > today:
            problems.append(f"{code}=未来日")
    death = parsed.get("死亡日_申告")
    knew_death = parsed.get("死亡を知った日_申告")
    knew_heir = parsed.get("相続人と知った日_申告")
    if death and knew_death and knew_death < death:
        problems.append("死亡を知った日_申告が死亡日_申告より前")
    if death and knew_heir and knew_heir < death:
        problems.append("相続人と知った日_申告が死亡日_申告より前")
    if knew_death and knew_heir and knew_heir < knew_death:
        problems.append("相続人と知った日_申告が死亡を知った日_申告より前")
    return problems


def split_valid_fields(fields: dict, existing: dict | None = None,
                       today: datetime.date | None = None
                       ) -> tuple[dict, list[str], list[str]]:
    """tool の fields を（書き込み可能な適合分, 日付矛盾理由, 選択肢外理由）へ
    分ける。

    - 許可集合外のフィールド名は黙って落とす（弁護士専権・サーバ計算欄の防壁）
    - 空値は落とす（非空を空で上書きしない）
    - fix1[01]: 日付整合は「**既存レコードの日付 3 欄+今回入力を合成した
      postimage 候補**」に対して検証する（一方が既存値・他方が今回値の
      cross-turn の組合せにも 3 順序規則を適用）。今回入力に日付が 1 つも
      無ければ検証しない（保存済みの確定値は書込時に検証済み）
    - 矛盾があれば**今回の日付 3 欄をすべて**書き込み対象から外す（write 0。
      部分書込で矛盾ペアの片側だけ残る事故を防ぐ）。他フィールドは書く
    - HEARING-FIX1: DROP_DOWN 閉集合（HEARING_CHOICE_FIELDS）の選択肢外値は
      当該フィールドのみ write 0 とし、固定語彙
      「<code>=選択肢外（値1/値2/…）」を第 3 戻り値で返す（tool_result で
      モデルに聞き直させる・日付整合と同型の fail-open。**日付矛盾の系
      〔メモのマーカー・危険類型フラグ〕には接続しない**——申告の矛盾では
      なく表現の言い換えが必要なだけのため）。他フィールドは書く
    """
    incoming_dates = [c for c in _DATE_FIELDS
                     if str((fields or {}).get(c) or "").strip()]
    if incoming_dates:
        merged: dict = {}
        for code in _DATE_FIELDS:
            raw_in = str((fields or {}).get(code) or "").strip()
            if raw_in:
                merged[code] = raw_in       # 形式不正は validate 側で検知
                continue
            raw_ex = str(((existing or {}).get(code) or {})
                         .get("value") or "").strip()
            if raw_ex and _DATE_RE.fullmatch(raw_ex):
                merged[code] = raw_ex       # 既存確定値（postimage 候補）
        problems = validate_hearing_dates(merged, today=today)
    else:
        problems = []
    out: dict = {}
    choice_problems: list[str] = []
    for code, value in (fields or {}).items():
        if code not in HEARING_WRITABLE_FIELDS:
            continue
        sval = str(value or "").strip()
        if not sval:
            continue
        if problems and code in _DATE_FIELDS:
            continue
        allowed = HEARING_CHOICE_FIELDS.get(code)
        if allowed is not None and sval not in allowed:
            choice_problems.append(
                f"{code}=選択肢外（{'/'.join(allowed)}）")
            continue
        out[code] = sval
    return out, problems, choice_problems


async def upsert_case_fields(user_id: str, fields: dict,
                             existing: dict | None) -> str:
    """適合済み fields を App 40 へ upsert し、レコード ID を返す。

    - 新規: 受付チャネル=LINE・status=問い合わせ で作成
    - 既存: **空でない現値は上書きしない**（record_hearing は追記専用）
    - fix2[H3-04]: 更新は existing の $revision で CAS。409（KintoneConflict）
      は**送出**する（収束＝再取得・再検証・再試行は apply_hearing_fields が
      担う。単発の低レベル書込としては作用 0）
    """
    if existing is None:
        # HOUKI-STORE-FIX1: hub.kintone へは plain 値（_wrap が包む契約）
        payload = dict(fields)
        payload["LINEユーザーID"] = user_id
        payload["受付チャネル"] = "LINE"
        payload[STATUS_FIELD] = STATUS_INQUIRY
        rid = await kintone.create_record(APP_HOUKI_CASE, payload)
        logger.info("[HOUKI_CASE] created record_id=%s",
                    emit(rid, "record_id", "log", "operator"))
        return str(rid)
    rid = _v(existing, "$id")
    update = {code: v for code, v in fields.items()
              if not _v(existing, code)}
    if update:
        await kintone.update_record(APP_HOUKI_CASE, rid, update,
                                    revision=_v(existing, "$revision") or None)
        logger.info("[HOUKI_CASE] updated record_id=%s fields=%s",
                    emit(rid, "record_id", "log", "operator"),
                    emit(len(update), "count", "log", "operator"))
    return rid


async def apply_hearing_fields(user_id: str, raw_fields: dict,
                               existing: dict | None,
                               fence: Callable[[], bool] | None = None
                               ) -> tuple[str, list[str], list[str]]:
    """record_hearing の生 fields を（検証→CAS upsert→409 収束）まで行う
    （fix2[H3-04]）。(レコード ID, 日付矛盾理由一覧, 選択肢外理由一覧) を返す
    （HEARING-FIX1: 選択肢外は日付矛盾の系と別チャネル）。

    収束: CAS 敗北（409）ごとに最新レコードを再取得し、
    split_valid_fields(raw_fields, 最新) を**再実行**——最新値との合成で
    矛盾が出れば日付 3 欄は write 0（矛盾理由を返し、不一致処理は呼び出し側）。
    再試行は _CAS_RETRIES 回まで。尽きたら書込を諦める（write 0・今ターンの
    データは次の発話で再収集される＝会話は継続。矛盾 postimage は成立しない）。

    fence（HOUKI-CARD-READ-fix3・任意）: 所有権検査の callable。既定 None=従来と
    完全に同一挙動。与えられたときは各 CAS 試行の前（初回を含む）と 409 後の
    再取得の前に呼び、False なら再試行せず write 0（problems に固定語 "fenced" を
    足して返す。戻り値の形は不変）。
    """
    fields, problems, choice_problems = split_valid_fields(
        raw_fields, existing)
    for _attempt in range(_CAS_RETRIES):
        if fence is not None and not fence():          # 各試行の前（初回を含む）
            return _v(existing or {}, "$id"), [*problems, "fenced"], choice_problems
        try:
            rid = await upsert_case_fields(user_id, fields, existing)
            return rid, problems, choice_problems
        except kintone.KintoneConflict:
            if fence is not None and not fence():      # 409 後・再取得の前
                return (_v(existing or {}, "$id"), [*problems, "fenced"],
                        choice_problems)
            latest = await fetch_case(user_id)
            if latest is None:
                logger.warning(
                    "[HOUKI_CASE] upsert cas refetch missing (write 0)")
                return _v(existing or {}, "$id"), problems, choice_problems
            existing = latest
            fields, problems, choice_problems = split_valid_fields(
                raw_fields, latest)
        except kintone.KintoneError:
            # fix3[H3-06]: 新規作成の二重 create 防止（方式(a)）。App 40 の
            # LINEユーザーID 欄は「値の重複を禁止する」を有効化する（大野の
            # 点火作業）——並行 2 タスクが双方 existing=None でも create は
            # 1 件しか成立しない。敗者の create 失敗は再検索し、既存レコードが
            # 見つかれば**そのレコードへの update に収束**（再検証込み）。
            # 見つからなければ重複起因でない障害＝従来どおり送出
            if existing is not None:
                raise
            latest = await fetch_case(user_id)
            if latest is None:
                raise
            logger.info("[HOUKI_CASE] duplicate create converged record_id=%s",
                        emit(_v(latest, "$id"), "record_id", "log",
                             "operator"))
            existing = latest
            fields, problems, choice_problems = split_valid_fields(
                raw_fields, latest)
    logger.warning("[HOUKI_CASE] upsert cas exhausted (write 0) record_id=%s",
                   emit(_v(existing or {}, "$id"), "record_id", "log",
                        "operator"))
    return _v(existing or {}, "$id"), problems, choice_problems


async def append_creditors(record_id: str, existing: dict | None,
                           names: list[str],
                           fence: Callable[[], bool] | None = None) -> int:
    """債権者一覧 SUBTABLE へ債権者名を追記（既存行保持・同名スキップ・
    新規行は 通知要否=未確認）。追加行数を返す。

    fix3[H3-07]: fix2 のマーカー/フラグと同型の $revision CAS 収束ループ——
    最新取得→既存行との併合（既存行保持+同名スキップの契約を維持）→CAS 更新。
    409 は再取得・再併合（≤_CAS_RETRIES）。収束不能=既存表を上書きせず
    要確認通知+0（write 0）。

    fence（HOUKI-CARD-READ-fix3・任意）: 所有権検査の callable。既定 None=従来と
    完全に同一挙動。与えられたときは各 CAS 試行の前（初回を含む）と 409 後の
    再取得の前に呼び、False なら再試行せず 0（要確認通知もしない=収束不能では
    なく失効）。"""
    clean = [str(n or "").strip() for n in (names or [])]
    clean = [n for n in clean if n]
    if not clean:
        return 0
    for _attempt in range(_CAS_RETRIES):
        if fence is not None and not fence():          # 各試行の前（初回を含む）
            return 0
        rows = list(((existing or {}).get(CREDITOR_TABLE) or {})
                    .get("value") or [])
        known = {str(((r.get("value") or {}).get("債権者名") or {})
                     .get("value") or "").strip() for r in rows}
        added = 0
        for name in clean:
            if name in known:
                continue
            rows.append({"value": {"債権者名": {"value": name},
                                   "通知要否": {"value": "未確認"}}})
            known.add(name)
            added += 1
        if not added:
            return 0
        try:
            # SUBTABLE の plain 値=行 list（行内の kintone 行構造はそのまま）
            await kintone.update_record(
                APP_HOUKI_CASE, record_id, {CREDITOR_TABLE: rows},
                revision=_v(existing or {}, "$revision") or None)
            logger.info("[HOUKI_CASE] creditors appended record_id=%s rows=%s",
                        emit(record_id, "record_id", "log", "operator"),
                        emit(added, "count", "log", "operator"))
            return added
        except kintone.KintoneConflict:
            if fence is not None and not fence():      # 409 後・再取得の前
                return 0
            latest = await _refetch_by_id(record_id)
            if latest is None:
                break
            existing = latest
    await _cas_unresolved_alert(record_id, "債権者一覧の追記")
    return 0


# ── fix1[03]: 日付整合失敗の永続正本（in-memory カウンタ廃止） ──────────────────
# 1 回目=日付申告メモへ固定マーカー追記（App 40 が正本＝再起動を跨いで持続）。
# 発火済み=危険類型フラグ「申告内容の矛盾」。判定はすべてレコード実値から導出
MEMO_FIELD = "日付申告メモ"
MISMATCH_MARKER = "【日付整合エラー検知】"


def has_mismatch_marker(record: dict | None) -> bool:
    return MISMATCH_MARKER in _v(record or {}, MEMO_FIELD)


def has_mismatch_flag(record: dict | None) -> bool:
    current = list((((record or {}).get(KIKEN_FLAG_FIELD) or {})
                    .get("value")) or [])
    return KIKEN_FLAG_DATE_MISMATCH in current


async def _cas_unresolved_alert(record_id: str, what: str) -> None:
    """fix2[H3-05]: 再取得・再照合不能（CAS 収束の再試行超過/最新取得失敗）＝
    **上書きせず**管理者へ要確認通知（固定文言+レコード番号のみ）。"""
    logger.warning("[HOUKI_CASE] cas unresolved (no write) record_id=%s",
                   emit(record_id, "record_id", "log", "operator"))
    await notify.notify_admin_line(
        "【相続放棄・要確認】案件レコードの更新が競合し収束できませんでした"
        f"（{what}・上書きはしていません）。\n"
        f"相続放棄案件レコードNo: {record_id}\n"
        "kintone で内容を確認してください。",
        throttle_key=f"houki_cas_unresolved:{record_id}",
    )


async def _refetch_by_id(record_id: str) -> dict | None:
    try:
        return await kintone.get_record(APP_HOUKI_CASE, record_id)
    except kintone.KintoneError:
        return None


async def add_mismatch_marker(record_id: str, existing: dict) -> bool:
    """1 回目の失敗マーカーを 日付申告メモ へ追記（冪等・固定文言・PII なし）。

    fix2[H3-05]: $revision CAS の read-modify-write。409 は最新を再取得して
    収束——マーカー既存=write 0／未存在=**最新メモ本文を保持したまま**追記。
    収束不能（再試行超過・再取得失敗）=上書きせず要確認通知。"""
    for _attempt in range(_CAS_RETRIES):
        memo = _v(existing, MEMO_FIELD)
        if MISMATCH_MARKER in memo:
            return False
        new_memo = (memo + "\n" if memo else "") + MISMATCH_MARKER
        try:
            await kintone.update_record(
                APP_HOUKI_CASE, record_id, {MEMO_FIELD: new_memo},
                revision=_v(existing, "$revision") or None)
            logger.info("[HOUKI_CASE] mismatch marker set record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return True
        except kintone.KintoneConflict:
            latest = await _refetch_by_id(record_id)
            if latest is None:
                break
            existing = latest
    await _cas_unresolved_alert(record_id, "日付申告メモのマーカー追記")
    return False


async def mark_date_mismatch_flag(record_id: str, existing: dict) -> bool:
    """日付整合の 2 回失敗（正本 §2.1）: 危険類型フラグへ
    「申告内容の矛盾」を追記する（既存チェックは保持・冪等）。

    fix2[H3-05]: $revision CAS の read-modify-write。409 は最新を再取得して
    収束——フラグ既存=write 0／未存在=**人が追加した別フラグを保持したまま**
    追加。収束不能=上書きせず要確認通知。"""
    for _attempt in range(_CAS_RETRIES):
        current = list(((existing.get(KIKEN_FLAG_FIELD) or {})
                        .get("value")) or [])
        if KIKEN_FLAG_DATE_MISMATCH in current:
            return False
        try:
            # CHECK_BOX の plain 値=選択値の list
            await kintone.update_record(
                APP_HOUKI_CASE, record_id,
                {KIKEN_FLAG_FIELD: current + [KIKEN_FLAG_DATE_MISMATCH]},
                revision=_v(existing, "$revision") or None)
            logger.info("[HOUKI_CASE] kiken flag set record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return True
        except kintone.KintoneConflict:
            latest = await _refetch_by_id(record_id)
            if latest is None:
                break
            existing = latest
    await _cas_unresolved_alert(record_id, "危険類型フラグの追加")
    return False


# ── H-4: 電話推奨度判定の書込面（危険類型フラグの複数追記+推奨度/根拠） ──────────
# 電話要否・受任判断・電話予定日時・起算日_確定 等の弁護士専権欄への書込関数は
# 本 module に存在しない（書込閉集合の維持・test_houki_phone_triage が payload を pin）
PHONE_RECO_FIELD = "電話推奨度"
PHONE_RECO_RATIONALE_FIELD = "電話推奨根拠"


async def add_kiken_flags(record_id: str, existing: dict,
                          labels: list[str]) -> int | None:
    """危険類型フラグへ複数値を追記する（mark_date_mismatch_flag の複数値
    一般形・H-4）。既存チェック（人の編集含む）は保持・既存在分は追加しない
    （冪等）。$revision CAS の read-modify-write・409 は最新再取得で収束
    （≤_CAS_RETRIES）・収束不能=上書きせず要確認通知。

    戻り値（fix1[H4-02]: 「write 0 正常」と保存失敗を区別）:
      int  = 追加した件数（0 = 追加対象なし＝正常）
      None = CAS 収束不能（再試行超過・再取得失敗）＝保存失敗。要確認通知は
             送信済み。呼び出し側は後段（通知・推奨度書込）に進まないこと"""
    want = [str(v or "").strip() for v in (labels or [])]
    want = [v for v in want if v]
    if not want:
        return 0
    for _attempt in range(_CAS_RETRIES):
        current = list(((existing.get(KIKEN_FLAG_FIELD) or {})
                        .get("value")) or [])
        add = [v for v in want if v not in current]
        if not add:
            return 0
        try:
            await kintone.update_record(
                APP_HOUKI_CASE, record_id,
                {KIKEN_FLAG_FIELD: current + add},
                revision=_v(existing, "$revision") or None)
            logger.info("[HOUKI_CASE] kiken flags added record_id=%s count=%s",
                        emit(record_id, "record_id", "log", "operator"),
                        emit(len(add), "count", "log", "operator"))
            return len(add)
        except kintone.KintoneConflict:
            latest = await _refetch_by_id(record_id)
            if latest is None:
                break
            existing = latest
    await _cas_unresolved_alert(record_id, "危険類型フラグの追加")
    return None


async def set_phone_recommendation(record_id: str, existing: dict,
                                   recommendation: str,
                                   rationale: str) -> bool:
    """電話推奨度+電話推奨根拠を書く（H-4 の冪等キー: **推奨度が空のとき
    だけ**書く。非空=判定済み=write 0。フラグの解除・推奨度の再判定は
    弁護士のみ〔正本 §3.1〕）。$revision CAS・409 は最新再取得で収束
    （最新で推奨度が非空なら他の勝者に譲り write 0）・収束不能=要確認通知。"""
    for _attempt in range(_CAS_RETRIES):
        if _v(existing, PHONE_RECO_FIELD):
            return False
        try:
            await kintone.update_record(
                APP_HOUKI_CASE, record_id,
                {PHONE_RECO_FIELD: recommendation,
                 PHONE_RECO_RATIONALE_FIELD: rationale},
                revision=_v(existing, "$revision") or None)
            logger.info("[HOUKI_CASE] phone recommendation set record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return True
        except kintone.KintoneConflict:
            latest = await _refetch_by_id(record_id)
            if latest is None:
                break
            existing = latest
    await _cas_unresolved_alert(record_id, "電話推奨度の書き込み")
    return False


def hearing_required_satisfied(record: dict, pending: dict) -> bool:
    """必須項目（HEARING_REQUIRED_FIELDS）が record+今回書込分で全て非空か。"""
    for code in HEARING_REQUIRED_FIELDS:
        if not (_v(record or {}, code) or str(pending.get(code) or "").strip()):
            return False
    return True


async def promote_status_to_phone_triage(record_id: str,
                                         existing: dict) -> bool:
    """status を 問い合わせ（または空）→ 電話判断待ち へ一方向遷移させる。
    それ以外の現値（受任 等）からは**絶対に動かさない**。遷移したら True。

    fix1[02]: $revision の CAS 更新（contract/notice 金型と同流儀）。
    409（KintoneConflict）＝読取後に人（弁護士）が変更した＝**作用 0**。
    最新を取り直しての自動再遷移はしない（人の変更を尊重して停止）。"""
    current = _v(existing, STATUS_FIELD)
    if current not in ("", STATUS_INQUIRY):
        return False
    revision = _v(existing, "$revision")
    try:
        await kintone.update_record(
            APP_HOUKI_CASE, record_id,
            {STATUS_FIELD: STATUS_PHONE_TRIAGE},
            revision=revision or None)
    except kintone.KintoneConflict:
        logger.info("[HOUKI_CASE] status promote cas_lost record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return False
    logger.info("[HOUKI_CASE] status -> phone triage record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    return True
