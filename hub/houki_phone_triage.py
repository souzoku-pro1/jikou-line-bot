"""電話推奨度判定+弁護士通知 — SOUZOKU-HOUKI-H4（正本 10-unit-02 §3）

設計（票 SOUZOKU-HOUKI-H4）:
- 発火: H-3 の status 遷移（→電話判断待ち）の **CAS 勝者のみ**が判定を実行
  （promote_status_to_phone_triage が True を返した経路）。取りこぼし
  （通知前クラッシュ等）は次のヒアリング受信時の**自己修復発火**
  （status=電話判断待ち かつ 電話推奨度が空）で拾う。
- 冪等: App 40 `電話推奨度` の非空を冪等キーとする（永続正本から導出・
  in-memory を持たない）。fix1[H4-01/H4-02]: フラグ保存→通知→推奨度書込の
  直列化・前段失敗で後段に進まない。冪等キーを閉じるのは**通知が実際に
  届いた（notify=True）とき**だけ（管理者未設定・HTTP 失敗・スロットル拒否は
  キー開放のまま自己修復発火で再試行）。通知成功→書込失敗→再発火→再通知の
  at-least-once（H-3 fix1[03] の queue 先行と同型・未通知の沈黙を作らない・
  重複通知は再発火時のみ・人が閉じる）は不変。
- 判定（正本 §3.1 の 10 類型・ルール一次+Claude補助）:
  決定的ルールで判定できるものはレコード実値から、会話の文脈を要するもの
  （紛争気配・グレー記述等）は Claude 補助（tool use set_phone_recommendation・
  正本 §3.1）で判定する。**Claude 補助は安全側にのみ働く**: 提案フラグは
  許可閉集合（CLAUDE_ASSISTABLE_FLAGS）との積を取ったうえでルール判定結果に
  **合併（追加）するだけ**で、ルールが立てたフラグを外したり推奨度を下げる
  経路は構造的に存在しない。Claude 全断・失敗はルールのみで判定を確定し、
  根拠と通知に固定マーカーで明示（判定を止めない・fail-safe は「必ず弁護士に
  通知が届く」ことで担保。スキップ判断は常に弁護士）。
- 熟慮期間の最小日数計算（票 5.・fail-closed）: 起算点=起算点確定済 yes なら
  起算日_確定、なければ申告 3 日付の最小値（souzoku-houki/03 §3.1）。
  社内締切日=初日算入の 3 ヶ月後応当日の前日・応当日なしは月末（同 §3.3）。
  起算点が導出できない（日付なし）場合は #2「熟慮期間の経過疑い」を該当扱い
  （安全側）とし #1 の数値判定は行わない。期日フィールドへの書込・閾値警報は
  H-8 スコープ（本票は判定にのみ使う）。
- 書込: 危険類型フラグ（追記・人の既存編集保持）・電話推奨度・電話推奨根拠のみ
  （houki_case_store の CAS 高位関数）。電話要否・受任判断・電話予定日時等の
  弁護士専権欄には触れない（書込閉集合の維持・test が payload を pin）。
- 通知: notify_admin_line の固定文言（推奨度+案件レコード番号のみ・顧客名/
  相談内容は非搭載の PII 規律）。「不要寄り」でも必ず通知（正本 §3.2・
  自動スキップ経路は作らない）。
"""

import calendar
import datetime
import logging
import os

import anthropic

from claude_gateway import ClaudeUnavailableError, create_message_with_fallback
from hub import houki_case_store
from hub import notify
from hub.redact import emit

logger = logging.getLogger("hub.houki_phone_triage")

_JST = datetime.timezone(datetime.timedelta(hours=9))

# ── 危険類型 10 種（App 40 CHECK_BOX の実選択肢値・正本 §3.1 逐語・実測確認済み） ──
FLAG_DEADLINE_NEAR = "熟慮期間の残りが短い"          # 1
FLAG_DEADLINE_DOUBT = "熟慮期間の経過疑い"           # 2
FLAG_ASSET_CONTACT = "遺産接触の申告あり or 曖昧"    # 3
FLAG_DEATH_3MONTHS = "被相続人死亡から3ヶ月超経過"   # 4
FLAG_DISPUTE = "他の相続人との紛争気配"              # 5
FLAG_MISMATCH = houki_case_store.KIKEN_FLAG_DATE_MISMATCH   # 6 申告内容の矛盾
FLAG_PRIOR_RENUNCIATION = "先順位放棄が絡む"         # 7
FLAG_NOT_PRINCIPAL = "依頼者が本人でない"            # 8
FLAG_MINOR_GUARDIAN = "未成年・後見関与"             # 9
FLAG_LITIGATION = "訴訟・督促あり"                   # 10

# 表示・書込の正準順（App 40 選択肢の index 順）
FLAG_ORDER: tuple = (
    FLAG_DEADLINE_NEAR, FLAG_DEADLINE_DOUBT, FLAG_ASSET_CONTACT,
    FLAG_DEATH_3MONTHS, FLAG_DISPUTE, FLAG_MISMATCH,
    FLAG_PRIOR_RENUNCIATION, FLAG_NOT_PRINCIPAL, FLAG_MINOR_GUARDIAN,
    FLAG_LITIGATION,
)

# 推奨度の算出（正本 §3.2）: #1〜#6=強推奨 / #7〜#10のみ=推奨 / 該当なし=不要寄り
STRONG_FLAGS = frozenset({
    FLAG_DEADLINE_NEAR, FLAG_DEADLINE_DOUBT, FLAG_ASSET_CONTACT,
    FLAG_DEATH_3MONTHS, FLAG_DISPUTE, FLAG_MISMATCH})
MODERATE_FLAGS = frozenset({
    FLAG_PRIOR_RENUNCIATION, FLAG_NOT_PRINCIPAL, FLAG_MINOR_GUARDIAN,
    FLAG_LITIGATION})

RECO_STRONG = "強推奨"
RECO_MODERATE = "推奨"
RECO_LOW = "不要寄り"

# Claude 補助が**追加提案できる**類型の閉集合（正本 §3.1 の Claude補助 列の
# うち自由記述の解釈で該当し得るもの。#1 は数値ルール専属〔曖昧日付由来の
# 懸念は安全側の #2 に集約〕・#4/#8/#9 は補助なし＝レコード実値で決定的）
CLAUDE_ASSISTABLE_FLAGS: tuple = (
    FLAG_DEADLINE_DOUBT, FLAG_ASSET_CONTACT, FLAG_DISPUTE,
    FLAG_MISMATCH, FLAG_PRIOR_RENUNCIATION, FLAG_LITIGATION,
)

# Claude 補助が読む自由記述フィールド（レコード実値のみ・会話履歴は使わない＝
# 決定的な入力。App 40 の実フィールドコード）
_FREE_TEXT_SOURCES: tuple = (
    "日付申告メモ", "知った経緯",
    "財産_不動産", "財産_現金預貯金", "財産_有価証券", "財産_負債",
    "先順位相続人の状況", "先順位者の放棄状況", "他の相続人", "続柄その他",
)

# 通知・根拠の固定マーカー（Claude 補助が実行できなかった判定の明示）
ASSIST_FAILED_NOTE = "※Claude補助が実行できなかったため、機械的ルールのみの判定です。"

_DATE_APPLICANT_FIELDS = ("死亡日_申告", "死亡を知った日_申告",
                          "相続人と知った日_申告")


def _v(record: dict, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "").strip()


def _parse_date(raw: str) -> datetime.date | None:
    try:
        return datetime.date.fromisoformat(raw)
    except ValueError:
        return None


def _today_jst() -> datetime.date:
    return datetime.datetime.now(_JST).date()


# ── 熟慮期間の最小日数計算（souzoku-houki/03 §3.1/§3.3 の判定用最小実装） ──────────
def _month_anniversary(start: datetime.date, months: int) -> datetime.date:
    """start の months ヶ月後の応当日（応当日なし=その月の末日）。"""
    m = start.month + months
    y = start.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    last = calendar.monthrange(y, m)[1]
    return datetime.date(y, m, min(start.day, last))


def shanai_deadline(start: datetime.date) -> datetime.date:
    """社内締切日（安全側・03 §3.3）: 初日算入=起算点を起算日として
    3 ヶ月後の応当日の前日。応当日なしはその月の末日。繰越なし。"""
    m = start.month + 3
    y = start.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    last = calendar.monthrange(y, m)[1]
    if start.day > last:
        return datetime.date(y, m, last)            # 応当日なし → 月末
    return datetime.date(y, m, start.day) - datetime.timedelta(days=1)


def _start_point(record: dict) -> datetime.date | None:
    """起算点（03 §3.1）: 確定済=起算日_確定／未確定=申告 3 日付の最小値
    （安全側=最も早い日）。導出不能は None（呼び出し側で #2 該当=fail-closed）。"""
    if _v(record, "起算点確定済") == "yes":
        d = _parse_date(_v(record, "起算日_確定"))
        if d:
            return d
    dates = [d for d in (_parse_date(_v(record, c))
                         for c in _DATE_APPLICANT_FIELDS) if d]
    return min(dates) if dates else None


# ── ルール一次判定（正本 §3.1 の機械的ルール列） ─────────────────────────────────
def compute_rule_flags(record: dict,
                       today: datetime.date | None = None
                       ) -> tuple[set, list[str]]:
    """レコード実値から決定的に判定できる危険類型と根拠行（固定分類語彙）を返す。

    安全側の裁定（設計判断・完了報告に明記）:
    - #1: 起算点が導出できない場合は数値判定せず #2 を該当扱い（fail-closed）
    - #3: 財産処分有無の空欄は「不明」扱いで該当（必須質問文言があるため
      通常は非空。空=未回答も安全側）
    - #8: 本人区分の空欄は「本人と未確認」として該当（安全側）
    - #9: 「不明」も該当（曖昧・不明は該当フラグの原則）。空欄は非該当
    - #10: 正本の機械的ルールどおり「あり」のみ（不明の解釈は Claude 補助）
    """
    today = today or _today_jst()
    flags: set = set()
    rationale: list[str] = []

    # #1 / #2: 熟慮期間
    start = _start_point(record)
    if start is None:
        flags.add(FLAG_DEADLINE_DOUBT)
        rationale.append("熟慮期間の経過疑い: 起算点となる日付が未確定"
                         "（安全側・fail-closed）")
    else:
        deadline = shanai_deadline(start)
        remaining = (deadline - today).days
        if remaining <= 30:
            flags.add(FLAG_DEADLINE_NEAR)
            rationale.append(
                f"熟慮期間の残りが短い: 社内締切 {deadline.isoformat()}"
                f"（残 {remaining} 日・暫定・起算点未確定の間は安全側計算）")
    if any(not _parse_date(_v(record, c)) for c in _DATE_APPLICANT_FIELDS):
        if FLAG_DEADLINE_DOUBT not in flags:
            flags.add(FLAG_DEADLINE_DOUBT)
            rationale.append("熟慮期間の経過疑い: 死亡日・知った日のいずれかが"
                             "不明または曖昧（安全側）")

    # #3: 遺産接触
    disposal = _v(record, "財産処分有無")
    if disposal != "なし":
        flags.add(FLAG_ASSET_CONTACT)
        label = disposal if disposal in ("あり", "不明") else "未回答（不明扱い）"
        rationale.append(f"遺産接触の申告あり or 曖昧: 財産処分有無={label}")

    # #4: 死亡から 3 ヶ月超（死亡日_申告が確定形式のときのみ決定的に判定。
    #     日付なしは #2 で捕捉済み）
    death = _parse_date(_v(record, "死亡日_申告"))
    if death and today > _month_anniversary(death, 3):
        flags.add(FLAG_DEATH_3MONTHS)
        rationale.append(
            f"被相続人死亡から3ヶ月超経過: 死亡日_申告 {death.isoformat()}")

    # #5: 紛争気配はルール化困難（正本 §3.1）＝ Claude 補助専属

    # #6: 申告内容の矛盾（H-3 の日付整合 2 回失敗でフラグ済みの実値を継承）
    if houki_case_store.has_mismatch_flag(record):
        flags.add(FLAG_MISMATCH)
        rationale.append("申告内容の矛盾: 日付整合検証の2回失敗（記録済み）")

    # #7: 先順位放棄が絡む
    rank = _v(record, "相続順位")
    prior_text = (_v(record, "先順位相続人の状況") + " "
                  + _v(record, "先順位者の放棄状況"))
    if rank not in ("配偶者", "子") and "放棄" in prior_text:
        flags.add(FLAG_PRIOR_RENUNCIATION)
        rationale.append(f"先順位放棄が絡む: 相続順位={rank or '未回答'}・"
                         "先順位者の状況に「放棄」の記載")

    # #8: 依頼者が本人でない
    principal = _v(record, "本人区分")
    if principal != "本人":
        flags.add(FLAG_NOT_PRINCIPAL)
        rationale.append(f"依頼者が本人でない: 本人区分={principal or '未回答'}")

    # #9: 未成年・後見関与（不明も該当=安全側）
    guardian = _v(record, "未成年後見関与")
    if guardian in ("あり", "不明"):
        flags.add(FLAG_MINOR_GUARDIAN)
        rationale.append(f"未成年・後見関与: 未成年後見関与={guardian}")

    # #10: 訴訟・督促あり
    if _v(record, "訴訟督促有無") == "あり":
        flags.add(FLAG_LITIGATION)
        rationale.append("訴訟・督促あり: 訴訟督促有無=あり")

    return flags, rationale


def derive_recommendation(flags: set) -> str:
    """正本 §3.2: #1〜#6 いずれか=強推奨 / #7〜#10 のみ=推奨 / なし=不要寄り。"""
    if flags & STRONG_FLAGS:
        return RECO_STRONG
    if flags & MODERATE_FLAGS:
        return RECO_MODERATE
    return RECO_LOW


# ── Claude 補助（正本 §3.1: 自由記述の解釈のみ tool use set_phone_recommendation） ─
SET_PHONE_RECOMMENDATION_TOOL = {
    "name": "set_phone_recommendation",
    "description": (
        "案件レコードの自由記述から読み取れる危険類型を申告する。"
        "該当が読み取れなければ flags は空配列。判定は安全側"
        "（曖昧・グレーな記述は該当に倒す）。"
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "flags": {
                "type": "array",
                "items": {"type": "string",
                          "enum": list(CLAUDE_ASSISTABLE_FLAGS)},
                "description": "自由記述から該当と判断した危険類型",
            },
            "reasons": {
                "type": "array",
                "items": {"type": "string"},
                "description": "flags と同順の判断根拠の短い要約（各60字以内・"
                               "記述の引用は最小限）",
            },
        },
        "required": ["flags"],
    },
}

_ASSIST_SYSTEM_PROMPT = """\
あなたは相続放棄案件の危険類型判定の補助を行います。与えられた案件レコードの
自由記述欄を読み、次の類型のうち該当が読み取れるものを set_phone_recommendation
ツールで申告してください。判定は安全側（曖昧・グレーは該当に倒す）。
機械的に判定済みの項目を打ち消す必要はありません（あなたの申告は追加のみに使われます）。

- 熟慮期間の経過疑い: 日付の記述が曖昧・期間経過をうかがわせる（「かなり前に亡くなった」等）
- 遺産接触の申告あり or 曖昧: 財産を使った・処分した・解約した・支払いに充てた等のグレー記述（「葬儀費用に使った」「形見分けした」「片付けた」等）
- 他の相続人との紛争気配: 「揉めている」「連絡が取れない」「勝手に手続きされた」等
- 申告内容の矛盾: 記述同士が食い違う
- 先順位放棄が絡む: 家族関係の記述から先順位者の放棄が関わると読める
- 訴訟・督促あり: 訴状・支払督促・差押え等の書類名や督促の記述

該当なしの場合も必ずツールを呼び、flags を空配列にしてください。
"""


async def _call_assist_model(system_prompt: str, messages: list[dict]) -> object:
    """Claude 補助の呼び出し（claude_gateway 経由=フォールバック・残高警報を
    共用〔正本 §11〕・tool 強制で構造化出力のみ受理）。"""
    client = anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
    return await create_message_with_fallback(
        client,
        context="相続放棄・電話推奨度判定 set_phone_recommendation",
        max_tokens=1024,
        system=system_prompt,
        tools=[SET_PHONE_RECOMMENDATION_TOOL],
        tool_choice={"type": "tool", "name": "set_phone_recommendation"},
        messages=messages,
    )


async def _claude_assist(record: dict) -> tuple[set, list[str], bool]:
    """(追加フラグ, 根拠行, 補助失敗か) を返す。

    - 自由記述が全欄空なら呼び出しゼロ（解釈対象なし・決定的にスキップ）
    - 提案は CLAUDE_ASSISTABLE_FLAGS との積のみ採用（安全側にのみ働く構造）
    - 失敗（全断含む）は (空, 空, True)＝ルールのみで確定・マーカーで明示
    """
    lines = []
    for code in _FREE_TEXT_SOURCES:
        val = _v(record, code)
        if val:
            lines.append(f"{code}: {val}")
    if not lines:
        return set(), [], False
    try:
        response = await _call_assist_model(
            _ASSIST_SYSTEM_PROMPT,
            [{"role": "user",
              "content": "【案件レコードの自由記述】\n" + "\n".join(lines)}])
        tool_use = None
        for b in response.content:
            if b.type == "tool_use" and b.name == "set_phone_recommendation":
                tool_use = b
                break
        if tool_use is None:
            return set(), [], True
        raw_flags = list((tool_use.input or {}).get("flags") or [])
        raw_reasons = list((tool_use.input or {}).get("reasons") or [])
        allowed = set(CLAUDE_ASSISTABLE_FLAGS)
        flags: set = set()
        rationale: list[str] = []
        for i, f in enumerate(raw_flags):
            if f not in allowed:
                continue        # 閉集合外の提案は破棄（enum 迂回の防壁）
            flags.add(f)
            reason = str(raw_reasons[i]).strip()[:100] \
                if i < len(raw_reasons) else ""
            rationale.append(f"{f}（Claude補助）" + (f": {reason}" if reason
                                                    else ""))
        return flags, rationale, False
    except ClaudeUnavailableError:
        logger.error("[HOUKI_PHONE_TRIAGE] claude assist unavailable")
        return set(), [], True
    except Exception:
        logger.error("[HOUKI_PHONE_TRIAGE] claude assist failed (fixed reason)")
        return set(), [], True


# ── 発火・冪等・通知 ──────────────────────────────────────────────────────────
def triage_pending(record: dict) -> bool:
    """自己修復発火の判定: 遷移済み（電話判断待ち）なのに判定未了（電話推奨度が
    空）。通常は遷移 CAS 勝者経路で判定済みのため、クラッシュ等の取りこぼし時
    のみ真になる。"""
    return (_v(record, houki_case_store.STATUS_FIELD)
            == houki_case_store.STATUS_PHONE_TRIAGE
            and not _v(record, houki_case_store.PHONE_RECO_FIELD))


def _notification_text(recommendation: str, record_id: str,
                       assist_failed: bool) -> str:
    """固定文言のみ+案件レコード番号（顧客名・相談内容は非搭載の PII 規律）。"""
    text = ("【電話推奨度】相続放棄\n"
            f"推奨度: {recommendation}\n"
            f"相続放棄案件レコードNo: {record_id}\n"
            "kintone で内容を確認し、「電話要否」を選択してください"
            "（電話する / スキップ）。")
    if assist_failed:
        text += "\n" + ASSIST_FAILED_NOTE
    return text


async def run_phone_triage(user_id: str) -> bool:
    """電話推奨度判定の本体（判定→通知→書込）。判定を確定し通知したら True。

    - 最新レコードを取り直してから判定（発火時点の据置レコードを使わない）
    - status=電話判断待ち 以外は作用 0（弁護士が先へ進めた案件を再判定しない）
    - 冪等キー: 電話推奨度の非空（永続正本）。fix1: フラグ保存→通知→
      推奨度書込の直列化・前段失敗（フラグ収束不能/通知 False）で後段に
      進まずキー開放のまま False＝自己修復発火で再試行
    - 例外は内部で握って False（ヒアリング応答を道連れにしない。書込前に
      落ちれば冪等キーが空のまま＝自己修復発火が拾う）
    """
    try:
        record = await houki_case_store.fetch_case(user_id)
        if record is None:
            return False
        record_id = _v(record, "$id")
        if not record_id or not triage_pending(record):
            return False

        rule_flags, rationale = compute_rule_flags(record)
        claude_flags, claude_rationale, assist_failed = \
            await _claude_assist(record)
        flags = rule_flags | claude_flags       # 合併のみ＝補助は追加方向専用
        recommendation = derive_recommendation(flags)
        rationale = rationale + claude_rationale
        if assist_failed:
            rationale.append(ASSIST_FAILED_NOTE)
        if not rationale:
            rationale.append("該当する危険類型はありませんでした"
                             "（機械的ルール+Claude補助）")

        # fix1[H4-02]: フラグ保存→通知→推奨度書込の直列化。前段失敗で後段に
        # 進まない（冪等キー=推奨度が空のまま＝自己修復発火で再試行可能）。
        # フラグ追記は冪等（既存在=正常 0）なので再発火で安全に再走する
        ordered = [f for f in FLAG_ORDER if f in flags]
        if ordered:
            added = await houki_case_store.add_kiken_flags(
                record_id, record, ordered)
            if added is None:
                # CAS 収束不能=保存失敗（要確認通知は store 側で送信済み）。
                # 「追加対象なし=0」は正常で後段へ進む
                logger.warning("[HOUKI_PHONE_TRIAGE] flag save unresolved "
                               "(no notify, key left open)")
                return False

        # fix1[H4-01]: 冪等キー（電話推奨度）を閉じるのは通知が実際に届いた
        # （True）ときだけ。False（管理者未設定・HTTP 失敗・スロットル拒否）は
        # 推奨度を空のまま残し自己修復発火で再試行する。スロットル刻印は
        # 成功時のみ（throttle_on_success_only・共用先の既定挙動は不変）＝
        # 失敗が interval を占有しない。通知成功→書込失敗→再発火→再通知の
        # at-least-once（重複通知は許容・人が閉じる）は従来どおり
        sent = await notify.notify_admin_line(
            _notification_text(recommendation, record_id, assist_failed),
            throttle_key=f"houki_phone_triage:{record_id}",
            throttle_on_success_only=True,
        )
        if not sent:
            logger.info("[HOUKI_PHONE_TRIAGE] notify not delivered "
                        "(key left open)")
            return False

        await houki_case_store.set_phone_recommendation(
            record_id, record, recommendation, "\n".join(rationale))
        logger.info("[HOUKI_PHONE_TRIAGE] done record_id=%s flags=%s",
                    emit(record_id, "record_id", "log", "operator"),
                    emit(len(ordered), "count", "log", "operator"))
        return True
    except Exception:
        logger.error("[HOUKI_PHONE_TRIAGE] triage failed (fixed reason)")
        return False
