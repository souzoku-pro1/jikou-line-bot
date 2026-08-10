"""Bot語彙: 相続人の導出（P3-003-CMD 実装票・隔離 module＝person_merge_task と同型）

正本: DRAFT_P3_003_CMD（設計凍結・D6 PASS）。経路8段（§1）:
  語彙 → confirm（既存標準パイプライン・案件特定は上位 T2 責務）→ App34 読取
  → derive（凍結エンジン・無改変）→ run 保存（P3-001 正規経路）
  → 封筒結線（P3-003a・§3B 改定契約）→ 応答（PII なし・件数と ID のみ）

- flag ゲート: HEIR_DERIVATION_ENABLED（既定 OFF・task 冒頭で辞退＝I/O ゼロ。
  語彙一覧への掲載も flag 連動＝registry.visible_fn・§2）
- 冪等（§4・裁定5）: 導出前に get_current_head → 新 input_hash が head と同一なら
  **run を作らない**（no_change）。この場合も file_heir_envelope(head) は呼ぶ
  （封筒未起票／ACK 不明の回収）。
- derive error は保存しない（裁定6改定・固定応答＋[HEIR-CMD] run=not_saved_error）。
- Declarations は供給源未確認の間 空で導出し provisional=True を強制＋応答明示
  （裁定1・§3A）。kosekis=None の間 rank=3 は held へ格下げ保存（裁定2・§3A）。
- pending invalidate は execute 内 finally（成功／分類済み失敗／想定外例外の全終端・
  裁定8。dispatch_bot handler 本体は無改変・既存タスクの二重 OK 動作不変）。
- 観測（§6）: [HEIR-CMD] run=<enum> envelope=<enum> の 2 軸構造化ログ。
  ログ生成関数は合法組合せ表と照合し、表外は ValueError（バグの即時顕在化）。
  想定外例外は run 保存前=(failed:unexpected, skipped)／保存後=(created|no_change,
  failed:unexpected) の段階分離（fix4 H01・可変 state で保存状態を共有）。
- PII 規律（§5）: 応答・ログ・警報に氏名・ラベル・例外本文を載せない
  （件数・run id・封筒 No・分類名のみ）。
"""

import logging
import os
from types import SimpleNamespace

from sqlalchemy.exc import IntegrityError

from heir_derivation import (ENGINE_VERSION, FROZEN_CASE_VERSION, Declarations,
                             derive_heirs, persons_from_records)
from hub import kintone
from hub.derivation_models import (ChainIntegrityError,
                                   DecisionChainCorruptionError,
                                   ImmutableRecordError, PayloadPolicyError,
                                   build_run_payload, compute_input_hash,
                                   compute_result_hash, create_derivation_run,
                                   get_current_head, get_leaf_decision,
                                   validate_result_payload)
from hub.heir_envelope import (EnvelopeCreateUnknownError, EnvelopePolicyError,
                               EnvelopeSearchError, file_heir_envelope,
                               heir_derivation_enabled)
from hub.redact import emit

TASK_TYPE = "heir_derivation"
logger = logging.getLogger("dispatch_bot.heir_derive_task")

APP_KOSEKI_PERSON = kintone.KintoneApp(
    "App 34 (人物)", "APP_KOSEKI_PERSON", "TOKEN_KOSEKI_PERSON")

# App34 読取 field（persons_from_records の入力＋$revision＋混入検証キー。
# 案件参照フィールドコードは R4-1 書込み実装（koseki_person_sync._person_fields）
# と逐語一致: 「案件アプリID」「案件レコードID」）
_APP34_FIELDS = ["$id", "$revision", "案件アプリID", "案件レコードID", "氏名",
                 "生死区分", "死亡日", "被相続人フラグ", "父人物ID", "母人物ID",
                 "養父人物ID", "養母人物ID", "身分事項"]
_APP34_LIMIT = 500   # kintone 1 リクエスト上限。上限充足＝取りこぼしの疑い→中止

MSG_DISABLED = "相続人導出は現在無効です（HEIR_DERIVATION_ENABLED 未設定）"
# §3A 裁定1: provisional 強制の固定表示（応答へ必ず付す）
MSG_PROVISIONAL = ("申告事項（放棄・欠格・胎児・養子区分）は未反映のため"
                   "参考値です（弁護士確認必須）")

# ── §6: 2軸 enum（閉集合）と合法組合せ表 ─────────────────────────────────────
RUN_RESULTS = frozenset({
    "created", "no_change", "not_saved_error", "run_conflict",
    "failed:chain_integrity", "failed:payload_policy", "failed:kintone_read",
    "failed:immutable", "failed:unexpected"})
ENVELOPE_RESULTS = frozenset({
    "filed", "already_filed", "failed:policy", "failed:search",
    "failed:unexpected", "ack_unknown", "disabled", "skipped"})
# §6 の対応表が正（fix4 H01 完全化）: created/no_change には封筒段の全結果、
# それ以外（run 段の失敗・非保存）は skipped のみ
LEGAL_COMBINATIONS = frozenset(
    {(r, e) for r in ("created", "no_change")
     for e in ("filed", "already_filed", "disabled", "failed:policy",
               "failed:search", "failed:unexpected", "ack_unknown")}
    | {(r, "skipped") for r in ("not_saved_error", "run_conflict",
                                "failed:chain_integrity", "failed:payload_policy",
                                "failed:kintone_read", "failed:immutable",
                                "failed:unexpected")})


def build_heir_cmd_log(run_result: str, envelope_result: str,
                       case_record_id: str, run_id, envelope_no) -> str:
    """[HEIR-CMD] 構造化ログ行の生成（§6 emit 契約）。

    合法組合せ表（LEGAL_COMBINATIONS）と照合し、表外は ValueError＝バグの即時
    顕在化（「封筒段に未到達なのに filed」等の矛盾ログを構造的に排除）。
    値は enum・case/run/record ID のみ（PII なし）。"""
    if (run_result, envelope_result) not in LEGAL_COMBINATIONS:
        raise ValueError(
            f"[HEIR-CMD] 定義外の組合せ: run={run_result} envelope={envelope_result}"
            "（§6 合法組合せ表を参照）")
    return (f"[HEIR-CMD] run={run_result} envelope={envelope_result} "
            f"case={case_record_id} run_id={run_id if run_id is not None else '-'} "
            f"envelope_no={envelope_no if envelope_no is not None else '-'}")


async def _alert_business(text: str) -> None:
    """業務チャネル警報（PayloadPolicyError/ImmutableRecordError＝バグ疑い・§5A）。
    既存 dispatch_bot 警報系（管理者 LINE・分類名のみ・値非搭載）を使用。
    警報自体の失敗で本経路を壊さない（best-effort・固定分類ログのみ）。"""
    from hub import notify
    try:
        await notify.notify_admin_line(text, throttle_key="heir_cmd_policy_alert")
    except Exception:
        logger.error("[HEIR-CMD] business alert failed (fixed classification only)")


class _CaseMixupError(Exception):
    """別案件人物の混入（fix2 M03・読取後検証）。文言に値を載せない。"""


def _extract_revisions(records: list[dict], case_record_id: str) -> dict:
    """$revision の抽出＋全レコードの案件参照検証（fix2 M03・混入は中止）。"""
    revisions: dict[str, str] = {}
    for r in records:
        def v(code, r=r):
            return str((r.get(code) or {}).get("value") or "").strip()
        if v("案件レコードID") != case_record_id:
            raise _CaseMixupError()          # 値は非反射（固定応答のみ）
        revisions[v("$id")] = v("$revision")   # 欠落は canonical 化で中止（§7-11）
    return revisions


def _envelope_line(env_status: str, envelope_no) -> str:
    if env_status == "filed":
        return f"要確認封筒 No.{envelope_no} を起票しました"
    if env_status == "already_filed":
        return f"既存の要確認封筒 No.{envelope_no} を回収しました（二重起票なし）"
    if env_status == "disabled":
        return "封筒は未起票です（機能停止中）。再開後の再指示で回収できます"
    raise RuntimeError(f"想定外の封筒結果: {env_status}")   # not_target 等＝バグ


async def execute(pending) -> tuple[str, str, str]:
    """OK 後の実行（handler の execute_fn フック）。経路8段の 3〜8 を担う。

    - 例外分類は §5A の表が正: 分類済みは固定文言へ変換・想定外は伝播
      （握り潰し禁止・finally でログ emit＋pending invalidate 後に再送出）。
    - 応答・ログは件数・run id・封筒 No・分類名のみ（PII なし・§5）。
    """
    from dispatch_bot import confirm   # 遅延 import（循環回避）

    user_id = getattr(pending, "user_id", "")
    if not heir_derivation_enabled():
        # flag ゲート（task 冒頭・§2）: 固定文言で辞退・I/O ゼロ。
        # 終端につき pending は無効化（裁定8）。[HEIR-CMD] は emit しない（経路未進入）
        confirm.invalidate(user_id)
        return MSG_DISABLED, "", ""

    case_record_id = str(getattr(getattr(pending, "case", None),
                                 "record_id", "") or "")
    case_app_id = os.environ.get("SOUZOKU_KINTONE_APP_ID", "").strip()
    # 2軸の現在値（fix4 H01: 想定外の既定は run 保存前=(failed:unexpected, skipped)。
    # _pipeline が進行に応じて更新する＝run 保存後の想定外は (created|no_change,
    # failed:unexpected) で emit される）
    state = {"run": "failed:unexpected", "env": "skipped",
             "run_id": None, "env_no": None}
    message = ""
    try:
        try:
            message = await _pipeline(state, case_app_id, case_record_id)
        except kintone.KintoneError:
            state["run"], state["env"] = "failed:kintone_read", "skipped"
            message = "読取に失敗しました（KintoneError）。再指示で再試行できます"
        except _CaseMixupError:
            state["run"], state["env"] = "failed:kintone_read", "skipped"
            message = ("読取結果に別案件の人物が混入しています（案件不一致）。"
                       "中止しました。人物レコードの案件参照を確認してください")
        except PayloadPolicyError:
            state["run"], state["env"] = "failed:payload_policy", "skipped"
            message = "保存規格に不適合のため中止しました（PayloadPolicyError）"
            await _alert_business(
                "【相続人導出: 保存規格不適合】\n"
                f"案件 No.{case_record_id} / 分類: PayloadPolicyError\n"
                "規格逸脱＝バグ疑い（値はログ・通知に出しません）")
        except ChainIntegrityError:
            state["run"], state["env"] = "failed:chain_integrity", "skipped"
            message = ("保存の前提が変化しました（ChainIntegrityError）。"
                       "再指示してください")
        except IntegrityError:
            state["run"], state["env"] = "run_conflict", "skipped"
            message = "並行実行と競合しました（run_conflict）。再指示で回収できます"
        except ImmutableRecordError:
            state["run"], state["env"] = "failed:immutable", "skipped"
            message = "内部整合性エラー（ImmutableRecordError）"
            await _alert_business(
                "【相続人導出: 内部整合性エラー】\n"
                f"案件 No.{case_record_id} / 分類: ImmutableRecordError（到達＝バグ）")
        rid = state["run_id"]
        return message, str(rid) if rid is not None else "", ""
    finally:
        # §5A: すべての終端（成功／分類済み失敗／想定外例外）でログ emit＋invalidate。
        # fix1 H01: 二重 finally 構造——ログ処理（build/emit/logger）の**いかなる
        # 例外**（表外組合せの ValueError・logger/emit 自体の想定外を含む）でも
        # 内側 finally の invalidate へ必ず到達する。捕捉は Exception 幅・失敗時の
        # logger.error は固定文言のみ（例外本文・値を載せない＝非露出維持）
        try:
            try:
                logger.info("%s", build_heir_cmd_log(
                    state["run"], state["env"],
                    emit(case_record_id, "record_id", "log", "operator"),
                    state["run_id"], state["env_no"]))
            except Exception:
                logger.error(
                    "[HEIR-CMD] log emission failed (fixed classification only)")
        finally:
            confirm.invalidate(user_id)   # 裁定8: 全終端で必ず実行・handler 無改変


async def _pipeline(state: dict, case_app_id: str, case_record_id: str) -> str:
    """経路 3〜8 段の本体。分類対象の例外はそのまま送出（execute が §5A で変換）。
    進行に応じて state（run/env/run_id/env_no）を更新し、応答文を返す。"""
    # ── 3. App34 読取（読取後に全レコードの案件参照を検証・fix2 M03）──────────
    if not case_record_id.isdigit():
        raise PayloadPolicyError("canonical: case_record_id は数字列であること")
    records = await kintone.search_records(
        APP_KOSEKI_PERSON,
        f'案件レコードID = "{case_record_id}" order by $id asc limit {_APP34_LIMIT}',
        fields=_APP34_FIELDS)
    if len(records) >= _APP34_LIMIT:
        raise PayloadPolicyError(
            "canonical: 対象人物が読取上限に達し全数取得を保証できない（中止）")
    revisions = _extract_revisions(records, case_record_id)
    persons = persons_from_records(records)
    declarations = Declarations()            # 裁定1: 供給源未確認の間は空
    kosekis = None                           # 裁定2: 初版 None 固定

    # ── 被相続人の特定（0名/複数名はエンジン error＝非保存・裁定6）────────────
    decedents = [p for p in persons if p.is_decedent]
    if len(decedents) != 1:
        deriv = derive_heirs(persons, declarations, kosekis)   # error を正で確認
        state["run"], state["env"] = "not_saved_error", "skipped"
        return (f"導出エラー: 保留理由 {len(deriv.hold_reasons)} 件"
                "（保存はしていません）。人物確認語彙で被相続人を特定してください")
    at_date = decedents[0].death_date        # §4A: エンジン入力と同一値

    # ── §4-1 導出前チェック（裁定5: head 同一 input_hash なら run 非作成）──────
    input_hash = compute_input_hash(
        case_app_id=case_app_id, case_record_id=case_record_id, at_date=at_date,
        persons=persons, person_revisions=revisions, declarations=declarations,
        kosekis=kosekis, engine_version=ENGINE_VERSION,
        frozen_case_version=FROZEN_CASE_VERSION)
    head = await get_current_head(case_record_id)
    if head is not None and head.input_hash == input_hash:
        state["run"], state["run_id"] = "no_change", head.id
        state["env"] = "failed:unexpected"   # run 段確定後の想定外の既定（fix4 H01）
        run_obj = head                       # 封筒は head で回収（§4-1）
        base_msg = f"入力に変化はありません（run #{head.id} を維持・追加保存なし）"
        # P3-003c §5（裁定③=(A)）: head が否認済みかつ入力未変更＝全面 no-op の
        # 行き止まりを応答で明示（新 run も新封筒も作られない・入力修正が正規経路）。
        # leaf 読取は read-only・破損は業務警報（fix1 M02・固定分類・値非搭載）の
        # 上で既存分類（failed:chain_integrity）へ委ねる・race 正規化は警報なし
        try:
            leaf = await get_leaf_decision(head.id)
        except DecisionChainCorruptionError as e:
            await _alert_business(
                "【相続人判断: decision 鎖の破損検出】\n"
                f"案件 No.{case_record_id} / run #{e.run_id} / "
                f"有効 leaf {e.count} 件\n"
                "一本鎖でない decision 鎖を検出しました（書き込みなし・人手調査要）")
            raise
        if leaf is not None and leaf.decision == "rejected":
            base_msg += ("\nこの導出は否認済みです（入力未変更のため新しい導出は"
                         "作成されません）。入力を修正してから再導出してください")
    else:
        # ── 4. 導出（凍結エンジン・無改変）───────────────────────────────────
        deriv = derive_heirs(persons, declarations, kosekis)
        if deriv.status == "error":
            state["run"], state["env"] = "not_saved_error", "skipped"
            return (f"導出エラー: 保留理由 {len(deriv.hold_reasons)} 件"
                    "（保存はしていません）")
        # ── 5〜6. payload 変換 → run 保存（§4A 写像・P3-001 正規経路）─────────
        payload, lawyer_flags = build_run_payload(deriv)
        validate_result_payload(payload)
        result_hash = compute_result_hash(payload)
        status = deriv.status
        if deriv.rank == 3 and kosekis is None:
            status = "held"                  # 裁定2: rank3 は常に held へ格下げ
        run_id = await create_derivation_run(
            case_app_id=case_app_id, case_record_id=case_record_id,
            decedent_person_id=decedents[0].record_id, at_date=at_date,
            frozen_case_version=FROZEN_CASE_VERSION,
            input_person_revisions=revisions,
            input_person_ids=sorted((p.record_id for p in persons), key=int),
            input_hash=input_hash, status=status, rank=deriv.rank,
            result_payload=payload, result_hash=result_hash,
            lawyer_flags=lawyer_flags,
            provisional=True,                # 裁定1: OR True（申告未反映の間は強制）
            supersedes_run_id=head.id if head is not None else None,
            engine_version=ENGINE_VERSION)
        state["run"], state["run_id"] = "created", run_id
        state["env"] = "failed:unexpected"   # run 保存後の想定外の既定（fix4 H01）
        run_obj = SimpleNamespace(
            id=run_id, case_app_id=case_app_id, case_record_id=case_record_id,
            input_hash=input_hash, result_hash=result_hash,
            status=status, provisional=True, lawyer_flags=lawyer_flags)
        if status == "held":
            base_msg = (f"保留として保存しました（run #{run_id}・"
                        f"保留 {len(deriv.hold_reasons)} 件・rank={deriv.rank}）")
        else:
            base_msg = (f"相続人導出を保存しました（run #{run_id}・"
                        f"候補 {len(payload['heirs'])} 名・rank={deriv.rank}）")

    # ── 7. 封筒結線（§3B 改定契約・run 保存後の失敗でも run は残存）────────────
    run_id = state["run_id"]
    try:
        env = await file_heir_envelope(run_obj)
    except (EnvelopePolicyError, PayloadPolicyError):
        state["env"] = "failed:policy"
        return f"run #{run_id} は保存済み・封筒の前提検証で中止しました"
    except EnvelopeSearchError:
        state["env"] = "failed:search"
        return (f"run #{run_id} は保存済み・封筒起票のみ失敗しました。"
                "再指示で封筒のみ再試行できます")
    except EnvelopeCreateUnknownError:
        state["env"] = "ack_unknown"
        return (f"run #{run_id} は保存済み・封筒は結果不明です。"
                "再指示すると完全一致検索で回収します")
    state["env_no"] = env.get("record_id")
    env_line = _envelope_line(env["status"], state["env_no"])
    state["env"] = {"filed": "filed", "already_filed": "already_filed",
                    "disabled": "disabled"}[env["status"]]

    # ── 8. 応答（PII なし・§3A の provisional 固定表示）────────────────────────
    return f"{base_msg}\n{env_line}\n{MSG_PROVISIONAL}"
