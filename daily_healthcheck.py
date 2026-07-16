"""
日次死活監視ジョブ

監視項目:
  A. Anthropic Models API (GET /v1/models/{model_id}) で
     PRIMARY_MODEL / FALLBACK_MODEL の有効性を確認する
  B. kintone フォーム設計取得 API で、コードが依存するアプリの
     フィールドコード・型・選択肢値が config.EXPECTED_KINTONE_SCHEMA と
     一致するか検証する
  C. docx テンプレートに、コードが差し込むプレースホルダが揃っているか検証する
     （config.EXPECTED_DOCX_TEMPLATES と照合・T0-3 で追加）
  D. App 32（同封物ブロックマスタ）の有効ブロックキーが App 30『同封物選択』の
     選択肢と同期しているか検証する（docs/architecture/02 §4.2・T2-1 で追加）
  E. inbound_event journal の滞留（processing/failed の24時間超残留）を検知する
     （P1-005d で追加・journal 未開通/DB未設定時は静かにスキップ）
  F. 業務通知チャネル（DISPATCHBOT）の dead-man。heartbeat の鮮度を検証し、
     長時間無音/未設定を検知する（P1-102 で追加・DB/heartbeat未適用時はスキップ）

異常時のみ LINE Push で管理者に通知する。正常時はログのみ。

実行方式（既存構成 = Railway 単一 Web サービスに合わせる）:
  - main.py の FastAPI startup で start_healthcheck_scheduler() を呼び、
    毎日 HEALTHCHECK_HOUR_JST 時（デフォルト 7 時）にアプリ内で実行する
  - 手動実行 / Railway cron サービス化する場合は
      railway run python daily_healthcheck.py
    （異常があれば exit code 1）
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

import anthropic
import httpx

from claude_gateway import notify_admin_line
from config import (
    EXPECTED_DOCX_TEMPLATES,
    EXPECTED_KINTONE_SCHEMA,
    FALLBACK_MODEL,
    PRIMARY_MODEL,
)

logger = logging.getLogger("daily_healthcheck")

_JST = timezone(timedelta(hours=9))


def _iso(dt) -> str:
    """最古時刻の表示（UTC・分精度・tz 無しは UTC 補完）。件数/時刻のみで PII は含まない。"""
    if dt is None:
        return "?"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")


# ══════════════════════════════════════════════════════════════
# 監視項目A: Anthropic Models API
# ══════════════════════════════════════════════════════════════

async def check_models() -> list[str]:
    """PRIMARY / FALLBACK モデルの有効性を確認し、問題のリストを返す"""
    problems: list[str] = []
    client = anthropic.AsyncAnthropic()
    for label, model_id in (("PRIMARY", PRIMARY_MODEL), ("FALLBACK", FALLBACK_MODEL)):
        try:
            info = await client.models.retrieve(model_id)
            logger.info("model OK: %s=%s (%s)", label, model_id, info.display_name)
        except anthropic.NotFoundError:
            problems.append(
                f"モデル {label}={model_id} が Models API に存在しません"
                "（廃止された可能性。config.py の更新が必要）"
            )
        except Exception as e:
            problems.append(f"モデル {label}={model_id} の確認に失敗: {type(e).__name__}")
    return problems


# ══════════════════════════════════════════════════════════════
# 監視項目B: kintone フォーム設計の検証
# ══════════════════════════════════════════════════════════════

async def check_kintone_schema() -> list[str]:
    """App 21/28/29 のフォーム設計を取得し、コードの想定値と照合する"""
    problems: list[str] = []
    sub = os.environ.get("KINTONE_SUBDOMAIN", "").replace(".cybozu.com", "")
    if not sub:
        return ["KINTONE_SUBDOMAIN が未設定です"]

    for app_label, spec in EXPECTED_KINTONE_SCHEMA.items():
        app_id = os.environ.get(spec["app_id_env"], "")
        token = os.environ.get(spec["token_env"], "")
        if not (app_id and token):
            if spec.get("optional"):
                # 未稼働の経路（例: 通帳スキャン）。env が設定されたら自動的に監視対象になる
                logger.info("kintone schema skipped (optional / env unset): %s", app_label)
                continue
            problems.append(
                f"{app_label}: 環境変数 {spec['app_id_env']} / {spec['token_env']} が未設定"
            )
            continue

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://{sub}.cybozu.com/k/v1/app/form/fields.json",
                    headers={"X-Cybozu-API-Token": token},
                    params={"app": app_id},
                )
            if not resp.is_success:
                problems.append(
                    f"{app_label}: フォーム設計取得失敗 (status {resp.status_code})"
                )  # H02/§13.1: vendor 応答本文(resp.text)は problems へ載せない
                continue
            actual_fields = resp.json().get("properties", {})
        except Exception as e:
            # H02: 例外本文は problems へ載せない（クラス名のみ）
            problems.append(f"{app_label}: フォーム設計取得エラー: {type(e).__name__}")
            continue

        for code, expected in spec["fields"].items():
            actual = actual_fields.get(code)
            if actual is None:
                problems.append(
                    f"{app_label}: フィールド「{code}」が存在しません"
                    "（フィールドコードが変更・削除された可能性）"
                )
                continue
            if actual.get("type") != expected["type"]:
                problems.append(
                    f"{app_label}: フィールド「{code}」の型が想定と不一致 "
                    f"(想定={expected['type']} 実際={actual.get('type')})"
                )
                continue
            required = expected.get("required_options")
            if required:
                actual_options = set(actual.get("options", {}).keys())
                missing = [v for v in required if v not in actual_options]
                if missing:
                    problems.append(
                        f"{app_label}: フィールド「{code}」にコードが依存する選択肢 "
                        f"{missing} がありません (実際={sorted(actual_options)})"
                    )
        logger.info("kintone schema checked: %s", app_label)

    return problems


# ══════════════════════════════════════════════════════════════
# 監視項目C: docx テンプレートのプレースホルダ検査（T0-3）
# ══════════════════════════════════════════════════════════════

def check_templates() -> list[str]:
    """コードが差し込むプレースホルダがテンプレートに揃っているか検証する"""
    from hub.docx_builder import TemplateNotFound, validate_template

    problems: list[str] = []
    for path, keys in EXPECTED_DOCX_TEMPLATES.items():
        try:
            missing = validate_template(path, keys)
        except TemplateNotFound as e:
            problems.append(f"テンプレート検査（種別: {type(e).__name__}）")
            continue
        except Exception as e:
            problems.append(f"テンプレート {path} の検査に失敗: {type(e).__name__}")
            continue
        if missing:
            problems.append(
                f"テンプレート {path} に差込プレースホルダ {missing} がありません"
                "（テンプレート編集で消された可能性）"
            )
        else:
            logger.info("template checked: %s", path)
    return problems


# ══════════════════════════════════════════════════════════════
# 監視項目E: inbound_event journal の滞留検知（P1-005d）
# ══════════════════════════════════════════════════════════════

async def check_journal_backlog() -> list[str]:
    """journal（inbound_event）の滞留を検知し、問題のリストを返す。

    検知対象:
      - state=processing で claimed_at が24時間超過（NULL=列追加前の行も対象）
      - state=failed で received_at が24時間超過
    閾値24hの根拠: Stripeの自動再送（指数バックオフ・最大3日）が生きている間は
    503→再送→stale再claim の自己回復が期待できるため即異常ではないが、
    丸1日の残留は「再送が来ていない／来るたび失敗している」の早期シグナル。

    静かにスキップする条件（既存監視を壊さない・D3のlazy原則）:
      - STRIPE_EVENT_JOURNAL_ENABLED != "1"（journal未開通）
      - DATABASE_URL 未設定
      - inbound_event テーブル不在（migration未適用）
    警報文面は件数とPKのみ（event ID・dedup_key を出さない・D17流儀）。
    """
    if not os.environ.get("DATABASE_URL"):
        return []

    # RV-04c D2-M03: provider 次元で分離集計する。既存 Stripe/LINE 監視（STRIPE_EVENT_JOURNAL_
    # ENABLED ゲート・24h・Stripe runbook）は **provider != kintone に限定**して不変を保ち、
    # kintone は §4.2b の専用検査（KINTONE_EVENT_DEDUP_ENABLED ゲート・既定1h・専用 runbook）
    # として別警報にする。どちらの flag も OFF なら何も見ない（現行 byte 同一）。
    stripe_on = os.environ.get("STRIPE_EVENT_JOURNAL_ENABLED") == "1"
    kintone_on = os.environ.get("KINTONE_EVENT_DEDUP_ENABLED", "").strip().lower() \
        in ("1", "true", "on", "yes")
    if not (stripe_on or kintone_on):
        return []

    import sqlalchemy as sa

    from hub.db import session_scope
    from hub.inbound_event import InboundEvent

    now = datetime.now(timezone.utc)
    cutoff24 = now - timedelta(hours=24)
    problems: list[str] = []
    try:
        async with session_scope() as session:
            if stripe_on:
                # 既存 Stripe/LINE: processing（stale）/failed が24時間超（kintone を除外）
                stuck = (await session.execute(
                    sa.select(InboundEvent.id).where(
                        InboundEvent.provider != "kintone",
                        InboundEvent.state == "processing",
                        sa.or_(InboundEvent.claimed_at.is_(None),
                               InboundEvent.claimed_at < cutoff24)))).scalars().all()
                failed = (await session.execute(
                    sa.select(InboundEvent.id).where(
                        InboundEvent.provider != "kintone",
                        InboundEvent.state == "failed",
                        InboundEvent.received_at < cutoff24))).scalars().all()
                if stuck:
                    problems.append(
                        f"journal滞留: processing が24時間超 {len(stuck)}件 "
                        f"(PK={sorted(stuck)[:10]}) — runbook: docs/runbooks/"
                        "stripe-journal-recovery.md")
                if failed:
                    problems.append(
                        f"journal滞留: failed が24時間超 {len(failed)}件 "
                        f"(PK={sorted(failed)[:10]}) — runbook: docs/runbooks/"
                        "stripe-journal-recovery.md")
            if kintone_on:
                # RV-04c §4.2b/M04/M05: kintone の滞留（received/sending）と失敗（failed）を
                # **別文言**で検知し、それぞれ provider 別**最古時刻**を併記する。
                from hub.kintone_lane import stale_hours
                _RB = "docs/runbooks/2026-07_kintone-lane-recovery.md"
                cutoff_k = now - timedelta(hours=stale_hours())
                # 滞留（未処理）: received/sending が閾値超
                stuck_rows = (await session.execute(
                    sa.select(InboundEvent.id, InboundEvent.received_at).where(
                        InboundEvent.provider == "kintone",
                        InboundEvent.state.in_(("received", "sending")),
                        InboundEvent.received_at < cutoff_k))).all()
                if stuck_rows:
                    ids = sorted(r[0] for r in stuck_rows)
                    oldest = min(r[1] for r in stuck_rows)
                    problems.append(
                        f"kintone滞留(未処理): received/sending が{stale_hours()}時間超 "
                        f"{len(stuck_rows)}件 最古={_iso(oldest)} "
                        f"(PK={ids[:10]}) — runbook: {_RB}")
                # 失敗: failed（transient 失敗＝mark_failed_preflight 等・再送なし＝人手）
                failed_rows = (await session.execute(
                    sa.select(InboundEvent.id, InboundEvent.received_at,
                              InboundEvent.last_error).where(
                        InboundEvent.provider == "kintone",
                        InboundEvent.state == "failed",
                        InboundEvent.received_at < cutoff_k))).all()
                if failed_rows:
                    ids = sorted(r[0] for r in failed_rows)
                    oldest = min(r[1] for r in failed_rows)
                    # last_error は境界化された分類コード（get_record_error_<status>/skip_*）＝
                    # 自由文字列でないため distinct をそのまま表示（PII なし）。
                    classes = sorted({r[2] or "unknown" for r in failed_rows})
                    problems.append(
                        f"kintone失敗: failed が{stale_hours()}時間超 {len(failed_rows)}件 "
                        f"最古={_iso(oldest)} 分類={classes[:5]} "
                        f"(PK={ids[:10]}) — runbook: {_RB}")
    except (sa.exc.ProgrammingError, sa.exc.OperationalError) as e:
        # テーブル不在（migration未適用）は静かにスキップ。それ以外のDB異常は
        # 実行失敗として上位に伝える（分類のみ・本文を握りつぶさない）
        if "inbound_event" in str(e).lower():
            logger.info("journal backlog check skipped (table not ready)")
            return []
        raise

    if not problems:
        logger.info("journal backlog OK")
    return problems


# ══════════════════════════════════════════════════════════════
# 監視項目F: 業務通知チャネルの dead-man（P1-102・RV-10 §4.2 最小版・統合形）
# ══════════════════════════════════════════════════════════════

async def check_business_notify_liveness() -> list[str]:
    """業務通知（DISPATCHBOT）チャネルの死活を heartbeat の鮮度で検証する。

    毎朝の健診がこの鮮度を見ることで、「業務通知が長時間無音＝経路が死んでいる
    可能性」を検知する（送信自体を synthetic 検証とする統合形・新規メッセージ追加なし）。
    静かにスキップ: DATABASE_URL 未設定 / heartbeat テーブル未適用（初回記録前）。
    """
    # M04: token 未設定は DATABASE_URL より先に検知（fail-closed で全業務通知が無音）
    if not os.environ.get("DISPATCHBOT_CHANNEL_ACCESS_TOKEN"):
        return ["業務通知チャネル(DISPATCHBOT_CHANNEL_ACCESS_TOKEN)未設定: "
                "業務通知が送信されません（要env投入）"]

    from hub.notify_heartbeat import get_heartbeat_status
    status, last = await get_heartbeat_status("business")
    if status in ("db_unset", "table_missing"):
        # H01: DB 未設定 / migration 未適用は許容（静かにスキップ）
        logger.info("business notify heartbeat check skipped (db未設定 or table未適用)")
        return []
    if status == "empty":
        # H01: テーブルはあるが成功記録が1件も無い＝異常として返す
        return ["業務通知の成功記録が1件もありません（dead-man）: "
                "DISPATCHBOTチャネルの死活を確認してください"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    try:
        hours = float(os.environ.get("BUSINESS_NOTIFY_STALE_HOURS", "25") or "25")
    except ValueError:
        hours = 25.0
    age = datetime.now(timezone.utc) - last
    if age > timedelta(hours=hours):
        # RCF-M08 恒久修正: 低トラフィックでは「無音」は常態であり死の証拠ではない。
        # 従来はここで即 dead-man 警報を返し、その警報送信自体が heartbeat を更新する
        # ため〔警報→翌朝OK→翌々朝また警報〕の約2日周期の偽警報オシレーションに
        # なっていた（P1-104 §5 の観察事項＝本再裁定）。恒久形: stale 時は同一チャネルへ
        # synthetic heartbeat を1通実送して死活を**実測**する。送信失敗のみ dead-man 警報
        # （実障害）。
        # RCFM08-M01「チャネル生存」の定義: probe 成功＝**LINE Push API の 2xx 受理**
        # （token・宛先・通信経路の生存）であり、管理者端末での実表示・端末の通知設定・
        # Bot のブロック有無までは保証しない（過大主張しない・work-log 参照）。
        if await _send_heartbeat_probe(int(age.total_seconds() // 3600)):
            logger.info("business notify synthetic heartbeat probe OK")
            return []
        return [f"業務通知経路が約{int(age.total_seconds() // 3600)}時間無音かつ"
                "死活確認送信に失敗（dead-man）: DISPATCHBOTチャネルの死活を"
                "確認してください"]
    return []


async def _send_heartbeat_probe(silent_hours: int) -> bool:
    """RCF-M08: 業務通知チャネルへの synthetic heartbeat 実送（stale 時のみ・応答不要の
    定型文・throttle なし＝日次実行）。成功時は notify 層（hub/notify.py）が heartbeat を
    記録するため、次回チェックは鮮度 OK になる。例外・失敗は False（呼び出し側が
    dead-man 警報にする）。
    RCFM08-M01: ここでの「成功＝チャネル生存」は **LINE Push API 2xx 受理**
    （token・宛先・通信経路の生存）の意。端末での実表示・通知設定・Bot ブロックは
    保証範囲外。"""
    try:
        return await notify_admin_line(
            "【定期死活確認】業務通知チャネルの synthetic heartbeat です（応答不要）。"
            f"直近の業務通知成功から約{silent_hours}時間無音のため送信しています。",
            throttle_key="")
    except Exception:
        logger.error("synthetic heartbeat probe の送信に失敗 (request failed)")  # 固定分類・L01
        return False


# ══════════════════════════════════════════════════════════════
# 実行本体
# ══════════════════════════════════════════════════════════════

def check_next_token_residual() -> str | None:
    """RV-04c D2-M01: KINTONE_WEBHOOK_TOKEN_NEXT の残置検査（notice・警報ではない）。
    NEXT が設定済みかつ _NEXT_EXPIRES 超過（または未設定/不正）なら notice 文字列を返す。
    rotation 5-4c の削除漏れの可視化のみ（可用性影響なし・owner=大野）。"""
    if not os.environ.get("KINTONE_WEBHOOK_TOKEN_NEXT"):
        return None
    exp = os.environ.get("KINTONE_WEBHOOK_TOKEN_NEXT_EXPIRES", "").strip()
    overdue = True
    if exp:
        try:
            d = datetime.strptime(exp, "%Y-%m-%d").date()
            overdue = datetime.now(_JST).date() > d
        except ValueError:
            overdue = True   # 不正な日付は残置扱い
    if overdue:
        return ("【notice】KINTONE_WEBHOOK_TOKEN_NEXT が残置しています"
                "（rotation 5-4c の削除漏れ？ owner=大野・可用性影響なし）")
    return None


async def run_healthcheck() -> list[str]:
    """全監視項目を実行し、問題リストを返す。異常時のみ LINE 通知。"""
    now = datetime.now(_JST).strftime("%Y-%m-%d %H:%M:%S JST")
    problems: list[str] = []

    try:
        problems += await check_models()
    except Exception as e:
        problems.append(f"モデル監視の実行自体が失敗: {type(e).__name__}")
    try:
        problems += await check_kintone_schema()
    except Exception as e:
        problems.append(f"kintone監視の実行自体が失敗: {type(e).__name__}")
    try:
        problems += check_templates()
    except Exception as e:
        problems.append(f"テンプレート監視の実行自体が失敗: {type(e).__name__}")
    try:
        from channels.soufu_annai import check_block_sync  # 監視項目D（T2-1）
        problems += await check_block_sync()
    except Exception as e:
        problems.append(f"App30/32同期監視の実行自体が失敗: {type(e).__name__}")
    try:
        problems += await check_journal_backlog()  # 監視項目E（P1-005d）
    except Exception as e:
        # DB接続情報が例外本文に含まれ得るため分類のみ（RCF-M05流儀）
        problems.append(f"journal滞留監視の実行自体が失敗: {type(e).__name__}")
    try:
        problems += await check_business_notify_liveness()  # 監視項目F（P1-102）
    except Exception as e:
        problems.append(f"業務通知dead-man監視の実行自体が失敗: {type(e).__name__}")

    # RV-04c D2-M01: NEXT 残置 notice（警報ではない・problems に混ぜない）。
    notice = check_next_token_residual()

    if problems:
        logger.error("healthcheck NG (%d problems): %s", len(problems), problems)
        body = "\n".join(f"・{p}" for p in problems)
        if notice:
            body += f"\n（{notice}）"   # 通知本文に 1 行添える（異常件数には数えない）
        sent_ok = await notify_admin_line(
            "【日次死活監視: 異常検知】\n"
            f"時刻: {now}\n"
            f"{body}\n"
            "対応手順は README「日次死活監視」を参照してください。",
            throttle_key="",  # 日次実行なのでスロットルしない
        )
        # dead-man（統合形）: 業務通知の送信自体が失敗＝経路が死んでいる。
        # 例外相当の警告ログ＋可能な範囲の代替警報（顧客Botは絶対に使わない）。
        if not sent_ok:
            logger.error(
                "業務通知チャネルへの死活通知送信に失敗（dead-man発火）。"
                "DISPATCHBOTチャネルの死活を確認すること。")
            await _deadman_alt_alert()
    else:
        # PR-4c: 従来の print による二重出力を廃止。OK ログは上の logger.info に一本化
        # （INFO 配線済み〔P1-107a〕で本番 Railway に出力される）。
        if notice:
            # notice は固定文言（check_next_token_residual）。sink には定数のみ渡す（送信はしない）。
            logger.info("healthcheck notice: KINTONE_WEBHOOK_TOKEN_NEXT residual")
        logger.info("healthcheck OK (%s) models=%s/%s", now, PRIMARY_MODEL, FALLBACK_MODEL)

    return problems


async def _deadman_alt_alert() -> None:
    """業務通知が送れないときの代替警報（best-effort・顧客Botは使わない）。
    宛先は ATTORNEY_LINE_USER_ID 固定・allowlist 検証は notify_business が担う。
    同じ DISPATCHBOT 経路なので届かない可能性はあるが、一次シグナルは上の
    exception ログ（Railway 監視で拾う）。外部主体の本格 dead-man は Phase 1 段8。"""
    attorney = os.environ.get("ATTORNEY_LINE_USER_ID", "")
    if not attorney:
        return
    try:
        from hub.notify import notify_business
        await notify_business(
            attorney,
            "【要確認】業務通知チャネルの死活通知に失敗しました。"
            "Railwayログと DISPATCHBOT チャネルを確認してください。")
    except Exception:
        logger.error("dead-man 代替警報の送信にも失敗 (request failed)")  # 固定分類・L01


# ══════════════════════════════════════════════════════════════
# アプリ内スケジューラ（FastAPI startup から呼ぶ）
#   T0-2 でループ実装を hub/scheduler（ジョブレジストリ）に移設。
#   登録名 "HEALTHCHECK" により Railway ログの登録行（PR-4c 以降は app ロガー経由）:
#     [HEALTHCHECK] scheduler registered: next run in N sec (daily H:00 JST)
# ══════════════════════════════════════════════════════════════

from hub import scheduler as hub_scheduler  # noqa: E402
from hub.scheduler import _seconds_until_next_run  # noqa: E402,F401  互換 re-export


def start_healthcheck_scheduler() -> None:
    """FastAPI startup から呼ぶ。HEALTHCHECK_DISABLED=1 で無効化できる。"""
    if os.environ.get("HEALTHCHECK_DISABLED", "") == "1":
        logger.info("healthcheck scheduler disabled by HEALTHCHECK_DISABLED=1")
        return
    hour = int(os.environ.get("HEALTHCHECK_HOUR_JST", "7"))
    if not hub_scheduler.is_registered("HEALTHCHECK"):
        hub_scheduler.register_daily("HEALTHCHECK", hour, run_healthcheck)
    hub_scheduler.start_all()  # 冪等（二重 startup でもタスクは1つ）


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = asyncio.run(run_healthcheck())
    raise SystemExit(1 if result else 0)
