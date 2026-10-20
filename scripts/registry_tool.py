"""registry_tool — HMAC 鍵 registry（SERVICE_HMAC_KEY_REGISTRY）の点検・延長・再構成（REGISTRY-EXPIRY-WARN-1・J-1）。

2026-10-17 の全鍵失効（起動停止 4 日）と 2026-10-20 の復旧時事故（`/tmp` が Python と Git Bash で別の場所に
解決し、`$(cat …)` の空展開で env が空文字で上書きされた）を受け、pgverify の reg_check / reg_extend /
reg_rebuild を 1 本に統合したもの。

サブコマンド（秘密値は一切表示しない。表示は key_id・status・残日数・件数のみ）:

  check    env の registry を読み、鍵ごとの status/expires_at/残日数と実効鍵数を表示。
           終了コード 0=実効鍵あり・1=未設定/不正/実効鍵 0。本番は `railway run python scripts/registry_tool.py check`。
  extend   全 entry の expires_at を --until（YYYY-MM-DD・JST 0 時）へ延長した JSON を %TEMP%/reg.json に書く。
           env が空・JSON 不正・延長後の実効鍵 0 のときは**何も書かず終了コード 2**。
  rebuild  --template（メタデータのみの JSON・secret なし）と --secrets-dir（<dir>/reg_secret_<kid>.txt＝hex）から
           registry を組み立て %TEMP%/reg.json に書く。secret ファイルが無い kid は生成して同所へ書く
           （--no-generate で禁止＝終了コード 2）。書く前に hub.service_auth.parse_registry と実効鍵数で検証。

出力先は **Python 側で %TEMP%（TEMP/TMP 環境変数）を解決**する。Git Bash の `$TEMP` と同じ場所なので
`$(cat "$TEMP/reg.json")` で読める（`/tmp` は Python=C:/tmp・Git Bash=%TEMP% で食い違う）。

設定（set）は本ツールでは行わない。**別コマンド**で、`&&` で直結せずに:
  railway variables --set "SERVICE_HMAC_KEY_REGISTRY=$(cat "$TEMP/reg.json")"
  rm "$TEMP/reg.json"
（`railway run` は子プロセスの失敗でも終了コード 0 を返しうるため、`&&` 直結は空文字上書きの再発経路になる。）
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import secrets
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from hub.redact import emit  # noqa: E402
from hub.service_auth import (_REGISTRY_ENV, ServiceAuthConfigError,  # noqa: E402
                              _effective_key_count, parse_registry)

JST = datetime.timezone(datetime.timedelta(hours=9))
EXIT_OK = 0
EXIT_NOT_EFFECTIVE = 1
EXIT_ABORT = 2
_EFFECTIVE = ("active", "retiring")


def _now_ts() -> int:
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp())


def _out_path() -> Path:
    base = os.environ.get("TEMP") or os.environ.get("TMP")
    if not base:
        raise SystemExit(EXIT_ABORT)
    return Path(base) / "reg.json"


def _parse_env_registry():
    """env を読み (registry dict | None, reason) を返す。reason は固定コード。"""
    raw = os.environ.get(_REGISTRY_ENV, "").strip()
    if not raw:
        return None, "empty"
    try:
        return parse_registry(raw), "ok"
    except ServiceAuthConfigError:
        return None, "invalid"


def _print_keys(reg: dict, now: int) -> int:
    eff = 0
    for kid, e in sorted(reg.items()):
        days = (e.expires_at - now) // 86400
        effective = e.status in _EFFECTIVE and now <= e.expires_at
        eff += 1 if effective else 0
        exp_date = datetime.datetime.fromtimestamp(e.expires_at, JST).strftime("%Y-%m-%d")
        if effective:
            print("key:", emit(str(kid), "record_id", "log", "operator"), "status=", emit(str(e.status), "record_id", "log", "operator"), "expires=", emit(str(exp_date), "record_id", "log", "operator"),
                  "days_left=", emit(int(days), "count", "log", "operator"), "EFFECTIVE")
        elif e.status not in _EFFECTIVE:
            print("key:", emit(str(kid), "record_id", "log", "operator"), "status=", emit(str(e.status), "record_id", "log", "operator"), "expires=", emit(str(exp_date), "record_id", "log", "operator"), "not effective")
        else:
            print("key:", emit(str(kid), "record_id", "log", "operator"), "status=", emit(str(e.status), "record_id", "log", "operator"), "expires=", emit(str(exp_date), "record_id", "log", "operator"),
                  "EXPIRED days_ago=", emit(int(-days), "count", "log", "operator"))
    return eff


def cmd_check(_args) -> int:
    reg, reason = _parse_env_registry()
    if reg is None:
        print("registry:", emit(str(reason), "record_id", "log", "operator"), "-> startup validation fails when signing is enabled")
        return EXIT_NOT_EFFECTIVE
    eff = _print_keys(reg, _now_ts())
    print("entries=", emit(int(len(reg)), "count", "log", "operator"), "effective_keys=", emit(int(eff), "count", "log", "operator"))
    return EXIT_OK if eff else EXIT_NOT_EFFECTIVE


def _write_validated(raw_entries: dict) -> int:
    """dict → JSON → parse_registry と実効鍵数で検証 → %TEMP%/reg.json。失敗は書かず 2。"""
    raw = json.dumps(raw_entries, ensure_ascii=False, separators=(",", ":"))
    try:
        reg = parse_registry(raw)
    except ServiceAuthConfigError:
        print("ABORT: assembled registry is invalid (nothing written)")
        return EXIT_ABORT
    eff = _effective_key_count(reg, _now_ts())
    if eff == 0:
        print("ABORT: assembled registry has no effective keys (nothing written)")
        return EXIT_ABORT
    out = _out_path()
    out.write_text(raw, encoding="utf-8")
    print("written: %TEMP%/reg.json entries=", emit(int(len(reg)), "count", "log", "operator"), "effective_keys=", emit(int(eff), "count", "log", "operator"))
    print("next (separately, never with &&): railway variables --set "
          "\"SERVICE_HMAC_KEY_REGISTRY=$(cat \"$TEMP/reg.json\")\"  then  rm \"$TEMP/reg.json\"")
    return EXIT_OK


def _until_ts(s: str) -> int:
    d = datetime.date.fromisoformat(s)
    return int(datetime.datetime(d.year, d.month, d.day, tzinfo=JST).timestamp())


def cmd_extend(args) -> int:
    raw = os.environ.get(_REGISTRY_ENV, "").strip()
    if not raw:
        print("ABORT: registry env is empty or unset (nothing written)")
        return EXIT_ABORT
    try:
        data = json.loads(raw)
        parse_registry(raw)          # 現状が構造として正しいことも確認
    except (ValueError, ServiceAuthConfigError):
        print("ABORT: registry env is not a valid registry JSON (nothing written)")
        return EXIT_ABORT
    until = _until_ts(args.until)
    for kid, entry in data.items():
        old = entry.get("expires_at")
        entry["expires_at"] = str(until) if isinstance(old, str) else until
        print("extend:", emit(str(kid), "record_id", "log", "operator"), "expires_at ->", emit(str(args.until), "record_id", "log", "operator"))
    return _write_validated(data)


def _load_or_generate_secret(kid: str, secrets_dir: Path, allow_generate: bool) -> str | None:
    path = secrets_dir / f"reg_secret_{kid}.txt"
    if path.exists():
        hx = path.read_text(encoding="utf-8").strip()
        try:
            if len(bytes.fromhex(hx)) < 32:
                raise ValueError
        except ValueError:
            print("ABORT: secret file for", emit(str(kid), "record_id", "log", "operator"), "is not hex of >= 32 bytes")
            return None
        print("secret:", emit(str(kid), "record_id", "log", "operator"), "from file")
        return hx
    if not allow_generate:
        print("ABORT: secret file missing for", emit(str(kid), "record_id", "log", "operator"), "(--no-generate)")
        return None
    hx = secrets.token_hex(32)
    path.write_text(hx + "\n", encoding="utf-8")
    print("secret:", emit(str(kid), "record_id", "log", "operator"), "GENERATED -> reg_secret_<kid>.txt (distribute to sender, then delete)")
    return hx


def cmd_rebuild(args) -> int:
    tpl_path = Path(args.template)
    try:
        template = json.loads(tpl_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        print("ABORT: template JSON unreadable (nothing written)")
        return EXIT_ABORT
    if not isinstance(template, dict) or not template:
        print("ABORT: template must be a non-empty object of key_id -> entry (nothing written)")
        return EXIT_ABORT
    secrets_dir = Path(args.secrets_dir)
    secrets_dir.mkdir(parents=True, exist_ok=True)
    assembled = {}
    for kid, meta in template.items():
        if not isinstance(meta, dict) or "secret" in meta:
            print("ABORT: template entry must be an object without 'secret':", emit(str(kid), "record_id", "log", "operator"))
            return EXIT_ABORT
        hx = _load_or_generate_secret(kid, secrets_dir, not args.no_generate)
        if hx is None:
            return EXIT_ABORT
        entry = dict(meta)
        if args.until:
            entry["expires_at"] = _until_ts(args.until)
        entry["secret"] = hx
        assembled[kid] = entry
    return _write_validated(assembled)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="registry_tool", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="env の registry を点検（秘密値なし）")
    e = sub.add_parser("extend", help="全 entry の expires_at を延長して %TEMP%/reg.json へ")
    e.add_argument("--until", required=True, help="YYYY-MM-DD（JST 0 時）")
    r = sub.add_parser("rebuild", help="template（secret なし）+ secret ファイルから再構成して %TEMP%/reg.json へ")
    r.add_argument("--template", required=True, help="scripts/registry_template.example.json 形式")
    r.add_argument("--secrets-dir", required=True, help="reg_secret_<kid>.txt を置く/生成する dir")
    r.add_argument("--until", default="", help="指定時は全 entry の expires_at を上書き（YYYY-MM-DD JST）")
    r.add_argument("--no-generate", action="store_true", help="secret ファイルが無い kid があれば生成せず中止")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return {"check": cmd_check, "extend": cmd_extend, "rebuild": cmd_rebuild}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
