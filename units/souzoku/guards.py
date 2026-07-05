"""相続一般ユニットの前提条件ガード（単一実装・docs/souzoku-shorui/02 §6）

財産目録（S3）・遺産分割協議書（S6・03 §5）・遺言（S7/S8・04）が共有する。
「評価確定=yes にする行為そのもの」は弁護士に残す分担（02 §6）であり、
本ガードはその確定操作が済んでいない財産行があれば docx 生成を拒否する。
"""


class ValuationNotConfirmed(Exception):
    """評価確定=yes でない財産行が残っている（弁護士の評価確定待ち）"""


def ensure_valuations_confirmed(records: list[dict]) -> None:
    """全行 評価確定=yes を検証し、未確定行があれば ValuationNotConfirmed を送出する"""
    unconfirmed = [
        str((r.get("$id") or {}).get("value") or "?")
        for r in records
        if ((r.get("評価確定") or {}).get("value") or "") != "yes"
    ]
    if unconfirmed:
        raise ValuationNotConfirmed(
            f"評価確定=yes でない財産行が {len(unconfirmed)} 件あります"
            f"（レコード: {', '.join(unconfirmed)}）。"
            "弁護士による評価確定後に再実行してください。"
        )
