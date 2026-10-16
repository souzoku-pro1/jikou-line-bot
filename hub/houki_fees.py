"""houki_fees — HOUKI-TEISHUTSU-1 (3): 相続放棄 家裁提出に要する 収入印紙・予納郵券 の額（定数 1 か所・**全項目 要大野確認**）

QUEUE-2026-10-16-R F-3。数値は本ファイルにだけ置く（画面・送付状・テストは本ファイルを参照し、数値を直書きしない）。
PC-A 仮置き。裁判所ごとの予納郵券は改定（郵便料金・各庁の運用）があるため、**登録済みの額も大野が裁判所 HP で確認してから使う**。
未登録の家裁は「要確認」と表示する（額を推定しない＝fail-closed）。

- STAMP_YEN_PER_APPLICANT: 収入印紙（申述人 1 人あたり）。
- POSTAGE_TABLE: 管轄家庭裁判所名（App 40「管轄家庭裁判所」の値と完全一致）→ 予納郵券の内訳。`yen` が None の行は「要確認」。
"""

from dataclasses import dataclass

STATUS_NEEDS_CONFIRMATION = "要大野確認"

# 収入印紙: 申述人 1 人につき 800 円（家事事件手続法・民事訴訟費用等に関する法律 別表第一の 15 項・PC-A 仮置き）【要大野確認】
STAMP_YEN_PER_APPLICANT = 800
STAMP_STATUS = STATUS_NEEDS_CONFIRMATION


@dataclass(frozen=True)
class Postage:
    court: str
    yen: int | None           # None = 要確認（額を推定しない）
    breakdown: str            # 内訳の説明（切手の種類×枚数）。要確認なら案内文
    status: str = STATUS_NEEDS_CONFIRMATION


# 管轄家庭裁判所 → 予納郵券（表形式の定数・PC-A 仮置き・全行 要大野確認）。
# 行を追加するときは court を App 40 の「管轄家庭裁判所」の値と完全一致させる。yen は合計額（円）。
POSTAGE_TABLE: tuple = (
    Postage("さいたま家庭裁判所", None, "要確認（さいたま家庭裁判所の相続放棄申述の予納郵券は裁判所 HP の最新額を確認して登録）"),
    Postage("東京家庭裁判所", None, "要確認（東京家庭裁判所の相続放棄申述の予納郵券は裁判所 HP の最新額を確認して登録）"),
)
POSTAGE_UNKNOWN_LABEL = "要確認"


def postage_for(court: str | None) -> Postage:
    """管轄家庭裁判所名 → 行。未登録は yen=None の「要確認」行（court はそのまま）。"""
    name = str(court or "").strip()
    for p in POSTAGE_TABLE:
        if p.court == name:
            return p
    return Postage(name or "（管轄家庭裁判所 未入力）", None, "要確認（未登録の家庭裁判所・hub/houki_fees.POSTAGE_TABLE に追加）")


def stamp_yen(applicants: int = 1) -> int:
    return STAMP_YEN_PER_APPLICANT * max(1, int(applicants))


def fmt_yen(yen: int | None) -> str:
    return POSTAGE_UNKNOWN_LABEL if yen is None else f"{int(yen):,}円"
