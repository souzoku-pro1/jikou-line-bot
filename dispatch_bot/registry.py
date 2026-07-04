"""タスクレジストリ（D2: TaskSpec 骨格＋送付案内エントリのみ）

設計: docs/dispatch-bot/05-task-registry.md
- タスク種別の追加＝レジストリへの1エントリ登録で完結させる（チャネルアダプタと同思想）
- 解析プロンプトの「タスク種別一覧」はここから自動生成する（03 §2・§5）
- D2 時点の登録は soufu_annai のみ。職務上請求は D4・即答型/内部タスクは第2弾以降
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TaskSpec:
    task_type: str            # 一意キー（英小文字スネーク）
    display_name: str         # 表示名（復唱・キューのDROP_DOWN値）
    answer_only: bool         # True=即答型（復唱不要・副作用ゼロ）／False=起票型
    destination: str          # 起票先: "app30" | "exec_queue" | ""（即答型）
    run_at: str               # 実行場所: "railway" | "office_pc" | "human"
    risk: str                 # リスク区分: 低 | 中 | 高（06 §4）
    auto_scope: str           # 自動実行可能範囲
    approval_scope: str       # 人の承認が要る範囲
    required_fields: list[str] = field(default_factory=list)  # 不足→聞き返し
    search_apps: list[str] = field(default_factory=list)      # 案件検索対象（env名）
    artifacts: str = ""       # 成果物種類・保存先
    adapter: str = ""         # 実行アダプタ（D3で実装）
    on_failure: str = ""      # 失敗時の扱い
    hint_for_parser: str = "" # 解析プロンプトの一覧に載せる補足


TASK_REGISTRY: dict[str, TaskSpec] = {}


def register(spec: TaskSpec) -> None:
    TASK_REGISTRY[spec.task_type] = spec


def get_task(task_type: str | None) -> TaskSpec | None:
    return TASK_REGISTRY.get(task_type or "")


def catalog_for_prompt() -> str:
    """解析プロンプトに埋め込むタスク種別一覧（03 §5。レジストリ駆動）"""
    answer = [s for s in TASK_REGISTRY.values() if s.answer_only]
    filing = [s for s in TASK_REGISTRY.values() if not s.answer_only]

    def lines(specs):
        return "\n".join(f"- {s.task_type}: {s.display_name}"
                         + (f"（{s.hint_for_parser}）" if s.hint_for_parser else "")
                         for s in specs) or "（なし）"

    return f"<即答型（intent=query）>\n{lines(answer)}\n<起票型（intent=task）>\n{lines(filing)}"


# ── 登録（D2: 送付案内のみ。設計 05 §3.1） ─────────────────────────────────
register(TaskSpec(
    task_type="soufu_annai",
    display_name="送付案内の作成",
    answer_only=False,
    destination="app30",
    run_at="railway",
    risk="低",
    auto_scope="App 30 起票→既存 prepare（docx+ラベル生成）→承認待ちまで",
    approval_scope="発送の承認（App 30 承認待ち→承認済・kintone上）",
    required_fields=["customer_name"],
    search_apps=["KINTONE_APP_ID"],
    artifacts="App 30 添付（送付案内.docx・宛名ラベル.pdf）",
    adapter="App30Filer",  # D3 で実装
    on_failure="起票失敗はLINEにエラー返信（prepare失敗は既存の警報系）",
    hint_for_parser="顧客へ書類を郵送する際の案内文書。「〜さんに送付案内」等",
))
