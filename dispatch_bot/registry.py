"""タスクレジストリ（D2: TaskSpec 骨格＋送付案内エントリのみ）

設計: docs/dispatch-bot/05-task-registry.md
- タスク種別の追加＝レジストリへの1エントリ登録で完結させる（チャネルアダプタと同思想）
- 解析プロンプトの「タスク種別一覧」はここから自動生成する（03 §2・§5）
- D2 時点の登録は soufu_annai のみ。職務上請求は D4・即答型/内部タスクは第2弾以降
"""

from dataclasses import dataclass, field
from typing import Callable


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
    # 必須項目ごとの聞き返し文（**聞き返しは必ずここから組み立てる**。
    # モデルの自由生成に任せない＝存在しない項目の創作防止・2026-07-04 不具合修正）
    field_questions: dict[str, str] = field(default_factory=dict)
    search_apps: list[str] = field(default_factory=list)      # 案件検索対象（env名）
    artifacts: str = ""       # 成果物種類・保存先
    adapter: str = ""         # 実行アダプタ（D3で実装）
    on_failure: str = ""      # 失敗時の扱い
    hint_for_parser: str = "" # 解析プロンプトの一覧に載せる補足
    # ── D4 追加: タスク固有フック（handler はこれらの有無だけを見る・個別分岐しない）──
    required_desc: str = ""   # 解析プロンプトに載せる必須項目の説明（空なら required_fields）
    max_clarify: int = 2      # 聞き返しの総往復上限（03 §7。D4変更: 職務上請求=8）
    param_normalizer: Callable | None = None   # task_params の検証・正規化
    missing_param_fn: Callable | None = None   # 不足項目キーを1つ返す（動的・条件付き必須用）
    pre_confirm_fn: Callable | None = None     # 復唱前の非同期チェック（App 31照合等）
    choice_fn: Callable | None = None          # pre_confirm の選択肢応答（1/2）の処理
    summary_fn: Callable | None = None         # 復唱フルテンプレに挿入する明細行
    # ── 第2段②追加: タスク固有フロー（handler はフックの有無だけを見る原則のまま）──
    flow_fn: Callable | None = None        # 標準パイプライン（必須項目→案件検索→復唱）
                                           # を丸ごと差し替える非同期フロー
    flow_reply_fn: Callable | None = None  # flow が張ったセッションへの応答処理
                                           # （番号選択等。(handled, reply) を返す）
    execute_fn: Callable | None = None     # OK 後の実行（App 30 起票の代わり。
                                           # (message, record_id, record_url) を返す）
    # ── P3-003-CMD 追加: 語彙一覧の可視条件（flag 連動。None=常に掲載＝既存不変）──
    visible_fn: Callable | None = None     # False を返す間は catalog に載せない


TASK_REGISTRY: dict[str, TaskSpec] = {}


def register(spec: TaskSpec) -> None:
    TASK_REGISTRY[spec.task_type] = spec


def get_task(task_type: str | None) -> TaskSpec | None:
    return TASK_REGISTRY.get(task_type or "")


def catalog_for_prompt() -> str:
    """解析プロンプトに埋め込むタスク種別一覧（03 §5。レジストリ駆動。
    visible_fn が False の間は掲載しない＝flag OFF の語彙非公開・P3-003-CMD §2）"""
    visible = [s for s in TASK_REGISTRY.values()
               if s.visible_fn is None or s.visible_fn()]
    answer = [s for s in visible if s.answer_only]
    filing = [s for s in visible if not s.answer_only]

    def lines(specs):
        out = []
        for s in specs:
            line = f"- {s.task_type}: {s.display_name}"
            if s.hint_for_parser:
                line += f"（{s.hint_for_parser}）"
            if not s.answer_only:
                fields = s.required_desc or ", ".join(s.required_fields) or "なし"
                line += f"｜必須入力項目: {fields}【これ以外の入力項目は存在しない】"
            out.append(line)
        return "\n".join(out) or "（なし）"

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
    # 必須は顧客名＋同封物（2026-07-04: 同封物選択を空で起票すると prepare が
    # エラーになるため必須化。宛先は案件から解決。「送付日」等は存在しない項目）。
    # enclosures の聞き返しは App 32 の有効ブロックから動的に番号選択式で組み立てる
    # （handler の動的フィールド扱い・field_questions には載せない）
    required_fields=["customer_name", "enclosures"],
    field_questions={"customer_name": "どの顧客（案件）への指示ですか？氏名を教えてください"},
    search_apps=["KINTONE_APP_ID"],
    artifacts="App 30 添付（送付案内.docx・宛名ラベル.pdf）",
    adapter="App30Filer",  # D3 で実装
    on_failure="起票失敗はLINEにエラー返信（prepare失敗は既存の警報系）",
    hint_for_parser=("顧客へ書類を郵送する際の案内文書。「〜さんに送付案内」等。"
                     "指示文に同封する書類名（例: 委任契約書）が含まれる場合のみ "
                     "task_params.enclosures に文字列配列で入れる（推測で補完しない）"),
))


# ── 職務上請求（D4・第1.5弾。設計 05 §3.1 / 必須項目は dispatch_bot/shokumu.py の
#    洗い出し結果に従う） ─────────────────────────────────────────────────────
from dispatch_bot import shokumu  # noqa: E402（循環回避のため末尾 import）

register(TaskSpec(
    task_type="shokumu_seikyu",
    display_name="職務上請求",
    answer_only=False,
    destination="app30",
    run_at="railway",
    risk="中",  # 金銭計算（小為替）・必須項目多数（06 §4）→復唱はフルテンプレ
    auto_scope="App 30 起票→既存 prepare（宛先解決・小為替計算・様式1/2重ね打ち）→承認待ちまで",
    approval_scope="発送の承認（App 30 承認待ち→承認済・kintone上）＋投函（発送済への変更）",
    required_fields=["customer_name"],  # 静的必須はここ。動的必須は missing_param_fn
    field_questions={
        "customer_name": "どの顧客（案件）への指示ですか？氏名を教えてください",
        **shokumu.QUESTIONS,
    },
    search_apps=["KINTONE_APP_ID"],
    artifacts="App 30 添付（チェックリスト・様式1/様式2 重ね打ちPDF・往復ラベル）",
    adapter="App30Filer",
    on_failure="起票失敗はLINEにエラー返信（prepare失敗・住所未登録警報は既存の警報系）",
    hint_for_parser=("戸籍謄本・住民票等を市区町村へ請求する。"
                     "task_params には指示文から取れた項目のみ入れる: "
                     'request_items=[{"type": 種別, "count": 通数}]（種別は 戸籍謄本/除籍謄本/'
                     "改製原戸籍/戸籍の附票/住民票/住民票の除票 のみ）・"
                     "municipality=請求先市区町村名・"
                     'target={"対象者","フリガナ","本籍","住所","筆頭者","世帯主","生年月日"}・'
                     "purpose=利用目的（あれば）"),
    required_desc=("customer_name, request_items（種別と通数）, municipality, "
                   "target.対象者, target.生年月日（戸籍系請求のみ必須）"),
    max_clarify=8,  # 03 §7 の D4 変更: 1論点1往復×必要項目数・全体8往復で打ち切り
    param_normalizer=shokumu.normalize_params,
    missing_param_fn=shokumu.first_missing,
    pre_confirm_fn=shokumu.pre_confirm,
    choice_fn=shokumu.choice,
    summary_fn=shokumu.summary_lines,
))


# ── 書類の仕分け（第2段②。フローは dispatch_bot/sortation_assign.py に隔離） ──
from dispatch_bot import sortation_assign  # noqa: E402（循環回避のため末尾 import）

register(TaskSpec(
    task_type="sortation_assign",
    display_name="書類の仕分け",
    answer_only=False,
    destination="sortation_log",  # App 30 には起票しない（App 38 の状態更新のみ）
    run_at="railway",
    risk="低",  # 対外効果ゼロ（Drive・LINE顧客側は動かない。移動は GAS=③の責務）
    auto_scope="仕分けログ（App 38）の 状態=照会中→確定 の更新まで",
    approval_scope="Drive のフォルダ移動（GAS のフォルダ整理実行・第2段③）",
    required_fields=["customer_name"],
    field_questions={"customer_name": sortation_assign.QUESTION_CUSTOMER},
    search_apps=["SOUZOKU_KINTONE_APP_ID"],
    artifacts="App 38 仕分けログ（状態=確定・仕分け先レコードID/氏名/フォルダ名/確定日時）",
    adapter="SortationAssign",
    on_failure="更新失敗はLINEにエラー返信（管理者警報つき・既存の警報系）",
    hint_for_parser=("Drive に届いた書類を顧客フォルダへ仕分けする。"
                     "「〇〇のフォルダに入れて」「〇〇さんの書類」等。"
                     "customer_name に顧客名のみ入れる（書類の特定は番号選択で行う）"),
    flow_fn=sortation_assign.flow,
    flow_reply_fn=sortation_assign.flow_reply,
    execute_fn=sortation_assign.execute,
))


# ── 名寄せ候補の確定（R4-2b T2。フローは dispatch_bot/person_merge_task.py に隔離）──
from dispatch_bot import person_merge_task  # noqa: E402（循環回避のため末尾 import）

register(TaskSpec(
    task_type="person_merge",
    display_name="名寄せ候補の確定",
    answer_only=False,
    destination="merge_queue",  # App 30 person_merge 封筒のクローズ＋App 34 統合
    run_at="railway",
    risk="中",  # 敗者レコードの物理削除を伴う（監査JSON＋二段確認で防御）
    auto_scope="App 34 の統合（勝者マージ・敗者削除・監査JSON添付）と App 30 クローズまで",
    approval_scope="なし（対外効果ゼロ。統合の確定判断そのものが LINE の OK）",
    required_fields=[],  # 顧客名不要（操作は一覧の番号指定・案件をまたぐ）
    search_apps=[],
    artifacts="App 30 封筒クローズ＋監査JSON添付・App 34 勝者レコード更新",
    adapter="PersonMerge",
    on_failure="実行失敗はLINEにエラー返信（候補ごとに独立・部分成功を報告）",
    hint_for_parser=("App 34 人物の名寄せ候補（同一人物の重複レコード）の一覧提示と"
                     "統合・棄却。「名寄せ候補を見せて」「人物を統合して」等。"
                     "追加パラメータは不要（操作は一覧提示後の番号指定で行う）"),
    flow_fn=person_merge_task.flow,
    flow_reply_fn=person_merge_task.flow_reply,
    execute_fn=person_merge_task.execute,
))


# ── 人物の確認（R4-2e T2。フローは dispatch_bot/person_confirm_task.py に隔離）──
from dispatch_bot import person_confirm_task  # noqa: E402（循環回避のため末尾 import）

register(TaskSpec(
    task_type="person_confirm",
    display_name="人物の確認",
    answer_only=False,
    destination="person_confirm",  # App 34 の確認5フィールド更新（起票なし）
    run_at="railway",
    risk="低",  # kintone 内部のみ（削除なし・Drive・LINE顧客側・対外送信なし）
    auto_scope="App 34 の確認フィールド（名寄せ確定/確認状態/生死区分/死亡日/"
               "被相続人フラグ）の更新まで",
    approval_scope="なし（対外効果ゼロ。書き込み自体が LINE の OK による人の確認）",
    required_fields=["customer_name"],
    field_questions={"customer_name": person_confirm_task.QUESTION_CUSTOMER},
    search_apps=["SOUZOKU_KINTONE_APP_ID"],
    artifacts="App 34 確認フィールド更新（確認済には確認者・確認日時を自動付記）",
    adapter="PersonConfirm",
    on_failure="更新失敗はLINEにエラー返信（人物ごと独立・部分成功を報告）",
    hint_for_parser=("App 34 人物レコードの確認操作（名寄せ・確認状態・生死・死亡日・"
                     "被相続人）。「〇〇さんの人物を確認して」「案件の人物一覧」等。"
                     "customer_name に顧客名。「No.4の人物」等の番号指定は "
                     "task_params.case_record_id に数字のみ入れる"),
    required_desc="customer_name または 案件No（例: No.4）",
    flow_fn=person_confirm_task.flow,
    flow_reply_fn=person_confirm_task.flow_reply,
    execute_fn=person_confirm_task.execute,
))


# ── 要確認の確定（S5-2.5 T2。フローは dispatch_bot/review_resolve_task.py に隔離）──
from dispatch_bot import review_resolve_task  # noqa: E402（循環回避のため末尾 import）

# ── 相続人の導出（P3-003-CMD。フローは dispatch_bot/heir_derive_task.py に隔離）──
from dispatch_bot import heir_derive_task  # noqa: E402（循環回避のため末尾 import）
from hub.heir_envelope import heir_derivation_enabled  # noqa: E402

register(TaskSpec(
    task_type="heir_derivation",
    display_name="相続人の導出",
    answer_only=False,
    destination="heir_derivation",  # DerivationRun 保存＋App30 要確認封筒（起票型ではない）
    run_at="railway",
    risk="低",  # 対外効果ゼロ（immutable 台帳への append＋要確認封筒のみ・削除なし）
    auto_scope="DerivationRun 保存（App36 導出台帳・append のみ）と App30 要確認封筒の起票まで",
    approval_scope="相続人の確定（App36 反映）は別経路（confirmed decision・P3-003b）",
    required_fields=["customer_name"],
    field_questions={"customer_name":
                     "どの顧客（案件）への指示ですか？氏名を教えてください"},
    search_apps=["SOUZOKU_KINTONE_APP_ID"],
    artifacts="DerivationRun（App36 導出台帳）＋App30 要確認封筒",
    adapter="HeirDerivation",
    on_failure="失敗は固定文言で LINE 返信（分類名のみ・§5A。構造化ログ [HEIR-CMD]）",
    hint_for_parser=("案件の相続人を戸籍人物（App 34）から機械導出する。"
                     "「相続人」「導出」の両語を含む明示指示のみ該当"
                     "（例:「相続人を導出して」）。customer_name に顧客名のみ入れる"),
    required_desc="customer_name または 案件No（例: No.4）",
    execute_fn=heir_derive_task.execute,
    visible_fn=heir_derivation_enabled,   # flag OFF の間は語彙一覧に載せない（§2）
))


register(TaskSpec(
    task_type="review_resolve",
    display_name="要確認の確定",
    answer_only=False,
    destination="review_queue",  # App 30 要確認のクローズ＋App 35 生成（起票はしない）
    run_at="railway",
    risk="低",  # kintone 内部のみ（Drive・LINE顧客側・対外送信なし）
    auto_scope="App 30 要確認→完了のクローズと App 35 財産行の生成まで",
    approval_scope="なし（対外効果ゼロ。以降の評価確定は従来どおり弁護士がkintoneで）",
    required_fields=["customer_name"],
    field_questions={"customer_name": review_resolve_task.QUESTION_CUSTOMER},
    search_apps=["SOUZOKU_KINTONE_APP_ID"],
    artifacts="App 35 財産行＋App 30 クローズ（完了・実行済み=yes）",
    adapter="ReviewResolve",
    on_failure="更新失敗はLINEにエラー返信（管理者警報つき・既存の警報系）",
    hint_for_parser=("要確認キュー（App 30）を案件へ確定する。"
                     "「〇〇さんの要確認を確定して」「要確認を処理して」等。"
                     "customer_name に顧客名。「No.12の案件へ」等の番号指定は "
                     "task_params.case_record_id に数字のみ入れる"),
    required_desc="customer_name または 案件No（例: No.12）",
    flow_fn=review_resolve_task.flow,
    flow_reply_fn=review_resolve_task.flow_reply,
    execute_fn=review_resolve_task.execute,
))
