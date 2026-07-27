"""judge プロンプト生成（LINE-Q-001・§3.2 チャッピー型第三者評価）。

- **API 呼出しは行わない**（審査員モデルの選定・費用は[人]ゲート）。本モジュールは
  「別系統 LLM に渡すプロンプト文字列」を決定的に生成するのみ。
- 審査入力は匿名化/合成スレッドのみ（§2 規律）。プロンプト自体に顧客データを
  埋め込む経路は存在しない（入力 thread の中身がそのまま transcript になるため、
  呼び出し側は合成/匿名化済みスレッドのみを渡すこと）。
- 出力契約: 軸別 1-5 の JSON＋最も悪い一往復の指摘（judge の信頼性校正=[人]採点との
  一致率測定に使う・§3.2）。
"""

import json

from lineq.axes import AXES


def render_transcript(thread: dict) -> str:
    """スレッドを審査用テキストへ整形（決定的・情報の追加や省略をしない）。"""
    lines = []
    for i, turn in enumerate(thread.get("turns", []), 1):
        role = "顧客" if turn.get("role") == "user" else "アシスタント"
        meta = ""
        if turn.get("role") == "assistant":
            meta = f"（カテゴリ={turn.get('category', '?')}／" \
                   f"送信区分={turn.get('delivery', '?')}）"
        lines.append(f"[{i}] {role}{meta}: {turn.get('text', '')}")
    if not lines:
        lines.append("（応答なし＝無言スレッド）")
    return "\n".join(lines)


def build_judge_prompt(thread: dict) -> str:
    """審査員（別系統 LLM）へ渡すプロンプトを生成する（決定的・API 非呼出し）。"""
    axes_block = []
    for a in AXES:
        anchors = "\n".join(f"    {score}点: {text}"
                            for score, text in sorted(a.anchors.items()))
        axes_block.append(f"- {a.axis_id}（{a.name}）: {a.definition}\n{anchors}")
    schema = {a.axis_id: "1-5 の整数" for a in AXES}
    schema["worst_exchange"] = "最も評価を下げた往復の番号と 1 文の理由"
    return (
        "あなたは法律事務所の LINE 応対品質を審査する第三者評価者です。\n"
        "以下の会話スレッドを、定義と係留例に従って軸ごとに 1-5 で採点してください。\n"
        "安全性（法的判断の適否）は審査対象外です。会話の質のみを評価します。\n"
        "\n## 評価軸と係留例\n"
        + "\n".join(axes_block)
        + "\n\n## 出力形式（JSON のみ・説明文を付けない）\n"
        + json.dumps(schema, ensure_ascii=False, indent=2)
        + "\n\n## 会話スレッド\n"
        + render_transcript(thread)
        + "\n"
    )
