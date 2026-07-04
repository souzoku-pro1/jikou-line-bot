"""自然言語解析（D2: claude_gateway 経由・tool use 構造化JSON）

設計: docs/dispatch-bot/03-natural-language-parser.md §1・§3・§5

- claude_gateway.create_message_with_fallback 経由（context="指示Bot解析"）。
  モデルフォールバック・残高警報・ClaudeUnavailableError を継承
- 出力は tool use（parse_instruction）の構造化JSONのみ。テキストからのJSON切り出しはしない
- chat_responder のプロンプト・ガードは一切使わない（確定判断9）
- モデルは claude_gateway 側で config.PRIMARY_MODEL が使われる（ここでは指定しない）
"""


import anthropic

from claude_gateway import create_message_with_fallback
from dispatch_bot import registry


_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic()
    return _client


PARSE_TOOL = {
    "name": "parse_instruction",
    "description": "オーナーの業務指示を構造化する。判断できない項目は null にし、勝手に補完しない",
    "input_schema": {
        "type": "object",
        "properties": {
            "intent": {"enum": ["query", "task", "confirm", "cancel", "unknown"]},
            "task_type": {"type": ["string", "null"],
                          "description": "タスクレジストリのtask_type。該当なしはnull"},
            "customer_name": {"type": ["string", "null"],
                              "description": "指示中の顧客名（敬称除去。例:「鈴木さん」→「鈴木」）"},
            "task_params": {"type": "object",
                            "description": "タスク種別ごとの追加項目"},
            "confidence": {"enum": ["high", "medium", "low"],
                           "description": "解釈全体の確信度"},
            "missing_fields": {"type": "array", "items": {"type": "string"},
                               "description": "起票に必須だが指示から読み取れなかった項目名"},
            "clarification": {"type": ["string", "null"],
                              "description": "confidenceがlow/unknown時にオーナーへ返す聞き返し文"},
        },
        "required": ["intent", "confidence"],
    },
}


def build_system_prompt() -> str:
    """指示Bot専用の解析プロンプト（03 §5。顧客Botと共有しない）"""
    return f"""あなたは法律事務所の業務指示を構造化するパーサーです。
入力者は事務所のオーナー弁護士本人です（顧客ではありません）。

# やること
- 指示を parse_instruction ツールで構造化する。それ以外の出力はしない
- 判断できない項目は null / missing_fields に入れる。推測で補完しない
- 「OK」「はい」「お願いします」等の短い肯定は intent=confirm、
  「キャンセル」「やめて」「違う」は intent=cancel
- 照会・一覧の依頼（「〜一覧」「〜どうなってる」）は intent=query

# タスク種別
{registry.catalog_for_prompt()}

# 制約
- 顧客への返信文を作らない（この会話に顧客はいない）
- 法的判断・文面の創作をしない（あなたの仕事は構造化のみ）
- 該当するタスク種別がなければ task_type=null とする（intent は task のままでよい）
- 解釈に自信がなければ confidence=low とし clarification に聞き返し文を書く
- missing_fields には各タスクの「必須入力項目」として上に明示されたものだけを入れる。
  一覧にない入力項目（書類名・送付日・部数など）を**創作して要求しない**
- clarification に書くのは missing_fields に挙げた項目の確認のみ。複数項目を
  列挙しない（聞き返しは1メッセージ1論点。文面は最終的にシステム側が組み立てる）
"""


def _normalize(data: dict) -> dict:
    """スキーマ準拠の最低限の正規化（欠損キーの既定値）"""
    return {
        "intent": data.get("intent") if data.get("intent") in
                  ("query", "task", "confirm", "cancel", "unknown") else "unknown",
        "task_type": data.get("task_type") or None,
        "customer_name": (data.get("customer_name") or "").strip() or None,
        "task_params": data.get("task_params") or {},
        "confidence": data.get("confidence") if data.get("confidence") in
                      ("high", "medium", "low") else "low",
        "missing_fields": list(data.get("missing_fields") or []),
        "clarification": data.get("clarification") or None,
    }


async def parse_instruction(text: str) -> dict:
    """指示テキスト → 構造化JSON（03 §3）。tool use を強制する"""
    response = await create_message_with_fallback(
        _get_client(),
        context="指示Bot解析",
        max_tokens=1024,
        system=build_system_prompt(),
        messages=[{"role": "user", "content": text}],
        tools=[PARSE_TOOL],
        tool_choice={"type": "tool", "name": "parse_instruction"},
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == "parse_instruction":
            parsed = _normalize(dict(block.input))
            print(f"[DISPATCHBOT] parsed intent={parsed['intent']} "
                  f"task={parsed['task_type']} conf={parsed['confidence']}")
            return parsed
    raise ValueError("parse_instruction の tool_use ブロックがありません "
                     f"(stop_reason={response.stop_reason})")
