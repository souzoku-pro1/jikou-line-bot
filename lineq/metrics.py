"""機械計数（LINE-Q-001・§3.2 の「機械計数」側・LLM 非依存）。

judge 採点と独立に、決定的に計測できる指標のみを実装する:
- 文長（assistant 発話の平均/最大文字数）
- 質問数の規律（1 発話内の質問記号数・2 問以上の違反数)
- 専門用語密度（用語リストの出現数 / assistant 100 字あたり）
- 不安への応答速度（不安表明の直後が降格定型 ack だった率）
- 反復（同一 assistant 本文の再送数）
入力はスレッド dict（合成/匿名化済み）。顧客データ・実ログへの接続は持たない。
"""

import re
import statistics

# 専門用語リスト（裸出現を数える・言い換え検出は judge 側の責務）
JARGON_TERMS = (
    "援用", "消滅時効", "時効の更新", "更新事由", "受任通知",
    "債権者", "差押え", "信用情報", "催告", "債務の承認",
)

_QUESTION_RE = re.compile(r"[？?]")
_ANXIETY_RE = re.compile(r"不安|怖い|眠れ|どうしよう|パニック|心配|焦っ")


def _assistant_turns(thread: dict) -> list[dict]:
    return [t for t in thread.get("turns", []) if t.get("role") == "assistant"]


def compute_thread_metrics(thread: dict) -> dict:
    """1 スレッドの機械指標。judge 非依存・決定的。"""
    asst = _assistant_turns(thread)
    texts = [t.get("text", "") for t in asst]
    lengths = [len(x) for x in texts]
    q_counts = [len(_QUESTION_RE.findall(x)) for x in texts]
    total_chars = sum(lengths)
    jargon_hits = sum(x.count(term) for x in texts for term in JARGON_TERMS)

    # 不安への応答速度: user の不安表明の「次の assistant 発話」が降格 ack か
    anxiety_events = 0
    demoted_after_anxiety = 0
    turns = thread.get("turns", [])
    for i, turn in enumerate(turns):
        if turn.get("role") != "user" or not _ANXIETY_RE.search(turn.get("text", "")):
            continue
        anxiety_events += 1
        nxt = next((t for t in turns[i + 1:] if t.get("role") == "assistant"), None)
        if nxt is not None and nxt.get("delivery") == "demoted":
            demoted_after_anxiety += 1

    return {
        "assistant_turns": len(asst),
        "mean_length": round(statistics.mean(lengths), 1) if lengths else 0,
        "max_length": max(lengths) if lengths else 0,
        "multi_question_turns": sum(1 for c in q_counts if c >= 2),
        "jargon_per_100_chars": round(jargon_hits * 100 / total_chars, 2)
        if total_chars else 0.0,
        "anxiety_events": anxiety_events,
        "demoted_after_anxiety": demoted_after_anxiety,
        "duplicate_assistant_texts": len(texts) - len(set(texts)),
    }


def compute_corpus_metrics(threads: list[dict]) -> dict:
    """コーパス集計（相1=合成スレッド・相2=匿名化コーパスの両方に適用可能）。"""
    per_thread = {t.get("thread_id", f"T{i}"): compute_thread_metrics(t)
                  for i, t in enumerate(threads)}
    n = len(per_thread) or 1
    anxiety_total = sum(m["anxiety_events"] for m in per_thread.values())
    return {
        "threads": len(per_thread),
        "mean_length_overall": round(
            statistics.mean(m["mean_length"] for m in per_thread.values()
                            if m["assistant_turns"]), 1) if per_thread else 0,
        "multi_question_rate": round(
            sum(1 for m in per_thread.values() if m["multi_question_turns"]) / n, 3),
        "anxiety_demoted_rate": round(
            sum(m["demoted_after_anxiety"] for m in per_thread.values())
            / anxiety_total, 3) if anxiety_total else 0.0,
        "threads_with_duplicates": sum(
            1 for m in per_thread.values() if m["duplicate_assistant_texts"]),
        "per_thread": per_thread,
    }
