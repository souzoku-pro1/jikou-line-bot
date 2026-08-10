"""相続関係図のグラフ構造体（Z1・中間表現の純関数群）

設計: docs/koseki-ocr/05 §1・2026-07-07 Z1 裁定
- App 34（人物）のレコード群 → 中間表現（PersonNode / Edge の純データ）。
  レンダラ（Z2=graphviz 関係図・Z3=reportlab 一覧図）への依存ゼロ
- 夫婦エッジ: 身分事項サブテーブルの婚姻行の相手方を**氏名照合**で解決
  （空白正規化・相手方不在は警告のみ・**同姓同名など候補複数は保留**＝エッジを
  張らず警告に列挙。誤連結は関係図の誤りに直結するため安全側）
- 親子エッジ: 父人物ID/母人物ID（kind=親子）。養父/養母人物ID は kind=養親子
  （02 §2 の器・関係図の描画規則で区別するため種別を分ける）
- 属性: 死亡（生死区分＋身分事項の死亡行の和暦）・被相続人（被相続人フラグ=yes）・
  代襲候補（相続資格=代襲相続人）・性別・生年月日和暦（身分事項の出生行）
- 生成前提の検証（validate_for_rendering）: 名寄せ確定=確定のみ・確認状態=確認済のみ・
  生死不明の混入なし・被相続人の存在と死亡日。**どの人物のどの項目が未充足かを
  列挙**して返す（人が次に何を確認すべきか分かる拒否レスポンス。空リスト=生成可）
- App 36 には触れない・App 34 への書き込みゼロ（読み取り専用）
"""

import json
from dataclasses import dataclass, field

from hub import kintone

APP_KOSEKI_PERSON = kintone.KintoneApp(
    "App 34 (人物)", "APP_KOSEKI_PERSON", "TOKEN_KOSEKI_PERSON")


@dataclass(frozen=True)
class PersonNode:
    """人物ノード（描画・検証に必要な最小属性の純データ）"""
    record_id: str
    name: str                 # 氏名（原文表記のまま・02 の原則）
    gender: str = "不明"      # 男/女/不明
    zokugara: str = ""        # 続柄メモ（戸籍原文）
    birth_wareki: str = ""    # 生年月日（和暦・身分事項の出生行）
    alive: str = "不明"       # 生死区分（生存/死亡/不明）
    death_wareki: str = ""    # 死亡日（和暦・身分事項の死亡行）
    death_date: str = ""      # 死亡日（DATE・確定値）
    is_decedent: bool = False  # 被相続人フラグ=yes
    daishu_candidate: bool = False  # 相続資格=代襲相続人
    meyose: str = ""          # 名寄せ確定（未確定/自動候補/確定）
    kakunin: str = ""         # 確認状態（未確認/確認済/要再確認）


@dataclass(frozen=True)
class Edge:
    """関係エッジ。kind = 婚姻 | 親子 | 養親子。
    婚姻は a/b の順不同（正規化のため record_id 昇順で格納）・
    親子/養親子は a=親・b=子"""
    kind: str
    a: str
    b: str


@dataclass
class KinshipGraph:
    nodes: list[PersonNode] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)  # 夫婦照合の保留等

    def node(self, record_id: str) -> PersonNode | None:
        return next((n for n in self.nodes if n.record_id == record_id), None)

    def decedents(self) -> list[PersonNode]:
        return [n for n in self.nodes if n.is_decedent]


def subgraph(graph: KinshipGraph, ids: set) -> KinshipGraph:
    """人物IDの部分集合に絞った部分グラフ（Z2 heir_scope 描画絞り込み用の純関数）。

    - ノード: ids に含まれるもののみ
    - エッジ: **両端が ids に含まれるもののみ**（片端が範囲外のエッジは落とす。
      夫婦連結の不可視点ノードは to_dot が残存する婚姻エッジからのみ生成する
      ため、夫婦の双方が範囲内の場合のみ描かれる）
    - warnings は元グラフのまま保持（情報提供・絞り込みで消さない）
    """
    keep = {n.record_id for n in graph.nodes if n.record_id in ids}
    return KinshipGraph(
        nodes=[n for n in graph.nodes if n.record_id in keep],
        edges=[e for e in graph.edges if e.a in keep and e.b in keep],
        warnings=list(graph.warnings))


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _norm(name: str) -> str:
    return (name or "").replace(" ", "").replace("　", "")


def _events(record: dict) -> list[dict]:
    rows = (record.get("身分事項") or {}).get("value") or []
    out = []
    for row in rows:
        value = row.get("value") or {}
        out.append({k: str((value.get(k) or {}).get("value") or "")
                    for k in ("事項種別", "年月日", "相手方")})
    return out


def _first_event_date(events: list[dict], kind: str) -> str:
    for e in events:
        if e["事項種別"] == kind:
            return e["年月日"]
    return ""


def _to_node(record: dict) -> PersonNode:
    events = _events(record)
    return PersonNode(
        record_id=_v(record, "$id"),
        name=_v(record, "氏名"),
        gender=_v(record, "性別") or "不明",
        zokugara=_v(record, "続柄メモ"),
        birth_wareki=_first_event_date(events, "出生"),
        alive=_v(record, "生死区分") or "不明",
        death_wareki=_first_event_date(events, "死亡"),
        death_date=_v(record, "死亡日"),
        is_decedent=_v(record, "被相続人フラグ") == "yes",
        daishu_candidate=_v(record, "相続資格") == "代襲相続人",
        meyose=_v(record, "名寄せ確定"),
        kakunin=_v(record, "確認状態"),
    )


def build_graph(records: list[dict]) -> KinshipGraph:
    """App 34 レコード群 → グラフ構造体（純関数・kintone 非依存）。

    生成前提を満たさないデータでも構造体は組める（拒否判定は
    validate_for_rendering の責務。下流レンダラが検証してから描く）。
    """
    graph = KinshipGraph(nodes=[_to_node(r) for r in records])
    by_id = {n.record_id: n for n in graph.nodes}

    # ── 親子・養親子エッジ（人物IDによる確定的な参照） ──────────────────────
    for record in records:
        child_id = _v(record, "$id")
        for code, kind in (("父人物ID", "親子"), ("母人物ID", "親子"),
                           ("養父人物ID", "養親子"), ("養母人物ID", "養親子")):
            parent_id = _v(record, code)
            if not parent_id:
                continue
            if parent_id not in by_id:
                graph.warnings.append(
                    f"No.{child_id} {by_id[child_id].name}: {code}={parent_id} の"
                    "人物レコードが見つかりません（エッジ未作成）")
                continue
            graph.edges.append(Edge(kind=kind, a=parent_id, b=child_id))

    # ── 婚姻エッジ（身分事項の相手方を氏名照合・2パス） ─────────────────────
    # パスA: 照合できた婚姻行からエッジを張る（相互記載・重複行は1本に正規化）。
    # パスB: 照合できなかった行の警告は、その人物の婚姻エッジ数が婚姻行数を
    #        満たしていれば抑止する（例: 相手方が旧姓「山嵜知子」で不一致でも、
    #        相手側レコードの相互記載＝婚姻後の氏で連結済みなら警告不要）
    seen: set[tuple[str, str]] = set()
    rows_count: dict[str, int] = {}
    pending_warnings: list[tuple[str, str]] = []  # (self_id, 警告文)
    for record in records:
        self_id = _v(record, "$id")
        for e in _events(record):
            if e["事項種別"] != "婚姻":
                continue
            partner = _norm(e["相手方"])
            if not partner:
                continue
            rows_count[self_id] = rows_count.get(self_id, 0) + 1
            candidates = [n for n in graph.nodes
                          if n.record_id != self_id and _norm(n.name) == partner]
            if not candidates:
                pending_warnings.append((self_id, (
                    f"No.{self_id} {by_id[self_id].name}: 婚姻の相手方"
                    f"「{e['相手方']}」に一致する人物がいません（エッジ未作成）")))
                continue
            if len(candidates) > 1:
                nos = "・".join(f"No.{c.record_id}" for c in candidates)
                pending_warnings.append((self_id, (
                    f"No.{self_id} {by_id[self_id].name}: 婚姻の相手方"
                    f"「{e['相手方']}」の候補が複数（{nos}）のため保留"
                    "（同姓同名の可能性・エッジ未作成）")))
                continue
            pair = tuple(sorted((self_id, candidates[0].record_id)))
            if pair in seen:
                continue
            seen.add(pair)
            graph.edges.append(Edge(kind="婚姻", a=pair[0], b=pair[1]))

    def marriage_edges(person_id: str) -> int:
        return sum(1 for e in graph.edges
                   if e.kind == "婚姻" and person_id in (e.a, e.b))

    for self_id, warning in pending_warnings:
        if marriage_edges(self_id) >= rows_count.get(self_id, 0):
            continue  # 相互記載で連結済み（旧姓等の表記差）＝警告不要
        graph.warnings.append(warning)
    return graph


def validate_for_rendering(graph: KinshipGraph,
                           required_ids: set[str] | None = None) -> list[str]:
    """生成前提の検証（05 §1「確認済データのみから描画」の機械化）。

    Returns: 未充足の列挙（**どの人物のどの項目か**を含む・人が次に確認すべき
    ことが分かる形）。空リスト = 生成可。レンダラ（Z2/Z3）は非空なら生成拒否する。

    required_ids（R4-3・D-5 裁定）: 相続順位エンジンの
    heir_derivation.required_persons が返す「相続人確定に必要な人物ID」に
    検証対象を絞る。None は従来どおり全ノード要求（後方互換・既定）。
    被相続人の存在・死亡日の検証は絞り込みに関わらず全体で行う
    """
    problems: list[str] = []
    targets = graph.nodes if required_ids is None else [
        n for n in graph.nodes if n.record_id in required_ids]
    for n in targets:
        who = f"No.{n.record_id} {n.name}"
        if n.meyose != "確定":
            problems.append(f"{who}: 名寄せ確定が「{n.meyose or '空'}」"
                            "（確定のみ描画可・R4-2 の関所で確定してください）")
        if n.kakunin != "確認済":
            problems.append(f"{who}: 確認状態が「{n.kakunin or '空'}」"
                            "（確認済のみ描画可）")
        if n.alive == "不明":
            problems.append(f"{who}: 生死区分が不明"
                            "（生存/死亡の確認が必要・死亡と推定しない原則）")
    decedents = graph.decedents()
    if not decedents:
        problems.append("被相続人が特定されていません"
                        "（被相続人フラグ=yes の人物がいません）")
    for d in decedents:
        if not d.death_date:
            problems.append(f"No.{d.record_id} {d.name}: 被相続人の死亡日（DATE）が"
                            "未確定です（確認時に和暦から確定してください）")
    return problems


APP_KOSEKI_BOOK = kintone.KintoneApp(
    "App 33 (戸籍読解)", "APP_KOSEKI_BOOK", "TOKEN_KOSEKI_BOOK")


async def load_koseki_summaries_for_case(case_record_id: str) -> list[dict]:
    """案件の取得済み戸籍（App 33）の最小表示情報（読み取り専用・MAINT-3 B）。

    - 読解JSON の 戸籍.本籍／筆頭者／従前戸籍.本籍 のみ（P4-005 画面の
      「取得済み戸籍」一覧用）。**chain 判定・収集見込みの表示はしない**——
      参考判定の提示は SHOKUMU-PLAN 票の領分（koseki_chain の規律に従う）。
    - env 未設定は空リスト（縮退・他機能に影響させない）。読解JSON の解釈不能
      行は空欄表示（行自体は record_id で見える＝黙って落とさない）。
    """
    if not (APP_KOSEKI_BOOK.app_id() and APP_KOSEKI_BOOK.token()):
        return []
    records = await kintone.search_records(
        APP_KOSEKI_BOOK,
        f'案件レコードID = "{case_record_id}" order by $id asc limit 100',
        fields=["$id", "読解JSON"])
    out = []
    for r in records:
        try:
            reading = json.loads(
                str((r.get("読解JSON") or {}).get("value") or "{}"))
        except (ValueError, TypeError):
            reading = {}
        koseki = (reading.get("戸籍") or {}) if isinstance(reading, dict) else {}
        juzen = koseki.get("従前戸籍") or {}
        out.append({
            "record_id": str((r.get("$id") or {}).get("value") or ""),
            "honseki": str(koseki.get("本籍") or ""),
            "hittousha": str(koseki.get("筆頭者") or ""),
            "juzen_honseki": str(juzen.get("本籍") or ""),
        })
    return out


async def load_graph_for_case(case_record_id: str) -> KinshipGraph:
    """案件の人物レコードを App 34 から読み、グラフ構造体を返す（読み取り専用）"""
    records = await kintone.search_records(
        APP_KOSEKI_PERSON,
        f'案件レコードID = "{case_record_id}" order by $id asc limit 100',
        fields=["$id", "氏名", "性別", "続柄メモ", "生死区分", "死亡日",
                "被相続人フラグ", "相続資格", "名寄せ確定", "確認状態",
                "父人物ID", "母人物ID", "養父人物ID", "養母人物ID", "身分事項"])
    return build_graph(records)
