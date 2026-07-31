"""相続順位エンジン（R4-3）: 順位・法定相続分・要弁護士フラグ＋Z1ゲート絞り込み

設計: docs/koseki-ocr/04・09（テストケース表 v0.1・弁護士承認済み=凍結仕様）・
2026-07-07 R4-3 裁定

- 「提示は機械・確定は弁護士」: 出力は候補（人物・続柄・相続分 Fraction・根拠条文・
  使用事実）＋要弁護士フラグの**提示のみ**。App 34/36 への書き込みゼロ
  （封筒起票も本スコープ外=R4-3b）
- 実装する規則: 順位（887/889/890）・相続分（900・Fraction 厳密演算）・
  親等優先（889①一但書）・代襲と再代襲（887②③・兄弟系は一代限り=889②の
  準用範囲）・放棄は代襲原因でない（939）・株分け（901）・数次と代襲の峻別
  （死亡日先後）・同時死亡推定は要弁護士（32条の2）・半血（900④但書・子の
  相続には不適用）・養子（普通=二重資格/特別=実方断絶817の9/区分未判定=保留/
  縁組前出生子の代襲なし）・胎児は提示のみ（886）
- 判定保留（D-2/D-3 裁定）: 死亡日不明（死亡確定・日付なし）で先後が結論に
  影響・生死不明者が順位影響位置・親エッジ欠落で全血/半血判定不能・
  婚姻相手方の同姓同名複数（F6・Z1と同じ原則）→ _Hold で全体を保留し
  理由と要求を返す（部分的な断定をしない）
- 養子区分は App 34 に器がないため申告（Declarations.adoption_kinds）で受ける
  （D-1 調査の結論。フィールド追加は別途裁定【人・CU】）
- Z1 ゲート絞り込み: required_persons(graph, decedent) が「相続人確定に必要な
  人物」のみ返す（上位順位の存在が確定した時点で下位順位は含めない）
"""

import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from fractions import Fraction

# ── P3-003-CMD 裁定3（2026-07-27・[人]・凍結エンジンへの承認変更として記録）──
# canonical input hash（DRAFT_P3_003_CMD §1.1）の材料となる版定数。
# ロジックは無改変（定数追加のみ）。値の変更は承認変更として記録する。
FROZEN_CASE_VERSION = "v0.1"    # 凍結テストケース表 09-heir-test-cases.md の版
ENGINE_VERSION = "r4-3.v1"      # 導出エンジン自体の版（本 module の凍結実装）


@dataclass(frozen=True)
class LifeEvent:
    kind: str          # 婚姻/離婚/死亡/出生/その他
    date: str = ""     # 和暦原文（参考先後の提示用・判定には使わない）
    partner: str = ""  # 相手方氏名（原文）


@dataclass
class HeirPerson:
    """エンジン入力の人物ビュー（App 34 レコード → persons_from_records で変換）"""
    record_id: str
    name: str
    alive: str = "生存"            # 生存/死亡/不明
    death_date: str = ""           # YYYY-MM-DD（DATE 確定値）
    death_wareki: str = ""         # 和暦の死亡記載（参考先後の提示のみ）
    is_decedent: bool = False
    father_id: str = ""
    mother_id: str = ""
    adoptive_father_id: str = ""
    adoptive_mother_id: str = ""
    born_before_parents_adoption: bool = False  # E5: 親の縁組前出生（申告）
    events: list[LifeEvent] = field(default_factory=list)


@dataclass
class Declarations:
    """申告事項（戸籍から機械検出できない事実・引数渡し）"""
    renounced: set = field(default_factory=set)      # F1 相続放棄（人物ID）
    disqualified: set = field(default_factory=set)   # F2 欠格・廃除（人物ID）
    fetuses: list = field(default_factory=list)      # F3 胎児（表示ラベルの列）
    adoption_kinds: dict = field(default_factory=dict)  # 人物ID→普通養子/特別養子


@dataclass
class HeirCandidate:
    person_id: str
    name: str
    zokugara: str          # App 36 の続柄に写像可能な区分
    share: Fraction
    basis: list = field(default_factory=list)   # 根拠条文
    facts: list = field(default_factory=list)   # 使用した事実
    via: str = ""          # 代襲元・数次承継元の人物ID


@dataclass
class Derivation:
    status: str                    # "derived" | "held" | "error"
    heirs: list = field(default_factory=list)          # HeirCandidate（明細）
    shares: dict = field(default_factory=dict)         # 人物ID→Fraction（集約）
    flags: list = field(default_factory=list)          # 要弁護士フラグ [{...}]
    hold_reasons: list = field(default_factory=list)   # 保留理由（held のとき）
    unused_facts: list = field(default_factory=list)   # 判定に使えなかった事実
    rank: int = 0                  # 確定した血族順位（1/2/3・0=配偶者のみ/なし）

    @property
    def provisional(self) -> bool:
        """フラグが1つでもあれば参考値（弁護士確認必須）の帯（04 §3）"""
        return bool(self.flags)


class _Hold(Exception):
    """判定保留（理由＋要求＋フラグを載せて全体を止める・部分断定をしない）"""

    def __init__(self, reason: str, flag: str = ""):
        self.reason = reason
        self.flag = flag
        super().__init__(reason)


def _norm(text: str) -> str:
    s = unicodedata.normalize("NFKC", str(text or ""))
    return s.replace(" ", "").replace("　", "")


def persons_from_records(records: list[dict]) -> list[HeirPerson]:
    """App 34 レコード（kintone GET 形）→ HeirPerson。読み取り専用の変換"""
    persons = []
    for r in records:
        def v(code):
            return str((r.get(code) or {}).get("value") or "").strip()
        events = []
        death_wareki = ""
        for row in (r.get("身分事項") or {}).get("value") or []:
            cols = {c: str((x or {}).get("value") or "")
                    for c, x in (row.get("value") or {}).items()}
            events.append(LifeEvent(kind=cols.get("事項種別", ""),
                                    date=cols.get("年月日", ""),
                                    partner=cols.get("相手方", "")))
            if cols.get("事項種別") == "死亡" and not death_wareki:
                death_wareki = cols.get("年月日", "")
        persons.append(HeirPerson(
            record_id=v("$id"), name=v("氏名"),
            alive=v("生死区分") or "不明",
            death_date=v("死亡日"), death_wareki=death_wareki,
            is_decedent=v("被相続人フラグ") == "yes",
            father_id=v("父人物ID"), mother_id=v("母人物ID"),
            adoptive_father_id=v("養父人物ID"),
            adoptive_mother_id=v("養母人物ID"),
            events=events))
    return persons


# ══════════════════════════════════════════════════════════════
# 家族関係の解決
# ══════════════════════════════════════════════════════════════

class _Family:
    def __init__(self, persons: list[HeirPerson], decl: Declarations):
        self.by_id = {p.record_id: p for p in persons}
        self.decl = decl

    def kind_of(self, person: HeirPerson) -> str:
        """養子区分（申告）。養親エッジがあるのに未申告なら「未判定」"""
        if not (person.adoptive_father_id or person.adoptive_mother_id):
            return ""
        return self.decl.adoption_kinds.get(person.record_id, "未判定")

    def bio_parent_ids(self, person: HeirPerson) -> list[str]:
        return [x for x in (person.father_id, person.mother_id) if x]

    def adoptive_parent_ids(self, person: HeirPerson) -> list[str]:
        return [x for x in (person.adoptive_father_id,
                            person.adoptive_mother_id) if x]

    def is_child_of(self, child: HeirPerson, parent_id: str) -> bool:
        """親子関係の実効判定（特別養子=実方断絶 817の9・未判定=保留）"""
        adoptive = parent_id in self.adoptive_parent_ids(child)
        biological = parent_id in self.bio_parent_ids(child)
        if not (adoptive or biological):
            return False
        kind = self.kind_of(child)
        if adoptive:
            if kind == "未判定":
                raise _Hold(f"No.{child.record_id} {child.name} の養子区分が"
                            "未判定です（普通養子/特別養子の確定が必要）", "E4")
            return True  # 普通・特別とも養親の子（809）
        # biological のみ
        if kind == "特別養子":
            return False  # 実方断絶（817の9）
        if kind == "未判定":
            raise _Hold(f"No.{child.record_id} {child.name} の養子区分が"
                        "未判定です（実方の相続資格の判定に必要）", "E4")
        return True

    def children_of(self, parent_id: str) -> list[HeirPerson]:
        return [p for p in self.by_id.values()
                if p.record_id != parent_id and self.is_child_of(p, parent_id)]

    def effective_parent_ids(self, person: HeirPerson) -> list[str]:
        """尊属方向の実効親（特別養子は実方断絶・普通養子は実方も存続）"""
        kind = self.kind_of(person)
        adoptive = self.adoptive_parent_ids(person)
        if kind == "特別養子":
            return adoptive
        if kind == "未判定" and adoptive:
            raise _Hold(f"No.{person.record_id} {person.name} の養子区分が"
                        "未判定です（尊属の範囲の判定に必要）", "E4")
        return self.bio_parent_ids(person) + adoptive


def _classify_death(person: HeirPerson, base_date: str) -> str:
    """基準日（被相続人の死亡日）に対する生死分類。
    Returns: alive / unknown / pre(先死亡) / post(後死亡=数次) / same(同日) /
             undated(死亡確定・日付なし)"""
    if person.alive == "生存":
        return "alive"
    if person.alive == "不明":
        return "unknown"
    if not person.death_date:
        return "undated"
    if person.death_date < base_date:
        return "pre"
    if person.death_date > base_date:
        return "post"
    return "same"


# ══════════════════════════════════════════════════════════════
# 配偶者の解決（氏名照合・Z1と同じ2パス・同姓同名は保留=F6）
# ══════════════════════════════════════════════════════════════

def _marriage_partners(person: HeirPerson) -> list[str]:
    return [_norm(e.partner) for e in person.events
            if e.kind == "婚姻" and _norm(e.partner)]


def _has_divorce_with(person: HeirPerson, other: HeirPerson) -> bool:
    """離婚の解消判定。相手方が空の離婚行は「その者の婚姻の解消」とみなす
    （実データで相手方欠落があるため・複数婚の厳密な対応付けは要弁護士領域）"""
    for e in person.events:
        if e.kind != "離婚":
            continue
        p = _norm(e.partner)
        if not p or p == _norm(other.name):
            return True
    return False


def _resolve_spouse(fam: _Family, decedent: HeirPerson,
                    ctx: dict) -> HeirPerson | None:
    others = [p for p in fam.by_id.values()
              if p.record_id != decedent.record_id]
    dname = _norm(decedent.name)
    candidates = []
    for p in others:
        if dname in _marriage_partners(p):
            candidates.append(p)
    # 被相続人側の婚姻行からも解決（2パス）
    for partner_name in _marriage_partners(decedent):
        matches = [p for p in others if _norm(p.name) == partner_name]
        if len(matches) > 1:
            nos = "・".join(f"No.{m.record_id}" for m in matches)
            raise _Hold(f"婚姻の相手方「{partner_name}」に同姓同名の候補が"
                        f"複数います（{nos}）。誤連結を作らないため保留します",
                        "F6")
        if matches and matches[0] not in candidates:
            candidates.append(matches[0])
    for c in candidates:
        ctx["used"].add(c.record_id)  # 配偶者判定に使用（G1: 未使用列挙から除外）
    if not candidates:
        return None
    if len(candidates) > 1:
        nos = "・".join(f"No.{c.record_id}" for c in candidates)
        raise _Hold(f"配偶者候補が複数います（{nos}）。婚姻関係の確認が必要です",
                    "F6")
    spouse = candidates[0]
    if _has_divorce_with(spouse, decedent) or _has_divorce_with(decedent, spouse):
        ctx["facts"].append(f"No.{spouse.record_id} {spouse.name}: 離婚により"
                            "婚姻解消（相続人でない・890条）")
        return None
    cls = _classify_death(spouse, decedent.death_date)
    if cls == "pre":
        ctx["facts"].append(f"No.{spouse.record_id} {spouse.name}: 被相続人より"
                            "先に死亡（死別・相続人でない）")
        return None
    if cls == "same":
        ctx["flags"].append({"flag": "同時死亡推定", "根拠": "民法32条の2",
                             "内容": f"No.{spouse.record_id} と被相続人の死亡日が"
                                     "同日です（相互に相続しない扱いで仮計算・"
                                     "認定は弁護士）"})
        return None
    if cls == "unknown":
        raise _Hold(f"配偶者 No.{spouse.record_id} {spouse.name} の生死が不明です"
                    "（生死確定または失踪宣告の確認が必要）", "F4")
    if cls == "undated":
        ref = f"（参考: 死亡記載 {spouse.death_wareki}）" if spouse.death_wareki else ""
        raise _Hold(f"配偶者 No.{spouse.record_id} {spouse.name} は死亡確定だが"
                    f"死亡日が未確定で先後を判定できません{ref}", "C5")
    return spouse  # alive または post（数次は上位で処理）


# ══════════════════════════════════════════════════════════════
# 順位ごとの相続ライン解決
# ══════════════════════════════════════════════════════════════

@dataclass
class _Line:
    """1株（901条の株分け単位）。entries は (person, zokugara, via, basis追記)"""
    holder_id: str
    entries: list = field(default_factory=list)
    weight: int = 1  # 半血=1・全血=2（第3順位のみ使用）


def _substitute_line(fam: _Family, dead_child: HeirPerson, base_date: str,
                     ctx: dict, depth_label: str, allow_regeneration: bool,
                     decedent_id: str) -> list:
    """代襲ラインの解決（887②③/889②準用）。Returns: entries（空=ライン消滅）"""
    entries = []
    substitutes = fam.children_of(dead_child.record_id)
    live_lines = []
    for sub in substitutes:
        if sub.record_id in fam.decl.renounced:
            ctx["flags"].append({"flag": "F1", "根拠": "民法939条",
                                 "内容": f"No.{sub.record_id} {sub.name} の相続放棄"
                                         "申告（放棄事実の確認は人）"})
            continue
        if sub.born_before_parents_adoption:
            ctx["flags"].append({"flag": "E5", "根拠": "民法887条2項（直系卑属要件）",
                                 "内容": f"No.{sub.record_id} {sub.name} は縁組前"
                                         "出生のため代襲しない扱いで提示"
                                         "（最終判断は弁護士）"})
            continue
        cls = _classify_death(sub, base_date)
        if cls in ("alive", "post"):
            live_lines.append([(sub, depth_label, dead_child.record_id, cls)])
        elif cls == "unknown":
            raise _Hold(f"代襲候補 No.{sub.record_id} {sub.name} の生死が不明です",
                        "F4")
        elif cls == "undated":
            ref = f"（参考: 死亡記載 {sub.death_wareki}）" if sub.death_wareki else ""
            raise _Hold(f"代襲候補 No.{sub.record_id} {sub.name} は死亡確定だが"
                        f"死亡日が未確定です{ref}", "C5")
        else:  # pre / same（先死亡）
            if cls == "same":
                ctx["flags"].append({"flag": "同時死亡推定", "根拠": "民法32条の2",
                                     "内容": f"No.{sub.record_id} の死亡日が基準日"
                                             "と同日（要弁護士）"})
            if not allow_regeneration:
                ctx["facts"].append(
                    f"No.{sub.record_id} {sub.name}: 先死亡だが兄弟姉妹系の"
                    "再代襲はない（889条2項は887条3項を準用しない）")
                continue
            deeper = _substitute_line(fam, sub, base_date, ctx,
                                      "再代襲（曾孫等）", True, decedent_id)
            if deeper:
                live_lines.append(deeper)
    if not live_lines:
        return []
    share_entries = []
    for line in live_lines:
        share_entries.append(line)
    return share_entries


def _flatten_lines(nested_lines: list, share: Fraction) -> list:
    """株分け（901条）: 代襲ラインの入れ子を等分で展開する"""
    out = []
    per = share / len(nested_lines)
    for line in nested_lines:
        if isinstance(line, list) and line and isinstance(line[0], tuple):
            for person, label, via, cls in line:
                out.append((person, label, via, cls, per))
        else:
            out.extend(_flatten_lines(line, per))
    return out


def _rank1_lines(fam: _Family, decedent: HeirPerson, ctx: dict) -> list:
    """第1順位（子・887条）: 各子=1株。代襲は887②③・放棄は939・欠格は891"""
    lines = []
    for child in fam.children_of(decedent.record_id):
        if child.record_id in fam.decl.renounced:
            ctx["flags"].append({"flag": "F1", "根拠": "民法939条",
                                 "内容": f"No.{child.record_id} {child.name} の"
                                         "相続放棄申告（除外・代襲しない。"
                                         "放棄事実の確認は人）"})
            ctx["facts"].append(f"No.{child.record_id} {child.name}: 放棄により"
                                "除外・その子は代襲しない（939条・887条2項）")
            continue
        if child.record_id in fam.decl.disqualified:
            ctx["flags"].append({"flag": "F2", "根拠": "民法891条",
                                 "内容": f"No.{child.record_id} {child.name} の"
                                         "欠格・廃除申告（除外・代襲あり。"
                                         "認定は人）"})
            nested = _substitute_line(fam, child, decedent.death_date, ctx,
                                      "孫（代襲）", True, decedent.record_id)
            if nested:
                lines.append(_Line(holder_id=child.record_id,
                                   entries=nested))
            continue
        cls = _classify_death(child, decedent.death_date)
        if cls in ("alive", "post"):
            lines.append(_Line(holder_id=child.record_id,
                               entries=[[(child, "子", "", cls)]]))
        elif cls == "unknown":
            raise _Hold(f"子 No.{child.record_id} {child.name} の生死が不明です"
                        "（生死確定または失踪宣告の確認が必要）", "F4")
        elif cls == "undated":
            ref = f"（参考: 死亡記載 {child.death_wareki}）" if child.death_wareki else ""
            raise _Hold(f"子 No.{child.record_id} {child.name} は死亡確定だが"
                        f"死亡日が未確定で代襲/数次を峻別できません{ref}", "C5")
        else:  # pre / same
            if cls == "same":
                ctx["flags"].append({"flag": "同時死亡推定", "根拠": "民法32条の2",
                                     "内容": f"No.{child.record_id} {child.name} と"
                                             "被相続人の死亡日が同日です（相互に"
                                             "相続しない＋代襲は生じる扱いで仮計算・"
                                             "認定は弁護士）"})
            ctx["facts"].append(f"No.{child.record_id} {child.name}: 先死亡"
                                f"（死亡日 {child.death_date or '同日'}）→ 代襲"
                                "（887条2項）")
            nested = _substitute_line(fam, child, decedent.death_date, ctx,
                                      "孫（代襲）", True, decedent.record_id)
            if nested:
                lines.append(_Line(holder_id=child.record_id, entries=nested))
    for label in fam.decl.fetuses:
        ctx["flags"].append({"flag": "F3", "根拠": "民法886条",
                             "内容": f"胎児（{label}）を既に生まれたものとみなして"
                                     "提示（確定は出生後=人）"})
        fetus = HeirPerson(record_id=f"胎児:{label}", name=f"胎児（{label}）")
        lines.append(_Line(holder_id=fetus.record_id,
                           entries=[[(fetus, "胎児", "", "alive")]]))
    return lines


def _rank2_members(fam: _Family, decedent: HeirPerson, ctx: dict) -> list:
    """第2順位（直系尊属・889①一）: 親等の近い世代の生存者のみ（但書）"""
    generation = [fam.by_id[i] for i in fam.effective_parent_ids(decedent)
                  if i in fam.by_id]
    while generation:
        selected = []
        for anc in generation:
            cls = _classify_death(anc, decedent.death_date)
            if cls in ("alive", "post"):
                selected.append((anc, cls))
            elif cls == "unknown":
                raise _Hold(f"直系尊属 No.{anc.record_id} {anc.name} の生死が"
                            "不明です", "F4")
            elif cls == "undated":
                raise _Hold(f"直系尊属 No.{anc.record_id} {anc.name} は死亡確定"
                            "だが死亡日が未確定です", "C5")
        if selected:
            skipped = [a for a in generation
                       if a.record_id not in {s.record_id for s, _ in selected}]
            for s in skipped:
                ctx["facts"].append(f"No.{s.record_id} {s.name}: 先死亡のため"
                                    "尊属から除外（尊属に代襲なし・親等優先は"
                                    "889条1項1号但書）")
            return selected
        next_gen = []
        for anc in generation:
            next_gen += [fam.by_id[i] for i in fam.effective_parent_ids(anc)
                         if i in fam.by_id]
        generation = next_gen
    return []


def _rank3_lines(fam: _Family, decedent: HeirPerson, ctx: dict) -> list:
    """第3順位（兄弟姉妹・889①二）: 全血=2・半血=1（900④但書）・代襲は一代限り"""
    dec_parents = set(fam.effective_parent_ids(decedent))
    if not dec_parents:
        return []
    lines = []
    for p in fam.by_id.values():
        if p.record_id == decedent.record_id:
            continue
        p_parents = set(fam.effective_parent_ids(p))
        shared = dec_parents & p_parents
        if not shared:
            continue
        # 全血/半血の判定（D5: 片親のエッジ欠落は判定不能=保留）
        if len(shared) >= 2:
            weight = 2
        else:
            if len(dec_parents) < 2 or len(p_parents) < 2:
                raise _Hold(f"兄弟姉妹 No.{p.record_id} {p.name} の全血/半血が"
                            "判定できません（親エッジの欠落。父母双方の人物ID"
                            "の充足が必要）", "D5")
            weight = 1
        if p.record_id in fam.decl.renounced:
            ctx["flags"].append({"flag": "F1", "根拠": "民法939条",
                                 "内容": f"No.{p.record_id} {p.name} の相続放棄"
                                         "申告（除外・代襲しない）"})
            continue
        cls = _classify_death(p, decedent.death_date)
        if cls in ("alive", "post"):
            lines.append(_Line(holder_id=p.record_id, weight=weight,
                               entries=[[(p, "兄弟姉妹", "", cls)]]))
        elif cls == "unknown":
            raise _Hold(f"兄弟姉妹 No.{p.record_id} {p.name} の生死が不明です",
                        "F4")
        elif cls == "undated":
            raise _Hold(f"兄弟姉妹 No.{p.record_id} {p.name} は死亡確定だが"
                        "死亡日が未確定です", "C5")
        else:
            nested = _substitute_line(fam, p, decedent.death_date, ctx,
                                      "甥姪（代襲）", False,  # 一代限り
                                      decedent.record_id)
            if nested:
                lines.append(_Line(holder_id=p.record_id, weight=weight,
                                   entries=nested))
            else:
                ctx["facts"].append(f"No.{p.record_id} {p.name}: 先死亡・代襲者"
                                    "なし（兄弟系の再代襲なし=889条2項）")
    return lines


# ══════════════════════════════════════════════════════════════
# 導出の本体
# ══════════════════════════════════════════════════════════════

_SPOUSE_SHARE = {1: Fraction(1, 2), 2: Fraction(2, 3), 3: Fraction(3, 4)}
_RANK_BASIS = {1: ["民法887条1項", "民法900条1号"],
               2: ["民法889条1項1号", "民法900条2号"],
               3: ["民法889条1項2号", "民法900条3号"]}


def derive_heirs(persons: list[HeirPerson],
                 declarations: Declarations | None = None,
                 kosekis: list[dict] | None = None,
                 decedent_id: str = "",
                 at_date: str = "") -> Derivation:
    """相続人候補の導出（提示のみ・書き込みゼロ）。

    - persons: HeirPerson の列（App 34 からは persons_from_records で変換）
    - declarations: 申告事項（放棄・欠格廃除・胎児・養子区分）
    - kosekis: App 33 読解済み戸籍（省略可。与えられた場合のみ第3順位で
      収集見込みを検査し、不足があれば F5 保留）
    - decedent_id/at_date: 数次の再帰用（通常は被相続人フラグ=yes を起点）
    """
    decl = declarations or Declarations()
    fam = _Family(persons, decl)
    ctx = {"flags": [], "facts": [], "unused": [], "used": set()}

    # ── 被相続人の特定（負側: 0名/複数名は error） ──────────────────────────
    if decedent_id:
        decedent = fam.by_id.get(decedent_id)
        if decedent is None:
            return Derivation(status="error",
                              hold_reasons=[f"人物 No.{decedent_id} が見つかりません"])
    else:
        decedents = [p for p in persons if p.is_decedent]
        if len(decedents) != 1:
            return Derivation(
                status="error",
                hold_reasons=[f"被相続人フラグ=yes が {len(decedents)} 名です"
                              "（1名のみ対応。人物確認語彙で被相続人を特定して"
                              "ください）"])
        decedent = decedents[0]
    base_date = at_date or decedent.death_date
    if decedent.alive != "死亡" or not base_date:
        return Derivation(
            status="held",
            hold_reasons=[f"被相続人 No.{decedent.record_id} {decedent.name} の"
                          "死亡日（DATE）が未確定です（04 §1 前提1）"])
    decedent = _with_date(decedent, base_date)

    try:
        spouse = _resolve_spouse(fam, decedent, ctx)
        rank, lines, members = _resolve_rank(fam, decedent, ctx, kosekis)
    except _Hold as hold:
        flags = list(ctx["flags"])
        if hold.flag:
            flags.append({"flag": hold.flag, "内容": hold.reason})
        return Derivation(status="held", flags=flags,
                          hold_reasons=[hold.reason],
                          unused_facts=ctx["unused"])

    # ── 相続分の割付（900条・Fraction 厳密演算） ────────────────────────────
    heirs: list[HeirCandidate] = []
    blood_total = Fraction(1)
    if spouse is not None and (lines or members):
        spouse_share = _SPOUSE_SHARE[rank]
        blood_total = 1 - spouse_share
    if spouse is not None:
        share = _SPOUSE_SHARE[rank] if (lines or members) else Fraction(1)
        basis = ["民法890条"]
        if lines or members:
            basis.append(_RANK_BASIS[rank][1])
        heirs.append(HeirCandidate(
            person_id=spouse.record_id, name=spouse.name, zokugara="配偶者",
            share=share, basis=basis,
            facts=[f"婚姻関係の相手方照合により配偶者と判定（No.{spouse.record_id}）"]))
    if rank in (1, 3) and lines:
        total_weight = sum(line.weight for line in lines)
        for line in lines:
            line_share = blood_total * line.weight / total_weight
            basis = list(_RANK_BASIS[rank])
            if rank == 3 and line.weight == 1:
                basis.append("民法900条4号但書")
            for person, label, via, cls, share in _flatten_lines(line.entries,
                                                                 line_share):
                b = list(basis)
                if via:
                    b += (["民法889条2項", "民法887条2項"] if rank == 3
                          else ["民法887条2項"])
                    b.append("民法901条")
                    if label.startswith("再代襲"):
                        b.append("民法887条3項")
                    label = "甥姪（代襲）" if rank == 3 else label
                heirs.append(HeirCandidate(
                    person_id=person.record_id, name=person.name,
                    zokugara=label if via else label,
                    share=share, basis=b, via=via,
                    facts=[f"死亡日先後の比較（基準日={base_date}）"]))
                _note_suji(person, cls, ctx)
    elif rank == 2 and members:
        per = blood_total / len(members)
        for anc, cls in members:
            heirs.append(HeirCandidate(
                person_id=anc.record_id, name=anc.name, zokugara="直系尊属",
                share=per, basis=list(_RANK_BASIS[2]),
                facts=[f"親等の近い直系尊属の生存者（基準日={base_date}）"]))
            _note_suji(anc, cls, ctx)

    # ── 数次相続（896条）: 基準日より後に死亡した相続人の地位を承継 ─────────
    heirs, suji_hold = _apply_suji(fam, decl, heirs, base_date, ctx)
    if suji_hold is not None:
        flags = list(ctx["flags"])
        flags.append({"flag": suji_hold.flag or "数次",
                      "内容": suji_hold.reason})
        return Derivation(status="held", flags=flags,
                          hold_reasons=[suji_hold.reason])

    # ── 判定に使えなかった事実（下位順位の人物・G1） ────────────────────────
    used = {h.person_id for h in heirs} | {decedent.record_id} | ctx["used"]
    used |= {h.via for h in heirs if h.via}
    for p in persons:
        if p.record_id not in used and rank and _is_lower_rank_relative(p):
            ctx["unused"].append(
                f"No.{p.record_id} {p.name}: 第{rank}順位の存在が確定したため"
                "生死・続柄を判定に使用していません")

    shares: dict = {}
    for h in heirs:
        shares[h.person_id] = shares.get(h.person_id, Fraction(0)) + h.share
    return Derivation(status="derived", heirs=heirs, shares=shares,
                      flags=ctx["flags"], unused_facts=ctx["unused"], rank=rank)


def _with_date(decedent: HeirPerson, base_date: str) -> HeirPerson:
    if decedent.death_date:
        return decedent
    d = HeirPerson(**{**decedent.__dict__})
    d.death_date = base_date
    return d


def _is_lower_rank_relative(person: HeirPerson) -> bool:
    return not person.is_decedent


def _note_suji(person: HeirPerson, cls: str, ctx: dict) -> None:
    if cls == "post":
        ctx["flags"].append({"flag": "数次相続", "根拠": "民法896条",
                             "内容": f"No.{person.record_id} {person.name} は"
                                     "被相続人の後に死亡（その相続人が地位を承継・"
                                     "遺産分割の当事者確認は弁護士）"})


def _resolve_rank(fam: _Family, decedent: HeirPerson, ctx: dict,
                  kosekis: list[dict] | None) -> tuple:
    lines = _rank1_lines(fam, decedent, ctx)
    if lines:
        return 1, lines, []
    members = _rank2_members(fam, decedent, ctx)
    if members:
        return 2, [], members
    # ── 第3順位に入る前に収集見込みを検査（F5・kosekis が与えられた場合のみ）──
    if kosekis is not None:
        from koseki_chain import assess_for_rank
        assessment = assess_for_rank(kosekis, rank=3)
        if assessment["未収集"]:
            missing = "・".join(
                f"{m.get('本籍') or '本籍不明'}（筆頭者 {m.get('筆頭者') or '不明'}）"
                for m in assessment["未収集"])
            raise _Hold("兄弟姉妹相続には父母の出生までの戸籍が必要ですが、"
                        f"未収集の従前戸籍があります: {missing}"
                        "（収集見込みは弁護士確認前の参考判定）", "F5")
    lines = _rank3_lines(fam, decedent, ctx)
    if lines:
        return 3, lines, []
    return 0, [], []


def _apply_suji(fam: _Family, decl: Declarations, heirs: list,
                base_date: str, ctx: dict) -> tuple:
    """数次相続の展開: post 死亡の相続人の相続分を、その者の相続人へ再帰配分"""
    out = []
    for h in heirs:
        person = fam.by_id.get(h.person_id)
        if person is None or _classify_death(person, base_date) != "post":
            out.append(h)
            continue
        sub = derive_heirs(list(fam.by_id.values()), decl,
                           decedent_id=person.record_id,
                           at_date=person.death_date)
        if sub.status != "derived":
            return heirs, _Hold(
                f"数次相続（No.{person.record_id} {person.name}）の展開で保留: "
                + "；".join(sub.hold_reasons), "数次")
        for sh in sub.heirs:
            out.append(HeirCandidate(
                person_id=sh.person_id, name=sh.name,
                zokugara=f"数次承継（No.{person.record_id} {person.name} の"
                         f"{sh.zokugara}）",
                share=h.share * sh.share,
                basis=list(dict.fromkeys(h.basis + ["民法896条"] + sh.basis)),
                facts=sh.facts, via=person.record_id))
        ctx["flags"].extend(f for f in sub.flags
                            if f not in ctx["flags"])
    return out, None


# ══════════════════════════════════════════════════════════════
# Z1 ゲート絞り込み（D-5 裁定・required_persons）
# ══════════════════════════════════════════════════════════════

def required_persons(graph, decedent) -> set:
    """相続人確定に必要な人物IDのみ返す（Z1 検証要求の絞り込み）。

    被相続人起点: 配偶者候補（婚姻エッジの相手）＋確定した順位の血族＋代襲経路。
    上位順位の存在（当該人物ノードの存在）が確定した時点で下位順位は含めない。
    graph: kinship_graph.KinshipGraph / decedent: PersonNode
    """
    children_of = defaultdict(list)
    parents_of = defaultdict(list)
    spouses_of = defaultdict(list)
    for e in graph.edges:
        if e.kind in ("親子", "養親子"):
            children_of[e.a].append(e.b)
            parents_of[e.b].append(e.a)
        elif e.kind == "婚姻":
            spouses_of[e.a].append(e.b)
            spouses_of[e.b].append(e.a)

    did = decedent.record_id
    ids = {did}
    ids.update(spouses_of[did])  # 配偶者候補（離婚等の確定は人）

    def descend(person_id: str, one_generation: bool = False) -> None:
        for child_id in children_of[person_id]:
            ids.add(child_id)
            node = graph.node(child_id)
            if node is not None and node.alive != "生存":
                # 代襲経路: 生存確定でない子の卑属は確認が必要
                if not one_generation:
                    descend(child_id)
                else:
                    ids.update(children_of[child_id])

    if children_of[did]:
        descend(did)  # 第1順位確定 → 尊属・兄弟は要求しない
        return ids
    generation = list(parents_of[did])
    while generation:
        ids.update(generation)
        living = [g for g in generation
                  if (graph.node(g) is not None
                      and graph.node(g).alive == "生存")]
        if living:
            return ids  # 第2順位確定
        generation = [p for g in generation for p in parents_of[g]]
    # 第3順位: 兄弟姉妹（親エッジ共有）＋甥姪（一代）
    for parent_id in parents_of[did]:
        for sibling_id in children_of[parent_id]:
            if sibling_id == did:
                continue
            ids.add(sibling_id)
            node = graph.node(sibling_id)
            if node is not None and node.alive != "生存":
                ids.update(children_of[sibling_id])
    return ids
