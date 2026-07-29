"""derivation_models — App36 導出台帳（DerivationRun／HeirConfirmationDecision）

正本: `docs/design-drafts/DRAFT_APP36_DERIVATION_APP37_TEMPLATE_REGISTRY.md` §2（P3-001）。
- **NH01（純粋 immutable 分離）**: DerivationRun は機械の導出事実のみを持つ
  純粋 immutable レコード。**human_state／decided_by／decided_at は持たない**。
  人の確定は HeirConfirmationDecision（追記のみ）へ分離し、§9.21 の human_state 等は
  「run＋最新 decision の join projection」として読む（テーブルに冗長保持しない）。
- **【裁定済み・2026-07-12 司令塔】専用モジュールの別 metadata**
  （inbound_event.Base 相乗りせず・L03 準拠）。alembic target_metadata へ統合。
- **UPDATE/DELETE 拒否**は二重で強制する:
  (i) ORM 層: before_update/before_delete listener が ImmutableRecordError を送出
  (ii) DB 層: BEFORE UPDATE/DELETE trigger（metadata.create_all にも migration にも付与・
      Core 文/外部クライアントからの変更も拒否）
  訂正・再導出は新行＋supersedes_*_id の連鎖のみ（連鎖の一意性は UNIQUE で担保）。
"""

import re
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


class ImmutableRecordError(RuntimeError):
    """immutable 台帳（derivation_run／heir_confirmation_decision）への UPDATE/DELETE。"""


class PayloadPolicyError(ValueError):
    """result_payload/lawyer_flags の schema allowlist・PII 防御違反（§3.5・fix1 H02）。
    immutable 台帳への誤保存は訂正不能のため、保存**前**の入口で拒否する。"""


class ChainIntegrityError(ValueError):
    """supersedes 連鎖の健全性違反（自己参照・cross-case・多重 head 等・fix1 H01）。"""


class DerivationBase(DeclarativeBase):
    """app-state DB 専用 metadata（L03: inbound_event.Base と相乗りしない）。"""


class DerivationRun(DerivationBase):
    """§2.1: 機械の導出事実のみ・純粋 immutable。"""

    __tablename__ = "derivation_run"
    __table_args__ = (
        sa.CheckConstraint("status IN ('derived', 'held', 'error')",
                           name="ck_derivation_run_status"),
        # fix1 M01: rank の語彙固定（1/2/3 順位・0=該当なし）
        sa.CheckConstraint("rank IN (0, 1, 2, 3)", name="ck_derivation_run_rank"),
        # fix1 H01: 自己参照の DB レベル拒否
        sa.CheckConstraint("supersedes_run_id IS NULL OR supersedes_run_id != id",
                           name="ck_derivation_run_no_self_supersede"),
        # 再導出連鎖の一意性: 1 つの旧 run を置き換えられる新 run は 1 つだけ
        sa.UniqueConstraint("supersedes_run_id", name="uq_derivation_run_supersedes"),
        # fix2 H03: 同一 case の root（supersedes 無し）は 1 行のみ（部分ユニーク・
        # 両 dialect）。supersedes UNIQUE と合わせて「連鎖は単一 root からの一本鎖」
        # ＝head 一意性を DB レベルで担保（並行初回作成の競合も遮断）
        sa.Index("uq_derivation_run_single_root", "case_record_id", unique=True,
                 sqlite_where=sa.text("supersedes_run_id IS NULL"),
                 postgresql_where=sa.text("supersedes_run_id IS NULL")),
    )

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    case_app_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    case_record_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    decedent_person_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    at_date: Mapped[str] = mapped_column(sa.Text, nullable=False)   # 相続開始日（確定西暦）
    frozen_case_version: Mapped[str] = mapped_column(sa.Text, nullable=False)
    input_person_revisions: Mapped[dict] = mapped_column(_JSON, nullable=False)
    input_person_ids: Mapped[list] = mapped_column(_JSON, nullable=False)
    input_hash: Mapped[str] = mapped_column(sa.Text, nullable=False)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)    # derived/held/error
    rank: Mapped[int] = mapped_column(sa.Integer, nullable=False)   # 1/2/3/0
    result_payload: Mapped[dict] = mapped_column(_JSON, nullable=False)  # person_id のみ（§4）
    result_hash: Mapped[str] = mapped_column(sa.Text, nullable=False)
    lawyer_flags: Mapped[dict | None] = mapped_column(_JSON)
    provisional: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)
    supersedes_run_id: Mapped[int | None] = mapped_column(
        sa.BigInteger, sa.ForeignKey("derivation_run.id"))
    engine_version: Mapped[str] = mapped_column(sa.Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())


class HeirConfirmationDecision(DerivationBase):
    """§2.2: 人の確定・追記のみ（訂正は新 decision＋supersedes_decision_id）。"""

    __tablename__ = "heir_confirmation_decision"
    __table_args__ = (
        sa.CheckConstraint("decision IN ('confirmed', 'held', 'rejected')",
                           name="ck_heir_decision_decision"),
        # fix2 H04: 自己参照の DB レベル拒否（run 側と同型）
        sa.CheckConstraint(
            "supersedes_decision_id IS NULL OR supersedes_decision_id != id",
            name="ck_heir_decision_no_self_supersede"),
        sa.UniqueConstraint("supersedes_decision_id", name="uq_heir_decision_supersedes"),
        # fix3 H04: 同一 run の root decision（supersedes 無し）は 1 行のみ
        # （run 側 single-root と同型・decision 連鎖も単一 root からの一本鎖になる）
        sa.Index("uq_heir_decision_single_root", "derivation_run_id", unique=True,
                 sqlite_where=sa.text("supersedes_decision_id IS NULL"),
                 postgresql_where=sa.text("supersedes_decision_id IS NULL")),
    )

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    derivation_run_id: Mapped[int] = mapped_column(
        sa.BigInteger, sa.ForeignKey("derivation_run.id"), nullable=False)
    decision: Mapped[str] = mapped_column(sa.Text, nullable=False)
    decided_by: Mapped[str] = mapped_column(sa.Text, nullable=False)
    decided_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False)
    amendments: Mapped[dict | None] = mapped_column(_JSON)   # 正本 §2.2「修正内容」（監査用）
    supersedes_decision_id: Mapped[int | None] = mapped_column(
        sa.BigInteger, sa.ForeignKey("heir_confirmation_decision.id"))


# ── (i) ORM 層の immutable 強制 ──────────────────────────────────────────────

def _reject_mutation(mapper, connection, target):  # noqa: ARG001
    raise ImmutableRecordError(
        f"{target.__tablename__} is append-only (UPDATE/DELETE 禁止・"
        "訂正は新行＋supersedes 連鎖で行う)")


for _cls in (DerivationRun, HeirConfirmationDecision):
    sa.event.listen(_cls, "before_update", _reject_mutation)
    sa.event.listen(_cls, "before_delete", _reject_mutation)


# ── (ii) DB 層の immutable 強制（trigger・create_all/migration 双方に付与） ──

def immutable_trigger_ddl(table: str) -> dict[str, list[str]]:
    """dialect 別の BEFORE UPDATE/DELETE 拒否 trigger DDL（migration からも共用）。"""
    return {
        "sqlite": [
            f"CREATE TRIGGER trg_{table}_no_update BEFORE UPDATE ON {table} "
            f"BEGIN SELECT RAISE(ABORT, '{table} is immutable'); END",
            f"CREATE TRIGGER trg_{table}_no_delete BEFORE DELETE ON {table} "
            f"BEGIN SELECT RAISE(ABORT, '{table} is immutable'); END",
        ],
        "postgresql": [
            f"CREATE OR REPLACE FUNCTION {table}_immutable() RETURNS trigger AS $$ "
            f"BEGIN RAISE EXCEPTION '{table} is immutable'; END; $$ LANGUAGE plpgsql",
            f"CREATE TRIGGER trg_{table}_no_mutation BEFORE UPDATE OR DELETE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION {table}_immutable()",
        ],
    }


for _table in ("derivation_run", "heir_confirmation_decision"):
    _ddl = immutable_trigger_ddl(_table)
    _tbl = DerivationBase.metadata.tables[_table]
    for _stmt in _ddl["sqlite"]:
        sa.event.listen(_tbl, "after_create", sa.DDL(_stmt).execute_if(dialect="sqlite"))
    for _stmt in _ddl["postgresql"]:
        sa.event.listen(_tbl, "after_create",
                        sa.DDL(_stmt).execute_if(dialect="postgresql"))


# ══════════════════════════════════════════════════════════════
# fix1 H02: result_payload / lawyer_flags の schema allowlist（§3.5・氏名非保持）
# ══════════════════════════════════════════════════════════════
# 正本 §3.5: result_payload は person_id のみ（氏名・住所・生年月日を保持しない）。
# facts は導出根拠の最小限（条文キー等）。許可キー以外は保存拒否（PII 混入を構造で防ぐ）。
# immutable 台帳のため誤保存は訂正不能 → ORM 層＋リポジトリ関数の**入口**で拒否する。
# 許可集合の拡張は正本 §3.5 の改定と同時にのみ行う。

_RESULT_TOP_KEYS = frozenset({"heirs", "facts"})
# P3-001 改定票（2026-07-30・P3-003B_DESIGN 裁定1）: heirs 行へ zokugara_code
# （続柄区分コード・固定9値 ASCII enum）を追加。改定前 run の payload はこのキーを
# 持たない＝版判別は payload_has_zokugara_codes（コード欠落=精密 projection 不可・
# 要確認扱い。P3-003B §3.2「旧 run」）。
_RESULT_HEIR_KEYS = frozenset({"person_id", "share", "relation_key", "zokugara_code"})
_LAWYER_FLAGS_KEYS = frozenset({"flags"})

# ── fix2 H01→fix3 改訂→fix4 H02: field 別 grammar/enum（自由文字列 field を残さない）──
# person_id:
#   (a) App34 kintone `$id`（heir_derivation.py:122 `record_id=v("$id")`＝数字列）
#   (b) 胎児合成 ID `胎児:F{n}`（同一 run 内の出現順連番）
# 【fix4 H02 司令塔裁定・収載】胎児 ID は「役割語の自由文字列」を保存しない。
#   導出器（凍結）の出力 `胎児:{label}`（label＝Declarations.fetuses の自由入力
#   表示ラベル＝役割語）は変えず、**保存層の変換（build_run_payload）で非PII
#   合成識別子 胎児:F1・胎児:F2…（出現順連番）へ写像**して吸収する。
#   元ラベルとの対応は保存しない（表示が必要なら run 内で再導出可能）。
#   role語 enum 方式は採らない（fetuses が自由入力である以上 enum は実データで割れる）。
# 【fix5 M01 司令塔裁定・収載】胎児連番は validate 時に**契約強制**する:
#   F1 起点・正整数・連続・重複なし（run 内の胎児 ID 集合が {F1..Fn} と完全一致）。
#   F0・先頭ゼロ（F01 等）・欠番・重複はいずれも拒否。
#   build_run_payload は**出現ごとに採番**する（同一ラベルが2回出現しても別番号。
#   fix4 の辞書写像＝同一ラベル同一番号は、重複 ID の温床となるため解消）。
_PERSON_ID_RE = re.compile(r"^[0-9]{1,10}$")
_FETUS_ID_RE = re.compile(r"^胎児:F[1-9][0-9]*$")   # fix5: F0・先頭ゼロも構文で拒否
# share: 分数の固定文法のみ（engine は Fraction。全部相続は "1/1"）
_SHARE_RE = re.compile(r"^[0-9]{1,6}/[1-9][0-9]{0,5}$")
# relation_key: ASCII enum（zokugara 9 区分の写像・_ZOKUGARA_TO_RELATION が単一の正）
# 公開 read-only 定数（RELATION_KEYS）。他 module（clause_library 等）はこの語彙を
# 「保存語彙の単一の正」として import する。frozenset＝不変型（再代入・変更不可）。
RELATION_KEYS = frozenset({
    "spouse", "child", "fetus", "lineal_ascendant", "sibling",
    "representative", "successive"})
_RELATION_KEYS = RELATION_KEYS   # 後方互換 alias（旧 private 名・同一オブジェクト）
# facts: 条文キー enum のみ。heir_derivation.py が用いる根拠条文 17 種の
# ASCII 写像（_BASIS_TO_FACT が単一の正）。拡張は正本改定と同時。
_BASIS_TO_FACT = {
    "民法32条の2": "minpo_32_2", "民法886条": "minpo_886",
    "民法887条1項": "minpo_887_1", "民法887条2項": "minpo_887_2",
    "民法887条3項": "minpo_887_3", "民法889条1項1号": "minpo_889_1_1",
    "民法889条1項2号": "minpo_889_1_2", "民法889条2項": "minpo_889_2",
    "民法890条": "minpo_890", "民法891条": "minpo_891", "民法896条": "minpo_896",
    "民法900条1号": "minpo_900_1", "民法900条2号": "minpo_900_2",
    "民法900条3号": "minpo_900_3", "民法900条4号但書": "minpo_900_4_proviso",
    "民法901条": "minpo_901", "民法939条": "minpo_939",
}
_FACT_KEYS = frozenset(_BASIS_TO_FACT.values())
# lawyer_flags.flags（fix3 M01: 実導出の全 flag 種と全数一致・provisional へ潰さない）:
#   英数コード F1〜F5/C5/D5/E4/E5（heir_derivation の flag リテラル全数）＋
#   日本語 flag 3 種の ASCII 写像（_FLAG_TO_KEY が単一の正）
_FLAG_TO_KEY = {
    "同時死亡推定": "simultaneous_death",
    "数次相続": "successive_inheritance",
    "数次": "successive_hold",
}
_FLAG_CODE_RE = re.compile(r"^[A-Z][0-9]$")
# 公開 read-only 定数（LAWYER_FLAG_KEYS）。frozenset＝不変型。
LAWYER_FLAG_KEYS = frozenset(
    {"F1", "F2", "F3", "F4", "F5", "F6", "C5", "D5", "E4", "E5"}
    | set(_FLAG_TO_KEY.values()))
_LAWYER_FLAG_KEYS = LAWYER_FLAG_KEYS   # 後方互換 alias（旧 private 名・同一オブジェクト）
# zokugara（App36 続柄区分・heir_derivation の生成 9 区分）→ relation_key
_ZOKUGARA_TO_RELATION = {
    "配偶者": "spouse", "子": "child", "胎児": "fetus",
    "孫（代襲）": "representative", "甥姪（代襲）": "representative",
    "再代襲（曾孫等）": "representative",
    "直系尊属": "lineal_ascendant", "兄弟姉妹": "sibling",
}
# ── P3-001 改定票（P3-003B_DESIGN 裁定1・§3.1/§3.2）: 続柄区分コード ──────────
# relation_key は representative が 孫（代襲）/甥姪（代襲）/再代襲 を collapse する
# ため App36 続柄への total 写像が成立しない。zokugara と 1:1 の固定9値 ASCII enum
# を heirs 行へ併存保存する（値は P3-003B §3.1 表・§3.2 の凍結9値と逐語一致）。
# 取扱い契約（§3.2 M03）: enum 閉集合（enum 外は PayloadPolicyError で保存拒否）・
# 最小化（person_id と結合した続柄は必要範囲=続柄写像を超えて保持・流通させない。
# PII とは断定しない）・非露出（値をログ・例外文言・業務通知へ出さない）。
# 公開 read-only 定数（ZOKUGARA_CODES）: P3-003b 実装票の enum⇔dropdown 写像が
# この語彙を単一の正として import する。frozenset＝不変型。拡張は正本改定と同時。
ZOKUGARA_CODES = frozenset({
    "spouse", "child", "lineal_ascendant", "sibling", "nephew_niece_rep",
    "grandchild_rep", "further_rep", "fetus", "successive"})
_ZOKUGARA_CODES = ZOKUGARA_CODES   # module 内参照用 alias（RELATION_KEYS と同型）
# zokugara → 続柄区分コード（§3.1 表の total 写像・_ZOKUGARA_TO_RELATION と同型）
_ZOKUGARA_TO_CODE = {
    "配偶者": "spouse", "子": "child", "胎児": "fetus",
    "孫（代襲）": "grandchild_rep", "甥姪（代襲）": "nephew_niece_rep",
    "再代襲（曾孫等）": "further_rep",
    "直系尊属": "lineal_ascendant", "兄弟姉妹": "sibling",
}
# 続柄区分コード → relation_key（§3.1 表の collapse 方向・両キー併存時の整合検査用）
_CODE_TO_RELATION = {
    "spouse": "spouse", "child": "child", "fetus": "fetus",
    "lineal_ascendant": "lineal_ascendant", "sibling": "sibling",
    "nephew_niece_rep": "representative", "grandchild_rep": "representative",
    "further_rep": "representative", "successive": "successive",
}


def flag_key(flag) -> str:
    """導出器の flag（日本語含む）→ ASCII enum への単一変換（fix3 M01）。"""
    if flag in _FLAG_TO_KEY:
        return _FLAG_TO_KEY[flag]
    if isinstance(flag, str) and _FLAG_CODE_RE.fullmatch(flag):
        return flag
    raise PayloadPolicyError("未知の lawyer flag: 写像に無い（拡張は正本改定と同時）")


def fact_key(basis: str) -> str:
    """導出器の根拠条文（日本語）→ 条文キー enum への単一変換（fix3 M01）。"""
    try:
        return _BASIS_TO_FACT[basis]
    except KeyError:
        raise PayloadPolicyError("未知の根拠条文: 写像に無い（拡張は正本改定と同時）")


def relation_key_of(zokugara) -> str:
    """zokugara → relation_key の単一変換。数次承継（No.… の …）は前方一致。"""
    if isinstance(zokugara, str) and zokugara.startswith("数次承継"):
        return "successive"
    try:
        return _ZOKUGARA_TO_RELATION[zokugara]
    except (KeyError, TypeError):
        raise PayloadPolicyError("未知の zokugara: 写像に無い（拡張は正本改定と同時）")


def zokugara_code_of(zokugara) -> str:
    """zokugara → 続柄区分コードの単一変換（P3-001 改定票・relation_key_of と同型）。
    数次承継（No.… の …）は前方一致。例外文言に zokugara の値は載せない（非露出）。"""
    if isinstance(zokugara, str) and zokugara.startswith("数次承継"):
        return "successive"
    try:
        return _ZOKUGARA_TO_CODE[zokugara]
    except (KeyError, TypeError):
        raise PayloadPolicyError("未知の zokugara: 写像に無い（拡張は正本改定と同時）")


def payload_has_zokugara_codes(payload) -> bool:
    """版判別（P3-001 改定票・P3-003B §3.2「旧 run」）: 全 heirs 行が zokugara_code
    を持てば True＝改定後 payload（精密 projection 可）。コード欠落行があれば False
    ＝改定前 run 相当（精密 projection 不可・要確認扱い。粗い relation_key 写像に
    頼らない）。heirs が空なら写像すべき行が無いため True。dict/list 構造でない
    payload は False（安全側＝要確認）。読み取り専用・保存は行わない。"""
    if not isinstance(payload, dict):
        return False
    heirs = payload.get("heirs")
    if not isinstance(heirs, list):
        return False
    return all(isinstance(h, dict) and "zokugara_code" in h for h in heirs)


def build_run_payload(derivation) -> tuple[dict, dict | None]:
    """Derivation（heir_derivation.derive_heirs の結果）→ §3.5 準拠 payload への
    単一変換（fix3 接続点）。氏名・日本語文はここで**落ちる**（保存されるのは
    person_id/share/relation_key/条文キー/flag コードのみ）。
    fix4 H02（裁定）: 導出器出力の胎児 ID `胎児:{label}` は、ここで非PII 合成識別子
    `胎児:F{n}`（同一 run 内の出現順連番）へ写像する。元ラベルは保存しない。
    fix5 M01（裁定）: 採番は**出現ごと**（同一ラベルが2回出現しても別番号＝F1/F2。
    辞書写像だと fetuses=["妻","妻"] が同一 ID に潰れ重複の温床になるため解消）。"""
    heirs = []
    facts: list[str] = []
    fetus_count = 0                    # 出現順連番（run 内のみ・ラベル対応は保存しない）

    def _synth_pid(pid):
        nonlocal fetus_count
        if not (isinstance(pid, str) and pid.startswith("胎児:")):
            return pid
        fetus_count += 1
        return f"胎児:F{fetus_count}"

    for h in derivation.heirs:
        entry = {"person_id": _synth_pid(h.person_id),
                 "relation_key": relation_key_of(h.zokugara),
                 "zokugara_code": zokugara_code_of(h.zokugara)}
        if h.share is not None:
            entry["share"] = f"{h.share.numerator}/{h.share.denominator}"
        heirs.append(entry)
        for b in h.basis:
            k = fact_key(b)
            if k not in facts:
                facts.append(k)
    payload = {"heirs": heirs, "facts": facts}
    flag_keys: list[str] = []
    for f in derivation.flags:
        k = flag_key(f.get("flag", "") if isinstance(f, dict) else f)
        if k not in flag_keys:
            flag_keys.append(k)
    lawyer_flags = {"flags": flag_keys} if flag_keys else None
    return payload, lawyer_flags


def _check_enum(value, allowed: frozenset, where: str) -> None:
    if not isinstance(value, str) or value not in allowed:
        raise PayloadPolicyError(f"{where}: enum 外の値は保存不可（§3.5・fix2 grammar）")


def _check_re(value, rx: re.Pattern, where: str) -> None:
    if not isinstance(value, str) or not rx.fullmatch(value):
        raise PayloadPolicyError(f"{where}: 固定文法に合致しない値は保存不可（fix2 grammar）")


def validate_result_payload(payload) -> None:
    """§3.5 allowlist＋fix2 field 別 grammar。違反は PayloadPolicyError（保存前）。"""
    if not isinstance(payload, dict):
        raise PayloadPolicyError("result_payload は dict であること")
    extra = set(payload) - _RESULT_TOP_KEYS
    if extra:
        raise PayloadPolicyError(f"result_payload: 許可キー外 {sorted(extra)}（§3.5）")
    heirs = payload.get("heirs", [])
    if not isinstance(heirs, list):
        raise PayloadPolicyError("result_payload.heirs は list であること")
    for h in heirs:
        if not isinstance(h, dict):
            raise PayloadPolicyError("result_payload.heirs[*] は dict であること")
        extra = set(h) - _RESULT_HEIR_KEYS
        if extra:
            raise PayloadPolicyError(
                f"result_payload.heirs[*]: 許可キー外 {sorted(extra)}（person_id 系のみ）")
        pid = h.get("person_id")
        if not isinstance(pid, str) or not (_PERSON_ID_RE.fullmatch(pid)
                                            or _FETUS_ID_RE.fullmatch(pid)):
            raise PayloadPolicyError(
                "result_payload.heirs[*].person_id: App34 $id（数字列）または"
                "胎児合成 ID（胎児:F{n}・出現順連番）のみ（fix4 H02 裁定・"
                "役割語の自由文字列は保存不可）")
        if "share" in h:
            _check_re(h["share"], _SHARE_RE, "result_payload.heirs[*].share")
        if "relation_key" in h:
            _check_enum(h["relation_key"], RELATION_KEYS,
                        "result_payload.heirs[*].relation_key")
        if "zokugara_code" in h:
            _check_enum(h["zokugara_code"], ZOKUGARA_CODES,
                        "result_payload.heirs[*].zokugara_code")
            # 両キー併存時は §3.1 表の collapse 整合を強制（矛盾 payload は
            # immutable 台帳へ入れない。値は例外文言に載せない=非露出）
            if "relation_key" in h and \
                    _CODE_TO_RELATION[h["zokugara_code"]] != h["relation_key"]:
                raise PayloadPolicyError(
                    "result_payload.heirs[*]: zokugara_code と relation_key が"
                    "写像表（P3-003B §3.1）で整合しない（保存不可）")
    # fix5 M01（裁定・契約強制）: run 内の胎児 ID 集合は {F1..Fn} と完全一致すること
    # （F1 起点・正整数・連続・重複なし。F0/先頭ゼロは _FETUS_ID_RE が構文で拒否済み）
    fetus_nums = [int(h["person_id"][len("胎児:F"):]) for h in heirs
                  if isinstance(h, dict) and isinstance(h.get("person_id"), str)
                  and h["person_id"].startswith("胎児:")]
    if fetus_nums:
        if len(set(fetus_nums)) != len(fetus_nums):
            raise PayloadPolicyError(
                "result_payload: 胎児合成 ID の重複（run 内で一意であること・fix5 M01）")
        if set(fetus_nums) != set(range(1, len(fetus_nums) + 1)):
            raise PayloadPolicyError(
                "result_payload: 胎児合成 ID は F1 起点の連番であること"
                "（欠番・非 F1 起点は保存不可・fix5 M01）")
    facts = payload.get("facts", [])
    if not isinstance(facts, list):
        raise PayloadPolicyError("result_payload.facts は list であること")
    for f in facts:
        if not isinstance(f, str):   # dict/数値等の型混入も拒否（fix2）
            raise PayloadPolicyError("result_payload.facts[*] は条文キー文字列のみ")
        _check_enum(f, _FACT_KEYS, "result_payload.facts[*]")


def validate_lawyer_flags(flags) -> None:
    if flags is None:
        return
    if not isinstance(flags, dict):
        raise PayloadPolicyError("lawyer_flags は dict/None であること")
    extra = set(flags) - _LAWYER_FLAGS_KEYS
    if extra:
        raise PayloadPolicyError(f"lawyer_flags: 許可キー外 {sorted(extra)}")
    values = flags.get("flags", [])
    if not isinstance(values, list):
        raise PayloadPolicyError("lawyer_flags.flags は list であること")
    for v in values:
        _check_enum(v, LAWYER_FLAG_KEYS, "lawyer_flags.flags[*]")


def _validate_run_payloads_orm(mapper, connection, target):  # noqa: ARG001
    validate_result_payload(target.result_payload)
    validate_lawyer_flags(target.lawyer_flags)


sa.event.listen(DerivationRun, "before_insert", _validate_run_payloads_orm)


# ══════════════════════════════════════════════════════════════
# fix1 H01: リポジトリ関数（supersedes 連鎖の健全性＋入口ガードの正規経路）
# ══════════════════════════════════════════════════════════════

async def create_derivation_run(**fields) -> int:
    """DerivationRun 作成の正規経路（アプリ層 guard・fix1）。

    強制する健全性（DB 制約と重畳）:
    - schema allowlist／PII 防御（validate_result_payload／validate_lawyer_flags）
    - supersedes_run_id: 実在・**同一 case**（cross-case 参照拒否）・
      **未 supersede の head のみ**（既に置換済みの run を再置換＝分岐/循環の芽を拒否。
      自己参照は insert 前に id 未確定のため構造的に不可＋DB CHECK でも拒否）
    - **同一 case の head 一意性**: supersedes 無しの新規 run は、その case に
      run が 1 件も無い場合のみ許可（2 本目以降は必ず現 head を supersede する）
    """
    from hub.db import session_scope

    validate_result_payload(fields.get("result_payload"))
    validate_lawyer_flags(fields.get("lawyer_flags"))
    case_record_id = fields.get("case_record_id")
    sup = fields.get("supersedes_run_id")
    t = DerivationRun.__table__
    async with session_scope() as s:
        if sup is not None:
            old = (await s.execute(
                sa.select(t.c.id, t.c.case_record_id)
                .where(t.c.id == sup))).one_or_none()
            if old is None:
                raise ChainIntegrityError(f"supersedes_run_id={sup} は存在しない")
            if old.case_record_id != case_record_id:
                raise ChainIntegrityError(
                    "cross-case の supersede は禁止（同一 case の連鎖のみ）")
            already = (await s.execute(
                sa.select(t.c.id).where(t.c.supersedes_run_id == sup))).first()
            if already is not None:
                raise ChainIntegrityError(
                    f"run {sup} は既に supersede 済み（head のみ置換可・分岐/循環禁止）")
        else:
            exists = (await s.execute(
                sa.select(t.c.id)
                .where(t.c.case_record_id == case_record_id).limit(1))).first()
            if exists is not None:
                raise ChainIntegrityError(
                    "同一 case に run が既に存在（head 一意性・新 run は現 head を "
                    "supersedes_run_id で指すこと)")
        r = await s.execute(sa.insert(t).values(**fields))
        return r.inserted_primary_key[0]


# ══════════════════════════════════════════════════════════════
# fix2 H02→fix3 改定裁定: 入口保証の層別整理（Core 迂回は AST 機械検査で防御）
# ══════════════════════════════════════════════════════════════
# 【fix3 改定裁定（司令塔）】旧「受容ライン (a)(b)」は撤回。Core 迂回の防御は
# **AST 機械検査**を採用する:
#   (1) DB 制約（正）  — CHECK（status/rank/自己参照）・UNIQUE（supersede 連鎖）・
#       部分ユニーク（single root＝run/decision とも・並行初回作成も遮断）・immutable trigger
#   (2) repository 検査 — create_derivation_run/create_heir_decision（grammar/enum・
#       cross-case/cross-run・head/root 検査）
#   (3) AST 機械検査 — test_p3_core_ast_policy が git 追跡 *.py 全域を走査し、
#       対象 table（derivation_run/heir_confirmation_decision/template_version）への
#       sa.insert/sa.update/sa.delete を正規 module・migration・当該テスト以外で機械禁止
#       （旧「迂回成功の pin テスト」は脆弱性目録になるため削除・Codex 判定採用）。
# JSON payload の trigger 化が SQLite/PG で実用不能である事実、および SQLAlchemy に
# table レベル Core insert event が無い事実は fix2 の検証どおり（AST 検査がその代替）。
# PG 並行実測はデプロイ前推奨回帰として追跡（受容・Codex 同意）。


async def create_heir_decision(**fields) -> int:
    """HeirConfirmationDecision 追記の正規経路（fix2 H04・run 側 guard の横展開)。

    - derivation_run_id: 実在検証
    - supersedes_decision_id: 実在・**同一 run 内**（cross-run 参照拒否）・
      未 supersede のみ（UNIQUE と重畳）。自己参照は DB CHECK でも拒否
    """
    from hub.db import session_scope

    run_id = fields.get("derivation_run_id")
    sup = fields.get("supersedes_decision_id")
    t = HeirConfirmationDecision.__table__
    async with session_scope() as s:
        run = (await s.execute(
            sa.select(DerivationRun.__table__.c.id)
            .where(DerivationRun.__table__.c.id == run_id))).one_or_none()
        if run is None:
            raise ChainIntegrityError(f"derivation_run_id={run_id} は存在しない")
        if sup is not None:
            old = (await s.execute(
                sa.select(t.c.id, t.c.derivation_run_id)
                .where(t.c.id == sup))).one_or_none()
            if old is None:
                raise ChainIntegrityError(f"supersedes_decision_id={sup} は存在しない")
            if old.derivation_run_id != run_id:
                raise ChainIntegrityError(
                    "cross-run の decision supersede は禁止（同一 run 内の連鎖のみ）")
            already = (await s.execute(
                sa.select(t.c.id).where(t.c.supersedes_decision_id == sup))).first()
            if already is not None:
                raise ChainIntegrityError(
                    f"decision {sup} は既に supersede 済み（最新 decision のみ置換可）")
        else:
            # fix3 H04: 同一 run の root decision は 1 行のみ（DB 部分ユニークと重畳）
            root = (await s.execute(
                sa.select(t.c.id)
                .where(t.c.derivation_run_id == run_id,
                       t.c.supersedes_decision_id.is_(None)).limit(1))).first()
            if root is not None:
                raise ChainIntegrityError(
                    "同一 run に root decision が既に存在（訂正は supersedes_decision_id "
                    "で最新 decision を指すこと）")
        r = await s.execute(sa.insert(t).values(**fields))
        return r.inserted_primary_key[0]
