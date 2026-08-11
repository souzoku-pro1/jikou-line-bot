"""webapp_kinship_view — P4-005: 相続人関係図ビュー（read-only API＋画面）

正本: DRAFT_P4_PWA_INVENTORY §2（相続人関係図——App34（人物）＋App33（戸籍）＋
（P3 merge 後）derivation_run／heir_confirmation_decision の projection。検索＋DB
読取。描画は kinship_graph→kinship_renderer 流用＝サーバ側で画像生成→PWA は img
表示が最小構成）＋§5 P4-005（kinship_renderer 流用のサーバ側画像生成 API＋画面。
導出結果の重畳表示は P3 merge 後に追加）。認証は P4-001 の関所（`_gate`）に乗る。

構成（再実装禁止の遵守）:
- グラフ構築・検証 = Z1（kinship_graph）・描画 = Z2（kinship_renderer・
  heir_scope=True＝相続人確定に必要な人物への絞り込みの既存思想を維持）。
  本 module はデータの受け渡しのみ（エンジンのロジックを持たない）。
- 導出結果の重畳 = DerivationRun head（P3-001 正規経路 get_current_head の
  read-only）を**凡例**として返す（SVG 自体は Z2 出力を不改変＝エンジン非改変）。
  head 不在は overlay=None（図のみ・正常）。**未確定注記（NOTICE_UNCONFIRMED）は
  overlay に必ず同梱**（機械は確定しない原則の画面反映・provisional の別も返す）。
- Z1 の「拒否は道案内」: KinshipValidationRejected.problems（誰の何が未充足か）を
  そのまま problems として返す（画面がそのまま列挙表示・白画面/不明エラーにしない）。

制約（P4 系先例の絶対条件）:
- **参照のみ**——書込み API 0 本・kintone 書込みゼロ（AST 機械検査テストで強制・
  P4-002 最終形 checker）。本 module は kintone を直接呼ばない（App34 読取は
  Z1 の load_graph_for_case・App36 は読まない）。
- 顧客 PII（氏名等）の画面表示は内部専用として可（kintone 直視と同格の既存裁定）。
  ログ・警報へは非搭載（本 module はログ自体を発行しない）。
- query 安全規律: case は数字列 grammar 検証のみ・自由文字列を下流へ渡さない
  （Z1 の query へは検証済み数字列だけが到達する・不正は固定 400 非反射）。
"""

import re

from fastapi import APIRouter, Request
from fastapi.responses import Response

# read-only 機械検査（P4-002 最終形 checker）の正規形 anchor。本 module は
# kintone を直接呼ばない（使用ゼロは test が別途 pin）——束縛検証の対象を
# module 直下の正規 import 1 本に固定するための規律準拠
from hub import kintone  # noqa: F401
from hub.webapp_auth import WEBAPP_ROOT, _gate

router = APIRouter()

_CASE_RE = re.compile(r"^[0-9]{1,10}$")

# 未確定注記（要件2: 必ず表示——overlay に常時同梱し画面が常時描画する）
NOTICE_UNCONFIRMED = (
    "この重畳表示は機械導出の結果（未確定の参考値）です。機械は相続人を確定"
    "しません——確定は関所（要確認の確定・弁護士）経由でのみ行われ、確定情報の"
    "正本は App36 です")


def _bad_request() -> Response:
    return Response(status_code=400)     # 固定応答（入力値を反射しない）


async def _overlay_for_case(case_record_id: str):
    """DerivationRun head の凡例重畳データ（P3-001 正規経路の read-only）。

    head 不在は None（図のみ・正常表示）。heirs は person_id・続柄表示・
    相続分表示のみ（氏名は返さない——画面側が Z1 の names で内部結合する）。
    続柄/相続分の表示写像は heir_projection の既存写像を流用（再実装しない）。
    旧 run（zokugara_code 欠落）は続柄を空欄で返す（粗い写像に頼らない）。
    """
    from hub.derivation_models import get_current_head
    from hub.heir_projection import (ProjectionPolicyError,
                                     ZOKUGARA_CODE_TO_APP36, share_to_display)

    head = await get_current_head(case_record_id)
    if head is None:
        return None
    payload = head.result_payload or {}
    heirs = []
    for h in payload.get("heirs") or []:
        code = h.get("zokugara_code")
        share = h.get("share")
        try:
            share_disp = share_to_display(share) if share else ""
        except ProjectionPolicyError:
            share_disp = ""              # grammar 外は表示しない（値非反射）
        heirs.append({
            "person_id": str(h.get("person_id") or ""),
            "zokugara": ZOKUGARA_CODE_TO_APP36.get(code, "") if code else "",
            "share_display": share_disp,
        })
    return {"run_id": head.id,
            "run_status": str(head.status),
            "provisional": bool(head.provisional),
            "notice": NOTICE_UNCONFIRMED,
            "heirs": heirs}


@router.get("/app/api/kinship")
@_gate
async def api_kinship(request: Request):
    """案件の相続関係図（Z2 SVG）＋導出結果の凡例重畳（read-only）。

    応答の閉集合（いずれも 200・固定 status）:
    - ok: svg（Z2 出力の文字列）＋names（record_id→氏名・凡例の内部結合用）＋
      warnings（Z1 の保留列挙）＋overlay（head 無しは None）
    - not_renderable: problems（Z1 の道案内列挙をそのまま）＋overlay
    - empty: 案件に人物レコードなし（固定 message）
    - unavailable: graphviz 不在の縮退（固定 message）
    不正な case は固定 400（非反射）。
    """
    case = request.query_params.get("case", "")
    if not _CASE_RE.fullmatch(case):
        return _bad_request()
    from kinship_graph import (load_graph_for_case,
                               load_koseki_summaries_for_case)
    from kinship_renderer import (GraphvizUnavailable, KinshipRenderError,
                                  KinshipValidationRejected, render_kinship)

    graph = await load_graph_for_case(case)
    if not graph.nodes:
        return {"status": "empty", "case_record_id": case,
                "message": "この案件に人物レコードがありません（App 34 未登録）"}
    # MAINT-3 B: 取得済み戸籍（App33）の最小一覧（正本 §2 の App33 言及の範囲・
    # read-only・chain 判定なし）。env 未設定は空リスト（縮退）
    kosekis = await load_koseki_summaries_for_case(case)
    overlay = await _overlay_for_case(case)
    try:
        # heir_scope=True: 相続人確定に必要な人物へ検証・描画とも絞る（既存思想）
        svg = render_kinship(graph, "svg", heir_scope=True)
    except KinshipValidationRejected as e:
        # Z1 の「拒否は道案内」をそのまま写像（誰の何が未充足かの列挙）
        return {"status": "not_renderable", "case_record_id": case,
                "problems": list(e.problems), "overlay": overlay,
                "kosekis": kosekis}
    except GraphvizUnavailable:
        return {"status": "unavailable", "case_record_id": case,
                "message": "関係図エンジン（graphviz）が未導入のため描画できません"
                           "（他機能は正常・デプロイ構成の確認が必要です）"}
    except KinshipRenderError:
        # fix1 H01: 描画失敗（dot 実行エラー等）の閉集合正規化。Z2 の例外文言は
        # stderr/DOT 断片を含み得るため**応答・ログとも非搭載**（本 module は
        # logger を持たない＝ログ経路自体が存在しない）。graphviz 不在（上）とは
        # 固定文言で区別可能にする
        return {"status": "unavailable", "case_record_id": case,
                "message": "関係図の描画に失敗しました（内部エラー・詳細は表示"
                           "しません。再試行で解消しない場合は管理者へ連絡して"
                           "ください）"}
    return {"status": "ok", "case_record_id": case,
            "svg": svg.decode("utf-8"),
            "names": {n.record_id: n.name for n in graph.nodes},
            "warnings": list(graph.warnings),
            "overlay": overlay,
            "kosekis": kosekis}


@router.get("/app/kinship")
@_gate
async def kinship_page(request: Request):
    path = WEBAPP_ROOT / "kinship.html"
    if not path.is_file():
        return Response(status_code=404)
    from fastapi.responses import FileResponse
    return FileResponse(path, media_type="text/html; charset=utf-8")
