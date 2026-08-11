"""Bot語彙: 職務上請求の請求案（SHOKUMU-PLAN 実装票・plan 生成の起動）

正本: DRAFT_SHOKUMU_PLAN.md（FROZEN）。裁定②=**起動は語彙のみ**（「請求案を
出して」・受任フック自動起票は別票）。裁定⑤=確定者要件なし（対外防壁は M1 承認）。

- flag ゲート: SHOKUMU_PLAN_ENABLED（既定 OFF・task 冒頭で辞退＝I/O ゼロ・
  語彙一覧への掲載も flag 連動＝registry.visible_fn）。
- 経路: 語彙 → confirm（既存標準パイプライン・案件特定は上位 T2 責務）→
  build_plan（read-only）→ 判定不能あり=道案内応答（起票なし・§2 fail-closed）→
  file_plan_envelope（App30 要確認・open 限定回収）→ 応答（候補数・phase・
  F5 注記=「収集見込み」の写像・**提案であり承認は別**の明示）。
- pending invalidate は execute 内 finally（全終端・CMD 裁定8 の型）。
- PII 規律: 応答は件数・封筒 No・phase・自治体名のみ（氏名・住所全文なし）。
"""

from hub.shokumu_plan import (build_plan, file_plan_envelope,
                              shokumu_plan_enabled)

TASK_TYPE = "shokumu_plan"

MSG_DISABLED = "請求案の作成は現在無効です（SHOKUMU_PLAN_ENABLED 未設定）"


async def execute(pending) -> tuple[str, str, str]:
    """OK 後の実行（handler の execute_fn フック）。plan 生成→提案封筒まで。"""
    from dispatch_bot import confirm   # 遅延 import（循環回避）

    user_id = getattr(pending, "user_id", "")
    if not shokumu_plan_enabled():
        confirm.invalidate(user_id)    # flag ゲート（冒頭辞退・I/O ゼロ）
        return MSG_DISABLED, "", ""
    try:
        case_record_id = str(getattr(getattr(pending, "case", None),
                                     "record_id", "") or "")
        materials = await build_plan(case_record_id)
        if materials.problems:
            # §2 fail-closed: 1 行でも判定不能なら全体要確認（起票なし・道案内）
            lines = ["請求案を作成できません。次を確認してください:"]
            lines += [f"・{p}" for p in materials.problems]
            return "\n".join(lines), "", ""
        env = await file_plan_envelope(materials)
        n = len(materials.candidates)
        propose = sum(1 for c in materials.candidates
                      if c["status"] == "propose")
        lines = [("既存の請求案封筒 No.{no} を回収しました（二重起票なし）"
                  if env["status"] == "already_filed"
                  else "請求案封筒 No.{no} を起票しました").format(
                      no=env["record_id"]),
                 f"候補 {n} 件（起票対象 {propose} 件・phase={materials.phase}）"]
        lines += [f"※{g}" for g in materials.guidance]
        lines.append("本件は提案です。起票の確定は関所（要確認の確定）・"
                     "発送は既存の承認フローで行われます（機械は確定しません）")
        return "\n".join(lines), env["record_id"], ""
    finally:
        confirm.invalidate(user_id)    # 全終端で必ず実行（裁定8 の型）
