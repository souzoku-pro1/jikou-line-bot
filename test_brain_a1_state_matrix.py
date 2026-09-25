"""BRAIN-A1 状態遷移表（fix3 自己点検・データ駆動）

行＝出典の状態（未登録／ingested／mismatch_hold／unavailable／detached／held と pending_recheck
の組合せ）、列＝操作（通常同期・追跡再照合それぞれの 新版／同値新版／旧版／取得失敗／参照変更／
参照消去／対象外化／一意不成立／判定不能、手動訂正、確認／却下／撤回）。
各セルの期待（状態・現在値・鮮度・履歴）は EXPECT_* の表に書き、同じ表から
Desktop\\claude\\BRAIN-A1_状態遷移表.md を生成する（表とテストの単一の正）。

セルの期待の形:
  ("n/a", 理由)                                     … A1 の範囲外／その状態では起こらない
  (state, pending, case, current, hist, src_fresh)   … 同期／再照合／手動訂正
      state     操作後の最新取込行の状態（"none"＝行なし）
      pending   pending_recheck が残るか
      case      出典の現在の紐付け（案件レコード番号 or None）
      current   出典由来の現在 fact/event が案件側にあるか
      hist      link_history の増分
      src_fresh 紐付け先の鮮度理由のうち当該出典のもの（None＝synced／"-"＝紐付けなし）
  ("op", 結果)                                       … 確認／却下／撤回: "ok" / "409:<reason>"
"""

import unittest

import sqlalchemy as sa

from brain_test_support import LINE_A, LINE_B, BrainDbMixin, app28, app30, app40, run
from hub import brain_ledger as ledger
from hub import brain_sync as sync
from hub.db import session_scope

T2 = "2026-09-21T02:00:00Z"
T4 = "2026-09-21T04:00:00Z"
T5 = "2026-09-21T05:00:00Z"

OPS = ("S_new", "S_same", "S_old", "S_fail", "S_refchange", "S_refclear", "S_catout",
       "S_nonunique", "S_undecidable",
       "R_new", "R_same", "R_old", "R_fail", "R_refchange", "R_refclear", "R_catout",
       "R_nonunique", "R_undecidable",
       "relink", "confirm", "reject", "revoke")
OP_LABELS = {
    "S_new": "同期:新版", "S_same": "同期:同値新版", "S_old": "同期:旧版", "S_fail": "同期:取得失敗",
    "S_refchange": "同期:参照変更", "S_refclear": "同期:参照消去", "S_catout": "同期:対象外化",
    "S_nonunique": "同期:一意不成立", "S_undecidable": "同期:判定不能",
    "R_new": "再照合:新版", "R_same": "再照合:同値新版", "R_old": "再照合:旧版",
    "R_fail": "再照合:取得失敗", "R_refchange": "再照合:参照変更", "R_refclear": "再照合:参照消去",
    "R_catout": "再照合:対象外化", "R_nonunique": "再照合:一意不成立",
    "R_undecidable": "再照合:判定不能", "relink": "手動訂正", "confirm": "確認",
    "reject": "却下", "revoke": "撤回",
}
STATE_LABELS = {
    "unregistered": "未登録", "ingested": "ingested", "ingested_pending": "ingested+pending",
    "mismatch_hold": "mismatch_hold", "unavailable": "unavailable",
    "unavailable_pending": "unavailable+pending", "held": "held（紐付け待ち）",
    "held_pending": "held+pending", "detached": "detached", "detached_pending": "detached+pending",
}

NA_APP30_ONLY = ("n/a", "App 30 の参照欄の操作＝App 28/40 には無い")
NA_APP28_ONLY = ("n/a", "App 28 の category／LINE ID の操作＝App 30/40 には無い")
NA_NO_ROWS = ("n/a", "行が無い出典には再照合・訂正が起きない")
NA_NO_FACT = ("n/a", "確認対象の fact が無い")
NA_SAME_AS_NEW = ("n/a", "未登録では新版と同じ（初回取込）")
NA_APP40 = ("n/a", "案件そのもの（紐付け・参照の操作は無い）")
NA_UNAVAIL_FETCH = ("n/a", "unavailable と同時に取得が成功する組合せは行の状態で表現済み")

# ── App 30（出典 30/5・紐付け先 40/2＝正本にのみ存在させ、実在確認が kintone を通る） ──
EXPECT30 = {
    "unregistered": {
        "S_new": ("ingested", False, "2", True, 1, None), "S_same": NA_SAME_AS_NEW,
        "S_old": NA_SAME_AS_NEW, "S_fail": ("none", False, None, False, 0, "-"),
        "S_refchange": ("ingested", False, "1", True, 1, None),
        "S_refclear": ("held", False, None, False, 1, "-"),
        "S_catout": NA_APP28_ONLY, "S_nonunique": NA_APP28_ONLY,
        "S_undecidable": ("none", False, None, False, 0, "-"),
        **{k: NA_NO_ROWS for k in ("R_new", "R_same", "R_old", "R_fail", "R_refchange",
                                   "R_refclear", "R_undecidable")},
        "R_catout": NA_APP28_ONLY, "R_nonunique": NA_APP28_ONLY,
        "relink": NA_NO_ROWS, "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
    "ingested": {
        "S_new": ("ingested", False, "2", True, 0, None), "S_same": ("ingested", False, "2", True, 0, None),
        "S_old": ("ingested", False, "2", True, 0, None), "S_fail": ("ingested", False, "2", True, 0, "cursor_error"),
        "S_refchange": ("ingested", False, "1", True, 1, None),
        "S_refclear": ("held", False, None, False, 1, "-"),
        "S_catout": NA_APP28_ONLY, "S_nonunique": NA_APP28_ONLY,
        "S_undecidable": ("ingested", True, "2", True, 0, "pending_recheck"),
        "R_new": ("ingested", False, "2", True, 0, None), "R_same": ("ingested", False, "2", True, 0, None),
        "R_old": ("ingested", False, "2", True, 0, None), "R_fail": ("ingested", False, "2", True, 0, None),
        "R_refchange": ("ingested", False, "1", True, 1, None),
        "R_refclear": ("held", False, None, False, 1, "-"),
        "R_catout": NA_APP28_ONLY, "R_nonunique": NA_APP28_ONLY,
        "R_undecidable": ("ingested", True, "2", True, 0, "pending_recheck"),
        "relink": ("ingested", False, "1", True, 1, None),
        "confirm": ("op", "ok"), "reject": ("op", "ok"), "revoke": ("op", "ok"),
    },
    "ingested_pending": {
        "S_new": ("ingested", False, "2", True, 0, None), "S_same": ("ingested", False, "2", True, 0, None),
        "S_old": ("ingested", True, "2", True, 0, "pending_recheck"),
        "S_fail": ("ingested", True, "2", True, 0, "pending_recheck"),
        "S_refchange": ("ingested", False, "1", True, 1, None),
        "S_refclear": ("held", False, None, False, 1, "-"),
        "S_catout": NA_APP28_ONLY, "S_nonunique": NA_APP28_ONLY,
        "S_undecidable": ("ingested", True, "2", True, 0, "pending_recheck"),
        "R_new": ("ingested", False, "2", True, 0, None), "R_same": ("ingested", False, "2", True, 0, None),
        "R_old": ("ingested", True, "2", True, 0, "pending_recheck"),
        "R_fail": ("ingested", True, "2", True, 0, "pending_recheck"),
        "R_refchange": ("ingested", False, "1", True, 1, None),
        "R_refclear": ("held", False, None, False, 1, "-"),
        "R_catout": NA_APP28_ONLY, "R_nonunique": NA_APP28_ONLY,
        "R_undecidable": ("ingested", True, "2", True, 0, "pending_recheck"),
        "relink": ("ingested", True, "1", True, 1, "pending_recheck"),
        "confirm": ("op", "ok"), "reject": ("op", "ok"), "revoke": ("op", "ok"),
    },
    "mismatch_hold": {
        "S_new": ("ingested", False, "2", True, 0, None), "S_same": ("ingested", False, "2", True, 0, None),
        "S_old": ("mismatch_hold", False, "2", True, 0, "source_mismatch_hold"),
        "S_fail": ("mismatch_hold", False, "2", True, 0, "source_mismatch_hold"),
        "S_refchange": ("ingested", False, "1", True, 1, None),
        "S_refclear": ("held", False, None, False, 1, "-"),
        "S_catout": NA_APP28_ONLY, "S_nonunique": NA_APP28_ONLY,
        "S_undecidable": ("mismatch_hold", True, "2", True, 0, "pending_recheck"),
        "R_new": ("ingested", False, "2", True, 0, None), "R_same": ("ingested", False, "2", True, 0, None),
        "R_old": ("mismatch_hold", False, "2", True, 0, "source_mismatch_hold"),
        "R_fail": ("mismatch_hold", False, "2", True, 0, "source_mismatch_hold"),
        "R_refchange": ("ingested", False, "1", True, 1, None),
        "R_refclear": ("held", False, None, False, 1, "-"),
        "R_catout": NA_APP28_ONLY, "R_nonunique": NA_APP28_ONLY,
        "R_undecidable": ("mismatch_hold", True, "2", True, 0, "pending_recheck"),
        "relink": ("mismatch_hold", False, "1", True, 1, "source_mismatch_hold"),
        "confirm": ("op", "ok"), "reject": ("op", "ok"), "revoke": ("op", "ok"),
    },
    "unavailable": {
        "S_new": ("ingested", False, "2", True, 0, None), "S_same": ("ingested", False, "2", True, 0, None),
        "S_old": ("unavailable", False, "2", True, 0, "source_unavailable"),
        "S_fail": ("unavailable", False, "2", True, 0, "source_unavailable"),
        "S_refchange": ("ingested", False, "1", True, 1, None),
        "S_refclear": ("held", False, None, False, 1, "-"),
        "S_catout": NA_APP28_ONLY, "S_nonunique": NA_APP28_ONLY,
        "S_undecidable": ("unavailable", True, "2", True, 0, "pending_recheck"),
        "R_new": ("ingested", False, "2", True, 0, None), "R_same": ("ingested", False, "2", True, 0, None),
        "R_old": ("unavailable", False, "2", True, 0, "source_unavailable"),
        "R_fail": ("unavailable", False, "2", True, 0, "source_unavailable"),
        "R_refchange": ("ingested", False, "1", True, 1, None),
        "R_refclear": ("held", False, None, False, 1, "-"),
        "R_catout": NA_APP28_ONLY, "R_nonunique": NA_APP28_ONLY,
        "R_undecidable": ("unavailable", True, "2", True, 0, "pending_recheck"),
        "relink": ("unavailable", False, "1", True, 1, "source_unavailable"),
        "confirm": ("op", "409:source_unavailable"), "reject": ("op", "409:source_unavailable"),
        "revoke": ("op", "ok"),
    },
    "unavailable_pending": {
        "S_new": ("ingested", False, "2", True, 0, None), "S_same": ("ingested", False, "2", True, 0, None),
        "S_old": ("unavailable", True, "2", True, 0, "pending_recheck"),
        "S_fail": ("unavailable", True, "2", True, 0, "pending_recheck"),
        "S_refchange": ("ingested", False, "1", True, 1, None),
        "S_refclear": ("held", False, None, False, 1, "-"),
        "S_catout": NA_APP28_ONLY, "S_nonunique": NA_APP28_ONLY,
        "S_undecidable": ("unavailable", True, "2", True, 0, "pending_recheck"),
        "R_new": ("ingested", False, "2", True, 0, None), "R_same": ("ingested", False, "2", True, 0, None),
        "R_old": ("unavailable", True, "2", True, 0, "pending_recheck"),
        "R_fail": ("unavailable", True, "2", True, 0, "pending_recheck"),
        "R_refchange": ("ingested", False, "1", True, 1, None),
        "R_refclear": ("held", False, None, False, 1, "-"),
        "R_catout": NA_APP28_ONLY, "R_nonunique": NA_APP28_ONLY,
        "R_undecidable": ("unavailable", True, "2", True, 0, "pending_recheck"),
        "relink": ("unavailable", True, "1", True, 1, "pending_recheck"),
        "confirm": ("op", "409:source_unavailable"), "reject": ("op", "409:source_unavailable"),
        "revoke": ("op", "ok"),
    },
    "held": {
        "S_new": ("held", False, None, False, 0, "-"), "S_same": ("held", False, None, False, 0, "-"),
        "S_old": ("held", False, None, False, 0, "-"), "S_fail": ("held", False, None, False, 0, "-"),
        "S_refchange": ("ingested", False, "1", True, 1, None),
        "S_refclear": ("held", False, None, False, 1, "-"),
        "S_catout": NA_APP28_ONLY, "S_nonunique": NA_APP28_ONLY,
        "S_undecidable": ("held", True, None, False, 0, "-"),
        "R_new": ("held", False, None, False, 0, "-"), "R_same": ("held", False, None, False, 0, "-"),
        "R_old": ("held", False, None, False, 0, "-"), "R_fail": ("held", False, None, False, 0, "-"),
        "R_refchange": ("ingested", False, "1", True, 1, None),
        "R_refclear": ("held", False, None, False, 1, "-"),
        "R_catout": NA_APP28_ONLY, "R_nonunique": NA_APP28_ONLY,
        "R_undecidable": ("held", True, None, False, 0, "-"),
        "relink": ("ingested", False, "1", False, 1, "relink_pending"),
        "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
    "held_pending": {
        "S_new": ("held", False, None, False, 0, "-"), "S_same": ("held", False, None, False, 0, "-"),
        "S_old": ("held", True, None, False, 0, "-"), "S_fail": ("held", True, None, False, 0, "-"),
        "S_refchange": ("ingested", False, "1", True, 1, None),
        "S_refclear": ("held", False, None, False, 1, "-"),
        "S_catout": NA_APP28_ONLY, "S_nonunique": NA_APP28_ONLY,
        "S_undecidable": ("held", True, None, False, 0, "-"),
        "R_new": ("held", False, None, False, 0, "-"), "R_same": ("held", False, None, False, 0, "-"),
        "R_old": ("held", True, None, False, 0, "-"), "R_fail": ("held", True, None, False, 0, "-"),
        "R_refchange": ("ingested", False, "1", True, 1, None),
        "R_refclear": ("held", False, None, False, 1, "-"),
        "R_catout": NA_APP28_ONLY, "R_nonunique": NA_APP28_ONLY,
        "R_undecidable": ("held", True, None, False, 0, "-"),
        "relink": ("ingested", True, "1", False, 1, "pending_recheck"),
        "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
}

# ── App 28（出典 28/100・LINE_A＝App 40 No.1。会話は case_event・fact は無い） ──
EXPECT28 = {
    "unregistered": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": NA_SAME_AS_NEW, "S_old": NA_SAME_AS_NEW,
        "S_fail": ("none", False, None, False, 0, "-"),
        "S_refchange": NA_APP30_ONLY, "S_refclear": NA_APP30_ONLY,
        "S_catout": ("none", False, None, False, 0, "-"), "S_nonunique": ("none", False, None, False, 0, "-"),
        "S_undecidable": ("none", False, None, False, 0, "-"),
        **{k: NA_NO_ROWS for k in ("R_new", "R_same", "R_old", "R_fail", "R_catout", "R_nonunique",
                                   "R_undecidable")},
        "R_refchange": NA_APP30_ONLY, "R_refclear": NA_APP30_ONLY,
        "relink": NA_NO_ROWS, "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
    "ingested": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": ("ingested", False, "1", True, 0, None),
        "S_old": ("ingested", False, "1", True, 0, None), "S_fail": ("ingested", False, "1", True, 0, "cursor_error"),
        "S_refchange": NA_APP30_ONLY, "S_refclear": NA_APP30_ONLY,
        "S_catout": ("detached", False, None, False, 1, "-"), "S_nonunique": ("detached", False, None, False, 1, "-"),
        "S_undecidable": ("ingested", True, "1", True, 0, "pending_recheck"),
        "R_new": ("ingested", False, "1", True, 0, None), "R_same": ("ingested", False, "1", True, 0, None),
        "R_old": ("ingested", False, "1", True, 0, None), "R_fail": ("ingested", False, "1", True, 0, None),
        "R_refchange": NA_APP30_ONLY, "R_refclear": NA_APP30_ONLY,
        "R_catout": ("detached", False, None, False, 1, "-"), "R_nonunique": ("detached", False, None, False, 1, "-"),
        "R_undecidable": ("ingested", True, "1", True, 0, "pending_recheck"),
        "relink": ("ingested", False, "2", True, 1, None),
        "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
    "ingested_pending": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": ("ingested", False, "1", True, 0, None),
        "S_old": ("ingested", True, "1", True, 0, "pending_recheck"),
        "S_fail": ("ingested", True, "1", True, 0, "pending_recheck"),
        "S_refchange": NA_APP30_ONLY, "S_refclear": NA_APP30_ONLY,
        "S_catout": ("detached", False, None, False, 1, "-"), "S_nonunique": ("detached", False, None, False, 1, "-"),
        "S_undecidable": ("ingested", True, "1", True, 0, "pending_recheck"),
        "R_new": ("ingested", False, "1", True, 0, None), "R_same": ("ingested", False, "1", True, 0, None),
        "R_old": ("ingested", True, "1", True, 0, "pending_recheck"),
        "R_fail": ("ingested", True, "1", True, 0, "pending_recheck"),
        "R_refchange": NA_APP30_ONLY, "R_refclear": NA_APP30_ONLY,
        "R_catout": ("detached", False, None, False, 1, "-"), "R_nonunique": ("detached", False, None, False, 1, "-"),
        "R_undecidable": ("ingested", True, "1", True, 0, "pending_recheck"),
        "relink": ("ingested", True, "2", True, 1, "pending_recheck"),
        "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
    "unavailable": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": ("ingested", False, "1", True, 0, None),
        "S_old": ("unavailable", False, "1", True, 0, "source_unavailable"),
        "S_fail": ("unavailable", False, "1", True, 0, "source_unavailable"),
        "S_refchange": NA_APP30_ONLY, "S_refclear": NA_APP30_ONLY,
        "S_catout": ("detached", False, None, False, 1, "-"), "S_nonunique": ("detached", False, None, False, 1, "-"),
        "S_undecidable": ("unavailable", True, "1", True, 0, "pending_recheck"),
        "R_new": ("ingested", False, "1", True, 0, None), "R_same": ("ingested", False, "1", True, 0, None),
        "R_old": ("unavailable", False, "1", True, 0, "source_unavailable"),
        "R_fail": ("unavailable", False, "1", True, 0, "source_unavailable"),
        "R_refchange": NA_APP30_ONLY, "R_refclear": NA_APP30_ONLY,
        "R_catout": ("detached", False, None, False, 1, "-"), "R_nonunique": ("detached", False, None, False, 1, "-"),
        "R_undecidable": ("unavailable", True, "1", True, 0, "pending_recheck"),
        "relink": ("unavailable", False, "2", True, 1, "source_unavailable"),
        "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
    "unavailable_pending": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": ("ingested", False, "1", True, 0, None),
        "S_old": ("unavailable", True, "1", True, 0, "pending_recheck"),
        "S_fail": ("unavailable", True, "1", True, 0, "pending_recheck"),
        "S_refchange": NA_APP30_ONLY, "S_refclear": NA_APP30_ONLY,
        "S_catout": ("detached", False, None, False, 1, "-"), "S_nonunique": ("detached", False, None, False, 1, "-"),
        "S_undecidable": ("unavailable", True, "1", True, 0, "pending_recheck"),
        "R_new": ("ingested", False, "1", True, 0, None), "R_same": ("ingested", False, "1", True, 0, None),
        "R_old": ("unavailable", True, "1", True, 0, "pending_recheck"),
        "R_fail": ("unavailable", True, "1", True, 0, "pending_recheck"),
        "R_refchange": NA_APP30_ONLY, "R_refclear": NA_APP30_ONLY,
        "R_catout": ("detached", False, None, False, 1, "-"), "R_nonunique": ("detached", False, None, False, 1, "-"),
        "R_undecidable": ("unavailable", True, "1", True, 0, "pending_recheck"),
        "relink": ("unavailable", True, "2", True, 1, "pending_recheck"),
        "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
    "detached": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": ("ingested", False, "1", True, 0, None),
        "S_old": ("detached", False, None, False, 0, "-"), "S_fail": ("detached", False, None, False, 0, "-"),
        "S_refchange": NA_APP30_ONLY, "S_refclear": NA_APP30_ONLY,
        "S_catout": ("detached", False, None, False, 0, "-"), "S_nonunique": ("detached", False, None, False, 0, "-"),
        "S_undecidable": ("detached", True, None, False, 0, "-"),
        "R_new": ("ingested", False, "1", True, 0, None), "R_same": ("ingested", False, "1", True, 0, None),
        "R_old": ("detached", False, None, False, 0, "-"), "R_fail": ("detached", False, None, False, 0, "-"),
        "R_refchange": NA_APP30_ONLY, "R_refclear": NA_APP30_ONLY,
        "R_catout": ("detached", False, None, False, 0, "-"), "R_nonunique": ("detached", False, None, False, 0, "-"),
        "R_undecidable": ("detached", True, None, False, 0, "-"),
        "relink": ("ingested", False, "2", False, 1, "relink_pending"),
        "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
    "detached_pending": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": ("ingested", False, "1", True, 0, None),
        "S_old": ("detached", True, None, False, 0, "-"), "S_fail": ("detached", True, None, False, 0, "-"),
        "S_refchange": NA_APP30_ONLY, "S_refclear": NA_APP30_ONLY,
        "S_catout": ("detached", False, None, False, 0, "-"), "S_nonunique": ("detached", False, None, False, 0, "-"),
        "S_undecidable": ("detached", True, None, False, 0, "-"),
        "R_new": ("ingested", False, "1", True, 0, None), "R_same": ("ingested", False, "1", True, 0, None),
        "R_old": ("detached", True, None, False, 0, "-"), "R_fail": ("detached", True, None, False, 0, "-"),
        "R_refchange": NA_APP30_ONLY, "R_refclear": NA_APP30_ONLY,
        "R_catout": ("detached", False, None, False, 0, "-"), "R_nonunique": ("detached", False, None, False, 0, "-"),
        "R_undecidable": ("detached", True, None, False, 0, "-"),
        "relink": ("ingested", True, "2", False, 1, "pending_recheck"),
        "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
}

# ── App 40（案件そのもの＝出典 40/1。参照・category の操作は無い） ──
_NA40 = {k: NA_APP40 for k in ("S_refchange", "S_refclear", "S_catout", "S_nonunique",
                                "S_undecidable", "R_refchange", "R_refclear", "R_catout",
                                "R_nonunique", "R_undecidable", "relink")}
EXPECT40 = {
    "unregistered": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": NA_SAME_AS_NEW, "S_old": NA_SAME_AS_NEW,
        "S_fail": ("none", False, None, False, 0, "-"), **_NA40,
        **{k: NA_NO_ROWS for k in ("R_new", "R_same", "R_old", "R_fail")},
        "confirm": NA_NO_FACT, "reject": NA_NO_FACT, "revoke": NA_NO_FACT,
    },
    "ingested": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": ("ingested", False, "1", True, 0, None),
        "S_old": ("ingested", False, "1", True, 0, None), "S_fail": ("ingested", False, "1", True, 0, "cursor_error"),
        "R_new": ("ingested", False, "1", True, 0, None), "R_same": ("ingested", False, "1", True, 0, None),
        "R_old": ("ingested", False, "1", True, 0, None), "R_fail": ("ingested", False, "1", True, 0, None),
        **_NA40, "confirm": ("op", "ok"), "reject": ("op", "ok"), "revoke": ("op", "ok"),
    },
    "mismatch_hold": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": ("ingested", False, "1", True, 0, None),
        "S_old": ("mismatch_hold", False, "1", True, 0, "source_mismatch_hold"),
        "S_fail": ("mismatch_hold", False, "1", True, 0, "source_mismatch_hold"),
        "R_new": ("ingested", False, "1", True, 0, None), "R_same": ("ingested", False, "1", True, 0, None),
        "R_old": ("mismatch_hold", False, "1", True, 0, "source_mismatch_hold"),
        "R_fail": ("mismatch_hold", False, "1", True, 0, "source_mismatch_hold"),
        **_NA40, "confirm": ("op", "ok"), "reject": ("op", "ok"), "revoke": ("op", "ok"),
    },
    "unavailable": {
        "S_new": ("ingested", False, "1", True, 0, None), "S_same": ("ingested", False, "1", True, 0, None),
        "S_old": ("unavailable", False, "1", True, 0, "source_unavailable"),
        "S_fail": ("unavailable", False, "1", True, 0, "source_unavailable"),
        "R_new": ("ingested", False, "1", True, 0, None), "R_same": ("ingested", False, "1", True, 0, None),
        "R_old": ("unavailable", False, "1", True, 0, "source_unavailable"),
        "R_fail": ("unavailable", False, "1", True, 0, "source_unavailable"),
        **_NA40, "confirm": ("op", "409:source_unavailable"),
        "reject": ("op", "409:source_unavailable"), "revoke": ("op", "ok"),
    },
}

MATRICES = (("App 30（発送管理・出典 30/5）", EXPECT30), ("App 28（チャットログ・出典 28/100）", EXPECT28),
            ("App 40（相続放棄案件・出典 40/1＝案件自身）", EXPECT40))


def matrix_counts() -> dict:
    total = na = tested = 0
    for _name, table in MATRICES:
        for _state, row in table.items():
            for _op, cell in row.items():
                total += 1
                if cell[0] == "n/a":
                    na += 1
                else:
                    tested += 1
    return {"total": total, "tested": tested, "n_a": na}


# ── 実行器 ───────────────────────────────────────────────────────────────────

async def _count(table):
    async with session_scope() as session:
        return int((await session.execute(sa.select(sa.func.count()).select_from(table))).scalar())


async def _has_current(app_id: str, rid: str) -> bool:
    async with session_scope() as session:
        return await ledger._has_current(session, app_id, rid)


async def _latest_state(app_id: str, rid: str):
    async with session_scope() as session:
        row = await ledger._latest_ingest_row(session, app_id, rid)
        if row is None:
            return "none", False
        pend = (await session.execute(sa.select(sa.func.count()).where(
            ledger.source_ingest.c.source_app_id == app_id,
            ledger.source_ingest.c.source_record_id == rid,
            ledger.source_ingest.c.pending_recheck.is_(True)))).scalar()
        return row.state, bool(pend)


class _Matrix(BrainDbMixin):
    """1 セル＝1 つの新しい DB（setUp/tearDown を回す）。"""

    SOURCE = ""

    def _fresh_db(self):
        self.tearDown()
        self.setUp()

    async def observe(self, app_id, rid, hist_before, case_hint):
        state, pending = await _latest_state(app_id, rid)
        case = await ledger.current_case_of_source(app_id, rid)
        src_fresh = "-"
        if case is not None:
            fd = await ledger.case_freshness_detail(case[0], case[1], "app40", sync.source_targets())
            hits = [r.split(":", 2)[2] for r in fd["reasons"] if r.startswith(f"app{app_id}:{rid}:")]
            if (app_id, rid) == (case[0], case[1]):        # 案件自身: 全体の理由
                hits = fd["reasons"]
            src_fresh = hits[0] if hits else None
        return (state, pending, case[1] if case else None, await _has_current(app_id, rid),
                await _count(ledger.link_history) - hist_before, src_fresh)

    async def op_result(self, fact_id, op, version):
        try:
            if op == "revoke":
                confs = await ledger.list_confirmations(fact_id)
                cid = confs[0]["confirmation_id"]
                await ledger.record_confirmation(fact_id=fact_id, seen_version=version, decision="revoke",
                                                 reason="", operation_id=f"m-{op}", actor="owner",
                                                 revoked_of=cid)
            else:
                await ledger.record_confirmation(fact_id=fact_id, seen_version=version, decision=op,
                                                 reason="", operation_id=f"m-{op}", actor="owner")
            return "ok"
        except ledger.SourceUnavailable:
            return "409:source_unavailable"
        except ledger.VersionConflict as exc:
            return "409:" + exc.reason

    def run_cell(self, table, state, op, setup, apply):
        cell = table[state][op]
        if cell[0] == "n/a":
            return
        self._fresh_db()
        ctx = run(setup(state))
        if cell[0] == "op":
            got = run(apply(op, ctx))
            self.assertEqual(got, cell[1], (self.SOURCE, state, op))
            return
        hist_before = run(_count(ledger.link_history))
        run(apply(op, ctx))
        got = run(self.observe(ctx["app"], ctx["rid"], hist_before, ctx.get("case")))
        self.assertEqual(got, cell, (self.SOURCE, state, op))

    def check_row(self, table, state, setup, apply):
        for op in OPS:
            with self.subTest(state=state, op=op):
                self.run_cell(table, state, op, setup, apply)


class TestMatrixApp30(_Matrix):
    SOURCE = "app30"

    async def setup(self, state):
        # App 40 No.1 は台帳へ同期、No.2 は正本にだけ存在（実在確認が kintone を通る）
        self.fake.data["40"] = [app40(1, 1)]
        await sync.sync_target(sync.TARGET_APP40)
        self.fake.data["40"].append(app40(2, 1, T2, LINEユーザーID=LINE_B))
        ctx = {"app": "30", "rid": "5", "ref": "2", "rev": 3, "case": "2"}
        if state == "unregistered":
            ctx["rev"] = 0
            return ctx
        ref = "999" if state.startswith("held") else "2"
        ctx["ref"] = ref
        self.fake.data["30"] = [app30(5, 1, 案件レコードID=ref)]
        await sync.sync_target(sync.TARGET_APP30)
        self.fake.data["30"] = [app30(5, 3, T2, 案件レコードID=ref, 件名="第3版")]   # rev3 で値が変わる
        await sync.sync_target(sync.TARGET_APP30)
        if state == "mismatch_hold":
            src = ledger.SourceRef("30", "5", 3, *sync.CONVERTER[sync.TARGET_APP30])
            s = await ledger.ingest_source(src, ("40", "2"), [ledger.FactIn(
                "shipping:5", "app30.件名", "text", "別の値", ledger.make_locator("件名"))])
            assert s["state"] == "mismatch_hold", s
        if state.startswith("unavailable"):
            saved = self.fake.data["30"]
            self.fake.data["30"] = []
            assert (await sync.recheck_target(sync.TARGET_APP30))["unavailable"] == 1
            self.fake.data["30"] = saved
        if state.endswith("_pending"):
            self.fake.fail_at = {len(self.fake.calls) + 2}      # 1=$id in・2=App 40 実在確認
            rc = await sync.recheck_target(sync.TARGET_APP30)
            assert rc["pending_recheck"] == 1, rc
            self.fake.fail_at = set()
        return ctx

    async def apply(self, op, ctx):
        rid, ref, rev = ctx["rid"], ctx["ref"], ctx["rev"]
        if op in ("confirm", "reject", "revoke"):
            facts = [f for f in await ledger.list_case_facts("40", ctx["case"], current_only=False)
                     if f["source_app_id"] == "30" and f["item_code"] == "app30.件名"]
            fact = sorted(facts, key=lambda f: f["fact_id"])[-1]
            if op == "revoke":
                # 撤回対象の確認は、出典が健在だった時点（状態を作る前）で付いていた想定
                async with session_scope() as session:
                    await session.execute(sa.insert(ledger.case_confirmation).values(
                        fact_id=fact["fact_id"], fact_version=fact["version"], actor="owner",
                        decision="confirm", reason="", operation_id="m-pre", seen_version=fact["version"]))
            return await self.op_result(fact["fact_id"], op, fact["version"])
        if op == "relink":
            await ledger.relink_source(source_app_id="30", source_record_id=rid, new_case=("40", "1"),
                                       reason="手動", operation_id="m-relink", actor="owner",
                                       seen_link_version=await ledger.link_version("30", rid),
                                       seen_source_revision=rev)
            return
        kind, what = op.split("_", 1)
        if what == "fail":
            self.fake.raise_all = True
        elif what == "old":
            self.fake.data["30"] = [app30(5, 2, T5, 案件レコードID=ref, 件名="旧版")]
        else:
            new_rev = rev + 1
            fields = {"案件レコードID": ref}
            if what == "new":
                fields["件名"] = "改版"
            elif what == "refchange":
                fields["案件レコードID"] = "1"
            elif what == "refclear":
                fields["案件レコードID"] = ""
            self.fake.data["30"] = [app30(5, new_rev, T4, **fields)]
            if what == "undecidable":
                self.fake.fail_at = {len(self.fake.calls) + 2}
        if kind == "S":
            await sync.sync_target(sync.TARGET_APP30)
        else:
            await sync.recheck_target(sync.TARGET_APP30)
        self.fake.raise_all = False
        self.fake.fail_at = set()

    def test_unregistered(self):
        self.check_row(EXPECT30, "unregistered", self.setup, self.apply)

    def test_ingested(self):
        self.check_row(EXPECT30, "ingested", self.setup, self.apply)

    def test_ingested_pending(self):
        self.check_row(EXPECT30, "ingested_pending", self.setup, self.apply)

    def test_mismatch_hold(self):
        self.check_row(EXPECT30, "mismatch_hold", self.setup, self.apply)

    def test_unavailable(self):
        self.check_row(EXPECT30, "unavailable", self.setup, self.apply)

    def test_unavailable_pending(self):
        self.check_row(EXPECT30, "unavailable_pending", self.setup, self.apply)

    def test_held(self):
        self.check_row(EXPECT30, "held", self.setup, self.apply)

    def test_held_pending(self):
        self.check_row(EXPECT30, "held_pending", self.setup, self.apply)


class TestMatrixApp28(_Matrix):
    SOURCE = "app28"

    async def setup(self, state):
        # App 40 No.1（LINE_A）と No.2（LINE_B）を台帳へ同期（No.2 は訂正先）
        self.fake.data["40"] = [app40(1, 1), app40(2, 1, T2, LINEユーザーID=LINE_B)]
        await sync.sync_target(sync.TARGET_APP40)
        ctx = {"app": "28", "rid": "100", "rev": 3, "case": "1"}
        if state == "unregistered":
            ctx["rev"] = 0
            return ctx
        self.fake.data["28"] = [app28(100, 1)]
        await sync.sync_target(sync.TARGET_APP28)
        self.fake.data["28"] = [app28(100, 3, T2)]
        await sync.sync_target(sync.TARGET_APP28)
        if state.startswith("detached"):
            self.fake.data["28"] = [app28(100, 3, T2, category="その他判断系")]
            rc = await sync.recheck_target(sync.TARGET_APP28)
            assert rc["moved"] == 1, rc
            self.fake.data["28"] = [app28(100, 3, T2)]        # 正本上は再び対象 category
        if state.startswith("unavailable"):
            saved = self.fake.data["28"]
            self.fake.data["28"] = []
            assert (await sync.recheck_target(sync.TARGET_APP28))["unavailable"] == 1
            self.fake.data["28"] = saved
        if state.endswith("_pending"):
            self.fake.fail_at = {len(self.fake.calls) + 2}      # 1=$id in・2=LINE 検索
            rc = await sync.recheck_target(sync.TARGET_APP28)
            assert rc["pending_recheck"] == 1, rc
            self.fake.fail_at = set()
        return ctx

    async def apply(self, op, ctx):
        rid, rev = ctx["rid"], ctx["rev"]
        if op == "relink":
            await ledger.relink_source(source_app_id="28", source_record_id=rid, new_case=("40", "2"),
                                       reason="手動", operation_id="m-relink", actor="owner",
                                       seen_link_version=await ledger.link_version("28", rid),
                                       seen_source_revision=rev)
            return
        kind, what = op.split("_", 1)
        if what == "fail":
            self.fake.raise_all = True
        elif what == "old":
            self.fake.data["28"] = [app28(100, 2, T5, message="旧版")]
        else:
            new_rev = rev + 1
            fields = {}
            if what == "new":
                fields["message"] = "改版"
            elif what == "catout":
                fields["category"] = "その他判断系"
            elif what == "nonunique":
                self.fake.data["40"].append(app40(3, 1, T4))    # 正本にだけ同じ LINE_A の No.3
            self.fake.data["28"] = [app28(100, new_rev, T4, **fields)]
            if what == "undecidable":
                self.fake.fail_at = {len(self.fake.calls) + 2}
        if kind == "S":
            await sync.sync_target(sync.TARGET_APP28)
        else:
            await sync.recheck_target(sync.TARGET_APP28)
        self.fake.raise_all = False
        self.fake.fail_at = set()

    def test_unregistered(self):
        self.check_row(EXPECT28, "unregistered", self.setup, self.apply)

    def test_ingested(self):
        self.check_row(EXPECT28, "ingested", self.setup, self.apply)

    def test_ingested_pending(self):
        self.check_row(EXPECT28, "ingested_pending", self.setup, self.apply)

    def test_unavailable(self):
        self.check_row(EXPECT28, "unavailable", self.setup, self.apply)

    def test_unavailable_pending(self):
        self.check_row(EXPECT28, "unavailable_pending", self.setup, self.apply)

    def test_detached(self):
        self.check_row(EXPECT28, "detached", self.setup, self.apply)

    def test_detached_pending(self):
        self.check_row(EXPECT28, "detached_pending", self.setup, self.apply)


class TestMatrixApp40(_Matrix):
    SOURCE = "app40"

    async def setup(self, state):
        ctx = {"app": "40", "rid": "1", "rev": 3, "case": "1"}
        if state == "unregistered":
            ctx["rev"] = 0
            return ctx
        self.fake.data["40"] = [app40(1, 1)]
        await sync.sync_target(sync.TARGET_APP40)
        self.fake.data["40"] = [app40(1, 3, T2, status="受理")]          # rev3 で値が変わる
        await sync.sync_target(sync.TARGET_APP40)
        if state == "mismatch_hold":
            src = ledger.SourceRef("40", "1", 3, *sync.CONVERTER[sync.TARGET_APP40])
            s = await ledger.ingest_source(src, ("40", "1"), [ledger.FactIn(
                "case", "app40.status", "choice", "辞任", ledger.make_locator("status"))])
            assert s["state"] == "mismatch_hold", s
        if state == "unavailable":
            saved = self.fake.data["40"]
            self.fake.data["40"] = []
            assert (await sync.recheck_target(sync.TARGET_APP40))["unavailable"] == 1
            self.fake.data["40"] = saved
        return ctx

    async def apply(self, op, ctx):
        rev = ctx["rev"]
        if op in ("confirm", "reject", "revoke"):
            facts = [f for f in await ledger.list_case_facts("40", "1", current_only=False)
                     if f["item_code"] == "app40.status"]
            fact = sorted(facts, key=lambda f: f["fact_id"])[-1]
            if op == "revoke":
                async with session_scope() as session:
                    await session.execute(sa.insert(ledger.case_confirmation).values(
                        fact_id=fact["fact_id"], fact_version=fact["version"], actor="owner",
                        decision="confirm", reason="", operation_id="m-pre", seen_version=fact["version"]))
            return await self.op_result(fact["fact_id"], op, fact["version"])
        kind, what = op.split("_", 1)
        if what == "fail":
            self.fake.raise_all = True
        elif what == "old":
            self.fake.data["40"] = [app40(1, 2, T5, status="書類収集中")]
        else:
            fields = {"status": "完了"} if what == "new" else {"status": "受理"}
            self.fake.data["40"] = [app40(1, rev + 1, T4, **fields)]
        if kind == "S":
            await sync.sync_target(sync.TARGET_APP40)
        else:
            await sync.recheck_target(sync.TARGET_APP40)
        self.fake.raise_all = False

    def test_unregistered(self):
        self.check_row(EXPECT40, "unregistered", self.setup, self.apply)

    def test_ingested(self):
        self.check_row(EXPECT40, "ingested", self.setup, self.apply)

    def test_mismatch_hold(self):
        self.check_row(EXPECT40, "mismatch_hold", self.setup, self.apply)

    def test_unavailable(self):
        self.check_row(EXPECT40, "unavailable", self.setup, self.apply)


class TestMatrixShape(unittest.TestCase):
    def test_every_state_has_every_op(self):
        for _name, table in MATRICES:
            for state, row in table.items():
                self.assertEqual(set(row), set(OPS), state)
        c = matrix_counts()
        self.assertEqual(c["total"], c["tested"] + c["n_a"])
        self.assertGreater(c["tested"], 250)


if __name__ == "__main__":
    unittest.main()
