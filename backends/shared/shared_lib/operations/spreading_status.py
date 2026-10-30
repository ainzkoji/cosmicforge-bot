"""Read-only administrative view of the platform order budget (Section H Step 5.5, external gate EXT-LIQ-01).

Reported from STORED state only (``platform_symbol_budgets``, ``platform_spread_decisions``,
``platform_spread_observations``): whether any budget was ever stored calibrated (each would carry an owner
``approval_ref``; no code creates one), how many entries were refused and why, how many reservations are open
or settled, and how many calibration observations exist. It never changes anything and never reads a venue.
"""
from __future__ import annotations

from typing import Any, Dict

EXTERNAL_GATE = "EXT-LIQ-01"
BLOCKER = ("Order-spreading capacity is uncalibrated: budgets are stored calibrated=0 and executable multi-account "
           "spreading on one venue/symbol/side is refused (ORDER_SPREADING_CALIBRATION_REQUIRED). A calibrated "
           "participation/impact model needs the owner's approval from measured fills (EXT-LIQ-01).")


def spreading_status(db: Any) -> Dict[str, Any]:
    try:
        with db.transaction(write=False) as conn:
            have = {int(r[0]) for r in conn.all("SELECT version FROM platform_schema_migrations "
                                                 "WHERE version IN (3, 60)")}
            if have != {3, 60}:
                return {"available": False, "reason": "ORDER_SPREADING_SCHEMA_MISSING", "external_gate": EXTERNAL_GATE,
                        "calibrated": False, "blocker": BLOCKER}
            now = conn.now_ms()
            budgets = conn.one("SELECT COUNT(*), COALESCE(SUM(CASE WHEN calibrated=1 THEN 1 ELSE 0 END), 0), "
                               "COALESCE(SUM(CASE WHEN expires_at > ? THEN 1 ELSE 0 END), 0) FROM platform_symbol_budgets",
                               (now,))
            refusals = {str(code): int(n) for code, n in conn.all(
                "SELECT code, COUNT(*) FROM platform_spread_decisions WHERE outcome='REFUSED' GROUP BY code ORDER BY code")}
            reservations = {str(k or "OPEN"): int(n) for k, n in conn.all(
                "SELECT settled_status, COUNT(*) FROM platform_spread_decisions WHERE outcome='RESERVED' "
                "GROUP BY settled_status")}
            observations = int(conn.scalar("SELECT COUNT(*) FROM platform_spread_observations") or 0)
    except Exception as exc:  # an unreadable store is reported as such, never as "fine"
        return {"available": False, "reason": f"ORDER_SPREADING_STATUS_UNAVAILABLE:{type(exc).__name__}",
                "external_gate": EXTERNAL_GATE, "calibrated": False, "blocker": BLOCKER}
    calibrated_budgets = int(budgets[1])
    return {
        "available": True,
        "external_gate": EXTERNAL_GATE,
        "calibrated": False,                       # no approved calibrated policy exists in this build
        "status": "BLOCKED_PENDING_CALIBRATION",
        "blocker": BLOCKER,
        "budgets": {"total": int(budgets[0]), "calibrated": calibrated_budgets,
                    "uncalibrated": int(budgets[0]) - calibrated_budgets, "unexpired": int(budgets[2])},
        "refusals_by_code": refusals,
        "reservations": reservations,
        "calibration_observations": observations,
    }
