"""Operator command line of the CATI platform pipeline.

    python -m app.trading_intelligence.pipeline status        the registry, the governance log, the latest cycles
    python -m app.trading_intelligence.pipeline register      record the code-defined families (idempotent)
    python -m app.trading_intelligence.pipeline verify        integrity of the governance log and of the registry
    python -m app.trading_intelligence.pipeline run-due       run the shadow cycles that are due (daily bar feed)

What is deliberately NOT here: any command that approves, rejects or revokes a promotion, switches a kill
switch, or records evidence. Those are decisions of an authenticated operator and exist only behind the
administrative interface (``app.api.family_governance``). A flag on a command line is not an authorization.

``register`` gives a family an identity and stage M0. It certifies nothing and grants no order authority.
``run-due`` runs in shadow: this module has no way to place an order.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, List


def _db() -> Any:
    from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
    from shared_lib.persistence.db import DB

    db = DB()
    ensure_cati_platform_schema(db)
    return db


def status(db: Any) -> Dict[str, Any]:
    from shared_lib.persistence.cati_platform_schema import CYCLE_TABLE, SHADOW_COMPARISON_TABLE

    from .family_registry import FamilyRegistry

    registry = FamilyRegistry(db)
    families: List[Dict[str, Any]] = []
    for entry in registry.entries():
        families.append({"family_id": entry.definition.family_id, "certification_status": entry.certification_status,
                         "governance_stage": entry.governance_stage, "allowed_modes": list(entry.allowed_modes),
                         "kill_switch_state": entry.kill_switch_state, "registered": entry.registered,
                         "executable": entry.executable, "blockers": list(entry.blockers)})
    with db.connect() as conn:
        cycles = [dict(r) for r in conn.execute(
            f"SELECT family_id, cycle_key, mode, status, accounts_total, accounts_done, accounts_failed FROM {CYCLE_TABLE} "
            "ORDER BY started_at DESC LIMIT 10")]
        comparisons = {r[0]: r[1] for r in conn.execute(f"SELECT status, COUNT(*) FROM {SHADOW_COMPARISON_TABLE} GROUP BY status")}
    return {"families": families, "executable_families": [f["family_id"] for f in families if f["executable"]],
            "governance_log": registry.governance.verify_chain(), "latest_cycles": cycles,
            "shadow_comparisons": comparisons}


def main(argv: Any = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.trading_intelligence.pipeline", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    sub.add_parser("register")
    sub.add_parser("verify")
    run = sub.add_parser("run-due")
    run.add_argument("--model-equity", type=float, default=None,
                     help="equity of the platform's model accounts (default: the evaluator's initial equity)")
    run.add_argument("--no-connected-accounts", action="store_true", help="model accounts only")
    args = parser.parse_args(argv)
    db = _db()
    if args.command == "status":
        print(json.dumps(status(db), indent=1, sort_keys=True, default=str))
        return 0
    from .family_registry import FamilyRegistry

    registry = FamilyRegistry(db)
    if args.command == "register":
        events = registry.register_all(actor_ref="system:pipeline-cli")
        print(json.dumps([{"family_id": e["family_id"], "recorded": not e.get("unchanged")} for e in events], indent=1))
        return 0
    if args.command == "verify":
        chain = registry.governance.verify_chain()
        drift = [e.definition.family_id for e in registry.entries() if "FAMILY_RULE_HASH_MISMATCH" in e.blockers]
        print(json.dumps({"governance_log": chain, "families_whose_code_differs_from_registration": drift}, indent=1))
        return 0 if chain["ok"] and not drift else 1
    if args.command == "run-due":
        from app.market_data.daily_feed import default_daily_bars
        from app.trading_intelligence.families.daily_trend import spec

        from .accounts import CombinedAccounts, ConnectedAccounts, ModelAccounts
        from .driver import COMPLETED, PlatformCycleDriver

        equity = args.model_equity or spec.INTERPRETATION["initial_equity_usdt"]
        sources: List[Any] = [ModelAccounts(initial_equity=equity)]
        if not args.no_connected_accounts:
            sources.append(ConnectedAccounts(db))
        driver = PlatformCycleDriver(db, accounts=CombinedAccounts(*sources), data={"1d": default_daily_bars(db)},
                                     registry=registry, owner="pipeline-cli")
        results = driver.run_due(int(time.time() * 1000))
        print(json.dumps([{"family_id": r.family_id, "cycle_key": r.cycle_key, "mode": r.mode, "status": r.status,
                           "reason": r.reason, "accounts_done": r.accounts_done, "accounts_failed": r.accounts_failed}
                          for r in results], indent=1))
        return 0 if all(r.status == COMPLETED for r in results) else 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
