"""SHADOW capital routing in the CATI cycle (Phase 5E / 6F).

After portfolio selection, for each selected opportunity: read the account's
wallet topology and broker-authoritative transferable balances, run the
CapitalAllocationPlanner and persist the decision (plus a simulated transfer
outcome) as append-only evidence. Nothing is submitted and no order is
placed -- a transfer proposal is never handed to the transfer service here.

AUTO_ACTIVE_IF_ELIGIBLE (``app.activation.cati.capital_routing_shadow``):
active whenever the cycle shadow is; ``CATI_CAPITAL_ROUTING_SHADOW_ENABLED``
set to an explicit off value is an operator override that switches it OFF.
It grants no execution authority and never submits a transfer. Every failure
is recorded and swallowed: shadow evidence can never block the runtime.
"""
from __future__ import annotations

import logging
import os
from decimal import Decimal
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)
ENV_FLAG = "CATI_CAPITAL_ROUTING_SHADOW_ENABLED"
_TRUE = ("1", "true", "yes", "on")
_PRODUCT = {"CRYPTO": "CRYPTO_PERPETUAL", "FX": "FX_PERPETUAL", "COMMODITIES": "TRADFI_PERPETUAL",
            "STOCK": "TRADFI_PERPETUAL", "INDEX": "TRADFI_PERPETUAL", "FUTURES": "TRADFI_PERPETUAL"}


def is_enabled() -> bool:
    from app.activation.cati import capital_routing_shadow
    from app.activation.transitions import observe

    return observe(capital_routing_shadow()).active


def account_capital_state(db: Any, *, user_id: str, broker_account_id: str, asset: str = "USDT",
                          adapter_factory=None, resolver=None):
    """Broker-authoritative AccountCapitalState, or None when unavailable."""
    from shared_lib.broker import resolve_broker_auth
    from shared_lib.broker.capabilities import Capability, declared_profile
    from shared_lib.broker.permissions import is_internal_transfer_permitted, load_evidence

    from app.trading_intelligence.capital.planner import AccountCapitalState
    from app.transfers.adapters import adapter_for
    from app.transfers.store import TransferStore

    auth = (resolver or resolve_broker_auth)(broker_account_id, user_id, db)
    adapter = (adapter_factory or adapter_for)(auth)
    if adapter is None:
        return None
    topo = adapter.topology()
    if topo is None:
        return None
    free: Dict[str, Optional[Decimal]] = {}
    for w in topo.wallets:
        try:
            free[w.native_type] = adapter.transferable(w, asset)
        except Exception:
            free[w.native_type] = None  # unknown, never zero
    with db.connect() as conn:
        r = conn.execute("SELECT permissions_json FROM broker_credentials_v2 WHERE account_id=? AND version=?",
                         (broker_account_id, auth.credential_version)).fetchone()
    ok, reason = is_internal_transfer_permitted(load_evidence(r[0] if r else None))
    usable = ok and declared_profile(auth.broker_type).usable(Capability.INTERNAL_TRANSFER, auth.environment)
    unresolved = len(TransferStore(db).in_flight(broker_account_id))
    return AccountCapitalState(broker_account_id, asset, topo, free, transfer_capability_usable=usable,
                               transfer_block_reason=None if usable else (reason if not ok else "CAPABILITY_NOT_USABLE"),
                               unresolved_transfers=unresolved)


def shadow_capital_routing(runner: Any, outcome: Any, evaluated: Dict[str, Any], *, required_by_opportunity=None) -> list:
    """Record one SHADOW capital plan per selected opportunity. Returns rows."""
    if not is_enabled():
        return []
    rows = []
    try:
        from app.ops import multi_asset_metrics as mm
        from app.trading_intelligence.capital.evidence import CapitalPlanEvidenceStore
        from app.trading_intelligence.capital.planner import CapitalSettings, plan_capital
        from app.transfers.store import TransferStore

        ctx = runner.context
        d = outcome.decision
        state = account_capital_state(runner.db, user_id=ctx.user_id, broker_account_id=d.broker_account_id)
        if state is None:
            return []
        settings = CapitalSettings.from_store(TransferStore(runner.db).settings(user_id=ctx.user_id,
                                                                               broker_account_id=d.broker_account_id))
        store = CapitalPlanEvidenceStore(runner.db)
        per_trade = Decimal(str(getattr(ctx, "trade_usdt_per_order", 0) or 0)) / Decimal(
            str(max(float(getattr(ctx, "max_leverage", 1) or 1), 1.0)))
        for opp_id in d.selected_opportunity_ids:
            opp = next((o for o in evaluated.values() if getattr(o, "economic_opportunity_id", None) == opp_id), None)
            ac = getattr(getattr(getattr(opp, "candidate", None), "instrument_key", None), "asset_class", "CRYPTO")
            required = (required_by_opportunity or {}).get(opp_id) or per_trade
            plan = plan_capital(state=state, product=_PRODUCT.get(ac, "CRYPTO_PERPETUAL"), required=required,
                                settings=settings, plan_key=f"{d.cycle_id}:{opp_id}")
            rows.append(store.record(plan, user_id=ctx.user_id, bot_instance_id=d.bot_instance_id, cycle_id=d.cycle_id,
                                     opportunity_id=opp_id))
            mm.capital_routing(str(getattr(ctx, "broker_type", "unknown")), plan.outcome)
    except Exception as exc:
        logger.warning("[CATI_CAPITAL_SHADOW] skipped: %s", type(exc).__name__)
    return rows


__all__ = ["ENV_FLAG", "account_capital_state", "is_enabled", "shadow_capital_routing"]
