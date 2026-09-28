"""Runtime facts for the Section 17 account capital stage (cycle shadow).

Reads, once per finalized epoch and never per candidate:

* the broker-authoritative account capital state (topology + transferable balance per wallet), through
  the existing transfer adapter (``capital.shadow_hook.account_capital_state``);
* the account's transfer settings (``TransferStore.settings``);
* the bot's configured per-trade allocation (provisional sizing, ``risk.capital_ledger``);
* capital already held by active CATI reservations on the account (all bots);
* per-family eligibility (``shared_lib.broker.capabilities.execution_readiness``) and the CATI new-entry
  kill switch (account-wide).

Every read fails CLOSED into an explicit unknown (state None, permissions None, kill switch assumed ON
when governance is unreadable) -- never into a permissive default. Nothing here moves funds or submits.
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any, Mapping, Optional

logger = logging.getLogger(__name__)


def _permissions(db: Any, broker_account_id: str) -> Optional[Mapping[str, Any]]:
    """The account's normalised permission evidence (no secrets) via the canonical capability gate, or None."""
    try:
        from app.core.broker_capability_gate import load_permission_evidence

        with db.connect() as conn:
            return load_permission_evidence(conn, broker_account_id)
    except Exception:
        return None


def _kill_switch(db: Any, broker_account_id: str) -> Optional[str]:
    try:
        from app.trading_intelligence.governance.promotion import PromotionGovernance

        return "CATI_NEW_ENTRY_KILL_SWITCH" if PromotionGovernance(db).kill_switch_on(scope=broker_account_id) else None
    except Exception:
        return "GOVERNANCE_STATE_UNAVAILABLE"  # an unreadable emergency control is never "off"


def build_account_capital_view(runner: Any, ranked: Any, evaluated: Mapping[str, Any], *, reservation_store: Any,
                               now_ms: int, cycle_id: str, state_reader=None, route_facts=None, policy=None):
    from app.risk.capital_ledger import per_trade_allocation_margin
    from app.trading_intelligence.capital.planner import CapitalSettings
    from app.trading_intelligence.capital.route_facts import ObservedRouteFacts
    from app.trading_intelligence.portfolio.account_capital import consider_account, family_eligibility

    ctx = runner.context
    db = runner.db
    account = str(getattr(ctx, "broker_account_id"))
    user_id = getattr(ctx, "user_id", None)
    # the same identity the canonical capability gate uses: broker type + broker_accounts.environment
    broker = str(getattr(ctx, "broker_type", "") or "")
    environment = getattr(ctx, "broker_environment", None)
    state = None
    try:
        if state_reader is None:
            from app.trading_intelligence.capital.shadow_hook import account_capital_state as state_reader
        state = state_reader(db, user_id=user_id, broker_account_id=account)
    except Exception as exc:
        logger.info("[CATI_ACCOUNT_CAPITAL] %s: capital state unavailable (%s)", account, type(exc).__name__)
        state = None
    try:
        from app.transfers.store import TransferStore

        settings = CapitalSettings.from_store(TransferStore(db).settings(user_id=user_id, broker_account_id=account))
    except Exception:
        settings = CapitalSettings()  # MANUAL_TRANSFER, not authorised: the most restrictive default
    margin = per_trade_allocation_margin(getattr(ctx, "allocation_type", ""), getattr(ctx, "allocation_value", 0.0),
                                         getattr(ctx, "capital_budget", 0.0))
    try:
        held_wallet, held_family = reservation_store.capital_reserved(account, now_ms)
    except Exception:
        held_wallet, held_family = None, None
    classes = sorted({str(r.instrument_key.asset_class) for r in ranked})
    family = family_eligibility(broker, environment or "UNKNOWN", classes, permissions=_permissions(db, account))
    block = _kill_switch(db, account)
    if held_wallet is None:
        block = block or "RESERVATION_CAPITAL_UNREADABLE"
    return consider_account(
        ranked, evaluated, user_id=user_id, broker_account_id=account, state=state, settings=settings,
        per_trade_margin=Decimal(str(margin)) if margin and margin > 0 else None,
        leverage=float(getattr(ctx, "max_leverage", 1.0) or 1.0), family=family,
        route_facts=route_facts or ObservedRouteFacts(db), now_ms=now_ms, cycle_id=cycle_id, account_block=block,
        reserved_by_wallet=held_wallet or {}, reserved_by_family=held_family or {}, policy=policy)


def record_capital_evidence(db: Any, view: Any, *, selected_ids, bot_instance_id: str, cycle_id: str) -> list:
    """Append-only SHADOW evidence of each SELECTED candidate's dry-run plan, with the topology and the
    per-wallet balances it was computed on. Never raises into the cycle."""
    rows = []
    try:
        from app.trading_intelligence.capital.evidence import CapitalPlanEvidenceStore

        store = CapitalPlanEvidenceStore(db)
        for rid in selected_ids:
            c = view.candidates.get(rid)
            if c is None or c.capital_plan is None:
                continue
            rows.append(store.record(c.capital_plan, user_id=view.user_id or "", bot_instance_id=bot_instance_id,
                                     cycle_id=cycle_id, opportunity_id=rid, topology=view.topology,
                                     balances={"asset": view.asset, "free_by_wallet": dict(view.balances),
                                               "reserved_by_wallet": {k: format(v, "f") for k, v in
                                                                      view.reserved_by_wallet.items()}}))
    except Exception as exc:
        logger.warning("[CATI_CAPITAL_EVIDENCE] skipped: %s", type(exc).__name__)
    return rows


def summary(view: Any) -> str:
    return json.dumps({"topology": view.topology_class, "viable": sorted(r for r in view.candidates if view.viable(r)),
                       "rejected": [list(x) for x in view.rejections()]}, default=str)


__all__ = ["build_account_capital_view", "record_capital_evidence", "summary"]
