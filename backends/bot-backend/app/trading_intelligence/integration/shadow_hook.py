"""The one, disabled-by-default entry point that wires CATI into the live
runner (Section 9.14). Feature-flagged via ``CATI_SHADOW_ENABLED`` (unset or
falsy = disabled = zero overhead beyond one environment-variable read).

This module is the single, intentional exception to "no catch-all exception
that converts a serious fault into a normal result": everything *inside*
CATI (the engine, the adapter, the regime model) fails closed into a typed
INVALID MarketState / TRANSITION_UNKNOWN RegimeDistribution rather than
swallowing a fault -- but this outermost boundary additionally guarantees
that not even a CATI *programming bug* can ever propagate into the runner's
per-symbol decision loop and interrupt live V2 trading (P9.14: "CATI
failures cannot stop existing V2 execution during this shadow phase").
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

ENV_FLAG = "CATI_SHADOW_ENABLED"

#: Separate flag for the Section 11-13 pipeline (setup discovery through
#: EconomicOpportunity). Kept independent of ENV_FLAG so the already-proven
#: MarketState/RegimeDistribution-only shadow path is never put at risk by
#: enabling the newer, heavier pipeline, and vice versa.
FULL_PIPELINE_ENV_FLAG = "CATI_FULL_PIPELINE_SHADOW_ENABLED"

_controller = None


def is_enabled() -> bool:
    return os.environ.get(ENV_FLAG, "").strip().lower() in ("1", "true", "yes", "on")


def is_full_pipeline_enabled() -> bool:
    return os.environ.get(FULL_PIPELINE_ENV_FLAG, "").strip().lower() in ("1", "true", "yes", "on")


def run_shadow_evaluation(
    snapshot: Any,
    *,
    venue: str,
    source: str,
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
    bot_instance_id: Optional[str] = None,
    symbol: Optional[str] = None,
    run_id: Optional[str] = None,
    cycle_id: Optional[str] = None,
) -> None:
    """Best-effort, fire-and-forget CATI shadow evaluation over one already
    causally-pinned MarketSnapshot. Returns nothing and NEVER raises: the
    caller (runner.py) does not need its own try/except around this call,
    and must not change behavior based on any value here -- there isn't one.

    ``bot_instance_id``/``run_id``/``cycle_id``/``symbol`` are attached only
    to the shadow log line for evidence/observability; they are never passed
    into ``evaluate_market_state`` and therefore never influence the shared
    MarketState or its cache identity (P3 tenant isolation).
    """
    if not is_enabled():
        return
    try:
        from app.trading_intelligence.integration.snapshot_adapter import evaluate_market_state
        from app.trading_intelligence.regime.engine import compute_regime_distribution

        market_state = evaluate_market_state(
            snapshot,
            venue=venue,
            source=source,
            base_asset=base_asset,
            quote_asset=quote_asset,
        )
        regime_distribution = compute_regime_distribution(market_state)

        logger.info(
            "[CATI_SHADOW] symbol=%s bot=%s run=%s cycle=%s market_state_id=%s "
            "data_quality=%s uncertainty=%.3f dominant_regime=%s dominant_weight=%.3f "
            "entropy=%.3f reason_codes=%s",
            symbol,
            bot_instance_id,
            run_id,
            cycle_id,
            market_state.market_state_id,
            market_state.data_quality.level.value,
            market_state.state_uncertainty.value,
            regime_distribution.dominant_regime,
            regime_distribution.dominant_weight,
            regime_distribution.entropy,
            ",".join(market_state.reason_codes) or "-",
        )
    except Exception as exc:  # Intentional: see module docstring -- CATI may never break live trading.
        from app.trading_intelligence.integration.errors import record_component_error, sanitize_message

        logger.error("[CATI_SHADOW] %s: shadow evaluation failed (non-fatal, no trading impact): %s", symbol, sanitize_message(exc))
        record_component_error("shadow_hook.run_shadow_evaluation", exc, cycle_id=cycle_id,
                               bot_instance_id=bot_instance_id, symbol=symbol)


def run_full_shadow_pipeline(
    snapshot: Any,
    *,
    venue: str,
    source: str,
    bot_instance_id: Optional[str] = None,
    symbol: Optional[str] = None,
    run_id: Optional[str] = None,
    cycle_id: Optional[str] = None,
) -> None:
    """Best-effort, fire-and-forget run of the full Sections 11-13 pipeline
    (setup discovery -> forecast -> cost estimate -> EconomicOpportunity).

    Disabled by default via ``CATI_FULL_PIPELINE_SHADOW_ENABLED``. No
    production HistoricalOutcomeLibrary is wired to this call, so every
    forecast will legitimately come back ``OUTCOME_LIBRARY_UNAVAILABLE`` and
    every opportunity ``INSUFFICIENT_EVIDENCE`` until a real library is built
    and injected (Section 12.15's offline research pipeline is intentionally
    out of scope for this runtime hook) -- that is the correct fail-closed
    behavior, not a defect. Same never-raises contract as
    ``run_shadow_evaluation`` above, and for the same reason (P9.14).
    """
    if not is_full_pipeline_enabled():
        return
    try:
        global _controller
        if _controller is None:
            from app.trading_intelligence.controller.cati_controller import CATIController

            _controller = CATIController()

        opportunities = _controller.run_cycle(snapshot=snapshot, venue=venue, source=source)
        for opportunity in opportunities:
            logger.info(
                "[CATI_ECONOMIC_SHADOW] symbol=%s bot=%s run=%s cycle=%s setup_family=%s "
                "setup_candidate_id=%s economic_opportunity_id=%s admission_status=%s "
                "ev_gross_r=%.4f ev_net_r=%.4f conservative_edge_r=%.4f reason_codes=%s",
                symbol, bot_instance_id, run_id, cycle_id, opportunity.setup_family,
                opportunity.setup_candidate_id, opportunity.economic_opportunity_id, opportunity.admission_status,
                opportunity.ev_gross_r, opportunity.ev_net_r, opportunity.conservative_edge_r,
                ",".join(opportunity.reason_codes) or "-",
            )
    except Exception as exc:  # Intentional: see module docstring -- CATI may never break live trading.
        from app.trading_intelligence.integration.errors import record_component_error, sanitize_message

        logger.error("[CATI_ECONOMIC_SHADOW] %s: full pipeline shadow evaluation failed (non-fatal, no trading impact): %s",
                     symbol, sanitize_message(exc))
        record_component_error("shadow_hook.run_full_shadow_pipeline", exc, cycle_id=cycle_id,
                               bot_instance_id=bot_instance_id, symbol=symbol)


__all__ = [
    "ENV_FLAG",
    "FULL_PIPELINE_ENV_FLAG",
    "is_enabled",
    "is_full_pipeline_enabled",
    "run_shadow_evaluation",
    "run_full_shadow_pipeline",
]
