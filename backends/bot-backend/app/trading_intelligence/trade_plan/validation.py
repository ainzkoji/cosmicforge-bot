"""Pre-execution TradePlan validation (Section 18.16) -- a PURE function the
future Section 20 execution path calls before it would ever submit. It
submits nothing, mutates nothing, and never modifies the plan: a plan that
fails here is dead; CATI re-evaluates and builds a new one.

Status priority (first failing wins; every failing reason is reported):
INVALID_INSTRUMENT_METADATA > EXPIRED > STALE > RESERVATION_LOST >
BROKER_DEGRADED > ENTRY_ZONE_VIOLATION > UNSUPPORTED_EXECUTION_PREFERENCE.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from app.trading_intelligence.contracts.portfolio import AccountPortfolioReservation, ReservationStatus
from app.trading_intelligence.contracts.system_health import BrokerHealthContext
from app.trading_intelligence.contracts.trade_plan import (
    InvalidationCode, SubmissionValidationResult, SubmissionValidity, TradePlan,
)
from app.trading_intelligence.contracts.venue_economics import ExecutionCapabilities
from app.trading_intelligence.trade_plan.builder import style_supported

V = SubmissionValidity
_PRIORITY = (V.INVALID_INSTRUMENT_METADATA, V.EXPIRED, V.STALE, V.RESERVATION_LOST, V.BROKER_DEGRADED,
             V.ENTRY_ZONE_VIOLATION, V.UNSUPPORTED_EXECUTION_PREFERENCE)


@dataclass(frozen=True)
class MarketReference:
    """The executable price the executor would act on, with its timestamp."""

    price: float
    observed_at: int
    spread_bps: Optional[float] = None


def validate_trade_plan_for_submission(
    plan: TradePlan,
    current_time: int,
    current_market_reference: Optional[MarketReference],
    broker_health: Optional[BrokerHealthContext],
    reservation_state: Optional[AccountPortfolioReservation],
    venue_capabilities: Optional[ExecutionCapabilities],
) -> SubmissionValidationResult:
    found: List[Tuple[SubmissionValidity, str]] = []
    add = lambda status, code: found.append((status, code))  # noqa: E731

    caps = venue_capabilities
    if caps is None:
        add(V.INVALID_INSTRUMENT_METADATA, "VENUE_CAPABILITIES_MISSING")
    else:
        if caps.venue_symbol and caps.venue_symbol.upper() != plan.instrument_key.venue_symbol.upper():
            add(V.INVALID_INSTRUMENT_METADATA, "VENUE_SYMBOL_MISMATCH")
        if not (caps.tick_size and caps.tick_size > 0 and caps.step_size and caps.step_size > 0):
            add(V.INVALID_INSTRUMENT_METADATA, "PRECISION_UNKNOWN")

    if current_time >= plan.plan_expiry_time:
        add(V.EXPIRED, InvalidationCode.PLAN_EXPIRED.value)
    if current_time > plan.allowed_entry_zone.valid_until:
        add(V.EXPIRED, InvalidationCode.ENTRY_ZONE_EXPIRED.value)

    ref = current_market_reference
    max_age = plan.execution_preferences.market_reference_max_age_ms
    if ref is None:
        add(V.STALE, "MARKET_REFERENCE_MISSING")
    elif ref.observed_at > current_time:
        add(V.STALE, "MARKET_REFERENCE_NON_CAUSAL")
    elif current_time - ref.observed_at > max_age:
        add(V.STALE, "MARKET_REFERENCE_STALE")

    r = reservation_state
    if r is None or r.reservation_id != plan.portfolio_reservation_id:
        add(V.RESERVATION_LOST, InvalidationCode.PORTFOLIO_RESERVATION_LOST.value)
    elif r.status != ReservationStatus.RESERVED.value or r.expires_at <= current_time \
            or r.broker_account_id != plan.broker_account_id:
        add(V.RESERVATION_LOST, InvalidationCode.PORTFOLIO_RESERVATION_LOST.value)

    if broker_health is None or broker_health.status != "HEALTHY":
        add(V.BROKER_DEGRADED, InvalidationCode.BROKER_HEALTH_DEGRADED.value)
    elif broker_health.broker_account_id not in (None, plan.broker_account_id):
        add(V.BROKER_DEGRADED, "BROKER_HEALTH_WRONG_ACCOUNT")

    if ref is not None:
        zone = plan.allowed_entry_zone
        if not (zone.minimum_price <= ref.price <= zone.maximum_price):
            add(V.ENTRY_ZONE_VIOLATION, "PRICE_OUTSIDE_ALLOWED_ENTRY_ZONE")
        broken = (ref.price <= plan.structural_invalidation_price if plan.side == "LONG"
                  else ref.price >= plan.structural_invalidation_price)
        if broken:
            add(V.ENTRY_ZONE_VIOLATION, InvalidationCode.STRUCTURAL_LEVEL_BROKEN.value)
        if ref.spread_bps is not None and ref.spread_bps > plan.execution_preferences.max_spread_bps:
            add(V.ENTRY_ZONE_VIOLATION, InvalidationCode.SPREAD_EXCEEDED_BUDGET.value)

    if caps is not None:
        prefs = plan.execution_preferences
        if not style_supported(prefs.preferred_order_style, caps):
            add(V.UNSUPPORTED_EXECUTION_PREFERENCE, f"ORDER_STYLE_UNSUPPORTED:{prefs.preferred_order_style}")
        if prefs.time_in_force not in caps.supported_time_in_force:
            add(V.UNSUPPORTED_EXECUTION_PREFERENCE, f"TIME_IN_FORCE_UNSUPPORTED:{prefs.time_in_force}")

    if not found:
        return SubmissionValidationResult(V.VALID.value, (), plan.trade_plan_id, int(current_time))
    statuses = {s for s, _ in found}
    status = next(s for s in _PRIORITY if s in statuses)
    return SubmissionValidationResult(status.value, tuple(dict.fromkeys(c for _, c in found)), plan.trade_plan_id,
                                      int(current_time))


def verify_trade_plan_integrity(plan: TradePlan) -> bool:
    """Section 20.3 plan-hash integrity: the analytical content still hashes
    to ``trade_plan_hash`` and the id is derived from that hash. A plan
    altered after creation (it is frozen, but a payload can be forged) fails."""
    from app.trading_intelligence.hashing import short_id

    from dataclasses import MISSING

    try:
        fields = {k: getattr(plan, k) for k in TradePlan.__dataclass_fields__}
        # ``TradePlan.build`` hashes the fields the builder PASSED; defaulted
        # fields it did not pass (mode, schema_version) are not part of that
        # digest. Accept exactly those two canonical field sets -- any changed
        # analytical value still changes both digests.
        defaults = {k: f.default for k, f in TradePlan.__dataclass_fields__.items() if f.default is not MISSING}
        explicit = {k: v for k, v in fields.items() if not (k in defaults and v == defaults[k])}
        for candidate in (fields, explicit):
            digest = TradePlan.content_hash(candidate)
            if digest == plan.trade_plan_hash and plan.trade_plan_id == short_id("tplan", digest):
                return True
        return False
    except Exception:
        return False


__all__ = ["MarketReference", "validate_trade_plan_for_submission", "verify_trade_plan_integrity"]
