"""Checked broker-health and capital-readiness evidence for the execution tail (Section H, Step 3.4).

Before this module the production tail told the final boundary two things it had not checked: that the broker
was ``HEALTHY`` and that capital was ready on the ``LOGICAL`` path. Both were constants. Here they are verdicts
over evidence, and the rule for every piece of evidence is the same: **absent or unreadable is not permission**.
A value that is missing, not a finite number, dated in the future or older than its bound never counts in
favour of an entry; it produces a status and a stable, machine-readable reason.

The two evaluators are pure. They read no clock, no database, no client and no setting; the caller hands them
what the cycle already holds (``BrokerEvidence``, ``CapitalEvidence``) and gets back the contracts the boundary
already consumes (``BrokerHealthContext``, ``CapitalReadiness``). They never raise: a verdict that cannot be
reached is itself a refusal with a reason. There is no override here, by argument, environment or setting.

**Broker health.** ``HEALTHY`` needs every one of these to be established:

* the account and the venue are named, and the client is bound to the environment the plan targets;
* the account snapshot is complete (equity, wallet, free margin and used margin are finite; equity and wallet
  are positive);
* an authenticated read of this account completed recently (``MAX_AUTHENTICATED_READ_AGE_MS``). A signed read
  that succeeds proves, for that moment, that the key is accepted, the clock is inside the venue's receive
  window and the address is not banned;
* a read of the venue answered during this preparation (the executable price: finite, positive, current);
* the process-wide rate-limit record of the venue endpoint is readable and shows no pending ``Retry-After``.
  A mutation is never delayed by the client, so without this check an entry would be sent to a venue that has
  just told this address to back off.

One signal can only withdraw health and never grants it: the request weight the venue reported on its latest
answer. A venue that reports nothing proves nothing, so its absence is ignored; a reported value at or above
the client's own pacing threshold is ``DEGRADED``.

**Capital readiness** reuses the one planner and the one readiness function of the platform
(``capital.planner.plan_capital`` and ``capital_readiness``), exactly as the full pipeline's dispatch does, over
the account evidence of this cycle. Ready needs: the account the plan names is the account that was
synchronised; no internal transfer of the account is unresolved; the trading wallet's free margin, net of
entries of this process the venue does not show yet, covers the smallest margin an admissible order of the plan
needs; and the plan's own reservation exists, is ``RESERVED``, has not expired, belongs to this account and this
bot, and holds execution capacity. The tail never moves funds, so a shortfall is ``INSUFFICIENT_FREE_MARGIN``
and never a proposed transfer.

The margin requirement is deliberately a floor (``minimum_order_margin``), not the size of the trade. The size
is decided later, by hard risk and by the executor, and both of them shrink an entry to the margin that is free
instead of refusing it. A stricter requirement here would be a second sizing authority that disagrees with the
first; the floor refuses only an entry that no later stage could have sent.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from app.trading_intelligence.capital.planner import (
    AccountCapitalState, CapitalReadiness, CapitalSettings, capital_readiness, plan_capital,
)
from app.trading_intelligence.contracts.system_health import BrokerHealthContext, BrokerHealthStatus
from shared_lib.broker.wallets import topology_for_account

from .contracts import FUTURE_TOLERANCE_MS

READINESS_VERSION = "execution-readiness-v1"
#: ``BrokerHealthContext.source`` of a verdict of this module
SOURCE = "PRODUCTION_CYCLE_EVIDENCE"

#: How old the last completed authenticated read of the account may be. The income ledger, whose refresh is
#: that read, is re-read from the venue at least every 300 s while an account is evaluated
#: (``integration.income_ledger.REFRESH_INTERVAL_MS``) and on every cycle in which an entry is possible. An age
#: beyond that interval plus one 30 s broker cycle therefore means the cycle is not reading the account as
#: designed. For a natural entry the refresh is forced, so the age measured is the time spent inside the cycle.
MAX_AUTHENTICATED_READ_AGE_MS = 330_000
#: How old the venue read of this preparation may be: one broker cycle. In the production tail the price is read
#: immediately before the evaluation, so this bound only fails evidence handed over from somewhere else.
MAX_VENUE_READ_AGE_MS = 30_000
#: The reservation mode whose insert validated that the user owns both the bot and the account and that
#: claimed the account's execution slot (``portfolio.reservation_store.MODE_PRODUCTION``).
EXECUTION_RESERVATION_MODE = "PRODUCTION"

# ------------------------------------------------------------------------------------------- broker reasons
BROKER_IDENTITY_MISSING = "BROKER_IDENTITY_MISSING"
BROKER_EVALUATION_TIME_MISSING = "BROKER_EVALUATION_TIME_MISSING"
BROKER_ENVIRONMENT_UNREADABLE = "BROKER_ENVIRONMENT_UNREADABLE"
BROKER_ENVIRONMENT_MISMATCH = "BROKER_ENVIRONMENT_MISMATCH"
BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE = "BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE"
BROKER_AUTHENTICATED_READ_UNPROVEN = "BROKER_AUTHENTICATED_READ_UNPROVEN"
BROKER_AUTHENTICATED_READ_FUTURE_DATED = "BROKER_AUTHENTICATED_READ_FUTURE_DATED"
BROKER_AUTHENTICATED_READ_STALE = "BROKER_AUTHENTICATED_READ_STALE"
BROKER_VENUE_READ_UNPROVEN = "BROKER_VENUE_READ_UNPROVEN"
BROKER_RATE_LIMIT_STATE_UNREADABLE = "BROKER_RATE_LIMIT_STATE_UNREADABLE"
BROKER_RATE_LIMIT_BACKOFF = "BROKER_RATE_LIMIT_BACKOFF"
BROKER_REQUEST_WEIGHT_NEAR_LIMIT = "BROKER_REQUEST_WEIGHT_NEAR_LIMIT"
BROKER_REASONS: Tuple[str, ...] = (
    BROKER_IDENTITY_MISSING, BROKER_EVALUATION_TIME_MISSING, BROKER_ENVIRONMENT_UNREADABLE, BROKER_ENVIRONMENT_MISMATCH,
    BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE, BROKER_AUTHENTICATED_READ_UNPROVEN, BROKER_AUTHENTICATED_READ_FUTURE_DATED,
    BROKER_AUTHENTICATED_READ_STALE, BROKER_VENUE_READ_UNPROVEN, BROKER_RATE_LIMIT_STATE_UNREADABLE,
    BROKER_RATE_LIMIT_BACKOFF, BROKER_REQUEST_WEIGHT_NEAR_LIMIT)

# ------------------------------------------------------------------------------------------ capital reasons
CAPITAL_EVALUATION_TIME_MISSING = "CAPITAL_EVALUATION_TIME_MISSING"
CAPITAL_ACCOUNT_MISMATCH = "CAPITAL_ACCOUNT_MISMATCH"
TRANSFER_STATE_UNREADABLE = "TRANSFER_STATE_UNREADABLE"
CAPITAL_REQUIREMENT_UNKNOWN = "CAPITAL_REQUIREMENT_UNKNOWN"
IN_FLIGHT_MARGIN_UNREADABLE = "IN_FLIGHT_MARGIN_UNREADABLE"
INSUFFICIENT_FREE_MARGIN = "INSUFFICIENT_FREE_MARGIN"
RESERVATION_PLAN_MISMATCH = "RESERVATION_PLAN_MISMATCH"
RESERVATION_EXPIRED = "RESERVATION_EXPIRED"
RESERVATION_ACCOUNT_MISMATCH = "RESERVATION_ACCOUNT_MISMATCH"
RESERVATION_BOT_MISMATCH = "RESERVATION_BOT_MISMATCH"
RESERVATION_NOT_EXECUTION_SCOPED = "RESERVATION_NOT_EXECUTION_SCOPED"
#: reasons of this module; the planner adds its own (``ACCOUNT_RECONCILIATION_REQUIRED``,
#: ``ACCOUNT_TOPOLOGY_UNKNOWN``, ``NO_WALLET_COLLATERALISES_PRODUCT``, ``TRADING_WALLET_BALANCE_UNKNOWN``,
#: ``LOGICAL_RESERVATION_UNKNOWN``, ``LOGICAL_RESERVATION_NOT_VALID``) and they are passed through unchanged
CAPITAL_REASONS: Tuple[str, ...] = (
    CAPITAL_EVALUATION_TIME_MISSING, CAPITAL_ACCOUNT_MISMATCH, TRANSFER_STATE_UNREADABLE, CAPITAL_REQUIREMENT_UNKNOWN,
    IN_FLIGHT_MARGIN_UNREADABLE, INSUFFICIENT_FREE_MARGIN, RESERVATION_PLAN_MISMATCH, RESERVATION_EXPIRED,
    RESERVATION_ACCOUNT_MISMATCH, RESERVATION_BOT_MISMATCH, RESERVATION_NOT_EXECUTION_SCOPED)

_H = BrokerHealthStatus
#: the worst status wins (the order ``integration.context_adapters`` uses for the runner's health)
_SEVERITY: Tuple[str, ...] = (_H.UNAVAILABLE.value, _H.UNKNOWN.value, _H.DEGRADED.value, _H.HEALTHY.value)
_ENVIRONMENTS = ("DEMO", "LIVE")


# ------------------------------------------------------------------------------------------------- evidence
@dataclass(frozen=True)
class RateLimitEvidence:
    """What this process has recorded about the venue endpoint the account's orders go to."""
    #: seconds until the venue's ``Retry-After`` has passed; 0 = none pending; None = no readable record
    backoff_remaining_s: Any
    #: request weight the venue reported on its latest answer; None = the venue reported nothing
    used_weight_1m: Any = None
    #: the venue's budget and the share of it at which the client itself starts pacing reads
    weight_limit_1m: Any = None
    pause_fraction: Any = None


@dataclass(frozen=True)
class BrokerEvidence:
    """What the cycle holds about one broker account at the moment a plan is prepared. Every field may be
    ``None`` or malformed; the evaluator decides what that means, the collector never substitutes a default."""
    broker_account_id: Any              # the account the cycle synchronised
    venue: Any
    plan_environment: Any               # DEMO | LIVE: where the plan asks to be executed
    client_environment: Any             # DEMO | LIVE: what the client is bound to; None = unreadable
    now_ms: Any
    equity: Any                         # the account document of this cycle, as account risk read it
    wallet: Any
    free_margin: Any
    margin_used: Any
    authenticated_read_at_ms: Any       # when the last authenticated read of the account completed
    venue_price: Any                    # the executable price read in this preparation
    venue_price_at_ms: Any
    rate_limit: Optional[RateLimitEvidence]


@dataclass(frozen=True)
class CapitalEvidence:
    """What the cycle holds about the money one plan depends on."""
    broker: Any                         # binance | bybit | bingx: selects the declared wallet topology
    product: Any                        # e.g. CRYPTO_PERPETUAL: selects the wallet that collateralises the plan
    broker_account_id: Any              # the account the cycle synchronised
    plan_broker_account_id: Any         # the account the plan names
    plan_bot_instance_id: Any
    plan_reservation_id: Any
    plan_key: Any                       # identity of the plan, for the planner's record only
    now_ms: Any
    free_margin: Any                    # the trading wallet's free margin in this cycle's account document
    in_flight_margin: Any               # margin of this process's entries the venue does not show yet
    required_margin: Any                # see ``minimum_order_margin``; None = unknown
    unresolved_transfers: Any           # internal transfers of the account not in a terminal state; None = unreadable
    reservation: Any                    # the stored reservation row of the plan, or None
    account_mode: Any = None            # as read from the venue; None = not read (never guessed unified)
    asset: str = "USDT"


# -------------------------------------------------------------------------------------------------- parsing
def _number(value: Any) -> Optional[float]:
    """A finite number, or None. Booleans, text and objects are not numbers: a value of the wrong type is
    unreadable evidence, not a zero."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _decimal(value: Any) -> Optional[Decimal]:
    if _number(value) is None:
        return None
    return value if isinstance(value, Decimal) else Decimal(repr(value) if isinstance(value, float) else str(value))


def _integer(value: Any) -> Optional[int]:
    number = _number(value)
    return int(number) if number is not None and number == int(number) else None


def _text(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value.strip() else None


def _environment(value: Any) -> Optional[str]:
    text = _text(value)
    return text.upper() if text is not None and text.upper() in _ENVIRONMENTS else None


# -------------------------------------------------------------------------------------------- broker health
def evaluate_broker_health(evidence: BrokerEvidence, *,
                           max_authenticated_read_age_ms: int = MAX_AUTHENTICATED_READ_AGE_MS,
                           max_venue_read_age_ms: int = MAX_VENUE_READ_AGE_MS) -> BrokerHealthContext:
    """The health of one broker account for one submission. ``HEALTHY`` only when every positive condition of
    the module docstring is established; otherwise the worst status found and every reason, in the order the
    checks ran. ``freshness_ms`` is the age of the last authenticated read (None when it is not proven)."""
    e = evidence
    found: List[Tuple[str, str]] = []
    account, venue = _text(e.broker_account_id), _text(e.venue)
    if account is None or venue is None:
        found.append((_H.UNKNOWN.value, BROKER_IDENTITY_MISSING))
    now = _integer(e.now_ms)
    if now is None or now <= 0:
        now = None
        found.append((_H.UNKNOWN.value, BROKER_EVALUATION_TIME_MISSING))

    bound, wanted = _environment(e.client_environment), _environment(e.plan_environment)
    if bound is None or wanted is None:
        found.append((_H.UNKNOWN.value, BROKER_ENVIRONMENT_UNREADABLE))
    elif bound != wanted:
        # evidence about another environment says nothing about the one the plan would trade in
        found.append((_H.UNAVAILABLE.value, BROKER_ENVIRONMENT_MISMATCH))

    equity, wallet, free, used = (_number(v) for v in (e.equity, e.wallet, e.free_margin, e.margin_used))
    if equity is None or wallet is None or free is None or used is None or equity <= 0 or wallet <= 0:
        found.append((_H.UNKNOWN.value, BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE))

    age: Optional[int] = None
    read_at = _integer(e.authenticated_read_at_ms)
    if read_at is None or read_at <= 0 or now is None:
        found.append((_H.UNKNOWN.value, BROKER_AUTHENTICATED_READ_UNPROVEN))
    elif read_at > now + FUTURE_TOLERANCE_MS:
        found.append((_H.UNKNOWN.value, BROKER_AUTHENTICATED_READ_FUTURE_DATED))
    else:
        age = max(0, now - read_at)
        if age > int(max_authenticated_read_age_ms):
            found.append((_H.DEGRADED.value, BROKER_AUTHENTICATED_READ_STALE))

    price, price_at = _number(e.venue_price), _integer(e.venue_price_at_ms)
    if price is None or price <= 0 or price_at is None or now is None or price_at > now + FUTURE_TOLERANCE_MS \
            or now - price_at > int(max_venue_read_age_ms):
        found.append((_H.UNKNOWN.value, BROKER_VENUE_READ_UNPROVEN))

    rate = e.rate_limit
    backoff = _number(rate.backoff_remaining_s) if isinstance(rate, RateLimitEvidence) else None
    if backoff is None or backoff < 0:
        found.append((_H.UNKNOWN.value, BROKER_RATE_LIMIT_STATE_UNREADABLE))
    elif backoff > 0:
        found.append((_H.UNAVAILABLE.value, BROKER_RATE_LIMIT_BACKOFF))
    if isinstance(rate, RateLimitEvidence):
        weight, limit = _number(rate.used_weight_1m), _number(rate.weight_limit_1m)
        fraction = _number(rate.pause_fraction)
        # negative evidence only: an unreported weight (or an unknown budget) neither helps nor hurts
        if weight is not None and limit is not None and fraction is not None and limit > 0 and 0 < fraction <= 1 \
                and weight >= limit * fraction:
            found.append((_H.DEGRADED.value, BROKER_REQUEST_WEIGHT_NEAR_LIMIT))

    status = min((s for s, _ in found), key=_SEVERITY.index, default=_H.HEALTHY.value)
    return BrokerHealthContext(
        broker_account_id=account, venue=venue, environment=bound, status=status, observed_at=now, source=SOURCE,
        freshness_ms=age, reason_codes=tuple(dict.fromkeys(reason for _, reason in found)))


# ----------------------------------------------------------------------------------------- capital readiness
def minimum_order_margin(min_notional: Any, leverage_cap: Any) -> Optional[Decimal]:
    """The smallest margin any order of a plan can need: the venue's minimum notional for the instrument at the
    highest leverage the account's policy may resolve for it. Every order hard risk can size needs at least
    this, so free margin below it is a certain refusal. None when either input is unknown: the requirement is
    then unknown, and an unknown requirement is not a satisfied one."""
    notional, leverage = _decimal(min_notional), _decimal(leverage_cap)
    if notional is None or leverage is None or notional <= 0 or leverage < 1:
        return None
    return notional / leverage


def _reservation_defect(e: CapitalEvidence, now: int) -> Optional[str]:
    """Why a ``RESERVED`` row is still not this plan's valid claim on the account, or None."""
    r = e.reservation
    if _text(e.plan_reservation_id) is None or getattr(r, "reservation_id", None) != e.plan_reservation_id:
        return RESERVATION_PLAN_MISMATCH
    expires = _integer(getattr(r, "expires_at", None))
    if expires is None or expires <= now:
        return RESERVATION_EXPIRED
    if getattr(r, "broker_account_id", None) != e.broker_account_id:
        return RESERVATION_ACCOUNT_MISMATCH
    if _text(e.plan_bot_instance_id) is None or getattr(r, "bot_instance_id", None) != e.plan_bot_instance_id:
        return RESERVATION_BOT_MISMATCH
    if getattr(r, "mode", None) != EXECUTION_RESERVATION_MODE:
        return RESERVATION_NOT_EXECUTION_SCOPED
    return None


def evaluate_capital_readiness(evidence: CapitalEvidence) -> CapitalReadiness:
    """May the plan proceed to hard risk on the capital this cycle saw? The first failing condition names the
    reason. ``pending`` is true only for an unresolved transfer, which a later cycle may see completed; every
    other refusal is definitive for this evaluation (the boundary then releases the reservation and the next
    cycle starts from new evidence)."""
    e = evidence

    def blocked(reason: str) -> CapitalReadiness:
        return CapitalReadiness(False, "BLOCKED", reason)

    now = _integer(e.now_ms)
    if now is None or now <= 0:
        return blocked(CAPITAL_EVALUATION_TIME_MISSING)
    account = _text(e.broker_account_id)
    if account is None or e.plan_broker_account_id != account:
        return blocked(CAPITAL_ACCOUNT_MISMATCH)
    transfers = _integer(e.unresolved_transfers)
    if transfers is None or transfers < 0:
        return blocked(TRANSFER_STATE_UNREADABLE)
    required = _decimal(e.required_margin)
    if required is None or required <= 0:
        return blocked(CAPITAL_REQUIREMENT_UNKNOWN)
    in_flight = _decimal(e.in_flight_margin)
    if in_flight is None or in_flight < 0:
        return blocked(IN_FLIGHT_MARGIN_UNREADABLE)

    broker, product = _text(e.broker) or "", _text(e.product) or ""
    topology = topology_for_account(broker, _text(e.account_mode))
    trading = topology.wallet_for_product(product) if topology is not None else None
    state = AccountCapitalState(
        broker_account_id=account, asset=e.asset, topology=topology,
        free_by_wallet={trading.native_type: _decimal(e.free_margin)} if trading is not None else {},
        reserved_by_wallet={trading.native_type: in_flight} if trading is not None else {},
        # the tail moves no funds: a shortfall is a refusal, never a proposed transfer
        transfer_capability_usable=False, transfer_block_reason=INSUFFICIENT_FREE_MARGIN,
        unresolved_transfers=transfers)
    capital_plan = plan_capital(state=state, product=product, required=required, settings=CapitalSettings(),
                                plan_key=str(e.plan_key or ""))
    status = getattr(e.reservation, "status", None) if e.reservation is not None else None
    verdict = capital_readiness(capital_plan, reservation_status=_text(status))
    if not verdict.ready:
        return verdict
    defect = _reservation_defect(e, now)
    return CapitalReadiness(False, verdict.path, defect) if defect is not None else verdict


# ------------------------------------------------------------------------------------------------ evidence
def _plain(value: Any) -> Any:
    """A JSON-compatible rendering of a piece of evidence; what could not be read is recorded as null."""
    if isinstance(value, Decimal):
        return format(value, "f") if value.is_finite() else None
    number = _number(value)
    if number is not None:
        return value if isinstance(value, int) else number
    return _text(value)


def readiness_document(health: BrokerHealthContext, capital: CapitalReadiness, *, broker: BrokerEvidence,
                       funds: CapitalEvidence) -> Dict[str, Any]:
    """Both verdicts with the evidence they were reached on, for the cycle's durable evaluation record. The
    boundary reports a broker-health refusal only as ``BROKER_HEALTH_DEGRADED``; the reason is kept here."""
    rate = broker.rate_limit if isinstance(broker.rate_limit, RateLimitEvidence) else None
    return {
        "version": READINESS_VERSION,
        "broker_health": {
            "status": health.status, "reason_codes": list(health.reason_codes), "source": health.source,
            "observed_at": health.observed_at, "environment": health.environment,
            "authenticated_read_age_ms": health.freshness_ms,
            "rate_limit_backoff_s": _plain(rate.backoff_remaining_s) if rate is not None else None,
            "used_weight_1m": _plain(rate.used_weight_1m) if rate is not None else None},
        "capital": {
            "ready": capital.ready, "path": capital.path, "reason": capital.reason, "pending": capital.pending,
            "free_margin": _plain(funds.free_margin), "in_flight_margin": _plain(funds.in_flight_margin),
            "required_margin": _plain(funds.required_margin), "unresolved_transfers": _plain(funds.unresolved_transfers),
            "reservation_status": _text(getattr(funds.reservation, "status", None))}}


__all__ = ["READINESS_VERSION", "SOURCE", "MAX_AUTHENTICATED_READ_AGE_MS", "MAX_VENUE_READ_AGE_MS",
           "EXECUTION_RESERVATION_MODE", "BROKER_REASONS", "CAPITAL_REASONS", "BROKER_IDENTITY_MISSING",
           "BROKER_EVALUATION_TIME_MISSING", "BROKER_ENVIRONMENT_UNREADABLE", "BROKER_ENVIRONMENT_MISMATCH",
           "BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE", "BROKER_AUTHENTICATED_READ_UNPROVEN",
           "BROKER_AUTHENTICATED_READ_FUTURE_DATED", "BROKER_AUTHENTICATED_READ_STALE", "BROKER_VENUE_READ_UNPROVEN",
           "BROKER_RATE_LIMIT_STATE_UNREADABLE", "BROKER_RATE_LIMIT_BACKOFF", "BROKER_REQUEST_WEIGHT_NEAR_LIMIT",
           "CAPITAL_EVALUATION_TIME_MISSING", "CAPITAL_ACCOUNT_MISMATCH", "TRANSFER_STATE_UNREADABLE",
           "CAPITAL_REQUIREMENT_UNKNOWN", "IN_FLIGHT_MARGIN_UNREADABLE", "INSUFFICIENT_FREE_MARGIN",
           "RESERVATION_PLAN_MISMATCH", "RESERVATION_EXPIRED", "RESERVATION_ACCOUNT_MISMATCH",
           "RESERVATION_BOT_MISMATCH", "RESERVATION_NOT_EXECUTION_SCOPED", "RateLimitEvidence", "BrokerEvidence",
           "CapitalEvidence", "evaluate_broker_health", "evaluate_capital_readiness", "minimum_order_margin",
           "readiness_document"]
