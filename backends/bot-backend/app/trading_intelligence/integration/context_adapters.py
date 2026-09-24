"""Adapters from EXISTING runtime infrastructure to CATI's typed veto inputs.
No external news/web API and no second health system is added.

Events (Section 14.13)
    Source: the repo's economic calendar (``shared_lib.persistence.
    economic_events``: ``economic_events`` + ``event_blackout_windows``).
    Each row becomes a normalized ``MarketEvent`` scoped by currency
    (``country_currency``) or asset -- asset-class neutral, so a USD release
    reaches EURUSD/USDJPY/BTCUSDT (USD-pegged settlement) but not EURJPY.
    The calendar is STALE when it has not been synced recently OR when it
    has no scheduled event at/after ``now`` (it no longer covers the future);
    it is UNAVAILABLE when it cannot be read. Neither state is an all-clear.

Maintenance
    No venue maintenance feed exists anywhere in the repo, so maintenance is
    reported as ``MaintenanceSourceState.UNAVAILABLE`` -- never as "no
    maintenance scheduled". A future venue feed implements
    ``MaintenanceProvider`` and plugs in here.

Broker health (Section 14.14)
    Source: the runtime's canonical broker-health state --
    (1) the per-bot, per-broker-account circuit breaker the runner feeds and
    ``PolicyEngine`` gates entries on (``app.risk.circuit``), and
    (2) the persisted broker quarantine flag
    ``bot_instances.broker_health_status`` (``broker_blocked``).
    Scoped to the runner's own ``{bot_id}:{broker_account_id}`` key, so one
    account's failures never mark another account degraded.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Iterable, List, Mapping, Optional, Protocol, Tuple

from app.trading_intelligence.contracts.events import (
    EventRiskContext, EventSourceState, MaintenanceContext, MarketEvent, MarketEventType,
)
from app.trading_intelligence.contracts.system_health import (
    BrokerHealthContext, BrokerHealthStatus, SystemHealthContext,
)

logger = logging.getLogger(__name__)

#: A calendar not synced for this long is not a reliable "no events" source.
MAX_FEED_STALENESS_HOURS = 48.0

#: economic_events.event_type -> normalized MarketEventType (versioned data).
EVENT_TYPE_MAP: Mapping[str, str] = {
    "FOMC": MarketEventType.CENTRAL_BANK.value, "FOMC_MINUTES": MarketEventType.CENTRAL_BANK.value,
    "FED_CHAIR_SPEECH": MarketEventType.CENTRAL_BANK.value, "ECB": MarketEventType.CENTRAL_BANK.value,
    "BOE": MarketEventType.CENTRAL_BANK.value, "BOJ": MarketEventType.CENTRAL_BANK.value,
    "RATE_DECISION": MarketEventType.RATE_DECISION.value, "INTEREST_RATE_DECISION": MarketEventType.RATE_DECISION.value,
    "CPI": MarketEventType.INFLATION.value, "CORE_CPI": MarketEventType.INFLATION.value,
    "PPI": MarketEventType.INFLATION.value, "CORE_PPI": MarketEventType.INFLATION.value,
    "PCE": MarketEventType.INFLATION.value, "CORE_PCE": MarketEventType.INFLATION.value,
    "NFP": MarketEventType.EMPLOYMENT.value, "UNEMPLOYMENT_RATE": MarketEventType.EMPLOYMENT.value,
    "AVERAGE_HOURLY_EARNINGS": MarketEventType.EMPLOYMENT.value, "JOBLESS_CLAIMS": MarketEventType.EMPLOYMENT.value,
    "GDP": MarketEventType.GDP.value,
    "ISM_MANUFACTURING_PMI": MarketEventType.PMI.value, "ISM_SERVICES_PMI": MarketEventType.PMI.value,
    "PMI": MarketEventType.PMI.value,
    "ETH_UPGRADE": MarketEventType.INSTRUMENT_CHANGE.value,
}

#: ISO-4217 codes treated as CURRENCY scope; any other ``country_currency``
#: value (e.g. "ETH") is kept as ASSET scope -- never dropped.
FIAT_CURRENCIES = frozenset({
    "USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD", "SEK", "NOK", "DKK", "CNY", "CNH", "HKD", "SGD",
    "KRW", "INR", "MXN", "BRL", "ZAR", "TRY", "PLN", "CZK", "HUF", "ILS", "THB", "IDR", "MYR", "PHP", "TWD",
    "RUB", "SAR", "AED", "CLP", "COP", "PEN", "ARS", "NGN", "EGP", "KES",
})
_TEST_SOURCE_MARKERS = ("dev", "proof", "test", "validation", "fixture", "synthetic")


def _to_ms(iso: Any) -> Optional[int]:
    try:
        dt = datetime.fromisoformat(str(iso))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except (TypeError, ValueError):
        return None


def _source_quality(source: str) -> str:
    s = str(source or "").lower()
    return "UNVERIFIED_TEST_SOURCE" if any(m in s for m in _TEST_SOURCE_MARKERS) else "VALID"


def market_event_from_row(row: Mapping[str, Any], *, pre_ms: int = 30 * 60_000, post_ms: int = 30 * 60_000) -> Optional[MarketEvent]:
    """One ``economic_events`` row -> MarketEvent (None if unparseable)."""
    t = _to_ms(row.get("scheduled_utc"))
    if t is None:
        return None
    scope = str(row.get("country_currency") or "").strip().upper()
    currencies = (scope,) if scope in FIAT_CURRENCIES else ()
    assets = (scope,) if scope and scope not in FIAT_CURRENCIES else ()
    return MarketEvent(
        event_id=str(row.get("event_id") or row.get("id")), source=str(row.get("source") or "economic_events"),
        event_type=EVENT_TYPE_MAP.get(str(row.get("event_type") or "").upper(), MarketEventType.MACRO.value),
        scheduled_time=t, importance=str(row.get("impact_level") or "MEDIUM").upper(),
        affected_currencies=currencies, affected_assets=assets,
        pre_event_window_ms=pre_ms, post_event_window_ms=post_ms,
        source_updated_at=_to_ms(row.get("updated_at")), source_quality=_source_quality(row.get("source")),
    )


def _blackout_event(w: Mapping[str, Any]) -> Optional[MarketEvent]:
    start, end = _to_ms(w.get("start_utc")), _to_ms(w.get("end_utc"))
    if start is None or end is None or end < start:
        return None
    symbols = w.get("affected_symbols")
    scoped = [] if (w.get("is_global") or not symbols) else [
        s.strip() for s in str(symbols).replace("[", "").replace("]", "").replace('"', "").split(",") if s.strip()]
    scope = str(w.get("country_currency") or "").strip().upper()
    return MarketEvent(
        event_id=f"blackout_{w.get('id')}", source="event_blackout_windows",
        event_type=EVENT_TYPE_MAP.get(str(w.get("event_type") or "").upper(), MarketEventType.MACRO.value),
        scheduled_time=start, importance=str(w.get("impact_level") or "HIGH").upper(),
        affected_instruments=tuple(scoped),
        # A global blackout keeps its original scope (the event's currency/asset); it
        # is never widened to every asset class of every account.
        affected_currencies=(scope,) if (not scoped and scope in FIAT_CURRENCIES) else (),
        affected_assets=(scope,) if (not scoped and scope and scope not in FIAT_CURRENCIES) else (),
        pre_event_window_ms=0, post_event_window_ms=end - start,
    )


def event_context_from_records(
    active_windows: List[dict], upcoming_events: List[dict], *, staleness_hours: Optional[float], now_ms: int,
    latest_scheduled_ms: Optional[int] = None, maintenance: Optional[MaintenanceContext] = None,
) -> EventRiskContext:
    """Pure conversion (unit-testable without a DB)."""
    maint = maintenance or MaintenanceContext.unavailable(as_of=now_ms)
    if staleness_hours is None:
        return EventRiskContext(source_state=EventSourceState.UNAVAILABLE.value, maintenance=maint, as_of=now_ms,
                                source="economic_events", reason_codes=("CALENDAR_NEVER_SYNCED",))
    reasons = []
    if staleness_hours > MAX_FEED_STALENESS_HOURS:
        reasons.append("CALENDAR_SYNC_STALE")
    if latest_scheduled_ms is not None and latest_scheduled_ms < now_ms:
        reasons.append("CALENDAR_HORIZON_EXPIRED")
    if reasons:
        return EventRiskContext(source_state=EventSourceState.STALE.value, maintenance=maint, as_of=now_ms,
                                source="economic_events", reason_codes=tuple(reasons))
    events = [e for e in (_blackout_event(w) for w in active_windows) if e is not None]
    events += [e for e in (market_event_from_row(r) for r in upcoming_events) if e is not None]
    events.sort(key=lambda e: (e.scheduled_time, e.event_id))
    return EventRiskContext(source_state=EventSourceState.AVAILABLE.value, events=tuple(events), maintenance=maint,
                            as_of=now_ms, source="economic_events")


class MaintenanceProvider(Protocol):
    """Interface a venue maintenance feed implements when one exists."""

    def maintenance_context(self, venue: str, now_ms: int) -> MaintenanceContext:
        ...


def build_event_risk_context(db: Any, now_ms: int, *, lookahead_hours: int = 4,
                             maintenance_provider: Optional[MaintenanceProvider] = None,
                             venue: Optional[str] = None) -> EventRiskContext:
    """Reads the existing economic-event tables. Any failure => UNAVAILABLE
    (fail closed), never an empty-but-available context."""
    maint = MaintenanceContext.unavailable(as_of=now_ms)
    if maintenance_provider is not None and venue:
        try:
            maint = maintenance_provider.maintenance_context(venue, now_ms)
        except Exception as exc:
            logger.warning("[CATI] maintenance source failed: %s", exc)
            maint = MaintenanceContext.unavailable(as_of=now_ms, reason="MAINTENANCE_SOURCE_ERROR")
    try:
        from datetime import timedelta

        from shared_lib.persistence.economic_events import (
            get_active_blackout_windows, get_last_sync_utc, get_upcoming_events,
        )

        now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        last_sync = get_last_sync_utc(db)
        staleness = None
        if last_sync:
            last = datetime.fromisoformat(last_sync)
            last = last if last.tzinfo else last.replace(tzinfo=timezone.utc)
            staleness = (now - last).total_seconds() / 3600.0
        with db.connect() as conn:
            row = conn.execute("SELECT MAX(scheduled_utc) FROM economic_events").fetchone()
        latest = _to_ms(row[0]) if row and row[0] else None
        active = get_active_blackout_windows(db, now.isoformat())
        upcoming = get_upcoming_events(db, now.isoformat(), (now + timedelta(hours=lookahead_hours)).isoformat(), ["HIGH"])
        return event_context_from_records(active, upcoming, staleness_hours=staleness, now_ms=now_ms,
                                          latest_scheduled_ms=latest, maintenance=maint)
    except Exception as exc:
        logger.warning("[CATI] event context unavailable: %s", exc)
        return EventRiskContext(source_state=EventSourceState.UNAVAILABLE.value, maintenance=maint, as_of=now_ms,
                                source="economic_events", reason_codes=("CALENDAR_READ_FAILED",))


# -- broker health -----------------------------------------------------------------------
_CIRCUIT_TO_STATUS = {
    "NORMAL": (BrokerHealthStatus.HEALTHY.value, ()),
    "DEGRADED": (BrokerHealthStatus.DEGRADED.value, ("CIRCUIT_DEGRADED",)),
    "RECOVERY": (BrokerHealthStatus.DEGRADED.value, ("CIRCUIT_RECOVERY_AFTER_HALT",)),
    "HALTED": (BrokerHealthStatus.UNAVAILABLE.value, ("CIRCUIT_HALTED",)),
}


def broker_health_from_sources(
    *, circuit_state: Optional[str], quarantine_status: Optional[str], broker_account_id: Optional[str],
    venue: Optional[str], environment: Optional[str], observed_at: int, circuit_readable: bool = True,
    quarantine_readable: bool = True,
) -> BrokerHealthContext:
    """Pure combination of the two canonical sources. Worst state wins.
    UNKNOWN only when NEITHER source could be read."""
    reasons: List[str] = []
    statuses: List[str] = []
    if circuit_readable and circuit_state is not None:
        st, rs = _CIRCUIT_TO_STATUS.get(str(circuit_state).upper(), (BrokerHealthStatus.UNKNOWN.value, ("CIRCUIT_STATE_UNRECOGNISED",)))
        statuses.append(st)
        reasons += rs
    else:
        reasons.append("CIRCUIT_STATE_UNREADABLE")
    if quarantine_readable:
        if str(quarantine_status or "ok").lower() == "broker_blocked":
            statuses.append(BrokerHealthStatus.UNAVAILABLE.value)
            reasons.append("BROKER_QUARANTINED")
    else:
        reasons.append("QUARANTINE_STATE_UNREADABLE")
    if not statuses:
        status = BrokerHealthStatus.UNKNOWN.value
    else:
        order = [BrokerHealthStatus.UNAVAILABLE.value, BrokerHealthStatus.UNKNOWN.value,
                 BrokerHealthStatus.DEGRADED.value, BrokerHealthStatus.HEALTHY.value]
        status = min(statuses, key=order.index)
    return BrokerHealthContext(
        broker_account_id=broker_account_id, venue=venue, environment=environment, status=status,
        observed_at=observed_at, source="circuit_breaker_registry+bot_instances.broker_health_status",
        freshness_ms=0, reason_codes=tuple(dict.fromkeys(reasons)),
    )


def broker_health_from_runner(runner: Any, now_ms: Optional[int] = None) -> BrokerHealthContext:
    """Read the runner's own canonical broker-health state. Never raises."""
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    ctx = getattr(runner, "context", None)
    account = getattr(ctx, "broker_account_id", None)
    bot_id = getattr(ctx, "bot_instance_id", None)
    venue = type(getattr(runner, "client", None)).__name__ if getattr(runner, "client", None) is not None else None
    environment = getattr(ctx, "execution_mode", None)
    circuit_state, circuit_ok = None, False
    try:
        key = getattr(runner, "_circuit_id", None)
        registry = getattr(runner, "circuit_registry", None)
        if key and registry is not None:
            circuit_state = registry.get_all_states().get(key)  # non-creating read
            circuit_ok = circuit_state is not None
    except Exception as exc:
        logger.warning("[CATI] circuit state unreadable: %s", exc)
    quarantine, quarantine_ok = None, False
    try:
        db = getattr(runner, "db", None)
        if db is not None and bot_id:
            with db.connect() as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()}
                if "broker_health_status" in cols:
                    row = conn.execute("SELECT broker_health_status FROM bot_instances WHERE id=?", (bot_id,)).fetchone()
                    quarantine = row[0] if row else None
                quarantine_ok = True
    except Exception as exc:
        logger.warning("[CATI] broker quarantine state unreadable: %s", exc)
    return broker_health_from_sources(
        circuit_state=circuit_state, quarantine_status=quarantine, broker_account_id=account, venue=venue,
        environment=environment, observed_at=now_ms, circuit_readable=circuit_ok, quarantine_readable=quarantine_ok,
    )


def system_context_from_runner(runner: Any, *, component_errors: Iterable[str] = (), now_ms: Optional[int] = None) -> SystemHealthContext:
    return SystemHealthContext(broker_health=broker_health_from_runner(runner, now_ms),
                               component_errors=tuple(component_errors))


__all__ = [
    "MAX_FEED_STALENESS_HOURS", "EVENT_TYPE_MAP", "FIAT_CURRENCIES", "market_event_from_row",
    "event_context_from_records", "MaintenanceProvider", "build_event_risk_context",
    "broker_health_from_sources", "broker_health_from_runner", "system_context_from_runner",
]
