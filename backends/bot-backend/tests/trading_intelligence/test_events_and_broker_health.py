"""Closure items 2, 3, 12 -- normalized event risk (Forex-aware), maintenance
capability, and broker health wired from the runtime's canonical source."""
from __future__ import annotations

import dataclasses
import sqlite3
from types import SimpleNamespace

import pytest
from _helpers import clear_event_context, full_chain, healthy_system_context, instrument, permissive_veto_policy
from _pf import add_bot, fx, make_db

from app.risk.circuit import CircuitBreakerRegistry, CircuitState
from app.trading_intelligence.contracts.events import (
    EventRiskContext, MaintenanceContext, MarketEvent, event_affects, instrument_legs,
)
from app.trading_intelligence.contracts.system_health import BrokerHealthContext, SystemHealthContext
from app.trading_intelligence.integration.context_adapters import (
    broker_health_from_runner, build_event_risk_context, event_context_from_records, market_event_from_row,
)
from app.trading_intelligence.veto.engine import evaluate_veto

T = 1_790_000_000_000


def ev(currencies=(), assets=(), instruments=(), venues=(), t=T, pre=60_000, post=60_000, etype="INFLATION"):
    return MarketEvent("e", "cal", etype, t, "HIGH", affected_assets=tuple(assets), affected_currencies=tuple(currencies),
                       affected_instruments=tuple(instruments), affected_venues=tuple(venues),
                       pre_event_window_ms=pre, post_event_window_ms=post)


# ================================ FOREX EVENT SCOPE ==================================
def test_eurusd_affected_by_usd_event():
    assert event_affects(ev(currencies=("USD",)), fx("EURUSD"))


def test_eurjpy_not_affected_by_usd_event_unless_metadata_says_so():
    assert not event_affects(ev(currencies=("USD",)), fx("EURJPY"))
    assert event_affects(ev(currencies=("USD",), instruments=("EURJPY",)), fx("EURJPY"))


def test_eurjpy_affected_by_eur_event():
    assert event_affects(ev(currencies=("EUR",)), fx("EURJPY"))


def test_gbpjpy_affected_by_jpy_event():
    assert event_affects(ev(currencies=("JPY",)), fx("GBPJPY"))


def test_crypto_btc_event_does_not_contaminate_forex():
    btc_event = ev(assets=("BTC",), etype="INSTRUMENT_CHANGE")
    assert event_affects(btc_event, instrument("BTCUSDT"))
    for pair in ("EURUSD", "USDJPY", "GBPJPY"):
        assert not event_affects(btc_event, fx(pair))


def test_fx_legs_from_canonical_identity_and_stablecoin_alias_is_policy():
    assert instrument_legs(fx("EURUSD")) == (frozenset({"EUR", "USD"}), frozenset())
    cur, assets = instrument_legs(instrument("BTCUSDT"))
    assert cur == frozenset({"USD"}) and assets == frozenset({"BTC"})  # USDT -> USD via versioned alias


def test_event_time_window_boundaries_exact():
    e = ev(currencies=("USD",), t=T, pre=1000, post=2000)
    assert e.window_start == T - 1000 and e.window_end == T + 2000
    assert e.overlaps(T + 2000, T + 5000)  # touches end: inside (closed interval)
    assert not e.overlaps(T + 2001, T + 5000)
    assert e.overlaps(T - 5000, T - 1000)  # touches start
    assert not e.overlaps(T - 5000, T - 1001)


def test_normalized_types_from_existing_calendar_rows():
    usd = market_event_from_row({"event_id": "a", "event_type": "CPI", "country_currency": "USD", "impact_level": "HIGH",
                                 "scheduled_utc": "2026-05-29T13:30:00+00:00", "source": "manual_historical_backfill"})
    assert usd.event_type == "INFLATION" and usd.affected_currencies == ("USD",) and usd.affected_assets == ()
    eth = market_event_from_row({"event_id": "b", "event_type": "ETH_UPGRADE", "country_currency": "ETH",
                                 "scheduled_utc": "2026-05-29T13:30:00+00:00", "source": "dev-proof"})
    assert eth.affected_assets == ("ETH",) and eth.affected_currencies == () and eth.source_quality == "UNVERIFIED_TEST_SOURCE"
    assert market_event_from_row({"event_type": "FOMC", "country_currency": "USD", "scheduled_utc": "x"}) is None


# ============================ SOURCE AVAILABILITY ==================================
def test_stale_calendar_is_explicit():
    stale = event_context_from_records([], [], staleness_hours=100.0, now_ms=T)
    horizon = event_context_from_records([], [], staleness_hours=1.0, now_ms=T, latest_scheduled_ms=T - 1)
    assert stale.source_state == horizon.source_state == "STALE"
    assert "CALENDAR_SYNC_STALE" in stale.reason_codes and "CALENDAR_HORIZON_EXPIRED" in horizon.reason_codes


def test_unavailable_event_source_is_explicit(tmp_path):
    class Broken:
        def connect(self):
            raise sqlite3.OperationalError("no db")
    ctx = build_event_risk_context(Broken(), T)
    assert ctx.source_state == "UNAVAILABLE" and not ctx.source_available and ctx.events == ()


def test_real_calendar_tables_are_read(tmp_path):
    db = make_db(tmp_path / "ev.db")
    from shared_lib.persistence.economic_events import insert_event
    from datetime import datetime, timezone
    now = datetime.fromtimestamp(T / 1000, tz=timezone.utc)
    insert_event(db, title="US CPI", event_type="CPI", country_currency="USD", impact_level="HIGH",
                 scheduled_utc=datetime.fromtimestamp((T + 3_600_000) / 1000, tz=timezone.utc).isoformat(), event_id="cpi1")
    with db.connect() as c:  # freshen sync time relative to T
        c.execute("UPDATE economic_events SET updated_at=?", (now.isoformat(),))
    ctx = build_event_risk_context(db, T)
    assert ctx.source_state == "AVAILABLE" and [e.event_id for e in ctx.events] == ["cpi1"]
    assert ctx.maintenance.state == "UNAVAILABLE"  # no maintenance feed exists: never "none scheduled"


def _veto(chain, **kw):
    kw.setdefault("system_context", healthy_system_context())
    kw.setdefault("event_context", clear_event_context())
    return evaluate_veto(opportunity=chain["opportunity"], candidate=chain["candidate"], market_state=chain["market_state"],
                         regime_distribution=chain["regime"], forecast=chain["forecast"], cost_estimate=chain["cost"],
                         policy=kw.pop("policy", permissive_veto_policy()), **kw)


@pytest.fixture(scope="module")
def chain():
    return full_chain()


def _check(d, name):
    return next(c for c in d.checks if c.check == name)


def test_maintenance_unavailable_is_not_no_maintenance(chain):
    ctx = dataclasses.replace(clear_event_context(), maintenance=MaintenanceContext.unavailable())
    d = _veto(chain, event_context=ctx)
    c = _check(d, "maintenance_source")
    assert c.status == "NOT_EVALUATED" and c.reason_code == "MAINTENANCE_SOURCE_UNAVAILABLE"
    assert not any(x.check == "maintenance_window" and x.status == "PASS" for x in d.checks)
    strict = _veto(chain, event_context=ctx, policy=permissive_veto_policy(maintenance_unavailable_status="WATCH"))
    assert strict.outcome == "WATCH"


def test_maintenance_available_window_rejects_only_affected_venue(chain):
    t = chain["candidate"].decision_time
    w = MarketEvent("m", "venue_feed", "EXCHANGE_MAINTENANCE", t + 1000, "HIGH", affected_venues=("binance",),
                    pre_event_window_ms=1000, post_event_window_ms=1000)
    other = dataclasses.replace(w, affected_venues=("bybit",))
    hit = _veto(chain, event_context=dataclasses.replace(clear_event_context(), maintenance=MaintenanceContext("AVAILABLE", (w,))))
    miss = _veto(chain, event_context=dataclasses.replace(clear_event_context(), maintenance=MaintenanceContext("AVAILABLE", (other,))))
    assert hit.outcome == "REJECT" and "EXCHANGE_MAINTENANCE" in hit.reason_codes
    assert miss.outcome == "APPROVE_FOR_RANKING"


def test_stale_maintenance_source_is_watch(chain):
    ctx = dataclasses.replace(clear_event_context(), maintenance=MaintenanceContext("STALE"))
    d = _veto(chain, event_context=ctx)
    assert d.outcome == "WATCH" and "MAINTENANCE_SOURCE_STALE" in d.reason_codes


# ================================== BROKER HEALTH ==================================
class _Client:
    pass


def _runner(db, bot, account, registry):
    ctx = SimpleNamespace(bot_instance_id=bot, broker_account_id=account, execution_mode="DEMO", user_id="u1")
    r = SimpleNamespace(context=ctx, db=db, client=_Client(), circuit_registry=registry, _circuit_id=f"{bot}:{account}")
    registry.get_breaker(r._circuit_id)  # the runner registers its breaker at construction
    return r


@pytest.fixture
def registry():
    reg = CircuitBreakerRegistry()
    reg.reset_all()
    yield reg
    reg.reset_all()


def test_broker_health_reads_the_runtime_circuit_breaker(tmp_path, registry):
    db = make_db(tmp_path / "bh.db")
    add_bot(db, "botA", "acct1")
    r = _runner(db, "botA", "acct1", registry)
    assert broker_health_from_runner(r).status == "HEALTHY"
    br = registry.get_breaker(r._circuit_id)
    br.state = CircuitState.DEGRADED
    assert broker_health_from_runner(r).status == "DEGRADED"
    br.state, br.last_trip_time = CircuitState.HALTED, 10**12
    h = broker_health_from_runner(r)
    assert h.status == "UNAVAILABLE" and "CIRCUIT_HALTED" in h.reason_codes
    assert h.broker_account_id == "acct1" and h.source.startswith("circuit_breaker_registry")


def test_broker_quarantine_flag_marks_unavailable(tmp_path, registry):
    db = make_db(tmp_path / "bh.db")
    add_bot(db, "botA", "acct1")
    with db.connect() as c:
        c.execute("UPDATE bot_instances SET broker_health_status='broker_blocked' WHERE id='botA'")
    h = broker_health_from_runner(_runner(db, "botA", "acct1", registry))
    assert h.status == "UNAVAILABLE" and "BROKER_QUARANTINED" in h.reason_codes


def test_broker_health_is_tenant_scoped(tmp_path, registry):
    db = make_db(tmp_path / "bh.db")
    add_bot(db, "botA", "acct1")
    add_bot(db, "botB", "acct2")
    ra, rb = _runner(db, "botA", "acct1", registry), _runner(db, "botB", "acct2", registry)
    b = registry.get_breaker(ra._circuit_id)
    b.state, b.last_trip_time = CircuitState.HALTED, 10**12
    assert broker_health_from_runner(ra).status == "UNAVAILABLE"
    assert broker_health_from_runner(rb).status == "HEALTHY"


def test_unknown_only_when_source_cannot_be_read(registry):
    class NoDB:
        def connect(self):
            raise sqlite3.OperationalError("down")
    ctx = SimpleNamespace(bot_instance_id="botZ", broker_account_id="acctZ", execution_mode="DEMO")
    r = SimpleNamespace(context=ctx, db=NoDB(), client=None, circuit_registry=registry, _circuit_id="unregistered:key")
    h = broker_health_from_runner(r)
    assert h.status == "UNKNOWN" and {"CIRCUIT_STATE_UNREADABLE", "QUARANTINE_STATE_UNREADABLE"} <= set(h.reason_codes)


def _bh(status, reasons=(), freshness=0):
    return SystemHealthContext(broker_health=BrokerHealthContext("acct1", "binance", "DEMO", status, T, "test",
                                                                 freshness, tuple(reasons)))


def test_veto_distinguishes_all_broker_states(chain):
    assert _veto(chain, system_context=_bh("HEALTHY")).outcome == "APPROVE_FOR_RANKING"
    d = _veto(chain, system_context=_bh("DEGRADED", ("CIRCUIT_DEGRADED",)))
    assert d.outcome == "WATCH" and "BROKER_DEGRADED" in d.reason_codes
    sev = _veto(chain, system_context=_bh("DEGRADED", ("CIRCUIT_RECOVERY_AFTER_HALT",)))
    assert sev.outcome == "REJECT"
    u = _veto(chain, system_context=_bh("UNAVAILABLE"))
    assert u.outcome == "REJECT" and "BROKER_UNAVAILABLE" in u.reason_codes
    k = _veto(chain, system_context=_bh("UNKNOWN"))
    assert k.outcome == "WATCH" and "BROKER_UNKNOWN" in k.reason_codes
    stale = _veto(chain, system_context=_bh("HEALTHY", freshness=10**9))
    assert stale.outcome == "WATCH" and "BROKER_HEALTH_STALE" in stale.reason_codes


def test_unwired_broker_health_is_distinct_from_unknown(chain):
    d = _veto(chain, system_context=SystemHealthContext())
    assert "BROKER_HEALTH_NOT_PROVIDED" in d.reason_codes and "BROKER_UNKNOWN" not in d.reason_codes
    assert d.outcome == "WATCH"


def test_cycle_shadow_wires_runtime_broker_health(monkeypatch):
    """The runner hook passes the canonical broker-health context into CATI."""
    from app.trading_intelligence.integration import cycle_shadow as cs
    import inspect
    src = inspect.getsource(cs.record_symbol)
    assert "system_context_from_runner" in src and "system_context=" in src
