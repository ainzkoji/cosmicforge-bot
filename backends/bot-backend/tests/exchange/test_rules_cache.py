"""Step 4.3 -- the persistent market-rule cache: refusal instead of defaults.

Proven here, against a stub adapter and a real SQLite file: every refusal is its own reason (empty cache,
stale, incomplete, not trading, delisted, conflicting, changed since the plan), the refresh behaviour under
outage, partial and implausible listings, bounded backoff, recovery, change history, and the quantisers.

Not proven here: that any venue's rules are read correctly. That is the adapters' contract suite.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Dict, List, Optional

import pytest

from app.exchange.platform_adapter import contract as C
from app.exchange.platform_adapter import rules_cache as RC
from shared_lib.persistence.cati_exchange_schema import RULES_TABLE
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB

NOW = 1_800_000_000_000
HOUR = 3_600_000
VENUE, ENV = C.BINANCE, C.DEMO


def rules(symbol: str = "BTCUSDT", *, fetched_at_ms: int = NOW, **over) -> C.InstrumentRules:
    base = dict(venue=VENUE, environment=ENV, symbol=symbol, venue_symbol=symbol, status=C.TRADING,
                venue_status="TRADING", base_asset=symbol[:-4], quote_asset="USDT", margin_asset="USDT",
                price_tick=0.1, quantity_step=0.001, min_quantity=0.001, max_quantity=1000.0, min_notional=100.0,
                max_leverage=125.0, contract_size=1.0, fetched_at_ms=fetched_at_ms, market_max_quantity=120.0,
                raw_hash="raw")
    return C.InstrumentRules(**{**base, **over})


class StubAdapter:
    """A venue listing the test controls. ``listing`` is what the venue would answer now."""
    venue, environment, adapter_version = VENUE, ENV, "stub-1.0"

    def __init__(self, *records: C.InstrumentRules) -> None:
        self.listing: Dict[str, C.InstrumentRules] = {r.symbol: r for r in records}
        self.fail: Optional[C.AdapterError] = None
        self.requests: List[Optional[tuple]] = []

    def capabilities(self) -> C.AdapterCapabilities:
        return C.AdapterCapabilities(
            venue=self.venue, environment=self.environment, native_stop_market=True, native_take_profit=True,
            reduce_only=True, close_position_stop=True, stop_amend_in_place=False, multiple_stops_per_position=True,
            one_way_mode=True, hedge_mode=False, user_stream=True, demo_environment=True, client_order_id_max_length=36)

    def instrument_rules(self, symbols=None):
        self.requests.append(None if symbols is None else tuple(symbols))
        if self.fail is not None:
            raise self.fail
        return {s: r for s, r in self.listing.items() if symbols is None or s in symbols}


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "rules.db"))
    ensure_cati_platform_schema(database)
    return database


@pytest.fixture
def cache(db):
    return RC.RulesCache(db, worker="w1")


def loaded(cache, *records, now: int = NOW) -> StubAdapter:
    adapter = StubAdapter(*(records or (rules(),)))
    assert cache.refresh(adapter, now).ok
    return adapter


# ------------------------------------------------------------------------------------------ refusals
def test_an_empty_cache_refuses_and_invents_nothing(cache):
    for ask in (cache.rules_for_planning, cache.rules_for_reduce):
        decision = ask(VENUE, ENV, "BTCUSDT", NOW)
        assert not decision.ok and decision.reasons == (RC.RULES_UNKNOWN_INSTRUMENT,) and decision.rules is None
    entry = cache.rules_for_entry(VENUE, ENV, "BTCUSDT", NOW, plan_time_ms=NOW)
    assert not entry.ok and entry.reason == RC.RULES_UNKNOWN_INSTRUMENT
    with pytest.raises(RC.RulesRefused) as refused:
        entry.require()
    assert refused.value.reason == RC.RULES_UNKNOWN_INSTRUMENT


def test_fresh_complete_rules_are_returned_exactly_as_the_venue_gave_them(cache):
    record = rules()
    loaded(cache, record)
    decision = cache.rules_for_planning(VENUE, ENV, "btc-usdt", NOW + HOUR)
    assert decision.ok and decision.rules == record and decision.rules_hash == RC.rules_hash(record)
    assert decision.fetched_at_ms == NOW and decision.expires_at_ms == NOW + RC.DEFAULT_MAX_AGE_MS
    assert cache.capabilities(VENUE, ENV, "BTCUSDT")["native_stop_market"] is True


def test_a_stale_cache_refuses_entries_and_keeps_a_reduce_possible_with_the_reason_visible(cache):
    loaded(cache)
    edge = NOW + RC.DEFAULT_MAX_AGE_MS
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", edge).ok
    stale = cache.rules_for_entry(VENUE, ENV, "BTCUSDT", edge + 1, plan_time_ms=NOW)
    assert not stale.ok and stale.reasons == (RC.RULES_STALE,) and stale.rules is None
    # emergency maintenance and flatten: the venue's own last step and tick, never a default, flagged degraded
    reduce = cache.rules_for_reduce(VENUE, ENV, "BTCUSDT", edge + 1)
    assert reduce.ok and reduce.degraded == (RC.RULES_STALE,) and reduce.rules.quantity_step == 0.001
    # a stricter caller is honoured without a refresh
    strict = RC.RulesCache(cache.db, max_age_ms=HOUR)
    assert strict.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW + HOUR + 1).reasons == (RC.RULES_STALE,)


def test_rules_stamped_in_the_future_are_refused_beyond_the_clock_tolerance(cache):
    loaded(cache, rules(fetched_at_ms=NOW + 60_000))
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW).ok            # within the tolerance
    early = NOW - RC.CLOCK_AHEAD_TOLERANCE_MS
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", early).reasons == (RC.RULES_DATED_IN_THE_FUTURE,)


@pytest.mark.parametrize("missing", ["price_tick", "quantity_step", "min_quantity", "min_notional"])
def test_incomplete_rules_are_stored_as_they_are_and_refused(cache, db, missing):
    loaded(cache, rules(**{missing: None}))
    decision = cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW)
    assert not decision.ok and decision.reasons == (RC.RULES_INCOMPLETE,)
    with db.connect() as conn:  # the unknown value stays unknown in the database: no zero, no default
        assert conn.execute(f"SELECT {missing}, complete FROM {RULES_TABLE}").fetchone()[:] == (None, 0)
    reduce = cache.rules_for_reduce(VENUE, ENV, "BTCUSDT", NOW)
    assert reduce.ok == (missing != "quantity_step")       # without a step even a close cannot be quantised
    if reduce.ok:
        assert reduce.degraded == (RC.RULES_INCOMPLETE,) and getattr(reduce.rules, missing) is None


def test_an_instrument_that_is_not_trading_is_refused_for_entries(cache):
    loaded(cache, rules(status=C.NOT_TRADING, venue_status="SETTLING"))
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW).reasons == (RC.RULES_INSTRUMENT_NOT_TRADING,)
    assert cache.rules_for_reduce(VENUE, ENV, "BTCUSDT", NOW).degraded == (RC.RULES_INSTRUMENT_NOT_TRADING,)


def test_an_entry_must_say_which_rules_its_plan_used(cache):
    loaded(cache)
    assert cache.rules_for_entry(VENUE, ENV, "BTCUSDT", NOW).reasons == (RC.RULES_PLAN_REFERENCE_MISSING,)
    assert cache.rules_for_entry(VENUE, ENV, "BTCUSDT", NOW, plan_time_ms=NOW).ok
    assert cache.rules_for_entry(VENUE, ENV, "BTCUSDT", NOW, plan_rules_hash=RC.rules_hash(rules())).ok


def test_rules_of_one_environment_or_venue_never_answer_for_another(cache):
    loaded(cache)
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW).ok
    for venue, environment in ((VENUE, C.LIVE), (C.BYBIT, ENV), (C.BINGX, C.LIVE)):
        assert cache.rules_for_planning(venue, environment, "BTCUSDT", NOW).reasons == (RC.RULES_UNKNOWN_INSTRUMENT,)
        assert cache.rules_for_reduce(venue, environment, "BTCUSDT", NOW).reasons == (RC.RULES_UNKNOWN_INSTRUMENT,)


def test_every_refusal_is_a_distinct_reason():
    reasons = [RC.RULES_UNKNOWN_INSTRUMENT, RC.RULES_STALE, RC.RULES_DATED_IN_THE_FUTURE, RC.RULES_INCOMPLETE,
               RC.RULES_INSTRUMENT_NOT_TRADING, RC.RULES_DELISTED, RC.RULES_CONFLICT, RC.RULES_CHANGED_SINCE_PLAN,
               RC.RULES_PLAN_REFERENCE_MISSING]
    assert len(set(reasons)) == len(reasons)


# ----------------------------------------------------------------------------------------- filter changes
def test_a_filter_change_refuses_the_plan_made_before_it_and_is_kept_in_the_history(cache):
    adapter = loaded(cache)
    plan_hash, plan_time = cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW).rules_hash, NOW + 1_000
    adapter.listing["BTCUSDT"] = rules(fetched_at_ms=NOW + HOUR, quantity_step=0.01, min_quantity=0.01)
    result = cache.refresh(adapter, NOW + HOUR)
    assert result.ok and (result.changed, result.unchanged, result.first_seen) == (1, 0, 0)
    for reference in (dict(plan_rules_hash=plan_hash), dict(plan_time_ms=plan_time)):
        decision = cache.rules_for_entry(VENUE, ENV, "BTCUSDT", NOW + HOUR, **reference)
        assert not decision.ok and decision.reasons == (RC.RULES_CHANGED_SINCE_PLAN,)
    # a plan made after the change, with the new rules, is admitted
    fresh = cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW + HOUR)
    assert fresh.rules.quantity_step == 0.01
    assert cache.rules_for_entry(VENUE, ENV, "BTCUSDT", NOW + HOUR, plan_rules_hash=fresh.rules_hash,
                                 plan_time_ms=NOW + HOUR).ok
    history = cache.history(VENUE, ENV, "BTCUSDT")
    assert [h["change_type"] for h in history] == [RC.FIRST_SEEN, RC.RULES_CHANGED]
    assert history[1]["changed_fields"] == ["quantity_step", "min_quantity"]
    assert history[1]["previous_hash"] == plan_hash and history[1]["document"]["quantity_step"] == 0.01


def test_an_unchanged_refresh_changes_nothing_but_the_age(cache):
    adapter = loaded(cache)
    adapter.listing["BTCUSDT"] = rules(fetched_at_ms=NOW + HOUR, raw_hash="another venue payload, same rules")
    result = cache.refresh(adapter, NOW + HOUR)
    assert (result.unchanged, result.changed) == (1, 0)
    assert cache.rules_for_entry(VENUE, ENV, "BTCUSDT", NOW + HOUR, plan_time_ms=NOW).ok
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW + HOUR).fetched_at_ms == NOW + HOUR
    assert len(cache.history(VENUE, ENV, "BTCUSDT")) == 1


def test_a_status_change_is_recorded_as_one(cache):
    adapter = loaded(cache)
    adapter.listing["BTCUSDT"] = rules(fetched_at_ms=NOW + 1, status=C.NOT_TRADING, venue_status="BREAK")
    cache.refresh(adapter, NOW + 1)
    assert cache.history(VENUE, ENV, "BTCUSDT")[-1]["change_type"] == RC.STATUS_CHANGED
    assert cache.rules_for_entry(VENUE, ENV, "BTCUSDT", NOW + 1, plan_time_ms=NOW).reasons == (
        RC.RULES_INSTRUMENT_NOT_TRADING, RC.RULES_CHANGED_SINCE_PLAN)


def test_a_late_answer_never_replaces_a_newer_one(cache):
    adapter = loaded(cache, rules(fetched_at_ms=NOW))
    adapter.listing["BTCUSDT"] = rules(fetched_at_ms=NOW - HOUR, quantity_step=0.5)
    result = cache.refresh(adapter, NOW + 1)
    assert result.skipped_older == 1 and result.stored == 0
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW + 1).rules.quantity_step == 0.001


# ------------------------------------------------------------------------------------------- delisting
def test_a_delisted_instrument_is_refused_kept_and_can_come_back(cache, db):
    adapter = loaded(cache, rules("BTCUSDT"), rules("ETHUSDT"))
    del adapter.listing["ETHUSDT"]
    result = cache.refresh(adapter, NOW + HOUR)
    assert result.ok and result.delisted == 1
    gone = cache.rules_for_entry(VENUE, ENV, "ETHUSDT", NOW + HOUR, plan_time_ms=NOW)
    assert not gone.ok and gone.reasons == (RC.RULES_DELISTED, RC.RULES_CHANGED_SINCE_PLAN)
    # an open position on it can still be closed with the last known step
    assert cache.rules_for_reduce(VENUE, ENV, "ETHUSDT", NOW + HOUR).degraded == (RC.RULES_DELISTED,)
    with db.connect() as conn:  # marked, never deleted
        assert conn.execute(f"SELECT COUNT(*) FROM {RULES_TABLE}").fetchone()[0] == 2
    adapter.listing["ETHUSDT"] = rules("ETHUSDT", fetched_at_ms=NOW + 2 * HOUR)
    assert cache.refresh(adapter, NOW + 2 * HOUR).relisted == 1
    assert cache.rules_for_planning(VENUE, ENV, "ETHUSDT", NOW + 2 * HOUR).ok
    assert [h["change_type"] for h in cache.history(VENUE, ENV, "ETHUSDT")] == [RC.FIRST_SEEN, RC.DELISTED, RC.RELISTED]
    # a plan from before the delisting is not grandfathered by the relisting
    assert cache.rules_for_entry(VENUE, ENV, "ETHUSDT", NOW + 2 * HOUR, plan_time_ms=NOW).reasons == (
        RC.RULES_CHANGED_SINCE_PLAN,)


def test_a_targeted_read_marks_only_the_symbol_the_venue_no_longer_returns(cache):
    adapter = loaded(cache, rules("BTCUSDT"), rules("ETHUSDT"), rules("SOLUSDT"))
    del adapter.listing["ETHUSDT"]
    result = cache.refresh(adapter, NOW + 1, symbols=["ETHUSDT", "BTCUSDT"])
    assert result.delisted == 1 and adapter.requests[-1] == ("BTCUSDT", "ETHUSDT")
    assert cache.rules_for_planning(VENUE, ENV, "SOLUSDT", NOW + 1).ok
    adapter.fail = C.AdapterError(C.INSTRUMENT_UNKNOWN, venue=VENUE)
    assert cache.refresh(adapter, NOW + 2, symbols=["SOLUSDT"]).delisted == 1


# --------------------------------------------------------------------------------- outage and backoff
def test_an_exchange_outage_leaves_the_cache_untouched_and_backs_off_within_bounds(cache):
    adapter = loaded(cache)
    adapter.fail = C.AdapterError(C.VENUE_UNAVAILABLE, venue=VENUE)
    first = cache.refresh(adapter, NOW + HOUR)
    assert not first.ok and first.reason == f"{RC.RULES_REFRESH_FAILED}:{C.VENUE_UNAVAILABLE}"
    assert first.called_venue and first.next_attempt_ms == NOW + HOUR + RC.REFRESH_BACKOFF_BASE_MS
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW + HOUR).ok             # still fresh, still usable
    calls = len(adapter.requests)
    waiting = cache.refresh(adapter, NOW + HOUR + 1)
    assert waiting.reason == RC.RULES_REFRESH_BACKOFF and not waiting.called_venue and len(adapter.requests) == calls
    assert not cache.refresh_due(VENUE, ENV, NOW + HOUR + 1)
    now, delays = NOW + HOUR, []
    for _ in range(12):
        now = cache.refresh_state(VENUE, ENV)["next_attempt_ms"]
        assert cache.refresh_due(VENUE, ENV, now)
        delays.append(cache.refresh(adapter, now).next_attempt_ms - now)
    assert delays[:3] == [10_000, 20_000, 40_000] and max(delays) == RC.REFRESH_BACKOFF_MAX_MS
    assert delays == sorted(delays)
    # the rows that could not be refreshed age into a refusal on their own
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW + RC.DEFAULT_MAX_AGE_MS + 1).reasons == (RC.RULES_STALE,)


def test_the_venues_instruction_to_wait_outlasts_the_backoff(cache):
    adapter = StubAdapter(rules())
    adapter.fail = C.AdapterError(C.RATE_LIMITED, venue=VENUE, retry_after_s=120)
    assert cache.refresh(adapter, NOW).next_attempt_ms == NOW + 120_001
    adapter.fail = C.AdapterError(C.BANNED, venue=VENUE, banned_until_ms=NOW + 7 * HOUR)
    assert cache.refresh(adapter, NOW + 120_001).next_attempt_ms == NOW + 7 * HOUR
    assert cache.refresh(adapter, NOW + HOUR, force=True).called_venue           # only an explicit operator action


def test_the_cache_recovers_after_an_outage(cache):
    adapter = loaded(cache)
    adapter.fail = C.AdapterError(C.VENUE_UNAVAILABLE, venue=VENUE)
    later = NOW + RC.DEFAULT_MAX_AGE_MS + HOUR
    cache.refresh(adapter, later)
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", later).reasons == (RC.RULES_STALE,)
    adapter.fail = None
    adapter.listing["BTCUSDT"] = rules(fetched_at_ms=later + 10_000)
    recovered = cache.refresh(adapter, later + 10_000)
    assert recovered.ok and recovered.unchanged == 1
    state = cache.refresh_state(VENUE, ENV)
    assert (state["consecutive_failures"], state["next_attempt_ms"], state["last_error"]) == (0, 0, None)
    assert cache.rules_for_entry(VENUE, ENV, "BTCUSDT", later + 10_000, plan_time_ms=NOW).ok


def test_an_empty_listing_is_a_failure_and_never_a_mass_delisting(cache):
    adapter = loaded(cache, rules("BTCUSDT"), rules("ETHUSDT"))
    adapter.listing.clear()
    result = cache.refresh(adapter, NOW + 1)
    assert not result.ok and result.reason == RC.RULES_REFRESH_EMPTY and result.delisted == 0
    assert cache.rules_for_planning(VENUE, ENV, "ETHUSDT", NOW + 1).ok


def test_a_partial_refresh_updates_what_came_back_and_delists_nothing(cache):
    symbols = [f"AAA{i:02d}USDT" for i in range(30)]
    adapter = loaded(cache, *(rules(s) for s in symbols))
    later = NOW + 20 * HOUR
    adapter.listing = {s: rules(s, fetched_at_ms=later) for s in symbols[:5]}     # 5 of 30: not a real listing
    result = cache.refresh(adapter, later)
    assert not result.ok and result.partial and result.reason == RC.RULES_REFRESH_SUSPECT_PARTIAL
    assert (result.stored, result.delisted) == (5, 0) and result.next_attempt_ms == later + RC.REFRESH_BACKOFF_BASE_MS
    after_expiry = NOW + RC.DEFAULT_MAX_AGE_MS + 1
    assert cache.rules_for_planning(VENUE, ENV, symbols[0], after_expiry).ok       # refreshed: fresh
    assert cache.rules_for_planning(VENUE, ENV, symbols[29], after_expiry).reasons == (RC.RULES_STALE,)
    # a few instruments leaving a large listing is an ordinary delisting
    adapter.listing = {s: rules(s, fetched_at_ms=later + HOUR) for s in symbols[:28]}
    assert cache.refresh(adapter, later + HOUR).delisted == 2


def test_two_workers_do_not_ask_the_venue_for_the_same_listing_at_once(cache, db):
    class Reentrant(StubAdapter):
        def instrument_rules(self, symbols=None):
            other = RC.RulesCache(db, worker="w2").refresh(StubAdapter(rules()), NOW)
            assert other.reason == RC.RULES_REFRESH_IN_PROGRESS and not other.called_venue
            return super().instrument_rules(symbols)

    assert cache.refresh(Reentrant(rules()), NOW).ok
    assert RC.RulesCache(db, worker="w2").refresh(StubAdapter(rules(fetched_at_ms=NOW + 1)), NOW + 1).ok


# ------------------------------------------------------------------------------------------- conflicts
@pytest.mark.parametrize("bad,problem", [
    (dict(min_quantity=5.0, max_quantity=1.0, market_max_quantity=None), "MIN_QUANTITY_ABOVE_MAX_QUANTITY"),
    (dict(min_quantity=200.0), "MIN_QUANTITY_ABOVE_MARKET_MAX_QUANTITY"),
    (dict(quantity_step=5000.0), "QUANTITY_STEP_ABOVE_MAX_QUANTITY"),
    (dict(environment=C.LIVE), "ENVIRONMENT_MISMATCH"),
    (dict(venue=C.BYBIT), "VENUE_MISMATCH"),
    (dict(fetched_at_ms=NOW + 10 * HOUR), "FETCHED_IN_THE_FUTURE"),
])
def test_conflicting_metadata_is_not_stored_and_refuses_until_a_clean_answer(cache, bad, problem):
    adapter = loaded(cache)
    adapter.listing["BTCUSDT"] = rules(**{"fetched_at_ms": NOW + HOUR, **bad})
    result = cache.refresh(adapter, NOW + HOUR)
    assert result.ok and (result.conflicts, result.stored, result.delisted) == (1, 0, 0)
    decision = cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW + HOUR)
    assert not decision.ok and decision.reasons == (RC.RULES_CONFLICT,) and decision.detail["conflict_reason"] == problem
    # the consistent rules of before are still the ones a close is quantised with
    assert cache.rules_for_reduce(VENUE, ENV, "BTCUSDT", NOW + HOUR).rules.quantity_step == 0.001
    cache.refresh(adapter, NOW + HOUR + 1, force=True)                               # the same contradiction again
    adapter.listing["BTCUSDT"] = rules(fetched_at_ms=NOW + 2 * HOUR)
    assert cache.refresh(adapter, NOW + 2 * HOUR).ok
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW + 2 * HOUR).ok
    assert [h["change_type"] for h in cache.history(VENUE, ENV, "BTCUSDT")] == [
        RC.FIRST_SEEN, RC.CONFLICT, RC.CONFLICT_CLEARED]
    assert cache.history(VENUE, ENV, "BTCUSDT")[1]["document"]["problem"] == problem


def test_a_record_returned_under_another_symbols_key_is_a_conflict(cache):
    adapter = StubAdapter()
    adapter.listing = {"BTCUSDT": rules("ETHUSDT"), "SOLUSDT": rules("SOLUSDT"), "DOGEUSDT": "not rules"}
    result = cache.refresh(adapter, NOW)
    assert (result.conflicts, result.stored) == (2, 1)
    assert cache.rules_for_planning(VENUE, ENV, "BTCUSDT", NOW).reasons == (RC.RULES_UNKNOWN_INSTRUMENT,)
    assert cache.rules_for_planning(VENUE, ENV, "ETHUSDT", NOW).reasons == (RC.RULES_UNKNOWN_INSTRUMENT,)
    assert cache.history(VENUE, ENV, "BTCUSDT")[0]["document"]["problem"] == "KEY_SYMBOL_MISMATCH"


# -------------------------------------------------------------------------------------- start-up, restart
def test_start_up_loads_the_required_rules_once_and_leaves_an_unknown_symbol_refused(cache):
    adapter = StubAdapter(rules("BTCUSDT"), rules("ETHUSDT"))
    decisions = cache.ensure_loaded(adapter, ["BTCUSDT", "ETHUSDT", "XRPUSDT"], NOW)
    assert decisions["BTCUSDT"].ok and decisions["ETHUSDT"].ok
    assert decisions["XRPUSDT"].reasons == (RC.RULES_UNKNOWN_INSTRUMENT,)
    assert len(adapter.requests) == 1
    cache.ensure_loaded(adapter, ["BTCUSDT"], NOW + 1)                  # usable and not due: the venue is not asked
    assert len(adapter.requests) == 1
    assert cache.refresh_due(VENUE, ENV, NOW + RC.DEFAULT_REFRESH_INTERVAL_MS)
    assert not cache.refresh_due(VENUE, ENV, NOW + RC.DEFAULT_REFRESH_INTERVAL_MS - 1)
    assert RC.DEFAULT_REFRESH_INTERVAL_MS < RC.DEFAULT_MAX_AGE_MS <= 24 * HOUR      # refreshed at least daily


def test_the_cache_survives_a_restart(tmp_path):
    path = str(tmp_path / "restart.db")
    database = DB(path)
    ensure_cati_platform_schema(database)
    record = rules(max_leverage=None)
    RC.RulesCache(database).refresh(StubAdapter(record), NOW)
    again = RC.RulesCache(DB(path)).rules_for_planning(VENUE, ENV, "BTCUSDT", NOW + HOUR)
    assert again.ok and again.rules == record and again.rules.max_leverage is None


# ------------------------------------------------------------------------------------------ quantisation
def test_quantities_only_round_down_and_prices_only_toward_less_risk():
    r = rules()
    assert RC.quantize_quantity(r, 0.0159) == 0.015 and RC.quantize_quantity(r, 0.015) == 0.015
    assert RC.quantize_quantity(r, 0.0009) == 0.0
    assert RC.quantize_quantity(r, 0.3) == 0.3 and RC.quantize_quantity(rules(quantity_step=0.1), 0.7) == 0.7
    # a LONG's stop moves up toward the entry, a SHORT's down: the allowed loss never grows by rounding
    assert RC.quantize_stop_price(r, C.LONG, 59_999.93) == 60_000.0
    assert RC.quantize_stop_price(r, C.SHORT, 60_000.07) == 60_000.0
    assert RC.quantize_stop_price(r, C.LONG, 60_000.0) == 60_000.0
    # a BUY limit never pays more, a SELL limit never takes less
    assert RC.quantize_limit_price(r, C.BUY, 60_000.07) == 60_000.0
    assert RC.quantize_limit_price(r, C.SELL, 60_000.01) == 60_000.1
    for call in (lambda: RC.quantize_quantity(r, -1), lambda: RC.quantize_stop_price(r, C.FLAT, 1.0),
                 lambda: RC.quantize_limit_price(r, "LONG", 1.0), lambda: RC.quantize_quantity(r, float("nan"))):
        with pytest.raises(ValueError):
            call()


def test_quantisers_refuse_when_the_venue_did_not_report_the_rule():
    with pytest.raises(RC.RulesRefused) as no_step:
        RC.quantize_quantity(rules(quantity_step=None), 1.0)
    assert no_step.value.reason == RC.RULES_INCOMPLETE and no_step.value.detail == {"missing": "quantity_step"}
    with pytest.raises(RC.RulesRefused):
        RC.quantize_stop_price(rules(price_tick=None), C.LONG, 100.0)
    assert RC.executable_quantity(rules(min_notional=None), 1.0, 100.0).reasons == (RC.RULES_INCOMPLETE,)
    with pytest.raises(RC.RulesRefused):
        RC.minimum_order_quantity(rules(min_quantity=None), 100.0)
    assert RC.leverage_allowed(rules(max_leverage=None), 1) == (False, RC.RULES_LEVERAGE_LIMIT_UNKNOWN)
    assert RC.leverage_allowed(rules(), 126) == (False, RC.LEVERAGE_ABOVE_VENUE_MAXIMUM)
    assert RC.leverage_allowed(rules(), 125) == (True, "")


def test_an_order_outside_the_limits_is_refused_never_moved_into_them():
    r = rules()
    assert RC.executable_quantity(r, 0.0159, 60_000.0) == RC.QuantityDecision(True, 0.015, ())
    assert RC.executable_quantity(r, 0.0009, 60_000.0).reasons == (RC.QUANTITY_BELOW_MINIMUM, RC.MIN_NOTIONAL_NOT_MET)
    assert RC.executable_quantity(r, 0.001, 60_000.0).reasons == (RC.MIN_NOTIONAL_NOT_MET,)
    too_much = RC.executable_quantity(r, 1000.5, 60_000.0)
    assert not too_much.ok and too_much.reasons == (RC.QUANTITY_ABOVE_MAXIMUM,) and too_much.quantity == 1000.5
    assert RC.executable_quantity(r, 500.0, 60_000.0).ok
    assert RC.executable_quantity(r, 500.0, 60_000.0, market=True).reasons == (RC.QUANTITY_ABOVE_MAXIMUM,)
    assert RC.executable_quantity(r, 0.0, 60_000.0).reasons == (RC.EXECUTABLE_QUANTITY_UNKNOWN,)
    assert RC.executable_quantity(r, 1.0, float("inf")).reasons == (RC.EXECUTABLE_QUANTITY_UNKNOWN,)


def test_the_venue_minimum_is_the_first_step_that_meets_both_floors():
    r = rules()
    assert RC.minimum_order_quantity(r, 60_000.0) == 0.002          # 100 / 60000 = 0.00167 -> next step up
    assert RC.minimum_order_quantity(r, 1_000_000.0) == 0.001       # the minimum quantity is the larger floor
    assert RC.minimum_order_quantity(replace(r, min_notional=5.0), 0.5) == 10.0
    with pytest.raises(RC.RulesRefused):
        RC.minimum_order_quantity(replace(r, max_quantity=1.0, market_max_quantity=None), 50.0)
