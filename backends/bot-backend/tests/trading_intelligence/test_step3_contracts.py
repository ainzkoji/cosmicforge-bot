"""Step 3.5: the family-neutral trading contracts. An object of these types that exists is valid."""
from __future__ import annotations

import dataclasses
import math

import pytest

from _exec import Harness
from app.trading_intelligence.pipeline import contracts as C

NOW = 1_800_000_000_000
PROVENANCE = C.DecisionProvenance("FAMILY", "v1", "MANDATE", "H-001", "rulehash", "dec-1", "dechash", NOW, "snapshot", "1d", C.SHADOW)
ACCOUNT = C.AccountRef("user-1", "account-1", "binance", "DEMO")
LIMITS = C.PortfolioConstraints(max_positions=6, max_open_risk=0.02, max_leverage=2.0, per_trade_risk_ceiling=0.004,
                                max_entry_stop_distance=0.15, equity_basis=10_000.0)
UNIVERSE = frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT"})


def entry(**over):
    base = dict(symbol="BTCUSDT", direction=C.LONG, quantity=0.01, reference_price=60_000.0, stop_price=54_000.0,
                risk_fraction=0.004, leverage=2.0)
    base.update(over)
    return C.EntryIntent(**base)


def batch(*intents, **over):
    base = dict(provenance=PROVENANCE, account=ACCOUNT, intents=tuple(intents), constraints=LIMITS, instruments=UNIVERSE,
                created_at_ms=NOW + 1_000, expires_at_ms=NOW + 86_400_000)
    base.update(over)
    return C.IntentBatch(**base)


def violation(code, build):
    with pytest.raises(C.ContractViolation) as caught:
        build()
    assert caught.value.code == code, caught.value
    return caught.value


# -------------------------------------------------------------------------------------------- valid shapes
def test_long_and_short_entries_with_and_without_a_target():
    long_ = entry()
    assert (long_.kind, long_.take_profit_price) == (C.ENTRY, None)             # no target is a valid entry
    assert long_.stop_distance == pytest.approx(0.1) and long_.notional == pytest.approx(600.0)
    short = entry(direction=C.SHORT, stop_price=66_000.0, take_profit_price=50_000.0)
    assert short.stop_distance == pytest.approx(0.1)
    limit = entry(order_style=C.LIMIT_THEN_MARKET, limit_price=59_900.0, max_slippage_bps=15.0)
    assert limit.order_style == C.LIMIT_THEN_MARKET


def test_every_intent_type_is_tagged_and_survives_storage():
    intents = (
        entry(),
        C.AddIntent("ETHUSDT", C.LONG, 0.5, 3_000.0, 2_700.0),
        C.ReduceIntent("SOLUSDT", C.SHORT, "PARTIAL_PROFIT", fraction=0.5),
        C.ReduceIntent("SOLUSDT", C.LONG, "RISK", quantity=2.0),
        C.ExitIntent("ETHUSDT", C.LONG, "SIGNAL"),
        C.RebalanceIntent("BTCUSDT", C.LONG, current_quantity=0.02, target_quantity=0.015, reference_price=60_000.0,
                          stop_price=55_000.0, band=0.25),
        C.StopReplaceIntent("BTCUSDT", C.LONG, C.TRAIL, new_stop_price=56_000.0, current_stop_price=54_000.0),
        C.StopReplaceIntent("BTCUSDT", C.SHORT, C.BREAK_EVEN, new_stop_price=59_990.0, current_stop_price=63_000.0,
                            entry_price=60_000.0),
        C.StopReplaceIntent("BTCUSDT", C.LONG, C.RESIZE, new_stop_price=54_000.0, current_stop_price=54_000.0, quantity=0.03),
    )
    assert [i.kind for i in intents] == [C.ENTRY, C.ADD, C.REDUCE, C.REDUCE, C.EXIT, C.REBALANCE, C.STOP_REPLACE,
                                         C.STOP_REPLACE, C.STOP_REPLACE]
    for intent in intents:
        assert C.intent_from_plain(C.to_plain(intent)) == intent
        with pytest.raises(dataclasses.FrozenInstanceError):
            intent.symbol = "X"
    assert [C.increases_risk(i) for i in intents] == [True, True, False, False, False, False, False, False, False]
    assert C.increases_risk(C.RebalanceIntent("BTCUSDT", C.LONG, 0.01, 0.02, 60_000.0, 55_000.0, 0.25))


def test_a_batch_of_simultaneous_intents_and_its_identity():
    b = batch(entry(), C.ExitIntent("ETHUSDT", C.LONG, "SIGNAL"),
              C.StopReplaceIntent("SOLUSDT", C.LONG, C.TIGHTEN, 150.0, 140.0))
    assert len(b.intents) == 3 and len(b.risk_increasing) == 1
    assert b.batch_hash == batch(entry(), C.ExitIntent("ETHUSDT", C.LONG, "SIGNAL"),
                                 C.StopReplaceIntent("SOLUSDT", C.LONG, C.TIGHTEN, 150.0, 140.0)).batch_hash
    assert b.batch_hash != batch(entry(quantity=0.011), C.ExitIntent("ETHUSDT", C.LONG, "SIGNAL")).batch_hash
    assert not b.stale(NOW + 5_000) and b.stale(NOW + 86_400_000) and not b.future_dated(NOW)
    assert b.future_dated(NOW - C.FUTURE_TOLERANCE_MS - 1)
    for frame in ("1m", "15m", "1h", "1d"):           # daily, hourly and shorter decisions
        p = dataclasses.replace(PROVENANCE, timeframe=frame)
        assert batch(entry(), provenance=p).provenance.timeframe == frame


# ------------------------------------------------------------------------------------------------ refusals
@pytest.mark.parametrize("code,build", [
    ("INVALID_QUANTITY", lambda: entry(quantity=-1.0)),
    ("INVALID_QUANTITY", lambda: entry(quantity=0.0)),
    ("INVALID_QUANTITY", lambda: entry(quantity=math.nan)),
    ("INVALID_QUANTITY", lambda: entry(quantity=math.inf)),
    ("INVALID_QUANTITY", lambda: entry(quantity="1")),
    ("INVALID_QUANTITY", lambda: entry(quantity=True)),
    ("INVALID_PRICE", lambda: entry(reference_price=0.0)),
    ("INVALID_STOP_GEOMETRY", lambda: entry(stop_price=60_000.0)),               # at the price: not protection
    ("INVALID_STOP_GEOMETRY", lambda: entry(stop_price=61_000.0)),               # a long protected from above
    ("INVALID_STOP_GEOMETRY", lambda: entry(direction=C.SHORT, stop_price=59_000.0)),
    ("INVALID_STOP_GEOMETRY", lambda: entry(stop_price=math.nan)),
    ("MISSING_RISK_INFORMATION", lambda: entry(stop_price=None)),
    ("MISSING_RISK_INFORMATION", lambda: entry(risk_fraction=None)),
    ("MISSING_RISK_INFORMATION", lambda: entry(risk_fraction=0.0)),
    ("MISSING_RISK_INFORMATION", lambda: entry(risk_fraction=1.5)),
    ("UNAUTHORIZED_LEVERAGE", lambda: entry(leverage=0.5)),
    ("UNAUTHORIZED_LEVERAGE", lambda: entry(leverage=math.inf)),
    ("INVALID_TARGET_GEOMETRY", lambda: entry(take_profit_price=59_000.0)),
    ("INVALID_DIRECTION", lambda: entry(direction="BUY")),
    ("UNSUPPORTED_INSTRUMENT", lambda: entry(symbol="btc-usdt")),
    ("UNSUPPORTED_INSTRUMENT", lambda: entry(symbol="")),
    ("INVALID_ORDER_STYLE", lambda: entry(order_style="ICEBERG")),
    ("CONTRADICTORY_ACTION", lambda: entry(limit_price=59_000.0)),               # a market order with a limit price
    ("INVALID_PRICE", lambda: entry(order_style=C.LIMIT)),
    ("MISSING_RISK_INFORMATION", lambda: entry(order_style=C.LIMIT_THEN_MARKET, limit_price=59_000.0)),
    ("MISSING_RISK_INFORMATION", lambda: C.AddIntent("BTCUSDT", C.LONG, 1.0, 100.0, None)),
    ("CONTRADICTORY_ACTION", lambda: C.ReduceIntent("BTCUSDT", C.LONG, "R")),
    ("CONTRADICTORY_ACTION", lambda: C.ReduceIntent("BTCUSDT", C.LONG, "R", quantity=1.0, fraction=0.5)),
    ("INVALID_QUANTITY", lambda: C.ReduceIntent("BTCUSDT", C.LONG, "R", fraction=1.0)),   # that is an exit
    ("MISSING_REASON", lambda: C.ExitIntent("BTCUSDT", C.LONG, "")),
    ("CONTRADICTORY_ACTION", lambda: C.RebalanceIntent("BTCUSDT", C.LONG, 1.0, 1.0, 100.0, 90.0, 0.25)),
    ("INVALID_QUANTITY", lambda: C.RebalanceIntent("BTCUSDT", C.LONG, 1.0, 0.0, 100.0, 90.0, 0.25)),
    ("STOP_WIDENING_REFUSED", lambda: C.StopReplaceIntent("BTCUSDT", C.LONG, C.TRAIL, 53_000.0, 54_000.0)),
    ("STOP_WIDENING_REFUSED", lambda: C.StopReplaceIntent("BTCUSDT", C.SHORT, C.TIGHTEN, 67_000.0, 66_000.0)),
    ("CONTRADICTORY_ACTION", lambda: C.StopReplaceIntent("BTCUSDT", C.LONG, C.TRAIL, 54_000.0, 54_000.0)),
    ("MISSING_RISK_INFORMATION", lambda: C.StopReplaceIntent("BTCUSDT", C.LONG, C.BREAK_EVEN, 60_000.0, 54_000.0)),
    ("INVALID_STOP_GEOMETRY", lambda: C.StopReplaceIntent("BTCUSDT", C.LONG, C.BREAK_EVEN, 59_000.0, 54_000.0, entry_price=60_000.0)),
    ("INVALID_STOP_REASON", lambda: C.StopReplaceIntent("BTCUSDT", C.LONG, "WIDEN", 55_000.0, 54_000.0)),
])
def test_an_invalid_intent_cannot_be_constructed(code, build):
    violation(code, build)


@pytest.mark.parametrize("code,build", [
    ("UNSUPPORTED_INSTRUMENT", lambda: batch(entry(symbol="DOGEUSDT"))),
    ("UNAUTHORIZED_LEVERAGE", lambda: batch(entry(leverage=3.0))),
    ("RISK_ABOVE_CEILING", lambda: batch(entry(risk_fraction=0.0041))),
    ("ENTRY_STOP_TOO_WIDE", lambda: batch(entry(stop_price=50_000.0))),          # 16.7 %: refused, never clipped
    ("CONTRADICTORY_ACTION", lambda: batch(entry(), C.ExitIntent("BTCUSDT", C.LONG, "SIGNAL"))),
    ("CONTRADICTORY_ACTION", lambda: batch(entry(), entry())),
    ("POSITION_LIMIT", lambda: batch(entry(), entry(symbol="ETHUSDT", reference_price=3_000.0, stop_price=2_700.0),
                                     constraints=dataclasses.replace(LIMITS, max_positions=1))),
    ("INCONSISTENT_TIMEFRAME", lambda: batch(entry(), intent_timeframe="1h")),
    ("INCONSISTENT_TIMEFRAME", lambda: dataclasses.replace(PROVENANCE, timeframe="3d")),
    ("STALE_DECISION", lambda: batch(entry(), created_at_ms=NOW + 86_400_000)),  # built after it expired
    ("STALE_DECISION", lambda: batch(entry(), expires_at_ms=NOW)),
    ("FUTURE_DATED_DECISION", lambda: batch(entry(), created_at_ms=NOW - 60_000)),
    ("INVALID_ACCOUNT_OWNERSHIP", lambda: C.AccountRef("", "account-1", "binance", "DEMO")),
    ("INVALID_ACCOUNT_OWNERSHIP", lambda: C.AccountRef("user-1", " ", "binance", "DEMO")),
    ("UNAUTHORIZED_ENVIRONMENT", lambda: C.AccountRef("user-1", "account-1", "binance", "TESTNET")),
    ("MISSING_PROVENANCE", lambda: dataclasses.replace(PROVENANCE, rule_hash="")),
    ("MISSING_PROVENANCE", lambda: dataclasses.replace(PROVENANCE, decision_time_ms=0)),
    ("INVALID_MODE", lambda: dataclasses.replace(PROVENANCE, mode="PAPER")),
    ("INVALID_CONSTRAINTS", lambda: dataclasses.replace(LIMITS, max_positions=0)),
    ("INVALID_CONSTRAINTS", lambda: dataclasses.replace(LIMITS, equity_basis=-1.0)),
    ("INVALID_BATCH", lambda: batch("not an intent")),
    ("INVALID_BATCH", lambda: C.intent_from_plain({"kind": "FLIP", "symbol": "BTCUSDT"})),
])
def test_an_invalid_batch_or_provenance_cannot_be_constructed(code, build):
    violation(code, build)


def test_a_stored_intent_that_was_made_invalid_is_refused_on_reload():
    plain = C.to_plain(entry())
    plain["stop_price"] = 61_000.0
    violation("INVALID_STOP_GEOMETRY", lambda: C.intent_from_plain(plain))


def test_only_demo_and_live_are_executing_modes():
    assert C.EXECUTING_MODES == (C.DEMO, C.LIVE)
    assert {C.RESEARCH_SIMULATION, C.SHADOW, C.OPERATOR_CERTIFICATION}.isdisjoint(C.EXECUTING_MODES)
    assert C.OPERATOR_CERTIFICATION == "DEMO_CERTIFICATION"     # the Step 1 harness label, never a strategy mode


# ------------------------------------------------------------------- compatibility with the existing tail
def test_one_provenance_view_serves_a_batch_and_an_existing_trade_plan(tmp_path):
    from_batch = C.provenance_of(batch(entry()))
    assert from_batch["family_id"] == "FAMILY" and from_batch["broker_account_id"] == "account-1"
    assert from_batch["rule_hash"] == "rulehash" and from_batch["expires_at_ms"] == NOW + 86_400_000
    plan = Harness(tmp_path).plan                      # a real Section 18 TradePlan of the existing tail
    from_plan = C.provenance_of(plan)
    assert from_plan["family_id"] == plan.setup_family and from_plan["decision_id"] == plan.source_candidate_id
    assert from_plan["broker_account_id"] == plan.broker_account_id and from_plan["mode"] == plan.mode
    assert "rule_hash" not in from_plan and "decision_hash" not in from_plan    # absent stays absent: the gate refuses
    carried = dataclasses.replace(plan, versions=plan.versions + tuple(
        (k, f"value-of-{k}") for k in C.PLAN_PROVENANCE_KEYS))
    view = C.provenance_of(carried)
    assert view["rule_hash"] == "value-of-family_rule_hash" and view["decision_hash"] == "value-of-decision_hash"
    # the existing plan's geometry is expressible as an entry intent, target included
    as_intent = C.EntryIntent(symbol=plan.instrument_key.venue_symbol, direction=plan.side, quantity=1.0,
                              reference_price=plan.entry_reference, stop_price=plan.structural_invalidation_price,
                              risk_fraction=0.004, take_profit_price=plan.target_zones[0].price_high if plan.side == C.LONG
                              else plan.target_zones[0].price_low)
    assert as_intent.stop_distance == pytest.approx(plan.initial_risk_distance / plan.entry_reference)
