"""Section K1 -- the portfolio-path sizing deviations D2, D3, D8, the strength range and the decision-bar / mark
price refusals (``docs/implementation/MASTER_PLAN_I_L_COMPLIANCE.md`` section 6).

Every fix here only refuses more or sizes smaller; no ceiling changes. Pure where it can be: the portfolio gate
(``pipeline.portfolio.authorize``) and the family's connected-account stage (``pipeline.daily_trend.
DailyTrendPipeline.broker_account``) are called directly.
"""
from __future__ import annotations

import dataclasses
import math
from decimal import Decimal
from types import SimpleNamespace

import numpy as np
import pytest

from app.exchange.platform_adapter.contract import AccountBalance, InstrumentRules, PositionInfo
from app.risk.system_limits import SystemLimits
from app.trading_intelligence.execution import risk_sizing
from app.trading_intelligence.execution.leverage import LIQUIDATION_DEDUCTION, resolve_cati_leverage
from app.trading_intelligence.families.daily_trend import spec as frozen_spec
from app.trading_intelligence.families.daily_trend import targets as frozen
from app.trading_intelligence.integration import portfolio_runtime as R
from app.trading_intelligence.integration import reason_registry as registry
from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import portfolio as P
from app.trading_intelligence.pipeline.brakes import NO_BRAKE
from app.trading_intelligence.pipeline.contracts import (
    LONG, AddIntent, EntryIntent, ExitIntent, RebalanceIntent, StopReplaceIntent,
)
from shared_lib.risk_levels import LEVELS, SYSTEM_PER_TRADE_RISK_CEILING_PCT

EQUITY, PRICE = 10_000.0, 100.0
A, B = "AAAUSDT", "BBBUSDT"


def rule(symbol: str) -> InstrumentRules:
    return InstrumentRules(venue="binance", environment="DEMO", symbol=symbol, venue_symbol=symbol, status="TRADING",
                           venue_status="TRADING", base_asset=symbol[:-4], quote_asset="USDT", margin_asset="USDT",
                           price_tick=0.01, quantity_step=0.001, min_quantity=0.001, max_quantity=1_000_000.0,
                           min_notional=5.0, max_leverage=125.0, contract_size=1.0, fetched_at_ms=1)


RULES = {s: rule(s) for s in (A, B)}


def snapshot(*positions, equity=EQUITY, available=None):
    return P.AccountSnapshot(account_id="acct-k1", equity=equity, wallet_balance=equity,
                             available_margin=equity if available is None else available, positions=tuple(positions),
                             reservations=(), as_of_ms=1_000)


def held(symbol=A, quantity=1.0, *, stop=90.0, mark=PRICE, leverage=None, margin_mode=None, position_mode=None):
    return P.PortfolioPosition(symbol=symbol, direction=LONG, quantity=quantity, entry_price=mark, mark_price=mark,
                               stop_price=stop, family_id="daily_trend", leverage=leverage, margin_mode=margin_mode,
                               position_mode=position_mode)


def entry(symbol=A, *, risk=0.004, distance=0.10, leverage=1.0, equity=EQUITY):
    return EntryIntent(symbol=symbol, direction=LONG, quantity=risk * equity / distance / PRICE, reference_price=PRICE,
                       stop_price=PRICE * (1.0 - distance), risk_fraction=risk, leverage=leverage)


def limits(level="balanced", **changes):
    return dataclasses.replace(P.AccountLimits.from_profile(level), **changes)


def decide(intents, *, snap=None, lim=None):
    return P.authorize(snap or snapshot(), lim or limits(), intents, RULES, brake_state=NO_BRAKE)


# ======================================================================== D8: one 15 % and one 0.40 % everywhere
def test_the_engine_ceilings_have_one_value_in_every_place_they_are_stated():
    """D8. The widest entry stop and the per-trade ceiling are stated by the frozen family specification, the
    system limits, the risk sizer and the portfolio limits. They must agree; the specification is not edited."""
    scenario = frozen_spec.INTERPRETATION["executable_policy_scenario"]
    stop_values = {
        "families/daily_trend/spec.py": scenario["max_entry_stop_distance"],
        "SystemLimits.max_stop_loss_pct": SystemLimits().max_stop_loss_pct,
        "risk_sizing.MAX_STOP_DISTANCE": float(risk_sizing.MAX_STOP_DISTANCE),
        **{f"AccountLimits({lvl})": P.AccountLimits.from_profile(lvl).max_entry_stop_distance for lvl in LEVELS},
        **{f"targets.risk_level({lvl})": frozen.risk_level(lvl, frozen.EXECUTABLE).max_entry_stop_distance
           for lvl in frozen_spec.RISK_LEVELS},
    }
    assert set(stop_values.values()) == {0.15}, stop_values
    risk_values = {
        "families/daily_trend/spec.py": scenario["risk_per_trade_ceiling"],
        "SystemLimits.max_risk_per_trade_ceiling": SystemLimits().max_risk_per_trade_ceiling,
        "SYSTEM_PER_TRADE_RISK_CEILING_PCT": float(SYSTEM_PER_TRADE_RISK_CEILING_PCT / Decimal(100)),
    }
    assert set(risk_values.values()) == {0.004}, risk_values
    for lvl in LEVELS:
        assert P.AccountLimits.from_profile(lvl).per_trade_risk <= 0.004


# ====================================================================== D2: liquidation distance and margin mode
def test_the_liquidation_ceiling_is_the_executor_invariant_and_holds_at_every_profile_and_the_widest_stop():
    """At 15 % the ceiling is 1 / (0.30 + 0.0061) = 3.27, above every profile's leverage: the gate cannot refuse
    a frozen-rule entry, it only refuses a position whose venue leverage is above the account's."""
    assert P.liquidation_ceiling(0.15) == pytest.approx(1.0 / (0.30 + LIQUIDATION_DEDUCTION))
    assert P.liquidation_ceiling(0.15) == pytest.approx(resolve_cati_leverage(
        stop_fraction=0.15, user_max=100, asset_class_max=100, compound_risk_limit=100).ceilings["LIQUIDATION_SAFETY"])
    for lvl in LEVELS:
        lim = P.AccountLimits.from_profile(lvl)
        assert lim.max_leverage <= P.liquidation_ceiling(lim.max_entry_stop_distance)
        out = decide([entry(distance=0.15, leverage=lim.max_leverage, risk=lim.per_trade_risk)], lim=lim)
        assert out.refused == () and len(out.accepted) == 1, lvl


@pytest.mark.parametrize("margin_mode,leverage,refused", [
    ("ISOLATED", 20.0, True),       # its own margin carries it: 1/20 = 5 % < 2 x 10 % + 0.61 %
    ("ISOLATED", 3.0, False),
    (None, 20.0, True),             # not reported: the venue leverage is assumed to decide
    (None, None, False),            # neither reported: the account's maximum
    ("CROSSED", 20.0, False),       # the account carries it; gross leverage is bounded by the leverage budget
])
def test_an_increase_whose_liquidation_lies_inside_twice_the_stop_is_refused(margin_mode, leverage, refused):
    position = held(quantity=1.0, stop=90.0, leverage=leverage, margin_mode=margin_mode)
    add = AddIntent(symbol=A, direction=LONG, quantity=0.5, reference_price=PRICE, stop_price=90.0)
    out = decide([add], snap=snapshot(position))
    if refused:
        (refusal,) = out.refused
        assert refusal.reason == P.LIQUIDATION_TOO_CLOSE and out.accepted == ()
        assert "twice the stop" in refusal.detail
    else:
        assert out.refused == () and out.accepted[0].quantity > 0


def test_the_liquidation_gate_never_withholds_a_reduction_or_an_exit():
    position = held(quantity=1.0, stop=90.0, leverage=50.0, margin_mode="ISOLATED")
    out = decide([ExitIntent(symbol=A, direction=LONG, reason="SIGNAL", reference_price=PRICE)], snap=snapshot(position))
    assert out.refused == () and out.accepted[0].quantity == 1.0


def test_an_entry_on_an_account_whose_maximum_leverage_cannot_survive_the_stop_is_refused():
    """An entry has no position, so its margin mode is not known: the larger of its leverage and the account's."""
    lim = limits("aggressive", max_leverage=8.0)
    out = decide([entry(distance=0.10, leverage=1.0)], lim=lim)       # 1 / (0.20 + 0.0061) = 4.85 < 8
    assert [r.reason for r in out.refused] == [P.LIQUIDATION_TOO_CLOSE]


def test_a_hedge_mode_position_is_not_increased():
    position = held(position_mode="HEDGE", margin_mode="CROSSED", leverage=2.0)
    out = decide([AddIntent(symbol=A, direction=LONG, quantity=0.1, reference_price=PRICE, stop_price=90.0)],
                 snap=snapshot(position))
    assert [r.reason for r in out.refused] == [P.POSITION_MODE_UNSUPPORTED]


def test_the_venue_margin_and_position_mode_reach_the_snapshot():
    info = PositionInfo(venue="binance", symbol=A, side=LONG, quantity=1.0, entry_price=PRICE, mark_price=PRICE,
                        unrealized_pnl=0.0, leverage=20.0, margin_mode="ISOLATED", position_mode="ONE_WAY",
                        observed_at_ms=1)
    balance = AccountBalance(venue="binance", margin_asset="USDT", wallet_balance=EQUITY, unrealized_pnl=0.0,
                             equity=EQUITY, available_balance=EQUITY, observed_at_ms=1)
    snap = P.AccountSnapshot.from_venue("acct-k1", balance, [info], stops={A: 90.0}, as_of_ms=1_000)
    (position,) = snap.positions
    assert (position.margin_mode, position.position_mode, position.leverage) == ("ISOLATED", "ONE_WAY", 20.0)


# ============================================================================ D3: caps on min(budget, equity)
def test_the_per_trade_clamp_is_measured_on_the_budget_when_it_is_below_the_equity():
    wanted = entry(risk=0.004, distance=0.10)                         # 40 USDT at the stop on 10,000 of equity
    unbounded = decide([wanted]).accepted[0]
    bounded = decide([wanted], lim=limits(capital_budget=2_000.0)).accepted[0]
    assert unbounded.quantity == pytest.approx(4.0)
    assert bounded.quantity == pytest.approx(0.8)                     # 0.40 % of 2,000 at a 10 USDT distance
    assert P.RISK_ABOVE_CEILING in bounded.limited_by
    assert bounded.risk_amount <= 0.004 * 2_000.0 * (1 + 1e-9)


def test_exposure_outside_the_bot_uses_up_the_budget_headroom_not_the_equity_headroom():
    """A manual position worth 3,990 USDT: with the equity as the basis (2 x 10,000) it leaves 16,010 of gross
    room; with a 2,000 budget (2 x 2,000) it leaves 10, and the entry is scaled down to it."""
    manual = held(B, quantity=39.9, stop=99.0)                        # 3,990 notional, 39.9 USDT at its stop
    snap, wanted = snapshot(manual), entry(risk=0.002, distance=0.10, equity=2_000.0)   # 0.4 units, 40 notional
    assert decide([wanted], snap=snap).accepted[0].quantity == pytest.approx(wanted.quantity)
    lim = limits(capital_budget=2_000.0, max_open_risk=1.0)          # isolate the leverage budget
    out = decide([wanted], snap=snap, lim=lim)
    (accepted,) = out.accepted
    assert out.scales.leverage == pytest.approx(10.0 / 40.0)
    assert accepted.quantity == pytest.approx(0.1) and P.LEVERAGE_CAP in accepted.limited_by
    assert manual.notional + accepted.notional <= 2.0 * 2_000.0 * (1 + 1e-9)


def test_open_risk_outside_the_bot_counts_against_the_budget():
    unprotected = held(B, quantity=0.5, stop=None)                    # 50 USDT counted whole: no stop is known
    lim = limits(capital_budget=2_000.0)                              # balanced: 2 % of 2,000 = 40 of open risk
    out = decide([entry(risk=0.002, distance=0.10, equity=2_000.0)], snap=snapshot(unprotected), lim=lim)
    assert [r.reason for r in out.refused] == [P.OPEN_RISK_CAP] and out.accepted == ()
    assert decide([entry(risk=0.002, distance=0.10, equity=2_000.0)], snap=snapshot(unprotected)).refused == ()


def test_a_budget_at_or_above_the_equity_changes_nothing():
    intents = [entry(A), entry(B, distance=0.05)]
    plain = decide(intents)
    for budget in (EQUITY, 5 * EQUITY):
        same = decide(intents, lim=limits(capital_budget=budget))
        assert [(a.symbol, a.quantity, a.limited_by) for a in same.accepted] == \
            [(a.symbol, a.quantity, a.limited_by) for a in plain.accepted]
        assert same.scales == plain.scales


@pytest.mark.parametrize("budget", [0.0, -1.0, float("nan"), float("inf")])
def test_an_unusable_budget_is_refused_by_the_limits(budget):
    with pytest.raises(P.PortfolioDataError, match="INVALID_LIMITS"):
        limits(capital_budget=budget)


def test_the_runtime_puts_the_resolved_budget_into_the_account_limits():
    boundary = SimpleNamespace(capital_budget_spec=("percent_balance", 20.0), capital_budget=None,
                               effective_policy=SimpleNamespace(max_leverage=2.0), limits=limits())
    assert R.resolve_capital_budget(boundary, 5_000.0) == pytest.approx(1_000.0)
    assert boundary.limits.capital_budget == pytest.approx(1_000.0)
    assert boundary.limits.capital_basis(5_000.0) == pytest.approx(1_000.0)
    assert boundary.limits.capital_basis(800.0) == pytest.approx(800.0)        # never more than the equity


# ================================================================== D4 (portfolio side): a ratchet on a rebalance
def test_an_increase_carrying_a_tighter_stop_is_sized_at_the_wider_resting_stop():
    """The order fills under the stop that rests; the tighter one only rests after a replacement that may not
    happen. Sizing at the stated (tighter) stop would let the position carry more than the ceiling meanwhile."""
    position = held(quantity=2.0, stop=85.0)
    at_resting = RebalanceIntent(symbol=A, direction=LONG, current_quantity=2.0, target_quantity=4.0,
                                 reference_price=PRICE, stop_price=85.0, band=0.25)
    ratcheted = dataclasses.replace(at_resting, stop_price=90.0)
    one = decide([at_resting], snap=snapshot(position)).accepted[0]
    two = decide([ratcheted], snap=snapshot(position)).accepted[0]
    assert two.quantity == pytest.approx(one.quantity)
    assert one.quantity == pytest.approx(40.0 / 15.0 - 2.0, abs=1e-3)  # 0.40 % of 10,000 at 15 USDT, less what is held
    assert two.stop_price == 90.0 and one.stop_price == 85.0        # the boundary tightens to it after the fill
    # the reserved risk is at the order's own stop, the one the boundary re-checks the executable price at: a
    # price above the reference is then refused, and below it the clamp at 85 holds the ceiling
    assert two.risk_amount == pytest.approx(two.quantity * 10.0)
    assert (2.0 + two.quantity) * (PRICE - 85.0) <= 0.004 * EQUITY * (1 + 1e-9)


# ======================================================== the family's broker stage: strength, bars, ratchet
def market(*, strength=1.0, close=PRICE, distance=0.10, symbols=(A,)):
    n = len(symbols)
    features = SimpleNamespace(close=np.full((1, n), float(close)), stop_distance=np.full((1, n), float(distance)))
    candidates = tuple(frozen.Candidate(s, strength, distance, 1e6, 1.0) for s in symbols)
    return DT.MarketDecision(family_id=DT.FAMILY_ID, day=20_000, row=0, features=features, universe=tuple(symbols),
                             candidates=candidates, data_snapshot_id="k1", freshness={},
                             _index={s: j for j, s in enumerate(symbols)})


def broker_decision(mkt, snap):
    return DT.PIPELINE.broker_account(mkt, snap, risk_level="balanced", brake_flags=NO_BRAKE)


@pytest.mark.parametrize("strength", [1.5, -0.1, float("nan"), float("inf")])
def test_a_strength_outside_zero_to_one_refuses_the_decision(strength):
    with pytest.raises(ValueError, match="DECISION_STRENGTH_INVALID"):
        broker_decision(market(strength=strength), snapshot())


@pytest.mark.parametrize("strength", [0.0, 1.0 / 7.0, 1.0])
def test_every_strength_the_frozen_rule_can_produce_is_accepted(strength):
    broker_decision(market(strength=strength), snapshot())


def test_a_held_coin_without_a_decision_bar_refuses_the_decision():
    mkt = market(close=float("nan"))
    with pytest.raises(ValueError, match="BROKER_POSITION_DECISION_BAR_MISSING"):
        broker_decision(mkt, snapshot(held(quantity=2.0, stop=85.0)))


def test_a_position_without_a_mark_price_cannot_be_put_into_a_snapshot():
    info = PositionInfo(venue="binance", symbol=A, side=LONG, quantity=1.0, entry_price=PRICE, mark_price=None,
                        unrealized_pnl=None, leverage=2.0, margin_mode=None, position_mode=None, observed_at_ms=1)
    balance = AccountBalance(venue="binance", margin_asset="USDT", wallet_balance=EQUITY, unrealized_pnl=0.0,
                             equity=EQUITY, available_balance=EQUITY, observed_at_ms=1)
    with pytest.raises(P.PortfolioDataError) as refused:
        P.AccountSnapshot.from_venue("acct-k1", balance, [info], stops={}, as_of_ms=1_000)
    assert refused.value.code == "POSITION_PRICE_UNKNOWN"


def test_a_rebalance_up_carries_the_ratcheted_stop_instead_of_dropping_it():
    """Research rule I10: the stop ratchets at every close. Held 2 at a resting stop of 85; the target is 4 and
    the ratchet is 100 x (1 - 0.10) = 90. Before the fix the rebalance stated 85 and the ratchet was skipped."""
    out = broker_decision(market(), snapshot(held(quantity=2.0, stop=85.0)))
    (intent,) = out.intents
    assert isinstance(intent, RebalanceIntent) and intent.target_quantity > intent.current_quantity
    assert intent.stop_price == pytest.approx(90.0)
    assert [i["stop_price"] for i in out.document["intents"]] == [pytest.approx(90.0)]


def test_inside_the_band_the_ratchet_is_its_own_stop_move():
    out = broker_decision(market(), snapshot(held(quantity=4.0, stop=85.0)))
    (intent,) = out.intents
    assert isinstance(intent, StopReplaceIntent) and intent.new_stop_price == pytest.approx(90.0)


def test_a_rebalance_down_keeps_the_resting_stop_until_the_next_close():
    """Residual of D4, documented: a reduction carries no stop and one symbol carries one intent per decision,
    so the ratchet of a rebalance-down day waits for the next close (the next target is max(resting, ...))."""
    out = broker_decision(market(), snapshot(held(quantity=6.0, stop=85.0)))
    (intent,) = out.intents
    assert isinstance(intent, RebalanceIntent) and intent.target_quantity < intent.current_quantity
    assert intent.stop_price == pytest.approx(85.0)


# ============================================================================================ reason codes
@pytest.mark.parametrize("code", [P.LIQUIDATION_TOO_CLOSE, P.POSITION_MODE_UNSUPPORTED, "DECISION_STRENGTH_INVALID",
                                  "BROKER_POSITION_DECISION_BAR_MISSING", "POSITION_PRICE_UNKNOWN",
                                  "BROKER_POSITION_NOT_SUPPORTED_OR_UNPROTECTED", "ACCOUNT_CAPITAL_BUDGET_REQUIRED"])
def test_the_new_and_the_uncovered_refusals_have_a_customer_description(code):
    entry_ = registry.describe(code)
    assert entry_["severity"] in (registry.WAITING, registry.BLOCKED, registry.ATTENTION) and entry_["message"]
    assert code in P.REFUSAL_REASONS or not code.startswith(("LIQUIDATION", "POSITION_MODE"))
