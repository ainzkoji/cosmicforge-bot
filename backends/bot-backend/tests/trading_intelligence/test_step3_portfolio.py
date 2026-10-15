"""Step 3.7: the per-account multi-position portfolio authorization (``pipeline.portfolio``). Pure, no database.

Every limit is taken from the shared risk-profile authority; a test that needs to isolate one cap derives a
variant with ``dataclasses.replace`` instead of typing a threshold of its own.
"""
from __future__ import annotations

import dataclasses
import math

import pytest

from app.exchange.platform_adapter.contract import AccountBalance, InstrumentRules, PositionInfo
from app.risk.system_limits import SystemLimits
from app.trading_intelligence.execution.risk_sizing import MARGIN_FILL_BUFFER
from app.trading_intelligence.families.daily_trend import targets as frozen
from app.trading_intelligence.pipeline import portfolio as P
from app.trading_intelligence.pipeline.brakes import NO_BRAKE, BrakeFlags
from app.trading_intelligence.pipeline.contracts import (
    LIMIT, LONG, SHORT, AccountRef, AddIntent, DecisionProvenance, EntryIntent, ExitIntent, IntentBatch,
    RebalanceIntent, ReduceIntent, StopReplaceIntent,
)
from shared_lib.risk_levels import LEVELS, SYSTEM_PER_TRADE_RISK_CEILING_PCT, get_profile

EQUITY = 10_000.0
PRICE = 100.0
SYMBOLS = ("AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT", "EEEUSDT", "FFFUSDT", "GGGUSDT", "HHHUSDT", "IIIUSDT")
PAUSED = BrakeFlags(paused=True, halve=False, halted=False)
HALVED = BrakeFlags(paused=False, halve=True, halted=False)
HALTED = BrakeFlags(paused=False, halve=True, halted=True)


def rule(symbol: str, **overrides) -> InstrumentRules:
    values = dict(venue="binance", environment="DEMO", symbol=symbol, venue_symbol=symbol, status="TRADING",
                  venue_status="TRADING", base_asset=symbol[:-4], quote_asset="USDT", margin_asset="USDT",
                  price_tick=0.01, quantity_step=0.001, min_quantity=0.001, max_quantity=1_000_000.0, min_notional=5.0,
                  max_leverage=20.0, contract_size=1.0, fetched_at_ms=1)
    values.update(overrides)
    return InstrumentRules(**values)


RULES = {s: rule(s) for s in SYMBOLS}


def snapshot(*, equity=EQUITY, positions=(), reservations=(), available=None, account="acct-1", as_of=1_000):
    return P.AccountSnapshot(account_id=account, equity=equity, wallet_balance=equity,
                             available_margin=equity if available is None else available, positions=tuple(positions),
                             reservations=tuple(reservations), as_of_ms=as_of)


def held(symbol, quantity, *, mark=PRICE, stop=90.0, direction=LONG, leverage=None):
    return P.PortfolioPosition(symbol=symbol, direction=direction, quantity=quantity, entry_price=mark, mark_price=mark,
                               stop_price=stop, family_id="daily_trend", leverage=leverage)


def entry(symbol, *, risk=0.002, distance=0.10, price=PRICE, equity=EQUITY, leverage=1.0, **extra) -> EntryIntent:
    """A long entry sized as a family sizes it: ``notional = risk x equity / stop distance``."""
    quantity = risk * equity / distance / price
    return EntryIntent(symbol=symbol, direction=LONG, quantity=quantity, reference_price=price,
                       stop_price=price * (1.0 - distance), risk_fraction=risk, leverage=leverage, **extra)


def limits(level="balanced", **changes) -> P.AccountLimits:
    return dataclasses.replace(P.AccountLimits.from_profile(level), **changes)


def decide(intents, *, snap=None, lim=None, rules=None, brake=NO_BRAKE):
    return P.authorize(snap or snapshot(), lim or limits(), intents, RULES if rules is None else rules, brake_state=brake)


# ----------------------------------------------------------------------------------------------------- limits
@pytest.mark.parametrize("level", LEVELS)
def test_limits_are_the_shared_profile_under_the_engine_clamp(level):
    profile, got = get_profile(level), P.AccountLimits.from_profile(level)
    ceiling = float(SYSTEM_PER_TRADE_RISK_CEILING_PCT) / 100
    assert got.approved_per_trade_risk == float(profile.per_trade_risk_pct) / 100
    assert got.per_trade_risk == min(got.approved_per_trade_risk, ceiling) <= SystemLimits().max_risk_per_trade_ceiling
    assert got.ceiling_applied is (got.approved_per_trade_risk > ceiling)
    assert got.max_open_risk == float(profile.max_open_risk_pct) / 100
    assert (got.max_positions, got.max_leverage) == (profile.max_positions, float(profile.leverage_ceiling))
    assert got.max_entry_stop_distance == SystemLimits().max_stop_loss_pct
    assert (got.risk_level, got.profile_version) == (profile.level, profile.version)


@pytest.mark.parametrize("level", LEVELS)
def test_limits_equal_the_frozen_executable_policy_of_the_daily_trend_family(level):
    family, got = frozen.risk_level(level, frozen.EXECUTABLE), P.AccountLimits.from_profile(level)
    assert (got.per_trade_risk, got.approved_per_trade_risk) == (family.risk_per_trade, family.approved_risk_per_trade)
    assert (got.max_open_risk, got.max_positions, got.max_leverage) == (family.max_open_risk, family.max_positions, family.leverage)
    assert got.max_entry_stop_distance == family.max_entry_stop_distance


def test_limits_reject_a_clamp_that_would_raise_risk_and_build_batch_constraints():
    with pytest.raises(P.PortfolioDataError):
        limits(per_trade_risk=0.02)
    with pytest.raises(P.PortfolioDataError):
        limits(max_position_notional=0.0)
    constraints = limits().constraints(EQUITY)
    assert (constraints.per_trade_risk_ceiling, constraints.max_entry_stop_distance) == (limits().per_trade_risk, 0.15)
    assert limits().limits_hash == limits().limits_hash != limits("conservative").limits_hash


# --------------------------------------------------------------------------------------------------- snapshot
def test_snapshot_hash_covers_the_content_and_ignores_the_order_it_was_given_in():
    a, b = held("AAAUSDT", 1.0), held("BBBUSDT", 2.0)
    one, two = snapshot(positions=(a, b)), snapshot(positions=(b, a))
    assert one.snapshot_hash == two.snapshot_hash and one.positions == two.positions
    assert snapshot(positions=(a, b), equity=EQUITY + 1).snapshot_hash != one.snapshot_hash
    assert snapshot(positions=(a, dataclasses.replace(b, stop_price=91.0))).snapshot_hash != one.snapshot_hash
    assert snapshot(positions=(a, b), as_of=2_000).snapshot_hash != one.snapshot_hash
    with pytest.raises(dataclasses.FrozenInstanceError):
        one.equity = 1.0


def test_snapshot_refuses_two_positions_on_one_symbol_and_unusable_numbers():
    with pytest.raises(P.PortfolioDataError):
        snapshot(positions=(held("AAAUSDT", 1.0), held("AAAUSDT", 2.0)))
    with pytest.raises(P.PortfolioDataError):
        snapshot(equity=math.nan)
    with pytest.raises(P.PortfolioDataError):
        held("AAAUSDT", 0.0)
    with pytest.raises(P.PortfolioDataError):
        held("aaausdt", 1.0)


def test_snapshot_from_the_adapter_contract_keeps_only_real_positions_and_never_guesses_a_price():
    balance = AccountBalance(venue="binance", margin_asset="USDT", wallet_balance=9_900.0, unrealized_pnl=100.0,
                             equity=10_000.0, available_balance=7_000.0, observed_at_ms=5)

    def info(symbol, side, quantity, mark=PRICE):
        return PositionInfo(venue="binance", symbol=symbol, side=side, quantity=quantity, entry_price=95.0,
                            mark_price=mark, unrealized_pnl=0.0, leverage=2.0, margin_mode="CROSSED",
                            position_mode="ONE_WAY", observed_at_ms=5)

    snap = P.AccountSnapshot.from_venue("acct-1", balance, [info("AAAUSDT", "LONG", 3.0), info("BBBUSDT", "FLAT", 0.0),
                                                           info("CCCUSDT", "SHORT", 1.0)],
                                        stops={"AAAUSDT": 90.0}, as_of_ms=5, families={"AAAUSDT": "daily_trend"})
    assert [(p.symbol, p.direction, p.stop_price, p.family_id, p.leverage) for p in snap.positions] == [
        ("AAAUSDT", "LONG", 90.0, "daily_trend", 2.0), ("CCCUSDT", "SHORT", None, "", 2.0)]
    assert (snap.equity, snap.wallet_balance, snap.available_margin) == (10_000.0, 9_900.0, 7_000.0)
    with pytest.raises(P.PortfolioDataError) as error:
        P.AccountSnapshot.from_venue("acct-1", balance, [info("AAAUSDT", "LONG", 3.0, mark=None)], stops={}, as_of_ms=5)
    assert error.value.code == "POSITION_PRICE_UNKNOWN"


# -------------------------------------------------------------------------------------------------- open risk
def test_open_risk_is_quantity_times_distance_to_the_stop_over_equity():
    snap = snapshot(positions=(held("AAAUSDT", 2.0, mark=100.0, stop=90.0),
                               held("BBBUSDT", 5.0, mark=50.0, stop=54.0, direction=SHORT)))
    risk = P.open_risk(snap)
    assert risk.by_symbol == (("AAAUSDT", 20.0), ("BBBUSDT", 20.0))
    assert (risk.amount, risk.fraction, risk.unprotected) == (40.0, 40.0 / EQUITY, ())


def test_a_position_without_a_protective_stop_counts_with_its_whole_notional_and_is_reported():
    unknown = held("AAAUSDT", 2.0, stop=None)
    breached = held("BBBUSDT", 1.0, mark=80.0, stop=85.0)           # a long whose stop lies above the mark
    risk = P.open_risk(snapshot(positions=(unknown, breached, held("CCCUSDT", 1.0))))
    assert dict(risk.by_symbol) == {"AAAUSDT": 200.0, "BBBUSDT": 80.0, "CCCUSDT": 10.0}
    assert risk.unprotected == ("AAAUSDT", "BBBUSDT") and risk.amount > 0
    assert P.usage(snapshot(positions=(unknown,)), limits()).unprotected == ("AAAUSDT",)


def test_open_risk_without_equity_is_unbounded_not_zero():
    assert P.open_risk(snapshot(equity=0.0, positions=(held("AAAUSDT", 1.0),))).fraction == math.inf
    assert P.open_risk(snapshot(equity=0.0)).fraction == 0.0


# ------------------------------------------------------------------------------- sizing and exchange precision
def test_an_entry_is_accepted_with_its_quantity_rounded_down_to_the_step_never_up():
    wanted = entry("AAAUSDT", risk=0.00123456)
    result = decide([wanted])
    got = result.authorized("AAAUSDT")
    assert result.refused == () and got.kind == "ENTRY" and got.increases_risk
    assert got.quantity == 1.234 < wanted.quantity and got.limited_by == (P.QUANTITY_STEP,)
    assert got.requested_quantity == wanted.quantity and got.scale < 1.0
    assert (got.notional, got.margin) == (got.quantity * PRICE, got.quantity * PRICE)
    assert got.risk_amount == pytest.approx(got.quantity * 10.0)
    assert result.usage_after.positions == 1 and result.allocation[0].quantity == got.quantity


@pytest.mark.parametrize("missing", ["price_tick", "quantity_step", "min_quantity", "min_notional"])
def test_incomplete_instrument_rules_refuse_the_entry(missing):
    result = decide([entry("AAAUSDT")], rules={"AAAUSDT": rule("AAAUSDT", **{missing: None})})
    assert result.accepted == () and result.refusal("AAAUSDT").reason == P.INSTRUMENT_RULES_INCOMPLETE


def test_missing_rules_and_a_halted_instrument_refuse_the_entry():
    assert decide([entry("AAAUSDT")], rules={}).refusal("AAAUSDT").reason == P.INSTRUMENT_RULES_MISSING
    assert decide([entry("AAAUSDT")], rules={"AAAUSDT": rule("BBBUSDT")}).refusal("AAAUSDT").reason == P.INSTRUMENT_RULES_MISSING
    closed = {"AAAUSDT": rule("AAAUSDT", status="NOT_TRADING", venue_status="SETTLING")}
    assert decide([entry("AAAUSDT")], rules=closed).refusal("AAAUSDT").reason == P.INSTRUMENT_NOT_TRADING


def test_an_order_below_the_exchange_minimum_is_refused_and_never_raised_to_it():
    small = entry("AAAUSDT", risk=0.00004)                          # 4 USDT of notional against a minimum of 5
    refusal = decide([small]).refusal("AAAUSDT")
    assert refusal.reason == P.BELOW_MIN_NOTIONAL and "never raised" in refusal.detail
    tiny = decide([small], rules={"AAAUSDT": rule("AAAUSDT", min_quantity=1.0, min_notional=1.0)})
    assert tiny.refusal("AAAUSDT").reason == P.BELOW_MIN_QUANTITY
    dust = decide([small], rules={"AAAUSDT": rule("AAAUSDT", quantity_step=1.0, min_quantity=1.0)})
    assert dust.refusal("AAAUSDT").reason == P.QUANTITY_ROUNDS_TO_ZERO
    at_minimum = decide([entry("AAAUSDT", risk=0.00005)])           # exactly 5 USDT
    assert at_minimum.authorized("AAAUSDT").notional == 5.0


def test_the_exchange_maximum_quantity_caps_the_order_and_a_market_order_has_its_own_maximum():
    capped = decide([entry("AAAUSDT", risk=0.004)], rules={"AAAUSDT": rule("AAAUSDT", max_quantity=2.5)})
    assert (capped.authorized("AAAUSDT").quantity, capped.authorized("AAAUSDT").limited_by) == (2.5, (P.EXCHANGE_MAX_QUANTITY,))
    market_rule = {"AAAUSDT": rule("AAAUSDT", market_max_quantity=1.5)}
    assert decide([entry("AAAUSDT", risk=0.004)], rules=market_rule).authorized("AAAUSDT").quantity == 1.5
    resting = entry("AAAUSDT", risk=0.004, order_style=LIMIT, limit_price=99.987)
    limit_order = decide([resting], rules=market_rule).authorized("AAAUSDT")
    assert limit_order.quantity == 4.0 and limit_order.limit_price == 99.98     # a buy limit never moves up


def test_prices_are_put_on_the_tick_toward_the_market_only():
    assert P.conform_stop_price(LONG, 90.004, 0.01) == 90.01 and P.conform_stop_price(SHORT, 110.006, 0.01) == 110.0
    assert P.conform_stop_price(LONG, 90.0, 0.01) == 90.0 and P.conform_stop_price(LONG, 90.004, None) == 90.004
    assert P.conform_limit_price(LONG, 99.987, 0.01) == 99.98 and P.conform_limit_price(SHORT, 99.981, 0.01) == 99.99
    off_tick = EntryIntent(symbol="AAAUSDT", direction=LONG, quantity=1.0, reference_price=PRICE, stop_price=90.004,
                           risk_fraction=0.001)
    got = decide([off_tick]).authorized("AAAUSDT")
    assert got.stop_price == 90.01 and P.PRICE_TICK in got.limited_by
    assert got.risk_amount == pytest.approx(1.0 * (PRICE - 90.004))     # measured on the stated, wider stop
    hairline = EntryIntent(symbol="AAAUSDT", direction=LONG, quantity=1.0, reference_price=PRICE, stop_price=99.999,
                           risk_fraction=0.0000001)
    assert decide([hairline], rules={"AAAUSDT": rule("AAAUSDT", price_tick=1.0)}).refusal("AAAUSDT").reason == P.INVALID_STOP_GEOMETRY


# ------------------------------------------------------------------------------------- the two engine limits
def test_an_entry_stop_wider_than_the_engine_limit_is_refused_and_never_clipped():
    at_limit = decide([entry("AAAUSDT", distance=0.15)])
    assert at_limit.refused == () and at_limit.authorized("AAAUSDT").stop_price == 85.0
    wide = entry("BBBUSDT", distance=0.150001)
    result = decide([wide, entry("AAAUSDT", distance=0.15)])
    refusal = result.refusal("BBBUSDT")
    assert refusal.reason == P.ENTRY_STOP_TOO_WIDE and "never clipped" in refusal.detail
    assert [a.symbol for a in result.accepted] == ["AAAUSDT"]       # no order exists for it, at any stop
    assert refusal.intent is wide and wide.stop_price == PRICE * (1.0 - 0.150001)
    much_wider = decide([entry("AAAUSDT", distance=0.40, risk=0.004)])
    assert much_wider.accepted == () and much_wider.refusal("AAAUSDT").reason == P.ENTRY_STOP_TOO_WIDE


def test_an_increase_of_a_held_position_obeys_the_same_stop_limit():
    snap = snapshot(positions=(held("AAAUSDT", 1.0, stop=80.0),))
    add = AddIntent(symbol="AAAUSDT", direction=LONG, quantity=0.5, reference_price=PRICE, stop_price=80.0)
    assert decide([add], snap=snap).refusal("AAAUSDT").reason == P.ENTRY_STOP_TOO_WIDE


def test_a_stated_risk_above_the_engine_ceiling_is_refused():
    assert limits("aggressive").approved_per_trade_risk > limits("aggressive").per_trade_risk
    mandate_sized = entry("AAAUSDT", risk=limits("aggressive").approved_per_trade_risk)
    refusal = decide([mandate_sized], lim=limits("aggressive")).refusal("AAAUSDT")
    assert refusal.reason == P.RISK_ABOVE_CEILING
    assert decide([entry("AAAUSDT", risk=limits().per_trade_risk)]).refused == ()      # exactly at the ceiling


def test_the_per_trade_clamp_is_measured_on_the_snapshot_equity():
    sized_for_more = entry("AAAUSDT", risk=limits().per_trade_risk, equity=EQUITY)     # 40 USDT at the stop
    result = decide([sized_for_more], snap=snapshot(equity=EQUITY * 0.9))              # equity fell since the decision
    got = result.authorized("AAAUSDT")
    assert got.limited_by[0] == P.RISK_ABOVE_CEILING and got.quantity == pytest.approx(3.6)
    assert got.risk_amount <= limits().per_trade_risk * EQUITY * 0.9 * (1 + 1e-9)


def test_an_add_cannot_take_a_position_beyond_the_per_trade_ceiling():
    ceiling_amount = limits().per_trade_risk * EQUITY               # 40 USDT
    snap = snapshot(positions=(held("AAAUSDT", 3.0, stop=90.0),))   # carries 30 of it
    add = AddIntent(symbol="AAAUSDT", direction=LONG, quantity=2.0, reference_price=PRICE, stop_price=90.0)
    got = decide([add], snap=snap).authorized("AAAUSDT")
    assert (got.quantity, got.position_quantity_after) == (1.0, 4.0) and got.limited_by == (P.RISK_ABOVE_CEILING,)
    assert got.position_quantity_after * 10.0 == ceiling_amount
    full = snapshot(positions=(held("AAAUSDT", 4.0, stop=90.0),))
    assert decide([add], snap=full).refusal("AAAUSDT").reason == P.RISK_ABOVE_CEILING


# ------------------------------------------------------------------------------------------- position limit
def test_the_position_limit_admits_in_the_given_order_and_refuses_the_rest():
    lim = limits("conservative")
    wanted = [entry(s, risk=0.001) for s in SYMBOLS[:6]]
    result = decide(wanted, lim=lim)
    assert [a.symbol for a in result.accepted] == list(SYMBOLS[:lim.max_positions])
    assert {r.symbol: r.reason for r in result.refused} == {s: P.POSITION_LIMIT for s in SYMBOLS[lim.max_positions:6]}
    assert result.usage_after.positions == lim.max_positions


def test_positions_and_open_reservations_occupy_slots():
    lim = limits("conservative")
    reserved = P.OpenReservation("r1", "CCCUSDT", LONG, "ENTRY", risk_amount=5.0, notional=50.0, margin=50.0, state="RESERVED")
    snap = snapshot(positions=(held("AAAUSDT", 0.5), held("BBBUSDT", 0.5)), reservations=(reserved,))
    result = decide([entry("DDDUSDT", risk=0.001), entry("EEEUSDT", risk=0.001)], snap=snap, lim=lim)
    assert result.usage_before.positions == 3 and result.usage_before.symbols == ("AAAUSDT", "BBBUSDT", "CCCUSDT")
    assert [a.symbol for a in result.accepted] == ["DDDUSDT"] and result.refusal("EEEUSDT").reason == P.POSITION_LIMIT


def test_a_reservation_and_the_position_it_became_are_one_commitment():
    reserved = P.OpenReservation("r1", "AAAUSDT", LONG, "ENTRY", risk_amount=40.0, notional=400.0, margin=400.0, state="SUBMITTED")
    partly_filled = snapshot(positions=(held("AAAUSDT", 1.0),), reservations=(reserved,))
    use = P.usage(partly_filled, limits())
    assert (use.positions, use.open_risk_amount, use.gross_notional, use.reserved_margin) == (1, 40.0, 400.0, 400.0)
    increase = P.OpenReservation("r2", "AAAUSDT", LONG, "ADD", risk_amount=10.0, notional=100.0, margin=100.0, state="RESERVED")
    more = P.usage(snapshot(positions=(held("AAAUSDT", 1.0),), reservations=(increase,)), limits())
    assert (more.positions, more.open_risk_amount, more.gross_notional) == (1, 20.0, 200.0)


# ------------------------------------------------------------------------------------------- shared budgets
def test_the_open_risk_cap_scales_every_increase_by_one_common_factor():
    lim = limits()                                                   # 6 positions, 0.40 % each, 2 % cap
    wanted = [entry(s, risk=lim.per_trade_risk) for s in SYMBOLS[:6]]
    result = decide(wanted, lim=lim)
    factor = lim.max_open_risk / (6 * lim.per_trade_risk)
    assert result.scales.open_risk == pytest.approx(factor) and result.refused == ()
    assert {a.quantity for a in result.accepted} == {3.333}          # 4.0 x 5/6, on the step: nobody is preferred
    assert all(a.limited_by[0] == P.OPEN_RISK_CAP for a in result.accepted)
    assert result.usage_after.open_risk_fraction <= lim.max_open_risk
    assert result.usage_after.open_risk_fraction == pytest.approx(lim.max_open_risk, rel=1e-3)


def test_the_open_risk_cap_counts_what_is_already_held_and_reserved():
    lim = limits()                                                   # cap: 200 USDT at risk
    reserved = P.OpenReservation("r1", "BBBUSDT", LONG, "ENTRY", risk_amount=40.0, notional=400.0, margin=400.0, state="RESERVED")
    snap = snapshot(positions=(held("AAAUSDT", 12.0, stop=90.0),), reservations=(reserved,))    # 120 + 40 committed
    result = decide([entry("CCCUSDT", risk=0.004), entry("DDDUSDT", risk=0.004)], snap=snap, lim=lim)
    assert result.scales.open_risk == pytest.approx(0.5)
    assert sum(a.risk_amount for a in result.accepted) == pytest.approx(40.0)
    assert result.usage_after.open_risk_amount == pytest.approx(200.0)
    assert result.authorized("AAAUSDT") is None                      # an existing position is never reduced here


def test_no_open_risk_headroom_refuses_with_the_cap_as_the_reason():
    snap = snapshot(positions=(held("AAAUSDT", 25.0, stop=90.0),))   # 250 at risk against a cap of 200
    result = decide([entry("BBBUSDT"), entry("CCCUSDT")], snap=snap)
    assert result.scales.open_risk == 0.0 and result.accepted == ()
    assert {r.reason for r in result.refused} == {P.OPEN_RISK_CAP}
    scaled_below_minimum = decide([entry("BBBUSDT", risk=0.004)], snap=snapshot(positions=(held("AAAUSDT", 19.96, stop=90.0),)))
    refusal = scaled_below_minimum.refusal("BBBUSDT")                # 0.4 USDT of room: 4 USDT of notional, minimum 5
    assert refusal.reason == P.OPEN_RISK_CAP and "minimum notional" in refusal.detail


def test_the_leverage_cap_limits_gross_notional_over_equity():
    lim = limits(max_leverage=1.0, max_open_risk=1.0)                # isolate the leverage budget
    snap = snapshot(positions=(held("AAAUSDT", 20.0, stop=99.0),))   # 2 000 of notional held
    wanted = [entry(s, risk=0.004, distance=0.01) for s in SYMBOLS[1:4]]     # 4 000 each
    result = decide(wanted, snap=snap, lim=lim)
    assert result.scales.leverage == pytest.approx(8_000.0 / 12_000.0) and result.scales.open_risk == 1.0
    assert all(P.LEVERAGE_CAP in a.limited_by for a in result.accepted)
    assert result.usage_after.gross_leverage <= lim.max_leverage
    assert result.usage_after.gross_notional == pytest.approx(EQUITY, rel=1e-3)
    assert decide([entry("BBBUSDT", leverage=3.0)]).refusal("BBBUSDT").reason == P.LEVERAGE_CAP      # above the account's 2x
    venue = {"BBBUSDT": rule("BBBUSDT", max_leverage=1.0)}
    assert decide([entry("BBBUSDT", leverage=2.0)], rules=venue).refusal("BBBUSDT").reason == P.LEVERAGE_CAP


def test_available_margin_is_account_wide_and_keeps_the_existing_fill_buffer():
    other_bot = P.OpenReservation("r1", "CCCUSDT", LONG, "ENTRY", risk_amount=10.0, notional=400.0, margin=400.0, state="SUBMITTED")
    snap = snapshot(available=1_000.0, reservations=(other_bot,))
    usable = (1_000.0 - 400.0) * (1.0 - float(MARGIN_FILL_BUFFER))
    assert P.usage(snap, limits()).usable_margin == usable
    result = decide([entry("AAAUSDT", risk=0.004), entry("BBBUSDT", risk=0.004)], snap=snap)     # 400 of margin each
    assert result.scales.margin == pytest.approx(usable / 800.0)
    assert sum(a.margin for a in result.accepted) <= usable
    assert all(P.INSUFFICIENT_MARGIN in a.limited_by for a in result.accepted)
    leveraged = decide([entry("AAAUSDT", risk=0.004, leverage=2.0)], snap=snapshot(available=250.0))
    assert leveraged.authorized("AAAUSDT").margin == 200.0 and leveraged.scales.margin == 1.0
    none_left = decide([entry("AAAUSDT")], snap=snapshot(available=400.0, reservations=(other_bot,)))
    assert none_left.refusal("AAAUSDT").reason == P.INSUFFICIENT_MARGIN


def test_budgets_apply_in_the_frozen_order_and_each_reports_its_factor():
    lim = limits(max_leverage=1.0)
    wanted = [entry(s, risk=lim.per_trade_risk, distance=0.01) for s in SYMBOLS[:6]]      # 4 000 of notional each
    result = decide(wanted, snap=snapshot(available=6_000.0), lim=lim)
    risk_factor = lim.max_open_risk / (6 * lim.per_trade_risk)
    assert result.scales.open_risk == pytest.approx(risk_factor)
    assert result.scales.leverage == pytest.approx(EQUITY / (24_000.0 * risk_factor))
    assert result.scales.margin == pytest.approx(6_000.0 * (1.0 - float(MARGIN_FILL_BUFFER)) / EQUITY)
    assert len(result.accepted) == 6 and len({a.quantity for a in result.accepted}) == 1
    assert all(a.limited_by[:3] == (P.OPEN_RISK_CAP, P.LEVERAGE_CAP, P.INSUFFICIENT_MARGIN) for a in result.accepted)


def test_the_customer_maximum_position_caps_one_position():
    lim = limits(max_position_notional=250.0)
    got = decide([entry("AAAUSDT", risk=0.004)], lim=lim).authorized("AAAUSDT")
    assert (got.quantity, got.limited_by) == (2.5, (P.CUSTOMER_MAX_POSITION,))
    snap = snapshot(positions=(held("AAAUSDT", 2.5),))
    add = AddIntent(symbol="AAAUSDT", direction=LONG, quantity=0.5, reference_price=PRICE, stop_price=90.0)
    assert decide([add], snap=snap, lim=lim).refusal("AAAUSDT").reason == P.CUSTOMER_MAX_POSITION
    below_minimum = decide([entry("AAAUSDT", risk=0.004)], lim=limits(max_position_notional=4.0))
    assert below_minimum.refusal("AAAUSDT").reason == P.CUSTOMER_MAX_POSITION


# ----------------------------------------------------------------------------------------------------- brakes
def test_an_unknown_brake_state_refuses_every_increase():
    result = decide([entry("AAAUSDT")], brake=None)
    assert result.refusal("AAAUSDT").reason == P.BRAKE_STATE_UNKNOWN and result.close_all_positions is False


def test_the_terminal_brake_refuses_every_increase_and_asks_for_every_position_to_be_closed():
    snap = snapshot(positions=(held("AAAUSDT", 1.0), held("BBBUSDT", 2.0, direction=SHORT, stop=110.0)))
    add = AddIntent(symbol="AAAUSDT", direction=LONG, quantity=0.5, reference_price=PRICE, stop_price=90.0)
    result = decide([entry("CCCUSDT"), add], snap=snap, brake=HALTED)
    assert result.close_all_positions and result.accepted == ()
    assert {r.reason for r in result.refused} == {P.BRAKE_ACTIVE} and all("TERMINAL_STOP" in r.detail for r in result.refused)
    exits = P.flatten_intents(snap)
    assert [(e.symbol, e.direction, e.reason) for e in exits] == [("AAAUSDT", LONG, "BRAKE"), ("BBBUSDT", SHORT, "BRAKE")]
    closing = decide(exits, snap=snap, brake=HALTED)
    assert closing.refused == () and [(a.symbol, a.quantity, a.position_quantity_after) for a in closing.accepted] == [
        ("AAAUSDT", 1.0, 0.0), ("BBBUSDT", 2.0, 0.0)]
    assert closing.usage_after.positions == 0 and closing.allocation == ()


def test_the_daily_pause_blocks_new_positions_only():
    snap = snapshot(positions=(held("AAAUSDT", 1.0),))
    add = AddIntent(symbol="AAAUSDT", direction=LONG, quantity=0.5, reference_price=PRICE, stop_price=90.0)
    result = decide([entry("BBBUSDT"), add, ExitIntent(symbol="CCCUSDT", direction=LONG, reason="SIGNAL")],
                    snap=snapshot(positions=(held("AAAUSDT", 1.0), held("CCCUSDT", 1.0))), brake=PAUSED)
    assert result.refusal("BBBUSDT").reason == P.BRAKE_ACTIVE and "DAILY_LOSS_PAUSE" in result.refusal("BBBUSDT").detail
    assert {a.symbol for a in result.accepted} == {"AAAUSDT", "CCCUSDT"} and not result.close_all_positions
    assert decide([add], snap=snap, brake=PAUSED).refused == ()


def test_the_halve_brake_bounds_a_position_at_half_the_per_trade_ceiling():
    lim = limits()
    halved_by_the_family = entry("AAAUSDT", risk=lim.per_trade_risk * P.HALVE_FACTOR)
    untouched = decide([halved_by_the_family], brake=HALVED).authorized("AAAUSDT")
    assert untouched.limited_by == () and untouched.quantity == 2.0
    not_halved = decide([entry("BBBUSDT", risk=lim.per_trade_risk)], brake=HALVED).authorized("BBBUSDT")
    assert not_halved.limited_by == (P.BRAKE_ACTIVE,) and not_halved.quantity == 2.0
    assert not_halved.risk_amount == pytest.approx(lim.per_trade_risk * P.HALVE_FACTOR * EQUITY)
    snap = snapshot(positions=(held("CCCUSDT", 2.0, stop=90.0),))
    add = AddIntent(symbol="CCCUSDT", direction=LONG, quantity=1.0, reference_price=PRICE, stop_price=90.0)
    refused = decide([add], snap=snap, brake=HALVED).refusal("CCCUSDT")
    assert refused.reason == P.BRAKE_ACTIVE and "DRAWDOWN_HALVE" in refused.detail
    assert decide([add], snap=snap).refused == ()


def test_risk_reducing_intents_pass_every_cap_and_every_brake():
    over_everything = snapshot(
        equity=1_000.0, available=0.0,
        positions=(held("AAAUSDT", 50.0, stop=None), held("BBBUSDT", 50.0, stop=80.0), held("CCCUSDT", 50.0, stop=90.0),
                   held("DDDUSDT", 50.0), held("EEEUSDT", 50.0)))
    lim = limits("conservative")
    use = P.usage(over_everything, lim)
    assert use.positions > lim.max_positions and use.open_risk_fraction > lim.max_open_risk and use.gross_leverage > lim.max_leverage
    reductions = [ExitIntent(symbol="AAAUSDT", direction=LONG, reason="SIGNAL"),
                  ReduceIntent(symbol="BBBUSDT", direction=LONG, reason="TRIM", fraction=0.5),
                  StopReplaceIntent(symbol="CCCUSDT", direction=LONG, reason="TRAIL", new_stop_price=95.0, current_stop_price=90.0),
                  RebalanceIntent(symbol="DDDUSDT", direction=LONG, current_quantity=50.0, target_quantity=20.0,
                                  reference_price=PRICE, stop_price=90.0, band=0.25)]
    for brake in (NO_BRAKE, PAUSED, HALVED, HALTED, None):
        result = decide(reductions, snap=over_everything, lim=lim, brake=brake)
        assert result.refused == () and [a.increases_risk for a in result.accepted] == [False] * 4
        assert [(a.symbol, a.quantity, a.position_quantity_after) for a in result.accepted] == [
            ("AAAUSDT", 50.0, 0.0), ("BBBUSDT", 25.0, 25.0), ("CCCUSDT", 50.0, 50.0), ("DDDUSDT", 30.0, 20.0)]
        assert result.authorized("CCCUSDT").stop_price == 95.0
        assert all(a.risk_amount == 0.0 and a.margin == 0.0 for a in result.accepted)


def test_reductions_respect_the_position_and_the_exchange_filters_but_a_full_close_passes_them():
    snap = snapshot(positions=(held("AAAUSDT", 1.2345), held("BBBUSDT", 0.04)))
    whole = decide([ReduceIntent(symbol="AAAUSDT", direction=LONG, reason="TRIM", quantity=5.0)], snap=snap).authorized("AAAUSDT")
    assert (whole.quantity, whole.position_quantity_after, whole.limited_by) == (1.2345, 0.0, (P.CAPPED_TO_POSITION,))
    part = decide([ReduceIntent(symbol="AAAUSDT", direction=LONG, reason="TRIM", fraction=0.3)], snap=snap).authorized("AAAUSDT")
    assert part.quantity == 0.37 and part.limited_by == (P.QUANTITY_STEP,)
    dust = decide([ReduceIntent(symbol="BBBUSDT", direction=LONG, reason="TRIM", fraction=0.5)], snap=snap)
    assert dust.refusal("BBBUSDT").reason == P.BELOW_MIN_NOTIONAL      # 2 USDT: the venue would reject a partial close
    closed = decide([ExitIntent(symbol="BBBUSDT", direction=LONG, reason="SIGNAL")], snap=snap, rules={})
    assert closed.authorized("BBBUSDT").quantity == 0.04              # 4 USDT, no rules at all: an exit is not withheld
    blind = decide([ReduceIntent(symbol="AAAUSDT", direction=LONG, reason="TRIM", quantity=0.5)], snap=snap, rules={})
    assert blind.authorized("AAAUSDT").limited_by == (P.RULES_UNAVAILABLE,)
    assert decide([ExitIntent(symbol="CCCUSDT", direction=LONG, reason="SIGNAL")], snap=snap).refusal("CCCUSDT").reason == P.NO_POSITION
    wrong_side = decide([ExitIntent(symbol="AAAUSDT", direction=SHORT, reason="SIGNAL")], snap=snap)
    assert wrong_side.refusal("AAAUSDT").reason == P.DIRECTION_MISMATCH


def test_a_stop_replacement_never_widens_the_resting_stop_and_must_stay_protective():
    snap = snapshot(positions=(held("AAAUSDT", 1.0, stop=95.0), held("BBBUSDT", 1.0, stop=None)))

    def restore(symbol, price):
        return StopReplaceIntent(symbol=symbol, direction=LONG, reason="RESIZE", new_stop_price=price, current_stop_price=None)

    assert decide([restore("AAAUSDT", 94.0)], snap=snap).refusal("AAAUSDT").reason == P.STOP_WIDENING_REFUSED
    assert decide([restore("AAAUSDT", 96.004)], snap=snap).authorized("AAAUSDT").stop_price == 96.01
    assert decide([restore("BBBUSDT", 80.0)], snap=snap).authorized("BBBUSDT").stop_price == 80.0     # protection restored
    assert decide([restore("BBBUSDT", 101.0)], snap=snap).refusal("BBBUSDT").reason == P.INVALID_STOP_GEOMETRY
    add_with_wider_stop = AddIntent(symbol="AAAUSDT", direction=LONG, quantity=0.1, reference_price=PRICE, stop_price=94.0)
    assert decide([add_with_wider_stop], snap=snap).refusal("AAAUSDT").reason == P.STOP_WIDENING_REFUSED


# --------------------------------------------------------------------------------------------------- rebalance
def rebalance(symbol, held_quantity, target, band=0.25, stop=95.0):
    return RebalanceIntent(symbol=symbol, direction=LONG, current_quantity=held_quantity, target_quantity=target,
                           reference_price=PRICE, stop_price=stop, band=band)


def test_the_rebalancing_band_is_the_frozen_rule():
    for held_quantity in (0.5, 0.74, 0.75, 0.76, 1.0, 1.24, 1.25, 1.26, 2.0):
        decision = frozen.Decision(targets={"AAAUSDT": 1.0 * PRICE}, stop_distance={"AAAUSDT": 0.1}, strength={"AAAUSDT": 1.0})
        orders = frozen.orders_from_targets(decision, held_quantity={"AAAUSDT": held_quantity}, close={"AAAUSDT": PRICE})
        assert P.rebalance_required(held_quantity, 1.0, 0.25) is bool(orders)


def test_a_rebalance_inside_the_band_is_not_an_action_in_either_direction():
    snap = snapshot(positions=(held("AAAUSDT", 1.0, stop=95.0), held("BBBUSDT", 1.0, stop=95.0)))
    result = decide([rebalance("AAAUSDT", 1.0, 1.3), rebalance("BBBUSDT", 1.0, 0.85)], snap=snap)
    assert {r.symbol: r.reason for r in result.refused} == {"AAAUSDT": P.INSIDE_REBALANCE_BAND, "BBBUSDT": P.INSIDE_REBALANCE_BAND}


def test_a_rebalance_beyond_the_band_is_sized_from_what_the_account_holds_now():
    snap = snapshot(positions=(held("AAAUSDT", 1.0, stop=95.0), held("BBBUSDT", 2.0, stop=95.0)))
    result = decide([rebalance("AAAUSDT", 1.0, 2.0), rebalance("BBBUSDT", 2.0, 1.0)], snap=snap)
    down, up = result.accepted
    assert (down.symbol, down.increases_risk, down.quantity, down.position_quantity_after) == ("BBBUSDT", False, 1.0, 1.0)
    assert (up.symbol, up.increases_risk, up.quantity, up.position_quantity_after) == ("AAAUSDT", True, 1.0, 2.0)
    assert up.risk_amount == 5.0
    stale = decide([rebalance("AAAUSDT", 0.5, 1.2)], snap=snap)       # the decision saw 0.5; the account holds 1.0
    assert stale.refusal("AAAUSDT").reason == P.INSIDE_REBALANCE_BAND
    assert decide([rebalance("CCCUSDT", 1.0, 2.0)], snap=snap).refusal("CCCUSDT").reason == P.NO_POSITION


# ---------------------------------------------------------------------------------------------------- releases
def test_an_exit_releases_its_slot_risk_and_margin_to_an_entry_of_the_same_decision():
    lim = limits("conservative")
    full = snapshot(available=0.0, positions=tuple(held(s, 25.0, stop=99.0, leverage=1.0) for s in SYMBOLS[:4]))
    assert P.usage(full, lim).positions == lim.max_positions and P.usage(full, lim).gross_leverage == lim.max_leverage
    newcomer = entry("EEEUSDT", risk=lim.per_trade_risk, distance=0.01)
    assert decide([newcomer], snap=full, lim=lim).refusal("EEEUSDT").reason == P.POSITION_LIMIT
    rotation = decide([newcomer, ExitIntent(symbol="AAAUSDT", direction=LONG, reason="SIGNAL")], snap=full, lim=lim)
    assert [(a.symbol, a.kind) for a in rotation.accepted] == [("AAAUSDT", "EXIT"), ("EEEUSDT", "ENTRY")]    # exits first
    assert rotation.releases == (P.Release("AAAUSDT", 0.0),)
    replacement = rotation.authorized("EEEUSDT")
    assert replacement.awaits_releases and replacement.quantity == pytest.approx(25.0 * (1.0 - float(MARGIN_FILL_BUFFER)), abs=1e-3)
    assert rotation.usage_after.positions == lim.max_positions and rotation.usage_after.symbols[-1] == "EEEUSDT"


def test_an_entry_that_fits_without_the_release_does_not_wait_for_it():
    snap = snapshot(positions=(held("AAAUSDT", 1.0),))
    result = decide([entry("BBBUSDT"), ExitIntent(symbol="AAAUSDT", direction=LONG, reason="SIGNAL")], snap=snap)
    assert result.authorized("BBBUSDT").awaits_releases is False and len(result.releases) == 1


# ------------------------------------------------------------------------------------- state and contradictions
def test_position_and_reservation_state_gate_an_intent_before_any_sizing():
    reserved = P.OpenReservation("r1", "BBBUSDT", LONG, "ENTRY", risk_amount=5.0, notional=50.0, margin=50.0, state="RESERVED")
    snap = snapshot(positions=(held("AAAUSDT", 1.0), held("CCCUSDT", 1.0, direction=SHORT, stop=110.0)), reservations=(reserved,))
    add_missing = AddIntent(symbol="DDDUSDT", direction=LONG, quantity=1.0, reference_price=PRICE, stop_price=90.0)
    add_wrong_side = AddIntent(symbol="CCCUSDT", direction=LONG, quantity=1.0, reference_price=PRICE, stop_price=90.0)
    result = decide([entry("AAAUSDT"), entry("BBBUSDT"), add_missing, add_wrong_side, entry("EEEUSDT"),
                     ExitIntent(symbol="EEEUSDT", direction=LONG, reason="SIGNAL")], snap=snap)
    assert {r.symbol: r.reason for r in result.refused} == {
        "AAAUSDT": P.POSITION_EXISTS, "BBBUSDT": P.DUPLICATE_RESERVATION, "DDDUSDT": P.NO_POSITION,
        "CCCUSDT": P.DIRECTION_MISMATCH, "EEEUSDT": P.CONTRADICTORY_ACTION}
    assert [a.symbol for a in result.accepted] == ["EEEUSDT"]        # the first statement on the symbol stands
    assert all(r.reason in P.REFUSAL_REASONS for r in result.refused)


def test_a_legacy_claim_counts_at_the_per_trade_ceiling_and_an_unsized_one_blocks_increases():
    lim = limits()
    sized = P.OpenReservation("l1", "AAAUSDT", LONG, "ENTRY", risk_amount=None, notional=300.0, margin=None,
                              state="SUBMITTED", source="LEGACY_PENDING_ENTRY")
    use = P.usage(snapshot(reservations=(sized,)), lim)
    assert (use.positions, use.open_risk_amount, use.gross_notional, use.reserved_margin) == (1, lim.per_trade_risk * EQUITY, 300.0, 300.0)
    assert decide([entry("BBBUSDT")], snap=snapshot(reservations=(sized,))).refused == ()
    unsized = dataclasses.replace(sized, notional=None)
    blocked = decide([entry("BBBUSDT"), ExitIntent(symbol="CCCUSDT", direction=LONG, reason="SIGNAL")],
                     snap=snapshot(positions=(held("CCCUSDT", 1.0),), reservations=(unsized,)))
    assert blocked.refusal("BBBUSDT").reason == P.LEGACY_CLAIM_UNSIZED and blocked.authorized("CCCUSDT") is not None
    realised = snapshot(positions=(held("AAAUSDT", 3.0),), reservations=(unsized,))
    assert P.usage(realised, lim).unsized == () and P.usage(realised, lim).open_risk_amount == 30.0
    assert decide([entry("BBBUSDT")], snap=realised).refused == ()


def test_no_equity_refuses_increases():
    assert decide([entry("AAAUSDT")], snap=snapshot(equity=0.0)).refusal("AAAUSDT").reason == P.EQUITY_UNAVAILABLE


# -------------------------------------------------------------------------------------- batches and determinism
def batch(intents, *, account="acct-1", lim=None, equity=EQUITY) -> IntentBatch:
    provenance = DecisionProvenance(family_id="daily_trend", family_version="1.0.0", mandate_id="M004", hypothesis_id="H18",
                                    rule_hash="r" * 8, decision_id="dec-1", decision_hash="h" * 8, decision_time_ms=1_000,
                                    data_snapshot_id="data-1", timeframe="1d", mode="SHADOW")
    return IntentBatch(provenance=provenance, account=AccountRef("user-1", account, "binance", "DEMO"),
                       intents=tuple(intents), constraints=(lim or limits()).constraints(equity),
                       instruments=frozenset(SYMBOLS), created_at_ms=1_000, expires_at_ms=90_000)


def test_a_batch_is_authorized_like_its_intents_and_only_for_its_own_account():
    wanted = [entry("AAAUSDT"), entry("BBBUSDT")]
    assert decide(batch(wanted)).authorization_hash == decide(wanted).authorization_hash
    with pytest.raises(P.PortfolioDataError) as error:
        decide(batch(wanted, account="acct-2"))
    assert error.value.code == "ACCOUNT_MISMATCH"


def test_authorization_is_deterministic_and_bound_to_its_inputs():
    wanted = [entry(s, risk=0.004) for s in SYMBOLS[:6]]
    first, second = decide(wanted), decide(wanted)
    assert first == second and first.authorization_hash == second.authorization_hash
    assert first.snapshot_hash == snapshot().snapshot_hash and first.limits_hash == limits().limits_hash
    assert decide(wanted, snap=snapshot(equity=EQUITY + 1)).authorization_hash != first.authorization_hash
    assert decide(wanted, brake=HALVED).authorization_hash != first.authorization_hash


def test_the_layer_never_reorders_what_the_family_asked_for():
    lim = limits("conservative")
    weakest_first = [entry(s, risk=0.0005 * (i + 1)) for i, s in enumerate(reversed(SYMBOLS[:6]))]
    result = decide(weakest_first, lim=lim)
    assert [a.symbol for a in result.accepted] == [i.symbol for i in weakest_first[:lim.max_positions]]
    assert [r.symbol for r in result.refused] == [i.symbol for i in weakest_first[lim.max_positions:]]


@pytest.mark.parametrize("level", LEVELS)
@pytest.mark.parametrize("halve", [False, True])
def test_a_frozen_daily_trend_decision_passes_unchanged(level, halve):
    """The family's own caps are this layer's caps: a decision of the frozen rule is authorized as it stands."""
    family = frozen.risk_level(level, frozen.EXECUTABLE)
    distances = (0.05, 0.08, 0.12, 0.15, 0.06, 0.09, 0.11, 0.14, 0.07)
    candidates = [frozen.Candidate(s, 1.0, d, volume=1e9 - i) for i, (s, d) in enumerate(zip(SYMBOLS, distances))]
    decision = frozen.decide_targets(equity=EQUITY, candidates=candidates, held={}, level=family, halve=halve)
    orders = frozen.orders_from_targets(decision, held_quantity={}, close={s: PRICE for s in SYMBOLS})
    assert len(orders) == family.max_positions
    # full strength on every slot: the frozen open-risk cap binds wherever slots x per-trade risk exceeds it
    binds = not halve and family.max_positions * family.risk_per_trade > family.max_open_risk
    assert (decision.scale["open_risk"] < 1.0) is binds
    wanted = [EntryIntent(symbol=o.symbol, direction=LONG, quantity=o.quantity, reference_price=PRICE,
                          stop_price=PRICE * (1.0 - o.stop_distance), leverage=family.leverage,
                          risk_fraction=o.target_notional * o.stop_distance / EQUITY) for o in orders]
    fine = {s: rule(s, quantity_step=1e-8, min_quantity=1e-8, price_tick=1e-8) for s in SYMBOLS}
    result = decide(wanted, lim=P.AccountLimits.from_profile(level), rules=fine,
                    brake=BrakeFlags(paused=False, halve=halve, halted=False))
    assert result.refused == () and result.scales == P.BudgetScales()
    for order, got in zip(orders, result.accepted):
        assert got.symbol == order.symbol and got.notional == pytest.approx(order.target_notional, rel=1e-6)
        assert got.limited_by in ((), (P.QUANTITY_STEP,), (P.PRICE_TICK,), (P.QUANTITY_STEP, P.PRICE_TICK))
    assert result.usage_after.open_risk_fraction == pytest.approx(decision.open_risk / EQUITY, rel=1e-6)
    assert result.usage_after.gross_notional == pytest.approx(decision.gross_notional, rel=1e-6)


def test_the_result_describes_the_budget_use_and_the_resulting_portfolio():
    snap = snapshot(positions=(held("AAAUSDT", 2.0, stop=90.0),))
    result = decide([entry("BBBUSDT", risk=0.002), ReduceIntent(symbol="AAAUSDT", direction=LONG, reason="TRIM", fraction=0.5)], snap=snap)
    assert (result.usage_before.positions, result.usage_before.open_risk_amount, result.usage_before.gross_notional) == (1, 20.0, 200.0)
    assert (result.usage_after.positions, result.usage_after.open_risk_amount, result.usage_after.gross_notional) == (2, 30.0, 300.0)
    assert [(line.symbol, line.quantity, line.notional, line.fraction_of_equity, line.risk_amount) for line in result.allocation] == [
        ("AAAUSDT", 1.0, 100.0, 0.01, 10.0), ("BBBUSDT", 2.0, 200.0, 0.02, 20.0)]
    assert result.increases == (result.authorized("BBBUSDT"),) and result.account_id == "acct-1"
