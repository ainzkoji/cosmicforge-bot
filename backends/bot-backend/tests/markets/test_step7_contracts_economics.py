"""Step 7.2 -- contract specifications, deterministic rolls with no trading of expired or unlisted contracts,
and contract economics in Decimal."""
from __future__ import annotations

from datetime import date
from decimal import Decimal as D

import pytest

import _step7 as F
from app.markets import economics as E
from app.markets.calendar import local_ms
from app.markets.contracts import ContractDefinitionError, ContractSpec, CorporateAction
from app.markets.roll import (AT_LAST_TRADE, SESSIONS_BEFORE_LAST_TRADE, RollDefinitionError, RollRule, active_contract,
                              holding_windows, roll_schedule, series_from_contracts, tradability_refusal)

CHI = "America/Chicago"
A_LAST = local_ms(CHI, 2026, 12, 18, 8, 30)
B_LAST = local_ms(CHI, 2027, 3, 19, 8, 30)
A_LIST = local_ms(CHI, 2026, 6, 1)
B_LIST = local_ms(CHI, 2026, 9, 1)


def spec(**over):
    last = over.pop("last_trade_ms", A_LAST)
    return ContractSpec.from_plain(F.contract_doc("FX1-A", last, listing_ms=A_LIST, **over))


@pytest.fixture
def cal():
    return F.calendar(F.FUTURES_FIXTURE)


@pytest.fixture
def series():
    a = ContractSpec.from_plain(F.contract_doc("FX1-A", A_LAST, listing_ms=A_LIST))
    b = ContractSpec.from_plain(F.contract_doc("FX1-B", B_LAST, listing_ms=B_LIST))
    return series_from_contracts("FX1", "fixture_futures", RollRule(SESSIONS_BEFORE_LAST_TRADE, 5), [b, a])


# ── the specification ──────────────────────────────────────────────────────

def test_tick_value_is_derived_and_prices_and_quantities_follow_the_grid():
    s = spec()
    assert s.tick_value == D("12.50")
    assert s.round_price(D("4500.37")) == D("4500.25")
    assert s.round_price(D("4500.37"), mode="up") == D("4500.50")
    assert s.round_price(D("4500.37"), mode="down") == D("4500.25")
    assert s.price_on_tick(D("4500.75")) and not s.price_on_tick(D("4500.10"))
    assert s.floor_quantity(D("2.9")) == D("2") and s.floor_quantity(D("0.9")) == D("0")   # never rounded up
    assert s.quantity_valid(D("3")) and not s.quantity_valid(D("1.5")) and not s.quantity_valid(D("0"))
    fx = ContractSpec.from_plain(F.contract_doc("PAIR", None, contract_type="SPOT", asset_class="FX", multiplier="1",
                                                tick_size="0.00001", quantity_step="1000", min_quantity="1000",
                                                margin={"kind": "PERCENT_OF_NOTIONAL", "initial": "0.05",
                                                        "maintenance": "0.025"}))
    assert fx.floor_quantity(D("12345")) == D("12000") and fx.floor_quantity(D("999")) == D("0")


def test_margin_rules():
    s = spec()
    assert s.margin.initial_margin(notional=D("450000"), contracts=D("2")) == D("24000")
    assert s.margin.maintenance_margin(notional=D("450000"), contracts=D("-2")) == D("22000")
    pct = spec(margin={"kind": "PERCENT_OF_NOTIONAL", "initial": "0.05", "maintenance": "0.03"})
    assert pct.margin.initial_margin(notional=D("-200000"), contracts=D("1")) == D("10000.00")


@pytest.mark.parametrize("over", [
    {"multiplier": 50.0},                                    # float is refused
    {"tick_size": "0"},
    {"min_quantity": "1.5"},                                 # not a multiple of the step
    {"asset_class": "COMMODITY_PIXIE_DUST"},
    {"verification_status": "TRUST_ME"},
    {"source": ""},
    {"last_trade_ms": None},                                 # a dated future without a last trading instant
    {"contract_type": "PERPETUAL"},                          # a perpetual with an expiry
    {"margin": {"kind": "FIXED_PER_CONTRACT", "initial": "100", "maintenance": "200"}},
    {"carry": {"kind": "OVERNIGHT_FINANCING"}},              # financing without rates
])
def test_invalid_contracts_are_refused(over):
    with pytest.raises(ContractDefinitionError):
        spec(**over)


# ── rolls ──────────────────────────────────────────────────────────────────

def test_the_roll_schedule_is_deterministic_and_counts_sessions(series, cal):
    first = roll_schedule(series, cal)
    assert first == roll_schedule(series, cal)
    assert [(e.from_symbol, e.to_symbol) for e in first] == [("FX1-A", "FX1-B")]
    # five sessions before the session of the last trade (trading date 2026-12-18) is the one closing 12-11
    assert first[0].roll_ms == local_ms(CHI, 2026, 12, 10, 17, 0)


def test_a_holiday_moves_a_session_counted_roll_one_session_earlier(cal):
    c = ContractSpec.from_plain(F.contract_doc("FX1-C", local_ms(CHI, 2026, 12, 29, 8, 30), listing_ms=A_LIST))
    d = ContractSpec.from_plain(F.contract_doc("FX1-D", B_LAST, listing_ms=B_LIST))
    s = series_from_contracts("FX1", "fixture_futures", RollRule(SESSIONS_BEFORE_LAST_TRADE, 5), [c, d])
    # sessions 12-29, 12-28, 12-24, 12-23, 12-22, 12-21 (12-25 is a holiday): roll at the open of 12-21's session
    assert roll_schedule(s, cal)[0].roll_ms == local_ms(CHI, 2026, 12, 20, 17, 0)


def test_the_active_contract_on_a_date(series, cal):
    roll = roll_schedule(series, cal)[0].roll_ms
    assert active_contract(series, local_ms(CHI, 2026, 5, 1), cal) is None             # nothing listed yet
    assert active_contract(series, local_ms(CHI, 2026, 11, 2), cal).symbol == "FX1-A"
    assert active_contract(series, roll - 1, cal).symbol == "FX1-A"
    assert active_contract(series, roll, cal).symbol == "FX1-B"
    assert active_contract(series, B_LAST, cal) is None                                 # after the last roll
    windows = holding_windows(series, cal)
    assert [w[0].symbol for w in windows] == ["FX1-A", "FX1-B"] and windows[0][2] == windows[1][1]


def test_at_last_trade_holds_until_the_last_trading_instant(series, cal):
    s = series_from_contracts("FX1", "fixture_futures", RollRule(AT_LAST_TRADE), list(series.contracts))
    assert roll_schedule(s, cal)[0].roll_ms == A_LAST
    assert active_contract(s, A_LAST - 1, cal).symbol == "FX1-A"


def test_expired_and_unlisted_contracts_are_never_tradable():
    s = spec()
    assert tradability_refusal(s, A_LIST - 1) == "CONTRACT_NOT_LISTED"
    assert tradability_refusal(s, A_LAST) == "CONTRACT_EXPIRED"
    assert tradability_refusal(s, A_LAST + 86_400_000) == "CONTRACT_EXPIRED"
    assert tradability_refusal(s, A_LAST - 1) is None


def test_invalid_series_are_refused():
    a = spec()
    perpetual = ContractSpec.from_plain(F.contract_doc("PERP", None, contract_type="PERPETUAL"))
    with pytest.raises(RollDefinitionError):
        series_from_contracts("FX1", "fixture_futures", RollRule(SESSIONS_BEFORE_LAST_TRADE, 5), [a, perpetual])
    twin = ContractSpec.from_plain(F.contract_doc("FX1-TWIN", A_LAST))
    with pytest.raises(RollDefinitionError):
        series_from_contracts("FX1", "fixture_futures", RollRule(SESSIONS_BEFORE_LAST_TRADE, 5), [a, twin])
    with pytest.raises(RollDefinitionError):
        RollRule("WHEN_IT_FEELS_RIGHT")


# ── economics ──────────────────────────────────────────────────────────────

def test_notional_pnl_and_ticks_in_decimal():
    s = spec()
    assert E.notional(s, D("4500.25"), D("2")) == D("450025.00")
    assert E.pnl(s, entry_price=D("4500"), exit_price=D("4510.25"), quantity=D("2"), side=E.LONG) == D("1025.00")
    assert E.pnl(s, entry_price=D("4500"), exit_price=D("4510.25"), quantity=D("2"), side=E.SHORT) == D("-1025.00")
    assert E.ticks_to_money(s, 4, 2) == D("100.00")
    assert E.convert(D("1025.00"), "0.9") == D("922.500")
    with pytest.raises(TypeError):
        E.notional(s, 4500.25, 2)
    with pytest.raises(ValueError):
        E.pnl(s, entry_price="1", exit_price="2", quantity="1", side=0)


def test_funding_accrual_a_long_pays_a_positive_rate():
    perp = ContractSpec.from_plain(F.contract_doc("PERP", None, contract_type="PERPETUAL", asset_class="CRYPTO",
                                                  multiplier="1", carry={"kind": "FUNDING_RATE"}))
    events = [(D("10000"), D("0.0001")), (D("10000"), D("-0.0002"))]
    assert E.funding_accrual(perp, side=E.LONG, events=events) == D("1.0000")
    assert E.funding_accrual(perp, side=E.SHORT, events=events) == D("-1.0000")
    with pytest.raises(ValueError):
        E.funding_accrual(spec(), side=E.LONG, events=events)                   # a future has no funding


def test_overnight_financing_and_futures_carry():
    fx = ContractSpec.from_plain(F.contract_doc(
        "PAIR", None, contract_type="SPOT", asset_class="FX", multiplier="1", tick_size="0.00001",
        quantity_step="1000", min_quantity="1000",
        margin={"kind": "PERCENT_OF_NOTIONAL", "initial": "0.05", "maintenance": "0.025"},
        carry={"kind": "OVERNIGHT_FINANCING", "long_rate": "0.02", "short_rate": "-0.01", "day_count": 365,
               "triple_weekday": 2}))
    nights = E.financing_nights(date(2026, 10, 19), date(2026, 10, 26), triple_weekday=2)   # Mon -> next Mon
    assert nights == 7                                       # 1 + 1 + 3 (Wed) + 1 + 1 + 0 + 0
    assert E.financing_nights(date(2026, 10, 19), date(2026, 10, 26), triple_weekday=None) == 7
    assert E.financing_nights(date(2026, 10, 19), date(2026, 10, 21), triple_weekday=2) == 2
    long_cost = E.financing_accrual(fx, side=E.LONG, notional_value=D("100000"), nights=nights)
    assert long_cost == -D("100000") * D("0.02") * 7 / 365
    assert E.financing_accrual(fx, side=E.SHORT, notional_value=D("100000"), nights=nights) == D("100000") * D("0.01") * 7 / 365
    assert E.financing_accrual(spec(), side=E.LONG, notional_value=D("450000"), nights=7) == D("0")   # carry is in the price


def test_dividends_and_splits():
    share = ContractSpec.from_plain(F.contract_doc("SHARE", None, contract_type="SPOT", asset_class="EQUITY",
                                                   multiplier="1", tick_size="0.01", carry={"kind": "NONE"}))
    div = CorporateAction(kind="DIVIDEND", effective_ms=0, amount=D("0.75"), currency="USD")
    assert E.dividend_adjustment(share, div, quantity=D("100"), side=E.LONG) == D("75.00")
    assert E.dividend_adjustment(share, div, quantity=D("100"), side=E.SHORT) == D("-75.00")
    with pytest.raises(ValueError):
        E.dividend_adjustment(share, CorporateAction(kind="DIVIDEND", effective_ms=0, amount=D("1"), currency="EUR"),
                              quantity=D("1"), side=E.LONG)
    split = CorporateAction(kind="SPLIT", effective_ms=0, ratio=D("3"))
    qty, price = E.split_adjustment(split, quantity=D("10"), price=D("300"))
    assert (qty, price) == (D("30"), D("100")) and qty * price == D("10") * D("300")
    assert E.back_adjust_price(split, D("300")) == D("100")
