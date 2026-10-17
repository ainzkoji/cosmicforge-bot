"""Steps 6.4 / 8.6: track-record return methodology on hand-computed fixtures (pure; no database).

Every expected number below is worked out by hand in the comment beside it.
"""
from __future__ import annotations

from decimal import Decimal as D

import pytest

from app.golive.track_record import DayFlows, compute_metrics, daily_return, pool_day


def day(date, start, end, dep="0", wd="0", fees="0", funding="0", exposure="0", slippage=None, outage=0,
        suspended=False):
    return DayFlows(date, D(start), D(end), D(dep), D(wd), D(fees), D(funding), D(exposure),
                    None if slippage is None else D(slippage), outage, suspended)


def test_plain_gain_and_loss_days():
    assert daily_return(D("1000"), D("1010"), D(0), D(0)) == D("0.01")       # +10 on 1000
    assert daily_return(D("10000"), D("9500"), D(0), D(0)) == D("-0.05")     # -500 on 10000


def test_large_deposit_day_is_not_a_return():
    # 1010 at the start, 10 000 deposited, nothing gained: 11 010 at the end. Naive (E_end/E_start - 1) = +990 %.
    assert daily_return(D("1010"), D("11010"), D("10000"), D(0)) == 0
    # Same deposit with a +110 trading gain: 110 / (1010 + 10000) = 0.00999091734786557...
    r = daily_return(D("1010"), D("11120"), D("10000"), D(0))
    assert r.quantize(D("1e-20")) == D("0.00999091734786557675")


def test_withdrawal_day_is_not_a_loss():
    # 11 010 at the start, 1 010 withdrawn, nothing lost: 10 000 at the end.
    assert daily_return(D("11010"), D("10000"), D(0), D("1010")) == 0
    # Withdrawal plus a -1 % day: capital 10 000, -100 -> 9 900 at the end.
    assert daily_return(D("11010"), D("9900"), D(0), D("1010")) == D("-0.01")


def test_zero_capital_day_is_excluded_not_divided():
    assert daily_return(D(0), D(0), D(0), D(0)) is None
    m = compute_metrics([day("2026-01-01", "0", "0"), day("2026-01-02", "1000", "1010")])
    assert m.excluded_days == 1 and m.trading_days == 2 and m.twr == D("0.01")


SERIES = [
    day("2026-01-01", "1000", "1010", exposure="2000"),              # r = +0.01        I = 1.01
    day("2026-01-02", "1010", "11010", dep="10000", exposure="0"),   # deposit, r = 0   I = 1.01
    day("2026-01-03", "11010", "10000", wd="1010", outage=30),       # withdrawal, r=0  I = 1.01
    day("2026-01-04", "10000", "9500", fees="20", suspended=True),   # loss, r = -0.05  I = 0.9595
]


def test_chain_linked_twr_drawdown_and_accounting():
    m = compute_metrics(SERIES)
    assert [p["index"] for p in m.index] == [D("1.01"), D("1.01"), D("1.01"), D("0.9595")]
    assert m.twr == D("-0.0405")                                   # 1.01 * 0.95 - 1
    assert m.max_drawdown == 1 - D("0.9595") / D("1.01") == D("0.05")
    # Fee drag: without the 20 of fees day 4 is -480/10000 = -0.048 -> 1.01 * 0.952 = 0.96152; 0.96152 - 0.9595
    assert m.fee_drag == D("0.00202")
    assert m.funding_drag == 0
    # Exposure ratios: 2000/1000 = 2, 0, 0, 0 -> mean 0.5, max 2
    assert m.avg_gross_exposure == D("0.5") and m.max_gross_exposure == D(2)
    assert (m.outage_minutes, m.outage_days, m.suspended_days, m.excluded_days) == (30, 1, 1, 0)
    assert m.slippage_drag is None and m.slippage_measured_days == 0


def test_funding_received_is_negative_drag_and_slippage_over_measured_days_only():
    m = compute_metrics([day("2026-01-01", "1000", "1010", funding="5", slippage="2"),    # r=0.01, ex-funding 0.005
                         day("2026-01-02", "1010", "1010")])                              # r=0, slippage unmeasured
    assert m.funding_drag == D("-0.005")                       # 1.005 - 1.01
    assert m.slippage_drag == D("0.002") and m.slippage_measured_days == 1   # 2 / 1000 on the measured day


def test_correction_replaces_the_day_it_supersedes():
    # The day-4 row is superseded by a correction (ending equity 9600, not 9500): only the active row counts.
    corrected = SERIES[:3] + [day("2026-01-04", "10000", "9600", fees="20", suspended=True)]
    m = compute_metrics(corrected)
    assert m.index[-1]["index"] == D("0.9696")                 # 1.01 * 0.96
    assert m.max_drawdown == D("0.04")


def test_pooled_day_is_asset_weighted():
    # A: 1000 -> 1100 (+10 %), B: 9000 -> 9000 (0 %). Pooled: +100 on 10 000 = +1 %, not the +5 % mean.
    p = pool_day("2026-01-01", [day("2026-01-01", "1000", "1100", slippage="1"), day("2026-01-01", "9000", "9000")])
    assert daily_return(p.starting_equity, p.ending_equity, p.deposits, p.withdrawals) == D("0.01")
    assert p.slippage is None                                  # not measured for every account -> not measured


@pytest.mark.parametrize("order", [SERIES, list(reversed(SERIES))])
def test_order_of_input_does_not_matter(order):
    assert compute_metrics(order).twr == D("-0.0405")
