"""Section H, Step 2.4: the daily trend rules, the portfolio rule and the simulator, on SYNTHETIC data only.

Every expected number in the accounting tests is worked out by hand in the test, independently of the code
under test. Nothing here reads market data, and nothing here is evidence about the strategy.
"""
from __future__ import annotations

import math

import numpy as np
import pytest

from app.trading_intelligence.families.daily_trend import rules, targets as T
from app.trading_intelligence.families.daily_trend.spec import INTERPRETATION, SPECIFICATION
from app.trading_intelligence.research.evaluator import metrics as M, simulator as S

D0 = 20000                      # first synthetic day index
FEE = SLIP = 0.0005


# ---------------------------------------------------------------------- helpers
def col(values):
    return np.array(values, dtype=np.float64).reshape(-1, 1)


def features(close, *, open_=None, high=None, low=None, strength=1.0, d=0.10, member=True, symbols=None,
             fund_mid=0.0, fund_pos=0.0, fund_neg=0.0, hours=24.0, volume=1e9, step=0.0, min_notional=0.0):
    """Hand-built simulator input: the signal columns are given, not computed, so the books can be checked."""
    close = np.array(close, dtype=np.float64)
    if close.ndim == 1:
        close = close.reshape(-1, 1)
    t, n = close.shape

    def mat(v, dtype=np.float64):
        a = np.array(v, dtype=dtype)
        return np.broadcast_to(a.reshape(-1, 1) if a.ndim == 1 and len(a) == t and n == 1 else a, (t, n)).copy()

    open_ = close.copy() if open_ is None else mat(open_)
    high = np.fmax(open_, close) if high is None else mat(high)
    low = np.fmin(open_, close) if low is None else mat(low)
    return S.Features(
        days=np.arange(D0, D0 + t), symbols=list(symbols or [f"C{j:02d}USDT" for j in range(n)]), open=open_, high=high,
        low=low, close=close, quote_volume=mat(volume), strength=mat(strength), stop_distance=mat(d),
        member=mat(member, dtype=bool), volume=mat(volume), funding_midnight=mat(fund_mid),
        funding_later_positive=mat(fund_pos), funding_later_negative=mat(fund_neg), funding_hours=mat(hours),
        overlay=np.ones((t, n)), in_scope=np.ones(n, dtype=bool), step=np.full(n, step),
        min_notional=np.full(n, min_notional))


def run(feat, level="balanced", policy=T.MANDATE, **kw):
    cfg = S.RunConfig(level=T.risk_level(level, policy), start_day=int(feat.days[0]), end_day=int(feat.days[-1]), **kw)
    return S.simulate(feat, cfg)


class Panel:
    def __init__(self, close, quote_volume=None, high=None, low=None, open_=None, symbols=None):
        close = np.array(close, dtype=np.float64)
        t, n = close.shape
        self.days = np.arange(D0, D0 + t)
        self.symbols = list(symbols or [f"C{j:03d}USDT" for j in range(n)])
        z = np.zeros((t, n))
        self.fields = {"open": close.copy() if open_ is None else np.array(open_, dtype=np.float64),
                       "high": close.copy() if high is None else np.array(high, dtype=np.float64),
                       "low": close.copy() if low is None else np.array(low, dtype=np.float64), "close": close,
                       "quote_volume": np.full((t, n), 1e6) if quote_volume is None else np.array(quote_volume, dtype=np.float64),
                       "funding_midnight": z.copy(), "funding_later_positive": z.copy(),
                       "funding_later_negative": z.copy(), "funding_hours": np.full((t, n), 24.0)}


def random_panel(seed, t=260, n=30):
    rng = np.random.default_rng(seed)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.0005, 0.03, (t, n)), axis=0))
    open_ = np.vstack([close[:1], close[:-1]]) * np.exp(rng.normal(0, 0.005, (t, n)))
    high = np.maximum(open_, close) * np.exp(np.abs(rng.normal(0, 0.01, (t, n))))
    low = np.minimum(open_, close) * np.exp(-np.abs(rng.normal(0, 0.01, (t, n))))
    qv = np.exp(rng.normal(15, 1.5, (1, n))) * np.exp(rng.normal(0, 0.3, (t, n)))
    for j in range(0, n, 7):                                   # late listings
        k = int(rng.integers(20, 120))
        for m in (close, open_, high, low, qv):
            m[:k, j] = np.nan
    for j in range(3, n, 11):                                  # contracts that end
        k = int(rng.integers(180, 250))
        for m in (close, open_, high, low, qv):
            m[k:, j] = np.nan
    p = Panel(close, qv, high, low, open_)
    p.fields["funding_midnight"] = rng.normal(0.0001, 0.0002, (t, n))
    p.fields["funding_later_positive"] = np.abs(rng.normal(0.0001, 0.0002, (t, n)))
    p.fields["funding_later_negative"] = -np.abs(rng.normal(0.00005, 0.0001, (t, n)))
    p.fields["funding_hours"][rng.random((t, n)) < 0.02] = 8.0
    return p


# ============================== RULES ==============================
def test_exit_lookbacks_are_half_rounded_down_with_a_minimum_of_five():
    assert [rules.exit_lookback(L) for L in SPECIFICATION["lookbacks"]] == [5, 10, 15, 22, 32, 50, 75]
    assert rules.exit_lookback(6) == 5


def test_previous_windows_exclude_today_and_need_every_bar():
    x = col([1, 5, 3, 2, 9, 4])
    assert np.array_equal(rules.previous_high(x, 3)[:, 0], [np.nan, np.nan, np.nan, 5, 5, 9], equal_nan=True)
    assert np.array_equal(rules.previous_low(x, 2)[:, 0], [np.nan, np.nan, 1, 3, 2, 2], equal_nan=True)
    x[2, 0] = np.nan                                            # a missing bar poisons every window that holds it
    assert np.array_equal(rules.previous_high(x, 3)[:, 0], [np.nan] * 5 + [np.nan], equal_nan=True)


def test_subsignal_turns_on_at_a_breakout_holds_and_turns_off_below_the_exit_low():
    #        day: 0..9 flat at 10 | 10: 10 (= highest of previous 10 -> ON) | 11..: holds until a close below
    close = [10.0] * 10 + [10.0, 9.9, 9.8, 9.9, 9.7, 9.6]
    sig = rules.subsignal(col(close), 10)[:, 0]
    assert not sig[:10].any()                                   # no window yet: cannot turn on
    assert sig[10]                                              # at (not only above) the previous high
    # exit window is the previous 5 closes: day 11 close 9.9 < min(10 x5) -> OFF
    assert not sig[11]
    rising = col([float(v) for v in range(1, 40)])
    assert rules.subsignal(rising, 10)[:, 0].tolist() == [False] * 10 + [True] * 29
    falling = col([float(v) for v in range(40, 1, -1)])
    assert not rules.subsignal(falling, 10).any()


def test_subsignal_is_unchanged_between_the_two_levels():
    # previous-10 high 10, previous-5 low 9: a close of 9.5 changes nothing in either state
    close = [9.0, 10.0] * 5 + [9.5, 9.5]
    sig = rules.subsignal(col(close), 10)[:, 0]
    assert not sig[10] and not sig[11]
    close = [9.0, 10.0] * 5 + [10.0, 9.5, 9.5, 8.9]
    sig = rules.subsignal(col(close), 10)[:, 0]
    assert sig[10] and sig[11] and sig[12] and not sig[13]      # 8.9 < lowest of the previous five (9.5 9.5 10 10 9)


def test_strength_counts_the_seven_subsignals_and_constant_prices_are_on():
    flat = np.full((200, 1), 50.0)
    s = rules.strength(flat)[:, 0]
    assert s[9] == 0 and s[10] == pytest.approx(1 / 7) and s[30] == pytest.approx(3 / 7) and s[150] == pytest.approx(1.0)
    assert set(np.round(s * 7).astype(int)) <= set(range(8))
    falling = np.linspace(100, 10, 200).reshape(-1, 1)
    assert rules.strength(falling).max() == 0


def test_a_missing_candle_resets_the_signal_and_is_never_filled():
    close = np.array([float(v) for v in range(1, 61)]).reshape(-1, 1)
    assert rules.subsignal(close, 10)[30:, 0].all()
    close[30, 0] = np.nan
    sig = rules.subsignal(close, 10)[:, 0]
    assert not sig[30] and not sig[31:41].any()                 # off, and cannot turn on until 10 real closes exist
    assert sig[41]
    assert rules.consecutive_bars(close)[[29, 30, 31, 59], 0].tolist() == [30, 0, 1, 29]


def test_atr_is_the_simple_mean_of_true_ranges_and_the_stop_distance_is_floored_and_capped():
    high, low, close = col([11, 12, 14, 13]), col([9, 10, 11, 12]), col([10, 11, 13, 12.5])
    tr = rules.true_range(high, low, close)[:, 0]
    assert np.isnan(tr[0]) and tr[1:].tolist() == [2.0, 3.0, 1.0]      # max(h-l, |h-pc|, |l-pc|)
    assert rules.atr(high, low, close, days=3)[:, 0].tolist()[3] == pytest.approx(2.0)
    assert np.isnan(rules.atr(high, low, close, days=3)[2, 0])         # needs days + 1 bars
    n = 40
    c = np.full((n, 3), 100.0)
    h, l = c + np.array([0.5, 5.0, 30.0]), c - np.array([0.5, 5.0, 30.0])
    d = rules.stop_distance(h, l, c)
    assert np.isnan(d[19]).all()                                       # 21 consecutive bars
    assert d[20].tolist() == pytest.approx([0.05, 0.30, 0.40])        # 3 x ATR / close = 3%, 30%, 180%


def test_universe_takes_the_top_twenty_by_previous_median_volume_with_120_days_of_history():
    t, n = 200, 30
    close = np.full((t, n), 10.0)
    qv = np.tile(np.arange(n, 0, -1, dtype=np.float64) * 1e6, (t, 1))   # column 0 is the most liquid
    close[:100, 5] = np.nan                                             # listed on day 100
    qv[:100, 5] = np.nan
    close[150:, 2] = np.nan                                             # ends on day 149
    qv[150:, 2] = np.nan
    scope = np.ones(n, dtype=bool)
    scope[0] = False                                                    # e.g. a stablecoin pair: never a member
    member, volume = rules.universe(close, qv, scope)
    assert not member[:119].any()                                       # nobody has 120 bars yet
    day = member[119]
    assert day.sum() == 20 and not day[0] and not day[5] and day[1] and day[21] and not day[22]
    last = member[199]
    assert last.sum() == 20 and not last[2] and not member[150, 2]      # the ended contract leaves the same day
    assert not last[5] and last[22]                                     # listed on day 100: 120 bars only on day 219
    assert volume[119, 1] == pytest.approx(29e6)
    qv[:, 22] = qv[:, 21]                                               # a tie at the boundary: the lower column wins
    m2, _ = rules.universe(close, qv, scope)
    assert m2[119, 21] and not m2[119, 22]

def test_a_new_listing_joins_only_after_120_consecutive_bars():
    t, n = 260, 25
    close = np.full((t, n), 10.0)
    qv = np.full((t, n), 1e6)
    qv[:, 7] = 1e9                                                      # would rank first from the day it is eligible
    close[:100, 7] = np.nan
    member, _ = rules.universe(close, qv, np.ones(n, dtype=bool))
    assert not member[:219, 7].any() and member[219, 7]                 # rows 100..219 are its first 120 bars


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_rules_never_look_ahead(seed):
    """Change every value after a cut; nothing computed for a day up to the cut may move."""
    p = random_panel(seed)
    meta = {"symbols": {}}
    base = S.prepare(p, meta)
    rng = np.random.default_rng(seed + 100)
    for cut in (60, 150, 222):
        q = random_panel(seed)
        for name in q.fields:
            q.fields[name][cut + 1:] = q.fields[name][cut + 1:] * rng.uniform(0.2, 5.0, q.fields[name][cut + 1:].shape)
        q.fields["close"][cut + 1:, ::3] = np.nan                       # and remove future bars outright
        other = S.prepare(q, meta)
        for name in ("strength", "stop_distance", "member", "volume", "overlay"):
            a, b = getattr(base, name)[:cut + 1], getattr(other, name)[:cut + 1]
            assert np.array_equal(a, b, equal_nan=True), (name, cut)


def test_computing_on_a_truncated_panel_equals_truncating_the_full_computation():
    p = random_panel(9)
    full = S.prepare(p, {"symbols": {}})
    short = Panel(p.fields["close"][:140], p.fields["quote_volume"][:140], p.fields["high"][:140], p.fields["low"][:140],
                  p.fields["open"][:140])
    for name in ("funding_midnight", "funding_later_positive", "funding_later_negative", "funding_hours"):
        short.fields[name] = p.fields[name][:140]
    part = S.prepare(short, {"symbols": {}})
    for name in ("strength", "stop_distance", "member", "overlay"):
        assert np.array_equal(getattr(full, name)[:140], getattr(part, name), equal_nan=True), name


def test_scope_and_relisting_rules():
    meta = {"symbols": {"XAUUSDT": {"contract_type": "TRADIFI_PERPETUAL"}, "BTCUSDT": {"contract_type": "PERPETUAL"},
                        "BTCDOMUSDT": {"contract_type": "PERPETUAL", "underlying_type": "INDEX"}}}
    syms = ["BTCUSDT", "USDCUSDT", "XAUUSDT", "BTCDOMUSDT", "LUNAUSDT", "FDUSDUSDT", "AERGOUSDTSETTLED"]
    assert S.in_scope_mask(syms, meta).tolist() == [True, False, False, True, True, False, True]
    close = np.array([[1.0, 2.0], [1.0, 2.0], [np.nan, 2.0], [1.0, np.nan]])
    f = {k: close.copy() for k in ("open", "high", "low", "close", "quote_volume")}
    removed = S.apply_relisting_rule(["AAAUSDT", "AAAUSDTSETTLED"], f)
    assert removed == 2 and np.isnan(f["close"][:2, 1]).all() and f["close"][2, 1] == 2.0 and f["close"][0, 0] == 1.0


def test_funding_overlay_thresholds():
    daily = np.full((6, 1), 0.30 / 365)                                # exactly 30% a year: not above
    assert rules.funding_overlay_factor(rules.funding_annualised(daily))[2:, 0].tolist() == [1.0] * 4
    f = rules.funding_overlay_factor(np.array([[0.31], [0.60], [0.61], [np.nan], [-0.9]]))
    assert f[:, 0].tolist() == [0.5, 0.5, 0.0, 1.0, 1.0]


# ============================== PORTFOLIO RULE ==============================
def cands(rows):
    return [T.Candidate(s, st, d, v) for s, st, d, v in rows]


def test_risk_levels_are_the_frozen_table_and_the_executable_policy_only_tightens():
    bal, agg = T.risk_level("balanced"), T.risk_level("aggressive")
    assert (bal.risk_per_trade, bal.max_open_risk, bal.max_positions, bal.leverage) == (0.005, 0.02, 6, 2.0)
    assert (agg.daily_pause, agg.halve_drawdown, agg.stop_drawdown) == (0.03, 0.12, 0.25)
    assert bal.max_entry_stop_distance is None
    ex = {n: T.risk_level(n, T.EXECUTABLE) for n in ("conservative", "balanced", "aggressive")}
    assert [ex[n].risk_per_trade for n in ex] == [0.0025, 0.004, 0.004]            # the engine's 0.40% ceiling
    assert [ex[n].approved_risk_per_trade for n in ex] == [0.0025, 0.005, 0.0075]  # what was approved stays visible
    assert all(v.max_entry_stop_distance == 0.15 for v in ex.values())
    with pytest.raises(ValueError):
        T.risk_level("balanced", "LIVE")


def test_target_notional_is_strength_times_risk_times_equity_over_stop_distance():
    d = T.decide_targets(equity=10_000.0, candidates=cands([("A", 1.0, 0.10, 5.0), ("B", 3 / 7, 0.25, 4.0)]), held={},
                         level=T.risk_level("balanced"))
    assert d.targets["A"] == pytest.approx(1.0 * 0.005 * 10_000 / 0.10)             # 500
    assert d.targets["B"] == pytest.approx((3 / 7) * 0.005 * 10_000 / 0.25)         # 85.71
    assert d.open_risk == pytest.approx(500 * 0.10 + (3 / 7) * 50) and d.scale == {"halve": 1.0, "open_risk": 1.0, "leverage": 1.0}
    # the engine's own sizing gives the same raw notional for the executable case
    from app.trading_intelligence.execution.risk_sizing import size_risk_based

    engine = size_risk_based(budget_usdt=10_000, risk_fraction=0.004, stop_distance_fraction=0.10, price=100,
                             leverage=2, leverage_ceiling=2, signal_strength=1)
    ex = T.decide_targets(equity=10_000.0, candidates=cands([("A", 1.0, 0.10, 5.0)]), held={},
                          level=T.risk_level("balanced", T.EXECUTABLE))
    assert float(engine.raw_notional_usdt) == pytest.approx(ex.targets["A"]) == pytest.approx(400.0)


def test_selection_keeps_the_strongest_then_the_most_liquid_then_by_symbol():
    rows = [("E", 1.0, 0.1, 1.0), ("D", 1.0, 0.1, 9.0), ("C", 5 / 7, 0.1, 50.0), ("B", 1.0, 0.1, 9.0), ("A", 0.0, 0.1, 99.0),
            ("F", 1.0, float("nan"), 99.0), ("G", 2 / 7, 0.1, 1.0), ("H", 2 / 7, 0.1, 2.0), ("I", 1 / 7, 0.1, 3.0)]
    d = T.decide_targets(equity=1000.0, candidates=cands(rows), held={"I": 10.0}, level=T.risk_level("conservative"))
    assert list(d.targets) == ["B", "D", "E", "C"]                                  # 4 positions; B before D on symbol
    assert d.qualified == 7                                                         # A (no strength) and F (no stop) are out
    limited = {r["symbol"] for r in d.rejected if r["reason"] == T.REJECT_POSITION_LIMIT}
    assert limited == {"G", "H", "I"}                                               # a held coin has no priority
    orders = T.orders_from_targets(d, held_quantity={"I": 1.0}, close={s: 10.0 for s in "ABCDEFGHI"})
    assert [(o.symbol, o.kind) for o in orders if o.symbol == "I"] == [("I", T.EXIT)]


def test_total_open_risk_scales_every_position_by_the_same_factor():
    rows = [(f"S{i}", 1.0, 0.10 + 0.01 * i, 1.0) for i in range(6)]
    d = T.decide_targets(equity=10_000.0, candidates=cands(rows), held={}, level=T.risk_level("balanced"))
    # six full-strength positions carry 6 x 0.5% = 3% open risk; the Balanced limit is 2% -> everything x 2/3
    assert d.scale["open_risk"] == pytest.approx(2 / 3) and d.open_risk == pytest.approx(200.0)
    raw = [0.005 * 10_000 / (0.10 + 0.01 * i) for i in range(6)]
    assert [d.targets[f"S{i}"] for i in range(6)] == pytest.approx([r * 2 / 3 for r in raw])


def test_the_leverage_ceiling_scales_down_and_cannot_bind_under_the_frozen_numbers():
    import dataclasses

    tight = dataclasses.replace(T.risk_level("balanced"), leverage=0.1)
    d = T.decide_targets(equity=10_000.0, candidates=cands([("A", 1.0, 0.05, 1.0), ("B", 1.0, 0.05, 1.0)]), held={}, level=tight)
    assert d.gross_notional == pytest.approx(1000.0) and d.scale["leverage"] == pytest.approx(0.5)
    # with a 5% floor on d, open risk caps exposure at 1%/5% = 20%, 2%/5% = 40%, 3%/5% = 60% of equity
    for name, most in (("conservative", 0.20), ("balanced", 0.40), ("aggressive", 0.60)):
        lv = T.risk_level(name)
        full = T.decide_targets(equity=1.0, candidates=cands([(f"S{i}", 1.0, 0.05, 1.0) for i in range(20)]), held={}, level=lv)
        assert full.gross_notional == pytest.approx(most) and most < lv.leverage and full.scale["leverage"] == 1.0


def test_brakes_halve_stop_and_pause():
    lv = T.risk_level("balanced")
    rows = cands([("A", 1.0, 0.10, 2.0), ("B", 1.0, 0.10, 1.0)])
    normal = T.decide_targets(equity=10_000.0, candidates=rows, held={}, level=lv)
    halved = T.decide_targets(equity=10_000.0, candidates=rows, held={}, level=lv, halve=True)
    assert halved.targets["A"] == pytest.approx(normal.targets["A"] / 2)
    assert T.decide_targets(equity=10_000.0, candidates=rows, held={"A": 500.0}, level=lv, halted=True).targets == {}
    paused = T.decide_targets(equity=10_000.0, candidates=rows, held={"A": 500.0}, level=lv, paused=True)
    assert list(paused.targets) == ["A"]                                            # the held one is still managed
    assert {"symbol": "B", "reason": T.REJECT_DAILY_PAUSE} in paused.rejected


def test_the_executable_policy_refuses_wide_entries_and_never_adds_to_a_wide_position():
    lv = T.risk_level("balanced", T.EXECUTABLE)
    rows = cands([("NEW", 1.0, 0.151, 2.0), ("OLD", 1.0, 0.30, 1.0), ("OK", 1.0, 0.15, 3.0)])
    d = T.decide_targets(equity=10_000.0, candidates=rows, held={"OLD": 50.0}, level=lv)
    assert "NEW" not in d.targets and d.targets["OK"] == pytest.approx(0.004 * 10_000 / 0.15)   # 15% itself is allowed
    assert d.targets["OLD"] == pytest.approx(50.0)                                  # kept, not increased to 133
    assert [r for r in d.rejected if r["reason"] == T.REJECT_STOP_DISTANCE] == [
        {"symbol": "NEW", "reason": T.REJECT_STOP_DISTANCE}, {"symbol": "OLD", "reason": T.REJECT_STOP_DISTANCE}]
    same = T.decide_targets(equity=10_000.0, candidates=rows, held={"OLD": 50.0}, level=T.risk_level("balanced"))
    assert same.targets["NEW"] == pytest.approx(0.005 * 10_000 / 0.151)             # the mandate itself allows it


def test_orders_respect_the_25_percent_band_and_are_sized_at_the_decision_close():
    d = T.Decision(targets={"A": 1000.0, "B": 1000.0, "C": 1000.0, "N": 300.0}, stop_distance={s: 0.1 for s in "ABCN"}, strength={})
    held = {"A": 7.6, "B": 7.4, "C": 12.6, "X": 3.0}                                # at a close of 100: 760, 740, 1260
    orders = {o.symbol: o for o in T.orders_from_targets(d, held_quantity=held, close={s: 100.0 for s in "ABCNX"})}
    assert "A" not in orders                                                        # |1000 - 760| = 240 <= 250
    assert orders["B"].kind == T.ADJUST and orders["B"].quantity == pytest.approx(10.0 - 7.4)
    assert orders["C"].kind == T.ADJUST and orders["C"].quantity == pytest.approx(10.0 - 12.6)
    assert (orders["N"].kind, orders["N"].quantity) == (T.ENTRY, pytest.approx(3.0))
    assert (orders["X"].kind, orders["X"].quantity) == (T.EXIT, -3.0)


def test_exchange_minimums_round_down_reject_small_orders_and_never_block_an_exit():
    o = T.Order("A", T.ENTRY, 1.2399, 0.0, 0.1)
    assert T.apply_exchange_filters(o, price=10.0, step=0.01, min_notional=5.0) == (pytest.approx(1.23), None)
    assert T.apply_exchange_filters(o, price=10.0, step=2.0, min_notional=5.0) == (0.0, T.REJECT_ZERO_QUANTITY)
    assert T.apply_exchange_filters(o, price=1.0, step=0.01, min_notional=5.0) == (0.0, T.REJECT_MIN_NOTIONAL)
    assert T.apply_exchange_filters(T.Order("A", T.ADJUST, -0.129, 0.0, 0.1), price=10.0, step=0.01,
                                    min_notional=0.0)[0] == pytest.approx(-0.12)
    assert T.apply_exchange_filters(T.Order("A", T.EXIT, -0.0004, 0.0, None), price=1.0, step=0.01, min_notional=5.0) == (-0.0004, None)
    assert T.apply_exchange_filters(o, price=10.0, step=None, min_notional=None) == (1.2399, None)


# ============================== SIMULATOR: THE BOOKS ==============================
def test_entry_at_the_next_open_with_fee_and_slippage_matches_a_hand_calculation():
    close = [100.0, 110.0, 121.0, 121.0]
    open_ = [100.0, 100.0, 110.0, 121.0]
    res = run(features(close, open_=open_, strength=[1.0, 1.0, 0.0, 0.0], d=0.10))
    # day 0 close: target = 1 x 0.5% x 10,000 / 10% = 500 -> 5 contracts at the close of 100
    qty = 0.005 * 10_000 / 0.10 / 100.0
    slip_in = qty * 100.0 * SLIP                                # adverse movement on the amount bought
    fee_in = qty * 100.0 * (1 + SLIP) * FEE                     # fee on the filled value
    e1 = 10_000 - slip_in - fee_in + qty * (110.0 - 100.0)
    e2 = e1 + qty * (121.0 - 110.0)                             # no order on day 1: 550 is within 25% of the new target
    slip_out, fee_out = qty * 121.0 * SLIP, qty * 121.0 * (1 - SLIP) * FEE
    e3 = e2 - slip_out - fee_out                                # strength 0 at the close of day 2 -> sold at the open of day 3
    assert res.daily["equity"].tolist() == pytest.approx([10_000.0, e1, e2, e3], abs=1e-9)
    assert e1 == pytest.approx(10_049.499875) and e3 == pytest.approx(10_103.89502625)
    (trade,) = res.trades
    assert trade["entry_price"] == pytest.approx(100.05) and trade["exit_price"] == pytest.approx(120.9395)
    assert trade["exit_reason"] == S.EXIT_SIGNAL and trade["days_held"] == 2
    assert trade["net_pnl"] == pytest.approx(e3 - 10_000) and trade["pnl_price"] == pytest.approx(qty * 21.0)
    assert res.totals["fees"] == pytest.approx(fee_in + fee_out) and res.totals["slippage"] == pytest.approx(slip_in + slip_out)
    summary = M.summarize(res)
    assert summary["ledger_residual"] == pytest.approx(0.0, abs=1e-9)
    assert summary["gross_return"] == pytest.approx(qty * 21.0 / 10_000) and summary["net_return"] == pytest.approx(e3 / 10_000 - 1)


def test_constant_prices_only_cost_money():
    res = run(features([50.0] * 30, strength=1.0, d=0.05))
    s = M.summarize(res)
    assert res.totals["pnl_price"] == 0.0 and s["net_return"] < 0 and s["gross_return"] == 0.0
    assert res.daily["equity"][-1] == pytest.approx(10_000 - res.totals["fees"] - res.totals["slippage"])
    assert s["sharpe"] is not None and s["sharpe"] < 0


def test_falling_prices_with_no_signal_never_trade_and_undefined_metrics_are_not_numbers():
    res = run(features(np.linspace(100, 40, 30), strength=0.0, d=0.10))
    s = M.summarize(res)
    assert not res.trades and not res.fills and s["net_return"] == 0.0
    assert s["sharpe"] is None and s["sortino"] is None and s["trades"]["hit_rate"] is None
    assert s["trades"]["profit_factor"] is None and s["calmar"] is None


def test_a_stop_fills_at_the_stop_price_less_slippage():
    close = [100.0, 100.0, 100.0, 96.0, 96.0]
    low = [100.0, 100.0, 100.0, 89.0, 96.0]                     # day 3 trades down through the stop
    res = run(features(close, low=low, strength=[1, 1, 1, 0, 0], d=0.10))
    qty = 5.0
    stop = 100.0 * (1 + SLIP) * 0.90                            # entry fill x (1 - d) = 90.045
    (trade,) = res.trades
    assert trade["exit_reason"] == S.EXIT_STOP and trade["exit_price"] == pytest.approx(stop * (1 - SLIP))
    assert trade["pnl_price"] == pytest.approx(qty * (stop - 100.0))        # never the day's close of 96
    cost = qty * 100.0 * SLIP + qty * 100.05 * FEE + qty * stop * SLIP + qty * stop * (1 - SLIP) * FEE
    assert res.daily["equity"][-1] == pytest.approx(10_000 + qty * (stop - 100.0) - cost)
    assert res.events[0]["event"] == S.EXIT_STOP and res.events[0]["stop"] == pytest.approx(90.045)


def test_a_gap_through_the_stop_fills_at_the_open_not_at_the_stop():
    close = [100.0, 100.0, 100.0, 82.0, 82.0]
    open_ = [100.0, 100.0, 100.0, 80.0, 82.0]                   # opens far below the 90.045 stop
    res = run(features(close, open_=open_, strength=[1, 1, 1, 0, 0], d=0.10))
    (trade,) = res.trades
    assert trade["exit_reason"] == S.EXIT_STOP_GAP and trade["exit_price"] == pytest.approx(80.0 * (1 - SLIP))
    assert trade["pnl_price"] == pytest.approx(5.0 * (80.0 - 100.0))        # the worse price, never the stop


def test_a_position_can_be_stopped_on_the_day_it_is_opened_and_reentered_later():
    close = [100.0, 95.0, 95.0, 95.0]
    open_ = [100.0, 100.0, 95.0, 95.0]
    low = [100.0, 85.0, 95.0, 95.0]                             # entered at 100.05 at the open, low 85 the same day
    res = run(features(close, open_=open_, low=low, strength=1.0, d=0.10))
    first = res.trades[0]
    assert first["exit_reason"] == S.EXIT_STOP and first["entry_day"] == first["exit_day"] == D0 + 1
    assert first["exit_price"] == pytest.approx(100.05 * 0.9 * (1 - SLIP))
    assert len(res.trades) == 2 and res.trades[1]["entry_day"] == D0 + 2   # still selected at that close -> back in next open


def test_the_stop_only_ratchets_up():
    close = [100.0, 100.0, 120.0, 110.0, 110.0, 108.0]
    low = [100.0, 100.0, 100.0, 110.0, 110.0, 107.9]
    res = run(features(close, low=low, strength=1.0, d=0.10))
    # stop: 90.045 at entry -> 108 after the close of 120 -> stays 108 after the close of 110 (99 would be lower)
    stops = [e for e in res.events if e["event"] == S.EXIT_STOP]
    assert stops and stops[0]["day"] == D0 + 5 and stops[0]["stop"] == pytest.approx(108.0)


def test_funding_sign_long_pays_a_positive_rate_and_receives_a_negative_one():
    close = [100.0] * 5
    pay = run(features(close, strength=[1, 1, 0, 0, 0], d=0.10, fund_pos=0.001))
    get = run(features(close, strength=[1, 1, 0, 0, 0], d=0.10, fund_neg=-0.001))
    # held through days 1 and 2 (entered at the open of 1, sold at the open of 3): two days of later events on 500 notional
    assert pay.totals["funding"] == pytest.approx(2 * 5.0 * 100.0 * 0.001) and pay.trades[0]["funding"] > 0
    assert get.totals["funding"] == pytest.approx(-2 * 5.0 * 100.0 * 0.001)
    assert get.daily["equity"][-1] - pay.daily["equity"][-1] == pytest.approx(2.0)


def test_the_midnight_event_belongs_to_whoever_held_before_the_days_fills():
    close = [100.0] * 5
    res = run(features(close, strength=[1, 1, 0, 0, 0], d=0.10, fund_mid=0.001))
    # entered at the open of day 1 (after 00:00: nothing), held at 00:00 of days 2 and 3 (sold just after 00:00 on day 3)
    assert res.totals["funding"] == pytest.approx(2 * 5.0 * 100.0 * 0.001)
    assert res.daily["funding"].tolist() == pytest.approx([0.0, 0.0, 0.5, 0.5, 0.0])


def test_on_a_stop_out_day_funding_costs_are_charged_and_funding_income_is_not_credited():
    close = [100.0, 100.0, 96.0, 96.0]
    low = [100.0, 100.0, 89.0, 96.0]
    cost = run(features(close, low=low, strength=[1, 1, 0, 0], d=0.10, fund_pos=[0, 0, 0.002, 0]))
    income = run(features(close, low=low, strength=[1, 1, 0, 0], d=0.10, fund_neg=[0, 0, -0.002, 0]))
    # the notional of a day's events is quantity x that day's open (96)
    assert cost.totals["funding"] == pytest.approx(5.0 * 96.0 * 0.002) and income.totals["funding"] == 0.0


def test_missing_funding_is_charged_never_assumed_zero_and_the_share_is_reported():
    close = [100.0] * 6
    hours = [24.0, 24.0, 0.0, 8.0, 24.0, 24.0]                  # day 2: no record at all; day 3: one of three events
    res = run(features(close, strength=[1, 1, 1, 1, 0, 0], d=0.10, hours=hours))
    rate = INTERPRETATION["missing_funding_rate_per_8h"]
    # day 2: 3 imputed events (00:00 + two later); day 3: 2 imputed later events; on 500 notional each
    assert res.totals["funding"] == pytest.approx(5.0 * 100.0 * rate * 5)
    s = M.summarize(res)
    # events: day 1 two later; days 2, 3, 4 three each; day 5 the 00:00 event before the sale = 12
    assert s["funding_events_imputed"] == 5.0 and s["funding_events"] == 12.0
    assert 0 < s["funding_imputed_share"] < 1


def test_a_held_coin_without_a_bar_is_closed_at_its_last_close_with_the_stress_cost():
    close = [100.0, 100.0, 104.0, np.nan, np.nan]
    res = run(features(close, strength=[1, 1, 1, 0, 0], d=0.10))
    (trade,) = res.trades
    assert trade["exit_reason"] == S.EXIT_DATA_GAP and trade["exit_day"] == D0 + 3
    assert trade["exit_price"] == pytest.approx(104.0 * (1 - 2 * SLIP))            # last real close, twice the base cost
    exit_cost = 5.0 * 104.0 * 2 * SLIP + 5.0 * 104.0 * (1 - 2 * SLIP) * 2 * FEE
    assert trade["fees"] + trade["slippage"] == pytest.approx(5.0 * 100.0 * SLIP + 5.0 * 100.05 * FEE + exit_cost)
    stress = run(features(close, strength=[1, 1, 1, 0, 0], d=0.10), cost_multiple=2.0)
    assert stress.trades[0]["exit_price"] == pytest.approx(104.0 * (1 - 2 * SLIP))  # the same in a stress run, not 4x


def test_an_entry_into_a_coin_with_no_bar_at_the_fill_is_rejected():
    close = [100.0, np.nan, 100.0]
    res = run(features(close, strength=1.0, d=0.10, member=[True, False, False]))
    assert not res.trades and {"day": D0 + 1, "stage": "FILL", "symbol": "C00USDT", "reason": T.REJECT_NO_BAR} in res.rejections


def test_stress_costs_are_exactly_twice_the_base_costs_on_the_same_path():
    close = [100.0] * 6
    base = run(features(close, strength=[1, 1, 0, 0, 0, 0], d=0.10))
    stress = run(features(close, strength=[1, 1, 0, 0, 0, 0], d=0.10), cost_multiple=2.0)
    assert stress.totals["slippage"] == pytest.approx(2 * base.totals["slippage"])
    assert stress.totals["fees"] == pytest.approx(2 * 5.0 * 100.0 * FEE * ((1 + 2 * SLIP) + (1 - 2 * SLIP)))
    assert stress.daily["equity"][-1] < base.daily["equity"][-1] < 10_000


def test_simultaneous_signals_obey_the_position_limit_and_the_risk_cap():
    close = np.full((6, 10), 100.0)
    res = run(features(close, strength=1.0, d=0.10, volume=np.tile(np.arange(10, 0, -1.0), (6, 1))), level="conservative")
    held = {f["symbol"] for f in res.fills if f["kind"] == T.ENTRY}
    assert held == {"C00USDT", "C01USDT", "C02USDT", "C03USDT"}                  # four positions, the most liquid first
    assert res.daily["positions"].max() == 4
    # 4 x 0.25% = 1% open risk exactly at the limit -> 250 notional each, 10% of equity in total
    assert res.daily["gross_notional"][1] == pytest.approx(1000.0, rel=1e-3)
    assert sum(1 for r in res.rejections if r["reason"] == T.REJECT_POSITION_LIMIT) > 0


def test_exchange_minimums_reject_orders_too_small_for_the_account():
    close = [100.0] * 4
    tiny = features(close, strength=1.0, d=0.40, min_notional=200.0, step=0.001)      # target 125 < minimum 200
    res = run(tiny)
    assert not res.trades and all(r["reason"] == T.REJECT_MIN_NOTIONAL for r in res.rejections if r["stage"] == "FILL")
    stepped = run(features(close, strength=1.0, d=0.10, step=2.0))                    # 5 contracts -> 4
    assert stepped.fills[0]["quantity"] == 4.0


def test_the_daily_pause_blocks_new_positions_only_for_that_decision():
    close = np.array([[100.0, 50.0], [100.0, 50.0], [60.0, 50.0], [60.0, 50.0], [60.0, 50.0]])
    low = close.copy()
    strength = np.array([[1, 0], [1, 0], [1, 1], [1, 1], [1, 1]], dtype=float)
    f = features(close, low=low, strength=strength, d=0.40)
    f.stop_distance[:, 0] = 0.05                       # a big position in coin 0: 1000 notional, loses 40% in a day
    f.low[2, 0] = 96.0                                 # (kept above its 95.05 stop so the loss shows at the close)
    f.low[3:, 0] = 60.0
    res = run(f, level="balanced")
    assert res.daily["return"][2] <= -0.02 and res.daily["paused"][2] == 1
    pause = [r for r in res.rejections if r["reason"] == T.REJECT_DAILY_PAUSE]
    assert pause and pause[0]["symbol"] == "C01USDT" and pause[0]["day"] == D0 + 2
    entered = [x["day"] for x in res.fills if x["symbol"] == "C01USDT" and x["kind"] == T.ENTRY]
    assert entered == [D0 + 4]                         # refused at the close of day 2, taken at the close of day 3


def test_the_drawdown_brakes_halve_then_stop_and_the_account_stays_flat():
    # four coins fall together: 100 -> 70 (-6% of equity: halve) -> 30 (beyond -10%: close everything and stop)
    path = np.array([100.0, 100.0, 70.0, 70.0, 30.0, 30.0, 60.0, 90.0, 120.0, 150.0])
    close = np.tile(path.reshape(-1, 1), (1, 4))
    f = features(close, low=np.full(close.shape, 1e3), strength=1.0, d=0.05)       # lows kept above every stop
    with_brakes = run(f, level="conservative")
    without = run(f, level="conservative", drawdown_brakes=False)
    s = M.summarize(with_brakes)
    assert s["brake_days"]["halved"] > 0 and s["drawdown_stop_fired_on"] is not None
    assert with_brakes.daily["drawdown"][2] <= -0.05 and with_brakes.daily["halved"][2] == 1
    assert any(x["kind"] == T.ADJUST and x["quantity"] < 0 and x["day"] == D0 + 3 for x in with_brakes.fills)   # halved
    stop_row = int(np.flatnonzero(with_brakes.daily["halted"])[0])
    assert with_brakes.daily["drawdown"][stop_row] <= -0.10
    assert with_brakes.daily["positions"][stop_row + 1:].max() == 0             # closed at the next open ...
    assert np.ptp(with_brakes.daily["equity"][stop_row + 1:]) == 0              # ... and flat for the rest of the test
    assert sum(1 for t in with_brakes.trades if t["exit_reason"] == S.EXIT_BRAKE) == 4
    assert M.summarize(without)["drawdown_stop_fired_on"] is None and without.daily["halved"].sum() == 0
    assert without.daily["positions"][-1] == 4                                   # without the brake it stays invested
    assert without.daily["equity"][-1] > with_brakes.daily["equity"][-1]         # and here rides the recovery


def test_a_fresh_account_trades_at_its_first_open_from_the_previous_close():
    close = [100.0] * 6
    f = features(close, strength=1.0, d=0.10)
    cfg = S.RunConfig(level=T.risk_level("balanced"), start_day=D0 + 3, end_day=D0 + 5)
    res = S.simulate(f, cfg)
    assert res.fills[0]["day"] == D0 + 3 and res.targets[0]["day"] == D0 + 2     # decided at the close before the start
    assert len(res.daily["equity"]) == 3 and res.days[0] == D0 + 3


def test_the_run_is_deterministic():
    p = random_panel(4)
    feat = S.prepare(p, {"symbols": {}})
    a, b = run(feat), run(S.prepare(random_panel(4), {"symbols": {}}))
    assert np.array_equal(a.daily["equity"], b.daily["equity"]) and a.trades == b.trades and a.targets == b.targets


@pytest.mark.parametrize("seed,level,policy,brakes,cost", [
    (11, "conservative", T.MANDATE, True, 1.0), (12, "balanced", T.MANDATE, False, 1.0),
    (13, "aggressive", T.MANDATE, True, 2.0), (14, "balanced", T.EXECUTABLE, True, 1.0),
    (15, "aggressive", T.EXECUTABLE, False, 2.0)])
def test_financial_invariants_hold_on_random_markets(seed, level, policy, brakes, cost):
    feat = S.prepare(random_panel(seed), {"symbols": {}})
    res = run(feat, level=level, policy=policy, drawdown_brakes=brakes, cost_multiple=cost)
    lv, s, d = res.config.level, M.summarize(res), res.daily
    assert res.trades, "the random market must produce trades for this check to mean anything"
    assert abs(s["ledger_residual"]) < 1e-6                                       # the four ledgers close on equity
    assert sum(t["net_pnl"] for t in res.trades) == pytest.approx(d["equity"][-1] - 10_000, abs=1e-6)
    assert d["positions"].max() <= lv.max_positions and np.all(d["equity"] > 0)
    assert np.allclose(d["equity"][1:] / d["equity"][:-1] - 1, d["return"][1:])
    assert res.totals["fees"] > 0 and res.totals["slippage"] > 0
    for t in res.targets:                                                         # caps hold at every decision
        risk = sum(v["notional"] * v["stop_distance"] for v in t["targets"].values())
        gross = sum(v["notional"] for v in t["targets"].values())
        assert risk <= lv.max_open_risk * t["equity"] * (1 + 1e-9) and gross <= lv.leverage * t["equity"] * (1 + 1e-9)
        assert len(t["targets"]) <= lv.max_positions
    if policy == T.EXECUTABLE:
        assert all(e["stop_distance"] <= 0.15 for e in res.entry_decisions if e["accepted"])
        assert lv.risk_per_trade <= 0.004
    for t in res.trades:                                                          # long only; a stop is never a gain vs entry...
        assert t["max_quantity"] > 0 and t["fees"] >= 0 and t["slippage"] >= 0


@pytest.mark.parametrize("seed", [21, 22])
def test_the_simulation_never_looks_ahead(seed):
    """Run on the full panel and on one whose future was rewritten; everything recorded up to the cut is equal."""
    cut = 170
    base = run(S.prepare(random_panel(seed), {"symbols": {}}))
    q = random_panel(seed)
    rng = np.random.default_rng(seed)
    for name in q.fields:
        q.fields[name][cut + 1:] = q.fields[name][cut + 1:] * rng.uniform(0.3, 3.0, q.fields[name][cut + 1:].shape)
    other = run(S.prepare(q, {"symbols": {}}))
    last = D0 + cut
    assert np.array_equal(base.daily["equity"][:cut + 1], other.daily["equity"][:cut + 1])
    for name in ("fills", "rejections", "events", "entry_decisions", "targets"):
        a = [x for x in getattr(base, name) if x["day"] <= last]
        b = [x for x in getattr(other, name) if x["day"] <= last]
        assert a == b, name
    assert not np.array_equal(base.daily["equity"], other.daily["equity"])        # the future really was different


# ============================== METRICS AND THE PASS RULE ==============================
def test_return_statistics_match_manual_values():
    r = np.array([0.01, -0.02, 0.03, 0.0])
    s = M.return_statistics(r)
    growth = 1.01 * 0.98 * 1.03
    assert s["net_return"] == pytest.approx(growth - 1)
    assert s["annual_volatility"] == pytest.approx(np.std(r, ddof=1) * math.sqrt(365))
    assert s["sharpe"] == pytest.approx(r.mean() / np.std(r, ddof=1) * math.sqrt(365))
    assert s["max_drawdown"] == pytest.approx(0.02)                               # from 1.01 down to 1.01 x 0.98
    assert s["annual_return"] == pytest.approx(growth ** (365 / 4) - 1)
    assert M.return_statistics(np.zeros(5))["sharpe"] is None


def test_calendar_years_compound_from_year_end_to_year_end_and_mark_partial_years():
    from datetime import date

    first = (date(2020, 1, 1) - date(1970, 1, 1)).days
    days = np.arange(first, first + 366 + 365 + 100)                              # 2020 (leap), 2021, part of 2022
    equity = np.linspace(100.0, 300.0, len(days))
    out = M.calendar_years(days, equity, 100.0)
    assert out["2020"]["return"] == pytest.approx(equity[365] / 100.0 - 1) and out["2020"]["complete"]
    assert out["2021"]["return"] == pytest.approx(equity[730] / equity[365] - 1) and out["2021"]["complete"]
    assert out["2022"]["complete"] is False


def _summary(years=None, dd=0.10, ret=0.2, sharpe=1.0):
    cal = {str(y): {"return": v, "complete": True} for y, v in (years or {}).items()}
    return {"calendar_years": cal, "max_drawdown": dd, "net_return": ret, "sharpe": sharpe}


def test_the_pass_rule_needs_all_four_and_anything_else_is_a_fail():
    good = {2020: .1, 2021: .2, 2022: -.05, 2023: .1, 2024: -.02, 2025: .08}
    full = _summary(good)
    ok = M.evaluate_pass_rule(full_as_specified=full, full_without_brake=_summary(dd=0.149),
                              holdout_base=_summary(ret=0.05, sharpe=0.31), holdout_stress=_summary(ret=0.001))
    assert ok["status"] == M.PASS and all(c["status"] == M.PASS for c in ok["criteria"].values())
    cases = {
        "1_holdout_net_return_positive": dict(holdout_stress=_summary(ret=-0.001)),          # stress cost flips it
        "2_positive_calendar_years": dict(full_as_specified=_summary({**good, 2025: -.01})),  # only 3 of 6
        "3_max_drawdown_without_brake": dict(full_without_brake=_summary(dd=0.1501)),
        "4_holdout_net_sharpe": dict(holdout_base=_summary(ret=0.05, sharpe=0.299)),
    }
    for name, change in cases.items():
        kw = dict(full_as_specified=full, full_without_brake=_summary(dd=0.149),
                  holdout_base=_summary(ret=0.05, sharpe=0.31), holdout_stress=_summary(ret=0.001))
        kw.update(change)
        out = M.evaluate_pass_rule(**kw)
        assert out["status"] == M.FAIL and out["criteria"][name]["status"] == M.FAIL, name
        assert [k for k, c in out["criteria"].items() if c["status"] == M.FAIL] == [name]
    none = M.evaluate_pass_rule(full_as_specified=full, full_without_brake=_summary(dd=0.149),
                                holdout_base=_summary(ret=0.05, sharpe=None), holdout_stress=_summary(ret=0.01))
    assert none["criteria"]["4_holdout_net_sharpe"]["status"] == M.FAIL          # an undefined Sharpe is not a pass


def test_before_the_holdout_the_rule_is_pending_unless_development_already_decides_it():
    dev_years = {2020: .1, 2021: .2, 2022: -.05, 2023: .1, 2024: -.02}
    kw = dict(full_as_specified=None, full_without_brake=None, holdout_base=None, holdout_stress=None)
    pending = M.evaluate_pass_rule(**kw, development_as_specified=_summary(dev_years), development_without_brake=_summary(dd=0.12))
    assert pending["status"] == M.PENDING and pending["decided_without_holdout"] is False
    assert pending["criteria"]["2_positive_calendar_years"]["status"] == M.PENDING     # 3 of 5: 2025 decides
    assert pending["criteria"]["3_max_drawdown_without_brake"]["status"] == M.PENDING  # the full period can only be worse
    four = M.evaluate_pass_rule(**kw, development_as_specified=_summary({**dev_years, 2024: .02}),
                                development_without_brake=_summary(dd=0.12))
    assert four["criteria"]["2_positive_calendar_years"]["status"] == M.PASS and four["status"] == M.PENDING
    dd = M.evaluate_pass_rule(**kw, development_as_specified=_summary(dev_years), development_without_brake=_summary(dd=0.16))
    assert dd["status"] == M.FAIL and dd["decided_without_holdout"] is True          # no held-back result can undo it
    years = M.evaluate_pass_rule(**kw, development_as_specified=_summary({2020: .1, 2021: -.2, 2022: -.05, 2023: -.1, 2024: .02}),
                                 development_without_brake=_summary(dd=0.12))
    assert years["status"] == M.FAIL and years["criteria"]["2_positive_calendar_years"]["observed"]["positive"] == 2


def test_benchmarks_are_lagged_costed_and_only_scaled_for_comparison():
    t, n = 140, 3
    close = np.full((t, n), 100.0)
    close[:, 0] = 100.0 * 1.01 ** np.arange(t)                                     # coin 0 rises 1% a day
    p = Panel(close, symbols=["BTCUSDT", "AAAUSDT", "BBBUSDT"])
    p.fields["funding_midnight"][:, 0] = 0.0001
    feat = S.prepare(p, {"symbols": {}})
    b = M.benchmark_returns(feat, D0 + 125, D0 + 139, cost=0.001)
    assert b["btc"][1:] == pytest.approx(0.01 - 0.0001) and b["btc"][0] == pytest.approx(0.01 - 0.0001 - 0.001)
    # equal weight of yesterday's universe (all three from day 119): (1% + 0 + 0)/3 minus a third of BTC's funding
    assert b["equal_weight"][5] == pytest.approx(0.01 / 3 - 0.0001 / 3)
    scaled = M.scaled_to(b["btc"], 0.10)
    assert scaled["scaled"]["annual_volatility"] == pytest.approx(0.10) and scaled["unscaled"]["net_return"] > 0


def test_entry_stop_distance_statistics_count_what_the_engine_would_refuse():
    close = np.full((6, 4), 100.0)
    f = features(close, strength=1.0, d=np.tile([0.05, 0.15, 0.16, 0.40], (6, 1)))
    mandate = run(f, level="balanced")
    st = M.entry_stop_distances(mandate)
    first = [e for e in mandate.entry_decisions if e["day"] == D0]
    assert len(first) == 4 and all(e["accepted"] for e in first)
    assert st["share_beyond_engine_limit"] == pytest.approx(0.5) and st["at_floor_5pct"] > 0 and st["at_cap_40pct"] > 0
    ex = run(f, level="balanced", policy=T.EXECUTABLE)
    refused = {e["symbol"] for e in ex.entry_decisions if not e["accepted"]}
    assert refused == {"C02USDT", "C03USDT"} and {t["symbol"] for t in ex.trades} == {"C00USDT", "C01USDT"}
    assert M.summarize(ex)["rejections"][T.REJECT_STOP_DISTANCE] > 0
