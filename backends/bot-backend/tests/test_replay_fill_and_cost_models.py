"""Phase 13 §13.5–§13.7 — explicit fill semantics and explicit costs.

The rule these pin down is that a replay must never quietly choose the
flattering answer. An entry fills on the bar *after* the signal, not at the
close of the bar that produced it. A bar that touches both the stop and the
target resolves by a named policy that defaults to the pessimistic side, and
the policy travels with the fill.
"""
from __future__ import annotations

import pytest

from app.replay.cost_model import BINANCE_FUTURES_STANDARD, CostModel
from app.replay.fill_models import (
    FillModel,
    IntrabarPolicy,
    IntrabarUnresolved,
    Fill,
    fill_entry,
    fill_limit,
    fill_stop,
    resolve_exit,
)

M1 = 60_000


def bar(open_: float, high: float, low: float, close: float, *, at: int = 0):
    return [at, f"{open_}", f"{high}", f"{low}", f"{close}", "1000",
            at + M1 - 1, "0", 0, "0", "0", "0"]


# ── Entry models (§13.5) ────────────────────────────────────────────────────


def test_an_entry_fills_on_the_bar_after_the_signal_at_its_open():
    signal_bar = bar(100, 105, 99, 104, at=0)          # noqa: F841 - context
    next_bar = bar(104.5, 107, 104, 106, at=M1)

    fill = fill_entry(next_bar, FillModel.NEXT_BAR_OPEN)

    assert fill.filled
    assert fill.price == pytest.approx(104.5)
    assert fill.at_ms == M1
    assert fill.model is FillModel.NEXT_BAR_OPEN


def test_the_market_model_is_named_as_a_modelling_choice():
    next_bar = bar(104.5, 107, 104, 106.5, at=M1)
    fill = fill_entry(next_bar, FillModel.NEXT_BAR_MARKET)

    assert fill.price == pytest.approx((104.5 + 106.5) / 2)
    assert "midpoint" in fill.reason


def test_an_entry_model_must_actually_be_an_entry_model():
    with pytest.raises(ValueError, match="not an entry model"):
        fill_entry(bar(1, 2, 0.5, 1.5), FillModel.BAR_TOUCH_STOP)


# ── Limits and stops ────────────────────────────────────────────────────────


def test_a_limit_fills_only_when_the_bar_trades_through_it():
    assert fill_limit(bar(100, 101, 98, 99), 98.5, "BUY").filled
    assert not fill_limit(bar(100, 101, 99, 99.5), 98.5, "BUY").filled


def test_a_sell_limit_needs_the_high_not_the_low():
    assert fill_limit(bar(100, 103, 99, 102), 102.5, "SELL").filled
    assert not fill_limit(bar(100, 101, 99, 100), 102.5, "SELL").filled


def test_a_stop_fills_at_the_stop_price_with_no_slippage_added():
    """Slippage belongs to the cost model; charging it here double-counts."""
    fill = fill_stop(bar(100, 101, 96, 97), 98.0, "LONG")
    assert fill.filled
    assert fill.price == pytest.approx(98.0)
    assert fill.model is FillModel.BAR_TOUCH_STOP


# ── §13.7 the ambiguous bar ─────────────────────────────────────────────────


AMBIGUOUS = bar(100, 110, 90, 105, at=0)  # touches both a 95 stop and a 108 target


def test_an_unambiguous_bar_resolves_without_the_policy_mattering():
    only_target = bar(100, 110, 99, 108)
    fill = resolve_exit(only_target, side="LONG", stop_price=95.0, target_price=108.0)
    assert fill.price == pytest.approx(108.0)
    assert "only the target" in fill.reason


def test_the_default_for_an_ambiguous_bar_is_the_stop():
    fill = resolve_exit(AMBIGUOUS, side="LONG", stop_price=95.0, target_price=108.0)

    assert fill.price == pytest.approx(95.0)
    assert fill.intrabar_policy is IntrabarPolicy.CONSERVATIVE_STOP_FIRST
    assert "both touched" in fill.reason


def test_the_optimistic_policy_must_be_asked_for_explicitly():
    fill = resolve_exit(
        AMBIGUOUS, side="LONG", stop_price=95.0, target_price=108.0,
        policy=IntrabarPolicy.OPTIMISTIC_TARGET_FIRST,
    )
    assert fill.price == pytest.approx(108.0)
    assert fill.intrabar_policy is IntrabarPolicy.OPTIMISTIC_TARGET_FIRST


def test_every_fill_records_the_policy_that_produced_it():
    for policy in (IntrabarPolicy.CONSERVATIVE_STOP_FIRST,
                   IntrabarPolicy.OPTIMISTIC_TARGET_FIRST):
        fill = resolve_exit(
            AMBIGUOUS, side="LONG", stop_price=95.0, target_price=108.0, policy=policy,
        )
        assert fill.intrabar_policy is policy


def test_reconstruction_takes_whichever_level_the_finer_bars_reach_first():
    fine = [
        bar(100, 102, 99, 101, at=0),        # neither
        bar(101, 109, 100, 108, at=10_000),  # target first
        bar(108, 108, 90, 94, at=20_000),    # stop later
    ]
    fill = resolve_exit(
        AMBIGUOUS, side="LONG", stop_price=95.0, target_price=108.0,
        policy=IntrabarPolicy.LOWER_TIMEFRAME_RECONSTRUCTION,
        lower_timeframe_bars=fine,
    )
    assert fill.price == pytest.approx(108.0)
    assert "the target was reached first" in fill.reason


def test_reconstruction_can_also_prove_the_stop_came_first():
    fine = [
        bar(100, 101, 94, 95, at=0),          # stop first
        bar(95, 109, 95, 108, at=10_000),
    ]
    fill = resolve_exit(
        AMBIGUOUS, side="LONG", stop_price=95.0, target_price=108.0,
        policy=IntrabarPolicy.LOWER_TIMEFRAME_RECONSTRUCTION,
        lower_timeframe_bars=fine,
    )
    assert fill.price == pytest.approx(95.0)
    assert "the stop was reached first" in fill.reason


def test_reconstruction_without_data_refuses_rather_than_guesses():
    with pytest.raises(IntrabarUnresolved, match="no lower-timeframe data"):
        resolve_exit(
            AMBIGUOUS, side="LONG", stop_price=95.0, target_price=108.0,
            policy=IntrabarPolicy.LOWER_TIMEFRAME_RECONSTRUCTION,
        )


def test_reconstruction_refuses_when_the_finer_bars_miss_the_window():
    outside = [bar(100, 101, 99, 100, at=10 * M1)]
    with pytest.raises(IntrabarUnresolved, match="do not cover this bar"):
        resolve_exit(
            AMBIGUOUS, side="LONG", stop_price=95.0, target_price=108.0,
            policy=IntrabarPolicy.LOWER_TIMEFRAME_RECONSTRUCTION,
            lower_timeframe_bars=outside,
        )


def test_a_bar_that_touches_neither_produces_no_fill():
    quiet = bar(100, 101, 99, 100)
    fill = resolve_exit(quiet, side="LONG", stop_price=95.0, target_price=108.0)
    assert fill == Fill.none("neither stop nor target touched")


def test_a_short_resolves_the_mirror_image():
    """For a short the stop is above and the target below."""
    fill = resolve_exit(AMBIGUOUS, side="SHORT", stop_price=108.0, target_price=95.0)
    assert fill.price == pytest.approx(108.0)  # the stop, conservatively


# ── Costs (§13.6) ───────────────────────────────────────────────────────────


def test_an_entry_charges_fee_spread_and_slippage():
    model = CostModel(taker_fee=0.0004, spread=0.0001, slippage=0.0002)
    assert model.entry_cost(10_000) == pytest.approx(10_000 * 0.0007)


def test_the_maker_side_is_cheaper_and_has_to_be_asked_for():
    model = CostModel(maker_fee=0.0002, taker_fee=0.0004, spread=0.0, slippage=0.0)
    assert model.entry_cost(10_000, taker=False) == pytest.approx(2.0)
    assert model.entry_cost(10_000, taker=True) == pytest.approx(4.0)


def test_funding_is_charged_per_completed_interval_not_pro_rata():
    model = CostModel(funding_rate=0.0001, funding_interval_ms=8 * 3600 * 1000)

    assert model.funding_cost(10_000, held_ms=7 * 3600 * 1000) == 0.0
    assert model.funding_cost(10_000, 8 * 3600 * 1000) == pytest.approx(1.0)
    assert model.funding_cost(10_000, 17 * 3600 * 1000) == pytest.approx(2.0)


def test_a_round_trip_is_entry_plus_exit_plus_funding():
    model = CostModel(taker_fee=0.0004, spread=0.0, slippage=0.0, funding_rate=0.0)
    assert model.round_trip_cost(10_000) == pytest.approx(8.0)


def test_the_zero_model_exists_but_says_what_it_is():
    model = CostModel.zero()
    assert model.round_trip_cost(1_000_000, held_ms=10**9) == 0.0
    assert "gross of all costs" in model.notes


def test_the_cost_model_is_hashable_so_a_run_can_name_its_assumptions():
    a = CostModel(taker_fee=0.0004)
    b = CostModel(taker_fee=0.0004)
    c = CostModel(taker_fee=0.0008)

    assert a.model_hash == b.model_hash
    assert a.model_hash != c.model_hash
    assert len(a.model_hash) == 16


def test_unmodelled_costs_are_declared_rather_than_forgotten():
    model = BINANCE_FUTURES_STANDARD
    assert model.models_latency is False
    assert model.models_market_impact is False
    assert model.models_borrow is False
    assert "funding is an average" in model.notes
