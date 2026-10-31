"""Phase 13 §13.6 -- explicit costs in the canonical research cost model (``app.replay.cost_model``).

The cost model is shared by CATI research, certification and economics (it is inside the research
evaluation scope and is not moved). The fill-model half of the former ``test_replay_fill_and_cost_models.py``
was retired in Section I Batch 7 with ``app/replay/fill_models.py``, whose only consumer was the legacy
replay engine. CATI resolves same-bar stop/target ambiguity against the trade in its own replay:
``tests/trading_intelligence/test_section22_replay.py::test_same_bar_tp_and_sl_is_resolved_against_the_trade``.
"""
from __future__ import annotations

import pytest

from app.replay.cost_model import BINANCE_FUTURES_STANDARD, CostModel


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
