"""Section K1 -- the account budget the portfolio path sizes from.

``budget = fixed amount OR percentage x account equity``. A ``percent_balance`` deployment stores the percentage;
the portfolio runtime used to pass that number on as if it were USDT (20 % of a 5,000 USDT account sized as a
20 USDT budget, with a 20 x leverage notional cap). It is now resolved against the broker-confirmed equity with
the shared rule (``risk_sizing.effective_budget``) before anything is sized.
"""
from types import SimpleNamespace

import pytest

import _step3_execution as E
from test_step3_connected_portfolio import setup
from app.trading_intelligence.execution.risk_sizing import effective_budget
from app.trading_intelligence.integration import portfolio_runtime as R
from app.trading_intelligence.pipeline.portfolio import AccountLimits


@pytest.fixture
def world(tmp_path_factory, tmp_path):
    return E.make_world(tmp_path_factory, tmp_path)


def _boundary(spec, *, budget=None, cap=None, leverage=3.0):
    return SimpleNamespace(capital_budget_spec=spec, capital_budget=budget,
                           effective_policy=SimpleNamespace(max_leverage=leverage),
                           limits=AccountLimits.from_profile("balanced", max_position_notional=cap))


def test_a_percentage_budget_is_that_share_of_the_equity_and_caps_the_notional_like_a_fixed_one():
    boundary = _boundary(("percent_balance", 20.0), cap=None)
    assert R.resolve_capital_budget(boundary, 5000.0) == pytest.approx(1000.0)
    assert boundary.capital_budget == pytest.approx(1000.0)
    assert boundary.limits.max_position_notional == pytest.approx(3000.0)        # budget x leverage
    # The same number the deployment preview shows for this request.
    assert float(effective_budget(budget_type="percent_balance", budget_value="20", account_equity="5000")) == 1000.0


def test_a_tighter_customer_maximum_position_is_kept():
    boundary = _boundary(("percent_balance", 50.0), cap=200.0)
    R.resolve_capital_budget(boundary, 4000.0)
    assert boundary.limits.max_position_notional == pytest.approx(200.0)


def test_a_fixed_budget_is_unchanged_and_needs_no_equity():
    boundary = _boundary(None, budget=750.0, cap=2250.0)
    assert R.resolve_capital_budget(boundary, None) == 750.0
    assert boundary.limits.max_position_notional == 2250.0


@pytest.mark.parametrize("equity", [0.0, -10.0, None, float("nan")])
def test_an_equity_that_cannot_carry_the_budget_refuses_instead_of_guessing(equity):
    with pytest.raises(ValueError, match="ACCOUNT_CAPITAL_BUDGET_UNRESOLVED"):
        R.resolve_capital_budget(_boundary(("percent_balance", 20.0)), equity)


def test_the_runtime_sizes_a_percentage_bot_from_the_broker_equity(world, monkeypatch):
    runtime, account, stage = setup(world, monkeypatch)
    real = runtime.make_boundary

    def percent_boundary(*args):
        boundary = real(*args)
        boundary.capital_budget, boundary.capital_budget_spec = None, ("percent_balance", 10.0)
        return boundary
    runtime.make_boundary = percent_boundary
    out = runtime.process(account, None, legacy_maintenance=lambda: None)
    equity = stage.call_args.args[1].equity
    assert out["capital_budget"] == pytest.approx(equity * 0.10)
    assert stage.call_args.kwargs["capital_budget"] == pytest.approx(equity * 0.10)   # not 10 "USDT"
    assert out["families"][0]["execution"]["status"]                                # it went on to execute
