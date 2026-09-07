"""Phase 3 — the user's capital and allocation must survive the whole chain.

The audited defect was that ``total_capital_budget`` was accepted at the API
boundary and then dropped before ``CreateBotInstanceRequest``, leaving NULL in
the database and letting the runtime substitute a fabricated 10,000 budget.

This test walks the canonical scenario from the blueprint —

    total_capital_budget      = 500
    trade_amount_per_position = 50
    allocation_type           = fixed_amount
    risk_level                = balanced
    symbols                   = BTCUSDT, ETHUSDT
    timeframe                 = 15m
    execution mode            = paper

— through every hop and asserts the numbers arrive unchanged.
"""
from __future__ import annotations

import inspect

import pytest

from app.api.auto_pilot import AutoPilotAllocation, DeployAutoPilotRequest
from app.core.bot_instance_service import BotInstanceService
from app.models.bot_instance_models import BotInstance, CreateBotInstanceRequest
from app.risk.system_limits import SystemLimits
from app.runner.bot_context import BotRunContext
from app.runner.effective_policy import EffectivePolicyError, resolve_effective_bot_policy

TOTAL_CAPITAL = 500.0
PER_POSITION = 50.0
SYMBOLS = ["BTCUSDT", "ETHUSDT"]


# ── Hop 1: the API request model ─────────────────────────────────────────────


def test_api_request_preserves_both_capital_fields():
    request = DeployAutoPilotRequest(
        broker_account_ids=["acct-1"],
        risk_mode="medium",
        execution_mode="paper",
        allocation=AutoPilotAllocation(
            total_capital_budget=TOTAL_CAPITAL,
            trade_amount_per_position=PER_POSITION,
            allocation_type="fixed_amount",
        ),
    )
    assert request.allocation.total_capital_budget == TOTAL_CAPITAL
    assert request.allocation.trade_amount_per_position == PER_POSITION
    assert request.allocation.allocation_type == "fixed_amount"


def test_api_rejects_per_position_larger_than_the_total_budget():
    with pytest.raises(ValueError):
        AutoPilotAllocation(
            total_capital_budget=100.0,
            trade_amount_per_position=500.0,
            allocation_type="fixed_amount",
        )


def test_api_preserves_a_selected_percentage_allocation_type():
    """percent_balance must not be forced back to fixed_amount."""
    allocation = AutoPilotAllocation(
        total_capital_budget=TOTAL_CAPITAL,
        trade_amount_per_position=10.0,
        allocation_type="percent_balance",
    )
    assert allocation.allocation_type == "percent_balance"
    assert allocation.trade_amount_per_position == 10.0


def test_api_rejects_a_percentage_outside_0_to_100():
    with pytest.raises(ValueError):
        AutoPilotAllocation(
            total_capital_budget=TOTAL_CAPITAL,
            trade_amount_per_position=150.0,
            allocation_type="percent_balance",
        )


# ── Hop 2: the deployment service passes capital into the create request ─────


def test_deploy_auto_pilot_accepts_and_forwards_the_total_capital_budget():
    signature = inspect.signature(BotInstanceService.deploy_auto_pilot)
    assert "capital_allocation" in signature.parameters
    assert "allocation_value" in signature.parameters
    assert "allocation_type" in signature.parameters

    source = inspect.getsource(BotInstanceService.deploy_auto_pilot)
    # The regression was capital_allocation never reaching CreateBotInstanceRequest.
    assert "CreateBotInstanceRequest(" in source
    assert "capital_allocation=capital_allocation" in source
    assert "allocation_value=allocation_value" in source
    assert "allocation_type=allocation_type" in source


# ── Hop 3: CreateBotInstanceRequest → BotInstance ────────────────────────────


def make_create_request() -> CreateBotInstanceRequest:
    return CreateBotInstanceRequest(
        user_id="user-1",
        broker_account_id="acct-1",
        market_type="CRYPTO",
        strategy_id="master_ensemble",
        strategy_version="1.0.0",
        risk_level="balanced",
        symbols=SYMBOLS,
        timeframes=["15m"],
        allocation_type="fixed_amount",
        allocation_value=PER_POSITION,
        mode="paper",
        capital_allocation=TOTAL_CAPITAL,
        capital_allocation_type="fixed_amount",
    )


def test_create_request_carries_both_capital_fields():
    request = make_create_request()
    assert request.capital_allocation == TOTAL_CAPITAL
    assert request.allocation_value == PER_POSITION


def make_instance(**overrides) -> BotInstance:
    defaults = dict(
        id="bot-1",
        user_id="user-1",
        broker_account_id="acct-1",
        market_type="CRYPTO",
        strategy_id="master_ensemble",
        strategy_version="1.0.0",
        risk_level="balanced",
        symbols=SYMBOLS,
        timeframes=["15m"],
        allocation_type="fixed_amount",
        allocation_value=PER_POSITION,
        mode="paper",
        capital_allocation=TOTAL_CAPITAL,
        capital_allocation_type="fixed_amount",
    )
    defaults.update(overrides)
    return BotInstance(**defaults)


def policy_for(instance: BotInstance):
    return resolve_effective_bot_policy(
        instance=instance,
        broker_environment="demo",
        risk_params=BotInstanceService.get_risk_profile_preset(instance.risk_level),
    )


# ── Hops 4–6: BotInstance → EffectiveBotPolicy → BotRunContext ───────────────


def test_capital_survives_instance_to_policy_to_context():
    instance = make_instance()
    policy = policy_for(instance)
    context = BotRunContext.from_effective_policy(policy, {"api_key": "k", "api_secret": "s"})

    assert instance.capital_allocation == TOTAL_CAPITAL
    assert policy.capital_budget == TOTAL_CAPITAL
    assert context.capital_budget == TOTAL_CAPITAL

    assert instance.allocation_value == PER_POSITION
    assert policy.position_allocation_value == PER_POSITION
    assert context.allocation_value == PER_POSITION
    assert context.trade_usdt_per_order == PER_POSITION


def test_context_reports_the_configured_fixed_trade_amount_to_the_executor():
    context = BotRunContext.from_effective_policy(
        policy_for(make_instance()), {"api_key": "k", "api_secret": "s"}
    )
    mode, value = context.get_trade_amount_settings()
    assert mode == "fixed"
    assert value == PER_POSITION


def test_percentage_allocation_resolves_against_the_capital_budget():
    """10% of a 500 budget is 50 USDT — and the percentage stays a percentage."""
    policy = policy_for(make_instance(allocation_type="percent_balance", allocation_value=10.0))
    context = BotRunContext.from_effective_policy(policy, {"api_key": "k", "api_secret": "s"})

    assert policy.position_allocation_type == "percent_balance"
    assert policy.position_allocation_value == 10.0
    assert context.trade_usdt_per_order == pytest.approx(50.0)

    mode, value = context.get_trade_amount_settings()
    assert mode == "percent"
    assert value == 10.0


def test_execution_mode_and_symbols_survive_to_the_context():
    context = BotRunContext.from_effective_policy(
        policy_for(make_instance()), {"api_key": "k", "api_secret": "s"}
    )
    assert context.execution_mode == "paper"
    assert context.symbols == SYMBOLS
    assert context.interval == "15m"


# ── The fabricated 10,000 budget is gone ─────────────────────────────────────


def test_no_fabricated_10000_capital_anywhere_in_the_chain():
    instance = make_instance()
    policy = policy_for(instance)
    context = BotRunContext.from_effective_policy(policy, {"api_key": "k", "api_secret": "s"})

    for value in (instance.capital_allocation, policy.capital_budget, context.capital_budget):
        assert value == TOTAL_CAPITAL
        assert value != 10000.0


def test_legacy_null_capital_is_reported_not_repaired():
    """A legacy row with NULL capital must be surfaced, never given 10,000."""
    with pytest.raises(EffectivePolicyError) as exc:
        policy_for(make_instance(capital_allocation=None))
    assert exc.value.reason_code == "CAPITAL_BUDGET_REQUIRED"


def test_runtime_path_contains_no_capital_fallback_literal():
    """Guard against the `instance.capital_allocation or 10000` pattern returning."""
    import app.runner.bot_context as bot_context
    import app.runner.effective_policy as effective_policy

    for module in (bot_context, effective_policy):
        source = inspect.getsource(module)
        assert "or 10000" not in source
        assert "or 10_000" not in source


# ── Risk contract ordering and ceilings ──────────────────────────────────────


def test_risk_profiles_are_ordered_and_within_the_system_ceiling():
    ceiling = SystemLimits().max_risk_per_trade_ceiling
    conservative, balanced, aggressive = (
        BotInstanceService.get_risk_profile_preset(level)["per_trade_risk_pct"]
        for level in ("conservative", "balanced", "aggressive")
    )
    assert conservative < balanced < aggressive <= ceiling


def test_each_profile_resolves_to_its_requested_risk_without_clamping():
    for level in ("conservative", "balanced", "aggressive"):
        policy = policy_for(make_instance(risk_level=level))
        requested = BotInstanceService.get_risk_profile_preset(level)["per_trade_risk_pct"]
        assert policy.risk_per_trade == requested
        assert policy.requested_risk_per_trade == requested


def test_limits_resolve_to_a_single_effective_value():
    """No hidden 3-vs-20-vs-6 ambiguity: one number, plus what was asked for."""
    policy = policy_for(make_instance())
    limits = SystemLimits()

    assert isinstance(policy.max_daily_trades, int)
    assert isinstance(policy.max_open_positions, int)
    assert 0 < policy.max_daily_trades <= limits.max_trades_per_day
    assert 0 < policy.max_open_positions <= limits.max_open_positions
    assert policy.requested_max_daily_trades >= policy.max_daily_trades
    assert policy.requested_max_open_positions >= policy.max_open_positions


def test_daily_loss_limit_is_derived_from_the_configured_capital():
    limits = SystemLimits()
    policy = policy_for(make_instance())
    assert 0 < policy.max_daily_loss <= limits.max_daily_loss_pct * TOTAL_CAPITAL


def test_a_larger_capital_budget_scales_the_daily_loss_limit():
    small = policy_for(make_instance(capital_allocation=500.0, allocation_value=50.0))
    large = policy_for(make_instance(capital_allocation=5000.0, allocation_value=50.0))
    assert large.max_daily_loss > small.max_daily_loss
