"""Phase 1 — EffectiveBotPolicy is the single runtime source of truth.

These tests pin the resolution contract:

* SystemLimits are absolute ceilings that a request can never exceed.
* Clamping is recorded, never silent.
* The policy hash is deterministic over material configuration and stable
  across volatile fields.
* Impossible configuration fails closed instead of being repaired with
  invented values.
"""
from __future__ import annotations

import pytest

from app.core.bot_instance_service import BotInstanceService
from app.models.bot_instance_models import BotInstance
from app.risk.system_limits import SystemLimits
from app.runner.effective_policy import (
    EffectivePolicyError,
    normalize_execution_mode,
    resolve_effective_bot_policy,
)


def make_instance(**overrides) -> BotInstance:
    defaults = dict(
        id="bot-1",
        user_id="user-1",
        broker_account_id="acct-1",
        market_type="CRYPTO",
        strategy_id="master_ensemble",
        strategy_version="1.0.0",
        risk_level="balanced",
        symbols=["BTCUSDT", "ETHUSDT"],
        timeframes=["15m"],
        allocation_type="fixed_amount",
        allocation_value=50.0,
        mode="paper",
        capital_allocation=500.0,
        capital_allocation_type="fixed_amount",
    )
    defaults.update(overrides)
    return BotInstance(**defaults)


def resolve(instance: BotInstance | None = None, risk_level: str | None = None, **kwargs):
    instance = instance or make_instance()
    params = BotInstanceService.get_risk_profile_preset(risk_level or instance.risk_level)
    return resolve_effective_bot_policy(
        instance=instance,
        broker_environment=kwargs.pop("broker_environment", "demo"),
        risk_params=kwargs.pop("risk_params", params),
        **kwargs,
    )


# ── Execution mode vs broker environment ─────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("paper", "paper"),
        ("sim", "paper"),
        ("simulation", "paper"),
        ("live", "broker"),
        ("broker", "broker"),
        ("testnet", "broker"),
        ("demo", "broker"),
        (None, "paper"),
    ],
)
def test_execution_mode_normalizes_to_paper_or_broker(raw, expected):
    assert normalize_execution_mode(raw) == expected


def test_unsupported_execution_mode_fails_closed():
    with pytest.raises(EffectivePolicyError) as exc:
        normalize_execution_mode("yolo")
    assert exc.value.reason_code == "INVALID_EXECUTION_MODE"


def test_legacy_live_mode_on_demo_is_broker_not_paper():
    """A DB row saying mode='live' against a demo account is broker/demo.

    It must never be reported as paper — that is the ambiguity Phase 2 removes.
    """
    policy = resolve(make_instance(mode="live"), broker_environment="demo")
    assert policy.execution_mode == "broker"
    assert policy.broker_environment == "demo"


# ── SystemLimits are absolute ceilings ───────────────────────────────────────


def test_risk_per_trade_never_exceeds_system_ceiling():
    limits = SystemLimits()
    policy = resolve(
        risk_params={**BotInstanceService.get_risk_profile_preset("balanced"), "per_trade_risk_pct": 0.5}
    )
    assert policy.requested_risk_per_trade == 0.5
    assert policy.risk_per_trade == limits.max_risk_per_trade_ceiling
    assert policy.risk_per_trade <= limits.max_risk_per_trade_ceiling


def test_leverage_never_exceeds_asset_class_ceiling():
    limits = SystemLimits()
    policy = resolve(
        risk_params={**BotInstanceService.get_risk_profile_preset("balanced"), "max_leverage": 125.0}
    )
    assert policy.requested_max_leverage == 125.0
    assert policy.max_leverage == limits.max_leverage_major
    assert policy.max_leverage_ceiling == limits.max_leverage_major


def test_alt_symbols_get_the_lower_leverage_ceiling():
    limits = SystemLimits()
    policy = resolve(
        make_instance(symbols=["SOLUSDT"]),
        risk_params={**BotInstanceService.get_risk_profile_preset("balanced"), "max_leverage": 125.0},
    )
    assert policy.max_leverage == limits.max_leverage_alt
    assert limits.max_leverage_alt < limits.max_leverage_major


# ── Clamping is recorded, never silent ───────────────────────────────────────


def test_clamping_records_requested_effective_ceiling_and_reason():
    policy = resolve(
        risk_params={
            **BotInstanceService.get_risk_profile_preset("balanced"),
            "per_trade_risk_pct": 0.5,
            "max_leverage": 125.0,
        }
    )
    settings_clamped = {c["setting"]: c for c in policy.clamps}
    assert "risk_per_trade" in settings_clamped
    assert "max_leverage" in settings_clamped

    risk_clamp = settings_clamped["risk_per_trade"]
    assert risk_clamp["requested_value"] == 0.5
    assert risk_clamp["effective_value"] == policy.risk_per_trade
    assert risk_clamp["hard_ceiling"] == policy.risk_per_trade_ceiling
    assert risk_clamp["clamp_reason"]
    assert policy.clamp_warnings  # human-readable form is emitted too


def test_routine_profiles_do_not_require_clamping():
    """Presets are defined inside the ceilings, so normal deploys stay unclamped."""
    for level in ("conservative", "balanced", "aggressive"):
        policy = resolve(make_instance(risk_level=level))
        risk_clamps = [c for c in policy.clamps if c["setting"] == "risk_per_trade"]
        assert risk_clamps == [], f"{level} profile should not need clamping"


# ── Policy hash determinism ──────────────────────────────────────────────────


def test_policy_hash_is_deterministic_across_resolutions():
    first = resolve()
    second = resolve()
    assert first.policy_hash == second.policy_hash
    assert first.resolved_at != second.resolved_at or True  # resolved_at is excluded


def test_policy_hash_excludes_volatile_fields():
    policy = resolve()
    payload = policy.runtime_payload()
    for volatile in ("resolved_at", "policy_hash", "clamp_warnings", "clamps"):
        assert volatile not in payload


@pytest.mark.parametrize(
    "overrides",
    [
        {"symbols": ["BTCUSDT"]},
        {"timeframes": ["1h"]},
        {"capital_allocation": 1000.0},
        {"allocation_value": 100.0},
        {"allocation_type": "percent_balance", "allocation_value": 10.0},
        {"risk_level": "aggressive"},
        {"mode": "live"},
    ],
)
def test_material_configuration_change_produces_a_new_hash(overrides):
    baseline = resolve()
    changed = resolve(make_instance(**overrides))
    assert changed.policy_hash != baseline.policy_hash


def test_broker_environment_change_produces_a_new_hash():
    baseline = resolve(broker_environment="demo")
    changed = resolve(broker_environment="mainnet")
    assert changed.policy_hash != baseline.policy_hash


# ── Fail closed on impossible configuration ──────────────────────────────────


def test_missing_capital_budget_fails_closed_and_does_not_invent_10000():
    with pytest.raises(EffectivePolicyError) as exc:
        resolve(make_instance(capital_allocation=None))
    assert exc.value.reason_code == "CAPITAL_BUDGET_REQUIRED"
    assert "10000" not in str(exc.value)


def test_zero_capital_budget_fails_closed():
    with pytest.raises(EffectivePolicyError) as exc:
        resolve(make_instance(capital_allocation=0.0))
    assert exc.value.reason_code == "CAPITAL_BUDGET_REQUIRED"


def test_position_allocation_above_capital_budget_fails_closed():
    with pytest.raises(EffectivePolicyError) as exc:
        resolve(make_instance(capital_allocation=100.0, allocation_value=500.0))
    assert exc.value.reason_code == "POSITION_ALLOCATION_EXCEEDS_CAPITAL"


def test_invalid_allocation_type_fails_closed():
    with pytest.raises(EffectivePolicyError) as exc:
        resolve(make_instance(allocation_type="vibes"))
    assert exc.value.reason_code == "INVALID_POSITION_ALLOCATION"


def test_empty_symbols_fails_closed():
    with pytest.raises(EffectivePolicyError) as exc:
        resolve(make_instance(symbols=[]))
    assert exc.value.reason_code == "MISSING_SYMBOLS"


def test_unsupported_timeframe_fails_closed():
    with pytest.raises(EffectivePolicyError) as exc:
        resolve(make_instance(timeframes=["13s"]))
    assert exc.value.reason_code == "INVALID_TIMEFRAME"


def test_non_positive_resolved_limit_fails_closed():
    with pytest.raises(EffectivePolicyError) as exc:
        resolve(
            risk_params={
                **BotInstanceService.get_risk_profile_preset("balanced"),
                "max_position_slots": 0,
            }
        )
    assert exc.value.reason_code == "NON_POSITIVE_LIMIT"


# ── The policy carries the values the runtime actually uses ──────────────────


def test_policy_carries_the_configured_capital_and_allocation():
    policy = resolve(make_instance(capital_allocation=500.0, allocation_value=50.0))
    assert policy.capital_budget == 500.0
    assert policy.position_allocation_value == 50.0
    assert policy.position_allocation_type == "fixed_amount"


def test_percentage_allocation_is_preserved_as_a_percentage():
    """percent_balance must not be silently converted into fixed USDT."""
    policy = resolve(make_instance(allocation_type="percent_balance", allocation_value=10.0))
    assert policy.position_allocation_type == "percent_balance"
    assert policy.position_allocation_value == 10.0


def test_limits_resolve_once_with_requested_and_effective_visible():
    policy = resolve()
    assert policy.requested_max_daily_trades >= policy.max_daily_trades
    assert policy.requested_max_open_positions >= policy.max_open_positions
    assert policy.max_daily_trades > 0
    assert policy.max_open_positions > 0
    assert policy.max_daily_loss > 0
