"""Step 1.1 -- the immutable, versioned risk profiles and their money view."""
import dataclasses
import json
import os
import sys
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "shared"))
from shared_lib import risk_levels as rl  # noqa: E402

D = Decimal

#: The master plan's approved starting values (Section H, Step 1.1).
APPROVED = {
    "conservative": dict(per_trade_risk_pct=D("0.25"), max_open_risk_pct=D("1"), max_positions=4, leverage_ceiling=1,
                         daily_loss_pause_pct=D("1"), drawdown_reduce_pct=D("5"), drawdown_stop_pct=D("10")),
    "balanced": dict(per_trade_risk_pct=D("0.50"), max_open_risk_pct=D("2"), max_positions=6, leverage_ceiling=2,
                     daily_loss_pause_pct=D("2"), drawdown_reduce_pct=D("8"), drawdown_stop_pct=D("15")),
    "aggressive": dict(per_trade_risk_pct=D("0.75"), max_open_risk_pct=D("3"), max_positions=8, leverage_ceiling=3,
                       daily_loss_pause_pct=D("3"), drawdown_reduce_pct=D("12"), drawdown_stop_pct=D("25")),
}


@pytest.mark.parametrize("level", rl.LEVELS)
def test_every_approved_table_value(level):
    profile = rl.get_profile(level)
    for name, value in APPROVED[level].items():
        assert getattr(profile, name) == value, name
    assert profile.version == rl.RISK_PROFILE_VERSION == "2026-10-08.v1"
    assert profile.fraction("per_trade_risk_pct") == APPROVED[level]["per_trade_risk_pct"] / 100


def test_profiles_are_immutable_and_the_table_is_read_only():
    profile = rl.get_profile("balanced")
    with pytest.raises(dataclasses.FrozenInstanceError):
        profile.per_trade_risk_pct = D("5")
    with pytest.raises(TypeError):
        rl.PROFILES["balanced"] = profile
    with pytest.raises(TypeError):
        rl.LEGACY_LEVEL_NAMES["yolo"] = "aggressive"


def test_legacy_names_resolve_for_new_deployments_only():
    assert rl.normalize_level("medium") == "balanced" and rl.normalize_level("LOW") == "conservative"
    assert rl.normalize_level(" High ") == "aggressive"
    with pytest.raises(rl.UnknownRiskLevel):
        rl.normalize_level("yolo")
    with pytest.raises(rl.UnknownRiskLevel):
        rl.get_profile("balanced", version="1999.v0")


def test_decimal_precision_of_the_balanced_example():
    view = rl.money_view("balanced", "1000.00", ceiling_widening_approved=True)
    assert view["risk_per_trade"] == D("5.00") and view["max_open_risk"] == D("20.00")
    assert view["daily_loss_pause"] == D("20.00") and view["drawdown_reduce_threshold"] == D("80.00")
    assert view["drawdown_stop_threshold"] == D("150.00")
    assert view["leverage_ceiling"] == 2 and view["max_positions"] == 6
    assert view["budget"] == {"currency": "USDT", "amount": D("1000.00")}
    # Decimal, never float: 0.1 + 0.2 style drift cannot appear.
    odd = rl.money_view("conservative", "333.33", ceiling_widening_approved=True)
    assert odd["risk_per_trade"] == D("0.83") and str(odd["risk_per_trade"]) == "0.83"   # 0.833325 rounded DOWN


def test_the_system_ceiling_is_applied_until_widening_is_approved():
    balanced = rl.money_view("balanced", "1000")
    assert balanced["per_trade_risk_pct"] == D("0.50") and balanced["effective_per_trade_risk_pct"] == D("0.40")
    assert balanced["ceiling_applied"] is True
    assert balanced["risk_per_trade"] == D("4.00") and balanced["approved_profile_risk_per_trade"] == D("5.00")
    conservative = rl.money_view("conservative", "1000")
    assert conservative["ceiling_applied"] is False and conservative["risk_per_trade"] == D("2.50")
    assert rl.exceeds_system_ceiling(rl.get_profile("aggressive")) and not rl.exceeds_system_ceiling(rl.get_profile("conservative"))
    approved = rl.money_view("aggressive", "1000", ceiling_widening_approved=True)
    assert approved["ceiling_applied"] is False and approved["risk_per_trade"] == D("7.50")


@pytest.mark.parametrize("budget", ["0", 0, "0.00"])
def test_zero_budget_gives_zero_money_and_no_error(budget):
    view = rl.money_view("aggressive", budget)
    assert view["risk_per_trade"] == D("0.00") and view["drawdown_stop_threshold"] == D("0.00")
    with pytest.raises(rl.InvalidBudget):
        rl.validate_budget(budget, allow_zero=False)


@pytest.mark.parametrize("budget", ["-1", -0.01, "nan", "inf", "abc", None, ""])
def test_invalid_budgets_are_rejected(budget):
    with pytest.raises(rl.InvalidBudget):
        rl.money_view("balanced", budget)


def test_very_small_and_very_large_budgets():
    small = rl.money_view("conservative", "0.01", ceiling_widening_approved=True)
    assert small["risk_per_trade"] == D("0.00") and small["max_open_risk"] == D("0.00")
    large = rl.money_view("aggressive", "123456789012.34", ceiling_widening_approved=True)
    assert large["risk_per_trade"] == D("925925917.59")                # exact to the cent, no float overflow
    assert large["drawdown_stop_threshold"] == D("30864197253.08") or large["drawdown_stop_threshold"] == D("30864197253.09")


def test_serialization_round_trip_and_json_rendering():
    profile = rl.get_profile("aggressive")
    data = profile.as_dict()
    assert data["per_trade_risk_pct"] == "0.75" and data["max_positions"] == 8
    assert rl.RiskProfile.from_dict(data) == profile
    assert rl.RiskProfile.from_dict(json.loads(json.dumps(data))) == profile
    rendered = rl.as_json(rl.money_view("balanced", "250", stop_distance_range_pct=("2", "8")))
    assert rendered["risk_per_trade"] == "1.00" and rendered["budget"]["amount"] == "250.00"
    json.dumps(rendered)                                               # fully JSON-serialisable


def test_typical_position_range_needs_an_explicit_stop_assumption():
    view = rl.money_view("balanced", "1000", ceiling_widening_approved=True)
    assert view["typical_position_notional"] is None
    assert view["typical_position_assumption"] == "STOP_DISTANCE_ASSUMPTION_REQUIRED"
    with_stops = rl.money_view("balanced", "1000", stop_distance_range_pct=("2", "8"), ceiling_widening_approved=True)
    # 5 USDT of risk: 62.50 at an 8 % stop, 250.00 at a 2 % stop (the master plan example).
    assert with_stops["typical_position_notional"] == {"min": D("62.50"), "max": D("250.00")}
    with pytest.raises(ValueError):
        rl.money_view("balanced", "1000", stop_distance_range_pct=("8", "2"))


def test_minimum_deployable_budget_never_inflates_the_order():
    # Binance USDⓈ-M minimum notional 5 USDT, widest stop 8 %, balanced at the 0.4 % ceiling:
    # budget * 0.004 / 0.08 >= 5  ->  budget >= 100.
    assert rl.minimum_deployable_budget("balanced", exchange_min_notional="5", widest_stop_distance_pct="8") == D("100.00")
    assert rl.minimum_deployable_budget("balanced", exchange_min_notional="5", widest_stop_distance_pct="8",
                                        ceiling_widening_approved=True) == D("80.00")
    assert rl.minimum_deployable_budget("conservative", exchange_min_notional="5", widest_stop_distance_pct="8") == D("160.00")
    with pytest.raises(ValueError):
        rl.minimum_deployable_budget("balanced", exchange_min_notional="0", widest_stop_distance_pct="8")


def test_invalid_profiles_cannot_be_constructed():
    base = rl.get_profile("balanced").as_dict()
    with pytest.raises(rl.UnknownRiskLevel):
        rl.RiskProfile.from_dict({**base, "level": "yolo"})
    with pytest.raises(ValueError):
        rl.RiskProfile.from_dict({**base, "per_trade_risk_pct": "-0.5"})
    with pytest.raises(ValueError):
        rl.RiskProfile.from_dict({**base, "per_trade_risk_pct": "5"})      # above the combined open risk
    with pytest.raises(ValueError):
        rl.RiskProfile.from_dict({**base, "drawdown_reduce_pct": "15"})    # not below the stop threshold
    with pytest.raises(ValueError):
        rl.RiskProfile.from_dict({**base, "leverage_ceiling": 0})


def test_versioning_keeps_old_deployments_readable():
    assert rl.get_profile("balanced", version=rl.RISK_PROFILE_VERSION) is rl.PROFILES["balanced"]
    assert set(rl.VERSIONS) == {rl.RISK_PROFILE_VERSION}
    # Deterministic: the same inputs always render the same bytes.
    a = json.dumps(rl.as_json(rl.money_view("aggressive", "999.99")), sort_keys=True)
    b = json.dumps(rl.as_json(rl.money_view("aggressive", "999.99")), sort_keys=True)
    assert a == b
