"""CATI production execution policy: user margin, leverage, structural stop,
live entry economics and the per-bot daily loss authority.

Historical natural decisions (ADA, PORTAL) are fixtures only; nothing here
replays a decision against a broker.
"""
from types import SimpleNamespace

import pytest

from app.risk.adaptive_daily_budget import AdaptiveDailyRiskBudgetEngine, AdaptiveDailyRiskInputs, AdaptiveDailyRiskPolicy
from app.risk.system_limits import SystemLimits, validate_daily_loss_limit_pct
from app.runner.effective_policy import EffectivePolicyError, resolve_effective_bot_policy
from app.trading_intelligence.execution.entry_economics import depth_vwap, evaluate_entry_economics
from app.trading_intelligence.execution.leverage import resolve_cati_leverage

LIMITS = SystemLimits()
RATES = dict(fee=0.0004, half_spread=0.0001, slippage=0.0002, funding_buffer=0.0001 * 6)
#: Natural ADAUSDT LONG, decision b36993a5 (2026-10-05T20:59:59Z), next-open reference.
ADA = dict(side="LONG", reference=0.2691, stop=0.2492285714285714, target=0.31842857142857156)
#: Natural PORTALUSDT SHORT, decision a0f1df0d (2026-10-05T21:59:59Z), score -2.52.
PORTAL = dict(side="SHORT", reference=0.01809, stop=0.02172285714285714, target=0.00900785714285714)


def economics(case, price, **kw):
    return evaluate_entry_economics(price=price, min_reward_risk=1.8, min_stop_fraction=LIMITS.min_stop_loss_pct,
                                    max_stop_fraction=LIMITS.max_stop_loss_pct, **{**case, **RATES, **kw})


def leverage(stop, *, user_max=10., asset=7., compound=0.225, slip=0.0002):
    return resolve_cati_leverage(stop_fraction=stop, user_max=user_max, asset_class_max=asset,
                                 compound_risk_limit=compound, slippage_fraction=slip)


# A/B/G -- a 7% structural stop executes at a lower legal leverage, never at a smaller margin.
def test_seven_percent_stop_resolves_three_x_under_balanced_profile():
    r = leverage(0.07)
    assert r.leverage == 3 and r.binding == "COMPOUND_RISK_LIMIT"
    assert r.compound_risk == pytest.approx(0.21) and r.compound_risk <= 0.225
    margin = 120.
    assert margin * r.leverage == pytest.approx(360.)   # 120 margin x 3x = 360 notional


def test_maximum_leverage_would_violate_profile_so_lower_leverage_is_chosen():
    assert 0.07 * 7 > 0.225                    # 49% at the user's 7x maximum
    assert leverage(0.07).leverage < 7


def test_user_maximum_is_a_ceiling_not_a_requirement():
    assert leverage(0.005).leverage == 7       # tight stop: asset-class 7x binds
    assert leverage(0.005, user_max=2.).leverage == 2
    assert leverage(0.12).leverage == 1        # 12% stop: 1x (12% compound)


# C -- no leverage >= 1x satisfies an explicit hard limit: blocked, margin untouched.
def test_no_legal_leverage_blocks_explicitly():
    r = leverage(0.07, compound=0.05)
    assert not r.ok and r.reason == "CATI_NO_LEGAL_LEVERAGE:COMPOUND_RISK_LIMIT"


def test_liquidation_safety_is_part_of_the_resolution():
    r = leverage(0.07, compound=0.30)          # profile alone would allow 4x
    assert r.ceilings["LIQUIDATION_SAFETY"] < 7 and r.leverage <= 4


# A/E -- historical ADA: next-open reference plus a later live price.
def test_historical_ada_executes_on_live_economics_after_drift():
    at_reference = economics(ADA, ADA["reference"])
    drifted = economics(ADA, 0.2665)            # moved ~100 bps from the next-open reference
    assert at_reference.valid and drifted.valid
    assert drifted.net_reward_risk >= 1.8 and LIMITS.min_stop_loss_pct <= drifted.stop_fraction <= 0.15
    assert leverage(drifted.stop_fraction).leverage == 3


# F -- an adverse chase that destroys the frozen 2.5R economics is blocked.
def test_chased_entry_that_destroys_economics_is_blocked():
    chased = economics(ADA, 0.2691 + 0.4 * (0.2691 - 0.2492285714285714))
    assert not chased.valid and chased.reason == "CATI_ENTRY_ECONOMICS_DEGRADED" and chased.chase_r > 0.25
    assert economics(ADA, 0.2490).reason == "STRUCTURAL_LEVEL_BROKEN"
    assert economics(ADA, 0.3200).reason == "CATI_TARGET_ALREADY_REACHED"


# D -- historical PORTAL: the structural stop exceeds the true system maximum.
def test_historical_portal_is_blocked_by_true_system_stop_maximum_not_the_entry_zone():
    for price in (0.01809, 0.01815, 0.01822):  # reference, DEMO and mainnet prints at evaluation
        r = economics(PORTAL, price)
        assert r.reason == "CATI_STRUCTURAL_STOP_EXCEEDS_SYSTEM_MAX" and r.stop_fraction > LIMITS.max_stop_loss_pct


def test_depth_vwap_walks_the_book_and_refuses_an_unfillable_quantity():
    book = [("0.2651", "125"), ("0.2665", "3038")]
    assert depth_vwap(book, 3163) == pytest.approx(842.7645 / 3163)
    assert depth_vwap(book, 4000) is None


# Daily loss authority -------------------------------------------------------
def instance(**kw):
    base = dict(id="bot_a", user_id="u", broker_account_id="brk", market_type="CRYPTO", strategy_id="cati",
                strategy_version="1", capital_allocation=500., allocation_type="fixed_amount", allocation_value=120.,
                symbols=["ADAUSDT"], universe_mode="ALLOWLIST", timeframes=["15m"], mode="live",
                risk_level="balanced", capital_allocation_type="fixed_amount", daily_loss_limit_pct=None)
    return SimpleNamespace(**{**base, **kw})


def policy(inst, profile_daily=0.025):
    return resolve_effective_bot_policy(instance=inst, broker_environment="demo",
                                        risk_params={"daily_loss_limit_pct": profile_daily, "max_leverage": 7})


def test_daily_limits_resolve_per_user_without_a_global_clamp():
    a = policy(instance(id="bot_a", user_id="ua", daily_loss_limit_pct=0.04))
    b = policy(instance(id="bot_b", user_id="ub", daily_loss_limit_pct=0.06))
    assert (a.max_daily_loss_pct, a.daily_loss_source) == (0.04, "USER_CONFIGURED")
    assert (b.max_daily_loss_pct, b.daily_loss_source) == (0.06, "USER_CONFIGURED")
    inherited = policy(instance(id="bot_c"))
    assert (inherited.max_daily_loss_pct, inherited.daily_loss_source) == (0.025, "RISK_PROFILE_DEFAULT")


def test_fixed_amount_120_resolves_unchanged_into_the_effective_policy():
    p = policy(instance())
    assert (p.position_allocation_type, p.position_allocation_value) == ("fixed_amount", 120.)
    pct = policy(instance(allocation_type="percent_balance", allocation_value=5.))
    assert (pct.position_allocation_type, pct.position_allocation_value) == ("percent_balance", 5.)


def test_invalid_daily_limit_is_rejected_not_rewritten():
    with pytest.raises(EffectivePolicyError):
        policy(instance(daily_loss_limit_pct=0.30))       # at/above the emergency drawdown halt
    with pytest.raises(ValueError):
        validate_daily_loss_limit_pct(0.0)
    assert validate_daily_loss_limit_pct(0.08) == 0.08    # above the old 2.5% ceiling, untouched


def test_adaptive_budget_adapts_inside_the_resolved_policy():
    def evaluate(pct, **kw):
        engine = AdaptiveDailyRiskBudgetEngine(AdaptiveDailyRiskPolicy(max_daily_loss_pct=pct))
        return engine.evaluate(AdaptiveDailyRiskInputs(bot_instance_id="b", risk_date=__import__("datetime").date(2026, 10, 6),
                                                        day_open_equity=400., current_equity=400., realized_pnl_today=0., **kw))
    no_history = evaluate(0.05)
    assert no_history.effective_daily_budget_usdt == pytest.approx(20.)   # the user's 5% of 400, no hidden 6/24 USDT
    assert evaluate(0.05, account_drawdown_pct=6.).effective_daily_budget_usdt < 20.  # tightening only
    with pytest.raises(ValueError, match="DAILY_LOSS_POLICY_REQUIRED"):
        evaluate(None)
