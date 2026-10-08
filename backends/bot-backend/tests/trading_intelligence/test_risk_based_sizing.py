"""Step 1.4 -- risk-based position sizing: one formula, every cap a ceiling,
never inflated to an exchange minimum, and the same path at preview and at
execution (through the real boundary and hard-risk chain)."""
from decimal import Decimal
from types import SimpleNamespace

import pytest
from _exec import Harness

from app.core import risk_profile_params
from app.trading_intelligence.execution import risk_sizing as rs
from app.trading_intelligence.execution.boundary import BoundaryStatus as B

D = Decimal
BINANCE = dict(min_notional="5", qty_step="0.001", min_qty="0.001", max_qty="10000")


def size(**over):
    base = dict(budget_usdt="1000", risk_fraction="0.005", stop_distance_fraction="0.08", price="100",
                leverage=5, leverage_ceiling=2, free_margin_usdt="1000", max_open_risk_fraction="0.02", **BINANCE)
    base.update(over)
    return rs.size_risk_based(**base)


# ── the master plan example ─────────────────────────────────────────────────

def test_the_balanced_example_sizes_five_usdt_of_risk_at_an_eight_percent_stop():
    s = size()
    assert s.approved and s.risk_usdt == D("5.000") and s.raw_notional_usdt == D("62.5")
    assert s.notional_usdt == D("62.500") and s.quantity == D("0.625")
    assert s.leverage == 2 and s.margin_usdt == D("31.25")               # leverage is derived, never a multiplier
    assert s.loss_at_stop_usdt == D("5.00000") and s.binding == "RISK_DERIVED"


@pytest.mark.parametrize("stop,notional,qty", [
    ("0.005", "1000.000", "10"),        # very tight stop: 5 / 0.005 = 1000 notional
    ("0.02", "250.000", "2.5"),         # normal
    ("0.08", "62.500", "0.625"),        # wide
    ("0.15", "33.300", "0.333"),        # the widest the system allows (rounded DOWN to the step)
])
def test_five_stop_distances_scale_the_notional_inversely(stop, notional, qty):
    s = size(stop_distance_fraction=stop, leverage_ceiling=3, leverage=3, free_margin_usdt="5000")
    assert s.approved and s.notional_usdt == D(notional) and s.quantity == D(qty)
    assert s.loss_at_stop_usdt <= s.risk_usdt                             # rounding down never exceeds the risk


def test_a_stop_that_produces_an_order_below_the_exchange_minimum_is_blocked_not_inflated():
    s = size(budget_usdt="50", stop_distance_fraction="0.08")             # 0.25 USDT of risk -> 3.125 notional
    assert not s.approved and s.reason == rs.RISK_SIZE_BELOW_EXCHANGE_MINIMUM
    assert s.notional_usdt == 0 and s.quantity == 0
    tiny = size(budget_usdt="1000", stop_distance_fraction="0.08", price="100000")   # qty below the step
    assert not tiny.approved and tiny.reason == rs.RISK_SIZE_BELOW_EXCHANGE_MINIMUM


@pytest.mark.parametrize("stop", ["0", "-0.01", "nan", "inf", None, "0.0001", "0.2", "abc"])
def test_invalid_stop_distances_never_produce_an_order(stop):
    s = size(stop_distance_fraction=stop)
    assert not s.approved and s.reason == rs.RISK_SIZE_STOP_DISTANCE_INVALID and s.notional_usdt == 0


@pytest.mark.parametrize("level,fraction,expected", [
    ("conservative", "0.0025", "31.200"), ("balanced", "0.005", "62.500"), ("aggressive", "0.0075", "93.700")])   # rounded DOWN to the 0.001 step
def test_all_three_profiles(level, fraction, expected):
    s = size(risk_fraction=fraction, leverage_ceiling=3, leverage=3)
    assert s.approved and s.notional_usdt == D(expected)


def test_fixed_and_percentage_budgets():
    assert rs.effective_budget(budget_type="fixed_amount", budget_value="1000", account_equity="5000") == D("1000")
    assert rs.effective_budget(budget_type="percent_balance", budget_value="20", account_equity="5000") == D("1000")
    for kwargs in (dict(budget_type="percent_balance", budget_value="150", account_equity="5000"),
                   dict(budget_type="fixed_amount", budget_value="-1", account_equity="5000"),
                   dict(budget_type="percent_balance", budget_value="20", account_equity="0"),
                   dict(budget_type="lottery", budget_value="1", account_equity="1")):
        with pytest.raises(ValueError, match=rs.RISK_SIZE_INVALID_INPUT):
            rs.effective_budget(**kwargs)


def test_each_cap_binds_in_the_validated_order():
    assert size(max_position_usdt="40").binding == "USER_MAX_POSITION"
    assert size(max_position_usdt="40").notional_usdt == D("40.000")
    assert size(max_open_risk_fraction="0.02", open_risk_usdt="17").binding == "OPEN_RISK_CAP"   # 3 USDT left / 0.08 = 37.5
    assert size(max_open_risk_fraction="0.02", open_risk_usdt="17").notional_usdt == D("37.500")
    blocked = size(max_open_risk_fraction="0.02", open_risk_usdt="20")
    assert not blocked.approved and blocked.reason == rs.RISK_SIZE_OPEN_RISK_CAP_REACHED
    low = size(free_margin_usdt="20")                                     # 20 * (1 - 0.0015) * 2 = 39.94 notional
    assert low.binding == "AVAILABLE_MARGIN" and low.notional_usdt == D("39.900")
    assert size(system_max_notional_usdt="50").binding == "SYSTEM_MAX_NOTIONAL"
    assert size(leverage=1, leverage_ceiling=3).leverage == 1 and size(leverage=5, leverage_ceiling=3).leverage == 3
    assert not size(leverage="0.5").approved and size(leverage="0.5").reason == rs.RISK_SIZE_NO_LEGAL_LEVERAGE
    assert not size(free_margin_usdt="0").approved and size(free_margin_usdt="0").reason == rs.RISK_SIZE_INSUFFICIENT_MARGIN


def test_exchange_filters_round_down_and_cap_quantity():
    s = size(stop_distance_fraction="0.03", qty_step="0.1")              # 166.666 notional -> 1.6 qty
    assert s.quantity == D("1.6") and s.notional_usdt == D("160.0")
    capped = size(stop_distance_fraction="0.005", max_qty="4")            # 10 qty wanted
    assert capped.quantity == D("4.000") and capped.binding == "EXCHANGE_MAX_QTY"
    multiplied = size(contract_multiplier="10")                           # 62.5 notional / (100 * 10) = 0.0625 -> 0.062
    assert multiplied.quantity == D("0.062") and multiplied.notional_usdt == D("62.000")


def test_signal_strength_scales_the_risk_not_the_cap():
    half = size(signal_strength="0.5")
    assert half.risk_usdt == D("2.5000") and half.notional_usdt == D("31.200")      # 0.3125 qty rounded down to 0.312
    assert not size(signal_strength="0").approved and not size(signal_strength="1.5").approved


def test_precision_and_overflow_cases():
    big = size(budget_usdt="123456789012.34", free_margin_usdt="999999999999", leverage_ceiling=1, leverage=1,
               max_qty="1000000000000")
    assert big.approved and big.notional_usdt == D("7716049313.200")      # exact Decimal (qty 77160493.132), no float drift
    assert size(price="0.00001234", qty_step="1", min_qty="1", max_qty=None).approved   # 5 064 829 contracts, exact
    with pytest.raises(ValueError, match=rs.RISK_SIZE_INVALID_INPUT):
        size(budget_usdt="abc")
    assert not size(risk_fraction="0.5").approved                          # 50 % per trade is not a risk level


def test_preview_and_execution_share_the_formula():
    preview = rs.preview_range(budget_usdt="1000", risk_fraction="0.005", price="100", leverage_ceiling=2,
                               stop_distances={"tight": "0.02", "typical": "0.05", "wide": "0.08"},
                               free_margin_usdt="1000", max_open_risk_fraction="0.02", **BINANCE)
    assert D(preview["wide"]["notional_usdt"]) == D("62.5") and D(preview["tight"]["notional_usdt"]) == D("250")
    assert D(preview["typical"]["margin_usdt"]) == D("50")
    assert preview["wide"] == size().evidence()


# ── the profile library feeds the engine ────────────────────────────────────

def test_the_versioned_profile_becomes_engine_risk_params():
    params = risk_profile_params.risk_based_params(SimpleNamespace(risk_level="aggressive", risk_profile_version=None))
    assert params["per_trade_risk_pct"] == 0.0075 and params["max_open_risk_fraction"] == 0.03
    assert params["leverage_ceiling"] == 3.0 and params["max_position_slots"] == 8
    assert params["daily_loss_limit_pct"] == 0.03 and params["risk_profile_version"] == "2026-10-08.v1"


def test_the_policy_resolver_keeps_the_system_ceiling_for_risk_based_bots():
    from app.runner.effective_policy import resolve_effective_bot_policy
    instance = SimpleNamespace(id="bot-rb", user_id="u", broker_account_id="acct", market_type="CRYPTO",
                               strategy_id="cati", strategy_version="1", risk_level="balanced",
                               risk_profile_version="2026-10-08.v1", allocation_type="risk_based", allocation_value=0.5,
                               capital_allocation=1000.0, capital_allocation_type="fixed_amount", symbols=["ADAUSDT"],
                               timeframes=["15m"], universe_mode="ALLOWLIST", mode="live", daily_loss_limit_pct=None,
                               max_position_usdt=200.0)
    policy = resolve_effective_bot_policy(instance=instance, broker_environment="demo",
                                          risk_params=risk_profile_params.risk_based_params(instance))
    assert policy.position_allocation_type == "risk_based" and policy.capital_budget == 1000.0
    assert policy.requested_risk_per_trade == 0.005 and policy.risk_per_trade == 0.004   # 0.4 % ceiling, unresolved conflict
    assert policy.max_leverage == 2.0 and policy.leverage_ceiling == 2.0 and policy.max_open_risk_fraction == 0.02
    assert policy.max_position_usdt == 200.0 and policy.risk_profile_version == "2026-10-08.v1"
    assert policy.max_daily_loss_pct == 0.02


def test_legacy_bots_are_untouched_by_the_new_model(tmp_path):
    from app.risk.capital_ledger import per_trade_allocation_margin
    assert per_trade_allocation_margin("fixed_amount", 100.0, 1000.0) == 100.0
    assert per_trade_allocation_margin("percent_balance", 10.0, 1000.0) == 100.0
    assert per_trade_allocation_margin("risk_based", 0.5, 1000.0) == 1000.0        # the whole budget is the ceiling
    h = Harness(tmp_path)                                                           # a legacy fixed-size bot
    assert h.orch.validated_config.use_fixed_size and h.orch.validated_config.fixed_size_usdt == 120.0
    out = h.run()
    # 120 margin x the resolved 4x leverage (compound-risk ceiling at a 5 % stop) / 100, less the 15 bps fill reserve
    assert out.status == B.EXECUTED and float(h.seen["orders"][0].qty) == pytest.approx(4.8, rel=2e-3)


# ── through the real boundary ───────────────────────────────────────────────

def risk_based_policy(**over):
    base = dict(position_allocation_type="risk_based", capital_allocation_type="fixed_amount", capital_budget=1000.0,
                risk_per_trade=0.004, max_leverage=2.0, leverage_ceiling=2.0, max_open_risk_fraction=0.02,
                max_position_usdt=None, risk_profile_version="2026-10-08.v1")
    base.update(over)
    return SimpleNamespace(**base)


def test_a_risk_based_bot_is_sized_by_the_boundary_and_executed_at_that_size(tmp_path):
    h = Harness(tmp_path)
    h.orch.effective_policy = risk_based_policy()
    h.executor._allocation_type, h.executor._allocation_value, h.executor._capital_budget = "risk_based", 0.5, 1000.0
    out = h.run()
    # plan: SHORT, entry 100, invalidation 105 -> 5 % stop; 1000 x 0.4 % = 4 USDT of risk -> 80 notional, 0.8 qty
    assert out.status == B.EXECUTED, out.reason_codes
    assert float(h.seen["orders"][0].qty) == pytest.approx(0.8, rel=2e-3)   # less the executor's 15 bps fill reserve
    assert out.attempt is not None and out.attempt.broker_order_id


def test_a_risk_based_bot_below_the_exchange_minimum_is_blocked_before_the_broker(tmp_path):
    h = Harness(tmp_path)
    h.orch.effective_policy = risk_based_policy(capital_budget=50.0)   # 0.2 USDT of risk -> 4 notional < 5 minimum
    h.executor._allocation_type, h.executor._allocation_value, h.executor._capital_budget = "risk_based", 0.5, 50.0
    out = h.run()
    assert out.status == B.RISK_REJECTED and rs.RISK_SIZE_BELOW_EXCHANGE_MINIMUM in out.reason_codes
    assert h.seen["orders"] == []


def test_a_user_maximum_position_binds_through_the_boundary(tmp_path):
    h = Harness(tmp_path)
    h.orch.effective_policy = risk_based_policy(max_position_usdt=40.0)
    h.executor._allocation_type, h.executor._allocation_value, h.executor._capital_budget = "risk_based", 0.5, 1000.0
    out = h.run()
    assert out.status == B.EXECUTED, out.reason_codes
    assert float(h.seen["orders"][0].qty) == pytest.approx(0.4, rel=2e-3)


@pytest.mark.parametrize("level,approved_pct,effective_pct", [("conservative", "0.25", "0.25"), ("balanced", "0.50", "0.40"),
                                                              ("aggressive", "0.75", "0.40")])
def test_the_per_trade_ceiling_is_one_value_in_the_engine_the_library_and_the_preview(level, approved_pct, effective_pct):
    """Step 1 closure, risk-ceiling conflict: the approved profiles say 0.25 / 0.50 /
    0.75 %, the engine's ceiling is 0.40 %. Until the owner approves a change, every
    path applies the stricter value and the preview shows exactly what the engine uses."""
    from decimal import Decimal
    from app.risk.system_limits import SystemLimits
    from app.runner.effective_policy import resolve_effective_bot_policy
    from shared_lib import risk_levels
    limits = SystemLimits()
    # 1. one ceiling: the engine's limit and the library's constant are the same number
    assert Decimal(str(limits.max_risk_per_trade_ceiling)) * 100 == risk_levels.SYSTEM_PER_TRADE_RISK_CEILING_PCT == Decimal("0.40")
    profile = risk_levels.get_profile(level)
    assert profile.per_trade_risk_pct == Decimal(approved_pct)                      # the approved value is not altered
    # 2. the engine's resolved policy (what sizing reads)
    instance = SimpleNamespace(id="bot-c", user_id="u", broker_account_id="acct", market_type="CRYPTO", strategy_id="cati",
                               strategy_version="1", risk_level=level, risk_profile_version=None, allocation_type="risk_based",
                               allocation_value=float(profile.per_trade_risk_pct), capital_allocation=1000.0,
                               capital_allocation_type="fixed_amount", symbols=["ADAUSDT"], timeframes=["15m"],
                               universe_mode="ALLOWLIST", mode="live", daily_loss_limit_pct=None, max_position_usdt=None)
    policy = resolve_effective_bot_policy(instance=instance, broker_environment="demo",
                                          risk_params=risk_profile_params.risk_based_params(instance))
    assert Decimal(str(policy.risk_per_trade)) * 100 == Decimal(effective_pct)
    assert Decimal(str(policy.requested_risk_per_trade)) * 100 == Decimal(approved_pct)
    # 3. the library and the preview say the same, and never claim the wider value
    assert risk_levels.effective_per_trade_risk_pct(profile) == Decimal(effective_pct)
    money = risk_levels.money_view(level, "1000")
    assert money["effective_per_trade_risk_pct"] == Decimal(effective_pct) and money["per_trade_risk_pct"] == Decimal(approved_pct)
    assert money["risk_per_trade"] == Decimal("1000") * Decimal(effective_pct) / 100
    assert money["ceiling_applied"] is (approved_pct != effective_pct)
    # 4. the size the engine would take loses no more than that at its stop
    sized = rs.size_risk_based(budget_usdt=1000, risk_fraction=policy.risk_per_trade, stop_distance_fraction=0.02, price=100,
                                        leverage=1, leverage_ceiling=policy.leverage_ceiling, max_position_usdt=None,
                                        free_margin_usdt=1000, open_risk_usdt=0, max_open_risk_fraction=policy.max_open_risk_fraction,
                                        system_max_notional_usdt=None, min_notional=5, qty_step=0.001, min_qty=0.001, max_qty=None,
                                        contract_multiplier=1)
    assert sized.approved and float(sized.quantity) * 100 * 0.02 <= float(money["risk_per_trade"]) + 1e-9
