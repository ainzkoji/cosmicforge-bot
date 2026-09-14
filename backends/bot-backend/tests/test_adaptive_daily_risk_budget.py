from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone

import pytest

from app.policy.policy_engine import PolicyContext, PolicyEngine, ReasonCode
from app.risk.adaptive_daily_budget import (
    AdaptiveDailyRiskBudgetEngine,
    AdaptiveDailyRiskInputs,
    AdaptiveDailyRiskPolicy,
    DailyRiskState,
    planned_initial_risk_usdt,
)


def policy(**overrides):
    defaults = dict(
        max_daily_loss_pct=0.025,
        daily_r_budget=1.5,
        minimum_history_trades=5,
        minimum_budget_usdt=6.0,
        maximum_budget_usdt=24.0,
        timezone_name="Europe/Rome",
    )
    defaults.update(overrides)
    return AdaptiveDailyRiskPolicy(**defaults)


def engine(**overrides):
    return AdaptiveDailyRiskBudgetEngine(policy(**overrides))


def inputs(**overrides):
    defaults = dict(
        bot_instance_id="bot-1",
        risk_date=date(2026, 9, 15),
        day_open_equity=760.0,
        current_equity=780.0,
        realized_pnl_today=0.0,
        initial_risk_history_usdt=(8, 9, 10, 11, 12, 1000),
        recent_r_history=(0.2,) * 20,
        account_drawdown_pct=0.0,
        volatility_stress=0.0,
        market_regime="WEAK_TREND",
    )
    defaults.update(overrides)
    return AdaptiveDailyRiskInputs(**defaults)


def test_daily_budget_derived_from_day_opening_equity():
    d = engine(maximum_budget_usdt=1000).evaluate(inputs(day_open_equity=1000.0))
    assert d.hard_daily_cap_usdt == 25.0


def test_hard_cap_does_not_grow_intraday_after_profits():
    d = engine(maximum_budget_usdt=1000).evaluate(inputs(day_open_equity=1000.0, current_equity=1300.0))
    assert d.hard_daily_cap_usdt == 25.0


def test_actual_risk_per_trade_influences_adaptive_budget():
    low = engine(maximum_budget_usdt=1000).evaluate(inputs(initial_risk_history_usdt=(4, 5, 6, 5, 4)))
    high = engine(maximum_budget_usdt=1000).evaluate(inputs(initial_risk_history_usdt=(14, 15, 16, 15, 14)))
    assert high.base_adaptive_budget_usdt > low.base_adaptive_budget_usdt


def test_outlier_loss_does_not_explode_rolling_risk_estimate():
    d = engine(maximum_budget_usdt=1000).evaluate(inputs(initial_risk_history_usdt=(10, 10, 11, 9, 10, 10_000)))
    assert d.typical_trade_risk_usdt <= 11


def test_poor_expectancy_tightens_budget():
    good = engine(maximum_budget_usdt=1000).evaluate(inputs(recent_r_history=(0.2,) * 20))
    poor = engine(maximum_budget_usdt=1000).evaluate(inputs(recent_r_history=(-0.5,) * 20))
    assert poor.performance_factor < good.performance_factor
    assert poor.effective_daily_budget_usdt < good.effective_daily_budget_usdt


def test_drawdown_tightens_budget():
    normal = engine(maximum_budget_usdt=1000).evaluate(inputs(account_drawdown_pct=0.0))
    defensive = engine(maximum_budget_usdt=1000).evaluate(inputs(account_drawdown_pct=6.0))
    assert defensive.drawdown_factor < normal.drawdown_factor


def test_volatility_can_tighten_budget():
    normal = engine(maximum_budget_usdt=1000).evaluate(inputs(volatility_stress=0.0))
    stressed = engine(maximum_budget_usdt=1000).evaluate(inputs(volatility_stress=1.0, market_regime="HIGH_VOLATILITY"))
    assert stressed.volatility_factor < normal.volatility_factor


def test_no_martingale_behavior_after_strong_performance():
    d = engine(performance_factor_max=1.0, maximum_budget_usdt=1000).evaluate(inputs(recent_r_history=(2.0,) * 50))
    assert d.performance_factor <= 1.0


def test_120_remains_per_trade_ceiling_outside_daily_engine():
    d = engine(maximum_budget_usdt=1000).evaluate(inputs())
    assert d.effective_daily_budget_usdt != 120.0


def test_margin_is_not_confused_with_risk():
    risk = planned_initial_risk_usdt(quantity=100, entry_price=10, stop_price=9.8, fee_slippage_buffer_pct=0)
    margin = 120.0
    assert risk == pytest.approx(20.0)
    assert risk != margin


def test_planned_stop_risk_includes_fee_slippage_buffer():
    risk = planned_initial_risk_usdt(quantity=100, entry_price=10, stop_price=9.8, fee_slippage_buffer_pct=0.001)
    assert risk > 20.0


def test_pending_trade_reserves_daily_risk():
    e = engine(maximum_budget_usdt=1000)
    d = e.evaluate(inputs())
    assert e.reserve("bot-1", date(2026, 9, 15), "a", 5.0, d)
    assert e.reserved_risk("bot-1", date(2026, 9, 15)) == 5.0


def test_concurrent_reservations_are_safe():
    e = engine(maximum_budget_usdt=15)
    d = e.evaluate(inputs())
    assert e.reserve("bot-1", date(2026, 9, 15), "a", 10.0, d)
    assert not e.reserve("bot-1", date(2026, 9, 15), "b", 10.0, e.evaluate(inputs()))


def test_failed_entry_releases_reservation():
    e = engine(maximum_budget_usdt=1000)
    d = e.evaluate(inputs())
    assert e.reserve("bot-1", date(2026, 9, 15), "a", 5.0, d)
    e.release("bot-1", date(2026, 9, 15), "a")
    assert e.reserved_risk("bot-1", date(2026, 9, 15)) == 0


def test_partial_fill_settles_actual_risk():
    e = engine(maximum_budget_usdt=1000)
    d = e.evaluate(inputs())
    assert e.reserve("bot-1", date(2026, 9, 15), "a", 10.0, d)
    e.settle_partial("bot-1", date(2026, 9, 15), "a", 4.0)
    assert e.reserved_risk("bot-1", date(2026, 9, 15)) == 4.0


def test_full_fill_settles_actual_risk():
    e = engine(maximum_budget_usdt=1000)
    d = e.evaluate(inputs())
    assert e.reserve("bot-1", date(2026, 9, 15), "a", 10.0, d)
    e.settle_full("bot-1", date(2026, 9, 15), "a")
    assert e.reserved_risk("bot-1", date(2026, 9, 15)) == 0


def test_fees_included_in_observability():
    d = engine().evaluate(inputs(fees_today=1.25))
    assert d.fees_today == 1.25


def test_funding_included_in_observability():
    d = engine().evaluate(inputs(funding_today=0.33))
    assert d.funding_today == 0.33


def test_normal_state():
    assert engine().evaluate(inputs(realized_pnl_today=0)).daily_risk_state == DailyRiskState.NORMAL.value


def test_caution_state():
    assert engine().evaluate(inputs(realized_pnl_today=-8)).daily_risk_state == DailyRiskState.CAUTION.value


def test_defensive_state():
    assert engine().evaluate(inputs(realized_pnl_today=-13)).daily_risk_state == DailyRiskState.DEFENSIVE.value


def test_hard_stop_state():
    assert engine().evaluate(inputs(realized_pnl_today=-20)).daily_risk_state == DailyRiskState.HARD_STOP.value


def test_new_day_resets_active_risk_state_by_risk_date():
    e = engine()
    today = e.evaluate(inputs(risk_date=date(2026, 9, 15), realized_pnl_today=-20))
    tomorrow = e.evaluate(inputs(risk_date=date(2026, 9, 16), realized_pnl_today=0))
    assert today.daily_risk_state == "HARD_STOP"
    assert tomorrow.daily_risk_state == "NORMAL"


def test_historical_day_evidence_preserved_by_insert_only_persistence():
    conn = sqlite3.connect(":memory:")
    e = AdaptiveDailyRiskBudgetEngine(policy(), db=conn)
    e.evaluate(inputs(risk_date=date(2026, 9, 15)))
    e.evaluate(inputs(risk_date=date(2026, 9, 16)))
    rows = conn.execute("SELECT risk_date FROM adaptive_daily_risk_decisions ORDER BY id").fetchall()
    assert [r[0] for r in rows] == ["2026-09-15", "2026-09-16"]


def test_timezone_boundary_uses_europe_rome():
    e = engine()
    assert e.risk_date_for(datetime(2026, 9, 13, 22, 15, tzinfo=timezone.utc)) == date(2026, 9, 14)


def test_restart_across_midnight_uses_new_risk_date():
    e = engine()
    before = e.risk_date_for(datetime(2026, 9, 13, 21, 59, tzinfo=timezone.utc))
    after = e.risk_date_for(datetime(2026, 9, 13, 22, 1, tzinfo=timezone.utc))
    assert before == date(2026, 9, 13)
    assert after == date(2026, 9, 14)


def test_insufficient_history_conservative_fallback():
    d = engine().evaluate(inputs(initial_risk_history_usdt=(10, 11)))
    assert not d.data_sufficient
    assert d.effective_daily_budget_usdt == 6.0


def test_weekly_risk_remains_independent():
    d = engine().evaluate(inputs())
    payload = d.as_policy_context()
    assert "weekly_drawdown_pct" not in payload


def test_slot_risk_remains_independent():
    d = engine().evaluate(inputs())
    payload = d.as_policy_context()
    assert "max_open_positions" not in payload


def test_capital_semantics_remain_correct():
    d = engine().evaluate(inputs(day_open_equity=760))
    assert d.hard_daily_cap_usdt == 19.0
    assert d.day_open_equity != 120.0


def test_broker_authoritative_fill_behavior_remains_external_to_budget_engine():
    d = engine().evaluate(inputs())
    assert "broker_fill" not in d.as_policy_context()


def test_policy_engine_blocks_adaptive_hard_stop():
    d = engine().evaluate(inputs(realized_pnl_today=-20))
    decision = PolicyEngine().evaluate(
        PolicyContext(symbol="BTCUSDT", signal="BUY", adaptive_daily_risk=d.as_policy_context())
    )
    assert decision.reason_code in {
        ReasonCode.DAILY_RISK_BUDGET_EXHAUSTED,
        ReasonCode.DAILY_HARD_EQUITY_CAP_REACHED,
    }


def test_policy_engine_preserves_legacy_gate_when_adaptive_not_effective():
    d = engine().evaluate(inputs(realized_pnl_today=-20, policy_effective=False))
    decision = PolicyEngine().evaluate(
        PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            daily_realized_pnl=-7,
            max_daily_loss=6,
            adaptive_daily_risk=d.as_policy_context(),
        )
    )
    assert decision.reason_code == ReasonCode.DAILY_LOSS_LIMIT


def test_no_canonical_db_test_writes(tmp_path):
    db = tmp_path / "risk.db"
    conn = sqlite3.connect(db)
    AdaptiveDailyRiskBudgetEngine(policy(), db=conn).evaluate(inputs())
    assert "cosmicforge.db" not in str(db)
