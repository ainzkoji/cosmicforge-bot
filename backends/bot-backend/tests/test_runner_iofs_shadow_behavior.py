from __future__ import annotations

from scripts.validation.run_paper_cycle_diagnostic import execution_reachability


def test_failed_iofs_shadow_result_does_not_block_executor_reachability():
    result = execution_reachability(
        strategy_action="BUY",
        gate_allowed=True,
        iofs_mode="shadow",
        iofs_passed=False,
        ml_enabled=False,
    )

    assert result["executor_would_be_called"] is True
    assert "iofs_blocked" not in result["blockers"]
    assert result["iofs_shadow_non_blocking"] is True


def test_failed_iofs_enforce_result_blocks_executor_reachability():
    result = execution_reachability(
        strategy_action="BUY",
        gate_allowed=True,
        iofs_mode="enforce",
        iofs_passed=False,
        ml_enabled=False,
    )

    assert result["executor_would_be_called"] is False
    assert "iofs_blocked" in result["blockers"]


def test_ml_disabled_does_not_block_executor_reachability():
    result = execution_reachability(
        strategy_action="SELL",
        gate_allowed=True,
        iofs_mode="shadow",
        iofs_passed=False,
        ml_enabled=False,
        ml_blocked=True,
    )

    assert result["executor_would_be_called"] is True
    assert "ml_blocked" not in result["blockers"]
    assert result["ml_disabled_non_blocking"] is True


def test_executor_is_reachable_when_all_active_gates_pass():
    result = execution_reachability(
        strategy_action="EXECUTE",
        gate_allowed=True,
        iofs_mode="shadow",
        iofs_passed=False,
        ml_enabled=False,
        circuit_tripped=False,
        kill_switch=False,
        daily_trade_count=0,
        max_daily_trades=3,
        open_positions=0,
        max_open_positions=3,
    )

    assert result == {
        "executor_would_be_called": True,
        "blockers": [],
        "iofs_shadow_non_blocking": True,
        "ml_disabled_non_blocking": True,
    }
