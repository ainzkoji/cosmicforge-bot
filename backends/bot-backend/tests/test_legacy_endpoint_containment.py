from __future__ import annotations

import pytest
from fastapi import HTTPException

import app.main as main
from shared_lib.persistence.migrations import migrate


@pytest.mark.parametrize(
    "endpoint,args",
    [
        (main.binance_balance, ()),
        (main.binance_open_orders, ("BTCUSDT",)),
        (main.binance_position, ("BTCUSDT",)),
        (main.trade_close, ("BTCUSDT",)),
        (main.trade_close_record, ("BTCUSDT",)),
        (main.trade_close_record_usertrades, ("BTCUSDT", 10)),
        (main.debug_run_cycle, ("BTCUSDT",)),
        (main.debug_run_force, ()),
    ],
)
def test_legacy_broker_mutations_fail_cleanly_without_env_credentials(endpoint, args):
    with pytest.raises(HTTPException) as exc_info:
        endpoint(*args)

    assert exc_info.value.status_code == 410
    assert "disabled" in str(exc_info.value.detail).lower() or "retired" in str(exc_info.value.detail).lower()


def test_runner_status_uses_multibot_state_without_constructing_legacy_runner(tmp_path, monkeypatch):
    db_path = tmp_path / "runner-status.db"
    migrate(str(db_path))
    monkeypatch.setattr(main, "get_db_path", lambda: str(db_path))
    monkeypatch.setattr(
        main,
        "get_runner",
        lambda: pytest.fail("runner/status must not construct the legacy env-backed runner"),
    )

    status = main.runner_status()

    assert "multi_bot_runner_initialized" in status
    assert status["bots"] == []

