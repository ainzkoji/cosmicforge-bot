"""Sole CATI identity must survive legacy API and scheduler callers."""
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from app.api.bot_instances import get_engine_status
from app.core.bot_instance_service import BotInstanceService
from app.signals.crypto_signal_engine import generate_crypto_signals


@pytest.mark.parametrize("legacy", ["master_ensemble", "sma_cross", "v2", "external_tradingview", ""])
def test_new_instances_cannot_select_legacy_intelligence(legacy):
    db = Mock()
    service = BotInstanceService(db)
    with pytest.raises(ValueError, match="CATI_ONLY_ENGINE"):
        service.create_bot_instance(SimpleNamespace(strategy_id=legacy))
    db.connect.assert_not_called()


def test_retired_generator_has_no_network_or_database_side_effects():
    market_data, repository = Mock(), Mock()
    result = generate_crypto_signals(market_data=market_data, repository=repository)
    assert result["status"] == "BLOCKED"
    assert result["signals_created"] == result["published"] == 0
    assert market_data.mock_calls == repository.mock_calls == []


@pytest.mark.parametrize("profile", ["conservative", "balanced", "aggressive"])
def test_displayed_risk_profiles_never_exceed_daily_hard_cap(profile):
    assert BotInstanceService.get_risk_profile_preset(profile)["daily_loss_limit_pct"] <= 0.025


def test_engine_status_rejects_another_users_instance_before_account_read():
    service = SimpleNamespace(get_bot_instance=lambda _: SimpleNamespace(user_id="other"), db=Mock())
    with pytest.raises(HTTPException) as exc:
        get_engine_status("bot", user={"id": "owner"}, service=service, _perm="bot:read")
    assert exc.value.status_code == 403
    service.db.connect.assert_not_called()


def test_engine_status_rejects_another_users_broker_account():
    instance = SimpleNamespace(user_id="owner", broker_account_id="broker")
    conn = Mock()
    conn.execute.return_value.fetchone.return_value = {"user_id": "other"}
    @contextmanager
    def connect():
        yield conn
    service = SimpleNamespace(get_bot_instance=lambda _: instance, db=SimpleNamespace(connect=connect))
    with pytest.raises(HTTPException) as exc:
        get_engine_status("bot", user={"id": "owner"}, service=service, _perm="bot:read")
    assert exc.value.status_code == 403


def test_new_backtest_cannot_queue_a_legacy_engine(monkeypatch):
    from app.api import backtesting
    database = Mock()
    monkeypatch.setattr(backtesting, "db", database)
    with pytest.raises(HTTPException) as exc:
        backtesting.create_backtest(SimpleNamespace(strategy_id="sma_cross"), user={"id": "owner"})
    assert exc.value.status_code == 409
    assert exc.value.detail["reason"] == "CATI_BACKTEST_ARTIFACT_REQUIRED"
    database.connect.assert_not_called()


def test_m0_status_keeps_cati_runtime_selected_and_entry_blocked(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    database = DB(str(tmp_path / "engine_status.db"))
    migrate(database)
    with database.connect() as conn:
        conn.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,status,environment,created_at,updated_at) VALUES ('broker','owner','bybit','crypto','connected','demo','now','now')")
    instance = SimpleNamespace(user_id="owner", broker_account_id="broker", status="active", strategy_id="master_ensemble")
    service = SimpleNamespace(get_bot_instance=lambda _: instance, db=database)
    status = get_engine_status("bot", user={"id": "owner"}, service=service, _perm="bot:read")
    assert status["engine"] == "CATI"
    assert status["cati_runtime_active"] is True
    assert status["cati_entry_authority"] == "BLOCKED"
    assert status["observe_mode"] is True
    assert status["historical_strategy_id"] == "master_ensemble"
    assert status["hard_daily_loss_cap_pct"] == 2.5
    assert status["auto_capital_routing_independent"] is True


def test_compatibility_payload_preserves_percentage_allocation():
    from app.api.auto_pilot import DeployAutoPilotRequest
    request = DeployAutoPilotRequest(broker_account_ids=["account"], allocation_value=10,
                                    capital_allocation=500, allocation_type="percent_balance")
    assert request.allocation.allocation_type == "percent_balance"
    assert request.allocation.trade_amount_per_position == 10
    assert request.allocation.total_capital_budget == 500
