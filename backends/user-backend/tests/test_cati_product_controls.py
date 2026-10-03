"""CATI is the sole new product choice; allocations survive the proxy."""
import asyncio
from unittest.mock import AsyncMock

import pytest
from app.api.auto_pilot_proxy import AllocationParams, DeployAutoPilotRequest, deploy_auto_pilot
from app.core import onboarding_service


def test_onboarding_has_only_cati_and_rejects_legacy_selection():
    assert [strategy.id for strategy in onboarding_service.get_strategy_catalog()] == ["cati"]
    for legacy in ("safe_trend", "mean_reversion", "scalp_master", "master_ensemble"):
        with pytest.raises(ValueError):
            onboarding_service.validate_strategy_choice(legacy)


@pytest.mark.parametrize("risk", ["low", "medium", "high"])
def test_onboarding_cannot_advertise_more_than_daily_hard_loss_cap(risk):
    assert onboarding_service.get_risk_preset(risk).max_daily_loss_pct <= 2.5


def test_proxy_preserves_percentage_and_custom_universe(monkeypatch):
    from app.api import auto_pilot_proxy
    from app.core import broker_service
    monkeypatch.setattr(broker_service, "get_decrypted_credentials", lambda user, account: {"broker_id": "bybit"})
    proxy = AsyncMock(return_value={"instances": []})
    monkeypatch.setattr(auto_pilot_proxy, "proxy_request", proxy)
    request = DeployAutoPilotRequest(
        broker_account_ids=["owned-account"], risk_mode="medium",
        allocation=AllocationParams(total_capital_budget=500, trade_amount_per_position=10, allocation_type="percent_balance"),
        symbol_universe_mode="custom", symbols=["ETHUSDT"],
    )
    asyncio.run(deploy_auto_pilot(object(), request, {"id": "owner"}))
    payload = proxy.call_args.kwargs["json_body"]
    assert payload["allocation"]["allocation_type"] == "percent_balance"
    assert payload["allocation"]["trade_amount_per_position"] == 10
    assert payload["allocation"]["total_capital_budget"] == 500
    assert payload["symbol_universe_mode"] == "custom"
    assert payload["symbols"] == ["ETHUSDT"]
    assert "credentials" not in payload


def test_proxy_rejects_invalid_percentage_before_downstream_call():
    with pytest.raises(ValueError):
        AllocationParams(total_capital_budget=500, trade_amount_per_position=101, allocation_type="percent_balance")
