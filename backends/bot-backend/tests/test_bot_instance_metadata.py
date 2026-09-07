import pytest
from app.models.bot_instance_models import BotInstance
import json

def test_to_dict_includes_debug_fields():
    # Create a dummy BotInstance with all debug fields populated
    bot = BotInstance(
        id="bot_debug_123",
        user_id="user_123",
        broker_account_id="brk_123",
        market_type="CRYPTO",
        strategy_id="test_strat",
        strategy_version="1.0",
        risk_level="conservative",
        symbols=["BTCUSDT"],
        timeframes=["1h"],
        allocation_type="percent_balance",
        allocation_value=10.0,
        mode="live",
        status="active",
        created_at="2023-11-01T10:00:00Z",
        updated_at="2023-11-01T10:00:00Z",
        last_run_at="2023-11-01T10:05:00Z",
        last_error="Some error",
        total_trades=5,
        active_positions=1,
        broker_id="binance",
        broker_health_status="broker_blocked",
        broker_error_code="INVALID_API_KEY",
        broker_blocked_at="2023-11-01T10:10:00Z",
        block_category="broker_auth_failure",
        block_reason_code="INVALID_API_KEY",
        block_reason_detail="[INVALID_API_KEY] API key is invalid",
        blocked_since="2023-11-01T10:10:00Z",
        last_validated_at="2023-11-01T10:10:00Z",
        last_validation_error="API key is invalid"
    )

    # Serialize to dict
    data = bot.to_dict()

    # Verify ID standard fields
    assert data["id"] == "bot_debug_123"
    assert data["broker_account_id"] == "brk_123"
    assert data["status"] == "active"
    assert data["mode"] == "live"
    
    # Verify standard debug fields
    assert data["last_run_at"] == "2023-11-01T10:05:00Z"
    assert data["created_at"] == "2023-11-01T10:00:00Z"
    assert data["broker_health_status"] == "broker_blocked"
    
    # Verify exact block fields required for UI support/debug section
    assert data["block_category"] == "broker_auth_failure"
    assert data["block_reason_code"] == "INVALID_API_KEY"
    assert data["block_reason_detail"] == "[INVALID_API_KEY] API key is invalid"
    assert data["blocked_since"] == "2023-11-01T10:10:00Z"
    assert data["last_validated_at"] == "2023-11-01T10:10:00Z"
    assert data["last_validation_error"] == "API key is invalid"
