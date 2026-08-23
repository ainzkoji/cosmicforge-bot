"""
Unit Tests for Forex Auto-Pilot Support

Tests the minimal changes to enable FOREX market deployments through Auto-Pilot
while ensuring CRYPTO behavior remains unchanged.
"""
import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from app.core.bot_instance_service import BotInstanceService
from app.models.bot_instance_models import BotInstance


class TestAutoPilotForexSupport:
    """Test suite for Forex Auto-Pilot functionality."""
    
    @pytest.fixture
    def mock_db(self):
        """Create mock database."""
        db = Mock()
        conn = Mock()
        db.connect = Mock(return_value=conn)
        conn.__enter__ = Mock(return_value=conn)
        conn.__exit__ = Mock(return_value=None)
        conn.execute = Mock()
        return db
    
    @pytest.fixture
    def service(self, mock_db):
        """Create BotInstanceService with mocked DB."""
        return BotInstanceService(db=mock_db)
    
    def test_crypto_deployment_unchanged(self, service, mock_db):
        """
        Test that CRYPTO deployment still uses TRADE_SYMBOLS from env.
        This ensures backward compatibility - crypto behavior must not change.
        """
        # Mock settings.TRADE_SYMBOLS at config module level
        with patch('app.core.config.settings.TRADE_SYMBOLS', "BTCUSDT,ETHUSDT,XRPUSDT"):
            
            # Mock the create_bot_instance to avoid DB interaction
            with patch.object(service, 'create_bot_instance') as mock_create:
                mock_instance = BotInstance(
                    id="bot_test123",
                    user_id="user_123",
                    broker_account_id="brk_abc",
                    market_type="crypto",
                    strategy_id="master_ensemble",
                    strategy_version="1.0.0",
                    risk_level="balanced",
                    symbols=["BTCUSDT", "ETHUSDT", "XRPUSDT"],
                    timeframes=["15m"],
                    allocation_type="fixed_amount",
                    allocation_value=1000.0,
                    mode="paper",
                    status="active",
                    created_at="2026-02-04T23:00:00Z",
                    updated_at="2026-02-04T23:00:00Z"
                )
                mock_create.return_value = mock_instance
                
                # Deploy CRYPTO Auto-Pilot
                instances = service.deploy_auto_pilot(
                    user_id="user_123",
                    risk_level="balanced",
                    allocation_type="fixed_amount",
                    allocation_value=1000.0,
                    broker_account_ids=["brk_abc"],
                    mode="paper",
                    market_type="crypto"
                )
                
                # Verify deployment succeeded
                assert len(instances) == 1
                assert instances[0].market_type == "crypto"
                
                # Verify create_bot_instance was called with TRADE_SYMBOLS
                assert mock_create.called
                call_args = mock_create.call_args[0][0]  # Get CreateBotInstanceRequest
                assert call_args.symbols == ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
                assert call_args.market_type == "crypto"
    
    def test_forex_deployment_with_allowlist(self, service, mock_db):
        """
        Test that FOREX deployment uses forex_config.allowlist.
        This is the primary path for Forex Auto-Pilot.
        """
        forex_pairs = ["EUR_USD", "GBP_USD", "USD_JPY"]
        
        with patch('app.core.config.settings.TRADE_SYMBOLS', "BTCUSDT,ETHUSDT"):  # Should be ignored for FOREX
            
            with patch.object(service, 'create_bot_instance') as mock_create:
                mock_instance = BotInstance(
                    id="bot_forex123",
                    user_id="user_123",
                    broker_account_id="brk_oanda",
                    market_type="forex",
                    strategy_id="master_ensemble",
                    strategy_version="1.0.0",
                    risk_level="conservative",
                    symbols=forex_pairs,
                    timeframes=["15m"],
                    allocation_type="fixed_amount",
                    allocation_value=5000.0,
                    mode="paper",
                    status="active",
                    created_at="2026-02-04T23:00:00Z",
                    updated_at="2026-02-04T23:00:00Z"
                )
                mock_create.return_value = mock_instance
                
                # Deploy FOREX Auto-Pilot with allowlist
                instances = service.deploy_auto_pilot(
                    user_id="user_123",
                    risk_level="conservative",
                    allocation_type="fixed_amount",
                    allocation_value=5000.0,
                    broker_account_ids=["brk_oanda"],
                    mode="paper",
                    market_type="FOREX",
                    forex_config={"allowlist": forex_pairs}
                )
                
                # Verify deployment succeeded
                assert len(instances) == 1
                assert instances[0].market_type == "forex"  # BotInstance normalizes to lowercase
                
                # Verify create_bot_instance was called with forex allowlist
                assert mock_create.called
                call_args = mock_create.call_args[0][0]
                assert call_args.symbols == forex_pairs
                assert call_args.market_type == "FOREX"  # Request uses uppercase
    
    def test_forex_deployment_with_env_fallback(self, service, mock_db):
        """
        Test FOREX deployment falls back to FOREX_SYMBOLS env when no allowlist provided.
        """
        with patch('app.core.config.settings.FOREX_SYMBOLS', "EUR_USD,GBP_USD"):
            
            with patch.object(service, 'create_bot_instance') as mock_create:
                mock_instance = BotInstance(
                    id="bot_forex_env",
                    user_id="user_123",
                    broker_account_id="brk_oanda",
                    market_type="forex",
                    strategy_id="master_ensemble",
                    strategy_version="1.0.0",
                    risk_level="balanced",
                    symbols=["EUR_USD", "GBP_USD"],
                    timeframes=["15m"],
                    allocation_type="fixed_amount",
                    allocation_value=3000.0,
                    mode="paper",
                    status="active",
                    created_at="2026-02-04T23:00:00Z",
                    updated_at="2026-02-04T23:00:00Z"
                )
                mock_create.return_value = mock_instance
                
                # Deploy FOREX without allowlist (uses env fallback)
                instances = service.deploy_auto_pilot(
                    user_id="user_123",
                    risk_level="balanced",
                    allocation_type="fixed_amount",
                    allocation_value=3000.0,
                    broker_account_ids=["brk_oanda"],
                    mode="paper",
                    market_type="FOREX",
                    forex_config=None  # No allowlist provided
                )
                
                assert len(instances) == 1
                call_args = mock_create.call_args[0][0]
                assert call_args.symbols == ["EUR_USD", "GBP_USD"]
    
    def test_forex_deployment_validation_error(self, service):
        """
        Test that FOREX deployment raises ValueError when no allowlist and no env config.
        """
        with patch('app.core.config.settings.FOREX_SYMBOLS', ""):  # Empty env
            
            # Deploy FOREX without allowlist or env should fail
            with pytest.raises(ValueError) as exc_info:
                service.deploy_auto_pilot(
                    user_id="user_123",
                    risk_level="balanced",
                    allocation_type="fixed_amount",
                    allocation_value=1000.0,
                    broker_account_ids=["brk_oanda"],
                    mode="paper",
                    market_type="FOREX",
                    forex_config=None
                )
            
            # Verify error message is clear
            assert "No forex allowlist provided" in str(exc_info.value)
    
    def test_crypto_deployment_validation_error(self, service):
        """
        Test that CRYPTO deployment raises ValueError when TRADE_SYMBOLS is empty.
        This ensures we removed the hardcoded fallback.
        """
        with patch('app.core.config.settings.TRADE_SYMBOLS', ""):  # Empty env
            
            with pytest.raises(ValueError) as exc_info:
                service.deploy_auto_pilot(
                    user_id="user_123",
                    risk_level="balanced",
                    allocation_type="fixed_amount",
                    allocation_value=1000.0,
                    broker_account_ids=["brk_binance"],
                    mode="paper",
                    market_type="crypto"
                )
            
            assert "No crypto symbols configured" in str(exc_info.value)
    
    def test_runner_reads_symbols_json_unchanged(self, mock_db):
        """
        Regression test: Ensure runner still reads symbols_json from DB correctly.
        No changes should be made to runner - it should work with both crypto and forex symbols.
        """
        # Mock DB query that runner would execute
        mock_row = {
            "id": "bot_123",
            "user_id": "user_123",
            "broker_account_id": "brk_abc",
            "market_type": "FOREX",
            "strategy_id": "master_ensemble",
            "strategy_version": "1.0.0",
            "risk_level": "balanced",
            "config_id": None,
            "risk_profile_id": None,
            "symbols_json": json.dumps(["EUR_USD", "GBP_USD"]),  # Forex symbols
            "timeframes_json": json.dumps(["15m"]),
            "allocation_type": "fixed_amount",
            "allocation_value": 5000.0,
            "capital_allocation": None,
            "capital_allocation_type": "fixed_amount",
            "mode": "paper",
            "status": "active",
            "created_at": "2026-02-04T23:00:00Z",
            "updated_at": "2026-02-04T23:00:00Z",
            "started_at": "2026-02-04T23:00:00Z",
            "stopped_at": None,
            "last_run_at": None,
            "last_error": None,
            "total_trades": 0,
            "active_positions": 0,
            "broker_id": "oanda"
        }
        
        # Test BotInstance deserialization (used by runner)
        instance = BotInstance.from_db_row(mock_row)
        
        # Verify symbols are correctly deserialized
        assert instance.symbols == ["EUR_USD", "GBP_USD"]
        assert instance.market_type == "FOREX"  # DB stores uppercase
        
        # Verify this works for crypto too
        mock_row["market_type"] = "CRYPTO"
        mock_row["symbols_json"] = json.dumps(["BTCUSDT", "ETHUSDT"])
        mock_row["broker_id"] = "binance"
        
        instance_crypto = BotInstance.from_db_row(mock_row)
        assert instance_crypto.symbols == ["BTCUSDT", "ETHUSDT"]
        assert instance_crypto.market_type == "CRYPTO"  # DB stores uppercase


class TestAutoPilotRequestValidation:
    """Test request model validation for Forex Auto-Pilot."""
    
    def test_forex_request_requires_allowlist(self):
        """Test that DeployAutoPilotRequest validates forex_config for FOREX market."""
        from app.api.auto_pilot import DeployAutoPilotRequest
        from pydantic import ValidationError
        
        # FOREX without forex_config should fail
        with pytest.raises(ValidationError) as exc_info:
            DeployAutoPilotRequest(
                risk_level="balanced",
                allocation_type="fixed_amount",
                allocation_value=1000.0,
                capital_allocation=5000.0,
                broker_account_ids=["brk_oanda"],
                mode="paper",
                market_type="forex",
                forex_config=None  # Missing required config
            )
        
        assert "forex_config is required" in str(exc_info.value)
    
    def test_forex_request_validates_allowlist(self):
        """Test that forex_config.allowlist must be non-empty list."""
        from app.api.auto_pilot import DeployAutoPilotRequest
        from pydantic import ValidationError
        
        # Empty allowlist should fail
        with pytest.raises(ValidationError) as exc_info:
            DeployAutoPilotRequest(
                risk_level="balanced",
                allocation_type="fixed_amount",
                allocation_value=1000.0,
                capital_allocation=5000.0,
                broker_account_ids=["brk_oanda"],
                mode="paper",
                market_type="forex",
                forex_config={"allowlist": []}  # Empty list
            )
        
        assert "must be a non-empty list" in str(exc_info.value)
    
    def test_crypto_request_no_forex_config_needed(self):
        """Test that CRYPTO requests don't require forex_config."""
        from app.api.auto_pilot import DeployAutoPilotRequest
        
        # CRYPTO without forex_config should succeed
        request = DeployAutoPilotRequest(
            risk_level="balanced",
            allocation_type="fixed_amount",
            allocation_value=1000.0,
            capital_allocation=5000.0,
            broker_account_ids=["brk_binance"],
            mode="paper",
            market_type="crypto"
            # forex_config not provided
        )
        
        assert request.market_type == "crypto"
        assert request.forex_config is None
    
    def test_valid_forex_request(self):
        """Test valid FOREX request with allowlist."""
        from app.api.auto_pilot import DeployAutoPilotRequest
        
        request = DeployAutoPilotRequest(
            risk_level="conservative",
            allocation_type="fixed_amount",
            allocation_value=5000.0,
            capital_allocation=20000.0,
            broker_account_ids=["brk_oanda"],
            mode="paper",
            market_type="forex",
            forex_config={"allowlist": ["EUR_USD", "GBP_USD", "USD_JPY"]}
        )
        
        assert request.market_type == "forex"
        assert request.forex_config["allowlist"] == ["EUR_USD", "GBP_USD", "USD_JPY"]
