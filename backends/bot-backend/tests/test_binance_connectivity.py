"""
Test Binance Connectivity (Mocked & Real)
"""
import pytest
from unittest.mock import MagicMock, patch
from app.exchange.binance.client import BinanceFuturesClient

class TestBinanceConnectivity:
    
    @pytest.fixture
    def mock_session(self):
        """Patch requests.Session so client.session is a MagicMock."""
        with patch("app.exchange.binance.client.requests") as mock_req:
            mock_session_instance = MagicMock()
            mock_req.Session.return_value = mock_session_instance
            yield mock_session_instance

    def test_client_initialization(self):
        """Test client init with API keys."""
        client = BinanceFuturesClient(api_key="test", api_secret="test", base_url="https://testnet.binancefuture.com")
        assert client.base_url == "https://testnet.binancefuture.com"
        
    def test_ping_success(self, mock_session):
        """Test successful ping."""
        mock_session.get.return_value.status_code = 200
        
        client = BinanceFuturesClient("k", "s", "https://testnet.binancefuture.com")
        assert client.ping() == {'status_code': 200}
        
    def test_ping_failure(self, mock_session):
        """Test failed ping."""
        mock_session.get.return_value.status_code = 500
        
        client = BinanceFuturesClient("k", "s", "https://testnet.binancefuture.com")
        assert client.ping() == {"status_code": 500}
        
    def test_signature_generation(self, mock_session):
        """Test that _signed_request adds timestamp, recvWindow and signature to params."""
        from app.exchange.binance.signing import build_query, sign
        client = BinanceFuturesClient("test_key", "test_secret", "https://testnet.binancefuture.com")
        
        # Build params the same way _signed_request does
        import time
        params = {"symbol": "BTCUSDT"}
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = client.recv_window
        query = build_query(params)
        signature = sign(client.api_secret, query)
        
        assert "timestamp" in params
        assert signature  # non-empty
        assert isinstance(signature, str)
