import pytest
from unittest.mock import MagicMock
from app.exchange.bingx.client import BingXClient

@pytest.fixture
def bingx_client():
    return BingXClient(api_key="key", api_secret="secret", testnet=True)

def test_account_normalization(bingx_client):
    """
    Test that BingX account balance response is normalized correctly.
    """
    # Mock raw response
    mock_response = {
        "code": 0,
        "data": {
            "balance": {
                "balance": "1000.50",
                "equity": "1050.75",
                "availableMargin": "900.25",
                "unrealisedPNL": "50.25"
            }
        }
    }
    
    bingx_client._request = MagicMock(return_value=mock_response)
    
    account = bingx_client.account()
    
    assert account["totalWalletBalance"] == 1000.50
    assert account["totalMarginBalance"] == 1050.75
    assert account["availableBalance"] == 900.25
    assert account["totalUnrealizedProfit"] == 50.25

def test_position_risk_normalization(bingx_client):
    """
    Test position risk normalization (signed amounts).
    """
    mock_response = {
        "code": 0,
        "data": [
            {
                "symbol": "BTC-USDT",
                "positionAmt": "0.5",
                "positionSide": "LONG",
                "avgPrice": "50000",
                "unrealisedPNL": "100"
            },
            {
                "symbol": "ETH-USDT",
                "positionAmt": "2.0",
                "positionSide": "SHORT",
                "avgPrice": "3000",
                "unrealisedPNL": "-50"
            }
        ]
    }
    
    bingx_client._request = MagicMock(return_value=mock_response)
    
    positions = bingx_client.position_risk()
    
    assert len(positions) == 2
    
    # Check Long
    btc = next(p for p in positions if p["symbol"] == "BTC-USDT")
    assert btc["positionAmt"] == 0.5  # Positive
    assert btc["bingx_side"] == "LONG"
    
    # Check Short
    eth = next(p for p in positions if p["symbol"] == "ETH-USDT")
    assert eth["positionAmt"] == -2.0 # Negative
    assert eth["bingx_side"] == "SHORT"

def test_klines_ordering(bingx_client):
    """
    Test kline ordering (should be ascending).
    """
    # Mock response (Descending from API typically)
    mock_response = {
        "code": 0,
        "data": [
            {"time": 2000, "open": "2", "close": "2", "high": "2", "low": "2", "volume": "100"},
            {"time": 1000, "open": "1", "close": "1", "high": "1", "low": "1", "volume": "100"}
        ]
    }
    
    bingx_client._request = MagicMock(return_value=mock_response)
    
    klines = bingx_client.klines("BTC-USDT")
    
    # Should be reversed to Ascending
    assert klines[0][0] == 1000
    assert klines[1][0] == 2000
