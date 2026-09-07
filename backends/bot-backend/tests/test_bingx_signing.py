import pytest
import hmac
import hashlib
from app.exchange.bingx.signing import sign_bingx, get_timestamp

def test_bingx_signature_generation():
    """
    Verify HMAC SHA256 signature generation matches expected output.
    Ref: BingX API Docs
    """
    secret = "test_secret_key"
    query_string = "symbol=BTC-USDT&timestamp=1678888888000"
    
    # Expected: HMAC-SHA256(secret, query_string).hexdigest()
    expected_sig = hmac.new(
        secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    
    generated_sig = sign_bingx(secret, query_string)
    
    assert generated_sig == expected_sig
    assert len(generated_sig) == 64

def test_bingx_timestamp_format():
    """
    Verify timestamp is in milliseconds (13 digits).
    """
    ts = get_timestamp()
    assert isinstance(ts, int)
    assert len(str(ts)) == 13
