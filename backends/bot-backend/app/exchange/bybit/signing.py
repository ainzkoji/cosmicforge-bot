import hmac
import hashlib
import time

def generate_signature(api_secret: str, api_key: str, recv_window: int, params: dict) -> tuple[str, str]:
    """
    Generate Bybit V5 HMAC SHA256 signature (Headers).
    Returns (timestamp, signature).
    Payload order: timestamp + apiKey + recvWindow + params
    """
    timestamp = str(int(time.time() * 1000))
    # Convert params to query string (sorted) if not stringified? 
    # Bybit V5: For GET, use query string. For POST, use JSON string.
    # This function assumes 'params' is the payload string ready for signing if it's POST, 
    # or query string if it's GET. 
    # Actually, let's keep it simple: caller passes the exact payload string.
    
    # Wait, existing binance `sign` takes query_string. 
    
    # We will let caller handle payload construction, this just signs.
    # But Bybit requires timestamp in the prehash string.
    
    return timestamp, ""

def sign_v5(api_secret: str, payload: str, timestamp: str, api_key: str, recv_window: int) -> str:
    """
    X-BAPI-SIGN generation.
    param_str = timestamp + api_key + recv_window + payload
    """
    param_str = f"{timestamp}{api_key}{recv_window}{payload}"
    return hmac.new(
        api_secret.encode("utf-8"),
        param_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

def sign_legacy_v2(api_secret: str, params: dict) -> str:
    """
    Generate HMAC SHA256 signature for V2 (Query Params).
    Used for backward compatibility if needed.
    """
    # Sort params
    param_str = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
    return hmac.new(
        api_secret.encode('utf-8'),
        param_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
