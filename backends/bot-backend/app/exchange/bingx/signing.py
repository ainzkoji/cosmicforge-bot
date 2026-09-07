import hmac
import hashlib

def sign_bingx(api_secret: str, payload_str: str) -> str:
    """
    Generate BingX V2 HMAC-SHA256 signature.
    
    Args:
        api_secret (str): The user's API Secret.
        payload_str (str): The sorted query string (e.g. "foo=bar&timestamp=123").
        
    Returns:
        str: The hex-encoded signature.
    """
    return hmac.new(
        api_secret.encode("utf-8"),
        payload_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def get_timestamp() -> int:
    """Return current timestamp in milliseconds."""
    import time
    return int(time.time() * 1000)
