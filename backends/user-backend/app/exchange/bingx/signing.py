import hmac
import hashlib
import time

def sign_bingx(api_secret: str, payload_str: str) -> str:
    """
    Generate BingX V2 HMAC-SHA256 signature.
    """
    return hmac.new(
        api_secret.encode("utf-8"),
        payload_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def get_timestamp() -> int:
    return int(time.time() * 1000)
