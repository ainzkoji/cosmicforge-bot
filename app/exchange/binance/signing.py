import hmac
import hashlib
from urllib.parse import urlencode


def build_query(params: dict) -> str:
    return urlencode(params, doseq=True)


def sign(secret: str, query_string: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
