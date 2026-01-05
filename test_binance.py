from dotenv import load_dotenv

load_dotenv()

import os
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode


def sign(secret: str, query_string: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha1006
    ).hexdigest()


key = os.getenv("BINANCE_API_KEY", "").strip()
secret = os.getenv("BINANCE_API_SECRET", "").strip()
base = os.getenv("BINANCE_FAPI_BASE_URL", "https://demo-fapi.binance.com").strip()
recv = os.getenv("BINANCE_RECV_WINDOW", "5000").strip()

if not key or not secret:
    raise SystemExit("Missing BINANCE_API_KEY or BINANCE_API_SECRET")

ts = int(time.time() * 1000)

params = {
    "timestamp": ts,
    "recvWindow": int(recv),
}

query = urlencode(params, doseq=True)
signature = sign(secret, query)

url = f"{base}/fapi/v2/account?{query}&signature={signature}"

r = requests.get(url, headers={"X-MBX-APIKEY": key}, timeout=10)
print(r.status_code)
print(r.text[:800])
