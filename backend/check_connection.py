
import os
import time
import hmac
import hashlib
import json
import urllib.request
import urllib.parse
from app.core.config import settings

def get_signature(params, secret):
    query_string = urllib.parse.urlencode(params)
    return hmac.new(
        secret.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

def request_api(method, url, params=None, headers=None):
    if params:
        query = urllib.parse.urlencode(params)
        url = f"{url}?{query}"
    
    req = urllib.request.Request(url, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        return {"error": str(e), "body": e.read().decode()}
    except Exception as e:
        return {"error": str(e)}

def check_connection():
    key = settings.BINANCE_API_KEY
    secret = settings.BINANCE_API_SECRET
    base_url = settings.BINANCE_FAPI_BASE_URL
    
    print(f"--- DIAGNOSTICS ---")
    print(f"ENV: {settings.BINANCE_ENV}")
    print(f"URL: {base_url}")
    print(f"Key: {key[:4]}...{key[-4:] if key else 'None'}")
    
    if not key or not secret:
        print("ERROR: API Key or Secret missing!")
        return

    # 1. Check Server Time (No Auth)
    print("\n1. Checking Time...")
    res = request_api("GET", f"{base_url}/fapi/v1/time")
    if "serverTime" in res:
         print(f"Server Time: OK ({res['serverTime']})")
    else:
         print(f"Server Time: FAILED - {res}")

    # 2. Check Account Balance (Auth)
    print("\n2. Checking Account...")
    endpoint = "/fapi/v2/account"
    params = {
        "timestamp": int(time.time() * 1000),
        "recvWindow": 5000
    }
    params["signature"] = get_signature(params, secret)
    headers = {"X-MBX-APIKEY": key}
    
    data = request_api("GET", f"{base_url}{endpoint}", params=params, headers=headers)
    
    if "totalWalletBalance" in data:
        print("\n--- ACCOUNT DATA ---")
        print(f"Can Deposit: {data.get('canDeposit')}")
        print(f"Can Trade: {data.get('canTrade')}")
        print(f"Total Wallet Balance: {data.get('totalWalletBalance')}")
        print(f"Total Margin Balance: {data.get('totalMarginBalance')}")
        print(f"Available Balance: {data.get('availableBalance')}")
        
        assets = data.get("assets", [])
        usdt = next((a for a in assets if a["asset"] == "USDT"), None)
        if usdt:
            print("\n--- USDT ASSET ---")
            print(f"Wallet Balance: {usdt.get('walletBalance')}")
            print(f"Margin Balance: {usdt.get('marginBalance')}")
        else:
            print("\nWARNING: No USDT asset found in account!")
    else:
        print(f"\nAccount Check: FAILED")
        print(f"Response: {data}")

if __name__ == "__main__":
    check_connection()

