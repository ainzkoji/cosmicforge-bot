import sqlite3
import os
import requests
import time
import hmac
import hashlib

def get_signature(api_secret, query_string):
    return hmac.new(
        api_secret.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

def close_btc():
    # Read DB
    db_path = "data/bot.db"
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT api_key, api_secret FROM broker_credentials WHERE broker_id='binance'")
    row = c.fetchone()
    if not row:
        print("No creds found")
        return
    
    api_key, api_secret = row
    
    # Binance request
    base_url = "https://fapi.binance.com"
    endpoint = "/fapi/v1/order"
    
    timestamp = int(time.time() * 1000)
    # First get position amt
    pos_query = f"symbol=BTCUSDT&timestamp={timestamp}"
    pos_sig = get_signature(api_secret, pos_query)
    
    headers = {"X-MBX-APIKEY": api_key}
    
    r = requests.get(f"{base_url}/fapi/v2/positionRisk?{pos_query}&signature={pos_sig}", headers=headers)
    pos_data = r.json()
    
    amt = 0.0
    if isinstance(pos_data, list):
        for p in pos_data:
            a = float(p.get("positionAmt", 0))
            if abs(a) > 0:
                amt = a
                break
                
    if amt == 0:
        print("BTC position is already closed or 0.")
        return
        
    print(f"Current BTC info: {amt}")
    side = "SELL" if amt > 0 else "BUY"
    qty = abs(amt)
    
    # Close it
    timestamp = int(time.time() * 1000)
    query = f"symbol=BTCUSDT&side={side}&type=MARKET&quantity={qty}&reduceOnly=true&timestamp={timestamp}"
    sig = get_signature(api_secret, query)
    
    r2 = requests.post(f"{base_url}{endpoint}?{query}&signature={sig}", headers=headers)
    print("Close response:", r2.json())

if __name__ == "__main__":
    close_btc()
