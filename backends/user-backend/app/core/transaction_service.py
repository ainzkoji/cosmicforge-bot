
import json
from typing import Dict, Any, List
from shared_lib.persistence.db import DB
from app.core.broker_service import list_user_broker_accounts
from app.core.broker_security import decrypt_credentials
from app.exchange.binance_client import BinanceClient

def get_portfolio_transactions(user_id: str, limit: int = 50) -> Dict[str, Any]:
    """
    Fetches recent deposit/withdrawal transactions from all connected brokers.
    """
    accounts = list_user_broker_accounts(user_id)
    
    all_transactions: List[Dict[str, Any]] = []
    
    db = DB()
    with db.connect() as conn:
        for acc in accounts:
            if acc["status"] != "connected":
                continue
                
            # Get credentials
            cred_row = conn.execute(
                "SELECT encrypted_blob FROM broker_credentials WHERE account_id = ?", 
                (acc["id"],)
            ).fetchone()
            
            if not cred_row:
                continue
                
            creds = decrypt_credentials(cred_row["encrypted_blob"])
            
            # Fetch data based on broker
            # Fetch data based on broker
            if acc["broker_id"] == "binance":
                try:
                    client = BinanceClient(
                        api_key=creds.get("api_key"), 
                        api_secret=creds.get("api_secret"),
                        testnet=(acc.get("environment") != "live")
                    )
                    
                    # Fetch income/transaction history
                    # Binance Futures: /fapi/v1/income
                    # incomeType: TRANSFER (deposits/withdrawals)
                    # Use _signed_get instead of incorrect _request(signed=True)
                    raw_income = client.client._signed_get(
                        '/fapi/v1/income',
                        {
                            'incomeType': 'TRANSFER',
                            'limit': limit
                        }
                    )
                    
                    # Ensure raw_income is a list (Binance sometimes returns dict on error, but _signed_get might raise)
                    if isinstance(raw_income, list):
                        for item in raw_income:
                            amount = float(item.get("income", 0))
                            all_transactions.append({
                                "account_id": acc["id"],
                                "broker": "Binance",
                                "type": "DEPOSIT" if amount > 0 else "WITHDRAWAL",
                                "asset": item.get("asset", "USDT"),
                                "amount": abs(amount),
                                "status": "SUCCESS",
                                "timestamp": int(item.get("time", 0)),
                                "tx_id": item.get("tranId", "")
                            })
                        
                except Exception as e:
                    print(f"[Transactions] Error fetching data for Binance {acc['id']}: {e}")

            elif acc["broker_id"] == "bybit":
                try:
                    from app.exchange.bybit.client import BybitClient
                    client = BybitClient(
                        api_key=creds.get("api_key"), 
                        api_secret=creds.get("api_secret"),
                        testnet=(acc.get("environment") != "live")
                    )
                    
                    logs = client.transaction_log(limit=limit)
                    
                    for item in logs:
                         # Bybit V5 structure: { type: "TRANSFER", change: "0.1", coin: "USDT", transactionTime: "..." }
                         try:
                             amount = float(item.get("change", 0))
                             if amount == 0: continue
                             
                             all_transactions.append({
                                "account_id": acc["id"],
                                "broker": "ByBit",
                                "type": "DEPOSIT" if amount > 0 else "WITHDRAWAL",
                                "asset": item.get("coin", "USDT"),
                                "amount": abs(amount),
                                "status": "SUCCESS",
                                "timestamp": int(item.get("transactionTime", 0)),
                                "tx_id": item.get("transactionId", "")
                             })
                         except:
                             continue
                             
                except Exception as e:
                     print(f"[Transactions] Error fetching data for Bybit {acc['id']}: {e}")
    
    # Sort by timestamp descending
    all_transactions.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    
    return {
        "transactions": all_transactions[:limit]
    }
