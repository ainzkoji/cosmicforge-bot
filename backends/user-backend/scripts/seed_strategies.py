import sys
import os
import json
import uuid
from datetime import datetime

# Add backend to path so we can import app modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from shared_lib.persistence.db import DB, utc_now_iso

def seed_strategies():
    print("Connecting to DB...")
    db = DB()
    
    strategies = [
        {
            "name": "MACD Trend Follower",
            "description": "A classic trend-following strategy that enters long when the MACD line crosses above the signal line, and exits when it crosses below.",
            "type": "official",
            "market_types": ["crypto", "forex"],
            "tags": ["trend", "momentum"],
            "risk": "moderate",
            "tier": "free",
            "spec": {
                "mode": "standard_v1",
                "indicators": [
                    {"id": "macd", "type": "MACD", "fast": 12, "slow": 26, "signal": 9}
                ],
                "entry_conditions": [
                    {"left": "macd.line", "operator": "crosses_above", "right": "macd.signal"}
                ],
                "exit_conditions": [
                    {"left": "macd.line", "operator": "crosses_below", "right": "macd.signal"}
                ]
            }
        },
        {
            "name": "RSI Mean Reversion",
            "description": "Buys when the asset is oversold (RSI < 30) and sells when it becomes overbought (RSI > 70). Best for ranging markets.",
            "type": "official",
            "market_types": ["crypto"],
            "tags": ["mean-reversion", "scalping"],
            "risk": "aggressive",
            "tier": "pro",
            "spec": {
                "mode": "standard_v1",
                "indicators": [
                    {"id": "rsi", "type": "RSI", "period": 14}
                ],
                "entry_conditions": [
                    {"left": "rsi.value", "operator": "less_than", "right": 30}
                ],
                "exit_conditions": [
                    {"left": "rsi.value", "operator": "greater_than", "right": 70}
                ]
            }
        },
        {
            "name": "Golden Cross",
            "description": "Long-term trend strategy. Enters when the 50-day SMA crosses above the 200-day SMA.",
            "type": "official",
            "market_types": ["stocks", "crypto"],
            "tags": ["trend", "long-term"],
            "risk": "conservative",
            "tier": "free",
            "spec": {
                "mode": "standard_v1",
                "indicators": [
                    {"id": "sma50", "type": "SMA", "period": 50},
                    {"id": "sma200", "type": "SMA", "period": 200}
                ],
                "entry_conditions": [
                    {"left": "sma50", "operator": "crosses_above", "right": "sma200"}
                ],
                "exit_conditions": [
                    {"left": "sma50", "operator": "crosses_below", "right": "sma200"}
                ]
            }
        }
    ]

    with db.connect() as conn:
        for s in strategies:
            strat_id = f"strat_{uuid.uuid4().hex[:8]}"
            now = utc_now_iso()
            
            print(f"Seeding: {s['name']}...")
            
            # Check if exists by name to avoid dupes on re-run
            existing = conn.execute("SELECT id FROM strategies WHERE name = ?", (s['name'],)).fetchone()
            if existing:
                print(f"  Skipping {s['name']} (already exists)")
                continue

            conn.execute(
                """
                INSERT INTO strategies (
                    id, owner_id, visibility, status, name, description, 
                    market_types, timeframes, tags, entitlement_tier, 
                    recommended_risk_style, constraints_json, metrics_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    strat_id,
                    None, # System/Official
                    "official",
                    "active",
                    s["name"],
                    s["description"],
                    json.dumps(s["market_types"]),
                    json.dumps(["1h", "4h", "1d"]),
                    json.dumps(s["tags"]),
                    s["tier"],
                    s["risk"],
                    json.dumps({}),
                    json.dumps({"win_rate": 0, "total_trades": 0}),
                    now,
                    now
                )
            )
            
            conn.execute(
                """
                INSERT INTO strategy_versions (
                    id, strategy_id, version_number, spec_json, param_schema_json, changelog, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"{strat_id}_v1",
                    strat_id,
                    1,
                    json.dumps(s["spec"]),
                    json.dumps({}),
                    "Initial Release",
                    now
                )
            )
            
        conn.commit()
    print("Done!")

if __name__ == "__main__":
    seed_strategies()
