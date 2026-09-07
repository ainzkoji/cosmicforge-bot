import sqlite3
import os
import sys
import time
from datetime import datetime, timedelta

# Add backends to path so we can import shared_lib
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backends")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backends/shared")))

from shared_lib.persistence.analytics_service import AnalyticsService
from shared_lib.persistence.db import DB
from shared_lib.persistence.trade_tracker import TradeStatus

def setup_mock_data(db_path):
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass
        
    # Initialize DB schema
    conn = sqlite3.connect(db_path)
    
    # Create trades table (minimal)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            run_id TEXT,
            symbol TEXT,
            side TEXT,
            strategy TEXT,
            mode TEXT,
            timeframe TEXT,
            entry_time TEXT,
            entry_price REAL,
            entry_qty REAL,
            entry_confidence REAL,
            exit_time TEXT,
            exit_price REAL,
            exit_reason TEXT,
            realized_pnl REAL,
            fees REAL,
            r_multiple REAL,
            initial_stop REAL,
            tp1_hit INTEGER,
            tp1_time TEXT,
            add_count INTEGER,
            status TEXT,
            created_at TEXT
        )
    """)
    
    # Insert mock closed trades for PnL
    # Trade 1: Win 100 USDT, Month: Current
    conn.execute("""
        INSERT INTO trades (trade_id, status, realized_pnl, exit_time, entry_price, entry_qty, symbol)
        VALUES ('t1', 'CLOSED', 100.0, ?, 1000, 1, 'BTCUSDT')
    """, (datetime.utcnow().isoformat(),))
    
    # Trade 2: Loss 50 USDT, Month: Last Month
    # Note: ensure this is > 30 days ago for Volatility testing if we check strict filtering
    last_month = datetime.utcnow() - timedelta(days=40) 
    conn.execute("""
        INSERT INTO trades (trade_id, status, realized_pnl, exit_time, entry_price, entry_qty, symbol)
        VALUES ('t2', 'CLOSED', -50.0, ?, 1000, 1, 'ETHUSDT')
    """, (last_month.isoformat(),))
    
    # Insert mock open trades for Allocation
    # Trade 3: Open BTC Position, 2000 USDT value
    conn.execute("""
        INSERT INTO trades (trade_id, status, entry_price, entry_qty, symbol)
        VALUES ('t3', 'OPEN', 50000, 0.04, 'BTCUSDT')
    """) # 2000 USDT
    
    # Trade 4: Open ETH Position, 1000 USDT value
    conn.execute("""
        INSERT INTO trades (trade_id, status, entry_price, entry_qty, symbol)
        VALUES ('t4', 'OPEN', 3000, 0.3333, 'ETHUSDT')
    """) # ~1000 USDT
    
    conn.commit()
    conn.close()
    return db_path

def verify():
    # Use unique DB name to avoid locks
    db_path = f"test_analytics_{int(time.time())}.db"
    setup_mock_data(db_path)
    
    # Create DB wrapper pointing to test db
    class MockDB:
        def connect(self):
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            return conn
            
    analytics = AnalyticsService(db=MockDB())
    
    print("--- Verifying Analytics Service ---")
    
    # 1. Total Stats
    stats = analytics.get_total_stats("u1")
    print(f"Total Stats: {stats}")
    assert stats["total_trades"] == 2
    assert stats["total_profit"] == 50.0  # 100 - 50
    assert stats["win_rate"] == 50.0
    
    # 2. Monthly PnL
    monthly = analytics.get_monthly_pnl("u1")
    print(f"Monthly PnL: {monthly}")
    # Should have 2 entries
    assert len(monthly) == 2
    
    # 3. Asset Allocation
    alloc = analytics.get_asset_allocation("u1")
    print(f"Asset Allocation: {alloc}")
    # Total value approx 3000. BTC=2000 (66%), ETH=1000 (33%)
    btc_alloc = next(x for x in alloc if x["symbol"] == "BTCUSDT")
    assert 60 < btc_alloc["percent"] < 70
    
    # 4. Risk Metrics
    metrics = analytics.get_risk_metrics("u1", timeframe="ALL")
    print(f"Risk Metrics (ALL): {metrics}")
    
    # Max Drawdown Logic:
    # Trades sorted by time:
    # 1. t2 (-40 days ago): PnL -50. Equity 0 -> -50. Peak 0. DD 50.
    # 2. t1 (Today): PnL +100. Equity -50 -> +50. Peak 50. DD 0.
    # Max DD should be 50.0
    assert metrics["max_drawdown"] == 50.0
    
    # 5. Risk Metrics (1M) - Should exclude the old trade
    metrics_1m = analytics.get_risk_metrics("u1", timeframe="1M")
    print(f"Risk Metrics (1M): {metrics_1m}")
    # Should only see t1 (+100).
    # Equity 0 -> +100. Peak 100. DD 0.
    assert metrics_1m["max_drawdown"] == 0.0
    
    print("✅ Verification Passed")
    
    # Cleanup
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
    except Exception as e:
        print(f"Warning: Failed to cleanup {db_path}: {e}")

if __name__ == "__main__":
    verify()
