"""
Daily Equity Snapshot Scheduler

Runs in the background as a daemon thread.
Records equity snapshots for all active broker accounts at 00:00 UTC daily.
"""
import logging
import time
import threading
from datetime import datetime, timezone
from typing import Optional
from shared_lib.persistence.db import DB
from app.analytics.equity_snapshot_service import get_equity_snapshot_service

logger = logging.getLogger(__name__)

class DailySnapshotScheduler:
    def __init__(self, db: Optional[DB] = None):
        self.db = db or DB()
        self.snapshot_service = get_equity_snapshot_service(db)
        self.running = False
        self.thread: Optional[threading.Thread] = None
    
    def start(self):
        """Start the daily snapshot scheduler in a background thread."""
        if self.running:
            logger.warning("Daily snapshot scheduler already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True, name="DailySnapshotScheduler")
        self.thread.start()
        logger.info("Daily snapshot scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Daily snapshot scheduler stopped")
    
    def _run_loop(self):
        """Main scheduler loop - checks every 5 minutes if it's time to snapshot."""
        last_snapshot_date = None
        
        while self.running:
            try:
                now_utc = datetime.now(timezone.utc)
                current_date = now_utc.date()
                
                # Run at 00:00-00:05 UTC once per day
                if (now_utc.hour == 0 and now_utc.minute < 5 
                    and current_date != last_snapshot_date):
                    logger.info("Running daily equity snapshots for all active broker accounts")
                    
                    # 1. Update Benchmark Prices
                    try:
                        import asyncio
                        from app.analytics.benchmark_service import get_benchmark_service
                        logger.info("Updating benchmark prices...")
                        benchmark_service = get_benchmark_service()
                        # Run async method in this thread (since it's a daemon thread)
                        # or use asyncio.run if not in an event loop
                        asyncio.run(benchmark_service.update_benchmark_prices())
                    except Exception as e:
                        logger.error(f"Failed to update benchmark prices: {e}")
                    
                    # 2. Record Equity Snapshots
                    self._record_daily_snapshots()
                    
                    last_snapshot_date = current_date
                
                # Sleep for 5 minutes
                time.sleep(300)
                
            except Exception as e:
                logger.error(f"Error in daily snapshot scheduler: {e}")
                time.sleep(300)  # Continue running despite errors
    
    def _record_daily_snapshots(self):
        """Record snapshots for all active broker accounts."""
        try:
            # Get all active broker accounts
            with self.db.get_connection() as conn:
                rows = conn.execute(
                    """
                    SELECT DISTINCT 
                        ba.id as broker_account_id,
                        ba.user_id,
                        ba.broker_id,
                        bc.encrypted_blob
                    FROM broker_accounts ba
                    JOIN broker_credentials bc ON ba.id = bc.account_id
                    WHERE ba.status = 'active'
                    """
                ).fetchall()
            
            logger.info(f"Found {len(rows)} active broker accounts for daily snapshot")
            
            for row in rows:
                try:
                    # Build client for this broker account
                    from shared_lib.core.security.broker_security import decrypt_credentials
                    from app.exchange.factory import build_exchange_client_from_broker
                    
                    creds = decrypt_credentials(row["encrypted_blob"])
                    
                    # Build a minimal context just for snapshot
                    # We need to build the client based on broker_id
                    if row["broker_id"] == "binance":
                        from app.exchange.binance.client import BinanceFuturesClient
                        client = BinanceFuturesClient(
                            creds["api_key"],
                            creds["api_secret"],
                            base_url=creds.get("base_url", "https://fapi.binance.com")
                        )
                    elif row["broker_id"] == "bybit":
                        from app.exchange.bybit.client import BybitClient
                        client = BybitClient(
                            creds["api_key"],
                            creds["api_secret"],
                            base_url=creds.get("base_url", "https://api.bybit.com")
                        )
                    elif row["broker_id"] == "bingx":
                        from app.exchange.bingx.client import BingXClient
                        client = BingXClient(
                            creds["api_key"],
                            creds["api_secret"],
                            base_url=creds.get("base_url", "https://open-api.bingx.com")
                        )
                    else:
                        logger.warning(f"Unknown broker_id: {row['broker_id']}, skipping")
                        continue
                    
                    # Record snapshot
                    self.snapshot_service.record_snapshot(
                        user_id=row["user_id"],
                        broker_account_id=row["broker_account_id"],
                        broker_id=row["broker_id"],
                        client=client,
                        source="daily_snapshot",
                        bot_instance_id=None  # Daily snapshots are account-level, not bot-specific
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to record daily snapshot for {row['broker_account_id']}: {e}")
                    continue
            
            logger.info("Daily snapshot batch completed")
            
        except Exception as e:
            logger.error(f"Failed to fetch broker accounts for daily snapshot: {e}")


# Singleton instance
_scheduler_instance: Optional[DailySnapshotScheduler] = None

def get_daily_snapshot_scheduler(db: Optional[DB] = None) -> DailySnapshotScheduler:
    """Get or create the daily snapshot scheduler instance."""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = DailySnapshotScheduler(db)
    return _scheduler_instance

def start_daily_snapshot_scheduler(db: Optional[DB] = None):
    """Start the daily snapshot scheduler (call once at app startup)."""
    scheduler = get_daily_snapshot_scheduler(db)
    scheduler.start()
