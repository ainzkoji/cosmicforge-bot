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
    
    def _record_daily_snapshots(self) -> dict:
        """Record one account-level snapshot per connected broker account.

        Credentials come ONLY from the canonical resolver (ownership, active
        credential version, environment, decryption) and the client ONLY from
        the canonical factory. A previous revision read the legacy
        broker_credentials table, filtered on status='active' (accounts are
        'connected'), defaulted every broker to its mainnet URL, and imported
        a non-existent factory function, so every snapshot failed silently.
        """
        from shared_lib.broker import BrokerResolverError, build_client_from_auth, resolve_broker_auth

        summary = {"accounts": 0, "recorded": 0, "failed": 0, "errors": {}}
        try:
            with self.db.connect() as conn:
                rows = conn.execute(
                    "SELECT id, user_id, broker_id FROM broker_accounts "
                    "WHERE LOWER(status) IN ('connected', 'active')"
                ).fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch broker accounts for daily snapshot: {e}")
            summary["errors"]["_query"] = str(e)
            return summary

        summary["accounts"] = len(rows)
        logger.info("Found %d connected broker accounts for daily snapshot", len(rows))
        for row in rows:
            account_id, user_id, broker_id = row["id"], row["user_id"], row["broker_id"]
            try:
                auth = resolve_broker_auth(account_id, user_id, self.db)
                client = build_client_from_auth(auth)
                self.snapshot_service.record_snapshot(
                    user_id=user_id,
                    broker_account_id=account_id,
                    broker_id=broker_id,
                    client=client,
                    source="daily_snapshot",
                    bot_instance_id=None,  # account-level, not bot-specific
                )
                summary["recorded"] += 1
            except BrokerResolverError as e:
                summary["failed"] += 1
                summary["errors"][account_id] = e.reason_code
                logger.warning("daily_snapshot_skipped account=%s reason=%s", account_id, e.reason_code)
            except Exception as e:
                summary["failed"] += 1
                summary["errors"][account_id] = type(e).__name__
                logger.error("Failed to record daily snapshot for %s: %s", account_id, e)
        logger.info("Daily snapshot batch completed: %s", {k: v for k, v in summary.items() if k != "errors"})
        return summary


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
