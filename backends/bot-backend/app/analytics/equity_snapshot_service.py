"""
Equity Snapshot Service

Records equity snapshots from exchange accounts to track portfolio value over time.
Snapshots are recorded at various trigger points:
- bot_start: When a bot instance first starts trading
- cycle_end: After each bot trading cycle
- daily_snapshot: Once daily at 00:00 UTC for all active accounts
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from shared_lib.persistence.db import DB
from app.core.config import settings

logger = logging.getLogger(__name__)


class EquitySnapshotService:
    """Service for recording equity snapshots from exchange accounts."""
    
    def __init__(self, db: Optional[DB] = None):
        if db is None:
            # Use the configured database path (cosmicforge.db), NOT the default bot.db fallback
            db_path = settings.DATABASE_URL.replace("sqlite:///", "")
            db = DB(db_path)
        self.db = db
        self.last_snapshot_time = {}  # Track last snapshot per account to enforce rate limiting
        self.min_snapshot_interval_seconds = 60  # Minimum 60 seconds between snapshots for same account
    
    def record_snapshot(
        self,
        user_id: str,
        broker_account_id: str,
        broker_id: str,
        client,  # Exchange client (e.g., BinanceFuturesClient, BybitClient)
        source: str,  # e.g., "bot_start", "cycle_end", "daily_snapshot"
        bot_instance_id: Optional[str] = None
    ):
        """
        Record an equity snapshot for the given broker account.
        
        Args:
            user_id: User ID who owns the account
            broker_account_id: Broker account identifier
            broker_id: Broker identifier (e.g., "binance", "bybit")
            client: Exchange client instance with account info methods
            source: Source of the snapshot (bot_start, cycle_end, daily_snapshot)
            bot_instance_id: Optional bot instance ID if snapshot is bot-specific
        """
        try:
            # Rate limiting: Prevent duplicate snapshots in quick succession
            rate_limit_key = f"{broker_account_id}:{source}"
            now = datetime.now(timezone.utc)
            
            if rate_limit_key in self.last_snapshot_time:
                last_time = self.last_snapshot_time[rate_limit_key]
                elapsed = (now - last_time).total_seconds()
                if elapsed < self.min_snapshot_interval_seconds:
                    logger.debug(f"Skipping snapshot for {broker_account_id} (source={source}), "
                               f"last snapshot was {elapsed:.1f}s ago")
                    return
            
            # Fetch account information from exchange
            account_info = client.account()
            
            # Extract relevant fields (structure varies by exchange)
            # Most exchanges return something like:
            # { "totalWalletBalance": ..., "totalUnrealizedProfit": ..., "availableBalance": ... }
            
            wallet_balance = None
            equity = None
            available_balance = None
            unrealized_pnl = None
            margin_used = None
            currency = "USDT"  # Default, most futures use USDT
            
            # Parse based on broker type
            if broker_id == "binance":
                # Binance Futures structure
                wallet_balance = float(account_info.get("totalWalletBalance", 0))
                unrealized_pnl = float(account_info.get("totalUnrealizedProfit", 0))
                available_balance = float(account_info.get("availableBalance", 0))
                margin_used = float(account_info.get("totalMarginBalance", 0)) - available_balance
                equity = wallet_balance + unrealized_pnl
                
            elif broker_id == "bybit":
                # Bybit structure (check actual API response)
                result = account_info.get("result", {})
                wallet_balance = float(result.get("walletBalance", 0))
                unrealized_pnl = float(result.get("unrealisedPnl", 0))
                available_balance = float(result.get("availableBalance", 0))
                equity = float(result.get("equity", wallet_balance + unrealized_pnl))
                margin_used = wallet_balance - available_balance
                
            elif broker_id == "bingx":
                # BingX structure
                balance = account_info.get("balance", {})
                wallet_balance = float(balance.get("balance", 0))
                unrealized_pnl = float(balance.get("unrealizedProfit", 0))
                available_balance = float(balance.get("availableMargin", 0))
                equity = wallet_balance + unrealized_pnl
                margin_used = wallet_balance - available_balance
            
            else:
                logger.warning(f"Unknown broker_id: {broker_id}, using generic parsing")
                # Generic fallback
                wallet_balance = float(account_info.get("balance", 0) or account_info.get("totalWalletBalance", 0))
                unrealized_pnl = float(account_info.get("unrealizedProfit", 0) or account_info.get("totalUnrealizedProfit", 0))
                equity = wallet_balance + unrealized_pnl
                available_balance = float(account_info.get("availableBalance", equity))
                margin_used = wallet_balance - available_balance
            
            # Get current UTC timestamp
            # The 'now' variable is already defined above, so we can reuse it.
            now_utc = now.isoformat()
            
            # Insert snapshot into database
            with self.db.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO equity_snapshots (
                        user_id, bot_instance_id, broker_account_id, broker_id,
                        timestamp_utc, wallet_balance, equity, available_balance, 
                        unrealized_pnl, margin_used, currency, source,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        bot_instance_id,
                        broker_account_id,
                        broker_id,
                        now.isoformat(),
                        wallet_balance,
                        equity,
                        available_balance,
                        unrealized_pnl,
                        margin_used,
                        currency,
                        source,
                        now_utc,  # created_at
                        now_utc   # updated_at
                    )
                )
                conn.commit()
            
            # Update rate limit tracker
            self.last_snapshot_time[rate_limit_key] = now
            
            logger.info(f"Recorded equity snapshot: account={broker_account_id}, "
                       f"equity={equity:.2f}, source={source}")
            
        except Exception as e:
            logger.error(f"Failed to record equity snapshot for {broker_account_id}: {e}")
            # Do not raise exception to avoid crashing the bot loop or bot startup
            pass


# Singleton instance
_service_instance: Optional[EquitySnapshotService] = None


def get_equity_snapshot_service(db: Optional[DB] = None) -> EquitySnapshotService:
    """Get or create the equity snapshot service singleton."""
    global _service_instance
    if _service_instance is None:
        _service_instance = EquitySnapshotService(db)
    return _service_instance
