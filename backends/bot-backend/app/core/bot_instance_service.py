"""
Bot Instance Service

Manages the lifecycle of bot instances: creation, deployment, and control.
Does NOT manage strategy configurations.
"""
from __future__ import annotations

import uuid
import logging
import json
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

from shared_lib.persistence.db import DB, utc_now_iso
from app.models.bot_instance_models import BotInstance, CreateBotInstanceRequest

logger = logging.getLogger(__name__)


class BotInstanceService:
    """Service for managing bot instances."""
    
    def __init__(self, db: Optional[DB] = None):
        self.db = db if db is not None else DB()
        
    @staticmethod
    def get_risk_profile_preset(profile: str) -> Dict[str, Any]:
        """Get preset risk parameters for a profile."""
        presets = {
            "conservative": {
                "risk_profile": "conservative",
                "portfolio_risk_pct": 0.02,
                "per_trade_risk_pct": 0.005,
                "max_margin_usage_pct": 0.35,
                "max_drawdown_pct": 0.10,
                "daily_loss_limit_pct": 0.03,
                "position_sizing_method": "risk_based",
                "base_position_slots": 3,
                "max_position_slots": 10,
                "stop_loss_multiplier": 2.0,
                "take_profit_multiplier": 6.0,
                "additional_params": {
                    "min_confidence_score": 0.8,
                    "volatility_filter_enabled": True
                }
            },
            "balanced": {
                "risk_profile": "balanced",
                "portfolio_risk_pct": 0.05,
                "per_trade_risk_pct": 0.01,
                "max_margin_usage_pct": 0.50,
                "max_drawdown_pct": 0.15,
                "daily_loss_limit_pct": 0.05,
                "position_sizing_method": "risk_based",
                "base_position_slots": 5,
                "max_position_slots": 20,
                "stop_loss_multiplier": 2.0,
                "take_profit_multiplier": 4.0,
                "additional_params": {
                    "min_confidence_score": 0.5,
                    "volatility_filter_enabled": True
                }
            },
            "aggressive": {
                "risk_profile": "aggressive",
                "portfolio_risk_pct": 0.10,
                "per_trade_risk_pct": 0.02,
                "max_margin_usage_pct": 0.65,
                "max_drawdown_pct": 0.25,
                "daily_loss_limit_pct": 0.10,
                "position_sizing_method": "risk_based",
                "base_position_slots": 8,
                "max_position_slots": 30,
                "stop_loss_multiplier": 1.5,
                "take_profit_multiplier": 3.0,
                "additional_params": {
                    "min_confidence_score": 0.3,
                    "volatility_filter_enabled": False,
                    "strict_circuit_breakers": True
                }
            }
        }
        
        return presets.get(profile, presets["balanced"])

    def create_bot_instance(self, request: CreateBotInstanceRequest) -> BotInstance:
        """Create a new bot instance."""
        # Validate request
        errors = request.validate()
        if errors:
            raise ValueError(f"Invalid request: {', '.join(errors)}")
            
        instance_id = f"bot_{uuid.uuid4().hex[:12]}"
        now = utc_now_iso()

        # Section F — product safety: user-capital readiness gate.
        # Live/user-capital activation must be explicitly approved and backed by
        # proven paper-mode performance (see app.product_safety.readiness_gate).
        if str(request.mode).lower() == "live":
            from app.product_safety.readiness_gate import assert_user_capital_activation_allowed, UserCapitalReadinessError
            try:
                assert_user_capital_activation_allowed(db=self.db, bot_instance_id=instance_id)
            except UserCapitalReadinessError as exc:
                raise ValueError(json.dumps(exc.payload))
        
        # Create instance object
        instance = BotInstance(
            id=instance_id,
            user_id=request.user_id,
            broker_account_id=request.broker_account_id,
            market_type=request.market_type,
            strategy_id=request.strategy_id,
            strategy_version=request.strategy_version,
            risk_level=request.risk_level,
            config_id=request.config_id,
            risk_profile_id=request.risk_profile_id,
            symbols=request.symbols,
            timeframes=request.timeframes,
            allocation_type=request.allocation_type,
            allocation_value=request.allocation_value,
            mode=request.mode,
            status="active",
            created_at=now,
            updated_at=now,
            started_at=now,  # Auto-start on creation for now? Or wait for explicit start?
            capital_allocation=request.capital_allocation,
            capital_allocation_type=request.capital_allocation_type
        )
        
        # For Auto Pilot, config_id and risk_profile_id are None (internal config)
        # Use placeholder values to satisfy NOT NULL constraints
        db_config_id = instance.config_id or "__auto_pilot__"
        db_risk_profile_id = instance.risk_profile_id or "__auto_pilot__"
        
        if not instance.broker_account_id:
            logger.critical(f"Attempted to insert BotInstance {instance.id} with NULL broker_account_id!")
            raise ValueError("Critical Error: broker_account_id cannot be None")

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO bot_instances (
                    id, user_id, broker_account_id, market_type, strategy_id, 
                    strategy_version, risk_level, config_id, risk_profile_id, 
                    symbols_json, timeframes_json, allocation_type, allocation_value, 
                    mode, status, created_at, updated_at, started_at, stopped_at,
                    capital_allocation, capital_allocation_type,
                    last_run_at, last_error, total_trades, active_positions
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    instance.id, instance.user_id, instance.broker_account_id, instance.market_type, 
                    instance.strategy_id, instance.strategy_version, instance.risk_level, 
                    db_config_id, db_risk_profile_id,
                    json.dumps(instance.symbols), json.dumps(instance.timeframes),
                    instance.allocation_type, instance.allocation_value,
                    instance.mode, instance.status, instance.created_at, instance.updated_at,
                    instance.started_at, instance.stopped_at,
                    instance.capital_allocation, instance.capital_allocation_type,
                    instance.last_run_at, instance.last_error, instance.total_trades, instance.active_positions
                )
            )
            
        logger.info(f"Created bot instance {instance_id} for user {request.user_id}")
        return instance
    
    def get_bot_instance(self, instance_id: str) -> Optional[BotInstance]:
        """
        Get a bot instance by ID.
        
        Args:
            instance_id: Bot instance ID
            
        Returns:
            BotInstance or None if not found
        """
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT bi.*, ba.broker_id 
                FROM bot_instances bi 
                LEFT JOIN broker_accounts ba ON bi.broker_account_id = ba.id
                WHERE bi.id = ?
                """,
                (instance_id,)
            ).fetchone()
            
            if row:
                return BotInstance.from_db_row(row)
            
            # Debugging: Why not found?
            all_rows = conn.execute("SELECT id, status FROM bot_instances").fetchall()
            ids = [f"{r['id']} ({r['status']})" for r in all_rows]
            logger.error(f"[DEBUG] Instance {instance_id} NOT FOUND. Available IDs: {ids}")
            
            return None
    
    def get_user_bot_instances(
        self,
        user_id: str,
        status_filter: Optional[str] = None,
    ) -> List[BotInstance]:
        """
        Get all bot instances for a user.

        Soft-deleted bots (status='deleted') are excluded from the default
        list view. Pass status_filter='deleted' explicitly to retrieve them.

        Args:
            user_id:       User ID
            status_filter: Optional status filter (active, paused, stopped,
                           error, deleted).  If None, excludes 'deleted'.
        """
        with self.db.connect() as conn:
            if status_filter:
                rows = conn.execute(
                    """
                    SELECT bi.*, ba.broker_id
                    FROM bot_instances bi
                    LEFT JOIN broker_accounts ba ON bi.broker_account_id = ba.id
                    WHERE bi.user_id = ? AND bi.status = ?
                    ORDER BY bi.created_at DESC
                    """,
                    (user_id, status_filter),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT bi.*, ba.broker_id
                    FROM bot_instances bi
                    LEFT JOIN broker_accounts ba ON bi.broker_account_id = ba.id
                    WHERE bi.user_id = ? AND bi.status NOT IN ('deleted', 'archived')
                    ORDER BY bi.created_at DESC
                    """,
                    (user_id,),
                ).fetchall()

            return [BotInstance.from_db_row(row) for row in rows]
    
    def get_all_bot_instances(self) -> List[BotInstance]:
        """
        Get all bot instances in the system including deleted and archived ones.
        Used for inventory and admin diagnostics.
        """
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT bi.*, ba.broker_id
                FROM bot_instances bi
                LEFT JOIN broker_accounts ba ON bi.broker_account_id = ba.id
                ORDER BY bi.created_at DESC
                """
            ).fetchall()
            return [BotInstance.from_db_row(row) for row in rows]
    
    def get_active_bot_instances(self) -> List[BotInstance]:
        """
        Return TRADABLE bot instances — active bots whose broker health is 'ok'.

        Bots quarantined due to broker failures (broker_health_status='broker_blocked')
        are intentionally excluded so they do not re-enter the execution pool every
        cycle while their broker remains invalid.

        The runner calls get_broker_blocked_instances() separately to log/display
        the blocked bots without executing them.
        """
        with self.db.connect() as conn:
            # broker_health_status column added in migration 36; guard with PRAGMA
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()}
            if "broker_health_status" in cols:
                rows = conn.execute(
                    """
                    SELECT bi.*, ba.broker_id
                    FROM bot_instances bi
                    LEFT JOIN broker_accounts ba ON bi.broker_account_id = ba.id
                    WHERE bi.status IN ('active', 'error')
                      AND (bi.broker_health_status IS NULL OR bi.broker_health_status = 'ok')
                    ORDER BY bi.last_run_at ASC NULLS FIRST
                    """
                ).fetchall()
            else:
                # Pre-migration fallback — return all active bots
                rows = conn.execute(
                    """
                    SELECT bi.*, ba.broker_id
                    FROM bot_instances bi
                    LEFT JOIN broker_accounts ba ON bi.broker_account_id = ba.id
                    WHERE bi.status IN ('active', 'error') AND bi.status NOT IN ('archived', 'deleted')
                    ORDER BY bi.last_run_at ASC NULLS FIRST
                    """
                ).fetchall()

            return [BotInstance.from_db_row(row) for row in rows]

    def get_broker_blocked_instances(self) -> List[BotInstance]:
        """
        Return bot instances that are active but quarantined due to a broker error.

        These bots have bi.status='active' but broker_health_status='broker_blocked'.
        They are logged each cycle so the UI/ops can see them, but they are NOT
        included in the execution pool.
        """
        with self.db.connect() as conn:
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()}
            if "broker_health_status" not in cols:
                return []
            rows = conn.execute(
                """
                SELECT bi.*, ba.broker_id
                FROM bot_instances bi
                LEFT JOIN broker_accounts ba ON bi.broker_account_id = ba.id
                WHERE bi.status IN ('active', 'error')
                  AND bi.broker_health_status = 'broker_blocked'
                  AND bi.status != 'archived'
                  AND bi.status != 'deleted'
                ORDER BY bi.broker_blocked_at ASC NULLS FIRST
                """
            ).fetchall()
            return [BotInstance.from_db_row(row) for row in rows]

    def quarantine_bot(
        self,
        instance_id: str,
        reason_code: str,
        error_message: str,
    ) -> None:
        """
        Mark a bot as broker-blocked so it exits the normal execution pool.

        Sets broker_health_status='broker_blocked', records reason_code and timestamp.
        The bot's user-visible status remains 'active' so it appears in dashboards
        with an actionable error instead of silently disappearing.

        Args:
            instance_id:   bot_instances.id
            reason_code:   BrokerResolverError.reason_code value
            error_message: human-readable description for UI display
        """
        now = utc_now_iso()
        with self.db.connect() as conn:
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()}
            if "broker_health_status" not in cols:
                # Migration hasn't run yet; fall back to last_error only
                conn.execute(
                    "UPDATE bot_instances SET last_error=?, updated_at=? WHERE id=?",
                    (f"[{reason_code}] {error_message}", now, instance_id),
                )
                return
            if "block_category" in cols:
                conn.execute(
                    """
                    UPDATE bot_instances
                    SET broker_health_status = 'broker_blocked',
                        broker_error_code    = ?,
                        broker_blocked_at    = ?,
                        block_category       = 'broker_auth_failure',
                        block_reason_code    = ?,
                        block_reason_detail  = ?,
                        blocked_since        = ?,
                        last_error           = ?,
                        updated_at           = ?
                    WHERE id = ?
                    """,
                    (reason_code, now, reason_code, f"[{reason_code}] {error_message}", now, f"[{reason_code}] {error_message}", now, instance_id),
                )
            elif "broker_health_status" in cols:
                conn.execute(
                    """
                    UPDATE bot_instances
                    SET broker_health_status = 'broker_blocked',
                        broker_error_code    = ?,
                        broker_blocked_at    = ?,
                        last_error           = ?,
                        updated_at           = ?
                    WHERE id = ?
                    """,
                    (reason_code, now, f"[{reason_code}] {error_message}", now, instance_id),
                )
        logger.warning(
            "bot_quarantined bot_id=%s reason=%s message=%r",
            instance_id, reason_code, error_message,
        )

    def unquarantine_bot(self, instance_id: str) -> None:
        """
        Clear broker quarantine — called after a successful broker reconnect or repair.

        Restores broker_health_status='ok' so the bot re-enters the execution pool
        on the next cycle.
        """
        now = utc_now_iso()
        with self.db.connect() as conn:
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()}
            if "broker_health_status" not in cols:
                return
            if "block_category" in cols:
                conn.execute(
                    """
                    UPDATE bot_instances
                    SET broker_health_status = 'ok',
                        broker_error_code    = NULL,
                        broker_blocked_at    = NULL,
                        block_category       = NULL,
                        block_reason_code    = NULL,
                        block_reason_detail  = NULL,
                        blocked_since        = NULL,
                        last_error           = NULL,
                        updated_at           = ?
                    WHERE id = ?
                    """,
                    (now, instance_id),
                )
            elif "broker_health_status" in cols:
                conn.execute(
                    """
                    UPDATE bot_instances
                    SET broker_health_status = 'ok',
                        broker_error_code    = NULL,
                        broker_blocked_at    = NULL,
                        last_error           = NULL,
                        updated_at           = ?
                    WHERE id = ?
                    """,
                    (now, instance_id),
                )
        logger.info("bot_unquarantined bot_id=%s", instance_id)

    def rebind_bot_broker(
        self,
        instance_id: str,
        new_broker_account_id: str,
    ) -> None:
        """
        Rebind a bot to a different broker account of the same broker type.

        Validates:
          - new_broker_account_id must belong to the same user as the bot
          - new_broker_account_id must be status='connected'
          - new_broker_account_id must have the same broker_id as the current account

        Clears quarantine on success.
        """
        now = utc_now_iso()
        with self.db.connect() as conn:
            bot_row = conn.execute(
                "SELECT user_id, broker_account_id FROM bot_instances WHERE id=?",
                (instance_id,),
            ).fetchone()
            if not bot_row:
                raise ValueError(f"Bot instance {instance_id!r} not found")

            user_id = bot_row["user_id"]
            old_account_id = bot_row["broker_account_id"]

            # Verify old account's broker_id
            old_acct = conn.execute(
                "SELECT broker_id FROM broker_accounts WHERE id=?",
                (old_account_id,),
            ).fetchone()
            old_broker_id = old_acct["broker_id"] if old_acct else None

            # Verify new account ownership + status + broker_id match
            new_acct = conn.execute(
                "SELECT broker_id, status, user_id FROM broker_accounts WHERE id=?",
                (new_broker_account_id,),
            ).fetchone()
            if not new_acct:
                raise ValueError(f"New broker account {new_broker_account_id!r} not found")
            if new_acct["user_id"] != user_id:
                raise ValueError("New broker account does not belong to this bot's user")
            if new_acct["status"] != "connected":
                raise ValueError(
                    f"New broker account {new_broker_account_id!r} is not connected "
                    f"(status={new_acct['status']!r})"
                )
            if old_broker_id and new_acct["broker_id"] != old_broker_id:
                raise ValueError(
                    f"Broker type mismatch: bot was on {old_broker_id!r}, "
                    f"new account is {new_acct['broker_id']!r}"
                )

            # Apply rebind atomically
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()}
            if "block_category" in cols:
                conn.execute(
                    """
                    UPDATE bot_instances
                    SET broker_account_id    = ?,
                        broker_health_status = 'ok',
                        broker_error_code    = NULL,
                        broker_blocked_at    = NULL,
                        block_category       = NULL,
                        block_reason_code    = NULL,
                        block_reason_detail  = NULL,
                        blocked_since        = NULL,
                        last_error           = NULL,
                        updated_at           = ?
                    WHERE id = ?
                    """,
                    (new_broker_account_id, now, instance_id),
                )
            elif "broker_health_status" in cols:
                conn.execute(
                    """
                    UPDATE bot_instances
                    SET broker_account_id    = ?,
                        broker_health_status = 'ok',
                        broker_error_code    = NULL,
                        broker_blocked_at    = NULL,
                        last_error           = NULL,
                        updated_at           = ?
                    WHERE id = ?
                    """,
                    (new_broker_account_id, now, instance_id),
                )
            else:
                conn.execute(
                    "UPDATE bot_instances SET broker_account_id=?, updated_at=? WHERE id=?",
                    (new_broker_account_id, now, instance_id),
                )

        logger.info(
            "bot_rebound bot_id=%s old_account=%s new_account=%s",
            instance_id, old_account_id, new_broker_account_id,
        )
    
    def find_replacement_broker(
        self,
        user_id: str,
        broker_id: str,
        exclude_account_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Find a connected broker account of the same broker type for the given user.

        Used by the repair flow: if a bot's broker is invalid, this method locates
        a healthy replacement account so rebind_bot_broker() can be called.

        Args:
            user_id:           owner user
            broker_id:         broker type to match (e.g. 'binance')
            exclude_account_id: current broken account to exclude from results

        Returns:
            broker_accounts.id of the best replacement, or None if none exists.
        """
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id FROM broker_accounts
                WHERE user_id   = ?
                  AND broker_id = ?
                  AND status    = 'connected'
                  AND id        != COALESCE(?, '')
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (user_id, broker_id, exclude_account_id),
            ).fetchone()
        return row["id"] if row else None

    def start_bot_instance(self, instance_id: str) -> BotInstance:
        """
        Start a paused or stopped bot instance.
        
        Args:
            instance_id: Bot instance ID
            
        Returns:
            Updated BotInstance
            
        Raises:
            ValueError: If instance not found or already active
        """
        instance = self.get_bot_instance(instance_id)
        if not instance:
            raise ValueError(f"Bot instance {instance_id} not found")
        
        if instance.status == "active":
            raise ValueError(f"Bot instance {instance_id} is already active")
        
        now = datetime.utcnow().isoformat()
        
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE bot_instances 
                SET status = 'active', 
                    started_at = ?, 
                    updated_at = ?,
                    last_error = NULL
                WHERE id = ?
                """,
                (now, now, instance_id)
            )
        
        logger.info(f"Started bot instance {instance_id}")
        return self.get_bot_instance(instance_id)
    
    def pause_bot_instance(self, instance_id: str) -> BotInstance:
        """
        Pause an active bot instance.
        
        Args:
            instance_id: Bot instance ID
            
        Returns:
            Updated BotInstance
            
        Raises:
            ValueError: If instance not found
        """
        instance = self.get_bot_instance(instance_id)
        if not instance:
            raise ValueError(f"Bot instance {instance_id} not found")
        
        now = datetime.utcnow().isoformat()
        
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE bot_instances SET status = 'paused', updated_at = ? WHERE id = ?",
                (now, instance_id)
            )
        
        logger.info(f"Paused bot instance {instance_id}")
        return self.get_bot_instance(instance_id)
    
    def stop_bot_instance(self, instance_id: str) -> BotInstance:
        """
        Stop a bot instance (cannot be restarted).
        
        Args:
            instance_id: Bot instance ID
            
        Returns:
            Updated BotInstance
            
        Raises:
            ValueError: If instance not found
        """
        instance = self.get_bot_instance(instance_id)
        if not instance:
            raise ValueError(f"Bot instance {instance_id} not found")
        
        now = datetime.utcnow().isoformat()
        
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE bot_instances SET status = 'stopped', stopped_at = ?, updated_at = ? WHERE id = ?",
                (now, now, instance_id)
            )
        
        logger.info(f"Stopped bot instance {instance_id}")
        return self.get_bot_instance(instance_id)
    
    def delete_bot_instance(self, instance_id: str):
        """
        Soft-delete a bot instance (status → 'deleted').

        Soft delete is used instead of hard DELETE so that:
        - The bot immediately exits the runner's active query (which only
          includes 'active' and 'error' statuses).
        - Trade history, fills, and audit events referencing the instance_id
          remain intact for auditing purposes.
        - The runner's _runners cache is invalidated on the next cycle
          (the bot will no longer appear in get_active_bot_instances()).

        Raises:
            ValueError: If instance not found
        """
        instance = self.get_bot_instance(instance_id)
        if not instance:
            raise ValueError(f"Bot instance {instance_id} not found")

        now = utc_now_iso()
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE bot_instances SET status='deleted', stopped_at=?, updated_at=? WHERE id=?",
                (now, now, instance_id),
            )

        logger.info("bot_instance_deleted bot_id=%s (soft)", instance_id)

    def archive_bot_instance(self, instance_id: str):
        """
        Archive a bot instance (status → 'archived').
        
        Like soft-delete, but semantically distinct. Archived bots are intentionally
        retired legacy records, excluded from active and blocked lists but preserved
        for history without looking like user-deleted items.
        
        Raises:
            ValueError: If instance not found
        """
        instance = self.get_bot_instance(instance_id)
        if not instance:
            raise ValueError(f"Bot instance {instance_id} not found")

        now = utc_now_iso()
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE bot_instances SET status='archived', stopped_at=?, updated_at=? WHERE id=?",
                (now, now, instance_id),
            )

        logger.info("bot_instance_archived bot_id=%s", instance_id)

    def deploy_auto_pilot(
        self,
        user_id: str,
        risk_level: str,
        allocation_type: str,
        allocation_value: float,
        broker_account_ids: List[str],
        mode: str = "paper",
        capital_allocation: Optional[float] = None,
        capital_allocation_type: str = "fixed_amount",
        market_type: str = "CRYPTO",
        forex_config: Optional[dict] = None
    ) -> List[BotInstance]:
        """
        Deploy the Auto Pilot (Master Ensemble) strategy to selected broker accounts.
        Creates bot instances with embedded risk configuration.
        """
        from app.models.bot_instance_models import CreateBotInstanceRequest
        from app.symbols.universe import parse_symbols
        from app.core.config import settings

        # Hardcoded Strategy Metadata for Auto Pilot
        strategy_id = "master_ensemble"
        strategy_name = "Auto Pilot (Master Ensemble)"
        strategy_version = "1.0.0"
        
        # 1. Verification
        if allocation_type == "percent_balance" and not (0 < allocation_value <= 100):
            raise ValueError("Allocation percentage must be between 0 and 100")

        # 2. Determine Symbols based on market type
        if market_type == "FOREX":
            # FOREX: Use user-provided allowlist or fallback to env config
            if forex_config and forex_config.get("allowlist"):
                target_symbols = forex_config["allowlist"]
                logger.info(f"Using user-provided Forex allowlist: {target_symbols}")
            else:
                # Fallback to FOREX_SYMBOLS env var
                target_symbols = parse_symbols(settings.FOREX_SYMBOLS)
                if not target_symbols:
                    raise ValueError(
                        "No forex allowlist provided. Either pass forex_config.allowlist or configure FOREX_SYMBOLS in .env"
                    )
                logger.info(f"Using Forex symbols from FOREX_SYMBOLS env: {target_symbols}")
        else:
            # CRYPTO: Use system default trade universe
            target_symbols = parse_symbols(settings.TRADE_SYMBOLS)
            if not target_symbols:
                raise ValueError(
                    "No crypto symbols configured. Please set TRADE_SYMBOLS in .env"
                )
            logger.info(f"Using Crypto symbols from TRADE_SYMBOLS: {target_symbols}")
        
        created_instances = []

        # 3. Deploy per account
        for broker_account_id in broker_account_ids:
            try:
                # Create Bot Instance with embedded risk level
                # No separate config/risk profile needed - it's all in the instance
                req = CreateBotInstanceRequest(
                    user_id=user_id,
                    broker_account_id=broker_account_id,
                    market_type=market_type, 
                    strategy_id=strategy_id,
                    strategy_version=strategy_version,
                    config_id=None,  # No external config
                    risk_profile_id=None,  # No external risk profile
                    risk_level=risk_level,  # Store risk level directly
                    symbols=target_symbols,
                    timeframes=["15m"], # Master Ensemble typically runs on specific timeframes, defaulting to 15m
                    allocation_type=allocation_type,
                    allocation_value=allocation_value,
                    mode=mode,
                )
                
                instance = self.create_bot_instance(req)
                created_instances.append(instance)
                
            except Exception as e:
                logger.error(f"Failed Auto Pilot deployment for broker {broker_account_id}: {e}")
                # We raise to stop partial deployments or handle gracefully? 
                # For now, let's fail the whole batch if one fails, or at least log it.
                # Raising here allows the API to catch it.
                raise ValueError(f"Deployment failed for account {broker_account_id}: {e}")

        # ✅ Observability: Log Deployment Event
        try:
            from shared_lib.persistence.audit import Audit
            audit = Audit(self.db)
            audit.event(
                event_type="AUTOPILOT_DEPLOYED",
                run_id=f"deploy_{uuid.uuid4().hex[:8]}",
                symbol=None,
                action="DEPLOY",
                details={
                    "user_id": user_id,
                    "count": len(created_instances),
                    "risk_level": risk_level,
                    "allocation_type": allocation_type,
                    "broker_accounts": broker_account_ids
                }
            )
        except Exception as audit_err:
             logger.warning(f"Failed to emit AUTOPILOT_DEPLOYED audit event: {audit_err}")

        logger.info(f"Deployed Auto Pilot to {len(created_instances)} accounts for user {user_id}")
        return created_instances
    
    def update_instance_runtime_state(
        self,
        instance_id: str,
        last_run_at: Optional[str] = None,
        last_run_id: Optional[str] = None,
        total_trades: Optional[int] = None,
        active_positions: Optional[int] = None,
        error_message: Optional[str] = None
    ):
        """
        Update runtime state of a bot instance.
        
        Args:
            instance_id: Bot instance ID
            last_run_at: Timestamp of last execution
            last_run_id: Run ID of last execution
            total_trades: Cumulative trade count
            active_positions: Current open positions
            error_message: Error message if any (sets status to 'error')
        """
        now = datetime.utcnow().isoformat()
        updates = ["updated_at = ?"]
        values = [now]
        
        if last_run_at:
            updates.append("last_run_at = ?")
            values.append(last_run_at)

        if last_run_id:
            updates.append("last_run_id = ?")
            values.append(last_run_id)
        
        if total_trades is not None:
            updates.append("total_trades = ?")
            values.append(total_trades)
        
        if active_positions is not None:
            updates.append("active_positions = ?")
            values.append(active_positions)
        
        if error_message:
            updates.append("last_error = ?")
            # Only change status to 'error' if it's not already in a terminal state
            updates.append("status = CASE WHEN status IN ('deleted', 'archived', 'stopped') THEN status ELSE 'error' END")
            values.append(error_message)
        
        values.append(instance_id)
        
        with self.db.connect() as conn:
            cursor = conn.execute(
                f"UPDATE bot_instances SET {', '.join(updates)} WHERE id = ?",
                values
            )
            if cursor.rowcount == 0:
                # IMMEDIATE INVESTIGATION: Does it exist?
                check = conn.execute("SELECT id, status FROM bot_instances WHERE id = ?", (instance_id,)).fetchone()
                if check:
                    logger.error(f"[CRITICAL] Instance {instance_id} EXISTS ({check['status']}) but UPDATE affected 0 rows. Params: {values}")
                else:
                    logger.warning(f"Runtime state update affected 0 rows for instance {instance_id} (Not Found in DB at all)")
        
        if error_message:
            logger.error(f"Bot instance {instance_id} encountered error: {error_message}")

    def update_bot_instance(self, instance_id: str, updates: Dict[str, Any]) -> BotInstance:
        """
        Update bot instance configuration.
        
        Args:
            instance_id: Bot instance ID
            updates: Dictionary of fields to update
            
        Returns:
            Updated BotInstance
            
        Raises:
            ValueError: If instance not found
        """
        import json # Ensure json is available for serialization
        
        instance = self.get_bot_instance(instance_id)
        if not instance:
            raise ValueError(f"Bot instance {instance_id} not found")

        valid_fields = {
            "risk_profile_id", "allocation_type", "allocation_value", 
            "capital_allocation", "capital_allocation_type", "mode", "symbols", "timeframes", "status"
        }
        
        filtered_updates = {k: v for k, v in updates.items() if k in valid_fields}
        
        if not filtered_updates:
            return instance # No changes
            
        db_updates = {}
        for k, v in filtered_updates.items():
            if k == "symbols":
                db_updates["symbols_json"] = json.dumps(v)
            elif k == "timeframes":
                db_updates["timeframes_json"] = json.dumps(v)
            else:
                db_updates[k] = v

        # Section F — product safety: block switching to live mode unless the
        # readiness gate is explicitly passed and approved.
        if "mode" in db_updates and str(db_updates["mode"]).lower() == "live":
            from app.product_safety.readiness_gate import assert_user_capital_activation_allowed, UserCapitalReadinessError
            try:
                assert_user_capital_activation_allowed(db=self.db, bot_instance_id=instance_id)
            except UserCapitalReadinessError as exc:
                raise ValueError(json.dumps(exc.payload))
        
        db_updates["updated_at"] = datetime.utcnow().isoformat()
        
        set_clause = ", ".join([f"{k} = ?" for k in db_updates.keys()])
        values = list(db_updates.values())
        values.append(instance_id)
        
        with self.db.connect() as conn:
            conn.execute(
                f"UPDATE bot_instances SET {set_clause} WHERE id = ?",
                values
            )
            
        logger.info(f"Updated bot instance {instance_id}: {list(filtered_updates.keys())}")
        return self.get_bot_instance(instance_id)

    def update_bot_health(
        self,
        instance_id: str,
        *,
        bot_health_status: str,
        bot_health_message: Optional[str] = None,
        bot_health_reason_code: Optional[str] = None,
        bot_health_recommended_action: Optional[str] = None,
        last_error: Optional[str] = None,
        last_warning: Optional[str] = None,
    ) -> None:
        now = utc_now_iso()
        updates = [
            "bot_health_status = ?",
            "bot_health_message = ?",
            "bot_health_reason_code = ?",
            "bot_health_recommended_action = ?",
            "bot_health_updated_at = ?",
            "updated_at = ?",
        ]
        values: list[Any] = [
            bot_health_status,
            bot_health_message,
            bot_health_reason_code,
            bot_health_recommended_action,
            now,
            now,
        ]
        if last_error is not None:
            updates.append("last_error = ?")
            values.append(last_error)
        if last_warning is not None:
            updates.append("last_warning = ?")
            values.append(last_warning)

        values.append(instance_id)
        with self.db.connect() as conn:
            conn.execute(f"UPDATE bot_instances SET {', '.join(updates)} WHERE id = ?", values)


# Global singleton instance
from app.core.config import settings

def _get_db_path() -> Optional[str]:
    url = settings.DATABASE_URL
    if url and url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "")
    return None

_db_path = _get_db_path()
_bot_instance_service = BotInstanceService(db=DB(path=_db_path) if _db_path else DB())


def get_bot_instance_service() -> BotInstanceService:
    """Get the global bot instance service."""
    return _bot_instance_service
