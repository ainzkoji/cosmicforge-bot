import asyncio
import logging
import uuid
import json
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional

from shared_lib.persistence.db import DB, utc_now_iso
from app.backtest.runner import BacktestRunner, BacktestConfig

logger = logging.getLogger(__name__)

class BacktestWorker:
    """
    Background worker that polls for pending backtest jobs and executes them.
    Safe for SQLite (uses token claiming).
    Processes ONE job at a time (CPU heavy).
    """

    def __init__(self, db: DB):
        self.db = db
        self.running = False
        self._stop_event = asyncio.Event()

    async def start(self):
        self.running = True
        logger.info("BacktestWorker: Started.")
        while self.running:
            try:
                # Process one job at a time
                processed = await self.process_job()
                if processed == 0:
                    # No jobs, sleep a bit
                    await asyncio.sleep(5)
                else:
                    # Had job, sleep briefly before checking again
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"BacktestWorker: Loop failed: {e}")
                traceback.print_exc()
                await asyncio.sleep(5)

    def stop(self):
        self.running = False
        logger.info("BacktestWorker: Stopping...")

    async def process_job(self) -> int:
        # Runs in thread pool to avoid blocking async loop with heavy backtest computation
        return await asyncio.to_thread(self._process_job_sync)

    def _process_job_sync(self) -> int:
        claim_token = str(uuid.uuid4())
        now = utc_now_iso()
        
        # 1. Claim ONE Job (Atomic Update)
        with self.db.connect() as conn:
            # Mark 1 pending job as processing
            conn.execute(
                """
                UPDATE backtest_jobs
                SET status = 'processing',
                    processing_token = ?,
                    processing_started_at = ?
                WHERE id IN (
                    SELECT id FROM backtest_jobs
                    WHERE status = 'pending' 
                      AND (next_retry_at IS NULL OR next_retry_at <= ?)
                    LIMIT 1
                )
                """,
                (claim_token, now, now)
            )
            
            # 2. Fetch Claimed Job
            row_obj = conn.execute(
                "SELECT * FROM backtest_jobs WHERE processing_token = ?",
                (claim_token,)
            ).fetchone()
            
            row = dict(row_obj) if row_obj else None
        
        if not row:
            return 0
            
        logger.info(f"BacktestWorker: Claimed job {row['id']} (run_id={row['run_id']})")
        
        self._execute_job(row)
            
        return 1

    def _execute_job(self, row):
        job_id = row["id"]
        run_id = row["run_id"]
        priority = row["priority"]
        attempts = row.get("attempts", 0)
        
        status = "failed"
        error = None
        
        try:
            # 1. Check for cancellation before starting
            if self._is_cancelled(run_id):
                logger.info(f"BacktestWorker: Job {job_id} cancelled before start.")
                self._finalize_job(job_id, "cancelled", run_id=run_id)
                return

            # 2. Load Run Config
            with self.db.connect() as conn:
                run_row_obj = conn.execute(
                    "SELECT * FROM backtest_runs WHERE id = ?",
                    (run_id,)
                ).fetchone()
                run_row = dict(run_row_obj) if run_row_obj else None
                
            if not run_row:
                raise ValueError(f"Backtest run {run_id} not found")
            
            if run_row["status"] == "cancelled":
                logger.info(f"BacktestWorker: Run {run_id} is cancelled. Skipping.")
                self._finalize_job(job_id, "cancelled", run_id=run_id)
                return
                
            # Parse config
            config = self._parse_config(run_row)
            
            # 3. Initialize Runner
            # Pass cancellation check callback
            runner = BacktestRunner(
                config, 
                db=self.db,
                cancellation_check=lambda: self._is_cancelled(run_id)
            )
            
            # 4. Run Backtest
            runner.run()
            
            status = "completed"
            logger.info(f"BacktestWorker: Job {job_id} completed successfully.")

        except Exception as e:
            error = str(e)
            
            # Check if it was a cancellation exception
            if "cancelled" in str(e).lower():
                status = "cancelled"
                error = None 
                logger.info(f"BacktestWorker: Job {job_id} cancelled during execution.")
            else:
                logger.error(f"BacktestWorker: Job {job_id} failed: {e}")
                traceback.print_exc()

        # 5. Update Job Status (if not already completed/cancelled by runner)
        if status != "completed":
             self._handle_failure(row, status, error)

    def _parse_config(self, row) -> BacktestConfig:
        """Convert database row to BacktestConfig"""
        try:
            strategy_params = json.loads(row["strategy_params_json"]) if row["strategy_params_json"] else {}
        except Exception:
            strategy_params = {}

        try:
            symbols = json.loads(row["symbols_json"]) if row["symbols_json"] else ["BTCUSDT"]
        except Exception:
            symbols = ["BTCUSDT"]

        # Parse risk params if present (optional)
        try:
            risk_params = json.loads(row["risk_params_json"]) if row.get("risk_params_json") else {}
        except Exception:
            risk_params = {}
        
        return BacktestConfig(
            run_id=row["id"],
            user_id=row.get("user_id", "system"),
            name=row.get("name", "Backtest"),
            strategy_id=row["strategy_id"],
            strategy_params=strategy_params,
            symbols=symbols,
            interval=row.get("timeframe", "1m"),
            start_date=row["start_date"],
            end_date=row["end_date"],
            initial_capital=float(row["initial_capital"]),
            slippage_bps=row.get("slippage_bps", 10.0),
            fee_bps=row.get("fee_bps", 6.0),
            max_daily_loss_pct=risk_params.get("max_daily_loss_pct", 0.05),
            max_trades_daily=risk_params.get("max_trades_daily", 20),
            max_open_positions=risk_params.get("max_open_positions", 3),
            stop_loss_pct=risk_params.get("stop_loss_pct", 0.02),
            max_leverage=row.get("leverage") or 1,
            data_source=row.get("data_source", "binance"),
            market_type=row.get("market_type", "crypto"),
            # Some fields might be missing in DB, use defaults from risk params or hardcoded
            trade_usdt_per_order=risk_params.get("trade_usdt_per_order", 1000.0),
            risk_level=risk_params.get("risk_level", "medium")
        )

    def _is_cancelled(self, run_id: str) -> bool:
        """Check if job should be cancelled"""
        try:
            with self.db.connect() as conn:
                row = conn.execute(
                    "SELECT status FROM backtest_runs WHERE id = ?",
                    (run_id,)
                ).fetchone()
                if row and row["status"] == "cancelled":
                    return True
        except Exception:
            pass
        return False

    def _finalize_job(self, job_id, status, error=None, run_id=None):
        try:
            with self.db.connect() as conn:
                now = utc_now_iso()
                
                # Update backtest_jobs
                updates = ["status = ?", "updated_at = ?", "processing_token = NULL"]
                params = [status, now]
                
                if error:
                    updates.append("last_error = ?")
                    params.append(error)
                    
                updates_str = ", ".join(updates)
                params.append(job_id)
                
                conn.execute(
                    f"UPDATE backtest_jobs SET {updates_str} WHERE id = ?",
                    params
                )
                
                # Also sync run status if cancelled/failed
                if run_id and status in ("cancelled", "failed"):
                    run_updates = ["status = ?", "updated_at = ?"]
                    run_params = [status, now]
                    
                    if error:
                        run_updates.append("error_message = ?")
                        run_params.append(error)
                        
                    run_updates_str = ", ".join(run_updates)
                    run_params.append(run_id)
                    
                    conn.execute(
                        f"UPDATE backtest_runs SET {run_updates_str} WHERE id = ?",
                        run_params
                    )
        except Exception as e:
            logger.error(f"Failed to finalize job {job_id}: {e}")

    def _handle_failure(self, row, status, error):
        job_id = row["id"]
        run_id = row["run_id"]
        attempts = row.get("attempts", 0) + 1
        
        # Retry Logic
        if status == "failed":
            if attempts >= 3:
                # Max retries reached
                self._finalize_job(job_id, "failed", error, run_id)
            else:
                # Schedule retry
                backoff_seconds = 60 * (2 ** (attempts - 1)) # 60s, 120s, 240s
                retry_dt = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
                retry_at = retry_dt.isoformat()
                now = utc_now_iso()
                
                try:
                    with self.db.connect() as conn:
                        conn.execute(
                            """
                            UPDATE backtest_jobs 
                            SET status = 'pending',
                                attempts = ?,
                                last_error = ?,
                                next_retry_at = ?,
                                processing_token = NULL,
                                updated_at = ?
                            WHERE id = ?
                            """,
                            (attempts, error or "Failed", retry_at, now, job_id)
                        )
                except Exception as e:
                    logger.error(f"Failed to schedule retry for job {job_id}: {e}")
        else:
            # Cancelled or other terminal status
            self._finalize_job(job_id, status, error, run_id)
