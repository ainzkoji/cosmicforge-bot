import logging
import traceback
import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timezone

from shared_lib.persistence.db import DB
from app.core.bot_instance_service import get_bot_instance_service, BotInstanceService
from app.core.broker_service import get_decrypted_credentials, resolve_broker_auth_for_bot
from app.exchange.binance.client import BinanceFuturesClient

# ✅ IMPORT PAPER RUNNER + CONTEXT (The Tested Path)
from app.runner.runner import PaperRunner
from app.runner.bot_context import BotRunContext
from app.exchange.factory import build_exchange_client, build_exchange_client_from_auth

from shared_lib.broker import BrokerResolverError

# ✅ IMPORT EQUITY SNAPSHOT SERVICE
from app.analytics.equity_snapshot_service import get_equity_snapshot_service

logger = logging.getLogger(__name__)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class MultiBotRunner:
    """
    Orchestrates the execution of multiple bot instances.
    Fetches active bots from DB and runs them sequentially via the Tested Engine (PaperRunner).
    """

    def __init__(self):
        self.service = get_bot_instance_service()
        # Ensure we have DB ref
        self.db = self.service.db
        self.iteration = 0
        self.running = False
        self._stop_requested = False
        self.last_snapshot_time: Dict[str, datetime] = {}
        self.bot_start_recorded: Set[str] = set()  # Track which bots have bot_start snapshot
        self.snapshot_service = get_equity_snapshot_service()
        
        # FIX: Persistent PaperRunner cache — keyed by bot_id.
        # Runners are created ONCE and reused across cycles so that:
        #   (a) DynamicThresholdCalculator rolling window accumulates samples over time
        #   (b) The 690-instrument registry refresh is not called every 15 seconds
        #   (c) TradingOrchestrator state is preserved across cycles
        # A runner is evicted from the cache on circuit-breaker open or config change.
        self._runners: Dict[str, PaperRunner] = {}
        
        # Circuit breaker for failure isolation
        self.error_counts: Dict[str, int] = {}  # bot_id -> consecutive error count
        self.circuit_open: Set[str] = set()  # bot_ids with open circuit breaker
        self.circuit_threshold = 5  # Max consecutive failures before opening circuit

        # ── Phase 4 TradingView processor state (per-bot) ─────────────────────
        # Overlap guard: True while a processor run is active for that bot.
        # Prevents double-processing if asyncio scheduling ever overlaps.
        self._ext_sig_running: Dict[str, bool] = {}
        
        # ========== CRITICAL: LOAD INSTRUMENT REGISTRY AT STARTUP ==========
        import sys
        try:
            from app.exchange.binance.client import BinanceFuturesClient
            from app.exchange.registry import get_instrument_registry
            from app.core.config import settings
            
            print("[REGISTRY] Initializing instrument registry...", file=sys.stderr)
            
            # Create temporary client for fetching specs
            temp_client = BinanceFuturesClient(
                api_key=settings.BINANCE_API_KEY or "dummy",
                api_secret=settings.BINANCE_API_SECRET or "dummy",
                base_url=settings.BINANCE_FAPI_BASE_URL
            )
            
            registry = get_instrument_registry()
            registry.refresh(broker_id="binance", client=temp_client, force=True)
            print("[REGISTRY] ✅ Successfully loaded instrument specs from Binance", file=sys.stderr)
        except Exception as e:
            print(f"[REGISTRY] ❌ CRITICAL ERROR loading instrument registry: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
        # ========== END REGISTRY INIT ==========

    async def run_loop(self, interval_seconds: int = 15):
        """
        Continuous execution loop.
        """
        self.running = True
        logger.info("Multi-Bot Runner Loop Started (Routed via PaperRunner)")

        # ── Phase 4 config visibility (logged once at startup) ───────────────
        try:
            from app.core.config import settings as _s
            _tv_enabled = getattr(_s, "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", False)
            _tv_testnet = getattr(_s, "TRADINGVIEW_TESTNET_ONLY", True)
            _tv_max     = getattr(_s, "TRADINGVIEW_QUEUE_MAX_PER_CYCLE", 3)
            _tv_live    = getattr(_s, "TRADINGVIEW_ALLOW_PAPER_LIVE_MODE", False)
            _tv_paper   = getattr(_s, "PAPER_TRADING_MODE", False)
            _binance_env = getattr(_s, "BINANCE_ENV", "testnet")
            logger.info(
                "[ExtSig] Phase 4 config — "
                "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=%s "
                "TRADINGVIEW_TESTNET_ONLY=%s "
                "TRADINGVIEW_QUEUE_MAX_PER_CYCLE=%d "
                "TRADINGVIEW_ALLOW_PAPER_LIVE_MODE=%s "
                "PAPER_TRADING_MODE=%s "
                "BINANCE_ENV=%s",
                _tv_enabled, _tv_testnet, _tv_max, _tv_live, _tv_paper, _binance_env,
            )
            if _tv_enabled:
                logger.info("[ExtSig] TradingView external signal processor ENABLED")
            else:
                logger.info("[ExtSig] TradingView external signal processor DISABLED (TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=false)")
        except Exception as _cfg_exc:
            logger.debug("[ExtSig] Could not read Phase 4 config at startup: %s", _cfg_exc)
        # ─────────────────────────────────────────────────────────────────────
        
        while self.running and not self._stop_requested:
            start_time = datetime.now()
            
            try:
                await self.run_once()
            except Exception as e:
                logger.critical(f"MultiBotRunner Loop Error: {e}")
                traceback.print_exc()
            
            # Sleep remainder of interval
            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_time = max(0.5, interval_seconds - elapsed)
            
            # Initial sleep in chunks to allow graceful stop
            slept = 0.0
            chunk = 0.5
            while slept < sleep_time and self.running and not self._stop_requested:
                await asyncio.sleep(chunk)
                slept += chunk
        
        logger.info("Multi-Bot Runner Loop Stopped")

    async def run_once(self):
        """Execution of one cycle for all bots using PaperRunner."""
        self.iteration += 1
        
        # ✅ DEBUG: Entry point (print bypasses log level filter)
        print(f"[CYCLE] >> Iteration {self.iteration} started")
        
        # 1. Fetch all instances for inventory/logging
        try:
            all_instances = self.service.get_all_bot_instances()
        except Exception as e:
            logger.error(f"Failed to fetch bot instances: {e}")
            return

        # Categorize instances for the cycle summary
        active_instances = []
        blocked_instances = []
        archived_deleted_count = 0
        
        for inst in all_instances:
            if inst.status in ("deleted", "archived"):
                archived_deleted_count += 1
            elif inst.status in ("active", "error") and getattr(inst, "broker_health_status", "ok") == "broker_blocked":
                blocked_instances.append(inst)
            elif inst.status in ("active", "error"):
                active_instances.append(inst)

        # ── Evict cached runners for bots no longer in the active set ─────────
        # Handles: soft-deleted bots, stopped bots, bots that became quarantined
        # since the last cycle.  Without this, a deleted bot's PaperRunner stays
        # in memory indefinitely even though its instance_id is gone from DB.
        active_ids = {inst.id for inst in active_instances}
        stale_ids = [bid for bid in list(self._runners.keys()) if bid not in active_ids]
        for stale_id in stale_ids:
            stale_runner = self._runners[stale_id]
            logger.warning(f"[RUNNER] Evicting stale runner {stale_id}. Executing safety flatten on live positions.")
            try:
                # Force close positions before eviction to prevent unmanaged exposure
                for sym in stale_runner.live_symbols:
                    try:
                        res = stale_runner.executor.execute_signal(sym, "CLOSE", 0.0)
                        if res.success:
                            logger.critical(f"[ORPHAN PREVENTION] Successfully flattened {sym} for evicted bot {stale_id}")
                    except Exception as e:
                        logger.error(f"[ORPHAN PREVENTION] Failed to flatten {sym} for {stale_id}: {e}")
            except Exception as e:
                logger.error(f"[ORPHAN PREVENTION] Safety flatten failed for bot {stale_id}: {e}")

            del self._runners[stale_id]
            logger.info("[RUNNER] Evicted stale runner for bot %s (no longer in active pool)", stale_id)

        # Print clean cycle summary
        print(f"[CYCLE] >> Iteration {self.iteration} | active_tradable={len(active_instances)} | blocked={len(blocked_instances)} | archived/deleted={archived_deleted_count} (ignored)")

        # Log broker-blocked bots each cycle so ops / UI can see them without
        # them re-entering the execution pool.
        if blocked_instances:
            for b in blocked_instances:
                logger.warning(
                    "bot_broker_blocked bot_id=%s account_id=%s reason=%s blocked_since=%s",
                    b.id,
                    b.broker_account_id,
                    getattr(b, "block_reason_code", getattr(b, "broker_error_code", "unknown")),
                    getattr(b, "blocked_since", getattr(b, "broker_blocked_at", "unknown")),
                )

        if not active_instances:
            print("[CYCLE] ⚠ No active tradable instances found — skipping execution phase")
            return

        logger.info(f"Cycle {self.iteration}: Processing {len(active_instances)} active bots via PaperRunner")

        # 2. Iterate and Execute
        for instance in active_instances:
            logger.info(f"[DEBUG] Processing bot {instance.id}")
            
            if not self.running or self._stop_requested:
                logger.info("[DEBUG] Stop requested, breaking loop")
                break
                
            try:
                # A. Resolve broker auth via canonical resolver.
                #    Uses broker_accounts.environment (authoritative) — NOT the credential blob.
                #    Raises BrokerResolverError with typed reason_code on every failure path.
                logger.info(f"[DEBUG] Bot {instance.id}: Resolving broker auth")
                try:
                    auth = resolve_broker_auth_for_bot(
                        account_id=instance.broker_account_id,
                        user_id=instance.user_id,
                    )
                except BrokerResolverError as exc:
                    # Surface the real reason instead of collapsing to "zero equity"
                    logger.error(
                        "bot_broker_auth_failed bot_id=%s account_id=%s reason=%s error=%s",
                        instance.id, instance.broker_account_id, exc.reason_code, exc,
                    )

                    # ── Quarantine the bot — remove it from the execution pool ──────
                    # The bot's user-visible status stays 'active' so it shows in
                    # dashboards with an actionable error instead of disappearing.
                    try:
                        self.service.quarantine_bot(
                            instance.id, exc.reason_code, str(exc)
                        )
                    except Exception as _qe:
                        logger.warning("quarantine_bot failed bot_id=%s: %s", instance.id, _qe)

                    # ── Evict cached runner so we start clean after repair ──────────
                    self._runners.pop(instance.id, None)

                    # ── Auto-invalidate broker account for non-transient errors ─────
                    # Transient errors (auth_failed, pending) may self-resolve.
                    # Non-transient errors mean the broker record is structurally
                    # broken and must be re-submitted by the user.
                    _NON_TRANSIENT = {
                        BrokerResolverError.REASON_ENV_MISMATCH,
                        BrokerResolverError.REASON_DECRYPT_FAILED,
                        BrokerResolverError.REASON_REVOKED,
                        BrokerResolverError.REASON_NO_CREDENTIALS,
                    }
                    if exc.reason_code in _NON_TRANSIENT and exc.account_id:
                        try:
                            with self.service.db.connect() as _conn:
                                _conn.execute(
                                    """
                                    UPDATE broker_accounts
                                    SET status           = 'invalid',
                                        validation_error = ?,
                                        updated_at       = ?
                                    WHERE id = ?
                                      AND status NOT IN ('invalid', 'deleted', 'revoked')
                                    """,
                                    (str(exc), utc_now_iso(), exc.account_id),
                                )
                            logger.warning(
                                "broker_auto_invalidated account_id=%s reason=%s",
                                exc.account_id, exc.reason_code,
                            )
                        except Exception as _be:
                            logger.warning(
                                "broker_auto_invalidate_failed account_id=%s: %s",
                                exc.account_id, _be,
                            )

                    continue

                # Build credentials dict for legacy BotRunContext shim
                creds = {
                    "api_key":    auth.api_key,
                    "api_secret": auth.api_secret,
                    "broker_type": auth.broker_type,
                    "environment": auth.environment.value,
                    "base_url":    auth.base_url,
                    **auth.extra,
                }

                # B. Fetch Configuration & Risk Params
                # Use internal presets based on instance.risk_level
                risk_params = BotInstanceService.get_risk_profile_preset(instance.risk_level)

                # C. Build Run Context — pass account_environment from auth (authoritative)
                context = BotRunContext.from_bot_instance(
                    instance=instance,
                    broker_credentials=creds,
                    risk_params=risk_params,
                    account_environment=auth.environment.value,
                )

                # D. Build client from auth (canonical URL, no environment inference)
                client = build_exchange_client_from_auth(auth)
                
                # E. Retrieve or create a PERSISTENT PaperRunner for this bot.
                #
                # FIX (process restart loop): Previously a new PaperRunner was created
                # every 15-second cycle, which:
                #   - Wiped the DynamicThresholdCalculator rolling window (stuck in cold-start)
                #   - Re-loaded 690 Binance instrument specs on every cycle (~1s overhead)
                #   - Re-initialised the TradingOrchestrator, DB connections, etc.
                #
                # Now: runners are cached by bot_id. A new runner is created only when
                # the bot is seen for the first time, or after a circuit-breaker eviction.
                if instance.id not in self._runners:
                    # First time seeing this bot — fully initialise.
                    # ========== POPULATE INSTRUMENT REGISTRY (ONCE PER BOT) ==========
                    from app.exchange.registry import get_instrument_registry
                    registry = get_instrument_registry()
                    registry.refresh(broker_id="binance", client=client, force=True)
                    logger.info(f"[REGISTRY] Loaded instrument specs for bot {instance.id} (first init)")
                    # ========== END REGISTRY INIT ==========

                    runner = PaperRunner(client, context=context)
                    self._runners[instance.id] = runner
                    _sym_count = len(getattr(runner, 'trade_symbols', None) or getattr(runner, 'symbols', []))
                    logger.info(
                        f"[RUNNER] Created new persistent PaperRunner for bot {instance.id} "
                        f"with {_sym_count} symbols"
                    )
                else:
                    # Reuse existing runner — update BOTH client references so the executor
                    # (which has its own .client attribute) also uses the refreshed connection.
                    runner = self._runners[instance.id]
                    runner.client = client              # Runner-level client
                    runner.executor.client = client    # Executor-level client (was missing!)
                    logger.info(
                        f"[RUNNER] Reusing persistent PaperRunner for bot {instance.id} "
                        f"(iteration {self.iteration})"
                    )
                
                _sym_count2 = len(getattr(runner, 'trade_symbols', None) or getattr(runner, 'symbols', []))
                print(f"[CYCLE] Bot {instance.id}: Starting cycle with {_sym_count2} symbols")
                
                # F. Execute Cycle (PaperRunner logic)
                # ✅ OFF-LOAD TO THREAD TO PREVENT EVENT LOOP BLOCKING
                cycle_result = await asyncio.to_thread(runner.run_cycle)
                
                # ✅ Log cycle completion
                print(f"[CYCLE] ✅ Bot {instance.id}: Cycle completed — status={cycle_result.get('status')} trades={cycle_result.get('trades_count', 0)}")

                # ── Phase 4: TradingView external signal processing ────────────
                if self._ext_sig_running.get(instance.id, False):
                    logger.info(
                        "[ExtSig] bot=%s processor still running from previous cycle — skipping",
                        instance.id,
                    )
                    # Write skipped heartbeat (non-blocking best-effort)
                    try:
                        from shared_lib.persistence.tradingview import upsert_processor_heartbeat
                        upsert_processor_heartbeat(
                            self.db,
                            bot_instance_id=instance.id,
                            processor_enabled=True,
                            last_skipped_reason="PREVIOUS_RUN_STILL_ACTIVE",
                        )
                    except Exception:
                        pass
                else:
                    self._ext_sig_running[instance.id] = True
                    _ext_start = utc_now_iso()
                    try:
                        from app.queue.external_signal_processor import ExternalSignalProcessor
                        from app.core.config import settings as _ext_settings
                        _ext_enabled = getattr(_ext_settings, "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", False)

                        if not _ext_enabled:
                            logger.debug("[ExtSig] bot=%s processor disabled — skipping", instance.id)
                        else:
                            logger.debug("[ExtSig] bot=%s processor started", instance.id)
                            _ext_proc = ExternalSignalProcessor(self.db)
                            _ext_results = await asyncio.to_thread(
                                _ext_proc.process_pending_for_bot,
                                instance.id,
                                runner,
                            )
                            _ext_finish = utc_now_iso()

                            _processed = sum(
                                1 for r in _ext_results
                                if r.get("outcome") in {"EXECUTED", "EXECUTED_PAPER"}
                            )
                            _rejected = sum(
                                1 for r in _ext_results
                                if (r.get("outcome") or "").startswith("REJECTED")
                            )
                            _failed = sum(
                                1 for r in _ext_results
                                if r.get("outcome") == "FAILED"
                            )
                            _skipped = sum(
                                1 for r in _ext_results
                                if r.get("outcome") == "SKIPPED_ALREADY_CLAIMED"
                            )

                            if _ext_results:
                                logger.info(
                                    "[ExtSig] bot=%s completed: total=%d "
                                    "processed=%d rejected=%d failed=%d skipped=%d",
                                    instance.id,
                                    len(_ext_results),
                                    _processed, _rejected, _failed, _skipped,
                                )

                            # Persist heartbeat for admin visibility
                            try:
                                import json as _json
                                from shared_lib.persistence.tradingview import upsert_processor_heartbeat
                                upsert_processor_heartbeat(
                                    self.db,
                                    bot_instance_id=instance.id,
                                    processor_enabled=True,
                                    env_gate_reason=None,
                                    last_started_at=_ext_start,
                                    last_finished_at=_ext_finish,
                                    last_processed_count=_processed,
                                    last_rejected_count=_rejected,
                                    last_failed_count=_failed,
                                    last_skipped_count=_skipped,
                                    last_result_json=_json.dumps(
                                        [{r["symbol"]: r.get("outcome")} for r in _ext_results]
                                    ) if _ext_results else None,
                                    last_error=None,
                                )
                            except Exception as _hb_exc:
                                logger.debug("[ExtSig] heartbeat write failed (non-fatal): %s", _hb_exc)

                    except Exception as _ext_exc:
                        _ext_finish = utc_now_iso()
                        logger.warning(
                            "[ExtSig] bot=%s processor error (caught safely): %s",
                            instance.id, _ext_exc,
                        )
                        # Persist error heartbeat
                        try:
                            from shared_lib.persistence.tradingview import upsert_processor_heartbeat
                            upsert_processor_heartbeat(
                                self.db,
                                bot_instance_id=instance.id,
                                processor_enabled=True,
                                last_started_at=_ext_start,
                                last_finished_at=_ext_finish,
                                last_error=str(_ext_exc),
                            )
                        except Exception:
                            pass
                    finally:
                        self._ext_sig_running[instance.id] = False
                # ──────────────────────────────────────────────────────────────

                # G. Record Equity Snapshots
                # Bot Start: First successful cycle
                if instance.id not in self.bot_start_recorded:
                    try:
                        await asyncio.to_thread(
                            self.snapshot_service.record_snapshot,
                            user_id=instance.user_id,
                            broker_account_id=instance.broker_account_id,
                            broker_id=instance.broker_id if instance.broker_id else "binance",
                            client=client,
                            source="bot_start",
                            bot_instance_id=instance.id
                        )
                        self.bot_start_recorded.add(instance.id)
                    except Exception as e:
                        logger.warning(f"Failed to record bot_start snapshot for {instance.id}: {e}")
                
                # Cycle End: After each cycle (with threshold)
                try:
                    await asyncio.to_thread(
                        self.snapshot_service.record_snapshot,
                        user_id=instance.user_id,
                        broker_account_id=instance.broker_account_id,
                        broker_id=instance.broker_id if instance.broker_id else "binance",
                        client=client,
                        source="cycle_end",
                        bot_instance_id=instance.id
                    )
                except Exception as e:
                    logger.debug(f"Cycle_end snapshot skipped or failed for {instance.id}: {e}")
                
                # H. Update Runtime State
                # Calculate active positions from runner state
                active_pos_count = len([s for s in runner.state.values() if s.position not in ("FLAT", "NONE")])
                trades_count = cycle_result.get("trades_count", 0)

                # ✅ Auto-recover bot status after a successful cycle
                # Clears 'error' status so the bot remains visible/active
                with self.db.connect() as conn:
                    conn.execute(
                        "UPDATE bot_instances SET status='active', last_error=NULL, updated_at=? WHERE id=? AND status='error'",
                        (utc_now_iso(), instance.id)
                    )

                self.service.update_instance_runtime_state(
                    instance.id,
                    last_run_at=utc_now_iso(),
                    last_run_id=context.run_id,
                    active_positions=active_pos_count
                )
                
                # Optionally log errors from cycle result
                results = cycle_result.get("results", [])
                
                # ✅ DEBUG: Show result count
                print(f"[CYCLE] Bot {instance.id}: Processing {len(results)} symbol results")
                
                # Handle both list and dict shapes to be safe
                result_items = results if isinstance(results, list) else list(results.values()) if isinstance(results, dict) else []
                
                for res in result_items:
                    if isinstance(res, dict):
                        # Some versions of res have the symbol as a key, some inside the dict
                        sym = res.get("symbol", "UNKNOWN")
                        if "error" in res:
                            logger.error(f"Bot {instance.id} symbol {sym} error: {res['error']}")
                        elif res.get("decision") == "blocked":
                            print(f"Bot {instance.id} symbol {sym} BLOCKED: {res.get('reason')}")
                        elif res.get("decision") == "execute":
                            details = res.get("details", {})
                            strat = details.get("strategy_output", {}) if isinstance(details, dict) else {}
                            sig = strat.get("signal", "?") if strat else "?"
                            conf = strat.get("confidence", 0.0) if strat else 0.0
                            print(f"[EXECUTE] 🚀 {sym}: signal={sig} conf={conf:.4f} → opening position")
                        

                
            except Exception as e:
                import traceback
                import sys
                error_msg = str(e)
                
                # ========== GUARANTEED ERROR VISIBILITY (BYPASS LOGGING) ==========
                print("\n" + "="*80, file=sys.stderr)
                print("🚨 BOT RUNNER EXCEPTION (TOP-LEVEL CATCH)", file=sys.stderr)
                print("="*80, file=sys.stderr)
                print(f"Bot Instance ID: {instance.id}", file=sys.stderr)
                print(f"Broker Account: {instance.broker_account_id}", file=sys.stderr)
                print(f"Error Type: {type(e).__name__}", file=sys.stderr)
                print(f"Error Message: {str(e)}", file=sys.stderr)
                print("\nFull Stack Trace:", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                print("="*80 + "\n", file=sys.stderr)
                
                # Enhanced error handling with broker-specific messages
                broker_id = instance.broker_account_id.lower() if instance.broker_account_id else ""
                
                # IBKR-specific error handling
                if "ibkr" in broker_id:
                    if "session" in error_msg.lower() or "auth" in error_msg.lower():
                        error_msg = "IBKR session expired. Re-authenticate via Client Portal Gateway."
                    elif "gateway" in error_msg.lower() or "connection" in error_msg.lower():
                        error_msg = "IBKR gateway not responding. Ensure gateway is running."
                    elif "not authenticated" in error_msg.lower():
                        error_msg = "IBKR not authenticated. Log in via gateway web interface."
                
                logger.error(f"Error running bot {instance.id} ({broker_id}): {error_msg}")
                
                # Record failure for circuit breaker
                self._record_failure(instance.id)
                
                # Evict the cached runner on circuit-breaker open so the next
                # cycle creates a fresh one (clean state after sustained errors).
                if instance.id in self.circuit_open and instance.id in self._runners:
                    logger.warning(
                        f"[RUNNER] Evicting cached runner for bot {instance.id} "
                        f"due to circuit breaker open (will recreate on next cycle)"
                    )
                    del self._runners[instance.id]
                
                self.service.update_instance_runtime_state(
                    instance.id,
                    error_message=error_msg
                )

        # ── Step 5F-2 Shadow Outcome Evaluation ────────────────────────────────
        # After all bots finish their cycle, check if shadow evaluations are needed.
        # Uses the first active Binance client available.
        try:
            from app.shadow.evaluator import get_shadow_evaluator
            from app.shadow.config import get_shadow_config
            _cfg = get_shadow_config()
            
            if _cfg.enabled:
                # Find the first valid binance client (to reuse its connection)
                eval_client = None
                for b_id, _runner in self._runners.items():
                    if getattr(_runner, "client", None):
                        eval_client = _runner.client
                        break
                
                if eval_client:
                    _evaluator = get_shadow_evaluator()
                    # Execute in thread to avoid blocking loop
                    _eval_sum = await asyncio.to_thread(
                        _evaluator.run_evaluation_pass, eval_client, "15m"
                    )
                    if _cfg.cycle_summary and _eval_sum.get("evaluated", 0) > 0:
                        logger.info(
                            "[SHADOW] Evaluated %d trades (TP: %d, SL: %d, EXPIRED: %d)",
                            _eval_sum.get("evaluated", 0),
                            _eval_sum.get("tp_hit", 0),
                            _eval_sum.get("sl_hit", 0),
                            _eval_sum.get("expired", 0)
                        )
        except Exception as exc:
            logger.debug("[Shadow] Evaluation pass failed: %s", exc)
        # ────────────────────────────────────────────────────────────────────────
    
    def _should_skip_bot(self, bot_id: str) -> bool:
        """Check if bot should be skipped due to circuit breaker."""
        if bot_id in self.circuit_open:
            error_count = self.error_counts.get(bot_id, 0)
            if error_count >= self.circuit_threshold:
                logger.warning(f"Circuit breaker OPEN for bot {bot_id} ({error_count} failures)")
                return True
        return False
    
    def _record_success(self, bot_id: str):
        """Reset error counter on success."""
        if bot_id in self.error_counts:
            prev_count = self.error_counts[bot_id]
            if prev_count > 0:
                logger.info(f"Bot {bot_id}: Circuit breaker reset (was {prev_count} errors)")
        self.error_counts[bot_id] = 0
        self.circuit_open.discard(bot_id)
    
    def _record_failure(self, bot_id: str):
        """Increment error counter and open circuit if threshold reached."""
        self.error_counts[bot_id] = self.error_counts.get(bot_id, 0) + 1
        count = self.error_counts[bot_id]
        
        if count >= self.circuit_threshold:
            self.circuit_open.add(bot_id)
            logger.error(f"Circuit breaker OPENED for bot {bot_id} after {count} failures")

    def stop(self):
        self._stop_requested = True
        self.running = False

    async def flatten_all(self) -> List[Dict[str, Any]]:
        """
        Emergency flatten: Closes all positions for all active bots in the cache.
        """
        overall_results = []
        # Local copy to avoid mutation during iteration
        bot_ids = list(self._runners.keys())
        
        for bot_id in bot_ids:
            runner = self._runners.get(bot_id)
            if not runner:
                continue
            
            bot_results = {"bot_id": bot_id, "symbols": []}
            # PaperRunner.trade_symbols is the authoritative list (getattr for safety)
            _runner_symbols = getattr(runner, 'trade_symbols', None) or getattr(runner, 'symbols', [])
            for sym in _runner_symbols:
                sym = sym.upper()
                res = {"symbol": sym}
                try:
                    # Use the runner's specific executor and client
                    runner.executor.client.cancel_all_orders(sym)
                    res["cancel_orders"] = "ok"
                    
                    # Close the position via executor
                    close_res = runner.executor.execute_signal(sym, "CLOSE", 0.0)
                    res["close_position"] = close_res.details if hasattr(close_res, 'details') else str(close_res)
                    
                    # Clear local state
                    if sym in runner.state:
                       st = runner.state[sym]
                       st.position = "NONE"
                       st.entry_qty = 0.0
                       st.entry_price = None
                       if hasattr(runner, "store"):
                           runner.store.save_symbol(sym, st)
                           
                except Exception as e:
                    res["error"] = str(e)
                
                bot_results["symbols"].append(res)
            
            overall_results.append(bot_results)
            
        return overall_results
