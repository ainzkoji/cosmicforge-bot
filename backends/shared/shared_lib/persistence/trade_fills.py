from __future__ import annotations

from typing import Any, Optional
import logging

from shared_lib.persistence.db import DB, utc_now_iso
from app.ops.context import get_run_id, get_cycle_id

logger = logging.getLogger(__name__)


def get_recent_regime_outcomes(
    db: DB,
    regime: str,
    limit: int = 50
) -> dict[str, list[bool]]:
    """
    Fetch the most recent trade outcomes (win/loss) per strategy, specifically
    for trades that occurred during the specified market regime.
    Returns: dict[strategy_name, list[is_win]]
    """
    outcomes = {}
    
    with db.connect() as conn:
        # We find trades where action='CLOSE', filtering by the notes/meta if regime was stored,
        # but since regime isn't historically stored in trade_fills yet, we will fetch the last N closes
        # globally to warm up the tracker as a fallback until regime DB logging is added.
        rows = conn.execute(
            """
            SELECT strategy, realized_pnl
            FROM trade_fills
            WHERE action = 'CLOSE' AND strategy != 'unknown' AND strategy != 'master_ensemble'
            ORDER BY timestamp_utc DESC
            LIMIT ?
            """,
            (limit * 7,) # Pull enough for all sub-strategies
        ).fetchall()
        
        for row in reversed(rows): # Reverse so oldest is first, appended sequentially
            strat = row["strategy"]
            pnl = row["realized_pnl"]
            if pnl is None:
                continue
            
            is_win = pnl > 0
            
            if strat not in outcomes:
                outcomes[strat] = []
            
            # Keep only the most recent `limit` trades per strategy
            if len(outcomes[strat]) >= limit:
                outcomes[strat].pop(0)
                
            outcomes[strat].append(is_win)
            
    return outcomes

class ExitReason:
    """
    Allowed values for the exit_reason field in trade_fills.

    E-2 standardised names: EXIT_* aliases map to canonical short codes.
    Both forms are in ALL so old and new callers are accepted.
    """
    # Short canonical codes (original)
    SL = "SL"
    TRAILING_SL = "TRAILING_SL"
    BREAK_EVEN = "BREAK_EVEN"
    # FIX-D: post-TP1 buffered stop-out (stop at entry + 20% of original SL distance)
    # Distinct from BREAK_EVEN (fee-only stop at exactly entry) for analytics clarity.
    BREAK_EVEN_BUFFER = "BREAK_EVEN_BUFFER"
    TP1 = "TP1"
    TP2 = "TP2"
    TIME_EXIT = "TIME_EXIT"
    SIGNAL_REVERSAL = "SIGNAL_REVERSAL"
    MANUAL = "MANUAL"
    KILL_SWITCH = "KILL_SWITCH"
    OTHER = "OTHER"

    # E-2 standardised EXIT_* aliases
    EXIT_STOP_LOSS = "SL"
    EXIT_TRAILING_STOP = "TRAILING_SL"
    EXIT_BREAK_EVEN = "BREAK_EVEN"
    EXIT_BREAK_EVEN_BUFFER = "BREAK_EVEN_BUFFER"
    EXIT_TP1 = "TP1"
    EXIT_TP2 = "TP2"
    EXIT_TIME_BASED = "TIME_EXIT"
    EXIT_DAILY_CLOSE = "DAILY_CLOSE"
    EXIT_KILL_SWITCH = "KILL_SWITCH"
    EXIT_MANUAL = "MANUAL"
    EXIT_RISK_LIMIT = "RISK_LIMIT"
    EXIT_EVENT_BLACKOUT = "EVENT_BLACKOUT"
    EXIT_UNKNOWN = "OTHER"

    ALL: frozenset = frozenset({
        SL, TRAILING_SL, BREAK_EVEN, BREAK_EVEN_BUFFER, TP1, TP2,
        TIME_EXIT, SIGNAL_REVERSAL, MANUAL, KILL_SWITCH, OTHER,
        "DAILY_CLOSE", "RISK_LIMIT", "EVENT_BLACKOUT",
    })


def calculate_close_pnl(
    *,
    side: str,
    entry_price: float,
    exit_price: float,
    quantity: float,
    entry_fee: float = 0.0,
    exit_fee: float = 0.0,
    funding_fees: float = 0.0,
    slippage_cost: float = 0.0,
    initial_risk: Optional[float] = None,
) -> dict:
    """
    E-2: Calculate standardised close PnL metrics.

    Returns dict with: gross_pnl, total_fees, net_pnl, r_multiple.
    For LONG:  gross_pnl = (exit - entry) * qty
    For SHORT: gross_pnl = (entry - exit) * qty
    """
    side_upper = (side or "").upper()
    if side_upper in ("LONG", "BUY"):
        gross_pnl = (exit_price - entry_price) * quantity
    else:
        gross_pnl = (entry_price - exit_price) * quantity

    total_fees = entry_fee + exit_fee + funding_fees
    net_pnl = gross_pnl - total_fees - slippage_cost

    r_multiple = None
    if initial_risk and initial_risk > 0:
        r_multiple = net_pnl / initial_risk

    return {
        "gross_pnl": round(gross_pnl, 8),
        "total_fees": round(total_fees, 8),
        "funding_fees": round(funding_fees, 8),
        "net_pnl": round(net_pnl, 8),
        "r_multiple": round(r_multiple, 4) if r_multiple is not None else None,
    }


def record_fill(
    db: DB,
    symbol: str,
    side: str,  # LONG/SHORT
    action: str,  # OPEN/CLOSE
    qty: float,
    price: float,
    fee: Optional[float] = None,
    realized_pnl: Optional[float] = None,
    pnl: Optional[float] = None,
    notes: Optional[dict[str, Any]] = None,  # ignored here
    *,
    strategy: str = "unknown",
    strategy_version: str = "0",
    broker_id: str = "binance_futures",
    account_id: str = "default",
    asset_class: str = "CRYPTO",
    timeframe: str = "",
    confidence: Optional[float] = None,
    user_id: Optional[str] = None,
    broker_account_id: Optional[str] = None,
    bot_instance_id: Optional[str] = None,
    client: Optional[Any] = None,  # Exchange client for snapshot
    slippage_pct: Optional[float] = None,
    entry_price_expected: Optional[float] = None,
    stop_loss_price: Optional[float] = None,
    position_id: Optional[str] = None,
    r_multiple: Optional[float] = None,
    exit_reason: Optional[str] = None,
    mfe_pct: Optional[float] = None,
    mae_pct: Optional[float] = None,
    exit_regime: Optional[str] = None,
    exit_regime_confidence: Optional[float] = None,
    order_id: Optional[str] = None,
    initiator_type: Optional[str] = None,
    trigger_source: Optional[str] = None,
    position_phase: Optional[str] = None,
    time_in_trade_sec: Optional[float] = None,
    sl_at_exit: Optional[float] = None,
    tp_at_exit: Optional[float] = None,
    market_price_used: Optional[float] = None,
    price_source: Optional[str] = None,
    opposite_signal_detected: Optional[bool] = None,
    ensemble_decision: Optional[str] = None,
    risk_force_close: Optional[bool] = None,
    sync_state_before: Optional[str] = None,
    sync_state_after: Optional[str] = None,
    close_order_type: Optional[str] = None,
    broker_response: Optional[str] = None,
    expected_close: Optional[bool] = None,
    run_id: Optional[str] = None,
    cycle_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    # E-2: Extended close performance fields
    gross_pnl: Optional[float] = None,
    total_fees: Optional[float] = None,
    funding_fees: Optional[float] = None,
    net_pnl: Optional[float] = None,
    net_pnl_percent: Optional[float] = None,
    entry_fee: Optional[float] = None,
    exit_fee: Optional[float] = None,
    fees_estimated: Optional[bool] = None,
    slippage_estimated: Optional[bool] = None,
) -> None:
    if realized_pnl is None and pnl is not None:
        realized_pnl = pnl
    # E-2: if net_pnl provided use it as realized_pnl if not already set
    if realized_pnl is None and net_pnl is not None:
        realized_pnl = net_pnl

    effective_run_id = run_id if run_id is not None else get_run_id()
    effective_cycle_id = cycle_id if cycle_id is not None else get_cycle_id()

    is_external_or_backfill = (account_id or "").lower() in {"backfill", "manual", "external"}
    requires_bot_lineage = bool(bot_instance_id) and not is_external_or_backfill
    if requires_bot_lineage:
        if not effective_run_id or not effective_cycle_id:
            raise ValueError(
                f"Bot-generated fill missing run_id/cycle_id: {action} {side} {symbol}"
            )
        if action in {"OPEN", "CLOSE"} and not position_id:
            raise ValueError(
                f"Bot-generated fill missing position_id: {action} {side} {symbol}"
            )
        if action == "OPEN" and not trace_id:
            raise ValueError(
                f"Bot-generated OPEN fill missing trace_id: {side} {symbol}"
            )

    # Validate and normalise exit_reason
    if exit_reason is not None and exit_reason not in ExitReason.ALL:
        logger.warning("record_fill: unknown exit_reason %r — storing as-is", exit_reason)
    if action == "CLOSE" and exit_reason is None:
        exit_reason = ExitReason.OTHER

    # Enforce MFE/MAE invariants silently — only relevant on CLOSE fills
    if mfe_pct is not None:
        mfe_pct = max(0.0, float(mfe_pct))
    if mae_pct is not None:
        mae_pct = min(0.0, float(mae_pct))

    # Warn (not block) when slippage is not populated — helps track data quality
    if slippage_pct is None and action in ("OPEN", "CLOSE"):
        logger.warning(
            "record_fill: slippage_pct is NULL for %s %s %s — execution quality data incomplete",
            action, side, symbol,
        )

    # Extract quote/base currency from symbol for FOREX readiness
    quote_curr = None
    base_curr = None
    if symbol:
        symbol_upper = symbol.upper()
        for quote in ['USDT', 'USDC', 'USD', 'BUSD', 'EUR', 'GBP', 'JPY']:
            if symbol_upper.endswith(quote):
                quote_curr = quote
                base_curr = symbol_upper[:-len(quote)]
                break
        if quote_curr is None and asset_class == "CRYPTO":
            quote_curr = "USDT"
            base_curr = symbol_upper.replace("USDT", "").replace("USDC", "").replace("BUSD", "")

    with db.connect() as conn:
        # --- Ensure trade_fills.user_id is persisted for new fills when possible ---
        # Historical rows may have NULL user_id; newer analytics prefer user_id present.
        # Do not change execution behaviour: best-effort enrichment only.
        if (user_id is None or user_id == "") and bot_instance_id:
            try:
                row = conn.execute(
                    "SELECT user_id FROM bot_instances WHERE id = ? LIMIT 1",
                    (bot_instance_id,),
                ).fetchone()
                if row is not None and row["user_id"]:
                    user_id = row["user_id"]
            except Exception as e:
                logger.warning(
                    "record_fill: could not derive user_id for bot_instance_id=%s (%s)",
                    bot_instance_id,
                    e,
                )

        effective_trace_id = trace_id
        if action == "CLOSE" and not effective_trace_id and position_id:
            row = conn.execute(
                """
                SELECT trace_id
                FROM trade_fills
                WHERE action = 'OPEN'
                  AND position_id = ?
                  AND trace_id IS NOT NULL
                ORDER BY timestamp_utc DESC, id DESC
                LIMIT 1
                """,
                (position_id,),
            ).fetchone()
            if row is not None:
                effective_trace_id = row["trace_id"]

        conn.execute(
            """
            INSERT INTO trade_fills (
                run_id, cycle_id, trace_id, symbol, side, action, qty, price, fee, realized_pnl,
                strategy, strategy_version, broker_id, account_id, asset_class, timeframe, confidence,
                user_id, bot_instance_id, broker_account_id, quote_currency, base_currency,
                timestamp_utc, slippage_pct, entry_price_expected, stop_loss_price, position_id, r_multiple,
                exit_reason, mfe_pct, mae_pct, exit_regime, exit_regime_confidence, order_id,
                initiator_type, trigger_source, position_phase, time_in_trade_sec, sl_at_exit, tp_at_exit,
                market_price_used, price_source, opposite_signal_detected, ensemble_decision,
                risk_force_close, sync_state_before, sync_state_after, close_order_type,
                broker_response, expected_close,
                gross_pnl, total_fees, funding_fees, net_pnl, net_pnl_percent,
                entry_fee, exit_fee, fees_estimated, slippage_estimated
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                effective_run_id,
                effective_cycle_id,
                effective_trace_id,
                symbol,
                side,
                action,
                float(qty),
                float(price),
                float(fee) if fee is not None else None,
                float(realized_pnl) if realized_pnl is not None else None,
                strategy,
                strategy_version,
                broker_id,
                account_id,
                asset_class,
                timeframe,
                float(confidence) if confidence is not None else None,
                user_id,
                bot_instance_id,
                broker_account_id,
                quote_curr,
                base_curr,
                utc_now_iso(),
                float(slippage_pct) if slippage_pct is not None else None,
                float(entry_price_expected) if entry_price_expected is not None else None,
                float(stop_loss_price) if stop_loss_price is not None else None,
                position_id,
                float(r_multiple) if r_multiple is not None else None,
                exit_reason,
                mfe_pct,
                mae_pct,
                exit_regime if action == "CLOSE" else None,
                float(exit_regime_confidence) if (action == "CLOSE" and exit_regime_confidence is not None) else None,
                order_id,
                initiator_type,
                trigger_source,
                position_phase,
                time_in_trade_sec,
                sl_at_exit,
                tp_at_exit,
                market_price_used,
                price_source,
                1 if opposite_signal_detected else 0 if opposite_signal_detected is not None else None,
                ensemble_decision,
                1 if risk_force_close else 0 if risk_force_close is not None else None,
                sync_state_before,
                sync_state_after,
                close_order_type,
                broker_response,
                1 if expected_close else 0 if expected_close is not None else None,
                # E-2 extended performance fields
                float(gross_pnl) if gross_pnl is not None else None,
                float(total_fees) if total_fees is not None else None,
                float(funding_fees) if funding_fees is not None else None,
                float(net_pnl) if net_pnl is not None else None,
                float(net_pnl_percent) if net_pnl_percent is not None else None,
                float(entry_fee) if entry_fee is not None else None,
                float(exit_fee) if exit_fee is not None else None,
                1 if fees_estimated else 0 if fees_estimated is not None else None,
                1 if slippage_estimated else 0 if slippage_estimated is not None else None,
            ),
        )

    
    
    # Record equity snapshot on position close
    if action == "CLOSE" and client is not None and user_id and broker_account_id:
        try:
            from app.analytics.equity_snapshot_service import get_equity_snapshot_service
            snapshot_service = get_equity_snapshot_service()
            snapshot_service.record_snapshot(
                user_id=user_id,
                broker_account_id=broker_account_id,
                broker_id=broker_id,
                client=client,
                source="trade_close",
                bot_instance_id=bot_instance_id
            )
        except Exception as e:
            logger.debug(f"Failed to record trade_close snapshot: {e}")

    # Invalidate Analytics Cache (Phase 7)
    if user_id:
        try:
            from shared_lib.persistence.analytics_cache import AnalyticsCacheService
            cache = AnalyticsCacheService(db=db)
            cache.invalidate(
                user_id=user_id,
                bot_instance_id=bot_instance_id,
                broker_account_id=broker_account_id
            )
        except Exception as e:
            logger.warning(f"Failed to invalidate analytics cache: {e}")
