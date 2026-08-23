"""
Shadow Trading System — Outcome Evaluator

ShadowEvaluator processes PENDING shadow trades by fetching future market data
and determining what would have happened (TP_HIT, SL_HIT, EXPIRED, DATA_MISSING).

SAFETY CONTRACT:
  - Never calls place_order() or execute_signal()
  - Never writes to trade_fills or decision_traces
  - Only reads from exchange for historical OHLCV data
  - Only writes to shadow_trades and shadow_trade_outcomes

Intrabar ambiguity rule (documented):
  When both TP and SL would be hit within the same candle, we assume SL hit
  first (conservative). This is recorded in evaluation_notes.ambiguity_applied.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.shadow.config import get_shadow_config
from app.shadow.store import (
    ShadowStore, get_shadow_store,
    OUTCOME_TP_HIT, OUTCOME_SL_HIT, OUTCOME_EXPIRED,
    OUTCOME_DATA_MISSING, STATUS_EVALUATING, STATUS_DATA_MISSING,
)

logger = logging.getLogger(__name__)


def _parse_iso(ts: str) -> Optional[datetime]:
    """Parse ISO timestamp string to datetime (UTC)."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _interval_to_minutes(interval: str) -> int:
    mapping = {
        "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
        "1h": 60, "2h": 120, "4h": 240, "6h": 360,
        "8h": 480, "12h": 720, "1d": 1440,
    }
    return mapping.get(interval, 15)


class ShadowEvaluator:
    """
    Evaluates PENDING shadow trades using future market data.

    Called asynchronously from MultiBotRunner.run_once() after the main
    trading cycle completes — never on the critical execution path.
    """

    def __init__(self, store: Optional[ShadowStore] = None):
        self._store = store or get_shadow_store()
        self._cfg = get_shadow_config()

    def run_evaluation_pass(
        self,
        client: Any,
        interval: str = "15m",
    ) -> Dict[str, int]:
        """
        Process up to max_eval_per_pass PENDING shadow trades.

        Args:
            client: BinanceFuturesClient (or compatible) with .klines() method
            interval: Default candle interval (used when trade doesn't specify one)

        Returns:
            Summary dict: {evaluated, tp_hit, sl_hit, expired, data_missing, errors}
        """
        if not self._cfg.enabled:
            return {}

        pending = self._store.get_pending_trades(limit=self._cfg.max_eval_per_pass)
        if not pending:
            return {"evaluated": 0}

        summary: Dict[str, int] = {
            "evaluated": 0, "tp_hit": 0, "sl_hit": 0,
            "expired": 0, "data_missing": 0, "errors": 0,
        }

        for trade in pending:
            try:
                # Mark as evaluating to prevent double-processing
                self._store.update_status(trade["id"], STATUS_EVALUATING)

                outcome_data = self.evaluate_one(trade, client, interval)

                self._store.insert_outcome(
                    shadow_trade_id=trade["id"],
                    **outcome_data,
                )

                summary["evaluated"] += 1
                outcome_key = outcome_data["outcome"].lower().replace("_hit", "_hit")
                if outcome_key in summary:
                    summary[outcome_key] += 1
                else:
                    # Map generic outcomes
                    if outcome_data["outcome"] == OUTCOME_TP_HIT:
                        summary["tp_hit"] += 1
                    elif outcome_data["outcome"] == OUTCOME_SL_HIT:
                        summary["sl_hit"] += 1
                    elif outcome_data["outcome"] == OUTCOME_EXPIRED:
                        summary["expired"] += 1
                    elif outcome_data["outcome"] == OUTCOME_DATA_MISSING:
                        summary["data_missing"] += 1

                # Brief sleep between evaluations to respect rate limits
                time.sleep(0.05)

            except Exception as exc:
                logger.error(
                    "[ShadowEvaluator] evaluate_one failed for shadow_id=%s symbol=%s: %s",
                    trade.get("id"), trade.get("symbol"), exc, exc_info=True,
                )
                # Mark as data_missing rather than leaving in EVALUATING state
                try:
                    self._store.update_status(trade["id"], STATUS_DATA_MISSING)
                except Exception:
                    pass
                summary["errors"] += 1

        return summary

    def evaluate_one(
        self,
        trade: Dict[str, Any],
        client: Any,
        default_interval: str = "15m",
    ) -> Dict[str, Any]:
        """
        Evaluate a single shadow trade.

        Returns a dict matching the insert_outcome() kwargs.
        """
        symbol = trade["symbol"]
        side = trade.get("side") or "BUY"
        entry_price = trade.get("entry_price")
        stop_loss = trade.get("stop_loss")
        take_profit = trade.get("take_profit")
        entry_time_str = trade.get("entry_time")
        expiry_time_str = trade.get("expiry_time")
        interval = default_interval  # Future: store per-trade interval

        # ── Validate required fields ──────────────────────────────────────────
        if not entry_price or entry_price <= 0:
            return self._data_missing_result("entry_price is null or zero")

        if not stop_loss or not take_profit:
            return self._data_missing_result("stop_loss or take_profit is null")

        entry_price = float(entry_price)
        stop_loss = float(stop_loss)
        take_profit = float(take_profit)

        # ── Parse times ───────────────────────────────────────────────────────
        entry_dt = _parse_iso(entry_time_str) if entry_time_str else None
        expiry_dt = _parse_iso(expiry_time_str) if expiry_time_str else None
        now_dt = datetime.now(timezone.utc)

        if not entry_dt:
            return self._data_missing_result("entry_time could not be parsed")

        # ── Fetch future klines ───────────────────────────────────────────────
        bars_needed = self._cfg.expiry_bars + 5  # extra buffer
        try:
            klines = client.klines(
                symbol=symbol,
                interval=interval,
                limit=bars_needed,
            )
        except Exception as exc:
            logger.warning(
                "[ShadowEvaluator] klines fetch failed for %s: %s", symbol, exc
            )
            return self._data_missing_result(f"klines fetch failed: {exc}")

        if not klines:
            return self._data_missing_result("klines returned empty list")

        # ── Filter klines to those AFTER entry time ───────────────────────────
        # Klines format: [open_time, open, high, low, close, volume, ...]
        interval_ms = _interval_to_minutes(interval) * 60 * 1000
        try:
            entry_ts_ms = int(entry_dt.timestamp() * 1000)
        except Exception:
            return self._data_missing_result("could not convert entry_time to ms")

        post_entry_klines = [
            k for k in klines
            if isinstance(k, (list, tuple)) and len(k) >= 5
            and int(k[0]) >= entry_ts_ms
        ]

        if not post_entry_klines:
            # All klines are before entry — check if expired already
            if expiry_dt and now_dt > expiry_dt:
                return self._expired_result(
                    entry_price=entry_price,
                    bars_elapsed=0,
                    interval=interval,
                    notes={"reason": "all_klines_before_entry_and_expired"},
                )
            return self._data_missing_result("no post-entry klines found")

        # ── Walk bars to find TP/SL hit ───────────────────────────────────────
        is_long = side.upper() in ("BUY", "LONG")
        mfe: float = 0.0   # max favorable excursion from entry
        mae: float = 0.0   # max adverse excursion from entry
        outcome = None
        exit_bar_idx = 0
        exit_price: Optional[float] = None
        ambiguity_applied = False

        for bar_idx, bar in enumerate(post_entry_klines):
            try:
                bar_open = float(bar[1])
                bar_high = float(bar[2])
                bar_low = float(bar[3])
                bar_close = float(bar[4])
                bar_open_ms = int(bar[0])
            except (IndexError, ValueError, TypeError) as exc:
                logger.debug("[ShadowEvaluator] bad bar data: %s", exc)
                continue

            # ── Excursion tracking ────────────────────────────────────────────
            if is_long:
                favorable = bar_high - entry_price
                adverse = entry_price - bar_low
            else:
                favorable = entry_price - bar_low
                adverse = bar_high - entry_price

            mfe = max(mfe, favorable)
            mae = max(mae, adverse)

            # ── TP / SL hit detection ─────────────────────────────────────────
            if is_long:
                tp_hit_this_bar = bar_high >= take_profit
                sl_hit_this_bar = bar_low <= stop_loss
            else:
                tp_hit_this_bar = bar_low <= take_profit
                sl_hit_this_bar = bar_high >= stop_loss

            if tp_hit_this_bar and sl_hit_this_bar:
                # Intrabar ambiguity: conservative rule = SL wins
                outcome = OUTCOME_SL_HIT
                exit_price = stop_loss
                exit_bar_idx = bar_idx
                ambiguity_applied = True
                break
            elif tp_hit_this_bar:
                outcome = OUTCOME_TP_HIT
                exit_price = take_profit
                exit_bar_idx = bar_idx
                break
            elif sl_hit_this_bar:
                outcome = OUTCOME_SL_HIT
                exit_price = stop_loss
                exit_bar_idx = bar_idx
                break

            # ── Check expiry ──────────────────────────────────────────────────
            bar_open_dt = datetime.fromtimestamp(bar_open_ms / 1000, tz=timezone.utc)
            if expiry_dt and bar_open_dt >= expiry_dt:
                outcome = OUTCOME_EXPIRED
                exit_price = bar_close
                exit_bar_idx = bar_idx
                break

        # If we exhausted bars without hitting outcome, check if expired
        if outcome is None:
            if expiry_dt and now_dt >= expiry_dt:
                outcome = OUTCOME_EXPIRED
                exit_price = float(post_entry_klines[-1][4]) if post_entry_klines else None
                exit_bar_idx = len(post_entry_klines) - 1
            else:
                # Not expired yet — should not happen since we only evaluate
                # PENDING trades, but guard against edge cases
                return self._data_missing_result("outcome undetermined — trade still open")

        # ── Compute PnL ───────────────────────────────────────────────────────
        pnl_abs: Optional[float] = None
        pnl_pct: Optional[float] = None
        pnl_net: Optional[float] = None

        if exit_price is not None and entry_price > 0:
            if is_long:
                pnl_abs = exit_price - entry_price
            else:
                pnl_abs = entry_price - exit_price

            pnl_pct = (pnl_abs / entry_price) * 100.0
            fee_cost = entry_price * self._cfg.assumed_fee_rate
            pnl_net = pnl_abs - fee_cost

        # ── Compute timing ────────────────────────────────────────────────────
        bars_elapsed = exit_bar_idx
        minutes_elapsed = bars_elapsed * _interval_to_minutes(interval)

        # ── Build exit time ───────────────────────────────────────────────────
        try:
            exit_bar_ms = int(post_entry_klines[exit_bar_idx][0])
            exit_time = datetime.fromtimestamp(
                exit_bar_ms / 1000, tz=timezone.utc
            ).isoformat()
        except Exception:
            exit_time = None

        # ── Build evaluation notes ────────────────────────────────────────────
        notes: Dict[str, Any] = {
            "bars_scanned": len(post_entry_klines),
            "interval": interval,
            "ambiguity_rule_applied": ambiguity_applied,
        }
        if ambiguity_applied:
            notes["ambiguity_rule"] = (
                "Both TP and SL hit in same candle. "
                "Conservative rule applied: SL assumed to hit first."
            )

        return dict(
            outcome=outcome,
            exit_time=exit_time,
            exit_price=exit_price,
            pnl_abs=pnl_abs,
            pnl_pct=pnl_pct,
            pnl_net=pnl_net,
            mfe=mfe,
            mae=mae,
            bars_elapsed=bars_elapsed,
            minutes_elapsed=float(minutes_elapsed),
            evaluation_notes=notes,
        )

    # ── Helper constructors ───────────────────────────────────────────────────

    def _data_missing_result(self, reason: str) -> Dict[str, Any]:
        return dict(
            outcome=OUTCOME_DATA_MISSING,
            exit_time=None,
            exit_price=None,
            pnl_abs=None,
            pnl_pct=None,
            pnl_net=None,
            mfe=None,
            mae=None,
            bars_elapsed=None,
            minutes_elapsed=None,
            evaluation_notes={"reason": reason, "data_missing": True},
        )

    def _expired_result(
        self,
        entry_price: float,
        bars_elapsed: int,
        interval: str,
        notes: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        minutes_elapsed = bars_elapsed * _interval_to_minutes(interval)
        return dict(
            outcome=OUTCOME_EXPIRED,
            exit_time=datetime.now(timezone.utc).isoformat(),
            exit_price=entry_price,  # approximate — closed at near entry
            pnl_abs=0.0,
            pnl_pct=0.0,
            pnl_net=-(entry_price * self._cfg.assumed_fee_rate),
            mfe=None,
            mae=None,
            bars_elapsed=bars_elapsed,
            minutes_elapsed=float(minutes_elapsed),
            evaluation_notes=notes or {},
        )


# ── Singleton ─────────────────────────────────────────────────────────────────
_evaluator: Optional[ShadowEvaluator] = None


def get_shadow_evaluator(store: Optional[ShadowStore] = None) -> ShadowEvaluator:
    """Return the singleton ShadowEvaluator."""
    global _evaluator
    if _evaluator is None:
        _evaluator = ShadowEvaluator(store=store)
    return _evaluator
