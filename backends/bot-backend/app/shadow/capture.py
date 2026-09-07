"""
Shadow Trading System — Candidate Capture Layer

ShadowCapture is the passive observer that sits in the finally block of
_step_symbol_orchestrated() and emits shadow trade records for rejected
or non-executed opportunities.

SAFETY CONTRACT:
  - Never calls execute_signal() or place_order()
  - Never writes fake fills to trade_fills
  - Never modifies existing decision_traces rows
  - Never raises exceptions that could affect the live execution path
  - All work is wrapped in try/except — failure is logged and swallowed
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from app.shadow.config import get_shadow_config
from app.shadow.store import (
    ShadowStore, get_shadow_store,
    STAGE_ML_BLOCKED, STAGE_THRESHOLD_BLOCKED, STAGE_CORRELATION_BLOCKED,
    STAGE_ALREADY_OPEN, STAGE_EXECUTOR_REJECTED, STAGE_SAFETY_BLOCKED,
    STAGE_NEAR_MISS,
)

logger = logging.getLogger(__name__)

# ── Interval to minutes mapping ───────────────────────────────────────────────
_INTERVAL_MINUTES: Dict[str, int] = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360,
    "8h": 480, "12h": 720, "1d": 1440,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _expiry_time(interval: str, bars: int) -> str:
    minutes = _INTERVAL_MINUTES.get(interval, 15)
    expiry = datetime.now(timezone.utc) + timedelta(minutes=minutes * bars)
    return expiry.isoformat()


def _extract_sl_tp_from_orch(
    orch_res: Optional[Dict[str, Any]],
    strat_out: Optional[Dict[str, Any]],
    side: Optional[str],
    entry_price: Optional[float],
    klines: Optional[Any],
) -> tuple[Optional[float], Optional[float]]:
    """
    Extract hypothetical SL and TP from orchestrator result or compute from klines.

    Priority:
    1. trade_params from orchestrator (already computed by live system)
    2. Compute from klines using StopLossCalculator (if klines available)
    3. Return None, None (outcome evaluator marks DATA_MISSING for SL/TP)
    """
    # Priority 1: orchestrator already computed SL/TP
    if orch_res and isinstance(orch_res, dict):
        tp = orch_res.get("trade_params", {}) or {}
        sl_price = tp.get("stop_loss")
        tp_price = tp.get("take_profit")
        if sl_price and tp_price:
            # The live system's TP1 is returned; compute TP2 (2.2R) as the runner does
            if entry_price and sl_price and entry_price > 0:
                sl_dist = abs(entry_price - float(sl_price))
                if side == "BUY" or side == "LONG":
                    tp_price = entry_price + 2.2 * sl_dist
                else:
                    tp_price = entry_price - 2.2 * sl_dist
            return float(sl_price), float(tp_price)

    # Priority 2: compute from klines
    if klines and entry_price and side and len(klines) >= 15:
        try:
            from app.execution.tp_sl import StopLossCalculator

            calc = StopLossCalculator()
            closes = [float(k[4]) for k in klines]
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            norm_side = "LONG" if side in ("BUY", "LONG") else "SHORT"
            result = calc.calculate(
                side=norm_side,
                entry_price=entry_price,
                highs=highs,
                lows=lows,
                closes=closes,
            )
            if result.valid:
                return result.stop_price, result.tp2_price
            else:
                logger.debug(
                    "[ShadowCapture] StopLossCalculator rejected: reason=%s detail=%s",
                    result.reject_reason, result.reject_detail
                )
        except Exception as exc:
            logger.debug("[ShadowCapture] StopLossCalculator exception: %s", exc)

    return None, None


class ShadowCapture:
    """
    Passive observer that emits shadow trade candidates from the live
    decision pipeline's finally block.

    One instance is shared per PaperRunner.
    """

    def __init__(
        self,
        store: Optional[ShadowStore] = None,
    ):
        self._store = store or get_shadow_store()
        self._cfg = get_shadow_config()

    def maybe_capture(
        self,
        *,
        # Identification
        bot_instance_id: str,
        trace_id: str,
        symbol: str,
        interval: str = "15m",
        # Decision outcome from _step_symbol_orchestrated finally block
        eval_decision: str,              # "HOLD", "EXECUTED", "EXEC_FAIL", "ALREADY_OPEN", "ERROR", ...
        orchestrator_decision: str,      # "execute", "hold", "skip"
        # Strategy output
        strat_out: Optional[Dict[str, Any]] = None,
        orch_res: Optional[Dict[str, Any]] = None,
        # ML gating
        ml_score: Optional[float] = None,
        ml_action: Optional[str] = None,
        # Dynamic threshold
        dyn_threshold: Optional[float] = None,
        # Market
        entry_price: Optional[float] = None,
        klines: Optional[Any] = None,
        client: Optional[Any] = None,
    ) -> Optional[str]:
        """
        Classify this decision and emit a shadow trade record if applicable.

        Returns the shadow_trade_id if a record was created, else None.

        This method MUST NEVER raise. All exceptions are caught and logged.
        """
        if not self._cfg.enabled:
            logger.debug("[ShadowCapture] System disabled by configuration. Skipping %s.", symbol)
            return None

        try:
            return self._capture_inner(
                bot_instance_id=bot_instance_id,
                trace_id=trace_id,
                symbol=symbol,
                interval=interval,
                eval_decision=eval_decision,
                orchestrator_decision=orchestrator_decision,
                strat_out=strat_out,
                orch_res=orch_res,
                ml_score=ml_score,
                ml_action=ml_action,
                dyn_threshold=dyn_threshold,
                entry_price=entry_price,
                klines=klines,
                client=client,
            )
        except Exception as exc:
            logger.error(
                "[ShadowCapture] ❌ ERROR trace_id=%s symbol=%s reason=%s",
                trace_id, symbol, str(exc),
            )
            return None

    def _capture_inner(
        self,
        *,
        bot_instance_id: str,
        trace_id: str,
        symbol: str,
        interval: str,
        eval_decision: str,
        orchestrator_decision: str,
        strat_out: Optional[Dict[str, Any]],
        orch_res: Optional[Dict[str, Any]],
        ml_score: Optional[float],
        ml_action: Optional[str],
        dyn_threshold: Optional[float],
        entry_price: Optional[float],
        klines: Optional[Any],
        client: Optional[Any] = None,
    ) -> Optional[str]:
        """Inner implementation — may raise (caller catches all)."""

        # ── Step 1: Entry price resolution ─────────────────────────────────────
        if entry_price is None or entry_price <= 0:
            if klines and len(klines) > 0:
                entry_price = float(klines[-1][4]) # Last close
            elif client:
                try:
                    entry_price = float(client.last_price(symbol))
                except Exception as exc:
                    logger.debug("[ShadowCapture] Entry price recovery failed for %s: %s", symbol, exc)
        
        if entry_price is None or entry_price <= 0:
            logger.info("[ShadowCapture] ⛔ SKIPPED symbol=%s reason=NO_ENTRY_PRICE", symbol)
            return None

        # ── Step 2: Kline context guarantee ──────────────────────────────────────
        if klines is None or len(klines) < 20:
            if client:
                try:
                    # Fetch minimal window for SL/TP calculation
                    klines = client.klines(symbol=symbol, interval=interval, limit=100)
                except Exception as exc:
                    logger.info("[ShadowCapture] ⛔ SKIPPED symbol=%s reason=NO_KLINES err=%s", symbol, exc)
                    return None
            else:
                logger.info("[ShadowCapture] ⛔ SKIPPED symbol=%s reason=NO_KLINES", symbol)
                return None

        # ── Extract signal metadata ────────────────────────────────────────────
        confidence: Optional[float] = None
        side: Optional[str] = None
        regime: Optional[str] = None
        strategy: Optional[str] = None
        gate_reason: Optional[str] = None

        if strat_out and isinstance(strat_out, dict):
            confidence = float(strat_out.get("confidence", 0.0) or 0.0)
            _meta = strat_out.get("meta") or {}
            regime = _meta.get("regime")
            strategy = strat_out.get("strategy_name")
            side = strat_out.get("signal") # Secondary priority

        if orch_res and isinstance(orch_res, dict):
            gate_reason = str(orch_res.get("reason", "") or "")
            tp = orch_res.get("trade_params") or {}
            if tp.get("side"):
                side = tp.get("side") # Primary priority

        # Normalize side to uppercase canonical form.
        # Strategy outputs lowercase ("buy", "sell", "hold") and may include
        # "Signal.BUY" prefixes; direction checks require "BUY"/"SELL"/"LONG"/"SHORT".
        if side and isinstance(side, str):
            side = side.upper().replace("SIGNAL.", "").strip()

        threshold = dyn_threshold
        confidence_gap: Optional[float] = None
        if confidence is not None and threshold is not None:
            confidence_gap = round(confidence - threshold, 6)

        # ── Classify rejection stage ──────────────────────────────────────────
        stage = self._classify_stage(
            eval_decision=eval_decision.upper(),
            orchestrator_decision=orchestrator_decision.lower(),
            ml_action=ml_action,
            gate_reason=gate_reason or "",
            confidence=confidence,
            threshold=threshold,
        )

        if stage is None:
            return None  # Not a capture-worthy event

        # ── Check per-category enable flag ────────────────────────────────────
        if not self._is_category_enabled(stage, confidence, threshold):
            logger.info("[ShadowCapture] ⛔ SKIPPED symbol=%s reason=CATEGORY_DISABLED stage=%s", symbol, stage)
            return None

        # ── Step 3: Mandatory SL/TP generation ───────────────────────────────
        sl_price, tp_price = _extract_sl_tp_from_orch(
            orch_res=orch_res,
            strat_out=strat_out,
            side=side,
            entry_price=entry_price,
            klines=klines,
        )

        if sl_price is None or tp_price is None:
            logger.info("[ShadowCapture] ⛔ SKIPPED symbol=%s reason=SLTP_COMPUTE_FAILED", symbol)
            return None

        # ── Step 4: Direction validation ──────────────────────────────────────
        is_valid = False
        if side in ("BUY", "LONG"):
            if sl_price < entry_price < tp_price:
                is_valid = True
        elif side in ("SELL", "SHORT"):
            if tp_price < entry_price < sl_price:
                is_valid = True
        
        if not is_valid:
            logger.info(
                "[ShadowCapture] ⛔ SKIPPED symbol=%s reason=INVALID_DIRECTION side=%s entry=%.4f sl=%.4f tp=%.4f",
                symbol, side, entry_price, sl_price, tp_price
            )
            return None

        # ── Build rejection reason description ────────────────────────────────
        rejection_reason = self._build_rejection_reason(
            stage=stage,
            eval_decision=eval_decision,
            gate_reason=gate_reason,
            ml_score=ml_score,
            ml_action=ml_action,
            confidence=confidence,
            threshold=threshold,
        )

        # ── Write to DB ───────────────────────────────────────────────────────
        shadow_id = self._store.insert_shadow_trade(
            bot_instance_id=bot_instance_id,
            trace_id=trace_id,
            symbol=symbol,
            side=side,
            regime=regime,
            strategy=strategy,
            confidence=confidence,
            threshold=threshold,
            confidence_gap=confidence_gap,
            ml_score=ml_score,
            ml_action=ml_action,
            gate_reason=gate_reason,
            rejection_stage=stage,
            rejection_reason=rejection_reason,
            entry_time=_utc_now(),
            entry_price=entry_price,
            stop_loss=sl_price,
            take_profit=tp_price,
            expiry_time=_expiry_time(interval, self._cfg.expiry_bars),
        )

        logger.info(
            "[ShadowCapture] ✅ CAPTURED symbol=%s side=%s category=%s entry=%.4f sl=%.4f tp=%.4f id=%s",
            symbol, side or "?", stage, entry_price, sl_price, tp_price, shadow_id,
        )

        return shadow_id

    # ── Classification ────────────────────────────────────────────────────────

    def _classify_stage(
        self,
        eval_decision: str,
        orchestrator_decision: str,
        ml_action: Optional[str],
        gate_reason: str,
        confidence: Optional[float],
        threshold: Optional[float],
    ) -> Optional[str]:
        """
        Map the combination of outputs to a rejection stage.
        Returns None if this decision should NOT produce a shadow record
        (e.g. normal HOLDs with no signal, successful executions, errors).
        """
        gate_lower = gate_reason.lower()

        # 1. ML blocked — was heading to execute but ML said BLOCK
        if ml_action == "BLOCK":
            return STAGE_ML_BLOCKED

        # 2. Already open — signal fired but position blocks re-entry
        if eval_decision in ("ALREADY_OPEN", "ALREADY_OPEN_NOT_TAKEN"):
            return STAGE_ALREADY_OPEN

        # 3. Executor rejected — execution attempted but failed
        if eval_decision == "EXEC_FAIL":
            return STAGE_EXECUTOR_REJECTED

        # 4. Orchestrator blocked this decision (hold/skip) — classify reason
        if orchestrator_decision in ("hold", "skip"):
            # 4a. Correlation filtered
            if "corr" in gate_lower or "correlation" in gate_lower:
                return STAGE_CORRELATION_BLOCKED
            # 4b. Threshold / confidence blocked
            if any(kw in gate_lower for kw in (
                "threshold", "confidence", "dynamic", "floor", "min_conf",
                "score_below", "below_threshold",
            )):
                return STAGE_THRESHOLD_BLOCKED
            # 4c. Safety / risk / account blocks
            if any(kw in gate_lower for kw in (
                "risk", "safety", "kill", "drawdown", "margin", "budget",
                "max_open", "position_limit", "daily_loss", "circuit",
            )):
                return STAGE_SAFETY_BLOCKED
            # 4d. Generic threshold blocked (no keyword match, had real signal)
            if confidence is not None and threshold is not None and confidence > 0:
                return STAGE_THRESHOLD_BLOCKED

        # 5. Near-miss HOLD — strategy signal existed but didn't pass gate
        if eval_decision in ("HOLD", "SKIP") and confidence is not None and threshold is not None:
            if threshold > 0 and confidence >= threshold * self._cfg.near_miss_factor:
                return STAGE_NEAR_MISS

        # No shadow-worthy event (normal HOLD with no signal, errors, etc.)
        return None

    def _is_category_enabled(
        self,
        stage: str,
        confidence: Optional[float],
        threshold: Optional[float],
    ) -> bool:
        """Check per-category enable flag from config."""
        cfg = self._cfg
        if stage == STAGE_ML_BLOCKED:
            return cfg.capture_ml_blocked
        if stage == STAGE_THRESHOLD_BLOCKED:
            return cfg.capture_threshold_blocked
        if stage == STAGE_CORRELATION_BLOCKED:
            return cfg.capture_correlation_blocked
        if stage == STAGE_ALREADY_OPEN:
            return cfg.capture_already_open
        if stage == STAGE_EXECUTOR_REJECTED:
            return cfg.capture_executor_rejected
        if stage == STAGE_SAFETY_BLOCKED:
            return cfg.capture_safety_blocked
        if stage == STAGE_NEAR_MISS:
            return cfg.capture_near_miss
        return False

    def _build_rejection_reason(
        self,
        stage: str,
        eval_decision: str,
        gate_reason: Optional[str],
        ml_score: Optional[float],
        ml_action: Optional[str],
        confidence: Optional[float],
        threshold: Optional[float],
    ) -> str:
        """Build a human-readable rejection reason string."""
        parts = [f"stage={stage}"]
        if gate_reason:
            parts.append(f"gate={gate_reason[:120]}")
        if ml_score is not None:
            parts.append(f"ml_score={ml_score:.4f}")
        if ml_action:
            parts.append(f"ml_action={ml_action}")
        if confidence is not None and threshold is not None:
            parts.append(f"conf={confidence:.4f} thr={threshold:.4f} gap={confidence - threshold:.4f}")
        return " | ".join(parts)


# ── Singleton ─────────────────────────────────────────────────────────────────
_capture: Optional[ShadowCapture] = None


def get_shadow_capture(store: Optional[ShadowStore] = None) -> ShadowCapture:
    """Return the singleton ShadowCapture."""
    global _capture
    if _capture is None:
        _capture = ShadowCapture(store=store)
    return _capture
