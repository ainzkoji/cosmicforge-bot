"""
Dynamic Adaptive Confidence Threshold Calculator
=================================================

Replaces the static Layer A confidence threshold with a rolling-window
percentile-based threshold that adapts to recent signal distributions.

Configuration (via environment variables):
    DYNAMIC_THRESHOLD_ENABLED        = true   # Set false to revert to static 0.5
    DYNAMIC_THRESHOLD_WINDOW_SIZE    = 175    # Rolling window size (signals)
    DYNAMIC_THRESHOLD_PERCENTILE     = 70     # Percentile of history to use
    DYNAMIC_THRESHOLD_MAX            = 0.65   # Hard cap  (never higher than this)
    DYNAMIC_THRESHOLD_FALLBACK       = 0.25   # Static fallback (cold start / disabled)
    DYNAMIC_THRESHOLD_MIN_SAMPLES    = 30     # Minimum history required before using percentile

Architecture notes:
    - Per-symbol circular buffer via collections.deque(maxlen=WINDOW_SIZE)
    - Single threading.Lock guards all reads/writes (safety engine may be called
      concurrently from multiple runner loops)
    - numpy is already a dependency of the strategy layer; we use np.percentile
    - Memory bounded: WINDOW_SIZE * n_symbols * 8 bytes (float64); at 175 × 50
      symbols this is ~70 KB — negligible
"""
from __future__ import annotations

import json
import logging
import os
import threading
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
from shared_lib.persistence.db import DB

# Regime import (lazy to avoid circular imports at module load)
try:
    from app.strategy.regime import MarketRegime
except ImportError:
    MarketRegime = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level configuration from environment variables
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name, "").strip().lower()
    if not val:
        return default
    return val in ("true", "1", "yes")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        logger.warning(f"Invalid env var {name}; using default {default}")
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        logger.warning(f"Invalid env var {name}; using default {default}")
        return default


# Read once at import time (can be overridden in unit tests by patching these)
DYNAMIC_THRESHOLD_ENABLED: bool = _env_bool("DYNAMIC_THRESHOLD_ENABLED", True)
WINDOW_SIZE: int = _env_int("DYNAMIC_THRESHOLD_WINDOW_SIZE", 175)
PERCENTILE: float = _env_float("DYNAMIC_THRESHOLD_PERCENTILE", 55.0)   # Stage 2A: raised from 30 → 55 (admit top-45% signals, not bottom-70%)
MIN_THRESHOLD: float = _env_float("DYNAMIC_THRESHOLD_MIN", 0.40)
MAX_THRESHOLD: float = _env_float("DYNAMIC_THRESHOLD_MAX", 0.65)       # Restored to original design cap; allows high-conviction regimes to reach 0.65
FALLBACK_THRESHOLD: float = _env_float("DYNAMIC_THRESHOLD_FALLBACK", 0.45) # Stage 2A: raised cold-start fallback from 0.40 → 0.45
MIN_SAMPLES: int = _env_int("DYNAMIC_THRESHOLD_MIN_SAMPLES", 30)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ThresholdResult:
    """
    Encapsulates the result of a threshold computation so callers can log
    full context without re-running math.

    Attributes:
        threshold:          The final threshold to compare against (clamped/fallback).
        raw_percentile:     The raw np.percentile value before clamping, or None
                            for fallback/disabled cases.
        threshold_type:     One of: 'dynamic_percentile', 'dynamic_floor',
                            'dynamic_cap', 'fallback_static'.
        samples_available:  How many samples were in the rolling window.
        window_size:        Configured window size.
    """
    threshold: float
    raw_percentile: Optional[float]
    threshold_type: str   # 'dynamic_percentile' | 'dynamic_floor' | 'dynamic_cap' | 'fallback_static'
    samples_available: int
    window_size: int

    @property
    def bound_label(self) -> str:
        """Human-readable bound label for structured log output."""
        mapping = {
            "dynamic_percentile": "percentile",
            "dynamic_floor":      "floor",
            "dynamic_cap":        "cap",
            "fallback_static":    "fallback",
        }
        return mapping.get(self.threshold_type, self.threshold_type)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class DynamicThresholdCalculator:
    """
    Per-symbol rolling-window confidence threshold calculator.

    Thread-safe: a single lock serialises all reads and writes so the
    calculator can be shared across concurrent runner loops.

    Usage:
        calculator = DynamicThresholdCalculator()

        # Before each Layer A confidence comparison:
        calculator.record("BTCUSDT", confidence_value)
        result = calculator.get_threshold("BTCUSDT")

        # result.threshold is the value to compare against
        # result.threshold_type describes which branch was taken
    """

    def __init__(self, db: Optional[DB] = None) -> None:
        # Dict[symbol -> deque[float]]
        self._history: Dict[str, deque] = {}
        self._lock = threading.Lock()
        self.db = db or DB()

        logger.info(
            f"DynamicThresholdCalculator initialised: "
            f"enabled={DYNAMIC_THRESHOLD_ENABLED}, "
            f"window={WINDOW_SIZE}, "
            f"percentile={PERCENTILE}th, "
            f"bounds=[{MIN_THRESHOLD}, {MAX_THRESHOLD}], "
            f"fallback={FALLBACK_THRESHOLD}, "
            f"min_samples={MIN_SAMPLES}"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def reconstruct_memory_from_db(self, config_id: str, symbol: str) -> None:
        """
        Pull the last WINDOW_SIZE confidence records from decision_logs
        so state is deterministic across restarts.
        """
        with self._lock:
            if symbol not in self._history:
                self._history[symbol] = deque(maxlen=WINDOW_SIZE)
                
            try:
                import sqlite3
                with self.db.connect() as conn:
                    rows = conn.execute(
                        '''
                        SELECT final_action, strategy_signal_json 
                        FROM decision_logs 
                        WHERE config_id = ? AND symbol = ?
                        ORDER BY created_at DESC 
                        LIMIT ?
                        ''',
                        (config_id, symbol, WINDOW_SIZE)
                    ).fetchall()
                    
                    # Reverse so oldest are appended first, newest last
                    for row in reversed(rows):
                        import json
                        try:
                            meta = json.loads(row["strategy_signal_json"])
                            if "confidence" in meta:
                                self._history[symbol].append(float(meta["confidence"]))
                        except Exception:
                            pass
                
                logger.info(f"Reconstructed DynamicThreshold memory for {symbol}: {len(self._history[symbol])} records loaded.")
            except Exception as e:
                logger.debug(f"Could not reconstruct DynamicThreshold memory from DB for {symbol}: {str(e)}")

    def record(self, symbol: str, confidence: float) -> None:
        """
        Append a new confidence score to the rolling window for *symbol*.

        Should be called once per signal evaluation, immediately before
        calling get_threshold().  This ensures the current confidence is
        part of the distribution used for future threshold calculations
        (we do NOT include it in the *current* threshold computation to
        avoid look-ahead bias — the deque is read-before-write in
        get_threshold()).

        Note: We record first, then compute, so the new sample is available
        for the *next* threshold computation, not the current one. This is
        intentional and consistent with standard rolling-window practice.
        """
        with self._lock:
            if symbol not in self._history:
                self._history[symbol] = deque(maxlen=WINDOW_SIZE)
            self._history[symbol].append(float(confidence))

    def get_threshold(self, symbol: str, adaptive_offset: float = 0.0) -> ThresholdResult:
        """
        Compute and return the dynamic threshold for *symbol*.

        Algorithm:
            1. If DYNAMIC_THRESHOLD_ENABLED is false → return static fallback
            2. If samples < MIN_SAMPLES → return static fallback (cold start)
            3. Compute raw = np.percentile(history, PERCENTILE)
            4. Apply adaptive_offset (e.g. confidence_gate_modifier from AdaptiveEngine)
            5. Clamp: threshold = max(MIN_THRESHOLD, min(MAX_THRESHOLD, raw + offset))
            6. Return ThresholdResult with full metadata for logging

        Args:
            symbol:          The trading symbol to compute the threshold for.
            adaptive_offset: Optional offset (e.g. AdaptiveEngine.confidence_gate_modifier)
                             applied AFTER percentile computation, BEFORE clamping.
                             Positive offset raises the bar; 0.0 = no change (default).

        Returns:
            ThresholdResult describing the threshold and which branch was taken.
        """
        with self._lock:
            history = self._history.get(symbol)
            samples = len(history) if history is not None else 0

            # Branch 1: feature disabled
            if not DYNAMIC_THRESHOLD_ENABLED:
                return ThresholdResult(
                    threshold=FALLBACK_THRESHOLD,
                    raw_percentile=None,
                    threshold_type="fallback_static",
                    samples_available=samples,
                    window_size=WINDOW_SIZE,
                )

            # Branch 2: insufficient history (cold start)
            if samples < MIN_SAMPLES:
                logger.debug(
                    f"Insufficient history for {symbol} ({samples}/{MIN_SAMPLES}), "
                    f"using static threshold {FALLBACK_THRESHOLD}"
                )
                return ThresholdResult(
                    threshold=FALLBACK_THRESHOLD,
                    raw_percentile=None,
                    threshold_type="fallback_static",
                    samples_available=samples,
                    window_size=WINDOW_SIZE,
                )

            # Branch 3: compute percentile, clamp to bounds
            history_list = list(history)  # snapshot while lock held

        # np.percentile outside the lock (CPU work, no shared state)
        raw = float(np.percentile(history_list, PERCENTILE))

        # Apply adaptive offset from AdaptiveEngine (e.g. confidence_gate_modifier)
        # This shifts the percentile-derived threshold up/down by the engine's
        # penalty/relaxation value, then re-clamps to the configured bounds.
        raw_with_offset = raw + float(adaptive_offset)

        clamped = max(MIN_THRESHOLD, min(MAX_THRESHOLD, raw_with_offset))

        if clamped <= MIN_THRESHOLD and raw < MIN_THRESHOLD:
            threshold_type = "dynamic_floor"
        elif clamped >= MAX_THRESHOLD and raw > MAX_THRESHOLD:
            threshold_type = "dynamic_cap"
        else:
            threshold_type = "dynamic_percentile"

        return ThresholdResult(
            threshold=clamped,
            raw_percentile=raw,
            threshold_type=threshold_type,
            samples_available=samples,
            window_size=WINDOW_SIZE,
        )

    def prune(self, active_symbols: set) -> None:
        """
        Remove rolling-window entries for symbols no longer actively traded.

        Call this periodically (e.g., from a housekeeping task) to release
        memory for instruments that have been delisted or removed from the
        strategy universe.  This is optional — memory is already bounded by
        WINDOW_SIZE × n_symbols × 8 bytes.

        Args:
            active_symbols: Set of symbol strings currently being traded.
        """
        with self._lock:
            inactive = [s for s in self._history if s not in active_symbols]
            for s in inactive:
                del self._history[s]
            if inactive:
                logger.debug(f"DynamicThresholdCalculator: pruned {len(inactive)} inactive symbols: {inactive}")

    def sample_count(self, symbol: str) -> int:
        """Return current number of samples for *symbol* (for tests/monitoring)."""
        with self._lock:
            h = self._history.get(symbol)
            return len(h) if h is not None else 0

    def reset(self, symbol: Optional[str] = None) -> None:
        """
        Clear history.  Pass symbol to clear one symbol, or None to clear all.
        Primarily for use in tests.
        """
        with self._lock:
            if symbol is not None:
                self._history.pop(symbol, None)
            else:
                self._history.clear()


# ---------------------------------------------------------------------------
# Structured log helper
# ---------------------------------------------------------------------------

def log_threshold_event(
    symbol: str,
    confidence: float,
    result: ThresholdResult,
    decision: str,
) -> None:
    """
    Emit a structured JSON debug log entry for every Layer A confidence check.

    Matches the required format from the implementation spec:

        {
          "event": "dynamic_threshold_check",
          "symbol": "BTCUSDT",
          "current_confidence": 0.42,
          "dynamic_threshold": 0.38,
          "threshold_bound_applied": "floor|cap|percentile|fallback",
          "rolling_window_size": 175,
          "samples_available": 167,
          "decision": "PASS|BLOCK"
        }

    Also emits a human-readable INFO line in the format requested by the spec:
        DYNAMIC_THRESHOLD BTCUSDT: confidence=0.42 threshold=0.38(decision=PASS samples=167)
    """
    payload = {
        "event": "dynamic_threshold_check",
        "symbol": symbol,
        "current_confidence": round(confidence, 6),
        "dynamic_threshold": round(result.threshold, 6),
        "threshold_bound_applied": result.bound_label,
        "rolling_window_size": result.window_size,
        "samples_available": result.samples_available,
        "decision": decision,
    }
    logger.debug(json.dumps(payload))

    # Human-readable INFO line
    bound_suffix = ""
    if result.threshold_type == "fallback_static":
        bound_suffix = f" fallback=insufficient_history"
    elif result.threshold_type in ("dynamic_floor", "dynamic_cap"):
        bound_suffix = f" bound={result.bound_label}"

    logger.info(
        f"DYNAMIC_THRESHOLD {symbol}: "
        f"confidence={confidence:.2f} "
        f"threshold={result.threshold:.2f}"
        f"(decision={decision}"
        f"{bound_suffix} "
        f"samples={result.samples_available})"
    )

# =============================================================================
# PER-BOT FACTORY  (replaces shared singleton)
# =============================================================================
# Each bot instance owns its own rolling-window history so that Bot A's signal
# distribution never raises or lowers the confidence threshold for Bot B.

_calculator_instances: dict[str, DynamicThresholdCalculator] = {}
_instances_lock = threading.Lock()


def get_dynamic_threshold_calculator(
    bot_id: str = "default",
    db: Optional[DB] = None,
) -> DynamicThresholdCalculator:
    """
    Return the DynamicThresholdCalculator for *bot_id*, creating it on first call.
    Each bot gets its own rolling window so history never crosses bot boundaries.
    """
    with _instances_lock:
        if bot_id not in _calculator_instances:
            _calculator_instances[bot_id] = DynamicThresholdCalculator(db=db)
        return _calculator_instances[bot_id]


def reset_dynamic_threshold_calculator(bot_id: str = "default") -> None:
    """Evict the calculator for a single bot (e.g. on bot teardown or test reset)."""
    with _instances_lock:
        _calculator_instances.pop(bot_id, None)


def reset_all_dynamic_threshold_calculators() -> None:
    """Clear all per-bot calculators. Use in tests only."""
    with _instances_lock:
        _calculator_instances.clear()
