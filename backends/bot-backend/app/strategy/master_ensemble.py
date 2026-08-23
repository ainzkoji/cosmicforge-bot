"""
MasterEnsembleStrategy v2 — Regime-Gated Weighted Voting
=========================================================

Architecture:
  1. Fetch klines once  → feed RegimeClassifier (sole regime authority)
  2. Activation matrix  → filter strategies per regime
  3. Parallel execution → run only activated strategies
  4. Regime multipliers → adjust effective weights before aggregation
  5. Dynamic threshold  → regime-aware confidence gate via DynamicThresholdCalculator
  6. Return signal      → with full observability meta

Single source of truth:
  - RegimeClassifier.classify_stable() is the ONLY regime authority.
  - DynamicThresholdCalculator.get_threshold_for_regime() is the ONLY threshold authority.
  - All 7 sub-strategies are untouched; activation is external filtering.

Failure policy:
  - Any failure before vote aggregation → HOLD, explicit reason logged.
  - No silent fallback to previous regime.
  - No partial swallowing of sub-strategy errors (they stay as HOLD votes).
"""
from __future__ import annotations

import concurrent.futures
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.hold_breakdown import classify_hold_reason, component_breakdown
from app.strategy.registry import register_strategy
from app.core.strong_trend_guard import evaluate_strong_trend_guard
from shared_lib.persistence.db import DB
from shared_lib.persistence.trade_fills import get_recent_regime_outcomes

# Sub-strategy imports — unchanged
from app.strategy.supertrend import SuperTrendStrategy
from app.strategy.vwap_reversion import VWAPReversionStrategy
from app.strategy.trend_pullback import TrendPullbackStrategy
from app.strategy.squeeze_breakout import SqueezeBreakoutStrategy
from app.strategy.sma_cross import SMACrossStrategy
from app.strategy.donchian_breakout import DonchianBreakoutStrategy
from app.strategy.bollinger_reversion import BollingerReversionStrategy

# Regime authority
from app.strategy.regime import RegimeClassifier, MarketRegime

# Dynamic threshold authority
from app.risk.dynamic_threshold import get_dynamic_threshold_calculator

logger = logging.getLogger(__name__)


# =============================================================================
# ACTIVATION MATRIX — single source of truth for regime→strategy routing
# Key: MarketRegime.value  |  Value: frozenset of active strategy names
# =============================================================================

_ACTIVATION_MATRIX: Dict[str, frozenset] = {
    MarketRegime.STRONG_TREND.value: frozenset({
        "supertrend", "trend_pullback", "donchian_breakout", "sma_cross",
    }),
    MarketRegime.WEAK_TREND.value: frozenset({
        # Stage 2B: narrowed to trend-following only.
        # Reversion strategies (bollinger, vwap) are NOT activated here — they
        # fight against the nascent trend direction and produce counter-trend noise.
        # Requiring consensus among 4 trend strategies provides a meaningful filter.
        "supertrend", "trend_pullback", "donchian_breakout", "sma_cross",
    }),
    MarketRegime.RANGE.value: frozenset({
        "bollinger_reversion", "vwap_reversion", "squeeze_breakout",
    }),
    MarketRegime.HIGH_VOLATILITY.value: frozenset({
        # Stage 2C: reduced to breakout-capable strategies only.
        # Reversion strategies are removed — mean-reversion in high volatility
        # means fading large moves that may continuation, producing large losers.
        # sma_cross and trend_pullback removed — lag too badly in fast moves.
        # Remaining: supertrend (adaptive ATR SL), donchian (breakout-native),
        # squeeze_breakout (volatility-expansion specialist).
        "supertrend", "donchian_breakout", "squeeze_breakout",
    }),
    MarketRegime.LOW_VOLATILITY_CHOP.value: frozenset(),  # No entries — suspend
}

# =============================================================================
# WEIGHT MULTIPLIERS — applied on top of base STRATEGY_WEIGHTS per regime
# Multiplier of 1.0 = no change. Applied only to strategies in the active set.
# =============================================================================

_BASE_WEIGHTS: Dict[str, float] = {
    "supertrend":          1.5,
    "trend_pullback":      1.3,
    "vwap_reversion":      1.2,
    "squeeze_breakout":    1.1,
    "bollinger_reversion": 1.0,
    "donchian_breakout":   1.0,
    "sma_cross":           0.9,
}

# regime.value → {strategy_name → multiplier}
_REGIME_WEIGHT_MULTIPLIERS: Dict[str, Dict[str, float]] = {
    MarketRegime.STRONG_TREND.value: {
        "supertrend":      1.3,
        "trend_pullback":  1.3,
        "donchian_breakout": 1.3,
        "sma_cross":       1.3,
    },
    MarketRegime.WEAK_TREND.value: {},   # no adjustments
    MarketRegime.RANGE.value: {
        "bollinger_reversion": 1.3,
        "vwap_reversion":      1.3,
        "squeeze_breakout":    1.3,
    },
    MarketRegime.HIGH_VOLATILITY.value: {},  # threshold does the gating
    MarketRegime.LOW_VOLATILITY_CHOP.value: {},
}

# =============================================================================
# VOLATILITY SPIKE MULTIPLIERS — per regime
# Reject entry if current_range > average_range * multiplier
# =============================================================================

_REGIME_SPIKE_MULTIPLIERS: Dict[str, float] = {
    MarketRegime.STRONG_TREND.value:       3.5,
    MarketRegime.WEAK_TREND.value:         3.0,
    MarketRegime.RANGE.value:              2.0,  # Strict in range to avoid fakeouts
    MarketRegime.HIGH_VOLATILITY.value:    4.5,  # Permissive of noise but blocks extremes
    MarketRegime.LOW_VOLATILITY_CHOP.value: 2.0,
}


# =============================================================================
# MASTER ENSEMBLE STRATEGY v2
# =============================================================================

@register_strategy(
    name="master_ensemble",
    version="2.0.0",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description=(
        "Regime-gated ensemble combining up to 7 strategies with weighted voting. "
        "RegimeClassifier is the sole regime authority. "
        "DynamicThresholdCalculator is the sole confidence threshold authority."
    ),
    params_schema={
        "type": "object",
        "properties": {
            "min_confidence": {
                "type": "number", "minimum": 0.0, "maximum": 1.0, "default": 0.20,
            },
            "consensus_threshold": {
                "type": "number", "minimum": 0.0, "maximum": 1.0, "default": 0.35,
            },
            "interval": {"type": "string", "default": "15m"},
            "klines_limit": {"type": "integer", "default": 250},
        },
    },
)
class MasterEnsembleStrategy(Strategy):
    """
    Regime-gated MasterEnsemble (v2).

    Decision flow
    -------------
    1. Fetch klines (250 candles) — single fetch, used only for regime computation.
    2. RegimeClassifier.classify_stable() → regime with 2-candle hysteresis.
    3. Activation matrix → frozenset of active strategy names.
    4. Parallel execution of active strategies only.
    5. Aggregate votes with regime+performance weight multipliers.
    6. DynamicThresholdCalculator.get_threshold_for_regime() → confidence gate.
    7. Return SignalResult with full observability meta.
    """

    name = "master_ensemble"
    version = "2.0.0"

    def __init__(
        self,
        client,
        interval: str = "15m",
        min_confidence: float = 0.15,
        consensus_threshold: float = 0.40,
        klines_limit: int = 250,
        htf_bias_enabled: bool = False,
    ) -> None:
        self.client = client
        self.interval = interval
        self.min_confidence = float(min_confidence)
        self.consensus_threshold = float(consensus_threshold)
        self.klines_limit = int(klines_limit)
        self.htf_bias_enabled = bool(htf_bias_enabled)

        # Regime authority — one instance per ensemble, per-symbol hysteresis
        # via classify_stable()'s internal _last_regime dict (one per symbol call)
        # NOTE: A single RegimeClassifier has per-symbol state; we store one
        # classifier per symbol lazily so hysteresis is correctly separated.
        self._regime_classifiers: Dict[str, RegimeClassifier] = {}

        # Dynamic threshold authority — module-level singleton
        self._threshold_calc = get_dynamic_threshold_calculator()

        # Sub-strategies — all 7, always instantiated
        self._strategies: Dict[str, Strategy] = {}
        self._init_strategies()

    def _init_strategies(self) -> None:
        """Initialise all sub-strategies. Failures are logged but non-fatal."""
        configs = [
            ("supertrend",          SuperTrendStrategy),
            ("vwap_reversion",      VWAPReversionStrategy),
            ("trend_pullback",      TrendPullbackStrategy),
            ("squeeze_breakout",    SqueezeBreakoutStrategy),
            ("sma_cross",           SMACrossStrategy),
            ("donchian_breakout",   DonchianBreakoutStrategy),
            ("bollinger_reversion", BollingerReversionStrategy),
        ]
        for strat_name, klass in configs:
            try:
                # F-13: VWAPReversion is designed for 5m candles; passing the ensemble's
                # 15m interval degrades mean-reversion signal quality. Use its default.
                if strat_name == "vwap_reversion":
                    self._strategies[strat_name] = klass(client=self.client)
                else:
                    self._strategies[strat_name] = klass(
                        client=self.client, interval=self.interval
                    )
            except Exception as exc:
                logger.warning(
                    f"[ENSEMBLE] Failed to initialise {strat_name}: {exc}. "
                    "Strategy excluded permanently."
                )

    # -------------------------------------------------------------------------
    # FIX-E: Execution gate helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _parse_blocked_regimes(blocked_str: str) -> Set[str]:
        """Parse comma-separated regime name string into a set of upper-cased names."""
        if not blocked_str:
            return set()
        return {r.strip().upper() for r in blocked_str.split(",") if r.strip()}

    @staticmethod
    def _parse_session_windows(windows_str: str) -> List[Tuple[int, int]]:
        """
        Parse 'HH:MM-HH:MM,...' into list of (start_hour_incl, end_hour_excl) pairs.
        Example: '06:00-19:00' → [(6, 19)].  '08:00-11:00,13:00-16:00' → [(8,11),(13,16)].
        """
        windows: List[Tuple[int, int]] = []
        for segment in windows_str.split(","):
            segment = segment.strip()
            if not segment:
                continue
            try:
                start_s, end_s = segment.split("-")
                start_h = int(start_s.split(":")[0])
                end_h = int(end_s.split(":")[0])
                windows.append((start_h, end_h))
            except (ValueError, IndexError):
                logger.warning(
                    "[ENSEMBLE SESSION] Could not parse window %r — skipping", segment
                )
        return windows

    @staticmethod
    def _check_session_gate(windows: List[Tuple[int, int]]) -> Tuple[bool, int]:
        """
        FIX-E: Check if the current UTC hour falls inside any configured session window.

        Returns (allowed: bool, current_utc_hour: int).
        Window boundary: start is inclusive, end is exclusive.
        Example: (8, 11) allows hours 8, 9, 10 but NOT 11.
        """
        now_utc = datetime.now(timezone.utc)
        hour = now_utc.hour
        for start, end in windows:
            if start <= end:
                if start <= hour < end:
                    return True, hour
            else:
                # Wraps midnight: e.g. (22, 3) allows 22, 23, 0, 1, 2
                if hour >= start or hour < end:
                    return True, hour
        return False, hour

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def get_signal(self, symbol: str, **kwargs) -> SignalResult:
        """
        Regime-gated signal computation.

        Returns HOLD immediately (with explicit reason) if:
        - klines cannot be fetched
        - fewer than 100 candles returned
        - RegimeClassifier raises
        - activation list is empty (LOW_VOL_CHOP)
        - no valid votes collected
        """

        # ------------------------------------------------------------------
        # Step 1 — Fetch klines (single fetch for regime computation)
        # ------------------------------------------------------------------
        try:
            klines = self.client.klines(
                symbol=symbol, interval=self.interval, limit=self.klines_limit
            )
        except Exception as exc:
            logger.warning(f"[REGIME] {symbol}: klines fetch failed: {exc}")
            return self._hold(symbol, "regime_klines_error", meta=self._null_meta(error=str(exc)))

        if not klines or len(klines) < 100:
            logger.warning(
                f"[REGIME] {symbol}: insufficient candles "
                f"({len(klines) if klines else 0} < 100)"
            )
            return self._hold(symbol, "regime_insufficient_data", meta=self._null_meta())

        highs  = [float(k[2]) for k in klines]
        lows   = [float(k[3]) for k in klines]
        closes = [float(k[4]) for k in klines]

        # ------------------------------------------------------------------
        # Step 2 — Classify regime (sole authority, 2-candle hysteresis)
        # ------------------------------------------------------------------
        try:
            classifier = self._get_classifier(symbol)
            regime_result = classifier.classify_stable(highs, lows, closes)
            regime = regime_result.regime
        except Exception as exc:
            logger.warning(f"[REGIME] {symbol}: classify_stable failed: {exc}")
            # Do NOT fall back to previous regime — return HOLD explicitly
            return self._hold(
                symbol, "regime_classify_error", meta=self._null_meta(error=str(exc))
            )

        logger.info(
            f"[REGIME] {symbol}: {regime.value} "
            f"conf={regime_result.regime_confidence:.2f} "
            f"ADX={regime_result.adx:.1f} "
            f"ATR%={regime_result.atr_percent:.2f}% "
            f"slope={regime_result.ma_slope:.3f}"
        )

        # ── FIX-E: Load execution-gate config once per call ───────────────────
        try:
            from app.core.config import settings as _settings
            _threshold_floor = float(_settings.ENSEMBLE_MIN_THRESHOLD_FLOOR)
            _strong_trend_guard = evaluate_strong_trend_guard(
                _settings,
                execution_mode=kwargs.get("execution_mode"),
            )
            _blocked_regimes = set(_strong_trend_guard.effective_blocked_regimes)
            _session_filter_enabled = bool(_settings.ENSEMBLE_SESSION_FILTER_ENABLED)
            _session_windows = self._parse_session_windows(_settings.ENSEMBLE_SESSION_WINDOWS_UTC)
            if _strong_trend_guard.forced_blocked:
                logger.error(
                    "[STRONG_TREND GUARD] Unsafe unblock rejected; effective block restored: %s",
                    _strong_trend_guard.reason,
                )
        except Exception:
            # Fail safe for STRONG_TREND if runtime safety config is unavailable.
            _threshold_floor = 0.0
            _blocked_regimes = {"STRONG_TREND"}
            _strong_trend_guard = None
            _session_filter_enabled = False
            _session_windows = []

        # ── Pre-compute threshold and base indicator snapshot ──────────────────
        # Done here so ALL return paths (including CHOP) emit a complete meta dict.
        _min_gate = kwargs.get("min_confidence_gate")
        _strat_adjustments = kwargs.get("strategy_weight_adjustments", {})
        if _min_gate is not None:
            _threshold_val = float(_min_gate)
            _threshold_type = "adaptive_engine"
        else:
            _thr = self._threshold_calc.get_threshold(symbol)
            _threshold_val = _thr.threshold
            _threshold_type = _thr.bound_label

        # FIX-E: apply threshold floor — dynamic threshold can never go below it
        _raw_dynamic_threshold = _threshold_val
        _threshold_val = max(_threshold_val, _threshold_floor)

        # All regime-level indicator fields + FIX-E observability fields.
        _imeta: dict = {
            "regime":                      regime.value,
            "htf_opposed":                 False,
            "regime_confidence":           round(regime_result.regime_confidence, 3),
            "adx":                         round(regime_result.adx, 1),
            "atr_pct":                     round(regime_result.atr_percent, 3),
            "ma_slope":                    round(regime_result.ma_slope, 4),
            "compression_ratio":           round(regime_result.compression_ratio, 4),
            "breakout_pressure":           round(regime_result.breakout_pressure, 4),
            "threshold":                   round(_threshold_val, 4),
            "threshold_type":              _threshold_type,
            "perf_multipliers":            _strat_adjustments,
            # FIX-E observability
            "ensemble_threshold_floor":    round(_threshold_floor, 4),
            "ensemble_threshold_raw":      round(_raw_dynamic_threshold, 4),
            "ensemble_threshold_used":     round(_threshold_val, 4),
            "regime_gate_blocked_regimes": sorted(_blocked_regimes),
            "strong_trend_allowed_only_in_paper": True,
            "strong_trend_guard_result": (
                _strong_trend_guard.reason if _strong_trend_guard else "fail_safe_blocked"
            ),
            "regime_gate_result":          "pending",   # updated below
            "session_gate_result":         "pending",   # updated below
            "execution_block_reason":      None,        # set if blocked
        }

        # ------------------------------------------------------------------
        # Step 3 — Activation matrix
        # ------------------------------------------------------------------
        active_names: frozenset = _ACTIVATION_MATRIX.get(regime.value, frozenset())
        # Restrict to strategies that actually initialised
        available_active = frozenset(active_names) & frozenset(self._strategies.keys())

        deactivated = sorted(
            frozenset(self._strategies.keys()) - available_active
        )

        logger.info(
            f"[REGIME-GATE] {symbol}: {regime.value} → "
            f"active={sorted(available_active)} "
            f"deactivated={deactivated}"
        )

        if not available_active:
            # LOW_VOL_CHOP or all strategies failed to init
            reason = (
                "regime_low_vol_chop_suspended"
                if regime == MarketRegime.LOW_VOLATILITY_CHOP
                else "regime_no_active_strategies"
            )
            _imeta["regime_gate_result"] = "no_active_strategies"
            _imeta["session_gate_result"] = "skipped_no_active_strategies"
            return self._hold(symbol, reason, meta={
                **_imeta,
                "active_strategies": [],
                "deactivated":       deactivated,
                "buy_score":         0.0,
                "sell_score":        0.0,
                "votes":             [],
                "strategies_used":   0,
                "errors":            [],
            })

        # ------------------------------------------------------------------
        # Step 3.5 — FIX-E: Regime Execution Gate
        # ------------------------------------------------------------------
        _regime_name_upper = regime.value.upper()
        if _blocked_regimes and _regime_name_upper in _blocked_regimes:
            block_reason = f"REGIME_BLOCKED_{_regime_name_upper}"
            logger.info(
                "[ENSEMBLE REGIME GATE] %s: regime=%s is in blocked list %s — returning HOLD",
                symbol, regime.value, sorted(_blocked_regimes),
            )
            _imeta["regime_gate_result"] = "blocked"
            _imeta["session_gate_result"] = "skipped_regime_blocked"
            _imeta["execution_block_reason"] = block_reason
            return self._hold(symbol, block_reason, meta={
                **_imeta,
                "active_strategies": sorted(available_active),
                "deactivated":       deactivated,
                "buy_score":         0.0,
                "sell_score":        0.0,
                "votes":             [],
                "strategies_used":   0,
                "errors":            [],
            })
        _imeta["regime_gate_result"] = "allowed"

        # ------------------------------------------------------------------
        # Step 3.6 — FIX-E: Session Filter Gate
        # ------------------------------------------------------------------
        if _session_filter_enabled and _session_windows:
            _session_allowed, _utc_hour = self._check_session_gate(_session_windows)
            _imeta["session_gate_result"] = "allowed" if _session_allowed else "blocked"
            if not _session_allowed:
                block_reason = "SESSION_BLOCKED"
                logger.info(
                    "[ENSEMBLE SESSION GATE] %s: UTC hour %d outside windows %s — HOLD",
                    symbol, _utc_hour, _session_windows,
                )
                _imeta["execution_block_reason"] = block_reason
                return self._hold(symbol, block_reason, meta={
                    **_imeta,
                    "active_strategies": sorted(available_active),
                    "deactivated":       deactivated,
                    "buy_score":         0.0,
                    "sell_score":        0.0,
                    "votes":             [],
                    "strategies_used":   0,
                    "errors":            [],
                })
        else:
            _imeta["session_gate_result"] = "disabled"

        # ------------------------------------------------------------------
        # Step 3.7 — Volatility Spike Guard (Regime-Aware)
        # ------------------------------------------------------------------
        is_spike, mult_used = self._check_volatility_spike(symbol, klines, regime)
        if is_spike:
            logger.warning(
                f"[ENSEMBLE SPIKE] {symbol} blocked: "
                f"Volatility spike detected (multiplier used: {mult_used:.1f}x)"
            )
            return self._hold(symbol, "volatility_spike_detected", meta={
                **_imeta,
                "active_strategies": sorted(available_active),
                "deactivated":       deactivated,
                "buy_score":         0.0,
                "sell_score":        0.0,
                "votes":             [],
                "strategies_used":   0,
                "errors":            [],
                "spike_multiplier":  mult_used,
            })

        # ------------------------------------------------------------------
        # Step 4 — Parallel execution (active strategies only)
        # ------------------------------------------------------------------
        active_strategies = {
            n: s for n, s in self._strategies.items() if n in available_active
        }

        votes: List[Tuple[str, Signal, float]] = []
        components: List[dict] = []
        errors: List[str] = []

        def _run(
            name: str,
            strat: Strategy,
        ) -> Tuple[str, Signal, float, str, dict, Optional[str]]:
            try:
                result = strat.get_signal(symbol)
                sig = result.signal if hasattr(result, "signal") else Signal.HOLD
                if isinstance(sig, str):
                    sig = Signal[sig.upper()] if sig.upper() in Signal.__members__ else Signal.HOLD
                conf = float(result.confidence) if hasattr(result, "confidence") else 0.0
                reason = str(getattr(result, "reason", "") or "")
                meta = getattr(result, "meta", None) or {}
                return name, sig, conf, reason, meta, None
            except Exception as exc:
                return name, Signal.HOLD, 0.0, "strategy_error", {}, str(exc)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(active_strategies)
        ) as pool:
            future_map = {
                pool.submit(_run, n, s): n
                for n, s in active_strategies.items()
            }
            for future in concurrent.futures.as_completed(future_map):
                try:
                    name, sig, conf, reason, component_meta, err = future.result()
                    components.append(
                        component_breakdown(
                            strategy=name,
                            signal=sig.value,
                            confidence=conf,
                            reason=reason,
                            meta=component_meta,
                            threshold_floor=_threshold_val,
                            symbol=symbol,
                            timeframe=self.interval,
                            market_regime=regime.value,
                            session_allowed=True,
                        )
                    )
                    if err:
                        errors.append(f"{name}:{err}")
                    else:
                        votes.append((name, sig, conf))
                except Exception as exc:
                    errors.append(f"{future_map[future]}:{type(exc).__name__}")

        for name in deactivated:
            components.append(
                component_breakdown(
                    strategy=name,
                    signal="DISABLED",
                    confidence=0.0,
                    reason=f"disabled_for_regime:{regime.value}",
                    meta={},
                    threshold_floor=_threshold_val,
                    symbol=symbol,
                    timeframe=getattr(self._strategies.get(name), "interval", self.interval),
                    market_regime=regime.value,
                    session_allowed=True,
                    enabled=False,
                )
            )

        if not votes:
            logger.warning(f"[ENSEMBLE] {symbol}: no valid votes (all strategies errored)")
            return self._hold(symbol, "no_valid_votes", meta={
                **_imeta,
                "active_strategies": sorted(available_active),
                "deactivated":       deactivated,
                "buy_score":         0.0,
                "sell_score":        0.0,
                "votes":             [],
                "strategies_used":   0,
                "errors":            errors,
            })

        # ------------------------------------------------------------------
        # Step 5 — Additive vote aggregation with regime + perf multipliers
        # ------------------------------------------------------------------
        # Use pre-computed values from the indicator snapshot block above.
        min_confidence_gate = _min_gate
        strategy_weight_adjustments = _strat_adjustments
        regime_mults = _REGIME_WEIGHT_MULTIPLIERS.get(regime.value, {})

        buy_score  = 0.0
        sell_score = 0.0
        vote_details: List[str] = []

        for name, sig, conf in votes:
            base_w     = _BASE_WEIGHTS.get(name, 1.0)
            regime_m   = regime_mults.get(name, 1.0)
            perf_m     = strategy_weight_adjustments.get(name, 1.0)
            eff_weight = base_w * regime_m * perf_m
            weighted   = eff_weight * conf

            if sig == Signal.BUY and conf > 0:
                buy_score  += weighted
                vote_details.append(f"{name}:BUY({conf:.2f})×{eff_weight:.2f}")
            elif sig == Signal.SELL and conf > 0:
                sell_score += weighted
                vote_details.append(f"{name}:SELL({conf:.2f})×{eff_weight:.2f}")
            else:
                vote_details.append(f"{name}:HOLD({conf:.2f})")

        # By dividing by a nominal weight instead of the sum of ALL active strategies (which can be 8.0+),
        # we prevent orthogonal strategies (which correctly output 0.0) from diluting the confidence.
        # Stage 2B: raised from 2.0 → 3.0. At 2.0 a single supertrend vote at 0.9 confidence
        # produced buy_pct=0.675 (1.5×0.9/2.0), enough to pass threshold alone.
        # At 3.0 that same single vote produces 0.45 — below any reasonable threshold —
        # requiring at least 2 strategies to agree before a signal clears the bar.
        NOMINAL_CONSENSUS_WEIGHT = 3.0
        
        buy_pct  = min(1.0, buy_score / NOMINAL_CONSENSUS_WEIGHT)
        sell_pct = min(1.0, sell_score / NOMINAL_CONSENSUS_WEIGHT)

        # ------------------------------------------------------------------
        # Step 6 — Regime-aware dynamic threshold (sole threshold authority)
        # ------------------------------------------------------------------
        raw_conf = max(buy_pct, sell_pct)
        self._threshold_calc.record(symbol, raw_conf)

        # Threshold already resolved in _threshold_val / _threshold_type above.
        effective_threshold = _threshold_val

        # ------------------------------------------------------------------
        # Step 7 — Determine final signal
        # ------------------------------------------------------------------
        final_signal = Signal.HOLD
        final_confidence = 0.0

        if buy_pct > sell_pct and buy_pct >= effective_threshold:
            final_signal = Signal.BUY
            final_confidence = buy_pct
        elif sell_pct > buy_pct and sell_pct >= effective_threshold:
            final_signal = Signal.SELL
            final_confidence = sell_pct
        else:
            final_signal = Signal.HOLD
            final_confidence = float(max(buy_pct, sell_pct))
            if final_confidence > 0:
                logger.debug(
                    f"[ENSEMBLE] {symbol}: BLOCKED by threshold "
                    f"({final_confidence:.3f} < {effective_threshold:.3f})"
                )

        if final_signal != Signal.HOLD:
            logger.info(
                f"[ENSEMBLE] {symbol}: "
                f"buy={buy_pct:.3f} sell={sell_pct:.3f} "
                f"thr={effective_threshold:.3f} "
                f"(type={_threshold_type}) "
                f"→ {final_signal.value} ({final_confidence:.3f})"
            )

        # Hard min_confidence gate (secondary guard after threshold)
        if final_confidence < self.min_confidence and final_signal != Signal.HOLD:
            logger.debug(
                f"[ENSEMBLE] {symbol}: BLOCKED by hard min_confidence "
                f"({final_confidence:.3f} < {self.min_confidence:.3f})"
            )
            final_signal = Signal.HOLD

        # HTF Bias check (Hard enforcement of 4h EMA200 trend)
        htf_opposed = False
        if self.htf_bias_enabled and final_signal != Signal.HOLD:
            try:
                # Need ~250 candles for stable EMA200
                htf_klines = self.client.klines(symbol=symbol, interval="4h", limit=250)
                if htf_klines and len(htf_klines) >= 200:
                    htf_closes = [float(k[4]) for k in htf_klines]
                    
                    # Calculate simple EMA200
                    period = 200
                    multiplier = 2 / (period + 1)
                    
                    # Ensure we have enough data for the initial SMA seed
                    seed_closes = htf_closes[:period]
                    ema = sum(seed_closes) / period
                    
                    for i in range(period, len(htf_closes)):
                        close = htf_closes[i]
                        ema = (close - ema) * multiplier + ema
                    
                    current_price = htf_closes[-1]
                    # Apply 0.05% buffer to prevent flickering entries
                    buffer = 0.0005 
                    if final_signal == Signal.BUY and current_price < ema * (1 + buffer):
                        htf_opposed = True
                    elif final_signal == Signal.SELL and current_price > ema * (1 - buffer):
                        htf_opposed = True
                    
                    if htf_opposed:
                        logger.info(
                            f"[ENSEMBLE HTF] {symbol}: 4h trend opposes {final_signal.value} signal "
                            f"(Price {current_price:.4f} vs EMA200 {ema:.4f} + buffer). BLOCKING SIGNAL."
                        )
                        final_signal = Signal.HOLD
                        final_confidence = 0.0
            except Exception as e:
                logger.warning(f"[HTF BIAS] Error computing 4h trend for {symbol}: {e}")

        return SignalResult(
            signal=final_signal,
            confidence=float(final_confidence),
            reason="master_ensemble_v2",
            meta={
                # Spread in the base indicator snapshot (regime indicators + threshold).
                # htf_opposed may have been updated above from False → True.
                **_imeta,
                "htf_opposed":       htf_opposed,
                "active_strategies": sorted(available_active),
                "deactivated":       deactivated,
                "buy_score":         round(buy_pct, 4),
                "sell_score":        round(sell_pct, 4),
                "votes":             vote_details,
                "strategies_used":   len(votes),
                "errors":            errors,
                "component_breakdown": components,
                "hold_reason": (
                    classify_hold_reason(
                        "master_ensemble_v2",
                        confidence=float(final_confidence),
                        threshold_floor=float(effective_threshold),
                        meta={
                            "htf_opposed": htf_opposed,
                            "component_breakdown": components,
                        },
                    )
                    if final_signal == Signal.HOLD
                    else None
                ),
            },
        )

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _get_classifier(self, symbol: str) -> RegimeClassifier:
        """Return (or lazily create) a per-symbol RegimeClassifier instance."""
        if symbol not in self._regime_classifiers:
            self._regime_classifiers[symbol] = RegimeClassifier()
        return self._regime_classifiers[symbol]

    def _check_volatility_spike(self, symbol: str, klines: list, regime: MarketRegime) -> Tuple[bool, float]:
        """
        Check if the short-term volatility (3 candles) is an outlier compared to a rolling baseline (20 candles).
        Uses regime-aware multipliers to adapt to market context.
        """
        if not klines or len(klines) < 26:
            return False, 0.0
            
        multiplier = _REGIME_SPIKE_MULTIPLIERS.get(regime.value, 3.0)

        # F-17: Use True Range (incorporates overnight gaps) instead of H-L only.
        # True Range = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
        def _true_range(k_curr, k_prev) -> float:
            h = float(k_curr[2])
            l = float(k_curr[3])
            pc = float(k_prev[4])  # previous close
            return max(h - l, abs(h - pc), abs(l - pc))

        true_ranges = [
            _true_range(klines[i], klines[i - 1])
            for i in range(1, len(klines))
        ]

        if len(true_ranges) < 23:
            return False, multiplier

        # Short-term ATR (3 most recent candles)
        short_term_atr = sum(true_ranges[-3:]) / 3.0

        # Rolling ATR baseline (20 periods, excluding the most recent 3)
        rolling_atr = sum(true_ranges[-23:-3]) / 20.0

        if rolling_atr <= 0:
            return False, multiplier

        return short_term_atr > (multiplier * rolling_atr), multiplier

    @staticmethod
    def _null_meta(error: str = "") -> dict:
        """
        Return a complete meta skeleton with zeroed indicator values.

        Used for pre-regime failure paths (klines unavailable, classify error)
        where regime_result does not exist yet.  Ensures every trace record
        has the same key set regardless of exit path.
        """
        return {
            "regime":            "unknown",
            "htf_opposed":       False,
            "regime_confidence": 0.0,
            "adx":               0.0,
            "atr_pct":           0.0,
            "ma_slope":          0.0,
            "compression_ratio": 0.0,
            "breakout_pressure": 0.0,
            "active_strategies": [],
            "deactivated":       [],
            "buy_score":         0.0,
            "sell_score":        0.0,
            "threshold":         0.0,
            "threshold_type":    "unknown",
            "votes":             [],
            "strategies_used":   0,
            "errors":            [error] if error else [],
            "perf_multipliers":  {},
        }

    @staticmethod
    def _hold(
        symbol: str,
        reason: str,
        meta: Optional[Dict] = None,
    ) -> SignalResult:
        """Convenience constructor for HOLD returns with mandatory reason."""
        details = dict(meta or {})
        details["hold_reason"] = classify_hold_reason(
            reason,
            confidence=0.0,
            threshold_floor=float(
                details.get("ensemble_threshold_floor", details.get("threshold", 0.0)) or 0.0
            ),
            meta=details,
        )
        details["failed_conditions"] = [details["hold_reason"]]
        return SignalResult(
            signal=Signal.HOLD,
            confidence=0.0,
            reason=reason,
            meta=details,
        )
