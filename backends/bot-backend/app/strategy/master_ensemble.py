"""
MasterEnsembleStrategy v2 — Regime-Gated Weighted Voting
=========================================================

Architecture:
  1. Fetch klines once  → feed RegimeClassifier (sole regime authority)
  2. Activation matrix  → filter strategies per regime
  3. Parallel execution → run only activated strategies
  4. Regime multipliers → adjust effective weights before aggregation
  5. Threshold engine   → AdaptiveEntryThresholdEngine resolves the entry bar
  6. Return signal      → with full observability meta

Single source of truth:
  - RegimeClassifier.classify_stable() is the ONLY regime authority.
  - AdaptiveEntryThresholdEngine is the ONLY entry-threshold authority.
    There is no other threshold calculator anywhere in the system.
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
from app.decision.decision_engine import TradingDecisionEngine
from app.decision.opportunity import NoOpportunity, build_opportunity
from app.decision.reasons import QualityReason
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

from app.threshold.contracts import (
    AdaptiveThresholdInput,
    HTFContext,
    MarketQualityContext,
    RegimeContext,
    VolatilityContext,
    experts_from_votes,
)
from app.threshold.engine import AdaptiveEntryThresholdEngine
from app.threshold.runtime import (
    get_performance_calibrator,
    get_threshold_policy,
    get_threshold_state_store,
)

logger = logging.getLogger(__name__)


def _fmt_threshold(value: Optional[float]) -> str:
    """Format a threshold for logs. ``None`` is printed as such, never as 0.000."""
    return "not-evaluated" if value is None else f"{float(value):.3f}"


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
        "AdaptiveEntryThresholdEngine is the sole entry-threshold authority."
    ),
    params_schema={
        "type": "object",
        "properties": {
            # NOTE: the consensus gate is deleted, parameter and all. It was
            # advertised here as a tunable and was never compared against
            # anything. Expert agreement is one bounded input to the threshold.
            "min_confidence": {
                "type": "number", "minimum": 0.0, "maximum": 1.0, "default": 0.20,
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
    6. AdaptiveEntryThresholdEngine.evaluate() → the one entry threshold.
    7. Return SignalResult with full observability meta.
    """

    name = "master_ensemble"
    version = "2.0.0"

    def __init__(
        self,
        client,
        interval: str = "15m",
        min_confidence: float = 0.15,
        klines_limit: int = 250,
        htf_bias_enabled: bool = False,
    ) -> None:
        self.client = client
        self.interval = interval
        self.min_confidence = float(min_confidence)
        self.klines_limit = int(klines_limit)
        self.htf_bias_enabled = bool(htf_bias_enabled)

        # The single entry-quality authority: it performs the one comparison.
        # It does not resolve a threshold -- that belongs to the threshold
        # engine below, and to nothing else.
        self._decision_engine = TradingDecisionEngine()

        # The single entry-threshold authority.
        self._threshold_engine = AdaptiveEntryThresholdEngine(
            state_store=get_threshold_state_store(),
            performance_calibrator=get_performance_calibrator(),
        )

        #: Evidence from the most recent evaluation, for diagnostics.
        #: Reset at the top of every get_signal() call -- see _reset_evaluation_state.
        self.last_opportunity = None
        self.last_entry_quality = None
        self.last_threshold_decision = None
        self.last_expert_evidence: tuple = ()

        # Regime authority — one instance per ensemble, per-symbol hysteresis
        # via classify_stable()'s internal _last_regime dict (one per symbol call)
        # NOTE: A single RegimeClassifier has per-symbol state; we store one
        # classifier per symbol lazily so hysteresis is correctly separated.
        self._regime_classifiers: Dict[str, RegimeClassifier] = {}


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
    # Threshold-engine inputs
    # -------------------------------------------------------------------------

    @property
    def snapshot_timeframes(self) -> tuple[str, ...]:
        """Timeframes, beyond the ensemble's own, that its experts read.

        The runner fetches these into the MarketSnapshot so every expert is
        served from the one immutable market view. vwap_reversion runs on 5m;
        without this the snapshot carried no 5m series and it could only fail.
        """
        return tuple(sorted({
            str(interval)
            for interval in (getattr(s, "interval", None) for s in self._strategies.values())
            if interval and str(interval) != str(self.interval)
        }))

    def _reset_evaluation_state(self) -> None:
        """Drop every piece of per-evaluation evidence.

        Called first thing in ``get_signal``. Previously these attributes were
        only ever assigned -- set once in ``__init__`` and then written at
        Step 7 -- so any evaluation that returned earlier (no new candle, a
        blocked regime, no votes, an error) left the *previous* symbol's
        opportunity in place for the evidence layer to read and record against
        this candle.
        """
        self.last_opportunity = None
        self.last_entry_quality = None
        self.last_threshold_decision = None
        self.last_expert_evidence = ()

    @staticmethod
    def _volatility_context(regime_result, klines) -> VolatilityContext:
        """Normalised volatility for the threshold engine.

        ATR is converted to a percentile against the recent distribution of the
        same measure, so the value means the same thing on BTC as it would on
        any other instrument. A raw ATR would not.
        """
        atr_pct = float(getattr(regime_result, "atr_percent", 0.0) or 0.0)
        percentile_value: float | None = None
        try:
            ranges = []
            for k in klines[-120:]:
                high, low, close = float(k[2]), float(k[3]), float(k[4])
                if close > 0:
                    ranges.append((high - low) / close * 100.0)
            if len(ranges) >= 20:
                below = sum(1 for r in ranges if r <= atr_pct)
                percentile_value = below / len(ranges)
        except (IndexError, TypeError, ValueError, ZeroDivisionError):
            percentile_value = None

        compression = float(getattr(regime_result, "compression_ratio", 0.0) or 0.0)
        return VolatilityContext(
            atr_percentile=percentile_value,
            compression=max(0.0, min(1.0, compression)),
        )

    @staticmethod
    def _ema(values: List[float], period: int) -> Optional[float]:
        """EMA seeded with an SMA over the first ``period`` values.

        Returns ``None`` rather than a partial value when there is not enough
        history: an EMA200 computed from 40 candles is a number, not a trend.
        """
        if len(values) < period or period <= 0:
            return None
        multiplier = 2.0 / (period + 1)
        ema = sum(values[:period]) / period
        for value in values[period:]:
            ema = (value - ema) * multiplier + ema
        return ema

    @classmethod
    def _htf_context(cls, market_snapshot, kwargs: dict) -> HTFContext:
        """Higher-timeframe context, derived from CLOSED higher-timeframe candles.

        The snapshot's ``higher_timeframe_candles`` are already filtered to
        closed candles, so no in-progress candle can reach this, and nothing
        here issues a network request -- the candles were fetched once, for the
        evaluation this belongs to.

        Direction is price versus the HTF EMA200, matching the existing HTF bias
        veto rather than inventing a second definition of "the 4h trend".
        Strength is the distance from that EMA, normalised by the EMA and
        saturating at 5%, so it is comparable across instruments.

        Unavailable stays unavailable: if the snapshot has no HTF series, the
        series is too short for a stable EMA200, or the HTF candle is not
        timestamp-aligned with the entry candle, the context reports nothing.
        Calling that "neutral" would assert something we did not verify.
        """
        if market_snapshot is None:
            return HTFContext()
        timeframe = getattr(market_snapshot, "higher_timeframe", None)
        if not timeframe:
            return HTFContext()

        aligned = getattr(market_snapshot, "htf_is_timestamp_aligned", None)
        try:
            fresh = bool(aligned()) if callable(aligned) else True
        except Exception:
            fresh = False

        close_time = getattr(market_snapshot, "higher_timeframe_closed_candle_time", None)
        candles = list(getattr(market_snapshot, "higher_timeframe_candles", ()) or ())
        if not fresh or len(candles) < 200:
            return HTFContext(
                timeframe=str(timeframe),
                candle_close_time=close_time,
                is_fresh=fresh,
            )

        try:
            closes = [float(k[4]) for k in candles]
        except (IndexError, TypeError, ValueError):
            return HTFContext(timeframe=str(timeframe), is_fresh=False)

        ema200 = cls._ema(closes, 200)
        if ema200 is None or ema200 <= 0:
            return HTFContext(
                timeframe=str(timeframe),
                candle_close_time=close_time,
                is_fresh=fresh,
            )

        price = closes[-1]
        # Same 0.05% buffer as the HTF bias veto: inside it, the trend is not
        # making a claim in either direction.
        buffer = 0.0005
        if price > ema200 * (1 + buffer):
            direction = "BUY"
        elif price < ema200 * (1 - buffer):
            direction = "SELL"
        else:
            direction = "NEUTRAL"

        distance = abs(price - ema200) / ema200
        strength = max(0.0, min(1.0, distance / 0.05))

        return HTFContext(
            timeframe=str(timeframe),
            direction=direction,
            strength=round(strength, 6),
            candle_close_time=close_time,
            is_fresh=True,
        )

    @staticmethod
    def _market_quality_context(market_snapshot, klines) -> MarketQualityContext:
        """Market quality from what the snapshot actually knows.

        Populated from the canonical candles, which is all the snapshot carries:

        * ``volume_percentile`` -- this candle's volume against the recent
          distribution of the same measure, so it means the same thing on any
          instrument.
        * ``price_discontinuity`` -- an open that gapped from the previous close
          by more than 0.5%.
        * ``data_stale`` -- the snapshot's own staleness flag, which is a hard
          gate rather than an adjustment.

        ``spread_percentile``, ``estimated_slippage_bps`` and ``liquidity_score``
        are deliberately left ``None``. The MarketSnapshot carries no order-book
        data, and the engine skips absent inputs. Filling them with a
        neutral-looking default would dilute the inputs that are real and would
        claim knowledge the runtime does not have.
        """
        volume_percentile: float | None = None
        discontinuity = False
        try:
            volumes = [float(k[5]) for k in klines[-120:]]
            if len(volumes) >= 20 and volumes[-1] >= 0:
                below = sum(1 for v in volumes if v <= volumes[-1])
                volume_percentile = below / len(volumes)
        except (IndexError, TypeError, ValueError):
            volume_percentile = None

        try:
            if len(klines) >= 2:
                prev_close = float(klines[-2][4])
                this_open = float(klines[-1][1])
                if prev_close > 0:
                    discontinuity = abs(this_open - prev_close) / prev_close > 0.005
        except (IndexError, TypeError, ValueError):
            discontinuity = False

        return MarketQualityContext(
            volume_percentile=volume_percentile,
            price_discontinuity=discontinuity,
            data_stale=bool(getattr(market_snapshot, "is_stale", False)),
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
        # Every evaluation starts from a clean evidence slate. Without this the
        # previous symbol's opportunity survived on the instance and was
        # published as this symbol's evidence on any path that returns before
        # Step 6 -- which is most of them.
        self._reset_evaluation_state()

        # ------------------------------------------------------------------
        # Step 1 — Fetch klines (single fetch for regime computation)
        # ------------------------------------------------------------------
        market_snapshot = kwargs.get("market_snapshot")
        try:
            klines = (
                list(market_snapshot.candles)
                if market_snapshot is not None
                else self.client.klines(symbol=symbol, interval=self.interval, limit=self.klines_limit)
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
        # NOTE: no threshold floor is read here any more. ENSEMBLE_MIN_THRESHOLD_FLOOR
        # was applied at this point and could never bind, because a higher floor
        # was applied downstream. The threshold now comes from
        # AdaptiveEntryThresholdEngine, after the opportunity exists.
        try:
            from app.core.config import settings as _settings
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
            _blocked_regimes = {"STRONG_TREND"}
            _strong_trend_guard = None
            _session_filter_enabled = False
            _session_windows = []

        # NOTE: the threshold policy is NOT resolved here. Hard gates -- blocked
        # regimes, sessions, volatility spikes -- must not depend on threshold
        # configuration being resolvable, and a candle that never reaches the
        # quality stage never needs a threshold. Resolution happens at Step 4,
        # after every hard gate has had its say.
        _strat_adjustments = kwargs.get("strategy_weight_adjustments", {})
        _threshold_policy = None
        _reference_threshold = 0.0

        # All regime-level indicator fields + FIX-E observability fields.
        _imeta: dict = {
            "regime":                      regime.value,
            # None until the HTF bias veto actually runs. NOT_EVALUATED is not
            # FALSE: a veto that never ran did not find the trend aligned.
            "htf_opposed":                 None,
            # Likewise the session gate: no verdict until it is reached. The
            # early returns (regime block, no active strategies) keep these.
            "session_allowed":             None,
            "regime_confidence":           round(regime_result.regime_confidence, 3),
            "adx":                         round(regime_result.adx, 1),
            "atr_pct":                     round(regime_result.atr_percent, 3),
            "ma_slope":                    round(regime_result.ma_slope, 4),
            "compression_ratio":           round(regime_result.compression_ratio, 4),
            "breakout_pressure":           round(regime_result.breakout_pressure, 4),
            # No threshold has been resolved yet on this path.
            "threshold":                   None,
            "threshold_type":              "not_evaluated",
            "threshold_status":            "NOT_EVALUATED",
            "threshold_policy_hash":       None,
            "threshold_mode":              None,
            "threshold_band":              None,
            "perf_multipliers":            _strat_adjustments,
            "regime_gate_blocked_regimes": sorted(_blocked_regimes),
            "strong_trend_allowed_only_in_paper": True,
            "strong_trend_guard_result": (
                _strong_trend_guard.reason if _strong_trend_guard else "fail_safe_blocked"
            ),
            "regime_gate_result":          "pending",   # updated below
            "session_gate_result":         "pending",   # updated below
            "session_reason_code":         "NOT_EVALUATED",
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
        _market_type = str(kwargs.get("market_type") or "UNKNOWN").upper()
        _explicit_session = kwargs.get("enforce_session")
        if _explicit_session is None:
            _explicit_session = bool(kwargs.get("crypto_session_enabled", False))
        _crypto_bypass = _market_type == "CRYPTO" and not bool(_explicit_session)

        if _crypto_bypass:
            _imeta["session_gate_result"] = "bypassed"
            _imeta["session_reason_code"] = "CRYPTO_SESSION_24_7_BYPASS"
            _imeta["session_allowed"] = True
            logger.info(
                "[ENSEMBLE SESSION GATE] %s: CRYPTO defaults to 24/7; fixed session bypassed",
                symbol,
            )
        elif _session_filter_enabled and _session_windows:
            _session_allowed, _utc_hour = self._check_session_gate(_session_windows)
            _imeta["session_gate_result"] = "allowed" if _session_allowed else "blocked"
            _imeta["session_reason_code"] = "SESSION_ALLOWED" if _session_allowed else "SESSION_BLOCKED"
            _imeta["session_allowed"] = bool(_session_allowed)
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
            _imeta["session_reason_code"] = "SESSION_FILTER_DISABLED"
            _imeta["session_allowed"] = True

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
        # Every hard gate has now passed, so this candle will reach the quality
        # stage and does need a threshold policy. Resolution is deliberately
        # allowed to raise: contradictory threshold configuration must stop the
        # process rather than degrade to a default, because a default is what
        # hid the previous stack's saturation for months.
        _threshold_policy = get_threshold_policy(
            symbol=symbol,
            venue=kwargs.get("venue"),
            market_type=kwargs.get("market_type"),
        )
        _reference_threshold = float(_threshold_policy.base_threshold)
        _imeta["threshold_policy_hash"] = _threshold_policy.policy_hash
        _imeta["threshold_mode"] = _threshold_policy.mode
        _imeta["threshold_band"] = [
            _threshold_policy.min_threshold,
            _threshold_policy.max_threshold,
        ]

        active_strategies = {
            n: s for n, s in self._strategies.items() if n in available_active
        }

        votes: List[Tuple[str, Signal, float]] = []
        components: List[dict] = []
        errors: List[str] = []
        #: Experts that ran and FAILED, with the failure reason. Recorded as
        #: ERROR -- never as a HOLD vote -- and the threshold engine fails the
        #: candle closed. See AdaptiveEntryThresholdEngine.evaluate.
        expert_errors: Dict[str, str] = {}

        def _expert_failure(reason: str) -> Optional[str]:
            """An expert's own report that it could not evaluate.

            The experts catch their own exceptions and return HOLD with an
            ``error:`` or ``data_error:`` reason. That is a failure to
            evaluate, not an opinion, and it was being counted as a neutral
            vote: sma_cross did exactly that on every candle from 2026-09-08.
            """
            text = str(reason or "").strip()
            if text.lower().startswith(("error:", "data_error:", "strategy_error")):
                return text
            return None

        def _run(
            name: str,
            strat: Strategy,
        ) -> Tuple[str, Signal, float, str, dict, Optional[str]]:
            try:
                original_client = getattr(strat, "client", None)
                if market_snapshot is not None and original_client is not None:
                    from app.runner.market_snapshot import SnapshotMarketClient
                    strat.client = SnapshotMarketClient(original_client, market_snapshot)
                try:
                    result = strat.get_signal(symbol)
                finally:
                    if market_snapshot is not None and original_client is not None:
                        strat.client = original_client
                sig = result.signal if hasattr(result, "signal") else Signal.HOLD
                if isinstance(sig, str):
                    sig = Signal[sig.upper()] if sig.upper() in Signal.__members__ else Signal.HOLD
                conf = float(result.confidence) if hasattr(result, "confidence") else 0.0
                reason = str(getattr(result, "reason", "") or "")
                meta = getattr(result, "meta", None) or {}
                return name, sig, conf, reason, meta, _expert_failure(reason)
            except Exception as exc:
                failure = f"strategy_error:{type(exc).__name__}: {exc}"
                return name, Signal.HOLD, 0.0, failure, {}, failure

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
                            # A failed expert is recorded as ERROR, not as the
                            # HOLD it returned while reporting its failure.
                            signal="ERROR" if err else sig.value,
                            confidence=0.0 if err else conf,
                            reason=err or reason,
                            meta=component_meta,
                            threshold_floor=_reference_threshold,
                            symbol=symbol,
                            timeframe=self.interval,
                            market_regime=regime.value,
                            session_allowed=True,
                        )
                    )
                    if err:
                        errors.append(f"{name}:{err}")
                        expert_errors[name] = err
                    else:
                        votes.append((name, sig, conf))
                except Exception as exc:
                    failed_name = future_map[future]
                    errors.append(f"{failed_name}:{type(exc).__name__}")
                    expert_errors[failed_name] = f"strategy_error:{type(exc).__name__}: {exc}"

        for name in deactivated:
            components.append(
                component_breakdown(
                    strategy=name,
                    signal="DISABLED",
                    confidence=0.0,
                    reason=f"disabled_for_regime:{regime.value}",
                    meta={},
                    threshold_floor=_reference_threshold,
                    symbol=symbol,
                    timeframe=getattr(self._strategies.get(name), "interval", self.interval),
                    market_regime=regime.value,
                    session_allowed=True,
                    enabled=False,
                )
            )

        # Every expert failing is still an ERROR candle, not "no valid votes":
        # it continues to the threshold engine, which fails it closed with the
        # expert evidence on record.
        if not votes and not expert_errors:
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
        # Step 6 — Produce a TradingOpportunity (market interpretation only)
        # ------------------------------------------------------------------
        # Phase 7: the ensemble stops being one of several execution
        # authorities.  It decides WHAT the market is doing and hands the
        # evidence to TradingDecisionEngine, which decides whether that clears
        # the bar.  The threshold comparison below is the only one in the
        # active path.
        raw_conf = max(buy_pct, sell_pct)

        _direction = None
        if buy_pct > sell_pct:
            _direction = "BUY"
        elif sell_pct > buy_pct:
            _direction = "SELL"

        _snapshot_id = getattr(market_snapshot, "market_snapshot_id", None) or f"inline_{symbol}"
        _closed_candle_time = getattr(market_snapshot, "latest_closed_candle_time", None)

        if _direction is None:
            # No directional candidate. "Nothing pointed anywhere" and "the
            # strategies cancelled each other out" are different facts, and
            # neither is a confidence failure.
            opportunity = NoOpportunity(
                symbol=symbol,
                timeframe=str(kwargs.get("timeframe") or self.interval),
                market_snapshot_id=_snapshot_id,
                reason_code=(
                    QualityReason.NO_OPPORTUNITY if raw_conf <= 0
                    else QualityReason.CONSENSUS_INSUFFICIENT
                ),
                buy_score=round(buy_pct, 4),
                sell_score=round(sell_pct, 4),
                consensus=round(raw_conf, 4),
                regime=regime.value,
                active_strategies=tuple(sorted(available_active)),
                closed_candle_time=_closed_candle_time,
            )
        else:
            opportunity = build_opportunity(
                symbol=symbol,
                timeframe=str(kwargs.get("timeframe") or self.interval),
                market_snapshot_id=_snapshot_id,
                side=_direction,
                raw_confidence=raw_conf,
                consensus=raw_conf,
                buy_score=round(buy_pct, 4),
                sell_score=round(sell_pct, 4),
                votes=[(n, s.value if hasattr(s, "value") else str(s), c) for n, s, c in votes],
                regime=regime.value,
                regime_confidence=float(regime_result.regime_confidence),
                active_strategies=tuple(sorted(available_active)),
                component_breakdown=components,
                strategy_reasons=tuple(vote_details),
                atr_pct=float(regime_result.atr_percent or 0.0),
                closed_candle_time=_closed_candle_time,
                htf_timeframe=getattr(market_snapshot, "higher_timeframe", None),
                bot_instance_id=kwargs.get("bot_instance_id"),
            )

        # ------------------------------------------------------------------
        # Step 6.5 — Resolve the threshold (the ONE threshold authority)
        # ------------------------------------------------------------------
        # Expert evidence is built from the votes that were already cast. No
        # strategy is executed a second time to produce it.
        expert_evidence = experts_from_votes(
            votes,
            eligible=sorted(available_active),
            all_strategies=sorted(self._strategies.keys()),
            weights={
                n: _BASE_WEIGHTS.get(n, 1.0)
                * regime_mults.get(n, 1.0)
                * strategy_weight_adjustments.get(n, 1.0)
                for n in self._strategies
            },
            reasons={c.get("strategy"): c.get("reason", "") for c in components if isinstance(c, dict)},
            errors=expert_errors,
        )
        self.last_expert_evidence = expert_evidence

        threshold_request = AdaptiveThresholdInput(
            bot_instance_id=str(kwargs.get("bot_instance_id") or "unknown"),
            symbol=symbol,
            timeframe=str(kwargs.get("timeframe") or self.interval),
            strategy_version=self.version,
            venue=str(kwargs.get("venue") or "unknown"),
            market_type=str(kwargs.get("market_type") or "UNKNOWN"),
            run_id=kwargs.get("run_id"),
            cycle_id=kwargs.get("cycle_id"),
            market_snapshot_id=_snapshot_id,
            opportunity_id=getattr(opportunity, "opportunity_id", None),
            closed_candle_time=_closed_candle_time,
            side=_direction,
            opportunity_confidence=(raw_conf if _direction is not None else None),
            buy_score=round(buy_pct, 6),
            sell_score=round(sell_pct, 6),
            consensus=round(raw_conf, 6),
            experts=expert_evidence,
            regime=RegimeContext(
                regime=regime.value,
                regime_confidence=float(regime_result.regime_confidence),
            ),
            volatility=self._volatility_context(regime_result, klines),
            htf=self._htf_context(market_snapshot, kwargs),
            market_quality=self._market_quality_context(market_snapshot, klines),
        )
        threshold_decision = self._threshold_engine.evaluate(
            threshold_request, _threshold_policy
        )
        self.last_threshold_decision = threshold_decision

        _imeta["threshold"] = threshold_decision.final_threshold
        _imeta["threshold_status"] = threshold_decision.status
        _imeta["threshold_type"] = f"adaptive_engine/{threshold_decision.threshold_mode}"
        _imeta["threshold_decision_id"] = threshold_decision.threshold_decision_id
        _imeta["threshold_components"] = threshold_decision.observability()

        # ------------------------------------------------------------------
        # Step 7 — THE single entry-quality comparison
        # ------------------------------------------------------------------
        # The decision engine compares. It does not resolve a threshold, and it
        # applies no consensus gate: expert agreement is already one bounded
        # input to the threshold above, and gating on it twice would be two
        # authorities for one question.
        entry_quality = self._decision_engine.evaluate(
            opportunity,
            threshold_decision=threshold_decision,
        )
        self.last_opportunity = opportunity
        self.last_entry_quality = entry_quality
        effective_threshold = entry_quality.effective_entry_threshold

        if entry_quality.approved:
            final_signal = Signal.BUY if opportunity.side == "BUY" else Signal.SELL
            final_confidence = raw_conf
        else:
            final_signal = Signal.HOLD
            final_confidence = float(raw_conf)
            if final_confidence > 0:
                logger.debug(
                    f"[ENSEMBLE] {symbol}: {entry_quality.primary_reason} "
                    f"({final_confidence:.3f} vs {_fmt_threshold(effective_threshold)})"
                )

        if final_signal != Signal.HOLD:
            logger.info(
                f"[ENSEMBLE] {symbol}: "
                f"buy={buy_pct:.3f} sell={sell_pct:.3f} "
                f"thr={_fmt_threshold(effective_threshold)} "
                f"(mode={threshold_decision.threshold_mode}) "
                f"→ {final_signal.value} ({final_confidence:.3f})"
            )

        # Deprecated compatibility gate.  The orchestrated path performs exactly
        # one entry-quality comparison above.  Legacy callers may opt in while
        # they are migrated, but must do so explicitly.
        if (
            bool(kwargs.get("legacy_secondary_confidence_gate", False))
            and final_confidence < self.min_confidence
            and final_signal != Signal.HOLD
        ):
            logger.debug(
                f"[ENSEMBLE] {symbol}: BLOCKED by hard min_confidence "
                f"({final_confidence:.3f} < {self.min_confidence:.3f})"
            )
            final_signal = Signal.HOLD

        # HTF Bias check (Hard enforcement of 4h EMA200 trend)
        # None = the veto never ran (disabled, or nothing to veto). NOT_EVALUATED
        # is not FALSE: a veto that did not run did not find the trend aligned.
        htf_opposed = None
        if self.htf_bias_enabled and final_signal != Signal.HOLD:
            htf_opposed = False
            try:
                # Need ~250 candles for stable EMA200
                htf_klines = (
                    list(market_snapshot.higher_timeframe_candles)
                    if market_snapshot is not None
                    else self.client.klines(symbol=symbol, interval="4h", limit=250)
                )
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

        # The reason is whatever the single entry-quality authority decided,
        # except where a hard veto (HTF) overrode an approved candidate.
        if htf_opposed:
            entry_quality = self._decision_engine.veto(entry_quality, QualityReason.HTF_NOT_ALIGNED)
            self.last_entry_quality = entry_quality
            primary_reason = "HTF_OPPOSED"
        else:
            primary_reason = entry_quality.primary_reason

        return SignalResult(
            signal=final_signal,
            confidence=float(final_confidence),
            reason=primary_reason,
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
                # Phase 7 observability: the opportunity and the single quality
                # verdict travel with the result, so no operator has to read
                # component logs to reconstruct why a candle did or did not trade.
                "entry_quality":      entry_quality.observability(),
                "opportunity":        (
                    opportunity.evidence_summary() if opportunity.is_opportunity
                    else opportunity.to_dict()
                ),
                "opportunity_id":     (
                    opportunity.opportunity_id if opportunity.is_opportunity else None
                ),
                "market_snapshot_id": _snapshot_id,
                "hold_reason": (
                    classify_hold_reason(
                        primary_reason,
                        confidence=float(final_confidence),
                        # None when no threshold was evaluated. float(None) here
                        # raised a TypeError that two callers silently swallowed
                        # by re-running the strategy with no kwargs at all.
                        threshold_floor=(
                            None if effective_threshold is None
                            else float(effective_threshold)
                        ),
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
        reason = {
            "regime_low_vol_chop_suspended": "REGIME_LOW_VOL_CHOP",
            "regime_no_active_strategies": "NO_OPPORTUNITY",
            "no_valid_votes": "NO_OPPORTUNITY",
            "volatility_spike_detected": "VOLATILITY_SPIKE",
            "regime_klines_error": "STALE_MARKET_DATA",
            "regime_insufficient_data": "STALE_MARKET_DATA",
            "regime_classify_error": "NO_OPPORTUNITY",
        }.get(reason, reason.upper())
        if reason.startswith("REGIME_BLOCKED_"):
            reason = "REGIME_BLOCKED"
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
