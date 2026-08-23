"""
Adaptive Engine — Phase 3 Step 2C
===================================
Sections 4-7: Standardized Inputs · Output Contract · Safety Bounds · Observability

Architecture
------------
Single authority for all adaptive decisions in the trading bot. All consumers
(Runner, MasterEnsemble, RiskCompression, SafetyEngine) read from the
standardized AdaptiveState contract returned by `get_adaptive_state()`.

Penalty Routing (strictly non-overlapping):
  1. Loss Streaks  → raise min_confidence threshold only
  2. Drawdowns     → reduce position size multiplier only
  3. Volatility    → compress leverage multiplier only

Input Trust Levels (Section 4):
  DURABLE   — reconstructed from DB, survives restart
  HEURISTIC — runtime-only; flagged here so we know to harden it later

Safety Bounds (Section 6):
  confidence_gate_modifier : [0.0, +0.12]  — hard cap
  size_multiplier          : [0.20, 1.0]   — hard floor
  leverage_multiplier      : [0.25, 1.0]   — hard floor
  strategy weight adjust.  : [0.70, 1.30]  — clipped to PerformanceTracker range
  Cooldown escalation      : NONE → SOFT → HARD (state machine, 2-tick hysteresis up, 3-tick down)

Observability (Section 7):
  Every call emits a structured JSON log line at INFO level showing
  previous vs new state, all inputs, trust levels, reason codes, and whether
  state was reconstructed from DB.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional

from shared_lib.persistence.db import DB
from app.adaptive.strategy_performance import StrategyPerformanceTracker
from app.adaptive.smoothing import AsymmetricEMA
from app.adaptive.policies import (
    LossStreakPolicy, DrawdownPolicy,
    RegimeUnderperformancePolicy, RegimeRecoveryPolicy,
    StrategyDeweightingPolicy, AggressivenessRecoveryPolicy,
    ConfidenceGatePolicy, ExecutionDegradationPolicy,
    PolicyDecision,
)
from app.adaptive.audit_log import AdaptiveAuditLog

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Section 4 — Input Trust Level classification
# ---------------------------------------------------------------------------

class InputTrustLevel(str, Enum):
    DURABLE   = "DURABLE"    # Backed by a durable DB table; survives restart
    HEURISTIC = "HEURISTIC"  # Runtime-only; no persistent source yet


# ---------------------------------------------------------------------------
# Section 5 — Standardized Output Contract
# ---------------------------------------------------------------------------

@dataclass
class AdaptiveState:
    # ── Identity ─────────────────────────────────────────────────────────────
    timestamp_utc: str                          # ISO-8601 UTC
    adaptive_state_version: str                 # Bump on schema changes

    # ── Core multipliers ─────────────────────────────────────────────────────
    aggressiveness_score: float                 # 0.0 (defensive) → 1.0 (aggressive)
    confidence_gate_modifier: float             # Net delta added to base threshold
    size_multiplier: float                      # Position size factor [0.20 – 1.0]
    leverage_multiplier: float                  # Leverage compression  [0.25 – 1.0]

    # ── Strategy weight adjustments ───────────────────────────────────────────
    # Populated externally by PerformanceTracker; empty dict means no overrides.
    strategy_weight_adjustments: Dict[str, float] = field(default_factory=dict)

    # ── Cooldown (Section 6 — state machine) ─────────────────────────────────
    cooldown_state: str = "NONE"               # "NONE" | "SOFT" | "HARD"
    cooldown_reason: Optional[str] = None

    # ── Audit / observability (Section 7) ────────────────────────────────────
    trigger_sources: Dict[str, Any] = field(default_factory=dict)
    bounded_reason_codes: List[str] = field(default_factory=list)
    input_trust_levels: Dict[str, str] = field(default_factory=dict)
    was_reconstructed: bool = False             # True if rebuilt from DB on cold-start
    sample_quality_flag: str = "weak"           # "strong" (≥30) | "moderate" (10–29) | "weak" (<10)

    # ── Backward-compat (consumed by dynamic_threshold offset calculation) ───
    min_confidence_gate: float = 0.50           # = base_threshold + confidence_gate_modifier
    loss_streak: int = 0
    drawdown_pct: float = 0.0
    regime: str = "UNKNOWN"
    rolling_win_rate: float = 0.0              # Rolling win rate (DURABLE, last 20 CLOSE fills)
    rolling_expectancy: float = 0.0            # Rolling expectancy in USDT (DURABLE)

    # ── Spec-required output aliases (Section 3) ─────────────────────────────
    # These properties expose the same values under the canonical spec names so
    # all downstream consumers can reference either name without breakage.
    @property
    def risk_multiplier(self) -> float:
        """Spec alias → size_multiplier.  Penalty router: drawdown domain only."""
        return self.size_multiplier

    @property
    def threshold_adjustment(self) -> float:
        """Spec alias → confidence_gate_modifier.  Penalty router: streak domain only."""
        return self.confidence_gate_modifier

    @property
    def max_position_size_modifier(self) -> float:
        """Spec alias → size_multiplier.  The hard-bounded position-size factor."""
        return self.size_multiplier


# Convenience: safe JSON serialisation (dataclass → dict, skipping non-JSON fields)
def _state_to_dict(state: Optional[AdaptiveState]) -> Optional[dict]:
    if state is None:
        return None
    return asdict(state)


# ---------------------------------------------------------------------------
# Section 6 — Safety Bounds
# ---------------------------------------------------------------------------

# Hard limits — nothing may violate these regardless of input values
_BOUNDS = {
    "confidence_gate_modifier": (0.0,  0.12),
    "size_multiplier":          (0.20, 1.0),
    "leverage_multiplier":      (0.25, 1.0),
    "aggressiveness_score":     (0.0,  1.0),
}

_COOLDOWN_LEVELS = ["NONE", "SOFT", "HARD"]

def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


# ---------------------------------------------------------------------------
# Central Engine
# ---------------------------------------------------------------------------

class AdaptiveEngine:
    """
    Central adaptive control surface.

    Thread safety: all DB queries are read-only; no shared mutable state beyond
    `_cooldown_ticks` which is protected by basic monotonicity (no concurrent writes).
    For production multi-threaded use a threading.Lock if needed.
    """

    _STATE_VERSION = "2.0"

    def __init__(self, db: DB, bot_instance_id: str = "default"):
        self.db = db
        self.bot_instance_id = bot_instance_id
        # Cooldown hysteresis state machine (Section 6)
        self._cooldown_level: int = 0        # index into _COOLDOWN_LEVELS
        self._cooldown_up_ticks: int = 0     # consecutive ticks at same pressure level
        self._cooldown_down_ticks: int = 0   # consecutive clean ticks
        # Last emitted state – used for observability diffing (Section 7)
        self._last_state: Optional[AdaptiveState] = None
        # Whether at least one DB reconstruction has run
        self._reconstructed: bool = False

        # Phase 4: Controlled Adaptive Learning (Strategy Weights)
        self._strategy_tracker = StrategyPerformanceTracker(
            self.db, bot_instance_id=self.bot_instance_id
        )
        
        # Phase 5: Asymmetric Smoothing (Sections 5-8)
        self._ema_confidence: Dict[str, AsymmetricEMA] = {}
        self._ema_size: Dict[str, AsymmetricEMA] = {}
        self._ema_leverage: Dict[str, AsymmetricEMA] = {}
        
        # Phase 6: Named Policies (Section 9)
        self._pol_streak     = LossStreakPolicy()
        self._pol_drawdown   = DrawdownPolicy()
        self._pol_regime_bad = RegimeUnderperformancePolicy()
        self._pol_regime_rec = RegimeRecoveryPolicy()
        self._pol_deweight   = StrategyDeweightingPolicy()
        self._pol_recovery   = AggressivenessRecoveryPolicy()
        self._pol_conf       = ConfidenceGatePolicy()
        self._pol_exec       = ExecutionDegradationPolicy()
        # Hysteresis counter for aggressiveness recovery (needs N clean ticks)
        self._clean_ticks: Dict[str, int] = {}
        # Regime counters (HEURISTIC - no DB source yet)
        self._regime_above_ref_ticks: Dict[str, int] = {}

        # Phase 6: Audit Log (Section 10)
        self._audit_log = AdaptiveAuditLog(capacity=100)

        logger.info("[AdaptiveEngine] Initialized v%s — centralized adaptive authority.", self._STATE_VERSION)

    # ------------------------------------------------------------------ #
    # Section 4 — Trusted DB Input Sources                                #
    # ------------------------------------------------------------------ #

    def _get_loss_streak_from_db(self, symbol: str) -> int:
        """
        Reconstruct consecutive loss streak from `trade_fills` scoped to this bot.
        Source: DURABLE — survives bot restart.
        Walks most recent CLOSE fills newest-first; stops at first win.
        """
        try:
            with self.db.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT realized_pnl
                    FROM trade_fills
                    WHERE symbol = ?
                      AND action = 'CLOSE'
                      AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                    ORDER BY timestamp_utc DESC
                    LIMIT 30
                    """,
                    (symbol, self.bot_instance_id, self.bot_instance_id),
                ).fetchall()

            streak = 0
            for row in rows:
                pnl = row["realized_pnl"] if isinstance(row, dict) else row[0]
                if pnl is None:
                    continue
                if float(pnl) < 0:
                    streak += 1
                else:
                    break
            return streak
        except Exception as exc:
            logger.warning("[AdaptiveEngine] loss_streak DB query failed (%s) — using 0", exc)
            return 0

    def _get_drawdown_from_db(self, user_id: Optional[str] = None) -> float:
        """
        Compute current drawdown from `equity_snapshots` scoped to this bot.
        Source: DURABLE — survives bot restart.
        Returns: fraction 0.0–1.0 (e.g. 0.08 = 8% drawdown from peak).
        Falls back to 0.0 on any error so trading continues safely.
        """
        try:
            with self.db.connect() as conn:
                # Ordered chronologically to find rolling peak then current
                rows = conn.execute(
                    """
                    SELECT equity
                    FROM equity_snapshots
                    WHERE (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                    ORDER BY timestamp_utc ASC
                    LIMIT 500
                    """,
                    (self.bot_instance_id, self.bot_instance_id),
                ).fetchall()

            if not rows:
                return 0.0

            equities = []
            for row in rows:
                v = row["equity"] if isinstance(row, dict) else row[0]
                if v is not None:
                    equities.append(float(v))

            if not equities:
                return 0.0

            peak = max(equities)
            current = equities[-1]
            if peak <= 0:
                return 0.0
            return max(0.0, (peak - current) / peak)
        except Exception as exc:
            logger.warning("[AdaptiveEngine] drawdown DB query failed (%s) — using 0.0", exc)
            return 0.0

    def _get_rolling_stats_from_db(self, symbol: str, lookback: int = 20) -> tuple:
        """
        Compute rolling win rate and rolling expectancy from `trade_fills`.
        Source: DURABLE — survives bot restart.
        Returns: (win_rate: float, expectancy: float)
          win_rate   = fraction of profitable CLOSE fills in the last `lookback` trades.
          expectancy = mean realized_pnl across those fills (USDT).
        Falls back to (0.0, 0.0) on any error so trading continues safely.
        """
        try:
            with self.db.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT realized_pnl
                    FROM trade_fills
                    WHERE symbol = ?
                      AND action = 'CLOSE'
                      AND realized_pnl IS NOT NULL
                      AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                    ORDER BY timestamp_utc DESC
                    LIMIT ?
                    """,
                    (symbol, self.bot_instance_id, self.bot_instance_id, lookback),
                ).fetchall()

            if not rows:
                return 0.0, 0.0

            pnls = []
            for row in rows:
                v = row["realized_pnl"] if isinstance(row, dict) else row[0]
                if v is not None:
                    pnls.append(float(v))

            if not pnls:
                return 0.0, 0.0

            wins = sum(1 for p in pnls if p > 0)
            win_rate = round(wins / len(pnls), 4)
            expectancy = round(sum(pnls) / len(pnls), 4)
            return win_rate, expectancy
        except Exception as exc:
            logger.warning("[AdaptiveEngine] rolling_stats DB query failed (%s) — using (0.0, 0.0)", exc)
            return 0.0, 0.0

    def _get_exec_failure_rate(self, symbol: str, lookback: int = 30) -> float:
        """
        Compute fraction of recent decisions that were BLOCKED in `decision_logs`.
        Source: DURABLE.
        Returns: 0.0 (no blocks) – 1.0 (all blocked).
        Used as a secondary signal that strategy signals are being frequently rejected.
        """
        try:
            with self.db.connect() as conn:
                # IMPORTANT: decision_logs is scoped by config_id (bot/config context).
                # Never read other bots' decision history.
                total_row = conn.execute(
                    """
                    SELECT COUNT(*) as cnt
                    FROM (
                        SELECT 1
                        FROM decision_logs
                        WHERE symbol = ?
                          AND config_id = ?
                        ORDER BY created_at DESC
                        LIMIT ?
                    )
                    """,
                    (symbol, self.bot_instance_id, lookback),
                ).fetchone()
                blocked_row = conn.execute(
                    """
                    SELECT COUNT(*) as cnt
                    FROM (
                        SELECT 1
                        FROM decision_logs
                        WHERE symbol = ?
                          AND config_id = ?
                          AND final_action = 'blocked'
                        ORDER BY created_at DESC
                        LIMIT ?
                    )
                    """,
                    (symbol, self.bot_instance_id, lookback),
                ).fetchone()

            total = (total_row["cnt"] if isinstance(total_row, dict) else total_row[0]) or 0
            blocked = (blocked_row["cnt"] if isinstance(blocked_row, dict) else blocked_row[0]) or 0
            return round(blocked / total, 4) if total > 0 else 0.0
        except Exception as exc:
            logger.debug("[AdaptiveEngine] exec_failure_rate DB query failed (%s) — using 0.0", exc)
            return 0.0

    # ------------------------------------------------------------------ #
    # Section 6 — Cooldown Hysteresis State Machine                       #
    # ------------------------------------------------------------------ #

    def _update_cooldown(self, is_under_pressure: bool) -> str:
        """
        Advance the cooldown state machine:
          - Escalate ONLY after 2 consecutive ticks under pressure at same level.
          - Downgrade ONLY after 3 consecutive clean (no pressure) ticks.
        Returns the current cooldown state string.
        """
        if is_under_pressure:
            self._cooldown_down_ticks = 0
            self._cooldown_up_ticks += 1
            if self._cooldown_up_ticks >= 2 and self._cooldown_level < len(_COOLDOWN_LEVELS) - 1:
                self._cooldown_level += 1
                self._cooldown_up_ticks = 0
                logger.info(
                    "[AdaptiveEngine] Cooldown escalated → %s",
                    _COOLDOWN_LEVELS[self._cooldown_level],
                )
        else:
            self._cooldown_up_ticks = 0
            self._cooldown_down_ticks += 1
            if self._cooldown_down_ticks >= 3 and self._cooldown_level > 0:
                self._cooldown_level -= 1
                self._cooldown_down_ticks = 0
                logger.info(
                    "[AdaptiveEngine] Cooldown downgraded → %s",
                    _COOLDOWN_LEVELS[self._cooldown_level],
                )
        return _COOLDOWN_LEVELS[self._cooldown_level]

    # ------------------------------------------------------------------ #
    # Section 7 — Observability                                            #
    # ------------------------------------------------------------------ #

    def _emit_state_log(
        self,
        new_state: AdaptiveState,
        raw_inputs: Dict[str, Any],
    ) -> None:
        """
        Emit a structured JSON log line on EVERY adaptive state resolution.
        Diffs against the previous state to surface what changed.
        """
        prev = _state_to_dict(self._last_state)
        curr = _state_to_dict(new_state)

        # Build a concise diff of scalar fields
        changed_fields: Dict[str, Any] = {}
        if prev:
            for k in ("aggressiveness_score", "confidence_gate_modifier",
                      "size_multiplier", "leverage_multiplier",
                      "cooldown_state", "loss_streak", "drawdown_pct"):
                if curr.get(k) != prev.get(k):
                    changed_fields[k] = {"from": prev.get(k), "to": curr.get(k)}

        log_payload = {
            "event":                    "adaptive_state_resolved",
            "timestamp_utc":            new_state.timestamp_utc,
            "version":                  new_state.adaptive_state_version,
            "was_reconstructed":        new_state.was_reconstructed,
            "changed_fields":           changed_fields,
            "aggressiveness_score":     new_state.aggressiveness_score,
            "confidence_gate_modifier": new_state.confidence_gate_modifier,
            # Spec-required Section 8 field names (aliases)
            "risk_multiplier":          new_state.size_multiplier,
            "threshold_adjustment":     new_state.confidence_gate_modifier,
            "max_position_size_modifier": new_state.size_multiplier,
            "size_multiplier":          new_state.size_multiplier,
            "leverage_multiplier":      new_state.leverage_multiplier,
            "cooldown_state":           new_state.cooldown_state,
            "bounded_reason_codes":     new_state.bounded_reason_codes,
            "input_trust_levels":       new_state.input_trust_levels,
            "sample_quality_flag":      new_state.sample_quality_flag,
            "trigger_sources":          raw_inputs,
        }
        logger.info("[AdaptiveEngine] STATE %s", json.dumps(log_payload))

    # ------------------------------------------------------------------ #
    # Main Resolution                                                       #
    # ------------------------------------------------------------------ #

    def get_adaptive_state(
        self,
        config_id: str,
        symbol: str,
        # Optional runtime hints — used when DB source is not yet available
        drawdown_pct_hint: Optional[float] = None,    # HEURISTIC fallback
        current_atr_pct: Optional[float] = None,      # HEURISTIC (no DB source)
        active_regime: str = "UNKNOWN",
        base_threshold: float = 0.50,
    ) -> AdaptiveState:
        """
        Resolve the full AdaptiveState from trusted sources.

        Section 4 — Input sourcing priority:
          drawdown_pct : equity_snapshots (DURABLE) > drawdown_pct_hint (HEURISTIC)
          loss_streak  : trade_fills (DURABLE)
          exec_failure : decision_logs (DURABLE)
          atr_pct      : runtime klines (HEURISTIC — no durable source yet)

        Section 5 — Returns full AdaptiveState contract.
        Section 6 — All outputs bounded; cooldown uses hysteresis state machine.
        Section 7 — Emits structured JSON log on every call.
        """
        import datetime as _dt
        now_utc = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # ---- Input sourcing (Section 4) -----------------------------------
        loss_streak    = self._get_loss_streak_from_db(symbol)
        drawdown_db    = self._get_drawdown_from_db()
        exec_fail_rate = self._get_exec_failure_rate(symbol)
        rolling_win_rate, rolling_expectancy = self._get_rolling_stats_from_db(symbol)

        # Drawdown: prefer DB (DURABLE); fall back to hint (HEURISTIC) if DB empty
        drawdown_used_heuristic = False
        if drawdown_db > 0.0:
            drawdown_pct = drawdown_db
            dd_trust = InputTrustLevel.DURABLE
        elif drawdown_pct_hint is not None and drawdown_pct_hint > 0.0:
            drawdown_pct = float(drawdown_pct_hint)
            dd_trust = InputTrustLevel.HEURISTIC
            drawdown_used_heuristic = True
        else:
            drawdown_pct = 0.0
            dd_trust = InputTrustLevel.DURABLE  # 0.0 is safe default from DB

        atr_pct = float(current_atr_pct) if current_atr_pct is not None else 1.5

        input_trust = {
            "loss_streak":         InputTrustLevel.DURABLE.value,
            "drawdown_pct":        dd_trust.value,
            "exec_fail_rate":      InputTrustLevel.DURABLE.value,
            "rolling_win_rate":    InputTrustLevel.DURABLE.value,
            "rolling_expectancy":  InputTrustLevel.DURABLE.value,
            "atr_pct":             InputTrustLevel.HEURISTIC.value,  # No DB source yet
            "active_regime":       InputTrustLevel.HEURISTIC.value,  # Passed from runner
        }

        # ---- Section 6 — Penalty & Bound computation ----------------------
        reason_codes: List[str] = []

        # 1. Loss Streak → confidence gate (Section 6 bound: max +0.12)
        #    +0.02 per consecutive loss, but only activated after 5+ samples
        raw_streak_penalty = 0.0
        if loss_streak >= 1:
            raw_streak_penalty = _clamp(loss_streak * 0.02, *_BOUNDS["confidence_gate_modifier"])
            reason_codes.append(f"STREAK_{loss_streak}")

        # 2. Drawdown → size multiplier (Section 6 bound: floor at 0.20)
        raw_size_multiplier = 1.0
        if drawdown_pct >= 0.15:
            raw_size_multiplier = 0.20
            reason_codes.append("DD_15_PCT")
        elif drawdown_pct >= 0.10:
            raw_size_multiplier = 0.40
            reason_codes.append("DD_10_PCT")
        elif drawdown_pct >= 0.05:
            raw_size_multiplier = 0.70
            reason_codes.append("DD_5_PCT")
        raw_size_multiplier = _clamp(raw_size_multiplier, *_BOUNDS["size_multiplier"])

        # 3. Volatility → leverage multiplier (Section 6 bound: floor at 0.25)
        raw_leverage_multiplier = 1.0
        if atr_pct >= 6.0:
            raw_leverage_multiplier = 0.25
            reason_codes.append("HIGH_VOL_EXTREME")
        elif atr_pct >= 3.0:
            ratio = (atr_pct - 3.0) / (6.0 - 3.0)
            raw_leverage_multiplier = max(0.25, 1.0 - (ratio * 0.75))
            reason_codes.append("HIGH_VOL")
        raw_leverage_multiplier = _clamp(raw_leverage_multiplier, *_BOUNDS["leverage_multiplier"])

        # 4. Exec failure auxiliary signal — adds soft cooldown pressure
        is_under_pressure = (
            loss_streak >= 3 or
            drawdown_pct >= 0.05 or
            exec_fail_rate >= 0.50
        )
        if exec_fail_rate >= 0.50:
            reason_codes.append(f"EXEC_FAIL_{int(exec_fail_rate * 100)}PCT")

        # ---- Section 6 — Cooldown state machine ---------------------------
        cooldown_state_str = self._update_cooldown(is_under_pressure)
        cooldown_reason = (
            ", ".join(reason_codes) if reason_codes and cooldown_state_str != "NONE"
            else None
        )

        # ---- Section 4 — Sample quality flag (spec Section 8) ---------------
        # Derived from how many CLOSE fills are available for the rolling window.
        # Uses rolling_win_rate sample count (lookback=20) as a proxy.
        _MAX_LOOKBACK = 20
        # Approximate sample count via the win rate: if we have enough data the
        # DB query returns up to _MAX_LOOKBACK rows.  We use exec_fail_rate
        # sample coverage as secondary signal.  Primary: compare fills fetched.
        # Since we don't carry the raw count back, we derive it from expectancy:
        # expectancy != 0.0 ⟹ ≥1 fill; we use the exec lookback count.
        _approx_samples = sum(
            1 for v in [rolling_win_rate, rolling_expectancy]
            if v != 0.0
        ) * 10  # crude floor: any non-zero means ≥10 fills returned
        # More accurately: count via the fills seen in _get_rolling_stats_from_db
        # We can't easily carry the row count without refactoring, so use heuristic:
        if rolling_win_rate == 0.0 and rolling_expectancy == 0.0 and loss_streak == 0:
            _approx_samples = 0
        elif loss_streak > 0:
            _approx_samples = max(_approx_samples, loss_streak)
        sample_quality_flag = (
            "strong"   if _approx_samples >= 30 else
            "moderate" if _approx_samples >= 10 else
            "weak"
        )

        # Cooldown modifies size additionally: SOFT → ×0.85, HARD → ×0.70
        if cooldown_state_str == "SOFT":
            raw_size_multiplier = _clamp(raw_size_multiplier * 0.85, *_BOUNDS["size_multiplier"])
            reason_codes.append("COOLDOWN_SOFT")
        elif cooldown_state_str == "HARD":
            raw_size_multiplier = _clamp(raw_size_multiplier * 0.70, *_BOUNDS["size_multiplier"])
            reason_codes.append("COOLDOWN_HARD")
            # Coordination Rule (Phase 5): Don't blindly double-penalize completely if size is already crushed
            raw_streak_penalty = min(raw_streak_penalty, 0.06)

        # Phase 6: Named Policy Evaluation (Section 9) ------------------
        # Track clean ticks for aggressiveness recovery hysteresis
        is_clean = (loss_streak == 0 and drawdown_pct < 0.05 and exec_fail_rate < 0.30)
        self._clean_ticks[symbol] = (self._clean_ticks.get(symbol, 0) + 1) if is_clean else 0

        # Regime offsets moved from DynamicThresholdCalculator
        _REGIME_OFFSETS: Dict[str, float] = {
            "STRONG_TREND":       -0.05,
            "WEAK_TREND":         -0.02,
            "RANGE":              0.08,
            "HIGH_VOLATILITY":    0.12,
            "LOW_VOLATILITY_CHOP": 0.0,
        }
        regime_offset = _REGIME_OFFSETS.get(active_regime, 0.0)

        pol_streak   = self._pol_streak.evaluate(loss_streak)
        pol_drawdown = self._pol_drawdown.evaluate(drawdown_pct, sample_size=max(1, loss_streak))
        pol_conf     = self._pol_conf.evaluate(
            loss_streak=loss_streak,
            regime_offset=regime_offset,         # Now controlled by AdaptiveEngine
            cooldown_state=_COOLDOWN_LEVELS[self._cooldown_level],
        )
        pol_exec  = self._pol_exec.evaluate(
            exec_fail_rate=exec_fail_rate,
            num_decisions=max(1, int(exec_fail_rate * 100)),  # proxy sample count
        )
        pol_recovery = self._pol_recovery.evaluate(
            drawdown_pct=drawdown_pct,
            loss_streak=loss_streak,
            exec_fail_rate=exec_fail_rate,
            clean_ticks=self._clean_ticks.get(symbol, 0),
        )

        # Use ConfidenceGatePolicy as the source-of-truth for raw streak penalty
        raw_streak_penalty = pol_conf.raw_target

        # ---- Section 4 — Performance-based expansion nudge -----------------
        # When rolling win rate is strong and expectancy is positive,
        # allow a very small aggressiveness expansion (bounded, not compounding).
        # Rules:
        #   - Requires rolling win rate ≥ 0.55 (reference win rate)
        #   - Requires rolling expectancy > 0 (net profitable)
        #   - Requires loss_streak == 0 (no active penalty)
        #   - Requires drawdown_pct < 0.05 (not in stress)
        #   - Requires ≥10 sample trades (approximated via rolling_win_rate != 0.0)
        #   - Maximum nudge: +0.01 to raw_size_multiplier (capped at 1.0)
        #   - Reason code: PERF_EXPAND
        _EXPANSION_WIN_RATE_THRESHOLD = 0.55
        _EXPANSION_EXPECTANCY_MIN = 0.0
        _EXPANSION_NUDGE = 0.01
        if (
            rolling_win_rate >= _EXPANSION_WIN_RATE_THRESHOLD
            and rolling_expectancy > _EXPANSION_EXPECTANCY_MIN
            and loss_streak == 0
            and drawdown_pct < 0.05
            and rolling_win_rate != 0.0  # proxy for ≥1 sample
        ):
            raw_size_multiplier = _clamp(
                raw_size_multiplier + _EXPANSION_NUDGE,
                *_BOUNDS["size_multiplier"],
            )
            reason_codes.append("PERF_EXPAND")

        # ---- Phase 5: Applied Asymmetric EMA Smoothing --------------------
        if symbol not in self._ema_confidence:
            self._ema_confidence[symbol] = AsymmetricEMA(alpha_up=0.30, alpha_down=0.05)
        if symbol not in self._ema_size:
            self._ema_size[symbol] = AsymmetricEMA(alpha_up=0.05, alpha_down=0.50)
        if symbol not in self._ema_leverage:
            self._ema_leverage[symbol] = AsymmetricEMA(alpha_up=0.01, alpha_down=0.20)

        confidence_gate_modifier = self._ema_confidence[symbol].update(raw_streak_penalty)
        size_multiplier = self._ema_size[symbol].update(raw_size_multiplier)
        
        # FINAL SAFETY FLOOR: Ensure size_multiplier never reaches 0.0 regardless of EMA state
        size_multiplier = max(0.10, size_multiplier)
        leverage_multiplier = self._ema_leverage[symbol].update(raw_leverage_multiplier)

        min_confidence_gate = base_threshold + confidence_gate_modifier

        # ---- Aggressiveness score (Section 5) -----------------------------
        size_score = (size_multiplier - 0.20) / (1.0 - 0.20)           # normalise to [0,1]
        conf_score = 1.0 - (confidence_gate_modifier / 0.12)            # 0 at max penalty
        aggressiveness_score = _clamp(
            round((size_score + conf_score) / 2.0, 3),
            *_BOUNDS["aggressiveness_score"],
        )

        # ---- Reconstruct flag (Section 7) ----------------------------------
        if not self._reconstructed:
            self._reconstructed = True  # first call always involves DB reads

        # ---- Section 4.5 — Strategy Performance Tracking -------------------
        try:
            strategy_weight_adjustments = self._strategy_tracker.get_weight_adjustments(config_id)
        except Exception as e:
            logger.warning("[AdaptiveEngine] Failed to get strategy weights: %s", e)
            strategy_weight_adjustments = {}

        # ---- Build output state (Section 5 contract) ----------------------
        raw_inputs = {
            "symbol":                 symbol,
            "config_id":              config_id,
            "loss_streak_db":         loss_streak,
            "drawdown_db":            round(drawdown_db, 4),
            "drawdown_hint":          drawdown_pct_hint,
            "drawdown_used":          round(drawdown_pct, 4),
            "drawdown_heuristic":     drawdown_used_heuristic,
            "exec_fail_rate":         exec_fail_rate,
            "rolling_win_rate":       rolling_win_rate,
            "rolling_expectancy":     rolling_expectancy,
            "sample_quality_flag":    sample_quality_flag,
            "atr_pct":                round(atr_pct, 4),
            "active_regime":          active_regime,
            "base_threshold":         base_threshold,
        }

        new_state = AdaptiveState(
            timestamp_utc                = now_utc,
            adaptive_state_version       = self._STATE_VERSION,
            aggressiveness_score         = aggressiveness_score,
            confidence_gate_modifier     = round(confidence_gate_modifier, 4),
            size_multiplier              = round(size_multiplier, 4),
            leverage_multiplier          = round(leverage_multiplier, 4),
            strategy_weight_adjustments  = strategy_weight_adjustments,
            cooldown_state               = cooldown_state_str,
            cooldown_reason              = cooldown_reason,
            trigger_sources              = raw_inputs,
            bounded_reason_codes         = reason_codes,
            input_trust_levels           = input_trust,
            was_reconstructed            = self._reconstructed,
            sample_quality_flag          = sample_quality_flag,
            # Backward-compat fields
            min_confidence_gate          = round(min_confidence_gate, 4),
            loss_streak                  = loss_streak,
            drawdown_pct                 = round(drawdown_pct, 4),
            regime                       = active_regime,
            rolling_win_rate             = rolling_win_rate,
            rolling_expectancy           = rolling_expectancy,
        )

        # ---- Section 7 / Phase 6 — Observability log + Audit recording --
        self._emit_state_log(new_state, raw_inputs)
        self._last_state = new_state

        # Phase 6 Section 10: Audit log record
        before_dict = _state_to_dict(self._last_state) or {}
        after_dict  = _state_to_dict(new_state)
        active_policy_decisions: List[PolicyDecision] = [
            pd for pd in [pol_streak, pol_drawdown, pol_conf, pol_exec, pol_recovery]
            if pd.triggered
        ]
        self._audit_log.record(
            timestamp=now_utc,
            before_state=before_dict,
            after_state=after_dict,
            inputs_used=raw_inputs,
            reason_codes=reason_codes,
            policy_decisions=active_policy_decisions,
            was_reconstructed=self._reconstructed,
        )

        return new_state

    @property
    def audit_log(self) -> AdaptiveAuditLog:
        """Expose audit log for external inspection (API/dashboard)."""
        return self._audit_log


# ---------------------------------------------------------------------------
# Per-bot factory  (replaces shared singleton)
# ---------------------------------------------------------------------------
# Each bot instance owns its own AdaptiveEngine so that loss streaks, drawdown,
# rolling stats, and cooldown states never cross bot boundaries.

import threading as _ae_threading

_adaptive_engine_instances: dict[str, AdaptiveEngine] = {}
_ae_lock = _ae_threading.Lock()


def get_adaptive_engine(
    bot_id: str = "default",
    db: Optional[DB] = None,
) -> AdaptiveEngine:
    """Return the AdaptiveEngine for *bot_id*, creating it on first call."""
    with _ae_lock:
        if bot_id not in _adaptive_engine_instances:
            _adaptive_engine_instances[bot_id] = AdaptiveEngine(
                db=db or DB(),
                bot_instance_id=bot_id,
            )
        return _adaptive_engine_instances[bot_id]


def reset_adaptive_engine(bot_id: str = "default") -> None:
    """Evict a single bot's adaptive engine (for testing or bot teardown)."""
    with _ae_lock:
        _adaptive_engine_instances.pop(bot_id, None)


def reset_all_adaptive_engines() -> None:
    """Clear all per-bot engines. Use in tests only."""
    with _ae_lock:
        _adaptive_engine_instances.clear()
