"""
app/adaptive/policies.py

Section 9: Explicit Adaptation Policies for the Controlled Adaptive Learning Layer.

Each policy is a self-contained unit that:
  - defines its trigger inputs
  - enforces min sample requirements
  - produces a bounded, attributable PolicyDecision
  - has explicit smoothing rules (delegated to the engine's AsymmetricEMA instances)
  - has an explicit recovery rule
  - carries logging metadata (sample_size, confidence_in_adjustment)

The AdaptiveEngine runs all policies per tick and assembles the outputs into AdaptiveState.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple


# ---------------------------------------------------------------------------
# PolicyDecision — standardized output contract for a single policy evaluation
# ---------------------------------------------------------------------------

@dataclass
class PolicyDecision:
    """
    The output of a single policy evaluation for one adaptive tick.

    Used for:
      - Structuring AdaptiveEngine decisions
      - Feeding the audit log (Section 10)
      - Enabling per-policy observability
    """
    policy_name: str
    triggered: bool                        # Whether the policy is actively adjusting
    trigger_inputs: Dict[str, Any]         # Raw inputs that drove this decision
    raw_target: float                      # Pre-smoothing target value (before EMA)
    sample_size: int                       # How many data points drove the decision
    min_samples_required: int              # Below this → no adjustment applied
    bounds_applied: Tuple[float, float]    # (floor, ceiling) enforced
    output_field: str                      # Which AdaptiveState field this affects
    persistence: str                       # "DURABLE" | "HEURISTIC"
    confidence_in_adjustment: float        # 0.0–1.0 proxy for statistical confidence
    notes: str = ""                        # Human-readable explanation


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _sample_confidence(sample_size: int, min_required: int, max_confident: int = 50) -> float:
    """Linear confidence proxy: 0.0 at min_required samples, 1.0 at max_confident."""
    if sample_size < min_required:
        return 0.0
    return min(1.0, (sample_size - min_required) / max(1, max_confident - min_required))


# ---------------------------------------------------------------------------
# Policy 1: Loss Streak → Confidence Gate Tightening
# ---------------------------------------------------------------------------

class LossStreakPolicy:
    """
    Raises the minimum confidence threshold when consecutive losses are detected.

    Trigger: loss_streak from trade_fills (DURABLE)
    Output:  confidence_gate_modifier in [0.0, 0.12]
    Min Samples: 1 (any confirmed consecutive loss qualifies)
    Smoothing:  alpha_up=0.30 (penalty), alpha_down=0.05 (recovery) — applied by engine EMA
    Recovery:   When loss_streak returns to 0, raw_target returns to 0.0 and EMA recovers slowly
    """
    NAME = "LossStreakPolicy"
    BOUNDS = (0.0, 0.12)
    MIN_SAMPLES = 1
    STEP_PER_LOSS = 0.02

    def evaluate(self, loss_streak: int) -> PolicyDecision:
        raw_target = min(loss_streak * self.STEP_PER_LOSS, self.BOUNDS[1])
        triggered = loss_streak >= self.MIN_SAMPLES
        confidence = _sample_confidence(loss_streak, self.MIN_SAMPLES, max_confident=6)

        return PolicyDecision(
            policy_name=self.NAME,
            triggered=triggered,
            trigger_inputs={"loss_streak": loss_streak},
            raw_target=raw_target,
            sample_size=loss_streak,
            min_samples_required=self.MIN_SAMPLES,
            bounds_applied=self.BOUNDS,
            output_field="confidence_gate_modifier",
            persistence="DURABLE",
            confidence_in_adjustment=confidence,
            notes=f"Streak={loss_streak}; modifier target={raw_target:.4f}",
        )


# ---------------------------------------------------------------------------
# Policy 2: Drawdown → Size Multiplier Compression
# ---------------------------------------------------------------------------

class DrawdownPolicy:
    """
    Reduces position size multiplier in response to equity drawdown.

    Trigger: drawdown_pct from equity_snapshots (DURABLE)
    Output:  size_multiplier in [0.20, 1.0]
    Min Samples: drawdown_pct > 0.01 from DB (any valid equity reading)
    Smoothing:  alpha_down=0.50 (fast penalty), alpha_up=0.05 (slow recovery) — applied by engine EMA
    Recovery:   When drawdown_pct < 0.05, raw_target returns to 1.0 and EMA recovers
    """
    NAME = "DrawdownPolicy"
    BOUNDS = (0.20, 1.0)
    MIN_SAMPLES = 1   # One valid DB drawdown reading required

    def evaluate(self, drawdown_pct: float, sample_size: int) -> PolicyDecision:
        if drawdown_pct >= 0.15:
            raw_target = 0.20
            note = "DD>=15%"
        elif drawdown_pct >= 0.10:
            raw_target = 0.40
            note = "DD>=10%"
        elif drawdown_pct >= 0.05:
            raw_target = 0.70
            note = "DD>=5%"
        else:
            raw_target = 1.0
            note = "DD<5%"

        triggered = drawdown_pct >= 0.05
        confidence = _sample_confidence(sample_size, self.MIN_SAMPLES, max_confident=20)

        return PolicyDecision(
            policy_name=self.NAME,
            triggered=triggered,
            trigger_inputs={"drawdown_pct": round(drawdown_pct, 4)},
            raw_target=raw_target,
            sample_size=sample_size,
            min_samples_required=self.MIN_SAMPLES,
            bounds_applied=self.BOUNDS,
            output_field="size_multiplier",
            persistence="DURABLE",
            confidence_in_adjustment=confidence,
            notes=note,
        )


# ---------------------------------------------------------------------------
# Policy 3: Regime Underperformance — depresses strategy weights in bad regime
# ---------------------------------------------------------------------------

class RegimeUnderperformancePolicy:
    """
    Suppresses per-strategy weights when the active regime is producing poor trade outcomes.
    Defers to StrategyPerformanceTracker for per-strategy adjustments (already implemented).
    Here we produce a scalar regime multiplier for the whole ensemble.

    Trigger: regime_win_rate over last N trades in the active regime
    Output:  regime_weight_multiplier in [0.70, 1.0]
    Min Samples: 10 trades in the current regime
    """
    NAME = "RegimeUnderperformancePolicy"
    BOUNDS = (0.70, 1.0)
    MIN_SAMPLES = 10
    REF_WIN_RATE = 0.50

    def evaluate(
        self, regime_win_rate: float, num_regime_trades: int
    ) -> PolicyDecision:
        sufficient = num_regime_trades >= self.MIN_SAMPLES
        if not sufficient or regime_win_rate >= self.REF_WIN_RATE:
            raw_target = 1.0
            triggered = False
        else:
            ratio = regime_win_rate / self.REF_WIN_RATE
            raw_target = max(self.BOUNDS[0], min(self.BOUNDS[1], ratio))
            triggered = True

        confidence = _sample_confidence(num_regime_trades, self.MIN_SAMPLES, max_confident=50)

        return PolicyDecision(
            policy_name=self.NAME,
            triggered=triggered,
            trigger_inputs={
                "regime_win_rate": round(regime_win_rate, 4),
                "num_regime_trades": num_regime_trades,
            },
            raw_target=raw_target,
            sample_size=num_regime_trades,
            min_samples_required=self.MIN_SAMPLES,
            bounds_applied=self.BOUNDS,
            output_field="regime_weight_multiplier",
            persistence="HEURISTIC",
            confidence_in_adjustment=confidence,
            notes=f"regime_wr={regime_win_rate:.2f}, samples={num_regime_trades}",
        )


# ---------------------------------------------------------------------------
# Policy 4: Regime Recovery — re-weights strategies as regime improves
# ---------------------------------------------------------------------------

class RegimeRecoveryPolicy:
    """
    Allows regime weight to recover when the regime win rate rebounds above reference.
    Recovery is slower than penalty (handled by EMA alpha asymmetry in engine).

    Trigger: regime_win_rate sustained above REF_WIN_RATE for MIN_RECOVERY_TICKS ticks
    Output:  regime_weight_multiplier → 1.0 (restored)
    """
    NAME = "RegimeRecoveryPolicy"
    BOUNDS = (0.70, 1.0)
    MIN_SAMPLES = 10
    REF_WIN_RATE = 0.50

    def evaluate(
        self, regime_win_rate: float, num_regime_trades: int, above_ref_ticks: int
    ) -> PolicyDecision:
        sufficient = num_regime_trades >= self.MIN_SAMPLES and above_ref_ticks >= 3
        raw_target = 1.0 if (regime_win_rate >= self.REF_WIN_RATE and sufficient) else 0.70
        triggered = sufficient and regime_win_rate >= self.REF_WIN_RATE
        confidence = _sample_confidence(num_regime_trades, self.MIN_SAMPLES)

        return PolicyDecision(
            policy_name=self.NAME,
            triggered=triggered,
            trigger_inputs={
                "regime_win_rate": round(regime_win_rate, 4),
                "above_ref_ticks": above_ref_ticks,
            },
            raw_target=raw_target,
            sample_size=num_regime_trades,
            min_samples_required=self.MIN_SAMPLES,
            bounds_applied=self.BOUNDS,
            output_field="regime_weight_multiplier",
            persistence="HEURISTIC",
            confidence_in_adjustment=confidence,
            notes=(
                "Recovering" if triggered else
                "Not yet recovered (insufficient sustained ticks or samples)"
            ),
        )


# ---------------------------------------------------------------------------
# Policy 5: Strategy De-weighting / Re-weighting
# ---------------------------------------------------------------------------

class StrategyDeweightingPolicy:
    """
    Per-strategy weight multiplier based on rolling DB win rate.
    Delegates the actual computation to StrategyPerformanceTracker (already integrated).
    This policy class provides explicit documentation and generates a PolicyDecision summary.

    Trigger: per-strategy win rates from trade_fills + decision_logs (DURABLE)
    Output:  per-strategy weight multiplier in [0.70, 1.30]
    Min Samples: 10 closed trades per strategy
    """
    NAME = "StrategyDeweightingPolicy"
    BOUNDS = (0.70, 1.30)
    MIN_SAMPLES = 10

    def evaluate(
        self, strategy_adjustments: Dict[str, float], strategy_sample_sizes: Dict[str, int]
    ) -> PolicyDecision:
        min_sample = min(strategy_sample_sizes.values()) if strategy_sample_sizes else 0
        avg_adjustment = (
            sum(strategy_adjustments.values()) / len(strategy_adjustments)
            if strategy_adjustments else 1.0
        )
        triggered = any(v != 1.0 for v in strategy_adjustments.values())
        confidence = _sample_confidence(min_sample, self.MIN_SAMPLES, max_confident=50)

        return PolicyDecision(
            policy_name=self.NAME,
            triggered=triggered,
            trigger_inputs={
                "strategy_adjustments": strategy_adjustments,
                "strategy_sample_sizes": strategy_sample_sizes,
            },
            raw_target=avg_adjustment,
            sample_size=min_sample,
            min_samples_required=self.MIN_SAMPLES,
            bounds_applied=self.BOUNDS,
            output_field="strategy_weight_adjustments",
            persistence="DURABLE",
            confidence_in_adjustment=confidence,
            notes=f"Strategies adjusted: {len([v for v in strategy_adjustments.values() if v != 1.0])}",
        )


# ---------------------------------------------------------------------------
# Policy 6: Aggressiveness Recovery
# ---------------------------------------------------------------------------

class AggressivenessRecoveryPolicy:
    """
    Allows the aggressiveness score to drift back toward 1.0 when all stress conditions clear.

    Trigger: drawdown_pct < 0.05 AND loss_streak == 0 AND exec_fail_rate < 0.30 for N ticks
    Output:  Signals that recovery path is active; actual size recovery handled by EMA alpha_up
    Min Samples: 3 consecutive clean ticks (hysteresis — avoids false recoveries)
    """
    NAME = "AggressivenessRecoveryPolicy"
    BOUNDS = (0.20, 1.0)
    MIN_CLEAN_TICKS = 3

    def evaluate(
        self, drawdown_pct: float, loss_streak: int, exec_fail_rate: float, clean_ticks: int
    ) -> PolicyDecision:
        all_clear = (drawdown_pct < 0.05 and loss_streak == 0 and exec_fail_rate < 0.30)
        triggered = all_clear and clean_ticks >= self.MIN_CLEAN_TICKS
        raw_target = 1.0 if all_clear else None   # None means engine should hold current

        return PolicyDecision(
            policy_name=self.NAME,
            triggered=triggered,
            trigger_inputs={
                "drawdown_pct": round(drawdown_pct, 4),
                "loss_streak": loss_streak,
                "exec_fail_rate": round(exec_fail_rate, 4),
                "clean_ticks": clean_ticks,
            },
            raw_target=raw_target if raw_target is not None else 0.0,
            sample_size=clean_ticks,
            min_samples_required=self.MIN_CLEAN_TICKS,
            bounds_applied=self.BOUNDS,
            output_field="aggressiveness_score",
            persistence="HEURISTIC",
            confidence_in_adjustment=min(1.0, clean_ticks / 10),
            notes="Recovery path active" if triggered else "Stress conditions still present",
        )


# ---------------------------------------------------------------------------
# Policy 7: Confidence Gate Tightening / Relaxation
# ---------------------------------------------------------------------------

class ConfidenceGatePolicy:
    """
    Unifies confidence gate computation, combining streak penalty and regime offset.
    Replaces the raw streak penalty in the engine with a structured policy output.
    Coordination rule: If cooldown is HARD, cap penalty at 0.06 to avoid blind compounding.

    Trigger: loss_streak + regime_offset + cooldown_state (DURABLE + HEURISTIC)
    Output:  confidence_gate_modifier in [0.0, 0.12]
    """
    NAME = "ConfidenceGatePolicy"
    BOUNDS = (0.0, 0.12)
    MIN_SAMPLES = 1
    HARD_COOLDOWN_CAP = 0.06  # coordination cap when size is already crushed

    def evaluate(
        self,
        loss_streak: int,
        regime_offset: float,
        cooldown_state: str,
    ) -> PolicyDecision:
        streak_component = min(loss_streak * 0.02, self.BOUNDS[1])
        raw_target = max(self.BOUNDS[0], min(self.BOUNDS[1], streak_component + regime_offset))

        # Coordination Rule: Don't compound fully when cooldown already crushed size
        if cooldown_state == "HARD":
            raw_target = min(raw_target, self.HARD_COOLDOWN_CAP)

        triggered = raw_target > 0.0
        confidence = _sample_confidence(loss_streak, self.MIN_SAMPLES, max_confident=6)

        return PolicyDecision(
            policy_name=self.NAME,
            triggered=triggered,
            trigger_inputs={
                "loss_streak": loss_streak,
                "regime_offset": round(regime_offset, 4),
                "cooldown_state": cooldown_state,
            },
            raw_target=raw_target,
            sample_size=loss_streak,
            min_samples_required=self.MIN_SAMPLES,
            bounds_applied=self.BOUNDS,
            output_field="confidence_gate_modifier",
            persistence="DURABLE",
            confidence_in_adjustment=confidence,
            notes=(
                f"streak={streak_component:.3f} + regime_offset={regime_offset:.3f}"
                + (" [HARD cooldown cap applied]" if cooldown_state == "HARD" else "")
            ),
        )


# ---------------------------------------------------------------------------
# Policy 8: Execution Degradation Response
# ---------------------------------------------------------------------------

class ExecutionDegradationPolicy:
    """
    Reduces aggressiveness when execution quality degrades (high failure/rejection rate).

    Trigger: exec_fail_rate from decision_logs (DURABLE)
    Output:  execution_size_modifier in [0.70, 1.0] (stacked on size_multiplier)
    Min Samples: 20 recent execution decisions
    Smoothing:  alpha_down=0.40 (fast penalty), alpha_up=0.05 (slow recovery)
    """
    NAME = "ExecutionDegradationPolicy"
    BOUNDS = (0.70, 1.0)
    MIN_SAMPLES = 20

    def evaluate(self, exec_fail_rate: float, num_decisions: int) -> PolicyDecision:
        sufficient = num_decisions >= self.MIN_SAMPLES
        if not sufficient or exec_fail_rate < 0.30:
            raw_target = 1.0
            triggered = False
        elif exec_fail_rate >= 0.70:
            raw_target = 0.70
            triggered = True
        elif exec_fail_rate >= 0.50:
            raw_target = 0.80
            triggered = True
        else:
            raw_target = 0.90
            triggered = True

        confidence = _sample_confidence(num_decisions, self.MIN_SAMPLES, max_confident=100)

        return PolicyDecision(
            policy_name=self.NAME,
            triggered=triggered,
            trigger_inputs={
                "exec_fail_rate": round(exec_fail_rate, 4),
                "num_decisions": num_decisions,
            },
            raw_target=raw_target,
            sample_size=num_decisions,
            min_samples_required=self.MIN_SAMPLES,
            bounds_applied=self.BOUNDS,
            output_field="execution_size_modifier",
            persistence="DURABLE",
            confidence_in_adjustment=confidence,
            notes=(
                f"exec_fail={exec_fail_rate:.1%}" if triggered
                else "Insufficient samples or below threshold"
            ),
        )
