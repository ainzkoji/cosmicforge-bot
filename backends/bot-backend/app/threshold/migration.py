"""Historical record of the threshold architecture that was removed.

**This module does not participate in runtime policy resolution.** It reads no
legacy setting, provides no fallback and maps no obsolete key. Every one of
those responsibilities existed during the transition and has been deleted along
with the architecture it was bridging.

What remains is two things:

1. :data:`LEGACY_SETTINGS` — a permanent, checked record of what each old
   control was and what happened to it. Published by the threshold policy route
   and asserted by tests, so "is this setting still live?" has an answer that
   cannot drift.
2. :func:`startup_report` — one resolved threshold configuration, printed at
   startup.

The rule that produced the removal: *a configuration setting that cannot affect
behaviour must not exist*. ``ENSEMBLE_MIN_THRESHOLD_FLOOR`` was documented as the
binding constraint on the entry threshold, was deliberately tuned from 0.50 to
0.55 by an operator, and could not affect anything, because a floor of 0.70 was
applied after it. The keys are now rejected at startup rather than ignored —
see ``app.core.config.detect_legacy_threshold_keys``.
"""
from __future__ import annotations

import logging
from typing import Any

from app.threshold.contracts import ThresholdMode

logger = logging.getLogger(__name__)


# -- Disposition of every legacy threshold control ---------------------------

DELETED = "DELETED"
MIGRATED_THEN_DELETED = "MIGRATED_THEN_DELETED"
RETAINED_NON_THRESHOLD = "RETAINED_NON_THRESHOLD"

DISPOSITIONS = frozenset({DELETED, MIGRATED_THEN_DELETED, RETAINED_NON_THRESHOLD})

#: name -> (disposition, where it lived, what happened to it)
LEGACY_SETTINGS: dict[str, tuple[str, str, str]] = {
    "MIN_CONFIDENCE_THRESHOLD": (
        MIGRATED_THEN_DELETED,
        "app/core/config.py + .env",
        "Its value was migrated once into THRESHOLD_BASE, the centre of the "
        "adaptive band. The setting, its parser, its startup validation and its "
        "deprecation warning are all deleted. The key is now rejected at "
        "startup: nothing reads it, ever again.",
    ),
    "ENSEMBLE_MIN_THRESHOLD_FLOOR": (
        DELETED,
        "app/core/config.py + app/strategy/master_ensemble.py + .env",
        "Deleted outright. It was dominated by MIN_CONFIDENCE_THRESHOLD and "
        "could never bind; the operator tuning recorded as T-04 (0.50 -> 0.55) "
        "had no effect. The single band is THRESHOLD_MIN/THRESHOLD_MAX. The key "
        "is rejected at startup.",
    ),
    "app/risk/dynamic_threshold.py": (
        DELETED,
        "app/risk/dynamic_threshold.py",
        "The whole module: DynamicThresholdCalculator, the rolling percentile "
        "window, log_threshold_event and the module-level MIN/MAX/FALLBACK "
        "bounds. It independently calculated an entry threshold, which is a "
        "second authority by definition. git rm.",
    ),
    "DYNAMIC_THRESHOLD_MIN": (
        DELETED,
        "app/risk/dynamic_threshold.py + .env.data_collection",
        "Deleted with the calculator. Its output was discarded by "
        "max(<=0.65, 0.70) on every candle for months.",
    ),
    "DYNAMIC_THRESHOLD_MAX": (
        DELETED,
        "app/risk/dynamic_threshold.py + .env.data_collection",
        "Deleted with the calculator.",
    ),
    "DYNAMIC_THRESHOLD_FALLBACK": (
        DELETED,
        "app/risk/dynamic_threshold.py + .env.data_collection",
        "Deleted with the calculator.",
    ),
    "DYNAMIC_THRESHOLD_MIN_SAMPLES": (
        DELETED,
        "app/risk/dynamic_threshold.py + .env.data_collection",
        "Deleted with the calculator.",
    ),
    "DYNAMIC_THRESHOLD_ENABLED": (
        DELETED,
        "app/risk/dynamic_threshold.py",
        "Deleted with the calculator.",
    ),
    "DYNAMIC_THRESHOLD_WINDOW_SIZE": (
        DELETED,
        "app/risk/dynamic_threshold.py + .env.data_collection",
        "Deleted with the calculator.",
    ),
    "DYNAMIC_THRESHOLD_PERCENTILE": (
        DELETED,
        "app/risk/dynamic_threshold.py + .env.data_collection",
        "Deleted with the calculator.",
    ),
    "consensus_threshold": (
        DELETED,
        "app/strategy/master_ensemble.py + app/decision/decision_engine.py",
        "Deleted, including from the strategy params_schema where it was "
        "advertised as tunable. The constructor default of 0.40 was stored and "
        "never compared against anything. Expert agreement is now one bounded "
        "input to the threshold.",
    ),
    "consensus_required": (
        DELETED,
        "app/decision/decision_engine.py + app/evidence/decision_recorder.py",
        "Deleted with consensus_threshold. consensus_observed survives as "
        "evidence; no consensus requirement is applied by any component.",
    ),
    "confidence_absolute_floor": (
        DELETED,
        "app/runner/effective_policy.py",
        "Deleted from EffectiveBotPolicy. It was "
        "max(MIN_CONFIDENCE_THRESHOLD, ENSEMBLE_MIN_THRESHOLD_FLOOR) and was the "
        "value the runner applied at runner.py:3883. Replaced by "
        "threshold_policy_hash: the bot policy records which threshold policy "
        "governed the run instead of duplicating its number.",
    ),
    "runner.min_confidence_gate_max": (
        DELETED,
        "app/runner/runner.py:3883",
        "The max(adaptive_gate, context.min_confidence) that saturated the whole "
        "chain. Deleted. The runner neither computes nor raises a threshold, and "
        "its log lines now print the engine's own value or 'not-evaluated'.",
    ),
    "AdaptiveState.min_confidence_gate": (
        DELETED,
        "app/adaptive/engine.py",
        "Deleted. It was base_threshold + confidence_gate_modifier and was the "
        "last value outside the threshold engine that still looked like an entry "
        "threshold. The get_adaptive_state(base_threshold=...) parameter is gone "
        "with it.",
    ),
    "AdaptiveState.confidence_gate_modifier": (
        RETAINED_NON_THRESHOLD,
        "app/adaptive/engine.py",
        "Renamed to caution_modifier and re-domained. Same arithmetic, but it is "
        "no longer expressed as threshold units and its only consumer is the "
        "aggressiveness score, which governs SIZE and LEVERAGE. "
        "ConfidenceGatePolicy became LossStreakCautionPolicy, and the "
        "'threshold_adjustment' alias was deleted.",
    ),
    "BotContext.min_confidence": (
        DELETED,
        "app/runner/bot_context.py",
        "Deleted. The name was ambiguous and its only purpose was the previous "
        "threshold design.",
    ),
    "PolicyEngine.min_confidence": (
        DELETED,
        "app/policy/policy_engine.py",
        "Deleted, along with its LOW_CONFIDENCE gate. PolicyEngine keeps budget, "
        "exposure, margin, R:R and circuit-breaker responsibilities.",
    ),
    "SafetyEngine.min_confidence_soft/hard": (
        DELETED,
        "app/risk/safety_engine.py",
        "Deleted, along with Gate 3 -- a complete duplicate entry-quality gate "
        "that resolved its own threshold from the dynamic calculator, capped it "
        "at its own floor and returned BlockReason.LOW_CONFIDENCE. Safety keeps "
        "loss, drawdown, kill switch, capital, exposure, position count, "
        "leverage, stale data and market conditions.",
    ),
    "SystemLimits.min_strategy_confidence": (
        DELETED,
        "app/risk/system_limits.py + app/core/trading_orchestrator.py",
        "Deleted. It fed the two gates above.",
    ),
    "ENSEMBLE_BLOCKED_REGIMES": (
        RETAINED_NON_THRESHOLD,
        "app/core/config.py",
        "Still a hard regime gate, deliberately outside the threshold engine. "
        "Hard gates are never expressed as an unreachable threshold.",
    ),
}


def legacy_inventory() -> list[dict[str, str]]:
    """The disposition record, in a form the API and the tests both consume."""
    return [
        {
            "setting": name,
            "disposition": disposition,
            "location": location,
            "note": note,
        }
        for name, (disposition, location, note) in sorted(LEGACY_SETTINGS.items())
    ]


# -- Startup report -----------------------------------------------------------


def startup_report(policy: Any, settings: Any = None) -> str:
    """One resolved threshold configuration, as a printable block."""
    summary = policy.summary()
    lines = [
        "[THRESHOLD_ENGINE] resolved configuration",
        f"  threshold_engine = {summary['threshold_engine']}",
        f"  mode             = {summary['mode']}",
        f"  policy_version   = {summary['policy_version']}",
        f"  policy_hash      = {summary['policy_hash']}",
        f"  base             = {summary['base']}",
        f"  band             = [{summary['min']}, {summary['max']}]",
        f"  adjustment_bounds= {summary['adjustment_bounds']}",
        f"  calibration      = {summary['calibration']}",
        f"  smoothing        = {summary['smoothing']}",
        f"  hard_block       = {summary['hard_block_regimes']}",
        f"  source_scopes    = {summary['source_scopes']}",
        "  active final threshold authorities = 1 (AdaptiveEntryThresholdEngine)",
        "  legacy threshold configuration     = none (deleted; rejected at startup)",
    ]
    return "\n".join(lines)


def log_startup_report(policy: Any, settings: Any = None) -> None:
    for line in startup_report(policy, settings).splitlines():
        logger.info(line)


__all__ = [
    "DELETED",
    "DISPOSITIONS",
    "LEGACY_SETTINGS",
    "MIGRATED_THEN_DELETED",
    "RETAINED_NON_THRESHOLD",
    "ThresholdMode",
    "legacy_inventory",
    "log_startup_report",
    "startup_report",
]
