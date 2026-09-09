"""Configuration migration from the legacy threshold stack.

The old stack is not patched here, it is *retired*. This module does three
things and nothing else:

1. Maps the legacy settings onto :class:`~app.threshold.policy.EffectiveThresholdPolicy`
   fields, so no value is silently reinterpreted.
2. Publishes :data:`LEGACY_SETTINGS` -- an explicit classification of every old
   threshold control, so "is this setting still live?" has a checked answer
   instead of a guess.
3. Prints one resolved threshold configuration at startup.

The rule that produced this file: *a configuration setting that cannot affect
behaviour must not be presented as active*. ``ENSEMBLE_MIN_THRESHOLD_FLOOR`` was
documented as the binding constraint, was deliberately tuned from 0.50 to 0.55
by an operator, and could not affect anything, because it sat below a floor of
0.70 that was applied after it. Nothing in the new engine may be tunable-looking
and dead.
"""
from __future__ import annotations

import logging
from typing import Any

from app.threshold.contracts import ThresholdMode
from app.threshold.policy import DEFAULTS, ThresholdPolicyError

logger = logging.getLogger(__name__)


# -- Classification of every legacy threshold control -------------------------

#: How a legacy setting was disposed of.
REMOVED = "REMOVED"
MIGRATED = "MIGRATED"
RETAINED_AS_NON_AUTHORITY = "RETAINED_AS_NON_AUTHORITY"
LEGACY_COMPATIBILITY = "LEGACY_COMPATIBILITY"
RESEARCH_ONLY = "RESEARCH_ONLY"

DISPOSITIONS = frozenset(
    {REMOVED, MIGRATED, RETAINED_AS_NON_AUTHORITY, LEGACY_COMPATIBILITY, RESEARCH_ONLY}
)

#: name -> (disposition, where it lived, what happened to it)
LEGACY_SETTINGS: dict[str, tuple[str, str, str]] = {
    "MIN_CONFIDENCE_THRESHOLD": (
        MIGRATED,
        "app/core/config.py",
        "Becomes EffectiveThresholdPolicy.base_threshold when THRESHOLD_BASE is "
        "unset. It is no longer a floor applied after the fact, so it can now be "
        "adapted away from within the policy band instead of saturating it.",
    ),
    "ENSEMBLE_MIN_THRESHOLD_FLOOR": (
        REMOVED,
        "app/core/config.py + app/strategy/master_ensemble.py",
        "Removed as an authority. It was dominated by MIN_CONFIDENCE_THRESHOLD "
        "and could never bind; the operator tuning recorded as T-04 "
        "(0.50 -> 0.55) had no effect. The single band is now "
        "THRESHOLD_MIN/THRESHOLD_MAX.",
    ),
    "DYNAMIC_THRESHOLD_MIN": (
        RESEARCH_ONLY,
        "app/risk/dynamic_threshold.py",
        "The dynamic percentile calculator is retained for research and its "
        "rolling record() is still fed, but it no longer resolves the entry "
        "threshold. Its output was discarded by max(<=0.65, 0.70) on every "
        "candle.",
    ),
    "DYNAMIC_THRESHOLD_MAX": (
        RESEARCH_ONLY,
        "app/risk/dynamic_threshold.py",
        "See DYNAMIC_THRESHOLD_MIN.",
    ),
    "DYNAMIC_THRESHOLD_FALLBACK": (
        RESEARCH_ONLY,
        "app/risk/dynamic_threshold.py",
        "See DYNAMIC_THRESHOLD_MIN.",
    ),
    "DYNAMIC_THRESHOLD_MIN_SAMPLES": (
        RESEARCH_ONLY,
        "app/risk/dynamic_threshold.py",
        "See DYNAMIC_THRESHOLD_MIN.",
    ),
    "consensus_threshold": (
        REMOVED,
        "app/strategy/master_ensemble.py + app/decision/decision_engine.py",
        "Removed as an authority. The constructor default of 0.40 was stored "
        "and never compared against anything, because the ensemble passed "
        "consensus_required=0.0. Expert agreement is now one bounded input to "
        "the threshold instead of a second, separate gate.",
    ),
    "consensus_required": (
        REMOVED,
        "app/decision/decision_engine.py",
        "Removed with consensus_threshold. consensus_observed survives as "
        "evidence; consensus_required is written NULL because no requirement "
        "is applied.",
    ),
    "confidence_absolute_floor": (
        REMOVED,
        "app/runner/effective_policy.py",
        "Removed from EffectiveBotPolicy. It was max(MIN_CONFIDENCE_THRESHOLD, "
        "ENSEMBLE_MIN_THRESHOLD_FLOOR) and was the value the runner applied at "
        "runner.py:3883. Replaced by threshold_policy_hash, which records which "
        "threshold policy governed the run rather than duplicating its number.",
    ),
    "runner.min_confidence_gate_max": (
        REMOVED,
        "app/runner/runner.py:3883",
        "The max(adaptive_gate, context.min_confidence) that saturated the "
        "whole chain. Deleted. The runner no longer computes or raises a "
        "threshold.",
    ),
    "adaptive.min_confidence_gate": (
        RETAINED_AS_NON_AUTHORITY,
        "app/adaptive/engine.py",
        "AdaptiveState.min_confidence_gate is still computed and logged as "
        "adaptive-engine observability, but nothing consumes it as a threshold. "
        "The adaptive engine keeps its size and leverage authority.",
    ),
    "BotContext.min_confidence": (
        RETAINED_AS_NON_AUTHORITY,
        "app/runner/bot_context.py",
        "Sourced from the threshold policy band floor and passed only to "
        "PolicyEngine, whose confidence check is already bypassed on the "
        "orchestrated path via confidence_already_approved. Not an entry "
        "authority.",
    ),
    "SafetyEngine.min_confidence_soft/hard": (
        RETAINED_AS_NON_AUTHORITY,
        "app/risk/safety_engine.py",
        "Hard safety backstop for callers outside the orchestrated path. The "
        "orchestrated path sets confidence_already_approved=True so it cannot "
        "re-litigate entry quality.",
    ),
    "ENSEMBLE_BLOCKED_REGIMES": (
        LEGACY_COMPATIBILITY,
        "app/core/config.py",
        "Still a hard regime gate, deliberately outside the threshold engine. "
        "Hard gates are not expressed as an unreachable threshold.",
    ),
}


def legacy_inventory() -> list[dict[str, str]]:
    """The inventory in a form the report and the tests can both consume."""
    return [
        {
            "setting": name,
            "disposition": disposition,
            "location": location,
            "note": note,
        }
        for name, (disposition, location, note) in sorted(LEGACY_SETTINGS.items())
    ]


# -- Settings -> GLOBAL scope -------------------------------------------------

#: settings attribute -> policy field. Only these are read; anything else in
#: settings is not threshold configuration.
_SETTING_MAP: dict[str, str] = {
    "THRESHOLD_ENGINE_MODE": "mode",
    "THRESHOLD_BASE": "base_threshold",
    "THRESHOLD_STATIC": "static_threshold",
    "THRESHOLD_MIN": "min_threshold",
    "THRESHOLD_MAX": "max_threshold",
    "THRESHOLD_REGIME_BOUND": "regime_bound",
    "THRESHOLD_VOLATILITY_BOUND": "volatility_bound",
    "THRESHOLD_AGREEMENT_BOUND": "agreement_bound",
    "THRESHOLD_HTF_BOUND": "htf_bound",
    "THRESHOLD_MARKET_QUALITY_BOUND": "market_quality_bound",
    "THRESHOLD_PERFORMANCE_BOUND": "performance_bound",
    "THRESHOLD_DISTRIBUTION_BOUND": "distribution_bound",
    "THRESHOLD_PERFORMANCE_MIN_SAMPLES": "performance_min_samples",
    "THRESHOLD_PERFORMANCE_LOOKBACK": "performance_lookback",
    "THRESHOLD_DISTRIBUTION_MIN_SAMPLES": "distribution_min_samples",
    "THRESHOLD_DISTRIBUTION_WINDOW": "distribution_window",
    "THRESHOLD_DISTRIBUTION_TARGET_PERCENTILE": "distribution_target_percentile",
    "THRESHOLD_SMOOTHING_ALPHA": "smoothing_alpha",
    "THRESHOLD_MAX_STEP_UP": "max_step_up",
    "THRESHOLD_MAX_STEP_DOWN": "max_step_down",
}

#: A numeric setting left at 0 means "not configured". Used for the optional
#: overrides where 0 is not a legitimate value anyway.
_UNSET_IS_ZERO = frozenset({"THRESHOLD_BASE", "THRESHOLD_STATIC"})


def global_scope_from_settings(settings: Any) -> tuple[dict[str, Any], float]:
    """Return ``(global_overrides, base_threshold)`` for the GLOBAL scope.

    ``base_threshold`` is resolved explicitly, in this order:

    1. ``THRESHOLD_BASE`` when the operator set one, or
    2. the migrated ``MIN_CONFIDENCE_THRESHOLD``.

    There is no third branch. If neither is available the policy resolver
    raises rather than inventing a number -- picking the entry bar is a
    research decision, not a default.
    """
    overrides: dict[str, Any] = {}
    for setting_name, field_name in _SETTING_MAP.items():
        value = getattr(settings, setting_name, None)
        if value is None:
            continue
        if setting_name in _UNSET_IS_ZERO and not float(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        overrides[field_name] = value

    base = overrides.pop("base_threshold", None)
    migrated_from = "THRESHOLD_BASE"
    if base is None:
        legacy = getattr(settings, "MIN_CONFIDENCE_THRESHOLD", None)
        if legacy is None:
            raise ThresholdPolicyError(
                "THRESHOLD_BASE_UNRESOLVED",
                "neither THRESHOLD_BASE nor MIN_CONFIDENCE_THRESHOLD is available",
            )
        base = float(legacy)
        migrated_from = "MIN_CONFIDENCE_THRESHOLD (migrated)"

    # Recorded for the startup report only; it is provenance, not policy, so it
    # must not reach the policy hash.
    _BASE_SOURCE[id(settings)] = migrated_from

    mode = str(overrides.get("mode", DEFAULTS["mode"])).upper()
    overrides["mode"] = mode

    return overrides, float(base)


#: Where the base came from, for the startup report. Keyed by settings identity
#: so a test settings object cannot leak into the production report.
_BASE_SOURCE: dict[int, str] = {}


def base_threshold_source(settings: Any) -> str:
    return _BASE_SOURCE.get(id(settings), "unknown")


# -- Deprecation warnings and the startup report ------------------------------


def deprecation_warnings(settings: Any) -> list[str]:
    """Warn about legacy settings that are still present but no longer bind.

    Presence is not an error -- an operator's ``.env`` should not have to be
    rewritten before the process will start -- but silence would be. Every
    warning names the setting, says it is inert, and says what replaced it.
    """
    out: list[str] = []

    floor = getattr(settings, "ENSEMBLE_MIN_THRESHOLD_FLOOR", None)
    if floor is not None:
        out.append(
            f"ENSEMBLE_MIN_THRESHOLD_FLOOR={floor} is DEPRECATED and no longer "
            "affects the entry threshold. Use THRESHOLD_MIN/THRESHOLD_MAX."
        )

    legacy_min = getattr(settings, "MIN_CONFIDENCE_THRESHOLD", None)
    explicit_base = getattr(settings, "THRESHOLD_BASE", None)
    if legacy_min is not None and explicit_base is not None and float(explicit_base):
        out.append(
            f"MIN_CONFIDENCE_THRESHOLD={legacy_min} is superseded by "
            f"THRESHOLD_BASE={explicit_base} and is ignored."
        )
    elif legacy_min is not None:
        out.append(
            f"MIN_CONFIDENCE_THRESHOLD={legacy_min} is DEPRECATED as a floor. It "
            "has been migrated to THRESHOLD_BASE (the centre of the adaptive "
            "band), not re-applied as a floor after adaptation."
        )

    import os

    for name in (
        "DYNAMIC_THRESHOLD_MIN",
        "DYNAMIC_THRESHOLD_MAX",
        "DYNAMIC_THRESHOLD_FALLBACK",
    ):
        if os.environ.get(name):
            out.append(
                f"{name}={os.environ[name]} is RESEARCH_ONLY. The dynamic "
                "percentile calculator no longer resolves the entry threshold."
            )
    return out


def startup_report(policy: Any, settings: Any = None) -> str:
    """One resolved threshold configuration, as a printable block."""
    summary = policy.summary()
    lines = [
        "[THRESHOLD_ENGINE] resolved configuration",
        f"  threshold_engine = {summary['threshold_engine']}",
        f"  mode             = {summary['mode']}",
        f"  policy_version   = {summary['policy_version']}",
        f"  policy_hash      = {summary['policy_hash']}",
        f"  base             = {summary['base']}"
        + (f"  (from {base_threshold_source(settings)})" if settings is not None else ""),
        f"  band             = [{summary['min']}, {summary['max']}]",
        f"  adjustment_bounds= {summary['adjustment_bounds']}",
        f"  calibration      = {summary['calibration']}",
        f"  smoothing        = {summary['smoothing']}",
        f"  hard_block       = {summary['hard_block_regimes']}",
        f"  source_scopes    = {summary['source_scopes']}",
        "  active final threshold authorities = 1 (AdaptiveEntryThresholdEngine)",
    ]
    if settings is not None:
        for warning in deprecation_warnings(settings):
            lines.append(f"  DEPRECATED: {warning}")
    return "\n".join(lines)


def log_startup_report(policy: Any, settings: Any = None) -> None:
    report = startup_report(policy, settings)
    for line in report.splitlines():
        logger.info(line)


__all__ = [
    "DISPOSITIONS",
    "LEGACY_SETTINGS",
    "LEGACY_COMPATIBILITY",
    "MIGRATED",
    "REMOVED",
    "RESEARCH_ONLY",
    "RETAINED_AS_NON_AUTHORITY",
    "ThresholdMode",
    "base_threshold_source",
    "deprecation_warnings",
    "global_scope_from_settings",
    "legacy_inventory",
    "log_startup_report",
    "startup_report",
]
