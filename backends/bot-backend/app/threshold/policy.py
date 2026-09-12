"""``EffectiveThresholdPolicy`` -- the one place threshold configuration lives.

The stack this replaces had four ranges (dynamic min/max, an ensemble floor, an
absolute floor, and a runner-level ``max()``) applied in series by components
that did not know about each other. The observable result was that
``MIN_CONFIDENCE_THRESHOLD = 0.70`` sat above the dynamic system's hard cap of
0.65, so no dynamic value could ever survive and two documented, operator-tuned
settings had no effect at all. Every one of those controls has since been
deleted; this is the only threshold policy in the system.

So this module enforces one rule above all others: **there is exactly one
band**. ``policy_min_threshold`` and ``policy_max_threshold`` are the only
bounds in the system, and :func:`validate_policy` refuses to start if any
configured value would recreate a hidden second floor.

Precedence, most general first::

    GLOBAL -> ASSET_CLASS -> VENUE -> SYMBOL -> BOT

A more specific scope may narrow the band, tighten adjustment bounds or change
calibration behaviour. It may not introduce a separate authority: every scope
configures the same fields of the same engine.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Sequence

from app.threshold.contracts import ThresholdMode

#: Bumped whenever the *meaning* of a policy field changes, so a stored
#: policy_hash cannot be silently reinterpreted.
#:
#: 1.1.0 -- base, band and every magnitude recalibrated to the confidence scale
#: the ensemble actually produces; agreement measured as breadth among the
#: eligible experts. 1.0.0 decisions stay on record under their own version and
#: hash, and a 1.0.0 adaptive state is never carried into a 1.1.0 epoch.
POLICY_VERSION = "1.1.0"

SCOPE_ORDER = ("GLOBAL", "ASSET_CLASS", "VENUE", "SYMBOL", "BOT")


class ThresholdPolicyError(ValueError):
    """Raised when configuration is internally contradictory.

    This is deliberately fatal. A threshold policy that cannot be satisfied is
    exactly how the previous stack went inert without anyone noticing.
    """

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"[{code}] {message}")


#: Canonical defaults. Every one of these is a *policy* value, not an answer.
#: Which entry bar is correct is a question for the sensitivity study, not for
#: this module -- so ``base_threshold`` has no default at all.
DEFAULTS: dict[str, Any] = {
    "mode": ThresholdMode.ADAPTIVE,
    # Supplied by THRESHOLD_BASE. Left None here so that nothing in this file
    # can be mistaken for a chosen production value: the engine never invents a
    # base, and resolution fails if configuration does not supply one.
    "base_threshold": None,
    "static_threshold": None,
    # The single band, read off the empirical opportunity-confidence
    # distribution (4,644 organic opportunities in production-parity replay;
    # docs/adaptive_threshold_recalibration_report.md):
    #   min 0.25 -- the top of the empty interval between the lone-sma_cross
    #               cluster (0.195) and every other opportunity (>= 0.25). The
    #               most permissive state still rejects the weakest expert alone.
    #   max 0.60 -- the median of genuine multi-expert consensus (P50 0.593,
    #               P60 0.602). The strictest state stays attainable by it.
    "min_threshold": 0.25,
    "max_threshold": 0.60,
    # Bounded contributions. Each is a maximum absolute magnitude.
    #
    # The 1.0.0 magnitudes were sized against a 0.70 base. Scaled together by
    # k = (P90 confidence - base) / 0.28 = (0.40 - 0.30) / 0.28, every ordinary
    # market penalty at once lifts the bar to the empirical P90 and no further,
    # and with both calibration terms added it stays below P95 (0.484). Their
    # proportions are unchanged: the replay gave no evidence that would justify
    # re-weighting one term against another.
    "regime_bound": 0.021,
    "volatility_bound": 0.018,
    "agreement_bound": 0.029,
    "htf_bound": 0.018,
    "market_quality_bound": 0.014,
    "performance_bound": 0.018,
    "distribution_bound": 0.018,
    # Slow calibration.
    "performance_min_samples": 30,
    "performance_lookback": 100,
    "distribution_min_samples": 40,
    "distribution_window": 200,
    "distribution_target_percentile": 0.60,
    # Smoothing / hysteresis. Tightening faster than loosening is the
    # conservative asymmetry: the engine may become strict quickly and must
    # become permissive slowly.
    "smoothing_alpha": 0.35,
    # Scaled by the same k: the same fraction of the adjustment range per
    # evaluated candle as the 1.0.0 design.
    "max_step_up": 0.018,
    "max_step_down": 0.011,
    # Regimes the policy refuses to evaluate at all. These are hard gates and
    # are never expressed as an unreachable threshold.
    "hard_block_regimes": ("LOW_VOLATILITY_CHOP",),
}

_ADJUSTMENT_BOUNDS = (
    "regime_bound",
    "volatility_bound",
    "agreement_bound",
    "htf_bound",
    "market_quality_bound",
    "performance_bound",
    "distribution_bound",
)


@dataclass(frozen=True)
class EffectiveThresholdPolicy:
    """One fully resolved policy. Immutable, hashed, and self-describing."""

    mode: str
    base_threshold: float
    min_threshold: float
    max_threshold: float

    static_threshold: float | None = None

    regime_bound: float = DEFAULTS["regime_bound"]
    volatility_bound: float = DEFAULTS["volatility_bound"]
    agreement_bound: float = DEFAULTS["agreement_bound"]
    htf_bound: float = DEFAULTS["htf_bound"]
    market_quality_bound: float = DEFAULTS["market_quality_bound"]
    performance_bound: float = DEFAULTS["performance_bound"]
    distribution_bound: float = DEFAULTS["distribution_bound"]

    performance_min_samples: int = DEFAULTS["performance_min_samples"]
    performance_lookback: int = DEFAULTS["performance_lookback"]
    distribution_min_samples: int = DEFAULTS["distribution_min_samples"]
    distribution_window: int = DEFAULTS["distribution_window"]
    distribution_target_percentile: float = DEFAULTS["distribution_target_percentile"]

    smoothing_alpha: float = DEFAULTS["smoothing_alpha"]
    max_step_up: float = DEFAULTS["max_step_up"]
    max_step_down: float = DEFAULTS["max_step_down"]

    hard_block_regimes: tuple[str, ...] = tuple(DEFAULTS["hard_block_regimes"])

    policy_version: str = POLICY_VERSION
    source_scopes: tuple[str, ...] = ("GLOBAL",)

    # -- Identity ---------------------------------------------------------

    def hashable_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        # source_scopes describes where the values came from, not what they
        # are. Two identical policies assembled from different scopes must
        # hash the same, or every scope change would look like a policy change.
        payload.pop("source_scopes", None)
        return payload

    @property
    def policy_hash(self) -> str:
        blob = json.dumps(self.hashable_payload(), sort_keys=True, default=str)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def adjustment_bound(self, name: str) -> float:
        return float(getattr(self, f"{name}_bound"))

    @property
    def total_adjustment_bound(self) -> float:
        return sum(float(getattr(self, b)) for b in _ADJUSTMENT_BOUNDS)

    def is_hard_blocked(self, regime: str | None) -> bool:
        if not regime:
            return False
        return str(regime).upper() in {r.upper() for r in self.hard_block_regimes}

    def summary(self) -> dict[str, Any]:
        """One-screen operator view. Printed at startup by the migration."""
        return {
            "threshold_engine": "AdaptiveEntryThresholdEngine",
            "mode": self.mode,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "base": self.base_threshold,
            "min": self.min_threshold,
            "max": self.max_threshold,
            "static_threshold": self.static_threshold,
            "adjustment_bounds": {
                b.replace("_bound", ""): float(getattr(self, b)) for b in _ADJUSTMENT_BOUNDS
            },
            "total_adjustment_bound": round(self.total_adjustment_bound, 6),
            "calibration": {
                "performance_min_samples": self.performance_min_samples,
                "performance_lookback": self.performance_lookback,
                "distribution_min_samples": self.distribution_min_samples,
                "distribution_window": self.distribution_window,
                "distribution_target_percentile": self.distribution_target_percentile,
            },
            "smoothing": {
                "alpha": self.smoothing_alpha,
                "max_step_up": self.max_step_up,
                "max_step_down": self.max_step_down,
            },
            "hard_block_regimes": list(self.hard_block_regimes),
            "source_scopes": list(self.source_scopes),
        }


# -- Validation ---------------------------------------------------------------


def validate_policy(policy: EffectiveThresholdPolicy) -> None:
    """Raise :class:`ThresholdPolicyError` if the policy cannot be satisfied.

    Every check here corresponds to a way the previous stack could go wrong
    silently. A policy that fails validation stops the process rather than
    degrading to a default, because a default is what hid the last one.
    """
    if policy.mode not in ThresholdMode.ALL:
        raise ThresholdPolicyError("UNKNOWN_THRESHOLD_MODE", f"mode {policy.mode!r} is not a mode")
    if policy.mode not in ThresholdMode.IMPLEMENTED:
        raise ThresholdPolicyError(
            "THRESHOLD_MODE_NOT_IMPLEMENTED",
            f"mode {policy.mode} is reserved and disabled; it must not be selected",
        )

    for name in ("base_threshold", "min_threshold", "max_threshold"):
        value = getattr(policy, name)
        if value is None or not _finite(value):
            raise ThresholdPolicyError("THRESHOLD_NOT_FINITE", f"{name} is {value!r}")
        if not 0.0 < float(value) <= 1.0:
            raise ThresholdPolicyError(
                "THRESHOLD_OUT_OF_UNIT_RANGE", f"{name}={value} must lie in (0, 1]"
            )

    if policy.min_threshold > policy.max_threshold:
        raise ThresholdPolicyError(
            "THRESHOLD_MIN_ABOVE_MAX",
            f"min_threshold={policy.min_threshold} > max_threshold={policy.max_threshold}",
        )
    if not policy.min_threshold <= policy.base_threshold <= policy.max_threshold:
        raise ThresholdPolicyError(
            "THRESHOLD_BASE_OUTSIDE_BAND",
            f"base_threshold={policy.base_threshold} is outside "
            f"[{policy.min_threshold}, {policy.max_threshold}]",
        )

    if policy.mode == ThresholdMode.STATIC:
        if policy.static_threshold is None:
            raise ThresholdPolicyError(
                "STATIC_THRESHOLD_MISSING", "mode STATIC requires static_threshold"
            )
        if not policy.min_threshold <= policy.static_threshold <= policy.max_threshold:
            raise ThresholdPolicyError(
                "STATIC_THRESHOLD_OUTSIDE_BAND",
                f"static_threshold={policy.static_threshold} is outside "
                f"[{policy.min_threshold}, {policy.max_threshold}]",
            )

    for bound in _ADJUSTMENT_BOUNDS:
        value = float(getattr(policy, bound))
        if not _finite(value) or value < 0.0:
            raise ThresholdPolicyError("ADJUSTMENT_BOUND_INVALID", f"{bound}={value} must be >= 0")
        if value > 1.0:
            raise ThresholdPolicyError("ADJUSTMENT_BOUND_INVALID", f"{bound}={value} exceeds 1.0")

    if not 0.0 < policy.smoothing_alpha <= 1.0:
        raise ThresholdPolicyError(
            "SMOOTHING_ALPHA_INVALID",
            f"smoothing_alpha={policy.smoothing_alpha} must lie in (0, 1]",
        )
    for name in ("max_step_up", "max_step_down"):
        value = float(getattr(policy, name))
        if not _finite(value) or value <= 0.0:
            raise ThresholdPolicyError("RATE_LIMIT_INVALID", f"{name}={value} must be > 0")

    for name in (
        "performance_min_samples",
        "performance_lookback",
        "distribution_min_samples",
        "distribution_window",
    ):
        value = int(getattr(policy, name))
        if value < 0:
            raise ThresholdPolicyError("CALIBRATION_SAMPLE_INVALID", f"{name}={value} must be >= 0")
    if not 0.0 <= policy.distribution_target_percentile <= 1.0:
        raise ThresholdPolicyError(
            "DISTRIBUTION_PERCENTILE_INVALID",
            f"distribution_target_percentile={policy.distribution_target_percentile} "
            "must lie in [0, 1]",
        )

    # The band must leave the engine somewhere to go. A band narrower than one
    # rate-limited step is a static threshold wearing an adaptive label, and
    # that is precisely the 0.70 failure in a new costume.
    span = float(policy.max_threshold) - float(policy.min_threshold)
    if span > 0 and policy.total_adjustment_bound <= 0.0:
        raise ThresholdPolicyError(
            "THRESHOLD_ADJUSTMENTS_ALL_ZERO",
            "every adjustment bound is 0, so ADAPTIVE mode cannot adapt; "
            "select mode STATIC if that is intended",
        )


def _finite(value: Any) -> bool:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return f == f and f not in (float("inf"), float("-inf"))


# -- Resolution ---------------------------------------------------------------

_FIELD_NAMES = tuple(
    f
    for f in EffectiveThresholdPolicy.__dataclass_fields__  # type: ignore[attr-defined]
    if f not in {"policy_version", "source_scopes"}
)

_INT_FIELDS = frozenset(
    {
        "performance_min_samples",
        "performance_lookback",
        "distribution_min_samples",
        "distribution_window",
    }
)


def resolve_threshold_policy(
    *,
    scopes: Sequence[tuple[str, Mapping[str, Any]]] = (),
    base_threshold: float | None = None,
) -> EffectiveThresholdPolicy:
    """Merge scoped overrides, most general first, into one policy.

    ``scopes`` is an ordered sequence of ``(scope_name, overrides)``. Unknown
    keys raise rather than being ignored -- a typo in a scope override is a
    configuration error, and ignoring it is how a setting becomes inert.
    """
    resolved: dict[str, Any] = {
        k: v for k, v in DEFAULTS.items() if k in _FIELD_NAMES
    }
    if base_threshold is not None:
        resolved["base_threshold"] = float(base_threshold)

    applied: list[str] = []
    for scope_name, overrides in scopes:
        scope = str(scope_name).upper()
        if scope not in SCOPE_ORDER:
            raise ThresholdPolicyError(
                "UNKNOWN_POLICY_SCOPE",
                f"scope {scope_name!r} is not one of {list(SCOPE_ORDER)}",
            )
        touched = False
        for key, value in (overrides or {}).items():
            if key not in _FIELD_NAMES:
                raise ThresholdPolicyError(
                    "UNKNOWN_POLICY_FIELD",
                    f"scope {scope} sets unknown threshold field {key!r}",
                )
            if value is None:
                continue
            resolved[key] = value
            touched = True
        if touched:
            applied.append(scope)

    if resolved.get("base_threshold") is None:
        raise ThresholdPolicyError(
            "THRESHOLD_BASE_UNRESOLVED",
            "base_threshold was not supplied by configuration or migration; "
            "the engine will not invent one",
        )

    resolved["hard_block_regimes"] = tuple(
        str(r).upper() for r in (resolved.get("hard_block_regimes") or ())
    )
    for key in _FIELD_NAMES:
        if key in _INT_FIELDS:
            resolved[key] = int(resolved[key])
        elif key in {"mode", "hard_block_regimes"}:
            continue
        elif resolved[key] is not None:
            resolved[key] = float(resolved[key])
    resolved["mode"] = str(resolved["mode"]).upper()

    policy = EffectiveThresholdPolicy(
        **resolved,
        policy_version=POLICY_VERSION,
        source_scopes=tuple(applied or ["GLOBAL"]),
    )
    validate_policy(policy)
    return policy


#: settings attribute -> policy field. This is the entire threshold
#: configuration surface. There is no alias, no fallback and no legacy key: a
#: deleted setting is rejected at startup by
#: ``app.core.config.detect_legacy_threshold_keys``, never quietly read.
SETTING_MAP: dict[str, str] = {
    "THRESHOLD_ENGINE_MODE": "mode",
    "THRESHOLD_BASE": "base_threshold",
    "THRESHOLD_STATIC": "static_threshold",
    "THRESHOLD_MIN": "min_threshold",
    "THRESHOLD_MAX": "max_threshold",
    "THRESHOLD_REGIME_ADJUSTMENT_MAX": "regime_bound",
    "THRESHOLD_VOLATILITY_ADJUSTMENT_MAX": "volatility_bound",
    "THRESHOLD_AGREEMENT_ADJUSTMENT_MAX": "agreement_bound",
    "THRESHOLD_HTF_ADJUSTMENT_MAX": "htf_bound",
    "THRESHOLD_MARKET_QUALITY_ADJUSTMENT_MAX": "market_quality_bound",
    "THRESHOLD_PERFORMANCE_ADJUSTMENT_MAX": "performance_bound",
    "THRESHOLD_DISTRIBUTION_ADJUSTMENT_MAX": "distribution_bound",
    "THRESHOLD_PERFORMANCE_MIN_SAMPLES": "performance_min_samples",
    "THRESHOLD_PERFORMANCE_LOOKBACK": "performance_lookback",
    "THRESHOLD_DISTRIBUTION_MIN_SAMPLES": "distribution_min_samples",
    "THRESHOLD_DISTRIBUTION_WINDOW": "distribution_window",
    "THRESHOLD_DISTRIBUTION_PERCENTILE": "distribution_target_percentile",
    "THRESHOLD_SMOOTHING_ALPHA": "smoothing_alpha",
    "THRESHOLD_MAX_STEP_UP": "max_step_up",
    "THRESHOLD_MAX_STEP_DOWN": "max_step_down",
    "THRESHOLD_HARD_BLOCK_REGIMES": "hard_block_regimes",
}

#: Numeric settings for which 0 means "not configured". 0 is not a legitimate
#: value for either.
_UNSET_IS_ZERO = frozenset({"THRESHOLD_BASE", "THRESHOLD_STATIC"})


def global_scope_from_settings(settings: Any) -> tuple[dict[str, Any], float]:
    """Return ``(global_overrides, base_threshold)`` for the GLOBAL scope.

    ``base_threshold`` comes from ``THRESHOLD_BASE`` and from nowhere else. There
    is deliberately no second branch: the previous stack's habit of falling back
    to another setting is what let an obsolete key keep deciding the entry bar.
    """
    overrides: dict[str, Any] = {}
    for setting_name, field_name in SETTING_MAP.items():
        value = getattr(settings, setting_name, None)
        if value is None:
            continue
        if setting_name in _UNSET_IS_ZERO and not float(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if field_name == "hard_block_regimes":
            value = tuple(
                r.strip().upper() for r in str(value).split(",") if r.strip()
            )
        overrides[field_name] = value

    base = overrides.pop("base_threshold", None)
    if base is None:
        raise ThresholdPolicyError(
            "THRESHOLD_BASE_UNRESOLVED",
            "THRESHOLD_BASE is not configured. The engine does not invent an "
            "entry bar, and no legacy setting is consulted.",
        )

    overrides["mode"] = str(overrides.get("mode", DEFAULTS["mode"])).upper()
    return overrides, float(base)


def policy_from_settings(
    settings: Any,
    *,
    symbol: str | None = None,
    venue: str | None = None,
    market_type: str | None = None,
    bot_overrides: Mapping[str, Any] | None = None,
) -> EffectiveThresholdPolicy:
    """Build the effective policy for one (venue, market, symbol, bot)."""
    global_scope, base = global_scope_from_settings(settings)

    scopes: list[tuple[str, Mapping[str, Any]]] = [("GLOBAL", global_scope)]

    table = _parse_scoped_overrides(getattr(settings, "THRESHOLD_SCOPED_OVERRIDES", None))
    if market_type:
        scopes.append(("ASSET_CLASS", table.get("ASSET_CLASS", {}).get(str(market_type).upper(), {})))
    if venue:
        scopes.append(("VENUE", table.get("VENUE", {}).get(str(venue).upper(), {})))
    if symbol:
        scopes.append(("SYMBOL", table.get("SYMBOL", {}).get(str(symbol).upper(), {})))
    if bot_overrides:
        scopes.append(("BOT", bot_overrides))

    return resolve_threshold_policy(scopes=scopes, base_threshold=base)


def _parse_scoped_overrides(raw: Any) -> dict[str, dict[str, dict[str, Any]]]:
    if not raw:
        return {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ThresholdPolicyError(
                "SCOPED_OVERRIDES_MALFORMED",
                f"THRESHOLD_SCOPED_OVERRIDES is not valid JSON: {exc}",
            ) from exc
    if not isinstance(raw, Mapping):
        raise ThresholdPolicyError(
            "SCOPED_OVERRIDES_MALFORMED",
            "THRESHOLD_SCOPED_OVERRIDES must be a mapping of scope -> key -> overrides",
        )
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for scope, entries in raw.items():
        scope_name = str(scope).upper()
        if scope_name not in SCOPE_ORDER:
            raise ThresholdPolicyError(
                "UNKNOWN_POLICY_SCOPE", f"scope {scope!r} is not one of {list(SCOPE_ORDER)}"
            )
        if not isinstance(entries, Mapping):
            raise ThresholdPolicyError(
                "SCOPED_OVERRIDES_MALFORMED", f"scope {scope_name} must map keys to overrides"
            )
        out[scope_name] = {
            str(key).upper(): dict(value) for key, value in entries.items()
        }
    return out
