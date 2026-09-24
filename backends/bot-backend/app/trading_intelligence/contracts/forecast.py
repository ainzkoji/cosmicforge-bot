"""Forecast contracts (Section 12) -- historical outcome labels, distribution
shift assessment, and the OutcomeForecast a cohort lookup produces.

Everything here is a RESEARCH LABEL or a downstream statistic of research
labels. Live CATI (Sections 9-11) never reads future price data; only the
labeling code in ``forecast/labels.py`` is allowed to look forward from a
decision point, and only when explicitly building a historical analog row
(P1: "Historical labels may use future paths ONLY inside explicitly
separated research-label generation").
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.versions import OOD_SCHEMA_VERSION, OUTCOME_FORECAST_SCHEMA_VERSION


class TerminalOutcome(str, Enum):
    TARGET_BEFORE_STOP = "TARGET_BEFORE_STOP"
    STOP_BEFORE_TARGET = "STOP_BEFORE_TARGET"
    TIMEOUT = "TIMEOUT"


class LabelQuality(str, Enum):
    VALID = "VALID"
    CENSORED = "CENSORED"  # horizon not fully observed (insufficient future data)
    INVALID = "INVALID"  # e.g. initial_R <= 0, or zero observable future rows


class ForecastStatus(str, Enum):
    VALID = "VALID"
    INSUFFICIENT_SUPPORT = "INSUFFICIENT_SUPPORT"
    OUTCOME_LIBRARY_UNAVAILABLE = "OUTCOME_LIBRARY_UNAVAILABLE"
    INVALID_INPUT = "INVALID_INPUT"


class CalibrationStatus(str, Enum):
    """Section 12.A12: whether a library's probabilities have been VALIDATED.
    A newly built library is never CALIBRATED just because it exists."""

    UNCALIBRATED = "UNCALIBRATED"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    CALIBRATED = "CALIBRATED"


class ShiftSeverity(str, Enum):
    NONE = "NONE"
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"


class ForecastReasonCode(str, Enum):
    SAME_BAR_CONSERVATIVE_STOP_ASSUMED = "SAME_BAR_CONSERVATIVE_STOP_ASSUMED"
    CENSORED_HORIZON = "CENSORED_HORIZON"
    ZERO_INITIAL_RISK = "ZERO_INITIAL_RISK"
    NO_OBSERVABLE_FUTURE = "NO_OBSERVABLE_FUTURE"
    COST_ASSUMPTION_MODELED = "COST_ASSUMPTION_MODELED"
    INSUFFICIENT_SUPPORT = "INSUFFICIENT_SUPPORT"
    INSUFFICIENT_ESS = "INSUFFICIENT_ESS"
    OUTCOME_LIBRARY_UNAVAILABLE = "OUTCOME_LIBRARY_UNAVAILABLE"
    UNSEEN_CATEGORY = "UNSEEN_CATEGORY"
    CAPABILITY_SHIFT = "CAPABILITY_SHIFT"
    SUPPORT_SHIFT = "SUPPORT_SHIFT"
    HIGH_OOD = "HIGH_OOD"
    DEEP_BACKOFF = "DEEP_BACKOFF"
    MARKET_STATE_INVALID = "MARKET_STATE_INVALID"


@dataclass(frozen=True)
class SetupOutcomeLabel:
    """One historical analog row (Section 12.2). Recordable for a candidate
    regardless of whether it was ever traded -- selection bias from learning
    only taken trades is exactly what Section 12.1 forbids."""

    label_id: str

    setup_candidate_id: str
    market_state_id: str
    instrument_key: InstrumentKey
    decision_time: int

    setup_family: str
    setup_version: str
    setup_policy_hash: str

    market_state_schema_version: str
    regime_model_version: str
    label_policy_version: str
    cost_model_version: str

    terminal_outcome: str  # TerminalOutcome
    net_profitable: bool

    gross_R: float
    net_R: float
    mfe_R: float
    mae_R: float

    time_to_target_bars: Optional[int]
    time_to_stop_bars: Optional[int]
    terminal_horizon_bars: int

    fee_R: float
    spread_R: float
    slippage_R: float
    funding_R: float
    carry_R: float
    total_cost_R: float

    label_quality: str
    reason_codes: Tuple[str, ...] = ()

    @staticmethod
    def build_id(*, setup_candidate_id: str, label_policy_version: str, cost_model_version: str) -> str:
        return short_id(
            "lbl", {
                "setup_candidate_id": setup_candidate_id,
                "label_policy_version": label_policy_version,
                "cost_model_version": cost_model_version,
            },
        )


@dataclass(frozen=True)
class DistributionShiftAssessment:
    """Section 12.13 -- every source visible, never collapsed to one
    unexplained scalar (``ood_score`` summarizes, it does not replace, the
    per-source breakdown below)."""

    ood_score: float
    continuous_feature_scores: Mapping[str, float] = field(default_factory=dict)
    unseen_categories: Tuple[str, ...] = ()
    capability_shift_flags: Tuple[str, ...] = ()
    support_shift: bool = False
    severity: str = ShiftSeverity.NONE.value
    reason_codes: Tuple[str, ...] = ()
    schema_version: str = OOD_SCHEMA_VERSION


@dataclass(frozen=True)
class OutcomeForecast:
    """Section 12.12 -- immutable, versioned. Contains no user capital, no
    account/bot identity (P3): everything here is a function of a
    SetupCandidate and a HistoricalOutcomeLibrary, both market-only."""

    forecast_id: str

    setup_candidate_id: str
    market_state_id: str

    forecast_version: str
    library_version: str
    library_hash: str

    cohort_signature: str
    backoff_level: int

    raw_support: int
    ess: float

    p_net_profitable_mean: float
    credible_interval_low: float
    credible_interval_high: float
    credible_interval_level: float

    p_target_before_stop: float
    p_stop_before_target: float
    p_timeout: float

    gross_R_mean: Optional[float] = None
    gross_R_median: Optional[float] = None
    gross_R_lower_quantile: Optional[float] = None
    gross_R_upper_quantile: Optional[float] = None

    net_R_reference_mean: Optional[float] = None
    net_R_reference_median: Optional[float] = None

    e_r_given_target: Optional[float] = None
    e_r_given_stop: Optional[float] = None
    e_r_given_timeout: Optional[float] = None

    mfe_R_quantiles: Mapping[str, float] = field(default_factory=dict)
    mae_R_quantiles: Mapping[str, float] = field(default_factory=dict)
    time_to_target_quantiles: Mapping[str, float] = field(default_factory=dict)
    time_to_stop_quantiles: Mapping[str, float] = field(default_factory=dict)

    distribution_shift_assessment: Optional[DistributionShiftAssessment] = None
    forecast_uncertainty: float = 1.0
    forecast_uncertainty_components: Mapping[str, float] = field(default_factory=dict)

    status: str = ForecastStatus.VALID.value
    reason_codes: Tuple[str, ...] = ()
    #: Copied from the library the forecast was drawn from (Section 14.7).
    calibration_status: str = CalibrationStatus.UNCALIBRATED.value
    schema_version: str = OUTCOME_FORECAST_SCHEMA_VERSION

    @property
    def is_usable(self) -> bool:
        return self.status == ForecastStatus.VALID.value

    @staticmethod
    def build_id(*, setup_candidate_id: str, library_hash: str, forecast_version: str, cohort_signature: str) -> str:
        return short_id(
            "fcst", {
                "setup_candidate_id": setup_candidate_id,
                "library_hash": library_hash,
                "forecast_version": forecast_version,
                "cohort_signature": cohort_signature,
            },
        )


__all__ = [
    "TerminalOutcome",
    "LabelQuality",
    "ForecastStatus",
    "CalibrationStatus",
    "ShiftSeverity",
    "ForecastReasonCode",
    "SetupOutcomeLabel",
    "DistributionShiftAssessment",
    "OutcomeForecast",
]
