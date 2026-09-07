"""
ML Data Readiness Gate -- Phase I.

Defines thresholds and checks that determine whether a training dataset
is clean enough to draw conclusions from offline event/news ML experiments.

This module does NOT train models.  It is called by:
  - scripts/ml/check_event_ml_readiness.py  (CLI audit)
  - scripts/ml/phase_f_experiment.py         (gate before verdict)
  - scripts/ml/phase_h_experiment.py         (gate before verdict)

All checks are pure functions on a pandas DataFrame + optional metadata dict.
No side effects.  No file I/O.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

try:
    from scipy.stats import spearmanr as _spearmanr
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False


# ── Thresholds ────────────────────────────────────────────────────────────────

THRESHOLDS: dict[str, Any] = {
    # Dataset composition
    "min_organic_rows":              1_000,
    "preferred_organic_rows":        5_000,
    "max_backfill_pct":              50.0,    # >50% backfill -> fail
    "min_organic_days":              14,      # at least 2 weeks of organic trades
    "max_single_day_pct":            50.0,    # one day cannot be >50% of rows
    "max_single_session_pct_train":  60.0,    # one burst cannot be >60% of train split

    # Base feature completeness
    "max_sl_distance_null_pct":      10.0,
    "max_planned_rr_null_pct":       10.0,
    "max_sl_atr_ratio_null_pct":     10.0,
    "min_trace_completeness_pct":    95.0,    # trace_id / cycle_id presence
    "max_regime_unknown_pct":        5.0,

    # Event coverage (for timing features to be trustworthy)
    "min_pre_event_rows":            200,
    "min_post_event_rows":           200,
    "min_blackout_rows_if_eval":     10,      # only checked when blackout modeling evaluated
    "min_distinct_high_impact_events": 5,
    "max_single_event_date_dominance_pct": 20.0,

    # Reaction coverage (only checked when reaction features are enabled)
    "min_completed_reactions":       50,
    "min_reaction_type_classes":     3,
    "max_reaction_numeric_null_pct": 20.0,

    # Date-proxy protection
    "max_timing_proxy_spearman":     0.70,    # |spearman(timing_col, row_idx)| threshold
}

# Columns whose null rates are critical base-feature checks
BASE_NULL_CHECKS: dict[str, str] = {
    "sl_distance_pct": "max_sl_distance_null_pct",
    "planned_rr":      "max_planned_rr_null_pct",
    "sl_atr_ratio":    "max_sl_atr_ratio_null_pct",
}


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    name:        str
    passed:      bool
    actual:      Any
    threshold:   Any
    message:     str
    blocking:    bool = True   # False = warning only, does not cause overall FAIL

@dataclass
class ReadinessResult:
    passed:   bool
    checks:   list[CheckResult] = field(default_factory=list)
    warnings: list[str]         = field(default_factory=list)

    # summary counts
    @property
    def n_passed(self) -> int:
        return sum(1 for c in self.checks if c.passed)

    @property
    def n_failed_blocking(self) -> int:
        return sum(1 for c in self.checks if not c.passed and c.blocking)

    @property
    def n_warnings(self) -> int:
        return sum(1 for c in self.checks if not c.passed and not c.blocking)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _null_pct(series: pd.Series) -> float:
    if len(series) == 0:
        return 100.0
    return float(series.isna().mean() * 100.0)


def _check(
    name: str,
    passed: bool,
    actual: Any,
    threshold: Any,
    message: str,
    blocking: bool = True,
) -> CheckResult:
    return CheckResult(
        name=name, passed=passed,
        actual=actual, threshold=threshold,
        message=message, blocking=blocking,
    )


# ── Main readiness function ───────────────────────────────────────────────────

def check_dataset_readiness(
    df: pd.DataFrame,
    *,
    meta:              dict[str, Any] | None = None,
    check_reactions:   bool = False,
    check_blackout:    bool = False,
    thresholds:        dict[str, Any] | None = None,
) -> ReadinessResult:
    """
    Run all readiness checks against a training DataFrame.

    Parameters
    ----------
    df               : the training parquet loaded as a DataFrame
    meta             : the _meta.json dict (optional -- enriches some checks)
    check_reactions  : if True, also run reaction feature readiness checks
    check_blackout   : if True, fail when blackout_active_row_count < threshold
    thresholds       : override any entry in THRESHOLDS (for tests)
    """
    t = {**THRESHOLDS, **(thresholds or {})}
    checks: list[CheckResult] = []
    warnings: list[str] = []

    n = len(df)
    if n == 0:
        return ReadinessResult(
            passed=False,
            checks=[_check("dataset_non_empty", False, 0, 1, "Dataset is empty")],
        )

    # ── 1. Dataset composition ─────────────────────────────────────────────────

    # Identify organic rows: account_id is not 'backfill' (and not null)
    if "account_id" in df.columns:
        backfill_mask = df["account_id"].fillna("").str.lower().str.startswith("backfill")
        organic_mask  = ~backfill_mask
    else:
        organic_mask  = pd.Series(True, index=df.index)
        backfill_mask = pd.Series(False, index=df.index)
        warnings.append("account_id column absent -- cannot distinguish organic vs backfill rows")

    n_organic  = int(organic_mask.sum())
    n_backfill = int(backfill_mask.sum())
    backfill_pct = n_backfill / n * 100.0

    checks.append(_check(
        "organic_row_count",
        n_organic >= t["min_organic_rows"],
        n_organic,
        t["min_organic_rows"],
        f"Organic rows: {n_organic:,} (need >= {t['min_organic_rows']:,})",
    ))
    checks.append(_check(
        "backfill_dominance",
        backfill_pct <= t["max_backfill_pct"],
        round(backfill_pct, 1),
        t["max_backfill_pct"],
        f"Backfill %: {backfill_pct:.1f}% (must be <= {t['max_backfill_pct']}%)",
    ))

    # Time coverage of organic rows
    if n_organic > 0 and "open_timestamp" in df.columns:
        org_ts = pd.to_datetime(df.loc[organic_mask, "open_timestamp"], errors="coerce", utc=True)
        org_ts = org_ts.dropna()
        if len(org_ts) >= 2:
            span_days = (org_ts.max() - org_ts.min()).days
            checks.append(_check(
                "organic_time_coverage_days",
                span_days >= t["min_organic_days"],
                span_days,
                t["min_organic_days"],
                f"Organic date range: {span_days} days (need >= {t['min_organic_days']})",
            ))
        else:
            checks.append(_check(
                "organic_time_coverage_days", False, 0, t["min_organic_days"],
                "Fewer than 2 organic rows with valid timestamps",
            ))
    else:
        warnings.append("open_timestamp column absent or no organic rows -- skipping time coverage check")

    # Single-day dominance (guard against one-burst datasets)
    if "open_timestamp" in df.columns:
        ts_all = pd.to_datetime(df["open_timestamp"], errors="coerce", utc=True)
        date_counts = ts_all.dt.date.value_counts()
        max_day_pct = float(date_counts.iloc[0] / n * 100.0) if len(date_counts) > 0 else 100.0
        max_day     = str(date_counts.index[0]) if len(date_counts) > 0 else "unknown"
        checks.append(_check(
            "single_day_dominance",
            max_day_pct <= t["max_single_day_pct"],
            round(max_day_pct, 1),
            t["max_single_day_pct"],
            f"Max single-day share: {max_day_pct:.1f}% on {max_day} (must be <= {t['max_single_day_pct']}%)",
        ))

    # ── 2. Base feature completeness ───────────────────────────────────────────

    for col, thresh_key in BASE_NULL_CHECKS.items():
        if col in df.columns:
            np_ = _null_pct(df[col])
            checks.append(_check(
                f"{col}_null_rate",
                np_ <= t[thresh_key],
                round(np_, 1),
                t[thresh_key],
                f"{col} null rate: {np_:.1f}% (must be <= {t[thresh_key]}%)",
            ))
        else:
            checks.append(_check(
                f"{col}_null_rate", False, "column_absent", t[thresh_key],
                f"{col} column not found in dataset",
            ))

    # trace_id / cycle_id completeness (metadata columns if present)
    for id_col in ("trace_id", "cycle_id"):
        if id_col in df.columns:
            present_pct = 100.0 - _null_pct(df[id_col])
            empty_pct   = float((df[id_col].fillna("").astype(str).str.strip() == "").mean() * 100.0)
            actual_completeness = present_pct - empty_pct
            checks.append(_check(
                f"{id_col}_completeness",
                actual_completeness >= t["min_trace_completeness_pct"],
                round(actual_completeness, 1),
                t["min_trace_completeness_pct"],
                f"{id_col} completeness: {actual_completeness:.1f}% (need >= {t['min_trace_completeness_pct']}%)",
                blocking=False,   # warning only
            ))

    # regime_state UNKNOWN rate
    if "regime_enc" in df.columns:
        unknown_pct = float((df["regime_enc"] == -1).mean() * 100.0)
        checks.append(_check(
            "regime_unknown_rate",
            unknown_pct <= t["max_regime_unknown_pct"],
            round(unknown_pct, 1),
            t["max_regime_unknown_pct"],
            f"Regime UNKNOWN rate: {unknown_pct:.1f}% (must be <= {t['max_regime_unknown_pct']}%)",
            blocking=False,
        ))

    # ── 3. Event feature coverage ──────────────────────────────────────────────

    # Pre-event rows
    pre_rows = 0
    if "minutes_to_next_event" in df.columns:
        mtn = pd.to_numeric(df["minutes_to_next_event"], errors="coerce")
        pre_rows = int(((mtn >= 0) & (mtn <= 60)).sum())
    checks.append(_check(
        "pre_event_row_count",
        pre_rows >= t["min_pre_event_rows"],
        pre_rows,
        t["min_pre_event_rows"],
        f"pre_event_60min rows: {pre_rows:,} (need >= {t['min_pre_event_rows']:,})",
    ))

    # Post-event rows
    post_rows = 0
    if "minutes_since_last_event" in df.columns:
        msl = pd.to_numeric(df["minutes_since_last_event"], errors="coerce")
        post_rows = int(((msl >= 0) & (msl <= 60)).sum())
    checks.append(_check(
        "post_event_row_count",
        post_rows >= t["min_post_event_rows"],
        post_rows,
        t["min_post_event_rows"],
        f"post_event_60min rows: {post_rows:,} (need >= {t['min_post_event_rows']:,})",
    ))

    # Blackout rows (only blocking when check_blackout=True)
    blackout_rows = 0
    if "is_blackout_active" in df.columns:
        blackout_rows = int((df["is_blackout_active"].fillna(0).astype(int) == 1).sum())
    if check_blackout:
        checks.append(_check(
            "blackout_row_count",
            blackout_rows >= t["min_blackout_rows_if_eval"],
            blackout_rows,
            t["min_blackout_rows_if_eval"],
            f"blackout_active rows: {blackout_rows:,} (need >= {t['min_blackout_rows_if_eval']:,})",
        ))
    else:
        # Warning only when not evaluating blackout modeling
        if blackout_rows == 0:
            warnings.append("blackout_active_row_count=0 -- blackout modeling cannot be evaluated")

    # Distinct high-impact events from metadata or timing diversity
    n_distinct_events: int | None = None
    if meta and "historical_event_count" in meta:
        n_distinct_events = int(meta.get("historical_event_count", 0))
    if n_distinct_events is not None:
        checks.append(_check(
            "distinct_high_impact_events",
            n_distinct_events >= t["min_distinct_high_impact_events"],
            n_distinct_events,
            t["min_distinct_high_impact_events"],
            f"Distinct high-impact events in DB: {n_distinct_events} (need >= {t['min_distinct_high_impact_events']})",
        ))

    # Single-event-date dominance in event windows
    if "open_timestamp" in df.columns and "minutes_to_next_event" in df.columns:
        mtn2 = pd.to_numeric(df["minutes_to_next_event"], errors="coerce")
        event_window_mask = ((mtn2 >= 0) & (mtn2 <= 60))
        if event_window_mask.sum() >= 10:
            ts_ew = pd.to_datetime(df.loc[event_window_mask, "open_timestamp"], errors="coerce", utc=True)
            date_counts_ew = ts_ew.dt.date.value_counts()
            max_ew_pct = float(date_counts_ew.iloc[0] / event_window_mask.sum() * 100.0)
            checks.append(_check(
                "event_window_date_dominance",
                max_ew_pct <= t["max_single_event_date_dominance_pct"],
                round(max_ew_pct, 1),
                t["max_single_event_date_dominance_pct"],
                f"Max event-window date share: {max_ew_pct:.1f}% (must be <= {t['max_single_event_date_dominance_pct']}%)",
            ))

    # ── 4. Reaction feature coverage (conditional) ─────────────────────────────

    if check_reactions:
        reaction_numeric_cols = ["volatility_post_event", "volume_spike_post_event", "max_adverse_move"]
        for col in reaction_numeric_cols:
            if col in df.columns:
                np_ = _null_pct(df[col])
                checks.append(_check(
                    f"{col}_null_rate",
                    np_ <= t["max_reaction_numeric_null_pct"],
                    round(np_, 1),
                    t["max_reaction_numeric_null_pct"],
                    f"{col} null rate: {np_:.1f}% (must be <= {t['max_reaction_numeric_null_pct']}%)",
                ))
            else:
                checks.append(_check(
                    f"{col}_null_rate", False, "column_absent",
                    t["max_reaction_numeric_null_pct"],
                    f"{col} column not present -- reaction features not in dataset",
                ))

        if "reaction_type" in df.columns:
            n_classes = int(df["reaction_type"].nunique())
            checks.append(_check(
                "reaction_type_diversity",
                n_classes >= t["min_reaction_type_classes"],
                n_classes,
                t["min_reaction_type_classes"],
                f"reaction_type unique classes: {n_classes} (need >= {t['min_reaction_type_classes']})",
            ))

    # ── 5. Safety checks (news/sentiment absent) ───────────────────────────────

    _NEWS_FORBIDDEN = {
        "narrative_effectiveness_score", "sentiment_accuracy_score",
        "false_signal_ratio", "validated_news_impact_score",
        "news_impact_score", "title", "headline", "body_snippet",
        "raw_text", "news_text", "sentiment_compound", "sentiment_label",
    }
    forbidden_found = _NEWS_FORBIDDEN & set(df.columns)
    checks.append(_check(
        "news_features_absent",
        len(forbidden_found) == 0,
        sorted(forbidden_found) or "none",
        "none",
        f"News/sentiment columns in dataset: {sorted(forbidden_found) or 'none'}",
    ))

    if meta and meta.get("deployed"):
        checks.append(_check(
            "not_deployed",
            False, True, False,
            "Metadata marks this dataset/model as deployed -- experiment cannot proceed",
        ))

    # ── 6. Date-proxy risk ─────────────────────────────────────────────────────

    if "minutes_to_next_event" in df.columns and _SCIPY_AVAILABLE:
        valid = pd.to_numeric(df["minutes_to_next_event"], errors="coerce").dropna()
        if len(valid) >= 10:
            import numpy as np
            idx = np.arange(len(valid), dtype=float)
            corr, _ = _spearmanr(idx, valid.values)
            corr = float(corr) if not math.isnan(corr) else 0.0
            proxy_risk = bool(abs(corr) > t["max_timing_proxy_spearman"])
            checks.append(_check(
                "timing_date_proxy_risk",
                not proxy_risk,
                round(abs(corr), 4),
                t["max_timing_proxy_spearman"],
                f"|Spearman(minutes_to_next_event, row_idx)| = {abs(corr):.4f} "
                f"(threshold {t['max_timing_proxy_spearman']})",
            ))
    elif not _SCIPY_AVAILABLE:
        warnings.append("scipy not installed -- date-proxy Spearman check skipped")

    # ── Aggregate ─────────────────────────────────────────────────────────────

    passed = all(c.passed for c in checks if c.blocking)
    return ReadinessResult(passed=passed, checks=checks, warnings=warnings)


# ── Verdict override ──────────────────────────────────────────────────────────

def apply_readiness_gate(
    result: ReadinessResult,
    current_verdict: str,
    promotable_verdicts: set[str] | None = None,
) -> tuple[str, str]:
    """
    If readiness failed, override any promotable verdict to NEEDS_MORE_DATA.

    Returns (final_verdict, reason_suffix).
    """
    if promotable_verdicts is None:
        promotable_verdicts = {"A", "SHOW_REAL_PROMISE", "READY_FOR_DEPLOYMENT", "MODEL_PROMOTION"}

    if result.passed:
        return current_verdict, ""

    failed_checks = [c.name for c in result.checks if not c.passed and c.blocking]
    reason = (
        f"Readiness gate FAIL ({len(failed_checks)} blocking check(s): "
        f"{', '.join(failed_checks[:3])}{'...' if len(failed_checks) > 3 else ''}). "
        "Metrics are exploratory only and cannot be used for deployment decisions."
    )
    if current_verdict in promotable_verdicts:
        return "NEEDS_MORE_DATA", reason
    return current_verdict, reason


# ── Report formatter ──────────────────────────────────────────────────────────

def format_readiness_report(result: ReadinessResult, title: str = "ML Data Readiness") -> str:
    lines: list[str] = []
    sep = "=" * 66
    lines.append(sep)
    lines.append(f"  {title}")
    lines.append(f"  {'PASS' if result.passed else 'FAIL'}  --  "
                 f"{result.n_passed}/{len(result.checks)} checks passed, "
                 f"{result.n_failed_blocking} blocking failure(s)")
    lines.append(sep)

    max_name = max((len(c.name) for c in result.checks), default=10)
    for c in result.checks:
        tag  = "[PASS]" if c.passed else ("[FAIL]" if c.blocking else "[WARN]")
        lines.append(f"  {tag}  {c.name:<{max_name}}  {c.message}")

    if result.warnings:
        lines.append("")
        for w in result.warnings:
            lines.append(f"  [INFO]  {w}")

    lines.append(sep)
    if not result.passed:
        lines.append(
            "  RESULT: Dataset does not meet minimum readiness thresholds.\n"
            "  Experiment metrics are EXPLORATORY ONLY and must not be used\n"
            "  for deployment, model promotion, or production gating decisions."
        )
    else:
        lines.append(
            "  RESULT: Dataset meets minimum readiness thresholds.\n"
            "  Experiment conclusions may be treated as valid for offline research."
        )
    lines.append(sep)
    return "\n".join(lines)
