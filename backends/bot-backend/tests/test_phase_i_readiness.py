"""
Phase I — ML Data Readiness Gate Tests.

Required tests (8 minimum):
  1. test_healthy_dataset_passes_all_checks
  2. test_fails_insufficient_organic_rows
  3. test_fails_backfill_dominance
  4. test_fails_organic_time_coverage
  5. test_fails_sl_distance_null_rate
  6. test_fails_pre_event_row_count
  7. test_fails_post_event_row_count
  8. test_fails_news_features_present

Additional gate function tests:
  9.  test_apply_gate_overrides_verdict_a_when_readiness_fails
  10. test_apply_gate_preserves_non_promotable_verdict
  11. test_apply_gate_no_override_when_readiness_passes
  12. test_format_report_pass
  13. test_format_report_fail_shows_failed_check_names
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_TESTS_DIR  = Path(__file__).resolve().parent
_BOT_ROOT   = _TESTS_DIR.parent
_SHARED     = _BOT_ROOT.parent / "shared"
for _p in (str(_BOT_ROOT), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared_lib.ml.readiness import (
    THRESHOLDS,
    check_dataset_readiness,
    apply_readiness_gate,
    format_readiness_report,
)


# ── Fixture helpers ────────────────────────────────────────────────────────────

def _make_timestamps(n: int, start: str = "2026-03-01", freq: str = "1h") -> list[str]:
    return pd.date_range(start, periods=n, freq=freq).strftime("%Y-%m-%dT%H:%M:%S+00:00").tolist()


def _healthy_df(n: int = 1_500) -> pd.DataFrame:
    """
    Minimal DataFrame that should pass all blocking checks.
    - All organic (account_id='default')
    - 14+ days spread across timestamps
    - Timing columns present so segment counts are met
    - No null base features
    - No news columns
    """
    # 1,500 rows across 15 days (daily freq gives us 25h slop per day)
    ts = pd.date_range("2026-03-01", periods=n, freq="15min").strftime(
        "%Y-%m-%dT%H:%M:%S+00:00"
    )
    df = pd.DataFrame({
        "open_timestamp":           list(ts),
        "account_id":               ["default"] * n,
        "sl_distance_pct":          [0.005] * n,
        "planned_rr":               [2.0]   * n,
        "sl_atr_ratio":             [1.5]   * n,
        # Timing columns: vary so both pre/post event segments have ≥200 rows each
        "minutes_to_next_event":    [30.0 if i % 6 == 0 else float("nan") for i in range(n)],
        "minutes_since_last_event": [30.0 if i % 6 == 1 else float("nan") for i in range(n)],
        "is_blackout_active":       [0] * n,
    })
    return df


# ── Test 1 ─────────────────────────────────────────────────────────────────────

class TestHealthyDatasetPasses:
    def test_healthy_dataset_passes_all_checks(self):
        df = _healthy_df(n=1_500)
        result = check_dataset_readiness(df, check_reactions=False, check_blackout=False)
        failed = [c.name for c in result.checks if not c.passed and c.blocking]
        assert result.passed, f"Expected PASS but blocking failures: {failed}"
        assert result.n_failed_blocking == 0


# ── Test 2 ─────────────────────────────────────────────────────────────────────

class TestOrganicRowCount:
    def test_fails_insufficient_organic_rows(self):
        df = _healthy_df(n=1_500)
        # Flip all but 5 rows to backfill
        df["account_id"] = "backfill"
        df.loc[df.index[:5], "account_id"] = "default"
        result = check_dataset_readiness(df, check_reactions=False)
        organic_check = next(c for c in result.checks if c.name == "organic_row_count")
        assert not organic_check.passed
        assert organic_check.actual == 5
        assert not result.passed

    def test_threshold_exact_boundary(self):
        df = _healthy_df(n=1_500)
        df["account_id"] = "backfill"
        # Exactly 1,000 organic rows spread over 15 days
        df.loc[df.index[:1_000], "account_id"] = "default"
        result = check_dataset_readiness(df, check_reactions=False)
        organic_check = next(c for c in result.checks if c.name == "organic_row_count")
        assert organic_check.actual == 1_000
        assert organic_check.passed  # exactly at threshold must pass


# ── Test 3 ─────────────────────────────────────────────────────────────────────

class TestBackfillDominance:
    def test_fails_backfill_dominance(self):
        df = _healthy_df(n=1_500)
        # 51% backfill → > 50% threshold → FAIL
        cutoff = int(len(df) * 0.51)
        df.loc[df.index[:cutoff], "account_id"] = "backfill"
        df.loc[df.index[cutoff:], "account_id"] = "default"
        result = check_dataset_readiness(df, check_reactions=False)
        bd_check = next(c for c in result.checks if c.name == "backfill_dominance")
        assert not bd_check.passed
        assert bd_check.actual > 50.0

    def test_passes_backfill_at_50_pct(self):
        df = _healthy_df(n=1_500)
        half = len(df) // 2
        df.loc[df.index[:half], "account_id"] = "backfill"
        df.loc[df.index[half:], "account_id"] = "default"
        result = check_dataset_readiness(df, check_reactions=False)
        bd_check = next(c for c in result.checks if c.name == "backfill_dominance")
        assert bd_check.actual <= 50.0
        assert bd_check.passed


# ── Test 4 ─────────────────────────────────────────────────────────────────────

class TestOrganicTimeCoverage:
    def test_fails_organic_time_coverage(self):
        # Only 2 rows of organic data → 0 days span
        df = pd.DataFrame({
            "open_timestamp": [
                "2026-03-22T15:00:00+00:00",
                "2026-03-22T15:01:00+00:00",
            ],
            "account_id":           ["default", "default"],
            "sl_distance_pct":      [0.005, 0.005],
            "planned_rr":           [2.0, 2.0],
            "sl_atr_ratio":         [1.5, 1.5],
            "minutes_to_next_event": [float("nan"), float("nan")],
            "minutes_since_last_event": [float("nan"), float("nan")],
        })
        result = check_dataset_readiness(
            df,
            check_reactions=False,
            thresholds={"min_organic_rows": 1},  # relax row count to isolate coverage
        )
        cov_check = next(c for c in result.checks if c.name == "organic_time_coverage_days")
        assert not cov_check.passed
        assert cov_check.actual < 14

    def test_passes_14_day_coverage(self):
        df = _healthy_df(n=1_500)
        # _healthy_df spans >14 days at 15-min intervals
        result = check_dataset_readiness(df, check_reactions=False)
        cov_check = next(c for c in result.checks if c.name == "organic_time_coverage_days")
        assert cov_check.passed
        assert cov_check.actual >= 14


# ── Test 5 ─────────────────────────────────────────────────────────────────────

class TestSlDistanceNullRate:
    def test_fails_sl_distance_null_rate(self):
        df = _healthy_df(n=1_500)
        # Make 15% of sl_distance_pct null → > 10% threshold
        null_count = int(len(df) * 0.15)
        df.loc[df.index[:null_count], "sl_distance_pct"] = float("nan")
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "sl_distance_pct_null_rate")
        assert not check.passed
        assert check.actual > 10.0

    def test_passes_sl_distance_below_threshold(self):
        df = _healthy_df(n=1_500)
        # Make 5% null → < 10% threshold
        null_count = int(len(df) * 0.05)
        df.loc[df.index[:null_count], "sl_distance_pct"] = float("nan")
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "sl_distance_pct_null_rate")
        assert check.passed

    def test_fails_planned_rr_null_rate(self):
        df = _healthy_df(n=1_500)
        null_count = int(len(df) * 0.20)
        df.loc[df.index[:null_count], "planned_rr"] = float("nan")
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "planned_rr_null_rate")
        assert not check.passed

    def test_fails_sl_atr_ratio_null_rate(self):
        df = _healthy_df(n=1_500)
        null_count = int(len(df) * 0.20)
        df.loc[df.index[:null_count], "sl_atr_ratio"] = float("nan")
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "sl_atr_ratio_null_rate")
        assert not check.passed


# ── Test 6 ─────────────────────────────────────────────────────────────────────

class TestPreEventRowCount:
    def test_fails_pre_event_row_count(self):
        df = _healthy_df(n=1_500)
        # Remove all minutes_to_next_event values → pre_event_rows = 0
        df["minutes_to_next_event"] = float("nan")
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "pre_event_row_count")
        assert not check.passed
        assert check.actual == 0

    def test_passes_pre_event_when_enough_rows(self):
        df = _healthy_df(n=1_500)
        # _healthy_df sets minutes_to_next_event=30 for every 6th row → 250 rows at n=1500
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "pre_event_row_count")
        assert check.actual >= 200
        assert check.passed


# ── Test 7 ─────────────────────────────────────────────────────────────────────

class TestPostEventRowCount:
    def test_fails_post_event_row_count(self):
        df = _healthy_df(n=1_500)
        df["minutes_since_last_event"] = float("nan")
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "post_event_row_count")
        assert not check.passed
        assert check.actual == 0

    def test_passes_post_event_when_enough_rows(self):
        df = _healthy_df(n=1_500)
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "post_event_row_count")
        assert check.actual >= 200
        assert check.passed


# ── Test 8 ─────────────────────────────────────────────────────────────────────

class TestNewsFeaturesAbsent:
    def test_fails_news_features_present(self):
        df = _healthy_df(n=1_500)
        df["sentiment_compound"] = 0.1
        df["news_impact_score"]  = 0.5
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "news_features_absent")
        assert not check.passed
        assert not result.passed

    def test_passes_when_no_news_columns(self):
        df = _healthy_df(n=1_500)
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "news_features_absent")
        assert check.passed

    @pytest.mark.parametrize("col", [
        "narrative_effectiveness_score",
        "sentiment_accuracy_score",
        "headline",
        "raw_text",
        "sentiment_label",
    ])
    def test_each_forbidden_column_triggers_fail(self, col):
        df = _healthy_df(n=1_500)
        df[col] = "dummy"
        result = check_dataset_readiness(df, check_reactions=False)
        check = next(c for c in result.checks if c.name == "news_features_absent")
        assert not check.passed


# ── Tests 9–11: apply_readiness_gate ──────────────────────────────────────────

class TestApplyReadinessGate:
    def _failed_result(self):
        """Return a ReadinessResult where all blocking checks fail."""
        df = pd.DataFrame({
            "account_id": ["backfill"] * 10,
            "open_timestamp": _make_timestamps(10),
            "sl_distance_pct": [float("nan")] * 10,
            "planned_rr": [float("nan")] * 10,
            "sl_atr_ratio": [float("nan")] * 10,
        })
        return check_dataset_readiness(df, check_reactions=False)

    def test_overrides_verdict_a_when_readiness_fails(self):
        failed = self._failed_result()
        assert not failed.passed
        final, reason = apply_readiness_gate(failed, "A")
        assert final == "NEEDS_MORE_DATA"
        assert "Readiness gate FAIL" in reason

    def test_preserves_non_promotable_verdict_when_readiness_fails(self):
        failed = self._failed_result()
        # Verdicts B, C, D are not in the promotable set — should not be overridden
        for v in ("B", "C", "D", "ABORTED_DATE_PROXY"):
            final, _ = apply_readiness_gate(failed, v, promotable_verdicts={"A"})
            assert final == v, f"Verdict {v} should not be overridden"

    def test_no_override_when_readiness_passes(self):
        df = _healthy_df(n=1_500)
        passed = check_dataset_readiness(df, check_reactions=False)
        assert passed.passed
        final, reason = apply_readiness_gate(passed, "A")
        assert final == "A"
        assert reason == ""

    def test_custom_promotable_set(self):
        failed = self._failed_result()
        # Make "B" also promotable
        final, _ = apply_readiness_gate(failed, "B", promotable_verdicts={"A", "B"})
        assert final == "NEEDS_MORE_DATA"


# ── Tests 12–13: format_readiness_report ──────────────────────────────────────

class TestFormatReadinessReport:
    def test_format_report_pass(self):
        df = _healthy_df(n=1_500)
        result = check_dataset_readiness(df, check_reactions=False)
        report = format_readiness_report(result, title="Test Report")
        assert "PASS" in report
        assert "Test Report" in report
        assert "RESULT:" in report

    def test_format_report_fail_shows_failed_check_names(self):
        df = _healthy_df(n=1_500)
        df["account_id"] = "backfill"  # trigger organic_row_count and backfill_dominance
        result = check_dataset_readiness(df, check_reactions=False)
        report = format_readiness_report(result)
        assert "FAIL" in report
        assert "organic_row_count" in report
        assert "backfill_dominance" in report
        assert "must not be used" in report.lower() or "exploratory only" in report.lower()


# ── Edge cases ─────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_dataframe_returns_failed_result(self):
        df = pd.DataFrame()
        result = check_dataset_readiness(df)
        assert not result.passed
        assert result.n_failed_blocking >= 1

    def test_missing_account_id_column_adds_warning(self):
        df = _healthy_df(n=1_500).drop(columns=["account_id"])
        result = check_dataset_readiness(df, check_reactions=False)
        assert any("account_id" in w for w in result.warnings)

    def test_thresholds_override_works(self):
        df = _healthy_df(n=1_500)
        df["account_id"] = "backfill"
        df.loc[df.index[:500], "account_id"] = "default"
        # Default threshold = 1_000, override to 500 → should pass organic count
        result = check_dataset_readiness(
            df,
            check_reactions=False,
            thresholds={"min_organic_rows": 500},
        )
        organic_check = next(c for c in result.checks if c.name == "organic_row_count")
        assert organic_check.passed

    def test_reaction_checks_only_run_when_requested(self):
        df = _healthy_df(n=1_500)
        result_no_react = check_dataset_readiness(df, check_reactions=False)
        result_with_react = check_dataset_readiness(df, check_reactions=True)
        names_no_react = {c.name for c in result_no_react.checks}
        names_with_react = {c.name for c in result_with_react.checks}
        reaction_checks = {
            "volatility_post_event_null_rate",
            "volume_spike_post_event_null_rate",
            "max_adverse_move_null_rate",
        }
        assert not reaction_checks & names_no_react
        assert reaction_checks <= names_with_react
