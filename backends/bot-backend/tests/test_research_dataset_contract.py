"""Phase 14 — the research data contract.

The four failure modes these guard, in order of how quietly they ruin a result:

1. a dataset that is not what it claims to be;
2. a dataset that is silently broken, or silently repaired;
3. derived timeframes that disagree with the base they came from;
4. a split that lets a model see its own future.
"""
from __future__ import annotations

import json

import pytest

from app.research.dataset import (
    BROKER_DEMO,
    FINAL_HOLDOUT,
    FORMAL_EVALUATION,
    LEGACY_BACKFILL,
    LIVE,
    PAPER_FORWARD,
    REAL_HISTORICAL,
    SYNTHETIC,
    TESTNET,
    TRAINING_EXAMPLE_SCHEMA_VERSION,
    TRAIN,
    DatasetError,
    FinalHoldoutViolation,
    InstrumentIdentity,
    Partition,
    PurgeEmbargoPolicy,
    assess_quality,
    build_manifest,
    build_training_example,
    derive,
    filter_by_provenance,
    guard_final_holdout,
    holdout_access_audit,
    include_for_training,
    partition,
    purge_embargo_partitions,
    series_checksum,
)

MINUTE = 60_000
ANCHOR = 1_700_000_000_000 - (1_700_000_000_000 % (24 * 60 * MINUTE))


def bar(open_ms, o=100.0, h=101.0, low=99.0, c=100.5, v=10.0, step=MINUTE):
    return [open_ms, f"{o}", f"{h}", f"{low}", f"{c}", f"{v}",
            open_ms + step - 1, "0", 0, "0", "0", "0"]


def minutes(count, *, start=ANCHOR, price=100.0):
    return [bar(start + i * MINUTE, o=price + i, h=price + i + 1,
                low=price + i - 1, c=price + i + 0.5) for i in range(count)]


# ══════════════════════════════════════════════════════════════════════════
# §14.8 Quality: detect, never repair
# ══════════════════════════════════════════════════════════════════════════


def test_a_clean_series_is_reported_clean():
    report = assess_quality(minutes(120), symbol="BTCUSDT", timeframe="1m")

    assert report.rows == 120
    assert report.missing_bars == 0
    assert report.duplicate_opens == 0
    assert report.out_of_order == 0
    assert report.is_usable
    assert report.completeness == pytest.approx(1.0)


def test_a_gap_is_reported_and_left_alone():
    rows = minutes(10)
    del rows[4:7]                      # three minutes missing
    report = assess_quality(rows, symbol="BTCUSDT", timeframe="1m")

    assert report.missing_bars == 3
    assert report.rows == 7, "the gap must not be filled in"
    assert report.completeness == pytest.approx(7 / 10)
    assert report.is_usable, "a gap is incomplete data, not broken data"
    assert report.missing_windows


def test_duplicates_make_a_series_unusable():
    rows = minutes(5)
    rows.insert(3, rows[2])
    report = assess_quality(rows, symbol="BTCUSDT", timeframe="1m")

    assert report.duplicate_opens == 1
    assert not report.is_usable


def test_out_of_order_bars_make_a_series_unusable():
    rows = minutes(5)
    rows[1], rows[3] = rows[3], rows[1]
    report = assess_quality(rows, symbol="BTCUSDT", timeframe="1m")

    assert report.out_of_order >= 1
    assert not report.is_usable


@pytest.mark.parametrize(
    "mutate,field",
    [
        (lambda r: r.__setitem__(4, "0"), "non_positive_prices"),
        (lambda r: r.__setitem__(4, "-5"), "non_positive_prices"),
        (lambda r: r.__setitem__(5, "-1"), "negative_volume"),
        (lambda r: r.__setitem__(2, "50"), "ohlc_violations"),   # high below close
    ],
)
def test_impossible_values_are_caught(mutate, field):
    rows = minutes(5)
    mutate(rows[2])
    report = assess_quality(rows, symbol="BTCUSDT", timeframe="1m")

    assert getattr(report, field) >= 1
    assert not report.is_usable


def test_an_empty_series_reports_rather_than_raises():
    report = assess_quality([], symbol="BTCUSDT", timeframe="1m")
    assert report.rows == 0
    assert report.completeness == 0.0


# ══════════════════════════════════════════════════════════════════════════
# §14.7 Derivation: deterministic, and never approximate
# ══════════════════════════════════════════════════════════════════════════


def test_derivation_aggregates_exactly():
    rows = minutes(60)
    five = derive(rows, "5m")

    assert len(five) == 12
    first = five[0]
    assert int(first[0]) == int(rows[0][0])
    assert float(first[1]) == float(rows[0][1])          # open of the first
    assert float(first[4]) == float(rows[4][4])          # close of the last
    assert float(first[2]) == max(float(r[2]) for r in rows[:5])
    assert float(first[3]) == min(float(r[3]) for r in rows[:5])
    assert float(first[5]) == pytest.approx(sum(float(r[5]) for r in rows[:5]))


def test_derivation_is_deterministic():
    rows = minutes(240)
    assert series_checksum(derive(rows, "15m")) == series_checksum(derive(rows, "15m"))


def test_an_incomplete_window_is_dropped_not_approximated():
    """A 15m bar built from 13 minutes is not a 15m bar."""
    rows = minutes(30)
    del rows[20]                       # punch a hole in the second window
    fifteen = derive(rows, "15m")

    assert len(fifteen) == 1, "the incomplete window must not be emitted"
    assert int(fifteen[0][0]) == int(rows[0][0])


def test_a_timeframe_that_is_not_derivable_is_refused():
    with pytest.raises(DatasetError, match="not derivable"):
        derive(minutes(60), "7m")


@pytest.mark.parametrize("timeframe,factor", [("5m", 5), ("15m", 15), ("1h", 60)])
def test_derived_bars_have_the_right_span(timeframe, factor):
    rows = minutes(60 * 4)
    derived = derive(rows, timeframe)
    assert derived
    for candle in derived:
        assert int(candle[6]) - int(candle[0]) == factor * MINUTE - 1


# ══════════════════════════════════════════════════════════════════════════
# §14.9 / §14.10 Partitions and the final holdout
# ══════════════════════════════════════════════════════════════════════════


def test_partitions_are_chronological_and_do_not_overlap():
    parts = partition(minutes(1000))

    assert [p.name for p in parts] == ["TRAIN", "VALIDATION", "TEST", FINAL_HOLDOUT]
    for earlier, later in zip(parts, parts[1:]):
        assert earlier.end_ms < later.start_ms


def test_every_row_lands_in_exactly_one_partition():
    rows = minutes(1000)
    parts = partition(rows)
    assert sum(p.rows for p in parts) == len(rows)


def test_the_final_holdout_is_the_most_recent_data():
    rows = minutes(1000)
    parts = partition(rows)
    holdout = next(p for p in parts if p.name == FINAL_HOLDOUT)
    assert holdout.end_ms == int(rows[-1][6])


def test_reading_the_final_holdout_raises():
    rows = minutes(1000)
    parts = partition(rows)
    holdout = next(p for p in parts if p.name == FINAL_HOLDOUT)

    with pytest.raises(FinalHoldoutViolation, match="stays untouched"):
        guard_final_holdout(parts, holdout.start_ms, purpose="threshold tuning")


def test_formal_evaluation_holdout_access_requires_explicit_mode_and_is_audited():
    rows = minutes(1000)
    parts = partition(rows)
    holdout = next(p for p in parts if p.name == FINAL_HOLDOUT)
    before = len(holdout_access_audit())

    guard_final_holdout(
        parts, holdout.start_ms, purpose="final evaluation",
        mode=FORMAL_EVALUATION,
    )

    audit = holdout_access_audit()
    assert len(audit) == before + 1
    assert audit[-1]["purpose"] == "final evaluation"
    assert audit[-1]["mode"] == FORMAL_EVALUATION


def test_reading_the_training_window_is_fine():
    rows = minutes(1000)
    parts = partition(rows)
    train = next(p for p in parts if p.name == TRAIN)
    guard_final_holdout(parts, train.start_ms, purpose="feature selection")


def test_a_split_that_leaves_no_holdout_is_refused():
    with pytest.raises(DatasetError, match="final holdout"):
        partition(minutes(100), train=0.7, validation=0.2, test=0.1)


def test_a_dataset_too_small_to_split_is_refused():
    with pytest.raises(DatasetError, match="too small"):
        partition(minutes(3))


def test_partitioning_nothing_is_refused():
    with pytest.raises(DatasetError, match="empty"):
        partition([])


def test_purge_removes_training_examples_whose_labels_cross_the_next_partition():
    rows = minutes(80)
    parts = (
        Partition(TRAIN, int(rows[0][0]), int(rows[39][6]), 40),
        Partition("VALIDATION", int(rows[40][0]), int(rows[59][6]), 20),
        Partition("TEST", int(rows[60][0]), int(rows[69][6]), 10),
        Partition(FINAL_HOLDOUT, int(rows[70][0]), int(rows[79][6]), 10),
    )
    example = build_training_example(
        dataset_id="ds", dataset_hash="hash", instrument=instrument(),
        timeframe="1m", decision_timestamp_ms=int(rows[38][6]), rows=rows,
        horizon_bars=12, policy_version="policy", strategy_version="strategy",
    )

    kept, report = purge_embargo_partitions(
        [example], parts, PurgeEmbargoPolicy(label_horizon_ms=12 * MINUTE),
    )

    assert kept == ()
    assert report.excluded_counts["TRAIN_PURGE"] == 1
    assert report.purge_ranges[0]["from_partition"] == TRAIN


def test_embargo_removes_early_examples_after_a_boundary():
    rows = minutes(80)
    parts = (
        Partition(TRAIN, int(rows[0][0]), int(rows[39][6]), 40),
        Partition("VALIDATION", int(rows[40][0]), int(rows[59][6]), 20),
        Partition("TEST", int(rows[60][0]), int(rows[69][6]), 10),
        Partition(FINAL_HOLDOUT, int(rows[70][0]), int(rows[79][6]), 10),
    )
    example = build_training_example(
        dataset_id="ds", dataset_hash="hash", instrument=instrument(),
        timeframe="1m", decision_timestamp_ms=int(rows[40][6]), rows=rows,
        horizon_bars=2, policy_version="policy", strategy_version="strategy",
    )

    kept, report = purge_embargo_partitions(
        [example], parts, PurgeEmbargoPolicy(
            label_horizon_ms=2 * MINUTE, embargo_ms=3 * MINUTE,
        ),
    )

    assert kept == ()
    assert report.excluded_counts["VALIDATION_EMBARGO"] == 1
    assert report.embargo_ranges[0]["to_partition"] == "VALIDATION"


# ══════════════════════════════════════════════════════════════════════════
# §14.11 Manifest
# ══════════════════════════════════════════════════════════════════════════


@pytest.fixture
def series():
    base = minutes(1000)
    return {"BTCUSDT": {"1m": base, "15m": derive(base, "15m")}}


def test_the_manifest_describes_the_dataset(series, tmp_path):
    manifest = build_manifest(series, dataset_id="ds_test")

    assert manifest.dataset_id == "ds_test"
    assert manifest.symbols == ("BTCUSDT",)
    assert manifest.base_timeframe == "1m"
    assert manifest.derived_timeframes == ("15m",)
    assert manifest.rows["BTCUSDT:1m"] == 1000
    assert manifest.checksums["BTCUSDT:1m"]
    assert manifest.quality["BTCUSDT:1m"]["is_usable"]
    assert len(manifest.partitions) == 4
    assert manifest.feature_schema_version
    assert manifest.label_schema_version


def test_the_manifest_hash_is_of_the_data_not_the_path(series):
    first = build_manifest(series, dataset_id="a")
    second = build_manifest(series, dataset_id="b")
    assert first.dataset_hash == second.dataset_hash

    altered = {"BTCUSDT": {**series["BTCUSDT"], "1m": series["BTCUSDT"]["1m"][:-1]}}
    assert build_manifest(altered, dataset_id="a").dataset_hash != first.dataset_hash


def test_exchange_data_provenance_is_fixed(series):
    manifest = build_manifest(series, dataset_id="ds_test")
    assert manifest.provenance == REAL_HISTORICAL

    with pytest.raises(TypeError):
        type(manifest)(
            dataset_id="x", venue="binance", symbols=("BTCUSDT",),
            base_timeframe="1m", derived_timeframes=(), start_ms=0, end_ms=1,
            rows={}, checksums={}, quality={}, partitions=(),
            provenance="SYNTHETIC",  # type: ignore[call-arg]
        )


def test_the_manifest_round_trips_to_disk(series, tmp_path):
    manifest = build_manifest(series, dataset_id="ds_test")
    path = tmp_path / "ds_test.manifest.json"
    manifest.write(str(path))

    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["dataset_hash"] == manifest.dataset_hash
    assert loaded["provenance"] == REAL_HISTORICAL
    assert loaded["partitions"][-1]["name"] == FINAL_HOLDOUT


def test_a_manifest_needs_a_dataset():
    with pytest.raises(DatasetError, match="no series"):
        build_manifest({}, dataset_id="empty")


# ══════════════════════════════════════════════════════════════════════════
# §14 training-example contract: features at t, labels after t
# ══════════════════════════════════════════════════════════════════════════


def instrument():
    return InstrumentIdentity(
        venue="binance_usdm",
        venue_symbol="BTCUSDT",
        canonical_symbol="BTC/USDT:PERP",
        instrument_type="PERP",
        asset_class="CRYPTO_PERP",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        contract_type="LINEAR_PERP",
        contract_multiplier=1.0,
        tick_size=0.1,
        step_size=0.001,
    )


def test_training_example_id_is_deterministic_and_schema_versioned():
    rows = minutes(20)
    example = build_training_example(
        dataset_id="ds",
        dataset_hash="hash",
        instrument=instrument(),
        timeframe="1m",
        decision_timestamp_ms=int(rows[9][6]),
        rows=rows,
        horizon_bars=5,
        policy_version="policy",
        strategy_version="strategy",
    )
    again = build_training_example(
        dataset_id="ds",
        dataset_hash="hash",
        instrument=instrument(),
        timeframe="1m",
        decision_timestamp_ms=int(rows[9][6]),
        rows=rows,
        horizon_bars=5,
        policy_version="policy",
        strategy_version="strategy",
    )

    assert example.schema_version == TRAINING_EXAMPLE_SCHEMA_VERSION
    assert example.example_id == again.example_id
    assert example.example_id.startswith("tex_")


def test_features_use_only_decision_time_data_and_labels_use_future_data():
    rows = minutes(20, price=100)
    example = build_training_example(
        dataset_id="ds",
        dataset_hash="hash",
        instrument=instrument(),
        timeframe="1m",
        decision_timestamp_ms=int(rows[9][6]),
        rows=rows,
        htf_rows=derive(rows[:10], "5m"),
        horizon_bars=3,
        policy_version="policy",
        strategy_version="strategy",
        stop_price=105.0,
        target_price=112.0,
        fee_cost=0.1,
        spread_cost=0.1,
        slippage_cost=0.1,
    )

    assert example.features.raw_ohlcv["close_time_ms"] == int(rows[9][6])
    assert example.features.htf_context["latest_close_time_ms"] <= int(rows[9][6])
    assert example.labels.horizon_bars == 3
    assert example.labels.net_return < example.labels.gross_return
    assert example.labels.tp_sl_outcome in {"TP_FIRST", "SL_FIRST", "AMBIGUOUS", "NEITHER"}


def test_missing_derivatives_context_is_explicit_not_zero_filled():
    rows = minutes(12)
    example = build_training_example(
        dataset_id="ds",
        dataset_hash="hash",
        instrument=instrument(),
        timeframe="1m",
        decision_timestamp_ms=int(rows[5][6]),
        rows=rows,
        horizon_bars=2,
        policy_version="policy",
        strategy_version="strategy",
    )

    assert example.features.funding["available"] is False
    assert example.features.open_interest["available"] is False
    assert example.features.basis["available"] is False


def test_observation_context_cannot_look_ahead():
    rows = minutes(12)
    decision_ts = int(rows[5][6])
    example = build_training_example(
        dataset_id="ds",
        dataset_hash="hash",
        instrument=instrument(),
        timeframe="1m",
        decision_timestamp_ms=decision_ts,
        rows=rows,
        horizon_bars=2,
        policy_version="policy",
        strategy_version="strategy",
        funding_observations=[
            {"timestamp_ms": decision_ts - 1, "rate": 0.01},
            {"timestamp_ms": decision_ts + MINUTE, "rate": 0.99},
        ],
    )

    assert example.features.funding["rate"] == 0.01


def test_provenance_taxonomy_and_default_training_filter():
    for provenance in (REAL_HISTORICAL, PAPER_FORWARD, TESTNET, BROKER_DEMO, SYNTHETIC, LEGACY_BACKFILL, LIVE):
        include_for_training(provenance)

    assert include_for_training(REAL_HISTORICAL)
    assert include_for_training(BROKER_DEMO)
    assert not include_for_training(SYNTHETIC)
    assert not include_for_training(LEGACY_BACKFILL)


def test_provenance_filter_excludes_synthetic_and_legacy_by_default():
    rows = minutes(12)
    kwargs = dict(
        dataset_id="ds",
        dataset_hash="hash",
        instrument=instrument(),
        timeframe="1m",
        decision_timestamp_ms=int(rows[5][6]),
        rows=rows,
        horizon_bars=2,
        policy_version="policy",
        strategy_version="strategy",
    )
    real = build_training_example(**kwargs, provenance=REAL_HISTORICAL)
    synthetic = build_training_example(**kwargs, provenance=SYNTHETIC)
    legacy = build_training_example(**kwargs, provenance=LEGACY_BACKFILL)

    assert filter_by_provenance([real, synthetic, legacy]) == (real,)


def test_unknown_provenance_is_rejected():
    rows = minutes(12)
    with pytest.raises(DatasetError, match="unknown research provenance"):
        build_training_example(
            dataset_id="ds",
            dataset_hash="hash",
            instrument=instrument(),
            timeframe="1m",
            decision_timestamp_ms=int(rows[5][6]),
            rows=rows,
            horizon_bars=2,
            policy_version="policy",
            strategy_version="strategy",
            provenance="BACKTEST_ALIAS",
        )
