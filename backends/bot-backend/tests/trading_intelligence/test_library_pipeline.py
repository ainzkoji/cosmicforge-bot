"""Section 12.15 -- the real historical outcome-library pipeline."""
from __future__ import annotations

import dataclasses
import json
import subprocess
import sys
from pathlib import Path

import pytest
from _helpers import HIST_START_MS, TF_MS, make_historical_db, rows_from_closes

from app.replay.historical_provider import HistoricalClock, HistoricalMarketDataProvider
from app.runner.market_snapshot import MarketSnapshot
from app.trading_intelligence.contracts.forecast import CalibrationStatus
from app.trading_intelligence.forecast import build_library as bl
from app.trading_intelligence.forecast.artifact import (
    LibraryArtifactError, MANIFEST_FILE, ROWS_FILE, load_library_artifact, manifest_hash_of,
)
from app.trading_intelligence.forecast.calibration_report import (
    CalibrationPolicy, build_calibration_record, derive_status, evaluate_library_calibration,
    status_from_stored_record,
)
from app.trading_intelligence.integration.snapshot_adapter import evaluate_market_state

BACKEND = Path(__file__).resolve().parents[2]
START = HIST_START_MS + 260 * TF_MS
END = HIST_START_MS + 400 * TF_MS


@pytest.fixture(scope="module")
def hist_db(tmp_path_factory):
    d = tmp_path_factory.mktemp("hist")
    return make_historical_db(d / "h.db", symbols=("BTCUSDT",), n_bars=560)


def _cfg(**kw):
    base = dict(symbols=("BTCUSDT",), timeframe="15m", start_ms=START, end_ms=END, label_horizon_bars=24,
                source_kind="SYNTHETIC_TEST", source_provider="synthetic_test_wave")
    base.update(kw)
    return bl.BuildConfig(**base)


def _load(db, cfg):
    return bl.load_series_from_db(str(db), cfg.symbols, cfg.timeframe, cfg.start_ms, cfg.end_ms,
                                  label_horizon_bars=cfg.label_horizon_bars, warmup_bars=cfg.snapshot_limit)


@pytest.fixture(scope="module")
def built(hist_db, tmp_path_factory):
    cfg = _cfg()
    series, meta, info = _load(hist_db, cfg)
    out = tmp_path_factory.mktemp("out")
    path, result = bl.build_and_write(series, meta, cfg, str(out), source_info=info)
    return dict(cfg=cfg, series=series, meta=meta, info=info, path=path, result=result, out=out)


# -- runnable builder ------------------------------------------------------------
def test_runnable_builder_exists_as_module_entry_point():
    proc = subprocess.run([sys.executable, "-m", "app.trading_intelligence.forecast.build_library", "--help"],
                          cwd=BACKEND, capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0 and "--label-horizon-bars" in proc.stdout and "--symbols" in proc.stdout


def test_builder_reads_existing_historical_candles_table_without_new_store(built):
    assert built["info"]["data_sources"] == ["synthetic_test_wave"]
    assert built["result"].provenance["source"]["name"] == "historical_candles"
    assert built["result"].provenance["labeled_count"] > 0


# -- causality ---------------------------------------------------------------------
def test_historical_state_matches_live_snapshot_state_for_identical_data(built):
    rows = built["series"]["BTCUSDT"]["15m"]
    k = 300
    t = rows[k - 1][6]
    provider = HistoricalMarketDataProvider({"BTCUSDT": {"15m": rows}}, HistoricalClock(t))
    hist = provider.build_snapshot("BTCUSDT", "15m", limit=250)
    live = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=rows[k - 250:k], source="historical_candles")
    for snap in (hist, live):
        object.__setattr__(snap, "market_snapshot_id", "hist_parity")
    assert hist.data_hash == live.data_hash
    ms_h = evaluate_market_state(hist, venue="binance", source="historical_candles", use_cache=False)
    ms_l = evaluate_market_state(live, venue="binance", source="historical_candles", use_cache=False)
    assert ms_h.canonical_hash == ms_l.canonical_hash


def test_frozen_candidates_identical_with_and_without_future_data(built):
    cfg, rows = built["cfg"], built["series"]["BTCUSDT"]["15m"]
    from app.trading_intelligence.regime.policy import default_policy
    from app.trading_intelligence.setups.policy import default_policies

    checked = 0
    for k in range(255, len(rows), 3):
        t = rows[k - 1][6]
        full = HistoricalMarketDataProvider({"BTCUSDT": {"15m": rows}}, HistoricalClock(t))
        cut = HistoricalMarketDataProvider({"BTCUSDT": {"15m": rows[:k]}}, HistoricalClock(t))
        a = bl._freeze_decision_point(full, "BTCUSDT", "15m", cfg, {}, default_policy(), default_policies())
        b = bl._freeze_decision_point(cut, "BTCUSDT", "15m", cfg, {}, default_policy(), default_policies())
        assert [c.setup_candidate_id for c in a[3]] == [c.setup_candidate_id for c in b[3]]
        assert a[1].canonical_hash == b[1].canonical_hash
        checked += len(a[3])
    assert checked > 0


def test_future_rows_reach_only_the_labeler_after_candidates_are_frozen(built, monkeypatch):
    events = []
    orig_discover, orig_label, orig_eval = bl.discover_all, bl.label_candidate, bl.evaluate_market_state

    def spy_eval(snapshot, **kw):
        events.append(("state", snapshot.latest_closed_candle_time, max(int(r[6]) for r in snapshot.candles)))
        return orig_eval(snapshot, **kw)

    def spy_discover(*, snapshot, **kw):
        events.append(("discover", snapshot.latest_closed_candle_time, max(int(r[6]) for r in snapshot.candles)))
        return orig_discover(snapshot=snapshot, **kw)

    def spy_label(candidate, all_rows, **kw):
        events.append(("label", candidate.decision_time, None))
        return orig_label(candidate, all_rows, **kw)

    monkeypatch.setattr(bl, "evaluate_market_state", spy_eval)
    monkeypatch.setattr(bl, "discover_all", spy_discover)
    monkeypatch.setattr(bl, "label_candidate", spy_label)
    cfg = _cfg(end_ms=START + 25 * TF_MS)
    bl.run_build(built["series"], built["meta"], cfg)
    assert any(e[0] == "label" for e in events)
    last_discover = None
    for kind, decision_time, max_close in events:
        if kind in ("state", "discover"):
            assert max_close <= decision_time, "feature path received a candle past the decision time"
            last_discover = decision_time
        else:
            assert last_discover == decision_time, "labeler ran before that decision point's candidates were frozen"


# -- library content -----------------------------------------------------------------
def _fixture_rows(family):
    from test_setups import _breakout_series, _momentum_series, _range_series, _trend_pullback_series
    from conftest import make_binance_klines

    if family == "MOMENTUM_CONTINUATION_V1":
        rows = [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), int(r[6])]
                for r in make_binance_klines(150, trend=0.0, vol=3.0, seed=3)]
    else:
        s = {"TREND_PULLBACK_V2": _trend_pullback_series, "BREAKOUT_VOL_EXPANSION_V2": _breakout_series,
             "RANGE_MEAN_REVERSION_V2": _range_series}[family]()
        rows = [[ct - 899_999, o, h, low, c, v, ct] for o, h, low, c, v, ct
                in zip(s.open, s.high, s.low, s.close, s.volume, s.close_time)]
    last = rows[-1]
    last_close = last[4]
    future = [[last[6] + 1 + i * TF_MS, last_close, last_close + 0.2, last_close - 0.2, last_close, 1000, last[6] + (i + 1) * TF_MS]
              for i in range(30)]
    return rows, future


@pytest.mark.parametrize("family", ["TREND_PULLBACK_V2", "BREAKOUT_VOL_EXPANSION_V2",
                                    "RANGE_MEAN_REVERSION_V2", "MOMENTUM_CONTINUATION_V1"])
def test_all_four_setup_families_can_enter_the_library(family):
    rows, future = _fixture_rows(family)
    decision = rows[-1][6]
    cfg = _cfg(start_ms=decision, end_ms=decision, label_horizon_bars=10)
    result = bl.run_build({"BTCUSDT": {"15m": rows + future}}, {}, cfg)
    assert result.provenance["counts_by_setup_family"].get(family, 0) >= 1
    assert any(r.label.setup_family == family for r in result.library.rows)


def test_non_executed_candidates_enter_the_library_and_builder_has_no_execution_inputs(built):
    import inspect
    for name in inspect.signature(bl.run_build).parameters:
        assert not any(x in name for x in ("execut", "trade", "order", "position", "capital"))
    assert built["result"].provenance["labeled_count"] == len(built["result"].library.rows) > 0


def test_truncated_future_horizon_is_recorded_not_silently_labeled(hist_db):
    cfg = _cfg(end_ms=HIST_START_MS + 559 * TF_MS, label_horizon_bars=48)
    series, meta, _ = _load(hist_db, cfg)
    result = bl.run_build(series, meta, cfg)
    assert result.skipped.get(bl.CENSORED_FUTURE_HORIZON, 0) > 0
    for r in result.library.rows:
        assert r.label.label_quality == "VALID" and r.label.terminal_horizon_bars == 48
    p = result.provenance
    assert p["skipped_count"] == p["candidate_count"] - p["labeled_count"] > 0


def test_same_bar_ambiguity_stays_conservative_in_pipeline():
    rows, future = _fixture_rows("TREND_PULLBACK_V2")
    future[0] = [future[0][0], future[0][1], future[0][1] + 1000, future[0][1] - 1000, future[0][1], 1000, future[0][6]]
    decision = rows[-1][6]
    result = bl.run_build({"BTCUSDT": {"15m": rows + future}}, {}, _cfg(start_ms=decision, end_ms=decision, label_horizon_bars=10))
    labels = [r.label for r in result.library.rows if r.label.setup_family == "TREND_PULLBACK_V2"]
    assert labels and labels[0].terminal_outcome == "STOP_BEFORE_TARGET"
    assert "SAME_BAR_CONSERVATIVE_STOP_ASSUMED" in labels[0].reason_codes


def test_costs_preserved_separately_and_zero_cost_refused_unless_explicit(built, hist_db):
    for r in built["result"].library.rows:
        l = r.label
        assert l.fee_R > 0 and l.spread_R > 0 and l.slippage_R > 0
        assert l.total_cost_R == pytest.approx(l.fee_R + l.spread_R + l.slippage_R + l.funding_R + l.carry_R)
    assert built["result"].provenance["cost_source_quality"] == "MODELED_RESEARCH_ASSUMPTION"
    with pytest.raises(bl.BuildError):
        bl.run_build(built["series"], built["meta"], _cfg(cost_model_name="zero_gross_only"))
    gross = bl.run_build(built["series"], built["meta"], _cfg(cost_model_name="zero_gross_only", allow_gross_only=True, end_ms=START + 10 * TF_MS))
    assert gross.provenance["cost_source_quality"] == "GROSS_ONLY"


# -- determinism / identity ---------------------------------------------------------------
def test_row_manifest_and_library_identity_are_deterministic(built, hist_db, tmp_path):
    cfg = built["cfg"]
    series, meta, info = _load(hist_db, cfg)
    path2, result2 = bl.build_and_write(series, meta, cfg, str(tmp_path), source_info=info)
    r1, r2 = built["result"], result2
    assert [x.label.label_id for x in r1.library.rows] == [x.label.label_id for x in r2.library.rows]
    assert r1.library.library_hash == r2.library.library_hash
    assert path2.name == built["path"].name
    m1 = json.loads((built["path"] / MANIFEST_FILE).read_text())
    m2 = json.loads((path2 / MANIFEST_FILE).read_text())
    assert m1["manifest_hash"] == m2["manifest_hash"]


def test_changed_data_or_versions_change_identity(built, hist_db):
    other = _cfg(label_horizon_bars=12)
    series, meta, _ = _load(hist_db, other)
    assert bl.run_build(series, meta, other).library.library_hash != built["result"].library.library_hash


def test_artifact_never_overwrites_a_different_library(built, tmp_path):
    from app.trading_intelligence.forecast.artifact import write_library_artifact

    lib, prov = built["result"].library, built["result"].provenance
    p = write_library_artifact(lib, tmp_path, provenance=prov)
    assert write_library_artifact(lib, tmp_path, provenance=prov) == p  # identical => idempotent
    (p / ROWS_FILE).write_text("tampered")  # different content under the same identity
    with pytest.raises(LibraryArtifactError, match="refusing to overwrite"):
        write_library_artifact(lib, tmp_path, provenance=prov)


# -- verified loader ------------------------------------------------------------------------
def _copy(built, tmp_path):
    import shutil
    dst = tmp_path / "copy"
    shutil.copytree(built["path"], dst)
    return dst


def test_loader_round_trips_and_accepts_expected_hash(built):
    lib, manifest = load_library_artifact(built["path"], expected_hash=built["result"].library.library_hash, mode="TEST")
    assert lib.library_hash == built["result"].library.library_hash
    assert manifest["labeled_count"] == len(lib.rows)


def test_loader_refuses_expected_hash_mismatch(built):
    with pytest.raises(LibraryArtifactError):
        load_library_artifact(built["path"], expected_hash="0" * 64, mode="TEST")


def test_loader_refuses_tampered_rows(built, tmp_path):
    d = _copy(built, tmp_path)
    text = (d / ROWS_FILE).read_text()
    (d / ROWS_FILE).write_text(text.replace('"net_profitable":true', '"net_profitable":false', 1), newline="\n")
    with pytest.raises(LibraryArtifactError, match="rows hash mismatch"):
        load_library_artifact(d, mode="TEST")


def test_loader_refuses_tampered_manifest(built, tmp_path):
    d = _copy(built, tmp_path)
    m = json.loads((d / MANIFEST_FILE).read_text())
    m["labeled_count"] += 1
    (d / MANIFEST_FILE).write_text(json.dumps(m))
    with pytest.raises(LibraryArtifactError, match="manifest hash mismatch"):
        load_library_artifact(d, mode="TEST")


def test_loader_refuses_incompatible_schema_even_with_valid_manifest_hash(built, tmp_path):
    d = _copy(built, tmp_path)
    m = json.loads((d / MANIFEST_FILE).read_text())
    m["label_policy_version"] = "999.0.0"
    m["manifest_hash"] = manifest_hash_of(m)
    (d / MANIFEST_FILE).write_text(json.dumps(m))
    with pytest.raises(LibraryArtifactError, match="incompatible"):
        load_library_artifact(d, mode="TEST")


def test_loader_refuses_missing_required_field(built, tmp_path):
    d = _copy(built, tmp_path)
    m = json.loads((d / MANIFEST_FILE).read_text())
    del m["cohort_schema_version"]
    (d / MANIFEST_FILE).write_text(json.dumps(m))
    with pytest.raises(LibraryArtifactError, match="missing required fields"):
        load_library_artifact(d, mode="TEST")


# -- configured loading / fail closed ---------------------------------------------------------
def test_explicit_configured_library_loads_and_unconfigured_fails_closed(built, monkeypatch):
    from app.trading_intelligence.config import (
        ENV_LIBRARY_EXPECTED_HASH, ENV_LIBRARY_MODE, ENV_LIBRARY_PATH, load_configured_library,
    )

    monkeypatch.setenv(ENV_LIBRARY_MODE, "TEST")  # synthetic library: explicit test override
    monkeypatch.delenv(ENV_LIBRARY_PATH, raising=False)
    assert load_configured_library() == (None, None)
    monkeypatch.setenv(ENV_LIBRARY_PATH, str(built["path"]))
    monkeypatch.setenv(ENV_LIBRARY_EXPECTED_HASH, built["result"].library.library_hash)
    lib, _ = load_configured_library()
    assert lib is not None and lib.library_hash == built["result"].library.library_hash
    monkeypatch.setenv(ENV_LIBRARY_EXPECTED_HASH, "f" * 64)
    assert load_configured_library() == (None, None)  # hash mismatch => refused, not best-effort


def test_no_library_means_outcome_library_unavailable():
    from app.trading_intelligence.forecast.engine import build_outcome_forecast
    from _helpers import candidate_for, flat_rows, market_state_and_regime

    ms, regime = market_state_and_regime(flat_rows(5))
    fc = build_outcome_forecast(candidate_for(ms), ms, regime, None)
    assert fc.status == "OUTCOME_LIBRARY_UNAVAILABLE"


# -- calibration ---------------------------------------------------------------------------------
def test_new_library_is_never_calibrated_by_default(built):
    lib, _ = load_library_artifact(built["path"], mode="TEST")
    assert lib.calibration_status == CalibrationStatus.UNCALIBRATED.value


def test_calibration_report_deterministic_and_status_policy_controlled(built):
    lib, manifest = load_library_artifact(built["path"], mode="TEST")
    embargo = manifest["label_horizon_bars"] * TF_MS
    r1 = evaluate_library_calibration(lib, embargo_ms=embargo)
    r2 = evaluate_library_calibration(lib, embargo_ms=embargo)
    assert r1.to_dict() == r2.to_dict()
    assert r1.n_evaluated > 0 and r1.brier_score is not None and r1.ece is not None
    assert len(r1.reliability) == CalibrationPolicy().reliability_bins
    assert set(r1.class_frequency) == {"TARGET_BEFORE_STOP", "STOP_BEFORE_TARGET", "TIMEOUT"}
    strict = CalibrationPolicy(min_calibrated_samples=10**9)
    assert derive_status(r1, strict) != "CALIBRATED"
    assert derive_status(r1, CalibrationPolicy(min_research_samples=10**9)) == "UNCALIBRATED"


def test_stored_calibration_cannot_simply_assert_calibrated(built):
    lib, manifest = load_library_artifact(built["path"], mode="TEST")
    policy = CalibrationPolicy(min_calibrated_samples=10**9)
    report = evaluate_library_calibration(lib, embargo_ms=manifest["label_horizon_bars"] * TF_MS, policy=policy)
    record = build_calibration_record(report, policy)
    record["status"] = "CALIBRATED"  # a bare assertion in the file
    assert status_from_stored_record(record, library_hash=lib.library_hash) != "CALIBRATED"
    assert status_from_stored_record(record, library_hash="wrong") == "UNCALIBRATED"
    record["policy"]["max_ece"] = 1.0  # editing the policy breaks its hash
    assert status_from_stored_record(record, library_hash=lib.library_hash) == "UNCALIBRATED"


def test_small_smoke_build_via_cli_is_reproducible(hist_db, tmp_path):
    args = [sys.executable, "-m", "app.trading_intelligence.forecast.build_library", "--db", str(hist_db),
            "--symbols", "BTCUSDT", "--timeframe", "15m", "--start", str(START), "--end", str(START + 30 * TF_MS),
            "--label-horizon-bars", "24", "--data-source", "synthetic_test_wave"]
    outs = []
    for name in ("one", "two"):
        proc = subprocess.run(args + ["--output", str(tmp_path / name)], cwd=BACKEND, capture_output=True, text=True, timeout=300)
        assert proc.returncode == 0, proc.stderr
        outs.append(json.loads(proc.stdout)["library_hash"])
    assert outs[0] == outs[1]
