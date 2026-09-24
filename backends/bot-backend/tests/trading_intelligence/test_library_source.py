"""Closure items 1 + 13 -- real vs synthetic library separation, determinism,
verified loading and calibration-status integrity."""
from __future__ import annotations

import dataclasses
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from _helpers import HIST_START_MS, TF_MS, make_historical_db

from app.trading_intelligence.config import (
    ENV_LIBRARY_EXPECTED_HASH, ENV_LIBRARY_MODE, ENV_LIBRARY_PATH, LIBRARY_EXPECTED_HASH_REQUIRED,
    load_configured_library_with_reason,
)
from app.trading_intelligence.forecast import build_library as bl
from app.trading_intelligence.forecast.artifact import (
    CALIBRATION_FILE, MANIFEST_FILE, LibraryArtifactError, load_library_artifact, manifest_hash_of,
)
from app.trading_intelligence.forecast.calibration_report import (
    CalibrationPolicy, build_calibration_record, evaluate_library_calibration,
)
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary
from app.trading_intelligence.forecast.source import (
    SourceClassificationError, allowed_source_kinds, classify_data_sources, resolve_source_kind,
)

BACKEND = Path(__file__).resolve().parents[2]
START = HIST_START_MS + 260 * TF_MS
END = HIST_START_MS + 330 * TF_MS


def _cfg(**kw):
    base = dict(symbols=("BTCUSDT",), timeframe="15m", start_ms=START, end_ms=END, label_horizon_bars=24)
    base.update(kw)
    return bl.BuildConfig(**base)


@pytest.fixture(scope="module")
def series(tmp_path_factory):
    db = make_historical_db(tmp_path_factory.mktemp("h") / "h.db", symbols=("BTCUSDT",), n_bars=420)
    cfg = _cfg()
    s, meta, info = bl.load_series_from_db(str(db), cfg.symbols, cfg.timeframe, cfg.start_ms, cfg.end_ms,
                                           label_horizon_bars=cfg.label_horizon_bars, warmup_bars=cfg.snapshot_limit)
    return dict(db=db, series=s, meta=meta, info=info)


def _build(series, out, **cfg_kw):
    info = cfg_kw.pop("source_info", series["info"])
    return bl.build_and_write(series["series"], series["meta"], _cfg(**cfg_kw), str(out), source_info=info)


@pytest.fixture(scope="module")
def synthetic(series, tmp_path_factory):
    return _build(series, tmp_path_factory.mktemp("syn"))


@pytest.fixture(scope="module")
def real_plumbing(series, tmp_path_factory):
    """CLASSIFICATION-PLUMBING fixture only: synthetic prices whose recorded
    provenance says ``binance``. Lives in a tmp dir, exercises only that the
    loader ACCEPTS a REAL_MARKET-classified artifact whose hashes verify."""
    return _build(series, tmp_path_factory.mktemp("real"), source_info={"data_sources": ["binance"]})


# -- classification --------------------------------------------------------------------
def test_classification_rules():
    assert classify_data_sources(["binance"]) == "REAL_MARKET"
    assert classify_data_sources(["synthetic_test_wave"]) == "SYNTHETIC_TEST"
    assert classify_data_sources(["fixture_x"]) == "FIXTURE_TEST"
    assert classify_data_sources(["binance", "synthetic_test_wave"]) == "SYNTHETIC_TEST"  # mixed never promoted
    assert classify_data_sources(["", "binance"]) == "UNKNOWN"  # a NULL provenance row poisons the dataset
    assert classify_data_sources([]) == "UNKNOWN"


def test_declared_kind_can_downgrade_never_upgrade():
    assert resolve_source_kind("FIXTURE_TEST", "REAL_MARKET") == "FIXTURE_TEST"
    with pytest.raises(SourceClassificationError):
        resolve_source_kind("REAL_MARKET", "SYNTHETIC_TEST")
    with pytest.raises(SourceClassificationError):
        resolve_source_kind("REAL_MARKET", "UNKNOWN")


def test_runtime_trusts_only_real_sources():
    assert allowed_source_kinds("RUNTIME") == {"REAL_MARKET", "REPLAY_CAPTURE"}
    assert "SYNTHETIC_TEST" in allowed_source_kinds("TEST")
    assert allowed_source_kinds("bogus") == frozenset()


# -- synthetic vs runtime ----------------------------------------------------------------
def test_synthetic_library_loads_in_test_mode(synthetic):
    path, result = synthetic
    lib, manifest = load_library_artifact(path, mode="TEST")
    assert lib.source_kind == manifest["source_kind"] == "SYNTHETIC_TEST"


def test_synthetic_library_refused_in_normal_runtime(synthetic):
    path, _ = synthetic
    with pytest.raises(LibraryArtifactError, match="SOURCE_KIND_NOT_TRUSTED"):
        load_library_artifact(path)


def test_fixture_and_unknown_libraries_refused_in_runtime(series, tmp_path):
    fixture_path, _ = _build(series, tmp_path / "f", source_kind="FIXTURE_TEST")
    unknown_path, _ = _build(series, tmp_path / "u", source_info={"data_sources": ["some_csv"]})
    for p in (fixture_path, unknown_path):
        with pytest.raises(LibraryArtifactError, match="SOURCE_KIND_NOT_TRUSTED"):
            load_library_artifact(p)


def test_real_market_library_accepted_when_hashes_and_versions_pass(real_plumbing):
    path, result = real_plumbing
    lib, manifest = load_library_artifact(path, expected_hash=result.library.library_hash)
    assert lib.source_kind == manifest["source_kind"] == "REAL_MARKET"
    assert lib.library_hash == manifest["library_hash"] == result.library.library_hash
    for field in ("source_kind", "source_provider", "source_dataset_id", "source_hash", "symbols", "venue", "timeframe",
                  "start_time", "end_time", "row_count", "candidate_count", "label_count"):
        assert field in manifest


def test_cannot_claim_real_market_for_synthetic_data(series, tmp_path):
    with pytest.raises(bl.BuildError, match="no upgrade"):
        _build(series, tmp_path, source_kind="REAL_MARKET")


def test_cli_refuses_declaring_synthetic_candles_real(series, tmp_path):
    proc = subprocess.run(
        [sys.executable, "-m", "app.trading_intelligence.forecast.build_library", "--db", str(series["db"]),
         "--symbols", "BTCUSDT", "--start", str(START), "--end", str(START + 5 * TF_MS), "--label-horizon-bars", "24",
         "--output", str(tmp_path), "--declare-source-kind", "REAL_MARKET"],
        cwd=BACKEND, capture_output=True, text=True, timeout=300)
    assert proc.returncode == 2 and "cannot declare REAL_MARKET" in proc.stderr


def test_synthetic_copied_into_configured_path_is_refused_by_runtime_config(synthetic, tmp_path, monkeypatch):
    path, result = synthetic
    dest = tmp_path / "prod_library"
    shutil.copytree(path, dest)
    monkeypatch.setenv(ENV_LIBRARY_PATH, str(dest))
    monkeypatch.setenv(ENV_LIBRARY_EXPECTED_HASH, result.library.library_hash)
    monkeypatch.delenv(ENV_LIBRARY_MODE, raising=False)  # default = RUNTIME
    lib, _m, reason = load_configured_library_with_reason()
    assert lib is None and "SOURCE_KIND_NOT_TRUSTED" in reason
    monkeypatch.setenv(ENV_LIBRARY_MODE, "TEST")
    lib, _m, reason = load_configured_library_with_reason()
    assert lib is not None and reason is None


def test_runtime_config_requires_expected_hash(real_plumbing, monkeypatch):
    path, _ = real_plumbing
    monkeypatch.setenv(ENV_LIBRARY_PATH, str(path))
    monkeypatch.delenv(ENV_LIBRARY_EXPECTED_HASH, raising=False)
    monkeypatch.delenv(ENV_LIBRARY_MODE, raising=False)
    lib, _m, reason = load_configured_library_with_reason()
    assert lib is None and reason == LIBRARY_EXPECTED_HASH_REQUIRED


# -- tampering / identity --------------------------------------------------------------------
def test_tampered_source_kind_rejected(real_plumbing, tmp_path):
    path, _ = real_plumbing
    d = tmp_path / "t"
    shutil.copytree(path, d)
    m = json.loads((d / MANIFEST_FILE).read_text(encoding="utf-8"))
    m["source_kind"] = "REAL_MARKET" if m["source_kind"] != "REAL_MARKET" else "SYNTHETIC_TEST"
    (d / MANIFEST_FILE).write_text(json.dumps(m), encoding="utf-8")
    with pytest.raises(LibraryArtifactError, match="manifest hash mismatch"):
        load_library_artifact(d, mode="TEST")
    m["manifest_hash"] = manifest_hash_of(m)  # a forger re-hashes the manifest...
    (d / MANIFEST_FILE).write_text(json.dumps(m), encoding="utf-8")
    with pytest.raises(LibraryArtifactError, match="library hash mismatch"):  # ...the recomputed library hash still differs
        load_library_artifact(d, mode="TEST")


def test_source_kind_is_part_of_library_hash(synthetic):
    _, result = synthetic
    lib = result.library
    relabelled = dataclasses.replace(lib, source_kind="REAL_MARKET")
    assert relabelled.library_hash != lib.library_hash


def test_row_field_tampering_rejected(synthetic, tmp_path):
    path, _ = synthetic
    d = tmp_path / "rows"
    shutil.copytree(path, d)
    m = json.loads((d / MANIFEST_FILE).read_text(encoding="utf-8"))
    m["setup_family_versions"] = {k: "0.0.0" for k in m["setup_family_versions"]}
    m["manifest_hash"] = manifest_hash_of(m)
    (d / MANIFEST_FILE).write_text(json.dumps(m), encoding="utf-8")
    with pytest.raises(LibraryArtifactError, match="candidate policy versions"):
        load_library_artifact(d, mode="TEST")


# -- determinism -------------------------------------------------------------------------------
def test_two_identical_builds_identical_manifest_library_and_rows(series, tmp_path):
    p1, r1 = _build(series, tmp_path / "a")
    p2, r2 = _build(series, tmp_path / "b")
    m1 = json.loads((p1 / MANIFEST_FILE).read_text(encoding="utf-8"))
    m2 = json.loads((p2 / MANIFEST_FILE).read_text(encoding="utf-8"))
    assert m1["manifest_hash"] == m2["manifest_hash"] and r1.library.library_hash == r2.library.library_hash
    assert [r.label.label_id for r in r1.library.rows] == [r.label.label_id for r in r2.library.rows]
    assert all(r.label.market_state_id for r in r1.library.rows)


# -- calibration status ------------------------------------------------------------------------
def test_calibration_defaults_by_source_and_never_auto_calibrated(synthetic, real_plumbing):
    syn, _ = load_library_artifact(synthetic[0], mode="TEST")
    real, _ = load_library_artifact(real_plumbing[0])
    assert syn.calibration_status == "UNCALIBRATED"
    assert real.calibration_status == "RESEARCH_ONLY"  # real evidence without a passing report


def test_calibration_status_independently_validated(real_plumbing, tmp_path):
    path, result = real_plumbing
    d = tmp_path / "cal"
    shutil.copytree(path, d)
    lib, manifest = load_library_artifact(d)
    policy = CalibrationPolicy(min_research_samples=1, min_calibrated_samples=10**9)
    report = evaluate_library_calibration(lib, embargo_ms=manifest["label_horizon_bars"] * TF_MS, policy=policy)
    record = build_calibration_record(report, policy)
    record["status"] = "CALIBRATED"  # bare assertion
    (d / CALIBRATION_FILE).write_text(json.dumps(record), encoding="utf-8")
    again, _ = load_library_artifact(d)
    assert again.calibration_status in ("RESEARCH_ONLY", "UNCALIBRATED")
    assert again.calibration_status != "CALIBRATED"
