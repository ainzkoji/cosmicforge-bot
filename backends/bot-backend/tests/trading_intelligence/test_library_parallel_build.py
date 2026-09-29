"""Per-symbol parallel library build is an execution detail: any worker count reproduces the sequential library
(rows, counts, provenance, identity) exactly."""
from __future__ import annotations

import pytest
from _helpers import HIST_START_MS, TF_MS, make_historical_db

from app.trading_intelligence.forecast import build_library as bl

SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")


@pytest.fixture(scope="module")
def series(tmp_path_factory):
    db = make_historical_db(tmp_path_factory.mktemp("hist") / "h.db", symbols=SYMBOLS, n_bars=520)
    cfg = _cfg()
    s, meta, info = bl.load_series_from_db(str(db), cfg.symbols, cfg.timeframe, cfg.start_ms, cfg.end_ms,
                                           label_horizon_bars=cfg.label_horizon_bars, warmup_bars=cfg.snapshot_limit)
    return s, meta, info


def _cfg():
    return bl.BuildConfig(symbols=SYMBOLS, timeframe="15m", start_ms=HIST_START_MS + 260 * TF_MS,
                          end_ms=HIST_START_MS + 380 * TF_MS, label_horizon_bars=24,
                          source_kind="SYNTHETIC_TEST", source_provider="synthetic_test_wave")


def test_parallel_build_equals_sequential_build(series):
    s, meta, info = series
    one = bl.run_build(s, meta, _cfg(), source_info=info, workers=1)
    many = bl.run_build(s, meta, _cfg(), source_info=info, workers=3)
    assert one.library.rows, "the fixture must produce library rows for the comparison to mean anything"
    assert many.library.library_hash == one.library.library_hash
    assert many.library.rows == one.library.rows
    assert many.provenance == one.provenance and many.skipped == one.skipped


def test_worker_count_is_not_part_of_build_identity(monkeypatch, series):
    s, meta, info = series
    monkeypatch.setenv("CATI_BUILD_WORKERS", "2")
    via_env = bl.run_build(s, meta, _cfg(), source_info=info)
    assert via_env.library.library_hash == bl.run_build(s, meta, _cfg(), source_info=info, workers=1).library.library_hash
    assert "workers" not in {f.name for f in bl.BuildConfig.__dataclass_fields__.values()}


# -- governed (runtime) library pins -----------------------------------------------------------------------
from pathlib import Path  # noqa: E402

DOCS = Path(__file__).resolve().parents[4] / "docs" / "research"
UNIVERSE = DOCS / "cati_crypto_universe_binance_v1.json"
DATASET = DOCS / "datasets" / "crypto_broad_binance_v1.dataset.json"
HOLDOUT = 1783876499999  # the reserved crypto holdout start of the certification chronology


def _frozen_cfg(end_ms):
    import json

    u = json.loads(UNIVERSE.read_text(encoding="utf-8"))
    return bl.BuildConfig(symbols=tuple(u["selected_symbols"]), timeframe="15m", start_ms=u["window_start_ms"],
                          end_ms=end_ms, label_horizon_bars=48)


def test_a_governed_build_can_never_read_holdout_candles():
    safe_end = HOLDOUT - 50 * TF_MS
    pins = bl.governance_pins(universe_manifest=str(UNIVERSE), dataset_manifest=str(DATASET),
                              holdout_start_ms=HOLDOUT, cfg=_frozen_cfg(safe_end))
    assert pins["last_candle_read_bound_ms"] < HOLDOUT and pins["dataset_manifest_hash"] and pins["universe_hash"]
    assert "code_commit" in pins and "source_tree_dirty" in pins
    with pytest.raises(bl.BuildError, match="HOLDOUT_OVERLAP"):  # labels would reach into the holdout
        bl.governance_pins(universe_manifest=None, dataset_manifest=None, holdout_start_ms=HOLDOUT,
                           cfg=_frozen_cfg(HOLDOUT - 10 * TF_MS))


def test_governed_build_refuses_other_members_or_a_tampered_dataset(tmp_path):
    import dataclasses
    import json

    cfg = _frozen_cfg(HOLDOUT - 50 * TF_MS)
    with pytest.raises(bl.BuildError, match="frozen universe membership"):
        bl.governance_pins(universe_manifest=str(UNIVERSE), dataset_manifest=None, holdout_start_ms=None,
                           cfg=dataclasses.replace(cfg, symbols=cfg.symbols[:-1]))
    tampered = json.loads(DATASET.read_text(encoding="utf-8"))
    tampered["partitions"][0]["rows"] += 1
    (tmp_path / "ds.json").write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(bl.BuildError, match="dataset manifest refused"):
        bl.governance_pins(universe_manifest=str(UNIVERSE), dataset_manifest=str(tmp_path / "ds.json"),
                           holdout_start_ms=None, cfg=cfg)
    assert bl.governance_pins(universe_manifest=None, dataset_manifest=None, holdout_start_ms=None, cfg=cfg) == {}
