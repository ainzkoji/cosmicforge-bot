"""Certification-scale libraries are written, hashed and loaded STREAMING with shared immutable values -- and
every byte and hash is identical to the original one-shot formulas (no identity changes)."""
from __future__ import annotations

import json

import pytest
from _helpers import HIST_START_MS, TF_MS, make_historical_db

from app.trading_intelligence.forecast import build_library as bl
from app.trading_intelligence.forecast.artifact import (
    MANIFEST_FILE, ROWS_FILE, LibraryArtifactError, load_library_artifact, row_to_dict, write_library_artifact,
)
from app.trading_intelligence.forecast.library import compact_rows
from app.trading_intelligence.hashing import stable_hash


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    db = make_historical_db(tmp_path_factory.mktemp("h") / "h.db", symbols=("BTCUSDT", "ETHUSDT"), n_bars=560)
    cfg = bl.BuildConfig(symbols=("BTCUSDT", "ETHUSDT"), timeframe="15m", start_ms=HIST_START_MS + 260 * TF_MS,
                         end_ms=HIST_START_MS + 400 * TF_MS, label_horizon_bars=24, source_kind="SYNTHETIC_TEST",
                         source_provider="synthetic_test_wave")
    s, m, info = bl.load_series_from_db(str(db), cfg.symbols, cfg.timeframe, cfg.start_ms, cfg.end_ms,
                                        label_horizon_bars=cfg.label_horizon_bars, warmup_bars=cfg.snapshot_limit)
    out = tmp_path_factory.mktemp("libs")
    path, result = bl.build_and_write(s, m, cfg, str(out), source_info=info)
    return path, result


def _one_shot_text(rows):  # the ORIGINAL rows-file formula
    return "".join(json.dumps(row_to_dict(r), sort_keys=True, separators=(",", ":"), default=str) + "\n"
                   for r in sorted(rows, key=lambda r: r.label.label_id))


def test_streamed_file_and_hashes_equal_the_original_formulas(built):
    path, result = built
    lib = result.library
    assert lib.rows
    text = (path / ROWS_FILE).read_text(encoding="utf-8")
    assert text == _one_shot_text(lib.rows)
    manifest = json.loads((path / MANIFEST_FILE).read_text(encoding="utf-8"))
    assert manifest["rows_sha256"] == stable_hash(text)
    original_content = stable_hash(sorted((row_to_dict(r) for r in lib.rows), key=lambda d: d["label"]["label_id"]))
    assert lib.rows_content_hash == original_content


def test_shared_values_do_not_change_content_or_identity(built):
    _path, result = built
    rows = result.library.rows
    compact = compact_rows(rows)
    assert [row_to_dict(r) for r in compact] == [row_to_dict(r) for r in rows]
    shared = {id(r.cohort_dimensions) for r in compact}
    assert len(shared) < len(compact)  # identical cohorts share one mapping
    loaded, manifest = load_library_artifact(_path, mode="TEST")
    assert loaded.library_hash == result.library.library_hash == manifest["library_hash"]


def test_rewrite_is_idempotent_and_tampering_is_still_detected(built, tmp_path):
    path, result = built
    assert write_library_artifact(result.library, path.parent, provenance=_provenance(path)) == path
    import shutil

    copy = tmp_path / path.name
    shutil.copytree(path, copy)
    rows = (copy / ROWS_FILE).read_text(encoding="utf-8").replace('"gross_R":', '"gross_R": ', 1)
    (copy / ROWS_FILE).write_text(rows, encoding="utf-8", newline="\n")
    with pytest.raises(LibraryArtifactError, match="rows hash mismatch"):
        load_library_artifact(copy, mode="TEST")


def _provenance(path):
    m = json.loads((path / MANIFEST_FILE).read_text(encoding="utf-8"))
    return {k: v for k, v in m.items() if k != "manifest_hash"}
