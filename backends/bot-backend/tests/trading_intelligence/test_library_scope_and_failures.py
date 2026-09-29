"""Section A5: a governed outcome library forecasts only its own asset class, and every reason a library cannot
be used is explicit -- never a neutral forecast."""
from __future__ import annotations

import pytest
from test_controller import _trend_pullback_rows

from app.runner.market_snapshot import MarketSnapshot
from app.trading_intelligence import config as C
from app.trading_intelligence.contracts.forecast import ForecastStatus
from app.trading_intelligence.controller.cati_controller import CATIController
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary


def _library():
    return HistoricalOutcomeLibrary.build((), dataset_source_hash="d", candidate_generation_versions={},
                                          label_policy_version="v", cost_model_version="c", source_kind="REAL_MARKET")


def _forecasts(controller, asset_class):
    snap = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=_trend_pullback_rows(), source="Test")
    ev = controller.evaluate_symbol(snapshot=snap, venue="binance", source="Test", asset_class=asset_class)
    assert ev.opportunities, "the fixture must produce candidates"
    return [o.forecast for o in ev.opportunities]


def test_a_crypto_library_never_forecasts_another_asset_class():
    controller = CATIController(outcome_library=_library(), library_scope=("CRYPTO",))
    for f in _forecasts(controller, "EQUITY"):
        assert f.status == ForecastStatus.OUTCOME_LIBRARY_SCOPE_UNAVAILABLE.value and not f.is_usable
        assert f.reason_codes == ("OUTCOME_LIBRARY_SCOPE_UNAVAILABLE",)
    for f in _forecasts(controller, "CRYPTO"):  # in scope: the normal cohort path (empty library -> no support)
        assert f.status != ForecastStatus.OUTCOME_LIBRARY_SCOPE_UNAVAILABLE.value


def test_a_manifest_without_market_type_scopes_to_nothing():
    assert C.library_scope({"market_type": "crypto"}) == ("CRYPTO",)
    assert C.library_scope({}) == () and C.library_scope(None) is None
    controller = CATIController(outcome_library=_library(), library_scope=C.library_scope({}))
    assert all(f.status == ForecastStatus.OUTCOME_LIBRARY_SCOPE_UNAVAILABLE.value for f in _forecasts(controller, "CRYPTO"))


@pytest.mark.parametrize("reason,code", [
    (C.LIBRARY_NOT_CONFIGURED, "OUTCOME_LIBRARY_NOT_CONFIGURED"),
    (C.LIBRARY_EXPECTED_HASH_REQUIRED, "OUTCOME_LIBRARY_NOT_CONFIGURED"),
    (f"{C.LIBRARY_REFUSED}: library hash does not match the configured expected hash (abc...)",
     "OUTCOME_LIBRARY_IDENTITY_MISMATCH"),
    (f"{C.LIBRARY_REFUSED}: manifest hash mismatch (manifest was altered)", "OUTCOME_LIBRARY_IDENTITY_MISMATCH"),
    (f"{C.LIBRARY_REFUSED}: incompatible label_policy_version: artifact='1' supported='2'",
     "OUTCOME_LIBRARY_VERSION_UNSUPPORTED"),
    (f"{C.LIBRARY_REFUSED}: SOURCE_KIND_NOT_TRUSTED: library source_kind=SYNTHETIC_TEST",
     "OUTCOME_LIBRARY_SOURCE_NOT_TRUSTED"),
    (None, None),
])
def test_every_library_failure_has_its_own_reason(reason, code):
    assert C.library_failure_code(reason) == code


def test_missing_library_forecasts_are_unusable_and_say_why():
    controller = CATIController(outcome_library=None, library_unavailable_code="OUTCOME_LIBRARY_IDENTITY_MISMATCH")
    for f in _forecasts(controller, "CRYPTO"):
        assert f.status == ForecastStatus.OUTCOME_LIBRARY_UNAVAILABLE.value and not f.is_usable
        assert f.reason_codes == ("OUTCOME_LIBRARY_UNAVAILABLE", "OUTCOME_LIBRARY_IDENTITY_MISMATCH")


def test_runtime_config_with_a_wrong_expected_hash_is_an_identity_mismatch(tmp_path):
    from _helpers import HIST_START_MS, TF_MS, make_historical_db

    from app.trading_intelligence.forecast import build_library as bl

    db = make_historical_db(tmp_path / "h.db", symbols=("BTCUSDT",), n_bars=560)
    cfg = bl.BuildConfig(symbols=("BTCUSDT",), timeframe="15m", start_ms=HIST_START_MS + 260 * TF_MS,
                         end_ms=HIST_START_MS + 400 * TF_MS, label_horizon_bars=24, source_kind="SYNTHETIC_TEST",
                         source_provider="synthetic_test_wave", market_type="crypto")
    series, meta, info = bl.load_series_from_db(str(db), cfg.symbols, cfg.timeframe, cfg.start_ms, cfg.end_ms,
                                                label_horizon_bars=cfg.label_horizon_bars, warmup_bars=cfg.snapshot_limit)
    path, result = bl.build_and_write(series, meta, cfg, str(tmp_path / "libs"), source_info=info)
    ok = C.CATIConfig(outcome_library_path=str(path), outcome_library_expected_hash=result.library.library_hash,
                      library_mode="TEST")
    _l, manifest, reason = C.load_configured_library_with_reason(ok)
    assert reason is None and C.library_scope(manifest) == ("CRYPTO",)
    bad = C.CATIConfig(outcome_library_path=str(path), outcome_library_expected_hash="0" * 64, library_mode="TEST")
    _l, _m, reason = C.load_configured_library_with_reason(bad)
    assert C.library_failure_code(reason) == "OUTCOME_LIBRARY_IDENTITY_MISMATCH"
    runtime = C.CATIConfig(outcome_library_path=str(path), outcome_library_expected_hash=result.library.library_hash)
    _l, _m, reason = C.load_configured_library_with_reason(runtime)  # synthetic evidence is refused at runtime
    assert C.library_failure_code(reason) == "OUTCOME_LIBRARY_SOURCE_NOT_TRUSTED"
