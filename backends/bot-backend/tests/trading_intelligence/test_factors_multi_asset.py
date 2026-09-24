"""Section 11.3 (closure matrix) -- asset-class-neutral factors, FX currency
legs, futures generic factors, cross-asset isolation, correlation hardening."""
from __future__ import annotations

import ast
import dataclasses
import inspect
from pathlib import Path

import pytest
from _pf import BAR, NOW, T0, context, fut, fx, gen, held, ranked, rows_from_returns, select

from app.trading_intelligence.contracts.factors import (
    FactorDefinition, FactorSet, default_factor_sets, make_factor_id,
)
from app.trading_intelligence.contracts.instrument import CRYPTO, FUTURES, FX
from app.trading_intelligence.contracts.portfolio_intel import PortfolioPolicy
from app.trading_intelligence.portfolio import returns as R
from app.trading_intelligence.portfolio.factors import FactorModel, net_exposures

APP = Path(__file__).resolve().parents[2] / "app" / "trading_intelligence"


def _model(policy=None):
    return FactorModel.from_policy(policy or PortfolioPolicy())


def _exp(symbol, side, key, ctx=None, policy=None):
    ctx = ctx or context(policy)
    return {e.factor_id: e.exposure_value for e in _model(policy).exposures(symbol, key, 1 if side == "LONG" else -1, ctx)}


# -- CRYPTO: configurable, no hardcoded engine dependency -----------------------------------------
def test_crypto_btc_and_eth_factors_are_configuration():
    crypto = next(fs for fs in default_factor_sets() if fs.asset_class == CRYPTO)
    ids = {d.factor_id for d in crypto.definitions}
    assert ids == {"CRYPTO:MARKET:BTC", "CRYPTO:ALT_MARKET:ETH"}
    assert {d.canonical_reference for d in crypto.definitions} == {"BTCUSDT", "ETHUSDT"}


def test_crypto_factor_set_is_replaceable_without_engine_change():
    custom = FactorSet(asset_class=CRYPTO, definitions=(
        FactorDefinition("CRYPTO:MARKET:SOL", CRYPTO, "MARKET", "STATISTICAL_BETA", "SOLUSDT", "ALT", "config:test"),))
    p = dataclasses.replace(PortfolioPolicy(), factor_sets=(custom,))
    rows = {"SOLUSDT": rows_from_returns(gen(1))}
    from app.trading_intelligence.portfolio.context import build_portfolio_market_context
    ctx = build_portfolio_market_context({"AVAXUSDT": rows_from_returns(gen(2))}, NOW, p, factor_rows=rows)
    assert set(ctx.factor_histories) == {"CRYPTO:MARKET:SOL"}
    from _helpers import instrument
    exps = FactorModel.from_policy(p).exposures("AVAXUSDT", instrument("AVAXUSDT"), 1, ctx)
    assert [e.factor_id for e in exps] == ["CRYPTO:MARKET:SOL"]


def test_engine_and_selector_source_name_no_specific_factor_or_symbol():
    for rel in ("portfolio/factors.py", "portfolio/selector.py", "portfolio/returns.py", "portfolio/service.py",
                "portfolio/reservation_store.py", "portfolio/exposure_builder.py"):
        tree = ast.parse((APP / rel).read_text(encoding="utf-8"))
        docstrings = {id(n.value) for n in ast.walk(tree) if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant)}
        literals = {n.value for n in ast.walk(tree)
                    if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings}
        assert not any(tok in lit for lit in literals for tok in ("BTC", "ETH", "USDT")), rel


# -- FX: currency legs ---------------------------------------------------------------------
@pytest.mark.parametrize("symbol,side,expected", [
    ("EURUSD", "LONG", {"FX:CURRENCY:EUR": 1.0, "FX:CURRENCY:USD": -1.0}),
    ("EURUSD", "SHORT", {"FX:CURRENCY:EUR": -1.0, "FX:CURRENCY:USD": 1.0}),
    ("USDJPY", "LONG", {"FX:CURRENCY:USD": 1.0, "FX:CURRENCY:JPY": -1.0}),
    ("GBPJPY", "SHORT", {"FX:CURRENCY:GBP": -1.0, "FX:CURRENCY:JPY": 1.0}),
])
def test_fx_currency_leg_decomposition(symbol, side, expected):
    assert _exp(symbol, side, fx(symbol)) == expected


def test_fx_legs_come_from_instrument_key_not_symbol_string():
    key = dataclasses.replace(fx("EURUSD"), venue_symbol="WEIRD_ALIAS_1")
    assert _exp("WEIRD_ALIAS_1", "LONG", key) == {"FX:CURRENCY:EUR": 1.0, "FX:CURRENCY:USD": -1.0}


def test_any_currency_code_works_not_just_majors():
    assert _exp("USDTRY", "LONG", fx("USDTRY")) == {"FX:CURRENCY:USD": 1.0, "FX:CURRENCY:TRY": -1.0}


def _fx_net(pairs, policy=None):
    ctx = context(policy)
    out = []
    for sym, side in pairs:
        out += _model(policy).exposures(sym, fx(sym), 1 if side == "LONG" else -1, ctx)
    return net_exposures(out)


def test_shared_short_usd_detected():
    net = _fx_net([("EURUSD", "LONG"), ("GBPUSD", "LONG"), ("AUDUSD", "LONG")])
    assert net["FX:CURRENCY:USD"] == -3.0


def test_shared_long_eur_detected():
    net = _fx_net([("EURUSD", "LONG"), ("EURJPY", "LONG"), ("EURGBP", "LONG")])
    assert net["FX:CURRENCY:EUR"] == 3.0


def test_factor_concentration_works_without_pairwise_correlation_data():
    ctx = context()  # NO return history at all
    cands = [ranked("EURUSD", 1.0, pos=1), ranked("GBPUSD", 1.0, pos=2), ranked("AUDUSD", 1.0, pos=3)]
    p = dataclasses.replace(PortfolioPolicy(), lambda_corr=0.0, lambda_sector=0.0, lambda_beta=1.0)
    d = select(cands, ctx, slots=3, policy=p)
    unpenalised = select(cands, ctx, slots=3, policy=dataclasses.replace(p, lambda_beta=0.0))
    assert len(unpenalised.selected_opportunity_ids) == 3
    # USD tolerance is 1 unit: 3 pairs score 3-(3-1)=1, 2 pairs 2-(2-1)=1, 1 pair 1-0=1;
    # the documented tie-break prefers fewer positions, so the USD pile-up is avoided.
    assert len(d.selected_opportunity_ids) < 3
    assert d.score_breakdown.factor_penalty < unpenalised.score_breakdown.factor_penalty == 2.0
    assert dict(d.score_breakdown.factor_net_exposures).get("FX:CURRENCY:USD", 0.0) > -3.0


def test_existing_account_usd_exposure_penalises_new_short_usd():
    ctx = context()
    p = dataclasses.replace(PortfolioPolicy(), lambda_corr=0.0, lambda_sector=0.0, lambda_beta=1.0)
    existing = [held("EURUSD", key=fx("EURUSD")), held("AUDUSD", key=fx("AUDUSD"))]
    d = select([ranked("GBPUSD", 0.5, pos=1)], ctx, slots=1, existing=existing, policy=p)
    assert d.selected_opportunity_ids == ()
    assert d.rejected_candidates[0].reason_code == "COMMON_FACTOR_CONCENTRATION"
    hedge = select([ranked("USDJPY", 0.5, pos=1)], ctx, slots=1, existing=existing, policy=p)
    assert hedge.selected_opportunity_ids  # LONG USD offsets the account's short USD


# -- FUTURES: generic configurable factors, asset-class isolation ----------------------------------
def test_futures_generic_factor_ids_work():
    energy = FactorSet(asset_class=FUTURES, definitions=(
        FactorDefinition(make_factor_id(FUTURES, "ENERGY", "BROAD"), FUTURES, "ENERGY", "STATIC_MEMBERSHIP", None,
                         "ENERGY", "config:test", members=("CL", "NG", "HO")),
        FactorDefinition(make_factor_id(FUTURES, "EQUITY_INDEX", "ES"), FUTURES, "EQUITY_INDEX", "STATISTICAL_BETA", "ES",
                         "EQUITY", "config:test"),
    ))
    p = dataclasses.replace(PortfolioPolicy(), factor_sets=(energy,))
    ctx = context(p, factor_rows={"ES": rows_from_returns(gen(5))}, CL=gen(6), NG=gen(7))
    cl = _exp("CL", "LONG", fut("CL"), ctx, p)
    assert cl["FUTURES:ENERGY:BROAD"] == 1.0 and "FUTURES:EQUITY_INDEX:ES" in cl
    ng_short = _exp("NG", "SHORT", fut("NG"), ctx, p)
    assert ng_short["FUTURES:ENERGY:BROAD"] == -1.0


def test_default_futures_set_invents_no_factor():
    fs = next(f for f in default_factor_sets() if f.asset_class == FUTURES)
    assert fs.definitions == () and not fs.currency_decomposition
    assert _exp("CL", "LONG", fut("CL")) == {}


def test_asset_class_factor_isolation():
    ctx = context(factor_rows={"BTCUSDT": rows_from_returns(gen(1))})
    fx_ids = set(_exp("EURUSD", "LONG", fx("EURUSD"), ctx))
    assert all(f.startswith("FX:") for f in fx_ids)
    from _helpers import instrument
    crypto_ids = set(_exp("SOLUSDT", "LONG", instrument("SOLUSDT"), ctx))
    assert all(f.startswith("CRYPTO:") for f in crypto_ids)
    assert not fx_ids & crypto_ids


def test_factor_definition_must_match_its_set_and_namespace():
    with pytest.raises(ValueError):
        FactorSet(asset_class=FX, definitions=(FactorDefinition("CRYPTO:MARKET:BTC", CRYPTO, "MARKET", "STATISTICAL_BETA",
                                                                "BTCUSDT", "g", "s"),))
    with pytest.raises(ValueError):
        FactorDefinition("MARKET:BTC", CRYPTO, "MARKET", "STATISTICAL_BETA", "BTCUSDT", "g", "s")


# -- correlation hardening (FX synthetic histories) ------------------------------------------------
USD_DRIVER = gen(40, scale=0.004)
EURUSD_R = [-u + e for u, e in zip(USD_DRIVER, gen(41, scale=0.0008))]
GBPUSD_R = [-u + e for u, e in zip(USD_DRIVER, gen(42, scale=0.0008))]
USDCHF_R = [u + e for u, e in zip(USD_DRIVER, gen(43, scale=0.0008))]


def test_strongly_related_fx_pairs_have_high_correlation():
    p = PortfolioPolicy()
    ctx = context(p, EURUSD=EURUSD_R, GBPUSD=GBPUSD_R)
    est = R.pair_correlation("EURUSD", "GBPUSD", ctx, p)
    assert est.source == "EWMA" and est.rho_ewma > 0.9 and not est.fallback_used
    assert est.effective_sample_size and est.effective_sample_size > 10 and est.shrinkage_lambda == p.correlation_shrinkage_lambda


def test_inverse_related_exposure_offsets():
    p = PortfolioPolicy()
    ctx = context(p, EURUSD=EURUSD_R, USDCHF=USDCHF_R)
    est = R.pair_correlation("EURUSD", "USDCHF", ctx, p)
    assert est.rho_ewma < -0.9
    assert R.effective_correlation(est, "LONG", "LONG") < 0  # LONG EURUSD + LONG USDCHF offset
    assert R.effective_correlation(est, "LONG", "SHORT") > 0.5  # LONG EURUSD + SHORT USDCHF compound


def test_correlation_is_causal_and_aligned_on_common_timestamps():
    p = PortfolioPolicy()
    a = rows_from_returns(EURUSD_R)
    b = rows_from_returns(GBPUSD_R)[::2]  # b has gaps: every other bar only
    from app.trading_intelligence.portfolio.context import build_portfolio_market_context
    ctx = build_portfolio_market_context({"EURUSD": a, "GBPUSD": b}, T0 + 200 * BAR, p)
    ta = {t for t, _ in ctx.return_histories["EURUSD"]}
    tb = {t for t, _ in ctx.return_histories["GBPUSD"]}
    assert max(ta | tb) <= T0 + 200 * BAR  # no future bars
    x, y = R._overlap(ctx.return_histories["EURUSD"], ctx.return_histories["GBPUSD"], 500, ctx.decision_time)
    assert len(x) == len(ta & tb)  # inner join, no forward fill


def test_insufficient_history_fallback_recorded_never_silent_zero():
    p = PortfolioPolicy()
    ctx = context(p, EURUSD=EURUSD_R[:20], GBPUSD=GBPUSD_R[:20])
    est = R.pair_correlation("EURUSD", "GBPUSD", ctx, p)
    assert est.fallback_used and est.source == "STATIC_GROUP_FALLBACK"
    assert {"STATISTICAL_HISTORY_INSUFFICIENT", "STATIC_CORRELATION_FALLBACK_USED"} <= set(est.reason_codes)
    assert est.rho_shrunk == p.unknown_group_correlation > 0.0  # FX pairs have no static group: conservative


def test_ewma_and_shrinkage_deterministic():
    p = PortfolioPolicy()
    ctx = context(p, EURUSD=EURUSD_R, GBPUSD=GBPUSD_R)
    assert R.pair_correlation("EURUSD", "GBPUSD", ctx, p) == R.pair_correlation("EURUSD", "GBPUSD", ctx, p)
    est = R.pair_correlation("EURUSD", "GBPUSD", ctx, p)
    assert est.rho_shrunk == pytest.approx((1 - p.correlation_shrinkage_lambda) * est.rho_ewma)
