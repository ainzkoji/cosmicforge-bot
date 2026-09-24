"""Causal PortfolioMarketContext builder (Section 16.4).

The ONLY place raw candle rows are turned into return histories. Fetching is
the caller's job (the runner already holds closed candles for every due
symbol); this function is pure and drops every row that closes after
``decision_time`` before forming a single return, so future returns can
never reach the selector.
"""
from __future__ import annotations

from typing import Mapping, Optional, Sequence

from app.trading_intelligence.contracts.portfolio_intel import PortfolioMarketContext, PortfolioPolicy
from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.portfolio.factors import FactorModel, resolve_factor_rows
from app.trading_intelligence.portfolio.groups import static_group_for
from app.trading_intelligence.portfolio.returns import log_returns_from_closes


def build_portfolio_market_context(
    rows_by_symbol: Mapping[str, Optional[Sequence[Sequence]]],
    decision_time: int,
    policy: PortfolioPolicy,
    *,
    factor_rows: Optional[Mapping[str, Optional[Sequence[Sequence]]]] = None,
    asset_classes: Optional[Mapping[str, str]] = None,
) -> PortfolioMarketContext:
    """``factor_rows`` may be keyed by factor id (``CRYPTO:MARKET:BTC``) or by
    a configured factor's reference symbol; unconfigured keys are ignored
    (a factor only exists if the policy's FactorSet defines it)."""
    returns, quality, groups = {}, {}, {}
    for symbol, rows in sorted(rows_by_symbol.items()):
        sym = symbol.upper()
        groups[sym] = static_group_for(sym)
        if not rows:
            quality[sym] = "MISSING"
            continue
        hist = log_returns_from_closes(rows, decision_time)
        returns[sym] = hist
        quality[sym] = "OK" if len(hist) >= policy.minimum_correlation_observations else "INSUFFICIENT_HISTORY"
    factors = {}
    resolved = resolve_factor_rows(dict(factor_rows or {}), FactorModel.from_policy(policy))
    for fid, rows in sorted(resolved.items()):
        if rows:
            factors[fid] = log_returns_from_closes(rows, decision_time)
            quality[f"FACTOR:{fid}"] = "OK" if len(factors[fid]) >= policy.minimum_beta_observations else "INSUFFICIENT_HISTORY"
        else:
            quality[f"FACTOR:{fid}"] = "MISSING"
    ctx = PortfolioMarketContext(
        portfolio_market_context_id="", decision_time=decision_time, return_histories=returns,
        factor_histories=factors, instrument_groups=groups, data_quality=quality,
        instrument_asset_classes={k.upper(): v for k, v in sorted((asset_classes or {}).items())},
    )
    import dataclasses

    return dataclasses.replace(ctx, portfolio_market_context_id=short_id("pmc", {"hash": ctx.context_hash}))


__all__ = ["build_portfolio_market_context"]
