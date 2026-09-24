"""Generic factor engine (pre-Section-17 closure, item 4) -- pure, no fetching.

The engine only knows three METHODS (statistical beta, structural currency
legs, static membership); which factors exist for an asset class is policy
configuration (``default_factor_sets``). BTC and ETH appear only there, as
the crypto configuration -- nothing in this module or the selector names
them.

Currency-leg normalization (FX, documented + deterministic)
-----------------------------------------------------------
One PRE_SIZE unit of ``BASE/QUOTE`` held LONG is +1 unit of BASE and -1 unit
of QUOTE (buying base, financed in quote); SHORT flips both signs:

    LONG EURUSD  -> EUR +1, USD -1        SHORT EURUSD -> EUR -1, USD +1
    LONG USDJPY  -> USD +1, JPY -1        SHORT GBPJPY -> GBP -1, JPY +1

Legs come from ``InstrumentKey.base_asset`` / ``quote_asset`` (canonical
identity), never from re-parsing a venue symbol. Currency codes are not
enumerated anywhere: any currency a pair contains becomes factor
``FX:CURRENCY:<CODE>``.
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.factors import (  # default_factor_sets re-exported
    FactorDefinition, FactorExposure, FactorMethod, FactorSet, FactorType, default_factor_sets, make_factor_id,
)
from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.contracts.portfolio_intel import PortfolioMarketContext
from app.trading_intelligence.portfolio.returns import ewma_beta

BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK = "BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK"
BETA_UNKNOWN_IGNORED = "BETA_UNKNOWN_IGNORED"


class FactorModel:
    """Resolves FactorExposures for one instrument from its OWN asset class's
    FactorSet only (cross-asset isolation)."""

    def __init__(self, factor_sets: Iterable[FactorSet], *, unit_weight: float = 1.0, ewma_lambda: float,
                 max_history_bars: int, variance_floor: float, minimum_beta_observations: int,
                 missing_beta_policy: str, missing_beta_conservative_abs: float, default_tolerance: float) -> None:
        self._sets: Dict[str, FactorSet] = {}
        for fs in factor_sets:
            if fs.asset_class in self._sets:
                raise ValueError(f"duplicate FactorSet for asset class {fs.asset_class}")
            self._sets[fs.asset_class] = fs
        self._unit = unit_weight
        self._lam, self._max_bars, self._floor = ewma_lambda, max_history_bars, variance_floor
        self._min_obs = minimum_beta_observations
        self._missing_policy, self._missing_abs = missing_beta_policy, missing_beta_conservative_abs
        self._default_tol = default_tolerance
        self._tolerance: Dict[str, float] = {}

    @classmethod
    def from_policy(cls, policy) -> "FactorModel":
        return cls(policy.factor_sets, unit_weight=policy.pre_size_unit_weight, ewma_lambda=policy.ewma_lambda,
                   max_history_bars=policy.max_history_bars, variance_floor=policy.variance_floor,
                   minimum_beta_observations=policy.minimum_beta_observations,
                   missing_beta_policy=policy.missing_beta_policy,
                   missing_beta_conservative_abs=policy.missing_beta_conservative_abs,
                   default_tolerance=policy.factor_tolerance_units)

    # -- configuration views ------------------------------------------------------------
    def factor_set(self, asset_class: str) -> Optional[FactorSet]:
        return self._sets.get(asset_class)

    def reference_series(self) -> Dict[str, str]:
        """factor_id -> reference venue symbol for every STATISTICAL_BETA factor."""
        return {d.factor_id: d.canonical_reference for fs in self._sets.values() for d in fs.definitions
                if d.method == FactorMethod.STATISTICAL_BETA.value}

    def tolerance(self, factor_id: str) -> float:
        return self._tolerance.get(factor_id, self._default_tol)

    # -- exposures ---------------------------------------------------------------------------
    def exposures(self, symbol: str, key: InstrumentKey, side_sign: int, ctx: PortfolioMarketContext) -> Tuple[FactorExposure, ...]:
        fs = self._sets.get(key.asset_class)
        if fs is None:
            return ()
        out: List[FactorExposure] = []
        for d in fs.definitions:
            self._tolerance.setdefault(d.factor_id, d.tolerance_units if d.tolerance_units is not None else self._default_tol)
            if d.method == FactorMethod.STATISTICAL_BETA.value:
                exp = self._beta_exposure(d, symbol, side_sign, ctx)
            elif d.method == FactorMethod.STATIC_MEMBERSHIP.value:
                exp = self._static_exposure(d, key, side_sign)
            else:
                exp = None
            if exp is not None:
                out.append(exp)
        if fs.currency_decomposition:
            out.extend(self._currency_exposures(fs, key, side_sign))
        return tuple(out)

    def _beta_exposure(self, d: FactorDefinition, symbol: str, sign: int, ctx: PortfolioMarketContext) -> Optional[FactorExposure]:
        fh = ctx.factor_histories.get(d.factor_id)
        ah = ctx.return_histories.get(symbol)
        n, quality, beta = 0, "FACTOR_MISSING", None
        if symbol.upper() == str(d.canonical_reference).upper() and fh:
            beta, n, quality = 1.0, len(fh), "OK"
        elif fh and ah:
            beta, n, quality = ewma_beta(ah, fh, ewma_lambda=self._lam, max_bars=self._max_bars,
                                         variance_floor=self._floor, min_obs=self._min_obs)
        elif fh:
            quality = "INSUFFICIENT_HISTORY"
        if beta is not None:
            return FactorExposure(d.factor_id, d.asset_class, d.factor_type, self._unit * sign * beta, beta, quality, n,
                                  False, (), d.source)
        if self._missing_policy == "CONSERVATIVE_UNIT":
            # Unknown beta is NOT zero: assume a conservative unit magnitude, recorded.
            return FactorExposure(d.factor_id, d.asset_class, d.factor_type, self._unit * sign * self._missing_abs, None,
                                  quality, n, True, (BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK,), d.source)
        return FactorExposure(d.factor_id, d.asset_class, d.factor_type, 0.0, None, quality, n, True,
                              (BETA_UNKNOWN_IGNORED,), d.source)

    def _static_exposure(self, d: FactorDefinition, key: InstrumentKey, sign: int) -> Optional[FactorExposure]:
        if str(key.base_asset).upper() not in {m.upper() for m in d.members}:
            return None
        return FactorExposure(d.factor_id, d.asset_class, d.factor_type, self._unit * sign, None, "STATIC", 0, False,
                              (), d.source)

    def _currency_exposures(self, fs: FactorSet, key: InstrumentKey, sign: int) -> List[FactorExposure]:
        out = []
        tol = fs.currency_tolerance_units if fs.currency_tolerance_units is not None else self._default_tol
        for code, leg_sign in ((key.base_asset, +1), (key.quote_asset, -1)):
            if not code:
                continue
            fid = make_factor_id(fs.asset_class, FactorType.CURRENCY.value, code)
            self._tolerance.setdefault(fid, tol)
            out.append(FactorExposure(fid, fs.asset_class, FactorType.CURRENCY.value, self._unit * sign * leg_sign,
                                      None, "STRUCTURAL", 0, False, (), fs.currency_factor_source))
        return out


def net_exposures(exposures: Iterable[FactorExposure]) -> Dict[str, float]:
    net: Dict[str, float] = {}
    for e in exposures:
        net[e.factor_id] = net.get(e.factor_id, 0.0) + e.exposure_value
    return net


def resolve_factor_rows(factor_rows: Mapping[str, object], model: FactorModel) -> Dict[str, object]:
    """Accept rows keyed by factor_id OR by a factor's reference symbol."""
    by_ref = {str(ref).upper(): fid for fid, ref in model.reference_series().items()}
    out: Dict[str, object] = {}
    for k, rows in factor_rows.items():
        fid = k if ":" in k else by_ref.get(str(k).upper())
        if fid is not None:
            out[fid] = rows
    return out


__all__ = [
    "BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK", "BETA_UNKNOWN_IGNORED", "default_factor_sets", "FactorModel",
    "net_exposures", "resolve_factor_rows",
]
