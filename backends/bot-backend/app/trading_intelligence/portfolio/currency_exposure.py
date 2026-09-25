"""Account-level currency exposure graph (Phase 5C/5D).

``portfolio/factors.py`` decomposes an instrument into UNIT currency legs for
portfolio SELECTION. This module uses the same leg convention (LONG
BASE/QUOTE = +BASE, -QUOTE; SHORT flips) but weighted by NOTIONAL and summed
across every open position, pending reservation and proposed trade of ONE
broker account, so hidden concentration is visible to hard risk:

    LONG EURUSD 10k + LONG EURGBP 10k + LONG EURJPY 10k  -> EUR +30k
    LONG USDJPY + LONG USDCHF + LONG USDCAD               -> USD long x3

Cross-asset: a crypto position contributes its BASE leg (LONG BTC/USDT =
+BTC). Its stablecoin quote leg is the account's settlement/margin currency,
not an FX position, so it is excluded unless ``crypto_quote_legs=True``.
Stablecoins are folded into USD (``stablecoin_as_usd``); the raw codes are
kept in ``by_code_raw`` for audit.

Share limits are only meaningful for a book: they apply once the combined
book holds at least ``min_instruments_for_share`` instruments (a single
position is always 100% of itself). Absolute limits always apply.

Pure and deterministic; an exposure whose notional is unknown is reported in
``unknown_notional`` and never counted as zero.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

STABLECOINS = frozenset({"USDT", "USDC", "FDUSD", "BUSD", "USD1", "TUSD", "DAI"})


@dataclass(frozen=True)
class ExposureItem:
    instrument: str          # canonical symbol
    asset_class: str         # CRYPTO | FX | ...
    base: str
    quote: str
    side: str                # LONG | SHORT
    notional: Optional[float]  # in quote-currency units converted to account currency; None = unknown
    source: str = "POSITION"   # POSITION | RESERVATION | PROPOSED


@dataclass(frozen=True)
class CurrencyExposure:
    by_code: Mapping[str, float]
    by_code_raw: Mapping[str, float]
    gross: float
    by_asset_class: Mapping[str, float]
    unknown_notional: Tuple[str, ...] = ()
    instruments: Tuple[str, ...] = ()

    def share(self, code: str) -> float:
        """|exposure(code)| / gross notional (0 when there is no exposure)."""
        return abs(self.by_code.get(code, 0.0)) / self.gross if self.gross else 0.0


@dataclass(frozen=True)
class ConcentrationLimits:
    max_single_currency_share: float = 0.6     # |net currency| / gross
    max_single_currency_abs: Optional[float] = None
    max_asset_class_share: Mapping[str, float] = field(default_factory=lambda: {"CRYPTO": 1.0, "FX": 1.0})
    ignore_codes: Tuple[str, ...] = ()         # e.g. the account currency, if desired
    min_instruments_for_share: int = 2


def build_exposure(items: Iterable[ExposureItem], *, stablecoin_as_usd: bool = True,
                   crypto_quote_legs: bool = False) -> CurrencyExposure:
    net: Dict[str, float] = {}
    raw: Dict[str, float] = {}
    by_class: Dict[str, float] = {}
    gross = 0.0
    unknown: List[str] = []
    instruments = set()
    for it in items:
        instruments.add(it.instrument)
        if it.notional is None:
            unknown.append(it.instrument)
            continue
        n = abs(float(it.notional))
        sign = 1.0 if it.side.upper() == "LONG" else -1.0
        gross += n
        by_class[it.asset_class] = by_class.get(it.asset_class, 0.0) + n
        for code, leg in ((it.base, +1.0), (it.quote, -1.0)):
            code = str(code or "").upper()
            if not code or (leg < 0 and it.asset_class == "CRYPTO" and not crypto_quote_legs):
                continue
            raw[code] = raw.get(code, 0.0) + sign * leg * n
            key = "USD" if (stablecoin_as_usd and code in STABLECOINS) else code
            net[key] = net.get(key, 0.0) + sign * leg * n
    return CurrencyExposure(by_code=dict(sorted(net.items())), by_code_raw=dict(sorted(raw.items())), gross=gross,
                            by_asset_class=dict(sorted(by_class.items())), unknown_notional=tuple(sorted(unknown)),
                            instruments=tuple(sorted(instruments)))


def check_concentration(current: CurrencyExposure, proposed: CurrencyExposure,
                        limits: ConcentrationLimits = ConcentrationLimits()) -> Tuple[bool, Tuple[str, ...]]:
    """Would adding the proposal breach a limit? Only breaches the proposal
    WORSENS are reported (an existing breach is not blamed on a trade that
    reduces it). Unknown notional anywhere fails closed."""
    if proposed.unknown_notional or current.unknown_notional:
        return False, ("CURRENCY_EXPOSURE_NOTIONAL_UNKNOWN",)
    combined_net = dict(current.by_code)
    for k, v in proposed.by_code.items():
        combined_net[k] = combined_net.get(k, 0.0) + v
    gross = current.gross + proposed.gross
    book_size = len(set(current.instruments) | set(proposed.instruments))
    share_applies = book_size >= limits.min_instruments_for_share
    reasons: List[str] = []
    for code, value in sorted(combined_net.items()):
        if code in limits.ignore_codes or gross <= 0:
            continue
        before = abs(current.by_code.get(code, 0.0))
        after = abs(value)
        if after <= before:
            continue
        if share_applies and after / gross > limits.max_single_currency_share:
            reasons.append(f"CURRENCY_CONCENTRATION:{code}")
        if limits.max_single_currency_abs is not None and after > limits.max_single_currency_abs:
            reasons.append(f"CURRENCY_ABS_LIMIT:{code}")
    for ac, n in proposed.by_asset_class.items():
        cap = limits.max_asset_class_share.get(ac)
        total = current.by_asset_class.get(ac, 0.0) + n
        if share_applies and cap is not None and gross > 0 and total / gross > cap + 1e-12:
            reasons.append(f"ASSET_CLASS_CONCENTRATION:{ac}")
    return (not reasons), tuple(reasons)


__all__ = ["ConcentrationLimits", "CurrencyExposure", "ExposureItem", "STABLECOINS", "build_exposure",
           "check_concentration"]
