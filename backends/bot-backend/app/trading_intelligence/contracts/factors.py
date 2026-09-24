"""Asset-class-neutral factor contracts (pre-Section-17 closure, item 4).

ONE factor architecture for every asset class. What differs between crypto,
FX and futures is CONFIGURATION (a ``FactorSet`` per asset class), never
engine semantics:

* ``STATISTICAL_BETA``        -- EWMA beta to a reference series (crypto
                                 BTC/ETH today; an equity-index future later).
* ``STRUCTURAL_CURRENCY_LEG`` -- exposure implied by the instrument's
                                 canonical base/quote currencies (FX): no
                                 history needed, so concentration is visible
                                 even when pairwise correlation data is not.
* ``STATIC_MEMBERSHIP``       -- configured membership (e.g. energy futures
                                 roots), used when no validated series exists.

Factor ids are namespaced ``<ASSET_CLASS>:<FACTOR_TYPE>:<REFERENCE>`` and
every exposure keeps its asset class, source and version, so "USD" in an FX
currency factor is never treated as the same identifier as a USD-related
factor defined for another asset class.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional, Tuple

from app.trading_intelligence.contracts.instrument import CRYPTO, FUTURES, FX
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import FACTOR_POLICY_VERSION, FACTOR_SCHEMA_VERSION


class FactorType(str, Enum):
    MARKET = "MARKET"
    ALT_MARKET = "ALT_MARKET"
    CURRENCY = "CURRENCY"
    SECTOR = "SECTOR"
    EQUITY_INDEX = "EQUITY_INDEX"
    RATES = "RATES"
    ENERGY = "ENERGY"
    METALS = "METALS"
    AGRICULTURE = "AGRICULTURE"
    COMMODITY_BROAD = "COMMODITY_BROAD"
    OTHER = "OTHER"


class FactorMethod(str, Enum):
    STATISTICAL_BETA = "STATISTICAL_BETA"
    STRUCTURAL_CURRENCY_LEG = "STRUCTURAL_CURRENCY_LEG"
    STATIC_MEMBERSHIP = "STATIC_MEMBERSHIP"


def make_factor_id(asset_class: str, factor_type: str, reference: str) -> str:
    return f"{str(asset_class).upper()}:{str(factor_type).upper()}:{str(reference).upper()}"


@dataclass(frozen=True)
class FactorDefinition:
    factor_id: str
    asset_class: str
    factor_type: str  # FactorType
    method: str  # FactorMethod
    #: STATISTICAL_BETA: reference venue symbol whose returns ARE the factor.
    canonical_reference: Optional[str]
    group: str
    source: str
    #: STATIC_MEMBERSHIP: canonical base assets that load 1.0 on this factor.
    members: Tuple[str, ...] = ()
    #: Per-factor concentration tolerance (units); None = the policy default.
    tolerance_units: Optional[float] = None
    version: str = FACTOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.factor_type not in {t.value for t in FactorType}:
            raise ValueError(f"unknown factor_type {self.factor_type!r}")
        if self.method not in {m.value for m in FactorMethod}:
            raise ValueError(f"unknown factor method {self.method!r}")
        if not self.factor_id.startswith(f"{self.asset_class}:"):
            raise ValueError("factor_id must be namespaced by its asset class")
        if self.method == FactorMethod.STATISTICAL_BETA.value and not self.canonical_reference:
            raise ValueError("a STATISTICAL_BETA factor needs a canonical_reference series")


@dataclass(frozen=True)
class FactorExposure:
    factor_id: str
    asset_class: str
    factor_type: str
    exposure_value: float  # signed, PRE_SIZE_EXPOSURE_PROXY units
    beta: Optional[float]  # STATISTICAL_BETA only; None = unknown (never 0)
    quality: str  # OK | INSUFFICIENT_HISTORY | ZERO_VARIANCE | FACTOR_MISSING | STRUCTURAL | STATIC
    sample_size: int
    fallback_used: bool
    reason_codes: Tuple[str, ...] = ()
    source: str = ""
    version: str = FACTOR_SCHEMA_VERSION


@dataclass(frozen=True)
class FactorSet:
    asset_class: str
    definitions: Tuple[FactorDefinition, ...] = ()
    #: Decompose each instrument into signed base/quote CURRENCY exposures.
    currency_decomposition: bool = False
    currency_factor_source: str = "instrument_key.base_quote"
    currency_tolerance_units: Optional[float] = None
    policy_version: str = FACTOR_POLICY_VERSION

    def __post_init__(self) -> None:
        for d in self.definitions:
            if d.asset_class != self.asset_class:
                raise ValueError(f"factor {d.factor_id} belongs to {d.asset_class}, not {self.asset_class}")

    @property
    def policy_hash(self) -> str:
        return stable_hash(asdict(self))


def default_factor_sets() -> Tuple[FactorSet, ...]:
    """Versioned DEFAULT configuration (research defaults, not certified).

    * CRYPTO  -- BTC broad-market factor + ETH alt-market factor (statistical).
    * FX      -- structural currency-leg decomposition (any currency).
    * FUTURES -- no factor defined: no validated factor data exists yet, so
                 futures rely on the static-group fallback until a series is
                 configured (e.g. FUTURES:EQUITY_INDEX:ES, FUTURES:ENERGY:*).
    """
    crypto = FactorSet(asset_class=CRYPTO, definitions=(
        FactorDefinition(make_factor_id(CRYPTO, FactorType.MARKET.value, "BTC"), CRYPTO, FactorType.MARKET.value,
                         FactorMethod.STATISTICAL_BETA.value, "BTCUSDT", "CRYPTO_MARKET", "config:crypto_default"),
        FactorDefinition(make_factor_id(CRYPTO, FactorType.ALT_MARKET.value, "ETH"), CRYPTO, FactorType.ALT_MARKET.value,
                         FactorMethod.STATISTICAL_BETA.value, "ETHUSDT", "CRYPTO_ALT", "config:crypto_default"),
    ))
    fx = FactorSet(asset_class=FX, currency_decomposition=True, currency_tolerance_units=1.0)
    futures = FactorSet(asset_class=FUTURES)
    return (crypto, fx, futures)


__all__ = [
    "default_factor_sets", "FactorType", "FactorMethod", "make_factor_id", "FactorDefinition", "FactorExposure", "FactorSet",
]
