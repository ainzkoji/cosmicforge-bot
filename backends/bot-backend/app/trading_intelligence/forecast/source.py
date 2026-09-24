"""Historical-library SOURCE classification (pre-Section-17 closure, item 11).

A library is only evidence if it was built from real market history. This
module is the single place that decides what a library's source IS and which
sources the runtime may trust:

* ``REAL_MARKET``     -- candles recorded from a real venue/provider
* ``REPLAY_CAPTURE``  -- real market data captured by the replay tooling
* ``SYNTHETIC_TEST``  -- generated series (tests, smoke builds)
* ``FIXTURE_TEST``    -- hand-made fixtures
* ``UNKNOWN``         -- provenance cannot be established

Runtime CATI trusts only REAL_MARKET / REPLAY_CAPTURE. SYNTHETIC_TEST,
FIXTURE_TEST and UNKNOWN load only under an explicit TEST/DEVELOPMENT mode.
``source_kind`` is part of the library hash and the manifest hash, so it
cannot be edited without breaking verification; a builder can DOWNGRADE a
classification (declare real data as a test fixture) but can never UPGRADE
non-real data to REAL_MARKET.
"""
from __future__ import annotations

from enum import Enum
from typing import FrozenSet, Iterable, Optional

from app.trading_intelligence.versions import LIBRARY_SOURCE_POLICY_VERSION


class HistoricalLibrarySourceKind(str, Enum):
    REAL_MARKET = "REAL_MARKET"
    SYNTHETIC_TEST = "SYNTHETIC_TEST"
    FIXTURE_TEST = "FIXTURE_TEST"
    REPLAY_CAPTURE = "REPLAY_CAPTURE"
    UNKNOWN = "UNKNOWN"


class LibraryLoadMode(str, Enum):
    RUNTIME = "RUNTIME"
    TEST = "TEST"
    DEVELOPMENT = "DEVELOPMENT"


SK = HistoricalLibrarySourceKind

#: Sources a normal CATI runtime may treat as evidence.
RUNTIME_TRUSTED_SOURCE_KINDS: FrozenSet[str] = frozenset({SK.REAL_MARKET.value, SK.REPLAY_CAPTURE.value})
_ALL_KINDS: FrozenSet[str] = frozenset(k.value for k in SK)

#: ``historical_candles.data_source`` values written by the repo's real
#: market-data importers/brokers. Versioned policy data, not engine logic;
#: extend it when a new real importer is added.
REAL_MARKET_PROVIDERS: FrozenSet[str] = frozenset({
    "binance", "bybit", "bingx", "oanda", "ibkr", "ibkr_tws", "mt5", "mt_bridge",
})

SOURCE_POLICY_VERSION = LIBRARY_SOURCE_POLICY_VERSION


class SourceClassificationError(ValueError):
    """A declared source kind contradicts the data it describes."""


def allowed_source_kinds(mode: str) -> FrozenSet[str]:
    """RUNTIME -> real sources only; TEST/DEVELOPMENT -> anything (explicit override)."""
    m = str(mode or "").upper()
    if m == LibraryLoadMode.RUNTIME.value:
        return RUNTIME_TRUSTED_SOURCE_KINDS
    if m in (LibraryLoadMode.TEST.value, LibraryLoadMode.DEVELOPMENT.value):
        return _ALL_KINDS
    return frozenset()  # unknown mode: trust nothing


def _classify_one(source: str) -> str:
    s = str(source or "").strip().lower()
    if not s:
        return SK.UNKNOWN.value
    if s in REAL_MARKET_PROVIDERS:
        return SK.REAL_MARKET.value
    if s.startswith("synthetic") or "synthetic" in s:
        return SK.SYNTHETIC_TEST.value
    if s.startswith("fixture") or "fixture" in s:
        return SK.FIXTURE_TEST.value
    if s.startswith("replay_capture"):
        return SK.REPLAY_CAPTURE.value
    return SK.UNKNOWN.value


def classify_data_sources(sources: Iterable[str]) -> str:
    """One kind for a whole dataset. Mixed or unrecognised provenance is
    never promoted: any non-real member makes the dataset non-real, and the
    weakest kind wins (UNKNOWN < FIXTURE < SYNTHETIC)."""
    kinds = {_classify_one(s) for s in sources}
    if not kinds:
        return SK.UNKNOWN.value
    if kinds == {SK.REPLAY_CAPTURE.value}:
        return SK.REPLAY_CAPTURE.value
    if kinds <= RUNTIME_TRUSTED_SOURCE_KINDS:
        return SK.REAL_MARKET.value
    for weakest in (SK.UNKNOWN.value, SK.FIXTURE_TEST.value, SK.SYNTHETIC_TEST.value):
        if weakest in kinds:
            return weakest
    return SK.UNKNOWN.value  # pragma: no cover - exhaustive above


def resolve_source_kind(declared: Optional[str], derived: str) -> str:
    """Declared may equal the derived kind or DOWNGRADE it to a non-trusted
    kind; it may never upgrade non-real data into a trusted kind."""
    if declared is None:
        return derived
    d = str(declared).upper()
    if d not in _ALL_KINDS:
        raise SourceClassificationError(f"unknown source kind {declared!r}")
    if d == derived:
        return d
    if d not in RUNTIME_TRUSTED_SOURCE_KINDS:
        return d  # downgrade (e.g. real candles declared as a test fixture)
    raise SourceClassificationError(
        f"cannot declare {d}: the data's provenance classifies as {derived} (no upgrade to trusted evidence)")


__all__ = [
    "HistoricalLibrarySourceKind", "LibraryLoadMode", "RUNTIME_TRUSTED_SOURCE_KINDS", "REAL_MARKET_PROVIDERS",
    "SOURCE_POLICY_VERSION", "SourceClassificationError", "allowed_source_kinds", "classify_data_sources",
    "resolve_source_kind",
]
