"""Dynamic research-universe selection and immutable manifests (Phase 4B/4C/4J).

No permanent symbol list: a universe is SELECTED from the venue's discovered
instruments by explicit, recorded criteria (status, listing age, liquidity,
history availability) and frozen into a content-hashed manifest. Every
research/certification run names the manifest it used.

Roles are distinct. Downloading 150 symbols makes them a RESEARCH_UNIVERSE;
it does not make any of them executable. ``execution_eligibility`` requires
data sufficiency, economics, calibration, liquidity, venue capability,
account capability AND a certification scope -- each missing input is a
reason code.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

RESEARCH, TRAINING, CERTIFICATION, EXECUTION = ("RESEARCH_UNIVERSE", "TRAINING_UNIVERSE", "CERTIFICATION_UNIVERSE",
                                                "EXECUTION_UNIVERSE")
ROLES = (RESEARCH, TRAINING, CERTIFICATION, EXECUTION)
DAY_MS = 86_400_000
SELECTION_RULE_VERSION = "universe-selection-v1"


@dataclass(frozen=True)
class SelectionCriteria:
    asset_class: str = "CRYPTO"
    product_types: Tuple[str, ...] = ("PERPETUAL",)
    quote_assets: Tuple[str, ...] = ("USDT",)
    min_listing_age_days: int = 731          # >= ~2 years of history possible
    min_quote_volume_24h: Optional[float] = 5_000_000.0
    max_spread_bps: Optional[float] = 15.0
    target_size: int = 150
    min_size: int = 100
    require_history_days: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["product_types"], d["quote_assets"] = list(self.product_types), list(self.quote_assets)
        return d


@dataclass(frozen=True)
class UniverseSelection:
    role: str
    venue: str
    asset_class: str
    as_of_ms: int
    criteria: Mapping[str, Any]
    selected: Tuple[str, ...]
    excluded: Mapping[str, str]
    ranking: Tuple[Tuple[str, Optional[float]], ...]
    shortfall: Optional[str] = None
    rule_version: str = SELECTION_RULE_VERSION

    @property
    def manifest_hash(self) -> str:
        body = json.dumps({"role": self.role, "venue": self.venue, "asset_class": self.asset_class,
                           "as_of_ms": self.as_of_ms, "criteria": self.criteria, "selected": list(self.selected),
                           "rule_version": self.rule_version}, sort_keys=True)
        return hashlib.sha256(body.encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {"role": self.role, "venue": self.venue, "asset_class": self.asset_class, "as_of_ms": self.as_of_ms,
                "criteria": dict(self.criteria), "selected": list(self.selected), "excluded": dict(self.excluded),
                "ranking": [list(r) for r in self.ranking], "shortfall": self.shortfall,
                "rule_version": self.rule_version, "manifest_hash": self.manifest_hash}


def select_universe(instruments: Sequence[Any], stats: Mapping[str, Any], *, venue: str, as_of_ms: int,
                    criteria: SelectionCriteria = SelectionCriteria(), role: str = RESEARCH,
                    history_days: Optional[Mapping[str, float]] = None) -> UniverseSelection:
    """``instruments``: DiscoveredInstrument; ``stats``: symbol -> MarketStats-like
    (quote_volume_24h, spread_bps). Unknown liquidity EXCLUDES (never "small")."""
    if role not in ROLES:
        raise ValueError(role)
    excluded: Dict[str, str] = {}
    ranked: List[Tuple[str, float]] = []
    for ins in instruments:
        sym = ins.venue_symbol
        if ins.asset_class != criteria.asset_class:
            excluded[sym] = "ASSET_CLASS"
            continue
        if ins.product_type not in criteria.product_types:
            excluded[sym] = "PRODUCT_TYPE"
            continue
        if criteria.quote_assets and ins.settlement_asset not in criteria.quote_assets:
            excluded[sym] = "QUOTE_ASSET"
            continue
        if not ins.api_tradable:
            excluded[sym] = "NOT_TRADING"
            continue
        if ins.listed_at_ms is None:
            excluded[sym] = "LISTING_TIME_UNKNOWN"
            continue
        if (as_of_ms - ins.listed_at_ms) < criteria.min_listing_age_days * DAY_MS:
            excluded[sym] = "LISTING_TOO_RECENT"
            continue
        st = stats.get(sym)
        qv = getattr(st, "quote_volume_24h", None) if st is not None else None
        spread = getattr(st, "spread_bps", None) if st is not None else None
        if criteria.min_quote_volume_24h is not None:
            if qv is None:
                excluded[sym] = "LIQUIDITY_UNKNOWN"
                continue
            if qv < criteria.min_quote_volume_24h:
                excluded[sym] = "LOW_LIQUIDITY"
                continue
        if criteria.max_spread_bps is not None and spread is not None and spread > criteria.max_spread_bps:
            excluded[sym] = "SPREAD_TOO_WIDE"
            continue
        if criteria.require_history_days is not None:
            have = (history_days or {}).get(sym)
            if have is None or have < criteria.require_history_days:
                excluded[sym] = "INSUFFICIENT_HISTORY"
                continue
        ranked.append((sym, float(qv) if qv is not None else 0.0))
    ranked.sort(key=lambda x: (-x[1], x[0]))  # deterministic: liquidity desc, symbol asc
    chosen = ranked[: criteria.target_size]
    for sym, _ in ranked[criteria.target_size:]:
        excluded[sym] = "RANK_BELOW_TARGET"
    shortfall = None if len(chosen) >= criteria.min_size else f"ONLY_{len(chosen)}_OF_MIN_{criteria.min_size}"
    return UniverseSelection(role=role, venue=venue, asset_class=criteria.asset_class, as_of_ms=as_of_ms,
                             criteria=criteria.to_dict(), selected=tuple(s for s, _ in chosen),
                             excluded=dict(sorted(excluded.items())), ranking=tuple(chosen), shortfall=shortfall)


def deep_subset(selection: UniverseSelection, size: int = 35) -> UniverseSelection:
    """The top-``size`` of an existing selection (the 30-40 deep-dataset universe)."""
    chosen = selection.ranking[:size]
    return UniverseSelection(role=selection.role, venue=selection.venue, asset_class=selection.asset_class,
                             as_of_ms=selection.as_of_ms, criteria={**selection.criteria, "deep_subset_size": size,
                                                                    "parent_manifest": selection.manifest_hash},
                             selected=tuple(s for s, _ in chosen), excluded={}, ranking=tuple(chosen))


def persist_universe_manifest(db: Any, sel: UniverseSelection) -> str:
    """Write once (immutable table). Re-writing the same content is a no-op."""
    with db.connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO universe_manifests (manifest_id, manifest_hash, role, asset_class, venue, "
            "instruments_json, selection_json, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (f"univ_{sel.manifest_hash[:16]}", sel.manifest_hash, sel.role, sel.asset_class, sel.venue,
             json.dumps(list(sel.selected)), json.dumps(sel.to_dict(), sort_keys=True), sel.as_of_ms))
    return sel.manifest_hash


def persist_dataset_manifest(db: Any, *, role: str, asset_class: str, venue: str, payload: Mapping[str, Any],
                             created_at: int) -> str:
    body = json.dumps(payload, sort_keys=True, default=str)
    h = hashlib.sha256(body.encode()).hexdigest()
    with db.connect() as conn:
        conn.execute("INSERT OR IGNORE INTO dataset_manifests (manifest_id, manifest_hash, role, asset_class, venue, "
                     "payload_json, created_at) VALUES (?,?,?,?,?,?,?)",
                     (f"ds_{h[:16]}", h, role, asset_class, venue, body, created_at))
    return h


#: What an instrument needs before it may join the EXECUTION universe.
EXECUTION_REQUIREMENTS = ("data_sufficient", "economics_available", "calibrated", "liquid", "venue_capable",
                          "account_capable", "certified_scope")


def execution_eligibility(evidence: Mapping[str, Optional[bool]]) -> Tuple[bool, Tuple[str, ...]]:
    """(eligible, missing reasons). ``None`` (unknown) is a missing requirement."""
    missing = tuple(f"NOT_{k.upper()}" for k in EXECUTION_REQUIREMENTS if evidence.get(k) is not True)
    return (not missing), missing


# ── Frozen, reproducible universes (certification) ──────────────────────────
#
# A live 24h ticker changes every minute, so a selection ranked on it cannot
# be reproduced. A CERTIFICATION universe is ranked on HISTORICAL liquidity
# inside a fixed window (median daily quote volume over the last N closed
# days that end at the window end): the same window always yields the same
# ranking. The frozen manifest file is the authority afterwards -- re-running
# the selector later produces a NEW research universe, never a silent edit of
# the frozen one (``load_frozen_universe`` refuses a manifest whose hash
# drifted).

HISTORICAL_SELECTION_RULE_VERSION = "universe-selection-historical-liquidity-v1"
FROZEN_UNIVERSE_SCHEMA_VERSION = "frozen-universe-manifest-v1"
#: stable-value bases are not directional crypto markets
STABLE_BASES = frozenset({"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "DAI", "USDP", "USDE", "USD1", "PYUSD"})
_FROZEN_IDENTITY_KEYS = ("schema_version", "role", "source_venue", "source_provider", "asset_class", "rule_version",
                         "selection_rule_version", "criteria", "window_start_ms", "window_end_ms", "timeframes",
                         "selected_symbols", "members", "metadata_hash")


@dataclass(frozen=True)
class HistoricalLiquidity:
    """Stats object for ``select_universe`` built from closed historical daily bars."""
    quote_volume_24h: Optional[float]      # median daily quote volume over the lookback
    spread_bps: Optional[float] = None     # historical spread is not published by the venue: UNAVAILABLE
    days_observed: int = 0
    source: str = "HISTORICAL_DAILY_KLINES_MEDIAN"


def historical_liquidity(daily_klines: Sequence[Sequence[Any]], *, lookback_days: int = 30,
                         end_ms: Optional[int] = None) -> HistoricalLiquidity:
    """Median daily quote volume of the last ``lookback_days`` CLOSED daily bars.

    Kline layout ``[open_time, o, h, l, c, v, close_time, quote_volume, ...]``.
    Fewer than half the lookback observed -> liquidity UNKNOWN (None), which
    the selector treats as an exclusion, never as "small".
    """
    rows = [k for k in daily_klines if end_ms is None or int(k[6] if len(k) > 6 else k[0]) <= end_ms]
    rows = list(rows)[-lookback_days:]
    vols = sorted(float(k[7]) for k in rows if len(k) > 7 and k[7] not in (None, ""))
    if len(vols) < max(1, lookback_days // 2):
        return HistoricalLiquidity(None, None, len(vols))
    mid = len(vols) // 2
    med = vols[mid] if len(vols) % 2 else (vols[mid - 1] + vols[mid]) / 2.0
    return HistoricalLiquidity(med, None, len(vols))


def exclude_stable_bases(instruments: Sequence[Any]) -> Tuple[List[Any], Dict[str, str]]:
    keep, dropped = [], {}
    for ins in instruments:
        if str(getattr(ins, "base_currency", "")).upper() in STABLE_BASES:
            dropped[ins.venue_symbol] = "STABLECOIN_BASE"
        else:
            keep.append(ins)
    return keep, dropped


def _metadata_hash(instruments: Sequence[Any], symbols: Sequence[str]) -> str:
    wanted = set(symbols)
    body = [{k: getattr(ins, k, None) for k in (
        "venue", "venue_symbol", "canonical_symbol", "asset_class", "product_type", "settlement_asset",
        "listed_at_ms", "tick_size", "qty_step", "min_qty", "min_notional")}
        for ins in sorted(instruments, key=lambda i: i.venue_symbol) if ins.venue_symbol in wanted]
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


def build_frozen_universe_manifest(selection: UniverseSelection, instruments: Sequence[Any], *, window_start_ms: int,
                                   window_end_ms: int, timeframes: Sequence[str], source_provider: str,
                                   generated_at: str, extra_excluded: Optional[Mapping[str, str]] = None,
                                   liquidity: Optional[Mapping[str, HistoricalLiquidity]] = None,
                                   role: str = CERTIFICATION) -> Dict[str, Any]:
    """The immutable file-level manifest for a research/certification universe.

    ``generated_at`` is operational metadata: it is NOT part of
    ``universe_hash`` (identical selections over the same window hash equal).
    """
    if role not in ROLES:
        raise ValueError(role)
    by_sym = {i.venue_symbol: i for i in instruments}
    excluded = dict(selection.excluded)
    excluded.update(extra_excluded or {})
    members = []
    for sym in selection.selected:
        ins = by_sym[sym]
        liq = (liquidity or {}).get(sym)
        members.append({"venue_symbol": sym, "canonical_instrument_id": ins.canonical_symbol,
                        "asset_class": ins.asset_class, "product_type": ins.product_type,
                        "base_asset": ins.base_currency, "quote_asset": ins.quote_currency,
                        "settlement_asset": ins.settlement_asset, "listed_at_ms": ins.listed_at_ms,
                        "median_daily_quote_volume": liq.quote_volume_24h if liq else None})
    identity = {
        "schema_version": FROZEN_UNIVERSE_SCHEMA_VERSION, "role": role, "source_venue": selection.venue,
        "source_provider": source_provider, "asset_class": selection.asset_class,
        "rule_version": HISTORICAL_SELECTION_RULE_VERSION, "selection_rule_version": selection.rule_version,
        "criteria": dict(selection.criteria), "window_start_ms": int(window_start_ms),
        "window_end_ms": int(window_end_ms), "timeframes": list(timeframes),
        "selected_symbols": list(selection.selected), "members": members,
        "metadata_hash": _metadata_hash(instruments, selection.selected),
    }
    universe_hash = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()
    return {**identity, "universe_id": f"univ_{selection.venue}_{universe_hash[:12]}", "universe_hash": universe_hash,
            "generated_at": generated_at, "shortfall": selection.shortfall,
            "rejected": dict(sorted(excluded.items())), "execution_authorized": False,
            "note": "research/certification membership never authorizes execution"}


class FrozenUniverseError(ValueError):
    pass


def verify_frozen_universe(manifest: Mapping[str, Any]) -> str:
    """Recompute the identity hash; raise if the manifest was edited."""
    missing = [k for k in _FROZEN_IDENTITY_KEYS if k not in manifest]
    if missing:
        raise FrozenUniverseError(f"frozen universe manifest missing {missing}")
    h = hashlib.sha256(json.dumps({k: manifest[k] for k in _FROZEN_IDENTITY_KEYS}, sort_keys=True,
                                  default=str).encode()).hexdigest()
    if h != manifest.get("universe_hash"):
        raise FrozenUniverseError(f"universe hash mismatch: recorded {manifest.get('universe_hash')} computed {h}")
    return h


def load_frozen_universe(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)
    verify_frozen_universe(manifest)
    return manifest


__all__ = ["CERTIFICATION", "EXECUTION", "EXECUTION_REQUIREMENTS", "FROZEN_UNIVERSE_SCHEMA_VERSION",
           "FrozenUniverseError", "HISTORICAL_SELECTION_RULE_VERSION", "HistoricalLiquidity", "RESEARCH", "ROLES",
           "STABLE_BASES", "SelectionCriteria", "TRAINING", "UniverseSelection", "build_frozen_universe_manifest",
           "deep_subset", "exclude_stable_bases", "execution_eligibility", "historical_liquidity",
           "load_frozen_universe", "persist_dataset_manifest", "persist_universe_manifest", "select_universe",
           "verify_frozen_universe"]
