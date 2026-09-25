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


__all__ = ["CERTIFICATION", "EXECUTION", "EXECUTION_REQUIREMENTS", "RESEARCH", "ROLES", "SelectionCriteria", "TRAINING",
           "UniverseSelection", "deep_subset", "execution_eligibility", "persist_dataset_manifest",
           "persist_universe_manifest", "select_universe"]
