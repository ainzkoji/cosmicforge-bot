"""Immutable certification dataset manifest (Section 22.4).

Built from the data itself (``series_checksum`` / ``assess_quality`` reused
from ``app.research.dataset``) plus every analytical version that shaped the
run. No wall-clock value enters ``manifest_hash``: two identical inputs and
configurations always produce the same manifest identity.

Provenance is DERIVED from ``historical_candles.data_source`` through the
existing source classifier (``forecast.source``). Only ``REAL_MARKET`` data
can support a certification claim; synthetic / fixture / unknown data is
research-only and the gates refuse it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from app.research.dataset import assess_quality, close_time, open_time, series_checksum
from app.trading_intelligence.forecast.source import SK, classify_data_sources
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import CERTIFICATION_MANIFEST_SCHEMA_VERSION

CERTIFIABLE_SOURCE_KINDS = frozenset({SK.REAL_MARKET.value})

#: HTF bars are consumed only once CLOSED at the decision time (the provider's
#: clock cut), never the forming bar.
HTF_ALIGNMENT_POLICY = "CLOSED_HTF_BAR_AT_OR_BEFORE_DECISION"
#: Same-bar TP+SL: the favorable outcome is never chosen (labels.label_candidate).
INTRABAR_AMBIGUITY_POLICY = "SL_FIRST_CONSERVATIVE"


def cati_version_set() -> Dict[str, str]:
    """Every CATI analytical version constant (versions.py) -- one registry."""
    from app.trading_intelligence import versions

    return {k: v for k, v in sorted(vars(versions).items()) if k.isupper() and isinstance(v, str)}


@dataclass(frozen=True)
class CertificationDatasetManifest:
    source_provider: str
    dataset_identity: str
    source_hash: str
    source_kind: str
    data_sources: Tuple[str, ...]
    asset_class: str
    venue: str
    environment: str
    symbols: Tuple[str, ...]
    timeframes: Tuple[str, ...]
    start_ms: int
    end_ms: int
    row_counts: Mapping[str, int]
    checksums: Mapping[str, str]
    missing_intervals: Mapping[str, Any]
    quality_summary: Mapping[str, Any]
    feature_transforms: Tuple[str, ...]
    exclusions: Tuple[str, ...]
    warmup_bars: int
    label_horizon_bars: int
    htf_alignment_policy: str
    intrabar_ambiguity_policy: str
    cost_model_version: str
    calendar_source_version: str
    cati_versions: Mapping[str, str]
    schema_version: str = CERTIFICATION_MANIFEST_SCHEMA_VERSION
    #: operational metadata -- excluded from identity
    created_at: Optional[str] = field(default=None, compare=False)

    def _identity(self) -> dict:
        return {
            "source_provider": self.source_provider, "dataset_identity": self.dataset_identity,
            "source_hash": self.source_hash, "source_kind": self.source_kind, "data_sources": list(self.data_sources),
            "asset_class": self.asset_class, "venue": self.venue, "environment": self.environment,
            "symbols": list(self.symbols), "timeframes": list(self.timeframes), "start_ms": self.start_ms,
            "end_ms": self.end_ms, "row_counts": dict(self.row_counts), "checksums": dict(self.checksums),
            "missing_intervals": dict(self.missing_intervals), "quality_summary": dict(self.quality_summary),
            "feature_transforms": list(self.feature_transforms), "exclusions": list(self.exclusions),
            "warmup_bars": self.warmup_bars, "label_horizon_bars": self.label_horizon_bars,
            "htf_alignment_policy": self.htf_alignment_policy,
            "intrabar_ambiguity_policy": self.intrabar_ambiguity_policy,
            "cost_model_version": self.cost_model_version, "calendar_source_version": self.calendar_source_version,
            "cati_versions": dict(self.cati_versions), "schema_version": self.schema_version,
        }

    @property
    def manifest_hash(self) -> str:
        return stable_hash(self._identity())

    @property
    def dataset_hash(self) -> str:
        """The DATA fingerprint alone (what the candles are), used to bind a holdout."""
        return stable_hash({"source_hash": self.source_hash, "checksums": dict(self.checksums),
                            "row_counts": dict(self.row_counts)})[:32]

    @property
    def manifest_id(self) -> str:
        return short_id("cman", self._identity())

    @property
    def certifiable_source(self) -> bool:
        return self.source_kind in CERTIFIABLE_SOURCE_KINDS

    @property
    def coverage_days(self) -> float:
        return max(0.0, (self.end_ms - self.start_ms + 1) / 86_400_000)

    def to_dict(self) -> dict:
        return {"manifest_id": self.manifest_id, "manifest_hash": self.manifest_hash,
                "dataset_hash": self.dataset_hash, **self._identity(),
                "certifiable_source": self.certifiable_source, "coverage_days": round(self.coverage_days, 4),
                "metadata": {"created_at": self.created_at}}


def build_certification_manifest(
    series: Mapping[str, Mapping[str, Sequence[Any]]], *, data_sources: Sequence[str], source_provider: str,
    dataset_identity: str, asset_class: str, venue: str, environment: str, warmup_bars: int,
    label_horizon_bars: int, cost_model_version: str, calendar_source_version: str = "UNAVAILABLE",
    exclusions: Sequence[str] = (), feature_transforms: Sequence[str] = ("canonical_market_state_v1",),
    created_at: Optional[str] = None,
) -> CertificationDatasetManifest:
    from app.replay.identity import dataset_hash

    rows: Dict[str, int] = {}
    checksums: Dict[str, str] = {}
    missing: Dict[str, Any] = {}
    quality: Dict[str, Any] = {}
    starts, ends, timeframes = [], [], set()
    for symbol in sorted(series):
        for tf in sorted(series[symbol]):
            data = series[symbol][tf]
            key = f"{symbol}:{tf}"
            timeframes.add(tf)
            rows[key] = len(data)
            checksums[key] = series_checksum(data)
            q = assess_quality(data, symbol=symbol, timeframe=tf)
            missing[key] = {"missing_bars": q.missing_bars, "windows": [list(w) for w in q.missing_windows[:50]],
                            "window_count": len(q.missing_windows)}
            quality[key] = {"rows": q.rows, "completeness": round(q.completeness, 6), "is_usable": q.is_usable,
                            "duplicates": q.duplicate_opens, "out_of_order": q.out_of_order,
                            "ohlc_violations": q.ohlc_violations, "non_positive_prices": q.non_positive_prices}
            if data:
                starts.append(open_time(data[0]))
                ends.append(close_time(data[-1]))
    if not starts:
        raise ValueError("certification manifest needs at least one non-empty series")
    return CertificationDatasetManifest(
        source_provider=source_provider, dataset_identity=dataset_identity, source_hash=dataset_hash(series),
        source_kind=classify_data_sources(data_sources), data_sources=tuple(sorted({str(s or "") for s in data_sources})),
        asset_class=str(asset_class).upper(), venue=str(venue).upper(), environment=str(environment).upper(),
        symbols=tuple(sorted(series)), timeframes=tuple(sorted(timeframes)), start_ms=min(starts), end_ms=max(ends),
        row_counts=rows, checksums=checksums, missing_intervals=missing, quality_summary=quality,
        feature_transforms=tuple(feature_transforms), exclusions=tuple(exclusions), warmup_bars=int(warmup_bars),
        label_horizon_bars=int(label_horizon_bars), htf_alignment_policy=HTF_ALIGNMENT_POLICY,
        intrabar_ambiguity_policy=INTRABAR_AMBIGUITY_POLICY, cost_model_version=cost_model_version,
        calendar_source_version=calendar_source_version, cati_versions=cati_version_set(), created_at=created_at)


__all__ = ["CertificationDatasetManifest", "build_certification_manifest", "cati_version_set",
           "CERTIFIABLE_SOURCE_KINDS", "HTF_ALIGNMENT_POLICY", "INTRABAR_AMBIGUITY_POLICY"]
