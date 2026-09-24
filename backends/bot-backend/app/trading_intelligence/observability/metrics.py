"""Bounded-cardinality CATI runtime metrics (Sections 21.4-21.15, 21.22).

In-process counters and summaries -- there is no Prometheus exporter in this
codebase, and none is added here; ``snapshot()`` is the read path for a
future exporter / health endpoint.

LABEL SAFETY (21.15): only the dimensions in ``ALLOWED_LABELS`` may be used,
and a label VALUE that looks like an identifier (trade_plan_id, position_id,
user_id, uuids, long digit runs, prefix_<hex>) is refused with
``MetricLabelError``. Each metric is also capped at ``MAX_SERIES_PER_METRIC``
label combinations; further combinations collapse into ``__overflow__``.
Detailed ids live in the evidence tables and structured logs, never here.
"""
from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

from app.trading_intelligence.versions import METRICS_SCHEMA_VERSION

ALLOWED_LABELS = frozenset({
    "stage", "status", "asset_class", "venue", "setup_family", "side", "regime", "action", "reason_family",
    "outcome", "component", "bucket", "source", "environment",
})
MAX_LABEL_VALUE_CHARS = 48
MAX_SERIES_PER_METRIC = 256
OVERFLOW = "__overflow__"

_ID_LIKE = (
    re.compile(r"^[a-z]{2,12}_[0-9a-f]{10,}$"),        # CATI short ids: tplan_..., pos_..., exd_...
    re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"),  # uuid
    re.compile(r"\d{6,}"),                                  # timestamps / numeric ids
    re.compile(r"[0-9a-f]{16,}"),                           # hashes
)


class MetricLabelError(ValueError):
    """A label key outside the allow-list, or a high-cardinality value."""


def _check_label(key: str, value: Any) -> str:
    if key not in ALLOWED_LABELS:
        raise MetricLabelError(f"metric label {key!r} is not an allowed bounded dimension")
    v = "UNKNOWN" if value is None else str(value)
    if len(v) > MAX_LABEL_VALUE_CHARS or any(p.search(v.lower()) for p in _ID_LIKE):
        raise MetricLabelError(f"metric label {key}={v[:24]!r}... looks like a high-cardinality identifier")
    return v


def reason_family(code: Any) -> str:
    """Bounded family of a reason code: its first token, e.g.
    ``DAILY_LOSS_LIMIT`` -> ``DAILY``; ``FAILED:TREND_...`` -> ``FAILED``."""
    c = str(code or "NONE").upper()
    for sep in (":", "_"):
        if sep in c:
            c = c.split(sep, 1)[0]
            break
    return re.sub(r"[^A-Z]", "", c)[:24] or "OTHER"


@dataclass
class _Summary:
    count: int = 0
    total: float = 0.0
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    buckets: Dict[str, int] = field(default_factory=dict)

    def add(self, value: float, edges: Tuple[float, ...]) -> None:
        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)
        label = next((f"le_{e:g}" for e in edges if value <= e), "le_inf")
        self.buckets[label] = self.buckets.get(label, 0) + 1


DEFAULT_EDGES = (0.0, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0)


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, Dict[Tuple[Tuple[str, str], ...], float]] = {}
        self._summaries: Dict[str, Dict[Tuple[Tuple[str, str], ...], _Summary]] = {}

    def _key(self, store: Dict, name: str, labels: Mapping[str, Any]):
        key = tuple(sorted((k, _check_label(k, v)) for k, v in labels.items()))
        series = store.setdefault(name, {})
        if key not in series and len(series) >= MAX_SERIES_PER_METRIC:
            key = tuple((k, OVERFLOW) for k, _ in key)
        return series, key

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            series, key = self._key(self._counters, name, labels)
            series[key] = series.get(key, 0.0) + float(value)

    def observe(self, name: str, value: Optional[float], edges: Tuple[float, ...] = DEFAULT_EDGES,
                **labels: Any) -> None:
        if value is None:
            return
        with self._lock:
            series, key = self._key(self._summaries, name, labels)
            series.setdefault(key, _Summary()).add(float(value), edges)

    def counter(self, name: str, **labels: Any) -> float:
        with self._lock:
            series = self._counters.get(name, {})
            want = {(k, str(v)) for k, v in labels.items()}
            return sum(v for k, v in series.items() if want <= set(k))

    def summary(self, name: str, **labels: Any) -> Optional[_Summary]:
        with self._lock:
            want = {(k, str(v)) for k, v in labels.items()}
            hits = [s for k, s in self._summaries.get(name, {}).items() if want <= set(k)]
        if not hits:
            return None
        out = _Summary()
        for s in hits:
            out.count += s.count
            out.total += s.total
        return out

    def names(self) -> Iterable[str]:
        with self._lock:
            return sorted(set(self._counters) | set(self._summaries))

    def label_keys(self) -> Iterable[str]:
        with self._lock:
            keys = set()
            for store in (self._counters, self._summaries):
                for series in store.values():
                    for key in series:
                        keys.update(k for k, _ in key)
            return keys

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": METRICS_SCHEMA_VERSION,
                "counters": {n: [{"labels": dict(k), "value": v} for k, v in s.items()] for n, s in self._counters.items()},
                "summaries": {n: [{"labels": dict(k), "count": v.count, "sum": v.total, "min": v.minimum,
                                   "max": v.maximum, "buckets": dict(v.buckets)} for k, v in s.items()]
                              for n, s in self._summaries.items()},
            }

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._summaries.clear()


METRICS = MetricsRegistry()


def get_metrics() -> MetricsRegistry:
    return METRICS


__all__ = ["ALLOWED_LABELS", "MAX_SERIES_PER_METRIC", "OVERFLOW", "MetricLabelError", "MetricsRegistry", "METRICS",
           "get_metrics", "reason_family", "DEFAULT_EDGES"]
