"""Operational metrics for the multi-asset broker layer (Phase 6J).

Uses the existing bounded-cardinality CATI metric registry (label allow-list,
id-like values refused), so no user/account/transfer id can ever become a
label. Emission never raises into the caller.

Metric names:
    broker_capability_block_total{venue,reason_family}
    broker_permission_failure_total{venue,reason_family}
    broker_transfer_total{venue,status,reason_family}
    broker_transfer_reconcile_total{venue,status}
    broker_instrument_sync_total{venue,status}
    capital_routing_decision_total{venue,outcome}
    market_data_unavailable_total{venue,source,reason_family}
    fx_reference_divergence_bps{venue}          (summary)
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _metrics():
    from app.trading_intelligence.observability.metrics import METRICS, reason_family

    return METRICS, reason_family


def _safe(fn):
    def wrapper(*a, **k):
        try:
            return fn(*a, **k)
        except Exception as exc:  # metrics must never break trading or transfers
            logger.debug("multi_asset_metric_dropped %s: %s", fn.__name__, type(exc).__name__)
    return wrapper


@_safe
def capability_block(venue: str, reason: Optional[str]) -> None:
    m, rf = _metrics()
    m.inc("broker_capability_block_total", venue=venue, reason_family=rf(reason))


@_safe
def permission_failure(venue: str, reason: Optional[str]) -> None:
    m, rf = _metrics()
    m.inc("broker_permission_failure_total", venue=venue, reason_family=rf(reason))


@_safe
def transfer(venue: str, status: str, reason: Optional[str] = None) -> None:
    m, rf = _metrics()
    m.inc("broker_transfer_total", venue=venue, status=status, reason_family=rf(reason))


@_safe
def transfer_reconcile(venue: str, status: str) -> None:
    m, _ = _metrics()
    m.inc("broker_transfer_reconcile_total", venue=venue, status=status)


@_safe
def instrument_sync(venue: str, status: str) -> None:
    m, _ = _metrics()
    m.inc("broker_instrument_sync_total", venue=venue, status=status)


@_safe
def capital_routing(venue: str, outcome: str) -> None:
    m, _ = _metrics()
    m.inc("capital_routing_decision_total", venue=venue, outcome=outcome)


@_safe
def market_data_unavailable(venue: str, source: str, reason: Optional[str]) -> None:
    m, rf = _metrics()
    m.inc("market_data_unavailable_total", venue=venue, source=source, reason_family=rf(reason))


@_safe
def fx_divergence(venue: str, bps: Any) -> None:
    if bps is None:
        return
    m, _ = _metrics()
    m.observe("fx_reference_divergence_bps", float(bps), edges=(1.0, 2.0, 5.0, 10.0, 25.0, 50.0), venue=venue)


__all__ = ["capability_block", "capital_routing", "fx_divergence", "instrument_sync", "market_data_unavailable",
           "permission_failure", "transfer", "transfer_reconcile"]
