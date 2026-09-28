"""Physical internal-transfer route facts for transfer economics (Section 16.C).

Sources, never invented:

* fee      -- only from an explicitly supplied, sourced fact (``declared``: e.g. a venue-validated route
              fee once the internal-transfer contract is validated). The declared broker topology
              publishes NO route fee (``shared_lib.broker.wallets``: UNAVAILABLE_FROM_VENUE_API), so the
              default is UNAVAILABLE -- never 0.
* latency  -- observed from THIS account's own broker-COMPLETED transfers on the same route and asset
              (``broker_transfer_requests``: submitted_at -> confirmed_at). At least ``min_samples``
              recent observations are required; the estimate is the slowest of them (conservative).
              No history -> UNAVAILABLE, never "instant".

Read-only: nothing here submits, reserves or reconciles anything.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Tuple

ROUTE_FACTS_VERSION = "transfer-route-facts-v1"


@dataclass(frozen=True)
class RouteFacts:
    fee: Optional[float]
    fee_currency: Optional[str]
    fee_source: Optional[str]
    latency_ms: Optional[int]
    latency_source: Optional[str]
    observed_at: int
    version: str = ROUTE_FACTS_VERSION


def _ms(iso: Any) -> Optional[int]:
    try:
        dt = datetime.fromisoformat(str(iso))
        return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp() * 1000)
    except (TypeError, ValueError):
        return None


class ObservedRouteFacts:
    """``declared``: {(source_wallet, destination_wallet, asset): (fee, fee_currency, fee_source)}."""

    def __init__(self, db: Any, *, declared: Optional[Mapping[Tuple[str, str, str], Tuple[float, str, str]]] = None,
                 min_samples: int = 3, lookback_days: int = 30, max_samples: int = 20) -> None:
        self.db = db
        self.declared = {tuple(str(x).upper() for x in k): v for k, v in (declared or {}).items()}
        self.min_samples, self.lookback_days, self.max_samples = int(min_samples), int(lookback_days), int(max_samples)

    def observed_latency(self, broker_account_id: str, source_wallet: str, destination_wallet: str, asset: str,
                         now_ms: int) -> Tuple[Optional[int], Optional[str]]:
        since = (datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc) - timedelta(days=self.lookback_days)).isoformat()
        if self.db is None:
            return None, None
        try:
            with self.db.connect() as conn:
                rows = conn.execute(
                    "SELECT submitted_at, confirmed_at FROM broker_transfer_requests WHERE broker_account_id=? AND "
                    "source_wallet=? AND destination_wallet=? AND asset=? AND status='COMPLETED' AND "
                    "submitted_at IS NOT NULL AND confirmed_at IS NOT NULL AND requested_at>=? "
                    "ORDER BY requested_at DESC LIMIT ?",
                    (broker_account_id, source_wallet.upper(), destination_wallet.upper(), asset.upper(), since,
                     self.max_samples)).fetchall()
        except Exception:
            return None, None  # unreadable history is unknown latency, never zero
        samples = [c - s for s, c in ((_ms(r[0]), _ms(r[1])) for r in rows) if s is not None and c is not None and c >= s]
        if len(samples) < self.min_samples:
            return None, None
        return max(samples), f"OBSERVED_ACCOUNT_HISTORY:max_of_{len(samples)}"

    def facts(self, *, broker_account_id: str, source_wallet: str, destination_wallet: str, asset: str,
              now_ms: int) -> RouteFacts:
        fee, currency, fee_source = self.declared.get((source_wallet.upper(), destination_wallet.upper(),
                                                       asset.upper()), (None, None, None))
        latency, latency_source = self.observed_latency(broker_account_id, source_wallet, destination_wallet, asset,
                                                        now_ms)
        return RouteFacts(fee, currency, fee_source, latency, latency_source, int(now_ms))


__all__ = ["ObservedRouteFacts", "ROUTE_FACTS_VERSION", "RouteFacts"]
