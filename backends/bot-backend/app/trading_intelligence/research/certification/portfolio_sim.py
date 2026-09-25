"""Combined multi-asset portfolio simulation (Phase 6D).

Per-scope walk-forward replays (crypto / FX, per venue) certify signal
quality in isolation. This harness answers the ACCOUNT questions they cannot:
what happens when crypto and FX signals arrive together on ONE account that
shares (or does not share) collateral --

* margin: is there free capital in the wallet that collateralises the product?
* currency concentration: does the new trade worsen a single-currency share?
* crypto concentration: asset-class share cap
* simultaneous signals: processed in (time, key) order, each sees the
  reservations of the previous ones
* capital routing: CapitalAllocationPlanner; a physical transfer goes through
  the seeded transfer model and the entry waits for confirmation
* failure handling: failed / slow / unknown transfers abandon the entry

Deterministic for a given seed and signal list.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from shared_lib.broker.wallets import BrokerTopology

from app.trading_intelligence.capital.planner import (
    AccountCapitalState, CapitalSettings, PHYSICAL_INTERNAL_TRANSFER_REQUIRED, plan_capital,
)
from app.trading_intelligence.portfolio.currency_exposure import (
    ConcentrationLimits, ExposureItem, build_exposure, check_concentration,
)
from app.trading_intelligence.research.certification.transfer_sim import (
    DependentEntry, TransferModel, resolve_dependent_entry, simulate_transfer,
)


@dataclass(frozen=True)
class SimSignal:
    key: str
    time_ms: int
    instrument: str
    asset_class: str
    product: str            # CRYPTO_PERPETUAL | FX_PERPETUAL
    base: str
    quote: str
    side: str
    notional: float
    margin: float           # collateral the entry consumes
    ttl_ms: int = 15 * 60_000


@dataclass
class SimResult:
    executed: List[str] = field(default_factory=list)
    rejected: Dict[str, str] = field(default_factory=dict)
    transfers: Dict[str, str] = field(default_factory=dict)
    final_free: Dict[str, float] = field(default_factory=dict)


def simulate_portfolio(signals: Sequence[SimSignal], *, topology: BrokerTopology, free_by_wallet: Mapping[str, float],
                       settings: CapitalSettings = CapitalSettings(mode="AUTOMATED_INTERNAL_REALLOCATION",
                                                                   auto_rebalance_enabled=True, authorized=True),
                       limits: ConcentrationLimits = ConcentrationLimits(), seed: int = 7,
                       model: TransferModel = TransferModel(), asset: str = "USDT") -> SimResult:
    free = {k: float(v) for k, v in free_by_wallet.items()}
    positions: List[ExposureItem] = []
    res = SimResult()
    for s in sorted(signals, key=lambda x: (x.time_ms, x.key)):
        cur = build_exposure(positions)
        prop = build_exposure([ExposureItem(s.instrument, s.asset_class, s.base, s.quote, s.side, s.notional, "PROPOSED")])
        ok, reasons = check_concentration(cur, prop, limits)
        if not ok:
            res.rejected[s.key] = reasons[0]
            continue
        state = AccountCapitalState("sim", asset, topology, {k: Decimal(str(v)) for k, v in free.items()},
                                    transfer_capability_usable=True)
        plan = plan_capital(state=state, product=s.product, required=Decimal(str(s.margin)), settings=settings,
                            plan_key=s.key)
        if plan.outcome in ("NO_ACTION_SHARED_COLLATERAL", "LOGICAL_REALLOCATION"):
            free[plan.trading_wallet] -= s.margin
        elif plan.outcome == PHYSICAL_INTERNAL_TRANSFER_REQUIRED and plan.transfer and plan.transfer.auto_submit:
            t = plan.transfer
            sim = simulate_transfer(seed=seed, transfer_key=t.idempotency_key, submitted_at_ms=s.time_ms,
                                    amount=float(t.amount), transferable_at_submit=free.get(t.source_wallet, 0.0),
                                    model=model)
            res.transfers[s.key] = sim.final_status + ("_VIA_UNKNOWN" if sim.went_unknown else "")
            shortfall = s.margin - max(free.get(plan.trading_wallet, 0.0), 0.0)  # what the transfer must cover
            status, _at = resolve_dependent_entry(DependentEntry(s.key, s.time_ms, s.time_ms + s.ttl_ms, shortfall), sim)
            if sim.final_status == "COMPLETED":  # funds moved whether or not the entry still executes
                free[t.source_wallet] -= float(t.amount)
                free[t.destination_wallet] = free.get(t.destination_wallet, 0.0) + float(t.amount)
            if status != "EXECUTED":
                res.rejected[s.key] = status
                continue
            free[plan.trading_wallet] -= s.margin
        else:
            res.rejected[s.key] = plan.outcome + (":" + plan.reason_codes[0] if plan.reason_codes else "")
            continue
        positions.append(ExposureItem(s.instrument, s.asset_class, s.base, s.quote, s.side, s.notional))
        res.executed.append(s.key)
    res.final_free = dict(sorted(free.items()))
    return res


__all__ = ["SimResult", "SimSignal", "simulate_portfolio"]
