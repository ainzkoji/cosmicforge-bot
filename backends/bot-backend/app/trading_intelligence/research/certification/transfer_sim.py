"""Internal-transfer latency / uncertainty model for replay (Phase 6E).

Funds do not teleport between wallets. In replay, a capital plan that needs a
physical internal transfer submits a SIMULATED transfer whose outcome and
confirmation delay come from a deterministic, seeded model:

    COMPLETED after a latency draw        (most transfers)
    FAILED                                 (broker refused / insufficient)
    UNKNOWN -> resolved later by "reconciliation" to COMPLETED or FAILED

The dependent entry may only execute once the simulated transfer is
COMPLETED at or before the entry's decision time, and the funded capital is
only counted from that moment (``capital.planner.is_fundable``). Every
scenario the specification lists is a deterministic function of the seed.
"""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

TRANSFER_SIM_VERSION = "transfer-sim-v1"


@dataclass(frozen=True)
class TransferModel:
    p_fail: float = 0.02
    p_unknown: float = 0.03
    p_unknown_resolves_completed: float = 0.8
    latency_ms: Tuple[int, int] = (1_000, 30_000)          # uniform confirmation latency
    unknown_resolution_ms: Tuple[int, int] = (60_000, 900_000)


@dataclass(frozen=True)
class SimulatedTransfer:
    transfer_key: str
    submitted_at_ms: int
    final_status: str                 # COMPLETED | FAILED
    confirmed_at_ms: Optional[int]    # when the broker-authoritative final state is known
    went_unknown: bool
    amount: float

    def status_at(self, t_ms: int) -> str:
        if t_ms < self.submitted_at_ms:
            return "NOT_SUBMITTED"
        if self.confirmed_at_ms is None or t_ms < self.confirmed_at_ms:
            return "UNKNOWN" if self.went_unknown else "CONFIRMATION_PENDING"
        return self.final_status

    def funds_available_at(self, t_ms: int) -> bool:
        return self.status_at(t_ms) == "COMPLETED"


def _rng(seed: int, key: str) -> random.Random:
    h = int(hashlib.sha256(f"{seed}|{key}".encode()).hexdigest()[:16], 16)
    return random.Random(h)


def simulate_transfer(*, seed: int, transfer_key: str, submitted_at_ms: int, amount: float,
                      transferable_at_submit: float, model: TransferModel = TransferModel()) -> SimulatedTransfer:
    if amount <= 0 or transferable_at_submit < amount:
        # insufficient transferable balance: refused immediately, nothing moves
        return SimulatedTransfer(transfer_key, submitted_at_ms, "FAILED", submitted_at_ms, False, amount)
    r = _rng(seed, transfer_key)
    u = r.random()
    if u < model.p_fail:
        return SimulatedTransfer(transfer_key, submitted_at_ms, "FAILED",
                                 submitted_at_ms + r.randint(*model.latency_ms), False, amount)
    if u < model.p_fail + model.p_unknown:
        final = "COMPLETED" if r.random() < model.p_unknown_resolves_completed else "FAILED"
        return SimulatedTransfer(transfer_key, submitted_at_ms, final,
                                 submitted_at_ms + r.randint(*model.unknown_resolution_ms), True, amount)
    return SimulatedTransfer(transfer_key, submitted_at_ms, "COMPLETED",
                             submitted_at_ms + r.randint(*model.latency_ms), False, amount)


@dataclass(frozen=True)
class DependentEntry:
    entry_key: str
    decision_ms: int
    expires_ms: int          # the opportunity is abandoned if not fundable by then
    required: float


def resolve_dependent_entry(entry: DependentEntry, transfer: SimulatedTransfer, *,
                            risk_still_accepts_at: Optional[Dict[int, bool]] = None) -> Tuple[str, Optional[int]]:
    """(status, execute_at_ms). The entry executes at the first moment the
    transfer is COMPLETED, provided that moment is before expiry AND risk
    still accepts at that moment (risk may change while pending)."""
    if transfer.final_status != "COMPLETED" or transfer.confirmed_at_ms is None:
        return "ABANDONED_TRANSFER_FAILED", None
    t = max(entry.decision_ms, transfer.confirmed_at_ms)
    if t > entry.expires_ms:
        return "ABANDONED_TRANSFER_TOO_SLOW", None
    if transfer.amount < entry.required:
        return "ABANDONED_UNDERFUNDED", None
    if risk_still_accepts_at is not None:
        ok = risk_still_accepts_at.get(t)
        if ok is not True:
            return "ABANDONED_RISK_CHANGED_WHILE_PENDING", None
    return "EXECUTED", t


def run_scenarios(seed: int, n: int, *, model: TransferModel = TransferModel(), amount: float = 100.0) -> Dict[str, int]:
    """Aggregate outcome counts over ``n`` seeded transfers (for replay reports)."""
    counts: Dict[str, int] = {}
    for i in range(n):
        t = simulate_transfer(seed=seed, transfer_key=f"t{i}", submitted_at_ms=0, amount=amount,
                              transferable_at_submit=amount * 2, model=model)
        k = f"{t.final_status}{'_VIA_UNKNOWN' if t.went_unknown else ''}"
        counts[k] = counts.get(k, 0) + 1
    return dict(sorted(counts.items()))


__all__ = ["DependentEntry", "SimulatedTransfer", "TRANSFER_SIM_VERSION", "TransferModel", "resolve_dependent_entry",
           "run_scenarios", "simulate_transfer"]
