"""Signal once, size per account (Section H, Step 5.4).

The market stage of a family (``daily_trend.market_stage``) is computed **once per eligible cycle** and is the
same object for every account. This module fans that one decision out to many accounts with bounded
concurrency, under three guarantees:

* **Immutability of the shared signal.** ``SharedSignal`` fingerprints the market decision (its data snapshot
  id, ranking and the bytes of every feature array) when it is created and verifies the fingerprint after the
  fan-out: an account sizer that wrote into the shared arrays would be detected, and the cycle refused.
* **Account isolation.** Each sizer call gets a deep copy of its own ``AccountSnapshot`` (an independently
  versioned view: book, equity, held positions). Results are keyed and checked by account id and snapshot
  version, so one account's data cannot reach another account's sizing, and an exception in one account is
  recorded for that account only.
* **A cycle budget.** Every account's duration is measured; the report says whether all finished inside the
  configured budget, and lists the ones that did not.

The default sizer is the frozen family decision itself (``daily_trend.decide``): risk level, equity, held
positions, exposure, the position limit, open-risk and leverage ceilings and the brake flags all live in the
book it is given. Broker instrument filters, margin and user maximums are applied afterwards by the
portfolio/execution boundary, unchanged. Nothing here submits an order.
"""
from __future__ import annotations

import copy
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

import numpy as np

FANOUT_VERSION = "1.0.0"


class SharedSignalMutated(RuntimeError):
    code = "SHARED_SIGNAL_MUTATED"


def _fingerprint(market: Any) -> str:
    h = hashlib.sha256()
    h.update(str(getattr(market, "data_snapshot_id", "")).encode())
    h.update(str(getattr(market, "day", "")).encode())
    for c in getattr(market, "candidates", ()):
        h.update(repr(c).encode())
    feat = getattr(market, "features", None)
    if feat is not None:
        for name in sorted(vars(feat)):
            value = getattr(feat, name)
            if isinstance(value, np.ndarray):
                h.update(name.encode())
                h.update(np.ascontiguousarray(value).tobytes())
    return h.hexdigest()


@dataclass(frozen=True)
class SharedSignal:
    family_id: str
    cycle_key: str
    market: Any
    fingerprint: str
    computed_ms: float

    @classmethod
    def compute(cls, family_id: str, cycle_key: str, build: Callable[[], Any]) -> "SharedSignal":
        started = time.perf_counter()
        market = build()
        elapsed = (time.perf_counter() - started) * 1000
        return cls(family_id, cycle_key, market, _fingerprint(market), elapsed)

    def verify_unchanged(self) -> None:
        if _fingerprint(self.market) != self.fingerprint:
            raise SharedSignalMutated(f"{self.family_id}/{self.cycle_key}: the shared market decision was modified")


@dataclass(frozen=True)
class AccountSnapshot:
    account_id: str
    tenant_id: str
    version: int
    book: Any


@dataclass
class AccountSizing:
    account_id: str
    version: int
    duration_ms: float
    result: Any = None
    error: Optional[str] = None


@dataclass
class FanoutReport:
    signal_fingerprint: str
    accounts: int
    concurrency: int
    total_ms: float
    sizings: Dict[str, AccountSizing] = field(default_factory=dict)
    over_budget: List[str] = field(default_factory=list)
    budget_ms: Optional[float] = None

    @property
    def errors(self) -> Dict[str, str]:
        return {k: v.error for k, v in self.sizings.items() if v.error}

    def latency(self) -> Dict[str, float]:
        d = sorted(s.duration_ms for s in self.sizings.values())
        if not d:
            return {"p50": 0.0, "p95": 0.0, "max": 0.0}
        pick = lambda q: d[min(len(d) - 1, int(round(q * (len(d) - 1))))]  # noqa: E731
        return {"p50": round(pick(0.5), 4), "p95": round(pick(0.95), 4), "max": round(d[-1], 4)}

    @property
    def within_budget(self) -> bool:
        return not self.over_budget and (self.budget_ms is None or self.total_ms <= self.budget_ms)


def default_sizer(signal: SharedSignal, book: Any) -> Any:
    from app.trading_intelligence.pipeline import daily_trend as DT
    return DT.decide(book, signal.market)


def size_accounts(signal: SharedSignal, snapshots: Sequence[AccountSnapshot], *,
                  sizer: Callable[[SharedSignal, Any], Any] = default_sizer, max_concurrency: int = 4,
                  account_budget_ms: Optional[float] = None, cycle_budget_ms: Optional[float] = None) -> FanoutReport:
    ids = [s.account_id for s in snapshots]
    if len(ids) != len(set(ids)):
        raise ValueError("FANOUT_DUPLICATE_ACCOUNT")
    report = FanoutReport(signal.fingerprint, len(snapshots), max(1, int(max_concurrency)), 0.0,
                          budget_ms=cycle_budget_ms)

    def one(snap: AccountSnapshot) -> AccountSizing:
        own = copy.deepcopy(snap.book)          # the sizer never sees another account's (or the caller's) object
        started = time.perf_counter()
        try:
            result = sizer(signal, own)
            return AccountSizing(snap.account_id, snap.version, (time.perf_counter() - started) * 1000, result)
        except Exception as exc:
            return AccountSizing(snap.account_id, snap.version, (time.perf_counter() - started) * 1000,
                                 error=f"{type(exc).__name__}: {exc}"[:300])

    started = time.perf_counter()
    if report.concurrency == 1:
        sized = [one(s) for s in snapshots]
    else:
        with ThreadPoolExecutor(max_workers=report.concurrency, thread_name_prefix="size") as pool:
            sized = list(pool.map(one, snapshots))
    report.total_ms = (time.perf_counter() - started) * 1000
    signal.verify_unchanged()
    for snap, s in zip(snapshots, sized):
        if s.account_id != snap.account_id or s.version != snap.version:  # pragma: no cover - defensive
            raise RuntimeError("FANOUT_RESULT_MISATTRIBUTED")
        report.sizings[s.account_id] = s
        if account_budget_ms is not None and s.duration_ms > account_budget_ms:
            report.over_budget.append(s.account_id)
    return report
