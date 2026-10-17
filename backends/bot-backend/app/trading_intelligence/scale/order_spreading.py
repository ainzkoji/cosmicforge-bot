"""Platform order coordination when many accounts answer one signal (Section H, Step 5.5).

When hundreds of accounts act on the same daily signal, each account's order is still sized and owned by that
account (``client_order_id`` per account, no pooled position), but the platform must not send them all into
one symbol at once. This module decides **how much** combined notional the platform may send for a
(venue, symbol, side) and **in which order and when** each account's share goes out.

1. **Depth evidence.** A ``DepthSnapshot`` is refused when it is older than the policy allows
   (``DEPTH_STALE``), too thin (``DEPTH_INSUFFICIENT_LEVELS``) or crossed. Stale depth is never treated as
   current.
2. **Aggregate cap.** The platform may take at most ``max_participation`` of the visible notional that lies
   within the permitted worst price (reference price +/- ``max_slippage_bps``), optionally capped absolutely.
   The default policy is deliberately conservative and is stored with ``calibrated = 0``: it is a safety
   mechanism, **not** a measured capacity. A calibrated policy needs an ``approval_ref`` (enforced by the
   schema too) -- nothing in this module creates one.
3. **Shared budget.** ``PlatformOrderBudget`` keeps the cap and every account's share in the database.
   Allocation runs under a lock on the budget row and counts everything already active for the same
   (venue, symbol, side) on any unexpired budget, so two workers -- or two signals -- cannot each spend the
   full allowance.
4. **Fair, reproducible order.** Accounts first ``request``; ``allocate`` then grants in the order of
   ``sha256(signal_id | account_id)``: the same inputs always give the same order, and a different signal
   gives a different one, so no customer is always first. Late requests are allocated from what is left, in
   the same key order.
5. **Bounded scheduling.** At most ``max_orders_per_slot`` orders start per ``slot_interval_ms``.
6. **Prices.** ``limit_price`` never lies beyond the permitted worst price (rounded inward to the tick);
   ``market_fallback_permitted`` allows a market order only when walking the *current* book for the remaining
   quantity stays within that worst price.
7. **Retries** are idempotent: one slot per (budget, account), deterministic client order ids.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Iterable, List, Optional, Tuple

from shared_lib.persistence.sql import Connection, Database, decimal_in, decimal_out
from shared_lib.persistence.sql.platform_schema import ACTIVE_SLOT_STATES

BUY, SELL = "BUY", "SELL"
BPS = Decimal("0.0001")


class DepthRefused(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


@dataclass(frozen=True)
class SpreadPolicy:
    policy_version: str = "conservative-uncalibrated-1"
    max_participation: Decimal = Decimal("0.10")
    max_slippage_bps: Decimal = Decimal("25")
    max_depth_age_ms: int = 5_000           # the venue cost model's own freshness bound (venue/policy.py)
    min_levels: int = 5                     # same source: min_reliable_depth_levels
    absolute_cap_notional: Optional[Decimal] = None
    slot_interval_ms: int = 1_000
    max_orders_per_slot: int = 10
    budget_ttl_ms: int = 15 * 60_000
    allow_market_fallback: bool = False
    calibrated: bool = False
    approval_ref: Optional[str] = None

    def __post_init__(self) -> None:
        if self.calibrated and not self.approval_ref:
            raise ValueError("CALIBRATED_POLICY_REQUIRES_APPROVAL_REF")
        if not (Decimal(0) < self.max_participation <= Decimal(1)):
            raise ValueError("PARTICIPATION_OUT_OF_RANGE")
        if self.max_slippage_bps <= 0 or self.max_orders_per_slot < 1 or self.slot_interval_ms < 1:
            raise ValueError("SPREAD_POLICY_INVALID")


CONSERVATIVE_DEFAULT = SpreadPolicy()


@dataclass(frozen=True)
class DepthSnapshot:
    venue: str
    symbol: str
    bids: Tuple[Tuple[Decimal, Decimal], ...]   # (price, quantity), best first
    asks: Tuple[Tuple[Decimal, Decimal], ...]
    observed_at_ms: int

    @classmethod
    def of(cls, venue, symbol, bids, asks, observed_at_ms) -> "DepthSnapshot":
        conv = lambda side: tuple((Decimal(str(p)), Decimal(str(q))) for p, q in side)  # noqa: E731
        return cls(venue, symbol, conv(bids), conv(asks), int(observed_at_ms))

    @property
    def digest(self) -> str:
        body = json.dumps([self.venue, self.symbol, [[str(p), str(q)] for p, q in self.bids],
                           [[str(p), str(q)] for p, q in self.asks], self.observed_at_ms])
        return hashlib.sha256(body.encode()).hexdigest()


@dataclass(frozen=True)
class CapDecision:
    cap_notional: Decimal
    reference_price: Decimal
    worst_price: Decimal
    band_notional: Decimal
    levels_used: int


def _book_side(depth: DepthSnapshot, side: str):
    return depth.asks if side == BUY else depth.bids


def aggregate_cap(depth: DepthSnapshot, side: str, policy: SpreadPolicy, *, now_ms: int) -> CapDecision:
    if side not in (BUY, SELL):
        raise ValueError("SIDE_INVALID")
    age = int(now_ms) - depth.observed_at_ms
    if age > policy.max_depth_age_ms or age < -policy.max_depth_age_ms:
        raise DepthRefused("DEPTH_STALE", f"{age} ms old, limit {policy.max_depth_age_ms} ms")
    if depth.bids and depth.asks and depth.bids[0][0] >= depth.asks[0][0]:
        raise DepthRefused("DEPTH_CROSSED")
    levels = _book_side(depth, side)
    if len(levels) < policy.min_levels:
        raise DepthRefused("DEPTH_INSUFFICIENT_LEVELS", f"{len(levels)} < {policy.min_levels}")
    ref = levels[0][0]
    slip = policy.max_slippage_bps * BPS
    worst = ref * (1 + slip) if side == BUY else ref * (1 - slip)
    within = [(p, q) for p, q in levels if (p <= worst if side == BUY else p >= worst)]
    band = sum((p * q for p, q in within), Decimal(0))
    cap = band * policy.max_participation
    if policy.absolute_cap_notional is not None:
        cap = min(cap, policy.absolute_cap_notional)
    return CapDecision(cap, ref, worst, band, len(within))


def limit_price(worst_price: Decimal, side: str, tick: Decimal) -> Decimal:
    """The most aggressive price still inside the worst permitted price, on the tick."""
    steps = worst_price / tick
    rounded = steps.to_integral_value(rounding=ROUND_FLOOR if side == BUY else ROUND_CEILING)
    return rounded * tick


def market_fallback_permitted(depth: DepthSnapshot, side: str, quantity: Decimal, worst_price: Decimal, *,
                              policy: SpreadPolicy, now_ms: int) -> Tuple[bool, str]:
    if not policy.allow_market_fallback:
        return False, "MARKET_FALLBACK_DISABLED_BY_POLICY"
    try:
        aggregate_cap(depth, side, policy, now_ms=now_ms)
    except DepthRefused as exc:
        return False, exc.code
    left = Decimal(quantity)
    for p, q in _book_side(depth, side):
        if (p > worst_price) if side == BUY else (p < worst_price):
            return False, "MARKET_WALK_EXCEEDS_WORST_PRICE"
        left -= q
        if left <= 0:
            return True, "WITHIN_WORST_PRICE"
    return False, "DEPTH_INSUFFICIENT_FOR_QUANTITY"


def rank_key(signal_id: str, account_id: str) -> str:
    return hashlib.sha256(f"{signal_id}|{account_id}".encode()).hexdigest()


def sequence(signal_id: str, account_ids: Iterable[str]) -> List[str]:
    return sorted(set(account_ids), key=lambda a: (rank_key(signal_id, a), a))


def slot_client_order_id(budget_id: str, account_id: str) -> str:
    return "cs" + hashlib.sha256(f"{budget_id}|{account_id}".encode()).hexdigest()[:30]


@dataclass
class Slot:
    slot_id: str
    account_id: str
    tenant_id: str
    status: str
    sequence: int
    requested: Decimal
    granted: Decimal
    limit_price: Optional[Decimal]
    client_order_id: str
    scheduled_at: int
    reason: Optional[str] = None


@dataclass
class Allocation:
    budget_id: str
    cap: Decimal
    used_elsewhere: Decimal
    granted_total: Decimal
    slots: List[Slot] = field(default_factory=list)


class BudgetRefused(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class PlatformOrderBudget:
    def __init__(self, db: Database, policy: SpreadPolicy = CONSERVATIVE_DEFAULT) -> None:
        self.db = db
        self.policy = policy

    def open_budget(self, *, venue: str, symbol: str, signal_id: str, side: str, depth: DepthSnapshot) -> str:
        """One budget per (venue, symbol, signal, side). A second worker opening it gets the first one's
        budget (and its depth evidence); depth is checked against the database clock."""
        with self.db.transaction() as conn:
            existing = conn.scalar("SELECT budget_id FROM platform_symbol_budgets WHERE venue=? AND symbol=? AND "
                                   "signal_id=? AND side=?", (venue, symbol, signal_id, side))
            if existing:
                return existing
            now = conn.now_ms()
            cap = aggregate_cap(depth, side, self.policy, now_ms=now)
            budget_id = uuid.uuid4().hex
            cur = conn.execute(
                "INSERT INTO platform_symbol_budgets (budget_id, venue, symbol, signal_id, side, cap_notional, calibrated, "
                "policy_version, depth_hash, depth_observed_at, reference_price, worst_price, approval_ref, created_at, "
                "expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (venue, symbol, signal_id, side) DO NOTHING",
                (budget_id, venue, symbol, signal_id, side, decimal_in(cap.cap_notional), 1 if self.policy.calibrated else 0,
                 self.policy.policy_version, depth.digest, depth.observed_at_ms, decimal_in(cap.reference_price),
                 decimal_in(cap.worst_price), self.policy.approval_ref, now, now + self.policy.budget_ttl_ms))
            if cur.rowcount != 1:
                return conn.scalar("SELECT budget_id FROM platform_symbol_budgets WHERE venue=? AND symbol=? AND "
                                   "signal_id=? AND side=?", (venue, symbol, signal_id, side))
            return budget_id

    def request(self, budget_id: str, account_id: str, tenant_id: str, notional) -> None:
        """Register an account's wanted notional. Idempotent per (budget, account)."""
        with self.db.transaction() as conn:
            row = conn.one("SELECT signal_id, expires_at FROM platform_symbol_budgets WHERE budget_id=?", (budget_id,))
            if row is None:
                raise BudgetRefused("BUDGET_UNKNOWN")
            now = conn.now_ms()
            if int(row[1]) <= now:
                raise BudgetRefused("BUDGET_EXPIRED")
            conn.execute(
                "INSERT INTO platform_order_slots (slot_id, budget_id, account_id, tenant_id, sequence, requested_notional, "
                "granted_notional, limit_price, client_order_id, status, scheduled_at, expires_at, created_at, updated_at, "
                "rank_key) VALUES (?,?,?,?,0,?,'0',NULL,?,'REQUESTED',0,?,?,?,?) ON CONFLICT (budget_id, account_id) DO NOTHING",
                (uuid.uuid4().hex, budget_id, account_id, tenant_id, decimal_in(notional),
                 slot_client_order_id(budget_id, account_id), int(row[1]), now, now, rank_key(row[0], account_id)))

    def _used_elsewhere(self, conn: Connection, budget_id: str, venue: str, symbol: str, side: str, now: int) -> Decimal:
        rows = conn.all(
            "SELECT s.granted_notional FROM platform_order_slots s JOIN platform_symbol_budgets b ON b.budget_id=s.budget_id "
            "WHERE b.venue=? AND b.symbol=? AND b.side=? AND b.expires_at > ? AND b.budget_id <> ? AND s.status IN ("
            + ",".join("?" * len(ACTIVE_SLOT_STATES)) + ")", (venue, symbol, side, now, budget_id, *ACTIVE_SLOT_STATES))
        return sum((decimal_out(r[0]) for r in rows), Decimal(0))

    def allocate(self, budget_id: str, *, tick: Decimal, min_notional: Decimal = Decimal(0)) -> Allocation:
        """Grant every REQUESTED slot in rank order until the shared cap is spent. Safe to call from every worker
        at once: the budget row is locked and already-allocated slots are left as they are."""
        with self.db.transaction() as conn:
            b = conn.one("SELECT venue, symbol, side, cap_notional, worst_price, expires_at, created_at FROM "
                         "platform_symbol_budgets WHERE budget_id=?" + conn.lock_rows_suffix(), (budget_id,))
            if b is None:
                raise BudgetRefused("BUDGET_UNKNOWN")
            venue, symbol, side = b[0], b[1], b[2]
            cap, worst = decimal_out(b[3]), decimal_out(b[4])
            now = conn.now_ms()
            if int(b[5]) <= now:
                raise BudgetRefused("BUDGET_EXPIRED")
            if conn.dialect == "postgresql":  # serialise allocations of the same symbol across budgets
                conn.execute("SELECT pg_advisory_xact_lock(hashtext(?))", (f"{venue}|{symbol}|{side}",))
            elsewhere = self._used_elsewhere(conn, budget_id, venue, symbol, side, now)
            rows = conn.all("SELECT slot_id, account_id, tenant_id, status, sequence, requested_notional, granted_notional, "
                            "limit_price, client_order_id, scheduled_at, reason, rank_key FROM platform_order_slots "
                            "WHERE budget_id=? ORDER BY rank_key, account_id", (budget_id,))
            here = sum((decimal_out(r[6]) for r in rows if r[3] in ACTIVE_SLOT_STATES), Decimal(0))
            remaining = cap - elsewhere - here
            next_seq = max([int(r[4]) for r in rows if r[3] != "REQUESTED"] + [0])
            price = limit_price(worst, side, tick)
            for r in rows:
                if r[3] != "REQUESTED":
                    continue
                want = decimal_out(r[5])
                grant = min(want, max(remaining, Decimal(0)))
                next_seq += 1
                scheduled = int(b[6]) + ((next_seq - 1) // self.policy.max_orders_per_slot) * self.policy.slot_interval_ms
                if grant <= 0 or grant < min_notional:
                    conn.execute("UPDATE platform_order_slots SET status='DEFERRED', sequence=?, reason=?, updated_at=? "
                                 "WHERE slot_id=?", (next_seq, "PLATFORM_SYMBOL_BUDGET_EXHAUSTED", now, r[0]))
                    continue
                remaining -= grant
                conn.execute("UPDATE platform_order_slots SET status='RESERVED', sequence=?, granted_notional=?, "
                             "limit_price=?, scheduled_at=?, reason=?, updated_at=? WHERE slot_id=?",
                             (next_seq, decimal_in(grant), decimal_in(price), scheduled,
                              None if grant == want else "PARTIAL_GRANT_BUDGET_LIMIT", now, r[0]))
            conn.execute("UPDATE platform_symbol_budgets SET allocated_at=COALESCE(allocated_at, ?) WHERE budget_id=?",
                         (now, budget_id))
            slots = self._slots_on(conn, budget_id)
        granted = sum((s.granted for s in slots if s.status in ACTIVE_SLOT_STATES), Decimal(0))
        return Allocation(budget_id, cap, elsewhere, granted, slots)

    @staticmethod
    def _slots_on(conn: Connection, budget_id: str) -> List[Slot]:
        return [Slot(r[0], r[1], r[2], r[3], int(r[4]), decimal_out(r[5]), decimal_out(r[6]), decimal_out(r[7]), r[8],
                     int(r[9]), r[10])
                for r in conn.all("SELECT slot_id, account_id, tenant_id, status, sequence, requested_notional, "
                                  "granted_notional, limit_price, client_order_id, scheduled_at, reason "
                                  "FROM platform_order_slots WHERE budget_id=? ORDER BY sequence, rank_key", (budget_id,))]

    def slots(self, budget_id: str) -> List[Slot]:
        with self.db.transaction(write=False) as conn:
            return self._slots_on(conn, budget_id)

    def due(self, budget_id: str) -> List[Slot]:
        with self.db.transaction(write=False) as conn:
            now = conn.now_ms()
            return [s for s in self._slots_on(conn, budget_id) if s.status == "RESERVED" and s.scheduled_at <= now]

    _TRANSITIONS = {"SUBMITTED": ("RESERVED",), "FILLED": ("SUBMITTED",),
                    "RELEASED": ("RESERVED", "SUBMITTED", "DEFERRED", "REQUESTED"),
                    "EXPIRED": ("RESERVED", "REQUESTED", "DEFERRED")}

    def mark(self, slot_id: str, status: str, *, tenant_id: str, filled_notional=None) -> None:
        """``tenant_id`` must match the slot's owner: a worker cannot move another customer's slot by id."""
        allowed = self._TRANSITIONS.get(status)
        if allowed is None:
            raise ValueError(f"SLOT_TRANSITION_UNKNOWN:{status}")
        with self.db.transaction() as conn:
            sets = "status=?, updated_at=?"
            params: list = [status, conn.now_ms()]
            if status == "FILLED" and filled_notional is not None:
                sets += ", granted_notional=?"
                params.append(decimal_in(filled_notional))
            cur = conn.execute(f"UPDATE platform_order_slots SET {sets} WHERE slot_id=? AND tenant_id=? AND status IN ("
                               + ",".join("?" * len(allowed)) + ")", (*params, slot_id, tenant_id, *allowed))
            if cur.rowcount != 1:
                raise BudgetRefused("SLOT_TRANSITION_REFUSED")
