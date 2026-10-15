"""Request budgets per venue and server address, shared by every worker of a host (Section H, Step 4.5).

Why this exists: a venue counts requests against the public address they come from, not against the customer
whose key signed them. A hundred connected accounts behind one address therefore share ONE allowance, and the
only rate-limit state the platform had (``app.exchange.binance.client``) lives inside one process and knows one
venue. This module keeps the allowance in the database, so every process that opens the same database draws
from the same budget and a restart forgets nothing.

How a request is admitted:

* The caller names the venue, the server address its requests leave from (it is never discovered here: no
  network call is made by this module), a weight and a priority class, and asks BEFORE it calls the venue.
* The budget is an exact sliding log: the weight granted in the last window is summed, so no window of the
  venue, however it is aligned, can receive more than the configured capacity from this database.
* Each priority class may fill the budget only up to its ceiling. Market data stops first, then new entries,
  then reconciliation; protection keeps capacity nothing below it can use and an emergency flatten keeps the
  last of it. A new entry can never be raised above its own class.
* After a call the caller reports what the venue said (``observe``). Usage the venue reports above the local
  sum (another process on the same address, an underestimated weight) is added to the log.
* HTTP 429 closes the budget until Retry-After; HTTP 418 (or any ban) closes it until the ban ends. A closed
  budget refuses every class, emergency included: a request to an address the venue has told to wait cannot
  succeed and lengthens the ban. A closure ends by time only. There is no reopen call and no bypass.
* A refusal always says when the caller may ask again.

Venue limits. Only a value that could be substantiated is used as a venue fact (see ``DEFAULT_LIMITS`` and
``docs/exchanges/EXCHANGE_OPERATIONS.md``); every other default is a deliberately low configuration value
marked ``UNVERIFIED`` that an operator replaces after reading the venue's current documentation.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from app.exchange.platform_adapter import contract as C

from shared_lib.persistence.cati_exchange_schema import RATE_BUDGET_TABLE, RATE_DENIAL_TABLE, RATE_RESERVATION_TABLE

EMERGENCY, PROTECTION, RECONCILIATION, ENTRY, MARKET_DATA = (
    "EMERGENCY", "PROTECTION", "RECONCILIATION", "ENTRY", "MARKET_DATA")
#: highest first; an earlier class always outranks a later one
PRIORITIES: Tuple[str, ...] = (EMERGENCY, PROTECTION, RECONCILIATION, ENTRY, MARKET_DATA)
#: the share of the usable capacity a class may fill. The gap between two neighbours is capacity the lower
#: class can never take: 15% for reconciliation above entries, 15% for protection above reconciliation and a
#: final 5% only an emergency close can use.
DEFAULT_CEILINGS: Mapping[str, float] = {
    EMERGENCY: 1.0, PROTECTION: 0.95, RECONCILIATION: 0.80, ENTRY: 0.65, MARKET_DATA: 0.50}
#: share of the venue's limit this database plans to use at all (the rest absorbs what it cannot see)
DEFAULT_UTILISATION = 0.8

VERIFIED_BY_DOC, UNVERIFIED = "VERIFIED_BY_DOC", "UNVERIFIED"
WEIGHT, REQUESTS = "WEIGHT", "REQUESTS"
SCOPE_IP, SCOPE_UID = "IP", "UID"

REASON_CLOSED_RATE_LIMITED = "BUDGET_CLOSED_RATE_LIMITED"
REASON_CLOSED_BANNED = "BUDGET_CLOSED_BANNED"
REASON_ACCOUNT_CLOSED = "ACCOUNT_BUDGET_CLOSED"
REASON_EXHAUSTED = "BUDGET_EXHAUSTED_FOR_PRIORITY"
REASON_WEIGHT_TOO_LARGE = "WEIGHT_EXCEEDS_PRIORITY_CEILING"

#: closure applied when the venue refused but did not say for how long (configuration, not a venue fact)
DEFAULT_RATE_LIMITED_CLOSURE_MS = 60_000
DEFAULT_BAN_CLOSURE_MS = 600_000
#: how long granted rows are kept for the usage breakdown after they left the window
DEFAULT_RETENTION_MS = 600_000
#: marker rows for usage the venue reported and this database had not granted
OBSERVED = "OBSERVED"


@dataclass(frozen=True)
class VenueLimit:
    """What one server address may send to one venue in one window."""
    venue: str
    capacity: float
    window_ms: int
    unit: str                           # WEIGHT | REQUESTS
    verification: str                   # VERIFIED_BY_DOC | UNVERIFIED
    source: str

    def __post_init__(self) -> None:
        if self.venue not in C.VENUES:
            raise ValueError(f"unknown venue {self.venue!r}")
        if not (math.isfinite(self.capacity) and self.capacity > 0 and int(self.window_ms) > 0):
            raise ValueError("a limit needs a positive capacity and window")
        if self.unit not in (WEIGHT, REQUESTS) or self.verification not in (VERIFIED_BY_DOC, UNVERIFIED):
            raise ValueError("unknown unit or verification label")


DEFAULT_LIMITS: Mapping[str, VenueLimit] = {
    C.BINANCE: VenueLimit(
        C.BINANCE, 2400.0, 60_000, WEIGHT, VERIFIED_BY_DOC,
        "USD-M futures REQUEST_WEIGHT limit per IP and minute, as published in the rateLimits of "
        "GET /fapi/v1/exchangeInfo and reported per response in X-MBX-USED-WEIGHT-1M; the same figure as "
        "app.exchange.binance.client.REQUEST_WEIGHT_LIMIT_1M. The venue's own figure, when an adapter "
        "reports one that is lower, replaces it."),
    C.BYBIT: VenueLimit(
        C.BYBIT, 150.0, 5_000, REQUESTS, UNVERIFIED,
        "Conservative placeholder. The V5 documentation is recalled as allowing 600 requests per 5 seconds "
        "per IP; that figure was not re-read for this build, so a quarter of it is used until an operator "
        "confirms the current limit."),
    C.BINGX: VenueLimit(
        C.BINGX, 20.0, 10_000, REQUESTS, UNVERIFIED,
        "Conservative placeholder. No BingX swap rate limit could be substantiated for this build; an "
        "operator must set the documented per-IP limit before this venue carries real traffic."),
}

#: Estimated cost of one ``VenueAdapter`` call. UNVERIFIED as a mapping: which HTTP requests an adapter sends
#: for a method is the adapter's business. On Binance a wrong estimate is corrected by the usage the venue
#: reports after every response; on the two request-counted venues it stays an estimate.
DEFAULT_METHOD_WEIGHTS: Mapping[str, Mapping[str, float]] = {
    C.BINANCE: {"server_time": 1, "instrument_rules": 5, "balance": 5, "positions": 5, "open_orders": 1,
                "open_orders_all": 40, "order": 1, "fills": 5, "fee_rates": 20, "funding": 1, "user_stream": 1,
                "set_leverage": 1, "place_order": 1, "cancel_order": 1, "amend_stop": 2},
    C.BYBIT: {"instrument_rules": 3, "fills": 2, "amend_stop": 2},
    C.BINGX: {"instrument_rules": 2, "fills": 2, "amend_stop": 2},
}
#: methods that never leave the process
LOCAL_METHODS: Tuple[str, ...] = ("capabilities", "rate_limit")


@dataclass(frozen=True)
class Reservation:
    """The answer to one request for budget. ``granted`` False means the venue must not be called."""
    granted: bool
    venue: str
    server_address: str
    priority: str
    weight: float
    reason: str
    retry_at_ms: Optional[int]          # when the caller may ask again; None when granted or never grantable
    used: float                         # weight in the window before this request
    capacity: float                     # usable capacity of the window (venue limit x utilisation)
    ceiling: float                      # the part of it this priority may fill
    reservation_id: Optional[int] = None

    def retry_after_s(self, now_ms: int) -> Optional[float]:
        return None if self.retry_at_ms is None else max(0.0, (int(self.retry_at_ms) - int(now_ms)) / 1000.0)


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded retry with backoff and jitter. The wait is never shorter than the venue's own instruction and
    jitter is only ever added to it, so no retry arrives before a Retry-After has passed."""
    max_attempts: int = 3
    base_delay_ms: int = 500
    max_delay_ms: int = 30_000
    max_wait_ms: int = 60_000
    jitter_fraction: float = 0.25

    def delay_ms(self, attempt: int, *, now_ms: int, retry_at_ms: Optional[int] = None,
                 rng: Callable[[], float] = random.random) -> Optional[int]:
        """Milliseconds to wait before attempt number ``attempt + 1`` (``attempt`` counts the tries already
        made, from 1). ``None`` = stop: the attempts are spent, or the venue asked for a wait longer than
        ``max_wait_ms`` and the caller must fail now instead of holding its work."""
        if attempt < 1 or attempt >= self.max_attempts:
            return None
        backoff = min(self.base_delay_ms * (2 ** (attempt - 1)), self.max_delay_ms)
        required = max(backoff, 0 if retry_at_ms is None else int(retry_at_ms) - int(now_ms))
        if required > self.max_wait_ms:
            return None
        return int(required + required * self.jitter_fraction * min(1.0, max(0.0, float(rng()))))


def _scope_key(account_ref: str) -> str:
    return f"uid:{account_ref}"


class RateBudget:
    """The shared budget. One object per worker; every object on the same database sees the same state."""

    def __init__(self, db: Any, *, limits: Optional[Mapping[str, VenueLimit]] = None,
                 ceilings: Optional[Mapping[str, float]] = None, utilisation: float = DEFAULT_UTILISATION,
                 worker: str = "", retention_ms: int = DEFAULT_RETENTION_MS,
                 rate_limited_closure_ms: int = DEFAULT_RATE_LIMITED_CLOSURE_MS,
                 ban_closure_ms: int = DEFAULT_BAN_CLOSURE_MS) -> None:
        self.db = db
        self.limits: Dict[str, VenueLimit] = dict(DEFAULT_LIMITS if limits is None else limits)
        self.ceilings: Dict[str, float] = dict(DEFAULT_CEILINGS if ceilings is None else ceilings)
        if set(self.ceilings) != set(PRIORITIES):
            raise ValueError("a ceiling is required for every priority class and for nothing else")
        ordered = [float(self.ceilings[p]) for p in PRIORITIES]
        if any(not 0.0 < c <= 1.0 for c in ordered) or ordered != sorted(ordered, reverse=True):
            raise ValueError("ceilings must lie in (0, 1] and never rise from a higher priority to a lower one")
        if not 0.0 < float(utilisation) <= 1.0:
            raise ValueError("utilisation must lie in (0, 1]")
        self.utilisation = float(utilisation)
        self.worker = str(worker)
        self.retention_ms = int(retention_ms)
        self.rate_limited_closure_ms = int(rate_limited_closure_ms)
        self.ban_closure_ms = int(ban_closure_ms)

    # ------------------------------------------------------------------------------------------- internals
    def _limit(self, venue: str) -> VenueLimit:
        limit = self.limits.get(venue)
        if limit is None:
            # an unknown venue is never treated as unlimited
            raise ValueError(f"no rate limit is configured for venue {venue!r}")
        return limit

    @staticmethod
    def _address(server_address: str) -> str:
        address = str(server_address or "").strip()
        if not address or address.startswith("uid:"):
            raise ValueError("the caller supplies the server address its requests leave from")
        return address

    def _row(self, conn: Any, venue: str, scope_key: str, scope: str, now_ms: int) -> Any:
        limit = self._limit(venue)
        conn.execute(
            f"INSERT OR IGNORE INTO {RATE_BUDGET_TABLE} (venue, scope_key, scope, capacity, window_ms, updated_at_ms) "
            "VALUES (?,?,?,?,?,?)", (venue, scope_key, scope, limit.capacity, limit.window_ms, int(now_ms)))
        return conn.execute(f"SELECT * FROM {RATE_BUDGET_TABLE} WHERE venue=? AND scope_key=?",
                            (venue, scope_key)).fetchone()

    def _capacity(self, limit: VenueLimit, row: Any) -> float:
        """Usable capacity: the configured limit, or the venue's own figure when that is lower."""
        venue_limit = limit.capacity
        observed = row["observed_limit"]
        if observed is not None and row["observed_window_ms"] == limit.window_ms and 0 < float(observed) < venue_limit:
            venue_limit = float(observed)
        return venue_limit * self.utilisation

    @staticmethod
    def _used(conn: Any, venue: str, scope_key: str, now_ms: int, window_ms: int) -> float:
        row = conn.execute(
            f"SELECT COALESCE(SUM(weight), 0) FROM {RATE_RESERVATION_TABLE} WHERE venue=? AND scope_key=? AND "
            "reserved_at_ms > ?", (venue, scope_key, int(now_ms) - int(window_ms))).fetchone()
        return float(row[0] or 0.0)

    def _deny(self, conn: Any, venue: str, scope_key: str, priority: str, reason: str, now_ms: int) -> None:
        conn.execute(
            f"INSERT INTO {RATE_DENIAL_TABLE} (venue, scope_key, priority, reason, denials, first_at_ms, last_at_ms) "
            "VALUES (?,?,?,?,1,?,?) ON CONFLICT(venue, scope_key, priority, reason) DO UPDATE SET "
            "denials=denials+1, last_at_ms=excluded.last_at_ms", (venue, scope_key, priority, reason, int(now_ms),
                                                                  int(now_ms)))

    def _free_at(self, conn: Any, venue: str, scope_key: str, now_ms: int, window_ms: int, used: float, weight: float,
                 ceiling: float) -> int:
        """The first instant at which enough granted weight has left the window for ``weight`` to fit."""
        freed = 0.0
        rows = conn.execute(
            f"SELECT reserved_at_ms, weight FROM {RATE_RESERVATION_TABLE} WHERE venue=? AND scope_key=? AND "
            "reserved_at_ms > ? ORDER BY reserved_at_ms, id", (venue, scope_key, int(now_ms) - int(window_ms)))
        for row in rows:
            freed += float(row[1])
            if used - freed + weight <= ceiling + 1e-9:
                return max(int(now_ms) + 1, int(row[0]) + int(window_ms))
        return int(now_ms) + int(window_ms)

    # ---------------------------------------------------------------------------------------------- reserve
    def reserve(self, venue: str, server_address: str, now_ms: int, *, priority: str, weight: float = 1.0,
                category: str = "", account_ref: Optional[str] = None) -> Reservation:
        """Ask for ``weight`` before calling the venue. One transaction decides and records; when the answer
        is a refusal nothing is reserved and ``retry_at_ms`` says when to ask again."""
        if priority not in PRIORITIES:
            raise ValueError(f"unknown priority {priority!r}")
        weight = float(weight)
        if not (math.isfinite(weight) and weight > 0):
            raise ValueError("a request weight is a positive number")
        limit, address, now_ms = self._limit(venue), self._address(server_address), int(now_ms)
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._row(conn, venue, address, SCOPE_IP, now_ms)
            capacity = self._capacity(limit, row)
            ceiling = capacity * float(self.ceilings[priority])
            used = self._used(conn, venue, address, now_ms, limit.window_ms)

            def refuse(reason: str, retry_at_ms: Optional[int]) -> Reservation:
                self._deny(conn, venue, address, priority, reason, now_ms)
                return Reservation(False, venue, address, priority, weight, reason, retry_at_ms, used, capacity,
                                   ceiling)

            if int(row["closed_until_ms"]) > now_ms:
                banned = row["closed_reason"] == C.BANNED
                return refuse(REASON_CLOSED_BANNED if banned else REASON_CLOSED_RATE_LIMITED,
                              int(row["closed_until_ms"]))
            if account_ref:
                account = conn.execute(
                    f"SELECT closed_until_ms FROM {RATE_BUDGET_TABLE} WHERE venue=? AND scope_key=?",
                    (venue, _scope_key(account_ref))).fetchone()
                if account is not None and int(account[0]) > now_ms:
                    return refuse(REASON_ACCOUNT_CLOSED, int(account[0]))
            if weight > ceiling + 1e-9:
                return refuse(REASON_WEIGHT_TOO_LARGE, None)
            if used + weight > ceiling + 1e-9:
                return refuse(REASON_EXHAUSTED,
                              self._free_at(conn, venue, address, now_ms, limit.window_ms, used, weight, ceiling))
            conn.execute(f"DELETE FROM {RATE_RESERVATION_TABLE} WHERE venue=? AND scope_key=? AND reserved_at_ms <= ?",
                         (venue, address, now_ms - max(self.retention_ms, limit.window_ms)))
            cursor = conn.execute(
                f"INSERT INTO {RATE_RESERVATION_TABLE} (venue, scope_key, reserved_at_ms, weight, priority, category, "
                "account_ref, worker) VALUES (?,?,?,?,?,?,?,?)",
                (venue, address, now_ms, weight, priority, str(category), account_ref, self.worker))
            return Reservation(True, venue, address, priority, weight, "", None, used, capacity, ceiling,
                               int(cursor.lastrowid))

    def entries_allowed(self, venue: str, server_address: str, now_ms: int, *, weight: float = 1.0,
                        account_ref: Optional[str] = None) -> Tuple[bool, str, Optional[int]]:
        """Would a new entry be admitted now? Reserves nothing. ``(False, reason, retry_at_ms)`` is the
        circuit breaker a caller uses to stop building entries it could not send."""
        limit, address, now_ms = self._limit(venue), self._address(server_address), int(now_ms)
        with self.db.connect() as conn:
            row = conn.execute(f"SELECT * FROM {RATE_BUDGET_TABLE} WHERE venue=? AND scope_key=?",
                               (venue, address)).fetchone()
            if row is not None and int(row["closed_until_ms"]) > now_ms:
                banned = row["closed_reason"] == C.BANNED
                return (False, REASON_CLOSED_BANNED if banned else REASON_CLOSED_RATE_LIMITED,
                        int(row["closed_until_ms"]))
            if account_ref:
                account = conn.execute(
                    f"SELECT closed_until_ms FROM {RATE_BUDGET_TABLE} WHERE venue=? AND scope_key=?",
                    (venue, _scope_key(account_ref))).fetchone()
                if account is not None and int(account[0]) > now_ms:
                    return False, REASON_ACCOUNT_CLOSED, int(account[0])
            capacity = limit.capacity * self.utilisation if row is None else self._capacity(limit, row)
            ceiling = capacity * float(self.ceilings[ENTRY])
            used = self._used(conn, venue, address, now_ms, limit.window_ms)
            if used + float(weight) > ceiling + 1e-9:
                return False, REASON_EXHAUSTED, self._free_at(conn, venue, address, now_ms, limit.window_ms, used,
                                                               float(weight), ceiling)
        return True, "", None

    # ------------------------------------------------------------------------------- what the venue answered
    def record_rejection(self, venue: str, server_address: str, now_ms: int, *, status: Optional[int],
                         retry_after_s: Optional[float] = None, banned_until_ms: Optional[int] = None,
                         banned: Optional[bool] = None, scope: str = SCOPE_IP,
                         account_ref: Optional[str] = None) -> int:
        """The venue refused for rate (HTTP 429) or banned the address (HTTP 418). Returns the instant the
        budget reopens. A closure is only ever extended: a later, shorter instruction does not shorten it."""
        self._limit(venue)
        now_ms = int(now_ms)
        is_ban = bool(banned) if banned is not None else (status == 418 or banned_until_ms is not None)
        if banned_until_ms is not None:
            until = int(banned_until_ms)
        elif retry_after_s is not None and math.isfinite(float(retry_after_s)) and float(retry_after_s) >= 0:
            until = now_ms + int(math.ceil(float(retry_after_s) * 1000.0))
        else:
            until = now_ms + (self.ban_closure_ms if is_ban else self.rate_limited_closure_ms)
        until = max(until, now_ms + 1)
        if scope == SCOPE_UID:
            if not account_ref:
                raise ValueError("an account-scoped rejection names the account it belongs to")
            key = _scope_key(account_ref)
        elif scope == SCOPE_IP:
            key = self._address(server_address)
        else:
            raise ValueError(f"unknown scope {scope!r}")
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._row(conn, venue, key, scope, now_ms)
            standing = int(row["closed_until_ms"])
            if until <= standing and (row["closed_reason"] == C.BANNED or not is_ban):
                return standing  # the same instruction reported twice, or a shorter one: nothing changes
            still_banned = row["closed_reason"] == C.BANNED and standing > now_ms
            conn.execute(
                f"UPDATE {RATE_BUDGET_TABLE} SET closed_until_ms=?, closed_reason=?, closed_status=?, closed_at_ms=?, "
                "closures=closures+1, updated_at_ms=? WHERE venue=? AND scope_key=?",
                (max(until, standing), C.BANNED if (is_ban or still_banned) else C.RATE_LIMITED, status, now_ms,
                 now_ms, venue, key))
        return max(until, standing)

    def note_error(self, error: C.AdapterError, server_address: str, now_ms: int, *, venue: Optional[str] = None,
                   scope: str = SCOPE_IP, account_ref: Optional[str] = None) -> Optional[int]:
        """Close the budget when an adapter error is a rate refusal or a ban. Returns the reopening instant,
        or ``None`` when the error is neither."""
        if error.code not in (C.RATE_LIMITED, C.BANNED):
            return None
        return self.record_rejection(
            venue or error.venue, server_address, now_ms, status=error.http_status, retry_after_s=error.retry_after_s,
            banned_until_ms=error.banned_until_ms, banned=error.code == C.BANNED, scope=scope, account_ref=account_ref)

    def observe(self, observation: C.RateLimitObservation, server_address: str, *,
                account_ref: Optional[str] = None) -> None:
        """Take in what the venue said after a call: its own count, its own limit and any instruction to wait."""
        limit = self._limit(observation.venue)
        now_ms = int(observation.observed_at_ms)
        window_ms = None if observation.window_seconds is None else int(observation.window_seconds) * 1000
        if observation.retry_after_s is not None or observation.banned_until_ms is not None:
            self.record_rejection(observation.venue, server_address, now_ms, status=None,
                                  retry_after_s=observation.retry_after_s,
                                  banned_until_ms=observation.banned_until_ms, scope=observation.scope,
                                  account_ref=account_ref)
        if observation.scope != SCOPE_IP:
            return  # an account-scoped count belongs to one endpoint of one account, not to this window
        address = self._address(server_address)
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._row(conn, observation.venue, address, SCOPE_IP, now_ms)
            conn.execute(
                f"UPDATE {RATE_BUDGET_TABLE} SET observed_used=?, observed_limit=?, observed_window_ms=?, "
                "observed_at_ms=?, updated_at_ms=? WHERE venue=? AND scope_key=?",
                (observation.used_weight, observation.limit, window_ms, now_ms, now_ms, observation.venue, address))
            same_window = window_ms is None or window_ms == limit.window_ms
            if observation.used_weight is not None and same_window:
                unseen = float(observation.used_weight) - self._used(conn, observation.venue, address, now_ms,
                                                                    limit.window_ms)
                if unseen > 1e-9:
                    conn.execute(
                        f"INSERT INTO {RATE_RESERVATION_TABLE} (venue, scope_key, reserved_at_ms, weight, priority, "
                        "category, account_ref, worker) VALUES (?,?,?,?,?,?,?,?)",
                        (observation.venue, address, now_ms, unseen, OBSERVED, "VENUE_REPORTED_USAGE", None,
                         self.worker))

    # ---------------------------------------------------------------------------------------- observability
    def snapshot(self, venue: str, server_address: str, now_ms: int) -> Dict[str, Any]:
        """The state of one budget for an operator: usage by class, closure, denials, the venue's own words."""
        limit, address, now_ms = self._limit(venue), self._address(server_address), int(now_ms)
        with self.db.connect() as conn:
            row = conn.execute(f"SELECT * FROM {RATE_BUDGET_TABLE} WHERE venue=? AND scope_key=?",
                               (venue, address)).fetchone()
            by_priority = {r[0]: float(r[1]) for r in conn.execute(
                f"SELECT priority, SUM(weight) FROM {RATE_RESERVATION_TABLE} WHERE venue=? AND scope_key=? AND "
                "reserved_at_ms > ? GROUP BY priority", (venue, address, now_ms - limit.window_ms))}
            denials = {f"{r[0]}:{r[1]}": int(r[2]) for r in conn.execute(
                f"SELECT priority, reason, denials FROM {RATE_DENIAL_TABLE} WHERE venue=? AND scope_key=? "
                "ORDER BY priority, reason", (venue, address))}
        capacity = limit.capacity * self.utilisation if row is None else self._capacity(limit, row)
        closed_until = 0 if row is None else int(row["closed_until_ms"])
        return {
            "venue": venue, "server_address": address, "unit": limit.unit, "window_ms": limit.window_ms,
            "venue_limit": limit.capacity, "limit_verification": limit.verification, "capacity": capacity,
            "used": sum(by_priority.values()), "used_by_priority": by_priority,
            "ceilings": {p: capacity * float(self.ceilings[p]) for p in PRIORITIES},
            "closed": closed_until > now_ms, "closed_until_ms": closed_until if closed_until > now_ms else None,
            "closed_reason": None if row is None or closed_until <= now_ms else row["closed_reason"],
            "closures": 0 if row is None else int(row["closures"]),
            "observed_used": None if row is None else row["observed_used"],
            "observed_limit": None if row is None else row["observed_limit"],
            "observed_at_ms": None if row is None else row["observed_at_ms"],
            "denials": denials,
        }


# ------------------------------------------------------------------------------------- budgeted adapter view
def _mutation_priority(view_priority: str, name: str, args: Sequence[Any]) -> str:
    """A mutation's class comes from what it does. An order that opens or adds exposure, and a leverage
    change, are entries whatever the view asks for; everything that reduces or protects is at least
    PROTECTION and keeps a higher class the view was given."""
    reducing = name in ("cancel_order", "amend_stop")
    if name == "place_order" and args:
        request = args[0]
        reducing = bool(getattr(request, "protective", False) or getattr(request, "reduce_only", False)
                        or getattr(request, "close_position", False))
    if not reducing:
        return ENTRY
    return view_priority if PRIORITIES.index(view_priority) < PRIORITIES.index(PROTECTION) else PROTECTION


class BudgetedVenueAdapter:
    """A ``VenueAdapter`` that asks the budget before every call and reports the venue's answer after it.

    It never retries and never sleeps. A refusal by the budget is raised as the same ``AdapterError`` a venue
    refusal would be (``RATE_LIMITED`` with the wait, or ``BANNED`` with the end of the ban) with
    ``venue_code`` beginning ``LOCAL_BUDGET``: the request did not leave the process.

    ``priority`` is the class of the reads made through this view: ``RECONCILIATION`` for a reconciliation
    pass, ``PROTECTION`` for the read-back of a stop or of an order whose submission outcome is unknown,
    ``EMERGENCY`` for a flatten, ``MARKET_DATA`` for anything else.
    """

    def __init__(self, adapter: Any, budget: RateBudget, *, server_address: str, account_ref: str,
                 clock: Callable[[], int], priority: str = RECONCILIATION,
                 weights: Optional[Mapping[str, float]] = None) -> None:
        if priority not in PRIORITIES:
            raise ValueError(f"unknown priority {priority!r}")
        self._adapter, self._budget, self._clock = adapter, budget, clock
        self._server_address, self._account_ref, self._priority = server_address, account_ref, priority
        self._weights = dict(DEFAULT_METHOD_WEIGHTS.get(adapter.venue, {}) if weights is None else weights)
        self._last_observed_ms = 0
        self.venue, self.environment, self.adapter_version = adapter.venue, adapter.environment, adapter.adapter_version

    def with_priority(self, priority: str) -> "BudgetedVenueAdapter":
        return BudgetedVenueAdapter(self._adapter, self._budget, server_address=self._server_address,
                                    account_ref=self._account_ref, clock=self._clock, priority=priority,
                                    weights=self._weights)

    def _weight(self, name: str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> float:
        if name == "open_orders" and not (args and args[0]) and not kwargs.get("symbol"):
            return float(self._weights.get("open_orders_all", self._weights.get("open_orders", 1.0)))
        return float(self._weights.get(name, 1.0))

    def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        method = getattr(self._adapter, name)
        if name in LOCAL_METHODS:
            return method(*args, **kwargs)
        now_ms = int(self._clock())
        priority = (_mutation_priority(self._priority, name, args) if name in C.MUTATING_METHODS else self._priority)
        from .http_budget import BudgetedHTTP, priority_context
        client = getattr(self._adapter, "_client", None)
        transport = getattr(client, "session", None) or getattr(client, "http", None)
        if isinstance(transport, BudgetedHTTP):
            # Each physical request (including retries) is charged by the transport.
            token = priority_context.set(priority)
            try:
                return method(*args, **kwargs)
            finally:
                priority_context.reset(token)
        granted = self._budget.reserve(self.venue, self._server_address, now_ms, priority=priority,
                                       weight=self._weight(name, args, kwargs), category=name,
                                       account_ref=self._account_ref)
        if not granted.granted:
            banned = granted.reason == REASON_CLOSED_BANNED
            raise C.AdapterError(
                C.BANNED if banned else C.RATE_LIMITED, f"{granted.reason} for {priority}", venue=self.venue,
                retry_after_s=granted.retry_after_s(now_ms), banned_until_ms=granted.retry_at_ms if banned else None,
                venue_code=f"LOCAL_BUDGET:{granted.reason}")
        try:
            return method(*args, **kwargs)
        except C.AdapterError as error:
            self._budget.note_error(error, self._server_address, int(self._clock()), venue=self.venue,
                                    account_ref=self._account_ref)
            raise
        finally:
            self._observe()

    def _observe(self) -> None:
        try:
            observation = self._adapter.rate_limit()
        except C.AdapterError:
            return  # the venue said nothing usable; the local estimate stands
        if observation is None or int(observation.observed_at_ms) <= self._last_observed_ms:
            return
        self._last_observed_ms = int(observation.observed_at_ms)
        if observation.scope == SCOPE_UID:
            if observation.retry_after_s is None and observation.banned_until_ms is None:
                return
            self._budget.observe(observation, self._server_address, account_ref=self._account_ref)
            return
        self._budget.observe(replace(observation, venue=self.venue), self._server_address)

    def __getattr__(self, name: str) -> Any:
        if name in C.READ_METHODS or name in C.MUTATING_METHODS:
            return lambda *args, **kwargs: self._call(name, *args, **kwargs)
        raise AttributeError(name)


__all__ = ["EMERGENCY", "PROTECTION", "RECONCILIATION", "ENTRY", "MARKET_DATA", "PRIORITIES", "DEFAULT_CEILINGS",
           "DEFAULT_UTILISATION", "VERIFIED_BY_DOC", "UNVERIFIED", "WEIGHT", "REQUESTS", "SCOPE_IP", "SCOPE_UID",
           "REASON_CLOSED_RATE_LIMITED", "REASON_CLOSED_BANNED", "REASON_ACCOUNT_CLOSED", "REASON_EXHAUSTED",
           "REASON_WEIGHT_TOO_LARGE", "DEFAULT_LIMITS", "DEFAULT_METHOD_WEIGHTS", "LOCAL_METHODS", "OBSERVED",
           "VenueLimit", "Reservation", "RetryPolicy", "RateBudget", "BudgetedVenueAdapter"]
