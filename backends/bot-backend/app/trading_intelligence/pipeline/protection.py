"""Exchange-side protection of open positions and durable stop replacement (Section H, Step 3.9).

**The invariant.** A position that exists on an exchange rests under an exchange-side stop that covers all of
it, and a stop that protects a position is never taken away before another one is confirmed to protect it. The
single-entry path replaces a stop by cancelling it and creating the next one (``exchange/binance/client.py``,
``modify_protection``): between the two calls the position has no stop, and if the second call fails it keeps
none. Nothing in this module ever cancels a stop in order to create its replacement.

**What a replacement is allowed to assume.** Only what ``adapter.capabilities()`` declares:

``stop_amend_in_place``          (a) ``AMEND_IN_PLACE``: the trigger of the resting stop is changed by one call
                                 and read back. The stop never stops resting.
``multiple_stops_per_position``  (b) ``CREATE_CONFIRM_RETIRE``: the replacement is created beside the old stop,
                                 read back resting and covering the position, and only then is the old stop
                                 cancelled. If that cancel fails both rest (over-protected is safe) and the
                                 retirement is retried later.
neither                          (c) the replacement is REFUSED with ``REPLACEMENT_NOT_POSSIBLE_SAFELY``, the
                                 old stop stays where it is and the refusal is recorded. This is the state of the
                                 Binance and BingX adapters today (one stop per position, no amend): a gap-free
                                 replacement needs a venue-certified overlap protocol that does not exist yet.

**The seven steps** (the assignment's protocol) and where they are in the code: 1 read the position and its
protection (``_coverage``); 2 build a valid replacement (``_build``: never wider than what rests, protective at
the mark); 3 establish it with the venue-safe method (``_amend`` / ``_overlap``); 4 confirm acceptance and
effective coverage by reading it back; 5 retire what it supersedes, only then (``_overlap``); 6 reconcile the
final coverage and quantity from one more read; 7 persist the final state (``_finish``).

**Crash safety.** A replacement is one row of ``cati_protection_replacements`` whose ``step`` is written before
the step is taken, and every venue mutation is bracketed by ``VENUE_BEFORE`` / ``VENUE_AFTER`` journal rows. The
protocol is a function of that row and of what the venue reports NOW: ``resume`` reads the position and the
resting stops again and either finishes the replacement or leaves the old stop in charge. A finished
replacement is final, so a duplicate request with the same key returns the recorded result and calls nothing.

**Ambiguity.** A timeout whose effect cannot be read back, or a read the venue does not answer, leaves the
replacement open, marks the protection ``UNCERTAIN`` and blocks new entries of the account
(``cati_execution_blocks``) until a later read settles it. A 429 is not ambiguity: the time the venue named is
recorded and nothing at all is sent before it.

**What this module does not decide.** When a position has no stop and none can be established it reports
``ABSENT``; closing that position is the execution boundary's job (``pipeline.execution``), with a reduce-only
order. The operator emergency flatten (``integration.production_execution.flatten_account``) is a separate
path and is not touched.

No database transaction of this module spans a venue call.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from app.exchange.platform_adapter import contract as V
from app.exchange.platform_adapter.contract import AdapterError, OrderInfo, OrderRequest, PositionInfo
from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.observability.sanitize import sanitize_text
from app.trading_intelligence.pipeline.contracts import LONG, STOP_REASONS, stop_side_valid
from shared_lib.persistence.cati_execution_schema import (
    BACKOFF_TABLE, BLOCK_ACTIVE, BLOCK_CLEARED, BLOCK_TABLE, EVENT_TABLE, PROTECTION_TABLE, REPLACEMENT_DONE,
    REPLACEMENT_OPEN, REPLACEMENT_REFUSED, REPLACEMENT_TABLE, REPLACEMENT_UNWOUND,
)

# ------------------------------------------------------------------------------------------------ vocabulary
AMEND_IN_PLACE, CREATE_CONFIRM_RETIRE = "AMEND_IN_PLACE", "CREATE_CONFIRM_RETIRE"
PROTOCOLS: Tuple[str, ...] = (AMEND_IN_PLACE, CREATE_CONFIRM_RETIRE)

#: protection status of one position (``cati_protection_state.status``)
PENDING, PROTECTED, UNCERTAIN, UNDER_PROTECTED, ABSENT, RETIRE_PENDING, FLAT = (
    "PENDING", "PROTECTED", "UNCERTAIN", "UNDER_PROTECTED", "ABSENT", "RETIRE_PENDING", "FLAT")
#: statuses ``resume`` works on (``ABSENT`` belongs to the execution boundary, which closes the position)
RESUMABLE: Tuple[str, ...] = (PENDING, UNCERTAIN, UNDER_PROTECTED, RETIRE_PENDING)

#: steps of a replacement, in order (``cati_protection_replacements.step``)
READ, BUILT, ESTABLISHING, ESTABLISHED, CONFIRMED, RETIRING, RECONCILED, PERSISTED = (
    "READ", "BUILT", "ESTABLISHING", "ESTABLISHED", "CONFIRMED", "RETIRING", "RECONCILED", "PERSISTED")

#: outcomes of a replacement
REPLACED, NO_CHANGE, RESTORED, POSITION_CLOSED = "REPLACED", "NO_CHANGE", "RESTORED", "POSITION_CLOSED"
RETIREMENT_PENDING, DEFERRED, OUTCOME_UNCERTAIN = "REPLACED_RETIREMENT_PENDING", "DEFERRED", "UNCERTAIN"
REPLACEMENT_NOT_POSSIBLE_SAFELY = "REPLACEMENT_NOT_POSSIBLE_SAFELY"
STOP_WIDENING_REFUSED = "STOP_WIDENING_REFUSED"
STOP_NOT_PROTECTIVE_AT_MARK = "STOP_NOT_PROTECTIVE_AT_MARK"
POSITION_DIRECTION_MISMATCH = "POSITION_DIRECTION_MISMATCH"
STOP_CHANGED_ELSEWHERE = "STOP_CHANGED_ELSEWHERE"
PROTECTION_ABSENT = "PROTECTION_ABSENT"
CONFLICTING_REPLAY = "CONFLICTING_REPLAY"
NO_VENUE_HELD_STOP = "NO_VENUE_HELD_STOP"
RATE_LIMIT_BACKOFF = "RATE_LIMIT_BACKOFF"

#: entry blocks this module raises; each is lifted by the read that settles it, never by time
BLOCK_PROTECTION_UNSETTLED = "PROTECTION_UNSETTLED"
BLOCK_PROTECTION_ABSENT = "PROTECTION_ABSENT"
BLOCK_STALE_PROTECTION = "STALE_PROTECTION_AWAITING_RETIREMENT"
ACCOUNT_SCOPE, SYMBOL_SCOPE = "ACCOUNT", "SYMBOL"

#: stops ever sent for one position before its protection is called absent (as ``production_protection``)
MAX_PROTECTION_ATTEMPTS = 3
#: reads a replacement may take in one call; each is followed by at most one mutation
MAX_DRIVE_STEPS = 6
#: closure applied when a venue refused with 429 / a ban and did not say for how long (as ``rate_budget``)
DEFAULT_RATE_LIMITED_CLOSURE_MS = 60_000
DEFAULT_BAN_CLOSURE_MS = 600_000
_RETRYABLE_UNSENT: Tuple[str, ...] = (V.VENUE_UNAVAILABLE, V.TIMESTAMP_OUT_OF_SYNC)
_LIMITED: Tuple[str, ...] = (V.RATE_LIMITED, V.BANNED)
_TOL = 1e-9


def closing_side(direction: str) -> str:
    return V.SELL if direction == LONG else V.BUY


def same_price(left: Optional[float], right: Optional[float]) -> bool:
    if left is None or right is None:
        return False
    return abs(float(left) - float(right)) <= _TOL * max(1.0, abs(float(left)))


def wider(direction: str, new_stop: float, resting_stop: float) -> bool:
    """Would the new stop lose more than the resting one? (never allowed)"""
    if same_price(new_stop, resting_stop):
        return False
    return new_stop < resting_stop if direction == LONG else new_stop > resting_stop


def stop_client_order_id(account_id: str, symbol: str, purpose: str, number: int) -> str:
    """The identity of a protective stop: 32 lower-case characters, valid on every venue of the contract."""
    seed = f"{account_id}|{symbol}|{purpose}|{int(number)}"
    return "cfs" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:29]


def _note(text: Any, limit: int = 240) -> str:
    """Stored text may quote a venue: credential-like content is removed, then it is bounded."""
    return sanitize_text(str(text or "")).strip()[:limit]


# --------------------------------------------------------------------------------------------------- journal
class VenueJournal:
    """The durable record around every venue call of ONE account on one venue and environment: the append-only
    journal, the entry blocks and the time before which the venue asked not to be called again.

    Every method is its own short transaction, committed before it returns, so no transaction is open while
    the venue is being called."""

    def __init__(self, db: Any, *, account_id: str, venue: str, environment: str,
                 clock: Optional[Callable[[], int]] = None) -> None:
        if not isinstance(account_id, str) or not account_id.strip():
            raise ValueError("a venue journal belongs to one account")
        self._db, self.account_id, self.venue, self.environment = db, account_id, venue, environment
        self._clock = clock or (lambda: int(time.time() * 1000))

    def now(self) -> int:
        return int(self._clock())

    # -- journal -------------------------------------------------------------------------------------------
    def record(self, event_type: str, *, symbol: Optional[str] = None, intent_id: Optional[str] = None,
               replacement_id: Optional[str] = None, method: Optional[str] = None,
               client_order_id: Optional[str] = None, outcome: Optional[str] = None,
               detail: Optional[Mapping[str, Any]] = None) -> int:
        with self._db.connect() as conn:
            cursor = conn.execute(
                f"INSERT INTO {EVENT_TABLE} (account_id, symbol, intent_id, replacement_id, event_type, method, "
                "client_order_id, outcome, recorded_at, detail) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (self.account_id, symbol, intent_id, replacement_id, event_type, method, client_order_id,
                 _note(outcome) if outcome is not None else None, self.now(),
                 _note(json.dumps(dict(detail or {}), sort_keys=True, default=str), 2_000)))
            return int(cursor.lastrowid)

    def mutate(self, method: str, call: Callable[[], Any], *, symbol: str, client_order_id: Optional[str] = None,
               intent_id: Optional[str] = None, replacement_id: Optional[str] = None,
               detail: Optional[Mapping[str, Any]] = None) -> Any:
        """Run one call that can change something at the venue, with a journal row before and after it. A
        ``VENUE_BEFORE`` that has no ``VENUE_AFTER`` is a call whose outcome nobody knows."""
        ids = dict(symbol=symbol, client_order_id=client_order_id, intent_id=intent_id, replacement_id=replacement_id,
                   method=method)
        before = self.record("VENUE_BEFORE", detail=detail, **ids)
        try:
            result = call()
        except AdapterError as error:
            self.record("VENUE_AFTER", outcome=error.code, **ids,
                        detail={"before_seq": before, "uncertain": error.uncertain, "retry_after_s": error.retry_after_s})
            self.note_limit(error, method)
            raise
        self.record("VENUE_AFTER", outcome="OK", detail={"before_seq": before, **_summary(result)}, **ids)
        return result

    def read(self, method: str, call: Callable[[], Any]) -> Any:
        """Run one venue read. A refusal to be asked (429, ban) is remembered; reads are not journaled."""
        try:
            return call()
        except AdapterError as error:
            self.note_limit(error, method)
            raise

    def events(self, *, symbol: Optional[str] = None, intent_id: Optional[str] = None,
               replacement_id: Optional[str] = None, event_type: Optional[str] = None) -> List[Dict[str, Any]]:
        where, params = ["account_id=?"], [self.account_id]
        for column, value in (("symbol", symbol), ("intent_id", intent_id), ("replacement_id", replacement_id),
                              ("event_type", event_type)):
            if value is not None:
                where.append(f"{column}=?")
                params.append(value)
        with self._db.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {EVENT_TABLE} WHERE {' AND '.join(where)} ORDER BY seq", params).fetchall()
        out = []
        for row in rows:
            event = dict(row)
            event["detail"] = json.loads(event["detail"] or "{}")
            out.append(event)
        return out

    def unanswered(self) -> List[Dict[str, Any]]:
        """Venue mutations that were announced and never answered: what a dead process left behind."""
        events = self.events()
        answered = {e["detail"].get("before_seq") for e in events if e["event_type"] == "VENUE_AFTER"}
        return [e for e in events if e["event_type"] == "VENUE_BEFORE" and e["seq"] not in answered]

    # -- back-off ------------------------------------------------------------------------------------------
    def note_limit(self, error: AdapterError, method: str = "") -> Optional[int]:
        """Remember until when the venue refuses to be asked. Returns that time, or None for another error."""
        if error.code not in _LIMITED:
            return None
        now = self.now()
        if error.code == V.BANNED:
            until = int(error.banned_until_ms) if error.banned_until_ms else now + DEFAULT_BAN_CLOSURE_MS
        elif error.retry_after_s is not None:
            until = now + int(round(float(error.retry_after_s) * 1000))
        else:
            until = now + DEFAULT_RATE_LIMITED_CLOSURE_MS
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(f"SELECT retry_not_before FROM {BACKOFF_TABLE} WHERE account_id=? AND venue=? AND environment=?",
                               (self.account_id, self.venue, self.environment)).fetchone()
            if row is None:
                conn.execute(f"INSERT INTO {BACKOFF_TABLE} VALUES (?,?,?,?,?,?,?)",
                             (self.account_id, self.venue, self.environment, until, error.code, method, now))
            elif until > int(row[0]):
                conn.execute(f"UPDATE {BACKOFF_TABLE} SET retry_not_before=?, code=?, method=?, recorded_at=? "
                             "WHERE account_id=? AND venue=? AND environment=?",
                             (until, error.code, method, now, self.account_id, self.venue, self.environment))
            else:
                until = int(row[0])
        return until

    def backoff_until(self, now_ms: Optional[int] = None) -> Optional[int]:
        """The time before which nothing may be sent to the venue for this account, or None."""
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT retry_not_before FROM {BACKOFF_TABLE} WHERE account_id=? AND venue=? AND environment=?",
                               (self.account_id, self.venue, self.environment)).fetchone()
        now = self.now() if now_ms is None else int(now_ms)
        return int(row[0]) if row is not None and int(row[0]) > now else None

    # -- entry blocks --------------------------------------------------------------------------------------
    def block(self, reason: str, *, symbol: str = "", scope: str = ACCOUNT_SCOPE, operator_only: bool = False,
              detail: str = "") -> str:
        """Block new entries (of the account, or of one symbol). Raising a block that is active changes nothing."""
        now = self.now()
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(f"SELECT block_id FROM {BLOCK_TABLE} WHERE account_id=? AND symbol=? AND reason=? AND state=?",
                               (self.account_id, symbol, reason, BLOCK_ACTIVE)).fetchone()
            if row is not None:
                return str(row[0])
            block_id = short_id("xblk", {"account": self.account_id, "symbol": symbol, "reason": reason, "at": now,
                                         "n": conn.execute(f"SELECT COUNT(*) FROM {BLOCK_TABLE}").fetchone()[0]})
            conn.execute(f"INSERT INTO {BLOCK_TABLE} (block_id, account_id, scope, symbol, reason, state, operator_only, "
                         "detail, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                         (block_id, self.account_id, scope, symbol, reason, BLOCK_ACTIVE, int(operator_only),
                          _note(detail), now))
        self.record("BLOCK", symbol=symbol or None, outcome=reason, detail={"block_id": block_id, "scope": scope,
                                                                              "operator_only": bool(operator_only)})
        return block_id

    def clear(self, reason: str, *, symbol: str = "", evidence: str, actor: str = "reconciliation") -> bool:
        """Lift a block this code raised and may lift, with the evidence that settles it."""
        return self._clear("symbol=? AND reason=? AND operator_only=0", (symbol, reason), evidence, actor)

    def clear_by_operator(self, block_id: str, *, operator_ref: str, evidence: str) -> bool:
        """Lift any block of this account by a named operator with stated evidence."""
        if not _note(operator_ref) or not _note(evidence):
            return False
        return self._clear("block_id=?", (block_id,), evidence, f"operator:{operator_ref}")

    def _clear(self, where: str, params: Sequence[Any], evidence: str, actor: str) -> bool:
        if not _note(evidence):
            return False
        with self._db.connect() as conn:
            done = conn.execute(
                f"UPDATE {BLOCK_TABLE} SET state=?, cleared_at=?, cleared_by=?, evidence=? WHERE account_id=? AND state=? AND {where}",
                (BLOCK_CLEARED, self.now(), _note(actor), _note(evidence), self.account_id, BLOCK_ACTIVE, *params)).rowcount
        if done:
            self.record("BLOCK", outcome="CLEARED", detail={"where": list(params), "by": actor})
        return bool(done)

    def active_blocks(self, symbol: Optional[str] = None) -> Tuple[Dict[str, Any], ...]:
        """Blocks that stop a new entry: every account-scope block, and the symbol-scope blocks of ``symbol``
        (all of them when no symbol is named)."""
        with self._db.connect() as conn:
            rows = [dict(r) for r in conn.execute(
                f"SELECT * FROM {BLOCK_TABLE} WHERE account_id=? AND state=? ORDER BY created_at, block_id",
                (self.account_id, BLOCK_ACTIVE)).fetchall()]
        return tuple(r for r in rows if r["scope"] == ACCOUNT_SCOPE or symbol is None or r["symbol"] == symbol)


def _summary(result: Any) -> Dict[str, Any]:
    """What a venue answer said, in the few fields a later reader of the journal needs."""
    if isinstance(result, OrderInfo):
        return {"status": result.status, "venue_order_id": result.venue_order_id,
                "filled_quantity": result.filled_quantity, "stop_price": result.stop_price}
    return {"result": result} if isinstance(result, (int, float, str)) else {}


# ------------------------------------------------------------------------------------------------- coverage
@dataclass(frozen=True)
class Coverage:
    """What the venue reports about one symbol: its position and the stops resting on its closing side."""
    symbol: str
    direction: str
    position: Optional[PositionInfo]            # None: flat
    stops: Tuple[OrderInfo, ...]

    @property
    def flat(self) -> bool:
        return self.position is None

    @property
    def quantity(self) -> float:
        return float(self.position.quantity) if self.position is not None else 0.0

    def covers(self, stop: OrderInfo) -> bool:
        """The stop closes the whole position when it fires."""
        return bool(stop.close_position) or (stop.quantity is not None
                                             and float(stop.quantity) >= self.quantity * (1.0 - _TOL))

    @property
    def effective(self) -> Optional[OrderInfo]:
        """The covering stop that fires first, or None when no resting stop covers the position."""
        covering = [s for s in self.stops if s.stop_price is not None and self.covers(s)]
        if not covering:
            return None
        return (max if self.direction == LONG else min)(covering, key=lambda s: float(s.stop_price))

    @property
    def nearest(self) -> Optional[OrderInfo]:
        """The resting stop that fires first, covering or not."""
        priced = [s for s in self.stops if s.stop_price is not None]
        if not priced:
            return None
        return (max if self.direction == LONG else min)(priced, key=lambda s: float(s.stop_price))


@dataclass(frozen=True)
class ProtectionOutcome:
    symbol: str
    status: str                                 # PROTECTED | PENDING | UNCERTAIN | UNDER_PROTECTED | ABSENT | ...
    reason: str = ""
    stop_price: Optional[float] = None          # of the resting stop that protects the position
    client_order_id: Optional[str] = None
    venue_order_id: Optional[str] = None
    position_quantity: Optional[float] = None
    retry_at_ms: Optional[int] = None

    @property
    def protected(self) -> bool:
        return self.status == PROTECTED


@dataclass(frozen=True)
class ReplacementOutcome:
    replacement_id: str
    symbol: str
    state: str                                  # OPEN | DONE | REFUSED | UNWOUND
    step: str
    outcome: str
    protocol: Optional[str]
    old_stop_price: Optional[float]
    new_stop_price: Optional[float]
    protection: str                             # status of the position's protection after this call
    replayed: bool = False                      # the recorded result of an earlier call; nothing was sent
    retry_at_ms: Optional[int] = None

    @property
    def done(self) -> bool:
        return self.state == REPLACEMENT_DONE

    @property
    def refused(self) -> bool:
        return self.state == REPLACEMENT_REFUSED


@dataclass(frozen=True)
class _Record:
    account_id: str
    symbol: str
    direction: str
    intended_stop_price: float
    status: str
    client_order_id: Optional[str]
    venue_order_id: Optional[str]
    resting_stop_price: Optional[float]
    form: Optional[str]
    covered_quantity: Optional[float]
    position_quantity: Optional[float]
    generation: int
    attempts: int
    requested_at: Optional[int]
    retry_not_before: Optional[int]
    superseded: Tuple[Tuple[Optional[str], Optional[str]], ...]
    reason: str
    origin: str
    created_at: int
    confirmed_at: Optional[int]
    updated_at: int


@dataclass(frozen=True)
class _Replacement:
    replacement_id: str
    account_id: str
    symbol: str
    direction: str
    request_key: str
    reason: str
    requested_stop_price: float
    protocol: Optional[str]
    step: str
    state: str
    outcome: str
    old_client_order_id: Optional[str]
    old_venue_order_id: Optional[str]
    old_stop_price: Optional[float]
    new_client_order_id: Optional[str]
    new_venue_order_id: Optional[str]
    new_stop_price: Optional[float]
    new_quantity: Optional[float]
    attempt: int
    retire_pending: int
    superseded: Tuple[Tuple[Optional[str], Optional[str]], ...]
    retry_not_before: Optional[int]
    detail: str
    created_at: int
    updated_at: int


_RECORD_COLUMNS = tuple(_Record.__dataclass_fields__)
_REPLACEMENT_COLUMNS = tuple(_Replacement.__dataclass_fields__)


def _pairs(text: Any) -> Tuple[Tuple[Optional[str], Optional[str]], ...]:
    return tuple((p[0], p[1]) for p in json.loads(text or "[]"))


def _is(stop: OrderInfo, client_order_id: Optional[str], venue_order_id: Optional[str]) -> bool:
    """Is this resting order the one with these ids? The venue order id decides where it is known: not every
    venue keeps the caller's id on a protective stop (``stop_client_order_id``)."""
    if venue_order_id and stop.venue_order_id:
        return str(stop.venue_order_id) == str(venue_order_id)
    return bool(client_order_id) and stop.client_order_id == client_order_id


# ---------------------------------------------------------------------------------------------------- manager
class ProtectionManager:
    """Establishes, verifies, replaces and retires the exchange-side stops of ONE account on one adapter."""

    def __init__(self, *, db: Any, adapter: Any, account_id: str, clock: Optional[Callable[[], int]] = None,
                 journal: Optional[VenueJournal] = None) -> None:
        self._db, self._adapter, self.account_id = db, adapter, account_id
        self.journal = journal or VenueJournal(db, account_id=account_id, venue=adapter.venue,
                                               environment=adapter.environment, clock=clock)

    # -- what the venue can do -----------------------------------------------------------------------------
    def protocol(self) -> Tuple[Optional[str], str]:
        """``(protocol, "")`` for the gap-free replacement this adapter declares, else ``(None, reason)``."""
        caps = self._adapter.capabilities()
        if not (caps.native_stop_market and caps.reduce_only):
            return None, NO_VENUE_HELD_STOP
        if caps.stop_amend_in_place:
            return AMEND_IN_PLACE, ""
        if caps.multiple_stops_per_position:
            return CREATE_CONFIRM_RETIRE, ""
        return None, REPLACEMENT_NOT_POSSIBLE_SAFELY

    def follows_position(self) -> bool:
        """The venue binds its stop to the whole position, so a fill needs no new stop (verified by read-back)."""
        return not self._adapter.capabilities().quantity_stop

    # -- reads ---------------------------------------------------------------------------------------------
    def coverage(self, symbol: str, direction: str) -> Coverage:
        """Step 1: the position and the stops resting on its closing side, from the venue. May raise."""
        side = closing_side(direction)

        def read() -> Coverage:
            held = [p for p in self._adapter.positions(symbol) if p.symbol == symbol and p.side != V.FLAT]
            stops = tuple(o for o in self._adapter.open_orders(symbol)
                          if o.symbol == symbol and o.open and o.order_type == V.STOP_MARKET and o.side == side)
            return Coverage(symbol=symbol, direction=direction, position=held[0] if held else None, stops=stops)

        found = self.journal.read("coverage", read)
        if found.position is not None and found.effective is None:
            # "open and uncovered" is the one answer that closes a position: it is read a second time, because
            # a stop that fired between the two reads above looks exactly like it
            found = self.journal.read("coverage", read)
        return found

    def record(self, symbol: str) -> Optional[ProtectionOutcome]:
        """What is recorded about the protection of ``symbol`` (no venue call)."""
        rec = self._record(symbol)
        return None if rec is None else self._as_outcome(rec)

    def unsettled(self) -> Tuple[ProtectionOutcome, ...]:
        """Recorded positions whose protection is not confirmed (no venue call)."""
        return tuple(self._as_outcome(r) for r in self._records() if r.status not in (PROTECTED, FLAT))

    def replacements(self, symbol: Optional[str] = None, *, open_only: bool = False) -> Tuple[ReplacementOutcome, ...]:
        return tuple(self._replacement_outcome(r) for r in self._replacement_rows(symbol, open_only=open_only))

    # -- establish / verify --------------------------------------------------------------------------------
    def expect(self, symbol: str, direction: str, stop_price: Optional[float] = None, *, origin: str = "") -> None:
        """Say durably, BEFORE an order that changes the position on ``symbol`` leaves, that its protection has
        to be read back afterwards (and at which stop, for an order that opens or enlarges it). No venue call.
        A process that dies after the order then leaves a record ``resume`` acts on. A symbol this module has no
        record of and no stop price for is left alone."""
        if stop_price is None and self._record(symbol) is None:
            return
        rec = self._remember(symbol, direction, float(stop_price or 0.0), origin)
        if rec.status in (PROTECTED, FLAT):
            self._save(rec, status=PENDING, reason="POSITION_CHANGE_EXPECTED")

    def protect(self, symbol: str, direction: str, stop_price: float, *, origin: str = "") -> ProtectionOutcome:
        """Make sure the position on ``symbol`` rests under a stop that covers all of it, and say what was read
        back. Called after every fill. Places a stop only when none rests; never moves or cancels one."""
        rec = self._remember(symbol, direction, float(stop_price), origin)
        retry_at = self.journal.backoff_until()
        if retry_at is not None:
            return self._unsettle(rec, PENDING if rec.status != PROTECTED else UNCERTAIN, RATE_LIMIT_BACKOFF,
                                  retry_at=retry_at)
        try:
            found = self.coverage(symbol, direction)
        except AdapterError as error:
            return self._unsettle(rec, UNCERTAIN, f"PROTECTION_READ_UNAVAILABLE:{error.code}",
                                  retry_at=self.journal.backoff_until())
        return self._settle(rec, found, may_place=True)

    def verify(self, symbol: str, direction: str) -> ProtectionOutcome:
        """Read back the protection of ``symbol`` after its position changed. Uses the recorded stop price when
        a stop has to be placed; a position this module has no record of is only reported."""
        rec = self._record(symbol)
        if rec is not None and rec.direction == direction:
            return self.protect(symbol, direction, rec.intended_stop_price, origin=rec.origin)
        try:
            found = self.coverage(symbol, direction)
        except AdapterError as error:
            return ProtectionOutcome(symbol, UNCERTAIN, f"PROTECTION_READ_UNAVAILABLE:{error.code}")
        if found.flat:
            return ProtectionOutcome(symbol, FLAT)
        stop = found.effective
        if stop is None:
            return ProtectionOutcome(symbol, UNDER_PROTECTED if found.stops else ABSENT, "NO_RECORDED_STOP_PRICE",
                                     position_quantity=found.quantity)
        return self._confirmed(self._remember(symbol, direction, float(stop.stop_price), "ADOPTED"), found, stop)

    def retire(self, symbol: str, direction: str) -> ProtectionOutcome:
        """The position is closed: cancel every stop of it that still rests. A position that is not flat is
        left exactly as it is."""
        rec = self._record(symbol) or self._remember(symbol, direction, 0.0, "RETIRE")
        try:
            found = self.coverage(symbol, direction)
        except AdapterError as error:
            return self._unsettle(rec, RETIRE_PENDING, f"PROTECTION_READ_UNAVAILABLE:{error.code}", block=False)
        return self._flat(rec, found) if found.flat else self._settle(rec, found, may_place=False)

    def _settle(self, rec: _Record, found: Coverage, *, may_place: bool) -> ProtectionOutcome:
        if found.flat:
            return self._flat(rec, found)
        if found.position.side != rec.direction:
            return self._unsettle(rec, UNCERTAIN, POSITION_DIRECTION_MISMATCH)
        stop = found.effective
        if stop is not None:
            return self._confirmed(rec, found, stop)
        if found.stops:
            # a stop rests but the position outgrew it: extend it where the venue allows an overlap, never by
            # taking the resting one away first
            if may_place and self.protocol()[0] == CREATE_CONFIRM_RETIRE:
                result = self.replace(found.symbol, rec.direction, float(found.nearest.stop_price), reason="RESIZE",
                                      request_key=f"resize|{rec.generation}|{found.quantity}")
                return self._as_outcome(self._record(found.symbol)) if result.done else \
                    self._unsettle(rec, UNDER_PROTECTED, f"RESIZE_{result.outcome}")
            return self._unsettle(rec, UNDER_PROTECTED, REPLACEMENT_NOT_POSSIBLE_SAFELY)
        if not may_place:
            return self._unsettle(rec, UNCERTAIN, "STOP_NOT_READ_BACK")
        return self._place_first(rec, found)

    def _place_first(self, rec: _Record, found: Coverage) -> ProtectionOutcome:
        """No stop rests on an open position: place one for the quantity that exists and read it back."""
        symbol, price = found.symbol, rec.intended_stop_price
        mark = found.position.mark_price
        if not price > 0:
            return self._absent(rec, "NO_RECORDED_STOP_PRICE")
        if mark is not None and not stop_side_valid(rec.direction, float(mark), price):
            return self._absent(rec, STOP_NOT_PROTECTIVE_AT_MARK)       # never moved to fit: the position is closed
        if rec.attempts >= MAX_PROTECTION_ATTEMPTS:
            return self._absent(rec, "PROTECTION_ATTEMPTS_EXHAUSTED")
        generation = rec.generation + 1
        client_order_id = stop_client_order_id(self.account_id, symbol, "stop", generation)
        superseded = rec.superseded + (((rec.client_order_id, rec.venue_order_id),) if rec.client_order_id else ())
        rec = self._save(rec, status=PENDING, generation=generation, attempts=rec.attempts + 1,
                         client_order_id=client_order_id, venue_order_id=None, requested_at=self.journal.now(),
                         superseded=json.dumps(superseded), reason="STOP_REQUESTED")       # durable before the send
        request = self._stop_request(symbol, rec.direction, price, found.quantity, client_order_id)
        try:
            self.journal.mutate("place_order", lambda: self._adapter.place_order(request), symbol=symbol,
                                client_order_id=client_order_id, detail={"stop_price": price, "purpose": "PROTECT"})
        except AdapterError as error:
            return self._place_failed(rec, error)
        return self._read_back(rec, "STOP_NOT_READ_BACK")

    def _place_failed(self, rec: _Record, error: AdapterError) -> ProtectionOutcome:
        if error.code in _LIMITED or error.code in _RETRYABLE_UNSENT:
            # the request never became an order: it does not count against the attempts, and it is not sent
            # again before the time the venue named
            rec = self._save(rec, attempts=max(0, rec.attempts - 1))
            return self._unsettle(rec, PENDING, f"STOP_DEFERRED:{error.code}", retry_at=self.journal.backoff_until())
        if error.uncertain:
            return self._read_back(rec, "STOP_SUBMIT_OUTCOME_UNKNOWN")
        if error.code in (V.STOP_ALREADY_EXISTS, V.REDUCE_ONLY_REJECTED):
            # the venue says a stop already rests, or that there is nothing left to protect: read which it is
            return self._read_back(rec, f"STOP_REFUSED:{error.code}")
        return self._absent(rec, f"STOP_REFUSED:{error.code}")

    def _read_back(self, rec: _Record, otherwise: str) -> ProtectionOutcome:
        try:
            found = self.coverage(rec.symbol, rec.direction)
        except AdapterError as error:
            return self._unsettle(rec, UNCERTAIN, f"{otherwise}|READ:{error.code}")
        if found.flat or found.effective is not None:
            return self._settle(rec, found, may_place=False)
        return self._unsettle(rec, UNCERTAIN, otherwise)

    def _confirmed(self, rec: _Record, found: Coverage, stop: OrderInfo) -> ProtectionOutcome:
        """Read back: ``stop`` rests and covers the whole position. The only way to ``PROTECTED``."""
        rec = self._save(rec, status=PROTECTED, client_order_id=stop.client_order_id or rec.client_order_id,
                         venue_order_id=stop.venue_order_id, resting_stop_price=float(stop.stop_price),
                         form="CLOSE_POSITION" if stop.close_position else "QUANTITY",
                         covered_quantity=None if stop.close_position else float(stop.quantity),
                         position_quantity=found.quantity, attempts=0, retry_not_before=None, reason="",
                         confirmed_at=self.journal.now())
        self._lift_blocks(rec)
        return self._as_outcome(rec)

    def _lift_blocks(self, rec: _Record) -> None:
        """Lift the blocks a confirmed read-back settles. While a replacement of the symbol is still in doubt
        they stay: a stop that rests does not say what became of the one that was being put beside it."""
        if rec.status != PROTECTED or self._ambiguous_replacement(rec.symbol):
            return
        evidence = (f"read-back: stop {rec.venue_order_id or rec.client_order_id} at {rec.resting_stop_price} covers "
                    f"{rec.position_quantity}")
        for reason in (BLOCK_PROTECTION_UNSETTLED, BLOCK_PROTECTION_ABSENT):
            self.journal.clear(reason, symbol=rec.symbol, evidence=evidence)

    def _flat(self, rec: _Record, found: Coverage, extra: Sequence[Tuple[Optional[str], Optional[str]]] = ()) -> ProtectionOutcome:
        """The position is closed: retire the stops of it that still rest (a stop bound to the position is
        already gone). A stop this module never sent is not touched."""
        known = tuple(rec.superseded) + ((rec.client_order_id, rec.venue_order_id),) + tuple(extra)
        for row in self._replacement_rows(rec.symbol):
            known += ((row.old_client_order_id, row.old_venue_order_id), (row.new_client_order_id, row.new_venue_order_id))
            known += tuple(row.superseded)
        mine = [s for s in found.stops if any(_is(s, c, v) for c, v in known if c or v)]
        failed = self._cancel_each(rec.symbol, mine, purpose="RETIRE_FLAT")
        if failed:
            self.journal.block(BLOCK_STALE_PROTECTION, symbol=rec.symbol, scope=SYMBOL_SCOPE,
                               detail=f"{len(failed)} stop(s) of a closed position still rest")
            return self._as_outcome(self._save(rec, status=RETIRE_PENDING, position_quantity=0.0,
                                               reason=f"RETIRE_FAILED:{failed[0][1]}"))
        rec = self._save(rec, status=FLAT, position_quantity=0.0, covered_quantity=None, resting_stop_price=None,
                         attempts=0, retry_not_before=None, reason="")
        for reason in (BLOCK_PROTECTION_UNSETTLED, BLOCK_PROTECTION_ABSENT, BLOCK_STALE_PROTECTION):
            self.journal.clear(reason, symbol=rec.symbol, evidence="read-back: the position is flat and none of its stops rests")
        return self._as_outcome(rec)

    def _unsettle(self, rec: _Record, status: str, reason: str, *, retry_at: Optional[int] = None,
                  block: bool = True) -> ProtectionOutcome:
        """Protection is not confirmed: record it and stop new entries of the account until a read settles it."""
        rec = self._save(rec, status=status, reason=_note(reason), retry_not_before=retry_at)
        if block:
            self.journal.block(BLOCK_PROTECTION_UNSETTLED, symbol=rec.symbol, detail=f"{status}: {reason}")
        self.journal.record("PROTECTION", symbol=rec.symbol, outcome=status, detail={"reason": reason})
        return replace(self._as_outcome(rec), retry_at_ms=retry_at)

    def _absent(self, rec: _Record, reason: str) -> ProtectionOutcome:
        """The venue answered and the position has no stop that can be established."""
        rec = self._save(rec, status=ABSENT, reason=_note(reason))
        self.journal.block(BLOCK_PROTECTION_ABSENT, symbol=rec.symbol, detail=reason)
        self.journal.record("PROTECTION", symbol=rec.symbol, outcome=ABSENT, detail={"reason": reason})
        return self._as_outcome(rec)

    # -- replace -------------------------------------------------------------------------------------------
    def replace(self, symbol: str, direction: str, new_stop_price: float, *, reason: str, request_key: str,
                current_stop_price: Optional[float] = None) -> ReplacementOutcome:
        """Move the stop of ``symbol`` to ``new_stop_price`` without an unprotected interval, or refuse.

        ``request_key`` identifies the request: the same key returns the recorded result of the first call (or
        continues it when it was interrupted) and never starts a second replacement. ``current_stop_price`` is
        what the caller believes rests; the venue's own answer decides, and it is only recorded."""
        if reason not in STOP_REASONS:
            raise ValueError(f"{reason!r} is not one of {STOP_REASONS}")
        if not isinstance(request_key, str) or not request_key.strip():
            raise ValueError("a replacement is identified by its request key")
        replacement_id = short_id("prep", {"account": self.account_id, "symbol": symbol, "key": request_key})
        row = self._replacement(replacement_id)
        if row is None:
            now = self.journal.now()
            with self._db.connect() as conn:
                conn.execute(
                    f"INSERT OR IGNORE INTO {REPLACEMENT_TABLE} (replacement_id, account_id, symbol, direction, request_key, "
                    "reason, requested_stop_price, step, state, detail, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (replacement_id, self.account_id, symbol, direction, request_key, reason, float(new_stop_price), READ,
                     REPLACEMENT_OPEN, _note(f"stated current stop {current_stop_price}"), now, now))
            row = self._replacement(replacement_id)
            self.journal.record("PROTECTION", symbol=symbol, replacement_id=replacement_id, outcome="REPLACEMENT_REQUESTED",
                                detail={"reason": reason, "new_stop_price": new_stop_price})
        elif (row.direction, row.reason) != (direction, reason) or not same_price(row.requested_stop_price, new_stop_price):
            return replace(self._replacement_outcome(row, replayed=True), state=REPLACEMENT_REFUSED, outcome=CONFLICTING_REPLAY)
        if row.state != REPLACEMENT_OPEN:
            return self._replacement_outcome(row, replayed=True)       # a duplicate retry changes nothing
        return self._drive(row)

    def _drive(self, row: _Replacement) -> ReplacementOutcome:
        """Take the replacement as far as the venue's present state allows. One read, then at most one mutation,
        repeated a bounded number of times."""
        retry_at = self.journal.backoff_until()
        if retry_at is not None:
            return self._replacement_outcome(self._step(row, retry_not_before=retry_at), outcome=DEFERRED, retry_at=retry_at)
        for _ in range(MAX_DRIVE_STEPS):
            try:
                found = self.coverage(row.symbol, row.direction)
            except AdapterError as error:
                if error.code in _LIMITED and row.step in (READ, BUILT):
                    retry_at = self.journal.backoff_until()     # nothing was sent: the old stop is what it was
                    return self._replacement_outcome(self._step(row, retry_not_before=retry_at), outcome=DEFERRED,
                                                     retry_at=retry_at)
                return self._ambiguous(row, f"READ_UNAVAILABLE:{error.code}")
            row, outcome = self._advance(row, found)
            if outcome is not None:
                return outcome
        return self._ambiguous(row, "REPLACEMENT_DID_NOT_CONVERGE")

    def _advance(self, row: _Replacement, found: Coverage) -> Tuple[_Replacement, Optional[ReplacementOutcome]]:
        if found.flat:
            # the position closed before or during the replacement: nothing is left to protect, and every
            # stop of it that still rests is retired
            rec = self._record(row.symbol) or self._remember(row.symbol, row.direction, row.requested_stop_price, "REPLACE")
            self._flat(rec, found)
            return row, self._finish(row, REPLACEMENT_DONE, POSITION_CLOSED)
        if found.position.side != row.direction:
            return row, self._finish(row, REPLACEMENT_REFUSED, POSITION_DIRECTION_MISMATCH)
        if row.step == READ:
            row, outcome = self._build(row, found)
            if outcome is not None:
                return row, outcome
        if row.protocol == AMEND_IN_PLACE:
            return self._amend(row, found)
        return self._overlap(row, found)

    def _build(self, row: _Replacement, found: Coverage) -> Tuple[_Replacement, Optional[ReplacementOutcome]]:
        """Step 2: a valid replacement for what rests now, or the reason there is none."""
        rec = self._record(row.symbol) or self._remember(row.symbol, row.direction, row.requested_stop_price, "REPLACE")
        old = found.effective or found.nearest
        new_price = float(row.requested_stop_price)
        if old is None:
            # nothing rests: this restores protection, it replaces nothing
            restored = self.protect(row.symbol, row.direction, new_price, origin=f"RESTORE:{row.replacement_id}")
            if restored.protected:
                return row, self._finish(row, REPLACEMENT_DONE, RESTORED)
            if restored.status == ABSENT:
                return row, self._finish(row, REPLACEMENT_UNWOUND, PROTECTION_ABSENT)
            return row, self._replacement_outcome(self._step(row, outcome=OUTCOME_UNCERTAIN, detail=restored.reason),
                                                  outcome=OUTCOME_UNCERTAIN, retry_at=restored.retry_at_ms)
        resting = float(old.stop_price)
        if wider(row.direction, new_price, resting):
            return row, self._finish(row, REPLACEMENT_REFUSED, STOP_WIDENING_REFUSED, old_stop_price=resting)
        if same_price(new_price, resting) and found.covers(old):
            self._confirmed(rec, found, old)
            return row, self._finish(row, REPLACEMENT_DONE, NO_CHANGE, old_stop_price=resting)
        mark = found.position.mark_price
        if mark is not None and not stop_side_valid(row.direction, float(mark), new_price):
            return row, self._finish(row, REPLACEMENT_REFUSED, STOP_NOT_PROTECTIVE_AT_MARK, old_stop_price=resting)
        protocol, why = self.protocol()
        if protocol is None:
            # no gap-free method is declared by this adapter: the old stop stays, and that is recorded
            self._save(rec, reason=why)
            return row, self._finish(row, REPLACEMENT_REFUSED, why, old_stop_price=resting)
        changes: Dict[str, Any] = dict(step=BUILT, protocol=protocol, old_client_order_id=old.client_order_id,
                                       old_venue_order_id=old.venue_order_id, old_stop_price=resting,
                                       new_stop_price=new_price, new_quantity=found.quantity)
        if protocol == CREATE_CONFIRM_RETIRE:
            changes.update(attempt=1, new_client_order_id=stop_client_order_id(self.account_id, row.symbol,
                                                                                row.replacement_id, 1))
        return self._step(row, **changes), None

    def _amend(self, row: _Replacement, found: Coverage) -> Tuple[_Replacement, Optional[ReplacementOutcome]]:
        """Protocol (a): the resting stop keeps resting while its trigger moves."""
        rec = self._record(row.symbol)
        target = next((s for s in found.stops if _is(s, row.old_client_order_id, row.old_venue_order_id)), None)
        if target is None:
            return row, self._stop_gone(row, rec, found)
        if same_price(target.stop_price, row.new_stop_price) and found.covers(target):
            # steps 4 and 6: read back at the new trigger, covering the position as it is now
            row = self._step(row, step=RECONCILED, new_client_order_id=target.client_order_id,
                             new_venue_order_id=target.venue_order_id)
            self._confirmed(rec, found, target)
            return row, self._finish(row, REPLACEMENT_DONE, REPLACED)
        row = self._step(row, step=ESTABLISHING)                         # journaled before the call
        address = target.client_order_id or row.old_client_order_id or str(target.venue_order_id)
        try:
            self.journal.mutate("amend_stop", lambda: self._adapter.amend_stop(
                row.symbol, client_order_id=address, new_stop_price=float(row.new_stop_price)),
                symbol=row.symbol, client_order_id=address, replacement_id=row.replacement_id,
                detail={"new_stop_price": row.new_stop_price})
        except AdapterError as error:
            return self._establish_failed(row, error)
        return self._step(row, step=ESTABLISHED), None

    def _overlap(self, row: _Replacement, found: Coverage) -> Tuple[_Replacement, Optional[ReplacementOutcome]]:
        """Protocol (b): create the replacement beside the old stop, confirm it, then retire the old one."""
        rec = self._record(row.symbol)
        old = next((s for s in found.stops if _is(s, row.old_client_order_id, row.old_venue_order_id)), None)
        stale_ids = tuple(row.superseded)
        new = next((s for s in found.stops if _is(s, row.new_client_order_id, row.new_venue_order_id)), None)
        if new is None and row.step in (ESTABLISHING, ESTABLISHED):
            # the answer to the create may be lost and the venue may not keep our id: recognise it by what it is
            new = next((s for s in found.stops if s is not old and same_price(s.stop_price, row.new_stop_price)
                        and not any(_is(s, c, v) for c, v in stale_ids)), None)
        if new is not None and found.covers(new):
            # step 4: the replacement rests and covers the position as it is now. From here it is the protection.
            if row.step != RETIRING:
                row = self._step(row, step=CONFIRMED, new_venue_order_id=new.venue_order_id)
            self._confirmed(rec, found, new)
            stale = [s for s in found.stops if s is not new and (s is old or any(_is(s, c, v) for c, v in stale_ids))]
            if not stale:
                # step 6: one read shows the replacement covering and nothing it superseded still resting
                return row, self._finish(self._step(row, step=RECONCILED, retire_pending=0), REPLACEMENT_DONE, REPLACED)
            row = self._step(row, step=RETIRING)                         # step 5, journaled before the cancel
            failed = self._cancel_each(row.symbol, stale, purpose="RETIRE_SUPERSEDED", replacement_id=row.replacement_id)
            if failed:
                # both rest: over-protected is safe. The retirement is retried by ``resume``.
                row = self._step(row, retire_pending=1, detail=f"RETIRE_FAILED:{failed[0][1]}",
                                 retry_not_before=self.journal.backoff_until())
                return row, self._replacement_outcome(row, outcome=RETIREMENT_PENDING)
            return row, None                                             # read once more: step 6
        if new is not None:
            # it rests, but the position grew while it was being placed: it covers too little. The old stop is
            # NOT retired; a replacement for the quantity that exists now is created beside both.
            if row.attempt >= MAX_PROTECTION_ATTEMPTS:
                return row, self._ambiguous(row, "REPLACEMENT_COVERAGE_NOT_REACHED")
            attempt = row.attempt + 1
            row = self._step(row, step=BUILT, attempt=attempt, new_venue_order_id=None,
                             superseded=json.dumps(stale_ids + ((new.client_order_id, new.venue_order_id),)),
                             new_client_order_id=stop_client_order_id(self.account_id, row.symbol, row.replacement_id, attempt))
        elif old is None:
            return row, self._stop_gone(row, rec, found)
        # step 3: establish the replacement for the quantity that exists now
        row = self._step(row, step=ESTABLISHING, new_quantity=found.quantity)        # journaled before the call
        request = self._stop_request(row.symbol, row.direction, float(row.new_stop_price), found.quantity,
                                     row.new_client_order_id)
        try:
            placed = self.journal.mutate("place_order", lambda: self._adapter.place_order(request), symbol=row.symbol,
                                         client_order_id=row.new_client_order_id, replacement_id=row.replacement_id,
                                         detail={"stop_price": row.new_stop_price, "purpose": "REPLACE"})
        except AdapterError as error:
            return self._establish_failed(row, error)
        if not placed.open:
            return row, self._refused_by_venue(row, f"NOT_RESTING:{placed.status}")
        return self._step(row, step=ESTABLISHED, new_venue_order_id=placed.venue_order_id), None

    def _establish_failed(self, row: _Replacement, error: AdapterError) -> Tuple[_Replacement, Optional[ReplacementOutcome]]:
        if error.code in _LIMITED:
            # the venue refused to be asked: nothing changed, the old stop rests, and nothing is sent sooner
            retry_at = self.journal.backoff_until()
            row = self._step(row, retry_not_before=retry_at, detail=f"DEFERRED:{error.code}")
            return row, self._replacement_outcome(row, outcome=DEFERRED, retry_at=retry_at)
        if error.code in _RETRYABLE_UNSENT:
            row = self._step(row, detail=f"DEFERRED:{error.code}")
            return row, self._replacement_outcome(row, outcome=DEFERRED)
        if error.uncertain:
            # the venue may have acted. One read decides: visible means it did; not visible stays unknown
            try:
                found = self.coverage(row.symbol, row.direction)
            except AdapterError as unread:
                return row, self._ambiguous(row, f"OUTCOME_UNKNOWN|READ:{unread.code}")
            if found.flat or self._visible(row, found):
                return row, None
            return row, self._ambiguous(row, "OUTCOME_UNKNOWN_NOT_VISIBLE")
        return row, self._refused_by_venue(row, error.code)

    def _visible(self, row: _Replacement, found: Coverage) -> bool:
        if row.protocol == AMEND_IN_PLACE:
            return any(_is(s, row.old_client_order_id, row.old_venue_order_id) and same_price(s.stop_price, row.new_stop_price)
                       for s in found.stops)
        return any(same_price(s.stop_price, row.new_stop_price)
                   and not _is(s, row.old_client_order_id, row.old_venue_order_id) for s in found.stops)

    def _refused_by_venue(self, row: _Replacement, code: str) -> ReplacementOutcome:
        """The venue answered no (a rejected trigger, an order-mode conflict, a second stop it does not take):
        nothing was created. The old stop is read back before the refusal is recorded as final."""
        rec = self._record(row.symbol)
        try:
            found = self.coverage(row.symbol, row.direction)
        except AdapterError as error:
            return self._ambiguous(row, f"VENUE_REFUSED:{code}|READ:{error.code}")
        if found.flat:
            # it refused because the position is gone: nothing is left to protect
            self._flat(rec, found)
            return self._finish(row, REPLACEMENT_DONE, POSITION_CLOSED)
        stop = found.effective
        if stop is not None:
            self._confirmed(rec, found, stop)
            return self._finish(row, REPLACEMENT_REFUSED, f"VENUE_REFUSED:{code}")
        if found.stops:
            self._unsettle(rec, UNDER_PROTECTED, f"VENUE_REFUSED:{code}")
            return self._finish(row, REPLACEMENT_REFUSED, f"VENUE_REFUSED:{code}")
        return self._stop_gone(row, rec, found)

    def _stop_gone(self, row: _Replacement, rec: _Record, found: Coverage) -> ReplacementOutcome:
        """The stop being replaced no longer rests although the position is open."""
        other = found.effective
        if other is not None:
            self._confirmed(rec, found, other)
            return self._finish(row, REPLACEMENT_UNWOUND, STOP_CHANGED_ELSEWHERE)
        self._absent(rec, "STOP_GONE_DURING_REPLACEMENT")
        return self._finish(row, REPLACEMENT_UNWOUND, PROTECTION_ABSENT)

    def _ambiguous(self, row: _Replacement, why: str) -> ReplacementOutcome:
        """The state of the stop is not known. The replacement stays open, the uncertainty is durable and new
        entries of the account are blocked until ``resume`` reads an answer."""
        row = self._step(row, outcome=OUTCOME_UNCERTAIN, detail=_note(why))
        rec = self._record(row.symbol) or self._remember(row.symbol, row.direction, row.requested_stop_price, "REPLACE")
        self._unsettle(rec, UNCERTAIN, f"REPLACEMENT:{why}")
        return self._replacement_outcome(row, outcome=OUTCOME_UNCERTAIN)

    def _finish(self, row: _Replacement, state: str, outcome: str, **changes: Any) -> ReplacementOutcome:
        """Step 7: the final state of the replacement, written once."""
        if state == REPLACEMENT_DONE:
            changes.setdefault("step", PERSISTED)
        row = self._step(row, state=state, outcome=outcome, retry_not_before=None, **changes)
        self.journal.record("PROTECTION", symbol=row.symbol, replacement_id=row.replacement_id, outcome=f"{state}:{outcome}",
                            detail={"protocol": row.protocol, "old_stop_price": row.old_stop_price,
                                    "new_stop_price": row.new_stop_price})
        rec = self._record(row.symbol)
        if rec is not None:
            self._lift_blocks(rec)                                       # this replacement is no longer in doubt
        return self._replacement_outcome(row)

    # -- restart -------------------------------------------------------------------------------------------
    def resume(self) -> Tuple[Any, ...]:
        """A new process (or a later cycle): finish or unwind every open replacement, retry what was deferred
        and settle every unconfirmed protection, from the venue's present state. Sends nothing while the venue's
        back-off runs."""
        if self.journal.backoff_until() is not None:
            return ()
        results: List[Any] = []
        unresolved = set()
        for row in self._replacement_rows(open_only=True):
            outcome = self._drive(row)
            results.append(outcome)
            if outcome.state == REPLACEMENT_OPEN and outcome.outcome == OUTCOME_UNCERTAIN:
                unresolved.add(row.symbol)
        for rec in self._records():
            if rec.status in RESUMABLE and rec.symbol not in unresolved:
                results.append(self.protect(rec.symbol, rec.direction, rec.intended_stop_price, origin=rec.origin))
        return tuple(results)

    # -- venue helpers -------------------------------------------------------------------------------------
    def _stop_request(self, symbol: str, direction: str, stop_price: float, quantity: float, client_order_id: str) -> OrderRequest:
        """The protective stop in the form the venue has: bound to the position where that exists, else for the
        whole quantity that is open."""
        if self._adapter.capabilities().close_position_stop:
            return OrderRequest(symbol=symbol, side=closing_side(direction), order_type=V.STOP_MARKET,
                                client_order_id=client_order_id, stop_price=stop_price, close_position=True)
        return OrderRequest(symbol=symbol, side=closing_side(direction), order_type=V.STOP_MARKET,
                            client_order_id=client_order_id, stop_price=stop_price, quantity=quantity, reduce_only=True)

    def _cancel_each(self, symbol: str, stops: Sequence[OrderInfo], *, purpose: str,
                     replacement_id: Optional[str] = None) -> List[Tuple[OrderInfo, str]]:
        """Cancel resting stops one by one. Returns those that could not be confirmed cancelled, with the code.
        An order the venue no longer knows is gone, which is what was wanted."""
        failed: List[Tuple[OrderInfo, str]] = []
        for stop in stops:
            if failed and failed[-1][1] in _LIMITED:
                failed.append((stop, failed[-1][1]))                     # told to wait: nothing more is sent
                continue
            by_venue_id = bool(stop.venue_order_id)
            try:
                self.journal.mutate("cancel_order", lambda s=stop, v=by_venue_id: self._adapter.cancel_order(
                    symbol, venue_order_id=s.venue_order_id if v else None, client_order_id=None if v else s.client_order_id),
                    symbol=symbol, client_order_id=stop.client_order_id, replacement_id=replacement_id,
                    detail={"purpose": purpose, "venue_order_id": stop.venue_order_id})
            except AdapterError as error:
                if error.code != V.ORDER_NOT_FOUND:
                    failed.append((stop, error.code))
        return failed

    # -- persistence ---------------------------------------------------------------------------------------
    def _record(self, symbol: str) -> Optional[_Record]:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT {', '.join(_RECORD_COLUMNS)} FROM {PROTECTION_TABLE} WHERE account_id=? AND symbol=?",
                               (self.account_id, symbol)).fetchone()
        return None if row is None else self._to_record(row)

    def _records(self) -> List[_Record]:
        with self._db.connect() as conn:
            rows = conn.execute(f"SELECT {', '.join(_RECORD_COLUMNS)} FROM {PROTECTION_TABLE} WHERE account_id=? ORDER BY symbol",
                                (self.account_id,)).fetchall()
        return [self._to_record(r) for r in rows]

    @staticmethod
    def _to_record(row: Any) -> _Record:
        values = dict(zip(_RECORD_COLUMNS, tuple(row)))
        values["superseded"] = _pairs(values["superseded"])
        return _Record(**values)

    def _remember(self, symbol: str, direction: str, stop_price: float, origin: str) -> _Record:
        """The durable statement of what should protect ``symbol``, written before anything is asked or sent. A
        position that was flat starts a new protection: the attempts of the previous one do not count."""
        now = self.journal.now()
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(f"SELECT status, direction FROM {PROTECTION_TABLE} WHERE account_id=? AND symbol=?",
                               (self.account_id, symbol)).fetchone()
            if row is None:
                conn.execute(
                    f"INSERT INTO {PROTECTION_TABLE} (account_id, symbol, direction, intended_stop_price, status, origin, "
                    "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
                    (self.account_id, symbol, direction, stop_price, PENDING, _note(origin), now, now))
            elif row[0] == FLAT or row[1] != direction:
                conn.execute(
                    f"UPDATE {PROTECTION_TABLE} SET direction=?, intended_stop_price=?, status=?, origin=?, attempts=0, "
                    "resting_stop_price=NULL, covered_quantity=NULL, confirmed_at=NULL, reason='', updated_at=? "
                    "WHERE account_id=? AND symbol=?", (direction, stop_price, PENDING, _note(origin), now, self.account_id, symbol))
            elif stop_price > 0:
                conn.execute(f"UPDATE {PROTECTION_TABLE} SET intended_stop_price=?, updated_at=? WHERE account_id=? AND symbol=?",
                             (stop_price, now, self.account_id, symbol))
        return self._record(symbol)

    def _save(self, rec: _Record, **changes: Any) -> _Record:
        changes["updated_at"] = self.journal.now()
        with self._db.connect() as conn:
            conn.execute(f"UPDATE {PROTECTION_TABLE} SET {', '.join(f'{k}=?' for k in changes)} WHERE account_id=? AND symbol=?",
                         (*changes.values(), self.account_id, rec.symbol))
        return self._record(rec.symbol)

    def _replacement(self, replacement_id: str) -> Optional[_Replacement]:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT {', '.join(_REPLACEMENT_COLUMNS)} FROM {REPLACEMENT_TABLE} WHERE replacement_id=? "
                               "AND account_id=?", (replacement_id, self.account_id)).fetchone()
        return None if row is None else self._to_replacement(row)

    def _replacement_rows(self, symbol: Optional[str] = None, *, open_only: bool = False) -> List[_Replacement]:
        where, params = "account_id=?", [self.account_id]
        if symbol is not None:
            where, params = where + " AND symbol=?", params + [symbol]
        if open_only:
            where, params = where + " AND state=?", params + [REPLACEMENT_OPEN]
        with self._db.connect() as conn:
            rows = conn.execute(f"SELECT {', '.join(_REPLACEMENT_COLUMNS)} FROM {REPLACEMENT_TABLE} WHERE {where} "
                                "ORDER BY created_at, replacement_id", params).fetchall()
        return [self._to_replacement(r) for r in rows]

    @staticmethod
    def _to_replacement(row: Any) -> _Replacement:
        values = dict(zip(_REPLACEMENT_COLUMNS, tuple(row)))
        values["superseded"] = _pairs(values["superseded"])
        return _Replacement(**values)

    def _step(self, row: _Replacement, **changes: Any) -> _Replacement:
        """Persist the next step of a replacement. Written before the step is taken."""
        changes["updated_at"] = self.journal.now()
        if "detail" in changes:
            changes["detail"] = _note(changes["detail"])
        if "outcome" not in changes and row.outcome == OUTCOME_UNCERTAIN and changes.get("step") not in (None, row.step):
            changes["outcome"] = ""                                      # it moved on: the earlier doubt is settled
        with self._db.connect() as conn:
            conn.execute(f"UPDATE {REPLACEMENT_TABLE} SET {', '.join(f'{k}=?' for k in changes)} WHERE replacement_id=? "
                         "AND account_id=?", (*changes.values(), row.replacement_id, self.account_id))
        return self._replacement(row.replacement_id)

    def _ambiguous_replacement(self, symbol: str) -> bool:
        with self._db.connect() as conn:
            return conn.execute(f"SELECT 1 FROM {REPLACEMENT_TABLE} WHERE account_id=? AND symbol=? AND state=? AND outcome=?",
                                (self.account_id, symbol, REPLACEMENT_OPEN, OUTCOME_UNCERTAIN)).fetchone() is not None

    def _as_outcome(self, rec: _Record) -> ProtectionOutcome:
        return ProtectionOutcome(symbol=rec.symbol, status=rec.status, reason=rec.reason, stop_price=rec.resting_stop_price,
                                 client_order_id=rec.client_order_id, venue_order_id=rec.venue_order_id,
                                 position_quantity=rec.position_quantity, retry_at_ms=rec.retry_not_before)

    def _replacement_outcome(self, row: _Replacement, *, outcome: Optional[str] = None, replayed: bool = False,
                             retry_at: Optional[int] = None) -> ReplacementOutcome:
        rec = self._record(row.symbol)
        return ReplacementOutcome(
            replacement_id=row.replacement_id, symbol=row.symbol, state=row.state, step=row.step,
            outcome=outcome if outcome is not None else row.outcome, protocol=row.protocol,
            old_stop_price=row.old_stop_price, new_stop_price=row.new_stop_price,
            protection=rec.status if rec is not None else UNCERTAIN, replayed=replayed,
            retry_at_ms=retry_at if retry_at is not None else row.retry_not_before)


__all__ = ["AMEND_IN_PLACE", "CREATE_CONFIRM_RETIRE", "PROTOCOLS", "PENDING", "PROTECTED", "UNCERTAIN", "UNDER_PROTECTED",
           "ABSENT", "RETIRE_PENDING", "FLAT", "RESUMABLE", "READ", "BUILT", "ESTABLISHING", "ESTABLISHED", "CONFIRMED",
           "RETIRING", "RECONCILED", "PERSISTED", "REPLACED", "NO_CHANGE", "RESTORED", "POSITION_CLOSED",
           "RETIREMENT_PENDING", "DEFERRED", "OUTCOME_UNCERTAIN", "REPLACEMENT_NOT_POSSIBLE_SAFELY",
           "STOP_WIDENING_REFUSED", "STOP_NOT_PROTECTIVE_AT_MARK", "POSITION_DIRECTION_MISMATCH", "STOP_CHANGED_ELSEWHERE",
           "PROTECTION_ABSENT", "CONFLICTING_REPLAY", "NO_VENUE_HELD_STOP", "RATE_LIMIT_BACKOFF",
           "BLOCK_PROTECTION_UNSETTLED", "BLOCK_PROTECTION_ABSENT", "BLOCK_STALE_PROTECTION", "ACCOUNT_SCOPE",
           "SYMBOL_SCOPE", "MAX_PROTECTION_ATTEMPTS", "MAX_DRIVE_STEPS", "closing_side", "same_price", "wider",
           "stop_client_order_id", "VenueJournal", "Coverage", "ProtectionOutcome", "ReplacementOutcome",
           "ProtectionManager"]
