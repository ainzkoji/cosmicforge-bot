"""The one canonical reservation ledger of the CATI platform pipeline (Section H, Step 3.7).

A reservation is the durable form of an authorization: it holds the position slot, the risk at the stop and the
margin of ONE risk-increasing intent of ONE account from the moment it is admitted until its order is filled,
proven absent, or the unsent intent expires. New portfolio reservations are written here and nowhere else.

**Why a second check.** ``portfolio.authorize`` is pure: two decisions that read the same account at the same
time would each find room for the same capacity. ``ReservationLedger.reserve`` therefore runs the SAME
authorization again inside one ``BEGIN IMMEDIATE`` transaction, against what the ledger holds at that instant,
and writes the accepted reservations before it commits. SQLite admits one writer at a time, so two threads or
two processes cannot both pass: the second one sees the first one's rows. Positions, open risk, leverage and
margin are thereby limited jointly, not per caller, and a terminal brake latched in the meantime is read in the
same transaction.

**Lifecycle** (enforced by the table's triggers as well as by this code)::

    RESERVED -> SUBMITTED -> FILLED
       |            |-----> RELEASED   only with evidence that the venue holds no order
       |            '-----> UNKNOWN -> FILLED | RELEASED   only by ``resolve_unknown`` with evidence
       |-> RELEASED   never sent
       |-> EXPIRED    never sent, its decision ran out
       '-> UNKNOWN    the outcome of a send is not known

A reservation is marked SUBMITTED before its order leaves, so a crash can never leave an order at the venue
behind a row that still expires. ``UNKNOWN`` never expires and is never released on silence: only
reconciliation that has asked the venue resolves it. ``FILLED``, ``RELEASED`` and ``EXPIRED`` are final. Every
insert and every state change is one row of an append-only event log, written by the database.

**What counts as committed.** Rows in RESERVED, SUBMITTED and UNKNOWN, and FILLED rows whose fill is not older
than the account snapshot in use (a snapshot taken before a fill cannot show the position). A reservation and
the position it became are counted once (``portfolio._book``).

**Identity.** One row per (account, book, decision, symbol, kind). Reserving the same intent again returns the
first row and writes nothing; an intent whose reservation ended is not reserved a second time. A partial unique
index allows one open pipeline claim per symbol of an account, so a concurrent duplicate entry cannot even be
inserted. Every statement is scoped by the account: a caller holding another account's reservation id is told
the reservation does not exist.

**Books.** Shadow reservations and executing ones (demo, live, operator certification) never consume each
other's capacity: a shadow account is hypothetical, and the frozen rule it is compared with knows no other
claims.

**The five earlier stores** are not modified and their code is not touched. Open claims of the two durable ones
are mirrored into the ledger (``source`` names the store) at the start of every ``reserve`` transaction and by
``import_legacy_reservations``, so the ledger is the one place a limit is checked against. A mirror follows its
legacy row and is never driven from here. ``reconcile_stores`` reports every disagreement without changing
anything. See ``docs/implementation/STEP3_PORTFOLIO_AND_BRAKES.md`` for the classification and the procedure.

No transaction of this module spans a call to an exchange: it has no exchange access at all.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from app.exchange.platform_adapter.contract import InstrumentRules, canonical_symbol
from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.observability.sanitize import sanitize_text
from app.trading_intelligence.pipeline import brakes
from app.trading_intelligence.pipeline.brakes import BrakeFlags
from app.trading_intelligence.pipeline.contracts import (
    DEMO, ENTRY, LIVE, LONG, OPERATOR_CERTIFICATION, SHADOW, SHORT, Intent, IntentBatch,
)
from app.trading_intelligence.pipeline.portfolio import (
    PIPELINE_SOURCE, AccountLimits, AccountSnapshot, OpenReservation, PortfolioAuthorization, RefusedIntent,
    Release, authorize,
)
from shared_lib.persistence.cati_portfolio_schema import LEDGER_EVENT_TABLE, LEDGER_OPEN_STATES, LEDGER_TABLE

RESERVED, SUBMITTED, UNKNOWN, FILLED, RELEASED, EXPIRED = "RESERVED", "SUBMITTED", "UNKNOWN", "FILLED", "RELEASED", "EXPIRED"
EXECUTION_BOOK, SHADOW_BOOK = "EXECUTION", "SHADOW"
LEGACY_CATI_RESERVATION, LEGACY_PENDING_ENTRY = "LEGACY_CATI_RESERVATION", "LEGACY_PENDING_ENTRY"
LEGACY_SOURCES: Tuple[str, ...] = (LEGACY_CATI_RESERVATION, LEGACY_PENDING_ENTRY)
LEGACY_RESERVATION_TABLE, LEGACY_PENDING_TABLE = "cati_portfolio_reservations", "pending_entries"
#: the legacy stores that live in process memory only: nothing in a database can be compared with them
VOLATILE_STORES: Tuple[str, ...] = ("app.risk.capital_ledger.AccountMarginReservations",
                                    "app.risk.adaptive_daily_budget.AdaptiveDailyRiskBudgetEngine._reservations")

#: refusals only the ledger can state
INTENT_ALREADY_FINAL = "INTENT_ALREADY_FINAL"
#: why a transition was not made
RESERVATION_NOT_FOUND = "RESERVATION_NOT_FOUND"
LEGACY_ROW_FOLLOWS_ITS_STORE = "LEGACY_ROW_FOLLOWS_ITS_STORE"
RESERVATION_EXPIRED = "RESERVATION_EXPIRED"
RELEASE_NOT_CONFIRMED = "RELEASE_NOT_CONFIRMED"
EVIDENCE_REQUIRED = "EVIDENCE_REQUIRED"
UNKNOWN_OUTCOME_NOT_RELEASABLE = "UNKNOWN_OUTCOME_NOT_RELEASABLE"
CONFLICTING_REPLAY = "CONFLICTING_REPLAY"

_COLUMNS: Tuple[str, ...] = (
    "reservation_id", "account_id", "user_id", "book", "mode", "source", "legacy_ref", "legacy_generation",
    "intent_key", "family_id", "decision_id", "decision_hash", "symbol", "direction", "kind", "quantity",
    "reference_price", "stop_price", "risk_amount", "notional", "margin", "leverage", "awaits_releases", "state",
    "created_at", "expires_at", "updated_at", "submitted_at", "client_order_id", "unknown_since",
    "resolution_deadline", "filled_at", "filled_quantity", "filled_price", "resolution_evidence", "snapshot_hash",
    "limits_hash", "authorization_hash")
_SELECT = f"SELECT {', '.join(_COLUMNS)} FROM {LEDGER_TABLE}"
_OPEN_SQL = "(" + ", ".join(f"'{s}'" for s in LEDGER_OPEN_STATES) + ")"
_TOL = 1e-9


class LedgerError(ValueError):
    """The request cannot be a reservation at all. ``code`` is stable and machine-readable."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


class LedgerSchemaMissing(RuntimeError):
    """The platform schema has not created the ledger. This module issues no DDL."""


def book_of(mode: str) -> str:
    """The book a mode reserves in. A research simulation reserves nothing."""
    if mode == SHADOW:
        return SHADOW_BOOK
    if mode in (DEMO, LIVE, OPERATOR_CERTIFICATION):
        return EXECUTION_BOOK
    raise LedgerError("MODE_NOT_RESERVABLE", f"{mode!r} takes no reservation")


def intent_key(decision_id: str, intent: Intent) -> str:
    """The identity of an intent inside its account and book: a decision acts once per symbol."""
    return f"{decision_id}|{intent.symbol}|{intent.kind}"


@dataclass(frozen=True)
class ReservationScope:
    """Whose decision a reservation belongs to."""
    account_id: str
    user_id: str
    family_id: str
    decision_id: str
    decision_hash: str
    mode: str
    expires_at_ms: int                  # an unsent reservation ends with its decision

    def __post_init__(self) -> None:
        for name in ("account_id", "family_id", "decision_id", "mode"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise LedgerError("INVALID_SCOPE", f"{name} is required")
        book_of(self.mode)

    @classmethod
    def of(cls, batch: IntentBatch) -> "ReservationScope":
        p = batch.provenance
        return cls(account_id=batch.account.broker_account_id, user_id=batch.account.user_id, family_id=p.family_id,
                   decision_id=p.decision_id, decision_hash=p.decision_hash, mode=p.mode, expires_at_ms=batch.expires_at_ms)


@dataclass(frozen=True)
class Reservation:
    """One row of the ledger. Times are epoch milliseconds."""
    reservation_id: str
    account_id: str
    user_id: Optional[str]
    book: str
    mode: str
    source: str
    legacy_ref: Optional[str]
    legacy_generation: int
    intent_key: str
    family_id: str
    decision_id: str
    decision_hash: Optional[str]
    symbol: str
    direction: str
    kind: str
    quantity: Optional[float]
    reference_price: Optional[float]
    stop_price: Optional[float]
    risk_amount: Optional[float]
    notional: Optional[float]
    margin: Optional[float]
    leverage: Optional[float]
    awaits_releases: Tuple[Release, ...]
    state: str
    created_at: int
    expires_at: Optional[int]
    updated_at: int
    submitted_at: Optional[int]
    client_order_id: Optional[str]
    unknown_since: Optional[int]
    resolution_deadline: Optional[int]
    filled_at: Optional[int]
    filled_quantity: Optional[float]
    filled_price: Optional[float]
    resolution_evidence: Optional[str]
    snapshot_hash: Optional[str]
    limits_hash: Optional[str]
    authorization_hash: Optional[str]

    @property
    def open(self) -> bool:
        return self.state in LEDGER_OPEN_STATES

    def claim(self) -> OpenReservation:
        return OpenReservation(reservation_id=self.reservation_id, symbol=self.symbol, direction=self.direction,
                               kind=self.kind, risk_amount=self.risk_amount, notional=self.notional, margin=self.margin,
                               state=self.state, source=self.source)


@dataclass(frozen=True)
class ReservationOutcome:
    """What ``reserve`` decided. ``authorization`` covers the intents evaluated in this call; an intent that was
    reserved earlier appears in ``reservations`` and ``replayed`` only."""
    authorization: PortfolioAuthorization
    reservations: Tuple[Reservation, ...]       # every reservation of these intents, earlier ones included
    created: Tuple[str, ...]
    replayed: Tuple[str, ...]
    refused: Tuple[RefusedIntent, ...]

    def reservation(self, symbol: str) -> Optional[Reservation]:
        return next((r for r in self.reservations if r.symbol == symbol), None)


@dataclass(frozen=True)
class Transition:
    """``ok``: the reservation is in the state asked for. ``changed``: this call put it there."""
    ok: bool
    changed: bool
    state: Optional[str]
    reason: Optional[str] = None


@dataclass(frozen=True)
class LegacyImport:
    created: Tuple[str, ...]
    updated: Tuple[str, ...]
    unchanged: int
    held_unknown: Tuple[str, ...]       # mirrors left UNKNOWN although their legacy row is gone


@dataclass(frozen=True)
class Finding:
    code: str
    store: str
    reference: str
    detail: str


@dataclass(frozen=True)
class StoreReconciliation:
    account_id: str
    consistent: bool
    findings: Tuple[Finding, ...]
    counts: Tuple[Tuple[str, int], ...]
    not_inspected: Tuple[str, ...]      # VOLATILE_STORES: process memory, outside any database


def _note(text: Any) -> str:
    """Evidence may quote a venue: credential-like content is removed before it is stored, then bounded.
    Blank text is no evidence."""
    return sanitize_text(str(text or "")).strip()[:240]


def _reservation(row: Sequence[Any]) -> Reservation:
    v = dict(zip(_COLUMNS, row))
    v["awaits_releases"] = tuple(Release(symbol=r["symbol"], quantity_after=float(r["quantity_after"]))
                                 for r in json.loads(v["awaits_releases"] or "[]"))
    return Reservation(**v)


def _query(conn: Any, where: str, params: Sequence[Any]) -> List[Reservation]:
    return [_reservation(tuple(r)) for r in conn.execute(f"{_SELECT} WHERE {where}", tuple(params)).fetchall()]


def _one(conn: Any, account_id: str, reservation_id: str) -> Optional[Reservation]:
    rows = _query(conn, "reservation_id=? AND account_id=?", (reservation_id, account_id))
    return rows[0] if rows else None


def _table(conn: Any, name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def _insert(conn: Any, values: Mapping[str, Any]) -> None:
    conn.execute(f"INSERT INTO {LEDGER_TABLE} ({', '.join(values)}) VALUES ({', '.join('?' * len(values))})",
                 tuple(values.values()))


def _set(conn: Any, row: Reservation, now_ms: int, actor: str, detail: str, **values: Any) -> None:
    """One guarded update: the row must still be in the state it was read in."""
    values.update(updated_at=int(now_ms), last_actor=actor, last_detail=_note(detail))
    done = conn.execute(
        f"UPDATE {LEDGER_TABLE} SET {', '.join(f'{k}=?' for k in values)} WHERE reservation_id=? AND account_id=? AND state=?",
        tuple(values.values()) + (row.reservation_id, row.account_id, row.state)).rowcount
    if done != 1:
        raise LedgerError("CONCURRENT_CHANGE", f"{row.reservation_id} changed inside its own transaction")


def _expire(conn: Any, now_ms: int, account_id: Optional[str] = None) -> int:
    """Unsent pipeline reservations whose decision ran out. Never a SUBMITTED or an UNKNOWN row."""
    sql = (f"UPDATE {LEDGER_TABLE} SET state='{EXPIRED}', updated_at=?, last_actor='ledger', last_detail='TTL'"
           f" WHERE state='{RESERVED}' AND source='{PIPELINE_SOURCE}' AND expires_at IS NOT NULL AND expires_at<=?")
    params: List[Any] = [int(now_ms), int(now_ms)]
    if account_id is not None:
        sql += " AND account_id=?"
        params.append(account_id)
    return conn.execute(sql, params).rowcount


def _claims(conn: Any, account_id: str, book: str, now_ms: int, as_of_ms: Optional[int]) -> Tuple[OpenReservation, ...]:
    """What the account is committed to beyond the positions a snapshot of ``as_of_ms`` can show."""
    rows = _query(conn, f"account_id=? AND book=? AND state IN {_OPEN_SQL}", (account_id, book))
    rows = [r for r in rows if r.state != RESERVED or r.expires_at is None or r.expires_at > now_ms]
    if as_of_ms is not None:
        rows += _query(conn, f"account_id=? AND book=? AND state='{FILLED}' AND filled_at>=?",
                       (account_id, book, int(as_of_ms)))
    return tuple(r.claim() for r in sorted(rows, key=lambda r: r.reservation_id))


# ---------------------------------------------------------------------------------------------- legacy stores
@dataclass(frozen=True)
class _LegacyClaim:
    """A row of a durable legacy store, translated: ``state`` is the ledger state it corresponds to."""
    source: str
    ref: str
    state: str
    symbol: str
    direction: str
    user_id: Optional[str]
    decision_id: str
    quantity: Optional[float]
    reference_price: Optional[float]
    notional: Optional[float]
    margin: Optional[float]
    expires_at: Optional[int]
    happened_at: Optional[int]
    client_order_id: Optional[str]
    note: str


def _direction(side: Any) -> str:
    text = str(side or "").upper()
    return {"BUY": LONG, "SELL": SHORT}.get(text, text)


def _positive(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 and number == number and number != float("inf") else None


def _legacy_reservation_id(ref: str) -> str:
    return ref.rsplit("#", 1)[0].rsplit("@", 1)[0]


def _reservation_claims(conn: Any, account_id: str, now_ms: int, wanted_ids: Sequence[str]) -> List[_LegacyClaim]:
    """Store 1, ``cati_portfolio_reservations``: the rows that hold execution capacity (PRODUCTION mode, or a
    trade plan in an executing mode, exactly as the legacy occupancy projection reads them). Shadow selection
    rows claim none."""
    if not _table(conn, LEGACY_RESERVATION_TABLE):
        return []
    live_plan = ("EXISTS (SELECT 1 FROM cati_trade_plans p WHERE p.reservation_id=r.reservation_id"
                 " AND p.broker_account_id=r.broker_account_id AND p.mode IN ('LIVE','DEMO'))") if _table(conn, "cati_trade_plans") else "0"
    marks = ", ".join("?" * len(wanted_ids))
    rows = conn.execute(
        f"SELECT r.reservation_id, r.created_at, r.expires_at, r.updated_at, r.status, r.selected_instruments,"
        f" r.capital_amount, r.cycle_id, r.user_id, r.resolution_note FROM {LEGACY_RESERVATION_TABLE} r"
        f" WHERE r.broker_account_id=? AND (r.mode='PRODUCTION' OR {live_plan})"
        f" AND (r.status IN ('RESERVED', 'RESOLUTION_PENDING') OR r.reservation_id IN ({marks}))",
        (account_id, *wanted_ids)).fetchall()
    out = []
    for rid, created, expires, updated, status, instruments, capital, cycle, user, note in rows:
        state = {"RESERVED": RESERVED if int(expires) > now_ms else EXPIRED, "RESOLUTION_PENDING": UNKNOWN,
                 "CONSUMED": FILLED, "RELEASED": RELEASED, "EXPIRED": EXPIRED}[status]
        for _canonical, _venue, venue_symbol, side in json.loads(instruments or "[]"):
            symbol = canonical_symbol(venue_symbol)
            out.append(_LegacyClaim(
                source=LEGACY_CATI_RESERVATION, ref=f"{rid}@{created}#{symbol}", state=state, symbol=symbol,
                direction=_direction(side), user_id=user, decision_id=str(cycle or ""), quantity=None,
                reference_price=None, notional=None, margin=_positive(capital),
                expires_at=int(expires) if state == RESERVED else None, happened_at=int(updated) if updated else None,
                client_order_id=None, note=f"{status} {note or ''}".strip()))
    return out


def _pending_claims(conn: Any, account_id: str) -> List[_LegacyClaim]:
    """Store 2, ``pending_entries``: every in-flight or confirmed entry lock of the account's bots."""
    if not (_table(conn, LEGACY_PENDING_TABLE) and _table(conn, "bot_instances")):
        return []
    rows = conn.execute(
        f"SELECT e.id, e.bot_id, e.symbol, e.side, e.state, e.submit_state, e.client_order_id, e.intended_notional,"
        f" e.sized_notional, e.sized_qty, e.submitted_notional, e.submitted_qty, e.filled_notional, e.filled_qty,"
        f" e.reference_price, e.confirmed_at_ms, e.cycle_id, b.user_id FROM {LEGACY_PENDING_TABLE} e"
        " JOIN bot_instances b ON b.id=e.bot_id WHERE b.broker_account_id=?", (account_id,)).fetchall()
    out = []
    for (row_id, bot, symbol, side, state, submit, client, intended, sized, sized_qty, sent, sent_qty, filled,
         filled_qty, reference, confirmed, cycle, user) in rows:
        mapped = (RELEASED if state == "OPEN_FAILED" else UNKNOWN if submit == "SUBMIT_UNKNOWN"
                  else FILLED if state == "OPEN_CONFIRMED" else SUBMITTED if submit == "SUBMIT_CONFIRMED" else RESERVED)
        sizes = [(filled, filled_qty)] if state == "OPEN_CONFIRMED" else []
        sizes += [(sent, sent_qty), (sized, sized_qty), (intended, None)]
        notional, quantity = next(((_positive(n), _positive(q)) for n, q in sizes if _positive(n)), (None, None))
        out.append(_LegacyClaim(
            source=LEGACY_PENDING_ENTRY, ref=f"{bot}|{str(symbol).upper()}|{str(side).upper()}|{row_id or ''}",
            state=mapped, symbol=canonical_symbol(symbol), direction=_direction(side), user_id=user,
            decision_id=str(cycle or ""), quantity=quantity, reference_price=_positive(reference), notional=notional,
            margin=None, expires_at=None, happened_at=int(confirmed) if confirmed else None,
            client_order_id=client, note=f"{state}/{submit}"))
    return out


def _legacy_view(conn: Any, account_id: str, now_ms: int) -> Tuple[Dict[Tuple[str, str], _LegacyClaim],
                                                                     Dict[Tuple[str, str], Reservation]]:
    """``(legacy rows, open mirrors)`` of an account, both keyed by (source, legacy reference)."""
    sources = ", ".join(f"'{s}'" for s in LEGACY_SOURCES)
    mirrors = {(m.source, m.legacy_ref): m for m in _query(
        conn, f"account_id=? AND source IN ({sources}) AND state IN {_OPEN_SQL}", (account_id,))}
    wanted = sorted({_legacy_reservation_id(ref) for source, ref in mirrors if source == LEGACY_CATI_RESERVATION})
    claims = _reservation_claims(conn, account_id, now_ms, wanted) + _pending_claims(conn, account_id)
    return {(c.source, c.ref): c for c in claims}, mirrors


def _may_follow(current: str, target: str) -> bool:
    """A mirror obeys the same lifecycle as any row: an unknown outcome only ends as FILLED or RELEASED."""
    if current == RESERVED:
        return True
    if current == SUBMITTED:
        return target in (UNKNOWN, FILLED, RELEASED)
    return current == UNKNOWN and target in (FILLED, RELEASED)


def _sync_legacy(conn: Any, account_id: str, now_ms: int, actor: str) -> LegacyImport:
    """Bring the mirrors of an account in line with its two durable legacy stores. Idempotent."""
    claims, mirrors = _legacy_view(conn, account_id, now_ms)
    created, updated, held, unchanged = [], [], [], 0
    for key in sorted(claims):
        claim, mirror = claims[key], mirrors.get(key)
        if mirror is None:
            if claim.state not in LEDGER_OPEN_STATES:
                continue
            generation = conn.execute(
                f"SELECT COALESCE(MAX(legacy_generation), -1) + 1 FROM {LEDGER_TABLE} WHERE source=? AND legacy_ref=?",
                key).fetchone()[0]
            key_text = f"legacy|{claim.source}|{claim.ref}|{generation}"
            reservation_id = short_id("lres", {"account": account_id, "key": key_text})
            _insert(conn, {
                "reservation_id": reservation_id, "account_id": account_id, "user_id": claim.user_id,
                "book": EXECUTION_BOOK, "mode": "LEGACY", "source": claim.source, "legacy_ref": claim.ref,
                "legacy_generation": generation, "intent_key": key_text, "family_id": "legacy",
                "decision_id": claim.decision_id, "symbol": claim.symbol, "direction": claim.direction, "kind": ENTRY,
                "quantity": claim.quantity, "reference_price": claim.reference_price, "notional": claim.notional,
                "margin": claim.margin, "state": claim.state, "created_at": now_ms, "expires_at": claim.expires_at,
                "updated_at": now_ms, "client_order_id": claim.client_order_id,
                "unknown_since": now_ms if claim.state == UNKNOWN else None, "last_actor": actor,
                "last_detail": _note(f"IMPORTED {claim.note}")})
            created.append(reservation_id)
            continue
        sizes = {"quantity": claim.quantity, "reference_price": claim.reference_price, "notional": claim.notional,
                 "margin": claim.margin, "expires_at": claim.expires_at}
        changed = {k: v for k, v in sizes.items() if getattr(mirror, k) != v}
        target = RELEASED if (mirror.state == SUBMITTED and claim.state == EXPIRED) else claim.state
        if target != mirror.state and _may_follow(mirror.state, target):
            changed["state"] = target
            if target == UNKNOWN:
                changed["unknown_since"] = now_ms
            if target == FILLED:
                changed["filled_at"] = claim.happened_at or now_ms
            if mirror.state in (SUBMITTED, UNKNOWN):
                changed["resolution_evidence"] = _note(f"LEGACY_STORE {claim.note}")
        if changed:
            _set(conn, mirror, now_ms, actor, f"FOLLOWED {claim.note}", **changed)
            updated.append(mirror.reservation_id)
        else:
            unchanged += 1
    for key in sorted(set(mirrors) - set(claims)):
        mirror = mirrors[key]
        if mirror.state == UNKNOWN:
            held.append(mirror.reservation_id)       # a vanished row proves nothing about the order
            continue
        evidence = {"resolution_evidence": "LEGACY_ROW_REMOVED"} if mirror.state == SUBMITTED else {}
        _set(conn, mirror, now_ms, actor, "LEGACY_ROW_REMOVED", state=RELEASED, **evidence)
        updated.append(mirror.reservation_id)
    return LegacyImport(created=tuple(created), updated=tuple(updated), unchanged=unchanged, held_unknown=tuple(held))


# ----------------------------------------------------------------------------------------------------- ledger
class ReservationLedger:
    """The ledger of one database. Every method that names a reservation also names its account."""

    def __init__(self, db: Any) -> None:
        self._db = db
        self.assert_schema()

    def assert_schema(self) -> None:
        with self._db.connect() as conn:
            if not (_table(conn, LEDGER_TABLE) and _table(conn, LEDGER_EVENT_TABLE)):
                raise LedgerSchemaMissing(f"{LEDGER_TABLE} is missing: run ensure_cati_platform_schema(db)")

    # -- reserve ---------------------------------------------------------------------------------------------
    def reserve(self, intents: Union[IntentBatch, Sequence[Intent]], *, snapshot: AccountSnapshot, limits: AccountLimits,
                rules: Mapping[str, InstrumentRules], brake_state: Optional[BrakeFlags], now_ms: int,
                scope: Optional[ReservationScope] = None, ttl_ms: Optional[int] = None,
                brake_scope: Optional[str] = None, actor: str = "pipeline") -> ReservationOutcome:
        """Authorize the intents of one decision against the ledger's current state and reserve what is
        accepted, atomically. ``scope`` is taken from an ``IntentBatch``; a plain list of intents needs it.

        ``brake_state`` is the result of this decision close's ``brakes.evaluate``. A terminal stop latched in
        the database is read again here and overrides it: for ``brake_scope`` (the scope that evaluation ran
        under) and, in the execution book, for the whole account. A shadow book has its own equity and its own
        brake state, so only the scope it names can stop it. An unsent reservation expires with its decision,
        or after ``ttl_ms`` when that is sooner.
        """
        batch = intents if isinstance(intents, IntentBatch) else None
        scope = scope or (ReservationScope.of(batch) if batch is not None else None)
        if scope is None:
            raise LedgerError("SCOPE_REQUIRED", "intents outside a batch name their account and decision")
        if batch is not None and ReservationScope.of(batch) != scope:
            raise LedgerError("SCOPE_MISMATCH", "the scope is not the one of the batch")
        if scope.account_id != snapshot.account_id:
            raise LedgerError("ACCOUNT_MISMATCH", "the decision and the snapshot belong to different accounts")
        now_ms = int(now_ms)
        expires_at = scope.expires_at_ms if ttl_ms is None else min(scope.expires_at_ms, now_ms + int(ttl_ms))
        if expires_at <= now_ms:
            raise LedgerError("DECISION_EXPIRED", "a decision that has run out reserves nothing")
        book = book_of(scope.mode)
        items = batch.intents if batch is not None else tuple(intents)
        scopes = ((brakes.ACCOUNT_SCOPE,) if book == EXECUTION_BOOK else ()) + ((brake_scope,) if brake_scope else ())
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")              # the serialization point: one writer per database
            _sync_legacy(conn, scope.account_id, now_ms, actor)
            _expire(conn, now_ms, scope.account_id)
            known = {r.intent_key: r for r in _query(
                conn, f"account_id=? AND book=? AND decision_id=? AND source='{PIPELINE_SOURCE}'",
                (scope.account_id, book, scope.decision_id))}
            fresh, replayed, final = [], [], []
            for intent in items:
                earlier = known.get(intent_key(scope.decision_id, intent))
                if earlier is None:
                    fresh.append(intent)
                elif earlier.state in (RELEASED, EXPIRED):
                    final.append(RefusedIntent(
                        intent=intent, symbol=intent.symbol, kind=intent.kind, reason=INTENT_ALREADY_FINAL,
                        detail=f"{earlier.reservation_id} ended {earlier.state}; an intent is reserved once"))
                else:
                    replayed.append(earlier)
            flags = brake_state
            if flags is not None and brakes.stopped_scopes(conn, scope.account_id, scopes):
                flags = replace(flags, halted=True)
            current = replace(snapshot, reservations=_claims(conn, scope.account_id, book, now_ms, snapshot.as_of_ms))
            authorization = authorize(current, limits, fresh, rules, brake_state=flags)
            waits = json.dumps([{"symbol": r.symbol, "quantity_after": r.quantity_after} for r in authorization.releases])
            created = []
            for accepted in authorization.increases:
                key = intent_key(scope.decision_id, accepted.intent)
                reservation_id = short_id("pres", {"account": scope.account_id, "book": book, "key": key})
                _insert(conn, {
                    "reservation_id": reservation_id, "account_id": scope.account_id, "user_id": scope.user_id,
                    "book": book, "mode": scope.mode, "source": PIPELINE_SOURCE, "intent_key": key,
                    "family_id": scope.family_id, "decision_id": scope.decision_id, "decision_hash": scope.decision_hash,
                    "symbol": accepted.symbol, "direction": accepted.direction, "kind": accepted.kind,
                    "quantity": accepted.quantity, "reference_price": accepted.reference_price,
                    "stop_price": accepted.stop_price, "risk_amount": accepted.risk_amount, "notional": accepted.notional,
                    "margin": accepted.margin, "leverage": accepted.leverage,
                    "awaits_releases": waits if accepted.awaits_releases else "[]", "state": RESERVED,
                    "created_at": now_ms, "expires_at": expires_at, "updated_at": now_ms, "last_actor": actor,
                    "last_detail": "RESERVED", "snapshot_hash": snapshot.snapshot_hash, "limits_hash": limits.limits_hash,
                    "authorization_hash": authorization.authorization_hash})
                created.append(reservation_id)
            rows = replayed + [_one(conn, scope.account_id, rid) for rid in created]
        return ReservationOutcome(authorization=authorization, reservations=tuple(rows), created=tuple(created),
                                  replayed=tuple(r.reservation_id for r in replayed),
                                  refused=tuple(final) + authorization.refused)

    # -- transitions -----------------------------------------------------------------------------------------
    def mark_submitted(self, *, account_id: str, reservation_id: str, client_order_id: str, now_ms: int,
                       snapshot: Optional[AccountSnapshot] = None, actor: str = "pipeline") -> Transition:
        """RESERVED -> SUBMITTED. Call it BEFORE the order leaves and send only when it returns ``ok``.

        A reservation that fits only once an exit or a reduction of its decision has happened
        (``awaits_releases``) needs an account snapshot taken after that release; while the snapshot still shows
        the position it replaces, the order must not be sent (``RELEASE_NOT_CONFIRMED``).
        """
        if not isinstance(client_order_id, str) or not client_order_id.strip():
            raise LedgerError("CLIENT_ORDER_ID_REQUIRED", "an order is identified before it is sent")
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row, refusal = self._owned(conn, account_id, reservation_id)
            if refusal is not None:
                return refusal
            if row.state == SUBMITTED:
                same = row.client_order_id == client_order_id
                return Transition(same, False, row.state, None if same else CONFLICTING_REPLAY)
            if row.state != RESERVED:
                return Transition(False, False, row.state, f"NOT_ALLOWED_FROM_{row.state}")
            if row.expires_at is not None and row.expires_at <= int(now_ms):
                _set(conn, row, now_ms, "ledger", "TTL", state=EXPIRED)
                return Transition(False, True, EXPIRED, RESERVATION_EXPIRED)
            if row.awaits_releases:
                if snapshot is None or snapshot.account_id != account_id or snapshot.as_of_ms <= row.created_at:
                    return Transition(False, False, row.state, RELEASE_NOT_CONFIRMED)
                for release in row.awaits_releases:
                    held = snapshot.position(release.symbol)
                    if held is not None and held.quantity > release.quantity_after * (1.0 + _TOL):
                        return Transition(False, False, row.state, RELEASE_NOT_CONFIRMED)
            _set(conn, row, now_ms, actor, f"SUBMITTED {client_order_id}", state=SUBMITTED, submitted_at=int(now_ms),
                 client_order_id=client_order_id)
        return Transition(True, True, SUBMITTED)

    def mark_unknown(self, *, account_id: str, reservation_id: str, now_ms: int, resolution_deadline_ms: int,
                     detail: str = "", actor: str = "pipeline") -> Transition:
        """RESERVED | SUBMITTED -> UNKNOWN: the order may exist at the venue. The capacity stays held until
        ``resolve_unknown``; the deadline only says when the case is overdue for escalation."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row, refusal = self._owned(conn, account_id, reservation_id)
            if refusal is not None:
                return refusal
            if row.state == UNKNOWN:
                return Transition(True, False, row.state)
            if row.state not in (RESERVED, SUBMITTED):
                return Transition(False, False, row.state, f"NOT_ALLOWED_FROM_{row.state}")
            _set(conn, row, now_ms, actor, f"UNKNOWN {detail}", state=UNKNOWN, unknown_since=int(now_ms),
                 resolution_deadline=int(resolution_deadline_ms))
        return Transition(True, True, UNKNOWN)

    def mark_filled(self, *, account_id: str, reservation_id: str, filled_quantity: float, average_price: float,
                    now_ms: int, evidence: str = "", actor: str = "pipeline") -> Transition:
        """SUBMITTED -> FILLED with what the venue reported (a partial fill ends the reservation as well: the
        position is what the account now holds). An UNKNOWN row is filled by ``resolve_unknown`` only."""
        if not (filled_quantity > 0 and average_price > 0):
            raise LedgerError("INVALID_FILL", "a fill has a positive quantity and price")
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row, refusal = self._owned(conn, account_id, reservation_id)
            if refusal is not None:
                return refusal
            if row.state == FILLED:
                return Transition(True, False, row.state)
            if row.state != SUBMITTED:
                return Transition(False, False, row.state, f"NOT_ALLOWED_FROM_{row.state}")
            _set(conn, row, now_ms, actor, f"FILLED {evidence}", state=FILLED, filled_at=int(now_ms),
                 filled_quantity=float(filled_quantity), filled_price=float(average_price),
                 resolution_evidence=_note(evidence) or None)
        return Transition(True, True, FILLED)

    def release(self, *, account_id: str, reservation_id: str, now_ms: int, reason: str, evidence: str = "",
                actor: str = "pipeline") -> Transition:
        """RESERVED -> RELEASED (never sent), or SUBMITTED -> RELEASED with ``evidence`` that the venue holds no
        order and no fill (a definitive rejection, or a lookup by client order id). Never from UNKNOWN."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row, refusal = self._owned(conn, account_id, reservation_id)
            if refusal is not None:
                return refusal
            if row.state == RELEASED:
                return Transition(True, False, row.state)
            if row.state == UNKNOWN:
                return Transition(False, False, row.state, UNKNOWN_OUTCOME_NOT_RELEASABLE)
            if row.state == SUBMITTED and not _note(evidence):
                return Transition(False, False, row.state, EVIDENCE_REQUIRED)
            if row.state not in (RESERVED, SUBMITTED):
                return Transition(False, False, row.state, f"NOT_ALLOWED_FROM_{row.state}")
            _set(conn, row, now_ms, actor, f"RELEASED {reason} {evidence}", state=RELEASED,
                 resolution_evidence=_note(evidence) or None)
        return Transition(True, True, RELEASED)

    def resolve_unknown(self, *, account_id: str, reservation_id: str, outcome: str, evidence: str, now_ms: int,
                        filled_quantity: Optional[float] = None, average_price: Optional[float] = None,
                        actor: str = "reconciliation") -> Transition:
        """UNKNOWN -> FILLED | RELEASED, from reconciliation that asked the venue. ``evidence`` says what it
        found (the order, its fills, or its proven absence); without it nothing changes. A legacy mirror is
        resolved by its own store; this call ends one only when its legacy row no longer exists."""
        if outcome not in (FILLED, RELEASED):
            raise LedgerError("INVALID_OUTCOME", "an unknown outcome resolves to FILLED or RELEASED")
        if outcome == FILLED and not (filled_quantity and average_price and filled_quantity > 0 and average_price > 0):
            raise LedgerError("INVALID_FILL", "a fill has a positive quantity and price")
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row, refusal = self._owned(conn, account_id, reservation_id)
            if refusal is not None and refusal.reason == LEGACY_ROW_FOLLOWS_ITS_STORE:
                # a mirror whose legacy row no longer exists has no store left to follow: only this call ends it
                if (row.source, row.legacy_ref) not in _legacy_view(conn, account_id, int(now_ms))[0]:
                    refusal = None
            if refusal is not None:
                return refusal
            if row.state == outcome and row.unknown_since is not None:
                return Transition(True, False, row.state)
            if row.state != UNKNOWN:
                return Transition(False, False, row.state, f"NOT_ALLOWED_FROM_{row.state}")
            if not _note(evidence):
                return Transition(False, False, row.state, EVIDENCE_REQUIRED)
            fill = ({"filled_at": int(now_ms), "filled_quantity": float(filled_quantity), "filled_price": float(average_price)}
                    if outcome == FILLED else {})
            _set(conn, row, now_ms, actor, f"RESOLVED {outcome} {evidence}", state=outcome,
                 resolution_evidence=_note(evidence), **fill)
        return Transition(True, True, outcome)

    def expire_stale(self, now_ms: int, account_id: Optional[str] = None) -> int:
        """Mark unsent reservations whose decision ran out. Idempotent; nothing is deleted."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return _expire(conn, now_ms, account_id)

    @staticmethod
    def _owned(conn: Any, account_id: str, reservation_id: str) -> Tuple[Optional[Reservation], Optional[Transition]]:
        row = _one(conn, account_id, reservation_id)
        if row is None:                                  # absent, or another account's: indistinguishable on purpose
            return None, Transition(False, False, None, RESERVATION_NOT_FOUND)
        if row.source != PIPELINE_SOURCE:
            return row, Transition(False, False, row.state, LEGACY_ROW_FOLLOWS_ITS_STORE)
        return row, None

    # -- reads -----------------------------------------------------------------------------------------------
    def get(self, account_id: str, reservation_id: str) -> Optional[Reservation]:
        with self._db.connect() as conn:
            return _one(conn, account_id, reservation_id)

    def open_reservations(self, account_id: str, *, book: str, now_ms: int,
                          as_of_ms: Optional[int] = None) -> Tuple[OpenReservation, ...]:
        """The claims to put into an ``AccountSnapshot`` taken at ``as_of_ms`` (read only; ``reserve`` reads
        them again inside its transaction)."""
        with self._db.connect() as conn:
            return _claims(conn, account_id, book, int(now_ms), as_of_ms)

    def unresolved(self, account_id: Optional[str] = None) -> List[Reservation]:
        """SUBMITTED and UNKNOWN rows: what a restarted process must reconcile with the venue before anything
        else. Overdue ones are those whose ``resolution_deadline`` has passed."""
        where, params = f"state IN ('{SUBMITTED}', '{UNKNOWN}') AND source='{PIPELINE_SOURCE}'", []
        if account_id is not None:
            where, params = where + " AND account_id=?", [account_id]
        with self._db.connect() as conn:
            return sorted(_query(conn, where, params), key=lambda r: (r.updated_at, r.reservation_id))

    def history(self, account_id: str, reservation_id: str) -> List[Dict[str, Any]]:
        """The append-only record of one reservation, oldest first."""
        with self._db.connect() as conn:
            rows = conn.execute(
                f"SELECT seq, from_state, to_state, recorded_at, actor, detail FROM {LEDGER_EVENT_TABLE}"
                " WHERE reservation_id=? AND account_id=? ORDER BY seq", (reservation_id, account_id)).fetchall()
        return [{"seq": r[0], "from_state": r[1], "to_state": r[2], "recorded_at": r[3], "actor": r[4], "detail": r[5]}
                for r in rows]

    def import_legacy(self, account_id: str, now_ms: int, actor: str = "legacy-import") -> LegacyImport:
        return import_legacy_reservations(self._db, account_id, now_ms=now_ms, actor=actor)


# ------------------------------------------------------------------------------------------------- transition
def import_legacy_reservations(db: Any, account_id: str, *, now_ms: int, actor: str = "legacy-import") -> LegacyImport:
    """Mirror the open claims of the two durable legacy stores of an account into the ledger and let existing
    mirrors follow their rows. Running it again changes nothing. No legacy row is written, moved or deleted."""
    with db.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        return _sync_legacy(conn, account_id, int(now_ms), actor)


def reconcile_stores(db: Any, account_id: str, *, now_ms: int) -> StoreReconciliation:
    """Compare the ledger with the durable legacy stores of an account and report every disagreement. Read only.

    Findings: ``LEGACY_CLAIM_NOT_MIRRORED``, ``MIRROR_STATE_DIFFERS``, ``MIRROR_WITHOUT_LEGACY_ROW``,
    ``SYMBOL_CLAIMED_TWICE`` (an open pipeline reservation and an open legacy claim on one symbol),
    ``UNKNOWN_PAST_DEADLINE`` and ``RESERVED_PAST_EXPIRY``. The slot view (store 3) is derived from the positions
    and from store 2 and has no rows of its own; the two in-memory stores are named in ``not_inspected``.
    """
    now_ms = int(now_ms)
    findings: List[Finding] = []
    with db.connect() as conn:
        claims, mirrors = _legacy_view(conn, account_id, now_ms)
        ledger_open = _query(conn, f"account_id=? AND state IN {_OPEN_SQL}", (account_id,))
    for key in sorted(claims):
        claim, mirror = claims[key], mirrors.get(key)
        if mirror is None:
            if claim.state in LEDGER_OPEN_STATES:
                findings.append(Finding("LEGACY_CLAIM_NOT_MIRRORED", claim.source, claim.ref,
                                        f"{claim.symbol} is {claim.state} in its store and absent from the ledger"))
        elif mirror.state != claim.state:
            findings.append(Finding("MIRROR_STATE_DIFFERS", claim.source, claim.ref,
                                    f"ledger {mirror.state}, store {claim.state} ({claim.note})"))
    for key in sorted(set(mirrors) - set(claims)):
        findings.append(Finding("MIRROR_WITHOUT_LEGACY_ROW", key[0], key[1],
                                f"{mirrors[key].reservation_id} is {mirrors[key].state}; its legacy row is gone"))
    pipeline = {r.symbol: r for r in ledger_open if r.source == PIPELINE_SOURCE and r.book == EXECUTION_BOOK}
    for claim in sorted(claims.values(), key=lambda c: (c.source, c.ref)):
        if claim.state in LEDGER_OPEN_STATES and claim.symbol in pipeline:
            findings.append(Finding("SYMBOL_CLAIMED_TWICE", claim.source, claim.ref,
                                    f"{claim.symbol} is also reserved by {pipeline[claim.symbol].reservation_id}"))
    for row in sorted(ledger_open, key=lambda r: r.reservation_id):
        if row.state == UNKNOWN and row.resolution_deadline is not None and row.resolution_deadline <= now_ms:
            findings.append(Finding("UNKNOWN_PAST_DEADLINE", row.source, row.reservation_id,
                                    f"unresolved since {row.unknown_since}"))
        if row.state == RESERVED and row.source == PIPELINE_SOURCE and row.expires_at is not None and row.expires_at <= now_ms:
            findings.append(Finding("RESERVED_PAST_EXPIRY", row.source, row.reservation_id, f"expired at {row.expires_at}"))
    counts = (("ledger_open_pipeline", sum(1 for r in ledger_open if r.source == PIPELINE_SOURCE)),
              ("ledger_open_mirrors", len(mirrors)),
              ("legacy_open_claims", sum(1 for c in claims.values() if c.state in LEDGER_OPEN_STATES)))
    return StoreReconciliation(account_id=account_id, consistent=not findings, findings=tuple(findings), counts=counts,
                               not_inspected=VOLATILE_STORES)


__all__ = ["RESERVED", "SUBMITTED", "UNKNOWN", "FILLED", "RELEASED", "EXPIRED", "EXECUTION_BOOK", "SHADOW_BOOK",
           "LEGACY_CATI_RESERVATION", "LEGACY_PENDING_ENTRY", "LEGACY_SOURCES", "VOLATILE_STORES",
           "INTENT_ALREADY_FINAL", "RESERVATION_NOT_FOUND", "LEGACY_ROW_FOLLOWS_ITS_STORE", "RESERVATION_EXPIRED",
           "RELEASE_NOT_CONFIRMED", "EVIDENCE_REQUIRED", "UNKNOWN_OUTCOME_NOT_RELEASABLE", "CONFLICTING_REPLAY",
           "LedgerError", "LedgerSchemaMissing", "book_of", "intent_key", "ReservationScope", "Reservation",
           "ReservationOutcome", "Transition", "LegacyImport", "Finding", "StoreReconciliation", "ReservationLedger",
           "import_legacy_reservations", "reconcile_stores"]
