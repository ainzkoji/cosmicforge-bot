"""The execution boundary for a list of portfolio intents of ONE account (Section H, Step 3.8).

``PortfolioExecutionBoundary.execute`` takes an ``IntentBatch`` (several simultaneous intents of one account from
one immutable decision) and a broker-neutral ``VenueAdapter`` and is the only place where such a batch can become
orders. It extends the single-entry boundary (``execution.boundary``) and keeps its rules: the same final family
authorization, exactly-once order identity, an unknown submit that is reconciled and never re-sent, and an
exchange-side stop for every position that exists.

The canonical broker cycle reaches this boundary through ``ConnectedPortfolioRuntime``. The independent shadow
driver has no execution callback. Every real family in this repository is refused by the first gates (none is
certified, all are at stage M0); execution tests use synthetic authorized families against ``FakeVenue``.

**Gate order.** Every refusal below happens before any call that can change something at a venue.

*The whole batch* (no venue call of any kind has been made yet):

1. the batch belongs to this boundary's account, venue and environment, and the adapter is that venue's;
2. its mode is an executing mode and equals the account environment (a ``SHADOW`` batch never executes);
3. its decision is neither stale nor dated in the future;
4. the family authorization of ``pipeline.provenance.FamilyExecutionGate`` (registered, research-certified,
   promoted, not killed, the batch equals its immutable decision record, and the venue adapter is certified).
   In production settings only the real gate class is accepted. One narrow exception: when the family kill
   switch is the ONLY thing between a certified, promoted family with a verified decision and its order
   authority, the batch's risk-reducing intents are still executed (the registered ``MANAGE_TO_EXIT`` policy)
   and every risk-increasing one is refused;
5. the order-submission flag of the environment (``shared_lib.core.production.order_submission_gate``);
6. the injected adapter-certification answer for ``(venue, environment)``;
7. the adapter declares one-way positions, venue-held stops and reduce-only orders;
8. a venue back-off (429 / ban) recorded for the account has ended.

*Then*, with the intents written durably as ``PLANNED``: orders this decision already sent are brought up to date
and never sent again; resting orders of EARLIER decisions on the batch's symbols are cancelled and reconciled;
ONE consistent venue snapshot is taken (positions, balance, open orders).

*Risk-reducing intents* (exit, reduce, rebalance down, stop replacement) go first. No cap, brake, kill switch,
entry block or stale instrument rule stops them. A close re-reads the venue position, refuses a direction or
position-mode mismatch, never asks for more than is held and is sent reduce-only, so it cannot open the
opposite position.

*Risk-increasing intents* (entry, add, rebalance up) additionally need: the platform and account kill switch off
and the governance authority's consent; no durable entry block and no order of the account whose outcome is
unknown; usable instrument rules (stale, unknown or incomplete rules refuse; nothing is defaulted); the
portfolio authorization and an atomic reservation (``ReservationLedger.reserve`` re-checks every limit); a venue
stop that can follow the position; the bounded-slippage check; the family authorization AGAIN, immediately
before the order; and ``mark_submitted`` before the order leaves.

**Intent state machine** (``cati_execution_intents``, one row per order)::

    PLANNED -> SUBMITTING -> OPEN -> FILLED | PARTIAL | CANCELED
       |           |           '-> CANCELING -> PARTIAL | CANCELED | FILLED
       |           |-> FILLED | PARTIAL | REJECTED          the venue answered at once
       |           |-> UNKNOWN -> any of the above, or ABSENT, only with venue evidence
       |           '-> DEFERRED -> SUBMITTING                the venue refused to be asked; the SAME order later
       |-> REFUSED | EXPIRED                                 never sent
       '-> DONE                                              nothing had to be sent

The row is written with its client order id before the order leaves, and that id is a function of (account,
decision, symbol, intent kind, sequence): a retry can only ever name the same order. Every venue mutation is
journaled before and after (``cati_execution_events``). No database transaction spans a venue call.

**Protection.** As soon as an order that increases a position has a fill, ``pipeline.protection`` places a stop
for the quantity that exists and reads it back; after every later fill it reads the coverage back again. When
no stop can be established the position is closed reduce-only and the account is durably blocked for new
entries until an operator lifts the block.

**Recovery.** ``recover`` is what a new process runs first: every order row that may have left is looked up by its
client order id, fills are absorbed, protection is completed and reservations are settled from what the venue
reports. It sends no risk-increasing order, ever. An order the venue does not know is ``ABSENT`` only after the
window in which the request could still arrive; before that it stays ``UNKNOWN`` and its reservation stays held.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

from app.exchange.platform_adapter import contract as V
from app.exchange.platform_adapter.contract import (
    AdapterError, InstrumentRules, OrderInfo, OrderRequest, PositionInfo, round_down_to_step,
)
from app.exchange.platform_adapter.rules_cache import DEFAULT_MAX_AGE_MS
from app.trading_intelligence.execution.boundary import (
    DEFAULT_RESOLUTION_ESCALATION_MS, ENTRY_ABSENT_MAX_MS, ENTRY_ABSENT_MIN_MS,
)
from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.pipeline import portfolio as P
from app.trading_intelligence.pipeline import protection as PR
from app.trading_intelligence.pipeline import reservations as R
from app.trading_intelligence.pipeline.brakes import BrakeFlags
from app.trading_intelligence.pipeline.contracts import (
    ADD, ENTRY, EXECUTING_MODES, EXIT, FUTURE_TOLERANCE_MS, LIMIT, LIMIT_THEN_MARKET, LIVE, LONG, MARKET, REBALANCE,
    SHORT, STOP_REPLACE, TIGHTEN, AccountRef, Intent, IntentBatch, increases_risk, intent_from_plain, to_plain,
)
from app.trading_intelligence.pipeline.family_registry import MANAGE_TO_EXIT
from app.trading_intelligence.pipeline.provenance import FAMILY_GATE_UNAVAILABLE, FAMILY_KILL_SWITCH, FamilyExecutionGate
from shared_lib.persistence.cati_execution_schema import (
    INTENT_FINAL_STATES, INTENT_SENT_STATES, INTENT_TABLE, INTENT_UNSENT_STATES,
)

# ------------------------------------------------------------------------------------------------ vocabulary
#: intent order states (``cati_execution_intents.state``)
PLANNED, DEFERRED, SUBMITTING, OPEN, CANCELING, UNKNOWN = "PLANNED", "DEFERRED", "SUBMITTING", "OPEN", "CANCELING", "UNKNOWN"
FILLED, PARTIAL, CANCELED, REJECTED, ABSENT, REFUSED, EXPIRED, DONE = (
    "FILLED", "PARTIAL", "CANCELED", "REJECTED", "ABSENT", "REFUSED", "EXPIRED", "DONE")
#: what an order row is for
PRIMARY, MARKET_FALLBACK, FAILSAFE_CLOSE, STOP_REPLACEMENT = "PRIMARY", "MARKET_FALLBACK", "FAILSAFE_CLOSE", "STOP_REPLACE"
#: the first sequence number of fail-safe closes of an intent (ordinary orders stay far below it)
FAILSAFE_SEQUENCE = 90

#: batch results
BATCH_REFUSED, BATCH_DEFERRED, BATCH_PROCESSED, BATCH_HALTED = "REFUSED", "DEFERRED", "PROCESSED", "HALTED"

EXECUTION_ACCOUNT_SCOPE_MISMATCH = "EXECUTION_ACCOUNT_SCOPE_MISMATCH"
EXECUTION_VENUE_MISMATCH = "EXECUTION_VENUE_MISMATCH"
BROKER_ENVIRONMENT_MISMATCH = "BROKER_ENVIRONMENT_MISMATCH"
MODE_NOT_EXECUTABLE = "MODE_NOT_EXECUTABLE"
MODE_ENVIRONMENT_MISMATCH = "MODE_ENVIRONMENT_MISMATCH"
DECISION_STALE = "DECISION_STALE"
DECISION_FUTURE_DATED = "DECISION_FUTURE_DATED"
FAMILY_GATE_NOT_AUTHORITATIVE = "FAMILY_GATE_NOT_AUTHORITATIVE"
SUBMISSION_GATE_NOT_AUTHORITATIVE = "SUBMISSION_GATE_NOT_AUTHORITATIVE"
GOVERNANCE_AUTHORITY_NOT_AUTHORITATIVE = "GOVERNANCE_AUTHORITY_NOT_AUTHORITATIVE"
GOVERNANCE_UNAVAILABLE = "GOVERNANCE_UNAVAILABLE"
TEST_ADAPTER_NOT_ALLOWED = "TEST_ADAPTER_NOT_ALLOWED"
ADAPTER_NOT_CERTIFIED = "ADAPTER_NOT_CERTIFIED"
NATIVE_REDUCE_ONLY_PROTECTION_REQUIRED = "NATIVE_REDUCE_ONLY_PROTECTION_REQUIRED"
POSITION_MODE_NOT_ONE_WAY = "POSITION_MODE_NOT_ONE_WAY"
CATI_NEW_ENTRY_KILL_SWITCH = "CATI_NEW_ENTRY_KILL_SWITCH"
ENTRIES_BLOCKED = "ENTRIES_BLOCKED"
SYMBOL_BLOCKED = "SYMBOL_BLOCKED"
UNRESOLVED_SUBMISSION = "UNRESOLVED_SUBMISSION"
INSTRUMENT_RULES = "INSTRUMENT_RULES"
RESERVATION_FAILED = "RESERVATION_FAILED"
SNAPSHOT_UNAVAILABLE = "SNAPSHOT_UNAVAILABLE"
SNAPSHOT_INCONSISTENT = "SNAPSHOT_INCONSISTENT"
INTENT_CONFLICTS_WITH_RECORDED = "INTENT_CONFLICTS_WITH_RECORDED_INTENT"
POSITION_ALREADY_FLAT = "POSITION_ALREADY_FLAT"
DIRECTION_MISMATCH = "DIRECTION_MISMATCH"
SLIPPAGE_BOUND_EXCEEDED = "SLIPPAGE_BOUND_EXCEEDED"
MARK_PRICE_UNAVAILABLE = "MARK_PRICE_UNAVAILABLE"
PROTECTION_CANNOT_FOLLOW_INCREASE = "PROTECTION_CANNOT_FOLLOW_INCREASE"
PROTECTION_CANNOT_BE_TIGHTENED = "PROTECTION_CANNOT_BE_TIGHTENED"
LEVERAGE_NOT_SET = "LEVERAGE_NOT_SET"
RELEASE_NOT_CONFIRMED = R.RELEASE_NOT_CONFIRMED
VENUE_REFUSED = "VENUE_REFUSED"
SUBMIT_OUTCOME_UNKNOWN = "SUBMIT_OUTCOME_UNKNOWN"
ORDER_NOT_FOUND_YET = "ORDER_NOT_FOUND_WITHIN_ARRIVAL_WINDOW"
ORDER_NOT_FOUND_TOO_LATE = "ORDER_NOT_FOUND_PAST_HISTORY_WINDOW"
ORDER_VANISHED = "ACKNOWLEDGED_ORDER_NOT_FOUND"
ORDER_ABSENT = "ORDER_ABSENT"
LIMIT_NOT_FILLED_IN_TIME = "LIMIT_NOT_FILLED_IN_TIME"
DECISION_EXPIRED = "DECISION_EXPIRED"
SUPERSEDED = "SUPERSEDED_BY_NEWER_DECISION"
PARTIAL_FILL_CUT = "PARTIAL_FILL_CUT_PROTECTION_CANNOT_GROW"
#: the operator-only block raised when a position had to be closed because it could not be protected
BLOCK_PROTECTION_FAILED = "PROTECTION_FAILED_POSITION_CLOSED"

_LIMITED: Tuple[str, ...] = (V.RATE_LIMITED, V.BANNED)
_RETRYABLE_UNSENT: Tuple[str, ...] = (V.VENUE_UNAVAILABLE, V.TIMESTAMP_OUT_OF_SYNC)
_TOL = 1e-9


class RulesSource(Protocol):
    """Where instrument rules come from. ``exchange.platform_adapter.rules_cache.RulesCache`` has this shape: an
    answer has ``ok``, ``rules`` and ``reasons``. The entry answer is strict; the reduce answer may be degraded so
    that a close stays possible."""

    def rules_for_entry(self, venue: str, environment: str, symbol: str, now_ms: int, *,
                        plan_rules_hash: Optional[str] = None, plan_time_ms: Optional[int] = None) -> Any: ...

    def rules_for_reduce(self, venue: str, environment: str, symbol: str, now_ms: int) -> Any: ...


@dataclass(frozen=True)
class ExecutionPolicy:
    """Operating values of the boundary. None of them is a risk limit."""
    max_rules_age_ms: int = DEFAULT_MAX_AGE_MS                   # rules older than this never size an increase
    limit_fallback_after_ms: int = 60_000                        # how long a LIMIT_THEN_MARKET limit may rest
    unknown_escalation_ms: int = DEFAULT_RESOLUTION_ESCALATION_MS
    absent_min_ms: int = ENTRY_ABSENT_MIN_MS                     # before this a "no such order" may still arrive
    absent_max_ms: int = ENTRY_ABSENT_MAX_MS                     # after this "no such order" may mean "purged"
    failsafe_attempts: int = 3

    def __post_init__(self) -> None:
        for name in ("max_rules_age_ms", "limit_fallback_after_ms", "unknown_escalation_ms", "absent_min_ms",
                     "absent_max_ms", "failsafe_attempts"):
            if int(getattr(self, name)) <= 0:
                raise ValueError(f"{name} is positive")
        if self.absent_max_ms <= self.absent_min_ms:
            raise ValueError("the absence window ends after it starts")


@dataclass(frozen=True)
class IntentOutcome:
    """What became of one intent of a batch, from the durable rows."""
    symbol: str
    kind: str
    state: str
    reasons: Tuple[str, ...]
    increases_risk: bool
    intent_id: str
    client_order_ids: Tuple[str, ...]
    quantity: Optional[float]
    filled_quantity: float
    average_price: Optional[float]
    stop_price: Optional[float]                 # of the resting stop that protects the position, as last read back
    protection: Optional[str]                   # ``pipeline.protection`` status of the position, when one is recorded
    reservation_state: Optional[str]

    @property
    def final(self) -> bool:
        return self.state in INTENT_FINAL_STATES


@dataclass(frozen=True)
class BatchResult:
    status: str                                 # REFUSED | DEFERRED | PROCESSED | HALTED
    reasons: Tuple[str, ...]
    outcomes: Tuple[IntentOutcome, ...]
    retry_at_ms: Optional[int] = None

    def outcome(self, symbol: str) -> Optional[IntentOutcome]:
        return next((o for o in self.outcomes if o.symbol == symbol), None)


@dataclass(frozen=True)
class RecoveryReport:
    status: str                                 # REFUSED | DEFERRED | PROCESSED | HALTED
    reasons: Tuple[str, ...]
    resolved: Tuple[str, ...]                   # intent ids brought to a final state
    unresolved: Tuple[str, ...]                 # intent ids still SUBMITTING or UNKNOWN: entries stay blocked
    protections: Tuple[Any, ...]                # what ``ProtectionManager.resume`` returned
    retry_at_ms: Optional[int] = None


@dataclass(frozen=True)
class _Row:
    intent_id: str
    account_id: str
    user_id: Optional[str]
    venue: str
    environment: str
    family_id: str
    decision_id: str
    decision_hash: str
    batch_hash: str
    symbol: str
    kind: str
    direction: str
    sequence: int
    role: str
    increases_risk: int
    client_order_id: Optional[str]
    venue_order_id: Optional[str]
    side: Optional[str]
    order_type: Optional[str]
    reduce_only: int
    quantity: Optional[float]
    limit_price: Optional[float]
    reference_price: Optional[float]
    stop_price: Optional[float]
    max_slippage_bps: Optional[float]
    reservation_id: Optional[str]
    state: str
    filled_quantity: float
    average_price: Optional[float]
    reason: str
    evidence: Optional[str]
    intent_document: str
    expires_at: int
    created_at: int
    submitted_at: Optional[int]
    updated_at: int

    @property
    def intent(self) -> Intent:
        return intent_from_plain(json.loads(self.intent_document))

    @property
    def final(self) -> bool:
        return self.state in INTENT_FINAL_STATES


_COLUMNS = tuple(_Row.__dataclass_fields__)


@dataclass
class _Run:
    """One call of ``execute`` or ``recover``."""
    batch: Optional[IntentBatch]
    now: int
    brake_state: Optional[BrakeFlags] = None
    increases_refused: Optional[str] = None
    notes: Dict[str, List[str]] = field(default_factory=dict)
    closing: set = field(default_factory=set)
    resolved: List[str] = field(default_factory=list)
    halted: Optional[str] = None

    def note(self, symbol: str, text: str) -> None:
        if text and text not in self.notes.setdefault(symbol, []):
            self.notes[symbol].append(text)


class _Halt(Exception):
    """The venue told this account to wait. Nothing more is sent in this run."""


@dataclass(frozen=True)
class _AuthorityView:
    """What ``GovernanceAuthority.authorize_entry`` reads from a plan."""
    user_id: str
    broker_account_id: str
    venue: str
    environment: str


def order_client_id(account_id: str, decision_id: str, symbol: str, kind: str, sequence: int) -> str:
    """The identity of one order: a pure function of the account, the decision, the symbol, the kind of intent
    and the sequence number, so a retry names the same order. 32 lower-case characters: valid on every venue."""
    seed = f"{account_id}|{decision_id}|{symbol}|{kind}|{int(sequence)}"
    return "cfx" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:29]


def _opening_side(direction: str) -> str:
    return V.BUY if direction == LONG else V.SELL


def _within_bound(side: str, mark: float, reference: float, bps: float) -> bool:
    """Is the mark no worse than ``bps`` beyond the reference for an order on ``side``?"""
    limit = float(bps) / 10_000.0
    return mark <= reference * (1.0 + limit) if side == V.BUY else mark >= reference * (1.0 - limit)


# --------------------------------------------------------------------------------------------------- boundary
class PortfolioExecutionBoundary:
    """One account, one adapter. See the module note for the order of everything it does."""

    def __init__(self, *, db: Any, adapter: Any, account: AccountRef, limits: P.AccountLimits, rules: RulesSource,
                 adapter_certification: Callable[[str, str], Tuple[bool, str]], family_gate: Any = None,
                 authority: Any = None, submission_gate: Optional[Callable[[str], Mapping[str, Any]]] = None,
                 policy: Optional[ExecutionPolicy] = None, clock: Optional[Callable[[], int]] = None,
                 price_source: Optional[Callable[[str], Optional[float]]] = None) -> None:
        """``adapter_certification``: ``(venue, environment) -> (certified, reason)``; there is no default, an
        adapter nobody vouches for sends nothing. ``family_gate``, ``authority`` and ``submission_gate`` default to
        the real ones; a substitute is honoured outside production settings only. ``price_source`` gives the
        current mark of a symbol for the slippage bound (default: the adapter's funding record)."""
        from app.trading_intelligence.governance.promotion import GovernanceAuthority, PromotionGovernance
        from shared_lib.core.production import order_submission_gate

        if not isinstance(account, AccountRef):
            raise ValueError("the boundary belongs to one account")
        if not callable(adapter_certification):
            raise ValueError("an adapter certification answer is required")
        self._db, self._adapter, self.account, self.limits = db, adapter, account, limits
        self._rules_source, self._certification = rules, adapter_certification
        self.policy = policy or ExecutionPolicy()
        self.family_gate = family_gate if family_gate is not None else FamilyExecutionGate(db)
        self._authority = authority if authority is not None else GovernanceAuthority(db)
        self._real_authority, self._real_submission_gate = GovernanceAuthority, order_submission_gate
        self._submission_gate = submission_gate if submission_gate is not None else order_submission_gate
        self._governance = PromotionGovernance(db)
        self._price_source = price_source
        self.journal = PR.VenueJournal(db, account_id=account.broker_account_id, venue=adapter.venue,
                                       environment=adapter.environment, clock=clock)
        self.protection = PR.ProtectionManager(db=db, adapter=adapter, account_id=account.broker_account_id,
                                               journal=self.journal)
        self.ledger = R.ReservationLedger(db)

    @property
    def account_id(self) -> str:
        return self.account.broker_account_id

    def account_snapshot(self) -> P.AccountSnapshot:
        """Fresh, reconciled broker holdings for a connected-account decision.

        This reads no model/shadow book and grants no submission permission.
        """
        return self._snapshot()[0]

    # =============================================================================================== execute
    def execute(self, batch: IntentBatch, *, brake_state: Optional[BrakeFlags], now_ms: Optional[int] = None) -> BatchResult:
        """Drive the intents of one decision as far as they can go now. Calling it again with the same batch
        continues where it stopped and never sends an order twice. ``brake_state`` is the account's result of
        ``brakes.evaluate`` for this decision close; ``None`` (not known) refuses every increase."""
        now = int(now_ms if now_ms is not None else self.journal.now())
        refusal, increases_refused = self._admission(batch, now)
        if refusal is not None:
            self.journal.record("GATE", outcome=refusal, detail={
                "decision_id": getattr(getattr(batch, "provenance", None), "decision_id", None)})
            return BatchResult(BATCH_REFUSED, (refusal,), ())
        retry_at = self.journal.backoff_until()
        if retry_at is not None:
            return BatchResult(BATCH_DEFERRED, (PR.RATE_LIMIT_BACKOFF,), (), retry_at_ms=retry_at)
        run = _Run(batch=batch, now=now, brake_state=brake_state, increases_refused=increases_refused)
        self._plan(run)
        try:
            self._drive(run)
        except _Halt as halt:
            run.halted = str(halt)
        return self._result(run)

    def _drive(self, run: _Run) -> None:
        batch = run.batch
        decision_id = batch.provenance.decision_id
        # orders this decision already sent: brought up to date, never sent again
        for row in self._rows("decision_id=?", (decision_id,)):
            if row.state in INTENT_SENT_STATES:
                self._guarded(run, row, self._continue_sent)
        # resting orders of earlier decisions on these symbols give way to this one
        symbols = sorted({i.symbol for i in batch.intents})
        marks = ", ".join("?" * len(symbols))
        for row in self._rows(f"decision_id<>? AND symbol IN ({marks}) AND state IN ('{OPEN}', '{CANCELING}') AND role IN (?, ?)",
                              (decision_id, *symbols, PRIMARY, MARKET_FALLBACK)):
            self._guarded(run, row, lambda r_, o: self._cancel(r_, o, SUPERSEDED))
        self._resume_fallbacks(run)
        ranking = {intent.symbol: n for n, intent in enumerate(batch.intents)}
        pending = sorted((r for r in self._rows("decision_id=? AND sequence=0", (decision_id,))
                          if r.state in INTENT_UNSENT_STATES and r.symbol in ranking
                          and r.symbol not in run.notes.get("__conflict__", ())), key=lambda r: ranking[r.symbol])
        if not pending:
            return
        # ONE consistent snapshot of the account
        try:
            snapshot, stops = self._snapshot()
        except AdapterError as error:
            for row in pending:
                run.note(row.symbol, f"{SNAPSHOT_UNAVAILABLE}:{error.code}")
            if error.code in _LIMITED:
                raise _Halt(PR.RATE_LIMIT_BACKOFF) from None
            return
        except P.PortfolioDataError as error:
            # the account cannot be read as one state, so nothing can be sized. An exit needs no sizing: it closes
            # what its own read of the position finds, and is not held back
            for row in pending:
                run.note(row.symbol, f"{SNAPSHOT_UNAVAILABLE}:{error.code}")
                if row.kind == EXIT:
                    self._guarded(run, row, lambda r_, o: self._reduce(r_, o, None))
            return
        reducing, raising = [], []
        for row in pending:
            intent, held = row.intent, snapshot.position(row.symbol)
            rises = intent.kind in (ENTRY, ADD) or (intent.kind == REBALANCE and held is not None
                                                    and intent.target_quantity > held.quantity)
            row = self._update(row, increases_risk=int(rises))
            (raising if rises else reducing).append(row)
        rules: Dict[str, InstrumentRules] = {}
        for row in reducing:
            rule, _why = self._rules(row.symbol, entry=False, decision_time_ms=batch.provenance.decision_time_ms)
            if rule is not None:
                rules[row.symbol] = rule
        account_refusal = run.increases_refused or (self._increase_refusal() if raising else None)
        eligible: List[_Row] = []
        for row in raising:
            why = account_refusal or self._symbol_block(row.symbol)
            if why is None:
                rule, why = self._rules(row.symbol, entry=True, decision_time_ms=batch.provenance.decision_time_ms)
            if why is not None:
                self._refuse(row, why)
                continue
            rules[row.symbol] = rule
            eligible.append(row)
        accepted, refused, reservations = self._authorize(run, snapshot, reducing, eligible, rules)
        eligible = [self._record_authorized(row, accepted.get(row.symbol), reservations.get(row.symbol), rules[row.symbol])
                    for row in eligible]
        # risk-reducing intents first; nothing above this line could stop them
        for row in reducing:
            authorized = accepted.get(row.symbol)
            if authorized is None:
                self._refuse(row, refused.get(row.symbol, "NOT_AUTHORIZED"))
            else:
                self._guarded(run, row, lambda r_, o, a=authorized: self._reduce(r_, o, a))
        # risk-increasing intents
        confirmation: Optional[P.AccountSnapshot] = None
        for row in eligible:
            reservation = reservations.get(row.symbol)
            if reservation is None:
                self._refuse(row, refused.get(row.symbol, "NOT_AUTHORIZED"))
                continue
            if reservation.awaits_releases and confirmation is None:
                try:
                    confirmation, _stops = self._snapshot()             # must show the releases this order waits for
                except (AdapterError, P.PortfolioDataError) as error:
                    run.note(row.symbol, f"{RELEASE_NOT_CONFIRMED}:{error.code}")
                    if error.code in _LIMITED:
                        raise _Halt(PR.RATE_LIMIT_BACKOFF) from None
                    continue
            self._guarded(run, row, lambda r_, o, res=reservation, c=confirmation, s=stops: self._increase(r_, o, res, c, s))

    def _guarded(self, run: _Run, row: _Row, step: Callable[[_Run, _Row], Any]) -> None:
        """Run one intent's step. Whatever goes wrong stays with that intent: an error on one symbol produces no
        mutation on another. Only a venue back-off stops the whole run, because nothing may be sent at all."""
        try:
            step(run, row)
        except _Halt:
            raise
        except AdapterError as error:
            run.note(row.symbol, f"VENUE_ERROR:{error.code}")
            if error.code in _LIMITED:
                raise _Halt(PR.RATE_LIMIT_BACKOFF) from None
        except Exception as error:  # noqa: BLE001 -- recorded, attributed to this intent, never propagated to another
            run.note(row.symbol, f"INTERNAL_ERROR:{type(error).__name__}")
            self.journal.record("NOTE", symbol=row.symbol, intent_id=row.intent_id, outcome="INTERNAL_ERROR",
                                detail={"type": type(error).__name__, "message": str(error)[:200]})

    # ================================================================================================= gates
    def _admission(self, batch: Any, now: int) -> Tuple[Optional[str], Optional[str]]:
        """``(why the whole batch is refused, why its risk-increasing intents are refused)``. No venue call."""
        from app.core.config import settings
        from app.trading_intelligence.governance.runtime_authority import canonical_venue

        if not isinstance(batch, IntentBatch):
            return "INVALID_BATCH", None
        mine, theirs = self.account, batch.account
        if (theirs.user_id, theirs.broker_account_id) != (mine.user_id, mine.broker_account_id):
            return EXECUTION_ACCOUNT_SCOPE_MISMATCH, None
        if not (canonical_venue(theirs.venue) == canonical_venue(mine.venue) == canonical_venue(self._adapter.venue)):
            return EXECUTION_VENUE_MISMATCH, None
        if not (theirs.environment == mine.environment == self._adapter.environment):
            return BROKER_ENVIRONMENT_MISMATCH, None
        mode = batch.provenance.mode
        if mode not in EXECUTING_MODES:
            return f"{MODE_NOT_EXECUTABLE}:{mode}", None
        if mode != theirs.environment:
            return MODE_ENVIRONMENT_MISMATCH, None
        if batch.future_dated(now):
            return DECISION_FUTURE_DATED, None
        if batch.stale(now):
            return DECISION_STALE, None
        production = bool(settings.production)
        if production and str(getattr(self._adapter, "adapter_version", "")).startswith("fake-"):
            return TEST_ADAPTER_NOT_ALLOWED, None
        refusal, increases_refused = self._source(batch, now)
        if refusal is not None:
            return refusal, None
        refusal = self._operating_refusal(production)
        return refusal, None if refusal else increases_refused

    def _operating_refusal(self, production: bool) -> Optional[str]:
        """The conditions under which this software may send anything at all through this adapter."""
        if production and self._submission_gate is not self._real_submission_gate:
            return SUBMISSION_GATE_NOT_AUTHORITATIVE
        try:
            gate = self._submission_gate(self.account.environment)
        except Exception:  # noqa: BLE001 -- an undecidable permission is a refusal
            return "ORDER_SUBMISSION_GATE_UNAVAILABLE"
        if not gate.get("enabled"):
            return str(gate.get("reason") or "ORDER_SUBMISSION_DISABLED")
        try:
            if production:
                from app.exchange.platform_adapter.certification import adapter_certified
                certified, why = adapter_certified(
                    self._db, self._adapter.venue, self.account.environment, self.journal.now(),
                    adapter_version=self._adapter.adapter_version)
            else:
                certified, why = self._certification(self._adapter.venue, self.account.environment)
        except Exception as error:  # noqa: BLE001
            certified, why = False, type(error).__name__
        if not certified:
            return f"{ADAPTER_NOT_CERTIFIED}:{why}"
        caps = self._adapter.capabilities()
        if not (caps.native_stop_market and caps.reduce_only):
            return NATIVE_REDUCE_ONLY_PROTECTION_REQUIRED
        if not caps.one_way_mode or caps.hedge_mode:
            return POSITION_MODE_NOT_ONE_WAY
        return None

    def _source(self, batch: IntentBatch, now: int) -> Tuple[Optional[str], Optional[str]]:
        """The family authorization of the batch: ``(refusal of everything, refusal of its increases)``."""
        from app.core.config import settings

        gate = self.family_gate
        real = type(gate) is FamilyExecutionGate
        if settings.production and not real:
            return FAMILY_GATE_NOT_AUTHORITATIVE, None
        try:
            allowed, why = gate.authorize(batch, now_ms=now)
        except Exception:  # noqa: BLE001 -- an undecidable source is a refusal
            allowed, why = False, FAMILY_GATE_UNAVAILABLE
        if allowed:
            return None, None
        if why == FAMILY_KILL_SWITCH and real and self._manage_to_exit(gate, batch, now):
            return None, FAMILY_KILL_SWITCH
        return str(why), None

    @staticmethod
    def _manage_to_exit(gate: FamilyExecutionGate, batch: IntentBatch, now: int) -> bool:
        """Is the family kill switch the ONLY thing between this batch and order authority? Then the positions the
        family holds are still managed to their exit (its registered policy): reductions only."""
        from app.trading_intelligence.governance.phases import phase_index
        from app.trading_intelligence.governance.runtime_authority import canonical_venue
        from app.trading_intelligence.pipeline.family_governance import LIVE_STAGE

        try:
            entry = gate.registry.entry(batch.provenance.family_id)
            if entry is None or set(entry.blockers) - {"NO_PLATFORM_PIPELINE_ADAPTER"} != {FAMILY_KILL_SWITCH}:
                return False
            definition = entry.definition
            if definition.activation_policy.get("kill_switch_position_policy") != MANAGE_TO_EXIT:
                return False
            if batch.provenance.mode == LIVE and phase_index(entry.governance_stage) < phase_index(LIVE_STAGE):
                return False
            if canonical_venue(batch.account.venue) not in {canonical_venue(v) for v, _ in definition.supported_markets}:
                return False
            return gate.decision_verification(entry, batch, now) is None and gate.venue_certification(batch, now) is None
        except Exception:  # noqa: BLE001
            return False

    def _increase_refusal(self) -> Optional[str]:
        """Why NO risk-increasing order may be sent for this account now, or None."""
        from app.core.config import settings

        if settings.production and type(self._authority) is not self._real_authority:
            return GOVERNANCE_AUTHORITY_NOT_AUTHORITATIVE
        if settings.production:
            from app.exchange.platform_adapter.user_stream import stream_allows_entries
            try:
                allowed, reason = stream_allows_entries(
                    self._db, self._adapter.venue, self.account.environment, self.account_id, self.journal.now())
            except Exception:
                return "ACCOUNT_STREAM_HEALTH_UNAVAILABLE"
            if not allowed:
                return reason

        try:
            if self._governance.kill_switch_on(scope=self.account_id):
                return CATI_NEW_ENTRY_KILL_SWITCH
        except Exception:  # noqa: BLE001 -- an unreadable kill switch is a refusal
            return GOVERNANCE_UNAVAILABLE
        if settings.production and type(self._authority) is not self._real_authority:
            return GOVERNANCE_AUTHORITY_NOT_AUTHORITATIVE
        view = _AuthorityView(self.account.user_id, self.account_id, self.account.venue, self.account.environment)
        try:
            allowed, why = self._authority.authorize_entry(view)
        except Exception:  # noqa: BLE001
            allowed, why = False, GOVERNANCE_UNAVAILABLE
        if not allowed:
            return str(why)
        blocks = [b for b in self.journal.active_blocks() if b["scope"] == PR.ACCOUNT_SCOPE]
        if blocks:
            return f"{ENTRIES_BLOCKED}:{blocks[0]['reason']}"
        unknown = self._rows(f"state IN ('{SUBMITTING}', '{UNKNOWN}')", ())
        if unknown:
            return f"{UNRESOLVED_SUBMISSION}:{unknown[0].symbol}"
        return None

    def _symbol_block(self, symbol: str) -> Optional[str]:
        blocks = [b for b in self.journal.active_blocks(symbol) if b["scope"] == PR.SYMBOL_SCOPE and b["symbol"] == symbol]
        return f"{SYMBOL_BLOCKED}:{blocks[0]['reason']}" if blocks else None

    def _rules(self, symbol: str, *, entry: bool, decision_time_ms: Optional[int]) -> Tuple[Optional[InstrumentRules], Optional[str]]:
        """The instrument rules of ``symbol`` for an increase (strict) or a reduction (as the source allows)."""
        venue, environment, now = self._adapter.venue, self._adapter.environment, self.journal.now()
        try:
            answer = (self._rules_source.rules_for_entry(venue, environment, symbol, now, plan_time_ms=decision_time_ms)
                      if entry else self._rules_source.rules_for_reduce(venue, environment, symbol, now))
        except Exception as error:  # noqa: BLE001 -- rules that cannot be read are rules that are not known
            return None, f"{INSTRUMENT_RULES}:UNAVAILABLE:{type(error).__name__}"
        rules = getattr(answer, "rules", None)
        if not getattr(answer, "ok", False) or rules is None:
            reasons = tuple(getattr(answer, "reasons", ()) or ()) or ("RULES_UNKNOWN_INSTRUMENT",)
            return None, f"{INSTRUMENT_RULES}:{reasons[0]}"
        if (rules.symbol, rules.venue, rules.environment) != (symbol, venue, environment):
            return None, f"{INSTRUMENT_RULES}:IDENTITY_MISMATCH"
        if entry:
            age = now - int(rules.fetched_at_ms)
            if not rules.complete:
                return None, f"{INSTRUMENT_RULES}:RULES_INCOMPLETE"
            if rules.status != V.TRADING:
                return None, f"{INSTRUMENT_RULES}:RULES_INSTRUMENT_NOT_TRADING"
            if age > self.policy.max_rules_age_ms or age < -FUTURE_TOLERANCE_MS:
                return None, f"{INSTRUMENT_RULES}:RULES_STALE"
        return rules, None

    # ============================================================================================== planning
    def _plan(self, run: _Run) -> None:
        """Write every intent of the batch as a ``PLANNED`` row with its order identity. A row that exists is
        left as it is: that is what makes a second call a continuation."""
        batch, account = run.batch, self.account
        p, now = batch.provenance, self.journal.now()
        documents = {}
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for intent in batch.intents:
                documents[intent.symbol] = document = json.dumps(to_plain(intent), sort_keys=True)
                sends = intent.kind != STOP_REPLACE
                conn.execute(
                    f"INSERT OR IGNORE INTO {INTENT_TABLE} (intent_id, account_id, user_id, venue, environment, family_id, "
                    "decision_id, decision_hash, batch_hash, symbol, kind, direction, sequence, role, increases_risk, "
                    "client_order_id, state, intent_document, expires_at, created_at, updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (self._intent_id(p.decision_id, intent.symbol, intent.kind, 0), self.account_id, account.user_id,
                     self._adapter.venue, account.environment, p.family_id, p.decision_id, p.decision_hash,
                     batch.batch_hash, intent.symbol, intent.kind, intent.direction, 0,
                     PRIMARY if sends else STOP_REPLACEMENT, int(increases_risk(intent)),
                     order_client_id(self.account_id, p.decision_id, intent.symbol, intent.kind, 0) if sends else None,
                     PLANNED, document, batch.expires_at_ms, now, now))
        for row in self._rows("decision_id=? AND sequence=0", (p.decision_id,)):
            if row.symbol in documents and row.intent_document != documents[row.symbol]:
                run.note("__conflict__", row.symbol)
                run.note(row.symbol, INTENT_CONFLICTS_WITH_RECORDED)

    def _intent_id(self, decision_id: str, symbol: str, kind: str, sequence: int) -> str:
        return short_id("xint", {"account": self.account_id, "decision": decision_id, "symbol": symbol, "kind": kind,
                                 "sequence": int(sequence)})

    def _plan_order(self, origin: _Row, *, sequence: int, role: str, **values: Any) -> _Row:
        """A further order of the same intent (a market fallback, a fail-safe close), durable before it is sent."""
        intent_id = self._intent_id(origin.decision_id, origin.symbol, origin.kind, sequence)
        now = self.journal.now()
        base = {c: getattr(origin, c) for c in ("account_id", "user_id", "venue", "environment", "family_id", "decision_id",
                                                 "decision_hash", "batch_hash", "symbol", "kind", "direction",
                                                 "intent_document", "expires_at", "reference_price", "stop_price")}
        base.update(intent_id=intent_id, sequence=int(sequence), role=role, state=PLANNED, created_at=now, updated_at=now,
                    increases_risk=origin.increases_risk, reduce_only=origin.reduce_only,
                    client_order_id=order_client_id(self.account_id, origin.decision_id, origin.symbol, origin.kind, sequence))
        base.update(values)
        with self._db.connect() as conn:
            conn.execute(f"INSERT OR IGNORE INTO {INTENT_TABLE} ({', '.join(base)}) VALUES ({', '.join('?' * len(base))})",
                         tuple(base.values()))
        return self._row(intent_id)

    # ========================================================================================== authorization
    def _snapshot(self) -> Tuple[P.AccountSnapshot, Dict[str, Optional[float]]]:
        """Positions, balance and open orders as ONE state: the positions are read again after the rest and must
        not have moved. Returns the snapshot and, per held symbol, the stop that protects it (None: none)."""
        def read() -> Tuple[Any, Any, Any, Any]:
            return (self._adapter.positions(), self._adapter.balance(), self._adapter.open_orders(), self._adapter.positions())

        def shape(positions: Sequence[PositionInfo]) -> Tuple[Tuple[str, str, float], ...]:
            return tuple(sorted((p.symbol, p.side, float(p.quantity)) for p in positions if p.side != V.FLAT))

        for _attempt in range(2):
            positions, balance, orders, again = self.journal.read("snapshot", read)
            if shape(positions) == shape(again):
                break
        else:
            raise P.PortfolioDataError(SNAPSHOT_INCONSISTENT, "the positions moved while the account was being read")
        stops: Dict[str, Optional[float]] = {}
        for held in positions:
            if held.side == V.FLAT:
                continue
            side = PR.closing_side(held.side)
            resting = tuple(o for o in orders if o.symbol == held.symbol and o.open and o.order_type == V.STOP_MARKET
                            and o.side == side)
            effective = PR.Coverage(held.symbol, held.side, held, resting).effective
            stops[held.symbol] = float(effective.stop_price) if effective is not None else None
        as_of = self.journal.now()
        claims = self.ledger.open_reservations(self.account_id, book=R.EXECUTION_BOOK, now_ms=as_of, as_of_ms=as_of)
        snapshot = P.AccountSnapshot.from_venue(self.account_id, balance, positions, stops=stops, as_of_ms=as_of,
                                                reservations=claims)
        return snapshot, stops

    def _authorize(self, run: _Run, snapshot: P.AccountSnapshot, reducing: Sequence[_Row], eligible: Sequence[_Row],
                   rules: Mapping[str, InstrumentRules]) -> Tuple[Dict[str, P.AuthorizedIntent], Dict[str, str],
                                                                  Dict[str, R.Reservation]]:
        """Size the reductions, and authorize and reserve the increases atomically (``ReservationLedger.reserve``
        runs the portfolio authorization again inside its own transaction). A failure to reserve refuses the
        increases and leaves the reductions untouched."""
        reductions = [r.intent for r in reducing]
        reservations: Dict[str, R.Reservation] = {}
        authorization = None
        if eligible:
            try:
                outcome = self.ledger.reserve(reductions + [r.intent for r in eligible], snapshot=snapshot,
                                              limits=self.limits, rules=rules, brake_state=run.brake_state,
                                              now_ms=self.journal.now(), scope=R.ReservationScope.of(run.batch))
            except (R.LedgerError, P.PortfolioDataError, sqlite3.Error) as error:
                code = getattr(error, "code", type(error).__name__)
                for row in eligible:
                    run.note(row.symbol, f"{RESERVATION_FAILED}:{code}")
            else:
                authorization = outcome.authorization
                reservations = {r.symbol: r for r in outcome.reservations}
                refusals = {r.symbol: f"{r.reason}: {r.detail}" for r in outcome.refused}
        if authorization is None:
            authorization = P.authorize(snapshot, self.limits, reductions, rules, brake_state=run.brake_state)
            refusals = {r.symbol: f"{r.reason}: {r.detail}" for r in authorization.refused}
            for row in eligible:
                refusals.setdefault(row.symbol, "; ".join(run.notes.get(row.symbol, ())) or RESERVATION_FAILED)
        return {a.symbol: a for a in authorization.accepted}, refusals, reservations

    # ============================================================================================== reductions
    def _reduce(self, run: _Run, row: _Row, authorized: Optional[P.AuthorizedIntent]) -> None:
        """Exit, reduce or rebalance down. The venue position is read again and decides what can be closed.
        ``authorized`` is None only for an exit sent while the account could not be read as one state."""
        intent = row.intent
        if intent.kind == STOP_REPLACE:
            self._stop_replace(run, self._update(row, stop_price=authorized.stop_price))
            return
        held = self._position(row.symbol)
        if held is None:
            self._settle_protection(run, row.symbol, intent.direction, self.protection.retire(row.symbol, intent.direction))
            self._close(row, DONE, POSITION_ALREADY_FLAT)
            return
        if held.side != intent.direction:
            self._close(row, REFUSED, f"{DIRECTION_MISMATCH}:{held.side}")
            return
        if held.position_mode not in (None, "ONE_WAY"):
            self._close(row, REFUSED, f"{POSITION_MODE_NOT_ONE_WAY}:{held.position_mode}")
            return
        whole = intent.kind == EXIT or authorized.position_quantity_after <= 0 \
            or authorized.quantity >= held.quantity * (1.0 - _TOL)
        quantity = float(held.quantity) if whole else float(authorized.quantity)
        style = getattr(intent, "order_style", MARKET)
        side = PR.closing_side(intent.direction)
        limit = None
        if style != MARKET:
            rule = self._rules(row.symbol, entry=False, decision_time_ms=None)[0]
            # a closing limit is never placed at a worse price than stated: a sell rounds up, a buy down
            limit = P.conform_limit_price(SHORT if intent.direction == LONG else LONG, float(intent.limit_price),
                                          rule.price_tick if rule is not None else None)
        reference = authorized.reference_price if authorized is not None else getattr(intent, "reference_price", None)
        row = self._update(row, quantity=quantity, side=side, order_type=MARKET if style == MARKET else LIMIT, reduce_only=1,
                           limit_price=limit, reference_price=reference, max_slippage_bps=getattr(intent, "max_slippage_bps", None))
        request = self._request(row)
        self.protection.expect(row.symbol, intent.direction, origin=row.intent_id)
        self._send(run, row, request)

    def _stop_replace(self, run: _Run, row: _Row) -> None:
        intent = row.intent
        result = self.protection.replace(row.symbol, intent.direction, float(row.stop_price), reason=intent.reason,
                                         request_key=f"intent|{row.intent_id}", current_stop_price=intent.current_stop_price)
        run.note(row.symbol, f"STOP_REPLACEMENT:{result.outcome}")
        if result.done or result.outcome == PR.RETIREMENT_PENDING:
            self._close(row, DONE, result.outcome)
        elif result.refused:
            self._close(row, REFUSED, result.outcome)
        elif result.state == PR.REPLACEMENT_UNWOUND:
            self._close(row, REJECTED, result.outcome)
            if result.outcome == PR.PROTECTION_ABSENT:
                self._failsafe(run, row, result.outcome)
        else:
            self._update(row, state=OPEN, reason=result.outcome)        # in progress: continued by the next call

    # =============================================================================================== increases
    def _record_authorized(self, row: _Row, authorized: Optional[P.AuthorizedIntent], reservation: Optional[R.Reservation],
                           rule: InstrumentRules) -> _Row:
        """Write the authorized order (quantity, stop, limit) of a reserved intent into its row. Done for every
        intent of the batch before the first one is sent, so a run that stops half-way leaves nothing unsized.
        An intent reserved by a call that died before this point is sized from its reservation, which holds
        exactly what was authorized."""
        if reservation is None or (authorized is None and row.quantity is not None):
            return row
        intent = row.intent
        style = getattr(intent, "order_style", MARKET)
        stated_limit = getattr(intent, "limit_price", None)
        limit = authorized.limit_price if authorized is not None else (
            P.conform_limit_price(intent.direction, float(stated_limit), rule.price_tick) if stated_limit is not None else None)
        source = authorized if authorized is not None else reservation
        return self._update(row, quantity=source.quantity, stop_price=source.stop_price, limit_price=limit,
                            reference_price=source.reference_price, reservation_id=reservation.reservation_id,
                            side=_opening_side(intent.direction), order_type=MARKET if style == MARKET else LIMIT,
                            max_slippage_bps=getattr(intent, "max_slippage_bps", None))

    def _increase(self, run: _Run, row: _Row, reservation: R.Reservation, confirmation: Optional[P.AccountSnapshot],
                  stops: Mapping[str, Optional[float]]) -> None:
        """Entry, add or rebalance up: every remaining gate, then ``mark_submitted``, then the order."""
        intent, symbol = row.intent, row.symbol
        why = self._cannot_follow(row, stops) if intent.kind != ENTRY else None
        if why is None and row.order_type == MARKET and row.max_slippage_bps is not None:
            why = self._slippage_refusal(row, float(row.reference_price))
        if why is None:
            why = self._executable_risk_refusal(row, reservation)
        if why is None:
            # the same family authorization, asked again immediately before the order
            refusal, blocked = self._source(run.batch, self.journal.now())
            why = refusal or blocked or self._increase_refusal() or self._symbol_block(symbol)
        if why is not None:
            self._refuse(row, why)
            return
        if row.state == PLANNED and intent.kind == ENTRY:
            leverage = max(1, int(math.floor(float(intent.leverage) + _TOL)))
            try:
                self.journal.mutate("set_leverage", lambda: self._adapter.set_leverage(symbol, leverage), symbol=symbol,
                                    intent_id=row.intent_id, detail={"leverage": leverage})
            except AdapterError as error:
                if error.code in _LIMITED:
                    raise
                self._refuse(row, f"{LEVERAGE_NOT_SET}:{error.code}")
                return
        try:
            request = self._request(row)
        except ValueError as error:
            self._refuse(row, f"INVALID_ORDER:{str(error)[:120]}")
            return
        marked = self.ledger.mark_submitted(account_id=self.account_id, reservation_id=row.reservation_id,
                                            client_order_id=row.client_order_id, now_ms=self.journal.now(),
                                            snapshot=confirmation if reservation.awaits_releases else None)
        if not marked.ok:
            if marked.reason == R.RELEASE_NOT_CONFIRMED:
                run.note(symbol, RELEASE_NOT_CONFIRMED)                 # stays PLANNED and reserved: the next call retries
            else:
                self._refuse(row, f"RESERVATION_{marked.reason}")
            return
        self.protection.expect(symbol, intent.direction, float(row.stop_price), origin=row.intent_id)
        self._send(run, row, request)

    def _cannot_follow(self, row: _Row, stops: Mapping[str, Optional[float]]) -> Optional[str]:
        """An increase of a position is sent only when the venue's stop can follow it. Where no gap-free
        replacement exists the stop can neither grow (a quantity stop) nor move to the price this increase was
        authorized with, and the increase is refused instead of being sent under a stop that does not fit."""
        resting = stops.get(row.symbol)
        protocol, why = self.protection.protocol()
        if protocol is not None or resting is None:
            return None
        if not self.protection.follows_position():
            return f"{PROTECTION_CANNOT_FOLLOW_INCREASE}:{why}"
        if not PR.same_price(resting, row.stop_price):
            return f"{PROTECTION_CANNOT_BE_TIGHTENED}:{why}"
        return None

    def _slippage_refusal(self, row: _Row, reference: float) -> Optional[str]:
        """The bound of a market order: the current mark may be at most ``max_slippage_bps`` beyond the reference.
        A mark that cannot be read refuses; a bound is never assumed to hold."""
        try:
            mark = self._mark(row.symbol)
        except AdapterError as error:
            if error.code in _LIMITED:
                raise
            return f"{MARK_PRICE_UNAVAILABLE}:{error.code}"
        if mark is None or not mark > 0:
            return MARK_PRICE_UNAVAILABLE
        if not _within_bound(row.side, float(mark), reference, float(row.max_slippage_bps)):
            return f"{SLIPPAGE_BOUND_EXCEEDED}: mark {mark} against {reference} with {row.max_slippage_bps} bps"
        return None

    def _executable_risk_refusal(self, row: _Row, reservation: R.Reservation) -> Optional[str]:
        """A known adverse price cannot spend more stop risk than was authorized.

        Limits use the worst fill their price permits; markets use a fresh mark.
        A fallback also accounts for the risk its earlier partial fill consumed.
        This does not guarantee a market fill price or remove gap/slippage risk.
        """
        try:
            price = float(row.limit_price) if row.order_type == LIMIT else self._mark(row.symbol)
        except AdapterError as error:
            if error.code in _LIMITED:
                raise
            return f"{MARK_PRICE_UNAVAILABLE}:{error.code}"
        if price is None or not math.isfinite(float(price)) or price <= 0:
            return MARK_PRICE_UNAVAILABLE
        distance = float(price) - float(row.stop_price) if row.direction == LONG else float(row.stop_price) - float(price)
        if distance <= 0:
            return "EXECUTABLE_STOP_GEOMETRY_INVALID"
        if row.kind == ENTRY and distance / float(price) > self.limits.max_entry_stop_distance * (1 + _TOL):
            return P.ENTRY_STOP_TOO_WIDE
        used = 0.0
        for prior in self._rows("reservation_id=? AND intent_id<>? AND filled_quantity>0",
                                (row.reservation_id, row.intent_id)):
            if prior.average_price is None:
                return "FILLED_RISK_EVIDENCE_REQUIRED"
            used += prior.filled_quantity * abs(float(prior.average_price) - float(prior.stop_price))
        if reservation.risk_amount is None:
            return "RESERVATION_RISK_EVIDENCE_REQUIRED"
        risk = used + float(row.quantity) * distance
        if risk > float(reservation.risk_amount) * (1 + _TOL) + _TOL:
            return "EXECUTABLE_PRICE_RISK_CHANGED"
        return None

    def _mark(self, symbol: str) -> Optional[float]:
        if self._price_source is not None:
            return self._price_source(symbol)
        return self.journal.read("funding", lambda: self._adapter.funding(symbol)).mark_price

    # ================================================================================================= orders
    def _request(self, row: _Row) -> OrderRequest:
        if row.order_type == LIMIT:
            return OrderRequest(symbol=row.symbol, side=row.side, order_type=V.LIMIT, client_order_id=row.client_order_id,
                                quantity=float(row.quantity), price=float(row.limit_price), reduce_only=bool(row.reduce_only),
                                time_in_force="GTC")
        return OrderRequest(symbol=row.symbol, side=row.side, order_type=V.MARKET, client_order_id=row.client_order_id,
                            quantity=float(row.quantity), reduce_only=bool(row.reduce_only))

    def _send(self, run: _Run, row: _Row, request: OrderRequest) -> _Row:
        """Send the order of a row. The row says ``SUBMITTING`` before the order leaves and the journal brackets
        the call, so a process that dies here leaves an order that will be looked up, not sent again."""
        row = self._update(row, state=SUBMITTING, submitted_at=self.journal.now(), reason="")
        try:
            from app.trading_intelligence.execution.entry_permit import portfolio_entry_permit
            with portfolio_entry_permit(self._db, row.intent_id, request):
                info = self.journal.mutate("place_order", lambda: self._adapter.place_order(request), symbol=row.symbol,
                                           client_order_id=row.client_order_id, intent_id=row.intent_id,
                                           detail={"side": row.side, "type": row.order_type, "quantity": row.quantity,
                                                   "reduce_only": bool(row.reduce_only), "role": row.role})
        except AdapterError as error:
            return self._send_failed(run, row, error)
        return self._absorb(run, row, info)

    def _send_failed(self, run: _Run, row: _Row, error: AdapterError) -> _Row:
        if error.uncertain:
            # the venue may hold the order. It is looked up by its client order id and never sent again blindly
            return self._lookup(run, row, SUBMIT_OUTCOME_UNKNOWN)
        if error.code in _LIMITED:
            self._update(row, state=DEFERRED, reason=f"{VENUE_REFUSED}:{error.code}")
            raise error                                                  # nothing more is sent in this run
        if error.code in _RETRYABLE_UNSENT:
            return self._update(row, state=DEFERRED, reason=f"{VENUE_REFUSED}:{error.code}")
        if error.code == V.REDUCE_ONLY_REJECTED and row.reduce_only and self._position(row.symbol) is None:
            self._settle_protection(run, row.symbol, row.direction, self.protection.retire(row.symbol, row.direction))
            return self._close(row, DONE, POSITION_ALREADY_FLAT, evidence="the venue refused the close: nothing is open")
        row = self._close(row, REJECTED, f"{VENUE_REFUSED}:{error.code}", evidence=f"definitive venue refusal {error.code}")
        self._finalize(run, row)
        return row

    def _lookup(self, run: _Run, row: _Row, reason: str) -> _Row:
        """An order whose submit outcome is not known: ask the venue for it by client order id."""
        try:
            info = self.journal.read("order", lambda: self._adapter.order(row.symbol, client_order_id=row.client_order_id))
        except AdapterError as error:
            if error.code == V.ORDER_NOT_FOUND:
                return self._not_found(run, row)
            self._unknown(row, f"{reason}|LOOKUP:{error.code}")
            raise
        return self._absorb(run, row, info)

    def _not_found(self, run: _Run, row: _Row) -> _Row:
        """The venue holds no order with this id. That is evidence of absence only once the request can no
        longer arrive and while the venue still keeps its order history; until then the order is unknown."""
        if row.venue_order_id:
            return self._unknown(row, ORDER_VANISHED)                   # it existed: silence is not a cancellation
        age = self.journal.now() - int(row.submitted_at or row.updated_at)
        if age < self.policy.absent_min_ms:
            return self._unknown(row, ORDER_NOT_FOUND_YET)
        if age > self.policy.absent_max_ms:
            return self._unknown(row, ORDER_NOT_FOUND_TOO_LATE)
        row = self._close(row, ABSENT, ORDER_ABSENT, evidence=f"ORDER_NOT_FOUND by client order id {age} ms after the send")
        self._finalize(run, row)
        return row

    def _unknown(self, row: _Row, reason: str) -> _Row:
        """The order may exist. Its reservation stays held and it is never treated as absent."""
        row = self._update(row, state=UNKNOWN, reason=reason) if row.state != UNKNOWN else self._update(row, reason=reason)
        if row.reservation_id:
            now = self.journal.now()
            self.ledger.mark_unknown(account_id=self.account_id, reservation_id=row.reservation_id, now_ms=now,
                                     resolution_deadline_ms=now + self.policy.unknown_escalation_ms, detail=reason)
        return row

    def _absorb(self, run: _Run, row: _Row, info: OrderInfo) -> _Row:
        """Take what the venue says about an order into its row: fills, protection for what filled, its end."""
        if info.symbol != row.symbol or info.side != row.side or \
                (info.client_order_id is not None and info.client_order_id != row.client_order_id):
            return self._unknown(row, "VENUE_RETURNED_ANOTHER_ORDER")
        if info.status == V.UNKNOWN:
            return self._unknown(row, "VENUE_ORDER_STATE_UNMAPPED")
        filled = float(info.filled_quantity)
        grew = filled > float(row.filled_quantity) + _TOL
        if info.final:
            wanted = float(row.quantity or 0.0)
            state = FILLED if filled >= wanted * (1.0 - _TOL) and filled > 0 else PARTIAL if filled > 0 else \
                REJECTED if info.status == V.REJECTED else CANCELED
        else:
            state = CANCELING if row.state == CANCELING else OPEN
        row = self._update(row, state=state, filled_quantity=filled, venue_order_id=info.venue_order_id,
                           average_price=info.average_price if info.average_price is not None else row.average_price,
                           evidence=f"venue order {info.venue_order_id} {info.status} filled {filled}")
        if grew and state == OPEN and not row.reduce_only and self._must_cut(row):
            # this venue's stop cannot grow with the position: the rest of the order is cancelled first, so the
            # stop is placed once, for a quantity that is final
            return self._cancel(run, row, PARTIAL_FILL_CUT)
        try:
            if grew or (row.final and row.reduce_only):
                self._after_fill(run, row)                               # also after a close that ended unfilled
        finally:
            if row.final:
                run.resolved.append(row.intent_id)
                self._finalize(run, row)                                 # whatever happened to the protection
        return row

    def _must_cut(self, row: _Row) -> bool:
        return not self.protection.follows_position() and self.protection.protocol()[0] != PR.CREATE_CONFIRM_RETIRE

    def _after_fill(self, run: _Run, row: _Row) -> None:
        """Protection follows the filled quantity: read back after every fill, placed when none rests."""
        symbol, direction = row.symbol, row.direction
        if row.reduce_only:
            self._settle_protection(run, symbol, direction, self.protection.verify(symbol, direction))
            return
        outcome = self.protection.protect(symbol, direction, float(row.stop_price), origin=row.intent_id)
        if outcome.protected and outcome.stop_price is not None and PR.wider(direction, outcome.stop_price, float(row.stop_price)):
            # the position grew under a stop that is wider than the one this increase was authorized with
            moved = self.protection.replace(symbol, direction, float(row.stop_price), reason=TIGHTEN,
                                            request_key=f"fill|{row.intent_id}")
            run.note(symbol, f"STOP_REPLACEMENT:{moved.outcome}")
            outcome = self.protection.record(symbol) or outcome
        self._settle_protection(run, symbol, direction, outcome, origin=row)

    def _settle_protection(self, run: _Run, symbol: str, direction: str, outcome: PR.ProtectionOutcome,
                           origin: Optional[_Row] = None) -> None:
        if outcome.status not in (PR.PROTECTED, PR.FLAT):
            run.note(symbol, f"PROTECTION_{outcome.status}:{outcome.reason}")
        if outcome.status == PR.ABSENT and origin is not None:
            self._failsafe(run, origin, outcome.reason)

    def _failsafe(self, run: _Run, origin: _Row, reason: str) -> None:
        """A position that cannot be protected is closed reduce-only, and the account takes no new entry until an
        operator lifts the block. The block is written first."""
        symbol, direction = origin.symbol, origin.direction
        if symbol in run.closing:
            return
        run.closing.add(symbol)
        self.journal.block(BLOCK_PROTECTION_FAILED, symbol=symbol, operator_only=True, detail=reason)
        for resting in self._rows(f"symbol=? AND state='{OPEN}' AND reduce_only=0", (symbol,)):
            self._cancel(run, resting, PR.PROTECTION_ABSENT)             # no further fill of an order that cannot be covered
        sent = len(self._rows("decision_id=? AND symbol=? AND kind=? AND role=?",
                              (origin.decision_id, symbol, origin.kind, FAILSAFE_CLOSE)))
        for attempt in range(sent, self.policy.failsafe_attempts):
            held = self._position(symbol)
            if held is None or held.side != direction:
                break
            close = self._plan_order(origin, sequence=FAILSAFE_SEQUENCE + attempt, role=FAILSAFE_CLOSE,
                                     quantity=float(held.quantity), side=PR.closing_side(direction), order_type=MARKET,
                                     reduce_only=1, increases_risk=0, limit_price=None, max_slippage_bps=None)
            close = self._send(run, close, self._request(close))
            if close.state not in (FILLED, PARTIAL):
                break                                                    # unknown or refused: looked up, not hammered
        run.note(symbol, f"FAILSAFE_CLOSE:{reason}")
        self._settle_protection(run, symbol, direction, self.protection.verify(symbol, direction))

    def _cancel(self, run: _Run, row: _Row, reason: str) -> _Row:
        """Cancel a resting order, then read the order itself: what the venue reports about it decides what
        filled before it ended. A cancel request that was not answered proves nothing."""
        row = self._update(row, state=CANCELING, reason=reason)          # durable before the cancel leaves
        try:
            self.journal.mutate("cancel_order", lambda: self._adapter.cancel_order(row.symbol, client_order_id=row.client_order_id),
                                symbol=row.symbol, client_order_id=row.client_order_id, intent_id=row.intent_id,
                                detail={"reason": reason})
        except AdapterError as error:
            if error.code in _LIMITED:
                raise
        return self._sync(run, row)

    def _sync(self, run: _Run, row: _Row) -> _Row:
        try:
            info = self.journal.read("order", lambda: self._adapter.order(row.symbol, client_order_id=row.client_order_id))
        except AdapterError as error:
            if error.code == V.ORDER_NOT_FOUND:
                return self._unknown(row, ORDER_VANISHED)
            raise
        return self._absorb(run, row, info)

    def _continue_sent(self, run: _Run, row: _Row) -> None:
        """An order row that may have left: bring it in line with the venue. Nothing here sends it again."""
        if row.role == STOP_REPLACEMENT:
            self._stop_replace(run, row)
            return
        if row.state in (SUBMITTING, UNKNOWN):
            row = self._lookup(run, row, row.reason or SUBMIT_OUTCOME_UNKNOWN)
        elif row.state == CANCELING:
            row = self._cancel(run, row, row.reason)
        else:
            row = self._sync(run, row)
        if row.state != OPEN:
            return
        if self.journal.now() >= row.expires_at:
            self._cancel(run, row, DECISION_EXPIRED)
        elif run.batch is not None and row.role == PRIMARY and row.order_type == LIMIT \
                and getattr(row.intent, "order_style", None) == LIMIT_THEN_MARKET \
                and self.journal.now() - int(row.submitted_at) >= self.policy.limit_fallback_after_ms:
            self._fallback(run, row)

    def _fallback(self, run: _Run, row: _Row) -> None:
        """LIMIT_THEN_MARKET: the limit did not fill in time. It is cancelled, its end is read from the venue, and
        only then is the remainder sent as a market order inside the intent's slippage bound."""
        fallback = self._plan_order(row, sequence=row.sequence + 1, role=MARKET_FALLBACK, order_type=MARKET, side=row.side,
                                    reservation_id=row.reservation_id, max_slippage_bps=row.max_slippage_bps)
        row = self._cancel(run, row, LIMIT_NOT_FILLED_IN_TIME)
        self._send_fallback(run, row, fallback)

    def _resume_fallbacks(self, run: _Run) -> None:
        """A fallback that was planned and never sent (the process died after the cancel): continue it."""
        for fallback in self._rows(f"decision_id=? AND role=? AND state='{PLANNED}'",
                                   (run.batch.provenance.decision_id, MARKET_FALLBACK)):
            primary = self._row(self._intent_id(fallback.decision_id, fallback.symbol, fallback.kind, fallback.sequence - 1))
            if primary is not None:
                self._guarded(run, fallback, lambda r_, o, first=primary: self._send_fallback(r_, first, o))

    def _send_fallback(self, run: _Run, row: _Row, fallback: _Row) -> None:
        if not row.final or fallback.state != PLANNED:
            return                                                       # no second order while the first may still fill
        why, remaining = None, float(row.quantity) - float(row.filled_quantity)
        rule, rules_refusal = self._rules(row.symbol, entry=bool(row.increases_risk),
                                          decision_time_ms=run.batch.provenance.decision_time_ms)
        if rule is None or rule.quantity_step is None:
            why = rules_refusal or f"{INSTRUMENT_RULES}:RULES_INCOMPLETE"
        else:
            remaining = round_down_to_step(remaining, rule.quantity_step)
            reference = float(row.limit_price)
            if remaining <= 0 or (rule.min_quantity is not None and remaining < rule.min_quantity) or \
                    (not row.reduce_only and rule.min_notional is not None and remaining * reference < rule.min_notional):
                why = "REMAINDER_BELOW_EXCHANGE_MINIMUM"                 # never raised to reach a minimum
            else:
                why = self._slippage_refusal(fallback, reference)
        if why is None and row.increases_risk:
            refusal, blocked = self._source(run.batch, self.journal.now())
            why = refusal or blocked or self._increase_refusal() or self._symbol_block(row.symbol)
            if why is None:
                reservation = self.ledger.get(account_id=self.account_id, reservation_id=row.reservation_id)
                why = "RESERVATION_RISK_EVIDENCE_REQUIRED" if reservation is None else self._executable_risk_refusal(
                    self._update(fallback, quantity=remaining), reservation)
        if why is not None:
            self._finalize(run, self._close(fallback, REFUSED, why))
            return
        fallback = self._update(fallback, quantity=remaining)
        request = self._request(fallback)
        self.protection.expect(fallback.symbol, fallback.direction,
                               None if fallback.reduce_only else float(fallback.stop_price), origin=fallback.intent_id)
        self._send(run, fallback, request)

    # =========================================================================================== reservations
    def _finalize(self, run: Optional[_Run], row: _Row) -> None:
        """Settle the reservation of an intent once every order of it has ended: consumed for what filled,
        released (with the venue evidence) when nothing did."""
        if not row.reservation_id:
            return
        orders = [o for o in self._rows("decision_id=? AND symbol=? AND kind=?", (row.decision_id, row.symbol, row.kind))
                  if o.role in (PRIMARY, MARKET_FALLBACK)]
        if any(not o.final for o in orders):
            return
        held = self.ledger.get(self.account_id, row.reservation_id)
        if held is None or not held.open:
            return
        filled = sum(float(o.filled_quantity) for o in orders)
        value = sum(float(o.filled_quantity) * float(o.average_price or o.limit_price or o.reference_price
                                                     or held.reference_price or 0.0) for o in orders)
        now = self.journal.now()
        evidence = "; ".join(f"{o.client_order_id} {o.state} filled {o.filled_quantity} ({o.evidence or o.reason})" for o in orders)
        common = dict(account_id=self.account_id, reservation_id=row.reservation_id, now_ms=now)
        if held.state == R.UNKNOWN:
            done = self.ledger.resolve_unknown(outcome=R.FILLED if filled > 0 else R.RELEASED, evidence=evidence,
                                               filled_quantity=filled or None,
                                               average_price=value / filled if filled > 0 else None, **common)
        elif filled > 0:
            done = self.ledger.mark_filled(filled_quantity=filled, average_price=value / filled, evidence=evidence, **common)
        else:
            done = self.ledger.release(reason="ORDER_ENDED_UNFILLED", evidence=evidence, **common)
        if not done.ok:
            if run is not None:
                run.note(row.symbol, f"RESERVATION_NOT_SETTLED:{done.reason}")
            self.journal.record("NOTE", symbol=row.symbol, intent_id=row.intent_id, outcome="RESERVATION_NOT_SETTLED",
                                detail={"reservation_id": row.reservation_id, "reason": done.reason, "state": done.state})

    def _refuse(self, row: _Row, reason: str) -> _Row:
        """An intent that is not sent. What it reserved is given back (with the refusal as the evidence that no
        order of it exists, when the reservation had already been marked submitted)."""
        row = self._close(row, REFUSED, reason)
        self._finalize(None, row)
        return row

    # ================================================================================================ recovery
    def recover(self, *, now_ms: Optional[int] = None) -> RecoveryReport:
        """What a new process runs before anything else, and what a running one repeats: resolve every order
        that may have left from venue truth, complete protection, settle reservations, end what expired. It
        sends cancels, protective stops and reduce-only closes; it never sends an order that increases risk."""
        from app.core.config import settings

        now = int(now_ms if now_ms is not None else self.journal.now())
        refusal = self._operating_refusal(bool(settings.production))
        if refusal is not None:
            return RecoveryReport(BATCH_REFUSED, (refusal,), (), self._unresolved_ids(), ())
        retry_at = self.journal.backoff_until()
        if retry_at is not None:
            return RecoveryReport(BATCH_DEFERRED, (PR.RATE_LIMIT_BACKOFF,), (), self._unresolved_ids(), (), retry_at)
        run = _Run(batch=None, now=now)
        protections: Tuple[Any, ...] = ()
        try:
            for row in self._rows("state IN (" + ", ".join(f"'{s}'" for s in INTENT_SENT_STATES) + ")", ()):
                self._guarded(run, row, self._continue_sent)
            for row in self._rows(f"state IN ('{PLANNED}', '{DEFERRED}') AND expires_at<=?", (now,)):
                self._expire(run, row)
            for held in self.ledger.unresolved(self.account_id):
                # a process that died between an order's end and its reservation: settled from the rows
                ended = self._rows("reservation_id=?", (held.reservation_id,))
                if ended:
                    self._finalize(run, ended[0])
            protections = self.protection.resume()
            for outcome in protections:
                if isinstance(outcome, PR.ProtectionOutcome) and outcome.status == PR.ABSENT:
                    self._failsafe_recorded(run, outcome)
            for outcome in self.protection.unsettled():
                if outcome.status == PR.ABSENT:
                    self._failsafe_recorded(run, outcome)
        except _Halt as halt:
            run.halted = str(halt)
        self.ledger.expire_stale(now, self.account_id)
        reasons = tuple(f"{s}: {'; '.join(n)}" for s, n in sorted(run.notes.items()))
        return RecoveryReport(BATCH_HALTED if run.halted else BATCH_PROCESSED, reasons, tuple(run.resolved),
                              self._unresolved_ids(), protections, self.journal.backoff_until())

    def _failsafe_recorded(self, run: _Run, outcome: PR.ProtectionOutcome) -> None:
        """A recorded position whose protection is absent: close it with the identity of the order that opened it."""
        origins = self._rows("symbol=? AND reduce_only=0 AND role IN (?, ?) ORDER BY created_at DESC",
                             (outcome.symbol, PRIMARY, MARKET_FALLBACK))
        if origins:
            self._guarded(run, origins[0], lambda r_, o: self._failsafe(r_, o, outcome.reason))

    def _expire(self, run: _Run, row: _Row) -> None:
        """An intent that was never sent and whose decision ran out. A reservation that was marked submitted for
        an order the venue definitively refused is released with that refusal as evidence."""
        row = self._close(row, EXPIRED, DECISION_EXPIRED, evidence=row.reason or DECISION_EXPIRED)
        run.resolved.append(row.intent_id)
        self._finalize(run, row)

    def cancel(self, intent_id: str, *, reason: str = "CANCEL_REQUESTED") -> Optional[IntentOutcome]:
        """Cancel the resting order of one intent of THIS account and reconcile its end. An id of another
        account does not exist here. Returns None when there is no such intent."""
        row = self._row(intent_id)
        if row is None:
            return None
        run = _Run(batch=None, now=self.journal.now())
        if row.state in (OPEN, CANCELING) and row.role != STOP_REPLACEMENT and self.journal.backoff_until() is None:
            try:
                self._guarded(run, row, lambda r_, o: self._cancel(r_, o, reason))
            except _Halt as halt:
                run.halted = str(halt)
        primary = self._row(self._intent_id(row.decision_id, row.symbol, row.kind, 0)) or row
        return self._outcome(run, primary)

    # ================================================================================================== reads
    def entry_blocks(self) -> Tuple[Dict[str, Any], ...]:
        """The durable blocks that stop new entries of this account (and of single symbols)."""
        return self.journal.active_blocks()

    def clear_entry_block(self, block_id: str, *, operator_ref: str, evidence: str) -> bool:
        """An operator lifts a block, naming themselves and the evidence. Recorded; never automatic."""
        return self.journal.clear_by_operator(block_id, operator_ref=operator_ref, evidence=evidence)

    def intents(self, decision_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """The durable order rows of this account (of one decision when named), oldest first."""
        rows = self._rows("decision_id=?", (decision_id,)) if decision_id else self._rows("1=1", ())
        return [{c: getattr(r, c) for c in _COLUMNS} for r in rows]

    def _unresolved_ids(self) -> Tuple[str, ...]:
        return tuple(r.intent_id for r in self._rows(f"state IN ('{SUBMITTING}', '{UNKNOWN}')", ()))

    def _position(self, symbol: str) -> Optional[PositionInfo]:
        """The venue position of one symbol, read now (None: flat)."""
        held = [p for p in self.journal.read("positions", lambda: self._adapter.positions(symbol))
                if p.symbol == symbol and p.side != V.FLAT]
        return held[0] if held else None

    def _result(self, run: _Run) -> BatchResult:
        rows = self._rows("decision_id=? AND sequence=0", (run.batch.provenance.decision_id,))
        order = {i.symbol: n for n, i in enumerate(run.batch.intents)}
        outcomes = tuple(self._outcome(run, r) for r in sorted(rows, key=lambda r: order.get(r.symbol, len(order))))
        return BatchResult(BATCH_HALTED if run.halted else BATCH_PROCESSED, (run.halted,) if run.halted else (), outcomes,
                           retry_at_ms=self.journal.backoff_until())

    def _outcome(self, run: _Run, primary: _Row) -> IntentOutcome:
        related = self._rows("decision_id=? AND symbol=? AND kind=?", (primary.decision_id, primary.symbol, primary.kind))
        orders = [o for o in related if o.role != FAILSAFE_CLOSE]
        unfinished = [o for o in orders if not o.final]
        filled = sum(float(o.filled_quantity) for o in orders)
        value = sum(float(o.filled_quantity) * float(o.average_price or 0.0) for o in orders)
        if unfinished:
            state = unfinished[-1].state
        elif len(orders) == 1 or filled <= 0:
            state = orders[-1].state if filled <= 0 else primary.state
        else:
            state = FILLED if filled >= float(primary.quantity or 0.0) * (1.0 - _TOL) else PARTIAL
        reasons = [o.reason for o in related if o.reason] + list(run.notes.get(primary.symbol, ()))
        reasons += [f"{FAILSAFE_CLOSE}:{o.state}" for o in related if o.role == FAILSAFE_CLOSE]
        protection = self.protection.record(primary.symbol)
        reservation = self.ledger.get(self.account_id, primary.reservation_id) if primary.reservation_id else None
        return IntentOutcome(
            symbol=primary.symbol, kind=primary.kind, state=state, reasons=tuple(dict.fromkeys(reasons)),
            increases_risk=bool(primary.increases_risk), intent_id=primary.intent_id,
            client_order_ids=tuple(o.client_order_id for o in related if o.client_order_id and o.submitted_at),
            quantity=primary.quantity, filled_quantity=filled, average_price=value / filled if filled > 0 else None,
            stop_price=protection.stop_price if protection is not None else None,
            protection=protection.status if protection is not None else None,
            reservation_state=reservation.state if reservation is not None else None)

    # ============================================================================================ persistence
    def _rows(self, where: str, params: Sequence[Any]) -> List[_Row]:
        """Order rows of THIS account. Every query is scoped by the account: another account's rows do not exist."""
        order = "" if "ORDER BY" in where else " ORDER BY created_at, sequence, intent_id"
        with self._db.connect() as conn:
            rows = conn.execute(f"SELECT {', '.join(_COLUMNS)} FROM {INTENT_TABLE} WHERE account_id=? AND {where}{order}",
                                (self.account_id, *params)).fetchall()
        return [_Row(**dict(zip(_COLUMNS, tuple(r)))) for r in rows]

    def _row(self, intent_id: str) -> Optional[_Row]:
        rows = self._rows("intent_id=?", (intent_id,))
        return rows[0] if rows else None

    def _update(self, row: _Row, **changes: Any) -> _Row:
        changes["updated_at"] = self.journal.now()
        for key in ("reason", "evidence"):
            if changes.get(key) is not None:
                changes[key] = PR._note(changes[key])
        with self._db.connect() as conn:
            conn.execute(f"UPDATE {INTENT_TABLE} SET {', '.join(f'{k}=?' for k in changes)} WHERE intent_id=? AND account_id=?",
                         (*changes.values(), row.intent_id, self.account_id))
        return self._row(row.intent_id)

    def _close(self, row: _Row, state: str, reason: str, *, evidence: Optional[str] = None) -> _Row:
        """Put a row into a final state. A row that is final already is returned as it is."""
        if row.final:
            return row
        return self._update(row, state=state, reason=reason, evidence=evidence or row.evidence or reason)


__all__ = ["PLANNED", "DEFERRED", "SUBMITTING", "OPEN", "CANCELING", "UNKNOWN", "FILLED", "PARTIAL", "CANCELED", "REJECTED",
           "ABSENT", "REFUSED", "EXPIRED", "DONE", "PRIMARY", "MARKET_FALLBACK", "FAILSAFE_CLOSE", "STOP_REPLACEMENT",
           "FAILSAFE_SEQUENCE", "BATCH_REFUSED", "BATCH_DEFERRED", "BATCH_PROCESSED", "BATCH_HALTED",
           "BLOCK_PROTECTION_FAILED", "RulesSource", "ExecutionPolicy", "IntentOutcome", "BatchResult", "RecoveryReport",
           "PortfolioExecutionBoundary", "order_client_id"]
