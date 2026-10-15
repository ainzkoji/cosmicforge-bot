"""Exchange adapter certification: the catalogue of checks, the runner and the register (Section H, Step 4.2).

**What a certification is, and is not.** It is evidence that one adapter, at one version, did what the
broker-neutral contract promises against one venue environment: it resolved the account, read the rules,
placed, found, filled, protected, replaced, cancelled, recovered and flattened. It says nothing about any
strategy. No order of a certification run comes from a family decision or a TradePlan, none is counted as
strategy performance, and every one carries the client order id prefix ``cert`` so it can never be mistaken
for a strategy trade.

**Who may run one.** Nobody by default. The runner needs an ``OperatorPermit``: a named operator, a reference
to the owner's authorisation, and exactly one venue, environment, account reference and symbol with a bound
on the notional it may hold. The runner refuses when the permit does not match the adapter and the account
it was given, refuses LIVE entirely in this build, sends no order unless the permit says orders are allowed,
and sends none when the account already has a position or an order on the test symbol: existing positions
are never touched. Every mutation is written to the database before it is sent and again when it returns,
and whatever happens the run ends by cancelling its own orders and closing only what its own orders opened.

**The register.** Append-only and hash-chained. ``certification_status`` answers one of NOT_CERTIFIED,
CERTIFIED, EXPIRED, REVOKED, FAILED, BLOCKED, and only CERTIFIED is certified:

* no evidence, or only incomplete runs                       -> NOT_CERTIFIED
* a run with any failed check                                -> FAILED
* a complete green run older than 90 days                    -> EXPIRED
* a recorded change of the adapter or contract version       -> EXPIRED (revalidation required)
* a caller whose adapter is not the certified version        -> EXPIRED
* an operation the venue lacks or no check exercised         -> not selectable for that operation

A run is certified by the register from the checks it recorded, never from what a caller says about them.
Nothing in this module wires the answer into the execution boundary; ``adapter_certified`` is the function
that boundary calls.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Tuple, runtime_checkable

from app.exchange.platform_adapter import contract as C
from app.exchange.platform_adapter import rules_cache as RC
from app.exchange.platform_adapter.user_stream import AUTH_FRAME, LISTEN_KEY_IN_URL
from app.trading_intelligence.hashing import short_id, stable_hash

from shared_lib.persistence.cati_exchange_schema import CERT_ACTION_TABLE, CERT_CHECK_TABLE, CERT_EVENT_TABLE

CERTIFICATION_FRAMEWORK_VERSION = "1.0.0"
CERTIFICATION_VALIDITY_DAYS = 90
CERTIFICATION_VALIDITY_MS = CERTIFICATION_VALIDITY_DAYS * 86_400_000
#: a certificate stamped later than ``now`` by more than this is a clock fault, not a certificate
CLOCK_TOLERANCE_MS = 120_000
#: every certification order begins with this; no strategy order does
CERTIFICATION_ORDER_PREFIX = "cert"

NOT_CERTIFIED, CERTIFIED, EXPIRED, REVOKED, FAILED, BLOCKED = (
    "NOT_CERTIFIED", "CERTIFIED", "EXPIRED", "REVOKED", "FAILED", "BLOCKED")
STATUSES: Tuple[str, ...] = (NOT_CERTIFIED, CERTIFIED, EXPIRED, REVOKED, FAILED, BLOCKED)
_DECISIVE: Tuple[str, ...] = (CERTIFIED, FAILED, REVOKED, BLOCKED)

PASSED, INCOMPLETE = "PASSED", "INCOMPLETE"             # run outcomes, with FAILED
RUN_STARTED, RUN_COMPLETED, VERSION_CHANGED = "RUN_STARTED", "RUN_COMPLETED", "VERSION_CHANGED"
PASS, FAIL, NOT_RUN, NOT_SUPPORTED = "PASS", "FAIL", "NOT_RUN", "NOT_SUPPORTED"

READ, ORDER, FAULT, CLEANUP = "READ", "ORDER", "FAULT", "CLEANUP"
CORE = "CORE"
FAULT_INJECTOR, ADAPTER_FACTORY = "FAULT_INJECTOR", "ADAPTER_FACTORY"
_GENESIS = "0" * 64
_EVENT_FIELDS: Tuple[str, ...] = (
    "event_id", "venue", "environment", "event_type", "status", "run_id", "adapter_version", "contract_version",
    "operator_ref", "account_ref", "authorized_scope", "started_at", "completed_at", "expires_at", "source_commit",
    "recorded_at", "document")


class CertificationRefused(RuntimeError):
    """The run, or the record, is not permitted. Nothing was sent to a venue."""


@dataclass(frozen=True)
class CheckSpec:
    check_id: str
    title: str
    phase: str                          # READ | ORDER | FAULT | CLEANUP
    scope: str                          # CORE, or the one operation the check certifies
    assignment_tests: Tuple[int, ...]   # the owner's numbered required tests this check covers
    needs: str = ""                     # FAULT_INJECTOR | ADAPTER_FACTORY: what a harness must supply


CATALOGUE: Tuple[CheckSpec, ...] = (
    CheckSpec("ACCOUNT_ENVIRONMENT", "The adapter is bound to the permitted venue, environment and account",
              READ, CORE, (1,)),
    CheckSpec("TIME_SYNC", "The local clock is within tolerance of the venue's", READ, CORE, (16,)),
    CheckSpec("INSTRUMENT_RULES", "The venue reports complete rules for a trading instrument", READ, CORE, (2,)),
    CheckSpec("ACCOUNT_STATE", "Balance, position and open orders can be read", READ, CORE, (2,)),
    CheckSpec("RATE_LIMIT_OBSERVATION", "The adapter reports what the venue said about its budget",
              READ, CORE, (15,)),
    CheckSpec("USER_STREAM_SPEC", "An authenticated stream specification can be obtained", READ, "USER_STREAM", ()),
    CheckSpec("TEST_SCOPE", "The account is flat on the test symbol and the test fits the permit", ORDER, CORE, ()),
    CheckSpec("LEVERAGE_AND_MODE", "One-way mode and the permitted leverage are in force", ORDER, CORE, (2,)),
    CheckSpec("VENUE_REJECTION", "A rejection by the venue arrives as a definite, mapped refusal",
              ORDER, CORE, (14,)),
    CheckSpec("ENTRY_SUBMIT", "An authorised minimum-size entry is accepted", ORDER, CORE, (3,)),
    CheckSpec("ORDER_LOOKUP", "The order is found by its client and by its venue order id", ORDER, CORE, (3,)),
    CheckSpec("ENTRY_FILL", "The fill, full or partial, agrees between order, fills and position",
              ORDER, CORE, (4,)),
    CheckSpec("IDEMPOTENT_REPEAT", "Repeating a client order id never creates a second order", ORDER, CORE, (3,)),
    CheckSpec("PROTECTION_CREATE", "A venue-held protective stop is placed", ORDER, CORE, (5,)),
    CheckSpec("PROTECTION_READ_BACK", "The protective stop is read back from the venue", ORDER, CORE, (6,)),
    CheckSpec("PROTECTION_GEOMETRY", "Side, trigger, quantity and reduce-only of the stop are correct",
              ORDER, CORE, (7,)),
    CheckSpec("PROTECTION_REPLACE", "The stop is replaced by a tighter one and exactly one remains",
              ORDER, CORE, (5, 6)),
    CheckSpec("UNKNOWN_SUBMIT_RECONCILE", "An unknown submission is settled by its client order id",
              ORDER, CORE, (8,)),
    CheckSpec("CANCEL", "A resting order is cancelled, stays cancelled, and a repeat changes nothing",
              ORDER, CORE, (9,)),
    CheckSpec("LOST_ACKNOWLEDGMENT", "An accepted order whose answer was lost is recovered, not resent",
              FAULT, CORE, (10,), FAULT_INJECTOR),
    CheckSpec("CONNECTION_INTERRUPTION", "An interrupted request is settled and the connection recovers",
              FAULT, CORE, (11,), FAULT_INJECTOR),
    CheckSpec("RATE_LIMIT", "A rate refusal carries the venue's wait and is not retried",
              FAULT, CORE, (15,), FAULT_INJECTOR),
    CheckSpec("CLOCK_DRIFT", "A drifted clock is reported as such and recovers", FAULT, CORE, (16,), FAULT_INJECTOR),
    CheckSpec("RESTART_WITH_PROTECTED_POSITION", "A fresh adapter finds the position and its stop",
              ORDER, CORE, (12,), ADAPTER_FACTORY),
    CheckSpec("MANUAL_POSITION_CHANGE", "A position changed at the venue is seen and adopted",
              FAULT, CORE, (13,), FAULT_INJECTOR),
    CheckSpec("CLOSE_POSITION", "The position is closed reduce-only", ORDER, CORE, (17,)),
    CheckSpec("CANCEL_OBSOLETE_PROTECTION", "The stop of the closed position is removed", ORDER, CORE, (18,)),
    CheckSpec("FLATTEN", "Every order of the run is closed and only its own exposure is flattened",
              CLEANUP, CORE, (19,)),
    CheckSpec("RECONCILIATION", "The fills of the run explain the venue's position exactly", CLEANUP, CORE, (20,)),
    CheckSpec("FINAL_ACCOUNT_STATE", "The account is flat and without orders on the test symbol",
              CLEANUP, CORE, (20,)),
)
CHECK_IDS: Tuple[str, ...] = tuple(spec.check_id for spec in CATALOGUE)

#: operation -> (capability the venue must have, checks that must have passed). An operation is selectable
#: under a certification only when both hold.
OPERATIONS: Mapping[str, Tuple[Optional[str], Tuple[str, ...]]] = {
    "ENTRY": (None, ("ENTRY_SUBMIT", "ORDER_LOOKUP", "ENTRY_FILL", "IDEMPOTENT_REPEAT")),
    "PROTECTIVE_STOP": ("native_stop_market", ("PROTECTION_CREATE", "PROTECTION_READ_BACK", "PROTECTION_GEOMETRY")),
    "STOP_REPLACEMENT": (None, ("PROTECTION_REPLACE",)),
    "CANCEL": (None, ("CANCEL",)),
    "CLOSE": ("reduce_only", ("CLOSE_POSITION", "FLATTEN")),
    "USER_STREAM": ("user_stream", ("USER_STREAM_SPEC",)),
}


def catalogue(venue: str, environment: str) -> Tuple[CheckSpec, ...]:
    """The checks of a venue and environment. The catalogue is the same for every venue on purpose: the
    contract is broker-neutral, so an adapter is held to the same list wherever it trades."""
    if venue not in C.VENUES or environment not in C.ENVIRONMENTS:
        raise ValueError(f"unknown venue or environment: {venue!r}, {environment!r}")
    return CATALOGUE


def is_certification_order(client_order_id: Optional[str]) -> bool:
    """Was this order sent by a certification run? (Venues may fold the case of a client order id.)"""
    return str(client_order_id or "").lower().startswith(CERTIFICATION_ORDER_PREFIX)


# -------------------------------------------------------------------------------------------------- permit
@dataclass(frozen=True)
class OperatorPermit:
    """What the owner authorised one named operator to do. It carries references, never a credential."""
    operator_ref: str
    owner_authorization_ref: str
    venue: str
    environment: str
    account_ref: str
    authorized_scope: str
    symbol: str
    max_notional: float                 # quote units the run may hold on the symbol at any moment
    issued_at_ms: int
    expires_at_ms: int
    allow_orders: bool = False
    leverage: int = 1

    def __post_init__(self) -> None:
        for name in ("operator_ref", "owner_authorization_ref", "account_ref", "authorized_scope"):
            if not str(getattr(self, name) or "").strip():
                raise C.ContractViolation(f"a permit names its {name}")
        if self.venue not in C.VENUES or self.environment not in C.ENVIRONMENTS:
            raise C.ContractViolation("a permit names one known venue and environment")
        if C.canonical_symbol(self.symbol) != self.symbol:
            raise C.ContractViolation(f"symbol {self.symbol!r} is not canonical")
        if not (float(self.max_notional) > 0 and float(self.max_notional) < float("inf")):
            raise C.ContractViolation("a permit bounds the notional")
        if int(self.expires_at_ms) <= int(self.issued_at_ms) or int(self.leverage) < 1:
            raise C.ContractViolation("a permit has a validity period and a leverage of at least 1")


def permit_refusal(permit: Any, adapter: Any, account_ref: str, now_ms: int) -> Optional[str]:
    """Why this permit does not allow a run with this adapter on this account now (``None`` when it does)."""
    if not isinstance(permit, OperatorPermit):
        return "OPERATOR_PERMIT_REQUIRED"
    if not isinstance(adapter, C.VenueAdapter):
        return "NOT_A_VENUE_ADAPTER"
    if permit.venue != adapter.venue:
        return "PERMIT_VENUE_MISMATCH"
    if permit.environment != adapter.environment:
        return "PERMIT_ENVIRONMENT_MISMATCH"
    if adapter.environment == C.LIVE:
        return "LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD"
    if not str(account_ref or "").strip() or permit.account_ref != str(account_ref):
        return "PERMIT_ACCOUNT_MISMATCH"
    if int(now_ms) < int(permit.issued_at_ms):
        return "PERMIT_NOT_YET_VALID"
    if int(now_ms) >= int(permit.expires_at_ms):
        return "PERMIT_EXPIRED"
    return None


@runtime_checkable
class FaultInjector(Protocol):
    """What a certification harness supplies so the faults a venue cannot be asked to produce can be made
    on the client side of the adapter's transport. Each call arms exactly one fault."""

    def lose_next_acknowledgment(self) -> None: ...          # the next mutation reaches the venue; its answer is lost
    def interrupt_next_request(self) -> None: ...            # the next request does not reach the venue
    def rate_limit_next_request(self, retry_after_s: float) -> None: ...
    def skew_clock(self, offset_ms: int) -> None: ...        # 0 restores the clock
    def reduce_position_externally(self, symbol: str, quantity: float) -> None: ...   # outside this adapter


# ------------------------------------------------------------------------------------------------ register
@dataclass(frozen=True)
class CertificationStatus:
    venue: str
    environment: str
    status: str
    reason: str = ""
    run_id: Optional[str] = None
    adapter_version: Optional[str] = None
    contract_version: Optional[str] = None
    completed_at_ms: Optional[int] = None
    expires_at_ms: Optional[int] = None
    operations: Tuple[str, ...] = ()
    capabilities: Mapping[str, Any] = field(default_factory=dict)

    @property
    def certified(self) -> bool:
        return self.status == CERTIFIED


@dataclass(frozen=True)
class CertificationResult:
    run_id: str
    venue: str
    environment: str
    outcome: str                        # PASSED | FAILED | INCOMPLETE
    recorded_status: Optional[str]      # CERTIFIED | FAILED, or None for a run that decides nothing
    checks: Mapping[str, Mapping[str, str]]
    failure_reasons: Tuple[str, ...]
    operations: Tuple[str, ...]
    expires_at_ms: Optional[int]
    cleanup_complete: bool


def _text_hash(text: str) -> str:
    return stable_hash(text)


class CertificationRegister:
    def __init__(self, db: Any) -> None:
        self.db = db

    # --------------------------------------------------------------------------------------------- writing
    def _append(self, *, venue: str, environment: str, event_type: str, operator_ref: str, recorded_at: int,
                status: Optional[str] = None, run_id: Optional[str] = None, adapter_version: Optional[str] = None,
                contract_version: Optional[str] = None, account_ref: Optional[str] = None,
                authorized_scope: Optional[str] = None, started_at: Optional[int] = None,
                completed_at: Optional[int] = None, expires_at: Optional[int] = None,
                source_commit: Optional[str] = None, document: Optional[Mapping[str, Any]] = None,
                unless_run_exists: bool = False) -> str:
        if venue not in C.VENUES or environment not in C.ENVIRONMENTS:
            raise ValueError(f"unknown venue or environment: {venue!r}, {environment!r}")
        if not str(operator_ref or "").strip():
            raise CertificationRefused("OPERATOR_REQUIRED")
        if status == CERTIFIED and environment == C.LIVE:
            raise CertificationRefused("LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD")
        values: Dict[str, Any] = dict(
            venue=venue, environment=environment, event_type=event_type, status=status, run_id=run_id,
            adapter_version=adapter_version, contract_version=contract_version, operator_ref=str(operator_ref),
            account_ref=account_ref, authorized_scope=authorized_scope, started_at=started_at,
            completed_at=completed_at, expires_at=expires_at, source_commit=source_commit,
            recorded_at=int(recorded_at), document=json.dumps(dict(document or {}), sort_keys=True, default=str))
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if unless_run_exists and conn.execute(
                    f"SELECT 1 FROM {CERT_EVENT_TABLE} WHERE run_id=? AND event_type=?", (run_id, event_type)).fetchone():
                raise CertificationRefused("RUN_ALREADY_RECORDED")
            last = conn.execute(f"SELECT seq, event_hash FROM {CERT_EVENT_TABLE} ORDER BY seq DESC LIMIT 1").fetchone()
            seq, prev_hash = (1, _GENESIS) if last is None else (int(last[0]) + 1, last[1])
            values["event_id"] = short_id("certev", [seq, venue, environment, event_type, run_id, int(recorded_at)])
            event_hash = stable_hash({"prev_hash": prev_hash, **{name: values[name] for name in _EVENT_FIELDS}})
            conn.execute(
                f"INSERT INTO {CERT_EVENT_TABLE} (seq, {', '.join(_EVENT_FIELDS)}, prev_hash, event_hash) "
                f"VALUES ({', '.join('?' * (len(_EVENT_FIELDS) + 3))})",
                (seq, *(values[name] for name in _EVENT_FIELDS), prev_hash, event_hash))
        return str(values["event_id"])

    def start_run(self, permit: OperatorPermit, *, adapter_version: str, source_commit: str, started_at: int) -> str:
        """Record that a run began, before anything is asked of the venue. Returns the run id."""
        if not str(source_commit or "").strip():
            raise CertificationRefused("SOURCE_COMMIT_REQUIRED")
        run_id = short_id("cert", [permit.venue, permit.environment, permit.account_ref, permit.operator_ref,
                                   int(started_at), adapter_version])
        self._append(
            venue=permit.venue, environment=permit.environment, event_type=RUN_STARTED, operator_ref=permit.operator_ref,
            recorded_at=started_at, run_id=run_id, adapter_version=adapter_version, contract_version=C.CONTRACT_VERSION,
            account_ref=permit.account_ref, authorized_scope=permit.authorized_scope, started_at=int(started_at),
            source_commit=str(source_commit), unless_run_exists=True,
            document={"owner_authorization_ref": permit.owner_authorization_ref, "symbol": permit.symbol,
                      "max_notional": permit.max_notional, "allow_orders": permit.allow_orders,
                      "leverage": permit.leverage, "permit_expires_at_ms": permit.expires_at_ms,
                      "framework_version": CERTIFICATION_FRAMEWORK_VERSION})
        return run_id

    def record_check(self, run_id: str, check_id: str, status: str, reason: str, evidence: Mapping[str, Any],
                     started_at: int, completed_at: int) -> None:
        if check_id not in CHECK_IDS or status not in (PASS, FAIL, NOT_RUN, NOT_SUPPORTED):
            raise ValueError(f"unknown check or status: {check_id!r}, {status!r}")
        text = json.dumps(dict(evidence), sort_keys=True, default=str)
        with self.db.connect() as conn:
            conn.execute(
                f"INSERT INTO {CERT_CHECK_TABLE} (run_id, check_id, status, reason, started_at, completed_at, evidence, "
                "evidence_hash) VALUES (?,?,?,?,?,?,?,?)",
                (run_id, check_id, status, str(reason), int(started_at), int(completed_at), text, _text_hash(text)))

    def record_action(self, run_id: str, *, phase: str, action: str, symbol: str, client_order_id: Optional[str],
                      outcome: Optional[str], document: Mapping[str, Any], recorded_at: int) -> int:
        """One row of the audit trail of a run's mutations: ``INTENT`` before the request, ``OUTCOME`` after."""
        text = json.dumps(dict(document), sort_keys=True, default=str)
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            seq = int(conn.execute(f"SELECT COALESCE(MAX(seq), 0) + 1 FROM {CERT_ACTION_TABLE} WHERE run_id=?",
                                   (run_id,)).fetchone()[0])
            conn.execute(
                f"INSERT INTO {CERT_ACTION_TABLE} (run_id, seq, phase, action, symbol, client_order_id, outcome, "
                "document, recorded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (run_id, seq, phase, action, symbol, client_order_id, outcome, text, int(recorded_at)))
        return seq

    def complete_run(self, run_id: str, *, completed_at: int, capabilities: Mapping[str, Any]) -> CertificationResult:
        """Close a run. The outcome is derived here from the recorded checks: any failed check fails the run,
        every core check passed certifies it for 90 days, anything else is incomplete and decides nothing."""
        with self.db.connect() as conn:
            started = conn.execute(f"SELECT * FROM {CERT_EVENT_TABLE} WHERE run_id=? AND event_type=?",
                                   (run_id, RUN_STARTED)).fetchone()
            rows = {row["check_id"]: row for row in conn.execute(
                f"SELECT * FROM {CERT_CHECK_TABLE} WHERE run_id=?", (run_id,)).fetchall()}
            actions = [dict(row) for row in conn.execute(
                f"SELECT * FROM {CERT_ACTION_TABLE} WHERE run_id=? ORDER BY seq", (run_id,)).fetchall()]
        if started is None:
            raise CertificationRefused("RUN_NOT_STARTED")
        checks = {spec.check_id: {"status": rows[spec.check_id]["status"] if spec.check_id in rows else NOT_RUN,
                                  "reason": rows[spec.check_id]["reason"] if spec.check_id in rows else "NOT_RECORDED"}
                  for spec in CATALOGUE}
        failed = [f"{check_id}:{entry['reason']}" for check_id, entry in checks.items() if entry["status"] == FAIL]
        missing = [f"{spec.check_id}:{checks[spec.check_id]['status']}:{checks[spec.check_id]['reason']}"
                   for spec in CATALOGUE if spec.scope == CORE and checks[spec.check_id]["status"] not in (PASS, FAIL)]
        outcome = FAILED if failed else INCOMPLETE if missing else PASSED
        operations = tuple(sorted(
            name for name, (capability, needed) in OPERATIONS.items()
            if (capability is None or capabilities.get(capability) is True)
            and all(checks[check_id]["status"] == PASS for check_id in needed))) if outcome == PASSED else ()
        recorded_status = {PASSED: CERTIFIED, FAILED: FAILED}.get(outcome)
        expires_at = int(completed_at) + CERTIFICATION_VALIDITY_MS if outcome == PASSED else None
        orders_sent = any(action["client_order_id"] for action in actions)
        cleanup_complete = not orders_sent or checks["FINAL_ACCOUNT_STATE"]["status"] == PASS
        reasons = tuple(failed + missing + ([] if cleanup_complete else ["CLEANUP_INCOMPLETE_OPERATOR_ACTION_REQUIRED"]))
        self._append(
            venue=started["venue"], environment=started["environment"], event_type=RUN_COMPLETED,
            operator_ref=started["operator_ref"], recorded_at=completed_at, status=recorded_status, run_id=run_id,
            adapter_version=started["adapter_version"], contract_version=started["contract_version"],
            account_ref=started["account_ref"], authorized_scope=started["authorized_scope"],
            started_at=started["started_at"], completed_at=int(completed_at), expires_at=expires_at,
            source_commit=started["source_commit"], unless_run_exists=True,
            document={"outcome": outcome, "test_cases": checks, "failure_reasons": list(reasons),
                      "exchange_capabilities": dict(capabilities), "operations": list(operations),
                      "evidence_hashes": {check_id: rows[check_id]["evidence_hash"] for check_id in sorted(rows)},
                      "actions_hash": stable_hash(actions), "validity_days": CERTIFICATION_VALIDITY_DAYS,
                      "framework_version": CERTIFICATION_FRAMEWORK_VERSION})
        return CertificationResult(run_id, started["venue"], started["environment"], outcome, recorded_status, checks,
                                   reasons, operations, expires_at, cleanup_complete)

    def revoke(self, venue: str, environment: str, *, operator_ref: str, reason: str, now_ms: int) -> str:
        """Withdraw whatever certification stands. Only a new complete green run certifies again."""
        return self._append(venue=venue, environment=environment, event_type=REVOKED, operator_ref=operator_ref,
                            recorded_at=now_ms, status=REVOKED, document={"reason": str(reason)})

    def block(self, venue: str, environment: str, *, operator_ref: str, reason: str, now_ms: int) -> str:
        """Record that certification cannot be performed here (no safe environment, no authorised account)."""
        return self._append(venue=venue, environment=environment, event_type=BLOCKED, operator_ref=operator_ref,
                            recorded_at=now_ms, status=BLOCKED, document={"reason": str(reason)})

    def record_adapter_version(self, venue: str, environment: str, *, adapter_version: str, now_ms: int,
                               contract_version: str = C.CONTRACT_VERSION, operator_ref: str = "system") -> bool:
        """Tell the register which adapter and contract version is deployed. When that differs from the
        certified one a ``VERSION_CHANGED`` event is appended and the certification is expired from then on,
        whatever version is deployed later. Returns True when a change was recorded."""
        with self.db.connect() as conn:
            row = conn.execute(
                f"SELECT adapter_version, contract_version FROM {CERT_EVENT_TABLE} WHERE venue=? AND environment=? AND "
                "(status=? OR event_type=?) ORDER BY seq DESC LIMIT 1",
                (venue, environment, CERTIFIED, VERSION_CHANGED)).fetchone()
        if row is None or (row["adapter_version"], row["contract_version"]) == (adapter_version, contract_version):
            return False
        self._append(venue=venue, environment=environment, event_type=VERSION_CHANGED, operator_ref=operator_ref,
                     recorded_at=now_ms, adapter_version=adapter_version, contract_version=contract_version,
                     document={"previous_adapter_version": row["adapter_version"],
                               "previous_contract_version": row["contract_version"]})
        return True

    # --------------------------------------------------------------------------------------------- reading
    def verify_chain(self) -> Tuple[bool, str]:
        """Is every event still the one that was written, in the order it was written?"""
        with self.db.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {CERT_EVENT_TABLE} ORDER BY seq").fetchall()
        prev_hash = _GENESIS
        for position, row in enumerate(rows, start=1):
            expected = stable_hash({"prev_hash": prev_hash, **{name: row[name] for name in _EVENT_FIELDS}})
            if int(row["seq"]) != position or row["prev_hash"] != prev_hash or row["event_hash"] != expected:
                return False, f"EVENT_{row['seq']}"
            prev_hash = row["event_hash"]
        return True, ""

    def verify_run(self, run_id: str) -> bool:
        """Do the recorded checks and actions of a run still match the hashes its completion sealed?"""
        with self.db.connect() as conn:
            done = conn.execute(f"SELECT document FROM {CERT_EVENT_TABLE} WHERE run_id=? AND event_type=?",
                                (run_id, RUN_COMPLETED)).fetchone()
            rows = conn.execute(f"SELECT * FROM {CERT_CHECK_TABLE} WHERE run_id=?", (run_id,)).fetchall()
            actions = [dict(row) for row in conn.execute(
                f"SELECT * FROM {CERT_ACTION_TABLE} WHERE run_id=? ORDER BY seq", (run_id,)).fetchall()]
        if done is None:
            return False
        document = json.loads(done[0])
        sealed = document.get("evidence_hashes", {})
        return (document.get("actions_hash") == stable_hash(actions) and set(sealed) == {row["check_id"] for row in rows}
                and all(sealed[row["check_id"]] == row["evidence_hash"] == _text_hash(row["evidence"])
                        and document["test_cases"][row["check_id"]]["status"] == row["status"] for row in rows))

    def status(self, venue: str, environment: str, now_ms: int, *,
               adapter_version: Optional[str] = None) -> CertificationStatus:
        if venue not in C.VENUES or environment not in C.ENVIRONMENTS:
            raise ValueError(f"unknown venue or environment: {venue!r}, {environment!r}")
        intact, where = self.verify_chain()
        if not intact:
            return CertificationStatus(venue, environment, NOT_CERTIFIED, f"REGISTER_INTEGRITY_FAILED:{where}")
        with self.db.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {CERT_EVENT_TABLE} WHERE venue=? AND environment=? ORDER BY seq",
                                (venue, environment)).fetchall()
        decisive, version_changed, undecided_runs = None, False, 0
        for row in rows:
            if row["status"] in _DECISIVE:
                decisive, version_changed = row, False
            elif row["event_type"] == VERSION_CHANGED:
                version_changed = True
            elif row["event_type"] == RUN_COMPLETED:
                undecided_runs += 1
        if decisive is None:
            return CertificationStatus(venue, environment, NOT_CERTIFIED,
                                       "ONLY_INCOMPLETE_RUNS" if undecided_runs else "NO_CERTIFICATION_EVIDENCE")
        document = json.loads(decisive["document"])
        common = dict(run_id=decisive["run_id"], adapter_version=decisive["adapter_version"],
                      contract_version=decisive["contract_version"], completed_at_ms=decisive["completed_at"],
                      expires_at_ms=decisive["expires_at"], capabilities=document.get("exchange_capabilities", {}))
        if decisive["status"] != CERTIFIED:
            reason = document.get("reason") or ";".join(document.get("failure_reasons", [])) or decisive["status"]
            return CertificationStatus(venue, environment, decisive["status"], str(reason), **common)
        if not self.verify_run(decisive["run_id"]):
            return CertificationStatus(venue, environment, NOT_CERTIFIED, "RUN_EVIDENCE_INTEGRITY_FAILED", **common)
        if int(now_ms) < int(decisive["completed_at"]) - CLOCK_TOLERANCE_MS:
            return CertificationStatus(venue, environment, NOT_CERTIFIED, "CERTIFICATION_DATED_IN_THE_FUTURE", **common)
        expired = None
        if version_changed:
            expired = "ADAPTER_OR_CONTRACT_VERSION_CHANGED"
        elif decisive["contract_version"] != C.CONTRACT_VERSION:
            expired = "CONTRACT_VERSION_CHANGED"
        elif adapter_version is not None and adapter_version != decisive["adapter_version"]:
            expired = "ADAPTER_VERSION_MISMATCH"
        elif int(now_ms) >= int(decisive["expires_at"]):
            expired = f"CERTIFICATION_OLDER_THAN_{CERTIFICATION_VALIDITY_DAYS}_DAYS"
        if expired is not None:
            return CertificationStatus(venue, environment, EXPIRED, expired, **common)
        return CertificationStatus(venue, environment, CERTIFIED, "", operations=tuple(document.get("operations", ())),
                                   **common)

    def open_runs(self) -> Tuple[Dict[str, Any], ...]:
        """Runs that began and never completed: a process died inside them. Each may have left orders or a
        position at the venue and is closed with ``CertificationRunner.recover``."""
        with self.db.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM {CERT_EVENT_TABLE} s WHERE s.event_type=? AND NOT EXISTS (SELECT 1 FROM "
                f"{CERT_EVENT_TABLE} c WHERE c.run_id=s.run_id AND c.event_type=?) ORDER BY s.seq",
                (RUN_STARTED, RUN_COMPLETED)).fetchall()
        return tuple({**dict(row), "document": json.loads(row["document"])} for row in rows)

    def run_evidence(self, run_id: str) -> Dict[str, Any]:
        """Everything recorded for one run: its events, its checks with their evidence, its actions."""
        with self.db.connect() as conn:
            events = [dict(r) for r in conn.execute(
                f"SELECT * FROM {CERT_EVENT_TABLE} WHERE run_id=? ORDER BY seq", (run_id,)).fetchall()]
            checks = [dict(r) for r in conn.execute(
                f"SELECT * FROM {CERT_CHECK_TABLE} WHERE run_id=? ORDER BY started_at, rowid", (run_id,)).fetchall()]
            actions = [dict(r) for r in conn.execute(
                f"SELECT * FROM {CERT_ACTION_TABLE} WHERE run_id=? ORDER BY seq", (run_id,)).fetchall()]
        return {"events": events, "checks": checks, "actions": actions}


def certification_status(db: Any, venue: str, environment: str, now_ms: int, *,
                         adapter_version: Optional[str] = None) -> CertificationStatus:
    return CertificationRegister(db).status(venue, environment, now_ms, adapter_version=adapter_version)


def adapter_certified(db: Any, venue: str, environment: str, now_ms: int, *, adapter_version: Optional[str] = None,
                      operation: Optional[str] = None) -> Tuple[bool, str]:
    """``(True, "")`` only for complete, green, unexpired evidence of this venue and environment, for the
    deployed adapter version when the caller names it and for ``operation`` when it names one. Every other
    answer is ``(False, reason)``; an operation this framework does not exercise is never selectable."""
    status = certification_status(db, venue, environment, now_ms, adapter_version=adapter_version)
    if not status.certified:
        return False, f"ADAPTER_{status.status}:{status.reason}"
    if operation is not None:
        if operation not in OPERATIONS:
            return False, f"OPERATION_NOT_EXERCISED_BY_CERTIFICATION:{operation}"
        if operation not in status.operations:
            return False, f"OPERATION_NOT_CERTIFIED:{operation}"
    return True, ""


def certification_matrix(db: Any, now_ms: int) -> Dict[str, CertificationStatus]:
    """The status of every venue and environment, keyed ``venue/environment``."""
    register = CertificationRegister(db)
    return {f"{venue}/{environment}": register.status(venue, environment, now_ms)
            for venue in C.VENUES for environment in C.ENVIRONMENTS}


# -------------------------------------------------------------------------------------------------- runner
@dataclass(frozen=True)
class RunnerConfig:
    max_clock_offset_ms: int = 1_000
    stop_distance_fraction: float = 0.05            # the first stop sits this far under the entry
    resting_limit_offset_fraction: float = 0.05     # resting test orders sit this far under the mark
    price_buffer_fraction: float = 0.05             # the minimum size is computed for a price this much lower
    fill_poll_attempts: int = 10
    fill_poll_interval_s: float = 0.5
    rate_limit_retry_after_s: float = 2.0
    clock_skew_ms: int = 60_000


class _Fail(Exception):
    def __init__(self, reason: str, evidence: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(reason)
        self.reason, self.evidence = reason, dict(evidence or {})


class _Skip(Exception):
    def __init__(self, status: str, reason: str) -> None:
        super().__init__(reason)
        self.status, self.reason = status, reason


def _error_evidence(error: C.AdapterError) -> Dict[str, Any]:
    """What an adapter error says, without its free text."""
    return {"code": error.code, "uncertain": error.uncertain, "retryable": error.retryable,
            "http_status": error.http_status, "venue_code": error.venue_code, "retry_after_s": error.retry_after_s,
            "banned_until_ms": error.banned_until_ms}


def _same_id(left: Optional[str], right: Optional[str]) -> bool:
    return left is not None and right is not None and str(left).lower() == str(right).lower()


class CertificationRunner:
    """Runs the catalogue against one adapter under one permit and records the evidence."""

    def __init__(self, db: Any, adapter: Any, *, account_ref: str, permit: OperatorPermit, source_commit: str,
                 clock: Callable[[], int], sleep: Callable[[float], None] = time.sleep,
                 faults: Optional[FaultInjector] = None, adapter_factory: Optional[Callable[[], Any]] = None,
                 config: Optional[RunnerConfig] = None) -> None:
        self.db, self.adapter, self.account_ref, self.permit = db, adapter, account_ref, permit
        self.source_commit, self.clock, self.sleep = source_commit, clock, sleep
        self.faults, self.adapter_factory, self.config = faults, adapter_factory, config or RunnerConfig()
        self.register = CertificationRegister(db)
        self._run_id = ""
        self._results: Dict[str, str] = {}
        self._stopped_at: Optional[str] = None
        # client order id -> every venue order id seen for it. More than one is itself a finding (an adapter
        # that created a second order for a repeated id), and every one of them is flattened.
        self._orders: Dict[str, List[str]] = {}
        self._order_number = 0
        self._caps: Optional[C.AdapterCapabilities] = None
        self._rules: Optional[C.InstrumentRules] = None
        self._flat_at_start = False
        self._wallet_before: Optional[float] = None
        self._price = self._unit = self._entry_quantity = self._position_quantity = self._external_reduction = 0.0
        self._entry_price = self._stop_price = 0.0
        self._entry_request: Optional[C.OrderRequest] = None
        self._entry_order: Optional[C.OrderInfo] = None
        self._stop_cid = ""
        self._stop_order: Optional[C.OrderInfo] = None

    # ------------------------------------------------------------------------------------------------ run
    def run(self) -> CertificationResult:
        refusal = permit_refusal(self.permit, self.adapter, self.account_ref, self.clock())
        if refusal is not None:
            raise CertificationRefused(refusal)
        self._run_id = self.register.start_run(self.permit, adapter_version=self.adapter.adapter_version,
                                               source_commit=self.source_commit, started_at=self.clock())
        try:
            self._read_checks()
            self._order_checks()
        finally:
            self._cleanup()  # whatever happened above, the run's own orders and exposure are taken off
        return self.register.complete_run(self._run_id, completed_at=self.clock(),
                                          capabilities={} if self._caps is None else asdict(self._caps))

    @classmethod
    def recover(cls, db: Any, adapter: Any, *, run_id: str, account_ref: str, permit: OperatorPermit,
                clock: Callable[[], int], sleep: Callable[[float], None] = time.sleep) -> CertificationResult:
        """Roll back a run whose process died: cancel the orders its recorded intents name, close what those
        orders left open, verify the account and complete the run. It can never certify: the checks the dead
        process did not reach stay not run. The permit must cover the same venue, environment, account and
        symbol as the run, and LIVE is refused as everywhere else."""
        refusal = permit_refusal(permit, adapter, account_ref, clock())
        if refusal is not None:
            raise CertificationRefused(refusal)
        runner = cls(db, adapter, account_ref=account_ref, permit=permit, source_commit="recovery", clock=clock,
                     sleep=sleep)
        evidence = runner.register.run_evidence(run_id)
        started = [e for e in evidence["events"] if e["event_type"] == RUN_STARTED]
        if not started:
            raise CertificationRefused("RUN_NOT_STARTED")
        if len(evidence["events"]) != 1:
            raise CertificationRefused("RUN_ALREADY_RECORDED")
        covered = (started[0]["venue"], started[0]["environment"], started[0]["account_ref"],
                   json.loads(started[0]["document"]).get("symbol"))
        if covered != (permit.venue, permit.environment, permit.account_ref, permit.symbol):
            raise CertificationRefused("PERMIT_DOES_NOT_COVER_THIS_RUN")
        runner._run_id = run_id
        runner._results = {check["check_id"]: check["status"] for check in evidence["checks"]}
        for action in evidence["actions"]:
            if action["client_order_id"]:
                runner._learn(action["client_order_id"], json.loads(action["document"]).get("venue_order_id"))
        runner._order_number = len(runner._orders) + 500  # ids of the recovery never repeat one the run used
        try:
            runner._caps = adapter.capabilities()
            runner._rules = adapter.instrument_rules([permit.symbol]).get(permit.symbol)
        except C.AdapterError:
            runner._rules = None  # the cleanup checks then record that nothing could be established
        if runner._orders and runner._results.get("FLATTEN") not in (None, PASS):
            # the dead process recorded a flatten that did not succeed: try again before anything is concluded
            try:
                runner._flatten()
            except (C.AdapterError, _Fail, _Skip) as failure:
                raise CertificationRefused(f"RECOVERY_FLATTEN_FAILED:{type(failure).__name__}") from None
        runner._cleanup()
        return runner.register.complete_run(run_id, completed_at=clock(),
                                            capabilities={} if runner._caps is None else asdict(runner._caps))

    def _check(self, check_id: str, body: Callable[[], Optional[Mapping[str, Any]]]) -> bool:
        started = self.clock()
        status, reason, evidence = PASS, "", {}
        try:
            evidence = dict(body() or {})
        except _Skip as skipped:
            status, reason = skipped.status, skipped.reason
        except _Fail as failure:
            status, reason, evidence = FAIL, failure.reason, failure.evidence
        except C.AdapterError as error:
            status, reason, evidence = FAIL, f"ADAPTER_ERROR:{error.code}", _error_evidence(error)
        except (C.ContractViolation, RC.RulesRefused) as refusal:
            status, reason, evidence = FAIL, type(refusal).__name__, {"detail": str(refusal)[:200]}
        except Exception as unexpected:  # a fault in an adapter is evidence against it, and cleanup must still run
            status, reason = FAIL, f"UNEXPECTED_ERROR:{type(unexpected).__name__}"
        self.register.record_check(self._run_id, check_id, status, reason, evidence, started, self.clock())
        self._results[check_id] = status
        return status == PASS

    def _step(self, check_id: str, body: Callable[[], Optional[Mapping[str, Any]]], *, gate: bool = False) -> None:
        """One check of the order sequence. A failed check stops the sequence; so does a ``gate`` that did
        not pass. What is not reached is recorded as not run, with the check that stopped it."""
        if self._stopped_at is not None:
            now = self.clock()
            self.register.record_check(self._run_id, check_id, NOT_RUN, f"NOT_REACHED:{self._stopped_at}", {}, now, now)
            self._results[check_id] = NOT_RUN
            return
        passed = self._check(check_id, body)
        if self._results[check_id] == FAIL or (gate and not passed):
            self._stopped_at = check_id

    # ----------------------------------------------------------------------------------------- venue calls
    @property
    def _symbol(self) -> str:
        return self.permit.symbol

    def _next_id(self) -> str:
        self._order_number += 1
        return f"{CERTIFICATION_ORDER_PREFIX}{stable_hash(self._run_id)[:9]}{self._order_number:03d}"

    def _mutate(self, action: str, client_order_id: Optional[str], document: Mapping[str, Any],
                call: Callable[[], Any]) -> Any:
        """Send one mutation with its intent recorded before and its outcome after."""
        log = dict(run_id=self._run_id, action=action, symbol=self._symbol, client_order_id=client_order_id)
        self.register.record_action(phase="INTENT", outcome=None, document=document, recorded_at=self.clock(), **log)
        if client_order_id is not None:
            self._orders.setdefault(client_order_id, [])
        try:
            result = call()
        except C.AdapterError as error:
            self.register.record_action(phase="OUTCOME", outcome="UNCERTAIN" if error.uncertain else "REFUSED",
                                        document=_error_evidence(error), recorded_at=self.clock(), **log)
            raise
        answer = ({"venue_order_id": result.venue_order_id, "status": result.status}
                  if isinstance(result, C.OrderInfo) else {"result": result})
        self.register.record_action(phase="OUTCOME", outcome="ANSWERED", document=answer, recorded_at=self.clock(), **log)
        if isinstance(result, C.OrderInfo) and client_order_id is not None:
            self._learn(client_order_id, result.venue_order_id)
        return result

    def _learn(self, client_order_id: str, venue_order_id: Optional[str]) -> None:
        known = self._orders.setdefault(client_order_id, [])
        if venue_order_id and venue_order_id not in known:
            known.append(venue_order_id)

    def _place(self, request: C.OrderRequest) -> C.OrderInfo:
        return self._mutate(
            "PLACE_ORDER", request.client_order_id,
            {"side": request.side, "order_type": request.order_type, "quantity": request.quantity,
             "price": request.price, "stop_price": request.stop_price, "reduce_only": request.reduce_only},
            lambda: self.adapter.place_order(request))

    def _cancel(self, client_order_id: str) -> C.OrderInfo:
        return self._mutate("CANCEL_ORDER", client_order_id, {},
                            lambda: self.adapter.cancel_order(self._symbol, client_order_id=client_order_id))

    def _position(self, adapter: Optional[Any] = None) -> C.PositionInfo:
        found = [p for p in (adapter or self.adapter).positions(self._symbol) if p.symbol == self._symbol]
        if len(found) > 1:
            raise _Fail("MORE_THAN_ONE_POSITION_ON_SYMBOL", {"count": len(found)})
        return found[0] if found else C.PositionInfo(self.adapter.venue, self._symbol, C.FLAT, 0.0, None, None, None,
                                                     None, None, None, self.clock())

    def _order_or_none(self, client_order_id: str) -> Optional[C.OrderInfo]:
        try:
            return self.adapter.order(self._symbol, client_order_id=client_order_id)
        except C.AdapterError as error:
            if error.code != C.ORDER_NOT_FOUND:
                raise
            return None

    def _mark(self) -> float:
        price = self.adapter.funding(self._symbol).mark_price
        if price is None or not float(price) > 0:
            raise _Skip(NOT_RUN, "NO_REFERENCE_PRICE")
        return float(price)

    def _near(self, left: Optional[float], right: Optional[float], unit: Optional[float]) -> bool:
        return left is not None and right is not None and unit is not None and abs(float(left) - float(right)) <= unit / 2

    def _same_quantity(self, left: Optional[float], right: Optional[float]) -> bool:
        return self._near(left, right, None if self._rules is None else self._rules.quantity_step)

    def _bounded(self, quantity: float) -> None:
        """Refuse an order that could take the position on the test symbol beyond the permit."""
        held = self._position().quantity
        if (held + quantity) * self._price > float(self.permit.max_notional):
            raise _Skip(NOT_RUN, "ORDER_WOULD_EXCEED_PERMIT_NOTIONAL")

    def _await_final(self, client_order_id: str) -> C.OrderInfo:
        info = self.adapter.order(self._symbol, client_order_id=client_order_id)
        for _ in range(self.config.fill_poll_attempts):
            if info.final:
                break
            self.sleep(self.config.fill_poll_interval_s)
            info = self.adapter.order(self._symbol, client_order_id=client_order_id)
        return info

    def _resting_limit(self) -> C.OrderRequest:
        rules = self._require_rules()
        price = RC.quantize_limit_price(rules, C.BUY, self._mark() * (1 - self.config.resting_limit_offset_fraction))
        quantity = RC.minimum_order_quantity(rules, price)
        self._bounded(quantity)
        return C.OrderRequest(self._symbol, C.BUY, C.LIMIT, self._next_id(), quantity=quantity, price=price)

    def _stop_request(self, stop_price: float) -> C.OrderRequest:
        return C.OrderRequest(self._symbol, C.SELL, C.STOP_MARKET, self._next_id(), quantity=self._position_quantity,
                              stop_price=stop_price, reduce_only=True)

    def _require_rules(self) -> C.InstrumentRules:
        if self._rules is None:
            raise _Skip(NOT_RUN, "INSTRUMENT_RULES_NOT_ESTABLISHED")
        return self._rules

    def _require_faults(self) -> FaultInjector:
        if self.faults is None:
            raise _Skip(NOT_RUN, "FAULT_INJECTOR_REQUIRED")
        return self.faults

    def _require_caps(self) -> C.AdapterCapabilities:
        if self._caps is None:
            raise _Skip(NOT_RUN, "CAPABILITIES_NOT_ESTABLISHED")
        return self._caps

    def _entry(self) -> Tuple[C.OrderRequest, C.OrderInfo]:
        if self._entry_request is None or self._entry_order is None:
            raise _Skip(NOT_RUN, "ENTRY_NOT_ESTABLISHED")
        return self._entry_request, self._entry_order

    # ------------------------------------------------------------------------------------------ read checks
    def _read_checks(self) -> None:
        self._check("ACCOUNT_ENVIRONMENT", self._account_environment)
        self._check("TIME_SYNC", self._time_sync)
        self._check("INSTRUMENT_RULES", self._instrument_rules)
        self._check("ACCOUNT_STATE", self._account_state)
        self._check("RATE_LIMIT_OBSERVATION", self._rate_limit_observation)
        self._check("USER_STREAM_SPEC", self._user_stream_spec)

    def _account_environment(self) -> Mapping[str, Any]:
        caps = self.adapter.capabilities()
        if (caps.venue, caps.environment) != (self.permit.venue, self.permit.environment):
            raise _Fail("CAPABILITIES_DO_NOT_MATCH_PERMIT", {"venue": caps.venue, "environment": caps.environment})
        self._caps = caps
        return {"venue": caps.venue, "environment": caps.environment, "account_ref": self.account_ref,
                "adapter_version": self.adapter.adapter_version, "contract_version": C.CONTRACT_VERSION,
                "capabilities": asdict(caps)}

    def _time_sync(self) -> Mapping[str, Any]:
        offset = self.adapter.server_time().offset_ms
        evidence = {"offset_ms": offset, "limit_ms": self.config.max_clock_offset_ms}
        if abs(offset) > self.config.max_clock_offset_ms:
            raise _Fail("CLOCK_OFFSET_EXCEEDS_LIMIT", evidence)
        return evidence

    def _instrument_rules(self) -> Mapping[str, Any]:
        rules = self.adapter.instrument_rules([self._symbol]).get(self._symbol)
        if rules is None:
            raise _Fail("INSTRUMENT_UNKNOWN")
        evidence = {"rules_hash": RC.rules_hash(rules), **{name: getattr(rules, name) for name in RC.RULE_FIELDS}}
        if (rules.venue, rules.environment, rules.symbol) != (self.permit.venue, self.permit.environment, self._symbol):
            raise _Fail("RULES_IDENTITY_MISMATCH", evidence)
        if not rules.complete:
            raise _Fail(RC.RULES_INCOMPLETE, evidence)
        if rules.status != C.TRADING:
            raise _Fail(RC.RULES_INSTRUMENT_NOT_TRADING, evidence)
        self._rules = rules
        return evidence

    def _account_state(self) -> Mapping[str, Any]:
        balance, position = self.adapter.balance(), self._position()
        orders = self.adapter.open_orders(self._symbol)
        self._wallet_before = float(balance.wallet_balance)
        self._flat_at_start = position.side == C.FLAT and not orders
        return {"margin_asset": balance.margin_asset, "wallet_balance": balance.wallet_balance,
                "available_balance": balance.available_balance, "position_side": position.side,
                "position_quantity": position.quantity, "position_mode": position.position_mode,
                "open_orders_on_symbol": len(orders)}

    def _rate_limit_observation(self) -> Mapping[str, Any]:
        observation = self.adapter.rate_limit()
        if observation.venue != self.permit.venue:
            raise _Fail("OBSERVATION_OF_ANOTHER_VENUE", {"venue": observation.venue})
        return asdict(observation)

    def _user_stream_spec(self) -> Mapping[str, Any]:
        if self._caps is None or not self._caps.user_stream:
            raise _Skip(NOT_SUPPORTED, "NO_USER_STREAM_IN_THIS_ENVIRONMENT")
        spec = self.adapter.user_stream()
        # the address and the subscribe frames can carry a key or a signature: neither becomes evidence
        evidence = {"auth_mode": spec.auth_mode, "keepalive_seconds": spec.keepalive_seconds,
                    "expires_after_seconds": spec.expires_after_seconds, "subscribe_frames": len(spec.subscribe_frames)}
        if (spec.venue != self.permit.venue or spec.auth_mode not in (LISTEN_KEY_IN_URL, AUTH_FRAME)
                or not str(spec.url).startswith("wss://") or int(spec.keepalive_seconds) <= 0):
            raise _Fail("USER_STREAM_SPEC_INVALID", evidence)
        return evidence

    # ----------------------------------------------------------------------------------------- order checks
    def _order_checks(self) -> None:
        reads_passed = all(self._results.get(spec.check_id) == PASS for spec in CATALOGUE
                           if spec.phase == READ and spec.scope == CORE)
        if not self.permit.allow_orders:
            self._stopped_at = "ORDERS_NOT_ALLOWED_BY_PERMIT"
        elif not reads_passed:
            self._stopped_at = "READ_CHECKS_NOT_PASSED"
        self._step("TEST_SCOPE", self._test_scope, gate=True)
        self._step("LEVERAGE_AND_MODE", self._leverage_and_mode, gate=True)
        self._step("VENUE_REJECTION", self._venue_rejection)
        self._step("ENTRY_SUBMIT", self._entry_submit, gate=True)
        self._step("ORDER_LOOKUP", self._order_lookup)
        self._step("ENTRY_FILL", self._entry_fill, gate=True)
        self._step("IDEMPOTENT_REPEAT", self._idempotent_repeat)
        self._step("PROTECTION_CREATE", self._protection_create, gate=True)
        self._step("PROTECTION_READ_BACK", self._protection_read_back, gate=True)
        self._step("PROTECTION_GEOMETRY", self._protection_geometry)
        self._step("PROTECTION_REPLACE", self._protection_replace)
        self._step("UNKNOWN_SUBMIT_RECONCILE", self._unknown_submit_reconcile)
        self._step("CANCEL", self._cancel_check)
        self._step("LOST_ACKNOWLEDGMENT", self._lost_acknowledgment)
        self._step("CONNECTION_INTERRUPTION", self._connection_interruption)
        self._step("RATE_LIMIT", self._rate_limit)
        self._step("CLOCK_DRIFT", self._clock_drift)
        self._step("RESTART_WITH_PROTECTED_POSITION", self._restart)
        self._step("MANUAL_POSITION_CHANGE", self._manual_position_change)
        self._step("CLOSE_POSITION", self._close_position, gate=True)
        self._step("CANCEL_OBSOLETE_PROTECTION", self._cancel_obsolete_protection)

    def _test_scope(self) -> Mapping[str, Any]:
        rules = self._require_rules()
        if not self._flat_at_start:
            # existing positions are protected: an account that is not flat on the symbol is not used
            raise _Skip(NOT_RUN, "ACCOUNT_NOT_FLAT_ON_TEST_SYMBOL")
        if rules.max_leverage is not None and self.permit.leverage > rules.max_leverage:
            raise _Skip(NOT_RUN, RC.LEVERAGE_ABOVE_VENUE_MAXIMUM)
        self._price = self._mark()
        self._unit = RC.minimum_order_quantity(rules, self._price * (1 - self.config.price_buffer_fraction))
        # two units when a harness will reduce the position from outside, so that a position remains to close
        units = 2 if self.faults is not None else 1
        self._entry_quantity = RC.quantize_quantity(rules, self._unit * units)
        notional = self._entry_quantity * self._price
        evidence = {"reference_price": self._price, "unit_quantity": self._unit, "entry_quantity": self._entry_quantity,
                    "entry_notional": notional, "permit_max_notional": self.permit.max_notional}
        if 2 * notional > float(self.permit.max_notional):
            # the permit must hold the entry twice: a failed idempotency check is a second entry
            raise _Skip(NOT_RUN, "MINIMUM_TEST_SIZE_EXCEEDS_PERMIT")
        return evidence

    def _leverage_and_mode(self) -> Mapping[str, Any]:
        if not self._require_caps().one_way_mode:
            raise _Fail("ONE_WAY_MODE_NOT_AVAILABLE")
        if self._position().position_mode == "HEDGE":
            raise _Fail("ACCOUNT_IN_HEDGE_MODE")
        applied = self._mutate("SET_LEVERAGE", None, {"leverage": self.permit.leverage},
                               lambda: self.adapter.set_leverage(self._symbol, self.permit.leverage))
        if int(applied) != int(self.permit.leverage):
            raise _Fail("LEVERAGE_NOT_APPLIED", {"requested": self.permit.leverage, "applied": applied})
        return {"leverage": applied}

    def _venue_rejection(self) -> Mapping[str, Any]:
        # a reduce-only order on a flat account must be refused, and cannot open anything if it is not
        request = C.OrderRequest(self._symbol, C.SELL, C.MARKET, self._next_id(), quantity=self._unit, reduce_only=True)
        try:
            info = self._place(request)
        except C.AdapterError as error:
            if error.uncertain or error.retryable or error.code in (C.UNMAPPED_VENUE_ERROR, C.MALFORMED_RESPONSE):
                raise _Fail(f"REJECTION_NOT_DEFINITE:{error.code}", _error_evidence(error)) from None
            return {"refused_with": error.code, **_error_evidence(error)}
        if info.status in (C.REJECTED, C.CANCELED, C.EXPIRED) and not info.filled_quantity > 0:
            return {"refused_as_status": info.status}
        raise _Fail("REDUCE_ONLY_ACCEPTED_ON_FLAT_ACCOUNT", {"status": info.status, "filled": info.filled_quantity})

    def _entry_submit(self) -> Mapping[str, Any]:
        self._bounded(self._entry_quantity)
        self._entry_request = C.OrderRequest(self._symbol, C.BUY, C.MARKET, self._next_id(),
                                             quantity=self._entry_quantity)
        info = self._place(self._entry_request)
        evidence = {"client_order_id": self._entry_request.client_order_id, "venue_order_id": info.venue_order_id,
                    "status": info.status, "quantity": self._entry_quantity}
        if info.status not in (C.NEW, C.PARTIALLY_FILLED, C.FILLED):
            raise _Fail("ENTRY_NOT_ACCEPTED", evidence)
        if not _same_id(info.client_order_id, self._entry_request.client_order_id) or not info.venue_order_id:
            raise _Fail("ORDER_IDENTITY_NOT_RETURNED", evidence)
        self._entry_order = info
        return evidence

    def _order_lookup(self) -> Mapping[str, Any]:
        request, order = self._entry()
        by_client = self.adapter.order(self._symbol, client_order_id=request.client_order_id)
        by_venue = self.adapter.order(self._symbol, venue_order_id=order.venue_order_id)
        if (by_client.venue_order_id != order.venue_order_id
                or not _same_id(by_venue.client_order_id, request.client_order_id)):
            raise _Fail("LOOKUP_IDENTITY_MISMATCH", {"by_client": by_client.venue_order_id,
                                                     "by_venue": by_venue.client_order_id})
        return {"by_client_order_id": by_client.status, "by_venue_order_id": by_venue.status}

    def _entry_fill(self) -> Mapping[str, Any]:
        cid = self._entry()[0].client_order_id
        info = self._await_final(cid)
        if info.open and info.filled_quantity > 0:
            self._cancel(cid)  # a partly filled entry: the rest is withdrawn and the filled part is the position
            info = self.adapter.order(self._symbol, client_order_id=cid)
        if not info.filled_quantity > 0:
            raise _Fail("ENTRY_NOT_FILLED", {"status": info.status})
        fills = [f for f in self.adapter.fills(self._symbol, venue_order_id=info.venue_order_id) if f.side == C.BUY]
        position = self._position()
        evidence = {"status": info.status, "filled_quantity": info.filled_quantity, "average_price": info.average_price,
                    "partial": not self._same_quantity(info.filled_quantity, self._entry_quantity),
                    "trade_ids": [f.trade_id for f in fills], "position_side": position.side,
                    "position_quantity": position.quantity}
        if not self._same_quantity(sum(f.quantity for f in fills), info.filled_quantity):
            raise _Fail("FILLS_DO_NOT_SUM_TO_ORDER", evidence)
        if position.side != C.LONG or not self._same_quantity(position.quantity, info.filled_quantity):
            raise _Fail("POSITION_DOES_NOT_MATCH_FILLS", evidence)
        self._position_quantity = float(info.filled_quantity)
        self._entry_price = float(info.average_price or sum(f.price * f.quantity for f in fills) / info.filled_quantity)
        return evidence

    def _idempotent_repeat(self) -> Mapping[str, Any]:
        request, order = self._entry()
        self._bounded(self._entry_quantity)  # if the adapter is wrong a second entry exists: it must fit the permit
        try:
            again = self._place(request)
            outcome = "RETURNED_THE_EXISTING_ORDER"
            if again.venue_order_id != order.venue_order_id:
                raise _Fail("REPEAT_CREATED_ANOTHER_ORDER", {"first": order.venue_order_id,
                                                            "second": again.venue_order_id})
        except C.AdapterError as error:
            if error.code != C.DUPLICATE_CLIENT_ORDER_ID:
                raise
            outcome = "REFUSED_AS_DUPLICATE"
        held = self._position().quantity
        if not self._same_quantity(held, self._position_quantity):
            raise _Fail("REPEAT_CHANGED_THE_POSITION", {"before": self._position_quantity, "after": held})
        return {"outcome": outcome, "position_quantity": held}

    def _protection_create(self) -> Mapping[str, Any]:
        if not self._require_caps().native_stop_market:
            raise _Fail("NO_VENUE_HELD_PROTECTIVE_STOP")
        stop = RC.quantize_stop_price(self._require_rules(), C.LONG,
                                      self._entry_price * (1 - self.config.stop_distance_fraction))
        if not stop < self._mark():
            raise _Fail("STOP_NOT_BELOW_MARK", {"stop_price": stop})
        request = self._stop_request(stop)
        info = self._place(request)
        evidence = {"client_order_id": request.client_order_id, "stop_price": stop, "status": info.status,
                    "quantity": request.quantity}
        if not info.open:
            raise _Fail("STOP_NOT_RESTING", evidence)
        self._stop_cid, self._stop_price = request.client_order_id, stop
        return evidence

    def _protection_read_back(self) -> Mapping[str, Any]:
        info = self.adapter.order(self._symbol, client_order_id=self._stop_cid)
        listed = [o for o in self.adapter.open_orders(self._symbol) if _same_id(o.client_order_id, self._stop_cid)]
        if not info.open or len(listed) != 1:
            raise _Fail("STOP_NOT_READ_BACK", {"status": info.status, "listed": len(listed)})
        self._stop_order = info
        return {"status": info.status, "venue_order_id": info.venue_order_id}

    def _protection_geometry(self) -> Mapping[str, Any]:
        rules, info = self._require_rules(), self._stop_order
        if info is None:
            raise _Skip(NOT_RUN, "STOP_NOT_ESTABLISHED")
        mark = self._mark()
        wrong = []
        if info.side != C.SELL:
            wrong.append("SIDE")
        if info.order_type != C.STOP_MARKET:
            wrong.append("ORDER_TYPE")
        if not self._near(info.stop_price, self._stop_price, rules.price_tick):
            wrong.append("TRIGGER")
        elif not (float(info.stop_price or 0) < mark and float(info.stop_price or 0) < self._entry_price):
            wrong.append("TRIGGER_NOT_BELOW_PRICE")
        if not (info.reduce_only or info.close_position):
            wrong.append("NOT_REDUCE_ONLY")
        if not info.close_position and not self._same_quantity(info.quantity, self._position_quantity):
            wrong.append("QUANTITY")
        evidence = {"side": info.side, "order_type": info.order_type, "stop_price": info.stop_price,
                    "requested_stop_price": self._stop_price, "quantity": info.quantity,
                    "position_quantity": self._position_quantity, "reduce_only": info.reduce_only,
                    "close_position": info.close_position, "mark_price": mark, "entry_price": self._entry_price}
        if wrong:
            raise _Fail("STOP_GEOMETRY:" + ",".join(wrong), evidence)
        return evidence

    def _protection_replace(self) -> Mapping[str, Any]:
        rules, caps = self._require_rules(), self._require_caps()
        tighter = RC.quantize_stop_price(rules, C.LONG, self._entry_price * (1 - self.config.stop_distance_fraction / 2))
        if not self._stop_price < tighter < self._mark():
            raise _Fail("NO_TIGHTER_STOP_AVAILABLE", {"current": self._stop_price, "tighter": tighter})
        old = self._stop_cid
        if caps.stop_amend_in_place:
            method = "AMEND_IN_PLACE"
            amended = self._mutate("AMEND_STOP", old, {"new_stop_price": tighter}, lambda: self.adapter.amend_stop(
                self._symbol, client_order_id=old, new_stop_price=tighter))
            survivor = amended.client_order_id or old
            self._learn(survivor, amended.venue_order_id)
        elif caps.multiple_stops_per_position:
            method = "CREATE_THEN_CANCEL"  # the new stop rests before the old one goes: never unprotected
            request = self._stop_request(tighter)
            self._place(request)
            self._cancel(old)
            survivor = request.client_order_id
        else:
            method = "CANCEL_THEN_CREATE"  # the venue allows one stop: the position is unprotected in between
            self._cancel(old)
            request = self._stop_request(tighter)
            self._place(request)
            survivor = request.client_order_id
        self._stop_cid, self._stop_price = survivor, tighter
        info = self.adapter.order(self._symbol, client_order_id=survivor)
        stops = [o for o in self.adapter.open_orders(self._symbol) if o.order_type == C.STOP_MARKET]
        evidence = {"method": method, "unprotected_between": method == "CANCEL_THEN_CREATE", "stop_price": info.stop_price,
                    "requested_stop_price": tighter, "protective_stops_open": len(stops)}
        if not info.open or not self._near(info.stop_price, tighter, rules.price_tick):
            raise _Fail("REPLACED_STOP_NOT_READ_BACK", evidence)
        if len(stops) != 1:
            raise _Fail("NOT_EXACTLY_ONE_PROTECTIVE_STOP", evidence)
        return evidence

    def _unknown_submit_reconcile(self) -> Mapping[str, Any]:
        request, order = self._entry()
        unsent = self._order_or_none(self._next_id())  # an id of this run that was never sent
        if unsent is not None:
            raise _Fail("UNSENT_ORDER_REPORTED_AS_EXISTING", {"status": unsent.status})
        sent = self.adapter.order(self._symbol, client_order_id=request.client_order_id)
        if sent.venue_order_id != order.venue_order_id:
            raise _Fail("SENT_ORDER_NOT_FOUND_BY_CLIENT_ORDER_ID")
        return {"order_never_sent": C.ORDER_NOT_FOUND, "order_sent": sent.status}

    def _cancel_check(self) -> Mapping[str, Any]:
        request = self._resting_limit()
        cid = request.client_order_id
        placed = self._place(request)
        if not placed.open:
            raise _Fail("RESTING_ORDER_NOT_OPEN", {"status": placed.status})
        self._cancel(cid)
        after = self.adapter.order(self._symbol, client_order_id=cid)
        listed = any(_same_id(o.client_order_id, cid) for o in self.adapter.open_orders(self._symbol))
        evidence = {"status_after_cancel": after.status, "still_listed": listed, "filled": after.filled_quantity}
        if after.filled_quantity > 0:
            raise _Fail("RESTING_ORDER_FILLED_BEFORE_CANCEL", evidence)
        if after.status != C.CANCELED or listed:
            raise _Fail("ORDER_NOT_CANCELED", evidence)
        try:
            repeated = self._cancel(cid).status
        except C.AdapterError as error:
            if error.code != C.ORDER_NOT_FOUND:
                raise
            repeated = C.ORDER_NOT_FOUND
        if repeated not in (C.CANCELED, C.ORDER_NOT_FOUND):
            raise _Fail("REPEATED_CANCEL_CHANGED_THE_ORDER", {**evidence, "repeated_cancel": repeated})
        return {**evidence, "repeated_cancel": repeated}

    def _lost_acknowledgment(self) -> Mapping[str, Any]:
        faults, request = self._require_faults(), self._resting_limit()
        cid = request.client_order_id
        faults.lose_next_acknowledgment()
        try:
            self._place(request)
        except C.AdapterError as error:
            if not error.uncertain:
                raise _Fail("LOST_ANSWER_NOT_REPORTED_AS_UNCERTAIN", _error_evidence(error)) from None
        else:
            raise _Fail("FAULT_NOT_INJECTED")
        # the only thing allowed after an uncertain submission: read the order back by its client order id
        found = self._order_or_none(cid)
        if found is None or not found.open:
            raise _Fail("ACCEPTED_ORDER_NOT_RECOVERED", {"found": None if found is None else found.status})
        self._learn(cid, found.venue_order_id)
        self._cancel(cid)
        return {"reported": C.NETWORK_UNCERTAIN, "recovered_as": found.status, "resent": False}

    def _connection_interruption(self) -> Mapping[str, Any]:
        faults, request = self._require_faults(), self._resting_limit()
        cid = request.client_order_id
        faults.interrupt_next_request()
        try:
            self._place(request)
        except C.AdapterError as error:
            if error.code not in (C.VENUE_UNAVAILABLE, C.NETWORK_UNCERTAIN):
                raise _Fail(f"INTERRUPTION_NOT_MAPPED:{error.code}", _error_evidence(error)) from None
            reported = error.code
        else:
            raise _Fail("FAULT_NOT_INJECTED")
        found = self._order_or_none(cid)
        if found is not None and found.open:
            self._learn(cid, found.venue_order_id)
            self._cancel(cid)
        offset = self.adapter.server_time().offset_ms  # the next request goes through: the connection is back
        return {"reported": reported, "settled_as": "NOT_ACCEPTED" if found is None else found.status,
                "recovered_offset_ms": offset}

    def _rate_limit(self) -> Mapping[str, Any]:
        faults, wait = self._require_faults(), self.config.rate_limit_retry_after_s
        evidence: Dict[str, Any] = {}
        faults.rate_limit_next_request(wait)
        try:
            self.adapter.server_time()
        except C.AdapterError as error:
            evidence = _error_evidence(error)
            if error.code != C.RATE_LIMITED:
                raise _Fail(f"RATE_REFUSAL_NOT_MAPPED:{error.code}", evidence) from None
            if error.retry_after_s is None or float(error.retry_after_s) < wait:
                raise _Fail("VENUE_WAIT_NOT_CARRIED", evidence) from None
        else:
            # either nothing was injected or the adapter asked again by itself before the wait had passed
            raise _Fail("RATE_REFUSAL_NOT_SURFACED")
        self.sleep(wait)  # the runner itself honours the wait before it asks the venue anything else
        return {**evidence, "waited_s": wait}

    def _clock_drift(self) -> Mapping[str, Any]:
        faults = self._require_faults()
        faults.skew_clock(self.config.clock_skew_ms)
        try:
            self.adapter.balance()
        except C.AdapterError as error:
            if error.code != C.TIMESTAMP_OUT_OF_SYNC:
                raise _Fail(f"CLOCK_DRIFT_NOT_MAPPED:{error.code}", _error_evidence(error)) from None
        else:
            raise _Fail("FAULT_NOT_INJECTED")
        finally:
            faults.skew_clock(0)
        self.adapter.balance()  # with the clock restored the same read succeeds
        return {"reported": C.TIMESTAMP_OUT_OF_SYNC, "skew_ms": self.config.clock_skew_ms, "recovered": True}

    def _restart(self) -> Mapping[str, Any]:
        if self.adapter_factory is None:
            raise _Skip(NOT_RUN, "ADAPTER_FACTORY_REQUIRED")
        fresh = self.adapter_factory()
        if fresh is self.adapter or (fresh.venue, fresh.environment) != (self.adapter.venue, self.adapter.environment):
            raise _Fail("NOT_A_FRESH_ADAPTER_OF_THE_SAME_ACCOUNT")
        position = self._position(fresh)
        stop = fresh.order(self._symbol, client_order_id=self._stop_cid)
        evidence = {"position_side": position.side, "position_quantity": position.quantity, "stop_status": stop.status,
                    "stop_price": stop.stop_price}
        if position.side != C.LONG or not self._same_quantity(position.quantity, self._position_quantity):
            raise _Fail("POSITION_NOT_FOUND_AFTER_RESTART", evidence)
        if not stop.open or not self._near(stop.stop_price, self._stop_price, self._require_rules().price_tick):
            raise _Fail("PROTECTION_NOT_FOUND_AFTER_RESTART", evidence)
        return evidence

    def _manual_position_change(self) -> Mapping[str, Any]:
        faults, rules = self._require_faults(), self._require_rules()
        expected = self._position_quantity - self._unit
        if not expected > float(rules.quantity_step or 0) / 2:
            raise _Skip(NOT_RUN, "POSITION_TOO_SMALL_TO_REDUCE_FROM_OUTSIDE")
        faults.reduce_position_externally(self._symbol, self._unit)
        position = self._position()
        stop = self._order_or_none(self._stop_cid)
        evidence = {"quantity_before": self._position_quantity, "reduced_by": self._unit,
                    "quantity_at_venue": position.quantity, "stop_status": None if stop is None else stop.status,
                    "stop_quantity": None if stop is None else stop.quantity}
        if position.side != C.LONG or not self._same_quantity(position.quantity, expected):
            raise _Fail("EXTERNAL_CHANGE_NOT_SEEN", evidence)
        # the venue's quantity replaces the run's own figure from here on
        self._external_reduction += self._unit
        self._position_quantity = float(position.quantity)
        return evidence

    def _close_position(self) -> Mapping[str, Any]:
        position = self._position()
        if position.side != C.LONG:
            raise _Fail("NO_POSITION_TO_CLOSE", {"position_side": position.side})
        request = C.OrderRequest(self._symbol, C.SELL, C.MARKET, self._next_id(), quantity=position.quantity,
                                 reduce_only=True)
        self._place(request)
        info = self._await_final(request.client_order_id)
        after = self._position()
        evidence = {"closed_quantity": position.quantity, "status": info.status, "position_side_after": after.side}
        if after.side != C.FLAT:
            raise _Fail("POSITION_NOT_CLOSED", evidence)
        self._position_quantity = 0.0
        return evidence

    def _cancel_obsolete_protection(self) -> Mapping[str, Any]:
        info = self._order_or_none(self._stop_cid)
        removed_by = "VENUE"
        if info is not None and info.open:
            self._cancel(self._stop_cid)
            removed_by, info = "RUN", self._order_or_none(self._stop_cid)
        stops = [o for o in self.adapter.open_orders(self._symbol) if o.order_type == C.STOP_MARKET]
        evidence = {"removed_by": removed_by, "status": None if info is None else info.status,
                    "protective_stops_open": len(stops)}
        if (info is not None and info.open) or stops:
            raise _Fail("OBSOLETE_PROTECTION_REMAINS", evidence)
        return evidence

    # ---------------------------------------------------------------------------------------------- cleanup
    def _cleanup(self) -> None:
        bodies = (("FLATTEN", self._flatten), ("RECONCILIATION", self._reconciliation),
                  ("FINAL_ACCOUNT_STATE", self._final_account_state))
        for check_id, body in bodies:
            if check_id not in self._results:  # a recovery does not record again what the run already did
                self._check(check_id, body if self._orders else self._nothing_was_sent)

    @staticmethod
    def _nothing_was_sent() -> Mapping[str, Any]:
        raise _Skip(NOT_RUN, "NO_ORDER_WAS_SENT")

    def _own_venue_orders(self) -> List[str]:
        """The venue order id of every order of this run, asking the venue about one it never answered."""
        for cid in sorted(self._orders):
            if not self._orders[cid]:
                found = self._order_or_none(cid)
                if found is not None:
                    self._learn(cid, found.venue_order_id)
        return [venue_order_id for cid in sorted(self._orders) for venue_order_id in self._orders[cid]]

    def _net_ordered(self) -> float:
        """Bought minus sold according to the venue's record of this run's orders. Flattening relies on this
        and on nothing else, so that it needs only what looking an order up needs."""
        net = 0.0
        for venue_order_id in self._own_venue_orders():
            try:
                info = self.adapter.order(self._symbol, venue_order_id=venue_order_id)
            except C.AdapterError as error:
                if error.code != C.ORDER_NOT_FOUND:
                    raise
                continue  # the venue no longer keeps it: an order it dropped is one that never traded
            net += float(info.filled_quantity) if info.side == C.BUY else -float(info.filled_quantity)
        return net

    def _net_filled(self) -> float:
        """Bought minus sold over the fills of this run's own orders."""
        net = 0.0
        for venue_order_id in self._own_venue_orders():
            for fill in self.adapter.fills(self._symbol, venue_order_id=venue_order_id):
                net += float(fill.quantity) if fill.side == C.BUY else -float(fill.quantity)
        return net

    def _flatten(self) -> Mapping[str, Any]:
        """Cancel every order of the run that is still open, then close what the run's own fills left open
        and nothing more: a position this run did not open is not this run's to close."""
        rules = self._require_rules()
        cancelled = []
        for cid in sorted(self._orders):
            info = self._order_or_none(cid)
            if info is not None and info.open:
                self._cancel(cid)
                cancelled.append(cid)
        position = self._position()
        own = max(self._net_ordered() - self._external_reduction, 0.0)
        residual = RC.quantize_quantity(rules, min(own, position.quantity if position.side == C.LONG else 0.0))
        if residual > 0:
            request = C.OrderRequest(self._symbol, C.SELL, C.MARKET, self._next_id(), quantity=residual, reduce_only=True)
            self._place(request)
            self._await_final(request.client_order_id)
        still_open = []
        for cid in sorted(self._orders):
            info = self._order_or_none(cid)
            if info is not None and info.open:
                still_open.append(cid)
        evidence = {"orders_cancelled": cancelled, "quantity_closed": residual, "orders_still_open": still_open}
        if still_open:
            raise _Fail("RUN_ORDERS_STILL_OPEN", evidence)
        return evidence

    def _reconciliation(self) -> Mapping[str, Any]:
        net, position = self._net_filled(), self._position()
        expected = net - self._external_reduction
        evidence = {"net_filled_by_run": net, "reduced_outside_the_run": self._external_reduction,
                    "position_at_venue": position.signed_quantity}
        if not self._same_quantity(position.signed_quantity, expected):
            raise _Fail("FILLS_DO_NOT_EXPLAIN_POSITION", evidence)
        return evidence

    def _final_account_state(self) -> Mapping[str, Any]:
        position, orders, balance = self._position(), self.adapter.open_orders(self._symbol), self.adapter.balance()
        evidence = {"position_side": position.side, "position_quantity": position.quantity,
                    "open_orders_on_symbol": len(orders), "wallet_balance_before": self._wallet_before,
                    "wallet_balance_after": balance.wallet_balance}
        if position.side != C.FLAT or orders:
            raise _Fail("ACCOUNT_NOT_RESTORED_OPERATOR_ACTION_REQUIRED", evidence)
        return evidence


__all__ = [
    "CERTIFICATION_FRAMEWORK_VERSION", "CERTIFICATION_VALIDITY_DAYS", "CERTIFICATION_VALIDITY_MS",
    "CERTIFICATION_ORDER_PREFIX", "NOT_CERTIFIED", "CERTIFIED", "EXPIRED", "REVOKED", "FAILED", "BLOCKED", "STATUSES",
    "PASSED", "INCOMPLETE", "RUN_STARTED", "RUN_COMPLETED", "VERSION_CHANGED", "PASS", "FAIL", "NOT_RUN",
    "NOT_SUPPORTED", "READ", "ORDER", "FAULT", "CLEANUP", "CORE", "FAULT_INJECTOR", "ADAPTER_FACTORY", "CheckSpec",
    "CATALOGUE", "CHECK_IDS", "OPERATIONS", "catalogue", "is_certification_order", "OperatorPermit",
    "permit_refusal", "FaultInjector", "CertificationRefused", "CertificationStatus", "CertificationResult",
    "CertificationRegister", "certification_status", "adapter_certified", "certification_matrix", "RunnerConfig",
    "CertificationRunner"]
