"""The governed single-account market-entry policy (master plan L-08; external gate EXT-LIQ-01, first step).

Every entry adapter of this repository sends a MARKET order, and the code-constant policy of the platform order
budget (``order_spreading.CONSERVATIVE_DEFAULT``, ``allow_market_fallback=False``) refuses every one of them
(``MARKET_FALLBACK_DISABLED_BY_POLICY``). EXT-LIQ-01's first owner step is to approve a policy that permits market
entries for ONE account at a time under the conservative cap. This module is the record of such an approval:

* **Recorded only through the operator interface** (``app.api.market_entry_policy``, the admin emergency token).
  ``record`` and ``revoke`` take an ``OperatorAuthorization`` -- built only by that interface from a verified
  operator token -- a mandatory owner ``approval_ref`` (record) and a reason. No code path of the engine records,
  extends or re-activates a policy of its own accord, and no setting does.
* **Append-only and hash-chained** (``platform_market_entry_policy_events``, platform migration 61): one
  ``POLICY_RECORDED`` and at most one ``POLICY_REVOKED`` per ``policy_id``; triggers refuse UPDATE and DELETE on both
  dialects; ``verify`` recomputes the chain and a broken chain refuses every governed market entry.
* **Single-account, never calibrated.** The terms can only be EQUAL TO OR TIGHTER THAN the conservative default
  (participation, slippage band, depth age, depth levels), they carry a mandatory absolute notional cap, a spread
  bound and an expiry. They cannot set ``calibrated``: the derived ``SpreadPolicy`` is always ``calibrated=False``
  (and the schema refuses ``calibrated <> 0``), so ``PlatformOrderBudget.reserve_for`` keeps refusing a second
  account on the same venue/symbol/side (``ORDER_SPREADING_CALIBRATION_REQUIRED``).
* **LIVE is refused in this build** (``BUILD_PERMITS_LIVE``) both when recording and when resolving; a LIVE entry is
  in any case stopped earlier by the go-live gate.

``resolve`` is what ``spread_gate.PlatformSpreadGate.reserve`` asks for a MARKET entry when its constructor policy
does not itself permit one. Each failure is its own refusal code (``RESOLUTION_CODES``); no record for the scope
is the default refusal, ``MARKET_FALLBACK_DISABLED_BY_POLICY``, exactly as before this module existed.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional, Tuple

from shared_lib.persistence.sql import Connection, Database

from .order_spreading import CONSERVATIVE_DEFAULT, SpreadPolicy

TABLE = "platform_market_entry_policy_events"
MIGRATION_VERSION = 61
SCOPE = "SINGLE_ACCOUNT"
POLICY_RECORDED, POLICY_REVOKED = "POLICY_RECORDED", "POLICY_REVOKED"
POLICY_VERSION_PREFIX = "single-account-market-entry:"
GENESIS = "0" * 64
ENVIRONMENTS = ("DEMO", "LIVE")

# ── bounds: a governed policy is never looser than the conservative default ──────────────────────────────────
MAX_PARTICIPATION = CONSERVATIVE_DEFAULT.max_participation          # 0.10
MAX_SLIPPAGE_BPS = CONSERVATIVE_DEFAULT.max_slippage_bps            # 25
MAX_SPREAD_BPS = Decimal("25")
MAX_DEPTH_AGE_MS = CONSERVATIVE_DEFAULT.max_depth_age_ms            # 5 000
MIN_DEPTH_AGE_MS = 250
MIN_LEVELS = CONSERVATIVE_DEFAULT.min_levels                        # 5
MAX_LEVELS = 50                                                     # spread_gate.DEPTH_LIMIT: what is read
MAX_ABSOLUTE_CAP_NOTIONAL = Decimal("100000")
MAX_LIFETIME_MS = 90 * 86_400_000
TERM_KEYS = ("max_participation", "max_slippage_bps", "max_spread_bps", "max_depth_age_ms", "min_levels",
             "absolute_cap_notional", "expires_at_ms")
#: keys a caller might use to smuggle calibration or approval into the terms: refused outright
FORBIDDEN_TERM_KEYS = ("calibrated", "approval_ref", "allow_market_fallback", "impact_model", "policy_version")

# ── refusal codes of record / revoke ────────────────────────────────────────────────────────────────────────
OPERATOR_REQUIRED = "OPERATOR_AUTHORIZATION_REQUIRED"
APPROVAL_REF_REQUIRED = "MARKET_POLICY_APPROVAL_REF_REQUIRED"
REASON_REQUIRED = "MARKET_POLICY_REASON_REQUIRED"
CALIBRATION_NOT_PERMITTED = "MARKET_POLICY_CALIBRATION_NOT_PERMITTED"
TERMS_INVALID = "MARKET_POLICY_TERMS_INVALID"
SCOPE_INVALID = "MARKET_POLICY_SCOPE_INVALID"
LIVE_NOT_PERMITTED = "MARKET_POLICY_LIVE_NOT_PERMITTED_IN_THIS_BUILD"
ALREADY_ACTIVE = "MARKET_POLICY_ALREADY_ACTIVE"
NOT_FOUND = "MARKET_POLICY_NOT_FOUND"
ALREADY_REVOKED = "MARKET_POLICY_ALREADY_REVOKED"
LEDGER_UNAVAILABLE = "MARKET_POLICY_LEDGER_UNAVAILABLE"

# ── refusal codes of resolve (what the gate reports) ────────────────────────────────────────────────────────
NO_POLICY = "MARKET_FALLBACK_DISABLED_BY_POLICY"   # the default refusal: no record for this scope
UNAVAILABLE = "MARKET_POLICY_UNAVAILABLE"
LEDGER_INTEGRITY_FAILED = "MARKET_POLICY_LEDGER_INTEGRITY_FAILED"
REVOKED = "MARKET_POLICY_REVOKED"
EXPIRED = "MARKET_POLICY_EXPIRED"
ENVIRONMENT_NOT_PERMITTED = "MARKET_POLICY_ENVIRONMENT_NOT_PERMITTED"
ACCOUNT_SCOPE_MISMATCH = "MARKET_POLICY_ACCOUNT_SCOPE_MISMATCH"
RESOLUTION_CODES = (NO_POLICY, UNAVAILABLE, LEDGER_INTEGRITY_FAILED, REVOKED, EXPIRED, ENVIRONMENT_NOT_PERMITTED,
                    ACCOUNT_SCOPE_MISMATCH, TERMS_INVALID)


class MarketPolicyError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _live_permitted() -> bool:
    """A live-capable build artifact (section 30, ``shared_lib.core.live_build``), not the source constant alone."""
    from shared_lib.core.live_build import live_build_status
    return live_build_status().capable


def _require_operator(operator: Any):
    from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
    if not isinstance(operator, OperatorAuthorization):
        raise MarketPolicyError(OPERATOR_REQUIRED, "recorded only by an authenticated operator")
    return operator


def _text(value: Any, code: str, *, minimum: int = 3, maximum: int = 500) -> str:
    if not isinstance(value, str) or not minimum <= len(value.strip()) <= maximum:
        raise MarketPolicyError(code)
    return value.strip()


def _decimal(name: str, value: Any) -> Decimal:
    if isinstance(value, (float, bool)) or value is None:
        raise MarketPolicyError(TERMS_INVALID, f"{name}: an exact decimal (string or integer) is required")
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise MarketPolicyError(TERMS_INVALID, f"{name}: not a number")
    if not d.is_finite():
        raise MarketPolicyError(TERMS_INVALID, f"{name}: not finite")
    return d


def _integer(name: str, value: Any) -> int:
    if isinstance(value, bool) or isinstance(value, float):
        raise MarketPolicyError(TERMS_INVALID, f"{name}: an integer is required")
    d = _decimal(name, value)
    if d != d.to_integral_value():
        raise MarketPolicyError(TERMS_INVALID, f"{name}: an integer is required")
    return int(d)


@dataclass(frozen=True)
class PolicyTerms:
    max_participation: Decimal
    max_slippage_bps: Decimal
    max_spread_bps: Decimal
    max_depth_age_ms: int
    min_levels: int
    absolute_cap_notional: Decimal
    expires_at_ms: int

    @classmethod
    def parse(cls, terms: Mapping[str, Any], *, recorded_at_ms: Optional[int] = None) -> "PolicyTerms":
        """Validated terms or ``MarketPolicyError``. ``recorded_at_ms`` (the database clock) bounds the expiry when
        recording; a stored record is re-checked without it."""
        if not isinstance(terms, Mapping):
            raise MarketPolicyError(TERMS_INVALID, "terms must be an object")
        smuggled = sorted(k for k in terms if k in FORBIDDEN_TERM_KEYS)
        if smuggled:
            raise MarketPolicyError(CALIBRATION_NOT_PERMITTED, ",".join(smuggled))
        unknown = sorted(k for k in terms if k not in TERM_KEYS)
        missing = sorted(k for k in TERM_KEYS if k not in terms)
        if unknown or missing:
            raise MarketPolicyError(TERMS_INVALID, f"unknown={unknown} missing={missing}")
        p = _decimal("max_participation", terms["max_participation"])
        slip = _decimal("max_slippage_bps", terms["max_slippage_bps"])
        spread = _decimal("max_spread_bps", terms["max_spread_bps"])
        age = _integer("max_depth_age_ms", terms["max_depth_age_ms"])
        levels = _integer("min_levels", terms["min_levels"])
        cap = _decimal("absolute_cap_notional", terms["absolute_cap_notional"])
        expires = _integer("expires_at_ms", terms["expires_at_ms"])
        checks = (
            (Decimal(0) < p <= MAX_PARTICIPATION, f"max_participation must be in (0, {MAX_PARTICIPATION}]"),
            (Decimal(0) < slip <= MAX_SLIPPAGE_BPS, f"max_slippage_bps must be in (0, {MAX_SLIPPAGE_BPS}]"),
            (Decimal(0) < spread <= MAX_SPREAD_BPS, f"max_spread_bps must be in (0, {MAX_SPREAD_BPS}]"),
            (MIN_DEPTH_AGE_MS <= age <= MAX_DEPTH_AGE_MS,
             f"max_depth_age_ms must be in [{MIN_DEPTH_AGE_MS}, {MAX_DEPTH_AGE_MS}]"),
            (MIN_LEVELS <= levels <= MAX_LEVELS, f"min_levels must be in [{MIN_LEVELS}, {MAX_LEVELS}]"),
            (Decimal(0) < cap <= MAX_ABSOLUTE_CAP_NOTIONAL,
             f"absolute_cap_notional must be in (0, {MAX_ABSOLUTE_CAP_NOTIONAL}]"),
            (expires > 0, "expires_at_ms must be a positive epoch-millisecond time"),
        )
        for ok, why in checks:
            if not ok:
                raise MarketPolicyError(TERMS_INVALID, why)
        if recorded_at_ms is not None and not recorded_at_ms < expires <= recorded_at_ms + MAX_LIFETIME_MS:
            raise MarketPolicyError(TERMS_INVALID, f"expires_at_ms must be after now and at most {MAX_LIFETIME_MS} ms ahead")
        return cls(p, slip, spread, age, levels, cap, expires)

    def to_plain(self) -> Dict[str, Any]:
        return {"max_participation": str(self.max_participation), "max_slippage_bps": str(self.max_slippage_bps),
                "max_spread_bps": str(self.max_spread_bps), "max_depth_age_ms": self.max_depth_age_ms,
                "min_levels": self.min_levels, "absolute_cap_notional": str(self.absolute_cap_notional),
                "expires_at_ms": self.expires_at_ms}


@dataclass(frozen=True)
class PolicyEvent:
    seq: int
    event_id: str
    event_type: str
    policy_id: str
    venue: str
    symbol: str
    environment: str
    account_id: Optional[str]
    scope: str
    calibrated: int
    approval_ref: Optional[str]
    reason: str
    actor_ref: str
    terms: str
    recorded_at: int
    prev_hash: str
    hash: str

    def to_plain(self) -> Dict[str, Any]:
        return {"seq": self.seq, "event_id": self.event_id, "event_type": self.event_type, "policy_id": self.policy_id,
                "venue": self.venue, "symbol": self.symbol, "environment": self.environment,
                "account_id": self.account_id, "scope": self.scope, "calibrated": bool(self.calibrated),
                "approval_ref": self.approval_ref, "reason": self.reason, "actor_ref": self.actor_ref,
                "terms": json.loads(self.terms), "recorded_at": self.recorded_at, "hash": self.hash}


@dataclass(frozen=True)
class PolicyRecord:
    """An approved, recorded policy (whatever its current state: ``revoked`` / expiry are reported, not hidden)."""
    policy_id: str
    venue: str
    symbol: str
    environment: str
    account_id: Optional[str]
    approval_ref: str
    actor_ref: str
    reason: str
    terms: PolicyTerms
    recorded_at: int
    revoked: Optional[PolicyEvent] = None

    @property
    def policy_version(self) -> str:
        return POLICY_VERSION_PREFIX + self.policy_id

    def active(self, now_ms: int) -> bool:
        return self.revoked is None and int(now_ms) < self.terms.expires_at_ms

    def spread_policy(self) -> SpreadPolicy:
        """The order-budget policy this record permits: the conservative default's scheduling, the record's own
        (equal or tighter) depth and cap terms, market entries allowed, and NEVER calibrated. It carries no
        ``approval_ref`` (that field belongs to a calibrated policy, which nothing builds): the budget and every
        decision row name the record through ``policy_version`` (``single-account-market-entry:<policy_id>``)."""
        t = self.terms
        return SpreadPolicy(policy_version=self.policy_version, max_participation=t.max_participation,
                            max_slippage_bps=t.max_slippage_bps, max_depth_age_ms=t.max_depth_age_ms,
                            min_levels=t.min_levels, absolute_cap_notional=t.absolute_cap_notional,
                            slot_interval_ms=CONSERVATIVE_DEFAULT.slot_interval_ms,
                            max_orders_per_slot=CONSERVATIVE_DEFAULT.max_orders_per_slot,
                            budget_ttl_ms=CONSERVATIVE_DEFAULT.budget_ttl_ms, allow_market_fallback=True,
                            calibrated=False)

    def to_plain(self, now_ms: Optional[int] = None) -> Dict[str, Any]:
        out = {"policy_id": self.policy_id, "policy_version": self.policy_version, "venue": self.venue,
               "symbol": self.symbol, "environment": self.environment, "account_id": self.account_id, "scope": SCOPE,
               "calibrated": False, "approval_ref": self.approval_ref, "recorded_by": self.actor_ref,
               "reason": self.reason, "terms": self.terms.to_plain(), "recorded_at": self.recorded_at,
               "revoked": None if self.revoked is None else {
                   "by": self.revoked.actor_ref, "reason": self.revoked.reason, "at": self.revoked.recorded_at}}
        if now_ms is not None:
            out["active"] = self.active(now_ms)
        return out


def event_hash(prev: str, event_id: str, event_type: str, policy_id: str, venue: str, symbol: str, environment: str,
               account_id: Optional[str], scope: str, calibrated: int, approval_ref: Optional[str], reason: str,
               actor_ref: str, terms: str, at: int) -> str:
    body = "\x1f".join([prev, event_id, event_type, policy_id, venue, symbol, environment, account_id or "", scope,
                        str(int(calibrated)), approval_ref or "", reason, actor_ref, terms, str(int(at))])
    return hashlib.sha256(body.encode()).hexdigest()


_COLUMNS = ("seq, event_id, event_type, policy_id, venue, symbol, environment, account_id, scope, calibrated, "
            "approval_ref, reason, actor_ref, terms, recorded_at, prev_hash, hash")


def _event(r) -> PolicyEvent:
    return PolicyEvent(int(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], int(r[9]), r[10], r[11], r[12], r[13],
                       int(r[14]), r[15], r[16])


#: The all-symbols scope: DEMO only, and only for a record that names its one account. A strategy that selects
#: its symbol each cycle (the residual family picks one of ~140) cannot be pre-approved symbol by symbol; the
#: uncalibrated budget still allows ONE account per venue / symbol / side (ORDER_SPREADING_CALIBRATION_REQUIRED).
ALL_SYMBOLS = "*"
ALL_SYMBOLS_REQUIRE_DEMO_ACCOUNT = "MARKET_POLICY_ALL_SYMBOLS_REQUIRES_ONE_DEMO_ACCOUNT"


def _scope(venue: Any, symbol: Any, environment: Any) -> Tuple[str, str, str]:
    v, s, e = (str(x).strip().upper() if isinstance(x, str) else "" for x in (venue, symbol, environment))
    if not v or not s or e not in ENVIRONMENTS:
        raise MarketPolicyError(SCOPE_INVALID, "venue, symbol and an environment of DEMO or LIVE are required")
    if s == ALL_SYMBOLS and e != "DEMO":
        raise MarketPolicyError(ALL_SYMBOLS_REQUIRE_DEMO_ACCOUNT, "the all-symbols scope exists for DEMO only")
    return v, s, e


def fold(events: List[PolicyEvent]) -> Dict[str, PolicyRecord]:
    """Every recorded policy by id, with its revocation (if any). Pure: the state is the fold of the events."""
    out: Dict[str, PolicyRecord] = {}
    for e in events:
        if e.event_type == POLICY_RECORDED:
            out[e.policy_id] = PolicyRecord(e.policy_id, e.venue, e.symbol, e.environment, e.account_id,
                                            e.approval_ref or "", e.actor_ref, e.reason,
                                            PolicyTerms.parse(json.loads(e.terms)), e.recorded_at)
        elif e.event_type == POLICY_REVOKED and e.policy_id in out and out[e.policy_id].revoked is None:
            rec = out[e.policy_id]
            out[e.policy_id] = PolicyRecord(rec.policy_id, rec.venue, rec.symbol, rec.environment, rec.account_id,
                                            rec.approval_ref, rec.actor_ref, rec.reason, rec.terms, rec.recorded_at, e)
    return out


class MarketEntryPolicyLedger:
    """The ledger on a platform database migrated through version 61."""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ── writing ──────────────────────────────────────────────────────────────────────────────────────────
    def record(self, operator: Any, *, venue: str, symbol: str, environment: str, terms: Mapping[str, Any],
               approval_ref: Any, reason: Any, account_id: Optional[str] = None) -> PolicyRecord:
        """Record an owner-approved single-account policy. Refused (``MarketPolicyError``) without an operator, an
        approval reference or a reason; with invalid or calibrating terms; for LIVE in this build; or while another
        policy for the same venue/symbol/environment is still active (revoke it first)."""
        op = _require_operator(operator)
        ref = _text(approval_ref, APPROVAL_REF_REQUIRED, maximum=200)
        why = _text(reason, REASON_REQUIRED)
        v, s, e = _scope(venue, symbol, environment)
        if e == "LIVE" and not _live_permitted():
            raise MarketPolicyError(LIVE_NOT_PERMITTED)
        account = None
        if account_id is not None:
            account = _text(account_id, SCOPE_INVALID, minimum=1, maximum=200)
        if s == ALL_SYMBOLS and account is None:
            raise MarketPolicyError(ALL_SYMBOLS_REQUIRE_DEMO_ACCOUNT, "an all-symbols record names its one account")
        PolicyTerms.parse(terms)   # shape and bounds before the transaction; the expiry is re-checked inside
        with self.db.transaction() as conn:
            self._lock(conn)
            now = conn.now_ms()
            parsed = PolicyTerms.parse(terms, recorded_at_ms=now)
            current = self._latest_on(conn, v, s, e)
            if current is not None and current.active(now):
                raise MarketPolicyError(ALREADY_ACTIVE, current.policy_id)
            policy_id = uuid.uuid4().hex
            ev = self._append_on(conn, POLICY_RECORDED, policy_id, v, s, e, account, ref, why, op.actor_ref,
                                 json.dumps(parsed.to_plain(), sort_keys=True), now)
        return fold([ev])[policy_id]

    def revoke(self, operator: Any, policy_id: str, *, reason: Any) -> PolicyEvent:
        """Revoke a recorded policy. Takes one operator and is never delayed: closing is always easier than
        opening. The gate refuses ``MARKET_POLICY_REVOKED`` from the moment this commits."""
        op = _require_operator(operator)
        why = _text(reason, REASON_REQUIRED)
        with self.db.transaction() as conn:
            self._lock(conn)
            rows = conn.all(f"SELECT {_COLUMNS} FROM {TABLE} WHERE policy_id=? ORDER BY seq", (str(policy_id),))
            events = [_event(r) for r in rows]
            records = fold(events)
            if str(policy_id) not in records:
                raise MarketPolicyError(NOT_FOUND, str(policy_id))
            rec = records[str(policy_id)]
            if rec.revoked is not None:
                raise MarketPolicyError(ALREADY_REVOKED, str(policy_id))
            return self._append_on(conn, POLICY_REVOKED, rec.policy_id, rec.venue, rec.symbol, rec.environment,
                                   rec.account_id, None, why, op.actor_ref, "{}", conn.now_ms())

    @staticmethod
    def _lock(conn: Connection) -> None:
        if conn.dialect == "postgresql":
            conn.execute("SELECT pg_advisory_xact_lock(?)", (0x1_08_0FF,))   # one chain, one writer at a time

    def _append_on(self, conn: Connection, event_type: str, policy_id: str, venue: str, symbol: str,
                   environment: str, account_id: Optional[str], approval_ref: Optional[str], reason: str,
                   actor_ref: str, terms: str, at: int) -> PolicyEvent:
        prev = conn.scalar(f"SELECT hash FROM {TABLE} ORDER BY seq DESC LIMIT 1") or GENESIS
        event_id = uuid.uuid4().hex
        h = event_hash(prev, event_id, event_type, policy_id, venue, symbol, environment, account_id, SCOPE, 0,
                       approval_ref, reason, actor_ref, terms, at)
        conn.execute(f"INSERT INTO {TABLE} (event_id, event_type, policy_id, venue, symbol, environment, account_id, "
                     "scope, calibrated, approval_ref, reason, actor_ref, terms, recorded_at, prev_hash, hash) "
                     "VALUES (?,?,?,?,?,?,?,?,0,?,?,?,?,?,?,?)",
                     (event_id, event_type, policy_id, venue, symbol, environment, account_id, SCOPE, approval_ref,
                      reason, actor_ref, terms, at, prev, h))
        return _event(conn.one(f"SELECT {_COLUMNS} FROM {TABLE} WHERE event_id=?", (event_id,)))

    # ── reading ──────────────────────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _events_on(conn: Connection, where: str = "", params: Tuple[Any, ...] = ()) -> List[PolicyEvent]:
        return [_event(r) for r in conn.all(f"SELECT {_COLUMNS} FROM {TABLE}{where} ORDER BY seq", params)]

    def _latest_on(self, conn: Connection, venue: str, symbol: str, environment: str) -> Optional[PolicyRecord]:
        events = self._events_on(conn, " WHERE venue=? AND symbol=? AND environment=?", (venue, symbol, environment))
        records = fold(events)
        recorded = [e for e in events if e.event_type == POLICY_RECORDED]
        return records[recorded[-1].policy_id] if recorded else None

    def events(self) -> List[PolicyEvent]:
        with self.db.transaction(write=False) as conn:
            return self._events_on(conn)

    def records(self) -> List[PolicyRecord]:
        return list(fold(self.events()).values())

    @staticmethod
    def verify_events(events: List[PolicyEvent]) -> Tuple[bool, str]:
        prev = GENESIS
        for e in events:
            expected = event_hash(prev, e.event_id, e.event_type, e.policy_id, e.venue, e.symbol, e.environment,
                                  e.account_id, e.scope, e.calibrated, e.approval_ref, e.reason, e.actor_ref, e.terms,
                                  e.recorded_at)
            if e.prev_hash != prev or e.hash != expected or e.calibrated != 0 or e.scope != SCOPE:
                return False, f"LEDGER_CHAIN_BROKEN:seq={e.seq}"
            prev = e.hash
        return True, ""

    def verify(self) -> Tuple[bool, str]:
        return self.verify_events(self.events())

    @staticmethod
    def installed(conn: Connection) -> bool:
        return conn.scalar("SELECT COUNT(*) FROM platform_schema_migrations WHERE version=?", (MIGRATION_VERSION,)) == 1

    def resolve(self, *, venue: str, symbol: str, environment: str, account_id: str) -> Tuple[PolicyRecord, int]:
        """The ACTIVE approved policy for this venue / symbol / environment and account, with the database time it
        was read at, or ``MarketPolicyError`` with the first failing condition (``RESOLUTION_CODES``):

        1. the ledger is installed and readable (not installed: no record can exist -> ``NO_POLICY``; unreadable ->
           ``MARKET_POLICY_UNAVAILABLE``) and its hash chain is intact (``MARKET_POLICY_LEDGER_INTEGRITY_FAILED``);
        2. a policy was recorded for this exact venue / symbol / environment (``MARKET_FALLBACK_DISABLED_BY_POLICY``);
        3. the latest one is not revoked (``MARKET_POLICY_REVOKED``) and not expired on the database clock
           (``MARKET_POLICY_EXPIRED``);
        4. the environment is one this build permits (``MARKET_POLICY_ENVIRONMENT_NOT_PERMITTED``: LIVE);
        5. an account-scoped record names this account (``MARKET_POLICY_ACCOUNT_SCOPE_MISMATCH``);
        6. its stored terms still pass today's bounds (``MARKET_POLICY_TERMS_INVALID``)."""
        try:
            v, s, e = _scope(venue, symbol, environment)
        except MarketPolicyError:
            raise MarketPolicyError(NO_POLICY, "scope not resolvable")
        try:
            with self.db.transaction(write=False) as conn:
                if not self.installed(conn):
                    raise MarketPolicyError(NO_POLICY, "ledger not installed")
                now = conn.now_ms()
                events = self._events_on(conn)
        except MarketPolicyError:
            raise
        except Exception as exc:
            raise MarketPolicyError(UNAVAILABLE, type(exc).__name__)
        ok, why = self.verify_events(events)
        if not ok:
            raise MarketPolicyError(LEDGER_INTEGRITY_FAILED, why)
        scoped = [x for x in events if (x.venue, x.symbol, x.environment) == (v, s, e)]
        recorded = [x for x in scoped if x.event_type == POLICY_RECORDED]
        if not recorded and e == "DEMO":
            # A symbol-specific record always wins; otherwise a DEMO all-symbols record of this venue.
            scoped = [x for x in events if (x.venue, x.symbol, x.environment) == (v, ALL_SYMBOLS, e)]
            recorded = [x for x in scoped if x.event_type == POLICY_RECORDED]
        if not recorded:
            raise MarketPolicyError(NO_POLICY, "no approved single-account policy for this scope")
        try:
            rec = fold(scoped)[recorded[-1].policy_id]
        except MarketPolicyError as exc:
            raise MarketPolicyError(TERMS_INVALID, exc.detail)
        if rec.revoked is not None:
            raise MarketPolicyError(REVOKED, rec.policy_id)
        if now >= rec.terms.expires_at_ms:
            raise MarketPolicyError(EXPIRED, rec.policy_id)
        if rec.environment == "LIVE" and not _live_permitted():
            raise MarketPolicyError(ENVIRONMENT_NOT_PERMITTED, "LIVE_NOT_PERMITTED_IN_THIS_BUILD")
        if rec.account_id is not None and rec.account_id != str(account_id):
            raise MarketPolicyError(ACCOUNT_SCOPE_MISMATCH, rec.policy_id)
        if not rec.approval_ref or len(rec.approval_ref.strip()) < 3:
            raise MarketPolicyError(TERMS_INVALID, "approval reference missing")
        return rec, now


__all__ = ["MarketEntryPolicyLedger", "MarketPolicyError", "PolicyRecord", "PolicyTerms", "PolicyEvent", "fold",
           "event_hash", "TABLE", "SCOPE", "RESOLUTION_CODES", "NO_POLICY", "UNAVAILABLE", "LEDGER_INTEGRITY_FAILED",
           "REVOKED", "EXPIRED", "ENVIRONMENT_NOT_PERMITTED", "ACCOUNT_SCOPE_MISMATCH", "TERMS_INVALID",
           "APPROVAL_REF_REQUIRED", "REASON_REQUIRED", "CALIBRATION_NOT_PERMITTED", "LIVE_NOT_PERMITTED",
           "ALREADY_ACTIVE", "NOT_FOUND", "ALREADY_REVOKED", "OPERATOR_REQUIRED", "SCOPE_INVALID"]
