"""Experimental DEMO authorization: an uncertified strategy family on ONE verified DEMO account, for a bounded time.

The owner wants to watch a research strategy trade an exchange DEMO account before any live release. The family
gate (``pipeline.provenance.FamilyExecutionGate``) normally refuses such a family twice: its research verdict is
not CERTIFIED, and its governance stage (M0) gives it no DEMO order authority. An experimental-DEMO grant waives
exactly those two checks, and nothing else, for one (account, user, family) on DEMO:

* **DEMO only, verified.** Granted only for an account whose stored environment is DEMO, whose resolved
  credentials are DEMO and whose canonical base URL is the venue's DEMO host. Checked again on every use: a plan
  for LIVE, an account moved to LIVE or a non-DEMO host is refused. There is no LIVE form of a grant.
* **Never certification, never promotion.** It writes only to its own ledger (``cati_experimental_demo_events``).
  It never writes the family governance log, the research register or the holdout registry, and nothing that
  reads those (promotion prerequisites, certification, the Mandate 004 holdout) reads this ledger. The family
  stays at its stage (M0) and keeps its research verdict.
* **Every other gate still applies.** Family registration, rule hash, disabled / kill switch, the immutable
  decision verification, venue (adapter) certification, account ownership and consent, the DEMO order switch,
  capital, hard risk and sizing, liquidation, market-data freshness, the market-entry (L-08) policy with its depth,
  spread and slippage bounds, the account fence, native reduce-only stop protection and reconciliation.
* **Bounded.** A grant names an operator, an owner authorization reference, a reason, a maximum entry notional
  and an expiry no further than ``MAX_DURATION_HOURS`` ahead; it lapses on its own. The operator must state the
  family's current research status (``acknowledged_research_status``), so nobody grants a FAILED family without
  seeing that it failed. Revocation takes effect on the next check (the boundary re-checks right before the
  order).
* **Recorded.** Append-only and hash-chained (triggers refuse UPDATE / DELETE). Every order claimed under a grant
  is recorded as an ``ENTRY_CLAIMED`` event with its trade plan, attempt and notional, so every position and
  every result it produces can be identified as experimental DEMO activity.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

TABLE = "cati_experimental_demo_events"
GENESIS = "0" * 64
AUTHORIZED, REVOKED, ENTRY_CLAIMED = "AUTHORIZED", "REVOKED", "ENTRY_CLAIMED"
EVENT_TYPES = (AUTHORIZED, REVOKED, ENTRY_CLAIMED)
ENVIRONMENT = "DEMO"
MAX_DURATION_HOURS = 14 * 24
MAX_ENTRY_NOTIONAL_USDT = Decimal("1000")
#: The family gate reports a use of a grant as this reason with the authorization id after the colon.
AUTHORIZED_REASON = "EXPERIMENTAL_DEMO_AUTHORIZED"
LABEL = "EXPERIMENTAL_DEMO"

# refusal codes (grant / revoke)
OPERATOR_REQUIRED = "OPERATOR_AUTHORIZATION_REQUIRED"
APPROVAL_REF_REQUIRED = "EXPERIMENTAL_DEMO_APPROVAL_REF_REQUIRED"
REASON_REQUIRED = "EXPERIMENTAL_DEMO_REASON_REQUIRED"
LIVE_NOT_PERMITTED = "EXPERIMENTAL_DEMO_LIVE_NOT_PERMITTED"
ACCOUNT_NOT_FOUND = "EXPERIMENTAL_DEMO_ACCOUNT_NOT_FOUND"
ACCOUNT_NOT_DEMO = "EXPERIMENTAL_DEMO_ACCOUNT_NOT_DEMO"
ACCOUNT_NOT_CONNECTED = "EXPERIMENTAL_DEMO_ACCOUNT_NOT_CONNECTED"
CREDENTIALS_NOT_DEMO = "EXPERIMENTAL_DEMO_CREDENTIALS_NOT_VERIFIED_DEMO"
FAMILY_UNKNOWN = "EXPERIMENTAL_DEMO_FAMILY_UNKNOWN"
FAMILY_NOT_ELIGIBLE = "EXPERIMENTAL_DEMO_FAMILY_NOT_ELIGIBLE"
FAMILY_ALREADY_AUTHORIZED = "EXPERIMENTAL_DEMO_FAMILY_ALREADY_CERTIFIED_FOR_DEMO"
RESEARCH_STATUS_NOT_ACKNOWLEDGED = "EXPERIMENTAL_DEMO_RESEARCH_STATUS_NOT_ACKNOWLEDGED"
DURATION_INVALID = "EXPERIMENTAL_DEMO_DURATION_INVALID"
NOTIONAL_INVALID = "EXPERIMENTAL_DEMO_NOTIONAL_INVALID"
ALREADY_ACTIVE = "EXPERIMENTAL_DEMO_ALREADY_ACTIVE"
NOT_FOUND = "EXPERIMENTAL_DEMO_AUTHORIZATION_NOT_FOUND"
ALREADY_REVOKED = "EXPERIMENTAL_DEMO_ALREADY_REVOKED"
# refusal codes (use)
NOTIONAL_EXCEEDED = "EXPERIMENTAL_DEMO_NOTIONAL_LIMIT_EXCEEDED"
GRANT_LAPSED = "EXPERIMENTAL_DEMO_AUTHORIZATION_LAPSED"
LEDGER_INTEGRITY_FAILED = "EXPERIMENTAL_DEMO_LEDGER_INTEGRITY_FAILED"


class ExperimentalDemoError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


DDL = (
    f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        seq INTEGER PRIMARY KEY,
        event_id TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL,
        authorization_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        family_id TEXT NOT NULL,
        environment TEXT NOT NULL CHECK (environment = 'DEMO'),
        actor_ref TEXT NOT NULL,
        recorded_at INTEGER NOT NULL,
        payload TEXT NOT NULL,
        prev_hash TEXT NOT NULL,
        event_hash TEXT NOT NULL UNIQUE)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_xdemo_scope ON {TABLE}(account_id, family_id, seq)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_xdemo_auth ON {TABLE}(authorization_id, seq)",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xdemo_no_update BEFORE UPDATE ON {TABLE}
        BEGIN SELECT RAISE(ABORT, '{TABLE} is append-only'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_xdemo_no_delete BEFORE DELETE ON {TABLE}
        BEGIN SELECT RAISE(ABORT, '{TABLE} is append-only'); END""",
)


def ensure(conn: Any) -> None:
    for statement in DDL:
        conn.execute(statement)


def _installed(conn: Any) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone())


def _hash(core: Dict[str, Any]) -> str:
    from app.trading_intelligence.hashing import stable_hash
    return stable_hash(core)


_CORE = ("seq", "event_type", "authorization_id", "account_id", "user_id", "family_id", "environment", "actor_ref",
         "recorded_at", "payload", "prev_hash")


@dataclass(frozen=True)
class Grant:
    authorization_id: str
    account_id: str
    user_id: str
    family_id: str
    family_rule_hash: str
    research_status_at_grant: str
    stage_at_grant: str
    venue: str
    base_url: str
    approval_ref: str
    reason: str
    actor_ref: str
    max_entry_notional_usdt: Decimal
    granted_at: int
    expires_at: int
    revoked_at: Optional[int] = None
    revoked_by: Optional[str] = None
    revoke_reason: Optional[str] = None
    entries: Tuple[Dict[str, Any], ...] = ()

    def active(self, now_ms: int) -> bool:
        return self.revoked_at is None and self.granted_at <= int(now_ms) < self.expires_at

    def state(self, now_ms: int) -> str:
        if self.revoked_at is not None:
            return "REVOKED"
        return "ACTIVE" if self.active(now_ms) else "EXPIRED"

    def to_plain(self, now_ms: Optional[int] = None) -> Dict[str, Any]:
        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        return {"authorization_id": self.authorization_id, "label": LABEL, "environment": ENVIRONMENT,
                "state": self.state(now), "account_id": self.account_id, "user_id": self.user_id,
                "family_id": self.family_id, "family_rule_hash": self.family_rule_hash,
                "research_status_at_grant": self.research_status_at_grant, "stage_at_grant": self.stage_at_grant,
                "venue": self.venue, "base_url": self.base_url, "approval_ref": self.approval_ref,
                "reason": self.reason, "granted_by": self.actor_ref,
                "max_entry_notional_usdt": str(self.max_entry_notional_usdt), "granted_at": self.granted_at,
                "expires_at": self.expires_at, "seconds_remaining": max(0, (self.expires_at - now) // 1000)
                if self.active(now) else 0, "revoked_at": self.revoked_at, "revoked_by": self.revoked_by,
                "revoke_reason": self.revoke_reason, "entries_claimed": len(self.entries),
                "entries": list(self.entries),
                "not_certification": True, "grants_live_authority": False, "promotes_family": False}


def _fold(rows: List[Dict[str, Any]]) -> Dict[str, Grant]:
    grants: Dict[str, Grant] = {}
    for r in rows:
        p = json.loads(r["payload"])
        aid = r["authorization_id"]
        if r["event_type"] == AUTHORIZED:
            grants[aid] = Grant(aid, r["account_id"], r["user_id"], r["family_id"], p["family_rule_hash"],
                                p["research_status_at_grant"], p["stage_at_grant"], p["venue"], p["base_url"],
                                p["approval_ref"], p["reason"], r["actor_ref"], Decimal(p["max_entry_notional_usdt"]),
                                int(r["recorded_at"]), int(p["expires_at"]))
        elif aid in grants and r["event_type"] == REVOKED and grants[aid].revoked_at is None:
            g = grants[aid]
            grants[aid] = Grant(**{**g.__dict__, "revoked_at": int(r["recorded_at"]), "revoked_by": r["actor_ref"],
                                   "revoke_reason": p.get("reason")})
        elif aid in grants and r["event_type"] == ENTRY_CLAIMED:
            g = grants[aid]
            grants[aid] = Grant(**{**g.__dict__, "entries": g.entries + ({**p, "recorded_at": int(r["recorded_at"])},)})
    return grants


def _require_operator(operator: Any):
    from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
    if not isinstance(operator, OperatorAuthorization):
        raise ExperimentalDemoError(OPERATOR_REQUIRED, "granted only by an authenticated administrator")
    return operator


def _text(value: Any, code: str, *, minimum: int = 3, maximum: int = 500) -> str:
    text = str(value or "").strip()
    if not (minimum <= len(text) <= maximum):
        raise ExperimentalDemoError(code, f"between {minimum} and {maximum} characters are required")
    return text


def verified_demo_account(db: Any, account_id: str, user_id: Optional[str] = None) -> Dict[str, Any]:
    """The account's DEMO identity, or ``ExperimentalDemoError``: stored environment DEMO, connected, its
    credentials resolve to DEMO and its canonical base URL is the venue's DEMO host."""
    from shared_lib.broker.environment import normalize_environment, resolve_base_url
    with db.connect() as c:
        row = c.execute("SELECT id,user_id,broker_id,environment,status FROM broker_accounts WHERE id=?",
                        (account_id,)).fetchone()
    if not row or (user_id is not None and row["user_id"] != user_id):
        raise ExperimentalDemoError(ACCOUNT_NOT_FOUND, account_id)
    account = dict(row)
    try:
        environment = normalize_environment(account["environment"]).value.upper()
    except ValueError:
        environment = ""
    if environment != ENVIRONMENT:
        raise ExperimentalDemoError(ACCOUNT_NOT_DEMO, f"stored environment {account['environment']!r}")
    if str(account["status"]).lower() not in {"connected", "active"}:
        raise ExperimentalDemoError(ACCOUNT_NOT_CONNECTED, str(account["status"]))
    demo_url = resolve_base_url(account["broker_id"], normalize_environment("DEMO"))
    from shared_lib.broker.resolver import resolve_broker_auth
    try:
        auth = resolve_broker_auth(account["id"], account["user_id"], db)
    except Exception as exc:  # noqa: BLE001 -- unusable credentials are a refusal, never a pass
        raise ExperimentalDemoError(CREDENTIALS_NOT_DEMO, type(exc).__name__) from None
    if normalize_environment(auth.environment).value.upper() != ENVIRONMENT or \
            str(getattr(auth, "base_url", "") or "").rstrip("/") != str(demo_url).rstrip("/"):
        raise ExperimentalDemoError(CREDENTIALS_NOT_DEMO, "resolved credentials are not on the DEMO host")
    return {**account, "environment": ENVIRONMENT, "base_url": demo_url}


class ExperimentalDemoLedger:
    def __init__(self, db: Any) -> None:
        self._db = db

    # ── reads ──────────────────────────────────────────────────────────────────────────────────────────
    def _rows(self, conn: Any) -> List[Dict[str, Any]]:
        if not _installed(conn):
            return []
        return [dict(r) for r in conn.execute(f"SELECT * FROM {TABLE} ORDER BY seq")]

    def events(self) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            rows = self._rows(conn)
        return [{**r, "payload": json.loads(r["payload"])} for r in rows]

    def grants(self) -> List[Grant]:
        with self._db.connect() as conn:
            return list(_fold(self._rows(conn)).values())

    def grant(self, authorization_id: str) -> Optional[Grant]:
        return next((g for g in self.grants() if g.authorization_id == authorization_id), None)

    def verify(self) -> Tuple[bool, str]:
        prev, count = GENESIS, 0
        with self._db.connect() as conn:
            for r in self._rows(conn):
                count += 1
                core = {k: r[k] for k in _CORE}
                if int(r["seq"]) != count or r["prev_hash"] != prev or _hash(core) != r["event_hash"]:
                    return False, f"BROKEN_AT_SEQ_{r['seq']}"
                prev = r["event_hash"]
        return True, f"OK_{count}_EVENTS"

    def active_for(self, *, account_id: str, user_id: str, family_id: str, now_ms: int) -> Optional[Grant]:
        """The active grant of this scope, or None. A broken ledger authorizes nothing."""
        ok, _ = self.verify()
        if not ok:
            return None
        return next((g for g in self.grants() if g.account_id == account_id and g.user_id == user_id
                     and g.family_id == family_id and g.active(now_ms)), None)

    # ── writes ─────────────────────────────────────────────────────────────────────────────────────────
    def _append(self, conn: Any, event_type: str, grant_scope: Dict[str, str], actor_ref: str,
                payload: Dict[str, Any], now_ms: int) -> Dict[str, Any]:
        last = conn.execute(f"SELECT seq, event_hash FROM {TABLE} ORDER BY seq DESC LIMIT 1").fetchone()
        seq, prev = (int(last[0]) + 1, last[1]) if last else (1, GENESIS)
        core = {"seq": seq, "event_type": event_type, **grant_scope, "environment": ENVIRONMENT,
                "actor_ref": actor_ref, "recorded_at": int(now_ms),
                "payload": json.dumps(payload, sort_keys=True, default=str), "prev_hash": prev}
        event_hash = _hash(core)
        event_id = f"xdev_{event_hash[:24]}"
        conn.execute(
            f"INSERT INTO {TABLE} (seq, event_id, event_type, authorization_id, account_id, user_id, family_id, "
            "environment, actor_ref, recorded_at, payload, prev_hash, event_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (seq, event_id, event_type, core["authorization_id"], core["account_id"], core["user_id"],
             core["family_id"], ENVIRONMENT, actor_ref, int(now_ms), core["payload"], prev, event_hash))
        return {**core, "event_id": event_id, "event_hash": event_hash}

    def authorize(self, operator: Any, *, account_id: str, family_id: str, environment: str,
                  acknowledged_research_status: str, approval_ref: Any, reason: Any, duration_hours: Any,
                  max_entry_notional_usdt: Any, now_ms: Optional[int] = None) -> Grant:
        operator = _require_operator(operator)
        if str(environment or "").strip().upper() != ENVIRONMENT:
            raise ExperimentalDemoError(LIVE_NOT_PERMITTED, "experimental authorization exists for DEMO only")
        approval_ref = _text(approval_ref, APPROVAL_REF_REQUIRED)
        reason = _text(reason, REASON_REQUIRED)
        try:
            hours = int(duration_hours)
        except (TypeError, ValueError):
            raise ExperimentalDemoError(DURATION_INVALID, "a whole number of hours is required") from None
        if not 1 <= hours <= MAX_DURATION_HOURS:
            raise ExperimentalDemoError(DURATION_INVALID, f"1 to {MAX_DURATION_HOURS} hours")
        try:
            cap = Decimal(str(max_entry_notional_usdt))
        except (InvalidOperation, ValueError):
            raise ExperimentalDemoError(NOTIONAL_INVALID, "a number is required") from None
        if not cap.is_finite() or not Decimal("0") < cap <= MAX_ENTRY_NOTIONAL_USDT:
            raise ExperimentalDemoError(NOTIONAL_INVALID, f"in (0, {MAX_ENTRY_NOTIONAL_USDT}] USDT")
        account = verified_demo_account(self._db, account_id)
        from app.trading_intelligence.pipeline.family_registry import CERTIFIED, FamilyRegistry
        entry = FamilyRegistry(self._db).entry(family_id)
        if entry is None:
            raise ExperimentalDemoError(FAMILY_UNKNOWN, family_id)
        blocking = [b for b in ("FAMILY_NOT_REGISTERED", "FAMILY_RULE_HASH_MISMATCH", "FAMILY_DISABLED",
                                "FAMILY_KILL_SWITCH") if b in entry.blockers]
        if not entry.registered or entry.disabled or entry.kill_switch_state == "ON" or blocking:
            raise ExperimentalDemoError(FAMILY_NOT_ELIGIBLE, ",".join(blocking) or "not registered / disabled")
        if entry.certification_status == CERTIFIED and "DEMO" in entry.allowed_modes:
            raise ExperimentalDemoError(FAMILY_ALREADY_AUTHORIZED, "the family already has DEMO authority")
        if str(acknowledged_research_status or "").strip().upper() != str(entry.certification_status).upper():
            raise ExperimentalDemoError(RESEARCH_STATUS_NOT_ACKNOWLEDGED,
                                        f"the family's current research status is {entry.certification_status}")
        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        authorization_id = "xdemo_" + uuid.uuid4().hex[:20]
        scope = {"authorization_id": authorization_id, "account_id": account["id"], "user_id": account["user_id"],
                 "family_id": family_id}
        payload = {"family_rule_hash": entry.definition.definition_hash,
                   "research_status_at_grant": entry.certification_status, "stage_at_grant": entry.governance_stage,
                   "venue": str(account["broker_id"]).lower(), "base_url": account["base_url"],
                   "approval_ref": approval_ref, "reason": reason, "duration_hours": hours,
                   "max_entry_notional_usdt": str(cap), "expires_at": now + hours * 3_600_000,
                   "label": LABEL, "not_certification": True, "grants_live_authority": False}
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ensure(conn)
            if any(g.account_id == account["id"] and g.family_id == family_id and g.active(now)
                   for g in _fold(self._rows(conn)).values()):
                raise ExperimentalDemoError(ALREADY_ACTIVE, "revoke the active authorization first")
            self._append(conn, AUTHORIZED, scope, operator.actor_ref, payload, now)
            grants = _fold(self._rows(conn))
        return grants[authorization_id]

    def revoke(self, operator: Any, authorization_id: str, *, reason: Any, now_ms: Optional[int] = None) -> Grant:
        operator = _require_operator(operator)
        reason = _text(reason, REASON_REQUIRED)
        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ensure(conn)
            grant = _fold(self._rows(conn)).get(authorization_id)
            if grant is None:
                raise ExperimentalDemoError(NOT_FOUND, authorization_id)
            if grant.revoked_at is not None:
                raise ExperimentalDemoError(ALREADY_REVOKED, authorization_id)
            self._append(conn, REVOKED, {"authorization_id": grant.authorization_id, "account_id": grant.account_id,
                                         "user_id": grant.user_id, "family_id": grant.family_id},
                         operator.actor_ref, {"reason": reason}, now)
            return _fold(self._rows(conn))[authorization_id]

    def record_entry(self, grant: Grant, *, trade_plan_id: str, attempt_id: str, symbol: str, side: str,
                     notional: float, now_ms: int) -> None:
        """One ``ENTRY_CLAIMED`` event per order claimed under the grant (the boundary calls it after the claim)."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ensure(conn)
            self._append(conn, ENTRY_CLAIMED, {"authorization_id": grant.authorization_id,
                                               "account_id": grant.account_id, "user_id": grant.user_id,
                                               "family_id": grant.family_id}, "engine:execution_boundary",
                         {"trade_plan_id": trade_plan_id, "attempt_id": attempt_id, "symbol": symbol, "side": side,
                          "notional_usdt": round(float(notional), 8), "label": LABEL}, now_ms)


def plan_scope(db: Any, plan: Any, entry: Any, now_ms: int) -> Optional[Grant]:
    """The grant that authorizes this plan's family on its DEMO account, or None. Called by the family gate ONLY
    for the two checks a grant can waive (research certification, stage). Every condition is re-checked here:
    a DEMO plan, a DEMO-mode provenance, the account still a verified DEMO account of this user, the family rule
    hash unchanged since the grant, the grant active and the ledger intact."""
    from app.trading_intelligence.pipeline import contracts as PC
    p = PC.provenance_of(plan)
    if str(getattr(plan, "environment", "")).upper() != ENVIRONMENT or str(p.get("environment") or "").upper() != \
            ENVIRONMENT or p.get("mode") != ENVIRONMENT:
        return None
    family_id = p.get("family_id")
    if not family_id or entry is None:
        return None
    with db.connect() as conn:
        if not _installed(conn):
            return None
    grant = ExperimentalDemoLedger(db).active_for(account_id=str(plan.broker_account_id), user_id=str(plan.user_id),
                                                  family_id=str(family_id), now_ms=now_ms)
    if grant is None or grant.family_rule_hash != entry.definition.definition_hash:
        return None
    try:
        verified_demo_account(db, grant.account_id, grant.user_id)
    except ExperimentalDemoError:
        return None
    return grant


def entry_refusal(db: Any, authorization_id: str, notional: float, now_ms: int) -> Optional[str]:
    """None when an order of ``notional`` may be claimed under the grant now; otherwise the refusal code."""
    grant = ExperimentalDemoLedger(db).grant(authorization_id)
    if grant is None or not grant.active(now_ms):
        return GRANT_LAPSED
    ok, _ = ExperimentalDemoLedger(db).verify()
    if not ok:
        return LEDGER_INTEGRITY_FAILED
    if not (float(notional) > 0) or Decimal(str(notional)) > grant.max_entry_notional_usdt:
        return f"{NOTIONAL_EXCEEDED}:{round(float(notional), 2)}>{grant.max_entry_notional_usdt}"
    return None


def experimental_plans(db: Any, account_id: str) -> Dict[str, str]:
    """trade_plan_id -> authorization_id of every order claimed under a grant on this account (for labelling)."""
    out: Dict[str, str] = {}
    for g in ExperimentalDemoLedger(db).grants():
        if g.account_id == account_id:
            for e in g.entries:
                out[str(e.get("trade_plan_id"))] = g.authorization_id
    return out


def account_status(db: Any, account_id: str, now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """The latest grant of the account for display (active first), or None."""
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    try:
        grants = [g for g in ExperimentalDemoLedger(db).grants() if g.account_id == account_id]
    except Exception:  # noqa: BLE001 -- a display helper never fails the caller
        return None
    if not grants:
        return None
    grants.sort(key=lambda g: (g.active(now), g.granted_at), reverse=True)
    plain = grants[0].to_plain(now)
    plain.pop("entries", None)
    return plain


__all__ = ["TABLE", "LABEL", "AUTHORIZED_REASON", "MAX_DURATION_HOURS", "MAX_ENTRY_NOTIONAL_USDT",
           "ExperimentalDemoError", "ExperimentalDemoLedger", "Grant", "plan_scope", "entry_refusal",
           "experimental_plans", "account_status", "verified_demo_account", "ensure"]
