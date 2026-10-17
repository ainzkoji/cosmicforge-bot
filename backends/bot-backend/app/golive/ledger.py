"""The go-live authorization ledger (Section H, Step 6.1).

One append-only, hash-chained event log (``golive_events``) holds every decision that can open live trading --
operator live switches, account approvals, customer live consents, demo-run evidence, beta invitations and
enrolments -- and every revocation of one. State is the fold of events; nothing is a mutable flag.

**Separation of duties.** Turning an exchange's live switch ON and granting an account approval are two-step:
one operator requests, a *different* operator approves (``SELF_APPROVAL_REFUSED``). Turning a switch OFF,
revoking an approval and invalidating evidence take one operator and are never delayed: closing is always
easier than opening.

**A restore cannot resurrect a revocation.** Every revoking event is also appended to a *revocation anchor*
outside the database (``GOLIVE_REVOCATION_ANCHOR``: an append-only file on separate storage). ``verify()``
requires every anchored revocation to be present, with the same hash, in the database ledger. A database
restored from a backup taken before a revocation is therefore detected (``LEDGER_BEHIND_REVOCATION_ANCHOR``)
and the go-live gate refuses everything until an operator reconciles it.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from shared_lib.persistence.sql import Connection, Database
from shared_lib.persistence.sql.launch_schema import GOLIVE_EVENT_TYPES, REVOKING_EVENTS

GENESIS = "0" * 64
LEDGER_VERSION = "1.0.0"
_ANCHOR_LOCK = threading.Lock()


class GoLiveError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


@dataclass(frozen=True)
class Operator:
    """An authenticated administrator. Built only by the admin API from a verified operator token."""
    operator_id: str
    channel: str

    def __post_init__(self) -> None:
        if not (self.operator_id or "").strip() or not (self.channel or "").strip():
            raise GoLiveError("OPERATOR_AUTHORIZATION_REQUIRED")

    @property
    def ref(self) -> str:
        return f"operator:{self.operator_id}@{self.channel}"


def _require_operator(op: Any) -> Operator:
    if not isinstance(op, Operator):
        raise GoLiveError("OPERATOR_AUTHORIZATION_REQUIRED")
    return op


def event_hash(prev: str, event_id: str, event_type: str, scope_type: str, scope_id: str, subject: Optional[str],
               actor: str, payload: str, at: int) -> str:
    body = "\x1f".join([prev, event_id, event_type, scope_type, scope_id, subject or "", actor, payload, str(at)])
    return hashlib.sha256(body.encode()).hexdigest()


@dataclass(frozen=True)
class Event:
    seq: int
    event_id: str
    event_type: str
    scope_type: str
    scope_id: str
    subject_ref: Optional[str]
    actor_ref: str
    payload: Dict[str, Any]
    recorded_at: int
    prev_hash: str
    hash: str


class GoLiveLedger:
    def __init__(self, db: Database, *, anchor_path: Optional[str] = None) -> None:
        self.db = db
        self.anchor_path = anchor_path if anchor_path is not None else os.environ.get("GOLIVE_REVOCATION_ANCHOR")

    # ── writing ────────────────────────────────────────────────────────────────────────────────────────
    def append(self, event_type: str, *, scope_type: str, scope_id: str, actor_ref: str,
               payload: Optional[Mapping[str, Any]] = None, subject_ref: Optional[str] = None,
               conn: Optional[Connection] = None) -> Event:
        if event_type not in GOLIVE_EVENT_TYPES:
            raise GoLiveError("GOLIVE_EVENT_TYPE_UNKNOWN", event_type)
        if event_type in REVOKING_EVENTS and not self.anchor_path:
            # a revocation that could be undone by a restore is not a revocation
            raise GoLiveError("REVOCATION_ANCHOR_NOT_CONFIGURED")
        body = json.dumps(dict(payload or {}), sort_keys=True, default=str)
        if conn is None:
            with self.db.transaction() as c:
                ev = self._append_on(c, event_type, scope_type, scope_id, actor_ref, body, subject_ref)
        else:
            ev = self._append_on(conn, event_type, scope_type, scope_id, actor_ref, body, subject_ref)
        if event_type in REVOKING_EVENTS:
            self._anchor(ev)
        return ev

    def _append_on(self, conn, event_type, scope_type, scope_id, actor_ref, body, subject_ref) -> Event:
        if conn.dialect == "postgresql":
            conn.execute("SELECT pg_advisory_xact_lock(?)", (0x0C05_601E,))   # one chain, one writer at a time
        prev = conn.scalar("SELECT hash FROM golive_events ORDER BY seq DESC LIMIT 1") or GENESIS
        at = conn.now_ms()
        event_id = uuid.uuid4().hex
        h = event_hash(prev, event_id, event_type, scope_type, scope_id, subject_ref, actor_ref, body, at)
        conn.execute("INSERT INTO golive_events (event_id, event_type, scope_type, scope_id, subject_ref, actor_ref, payload, "
                     "recorded_at, prev_hash, hash) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (event_id, event_type, scope_type, scope_id, subject_ref, actor_ref, body, at, prev, h))
        seq = int(conn.scalar("SELECT seq FROM golive_events WHERE event_id=?", (event_id,)))
        return Event(seq, event_id, event_type, scope_type, scope_id, subject_ref, actor_ref, json.loads(body), at, prev, h)

    def _anchor(self, ev: Event) -> None:
        line = json.dumps({"seq": ev.seq, "hash": ev.hash, "event_type": ev.event_type, "scope_type": ev.scope_type,
                           "scope_id": ev.scope_id, "recorded_at": ev.recorded_at}, sort_keys=True)
        with _ANCHOR_LOCK:
            path = Path(self.anchor_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())

    # ── reading ────────────────────────────────────────────────────────────────────────────────────────
    def events(self, *, scope_type: Optional[str] = None, scope_id: Optional[str] = None) -> List[Event]:
        sql = ("SELECT seq, event_id, event_type, scope_type, scope_id, subject_ref, actor_ref, payload, recorded_at, "
               "prev_hash, hash FROM golive_events")
        where, params = [], []
        if scope_type:
            where.append("scope_type=?")
            params.append(scope_type)
        if scope_id:
            where.append("scope_id=?")
            params.append(scope_id)
        if where:
            sql += " WHERE " + " AND ".join(where)
        with self.db.transaction(write=False) as conn:
            rows = conn.all(sql + " ORDER BY seq", params)
        return [Event(int(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], json.loads(r[7]), int(r[8]), r[9], r[10])
                for r in rows]

    def verify(self) -> Tuple[bool, str]:
        """Chain intact, and every anchored revocation present. ``(True, "")`` or ``(False, reason)``."""
        prev = GENESIS
        by_seq: Dict[int, Event] = {}
        for e in self.events():
            expected = event_hash(prev, e.event_id, e.event_type, e.scope_type, e.scope_id, e.subject_ref, e.actor_ref,
                                  json.dumps(e.payload, sort_keys=True, default=str), e.recorded_at)
            if e.prev_hash != prev or e.hash != expected:
                return False, f"LEDGER_CHAIN_BROKEN:seq={e.seq}"
            prev = e.hash
            by_seq[e.seq] = e
        if not self.anchor_path:
            return False, "REVOCATION_ANCHOR_NOT_CONFIGURED"
        path = Path(self.anchor_path)
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                a = json.loads(line)
                e = by_seq.get(int(a["seq"]))
                if e is None or e.hash != a["hash"]:
                    return False, f"LEDGER_BEHIND_REVOCATION_ANCHOR:seq={a['seq']}:{a['event_type']}"
        return True, ""


# ── folds ─────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class SwitchState:
    venue: str
    on: bool = False
    pending_request: Optional[Event] = None
    last_change: Optional[Event] = None


def fold_switch(events: Iterable[Event], venue: str) -> SwitchState:
    st = SwitchState(venue)
    for e in events:
        if e.scope_type != "VENUE" or e.scope_id != venue:
            continue
        if e.event_type == "SWITCH_ON_REQUESTED":
            st.pending_request = e
        elif e.event_type == "SWITCH_ON_APPROVED" and st.pending_request is not None \
                and e.payload.get("request_event_id") == st.pending_request.event_id:
            st.on, st.pending_request, st.last_change = True, None, e
        elif e.event_type == "SWITCH_ON_REJECTED":
            st.pending_request = None
        elif e.event_type == "SWITCH_OFF":
            st.on, st.pending_request, st.last_change = False, None, e
    return st


@dataclass
class ApprovalState:
    approval_id: str
    account_id: str
    family_id: str
    venue: str
    requested: Optional[Event] = None
    granted: Optional[Event] = None
    revoked: Optional[Event] = None
    rejected: Optional[Event] = None

    def active(self, now_ms: int) -> bool:
        if self.granted is None or self.revoked is not None or self.rejected is not None:
            return False
        exp = self.requested.payload.get("expires_at_ms") if self.requested else None
        return exp is None or int(exp) > int(now_ms)


def fold_approvals(events: Iterable[Event]) -> Dict[str, ApprovalState]:
    out: Dict[str, ApprovalState] = {}
    for e in events:
        if e.scope_type != "APPROVAL":
            continue
        if e.event_type == "APPROVAL_REQUESTED":
            p = e.payload
            out[e.scope_id] = ApprovalState(e.scope_id, p["account_id"], p["family_id"], p["venue"], requested=e)
        elif e.scope_id in out:
            st = out[e.scope_id]
            if e.event_type == "APPROVAL_GRANTED" and st.granted is None and st.rejected is None:
                st.granted = e
            elif e.event_type == "APPROVAL_REJECTED" and st.granted is None:
                st.rejected = e
            elif e.event_type == "APPROVAL_REVOKED":
                st.revoked = st.revoked or e
    return out


@dataclass
class ConsentState:
    account_id: str
    user_id: str
    consent_version: Optional[str] = None
    given: Optional[Event] = None
    revoked: Optional[Event] = None

    @property
    def active(self) -> bool:
        return self.given is not None and self.revoked is None


def fold_consent(events: Iterable[Event], account_id: str) -> ConsentState:
    st = ConsentState(account_id, "")
    for e in events:
        if e.scope_type != "ACCOUNT" or e.scope_id != account_id:
            continue
        if e.event_type == "CONSENT_GIVEN":
            st = ConsentState(account_id, e.payload.get("user_id", ""), e.payload.get("consent_version"), given=e)
        elif e.event_type == "CONSENT_REVOKED" and st.given is not None:
            st.revoked = e
    return st


@dataclass
class DemoEvidence:
    family_id: str
    family_version: str
    runs: List[Event] = field(default_factory=list)
    invalidated: set = field(default_factory=set)

    def valid_runs(self) -> List[Event]:
        return [r for r in self.runs if r.event_id not in self.invalidated]


def fold_demo(events: Iterable[Event], family_id: str, family_version: str) -> DemoEvidence:
    st = DemoEvidence(family_id, family_version)
    for e in events:
        if e.scope_type != "FAMILY" or e.scope_id != family_id:
            continue
        if e.event_type == "DEMO_RUN_RECORDED" and e.payload.get("family_version") == family_version:
            st.runs.append(e)
        elif e.event_type == "DEMO_RUN_INVALIDATED":
            st.invalidated.add(e.payload.get("run_event_id"))
    return st


def latest_policy(events: Iterable[Event]) -> Dict[str, Any]:
    policy: Dict[str, Any] = {}
    for e in events:
        if e.event_type == "POLICY_SET" and e.scope_type == "POLICY":
            policy[e.scope_id] = e.payload.get("value")
    return policy
