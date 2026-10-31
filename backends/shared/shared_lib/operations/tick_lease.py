"""Single ownership of a periodic operations job across processes and hosts (Section H Step 8.5).

``TickLease`` is one row of ``ops_tick_leases`` (``operations_schema`` migration 33) on the platform database:

* **one owner**: ``acquire`` grants the lease only when nobody else holds an unexpired one; time is the DATABASE
  server's clock (``conn.now_ms()``), never a host clock, so two hosts with skewed clocks still agree;
* **fencing**: every change of holder increments ``fencing_token``. ``record`` (the tick's result) and ``renew``
  succeed only for the current holder with the current token: a process that stalled past its expiry and was
  taken over can no longer write;
* **takeover after expiry**: a holder that died (crash, kill -9, lost host) is replaced once its lease expired; the
  takeover is counted. A clean stop releases the lease so a successor starts immediately;
* **persisted last tick**: ``last_tick_started_at`` / ``last_tick_finished_at`` / ``last_tick_outcome`` /
  ``last_tick_result`` survive restarts, and ``state`` carries what the job keeps between ticks (disk samples,
  pending conditions) to whichever process runs the next tick;
* **pacing**: ``min_interval_ms`` refuses a tick that would follow the last recorded tick too closely, so timers
  firing on several hosts produce one tick per interval, not one per host.

The ticks themselves must still be idempotent (they are: alert deduplication by fingerprint, delivery claims); the
lease removes duplicate work, it is not what makes a duplicate harmless.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from shared_lib.persistence.sql import Database

DEFAULT_TICK = "ops-alerting"
TICK_OK, TICK_ERROR = "OK", "ERROR"
_COLS = ("tick_name", "owner", "fencing_token", "acquired_at", "renewed_at", "expires_at", "released_at", "takeovers",
         "last_tick_started_at", "last_tick_finished_at", "last_tick_owner", "last_tick_token", "last_tick_outcome",
         "last_tick_result", "state", "updated_at")


class TickLeaseError(RuntimeError):
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


@dataclass
class Grant:
    acquired: bool
    token: Optional[int] = None
    reason: str = ""
    holder: Optional[str] = None
    expires_at: Optional[int] = None
    takeover: bool = False
    previous_owner: Optional[str] = None
    now_ms: Optional[int] = None
    state: Dict[str, Any] = field(default_factory=dict)
    #: When the previous tick (any owner) finished, database clock; None before the first. The tick's dead-man
    #: check (``app.ops.alerting``) compares it with ``now_ms``: a long gap means alerts went unevaluated.
    last_tick_finished_at: Optional[int] = None


def _json(value: Optional[str]) -> Dict[str, Any]:
    try:
        out = json.loads(value) if value else {}
    except (TypeError, ValueError):
        return {}
    return out if isinstance(out, dict) else {}


class TickLease:
    def __init__(self, db: Database, *, owner: str, name: str = DEFAULT_TICK, ttl_ms: int = 180_000) -> None:
        if not (owner or "").strip():
            raise TickLeaseError("OWNER_REQUIRED")
        if int(ttl_ms) <= 0:
            raise TickLeaseError("INVALID_TTL")
        self.db, self.owner, self.name, self.ttl_ms = db, owner.strip()[:200], name, int(ttl_ms)

    def acquire(self, *, min_interval_ms: int = 0, now_ms: Optional[int] = None) -> Grant:
        """Take (or keep) the lease. Refused with ``LEASE_HELD`` while another owner's lease is unexpired, and with
        ``TICK_NOT_DUE`` when a tick was recorded less than ``min_interval_ms`` ago by someone else (pacing)."""
        with self.db.transaction() as conn:
            now = conn.now_ms() if now_ms is None else int(now_ms)
            conn.execute("INSERT INTO ops_tick_leases (tick_name, fencing_token, expires_at, state, updated_at) "
                         "VALUES (?,0,0,'{}',?) ON CONFLICT (tick_name) DO NOTHING", (self.name, now))
            row = dict(zip(_COLS, conn.one(f"SELECT {', '.join(_COLS)} FROM ops_tick_leases WHERE tick_name=?"
                                           + conn.lock_rows_suffix(), (self.name,))))
            live = row["owner"] is not None and row["released_at"] is None and int(row["expires_at"]) > now
            mine = live and row["owner"] == self.owner
            if live and not mine:
                return Grant(False, reason="LEASE_HELD", holder=row["owner"], expires_at=int(row["expires_at"]),
                             now_ms=now)
            last = row["last_tick_finished_at"]
            if (min_interval_ms and not mine and last is not None and now - int(last) < int(min_interval_ms)):
                return Grant(False, reason="TICK_NOT_DUE", holder=row["last_tick_owner"], now_ms=now)
            expires = now + self.ttl_ms
            if mine:
                conn.execute("UPDATE ops_tick_leases SET renewed_at=?, expires_at=?, updated_at=? WHERE tick_name=? "
                             "AND owner=? AND fencing_token=?", (now, expires, now, self.name, self.owner,
                                                                 row["fencing_token"]))
                return Grant(True, int(row["fencing_token"]), "RENEWED", self.owner, expires, now_ms=now,
                             state=_json(row["state"]),
                             last_tick_finished_at=None if last is None else int(last))
            takeover = row["owner"] is not None and row["released_at"] is None      # expired, never released
            token = int(row["fencing_token"]) + 1
            conn.execute("UPDATE ops_tick_leases SET owner=?, fencing_token=?, acquired_at=?, renewed_at=?, "
                         "expires_at=?, released_at=NULL, takeovers=takeovers+?, updated_at=? WHERE tick_name=?",
                         (self.owner, token, now, now, expires, 1 if takeover else 0, now, self.name))
        return Grant(True, token, "TAKEN_OVER" if takeover else "ACQUIRED", self.owner, expires, takeover,
                     row["owner"], now, _json(row["state"]), None if last is None else int(last))

    def renew(self, token: int, *, now_ms: Optional[int] = None) -> bool:
        with self.db.transaction() as conn:
            now = conn.now_ms() if now_ms is None else int(now_ms)
            cur = conn.execute("UPDATE ops_tick_leases SET renewed_at=?, expires_at=?, updated_at=? WHERE tick_name=? "
                               "AND owner=? AND fencing_token=? AND released_at IS NULL",
                               (now, now + self.ttl_ms, now, self.name, self.owner, int(token)))
            return cur.rowcount == 1

    def record(self, token: int, *, started_at_ms: int, outcome: str, result: Dict[str, Any],
               state: Dict[str, Any], now_ms: Optional[int] = None) -> bool:
        """Persist a finished tick (and renew). False: this process no longer holds the lease (fenced)."""
        with self.db.transaction() as conn:
            now = conn.now_ms() if now_ms is None else int(now_ms)
            cur = conn.execute(
                "UPDATE ops_tick_leases SET last_tick_started_at=?, last_tick_finished_at=?, last_tick_owner=?, "
                "last_tick_token=?, last_tick_outcome=?, last_tick_result=?, state=?, renewed_at=?, expires_at=?, "
                "updated_at=? WHERE tick_name=? AND owner=? AND fencing_token=? AND released_at IS NULL",
                (int(started_at_ms), now, self.owner, int(token), outcome,
                 json.dumps(result, sort_keys=True, default=str)[:60_000], json.dumps(state, sort_keys=True, default=str),
                 now, now + self.ttl_ms, now, self.name, self.owner, int(token)))
            return cur.rowcount == 1

    def release(self, token: int, *, now_ms: Optional[int] = None) -> bool:
        with self.db.transaction() as conn:
            now = conn.now_ms() if now_ms is None else int(now_ms)
            cur = conn.execute("UPDATE ops_tick_leases SET released_at=?, expires_at=?, updated_at=? WHERE tick_name=? "
                               "AND owner=? AND fencing_token=? AND released_at IS NULL",
                               (now, now, now, self.name, self.owner, int(token)))
            return cur.rowcount == 1

    def status(self, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
        """The lease and the last recorded tick, for operators (``/admin/operations`` or the CLI)."""
        with self.db.transaction(write=False) as conn:
            now = conn.now_ms() if now_ms is None else int(now_ms)
            row = conn.one(f"SELECT {', '.join(_COLS)} FROM ops_tick_leases WHERE tick_name=?", (self.name,))
        if row is None:
            return {"tick_name": self.name, "held": False, "owner": None, "last_tick_finished_at": None,
                    "last_tick_age_ms": None}
        out = dict(zip(_COLS, row))
        out["last_tick_result"] = _json(out["last_tick_result"])
        out.pop("state", None)
        out["held"] = out["owner"] is not None and out["released_at"] is None and int(out["expires_at"]) > now
        finished = out["last_tick_finished_at"]
        out["last_tick_age_ms"] = None if finished is None else now - int(finished)
        return out
