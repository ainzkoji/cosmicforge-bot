"""Section H: tables of account leases, emergency preemption and evidence retention (Steps 3.13, 3.14).

Part of the CATI platform schema; see ``cati_platform_schema`` for the rules every statement here follows
(additive, idempotent, ``IF NOT EXISTS``, no existing table rewritten).

Three groups, each owned by one module of ``app.trading_intelligence.pipeline``:

* ``coordination`` -- ``cati_account_leases``: the CURRENT lease of every account (one row per account). A
  lease is a claim that expires by time; ``fencing_token`` grows by one on every acquisition and never goes
  back, so work that was started under an earlier claim can be told apart from the present holder's.
* ``coordination`` -- ``cati_emergency_requests`` and ``cati_emergency_events``: the current state of every
  emergency request (who, when, which scope, why, how far it got) and the append-only log of everything that
  happened to it. The state row is a projection; the log is the evidence and is never changed.
* ``retention`` -- ``cati_retention_summaries``: what a retention run kept of the rows it removed (counts,
  errors and vetoes per day), written in the same transaction as the removal. Append-only: the summary of
  removed evidence is itself never removed. ``cati_retention_runs`` (what was exported and removed, with the
  archive hash) lives in ``cati_platform_schema``.

No table here stores a credential or a secret. An account appears only as its id.
"""
from __future__ import annotations

from typing import Any, Tuple

from shared_lib.persistence.cati_platform_schema import append_only_triggers

ACCOUNT_LEASE_TABLE = "cati_account_leases"
EMERGENCY_TABLE = "cati_emergency_requests"
EMERGENCY_EVENT_TABLE = "cati_emergency_events"
RETENTION_SUMMARY_TABLE = "cati_retention_summaries"

DDL: Tuple[str, ...] = (
    # ------------------------------------------------------------------------------- account leases (3.13)
    # ``released_at`` NULL and ``expires_at`` in the future: held. ``takeover`` says how the present holder
    # came by it: NULL (it was free), STALE (the previous holder never released it and its time ran out),
    # PREEMPTED (an emergency took it from a live ordinary holder, named in ``previous_owner``).
    f"""CREATE TABLE IF NOT EXISTS {ACCOUNT_LEASE_TABLE} (
        account_id TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        purpose TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        fencing_token INTEGER NOT NULL,
        acquired_at INTEGER NOT NULL,
        renewed_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        released_at INTEGER,
        release_reason TEXT,
        previous_owner TEXT,
        takeover TEXT,
        detail TEXT NOT NULL DEFAULT '{{}}')""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_lease_expiry ON {ACCOUNT_LEASE_TABLE}(released_at, expires_at)",
    # --------------------------------------------------------------------------- emergency requests (3.13)
    # ``hold_scope`` is the kill-switch scope this request closed to new entries (GLOBAL, or the account id);
    # ``hold_raised`` is 1 when this request had to switch it on itself (0: it was already on).
    f"""CREATE TABLE IF NOT EXISTS {EMERGENCY_TABLE} (
        request_id TEXT PRIMARY KEY,
        scope TEXT NOT NULL CHECK (scope IN ('ALL', 'ACCOUNT')),
        account_id TEXT,
        action TEXT NOT NULL DEFAULT 'FLATTEN',
        requested_by TEXT NOT NULL,
        reason TEXT NOT NULL,
        state TEXT NOT NULL,
        outcome TEXT,
        requested_at INTEGER NOT NULL,
        started_at INTEGER,
        heartbeat_at INTEGER NOT NULL,
        finished_at INTEGER,
        hold_scope TEXT NOT NULL,
        hold_raised INTEGER NOT NULL DEFAULT 0,
        detail TEXT NOT NULL DEFAULT '{{}}',
        CHECK ((scope = 'ALL' AND account_id IS NULL) OR (scope = 'ACCOUNT' AND account_id IS NOT NULL)))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_emg_state ON {EMERGENCY_TABLE}(state, heartbeat_at)",
    f"""CREATE TABLE IF NOT EXISTS {EMERGENCY_EVENT_TABLE} (
        seq INTEGER PRIMARY KEY,
        request_id TEXT NOT NULL,
        event TEXT NOT NULL,
        actor_ref TEXT NOT NULL,
        recorded_at INTEGER NOT NULL,
        detail TEXT NOT NULL DEFAULT '{{}}')""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_emgev_request ON {EMERGENCY_EVENT_TABLE}(request_id, seq)",
    *append_only_triggers(EMERGENCY_EVENT_TABLE, "cati_emgev"),
    # ------------------------------------------------------------------------- retention summaries (3.14)
    # One row per (run, day) of what the run removed. ``summary`` carries the per-status, per-reason, error
    # and veto counts the removed rows had, so the totals survive the detail.
    f"""CREATE TABLE IF NOT EXISTS {RETENTION_SUMMARY_TABLE} (
        run_id TEXT NOT NULL,
        table_name TEXT NOT NULL,
        bucket TEXT NOT NULL,
        rows INTEGER NOT NULL,
        first_at INTEGER,
        last_at INTEGER,
        summary TEXT NOT NULL,
        recorded_at INTEGER NOT NULL,
        PRIMARY KEY(run_id, bucket))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_retsum_table ON {RETENTION_SUMMARY_TABLE}(table_name, bucket)",
    *append_only_triggers(RETENTION_SUMMARY_TABLE, "cati_retsum"),
)


def upgrade(conn: Any) -> None:
    """Additive column changes of this module's tables. None yet."""


__all__ = ["DDL", "upgrade", "ACCOUNT_LEASE_TABLE", "EMERGENCY_TABLE", "EMERGENCY_EVENT_TABLE",
           "RETENTION_SUMMARY_TABLE"]
