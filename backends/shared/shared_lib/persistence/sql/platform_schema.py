"""The platform schema: every Step 5-8 table, as ordered versioned migrations (Section H, Step 5.1).

Templates use ``{MONEY}`` (``NUMERIC(38,18)`` / exact TEXT), ``{BIGINT}``, ``{ID}`` (auto-increment key) and
``{JSON}``; ``render`` expands them per dialect. Every migration here is additive (new tables and indexes)
and therefore reversible by dropping what it created; none is marked irreversible.

Ownership columns are explicit: every per-customer row carries ``tenant_id`` and a foreign key to the account
or user it belongs to, so a query that forgets the tenant filter is a visible defect, not a silent leak.
"""
from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

from .database import POSTGRESQL, SQLITE
from .migrations import Migration

_TYPES = {
    SQLITE: {"MONEY": "TEXT", "BIGINT": "INTEGER", "ID": "INTEGER PRIMARY KEY AUTOINCREMENT", "JSON": "TEXT"},
    POSTGRESQL: {"MONEY": "NUMERIC(38,18)", "BIGINT": "BIGINT", "ID": "BIGSERIAL PRIMARY KEY", "JSON": "TEXT"},
}


def render(statements: Sequence[str]) -> Dict[str, Tuple[str, ...]]:
    return {d: tuple(s.format(**t) for s in statements) for d, t in _TYPES.items()}


def _in(*values: str) -> str:
    return "(" + ",".join(f"'{v}'" for v in values) + ")"


def _drops(*tables: str) -> Tuple[str, ...]:
    return tuple(f"DROP TABLE IF EXISTS {t}" for t in tables)


# ── 1. workers and account assignment (5.2) ─────────────────────────────────────────────────────────────────
WORKER_STATES = ("ACTIVE", "DRAINING", "STOPPED")
ACCOUNT_STATES = ("ACTIVE", "PAUSED", "REMOVED")

V1 = [
    "CREATE TABLE platform_workers (worker_id TEXT PRIMARY KEY, role TEXT NOT NULL, host TEXT NOT NULL, pid INTEGER NOT NULL, "
    "build_version TEXT NOT NULL, started_at {BIGINT} NOT NULL, heartbeat_at {BIGINT} NOT NULL, "
    f"state TEXT NOT NULL CHECK (state IN {_in(*WORKER_STATES)}), stopped_at {{BIGINT}})",
    "CREATE TABLE platform_accounts (account_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, venue TEXT NOT NULL, "
    f"environment TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN {_in(*ACCOUNT_STATES)}), created_at {{BIGINT}} NOT NULL)",
    "CREATE INDEX ix_platform_accounts_tenant ON platform_accounts (tenant_id)",
    "CREATE TABLE platform_account_assignments (account_id TEXT PRIMARY KEY REFERENCES platform_accounts(account_id), "
    "worker_id TEXT REFERENCES platform_workers(worker_id), fencing_token {BIGINT} NOT NULL CHECK (fencing_token >= 0), "
    "acquired_at {BIGINT}, renewed_at {BIGINT}, expires_at {BIGINT}, released_at {BIGINT}, release_reason TEXT, "
    "last_success_at {BIGINT}, recovered_token {BIGINT})",
    "CREATE INDEX ix_platform_assignments_worker ON platform_account_assignments (worker_id, expires_at)",
    "CREATE TABLE platform_assignment_history (id {ID}, account_id TEXT NOT NULL REFERENCES platform_accounts(account_id), "
    "from_worker TEXT, to_worker TEXT, fencing_token {BIGINT} NOT NULL, reason TEXT NOT NULL, at {BIGINT} NOT NULL)",
]

# ── 2. durable job queue (5.3) ──────────────────────────────────────────────────────────────────────────────
JOB_STATES = ("PENDING", "CLAIMED", "SUCCEEDED", "RETRY", "DEAD", "CANCELLED")
JOB_CATEGORIES = ("ORDER", "RECONCILIATION", "NOTIFICATION", "SHADOW_COMPARISON", "OPERATIONAL_RETRY")
INTENT_STATES = ("PENDING", "SUBMITTING", "ACKNOWLEDGED", "REJECTED", "NOT_SUBMITTED")

V2 = [
    "CREATE TABLE platform_jobs (job_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, "
    f"category TEXT NOT NULL CHECK (category IN {_in(*JOB_CATEGORIES)}), payload_version INTEGER NOT NULL, payload {{JSON}} NOT NULL, "
    "account_id TEXT REFERENCES platform_accounts(account_id), tenant_id TEXT, created_at {BIGINT} NOT NULL, "
    "eligible_at {BIGINT} NOT NULL, priority INTEGER NOT NULL DEFAULT 0, "
    f"status TEXT NOT NULL CHECK (status IN {_in(*JOB_STATES)}), lease_owner TEXT, lease_token {{BIGINT}} NOT NULL DEFAULT 0, "
    "lease_expires_at {BIGINT}, attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL CHECK (max_attempts > 0), "
    "next_attempt_at {BIGINT} NOT NULL, completed_at {BIGINT}, completion_evidence {JSON}, failure_reason TEXT, "
    "dead_lettered_at {BIGINT}, CHECK (account_id IS NULL OR tenant_id IS NOT NULL))",
    "CREATE INDEX ix_platform_jobs_due ON platform_jobs (status, next_attempt_at, priority)",
    "CREATE TABLE platform_job_attempts (id {ID}, job_id TEXT NOT NULL REFERENCES platform_jobs(job_id), attempt INTEGER NOT NULL, "
    "lease_owner TEXT NOT NULL, lease_token {BIGINT} NOT NULL, started_at {BIGINT} NOT NULL, finished_at {BIGINT}, outcome TEXT, "
    "detail TEXT)",
    # The queue-side delivery ledger of an order: written (SUBMITTING) and committed before the venue call, so a
    # lost acknowledgement is recovered by reading the venue by client order id, never by a blind resubmission.
    "CREATE TABLE platform_order_intents (intent_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, "
    "account_id TEXT NOT NULL REFERENCES platform_accounts(account_id), tenant_id TEXT NOT NULL, source_intent_id TEXT, "
    "venue TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL CHECK (side IN ('BUY','SELL')), "
    "quantity {MONEY} NOT NULL, limit_price {MONEY}, client_order_id TEXT NOT NULL UNIQUE, "
    f"status TEXT NOT NULL CHECK (status IN {_in(*INTENT_STATES)}), venue_order_id TEXT, fencing_token {{BIGINT}} NOT NULL, "
    "submit_attempts INTEGER NOT NULL DEFAULT 0, created_at {BIGINT} NOT NULL, updated_at {BIGINT} NOT NULL, detail TEXT)",
]

# ── 3. platform order coordination (5.5) ────────────────────────────────────────────────────────────────────
#: REQUESTED -> (allocation) RESERVED | DEFERRED; RESERVED -> SUBMITTED -> FILLED; any active -> RELEASED | EXPIRED
SLOT_STATES = ("REQUESTED", "RESERVED", "DEFERRED", "SUBMITTED", "FILLED", "RELEASED", "EXPIRED")
ACTIVE_SLOT_STATES = ("RESERVED", "SUBMITTED", "FILLED")

V3 = [
    "CREATE TABLE platform_symbol_budgets (budget_id TEXT PRIMARY KEY, venue TEXT NOT NULL, symbol TEXT NOT NULL, "
    "signal_id TEXT NOT NULL, side TEXT NOT NULL CHECK (side IN ('BUY','SELL')), cap_notional {MONEY} NOT NULL, "
    "calibrated INTEGER NOT NULL CHECK (calibrated IN (0,1)), policy_version TEXT NOT NULL, depth_hash TEXT NOT NULL, "
    "depth_observed_at {BIGINT} NOT NULL, reference_price {MONEY} NOT NULL, worst_price {MONEY} NOT NULL, "
    "approval_ref TEXT, created_at {BIGINT} NOT NULL, expires_at {BIGINT} NOT NULL, allocated_at {BIGINT}, "
    "CHECK (calibrated = 0 OR approval_ref IS NOT NULL), UNIQUE (venue, symbol, signal_id, side))",
    "CREATE INDEX ix_platform_budgets_symbol ON platform_symbol_budgets (venue, symbol, side, expires_at)",
    "CREATE TABLE platform_order_slots (slot_id TEXT PRIMARY KEY, budget_id TEXT NOT NULL REFERENCES platform_symbol_budgets(budget_id), "
    "account_id TEXT NOT NULL REFERENCES platform_accounts(account_id), tenant_id TEXT NOT NULL, sequence INTEGER NOT NULL, "
    "requested_notional {MONEY} NOT NULL, granted_notional {MONEY} NOT NULL, limit_price {MONEY}, "
    f"client_order_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL CHECK (status IN {_in(*SLOT_STATES)}), "
    "scheduled_at {BIGINT} NOT NULL, expires_at {BIGINT} NOT NULL, created_at {BIGINT} NOT NULL, updated_at {BIGINT} NOT NULL, "
    "rank_key TEXT NOT NULL, reason TEXT, UNIQUE (budget_id, account_id))",
]

STEP5_MIGRATIONS: List[Migration] = [
    Migration(1, "workers_and_account_assignments", render(V1),
              down=_drops("platform_assignment_history", "platform_account_assignments", "platform_accounts", "platform_workers")),
    Migration(2, "durable_job_queue", render(V2),
              down=_drops("platform_order_intents", "platform_job_attempts", "platform_jobs")),
    Migration(3, "platform_order_coordination", render(V3),
              down=_drops("platform_order_slots", "platform_symbol_budgets")),
]


#: Per-area migration modules and their reserved version ranges. Each module exposes ``MIGRATIONS``; a
#: version outside the module's range is refused, so independent work never collides on a number.
MIGRATION_MODULES = (
    ("launch_schema", 10, 19),          # Step 6: go-live authorization, beta stages, incidents
    ("track_record_schema", 20, 29),    # Steps 6.4 / 8.6: performance recorder and publication
    ("operations_schema", 30, 39),      # Step 8.5: alerting, on-call, status page, support desk
)


def all_migrations() -> List[Migration]:
    """Every platform migration in version order."""
    import importlib
    found: List[Migration] = list(STEP5_MIGRATIONS)
    for name, lo, hi in MIGRATION_MODULES:
        try:
            module = importlib.import_module(f"{__package__}.{name}")
        except ModuleNotFoundError as exc:
            if exc.name != f"{__package__}.{name}":
                raise
            continue
        for m in module.MIGRATIONS:
            if not lo <= m.version <= hi:
                raise ValueError(f"MIGRATION_VERSION_OUT_OF_RANGE:{name}:{m.version}")
            found.append(m)
    return sorted(found, key=lambda m: m.version)
