"""Runtime job tables (Section H, Step 5.3 integration): what the production job handlers need beyond the queue.

Reserved migration range 50-59 (``platform_schema.MIGRATION_MODULES``). Every migration here is additive and
reversible by dropping what it created.

``platform_job_watermarks`` makes out-of-order delivery harmless for work that reads the exchange's current
truth (``app.platform_workers.runtime_jobs``): after a reconciliation of an account completed, the instant at
which it began reading the venue is recorded here, and a reconciliation request older than that instant is
already satisfied -- it completes as ``SUPERSEDED`` without touching the venue. The watermark only ever rises
(the upsert refuses to lower it), so a late, slow worker can never move it backwards.
"""
from __future__ import annotations

from typing import List

from .migrations import Migration
from .platform_schema import render

V50 = [
    "CREATE TABLE platform_job_watermarks (scope TEXT NOT NULL, account_id TEXT NOT NULL "
    "REFERENCES platform_accounts(account_id), tenant_id TEXT NOT NULL, watermark {BIGINT} NOT NULL, "
    "job_id TEXT NOT NULL, updated_at {BIGINT} NOT NULL, PRIMARY KEY (scope, account_id))",
]

MIGRATIONS: List[Migration] = [
    Migration(50, "runtime_job_watermarks", render(V50), down=("DROP TABLE IF EXISTS platform_job_watermarks",)),
]
