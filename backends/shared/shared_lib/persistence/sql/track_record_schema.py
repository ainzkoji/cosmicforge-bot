"""Track-record tables (Section H, Steps 6.4 and 8.6): the daily performance ledger and its publications.

Reserved migration range 20-29 (``platform_schema.MIGRATION_MODULES``). Every migration here is additive and
reversible by dropping what it created.

``track_record_daily`` is **append-only**. A correction is a new row whose ``supersedes_record_id`` names the
row it replaces; the original is never updated or deleted. This is enforced three ways: the recorder has no
update path, the schema refuses ``UPDATE``/``DELETE`` with a trigger on both dialects, and every row carries a
provenance hash chained to the previous row of the same series, so an out-of-band edit is detectable.

``track_record_publications`` holds the exported, aggregated documents that the public API serves. The public
API reads only this table, never ``track_record_daily``; it is append-only as well (a re-publication is a new
row and the newest one per ``series_id`` is current).

Nothing here stores an account id, a user id, an email or a credential: a daily row identifies its account
only by ``account_ref``, a keyed HMAC pseudonym computed by the recorder.
"""
from __future__ import annotations

from typing import Dict, List, Tuple

from .database import POSTGRESQL, SQLITE
from .migrations import Migration
from .platform_schema import render

SOURCE_KINDS = ("BACKTEST", "RESEARCH_HOLDOUT", "PAPER", "DEMO", "LIVE")
SIMULATED_SOURCE_KINDS = ("BACKTEST", "RESEARCH_HOLDOUT")
ACCOUNT_CLASSES = ("OPERATOR", "BETA_INVITED", "CUSTOMER", "SIMULATION")
RECONCILIATION_STATUSES = ("RECONCILED", "MISMATCH", "PENDING")


def _in(*values: str) -> str:
    return "(" + ",".join(f"'{v}'" for v in values) + ")"


V20 = [
    "CREATE TABLE track_record_daily (record_id TEXT PRIMARY KEY, series_key TEXT NOT NULL, seq INTEGER NOT NULL CHECK (seq >= 1), "
    "trading_date TEXT NOT NULL, venue TEXT NOT NULL, family_id TEXT NOT NULL, family_version TEXT NOT NULL, "
    f"account_class TEXT NOT NULL CHECK (account_class IN {_in(*ACCOUNT_CLASSES)}), risk_level TEXT NOT NULL, "
    f"source_kind TEXT NOT NULL CHECK (source_kind IN {_in(*SOURCE_KINDS)}), "
    "starting_equity {MONEY} NOT NULL, ending_equity {MONEY} NOT NULL, deposits {MONEY} NOT NULL, withdrawals {MONEY} NOT NULL, "
    "fees {MONEY} NOT NULL, funding {MONEY} NOT NULL, slippage {MONEY}, realized_pnl {MONEY} NOT NULL, "
    "unrealized_pnl {MONEY} NOT NULL, drawdown {MONEY} NOT NULL, gross_exposure {MONEY} NOT NULL, "
    f"reconciliation_status TEXT NOT NULL CHECK (reconciliation_status IN {_in(*RECONCILIATION_STATUSES)}), "
    "outage_minutes INTEGER NOT NULL CHECK (outage_minutes >= 0 AND outage_minutes <= 1440), "
    "strategy_suspended INTEGER NOT NULL CHECK (strategy_suspended IN (0,1)), amendment_note TEXT, "
    "account_ref TEXT NOT NULL, evidence_ref TEXT NOT NULL, authorization_ref TEXT, "
    "publishable INTEGER NOT NULL CHECK (publishable IN (0,1)), "
    "supersedes_record_id TEXT UNIQUE REFERENCES track_record_daily(record_id), "
    "prev_hash TEXT NOT NULL, row_hash TEXT NOT NULL UNIQUE, recorded_at {BIGINT} NOT NULL, "
    "UNIQUE (series_key, seq), "
    "CHECK (source_kind <> 'LIVE' OR authorization_ref IS NOT NULL), "
    "CHECK (source_kind <> 'LIVE' OR publishable = 0 OR reconciliation_status = 'RECONCILED'), "
    "CHECK (publishable = 0 OR reconciliation_status <> 'MISMATCH'), "
    "CHECK (supersedes_record_id IS NULL OR amendment_note IS NOT NULL), "
    f"CHECK ((source_kind IN {_in(*SIMULATED_SOURCE_KINDS)}) = (account_class = 'SIMULATION')))",
    "CREATE INDEX ix_track_record_daily_series ON track_record_daily (source_kind, family_id, venue, trading_date)",
    "CREATE TABLE track_record_publications (publication_id TEXT PRIMARY KEY, series_id TEXT NOT NULL, "
    f"source_kind TEXT NOT NULL CHECK (source_kind IN {_in(*SOURCE_KINDS)}), family_id TEXT NOT NULL, "
    "methodology_version TEXT NOT NULL, document {JSON} NOT NULL, document_hash TEXT NOT NULL, "
    "chain_head_hash TEXT NOT NULL, row_count INTEGER NOT NULL CHECK (row_count >= 0), published_at {BIGINT} NOT NULL)",
    "CREATE INDEX ix_track_record_publications_series ON track_record_publications (series_id, published_at)",
]

_APPEND_ONLY_TABLES = ("track_record_daily", "track_record_publications")


def _append_only_sqlite() -> Tuple[str, ...]:
    out = []
    for t in _APPEND_ONLY_TABLES:
        for op in ("UPDATE", "DELETE"):
            out.append(f"CREATE TRIGGER trg_{t}_no_{op.lower()} BEFORE {op} ON {t} "
                       f"BEGIN SELECT RAISE(ABORT, 'TRACK_RECORD_APPEND_ONLY'); END")
    return tuple(out)


def _append_only_postgresql() -> Tuple[str, ...]:
    out = ["CREATE FUNCTION track_record_append_only() RETURNS trigger LANGUAGE plpgsql AS $$ "
           "BEGIN RAISE EXCEPTION 'TRACK_RECORD_APPEND_ONLY'; END $$"]
    for t in _APPEND_ONLY_TABLES:
        out.append(f"CREATE TRIGGER trg_{t}_append_only BEFORE UPDATE OR DELETE ON {t} "
                   "FOR EACH ROW EXECUTE FUNCTION track_record_append_only()")
    return tuple(out)


def _up() -> Dict[str, Tuple[str, ...]]:
    tables = render(V20)
    return {SQLITE: tables[SQLITE] + _append_only_sqlite(),
            POSTGRESQL: tables[POSTGRESQL] + _append_only_postgresql()}


_DOWN = {
    SQLITE: ("DROP TABLE IF EXISTS track_record_publications", "DROP TABLE IF EXISTS track_record_daily"),
    POSTGRESQL: ("DROP TABLE IF EXISTS track_record_publications", "DROP TABLE IF EXISTS track_record_daily",
                 "DROP FUNCTION IF EXISTS track_record_append_only()"),
}

MIGRATIONS: List[Migration] = [
    Migration(20, "track_record_ledger_and_publications", _up(), down=_DOWN),
]
