"""Section H: tables of the daily bar feed of the platform cycle driver (Step 3.1 market data, Step 3.12).

Part of the CATI platform schema; see ``cati_platform_schema`` for the rules every statement here follows
(additive, idempotent, ``IF NOT EXISTS``, no existing table rewritten).

The feed (``app.market_data.daily_feed``) is production market data. It is not the research dataset and shares
no storage with it: nothing here is read by the research evaluator, and the research dataset store is never
read or written by the feed.

Market data is evidence of what a decision saw, so the three tables that hold it are append-only: a daily bar,
a funding record and an exchange-metadata snapshot are stored once and never changed. A later download that
disagrees with a stored bar or funding record is written to ``cati_feed_revisions`` and is NOT applied.

``seq`` is the order in which rows were committed. A decision day is *sealed* the first time it is served: the
seal records the highest ``seq`` of each table at that moment, and the day is served from rows at or below
those marks from then on, so a replayed cycle reads exactly the data the first run read, whatever arrived
later.
"""
from __future__ import annotations

from typing import Any, Tuple

from shared_lib.persistence.cati_platform_schema import append_only_triggers

FEED_BAR_TABLE = "cati_feed_daily_bars"
FEED_FUNDING_TABLE = "cati_feed_funding"
FEED_METADATA_TABLE = "cati_feed_metadata"
FEED_REJECTION_TABLE = "cati_feed_rejections"
FEED_REVISION_TABLE = "cati_feed_revisions"
FEED_SYMBOL_TABLE = "cati_feed_symbols"
FEED_UPDATE_TABLE = "cati_feed_updates"
FEED_SEAL_TABLE = "cati_feed_sealed_days"
FEED_STATE_TABLE = "cati_feed_state"

DDL: Tuple[str, ...] = (
    # One closed, valid, traded daily bar per (symbol, day). ``day`` is days since 1970-01-01 (UTC).
    f"""CREATE TABLE IF NOT EXISTS {FEED_BAR_TABLE} (
        seq INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        day INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        quote_volume REAL NOT NULL,
        trades INTEGER NOT NULL,
        update_id INTEGER NOT NULL,
        stored_at INTEGER NOT NULL,
        UNIQUE(symbol, day))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_feed_bar_day ON {FEED_BAR_TABLE}(day, symbol)",
    *append_only_triggers(FEED_BAR_TABLE, "cati_feed_bar"),
    # One funding event per (symbol, time). A missing event is a missing row: never a zero rate.
    f"""CREATE TABLE IF NOT EXISTS {FEED_FUNDING_TABLE} (
        seq INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        time_ms INTEGER NOT NULL,
        interval_hours INTEGER NOT NULL,
        rate REAL NOT NULL,
        update_id INTEGER NOT NULL,
        stored_at INTEGER NOT NULL,
        UNIQUE(symbol, time_ms))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_feed_fund_time ON {FEED_FUNDING_TABLE}(time_ms)",
    *append_only_triggers(FEED_FUNDING_TABLE, "cati_feed_fund"),
    # Exchange metadata snapshots, one row per distinct content. A snapshot says what the filters are now.
    f"""CREATE TABLE IF NOT EXISTS {FEED_METADATA_TABLE} (
        seq INTEGER PRIMARY KEY,
        content_hash TEXT NOT NULL UNIQUE,
        fetched_at INTEGER NOT NULL,
        symbols INTEGER NOT NULL,
        document TEXT NOT NULL,
        stored_at INTEGER NOT NULL)""",
    *append_only_triggers(FEED_METADATA_TABLE, "cati_feed_meta"),
    # A symbol's day the source answered with something that is not a bar: no trades, or an invalid row. It
    # stays rejected (the research dataset quarantines the same rows and never corrects them).
    f"""CREATE TABLE IF NOT EXISTS {FEED_REJECTION_TABLE} (
        seq INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        day INTEGER NOT NULL,
        reason TEXT NOT NULL,
        detail TEXT NOT NULL DEFAULT '{{}}',
        update_id INTEGER NOT NULL,
        recorded_at INTEGER NOT NULL,
        UNIQUE(symbol, day))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_feed_rej_day ON {FEED_REJECTION_TABLE}(day)",
    *append_only_triggers(FEED_REJECTION_TABLE, "cati_feed_rej"),
    # A later download that differs from a stored bar or funding record. Recorded, never applied.
    f"""CREATE TABLE IF NOT EXISTS {FEED_REVISION_TABLE} (
        seq INTEGER PRIMARY KEY,
        kind TEXT NOT NULL,
        symbol TEXT NOT NULL,
        record_key INTEGER NOT NULL,
        stored TEXT NOT NULL,
        offered TEXT NOT NULL,
        offered_hash TEXT NOT NULL,
        applied INTEGER NOT NULL DEFAULT 0,
        update_id INTEGER NOT NULL,
        detected_at INTEGER NOT NULL,
        UNIQUE(kind, symbol, record_key, offered_hash))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_feed_rev_symbol ON {FEED_REVISION_TABLE}(symbol, record_key)",
    *append_only_triggers(FEED_REVISION_TABLE, "cati_feed_rev"),
    # Operational state of every contract the feed follows: how far its days are resolved, its last failure.
    f"""CREATE TABLE IF NOT EXISTS {FEED_SYMBOL_TABLE} (
        symbol TEXT PRIMARY KEY,
        first_seen_at INTEGER NOT NULL,
        bars_through INTEGER NOT NULL,
        funding_through INTEGER NOT NULL,
        last_bar_day INTEGER,
        last_success_at INTEGER,
        last_attempt_at INTEGER,
        failures INTEGER NOT NULL DEFAULT 0,
        last_error TEXT)""",
    # One row per update run: what was asked of the source and what came back (request volume, failures).
    f"""CREATE TABLE IF NOT EXISTS {FEED_UPDATE_TABLE} (
        update_id INTEGER PRIMARY KEY,
        status TEXT NOT NULL,
        started_at INTEGER NOT NULL,
        completed_at INTEGER,
        through_day INTEGER NOT NULL,
        symbols_tracked INTEGER NOT NULL DEFAULT 0,
        symbols_requested INTEGER NOT NULL DEFAULT 0,
        symbols_failed INTEGER NOT NULL DEFAULT 0,
        symbol_days_requested INTEGER NOT NULL DEFAULT 0,
        source_calls INTEGER NOT NULL DEFAULT 0,
        http_requests INTEGER NOT NULL DEFAULT 0,
        bars_stored INTEGER NOT NULL DEFAULT 0,
        funding_stored INTEGER NOT NULL DEFAULT 0,
        days_rejected INTEGER NOT NULL DEFAULT 0,
        revisions INTEGER NOT NULL DEFAULT 0,
        detail TEXT NOT NULL DEFAULT '{{}}')""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_feed_upd_day ON {FEED_UPDATE_TABLE}(through_day, status)",
    # The seal of a decision day: the marks below which it is served, and what it held when first served.
    f"""CREATE TABLE IF NOT EXISTS {FEED_SEAL_TABLE} (
        day INTEGER PRIMARY KEY,
        first_day INTEGER NOT NULL,
        bar_seq INTEGER NOT NULL,
        funding_seq INTEGER NOT NULL,
        rejection_seq INTEGER NOT NULL,
        metadata_seq INTEGER NOT NULL,
        bars INTEGER NOT NULL,
        funding_records INTEGER NOT NULL,
        symbols INTEGER NOT NULL,
        freshness TEXT NOT NULL,
        sealed_at INTEGER NOT NULL)""",
    *append_only_triggers(FEED_SEAL_TABLE, "cati_feed_seal"),
    f"""CREATE TABLE IF NOT EXISTS {FEED_STATE_TABLE} (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL)""",
)


def upgrade(conn: Any) -> None:
    """Additive column changes of this module's tables. None yet."""


__all__ = ["DDL", "upgrade", "FEED_BAR_TABLE", "FEED_FUNDING_TABLE", "FEED_METADATA_TABLE", "FEED_REJECTION_TABLE",
           "FEED_REVISION_TABLE", "FEED_SYMBOL_TABLE", "FEED_UPDATE_TABLE", "FEED_SEAL_TABLE", "FEED_STATE_TABLE"]
