"""Venue-aware market-data schema (Phase 3A registry + Phase 4 datasets).

Additive and idempotent, called from ``migrations.migrate()``. The existing
``historical_candles`` table is NOT altered: Section 22 certification
datasets hash its rows, so its identity and uniqueness stay byte-identical.
New research data goes to venue-aware tables instead:

* ``venue_instruments``        discovered broker instruments (venue-global;
                               delisted rows are kept with delisted_at_ms).
* ``market_candles``           OHLCV keyed by (venue, venue_symbol,
                               product_type, timeframe, open_time, source).
                               Binance / Bybit / BingX ``BTCUSDT`` are
                               three different observations.
* ``market_feature_observations`` funding, open interest, mark/index price,
                               basis, spread, book summaries, liquidations,
                               turnover. ``status`` is AVAILABLE or
                               UNAVAILABLE -- an unavailable observation has
                               ``value IS NULL`` and a reason, never 0.
* ``fx_reference_quotes``      provider FX reference prices (bid/ask/mid,
                               REFERENCE_MARKET_PRICE -- not an execution
                               venue price).
* ``fx_reference_ingest_log``  one row per (provider, pair, timeframe, period,
                               side): FETCHED / NO_FILE / EMPTY / FAILED, so a
                               missing provider file is an explicit gap marker
                               and ingestion resumes without re-downloading.
* ``fx_reference_repairs``     append-only lineage of controlled re-ingests of
                               provider periods QA proved corrupt.
* ``market_ingest_log``        resumable per-period acquisition state for venue
                               datasets (crypto deep candles, features).
* ``dataset_manifests``        immutable research/certification manifests
                               (content-hashed; a manifest row is never
                               updated or deleted).
* ``universe_manifests``       immutable universe snapshots per role
                               (RESEARCH / TRAINING / CERTIFICATION / EXECUTION).
"""
from __future__ import annotations

from typing import Any

_DDL = (
    """
    CREATE TABLE IF NOT EXISTS venue_instruments (
        venue TEXT NOT NULL,
        environment TEXT NOT NULL,
        venue_symbol TEXT NOT NULL,
        asset_class TEXT NOT NULL,
        product_type TEXT NOT NULL,
        canonical_symbol TEXT NOT NULL,
        status TEXT,
        api_tradable INTEGER NOT NULL DEFAULT 0,
        payload_json TEXT NOT NULL,
        first_seen_ms INTEGER NOT NULL,
        last_seen_ms INTEGER NOT NULL,
        delisted_at_ms INTEGER,
        PRIMARY KEY (venue, environment, venue_symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_candles (
        venue TEXT NOT NULL,
        venue_symbol TEXT NOT NULL,
        canonical_symbol TEXT NOT NULL,
        asset_class TEXT NOT NULL,
        product_type TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        close_time INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL,
        quote_volume REAL,
        trades INTEGER,
        source TEXT NOT NULL,
        source_version TEXT,
        environment TEXT NOT NULL DEFAULT 'REAL',
        derived_from TEXT,
        ingested_at INTEGER NOT NULL,
        PRIMARY KEY (venue, venue_symbol, product_type, timeframe, open_time, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_feature_observations (
        venue TEXT NOT NULL,
        venue_symbol TEXT NOT NULL,
        canonical_symbol TEXT NOT NULL,
        asset_class TEXT NOT NULL,
        feature TEXT NOT NULL,
        observed_at INTEGER NOT NULL,
        value REAL,
        value_json TEXT,
        status TEXT NOT NULL CHECK (status IN ('AVAILABLE', 'UNAVAILABLE')),
        unavailable_reason TEXT,
        source TEXT NOT NULL,
        source_version TEXT,
        ingested_at INTEGER NOT NULL,
        CHECK (status = 'AVAILABLE' OR (value IS NULL AND unavailable_reason IS NOT NULL)),
        PRIMARY KEY (venue, venue_symbol, feature, observed_at, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fx_reference_quotes (
        provider TEXT NOT NULL,
        pair TEXT NOT NULL,
        base_currency TEXT NOT NULL,
        quote_currency TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        bid_open REAL, bid_high REAL, bid_low REAL, bid_close REAL,
        ask_open REAL, ask_high REAL, ask_low REAL, ask_close REAL,
        mid_close REAL,
        spread_close REAL,
        volume REAL,
        session TEXT,
        price_kind TEXT NOT NULL DEFAULT 'REFERENCE_MARKET_PRICE',
        source_version TEXT,
        ingested_at INTEGER NOT NULL,
        PRIMARY KEY (provider, pair, timeframe, open_time)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fx_reference_ingest_log (
        provider TEXT NOT NULL,
        pair TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        period TEXT NOT NULL,          -- UTC day (1m files) or month (1h files)
        side TEXT NOT NULL,            -- BID | ASK
        status TEXT NOT NULL CHECK (status IN ('FETCHED', 'NO_FILE', 'EMPTY', 'FAILED')),
        rows INTEGER NOT NULL DEFAULT 0,
        reason TEXT,
        recorded_at INTEGER NOT NULL,
        PRIMARY KEY (provider, pair, timeframe, period, side)
    )
    """,
    # Section 12.2 repair lineage: one append-only row per controlled re-ingest of a provider period that QA
    # proved corrupt (old state, reason, algorithm, raw-source evidence, affected rows, post-repair validation).
    """
    CREATE TABLE IF NOT EXISTS fx_reference_repairs (
        repair_id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        pair TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        period TEXT NOT NULL,
        old_status TEXT,
        reason TEXT NOT NULL,
        algorithm_version TEXT NOT NULL,
        source_evidence_json TEXT NOT NULL,
        rows_removed INTEGER NOT NULL,
        rows_inserted INTEGER NOT NULL,
        validation_json TEXT,
        recorded_at INTEGER NOT NULL
    )
    """,
    # Section 10.4/10.5: resumable acquisition state for venue datasets (crypto deep candles, supplemental
    # features) -- the venue counterpart of fx_reference_ingest_log. One row per (venue, symbol, dataset,
    # timeframe, period); FAILED periods are retried, FETCHED/EMPTY/NOT_LISTED/UNAVAILABLE are skipped.
    """
    CREATE TABLE IF NOT EXISTS market_ingest_log (
        venue TEXT NOT NULL,
        venue_symbol TEXT NOT NULL,
        dataset TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        period TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('FETCHED', 'EMPTY', 'NOT_LISTED', 'UNAVAILABLE', 'FAILED')),
        rows INTEGER NOT NULL DEFAULT 0,
        reason TEXT,
        recorded_at INTEGER NOT NULL,
        PRIMARY KEY (venue, venue_symbol, dataset, timeframe, period)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dataset_manifests (
        manifest_id TEXT PRIMARY KEY,
        manifest_hash TEXT NOT NULL UNIQUE,
        role TEXT NOT NULL,
        asset_class TEXT NOT NULL,
        venue TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS universe_manifests (
        manifest_id TEXT PRIMARY KEY,
        manifest_hash TEXT NOT NULL UNIQUE,
        role TEXT NOT NULL CHECK (role IN ('RESEARCH_UNIVERSE', 'TRAINING_UNIVERSE',
                                           'CERTIFICATION_UNIVERSE', 'EXECUTION_UNIVERSE')),
        asset_class TEXT NOT NULL,
        venue TEXT NOT NULL,
        instruments_json TEXT NOT NULL,
        selection_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )
    """,
)

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_vi_class ON venue_instruments(venue, environment, asset_class, api_tradable)",
    "CREATE INDEX IF NOT EXISTS idx_mc_canon ON market_candles(canonical_symbol, timeframe, open_time)",
    "CREATE INDEX IF NOT EXISTS idx_mfo_feature ON market_feature_observations(canonical_symbol, feature, observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_fxq_pair ON fx_reference_quotes(pair, timeframe, open_time)",
)

_IMMUTABLE = ("dataset_manifests", "universe_manifests", "fx_reference_repairs")


#: additive columns on existing tables (table, column, declaration); NULL for rows written before them
_ADDED_COLUMNS = (
    # hash of the venue's execution metadata (filters, status) at last discovery -> detects metadata changes
    ("venue_instruments", "metadata_hash", "TEXT"),
    ("venue_instruments", "metadata_changed_ms", "INTEGER"),
)


def ensure_market_data_schema(db: Any) -> None:
    with db.connect() as conn:
        for ddl in _DDL:
            conn.execute(ddl)
        for table, col, decl in _ADDED_COLUMNS:
            if col not in {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        for idx in _INDEXES:
            conn.execute(idx)
        for table in _IMMUTABLE:
            conn.execute(f"""CREATE TRIGGER IF NOT EXISTS trg_{table}_no_update BEFORE UPDATE ON {table}
                             BEGIN SELECT RAISE(ABORT, '{table} is immutable'); END""")
            conn.execute(f"""CREATE TRIGGER IF NOT EXISTS trg_{table}_no_delete BEFORE DELETE ON {table}
                             BEGIN SELECT RAISE(ABORT, '{table} is immutable'); END""")


__all__ = ["ensure_market_data_schema"]
