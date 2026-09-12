"""Schema for the broker-derived market universe, and the one-time bot migration.

``bot_instances.universe_mode`` says where a bot's markets come from:

* ``BROKER``    -- the connected broker account's eligible markets;
* ``ALLOWLIST`` -- an explicit user restriction to ``symbols_json``;
* ``NULL``      -- a row written by code that predates the column. The runtime
  treats it as ``ALLOWLIST`` so nothing silently widens.

The migration below resolves existing rows once. Crypto Auto Pilot bots become
``BROKER``: the deploy API only ever accepted ``symbol_universe_mode="auto"``
and filled ``symbols_json`` from the ``TRADE_SYMBOLS`` environment variable, so
their lists are provably a copied default, not a user choice. Their previous
list is kept in ``universe_mode_migrations``. Deleted bots are left alone.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

UNIVERSE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS universe_snapshots (
    snapshot_id              TEXT PRIMARY KEY,
    bot_instance_id          TEXT,
    broker_account_id        TEXT,
    run_id                   TEXT,
    runtime_session_id       TEXT,
    venue                    TEXT,
    universe_mode            TEXT NOT NULL,
    generated_at             TEXT NOT NULL,
    discovered_count         INTEGER NOT NULL DEFAULT 0,
    eligible_count           INTEGER NOT NULL DEFAULT 0,
    ranked_count             INTEGER NOT NULL DEFAULT 0,
    active_count             INTEGER NOT NULL DEFAULT 0,
    excluded_count           INTEGER NOT NULL DEFAULT 0,
    managed_count            INTEGER NOT NULL DEFAULT 0,
    excluded_by_reason_json  TEXT,
    excluded_json            TEXT,
    active_symbols_json      TEXT,
    managed_symbols_json     TEXT,
    open_symbols_json        TEXT,
    stale                    INTEGER NOT NULL DEFAULT 0,
    error                    TEXT,
    capabilities_json        TEXT,
    config_json              TEXT,
    metadata_age_seconds     REAL,
    stats_age_seconds        REAL,
    request_weight_used      INTEGER,
    request_weight_limit     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_universe_snapshots_bot
    ON universe_snapshots(bot_instance_id, generated_at);

CREATE TABLE IF NOT EXISTS universe_members (
    snapshot_id       TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    rank              INTEGER,
    canonical_id      TEXT,
    underlying        TEXT,
    quote_volume_24h  REAL,
    spread_bps        REAL,
    trade_count_24h   INTEGER,
    last_price        REAL,
    selection_reason  TEXT,
    PRIMARY KEY (snapshot_id, symbol)
);

CREATE TABLE IF NOT EXISTS universe_mode_migrations (
    bot_instance_id        TEXT PRIMARY KEY,
    migrated_at            TEXT NOT NULL,
    previous_mode          TEXT,
    new_mode               TEXT NOT NULL,
    previous_symbols_json  TEXT,
    reason                 TEXT NOT NULL
);
"""

AUTO_PILOT_REASON = "AUTO_PILOT_CRYPTO_DEPLOY_COPIED_TRADE_SYMBOLS"
EXPLICIT_REASON = "EXPLICIT_SYMBOL_LIST"
EMPTY_REASON = "NO_SYMBOL_LIST"
_IGNORED_STATUSES = frozenset({"deleted", "archived"})


def _add_column_if_missing(conn: Any, table: str, column: str, ddl: str) -> None:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def migrate_bot_universe_modes(conn: Any) -> list[dict]:
    """Resolve ``universe_mode`` for every live bot row that has none. Idempotent."""
    rows = conn.execute(
        "SELECT id, strategy_id, market_type, config_id, status, symbols_json "
        "FROM bot_instances WHERE universe_mode IS NULL"
    ).fetchall()
    now = datetime.now(timezone.utc).isoformat()
    changed: list[dict] = []
    for row in rows:
        bot_id, strategy_id, market_type, config_id, status, symbols_json = tuple(row)
        if str(status or "").lower() in _IGNORED_STATUSES:
            continue
        try:
            symbols = json.loads(symbols_json or "[]")
        except (TypeError, ValueError):
            symbols = []
        auto_pilot_crypto = (
            str(strategy_id or "") == "master_ensemble"
            and str(market_type or "").upper() == "CRYPTO"
            and str(config_id or "") in {"__auto_pilot__", ""}
        )
        if auto_pilot_crypto:
            new_mode, reason, new_symbols = "BROKER", AUTO_PILOT_REASON, []
        elif symbols:
            new_mode, reason, new_symbols = "ALLOWLIST", EXPLICIT_REASON, symbols
        else:
            new_mode, reason, new_symbols = "BROKER", EMPTY_REASON, []
        conn.execute(
            "INSERT OR IGNORE INTO universe_mode_migrations "
            "(bot_instance_id, migrated_at, previous_mode, new_mode, previous_symbols_json, reason) "
            "VALUES (?, ?, NULL, ?, ?, ?)",
            (bot_id, now, new_mode, json.dumps(symbols), reason),
        )
        conn.execute(
            "UPDATE bot_instances SET universe_mode=?, symbols_json=? WHERE id=? AND universe_mode IS NULL",
            (new_mode, json.dumps(new_symbols), bot_id),
        )
        changed.append({"bot_instance_id": bot_id, "universe_mode": new_mode, "reason": reason,
                        "previous_symbols": symbols})
    return changed


def ensure_universe_schema(db: Any) -> list[dict]:
    """Create the universe tables and column, then resolve legacy rows once."""
    with db.connect() as conn:
        conn.executescript(UNIVERSE_TABLES_SQL)
        _add_column_if_missing(conn, "bot_instances", "universe_mode", "TEXT")
        return migrate_bot_universe_modes(conn)


__all__ = [
    "AUTO_PILOT_REASON",
    "UNIVERSE_TABLES_SQL",
    "ensure_universe_schema",
    "migrate_bot_universe_modes",
]
