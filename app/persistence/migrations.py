from app.persistence.db import DB


def _add_column_if_missing(conn, table: str, col: str, col_type: str):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {r["name"] for r in rows}
    if col not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def migrate():
    db = DB()
    with db.connect() as conn:
        # 1) Ensure table exists (pure SQL only)
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS strategy_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            symbol TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            broker_id TEXT NOT NULL,
            account_id TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            trades INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            net_pnl REAL NOT NULL DEFAULT 0,
            gross_pnl REAL NOT NULL DEFAULT 0,
            fees REAL NOT NULL DEFAULT 0,
            avg_slippage REAL NOT NULL DEFAULT 0,
            avg_r REAL NOT NULL DEFAULT 0,
            max_drawdown REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        """
        )

        # 2) Apply ALTER TABLE upgrades (Python calls happen OUTSIDE SQL strings)
        _add_column_if_missing(conn, "trade_fills", "strategy", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "strategy_version", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "timeframe", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "confidence", "REAL")
        _add_column_if_missing(conn, "trade_fills", "broker_id", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "account_id", "TEXT")
        _add_column_if_missing(conn, "trade_fills", "asset_class", "TEXT")

        # 3) Ensure signal_outcomes exists
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS signal_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            symbol TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            broker_id TEXT NOT NULL,
            account_id TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            confidence REAL NOT NULL,
            outcome INTEGER NOT NULL, -- 1 win / 0 loss
            pnl REAL NOT NULL,
            r_multiple REAL,
            opened_at TEXT,
            closed_at TEXT,
            created_at TEXT NOT NULL
        );
        """
        )
