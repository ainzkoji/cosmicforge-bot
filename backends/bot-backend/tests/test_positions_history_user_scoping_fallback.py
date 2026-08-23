import sqlite3


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE bot_instances (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL
        );

        CREATE TABLE trade_fills (
            id INTEGER PRIMARY KEY,
            user_id TEXT,
            bot_instance_id TEXT,
            position_id TEXT,
            symbol TEXT,
            side TEXT,
            action TEXT,
            qty REAL,
            price REAL,
            fee REAL,
            realized_pnl REAL,
            timestamp_utc TEXT
        );
        """
    )


def test_scope_allows_fallback_via_bot_instance_owner():
    """
    Regression test for real-world data: many trade_fills rows can have NULL/empty user_id.

    The positions/history endpoint must still return the requesting user's fills by
    scoping via bot_instances.user_id joined on trade_fills.bot_instance_id.
    """
    from app.api.analytics_reporting import _build_trade_fills_user_scope_sql

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _create_schema(conn)

    # bot_instances owned by different users
    conn.execute("INSERT INTO bot_instances (id, user_id) VALUES (?, ?)", ("bot-1", "user-123"))
    conn.execute("INSERT INTO bot_instances (id, user_id) VALUES (?, ?)", ("bot-2", "user-999"))

    # 1) Direct user_id fill
    conn.execute(
        """
        INSERT INTO trade_fills (id, user_id, bot_instance_id, position_id, symbol, side, action, qty, price, timestamp_utc)
        VALUES (1, 'user-123', 'bot-1', 'pos-A', 'BTCUSDT', 'LONG', 'OPEN', 1, 100, '2026-01-01T00:00:00Z')
        """
    )
    # 2) NULL user_id but bot_instance owned by user-123 (should be included)
    conn.execute(
        """
        INSERT INTO trade_fills (id, user_id, bot_instance_id, position_id, symbol, side, action, qty, price, timestamp_utc)
        VALUES (2, NULL, 'bot-1', 'pos-B', 'ETHUSDT', 'LONG', 'OPEN', 1, 100, '2026-01-01T00:00:00Z')
        """
    )
    # 3) NULL user_id and bot_instance owned by someone else (must be excluded)
    conn.execute(
        """
        INSERT INTO trade_fills (id, user_id, bot_instance_id, position_id, symbol, side, action, qty, price, timestamp_utc)
        VALUES (3, NULL, 'bot-2', 'pos-C', 'SOLUSDT', 'LONG', 'OPEN', 1, 100, '2026-01-01T00:00:00Z')
        """
    )
    # 4) NULL user_id and NULL bot_instance_id (must be excluded; cannot be scoped safely)
    conn.execute(
        """
        INSERT INTO trade_fills (id, user_id, bot_instance_id, position_id, symbol, side, action, qty, price, timestamp_utc)
        VALUES (4, NULL, NULL, 'pos-D', 'XRPUSDT', 'LONG', 'OPEN', 1, 100, '2026-01-01T00:00:00Z')
        """
    )
    # 5) Scoped but missing position_id (excluded by position_id filter)
    conn.execute(
        """
        INSERT INTO trade_fills (id, user_id, bot_instance_id, position_id, symbol, side, action, qty, price, timestamp_utc)
        VALUES (5, NULL, 'bot-1', NULL, 'BNBUSDT', 'LONG', 'OPEN', 1, 100, '2026-01-01T00:00:00Z')
        """
    )

    user_id = "user-123"
    scope_sql = _build_trade_fills_user_scope_sql()
    where_sql = " AND ".join([scope_sql, "f.position_id IS NOT NULL", "f.position_id != ''"])
    params = [user_id, user_id]

    count = conn.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM trade_fills f
        LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
        WHERE {where_sql}
        """,
        params,
    ).fetchone()["cnt"]

    # Only ids 1 and 2 should be included
    assert count == 2

