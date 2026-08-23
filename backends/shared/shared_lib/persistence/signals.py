from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from shared_lib.persistence.db import DB


CANDIDATE_STATUS_CANDIDATE = "CANDIDATE"
CANDIDATE_STATUS_ACCEPTED = "ACCEPTED"
CANDIDATE_STATUS_REJECTED = "REJECTED"

SIGNAL_STATUS_PENDING_ENTRY = "PENDING_ENTRY"
SIGNAL_STATUS_ACTIVE = "ACTIVE"
SIGNAL_STATUS_EXPIRED = "EXPIRED"
SIGNAL_STATUS_TP1_HIT = "TP1_HIT"
SIGNAL_STATUS_TP2_HIT = "TP2_HIT"
SIGNAL_STATUS_TP3_HIT = "TP3_HIT"
SIGNAL_STATUS_SL_HIT = "SL_HIT"
SIGNAL_STATUS_CANCELLED = "CANCELLED"
SIGNAL_STATUS_INVALIDATED = "INVALIDATED"

PERFORMANCE_RESULT_OPEN = "OPEN"
PERFORMANCE_RESULT_WIN = "WIN"
PERFORMANCE_RESULT_LOSS = "LOSS"
PERFORMANCE_RESULT_EXPIRED = "EXPIRED"
PERFORMANCE_RESULT_CANCELLED = "CANCELLED"
PERFORMANCE_RESULT_INVALIDATED = "INVALIDATED"
PERFORMANCE_RESULT_AMBIGUOUS = "AMBIGUOUS"

SOURCE_INTERNAL_SIGNAL_ENGINE = "internal_signal_engine"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _require(data: dict[str, Any], fields: list[str]) -> None:
    missing = [field for field in fields if data.get(field) is None or data.get(field) == ""]
    if missing:
        raise ValueError(f"Missing required signal field(s): {', '.join(missing)}")


def _row_to_dict(row: Any) -> dict[str, Any] | None:
    return dict(row) if row else None


def _insert_row(db: DB, table: str, data: dict[str, Any]) -> str:
    columns = list(data.keys())
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    with db.connect() as conn:
        conn.execute(sql, tuple(data[col] for col in columns))
    return str(data["id"])


def _update_row(db: DB, table: str, row_id: str, data: dict[str, Any]) -> None:
    if not data:
        return
    updates = dict(data)
    updates["updated_at"] = utc_now_iso()
    assignments = ", ".join(f"{col}=?" for col in updates.keys())
    with db.connect() as conn:
        conn.execute(
            f"UPDATE {table} SET {assignments} WHERE id=?",
            (*updates.values(), row_id),
        )


def _list_rows(
    db: DB,
    table: str,
    *,
    limit: int,
    offset: int,
    filters: dict[str, Any] | None = None,
    order_by: str = "created_at DESC",
) -> list[dict[str, Any]]:
    where = []
    params: list[Any] = []
    for column, value in (filters or {}).items():
        if value is None:
            continue
        where.append(f"{column}=?")
        params.append(value)
    query = f"SELECT * FROM {table}"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
    params.extend([int(limit), int(offset)])
    with db.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def ensure_signals_schema(db: DB | None = None) -> None:
    db = db or DB()
    with db.connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_candidates (
                id TEXT PRIMARY KEY,
                asset_class TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                timeframe TEXT,
                strategy_name TEXT,
                entry_price REAL,
                entry_zone_low REAL,
                entry_zone_high REAL,
                stop_loss REAL,
                take_profit_1 REAL,
                take_profit_2 REAL,
                take_profit_3 REAL,
                risk_reward REAL,
                confidence_score REAL,
                signal_reason TEXT,
                rejection_reason TEXT,
                source TEXT NOT NULL DEFAULT 'internal_signal_engine',
                status TEXT NOT NULL DEFAULT 'CANDIDATE',
                dev_mode INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_candidates_symbol ON signal_candidates(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_candidates_status ON signal_candidates(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_candidates_created_at ON signal_candidates(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_candidates_asset_class ON signal_candidates(asset_class)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trading_signals (
                id TEXT PRIMARY KEY,
                candidate_id TEXT,
                asset_class TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                timeframe TEXT,
                strategy_name TEXT,
                entry_price REAL NOT NULL,
                entry_zone_low REAL,
                entry_zone_high REAL,
                stop_loss REAL NOT NULL,
                take_profit_1 REAL NOT NULL,
                take_profit_2 REAL,
                take_profit_3 REAL,
                risk_reward REAL NOT NULL,
                confidence_score REAL NOT NULL,
                signal_reason TEXT,
                status TEXT NOT NULL,
                is_published INTEGER DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'internal_signal_engine',
                dev_mode INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                published_at TEXT,
                expires_at TEXT NOT NULL,
                invalidated_at TEXT,
                tp1_hit_at TEXT,
                tp2_hit_at TEXT,
                tp3_hit_at TEXT,
                sl_hit_at TEXT,
                cancelled_at TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(candidate_id) REFERENCES signal_candidates(id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_signals_status ON trading_signals(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_signals_symbol ON trading_signals(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_signals_asset_class ON trading_signals(asset_class)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_signals_is_published ON trading_signals(is_published)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_signals_expires_at ON trading_signals(expires_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_signals_published_at ON trading_signals(published_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_signals_dev_mode ON trading_signals(dev_mode)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_performance (
                id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_triggered INTEGER DEFAULT 0,
                tp1_hit INTEGER DEFAULT 0,
                tp2_hit INTEGER DEFAULT 0,
                tp3_hit INTEGER DEFAULT 0,
                sl_hit INTEGER DEFAULT 0,
                expired INTEGER DEFAULT 0,
                cancelled INTEGER DEFAULT 0,
                invalidated INTEGER DEFAULT 0,
                max_favorable_move REAL,
                max_adverse_move REAL,
                result TEXT,
                closed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(signal_id) REFERENCES trading_signals(id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_performance_signal_id ON signal_performance(signal_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_performance_symbol ON signal_performance(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_performance_result ON signal_performance(result)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_performance_created_at ON signal_performance(created_at)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_delivery (
                id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                delivered_at TEXT,
                viewed_at TEXT,
                saved INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(signal_id) REFERENCES trading_signals(id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_delivery_signal_id ON signal_delivery(signal_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_delivery_user_id ON signal_delivery(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_delivery_saved ON signal_delivery(saved)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_delivery_viewed_at ON signal_delivery(viewed_at)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_signal_preferences (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                crypto_enabled INTEGER DEFAULT 1,
                forex_enabled INTEGER DEFAULT 0,
                preferred_symbols TEXT,
                minimum_confidence REAL DEFAULT 70,
                notifications_enabled INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_signal_preferences_user_id ON user_signal_preferences(user_id)")
        pref_columns = {row["name"] for row in conn.execute("PRAGMA table_info(user_signal_preferences)").fetchall()}
        for column, definition in [
            ("favorite_symbols", "TEXT"),
            ("hidden_symbols", "TEXT"),
            ("majors_only", "INTEGER DEFAULT 0"),
            ("risk_style", "TEXT DEFAULT 'balanced'"),
            ("notify_new_signal", "INTEGER DEFAULT 1"),
            ("notify_signal_invalidated", "INTEGER DEFAULT 1"),
            ("notify_tp1_hit", "INTEGER DEFAULT 0"),
            ("notify_tp2_hit", "INTEGER DEFAULT 1"),
            ("notify_tp3_hit", "INTEGER DEFAULT 1"),
            ("notify_sl_hit", "INTEGER DEFAULT 1"),
            ("notify_entry_window_expiring", "INTEGER DEFAULT 1"),
        ]:
            if column not in pref_columns:
                conn.execute(f"ALTER TABLE user_signal_preferences ADD COLUMN {column} {definition}")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_notifications (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                signal_id TEXT,
                symbol TEXT,
                event_type TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                channel TEXT DEFAULT 'in_app',
                status TEXT DEFAULT 'PENDING',
                read_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_notifications_user_id ON signal_notifications(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_notifications_signal_id ON signal_notifications(signal_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_notifications_event_type ON signal_notifications(event_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_notifications_status ON signal_notifications(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_notifications_created_at ON signal_notifications(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_notifications_read_at ON signal_notifications(read_at)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_pair_universe (
                symbol TEXT PRIMARY KEY,
                exchange TEXT NOT NULL,
                asset_class TEXT NOT NULL DEFAULT 'crypto',
                quote_asset TEXT,
                contract_type TEXT,
                tier TEXT,
                enabled INTEGER DEFAULT 1,
                whitelisted INTEGER DEFAULT 0,
                blacklisted INTEGER DEFAULT 0,
                blacklist_reason TEXT,
                discovered_at TEXT,
                last_seen_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_universe_exchange ON signal_pair_universe(exchange)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_universe_asset_class ON signal_pair_universe(asset_class)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_universe_quote_asset ON signal_pair_universe(quote_asset)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_universe_contract_type ON signal_pair_universe(contract_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_universe_tier ON signal_pair_universe(tier)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_universe_enabled ON signal_pair_universe(enabled)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_universe_whitelisted ON signal_pair_universe(whitelisted)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_universe_blacklisted ON signal_pair_universe(blacklisted)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_universe_last_seen_at ON signal_pair_universe(last_seen_at)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_pair_metrics (
                symbol TEXT PRIMARY KEY,
                exchange TEXT NOT NULL,
                quote_volume_24h REAL,
                spread_percent REAL,
                bid_price REAL,
                ask_price REAL,
                candle_count INTEGER,
                atr_percent REAL,
                volatility_score REAL,
                liquidity_score REAL,
                spread_score REAL,
                reliability_score REAL,
                is_safe INTEGER DEFAULT 0,
                unsafe_reason TEXT,
                last_updated TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_metrics_exchange ON signal_pair_metrics(exchange)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_metrics_quote_volume_24h ON signal_pair_metrics(quote_volume_24h)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_metrics_spread_percent ON signal_pair_metrics(spread_percent)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_metrics_is_safe ON signal_pair_metrics(is_safe)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_pair_metrics_last_updated ON signal_pair_metrics(last_updated)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_scan_runs (
                id TEXT PRIMARY KEY,
                scan_type TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_seconds REAL,
                symbols_discovered INTEGER DEFAULT 0,
                symbols_eligible INTEGER DEFAULT 0,
                symbols_scanned INTEGER DEFAULT 0,
                candidates_created INTEGER DEFAULT 0,
                signals_published INTEGER DEFAULT 0,
                errors TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_runs_scan_type ON signal_scan_runs(scan_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_runs_status ON signal_scan_runs(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_runs_started_at ON signal_scan_runs(started_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_runs_ended_at ON signal_scan_runs(ended_at)")
        rows = conn.execute("PRAGMA table_info(signal_scan_runs)").fetchall()
        if "duration_seconds" not in {row["name"] for row in rows}:
            conn.execute("ALTER TABLE signal_scan_runs ADD COLUMN duration_seconds REAL")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_scan_results (
                id TEXT PRIMARY KEY,
                scan_run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                was_scanned INTEGER DEFAULT 0,
                was_skipped INTEGER DEFAULT 0,
                skip_reason TEXT,
                candidate_count INTEGER DEFAULT 0,
                accepted_count INTEGER DEFAULT 0,
                rejected_count INTEGER DEFAULT 0,
                published_count INTEGER DEFAULT 0,
                error TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(scan_run_id) REFERENCES signal_scan_runs(id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_results_scan_run_id ON signal_scan_results(scan_run_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_results_symbol ON signal_scan_results(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_results_was_scanned ON signal_scan_results(was_scanned)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_results_was_skipped ON signal_scan_results(was_skipped)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_results_skip_reason ON signal_scan_results(skip_reason)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_scan_results_created_at ON signal_scan_results(created_at)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_operation_locks (
                lock_name TEXT PRIMARY KEY,
                locked_by TEXT,
                locked_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                metadata TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_operation_locks_expires_at ON signal_operation_locks(expires_at)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_system_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )


def create_signal_candidate(data: dict[str, Any], db: DB | None = None) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    payload = dict(data or {})
    _require(payload, ["asset_class", "symbol", "side"])
    now = utc_now_iso()
    payload.setdefault("id", _new_id("sigcand"))
    payload.setdefault("source", SOURCE_INTERNAL_SIGNAL_ENGINE)
    payload.setdefault("status", CANDIDATE_STATUS_CANDIDATE)
    payload.setdefault("dev_mode", 0)
    payload.setdefault("created_at", now)
    payload.setdefault("updated_at", now)
    return _insert_row(db, "signal_candidates", payload)


def get_signal_candidate(candidate_id: str, db: DB | None = None) -> dict[str, Any] | None:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM signal_candidates WHERE id=?", (candidate_id,)).fetchone()
    return _row_to_dict(row)


def list_signal_candidates(
    limit: int = 50,
    offset: int = 0,
    status: str | None = None,
    symbol: str | None = None,
    asset_class: str | None = None,
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    return _list_rows(
        db,
        "signal_candidates",
        limit=limit,
        offset=offset,
        filters={"status": status, "symbol": symbol, "asset_class": asset_class},
    )


def update_signal_candidate(candidate_id: str, data: dict[str, Any], db: DB | None = None) -> None:
    db = db or DB()
    ensure_signals_schema(db)
    _update_row(db, "signal_candidates", candidate_id, dict(data or {}))


def mark_candidate_rejected(candidate_id: str, rejection_reason: str, db: DB | None = None) -> None:
    update_signal_candidate(
        candidate_id,
        {"status": CANDIDATE_STATUS_REJECTED, "rejection_reason": rejection_reason},
        db=db,
    )


def delete_signal_candidate(candidate_id: str, db: DB | None = None) -> None:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        conn.execute("DELETE FROM signal_candidates WHERE id=?", (candidate_id,))


def create_trading_signal(data: dict[str, Any], db: DB | None = None) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    payload = dict(data or {})
    _require(
        payload,
        [
            "asset_class",
            "symbol",
            "side",
            "entry_price",
            "stop_loss",
            "take_profit_1",
            "risk_reward",
            "confidence_score",
            "status",
            "expires_at",
        ],
    )
    now = utc_now_iso()
    payload.setdefault("id", _new_id("sig"))
    payload.setdefault("source", SOURCE_INTERNAL_SIGNAL_ENGINE)
    payload.setdefault("is_published", 0)
    payload.setdefault("dev_mode", 0)
    payload.setdefault("created_at", now)
    payload.setdefault("updated_at", now)
    return _insert_row(db, "trading_signals", payload)


def get_trading_signal(signal_id: str, db: DB | None = None) -> dict[str, Any] | None:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM trading_signals WHERE id=?", (signal_id,)).fetchone()
    return _row_to_dict(row)


def list_trading_signals(
    limit: int = 50,
    offset: int = 0,
    status: str | None = None,
    symbol: str | None = None,
    asset_class: str | None = None,
    is_published: int | bool | None = None,
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    filters = {
        "status": status,
        "symbol": symbol,
        "asset_class": asset_class,
        "is_published": int(is_published) if is_published is not None else None,
    }
    return _list_rows(db, "trading_signals", limit=limit, offset=offset, filters=filters)


def list_active_trading_signals(
    limit: int = 50,
    offset: int = 0,
    asset_class: str = "crypto",
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    statuses = (
        SIGNAL_STATUS_PENDING_ENTRY,
        SIGNAL_STATUS_ACTIVE,
    )
    now = utc_now_iso()
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM trading_signals
            WHERE is_published = 1
              AND UPPER(asset_class) = UPPER(?)
              AND status IN (?, ?)
              AND expires_at > ?
            ORDER BY published_at DESC, created_at DESC
            LIMIT ? OFFSET ?
            """,
            (asset_class, *statuses, now, int(limit), int(offset)),
        ).fetchall()
    return [dict(row) for row in rows]


def list_signal_history(
    limit: int = 50,
    offset: int = 0,
    asset_class: str = "crypto",
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    terminal_statuses = (
        SIGNAL_STATUS_EXPIRED,
        SIGNAL_STATUS_TP3_HIT,
        SIGNAL_STATUS_SL_HIT,
        SIGNAL_STATUS_CANCELLED,
        SIGNAL_STATUS_INVALIDATED,
    )
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM trading_signals
            WHERE UPPER(asset_class) = UPPER(?)
              AND status IN (?, ?, ?, ?, ?)
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ? OFFSET ?
            """,
            (asset_class, *terminal_statuses, int(limit), int(offset)),
        ).fetchall()
    return [dict(row) for row in rows]


def update_trading_signal(signal_id: str, data: dict[str, Any], db: DB | None = None) -> None:
    db = db or DB()
    ensure_signals_schema(db)
    _update_row(db, "trading_signals", signal_id, dict(data or {}))


def publish_trading_signal(signal_id: str, published_at: str | None = None, db: DB | None = None) -> None:
    update_trading_signal(
        signal_id,
        {"is_published": 1, "published_at": published_at or utc_now_iso()},
        db=db,
    )


def unpublish_trading_signal(signal_id: str, db: DB | None = None) -> None:
    update_trading_signal(signal_id, {"is_published": 0, "published_at": None}, db=db)


def cancel_trading_signal(signal_id: str, cancelled_at: str | None = None, db: DB | None = None) -> None:
    update_trading_signal(
        signal_id,
        {"status": SIGNAL_STATUS_CANCELLED, "cancelled_at": cancelled_at or utc_now_iso()},
        db=db,
    )


def expire_trading_signal(signal_id: str, expired_at: str | None = None, db: DB | None = None) -> None:
    update_trading_signal(signal_id, {"status": SIGNAL_STATUS_EXPIRED}, db=db)


def invalidate_trading_signal(signal_id: str, invalidated_at: str | None = None, db: DB | None = None) -> None:
    update_trading_signal(
        signal_id,
        {"status": SIGNAL_STATUS_INVALIDATED, "invalidated_at": invalidated_at or utc_now_iso()},
        db=db,
    )


def create_signal_performance(data: dict[str, Any], db: DB | None = None) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    payload = dict(data or {})
    _require(payload, ["signal_id", "asset_class", "symbol", "side"])
    now = utc_now_iso()
    payload.setdefault("id", _new_id("sigperf"))
    payload.setdefault("entry_triggered", 0)
    payload.setdefault("tp1_hit", 0)
    payload.setdefault("tp2_hit", 0)
    payload.setdefault("tp3_hit", 0)
    payload.setdefault("sl_hit", 0)
    payload.setdefault("expired", 0)
    payload.setdefault("cancelled", 0)
    payload.setdefault("invalidated", 0)
    payload.setdefault("result", PERFORMANCE_RESULT_OPEN)
    payload.setdefault("created_at", now)
    payload.setdefault("updated_at", now)
    return _insert_row(db, "signal_performance", payload)


def get_signal_performance(signal_id: str, db: DB | None = None) -> dict[str, Any] | None:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM signal_performance WHERE signal_id=?", (signal_id,)).fetchone()
    return _row_to_dict(row)


def update_signal_performance(signal_id: str, data: dict[str, Any], db: DB | None = None) -> None:
    db = db or DB()
    ensure_signals_schema(db)
    updates = dict(data or {})
    updates["updated_at"] = utc_now_iso()
    assignments = ", ".join(f"{col}=?" for col in updates.keys())
    with db.connect() as conn:
        conn.execute(
            f"UPDATE signal_performance SET {assignments} WHERE signal_id=?",
            (*updates.values(), signal_id),
        )


def mark_signal_entry_triggered(signal_id: str, db: DB | None = None) -> None:
    update_signal_performance(signal_id, {"entry_triggered": 1}, db=db)
    update_trading_signal(signal_id, {"status": SIGNAL_STATUS_ACTIVE}, db=db)


def mark_signal_tp_hit(signal_id: str, tp_level: int, db: DB | None = None) -> None:
    if tp_level not in (1, 2, 3):
        raise ValueError("tp_level must be 1, 2, or 3")
    now = utc_now_iso()
    perf_updates = {f"tp{tp_level}_hit": 1}
    signal_updates = {"status": f"TP{tp_level}_HIT", f"tp{tp_level}_hit_at": now}
    if tp_level == 3:
        perf_updates.update({"result": PERFORMANCE_RESULT_WIN, "closed_at": now})
    update_signal_performance(signal_id, perf_updates, db=db)
    update_trading_signal(signal_id, signal_updates, db=db)


def mark_signal_sl_hit(signal_id: str, db: DB | None = None) -> None:
    now = utc_now_iso()
    update_signal_performance(
        signal_id,
        {"sl_hit": 1, "result": PERFORMANCE_RESULT_LOSS, "closed_at": now},
        db=db,
    )
    update_trading_signal(signal_id, {"status": SIGNAL_STATUS_SL_HIT, "sl_hit_at": now}, db=db)


def mark_signal_expired(signal_id: str, db: DB | None = None) -> None:
    now = utc_now_iso()
    update_signal_performance(
        signal_id,
        {"expired": 1, "result": PERFORMANCE_RESULT_EXPIRED, "closed_at": now},
        db=db,
    )
    expire_trading_signal(signal_id, expired_at=now, db=db)


def mark_signal_cancelled(signal_id: str, db: DB | None = None) -> None:
    now = utc_now_iso()
    update_signal_performance(
        signal_id,
        {"cancelled": 1, "result": PERFORMANCE_RESULT_CANCELLED, "closed_at": now},
        db=db,
    )
    cancel_trading_signal(signal_id, cancelled_at=now, db=db)


def mark_signal_invalidated(signal_id: str, db: DB | None = None) -> None:
    now = utc_now_iso()
    update_signal_performance(
        signal_id,
        {"invalidated": 1, "result": PERFORMANCE_RESULT_INVALIDATED, "closed_at": now},
        db=db,
    )
    invalidate_trading_signal(signal_id, invalidated_at=now, db=db)


def list_signal_performance(
    limit: int = 50,
    offset: int = 0,
    symbol: str | None = None,
    result: str | None = None,
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    return _list_rows(
        db,
        "signal_performance",
        limit=limit,
        offset=offset,
        filters={"symbol": symbol, "result": result},
    )


def create_signal_delivery(data: dict[str, Any], db: DB | None = None) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    payload = dict(data or {})
    _require(payload, ["signal_id", "user_id"])
    now = utc_now_iso()
    payload.setdefault("id", _new_id("sigdel"))
    payload.setdefault("saved", 0)
    payload.setdefault("created_at", now)
    payload.setdefault("updated_at", now)
    return _insert_row(db, "signal_delivery", payload)


def _get_signal_delivery(signal_id: str, user_id: str, db: DB) -> dict[str, Any] | None:
    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM signal_delivery
            WHERE signal_id=? AND user_id=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (signal_id, user_id),
        ).fetchone()
    return _row_to_dict(row)


def _upsert_signal_delivery(signal_id: str, user_id: str, updates: dict[str, Any], db: DB | None = None) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    existing = _get_signal_delivery(signal_id, user_id, db)
    if existing:
        _update_row(db, "signal_delivery", existing["id"], updates)
        return str(existing["id"])
    payload = {"signal_id": signal_id, "user_id": user_id, **updates}
    return create_signal_delivery(payload, db=db)


def mark_signal_delivered(signal_id: str, user_id: str, db: DB | None = None) -> str:
    return _upsert_signal_delivery(signal_id, user_id, {"delivered_at": utc_now_iso()}, db=db)


def mark_signal_viewed(signal_id: str, user_id: str, db: DB | None = None) -> str:
    return _upsert_signal_delivery(signal_id, user_id, {"viewed_at": utc_now_iso()}, db=db)


def save_signal_for_user(signal_id: str, user_id: str, db: DB | None = None) -> str:
    return _upsert_signal_delivery(signal_id, user_id, {"saved": 1}, db=db)


def unsave_signal_for_user(signal_id: str, user_id: str, db: DB | None = None) -> str:
    return _upsert_signal_delivery(signal_id, user_id, {"saved": 0}, db=db)


def list_user_signal_deliveries(
    user_id: str,
    limit: int = 50,
    offset: int = 0,
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    return _list_rows(
        db,
        "signal_delivery",
        limit=limit,
        offset=offset,
        filters={"user_id": user_id},
    )


def get_user_signal_preferences(user_id: str, db: DB | None = None) -> dict[str, Any] | None:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM user_signal_preferences
            WHERE user_id=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
    return _row_to_dict(row)


def _json_symbol_list(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        raw = value
    else:
        try:
            parsed = json.loads(str(value))
            raw = parsed if isinstance(parsed, list) else []
        except (TypeError, ValueError, json.JSONDecodeError):
            raw = [item.strip() for item in str(value).split(",")]
    symbols: list[str] = []
    for item in raw:
        symbol = str(item or "").strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _encode_symbol_list(value: Any) -> str:
    return json.dumps(_json_symbol_list(value))


def normalize_user_signal_preferences(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    normalized = dict(row)
    normalized["favorite_symbols"] = _json_symbol_list(normalized.get("favorite_symbols"))
    normalized["hidden_symbols"] = _json_symbol_list(normalized.get("hidden_symbols"))
    for key in [
        "crypto_enabled",
        "forex_enabled",
        "notifications_enabled",
        "majors_only",
        "notify_new_signal",
        "notify_signal_invalidated",
        "notify_tp1_hit",
        "notify_tp2_hit",
        "notify_tp3_hit",
        "notify_sl_hit",
        "notify_entry_window_expiring",
    ]:
        normalized[key] = bool(int(normalized.get(key) or 0))
    normalized.setdefault("risk_style", "balanced")
    normalized["minimum_confidence"] = float(normalized.get("minimum_confidence") or 70)
    return normalized


def create_user_signal_preferences(
    user_id: str,
    data: dict[str, Any] | None = None,
    db: DB | None = None,
) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    now = utc_now_iso()
    payload = dict(data or {})
    payload.setdefault("id", _new_id("sigpref"))
    payload["user_id"] = user_id
    payload.setdefault("crypto_enabled", 1)
    payload.setdefault("forex_enabled", 0)
    payload["favorite_symbols"] = _encode_symbol_list(payload.get("favorite_symbols"))
    payload["hidden_symbols"] = _encode_symbol_list(payload.get("hidden_symbols"))
    payload.setdefault("minimum_confidence", 70)
    payload.setdefault("notifications_enabled", 1)
    payload.setdefault("majors_only", 0)
    payload.setdefault("risk_style", "balanced")
    payload.setdefault("notify_new_signal", 1)
    payload.setdefault("notify_signal_invalidated", 1)
    payload.setdefault("notify_tp1_hit", 0)
    payload.setdefault("notify_tp2_hit", 1)
    payload.setdefault("notify_tp3_hit", 1)
    payload.setdefault("notify_sl_hit", 1)
    payload.setdefault("notify_entry_window_expiring", 1)
    payload.setdefault("created_at", now)
    payload.setdefault("updated_at", now)
    return _insert_row(db, "user_signal_preferences", payload)


def update_user_signal_preferences(user_id: str, data: dict[str, Any], db: DB | None = None) -> None:
    db = db or DB()
    ensure_signals_schema(db)
    existing = get_user_signal_preferences(user_id, db=db)
    if not existing:
        create_user_signal_preferences(user_id, data=data, db=db)
        return
    updates = dict(data or {})
    if "favorite_symbols" in updates:
        updates["favorite_symbols"] = _encode_symbol_list(updates.get("favorite_symbols"))
    if "hidden_symbols" in updates:
        updates["hidden_symbols"] = _encode_symbol_list(updates.get("hidden_symbols"))
    _update_row(db, "user_signal_preferences", existing["id"], updates)


def get_or_create_user_signal_preferences(user_id: str, db: DB | None = None) -> dict[str, Any]:
    db = db or DB()
    ensure_signals_schema(db)
    existing = get_user_signal_preferences(user_id, db=db)
    if existing:
        return existing
    pref_id = create_user_signal_preferences(user_id, db=db)
    created = get_user_signal_preferences(user_id, db=db)
    if not created:
        raise RuntimeError(f"Failed to create user signal preferences: {pref_id}")
    return created


def add_user_signal_favorite(user_id: str, symbol: str, db: DB | None = None) -> dict[str, Any]:
    pref = get_or_create_user_signal_preferences(user_id, db=db)
    favorites = _json_symbol_list(pref.get("favorite_symbols"))
    normalized = str(symbol).upper()
    if normalized not in favorites:
        favorites.append(normalized)
    update_user_signal_preferences(user_id, {"favorite_symbols": favorites}, db=db)
    return get_or_create_user_signal_preferences(user_id, db=db)


def remove_user_signal_favorite(user_id: str, symbol: str, db: DB | None = None) -> dict[str, Any]:
    pref = get_or_create_user_signal_preferences(user_id, db=db)
    normalized = str(symbol).upper()
    favorites = [item for item in _json_symbol_list(pref.get("favorite_symbols")) if item != normalized]
    update_user_signal_preferences(user_id, {"favorite_symbols": favorites}, db=db)
    return get_or_create_user_signal_preferences(user_id, db=db)


def add_user_signal_hidden_symbol(user_id: str, symbol: str, db: DB | None = None) -> dict[str, Any]:
    pref = get_or_create_user_signal_preferences(user_id, db=db)
    hidden = _json_symbol_list(pref.get("hidden_symbols"))
    normalized = str(symbol).upper()
    if normalized not in hidden:
        hidden.append(normalized)
    update_user_signal_preferences(user_id, {"hidden_symbols": hidden}, db=db)
    return get_or_create_user_signal_preferences(user_id, db=db)


def remove_user_signal_hidden_symbol(user_id: str, symbol: str, db: DB | None = None) -> dict[str, Any]:
    pref = get_or_create_user_signal_preferences(user_id, db=db)
    normalized = str(symbol).upper()
    hidden = [item for item in _json_symbol_list(pref.get("hidden_symbols")) if item != normalized]
    update_user_signal_preferences(user_id, {"hidden_symbols": hidden}, db=db)
    return get_or_create_user_signal_preferences(user_id, db=db)


def upsert_signal_pair(pair_data: dict[str, Any], db: DB | None = None) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    payload = dict(pair_data or {})
    _require(payload, ["symbol", "exchange"])
    now = utc_now_iso()
    payload["symbol"] = str(payload["symbol"]).upper()
    payload.setdefault("asset_class", "crypto")
    payload.setdefault("enabled", 1)
    payload.setdefault("whitelisted", 0)
    payload.setdefault("blacklisted", 0)
    payload.setdefault("discovered_at", now)
    payload.setdefault("last_seen_at", now)
    payload.setdefault("created_at", now)
    payload.setdefault("updated_at", now)
    columns = list(payload.keys())
    update_columns = [col for col in columns if col not in {"symbol", "created_at"}]
    sql = f"""
        INSERT INTO signal_pair_universe ({', '.join(columns)})
        VALUES ({', '.join('?' for _ in columns)})
        ON CONFLICT(symbol) DO UPDATE SET
            {', '.join(f'{col}=excluded.{col}' for col in update_columns)}
    """
    with db.connect() as conn:
        conn.execute(sql, tuple(payload[col] for col in columns))
    return str(payload["symbol"])


def get_signal_pair(symbol: str, db: DB | None = None) -> dict[str, Any] | None:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM signal_pair_universe WHERE symbol=?",
            (symbol.upper(),),
        ).fetchone()
    return _row_to_dict(row)


def list_signal_pairs(
    exchange: str | None = None,
    asset_class: str | None = None,
    quote_asset: str | None = None,
    contract_type: str | None = None,
    tier: str | None = None,
    enabled: int | bool | None = None,
    whitelisted: int | bool | None = None,
    blacklisted: int | bool | None = None,
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    where: list[str] = []
    params: list[Any] = []
    filters = {
        "exchange": exchange,
        "asset_class": asset_class,
        "quote_asset": quote_asset,
        "contract_type": contract_type,
        "tier": tier,
        "enabled": int(enabled) if enabled is not None else None,
        "whitelisted": int(whitelisted) if whitelisted is not None else None,
        "blacklisted": int(blacklisted) if blacklisted is not None else None,
    }
    for column, value in filters.items():
        if value is None:
            continue
        where.append(f"{column}=?")
        params.append(value)
    if search:
        where.append("UPPER(symbol) LIKE ?")
        params.append(f"%{str(search).strip().upper()}%")
    query = "SELECT * FROM signal_pair_universe"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY updated_at DESC LIMIT ? OFFSET ?"
    params.extend([int(limit), int(offset)])
    with db.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def update_signal_pair(symbol: str, data: dict[str, Any], db: DB | None = None) -> None:
    db = db or DB()
    ensure_signals_schema(db)
    updates = dict(data or {})
    if not updates:
        return
    updates["updated_at"] = utc_now_iso()
    assignments = ", ".join(f"{col}=?" for col in updates.keys())
    with db.connect() as conn:
        conn.execute(
            f"UPDATE signal_pair_universe SET {assignments} WHERE symbol=?",
            (*updates.values(), symbol.upper()),
        )


def enable_signal_pair(symbol: str, db: DB | None = None) -> None:
    update_signal_pair(symbol, {"enabled": 1}, db=db)


def disable_signal_pair(symbol: str, db: DB | None = None) -> None:
    update_signal_pair(symbol, {"enabled": 0}, db=db)


def whitelist_signal_pair(symbol: str, db: DB | None = None) -> None:
    update_signal_pair(symbol, {"whitelisted": 1}, db=db)


def blacklist_signal_pair(symbol: str, reason: str | None = None, db: DB | None = None) -> None:
    update_signal_pair(symbol, {"blacklisted": 1, "enabled": 0, "blacklist_reason": reason}, db=db)


def unblacklist_signal_pair(symbol: str, db: DB | None = None) -> None:
    update_signal_pair(symbol, {"blacklisted": 0, "blacklist_reason": None}, db=db)


def upsert_signal_pair_metrics(metrics_data: dict[str, Any], db: DB | None = None) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    payload = dict(metrics_data or {})
    _require(payload, ["symbol", "exchange"])
    payload["symbol"] = str(payload["symbol"]).upper()
    payload.setdefault("is_safe", 0)
    payload.setdefault("last_updated", utc_now_iso())
    columns = list(payload.keys())
    update_columns = [col for col in columns if col != "symbol"]
    sql = f"""
        INSERT INTO signal_pair_metrics ({', '.join(columns)})
        VALUES ({', '.join('?' for _ in columns)})
        ON CONFLICT(symbol) DO UPDATE SET
            {', '.join(f'{col}=excluded.{col}' for col in update_columns)}
    """
    with db.connect() as conn:
        conn.execute(sql, tuple(payload[col] for col in columns))
    return str(payload["symbol"])


def get_signal_pair_metrics(symbol: str, db: DB | None = None) -> dict[str, Any] | None:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM signal_pair_metrics WHERE symbol=?",
            (symbol.upper(),),
        ).fetchone()
    return _row_to_dict(row)


def list_signal_pair_metrics(
    exchange: str | None = None,
    is_safe: int | bool | None = None,
    min_volume: float | None = None,
    max_spread: float | None = None,
    symbol: str | None = None,
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    where = []
    params: list[Any] = []
    if exchange is not None:
        where.append("exchange=?")
        params.append(exchange)
    if is_safe is not None:
        where.append("is_safe=?")
        params.append(int(is_safe))
    if min_volume is not None:
        where.append("quote_volume_24h>=?")
        params.append(float(min_volume))
    if max_spread is not None:
        where.append("spread_percent<=?")
        params.append(float(max_spread))
    if symbol is not None:
        where.append("UPPER(symbol)=?")
        params.append(str(symbol).upper())
    if search:
        where.append("UPPER(symbol) LIKE ?")
        params.append(f"%{str(search).strip().upper()}%")
    query = "SELECT * FROM signal_pair_metrics"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY reliability_score DESC, quote_volume_24h DESC LIMIT ? OFFSET ?"
    params.extend([int(limit), int(offset)])
    with db.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def mark_pair_safe(symbol: str, db: DB | None = None) -> None:
    upsert_signal_pair_metrics(
        {"symbol": symbol, "exchange": (get_signal_pair_metrics(symbol, db=db) or {}).get("exchange", "unknown"), "is_safe": 1, "unsafe_reason": None},
        db=db,
    )


def mark_pair_unsafe(symbol: str, reason: str, db: DB | None = None) -> None:
    upsert_signal_pair_metrics(
        {"symbol": symbol, "exchange": (get_signal_pair_metrics(symbol, db=db) or {}).get("exchange", "unknown"), "is_safe": 0, "unsafe_reason": reason},
        db=db,
    )


def create_signal_scan_run(
    scan_type: str,
    status: str = "STARTED",
    data: dict[str, Any] | None = None,
    db: DB | None = None,
) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    _require({"scan_type": scan_type, "status": status}, ["scan_type", "status"])
    now = utc_now_iso()
    payload = dict(data or {})
    payload.setdefault("id", _new_id("sigscan"))
    payload["scan_type"] = scan_type
    payload["status"] = status
    payload.setdefault("started_at", now)
    payload.setdefault("created_at", now)
    payload.setdefault("updated_at", now)
    return _insert_row(db, "signal_scan_runs", payload)


def update_signal_scan_run(scan_run_id: str, data: dict[str, Any], db: DB | None = None) -> None:
    db = db or DB()
    ensure_signals_schema(db)
    _update_row(db, "signal_scan_runs", scan_run_id, dict(data or {}))


def complete_signal_scan_run(scan_run_id: str, summary_data: dict[str, Any], db: DB | None = None) -> None:
    db = db or DB()
    updates = dict(summary_data or {})
    updates.setdefault("status", "COMPLETED")
    ended_at = updates.setdefault("ended_at", utc_now_iso())
    existing = get_signal_scan_run(scan_run_id, db=db)
    if existing and updates.get("duration_seconds") is None:
        try:
            started = datetime.fromisoformat(str(existing["started_at"]).replace("Z", "+00:00"))
            ended = datetime.fromisoformat(str(ended_at).replace("Z", "+00:00"))
            updates["duration_seconds"] = max(0.0, (ended - started).total_seconds())
        except Exception:
            pass
    update_signal_scan_run(scan_run_id, updates, db=db)


def fail_signal_scan_run(scan_run_id: str, error: str, db: DB | None = None) -> None:
    db = db or DB()
    ended_at = utc_now_iso()
    updates = {"status": "FAILED", "errors": error, "ended_at": ended_at}
    existing = get_signal_scan_run(scan_run_id, db=db)
    if existing:
        try:
            started = datetime.fromisoformat(str(existing["started_at"]).replace("Z", "+00:00"))
            ended = datetime.fromisoformat(str(ended_at).replace("Z", "+00:00"))
            updates["duration_seconds"] = max(0.0, (ended - started).total_seconds())
        except Exception:
            pass
    update_signal_scan_run(
        scan_run_id,
        updates,
        db=db,
    )


def get_signal_scan_run(scan_run_id: str, db: DB | None = None) -> dict[str, Any] | None:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM signal_scan_runs WHERE id=?", (scan_run_id,)).fetchone()
    return _row_to_dict(row)


def list_signal_scan_runs(
    scan_type: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    return _list_rows(
        db,
        "signal_scan_runs",
        limit=limit,
        offset=offset,
        filters={"scan_type": scan_type, "status": status},
        order_by="started_at DESC",
    )


def create_signal_scan_result(data: dict[str, Any], db: DB | None = None) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    payload = dict(data or {})
    _require(payload, ["scan_run_id", "symbol"])
    payload.setdefault("id", _new_id("sigscanres"))
    payload["symbol"] = str(payload["symbol"]).upper()
    payload.setdefault("was_scanned", 0)
    payload.setdefault("was_skipped", 0)
    payload.setdefault("candidate_count", 0)
    payload.setdefault("accepted_count", 0)
    payload.setdefault("rejected_count", 0)
    payload.setdefault("published_count", 0)
    payload.setdefault("created_at", utc_now_iso())
    return _insert_row(db, "signal_scan_results", payload)


def list_signal_scan_results(
    scan_run_id: str | None = None,
    symbol: str | None = None,
    was_skipped: int | bool | None = None,
    skip_reason: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    return _list_rows(
        db,
        "signal_scan_results",
        limit=limit,
        offset=offset,
        filters={
            "scan_run_id": scan_run_id,
            "symbol": symbol.upper() if symbol else None,
            "was_skipped": int(was_skipped) if was_skipped is not None else None,
            "skip_reason": skip_reason,
        },
    )


def get_eligible_signal_symbols(
    exchange: str = "binance_futures",
    asset_class: str = "crypto",
    quote_asset: str = "USDT",
    tiers: list[str] | tuple[str, ...] | None = None,
    min_quote_volume_24h: float = 50_000_000,
    max_spread_percent: float = 0.20,
    require_safe: bool = True,
    limit: int | None = None,
    db: DB | None = None,
) -> list[str]:
    db = db or DB()
    ensure_signals_schema(db)
    where = [
        "u.exchange = ?",
        "LOWER(u.asset_class) = LOWER(?)",
        "u.quote_asset = ?",
        "u.enabled = 1",
        "COALESCE(u.blacklisted, 0) = 0",
        "COALESCE(m.quote_volume_24h, 0) >= ?",
        "COALESCE(m.spread_percent, 999999) <= ?",
    ]
    params: list[Any] = [exchange, asset_class, quote_asset, float(min_quote_volume_24h), float(max_spread_percent)]
    if tiers:
        tier_values = [str(tier).upper() for tier in tiers]
        where.append(f"u.tier IN ({', '.join('?' for _ in tier_values)})")
        params.extend(tier_values)
    if require_safe:
        where.append("COALESCE(m.is_safe, 0) = 1")
    query = f"""
        SELECT u.symbol
        FROM signal_pair_universe u
        JOIN signal_pair_metrics m ON m.symbol = u.symbol
        WHERE {' AND '.join(where)}
        ORDER BY
            CASE u.tier
                WHEN 'TIER_1' THEN 1
                WHEN 'TIER_2' THEN 2
                WHEN 'TIER_3' THEN 3
                ELSE 4
            END,
            m.reliability_score DESC,
            m.quote_volume_24h DESC,
            u.symbol ASC
    """
    if limit is not None:
        query += " LIMIT ?"
        params.append(int(limit))
    with db.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [str(row["symbol"]) for row in rows]


def _json_or_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


def cleanup_expired_signal_operation_locks(db: DB | None = None) -> None:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        conn.execute("DELETE FROM signal_operation_locks WHERE expires_at <= ?", (utc_now_iso(),))


def acquire_signal_operation_lock(
    lock_name: str,
    ttl_seconds: int,
    locked_by: str | None = None,
    metadata: Any | None = None,
    db: DB | None = None,
) -> bool:
    db = db or DB()
    ensure_signals_schema(db)
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    expires_at = (now_dt + timedelta(seconds=int(ttl_seconds))).isoformat()
    normalized = str(lock_name).strip().upper()
    owner = locked_by or f"signal_scheduler:{uuid.uuid4().hex[:8]}"
    with db.connect() as conn:
        conn.execute("DELETE FROM signal_operation_locks WHERE lock_name=? AND expires_at <= ?", (normalized, now))
        try:
            conn.execute(
                """
                INSERT INTO signal_operation_locks
                    (lock_name, locked_by, locked_at, expires_at, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (normalized, owner, now, expires_at, _json_or_text(metadata), now, now),
            )
            return True
        except Exception:
            row = conn.execute(
                "SELECT expires_at FROM signal_operation_locks WHERE lock_name=?",
                (normalized,),
            ).fetchone()
            if row and str(row["expires_at"]) <= now:
                conn.execute(
                    """
                    UPDATE signal_operation_locks
                    SET locked_by=?, locked_at=?, expires_at=?, metadata=?, updated_at=?
                    WHERE lock_name=?
                    """,
                    (owner, now, expires_at, _json_or_text(metadata), now, normalized),
                )
                return True
            return False


def release_signal_operation_lock(
    lock_name: str,
    locked_by: str | None = None,
    db: DB | None = None,
) -> bool:
    db = db or DB()
    ensure_signals_schema(db)
    normalized = str(lock_name).strip().upper()
    where = "lock_name=?"
    params: list[Any] = [normalized]
    if locked_by is not None:
        where += " AND locked_by=?"
        params.append(locked_by)
    with db.connect() as conn:
        cur = conn.execute(f"DELETE FROM signal_operation_locks WHERE {where}", params)
        return cur.rowcount > 0


def is_signal_operation_locked(lock_name: str, db: DB | None = None) -> bool:
    db = db or DB()
    ensure_signals_schema(db)
    normalized = str(lock_name).strip().upper()
    with db.connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM signal_operation_locks WHERE lock_name=? AND expires_at > ?",
            (normalized, utc_now_iso()),
        ).fetchone()
    return row is not None


def get_signal_setting(key: str, default: Any = None, db: DB | None = None) -> Any:
    db = db or DB()
    ensure_signals_schema(db)
    with db.connect() as conn:
        row = conn.execute("SELECT value FROM signal_system_settings WHERE key=?", (str(key),)).fetchone()
    return row["value"] if row else default


def set_signal_setting(key: str, value: Any, db: DB | None = None) -> None:
    db = db or DB()
    ensure_signals_schema(db)
    now = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO signal_system_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (str(key), str(value), now),
        )


def _setting_truthy(key: str, db: DB | None = None) -> bool:
    value = get_signal_setting(key, "0", db=db)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def is_signal_generation_paused(db: DB | None = None) -> bool:
    return _setting_truthy("signal_generation_paused", db=db)


def is_pair_discovery_paused(db: DB | None = None) -> bool:
    return _setting_truthy("pair_discovery_paused", db=db)


def is_status_updater_paused(db: DB | None = None) -> bool:
    return _setting_truthy("status_updater_paused", db=db)


def create_signal_notification(data: dict[str, Any], db: DB | None = None) -> str:
    db = db or DB()
    ensure_signals_schema(db)
    payload = dict(data or {})
    _require(payload, ["event_type", "title", "message"])
    now = utc_now_iso()
    payload.setdefault("id", _new_id("signotif"))
    if payload.get("symbol"):
        payload["symbol"] = str(payload["symbol"]).upper()
    payload.setdefault("channel", "in_app")
    payload.setdefault("status", "PENDING")
    payload.setdefault("created_at", now)
    payload.setdefault("updated_at", now)
    return _insert_row(db, "signal_notifications", payload)


def list_signal_notifications(
    user_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: DB | None = None,
) -> list[dict[str, Any]]:
    db = db or DB()
    ensure_signals_schema(db)
    where: list[str] = []
    params: list[Any] = []
    if user_id is not None:
        where.append("(user_id IS NULL OR user_id = ?)")
        params.append(user_id)
    if status is not None:
        where.append("status = ?")
        params.append(status)
    query = "SELECT * FROM signal_notifications"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([int(limit), int(offset)])
    with db.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def mark_signal_notification_read(notification_id: str, user_id: str | None = None, db: DB | None = None) -> bool:
    db = db or DB()
    ensure_signals_schema(db)
    now = utc_now_iso()
    where = ["id = ?"]
    params: list[Any] = [notification_id]
    if user_id is not None:
        where.append("(user_id IS NULL OR user_id = ?)")
        params.append(user_id)
    with db.connect() as conn:
        cur = conn.execute(
            f"""
            UPDATE signal_notifications
            SET status = 'READ', read_at = ?, updated_at = ?
            WHERE {' AND '.join(where)}
            """,
            (now, now, *params),
        )
    return cur.rowcount > 0


def create_signal_event_notification(
    signal_id: str,
    event_type: str,
    title: str,
    message: str,
    symbol: str | None = None,
    db: DB | None = None,
) -> str:
    return create_signal_notification(
        {
            "user_id": None,
            "signal_id": signal_id,
            "symbol": symbol,
            "event_type": event_type,
            "title": title,
            "message": message,
            "channel": "in_app",
            "status": "PENDING",
        },
        db=db,
    )
