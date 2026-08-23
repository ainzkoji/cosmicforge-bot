from __future__ import annotations

from typing import Any

from app.persistence.db import AdminDB


SIGNAL_COLUMNS = (
    "id",
    "candidate_id",
    "asset_class",
    "symbol",
    "side",
    "timeframe",
    "strategy_name",
    "entry_price",
    "entry_zone_low",
    "entry_zone_high",
    "stop_loss",
    "take_profit_1",
    "take_profit_2",
    "take_profit_3",
    "risk_reward",
    "confidence_score",
    "signal_reason",
    "status",
    "is_published",
    "source",
    "dev_mode",
    "created_at",
    "published_at",
    "expires_at",
    "invalidated_at",
    "tp1_hit_at",
    "tp2_hit_at",
    "tp3_hit_at",
    "sl_hit_at",
    "cancelled_at",
    "updated_at",
)

CANDIDATE_COLUMNS = (
    "id",
    "asset_class",
    "symbol",
    "side",
    "timeframe",
    "strategy_name",
    "entry_price",
    "entry_zone_low",
    "entry_zone_high",
    "stop_loss",
    "take_profit_1",
    "take_profit_2",
    "take_profit_3",
    "risk_reward",
    "confidence_score",
    "signal_reason",
    "rejection_reason",
    "source",
    "status",
    "dev_mode",
    "created_at",
    "updated_at",
)

PAIR_COLUMNS = (
    "symbol",
    "exchange",
    "asset_class",
    "quote_asset",
    "contract_type",
    "tier",
    "enabled",
    "whitelisted",
    "blacklisted",
    "blacklist_reason",
    "discovered_at",
    "last_seen_at",
    "created_at",
    "updated_at",
)

METRIC_COLUMNS = (
    "symbol",
    "exchange",
    "quote_volume_24h",
    "spread_percent",
    "bid_price",
    "ask_price",
    "candle_count",
    "atr_percent",
    "volatility_score",
    "liquidity_score",
    "spread_score",
    "reliability_score",
    "is_safe",
    "unsafe_reason",
    "last_updated",
)

SCAN_RUN_COLUMNS = (
    "id",
    "scan_type",
    "started_at",
    "ended_at",
    "duration_seconds",
    "symbols_discovered",
    "symbols_eligible",
    "symbols_scanned",
    "candidates_created",
    "signals_published",
    "errors",
    "status",
    "created_at",
    "updated_at",
)

SCAN_RESULT_COLUMNS = (
    "id",
    "scan_run_id",
    "symbol",
    "was_scanned",
    "was_skipped",
    "skip_reason",
    "candidate_count",
    "accepted_count",
    "rejected_count",
    "published_count",
    "error",
    "created_at",
)

SIGNAL_STATUSES = {
    "PENDING_ENTRY",
    "ACTIVE",
    "EXPIRED",
    "TP1_HIT",
    "TP2_HIT",
    "TP3_HIT",
    "SL_HIT",
    "CANCELLED",
    "INVALIDATED",
}
CANDIDATE_STATUSES = {"CANDIDATE", "ACCEPTED", "REJECTED"}
ALLOWED_SIDES = {"BUY", "SELL"}


class InvalidSignalFilter(ValueError):
    pass


def _bounded_limit(limit: int | None, *, default: int, maximum: int) -> int:
    try:
        parsed = int(limit or default)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, maximum))


def _bounded_offset(offset: int | None) -> int:
    try:
        parsed = int(offset or 0)
    except (TypeError, ValueError):
        parsed = 0
    return max(0, parsed)


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _normalize_asset_class(asset_class: str | None) -> str:
    return (asset_class or "crypto").strip().lower()


def _normalize_symbol(symbol: str | None) -> str | None:
    return symbol.strip().upper() if symbol and symbol.strip() else None


def _normalize_side(side: str | None) -> str | None:
    if not side:
        return None
    normalized = side.strip().upper()
    if normalized not in ALLOWED_SIDES:
        raise InvalidSignalFilter(f"Unsupported side: {side}")
    return normalized


def _normalize_status(status: str | None, *, allowed: set[str]) -> str | None:
    if not status:
        return None
    normalized = status.strip().upper()
    if normalized not in allowed:
        raise InvalidSignalFilter(f"Unsupported status: {status}")
    return normalized


def _normalize_optional_bool(value: bool | int | str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        if value in (0, 1):
            return value
        raise InvalidSignalFilter("Boolean filter must be 0 or 1")
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return 1
    if normalized in {"0", "false", "no", "off"}:
        return 0
    raise InvalidSignalFilter(f"Invalid boolean filter: {value}")


def _query_items(
    db: AdminDB,
    *,
    table: str,
    columns: tuple[str, ...],
    where: list[str],
    params: list[Any],
    limit: int,
    offset: int,
    order_by: str,
) -> dict[str, Any]:
    with db.connect() as conn:
        if not _table_exists(conn, table):
            return {"items": [], "count": 0, "limit": limit, "offset": offset}
        where_sql = " AND ".join(where) if where else "1=1"
        column_sql = ", ".join(columns)
        count_row = conn.execute(
            f"SELECT COUNT(*) AS count FROM {table} WHERE {where_sql}",
            params,
        ).fetchone()
        rows = conn.execute(
            f"""
            SELECT {column_sql}
            FROM {table}
            WHERE {where_sql}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        ).fetchall()
    return {
        "items": [dict(row) for row in rows],
        "count": int(count_row["count"] if count_row else 0),
        "limit": limit,
        "offset": offset,
    }


def list_trading_signals(
    db: AdminDB,
    *,
    asset_class: str | None = "crypto",
    status: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    is_published: bool | int | str | None = None,
    dev_mode: bool | int | str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    where: list[str] = ["LOWER(asset_class) = ?"]
    params: list[Any] = [_normalize_asset_class(asset_class)]
    normalized_status = _normalize_status(status, allowed=SIGNAL_STATUSES)
    normalized_symbol = _normalize_symbol(symbol)
    normalized_side = _normalize_side(side)
    normalized_published = _normalize_optional_bool(is_published)
    normalized_dev_mode = _normalize_optional_bool(dev_mode)
    if normalized_status:
        where.append("status = ?")
        params.append(normalized_status)
    if normalized_symbol:
        where.append("UPPER(symbol) = ?")
        params.append(normalized_symbol)
    if normalized_side:
        where.append("UPPER(side) = ?")
        params.append(normalized_side)
    if normalized_dev_mode is not None:
        where.append("COALESCE(dev_mode, 0) = ?")
        params.append(normalized_dev_mode)
    if normalized_published is not None:
        where.append("COALESCE(is_published, 0) = ?")
        params.append(normalized_published)
    return _query_items(
        db,
        table="trading_signals",
        columns=SIGNAL_COLUMNS,
        where=where,
        params=params,
        limit=_bounded_limit(limit, default=50, maximum=100),
        offset=_bounded_offset(offset),
        order_by="created_at DESC",
    )


def list_signal_candidates(
    db: AdminDB,
    *,
    asset_class: str | None = "crypto",
    status: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    dev_mode: bool | int | str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    where: list[str] = ["LOWER(asset_class) = ?"]
    params: list[Any] = [_normalize_asset_class(asset_class)]
    normalized_status = _normalize_status(status, allowed=CANDIDATE_STATUSES)
    normalized_symbol = _normalize_symbol(symbol)
    normalized_side = _normalize_side(side)
    normalized_dev_mode = _normalize_optional_bool(dev_mode)
    if normalized_status:
        where.append("status = ?")
        params.append(normalized_status)
    if normalized_symbol:
        where.append("UPPER(symbol) = ?")
        params.append(normalized_symbol)
    if normalized_side:
        where.append("UPPER(side) = ?")
        params.append(normalized_side)
    if normalized_dev_mode is not None:
        where.append("COALESCE(dev_mode, 0) = ?")
        params.append(normalized_dev_mode)
    return _query_items(
        db,
        table="signal_candidates",
        columns=CANDIDATE_COLUMNS,
        where=where,
        params=params,
        limit=_bounded_limit(limit, default=50, maximum=100),
        offset=_bounded_offset(offset),
        order_by="created_at DESC",
    )


def list_signal_pairs(
    db: AdminDB,
    *,
    exchange: str | None = None,
    asset_class: str | None = None,
    quote_asset: str | None = None,
    contract_type: str | None = None,
    tier: str | None = None,
    enabled: bool | int | str | None = None,
    whitelisted: bool | int | str | None = None,
    blacklisted: bool | int | str | None = None,
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    filters = {
        "exchange": exchange,
        "asset_class": asset_class,
        "quote_asset": quote_asset,
        "contract_type": contract_type,
        "tier": tier,
        "enabled": _normalize_optional_bool(enabled),
        "whitelisted": _normalize_optional_bool(whitelisted),
        "blacklisted": _normalize_optional_bool(blacklisted),
    }
    for column, value in filters.items():
        if value is None:
            continue
        where.append(f"{column} = ?")
        params.append(value)
    if search:
        where.append("UPPER(symbol) LIKE ?")
        params.append(f"%{search.strip().upper()}%")
    return _query_items(
        db,
        table="signal_pair_universe",
        columns=PAIR_COLUMNS,
        where=where,
        params=params,
        limit=_bounded_limit(limit, default=100, maximum=200),
        offset=_bounded_offset(offset),
        order_by="updated_at DESC",
    )


def list_signal_pair_metrics(
    db: AdminDB,
    *,
    exchange: str | None = None,
    is_safe: bool | int | str | None = None,
    min_volume: float | None = None,
    max_spread: float | None = None,
    symbol: str | None = None,
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    normalized_is_safe = _normalize_optional_bool(is_safe)
    normalized_limit = _bounded_limit(limit, default=100, maximum=200)
    normalized_offset = _bounded_offset(offset)
    where: list[str] = []
    params: list[Any] = []
    if exchange is not None:
        where.append("m.exchange = ?")
        params.append(exchange)
    if normalized_is_safe is not None:
        where.append("m.is_safe = ?")
        params.append(normalized_is_safe)
    if min_volume is not None:
        where.append("m.quote_volume_24h >= ?")
        params.append(float(min_volume))
    if max_spread is not None:
        where.append("m.spread_percent <= ?")
        params.append(float(max_spread))
    if normalized_symbol is not None:
        where.append("UPPER(m.symbol) = ?")
        params.append(normalized_symbol)
    if search:
        where.append("UPPER(m.symbol) LIKE ?")
        params.append(f"%{search.strip().upper()}%")
    where_sql = " AND ".join(where) if where else "1=1"
    metric_sql = ", ".join(f"m.{column}" for column in METRIC_COLUMNS)
    with db.connect() as conn:
        if not _table_exists(conn, "signal_pair_metrics"):
            return {"items": [], "count": 0, "limit": normalized_limit, "offset": normalized_offset}
        count_row = conn.execute(
            f"SELECT COUNT(*) AS count FROM signal_pair_metrics m WHERE {where_sql}",
            params,
        ).fetchone()
        pair_fields = (
            "u.tier AS tier, u.enabled AS enabled, u.whitelisted AS whitelisted, "
            "u.blacklisted AS blacklisted"
        )
        join_sql = "LEFT JOIN signal_pair_universe u ON u.symbol = m.symbol" if _table_exists(conn, "signal_pair_universe") else ""
        pair_sql = pair_fields if join_sql else "NULL AS tier, NULL AS enabled, NULL AS whitelisted, NULL AS blacklisted"
        rows = conn.execute(
            f"""
            SELECT {metric_sql}, {pair_sql}
            FROM signal_pair_metrics m
            {join_sql}
            WHERE {where_sql}
            ORDER BY m.reliability_score DESC, m.quote_volume_24h DESC
            LIMIT ? OFFSET ?
            """,
            (*params, normalized_limit, normalized_offset),
        ).fetchall()
    return {
        "items": [dict(row) for row in rows],
        "count": int(count_row["count"] if count_row else 0),
        "limit": normalized_limit,
        "offset": normalized_offset,
    }


def list_signal_scan_runs(
    db: AdminDB,
    *,
    scan_type: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if scan_type:
        where.append("scan_type = ?")
        params.append(scan_type)
    if status:
        where.append("status = ?")
        params.append(status)
    return _query_items(
        db,
        table="signal_scan_runs",
        columns=SCAN_RUN_COLUMNS,
        where=where,
        params=params,
        limit=_bounded_limit(limit, default=50, maximum=200),
        offset=_bounded_offset(offset),
        order_by="started_at DESC",
    )


def get_signal_scan_run_detail(db: AdminDB, scan_run_id: str) -> dict[str, Any] | None:
    with db.connect() as conn:
        if not _table_exists(conn, "signal_scan_runs"):
            return None
        scan_run = conn.execute(
            f"SELECT {', '.join(SCAN_RUN_COLUMNS)} FROM signal_scan_runs WHERE id = ?",
            (scan_run_id,),
        ).fetchone()
        if not scan_run:
            return None
        if _table_exists(conn, "signal_scan_results"):
            rows = conn.execute(
                f"""
                SELECT {', '.join(SCAN_RESULT_COLUMNS)}
                FROM signal_scan_results
                WHERE scan_run_id = ?
                ORDER BY created_at DESC
                LIMIT 1000 OFFSET 0
                """,
                (scan_run_id,),
            ).fetchall()
        else:
            rows = []
    return {"scan_run": dict(scan_run), "results": [dict(row) for row in rows]}
