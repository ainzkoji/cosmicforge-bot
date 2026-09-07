from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.config import settings
from app.core.deps import require_admin
from shared_lib.persistence.db import DB, utc_now_iso
from shared_lib.persistence.signals import (
    CANDIDATE_STATUS_ACCEPTED,
    CANDIDATE_STATUS_CANDIDATE,
    CANDIDATE_STATUS_REJECTED,
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_INVALIDATED,
    SIGNAL_STATUS_PENDING_ENTRY,
    SIGNAL_STATUS_SL_HIT,
    SIGNAL_STATUS_TP1_HIT,
    SIGNAL_STATUS_TP2_HIT,
    SIGNAL_STATUS_TP3_HIT,
    ensure_signals_schema,
    get_trading_signal,
    publish_trading_signal,
    unpublish_trading_signal,
)


router = APIRouter(prefix="/admin/signals", tags=["admin-signals"])

TRADING_SIGNAL_COLUMNS = (
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

SIGNAL_STATUSES = {
    SIGNAL_STATUS_PENDING_ENTRY,
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_TP1_HIT,
    SIGNAL_STATUS_TP2_HIT,
    SIGNAL_STATUS_TP3_HIT,
    SIGNAL_STATUS_SL_HIT,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_INVALIDATED,
}
CANDIDATE_STATUSES = {
    CANDIDATE_STATUS_CANDIDATE,
    CANDIDATE_STATUS_ACCEPTED,
    CANDIDATE_STATUS_REJECTED,
}
ALLOWED_SIDES = {"BUY", "SELL"}
PUBLISH_BLOCKED_STATUSES = {
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_INVALIDATED,
}


def _get_db() -> DB:
    return DB()


def _is_production() -> bool:
    values = [
        os.getenv("APP_ENV"),
        os.getenv("ENVIRONMENT"),
        os.getenv("ENV"),
        os.getenv("NODE_ENV"),
        getattr(settings, "APP_ENV", None),
        getattr(settings, "ENVIRONMENT", None),
        getattr(settings, "ENV", None),
    ]
    return any(str(value).strip().lower() in {"prod", "production"} for value in values if value)


def _normalize_limit(limit: int) -> int:
    return max(1, min(int(limit or 50), 100))


def _normalize_offset(offset: int) -> int:
    return max(0, int(offset or 0))


def _normalize_asset_class(asset_class: str | None) -> str:
    return (asset_class or "crypto").strip().lower()


def _normalize_symbol(symbol: str | None) -> str | None:
    return symbol.strip().upper() if symbol and symbol.strip() else None


def _normalize_side(side: str | None) -> str | None:
    if not side:
        return None
    normalized = side.strip().upper()
    if normalized not in ALLOWED_SIDES:
        raise HTTPException(status_code=400, detail=f"Unsupported side: {side}")
    return normalized


def _normalize_status(status: str | None, *, allowed: set[str]) -> str | None:
    if not status:
        return None
    normalized = status.strip().upper()
    if normalized not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported status: {status}")
    return normalized


def _normalize_optional_bool(value: bool | int | str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        if value in (0, 1):
            return value
        raise HTTPException(status_code=400, detail="Boolean filter must be 0 or 1")
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return 1
    if normalized in {"0", "false", "no", "off"}:
        return 0
    raise HTTPException(status_code=400, detail=f"Invalid boolean filter: {value}")


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _row_to_dict(row: Any, columns: tuple[str, ...]) -> dict[str, Any]:
    return {column: row[column] for column in columns if column in row.keys()}


def _append_common_filters(
    *,
    where: list[str],
    params: list[Any],
    asset_class: str,
    status: str | None,
    symbol: str | None,
    side: str | None,
    dev_mode: int | None,
) -> None:
    where.append("LOWER(asset_class) = ?")
    params.append(asset_class)
    if status:
        where.append("status = ?")
        params.append(status)
    if symbol:
        where.append("UPPER(symbol) = ?")
        params.append(symbol)
    if side:
        where.append("UPPER(side) = ?")
        params.append(side)
    if dev_mode is not None:
        where.append("COALESCE(dev_mode, 0) = ?")
        params.append(dev_mode)


def _query_table(
    db: DB,
    *,
    table: str,
    columns: tuple[str, ...],
    where: list[str],
    params: list[Any],
    limit: int,
    offset: int,
    order_by: str,
) -> dict[str, Any]:
    ensure_signals_schema(db)
    where_sql = " AND ".join(where) if where else "1=1"
    column_sql = ", ".join(columns)
    with db.connect() as conn:
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
        "items": [_row_to_dict(row, columns) for row in rows],
        "count": int(count_row["count"] if count_row else 0),
        "limit": limit,
        "offset": offset,
    }


def _publish_error(reason: str, status_code: int = 400) -> HTTPException:
    return HTTPException(status_code=status_code, detail=reason)


def validate_signal_publishable(signal: dict[str, Any] | None) -> None:
    if not signal:
        raise _publish_error("SIGNAL_NOT_FOUND", status_code=404)
    if int(signal.get("is_published") or 0) == 1:
        raise _publish_error("ALREADY_PUBLISHED")
    if signal.get("confidence_score") is None or float(signal["confidence_score"]) < 70:
        raise _publish_error("LOW_CONFIDENCE")
    if signal.get("risk_reward") is None or float(signal["risk_reward"]) < 1.8:
        raise _publish_error("LOW_RISK_REWARD")
    if signal.get("entry_price") is None:
        raise _publish_error("MISSING_ENTRY_PRICE")
    if signal.get("stop_loss") is None:
        raise _publish_error("MISSING_STOP_LOSS")
    if signal.get("take_profit_1") is None:
        raise _publish_error("MISSING_TP1")
    if not signal.get("expires_at"):
        raise _publish_error("MISSING_EXPIRY")

    expires_at = _parse_iso(signal.get("expires_at"))
    if not expires_at or expires_at <= datetime.now(timezone.utc):
        raise _publish_error("SIGNAL_EXPIRED")

    status = str(signal.get("status") or "").upper()
    if status == SIGNAL_STATUS_EXPIRED:
        raise _publish_error("SIGNAL_EXPIRED")
    if status == SIGNAL_STATUS_CANCELLED:
        raise _publish_error("SIGNAL_CANCELLED")
    if status == SIGNAL_STATUS_INVALIDATED:
        raise _publish_error("SIGNAL_INVALIDATED")
    if status in PUBLISH_BLOCKED_STATUSES:
        raise _publish_error(f"SIGNAL_{status}")

    if _is_production() and int(signal.get("dev_mode") or 0) == 1:
        raise _publish_error("DEV_SIGNAL_BLOCKED_IN_PRODUCTION")


def _get_admin_signal_or_404(signal_id: str, db: DB) -> dict[str, Any]:
    signal = get_trading_signal(signal_id, db=db)
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")
    return signal


def _get_updated_signal(signal_id: str, db: DB) -> dict[str, Any]:
    signal = _get_admin_signal_or_404(signal_id, db)
    return {column: signal.get(column) for column in TRADING_SIGNAL_COLUMNS}


@router.get("")
def list_admin_signals(
    asset_class: str | None = Query(default="crypto"),
    status: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    side: str | None = Query(default=None),
    is_published: bool | None = Query(default=None),
    dev_mode: bool | None = Query(default=None),
    limit: int = Query(default=50, ge=1),
    offset: int = Query(default=0, ge=0),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    del _admin
    where: list[str] = []
    params: list[Any] = []
    _append_common_filters(
        where=where,
        params=params,
        asset_class=_normalize_asset_class(asset_class),
        status=_normalize_status(status, allowed=SIGNAL_STATUSES),
        symbol=_normalize_symbol(symbol),
        side=_normalize_side(side),
        dev_mode=_normalize_optional_bool(dev_mode),
    )
    published_filter = _normalize_optional_bool(is_published)
    if published_filter is not None:
        where.append("COALESCE(is_published, 0) = ?")
        params.append(published_filter)
    return _query_table(
        db,
        table="trading_signals",
        columns=TRADING_SIGNAL_COLUMNS,
        where=where,
        params=params,
        limit=_normalize_limit(limit),
        offset=_normalize_offset(offset),
        order_by="created_at DESC",
    )


@router.get("/candidates")
def list_admin_signal_candidates(
    asset_class: str | None = Query(default="crypto"),
    status: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    side: str | None = Query(default=None),
    dev_mode: bool | None = Query(default=None),
    limit: int = Query(default=50, ge=1),
    offset: int = Query(default=0, ge=0),
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    del _admin
    where: list[str] = []
    params: list[Any] = []
    _append_common_filters(
        where=where,
        params=params,
        asset_class=_normalize_asset_class(asset_class),
        status=_normalize_status(status, allowed=CANDIDATE_STATUSES),
        symbol=_normalize_symbol(symbol),
        side=_normalize_side(side),
        dev_mode=_normalize_optional_bool(dev_mode),
    )
    return _query_table(
        db,
        table="signal_candidates",
        columns=CANDIDATE_COLUMNS,
        where=where,
        params=params,
        limit=_normalize_limit(limit),
        offset=_normalize_offset(offset),
        order_by="created_at DESC",
    )


@router.post("/{signal_id}/publish")
def publish_admin_signal(
    signal_id: str,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    del _admin
    signal = get_trading_signal(signal_id, db=db)
    validate_signal_publishable(signal)
    publish_trading_signal(signal_id, published_at=utc_now_iso(), db=db)
    return _get_updated_signal(signal_id, db)


@router.post("/{signal_id}/unpublish")
def unpublish_admin_signal(
    signal_id: str,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    del _admin
    _get_admin_signal_or_404(signal_id, db)
    unpublish_trading_signal(signal_id, db=db)
    return _get_updated_signal(signal_id, db)


@router.post("/{signal_id}/cancel")
def cancel_admin_signal(
    signal_id: str,
    _admin: dict = Depends(require_admin),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    del _admin
    _get_admin_signal_or_404(signal_id, db)
    now = utc_now_iso()
    ensure_signals_schema(db)
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE trading_signals
            SET status = ?, cancelled_at = ?, is_published = 0, updated_at = ?
            WHERE id = ?
            """,
            (SIGNAL_STATUS_CANCELLED, now, now, signal_id),
        )
        conn.execute(
            """
            UPDATE signal_performance
            SET cancelled = 1, result = 'CANCELLED', closed_at = ?, updated_at = ?
            WHERE signal_id = ?
            """,
            (now, now, signal_id),
        )
    return _get_updated_signal(signal_id, db)
