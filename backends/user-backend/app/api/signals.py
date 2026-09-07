from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api.auth import get_current_active_user
from app.core.config import settings
from shared_lib.persistence.db import DB, utc_now_iso
from shared_lib.persistence.signals import (
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_INVALIDATED,
    SIGNAL_STATUS_PENDING_ENTRY,
    SIGNAL_STATUS_SL_HIT,
    SIGNAL_STATUS_TP1_HIT,
    SIGNAL_STATUS_TP2_HIT,
    SIGNAL_STATUS_TP3_HIT,
    add_user_signal_favorite,
    add_user_signal_hidden_symbol,
    create_signal_event_notification,
    ensure_signals_schema,
    get_or_create_user_signal_preferences,
    list_signal_notifications,
    mark_signal_notification_read,
    normalize_user_signal_preferences,
    remove_user_signal_favorite,
    remove_user_signal_hidden_symbol,
    update_user_signal_preferences,
)


router = APIRouter()

DISCLAIMER = (
    "Signals are for educational and informational purposes only. Trading involves risk. "
    "Past performance does not guarantee future results."
)

PUBLIC_SIGNAL_COLUMNS = (
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
    "status",
    "is_published",
    "dev_mode",
    "published_at",
    "expires_at",
    "created_at",
    "updated_at",
)

ACTIVE_STATUSES = {SIGNAL_STATUS_ACTIVE, SIGNAL_STATUS_PENDING_ENTRY}
HISTORY_STATUSES = {
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_TP1_HIT,
    SIGNAL_STATUS_TP2_HIT,
    SIGNAL_STATUS_TP3_HIT,
    SIGNAL_STATUS_SL_HIT,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_INVALIDATED,
}
ALL_SIGNAL_STATUSES = ACTIVE_STATUSES | HISTORY_STATUSES
ALLOWED_SIDES = {"BUY", "SELL"}
ALLOWED_TIMEFRAMES = {"15M", "30M", "1H", "4H"}
ALLOWED_SORTS = {"newest", "confidence", "time_left", "risk_reward"}
ALLOWED_RISK_STYLES = {"conservative", "balanced", "aggressive"}
MAJOR_SYMBOLS = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}


class SignalPreferencesUpdate(BaseModel):
    crypto_enabled: bool | None = None
    forex_enabled: bool | None = None
    favorite_symbols: list[str] | None = None
    hidden_symbols: list[str] | None = None
    minimum_confidence: float | None = None
    majors_only: bool | None = None
    risk_style: str | None = None
    notifications_enabled: bool | None = None
    notify_new_signal: bool | None = None
    notify_signal_invalidated: bool | None = None
    notify_tp1_hit: bool | None = None
    notify_tp2_hit: bool | None = None
    notify_tp3_hit: bool | None = None
    notify_sl_hit: bool | None = None
    notify_entry_window_expiring: bool | None = None


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
    return max(1, min(int(limit or 20), 100))


def _normalize_offset(offset: int) -> int:
    return max(0, int(offset or 0))


def _normalize_asset_class(asset_class: str | None) -> str:
    return (asset_class or "crypto").strip().lower()


def _normalize_symbol(symbol: str | None) -> str | None:
    return symbol.strip().upper() if symbol and symbol.strip() else None


def _normalize_status(status: str | None, *, allowed: set[str] | None = None) -> str | None:
    if not status:
        return None
    normalized = status.strip().upper()
    allowed_statuses = allowed or ALL_SIGNAL_STATUSES
    if normalized not in allowed_statuses:
        raise HTTPException(status_code=400, detail=f"Unsupported signal status: {status}")
    return normalized


def _normalize_side(side: str | None) -> str | None:
    if not side:
        return None
    normalized = side.strip().upper()
    if normalized not in ALLOWED_SIDES:
        raise HTTPException(status_code=400, detail=f"Unsupported signal side: {side}")
    return normalized


def _normalize_timeframe(timeframe: str | None) -> str | None:
    if not timeframe:
        return None
    normalized = timeframe.strip().upper()
    if normalized not in ALLOWED_TIMEFRAMES:
        raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {timeframe}")
    return normalized


def _normalize_sort(sort: str | None) -> str:
    normalized = (sort or "newest").strip().lower()
    if normalized not in ALLOWED_SORTS:
        raise HTTPException(status_code=400, detail=f"Unsupported sort: {sort}")
    return normalized


def _normalize_optional_bool(value: bool | int | str | None) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
        raise HTTPException(status_code=400, detail="Boolean filter must be 0 or 1")
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise HTTPException(status_code=400, detail=f"Invalid boolean filter: {value}")


def _user_id(user: dict) -> str:
    user_id = user.get("id") or user.get("sub") or user.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User identity unavailable")
    return str(user_id)


def _format_preferences(row: dict[str, Any] | None) -> dict[str, Any]:
    normalized = normalize_user_signal_preferences(row) or {}
    return {
        "user_id": normalized.get("user_id"),
        "crypto_enabled": bool(normalized.get("crypto_enabled", True)),
        "forex_enabled": bool(normalized.get("forex_enabled", False)),
        "favorite_symbols": normalized.get("favorite_symbols") or [],
        "hidden_symbols": normalized.get("hidden_symbols") or [],
        "minimum_confidence": normalized.get("minimum_confidence", 70),
        "majors_only": bool(normalized.get("majors_only", False)),
        "risk_style": normalized.get("risk_style") or "balanced",
        "notifications_enabled": bool(normalized.get("notifications_enabled", True)),
        "notify_new_signal": bool(normalized.get("notify_new_signal", True)),
        "notify_signal_invalidated": bool(normalized.get("notify_signal_invalidated", True)),
        "notify_tp1_hit": bool(normalized.get("notify_tp1_hit", False)),
        "notify_tp2_hit": bool(normalized.get("notify_tp2_hit", True)),
        "notify_tp3_hit": bool(normalized.get("notify_tp3_hit", True)),
        "notify_sl_hit": bool(normalized.get("notify_sl_hit", True)),
        "notify_entry_window_expiring": bool(normalized.get("notify_entry_window_expiring", True)),
    }


def _validate_symbol_list(symbols: list[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    normalized: list[str] = []
    for symbol in symbols:
        value = _normalize_symbol(symbol)
        if not value:
            raise HTTPException(status_code=400, detail="INVALID_SYMBOL")
        if value not in normalized:
            normalized.append(value)
    return normalized


def _effective_min_confidence(value: float | None, preferences: dict[str, Any]) -> float | None:
    if value is not None:
        return float(value)
    minimum = preferences.get("minimum_confidence")
    return float(minimum) if minimum is not None else None


def _order_by(sort: str) -> str:
    if sort == "confidence":
        return "confidence_score DESC, COALESCE(published_at, created_at) DESC"
    if sort == "time_left":
        return "expires_at ASC, COALESCE(published_at, created_at) DESC"
    if sort == "risk_reward":
        return "risk_reward DESC, COALESCE(published_at, created_at) DESC"
    return "COALESCE(published_at, created_at) DESC, created_at DESC"


def _append_symbol_list_filter(where: list[str], params: list[Any], symbols: set[str], *, include: bool) -> None:
    if not symbols:
        if include:
            where.append("1 = 0")
        return
    placeholders = ", ".join("?" for _ in symbols)
    operator = "IN" if include else "NOT IN"
    where.append(f"UPPER(symbol) {operator} ({placeholders})")
    params.extend(sorted(symbols))


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


def _time_left_seconds(expires_at: str | None) -> int | None:
    expires = _parse_iso(expires_at)
    if not expires:
        return None
    return max(0, int((expires - datetime.now(timezone.utc)).total_seconds()))


def _public_signal(row: Any, *, include_time_left: bool = False, normalize_expired: bool = False) -> dict[str, Any]:
    item = {column: row[column] for column in PUBLIC_SIGNAL_COLUMNS if column in row.keys()}
    if normalize_expired and item.get("status") in ACTIVE_STATUSES and _time_left_seconds(item.get("expires_at")) == 0:
        item["status"] = SIGNAL_STATUS_EXPIRED
    if include_time_left:
        item["time_left_seconds"] = _time_left_seconds(item.get("expires_at"))
    return item


def _base_public_filters(
    *,
    asset_class: str,
    production: bool,
    status: str | None = None,
    symbol: str | None = None,
    search: str | None = None,
    side: str | None = None,
    timeframe: str | None = None,
    min_confidence: float | None = None,
) -> tuple[list[str], list[Any]]:
    where = ["is_published = 1", "LOWER(asset_class) = ?"]
    params: list[Any] = [asset_class]
    if production:
        where.append("COALESCE(dev_mode, 0) = 0")
    if status:
        where.append("status = ?")
        params.append(status)
    if symbol:
        where.append("UPPER(symbol) = ?")
        params.append(symbol)
    if search:
        where.append("UPPER(symbol) LIKE ?")
        params.append(f"%{search.strip().upper()}%")
    if side:
        where.append("UPPER(side) = ?")
        params.append(side)
    if timeframe:
        where.append("UPPER(timeframe) = ?")
        params.append(timeframe)
    if min_confidence is not None:
        where.append("confidence_score >= ?")
        params.append(float(min_confidence))
    return where, params


def _query_signals(
    db: DB,
    *,
    where: list[str],
    params: list[Any],
    limit: int,
    offset: int,
    order_by: str = "COALESCE(published_at, created_at) DESC, created_at DESC",
    normalize_expired: bool = False,
) -> dict[str, Any]:
    ensure_signals_schema(db)
    where_sql = " AND ".join(where)
    columns = ", ".join(PUBLIC_SIGNAL_COLUMNS)
    with db.connect() as conn:
        count_row = conn.execute(
            f"SELECT COUNT(*) AS count FROM trading_signals WHERE {where_sql}",
            params,
        ).fetchone()
        rows = conn.execute(
            f"""
            SELECT {columns}
            FROM trading_signals
            WHERE {where_sql}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        ).fetchall()
    return {
        "items": [_public_signal(row, normalize_expired=normalize_expired) for row in rows],
        "count": int(count_row["count"] if count_row else 0),
        "limit": limit,
        "offset": offset,
    }


@router.get("")
def list_signals(
    asset_class: str | None = Query(default="crypto"),
    status: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    search: str | None = Query(default=None),
    side: str | None = Query(default=None),
    timeframe: str | None = Query(default=None),
    min_confidence: float | None = Query(default=None),
    sort: str | None = Query(default="newest"),
    favorites_only: bool | int | str | None = Query(default=None),
    majors_only: bool | int | str | None = Query(default=None),
    include_hidden: bool | int | str | None = Query(default=None),
    limit: int = Query(default=20, ge=1),
    offset: int = Query(default=0, ge=0),
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    preferences = _format_preferences(get_or_create_user_signal_preferences(_user_id(user), db=db))
    normalized_asset_class = _normalize_asset_class(asset_class)
    where, params = _base_public_filters(
        asset_class=normalized_asset_class,
        production=_is_production(),
        status=_normalize_status(status),
        symbol=_normalize_symbol(symbol),
        search=search,
        side=_normalize_side(side),
        timeframe=_normalize_timeframe(timeframe),
        min_confidence=_effective_min_confidence(min_confidence, preferences),
    )
    if _normalize_optional_bool(favorites_only):
        _append_symbol_list_filter(where, params, set(preferences.get("favorite_symbols") or []), include=True)
    if _normalize_optional_bool(majors_only) or (majors_only is None and preferences.get("majors_only")):
        _append_symbol_list_filter(where, params, MAJOR_SYMBOLS, include=True)
    if not _normalize_optional_bool(include_hidden):
        _append_symbol_list_filter(where, params, set(preferences.get("hidden_symbols") or []), include=False)
    return _query_signals(
        db,
        where=where,
        params=params,
        limit=_normalize_limit(limit),
        offset=_normalize_offset(offset),
        order_by=_order_by(_normalize_sort(sort)),
    )


@router.get("/active")
def list_active_signals(
    asset_class: str | None = Query(default="crypto"),
    symbol: str | None = Query(default=None),
    search: str | None = Query(default=None),
    side: str | None = Query(default=None),
    timeframe: str | None = Query(default=None),
    min_confidence: float | None = Query(default=None),
    status: str | None = Query(default=None),
    sort: str | None = Query(default="time_left"),
    favorites_only: bool | int | str | None = Query(default=None),
    majors_only: bool | int | str | None = Query(default=None),
    include_hidden: bool | int | str | None = Query(default=None),
    limit: int = Query(default=20, ge=1),
    offset: int = Query(default=0, ge=0),
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    preferences = _format_preferences(get_or_create_user_signal_preferences(_user_id(user), db=db))
    normalized_asset_class = _normalize_asset_class(asset_class)
    where, params = _base_public_filters(
        asset_class=normalized_asset_class,
        production=_is_production(),
        status=_normalize_status(status, allowed=ACTIVE_STATUSES) if status else None,
        symbol=_normalize_symbol(symbol),
        search=search,
        side=_normalize_side(side),
        timeframe=_normalize_timeframe(timeframe),
        min_confidence=_effective_min_confidence(min_confidence, preferences),
    )
    if not status:
        where.append("status IN (?, ?)")
        params.extend([SIGNAL_STATUS_ACTIVE, SIGNAL_STATUS_PENDING_ENTRY])
    where.append("expires_at > ?")
    params.append(utc_now_iso())
    if _normalize_optional_bool(favorites_only):
        _append_symbol_list_filter(where, params, set(preferences.get("favorite_symbols") or []), include=True)
    if _normalize_optional_bool(majors_only) or (majors_only is None and preferences.get("majors_only")):
        _append_symbol_list_filter(where, params, MAJOR_SYMBOLS, include=True)
    if not _normalize_optional_bool(include_hidden):
        _append_symbol_list_filter(where, params, set(preferences.get("hidden_symbols") or []), include=False)
    return _query_signals(
        db,
        where=where,
        params=params,
        limit=_normalize_limit(limit),
        offset=_normalize_offset(offset),
        order_by=_order_by(_normalize_sort(sort)),
    )


@router.get("/history")
def list_signal_history(
    asset_class: str | None = Query(default="crypto"),
    status: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    search: str | None = Query(default=None),
    side: str | None = Query(default=None),
    timeframe: str | None = Query(default=None),
    min_confidence: float | None = Query(default=None),
    sort: str | None = Query(default="newest"),
    favorites_only: bool | int | str | None = Query(default=None),
    majors_only: bool | int | str | None = Query(default=None),
    include_hidden: bool | int | str | None = Query(default=None),
    limit: int = Query(default=20, ge=1),
    offset: int = Query(default=0, ge=0),
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    preferences = _format_preferences(get_or_create_user_signal_preferences(_user_id(user), db=db))
    normalized_status = _normalize_status(status, allowed=HISTORY_STATUSES) if status else None
    normalized_asset_class = _normalize_asset_class(asset_class)
    where, params = _base_public_filters(
        asset_class=normalized_asset_class,
        production=_is_production(),
        status=None if normalized_status == SIGNAL_STATUS_EXPIRED else normalized_status,
        symbol=_normalize_symbol(symbol),
        search=search,
        side=_normalize_side(side),
        timeframe=_normalize_timeframe(timeframe),
        min_confidence=_effective_min_confidence(min_confidence, preferences),
    )
    if not normalized_status:
        placeholders = ", ".join("?" for _ in HISTORY_STATUSES)
        active_placeholders = ", ".join("?" for _ in ACTIVE_STATUSES)
        where.append(f"(status IN ({placeholders}) OR (status IN ({active_placeholders}) AND expires_at <= ?))")
        params.extend(sorted(HISTORY_STATUSES))
        params.extend(sorted(ACTIVE_STATUSES))
        params.append(utc_now_iso())
    elif normalized_status == SIGNAL_STATUS_EXPIRED:
        active_placeholders = ", ".join("?" for _ in ACTIVE_STATUSES)
        where.append(f"(status = ? OR (status IN ({active_placeholders}) AND expires_at <= ?))")
        params.append(SIGNAL_STATUS_EXPIRED)
        params.extend(sorted(ACTIVE_STATUSES))
        params.append(utc_now_iso())
    if _normalize_optional_bool(favorites_only):
        _append_symbol_list_filter(where, params, set(preferences.get("favorite_symbols") or []), include=True)
    if _normalize_optional_bool(majors_only) or (majors_only is None and preferences.get("majors_only")):
        _append_symbol_list_filter(where, params, MAJOR_SYMBOLS, include=True)
    if not _normalize_optional_bool(include_hidden):
        _append_symbol_list_filter(where, params, set(preferences.get("hidden_symbols") or []), include=False)
    return _query_signals(
        db,
        where=where,
        params=params,
        limit=_normalize_limit(limit),
        offset=_normalize_offset(offset),
        order_by=_order_by(_normalize_sort(sort)),
        normalize_expired=True,
    )


def _preferences_update_payload(payload: SignalPreferencesUpdate) -> dict[str, Any]:
    updates = payload.model_dump(exclude_unset=True)
    if "minimum_confidence" in updates and updates["minimum_confidence"] is not None:
        value = float(updates["minimum_confidence"])
        if value < 50 or value > 95:
            raise HTTPException(status_code=400, detail="MINIMUM_CONFIDENCE_OUT_OF_RANGE")
        updates["minimum_confidence"] = value
    if "risk_style" in updates and updates["risk_style"] is not None:
        risk_style = str(updates["risk_style"]).strip().lower()
        if risk_style not in ALLOWED_RISK_STYLES:
            raise HTTPException(status_code=400, detail="INVALID_RISK_STYLE")
        updates["risk_style"] = risk_style
    for key in ("favorite_symbols", "hidden_symbols"):
        if key in updates:
            updates[key] = _validate_symbol_list(updates[key])
    for key, value in list(updates.items()):
        if isinstance(value, bool):
            updates[key] = int(value)
    return updates


@router.get("/preferences")
def get_signal_preferences(
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    return _format_preferences(get_or_create_user_signal_preferences(_user_id(user), db=db))


@router.put("/preferences")
def update_signal_preferences(
    payload: SignalPreferencesUpdate,
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    user_id = _user_id(user)
    get_or_create_user_signal_preferences(user_id, db=db)
    update_user_signal_preferences(user_id, _preferences_update_payload(payload), db=db)
    return _format_preferences(get_or_create_user_signal_preferences(user_id, db=db))


@router.post("/preferences/favorites/{symbol}")
def add_signal_favorite(
    symbol: str,
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    normalized = _normalize_symbol(symbol)
    if not normalized:
        raise HTTPException(status_code=400, detail="INVALID_SYMBOL")
    return _format_preferences(add_user_signal_favorite(_user_id(user), normalized, db=db))


@router.delete("/preferences/favorites/{symbol}")
def remove_signal_favorite(
    symbol: str,
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    normalized = _normalize_symbol(symbol)
    if not normalized:
        raise HTTPException(status_code=400, detail="INVALID_SYMBOL")
    return _format_preferences(remove_user_signal_favorite(_user_id(user), normalized, db=db))


@router.post("/preferences/hidden/{symbol}")
def add_hidden_signal_symbol(
    symbol: str,
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    normalized = _normalize_symbol(symbol)
    if not normalized:
        raise HTTPException(status_code=400, detail="INVALID_SYMBOL")
    return _format_preferences(add_user_signal_hidden_symbol(_user_id(user), normalized, db=db))


@router.delete("/preferences/hidden/{symbol}")
def remove_hidden_signal_symbol(
    symbol: str,
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    normalized = _normalize_symbol(symbol)
    if not normalized:
        raise HTTPException(status_code=400, detail="INVALID_SYMBOL")
    return _format_preferences(remove_user_signal_hidden_symbol(_user_id(user), normalized, db=db))


@router.get("/notifications")
def get_signal_notifications(
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1),
    offset: int = Query(default=0, ge=0),
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    items = list_signal_notifications(
        user_id=_user_id(user),
        status=status.strip().upper() if status else None,
        limit=_normalize_limit(limit),
        offset=_normalize_offset(offset),
        db=db,
    )
    return {"items": items, "count": len(items), "limit": _normalize_limit(limit), "offset": _normalize_offset(offset)}


@router.post("/notifications/{notification_id}/read")
def mark_signal_notification_as_read(
    notification_id: str,
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    if not mark_signal_notification_read(notification_id, user_id=_user_id(user), db=db):
        raise HTTPException(status_code=404, detail="NOTIFICATION_NOT_FOUND")
    return {"ok": True}


def create_signal_notification_best_effort(
    signal_id: str,
    event_type: str,
    title: str,
    message: str,
    symbol: str | None = None,
    db: DB | None = None,
) -> None:
    try:
        create_signal_event_notification(signal_id, event_type, title, message, symbol=symbol, db=db)
    except Exception:
        # Notification persistence is intentionally best-effort; it must never
        # block signal publishing or lifecycle status updates.
        return


@router.get("/performance")
def get_signal_performance(
    asset_class: str | None = Query(default="crypto"),
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    del user
    ensure_signals_schema(db)
    normalized_asset_class = _normalize_asset_class(asset_class)
    production_filter = "AND COALESCE(ts.dev_mode, 0) = 0" if _is_production() else ""
    history_placeholders = ", ".join("?" for _ in HISTORY_STATUSES)
    active_placeholders = ", ".join("?" for _ in ACTIVE_STATUSES)
    with db.connect() as conn:
        summary = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_signals,
                SUM(CASE WHEN ts.status IN ({active_placeholders}) AND ts.expires_at > ? THEN 1 ELSE 0 END) AS active_signals,
                SUM(CASE WHEN ts.status IN ({history_placeholders}) THEN 1 ELSE 0 END) AS completed_signals,
                SUM(CASE WHEN ts.status = ? THEN 1 ELSE 0 END) AS expired_signals,
                SUM(CASE WHEN COALESCE(sp.tp1_hit, 0) = 1 OR ts.status IN (?, ?, ?) THEN 1 ELSE 0 END) AS tp1_hits,
                SUM(CASE WHEN COALESCE(sp.tp2_hit, 0) = 1 OR ts.status IN (?, ?) THEN 1 ELSE 0 END) AS tp2_hits,
                SUM(CASE WHEN COALESCE(sp.tp3_hit, 0) = 1 OR ts.status = ? THEN 1 ELSE 0 END) AS tp3_hits,
                SUM(CASE WHEN COALESCE(sp.sl_hit, 0) = 1 OR ts.status = ? THEN 1 ELSE 0 END) AS sl_hits,
                SUM(CASE WHEN COALESCE(sp.result, '') = 'WIN' OR COALESCE(sp.tp2_hit, 0) = 1 OR COALESCE(sp.tp3_hit, 0) = 1 OR ts.status IN (?, ?) THEN 1 ELSE 0 END) AS win_count,
                AVG(CASE WHEN ts.status IN ({history_placeholders}) THEN ts.risk_reward ELSE NULL END) AS average_risk_reward
            FROM trading_signals ts
            LEFT JOIN signal_performance sp ON sp.signal_id = ts.id
            WHERE ts.is_published = 1
              AND LOWER(ts.asset_class) = ?
              {production_filter}
            """,
            (
                *sorted(ACTIVE_STATUSES),
                utc_now_iso(),
                *sorted(HISTORY_STATUSES),
                SIGNAL_STATUS_EXPIRED,
                SIGNAL_STATUS_TP1_HIT,
                SIGNAL_STATUS_TP2_HIT,
                SIGNAL_STATUS_TP3_HIT,
                SIGNAL_STATUS_TP2_HIT,
                SIGNAL_STATUS_TP3_HIT,
                SIGNAL_STATUS_TP3_HIT,
                SIGNAL_STATUS_SL_HIT,
                SIGNAL_STATUS_TP2_HIT,
                SIGNAL_STATUS_TP3_HIT,
                *sorted(HISTORY_STATUSES),
                normalized_asset_class,
            ),
        ).fetchone()
        symbol_rows = conn.execute(
            f"""
            SELECT
                ts.symbol,
                SUM(CASE WHEN COALESCE(sp.result, '') = 'WIN' OR ts.status IN (?, ?) THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN COALESCE(sp.result, '') = 'LOSS' OR ts.status = ? THEN 1 ELSE 0 END) AS losses,
                COUNT(*) AS completed
            FROM trading_signals ts
            LEFT JOIN signal_performance sp ON sp.signal_id = ts.id
            WHERE ts.is_published = 1
              AND LOWER(ts.asset_class) = ?
              AND ts.status IN ({history_placeholders})
              {production_filter}
            GROUP BY ts.symbol
            """,
            (
                SIGNAL_STATUS_TP2_HIT,
                SIGNAL_STATUS_TP3_HIT,
                SIGNAL_STATUS_SL_HIT,
                normalized_asset_class,
                *sorted(HISTORY_STATUSES),
            ),
        ).fetchall()

    total = int(summary["total_signals"] or 0) if summary else 0
    active = int(summary["active_signals"] or 0) if summary else 0
    completed = int(summary["completed_signals"] or 0) if summary else 0
    expired = int(summary["expired_signals"] or 0) if summary else 0

    if completed <= 0:
        return {
            "total_signals": total,
            "active_signals": active,
            "completed_signals": completed,
            "expired_signals": expired,
            "tp1_hit_rate": None,
            "tp2_hit_rate": None,
            "tp3_hit_rate": None,
            "sl_hit_rate": None,
            "win_rate": None,
            "average_risk_reward": None,
            "best_symbol": None,
            "worst_symbol": None,
            "message": "Performance data will appear after enough completed signals.",
        }

    def pct(value: Any) -> float:
        return round((float(value or 0) / completed) * 100, 2)

    ranked_symbols = []
    for row in symbol_rows:
        wins = int(row["wins"] or 0)
        losses = int(row["losses"] or 0)
        completed_count = int(row["completed"] or 0)
        if completed_count:
            ranked_symbols.append((row["symbol"], (wins - losses) / completed_count))
    ranked_symbols.sort(key=lambda item: item[1], reverse=True)

    win_count = int(summary["win_count"] or 0) if summary else 0
    return {
        "total_signals": total,
        "active_signals": active,
        "completed_signals": completed,
        "expired_signals": expired,
        "tp1_hit_rate": pct(summary["tp1_hits"]),
        "tp2_hit_rate": pct(summary["tp2_hits"]),
        "tp3_hit_rate": pct(summary["tp3_hits"]),
        "sl_hit_rate": pct(summary["sl_hits"]),
        "win_rate": round((win_count / completed) * 100, 2),
        "average_risk_reward": (
            round(float(summary["average_risk_reward"]), 2)
            if summary and summary["average_risk_reward"] is not None
            else None
        ),
        "best_symbol": ranked_symbols[0][0] if ranked_symbols else None,
        "worst_symbol": ranked_symbols[-1][0] if ranked_symbols else None,
        "message": "Performance metrics are based on published completed signals only.",
    }


@router.get("/{signal_id}")
def get_signal_detail(
    signal_id: str,
    user: dict = Depends(get_current_active_user),
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    del user
    ensure_signals_schema(db)
    where = ["id = ?", "is_published = 1"]
    params: list[Any] = [signal_id]
    if _is_production():
        where.append("COALESCE(dev_mode, 0) = 0")
    columns = ", ".join(PUBLIC_SIGNAL_COLUMNS)
    with db.connect() as conn:
        row = conn.execute(
            f"SELECT {columns} FROM trading_signals WHERE {' AND '.join(where)} LIMIT 1",
            params,
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Signal not found")
    item = _public_signal(row, include_time_left=True)
    item["disclaimer"] = DISCLAIMER
    return item
