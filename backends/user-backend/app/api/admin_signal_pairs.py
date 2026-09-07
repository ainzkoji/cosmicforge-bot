from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.core.deps import require_admin
from shared_lib.persistence.db import DB
from shared_lib.persistence.signals import (
    blacklist_signal_pair,
    disable_signal_pair,
    enable_signal_pair,
    ensure_signals_schema,
    get_signal_pair,
    get_signal_scan_run,
    list_signal_pair_metrics,
    list_signal_pairs,
    list_signal_scan_results,
    list_signal_scan_runs,
    update_signal_pair,
    whitelist_signal_pair,
)


router = APIRouter(prefix="/admin/signals", tags=["admin-signal-pairs"])


class BlacklistRequest(BaseModel):
    reason: str | None = None


class RefreshPairsRequest(BaseModel):
    min_quote_volume_24h: float = 50_000_000.0
    max_spread_percent: float = 0.20
    quote_asset: str = "USDT"
    contract_type: str = "PERPETUAL"
    validate_candles: bool = True
    candle_timeframe: str = "1h"
    min_candles: int = 200


DiscoveryRunner = Callable[[RefreshPairsRequest, DB], dict[str, Any]]


def _get_db() -> DB:
    return DB()


def _normalize_limit(limit: int, default: int = 100, maximum: int = 200) -> int:
    return max(1, min(int(limit or default), maximum))


def _normalize_offset(offset: int) -> int:
    return max(0, int(offset or 0))


def _normalize_symbol(symbol: str) -> str:
    normalized = (symbol or "").strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="SYMBOL_REQUIRED")
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


def _response(items: list[dict[str, Any]], *, count: int, limit: int, offset: int) -> dict[str, Any]:
    return {"items": items, "count": count, "limit": limit, "offset": offset}


def _count_pairs(
    db: DB,
    *,
    exchange: str | None,
    asset_class: str | None,
    quote_asset: str | None,
    contract_type: str | None,
    tier: str | None,
    enabled: int | None,
    whitelisted: int | None,
    blacklisted: int | None,
    search: str | None,
) -> int:
    where: list[str] = []
    params: list[Any] = []
    filters = {
        "exchange": exchange,
        "asset_class": asset_class,
        "quote_asset": quote_asset,
        "contract_type": contract_type,
        "tier": tier,
        "enabled": enabled,
        "whitelisted": whitelisted,
        "blacklisted": blacklisted,
    }
    for column, value in filters.items():
        if value is None:
            continue
        where.append(f"{column}=?")
        params.append(value)
    if search:
        where.append("UPPER(symbol) LIKE ?")
        params.append(f"%{search.strip().upper()}%")
    where_sql = " AND ".join(where) if where else "1=1"
    with db.connect() as conn:
        row = conn.execute(f"SELECT COUNT(*) AS count FROM signal_pair_universe WHERE {where_sql}", params).fetchone()
    return int(row["count"] if row else 0)


def _count_metrics(
    db: DB,
    *,
    exchange: str | None,
    is_safe: int | None,
    min_volume: float | None,
    max_spread: float | None,
    symbol: str | None,
    search: str | None,
) -> int:
    where: list[str] = []
    params: list[Any] = []
    if exchange is not None:
        where.append("exchange=?")
        params.append(exchange)
    if is_safe is not None:
        where.append("is_safe=?")
        params.append(is_safe)
    if min_volume is not None:
        where.append("quote_volume_24h>=?")
        params.append(float(min_volume))
    if max_spread is not None:
        where.append("spread_percent<=?")
        params.append(float(max_spread))
    if symbol is not None:
        where.append("UPPER(symbol)=?")
        params.append(symbol.upper())
    if search:
        where.append("UPPER(symbol) LIKE ?")
        params.append(f"%{search.strip().upper()}%")
    where_sql = " AND ".join(where) if where else "1=1"
    with db.connect() as conn:
        row = conn.execute(f"SELECT COUNT(*) AS count FROM signal_pair_metrics WHERE {where_sql}", params).fetchone()
    return int(row["count"] if row else 0)


def _get_pair_or_404(symbol: str, db: DB) -> dict[str, Any]:
    pair = get_signal_pair(_normalize_symbol(symbol), db=db)
    if not pair:
        raise HTTPException(status_code=404, detail="PAIR_NOT_FOUND")
    return pair


def _run_pair_discovery_script(request: RefreshPairsRequest, db: DB) -> dict[str, Any]:
    backends_dir = Path(__file__).resolve().parents[3]
    script = backends_dir / "bot-backend" / "scripts" / "discover_signal_pairs.py"
    if not script.exists():
        raise HTTPException(status_code=503, detail="PAIR_DISCOVERY_SCRIPT_NOT_AVAILABLE")
    command = [
        sys.executable,
        str(script),
        "--db-path",
        str(db.path),
        "--min-volume",
        str(request.min_quote_volume_24h),
        "--max-spread",
        str(request.max_spread_percent),
        "--quote-asset",
        request.quote_asset,
        "--contract-type",
        request.contract_type,
        "--candle-timeframe",
        request.candle_timeframe,
        "--min-candles",
        str(request.min_candles),
    ]
    if not request.validate_candles:
        command.append("--skip-candle-validation")
    try:
        completed = subprocess.run(
            command,
            cwd=str(backends_dir / "bot-backend"),
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail="PAIR_DISCOVERY_TIMEOUT") from exc

    stdout = completed.stdout.strip()
    try:
        summary = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="PAIR_DISCOVERY_INVALID_RESPONSE") from exc
    if completed.returncode != 0 and not summary:
        detail = completed.stderr.strip() or "PAIR_DISCOVERY_FAILED"
        raise HTTPException(status_code=502, detail=detail)
    return summary


def _get_discovery_runner() -> DiscoveryRunner:
    return _run_pair_discovery_script


def _enrich_metric_rows(rows: list[dict[str, Any]], db: DB) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in rows:
        pair = get_signal_pair(str(row.get("symbol") or ""), db=db) or {}
        enriched.append(
            {
                **row,
                "tier": pair.get("tier"),
                "enabled": pair.get("enabled"),
                "whitelisted": pair.get("whitelisted"),
                "blacklisted": pair.get("blacklisted"),
            }
        )
    return enriched


@router.get("/pairs", dependencies=[Depends(require_admin)])
def list_admin_signal_pairs(
    exchange: str | None = None,
    asset_class: str | None = None,
    quote_asset: str | None = None,
    contract_type: str | None = None,
    tier: str | None = None,
    enabled: bool | int | str | None = Query(default=None),
    whitelisted: bool | int | str | None = Query(default=None),
    blacklisted: bool | int | str | None = Query(default=None),
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    ensure_signals_schema(db)
    normalized_limit = _normalize_limit(limit)
    normalized_offset = _normalize_offset(offset)
    enabled_value = _normalize_optional_bool(enabled)
    whitelisted_value = _normalize_optional_bool(whitelisted)
    blacklisted_value = _normalize_optional_bool(blacklisted)
    items = list_signal_pairs(
        exchange=exchange,
        asset_class=asset_class,
        quote_asset=quote_asset,
        contract_type=contract_type,
        tier=tier,
        enabled=enabled_value,
        whitelisted=whitelisted_value,
        blacklisted=blacklisted_value,
        search=search,
        limit=normalized_limit,
        offset=normalized_offset,
        db=db,
    )
    count = _count_pairs(
        db,
        exchange=exchange,
        asset_class=asset_class,
        quote_asset=quote_asset,
        contract_type=contract_type,
        tier=tier,
        enabled=enabled_value,
        whitelisted=whitelisted_value,
        blacklisted=blacklisted_value,
        search=search,
    )
    return _response(items, count=count, limit=normalized_limit, offset=normalized_offset)


@router.get("/pairs/metrics", dependencies=[Depends(require_admin)])
def list_admin_signal_pair_metrics(
    exchange: str | None = None,
    is_safe: bool | int | str | None = Query(default=None),
    min_volume: float | None = None,
    max_spread: float | None = None,
    symbol: str | None = None,
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    ensure_signals_schema(db)
    normalized_limit = _normalize_limit(limit)
    normalized_offset = _normalize_offset(offset)
    normalized_symbol = _normalize_symbol(symbol) if symbol else None
    is_safe_value = _normalize_optional_bool(is_safe)
    items = list_signal_pair_metrics(
        exchange=exchange,
        is_safe=is_safe_value,
        min_volume=min_volume,
        max_spread=max_spread,
        symbol=normalized_symbol,
        search=search,
        limit=normalized_limit,
        offset=normalized_offset,
        db=db,
    )
    count = _count_metrics(
        db,
        exchange=exchange,
        is_safe=is_safe_value,
        min_volume=min_volume,
        max_spread=max_spread,
        symbol=normalized_symbol,
        search=search,
    )
    return _response(_enrich_metric_rows(items, db), count=count, limit=normalized_limit, offset=normalized_offset)


@router.post("/pairs/{symbol}/enable", dependencies=[Depends(require_admin)])
def enable_admin_signal_pair(symbol: str, db: DB = Depends(_get_db)) -> dict[str, Any]:
    pair = _get_pair_or_404(symbol, db)
    enable_signal_pair(pair["symbol"], db=db)
    updated = _get_pair_or_404(pair["symbol"], db)
    if int(updated.get("blacklisted") or 0):
        updated["warning"] = "BLACKLIST_OVERRIDES_ENABLED"
    return updated


@router.post("/pairs/{symbol}/disable", dependencies=[Depends(require_admin)])
def disable_admin_signal_pair(symbol: str, db: DB = Depends(_get_db)) -> dict[str, Any]:
    pair = _get_pair_or_404(symbol, db)
    disable_signal_pair(pair["symbol"], db=db)
    return _get_pair_or_404(pair["symbol"], db)


@router.post("/pairs/{symbol}/blacklist", dependencies=[Depends(require_admin)])
def blacklist_admin_signal_pair(
    symbol: str,
    payload: BlacklistRequest | None = None,
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    pair = _get_pair_or_404(symbol, db)
    reason = (payload.reason if payload else None) or "Admin blacklisted"
    blacklist_signal_pair(pair["symbol"], reason=reason, db=db)
    return _get_pair_or_404(pair["symbol"], db)


@router.post("/pairs/{symbol}/whitelist", dependencies=[Depends(require_admin)])
def whitelist_admin_signal_pair(symbol: str, db: DB = Depends(_get_db)) -> dict[str, Any]:
    pair = _get_pair_or_404(symbol, db)
    whitelist_signal_pair(pair["symbol"], db=db)
    updated = _get_pair_or_404(pair["symbol"], db)
    if not int(updated.get("blacklisted") or 0):
        update_signal_pair(updated["symbol"], {"enabled": 1}, db=db)
        updated = _get_pair_or_404(pair["symbol"], db)
    else:
        updated["warning"] = "BLACKLIST_OVERRIDES_WHITELIST"
    return updated


@router.post("/pairs/refresh", dependencies=[Depends(require_admin)])
def refresh_admin_signal_pairs(
    payload: RefreshPairsRequest | None = None,
    db: DB = Depends(_get_db),
    runner: DiscoveryRunner = Depends(_get_discovery_runner),
) -> dict[str, Any]:
    ensure_signals_schema(db)
    return runner(payload or RefreshPairsRequest(), db)


@router.get("/scan-runs", dependencies=[Depends(require_admin)])
def list_admin_signal_scan_runs(
    scan_type: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    ensure_signals_schema(db)
    normalized_limit = _normalize_limit(limit, default=50)
    normalized_offset = _normalize_offset(offset)
    items = list_signal_scan_runs(scan_type=scan_type, status=status, limit=normalized_limit, offset=normalized_offset, db=db)
    with db.connect() as conn:
        where: list[str] = []
        params: list[Any] = []
        if scan_type:
            where.append("scan_type=?")
            params.append(scan_type)
        if status:
            where.append("status=?")
            params.append(status)
        where_sql = " AND ".join(where) if where else "1=1"
        row = conn.execute(f"SELECT COUNT(*) AS count FROM signal_scan_runs WHERE {where_sql}", params).fetchone()
    return _response(items, count=int(row["count"] if row else 0), limit=normalized_limit, offset=normalized_offset)


@router.get("/scan-runs/{scan_run_id}", dependencies=[Depends(require_admin)])
def get_admin_signal_scan_run_detail(
    scan_run_id: str,
    db: DB = Depends(_get_db),
) -> dict[str, Any]:
    ensure_signals_schema(db)
    scan_run = get_signal_scan_run(scan_run_id, db=db)
    if not scan_run:
        raise HTTPException(status_code=404, detail="SCAN_RUN_NOT_FOUND")
    results = list_signal_scan_results(scan_run_id=scan_run_id, limit=1000, offset=0, db=db)
    return {"scan_run": scan_run, "results": results}
