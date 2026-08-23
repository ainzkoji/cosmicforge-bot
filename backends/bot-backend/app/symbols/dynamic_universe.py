from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import requests

from app.core.config import settings
from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)

_KLINE_AVAILABILITY_CACHE: dict[tuple[str, str, str, int], tuple[bool, str | None, float]] = {}


@dataclass(frozen=True)
class DynamicSymbolCandidate:
    symbol: str
    rank: int | None
    base_asset: str | None
    quote_asset: str | None
    contract_type: str | None
    status: str | None
    quote_volume_24h: float | None
    spread_bps: float | None
    exclusion_reasons: list[str]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _public_get(path: str, timeout: int = 10) -> Any:
    configured = str(getattr(settings, "DYNAMIC_UNIVERSE_BASE_URL", "") or "https://fapi.binance.com")
    binance_env = str(getattr(settings, "BINANCE_ENV", "") or "").lower()
    bot_base = str(getattr(settings, "BINANCE_FAPI_BASE_URL", "") or "").strip()
    if binance_env in {"testnet", "demo"} and bot_base:
        # Shadow diagnostics must rank the same exchange environment used for
        # strategy klines. Mainnet exchangeInfo can list symbols unavailable on
        # demo-fapi, causing repeated 400s during shadow evaluation.
        base_url = bot_base
    else:
        base_url = configured
    url = f"{base_url.rstrip('/')}{path}"
    session = requests.Session()
    session.trust_env = False
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _dynamic_base_url() -> str:
    configured = str(getattr(settings, "DYNAMIC_UNIVERSE_BASE_URL", "") or "https://fapi.binance.com")
    binance_env = str(getattr(settings, "BINANCE_ENV", "") or "").lower()
    bot_base = str(getattr(settings, "BINANCE_FAPI_BASE_URL", "") or "").strip()
    if binance_env in {"testnet", "demo"} and bot_base:
        return bot_base.rstrip("/")
    return configured.rstrip("/")


def _kline_availability_reason(
    symbol: str,
    *,
    interval: str,
    limit: int,
    timeout: int,
    ttl_seconds: int,
) -> str | None:
    """Return None when klines are readable, otherwise a stable exclusion reason."""
    base_url = _dynamic_base_url()
    key = (base_url, symbol.upper(), interval, int(limit))
    now = time.monotonic()
    cached = _KLINE_AVAILABILITY_CACHE.get(key)
    if cached and now < cached[2]:
        available, reason, _expires = cached
        return None if available else reason

    params = urlencode({"symbol": symbol.upper(), "interval": interval, "limit": int(limit)})
    try:
        data = _public_get(f"/fapi/v1/klines?{params}", timeout)
        available = isinstance(data, list) and len(data) >= max(1, int(limit))
        reason = None if available else "klines_unavailable"
    except Exception as exc:
        available = False
        reason = "klines_unavailable"
        logger.info("[DYNAMIC_UNIVERSE] %s excluded: klines unavailable (%s)", symbol, exc)

    _KLINE_AVAILABILITY_CACHE[key] = (available, reason, now + max(0, int(ttl_seconds)))
    return reason


def _structural_reasons(symbol_info: dict[str, Any]) -> list[str]:
    symbol = str(symbol_info.get("symbol") or "")
    reasons: list[str] = []
    if symbol_info.get("status") != "TRADING":
        reasons.append("status_not_trading")
    if symbol_info.get("quoteAsset") != "USDT":
        reasons.append("quote_asset_not_usdt")
    if symbol_info.get("contractType") != "PERPETUAL":
        reasons.append("contract_type_not_perpetual")
    if not symbol.endswith("USDT"):
        reasons.append("symbol_not_usdt_suffix")
    return reasons


def _quote_volume(ticker: dict[str, Any] | None) -> float | None:
    return _float_or_none((ticker or {}).get("quoteVolume"))


def _spread_bps(book: dict[str, Any] | None) -> float | None:
    bid = _float_or_none((book or {}).get("bidPrice"))
    ask = _float_or_none((book or {}).get("askPrice"))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return ((ask - bid) / mid) * 10_000.0


def _quality_reasons(
    quote_volume_24h: float | None,
    spread_bps: float | None,
    min_quote_volume_usdt: float,
    max_spread_bps: float,
) -> list[str]:
    reasons: list[str] = []
    if quote_volume_24h is None:
        reasons.append("missing_24h_quote_volume")
    elif quote_volume_24h < min_quote_volume_usdt:
        reasons.append("low_24h_quote_volume")

    if spread_bps is None:
        reasons.append("missing_or_invalid_book_spread")
    elif spread_bps > max_spread_bps:
        reasons.append("wide_book_spread")
    return reasons


class DynamicUniverseService:
    """Read-only Binance Futures universe discovery and ranking."""

    def __init__(
        self,
        *,
        min_quote_volume_usdt: float | None = None,
        max_spread_bps: float | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        self.min_quote_volume_usdt = float(
            min_quote_volume_usdt
            if min_quote_volume_usdt is not None
            else getattr(settings, "DYNAMIC_UNIVERSE_MIN_QUOTE_VOLUME_USDT", 50_000_000.0)
        )
        self.max_spread_bps = float(
            max_spread_bps
            if max_spread_bps is not None
            else getattr(settings, "DYNAMIC_UNIVERSE_MAX_SPREAD_BPS", 10.0)
        )
        self.timeout_seconds = int(
            timeout_seconds
            if timeout_seconds is not None
            else getattr(settings, "DYNAMIC_UNIVERSE_HTTP_TIMEOUT_SECONDS", 10)
        )
        self.require_klines = bool(getattr(settings, "DYNAMIC_UNIVERSE_REQUIRE_KLINES", True))
        self.kline_interval = str(getattr(settings, "DYNAMIC_UNIVERSE_KLINE_INTERVAL", "1m") or "1m")
        self.kline_limit = int(getattr(settings, "DYNAMIC_UNIVERSE_KLINE_LIMIT", 2) or 2)
        self.kline_cache_seconds = int(getattr(settings, "DYNAMIC_UNIVERSE_KLINE_CACHE_SECONDS", 3600) or 3600)

    def discover(self) -> dict[str, Any]:
        exchange_info = _public_get("/fapi/v1/exchangeInfo", self.timeout_seconds)
        ticker_rows = _public_get("/fapi/v1/ticker/24hr", self.timeout_seconds)
        book_rows = _public_get("/fapi/v1/ticker/bookTicker", self.timeout_seconds)

        tickers = {str(row.get("symbol") or ""): row for row in ticker_rows if row.get("symbol")}
        books = {str(row.get("symbol") or ""): row for row in book_rows if row.get("symbol")}

        structural_candidates: list[DynamicSymbolCandidate] = []
        ranked_candidates: list[DynamicSymbolCandidate] = []
        excluded_counts: dict[str, int] = {}

        for info in exchange_info.get("symbols", []) or []:
            symbol = str(info.get("symbol") or "")
            reasons = _structural_reasons(info)
            if not reasons:
                quote_volume_24h = _quote_volume(tickers.get(symbol))
                spread = _spread_bps(books.get(symbol))
                reasons = _quality_reasons(
                    quote_volume_24h,
                    spread,
                    self.min_quote_volume_usdt,
                    self.max_spread_bps,
                )
                if not reasons and self.require_klines:
                    kline_reason = _kline_availability_reason(
                        symbol,
                        interval=self.kline_interval,
                        limit=self.kline_limit,
                        timeout=self.timeout_seconds,
                        ttl_seconds=self.kline_cache_seconds,
                    )
                    if kline_reason:
                        reasons.append(kline_reason)
                candidate = DynamicSymbolCandidate(
                    symbol=symbol,
                    rank=None,
                    base_asset=info.get("baseAsset"),
                    quote_asset=info.get("quoteAsset"),
                    contract_type=info.get("contractType"),
                    status=info.get("status"),
                    quote_volume_24h=quote_volume_24h,
                    spread_bps=spread,
                    exclusion_reasons=reasons,
                )
                structural_candidates.append(candidate)
                if not reasons:
                    ranked_candidates.append(candidate)

            for reason in reasons:
                excluded_counts[reason] = excluded_counts.get(reason, 0) + 1

        ranked_candidates.sort(
            key=lambda c: (
                -(c.quote_volume_24h or 0.0),
                c.spread_bps if c.spread_bps is not None else 999_999.0,
                c.symbol,
            )
        )
        ranked_candidates = [
            DynamicSymbolCandidate(**{**c.__dict__, "rank": idx})
            for idx, c in enumerate(ranked_candidates, start=1)
        ]

        return {
            "generated_at": _utc_now_iso(),
            "total_exchange_symbols": len(exchange_info.get("symbols", []) or []),
            "total_structural_usdt_perpetual_symbols": len(structural_candidates),
            "total_ranked_candidates": len(ranked_candidates),
            "excluded_symbol_count_by_reason": dict(sorted(excluded_counts.items())),
            "ranked_candidates": [c.__dict__ for c in ranked_candidates],
            "structural_candidates": [c.__dict__ for c in structural_candidates],
        }


class DynamicUniverseShadowRecorder:
    """Persistence for dynamic universe shadow diagnostics."""

    def __init__(self, db: DB | None = None) -> None:
        self.db = db or DB()
        self.ensure_schema()

    def ensure_schema(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dynamic_universe_shadow_diagnostics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    run_id TEXT,
                    cycle_id TEXT,
                    bot_instance_id TEXT,
                    symbol TEXT NOT NULL,
                    rank INTEGER,
                    in_live_config INTEGER NOT NULL DEFAULT 0,
                    was_evaluated INTEGER NOT NULL DEFAULT 0,
                    would_pass_strategy INTEGER NOT NULL DEFAULT 0,
                    signal TEXT,
                    confidence REAL,
                    threshold REAL,
                    reason TEXT,
                    quote_volume_24h REAL,
                    spread_bps REAL,
                    exclusion_reasons_json TEXT NOT NULL DEFAULT '[]',
                    diagnostics_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dusd_created "
                "ON dynamic_universe_shadow_diagnostics(created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dusd_cycle "
                "ON dynamic_universe_shadow_diagnostics(cycle_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dusd_symbol "
                "ON dynamic_universe_shadow_diagnostics(symbol)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dusd_pass "
                "ON dynamic_universe_shadow_diagnostics(would_pass_strategy)"
            )

    def record_many(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT INTO dynamic_universe_shadow_diagnostics (
                    created_at, run_id, cycle_id, bot_instance_id, symbol, rank,
                    in_live_config, was_evaluated, would_pass_strategy,
                    signal, confidence, threshold, reason,
                    quote_volume_24h, spread_bps, exclusion_reasons_json,
                    diagnostics_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.get("created_at") or _utc_now_iso(),
                        row.get("run_id"),
                        row.get("cycle_id"),
                        row.get("bot_instance_id"),
                        row["symbol"],
                        row.get("rank"),
                        1 if row.get("in_live_config") else 0,
                        1 if row.get("was_evaluated") else 0,
                        1 if row.get("would_pass_strategy") else 0,
                        row.get("signal"),
                        row.get("confidence"),
                        row.get("threshold"),
                        row.get("reason"),
                        row.get("quote_volume_24h"),
                        row.get("spread_bps"),
                        json.dumps(row.get("exclusion_reasons") or []),
                        json.dumps(row.get("diagnostics") or {}),
                    )
                    for row in rows
                ],
            )
