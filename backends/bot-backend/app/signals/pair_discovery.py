from __future__ import annotations

from typing import Any, Protocol

from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient
from shared_lib.persistence.db import DB
from shared_lib.persistence.signals import (
    complete_signal_scan_run,
    create_signal_scan_result,
    create_signal_scan_run,
    fail_signal_scan_run,
    get_signal_pair,
    upsert_signal_pair,
    upsert_signal_pair_metrics,
)


EXCHANGE_BINANCE_FUTURES = "binance_futures"
SCAN_TYPE_PAIR_DISCOVERY = "PAIR_DISCOVERY"

DEFAULT_MIN_QUOTE_VOLUME_24H = 50_000_000.0
DEFAULT_MAX_SPREAD_PERCENT = 0.20
MAX_ATR_PERCENT = 0.10
MAX_ABNORMAL_CANDLE_RANGE_PERCENT = 0.25
MAX_ZERO_VOLUME_RATIO = 0.10
MAX_WICK_TO_BODY_RATIO = 20.0

TIER_1 = "TIER_1"
TIER_2 = "TIER_2"
TIER_3 = "TIER_3"
DISCOVERED = "DISCOVERED"

TIER_1_CRYPTO_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
)

TIER_2_CRYPTO_SYMBOLS = (
    "ADAUSDT",
    "DOGEUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "LTCUSDT",
    "DOTUSDT",
    "NEARUSDT",
    "ATOMUSDT",
    "AAVEUSDT",
    "APTUSDT",
    "SUIUSDT",
    "INJUSDT",
    "OPUSDT",
    "ARBUSDT",
    "MATICUSDT",
    "FILUSDT",
    "UNIUSDT",
    "ETCUSDT",
    "BCHUSDT",
    "TRXUSDT",
    "XLMUSDT",
    "HBARUSDT",
    "ICPUSDT",
    "RNDRUSDT",
    "TIAUSDT",
    "SEIUSDT",
    "WIFUSDT",
    "ORDIUSDT",
    "FETUSDT",
    "GRTUSDT",
)

DEFAULT_SEED_CRYPTO_SYMBOLS = TIER_1_CRYPTO_SYMBOLS + (
    "ADAUSDT",
    "DOGEUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "LTCUSDT",
)


class PairMarketClient(Protocol):
    def exchange_info(self) -> dict[str, Any]:
        ...

    def klines(self, symbol: str, interval: str = "1h", limit: int = 200) -> list[Any]:
        ...


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _rows_by_symbol(rows: Any) -> dict[str, dict[str, Any]]:
    if isinstance(rows, dict):
        if "symbol" in rows:
            return {str(rows["symbol"]).upper(): rows}
        return {str(key).upper(): value for key, value in rows.items() if isinstance(value, dict)}
    if isinstance(rows, list):
        return {str(item.get("symbol") or "").upper(): item for item in rows if isinstance(item, dict) and item.get("symbol")}
    return {}


def _spread_percent(book: dict[str, Any] | None) -> tuple[float | None, float | None, float | None]:
    if not book:
        return None, None, None
    bid = _float_or_none(book.get("bidPrice"))
    ask = _float_or_none(book.get("askPrice"))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None, bid, ask
    midpoint = (bid + ask) / 2.0
    if midpoint <= 0:
        return None, bid, ask
    return ((ask - bid) / midpoint) * 100.0, bid, ask


def liquidity_score(quote_volume_24h: float | None) -> float | None:
    if quote_volume_24h is None:
        return None
    if quote_volume_24h >= 500_000_000:
        return 100.0
    if quote_volume_24h >= 100_000_000:
        return 80.0
    if quote_volume_24h >= 50_000_000:
        return 65.0
    if quote_volume_24h >= 20_000_000:
        return 50.0
    return 0.0


def spread_score(spread_percent: float | None) -> float | None:
    if spread_percent is None:
        return None
    if spread_percent <= 0.03:
        return 100.0
    if spread_percent <= 0.05:
        return 90.0
    if spread_percent <= 0.10:
        return 75.0
    if spread_percent <= 0.20:
        return 60.0
    return 0.0


def reliability_score(
    liquidity: float | None,
    spread: float | None,
    *,
    candle_count: int | None = None,
    volatility: float | None = None,
    min_candles: int = 200,
    validate_candles: bool = False,
) -> float | None:
    if liquidity is None or spread is None:
        return None
    components = [liquidity, spread]
    if validate_candles:
        components.append(100.0 if (candle_count or 0) >= min_candles else 0.0)
        if volatility is not None:
            components.append(volatility)
    return round(sum(components) / len(components), 2)


def _normalize_candle(row: Any) -> dict[str, float] | None:
    try:
        if isinstance(row, dict):
            return {
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
            }
        return {
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]) if len(row) > 5 else 0.0,
        }
    except Exception:
        return None


def candle_safety_metrics(candles: list[Any]) -> dict[str, Any]:
    normalized = [item for item in (_normalize_candle(row) for row in candles or []) if item]
    if not normalized:
        return {
            "candle_count": 0,
            "atr_percent": None,
            "volatility_score": None,
            "reliable": False,
            "unsafe_reason": "INSUFFICIENT_HISTORY",
        }
    zero_volume_count = sum(1 for candle in normalized if candle["volume"] <= 0)
    ranges = []
    true_ranges = []
    wick_spike_count = 0
    for index, candle in enumerate(normalized):
        close = candle["close"]
        if close <= 0 or candle["high"] < candle["low"] or candle["high"] <= 0 or candle["low"] <= 0:
            return {
                "candle_count": len(normalized),
                "atr_percent": None,
                "volatility_score": None,
                "reliable": False,
                "unsafe_reason": "UNRELIABLE_CANDLES",
            }
        candle_range = candle["high"] - candle["low"]
        ranges.append(candle_range / close)
        prev_close = normalized[index - 1]["close"] if index else candle["open"]
        true_ranges.append(max(candle["high"] - candle["low"], abs(candle["high"] - prev_close), abs(candle["low"] - prev_close)))
        body = abs(candle["close"] - candle["open"])
        if body > 0 and candle_range / body > MAX_WICK_TO_BODY_RATIO:
            wick_spike_count += 1
    zero_volume_ratio = zero_volume_count / len(normalized)
    if zero_volume_ratio > MAX_ZERO_VOLUME_RATIO or wick_spike_count > max(2, int(len(normalized) * 0.05)):
        return {
            "candle_count": len(normalized),
            "atr_percent": None,
            "volatility_score": 0.0,
            "reliable": False,
            "unsafe_reason": "UNRELIABLE_CANDLES",
        }
    latest_close = normalized[-1]["close"]
    atr = sum(true_ranges[-14:]) / min(14, len(true_ranges))
    atr_percent = atr / latest_close if latest_close > 0 else None
    if atr_percent is None or atr_percent > MAX_ATR_PERCENT or max(ranges) > MAX_ABNORMAL_CANDLE_RANGE_PERCENT:
        return {
            "candle_count": len(normalized),
            "atr_percent": atr_percent,
            "volatility_score": 0.0,
            "reliable": False,
            "unsafe_reason": "EXTREME_VOLATILITY",
        }
    volatility = max(0.0, round(100.0 * (1.0 - (atr_percent / MAX_ATR_PERCENT)), 2))
    return {
        "candle_count": len(normalized),
        "atr_percent": atr_percent,
        "volatility_score": volatility,
        "reliable": True,
        "unsafe_reason": None,
    }


def seed_default_signal_pair_universe(
    *,
    db: DB | None = None,
    exchange: str = EXCHANGE_BINANCE_FUTURES,
    asset_class: str = "crypto",
) -> dict[str, Any]:
    db = db or DB()
    seeded = 0
    for tier, symbols in ((TIER_1, TIER_1_CRYPTO_SYMBOLS), (TIER_2, TIER_2_CRYPTO_SYMBOLS)):
        for symbol in symbols:
            existing = get_signal_pair(symbol, db=db)
            payload = {
                "symbol": symbol,
                "exchange": exchange,
                "asset_class": asset_class,
                "quote_asset": "USDT",
                "contract_type": "PERPETUAL",
                "tier": tier,
                "enabled": 0 if existing and int(existing.get("blacklisted") or 0) else 1,
                "whitelisted": 1,
            }
            if existing and int(existing.get("blacklisted") or 0):
                payload["blacklisted"] = 1
                payload["blacklist_reason"] = existing.get("blacklist_reason")
            upsert_signal_pair(payload, db=db)
            seeded += 1
    return {
        "exchange": exchange,
        "tier_1_seeded": len(TIER_1_CRYPTO_SYMBOLS),
        "tier_2_seeded": len(TIER_2_CRYPTO_SYMBOLS),
        "total_seeded": seeded,
    }


class PairDiscoveryService:
    """Read-only pair discovery for the manual Signal Center.

    This service records exchange symbols and safety metrics only. It does not
    generate signals, publish signals, place orders, or call execution paths.
    """

    def __init__(
        self,
        *,
        market_client: Any | None = None,
        db: DB | None = None,
        exchange: str = EXCHANGE_BINANCE_FUTURES,
    ):
        self.market_client = market_client or BinanceFuturesClient(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_API_SECRET,
            base_url=settings.BINANCE_FAPI_BASE_URL,
            recv_window=settings.BINANCE_RECV_WINDOW,
        )
        self.db = db or DB()
        self.exchange = exchange

    def discover_binance_futures_pairs(
        self,
        *,
        min_quote_volume_24h: float = DEFAULT_MIN_QUOTE_VOLUME_24H,
        max_spread_percent: float = DEFAULT_MAX_SPREAD_PERCENT,
        quote_asset: str = "USDT",
        contract_type: str = "PERPETUAL",
        validate_candles: bool = True,
        candle_timeframe: str = "1h",
        min_candles: int = 200,
    ) -> dict[str, Any]:
        scan_run_id = create_signal_scan_run(SCAN_TYPE_PAIR_DISCOVERY, db=self.db)
        summary = {
            "scan_run_id": scan_run_id,
            "exchange": self.exchange,
            "symbols_discovered": 0,
            "symbols_eligible": 0,
            "symbols_skipped": 0,
            "metrics_updated": 0,
            "errors": [],
        }
        try:
            exchange_info = self._exchange_info()
            tickers = _rows_by_symbol(self._ticker_24h())
            book_tickers = _rows_by_symbol(self._book_tickers())
            symbols = exchange_info.get("symbols", []) or []
            summary["symbols_discovered"] = len(symbols)

            for symbol_info in symbols:
                symbol = str(symbol_info.get("symbol") or "").upper()
                if not symbol:
                    continue
                existing_pair = get_signal_pair(symbol, db=self.db)
                reason = "BLACKLISTED_SYMBOL" if existing_pair and int(existing_pair.get("blacklisted") or 0) else None
                if reason is None:
                    reason = self._structural_skip_reason(symbol_info, quote_asset, contract_type)
                upsert_signal_pair(
                    {
                        "symbol": symbol,
                        "exchange": self.exchange,
                        "asset_class": "crypto",
                        "quote_asset": symbol_info.get("quoteAsset"),
                        "contract_type": symbol_info.get("contractType"),
                        "tier": existing_pair.get("tier") if existing_pair and existing_pair.get("tier") else DISCOVERED,
                        "enabled": 0 if reason else 1,
                        "blacklisted": existing_pair.get("blacklisted") if existing_pair else 0,
                        "blacklist_reason": existing_pair.get("blacklist_reason") if existing_pair else None,
                    },
                    db=self.db,
                )
                if reason:
                    self._mark_unsafe_without_market_data(
                        symbol=symbol,
                        reason=reason,
                        ticker=tickers.get(symbol),
                        book=book_tickers.get(symbol),
                    )
                    summary["symbols_skipped"] += 1
                    summary["metrics_updated"] += 1
                    create_signal_scan_result(
                        {"scan_run_id": scan_run_id, "symbol": symbol, "was_skipped": 1, "skip_reason": reason},
                        db=self.db,
                    )
                    continue

                eligible, skip_reason = self._evaluate_market_quality(
                    symbol=symbol,
                    ticker=tickers.get(symbol),
                    book=book_tickers.get(symbol),
                    min_quote_volume_24h=min_quote_volume_24h,
                    max_spread_percent=max_spread_percent,
                    validate_candles=validate_candles,
                    candle_timeframe=candle_timeframe,
                    min_candles=min_candles,
                )
                if eligible:
                    summary["symbols_eligible"] += 1
                    create_signal_scan_result(
                        {"scan_run_id": scan_run_id, "symbol": symbol, "was_scanned": 1},
                        db=self.db,
                    )
                else:
                    summary["symbols_skipped"] += 1
                    if skip_reason == "API_ERROR":
                        summary["errors"].append({"symbol": symbol, "error": "API_ERROR"})
                    create_signal_scan_result(
                        {"scan_run_id": scan_run_id, "symbol": symbol, "was_skipped": 1, "skip_reason": skip_reason},
                        db=self.db,
                    )
                summary["metrics_updated"] += 1

            complete_signal_scan_run(
                scan_run_id,
                {
                    "status": "PARTIAL" if summary["errors"] else "COMPLETED",
                    "symbols_discovered": summary["symbols_discovered"],
                    "symbols_eligible": summary["symbols_eligible"],
                    "symbols_scanned": summary["symbols_eligible"],
                    "errors": str(summary["errors"]) if summary["errors"] else None,
                },
                db=self.db,
            )
            return summary
        except Exception as exc:
            summary["errors"].append({"error": str(exc)})
            fail_signal_scan_run(scan_run_id, str(exc), db=self.db)
            return summary

    def _exchange_info(self) -> dict[str, Any]:
        if hasattr(self.market_client, "exchange_info_cached"):
            return self.market_client.exchange_info_cached()
        return self.market_client.exchange_info()

    def _ticker_24h(self) -> Any:
        for method_name in ("ticker_24h", "tickers_24h", "fetch_24h_tickers"):
            method = getattr(self.market_client, method_name, None)
            if callable(method):
                return method()
        if hasattr(self.market_client, "_request"):
            return self.market_client._request("GET", "/fapi/v1/ticker/24hr")
        raise RuntimeError("Market client does not expose 24h ticker data")

    def _book_tickers(self) -> Any:
        for method_name in ("book_tickers", "fetch_book_tickers"):
            method = getattr(self.market_client, method_name, None)
            if callable(method):
                return method()
        if hasattr(self.market_client, "_request"):
            return self.market_client._request("GET", "/fapi/v1/ticker/bookTicker")
        raise RuntimeError("Market client does not expose book ticker data")

    def _structural_skip_reason(self, symbol_info: dict[str, Any], quote_asset: str, contract_type: str) -> str | None:
        if symbol_info.get("status") != "TRADING":
            return "SYMBOL_NOT_TRADING"
        if symbol_info.get("quoteAsset") != quote_asset:
            return "UNSUPPORTED_QUOTE_ASSET"
        if symbol_info.get("contractType") != contract_type:
            return "UNSUPPORTED_CONTRACT_TYPE"
        return None

    def _evaluate_market_quality(
        self,
        *,
        symbol: str,
        ticker: dict[str, Any] | None,
        book: dict[str, Any] | None,
        min_quote_volume_24h: float,
        max_spread_percent: float,
        validate_candles: bool,
        candle_timeframe: str,
        min_candles: int,
    ) -> tuple[bool, str | None]:
        quote_volume = _float_or_none((ticker or {}).get("quoteVolume"))
        spread_pct, bid, ask = _spread_percent(book)
        candle_count: int | None = None
        reason: str | None = None

        if quote_volume is None:
            reason = "MISSING_TICKER"
        elif quote_volume < min_quote_volume_24h:
            reason = "LOW_VOLUME"
        elif spread_pct is None or bid is None or ask is None:
            reason = "MISSING_BID_ASK"
        elif spread_pct > max_spread_percent:
            reason = "SPREAD_TOO_WIDE"

        candle_metrics: dict[str, Any] | None = None
        if reason is None and validate_candles:
            try:
                candles = self.market_client.klines(symbol=symbol, interval=candle_timeframe, limit=min_candles)
                candle_count = len(candles)
            except Exception as exc:
                reason = "API_ERROR"
                upsert_signal_pair_metrics(
                    {
                        "symbol": symbol,
                        "exchange": self.exchange,
                        "quote_volume_24h": quote_volume,
                        "spread_percent": spread_pct,
                        "bid_price": bid,
                        "ask_price": ask,
                        "is_safe": 0,
                        "unsafe_reason": "API_ERROR",
                    },
                    db=self.db,
                )
                return False, reason
            if candle_count < min_candles:
                reason = "INSUFFICIENT_HISTORY"
            else:
                candle_metrics = candle_safety_metrics(candles)
                if not candle_metrics["reliable"]:
                    reason = candle_metrics["unsafe_reason"]

        liq_score = liquidity_score(quote_volume)
        spr_score = spread_score(spread_pct)
        volatility = candle_metrics.get("volatility_score") if candle_metrics else None
        rel_score = reliability_score(
            liq_score,
            spr_score,
            candle_count=candle_count,
            volatility=volatility,
            min_candles=min_candles,
            validate_candles=validate_candles,
        )
        upsert_signal_pair_metrics(
            {
                "symbol": symbol,
                "exchange": self.exchange,
                "quote_volume_24h": quote_volume,
                "spread_percent": spread_pct,
                "bid_price": bid,
                "ask_price": ask,
                "candle_count": candle_count,
                "atr_percent": candle_metrics.get("atr_percent") if candle_metrics else None,
                "volatility_score": volatility,
                "liquidity_score": liq_score,
                "spread_score": spr_score,
                "reliability_score": rel_score,
                "is_safe": 1 if reason is None else 0,
                "unsafe_reason": reason,
            },
            db=self.db,
        )
        return reason is None, reason

    def _mark_unsafe_without_market_data(
        self,
        *,
        symbol: str,
        reason: str,
        ticker: dict[str, Any] | None,
        book: dict[str, Any] | None,
    ) -> None:
        quote_volume = _float_or_none((ticker or {}).get("quoteVolume"))
        spread_pct, bid, ask = _spread_percent(book)
        upsert_signal_pair_metrics(
            {
                "symbol": symbol,
                "exchange": self.exchange,
                "quote_volume_24h": quote_volume,
                "spread_percent": spread_pct,
                "bid_price": bid,
                "ask_price": ask,
                "liquidity_score": liquidity_score(quote_volume),
                "spread_score": spread_score(spread_pct),
                "reliability_score": None,
                "is_safe": 0,
                "unsafe_reason": reason,
            },
            db=self.db,
        )


def discover_pairs(**kwargs: Any) -> dict[str, Any]:
    return PairDiscoveryService(
        market_client=kwargs.pop("market_client", None),
        db=kwargs.pop("db", None),
    ).discover_binance_futures_pairs(**kwargs)
