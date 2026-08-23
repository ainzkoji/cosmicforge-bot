from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


DEFAULT_BASE_URL = "https://fapi.binance.com"
DEFAULT_OUTPUT = Path("reports/symbol_discovery/binance_futures_usdt_perpetuals.json")
DEFAULT_MIN_QUOTE_VOLUME_USDT = 50_000_000.0
DEFAULT_MAX_SPREAD_BPS = 10.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_exchange_info(base_url: str = DEFAULT_BASE_URL, timeout: int = 20) -> dict[str, Any]:
    """Fetch Binance USD-M Futures exchangeInfo using the public read-only API."""
    url = f"{base_url.rstrip('/')}/fapi/v1/exchangeInfo"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or not isinstance(data.get("symbols"), list):
        raise ValueError("Unexpected exchangeInfo response: missing symbols list")
    return data


def fetch_24h_tickers(base_url: str = DEFAULT_BASE_URL, timeout: int = 20) -> dict[str, Any]:
    """Fetch all Binance USD-M Futures 24h ticker rows keyed by symbol."""
    url = f"{base_url.rstrip('/')}/fapi/v1/ticker/24hr"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise ValueError("Unexpected 24h ticker response: expected list")
    return {str(item.get("symbol") or ""): item for item in data if item.get("symbol")}


def fetch_book_tickers(base_url: str = DEFAULT_BASE_URL, timeout: int = 20) -> dict[str, Any]:
    """Fetch all Binance USD-M Futures best bid/ask rows keyed by symbol."""
    url = f"{base_url.rstrip('/')}/fapi/v1/ticker/bookTicker"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise ValueError("Unexpected bookTicker response: expected list")
    return {str(item.get("symbol") or ""): item for item in data if item.get("symbol")}


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _quote_volume_usdt(ticker: dict[str, Any] | None) -> float | None:
    if not ticker:
        return None
    return _float_or_none(ticker.get("quoteVolume"))


def _spread_bps(book: dict[str, Any] | None) -> float | None:
    if not book:
        return None
    bid = _float_or_none(book.get("bidPrice"))
    ask = _float_or_none(book.get("askPrice"))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return ((ask - bid) / mid) * 10_000.0


def _filter_map(symbol_info: dict[str, Any]) -> dict[str, dict[str, Any]]:
    filters: dict[str, dict[str, Any]] = {}
    for item in symbol_info.get("filters", []) or []:
        filter_type = item.get("filterType")
        if filter_type:
            filters[str(filter_type)] = item
    return filters


def _min_notional(filters: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    raw = filters.get("MIN_NOTIONAL")
    if not raw:
        return None
    return {
        "notional": raw.get("notional") or raw.get("minNotional"),
        "applyMinToMarket": raw.get("applyMinToMarket"),
    }


def eligibility_reasons(symbol_info: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    symbol = str(symbol_info.get("symbol") or "")

    if symbol_info.get("status") != "TRADING":
        reasons.append("status_not_trading")
    if symbol_info.get("quoteAsset") != "USDT":
        reasons.append("quote_asset_not_usdt")
    if symbol_info.get("contractType") != "PERPETUAL":
        reasons.append("contract_type_not_perpetual")
    if not symbol.endswith("USDT"):
        reasons.append("symbol_not_usdt_suffix")

    return reasons


def market_quality_reasons(
    symbol: str,
    ticker_24h: dict[str, Any] | None,
    book_ticker: dict[str, Any] | None,
    min_quote_volume_usdt: float,
    max_spread_bps: float,
) -> tuple[list[str], float | None, float | None]:
    reasons: list[str] = []
    quote_volume = _quote_volume_usdt(ticker_24h)
    spread_bps = _spread_bps(book_ticker)

    if quote_volume is None:
        reasons.append("missing_24h_quote_volume")
    elif quote_volume < min_quote_volume_usdt:
        reasons.append("low_24h_quote_volume")

    if spread_bps is None:
        reasons.append("missing_or_invalid_book_spread")
    elif spread_bps > max_spread_bps:
        reasons.append("wide_book_spread")

    return reasons, quote_volume, spread_bps


def _candidate_view(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "rank": item["rank"],
        "symbol": item["symbol"],
        "baseAsset": item["baseAsset"],
        "quoteVolume24h": item["quoteVolume24h"],
        "spreadBps": item["spreadBps"],
        "bidPrice": item["bidPrice"],
        "askPrice": item["askPrice"],
        "pricePrecision": item["pricePrecision"],
        "quantityPrecision": item["quantityPrecision"],
        "MIN_NOTIONAL": item["MIN_NOTIONAL"],
    }


def summarize_exchange_info(
    exchange_info: dict[str, Any],
    tickers_24h: dict[str, Any],
    book_tickers: dict[str, Any],
    min_quote_volume_usdt: float,
    max_spread_bps: float,
) -> dict[str, Any]:
    symbols = exchange_info.get("symbols", []) or []
    eligible: list[dict[str, Any]] = []
    ranked_candidates: list[dict[str, Any]] = []
    structural_excluded_reasons: Counter[str] = Counter()
    market_quality_excluded_reasons: Counter[str] = Counter()

    for symbol_info in symbols:
        reasons = eligibility_reasons(symbol_info)
        if reasons:
            structural_excluded_reasons.update(reasons)
            continue

        symbol = str(symbol_info.get("symbol") or "")
        filters = _filter_map(symbol_info)
        ticker_24h = tickers_24h.get(symbol)
        book_ticker = book_tickers.get(symbol)
        quality_reasons, quote_volume, spread_bps = market_quality_reasons(
            symbol,
            ticker_24h,
            book_ticker,
            min_quote_volume_usdt,
            max_spread_bps,
        )

        item = {
            "symbol": symbol,
            "baseAsset": symbol_info.get("baseAsset"),
            "quoteAsset": symbol_info.get("quoteAsset"),
            "contractType": symbol_info.get("contractType"),
            "status": symbol_info.get("status"),
            "pricePrecision": symbol_info.get("pricePrecision"),
            "quantityPrecision": symbol_info.get("quantityPrecision"),
            "LOT_SIZE": filters.get("LOT_SIZE"),
            "PRICE_FILTER": filters.get("PRICE_FILTER"),
            "MIN_NOTIONAL": _min_notional(filters),
            "quoteVolume24h": quote_volume,
            "spreadBps": spread_bps,
            "bidPrice": book_ticker.get("bidPrice") if book_ticker else None,
            "askPrice": book_ticker.get("askPrice") if book_ticker else None,
            "market_quality_exclusion_reasons": quality_reasons,
        }
        eligible.append(item)

        if quality_reasons:
            market_quality_excluded_reasons.update(quality_reasons)
        else:
            ranked_candidates.append(item)

    eligible.sort(key=lambda item: str(item.get("symbol") or ""))
    ranked_candidates.sort(
        key=lambda item: (
            -float(item["quoteVolume24h"] or 0.0),
            float(item["spreadBps"] or 999_999.0),
            str(item["symbol"]),
        )
    )
    for idx, item in enumerate(ranked_candidates, start=1):
        item["rank"] = idx

    return {
        "generated_at": _utc_now_iso(),
        "source": "binance_usdm_futures_public_market_data",
        "endpoints": [
            "/fapi/v1/exchangeInfo",
            "/fapi/v1/ticker/24hr",
            "/fapi/v1/ticker/bookTicker",
        ],
        "criteria": {
            "status": "TRADING",
            "quoteAsset": "USDT",
            "contractType": "PERPETUAL",
            "symbol_suffix": "USDT",
            "min_quote_volume_usdt": min_quote_volume_usdt,
            "max_spread_bps": max_spread_bps,
        },
        "total_exchange_symbols": len(symbols),
        "total_eligible_usdt_perpetual_symbols": len(eligible),
        "total_ranked_candidates_after_volume_spread_filters": len(ranked_candidates),
        "excluded_symbol_count_by_reason": {
            **dict(sorted(structural_excluded_reasons.items())),
            **dict(sorted(market_quality_excluded_reasons.items())),
        },
        "structural_excluded_symbol_count_by_reason": dict(sorted(structural_excluded_reasons.items())),
        "market_quality_excluded_symbol_count_by_reason": dict(sorted(market_quality_excluded_reasons.items())),
        "eligible_symbols": eligible,
        "eligible_symbol_names": [str(item["symbol"]) for item in eligible],
        "ranked_candidates": [_candidate_view(item) for item in ranked_candidates],
        "top_30_candidates": [_candidate_view(item) for item in ranked_candidates[:30]],
        "top_50_candidates": [_candidate_view(item) for item in ranked_candidates[:50]],
        "top_100_candidates": [_candidate_view(item) for item in ranked_candidates[:100]],
    }


def write_report(report: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def print_summary(report: dict[str, Any], output_path: Path) -> None:
    names = report["eligible_symbol_names"]
    excluded = report["excluded_symbol_count_by_reason"]
    top_30 = report["top_30_candidates"]

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("Binance Futures USDT Perpetual Discovery")
    print("----------------------------------------")
    print(f"Endpoints: {', '.join(report['endpoints'])}")
    print(f"Generated at: {report['generated_at']}")
    print(f"Total exchange symbols: {report['total_exchange_symbols']}")
    print(f"Eligible USDT perpetual symbols: {report['total_eligible_usdt_perpetual_symbols']}")
    print(
        "Ranked candidates after volume/spread filters: "
        f"{report['total_ranked_candidates_after_volume_spread_filters']}"
    )
    print(f"Minimum 24h quote volume: {report['criteria']['min_quote_volume_usdt']:.2f} USDT")
    print(f"Maximum spread: {report['criteria']['max_spread_bps']:.2f} bps")
    print(f"Report: {output_path}")
    print("")
    print("Excluded count by reason:")
    if excluded:
        for reason, count in excluded.items():
            print(f"  - {reason}: {count}")
    else:
        print("  - none")
    print("")
    print("Top 30 candidates:")
    for item in top_30:
        print(
            f"  {item['rank']:>3}. {item['symbol']:<18} "
            f"quoteVol={float(item['quoteVolume24h'] or 0):>15,.2f} "
            f"spreadBps={float(item['spreadBps'] or 0):>7.3f}"
        )
    print("")
    print("Eligible structural symbol names:")
    print(", ".join(names))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only Binance USD-M Futures symbol discovery report."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--min-quote-volume-usdt", type=float, default=DEFAULT_MIN_QUOTE_VOLUME_USDT)
    parser.add_argument("--max-spread-bps", type=float, default=DEFAULT_MAX_SPREAD_BPS)
    args = parser.parse_args()

    exchange_info = fetch_exchange_info(args.base_url, args.timeout)
    tickers_24h = fetch_24h_tickers(args.base_url, args.timeout)
    book_tickers = fetch_book_tickers(args.base_url, args.timeout)
    report = summarize_exchange_info(
        exchange_info,
        tickers_24h,
        book_tickers,
        args.min_quote_volume_usdt,
        args.max_spread_bps,
    )
    output_path = write_report(report, args.output)
    print_summary(report, output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
