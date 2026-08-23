from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "bot-backend"))
sys.path.insert(0, str(ROOT / "backends" / "bot-backend" / "scripts"))

from app.signals.crypto_signal_engine import ALLOWED_CRYPTO_SYMBOLS, TIER_1_CRYPTO_SYMBOLS, TIER_2_CRYPTO_SYMBOLS  # noqa: E402
from app.signals.pair_discovery import PairDiscoveryService, seed_default_signal_pair_universe  # noqa: E402
import discover_signal_pairs as discover_script  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    blacklist_signal_pair,
    acquire_signal_operation_lock,
    complete_signal_scan_run,
    create_signal_scan_result,
    create_signal_scan_run,
    get_signal_pair,
    get_signal_pair_metrics,
    get_signal_scan_run,
    get_eligible_signal_symbols,
    list_signal_pairs,
    list_signal_scan_results,
    mark_pair_safe,
    mark_pair_unsafe,
    release_signal_operation_lock,
    set_signal_setting,
    upsert_signal_pair,
    upsert_signal_pair_metrics,
)


class FakePairMarketClient:
    def exchange_info(self):
        return {
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
                {"symbol": "LOWUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
                {"symbol": "WIDEUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
                {"symbol": "MISSUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
                {"symbol": "SHORTUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
                {"symbol": "VOLUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
                {"symbol": "BADUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
                {"symbol": "BLACKUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
                {"symbol": "OLDUSDT", "status": "BREAK", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
                {"symbol": "ETHBUSD", "status": "TRADING", "quoteAsset": "BUSD", "contractType": "PERPETUAL"},
                {"symbol": "BTCUSDT_240628", "status": "TRADING", "quoteAsset": "USDT", "contractType": "CURRENT_QUARTER"},
                {"symbol": "ERRUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"},
            ]
        }

    def ticker_24h(self):
        return [
            {"symbol": "BTCUSDT", "quoteVolume": "600000000"},
            {"symbol": "LOWUSDT", "quoteVolume": "1000000"},
            {"symbol": "WIDEUSDT", "quoteVolume": "70000000"},
            {"symbol": "MISSUSDT", "quoteVolume": "70000000"},
            {"symbol": "SHORTUSDT", "quoteVolume": "80000000"},
            {"symbol": "VOLUSDT", "quoteVolume": "80000000"},
            {"symbol": "BADUSDT", "quoteVolume": "80000000"},
            {"symbol": "BLACKUSDT", "quoteVolume": "80000000"},
            {"symbol": "ERRUSDT", "quoteVolume": "80000000"},
        ]

    def book_tickers(self):
        return [
            {"symbol": "BTCUSDT", "bidPrice": "99999", "askPrice": "100001"},
            {"symbol": "LOWUSDT", "bidPrice": "10", "askPrice": "10.01"},
            {"symbol": "WIDEUSDT", "bidPrice": "10", "askPrice": "10.10"},
            {"symbol": "MISSUSDT", "bidPrice": "", "askPrice": ""},
            {"symbol": "SHORTUSDT", "bidPrice": "20", "askPrice": "20.01"},
            {"symbol": "VOLUSDT", "bidPrice": "20", "askPrice": "20.01"},
            {"symbol": "BADUSDT", "bidPrice": "20", "askPrice": "20.01"},
            {"symbol": "BLACKUSDT", "bidPrice": "20", "askPrice": "20.01"},
            {"symbol": "ERRUSDT", "bidPrice": "20", "askPrice": "20.01"},
        ]

    def klines(self, symbol: str, interval: str = "1h", limit: int = 200):
        if symbol == "ERRUSDT":
            raise RuntimeError("mock candle failure")
        if symbol == "SHORTUSDT":
            return [[0, "1", "1.01", "0.99", "1", "100"]] * (limit - 1)
        if symbol == "VOLUSDT":
            return [[0, "10", "20", "1", "10", "100"]] * limit
        if symbol == "BADUSDT":
            return [[0, "1", "1.01", "0.99", "1", "0"]] * limit
        return [[0, "1", "1", "1", "1", "100"]] * limit

    def place_order(self, *args, **kwargs):  # pragma: no cover - must never be called
        raise AssertionError("pair discovery must not place orders")


def _db(tmp_path: Path) -> DB:
    db_path = tmp_path / "pair_universe.db"
    migrate(str(db_path))
    migrate(str(db_path))
    return DB(path=str(db_path))


def test_pair_universe_migration_and_helpers(tmp_path):
    db = _db(tmp_path)
    with db.connect() as conn:
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'signal_%'"
            ).fetchall()
        }
        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_signal_pair_%'"
            ).fetchall()
        }
    assert {"signal_pair_universe", "signal_pair_metrics", "signal_scan_runs", "signal_scan_results"} <= tables
    assert {"idx_signal_pair_universe_exchange", "idx_signal_pair_metrics_is_safe"} <= indexes

    upsert_signal_pair({"symbol": "btcusdt", "exchange": "binance_futures", "quote_asset": "USDT"}, db=db)
    upsert_signal_pair({"symbol": "BTCUSDT", "exchange": "binance_futures", "tier": "TIER_1"}, db=db)
    assert get_signal_pair("BTCUSDT", db=db)["tier"] == "TIER_1"
    blacklist_signal_pair("BTCUSDT", "manual test", db=db)
    assert list_signal_pairs(blacklisted=1, db=db)[0]["blacklist_reason"] == "manual test"

    upsert_signal_pair_metrics(
        {
            "symbol": "BTCUSDT",
            "exchange": "binance_futures",
            "quote_volume_24h": 600_000_000,
            "spread_percent": 0.01,
            "is_safe": 1,
        },
        db=db,
    )
    mark_pair_unsafe("BTCUSDT", "TEST_UNSAFE", db=db)
    assert get_signal_pair_metrics("BTCUSDT", db=db)["unsafe_reason"] == "TEST_UNSAFE"
    mark_pair_safe("BTCUSDT", db=db)
    assert get_signal_pair_metrics("BTCUSDT", db=db)["is_safe"] == 1

    run_id = create_signal_scan_run("PAIR_DISCOVERY", db=db)
    create_signal_scan_result({"scan_run_id": run_id, "symbol": "BTCUSDT", "was_scanned": 1}, db=db)
    complete_signal_scan_run(run_id, {"symbols_discovered": 1, "symbols_eligible": 1}, db=db)
    assert get_signal_scan_run(run_id, db=db)["status"] == "COMPLETED"
    assert list_signal_scan_results(scan_run_id=run_id, db=db)[0]["symbol"] == "BTCUSDT"


def test_pair_discovery_filters_stores_metrics_and_logs_results(tmp_path):
    db = _db(tmp_path)
    upsert_signal_pair({"symbol": "BLACKUSDT", "exchange": "binance_futures"}, db=db)
    blacklist_signal_pair("BLACKUSDT", "blocked by admin", db=db)
    service = PairDiscoveryService(market_client=FakePairMarketClient(), db=db)
    summary = service.discover_binance_futures_pairs(validate_candles=True, min_candles=200)

    assert summary["symbols_discovered"] == 12
    assert summary["symbols_eligible"] == 1
    assert summary["metrics_updated"] == 12
    assert summary["errors"] == [{"symbol": "ERRUSDT", "error": "API_ERROR"}]
    assert get_signal_pair("BTCUSDT", db=db)["enabled"] == 1
    assert get_signal_pair_metrics("BTCUSDT", db=db)["is_safe"] == 1
    assert get_signal_pair_metrics("LOWUSDT", db=db)["unsafe_reason"] == "LOW_VOLUME"
    assert get_signal_pair_metrics("WIDEUSDT", db=db)["unsafe_reason"] == "SPREAD_TOO_WIDE"
    assert get_signal_pair_metrics("MISSUSDT", db=db)["unsafe_reason"] == "MISSING_BID_ASK"
    assert get_signal_pair_metrics("ERRUSDT", db=db)["unsafe_reason"] == "API_ERROR"
    assert get_signal_pair_metrics("SHORTUSDT", db=db)["unsafe_reason"] == "INSUFFICIENT_HISTORY"
    assert get_signal_pair_metrics("VOLUSDT", db=db)["unsafe_reason"] == "EXTREME_VOLATILITY"
    assert get_signal_pair_metrics("BADUSDT", db=db)["unsafe_reason"] == "UNRELIABLE_CANDLES"
    assert get_signal_pair_metrics("BLACKUSDT", db=db)["unsafe_reason"] == "BLACKLISTED_SYMBOL"

    skipped = list_signal_scan_results(scan_run_id=summary["scan_run_id"], was_skipped=1, db=db)
    skip_reasons = {row["skip_reason"] for row in skipped}
    assert {
        "LOW_VOLUME",
        "SPREAD_TOO_WIDE",
        "MISSING_BID_ASK",
        "SYMBOL_NOT_TRADING",
        "UNSUPPORTED_QUOTE_ASSET",
        "UNSUPPORTED_CONTRACT_TYPE",
        "BLACKLISTED_SYMBOL",
        "INSUFFICIENT_HISTORY",
        "EXTREME_VOLATILITY",
        "UNRELIABLE_CANDLES",
        "API_ERROR",
    } <= skip_reasons
    run = get_signal_scan_run(summary["scan_run_id"], db=db)
    assert run["status"] == "PARTIAL"
    assert run["symbols_discovered"] == 12
    assert run["symbols_eligible"] == 1


def test_tier_seeding_preserves_blacklist_and_eligible_symbol_helper(tmp_path):
    db = _db(tmp_path)
    upsert_signal_pair({"symbol": "BTCUSDT", "exchange": "binance_futures"}, db=db)
    blacklist_signal_pair("BTCUSDT", "manual risk block", db=db)

    summary = seed_default_signal_pair_universe(db=db)
    assert summary["tier_1_seeded"] == 5
    assert summary["tier_2_seeded"] == 30
    assert get_signal_pair("BTCUSDT", db=db)["tier"] == "TIER_1"
    assert get_signal_pair("BTCUSDT", db=db)["blacklisted"] == 1
    assert get_signal_pair("ADAUSDT", db=db)["tier"] == "TIER_2"
    assert get_signal_pair("DOTUSDT", db=db)["tier"] == "TIER_2"

    upsert_signal_pair_metrics(
        {
            "symbol": "ETHUSDT",
            "exchange": "binance_futures",
            "quote_volume_24h": 600_000_000,
            "spread_percent": 0.01,
            "is_safe": 1,
            "reliability_score": 95,
        },
        db=db,
    )
    upsert_signal_pair_metrics(
        {
            "symbol": "ADAUSDT",
            "exchange": "binance_futures",
            "quote_volume_24h": 80_000_000,
            "spread_percent": 0.05,
            "is_safe": 1,
            "reliability_score": 80,
        },
        db=db,
    )
    upsert_signal_pair_metrics(
        {
            "symbol": "DOGEUSDT",
            "exchange": "binance_futures",
            "quote_volume_24h": 80_000_000,
            "spread_percent": 0.05,
            "is_safe": 0,
            "reliability_score": 80,
        },
        db=db,
    )

    eligible = get_eligible_signal_symbols(tiers=("TIER_1", "TIER_2"), db=db)
    assert eligible == ["ETHUSDT", "ADAUSDT"]
    assert "BTCUSDT" not in eligible
    assert "DOGEUSDT" not in eligible
    assert ALLOWED_CRYPTO_SYMBOLS == (
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "ADAUSDT",
        "DOGEUSDT",
        "LINKUSDT",
        "AVAXUSDT",
        "LTCUSDT",
    )
    assert TIER_1_CRYPTO_SYMBOLS == ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT")
    assert "DOTUSDT" in TIER_2_CRYPTO_SYMBOLS


def test_scheduled_pair_discovery_respects_pause_and_lock(tmp_path, capsys):
    db = _db(tmp_path)
    set_signal_setting("pair_discovery_paused", "true", db=db)

    result = discover_script.main(["--db-path", db.path, "--scheduled"])
    paused_output = capsys.readouterr().out
    assert result == 0
    assert '"paused": true' in paused_output

    set_signal_setting("pair_discovery_paused", "false", db=db)
    assert acquire_signal_operation_lock("PAIR_DISCOVERY", 60, locked_by="existing", db=db)
    result = discover_script.main(["--db-path", db.path, "--scheduled"])
    locked_output = capsys.readouterr().out
    assert result == 0
    assert '"lock_not_acquired": true' in locked_output
    release_signal_operation_lock("PAIR_DISCOVERY", db=db)
