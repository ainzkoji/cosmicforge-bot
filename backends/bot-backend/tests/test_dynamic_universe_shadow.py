from __future__ import annotations

import sqlite3

from app.symbols import dynamic_universe
from app.symbols.dynamic_universe import DynamicUniverseService, DynamicUniverseShadowRecorder
from app.symbols.symbol_scoring import SymbolScoreInput, score_symbol
from app.symbols.symbol_selector import DynamicSymbolSelector
from shared_lib.persistence.db import DB


def test_dynamic_universe_filters_and_ranks(monkeypatch):
    def fake_public_get(path: str, timeout: int = 10):
        if path == "/fapi/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "TRADING",
                        "quoteAsset": "USDT",
                        "baseAsset": "BTC",
                        "contractType": "PERPETUAL",
                    },
                    {
                        "symbol": "THINUSDT",
                        "status": "TRADING",
                        "quoteAsset": "USDT",
                        "baseAsset": "THIN",
                        "contractType": "PERPETUAL",
                    },
                    {
                        "symbol": "ETHUSD_PERP",
                        "status": "TRADING",
                        "quoteAsset": "USD",
                        "baseAsset": "ETH",
                        "contractType": "PERPETUAL",
                    },
                ]
            }
        if path == "/fapi/v1/ticker/24hr":
            return [
                {"symbol": "BTCUSDT", "quoteVolume": "100000000"},
                {"symbol": "THINUSDT", "quoteVolume": "1000"},
            ]
        if path == "/fapi/v1/ticker/bookTicker":
            return [
                {"symbol": "BTCUSDT", "bidPrice": "100.00", "askPrice": "100.01"},
                {"symbol": "THINUSDT", "bidPrice": "10.00", "askPrice": "10.50"},
            ]
        if path.startswith("/fapi/v1/klines?"):
            return [[1, "100", "101", "99", "100", "10"], [2, "100", "101", "99", "100", "10"]]
        raise AssertionError(path)

    monkeypatch.setattr(dynamic_universe, "_public_get", fake_public_get)

    result = DynamicUniverseService(
        min_quote_volume_usdt=50_000_000,
        max_spread_bps=10,
    ).discover()

    assert result["total_exchange_symbols"] == 3
    assert result["total_structural_usdt_perpetual_symbols"] == 2
    assert [item["symbol"] for item in result["ranked_candidates"]] == ["BTCUSDT"]
    assert result["excluded_symbol_count_by_reason"]["low_24h_quote_volume"] == 1
    assert result["excluded_symbol_count_by_reason"]["quote_asset_not_usdt"] == 1


def test_dynamic_universe_excludes_symbols_without_demo_klines(monkeypatch):
    def fake_public_get(path: str, timeout: int = 10):
        if path == "/fapi/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "TRADING",
                        "quoteAsset": "USDT",
                        "baseAsset": "BTC",
                        "contractType": "PERPETUAL",
                    },
                    {
                        "symbol": "NOMUSDT",
                        "status": "TRADING",
                        "quoteAsset": "USDT",
                        "baseAsset": "NOM",
                        "contractType": "PERPETUAL",
                    },
                ]
            }
        if path == "/fapi/v1/ticker/24hr":
            return [
                {"symbol": "BTCUSDT", "quoteVolume": "1000000000"},
                {"symbol": "NOMUSDT", "quoteVolume": "900000000"},
            ]
        if path == "/fapi/v1/ticker/bookTicker":
            return [
                {"symbol": "BTCUSDT", "bidPrice": "100.00", "askPrice": "100.01"},
                {"symbol": "NOMUSDT", "bidPrice": "10.00", "askPrice": "10.01"},
            ]
        if path.startswith("/fapi/v1/klines?") and "BTCUSDT" in path:
            return [[1, "100", "101", "99", "100", "10"], [2, "100", "101", "99", "100", "10"]]
        if path.startswith("/fapi/v1/klines?") and "NOMUSDT" in path:
            raise RuntimeError("Binance HTTP 400: Invalid symbol")
        raise AssertionError(path)

    monkeypatch.setattr(dynamic_universe, "_KLINE_AVAILABILITY_CACHE", {})
    monkeypatch.setattr(dynamic_universe, "_public_get", fake_public_get)

    result = DynamicUniverseService(
        min_quote_volume_usdt=50_000_000,
        max_spread_bps=10,
    ).discover()

    assert [item["symbol"] for item in result["ranked_candidates"]] == ["BTCUSDT"]
    nom = next(item for item in result["structural_candidates"] if item["symbol"] == "NOMUSDT")
    assert nom["exclusion_reasons"] == ["klines_unavailable"]
    assert result["excluded_symbol_count_by_reason"]["klines_unavailable"] == 1


def test_dynamic_universe_shadow_recorder_persists_rows(tmp_path):
    db_path = tmp_path / "shadow.db"
    recorder = DynamicUniverseShadowRecorder(DB(path=str(db_path)))

    recorder.record_many(
        [
            {
                "created_at": "2026-04-26T00:00:00+00:00",
                "run_id": "bot_1",
                "cycle_id": "cycle_1",
                "bot_instance_id": "bot_1",
                "symbol": "BTCUSDT",
                "rank": 1,
                "in_live_config": True,
                "was_evaluated": False,
                "would_pass_strategy": False,
                "reason": "already_live_configured",
                "quote_volume_24h": 100000000.0,
                "spread_bps": 0.5,
                "exclusion_reasons": [],
                "diagnostics": {"shadow_only": True},
            },
            {
                "created_at": "2026-04-26T00:00:01+00:00",
                "run_id": "bot_1",
                "cycle_id": "cycle_1",
                "bot_instance_id": "bot_1",
                "symbol": "RAVEUSDT",
                "rank": 8,
                "in_live_config": False,
                "was_evaluated": True,
                "would_pass_strategy": True,
                "signal": "BUY",
                "confidence": 0.7,
                "threshold": 0.4,
                "reason": "master_ensemble_v2",
                "quote_volume_24h": 90000000.0,
                "spread_bps": 1.2,
                "exclusion_reasons": [],
                "diagnostics": {"shadow_only": True},
            },
        ]
    )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT symbol, in_live_config, was_evaluated, would_pass_strategy "
            "FROM dynamic_universe_shadow_diagnostics ORDER BY id"
        ).fetchall()

    assert [row["symbol"] for row in rows] == ["BTCUSDT", "RAVEUSDT"]
    assert rows[0]["in_live_config"] == 1
    assert rows[1]["was_evaluated"] == 1
    assert rows[1]["would_pass_strategy"] == 1


def test_symbol_scoring_actions_and_manual_review():
    trade = score_symbol(
        SymbolScoreInput(
            symbol="RAVEUSDT",
            rank=5,
            quote_volume_24h=500_000_000,
            spread_bps=1.0,
            evaluated_count=10,
            would_pass_count=3,
            confidence_sample_count=10,
            average_pass_confidence=0.7,
            average_confidence=0.45,
            max_confidence=0.8,
        )
    )
    assert trade["recommended_action"] == "TRADE"
    assert trade["score"] > 55

    denied = score_symbol(
        SymbolScoreInput(
            symbol="RAVEUSDT",
            quote_volume_24h=500_000_000,
            spread_bps=1.0,
            evaluated_count=10,
            would_pass_count=3,
            confidence_sample_count=10,
            average_pass_confidence=0.7,
            denylisted=True,
        )
    )
    assert denied["recommended_action"] == "EXCLUDE"
    assert denied["exclusion_reason"] == "denylisted"

    manual = score_symbol(
        SymbolScoreInput(
            symbol="TRUMPUSDT",
            quote_volume_24h=900_000_000,
            spread_bps=1.0,
            evaluated_count=10,
            would_pass_count=4,
            confidence_sample_count=10,
            average_pass_confidence=0.8,
        )
    )
    assert manual["recommended_action"] == "MANUAL_REVIEW"


def test_dynamic_symbol_selector_persists_shadow_rankings(tmp_path, monkeypatch):
    db_path = tmp_path / "rankings.db"
    db = DB(path=str(db_path))
    recorder = DynamicUniverseShadowRecorder(db)
    rows_to_record = []
    for idx in range(8):
        rows_to_record.append(
            {
                "created_at": f"2026-04-26T00:00:0{idx}+00:00",
                "run_id": "bot_1",
                "cycle_id": f"cycle_{idx}",
                "bot_instance_id": "bot_1",
                "symbol": "RAVEUSDT",
                "rank": 1,
                "in_live_config": False,
                "was_evaluated": True,
                "would_pass_strategy": idx < 3,
                "signal": "BUY" if idx < 3 else "HOLD",
                "confidence": 0.72 if idx < 3 else 0.28,
                "threshold": 0.45,
                "reason": "master_ensemble_v2",
                "quote_volume_24h": 500_000_000.0,
                "spread_bps": 1.1,
                "exclusion_reasons": [],
                "diagnostics": {"meta": {"atr_pct": 2.4}},
            },
        )
    recorder.record_many(rows_to_record)
    monkeypatch.setattr("app.symbols.symbol_selector.settings.SYMBOL_UNIVERSE_MODE", "dynamic_shadow")
    monkeypatch.setattr("app.symbols.symbol_selector.settings.AUTO_SYMBOL_SELECTION_ENABLED", False)
    monkeypatch.setattr("app.symbols.symbol_selector.settings.AUTO_SYMBOL_TOP_N", 20)
    monkeypatch.setattr("app.symbols.symbol_selector.settings.AUTO_SYMBOL_DENYLIST", "BADUSDT")

    universe = {
        "structural_candidates": [
            {
                "symbol": "RAVEUSDT",
                "rank": 1,
                "quote_volume_24h": 500_000_000.0,
                "spread_bps": 1.1,
                "exclusion_reasons": [],
            },
            {
                "symbol": "BADUSDT",
                "rank": 2,
                "quote_volume_24h": 900_000_000.0,
                "spread_bps": 1.0,
                "exclusion_reasons": [],
            },
            {
                "symbol": "THINUSDT",
                "rank": None,
                "quote_volume_24h": 10_000.0,
                "spread_bps": 20.0,
                "exclusion_reasons": ["low_24h_quote_volume", "wide_book_spread"],
            },
        ]
    }

    rows = DynamicSymbolSelector(db).rank_shadow_universe(
        universe,
        live_symbols={"BTCUSDT"},
        bot_instance_id="bot_1",
        persist=True,
    )

    assert rows[0]["symbol"] == "RAVEUSDT"
    assert rows[0]["recommended_action"] == "TRADE"
    assert rows[0]["selected_for_trading"] is False

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        stored = conn.execute(
            "SELECT symbol, recommended_action, selected_for_trading, exclusion_reason "
            "FROM symbol_universe_rankings ORDER BY rank"
        ).fetchall()

    by_symbol = {row["symbol"]: dict(row) for row in stored}
    assert by_symbol["RAVEUSDT"]["recommended_action"] == "TRADE"
    assert by_symbol["RAVEUSDT"]["selected_for_trading"] == 0
    assert by_symbol["BADUSDT"]["recommended_action"] == "EXCLUDE"
    assert by_symbol["BADUSDT"]["exclusion_reason"] == "denylisted"
    assert by_symbol["THINUSDT"]["recommended_action"] == "EXCLUDE"
