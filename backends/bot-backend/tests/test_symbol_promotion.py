from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from app.symbols.symbol_promotion import SymbolPromotionEvaluator
from shared_lib.persistence.db import DB


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _create_ranking_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_universe_rankings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ranking_run_id TEXT,
            created_at TEXT NOT NULL,
            bot_instance_id TEXT,
            mode TEXT NOT NULL,
            symbol TEXT NOT NULL,
            rank INTEGER,
            score REAL,
            recommended_action TEXT NOT NULL,
            selected_for_trading INTEGER NOT NULL DEFAULT 0,
            preserved_for_management INTEGER NOT NULL DEFAULT 0,
            quote_volume_24h REAL,
            spread_bps REAL,
            volatility_quality REAL,
            candle_sufficiency INTEGER,
            funding_stability REAL,
            open_interest REAL,
            signal_frequency REAL,
            average_confidence REAL,
            would_pass_count INTEGER,
            recent_performance_score REAL,
            inclusion_reason TEXT,
            exclusion_reason TEXT,
            diagnostics_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dynamic_universe_shadow_diagnostics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            was_evaluated INTEGER NOT NULL DEFAULT 0,
            would_pass_strategy INTEGER NOT NULL DEFAULT 0,
            confidence REAL
        )
        """
    )


def _insert_ranking_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    created_at: str,
    bot_id: str,
    trade_symbols: list[str],
    watch_symbols: list[str] | None = None,
) -> None:
    watch_symbols = watch_symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    symbols = list(dict.fromkeys(trade_symbols + watch_symbols))
    for idx, symbol in enumerate(symbols, start=1):
        action = "TRADE" if symbol in trade_symbols else "WATCH"
        conn.execute(
            """
            INSERT INTO symbol_universe_rankings (
                ranking_run_id, created_at, bot_instance_id, mode, symbol, rank,
                score, recommended_action, selected_for_trading, preserved_for_management,
                quote_volume_24h, spread_bps, volatility_quality, candle_sufficiency,
                funding_stability, open_interest, signal_frequency, average_confidence,
                would_pass_count, recent_performance_score, inclusion_reason,
                exclusion_reason, diagnostics_json
            )
            VALUES (?, ?, ?, 'dynamic_shadow', ?, ?, ?, ?, 0, 0, ?, ?, 0.8, 1,
                    0.9, 1000000, 0.4, 0.42, 5, 1.0, ?, NULL, ?)
            """,
            (
                run_id,
                created_at,
                bot_id,
                symbol,
                idx,
                80.0 - idx,
                action,
                500_000_000.0,
                1.0,
                "trusted_liquid_tiered_symbol_with_repeat_shadow_signal"
                if action == "TRADE"
                else "passes_quality_filters_but_needs_more_signal_history",
                json.dumps(
                    {
                        "manual_review": False,
                        "denylisted": False,
                        "components": {
                            "meets_trade_trust": True,
                            "meets_market_quality": True,
                            "manual_penalty": 0.0,
                        },
                    }
                ),
            ),
        )


def _insert_shadow_samples(conn: sqlite3.Connection, symbols: list[str], *, now: datetime) -> None:
    for symbol in symbols:
        for idx in range(25):
            conn.execute(
                """
                INSERT INTO dynamic_universe_shadow_diagnostics (
                    created_at, symbol, was_evaluated, would_pass_strategy, confidence
                )
                VALUES (?, ?, 1, ?, ?)
                """,
                (_iso(now - timedelta(minutes=idx)), symbol, 1 if idx < 5 else 0, 0.40),
            )


def test_symbol_promotion_persists_not_ready_without_live_switch(tmp_path, monkeypatch):
    db = DB(path=str(tmp_path / "promotion_not_ready.db"))
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
    changed_at = now - timedelta(hours=12)
    bot_id = "bot_test"
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.SYMBOL_UNIVERSE_MODE", "dynamic_shadow")
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.AUTO_SYMBOL_PROMOTION_ENABLED", False)
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.AUTO_SYMBOL_SCORING_MODEL_CHANGED_AT", _iso(changed_at))

    with db.connect() as conn:
        _create_ranking_schema(conn)
        _insert_ranking_run(
            conn,
            run_id="rank_1",
            created_at=_iso(now),
            bot_id=bot_id,
            trade_symbols=["AAAUSDT"],
        )
        _insert_shadow_samples(conn, ["AAAUSDT"], now=now)

    decision = SymbolPromotionEvaluator(db).evaluate_and_record(bot_instance_id=bot_id)

    assert decision["decision_type"] == "PROMOTION_EVALUATED"
    assert decision["status"] == "FAIL"
    assert decision["executed"] is False
    assert "observation_window_too_short" in decision["failure_reasons"]
    assert "insufficient_ranking_runs" in decision["failure_reasons"]
    assert getattr(__import__("app.core.config", fromlist=["settings"]).settings, "SYMBOL_UNIVERSE_MODE") == "dynamic_shadow"

    with sqlite3.connect(db.path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT decision_type, status, executed, failure_reasons_json "
            "FROM symbol_universe_promotion_decisions ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row["decision_type"] == "PROMOTION_EVALUATED"
    assert row["status"] == "FAIL"
    assert row["executed"] == 0


def test_symbol_promotion_recommends_when_all_gates_pass_but_does_not_switch(tmp_path, monkeypatch):
    db = DB(path=str(tmp_path / "promotion_ready.db"))
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
    changed_at = now - timedelta(hours=96)
    bot_id = "bot_ready"
    trade_symbols = [f"Q{i:02d}USDT" for i in range(10)]
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.SYMBOL_UNIVERSE_MODE", "dynamic_shadow")
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.AUTO_SYMBOL_PROMOTION_ENABLED", False)
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.AUTO_SYMBOL_SCORING_MODEL_CHANGED_AT", _iso(changed_at))
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.AUTO_SYMBOL_PROMOTION_MIN_HOURS", 72)
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.AUTO_SYMBOL_PROMOTION_MIN_RANKING_RUNS", 100)
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.AUTO_SYMBOL_PROMOTION_MIN_TRADE_SYMBOLS", 10)
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.AUTO_SYMBOL_PROMOTION_MIN_CONFIDENCE_SAMPLES", 20)
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.AUTO_SYMBOL_TOP_N", 10)

    with db.connect() as conn:
        _create_ranking_schema(conn)
        _insert_shadow_samples(conn, trade_symbols, now=now)
        for idx in range(100):
            _insert_ranking_run(
                conn,
                run_id=f"rank_{idx}",
                created_at=_iso(now - timedelta(minutes=idx)),
                bot_id=bot_id,
                trade_symbols=trade_symbols,
                watch_symbols=[],
            )

    decision = SymbolPromotionEvaluator(db).evaluate_and_record(bot_instance_id=bot_id)

    assert decision["decision_type"] == "PROMOTION_RECOMMENDED"
    assert decision["status"] == "PASS"
    assert decision["executed"] is False
    assert decision["selected_symbols"] == trade_symbols
    assert getattr(__import__("app.core.config", fromlist=["settings"]).settings, "SYMBOL_UNIVERSE_MODE") == "dynamic_shadow"

    with sqlite3.connect(db.path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT decision_type, status, executed, selected_symbols_json "
            "FROM symbol_universe_promotion_decisions ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row["decision_type"] == "PROMOTION_RECOMMENDED"
    assert row["status"] == "PASS"
    assert row["executed"] == 0
    assert json.loads(row["selected_symbols_json"]) == trade_symbols
