from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from app.events.event_news_influence_engine import EventNewsInfluenceEngine
from app.events.event_news_mode_controller import EventNewsModeController
from shared_lib.persistence.db import DB
from shared_lib.persistence.event_news_mode import (
    ACTION_ANNOTATE_ONLY,
    ACTION_DELAY_ENTRY,
    ACTION_SIZE_REDUCTION,
    MODE_ADVISORY,
    MODE_RISK_LITE,
    ensure_event_news_mode_schema,
    get_event_news_runtime_mode,
)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _seed_schema(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS raw_news_items (id INTEGER PRIMARY KEY, ingested_utc TEXT, is_duplicate INTEGER DEFAULT 0)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_clusters (
            id INTEGER PRIMARY KEY,
            cluster_confidence REAL,
            highest_reliability_score REAL,
            conflict_flag INTEGER DEFAULT 0,
            fake_news_risk_score REAL DEFAULT 0,
            market_confirmation_status TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_asset_mappings (
            id INTEGER PRIMARY KEY,
            cluster_id INTEGER,
            symbol TEXT,
            is_global_market_event INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_intelligence_signals (
            id INTEGER PRIMARY KEY,
            cluster_id INTEGER,
            symbol TEXT,
            confidence_score REAL,
            reliability_score REAL,
            fake_news_risk_score REAL,
            conflict_flag INTEGER DEFAULT 0,
            market_confirmation_status TEXT,
            severity_level TEXT,
            created_at TEXT,
            shadow_only INTEGER DEFAULT 1,
            should_affect_trading INTEGER DEFAULT 0
        )
        """
    )
    conn.execute("CREATE TABLE IF NOT EXISTS news_provider_health (id INTEGER PRIMARY KEY, source_id TEXT, status TEXT)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_market_reactions (
            id INTEGER PRIMARY KEY,
            cluster_id INTEGER,
            symbol TEXT,
            volatility_expansion REAL,
            is_false_signal INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_event_reactions (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            volatility_expansion_ratio REAL,
            created_at TEXT
        )
        """
    )
    conn.execute("CREATE TABLE IF NOT EXISTS decision_traces (trace_id TEXT, ts TEXT, intended_action TEXT, signal TEXT)")


def _set_mode(conn: sqlite3.Connection, mode: str) -> None:
    ensure_event_news_mode_schema(DB(path=":memory:"))  # exercises schema import without touching test DB
    now = _iso(datetime.now(timezone.utc))
    conn.execute(
        """
        INSERT OR IGNORE INTO event_news_runtime_mode (
            id, current_mode, previous_mode, max_allowed_action, readiness_score,
            safety_status, auto_promotion_enabled, auto_demotion_enabled, reason,
            failed_criteria_json, passed_criteria_json, created_at, updated_at
        )
        VALUES (1, ?, NULL, 'ANNOTATE_ONLY', 100, 'SAFE', 1, 1, 'test', '[]', '[]', ?, ?)
        """,
        (mode, now, now),
    )
    max_action = ACTION_DELAY_ENTRY if mode == MODE_RISK_LITE else ACTION_ANNOTATE_ONLY
    conn.execute(
        "UPDATE event_news_runtime_mode SET current_mode=?, max_allowed_action=?, updated_at=? WHERE id=1",
        (mode, max_action, now),
    )


def _seed_high_quality_signal(conn: sqlite3.Connection, *, symbol: str = "ETHUSDT", conflict: int = 0, fake_risk: float = 0.05) -> None:
    now = _iso(datetime.now(timezone.utc) - timedelta(minutes=5))
    conn.execute(
        """
        INSERT INTO news_clusters (
            id, cluster_confidence, highest_reliability_score, conflict_flag,
            fake_news_risk_score, market_confirmation_status
        )
        VALUES (1, 0.93, 0.94, ?, ?, 'CONFIRMED')
        """,
        (conflict, fake_risk),
    )
    conn.execute("INSERT INTO news_asset_mappings (cluster_id, symbol) VALUES (1, ?)", (symbol,))
    conn.execute(
        """
        INSERT INTO news_intelligence_signals (
            cluster_id, symbol, confidence_score, reliability_score,
            fake_news_risk_score, conflict_flag, market_confirmation_status,
            severity_level, created_at, shadow_only, should_affect_trading
        )
        VALUES (1, ?, 0.92, 0.93, ?, ?, 'CONFIRMED', 'HIGH', ?, 1, 0)
        """,
        (symbol, fake_risk, conflict, now),
    )
    conn.execute("INSERT INTO news_provider_health (source_id, status) VALUES ('rss', 'HEALTHY')")


def _db(tmp_path, name: str) -> DB:
    db = DB(path=str(tmp_path / name))
    ensure_event_news_mode_schema(db)
    with db.connect() as conn:
        _seed_schema(conn)
    return db


def test_advisory_remains_annotate_only(tmp_path):
    db = _db(tmp_path, "advisory.db")
    with db.connect() as conn:
        _set_mode(conn, MODE_ADVISORY)
        _seed_high_quality_signal(conn)

    result = EventNewsInfluenceEngine(db).evaluate(symbol="ETHUSDT", trace_id="t1", trade_usdt=100)

    assert result.mode == MODE_ADVISORY
    assert result.applied_action == ACTION_ANNOTATE_ONLY
    assert result.execution_impact_allowed is False
    assert result.size_multiplier == 1.0


def test_risk_lite_high_quality_news_can_reduce_size_but_not_below_cap(tmp_path):
    db = _db(tmp_path, "risk_lite_size.db")
    with db.connect() as conn:
        _set_mode(conn, MODE_RISK_LITE)
        _seed_high_quality_signal(conn)

    result = EventNewsInfluenceEngine(db).evaluate(symbol="ETHUSDT", trace_id="t2", trade_usdt=100)

    assert result.applied_action == ACTION_SIZE_REDUCTION
    assert result.execution_impact_allowed is True
    assert 0.75 <= result.size_multiplier <= 1.0


def test_conflict_or_high_fake_risk_news_cannot_reduce_size(tmp_path):
    db = _db(tmp_path, "risk_lite_fake.db")
    with db.connect() as conn:
        _set_mode(conn, MODE_RISK_LITE)
        _seed_high_quality_signal(conn, conflict=1, fake_risk=0.60)

    result = EventNewsInfluenceEngine(db).evaluate(symbol="ETHUSDT", trace_id="t3", trade_usdt=100)

    assert result.applied_action != ACTION_SIZE_REDUCTION
    assert result.size_multiplier == 1.0


def test_risk_lite_delay_is_capped(tmp_path):
    db = _db(tmp_path, "risk_lite_delay.db")
    with db.connect() as conn:
        _set_mode(conn, MODE_RISK_LITE)
        conn.execute("INSERT INTO news_provider_health (source_id, status) VALUES ('rss', 'HEALTHY')")
        conn.execute(
            "INSERT INTO market_event_reactions (symbol, volatility_expansion_ratio, created_at) VALUES ('ETHUSDT', 3.0, ?)",
            (_iso(datetime.now(timezone.utc) - timedelta(minutes=2)),),
        )

    result = EventNewsInfluenceEngine(db).evaluate(symbol="ETHUSDT", trace_id="t4", trade_usdt=100)

    assert result.applied_action == ACTION_DELAY_ENTRY
    assert 0 <= result.delay_seconds <= 300


def test_influence_engine_writes_ledger(tmp_path):
    db = _db(tmp_path, "ledger.db")
    with db.connect() as conn:
        _set_mode(conn, MODE_RISK_LITE)
        _seed_high_quality_signal(conn)

    result = EventNewsInfluenceEngine(db).evaluate(symbol="ETHUSDT", trace_id="ledger-trace", trade_usdt=100)

    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM event_news_influence_decisions WHERE id=?",
            (result.ledger_id,),
        ).fetchone()
    assert row is not None
    assert row["trace_id"] == "ledger-trace"
    assert row["applied_action"] == ACTION_SIZE_REDUCTION


def test_controller_does_not_promote_to_risk_lite_by_default(tmp_path, monkeypatch):
    db = _db(tmp_path, "controller_default.db")
    now = datetime.now(timezone.utc)
    with db.connect() as conn:
        _set_mode(conn, MODE_ADVISORY)
        conn.execute(
            "UPDATE event_news_runtime_mode SET promoted_at=?, updated_at=? WHERE id=1",
            (_iso(now - timedelta(days=8)), _iso(now - timedelta(days=8))),
        )
        for i in range(250):
            conn.execute("INSERT INTO raw_news_items (ingested_utc) VALUES (?)", (_iso(now - timedelta(days=2)),))
            conn.execute("INSERT INTO news_clusters (id) VALUES (?)", (i + 1,))
            conn.execute(
                "INSERT INTO news_market_reactions (cluster_id, symbol, is_false_signal, created_at) VALUES (?, 'ETHUSDT', 0, ?)",
                (i + 1, _iso(now - timedelta(hours=1))),
            )
        conn.execute("INSERT INTO news_provider_health (source_id, status) VALUES ('rss', 'HEALTHY')")
        conn.execute("INSERT INTO news_intelligence_signals (shadow_only, should_affect_trading) VALUES (1, 0)")

    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_INTELLIGENCE_ENABLED", True)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_SHADOW_ONLY", True)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_TRADING_ENABLED", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_SIGNAL_CAN_OPEN_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_SIGNAL_CAN_CLOSE_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_SIGNAL_CAN_BLOCK_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.REAL_TIME_NEWS_CAN_OPEN_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.REAL_TIME_NEWS_CAN_CLOSE_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.REAL_TIME_NEWS_CAN_BLOCK_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.REACTION_ALLOW_RISK_INFLUENCE", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.EVENT_NEWS_ADVISORY_MIN_RUNTIME_HOURS", 1.0)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.EVENT_NEWS_ADVISORY_MIN_RAW_NEWS_ITEMS", 1)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.EVENT_NEWS_ADVISORY_MIN_CLUSTERS", 1)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.EVENT_NEWS_ADVISORY_MIN_PROVIDER_HEALTH_ROWS", 1)
    monkeypatch.setattr("app.events.event_news_mode_controller.settings.EVENT_NEWS_AUTO_PROMOTION_ENABLED", False)

    decision = EventNewsModeController(db).evaluate_and_record()

    assert decision["current_mode"] == MODE_ADVISORY
    assert get_event_news_runtime_mode(db)["current_mode"] == MODE_ADVISORY


def test_controller_dev_can_promote_to_risk_lite_when_all_gates_pass(tmp_path, monkeypatch):
    db = _db(tmp_path, "controller_dev.db")
    now = datetime.now(timezone.utc)
    with db.connect() as conn:
        _set_mode(conn, MODE_ADVISORY)
        conn.execute(
            "UPDATE event_news_runtime_mode SET promoted_at=?, updated_at=? WHERE id=1",
            (_iso(now - timedelta(hours=1)), _iso(now - timedelta(hours=1))),
        )
        for i in range(120):
            conn.execute("INSERT INTO raw_news_items (ingested_utc) VALUES (?)", (_iso(now - timedelta(hours=1)),))
            conn.execute("INSERT INTO news_clusters (id) VALUES (?)", (i + 1,))
            conn.execute("INSERT INTO news_intelligence_signals (cluster_id, shadow_only, should_affect_trading) VALUES (?, 1, 0)", (i + 1,))
        conn.execute("INSERT INTO news_provider_health (source_id, status) VALUES ('rss', 'HEALTHY')")

    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_INTELLIGENCE_ENABLED", True)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_SHADOW_ONLY", True)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_TRADING_ENABLED", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_SIGNAL_CAN_OPEN_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_SIGNAL_CAN_CLOSE_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.NEWS_SIGNAL_CAN_BLOCK_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.REAL_TIME_NEWS_CAN_OPEN_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.REAL_TIME_NEWS_CAN_CLOSE_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.REAL_TIME_NEWS_CAN_BLOCK_TRADES", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.REACTION_ALLOW_RISK_INFLUENCE", False)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.EVENT_NEWS_ADVISORY_MIN_RUNTIME_HOURS", 0.1)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.EVENT_NEWS_ADVISORY_MIN_RAW_NEWS_ITEMS", 1)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.EVENT_NEWS_ADVISORY_MIN_CLUSTERS", 1)
    monkeypatch.setattr("app.events.event_news_readiness_evaluator.settings.EVENT_NEWS_ADVISORY_MIN_PROVIDER_HEALTH_ROWS", 1)
    monkeypatch.setattr("app.events.event_news_mode_controller.settings.EVENT_NEWS_AUTO_PROMOTION_ENABLED", True)
    monkeypatch.setattr("app.events.event_news_mode_controller.settings.EVENT_NEWS_ALLOW_DEV_RISK_LITE_PROMOTION", True)
    monkeypatch.setattr("app.events.event_news_mode_controller.settings.EVENT_NEWS_DEV_MIN_ADVISORY_MINUTES", 30)
    monkeypatch.setattr("app.events.event_news_mode_controller.settings.EVENT_NEWS_DEV_MIN_CLUSTERS", 100)
    monkeypatch.setattr("app.events.event_news_mode_controller.settings.EVENT_NEWS_DEV_MIN_SIGNALS", 100)
    monkeypatch.setattr("app.events.event_news_mode_controller.settings.BINANCE_ENV", "testnet")

    decision = EventNewsModeController(db).evaluate_and_record()

    assert decision["current_mode"] == MODE_RISK_LITE
    assert decision["max_allowed_action"] == ACTION_DELAY_ENTRY
