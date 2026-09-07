from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from app.events.event_news_mode_controller import EventNewsModeController
from shared_lib.persistence.db import DB
from shared_lib.persistence.event_news_mode import get_event_news_runtime_mode


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _seed_schema(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE raw_news_items (id INTEGER PRIMARY KEY, ingested_utc TEXT)")
    conn.execute("CREATE TABLE news_clusters (id INTEGER PRIMARY KEY)")
    conn.execute("CREATE TABLE news_provider_health (id INTEGER PRIMARY KEY, source_id TEXT, status TEXT)")
    conn.execute(
        """
        CREATE TABLE news_intelligence_signals (
            id INTEGER PRIMARY KEY,
            shadow_only INTEGER NOT NULL DEFAULT 1,
            should_affect_trading INTEGER NOT NULL DEFAULT 0
        )
        """
    )


def _seed_ready_news(conn: sqlite3.Connection, *, now: datetime) -> None:
    old = _iso(now - timedelta(hours=30))
    recent = _iso(now - timedelta(minutes=5))
    conn.execute("INSERT INTO raw_news_items (ingested_utc) VALUES (?)", (old,))
    conn.execute("INSERT INTO raw_news_items (ingested_utc) VALUES (?)", (recent,))
    conn.execute("INSERT INTO news_clusters DEFAULT VALUES")
    conn.execute("INSERT INTO news_provider_health (source_id, status) VALUES ('rss', 'HEALTHY')")
    conn.execute("INSERT INTO news_intelligence_signals (shadow_only, should_affect_trading) VALUES (1, 0)")


def _patch_safe_config(monkeypatch, *, min_hours: float = 24.0) -> None:
    base = "app.events.event_news_readiness_evaluator.settings"
    monkeypatch.setattr(f"{base}.NEWS_INTELLIGENCE_ENABLED", True)
    monkeypatch.setattr(f"{base}.NEWS_SHADOW_ONLY", True)
    monkeypatch.setattr(f"{base}.NEWS_TRADING_ENABLED", False)
    monkeypatch.setattr(f"{base}.NEWS_SIGNAL_CAN_OPEN_TRADES", False)
    monkeypatch.setattr(f"{base}.NEWS_SIGNAL_CAN_CLOSE_TRADES", False)
    monkeypatch.setattr(f"{base}.NEWS_SIGNAL_CAN_BLOCK_TRADES", False)
    monkeypatch.setattr(f"{base}.REAL_TIME_NEWS_CAN_OPEN_TRADES", False)
    monkeypatch.setattr(f"{base}.REAL_TIME_NEWS_CAN_CLOSE_TRADES", False)
    monkeypatch.setattr(f"{base}.REAL_TIME_NEWS_CAN_BLOCK_TRADES", False)
    monkeypatch.setattr(f"{base}.REACTION_ALLOW_RISK_INFLUENCE", False)
    monkeypatch.setattr(f"{base}.EVENT_NEWS_ADVISORY_MIN_RUNTIME_HOURS", min_hours)
    monkeypatch.setattr(f"{base}.EVENT_NEWS_ADVISORY_MIN_RAW_NEWS_ITEMS", 1)
    monkeypatch.setattr(f"{base}.EVENT_NEWS_ADVISORY_MIN_CLUSTERS", 1)
    monkeypatch.setattr(f"{base}.EVENT_NEWS_ADVISORY_MIN_PROVIDER_HEALTH_ROWS", 1)
    monkeypatch.setattr(f"{base}.EVENT_NEWS_ADVISORY_MAX_FAILED_PROVIDERS", 999999)
    monkeypatch.setattr(f"{base}.EVENT_NEWS_ADVISORY_MAX_UNSAFE_SIGNALS", 0)
    monkeypatch.setattr("app.events.event_news_mode_controller.settings.EVENT_NEWS_AUTO_PROMOTION_ENABLED", True)
    monkeypatch.setattr("app.events.event_news_mode_controller.settings.EVENT_NEWS_AUTO_DEMOTION_ENABLED", True)


def test_controller_initializes_shadow_annotate_only(tmp_path):
    db = DB(path=str(tmp_path / "mode_init.db"))

    state = get_event_news_runtime_mode(db)

    assert state["current_mode"] == "SHADOW"
    assert state["max_allowed_action"] == "ANNOTATE_ONLY"
    assert state["reason"] == "Initial safe shadow mode"


def test_controller_promotes_shadow_to_advisory_without_execution_impact(tmp_path, monkeypatch):
    db = DB(path=str(tmp_path / "mode_promote.db"))
    now = datetime.now(timezone.utc)
    _patch_safe_config(monkeypatch, min_hours=24.0)
    with db.connect() as conn:
        _seed_schema(conn)
        _seed_ready_news(conn, now=now)

    decision = EventNewsModeController(db).evaluate_and_record()
    state = get_event_news_runtime_mode(db)

    assert decision["decision_type"] == "PROMOTE"
    assert decision["current_mode"] == "ADVISORY"
    assert decision["max_allowed_action"] == "ANNOTATE_ONLY"
    assert decision["execution_impact"] is False
    assert state["current_mode"] == "ADVISORY"
    assert state["max_allowed_action"] == "ANNOTATE_ONLY"


def test_controller_demotes_advisory_to_shadow_when_readiness_degrades(tmp_path, monkeypatch):
    db = DB(path=str(tmp_path / "mode_demote.db"))
    _patch_safe_config(monkeypatch, min_hours=24.0)
    with db.connect() as conn:
        _seed_schema(conn)
    EventNewsModeController(db).evaluate_and_record()
    with db.connect() as conn:
        conn.execute("UPDATE event_news_runtime_mode SET current_mode='ADVISORY', max_allowed_action='ANNOTATE_ONLY'")

    decision = EventNewsModeController(db).evaluate_and_record()

    assert decision["decision_type"] == "DEMOTE"
    assert decision["current_mode"] == "SHADOW"
    assert "insufficient_raw_news_items" in decision["failed_criteria"]


def test_controller_disables_on_unsafe_news_signal_invariant(tmp_path, monkeypatch):
    db = DB(path=str(tmp_path / "mode_disable.db"))
    _patch_safe_config(monkeypatch, min_hours=0.0)
    with db.connect() as conn:
        _seed_schema(conn)
        _seed_ready_news(conn, now=datetime.now(timezone.utc))
        conn.execute(
            "INSERT INTO news_intelligence_signals (shadow_only, should_affect_trading) VALUES (0, 1)"
        )

    decision = EventNewsModeController(db).evaluate_and_record()

    assert decision["decision_type"] == "DISABLE"
    assert decision["current_mode"] == "DISABLED"
    assert decision["max_allowed_action"] == "NONE"
    assert "unsafe_news_signal_invariant_failed" in decision["failed_criteria"]


def test_controller_forces_future_modes_back_to_shadow_in_prompt_2(tmp_path, monkeypatch):
    db = DB(path=str(tmp_path / "mode_future.db"))
    _patch_safe_config(monkeypatch, min_hours=0.0)
    with db.connect() as conn:
        _seed_schema(conn)
    EventNewsModeController(db)
    with db.connect() as conn:
        conn.execute("UPDATE event_news_runtime_mode SET current_mode='RISK_GUARD'")

    decision = EventNewsModeController(db).evaluate_and_record()

    assert decision["decision_type"] == "DEMOTE"
    assert decision["current_mode"] == "SHADOW"
    assert decision["max_allowed_action"] == "ANNOTATE_ONLY"
