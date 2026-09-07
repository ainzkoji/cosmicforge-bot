from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_events, admin_news  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402


def _iso(offset_minutes: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)).isoformat()


def _make_client(tmp_path: Path) -> tuple[TestClient, DB]:
    db_path = tmp_path / "event_news_admin_smoke.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))

    app = FastAPI()
    app.include_router(admin_events.router, prefix="/api")
    app.include_router(admin_news.router, prefix="/api")
    app.dependency_overrides[require_admin] = lambda: {"id": "admin-test", "role": "admin"}
    app.dependency_overrides[admin_events._get_db] = lambda: db
    app.dependency_overrides[admin_news._get_db] = lambda: db
    return TestClient(app), db


def _seed_full_event_news_chain(db: DB) -> None:
    now = _iso()
    future = _iso(15)
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO economic_events
                (event_id, title, event_type, country_currency, impact_level,
                 scheduled_utc, source, created_at, updated_at)
            VALUES ('evt-smoke-cpi', 'US CPI Smoke Test', 'CPI', 'USD', 'HIGH',
                    ?, 'smoke', ?, ?)
            """,
            (future, now, now),
        )
        event_db_id = cur.lastrowid
        conn.execute(
            """
            INSERT INTO event_blackout_windows
                (event_id, start_utc, end_utc, affected_symbols, is_global,
                 is_active, reason, created_at)
            VALUES (?, ?, ?, NULL, 1, 1, 'HIGH impact macro event', ?)
            """,
            (event_db_id, _iso(-5), _iso(25), now),
        )
        conn.execute(
            """
            INSERT INTO market_event_reactions
                (event_id, symbol, exchange, event_time_utc,
                 price_before_event, price_at_event, price_after_5m,
                 max_move_pct, min_move_pct, net_move_pct,
                 atr_before, atr_after, volatility_expansion_ratio,
                 average_volume_before, event_volume, volume_spike_ratio,
                 reaction_type, confidence_score, data_quality, created_at, updated_at)
            VALUES ('evt-smoke-cpi', 'BTCUSDT', 'BINANCE', ?,
                    100.0, 101.0, 102.0,
                    2.0, -0.5, 1.5,
                    1.0, 2.8, 2.8,
                    1000.0, 3000.0, 3.0,
                    'VOL_SPIKE', 0.9, 'COMPLETE', ?, ?)
            """,
            (now, now, now),
        )
        conn.execute(
            """
            INSERT INTO news_sources
                (id, source_name, base_reliability_score, dynamic_reliability_score,
                 is_trusted, is_blocked, created_at, updated_at, source_type,
                 category, is_enabled, rss_url, last_fetch_utc, last_success_utc)
            VALUES ('coindesk-smoke', 'CoinDesk Smoke', 0.8, 0.82,
                    1, 0, ?, ?, 'RSS', 'CRYPTO', 1,
                    'https://example.test/rss', ?, ?)
            """,
            (now, now, now, now),
        )
        conn.execute(
            """
            INSERT INTO news_provider_health
                (source_id, status, last_checked_utc, last_success_utc,
                 items_fetched_last_run, duplicate_count_last_run, created_at)
            VALUES ('coindesk-smoke', 'HEALTHY', ?, ?, 1, 0, ?)
            """,
            (now, now, now),
        )
        conn.execute(
            """
            INSERT INTO real_time_news_provider_status
                (provider, is_enabled, last_fetch_utc, last_success_utc,
                 latency_avg_seconds, items_fetched_today, duplicate_rate,
                 health_status, created_at, updated_at)
            VALUES ('rss', 1, ?, ?, 1.5, 1, 0.0, 'HEALTHY', ?, ?)
            """,
            (now, now, now, now),
        )
        raw = conn.execute(
            """
            INSERT INTO raw_news_items
                (provider, source_name, source_domain, source_url, external_id,
                 title, body_snippet, published_utc, ingested_utc,
                 latency_seconds, language, is_duplicate, created_at)
            VALUES ('rss', 'CoinDesk Smoke', 'example.test', 'https://example.test/news',
                    'smoke-raw-1', 'Bitcoin volatility around CPI',
                    'Smoke item for admin API test', ?, ?, 2.0, 'en', 0, ?)
            """,
            (now, now, now),
        ).lastrowid
        cluster = conn.execute(
            """
            INSERT INTO news_clusters
                (canonical_title, summary, first_seen_utc, last_seen_utc,
                 source_count, provider_count, highest_reliability_score,
                 cluster_confidence, is_manipulation_suspect, created_at, updated_at,
                 spam_score, latency_score, is_valid_signal, data_quality_status,
                 first_seen_provider, confirmation_count, conflict_flag,
                 fake_news_risk_score, market_confirmation_status)
            VALUES ('Bitcoin volatility around CPI',
                    'Smoke cluster for admin API test', ?, ?, 1, 1, 0.82,
                    0.88, 0, ?, ?, 0.0, 0.95, 1, 'HIGH_CONFIDENCE',
                    'rss', 1, 0, 0.1, 'CONFIRMED')
            """,
            (now, now, now, now),
        ).lastrowid
        conn.execute(
            "INSERT INTO news_cluster_items (cluster_id, raw_news_item_id, similarity_score, created_at) VALUES (?, ?, 1.0, ?)",
            (cluster, raw, now),
        )
        conn.execute(
            "INSERT INTO news_asset_mappings (cluster_id, symbol, asset, mapping_reason, mapping_confidence, created_at) VALUES (?, 'BTCUSDT', 'crypto', 'smoke', 1.0, ?)",
            (cluster, now),
        )
        conn.execute(
            """
            INSERT INTO news_sentiment_scores
                (cluster_id, sentiment_score, sentiment_label, confidence_score,
                 model_version, created_at)
            VALUES (?, 0.35, 'POSITIVE', 0.8, 'smoke-1.0', ?)
            """,
            (cluster, now),
        )
        conn.execute(
            """
            INSERT INTO news_narratives
                (cluster_id, narrative_type, narrative_confidence,
                 severity_level, matched_keywords, created_at)
            VALUES (?, 'MACRO_POLICY', 0.8, 'MEDIUM', 'cpi', ?)
            """,
            (cluster, now),
        )
        conn.execute(
            """
            INSERT INTO news_intelligence_signals
                (cluster_id, symbol, signal_type, sentiment_score, narrative_type,
                 severity_level, reliability_score, confidence_score,
                 should_affect_trading, shadow_only, suppression_reason,
                 spam_score, latency_score, is_valid_signal,
                 data_quality_status, market_confirmation_status,
                 sentiment_label, source_validation_passed,
                 market_validation_passed, validation_status, created_at)
            VALUES (?, 'BTCUSDT', 'NEWS_INTELLIGENCE', 0.35, 'MACRO_POLICY',
                    'MEDIUM', 0.82, 0.8, 0, 1, 'SHADOW_MODE_ENFORCED',
                    0.0, 0.95, 1, 'HIGH_CONFIDENCE', 'CONFIRMED',
                    'POSITIVE', 1, 1, 'VALIDATED', ?)
            """,
            (cluster, now),
        )
        conn.execute(
            """
            INSERT INTO news_market_reactions
                (cluster_id, symbol, event_reaction_id, sentiment_score,
                 sentiment_direction, actual_direction, sentiment_accuracy,
                 sentiment_accuracy_score, impact_score, max_price_move_pct,
                 volatility_expansion, volume_spike, reaction_type,
                 reaction_latency_category, signal_effectiveness_score,
                 is_false_signal, data_quality_score, reliability_score,
                 created_at, updated_at)
            VALUES (?, 'BTCUSDT', 1, 0.35, 'BULLISH', 'UP', 'CORRECT',
                    1.0, 0.8, 2.0, 2.8, 3.0, 'VOL_SPIKE',
                    'IMMEDIATE', 0.82, 0, 0.9, 0.82, ?, ?)
            """,
            (cluster, now, now),
        )
        conn.execute(
            """
            INSERT INTO narrative_effectiveness_scores
                (narrative_type, sample_count, avg_impact_score,
                 avg_price_move_pct, correct_sentiment_ratio,
                 false_signal_ratio, avg_effectiveness_score, last_updated)
            VALUES ('MACRO_POLICY', 1, 0.8, 2.0, 1.0, 0.0, 0.82, ?)
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO event_news_influence_decisions
                (trace_id, symbol, mode, requested_action, applied_action,
                 reason, confidence, reliability_score, fake_news_risk_score,
                 conflict_flag, market_confirmation_status, size_multiplier,
                 confidence_penalty, delay_seconds, source_context_json, created_at)
            VALUES ('trace-smoke', 'BTCUSDT', 'ADVISORY', 'ANNOTATE_ONLY',
                    'ANNOTATE_ONLY', 'Admin smoke annotation', 0.8, 0.82,
                    0.1, 0, 'CONFIRMED', 1.0, 0.0, 0, '{}', ?)
            """,
            (now,),
        )


def test_admin_event_endpoints_return_seeded_data(tmp_path: Path) -> None:
    client, db = _make_client(tmp_path)
    _seed_full_event_news_chain(db)

    assert client.get("/api/admin/events/upcoming").json()[0]["event_id"] == "evt-smoke-cpi"
    assert client.get("/api/admin/events/active-blackouts").json()[0]["is_global"] is True
    assert client.get("/api/admin/events/feed-status").json()["active_blackout_count"] == 1
    assert client.get("/api/admin/events/reactions/recent").json()[0]["reaction_type"] == "VOL_SPIKE"
    assert client.get("/api/admin/events/reactions/summary").json()["total_reactions"] == 1


def test_admin_news_endpoints_return_seeded_shadow_data(tmp_path: Path) -> None:
    client, db = _make_client(tmp_path)
    _seed_full_event_news_chain(db)

    assert client.get("/api/admin/news/feed-status").json()["shadow_only"] is True
    sources = client.get("/api/admin/news/sources").json()
    smoke_source = next(row for row in sources if row["id"] == "coindesk-smoke")
    assert smoke_source["health_status"] == "HEALTHY"
    assert client.get("/api/admin/news/items").json()[0]["title"] == "Bitcoin volatility around CPI"
    cluster = client.get("/api/admin/news/clusters").json()[0]
    assert cluster["narratives"][0]["narrative_type"] == "MACRO_POLICY"
    signal = client.get("/api/admin/news/signals").json()[0]
    assert signal["shadow_only"] == 1
    assert signal["should_affect_trading"] == 0
    assert client.get("/api/admin/news/validations").json()[0]["sentiment_accuracy"] == "CORRECT"
    assert client.get("/api/admin/news/validations/summary").json()["total_validations"] == 1
    assert client.get("/api/admin/news/narrative-effectiveness").json()[0]["narrative_type"] == "MACRO_POLICY"
    assert client.get(f"/api/admin/news/clusters/{cluster['id']}/validation").json()[0]["reaction_type"] == "VOL_SPIKE"
    mode = client.get("/api/admin/news/runtime-mode").json()
    assert mode["state"]["current_mode"] == "SHADOW"
    assert mode["max_allowed_action"] == "ANNOTATE_ONLY"
    assert mode["execution_impact"] is False
    influence = client.get("/api/admin/news/influence-decisions").json()
    assert influence[0]["trace_id"] == "trace-smoke"
    assert influence[0]["applied_action"] == "ANNOTATE_ONLY"
    summary = client.get("/api/admin/news/influence-summary").json()
    assert summary["summary"]["forbidden_action_count"] == 0
    assert summary["news_execution_allowed"] is False
