"""
Local/dev-only smoke seeder for Event/News Intelligence admin screens.

This inserts deterministic demo rows into event/news intelligence tables so the
admin UI and admin APIs can be verified when live providers are not connected.
It does not touch execution state and does not enable news trading.

Usage:
  python scripts/dev_seed_event_news_intelligence.py --db-path path/to/dev.db --yes
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))


def iso(minutes: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


def seed(db_path: Path) -> None:
    from shared_lib.persistence.migrations import migrate

    db_path.parent.mkdir(parents=True, exist_ok=True)
    migrate(str(db_path))
    now = iso()
    future = iso(15)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        event = conn.execute(
            """
            INSERT INTO economic_events
                (event_id, title, event_type, country_currency, impact_level,
                 scheduled_utc, source, created_at, updated_at)
            VALUES ('dev-smoke-cpi', 'Dev Smoke CPI Event', 'CPI', 'USD',
                    'HIGH', ?, 'dev_seed', ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                title=excluded.title,
                scheduled_utc=excluded.scheduled_utc,
                updated_at=excluded.updated_at
            """,
            (future, now, now),
        )
        row = conn.execute("SELECT id FROM economic_events WHERE event_id='dev-smoke-cpi'").fetchone()
        event_id = row["id"] if row else event.lastrowid
        conn.execute(
            """
            INSERT OR REPLACE INTO event_blackout_windows
                (event_id, start_utc, end_utc, affected_symbols, is_global,
                 is_active, reason, created_at)
            VALUES (?, ?, ?, NULL, 1, 1, 'HIGH impact macro event', ?)
            """,
            (event_id, iso(-5), iso(25), now),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO market_event_reactions
                (event_id, symbol, exchange, event_time_utc,
                 price_before_event, price_at_event, price_after_5m,
                 max_move_pct, min_move_pct, net_move_pct,
                 atr_before, atr_after, volatility_expansion_ratio,
                 average_volume_before, event_volume, volume_spike_ratio,
                 reaction_type, confidence_score, data_quality, created_at, updated_at)
            VALUES ('dev-smoke-cpi', 'BTCUSDT', 'BINANCE', ?,
                    100.0, 101.0, 102.0, 2.0, -0.5, 1.5,
                    1.0, 2.8, 2.8, 1000.0, 3000.0, 3.0,
                    'VOL_SPIKE', 0.9, 'COMPLETE', ?, ?)
            """,
            (now, now, now),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO news_sources
                (id, source_name, base_reliability_score, dynamic_reliability_score,
                 is_trusted, is_blocked, created_at, updated_at, source_type,
                 category, is_enabled, rss_url, last_fetch_utc, last_success_utc)
            VALUES ('dev-smoke-rss', 'Dev Smoke RSS', 0.8, 0.82,
                    1, 0, ?, ?, 'RSS', 'CRYPTO', 1,
                    'https://example.test/rss', ?, ?)
            """,
            (now, now, now, now),
        )
        raw_id = conn.execute(
            """
            INSERT INTO raw_news_items
                (provider, source_name, source_domain, source_url, external_id,
                 title, body_snippet, published_utc, ingested_utc,
                 latency_seconds, language, is_duplicate, created_at)
            VALUES ('rss', 'Dev Smoke RSS', 'example.test', 'https://example.test/news',
                    ?, 'Bitcoin volatility around CPI',
                    'Dev-only seeded news item', ?, ?, 2.0, 'en', 0, ?)
            """,
            (f"dev-smoke-{now}", now, now, now),
        ).lastrowid
        cluster_id = conn.execute(
            """
            INSERT INTO news_clusters
                (canonical_title, summary, first_seen_utc, last_seen_utc,
                 source_count, provider_count, highest_reliability_score,
                 cluster_confidence, is_manipulation_suspect, created_at, updated_at,
                 spam_score, latency_score, is_valid_signal, data_quality_status,
                 first_seen_provider, confirmation_count, conflict_flag,
                 fake_news_risk_score, market_confirmation_status)
            VALUES ('Bitcoin volatility around CPI', 'Dev-only seeded cluster',
                    ?, ?, 1, 1, 0.82, 0.88, 0, ?, ?,
                    0.0, 0.95, 1, 'HIGH_CONFIDENCE',
                    'rss', 1, 0, 0.1, 'CONFIRMED')
            """,
            (now, now, now, now),
        ).lastrowid
        conn.execute(
            "INSERT INTO news_cluster_items (cluster_id, raw_news_item_id, similarity_score, created_at) VALUES (?, ?, 1.0, ?)",
            (cluster_id, raw_id, now),
        )
        conn.execute(
            "INSERT INTO news_asset_mappings (cluster_id, symbol, asset, mapping_reason, mapping_confidence, created_at) VALUES (?, 'BTCUSDT', 'crypto', 'dev_seed', 1.0, ?)",
            (cluster_id, now),
        )
        conn.execute(
            "INSERT INTO news_sentiment_scores (cluster_id, sentiment_score, sentiment_label, confidence_score, model_version, created_at) VALUES (?, 0.35, 'POSITIVE', 0.8, 'dev-seed-1.0', ?)",
            (cluster_id, now),
        )
        conn.execute(
            "INSERT INTO news_narratives (cluster_id, narrative_type, narrative_confidence, severity_level, matched_keywords, created_at) VALUES (?, 'MACRO_POLICY', 0.8, 'MEDIUM', 'cpi', ?)",
            (cluster_id, now),
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
            (cluster_id, now),
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
            (cluster_id, now, now),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO narrative_effectiveness_scores
                (narrative_type, sample_count, avg_impact_score,
                 avg_price_move_pct, correct_sentiment_ratio,
                 false_signal_ratio, avg_effectiveness_score, last_updated)
            VALUES ('MACRO_POLICY', 1, 0.8, 2.0, 1.0, 0.0, 0.82, ?)
            """,
            (now,),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True, help="Development SQLite DB path to seed.")
    parser.add_argument("--yes", action="store_true", help="Required confirmation for local/dev seeding.")
    args = parser.parse_args()
    if not args.yes:
        raise SystemExit("Refusing to seed without --yes. This script is local/dev only.")
    seed(Path(args.db_path).resolve())
    print(f"Seeded local Event/News Intelligence smoke data into {Path(args.db_path).resolve()}")


if __name__ == "__main__":
    main()
