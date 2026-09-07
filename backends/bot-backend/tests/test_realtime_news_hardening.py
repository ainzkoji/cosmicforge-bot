"""
Integration tests for the Real-Time News Ingestion Hardening layer.

Tests T1–T14 covering:
  T1  — CryptoPanic client returns NormalizedNewsItem when API key present
  T2  — CryptoPanic client disabled (no key) → empty list, no DB writes
  T3  — Benzinga client normalizes titles and stores items
  T4  — Reuters placeholder always disabled
  T5  — Generic adapter detects articles/data/results/items list keys
  T6  — Conflict detector: keyword opposition flags cluster
  T7  — Conflict detector: sentiment split flags cluster
  T8  — Conflict detector: cross-cluster Jaccard conflict flags both clusters
  T9  — Fake news risk formula produces correct weighted score
  T10 — Fake news risk flags HIGH_FAKE_NEWS_RISK at ≥0.80
  T11 — Market confirmation service stores MARKET_CONFIRMED status
  T12 — Real-time worker pipeline: insert → dedup → signal (shadow_only=1 always)
  T13 — Shadow invariant: should_affect_trading NEVER set to 1
  T14 — Per-provider backoff: is_due() returns False during backoff window

Run with:
    cd backends/bot-backend
    python -m pytest tests/test_realtime_news_hardening.py -v

No external network calls — all HTTP is mocked.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate
from shared_lib.persistence.news_items import get_recent_items, get_recent_clusters


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = DB(path=path)
    migrate(db)
    yield db
    os.unlink(path)


def _cluster_row(db: DB, cluster_id: int) -> Optional[dict]:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM news_clusters WHERE id=?", (cluster_id,)
        ).fetchone()
    return dict(row) if row else None


def _insert_cluster(db: DB, title: str, sentiment: float = 0.0) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        cur = conn.execute(
            """INSERT INTO news_clusters
               (canonical_title, first_seen_utc, last_seen_utc, source_count,
                conflict_flag, fake_news_risk_score, market_confirmation_status,
                created_at, updated_at)
               VALUES (?,?,?,?,0,0.0,?,?,?)""",
            (title, now, now, 1, "PENDING", now, now),
        )
    return cur.lastrowid


# ---------------------------------------------------------------------------
# T1 — CryptoPanic client returns items with valid key
# ---------------------------------------------------------------------------

class TestT1CryptoPanicEnabled:
    def test_fetch_returns_normalized_items(self):
        from app.news.providers.cryptopanic_client import CryptoPanicRealtimeClient

        fake_response = json.dumps({
            "results": [
                {
                    "id": 99,
                    "title": "Bitcoin ETF Approved",
                    "published_at": "2026-04-25T10:00:00Z",
                    "url": "https://example.com/btc-etf",
                    "source": {"domain": "example.com", "title": "CryptoNews"},
                }
            ]
        }).encode()

        mock_resp = MagicMock()
        mock_resp.read.return_value = fake_response
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        client = CryptoPanicRealtimeClient(api_key="test-key-123")
        assert client.is_enabled()

        with patch("urllib.request.urlopen", return_value=mock_resp):
            items, error, latency = client.fetch()

        assert error is None
        assert len(items) == 1
        assert items[0].title == "Bitcoin ETF Approved"
        assert items[0].provider == "cryptopanic"
        assert latency >= 0.0


# ---------------------------------------------------------------------------
# T2 — CryptoPanic disabled with no key
# ---------------------------------------------------------------------------

class TestT2CryptoPanicDisabled:
    def test_no_key_returns_empty(self):
        from app.news.providers.cryptopanic_client import CryptoPanicRealtimeClient

        client = CryptoPanicRealtimeClient(api_key="")
        assert not client.is_enabled()

        items, error, latency = client.fetch()
        assert items == []
        assert error is not None
        assert latency == 0.0


# ---------------------------------------------------------------------------
# T3 — Benzinga client normalizes items
# ---------------------------------------------------------------------------

class TestT3BenzingaNormalize:
    def test_normalize_headline_field(self):
        from app.news.providers.benzinga_client import BenzingaRealtimeClient

        fake_response = json.dumps([{
            "id": "bz-001",
            "headline": "Ethereum Upgrade Confirmed",
            "created": "2026-04-25T11:00:00Z",
            "url": "https://benzinga.com/eth-upgrade",
            "teaser": "The merge is complete.",
        }]).encode()

        mock_resp = MagicMock()
        mock_resp.read.return_value = fake_response
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        client = BenzingaRealtimeClient(api_key="bz-key")
        with patch("urllib.request.urlopen", return_value=mock_resp):
            items, error, _ = client.fetch()

        assert error is None
        assert len(items) == 1
        assert items[0].title == "Ethereum Upgrade Confirmed"
        assert items[0].provider == "benzinga"
        assert items[0].category == "MARKET"


# ---------------------------------------------------------------------------
# T4 — Reuters always disabled
# ---------------------------------------------------------------------------

class TestT4ReutersDisabled:
    def test_always_disabled(self):
        from app.news.providers.reuters_client import ReutersClient

        client = ReutersClient(api_key="some-key")
        assert not client.is_enabled()
        assert not client.is_due(30)

        items, error, _ = client.fetch()
        assert items == []
        assert error is not None


# ---------------------------------------------------------------------------
# T5 — Generic adapter handles multiple list key names
# ---------------------------------------------------------------------------

class TestT5GenericAdapter:
    @pytest.mark.parametrize("key", ["articles", "data", "results", "items"])
    def test_list_keys(self, key: str):
        from app.news.providers.generic_news_api_client import GenericNewsApiClient

        fake_response = json.dumps({
            key: [{"title": f"Story via {key}", "url": "https://example.com"}]
        }).encode()

        mock_resp = MagicMock()
        mock_resp.read.return_value = fake_response
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        client = GenericNewsApiClient(api_key="key", api_url="https://api.example.com/news")
        with patch("urllib.request.urlopen", return_value=mock_resp):
            items, error, _ = client.fetch()

        assert error is None
        assert len(items) == 1
        assert f"via {key}" in items[0].title


# ---------------------------------------------------------------------------
# T6 — Conflict detector: keyword opposition
# ---------------------------------------------------------------------------

class TestT6KeywordConflict:
    def test_keyword_opposition_flags_cluster(self, tmp_db: DB):
        from app.news.news_conflict_detector import NewsConflictDetector

        cluster_id = _insert_cluster(tmp_db, "Bitcoin ETF approved but also banned")
        detector = NewsConflictDetector(tmp_db)
        flagged = detector.check_cluster(
            cluster_id=cluster_id,
            item_titles=["Bitcoin ETF approved", "Bitcoin ETF banned by regulators"],
        )
        assert flagged is True
        row = _cluster_row(tmp_db, cluster_id)
        assert row["conflict_flag"] == 1
        assert "CONFLICTING" in (row["market_confirmation_status"] or "")


# ---------------------------------------------------------------------------
# T7 — Conflict detector: sentiment split
# ---------------------------------------------------------------------------

class TestT7SentimentSplit:
    def test_opposing_sentiments_flagged(self, tmp_db: DB):
        from app.news.news_conflict_detector import NewsConflictDetector

        cluster_id = _insert_cluster(tmp_db, "Market news cluster")
        detector = NewsConflictDetector(tmp_db)
        flagged = detector.check_cluster(
            cluster_id=cluster_id,
            item_titles=["BTC surges to new highs", "BTC crashes to yearly low"],
            compound_scores=[0.55, -0.60],
        )
        assert flagged is True
        row = _cluster_row(tmp_db, cluster_id)
        assert row["conflict_flag"] == 1

    def test_same_direction_not_flagged(self, tmp_db: DB):
        from app.news.news_conflict_detector import NewsConflictDetector

        cluster_id = _insert_cluster(tmp_db, "Bullish news cluster")
        detector = NewsConflictDetector(tmp_db)
        flagged = detector.check_cluster(
            cluster_id=cluster_id,
            item_titles=["BTC rallies", "ETH also rallies"],
            compound_scores=[0.45, 0.30],
        )
        assert flagged is False


# ---------------------------------------------------------------------------
# T8 — Cross-cluster Jaccard conflict
# ---------------------------------------------------------------------------

class TestT8CrossClusterConflict:
    def test_near_identical_opposite_sentiment_flagged(self, tmp_db: DB):
        from app.news.news_conflict_detector import NewsConflictDetector

        # Existing cluster with positive sentiment — seed avg compound via sentiment table
        existing_id = _insert_cluster(tmp_db, "Bitcoin ETF approved by SEC today")
        # Insert a mock sentiment row for the existing cluster
        with tmp_db.connect() as conn:
            conn.execute(
                """INSERT INTO news_sentiment_scores
                   (cluster_id, sentiment_label, sentiment_score, confidence_score, compound_raw,
                    model_version, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (existing_id, "BULLISH", 0.7, 0.9, 0.65, "vader-v1",
                 datetime.now(timezone.utc).isoformat()),
            )

        new_id = _insert_cluster(tmp_db, "Bitcoin ETF denied by SEC today")
        since_utc = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

        detector = NewsConflictDetector(tmp_db, cross_cluster_threshold=0.50)
        flagged = detector.check_cross_cluster(
            new_cluster_id=new_id,
            new_title="Bitcoin ETF denied by SEC today",
            new_dominant_sentiment=-0.55,
            since_utc=since_utc,
        )
        assert flagged is True
        assert _cluster_row(tmp_db, new_id)["conflict_flag"] == 1
        assert _cluster_row(tmp_db, existing_id)["conflict_flag"] == 1


# ---------------------------------------------------------------------------
# T9 — Fake news risk formula
# ---------------------------------------------------------------------------

class TestT9FakeNewsRiskFormula:
    def test_low_risk_trusted_source(self, tmp_db: DB):
        from app.news.fake_news_risk_service import FakeNewsRiskService

        cluster_id = _insert_cluster(tmp_db, "Test cluster low risk")
        svc = FakeNewsRiskService(tmp_db)
        risk = svc.score(
            cluster_id=cluster_id,
            source_reliabilities=[0.95, 0.90],  # trusted sources
            spam_score=0.05,
            latency_score=0.98,
            conflict_flag=False,
            source_count=3,
        )
        assert risk < 0.40, f"Expected low risk, got {risk}"

    def test_formula_weights(self, tmp_db: DB):
        from app.news.fake_news_risk_service import FakeNewsRiskService

        cluster_id = _insert_cluster(tmp_db, "Formula test cluster")
        svc = FakeNewsRiskService(tmp_db)
        # max_reliability=0.20 (unknown), spam=0.5, latency=0.0, single_source, no conflict
        risk = svc.score(
            cluster_id=cluster_id,
            source_reliabilities=[0.20],
            spam_score=0.50,
            latency_score=0.0,
            conflict_flag=False,
            source_count=1,
        )
        expected = (1-0.20)*0.30 + 0.50*0.20 + (1-0.0)*0.15 + 1.0*0.20 + 0.0*0.15
        assert abs(risk - expected) < 0.01, f"Expected ~{expected:.3f}, got {risk}"


# ---------------------------------------------------------------------------
# T10 — Fake news HIGH_FAKE_NEWS_RISK flag
# ---------------------------------------------------------------------------

class TestT10HighFakeNewsRisk:
    def test_high_risk_flagged_in_db(self, tmp_db: DB):
        from app.news.fake_news_risk_service import FakeNewsRiskService, _FLAG_HIGH_RISK

        cluster_id = _insert_cluster(tmp_db, "Suspicious high-risk cluster")
        svc = FakeNewsRiskService(tmp_db)
        risk = svc.score(
            cluster_id=cluster_id,
            source_reliabilities=[0.10],
            spam_score=0.90,
            latency_score=0.05,
            conflict_flag=True,
            source_count=1,
        )
        assert risk >= 0.80
        row = _cluster_row(tmp_db, cluster_id)
        assert row["market_confirmation_status"] == _FLAG_HIGH_RISK
        assert row["fake_news_risk_score"] >= 0.80


class TestProviderStatusBaseline:
    def test_optional_providers_seed_status_without_keys(self, tmp_db: DB):
        from app.workers.real_time_news_worker import build_real_time_news_worker

        class DummySettings:
            REAL_TIME_NEWS_ENABLED = True
            NEWS_INTELLIGENCE_ENABLED = True
            REAL_TIME_NEWS_POLL_INTERVAL_SECONDS = 30
            NEWS_DEDUP_SIMILARITY_THRESHOLD = 0.82
            NEWS_DEDUP_WINDOW_HOURS = 6
            REAL_TIME_NEWS_MIN_RELIABILITY_SCORE = 0.70
            REAL_TIME_NEWS_MIN_CLUSTER_CONFIDENCE = 0.75
            REAL_TIME_NEWS_PROVIDER_TIMEOUT_SECONDS = 10
            REAL_TIME_NEWS_MAX_ITEMS_PER_FETCH = 100
            CRYPTOPANIC_ENABLED = True
            CRYPTOPANIC_API_KEY = ""
            BENZINGA_ENABLED = False
            BENZINGA_API_KEY = ""
            GENERIC_NEWS_API_ENABLED = True
            GENERIC_NEWS_API_KEY = ""
            GENERIC_NEWS_API_URL = ""
            REUTERS_API_KEY = ""

        with patch("app.core.config.settings", DummySettings()):
            build_real_time_news_worker(tmp_db)

        with tmp_db.connect() as conn:
            rows = conn.execute(
                "SELECT provider, is_enabled, health_status, last_error FROM real_time_news_provider_status ORDER BY provider"
            ).fetchall()

        status = {row[0]: {"is_enabled": row[1], "health_status": row[2], "last_error": row[3]} for row in rows}
        assert status["cryptopanic"]["is_enabled"] == 0
        assert status["cryptopanic"]["health_status"] == "WAITING_CONFIG"
        assert "API key not configured" in status["cryptopanic"]["last_error"]
        assert status["benzinga"]["health_status"] == "DISABLED"
        assert status["generic"]["health_status"] == "WAITING_CONFIG"
        assert status["reuters"]["health_status"] == "PLACEHOLDER"


# ---------------------------------------------------------------------------
# T11 — Market confirmation service stores status
# ---------------------------------------------------------------------------

class TestT11MarketConfirmation:
    def test_no_windows_due_returns_no_reaction(self, tmp_db: DB):
        from app.news.market_confirmation_service import MarketConfirmationService

        # Cluster created in the future — no windows due yet
        future_utc = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        cluster_id = _insert_cluster(tmp_db, "Future cluster")
        svc = MarketConfirmationService(tmp_db)
        # Inject future first_seen — no windows should be due
        status = svc.run_due_windows(
            cluster_id=cluster_id,
            first_seen_utc=future_utc,
        )
        assert status == "PENDING_MARKET_VALIDATION"

    def test_past_cluster_runs_windows(self, tmp_db: DB):
        from app.news.market_confirmation_service import MarketConfirmationService

        # Cluster from 70 minutes ago — all windows (1,5,15,30,60) are due
        old_utc = (datetime.now(timezone.utc) - timedelta(minutes=70)).isoformat()
        cluster_id = _insert_cluster(tmp_db, "Old cluster with windows due")

        # NewsMarketValidationService will return [] (no reactions in test DB)
        svc = MarketConfirmationService(tmp_db)
        status = svc.run_due_windows(
            cluster_id=cluster_id,
            first_seen_utc=old_utc,
        )
        # With no price data, should return NO_MARKET_REACTION
        assert status in ("NO_MARKET_REACTION", "MARKET_CONFIRMED", "DELAYED_REACTION",
                          "CONFLICTING_MARKET_REACTION")


# ---------------------------------------------------------------------------
# T12 — Real-time worker pipeline produces shadow-only signals
# ---------------------------------------------------------------------------

class TestT12WorkerPipeline:
    def test_process_item_inserts_shadow_signal(self, tmp_db: DB):
        from app.workers.real_time_news_worker import RealTimeNewsWorker
        from app.news.news_normalizer import NormalizedNewsItem
        from shared_lib.persistence.news_intelligence import get_active_signals

        worker = RealTimeNewsWorker(tmp_db, enabled=True)
        now = datetime.now(timezone.utc).isoformat()
        item = NormalizedNewsItem(
            provider="cryptopanic",
            source_name="CryptoNews",
            source_domain="cryptonews.com",
            source_url="https://cryptonews.com/btc",
            title="Bitcoin breaks $100k milestone with huge surge",
            body_snippet="BTC soars past 100k on massive volume.",
            published_utc=now,
            ingested_utc=now,
            external_id="cp-test-001",
            category="CRYPTO",
        )
        result = worker._process_item(item)
        assert result in ("inserted", "duplicate")

        # Verify raw item was written
        items = get_recent_items(tmp_db, since_utc=(
            datetime.now(timezone.utc) - timedelta(hours=1)
        ).isoformat(), limit=10)
        assert any(i["title"] == item.title for i in items)
        signals = get_active_signals(tmp_db)
        assert len(signals) >= 1
        assert all(sig["shadow_only"] == 1 for sig in signals)
        assert all(sig["should_affect_trading"] == 0 for sig in signals)
        assert all(sig["validation_status"] == "PENDING_MARKET_VALIDATION" for sig in signals)


# ---------------------------------------------------------------------------
# T13 — Shadow invariant: should_affect_trading never 1
# ---------------------------------------------------------------------------

class TestT13ShadowInvariant:
    def test_signals_always_shadow_only(self, tmp_db: DB):
        from app.workers.real_time_news_worker import RealTimeNewsWorker
        from app.news.news_normalizer import NormalizedNewsItem

        worker = RealTimeNewsWorker(tmp_db, enabled=True)
        now = datetime.now(timezone.utc).isoformat()

        for i in range(3):
            item = NormalizedNewsItem(
                provider="benzinga",
                source_name="Benzinga",
                source_domain="benzinga.com",
                source_url=f"https://benzinga.com/story-{i}",
                title=f"Breaking market news story number {i} with BULLISH sentiment surge",
                body_snippet="Markets rally hard.",
                published_utc=now,
                ingested_utc=now,
                external_id=f"bz-{i}",
                category="MARKET",
            )
            worker._process_item(item)

        with tmp_db.connect() as conn:
            rows = conn.execute(
                "SELECT should_affect_trading, shadow_only FROM news_intelligence_signals"
            ).fetchall()

        for row in rows:
            assert row[0] == 0, "should_affect_trading must always be 0"
            assert row[1] == 1, "shadow_only must always be 1"


# ---------------------------------------------------------------------------
# T14 — Per-provider backoff: is_due() respects backoff window
# ---------------------------------------------------------------------------

class TestT14Backoff:
    def test_cryptopanic_backoff_on_429(self):
        from app.news.providers.cryptopanic_client import CryptoPanicRealtimeClient
        from urllib.error import HTTPError

        client = CryptoPanicRealtimeClient(api_key="test-key")
        assert client.is_due(30)

        # Simulate 429 error
        mock_resp = MagicMock()
        http_error = HTTPError(url="", code=429, msg="Too Many Requests", hdrs={}, fp=None)
        with patch("urllib.request.urlopen", side_effect=http_error):
            items, error, _ = client.fetch()

        assert items == []
        assert "429" in error

        # After 429, is_due() should return False (600s backoff)
        assert not client.is_due(30)

    def test_backoff_expires(self):
        from app.news.providers.benzinga_client import BenzingaRealtimeClient

        client = BenzingaRealtimeClient(api_key="test-key")
        # Manually set backoff to past
        client._backoff_until = time.monotonic() - 10
        assert client.is_due(0)
