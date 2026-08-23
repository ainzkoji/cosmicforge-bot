"""
Integration tests for the RSS / real news source connection layer.

Run with:
    cd backends/bot-backend
    python -m pytest tests/test_rss_ingestion.py -v

No external network calls — all HTTP is mocked.
No API keys required.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate
from shared_lib.persistence.news_items import (
    get_recent_items,
    get_recent_clusters,
)


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


_SAMPLE_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>CoinDesk Test</title>
    <item>
      <title>Bitcoin &amp; ETH rally as ETF inflows surge</title>
      <link>https://coindesk.com/story/btc-eth-rally-001</link>
      <description>Bitcoin surged past $90k as spot ETF inflows hit a record.</description>
      <pubDate>Mon, 01 Jan 2024 12:00:00 +0000</pubDate>
    </item>
    <item>
      <title>Ethereum upgrade scheduled for Q2</title>
      <link>https://coindesk.com/story/eth-upgrade-002</link>
      <description>The next Ethereum hard fork is set for Q2 2024.</description>
      <pubDate>Mon, 01 Jan 2024 13:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>"""

_BAD_XML = b"<<< this is not xml"


def _make_urlopen_mock(xml_bytes: bytes):
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=ctx)
    ctx.__exit__ = MagicMock(return_value=False)
    ctx.read = MagicMock(return_value=xml_bytes)
    return ctx


# ---------------------------------------------------------------------------
# T1 — RSS item is fetched and parsed
# ---------------------------------------------------------------------------

class TestRSSFetch:
    def test_fetch_returns_items(self):
        from app.news.rss_provider_client import RSSProviderClient

        client = RSSProviderClient(
            source_id="coindesk.com",
            source_name="CoinDesk",
            rss_url="https://www.coindesk.com/arc/outboundfeeds/rss/",
        )
        mock_ctx = _make_urlopen_mock(_SAMPLE_RSS)

        with patch("app.news.rss_provider_client.urllib_request.urlopen", return_value=mock_ctx):
            items, error = client.fetch()

        assert error is None
        assert len(items) == 2
        assert items[0].title == "Bitcoin & ETH rally as ETF inflows surge"

    def test_no_api_key_required(self):
        """RSS mode must work without any API key configured."""
        from app.news.rss_provider_client import RSSProviderClient
        # Instantiate with no API key whatsoever — should not raise
        client = RSSProviderClient(
            source_id="coindesk.com",
            source_name="CoinDesk",
            rss_url="https://www.coindesk.com/arc/outboundfeeds/rss/",
        )
        assert client is not None


# ---------------------------------------------------------------------------
# T2 — Item normalized correctly
# ---------------------------------------------------------------------------

class TestNormalization:
    def test_title_html_decoded(self):
        from app.news.news_normalizer import normalize_rss_entry
        entry = {
            "title": "Bitcoin &amp; ETH &lt;rally&gt;",
            "link": "https://coindesk.com/story/001",
            "pubDate": "Mon, 01 Jan 2024 12:00:00 +0000",
        }
        item = normalize_rss_entry(entry, source_name="CoinDesk", source_domain="coindesk.com")
        assert item is not None
        assert item.title == "Bitcoin & ETH <rally>"

    def test_utc_timestamp_normalized(self):
        from app.news.news_normalizer import normalize_rss_entry
        entry = {
            "title": "Test article",
            "link": "https://coindesk.com/story/002",
            "pubDate": "Mon, 15 Apr 2024 10:30:00 +0200",
        }
        item = normalize_rss_entry(entry, source_name="CoinDesk", source_domain="coindesk.com")
        assert item is not None
        assert "08:30:00" in item.published_utc  # +0200 → UTC
        assert "+00:00" in item.published_utc or "Z" in item.published_utc or "08:30" in item.published_utc

    def test_domain_extracted_from_link(self):
        from app.news.news_normalizer import normalize_rss_entry
        entry = {
            "title": "Story",
            "link": "https://www.cointelegraph.com/news/some-story",
            "pubDate": "Mon, 01 Jan 2024 12:00:00 +0000",
        }
        item = normalize_rss_entry(entry, source_name="CT", source_domain="cointelegraph.com")
        assert item is not None
        assert item.source_domain == "cointelegraph.com"

    def test_missing_title_returns_none(self):
        from app.news.news_normalizer import normalize_rss_entry
        entry = {"link": "https://coindesk.com/story/003", "pubDate": "Mon, 01 Jan 2024 12:00:00 +0000"}
        item = normalize_rss_entry(entry, source_name="CoinDesk", source_domain="coindesk.com")
        assert item is None


# ---------------------------------------------------------------------------
# T3 — Duplicate RSS item not inserted twice
# ---------------------------------------------------------------------------

class TestDuplication:
    def test_same_url_not_inserted_twice(self, tmp_db):
        from app.news.rss_provider_client import RSSProviderClient
        from app.workers.news_ingestion_worker import NewsIngestionWorker
        from app.news.news_source_registry import NewsSourceRegistry
        from app.news.news_provider_health import ProviderHealthService

        client = RSSProviderClient(
            source_id="coindesk.com",
            source_name="CoinDesk",
            rss_url="https://fake/rss",
        )
        mock_ctx = _make_urlopen_mock(_SAMPLE_RSS)

        registry = MagicMock(spec=NewsSourceRegistry)
        registry.get_due_rss_clients.return_value = [client]
        registry.all_rss_clients.return_value = [client]
        registry.mark_fetched = MagicMock()

        health_svc = MagicMock(spec=ProviderHealthService)

        worker = NewsIngestionWorker(
            db=tmp_db, registry=registry, health_svc=health_svc, enabled=True
        )

        with patch("app.news.rss_provider_client.urllib_request.urlopen", return_value=mock_ctx):
            client._last_fetch_ts = 0.0
            worker._poll_source(client)

        # Second poll — same feed, same URLs
        mock_ctx2 = _make_urlopen_mock(_SAMPLE_RSS)
        with patch("app.news.rss_provider_client.urllib_request.urlopen", return_value=mock_ctx2):
            client._last_fetch_ts = 0.0
            worker._poll_source(client)

        rows = get_recent_items(tmp_db, since_utc="2000-01-01T00:00:00+00:00", limit=100)
        urls = [r["source_url"] for r in rows]
        assert len(set(urls)) == len(urls), "Duplicate URLs found in raw_news_items"

    def test_same_rss_tick_does_not_duplicate_narratives(self, tmp_db):
        from app.news.rss_provider_client import RSSProviderClient
        from app.workers.news_ingestion_worker import NewsIngestionWorker
        from app.news.news_source_registry import NewsSourceRegistry
        from app.news.news_provider_health import ProviderHealthService

        client = RSSProviderClient(
            source_id="coindesk.com",
            source_name="CoinDesk",
            rss_url="https://fake/rss",
        )
        registry = MagicMock(spec=NewsSourceRegistry)
        registry.get_due_rss_clients.return_value = [client]
        registry.all_rss_clients.return_value = [client]
        registry.mark_fetched = MagicMock()
        health_svc = MagicMock(spec=ProviderHealthService)

        worker = NewsIngestionWorker(
            db=tmp_db, registry=registry, health_svc=health_svc, enabled=True
        )

        with patch("app.news.rss_provider_client.urllib_request.urlopen", return_value=_make_urlopen_mock(_SAMPLE_RSS)):
            client._last_fetch_ts = 0.0
            worker._poll_source(client)
        with patch("app.news.rss_provider_client.urllib_request.urlopen", return_value=_make_urlopen_mock(_SAMPLE_RSS)):
            client._last_fetch_ts = 0.0
            worker._poll_source(client)

        with tmp_db.connect() as conn:
            duplicate_rows = conn.execute(
                """
                SELECT cluster_id, narrative_type, COUNT(*) AS cnt
                FROM news_narratives
                GROUP BY cluster_id, narrative_type
                HAVING COUNT(*) > 1
                """
            ).fetchall()

        assert duplicate_rows == []


# ---------------------------------------------------------------------------
# T4 — Bad XML feed does not crash worker
# ---------------------------------------------------------------------------

class TestBadFeed:
    def test_malformed_xml_handled_gracefully(self, tmp_db):
        from app.news.rss_provider_client import RSSProviderClient
        from app.news.news_source_registry import NewsSourceRegistry
        from app.news.news_provider_health import ProviderHealthService
        from app.workers.news_ingestion_worker import NewsIngestionWorker

        client = RSSProviderClient(
            source_id="coindesk.com", source_name="CoinDesk", rss_url="https://fake/rss"
        )
        mock_ctx = _make_urlopen_mock(_BAD_XML)
        registry = MagicMock(spec=NewsSourceRegistry)
        registry.mark_fetched = MagicMock()
        health_svc = MagicMock(spec=ProviderHealthService)

        worker = NewsIngestionWorker(
            db=tmp_db, registry=registry, health_svc=health_svc, enabled=True
        )
        with patch("app.news.rss_provider_client.urllib_request.urlopen", return_value=mock_ctx):
            client._last_fetch_ts = 0.0
            # Must not raise
            worker._poll_source(client)

        health_svc.record.assert_called_once()
        call_kwargs = health_svc.record.call_args[1]
        assert call_kwargs["error_message"] is not None

    def test_narrative_error_does_not_crash_worker(self, tmp_db):
        from app.news.rss_provider_client import RSSProviderClient
        from app.news.news_source_registry import NewsSourceRegistry
        from app.news.news_provider_health import ProviderHealthService
        from app.workers.news_ingestion_worker import NewsIngestionWorker

        client = RSSProviderClient(
            source_id="coindesk.com", source_name="CoinDesk", rss_url="https://fake/rss"
        )
        registry = MagicMock(spec=NewsSourceRegistry)
        registry.mark_fetched = MagicMock()
        health_svc = MagicMock(spec=ProviderHealthService)

        worker = NewsIngestionWorker(
            db=tmp_db, registry=registry, health_svc=health_svc, enabled=True
        )
        with patch("app.news.rss_provider_client.urllib_request.urlopen", return_value=_make_urlopen_mock(_SAMPLE_RSS)):
            with patch.object(worker._narrative_clf, "classify_and_store", side_effect=RuntimeError("boom")):
                client._last_fetch_ts = 0.0
                worker._poll_source(client)

        rows = get_recent_items(tmp_db, since_utc="2000-01-01T00:00:00+00:00", limit=100)
        assert rows
        health_svc.record.assert_called_once()


# ---------------------------------------------------------------------------
# T5 — Network timeout handled gracefully → DEGRADED health
# ---------------------------------------------------------------------------

class TestNetworkTimeout:
    def test_timeout_marks_degraded(self, tmp_db):
        from urllib.error import URLError
        from app.news.rss_provider_client import RSSProviderClient
        from app.news.news_provider_health import ProviderHealthService
        from app.news.news_source_registry import NewsSourceRegistry
        from app.workers.news_ingestion_worker import NewsIngestionWorker

        client = RSSProviderClient(
            source_id="coindesk.com", source_name="CoinDesk", rss_url="https://fake/rss"
        )
        registry = MagicMock(spec=NewsSourceRegistry)
        registry.mark_fetched = MagicMock()
        health_svc = MagicMock(spec=ProviderHealthService)

        worker = NewsIngestionWorker(
            db=tmp_db, registry=registry, health_svc=health_svc, enabled=True
        )
        with patch("app.news.rss_provider_client.urllib_request.urlopen", side_effect=URLError("timed out")):
            client._last_fetch_ts = 0.0
            worker._poll_source(client)

        call_kwargs = health_svc.record.call_args[1]
        assert call_kwargs["items_fetched"] == 0
        assert call_kwargs["error_message"] is not None


# ---------------------------------------------------------------------------
# T6 — Health row created after each poll
# ---------------------------------------------------------------------------

class TestHealthTracking:
    def test_health_row_recorded(self, tmp_db):
        from app.news.news_provider_health import ProviderHealthService

        svc = ProviderHealthService(tmp_db)
        now = datetime.now(timezone.utc).isoformat()

        status = svc.record(
            source_id="coindesk.com",
            items_fetched=12,
            duplicate_count=2,
            error_message=None,
            last_success_utc=now,
        )
        assert status == "HEALTHY"

        row = svc.get_latest("coindesk.com")
        assert row is not None
        assert row["status"] == "HEALTHY"
        assert row["items_fetched_last_run"] == 12


class TestLatencyTracking:
    def test_latency_seconds_persisted_on_raw_items(self, tmp_db):
        from app.news.rss_provider_client import RSSProviderClient
        from app.workers.news_ingestion_worker import NewsIngestionWorker
        from app.news.news_source_registry import NewsSourceRegistry
        from app.news.news_provider_health import ProviderHealthService

        client = RSSProviderClient(
            source_id="coindesk.com",
            source_name="CoinDesk",
            rss_url="https://fake/rss",
        )
        registry = MagicMock(spec=NewsSourceRegistry)
        registry.mark_fetched = MagicMock()
        health_svc = MagicMock(spec=ProviderHealthService)
        worker = NewsIngestionWorker(
            db=tmp_db, registry=registry, health_svc=health_svc, enabled=True
        )

        mock_ctx = _make_urlopen_mock(_SAMPLE_RSS)
        with patch("app.news.rss_provider_client.urllib_request.urlopen", return_value=mock_ctx):
            client._last_fetch_ts = 0.0
            worker._poll_source(client)

        rows = get_recent_items(tmp_db, since_utc="2000-01-01T00:00:00+00:00", limit=10)
        assert rows
        assert rows[0]["latency_seconds"] is not None
        assert rows[0]["latency_seconds"] >= 0


# ---------------------------------------------------------------------------
# T7 — Manual import creates raw_news_items row
# ---------------------------------------------------------------------------

class TestManualImport:
    def test_import_creates_raw_item(self, tmp_db):
        from app.news.manual_news_import_service import ManualNewsImportService

        svc = ManualNewsImportService(tmp_db)
        result = svc.import_item(
            title="SEC approves Bitcoin ETF applications",
            source_name="Test Source",
            source_url="https://example.com/story/1",
            published_utc_raw="2024-01-15T10:00:00Z",
            body_snippet="The SEC has approved multiple Bitcoin ETF applications.",
            affected_symbols=["BTCUSDT"],
        )

        assert "error" not in result or result.get("error") is None
        assert result["raw_news_item_id"] is not None

        rows = get_recent_items(tmp_db, since_utc="2000-01-01T00:00:00+00:00", limit=10)
        assert any(r["title"] == "SEC approves Bitcoin ETF applications" for r in rows)

    def test_manual_import_does_not_mark_live_feed_active(self, tmp_db):
        from app.news.manual_news_import_service import ManualNewsImportService
        from app.news.news_provider_health import ProviderHealthService

        svc = ManualNewsImportService(tmp_db)
        svc.import_item(
            title="Manual shadow item",
            source_name="Manual",
            source_url="https://example.com/manual-story",
            published_utc_raw="2024-01-15T10:00:00Z",
            body_snippet="Manual item should not count as real-time live ingestion.",
            affected_symbols=["BTCUSDT"],
        )

        summary = ProviderHealthService(tmp_db).get_feed_summary()
        assert summary["today_count"] == 0
        assert summary["has_live_data"] is False


# ---------------------------------------------------------------------------
# T8 — Manual import triggers cluster creation
# ---------------------------------------------------------------------------

    def test_import_creates_cluster(self, tmp_db):
        from app.news.manual_news_import_service import ManualNewsImportService

        svc = ManualNewsImportService(tmp_db)
        result = svc.import_item(
            title="Ethereum hard fork delayed by developers",
            source_name="CryptoTest",
            source_url="https://example.com/story/2",
            published_utc_raw="2024-01-16T09:00:00Z",
        )

        assert result.get("cluster_id") is not None
        clusters = get_recent_clusters(tmp_db, since_utc="2000-01-01T00:00:00+00:00", limit=10)
        assert len(clusters) >= 1


# ---------------------------------------------------------------------------
# T9 — NEWS_REAL_SOURCE_INGESTION_ENABLED=False → no HTTP calls
# ---------------------------------------------------------------------------

class TestDisabledMode:
    def test_no_http_when_disabled(self, tmp_db):
        from app.workers.news_ingestion_worker import build_news_ingestion_worker

        with patch("app.core.config.settings") as mock_settings:
            mock_settings.NEWS_INTELLIGENCE_ENABLED = False
            mock_settings.NEWS_REAL_SOURCE_INGESTION_ENABLED = False
            mock_settings.NEWS_RSS_INGESTION_ENABLED = True
            mock_settings.NEWS_RSS_TIMEOUT_SECONDS = 15
            mock_settings.NEWS_RSS_MAX_ITEMS_PER_SOURCE = 50
            mock_settings.NEWS_SOURCE_STALE_MINUTES = 60
            mock_settings.NEWS_DEDUP_SIMILARITY_THRESHOLD = 0.82
            mock_settings.NEWS_DEDUP_WINDOW_HOURS = 6
            mock_settings.NEWS_MIN_SOURCE_RELIABILITY_FOR_SIGNAL = 0.70
            mock_settings.NEWS_MIN_CONFIDENCE_FOR_SIGNAL = 0.75
            mock_settings.NEWS_RETENTION_HOURS = 168

            with patch("app.news.rss_provider_client.urllib_request.urlopen") as mock_urlopen:
                worker = build_news_ingestion_worker(tmp_db)
                assert not worker._enabled
                # Polling should be a no-op
                worker._poll_due_sources()
                mock_urlopen.assert_not_called()


# ---------------------------------------------------------------------------
# T10 — 0 items today → feed status returns has_live_data: false
# ---------------------------------------------------------------------------

class TestFeedStatus:
    def test_empty_feed_status(self, tmp_db):
        from app.news.news_provider_health import ProviderHealthService

        svc = ProviderHealthService(tmp_db)
        summary = svc.get_feed_summary()

        assert summary["today_count"] == 0
        assert summary["has_live_data"] is False
        assert summary["latest_title"] is None


# ---------------------------------------------------------------------------
# T11 + T12 — Shadow enforcement: should_affect_trading=0, shadow_only=1
# ---------------------------------------------------------------------------

class TestShadowEnforcement:
    def test_manual_import_signals_are_shadow_only(self, tmp_db):
        from app.news.manual_news_import_service import ManualNewsImportService
        from shared_lib.persistence.news_intelligence import get_active_signals

        svc = ManualNewsImportService(tmp_db)
        svc.import_item(
            title="Bitcoin ETF approved — market expected to surge massively",
            source_name="Reuters",
            source_url="https://reuters.com/story/btc-etf-001",
            published_utc_raw="2024-06-01T10:00:00Z",
            body_snippet="Authorities approved the Bitcoin spot ETF. Bullish outlook.",
            affected_symbols=["BTCUSDT"],
        )

        signals = get_active_signals(tmp_db)
        for sig in signals:
            # T11: NEWS_SIGNAL_CAN_OPEN_TRADES=False → should_affect_trading always 0
            assert sig.get("should_affect_trading", 0) == 0, \
                f"Signal {sig['id']} has should_affect_trading != 0"
            # T12: NEWS_SIGNAL_CAN_BLOCK_TRADES=False → shadow_only always 1
            assert sig.get("shadow_only", 1) == 1, \
                f"Signal {sig['id']} has shadow_only != 1"
