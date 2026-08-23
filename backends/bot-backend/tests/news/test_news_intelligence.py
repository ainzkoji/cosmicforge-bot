"""
Expanded test suite covering Phase 3 hardening engines:
  - Spam detector
  - Manipulation detector
  - Latency engine
  - Signal validator
  - Full hardened pipeline
  - Shadow-only invariant enforcement
"""
import pytest
import os
import tempfile
from datetime import datetime, timezone, timedelta

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_items import insert_raw_item, insert_cluster, link_item_to_cluster
from shared_lib.persistence.news_intelligence import (
    upsert_asset_mapping,
    upsert_sentiment,
    upsert_narrative,
    insert_signal,
    get_active_signals,
    get_signal_stats,
    update_cluster_quality,
    get_data_quality_summary,
)
from app.news.news_asset_mapper import NewsAssetMapper
from app.news.news_sentiment_service import NewsSentimentService
from app.news.news_narrative_classifier import NewsNarrativeClassifier
from app.news.news_intelligence_signal_service import NewsIntelligenceSignalService
from app.news.news_spam_detector import compute_spam_score, is_spam_cluster
from app.news.news_manipulation_detector import detect_manipulation
from app.news.news_latency_engine import compute_latency_score
from app.news.news_signal_validator import evaluate_signal_validity, DataQualityStatus


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def memory_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = DB(path=path)
    from shared_lib.persistence.migrations import migrate
    migrate(db_path=path)
    yield db
    try:
        os.remove(path)
    except Exception:
        pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ago(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


# ──────────────────────────────────────────────────────────────────────────────
# Phase 3 base tests (kept from previous suite)
# ──────────────────────────────────────────────────────────────────────────────

def test_insert_raw_item_and_cluster(memory_db):
    now = _now()
    raw_id = insert_raw_item(
        memory_db, provider="cryptopanic", title="Bitcoin surges to $100k",
        published_utc=now, ingested_utc=now, external_id="cp-1234",
    )
    assert raw_id is not None

    cluster_id = insert_cluster(
        memory_db, canonical_title="Bitcoin hits new high",
        first_seen_utc=now, reliability_score=0.9,
    )
    assert cluster_id is not None
    link_item_to_cluster(memory_db, cluster_id, raw_id, 0.95)


def test_news_asset_mapper(memory_db):
    mapper = NewsAssetMapper(memory_db)
    now = _now()
    raw_id = insert_raw_item(
        memory_db, provider="test", title="Ethereum Foundation announces network upgrade",
        published_utc=now, ingested_utc=now, external_id="test-1",
    )
    cluster_id = insert_cluster(memory_db, canonical_title="ETH upgrade", first_seen_utc=now)
    symbols = mapper.map_and_store(
        cluster_id=cluster_id, title="Ethereum Foundation announces network upgrade",
        body="ETH expected to get faster."
    )
    assert "ETHUSDT" in symbols


def test_news_sentiment_scoring(memory_db):
    svc = NewsSentimentService(memory_db)
    now = _now()
    cluster_id = insert_cluster(memory_db, canonical_title="Hack event", first_seen_utc=now)
    sentiment = svc.score_and_store(
        cluster_id=cluster_id,
        title="Terrible news, exchange hacked, market crashing",
        body="",
    )
    assert sentiment < 0.0  # bearish


def test_news_narrative_classification(memory_db):
    clf = NewsNarrativeClassifier(memory_db)
    now = _now()
    cluster_id = insert_cluster(memory_db, canonical_title="SEC approves new Bitcoin ETF", first_seen_utc=now)
    narratives = clf.classify_and_store(
        cluster_id=cluster_id, canonical_title="SEC approves new Bitcoin ETF",
        body_text="The securities and exchange commission has approved it.",
    )
    types = [n["narrative_type"] for n in narratives]
    assert "ETF_APPROVAL" in types
    assert "REGULATORY_ACTION" in types


def test_multiple_narratives_allowed_per_cluster(memory_db):
    now = _now()
    cluster_id = insert_cluster(memory_db, canonical_title="SEC delays ETF on liquidity concerns", first_seen_utc=now)

    upsert_narrative(
        memory_db,
        cluster_id=cluster_id,
        narrative_type="ETF_APPROVAL",
        narrative_confidence=0.55,
        matched_keywords="etf",
    )
    upsert_narrative(
        memory_db,
        cluster_id=cluster_id,
        narrative_type="REGULATORY_ACTION",
        narrative_confidence=0.65,
        matched_keywords="sec",
    )

    with memory_db.connect() as conn:
        rows = conn.execute(
            "SELECT narrative_type FROM news_narratives WHERE cluster_id=? ORDER BY narrative_type",
            (cluster_id,),
        ).fetchall()

    assert [r["narrative_type"] for r in rows] == ["ETF_APPROVAL", "REGULATORY_ACTION"]


def test_duplicate_narrative_type_is_idempotent_and_updates_confidence(memory_db):
    now = _now()
    cluster_id = insert_cluster(memory_db, canonical_title="Ethereum ETF decision", first_seen_utc=now)

    upsert_narrative(
        memory_db,
        cluster_id=cluster_id,
        narrative_type="ETF_APPROVAL",
        narrative_confidence=0.40,
        matched_keywords="etf",
    )
    upsert_narrative(
        memory_db,
        cluster_id=cluster_id,
        narrative_type="ETF_APPROVAL",
        narrative_confidence=0.70,
        matched_keywords="ethereum,etf",
    )

    with memory_db.connect() as conn:
        rows = conn.execute(
            """SELECT narrative_type, narrative_confidence, matched_keywords
               FROM news_narratives WHERE cluster_id=?""",
            (cluster_id,),
        ).fetchall()

    assert len(rows) == 1
    assert rows[0]["narrative_type"] == "ETF_APPROVAL"
    assert rows[0]["narrative_confidence"] == pytest.approx(0.70)
    assert rows[0]["matched_keywords"] == "ethereum,etf"


def test_shadow_signal_emission(memory_db):
    svc = NewsIntelligenceSignalService(memory_db, shadow_mode=True)
    now = _now()
    cluster_id = insert_cluster(
        memory_db, canonical_title="Massive bullish breakout expected",
        first_seen_utc=now, reliability_score=0.85,
    )
    svc.maybe_emit_signal(
        cluster_id=cluster_id,
        cluster_row={"canonical_title": "test", "highest_reliability_score": 0.85, "first_seen_utc": now},
        symbols=["BTCUSDT"],
        sentiment={"confidence": 0.8, "label": "BULLISH", "score": 0.8},
        narratives=[{"narrative_type": "MACRO_POLICY", "label": "Macro Policy", "confidence": 0.9}],
        is_manipulation_suspect=False,
    )
    signals = get_active_signals(memory_db)
    assert len(signals) == 1
    signal = signals[0]
    assert signal["symbol"] == "BTCUSDT"
    assert signal["signal_type"] == "NEWS_SENTIMENT_BULLISH"
    assert signal["should_affect_trading"] == 0   # MUST ALWAYS BE 0
    assert signal["shadow_only"] == 1             # MUST ALWAYS BE 1
    assert signal["source_validation_passed"] == 1
    assert signal["market_validation_passed"] == 0
    assert signal["is_valid_signal"] == 0
    assert signal["validation_status"] == "PENDING_MARKET_VALIDATION"


# ──────────────────────────────────────────────────────────────────────────────
# Spam Detector
# ──────────────────────────────────────────────────────────────────────────────

def test_spam_score_zero_for_clean_data():
    score = compute_spam_score(
        titles=["Bitcoin ETF approved by SEC", "BlackRock files for Bitcoin ETF"],
        source_reliabilities=[0.95, 0.90],
        duplicate_count=0,
        total_count=2,
    )
    assert score < 0.30, f"Expected low spam score, got {score}"


def test_spam_score_high_for_identical_titles():
    titles = ["BTC MOON 1000x now!!!"] * 10
    score = compute_spam_score(
        titles=titles,
        source_reliabilities=[0.10] * 10,
        ingestion_timestamps=[_now()] * 10,
        duplicate_count=8,
        total_count=10,
    )
    assert score >= 0.60, f"Expected high spam score, got {score}"


def test_is_spam_cluster_threshold():
    assert is_spam_cluster(0.65) is True
    assert is_spam_cluster(0.30) is False


def test_spam_velocity_spike():
    # 8 items within 30 seconds = bot-like
    base = datetime.now(timezone.utc)
    timestamps = [(base + timedelta(seconds=i * 3)).isoformat() for i in range(8)]
    score = compute_spam_score(
        titles=["crypto pump incoming"] * 8,
        source_reliabilities=[0.15] * 8,
        ingestion_timestamps=timestamps,
        duplicate_count=5,
        total_count=8,
    )
    assert score >= 0.50, f"Expected elevated spam score for velocity spike, got {score}"


# ──────────────────────────────────────────────────────────────────────────────
# Manipulation Detector
# ──────────────────────────────────────────────────────────────────────────────

def test_no_manipulation_clean_cluster():
    flag = detect_manipulation(
        source_reliabilities=[0.90, 0.85, 0.80],
        source_domains=["reuters.com", "bloomberg.com", "ft.com"],
        spam_score=0.05,
        narrative_types=["ETF_APPROVAL", "REGULATORY_ACTION"],
        source_count=3,
        provider_count=3,
    )
    assert flag is None


def test_manipulation_low_quality_surge():
    flag = detect_manipulation(
        source_reliabilities=[0.10, 0.12, 0.08, 0.15, 0.10],
        source_domains=["t.me", "t.me", "t.me", "reddit.com", "twitter.com"],
        spam_score=0.70,
        narrative_types=["RUMOR_SPECULATION"],
        source_count=5,
        provider_count=2,
        velocity_items_per_minute=4.5,
    )
    assert flag in ("POSSIBLE_MANIPULATION", "BOT_AMPLIFICATION", "RUMOR_ONLY", "LOW_CONFIDENCE_EVENT")


def test_manipulation_rumor_only():
    flag = detect_manipulation(
        source_reliabilities=[0.20, 0.25],
        source_domains=["reddit.com", "twitter.com"],
        spam_score=0.30,
        narrative_types=["RUMOR_SPECULATION", "GENERAL_CRYPTO_NEWS"],
        source_count=2,
        provider_count=2,
    )
    assert flag == "RUMOR_ONLY"


def test_manipulation_single_provider_amplification():
    flag = detect_manipulation(
        source_reliabilities=[0.15] * 8,
        source_domains=["t.me"] * 8,
        spam_score=0.55,
        narrative_types=["MARKET_SENTIMENT"],
        source_count=8,
        provider_count=1,
    )
    assert flag in ("BOT_AMPLIFICATION", "POSSIBLE_MANIPULATION")


# ──────────────────────────────────────────────────────────────────────────────
# Latency Engine
# ──────────────────────────────────────────────────────────────────────────────

def test_latency_real_time():
    score, flag = compute_latency_score(first_seen_utc=_now())
    assert score == 1.0
    assert flag is None


def test_latency_delayed():
    score, flag = compute_latency_score(first_seen_utc=_ago(6))
    assert 0.30 <= score <= 0.80
    assert flag == "DELAYED_REACTION"


def test_latency_stale():
    score, flag = compute_latency_score(first_seen_utc=_ago(36))
    assert score < 0.30
    assert flag == "STALE_NEWS"


def test_latency_edge_exactly_at_delay_threshold():
    score, flag = compute_latency_score(first_seen_utc=_ago(4.1))
    assert flag == "DELAYED_REACTION"


# ──────────────────────────────────────────────────────────────────────────────
# Signal Validator
# ──────────────────────────────────────────────────────────────────────────────

def test_valid_high_confidence_signal():
    is_valid, status = evaluate_signal_validity(
        cluster_confidence=0.80,
        reliability_score=0.88,
        spam_score=0.05,
        latency_score=1.0,
        manipulation_flag=None,
        latency_flag=None,
        sentiment_confidence=0.85,
    )
    assert is_valid is True
    assert status == DataQualityStatus.HIGH_CONFIDENCE


def test_invalid_spam_signal():
    is_valid, status = evaluate_signal_validity(
        cluster_confidence=0.30,
        reliability_score=0.20,
        spam_score=0.75,
        latency_score=0.9,
        manipulation_flag=None,
        latency_flag=None,
    )
    assert is_valid is False
    assert status == DataQualityStatus.SPAM


def test_invalid_manipulated_signal():
    is_valid, status = evaluate_signal_validity(
        cluster_confidence=0.50,
        reliability_score=0.40,
        spam_score=0.20,
        latency_score=0.9,
        manipulation_flag="POSSIBLE_MANIPULATION",
        latency_flag=None,
    )
    assert is_valid is False
    assert status == DataQualityStatus.MANIPULATED


def test_invalid_stale_signal():
    is_valid, status = evaluate_signal_validity(
        cluster_confidence=0.70,
        reliability_score=0.85,
        spam_score=0.05,
        latency_score=0.05,
        manipulation_flag=None,
        latency_flag="STALE_NEWS",
    )
    assert is_valid is False
    assert status == DataQualityStatus.STALE


def test_medium_confidence_signal():
    is_valid, status = evaluate_signal_validity(
        cluster_confidence=0.55,
        reliability_score=0.50,
        spam_score=0.15,
        latency_score=0.70,
        manipulation_flag="RUMOR_ONLY",
        latency_flag=None,
    )
    # RUMOR_ONLY degrades quality but shouldn't hard-block
    assert status == DataQualityStatus.MEDIUM_CONFIDENCE


# ──────────────────────────────────────────────────────────────────────────────
# DB persistence of quality scores
# ──────────────────────────────────────────────────────────────────────────────

def test_update_cluster_quality(memory_db):
    now = _now()
    cluster_id = insert_cluster(memory_db, canonical_title="Test cluster", first_seen_utc=now)
    update_cluster_quality(
        memory_db, cluster_id,
        cluster_confidence=0.75,
        spam_score=0.10,
        latency_score=0.95,
        is_valid_signal=True,
        manipulation_flag=None,
        data_quality_status="HIGH_CONFIDENCE",
    )
    with memory_db.connect() as conn:
        row = conn.execute("SELECT * FROM news_clusters WHERE id=?", (cluster_id,)).fetchone()
    assert dict(row)["cluster_confidence"] == pytest.approx(0.75)
    assert dict(row)["is_valid_signal"] == 1
    assert dict(row)["data_quality_status"] == "HIGH_CONFIDENCE"


def test_signal_carries_quality_fields(memory_db):
    now = _now()
    cluster_id = insert_cluster(memory_db, canonical_title="BTC Signal Test", first_seen_utc=now)
    insert_signal(
        memory_db,
        cluster_id=cluster_id,
        symbol="BTCUSDT",
        signal_type="NEWS_SENTIMENT_BULLISH",
        sentiment_label="BULLISH",
        confidence_score=0.80,
        reliability_score=0.90,
        spam_score=0.05,
        latency_score=0.95,
        source_validation_passed=True,
        market_validation_passed=True,
        is_valid_signal=True,
        data_quality_status="HIGH_CONFIDENCE",
        validation_status="VALIDATED",
        market_confirmation_status="MARKET_CONFIRMED",
        should_affect_trading=False,   # will be hard-enforced to 0
        shadow_only=True,              # will be hard-enforced to 1
    )
    signals = get_active_signals(memory_db)
    assert len(signals) == 1
    s = signals[0]
    assert s["should_affect_trading"] == 0    # INVARIANT
    assert s["shadow_only"] == 1               # INVARIANT
    assert s["spam_score"] == pytest.approx(0.05)
    assert s["is_valid_signal"] == 1
    assert s["source_validation_passed"] == 1
    assert s["market_validation_passed"] == 1
    assert s["validation_status"] == "VALIDATED"
    assert s["data_quality_status"] == "HIGH_CONFIDENCE"


def test_data_quality_summary(memory_db):
    now = _now()
    for i, status in enumerate(["HIGH_CONFIDENCE", "SPAM", "MANIPULATED"]):
        cid = insert_cluster(memory_db, canonical_title=f"Cluster {i}", first_seen_utc=now)
        update_cluster_quality(
            memory_db, cid,
            cluster_confidence=0.5, spam_score=0.1, latency_score=0.9,
            is_valid_signal=(status == "HIGH_CONFIDENCE"),
            manipulation_flag=None,
            data_quality_status=status,
        )
    summary = get_data_quality_summary(memory_db)
    assert summary["total_clusters"] == 3
    assert summary["valid_clusters"] == 1
    statuses = {s["data_quality_status"] for s in summary["by_status"]}
    assert "HIGH_CONFIDENCE" in statuses
    assert "SPAM" in statuses
