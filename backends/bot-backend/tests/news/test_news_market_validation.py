import os
import tempfile
import pytest
from datetime import datetime, timezone, timedelta
from shared_lib.persistence.db import DB
from shared_lib.persistence.news_items import insert_cluster
from shared_lib.persistence.news_intelligence import upsert_asset_mapping
from shared_lib.persistence.news_intelligence import insert_signal, get_active_signals
from shared_lib.persistence.market_reactions import upsert_reaction
from app.news.news_market_linker import find_market_reaction
from app.news.news_impact_scorer import compute_impact_score
from app.news.news_sentiment_validator import validate_sentiment, sentiment_to_direction
from app.news.news_signal_effectiveness import classify_signal
from app.news.news_narrative_tracker import update_narrative_effectiveness, get_narrative_effectiveness

def _now() -> datetime:
    return datetime.now(timezone.utc)

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

def test_market_linker(memory_db):
    now = _now()
    now_str = now.isoformat()
    cluster_id = insert_cluster(memory_db, canonical_title="Test", first_seen_utc=now_str)
    upsert_asset_mapping(memory_db, cluster_id=cluster_id, symbol="BTC", mapping_confidence=0.9)

    # Insert a market reaction 10 minutes AFTER the news
    event_time = (now + timedelta(minutes=10)).isoformat()
    upsert_reaction(memory_db, event_id="E1", symbol="BTC", event_time_utc=event_time, net_move_pct=0.02)

    matches = find_market_reaction(memory_db, cluster_id=cluster_id, first_seen_utc=now_str)
    assert len(matches) == 1
    assert matches[0]["symbol"] == "BTC"
    assert matches[0]["latency_minutes"] == 10.0

def test_market_linker_no_match(memory_db):
    now_str = _now().isoformat()
    cluster_id = insert_cluster(memory_db, canonical_title="Test", first_seen_utc=now_str)
    upsert_asset_mapping(memory_db, cluster_id=cluster_id, symbol="ETH", mapping_confidence=0.9)

    matches = find_market_reaction(memory_db, cluster_id=cluster_id, first_seen_utc=now_str)
    assert len(matches) == 0

def test_impact_scorer():
    row = {
        "net_move_pct": 3.0,               # 3% = 1.0 weight for this component
        "volatility_expansion_ratio": 4.0, # (4-1)/3 = 1.0 weight
        "volume_spike_ratio": 5.0,         # (5-1)/4 = 1.0 weight
        "reaction_type": "CONTINUATION"
    }
    score, cat = compute_impact_score(row, latency_minutes=3.0)
    assert score == 1.0
    assert cat == "IMMEDIATE"

    # Low impact downgrades category
    row2 = {"net_move_pct": 0.01, "reaction_type": "CONTINUATION"}
    score2, cat2 = compute_impact_score(row2, latency_minutes=3.0)
    assert score2 < 0.02
    assert cat2 == "NO_REACTION"

def test_sentiment_validator():
    assert sentiment_to_direction(0.8) == "BULLISH"
    assert sentiment_to_direction(-0.8) == "BEARISH"
    assert sentiment_to_direction(0.0) == "NEUTRAL"

    # Bullish correct
    sd, ad, acc, val = validate_sentiment(0.8, {"direction_after_event": "UP"})
    assert acc == "CORRECT"

    # Bearish incorrect
    sd, ad, acc, val = validate_sentiment(-0.8, {"direction_after_event": "UP"})
    assert acc == "INCORRECT"

def test_false_signal_detection():
    # Strong sentiment (correct), but tiny impact
    eff, reason = classify_signal(
        impact_score=0.01,
        sentiment_accuracy_score=1.0,
        sentiment_accuracy="CORRECT",
        data_quality_score=0.8,
        reliability_score=0.8,
        reaction_type="CONTINUATION"
    )
    assert reason == "FALSE_SIGNAL"

    # No impact event
    eff2, reason2 = classify_signal(
        impact_score=0.01,
        sentiment_accuracy_score=0.3,
        sentiment_accuracy="NEUTRAL",
        data_quality_score=0.8,
        reliability_score=0.8,
        reaction_type="NO_REACTION"
    )
    assert reason2 == "NO_IMPACT_EVENT"

    # Misleading
    eff3, reason3 = classify_signal(
        impact_score=0.5,
        sentiment_accuracy_score=0.0,
        sentiment_accuracy="INCORRECT",
        data_quality_score=0.8,
        reliability_score=0.8,
        reaction_type="REVERSAL"
    )
    assert reason3 == "MISLEADING_NEWS"

def test_narrative_tracker(memory_db):
    update_narrative_effectiveness(
        memory_db,
        narrative_type="TEST_NARRATIVE",
        impact_score=1.0,
        price_move_pct=0.05,
        sentiment_accuracy="CORRECT",
        is_false_signal=False,
        effectiveness_score=1.0
    )
    row = get_narrative_effectiveness(memory_db, "TEST_NARRATIVE")
    assert row is not None
    assert row["sample_count"] == 1
    assert row["avg_impact_score"] == 1.0

    # Apply EMA logic
    update_narrative_effectiveness(
        memory_db,
        narrative_type="TEST_NARRATIVE",
        impact_score=0.5,
        price_move_pct=0.02,
        sentiment_accuracy="INCORRECT",
        is_false_signal=True,
        effectiveness_score=0.5
    )
    row2 = get_narrative_effectiveness(memory_db, "TEST_NARRATIVE")
    assert row2["sample_count"] == 2
    # EMA: old*0.8 + new*0.2 = 1.0*0.8 + 0.5*0.2 = 0.9
    assert row2["avg_impact_score"] == 0.9
    # False signal ratio: 0.0*0.8 + 1.0*0.2 = 0.2
    assert row2["false_signal_ratio"] == 0.2


def test_market_validation_updates_signal_validity(memory_db):
    from app.news.news_market_validation_service import NewsMarketValidationService

    now = _now()
    now_str = now.isoformat()
    cluster_id = insert_cluster(memory_db, canonical_title="ETF approval", first_seen_utc=now_str)
    upsert_asset_mapping(memory_db, cluster_id=cluster_id, symbol="BTC", mapping_confidence=0.9)
    insert_signal(
        memory_db,
        cluster_id=cluster_id,
        symbol="BTC",
        signal_type="NEWS_SENTIMENT_BULLISH",
        sentiment_label="BULLISH",
        confidence_score=0.82,
        reliability_score=0.91,
        source_validation_passed=True,
        market_validation_passed=False,
        validation_status="PENDING_MARKET_VALIDATION",
        market_confirmation_status="PENDING_MARKET_VALIDATION",
        data_quality_status="HIGH_CONFIDENCE",
        is_valid_signal=False,
    )

    event_time = (now + timedelta(minutes=10)).isoformat()
    upsert_reaction(
        memory_db,
        event_id="E_VALID",
        symbol="BTC",
        event_time_utc=event_time,
        net_move_pct=3.0,
        volatility_expansion_ratio=2.5,
        volume_spike_ratio=3.0,
        reaction_type="TREND_CONTINUATION",
        direction_after_event="UP",
    )

    svc = NewsMarketValidationService(memory_db)
    results = svc.validate_cluster(
        cluster_id,
        first_seen_utc=now_str,
        sentiment_score=0.9,
        data_quality_score=0.8,
        reliability_score=0.9,
        top_narrative="ETF_APPROVAL",
    )
    assert len(results) == 1

    signals = get_active_signals(memory_db, symbol="BTC")
    assert len(signals) == 1
    signal = signals[0]
    assert signal["market_validation_passed"] == 1
    assert signal["is_valid_signal"] == 1
    assert signal["validation_status"] == "VALIDATED"
    assert signal["market_confirmation_status"] == "DELAYED_REACTION"


def test_market_validation_invalidates_no_reaction_signal(memory_db):
    from app.news.news_market_validation_service import NewsMarketValidationService

    now = _now().isoformat()
    cluster_id = insert_cluster(memory_db, canonical_title="Quiet news", first_seen_utc=now)
    upsert_asset_mapping(memory_db, cluster_id=cluster_id, symbol="ETH", mapping_confidence=0.9)
    insert_signal(
        memory_db,
        cluster_id=cluster_id,
        symbol="ETH",
        signal_type="NEWS_SENTIMENT_BEARISH",
        sentiment_label="BEARISH",
        confidence_score=0.7,
        reliability_score=0.85,
        source_validation_passed=True,
        market_validation_passed=False,
        validation_status="PENDING_MARKET_VALIDATION",
        market_confirmation_status="PENDING_MARKET_VALIDATION",
        data_quality_status="HIGH_CONFIDENCE",
        is_valid_signal=False,
    )

    svc = NewsMarketValidationService(memory_db)
    results = svc.validate_cluster(
        cluster_id,
        first_seen_utc=now,
        sentiment_score=-0.8,
        data_quality_score=0.8,
        reliability_score=0.85,
        top_narrative="REGULATORY_ACTION",
    )
    assert len(results) == 1
    assert results[0]["reaction_latency_category"] == "NO_REACTION"

    signals = get_active_signals(memory_db, symbol="ETH")
    signal = signals[0]
    assert signal["market_validation_passed"] == 0
    assert signal["is_valid_signal"] == 0
    assert signal["validation_status"] == "INVALIDATED"
    assert signal["market_confirmation_status"] == "NO_MARKET_REACTION"
