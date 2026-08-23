from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "bot-backend"))

from app.signals.signal_ranking import RANKING_WEIGHTS, rank_signal_candidates, select_top_candidates  # noqa: E402


def _candidate(symbol: str, confidence: float, rr: float):
    return {
        "symbol": symbol,
        "side": "BUY",
        "signal_id": f"sig_{symbol}",
        "candidate_id": f"cand_{symbol}",
        "confidence_score": confidence,
        "risk_reward": rr,
    }


def test_ranking_weights_sum_to_one():
    assert round(sum(RANKING_WEIGHTS.values()), 6) == 1.0


def test_signal_ranking_is_deterministic_and_confidence_improves_rank():
    metrics = {
        "BTCUSDT": {"liquidity_score": 90, "spread_score": 90, "volatility_score": 80},
        "ETHUSDT": {"liquidity_score": 90, "spread_score": 90, "volatility_score": 80},
    }
    ranked = rank_signal_candidates(
        [_candidate("ETHUSDT", 75, 2.0), _candidate("BTCUSDT", 90, 2.0)],
        pair_metrics=metrics,
    )

    assert [item["symbol"] for item in ranked] == ["BTCUSDT", "ETHUSDT"]
    assert ranked[0]["rank_position"] == 1
    assert ranked[0]["total_rank_score"] > ranked[1]["total_rank_score"]


def test_better_risk_reward_and_liquidity_improve_rank():
    ranked = rank_signal_candidates(
        [_candidate("LOWUSDT", 80, 1.8), _candidate("HIGHUSDT", 80, 3.0)],
        pair_metrics={
            "LOWUSDT": {"liquidity_score": 50, "spread_score": 60, "volatility_score": 70},
            "HIGHUSDT": {"liquidity_score": 100, "spread_score": 100, "volatility_score": 90},
        },
    )

    assert ranked[0]["symbol"] == "HIGHUSDT"


def test_missing_symbol_performance_uses_neutral_score_and_top_selection():
    ranked = rank_signal_candidates([_candidate("BTCUSDT", 80, 2.0)])

    assert ranked[0]["component_scores"]["symbol_performance_score"] == 50.0
    assert "neutral" in ranked[0]["ranking_reason"].lower()
    assert select_top_candidates(ranked, max_published_per_scan=1) == ranked
