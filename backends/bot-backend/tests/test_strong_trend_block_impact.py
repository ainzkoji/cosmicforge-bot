from __future__ import annotations

import json
from pathlib import Path

from scripts.validation.analyze_strong_trend_block import (
    RECOMMENDATIONS,
    choose_recommendation,
    run_audit,
    sha256,
)


BOT_ROOT = Path(__file__).resolve().parents[1]


def test_recommendation_never_enables_live_or_ml():
    recommendation, _ = choose_recommendation(
        {
            "accepted_trades": 30,
            "expectancy_r": 0.2,
            "profit_factor_r": 1.5,
            "max_drawdown_r": 3.0,
        }
    )
    assert recommendation in RECOMMENDATIONS
    assert "LIVE" not in recommendation
    assert "ML" not in recommendation


def test_negative_strong_trend_expectancy_stays_blocked():
    recommendation, _ = choose_recommendation(
        {
            "accepted_trades": 30,
            "expectancy_r": -0.1,
            "profit_factor_r": 0.8,
            "max_drawdown_r": 6.0,
        }
    )
    assert recommendation == "KEEP_STRONG_TREND_BLOCKED"


def test_small_positive_sample_can_only_be_recommended_for_paper():
    recommendation, reasons = choose_recommendation(
        {
            "accepted_trades": 7,
            "expectancy_r": 0.3,
            "profit_factor_r": 1.7,
            "max_drawdown_r": 1.4,
        }
    )
    assert recommendation == "ALLOW_STRONG_TREND_IN_PAPER_ONLY"
    assert any("fewer than 20" in reason for reason in reasons)


def test_audit_generates_report_without_modifying_active_env(tmp_path):
    env_path = BOT_ROOT / ".env"
    before = sha256(env_path)
    output_md = tmp_path / "strong_trend.md"
    output_json = tmp_path / "strong_trend.json"

    report = run_audit(
        symbols=["BTCUSDT", "ETHUSDT"],
        lookback_decisions=20,
        output_md=output_md,
        output_json=output_json,
    )

    assert sha256(env_path) == before
    assert report["active_env_modified"] is False
    assert output_md.exists()
    assert output_json.exists()
    saved = json.loads(output_json.read_text(encoding="utf-8"))
    assert saved["safety"]["live_use_recommended"] is False
    assert saved["safety"]["ml_enable_recommended"] is False
    assert saved["runtime_config"]["TRADE_SYMBOLS_parsed"] == ["BTCUSDT", "ETHUSDT"]
