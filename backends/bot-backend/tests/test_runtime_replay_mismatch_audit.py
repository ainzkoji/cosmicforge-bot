from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from scripts.validation.audit_runtime_replay_mismatch import (
    candle_timing_audit,
    choose_recommendation,
    compare_components,
    compare_configs,
    compare_sessions,
    compare_symbols,
    replay_realism_check,
    run_audit,
    sha256,
    summarize_runtime,
)


NOW = datetime(2026, 6, 15, 18, 0, tzinfo=timezone.utc)


def _component(name: str, failed: list[str], signal: str = "HOLD") -> dict:
    return {
        "component_name": name,
        "component_signal": signal,
        "component_failed_conditions": failed,
        "indicator_snapshot": {"candle_count": 120},
    }


def _trace(
    trace_id: str,
    symbol: str,
    ts: str,
    *,
    regime: str = "STRONG_TREND",
    components: list[dict] | None = None,
) -> dict:
    breakdown = {
        "component_breakdown": components or [],
        "session_allowed": True,
    }
    return {
        "trace_id": trace_id,
        "cycle_id": trace_id,
        "ts": ts,
        "symbol": symbol,
        "timeframe": "15m",
        "regime_state": regime,
        "signal": "HOLD",
        "confidence": 0.0,
        "reason_codes": "NO_PATTERN",
        "gate_reason": "NO_PATTERN",
        "gate_details_json": json.dumps({"hold_breakdown": breakdown}),
        "adx": 30.0,
        "atr_pct": 1.0,
        "ma_slope": 0.2,
        "compression_ratio": 0.8,
        "breakout_pressure": 0.2,
    }


def _runtime_rows() -> list[dict]:
    return [
        _trace(
            "t1",
            "BTCUSDT",
            "2026-06-15T08:00:00+00:00",
            components=[
                _component("sma_cross", ["fresh_fast_slow_sma_cross"]),
                _component("trend_pullback", ["rsi_reset_and_turn"]),
            ],
        ),
        _trace(
            "t2",
            "ETHUSDT",
            "2026-06-15T17:00:00+00:00",
            components=[
                _component("donchian_breakout", ["fresh_donchian_breakout"]),
                _component("supertrend", ["supertrend_flip_or_continuation"]),
            ],
        ),
    ]


def _historical_replay() -> dict:
    metric = {"accepted_trades": 0, "win_rate": None}
    return {
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "replay": {
            "strong_trend_cycles": 217,
            "strong_trend_only": {
                "accepted_trades": 7,
                "btc_vs_eth": {
                    "BTCUSDT": {**metric, "accepted_trades": 2, "win_rate": 0.0},
                    "ETHUSDT": {**metric, "accepted_trades": 5, "win_rate": 0.8},
                },
                "session_performance": {
                    "06:00-10:00": {**metric, "accepted_trades": 4},
                    "13:00-16:00": {**metric, "accepted_trades": 2},
                    "16:00-19:00": {**metric, "accepted_trades": 1},
                },
                "component_source_performance": {
                    "supertrend": {**metric, "accepted_trades": 6},
                    "trend_pullback": {**metric, "accepted_trades": 1},
                },
            },
        },
    }


def _component_replay() -> dict:
    return {
        "configuration": {
            "ensemble_threshold_floor": 0.55,
            "session_filter_enabled": True,
            "session_windows_utc": "06:00-19:00",
        },
        "component_configuration": [
            {"component_name": "supertrend", "minimum_confidence": 0.5},
            {"component_name": "trend_pullback", "minimum_confidence": 0.75},
        ],
        "symbols": [],
    }


def _paths(tmp_path: Path) -> dict:
    env = tmp_path / ".env"
    env.write_text(
        "EXECUTION_MODE=paper\nML_ENABLED=False\nIOFS_GATE_MODE=shadow\n",
        encoding="utf-8",
    )
    production = tmp_path / "production"
    production.mkdir()
    (production / "README.md").write_text("unchanged", encoding="utf-8")
    return {
        "active_env": env,
        "production_dir": production,
        "output_md": tmp_path / "mismatch.md",
        "output_json": tmp_path / "mismatch.json",
    }


def test_audit_generates_json_and_markdown_reports(tmp_path):
    paths = _paths(tmp_path)
    report = run_audit(
        runtime_decisions=500,
        runtime_rows=_runtime_rows(),
        historical_replay=_historical_replay(),
        component_replay=_component_replay(),
        now=NOW,
        **paths,
    )
    assert report["runtime"]["decision_sample_size"] == 2
    assert paths["output_json"].exists()
    assert paths["output_md"].exists()


def test_config_mismatch_detection_works():
    result = compare_configs(
        {
            "DEFAULT_INTERVAL": "15m",
            "ENSEMBLE_BLOCKED_REGIMES": "",
            "component weights": {"adaptive": True},
        },
        {
            "DEFAULT_INTERVAL": "15m",
            "ENSEMBLE_BLOCKED_REGIMES": "ignored for STRONG_TREND analysis",
            "component weights": {"adaptive": False},
        },
    )
    assert "CONFIG_MISMATCH" in result["flags"]
    assert "ENSEMBLE_BLOCKED_REGIMES" in result["signal_creation_mismatches"]


def test_session_window_comparison_works():
    result = compare_sessions(
        _runtime_rows(),
        _historical_replay(),
        runtime_windows="06:00-19:00",
    )
    assert result["runtime_cycles_inside_replay_windows"] == 1
    assert result["runtime_cycles_outside_replay_windows"] == 1
    assert result["replay_opportunities_inside_runtime_window"] == 7


def test_symbol_distribution_comparison_works():
    result = compare_symbols(summarize_runtime(_runtime_rows()), _historical_replay())
    assert result["replay_STRONG_TREND_opportunities_by_symbol"]["ETHUSDT"] == 5
    assert result["runtime_STRONG_TREND_cycles_by_symbol"] == {
        "BTCUSDT": 1,
        "ETHUSDT": 1,
    }


def test_component_failure_aggregation_works():
    summary = summarize_runtime(_runtime_rows())
    comparison = compare_components(summary["component_failures"], _historical_replay())
    by_name = {item["condition"]: item for item in comparison}
    assert by_name["fresh_fast_slow_sma_cross"]["runtime_fail_count"] == 1
    assert by_name["supertrend_flip"]["replay_pass_count"] == 6


def test_candle_timing_mismatch_is_flagged():
    assert "CANDLE_TIMING_MISMATCH" in candle_timing_audit(NOW)["flags"]


def test_replay_realism_flags_simplified_replay():
    realism = replay_realism_check()
    assert realism["uses_master_ensemble"] is False
    assert "REPLAY_NOT_RUNTIME_EQUIVALENT" in realism["flags"]


def test_recommendation_never_enables_ml_or_live():
    recommendation, _ = choose_recommendation(
        {
            "replay_realism": {"flags": ["REPLAY_NOT_RUNTIME_EQUIVALENT"]},
            "candle_timing": {"flags": ["CANDLE_TIMING_MISMATCH"]},
        }
    )
    assert "ML" not in recommendation
    assert "LIVE" not in recommendation


def test_active_env_remains_unchanged(tmp_path):
    paths = _paths(tmp_path)
    before = sha256(paths["active_env"])
    run_audit(
        runtime_decisions=500,
        runtime_rows=_runtime_rows(),
        historical_replay=_historical_replay(),
        component_replay=_component_replay(),
        now=NOW,
        **paths,
    )
    assert sha256(paths["active_env"]) == before


def test_models_production_remains_unchanged(tmp_path):
    paths = _paths(tmp_path)
    run_audit(
        runtime_decisions=500,
        runtime_rows=_runtime_rows(),
        historical_replay=_historical_replay(),
        component_replay=_component_replay(),
        now=NOW,
        **paths,
    )
    assert sorted(item.name for item in paths["production_dir"].iterdir()) == ["README.md"]


def test_audit_safety_never_recommends_live_or_ml(tmp_path):
    paths = _paths(tmp_path)
    report = run_audit(
        runtime_decisions=500,
        runtime_rows=_runtime_rows(),
        historical_replay=_historical_replay(),
        component_replay=_component_replay(),
        now=NOW,
        **paths,
    )
    assert report["safety"]["live_mode_recommended"] is False
    assert report["safety"]["ml_enable_recommended"] is False
    assert report["recommendation"] == "FIX_REPLAY_TO_USE_RUNTIME_ENSEMBLE"
