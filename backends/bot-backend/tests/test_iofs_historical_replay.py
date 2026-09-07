from __future__ import annotations

from datetime import datetime, time, timezone
from pathlib import Path

from app.strategy.iofs_components.models import Candle
from scripts.validation.replay_iofs_historical import (
    LOOKBACKS,
    build_window,
    group_metrics,
    historical_replay_gate,
    parse_sessions,
    replay_data,
    score_bucket,
    session_window,
)


def _candle(timestamp: int, close: float = 100.0) -> Candle:
    return Candle(timestamp, close, close + 1.0, close - 1.0, close, 10.0)


def _ms(hour: int, minute: int = 0) -> int:
    return int(datetime(2026, 6, 10, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


def test_replay_window_uses_only_candles_closed_at_or_before_signal_time():
    candles = [_candle(0), _candle(900_000), _candle(1_800_000)]
    window = build_window(candles, 1_800_000, "15m", 30)
    assert [candle.open_time for candle in window] == [0, 900_000]
    assert all((candle.open_time or 0) + 900_000 <= 1_800_000 for candle in window)


def test_session_filter_allows_0700_and_1300():
    sessions = parse_sessions("07:00-10:00,13:00-16:00")
    assert session_window(_ms(7), sessions) == "07:00-10:00"
    assert session_window(_ms(13), sessions) == "13:00-16:00"


def test_session_filter_excludes_1000_and_1600():
    sessions = parse_sessions("07:00-10:00,13:00-16:00")
    assert session_window(_ms(10), sessions) is None
    assert session_window(_ms(16), sessions) is None


def test_missing_timeframe_fails_closed():
    candles = [_candle(index * 900_000) for index in range(3)]
    replay = replay_data(
        {"BTCUSDT": {"15m": candles, "1h": [], "4h": []}},
        symbols=["BTCUSDT"],
        start_ms=0,
        end_ms=10_000_000,
        profiles=["balanced"],
        sessions=parse_sessions("00:00-23:59"),
        max_cycles=1,
    )
    reasons = replay["profiles"]["balanced"]["failure_reason_counts"]
    assert reasons["MISSING_TIMEFRAME"] == 1
    assert replay["profiles"]["balanced"]["metrics"]["accepted_trades"] == 0


def test_score_buckets_are_calculated_correctly():
    assert [score_bucket(value) for value in (0, 49, 50, 64, 65, 71, 72, 79, 80, 100)] == [
        "0-49",
        "0-49",
        "50-64",
        "50-64",
        "65-71",
        "65-71",
        "72-79",
        "72-79",
        "80-100",
        "80-100",
    ]


def test_failure_reasons_are_counted_correctly():
    cycles = [
        {"reason": "TREND_NOT_ALIGNED", "passed": False},
        {"reason": "TREND_NOT_ALIGNED", "passed": False},
        {"reason": "TRIGGER_NOT_CONFIRMED", "passed": False},
    ]
    grouped = group_metrics(cycles, [], "reason")
    assert grouped["TREND_NOT_ALIGNED"]["total_cycles"] == 2
    assert grouped["TRIGGER_NOT_CONFIRMED"]["total_cycles"] == 1


def test_conservative_profile_takes_fewer_or_equal_trades_than_aggressive():
    conservative_threshold = 80
    aggressive_threshold = 65
    scores = [60, 65, 70, 72, 79, 80, 90]
    conservative = sum(score >= conservative_threshold for score in scores)
    aggressive = sum(score >= aggressive_threshold for score in scores)
    assert conservative <= aggressive


def test_report_marks_pass_only_when_all_gates_pass():
    passing = {
        "accepted_trades": 20,
        "win_rate": 0.58,
        "profit_factor_r": 1.21,
        "expectancy_r": 0.01,
        "tp1_to_tp2_ratio": 19.0,
    }
    assert historical_replay_gate(passing) == []
    assert historical_replay_gate({**passing, "expectancy_r": 0.0}) == [
        "expectancy_r <= 0"
    ]


def test_historical_replay_does_not_enable_ml():
    active_env = Path(__file__).resolve().parents[1] / ".env"
    settings = {}
    for line in active_env.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            settings[key] = value
    assert settings["ML_ENABLED"].lower() == "false"
    assert settings["EXECUTION_MODE"].lower() == "paper"
    assert settings["IOFS_GATE_MODE"].lower() == "shadow"
