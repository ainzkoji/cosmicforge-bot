from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from app.strategy.base import Signal, SignalResult
from scripts.validation.replay_runtime_equivalent import (
    ReplayOptions,
    ReplayState,
    apply_friction,
    compare_old_vs_runtime,
    evaluate_symbol,
    run_runtime_equivalent_replay,
    session_allowed,
    sha256,
)


BOT_ROOT = Path(__file__).resolve().parents[1]


def _rows(count: int = 310, *, start_hour: int = 6) -> list[list[float]]:
    start = datetime(2026, 1, 1, start_hour, 0, tzinfo=timezone.utc)
    rows = []
    price = 100.0
    for index in range(count):
        ts = int((start + timedelta(minutes=15 * index)).timestamp() * 1000)
        # Tight candles avoid immediate TP/SL and make no-overlap deterministic.
        rows.append([ts, price, price + 0.05, price - 0.05, price, 1000.0])
    return rows


class FakeMasterEnsemble:
    calls: list[dict] = []

    def __init__(self, client, *, signal_indices: set[int] | None = None):
        self.client = client
        self.signal_indices = signal_indices or {249}

    def get_signal(self, symbol: str, **kwargs):
        allowed, _ = self._check_session_gate([])
        last_open = int(self.client.rows[-1][0])
        FakeMasterEnsemble.calls.append(
            {
                "symbol": symbol,
                "kwargs": kwargs,
                "last_open": last_open,
                "window_len": len(self.client.rows),
                "allowed": allowed,
            }
        )
        idx = len(FakeMasterEnsemble.calls) - 1
        if not allowed:
            return SignalResult(
                Signal.HOLD,
                0.0,
                "SESSION_BLOCKED",
                {"regime": "WEAK_TREND", "execution_block_reason": "SESSION_BLOCKED"},
            )
        if idx in self.signal_indices:
            return SignalResult(
                Signal.BUY,
                0.9,
                "master_ensemble_v2",
                {
                    "regime": "STRONG_TREND",
                    "buy_score": 0.9,
                    "sell_score": 0.0,
                    "threshold": 0.55,
                    "component_breakdown": [{"component_name": "supertrend"}],
                },
            )
        return SignalResult(
            Signal.HOLD,
            0.0,
            "master_ensemble_v2",
            {"regime": "WEAK_TREND", "threshold": 0.55, "hold_reason": "NO_PATTERN"},
        )


def _factory(signals: set[int] | None = None):
    def build(client):
        return FakeMasterEnsemble(client, signal_indices=signals)

    return build


def _adaptive(**_kwargs):
    return {
        "min_confidence_gate": 0.55,
        "strategy_weight_adjustments": {"supertrend": 0.75},
    }


def _options(**overrides) -> ReplayOptions:
    data = {
        "symbols": ["BTCUSDT"],
        "start_date": date(2026, 1, 1),
        "end_date": date(2026, 1, 5),
        "session_windows": "00:00-23:00",
        "candle_mode": "closed",
        "fees_bps": 4,
        "slippage_bps": 2,
        "no_overlap": True,
        "max_daily_trades": 99,
    }
    data.update(overrides)
    return ReplayOptions(**data)


def test_runtime_equivalent_replay_calls_master_ensemble_path():
    FakeMasterEnsemble.calls = []
    state = ReplayState()
    evaluate_symbol(
        symbol="BTCUSDT",
        rows=_rows(),
        options=_options(),
        state=state,
        strategy_factory=_factory({0}),
        adaptive_provider=_adaptive,
    )
    assert state.master_ensemble_calls > 0
    assert FakeMasterEnsemble.calls


def test_closed_candle_mode_uses_only_closed_candles_and_next_open_entry():
    FakeMasterEnsemble.calls = []
    rows = _rows()
    state = ReplayState()
    evaluate_symbol(
        symbol="BTCUSDT",
        rows=rows,
        options=_options(candle_mode="closed"),
        state=state,
        strategy_factory=_factory({0}),
        adaptive_provider=_adaptive,
    )
    assert state.trades[0]["signal_open_time"] == datetime.fromtimestamp(rows[249][0] / 1000, timezone.utc).isoformat()
    assert state.trades[0]["entry_time"] == datetime.fromtimestamp(rows[250][0] / 1000, timezone.utc).isoformat()
    assert FakeMasterEnsemble.calls[0]["last_open"] == rows[249][0]


def test_replay_does_not_use_future_candles_in_closed_mode():
    FakeMasterEnsemble.calls = []
    rows = _rows()
    state = ReplayState()
    evaluate_symbol(
        symbol="BTCUSDT",
        rows=rows,
        options=_options(candle_mode="closed"),
        state=state,
        strategy_factory=_factory({0}),
        adaptive_provider=_adaptive,
    )
    assert FakeMasterEnsemble.calls[0]["last_open"] < rows[250][0]
    assert FakeMasterEnsemble.calls[0]["window_len"] == 250


def test_adaptive_component_multipliers_are_applied():
    FakeMasterEnsemble.calls = []
    state = ReplayState()
    evaluate_symbol(
        symbol="BTCUSDT",
        rows=_rows(),
        options=_options(),
        state=state,
        strategy_factory=_factory({0}),
        adaptive_provider=_adaptive,
    )
    assert FakeMasterEnsemble.calls[0]["kwargs"]["strategy_weight_adjustments"] == {
        "supertrend": 0.75
    }


def test_session_filter_matches_runtime_window():
    assert session_allowed(
        int(datetime(2026, 1, 1, 6, 0, tzinfo=timezone.utc).timestamp() * 1000),
        "06:00-19:00",
    )
    assert not session_allowed(
        int(datetime(2026, 1, 1, 19, 0, tzinfo=timezone.utc).timestamp() * 1000),
        "06:00-19:00",
    )


def test_no_overlap_skips_overlapping_trades():
    FakeMasterEnsemble.calls = []
    state = ReplayState()
    evaluate_symbol(
        symbol="BTCUSDT",
        rows=_rows(360),
        options=_options(no_overlap=True),
        state=state,
        strategy_factory=_factory({0, 1, 2, 3, 4, 5}),
        adaptive_provider=_adaptive,
    )
    assert state.trades
    assert state.skipped["overlap"] > 0


def test_fees_and_slippage_reduce_expectancy():
    result = apply_friction(
        {"r_multiple": 1.0},
        entry=100.0,
        risk=10.0,
        fees_bps=4,
        slippage_bps=2,
    )
    assert result["r_multiple"] < result["gross_r_multiple"]


def test_old_replay_opportunities_can_be_marked_missing_with_reasons():
    state = ReplayState()
    state.cycles.append(
        {
            "symbol": "BTCUSDT",
            "signal_time": "2026-01-01T08:00:00+00:00",
            "action": "HOLD",
            "confidence": 0.2,
            "threshold": 0.55,
        }
    )
    result = compare_old_vs_runtime(
        [{"symbol": "BTCUSDT", "side": "BUY", "signal_time": "2026-01-01T08:00:00+00:00"}],
        state,
    )
    assert result["missing_old_opportunities"][0]["missing_reason"] == "confidence_below_floor"


def test_report_is_generated_and_safety_holds(tmp_path):
    FakeMasterEnsemble.calls = []
    env_hash = sha256(BOT_ROOT / ".env")
    report = run_runtime_equivalent_replay(
        options=_options(),
        output_md=tmp_path / "runtime_equivalent.md",
        output_json=tmp_path / "runtime_equivalent.json",
        save_setups=tmp_path / "setups.jsonl",
        rows_by_symbol_input={"BTCUSDT": _rows()},
        old_opportunities=[],
        strategy_factory=_factory({0}),
        adaptive_provider=_adaptive,
    )
    assert (tmp_path / "runtime_equivalent.md").exists()
    assert (tmp_path / "runtime_equivalent.json").exists()
    assert (tmp_path / "setups.jsonl").exists()
    assert report["uses_master_ensemble"] is True
    assert sha256(BOT_ROOT / ".env") == env_hash
    assert report["safety"]["ml_disabled"] is True
    assert report["safety"]["live_mode_enabled"] is False


def test_runtime_current_mode_records_current_open_as_entry(tmp_path):
    FakeMasterEnsemble.calls = []
    report = run_runtime_equivalent_replay(
        options=_options(candle_mode="runtime-current"),
        output_md=tmp_path / "runtime_current.md",
        output_json=tmp_path / "runtime_current.json",
        rows_by_symbol_input={"BTCUSDT": _rows()},
        old_opportunities=[],
        strategy_factory=_factory({0}),
        adaptive_provider=_adaptive,
    )
    trade = report["accepted_trades"][0]
    assert trade["signal_open_time"] == trade["entry_open_time"]
    assert report["runtime_candle_timing_recommendation"] == "AUDIT_RUNTIME_CANDLE_TIMING_MORE"
