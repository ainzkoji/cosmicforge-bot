"""Expert failures are ERROR, never HOLD -- and every expert honours the snapshot contract.

Found live: ``sma_cross`` called ``client.klines(symbol, ...)`` positionally while
``SnapshotMarketClient.klines`` is keyword-only. From 2026-09-08 every
evaluation raised a TypeError that the expert caught and returned as
``HOLD 0.0 "data_error:..."``, and ``expert_evaluations`` recorded it as
``eligible=1, executed=1, signal=HOLD, weight=0.9`` -- a system failure booked
as a neutral opinion. ``vwap_reversion`` had the same defect in a different
shape: it reads 5m candles, which the snapshot never carried.

Policy, chosen as the safer deterministic option: an eligible expert in ERROR
fails the whole candle closed (``EXPERT_EVALUATION_ERROR``). It is not dropped
from the agreement denominator -- that would *raise* agreement and lower the bar
on the strength of a failure.
"""
from __future__ import annotations

import random
from types import SimpleNamespace

import pytest

import app.strategy.master_ensemble as ensemble_module
from app.runner.market_snapshot import MarketSnapshot, SnapshotMarketClient
from app.strategy.master_ensemble import MasterEnsembleStrategy
from app.strategy.regime import MarketRegime
from app.threshold.contracts import ExpertEvidence, ThresholdStatus, experts_from_votes
from app.threshold.engine import REASON_EXPERT_ERROR, AdaptiveEntryThresholdEngine
from app.threshold.policy import resolve_threshold_policy

SYMBOL = "BTCUSDT"
#: 2025-12-31T23:59:59.999Z: aligned to 5m, 15m and 4h boundaries.
PRIMARY_CLOSE = 1767225599999
EXPERTS = (
    "supertrend", "trend_pullback", "vwap_reversion", "squeeze_breakout",
    "bollinger_reversion", "donchian_breakout", "sma_cross",
)


def candles(n: int, interval_ms: int, end_close_ms: int = PRIMARY_CLOSE, *, seed: int = 11):
    """Deterministic Binance-shaped klines ending at ``end_close_ms``."""
    rnd = random.Random(seed)
    rows, price = [], 100.0
    first_open = end_close_ms + 1 - n * interval_ms
    for i in range(n):
        open_ = price
        close = open_ * (1 + 0.0004 + rnd.uniform(-0.004, 0.004))
        high = max(open_, close) * (1 + rnd.uniform(0, 0.002))
        low = min(open_, close) * (1 - rnd.uniform(0, 0.002))
        open_time = first_open + i * interval_ms
        rows.append([open_time, f"{open_}", f"{high}", f"{low}", f"{close}",
                     f"{1000 + rnd.uniform(0, 500)}", open_time + interval_ms - 1,
                     "0", 0, "0", "0", "0"])
        price = close
    return rows


def snapshot(*, with_5m: bool = True) -> MarketSnapshot:
    return MarketSnapshot.build(
        symbol=SYMBOL, timeframe="15m", candles=candles(250, 15 * 60_000),
        source="test", higher_timeframe="4h",
        higher_timeframe_candles=candles(250, 4 * 3_600_000, seed=12),
        auxiliary_candles={"5m": candles(300, 5 * 60_000, seed=13)} if with_5m else None,
    )


class NoNetwork:
    """A delegate that fails the test if an expert reads outside the snapshot."""

    def __getattr__(self, name):
        raise AssertionError(f"expert reached past the snapshot: {name}")


def _is_error(reason: str) -> bool:
    return str(reason or "").lower().startswith(("error:", "data_error:", "strategy_error"))


# ── Every expert honours the SnapshotMarketClient contract ──────────────────


@pytest.mark.parametrize("name", EXPERTS)
def test_every_expert_runs_against_the_snapshot_client(name):
    strategies = MasterEnsembleStrategy(client=NoNetwork())._strategies
    assert set(strategies) == set(EXPERTS)
    expert = strategies[name]
    expert.client = SnapshotMarketClient(NoNetwork(), snapshot())
    result = expert.get_signal(SYMBOL)
    assert not _is_error(result.reason), f"{name}: {result.reason}"


def test_sma_cross_uses_the_keyword_contract():
    expert = MasterEnsembleStrategy(client=NoNetwork())._strategies["sma_cross"]
    expert.client = SnapshotMarketClient(NoNetwork(), snapshot())
    result = expert.get_signal(SYMBOL)
    assert "takes 1 positional argument" not in result.reason
    assert not result.reason.startswith("data_error")


def test_without_its_timeframe_vwap_reports_an_error_rather_than_an_opinion():
    expert = MasterEnsembleStrategy(client=NoNetwork())._strategies["vwap_reversion"]
    expert.client = SnapshotMarketClient(NoNetwork(), snapshot(with_5m=False))
    result = expert.get_signal(SYMBOL)
    assert _is_error(result.reason)
    assert "snapshot_timeframe_unavailable:5m" in result.reason


def test_the_ensemble_declares_the_extra_timeframes_its_experts_read():
    assert MasterEnsembleStrategy(client=NoNetwork()).snapshot_timeframes == ("5m",)


def test_auxiliary_series_are_cut_at_the_decision_candle():
    late = candles(10, 5 * 60_000, end_close_ms=PRIMARY_CLOSE + 30 * 60_000, seed=5)
    snap = MarketSnapshot.build(
        symbol=SYMBOL, timeframe="15m", candles=candles(250, 15 * 60_000),
        source="test", auxiliary_candles={"5m": late},
    )
    assert all(int(row[6]) <= PRIMARY_CLOSE for row in snap.auxiliary_candles["5m"])
    assert len(snap.auxiliary_candles["5m"]) == 4  # the 6 later candles are not visible


def test_a_snapshot_without_auxiliary_series_keeps_its_fingerprint():
    import hashlib

    snap = snapshot(with_5m=False)
    legacy = hashlib.sha256(repr(
        (snap.symbol, snap.timeframe, snap.candles, snap.higher_timeframe_candles)
    ).encode()).hexdigest()[:32]
    assert snap.data_hash == legacy


# ── ERROR is a distinct expert outcome ──────────────────────────────────────


def test_an_errored_expert_is_recorded_as_error_with_its_eligibility():
    evidence = {
        e.strategy: e for e in experts_from_votes(
            [("supertrend", "BUY", 0.6), ("trend_pullback", "HOLD", 0.0)],
            eligible=["supertrend", "trend_pullback", "sma_cross"],
            all_strategies=list(EXPERTS),
            weights={"sma_cross": 0.9, "supertrend": 1.5, "trend_pullback": 1.3},
            errors={"sma_cross": "data_error:boom\nwith a traceback"},
        )
    }
    failed = evidence["sma_cross"]
    assert failed.signal == "ERROR"
    assert failed.signal != "HOLD"
    assert failed.eligible is True
    assert failed.executed is True
    assert failed.confidence == 0.0
    assert failed.weighted_contribution == 0.0
    assert not failed.is_directional
    assert "\n" not in failed.reason and failed.reason.startswith("data_error:boom")
    assert evidence["trend_pullback"].signal == "HOLD"
    assert evidence["vwap_reversion"].signal == "DISABLED"


def _request(experts):
    from app.threshold.contracts import AdaptiveThresholdInput, RegimeContext, VolatilityContext

    return AdaptiveThresholdInput(
        bot_instance_id="bot_err", symbol=SYMBOL, timeframe="15m",
        closed_candle_time=PRIMARY_CLOSE, side="BUY", opportunity_confidence=0.95,
        experts=tuple(experts),
        regime=RegimeContext(regime="WEAK_TREND", regime_confidence=0.9),
        volatility=VolatilityContext(atr_percentile=0.5),
    )


def test_the_threshold_engine_fails_an_errored_candle_closed():
    experts = [
        ExpertEvidence("supertrend", True, True, "BUY", 0.95, weight=1.5),
        ExpertEvidence("sma_cross", True, True, "ERROR", 0.0, weight=0.9, reason="data_error:x"),
    ]
    decision = AdaptiveEntryThresholdEngine().evaluate(
        _request(experts), resolve_threshold_policy(base_threshold=0.30),
    )
    assert decision.status == ThresholdStatus.ERROR
    assert decision.reason == REASON_EXPERT_ERROR
    assert decision.final_threshold is None
    assert decision.passed is None
    assert decision.expert_agreement_score is None, "agreement is never computed around a failure"
    assert "sma_cross" in decision.detail


def test_an_ineligible_expert_in_error_does_not_block():
    experts = [
        ExpertEvidence("supertrend", True, True, "BUY", 0.95, weight=1.5),
        ExpertEvidence("vwap_reversion", False, False, "ERROR", 0.0, weight=1.2),
    ]
    decision = AdaptiveEntryThresholdEngine().evaluate(
        _request(experts), resolve_threshold_policy(base_threshold=0.30),
    )
    assert decision.status == ThresholdStatus.EVALUATED


# ── The ensemble end to end ─────────────────────────────────────────────────


class FakeExpert:
    client = None
    interval = "15m"

    def __init__(self, signal: str, confidence: float, reason: str):
        self._result = ensemble_module.SignalResult(
            ensemble_module.Signal[signal], confidence, reason,
        )

    def get_signal(self, symbol):
        return self._result


class FixedClassifier:
    def __init__(self, regime: MarketRegime):
        self.regime = regime

    def classify_stable(self, highs, lows, closes):
        return SimpleNamespace(
            regime=self.regime, regime_confidence=0.9, adx=25.0, atr_percent=0.5,
            ma_slope=0.01, compression_ratio=0.2, breakout_pressure=0.1,
        )


def _ensemble(monkeypatch, regime: MarketRegime, experts: dict[str, FakeExpert]):
    ensemble = MasterEnsembleStrategy(client=NoNetwork())
    monkeypatch.setattr(ensemble, "_get_classifier", lambda symbol: FixedClassifier(regime))
    monkeypatch.setattr(ensemble, "_check_volatility_spike", lambda *a: (False, 3.0))
    ensemble._strategies.update(experts)
    return ensemble


def _evaluate(ensemble):
    return ensemble.get_signal(
        SYMBOL, market_snapshot=snapshot(), market_type="CRYPTO",
        bot_instance_id="bot_err", timeframe="15m",
    )


def test_a_broken_expert_fails_the_candle_closed_and_is_not_a_hold_vote(monkeypatch):
    ensemble = _ensemble(monkeypatch, MarketRegime.WEAK_TREND, {
        "supertrend": FakeExpert("BUY", 0.9, "supertrend_ema"),
        "trend_pullback": FakeExpert("HOLD", 0.0, "no_signal"),
        "donchian_breakout": FakeExpert("HOLD", 0.0, "no_signal"),
        "sma_cross": FakeExpert("HOLD", 0.0, "data_error:klines() takes 1 positional argument"),
    })
    result = _evaluate(ensemble)

    assert result.signal == ensemble_module.Signal.HOLD
    assert result.reason == REASON_EXPERT_ERROR
    assert ensemble.last_threshold_decision.status == ThresholdStatus.ERROR
    evidence = {e.strategy: e for e in ensemble.last_expert_evidence}
    assert evidence["sma_cross"].signal == "ERROR"
    assert evidence["sma_cross"].eligible is True
    components = {c["strategy"]: c for c in result.meta["component_breakdown"]}
    assert components["sma_cross"]["component_signal"] == "ERROR"


def test_every_expert_failing_is_still_an_error_candle(monkeypatch):
    ensemble = _ensemble(monkeypatch, MarketRegime.WEAK_TREND, {
        name: FakeExpert("HOLD", 0.0, "error:down")
        for name in ("supertrend", "trend_pullback", "donchian_breakout", "sma_cross")
    })
    result = _evaluate(ensemble)
    assert result.reason == REASON_EXPERT_ERROR
    assert ensemble.last_threshold_decision.status == ThresholdStatus.ERROR


def test_healthy_experts_are_not_affected(monkeypatch):
    ensemble = _ensemble(monkeypatch, MarketRegime.WEAK_TREND, {
        "supertrend": FakeExpert("BUY", 0.9, "supertrend_ema"),
        "trend_pullback": FakeExpert("HOLD", 0.0, "no_signal"),
        "donchian_breakout": FakeExpert("HOLD", 0.0, "no_signal"),
        "sma_cross": FakeExpert("HOLD", 0.0, "no_cross"),
    })
    result = _evaluate(ensemble)
    assert result.reason != REASON_EXPERT_ERROR
    assert ensemble.last_threshold_decision.status == ThresholdStatus.EVALUATED
    evidence = {e.strategy: e for e in ensemble.last_expert_evidence}
    assert evidence["sma_cross"].signal == "HOLD"
