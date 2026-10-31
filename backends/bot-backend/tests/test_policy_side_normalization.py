"""Pre-Section-22 closure: ONE canonical BUY/SELL boundary for PolicyEngine.

Root cause fixed: TradingOrchestrator passed ``Signal.value`` ("buy"/"sell")
into PolicyContext while PolicyEngine compares against "BUY"/"SELL", so
``is_new_open`` was always False on the orchestrator path and the
max-open-position, R:R and ATR noise-floor / ATR risk-cap gates never ran --
for V2 and for the CATI TradePlan path alike.

The scalar V2 entry (``process_trading_opportunity``) is retired; the ONE
entry into hard risk is ``TradingOrchestrator.process_trade_plan`` (a real,
hash-verified Section 18 TradePlan -> ``_evaluate_hard_risk`` -> real
SafetyEngine + real PolicyEngine). These tests drive that path and prove each
gate fires on it.
"""
from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

import pytest

from app.models.unified_trading import Side
from app.policy import policy_engine as pe
from app.policy.policy_engine import (
    InvalidPolicySignal, PolicyContext, ReasonCode, normalize_policy_signal, normalize_trade_side,
)
from app.strategy.strategy_framework import Signal

_TI = Path(__file__).resolve().parent / "trading_intelligence"
if str(_TI) not in sys.path:
    sys.path.append(str(_TI))


@pytest.fixture(scope="module", autouse=True)
def _migrated():
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    migrate(DB())


# ============================== normalization ==============================
@pytest.mark.parametrize("raw,expected", [
    ("buy", "BUY"), ("BUY", "BUY"), ("Buy", "BUY"), (" buy ", "BUY"), (Signal.BUY, "BUY"), (Side.BUY, "BUY"),
    ("sell", "SELL"), ("SELL", "SELL"), ("Sell", "SELL"), (Signal.SELL, "SELL"), (Side.SELL, "SELL"),
])
def test_trade_side_normalization(raw, expected):
    assert normalize_trade_side(raw) == expected
    assert PolicyContext(symbol="BTCUSDT", signal=raw).signal == expected


@pytest.mark.parametrize("bad", ["hold", "longish", "", "   ", None, 1, "LONG"])
def test_invalid_trade_side_fails_closed(bad):
    with pytest.raises(InvalidPolicySignal):
        normalize_trade_side(bad)


@pytest.mark.parametrize("bad", ["longish", "", None, 3.0, "BUYX"])
def test_invalid_policy_signal_cannot_build_a_context(bad):
    with pytest.raises(InvalidPolicySignal):
        PolicyContext(symbol="BTCUSDT", signal=bad)


def test_non_directional_signals_stay_canonical():
    """HOLD / CLOSE are legitimate PolicyEngine signals (the runner routes them);
    they are normalized, never mapped to a trade side."""
    assert normalize_policy_signal("hold") == "HOLD" and normalize_policy_signal(Signal.HOLD) == "HOLD"
    assert PolicyContext(symbol="X", signal="close").signal == "CLOSE"


# ============================== real orchestrator gates (TradePlan path) ==============================
# The harness plan: LONG BTCUSDT, entry 100, structural invalidation 95 (5 % stop),
# first target 110 (R:R 2.0); a 120 USDT fixed margin.
@pytest.fixture
def h(tmp_path):
    from _exec import Harness

    return Harness(tmp_path)


def _mirrored_short(plan):
    """The harness plan mirrored around 100 to a SHORT (stop 105, targets below), re-hashed through
    ``TradePlan.build`` so it passes the Section 20.3 integrity check like any real plan."""
    from app.trading_intelligence.contracts.trade_plan import TradePlan

    defaults = {k: f.default for k, f in TradePlan.__dataclass_fields__.items() if f.default is not dataclasses.MISSING}
    fields = {k: getattr(plan, k) for k in TradePlan.__dataclass_fields__
              if k not in TradePlan._NON_ANALYTICAL and not (k in defaults and getattr(plan, k) == defaults[k])}
    zone = plan.allowed_entry_zone
    fields.update(
        side="SHORT", structural_invalidation_price=200.0 - plan.structural_invalidation_price,
        allowed_entry_zone=dataclasses.replace(zone, minimum_price=200.0 - zone.maximum_price,
                                               maximum_price=200.0 - zone.minimum_price),
        target_zones=tuple(dataclasses.replace(z, price_low=200.0 - z.price_high, price_high=200.0 - z.price_low)
                           for z in plan.target_zones))
    return TradePlan.build(**fields)


def _run(h, plan=None, *, equity=50_000.0, open_positions=0, atr=2.0, **kw):
    from app.trading_intelligence.contracts.system_health import BrokerHealthContext
    from app.trading_intelligence.trade_plan.validation import MarketReference

    plan = plan or h.plan
    return h.orch.process_trade_plan(
        plan, now_ms=h.now, market_reference=MarketReference(100.0, h.now - 100, 1.0),
        broker_health=BrokerHealthContext(plan.broker_account_id, plan.venue, plan.environment, "HEALTHY", h.now, "t"),
        reservation_state=h.reservations.get(plan.portfolio_reservation_id),
        venue_capabilities=h.kw["evaluated"].venue_observation.execution_capabilities, klines=[],
        current_equity=equity, margin_used=0.0, margin_available=equity, open_positions=open_positions, atr=atr,
        **kw)


def _code(result):
    return result["details"].get("reason_code")


def _blocked_by(result, code):
    """Blocked by THIS PolicyEngine gate, with the CATI risk evidence naming it."""
    return (result["decision"] == "blocked" and _code(result) == code
            and result["risk_decision"].approved is False and code in result["risk_decision"].reason_codes)


def _spy(monkeypatch, orch):
    seen = []
    real = orch.policy_engine.evaluate

    def evaluate(ctx):
        decision = real(ctx)
        seen.append((ctx, decision))
        return decision

    monkeypatch.setattr(orch.policy_engine, "evaluate", evaluate)
    return seen


def test_plan_path_reaches_policy_engine_with_canonical_side(h, monkeypatch):
    long_plan, short_plan = h.plan, _mirrored_short(h.plan)
    assert (long_plan.side, short_plan.side) == ("LONG", "SHORT")
    for plan, expected in ((long_plan, "BUY"), (short_plan, "SELL")):
        seen = _spy(monkeypatch, h.orch)
        res = _run(h, plan)
        assert res["decision"] == "execute", res["reason"]
        assert [ctx.signal for ctx, _ in seen] == [expected]
        assert res["trade_params"]["side"] == expected


def test_max_open_position_gate_now_fires_on_the_real_path(h):
    """Before the fix this exact call was APPROVED (is_new_open was False)."""
    res = _run(h, open_positions=5, max_open_positions=5)
    assert _blocked_by(res, ReasonCode.MAX_POSITIONS_REACHED.value)
    assert _run(h, open_positions=4, max_open_positions=5)["decision"] == "execute"


def test_risk_reward_gate_now_fires(h):
    # the plan's first target is 2.0 x its structural stop distance, so R:R = 2.0 on this path
    res = _run(h, min_risk_reward=3.0)
    assert _blocked_by(res, ReasonCode.RISK_REWARD_TOO_LOW.value)
    assert _run(h, min_risk_reward=2.0)["decision"] == "execute"


def test_atr_noise_floor_gate_now_fires(h):
    res = _run(h, atr=12.0)  # stop distance 5.0 < 0.5 x ATR 12.0
    assert _blocked_by(res, ReasonCode.STOP_BELOW_ATR_NOISE_FLOOR.value)
    assert _run(h, atr=9.0)["decision"] == "execute"  # 5.0 >= 0.5 x 9.0


def test_missing_atr_gate_now_fires(h):
    res = _run(h, atr=0.0)
    assert _blocked_by(res, ReasonCode.MISSING_ATR_FOR_FIXED_SIZING.value)


def test_atr_risk_cap_gate_now_fires(h, monkeypatch):
    """The ATR risk cap is evaluated on the plan path's canonical context.

    On ``process_trade_plan`` every allocation enters PolicyEngine as a FIXED margin
    (the user's fixed amount, or a risk-based deployment's already-sized margin), and
    for a fixed margin the cap REPORTS the loss at stop (the user's margin is
    authoritative; risk is governed by leverage, compound risk and the account
    limits). The cap's BLOCK applies to a risk-sized ("atr_risk") allocation: the
    same canonical context, sized that way, is rejected by the real PolicyEngine.
    """
    seen = _spy(monkeypatch, h.orch)
    _run(h, equity=200.0)  # 120 USDT x 4x to a 5 % stop: ~24 USDT at stop, far above 1 % of 200
    (ctx, decision), = seen
    assert ctx.signal == "BUY" and ctx.trade_amount_mode == "fixed" and ctx.trade_amount_value == 120.0
    diagnostic = decision.details["fixed_margin_loss_diagnostic"]
    assert diagnostic["status"] == "REPORTED_FIXED_MARGIN_AUTHORITATIVE"
    assert diagnostic["estimated_loss_at_stop"] > diagnostic["per_trade_risk_reference"]

    engine = pe.PolicyEngine()
    risk_sized = dataclasses.replace(ctx, trade_amount_mode="atr_risk")
    blocked = engine.evaluate(risk_sized)
    assert not blocked.allowed and blocked.reason_code == ReasonCode.ATR_RISK_CAP_EXCEEDED
    assert blocked.details["estimated_loss_at_stop"] > blocked.details["max_allowed_loss"]
    # the gate keys on the canonical side: a lowercase "buy" normalizes and is still capped
    lower = engine.evaluate(dataclasses.replace(risk_sized, signal="buy"))
    assert lower.reason_code == ReasonCode.ATR_RISK_CAP_EXCEEDED
    # control: the same risk-sized trade within the allowance is not capped
    ok = engine.evaluate(dataclasses.replace(risk_sized, equity=50_000.0))
    assert ok.reason_code != ReasonCode.ATR_RISK_CAP_EXCEEDED


def test_thresholds_and_signals_are_untouched():
    """The fix changes WHICH checks run, never their thresholds or the signal."""
    ctx = PolicyContext(symbol="BTCUSDT", signal="buy")
    defaults = PolicyContext.__dataclass_fields__
    assert ctx.min_stop_atr_multiplier == defaults["min_stop_atr_multiplier"].default
    assert ctx.max_open_positions == defaults["max_open_positions"].default
    assert pe.POLICY_SIGNALS == ("BUY", "SELL", "HOLD", "CLOSE")
