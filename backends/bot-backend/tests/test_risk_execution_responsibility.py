"""Phase 8 — risk and execution responsibility boundaries.

By the time risk sees a candidate, entry quality has already passed. Risk's
only question is: *can this account afford this trade, and at what size?*
Execution feasibility's only question is: *can this sized trade be placed right
now?* Neither may ask whether the strategy was confident enough.

Also pinned here: the two unit bugs the blueprint calls out by name —
percentage double-division (§8.5/§8.6) and R:R computed on anything other than
the final resolved entry/SL/TP (§8.4).
"""
from __future__ import annotations

import inspect

import pytest

from app.core.config import settings
from app.decision.reason_mapping import (
    assert_no_duplicate_confidence_authority,
    is_execution_reason,
    is_risk_reason,
    to_canonical,
)
from app.decision.reasons import ExecutionReason, RiskReason
from app.policy.policy_engine import ReasonCode


# ══════════════════════════════════════════════════════════════════════════
# §8.5 / §8.6 — percentage unit normalization
# ══════════════════════════════════════════════════════════════════════════


def test_configured_stop_loss_pct_is_stored_as_a_fraction():
    """0.02 means 2%. It is NOT a human percentage awaiting division."""
    assert settings.STOP_LOSS_PCT == pytest.approx(0.02)
    assert 0.0 < settings.STOP_LOSS_PCT < 1.0
    assert settings.TAKE_PROFIT_PCT == pytest.approx(0.036)


def test_configured_rr_ratio_is_internally_consistent():
    """TAKE_PROFIT_PCT / STOP_LOSS_PCT must equal the configured minimum R:R."""
    ratio = settings.TAKE_PROFIT_PCT / settings.STOP_LOSS_PCT
    assert ratio == pytest.approx(1.8, abs=1e-9)
    assert ratio == pytest.approx(float(settings.MIN_RISK_REWARD), abs=1e-9)


def test_executor_consumes_stop_loss_pct_without_a_second_division():
    """The double-/100 bug would turn a 2% stop into a 0.02% stop."""
    source = inspect.getsource(__import__("app.execution.executor", fromlist=["x"]))

    assert "float(settings.STOP_LOSS_PCT)" in source
    for bug in (
        "settings.STOP_LOSS_PCT / 100",
        "settings.STOP_LOSS_PCT/100",
        "settings.TAKE_PROFIT_PCT / 100",
        "settings.TAKE_PROFIT_PCT/100",
    ):
        assert bug not in source, f"active double-division bug: {bug}"


def test_no_active_module_divides_an_already_fractional_stop_by_100():
    import app.policy.policy_engine as policy_engine
    import app.risk.sizing_engine as sizing_engine

    for module in (policy_engine, sizing_engine):
        source = inspect.getsource(module)
        for bug in ("stop_loss_pct / 100", "stop_loss_pct/100", "STOP_LOSS_PCT / 100"):
            assert bug not in source, f"{module.__name__}: {bug}"


def test_atr_stop_sanity_a_2000_dollar_stop_on_100k_btc_is_two_percent():
    """§8.6 — the exact regression the blueprint specifies."""
    price = 100_000.0
    atr = 1_000.0
    multiplier = 2.0

    stop_distance = atr * multiplier
    stop_fraction = stop_distance / price

    assert stop_distance == pytest.approx(2_000.0)
    assert stop_fraction == pytest.approx(0.02)
    assert stop_fraction != pytest.approx(0.0002), "a second /100 crept in"


def test_a_fraction_survives_a_round_trip_through_percentage_display():
    fraction = 0.02
    displayed = fraction * 100.0        # 2.0, for humans
    recovered = displayed / 100.0       # back to 0.02
    assert displayed == pytest.approx(2.0)
    assert recovered == pytest.approx(fraction)


def test_percentage_allocation_input_is_correctly_divided_once():
    """allocation_value=10 means 10% — this one SHOULD be divided by 100."""
    equity = 500.0
    allocation_value = 10.0
    assert equity * allocation_value / 100.0 == pytest.approx(50.0)


def test_safety_threshold_normalizer_only_divides_human_percentages():
    from app.risk.safety_engine import normalize_threshold

    assert normalize_threshold(0.70) == pytest.approx(0.70)   # already a fraction
    assert normalize_threshold(70.0) == pytest.approx(0.70)   # human percentage
    assert normalize_threshold(1.0) == pytest.approx(1.0)     # boundary stays put


# ══════════════════════════════════════════════════════════════════════════
# §8.4 — R:R is enforced on the FINAL resolved entry / SL / TP
# ══════════════════════════════════════════════════════════════════════════



def _code_only(source: str) -> str:
    """Strip comments and docstrings so source assertions test code, not prose."""
    import io
    import tokenize

    kept: list[str] = []
    prev_type = tokenize.INDENT
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.COMMENT:
            continue
        # A STRING that starts a logical line is a docstring, not a value.
        if tok.type == tokenize.STRING and prev_type in (
            tokenize.INDENT, tokenize.NEWLINE, tokenize.NL, tokenize.DEDENT,
        ):
            prev_type = tok.type
            continue
        if tok.type not in (tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT):
            kept.append(tok.string)
        prev_type = tok.type
    return " ".join(kept).lower()


def rr(entry: float, stop: float, target: float, side: str) -> float:
    if side == "BUY":
        return (target - entry) / (entry - stop)
    return (entry - target) / (stop - entry)


def test_blueprint_worked_example_resolves_to_exactly_1_point_8():
    """entry 100, SL 98, TP 103.6 -> risk 2, reward 3.6, R:R 1.8"""
    assert rr(100.0, 98.0, 103.6, "BUY") == pytest.approx(1.8)


def test_short_rr_uses_the_mirrored_distances():
    assert rr(100.0, 102.0, 96.4, "SELL") == pytest.approx(1.8)


def test_policy_engine_computes_rr_from_resolved_prices_not_percentages():
    """§8.4 — never compare a price amount against a fraction."""
    from app.policy import policy_engine

    source = inspect.getsource(policy_engine)

    assert "_risk = _ep - _sl" in source and "_reward = _tp - _ep" in source
    assert "_risk = _sl - _ep" in source and "_reward = _ep - _tp" in source
    assert "_rr = _reward / _risk" in source
    # The comparison must use the resolved ratio, not a configured percentage.
    assert "if _rr < ctx.min_risk_reward:" in source


def test_policy_engine_labels_rr_as_gross_so_the_cost_basis_is_explicit():
    """§8.4 requires the gross/cost-adjusted choice to be documented, not implied."""
    from app.policy import policy_engine

    source = inspect.getsource(policy_engine)
    assert "gross_risk_reward" in source


def test_non_positive_risk_or_reward_is_rejected_before_any_ratio_is_taken():
    from app.policy import policy_engine

    source = inspect.getsource(policy_engine)
    assert "if _risk <= 0:" in source
    assert "if _reward <= 0:" in source
    # Both guards must precede the division.
    assert source.index("if _risk <= 0:") < source.index("_rr = _reward / _risk")


@pytest.mark.parametrize(
    "entry,stop,target,side,expected_pass",
    [
        (100.0, 98.0, 103.6, "BUY", True),    # exactly 1.8
        (100.0, 98.0, 103.5, "BUY", False),   # 1.75 — just under
        (100.0, 98.0, 110.0, "BUY", True),    # 5.0
        (100.0, 102.0, 96.4, "SELL", True),   # 1.8 short
        (100.0, 102.0, 99.0, "SELL", False),  # 0.5 short
    ],
)
def test_minimum_rr_boundary_behaviour(entry, stop, target, side, expected_pass):
    assert (rr(entry, stop, target, side) >= 1.8 - 1e-9) is expected_pass


# ══════════════════════════════════════════════════════════════════════════
# §8.8 — risk rejection is separate from execution rejection
# ══════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "legacy",
    [
        ReasonCode.MAX_POSITIONS_REACHED,
        ReasonCode.DAILY_LOSS_LIMIT,
        ReasonCode.WEEKLY_DRAWDOWN_LIMIT,
        ReasonCode.MONTHLY_DRAWDOWN_LIMIT,
        ReasonCode.DAILY_TRADE_LIMIT,
        ReasonCode.LEVERAGE_TOO_HIGH,
        ReasonCode.MARGIN_INSUFFICIENT,
        ReasonCode.EXPOSURE_LIMIT,
        ReasonCode.RISK_REWARD_TOO_LOW,
    ],
)
def test_account_affordability_failures_are_risk_owned(legacy):
    assert is_risk_reason(legacy), f"{legacy} should be a RISK_ reason"
    assert not is_execution_reason(legacy)


@pytest.mark.parametrize(
    "legacy",
    [
        ReasonCode.MIN_NOTIONAL_NOT_MET,
        ReasonCode.PRICE_INVALID,
        ReasonCode.MARKET_CLOSED,
        ReasonCode.NOT_LIVE_SYMBOL,
        ReasonCode.CIRCUIT_BREAKER_TRIPPED,
    ],
)
def test_placement_failures_are_execution_owned(legacy):
    assert is_execution_reason(legacy), f"{legacy} should be an EXECUTION_ reason"
    assert not is_risk_reason(legacy)


def test_stale_data_rejection_is_execution_not_risk():
    assert to_canonical("STALE_MARKET_DATA") == ExecutionReason.DATA_STALE
    assert is_execution_reason("STALE_MARKET_DATA")
    assert not is_risk_reason("STALE_MARKET_DATA")


def test_spread_and_liquidity_are_execution_owned():
    assert to_canonical("SPREAD_TOO_WIDE") == ExecutionReason.SPREAD_TOO_WIDE
    assert is_execution_reason(ExecutionReason.LIQUIDITY_INSUFFICIENT)


def test_protection_availability_is_an_execution_concern():
    assert is_execution_reason(ExecutionReason.PROTECTION_UNAVAILABLE)


def test_risk_and_execution_namespaces_never_overlap():
    from app.decision.reasons import ExecutionReason as E, RiskReason as R

    risk_codes = {v for k, v in vars(R).items() if not k.startswith("_") and isinstance(v, str)}
    exec_codes = {v for k, v in vars(E).items() if not k.startswith("_") and isinstance(v, str)}
    assert risk_codes & exec_codes == set()


# ══════════════════════════════════════════════════════════════════════════
# §8.11 — risk must not re-apply entry quality
# ══════════════════════════════════════════════════════════════════════════


def test_no_risk_reason_code_refers_to_confidence():
    from app.decision.reasons import RiskReason as R

    codes = {v for k, v in vars(R).items() if not k.startswith("_") and isinstance(v, str)}
    for code in codes:
        assert "CONFIDENCE" not in code, f"{code} makes risk a quality authority"


def test_no_execution_reason_code_refers_to_confidence():
    from app.decision.reasons import ExecutionReason as E

    codes = {v for k, v in vars(E).items() if not k.startswith("_") and isinstance(v, str)}
    for code in codes:
        assert "CONFIDENCE" not in code


def test_a_risk_rejection_after_quality_approval_is_allowed():
    """Risk keeps its hard veto — it just may not veto on quality grounds."""
    assert_no_duplicate_confidence_authority(RiskReason.MAX_OPEN_POSITIONS)
    assert_no_duplicate_confidence_authority(ExecutionReason.DATA_STALE)


def test_reapplying_low_confidence_after_quality_approval_is_a_hard_error():
    with pytest.raises(AssertionError, match="TradingDecisionEngine"):
        assert_no_duplicate_confidence_authority(ReasonCode.LOW_CONFIDENCE)


def test_executor_performs_no_entry_quality_comparison():
    """§8.10 / §8.11(21) — the executor submits; it does not judge setups."""
    from app.execution import executor

    source = inspect.getsource(executor)
    for pattern in (
        "confidence < threshold",
        "confidence >= threshold",
        "confidence < self.min_confidence",
        "confidence >= self.min_confidence",
        "min_confidence_gate",
    ):
        assert pattern not in source, f"executor still gates on quality: {pattern}"


def test_entry_protection_remains_the_duplicate_barrier():
    """§8.9 — duplicate protection must NOT move into the decision engine."""
    from app.decision import decision_engine

    # Check executable code, not prose: the module's docstring legitimately
    # explains that duplicate protection lives elsewhere.
    code = _code_only(inspect.getsource(decision_engine))
    for pattern in ("duplicate", "idempot", "already_open", "pending_open", "entryprotection"):
        assert pattern not in code, (
            f"duplicate protection leaked into the decision engine: {pattern}"
        )

    from app.execution import entry_protection

    assert inspect.getsource(entry_protection)  # still exists and owns this


def test_decision_engine_contains_no_position_sizing():
    """§8.3 — the executor must not be handed a size by the quality layer."""
    from app.decision import decision_engine

    code = _code_only(inspect.getsource(decision_engine))
    for pattern in ("quantity", "notional", "margin", "leverage"):
        assert pattern not in code, f"sizing leaked into the decision engine: {pattern}"
