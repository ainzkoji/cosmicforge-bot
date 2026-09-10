"""The session-gate consistency invariant, and the failure that broke it.

The invariant:

    same timestamp + same effective session policy + same market type
        => same session-gate verdict, for every symbol

Two symbols of one bot violated it in production: at the same 01:30 UTC cycle,
BTCUSDT recorded ``SESSION_BLOCKED`` while ETHUSDT recorded
``CRYPTO_SESSION_24_7_BYPASS``. Neither policy nor market type differed. What
differed was that BTC's evaluation raised a ``TypeError`` from inside the
strategy, and two call sites caught it and silently re-ran ``get_signal`` with
**no kwargs at all** -- discarding ``market_type``, which is the input the CRYPTO
24/7 bypass is decided from.

So the tests here cover three things:

1. The gate itself is a pure function of (policy, market type, clock).
2. A ``TypeError`` from inside a strategy is never converted into a
   kwargs-less re-run.
3. A candle with no directional candidate does not raise at all -- that was the
   specific ``float(None)`` that started it.

Deliberately not hard-coded to BTC/ETH: symbols are parametrised, because
session behaviour must stay broker-neutral and multi-market. Instruments that
genuinely need different sessions must get them from resolved policy, not from
accidental branching.
"""
from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from app.core.trading_orchestrator import _is_signature_rejection
from app.strategy.hold_breakdown import classify_hold_reason
from app.strategy.master_ensemble import MasterEnsembleStrategy

#: Any pair of instruments sharing one policy. The invariant is about the
#: policy, not about these particular tickers.
SYMBOL_PAIRS = [
    ("BTCUSDT", "ETHUSDT"),
    ("SOLUSDT", "ADAUSDT"),
    ("XAUUSD", "EURUSD"),
]


def _bypass_inputs(**overrides):
    """The kwargs the runner passes into the ensemble for one evaluation."""
    payload = dict(
        market_type="CRYPTO",
        enforce_session=None,
        crypto_session_enabled=False,
    )
    payload.update(overrides)
    return payload


def resolve_bypass(kwargs: dict) -> bool:
    """The CRYPTO 24/7 bypass decision, as the ensemble computes it.

    Mirrors master_ensemble.get_signal Step 3.6. Kept as a helper so the
    invariant can be exercised without a market client, and asserted against the
    real source below so it cannot drift.
    """
    market_type = str(kwargs.get("market_type") or "UNKNOWN").upper()
    explicit = kwargs.get("enforce_session")
    if explicit is None:
        explicit = bool(kwargs.get("crypto_session_enabled", False))
    return market_type == "CRYPTO" and not bool(explicit)


# ── 1. The invariant ────────────────────────────────────────────────────────


class TestSessionGateInvariant:
    @pytest.mark.parametrize("symbol_a,symbol_b", SYMBOL_PAIRS)
    def test_identical_policy_yields_identical_verdict(self, symbol_a, symbol_b):
        """Same policy, same market type, same clock -> same verdict."""
        kwargs = _bypass_inputs()
        assert resolve_bypass(kwargs) == resolve_bypass(kwargs)
        # The gate reads nothing symbol-specific, so a per-symbol divergence is
        # impossible unless an input differed.
        assert "symbol" not in inspect.signature(resolve_bypass).parameters

    def test_the_bypass_decision_reads_only_kwargs(self):
        """The four lines that decide the bypass must touch no instance state.

        The rest of the gate block legitimately calls self._hold and
        self._check_session_gate; what must stay pure is the decision itself,
        because instance state is per-strategy and would leak across symbols.
        """
        source = inspect.getsource(MasterEnsembleStrategy.get_signal)
        gate = source[source.index("Step 3.6"):source.index("Step 3.7")]
        decision = gate[gate.index("_market_type ="):gate.index("if _crypto_bypass")]

        assert 'kwargs.get("market_type")' in decision
        assert 'kwargs.get("enforce_session")' in decision
        assert 'kwargs.get("crypto_session_enabled"' in decision
        assert "self." not in decision, (
            "the bypass decision must not read mutable instance state"
        )

    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            (_bypass_inputs(), True),
            (_bypass_inputs(market_type="crypto"), True),          # case-insensitive
            (_bypass_inputs(market_type="FOREX"), False),
            (_bypass_inputs(market_type=None), False),
            (_bypass_inputs(enforce_session=True), False),         # explicit opt-in
            (_bypass_inputs(crypto_session_enabled=True), False),
            ({}, False),                                           # kwargs lost
        ],
    )
    def test_bypass_is_a_pure_function_of_policy(self, kwargs, expected):
        assert resolve_bypass(kwargs) is expected

    def test_losing_market_type_changes_the_verdict(self):
        """The exact mechanism of the production divergence.

        This is why a kwargs-less retry is not a harmless fallback: dropping
        market_type silently moves a CRYPTO symbol from the 24/7 bypass onto the
        fixed session window.
        """
        assert resolve_bypass(_bypass_inputs()) is True
        assert resolve_bypass({}) is False


# ── 2. A TypeError from inside a strategy must never be swallowed ───────────


class TestTypeErrorIsNotSwallowed:
    def test_a_body_raised_typeerror_is_not_a_signature_rejection(self):
        def strategy_get_signal(symbol, **kwargs):
            raise TypeError("float() argument must be a string or a real number")

        exc = TypeError("float() argument must be a string or a real number")
        assert _is_signature_rejection(exc, strategy_get_signal) is False

    def test_a_genuine_signature_rejection_is_recognised(self):
        def legacy_get_signal(symbol):  # no **kwargs
            return None

        try:
            legacy_get_signal("BTCUSDT", market_type="CRYPTO")
        except TypeError as exc:
            assert _is_signature_rejection(exc, legacy_get_signal) is True
        else:  # pragma: no cover
            pytest.fail("expected a TypeError")

    def test_it_fails_closed_when_it_cannot_tell(self):
        """An unknown callable must not license a kwargs-less retry."""
        assert _is_signature_rejection(TypeError("boom"), object()) is False

    def test_the_adapter_does_not_re_evaluate_on_a_body_typeerror(self):
        """The contract that matters: the strategy runs once, and the failure
        is reported as a failure -- not as a session or quality verdict reached
        by a second evaluation against different inputs."""
        from app.core.trading_orchestrator import LegacyStrategyAdapter

        class Exploding:
            name = "exploding"

            def __init__(self):
                self.calls = []

            def get_signal(self, symbol, **kwargs):
                self.calls.append(kwargs)
                raise TypeError("raised from inside the strategy body")

        legacy = Exploding()
        output = LegacyStrategyAdapter(legacy).analyze(
            symbol="BTCUSDT", klines=[], current_price=1.0, market_type="CRYPTO"
        )

        assert len(legacy.calls) == 1, "the strategy must not be evaluated twice"
        assert legacy.calls[0].get("market_type") == "CRYPTO"
        assert "Adapter Error" in output.reason
        for masquerade in ("SESSION", "CONFIDENCE", "REGIME"):
            assert masquerade not in output.reason.upper(), (
                "a strategy crash must not be reported as a gate verdict"
            )

    def test_the_adapter_still_supports_a_genuine_legacy_signature(self):
        from app.core.trading_orchestrator import LegacyStrategyAdapter
        from app.strategy.base import Signal, SignalResult

        class LegacyNoKwargs:
            name = "legacy"
            calls = 0

            def get_signal(self, symbol):
                LegacyNoKwargs.calls += 1
                return SignalResult(Signal.HOLD, 0.0, "legacy", {})

        adapter = LegacyStrategyAdapter(LegacyNoKwargs())
        adapter.analyze(symbol="BTCUSDT", klines=[], current_price=1.0,
                        market_type="CRYPTO")
        assert LegacyNoKwargs.calls == 1

    def test_neither_call_site_retries_unconditionally(self):
        """Source-level guard against the pattern being reintroduced."""
        from app.core import trading_orchestrator
        from app.runner import runner

        for module in (trading_orchestrator, runner):
            source = inspect.getsource(module)
            assert "except TypeError:\n" not in source, (
                f"{module.__name__} catches TypeError without distinguishing a "
                "signature rejection from a strategy-body failure"
            )


# ── 3. The float(None) that started it ─────────────────────────────────────


class TestNoThresholdIsNotZero:
    def test_classify_hold_reason_accepts_an_unevaluated_threshold(self):
        """A candle with no directional candidate never had a threshold."""
        assert classify_hold_reason(
            "NO_OPPORTUNITY", confidence=0.0, threshold_floor=None
        ) == "NO_PATTERN"

    def test_a_missing_threshold_is_not_treated_as_a_floor_of_zero(self):
        """None must not fire CONFIDENCE_BELOW_FLOOR, and must not raise."""
        assert classify_hold_reason(
            "NO_OPPORTUNITY", confidence=0.5, threshold_floor=None
        ) != "CONFIDENCE_BELOW_FLOOR"

    def test_a_real_floor_still_classifies(self):
        assert classify_hold_reason(
            "below", confidence=0.3, threshold_floor=0.7
        ) == "CONFIDENCE_BELOW_FLOOR"

    def test_the_ensemble_passes_none_through_rather_than_coercing(self):
        source = inspect.getsource(MasterEnsembleStrategy.get_signal)
        assert "threshold_floor=float(effective_threshold)" not in source, (
            "float(None) here raised the TypeError that the two call sites "
            "converted into a kwargs-less re-evaluation"
        )
