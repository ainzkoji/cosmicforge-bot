"""The controlled approved opportunity.

Where this sits is the whole point. The blueprint asks for a *deterministic*
opportunity that still has to clear the *real* bar, so the injection goes in at
the lowest possible level — the component votes — and nothing downstream is
touched:

    component strategies      <- CONTROLLED HERE (the market interpretation)
      -> regime classification            real
      -> regime weighting / aggregation   real
      -> dynamic threshold resolution     real
      -> build_opportunity                real
      -> TradingDecisionEngine.evaluate   real  (the one quality comparison)
      -> HTF bias veto                    real
      -> Risk                             real
      -> ExecutionFeasibility             real
      -> EntryProtection                  real
      -> PaperExecutor                    real
      -> PositionManager                  real

An earlier draft of this file overrode the ensemble's own decision step. That
was wrong: it would have stepped over the regime, session, volatility and HTF
gates, which the blueprint explicitly forbids disabling. Replacing the seven
component strategies with deterministic ones leaves every one of those gates in
force — a controlled opportunity that the regime or HTF filter refuses is a
*correct* refusal, and the harness reports it rather than routing around it.

Nothing here lowers a threshold. If the controlled votes do not clear the real
resolved threshold, the run fails and says so.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

#: The seven real component names. Keeping the names means the ensemble's
#: regime weight multipliers and its enable/disable-by-regime logic apply
#: exactly as they would in production.
COMPONENT_NAMES = (
    "supertrend",
    "vwap_reversion",
    "trend_pullback",
    "squeeze_breakout",
    "sma_cross",
    "donchian_breakout",
    "bollinger_reversion",
)


class ControlledComponent:
    """One deterministic component vote.

    Deliberately dumb: it looks at nothing and always votes the same way. That
    is what makes the lifecycle proof reproducible — the interesting behaviour
    under test is everything *after* the vote.
    """

    version = "phase12-controlled"

    def __init__(self, name: str, *, side: str, confidence: float, client=None,
                 interval: str = "15m") -> None:
        self.name = name
        self.side = side.upper()
        self.confidence = float(confidence)
        # `_run` temporarily swaps this for a SnapshotMarketClient, so it has
        # to be a plain settable attribute.
        self.client = client
        self.interval = interval

    def get_signal(self, symbol: str, **kwargs):
        from app.strategy.base import Signal, SignalResult

        return SignalResult(
            signal=Signal.BUY if self.side == "BUY" else Signal.SELL,
            confidence=self.confidence,
            reason="PHASE12_CONTROLLED_COMPONENT",
            meta={"controlled_validation": True, "component": self.name},
        )


def install_controlled_opportunity(
    strategy,
    *,
    side: str = "BUY",
    confidence: float = 0.95,
):
    """Replace the ensemble's component strategies with deterministic votes.

    Returns the strategy. Also attaches a recorder so the harness can report
    what the real engine decided, without changing the decision.
    """
    components = getattr(strategy, "_strategies", None)
    if not isinstance(components, dict) or not components:
        raise RuntimeError(
            "expected the ensemble's _strategies dict; the controlled "
            f"opportunity cannot be installed on {type(strategy).__name__}"
        )

    replaced = []
    for name in list(components):
        original = components[name]
        components[name] = ControlledComponent(
            name, side=side, confidence=confidence,
            client=getattr(original, "client", None) or getattr(strategy, "client", None),
            interval=getattr(original, "interval", getattr(strategy, "interval", "15m")),
        )
        replaced.append(name)

    _install_recorder(strategy)
    strategy._phase12_control["side"] = side.upper()
    strategy._phase12_control["confidence"] = float(confidence)
    strategy._phase12_control["components_replaced"] = replaced
    logger.info(
        "[PHASE12] controlled components installed: %s (side=%s confidence=%.2f)",
        ", ".join(replaced), side.upper(), confidence,
    )
    return strategy


def _install_recorder(strategy) -> None:
    """Record each real verdict. Observation only — it changes no decision."""
    if getattr(strategy, "_phase12_control", None) is not None:
        return

    base = type(strategy)

    class RecordingEnsemble(base):  # type: ignore[misc, valid-type]
        def get_signal(self, symbol: str, **kwargs):
            result = super().get_signal(symbol, **kwargs)
            quality = getattr(self, "last_entry_quality", None)
            meta = result.meta or {}
            self._phase12_control["observed"].append({
                "symbol": symbol,
                "signal": result.signal.value,
                "confidence": round(float(result.confidence), 4),
                "reason": result.reason,
                "regime": meta.get("regime"),
                "buy_score": meta.get("buy_score"),
                "sell_score": meta.get("sell_score"),
                "effective_threshold": meta.get("threshold"),
                "threshold_type": meta.get("threshold_type"),
                "htf_opposed": meta.get("htf_opposed"),
                "approved": bool(getattr(quality, "approved", False)),
                "quality_reason": getattr(quality, "primary_reason", None),
                "opportunity_id": meta.get("opportunity_id"),
                "market_snapshot_id": meta.get("market_snapshot_id"),
            })
            if getattr(quality, "approved", False):
                self._phase12_control["approvals"] += 1
            return result

    strategy._phase12_control = {"approvals": 0, "observed": []}
    strategy.__class__ = RecordingEnsemble


def controlled_summary(strategy) -> dict:
    control = getattr(strategy, "_phase12_control", None) or {}
    return {
        "side": control.get("side"),
        "component_confidence": control.get("confidence"),
        "components_replaced": control.get("components_replaced", []),
        "engine_approvals": control.get("approvals", 0),
        "observations": control.get("observed", []),
    }
