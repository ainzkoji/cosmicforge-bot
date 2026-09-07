"""``TradingOpportunity`` — what Master Ensemble produces instead of a verdict.

An opportunity says: *there is a directional candidate here, and this is the
evidence for it*. It deliberately does NOT say whether the trade is allowed.

Excluded on purpose (Phase 7 §7.3):

  * final account-risk permission  — that belongs to the risk layer
  * broker execution approval      — that belongs to execution feasibility
  * position size                  — that belongs to the risk layer

Including any of those here would recreate the multi-authority problem this
model exists to remove.
"""
from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from app.decision.reasons import QualityReason


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_opportunity_id() -> str:
    return f"opp_{uuid.uuid4().hex[:16]}"


@dataclass(frozen=True)
class TradingOpportunity:
    """An immutable directional candidate with its supporting evidence."""

    # ── Identity / correlation ───────────────────────────────────────────────
    opportunity_id: str
    market_snapshot_id: str
    symbol: str
    timeframe: str
    side: str  # BUY | SELL

    # ── Quality evidence (inputs to the single entry-quality comparison) ──────
    raw_confidence: float
    consensus: float
    buy_score: float
    sell_score: float

    # ── Market interpretation ────────────────────────────────────────────────
    regime: str = "UNKNOWN"
    regime_confidence: float = 0.0

    # ── Strategy evidence ────────────────────────────────────────────────────
    active_strategies: tuple[str, ...] = ()
    supporting_strategies: tuple[str, ...] = ()
    opposing_strategies: tuple[str, ...] = ()
    # The ensemble emits a list of per-strategy dicts; other producers may use a
    # mapping. Both are accepted verbatim — this is evidence, not a schema.
    component_breakdown: Any = field(default_factory=dict)
    strategy_reasons: tuple[str, ...] = ()
    failed_conditions: tuple[str, ...] = ()

    # ── Volatility / protection suggestions (advisory, not sizing) ───────────
    atr: float = 0.0
    atr_pct: float = 0.0
    suggested_stop_price: float | None = None
    suggested_target_price: float | None = None

    # ── Context ──────────────────────────────────────────────────────────────
    htf_aligned: bool | None = None
    htf_timeframe: str | None = None
    htf_candle_close_time: int | None = None
    session_context: str | None = None
    volatility_context: str | None = None
    closed_candle_time: int | None = None

    bot_instance_id: str | None = None
    created_at: str = field(default_factory=_now)

    def __post_init__(self) -> None:
        side = str(self.side).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"TradingOpportunity.side must be BUY or SELL, got {self.side!r}")
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "symbol", str(self.symbol).upper())

    @property
    def is_opportunity(self) -> bool:
        return True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def evidence_summary(self) -> dict[str, Any]:
        """Compact operator-facing view (Phase 7 §7.12).

        Answers, without reading component logs: side, raw confidence,
        consensus, buy/sell score, regime, and which strategies agreed.
        """
        return {
            "opportunity_id": self.opportunity_id,
            "market_snapshot_id": self.market_snapshot_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "side": self.side,
            "raw_confidence": round(float(self.raw_confidence), 4),
            "consensus": round(float(self.consensus), 4),
            "buy_score": round(float(self.buy_score), 4),
            "sell_score": round(float(self.sell_score), 4),
            "regime": self.regime,
            "regime_confidence": round(float(self.regime_confidence), 4),
            "supporting_strategies": list(self.supporting_strategies),
            "opposing_strategies": list(self.opposing_strategies),
            "htf_aligned": self.htf_aligned,
            "closed_candle_time": self.closed_candle_time,
        }


@dataclass(frozen=True)
class NoOpportunity:
    """No directional candidate on this candle.

    ``reason_code`` distinguishes the two genuinely different cases:

      * ``NO_OPPORTUNITY``         — nothing pointed anywhere. There was
                                     nothing to be confident about.
      * ``CONSENSUS_INSUFFICIENT`` — a direction existed, but the strategies
                                     did not agree strongly enough to call it a
                                     candidate.

    Neither is a confidence failure. A confidence failure requires an
    opportunity to have existed in the first place, and is decided downstream by
    ``TradingDecisionEngine``.
    """

    symbol: str
    timeframe: str
    market_snapshot_id: str
    reason_code: str = QualityReason.NO_OPPORTUNITY
    buy_score: float = 0.0
    sell_score: float = 0.0
    consensus: float = 0.0
    regime: str = "UNKNOWN"
    active_strategies: tuple[str, ...] = ()
    failed_conditions: tuple[str, ...] = ()
    detail: str = ""
    closed_candle_time: int | None = None
    created_at: str = field(default_factory=_now)

    def __post_init__(self) -> None:
        if self.reason_code not in {
            QualityReason.NO_OPPORTUNITY,
            QualityReason.CONSENSUS_INSUFFICIENT,
        }:
            raise ValueError(
                "NoOpportunity.reason_code must be NO_OPPORTUNITY or "
                f"CONSENSUS_INSUFFICIENT, got {self.reason_code!r}"
            )
        object.__setattr__(self, "symbol", str(self.symbol).upper())

    @property
    def is_opportunity(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_opportunity(
    *,
    symbol: str,
    timeframe: str,
    market_snapshot_id: str,
    side: str,
    raw_confidence: float,
    consensus: float,
    buy_score: float,
    sell_score: float,
    votes: Sequence[tuple[str, str, float]] = (),
    **extra: Any,
) -> TradingOpportunity:
    """Build an opportunity, deriving supporting/opposing sets from raw votes.

    ``votes`` entries are ``(strategy_name, signal, confidence)``.
    """
    side_upper = str(side).upper()
    supporting = tuple(
        name for name, signal, conf in votes if str(signal).upper() == side_upper and conf > 0
    )
    opposing = tuple(
        name
        for name, signal, conf in votes
        if str(signal).upper() in {"BUY", "SELL"} and str(signal).upper() != side_upper and conf > 0
    )
    return TradingOpportunity(
        opportunity_id=new_opportunity_id(),
        market_snapshot_id=market_snapshot_id,
        symbol=symbol,
        timeframe=timeframe,
        side=side_upper,
        raw_confidence=float(raw_confidence),
        consensus=float(consensus),
        buy_score=float(buy_score),
        sell_score=float(sell_score),
        supporting_strategies=supporting,
        opposing_strategies=opposing,
        **extra,
    )
