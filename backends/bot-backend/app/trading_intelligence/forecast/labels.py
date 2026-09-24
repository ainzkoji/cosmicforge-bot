"""Historical outcome labeling (Section 12.2) -- RESEARCH LABELS ONLY.

Reuses ``app.research.dataset.future_rows``/``build_future_labels`` for the
walk-forward MFE/MAE/R-multiple/touch-ordering machinery -- that engine
already exists and is correct; this module only (a) applies CATI's own
conservative same-bar rule on top of its ``AMBIGUOUS`` outcome, (b) derives
CATI's three-way terminal outcome and separate cost components in R units,
and (c) assembles the result into ``SetupOutcomeLabel``.

This is the ONLY module in CATI allowed to read price data timestamped
after a candidate's decision_time (P1: "Historical labels may use future
paths ONLY inside explicitly separated research-label generation"). Live
CATI (Sections 9-11, and forecast/cohorts.py & posterior.py below) never
imports this module's future-reading functions for a live evaluation.
"""
from __future__ import annotations

from typing import Any, Optional, Sequence, Tuple

from app.replay.cost_model import CostModel
from app.research.dataset import IntrabarOutcome, build_future_labels, future_rows, ohlcv
from app.trading_intelligence.contracts.forecast import (
    ForecastReasonCode,
    LabelQuality,
    SetupOutcomeLabel,
    TerminalOutcome,
)
from app.trading_intelligence.contracts.setup import SetupCandidate, SetupSide
from app.trading_intelligence.versions import LABEL_POLICY_VERSION, MARKET_STATE_SCHEMA_VERSION, REGIME_MODEL_VERSION, RESEARCH_COST_MODEL_VERSION

#: Default forward-label horizon in bars when the caller does not override it.
DEFAULT_HORIZON_BARS = 48


def _first_touch_bar(rows: Sequence[Any], *, side: str, level: float, is_target: bool) -> Optional[int]:
    """0-based index of the first bar whose range touches ``level`` in the
    direction implied by ``is_target``. Mirrors the touch-check used inside
    ``app.research.dataset._tp_sl_outcome`` but returns *timing*, which that
    function does not expose."""
    for i, row in enumerate(rows):
        _, high, low, _, _ = ohlcv(row)
        if side == SetupSide.SHORT.value:
            touched = (low <= level) if is_target else (high >= level)
        else:
            touched = (high >= level) if is_target else (low <= level)
        if touched:
            return i
    return None


def label_candidate(
    candidate: SetupCandidate,
    all_rows: Sequence[Any],
    *,
    cost_model: Optional[CostModel] = None,
    horizon_bars: int = DEFAULT_HORIZON_BARS,
    label_policy_version: str = LABEL_POLICY_VERSION,
    cost_model_version: str = RESEARCH_COST_MODEL_VERSION,
    market_state_schema_version: str = MARKET_STATE_SCHEMA_VERSION,
    regime_model_version: str = REGIME_MODEL_VERSION,
) -> SetupOutcomeLabel:
    """Labels one SetupCandidate against its future price path.

    ``all_rows`` must be the full causal candle series covering (and
    extending past) ``candidate.decision_time`` -- typically the same
    historical dataset a research pipeline is walking forward through.
    """
    cost_model = cost_model or CostModel.zero()
    reason_codes = []

    if candidate.initial_structural_risk <= 0:
        # Contract-level invariant already prevents this at construction,
        # but a labeling pipeline reading old/foreign candidates must still
        # fail closed rather than divide by zero.
        return _invalid_label(
            candidate, label_policy_version, cost_model_version,
            reason_codes=(ForecastReasonCode.ZERO_INITIAL_RISK.value,),
        )

    future = future_rows(all_rows, candidate.decision_time, horizon_bars)
    if not future:
        return _invalid_label(
            candidate, label_policy_version, cost_model_version,
            reason_codes=(ForecastReasonCode.NO_OBSERVABLE_FUTURE.value,),
        )

    entry = candidate.trigger_reference
    risk = candidate.initial_structural_risk

    # -- cost components, computed separately (never lumped) --------------
    notional = entry  # 1-unit position: cost currency == cost in price units
    fee_currency = notional * cost_model.taker_fee * 2  # round trip
    spread_currency = notional * cost_model.spread * 2
    slippage_currency = notional * cost_model.slippage * 2

    fls = build_future_labels(
        entry_price=entry, future=future, side=candidate.side,
        stop_price=candidate.structural_invalidation, target_price=candidate.target_reference,
        risk_per_unit=risk, horizon_bars=horizon_bars,
        cost_model_hash=cost_model.model_hash,
        fee_cost=fee_currency, spread_cost=spread_currency, slippage_cost=slippage_currency,
        funding_cost=0.0,  # filled in below once horizon_ms is known
    )
    funding_currency = cost_model.funding_cost(notional, fls.horizon_ms)

    fee_R = fee_currency / risk
    spread_R = spread_currency / risk
    slippage_R = slippage_currency / risk
    funding_R = funding_currency / risk
    carry_R = 0.0  # captured via funding for perpetual crypto; distinct field for other asset classes
    total_cost_R = fee_R + spread_R + slippage_R + funding_R + carry_R

    outcome = fls.tp_sl_outcome
    if outcome == IntrabarOutcome.AMBIGUOUS.value:
        # Section 12.2.3: same-bar ambiguity is ALWAYS resolved adversely.
        outcome = IntrabarOutcome.SL_FIRST.value
        reason_codes.append(ForecastReasonCode.SAME_BAR_CONSERVATIVE_STOP_ASSUMED.value)

    time_to_target = _first_touch_bar(future, side=candidate.side, level=candidate.target_reference, is_target=True) \
        if candidate.target_reference is not None else None
    time_to_stop = _first_touch_bar(future, side=candidate.side, level=candidate.structural_invalidation, is_target=False)

    if outcome == IntrabarOutcome.TP_FIRST.value:
        terminal_outcome = TerminalOutcome.TARGET_BEFORE_STOP.value
        gross_R = candidate.room_to_target_R if candidate.room_to_target_R is not None else 0.0
    elif outcome == IntrabarOutcome.SL_FIRST.value:
        terminal_outcome = TerminalOutcome.STOP_BEFORE_TARGET.value
        gross_R = -1.0
        time_to_target = None  # stop resolved first -- target time is not meaningful
    else:  # NEITHER -> timeout (whether or not the horizon was fully observed)
        terminal_outcome = TerminalOutcome.TIMEOUT.value
        direction_sign = 1.0 if candidate.side == SetupSide.LONG.value else -1.0
        last_close = ohlcv(future[-1])[3]
        gross_R = direction_sign * (last_close - entry) / risk
        time_to_target = None
        time_to_stop = None

    net_R = gross_R - total_cost_R
    # dataset.py's mfe/mae are already side-aware fractions of entry: mfe is
    # the favorable excursion (>= 0), mae the adverse one (<= 0) for either
    # side -- no side-conditional needed here.
    mfe_R = max(0.0, fls.mfe) * entry / risk
    mae_R = abs(min(0.0, fls.mae)) * entry / risk

    label_quality = LabelQuality.VALID.value
    if not fls.label_complete:
        label_quality = LabelQuality.CENSORED.value
        reason_codes.append(ForecastReasonCode.CENSORED_HORIZON.value)

    identity = dict(
        setup_candidate_id=candidate.setup_candidate_id,
        label_policy_version=label_policy_version,
        cost_model_version=cost_model_version,
    )
    return SetupOutcomeLabel(
        label_id=SetupOutcomeLabel.build_id(**identity),
        setup_candidate_id=candidate.setup_candidate_id,
        market_state_id=candidate.market_state_id,
        instrument_key=candidate.instrument_key,
        decision_time=candidate.decision_time,
        setup_family=candidate.setup_family,
        setup_version=candidate.setup_version,
        setup_policy_hash=candidate.setup_policy_hash,
        market_state_schema_version=market_state_schema_version,
        regime_model_version=regime_model_version,
        label_policy_version=label_policy_version,
        cost_model_version=cost_model_version,
        terminal_outcome=terminal_outcome,
        net_profitable=net_R > 0,
        gross_R=gross_R,
        net_R=net_R,
        mfe_R=mfe_R,
        mae_R=mae_R,
        time_to_target_bars=time_to_target,
        time_to_stop_bars=time_to_stop,
        terminal_horizon_bars=len(future),
        fee_R=fee_R,
        spread_R=spread_R,
        slippage_R=slippage_R,
        funding_R=funding_R,
        carry_R=carry_R,
        total_cost_R=total_cost_R,
        label_quality=label_quality,
        reason_codes=tuple(reason_codes),
    )


def _invalid_label(candidate: SetupCandidate, label_policy_version: str, cost_model_version: str, *, reason_codes: Tuple[str, ...]) -> SetupOutcomeLabel:
    identity = dict(
        setup_candidate_id=candidate.setup_candidate_id,
        label_policy_version=label_policy_version,
        cost_model_version=cost_model_version,
    )
    return SetupOutcomeLabel(
        label_id=SetupOutcomeLabel.build_id(**identity),
        setup_candidate_id=candidate.setup_candidate_id,
        market_state_id=candidate.market_state_id,
        instrument_key=candidate.instrument_key,
        decision_time=candidate.decision_time,
        setup_family=candidate.setup_family,
        setup_version=candidate.setup_version,
        setup_policy_hash=candidate.setup_policy_hash,
        market_state_schema_version=MARKET_STATE_SCHEMA_VERSION,
        regime_model_version=REGIME_MODEL_VERSION,
        label_policy_version=label_policy_version,
        cost_model_version=cost_model_version,
        terminal_outcome=TerminalOutcome.TIMEOUT.value,
        net_profitable=False,
        gross_R=0.0, net_R=0.0, mfe_R=0.0, mae_R=0.0,
        time_to_target_bars=None, time_to_stop_bars=None, terminal_horizon_bars=0,
        fee_R=0.0, spread_R=0.0, slippage_R=0.0, funding_R=0.0, carry_R=0.0, total_cost_R=0.0,
        label_quality=LabelQuality.INVALID.value,
        reason_codes=reason_codes,
    )


__all__ = ["DEFAULT_HORIZON_BARS", "label_candidate"]
