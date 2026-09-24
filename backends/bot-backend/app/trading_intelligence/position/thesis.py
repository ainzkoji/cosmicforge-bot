"""Thesis re-evaluation (Sections 19.7-19.8) -- deterministic, interpretable.

Every TradePlan ``ThesisCode`` has an evaluator here (an unknown code is
``UNKNOWN`` with ``THESIS_CODE_NOT_EVALUABLE``, never a silent PASS). Rules
read the CURRENT MarketState / RegimeDistribution / event / broker context --
never PnL:

* a thesis is NOT valid just because price is above entry: every PASS needs
  structural/state confirmation, and
* an unrealized LOSS is NOT an invalidation: only the structural level, the
  state rules below, or a context FAIL can invalidate / force de-risk.

Aggregation (``aggregate_thesis_status``):
    any CORE/STRUCTURAL FAIL -> INVALIDATED
    any CORE UNKNOWN          -> UNKNOWN
    any CORE WEAKENED         -> WEAKENED
    any CONTEXT FAIL/WEAKENED -> at least WEAKENED (a FAIL additionally
                                 raises a mandatory de-risk signal)
    any CONTEXT UNKNOWN       -> policy.context_unknown_thesis_status
                                 (default WEAKENED: e.g. a stale calendar
                                 is never read as "no event")
    ECONOMIC results are reported but judged only by the edge floors.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.trading_intelligence.contracts.events import EventSourceState, MaintenanceSourceState, event_affects
from app.trading_intelligence.contracts.position import (
    ConditionKind as K, ConditionResult as C, ExitPolicy, PositionConditionCode as PC, PositionPathSnapshot,
    PositionReasonCode as PR, ThesisConditionResult, ThesisStatus,
)
from app.trading_intelligence.contracts.trade_plan import ThesisCode as T, TradePlan

_OPP = {"LONG": ("DOWN",), "SHORT": ("UP",)}
_WITH = {"LONG": "UP", "SHORT": "DOWN"}


@dataclass(frozen=True)
class ThesisContext:
    plan: TradePlan
    path: PositionPathSnapshot
    market_state: Any
    regime: Any
    event_context: Any
    system_context: Any
    policy: ExitPolicy


def _r(code, result, kind, source="TRADE_PLAN", reasons=(), **evidence) -> ThesisConditionResult:
    return ThesisConditionResult(
        code=code.value if hasattr(code, "value") else str(code), result=result.value, kind=kind.value, source=source,
        evidence=tuple(sorted((k, f"{v:.6g}" if isinstance(v, float) else str(v)) for k, v in evidence.items())),
        reason_codes=tuple(r.value if hasattr(r, "value") else str(r) for r in reasons))


def _ms_ok(ctx: ThesisContext) -> bool:
    ms = ctx.market_state
    return ms is not None and bool(getattr(ms, "is_usable", False))


# -- core structural theses -------------------------------------------------------------
def _trend(ctx: ThesisContext) -> ThesisConditionResult:
    code, side, p = T.TREND_CONTINUATION_REMAINS_VALID, ctx.plan.side, ctx.policy
    if not _ms_ok(ctx) or not ctx.market_state.trend_state.available:
        return _r(code, C.UNKNOWN, K.CORE, reasons=(PR.THESIS_UNKNOWN,))
    t, s = ctx.market_state.trend_state, ctx.market_state.structure_state
    ev = dict(direction=t.direction, strength=float(t.strength), maturity=t.maturity, choch=s.choch_direction,
              swings=s.swing_sequence)
    against_swings = "LH_LL" if side == "LONG" else "HH_HL"
    if (t.direction in _OPP[side] and t.strength >= p.trend_reversal_strength) or s.choch_direction in _OPP[side] \
            or s.swing_sequence == against_swings:
        return _r(code, C.FAIL, K.CORE, **ev)
    if t.direction == _WITH[side] and t.strength >= p.trend_min_strength and t.maturity != "LATE":
        return _r(code, C.PASS, K.CORE, **ev)
    return _r(code, C.WEAKENED, K.CORE, **ev)


def _htf(ctx: ThesisContext) -> ThesisConditionResult:
    code, side = T.HTF_ALIGNMENT_VALID, ctx.plan.side
    if not _ms_ok(ctx) or not ctx.market_state.higher_timeframe_state.available:
        return _r(code, C.UNKNOWN, K.CORE, reasons=(PR.THESIS_UNKNOWN,))
    h = ctx.market_state.higher_timeframe_state
    ev = dict(direction=h.direction, alignment=h.structure_alignment)
    if h.direction in _OPP[side]:
        return _r(code, C.FAIL, K.CORE, **ev)
    if h.structure_alignment == "ALIGNED" and h.direction == _WITH[side]:
        return _r(code, C.PASS, K.CORE, **ev)
    if h.structure_alignment is None:
        return _r(code, C.UNKNOWN, K.CORE, **ev)
    return _r(code, C.WEAKENED, K.CORE, **ev)


def _breakout(ctx: ThesisContext, code) -> ThesisConditionResult:
    """The breakout boundary is the plan's entry reference. FAIL = price has
    fallen back into the prior structure beyond tolerance AND structure no
    longer confirms the break. Price above entry alone is never a PASS."""
    side, p, path = ctx.plan.side, ctx.policy, ctx.path
    if not _ms_ok(ctx) or not ctx.market_state.structure_state.available:
        return _r(code, C.UNKNOWN, K.CORE, reasons=(PR.THESIS_UNKNOWN,))
    s = ctx.market_state.structure_state
    confirms = s.last_bos_direction == _WITH[side] and s.choch_direction not in _OPP[side]
    back_inside = path.current_R < -p.breakout_failure_tolerance_R
    ev = dict(bos=s.last_bos_direction, choch=s.choch_direction, current_R=path.current_R)
    if s.choch_direction in _OPP[side] or (back_inside and not confirms):
        return _r(code, C.FAIL, K.CORE, **ev)
    if confirms and path.current_R >= 0:
        return _r(code, C.PASS, K.CORE, **ev)
    return _r(code, C.WEAKENED, K.CORE, **ev)


def _range(ctx: ThesisContext) -> ThesisConditionResult:
    code, side = T.RANGE_BOUNDARY_REMAINS_INTACT, ctx.plan.side
    if not _ms_ok(ctx) or not ctx.market_state.structure_state.available:
        return _r(code, C.UNKNOWN, K.CORE, reasons=(PR.THESIS_UNKNOWN,))
    s, price = ctx.market_state.structure_state, ctx.path.current_price
    if s.range_high is None or s.range_low is None:
        return _r(code, C.UNKNOWN, K.CORE, reasons=(PR.THESIS_UNKNOWN,))
    ev = dict(range_low=float(s.range_low), range_high=float(s.range_high), price=float(price), bos=s.last_bos_direction)
    broken = price < s.range_low if side == "LONG" else price > s.range_high
    if broken or s.last_bos_direction in _OPP[side]:
        return _r(code, C.FAIL, K.CORE, **ev)
    dominant = getattr(ctx.regime, "dominant_regime", None) if ctx.regime is not None else None
    if dominant in ("TREND_CONTINUATION", "VOL_EXPANSION") or s.structure_integrity < 0.5:
        return _r(code, C.WEAKENED, K.CORE, dominant_regime=dominant, **ev)
    return _r(code, C.PASS, K.CORE, **ev)


def _momentum(ctx: ThesisContext) -> ThesisConditionResult:
    code, side, p = T.MOMENTUM_PARTICIPATION_PRESENT, ctx.plan.side, ctx.policy
    if not _ms_ok(ctx) or not ctx.market_state.momentum_state.available \
            or not ctx.market_state.participation_state.available:
        return _r(code, C.UNKNOWN, K.CORE, reasons=(PR.THESIS_UNKNOWN,))
    m, pa = ctx.market_state.momentum_state, ctx.market_state.participation_state
    if m.short_return is None or pa.relative_volume is None:
        return _r(code, C.UNKNOWN, K.CORE, reasons=(PR.THESIS_UNKNOWN,))
    aligned = (m.short_return > 0) if side == "LONG" else (m.short_return < 0)
    ev = dict(short_return=float(m.short_return), relative_volume=float(pa.relative_volume),
              exhaustion=float(m.exhaustion_proxy) if m.exhaustion_proxy is not None else "NA")
    medium_against = m.medium_return is not None and ((m.medium_return < 0) if side == "LONG" else (m.medium_return > 0))
    if not aligned and medium_against:
        return _r(code, C.FAIL, K.CORE, **ev)
    exhausted = m.exhaustion_proxy is not None and m.exhaustion_proxy > p.momentum_exhaustion_proxy_max
    if aligned and pa.relative_volume >= p.momentum_min_relative_volume and not exhausted:
        return _r(code, C.PASS, K.CORE, **ev)
    return _r(code, C.WEAKENED, K.CORE, **ev)


# -- context conditions -------------------------------------------------------------------
def _event(ctx: ThesisContext) -> ThesisConditionResult:
    code, e = T.EVENT_CONTEXT_ACCEPTABLE, ctx.event_context
    now, end = ctx.path.current_time, ctx.path.current_time + ctx.policy.event_lookahead_ms
    if e is None:
        return _r(code, C.UNKNOWN, K.CONTEXT, reasons=(PR.CONTEXT_UNVERIFIED,), source_state="NOT_PROVIDED")
    key = ctx.plan.instrument_key
    maint = e.maintenance
    if maint is not None and maint.state == MaintenanceSourceState.AVAILABLE.value:
        hits = [w for w in maint.windows if w.overlaps(now, end) and event_affects(w, key, e.scope_policy)]
        if hits:
            return _r(code, C.FAIL, K.CONTEXT, reasons=(PR.SYSTEM_DE_RISK_REQUIRED,), maintenance=hits[0].event_id)
    if e.source_state != EventSourceState.AVAILABLE.value:
        return _r(code, C.UNKNOWN, K.CONTEXT, reasons=(PR.CONTEXT_UNVERIFIED,), source_state=e.source_state)
    hits = [x for x in e.events if not x.is_maintenance and x.overlaps(now, end) and event_affects(x, key, e.scope_policy)]
    high = [x for x in hits if x.importance == "HIGH"]
    if high:
        return _r(code, C.FAIL, K.CONTEXT, reasons=(PR.EVENT_DE_RISK_REQUIRED,), events=",".join(sorted(x.event_id for x in high)))
    if hits:
        return _r(code, C.WEAKENED, K.CONTEXT, events=",".join(sorted(x.event_id for x in hits)))
    return _r(code, C.PASS, K.CONTEXT, source_state=e.source_state)


def _liquidity(ctx: ThesisContext) -> ThesisConditionResult:
    code, p = T.LIQUIDITY_NOT_DEGRADED, ctx.policy
    if not _ms_ok(ctx) or not ctx.market_state.liquidity_state.available:
        return _r(code, C.UNKNOWN, K.CONTEXT, reasons=(PR.CONTEXT_UNVERIFIED,))
    liq = ctx.market_state.liquidity_state
    pct, bps = liq.spread_percentile, liq.spread_bps
    budget = ctx.plan.execution_preferences.max_spread_bps
    ev = dict(spread_percentile=pct if pct is not None else "NA", spread_bps=bps if bps is not None else "NA")
    if liq.stale_book or (pct is not None and pct >= p.liquidity_degraded_spread_percentile) \
            or (bps is not None and bps > budget):
        return _r(code, C.FAIL, K.CONTEXT, reasons=(PR.LIQUIDITY_DE_RISK_REQUIRED,), **ev)
    if pct is None:
        return _r(code, C.UNKNOWN, K.CONTEXT, reasons=(PR.CONTEXT_UNVERIFIED,), **ev)
    if pct >= p.liquidity_weak_spread_percentile:
        return _r(code, C.WEAKENED, K.CONTEXT, **ev)
    return _r(code, C.PASS, K.CONTEXT, **ev)


def _broker(ctx: ThesisContext) -> ThesisConditionResult:
    code = T.BROKER_HEALTH_ACCEPTABLE
    bh = getattr(ctx.system_context, "broker_health", None) if ctx.system_context is not None else None
    if bh is None or (bh.broker_account_id not in (None, ctx.plan.broker_account_id)):
        return _r(code, C.UNKNOWN, K.CONTEXT, reasons=(PR.CONTEXT_UNVERIFIED,))
    if bh.status == "HEALTHY":
        return _r(code, C.PASS, K.CONTEXT, status=bh.status)
    if bh.status == "DEGRADED":
        return _r(code, C.WEAKENED, K.CONTEXT, status=bh.status)
    if bh.status == "UNAVAILABLE":
        return _r(code, C.FAIL, K.CONTEXT, reasons=(PR.SYSTEM_DE_RISK_REQUIRED,), status=bh.status)
    return _r(code, C.UNKNOWN, K.CONTEXT, reasons=(PR.CONTEXT_UNVERIFIED,), status=bh.status)


def _economic_placeholder(ctx: ThesisContext) -> ThesisConditionResult:
    """ECONOMIC_EDGE_POSITIVE is re-evaluated from the PositionForecast
    (``economic_condition``); before the forecast exists it is UNKNOWN."""
    return _r(T.ECONOMIC_EDGE_POSITIVE, C.UNKNOWN, K.ECONOMIC)


EVALUATORS: Dict[str, Callable[[ThesisContext], ThesisConditionResult]] = {
    T.TREND_CONTINUATION_REMAINS_VALID.value: _trend,
    T.HTF_ALIGNMENT_VALID.value: _htf,
    T.BREAKOUT_HOLDS_ABOVE_BOUNDARY.value: lambda c: _breakout(c, T.BREAKOUT_HOLDS_ABOVE_BOUNDARY),
    T.BREAKOUT_HOLDS_BELOW_BOUNDARY.value: lambda c: _breakout(c, T.BREAKOUT_HOLDS_BELOW_BOUNDARY),
    T.RANGE_BOUNDARY_REMAINS_INTACT.value: _range,
    T.MOMENTUM_PARTICIPATION_PRESENT.value: _momentum,
    T.EVENT_CONTEXT_ACCEPTABLE.value: _event,
    T.LIQUIDITY_NOT_DEGRADED.value: _liquidity,
    T.BROKER_HEALTH_ACCEPTABLE.value: _broker,
    T.ECONOMIC_EDGE_POSITIVE.value: _economic_placeholder,
}


# -- position-policy checks (not plan thesis codes) ----------------------------------------
def _structural(ctx: ThesisContext) -> ThesisConditionResult:
    inv, price = ctx.plan.structural_invalidation_price, ctx.path.current_price
    broken = price <= inv if ctx.plan.side == "LONG" else price >= inv
    if broken:
        return _r(PC.STRUCTURAL_INVALIDATION_INTACT, C.FAIL, K.STRUCTURAL, "POSITION_POLICY",
                  (PR.STRUCTURAL_INVALIDATION_BROKEN,), invalidation=float(inv), price=float(price))
    return _r(PC.STRUCTURAL_INVALIDATION_INTACT, C.PASS, K.STRUCTURAL, "POSITION_POLICY",
              invalidation=float(inv), price=float(price))


_TREND_FAMILIES = ("TREND", "MOMENTUM", "BREAKOUT")


def _regime(ctx: ThesisContext) -> ThesisConditionResult:
    reg, p = ctx.regime, ctx.policy
    if reg is None:
        return _r(PC.REGIME_SUPPORTS_THESIS, C.UNKNOWN, K.CORE, "POSITION_POLICY", (PR.THESIS_UNKNOWN,))
    w = dict(reg.weights)
    fam = ctx.plan.setup_family.upper()
    trendish = any(fam.startswith(f) for f in _TREND_FAMILIES)
    against = "EXHAUSTION_REVERSAL" if trendish else "TREND_CONTINUATION"
    ev = dict(dominant=reg.dominant_regime, against_weight=float(w.get(against, 0.0)))
    if w.get(against, 0.0) >= p.regime_against_weight:
        return _r(PC.REGIME_SUPPORTS_THESIS, C.FAIL, K.CORE, "POSITION_POLICY", **ev)
    supportive = ("TREND_CONTINUATION", "VOL_EXPANSION") if trendish else ("RANGE_EQUILIBRIUM",)
    if reg.dominant_regime in supportive:
        return _r(PC.REGIME_SUPPORTS_THESIS, C.PASS, K.CORE, "POSITION_POLICY", **ev)
    return _r(PC.REGIME_SUPPORTS_THESIS, C.WEAKENED, K.CORE, "POSITION_POLICY", **ev)


def _shock(ctx: ThesisContext) -> ThesisConditionResult:
    reg, p = ctx.regime, ctx.policy
    shock_w = float(dict(reg.weights).get("SHOCK", 0.0)) if reg is not None else 0.0
    vol_shock = bool(_ms_ok(ctx) and ctx.market_state.volatility_state.available
                     and ctx.market_state.volatility_state.shock_state)
    if shock_w >= p.shock_regime_weight or vol_shock:
        return _r(PC.NO_SHOCK_STATE, C.FAIL, K.CONTEXT, "POSITION_POLICY", (PR.SHOCK_DE_RISK_REQUIRED,),
                  shock_weight=shock_w, vol_shock=vol_shock)
    if reg is None:
        return _r(PC.NO_SHOCK_STATE, C.UNKNOWN, K.CONTEXT, "POSITION_POLICY", (PR.CONTEXT_UNVERIFIED,))
    return _r(PC.NO_SHOCK_STATE, C.PASS, K.CONTEXT, "POSITION_POLICY", shock_weight=shock_w)


def evaluate_thesis(ctx: ThesisContext) -> Tuple[ThesisConditionResult, ...]:
    out: List[ThesisConditionResult] = [_structural(ctx), _regime(ctx), _shock(ctx)]
    for cond in ctx.plan.thesis_conditions:
        fn = EVALUATORS.get(cond.code)
        if fn is None:
            out.append(ThesisConditionResult(cond.code, C.UNKNOWN.value, K.CORE.value, "TRADE_PLAN", (),
                                             (PR.THESIS_CODE_NOT_EVALUABLE.value,)))
            continue
        out.append(fn(ctx))
    return tuple(out)


def aggregate_thesis_status(results, policy: ExitPolicy) -> str:
    core = [r for r in results if r.kind in (K.CORE.value, K.STRUCTURAL.value)]
    ctxs = [r for r in results if r.kind == K.CONTEXT.value]
    if any(r.result == C.FAIL.value for r in core):
        return ThesisStatus.INVALIDATED.value
    if any(r.result == C.UNKNOWN.value for r in core):
        return ThesisStatus.UNKNOWN.value
    status = ThesisStatus.VALID.value
    if any(r.result == C.WEAKENED.value for r in core) or any(r.result in (C.FAIL.value, C.WEAKENED.value) for r in ctxs):
        status = ThesisStatus.WEAKENED.value
    if status == ThesisStatus.VALID.value and any(r.result == C.UNKNOWN.value for r in ctxs):
        status = policy.context_unknown_thesis_status
    return status


def economic_condition(conservative_remaining_edge_R: Optional[float], policy: ExitPolicy) -> ThesisConditionResult:
    if conservative_remaining_edge_R is None:
        return _r(T.ECONOMIC_EDGE_POSITIVE, C.UNKNOWN, K.ECONOMIC)
    e = float(conservative_remaining_edge_R)
    res = C.PASS if e >= policy.hold_edge_floor else (C.FAIL if e < policy.exit_edge_floor else C.WEAKENED)
    return _r(T.ECONOMIC_EDGE_POSITIVE, res, K.ECONOMIC, conservative_remaining_edge_R=e)


__all__ = ["ThesisContext", "EVALUATORS", "evaluate_thesis", "aggregate_thesis_status", "economic_condition"]
