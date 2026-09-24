"""Venue-neutral cost model (Sections 17.3-17.16): ONE VenueEconomicObservation
+ ONE SetupCandidate (+ its forecast, for holding time) -> ONE Section 13
``CostEstimate``.

This module computes COSTS ONLY -- fee/spread/slippage/funding/carry in R
plus explicit per-component uncertainty and lineage. It never computes EV,
edge or admission: the existing Section 13 engine
(``economics/engine.evaluate_economic_opportunity``) consumes the estimate and
subtracts ``total_cost_R`` exactly once.

R normalization: quantity q (venue units) at the reference/provisional
notional, venue price multiplier M, structural risk distance D (price units,
from the candidate). risk_ccy = D * q * M; every component's quote-currency
amount divided by risk_ccy is its R value.

Fail-closed rules (P7): an unknown cost is never zero. Anything that makes
the evidence untrustworthy (unvalidated adapter, closed/unknown session,
missing fee semantics, unconvertible currency, a hold crossing a rollover
with no swap data, expiry inside the hold, a mapping mismatch, non-causal
data) adds ``COST_NOT_VIABLE`` so the Section 13 COST_QUALITY gate fails and
the opportunity is INSUFFICIENT_EVIDENCE.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from app.trading_intelligence.contracts.economics import (
    ComponentLineage, CostEstimate, CostScope, CostUncertaintyBreakdown, EconomicsReasonCode,
)
from app.trading_intelligence.contracts.forecast import OutcomeForecast
from app.trading_intelligence.contracts.setup import SetupCandidate, timeframe_to_ms
from app.trading_intelligence.contracts.venue_economics import (
    FATAL_VENUE_REASONS, CarrySource, FeeModel, FeeSource, FinancingSource, FundingSource, SessionStatus,
    SlippageSource, SpreadSource, SwapUnit, VenueEconomicObservation, VenueReasonCode,
)
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.venue.policy import VenueCostPolicy, default_venue_cost_policy
from app.trading_intelligence.versions import VENUE_ECONOMIC_COST_MODEL_VERSION

R = VenueReasonCode
_V = VENUE_ECONOMIC_COST_MODEL_VERSION
_FEE_LEVEL = {FeeSource.OBSERVED_ACCOUNT_TIER.value: 0, FeeSource.BROKER_METADATA.value: 1,
              FeeSource.VENUE_DEFAULT.value: 2, FeeSource.CONSERVATIVE_CONFIGURED_FALLBACK.value: 3,
              FeeSource.UNAVAILABLE.value: 4}


# ---------------------------------------------------------------------------
# Holding time and size
# ---------------------------------------------------------------------------
def expected_holding(candidate: SetupCandidate, forecast: Optional[OutcomeForecast],
                     policy: VenueCostPolicy) -> Tuple[int, int, str]:
    """(expected_ms, maximum_ms, source). Expected hold is the forecast's
    outcome-weighted median time-to-exit when available, else a policy
    default; the maximum is the label horizon (a hold cannot outlast it)."""
    bar_ms = timeframe_to_ms(candidate.timeframe) or 3_600_000
    max_bars = policy.max_holding_bars
    source = "POLICY_DEFAULT_HOLDING_BARS"
    bars = float(policy.default_holding_bars)
    if forecast is not None and forecast.is_usable:
        tt = forecast.time_to_target_quantiles.get("p50")
        ts = forecast.time_to_stop_quantiles.get("p50")
        if tt is not None or ts is not None:
            bars = (forecast.p_target_before_stop * (tt if tt is not None else max_bars)
                    + forecast.p_stop_before_target * (ts if ts is not None else max_bars)
                    + forecast.p_timeout * max_bars)
            source = "FORECAST_TIME_QUANTILES"
    bars = min(max(bars, 1.0), float(max_bars))
    return int(round(bars * bar_ms)), int(max_bars * bar_ms), source


def _quantity(notional: float, price: float, multiplier: float, step: float, min_qty: float,
              min_notional: Optional[float]) -> Tuple[float, bool]:
    raw = notional / (price * multiplier)
    q = math.floor(round(raw / step, 9)) * step
    q = max(q, min_qty, step)
    bumped = False
    if min_notional and q * price * multiplier < min_notional:
        q = math.ceil(round(min_notional / (price * multiplier) / step, 9)) * step
        bumped = True
    return q, bumped


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------
@dataclass
class _Acc:
    reasons: List[str] = field(default_factory=list)
    fatal: List[str] = field(default_factory=list)
    native: Dict[str, float] = field(default_factory=dict)
    lineage: List[ComponentLineage] = field(default_factory=list)

    def reason(self, code, fatal: bool = False) -> None:
        v = code.value if hasattr(code, "value") else str(code)
        if v not in self.reasons:
            self.reasons.append(v)
        if fatal and v not in self.fatal:
            self.fatal.append(v)


def _fees(obs: VenueEconomicObservation, q: float, entry: float, mult: float, risk_ccy: float,
          policy: VenueCostPolicy, acc: Optional[_Acc]) -> Tuple[float, float]:
    fee = obs.fee_observation
    meta = obs.instrument_metadata
    notional = entry * q * mult
    if fee.source == FeeSource.UNAVAILABLE.value:
        if acc:
            acc.reason(R.FEE_UNAVAILABLE, fatal=True)
        return 0.0, 0.0
    conv = 1.0
    if fee.commission_currency and meta is not None and fee.commission_currency != meta.quote_currency:
        if fee.commission_to_quote_rate:
            conv = fee.commission_to_quote_rate
        elif fee.commission_per_contract or fee.exchange_fee_per_contract or fee.clearing_fee_per_contract \
                or fee.other_charge_per_trade:
            if acc:
                acc.reason(R.CURRENCY_CONVERSION_UNAVAILABLE, fatal=True)
            return 0.0, 0.0
    ccy = 0.0
    model = fee.fee_model
    if model == FeeModel.PERCENT_NOTIONAL.value:
        r_in = fee.taker_fee_rate if policy.assume_taker_entry else fee.maker_fee_rate
        r_out = fee.taker_fee_rate if policy.assume_taker_exit else fee.maker_fee_rate
        if r_in is None or r_out is None:
            if acc:
                acc.reason(R.FEE_UNAVAILABLE, fatal=True)
            return 0.0, 0.0
        ccy = (r_in + r_out) * notional
    elif model in (FeeModel.PER_CONTRACT.value, FeeModel.SPREAD_PLUS_COMMISSION.value):
        per = fee.commission_per_contract
        rate = fee.broker_commission_rate
        if per is None and rate is None:
            if acc:
                acc.reason(R.FEE_UNAVAILABLE, fatal=True)
            return 0.0, 0.0
        if per is not None:
            per_total = per + (fee.exchange_fee_per_contract or 0.0) + (fee.clearing_fee_per_contract or 0.0)
            ccy += 2.0 * per_total * (q / (fee.commission_contract_size or 1.0)) * conv
        if rate is not None:
            ccy += 2.0 * rate * notional
    if fee.other_charge_per_trade:
        ccy += 2.0 * fee.other_charge_per_trade * conv
    if fee.currency_conversion_fee_rate:
        ccy += fee.currency_conversion_fee_rate * notional
    if model != FeeModel.SPREAD_ONLY.value and ccy <= 0:
        if acc:
            acc.reason(R.ZERO_FEE_REFUSED, fatal=True)  # a non-spread-only venue never trades for free
        return 0.0, 0.0
    fee_r = ccy / risk_ccy
    unc = policy.fee_uncertainty_fraction.get(fee.source, 1.0) * fee_r
    if acc:
        acc.native.update(fee_ccy=ccy)
        acc.lineage.append(ComponentLineage(
            "FEE", fee.source, fee.observed_at,
            "VALID" if _FEE_LEVEL.get(fee.source, 4) <= 1 else "FALLBACK", _FEE_LEVEL.get(fee.source, 4), _V))
    return fee_r, unc


def _spread(obs, entry: float, risk: float, policy: VenueCostPolicy, thin: bool, acc: Optional[_Acc]):
    sp = obs.spread_observation
    if sp.source == SpreadSource.UNAVAILABLE.value or (sp.spread_absolute is None and sp.spread_bps is None):
        if acc:
            acc.reason(R.SPREAD_UNAVAILABLE, fatal=True)
        return 0.0, 0.0
    px = sp.spread_absolute if sp.spread_absolute is not None else sp.spread_bps / 1e4 * entry
    spread_r = px / risk  # half-spread paid on entry + half on exit = one full spread
    unc = policy.spread_uncertainty_fraction.get(sp.source, 1.5) * spread_r
    if thin:
        unc *= policy.thin_session_uncertainty_multiplier
    if acc:
        acc.native.update(spread_price=px)
        acc.lineage.append(ComponentLineage("SPREAD", sp.source, sp.book_as_of if sp.fallback_level == 0 else None,
                                            "VALID" if sp.fallback_level == 0 else "FALLBACK", sp.fallback_level, _V))
    return spread_r, unc


def _walk(levels, qty: float, buy: bool, penalty_bps: float) -> Tuple[float, bool]:
    """VWAP of taking ``qty`` through ``levels``; beyond the visible book the
    remainder is priced at the last level plus a policy penalty."""
    remaining, cost = qty, 0.0
    last = levels[-1][0]
    for price, size in levels:
        take = min(remaining, size)
        cost += take * price
        remaining -= take
        if remaining <= 1e-12:
            return cost / qty, True
    beyond = last * (1 + penalty_bps / 1e4) if buy else last * (1 - penalty_bps / 1e4)
    cost += remaining * beyond
    return cost / qty, False


def _slippage(obs, side: str, q: float, entry: float, risk: float, policy: VenueCostPolicy, thin: bool,
              acc: Optional[_Acc]):
    sl = obs.slippage_observation
    floor_px = policy.min_slippage_bps_floor / 1e4 * entry
    large = False
    if sl.source == SlippageSource.DEPTH_WALK.value and sl.depth_asks and sl.depth_bids:
        entry_levels, exit_levels = (sl.depth_asks, sl.depth_bids) if side == "LONG" else (sl.depth_bids, sl.depth_asks)
        vwap_in, full_in = _walk(entry_levels, q, side == "LONG", policy.beyond_depth_penalty_bps)
        vwap_out, full_out = _walk(exit_levels, q, side != "LONG", policy.beyond_depth_penalty_bps)
        slip_in = max(abs(vwap_in - entry_levels[0][0]), floor_px)
        slip_out = max(abs(vwap_out - exit_levels[0][0]), floor_px)
        px = slip_in + slip_out
        visible = sum(s for _p, s in entry_levels)
        if not (full_in and full_out) and acc:
            acc.reason(R.DEPTH_INSUFFICIENT_FOR_SIZE)
        large = q > policy.large_order_depth_fraction * visible
    else:
        bps = sl.per_side_bps if sl.per_side_bps is not None else policy.conservative_slippage_bps
        top = sl.top_ask_quantity if side == "LONG" else sl.top_bid_quantity
        scale = max(1.0, math.sqrt(q / top)) if top else 1.0  # size-aware, never improving with size
        px = 2.0 * max(bps * scale / 1e4 * entry, floor_px)
        large = bool(top) and q > policy.large_order_depth_fraction * top
    slip_r = px / risk
    unc = policy.slippage_uncertainty_fraction.get(sl.source, 2.0) * slip_r
    if large:
        unc += slip_r
        if acc:
            acc.reason(R.LARGE_ORDER_VS_DEPTH)
    if thin:
        unc *= policy.thin_session_uncertainty_multiplier
    if acc:
        acc.native.update(slippage_price=px)
        acc.lineage.append(ComponentLineage("SLIPPAGE", sl.source, sl.depth_as_of,
                                            "VALID" if sl.fallback_level == 0 else "FALLBACK", sl.fallback_level, _V))
    return slip_r, unc


def _stamps(t0: int, hold: int, next_time: Optional[int], interval: int) -> int:
    if hold <= 0:
        return 0
    if next_time is None or next_time <= t0:
        return math.ceil(hold / interval)  # schedule unknown: every interval the hold could span
    end = t0 + hold
    return 0 if end < next_time else 1 + (end - next_time) // interval


def _funding(obs, side: str, entry: float, risk: float, hold: int, max_hold: int, policy: VenueCostPolicy,
             acc: _Acc):
    f = obs.funding_observation
    if not f.applicable:
        acc.lineage.append(ComponentLineage("FUNDING", FundingSource.NOT_APPLICABLE.value, None, "NOT_APPLICABLE", 0, _V))
        return 0.0, 0.0
    t0 = obs.decision_time
    if f.source == FundingSource.UNAVAILABLE.value or f.current_funding_rate is None:
        # missing funding is not zero funding: charge the conservative rate, side-blind
        rate, interval = policy.fallback_funding_rate_per_interval, policy.fallback_funding_interval_ms
        n, n_max = math.ceil(hold / interval), math.ceil(max_hold / interval)
        cost_r = n * rate * entry / risk
        acc.reason(R.FUNDING_FALLBACK_USED)
        acc.native.update(funding_stamps_expected=float(n), funding_rate_used=rate)
        acc.lineage.append(ComponentLineage("FUNDING", FundingSource.CONSERVATIVE_CONFIGURED_FALLBACK.value, None,
                                            "FALLBACK", 2, _V))
        return cost_r, cost_r + (n_max - n) * rate * entry / risk
    interval = f.funding_interval_ms or policy.fallback_funding_interval_ms
    n = _stamps(t0, hold, f.next_funding_time, interval)
    n_max = _stamps(t0, max_hold, f.next_funding_time, interval)
    use_predicted = (f.predicted_funding_rate is not None and f.predicted_as_of is not None
                     and f.predicted_as_of <= t0)
    rate0 = f.predicted_funding_rate if use_predicted else f.current_funding_rate
    src0 = FundingSource.PREDICTED_RATE.value if use_predicted else FundingSource.CURRENT_RATE_SCHEDULE.value
    later = f.current_funding_rate
    sign = 1.0 if side == "LONG" else -1.0  # positive funding: longs pay, shorts receive
    mark = f.mark_price or entry
    per = mark / risk
    raw_r = (sign * rate0 * per if n >= 1 else 0.0) + sign * later * per * max(0, n - 1)
    unc = 0.0
    if n >= 1:
        unc += policy.funding_first_stamp_uncertainty.get(src0, 1.0) * abs(rate0) * per
        unc += policy.funding_later_stamp_uncertainty * abs(later) * per * max(0, n - 1)
    unc += max(abs(later), policy.fallback_funding_rate_per_interval) * per * max(0, n_max - n)
    cost_r = raw_r
    if raw_r < 0:  # expected funding INCOME
        if policy.credit_funding_income:
            cost_r = raw_r * (1.0 - policy.funding_income_haircut)
            unc += abs(cost_r)  # credited income is fully uncertain: it can never improve the conservative edge
        else:
            cost_r = 0.0
            acc.reason(R.FUNDING_INCOME_NOT_CREDITED)
            acc.native.update(uncredited_funding_income_R=-raw_r)
    acc.native.update(funding_stamps_expected=float(n), funding_stamps_max=float(n_max), funding_rate_used=rate0)
    acc.lineage.append(ComponentLineage("FUNDING", src0, f.observed_at, "VALID", 0 if use_predicted else 1, _V))
    return cost_r, unc


_NY = ZoneInfo("America/New_York")


def rollover_count(t0: int, hold: int, *, hour: int = 17, tz: str = "America/New_York",
                   days: Tuple[int, ...] = (0, 1, 2, 3, 4), triple_weekday: Optional[int] = None) -> int:
    """Weighted number of daily rollovers (``hour`` local) strictly after t0
    and at or before t0 + hold; the triple-swap weekday counts three times."""
    if hold <= 0:
        return 0
    zone = ZoneInfo(tz)
    start = datetime.fromtimestamp(t0 / 1000, tz=timezone.utc).astimezone(zone)
    end_ms = t0 + hold
    total = 0
    day = start.replace(hour=hour, minute=0, second=0, microsecond=0)
    for _ in range(int(hold / 86_400_000) + 3):
        instant = int(day.timestamp() * 1000)
        if t0 < instant <= end_ms and day.weekday() in days:
            total += 3 if triple_weekday is not None and day.weekday() == triple_weekday else 1
        day = (day + timedelta(days=1)).replace(hour=hour)
    return total


def _financing(obs, side: str, entry: float, risk: float, hold: int, max_hold: int, policy: VenueCostPolicy,
               acc: _Acc):
    fin = obs.financing_observation
    if not fin.applicable:
        return 0.0, 0.0
    kw = dict(hour=fin.rollover_hour_local, tz=fin.rollover_timezone, days=fin.financing_days_of_week,
              triple_weekday=fin.triple_swap_weekday)
    t0 = obs.decision_time
    nights, nights_max = rollover_count(t0, hold, **kw), rollover_count(t0, max_hold, **kw)
    acc.native.update(fx_rollovers_expected=float(nights), fx_rollovers_max=float(nights_max))
    if nights_max == 0:
        acc.lineage.append(ComponentLineage("FINANCING", fin.source, fin.observed_at, "NOT_APPLICABLE", 0, _V))
        return 0.0, 0.0
    native = fin.swap_long if side == "LONG" else fin.swap_short
    per_night_px: Optional[float] = None
    if fin.source == FinancingSource.BROKER_SWAP_RATES.value and native is not None:
        if fin.swap_unit == SwapUnit.PRICE_POINTS_PER_UNIT.value:
            per_night_px = native * fin.point_size if fin.point_size else None
        else:
            per_night_px = native / 365.0 * entry
    if per_night_px is None:
        if policy.fx_financing_fallback_annual_rate is None:
            acc.reason(R.FINANCING_REQUIRED_UNAVAILABLE, fatal=True)  # missing swap is not zero swap
            acc.lineage.append(ComponentLineage("FINANCING", FinancingSource.UNAVAILABLE.value, None, "UNAVAILABLE", 2, _V))
            return 0.0, 0.0
        charge = abs(policy.fx_financing_fallback_annual_rate) / 365.0 * entry
        acc.reason(R.FINANCING_FALLBACK_USED)
        acc.lineage.append(ComponentLineage("FINANCING", FinancingSource.CONSERVATIVE_CONFIGURED_FALLBACK.value, None,
                                            "FALLBACK", 1, _V))
        return charge * nights / risk, charge * nights_max / risk
    cost_per_night = -per_night_px  # broker sign: positive = credit
    raw_r = cost_per_night * nights / risk
    unc = policy.financing_uncertainty_fraction * abs(raw_r) + abs(cost_per_night) * (nights_max - nights) / risk
    cost_r = raw_r
    if raw_r < 0:
        if policy.credit_financing_income:
            unc += abs(raw_r)
        else:
            cost_r = 0.0
            acc.reason(R.FINANCING_INCOME_NOT_CREDITED)
            acc.native.update(uncredited_financing_income_R=-raw_r)
    acc.native.update(fx_swap_R=raw_r)
    acc.lineage.append(ComponentLineage("FINANCING", fin.source, fin.observed_at, "VALID", 0, _V))
    return cost_r, unc


def _carry(obs, side: str, entry: float, risk: float, hold: int, max_hold: int, policy: VenueCostPolicy, acc: _Acc):
    c = obs.carry_observation
    if not c.applicable:
        return 0.0, 0.0
    tte = c.time_to_expiry_ms
    if tte is not None and (tte <= 0 or hold >= tte):
        acc.reason(R.EXPIRY_WITHIN_HOLD, fatal=True)
        return 0.0, 0.0
    if c.source != CarrySource.OBSERVED_BASIS.value or c.basis_absolute is None or not tte:
        acc.reason(R.CARRY_UNAVAILABLE)
        acc.lineage.append(ComponentLineage("CARRY", CarrySource.UNAVAILABLE.value, None, "UNAVAILABLE", 1, _V))
        return 0.0, policy.carry_unavailable_uncertainty_bps / 1e4 * entry / risk
    # expected linear convergence of the future toward spot over the hold:
    # a drag for LONG in contango (basis > 0), for SHORT in backwardation
    conv = c.basis_absolute * (hold / tte)
    conv_max = c.basis_absolute * (min(max_hold, tte) / tte)
    sign = 1.0 if side == "LONG" else -1.0
    raw_r = sign * conv / risk
    unc = policy.carry_uncertainty_fraction * abs(raw_r) + abs(conv_max - conv) / risk
    cost_r = raw_r
    if raw_r < 0:
        if policy.credit_carry_benefit:
            unc += abs(raw_r)
        else:
            cost_r = 0.0
            acc.reason(R.CARRY_BENEFIT_NOT_CREDITED)
            acc.native.update(uncredited_carry_benefit_R=-raw_r)
    acc.native.update(basis_convergence_price=conv, basis_absolute=c.basis_absolute)
    acc.lineage.append(ComponentLineage("CARRY", c.source, c.observed_at, "VALID", 0, _V))
    return cost_r, unc


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def build_venue_cost_estimate(
    candidate: SetupCandidate,
    observation: VenueEconomicObservation,
    *,
    forecast: Optional[OutcomeForecast] = None,
    policy: Optional[VenueCostPolicy] = None,
    reference_notional: Optional[float] = None,
) -> CostEstimate:
    policy = policy or default_venue_cost_policy()
    acc = _Acc()
    for code in observation.reason_codes:
        acc.reason(code, fatal=code in FATAL_VENUE_REASONS)
    key = candidate.instrument_key
    if observation.instrument_key.canonical_symbol != key.canonical_symbol \
            or observation.instrument_key.venue_symbol.upper() != key.venue_symbol.upper():
        acc.reason(R.INSTRUMENT_MAPPING_MISMATCH, fatal=True)
    if observation.decision_time < candidate.decision_time or observation.observed_at > observation.decision_time:
        acc.reason(R.NON_CAUSAL_OBSERVATION, fatal=True)
    if not observation.session_state.tradable:
        acc.reason(R.MARKET_CLOSED if observation.session_state.status in (SessionStatus.CLOSED.value,
                                                                           SessionStatus.ROLLOVER.value)
                   else R.SESSION_UNKNOWN, fatal=True)

    entry, risk = float(candidate.trigger_reference), float(candidate.initial_structural_risk)
    meta = observation.instrument_metadata
    mult = meta.contract_multiplier if meta else 1.0
    step = meta.step_size if meta else 1e-9
    min_qty = meta.minimum_quantity if meta else 0.0
    min_notional = meta.minimum_notional if meta else None
    if reference_notional is None:
        reference_notional = policy.reference_notional.get(key.asset_class)
    if reference_notional is None:  # one minimum tradable unit (e.g. one futures contract)
        reference_notional = max(min_qty, step) * entry * mult
    hold, max_hold, hold_source = expected_holding(candidate, forecast, policy)
    thin = observation.session_state.status == SessionStatus.THIN.value

    def at(notional: float, record: Optional[_Acc]):
        q, bumped = _quantity(notional, entry, mult, step, min_qty, min_notional)
        risk_ccy = risk * q * mult
        fee_r, fee_u = _fees(observation, q, entry, mult, risk_ccy, policy, record)
        sp_r, sp_u = _spread(observation, entry, risk, policy, thin, record)
        sl_r, sl_u = _slippage(observation, candidate.side, q, entry, risk, policy, thin, record)
        return q, bumped, (fee_r, fee_u), (sp_r, sp_u), (sl_r, sl_u)

    q, bumped, (fee_r, fee_u), (spread_r, spread_u), (slip_r, slip_u) = at(reference_notional, acc)
    if bumped:
        acc.reason(R.BELOW_MIN_NOTIONAL_AT_REFERENCE)
    funding_r, funding_u = _funding(observation, candidate.side, entry, risk, hold, max_hold, policy, acc)
    fin_r, fin_u = _financing(observation, candidate.side, entry, risk, hold, max_hold, policy, acc)
    basis_r, basis_u = _carry(observation, candidate.side, entry, risk, hold, max_hold, policy, acc)
    carry_r, carry_u = fin_r + basis_r, fin_u + basis_u

    if fee_r + spread_r + slip_r <= 0:
        acc.reason(R.ZERO_EXECUTION_COST_REFUSED, fatal=True)

    gross = abs(fee_r) + abs(spread_r) + abs(slip_r) + abs(funding_r) + abs(carry_r)
    adapter_u = policy.adapter_uncertainty_fraction.get(observation.adapter_status, 1.0) * gross
    if R.BROKER_DEGRADED.value in observation.reason_codes:
        adapter_u += policy.broker_degraded_uncertainty_fraction * gross
    breakdown = CostUncertaintyBreakdown(
        fee_uncertainty_R=fee_u, spread_uncertainty_R=spread_u, slippage_uncertainty_R=slip_u,
        funding_uncertainty_R=funding_u, carry_uncertainty_R=carry_u, adapter_uncertainty_R=adapter_u)
    total = fee_r + spread_r + slip_r + funding_r + carry_r

    # size-aware curve: cost at larger sizes can only stay equal or worsen
    curve: List[Tuple[float, float]] = []
    running = -math.inf
    for m in policy.marginal_curve_multiples:
        _q, _b, (f_r, _), (s_r, _), (l_r, _) = at(reference_notional * m, None)
        running = max(running, f_r + s_r + l_r + funding_r + carry_r)
        curve.append((reference_notional * m, running))

    acc.native.update(reference_notional=reference_notional, quantity=q, notional_ccy=q * entry * mult,
                      risk_ccy=risk * q * mult, expected_holding_ms=float(hold), max_holding_ms=float(max_hold))
    reasons = list(acc.reasons)
    if acc.fatal:
        reasons.append(EconomicsReasonCode.COST_NOT_VIABLE.value)
        quality = "INVALID"
    else:
        fallback = any(l.quality == "FALLBACK" for l in acc.lineage)
        quality = "DEGRADED" if fallback or observation.source_quality != "VALID" else "VALID"
    reasons.append(f"HOLDING_SOURCE:{hold_source}")

    account = bool(observation.user_id and observation.broker_account_id)
    scope = CostScope.ACCOUNT.value if account else CostScope.VENUE.value
    sizing_hash = stable_hash({"reference_notional": reference_notional, "hold": hold, "max_hold": max_hold,
                               "side": candidate.side})
    return CostEstimate(
        cost_estimate_id=CostEstimate.build_id(
            instrument_key=key, venue=observation.venue, cost_scope=scope, cost_policy_hash=policy.policy_hash,
            decision_time=candidate.decision_time, observation_hash=observation.observation_hash,
            broker_account_id=observation.broker_account_id if account else None,
            setup_candidate_id=candidate.setup_candidate_id, sizing_context_hash=sizing_hash),
        instrument_key=key, venue=observation.venue, cost_scope=scope,
        fee_R=fee_r, spread_R=spread_r, slippage_R=slip_r, funding_R=funding_r, carry_R=carry_r,
        total_cost_R=total, cost_uncertainty_R=breakdown.total,
        cost_model_version=VENUE_ECONOMIC_COST_MODEL_VERSION, cost_policy_hash=policy.policy_hash,
        source_quality=quality, reason_codes=tuple(dict.fromkeys(reasons)),
        user_id=observation.user_id if account else None,
        broker_account_id=observation.broker_account_id if account else None,
        bot_instance_id=observation.bot_instance_id if account else None,
        run_id=observation.run_id if account else None, cycle_id=observation.cycle_id if account else None,
        environment=observation.environment, venue_observation_id=observation.observation_id,
        adapter_status=observation.adapter_status, decision_time=observation.decision_time,
        expected_holding_ms=hold, uncertainty_breakdown=breakdown, component_lineage=tuple(acc.lineage),
        native_costs=dict(sorted(acc.native.items())), reference_notional=reference_notional,
        marginal_cost_curve=tuple(curve),
    )


# ---------------------------------------------------------------------------
# Section 19.9 -- REMAINING (from-NOW) holding and exit costs
# ---------------------------------------------------------------------------
def _exit_fee_r(obs, q: float, price: float, mult: float, risk_ccy: float, policy: VenueCostPolicy,
                acc: _Acc) -> Tuple[float, float]:
    """ONE side (the exit) of the fee model ``_fees`` prices round trip."""
    fee, meta = obs.fee_observation, obs.instrument_metadata
    notional = price * q * mult
    if fee.source == FeeSource.UNAVAILABLE.value:
        acc.reason(R.FEE_UNAVAILABLE, fatal=True)
        return 0.0, 0.0
    conv = 1.0
    if fee.commission_currency and meta is not None and fee.commission_currency != meta.quote_currency:
        if fee.commission_to_quote_rate:
            conv = fee.commission_to_quote_rate
        elif fee.commission_per_contract or fee.exchange_fee_per_contract or fee.clearing_fee_per_contract:
            acc.reason(R.CURRENCY_CONVERSION_UNAVAILABLE, fatal=True)
            return 0.0, 0.0
    ccy = 0.0
    if fee.fee_model == FeeModel.PERCENT_NOTIONAL.value:
        rate = fee.taker_fee_rate if policy.assume_taker_exit else fee.maker_fee_rate
        if rate is None:
            acc.reason(R.FEE_UNAVAILABLE, fatal=True)
            return 0.0, 0.0
        ccy = rate * notional
    elif fee.fee_model in (FeeModel.PER_CONTRACT.value, FeeModel.SPREAD_PLUS_COMMISSION.value):
        if fee.commission_per_contract is None and fee.broker_commission_rate is None:
            acc.reason(R.FEE_UNAVAILABLE, fatal=True)
            return 0.0, 0.0
        if fee.commission_per_contract is not None:
            per = fee.commission_per_contract + (fee.exchange_fee_per_contract or 0.0) + (fee.clearing_fee_per_contract or 0.0)
            ccy += per * (q / (fee.commission_contract_size or 1.0)) * conv
        if fee.broker_commission_rate is not None:
            ccy += fee.broker_commission_rate * notional
    if fee.other_charge_per_trade:
        ccy += fee.other_charge_per_trade * conv
    fee_r = ccy / risk_ccy if risk_ccy > 0 else 0.0
    return fee_r, policy.fee_uncertainty_fraction.get(fee.source, 1.0) * fee_r


def _exit_slippage_r(obs, side: str, q: float, price: float, risk: float, policy: VenueCostPolicy,
                     acc: _Acc) -> Tuple[float, float]:
    sl = obs.slippage_observation
    floor_px = policy.min_slippage_bps_floor / 1e4 * price
    if sl.source == SlippageSource.DEPTH_WALK.value and sl.depth_asks and sl.depth_bids:
        levels = sl.depth_bids if side == "LONG" else sl.depth_asks  # exiting a LONG sells into the bids
        vwap, full = _walk(levels, q, side != "LONG", policy.beyond_depth_penalty_bps)
        px = max(abs(vwap - levels[0][0]), floor_px)
        if not full:
            acc.reason(R.DEPTH_INSUFFICIENT_FOR_SIZE)
    else:
        bps = sl.per_side_bps if sl.per_side_bps is not None else policy.conservative_slippage_bps
        top = sl.top_bid_quantity if side == "LONG" else sl.top_ask_quantity
        scale = max(1.0, math.sqrt(q / top)) if top else 1.0
        px = max(bps * scale / 1e4 * price, floor_px)
    slip_r = px / risk
    return slip_r, policy.slippage_uncertainty_fraction.get(sl.source, 2.0) * slip_r


def build_remaining_cost_estimate(
    *,
    path,
    observation: Optional[VenueEconomicObservation],
    expected_remaining_holding_ms: int,
    max_remaining_holding_ms: int,
    policy: Optional[VenueCostPolicy] = None,
):
    """Remaining costs for one open position, from ``path.current_time``.

    Reuses the Section 17 component models (funding stamps, FX rollovers,
    futures basis convergence, spread, depth walk) with t0 = NOW, the
    CURRENT price and the REMAINING hold -- never the original full-hold
    estimate. Only the EXIT side of fee/spread/slippage is future cost (the
    entry side is already paid). Sunk costs (entry fees, funding/financing
    already paid) are reported in ``costs_already_realized_R`` and are never
    part of ``expected_future_holding_and_exit_costs_R``. Every R value is in
    ORIGINAL-R units (see contracts/position.py). Fail closed: missing or
    non-causal evidence -> source_quality INVALID (unknown cost is never 0).
    """
    from app.trading_intelligence.contracts.position import RemainingCostEstimate

    policy = policy or default_venue_cost_policy()
    R0 = float(path.original_R_reference)
    mult = float(path.contract_multiplier or 1.0)
    orig_risk_ccy = R0 * max(float(path.original_quantity), 1e-12) * mult
    entry_fee_R = float(path.entry_fees_paid) / orig_risk_ccy
    funding_paid_R = float(path.funding_paid_or_accrued) / orig_risk_ccy
    financing_paid_R = float(path.financing_paid_or_accrued) / orig_risk_ccy
    sunk = entry_fee_R + funding_paid_R + financing_paid_R
    acc = _Acc()
    hold, max_hold = max(0, int(expected_remaining_holding_ms)), max(0, int(max_remaining_holding_ms))

    def _result(quality, parts, unc, obs_id, policy_hash):
        fee_r, sp_r, sl_r, fu_r, fi_r, ca_r = parts
        reasons = list(acc.reasons)
        return RemainingCostEstimate(
            remaining_cost_id=short_id("rcost", {"path": path.position_path_id, "obs": obs_id, "hold": hold,
                                                 "max_hold": max_hold, "policy": policy_hash}),
            venue_observation_id=obs_id, decision_time=int(path.current_time),
            expected_remaining_holding_ms=hold, max_remaining_holding_ms=max_hold,
            exit_fee_R=fee_r, exit_spread_R=sp_r, exit_slippage_R=sl_r, future_funding_R=fu_r,
            future_financing_R=fi_r, future_carry_R=ca_r,
            expected_future_holding_and_exit_costs_R=fee_r + sp_r + sl_r + fu_r + fi_r + ca_r,
            remaining_cost_uncertainty_R=unc, costs_already_realized_R=sunk, entry_fees_paid_R=entry_fee_R,
            funding_paid_R=funding_paid_R, financing_paid_R=financing_paid_R, source_quality=quality,
            reason_codes=tuple(dict.fromkeys(reasons)), cost_policy_hash=policy_hash)

    if observation is None:
        acc.reason("REMAINING_COST_OBSERVATION_MISSING", fatal=True)
        return _result("INVALID", (0.0,) * 6, 0.0, None, policy.policy_hash)
    obs = observation
    for code in obs.reason_codes:
        acc.reason(code, fatal=code in FATAL_VENUE_REASONS)
    if obs.instrument_key.canonical_symbol != path.instrument_key.canonical_symbol:
        acc.reason(R.INSTRUMENT_MAPPING_MISMATCH, fatal=True)
    if obs.observed_at > path.current_time or obs.decision_time > path.current_time:
        acc.reason(R.NON_CAUSAL_OBSERVATION, fatal=True)
    if not obs.session_state.tradable:
        acc.reason(R.MARKET_CLOSED if obs.session_state.status in (SessionStatus.CLOSED.value,
                                                                   SessionStatus.ROLLOVER.value)
                   else R.SESSION_UNKNOWN, fatal=True)
    price, side = float(path.current_price), path.side
    q = float(path.current_quantity) if path.current_quantity > 0 else float(
        max(getattr(obs.instrument_metadata, "minimum_quantity", 0.0) or 0.0,
            getattr(obs.instrument_metadata, "step_size", 1e-9) or 1e-9))
    risk_ccy = R0 * q * mult
    thin = obs.session_state.status == SessionStatus.THIN.value
    fee_r, fee_u = _exit_fee_r(obs, q, price, mult, risk_ccy, policy, acc)
    sp_full, sp_u_full = _spread(obs, price, R0, policy, thin, acc)
    sp_r, sp_u = sp_full / 2.0, sp_u_full / 2.0  # only the exit half-spread is still to be paid
    sl_r, sl_u = _exit_slippage_r(obs, side, q, price, R0, policy, acc)
    fu_r, fu_u = _funding(obs, side, price, R0, hold, max_hold, policy, acc)
    fi_r, fi_u = _financing(obs, side, price, R0, hold, max_hold, policy, acc)
    ca_r, ca_u = _carry(obs, side, price, R0, hold, max_hold, policy, acc)
    unc = fee_u + sp_u + sl_u + fu_u + fi_u + ca_u
    unc += policy.adapter_uncertainty_fraction.get(obs.adapter_status, 1.0) * (
        abs(fee_r) + abs(sp_r) + abs(sl_r) + abs(fu_r) + abs(fi_r) + abs(ca_r))
    if acc.fatal:
        quality = "INVALID"
    else:
        quality = "DEGRADED" if any(l.quality == "FALLBACK" for l in acc.lineage) or obs.source_quality != "VALID" \
            else "VALID"
    return _result(quality, (fee_r, sp_r, sl_r, fu_r, fi_r, ca_r), unc, obs.observation_id, policy.policy_hash)


__all__ = ["expected_holding", "rollover_count", "build_venue_cost_estimate", "build_remaining_cost_estimate"]
