"""Portfolio selector (Sections 16.13-16.21) -- pure and deterministic.

PortfolioScore(S) = sum(RankScore_i)
                    - lambda_corr   * pairwise_correlation_penalty(S)
                    - lambda_beta   * common_factor_concentration(S)
                    - lambda_sector * sector_concentration(S)
                    - lambda_liq    * liquidity_concentration(S)

* pairwise penalty = sum max(0, effective_corr - tolerance) over selected
  pairs AND selected-vs-existing account exposures; effective_corr =
  rho_shrunk * sign_i * sign_j (a LONG/SHORT pair with positive correlation
  offsets rather than compounds).
* factor / sector penalties are INCREMENTAL: pen(S + existing) - pen(existing).
  Factors are asset-class-neutral (``portfolio/factors.FactorModel``): crypto
  statistical betas, FX structural currency legs, futures configured
  factors -- the selector never names a specific factor or symbol.
* exposure weights are the PRE_SIZE_EXPOSURE_PROXY (one unit each), not real
  notional -- there is no TradePlan sizing until Section 18.
* The EMPTY set is always a feasible choice; free slots never force a pick.
* Exact enumeration for small slot counts; a deterministic bounded beam
  behind the same interface for larger ones.

It is NOT hard risk: no leverage, margin, liquidation or final-capacity
logic lives here, and nothing here reserves anything (see service.py).
"""
from __future__ import annotations

import itertools
from typing import Dict, List, Mapping, Optional, Sequence, Set, Tuple

from app.trading_intelligence.contracts.exposure import AccountExposureSnapshot, ExposureRecord, ExposureStatus
from app.trading_intelligence.contracts.portfolio_intel import (
    ACCOUNT_CORRELATION_CONFLICT, ACCOUNT_RESERVATION_CONFLICT, BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK,
    COMMON_FACTOR_CONCENTRATION, DUPLICATE_EXPOSURE,
    INSUFFICIENT_CORRELATION_HISTORY_FALLBACK, NO_AVAILABLE_SLOTS, NOT_SELECTED_BY_OBJECTIVE,
    PortfolioMarketContext, PortfolioPolicy, PortfolioSelectionDecision, RejectedCandidate, ScoreBreakdown,
    SOLVER_BEAM, SOLVER_EXACT, SOLVER_NONE,
)
from app.trading_intelligence.contracts.ranking import RankedOpportunity
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.portfolio.exposure_builder import account_state_fingerprint
from app.trading_intelligence.portfolio.factors import FactorModel
from app.trading_intelligence.portfolio.groups import UNKNOWN_GROUP
from app.trading_intelligence.portfolio.returns import effective_correlation, pair_correlation, side_sign


class _Cand:
    """Selector-internal view of one ranked opportunity."""

    __slots__ = ("ranked", "key", "symbol", "canonical", "side", "group", "liq", "score", "cid")

    def __init__(self, ranked: RankedOpportunity, ctx: PortfolioMarketContext):
        self.ranked = ranked
        self.key = ranked.instrument_key
        self.symbol = ranked.instrument_key.venue_symbol.upper()
        self.canonical = ranked.instrument_key.canonical_symbol
        self.side = ranked.side
        self.group = ctx.instrument_groups.get(self.symbol, UNKNOWN_GROUP)
        self.liq = ranked.liquidity_quality
        self.score = ranked.rank_score
        self.cid = ranked.setup_candidate_id


class _Existing:
    __slots__ = ("key", "symbol", "canonical", "side", "group", "status")

    def __init__(self, rec: ExposureRecord, ctx: PortfolioMarketContext):
        self.key = rec.instrument_key
        self.symbol = rec.instrument_key.venue_symbol.upper()
        self.canonical = rec.instrument_key.canonical_symbol
        self.side = rec.side
        self.group = ctx.instrument_groups.get(self.symbol, UNKNOWN_GROUP)
        self.status = rec.exposure_status


def exposure_fingerprint(exposure: AccountExposureSnapshot) -> str:
    """Content hash of the account state (excludes the random snapshot id)."""
    return account_state_fingerprint(exposure.broker_account_id, exposure.all_exposures)


def duplicate_findings(
    candidates: Sequence[_Cand], existing: Sequence[_Existing], policy: PortfolioPolicy,
) -> Tuple[List[_Cand], List[RejectedCandidate]]:
    """Section 16.13: never select a candidate whose canonical economic
    instrument already has an open position, pending entry or active CATI
    reservation on the same broker account (unless hedge duplicates are
    explicitly permitted for the opposite side)."""
    feasible, rejected = [], []
    for c in candidates:
        clash = None
        for e in existing:
            if e.canonical != c.canonical:
                continue
            if policy.allow_hedge_mode_duplicates and e.side != c.side:
                continue
            clash = e
            break
        if clash is None:
            feasible.append(c)
        else:
            reason = ACCOUNT_RESERVATION_CONFLICT if clash.status == ExposureStatus.SHADOW_RESERVED.value else DUPLICATE_EXPOSURE
            rejected.append(RejectedCandidate(c.ranked.ranked_opportunity_id, c.cid, reason,
                                              f"{c.canonical} already {clash.status} on this account"))
    return feasible, rejected


class _Scorer:
    def __init__(self, existing: Sequence[_Existing], ctx: PortfolioMarketContext, policy: PortfolioPolicy):
        self.existing, self.ctx, self.p = list(existing), ctx, policy
        self.model = FactorModel.from_policy(policy)
        self._corr: Dict[Tuple[str, str], object] = {}
        self._exp: Dict[Tuple[str, str], tuple] = {}
        self.fallbacks: Set[str] = set()
        self._base_factor = self.factor_net([])
        self._base_sector = self._sector_net([])

    # -- lazily cached estimates ----------------------------------------------------
    def _rho(self, a: str, b: str):
        key = (a, b) if a <= b else (b, a)
        if key not in self._corr:
            self._corr[key] = pair_correlation(key[0], key[1], self.ctx, self.p)
        est = self._corr[key]
        if est.fallback_used:
            self.fallbacks.add(f"{INSUFFICIENT_CORRELATION_HISTORY_FALLBACK}:{key[0]}|{key[1]}")
        return est

    def _exposures(self, item) -> tuple:
        k = (item.symbol, item.side)
        if k not in self._exp:
            self._exp[k] = self.model.exposures(item.symbol, item.key, side_sign(item.side), self.ctx)
        for e in self._exp[k]:
            for r in e.reason_codes:
                self.fallbacks.add(f"{r}:{item.symbol}|{e.factor_id}")
        return self._exp[k]

    # -- components ------------------------------------------------------------------
    def _pair_pen(self, a, b) -> float:
        return max(0.0, effective_correlation(self._rho(a.symbol, b.symbol), a.side, b.side) - self.p.correlation_tolerance)

    def correlation_penalty(self, subset: Sequence[_Cand]) -> float:
        pen = 0.0
        for a, b in itertools.combinations(subset, 2):
            pen += self._pair_pen(a, b)
        for a in subset:
            for e in self.existing:
                if e.symbol == a.symbol:
                    continue
                pen += self._pair_pen(a, e)
        return pen

    def factor_net(self, subset: Sequence[_Cand]) -> Dict[str, float]:
        net: Dict[str, float] = {}
        for item in list(subset) + self.existing:
            for e in self._exposures(item):
                net[e.factor_id] = net.get(e.factor_id, 0.0) + e.exposure_value
        return net

    def factor_penalty(self, subset: Sequence[_Cand]) -> float:
        now = self.factor_net(subset)
        pen = 0.0
        for fid in set(now) | set(self._base_factor):
            tol = self.model.tolerance(fid)
            pen += max(0.0, abs(now.get(fid, 0.0)) - tol) - max(0.0, abs(self._base_factor.get(fid, 0.0)) - tol)
        return max(0.0, pen)

    def _sector_net(self, subset: Sequence[_Cand]) -> Dict[str, float]:
        net: Dict[str, float] = {}
        for item in list(subset) + self.existing:
            if item.group == UNKNOWN_GROUP:  # unknown stays unknown: no false precision
                continue
            net[item.group] = net.get(item.group, 0.0) + self.p.pre_size_unit_weight * side_sign(item.side)
        return net

    def sector_penalty(self, subset: Sequence[_Cand]) -> float:
        now, tol = self._sector_net(subset), self.p.sector_tolerance_units
        pen = 0.0
        for g, v in now.items():
            pen += max(0.0, abs(v) - tol) - max(0.0, abs(self._base_sector.get(g, 0.0)) - tol)
        return max(0.0, pen)

    def liquidity_penalty(self, subset: Sequence[_Cand]) -> float:
        low = sum(1 for c in subset if c.liq < self.p.liquidity_low_quality)
        return float(max(0, low - self.p.liquidity_low_tolerance))

    def score(self, subset: Sequence[_Cand]) -> Tuple[float, ScoreBreakdown]:
        raw = sum(c.score for c in subset)
        cp, fp, sp, lp = (self.correlation_penalty(subset), self.factor_penalty(subset),
                          self.sector_penalty(subset), self.liquidity_penalty(subset))
        total = raw - self.p.lambda_corr * cp - self.p.lambda_beta * fp - self.p.lambda_sector * sp - self.p.lambda_liq * lp
        return total, ScoreBreakdown(raw, cp, fp, sp, lp)

    def unselected_reason(self, c: _Cand) -> str:
        """Why a feasible candidate was left out: its own conflict with what
        the account already holds, if any; otherwise the objective."""
        if any(self._pair_pen(c, e) > 0.0 for e in self.existing if e.symbol != c.symbol):
            return ACCOUNT_CORRELATION_CONFLICT
        if self.factor_penalty([c]) > 0.0:
            return COMMON_FACTOR_CONCENTRATION
        return NOT_SELECTED_BY_OBJECTIVE


def _valid_subset(subset: Sequence[_Cand], policy: PortfolioPolicy) -> bool:
    """No two selected candidates may be the same economic instrument."""
    seen: Dict[str, str] = {}
    for c in subset:
        prev = seen.get(c.canonical)
        if prev is not None and not (policy.allow_hedge_mode_duplicates and prev != c.side):
            return False
        seen[c.canonical] = c.side
    return True


def _order_key(item: Tuple[float, Tuple[_Cand, ...]]):
    score, subset = item
    return (-round(score, 12), len(subset), tuple(sorted(c.cid for c in subset)))


def _solve_exact(feasible: Sequence[_Cand], slots: int, scorer: _Scorer, policy: PortfolioPolicy):
    best = None
    for k in range(0, min(slots, len(feasible)) + 1):
        for subset in itertools.combinations(feasible, k):
            if not _valid_subset(subset, policy):
                continue
            item = (scorer.score(subset)[0], tuple(subset))
            if best is None or _order_key(item) < _order_key(best):
                best = item
    return best[1]


def _solve_beam(feasible: Sequence[_Cand], slots: int, scorer: _Scorer, policy: PortfolioPolicy):
    beam: List[Tuple[float, Tuple[_Cand, ...], int]] = [(0.0, (), -1)]
    best = (0.0, ())
    for _ in range(min(slots, len(feasible))):
        nxt = []
        for _score, subset, last in beam:
            for idx in range(last + 1, len(feasible)):
                cand = subset + (feasible[idx],)
                if _valid_subset(cand, policy):
                    nxt.append((scorer.score(cand)[0], cand, idx))
        if not nxt:
            break
        nxt.sort(key=lambda t: _order_key((t[0], t[1])))
        beam = nxt[: policy.beam_width]
        for s, cand, _ in beam:
            if _order_key((s, cand)) < _order_key(best):
                best = (s, cand)
    return best[1]


def select_portfolio(
    *,
    ranked: Sequence[RankedOpportunity],
    exposure: AccountExposureSnapshot,
    context: PortfolioMarketContext,
    policy: PortfolioPolicy,
    bot_instance_id: str,
    cycle_id: str,
    available_slots: int,
    decision_time: int,
) -> PortfolioSelectionDecision:
    """Pure selection. Does NOT reserve anything (service.py owns the
    account-scoped reservation transaction)."""
    ordered = sorted(ranked, key=lambda r: r.rank_position)  # ranking already fixed the order
    cands = [_Cand(r, context) for r in ordered]
    existing = [_Existing(r, context) for r in exposure.all_exposures]
    feasible, rejected = duplicate_findings(cands, existing, policy)
    scorer = _Scorer(existing, context, policy)

    reason_codes: List[str] = []
    if available_slots <= 0:
        selected: Tuple[_Cand, ...] = ()
        solver = SOLVER_NONE
        reason_codes.append(NO_AVAILABLE_SLOTS)
    elif not feasible:
        selected, solver = (), SOLVER_NONE
    elif available_slots <= policy.exact_enumeration_max_slots:
        selected, solver = _solve_exact(feasible, available_slots, scorer, policy), SOLVER_EXACT
    else:
        selected, solver = _solve_beam(feasible, available_slots, scorer, policy), SOLVER_BEAM

    total, breakdown = scorer.score(selected)
    breakdown = ScoreBreakdown(breakdown.raw_rank_sum, breakdown.correlation_penalty, breakdown.factor_penalty,
                               breakdown.sector_penalty, breakdown.liquidity_penalty,
                               tuple(sorted(scorer.fallbacks)),
                               tuple(sorted((f, round(v, 12)) for f, v in scorer.factor_net(selected).items())))
    chosen_ids = {c.ranked.ranked_opportunity_id for c in selected}
    for c in feasible:
        if c.ranked.ranked_opportunity_id not in chosen_ids:
            rejected.append(RejectedCandidate(c.ranked.ranked_opportunity_id, c.cid, scorer.unselected_reason(c)))
    if any(f.startswith(INSUFFICIENT_CORRELATION_HISTORY_FALLBACK) for f in scorer.fallbacks):
        reason_codes.append(INSUFFICIENT_CORRELATION_HISTORY_FALLBACK)
    if any(f.startswith(BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK) for f in scorer.fallbacks):
        reason_codes.append(BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK)
    reason_codes += sorted({r.reason_code for r in rejected if r.reason_code != NOT_SELECTED_BY_OBJECTIVE})

    ranked_ids = tuple(r.ranked_opportunity_id for r in ordered)
    policy_hash = policy.policy_hash
    return PortfolioSelectionDecision(
        portfolio_selection_id=PortfolioSelectionDecision.build_id(
            broker_account_id=exposure.broker_account_id, bot_instance_id=bot_instance_id, cycle_id=cycle_id,
            ranked_ids=ranked_ids, exposure_snapshot_hash=exposure_fingerprint(exposure),
            context_hash=context.context_hash, policy_hash=policy_hash),
        broker_account_id=exposure.broker_account_id, bot_instance_id=bot_instance_id, cycle_id=cycle_id,
        account_exposure_snapshot_id=exposure.exposure_snapshot_id,
        portfolio_market_context_id=context.portfolio_market_context_id,
        ranked_opportunity_ids=ranked_ids,
        selected_opportunity_ids=tuple(c.ranked.ranked_opportunity_id for c in selected),
        rejected_candidates=tuple(rejected), available_slots=available_slots, portfolio_score=total,
        score_breakdown=breakdown, solver=solver, portfolio_policy_version=policy.schema_version,
        portfolio_policy_hash=policy_hash, reservation_id=None, reservation_status="NOT_REQUIRED",
        reason_codes=tuple(dict.fromkeys(reason_codes)), decision_time=decision_time,
    )


__all__ = ["exposure_fingerprint", "duplicate_findings", "select_portfolio"]
