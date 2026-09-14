from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from app.risk.adaptive_daily_budget import (
    AdaptiveDailyRiskBudgetEngine,
    AdaptiveDailyRiskInputs,
    AdaptiveDailyRiskPolicy,
    DailyRiskState,
    POLICY_VERSION,
)


DEFAULT_R_GRID = (1.0, 1.25, 1.5, 1.75, 2.0, 2.5)
DEFAULT_CAP_GRID = (0.015, 0.020, 0.025, 0.030, 0.035)
DEFAULT_CANDIDATE_R = 1.5
DEFAULT_CANDIDATE_CAP = 0.025
DEFAULT_STARTING_EQUITY = 761.30525668


@dataclass(frozen=True)
class Opportunity:
    decision_id: str
    position_id: str
    symbol: str
    side: str
    regime: str
    evaluated_at: datetime
    closed_at: datetime
    planned_risk_usdt: float
    realized_pnl_usdt: float
    fees_usdt: float = 0.0
    funding_usdt: float = 0.0


@dataclass(frozen=True)
class RiskObservation:
    evaluated_at: datetime
    risk_amount_usdt: float


@dataclass(frozen=True)
class ReplayPolicy:
    name: str
    kind: str
    daily_r_budget: float | None = None
    hard_daily_equity_cap_pct: float | None = None
    fixed_daily_loss_usdt: float | None = None


@dataclass
class OpenReplayTrade:
    opportunity: Opportunity
    reservation_id: str
    risk_date: date


@dataclass
class PolicyReplayResult:
    policy_name: str
    policy: dict[str, Any]
    opportunities_seen: int
    approved_trades: int
    blocked_trades: int
    blocked_winners: int
    blocked_losers: int
    blocked_flat: int
    blocked_net_pnl_usdt: float
    net_pnl_usdt: float
    return_pct: float
    fees_usdt: float
    funding_usdt: float
    slippage_cost_usdt: float
    expectancy_usdt: float
    profit_factor: float | None
    win_rate: float | None
    average_winner_usdt: float | None
    average_loser_usdt: float | None
    median_winner_usdt: float | None
    median_loser_usdt: float | None
    max_daily_loss_usdt: float
    max_daily_drawdown_pct: float
    max_weekly_drawdown_pct: float
    max_drawdown_pct: float
    profitable_days: int
    losing_days: int
    flat_days: int
    hard_stop_days: int
    caution_events: int
    defensive_events: int
    hard_stop_events: int
    avg_trades_per_day: float
    median_trades_per_day: float
    longest_losing_streak: int
    recovery_duration_days: int | None
    avg_daily_budget_used_pct: float
    median_daily_budget_used_pct: float
    p95_daily_budget_used_pct: float
    by_regime: dict[str, dict[str, Any]]
    day_open_equity: dict[str, float]
    daily_pnl: dict[str, float]
    daily_budget_used_pct: dict[str, float]
    approved_decision_ids: list[str]
    blocked_decision_ids: list[str]
    blocked_positive_r: int
    blocked_negative_r: int
    state_trace: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplayDataset:
    opportunities: tuple[Opportunity, ...]
    risk_observations: tuple[RiskObservation, ...]
    dataset_hash: str
    source_summary: dict[str, Any]


def parse_utc(value: str | None) -> datetime:
    if not value:
        raise ValueError("missing timestamp")
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def dataset_hash(opportunities: Sequence[Opportunity], risk_observations: Sequence[RiskObservation]) -> str:
    payload = {
        "opportunities": [
            {
                "decision_id": o.decision_id,
                "position_id": o.position_id,
                "evaluated_at": o.evaluated_at.isoformat(),
                "closed_at": o.closed_at.isoformat(),
                "risk": round(o.planned_risk_usdt, 12),
                "pnl": round(o.realized_pnl_usdt, 12),
            }
            for o in opportunities
        ],
        "risk_observations": [
            {"evaluated_at": r.evaluated_at.isoformat(), "risk": round(r.risk_amount_usdt, 12)}
            for r in risk_observations
        ],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def load_replay_dataset(conn: sqlite3.Connection) -> ReplayDataset:
    conn.row_factory = sqlite3.Row
    opportunity_rows = conn.execute(
        """
        SELECT
            d.decision_id,
            p.position_id,
            d.symbol,
            COALESCE(p.side, 'UNKNOWN') AS side,
            COALESCE(d.regime, 'UNKNOWN') AS regime,
            d.evaluated_at,
            p.closed_at,
            d.risk_amount,
            COALESCE(p.realized_pnl, 0.0) AS realized_pnl,
            COALESCE((SELECT SUM(f.fee) FROM trade_fills f WHERE f.position_id = p.position_id), 0.0) AS fees
        FROM trading_decisions d
        JOIN positions p ON p.decision_id = d.decision_id
        WHERE d.evaluated_at IS NOT NULL
          AND p.closed_at IS NOT NULL
          AND d.risk_amount IS NOT NULL
          AND d.risk_amount > 0
          AND p.realized_pnl IS NOT NULL
          AND p.status = 'CLOSED'
        ORDER BY d.evaluated_at ASC, d.decision_id ASC
        """
    ).fetchall()
    opportunities = tuple(
        Opportunity(
            decision_id=str(row["decision_id"]),
            position_id=str(row["position_id"]),
            symbol=str(row["symbol"]),
            side=str(row["side"]),
            regime=str(row["regime"]),
            evaluated_at=parse_utc(row["evaluated_at"]),
            closed_at=parse_utc(row["closed_at"]),
            planned_risk_usdt=float(row["risk_amount"]),
            realized_pnl_usdt=float(row["realized_pnl"]),
            fees_usdt=float(row["fees"] or 0.0),
        )
        for row in opportunity_rows
    )
    risk_rows = conn.execute(
        """
        SELECT evaluated_at, risk_amount
        FROM trading_decisions
        WHERE evaluated_at IS NOT NULL
          AND risk_amount IS NOT NULL
          AND risk_amount > 0
        ORDER BY evaluated_at ASC, decision_id ASC
        """
    ).fetchall()
    risk_observations = tuple(
        RiskObservation(parse_utc(row["evaluated_at"]), float(row["risk_amount"]))
        for row in risk_rows
    )
    summary = {
        "opportunity_count": len(opportunities),
        "risk_observation_count": len(risk_observations),
        "first_opportunity_at": opportunities[0].evaluated_at.isoformat() if opportunities else None,
        "last_opportunity_at": opportunities[-1].evaluated_at.isoformat() if opportunities else None,
        "old_daily_loss_blocked_decisions": _scalar_int(
            conn,
            """
            SELECT COUNT(*)
            FROM trading_decisions
            WHERE COALESCE(primary_reason, '') LIKE '%Daily loss limit%'
            """,
        ),
        "old_daily_loss_unique_risk_days": _scalar_int(
            conn,
            """
            SELECT COUNT(DISTINCT substr(evaluated_at, 1, 10))
            FROM trading_decisions
            WHERE COALESCE(primary_reason, '') LIKE '%Daily loss limit%'
              AND evaluated_at IS NOT NULL
            """,
        ),
    }
    return ReplayDataset(
        opportunities=opportunities,
        risk_observations=risk_observations,
        dataset_hash=dataset_hash(opportunities, risk_observations),
        source_summary=summary,
    )


def default_policy_grid() -> list[ReplayPolicy]:
    policies = [ReplayPolicy(name="OLD_6_USDT", kind="fixed", fixed_daily_loss_usdt=6.0)]
    policies.extend(
        ReplayPolicy(
            name=f"R_{str(r).replace('.', '_')}_CAP_2_5",
            kind="adaptive",
            daily_r_budget=r,
            hard_daily_equity_cap_pct=DEFAULT_CANDIDATE_CAP,
        )
        for r in DEFAULT_R_GRID
    )
    policies.extend(
        ReplayPolicy(
            name=f"R_1_5_CAP_{int(cap * 1000):03d}",
            kind="adaptive",
            daily_r_budget=DEFAULT_CANDIDATE_R,
            hard_daily_equity_cap_pct=cap,
        )
        for cap in DEFAULT_CAP_GRID
        if cap != DEFAULT_CANDIDATE_CAP
    )
    return policies


def replay_policies(
    dataset: ReplayDataset,
    policies: Sequence[ReplayPolicy] | None = None,
    *,
    starting_equity: float = DEFAULT_STARTING_EQUITY,
    bot_instance_id: str = "adaptive-daily-risk-replay",
) -> dict[str, PolicyReplayResult]:
    active_policies = list(policies or default_policy_grid())
    return {
        policy.name: replay_policy(
            dataset.opportunities,
            dataset.risk_observations,
            policy,
            starting_equity=starting_equity,
            bot_instance_id=bot_instance_id,
        )
        for policy in active_policies
    }


def replay_policy(
    opportunities: Sequence[Opportunity],
    risk_observations: Sequence[RiskObservation],
    policy: ReplayPolicy,
    *,
    starting_equity: float = DEFAULT_STARTING_EQUITY,
    bot_instance_id: str = "adaptive-daily-risk-replay",
) -> PolicyReplayResult:
    if policy.kind == "adaptive":
        engine = AdaptiveDailyRiskBudgetEngine(
            AdaptiveDailyRiskPolicy(
                max_daily_loss_pct=float(policy.hard_daily_equity_cap_pct or DEFAULT_CANDIDATE_CAP),
                daily_r_budget=float(policy.daily_r_budget or DEFAULT_CANDIDATE_R),
            )
        )
    else:
        engine = None

    opportunities = tuple(sorted(opportunities, key=lambda o: (o.evaluated_at, o.decision_id)))
    risk_observations = tuple(sorted(risk_observations, key=lambda r: r.evaluated_at))
    risk_index = 0
    prior_risks: list[float] = []
    prior_rs: list[float] = []
    open_trades: list[OpenReplayTrade] = []
    day_open_equity: dict[str, float] = {}
    daily_pnl: dict[str, float] = defaultdict(float)
    daily_reserved: dict[str, float] = defaultdict(float)
    daily_consumed: dict[str, float] = defaultdict(float)
    daily_effective_budget: dict[str, float] = {}
    daily_budget_used_pct: dict[str, float] = {}
    daily_trade_counts: dict[str, int] = defaultdict(int)
    daily_hard_stopped: set[str] = set()
    daily_states: dict[str, str] = defaultdict(lambda: DailyRiskState.NORMAL.value)
    state_trace: list[dict[str, Any]] = []
    approved: list[Opportunity] = []
    blocked: list[Opportunity] = []
    equity = float(starting_equity)
    equity_curve: list[tuple[datetime, float]] = []
    realised_r_events: list[tuple[date, float]] = []
    caution_events = defensive_events = hard_stop_events = 0

    def risk_date_for(ts: datetime) -> date:
        if engine is not None:
            return engine.risk_date_for(ts)
        return AdaptiveDailyRiskBudgetEngine().risk_date_for(ts)

    def ensure_day(day: date) -> str:
        key = str(day)
        day_open_equity.setdefault(key, equity)
        return key

    def close_due(cutoff: datetime) -> None:
        nonlocal equity, open_trades
        keep: list[OpenReplayTrade] = []
        for trade in sorted(open_trades, key=lambda t: t.opportunity.closed_at):
            if trade.opportunity.closed_at <= cutoff:
                day_key = ensure_day(risk_date_for(trade.opportunity.closed_at))
                pnl = float(trade.opportunity.realized_pnl_usdt)
                equity += pnl
                daily_pnl[day_key] += pnl
                daily_consumed[day_key] = max(0.0, -daily_pnl[day_key])
                if engine is not None:
                    engine.settle_full(bot_instance_id, trade.risk_date, trade.reservation_id)
                else:
                    daily_reserved[str(trade.risk_date)] = max(
                        0.0, daily_reserved[str(trade.risk_date)] - trade.opportunity.planned_risk_usdt
                    )
                r_mult = pnl / trade.opportunity.planned_risk_usdt if trade.opportunity.planned_risk_usdt > 0 else 0.0
                prior_rs.append(r_mult)
                realised_r_events.append((trade.risk_date, r_mult))
                equity_curve.append((trade.opportunity.closed_at, equity))
            else:
                keep.append(trade)
        open_trades = keep

    for opportunity in opportunities:
        close_due(opportunity.evaluated_at)
        while risk_index < len(risk_observations) and risk_observations[risk_index].evaluated_at < opportunity.evaluated_at:
            prior_risks.append(risk_observations[risk_index].risk_amount_usdt)
            risk_index += 1

        risk_date = risk_date_for(opportunity.evaluated_at)
        day_key = ensure_day(risk_date)
        hard_stop = False
        remaining = float("inf")
        effective_budget = float("inf")
        state = daily_states[day_key]
        reason = "OLD_DAILY_LOSS_OK"

        if policy.kind == "fixed":
            effective_budget = float(policy.fixed_daily_loss_usdt or 0.0)
            consumed = max(0.0, -daily_pnl[day_key])
            reserved = daily_reserved[day_key]
            remaining = max(0.0, effective_budget - consumed - reserved)
            if consumed >= effective_budget > 0 or remaining <= 1e-9:
                hard_stop = True
                state = DailyRiskState.HARD_STOP.value
                reason = "OLD_6_USDT_DAILY_LOSS_STOP"
        else:
            assert engine is not None
            decision = engine.evaluate(
                AdaptiveDailyRiskInputs(
                    bot_instance_id=bot_instance_id,
                    risk_date=risk_date,
                    day_open_equity=day_open_equity[day_key],
                    current_equity=equity,
                    realized_pnl_today=daily_pnl[day_key],
                    fees_today=0.0,
                    funding_today=0.0,
                    realized_r_today=sum(r for d, r in realised_r_events if str(d) == day_key),
                    initial_risk_history_usdt=tuple(prior_risks),
                    recent_r_history=tuple(prior_rs),
                    account_drawdown_pct=_max_drawdown_pct([v for _, v in equity_curve] or [equity]),
                    volatility_stress=0.0,
                    market_regime=opportunity.regime,
                )
            )
            effective_budget = decision.effective_daily_budget_usdt
            remaining = decision.remaining_daily_risk_usdt
            state = decision.daily_risk_state
            reason = decision.decision_reason
            if state == DailyRiskState.CAUTION.value:
                caution_events += 1
            if state == DailyRiskState.DEFENSIVE.value:
                defensive_events += 1
            if state == DailyRiskState.HARD_STOP.value:
                hard_stop = True
                hard_stop_events += 1

        daily_effective_budget[day_key] = max(daily_effective_budget.get(day_key, 0.0), effective_budget)
        planned = float(opportunity.planned_risk_usdt)
        can_reserve = planned <= remaining + 1e-9 and not hard_stop
        if policy.kind == "adaptive" and can_reserve:
            assert engine is not None
            can_reserve = engine.reserve(bot_instance_id, risk_date, opportunity.decision_id, planned, decision)
        elif policy.kind == "fixed" and can_reserve:
            daily_reserved[day_key] += planned

        if can_reserve:
            approved.append(opportunity)
            daily_trade_counts[day_key] += 1
            open_trades.append(OpenReplayTrade(opportunity, opportunity.decision_id, risk_date))
            state_trace.append(
                {
                    "decision_id": opportunity.decision_id,
                    "at": opportunity.evaluated_at.isoformat(),
                    "risk_date": day_key,
                    "action": "APPROVED",
                    "state": state,
                    "reason": reason,
                    "planned_risk_usdt": planned,
                    "remaining_before_entry_usdt": remaining,
                    "effective_budget_usdt": effective_budget,
                }
            )
        else:
            blocked.append(opportunity)
            daily_hard_stopped.add(day_key)
            daily_states[day_key] = DailyRiskState.HARD_STOP.value
            state_trace.append(
                {
                    "decision_id": opportunity.decision_id,
                    "at": opportunity.evaluated_at.isoformat(),
                    "risk_date": day_key,
                    "action": "BLOCKED",
                    "state": DailyRiskState.HARD_STOP.value if hard_stop else state,
                    "reason": "PLANNED_RISK_EXCEEDS_REMAINING_DAILY_BUDGET" if not hard_stop else reason,
                    "planned_risk_usdt": planned,
                    "remaining_before_entry_usdt": remaining,
                    "effective_budget_usdt": effective_budget,
                }
            )

    if opportunities:
        close_due(max(o.closed_at for o in opportunities) + timedelta(seconds=1))
    for day_key, budget in daily_effective_budget.items():
        used = daily_consumed[day_key]
        if policy.kind == "fixed":
            used += daily_reserved[day_key]
        daily_budget_used_pct[day_key] = 0.0 if budget <= 0 else min(1.0, used / budget)

    return _build_result(
        policy=policy,
        opportunities=opportunities,
        approved=approved,
        blocked=blocked,
        starting_equity=starting_equity,
        equity_curve=equity_curve,
        day_open_equity=day_open_equity,
        daily_pnl=dict(daily_pnl),
        daily_budget_used_pct=daily_budget_used_pct,
        daily_trade_counts=daily_trade_counts,
        hard_stop_days=daily_hard_stopped,
        caution_events=caution_events,
        defensive_events=defensive_events,
        hard_stop_events=hard_stop_events,
        state_trace=state_trace,
    )


def _build_result(
    *,
    policy: ReplayPolicy,
    opportunities: Sequence[Opportunity],
    approved: Sequence[Opportunity],
    blocked: Sequence[Opportunity],
    starting_equity: float,
    equity_curve: Sequence[tuple[datetime, float]],
    day_open_equity: Mapping[str, float],
    daily_pnl: Mapping[str, float],
    daily_budget_used_pct: Mapping[str, float],
    daily_trade_counts: Mapping[str, int],
    hard_stop_days: set[str],
    caution_events: int,
    defensive_events: int,
    hard_stop_events: int,
    state_trace: list[dict[str, Any]],
) -> PolicyReplayResult:
    net = sum(float(o.realized_pnl_usdt) for o in approved)
    wins = [float(o.realized_pnl_usdt) for o in approved if o.realized_pnl_usdt > 0]
    losses = [float(o.realized_pnl_usdt) for o in approved if o.realized_pnl_usdt < 0]
    blocked_winners = sum(1 for o in blocked if o.realized_pnl_usdt > 0)
    blocked_losers = sum(1 for o in blocked if o.realized_pnl_usdt < 0)
    trades_by_day = list(daily_trade_counts.values())
    used_pcts = list(daily_budget_used_pct.values())
    by_regime: dict[str, dict[str, Any]] = {}
    for regime, rows in _group_by_regime(approved).items():
        regime_pnl = [float(o.realized_pnl_usdt) for o in rows]
        regime_wins = sum(1 for p in regime_pnl if p > 0)
        by_regime[regime] = {
            "trades": len(rows),
            "net_pnl_usdt": round(sum(regime_pnl), 12),
            "win_rate": None if not rows else regime_wins / len(rows),
        }
    account_dd = round(_max_drawdown_pct([starting_equity, *[v for _, v in equity_curve]]), 12)
    return PolicyReplayResult(
        policy_name=policy.name,
        policy={**asdict(policy), "policy_hash": _policy_hash(policy)},
        opportunities_seen=len(opportunities),
        approved_trades=len(approved),
        blocked_trades=len(blocked),
        blocked_winners=blocked_winners,
        blocked_losers=blocked_losers,
        blocked_flat=sum(1 for o in blocked if o.realized_pnl_usdt == 0),
        blocked_net_pnl_usdt=round(sum(float(o.realized_pnl_usdt) for o in blocked), 12),
        net_pnl_usdt=round(net, 12),
        return_pct=round((net / starting_equity * 100.0) if starting_equity else 0.0, 12),
        fees_usdt=round(sum(float(o.fees_usdt) for o in approved), 12),
        funding_usdt=round(sum(float(o.funding_usdt) for o in approved), 12),
        slippage_cost_usdt=0.0,
        expectancy_usdt=round(net / len(approved), 12) if approved else 0.0,
        profit_factor=_profit_factor(wins, losses),
        win_rate=None if not approved else len(wins) / len(approved),
        average_winner_usdt=round(sum(wins) / len(wins), 12) if wins else None,
        average_loser_usdt=round(sum(losses) / len(losses), 12) if losses else None,
        median_winner_usdt=round(median(wins), 12) if wins else None,
        median_loser_usdt=round(median(losses), 12) if losses else None,
        max_daily_loss_usdt=round(min(daily_pnl.values()) if daily_pnl else 0.0, 12),
        max_daily_drawdown_pct=account_dd,
        max_weekly_drawdown_pct=account_dd,
        max_drawdown_pct=account_dd,
        profitable_days=sum(1 for pnl in daily_pnl.values() if pnl > 0),
        losing_days=sum(1 for pnl in daily_pnl.values() if pnl < 0),
        flat_days=sum(1 for pnl in daily_pnl.values() if pnl == 0),
        hard_stop_days=len(hard_stop_days),
        caution_events=caution_events,
        defensive_events=defensive_events,
        hard_stop_events=hard_stop_events,
        avg_trades_per_day=sum(trades_by_day) / len(day_open_equity) if day_open_equity else 0.0,
        median_trades_per_day=median(trades_by_day) if trades_by_day else 0.0,
        longest_losing_streak=_longest_losing_streak(approved),
        recovery_duration_days=_recovery_duration_days(equity_curve),
        avg_daily_budget_used_pct=sum(used_pcts) / len(used_pcts) if used_pcts else 0.0,
        median_daily_budget_used_pct=median(used_pcts) if used_pcts else 0.0,
        p95_daily_budget_used_pct=_percentile(sorted(used_pcts), 95.0) if used_pcts else 0.0,
        by_regime=by_regime,
        day_open_equity={k: round(v, 12) for k, v in sorted(day_open_equity.items())},
        daily_pnl={k: round(v, 12) for k, v in sorted(daily_pnl.items())},
        daily_budget_used_pct={k: round(v, 12) for k, v in sorted(daily_budget_used_pct.items())},
        approved_decision_ids=[o.decision_id for o in approved],
        blocked_decision_ids=[o.decision_id for o in blocked],
        blocked_positive_r=sum(1 for o in blocked if o.planned_risk_usdt > 0 and o.realized_pnl_usdt / o.planned_risk_usdt > 0),
        blocked_negative_r=sum(1 for o in blocked if o.planned_risk_usdt > 0 and o.realized_pnl_usdt / o.planned_risk_usdt < 0),
        state_trace=state_trace,
    )


def build_report_payload(
    *,
    dataset: ReplayDataset,
    results: Mapping[str, PolicyReplayResult],
    code_revision: str,
    canonical_db_path: str,
) -> dict[str, Any]:
    candidate_name = "R_1_5_CAP_2_5"
    ranked = sorted(results.values(), key=lambda r: _ranking_key(r), reverse=True)
    return {
        "replay_type": "adaptive_daily_risk_policy_calibration",
        "code_revision": code_revision,
        "risk_policy_version": POLICY_VERSION,
        "canonical_db_path": canonical_db_path,
        "canonical_db_access": "sqlite-uri-mode-ro",
        "dataset": {
            **dataset.source_summary,
            "dataset_hash": dataset.dataset_hash,
            "stream_contract": "positions joined to originating trading_decisions with closed broker-authoritative outcomes",
        },
        "policy_grid": [{**asdict(p), "policy_hash": _policy_hash(p)} for p in default_policy_grid()],
        "cost_model": {
            "fees": "canonical trade_fills fee sums when present",
            "funding": "canonical funding evidence when present; zero when absent",
            "slippage": "not separately reconstructable from current closed-position evidence",
        },
        "fill_model": {
            "source": "broker-authoritative closed positions and fills already recorded by runtime",
            "partial_final_lifecycle": "position close timestamps and realized PnL are replayed; missing intratrade management events are not fabricated",
        },
        "timezone": AdaptiveDailyRiskPolicy().timezone_name,
        "old_policy": results["OLD_6_USDT"].as_dict(),
        "candidate_policy": results[candidate_name].as_dict(),
        "results": {name: result.as_dict() for name, result in sorted(results.items())},
        "best_policy_by_replay_score": ranked[0].policy_name if ranked else None,
        "ranking": [{"policy": r.policy_name, "score": _ranking_key(r)} for r in ranked],
        "holdout": _holdout_summary(dataset.opportunities, results),
        "limitations": [
            "The canonical broker-outcome stream currently has only the closed positions linked to originating decisions; it is suitable for deterministic policy comparison but not statistically decisive.",
            "Costs are whatever the broker-fill evidence recorded on positions/trade_fills; missing funding remains zero rather than fabricated.",
            "This harness does not place orders, does not mutate canonical storage, and does not repair the older wall-clock production replay defect documented separately.",
        ],
    }


def render_markdown_report(payload: Mapping[str, Any]) -> str:
    old = payload["old_policy"]
    cand = payload["candidate_policy"]
    rows = []
    for name, result in payload["results"].items():
        rows.append(
            "| {name} | {approved} | {blocked} | {pnl:.4f} | {exp:.4f} | {pf} | {dd:.4f}% | {hard} |".format(
                name=name,
                approved=result["approved_trades"],
                blocked=result["blocked_trades"],
                pnl=result["net_pnl_usdt"],
                exp=result["expectancy_usdt"],
                pf="inf" if result["profit_factor"] is None else f"{result['profit_factor']:.4f}",
                dd=result["max_drawdown_pct"],
                hard=result["hard_stop_days"],
            )
        )
    return "\n".join(
        [
            "# Adaptive Daily Risk Policy Replay Calibration",
            "",
            f"- Code revision: `{payload['code_revision']}`",
            f"- Risk policy version: `{payload['risk_policy_version']}`",
            f"- Dataset hash: `{payload['dataset']['dataset_hash']}`",
            f"- Opportunity stream: {payload['dataset']['opportunity_count']} closed broker-outcome trades",
            f"- Risk observations: {payload['dataset']['risk_observation_count']}",
            f"- Canonical DB access: `{payload['canonical_db_access']}`",
            "",
            "## Verdict",
            "",
            "The replay harness is deterministic and policy-isolated, but the current broker-linked outcome sample is small. "
            "Use this as calibration evidence and guardrail validation, not as a statistically complete production-readiness claim.",
            "",
            "## Old vs Candidate",
            "",
            "| Policy | Approved | Blocked | Net PnL | Expectancy | PF | Max DD | Hard-stop days |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            "| OLD_6_USDT | {approved} | {blocked} | {pnl:.4f} | {exp:.4f} | {pf} | {dd:.4f}% | {hard} |".format(
                approved=old["approved_trades"],
                blocked=old["blocked_trades"],
                pnl=old["net_pnl_usdt"],
                exp=old["expectancy_usdt"],
                pf="inf" if old["profit_factor"] is None else f"{old['profit_factor']:.4f}",
                dd=old["max_drawdown_pct"],
                hard=old["hard_stop_days"],
            ),
            "| R_1_5_CAP_2_5 | {approved} | {blocked} | {pnl:.4f} | {exp:.4f} | {pf} | {dd:.4f}% | {hard} |".format(
                approved=cand["approved_trades"],
                blocked=cand["blocked_trades"],
                pnl=cand["net_pnl_usdt"],
                exp=cand["expectancy_usdt"],
                pf="inf" if cand["profit_factor"] is None else f"{cand['profit_factor']:.4f}",
                dd=cand["max_drawdown_pct"],
                hard=cand["hard_stop_days"],
            ),
            "",
            "## Full Grid",
            "",
            "| Policy | Approved | Blocked | Net PnL | Expectancy | PF | Max DD | Hard-stop days |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            *rows,
            "",
            "## Holdout",
            "",
            json.dumps(payload["holdout"], indent=2, sort_keys=True),
            "",
            "## Limitations",
            "",
            *[f"- {item}" for item in payload["limitations"]],
            "",
        ]
    )


def write_replay_artifacts(
    payload: Mapping[str, Any],
    *,
    report_path: Path,
    json_path: Path,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_markdown_report(payload), encoding="utf-8")
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _ranking_key(result: PolicyReplayResult) -> tuple[float, float, float, float]:
    pf = result.profit_factor if result.profit_factor is not None else 999.0
    participation = result.approved_trades / result.opportunities_seen if result.opportunities_seen else 0.0
    return (
        result.net_pnl_usdt,
        result.expectancy_usdt,
        min(pf, 10.0),
        participation - result.max_drawdown_pct / 100.0,
    )


def _holdout_summary(opportunities: Sequence[Opportunity], results: Mapping[str, PolicyReplayResult]) -> dict[str, Any]:
    n = len(opportunities)
    start = int(n * 0.8)
    holdout_ids = {o.decision_id for o in opportunities[start:]}
    return {
        "split": "chronological final 20%",
        "holdout_opportunities": len(holdout_ids),
        "policy_results": {
            name: {
                "approved_holdout_trades": len([i for i in r.approved_decision_ids if i in holdout_ids]),
                "blocked_holdout_trades": len([i for i in r.blocked_decision_ids if i in holdout_ids]),
            }
            for name, r in results.items()
        },
        "tuning_note": "Ranking is reported after the fixed grid replay; no threshold, strategy, regime, slot, leverage, or affordability parameter is optimized on the holdout.",
    }


def _profit_factor(wins: Sequence[float], losses: Sequence[float]) -> float | None:
    total_wins = sum(wins)
    total_losses = abs(sum(losses))
    if total_losses <= 0:
        return None if total_wins > 0 else 0.0
    return total_wins / total_losses


def _policy_hash(policy: ReplayPolicy) -> str:
    payload = json.dumps(asdict(policy), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _scalar_int(conn: sqlite3.Connection, sql: str) -> int:
    try:
        return int(conn.execute(sql).fetchone()[0] or 0)
    except Exception:
        return 0


def _max_drawdown_pct(equity_values: Sequence[float]) -> float:
    if not equity_values:
        return 0.0
    peak = float(equity_values[0])
    max_dd = 0.0
    for value in equity_values:
        value = float(value)
        peak = max(peak, value)
        if peak > 0:
            max_dd = max(max_dd, (peak - value) / peak * 100.0)
    return max_dd


def _longest_losing_streak(opportunities: Sequence[Opportunity]) -> int:
    longest = current = 0
    for opportunity in sorted(opportunities, key=lambda o: o.closed_at):
        if opportunity.realized_pnl_usdt < 0:
            current += 1
            longest = max(longest, current)
        elif opportunity.realized_pnl_usdt > 0:
            current = 0
    return longest


def _recovery_duration_days(equity_curve: Sequence[tuple[datetime, float]]) -> int | None:
    peak_value: float | None = None
    peak_time: datetime | None = None
    worst_start: datetime | None = None
    worst_depth = 0.0
    for ts, equity in equity_curve:
        value = float(equity)
        if peak_value is None or value >= peak_value:
            if worst_start is not None and value >= peak_value:
                return max(0, (ts.date() - worst_start.date()).days)
            peak_value = value
            peak_time = ts
            continue
        if peak_value and (peak_value - value) > worst_depth:
            worst_depth = peak_value - value
            worst_start = peak_time
    return None


def _group_by_regime(rows: Iterable[Opportunity]) -> dict[str, list[Opportunity]]:
    grouped: dict[str, list[Opportunity]] = defaultdict(list)
    for row in rows:
        grouped[(row.regime or "UNKNOWN").upper()].append(row)
    return grouped


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    pos = (len(values) - 1) * float(percentile) / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(values) - 1)
    frac = pos - lo
    return float(values[lo]) * (1.0 - frac) + float(values[hi]) * frac
