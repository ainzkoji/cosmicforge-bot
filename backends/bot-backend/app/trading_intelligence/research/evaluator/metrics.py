"""Performance figures, benchmarks and the frozen pass rule for the daily trend evaluator (Step 2.4 / 2.6).

Everything is computed from a ``SimResult``'s own ledgers, so each reported number can be traced to the equity
series and the trade list of one run. A figure that is not defined (a Sharpe ratio of a flat series, a profit
factor without losses) is ``None`` -- never zero, never infinity.
"""
from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any, Dict, Mapping, Optional, Sequence

import numpy as np

from app.trading_intelligence.families.daily_trend.spec import SPECIFICATION

from .simulator import EXIT_END, Features, SimResult

PERIODS_PER_YEAR = 365
EPOCH = date(1970, 1, 1)
PASS, FAIL, PENDING = "PASS", "FAIL", "PENDING_HOLDOUT"


def _date(day: int) -> date:
    return EPOCH + timedelta(days=int(day))


def _ratio(a: float, b: float) -> Optional[float]:
    return a / b if b not in (0, 0.0) and math.isfinite(a) and math.isfinite(b) else None


def return_statistics(returns: np.ndarray) -> Dict[str, Any]:
    """Statistics of a daily return series (compounded)."""
    r = np.asarray(returns, dtype=np.float64)
    n = len(r)
    if n == 0:
        return {"observations": 0}
    growth = np.cumprod(1.0 + r)
    total = float(growth[-1] - 1.0)
    years = n / PERIODS_PER_YEAR
    sd = float(r.std(ddof=1)) if n > 1 else 0.0
    downside = math.sqrt(float(np.mean(np.minimum(r, 0.0) ** 2)))
    peak = np.maximum.accumulate(np.concatenate([[1.0], growth]))[1:]
    dd = growth / peak - 1.0
    cagr = float(growth[-1] ** (1.0 / years) - 1.0) if growth[-1] > 0 and years > 0 else None
    tail = np.sort(r)[:max(1, int(math.floor(n * 0.05)))]
    return {"observations": n, "years": years, "net_return": total, "annual_return": cagr,
            "annual_volatility": sd * math.sqrt(PERIODS_PER_YEAR),
            "sharpe": _ratio(float(r.mean()) * math.sqrt(PERIODS_PER_YEAR), sd),
            "sortino": _ratio(float(r.mean()) * math.sqrt(PERIODS_PER_YEAR), downside),
            "max_drawdown": float(-dd.min()), "calmar": _ratio(cagr, float(-dd.min())) if cagr is not None else None,
            "worst_day": float(r.min()), "best_day": float(r.max()), "expected_shortfall_5pct": float(tail.mean()),
            "positive_days_share": float(np.mean(r > 0))}


def calendar_years(days: np.ndarray, equity: np.ndarray, initial: float) -> Dict[str, Dict[str, Any]]:
    """Year-end equity over the previous year-end equity. A year the run does not cover to its last day is
    marked partial and is not a calendar-year result."""
    out: Dict[str, Dict[str, Any]] = {}
    years = np.array([_date(d).year for d in days])
    prev = float(initial)
    for y in sorted(set(years.tolist())):
        idx = np.flatnonzero(years == y)
        first, last = _date(days[idx[0]]), _date(days[idx[-1]])
        complete = last == date(y, 12, 31) and (first == date(y, 1, 1) or idx[0] > 0)
        end = float(equity[idx[-1]])
        out[str(y)] = {"return": end / prev - 1.0 if prev > 0 else None, "complete": bool(complete),
                       "first_day": first.isoformat(), "last_day": last.isoformat()}
        prev = end
    return out


def trade_statistics(trades: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    closed = [t for t in trades if t["exit_reason"] != EXIT_END]
    net = [t["net_pnl"] for t in closed]
    wins, losses = [x for x in net if x > 0], [x for x in net if x < 0]
    reasons: Dict[str, int] = {}
    for t in closed:
        reasons[t["exit_reason"]] = reasons.get(t["exit_reason"], 0) + 1
    return {"trades_closed": len(closed), "trades_open_at_end": len(trades) - len(closed),
            "hit_rate": _ratio(len(wins), len(closed)), "profit_factor": _ratio(sum(wins), -sum(losses)),
            "average_win": _ratio(sum(wins), len(wins)), "average_loss": _ratio(sum(losses), len(losses)),
            "average_days_held": _ratio(sum(t["days_held"] for t in closed), len(closed)),
            "exit_reasons": dict(sorted(reasons.items())),
            "largest_win": max(wins) if wins else None, "largest_loss": min(losses) if losses else None,
            "symbols_traded": len({t["symbol"] for t in trades})}


def summarize(res: SimResult) -> Dict[str, Any]:
    d, cfg = res.daily, res.config
    initial = float(cfg.initial_equity)
    equity = d["equity"]
    gross_equity = initial + np.cumsum(d["pnl_price"])
    gross_returns = np.diff(np.concatenate([[initial], gross_equity])) / np.concatenate([[initial], equity[:-1]])
    years = len(equity) / PERIODS_PER_YEAR
    mean_equity = float(equity.mean())
    stats = return_statistics(d["return"])
    events, imputed = float(d["funding_events"].sum()), float(d["funding_events_imputed"].sum())
    exposure = d["gross_notional"] / np.maximum(equity, 1e-12)
    out = {
        "label": cfg.label, "policy": cfg.level.policy, "risk_level": cfg.level.name,
        "risk_per_trade_effective": cfg.level.risk_per_trade, "risk_per_trade_approved": cfg.level.approved_risk_per_trade,
        "cost_multiple": cfg.cost_multiple, "drawdown_brakes": cfg.drawdown_brakes, "funding_overlay": cfg.funding_overlay,
        "first_day": _date(res.days[0]).isoformat(), "last_day": _date(res.days[-1]).isoformat(),
        "initial_equity": initial, "final_equity": float(equity[-1]), **stats,
        "gross_return": float(gross_equity[-1] / initial - 1.0),
        "gross_annual_return_arithmetic": float(gross_returns.mean() * PERIODS_PER_YEAR),
        "net_annual_return_arithmetic": float(d["return"].mean() * PERIODS_PER_YEAR),
        "calendar_years": calendar_years(res.days, equity, initial),
        "costs": {"fees": res.totals["fees"], "slippage": res.totals["slippage"], "funding": res.totals["funding"],
                  "fee_drag_per_year": _ratio(res.totals["fees"] / years, mean_equity),
                  "slippage_drag_per_year": _ratio(res.totals["slippage"] / years, mean_equity),
                  "funding_drag_per_year": _ratio(res.totals["funding"] / years, mean_equity),
                  "price_pnl": res.totals["pnl_price"]},
        "turnover_per_year": _ratio(res.totals["turnover"] / years, mean_equity),
        "share_of_days_with_a_position": float(np.mean(d["positions"] > 0)),
        "average_positions": float(d["positions"].mean()), "average_exposure": float(exposure.mean()),
        "max_exposure": float(exposure.max()), "max_order_share_of_daily_volume": res.totals["max_participation"],
        "brake_days": {"daily_pause": int(d["paused"].sum()), "halved": int(d["halved"].sum()),
                       "halted": int(d["halted"].sum())},
        "drawdown_stop_fired_on": next((_date(e["day"]).isoformat() for e in res.events
                                        if e["event"] == "DRAWDOWN_STOP_FIRED"), None),
        "funding_events": events, "funding_events_imputed": imputed,
        "funding_imputed_share": _ratio(imputed, events) or 0.0,
        "trades": trade_statistics(res.trades), "fills": len(res.fills),
        "rejections": _count(res.rejections, "reason"), "stop_events": _count(res.events, "event"),
    }
    # the books must close: equity = initial + price P&L - fees - slippage - funding
    out["ledger_residual"] = float(equity[-1] - (initial + res.totals["pnl_price"] - res.totals["fees"]
                                                 - res.totals["slippage"] - res.totals["funding"]))
    return out


def _count(rows: Sequence[Mapping[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        out[r[key]] = out.get(r[key], 0) + 1
    return dict(sorted(out.items()))


def entry_stop_distances(res: SimResult, limit: float = 0.15) -> Dict[str, Any]:
    """Raw strategy decision statistics: the stop distance of every new position the rule selected."""
    d = np.array([e["stop_distance"] for e in res.entry_decisions], dtype=np.float64)
    if not len(d):
        return {"entry_decisions": 0}
    q = np.quantile(d, [0.05, 0.25, 0.5, 0.75, 0.95])
    return {"entry_decisions": int(len(d)), "accepted": int(sum(1 for e in res.entry_decisions if e["accepted"])),
            "stop_distance_quantiles": {"p05": float(q[0]), "p25": float(q[1]), "p50": float(q[2]), "p75": float(q[3]),
                                        "p95": float(q[4]), "min": float(d.min()), "max": float(d.max())},
            "engine_limit": limit, "beyond_engine_limit": int((d > limit).sum()),
            "share_beyond_engine_limit": float((d > limit).mean()),
            "at_floor_5pct": int((d <= SPECIFICATION["stop_floor"] + 1e-12).sum()),
            "at_cap_40pct": int((d >= SPECIFICATION["stop_cap"] - 1e-12).sum())}


# ---------------------------------------------------------------------- benchmarks
def benchmark_returns(feat: Features, first_day: int, last_day: int, cost: float) -> Dict[str, np.ndarray]:
    """Daily returns of the two registered benchmarks over ``[first_day, last_day]`` (interpretation I17):

    * ``btc``          -- BTCUSDT perpetual held long at 1x: close-to-close, minus funding, one entry cost;
    * ``equal_weight`` -- each day, equal weights in the PREVIOUS day's universe: close-to-close, minus funding,
                          minus ``cost`` on the weight traded."""
    i0 = int(np.searchsorted(feat.days, first_day))
    i1 = int(np.searchsorted(feat.days, last_day, side="right")) - 1
    funding = feat.funding_midnight + feat.funding_later_positive + feat.funding_later_negative
    with np.errstate(invalid="ignore", divide="ignore"):
        ret = feat.close[1:] / feat.close[:-1] - 1.0
    ret = np.vstack([np.full((1, ret.shape[1]), np.nan), ret])
    out = {}
    if "BTCUSDT" in feat.symbols:
        j = feat.symbols.index("BTCUSDT")
        btc = np.nan_to_num(ret[i0:i1 + 1, j]) - np.nan_to_num(funding[i0:i1 + 1, j])
        btc[0] -= cost
        out["btc"] = btc
    ew = np.zeros(i1 - i0 + 1)
    prev_w = np.zeros(len(feat.symbols))
    for i in range(i0, i1 + 1):
        members = feat.member[i - 1] & ~np.isnan(ret[i]) if i > 0 else np.zeros(len(feat.symbols), dtype=bool)
        w = members / members.sum() if members.any() else np.zeros(len(feat.symbols))
        day_ret = float(np.sum(w * np.nan_to_num(ret[i]))) - float(np.sum(w * funding[i]))
        ew[i - i0] = day_ret - cost * float(np.abs(w - prev_w).sum())
        prev_w = w
    out["equal_weight"] = ew
    return out


def scaled_to(returns: np.ndarray, target_volatility: Optional[float]) -> Dict[str, Any]:
    """A benchmark scaled after the fact to the strategy's realised volatility: a comparison device."""
    stats = return_statistics(returns)
    vol = stats.get("annual_volatility") or 0.0
    if not target_volatility or vol <= 0:
        return {"unscaled": stats, "scaled": None, "scale": None}
    k = target_volatility / vol
    return {"unscaled": stats, "scaled": return_statistics(np.asarray(returns) * k), "scale": k}


# ---------------------------------------------------------------------- the frozen pass rule
def evaluate_pass_rule(*, full_as_specified: Optional[Mapping[str, Any]], full_without_brake: Optional[Mapping[str, Any]],
                       holdout_base: Optional[Mapping[str, Any]], holdout_stress: Optional[Mapping[str, Any]],
                       development_as_specified: Optional[Mapping[str, Any]] = None,
                       development_without_brake: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """SPEC "Pass rule": all four at Balanced, anything else is a fail.

    Before the holdout is opened only the development summaries exist. A criterion is then FAIL when no
    held-back outcome could still satisfy it (drawdown already beyond the limit; too few positive years even
    if 2025 were positive), and PENDING_HOLDOUT otherwise. It is never PASS on development data alone unless
    the held-back data cannot change it."""
    rule = SPECIFICATION["pass_rule"]
    years = [str(y) for y in rule["calendar_years"]]
    crit: Dict[str, Dict[str, Any]] = {}

    # 1. held-back net return positive at base and at stress cost
    if holdout_base is None or holdout_stress is None:
        crit["1_holdout_net_return_positive"] = {"status": PENDING, "observed": None}
    else:
        obs = {"base": holdout_base["net_return"], "stress": holdout_stress["net_return"]}
        crit["1_holdout_net_return_positive"] = {"status": PASS if min(obs.values()) > 0 else FAIL, "observed": obs}

    # 2. net return positive in at least 4 of the 6 calendar years
    src = full_as_specified or development_as_specified
    known = {y: src["calendar_years"][y]["return"] for y in years
             if src and y in src["calendar_years"] and src["calendar_years"][y]["complete"]}
    positive = sum(1 for v in known.values() if v is not None and v > 0)
    unknown = len(years) - len(known)
    need = rule["positive_calendar_years_min"]
    if full_as_specified is not None:       # the holdout was evaluated: a year the run does not cover cannot count
        status2 = PASS if positive >= need else FAIL
    else:
        status2 = PASS if positive >= need else (FAIL if positive + unknown < need else PENDING)
    crit["2_positive_calendar_years"] = {"status": status2, "observed": {"positive": positive, "of_known": len(known),
                                                                         "years": known, "required": need,
                                                                         "years_not_covered": unknown}}

    # 3. without the brake, maximum drawdown over the full period inside 15%
    limit = rule["max_drawdown_without_brake"]
    if full_without_brake is not None:
        dd = full_without_brake["max_drawdown"]
        crit["3_max_drawdown_without_brake"] = {"status": PASS if dd <= limit else FAIL,
                                                "observed": {"max_drawdown": dd, "period": "FULL", "limit": limit}}
    elif development_without_brake is not None:
        dd = development_without_brake["max_drawdown"]      # the full-period drawdown cannot be smaller than this
        crit["3_max_drawdown_without_brake"] = {"status": FAIL if dd > limit else PENDING,
                                                "observed": {"max_drawdown": dd, "period": "DEVELOPMENT_ONLY", "limit": limit}}
    else:
        crit["3_max_drawdown_without_brake"] = {"status": PENDING, "observed": None}

    # 4. held-back net Sharpe ratio at least 0.3
    if holdout_base is None:
        crit["4_holdout_net_sharpe"] = {"status": PENDING, "observed": None}
    else:
        sharpe = holdout_base["sharpe"]
        ok = sharpe is not None and sharpe >= rule["holdout_net_sharpe_min"]
        crit["4_holdout_net_sharpe"] = {"status": PASS if ok else FAIL,
                                        "observed": {"sharpe": sharpe, "required": rule["holdout_net_sharpe_min"]}}

    statuses = [c["status"] for c in crit.values()]
    overall = FAIL if FAIL in statuses else (PENDING if PENDING in statuses else PASS)
    return {"level": rule["level"], "criteria": crit, "status": overall,
            "decided_without_holdout": overall == FAIL and (holdout_base is None)}


__all__ = ["PERIODS_PER_YEAR", "PASS", "FAIL", "PENDING", "return_statistics", "calendar_years", "trade_statistics",
           "summarize", "entry_stop_distances", "benchmark_returns", "scaled_to", "evaluate_pass_rule"]
