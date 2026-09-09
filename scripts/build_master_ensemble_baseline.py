"""Master Ensemble baseline measurement (§15.1–§15.5, §15.8, §15.9). Read-only.

The instruction this serves is "measure first, do not tune". So this script
computes the decision funnel, the funnel ratios, the confidence and consensus
distributions, the regime distribution and the component contributions from
canonical ``trading_decisions`` — and it names the dominant bottleneck from the
numbers rather than from intuition.

It changes nothing, and it deliberately reports the sample size next to every
conclusion, because §15.8 forbids removing a component on the strength of a
tiny sample and the same caution applies to every other verdict here.

    python scripts/build_master_ensemble_baseline.py
    python scripts/build_master_ensemble_baseline.py --bot bot_a8117dc719fc --json
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from collections import Counter

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = os.path.join(REPO_ROOT, "backends", "bot-backend")
SHARED = os.path.join(REPO_ROOT, "backends", "shared")

#: A heartbeat is cycle evidence, not an evaluation. Counting it in a funnel
#: makes every ratio meaningless -- it is 98% of the rows.
HEARTBEAT = "NO_NEW_CANDLE"

#: §15.1 canonical funnel order, most upstream first.
FUNNEL_ORDER = (
    "NO_NEW_CANDLE",
    "NO_OPPORTUNITY",
    "REGIME_LOW_VOL_CHOP",
    "REGIME_BLOCKED",
    "SESSION_BLOCKED",
    "EVENT_BLACKOUT",
    "VOLATILITY_SPIKE",
    "INSUFFICIENT_CONSENSUS",
    "CONSENSUS_INSUFFICIENT",
    "ENTRY_CONFIDENCE_BELOW_THRESHOLD",
    "HTF_OPPOSED",
    "HTF_NOT_ALIGNED",
    "RISK_BLOCKED",
    "CORRELATION_BLOCKED",
    "EXECUTION_BLOCKED",
    "DUPLICATE_ENTRY",
    "APPROVED_FOR_EXECUTION",
)


def percentiles(values: list[float]) -> dict:
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    def at(q):
        return ordered[min(len(ordered) - 1, int(len(ordered) * q))]
    return {
        "n": len(ordered),
        "min": ordered[0],
        "p10": at(0.10),
        "p50": at(0.50),
        "p90": at(0.90),
        "max": ordered[-1],
        "mean": statistics.fmean(ordered),
    }


def build(db, *, bot_id: str | None, provenance: str | None) -> dict:
    where, args = ["1=1"], []
    if bot_id:
        where.append("bot_instance_id=?")
        args.append(bot_id)
    if provenance:
        where.append("provenance=?")
        args.append(provenance)

    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            f"SELECT * FROM trading_decisions WHERE {' AND '.join(where)} "
            f"ORDER BY evaluated_at", tuple(args),
        )]

    if not rows:
        return {"error": "no decisions matched"}

    heartbeats = [r for r in rows if r["primary_reason"] == HEARTBEAT]
    evaluations = [r for r in rows if r["primary_reason"] != HEARTBEAT]

    report: dict = {
        "window": {"from": rows[0]["evaluated_at"], "to": rows[-1]["evaluated_at"]},
        "bots": sorted({r["bot_instance_id"] for r in rows}),
        "provenance": sorted({str(r["provenance"]) for r in rows}),
        "symbols": dict(Counter(r["symbol"] for r in evaluations)),
        "totals": {
            "decision_rows": len(rows),
            "heartbeats": len(heartbeats),
            "real_evaluations": len(evaluations),
        },
    }

    # §15.1 funnel
    counts = Counter(str(r["primary_reason"]) for r in rows)
    ordered = {k: counts[k] for k in FUNNEL_ORDER if counts.get(k)}
    ordered.update({k: v for k, v in counts.items() if k not in FUNNEL_ORDER})
    report["funnel"] = ordered

    # §15.2 ratios, over real evaluations only
    n = len(evaluations) or 1
    opportunities = [r for r in evaluations if (r["raw_confidence"] or 0) > 0]
    approved = [r for r in evaluations if r["final_action"] == "APPROVED"]
    risk_seen = [r for r in evaluations if r["risk_result"] is not None]
    feasible = [r for r in evaluations
                if r["execution_feasibility_result"] not in (None, "FAIL")]
    attempts = [r for r in evaluations if r["execution_attempt_id"]]
    fills = [r for r in evaluations if r["position_id"]]
    report["ratios"] = {
        "closed_candles_evaluated": len(evaluations),
        "produced_an_opportunity": len(opportunities),
        "quality_approved": len(approved),
        "reached_risk": len(risk_seen),
        "execution_feasible": len(feasible),
        "execution_attempts": len(attempts),
        "fills": len(fills),
        "opportunity_rate": len(opportunities) / n,
        "approval_rate": len(approved) / n,
    }

    # §15.3 / §15.4 distributions
    def col(key):
        return [float(r[key]) for r in evaluations if r[key] is not None]

    report["confidence"] = {
        "raw_confidence": percentiles(col("raw_confidence")),
        "effective_entry_threshold": percentiles(col("effective_entry_threshold")),
        "confidence_minus_threshold": percentiles([
            float(r["raw_confidence"]) - float(r["effective_entry_threshold"])
            for r in evaluations
            if r["raw_confidence"] is not None
            and r["effective_entry_threshold"] is not None
        ]),
    }
    report["consensus"] = {
        "buy_score": percentiles(col("buy_score")),
        "sell_score": percentiles(col("sell_score")),
        "consensus_observed": percentiles(col("consensus_observed")),
    }

    # §15.5 regimes
    report["regimes"] = dict(Counter(
        str(r["regime"]) for r in evaluations if r["regime"] is not None
    ))

    # §15.8 component contribution
    active, supporting, opposing = Counter(), Counter(), Counter()
    for r in evaluations:
        for name in json.loads(r["active_strategies_json"] or "[]"):
            active[name] += 1
        for name in json.loads(r["supporting_strategies_json"] or "[]"):
            supporting[name] += 1
        for name in json.loads(r["opposing_strategies_json"] or "[]"):
            opposing[name] += 1
    report["components"] = {
        "active": dict(active),
        "supporting": dict(supporting),
        "opposing": dict(opposing),
        "never_active": sorted(set(_ALL_COMPONENTS) - set(active)),
        "never_supported": sorted(set(active) - set(supporting)),
    }

    report["profitability"] = profitability(db, bot_id=bot_id, provenance=provenance)
    report["diagnosis"] = diagnose(report, evaluations)
    return report


_ALL_COMPONENTS = (
    "supertrend", "vwap_reversion", "trend_pullback", "squeeze_breakout",
    "sma_cross", "donchian_breakout", "bollinger_reversion",
)


def diagnose(report: dict, evaluations: list[dict]) -> dict:
    """§15.9 — name the dominant bottleneck from the numbers, not from a hunch."""
    n = len(evaluations)
    findings: list[str] = []

    confidence = report["confidence"]["raw_confidence"]
    threshold = report["confidence"]["effective_entry_threshold"]
    no_direction = sum(1 for r in evaluations if not (r["raw_confidence"] or 0))

    if n and no_direction / n >= 0.5:
        findings.append(
            f"NO_DIRECTIONAL_CANDIDATE is dominant: {no_direction}/{n} "
            f"({no_direction / n:.0%}) of evaluations produced no opportunity at "
            f"all, so the entry threshold was never consulted on them."
        )
    if confidence.get("n") and threshold.get("n"):
        if confidence["p50"] == 0:
            findings.append(
                "Median raw confidence is exactly 0.0 — on a typical candle the "
                "ensemble has no directional conviction whatsoever."
            )
        if confidence["max"] < threshold.get("p50", 0):
            findings.append(
                f"Even the best observed confidence ({confidence['max']:.4f}) is "
                f"below the median threshold ({threshold['p50']:.4f})."
            )

    never_supported = report["components"]["never_supported"]
    if never_supported:
        findings.append(
            f"Components active but never supporting a direction: "
            f"{', '.join(never_supported)}."
        )
    never_active = report["components"]["never_active"]
    if never_active:
        findings.append(
            f"Components never active in this window (regime-disabled): "
            f"{', '.join(never_active)}."
        )

    approvals = report["ratios"]["quality_approved"]
    if approvals == 0:
        findings.append(
            f"Zero approvals in {n} evaluations: nothing reached the risk layer, "
            f"so risk, sizing, feasibility and execution are entirely unmeasured."
        )

    return {
        "sample_size": n,
        "sample_adequacy": (
            "ADEQUATE" if n >= 500 else
            "THIN" if n >= 100 else
            "TOO_SMALL_FOR_COMPONENT_DECISIONS"
        ),
        "findings": findings or ["No dominant bottleneck identified."],
    }


# ── §15.6 / §15.7 Profitability, after costs ────────────────────────────────


def _session_bucket(iso: str | None) -> str:
    """Crude UTC session label. Named as crude so nobody over-reads it."""
    if not iso:
        return "unknown"
    try:
        hour = int(str(iso)[11:13])
    except Exception:
        return "unknown"
    if 0 <= hour < 8:
        return "asia"
    if 8 <= hour < 16:
        return "europe"
    return "americas"


def _drawdown(pnls: list[float]) -> dict:
    """Peak-to-trough on the realized equity curve, in currency and percent."""
    equity = 0.0
    peak = 0.0
    worst = 0.0
    worst_pct = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drop = peak - equity
        if drop > worst:
            worst = drop
            worst_pct = drop / peak if peak > 0 else 0.0
    return {"max_drawdown": worst, "max_drawdown_pct": worst_pct}


def profitability(db, *, bot_id: str | None, provenance: str | None) -> dict:
    """Realized results for closed positions, net of the fees actually charged.

    Gross numbers are not reported. A strategy that is profitable before costs
    and unprofitable after them is unprofitable, and showing the gross figure
    invites the wrong conclusion.
    """
    where, args = ["status='CLOSED'"], []
    if bot_id:
        where.append("bot_instance_id=?")
        args.append(bot_id)
    if provenance:
        where.append("provenance=?")
        args.append(provenance)

    with db.connect() as conn:
        positions = [dict(r) for r in conn.execute(
            f"SELECT * FROM positions WHERE {' AND '.join(where)} ORDER BY opened_at",
            tuple(args),
        )]
        decisions = {
            r["decision_id"]: dict(r) for r in conn.execute(
                "SELECT decision_id, regime, symbol, raw_confidence, "
                "consensus_observed, stop_price FROM trading_decisions"
            )
        }

    if not positions:
        return {"closed_trades": 0,
                "note": "no closed positions; profitability is not measurable"}

    trades = []
    for row in positions:
        gross = float(row.get("realized_pnl") or 0.0)
        fees = float(row.get("fees") or 0.0)
        net = gross - fees
        decision = decisions.get(row.get("decision_id")) or {}
        entry = float(row.get("entry_price") or 0.0)
        qty = float(row.get("original_qty") or 0.0)
        stop = float(decision.get("stop_price") or 0.0)
        risk = abs(entry - stop) * qty if stop > 0 and entry > 0 else None
        trades.append({
            "symbol": row.get("symbol"),
            "side": str(row.get("side") or "").upper(),
            "gross": gross, "fees": fees, "net": net,
            "regime": decision.get("regime") or "unknown",
            "month": str(row.get("opened_at") or "")[:7],
            "session": _session_bucket(row.get("opened_at")),
            "r_multiple": (net / risk) if risk else None,
            "notional": entry * qty,
        })

    nets = [t["net"] for t in trades]
    wins = [n for n in nets if n > 0]
    losses = [n for n in nets if n < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    r_values = [t["r_multiple"] for t in trades if t["r_multiple"] is not None]

    summary = {
        "closed_trades": len(trades),
        "gross_pnl": sum(t["gross"] for t in trades),
        "total_fees": sum(t["fees"] for t in trades),
        "net_pnl": sum(nets),
        "expectancy_per_trade": sum(nets) / len(nets),
        "win_rate": len(wins) / len(nets),
        "wins": len(wins), "losses": len(losses),
        "average_win": (gross_profit / len(wins)) if wins else 0.0,
        "average_loss": (-gross_loss / len(losses)) if losses else 0.0,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else None,
        "average_r_multiple": (sum(r_values) / len(r_values)) if r_values else None,
        **_drawdown(nets),
    }

    def segment(key: str) -> dict:
        out: dict[str, dict] = {}
        for trade in trades:
            bucket = out.setdefault(str(trade[key]), {"trades": 0, "net_pnl": 0.0, "wins": 0})
            bucket["trades"] += 1
            bucket["net_pnl"] += trade["net"]
            bucket["wins"] += 1 if trade["net"] > 0 else 0
        for bucket in out.values():
            bucket["win_rate"] = bucket["wins"] / bucket["trades"]
            bucket["expectancy"] = bucket["net_pnl"] / bucket["trades"]
        return out

    return {
        **summary,
        "by_regime": segment("regime"),
        "by_symbol": segment("symbol"),
        "by_side": segment("side"),
        "by_month": segment("month"),
        "by_session": segment("session"),
    }


def render(report: dict) -> str:
    if "error" in report:
        return report["error"]
    out: list[str] = []
    t = report["totals"]
    out.append(f"window            {report['window']['from']} -> {report['window']['to']}")
    out.append(f"bots              {', '.join(report['bots'])}")
    out.append(f"provenance        {', '.join(report['provenance'])}")
    out.append(f"symbols           {report['symbols']}")
    out.append("")
    out.append(f"decision rows     {t['decision_rows']:,}")
    out.append(f"  heartbeats      {t['heartbeats']:,}")
    out.append(f"  real evals      {t['real_evaluations']:,}")
    out.append("")
    out.append("DECISION FUNNEL (§15.1)")
    for reason, count in report["funnel"].items():
        out.append(f"  {reason:<36} {count:>7,}")
    out.append("")
    out.append("FUNNEL RATIOS (§15.2)")
    for key, value in report["ratios"].items():
        out.append(f"  {key:<30} {value:.4f}" if isinstance(value, float)
                   else f"  {key:<30} {value:,}")
    out.append("")
    for section, title in (("confidence", "CONFIDENCE (§15.3)"),
                           ("consensus", "CONSENSUS (§15.4)")):
        out.append(title)
        for key, stats in report[section].items():
            if not stats.get("n"):
                out.append(f"  {key:<30} (no data)")
                continue
            out.append(
                f"  {key:<30} n={stats['n']:<5} min={stats['min']:.4f} "
                f"p50={stats['p50']:.4f} p90={stats['p90']:.4f} "
                f"max={stats['max']:.4f} mean={stats['mean']:.4f}"
            )
        out.append("")
    out.append(f"REGIMES (§15.5)     {report['regimes']}")
    out.append("")
    out.append("COMPONENTS (§15.8)")
    for key, value in report["components"].items():
        out.append(f"  {key:<18} {value}")
    out.append("")
    profit = report.get("profitability") or {}
    out.append("PROFITABILITY AFTER COSTS (\u00a715.6)")
    if not profit.get("closed_trades"):
        out.append(f"  {profit.get('note', 'no closed positions')}")
    else:
        for key in ("closed_trades", "net_pnl", "gross_pnl", "total_fees",
                    "expectancy_per_trade", "win_rate", "profit_factor",
                    "average_win", "average_loss", "average_r_multiple",
                    "max_drawdown", "max_drawdown_pct"):
            value = profit.get(key)
            if isinstance(value, float):
                out.append(f"  {key:<24} {value:.6g}")
            else:
                out.append(f"  {key:<24} {value}")
        for segment in ("by_regime", "by_symbol", "by_side", "by_month", "by_session"):
            out.append(f"  {segment} (\u00a715.7)")
            for name, stats in sorted((profit.get(segment) or {}).items()):
                out.append(
                    f"    {name:<18} trades={stats['trades']:<4} "
                    f"net={stats['net_pnl']:.6g} win_rate={stats['win_rate']:.2%}"
                )
    out.append("")
    diagnosis = report["diagnosis"]
    out.append(f"DIAGNOSIS (§15.9)   sample={diagnosis['sample_size']} "
               f"({diagnosis['sample_adequacy']})")
    for finding in diagnosis["findings"]:
        out.append(f"  * {finding}")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None)
    parser.add_argument("--bot", default=None)
    parser.add_argument("--provenance", default=None,
                        help="restrict to one provenance, e.g. PAPER_FORWARD")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    for path in (BACKEND, SHARED):
        if path not in sys.path:
            sys.path.insert(0, path)
    os.chdir(BACKEND)
    env = os.path.join(BACKEND, ".env")
    if os.path.exists(env):
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env, override=True)

    from shared_lib.persistence.db import DB

    report = build(DB(args.db) if args.db else DB(),
                   bot_id=args.bot, provenance=args.provenance)
    print(json.dumps(report, indent=2, default=str) if args.json else render(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
