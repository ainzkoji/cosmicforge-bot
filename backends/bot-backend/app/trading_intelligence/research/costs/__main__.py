"""Repeatable collection of measured market costs::

    python -m app.trading_intelligence.research.costs collect --rounds 12 --interval 10
    python -m app.trading_intelligence.research.costs collect --rounds 0 --demo-db <runtime database>

``collect`` appends public top-of-book observations for the 20 most traded in-scope contracts, optionally reads
demo fills the engine already stored, summarises the archive's funding history for the development period, and
rewrites the cost table and the calibration status. It places no order and reads no credential. Run it again
on other days and at other hours: the table only calls a component calibrated once it has enough observations
on enough different days.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.trading_intelligence.research.governance.register import repository_root

from . import measured as MC

EVIDENCE_DIR = "docs/research/mandate_004/market_costs"


def _funding_history(store: Path, symbols: List[str], through: str) -> Dict[str, Any]:
    """Archive funding per symbol up to ``through`` (the development end): published history, not an estimate."""
    import pandas as pd

    from app.market_data.daily_dataset import day_index

    f = pd.read_parquet(Path(store) / "normalized" / "funding.parquet")
    f = f[(f["time_ms"] // 86_400_000 <= day_index(through)) & f["symbol"].isin(symbols)]
    f = f.assign(per_8h=f["rate"] * 8.0 / f["interval_hours"])
    out = {}
    for sym, g in f.groupby("symbol"):
        d = MC._distribution(g["per_8h"].tolist())
        out[sym] = {**d, "annualised_mean": d["mean"] * 3 * 365, "share_positive": float((g["rate"] > 0).mean()),
                    "data_source": MC.HISTORICAL_PUBLIC}
    allv = f["per_8h"].tolist()
    return {"through": through, "unit": "rate per 8 hours", "symbols": out,
            "all_symbols": {**MC._distribution(allv), "annualised_mean": (sum(allv) / len(allv)) * 3 * 365 if allv else None}}


def cmd_collect(args) -> int:
    from app.market_data.binance_archive import http_get
    from app.trading_intelligence.research.evaluator.official import frozen_cost_model
    from app.trading_intelligence.research.evaluator.simulator import in_scope_mask

    root = repository_root()
    out = root / EVIDENCE_DIR
    store = MC.MeasuredCostStore(out / "observations.jsonl")
    meta = json.loads((root / "docs/research/datasets/binance_usdm_daily_v1.metadata.json").read_text(encoding="utf-8"))
    added = {"public": 0, "demo": 0}
    symbols: List[str] = []
    if args.rounds > 0:
        symbols = MC.most_traded_symbols(http_get, lambda s: bool(in_scope_mask([s], meta)[0]))
        for i in range(args.rounds):
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            added["public"] += store.append(MC.observe_public_book(http_get, symbols, timestamp_utc=now,
                                                                   quality="SHORT_WINDOW_SNAPSHOT"))
            if i + 1 < args.rounds:
                time.sleep(args.interval)
    if args.demo_db:
        added["demo"] = store.append(MC.demo_execution_records(Path(args.demo_db)))
    records = store.records()
    table = MC.cost_table(records)
    covered = sorted({r["symbol"] for r in records if r["data_source"] == MC.LIVE_PUBLIC_OBSERVATION})
    cal = MC.calibrate(table, frozen_cost_model(), version=datetime.now(timezone.utc).strftime("%Y%m%d"), symbols=covered)
    evidence = {
        "schema": "market-cost-evidence-v1", "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "records": len(records), "by_provenance": {p: sum(1 for r in records if r["data_source"] == p) for p in MC.PROVENANCES},
        "observation_days": sorted({r["timestamp_utc"][:10] for r in records}),
        "cost_table": table, "calibration": {"status": cal.status, "basis": cal.basis, "reasons": list(cal.reasons),
                                             "cost_model": cal.cost_model},
        "frozen_evaluation_cost_model": frozen_cost_model(),
        "limits": ["Top-of-book spread only: no depth, no market impact, no fill was made.",
                   "A short window on one day is not a distribution over time of day or over market conditions.",
                   "Demo fills validate the integration; their slippage and fees are not representative of real money.",
                   "No real-money order was placed and none is needed for this collection."]}
    if args.funding_store:
        dev_symbols = sorted({line.split(",")[1] for line in Path(args.targets).read_text(encoding="utf-8").splitlines()[1:]}) \
            if args.targets else covered
        evidence["funding_history"] = _funding_history(Path(args.funding_store), dev_symbols, args.funding_through)
    out.mkdir(parents=True, exist_ok=True)
    (out / "market_costs.json").write_bytes((json.dumps(evidence, indent=1, sort_keys=True) + "\n").encode("utf-8"))
    print(json.dumps({"added": added, "records": len(records), "by_provenance": evidence["by_provenance"],
                      "calibration": cal.status, "symbols_observed": len(covered)}, indent=1))
    return 0


def render_markdown(ev: Dict[str, Any]) -> str:
    """The market-cost evidence as a readable document. Every figure is read from ``market_costs.json``."""
    def bps(x):
        return "n/a" if x is None else f"{x:.2f}"

    rows = ev["cost_table"]["symbols"]
    live = [(k.split(":")[1], r) for k, r in rows.items() if r["by_provenance"].get(MC.LIVE_PUBLIC_OBSERVATION)]
    spreads = sorted(r["spread_bps"]["median"] for _, r in live if r["spread_bps"]["n"])
    cal = ev["calibration"]
    frozen = ev["frozen_evaluation_cost_model"]["model"]
    L = ["# Mandate 004: market-cost evidence", "",
         f"Generated {ev['generated_at']} from `market_costs.json` (deliverable D11, Step 2.7).", "",
         "## Status", "",
         "| Item | Status |", "|---|---|",
         "| Measurement system (records, provenance, validation, cost table, versioned calibration) | IMPLEMENTED AND TESTED |",
         f"| Observed-cost calibration | **{cal['status']}** |",
         f"| Cost model used by the evaluation | unchanged: {cal['basis'] if cal['status'] != MC.CALIBRATED else 'the frozen mandate costs; the calibrated version is a separate, later version'} |",
         "", "## What was actually observed", "",
         "| Provenance | Records | What it is |", "|---|---|---|"]
    what = {MC.HISTORICAL_PUBLIC: "published history", MC.LIVE_PUBLIC_OBSERVATION: "public top-of-book snapshots (no account, no order)",
            MC.DEMO_EXECUTION: "fills on the exchange demo environment (not representative of real money)",
            MC.SIMULATED: "simulator output", MC.ASSUMED: "research assumptions"}
    for prov in MC.PROVENANCES:
        L.append(f"| {prov} | {ev['by_provenance'].get(prov, 0)} | {what[prov]} |")
    L += ["", f"Observation days: {', '.join(ev['observation_days']) or 'none'}.", ""]
    if live:
        L += ["### Top-of-book spread, most traded contracts", "",
              f"{sum(r['spread_bps']['n'] for _, r in live)} snapshots of {len(live)} contracts. Median of the per-contract medians: "
              f"{bps(spreads[len(spreads) // 2])} bps; tightest {bps(spreads[0])} bps, widest {bps(spreads[-1])} bps.", "",
              "| Contract | Snapshots | Median (bps) | 95th percentile (bps) | Largest (bps) | Days | Status |", "|---|---|---|---|---|---|---|"]
        for sym, r in sorted(live, key=lambda x: x[1]["spread_bps"]["median"]):
            d = r["spread_bps"]
            L.append(f"| {sym} | {d['n']} | {bps(d['median'])} | {bps(d['p95'])} | {bps(d['max'])} | {d['distinct_days']} | {d['status']} |")
        L += ["", f"For comparison, the frozen evaluation charges {frozen['slippage'] * 1e4:.1f} bps of slippage per side on top of a "
                  f"{frozen['taker_fee'] * 1e4:.1f} bps fee. A top-of-book spread says nothing about the cost of an order larger "
                  "than the quoted size.", ""]
    demo = [(k.split(":")[1], r) for k, r in rows.items() if r["by_provenance"].get(MC.DEMO_EXECUTION)]
    if demo:
        L += ["### Demo fills already recorded by the engine (Step 1 certification)", ""]
        for sym, r in demo:
            L.append(f"- {sym}: {r['slippage_bps']['demo_samples']} fill(s) with a recorded slippage, {r['fee_amount']['demo_samples']} with a "
                     "recorded fee. They are counted here and are not used for calibration.")
        L += ["", "No order-book snapshot and no bid or ask was captured at the time of those fills, so no spread can be "
                  "attributed to them. Their expected price is the plan's reference price at decision time; the difference to the "
                  "fill includes the time between decision and submission, not only execution cost.", ""]
    fh = ev.get("funding_history")
    if fh:
        a = fh["all_symbols"]
        L += ["### Funding history (published by the archive, development period)", "",
              f"Through {fh['through']}, {len(fh['symbols'])} contracts the strategy held a target in: {a['n']:,} funding records. "
              f"Mean {a['mean'] * 1e4:.3f} bps per 8 hours ({a['annualised_mean'] * 100:.1f}% a year for a long position), median "
              f"{a['median'] * 1e4:.3f} bps, 95th percentile {a['p95'] * 1e4:.3f} bps, extremes {a['min'] * 1e4:.1f} to {a['max'] * 1e4:.1f} bps.",
              "", "The evaluation does not use these summary figures: it charges each position the actual rate of each event.", ""]
    L += ["## Why this is not a calibration", ""] + [f"- {x}" for x in (cal["reasons"][:3] and
          ["A component is calibrated only with at least "
           f"{ev['cost_table']['min_samples']} representative observations on at least {ev['cost_table']['min_distinct_days']} different days; "
           "today's observations come from one short window."] or ["Enough observations exist."])]
    L += ["", "## Limits", ""] + [f"- {x}" for x in ev["limits"]]
    L += ["", "## How to collect more", "", "```bash",
          "cd backends/bot-backend && ../venv/Scripts/python.exe -m app.trading_intelligence.research.costs collect --rounds 12 --interval 10",
          "```", "", "Run it on different days and at different hours. It places no order and needs no credential. When the table "
          "reaches the sample requirement the calibration produces a new cost-model version; the frozen evaluation keeps the "
          "model it was run with.", ""]
    return "\n".join(L)


def cmd_report(args) -> int:
    out = repository_root() / EVIDENCE_DIR
    ev = json.loads((out / "market_costs.json").read_text(encoding="utf-8"))
    (out / "MARKET_COSTS.md").write_bytes(render_markdown(ev).encode("utf-8"))
    print(json.dumps({"report": str(out / "MARKET_COSTS.md"), "calibration": ev["calibration"]["status"]}))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="costs", description="CATI measured market costs")
    sub = p.add_subparsers(dest="command", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--rounds", type=int, default=12, help="public order-book snapshots to take (0 = none)")
    c.add_argument("--interval", type=float, default=10.0, help="seconds between snapshots")
    c.add_argument("--demo-db", default=None, help="runtime database to read stored DEMO fills from (read-only)")
    c.add_argument("--funding-store", default=None, help="dataset store: add the archive's funding history")
    c.add_argument("--funding-through", default="2024-12-31", help="last day of funding history to summarise")
    c.add_argument("--targets", default=None, help="a run's daily targets CSV: the symbols to summarise funding for")
    c.set_defaults(func=cmd_collect)
    sub.add_parser("report").set_defaults(func=cmd_report)
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
