"""Independent check of a Mandate 004 run against the frozen tables (Section H, Step 2.5, deliverable D12).

    backends/venv/Scripts/python.exe scripts/verify_mandate_004_run.py --run-id run_... [--samples 60]

It does NOT import the evaluator, the rule functions or the simulator. From the normalized tables alone it
recomputes, with plain loops, a random sample of what the run recorded:

* entry decisions -- signal strength (seven breakout windows with their exit windows), ATR(20), stop distance,
                     120-day history, 30-day median volume and top-20 universe membership;
* trades          -- entry fill = that day's open plus slippage; exit fill = next open, or the stop price, less
                     slippage; price P&L; fee and slippage; funding from the archive's own records;
* equity          -- the daily equity series equals initial equity plus the four ledgers, and its last value
                     equals the sum of the trades' net results.

Only rows of the period the run evaluated are read: for a development run nothing after the development end.
Exit code 0 when every sampled item agrees within the stated tolerance.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
EPOCH = date(1970, 1, 1)
LOOKBACKS = (10, 20, 30, 45, 65, 100, 150)
FEE = SLIP = 0.0005
TOL = 1e-9
STABLE = {"AEUR", "BFUSD", "BUSD", "DAI", "EURI", "FDUSD", "PYUSD", "RLUSD", "TUSD", "USD0", "USD1", "USDC", "USDD",
          "USDE", "USDP", "USDS", "UST", "XUSD"}


def day(text: str) -> int:
    return (date.fromisoformat(text) - EPOCH).days


def close_enough(a: float, b: float, tol: float = TOL) -> bool:
    return abs(a - b) <= tol * max(1.0, abs(a), abs(b))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--samples", type=int, default=60)
    ap.add_argument("--seed", type=int, default=18)
    ap.add_argument("--store", default=str(ROOT / "data" / "research" / "binance_usdm_daily_v1"))
    args = ap.parse_args()
    run = ROOT / "docs" / "research" / "mandate_004" / "runs" / args.run_id
    result = json.loads((run / "result.json").read_text(encoding="utf-8"))
    block = "holdout" if "holdout" in result["blocks"] else "development"
    first, last = day(result["blocks"][block]["first_day"]), day(result["blocks"][block]["last_day"])
    data_first = day(result["periods"]["development_start"])
    meta = json.loads((ROOT / "docs" / "research" / "datasets" / "binance_usdm_daily_v1.metadata.json").read_text(encoding="utf-8"))["symbols"]

    k = pd.read_parquet(Path(args.store) / "normalized" / "klines_1d.parquet")
    k = k[(k["day"] >= data_first) & (k["day"] <= last) & (k["trades"] > 0)]            # never a row after the period
    f = pd.read_parquet(Path(args.store) / "normalized" / "funding.parquet")
    f = f[(f["time_ms"] // 86_400_000 >= data_first) & (f["time_ms"] // 86_400_000 <= last)]
    bars = {s: g.set_index("day") for s, g in k.groupby("symbol")}
    # interpretation I1: a SETTLED symbol gives way to the same-base symbol on a day both have a bar
    for s in [x for x in bars if x.endswith("SETTLED")]:
        shorter = s
        while shorter.endswith("SETTLED"):
            shorter = shorter[:-7]
            if shorter in bars:
                bars[s] = bars[s].drop(index=bars[s].index.intersection(bars[shorter].index))
    funding = {s: g for s, g in f.groupby("symbol")}

    def in_scope(sym: str) -> bool:
        base = sym.replace("SETTLED", "")
        m = meta.get(base)
        return base[:-4] not in STABLE and not (m and m.get("contract_type") == "TRADIFI_PERPETUAL")

    def closes(sym: str, d: int, n: int):
        """The n closes ending on day d (None unless every one of the n days has a bar)."""
        b = bars.get(sym)
        if b is None:
            return None
        out = []
        for x in range(d - n + 1, d + 1):
            if x not in b.index:
                return None
            out.append(float(b.at[x, "close"]))
        return out

    def strength(sym: str, d: int) -> float:
        """Replays each window's ON/OFF state from the first bar of the symbol's current unbroken history."""
        b = bars[sym]
        start = d
        while start - 1 in b.index:
            start -= 1
        on = 0
        for look in LOOKBACKS:
            exit_n, state = max(5, look // 2), False
            for x in range(start, d + 1):
                c = float(b.at[x, "close"])
                prev = closes(sym, x - 1, look) if x - look >= start else None
                low = closes(sym, x - 1, exit_n) if x - exit_n >= start else None
                if prev is not None and c >= max(prev):
                    state = True
                elif low is not None and c < min(low):
                    state = False
            on += state
        return on / 7.0

    def stop_distance(sym: str, d: int):
        b = bars[sym]
        trs = []
        for x in range(d - 19, d + 1):
            if x not in b.index or x - 1 not in b.index:
                return None
            pc = float(b.at[x - 1, "close"])
            trs.append(max(float(b.at[x, "high"]) - float(b.at[x, "low"]), abs(float(b.at[x, "high"]) - pc), abs(float(b.at[x, "low"]) - pc)))
        return min(0.40, max(0.05, 3.0 * (sum(trs) / 20.0) / float(b.at[d, "close"])))

    def median_volume(sym: str, d: int):
        b = bars.get(sym)
        if b is None or any(x not in b.index for x in range(d - 119, d + 1)):       # 120 consecutive bars ending on d
            return None
        vals = sorted(float(b.at[x, "quote_volume"]) for x in range(d - 30, d))      # the 30 days before d
        return (vals[14] + vals[15]) / 2.0

    def universe(d: int):
        scored = [(-(v), s) for s in sorted(bars) if in_scope(s) and (v := median_volume(s, d)) is not None]
        return [s for _, s in sorted(scored)[:20]]

    def rows(name: str):
        with open(run / f"{block}_primary_{name}.csv", encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))

    rng = random.Random(args.seed)
    failures, checked = [], {"entry_decisions": 0, "universe_days": 0, "trades": 0, "funding_trades": 0}

    def check(ok: bool, what: str) -> None:
        if not ok:
            failures.append(what)

    # ---- entry decisions: strength, stop distance, universe membership ---------------------------------------
    decisions = rows("entry_decisions")
    universe_cache = {}
    for e in rng.sample(decisions, min(args.samples, len(decisions))):
        d, sym = day(e["day"]), e["symbol"]
        check(close_enough(strength(sym, d), float(e["strength"])), f"strength {sym} {e['day']}")
        sd = stop_distance(sym, d)
        check(sd is not None and close_enough(sd, float(e["stop_distance"])), f"stop distance {sym} {e['day']}")
        if d not in universe_cache:
            universe_cache[d] = universe(d)
            checked["universe_days"] += 1
        check(sym in universe_cache[d], f"universe membership {sym} {e['day']}")
        checked["entry_decisions"] += 1

    # ---- trades: fills, price P&L, fee, slippage, funding ------------------------------------------------------
    trades = [t for t in rows("trades") if t["exit_day"] and t["adjustments"] == "0" and t["exit_reason"] in ("TARGET_ZERO", "STOP")]
    for t in rng.sample(trades, min(args.samples, len(trades))):
        sym, e, x, qty = t["symbol"], day(t["entry_day"]), day(t["exit_day"]), float(t["quantity"])
        b = bars[sym]
        o_in = float(b.at[e, "open"])
        check(close_enough(float(t["entry_price"]), o_in * (1 + SLIP)), f"entry fill {sym} {t['entry_day']}")
        if t["exit_reason"] == "TARGET_ZERO":
            ref = float(b.at[x, "open"])
        else:                                                    # the stop: entry fill x (1 - d), ratcheted at each close
            stop = float(t["entry_price"]) * (1 - float(t["entry_stop_distance"]))
            for z in range(e, x):
                dz = stop_distance(sym, z)
                if dz is not None:
                    stop = max(stop, float(b.at[z, "close"]) * (1 - dz))
            check(float(b.at[x, "low"]) <= stop, f"stop was really touched {sym} {t['exit_day']}")
            check(all(float(b.at[z, "low"]) > s for z, s in _stops(b, e, x, t, stop_distance, sym)), f"stop not touched earlier {sym}")
            ref = float(b.at[x, "open"]) if float(b.at[x, "open"]) < stop else stop
        check(close_enough(float(t["exit_price"]), ref * (1 - SLIP)), f"exit fill {sym} {t['exit_day']}")
        check(close_enough(float(t["pnl_price"]), qty * (ref - o_in), 1e-7), f"price pnl {sym} {t['entry_day']}")
        slip = qty * o_in * SLIP + qty * ref * SLIP
        fee = qty * o_in * (1 + SLIP) * FEE + qty * ref * (1 - SLIP) * FEE
        check(close_enough(float(t["slippage"]), slip, 1e-7) and close_enough(float(t["fees"]), fee, 1e-7), f"costs {sym} {t['entry_day']}")
        # funding: later events on days e .. x-1 (and on a stop day x when a cost); 00:00 events on days e+1 .. x
        fr = funding.get(sym)
        paid, complete = 0.0, fr is not None
        if complete:
            fr = fr.assign(d=fr["time_ms"] // 86_400_000, mid=(fr["time_ms"] % 86_400_000) < 3_600_000)
            for z in range(e, x + 1):
                g = fr[fr["d"] == z]
                if g["interval_hours"].sum() < 24:
                    complete = False
                    break
                price = float(b.at[z, "open"])
                if z > e:
                    paid += qty * price * float(g[g["mid"]]["rate"].sum())
                later = g[~g["mid"]]["rate"]
                if z < x:
                    paid += qty * price * float(later.sum())
                elif t["exit_reason"] == "STOP":
                    paid += qty * price * float(later[later > 0].sum())
        if complete:
            check(close_enough(float(t["funding"]), paid, 1e-7), f"funding {sym} {t['entry_day']}: {t['funding']} vs {paid}")
            checked["funding_trades"] += 1
        checked["trades"] += 1

    # ---- the books ------------------------------------------------------------------------------------------------
    daily = rows("daily")
    eq, pnl, fees, slp, fund = 10_000.0, 0.0, 0.0, 0.0, 0.0
    for r in daily:
        pnl, fees, slp, fund = pnl + float(r["pnl_price"]), fees + float(r["fees"]), slp + float(r["slippage"]), fund + float(r["funding"])
        check(close_enough(float(r["equity"]), eq + pnl - fees - slp - fund, 1e-9), f"ledger {r['day']}")
    net = sum(float(t["net_pnl"]) for t in rows("trades"))
    check(close_enough(float(daily[-1]["equity"]) - 10_000.0, net, 1e-7), "sum of trade results equals the change in equity")
    s = result["blocks"][block]["scenarios"][result["primary_scenario"]]
    check(close_enough(s["final_equity"], float(daily[-1]["equity"])), "reported final equity")
    check(first == day(daily[0]["day"]) and last == day(daily[-1]["day"]), "period")

    print(json.dumps({"run_id": args.run_id, "block": block, "period": [result["blocks"][block]["first_day"], result["blocks"][block]["last_day"]],
                      "checked": checked, "daily_rows": len(daily), "failures": failures[:20], "status": "AGREES" if not failures else "DISAGREES",
                      "tolerance": TOL, "rows_read_through": (EPOCH + timedelta(days=int(k["day"].max()))).isoformat()}, indent=1))
    return 0 if not failures else 1


def _stops(b, e, x, t, stop_distance, sym):
    """(day, stop in force on that day) for the days before the exit."""
    stop = float(t["entry_price"]) * (1 - float(t["entry_stop_distance"]))
    for z in range(e, x):
        yield z, stop
        dz = stop_distance(sym, z)
        if dz is not None:
            stop = max(stop, float(b.at[z, "close"]) * (1 - dz))


if __name__ == "__main__":
    sys.exit(main())
