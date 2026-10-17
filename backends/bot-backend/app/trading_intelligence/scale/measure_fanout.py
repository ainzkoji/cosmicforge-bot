"""Measure signal-once / size-per-account at 1, 10, 100 and 1,000 accounts (Section H, Step 5.4).

    python -m app.trading_intelligence.scale.measure_fanout --out evidence/step5/signal_fanout.json

Same synthetic market fixture and the same signal for every account count (``performance.synthetic_market``).
For each count the shared market stage is recomputed and timed (it must not depend on the count), then every
account is sized by the frozen decision with concurrency 1 and with a bounded pool. Accounts differ in risk
level, equity and holdings. Synthetic data on the host recorded in the file; not venue throughput and not a
Linux certification.
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
import tracemalloc
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import performance as P

from .fanout import AccountSnapshot, SharedSignal, size_accounts

COUNTS = (1, 10, 100, 1000)
LEVELS = ("conservative", "balanced", "aggressive")
DECISION_DAY = "2020-12-01"
#: Production cycles are daily; a sizing pass for every account must finish far inside the one-minute window the
#: executable lane allows after the close. The budget recorded here is that minute, per cycle.
CYCLE_BUDGET_MS = 60_000.0


def _books(n: int, market: Any) -> List[AccountSnapshot]:
    from app.trading_intelligence.families.daily_trend import targets as T
    snaps = []
    for k in range(n):
        book = DT.new_book(LEVELS[k % 3], T.EXECUTABLE, 5_000.0 + 97.0 * k, market.day - 30)
        book.last_day = market.day - 1
        if k % 2:  # half the accounts already hold what an earlier decision bought
            first = DT.decide(book, market)
            for o in first.orders[: 1 + k % 3]:
                price = float(market.features.close[market.row, market.index(o.symbol)])
                book.positions[o.symbol] = DT.Position(quantity=o.quantity, stop=price * (1 - o.stop_distance),
                                                       mark=price, entry_day=market.day - 5, entry_price=price,
                                                       entry_stop_distance=o.stop_distance)
        snaps.append(AccountSnapshot(f"acct-{k:05d}", f"tenant-{k % 97:03d}", 1, book))
    return snaps


def measure(counts: Sequence[int] = COUNTS, *, concurrency: int = 8, repeats: int = 3) -> Dict[str, Any]:
    panel, metadata = P.synthetic_market()
    day = DT.day_of(DECISION_DAY)
    rows = []
    for n in counts:
        signal_ms, seq_ms, par_ms, cpu_ms = [], [], [], []
        last = None
        for _ in range(repeats):
            signal = SharedSignal.compute(DT.FAMILY_ID, DECISION_DAY, lambda: DT.market_stage(panel, metadata, day))
            signal_ms.append(signal.computed_ms)
            snaps = _books(n, signal.market)
            c0 = time.process_time()
            seq = size_accounts(signal, snaps, max_concurrency=1, cycle_budget_ms=CYCLE_BUDGET_MS)
            cpu_ms.append((time.process_time() - c0) * 1000)
            par = size_accounts(signal, snaps, max_concurrency=concurrency, cycle_budget_ms=CYCLE_BUDGET_MS)
            seq_ms.append(seq.total_ms)
            par_ms.append(par.total_ms)
            assert not seq.errors and not par.errors
            assert all(seq.sizings[a].result.document["targets"] == par.sizings[a].result.document["targets"]
                       for a in seq.sizings), "parallel sizing changed a result"
            last = par
        tracemalloc.start()
        signal = SharedSignal.compute(DT.FAMILY_ID, DECISION_DAY, lambda: DT.market_stage(panel, metadata, day))
        size_accounts(signal, _books(n, signal.market), max_concurrency=concurrency)
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        med = statistics.median
        rows.append({
            "accounts": n,
            "shared_signal_ms_median": round(med(signal_ms), 3),
            "shared_signal_computations_per_cycle": 1,
            "sizing_sequential_ms_median": round(med(seq_ms), 3),
            "sizing_parallel_ms_median": round(med(par_ms), 3),
            "sizing_parallel_concurrency": concurrency,
            "sizing_cpu_ms_sequential_median": round(med(cpu_ms), 3),
            "per_account_ms_sequential": round(med(seq_ms) / n, 4),
            "per_account_latency_parallel": last.latency(),
            "cycle_budget_ms": CYCLE_BUDGET_MS,
            "all_within_cycle_budget": bool(last.within_budget),
            "peak_traced_memory_bytes": int(peak),
        })
    base = rows[0]["shared_signal_ms_median"]
    return {
        "schema": "cati-signal-fanout-v1",
        "measured_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": P.host_description(),
        "fixture": {"days": P.FIXTURE_DAYS, "symbols": P.FIXTURE_SYMBOLS, "seed": P.FIXTURE_SEED,
                    "decision_day": DECISION_DAY},
        "repeats": repeats,
        "rows": rows,
        "shared_signal_ratio_max_over_min": round(
            max(r["shared_signal_ms_median"] for r in rows) / max(min(r["shared_signal_ms_median"] for r in rows), 1e-9), 3),
        "shared_signal_ms_at_1_account": base,
        "interpretation": (
            "The shared market stage runs once per cycle whatever the account count; its time varies only with "
            "host noise. Total sizing work grows with the number of accounts. Thread parallelism helps only where "
            "the sizer releases the GIL; the figures say how much it did on this host."),
        "not_a_certification": P.NOT_A_CERTIFICATION,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args(argv)
    result = measure(concurrency=a.concurrency, repeats=a.repeats)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(result, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    for r in result["rows"]:
        print(f"{r['accounts']:>5} accounts: signal {r['shared_signal_ms_median']:.1f} ms, sizing seq "
              f"{r['sizing_sequential_ms_median']:.1f} ms / par {r['sizing_parallel_ms_median']:.1f} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
