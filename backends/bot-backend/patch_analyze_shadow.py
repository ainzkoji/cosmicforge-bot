import re

with open("scripts/ml/analyze_shadow.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. Remove WHERE dt.gate_allowed = 1
code = code.replace("WHERE dt.gate_allowed = 1\n            ORDER BY dt.ts", "ORDER BY dt.ts")

# 2. Fix metrics calculation to reflect true improvement
old_metrics = """    # Scalar win rates
    pass_wr = _win_rate([r["realized_pnl"] for r in above_with_outcome])
    block_wr = _win_rate([r["realized_pnl"] for r in below_with_outcome])

    # Would-have-blocked count
    n_would_block = len(below_thresh)
    n_total = total_scored
    block_rate = n_would_block / n_total if n_total else 0.0

    # False positive: score < threshold but actual win (would have wrongly blocked a winner)
    false_positives = [r for r in below_with_outcome if r["realized_pnl"] is not None and r["realized_pnl"] > 0]
    fp_rate = len(false_positives) / len(below_with_outcome) if below_with_outcome else 0.0

    # Coverage: scored / total gate-allowed traces in period
    total_gate_allowed = len([t for t in trace_outcomes.values() if t["gate_allowed"]])
    coverage = total_scored / total_gate_allowed if total_gate_allowed else 0.0

    # Score concordance with rule engine: score > 0.5 when gate_allowed=1
    high_score_allowed = [r for r in matched if r["score"] > 0.5 and r["gate_allowed"]]
    concordance = len(high_score_allowed) / len([r for r in matched if r["score"] > 0.5]) if [r for r in matched if r["score"] > 0.5] else 0.0

    # Precision lift: pass_wr - block_wr (positive = model identifies weaker entries)
    precision_lift = (pass_wr - block_wr) if (pass_wr is not None and block_wr is not None) else None"""

new_metrics = """    # True improvement (PnL, win rate)
    baseline_outcomes = [r for r in matched if r["gate_allowed"] and r["realized_pnl"] is not None]
    ml_outcomes = [r for r in matched if r["gate_allowed"] and r["score"] >= threshold and r["realized_pnl"] is not None]
    blocked_outcomes = [r for r in matched if r["gate_allowed"] and r["score"] < threshold and r["realized_pnl"] is not None]

    baseline_wr = _win_rate([r["realized_pnl"] for r in baseline_outcomes])
    pass_wr     = _win_rate([r["realized_pnl"] for r in ml_outcomes])
    block_wr    = _win_rate([r["realized_pnl"] for r in blocked_outcomes])

    # True improvement over baseline
    precision_lift = (pass_wr - baseline_wr) if (pass_wr is not None and baseline_wr is not None) else None

    # Would-have-blocked count (only out of those rule engine ALLOWED)
    n_would_block = len([r for r in matched if r["gate_allowed"] and r["score"] < threshold])
    n_total_allowed = len([r for r in matched if r["gate_allowed"]])
    block_rate = n_would_block / n_total_allowed if n_total_allowed else 0.0

    # False positive: model blocked it, but it was a winner that rule engine allowed
    false_positives = [r for r in blocked_outcomes if r["realized_pnl"] > 0]
    fp_rate = len(false_positives) / len(blocked_outcomes) if blocked_outcomes else 0.0

    # Coverage: scored / total ALL traces in period
    total_traces = len(trace_outcomes.values())
    coverage = total_scored / total_traces if total_traces else 0.0

    # Concordance: ML and Rule Engine Agree (both pass OR both block)
    agreements = [
        r for r in matched 
        if (r["score"] >= 0.5 and r["gate_allowed"]) or (r["score"] < 0.5 and not r["gate_allowed"])
    ]
    concordance = len(agreements) / total_scored if total_scored else 0.0"""
code = code.replace(old_metrics, new_metrics)

# Fix precision output log
old_print = """    print(f"Total scored entries     : {total_scored}")
    print(f"  → Would PASS (≥{threshold:.2f})  : {len(above_thresh)}")
    print(f"  → Would BLOCK (<{threshold:.2f}) : {n_would_block}")
    print()
    print(f"Win rate — would PASS    : {pass_wr * 100:.1f}%" if pass_wr is not None else "Win rate — would PASS    : N/A (no closed trades)")
    print(f"Win rate — would BLOCK   : {block_wr * 100:.1f}%" if block_wr is not None else "Win rate — would BLOCK   : N/A (no closed trades)")
    print(f"Precision lift           : {precision_lift * 100:+.1f} pp" if precision_lift is not None else "Precision lift           : N/A")
    print(f"Block rate               : {block_rate * 100:.1f}% ({n_would_block}/{n_total})")"""

new_print = """    print(f"Total scored entries     : {total_scored}")
    print(f"  → ML passes (of gate allowed)   : {len(ml_outcomes)}")
    print(f"  → ML blocks (of gate allowed)   : {n_would_block}")
    print()
    print(f"Baseline Win Rate        : {baseline_wr * 100:.1f}%" if baseline_wr is not None else "Baseline Win Rate        : N/A")
    print(f"ML filtered Win Rate     : {pass_wr * 100:.1f}%" if pass_wr is not None else "ML filtered Win Rate     : N/A")
    print(f"Win rate of blocked      : {block_wr * 100:.1f}%" if block_wr is not None else "Win rate of blocked      : N/A")
    print(f"True improvement (Lift)  : {precision_lift * 100:+.1f} pp" if precision_lift is not None else "True improvement (Lift)  : N/A")
    print(f"Block rate on allowed    : {block_rate * 100:.1f}% ({n_would_block}/{n_total_allowed})")"""
code = code.replace(old_print, new_print)

with open("scripts/ml/analyze_shadow.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Analyze shadow patch completed.")
