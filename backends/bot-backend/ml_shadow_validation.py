"""
CosmicForge ML Shadow-Mode Validation Analysis
Sections 1-6: Data availability, join coverage, counterfactual impact,
error profile, regime/symbol behavior, readiness judgment.
"""
import sys, io, json, math, sqlite3
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB = r'c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db'
conn = sqlite3.connect(DB, timeout=30)
conn.row_factory = sqlite3.Row

SEP = "=" * 70

# SECTION 1
print(SEP)
print("SECTION 1 - SHADOW DATA AVAILABILITY")
print(SEP)

total_scored = conn.execute(
    "SELECT COUNT(*) FROM decision_traces WHERE ml_score IS NOT NULL"
).fetchone()[0]

actions = conn.execute(
    "SELECT ml_action, COUNT(*) as n FROM decision_traces WHERE ml_action IS NOT NULL GROUP BY ml_action"
).fetchall()

print(f"Total ML-scored decision records: {total_scored:,}")
print("Action distribution:")
for row in actions:
    print(f"  {row['ml_action']:10s}: {row['n']:,}")

coverage = conn.execute(
    "SELECT MIN(ts), MAX(ts) FROM decision_traces WHERE ml_score IS NOT NULL"
).fetchone()
print(f"Date coverage: {coverage[0]} to {coverage[1]}")

sym_counts = conn.execute(
    "SELECT symbol, COUNT(*) as n FROM decision_traces WHERE ml_score IS NOT NULL "
    "GROUP BY symbol ORDER BY n DESC"
).fetchall()
print(f"Symbols scored: {len(sym_counts)}")
for r in sym_counts[:15]:
    print(f"  {r['symbol']:15s}: {r['n']:,}")

executed_ml = conn.execute(
    "SELECT COUNT(*) FROM decision_traces "
    "WHERE ml_score IS NOT NULL AND gate_allowed=1 AND position_opened=1"
).fetchone()[0]
print(f"ML-scored with position_opened=1: {executed_ml:,}")

completed = conn.execute("""
    SELECT COUNT(*) FROM trade_fills tf_close
    INNER JOIN trade_fills tf_open ON tf_open.position_id = tf_close.position_id
    WHERE tf_close.action='CLOSE' AND tf_open.action='OPEN'
      AND tf_close.realized_pnl IS NOT NULL
""").fetchone()[0]
print(f"Completed trades (OPEN+CLOSE): {completed:,}")

linked_count = conn.execute("""
    SELECT COUNT(*) FROM trade_fills tf_open
    INNER JOIN trade_fills tf_close ON tf_open.position_id = tf_close.position_id
    INNER JOIN decision_traces dt
        ON dt.run_id = tf_open.run_id
       AND dt.cycle_id = tf_open.cycle_id
       AND dt.symbol = tf_open.symbol
    WHERE tf_open.action='OPEN'
      AND tf_close.action='CLOSE'
      AND dt.ml_score IS NOT NULL
      AND tf_close.realized_pnl IS NOT NULL
""").fetchone()[0]
print(f"Completed trades WITH ML score linkable: {linked_count:,}")

if linked_count < 30:
    print("[!] WARNING: <30 linked trades. LOW statistical confidence.")
elif linked_count < 100:
    print("[!] CAUTION: <100 linked trades. Directional signal only.")
else:
    print(f"[OK] {linked_count} linked trades - sufficient for evaluation.")

# SECTION 2
print()
print(SEP)
print("SECTION 2 - JOIN ML SCORES TO REAL OUTCOMES")
print(SEP)
print("Join: trade_fills[OPEN] -> trade_fills[CLOSE] (position_id)")
print("      -> decision_traces (run_id + cycle_id + symbol)")

rows = conn.execute("""
    SELECT
        dt.ml_score, dt.ml_action, dt.ml_threshold,
        dt.symbol, dt.regime_state, dt.regime_confidence,
        dt.confidence AS dt_confidence, dt.ts,
        tf_open.side, tf_open.strategy,
        tf_close.realized_pnl, tf_close.r_multiple,
        tf_close.exit_reason, tf_close.mfe_pct, tf_close.mae_pct,
        tf_open.position_id
    FROM trade_fills tf_open
    INNER JOIN trade_fills tf_close ON tf_open.position_id = tf_close.position_id
    INNER JOIN (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY run_id, cycle_id, symbol
            ORDER BY COALESCE(confidence, 0) DESC, ts DESC
        ) AS rn FROM decision_traces
    ) dt
        ON dt.run_id = tf_open.run_id
       AND dt.cycle_id = tf_open.cycle_id
       AND dt.symbol = tf_open.symbol
       AND dt.rn = 1
    WHERE tf_open.action='OPEN'
      AND tf_close.action='CLOSE'
      AND dt.ml_score IS NOT NULL
      AND tf_close.realized_pnl IS NOT NULL
    ORDER BY dt.ts ASC
""").fetchall()

print(f"Joined rows: {len(rows):,}")
pos_ids = [r['position_id'] for r in rows]
dups = len(pos_ids) - len(set(pos_ids))
print(f"Duplicate position_ids: {dups}")
r_null = sum(1 for r in rows if r['r_multiple'] is None)
print(f"Null r_multiple: {r_null}/{len(rows)}")

wins   = [r for r in rows if (r['realized_pnl'] or 0) > 0]
losses = [r for r in rows if (r['realized_pnl'] or 0) < 0]
be     = [r for r in rows if (r['realized_pnl'] or 0) == 0]
n = max(len(rows), 1)
total_pnl = sum(r['realized_pnl'] for r in rows if r['realized_pnl'])
print(f"Wins: {len(wins):,} ({len(wins)/n*100:.1f}%)  "
      f"Losses: {len(losses):,} ({len(losses)/n*100:.1f}%)  "
      f"BE: {len(be):,}")
print(f"Total realized PnL (all linked): ${total_pnl:+,.4f}")

# SECTION 3
print()
print(SEP)
print("SECTION 3 - COUNTERFACTUAL ML IMPACT")
print(SEP)

def cf(data, thr):
    blocked = [r for r in data if (r['ml_score'] or 0) < thr]
    allowed = [r for r in data if (r['ml_score'] or 0) >= thr]
    bw = sum(1 for r in blocked if (r['realized_pnl'] or 0) > 0)
    bl = sum(1 for r in blocked if (r['realized_pnl'] or 0) < 0)
    aw = sum(1 for r in allowed if (r['realized_pnl'] or 0) > 0)
    al = sum(1 for r in allowed if (r['realized_pnl'] or 0) < 0)
    all_pnl = sum(r['realized_pnl'] for r in data   if r['realized_pnl'])
    alw_pnl = sum(r['realized_pnl'] for r in allowed if r['realized_pnl'])
    all_wr  = len(wins) / max(len(data), 1)
    alw_wr  = aw / max(len(allowed), 1)
    all_r   = [r['r_multiple'] for r in data   if r['r_multiple'] is not None]
    alw_r   = [r['r_multiple'] for r in allowed if r['r_multiple'] is not None]
    blk_r   = [r['r_multiple'] for r in blocked if r['r_multiple'] is not None]
    return {
        "thr": thr, "total": len(data), "blocked": len(blocked), "allowed": len(allowed),
        "bw": bw, "bl": bl, "aw": aw, "al": al,
        "all_pnl": all_pnl, "alw_pnl": alw_pnl,
        "pnl_delta": alw_pnl - all_pnl,
        "all_wr": all_wr, "alw_wr": alw_wr, "wr_delta": alw_wr - all_wr,
        "mr_all": sum(all_r)/len(all_r) if all_r else None,
        "mr_alw": sum(alw_r)/len(alw_r) if alw_r else None,
        "mr_blk": sum(blk_r)/len(blk_r) if blk_r else None,
    }

thresholds = [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]
results = {t: cf(rows, t) for t in thresholds}
live = results[0.30]

print(f"=== LIVE THRESHOLD = 0.30 ===")
print(f"  Total  : {live['total']:,}")
print(f"  Blocked: {live['blocked']:,} ({live['blocked']/max(live['total'],1)*100:.1f}%) "
      f"-- winners blocked: {live['bw']}, losers blocked: {live['bl']}")
print(f"  Allowed: {live['allowed']:,} ({live['allowed']/max(live['total'],1)*100:.1f}%)")
print(f"  PnL   actual: ${live['all_pnl']:+,.4f}")
print(f"  PnL   gated : ${live['alw_pnl']:+,.4f}")
print(f"  PnL   delta : ${live['pnl_delta']:+,.4f}")
print(f"  WR    actual: {live['all_wr']*100:.1f}%")
print(f"  WR    gated : {live['alw_wr']*100:.1f}%")
print(f"  WR    delta : {live['wr_delta']*100:+.1f}pp")
if live['mr_all'] is not None:
    print(f"  R all: {live['mr_all']:+.4f}  R gated: {live['mr_alw']:+.4f}  R blocked: {live['mr_blk']:+.4f}")

print()
print(f"{'Threshold':>10} {'Blocked%':>10} {'PnL-delta':>12} {'WR-delta':>10} {'R-gated':>9}")
print("-" * 55)
for t in thresholds:
    r = results[t]
    mr = f"{r['mr_alw']:+.4f}" if r['mr_alw'] is not None else "    N/A"
    print(f"  {t:>8.2f}  {r['blocked']/max(r['total'],1)*100:>8.1f}%  "
          f"${r['pnl_delta']:>+10.4f}  {r['wr_delta']*100:>+8.1f}pp  {mr:>8}")

# SECTION 4
print()
print(SEP)
print("SECTION 4 - ERROR PROFILE (threshold=0.30)")
print(SEP)

THR = 0.30
good_blocks = [r for r in rows if (r['ml_score'] or 0) < THR and (r['realized_pnl'] or 0) < 0]
bad_blocks  = [r for r in rows if (r['ml_score'] or 0) < THR and (r['realized_pnl'] or 0) > 0]
good_allows = [r for r in rows if (r['ml_score'] or 0) >= THR and (r['realized_pnl'] or 0) > 0]
bad_allows  = [r for r in rows if (r['ml_score'] or 0) >= THR and (r['realized_pnl'] or 0) < 0]

def avg_pnl(lst):
    p = [r['realized_pnl'] for r in lst if r['realized_pnl']]
    return sum(p)/len(p) if p else 0.0

print(f"  Good blocks (losers blocked) : {len(good_blocks):,} ({len(good_blocks)/n*100:.1f}%) avg PnL ${avg_pnl(good_blocks):+.4f}")
print(f"  Bad  blocks (winners blocked): {len(bad_blocks):,} ({len(bad_blocks)/n*100:.1f}%) avg PnL ${avg_pnl(bad_blocks):+.4f}")
print(f"  Good allows (winners allowed): {len(good_allows):,} ({len(good_allows)/n*100:.1f}%) avg PnL ${avg_pnl(good_allows):+.4f}")
print(f"  Bad  allows (losers allowed) : {len(bad_allows):,} ({len(bad_allows)/n*100:.1f}%) avg PnL ${avg_pnl(bad_allows):+.4f}")

total_blocked_ct = len(good_blocks) + len(bad_blocks)
if total_blocked_ct > 0:
    bp = len(good_blocks) / total_blocked_ct
    print(f"\n  Block precision: {bp*100:.1f}% (% of blocks that were correct)")
    if bp >= 0.70:
        print("  THRESHOLD JUDGMENT: VIABLE - mostly blocks losers.")
    elif bp >= 0.50:
        print("  THRESHOLD JUDGMENT: MARGINAL - mixed precision.")
    else:
        print("  THRESHOLD JUDGMENT: RISKY - blocks more winners than losers.")
else:
    print("  THRESHOLD JUDGMENT: No trades blocked - no gating power at 0.30.")
    bp = 0.0

print("\n  GOOD BLOCKS (top losers ML would have caught):")
for r in sorted(good_blocks, key=lambda x: x['realized_pnl'])[:5]:
    print(f"    {r['symbol']:12s} score={r['ml_score']:.3f}  pnl=${r['realized_pnl']:+.4f}  exit={r['exit_reason']}")

print("\n  BAD BLOCKS (top winners ML would have incorrectly blocked):")
for r in sorted(bad_blocks, key=lambda x: -(x['realized_pnl'] or 0))[:5]:
    print(f"    {r['symbol']:12s} score={r['ml_score']:.3f}  pnl=${r['realized_pnl']:+.4f}  exit={r['exit_reason']}")

# SECTION 5
print()
print(SEP)
print("SECTION 5 - REGIME / SYMBOL BEHAVIOR")
print(SEP)

sym_stats = defaultdict(lambda: {"n":0,"wins":0,"pnl":0.0,"ml_pnl":0.0,"scores":[]})
for r in rows:
    s = r['symbol']; pnl = r['realized_pnl'] or 0; score = r['ml_score'] or 0
    sym_stats[s]["n"] += 1; sym_stats[s]["pnl"] += pnl; sym_stats[s]["scores"].append(score)
    if pnl > 0: sym_stats[s]["wins"] += 1
    if score >= THR: sym_stats[s]["ml_pnl"] += pnl

print(f"{'Symbol':14} {'N':>6} {'WinRate':>8} {'TotalPnL':>12} {'ML-gatePnL':>12} {'AvgScore':>9}")
print("-" * 65)
for sym, st in sorted(sym_stats.items(), key=lambda x: -abs(x[1]["pnl"])):
    wr = st["wins"] / max(st["n"], 1)
    avs = sum(st["scores"]) / max(len(st["scores"]), 1)
    print(f"  {sym:12} {st['n']:>5,}  {wr*100:>7.1f}%  ${st['pnl']:>+10.4f}  ${st['ml_pnl']:>+10.4f}  {avs:>8.3f}")

regime_stats = defaultdict(lambda: {"n":0,"wins":0,"pnl":0.0,"ml_pnl":0.0})
for r in rows:
    rg = r['regime_state'] or 'UNKNOWN'; pnl = r['realized_pnl'] or 0; score = r['ml_score'] or 0
    regime_stats[rg]["n"] += 1; regime_stats[rg]["pnl"] += pnl
    if pnl > 0: regime_stats[rg]["wins"] += 1
    if score >= THR: regime_stats[rg]["ml_pnl"] += pnl

print()
print(f"{'Regime':24} {'N':>6} {'WinRate':>8} {'TotalPnL':>12} {'ML-gatePnL':>12}")
print("-" * 65)
for rg, st in sorted(regime_stats.items(), key=lambda x: -x[1]["n"]):
    wr = st["wins"] / max(st["n"], 1)
    print(f"  {rg:22} {st['n']:>5,}  {wr*100:>7.1f}%  ${st['pnl']:>+10.4f}  ${st['ml_pnl']:>+10.4f}")

win_scores  = [r['ml_score'] for r in rows if r['ml_score'] is not None and (r['realized_pnl'] or 0) > 0]
loss_scores = [r['ml_score'] for r in rows if r['ml_score'] is not None and (r['realized_pnl'] or 0) < 0]
print(f"\n  Avg ML score - Winners: {sum(win_scores)/len(win_scores):.4f}" if win_scores else "  Avg ML score - Winners: N/A")
print(f"  Avg ML score - Losers : {sum(loss_scores)/len(loss_scores):.4f}" if loss_scores else "  Avg ML score - Losers : N/A")

print(f"\n  Win rate by ML score bucket:")
buckets = [(0.0,0.20,"0.00-0.20"),(0.20,0.30,"0.20-0.30"),(0.30,0.40,"0.30-0.40"),
           (0.40,0.50,"0.40-0.50"),(0.50,0.60,"0.50-0.60"),(0.60,1.01,"0.60+")]
print(f"  {'Bucket':12} {'N':>6} {'Wins':>6} {'WinRate':>9} {'AvgPnL':>12}")
print("  " + "-" * 48)
for lo, hi, label in buckets:
    br = [r for r in rows if r['ml_score'] is not None and lo <= r['ml_score'] < hi]
    bn = len(br)
    if bn == 0: continue
    bw = sum(1 for r in br if (r['realized_pnl'] or 0) > 0)
    bpnl = sum(r['realized_pnl'] for r in br if r['realized_pnl']) / bn
    print(f"  {label:12} {bn:>6,} {bw:>6,} {bw/bn*100:>8.1f}%  ${bpnl:>+10.4f}")

# SECTION 6
print()
print(SEP)
print("SECTION 6 - READINESS JUDGMENT")
print(SEP)

verdicts = []
if len(rows) < 30:
    verdicts.append(f"BLOCKER [data]      Only {len(rows)} linked trades. Need >=30.")
elif len(rows) < 100:
    verdicts.append(f"CAUTION [data]      {len(rows)} linked trades. Direction OK, no certainty.")
else:
    verdicts.append(f"OK      [data]      {len(rows)} linked trades - statistically usable.")

if total_blocked_ct == 0:
    verdicts.append("BLOCKER [gating]    Zero trades blocked at 0.30. No gating power.")
elif bp >= 0.70:
    verdicts.append(f"OK      [precision] {bp*100:.0f}% block precision - correctly targets losers.")
elif bp >= 0.50:
    verdicts.append(f"CAUTION [precision] {bp*100:.0f}% block precision - marginal discrimination.")
else:
    verdicts.append(f"BLOCKER [precision] {bp*100:.0f}% block precision - blocks more winners than losers.")

if live['pnl_delta'] > 0:
    verdicts.append(f"OK      [pnl]       ML gating improves PnL by ${live['pnl_delta']:+.4f}.")
elif live['pnl_delta'] < 0:
    verdicts.append(f"BLOCKER [pnl]       ML gating WORSENS PnL by ${live['pnl_delta']:+.4f}.")
else:
    verdicts.append("NEUTRAL [pnl]       Zero PnL impact from ML gating.")

if live['wr_delta'] > 0.02:
    verdicts.append(f"OK      [winrate]   WR improves by {live['wr_delta']*100:+.1f}pp with ML gating.")
elif live['wr_delta'] < -0.05:
    verdicts.append(f"CAUTION [winrate]   WR drops {live['wr_delta']*100:+.1f}pp with ML gating.")
else:
    verdicts.append(f"NEUTRAL [winrate]   WR delta: {live['wr_delta']*100:+.1f}pp.")

print("Per-check verdicts:")
for v in verdicts:
    print(f"  {v}")

blockers = [v for v in verdicts if v.startswith("BLOCKER")]
cautions = [v for v in verdicts if v.startswith("CAUTION")]
print()
if blockers:
    print(f"OVERALL: NOT READY FOR ACTIVATION ({len(blockers)} blocker(s), {len(cautions)} caution(s))")
elif cautions:
    print(f"OVERALL: CONDITIONALLY READY ({len(cautions)} caution(s)) - controlled activation possible")
    print("         Recommend 1-2 symbol test, shadow for 2 more weeks before full activation.")
else:
    print(f"OVERALL: READY FOR CONTROLLED ACTIVATION - all checks passed.")

conn.close()
print()
print(SEP)
print("AUDIT COMPLETE")
print(SEP)
