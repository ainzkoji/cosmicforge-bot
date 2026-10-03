"""Bounded CATI generator economics; no fitting, promotion or holdout query.

Run with the canonical venv. The fixed registry is required before any price
read. SQL predicates bound source closes strictly below the reserved holdout.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import numpy as np

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO/'backends/bot-backend'), str(REPO/'backends/shared')]
from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.contracts.setup import timeframe_to_ms
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.setups.alpha_v1 import (
    GENERATOR_ID, LABEL_VERSION, HYPOTHESES, AlphaCosts, candidate_at, closed_features, signal_mask,
)

# Boundary inherited from the existing reserved holdout; callers cannot move it.
HOLDOUT_START_MS = 1783876499999
DEVELOPMENT_START_MS = 1727275499999
DEVELOPMENT_STOP_MS = 1783833299999
SYMBOLS = ("ADAUSDT", "BNBUSDT", "BTCUSDT", "DOGEUSDT", "ETHUSDT", "LINKUSDT", "SOLUSDT", "XRPUSDT")


def registry():
    return dict(generator_id=GENERATOR_ID, label_version=LABEL_VERSION,
                hypotheses=[asdict(h) for h in HYPOTHESES], costs=asdict(AlphaCosts()),
                symbols=list(SYMBOLS), development_start_ms=DEVELOPMENT_START_MS,
                development_stop_ms=DEVELOPMENT_STOP_MS, holdout_start_ms=HOLDOUT_START_MS,
                maximum_hypotheses=6, maximum_evaluation_runs=1,
                minimum_fold_samples=300, minimum_fold_days=60,
                gate="Every fold gross>0 and net day-clustered 95% lower bound>0; pooled 2x-cost net>0",
                DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED="YES",
                universe_limitation="Fixed eight-symbol feasibility panel; not whole-universe certification. Survivorship and previously inspected development range prevent confirmation claims.",
                runtime_eligible=False, model_fits=0, holdout_query_count=0)


def load_closed(conn, symbol, timeframe, start, stop):
    if stop >= HOLDOUT_START_MS or start >= stop:
        raise ValueError("holdout boundary or invalid range")
    step = timeframe_to_ms(timeframe)
    source_timeframe = '15m' if timeframe == '1h' else timeframe
    source_step = timeframe_to_ms(source_timeframe)
    rows = conn.execute(
        "SELECT open_time,open,high,low,close,volume FROM historical_candles "
        "WHERE symbol=? AND interval=? AND data_source='binance' AND market_type='crypto' "
        "AND open_time>=? AND open_time+?-1<=? ORDER BY open_time",
        (symbol, source_timeframe, start, source_step, stop)).fetchall()
    if not rows:
        raise ValueError(f"missing real history: {symbol}/{timeframe}")
    if timeframe == '1h':
        # Research-local aggregation, no new acquisition or database writer.
        grouped = {}
        for row in rows:
            grouped.setdefault(row[0]//step*step, []).append(row)
        rows = [(time, chunk[0][1], max(r[2] for r in chunk), min(r[3] for r in chunk),
                 chunk[-1][4], sum(r[5] for r in chunk))
                for time, chunk in sorted(grouped.items())
                if len(chunk) == 4 and [r[0] for r in chunk] == [time+i*source_step for i in range(4)]
                and time+step-1 <= stop]
        if not rows:
            raise ValueError(f"missing complete derived 1h history: {symbol}")
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, separators=(',', ':')).encode()+b'\n')
    return rows, digest.hexdigest()


def label_path(candidate, future, hypothesis, costs):
    """Research-only next-open entry; gap stops, adverse same-bar ordering.

    Uses fixed decision-time risk. Gap entry deviations count in gross R;
    stop gap exits use the worse open, targets never claim price improvement.
    MFE/MAE are truncated at exit, with adverse ambiguous-bar timing.
    Full declared horizon funding reserve is retained even on early exits.
    """
    step = timeframe_to_ms(hypothesis.timeframe)
    expected = candidate.decision_time+1
    if len(future) != hypothesis.horizon or any(r[0] != expected+i*step for i, r in enumerate(future)):
        return None
    if future[-1][0]+step-1 >= HOLDOUT_START_MS:
        raise ValueError("holdout label overlap")
    sign = 1 if candidate.side == "LONG" else -1
    entry = future[0][1]
    stop, target = candidate.structural_invalidation, candidate.target_reference
    exit_price, outcome, exit_bar = future[-1][4], "TIMEOUT", len(future)-1
    ambiguous = False
    mfe, mae = 0., 0.
    for i, (_, op, high, low, close, volume) in enumerate(future):
        stop_touch = low <= stop if sign > 0 else high >= stop
        target_touch = high >= target if sign > 0 else low <= target
        if stop_touch:
            exit_price = min(op, stop) if sign > 0 else max(op, stop)
            outcome, exit_bar, ambiguous = "STOP", i, target_touch
        elif target_touch:
            exit_price, outcome, exit_bar = target, "TARGET", i
        # Excursions inside a terminal candle are interval-censored; report
        # bounds separately, never use them as training targets as exact values.
        mfe = max(mfe, sign*((high if sign > 0 else low)-entry)/candidate.initial_structural_risk)
        mae = max(mae, -sign*((low if sign > 0 else high)-entry)/candidate.initial_structural_risk)
        if outcome != "TIMEOUT":
            break
    risk = candidate.initial_structural_risk
    gross = sign*(exit_price-entry)/risk
    turnover = abs(entry)+abs(exit_price)
    parts = dict(fee_R=turnover*costs.fee/risk, spread_R=turnover*costs.half_spread/risk,
                 slippage_R=turnover*costs.slippage/risk,
                 funding_R=abs(entry)*costs.fractions(hypothesis)['funding']/risk)
    total = sum(parts.values())
    payload = dict(candidate_id=candidate.setup_candidate_id, label_policy=LABEL_VERSION,
                   policy_hash=candidate.setup_policy_hash, future_hash=stable_hash(future))
    return dict(label_id=stable_hash(payload), setup_candidate_id=candidate.setup_candidate_id,
                setup_version=candidate.setup_version, decision_time=candidate.decision_time,
                horizon=hypothesis.horizon, terminal=outcome, gross_R=gross, net_R=gross-total,
                cost_R=total, **parts, entry_next_open=entry, exit_price=exit_price,
                event_bar=exit_bar, ambiguous_stop=ambiguous, mfe_bar_bound_R=mfe, mae_bar_bound_R=mae)


def metrics(rows):
    if not rows:
        return dict(samples=0, gross_R=None, net_R=None, cost_R=None, net_lower_95_R=None, days=0)
    net = np.array([r['net_R'] for r in rows])
    gross = np.array([r['gross_R'] for r in rows])
    cost = gross-net
    days = np.array([r['decision_time']//86400000 for r in rows])
    unique = np.unique(days)
    centered = np.array([(net[days == d]-net.mean()).sum() for d in unique])
    se = float(np.sqrt(len(unique)/(len(unique)-1)*np.sum(centered**2))/len(net)) if len(unique) > 1 else None
    return dict(samples=len(rows), gross_R=float(gross.mean()), net_R=float(net.mean()),
                cost_R=float(cost.mean()), net_lower_95_R=float(net.mean()-1.96*se) if se is not None else None,
                days=len(unique), net_2x_cost_R=float((gross-2*cost).mean()),
                TARGET_rate=sum(r['terminal'] == 'TARGET' for r in rows)/len(rows),
                STOP_rate=sum(r['terminal'] == 'STOP' for r in rows)/len(rows),
                TIMEOUT_rate=sum(r['terminal'] == 'TIMEOUT' for r in rows)/len(rows),
                profit_rate=float((net > 0).mean()),
                long_rate=sum(r['side'] == 'LONG' for r in rows)/len(rows))


def viability(folds, pooled):
    # No pooled average can hide recent deterioration or missing evidence.
    return (len(folds) == 5 and all(f['samples'] >= 300 and f['days'] >= 60
            and f['gross_R'] > 0 and f['net_lower_95_R'] is not None
            and f['net_lower_95_R'] > 0 for f in folds)
            and pooled.get('net_2x_cost_R', -1) > 0)


def main(args):
    expected = registry()
    declared = json.loads(Path(args.registry).read_text())
    if declared != expected:
        raise ValueError("registry differs from the predeclared fixed budget")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    (out/'registry.json').write_text(json.dumps(expected, indent=2)+'\n')
    conn = sqlite3.connect(Path(args.database).resolve().as_uri()+'?mode=ro', uri=True)
    conn.execute('PRAGMA query_only=ON')
    costs, sources, results = AlphaCosts(), {}, {}
    # Seed + five equal calendar windows, same range as V5. Maturity is
    # enforced within each test window, not merely before the holdout.
    edges = np.linspace(DEVELOPMENT_START_MS, DEVELOPMENT_STOP_MS, 7, dtype=np.int64)
    rows_file = (out/'labels.jsonl').open('w', encoding='utf-8')
    for timeframe in ('5m', '15m', '1h'):
        step = timeframe_to_ms(timeframe)
        loaded, frames = {}, {}
        unavailable = None
        for symbol in SYMBOLS:
            try:
                rows, digest = load_closed(conn, symbol, timeframe, DEVELOPMENT_START_MS-80*step, DEVELOPMENT_STOP_MS-1)
            except ValueError as exc:
                unavailable = str(exc)
                break
            loaded[symbol] = rows
            frames[symbol] = closed_features(rows, timeframe, DEVELOPMENT_STOP_MS-1)
            sources[f'{symbol}/{timeframe}'] = dict(rows=len(rows), hash=digest, first_open=rows[0][0], last_close=rows[-1][0]+step-1,
                                                   derivation='4 complete 15m bars' if timeframe == '1h' else 'native')
        if unavailable:
            for h in (h for h in HYPOTHESES if h.timeframe == timeframe):
                results[h.setup_version] = dict(folds=[metrics([]) for _ in range(5)], pooled=metrics([]),
                    status='DATA_UNAVAILABLE', reason=unavailable, ECONOMIC_VIABILITY_GATE='FAIL',
                    NEXT_CATI_MODEL_TRAINING_ALLOWED='NO')
            print(json.dumps(dict(timeframe=timeframe, status='DATA_UNAVAILABLE', reason=unavailable)), flush=True)
            continue
        for hypothesis in (h for h in HYPOTHESES if h.timeframe == timeframe):
            observations, counts = [], dict(signals=0, geometry_cost_rejected=0, incomplete_or_boundary=0)
            for symbol in SYMBOLS:
                f, rows = frames[symbol], loaded[symbol]
                mask = signal_mask(f, hypothesis, frames['BTCUSDT'], frames['ETHUSDT'])
                key = InstrumentKey('CRYPTO', symbol[:-4], 'USDT', symbol[:-4]+'/USDT:PERP', 'BINANCE_USDM', symbol)
                for index in np.flatnonzero(mask):
                    time = int(f.closed_at.iloc[index])
                    fold = int(np.searchsorted(edges, time, side='right')-1)
                    if fold < 1 or fold > 5:
                        continue
                    counts['signals'] += 1
                    if time+hypothesis.horizon*step >= edges[fold+1]:
                        counts['incomplete_or_boundary'] += 1
                        continue
                    c = candidate_at(f, int(index), hypothesis, key, sources[f'{symbol}/{timeframe}']['hash'], costs)
                    if c is None:
                        counts['geometry_cost_rejected'] += 1
                        continue
                    label = label_path(c, rows[index+1:index+1+hypothesis.horizon], hypothesis, costs)
                    if label is None:
                        counts['incomplete_or_boundary'] += 1
                        continue
                    label.update(fold=fold, symbol=symbol, side=c.side,
                                 candidate=c.canonical_payload(), causal_features=dict(c.evidence_components),
                                 geometry_features=dict(c.geometry_features))
                    rows_file.write(json.dumps(label, sort_keys=True)+'\n')
                    observations.append(label)
            folds = [metrics([r for r in observations if r['fold'] == i]) for i in range(1, 6)]
            pooled = metrics(observations)
            passed = viability(folds, pooled)
            results[hypothesis.setup_version] = dict(folds=folds, pooled=pooled, counts=counts,
                by_instrument={s: metrics([r for r in observations if r['symbol'] == s]) for s in SYMBOLS},
                ECONOMIC_VIABILITY_GATE='PASS' if passed else 'FAIL',
                NEXT_CATI_MODEL_TRAINING_ALLOWED='YES' if passed else 'NO')
            print(json.dumps(dict(hypothesis=hypothesis.setup_version, samples=pooled['samples'], gross_R=pooled['gross_R'], net_R=pooled['net_R'], gate='PASS' if passed else 'FAIL')), flush=True)
    rows_file.close()
    conn.close()
    report = dict(generator_id=GENERATOR_ID, registry_hash=stable_hash(expected), results=results,
                  source_queries=sources, boundary=dict(HOLDOUT_OPENED='NO', HOLDOUT_QUERY_COUNT=0,
                  CATI_AUTHORITY='OFF', LEGACY_V2_AUTHORITY_USED='NO', model_fits=0,
                  DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED='YES'),
                  source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
                  source_tree_dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO)),
                  generator_source_sha256=hashlib.sha256((REPO/'backends/bot-backend/app/trading_intelligence/setups/alpha_v1.py').read_bytes()).hexdigest(),
                  evaluator_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  labels_sha256=hashlib.sha256((out/'labels.jsonl').read_bytes()).hexdigest(),
                  limitations=['Hypothetical overlapping per-candidate returns, not portfolio P&L or broker fills.',
                    'UTC-day normal intervals do not remove serial dependence or multiple testing.',
                    'OHLC same-bar stop first; terminal-bar excursions are censored bounds.',
                    'Modeled costs; no observed spread, slippage, funding, liquidity, breadth or borrow.',
                    'Eight-symbol panel; gate PASS alone cannot authorize runtime, training on a broader universe, holdout or production.'])
    (out/'report.json').write_text(json.dumps(report, indent=2)+'\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--registry', required=True)
    parser.add_argument('--output', required=True)
    main(parser.parse_args())
