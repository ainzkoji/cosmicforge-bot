"""Single bounded alpha V2 run; fresh labels, no ML fitting or holdout access."""
from __future__ import annotations
import argparse, hashlib, json, sqlite3, subprocess, sys, time
from dataclasses import asdict
from pathlib import Path
import numpy as np
import pandas as pd
import psutil
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'backends/bot-backend'),str(REPO/'backends/shared'),str(REPO/'scripts')]
from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.contracts.setup import timeframe_to_ms
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.setups.alpha_v2 import (GENERATOR_ID,LABEL_VERSION,COST_VERSION,HYPOTHESES,AlphaCosts,closed_features,htf_context,signal_mask,candidate_at)
from evaluate_cati_alpha import load_closed,HOLDOUT_START_MS,DEVELOPMENT_START_MS,DEVELOPMENT_STOP_MS


def registry():
    meta=json.loads((REPO/'data/research/calibration_diagnostics/v4_preparation/metadata.json').read_text())
    return dict(generator_id=GENERATOR_ID,label_version=LABEL_VERSION,cost_version=COST_VERSION,
        costs=asdict(AlphaCosts()),hypotheses=[asdict(h) for h in HYPOTHESES],
        symbols=sorted(meta['context_sources']),universe_policy='All V4 source universe, no profitability selection; exact-close basket requires >=30 complete assets',
        dataset_manifest_hash=meta['dataset_manifest_hash'],universe_source_sha256=hashlib.sha256((REPO/'data/research/calibration_diagnostics/v4_preparation/metadata.json').read_bytes()).hexdigest(),
        development_start_ms=DEVELOPMENT_START_MS,development_stop_ms=DEVELOPMENT_STOP_MS,holdout_start_ms=HOLDOUT_START_MS,
        maximum_mechanisms=4,maximum_evaluation_runs=1,minimum_fold_samples=300,minimum_fold_days=60,
        gate='All five folds gross>0, net UTC-day clustered two-sided95% lower bound>0; pooled2xcost net>0',
        RESEARCH_PROMISING_policy='Pooled>=1500 samples,>=300days,gross>0,net>0 and >=4/5 folds net>0; this is not admission',
        DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED='YES',runtime_eligible=False,model_fits=0,holdout_query_count=0,
        native_timeframe='15m',HTF_derivation='1h=4 and4h=16 exact complete aligned15m bars; closed-bar asof only, stale gaps reject',
        unsupported_observations=['5m','open_interest','actual_funding','basis','signed_trade_flow','orderbook_spread','actual_slippage'],
        cost_evidence='OHLCV only; retain V1 conservative modeled fee .0004,halfspread .0001,slippage .0002 each leg,funding .0001 per8h ceil full12h horizon. Not observed user fee tier or historical execution.',
        geometry='8bar structural invalidation+.25ATR,1.5ATR floor,.003 executable fraction floor; target prior96bar extreme; room>=1.25R; estimatedcost<=.15R')

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
    expected=registry()
    if json.loads(Path(args.registry).read_text())!=expected:
        raise ValueError('registry differs from predeclared budget')
    out=Path(args.output); out.mkdir(parents=True,exist_ok=False)
    (out/'registry.json').write_text(json.dumps(expected,indent=2)+'\n')
    started=time.monotonic(); peak=0
    conn=sqlite3.connect(Path(args.database).resolve().as_uri()+'?mode=ro',uri=True)
    conn.execute('PRAGMA query_only=ON')
    step=900000; start=DEVELOPMENT_START_MS-1280*step; stop=DEVELOPMENT_STOP_MS-1
    edges=np.linspace(DEVELOPMENT_START_MS,DEVELOPMENT_STOP_MS,7,dtype=np.int64)
    sources={}; unavailable={}; benchmarks={}; total=None; counts=None
    def load(symbol):
        return load_closed(conn,symbol,'15m',start,stop)
    # Pass1: exact timestamp equal-weight breadth, observed assets only.
    # Symbols are fixed by metadata before any economics is evaluated.
    for symbol in expected['symbols']:
        try:
            rows,digest=load(symbol); f=closed_features(rows,'15m',stop)
        except ValueError as exc:
            unavailable[symbol]=str(exc); continue
        sources[symbol]=dict(rows=len(rows),sha256=digest,first_open=rows[0][0],last_close=rows[-1][0]+step-1)
        series=f.set_index('closed_at').return_96.where(f.continuous_97.to_numpy())
        good=series.notna().astype(int)
        total=series.fillna(0) if total is None else total.add(series.fillna(0),fill_value=0)
        counts=good if counts is None else counts.add(good,fill_value=0)
        if symbol in ('BTCUSDT','ETHUSDT'): benchmarks[symbol]=f
        peak=max(peak,psutil.Process().memory_info().rss)
    if len(benchmarks)!=2:
        raise ValueError('causal BTC/ETH benchmark history unavailable')
    basket=(total/counts).where(counts>=30)
    observations={h.family:[] for h in HYPOTHESES}
    rejected={h.family:dict(signals=0,geometry_cost_rejected=0,incomplete_or_boundary=0) for h in HYPOTHESES}
    with (out/'labels.jsonl').open('w',encoding='utf-8') as labels:
        for number,symbol in enumerate(sources,1):
            rows,digest=load(symbol)
            if digest!=sources[symbol]['sha256']: raise ValueError('source changed between passes')
            f=benchmarks[symbol] if symbol in benchmarks else closed_features(rows,'15m',stop)
            one=htf_context(rows,1,stop,f.closed_at.to_numpy())
            four=htf_context(rows,4,stop,f.closed_at.to_numpy())
            aligned=basket.reindex(f.closed_at).to_numpy()
            key=InstrumentKey('CRYPTO',symbol[:-4],'USDT',symbol[:-4]+'/USDT:PERP','BINANCE_USDM',symbol)
            for h in HYPOTHESES:
                mask,signs=signal_mask(f,h,benchmarks['BTCUSDT'],benchmarks['ETHUSDT'],aligned,one,four)
                for index in np.flatnonzero(mask):
                    t=int(f.closed_at.iloc[index]); fold=int(np.searchsorted(edges,t,side='right')-1)
                    if not 1<=fold<=5: continue
                    rejected[h.family]['signals']+=1
                    if t+h.horizon*step>=edges[fold+1]:
                        rejected[h.family]['incomplete_or_boundary']+=1; continue
                    c=candidate_at(f,int(index),h,key,digest,int(signs[index]))
                    if c is None:
                        rejected[h.family]['geometry_cost_rejected']+=1; continue
                    label=label_path(c,rows[index+1:index+1+h.horizon],h,AlphaCosts())
                    if label is None:
                        rejected[h.family]['incomplete_or_boundary']+=1; continue
                    label.update(fold=fold,symbol=symbol,side=c.side,family=h.family)
                    # Artifact stores fresh canonical opportunity/label identities
                    # and actual causal contexts, while metrics stay bounded.
                    payload=dict(label, candidate=c.canonical_payload(),causal_features=dict(c.evidence_components),
                        basket_return96=None if not np.isfinite(aligned[index]) else float(aligned[index]),
                        closed_1h_at=None if pd.isna(one.closed_at.iloc[index]) else int(one.closed_at.iloc[index]),
                        closed_4h_at=None if pd.isna(four.closed_at.iloc[index]) else int(four.closed_at.iloc[index]))
                    labels.write(json.dumps(payload,sort_keys=True)+'\n')
                    observations[h.family].append(label)
            peak=max(peak,psutil.Process().memory_info().rss)
            if number%10==0: print(json.dumps(dict(processed=number,total=len(sources),peak_MB=peak/1048576)),flush=True)
    conn.close()
    results={}
    for h in HYPOTHESES:
        records=observations[h.family]; folds=[metrics([r for r in records if r['fold']==i]) for i in range(1,6)]; pooled=metrics(records)
        passed=viability(folds,pooled)
        promising=(pooled['samples']>=1500 and pooled['days']>=300 and pooled['gross_R']>0 and pooled['net_R']>0
                   and sum(x['net_R'] is not None and x['net_R']>0 for x in folds)>=4)
        results[h.family]=dict(folds=folds,pooled=pooled,counts=rejected[h.family],
            by_instrument={s:metrics([r for r in records if r['symbol']==s]) for s in sources},
            RESEARCH_PROMISING=promising,ECONOMIC_VIABILITY_PASS=passed,NEXT_CATI_MODEL_TRAINING_ALLOWED=passed)
        print(json.dumps(dict(hypothesis=h.family,pooled=pooled,gate=passed)),flush=True)
    digest=hashlib.sha256()
    with (out/'labels.jsonl').open('rb') as stream:
        for chunk in iter(lambda:stream.read(1048576),b''): digest.update(chunk)
    report=dict(generator_id=GENERATOR_ID,registry_hash=stable_hash(expected),results=results,source_queries=sources,
        unavailable_assets=unavailable,universe_registered=len(expected['symbols']),universe_available=len(sources),
        unsupported_observations=expected['unsupported_observations'],
        boundary=dict(HOLDOUT_OPENED=False,HOLDOUT_QUERY_COUNT=0,model_fits=0,runtime_eligible=False,DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED='YES'),
        source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),
        source_tree_dirty=bool(subprocess.check_output(['git','status','--porcelain'],cwd=REPO)),
        generator_source_sha256=hashlib.sha256((REPO/'backends/bot-backend/app/trading_intelligence/setups/alpha_v2.py').read_bytes()).hexdigest(),
        evaluator_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),labels_sha256=digest.hexdigest(),
        peak_RAM_MB=peak/1048576,elapsed_seconds=time.monotonic()-started,
        limitations=['Previously repeatedly inspected development only; no pristine confirmation.',
        'Survivor-biased existing source universe; exact-close basket coverage varies; no profitability selection.',
        'Overlapping hypothetical candidate returns; not portfolio PnL or actual broker fills.',
        'Day-clustered normal interval does not remove cross-day serial dependence or multiple testing.',
        'No actual spread/slippage/fee tier/funding observations; conservative inherited modeled assumptions.',
        'OHLC stop-first ambiguous ordering and censored terminal-bar MFE/MAE bounds.',
        'Source code was in an uncommitted shared main working tree; exact evaluator/generator hashes recorded; research-only.'])
    (out/'report.json').write_text(json.dumps(report,indent=2)+'\n')

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',required=True); parser.add_argument('--registry',required=True); parser.add_argument('--output',required=True)
    main(parser.parse_args())
