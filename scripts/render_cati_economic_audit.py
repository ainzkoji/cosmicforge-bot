"""Render the fixed V5 diagnostic evidence and explicitly scoped interpretation."""
import argparse,csv,json
from pathlib import Path


def render(source, output, verification=None):
    r=json.loads(Path(source).read_text(encoding='utf-8'))
    assert r['candidate_id']=='cati_v5_b131674954a223230581dbbc'
    e=r['outer_economics'];p=e['POOLED'];early=e['EARLY_1_2'];late=e['LATE_3_5'];lines=[]
    # Additional tabular views contain precisely the JSON evidence, no selection.
    def csvfile(name,rows):
        fields=list(dict.fromkeys(k for row in rows for k in row))
        with (Path(source).parent/name).open('w',newline='',encoding='utf-8') as f:
            w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
    raw=[]
    for prefix,populations in [('',e),('FULL_LABELS_',r['full_frozen_parent_raw_economics'])]:
        for name,value in populations.items():
            row=dict(population=prefix+name,**value)
            for key,amount in row.pop('cost_components').items():row[key]=amount
            raw.append(row)
    csvfile('raw_economics.csv',raw)
    csvfile('ranking_bins.csv',[dict(population=k,ranking=rank,partition=bins,**x) for k,v in r['rank_quality'].items() for rank in ('expected_R','probability_net_R') for bins in ('quintiles','deciles') for x in v[rank][bins]])
    csvfile('rank_correlations.csv',[dict(population=k,expected_R_spearman=v['expected_R']['spearman'],expected_R_kendall=v['expected_R']['kendall_tau'],gross_spearman=v['expected_R_gross_spearman'],cost_spearman=v['expected_R_cost_spearman'],probability_binary_spearman=v['probability_binary_spearman'],probability_binary_kendall=v['probability_binary_kendall']) for k,v in r['rank_quality'].items()])
    csvfile('oracle_diagnostic.csv',[dict(population=k,**v) for k,v in r['NON_DEPLOYABLE_HINDSIGHT_DIAGNOSTIC'].items()])
    def add(text=''):lines.append(text)
    def section(title):add('## '+title);add()
    def fmt(x):
        if x is None:return '—'
        if isinstance(x,float):return f'{x:.6f}'
        return str(x).replace('|','\\|')
    def table(heads,rows):
        add('| '+' | '.join(heads)+' |');add('| '+' | '.join(['---']*len(heads))+' |')
        for row in rows:add('| '+' | '.join(fmt(x) for x in row)+' |')
        add()
    populations=[(k,v) for k,v in e.items() if not k.startswith(('EARLY','LATE'))]
    add('# CATI economic edge viability audit — existing V5, no V6');add()
    add('**PRIMARY_DIAGNOSIS = MIXED.** Weak/slightly negative gross candidate economics, substantial cost/geometry burden, temporal deterioration and optimistic high-score payoff estimates act together. The existing V5 evidence does not establish a realistic, reliably selectable positive net trading edge. No V6 is built.');add()
    section('Population, boundaries and reproducibility')
    add('The primary matched ranking/economics population is the exact **51,887 saved nested outer predictions**. Pooled tails sort saved scores globally; fold tails sort within each fold. Fixed tails take ceil(n × fraction), with stable saved order resolving ties. All seven percentages and all eight fixed thresholds are reported; empty cells remain empty. No best percentage/threshold is chosen.');add()
    add('Separately, raw economics covers **all 3,007,222 frozen parent candidates**, including 2,494,072 in the outer calendar windows, plus the 250,602-row stride-12 development sample. Unsampled parent rows receive no invented predictions. Final POOLED fields use the matched outer population for comparability with V5 scores; full-label differences are shown below.');add()
    add('Exact frozen label fee/spread/slippage/funding/carry fields are used. Parent row hashes, cache identities, saved Brier/RMSE replay, joint weighted-payoff identity, fold maturity and label-horizon boundaries are verified. Holdout cutoff remains 1783876499999; no source-price/database query, model fit, output change, threshold optimization or production subset is part of this audit.');add()
    add('Candidate returns overlap across setups, instruments and time. They are hypothetical per-candidate R, not executable portfolio P&L, fills, turnover or capacity. UTC-day-clustered normal intervals handle simultaneous rows but not all serial/instrument dependence; no multiplicity adjustment is claimed.');add()
    add('**DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED = YES.** V1–V5 and this audit have inspected this range. No portion is pristine confirmation; positive development cells are not independent validation.');add()
    add('```powershell\n& C:/Projects/cosmicforge-bot/backends/venv/Scripts/python.exe scripts/audit_cati_economic_edge.py --full-parent-raw --output data/research/calibration_diagnostics/economic_edge_audit_reproduction\n& C:/Projects/cosmicforge-bot/backends/venv/Scripts/python.exe scripts/render_cati_economic_audit.py --input data/research/calibration_diagnostics/economic_edge_audit_reproduction/audit.json --output data/research/calibration_diagnostics/economic_edge_audit_reproduction/report.md\n```');add()
    add('Fresh output directories are required. `--cost-cache` can reuse a SHA-verified identical extraction. [Complete evidence and input/tool identities](artifacts/cati_v5_economic_edge_audit/audit.json); [assessment](artifacts/cati_v5_economic_edge_audit/assessment.json). The renderer supplies interpretation for this fixed V5 audit, not an automatic model admission decision.');add()
    section('Raw economics — matched outer population')
    table(['Population','n','Gross mean','Gross median','Cost mean','Net mean','Net median','Profitable'],[[k,v['samples'],v['mean_gross_R'],v['median_gross_R'],v['mean_cost_R'],v['mean_net_R'],v['median_net_R'],v['profitable_rate']] for k,v in populations])
    table(['Population','TARGET freq','STOP freq','TIMEOUT freq','TARGET net R','STOP net R','TIMEOUT net R'],[[k,*[v[x+'_frequency'] for x in ('TARGET','STOP','TIMEOUT')],*[v[x+'_mean_net_R'] for x in ('TARGET','STOP','TIMEOUT')]] for k,v in populations])
    table(['Population','Net std','p05','p25','p50','p75','p95'],[[k,v['std_net_R'],*[v['p'+str(q).zfill(2)] for q in (5,25,50,75,95)]] for k,v in populations])
    add('Matched gross means are slightly positive in folds 1–2 and negative in 3–5; all net means are negative. Pooled descriptive day-clustered net interval is [-0.172892, -0.131583] R. A slightly negative gross mean does not prove absence of every possible causal alpha; positive gross edge is not demonstrated in this population.');add()
    section('Full frozen parent population check')
    table(['Full label population','n','Gross R','Cost R','Net R'],[[k,v['samples'],v['mean_gross_R'],v['mean_cost_R'],v['mean_net_R']] for k,v in r['full_frozen_parent_raw_economics'].items()])
    v=r['full_development_sample_economics'];add(f"The stride-12 development sample (n={v['samples']:,}) averages {v['mean_gross_R']:.6f} gross, {v['mean_cost_R']:.6f} cost and {v['mean_net_R']:.6f} net R. Only full-parent fold 1 has positive gross mean; all full-parent folds lose net. Full/sample quantiles, terminal breakdowns and scenarios are in [raw_economics.csv](artifacts/cati_v5_economic_edge_audit/raw_economics.csv).");add()
    section('Frozen cost decomposition and stress scenarios')
    table(['Population','Fee R','Spread R','Slippage R','Funding R','Carry R','0× net R','1× net R','2× net R'],[[k,*[v['cost_components'][x] for x in ('fee_R','spread_R','slippage_R','funding_R','carry_R')],v['zero_x_cost_net_R'],v['one_x_cost_net_R'],v['two_x_cost_net_R']] for k,v in populations])
    add('Frozen labels charge round-trip taker fees/spread/slippage and funding over the **full 48-bar label horizon**, even for an earlier touch. Twelve hours completes one modeled 8-hour funding interval for every candidate. Cost burden is approximately 0.0015 / initial_risk_fraction. Funding is an average assumption, not observed funding stamps. Carry is zero; latency, market impact and borrow are not modeled. No extra components are fabricated.');add()
    add(f"Costs remove {p['mean_cost_R']:.6f} R per candidate, arithmetically {100*p['mean_cost_R']/(-p['mean_net_R']):.2f}% of the pooled net deficit. **GROSS_EDGE_POSITIVE = NO; EDGE_KILLED_BY_COSTS = NO for the pooled sign-flip definition**: gross is already negative. Costs do destroy small early and selected-tail positive gross means.");add()
    add(f"Early matched folds need more than {100*(1-early['mean_gross_R']/early['mean_cost_R']):.2f}% aggregate cost reduction to make their observed gross mean net positive. Halving fees under the frozen maker assumption alone, ignoring fill/adverse-selection effects, still gives {p['mean_net_R']+p['cost_components']['fee_R']/2:.6f} R pooled net. Removing all fees leaves {p['mean_net_R']+p['cost_components']['fee_R']:.6f} R; zero total costs still leaves -0.008316 R. Cost reduction alone cannot rescue the pooled population. These are counterfactual arithmetic, not evidence that such execution is feasible.");add()
    section('Every fixed V5 expected-R ranking tail')
    table(['Population','Top','n','Predicted R','Gross R','Net R','Profitable','TARGET','STOP','TIMEOUT'],[[x['population'],f"{100*x['fraction']:g}%",x['samples'],x['predicted_mean_R'],x['mean_gross_R'],x['mean_net_R'],x['profitable_rate'],x['TARGET_frequency'],x['STOP_frequency'],x['TIMEOUT_frequency']] for x in r['fixed_tails']])
    add('All seven pooled tails lose net. Fold 2 has isolated positive tails; fold 3 top 2% is positive; no fixed fraction is positive in every fold. Top-0.5% cells have roughly 50–55 rows/fold; its pooled descriptive interval spans [-0.221058, +0.101010] R. No fraction is selected. [Exact tail cells and intervals](artifacts/cati_v5_economic_edge_audit/fixed_tails.csv).');add()
    section('Fixed expectancy thresholds — no optimization')
    table(['Population','Predicted R >','n','Folds represented / 5','Predicted mean R','Realized net R','Profitable'],[[x['population'],x['threshold'],x['samples'],x['fold_coverage'],x['predicted_mean_R'],x['mean_net_R'],x['profitable_rate']] for x in r['fixed_expectancy_thresholds']])
    add('The >+0.10 cell averages +0.019395 R on 801 rows but loses in folds 1, 4 and 5; >+0.20 loses pooled. This is not a production rule or stable edge confirmation.');add()
    section('Coherent profit-probability thresholds')
    table(['Population','p >','n','Mean p','Actual profitable','Net R'],[[x['population'],x['threshold'],x['samples'],x['mean_probability'],x['profitable_rate'],x['mean_net_R']] for x in r['fixed_probability_thresholds']])
    add('p > 0.55 averages +0.045946 R on 187 rows but loses in folds 1, 4 and 5. p > 0.60 / 0.65 lose pooled, with 39 / 13 rows. Pooled probability calibration does not establish sparse-tail calibration or profitable decisions.');add()
    section('Rank quality, ordering breaks and feature selectability')
    table(['Population','Expected-R Spearman','Kendall','Gross Spearman','Cost Spearman','p vs binary Spearman','p vs binary Kendall'],[[k,v['expected_R']['spearman'],v['expected_R']['kendall_tau'],v['expected_R_gross_spearman'],v['expected_R_cost_spearman'],v['probability_binary_spearman'],v['probability_binary_kendall']] for k,v in r['rank_quality'].items()])
    add('Rank classification **D. mixed**: ranking persists late and improves relative losses, but strong gross discrimination/stable positive expectancy is absent. Spearman with net R is +0.312 pooled, with gross R +0.110, and with cost burden -0.667. These associations do not establish causal attribution; much of the forecast’s usefulness is compatible with avoiding cost-heavy candidates.');add()
    table(['Population','Ranking','Quintile realized means low→high','Decile realized means low→high','Breaks after decile'],[[k,key,' / '.join(f"{x['realized_mean']:.4f}" for x in v[key]['quintiles']),' / '.join(f"{x['realized_mean']:.4f}" for x in v[key]['deciles']),', '.join(map(str,v[key]['deciles_ordering_breaks'])) or 'none'] for k,v in r['rank_quality'].items() for key in ('expected_R','probability_net_R')])
    add('Both pooled quintile orderings are monotone. Expected-R pooled deciles reverse at 6→7; probability deciles reverse at 5→6 and 9→10. Fold reversals are reported, not hidden by pooling. [All bin counts/predicted means](artifacts/cati_v5_economic_edge_audit/ranking_bins.csv).');add()
    table(['Population','Score','Top minus bottom decile R','Top decile minus population R','Top 5% minus population R'],[[k,key,v[key]['top_minus_bottom_decile_R'],v[key]['top_decile_minus_population_R'],v[key]['top_5_percent_minus_population_R']] for k,v in r['rank_quality'].items() for key in ('expected_R','probability_net_R')])
    add('Deciles use np.array_split on ascending scores; fixed top fractions use ceil on descending scores. They can differ by one pooled boundary row. Both conventions are fixed and disclosed.');add()
    section('Joint payoff contribution errors in the top 20%')
    table(['State','Predicted p','Observed freq','Predicted R contribution','Realized R contribution','Bias R'],[[x['state'],x['mean_predicted_state_probability'],x['actual_state_frequency'],x['predicted_R_contribution'],x['realized_R_contribution'],x['contribution_bias_R']] for x in r['joint_payoff_bias_decomposition'] if x['population']=='POOLED' and x['ranking_slice']=='TOP_20_PERCENT'])
    add('Forecast +0.018041 R versus -0.066062 R realized is +0.084103 R optimism. TARGET_PROFIT contributes +0.072649 R (86.38%): 17.62% predicted state probability versus 14.15% observed. STOP contributes -0.010082 R bias, so underestimating stops is not the main pooled top-quintile explanation. TIMEOUT_LOSS is underpredicted (14.31% versus 18.03%), adding +0.012072 R optimism; TIMEOUT_PROFIT adds +0.009464 R. These are exact state-weighted contribution errors, not separate causal estimates of probability versus magnitude error. [Every fold/all-candidate decomposition](artifacts/cati_v5_economic_edge_audit/joint_payoff_bias.csv).');add()
    section('Setup economics — every group and fold retained')
    add('Diagnostic sufficient-evidence flag: ≥300 pooled rows and ≥50 in each fold, fixed before group inspection. This is a count flag, not a significance test or production filter. All groups, including sparse/empty cells, remain in the evidence.');add()
    table(['Dimension','Group','n','Gross R','Cost R','Net R','Profitable','Sufficient','Fold 1–5 net R'],[[x['dimension'],x['group'],x['samples'],x['mean_gross_R'],x['mean_cost_R'],x['mean_net_R'],x['profitable_rate'],x['evidence_sufficient'],' / '.join('—' if y['mean_net_R'] is None else f"{y['mean_net_R']:.4f}" for y in r['setup_group_economics'] if y['dimension']==x['dimension'] and y['group']==x['group'] and y['population'].startswith('FOLD'))] for x in r['setup_group_economics'] if x['population']=='POOLED'])
    add('Every sufficient pooled group loses net; none is positive in all five folds. Four positive pooled combination cells have only 1, 2, 67 and 132 rows. [All setup-family, side, regime, volatility and declared combination metrics in every fold](artifacts/cati_v5_economic_edge_audit/setup_groups.csv). No group deletion/universe filter is recommended.');add()
    section('Fixed population geometry deciles')
    add('Pooled outer q10–q90 boundaries are fixed, then applied unchanged to each fold. Labels never determine boundaries. Tied boundaries can create unequal/empty cells; none is rebalanced or optimized. [Exact edges and every per-fold decile](artifacts/cati_v5_economic_edge_audit/geometry_deciles.csv), with boundaries also in the JSON.');add()
    table(['Feature','Decile','n','Feature mean','TARGET','STOP','TIMEOUT','Gross R','Cost R','Net R'],[[x['feature'],x['decile'],x['samples'],x['value_mean'],x['TARGET_frequency'],x['STOP_frequency'],x['TIMEOUT_frequency'],x['mean_gross_R'],x['mean_cost_R'],x['mean_net_R']] for x in r['geometry_economics'] if x['population']=='POOLED'])
    add('All 30 pooled deciles lose net. Narrowest-risk decile: mean fraction 0.003872, cost 0.494659 R, net -0.487709 R; widest: 0.063585, 0.026593 R, -0.014990 R. Highest target-room decile reaches TARGET only 9.98% and STOP 70.88%, versus 27.33% / 37.52% in the lowest. Larger stated reward is not itself alpha. Cost and risk bins largely invert because cost is deterministic in risk fraction. These are diagnostic associations, not chosen geometry thresholds.');add()
    section('Event-time economics and unchanged 48-bar horizon')
    table(['Population','Terminal','n','Mean bars','Median','First ≤4','First ≤8','Net R','Funding R'],[[k,label,v['samples'],v['mean_elapsed_bars'],v['median_elapsed_bars'],v['first_4_bars_fraction'],v['first_8_bars_fraction'],v['net_R'],v['funding_R']] for k,terms in r['event_time_summary'].items() for label,v in terms.items()])
    table(['Terminal','Elapsed bucket','n','Gross R','Cost R','Net R','Profitable'],[[x['terminal'],f"{x['elapsed_bars_low']}–{x['elapsed_bars_high']}",x['samples'],x['mean_gross_R'],x['mean_cost_R'],x['mean_net_R'],x['profitable_rate']] for x in r['event_time_economics'] if x['population']=='POOLED'])
    add('Targets average 17.89 bars (4.47 h), stops 13.39 (3.35 h). 29.95% of stops versus 13.76% of targets occur within four bars. Stops are disproportionately early and targets slower; this does not prove targets are “too slow” or identify an optimal horizon. TIMEOUT is administrative censoring at 48 bars (12 h), with +0.322705 R conditional net mean.');add()
    add('Full-horizon funding is charged even to early exits in the frozen labels. TIMEOUT therefore does not accumulate more duration-based funding in this dataset; its lower mean funding reflects different risk geometry. Actual event-duration carry/cost accumulation and post-censor outcomes are unavailable. Horizon mismatch is a separately versioned hypothesis, not demonstrated optimality. No horizon or label changed. [Every fold event-time bucket](artifacts/cati_v5_economic_edge_audit/event_time_buckets.csv).');add()
    section('Temporal deterioration and existing feature drift')
    table(['Metric','Folds 1–2','Folds 3–5','Late minus early'],[[k,early[k],late[k],late[k]-early[k]] for k in ('mean_gross_R','mean_cost_R','mean_net_R','TARGET_frequency','STOP_frequency','TIMEOUT_frequency','predicted_mean_R','roc_auc','brier_skill')])
    add('**TEMPORAL_CLASSIFICATION = MIXED.** Net deterioration is 0.053377 R: gross deterioration 0.031688 R (59.37%), increased modeled cost 0.021690 R (40.63%). This is arithmetic, not causal identification. Predicted expectancy barely changes (+0.000919 R) while realized expectancy drops. AUC declines 0.592572→0.579253; skill 0.024968→0.016485. Net-R ranking remains positive late.');add()
    add('The full-label calendar check also has negative net in every fold but is not perfectly monotone (fold 4 improves over fold 3). An abrupt regime break, universally gradual decay or stale history alone is not established. PSI supports distribution shift, without proving its causal source.');add()
    table(['Fold','Brier skill','ROC-AUC','ATR-fraction PSI','Realized-vol PSI'],[[x['fold'],x['brier_skill'],x['roc_auc'],r['feature_drift'][str(x['fold'])]['atr_14_fraction'],r['feature_drift'][str(x['fold'])]['realized_vol_24']] for x in r['fold_probability_metrics']])
    section('NON_DEPLOYABLE_HINDSIGHT_DIAGNOSTIC')
    table(['Population','net > 0','net > +0.25','net > +0.50','net > +1.00'],[[k,*v.values()] for k,v in r['NON_DEPLOYABLE_HINDSIGHT_DIAGNOSTIC'].items()])
    add('Individual worthwhile outcomes exist (36.11% positive, 24.63% above +1 R). This is hindsight availability, not positive ex-ante conditional expectancy or predictive capability. No hindsight rule is defined.');add()
    section('One primary diagnosis and next architectural decision')
    add('**PRIMARY_DIAGNOSIS = MIXED**, ranked by measured impact:');add()
    add('1. Cost/geometry burden: 0.143921 R cost versus -0.008316 R matched gross; narrow risks amplify ordinary modeled notional charges into large R losses.');
    add('2. Weak/deteriorating gross setup economics: full-parent gross -0.015416 R, late matched gross -0.021590 R. Stable positive gross alpha is not demonstrated.');
    add('3. Tail payoff optimism/limited gross discrimination: +0.084103 R top-quintile optimism, mainly TARGET_PROFIT contributions; relative-loss ranking does not produce stable positive net selection.');add()
    add('**Do not build V6 on the unchanged generator yet.** Preregister separately versioned causal setup/entry and target/stop-geometry alpha hypotheses, with economic viability gates before further ML admission. Horizon/timeframe and additional actually observable market information should be independent hypotheses, not retrospective variants optimized on this audit. Validate execution-cost and event-duration funding semantics against independent fill/funding evidence before claiming maker routing, another venue or lower turnover restores edge. Do not simply lower modeled costs.');add()
    add('Do not remove the bad groups or promote sparse positive cells. Do not deploy expected R >0.10 or p >0.55. A bounded direct net-R/distributional ranking experiment could follow only after a separately researched generator demonstrates stable net economics under credible costs. Adaptive recency is a future hypothesis after distinguishing setup/cost decay from stale training with predeclared temporal tests. No V6 is implemented or independently validated here.');add()
    section('Engineering, runtime, FX and blockers')
    add('The first extraction rejected a raw SHA comparison against the library’s JSON-escaped-text hash; the canonical TextHasher corrected verification, without changing labels. Later numeric-only passes added diagnostics/full-parent checks, with zero new fits. A synthetic test fixture initially converted line endings on Windows; byte-exact writing corrected it. These attempts are disclosed.');add()
    if verification:
        observed=json.loads(Path(verification).read_text(encoding='utf-8'))
        add(observed['verification_summary']);add()
        add(observed['runtime_fx_observation']);add()
    else:
        add('Historical verification/runtime observations are separate from numeric reproduction; pass --verification with the published assessment.json to include them.');add()
    add('Production forecast behavior is untouched. Runtime remains PAPER/M0, CATI OFF, PID 19436/session rts_a605e6301df149ccb931, startup revision 81aceaa6, hard daily loss cap 0.025. Current Git HEAD does not reload startup modules. No restart, pin, promotion or V2 fallback.');add()
    add('FX uses the existing supervisor PID 31396 and queued finalization PID 35444. No duplicate writer was started. Strict UNKNOWN_GAP classification and freeze only on PASS remain unchanged.');add()
    add('Remaining blockers: no demonstrated stable selectable positive net candidate economics; execution/cost/horizon semantics; unresolved causal origin of temporal deterioration; sparse positives and repeated development inspection; untouched holdout/governance before admission; separate FX strict-freeze completion. HOLDOUT_OPENED = NO; HOLDOUT_INSPECTED = NO; HOLDOUT_QUERY_COUNT = 0; GOVERNANCE = M0; CATI_EXECUTION = OFF.');add()
    Path(output).write_text('\n'.join(lines)+'\n',encoding='utf-8')


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--input',required=True);parser.add_argument('--output',required=True);parser.add_argument('--verification')
    args=parser.parse_args();render(args.input,args.output,args.verification)
