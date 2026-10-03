"""Freeze the next design only. No evaluation, label generation or model fit."""
import hashlib
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'docs/research/cati_alpha_root_cause'

def sha(path): return hashlib.sha256((ROOT/path).read_bytes()).hexdigest()

def register():
    output=OUT/'next_edge_registry.json'
    if output.exists(): raise RuntimeError('FROZEN_REGISTRY_ALREADY_EXISTS')
    old=json.loads((ROOT/'docs/research/artifacts/cati_alpha_v2_fixed/registry.json').read_text())
    diag=json.loads((OUT/'diagnostics.json').read_text())
    features=json.loads((OUT/'historical_feature_source_hashes.json').read_text())
    universe=old['symbols']; carry_universe=sorted(features['sources'])
    common={
        'timeframe':'1h', 'decision_timestamp':'UTC aligned hour close, all input candle closes <= decision; only exact complete aligned 4x15m aggregation, reject gaps.',
        'beta_definition':'Trailing 672 complete hourly log returns, OLS with intercept on BTC and ETH hourly returns, no future samples; require full rank or SKIP. Coefficients are causal nuisance beta estimates, not a CATI prediction model.',
        'residual_definition':'Asset hourly log return minus contemporaneous fitted intercept/BTC/ETH common component; rolling windows use decision-time coefficients only.',
        'entry_timestamp':'Next native 15m open after hourly close, identical to the next aligned hourly open; no entry at inspected close.',
        'missing_data':'SKIP with reason; never forward-fill across missing prices. Mark/index max age 1h+1ms; funding may use only completed settlements strictly earlier than decision, age <=12h.',
        'entry_gap_rule':'If next-open price is beyond invalidation or target, mark NON_EXECUTABLE_GAP; never rescue geometry after observing an entry.',
        'selection':'Lexicographic symbol tie-break. One ranked opportunity per concurrent timestamp per family. Counterfactual labels and one-position-per-family portfolio replay are reported separately; overlapping labels never credited as independent fills.',
        'runtime_eligible':False,
    }
    families=[
        dict(common, family='RESIDUAL_MOMENTUM_PORTFOLIO_TOP1',universe=[s for s in universe if s not in ('BTCUSDT','ETHUSDT')],
             causal_inputs=['native 15m OHLCV -> complete 1h bars','BTC/ETH common beta','24h cumulative residual','672h residual volatility','14h ATR','prior24h swing'],
             entry_rule='Require >=30 complete eligible assets. Score=sum(last24h residual)/(std(last672h residual)*sqrt(24)). Select maximum absolute score >=2; LONG positive or SHORT negative. No V2 trigger/pullback/volume rules.',
             structural_risk='At decision: invalidation beyond prior24h low/high by .25 ATR14; risk=max(distance to swing,2*ATR14,.003*decision close). Fixed stop at decision close minus/plus risk.',
             target_logic='Decision close plus/minus 2.5 fixed structural R.',
             exit_label_rule='First stop/target touch in next native15m bars; gap-aware stop and next-open handling as immutable V2 label convention, stop priority for ambiguous same bar. Otherwise close after48h. MFE/MAE censored bounds. Funding reserve covers full48h.'),
        dict(common, family='DISPERSION_BREAK_RESIDUAL_RELATIVE_VALUE',universe=[s for s in universe if s not in ('BTCUSDT','ETHUSDT')],
             causal_inputs=['complete 1h OHLCV','672h BTC/ETH betas','24h cumulative residual zscores','cross-sectional dispersion','prior168h dispersion median'],
             entry_rule='Require >=30 complete assets; dispersion=std(24h residual zscores). Require dispersion/current trailing168h median >=1.5. Pair weakest and strongest residual zscores with spread >=3; LONG weakest, SHORT strongest. Rank only greatest eligible spread; no price-only V2 signal reuse.',
             leg_weights='Gross notional1: weights inverse residual hourly volatility, normalized to sum1. Paired two-leg capability and margin/protection validation mandatory before any governed use; no naked-leg fallback.',
             structural_risk='Fixed basket R fraction=2*std(last672h weighted paired hourly log-return spread)*sqrt(24), computed before entry, floor .003 of gross notional. Reject nonfinite/zero variance.',
             target_logic='Basket price PnL >=+2 fixed R or residual zscore spread <=.5.',
             exit_label_rule='At each subsequent synchronized closed hourly bar: stop basket price PnL<=-1R before target/convergence; timeout24h. Exact mark-to-market next-open fills; no intrabar basket touch inferred from unrelated leg extrema. Full24h funding buffer.'),
        dict(common, family='SETTLED_FUNDING_BASIS_RELATIVE_CARRY',universe=carry_universe,
             causal_inputs=['complete 1h prices from native15m','hourly historical mark/index closes and derived basis','last3 completed funding settlements','672h paired return volatility'],
             entry_rule='For assets with three prior settled rates, compute mean rate and latest basis. Short maximum mean-funding asset with basis>=+5bps; long minimum mean-funding asset with basis<=-5bps. Require funding difference>=.0002 and basis difference>=10bps. Select greatest funding difference, symbol tie-break; no predicted or same-settlement funding used.',
             leg_weights='Inverse paired hourly volatility weights normalized gross1; no spot/index assumed tradable, no cash-and-carry claim. Pair execution capability/protection/margin validation required before governed use.',
             structural_risk='Fixed basket R fraction=2*std(last672h weighted hourly leg-return spread)*sqrt(24), floor .003 of gross notional. No post-entry recalculation.',
             target_logic='Basis difference<=2bps or total basket gross price-plus-realized-funding PnL>=+2R.',
             exit_label_rule='Synchronized hourly basket mark: stop total gross<=-1R first, then target/convergence; timeout72h. Funding cashflows only from settlements after entry and before exit, long pays positive/short receives positive. Realized carry is gross return; full72h modeled funding buffer remains a separate conservative cost, never netted away.'),
    ]
    costs=dict(old['costs'])
    code_paths=['scripts/diagnose_cati_alpha_v2.py','scripts/audit_cati_data_inventory.py','scripts/audit_cati_additional_inventory.py','scripts/audit_cati_feature_inventory.py','scripts/register_cati_next_edge_mandate.py','backends/bot-backend/app/trading_intelligence/research/certification/policy.py']
    start,stop=old['development_start_ms'],old['development_stop_ms']
    width=(stop-start)//5
    folds=[{'fold':i+1,'start_inclusive_ms':start+i*width,'end_exclusive_ms':start+(i+1)*width if i<4 else stop+1} for i in range(5)]
    registry={
        'registry_id':'CATI_NEXT_EDGE_DISCOVERY_MANDATE_003','status':'FROZEN_PREPARED_NOT_EVALUATED','research_attempt_number':3,
        'maximum_major_hypotheses':3,'maximum_evaluation_runs':1,'families':families,
        'NEXT_RESEARCH_EXECUTED':'NO','model_fits':0,'evaluation_authorized':False,'holdout_query_count':0,
        'development_range_adaptively_inspected':True,'new_family_outcomes_queried':False,
        'development_start_ms':start,'development_stop_ms':stop,'holdout_start_ms':old['holdout_start_ms'],
        'cost_policy':{'version':'CATI_NEXT_EDGE_CONSERVATIVE_INHERITED_COST_003','rates':costs,
            'application':'Each leg pays fee+halfspread+slippage on entry/exit actual notional. Funding buffer abs .0001 per8h ceil full registered horizon for EACH leg. Gross funding carry in family3 is distinct and does not lower this reserve. Report1x/1.5x/2x entire modeled cost schedule; actual public-book proxies never replace conservative costs.',
            'costs_not_lowered':True},
        'minimum_evidence':{'per_fold_counterfactual_labels':300,'per_fold_active_UTC_days':60,'pooled_labels':1500,'pooled_active_UTC_days':300,
            'portfolio_evidence':'Report actual selected nonoverlapping positions, rejected/overlapping opportunities, group/time clustering and variance-based ESS. Research label support is not admission support; insufficient portfolio support stays BLOCKED.'},
        'fold_structure':folds,'purge':'Remove entries whose entire label horizon reaches next fold or development cutoff; no same-day resampling as independent evidence.',
        'economic_gate':'All five folds gross>0 and net UTC-day clustered two-sided95% lower bound>0; pooled net with2x modeled costs>0. Report all three attempts/multiplicity, no automatic promotion. Existing Section22 frozen certification and all configured/missing thresholds remain authoritative.',
        'portfolio_replay':'One open selected opportunity per family, no overlapping leg across selected baskets. Family results separate; no opportunistic combination after results. Count sample inadequacy honestly.',
        'failure_policy':'No adaptive replacement, threshold grid, model training, promotion, or automatic next-version sequence. If a family fails or lacks evidence, retain failure and stop.',
        'source_hashes':{'immutable_V2_labels':diag['source_labels_sha256'],'main_native15m_closed_prefix':diag['causal_source_hashes'],
            'deep_historical_feature_prefix':features,'code_sha256':{p:sha(p) for p in code_paths},
            'inventory_receipts_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in OUT.glob('*inventory.json')}},
        'source_causal_limit':'Backfilled historical observations are reconstructed causal timestamps, not proven historical ingestion vintages. Require source/gap/hash audit before evaluation. Historical 1m/5m inventory is not permission to inspect post-cutoff prices.',
        'governance':'M0','entry_authority':'BLOCKED','holdout_opened':False,
    }
    raw=json.dumps(registry,indent=2,allow_nan=False).encode()
    output.write_bytes(raw)
    (OUT/'next_edge_registry.sha256').write_text(hashlib.sha256(raw).hexdigest()+'  next_edge_registry.json\n')
    print(registry['registry_id'],hashlib.sha256(raw).hexdigest())

if __name__=='__main__': register()
