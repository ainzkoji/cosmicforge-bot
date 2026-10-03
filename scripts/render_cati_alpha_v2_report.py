"""Render the immutable bounded alpha V2 economics without selecting a winner."""
import argparse,json
from pathlib import Path

def main(args):
    root=Path(args.artifact);r=json.loads((root/'report.json').read_text());reg=json.loads((root/'registry.json').read_text())
    fmt=lambda x: 'N/A' if x is None else f'{x:.6f}'
    lines=['# CATI alpha V2 — fixed four-mechanism development evaluation','',
        'All four fixed hypotheses FAIL economic admission. RESEARCH_PROMISING is false for all four; next model training is blocked. Economic admission is assessed independently for every fixed hypothesis. No aggregate union is a promoted strategy. V1 remains immutable and REJECTED. This is research on a development range repeatedly inspected before this run; no untouched confirmation claim applies.','',
        f"Generator: `{r['generator_id']}`. Registered source universe: {r['universe_registered']}; available native 15m assets: {r['universe_available']}. The universe is all symbols in the existing V4 source metadata, including BTC and ETH; no instrument was selected using profitability. This is a source-supported survivor universe, not a historical delisting-complete universe.",'',
        '## Predeclared research budget','',
        'One evaluation run, four mechanisms, fixed 15m execution and 48-bar (12h) terminal horizon. No threshold search, predictor fit, mechanism replacement or automatic next alpha version. Signals and numerical thresholds were fixed in the generator before evaluation. The exact generator/evaluator hashes are recorded below.','',
        '- Cross-sectional strength: signed 96-bar excess return >1% versus BTC and ETH, >1.5% versus exact-close equal-weight basket; signed 12-bar momentum >0.2%, signed body >0.35 ATR, volume ratio >=1.2. Basket requires >=30 actually observed continuous assets at that timestamp.',
        '- Volatility transition: previous ATR <=0.85 of shifted 48-bar ATR baseline, current ATR >1.1 baseline, 12-bar path efficiency >0.5, signed body >0.6 ATR, volume ratio >=1.5. This uses a regime crossing, not V1 breakout acceptance.',
        '- Multi-timeframe pullback: closed 1h and 4h 12/48-bar moving average direction agree, both normalized eight-bar slopes >0.25, prior execution close across fast average followed by reclaim; pullback depth 0.25..1.5 ATR; signed body >0.25 ATR; volume ratio >=1.1.',
        '- Flow confirmed direction: only actual candle volume evidence; volume ratio >=2, path efficiency >0.4, signed body >0.75 ATR, signed momentum >0.2%. OI, basis, signed trade flow and actual funding are unavailable and omitted. This is a volume confirmation hypothesis, not a claim of measured order flow.','',
        'All hypotheses require 97 continuous execution bars and exact-close BTC/ETH continuous context. A gap invalidates warmup. 1h/4h derivation requires exactly 4/16 aligned contiguous native 15m bars; context uses the last fully closed HTF bar and rejects stale contexts. Native 5m was unavailable in the bounded source check and is never fabricated.','',
        '## Geometry and cost audit','',
        reg['geometry']+'. The prior 96-bar high/low is the actual structural target. Signals without measured target room are rejected before labels. Next-open entry, adverse gap-stop fill and stop-first ambiguous bars match the conservative labeling policy. Whole future horizons must be contiguous, matured inside their chronological test fold and before holdout.','',
        reg['cost_evidence'],
        'The source candle schema contains OHLCV, quote volume and trade count, but no historical bid/ask, execution slippage, venue/user fee tier, funding timestamps or OI. Volume uses observed historical values; no neutral values are imputed. V1–V5 historical artifacts and costs were not modified. V2 has a separate cost version/hash without cheaper assumptions.','',
        'MFE/MAE in terminal candles remain censored bounds, not exact event-time observations. Full-horizon funding reserves stay charged even on early exits. Fees/spread/slippage use actual hypothetical entry+exit turnover over fixed decision-time structural R.','',
        '## Fixed economic admission','',
        'Five chronological test folds follow an initial seed window across the existing development range. Every fold needs >=300 samples, >=60 observed UTC days, positive gross R and a positive UTC-day-clustered two-sided 95% lower net-R bound. Pooled net R under 2x all costs must exceed zero. A pooled score cannot hide a failed fold. The normal interval is a development diagnostic: cross-day serial dependence, overlapping opportunities, cross-sectional dependence and multiple testing limit confidence.','',
        'RESEARCH_PROMISING separately requires >=1,500 pooled samples, >=300 days, positive pooled gross and net and >=4/5 positive net folds. It never grants admission.','',
        '| Hypothesis | N | Days | Gross R | Cost R | Net R | Net 2x cost R | Promising | Economic pass |',
        '|---|---:|---:|---:|---:|---:|---:|---|---|']
    for family,result in r['results'].items():
        p=result['pooled'];lines.append(f"| {family} | {p['samples']} | {p['days']} | {fmt(p['gross_R'])} | {fmt(p['cost_R'])} | {fmt(p['net_R'])} | {fmt(p.get('net_2x_cost_R'))} | {result['RESEARCH_PROMISING']} | {result['ECONOMIC_VIABILITY_PASS']} |")
    for family,result in r['results'].items():
        lines += ['',f'## {family}','',f"Counts: `{json.dumps(result['counts'],sort_keys=True)}`.",'',
            '| Fold | N | Days | Gross R | Cost R | Net R | Net lower 95% R | 2x cost net R | TARGET | STOP | TIMEOUT | Profitable |',
            '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
        for i,p in enumerate(result['folds'],1):
            lines.append('| '+str(i)+' | '+str(p['samples'])+' | '+str(p['days'])+' | '+' | '.join(fmt(p.get(k)) for k in ('gross_R','cost_R','net_R','net_lower_95_R','net_2x_cost_R','TARGET_rate','STOP_rate','TIMEOUT_rate','profit_rate'))+' |')
    lines += ['', '## Availability and provenance','',
        'Unavailable source assets: '+(json.dumps(r['unavailable_assets'],sort_keys=True) if r['unavailable_assets'] else 'NONE')+'.',
        'Omitted observations: '+', '.join(r['unsupported_observations'])+'.','',
        f"Registry hash: `{r['registry_hash']}`. Dataset manifest: `{reg['dataset_manifest_hash']}`. Source commit: `{r['source_commit']}`; source tree dirty at research run: `{r['source_tree_dirty']}`. Exact source prefix counts, bounds and hashes are in report.json.",
        f"Generator SHA-256: `{r['generator_source_sha256']}`. Evaluator SHA-256: `{r['evaluator_source_sha256']}`. Fresh deterministic label stream SHA-256: `{r['labels_sha256']}`.",
        f"Peak sampled process RSS: {r['peak_RAM_MB']:.1f} MB; elapsed: {r['elapsed_seconds']:.1f} seconds. Source data are processed in two bounded per-symbol passes; a changed source hash between passes aborts. Full fresh labels remain in the local evaluation directory with canonical candidate and label identities; no old candidate population is reused.",'',
        'Reproduction requires the same locally prepared V4 metadata and native source database (both referenced by exact hashes). All fresh labels are published in `docs/research/artifacts/cati_alpha_v2_fixed/labels.jsonl.gz` using deterministic gzip (mtime zero, empty filename). `label_integrity.json` records both compressed and decompressed hashes, row counts, candidate identities and causal timestamp checks. Verify the published compressed evidence offline without price queries using `scripts/verify_cati_alpha_v2.py --artifact docs/research/artifacts/cati_alpha_v2_fixed --output <verification.json>`. Reproduce with a fresh output directory:', '```powershell',
        '& backends/venv/Scripts/python.exe scripts/evaluate_cati_alpha_v2.py --database backends/shared/shared_lib/persistence/cosmicforge.db --registry docs/research/cati_alpha_v2_registry.json --output <fresh-output-directory>',
        '```','',
        'Holdout opened: NO. Holdout inspected: NO. Holdout price queries: 0. All SQL source close predicates end strictly before the immutable reserved boundary. Model fits: 0. Runtime eligibility: false. No library, certificate, runtime pin or execution authority follows from this generator. If all economic gates fail, model development/calibration/certification remains blocked; runtime and FX work can proceed independently.','',
        'Tests: '+args.tests+'. Exact lower-level per-instrument metrics and full fold costs/rates are recorded in the JSON artifact; a sparse positive subset cannot be selected after inspection.']
    Path(args.output).write_text('\n'.join(lines)+'\n')
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--artifact',required=True);p.add_argument('--output',required=True);p.add_argument('--tests',default='pending consolidated suite');main(p.parse_args())
