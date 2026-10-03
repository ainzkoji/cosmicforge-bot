"""Render the first CATI alpha design/evaluation report from saved evidence."""
import argparse
import json
from pathlib import Path


def render(artifact, tests):
    r = json.loads((artifact/'report.json').read_text())
    s = json.loads((artifact/'summary.json').read_text())
    operations = json.loads((artifact/'operations.json').read_text())
    def number(value):
        return 'N/A' if value is None else f'{value:.9f}'
    fields = dict(CURRENT_CATI_GENERATOR='Historical baseline: BREAKOUT_VOL_EXPANSION_V2, MOMENTUM_CONTINUATION_V1, RANGE_MEAN_REVERSION_V2, TREND_PULLBACK_V2 (unchanged)',
        ROOT_CAUSE_CONFIRMED='YES: audit matched gross -0.0083164315 R / net -0.1522378435 R; no stable positive gross alpha established',
        NEW_ALPHA_GENERATOR_ID=r['generator_id'], NEW_SETUP_FAMILIES='BREAKOUT_ACCEPTANCE_V1; TREND_RECLAIM_V1',
        ENTRY_CHANGES='Two-bar acceptance after contraction; pullback structure reclaim; trend, volume and exact closed BTC/ETH alignment',
        STOP_CHANGES='8-bar structural extreme plus 0.25 ATR buffer, minimum 1.5 ATR; minimum risk fraction 0.003',
        TARGET_CHANGES='3 ATR from entry; reject room below 1.25 R; no partials/trailing/breakeven hypothesis in this budget',
        COST_AWARENESS='Pre-candidate maximum estimated 0.15 R; modeled taker fee/spread/slippage/funding retained, no cost reduction',
        TIMEFRAME_HYPOTHESES='5m (unavailable); 15m (native); 1h (4 complete 15m bars)',
        HORIZON_HYPOTHESES='5m/72 bars (6h); 15m/32 bars (8h); 1h/16 bars (16h)',
        DEVELOPMENT_GROSS_R=number(s['pooled']['gross_R']), DEVELOPMENT_NET_R=number(s['pooled']['net_R']))
    for i, fold in enumerate(s['folds'], 1):
        fields[f'FOLD_{i}_GROSS_R'] = number(fold['gross_R'])
    for i, fold in enumerate(s['folds'], 1):
        fields[f'FOLD_{i}_NET_R'] = number(fold['net_R'])
    fields.update(ECONOMIC_VIABILITY_GATE='FAIL', NEXT_CATI_MODEL_TRAINING_ALLOWED='NO',
        CATI_AUTHORITY='OFF', LEGACY_V2_AUTHORITY_USED='NO', HOLDOUT_OPENED='NO', HOLDOUT_QUERY_COUNT=0,
        DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED='YES',
        FX_STATUS=f"STALLED: latest completion snapshot has {operations['fx']['remaining_periods']} remaining pair/day periods; provider failures; no worker visible; no writer started",
        RUNTIME_STATUS='STOPPED / STALE_LEASE at inspection. M0 governance would grant V2 entries on restart; runtime left stopped to respect CATI-only authority and preserve the router/runtime.',
        TESTS=tests, MAIN_COMMIT='Implementation commit is identified in the delivery message and git history',
        REMOTE_MAIN_PUSHED='Push confirmation is recorded in the delivery message')
    lines = ['# CATI alpha V1 — first bounded design and evaluation', '',
        '**Economic viability FAIL. Next CATI model training is blocked. CATI is not live ready.**', '',
        'The numbers below describe the union of all four available hypotheses (1,469 overlapping hypothetical opportunities). '
        'This union is diagnostic, never a selected strategy or executable portfolio. Every hypothesis is assessed independently below. '
        'Costs and execution differ from the frozen baseline (next-open entry, gap-aware stop, conservative funding reserve); '
        'the samples and horizon domains also differ. This is not a matched proof of improvement over V5.', '', '```text']
    lines += [f'{key} = {value}' for key, value in fields.items()]
    lines += ['```', '', '## Fixed design, source timestamps and invalidity', '',
        'Research generator only, implemented under CATI setups. The existing specialist registry, authority router, dispatcher, '
        'V5 joint distribution, broker/execution paths, portfolio exposure and 2.5% hard daily-loss policy are unchanged. '
        'No model is fitted and no setup is registered for runtime use.', '',
        'Two mechanisms × three domains = six declared hypotheses, one completed economic evaluation, no outcome-based parameter changes. '
        'An initial attempt aborted before any labels because 5m data was absent. The second attempt records this as unavailable and '
        'derives 1h bars from complete 15m groups. Both attempts are disclosed in summary.json.', '',
        '- Side: long when the 12-bar mean exceeds the 48-bar mean, otherwise short. Require signed 8-bar slow-mean slope ≥0.25 ATR, '
        'signed 12-bar return >0, prior-24-bar volume ratio ≥1.1, and distance from fast mean ≤2 ATR.',
        '- Breakout acceptance: previous and current closes beyond the prior 24-bar extreme (excluding both trigger bars); '
        'current wick holds within 0.25 ATR of the boundary; preceding 8/24 true-range compression ≤0.85; body displacement ≥0.25 ATR.',
        '- Trend reclaim: previous wick reaches its fast mean, then a directional candle closes beyond the previous high/low; displacement ≥0.35 ATR.',
        '- BTC and ETH signed 12-bar returns must align at the exact instrument candle close. No stale/as-of substitution. '
        'No order-book, actual funding, market breadth, relative-strength ranking or unavailable context is fabricated.',
        '- ATR uses 14 closed true ranges. Initial risk is max(distance to 8-bar swing plus 0.25 ATR, 1.5 ATR). '
        'Target is 3 ATR from the reference close, with at least 1.25 R room. Reject risk fraction <0.003, cost/R >0.15, or nonpositive levels.',
        '- Invalid/nonfinite OHLCV, nonpositive prices, negative volume, unordered/unaligned timestamps and future input fail closed. '
        'A missing bar invalidates 80-bar warmup. Incomplete future paths or fold-boundary maturity are excluded and counted.',
        '- Each setup version contains family, timeframe and horizon. Policy identity hashes generator, geometry and cost assumptions; '
        'source code SHA-256 also freezes exact entry rules. Labels have their own policy version and candidate/path identity.', '',
        'Candidates use the last closed candle reference; research execution occurs at the next bar open. Gap deviations enter gross R. '
        'Stops fill at the worse of stop/open; targets never claim favorable gap improvement; simultaneous stop/target touches resolve to stop. '
        'Terminal-bar MFE/MAE are censored OHLC bounds, not exact event-path training targets.', '',
        'Costs inherit the audit assumptions: fee 4 bps per side, half-spread 1 bp per side, slippage 2 bps per side, funding 1 bp per 8h. '
        'Admission reserves ceil(full horizon/8h) funding stamps; labels retain this reserve even for early exits. Fees/spread/slippage apply '
        'to entry plus exit notional. These are disclosed historical model assumptions, not current account-specific venue quotes.', '',
        'The fixed feasibility panel is ADA, BNB, BTC, DOGE, ETH, LINK, SOL and XRP. Selection was not based on profitable audited cells. '
        'It is still a small surviving-instrument panel on adaptively inspected development data; no whole-universe or untouched-confirmation claim is made. '
        'Source SQL is read-only, bounded below the inherited reserved holdout cutoff 1783876499999 ms. Labels also mature below each '
        'fold end. Five test windows follow a seed window over the same calendar range used by V5.', '',
        '## All hypotheses and chronological folds', '',
        '| Setup version | Fold | n | Gross R | Net R | Cost R | TARGET | STOP | TIMEOUT | Profit |',
        '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for name, result in r['results'].items():
        for i, f in enumerate(result['folds'], 1):
            lines.append('| '+ ' | '.join([name, str(i), str(f['samples'])]+[number(f.get(k)) for k in
                ('gross_R','net_R','cost_R','TARGET_rate','STOP_rate','TIMEOUT_rate','profit_rate')])+' |')
    lines += ['', '| Setup version | n | Pooled gross R | Pooled net R | Gate |',
              '| --- | ---: | ---: | ---: | --- |']
    for name, result in r['results'].items():
        f = result['pooled']
        lines.append(f"| {name} | {f['samples']} | {number(f['gross_R'])} | {number(f['net_R'])} | {result['ECONOMIC_VIABILITY_GATE']} |")
    lines += ['', '## Admission and drift', '',
        'Each hypothesis must independently have ≥300 candidates and ≥60 observed UTC days in every fold, positive gross expectancy '
        'in every fold, positive day-clustered 95% net lower bound in every fold, and pooled net expectancy positive at twice modeled costs. '
        'Missing data, empty folds, low support or recent deterioration fail admission. No pooled result can override a failed fold. '
        'The normal day-cluster interval is descriptive and does not remove serial dependence or multiplicity; even a future pass would '
        'still require broader validation, coherent CATI forecast/calibration, pre-holdout certification and explicit holdout authorization.', '',
        'Both reclaim hypotheses are gross-negative overall and net-negative in every fold; fold 3 deteriorates substantially. '
        'The pooled diagnostic union is gross-positive only in folds 2 and 4, but every fold loses net. '
        'The 15m breakout has a positive gross average but only 35 samples, a negative net average and three stop losses in the latest fold. '
        'The 1h breakout has only 10 samples and an empty fourth fold. These samples do not support a credible regime-specific claim. '
        'Report.json includes counts, cost burden, direction mix, uncertainty and all per-instrument diagnostics. No profitable cell is promoted.', '',
        'The generator hypotheses are rejected for promotion; no next forecast model is trained. A revised hypothesis needs a new identity '
        'and declared budget, never retuned V1 definitions. The reserved holdout stays closed. Demo, forward demo and explicit production '
        'approval remain separate later requirements; legacy V2 is absent from that progression.', '',
        '## Evidence and reproduction', '',
        'Saved evidence: [report](artifacts/cati_alpha_v1/report.json), [registry](cati_alpha_v1_registry.json), '
        '[summary and attempts](artifacts/cati_alpha_v1/summary.json), [full fresh labels](artifacts/cati_alpha_v1/labels.jsonl.gz), '
        '[operational observations](artifacts/cati_alpha_v1/operations.json). '
        'The report records source prefix hashes, source/evaluator code hashes, label hash, baseline revision and dirty-tree disclosure.', '',
        '```powershell',
        '& backends/venv/Scripts/python.exe scripts/evaluate_cati_alpha.py --database backends/shared/shared_lib/persistence/cosmicforge.db --registry docs/research/cati_alpha_v1_registry.json --output <fresh-output-directory>',
        '& backends/venv/Scripts/python.exe scripts/render_cati_alpha_report.py --artifact docs/research/artifacts/cati_alpha_v1 --tests "<test-result>" --output docs/research/cati_alpha_v1_first_report.md',
        '```', '',
        'Reproduction runs are not additional tuning attempts; preserve their identities. No source writer, holdout registry mutation, '
        'runtime promotion or production activation occurs in either tool.']
    return '\n'.join(lines)+'\n'


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--artifact', type=Path, required=True)
    p.add_argument('--tests', required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    a.output.write_text(render(a.artifact, a.tests), encoding='utf-8')
