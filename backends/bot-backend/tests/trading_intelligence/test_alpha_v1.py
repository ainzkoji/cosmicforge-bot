"""Causal boundaries, executable geometry and temporal economic admission."""
import importlib.util
from pathlib import Path

import numpy as np
import pytest

from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.setups.alpha_v1 import (
    AlphaCosts, HYPOTHESES, candidate_at, closed_features, signal_mask,
)

PATH = Path(__file__).resolve().parents[4]/'scripts/evaluate_cati_alpha.py'
spec = importlib.util.spec_from_file_location('alpha_evaluation', PATH)
evaluation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evaluation)
KEY = InstrumentKey('CRYPTO', 'BTC', 'USDT', 'BTC/USDT:PERP', 'BINANCE_USDM', 'BTCUSDT')


def history(n=160):
    rows = []
    for i in range(n):
        price = 100+i*.1+np.sin(i)*.2
        rows.append((i*900000, price, price+1, price-1, price+.1, 100+i%7))
    return rows


def test_features_and_signals_are_prefix_invariant():
    rows = history()
    prefix = closed_features(rows[:120], '15m', 120*900000-1)
    full = closed_features(rows, '15m', 160*900000-1)
    np.testing.assert_allclose(prefix.to_numpy(dtype=float), full.iloc[:120].to_numpy(dtype=float), equal_nan=True)
    for h in HYPOTHESES:
        if h.timeframe == '15m':
            np.testing.assert_array_equal(signal_mask(prefix, h, prefix, prefix),
                                          signal_mask(full, h, full, full)[:120])


def test_future_stale_gap_and_malformed_source_are_not_fabricated():
    rows = history()
    with pytest.raises(ValueError, match='future'):
        closed_features(rows, '15m', rows[-1][0])
    invalid = rows.copy()
    invalid[-1] = (*invalid[-1][:2], 0, *invalid[-1][3:])
    with pytest.raises(ValueError):
        closed_features(invalid, '15m', 160*900000-1)
    missing = rows[:90]+rows[91:]
    f = closed_features(missing, '15m', 160*900000-1)
    assert not f.continuous.iloc[-1]
    benchmark = closed_features(rows[:120], '15m', 120*900000-1)
    complete = closed_features(rows, '15m', 160*900000-1)
    assert not signal_mask(complete, HYPOTHESES[3], benchmark, benchmark)[120:].any()


def make_candidate():
    f = closed_features(history(), '15m', 160*900000-1)
    c = candidate_at(f, len(f)-1, HYPOTHESES[3], KEY, 'fixed-provenance')
    assert c is not None
    return c, f


def test_cost_and_geometry_gate_runs_before_candidate_acceptance():
    c, f = make_candidate()
    assert c.geometry_features['estimated_cost_R'] <= .15
    assert c.room_to_target_R >= 1.25
    assert candidate_at(f, len(f)-1, HYPOTHESES[3], KEY, 'source', AlphaCosts(fee=.1)) is None
    with pytest.raises(ValueError):
        AlphaCosts(fee=float('nan'))
    with pytest.raises(ValueError):
        AlphaCosts(funding_per_8h=-.1)
    assert c.setup_version != 'TREND_PULLBACK_V2'
    assert len({h.setup_version for h in HYPOTHESES}) == 6


def future_for(c):
    return [(c.decision_time+1+i*900000, c.trigger_reference,
             c.trigger_reference+.01, c.trigger_reference-.01, c.trigger_reference, 100.)
            for i in range(32)]


def test_next_open_and_stop_gaps_are_counted_and_ambiguity_is_adverse():
    c, _ = make_candidate()
    future = future_for(c)
    future[0] = (future[0][0], c.trigger_reference+.2, c.target_reference+.1,
                 c.structural_invalidation-.1, c.trigger_reference, 100.)
    label = evaluation.label_path(c, future, HYPOTHESES[3], AlphaCosts())
    assert label['terminal'] == 'STOP' and label['ambiguous_stop']
    assert label['gross_R'] < -1  # adverse next-open deviation
    assert label['net_R'] == pytest.approx(label['gross_R']-label['cost_R'])
    future[0] = (future[0][0], c.structural_invalidation-1,
                 c.structural_invalidation-.5, c.structural_invalidation-2,
                 c.structural_invalidation-1, 100.)
    gap = evaluation.label_path(c, future, HYPOTHESES[3], AlphaCosts())
    assert gap['exit_price'] == future[0][1]
    assert evaluation.label_path(c, future[:-1], HYPOTHESES[3], AlphaCosts()) is None
    future[10] = (future[10][0]+900000, *future[10][1:])
    assert evaluation.label_path(c, future, HYPOTHESES[3], AlphaCosts()) is None


def test_empty_or_deteriorating_folds_never_pass_pooled_gate():
    good = dict(samples=500, days=80, gross_R=.2, net_lower_95_R=.05)
    pool = dict(net_2x_cost_R=.01)
    assert evaluation.viability([good]*5, pool)
    assert not evaluation.viability([good]*4, pool)
    assert not evaluation.viability([good]*4+[dict(good, gross_R=-.01)], pool)
    assert not evaluation.viability([good]*4+[dict(good, samples=0)], pool)
    assert not evaluation.viability([good]*5, dict(net_2x_cost_R=-.01))


def test_source_query_blocks_holdout_and_derived_bars_require_completeness():
    import sqlite3
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE historical_candles (open_time,open,high,low,close,volume,symbol,interval,data_source,market_type)')
    for row in history(7):
        conn.execute('INSERT INTO historical_candles VALUES (?,?,?,?,?,?,?,?,?,?)', (*row, 'BTCUSDT', '15m', 'binance', 'crypto'))
    rows, _ = evaluation.load_closed(conn, 'BTCUSDT', '1h', 0, 7*900000-1)
    assert len(rows) == 1 and rows[0][0] == 0
    with pytest.raises(ValueError, match='holdout'):
        evaluation.load_closed(conn, 'BTCUSDT', '15m', 0, evaluation.HOLDOUT_START_MS)


def test_registry_and_runtime_registration_remain_separate():
    import json
    from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY
    registry = json.loads((PATH.parents[1]/'docs/research/cati_alpha_v1_registry.json').read_text())
    assert registry == evaluation.registry()
    assert not registry['runtime_eligible']
    assert not set(h.family for h in HYPOTHESES).intersection(SPECIALIST_REGISTRY)
    report_path = PATH.parents[1]/'docs/research/artifacts/cati_alpha_v1/report.json'
    report = json.loads(report_path.read_text())
    assert report['boundary']['HOLDOUT_QUERY_COUNT'] == 0
    assert all(r['NEXT_CATI_MODEL_TRAINING_ALLOWED'] == 'NO' for r in report['results'].values())


def test_published_labels_reproduce_every_fold_and_hash():
    import gzip
    import hashlib
    import json
    from app.trading_intelligence.hashing import stable_hash
    root = PATH.parents[1]/'docs/research/artifacts/cati_alpha_v1'
    report = json.loads((root/'report.json').read_text())
    # Git checks out CRLF on Windows; compare the frozen LF code bytes.
    assert report['evaluator_source_sha256'] == hashlib.sha256(PATH.read_bytes().replace(b'\r\n', b'\n')).hexdigest()
    generator = PATH.parents[1]/'backends/bot-backend/app/trading_intelligence/setups/alpha_v1.py'
    assert report['generator_source_sha256'] == hashlib.sha256(generator.read_bytes().replace(b'\r\n', b'\n')).hexdigest()
    raw = gzip.decompress((root/'labels.jsonl.gz').read_bytes())
    assert hashlib.sha256(raw).hexdigest() == report['labels_sha256']
    assert report['registry_hash'] == stable_hash(evaluation.registry())
    rows = [json.loads(line) for line in raw.splitlines()]
    assert len({r['label_id'] for r in rows}) == len(rows) == 1469
    for name, result in report['results'].items():
        subset = [r for r in rows if r['setup_version'] == name]
        assert evaluation.metrics(subset) == result['pooled']
        for i, fold in enumerate(result['folds'], 1):
            assert evaluation.metrics([r for r in subset if r['fold'] == i]) == fold
        assert not evaluation.viability(result['folds'], result['pooled'])
    for row in rows:
        step = 900000 if '_15m_' in row['setup_version'] else 3600000
        assert row['decision_time']+step*row['horizon'] < evaluation.HOLDOUT_START_MS
        assert row['net_R'] == pytest.approx(row['gross_R']-sum(row[k] for k in ('fee_R','spread_R','slippage_R','funding_R')))
