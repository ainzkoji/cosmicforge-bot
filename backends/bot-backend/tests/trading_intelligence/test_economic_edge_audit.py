"""Audit arithmetic/selection checks; no fitted models or production rules."""
import importlib.util
import json
from pathlib import Path
import numpy as np
import pytest

PATH=Path(__file__).resolve().parents[4]/'scripts/audit_cati_economic_edge.py'
spec=importlib.util.spec_from_file_location('edge_audit',PATH)
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)


def test_fixed_tail_uses_ceiling_and_stable_ties_without_outcome_selection():
    score=np.array([.4,.5,.5,-1.])
    np.testing.assert_array_equal(audit.fixed_tail(score,.30),[1,2])
    np.testing.assert_array_equal(audit.fixed_tail(score,.005),[1])
    assert audit.fixed_tail(np.array([]),.01).size==0
    with pytest.raises(ValueError):audit.fixed_tail(np.array([np.nan]),.1)


def test_cost_scenarios_preserve_frozen_net_and_terminal_labels():
    gross=np.array([2.,-1.,.1]);cost=np.array([.2,.3,.4]);net=gross-cost
    terminal=np.array([0,1,2]);before=net.copy()
    result=audit.summary(gross,cost,net,terminal)
    assert result['mean_net_R']==pytest.approx(gross.mean()-cost.mean())
    assert result['profitable_rate']==pytest.approx(1/3)
    assert result['TARGET_mean_net_R']==1.8
    assert (gross-2*cost).mean()==pytest.approx(net.mean()-cost.mean())
    np.testing.assert_array_equal(net,before)
    assert audit.summary(gross[:0],cost[:0],net[:0],terminal[:0])['mean_net_R'] is None


def test_population_bins_retain_ties_and_empty_cells_instead_of_optimizing():
    np.testing.assert_array_equal(audit.fixed_population_bins([0,1,1,2],[1,1]),[0,2,2,2])


def test_rank_report_exposes_reversals_instead_of_selecting_best_tail():
    r=audit.rank_report(np.arange(20.),np.tile([0.,2.,1.,3.],5))
    assert len(r['deciles'])==10 and len(r['quintiles'])==5
    assert not r['deciles_monotonic']
    assert r['deciles_ordering_breaks']


def test_daily_cluster_interval_handles_many_simultaneous_candidates():
    r=audit.clustered_mean_interval(np.array([1.,1.,-1.,-1.]),np.array([0,0,86400000,86400000]))
    assert r['utc_day_clusters']==2
    assert r['clustered_standard_error_R']==pytest.approx(1.)


def test_full_parent_raw_verifies_identity_and_denies_holdout_overlap(tmp_path):
    from app.trading_intelligence.hashing import TextHasher
    label=dict(decision_time=100000000,terminal_horizon_bars=48,terminal_outcome='STOP_BEFORE_TARGET',
               gross_R=-1.,total_cost_R=.2,net_R=-1.2,fee_R=.1,spread_R=.02,slippage_R=.06,funding_R=.02,carry_R=0.)
    text=json.dumps({'label':label})+'\n';h=TextHasher();h.update(text)
    (tmp_path/'rows.jsonl').write_bytes(text.encode('utf-8'))
    (tmp_path/'manifest.json').write_text(json.dumps(dict(row_count=1,rows_sha256=h.hexdigest())))
    meta=dict(start=0,stop=200000000,holdout_start_ms=300000000)
    r=audit.full_parent_raw(tmp_path,meta)
    assert r['FULL_PARENT']['samples']==1 and r['FULL_PARENT']['mean_net_R']==-1.2
    meta['holdout_start_ms']=100000001
    with pytest.raises(ValueError,match='holdout'):audit.full_parent_raw(tmp_path,meta)


def test_published_audit_retains_every_fixed_cell_and_conserves_returns():
    path=PATH.parents[1]/'docs/research/artifacts/cati_v5_economic_edge_audit/audit.json'
    assert path.exists(), 'Published audit evidence must ship with diagnostic tooling'
    r=json.loads(path.read_text());e=r['outer_economics'];pool=e['POOLED']
    assert len(r['fixed_tails'])==42 and len(r['fixed_expectancy_thresholds'])==24 and len(r['fixed_probability_thresholds'])==24
    assert len(r['geometry_economics'])==180 and len(r['event_time_economics'])==126
    assert r['boundary']['fitted_models']==0 and r['boundary']['holdout_query_count']==0
    assert r['DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED'] is True
    assert sum(e['FOLD_'+str(i)]['samples'] for i in range(1,6))==pool['samples']==51887
    for v in e.values():
        assert v['mean_gross_R']-v['mean_cost_R']==pytest.approx(v['mean_net_R'],abs=1e-12)
        assert sum(v['cost_components'].values())==pytest.approx(v['mean_cost_R'],abs=1e-12)
    for row in r['fixed_tails']:
        assert row['samples']==int(np.ceil(e[row['population']]['samples']*row['fraction']))
    for v in r['full_frozen_parent_raw_economics'].values():
        assert v['mean_gross_R']-v['mean_cost_R']==pytest.approx(v['mean_net_R'],abs=1e-12)
    assert r['full_frozen_parent_raw_economics']['FULL_PARENT']['samples']==3007222
    for population in ['POOLED']+['FOLD_'+str(i) for i in range(1,6)]:
        for dimension in audit.GROUPS:
            assert sum(x['samples'] for x in r['setup_group_economics'] if x['population']==population and x['dimension']==dimension)==e[population]['samples']
