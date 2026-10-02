"""Independent score arithmetic and parity against the unmodified forecast engine."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from _helpers import build_library
from app.trading_intelligence.forecast.engine import forecast_from_dimensions, _rows_matching, _family_rows
from app.trading_intelligence.forecast.cohorts import BACKOFF_LEVELS
from app.trading_intelligence.forecast.calibration_report import (
    CalibrationPolicy, CausalCalibrationPolicy, build_calibration_record,
    status_from_stored_record, _CandidateStub, _TrainView, evaluate_library_calibration,
)

spec=importlib.util.spec_from_file_location('audit',Path(__file__).resolve().parents[4]/'scripts/cati_calibration_diagnostics.py')
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)

def counts(rows):
    return np.array([len(rows),sum(r.label.net_profitable for r in rows),
                     *[sum(r.label.terminal_outcome==c for r in rows) for c in audit.CLASSES]],dtype=float)

def test_brier_fixture_independent_arithmetic():
    rr=[{'p':p,'y':y,'baseline_v1':.5} for p,y in [(.1,0),(.9,1),(.6,1),(.2,0)]]
    result=audit.metrics(rr)
    assert result['model_brier']==pytest.approx(.055)
    assert result['baseline_brier']==.25
    assert result['brier_skill']==pytest.approx(.78)

@pytest.mark.parametrize('family',['TREND_PULLBACK_V2','RANGE_MEAN_REVERSION_V2'])
@pytest.mark.parametrize('n,win_fraction',[(40,.2),(80,.7),(140,.95)])
def test_scored_probability_parity_with_existing_engine(family,n,win_fraction):
    lib=build_library(n_rows=n,win_fraction=win_fraction,family=family)
    candidate=_CandidateStub(lib.rows[-1]);dims=lib.rows[-1].cohort_dimensions
    for forced_level,level_dims in enumerate(BACKOFF_LEVELS):
        # Make omitted dimensions novel so each reachable backoff is exercised.
        dd=dict(dims)
        for k in BACKOFF_LEVELS[0]:
            if k not in level_dims:dd[k]='NOVEL'
        fc=forecast_from_dimensions(candidate,dd,lib)
        selected=_rows_matching(lib,dd,BACKOFF_LEVELS[fc.backoff_level])
        p,multi=audit.score_counts(counts(selected),counts(_family_rows(lib,family)))
        assert p==fc.p_net_profitable_mean
        assert multi['TARGET_BEFORE_STOP']==fc.p_target_before_stop
        assert multi['STOP_BEFORE_TARGET']==fc.p_stop_before_target
        assert multi['TIMEOUT']==fc.p_timeout

def test_walk_forward_legacy_metric_parity():
    lib=build_library(n_rows=120,win_fraction=.6)
    policy=CalibrationPolicy(max_eval_rows=50,min_train_rows=10)
    embargo=10*900000
    reference=evaluate_library_calibration(lib,embargo_ms=embargo,policy=policy)
    rows=sorted(lib.rows,key=lambda r:(r.label.decision_time,r.label.label_id))
    stride=max(1,-(-len(rows)//policy.max_eval_rows));trace=[]
    for j in range(0,len(rows),stride):
        test=rows[j];train=[r for r in rows[:j] if r.label.decision_time+embargo<=test.label.decision_time]
        if len(train)<policy.min_train_rows:continue
        fc=forecast_from_dimensions(_CandidateStub(test),test.cohort_dimensions,_TrainView(train))
        if fc.status!='VALID':continue
        local=_rows_matching(_TrainView(train),test.cohort_dimensions,BACKOFF_LEVELS[fc.backoff_level])
        p,_=audit.score_counts(counts(local),counts(_family_rows(_TrainView(train),test.label.setup_family)))
        trace.append({'p':p,'y':int(test.label.net_profitable)})
    base=sum(r['y'] for r in trace)/len(trace)
    for r in trace:r['baseline_v1']=base
    result=audit.metrics(trace)
    assert result['samples']==reference.n_evaluated
    assert result['model_brier']==reference.brier_score
    assert result['baseline_brier']==reference.brier_baseline
    assert result['brier_skill']==reference.brier_skill
    assert result['ece']==reference.ece

def test_v2_baseline_uses_only_matured_training_labels():
    lib=build_library(n_rows=120,win_fraction=.6)
    policy=CausalCalibrationPolicy(max_eval_rows=50,min_train_rows=10)
    embargo=10*900000
    report=evaluate_library_calibration(lib,embargo_ms=embargo,policy=policy)
    rows=sorted(lib.rows,key=lambda r:(r.label.decision_time,r.label.label_id))
    errors=[];stride=max(1,-(-len(rows)//policy.max_eval_rows))
    for j in range(0,len(rows),stride):
        test=rows[j];train=[r for r in rows[:j] if r.label.decision_time+embargo<=test.label.decision_time]
        if len(train)<policy.min_train_rows:continue
        fc=forecast_from_dimensions(_CandidateStub(test),test.cohort_dimensions,_TrainView(train))
        if fc.status!='VALID':continue
        p=sum(r.label.net_profitable for r in train)/len(train)
        errors.append((p-int(test.label.net_profitable))**2)
    assert report.brier_baseline==sum(errors)/len(errors)
    assert report.brier_skill==1-report.brier_score/report.brier_baseline
    stored=build_calibration_record(report,policy)
    assert status_from_stored_record(stored,library_hash=lib.library_hash)==stored['status']

def test_v1_policy_and_gates_remain_frozen():
    assert CalibrationPolicy().policy_hash=='6c89935031969767874c8a18d351c9d324086fed6a5e349475d7a1554b74ff14'
    assert CausalCalibrationPolicy().min_brier_skill==CalibrationPolicy().min_brier_skill==.02
    assert CausalCalibrationPolicy().max_ece==CalibrationPolicy().max_ece==.05
    assert CausalCalibrationPolicy().min_calibrated_samples==CalibrationPolicy().min_calibrated_samples==300
    assert CausalCalibrationPolicy().policy_hash!=CalibrationPolicy().policy_hash
