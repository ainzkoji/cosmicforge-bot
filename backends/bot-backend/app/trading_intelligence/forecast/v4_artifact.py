"""Governed research-only V4 loader and canonical forecast contract adapter."""
from __future__ import annotations
from dataclasses import dataclass
import json
from pathlib import Path
import numpy as np
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.forecast.artifact import LibraryArtifactError
from app.trading_intelligence.forecast.information_conditioning import causal_features
from app.trading_intelligence.forecast.v4_models import V4Probability,ConditionalPayoff,FEATURE_SCHEMA,probability_gate,payoff_gate
from app.trading_intelligence.contracts.forecast import OutcomeForecast


@dataclass(frozen=True)
class V4Library:
    library_hash:str
    library_id:str
    v4_probability:V4Probability
    payoff:ConditionalPayoff
    training_label_end:int
    training_rows:int
    payoff_validated:bool
    calibration_status:str='RESEARCH_ONLY'
    library_version:str='4.0.0-research-edge-and-payoff'
    source_kind:str='REAL_MARKET'

    def forecast(self,candidate,dims,context):
        if candidate.decision_time<=self.training_label_end or candidate.timeframe!='15m':
            raise ValueError('V4 model unavailable at decision time/domain')
        if (not context or context.get('schema')!='closed-candle-context-v4-1'
            or context.get('instrument')!=candidate.instrument_key.venue_symbol
            or context.get('decision_time')!=candidate.decision_time
            or len(context.get('source_close_times',[]))!=3
            or any(t!=candidate.decision_time for t in context['source_close_times'])):
            raise ValueError('actual exactly aligned V4 context required')
        _,g=causal_features(dimensions=dims,room=candidate.room_to_target_R,
            risk_fraction=candidate.initial_structural_risk/candidate.trigger_reference,
            timeframe=candidate.timeframe,horizon=48,instrument=candidate.instrument_key.venue_symbol)
        c=np.asarray(context['values'],dtype=float).reshape(1,-1)
        cats=np.asarray([[dims[k] for k in ('setup_family','side','dominant_regime','volatility_bucket')]+[candidate.instrument_key.venue_symbol]])
        p=self.v4_probability.predict(g[None,:],c,cats)
        payoff=self.payoff.predict(g[None,:],c,cats,p)
        terminal=payoff['terminal'][0]
        version='4.0.0-research-edge-and-payoff'
        signature=stable_hash(dict(dimensions=dims,context=context,geometry=g.tolist()))
        return OutcomeForecast(forecast_id=OutcomeForecast.build_id(setup_candidate_id=candidate.setup_candidate_id,
            library_hash=self.library_hash,forecast_version=version,cohort_signature=signature),
            setup_candidate_id=candidate.setup_candidate_id,market_state_id=candidate.market_state_id,
            forecast_version=version,library_version=self.library_version,library_hash=self.library_hash,
            cohort_signature=signature,backoff_level=0,raw_support=self.training_rows,ess=float(self.training_rows),
            p_net_profitable_mean=float(p[0]),credible_interval_low=0.,credible_interval_high=1.,credible_interval_level=.9,
            p_target_before_stop=float(terminal[0]),p_stop_before_target=float(terminal[1]),p_timeout=float(terminal[2]),
            gross_R_mean=float(payoff['expected_gross_R'][0]),net_R_reference_mean=float(payoff['expected_net_R'][0]),
            expected_net_R=float(payoff['expected_net_R'][0]),conditional_positive_net_R=float(payoff['conditional_positive_net_R'][0]),
            conditional_loss_net_R=float(payoff['conditional_loss_net_R'][0]),
            e_r_given_target=candidate.room_to_target_R,e_r_given_stop=-1.,e_r_given_timeout=float(payoff['timeout_gross_R'][0]),
            mfe_R_quantiles={str(q):float(payoff['mfe'][0,j]) for j,q in enumerate((.1,.5,.9))},
            mae_R_quantiles={str(q):float(payoff['mae'][0,j]) for j,q in enumerate((.1,.5,.9))},
            forecast_uncertainty=1.,reason_codes=('V4_RESEARCH_ONLY','V4_TIME_DISTRIBUTION_UNMODELED','V4_JOINT_PAYOFF_NOT_VALIDATED',
                'V4_PAYOFF_DEVELOPMENT_VALIDATED' if self.payoff_validated else 'V4_PAYOFF_VALIDATION_FAILED'),calibration_status='RESEARCH_ONLY')


def load_v4_artifact(path,*,expected_hash=None,mode='RUNTIME'):
    try:
        d=json.loads((Path(path)/'v4_model.json').read_text(encoding='utf-8'))
        if stable_hash({k:v for k,v in d.items() if k not in ('library_hash','candidate_id')})!=d['library_hash']:
            raise ValueError('V4 artifact identity mismatch')
        if d['candidate_id']!='cati_v4_'+d['library_hash'][:24] or (expected_hash is not None and expected_hash!=d['library_hash']):
            raise ValueError('unknown V4 candidate identity')
        registry=json.loads((Path(__file__).resolve().parents[5]/'docs/research/cati_v4_research_registry.json').read_text())
        if d['registry_hash']!=stable_hash(registry) or d['role']!=registry['role']: raise ValueError('unregistered V4 candidate')
        if d['feature_schema']!=FEATURE_SCHEMA: raise ValueError('wrong V4 feature schema')
        if any(d[k]!=registry[k] for k in ('parent_library_hash','dataset_manifest_hash')): raise ValueError('wrong V4 dataset')
        if d['source_tree_dirty'] is not False or len(d['code_revision'])!=40 or any(c not in '0123456789abcdef' for c in d['code_revision']):
            raise ValueError('dirty/invalid V4 provenance')
        if d['training_label_end']>=d['holdout_start_ms'] or d['holdout_query_count']!=0: raise ValueError('V4 holdout violation')
        if d['runtime_eligible'] is not False or d['calibration_status']!='RESEARCH_ONLY': raise ValueError('V4 research artifact authority claim')
        passed=probability_gate(d['metrics'],d['folds'],registry['probability_gate'])
        paypassed=payoff_gate(d['payoff_validation'],[f['payoff'] for f in d['folds']],registry['payoff']['gate'])
        if (d['probability_gate_pass']!=passed or d['payoff_gate_pass']!=paypassed
            or d['model_ready']!=bool(passed and paypassed)
            or d['development_status']!=('DEVELOPMENT_GATE_PASS' if passed else 'REJECTED_PRE_HOLDOUT')):
            raise ValueError('failed V4 calibration/payoff verdict integrity')
        # No development calibration or payoff verdict can grant runtime authority.
        if mode!='DEVELOPMENT': raise ValueError('V4 RESEARCH_ONLY; runtime remains closed pending governance')
        probability=V4Probability.from_dict(d['probability_model']); payoff=ConditionalPayoff.from_dict(d['payoff_model'])
        if probability.mode not in registry['families'] or probability.C not in registry['C_grid']:
            raise ValueError('unregistered V4 parameters')
    except (OSError,ValueError,KeyError,TypeError,IndexError) as exc:
        raise LibraryArtifactError(str(exc)) from exc
    return V4Library(d['library_hash'],d['candidate_id'],probability,payoff,d['training_label_end'],d['training_rows'],d['payoff_gate_pass']),{**d,'market_type':'crypto'}
