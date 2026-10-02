"""V5 numeric artifact loader: development only, never grants runtime authority."""
from __future__ import annotations
from dataclasses import dataclass
import gzip,json
from pathlib import Path
import numpy as np
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.contracts.forecast import OutcomeForecast
from .artifact import LibraryArtifactError
from .information_conditioning import causal_features
from .v4_models import probability_gate,payoff_gate
from .v5_models import JointProbability,ConditionalAtoms,FEATURE_SCHEMA,STATES,TERMINAL,coherence_pass,decision_gate


@dataclass(frozen=True)
class V5Library:
    library_hash:str
    library_id:str
    v5_joint_probability:JointProbability
    distribution:ConditionalAtoms
    training_label_end:int
    training_rows:int
    decision_payoff_ready:bool
    calibration_status:str='RESEARCH_ONLY'
    library_version:str='5.0.0-research-coherent-joint'
    source_kind:str='REAL_MARKET'

    def forecast(self,candidate,dims,context):
        if candidate.decision_time<=self.training_label_end or candidate.timeframe!='15m': raise ValueError('V5 unavailable at decision time/domain')
        if (not context or context.get('schema')!='closed-candle-context-v4-1'
            or context.get('instrument')!=candidate.instrument_key.venue_symbol or context.get('decision_time')!=candidate.decision_time
            or len(context.get('source_close_times',[]))!=3 or any(t!=candidate.decision_time for t in context['source_close_times'])):
            raise ValueError('actual exactly aligned V5 context required')
        _,g=causal_features(dimensions=dims,room=candidate.room_to_target_R,risk_fraction=candidate.initial_structural_risk/candidate.trigger_reference,
            timeframe=candidate.timeframe,horizon=48,instrument=candidate.instrument_key.venue_symbol)
        c=np.asarray(context['values'],dtype=float).reshape(1,-1)
        cats=np.asarray([[dims[k] for k in ('setup_family','side','dominant_regime','volatility_bucket')]+[candidate.instrument_key.venue_symbol]])
        joint=self.v5_joint_probability.predict(g[None,:],c,cats); pred=self.distribution.predict(g[None,:],c,cats,joint)
        terminal=pred['terminal'][0]; timing={}
        for term,name in [(0,'target'),(1,'stop')]:
            if terminal[term]==0:
                timing[name]={}; continue  # Conditional time is undefined for a zero-mass event.
            cdf=np.cumsum(pred['joint_time_pmf'][0,TERMINAL==term,:].sum(axis=0)/terminal[term])
            # Canonical first-touch quantiles retain frozen zero-based index units.
            timing[name]={str(q):float(np.argmax(cdf>=q)) for q in (.1,.5,.9)}
        timeout_gross=float(np.sum(joint[0,3:]*pred['state_gross_R_mean'][0,3:])/terminal[2]) if terminal[2]>0 else None
        signature=stable_hash(dict(dimensions=dims,context=context,geometry=g.tolist()))
        return OutcomeForecast(forecast_id=OutcomeForecast.build_id(setup_candidate_id=candidate.setup_candidate_id,
            library_hash=self.library_hash,forecast_version=self.library_version,cohort_signature=signature),setup_candidate_id=candidate.setup_candidate_id,
            market_state_id=candidate.market_state_id,forecast_version=self.library_version,library_version=self.library_version,library_hash=self.library_hash,
            cohort_signature=signature,backoff_level=0,raw_support=self.training_rows,ess=float(self.training_rows),
            p_net_profitable_mean=float(pred['p'][0]),credible_interval_low=0.,credible_interval_high=1.,credible_interval_level=.9,
            p_target_before_stop=float(terminal[0]),p_stop_before_target=float(terminal[1]),p_timeout=float(terminal[2]),
            gross_R_mean=float(pred['expected_gross_R'][0]),net_R_reference_mean=float(pred['expected_net_R'][0]),expected_net_R=float(pred['expected_net_R'][0]),
            conditional_positive_net_R=float(pred['conditional_positive_net_R'][0]),conditional_loss_net_R=float(pred['conditional_loss_net_R'][0]),
            joint_outcome_probabilities={s:float(joint[0,j]) for j,s in enumerate(STATES)},
            joint_state_net_R_means={s:float(pred['state_net_R_mean'][0,j]) for j,s in enumerate(STATES)},
            joint_event_time_probabilities={s:tuple(pred['joint_time_pmf'][0,j].tolist()) for j,s in enumerate(STATES)},event_time_unit='ELAPSED_BARS_1_TO_48',
            e_r_given_target=candidate.room_to_target_R,e_r_given_stop=-1.,e_r_given_timeout=timeout_gross,
            mfe_R_quantiles={str(q):float(pred['mfe'][0,j]) for j,q in enumerate((.1,.5,.9))},
            mae_R_quantiles={str(q):float(pred['mae'][0,j]) for j,q in enumerate((.1,.5,.9))},
            time_to_target_quantiles=timing['target'],time_to_stop_quantiles=timing['stop'],forecast_uncertainty=1.,
            reason_codes=('V5_RESEARCH_ONLY','V5_COHERENT_JOINT_DISTRIBUTION','V5_TIMEOUT_ADMINISTRATIVE_CENSORING',
                'V5_DECISION_PAYOFF_READY' if self.decision_payoff_ready else 'V5_DECISION_PAYOFF_NOT_READY'),calibration_status='RESEARCH_ONLY')


def load_v5_artifact(path,*,expected_hash=None,mode='RUNTIME'):
    try:
        root=Path(path)
        if (root/'v5_model.json').is_file(): d=json.loads((root/'v5_model.json').read_text(encoding='utf-8'))
        else:
            with gzip.open(root/'v5_model.json.gz','rt',encoding='utf-8') as f: d=json.load(f)
        if stable_hash({k:v for k,v in d.items() if k not in ('library_hash','candidate_id')})!=d['library_hash']: raise ValueError('V5 artifact identity mismatch')
        if d['candidate_id']!='cati_v5_'+d['library_hash'][:24] or (expected_hash is not None and expected_hash!=d['library_hash']): raise ValueError('unknown V5 identity')
        registry=json.loads((Path(__file__).resolve().parents[5]/'docs/research/cati_v5_research_registry.json').read_text())
        if d['registry_hash']!=stable_hash(registry) or d['role']!=registry['role']: raise ValueError('unregistered V5 candidate')
        if d['feature_schema']!=FEATURE_SCHEMA or tuple(d['joint_states'])!=STATES: raise ValueError('V5 schema/state mismatch')
        if any(d[k]!=registry[k] for k in ('parent_library_hash','dataset_manifest_hash')): raise ValueError('wrong V5 dataset')
        if d['source_tree_dirty'] is not False or len(d['code_revision'])!=40 or any(c not in '0123456789abcdef' for c in d['code_revision']): raise ValueError('dirty/invalid V5 provenance')
        if d['training_label_end']>=d['holdout_start_ms'] or d['holdout_query_count']!=0: raise ValueError('V5 holdout violation')
        if d['holdout_start_ms']!=1783876499999: raise ValueError('changed holdout boundary')
        if d['runtime_eligible'] is not False or d['calibration_status']!='RESEARCH_ONLY': raise ValueError('V5 authority claim')
        m=d['metrics']; coherent=coherence_pass(m['coherence']) and all(coherence_pass(f['coherence']) for f in d['folds'])
        model_ready=probability_gate(m,d['folds'],registry['probability_gate']) and coherent
        paypassed=payoff_gate(m['payoff'],[f['payoff'] for f in d['folds']],registry['payoff_gate'])
        decision=decision_gate(m['payoff'],d['folds'],m['time_validation'],registry,coherent)
        if (d['coherence_pass']!=coherent or d['model_ready']!=model_ready or d['decision_payoff_ready']!=decision or d['payoff_gate_pass']!=paypassed
            or d['development_status']!=('DEVELOPMENT_GATE_PASS' if model_ready else 'REJECTED_PRE_HOLDOUT')): raise ValueError('V5 readiness verdict integrity')
        if mode!='DEVELOPMENT': raise ValueError('V5 RESEARCH_ONLY; runtime remains closed pending governance')
        if d['probability_model']['spec'] not in registry['variants']: raise ValueError('unregistered V5 variant')
        probability=JointProbability.from_dict(d['probability_model']); distribution=ConditionalAtoms.from_dict(d['conditional_model'])
    except (OSError,ValueError,KeyError,TypeError,IndexError) as exc:
        raise LibraryArtifactError(str(exc)) from exc
    return V5Library(d['library_hash'],d['candidate_id'],probability,distribution,d['training_label_end'],d['training_rows'],decision),{**d,'market_type':'crypto'}
