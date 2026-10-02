"""One bounded coherent joint distribution; only fitted numeric data is serialized."""
from __future__ import annotations
import gc,warnings
import numpy as np
from scipy.special import softmax
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.exceptions import ConvergenceWarning
from .v4_models import FeatureEncoder,NumericTrees,MODES

FEATURE_SCHEMA='cati-v5-joint-closed-context-geometry-1'
STATES=('TARGET_PROFIT','TARGET_LOSS','STOP_LOSS','TIMEOUT_PROFIT','TIMEOUT_LOSS')
PROFIT=np.array([True,False,False,True,False])
TERMINAL=np.array([0,0,1,2,2])


def compact_categories(categories):
    """Shared Python string references avoid copying fixed-width Unicode prefixes."""
    raw=np.asarray(categories); result=np.empty(raw.shape,dtype=object)
    for j in range(raw.shape[1]):
        keys=np.unique(raw[:,j]); positions=np.searchsorted(keys,raw[:,j])
        result[:,j]=keys.astype(object)[positions]
    return result


def joint_labels(targets):
    t=np.asarray(targets); terminal=t[:,5].astype(int); positive=t[:,0].astype(bool)
    if (not np.all(np.isfinite(t)) or not np.all(np.isin(terminal,[0,1,2]))
        or np.any(positive!=(t[:,1]>0)) or np.any((terminal==1)&positive)):
        raise ValueError('frozen joint label semantics violated')
    return np.where(terminal==0,np.where(positive,0,1),np.where(terminal==1,2,np.where(positive,3,4)))


def latent_labels(targets):
    state=joint_labels(targets)
    return np.choose(state,[0,0,1,2,3])


def recency_weights(times,cutoff,half_life_days):
    times=np.asarray(times,dtype=np.int64)
    if np.any(times>=cutoff): raise ValueError('future training timestamp')
    if half_life_days is None: return np.ones(len(times))
    if half_life_days not in (180,365): raise ValueError('unregistered recency policy')
    weights=np.exp2(-(cutoff-times)/(half_life_days*86400000.))
    return weights/weights.mean()


def joint_from_logits(logits,geometry):
    """Support-aware softmax; known costs determine target profit/loss exactly."""
    g=np.asarray(geometry,dtype=float); room=np.exp(g[:,0]); cost=np.expm1(g[:,2])
    if np.any(cost<0) or not np.all(np.isfinite(g)): raise ValueError('invalid frozen geometry')
    z=np.array(logits,dtype=float,copy=True)
    if z.shape!=(len(g),4) or not np.all(np.isfinite(z)): raise ValueError('invalid joint logits')
    profitable_target=room>cost
    z[~profitable_target,2]=-np.inf  # No profitable timeout in this known price/cost support.
    latent=softmax(z,axis=1)
    result=np.zeros((len(g),5))
    result[:,0]=latent[:,0]*profitable_target
    result[:,1]=latent[:,0]*(~profitable_target)
    result[:,2:]=latent[:,1:]
    return result


class JointProbability:
    def __init__(self,spec): self.spec=dict(spec)

    def fit(self,g,c,cats,targets,times,cutoff):
        gc.collect(); self.encoder=FeatureEncoder(MODES[3]).fit(g,c,cats)
        x=self.encoder.transform(g,c,cats); y=latent_labels(targets)
        if set(y)!={0,1,2,3}: raise ValueError('missing latent state in training prefix')
        weights=recency_weights(times,cutoff,self.spec['half_life_days'])
        if self.spec['architecture']=='JOINT_HISTOGRAM_BOOSTING':
            x=x.toarray().astype(np.float32)
            model=HistGradientBoostingClassifier(max_iter=60,max_leaf_nodes=15,min_samples_leaf=400,
                l2_regularization=20.,learning_rate=.1,max_bins=128,early_stopping=False,random_state=0).fit(x,y,sample_weight=weights)
            self.trees=[]
            for j in range(4):
                tree=NumericTrees(); tree.base=float(model._baseline_prediction[0,j]); tree.width=x.shape[1]
                tree.trees=[]
                for stage in model._predictors:
                    nodes=stage[j].nodes
                    if np.any(nodes['is_categorical']): raise ValueError('categorical tree export unsupported')
                    tree.trees.append([{k:float(n[k]) if k in ('value','num_threshold') else int(n[k]) for k in
                        ('value','num_threshold','feature_idx','left','right','is_leaf','missing_go_to_left')} for n in nodes])
                self.trees.append(tree)
            # Exact sklearn numeric replay before structural support mapping.
            for start in range(0,len(x),2048):
                xx=x[start:start+2048]
                np.testing.assert_allclose(softmax(np.column_stack([m.predict(xx) for m in self.trees]),axis=1),
                    model.predict_proba(xx),rtol=1e-11,atol=1e-11)
        else:
            model=LogisticRegression(C=self.spec['C'],max_iter=2000,tol=1e-6,random_state=0)
            with warnings.catch_warnings():
                warnings.simplefilter('error',ConvergenceWarning); model.fit(x,y,sample_weight=weights)
            self.coefficients=model.coef_.copy(); self.intercept=model.intercept_.copy()
        del model,x; gc.collect(); return self

    def predict(self,g,c,cats):
        x=self.encoder.transform(g,c,cats)
        if hasattr(self,'trees'):
            x=x.toarray().astype(np.float32); logits=np.column_stack([m.predict(x) for m in self.trees])
        else: logits=x@self.coefficients.T+self.intercept
        return joint_from_logits(logits,g)

    def to_dict(self):
        d=dict(schema=FEATURE_SCHEMA,spec=self.spec,encoder=self.encoder.to_dict())
        if hasattr(self,'trees'): d['trees']=[t.to_dict() for t in self.trees]
        else: d.update(coefficients=self.coefficients.tolist(),intercept=self.intercept.tolist())
        return d

    @classmethod
    def from_dict(cls,d):
        if d['schema']!=FEATURE_SCHEMA: raise ValueError('V5 schema mismatch')
        obj=cls(d['spec']); obj.encoder=FeatureEncoder.from_dict(d['encoder'])
        if obj.encoder.mode!=MODES[3]: raise ValueError('wrong joint encoder')
        width=len(obj.encoder.mean)+sum(map(len,obj.encoder.vocabularies))+4*len(obj.encoder.families)
        if obj.spec['architecture']=='JOINT_HISTOGRAM_BOOSTING':
            obj.trees=[NumericTrees.from_dict(t) for t in d['trees']]
            if len(obj.trees)!=4 or any(t.width!=width for t in obj.trees): raise ValueError('joint tree width')
        else:
            obj.coefficients=np.asarray(d['coefficients'],dtype=float); obj.intercept=np.asarray(d['intercept'],dtype=float)
            if (obj.coefficients.shape!=(4,width) or obj.intercept.shape!=(4,)
                or not np.all(np.isfinite(obj.coefficients)) or not np.all(np.isfinite(obj.intercept))): raise ValueError('joint coefficient shape')
        return obj


class ConditionalAtoms:
    """Bounded state-conditioned payoff/path/time distributions, never independent marginals."""
    def fit(self,g,c,cats,targets,event_bars):
        self.generation_policy='CAUSAL_TERMINAL_SUPPORT_V5_3'
        gc.collect(); self.encoder=FeatureEncoder(MODES[3]).fit(g,c,cats)
        x=self.encoder.transform(g,c,cats).toarray().astype(np.float32)
        states=joint_labels(targets); self.models={}; self.event_pmf=[]; self.support=[]
        if np.any((event_bars<1)|(event_bars>48)): raise ValueError('event support')
        levels=(np.arange(101)+.5)/101
        for state in range(5):
            rows=states==state; n=int(rows.sum()); self.support.append(n)
            hist=np.bincount(event_bars[rows].astype(int)-1,minlength=48).astype(float)+.5
            if state in (3,4): hist[:]=0.; hist[-1]=1.  # Administrative censoring at horizon.
            self.event_pmf.append((hist/hist.sum()).tolist())
            self.models[str(state)]={}
            for name,j in [('net',1),('mfe',3),('mae',4)]:
                if name=='net' and state<3: continue  # Frozen target/stop payoff is known at forecast.
                values=abs(targets[rows,j]) if name=='net' else np.maximum(targets[rows,j],0.)
                if n>=400:
                    tree=NumericTrees.fit(x[rows],values); means=np.maximum(tree.predict(x[rows]),1e-9)
                    ratios=np.quantile(values/means,levels); average=ratios.mean()
                    ratios=ratios/average if average>0 else np.ones(101)
                    record=dict(tree=tree.to_dict(),ratios=ratios.tolist())
                else:
                    # Rare TARGET_LOSS and any absent state retain bounded empirical
                    # conditional support. Zero support is declared, not fabricated evidence.
                    atoms=np.quantile(values,levels) if n else np.zeros(101)
                    record=dict(atoms=atoms.tolist())
                self.models[str(state)][name]=record
        del x; gc.collect(); return self

    def _atoms(self,state,name,x):
        record=self.models[str(state)][name]
        if 'atoms' in record: return np.broadcast_to(record['atoms'],(len(x),101)).copy()
        if not hasattr(self,'_tree_cache'): self._tree_cache={}
        key=(state,name)
        if key not in self._tree_cache: self._tree_cache[key]=NumericTrees.from_dict(record['tree'])
        mean=np.maximum(self._tree_cache[key].predict(x),0.)
        return mean[:,None]*np.asarray(record['ratios'])[None,:]

    @staticmethod
    def mixture_quantiles(atoms,joint):
        n=len(joint); flat=atoms.reshape(n,-1); weights=np.repeat(joint/101,101,axis=1)
        order=np.argsort(flat,axis=1,kind='stable'); values=np.take_along_axis(flat,order,axis=1)
        cdf=np.cumsum(np.take_along_axis(weights,order,axis=1),axis=1)
        return np.column_stack([values[np.arange(n),np.argmax(cdf>=q,axis=1)] for q in (.1,.5,.9)])

    def predict(self,g,c,cats,joint):
        if joint.shape!=(len(g),5) or np.any(joint<0) or np.max(abs(joint.sum(axis=1)-1))>1e-12:
            raise ValueError('invalid joint distribution')
        x=self.encoder.transform(g,c,cats).toarray().astype(np.float32)
        room=np.exp(g[:,0]); cost=np.expm1(g[:,2]); target=room-cost
        if (np.any((joint[:,0]>0)&(target<=0)) or np.any((joint[:,1]>0)&(target>0))
            or np.any((joint[:,3]>0)&(target<=0))): raise ValueError('joint probability outside causal state support')
        net=np.empty((len(g),5,101))
        net[:,0,:]=np.maximum(target,0.)[:,None]; net[:,1,:]=np.minimum(target,0.)[:,None]; net[:,2,:]=(-1-cost)[:,None]
        net[:,3,:]=np.minimum(self._atoms(3,'net',x),np.maximum(target,0.)[:,None])
        magnitude=self._atoms(4,'net',x)
        if self.generation_policy in ('CAUSAL_TERMINAL_SUPPORT_V5_2','CAUSAL_TERMINAL_SUPPORT_V5_3'):
            magnitude=np.maximum(magnitude,np.maximum(cost-room,0.)[:,None])
        net[:,4,:]=-np.minimum(magnitude,(1+cost)[:,None])
        if self.generation_policy=='CAUSAL_TERMINAL_SUPPORT_V5_3':
            upper=np.nextafter(target,-np.inf); lower=np.nextafter(-1-cost,np.inf)
            net[:,3,:]=np.minimum(net[:,3,:],np.maximum(upper,0.)[:,None])
            net[:,4,:]=np.maximum(np.minimum(net[:,4,:],np.minimum(upper,0.)[:,None]),lower[:,None])
        means=net.mean(axis=2); p=joint[:,PROFIT].sum(axis=1); terminal=np.column_stack([joint[:,TERMINAL==j].sum(axis=1) for j in range(3)])
        expected=np.sum(joint*means,axis=1)
        positive=np.divide(np.sum(joint[:,PROFIT]*means[:,PROFIT],axis=1),p,out=np.zeros(len(p)),where=p>0)
        loss=np.divide(np.sum(joint[:,~PROFIT]*means[:,~PROFIT],axis=1),1-p,out=np.zeros(len(p)),where=p<1)
        result=dict(joint=joint,p=p,terminal=terminal,expected_net_R=expected,expected_gross_R=expected+cost,
            conditional_positive_net_R=positive,conditional_loss_net_R=loss,state_net_R_mean=means,
            state_gross_R_mean=means+cost[:,None],net_R_quantiles=self.mixture_quantiles(net,joint))
        paths={}
        for name in ('mfe','mae'):
            atoms=np.stack([self._atoms(s,name,x) for s in range(5)],axis=1)
            # A common equiprobable atom index couples payoff and path draws.
            # Every generated path contains its terminal gross payoff and the
            # required first-touch boundary; these are causal support bounds.
            gross_atoms=net+cost[:,None,None]
            if self.generation_policy=='CAUSAL_TERMINAL_SUPPORT_V5_3':
                gross_atoms[:,3:,:]=np.clip(gross_atoms[:,3:,:],np.nextafter(-1.,0.),np.nextafter(room,0.)[:,None,None])
            floor=np.maximum(gross_atoms,0.) if name=='mfe' else np.maximum(-gross_atoms,0.)
            if name=='mfe': floor[:,:2,:]=np.maximum(floor[:,:2,:],room[:,None,None])
            else: floor[:,2,:]=np.maximum(floor[:,2,:],1.)
            atoms=np.maximum(atoms,floor)
            if self.generation_policy=='CAUSAL_TERMINAL_SUPPORT_V5_3':
                ceiling=np.nextafter(room,0.)[:,None,None] if name=='mfe' else np.nextafter(1.,0.)
                atoms[:,3:,:]=np.minimum(atoms[:,3:,:],ceiling)
            paths[name]=atoms
            result[name]=self.mixture_quantiles(atoms,joint)
        event=np.asarray(self.event_pmf)
        conditional_time=np.broadcast_to(event,(len(g),5,48)).copy()
        if self.generation_policy=='CAUSAL_TERMINAL_SUPPORT_V5_3':
            # TARGET followed by a full-horizon stop excursion must happen by
            # bar 47: simultaneous final-bar touches resolve conservatively STOP.
            for state in (0,1):
                fraction=np.mean(paths['mae'][:,state,:]>=1.,axis=1)
                earlier=event[state].copy(); earlier[-1]=0.; earlier/=earlier.sum()
                conditional_time[:,state,:]=(1-fraction[:,None])*event[state]+fraction[:,None]*earlier
        result['state_time_pmf']=conditional_time
        result['joint_time_pmf']=joint[:,:,None]*conditional_time
        return result

    def to_dict(self): return dict(encoder=self.encoder.to_dict(),models=self.models,event_pmf=self.event_pmf,support=self.support,generation_policy=self.generation_policy)

    @classmethod
    def from_dict(cls,d):
        obj=cls(); obj.encoder=FeatureEncoder.from_dict(d['encoder']); obj.models=d['models']; obj.event_pmf=d['event_pmf']; obj.support=d['support']
        obj.generation_policy=d.get('generation_policy','LEGACY_V5_1')
        width=len(obj.encoder.mean)+sum(map(len,obj.encoder.vocabularies))+4*len(obj.encoder.families)
        event=np.asarray(obj.event_pmf)
        if (obj.generation_policy not in ('LEGACY_V5_1','CAUSAL_TERMINAL_SUPPORT_V5_2','CAUSAL_TERMINAL_SUPPORT_V5_3') or obj.encoder.mode!=MODES[3] or event.shape!=(5,48) or np.any(event<0)
            or not np.all(np.isfinite(event)) or np.max(abs(event.sum(axis=1)-1))>1e-12
            or len(obj.support)!=5 or any(n<0 for n in obj.support)
            or set(obj.models)!=set(map(str,range(5)))
            or np.any(event[3:,:47]!=0) or np.any(event[3:,47]!=1)):
            raise ValueError('invalid joint conditional distribution')
        if np.any(event[:2,:47].sum(axis=1)<=0): raise ValueError('target time support unavailable')
        for s,records in obj.models.items():
            if set(records)!=({'mfe','mae'} if int(s)<3 else {'net','mfe','mae'}): raise ValueError('conditional state target mismatch')
            for record in records.values():
                values=np.asarray(record.get('atoms',record.get('ratios',[])))
                if values.shape!=(101,) or np.any(values<0) or not np.all(np.isfinite(values)): raise ValueError('invalid conditional atoms')
                if 'tree' in record and NumericTrees.from_dict(record['tree']).width!=width: raise ValueError('conditional tree width')
        return obj


def coherence_metrics(pred):
    joint=pred['joint']; p=pred['p']; means=pred['state_net_R_mean']; eps=1e-12
    return dict(max_probability_sum_error=float(np.max(abs(joint.sum(axis=1)-1))),
        negative_probability_count=int(np.sum(joint<0)),over_one_probability_count=int(np.sum(joint>1)),
        profit_stop_incoherence_count=int(np.sum(p+pred['terminal'][:,1]>1+eps)),
        max_profit_identity_error=float(np.max(abs(p-joint[:,PROFIT].sum(axis=1)))),
        max_terminal_identity_error=float(np.max(abs(pred['terminal']-np.column_stack([joint[:,TERMINAL==j].sum(axis=1) for j in range(3)])))),
        max_expected_R_identity_error=float(np.max(abs(pred['expected_net_R']-np.sum(joint*means,axis=1)))),
        max_conditional_R_identity_error=float(np.max(abs(pred['expected_net_R']-(p*pred['conditional_positive_net_R']+(1-p)*pred['conditional_loss_net_R'])))),
        state_payoff_sign_violation_count=int(np.sum(means[:,PROFIT]<0)+np.sum(means[:,~PROFIT]>0)))


def coherence_pass(m):
    required={'max_probability_sum_error','negative_probability_count','over_one_probability_count',
        'profit_stop_incoherence_count','max_profit_identity_error','max_terminal_identity_error',
        'max_expected_R_identity_error','max_conditional_R_identity_error','state_payoff_sign_violation_count'}
    return (set(m)==required and all(np.isfinite(v) and v>=0 and
        (v==0 if k.endswith('_count') else v<=1e-12) for k,v in m.items()))


def time_gate(m,g):
    return (m['conditional_MAE']<=m['causal_terminal_frequency_MAE']*g['time_conditional_MAE_relative_to_causal_terminal_frequency_maximum']
        and m['conditional_CRPS']<=m['causal_terminal_frequency_CRPS']*g['time_conditional_CRPS_relative_to_causal_terminal_frequency_maximum'])


def decision_gate(pay,folds,time_result,registry,coherent):
    from .v4_models import payoff_gate
    g=registry['decision_payoff_gate']
    return (coherent and payoff_gate(pay,[f['payoff'] for f in folds],registry['payoff_gate'])
        and pay['expected_R_buckets'][-1]['realized_mean']>=g['pooled_top_bucket_realized_R_minimum']
        and all(f['payoff']['maximum_expectancy_quintile_inversion_R']<=g['all_outer_maximum_adjacent_inversion_R']
            and f['payoff']['expected_R_buckets'][-1]['realized_mean']>=g['all_outer_top_bucket_realized_R_minimum']
            and f['payoff']['expected_R_buckets'][-1]['realized_mean']>f['payoff']['expected_R_buckets'][0]['realized_mean']
            and f['payoff']['maximum_bucket_absolute_bias']<=g['each_outer_maximum_bucket_absolute_bias'] for f in folds)
        and time_gate(time_result,g) and all(time_gate(f['time_validation'],g) for f in folds))
