"""Registered V4 encoders, pooled logistic models and conditional payoff trees.

Training statistics and labels never enter prediction inputs. Models serialize
to validated numeric JSON, with no executable pickle/joblib deserialization.
"""
from __future__ import annotations
import warnings
import numpy as np
from scipy import sparse
from scipy.special import expit,softmax
from sklearn.linear_model import LogisticRegression
from sklearn.exceptions import ConvergenceWarning
from sklearn.ensemble import HistGradientBoostingRegressor

FEATURE_SCHEMA='cati-v4-closed-context-geometry-1'
MODES=('REGULARIZED_SPLINE_LOGISTIC','FAMILY_AWARE_GEOMETRY','CAUSAL_MARKET_CONTEXT','BOUNDED_HYBRID')
CAT_NAMES=('family','side','regime','volatility','instrument')


class FeatureEncoder:
    def __init__(self,mode):
        if mode not in MODES: raise ValueError('unknown V4 model family')
        self.mode=mode

    def _numeric(self,geometry,context):
        g=np.asarray(geometry,dtype=float)
        parts=[g]
        if self.mode in (MODES[0],MODES[3]):
            # Smooth C1 truncated quadratic spline; knots and clipping are fitted
            # exclusively on training geometry. Twelve extra continuous terms.
            z=np.clip(g[:,:3],self.lower,self.upper)
            parts.extend((z*z,np.maximum(z[:,:,None]-self.knots,0.).reshape(len(g),-1)**2))
        if self.mode in (MODES[2],MODES[3]):
            c=np.asarray(context,dtype=float)
            if c.shape!=(len(g),11) or not np.all(np.isfinite(c)):
                raise ValueError('actual aligned market context required')
            parts.append(c)
        return np.column_stack(parts)

    def fit(self,geometry,context,categories):
        g=np.asarray(geometry); cats=np.asarray(categories)
        if g.shape[1]!=4 or cats.shape!=(len(g),5) or not np.all(np.isfinite(g)):
            raise ValueError('invalid causal feature shape/values')
        self.lower=np.min(g[:,:3],axis=0); self.upper=np.max(g[:,:3],axis=0)
        self.knots=np.quantile(g[:,:3],[.25,.5,.75],axis=0).T
        x=self._numeric(g,context); self.mean=x.mean(axis=0); self.scale=x.std(axis=0)
        self.scale[self.scale<1e-10]=1.
        self.vocabularies=[]
        for j in range(5):
            keys,counts=np.unique(cats[:,j],return_counts=True)
            self.vocabularies.append([str(k) for k,n in zip(keys,counts) if k not in ('','UNKNOWN','UNVERIFIED') and n>=(300 if j==4 else 30)])
        self.families=self.vocabularies[0]
        return self

    def transform(self,geometry,context,categories):
        cats=np.asarray(categories); g=np.asarray(geometry)
        z=(self._numeric(g,context)-self.mean)/self.scale
        if not np.all(np.isfinite(z)): raise ValueError('nonfinite V4 predictors')
        parts=[sparse.csr_matrix(z)]
        for j,keys in enumerate(self.vocabularies):
            parts.append(sparse.csr_matrix(np.column_stack([cats[:,j]==k for k in keys]).astype(float)) if keys else sparse.csr_matrix((len(g),0)))
        if self.mode in (MODES[1],MODES[3]) and self.families:
            # Family slopes are L2-penalized deviations from shared geometry.
            # Unpopulated families retain the shared slopes.
            parts.append(sparse.csr_matrix(np.column_stack([(cats[:,0]==f)[:,None]*z[:,:4] for f in self.families])))
        return sparse.hstack(parts,format='csr')

    def to_dict(self):
        return dict(mode=self.mode,lower=self.lower.tolist(),upper=self.upper.tolist(),knots=self.knots.tolist(),
                    mean=self.mean.tolist(),scale=self.scale.tolist(),vocabularies=self.vocabularies)

    @classmethod
    def from_dict(cls,d):
        obj=cls(d['mode'])
        for k in ('lower','upper','knots','mean','scale'): setattr(obj,k,np.asarray(d[k],dtype=float))
        obj.vocabularies=d['vocabularies']; obj.families=obj.vocabularies[0]
        expected=4+(12 if obj.mode in (MODES[0],MODES[3]) else 0)+(11 if obj.mode in (MODES[2],MODES[3]) else 0)
        if (obj.mean.shape!=(expected,) or obj.scale.shape!=(expected,) or obj.knots.shape!=(3,3)
                or obj.lower.shape!=(3,) or obj.upper.shape!=(3,) or len(obj.vocabularies)!=5
                or np.any(obj.scale<=0) or np.any(obj.lower>obj.upper)
                or any(len(v)!=len(set(v)) for v in obj.vocabularies)
                or not all(np.all(np.isfinite(getattr(obj,k))) for k in ('lower','upper','knots','mean','scale'))):
            raise ValueError('invalid V4 transform parameters')
        return obj


def probability_gate(m,folds,gate):
    return (len(folds)==5 and m['samples']>=gate['minimum_samples'] and m['brier_skill']>=gate['minimum_brier_skill']
        and m['ece']<=gate['maximum_ece'] and all(f['brier_skill']>0 and f['ece']<=gate['each_outer_ece_maximum'] for f in folds)
        and all(folds[f-1]['brier_skill']>=gate[f'minimum_fold_{f}_skill'] for f in (3,4,5)))


def payoff_gate(m,folds,gate):
    return (len(folds)==5 and m['expected_net_R']['rmse']<=m['causal_baseline_net_R']['rmse']
        and m['expected_net_R']['mae']<=m['causal_baseline_net_R']['mae']
        and m['terminal_multiclass_brier']<=m['causal_terminal_baseline_brier']
        and m['maximum_bucket_absolute_bias']<=gate['maximum_expected_R_bucket_absolute_bias']
        and m['maximum_expectancy_quintile_inversion_R']<=gate['maximum_expectancy_quintile_inversion_R']
        and all(m['quantiles'][k][str(q)]['coverage_error']<=gate['maximum_quantile_coverage_error'] for k in ('mfe','mae') for q in (.1,.5,.9))
        and all(f['expected_net_R']['rmse']<=f['causal_baseline_net_R']['rmse']*(1+gate['maximum_each_fold_RMSE_relative_degradation']) for f in folds))


class V4Probability:
    def __init__(self,mode,C=.01): self.mode=mode; self.C=float(C)

    def fit(self,g,c,cats,y):
        self.encoder=FeatureEncoder(self.mode).fit(g,c,cats)
        model=LogisticRegression(C=self.C,solver='lbfgs',max_iter=2000,tol=1e-6,random_state=0)
        with warnings.catch_warnings():
            warnings.simplefilter('error',ConvergenceWarning)
            model.fit(self.encoder.transform(g,c,cats),y)
        self.coefficients=model.coef_[0]; self.intercept=float(model.intercept_[0])
        return self

    def predict(self,g,c,cats):
        return expit(self.encoder.transform(g,c,cats)@self.coefficients+self.intercept)

    def to_dict(self):
        return dict(schema=FEATURE_SCHEMA,mode=self.mode,C=self.C,encoder=self.encoder.to_dict(),coefficients=self.coefficients.tolist(),intercept=self.intercept)

    @classmethod
    def from_dict(cls,d):
        if d['schema']!=FEATURE_SCHEMA: raise ValueError('wrong V4 feature schema')
        obj=cls(d['mode'],d['C']); obj.encoder=FeatureEncoder.from_dict(d['encoder'])
        obj.coefficients=np.asarray(d['coefficients']); obj.intercept=float(d['intercept'])
        width=len(obj.encoder.mean)+sum(map(len,obj.encoder.vocabularies))+(4*len(obj.encoder.families) if obj.mode in (MODES[1],MODES[3]) else 0)
        if len(obj.coefficients)!=width or not np.all(np.isfinite(obj.coefficients)) or not np.isfinite(obj.intercept) or obj.mode!=obj.encoder.mode:
            raise ValueError('invalid V4 probability coefficients')
        return obj


class NumericTrees:
    """Numeric-only export of sklearn histogram boosting, including missing routing."""
    @classmethod
    def fit(cls,x,y,*,quantile=None):
        model=HistGradientBoostingRegressor(loss='squared_error' if quantile is None else 'quantile',
            quantile=quantile,max_iter=60,max_leaf_nodes=15,min_samples_leaf=200,
            l2_regularization=10.,early_stopping=False,random_state=0)
        model.fit(x,y)
        obj=cls(); obj.base=float(model._baseline_prediction[0,0]); obj.width=x.shape[1]; obj.trees=[]
        for stage in model._predictors:
            nodes=stage[0].nodes
            if np.any(nodes['is_categorical']): raise ValueError('categorical tree export unsupported')
            obj.trees.append([{k:float(n[k]) if k in ('value','num_threshold') else int(n[k]) for k in
                ('value','num_threshold','feature_idx','left','right','is_leaf','missing_go_to_left')} for n in nodes])
        np.testing.assert_allclose(obj.predict(x),model.predict(x),rtol=1e-11,atol=1e-11)
        return obj

    def predict(self,x):
        x=np.asarray(x)
        if x.shape[1]!=self.width: raise ValueError('payoff predictor width mismatch')
        result=np.full(len(x),self.base)
        for nodes in self.trees:
            queue=[(0,np.arange(len(x)))]
            while queue:
                i,rows=queue.pop()
                if not len(rows): continue
                n=nodes[i]
                if n['is_leaf']: result[rows]+=n['value']; continue
                col=x[rows,n['feature_idx']]; left=col<=n['num_threshold']
                left=np.where(np.isnan(col),bool(n['missing_go_to_left']),left)
                queue.extend(((n['left'],rows[left]),(n['right'],rows[~left])))
        return result

    def to_dict(self): return dict(base=self.base,width=self.width,trees=self.trees)

    @classmethod
    def from_dict(cls,d):
        obj=cls(); obj.base=float(d['base']); obj.width=int(d['width']); obj.trees=d['trees']
        if not np.isfinite(obj.base) or obj.width<1 or len(obj.trees)!=60: raise ValueError('invalid payoff tree model')
        for tree in obj.trees:
            if not tree or len(tree)>29: raise ValueError('invalid payoff tree size')
            for i,n in enumerate(tree):
                if not np.isfinite(n['value']) or not np.isfinite(n['num_threshold']): raise ValueError('nonfinite payoff node')
                if not n['is_leaf'] and (not i<n['left']<len(tree) or not i<n['right']<len(tree) or not 0<=n['feature_idx']<obj.width):
                    raise ValueError('invalid payoff tree topology')
        return obj


class ConditionalPayoff:
    """Independent terminal, conditional mean magnitude and MFE/MAE quantile models."""
    def fit(self,g,c,cats,targets):
        self.encoder=FeatureEncoder(MODES[3]).fit(g,c,cats)
        x=self.encoder.transform(g,c,cats).toarray().astype(np.float32)
        y=targets[:,0].astype(bool); net=targets[:,1]
        self.positive=NumericTrees.fit(x[y],net[y]); self.loss=NumericTrees.fit(x[~y],net[~y])
        terminal=LogisticRegression(C=.01,max_iter=2000,tol=1e-6,random_state=0)
        with warnings.catch_warnings():
            warnings.simplefilter('error',ConvergenceWarning); terminal.fit(x,targets[:,5].astype(int))
        if terminal.classes_.tolist()!=[0,1,2]: raise ValueError('terminal classes missing')
        self.terminal_coefficients=terminal.coef_; self.terminal_intercept=terminal.intercept_
        timeout=targets[:,5]==2
        self.timeout_gross=NumericTrees.fit(x[timeout],targets[timeout,2])
        self.quantiles={}
        for name,j in (('mfe',3),('mae',4)):
            self.quantiles[name]=[NumericTrees.fit(x,np.log1p(np.maximum(targets[:,j],0)),quantile=q) for q in (.1,.5,.9)]
        return self

    def predict(self,g,c,cats,p):
        x=self.encoder.transform(g,c,cats).toarray().astype(np.float32)
        positive=np.maximum(self.positive.predict(x),0.)
        loss=np.minimum(self.loss.predict(x),0.)
        terminal=softmax(x@self.terminal_coefficients.T+self.terminal_intercept,axis=1)
        timeout=self.timeout_gross.predict(x)
        raw={k:np.column_stack([np.maximum(np.expm1(m.predict(x)),0.) for m in ms]) for k,ms in self.quantiles.items()}
        quantiles={k:np.sort(v,axis=1) for k,v in raw.items()}
        return dict(expected_net_R=p*positive+(1-p)*loss,conditional_positive_net_R=positive,
            conditional_loss_net_R=loss,terminal=terminal,timeout_gross_R=timeout,
            expected_gross_R=terminal[:,0]*np.exp(g[:,0])-terminal[:,1]+terminal[:,2]*timeout,
            **quantiles,quantile_crossings={k:int(np.sum(np.any(np.diff(v,axis=1)<0,axis=1))) for k,v in raw.items()})

    def to_dict(self):
        return dict(encoder=self.encoder.to_dict(),positive=self.positive.to_dict(),loss=self.loss.to_dict(),
            timeout_gross=self.timeout_gross.to_dict(),terminal_coefficients=self.terminal_coefficients.tolist(),
            terminal_intercept=self.terminal_intercept.tolist(),quantiles={k:[m.to_dict() for m in ms] for k,ms in self.quantiles.items()})

    @classmethod
    def from_dict(cls,d):
        obj=cls(); obj.encoder=FeatureEncoder.from_dict(d['encoder'])
        for k in ('positive','loss','timeout_gross'): setattr(obj,k,NumericTrees.from_dict(d[k]))
        obj.terminal_coefficients=np.asarray(d['terminal_coefficients']); obj.terminal_intercept=np.asarray(d['terminal_intercept'])
        obj.quantiles={k:[NumericTrees.from_dict(m) for m in ms] for k,ms in d['quantiles'].items()}
        width=obj.positive.width
        if (obj.terminal_coefficients.shape!=(3,width) or obj.terminal_intercept.shape!=(3,)
            or set(obj.quantiles)!={'mfe','mae'} or any(len(v)!=3 for v in obj.quantiles.values())
            or not np.all(np.isfinite(obj.terminal_coefficients)) or not np.all(np.isfinite(obj.terminal_intercept))
            or any(m.width!=width for m in (obj.loss,obj.timeout_gross,*[m for ms in obj.quantiles.values() for m in ms]))):
            raise ValueError('invalid conditional payoff identity/shape')
        return obj
