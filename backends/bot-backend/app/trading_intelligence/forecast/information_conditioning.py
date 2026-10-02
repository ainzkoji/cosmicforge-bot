"""V3 additive logistic conditioning. No outcome fields enter the feature path.

L2-regularized categorical deviations share a global intercept and continuous
geometry slopes. Infrequent instrument effects are omitted, hence shrink to the
broader family/side/regime/volatility prediction. Scaling is training-only.
The serializable model contains numbers and vocabulary, never executable pickle.
"""
from __future__ import annotations

from collections import Counter
import math
import numpy as np
from scipy import sparse
from scipy.special import expit

from app.replay.cost_model import BINANCE_FUTURES_STANDARD
from app.trading_intelligence.contracts.setup import timeframe_to_ms

FEATURE_SCHEMA = "cati-v3-causal-geometry-additive-1"
ESTIMATOR = "L2_ADDITIVE_LOGISTIC_LOG_GEOMETRY"
CATEGORICAL = ("setup_family", "side", "dominant_regime", "volatility_bucket",
               "timeframe", "horizon", "instrument_group", "instrument")
CONTINUOUS = ("log_room", "log_risk_fraction", "log1p_cost_R", "log_room_x_log_risk")


def causal_features(*, dimensions, room, risk_fraction, timeframe, horizon, instrument):
    span = timeframe_to_ms(timeframe)
    if span is None or int(horizon) != horizon or horizon <= 0:
        raise ValueError("explicit supported timeframe and positive horizon required")
    if not all(math.isfinite(float(v)) and v > 0 for v in (room, risk_fraction)):
        raise ValueError("finite positive decision-time geometry required")
    cost = BINANCE_FUTURES_STANDARD.round_trip_cost(1., int(horizon)*span) / risk_fraction
    lr, lk = math.log(room), math.log(risk_fraction)
    cats = {k: str(dimensions[k]) for k in CATEGORICAL[:4]}
    cats.update(timeframe=timeframe, horizon=str(int(horizon)), instrument=instrument,
                instrument_group=str(dimensions.get("instrument_group", "UNKNOWN")))
    # UNKNOWN is absence, not invented information.
    cats = {k:v for k,v in cats.items() if v not in ("UNKNOWN", "UNVERIFIED", "")}
    return cats, np.asarray((lr, lk, math.log1p(cost), lr*lk), dtype=float)


class InformationConditioner:
    def __init__(self, regularization=0.1, minimum_instrument_support=300):
        self.regularization = float(regularization)
        self.minimum_instrument_support = int(minimum_instrument_support)

    def _matrix(self, features):
        cats, values = zip(*features)
        z = (np.asarray(values)-self.mean)/self.scale
        ri, ci = [], []
        for i, row in enumerate(cats):
            for k,v in row.items():
                j = self.vocabulary.get(k+"="+v)
                if j is not None: ri.append(i); ci.append(j)
        onehot = sparse.csr_matrix((np.ones(len(ri)), (ri,ci)),
                                   shape=(len(cats),len(self.vocabulary)))
        return sparse.hstack((sparse.csr_matrix(z),onehot),format="csr")

    def fit(self, features, outcomes):
        from sklearn.linear_model import LogisticRegression
        values = np.asarray([f[1] for f in features])
        self.mean = values.mean(axis=0)
        self.scale = values.std(axis=0)
        self.scale[self.scale < 1e-10] = 1.
        counts = Counter(k+"="+v for cats,_ in features for k,v in cats.items())
        keys = sorted(k for k,n in counts.items()
                      if n >= (self.minimum_instrument_support if k.startswith("instrument") else 30))
        self.vocabulary = {k:i for i,k in enumerate(keys)}
        model = LogisticRegression(C=self.regularization, solver="lbfgs", max_iter=250,
                                   tol=1e-7, random_state=0)
        import warnings
        from sklearn.exceptions import ConvergenceWarning
        with warnings.catch_warnings():
            warnings.simplefilter("error", ConvergenceWarning)
            model.fit(self._matrix(features), outcomes)
        self.coefficients = model.coef_[0]
        self.intercept = float(model.intercept_[0])
        return self

    def predict(self, features):
        return expit(self._matrix(features) @ self.coefficients + self.intercept)

    def to_dict(self):
        return dict(feature_schema=FEATURE_SCHEMA, estimator=ESTIMATOR,
                    regularization=self.regularization,
                    minimum_instrument_support=self.minimum_instrument_support,
                    mean=self.mean.tolist(), scale=self.scale.tolist(), vocabulary=self.vocabulary,
                    coefficients=self.coefficients.tolist(), intercept=self.intercept)

    @classmethod
    def from_dict(cls, d):
        if d["feature_schema"] != FEATURE_SCHEMA or d["estimator"] != ESTIMATOR:
            raise ValueError("unsupported V3 schema/estimator")
        obj = cls(d["regularization"], d["minimum_instrument_support"])
        obj.mean, obj.scale = np.asarray(d["mean"]), np.asarray(d["scale"])
        obj.vocabulary = dict(d["vocabulary"])
        obj.coefficients, obj.intercept = np.asarray(d["coefficients"]), float(d["intercept"])
        if (obj.mean.shape != (4,) or obj.scale.shape != (4,)
                or len(obj.coefficients) != 4+len(obj.vocabulary)
                or sorted(obj.vocabulary.values()) != list(range(len(obj.vocabulary)))
                or not np.all(obj.scale > 0)
                or not all(np.all(np.isfinite(a)) for a in
                           (obj.mean,obj.scale,obj.coefficients,obj.intercept))):
            raise ValueError("invalid V3 model parameters")
        return obj


def matured_indices(times, label_ends, forecast_time):
    """Strict information barrier, including ties and horizon maturity."""
    return np.flatnonzero((times < forecast_time) & (label_ends < forecast_time))


def causal_baseline(outcomes, times, label_ends, forecast_time):
    idx = matured_indices(times,label_ends,forecast_time)
    if not len(idx):
        raise ValueError("no matured baseline observations")
    return float(np.mean(outcomes[idx]))
