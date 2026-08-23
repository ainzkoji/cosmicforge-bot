"""
calibrated_model.py  --  Shared isotonic calibration wrapper for ML artifacts.

Must be importable from BOTH training scripts and inference scripts so that
joblib.load() can resolve the class during unpickling.

Used by:
  - scripts/ml/train_entry_model.py  (saves artifacts)
  - scripts/ml/analyze_holdout.py    (loads artifacts for analysis)
  - app/ml/scorer.py                 (loads artifacts for live inference)
"""

from __future__ import annotations

import numpy as np


class IsotonicCalibratedModel:
    """
    Wraps a fitted LightGBM classifier with an IsotonicRegression post-layer.

    Produces well-spread probability scores instead of the bimodal 0.0/1.0
    distribution that results from near-perfect training data memorization.

    The base LightGBM model's feature_importances_ are preserved for analysis.
    """

    def __init__(self, base, iso):
        self._base = base
        self._iso = iso
        try:
            self.feature_importances_ = base.feature_importances_
        except AttributeError:
            pass

    def predict_proba(self, X):
        """Return calibrated probabilities in sklearn [n_samples, 2] format."""
        raw = self._base.predict_proba(X)[:, 1]
        cal = self._iso.predict(raw)
        return np.column_stack([1.0 - cal, cal])

    def predict(self, X):
        """Return binary predictions using 0.5 threshold on calibrated proba."""
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)
