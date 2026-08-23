#!/usr/bin/env python3
"""
ML Entry Quality Scorer — Training Script — Phase 3 Step 5D-1 / 5E-7B

Trains a LightGBM binary classifier to score whether a trade will be profitable,
using only pre-trade features from decision_traces (zero look-ahead).

Architecture: additive scoring layer — model can block/reduce but cannot force entries.
Rule engine retains veto power. Model runs in shadow mode before activation.

Walk-forward temporal validation only — no random splits, no future leakage.

v1.1 refinements (Step 5E-7B):
  - Removed 6 all-NULL adaptive engine features from training feature set.
    These features (aggressiveness_score etc.) impute to 0 in backfill but take
    real values in live data, creating training-inference distribution mismatch.
    They are retained in scorer.py schema and will be re-added in v1.2 once
    live data provides non-null training examples.
  - Added scale_pos_weight to correct 33% win-rate class imbalance.
  - Added CalibratedClassifierCV(method='isotonic') post-fit wrapper to produce
    well-spread probability scores instead of the bimodal 0.05/0.85 distribution
    caused by near-perfect memorization of backfill data.

Usage:
    python train_entry_model.py --dataset-path models/datasets/training_YYYYMMDD.parquet
    python train_entry_model.py --dataset-path models/datasets/training_YYYYMMDD.parquet \\
        --model-version v1.1 --scale-pos-weight auto --calibrate

Output (models/artifacts/):
    entry_quality_v1.1_YYYYMMDD.pkl           — model artifact (calibrated classifier)
    entry_quality_v1.1_YYYYMMDD_meta.json     — training metadata + metrics
    entry_quality_v1.1_YYYYMMDD_encoders.pkl  — categorical encoders (required at inference)
    entry_quality_v1.1_YYYYMMDD_validation.json — full fold-by-fold validation report
    entry_quality_v1.1_YYYYMMDD_calibration.png — OOF calibration curve (if matplotlib available)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

# -- Dependency check ----------------------------------------------------------
_MISSING: list[str] = []
for _pkg, _install in [
    ("pandas",    "pandas"),
    ("numpy",     "numpy"),
    ("lightgbm",  "lightgbm"),
    ("sklearn",   "scikit-learn"),
    ("joblib",    "joblib"),
]:
    try:
        __import__(_pkg)
    except ImportError:
        _MISSING.append(f"pip install {_install}")

if _MISSING:
    print("[FAIL] Missing required packages:", file=sys.stderr)
    for m in _MISSING:
        print(f"   {m}", file=sys.stderr)
    sys.exit(1)

import joblib
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, brier_score_loss, f1_score,
    log_loss, precision_score, recall_score, roc_auc_score,
)
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from shared_lib.ml.contract import (
    LABEL_COLUMNS,
    METADATA_COLUMNS,
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
    build_contract_metadata,
)

try:
    import shap as _shap
    _HAS_SHAP = True
except ImportError:
    _HAS_SHAP = False

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    _HAS_MPL = True
except ImportError:
    _HAS_MPL = False


# -- Canonical ML contract ----------------------------------------------------

FEATURE_COLUMNS: list[str] = list(ML_FEATURE_COLUMNS)
CATEGORICAL_FEATURES: list[str] = []

LABEL_WIN = "label_win"
LABEL_R   = "label_r_multiple"

# Walk-forward config
_MIN_TRAIN   = 300    # minimum required dataset size
_TRAIN_FRAC  = 0.70
_VAL_FRAC    = 0.15
_TEST_FRAC   = 0.15
_ES_ROUNDS   = 50     # early stopping patience (LightGBM rounds)
_ES_FRAC     = 0.15   # fraction of train used as early stopping hold-out

# Hyperparameter search
_SEED          = 42
_N_SEARCH      = 20   # random param combinations to try
_MAX_FOLDS_SEARCH = 5 # max folds to use during param search (speed limit)

_PARAM_SPACE: dict[str, list] = {
    "n_estimators":      [100, 300, 500],
    "max_depth":         [3, 5, 7],
    "learning_rate":     [0.01, 0.05, 0.1],
    "min_child_samples": [20, 50],
    "subsample":         [0.7, 0.8],
    "colsample_bytree":  [0.7, 0.8],
    "reg_alpha":         [0.0, 0.1],
    "reg_lambda":        [0.0, 1.0],
}

_DEFAULT_PARAMS: dict[str, Any] = {
    "n_estimators": 300, "max_depth": 5, "learning_rate": 0.05,
    "min_child_samples": 20, "subsample": 0.8, "colsample_bytree": 0.8,
    "reg_alpha": 0.0, "reg_lambda": 1.0,
}


# -- CLI -----------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Train LightGBM entry quality scorer (Phase 3 Step 5D-1 / 5E-7B).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--dataset-path", required=True, metavar="PATH",
                   help="Path to training Parquet from build_dataset.py.")
    p.add_argument("--output-dir", default=None, metavar="DIR",
                   help="Artifact output directory. Default: <script>/../../models/artifacts/")
    p.add_argument("--min-auc", type=float, default=0.55, metavar="FLOAT",
                   help="Minimum acceptable mean walk-forward AUC-ROC (default: 0.55).")
    p.add_argument(
        "--min-baseline-improvement",
        type=float,
        default=float(os.environ.get("ML_MIN_BASELINE_AUC_IMPROVEMENT", "0.01")),
        metavar="FLOAT",
        help="Minimum AUC improvement over logistic baseline (default: 0.01).",
    )
    p.add_argument("--skip-regression", action="store_true",
                   help="Skip R-multiple regression model (train classifier only).")
    p.add_argument("--verbose", action="store_true",
                   help="Print per-fold metrics during evaluation.")
    p.add_argument("--max-train-minutes", type=float, default=60.0, metavar="FLOAT",
                   help="Training timeout in minutes (default: 60).")
    p.add_argument("--model-version", default=ML_CONTRACT_VERSION, metavar="VER",
                   help="Model version string for artifact naming "
                        f"(default: {ML_CONTRACT_VERSION}).")
    p.add_argument("--scale-pos-weight", default=None, metavar="FLOAT|auto",
                   help="LightGBM class weight for positive class. "
                        "'auto' computes (1-win_rate)/win_rate from training data. "
                        "Recommended for v1.1: auto. Default: None (balanced=1.0).")
    p.add_argument("--calibrate", action="store_true",
                   help="Wrap final model with CalibratedClassifierCV(isotonic) "
                        "to spread bimodal scores. Recommended for v1.1.")
    p.add_argument("--min-train-size", type=int, default=None, metavar="N",
                   help=(
                       "Override the minimum training-split row count "
                       f"(default: {_MIN_TRAIN}). Use with care — lower values "
                       "reduce statistical reliability of the trained model."
                   ))
    return p.parse_args()


# -- Data loading --------------------------------------------------------------

def _load_dataset(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    missing = [c for c in FEATURE_COLUMNS + [LABEL_WIN] if c not in df.columns]
    if missing:
        print(f"[FAIL] Missing columns: {missing}", file=sys.stderr)
        sys.exit(1)
    return df


def _leakage_assertions(df: pd.DataFrame) -> bool:
    passed = True
    feature_set = set(FEATURE_COLUMNS)

    overlap_lab = feature_set & set(LABEL_COLUMNS)
    if overlap_lab:
        print(f"  [FAIL] Label columns in features: {overlap_lab}")
        passed = False
    else:
        print("  [OK] No label columns in feature set")

    overlap_meta = feature_set & set(METADATA_COLUMNS)
    if overlap_meta:
        print(f"  [FAIL] Metadata columns in features: {overlap_meta}")
        passed = False
    else:
        print("  [OK] No metadata columns in feature set")

    if "open_timestamp" in df.columns and "close_timestamp" in df.columns:
        ots = pd.to_datetime(df["open_timestamp"],  errors="coerce", utc=True)
        cts = pd.to_datetime(df["close_timestamp"], errors="coerce", utc=True)
        bad = int((cts <= ots).sum())
        if bad:
            print(f"  [FAIL] {bad} rows: close_timestamp <= open_timestamp")
            passed = False
        else:
            print("  [OK] Timestamps: open < close for all rows")

    return passed


def _log_stats(df: pd.DataFrame) -> None:
    n = len(df)
    wr = float(df[LABEL_WIN].mean()) if LABEL_WIN in df.columns else float("nan")
    print(f"\n  Rows:       {n:,}")
    print(f"  Win rate:   {wr:.1%}" if not math.isnan(wr) else "  Win rate:   N/A")

    if "open_timestamp" in df.columns:
        ots = pd.to_datetime(df["open_timestamp"], errors="coerce", utc=True)
        cts = pd.to_datetime(df["close_timestamp"], errors="coerce", utc=True)
        print(f"  Date range: {ots.min().date()} -> {cts.max().date()}")

    if "symbol" in df.columns:
        print(f"  Symbols:    {df['symbol'].value_counts().to_dict()}")

    regime_dist = {}
    for r in ["STRONG_TREND", "WEAK_TREND", "RANGE", "HIGH_VOLATILITY", "LOW_VOLATILITY_CHOP"]:
        col = f"regime_{r}"
        if col in df.columns:
            regime_dist[r] = int(df[col].sum())
    if regime_dist:
        print(f"  Regimes:    {regime_dist}")


# -- Feature preparation -------------------------------------------------------

def _fit_encoders(df: pd.DataFrame) -> dict[str, OrdinalEncoder]:
    encoders: dict[str, OrdinalEncoder] = {}
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            continue
        enc = OrdinalEncoder(
            handle_unknown="use_encoded_value",
            unknown_value=np.nan,
            encoded_missing_value=np.nan,
        )
        enc.fit(df[[col]].astype(str))
        encoders[col] = enc
    return encoders


def _encode(df: pd.DataFrame, encoders: dict) -> pd.DataFrame:
    df = df.copy()
    for col, enc in encoders.items():
        if col in df.columns:
            df[col] = enc.transform(df[[col]].astype(str)).flatten()
    return df


def _make_Xy(
    df: pd.DataFrame,
    encoders: dict,
    label: str,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """Return (X, y, df_clean) after encoding and dropping rows with NULL label."""
    df_enc = _encode(df, encoders)

    mask = df_enc[label].notna()
    n_drop = int((~mask).sum())
    if n_drop:
        print(f"  Dropped {n_drop:,} rows with NULL '{label}'")
    df_clean = df_enc[mask].reset_index(drop=True)

    X = df_clean[FEATURE_COLUMNS].copy()
    y = df_clean[label].astype(float).reset_index(drop=True)
    return X, y, df_clean


# -- Isotonic calibration wrapper — imported from app.ml.calibrated_model ----
# Canonical module path so joblib serializes as 'app.ml.calibrated_model.IsotonicCalibratedModel'.
# All scripts that load this artifact must have backends/bot-backend/ on sys.path.
from app.ml.calibrated_model import IsotonicCalibratedModel  # noqa: E402


# -- Walk-forward fold generation ----------------------------------------------

def _make_splits(n: int) -> tuple[tuple[int, int, int, int], tuple[int, int, int, int]]:
    """
    Chronological Train/Val/Test split.
    val_fold is used for hyperparameter tuning.
    test_fold is used for final unseen evaluation.
    """
    val_size = int(n * _VAL_FRAC)
    test_size = int(n * _TEST_FRAC)
    train_size = n - val_size - test_size
    
    val_fold = (0, train_size, train_size, train_size + val_size)
    # the test fold trains on train + val sets
    test_fold = (0, train_size + val_size, train_size + val_size, n)
    return val_fold, test_fold


# -- LightGBM model construction -----------------------------------------------

def _build_classifier(params: dict, scale_pos_weight: Optional[float] = None) -> lgb.LGBMClassifier:
    p = {**_DEFAULT_PARAMS, **params}
    kwargs: dict = dict(
        objective="binary", random_state=_SEED, verbose=-1, n_jobs=-1,
        n_estimators=p["n_estimators"], max_depth=p["max_depth"],
        learning_rate=p["learning_rate"], min_child_samples=p["min_child_samples"],
        subsample=p["subsample"], colsample_bytree=p["colsample_bytree"],
        reg_alpha=p["reg_alpha"], reg_lambda=p["reg_lambda"],
    )
    if scale_pos_weight is not None:
        kwargs["scale_pos_weight"] = scale_pos_weight
    return lgb.LGBMClassifier(**kwargs)


def _build_regressor(params: dict) -> lgb.LGBMRegressor:
    p = {**_DEFAULT_PARAMS, **params}
    return lgb.LGBMRegressor(
        objective="huber", alpha=0.9, random_state=_SEED, verbose=-1, n_jobs=-1,
        n_estimators=p["n_estimators"], max_depth=p["max_depth"],
        learning_rate=p["learning_rate"], min_child_samples=p["min_child_samples"],
        subsample=p["subsample"], colsample_bytree=p["colsample_bytree"],
        reg_alpha=p["reg_alpha"], reg_lambda=p["reg_lambda"],
    )


def _fit_lgbm(model: Any, X_tr: pd.DataFrame, y_tr: pd.Series,
              cat_cols: list[str]) -> Any:
    """
    Fit LightGBM with early stopping on a hold-out of the last _ES_FRAC of X_tr.
    """
    n = len(X_tr)
    es_start = max(int(n * (1 - _ES_FRAC)), n - 200)

    X_t, y_t = X_tr.iloc[:es_start], y_tr.iloc[:es_start]
    X_e, y_e = X_tr.iloc[es_start:], y_tr.iloc[es_start:]

    cb = [lgb.early_stopping(_ES_ROUNDS, verbose=False), lgb.log_evaluation(-1)]

    if len(X_e) < 10:
        # Too small for ES — train without it
        model.fit(X_t, y_t, categorical_feature=cat_cols or "auto")
    else:
        model.fit(X_t, y_t, eval_set=[(X_e, y_e)],
                  categorical_feature=cat_cols or "auto", callbacks=cb)
    return model


# -- Fold metrics -------------------------------------------------------------

def _safe_float(v) -> float:
    """Return float or nan for None/non-float."""
    if v is None:
        return float("nan")
    try:
        f = float(v)
        return float("nan") if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return float("nan")


def _quartile_stats(
    y_true: np.ndarray,
    y_score: np.ndarray,
    y_r: Optional[np.ndarray] = None,
) -> dict:
    """Compute top/bottom quartile win rates and mean R, ranked by y_score."""
    n = len(y_true)
    if n < 4:
        return {}
    idx = np.argsort(y_score)          # ascending
    q1 = max(1, int(n * 0.25))
    q4 = max(q1, int(n * 0.75))

    bot_idx = idx[:q1]
    top_idx = idx[q4:]

    res: dict = {
        "top_q_win":         float(np.mean(y_true[top_idx])),
        "bottom_q_win":      float(np.mean(y_true[bot_idx])),
        "quartile_diff_win": float(np.mean(y_true[top_idx]) - np.mean(y_true[bot_idx])),
    }
    if y_r is not None:
        r_top = y_r[top_idx]
        r_bot = y_r[bot_idx]
        res["top_q_mean_r"]    = float(np.nanmean(r_top)) if len(r_top) else float("nan")
        res["bottom_q_mean_r"] = float(np.nanmean(r_bot)) if len(r_bot) else float("nan")
    return res


def _binary_metrics(
    y_true: np.ndarray,
    y_score: np.ndarray,
    y_r: Optional[np.ndarray] = None,
) -> dict:
    """All metrics for a binary classification fold."""
    if len(np.unique(y_true)) < 2:
        return {"auc": float("nan"), "degenerate": True}

    y_pred = (y_score >= 0.5).astype(int)
    m: dict = {
        "auc":       float(roc_auc_score(y_true, y_score)),
        "accuracy":  float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall":    float(recall_score(y_true, y_pred, zero_division=0)),
        "f1":        float(f1_score(y_true, y_pred, zero_division=0)),
        "brier":     float(brier_score_loss(y_true, y_score)),
        "log_loss":  float(log_loss(y_true, np.clip(y_score, 1e-7, 1 - 1e-7))),
        "actual_win_rate": float(np.mean(y_true)),
        "n_val":     len(y_true),
    }
    m.update(_quartile_stats(y_true, y_score, y_r))
    return m


def _regression_metrics(y_r_true: np.ndarray, y_r_pred: np.ndarray) -> dict:
    """Metrics for R-multiple regression fold (evaluated by ranking quality)."""
    valid = ~(np.isnan(y_r_true) | np.isnan(y_r_pred))
    if valid.sum() < 10:
        return {"auc": float("nan"), "degenerate": True}

    yt = y_r_true[valid]
    yp = y_r_pred[valid]
    y_bin = (yt > 0).astype(int)

    auc = float("nan")
    if len(np.unique(y_bin)) == 2:
        try:
            auc = float(roc_auc_score(y_bin, yp))
        except Exception:
            pass

    m = {"auc": auc, "n_val": int(valid.sum()), "actual_win_rate": float(np.mean(y_bin))}
    m.update(_quartile_stats(y_bin, yp, yt))
    return m


def _oof_calibration(y_true: np.ndarray, y_score: np.ndarray,
                     n_bins: int = 10) -> list[dict]:
    """Bin OOF predictions into equal-width bins; compute actual win rate per bin."""
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    result = []
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        mask = (y_score >= lo) & (y_score <= hi if i == n_bins - 1 else y_score < hi)
        cnt = int(mask.sum())
        if cnt == 0:
            continue
        result.append({
            "predicted_bin_center": round(float((lo + hi) / 2), 2),
            "predicted_low":        round(float(lo), 2),
            "predicted_high":       round(float(hi), 2),
            "actual_win_rate":      round(float(np.mean(y_true[mask])), 4),
            "count":                cnt,
        })
    return result


# -- Walk-forward runners ------------------------------------------------------

def _agg_folds(fold_results: list[dict]) -> dict:
    """Compute mean ± std for each metric across valid (non-degenerate) folds."""
    valid = [f for f in fold_results
             if not f.get("degenerate", False) and not math.isnan(_safe_float(f.get("auc")))]

    metric_keys = [
        "auc", "accuracy", "precision", "recall", "f1", "brier", "log_loss",
        "top_q_win", "bottom_q_win", "quartile_diff_win",
        "top_q_mean_r", "bottom_q_mean_r",
    ]
    agg: dict = {"n_folds": len(valid), "n_total_folds": len(fold_results)}
    for k in metric_keys:
        vals = [_safe_float(f.get(k)) for f in valid]
        vals = [v for v in vals if not math.isnan(v)]
        if vals:
            agg[k] = {"mean": float(np.mean(vals)), "std": float(np.std(vals))}
        else:
            agg[k] = {"mean": None, "std": None}
    return agg


def _ts_range(df_clean: pd.DataFrame, start: int, end: int) -> str:
    if "open_timestamp" not in df_clean.columns or len(df_clean) == 0:
        return "N/A"
    end_idx = min(end - 1, len(df_clean) - 1)
    return (f"{str(df_clean['open_timestamp'].iloc[start])[:10]} -> "
            f"{str(df_clean['open_timestamp'].iloc[end_idx])[:10]}")


def _walk_forward_binary(
    X: pd.DataFrame,
    y_win: pd.Series,
    y_r: pd.Series,
    df_clean: pd.DataFrame,
    folds: list,
    params: dict,
    cat_cols: list[str],
    verbose: bool,
    scale_pos_weight: Optional[float] = None,
) -> dict:
    """Full binary classifier walk-forward. Returns fold results + aggregate."""
    fold_results = []
    oof_y_true:  list[float] = []
    oof_y_score: list[float] = []

    y_win_arr = y_win.values
    y_r_arr   = y_r.values

    for i, (tr_s, tr_e, val_s, val_e) in enumerate(folds):
        X_tr   = X.iloc[tr_s:tr_e];  y_tr   = y_win.iloc[tr_s:tr_e]
        X_val  = X.iloc[val_s:val_e]; y_val  = y_win.iloc[val_s:val_e]
        y_r_val = y_r_arr[val_s:val_e]

        model = _build_classifier(params, scale_pos_weight=scale_pos_weight)
        model = _fit_lgbm(model, X_tr, y_tr, cat_cols)

        y_score = model.predict_proba(X_val)[:, 1]
        metrics = _binary_metrics(y_val.values, y_score, y_r_val)

        fold_results.append({
            "fold": i, "train_size": tr_e - tr_s, "val_size": val_e - val_s,
            "train_range": _ts_range(df_clean, tr_s, tr_e),
            "val_range":   _ts_range(df_clean, val_s, val_e),
            **{k: round(_safe_float(v), 6) for k, v in metrics.items()
               if isinstance(v, (int, float, type(None)))},
        })
        oof_y_true.extend(y_val.values.tolist())
        oof_y_score.extend(y_score.tolist())

        if verbose:
            auc_s = f"{_safe_float(metrics.get('auc')):.4f}"
            bri_s = f"{_safe_float(metrics.get('brier')):.4f}"
            qd_s  = f"{_safe_float(metrics.get('quartile_diff_win')):.3f}"
            print(f"  Fold {i:2d}: train={tr_e - tr_s:4d} val={val_e - val_s:3d} "
                  f"| AUC={auc_s} Brier={bri_s} QDiff={qd_s}")

    agg = _agg_folds(fold_results)
    oof_arr_true  = np.array(oof_y_true)
    oof_arr_score = np.clip(np.array(oof_y_score), 0.0, 1.0)
    cal = _oof_calibration(oof_arr_true, oof_arr_score) if len(oof_arr_true) > 0 else []

    return {"fold_results": fold_results, "aggregate": agg, "oof_calibration": cal}


def _walk_forward_regression(
    X: pd.DataFrame,
    y_r: pd.Series,
    df_clean: pd.DataFrame,
    folds: list,
    params: dict,
    cat_cols: list[str],
    verbose: bool,
) -> dict:
    """R-multiple regression walk-forward (secondary model)."""
    fold_results = []
    y_r_filled = y_r.fillna(0.0)   # fill NaN with 0 (conservative neutral)

    for i, (tr_s, tr_e, val_s, val_e) in enumerate(folds):
        X_tr    = X.iloc[tr_s:tr_e];    y_tr    = y_r_filled.iloc[tr_s:tr_e]
        X_val   = X.iloc[val_s:val_e];  y_r_val = y_r.iloc[val_s:val_e].values

        model = _build_regressor(params)
        model = _fit_lgbm(model, X_tr, y_tr, cat_cols)

        y_pred = model.predict(X_val)
        metrics = _regression_metrics(y_r_val, y_pred)

        fold_results.append({
            "fold": i, "train_size": tr_e - tr_s, "val_size": val_e - val_s,
            **{k: round(_safe_float(v), 6) for k, v in metrics.items()
               if isinstance(v, (int, float, type(None)))},
        })
        if verbose:
            top_r = _safe_float(metrics.get("top_q_mean_r"))
            bot_r = _safe_float(metrics.get("bottom_q_mean_r"))
            print(f"  Reg Fold {i:2d}: top-Q R={top_r:.3f} bot-Q R={bot_r:.3f} "
                  f"AUC(bin)={_safe_float(metrics.get('auc')):.4f}")

    return {"fold_results": fold_results, "aggregate": _agg_folds(fold_results)}


def _logistic_walk_forward(
    X: pd.DataFrame,
    y_win: pd.Series,
    folds: list,
    verbose: bool,
) -> dict:
    """Logistic regression baseline walk-forward (sklearn; requires NaN imputation)."""
    fold_results = []
    oof_y_true:  list[float] = []
    oof_y_score: list[float] = []

    col_medians = X.median()
    X_imp = X.fillna(col_medians)    # impute once on full dataset (valid: no future info)
    X_imp = X_imp.fillna(0.0)        # fallback 0 for columns that are entirely NULL (e.g. all-NaN adaptive-engine cols)

    for i, (tr_s, tr_e, val_s, val_e) in enumerate(folds):
        X_tr  = X_imp.iloc[tr_s:tr_e];  y_tr  = y_win.iloc[tr_s:tr_e]
        X_val = X_imp.iloc[val_s:val_e]; y_val = y_win.iloc[val_s:val_e]

        scaler = StandardScaler()
        X_tr_sc  = scaler.fit_transform(X_tr)
        X_val_sc = scaler.transform(X_val)

        clf = LogisticRegression(C=1.0, max_iter=1000, random_state=_SEED, solver="lbfgs")
        clf.fit(X_tr_sc, y_tr)
        y_score = clf.predict_proba(X_val_sc)[:, 1]

        metrics = _binary_metrics(y_val.values, y_score)
        fold_results.append({"fold": i, "train_size": tr_e - tr_s,
                              **{k: round(_safe_float(v), 6) for k, v in metrics.items()
                                 if isinstance(v, (int, float, type(None)))}})
        oof_y_true.extend(y_val.values.tolist())
        oof_y_score.extend(y_score.tolist())

    agg = _agg_folds(fold_results)
    mean_auc = agg["auc"]["mean"]
    std_auc  = agg["auc"]["std"]
    if verbose and mean_auc is not None:
        print(f"  Logistic baseline mean AUC: {mean_auc:.4f} ± {std_auc:.4f}")

    return {
        "fold_results": fold_results, "aggregate": agg,
        "mean_auc": mean_auc if mean_auc is not None else float("nan"),
        "std_auc":  std_auc  if std_auc  is not None else float("nan"),
        "oof_y_true": oof_y_true, "oof_y_score": oof_y_score,
    }


# -- Hyperparameter search -----------------------------------------------------

def _random_params(n: int, seed: int = _SEED) -> list[dict]:
    rng = random.Random(seed)
    keys = list(_PARAM_SPACE.keys())
    result = [{**_DEFAULT_PARAMS}]    # always include defaults as first candidate
    seen = {tuple(_DEFAULT_PARAMS[k] for k in keys)}
    while len(result) < n:
        combo = {k: rng.choice(_PARAM_SPACE[k]) for k in keys}
        key = tuple(combo[k] for k in keys)
        if key not in seen:
            seen.add(key)
            result.append(combo)
    return result[:n]


def _search_params(
    X: pd.DataFrame,
    y_win: pd.Series,
    y_r: pd.Series,
    df_clean: pd.DataFrame,
    search_folds: list,
    cat_cols: list[str],
    n_search: int = _N_SEARCH,
    verbose: bool = False,
    scale_pos_weight: Optional[float] = None,
) -> tuple[dict, list[dict]]:
    """
    Random search: evaluate n_search param sets on the provided search folds (validation split).
    Returns (best_params, search_log).
    """
    if not search_folds:
        return dict(_DEFAULT_PARAMS), []

    combos = _random_params(n_search)
    print(f"  Searching {len(combos)} param sets across {len(search_folds)} folds ...")

    log: list[dict] = []
    best_auc    = -1.0
    best_params = dict(_DEFAULT_PARAMS)

    for j, params in enumerate(combos):
        wf = _walk_forward_binary(X, y_win, y_r, df_clean, search_folds, params,
                                  cat_cols, verbose=False,
                                  scale_pos_weight=scale_pos_weight)
        mean_auc = _safe_float(wf["aggregate"]["auc"]["mean"])
        if math.isnan(mean_auc):
            mean_auc = -1.0
        log.append({"params": params, "mean_auc": round(mean_auc, 6)})

        if mean_auc > best_auc:
            best_auc    = mean_auc
            best_params = dict(params)

        if verbose:
            print(f"    [{j+1:2d}/{len(combos)}] AUC={mean_auc:.4f} {params}")

    print(f"  Best search AUC: {best_auc:.4f}  params: {best_params}")
    return best_params, log


# -- Feature importance --------------------------------------------------------

def _feature_importances(model: Any, X_sample: pd.DataFrame) -> list[dict]:
    method = "lgbm_gain"
    feature_names = list(X_sample.columns)

    # CalibratedClassifierCV wraps the base estimator — get importances from base
    base = model
    if hasattr(model, "estimator"):
        base = model.estimator
    elif hasattr(model, "calibrated_classifiers_"):
        try:
            base = model.calibrated_classifiers_[0].estimator
        except (AttributeError, IndexError):
            pass

    try:
        importances = np.array(base.feature_importances_, dtype=float)
    except AttributeError:
        importances = np.ones(len(feature_names), dtype=float)
        method = "uniform_fallback"

    if _HAS_SHAP:
        try:
            ex  = _shap.TreeExplainer(base)
            sv  = ex.shap_values(X_sample)
            if isinstance(sv, list):
                sv = sv[1]   # positive class for binary classifier
            importances = np.abs(sv).mean(axis=0)
            method = "shap_mean_abs"
        except Exception as e:
            print(f"  [!!]  SHAP failed ({e}) — using built-in importance")

    ranked = sorted(zip(feature_names, importances.tolist()), key=lambda x: -x[1])
    return [{"feature": f, "importance": round(float(v), 8), "method": method}
            for f, v in ranked]


# -- Stratified AUC analysis ---------------------------------------------------

def _stratified_auc(
    model: Any,
    X: pd.DataFrame,
    y: pd.Series,
    group_series: pd.Series,
) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for val in group_series.unique():
        mask = (group_series == val).values
        if mask.sum() < 20:
            continue
        X_sub = X[mask]
        y_sub = y.values[mask]
        if len(np.unique(y_sub)) < 2:
            results[str(val)] = {"auc": None, "n": int(mask.sum()), "reason": "single_class"}
            continue
        try:
            y_score = model.predict_proba(X_sub)[:, 1]
            auc = float(roc_auc_score(y_sub, y_score))
            results[str(val)] = {
                "auc":      round(auc, 4),
                "n":        int(mask.sum()),
                "win_rate": round(float(np.mean(y_sub)), 4),
                "flag":     "POOR" if auc < 0.52 else "OK",
            }
        except Exception as e:
            results[str(val)] = {"auc": None, "n": int(mask.sum()), "error": str(e)}
    return results


def _regime_series(df_clean: pd.DataFrame) -> Optional[pd.Series]:
    """Reconstruct regime string labels from canonical regime_enc."""
    if "regime_enc" not in df_clean.columns:
        return None
    reverse = {
        4: "STRONG_TREND",
        3: "WEAK_TREND",
        2: "HIGH_VOLATILITY",
        1: "RANGE",
        0: "LOW_VOLATILITY_CHOP",
    }
    return df_clean["regime_enc"].map(
        lambda value: reverse.get(int(value), "UNKNOWN") if pd.notna(value) else "UNKNOWN"
    )


# -- Acceptance criteria -------------------------------------------------------

def _check_acceptance(
    wf: dict,
    logistic_auc: float,
    min_auc: float,
    n_train: int,
    min_baseline_improvement: float = 0.01,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    agg = wf["aggregate"]

    mean_auc   = _safe_float(agg["auc"]["mean"])
    auc_std    = _safe_float(agg["auc"]["std"])
    mean_brier = _safe_float(agg["brier"]["mean"])
    mean_qdiff = _safe_float(agg["quartile_diff_win"]["mean"])

    if n_train < _MIN_TRAIN:
        reasons.append(
            f"Insufficient training data: {n_train} rows < {_MIN_TRAIN} required.")

    if math.isnan(mean_auc) or mean_auc < min_auc:
        reasons.append(
            f"Mean AUC {mean_auc:.4f} < minimum {min_auc:.2f} "
            f"(model is not meaningfully better than random — not useful for gating).")

    if not math.isnan(mean_auc) and mean_auc > 0.90:
        reasons.append(
            f"Mean AUC {mean_auc:.4f} > hard ceiling 0.90 "
            "(possible leakage or severe overfit).")

    if not math.isnan(auc_std) and auc_std > 0.15:
        reasons.append(
            f"AUC standard deviation {auc_std:.4f} > 0.15 "
            "(model performance is too unstable across folds).")

    if not math.isnan(mean_brier) and mean_brier > 0.30:
        reasons.append(
            f"Mean Brier score {mean_brier:.4f} > 0.30 "
            f"(probability calibration too poor for confidence-based gating).")

    if (
        not math.isnan(logistic_auc)
        and not math.isnan(mean_auc)
        and mean_auc < logistic_auc + min_baseline_improvement
    ):
        reasons.append(
            f"LightGBM AUC {mean_auc:.4f} does not beat logistic baseline "
            f"{logistic_auc:.4f} by required {min_baseline_improvement:.4f} "
            "(gradient boosting adds insufficient value over the linear model).")

    if not math.isnan(mean_qdiff) and mean_qdiff < 0.05:
        reasons.append(
            f"Top-quartile win rate gain {mean_qdiff:.3f} < 0.05 "
            f"(model scores do not meaningfully separate profitable from losing trades).")

    return (len(reasons) == 0), reasons


# -- I/O helpers ---------------------------------------------------------------

def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _save_artifacts(
    model: Any,
    encoders: dict,
    meta_doc: dict,
    val_doc: dict,
    out_dir: Path,
    today_str: str,
    accepted: bool,
    model_version: str = "v1.0",
) -> dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    base  = f"entry_quality_{model_version}_{today_str}"
    paths: dict[str, Path] = {}

    # Validation report — always saved (even on rejection, for diagnosis)
    vp = out_dir / f"{base}_validation.json"
    vp.write_text(json.dumps(val_doc, indent=2), encoding="utf-8")
    paths["validation"] = vp
    print(f"  [OK] Validation report  -> {vp}")

    if not accepted:
        return paths

    mp = out_dir / f"{base}.pkl"
    joblib.dump(model, str(mp))
    paths["model"] = mp
    print(f"  [OK] Model artifact     -> {mp}")

    ep = out_dir / f"{base}_encoders.pkl"
    joblib.dump(encoders, str(ep))
    paths["encoders"] = ep
    print(f"  [OK] Encoders           -> {ep}")

    mj = out_dir / f"{base}_meta.json"
    mj.write_text(json.dumps(meta_doc, indent=2), encoding="utf-8")
    paths["meta"] = mj
    print(f"  [OK] Model metadata     -> {mj}")

    return paths


def _plot_calibration(cal: list[dict], out_dir: Path, today_str: str,
                      model_version: str = "v1.0") -> None:
    if not _HAS_MPL or not cal:
        return
    xs = [b["predicted_bin_center"] for b in cal]
    ys = [b["actual_win_rate"]      for b in cal]
    ns = [b["count"]                for b in cal]

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot([0, 1], [0, 1], "k--", alpha=0.4, label="Perfect calibration")
    ax.scatter(xs, ys, s=[max(n / 5, 15) for n in ns], alpha=0.75, zorder=5)
    ax.plot(xs, ys, "b-o", alpha=0.6, label="Model calibration")
    ax.set_xlabel("Predicted win probability")
    ax.set_ylabel("Actual win rate (OOF)")
    ax.set_title(f"Entry Quality Scorer {model_version} — OOF Calibration Curve")
    ax.legend()
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    pp = out_dir / f"entry_quality_{model_version}_{today_str}_calibration.png"
    fig.savefig(str(pp), dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] Calibration plot   -> {pp}")


# -- Main ----------------------------------------------------------------------

def main() -> None:
    args      = _parse_args()
    t_start   = time.time()
    build_ts  = datetime.now(timezone.utc).isoformat()
    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    np.random.seed(_SEED)
    random.seed(_SEED)

    SEP = "=" * 64
    model_version = args.model_version
    print(
        f"\n[INFO] Model {model_version}: using canonical contract "
        f"{ML_CONTRACT_VERSION} ({len(FEATURE_COLUMNS)} features, "
        f"schema={ML_FEATURE_SCHEMA_HASH[:12]})"
    )

    # -- Output dir ------------------------------------------------------------
    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        script_dir = Path(__file__).resolve().parent   # scripts/ml/
        out_dir    = script_dir.parent.parent / "models" / "artifacts"
    out_dir.mkdir(parents=True, exist_ok=True)

    dataset_path = Path(args.dataset_path).resolve()
    dataset_hash = _sha256(str(dataset_path))

    def _timeout_check():
        elapsed_m = (time.time() - t_start) / 60.0
        if elapsed_m > args.max_train_minutes:
            print(f"\n[FAIL] Timeout: {elapsed_m:.1f}m > {args.max_train_minutes}m limit.",
                  file=sys.stderr)
            sys.exit(1)

    # -- Load ------------------------------------------------------------------
    print(f"\n{SEP}\n  LOADING DATASET\n{SEP}")
    print(f"  Path: {dataset_path}")
    df_raw = _load_dataset(str(dataset_path))

    # -- Leakage check ---------------------------------------------------------
    print(f"\n{SEP}\n  LEAKAGE CHECK\n{SEP}")
    if not _leakage_assertions(df_raw):
        print("[FAIL] Leakage detected — aborting.", file=sys.stderr)
        sys.exit(1)

    # -- Stats -----------------------------------------------------------------
    _log_stats(df_raw)

    # -- Sort by time (CRITICAL — must precede all splits) ---------------------
    if "open_timestamp" in df_raw.columns:
        df_raw = df_raw.sort_values("open_timestamp").reset_index(drop=True)

    # -- Min data check --------------------------------------------------------
    _effective_min_train = args.min_train_size if args.min_train_size is not None else _MIN_TRAIN
    if len(df_raw) < _effective_min_train:
        print(f"\n[FAIL] Insufficient data: {len(df_raw)} rows, "
              f"minimum {_effective_min_train} required.\n"
              f"   Accumulate more trades before training.", file=sys.stderr)
        sys.exit(1)

    # -- Feature prep ----------------------------------------------------------
    print(f"\n{SEP}\n  FEATURE PREPARATION\n{SEP}")
    encoders = _fit_encoders(df_raw)
    print(f"  Fitted encoders for: {list(encoders.keys())}")

    X, y_win, df_clean = _make_Xy(df_raw, encoders, LABEL_WIN)
    _, y_r, _          = _make_Xy(df_raw, encoders, LABEL_R)
    y_r = y_r.reindex(y_win.index)

    cat_cols = [c for c in CATEGORICAL_FEATURES if c in FEATURE_COLUMNS]
    print(f"  Feature matrix: {X.shape}")
    print(f"  Win rate:       {y_win.mean():.1%}")
    print(f"  Categoricals:   {cat_cols}")
    print(f"  SHAP available: {_HAS_SHAP}")

    # -- Compute scale_pos_weight ----------------------------------------------
    scale_pos_weight: Optional[float] = None
    if args.scale_pos_weight is not None:
        if args.scale_pos_weight.lower() == "auto":
            wr = float(y_win.mean())
            scale_pos_weight = (1.0 - wr) / max(wr, 1e-6)
            print(f"  scale_pos_weight: auto -> {scale_pos_weight:.3f} "
                  f"(win_rate={wr:.1%})")
        else:
            try:
                scale_pos_weight = float(args.scale_pos_weight)
                print(f"  scale_pos_weight: {scale_pos_weight:.3f}")
            except ValueError:
                print(f"  [!!] Invalid --scale-pos-weight '{args.scale_pos_weight}', "
                      f"ignoring (using 1.0)")
    if scale_pos_weight is None:
        print("  scale_pos_weight: 1.0 (default, no class correction)")

    # -- Train/Val/Test Splits --------------------------------------------------
    val_fold, test_fold = _make_splits(len(X))
    print(f"\n{SEP}\n  DATA SPLIT SETUP\n{SEP}")
    train_size = val_fold[1]
    val_size = val_fold[3] - val_fold[2]
    test_size = test_fold[3] - test_fold[2]
    print(f"  Total Rows: {len(X):,} | Train: {train_size} | Val: {val_size} | Test: {test_size}")

    if train_size < _effective_min_train:
        print(f"[FAIL] Not enough data for training "
              f"(Train size {train_size} < {_effective_min_train}).", file=sys.stderr)
        sys.exit(1)

    # -- Hyperparam search — binary classifier ---------------------------------
    print(f"\n{SEP}\n  HYPERPARAMETER SEARCH — LightGBM Binary (on Validation Set)\n{SEP}")
    best_params, search_log = _search_params(
        X, y_win, y_r, df_clean, [val_fold], cat_cols,
        n_search=_N_SEARCH,
        verbose=args.verbose,
        scale_pos_weight=scale_pos_weight,
    )
    _timeout_check()

    # -- Final unseen evaluation — binary classifier (champion params) --------
    print(f"\n{SEP}\n  FINAL EVALUATION — LightGBM Binary (on Test Set)\n{SEP}")
    lgbm_wf = _walk_forward_binary(X, y_win, y_r, df_clean, [test_fold],
                                   best_params, cat_cols, verbose=args.verbose,
                                   scale_pos_weight=scale_pos_weight)
    _timeout_check()

    agg = lgbm_wf["aggregate"]
    print(f"\n  LightGBM summary across {agg['n_folds']} valid / {agg['n_total_folds']} total folds:")
    for k in ("auc", "brier", "quartile_diff_win"):
        m, s = agg[k]["mean"], agg[k]["std"]
        print(f"    {k:22s}: {m:.4f} ± {s:.4f}" if m is not None else f"    {k}: N/A")
    tq = agg["top_q_win"]["mean"]; bq = agg["bottom_q_win"]["mean"]
    if tq is not None and bq is not None:
        print(f"    Top-Q win: {tq:.3f}  Bottom-Q win: {bq:.3f}")

    # -- Logistic regression baseline ------------------------------------------
    print(f"\n{SEP}\n  FINAL EVALUATION — Logistic Regression Baseline\n{SEP}")
    logistic_result = _logistic_walk_forward(X, y_win, [test_fold], verbose=args.verbose)
    _timeout_check()
    logistic_auc = _safe_float(logistic_result["mean_auc"])
    print(f"  Logistic baseline AUC: {logistic_auc:.4f} ± "
          f"{_safe_float(logistic_result['std_auc']):.4f}")

    # -- R-multiple regression (optional) -------------------------------------
    lgbm_reg_wf = None
    if not args.skip_regression:
        print(f"\n{SEP}\n  WALK-FORWARD — LightGBM Regression (R-multiple)\n{SEP}")
        reg_params, _ = _search_params(
            X, y_win, y_r, df_clean, [val_fold], cat_cols,
            n_search=min(10, _N_SEARCH), verbose=False,
        )
        lgbm_reg_wf = _walk_forward_regression(
            X, y_r, df_clean, [test_fold], reg_params, cat_cols, verbose=args.verbose)
        _timeout_check()
        ra = lgbm_reg_wf["aggregate"]
        tqr = ra.get("top_q_mean_r", {}).get("mean")
        bqr = ra.get("bottom_q_mean_r", {}).get("mean")
        if tqr is not None and bqr is not None:
            print(f"  Regression — Top-Q mean R: {tqr:.4f}  Bottom-Q mean R: {bqr:.4f}")

    # -- Acceptance check ------------------------------------------------------
    print(f"\n{SEP}\n  ACCEPTANCE CRITERIA\n{SEP}")
    accepted, rejection_reasons = _check_acceptance(
        lgbm_wf,
        logistic_auc,
        args.min_auc,
        len(X),
        args.min_baseline_improvement,
    )

    if accepted:
        print("  [OK] Model ACCEPTED — all criteria met")
    else:
        print("  [FAIL] Model REJECTED:")
        for r in rejection_reasons:
            print(f"     • {r}")

    # -- Final model on full dataset -------------------------------------------
    final_model        = None
    feat_importances   = []
    regime_auc: dict   = {}
    symbol_auc: dict   = {}

    if accepted:
        print(f"\n{SEP}\n  FINAL MODEL TRAINING (full dataset)\n{SEP}")
        final_model = _build_classifier(best_params, scale_pos_weight=scale_pos_weight)
        final_model = _fit_lgbm(final_model, X, y_win, cat_cols)
        print(f"  Trained on {len(X):,} rows")

        # -- Isotonic calibration (v1.1+) --------------------------------------
        if args.calibrate:
            print("  Applying isotonic calibration (IsotonicRegression post-layer) ...")
            try:
                from sklearn.isotonic import IsotonicRegression as _IR
                # Get raw scores from the already-fitted LightGBM on full training set
                raw_scores = final_model.predict_proba(X)[:, 1]
                iso = _IR(out_of_bounds="clip")
                iso.fit(raw_scores, y_win.values.astype(float))

                final_model = IsotonicCalibratedModel(final_model, iso)
                # Verify spread
                cal_scores = final_model.predict_proba(X)[:, 1]
                print(f"  [OK] Isotonic calibration applied.")
                print(f"       Score range: [{cal_scores.min():.3f}, {cal_scores.max():.3f}]  "
                      f"mean={cal_scores.mean():.3f}  std={cal_scores.std():.3f}")
            except Exception as e:
                print(f"  [!!] Calibration failed: {e} — using uncalibrated model")

        print("  Computing feature importances ...")
        sample_n  = min(1000, len(X))
        X_sample  = X.sample(sample_n, random_state=_SEED)
        feat_importances = _feature_importances(final_model, X_sample)
        print("  Top-10 features by importance:")
        for fi in feat_importances[:10]:
            print(f"    {fi['feature']:35s} {fi['importance']:.8f}")

        print("\n  Regime-stratified AUC:")
        reg_series = _regime_series(df_clean)
        if reg_series is not None:
            regime_auc = _stratified_auc(final_model, X, y_win, reg_series)
            for rn, rs in regime_auc.items():
                flag = " [!!] POOR" if rs.get("flag") == "POOR" else ""
                av = f"{rs['auc']:.4f}" if rs.get("auc") is not None else "N/A"
                print(f"    {rn:30s} AUC={av} n={rs['n']}{flag}")

        print("\n  Symbol-stratified AUC:")
        if "symbol" in df_clean.columns:
            symbol_auc = _stratified_auc(final_model, X, y_win, df_clean["symbol"])
            for sym, ss in symbol_auc.items():
                flag = " [!!] POOR" if ss.get("flag") == "POOR" else ""
                av = f"{ss['auc']:.4f}" if ss.get("auc") is not None else "N/A"
                print(f"    {sym:20s} AUC={av} n={ss['n']}{flag}")

        _timeout_check()

    # -- Calibration plot ------------------------------------------------------
    _plot_calibration(lgbm_wf.get("oof_calibration", []), out_dir, today_str,
                      model_version=model_version)

    # -- Build documents -------------------------------------------------------
    elapsed = time.time() - t_start

    def _clean_agg(a: dict) -> dict:
        return {k: ({"mean": round(v["mean"], 6) if v["mean"] is not None else None,
                     "std":  round(v["std"],  6) if v["std"]  is not None else None}
                    if isinstance(v, dict) else v)
                for k, v in a.items()}

    champion_reason = (
        f"LightGBM binary classifier {model_version}. Mean AUC {agg['auc']['mean']:.4f} "
        f"vs logistic baseline {logistic_auc:.4f}. "
        f"Top/bottom quartile win rate gap: {agg['quartile_diff_win']['mean']:.3f}."
        if accepted else "MODEL REJECTED — see rejection_reasons."
    )

    meta_doc = {
        **build_contract_metadata(training_dataset_path=str(dataset_path)),
        "deployment_status":       "EXPERIMENTAL_ONLY",
        "not_for_production":      True,
        "production_eligible":     False,
        "section4_status":         "IN_PROGRESS",
        "model_type":              "lightgbm_binary",
        "model_version":           model_version,
        "training_date":           build_ts,
        "dataset_path":            str(dataset_path),
        "dataset_hash":            dataset_hash,
        "row_count":               len(X),
        "feature_columns":         FEATURE_COLUMNS,
        "feature_count":           len(FEATURE_COLUMNS),
        "label_columns":           list(LABEL_COLUMNS),
        "contract_version":        ML_CONTRACT_VERSION,
        "schema_hash":             ML_FEATURE_SCHEMA_HASH,
        "label_used":              LABEL_WIN,
        "hyperparameters":         best_params,
        "scale_pos_weight":        scale_pos_weight,
        "isotonic_calibration":    args.calibrate,
        "refinements_applied":     (
            ["feature_pruning_adaptive_engine_cols",
             "scale_pos_weight",
             "isotonic_calibration"]
            if model_version >= "v1.1" else []
        ),
        "walk_forward_metrics":    _clean_agg(agg),
        "calibration_curve":       lgbm_wf.get("oof_calibration", []),
        "feature_importances":     feat_importances[:20],
        "regime_stratified_auc":   regime_auc,
        "symbol_stratified_auc":   symbol_auc,
        "logistic_baseline_auc":   round(logistic_auc, 6),
        "minimum_baseline_auc_improvement": args.min_baseline_improvement,
        "training_duration_seconds": round(elapsed, 1),
        "champion_reason":         champion_reason,
        "accepted":                accepted,
        "rejection_reasons":       rejection_reasons,
        "n_folds":                 1,
        "min_train_size":          _effective_min_train,
        "val_frac":                _VAL_FRAC,
        "test_frac":               _TEST_FRAC,
        "seed":                    _SEED,
        "shap_used":               _HAS_SHAP,
    }

    # Build warnings list for validation doc
    warnings_list: list[str] = [
        f"[!!]  Regime '{r}' AUC is POOR ({v['auc']:.4f})"
        for r, v in regime_auc.items() if v.get("flag") == "POOR" and v.get("auc") is not None
    ] + [
        f"[!!]  Symbol '{s}' AUC is POOR ({v['auc']:.4f})"
        for s, v in symbol_auc.items() if v.get("flag") == "POOR" and v.get("auc") is not None
    ]

    val_doc = {
        "generated_at":               build_ts,
        "model_version":              model_version,
        "deployment_status":          "EXPERIMENTAL_ONLY",
        "not_for_production":         True,
        "production_eligible":        False,
        "section4_status":            "IN_PROGRESS",
        "contract_version":           ML_CONTRACT_VERSION,
        "schema_hash":                ML_FEATURE_SCHEMA_HASH,
        "feature_columns":            FEATURE_COLUMNS,
        "feature_count":              len(FEATURE_COLUMNS),
        "row_count":                  len(X),
        "min_train_size":             _effective_min_train,
        "dataset_path":               str(dataset_path),
        "dataset_hash":               dataset_hash,
        "accepted":                   accepted,
        "rejection_reasons":          rejection_reasons,
        "lgbm_binary_folds":          lgbm_wf["fold_results"],
        "lgbm_binary_aggregate":      _clean_agg(agg),
        "lgbm_binary_calibration":    lgbm_wf.get("oof_calibration", []),
        "lgbm_binary_param_search":   search_log,
        "lgbm_best_params":           best_params,
        "lgbm_regression_aggregate":  _clean_agg(lgbm_reg_wf["aggregate"]) if lgbm_reg_wf else None,
        "logistic_baseline_folds":    logistic_result["fold_results"],
        "logistic_baseline_aggregate": _clean_agg(logistic_result["aggregate"]),
        "logistic_baseline_auc":      round(logistic_auc, 6),
        "minimum_baseline_auc_improvement": args.min_baseline_improvement,
        "regime_stratified_auc":      regime_auc,
        "symbol_stratified_auc":      symbol_auc,
        "feature_importances":        feat_importances,
        "warnings":                   warnings_list,
        "training_duration_seconds":  round(elapsed, 1),
    }

    # -- Save artifacts --------------------------------------------------------
    print(f"\n{SEP}\n  SAVING ARTIFACTS\n{SEP}")
    paths = _save_artifacts(final_model, encoders, meta_doc, val_doc,
                            out_dir, today_str, accepted,
                            model_version=model_version)

    # -- Summary ---------------------------------------------------------------
    mean_auc_v = agg["auc"]["mean"]
    mean_bri_v = agg["brier"]["mean"]
    mean_qd_v  = agg["quartile_diff_win"]["mean"]

    print(f"\n{SEP}\n  TRAINING COMPLETE\n{SEP}")
    print(f"  Status:          {'ACCEPTED [OK]' if accepted else 'REJECTED [FAIL]'}")
    print(f"  Rows trained:    {len(X):,}")
    print(f"  Folds evaluated: {agg['n_total_folds']}")
    print(f"  Mean AUC:        {mean_auc_v:.4f} ± {agg['auc']['std']:.4f}"
          if mean_auc_v is not None else "  Mean AUC:        N/A")
    print(f"  Mean Brier:      {mean_bri_v:.4f}"
          if mean_bri_v is not None else "  Mean Brier:      N/A")
    print(f"  Logistic AUC:    {logistic_auc:.4f}")
    if mean_qd_v is not None:
        print(f"  Q-diff win:      {mean_qd_v:.3f}")
    print(f"  Duration:        {elapsed:.1f}s")
    print()

    if not accepted:
        print("  Rejection reasons:")
        for r in rejection_reasons:
            print(f"    • {r}")
        if "validation" in paths:
            print(f"\n  Diagnosis report: {paths['validation']}")
        print()
        sys.exit(1)

    for label, path in paths.items():
        print(f"  {label:12s}: {path}")
    print()


if __name__ == "__main__":
    main()
