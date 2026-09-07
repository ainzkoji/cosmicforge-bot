"""
analyze_holdout.py -- Step 5E-6C: Holdout Shadow Analysis
==========================================================

Evaluates the trained ML entry model on the genuine holdout test set
(last 15% of the training dataset -- never seen during training or validation).

This is the best available proxy for real shadow analysis until sufficient
live scored entries accumulate (target: >=100).

Usage:
    python scripts/ml/analyze_holdout.py \
        --dataset models/datasets/training_20260322.parquet \
        --model models/artifacts/entry_quality_v1.0_20260322.pkl \
        --encoders models/artifacts/entry_quality_v1.0_20260322_encoders.pkl \
        --threshold 0.40

Output:
    - Full metric report to stdout
    - Per-symbol breakdown
    - Per-regime breakdown
    - Score-bin monotonicity table
    - Feature importance ranking
    - GO / NO-GO / INSUFFICIENT DATA verdict
    - Refinement flags for Step 5E-7A
"""

from __future__ import annotations

import argparse
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

import sys as _sys, os as _os
_bot_root = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", ".."))
if _bot_root not in _sys.path:
    _sys.path.insert(0, _bot_root)
from app.ml.calibrated_model import IsotonicCalibratedModel  # noqa: F401 — needed for joblib unpickling


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

TRAIN_FRAC = 0.70
VAL_FRAC = 0.15
# TEST = last 15%

META_COLS = {
    "trace_id", "position_id", "open_timestamp", "close_timestamp",
    "account_id", "bot_instance_id", "dataset_build_timestamp",
}
LABEL_PREFIX = "label_"

REGIME_COLS = [
    "regime_STRONG_TREND", "regime_WEAK_TREND", "regime_RANGE",
    "regime_HIGH_VOLATILITY", "regime_LOW_VOLATILITY_CHOP",
]

CATEGORICAL_COLS = {"symbol", "timeframe", "chosen_strategy"}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _pct(n: int, d: int, decimals: int = 1) -> str:
    if d == 0:
        return "N/A"
    return f"{100.0 * n / d:.{decimals}f}%"


def _fmt_float(v, decimals: int = 3, pct: bool = False) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "N/A"
    if pct:
        return f"{100.0 * v:.{decimals}f}%"
    return f"{v:.{decimals}f}"


def _win_rate(pnls: pd.Series) -> float | None:
    valid = pnls.dropna()
    if len(valid) == 0:
        return None
    return float((valid > 0).mean())


def _avg_pnl(pnls: pd.Series) -> float | None:
    valid = pnls.dropna()
    if len(valid) == 0:
        return None
    return float(valid.mean())


def _auc_simple(labels: np.ndarray, scores: np.ndarray) -> float:
    """Compute AUC-ROC without sklearn (avoid warning spam)."""
    try:
        from sklearn.metrics import roc_auc_score
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return float(roc_auc_score(labels, scores))
    except Exception:
        return float("nan")


def _brier(labels: np.ndarray, scores: np.ndarray) -> float:
    return float(np.mean((scores - labels) ** 2))


# ──────────────────────────────────────────────────────────────────────────────
# Load model + encoders
# ──────────────────────────────────────────────────────────────────────────────

def _load_artifacts(model_path: Path, encoders_path: Path):
    import joblib
    model = joblib.load(model_path)
    encoders = joblib.load(encoders_path)

    # Load feature list from companion meta JSON if it exists
    meta_path = model_path.parent / (model_path.stem + "_meta.json")
    feature_cols_from_meta = None
    if meta_path.exists():
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        feature_cols_from_meta = meta.get("feature_columns")

    return model, encoders, feature_cols_from_meta


# ──────────────────────────────────────────────────────────────────────────────
# Encode + score the test set
# ──────────────────────────────────────────────────────────────────────────────

def _encode_row(row: pd.Series, encoders: dict, feature_cols: list[str]) -> np.ndarray:
    """Encode a single row the same way scorer.py does it."""
    vec = []
    for col in feature_cols:
        val = row.get(col, np.nan)
        if col in CATEGORICAL_COLS:
            enc = encoders.get(col)
            if enc is not None and val is not None and not (isinstance(val, float) and np.isnan(val)):
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        encoded = enc.transform([[val]])[0][0]
                    vec.append(float(encoded))
                except Exception:
                    vec.append(0.0)
            else:
                vec.append(0.0)
        elif col == "side":
            # Binary: 1=LONG/BUY, 0=SHORT/SELL
            if isinstance(val, (int, float)) and not np.isnan(val):
                vec.append(float(val))
            else:
                s = str(val).upper()
                vec.append(1.0 if s in ("LONG", "BUY", "1") else 0.0)
        else:
            try:
                fval = float(val)
                vec.append(0.0 if np.isnan(fval) else fval)
            except (TypeError, ValueError):
                vec.append(0.0)  # impute as 0
    return np.array(vec, dtype=np.float32)


def _score_test_set(
    test: pd.DataFrame, model, encoders: dict, feature_cols: list[str], threshold: float
) -> pd.DataFrame:
    print("Scoring test set rows...")
    X = np.stack([_encode_row(row, encoders, feature_cols) for _, row in test.iterrows()])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        proba = model.predict_proba(X)
    scores = proba[:, 1]

    result = test.copy()
    result["ml_score"] = scores
    result["ml_pass"] = scores >= threshold
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Feature importance
# ──────────────────────────────────────────────────────────────────────────────

def _feature_importance(model, feature_cols: list[str]) -> pd.DataFrame | None:
    try:
        imp = model.feature_importances_
        df = pd.DataFrame({"feature": feature_cols, "importance": imp})
        return df.sort_values("importance", ascending=False).reset_index(drop=True)
    except AttributeError:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Calibration bins
# ──────────────────────────────────────────────────────────────────────────────

def _score_bins(scored: pd.DataFrame, n_bins: int = 10) -> pd.DataFrame:
    scored = scored.copy()
    scored["bin"] = pd.cut(scored["ml_score"], bins=n_bins, labels=False, right=True)
    rows = []
    for b in range(n_bins):
        group = scored[scored["bin"] == b]
        if len(group) == 0:
            continue
        wr = _win_rate(group["label_realized_pnl"])
        avg_pnl = _avg_pnl(group["label_realized_pnl"])
        rows.append({
            "bin": f"{b/n_bins:.1f}-{(b+1)/n_bins:.1f}",
            "count": len(group),
            "avg_score": group["ml_score"].mean(),
            "win_rate": wr,
            "avg_pnl": avg_pnl,
        })
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────────
# Monotonicity check
# ──────────────────────────────────────────────────────────────────────────────

def _check_monotonicity(bins_df: pd.DataFrame) -> tuple[bool, str]:
    wr_vals = bins_df["win_rate"].dropna().tolist()
    if len(wr_vals) < 2:
        return False, "insufficient bins"
    violations = sum(1 for i in range(len(wr_vals) - 1) if wr_vals[i] > wr_vals[i + 1])
    total_pairs = len(wr_vals) - 1
    mono_pct = 100.0 * (total_pairs - violations) / total_pairs
    is_mono = violations <= 1
    return is_mono, f"{mono_pct:.0f}% monotonic ({violations}/{total_pairs} violations)"


# ──────────────────────────────────────────────────────────────────────────────
# Per-group analysis
# ──────────────────────────────────────────────────────────────────────────────

def _group_analysis(
    scored: pd.DataFrame, group_col: str, threshold: float
) -> pd.DataFrame:
    rows = []
    for val, group in scored.groupby(group_col):
        if len(group) < 5:
            continue
        baseline_wr = _win_rate(group["label_realized_pnl"])
        passed = group[group["ml_pass"]]
        blocked = group[~group["ml_pass"]]
        pass_wr = _win_rate(passed["label_realized_pnl"]) if len(passed) > 0 else None
        block_wr = _win_rate(blocked["label_realized_pnl"]) if len(blocked) > 0 else None
        lift = (pass_wr - baseline_wr) if (pass_wr is not None and baseline_wr is not None) else None
        avg_score = group["ml_score"].mean()
        auc = _auc_simple(group["label_win"].values, group["ml_score"].values)
        rows.append({
            group_col: val,
            "count": len(group),
            "baseline_wr": baseline_wr,
            "pass_wr": pass_wr,
            "block_wr": block_wr,
            "lift_pp": lift,
            "block_rate": len(blocked) / len(group),
            "avg_ml_score": avg_score,
            "auc": auc if not np.isnan(auc) else None,
        })
    df = pd.DataFrame(rows)
    if len(df) > 0:
        df = df.sort_values("lift_pp", ascending=False, na_position="last")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Separation check: high vs low score groups
# ──────────────────────────────────────────────────────────────────────────────

def _separation_check(scored: pd.DataFrame) -> dict:
    top = scored[scored["ml_score"] >= 0.6]
    mid = scored[(scored["ml_score"] >= 0.4) & (scored["ml_score"] < 0.6)]
    low = scored[scored["ml_score"] < 0.4]

    return {
        "high_wr":  _win_rate(top["label_realized_pnl"]),
        "mid_wr":   _win_rate(mid["label_realized_pnl"]),
        "low_wr":   _win_rate(low["label_realized_pnl"]),
        "high_n":   len(top),
        "mid_n":    len(mid),
        "low_n":    len(low),
        "high_avg_pnl": _avg_pnl(top["label_realized_pnl"]),
        "low_avg_pnl":  _avg_pnl(low["label_realized_pnl"]),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Zero-variance / NULL feature audit
# ──────────────────────────────────────────────────────────────────────────────

def _feature_audit(test: pd.DataFrame, feature_cols: list[str]) -> dict:
    zero_var = []
    all_null = []
    high_null = []

    for col in feature_cols:
        s = test[col]
        if pd.api.types.is_numeric_dtype(s):
            null_frac = s.isna().mean()
            if null_frac == 1.0:
                all_null.append(col)
            elif null_frac >= 0.5:
                high_null.append((col, null_frac))
            elif s.dropna().std() == 0:
                zero_var.append((col, s.dropna().iloc[0] if len(s.dropna()) > 0 else None))
        else:
            if s.nunique() <= 1:
                zero_var.append((col, s.iloc[0] if len(s) > 0 else None))

    return {
        "zero_variance": zero_var,
        "all_null": all_null,
        "high_null": high_null,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Main analysis
# ──────────────────────────────────────────────────────────────────────────────

def run_analysis(
    dataset_path: Path,
    model_path: Path,
    encoders_path: Path,
    threshold: float = 0.40,
    top_n_features: int = 15,
) -> int:
    # ── Load dataset ──────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("STEP 5E-6C: ML HOLDOUT SHADOW ANALYSIS")
    print("=" * 70)

    df = pd.read_parquet(dataset_path)
    n = len(df)
    val_end = int(n * (TRAIN_FRAC + VAL_FRAC))
    test = df.iloc[val_end:].copy().reset_index(drop=True)

    label_cols = [c for c in df.columns if c.startswith(LABEL_PREFIX)]
    feature_cols = [c for c in df.columns if c not in META_COLS and not c.startswith(LABEL_PREFIX)]

    print(f"\nDataset   : {dataset_path.name}")
    print(f"Total rows: {n:,}   Train: {val_end:,}   Test: {len(test):,} (last {100*(1-TRAIN_FRAC-VAL_FRAC):.0f}%)")
    print(f"Test period: {test['open_timestamp'].min()} -> {test['open_timestamp'].max()}")
    print(f"Features  : {len(feature_cols)}")
    print(f"Threshold : {threshold}")

    # ── Feature audit ─────────────────────────────────────────────────────────
    audit = _feature_audit(test, feature_cols)

    # ── Load model ────────────────────────────────────────────────────────────
    print("\nLoading model artifacts...")
    model, encoders, meta_feature_cols = _load_artifacts(model_path, encoders_path)
    print(f"  Model   : {model_path.name}")
    print(f"  Encoders: {encoders_path.name}")

    # Use feature columns from model meta if available (handles v1.1 pruned set)
    if meta_feature_cols is not None and len(meta_feature_cols) != len(feature_cols):
        print(f"  Feature set: {len(meta_feature_cols)} (from model meta, "
              f"overrides dataset default of {len(feature_cols)})")
        # Validate all meta features exist in dataset
        missing = [c for c in meta_feature_cols if c not in test.columns]
        if missing:
            print(f"  [FAIL] Meta feature columns missing from dataset: {missing}")
            return 2
        feature_cols = meta_feature_cols
    elif meta_feature_cols is not None:
        feature_cols = meta_feature_cols

    # ── Score test set ────────────────────────────────────────────────────────
    scored = _score_test_set(test, model, encoders, feature_cols, threshold)
    print(f"  Scored  : {len(scored):,} rows")

    # ── Overall metrics ───────────────────────────────────────────────────────
    total = len(scored)
    passed = scored[scored["ml_pass"]]
    blocked = scored[~scored["ml_pass"]]

    baseline_wr = _win_rate(scored["label_realized_pnl"])
    pass_wr = _win_rate(passed["label_realized_pnl"])
    block_wr = _win_rate(blocked["label_realized_pnl"])
    lift = (pass_wr - baseline_wr) if (pass_wr is not None and baseline_wr is not None) else None

    baseline_pnl = _avg_pnl(scored["label_realized_pnl"])
    pass_pnl = _avg_pnl(passed["label_realized_pnl"])
    block_pnl = _avg_pnl(blocked["label_realized_pnl"])

    block_rate = len(blocked) / total

    false_pos = blocked[blocked["label_win"] == 1]
    fp_rate = len(false_pos) / len(blocked) if len(blocked) > 0 else 0.0

    auc_overall = _auc_simple(scored["label_win"].values, scored["ml_score"].values)
    brier_overall = _brier(scored["label_win"].values, scored["ml_score"].values)

    concordance_pairs = (
        ((scored["ml_score"] >= 0.5) & (scored["label_win"] == 1)).sum() +
        ((scored["ml_score"] < 0.5) & (scored["label_win"] == 0)).sum()
    )
    concordance = concordance_pairs / total

    # Score distribution
    sep = _separation_check(scored)

    # Bins
    bins_df = _score_bins(scored)
    is_mono, mono_str = _check_monotonicity(bins_df)

    # Per-symbol
    sym_df = _group_analysis(scored, "symbol", threshold)

    # Per-regime (reconstruct regime label from one-hot)
    for rc in REGIME_COLS:
        if rc in scored.columns:
            scored[rc] = scored[rc].fillna(0)
    regime_map = {
        "regime_STRONG_TREND": "STRONG_TREND",
        "regime_WEAK_TREND": "WEAK_TREND",
        "regime_RANGE": "RANGE",
        "regime_HIGH_VOLATILITY": "HIGH_VOLATILITY",
        "regime_LOW_VOLATILITY_CHOP": "LOW_VOLATILITY_CHOP",
    }
    scored["regime_label"] = "UNKNOWN"
    for col, label in regime_map.items():
        if col in scored.columns:
            scored.loc[scored[col] == 1, "regime_label"] = label

    regime_df = _group_analysis(scored, "regime_label", threshold)

    # Feature importance
    fi = _feature_importance(model, feature_cols)

    # ── Print report ──────────────────────────────────────────────────────────
    print("\n" + "-" * 70)
    print("SECTION 1: OVERALL METRICS")
    print("-" * 70)
    print(f"  Test set size            : {total:,} trades")
    print(f"  Baseline win rate        : {_fmt_float(baseline_wr, pct=True)}")
    print(f"  Baseline avg PnL         : {_fmt_float(baseline_pnl, 4)} USDT")
    print()
    print(f"  ML threshold             : {threshold:.2f}")
    print(f"  ML PASS count            : {len(passed):,}  ({_pct(len(passed), total)})")
    print(f"  ML BLOCK count           : {len(blocked):,}  ({_pct(len(blocked), total)})")
    print(f"  Block rate               : {_fmt_float(block_rate, pct=True)}")
    print()
    print(f"  PASS win rate            : {_fmt_float(pass_wr, pct=True)}")
    print(f"  BLOCK win rate           : {_fmt_float(block_wr, pct=True)}")
    print(f"  Precision lift (vs base) : {('+' if lift and lift >= 0 else '') + _fmt_float(lift, 3, pct=True)}")
    print()
    print(f"  PASS avg PnL             : {_fmt_float(pass_pnl, 4)} USDT")
    print(f"  BLOCK avg PnL            : {_fmt_float(block_pnl, 4)} USDT")
    print()
    print(f"  False positive rate      : {_fmt_float(fp_rate, pct=True)} (BLOCK but win)")
    print(f"  AUC-ROC                  : {_fmt_float(auc_overall, 4)}")
    print(f"  Brier score              : {_fmt_float(brier_overall, 4)} (lower = better)")
    print(f"  Concordance (score>0.5)  : {_fmt_float(concordance, pct=True)}")

    print("\n" + "-" * 70)
    print("SECTION 2: SCORE SEPARATION (HIGH / MID / LOW SCORE GROUPS)")
    print("-" * 70)
    print(f"  Score >= 0.60  (HIGH) : n={sep['high_n']:4d}  WR={_fmt_float(sep['high_wr'], pct=True)}  avgPnL={_fmt_float(sep['high_avg_pnl'], 4)}")
    print(f"  Score 0.40-0.60 (MID) : n={sep['mid_n']:4d}  WR={_fmt_float(sep['mid_wr'], pct=True)}  avgPnL=N/A")
    print(f"  Score < 0.40   (LOW)  : n={sep['low_n']:4d}  WR={_fmt_float(sep['low_wr'], pct=True)}  avgPnL={_fmt_float(sep['low_avg_pnl'], 4)}")
    separation_ok = (
        sep["high_wr"] is not None and sep["low_wr"] is not None
        and sep["high_wr"] > sep["low_wr"]
    )
    sep_label = "[OK]" if separation_ok else "[FAIL]"
    delta = (sep["high_wr"] - sep["low_wr"]) if separation_ok else None
    print(f"  {sep_label} High > Low separation: {_fmt_float(delta, 3, pct=True) if delta is not None else 'N/A'} gap")

    print("\n" + "-" * 70)
    print("SECTION 3: SCORE BIN MONOTONICITY")
    print("-" * 70)
    mono_label = "[OK]" if is_mono else "[FAIL]"
    print(f"  {mono_label} {mono_str}")
    print()
    print(f"  {'Bin':<12} {'Count':>6}  {'AvgScore':>9}  {'WinRate':>9}  {'AvgPnL':>9}")
    for _, row in bins_df.iterrows():
        wr_str = _fmt_float(row["win_rate"], pct=True) if row["win_rate"] is not None else "N/A"
        pnl_str = _fmt_float(row["avg_pnl"], 4) if row["avg_pnl"] is not None else "N/A"
        print(f"  {row['bin']:<12} {int(row['count']):>6}  {row['avg_score']:>9.3f}  {wr_str:>9}  {pnl_str:>9}")

    print("\n" + "-" * 70)
    print("SECTION 4: PER-SYMBOL BREAKDOWN")
    print("-" * 70)
    if len(sym_df) == 0:
        print("  No per-symbol data.")
    else:
        header = f"  {'Symbol':<12} {'N':>5}  {'BaseWR':>8}  {'PassWR':>8}  {'BlockWR':>8}  {'Lift':>8}  {'BlockRate':>10}  {'AUC':>7}"
        print(header)
        for _, row in sym_df.iterrows():
            lift_str = (("+" if row["lift_pp"] >= 0 else "") + f"{100*row['lift_pp']:.1f}pp") if row["lift_pp"] is not None else "N/A"
            print(
                f"  {str(row['symbol']):<12} {int(row['count']):>5}"
                f"  {_fmt_float(row['baseline_wr'], pct=True):>8}"
                f"  {_fmt_float(row['pass_wr'], pct=True):>8}"
                f"  {_fmt_float(row['block_wr'], pct=True):>8}"
                f"  {lift_str:>8}"
                f"  {_fmt_float(row['block_rate'], pct=True):>10}"
                f"  {_fmt_float(row['auc'], 3):>7}"
            )

    print("\n" + "-" * 70)
    print("SECTION 5: PER-REGIME BREAKDOWN")
    print("-" * 70)
    if len(regime_df) == 0:
        print("  No per-regime data (all regime columns may be zero-variance).")
    else:
        header = f"  {'Regime':<28} {'N':>5}  {'BaseWR':>8}  {'PassWR':>8}  {'BlockWR':>8}  {'Lift':>8}"
        print(header)
        for _, row in regime_df.iterrows():
            lift_str = (("+" if row["lift_pp"] >= 0 else "") + f"{100*row['lift_pp']:.1f}pp") if row["lift_pp"] is not None else "N/A"
            print(
                f"  {str(row['regime_label']):<28} {int(row['count']):>5}"
                f"  {_fmt_float(row['baseline_wr'], pct=True):>8}"
                f"  {_fmt_float(row['pass_wr'], pct=True):>8}"
                f"  {_fmt_float(row['block_wr'], pct=True):>8}"
                f"  {lift_str:>8}"
            )

    print("\n" + "-" * 70)
    print("SECTION 6: FEATURE IMPORTANCE (TOP {})".format(top_n_features))
    print("-" * 70)
    if fi is None:
        print("  Feature importance not available for this model type.")
    else:
        total_imp = fi["importance"].sum()
        for _, row in fi.head(top_n_features).iterrows():
            bar = "#" * int(40 * row["importance"] / fi["importance"].max())
            pct_str = f"{100*row['importance']/total_imp:.1f}%"
            print(f"  {row['feature']:<35} {pct_str:>6}  {bar}")

    print("\n" + "-" * 70)
    print("SECTION 7: FEATURE QUALITY AUDIT")
    print("-" * 70)
    if audit["all_null"]:
        print(f"  [!!] All-NULL features ({len(audit['all_null'])}): {audit['all_null']}")
    if audit["zero_variance"]:
        print(f"  [!!] Zero-variance features ({len(audit['zero_variance'])}):")
        for col, val in audit["zero_variance"]:
            print(f"       {col} = {val}")
    if audit["high_null"]:
        print(f"  [!!] High-null features (>=50%):")
        for col, frac in audit["high_null"]:
            print(f"       {col}: {100*frac:.1f}% NULL")
    dead_count = len(audit["all_null"]) + len(audit["zero_variance"])
    print(f"\n  Dead features (all-null + zero-variance): {dead_count} of {len(feature_cols)}")
    print(f"  Useful features: {len(feature_cols) - dead_count} of {len(feature_cols)}")

    # ── GO / NO-GO assessment ─────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("STEP 5E-6C: GO / NO-GO VERDICT")
    print("=" * 70)

    checks = []

    def check(name: str, passed: bool, detail: str) -> bool:
        marker = "[OK]  " if passed else "[FAIL]"
        print(f"  {marker} {name}: {detail}")
        checks.append(passed)
        return passed

    check(
        "Sample size sufficient (>=200 holdout rows)",
        total >= 200,
        f"{total:,} rows"
    )
    check(
        "Score separation: HIGH > LOW win rate",
        separation_ok,
        f"HIGH={_fmt_float(sep['high_wr'], pct=True)} vs LOW={_fmt_float(sep['low_wr'], pct=True)}"
    )
    check(
        "Precision lift > 0 pp",
        lift is not None and lift > 0.0,
        _fmt_float(lift, 3, pct=True) if lift is not None else "N/A"
    )
    check(
        "Block rate <= 40%",
        block_rate <= 0.40,
        _fmt_float(block_rate, pct=True)
    )
    check(
        "False positive rate <= 30%",
        fp_rate <= 0.30,
        _fmt_float(fp_rate, pct=True)
    )
    check(
        "Score bins monotonic",
        is_mono,
        mono_str
    )
    check(
        "AUC > 0.55 on holdout",
        not np.isnan(auc_overall) and auc_overall > 0.55,
        _fmt_float(auc_overall, 4)
    )
    check(
        "Dead features <= 30% of feature set",
        dead_count / len(feature_cols) <= 0.30,
        f"{dead_count}/{len(feature_cols)} = {_pct(dead_count, len(feature_cols))}"
    )

    n_pass = sum(checks)
    n_total_checks = len(checks)
    all_pass = all(checks)
    core_checks = checks[1:4]  # separation, lift, block rate
    core_pass = all(core_checks)

    print()
    if total < 50:
        verdict = "INSUFFICIENT DATA"
        code = 2
    elif all_pass:
        verdict = "GO"
        code = 0
    elif core_pass:
        verdict = "CONDITIONAL GO (minor issues -- refine before promoting)"
        code = 0
    else:
        verdict = "NO-GO (refine model before promotion)"
        code = 1

    print(f"  VERDICT: [{verdict}]  ({n_pass}/{n_total_checks} checks passed)")

    # ── Strengths / Weaknesses for 5E-7A ─────────────────────────────────────
    print("\n" + "-" * 70)
    print("STEP 5E-6C -> 5E-7A: STRENGTHS AND WEAKNESSES")
    print("-" * 70)

    print("\n  STRENGTHS:")
    strengths = []
    if separation_ok and delta and delta > 0.05:
        strengths.append(f"Clear score separation: +{100*delta:.1f}pp gap between HIGH and LOW score groups")
    if lift is not None and lift > 0.02:
        strengths.append(f"Positive precision lift: {100*lift:+.1f}pp over baseline win rate")
    if block_rate <= 0.25:
        strengths.append(f"Conservative block rate ({100*block_rate:.1f}%): not over-filtering")
    if fp_rate <= 0.20:
        strengths.append(f"Low false positive rate ({100*fp_rate:.1f}%): blocked trades mostly correct")
    if not np.isnan(auc_overall) and auc_overall > 0.60:
        strengths.append(f"AUC {auc_overall:.4f}: model has real discriminative power")
    if is_mono:
        strengths.append("Score bins are monotonically ordered (higher score = higher win rate)")
    for s in strengths:
        print(f"    [+] {s}")
    if not strengths:
        print("    None identified -- model needs refinement before any strengths can be claimed")

    print("\n  WEAKNESSES:")
    weaknesses = []
    if dead_count > 0:
        null_names = audit["all_null"]
        zv_names = [c for c, _ in audit["zero_variance"]]
        weaknesses.append(
            f"{dead_count} dead features consuming model capacity: {null_names + zv_names}"
        )
    if not separation_ok or (delta is not None and delta < 0.03):
        weaknesses.append("Weak score separation: model struggles to distinguish winners from losers")
    if lift is None or lift <= 0:
        weaknesses.append("No positive precision lift: filtering would not improve realized win rate")
    if not is_mono:
        weaknesses.append(f"Non-monotonic score bins ({mono_str}): score not a reliable quality signal")
    if brier_overall > 0.25:
        weaknesses.append(f"High Brier score ({brier_overall:.3f}): model is poorly calibrated")
    if fp_rate > 0.35:
        weaknesses.append(f"High false positive rate ({100*fp_rate:.1f}%): model blocks too many actual winners")
    if len(sym_df) > 0:
        worst_sym = sym_df.iloc[-1]
        if worst_sym["lift_pp"] is not None and worst_sym["lift_pp"] < -0.03:
            weaknesses.append(f"Model hurts {worst_sym['symbol']} ({100*worst_sym['lift_pp']:+.1f}pp lift): symbol-specific failure")
    if baseline_wr is not None and baseline_wr < 0.40:
        weaknesses.append(f"Low baseline win rate ({100*baseline_wr:.1f}%): underlying strategy may need tuning independent of ML")
    for w in weaknesses:
        print(f"    [-] {w}")
    if not weaknesses:
        print("    None significant")

    # ── 5E-7A Refinement flags ─────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("STEP 5E-7A: REFINEMENT PRIORITIES")
    print("=" * 70)

    high_priority = []
    medium_priority = []
    optional = []

    if dead_count > 3:
        high_priority.append(
            f"PRUNE dead features: remove {len(audit['all_null'])} all-NULL + {len(audit['zero_variance'])} zero-variance "
            f"cols from feature set before retraining"
        )
    if lift is None or lift <= 0:
        high_priority.append(
            "LABEL REVIEW: binary win/loss label may be too coarse. Consider using "
            "realized_pnl > threshold (e.g. > 0.5 USDT) as positive label to create cleaner signal"
        )
    if not is_mono:
        high_priority.append(
            "MONOTONICITY FIX: apply Isotonic Regression or Platt Scaling post-calibration layer "
            "to enforce monotonic score-to-win-rate mapping"
        )
    if brier_overall > 0.25:
        high_priority.append(
            "CALIBRATION: add CalibratedClassifierCV(method='isotonic') wrapper after LightGBM "
            "training to improve probability calibration"
        )

    # Regime coverage
    regimes_seen = scored["regime_label"].unique().tolist()
    regimes_missing = [r for r in ["STRONG_TREND", "WEAK_TREND", "RANGE", "HIGH_VOLATILITY", "LOW_VOLATILITY_CHOP"]
                       if r not in regimes_seen]
    if regimes_missing:
        high_priority.append(
            f"REGIME COVERAGE: {len(regimes_missing)} regimes absent from dataset: {regimes_missing}. "
            f"Model cannot generalize to these regimes. Need live data from diversified market conditions."
        )

    # Feature additions
    if len(audit["all_null"]) >= 5:
        medium_priority.append(
            "ADAPTIVE ENGINE FIELDS: aggressiveness_score, size_multiplier, rolling_win_rate etc. "
            "are all NULL in backfill data. These are live-only fields. Once live data accumulates, "
            "these features may add real signal -- do not drop permanently, flag for re-evaluation."
        )
    if "price_spread_pct" in [c for c, _ in audit["zero_variance"]]:
        medium_priority.append(
            "PRICE SPREAD: price_spread_pct is always 0 (backfill limitation). On live data "
            "this field is non-zero and may provide liquidity signal. Retain in schema."
        )

    medium_priority.append(
        "CLASS IMBALANCE: baseline win rate is ~32-40%. Consider class_weight='balanced' "
        "in LightGBM (scale_pos_weight) to reduce false negative rate on winners."
    )
    medium_priority.append(
        "THRESHOLD TUNING: evaluate precision/recall tradeoff at thresholds 0.30, 0.35, 0.40, 0.45, 0.50. "
        "The optimal threshold may not be 0.40 on live data."
    )

    optional.append(
        "REGIME-STRATIFIED TRAINING: if regime distribution becomes more varied with live data, "
        "consider separate calibrators per regime (not separate models -- insufficient data)"
    )
    optional.append(
        "FEATURE INTERACTIONS: LightGBM handles these implicitly, but explicit features like "
        "'adx * regime_confidence' or 'consensus_gap * drawdown_pct' may improve fit"
    )

    if len(high_priority) > 0:
        print("\n  HIGH PRIORITY (must fix before retraining):")
        for i, item in enumerate(high_priority, 1):
            print(f"\n    {i}. {item}")

    if len(medium_priority) > 0:
        print("\n  MEDIUM PRIORITY (recommended for next training cycle):")
        for i, item in enumerate(medium_priority, 1):
            print(f"\n    {i}. {item}")

    if len(optional) > 0:
        print("\n  OPTIONAL (after >= 500 live trades):")
        for i, item in enumerate(optional, 1):
            print(f"\n    {i}. {item}")

    print("\n" + "=" * 70)
    print(f"DATA CAVEAT: This analysis uses holdout backfill data (same 90-day simulation")
    print(f"as training). AUC inflation artifact applies. Treat as structural validation,")
    print(f"not a live performance guarantee. Re-run with real live data once available.")
    print("=" * 70)
    print()

    return code


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Step 5E-6C: Holdout shadow analysis for ML entry quality model."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("models/datasets/training_20260322.parquet"),
        help="Path to training parquet"
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=Path("models/artifacts/entry_quality_v1.0_20260322.pkl"),
    )
    parser.add_argument(
        "--encoders",
        type=Path,
        default=Path("models/artifacts/entry_quality_v1.0_20260322_encoders.pkl"),
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.40,
    )
    args = parser.parse_args()
    return run_analysis(args.dataset, args.model, args.encoders, args.threshold)


if __name__ == "__main__":
    import sys
    sys.exit(main())
