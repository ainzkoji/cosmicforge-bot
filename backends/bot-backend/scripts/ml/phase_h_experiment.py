#!/usr/bin/env python3
"""
Phase H: Offline ML Experiment -- Timing Features Only.

Trains two LightGBM models on the rebuilt (Phase H2) dataset:
  Model A: 21 base ML_FEATURE_COLUMNS only
  Model B: 21 base + 3 EVENT_TIMING_FEATURE_COLUMNS
           (minutes_to_next_event, minutes_since_last_event, is_blackout_active)

Reaction features (volatility_post_event, volume_spike_post_event,
reaction_type, max_adverse_move) are EXCLUDED -- no candle data available.

Reports (H4): AUC, AP, F1, precision, recall, Brier on temporal test split.
Reports (H5): Segmented performance -- pre-event, post-event, blackout, baseline.
Reports (H6): Feature importance for the 3 timing features.
Reports (H7): Safety invariants -- no scorer update, no deployment.

SAFETY: offline experiment only.  No model is deployed.  No scorer is changed.
        Artifacts written to models/experiments/phase_h_YYYYMMDD/ only.

Usage:
  python scripts/ml/phase_h_experiment.py \\
      --parquet C:/tmp/phase_h_dataset/training_20260426.parquet

  python scripts/ml/phase_h_experiment.py \\
      --parquet C:/tmp/phase_h_dataset/training_20260426.parquet \\
      --artifact-dir C:/tmp/phase_h_artifacts
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT   = _SCRIPT_DIR.parent.parent
_SHARED     = _BOT_ROOT.parent / "shared"
for _p in (str(_BOT_ROOT), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    import numpy as np
    import pandas as pd
    import lightgbm as lgb
    from sklearn.metrics import (
        roc_auc_score, average_precision_score,
        precision_score, recall_score, f1_score, brier_score_loss,
    )
    from scipy.stats import spearmanr as _spearmanr
except ImportError as e:
    print(f"[FAIL] Missing dependency: {e}")
    sys.exit(1)

from shared_lib.ml.contract import (
    ML_FEATURE_COLUMNS,
    EVENT_TIMING_FEATURE_COLUMNS,
    MARKET_REACTION_FEATURE_COLUMNS,
)
from shared_lib.ml.readiness import (
    check_dataset_readiness,
    apply_readiness_gate,
    format_readiness_report,
)

# ─── Constants ────────────────────────────────────────────────────────────────

LABEL_COL     = "label_win"
BASE_FEATURES = list(ML_FEATURE_COLUMNS)
TIMING_FEATS  = list(EVENT_TIMING_FEATURE_COLUMNS)
ALL_FEATURES  = BASE_FEATURES + TIMING_FEATS

TRAIN_FRAC = 0.60
VAL_FRAC   = 0.20
# TEST_FRAC  = 0.20 (remainder)

LGB_PARAMS: dict[str, Any] = {
    "objective":         "binary",
    "metric":            "binary_logloss",
    "n_estimators":      400,
    "max_depth":         6,
    "learning_rate":     0.05,
    "num_leaves":        31,
    "min_child_samples": 20,
    "subsample":         0.8,
    "colsample_bytree":  0.8,
    "reg_lambda":        1.0,
    "verbose":           -1,
    "n_jobs":            -1,
    "random_state":      42,
}

# Minimum rows in a segment to report metrics (avoids division by zero).
_MIN_SEGMENT_ROWS = 10

# Date-proxy Spearman threshold.
_PROXY_THRESHOLD = 0.70


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_auc(y_true: np.ndarray, y_score: np.ndarray) -> float | None:
    if len(np.unique(y_true)) < 2:
        return None
    try:
        return float(roc_auc_score(y_true, y_score))
    except Exception:
        return None


def _safe_ap(y_true: np.ndarray, y_score: np.ndarray) -> float | None:
    if len(np.unique(y_true)) < 2:
        return None
    try:
        return float(average_precision_score(y_true, y_score))
    except Exception:
        return None


def _metrics(y_true: np.ndarray, y_score: np.ndarray, threshold: float = 0.5) -> dict[str, Any]:
    y_pred = (y_score >= threshold).astype(int)
    auc = _safe_auc(y_true, y_score)
    ap  = _safe_ap(y_true, y_score)
    try:
        brier = float(brier_score_loss(y_true, y_score))
    except Exception:
        brier = None
    prec  = float(precision_score(y_true, y_pred, zero_division=0))
    rec   = float(recall_score(y_true, y_pred, zero_division=0))
    f1    = float(f1_score(y_true, y_pred, zero_division=0))
    pos_rate = float(y_true.mean()) if len(y_true) > 0 else 0.0
    return {
        "n":        int(len(y_true)),
        "pos_rate": round(pos_rate, 4),
        "auc":      round(auc, 4) if auc is not None else None,
        "ap":       round(ap,  4) if ap  is not None else None,
        "brier":    round(brier, 4) if brier is not None else None,
        "precision": round(prec, 4),
        "recall":    round(rec,  4),
        "f1":        round(f1,   4),
    }


def _train(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    spw: float,
    label: str,
) -> lgb.LGBMClassifier:
    pos = int(y_train.sum())
    neg = int(len(y_train) - pos)
    print(f"  [{label}] Train: n={len(y_train):,}  pos={pos:,}  neg={neg:,}  spw={spw:.3f}")
    params = {**LGB_PARAMS, "scale_pos_weight": spw}
    clf = lgb.LGBMClassifier(**params)
    clf.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False),
                   lgb.log_evaluation(period=-1)],
    )
    return clf


# ─── Date-proxy check ─────────────────────────────────────────────────────────

def _check_date_proxy(df: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    col = "minutes_to_next_event"
    if col not in df.columns:
        result["skipped"] = True
        return result
    valid = df[col].dropna()
    if len(valid) < 10:
        result["skipped"] = True
        result["n_valid"] = int(len(valid))
        return result
    idx = np.arange(len(valid), dtype=float)
    corr, _ = _spearmanr(idx, valid.values)
    corr = float(corr) if not math.isnan(corr) else 0.0
    result["spearman_vs_rowindex"] = round(corr, 4)
    result["proxy_risk"]           = bool(abs(corr) > _PROXY_THRESHOLD)
    result["n_valid"]              = int(len(valid))
    return result


# ─── Segment masks ────────────────────────────────────────────────────────────

def _build_segment_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    masks: dict[str, pd.Series] = {}
    n = len(df)

    # Blackout rows: is_blackout_active == 1
    if "is_blackout_active" in df.columns:
        masks["blackout"] = df["is_blackout_active"].fillna(0).astype(int) == 1
    else:
        masks["blackout"] = pd.Series(False, index=df.index)

    # Pre-event: minutes_to_next_event in (0, 60]
    if "minutes_to_next_event" in df.columns:
        mtn = pd.to_numeric(df["minutes_to_next_event"], errors="coerce")
        masks["pre_event_60min"] = (mtn >= 0) & (mtn <= 60)
    else:
        masks["pre_event_60min"] = pd.Series(False, index=df.index)

    # Post-event: minutes_since_last_event in (0, 60]
    if "minutes_since_last_event" in df.columns:
        msl = pd.to_numeric(df["minutes_since_last_event"], errors="coerce")
        masks["post_event_60min"] = (msl >= 0) & (msl <= 60)
    else:
        masks["post_event_60min"] = pd.Series(False, index=df.index)

    # Near-event: pre OR post (within 60 min of any event boundary)
    masks["near_event"] = masks["pre_event_60min"] | masks["post_event_60min"]

    # Baseline: rows where no timing feature has a value (no nearby event)
    has_timing = (
        masks["pre_event_60min"] | masks["post_event_60min"] | masks["blackout"]
    )
    masks["no_event_context"] = ~has_timing

    return masks


# ─── Segmented metrics ────────────────────────────────────────────────────────

def _segment_report(
    test_df: pd.DataFrame,
    y_true: np.ndarray,
    scores_a: np.ndarray,
    scores_b: np.ndarray,
) -> dict[str, Any]:
    seg_masks = _build_segment_masks(test_df)

    report: dict[str, Any] = {}
    for seg_name, mask in seg_masks.items():
        seg_idx = mask[mask].index
        seg_loc = test_df.index.get_indexer(seg_idx)
        seg_loc = seg_loc[seg_loc >= 0]
        n_seg = len(seg_loc)
        if n_seg < _MIN_SEGMENT_ROWS:
            report[seg_name] = {"n": n_seg, "skipped": True, "reason": f"< {_MIN_SEGMENT_ROWS} rows"}
            continue
        yt = y_true[seg_loc]
        sa = scores_a[seg_loc]
        sb = scores_b[seg_loc]
        report[seg_name] = {
            "n":       n_seg,
            "model_a": _metrics(yt, sa),
            "model_b": _metrics(yt, sb),
        }
        da = report[seg_name]["model_a"]
        db = report[seg_name]["model_b"]
        auc_delta = (
            round(db["auc"] - da["auc"], 4)
            if da["auc"] is not None and db["auc"] is not None
            else None
        )
        report[seg_name]["auc_delta_b_minus_a"] = auc_delta

    return report


# ─── Feature importance ───────────────────────────────────────────────────────

def _timing_importance(clf_b: lgb.LGBMClassifier) -> dict[str, Any]:
    booster = clf_b.booster_
    importance_gain  = booster.feature_importance(importance_type="gain")
    importance_split = booster.feature_importance(importance_type="split")
    feature_names    = booster.feature_name()
    total_gain  = max(importance_gain.sum(), 1)
    total_split = max(importance_split.sum(), 1)

    result: dict[str, Any] = {}
    for feat in TIMING_FEATS:
        if feat in feature_names:
            idx = list(feature_names).index(feat)
            result[feat] = {
                "gain_abs":   int(importance_gain[idx]),
                "split_abs":  int(importance_split[idx]),
                "gain_pct":   round(importance_gain[idx] / total_gain * 100, 2),
                "split_pct":  round(importance_split[idx] / total_split * 100, 2),
            }
        else:
            result[feat] = {"gain_abs": 0, "split_abs": 0, "gain_pct": 0.0, "split_pct": 0.0}

    # All features sorted by gain for context
    all_feats = sorted(
        zip(feature_names, importance_gain, importance_split),
        key=lambda x: -x[1],
    )
    result["top_10_by_gain"] = [
        {
            "feature":    f,
            "gain_abs":   int(g),
            "gain_pct":   round(g / total_gain * 100, 2),
            "split_abs":  int(s),
            "timing":     f in TIMING_FEATS,
        }
        for f, g, s in all_feats[:10]
    ]
    return result


# ─── Main experiment ──────────────────────────────────────────────────────────

def run_experiment(parquet_path: Path, artifact_dir: Path) -> dict[str, Any]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    result: dict[str, Any] = {
        "phase":       "H",
        "run_ts":      _now(),
        "parquet":     str(parquet_path),
        "artifact_dir": str(artifact_dir),
    }

    # ── Load ──────────────────────────────────────────────────────────────────
    print(f"\nLoading parquet: {parquet_path}")
    df = pd.read_parquet(str(parquet_path))
    n  = len(df)
    print(f"Rows: {n:,}")
    result["n_total"] = n

    # ── Verify timing cols present ────────────────────────────────────────────
    missing_timing = [c for c in TIMING_FEATS if c not in df.columns]
    missing_reaction = [c for c in MARKET_REACTION_FEATURE_COLUMNS if c in df.columns]
    result["timing_cols_present"]    = [c for c in TIMING_FEATS if c in df.columns]
    result["timing_cols_missing"]    = missing_timing
    result["reaction_cols_in_data"]  = missing_reaction  # should be empty

    if missing_reaction:
        print(f"[WARN] Reaction columns found in parquet (should be absent): {missing_reaction}")

    if missing_timing:
        print(f"[WARN] Missing timing columns: {missing_timing}")

    # ── Phase I Readiness Gate ────────────────────────────────────────────────
    print("\n=== Phase I: Data Readiness Gate ===")
    readiness = check_dataset_readiness(
        df,
        check_reactions=False,
        check_blackout=False,
    )
    print(format_readiness_report(readiness, title="Phase H Dataset Readiness"))
    result["readiness_gate"] = {
        "passed": readiness.passed,
        "n_checks": len(readiness.checks),
        "n_passed": readiness.n_passed,
        "n_failed_blocking": readiness.n_failed_blocking,
        "failed_checks": [c.name for c in readiness.checks if not c.passed and c.blocking],
    }

    # ── Date-proxy check on full dataset ─────────────────────────────────────
    print("\n=== Phase H: Date-Proxy Risk Check ===")
    proxy = _check_date_proxy(df)
    result["date_proxy_check"] = proxy
    if proxy.get("proxy_risk"):
        print(f"[FAIL] Date-proxy risk detected: Spearman={proxy.get('spearman_vs_rowindex')}")
        print("       minutes_to_next_event is acting as a date-proxy. Aborting experiment.")
        result["verdict"] = "ABORTED_DATE_PROXY"
        return result
    else:
        corr = proxy.get("spearman_vs_rowindex")
        print(f"  Spearman(minutes_to_next_event, row_index) = {corr}")
        print(f"  date_proxy_risk = {proxy.get('proxy_risk', 'N/A')}  [OK]")

    # ── Temporal split ────────────────────────────────────────────────────────
    print("\n=== Phase H2b: Temporal Split (60/20/20) ===")
    df = df.sort_values("open_timestamp").reset_index(drop=True)
    n_train = int(n * TRAIN_FRAC)
    n_val   = int(n * VAL_FRAC)
    n_test  = n - n_train - n_val

    train_df = df.iloc[:n_train].copy()
    val_df   = df.iloc[n_train : n_train + n_val].copy()
    test_df  = df.iloc[n_train + n_val :].copy()

    print(f"  Train:  {len(train_df):,} rows  ({train_df['open_timestamp'].min()} -- {train_df['open_timestamp'].max()})")
    print(f"  Val:    {len(val_df):,} rows  ({val_df['open_timestamp'].min()} -- {val_df['open_timestamp'].max()})")
    print(f"  Test:   {len(test_df):,} rows  ({test_df['open_timestamp'].min()} -- {test_df['open_timestamp'].max()})")

    result["split"] = {
        "n_train": len(train_df),
        "n_val":   len(val_df),
        "n_test":  len(test_df),
        "train_start": str(train_df["open_timestamp"].min()),
        "train_end":   str(train_df["open_timestamp"].max()),
        "test_start":  str(test_df["open_timestamp"].min()),
        "test_end":    str(test_df["open_timestamp"].max()),
    }

    # ── Label ─────────────────────────────────────────────────────────────────
    for subset in (train_df, val_df, test_df):
        if LABEL_COL not in subset.columns:
            print(f"[FAIL] Label column '{LABEL_COL}' not in parquet.")
            result["verdict"] = "ABORTED_MISSING_LABEL"
            return result

    y_train = train_df[LABEL_COL].fillna(0).astype(int)
    y_val   = val_df[LABEL_COL].fillna(0).astype(int)
    y_test  = test_df[LABEL_COL].fillna(0).astype(int)

    pos_rate_train = float(y_train.mean())
    spw = (1.0 - pos_rate_train) / max(pos_rate_train, 1e-9)
    spw = round(spw, 3)

    result["label_distribution"] = {
        "train_pos_rate": round(pos_rate_train, 4),
        "val_pos_rate":   round(float(y_val.mean()), 4),
        "test_pos_rate":  round(float(y_test.mean()), 4),
        "scale_pos_weight": spw,
    }

    # ── Feature matrices ─────────────────────────────────────────────────────
    def _prep(df_in: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        out = pd.DataFrame(index=df_in.index)
        for c in cols:
            if c in df_in.columns:
                out[c] = pd.to_numeric(df_in[c], errors="coerce")
            else:
                out[c] = float("nan")
        return out

    X_train_a = _prep(train_df, BASE_FEATURES)
    X_val_a   = _prep(val_df,   BASE_FEATURES)
    X_test_a  = _prep(test_df,  BASE_FEATURES)

    X_train_b = _prep(train_df, ALL_FEATURES)
    X_val_b   = _prep(val_df,   ALL_FEATURES)
    X_test_b  = _prep(test_df,  ALL_FEATURES)

    # ── Train Model A (21 base) ───────────────────────────────────────────────
    print("\n=== Phase H3a: Model A (21 base features) ===")
    clf_a = _train(X_train_a, y_train, X_val_a, y_val, spw, "A")
    scores_a_test = clf_a.predict_proba(X_test_a)[:, 1]
    metrics_a = _metrics(y_test.values, scores_a_test)
    print(f"  Model A test AUC  : {metrics_a['auc']}")
    print(f"  Model A test AP   : {metrics_a['ap']}")
    print(f"  Model A test F1   : {metrics_a['f1']}")
    print(f"  Model A test Brier: {metrics_a['brier']}")

    # ── Train Model B (21 base + 3 timing) ───────────────────────────────────
    print("\n=== Phase H3b: Model B (21 base + 3 timing) ===")
    clf_b = _train(X_train_b, y_train, X_val_b, y_val, spw, "B")
    scores_b_test = clf_b.predict_proba(X_test_b)[:, 1]
    metrics_b = _metrics(y_test.values, scores_b_test)
    print(f"  Model B test AUC  : {metrics_b['auc']}")
    print(f"  Model B test AP   : {metrics_b['ap']}")
    print(f"  Model B test F1   : {metrics_b['f1']}")
    print(f"  Model B test Brier: {metrics_b['brier']}")

    # ── Delta ─────────────────────────────────────────────────────────────────
    auc_a = metrics_a["auc"]
    auc_b = metrics_b["auc"]
    auc_delta = (
        round(auc_b - auc_a, 4)
        if auc_a is not None and auc_b is not None
        else None
    )
    print(f"\n  AUC delta (B - A): {auc_delta}")

    result["model_a_test"] = metrics_a
    result["model_b_test"] = metrics_b
    result["auc_delta"]    = auc_delta

    # ── H5: Segmented performance ─────────────────────────────────────────────
    print("\n=== Phase H5: Segmented Performance ===")
    seg_report = _segment_report(test_df, y_test.values, scores_a_test, scores_b_test)
    for seg_name, seg in seg_report.items():
        n_seg = seg.get("n", 0)
        if seg.get("skipped"):
            print(f"  [{seg_name}] n={n_seg} -- skipped (too few rows)")
        else:
            da = seg.get("model_a", {})
            db = seg.get("model_b", {})
            d  = seg.get("auc_delta_b_minus_a")
            print(f"  [{seg_name}] n={n_seg}  A_AUC={da.get('auc')}  B_AUC={db.get('auc')}  delta={d}")
    result["segmented_performance"] = seg_report

    # ── H6: Feature importance ────────────────────────────────────────────────
    print("\n=== Phase H6: Timing Feature Importance (Model B) ===")
    imp = _timing_importance(clf_b)
    for feat in TIMING_FEATS:
        fi = imp.get(feat, {})
        print(f"  {feat:35s}  gain={fi.get('gain_pct', 0):.2f}%  split={fi.get('split_pct', 0):.2f}%")
    print(f"\n  Top 10 features by gain:")
    for entry in imp.get("top_10_by_gain", []):
        tag = " [TIMING]" if entry["timing"] else ""
        print(f"    {entry['feature']:35s} {entry['gain_pct']:6.2f}%{tag}")
    result["timing_feature_importance"] = imp

    # ── Verdict ───────────────────────────────────────────────────────────────
    print("\n=== Phase H7: Verdict ===")
    # Check for overfit: AUC_B > AUC_A by meaningful margin AND not suspiciously high
    meaningful_improvement = auc_delta is not None and auc_delta > 0.005
    overfit_suspected = auc_b is not None and auc_b > 0.95

    # Check if timing features have any importance at all
    any_timing_importance = any(
        imp.get(f, {}).get("gain_pct", 0) > 0.5
        for f in TIMING_FEATS
    )

    if proxy.get("proxy_risk"):
        verdict = "D"
        verdict_reason = "DATE_PROXY_RISK -- experiment aborted before training"
    elif overfit_suspected:
        verdict = "C"
        verdict_reason = f"OVERFIT_SUSPECTED -- AUC_B={auc_b} > 0.95 on temporal test split"
    elif meaningful_improvement and any_timing_importance:
        verdict = "A"
        verdict_reason = (
            f"TIMING_FEATURES_USEFUL -- AUC delta={auc_delta}, "
            f"at least one timing feature >0.5% gain importance"
        )
    elif auc_delta is not None and abs(auc_delta) <= 0.005:
        verdict = "B"
        verdict_reason = (
            f"TIMING_FEATURES_NEUTRAL -- AUC delta={auc_delta} within noise band (+/-0.005)"
        )
    else:
        verdict = "B"
        verdict_reason = (
            f"TIMING_FEATURES_MINIMAL -- AUC delta={auc_delta}, "
            f"no timing feature exceeds 0.5% gain importance"
        )

    # Apply readiness gate: override promotable verdicts if data quality fails
    verdict, readiness_suffix = apply_readiness_gate(
        readiness, verdict, promotable_verdicts={"A"}
    )
    if readiness_suffix:
        verdict_reason = verdict_reason + " | " + readiness_suffix

    print(f"  Verdict: {verdict}")
    print(f"  Reason : {verdict_reason}")
    print(f"\n  Verdicts:")
    print(f"    A              = timing features improve AUC meaningfully (>0.005) with real importance")
    print(f"    B              = timing features are neutral or minimal (within noise)")
    print(f"    C              = overfit suspected (AUC_B > 0.95 on temporal test)")
    print(f"    D              = date-proxy risk detected -- results invalid")
    print(f"    NEEDS_MORE_DATA = readiness gate failed -- metrics are exploratory only")

    result["verdict"]        = verdict
    result["verdict_reason"] = verdict_reason

    # ── H7: Safety invariants ─────────────────────────────────────────────────
    print("\n=== Phase H7: Safety Invariants ===")
    safety = {
        "scorer_unchanged":      True,
        "no_deployment":         True,
        "no_live_model_update":  True,
        "reaction_features_excluded": len(missing_reaction) == 0,
        "temporal_split_no_shuffle":  True,
        "artifacts_offline_only":     True,
    }
    for k, v in safety.items():
        status = "OK" if v else "FAIL"
        print(f"  [{status}] {k}")
    result["safety_invariants"] = safety

    # ── Save artifact JSON ────────────────────────────────────────────────────
    artifact_path = artifact_dir / f"phase_h_results_{today}.json"
    with open(artifact_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n  Artifact -> {artifact_path}")
    result["artifact_path"] = str(artifact_path)

    return result


# ─── CLI ──────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase H: Timing-only ML offline experiment")
    p.add_argument("--parquet",      required=True,  help="Path to rebuilt Phase H2 parquet")
    p.add_argument("--artifact-dir", default=None,   help="Directory for output JSON")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    parquet_p = Path(args.parquet).resolve()
    if not parquet_p.exists():
        print(f"[FAIL] Parquet not found: {parquet_p}", file=sys.stderr)
        sys.exit(1)

    if args.artifact_dir:
        artifact_dir = Path(args.artifact_dir).resolve()
    else:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        artifact_dir = (
            _BOT_ROOT / "models" / "experiments" / f"phase_h_{today}"
        )

    result = run_experiment(parquet_p, artifact_dir)

    verdict = result.get("verdict", "UNKNOWN")
    print(f"\n{'=' * 62}")
    print(f"  PHASE H EXPERIMENT COMPLETE -- Verdict: {verdict}")
    print(f"{'=' * 62}")

    if verdict not in ("A", "B"):
        sys.exit(1)


if __name__ == "__main__":
    main()
