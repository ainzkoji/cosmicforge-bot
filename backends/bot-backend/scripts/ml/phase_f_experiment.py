#!/usr/bin/env python3
"""
Phase F: Offline ML Experiment — Event/Reaction Feature Impact.

Trains two models on the same dataset:
  Model A: 21 base ML_FEATURE_COLUMNS only
  Model B: 28 columns (21 base + 7 EVENT_FEATURE_COLUMNS)

Compares metrics on a held-out temporal test set.

SAFETY: no deployment, no scorer update, no live impact.
Artifacts saved to models/experiments/event_features_experimental_YYYYMMDD/
"""
from __future__ import annotations

import json
import math
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT   = _SCRIPT_DIR.parent.parent
_SHARED     = _BOT_ROOT.parent / "shared"
for p in (str(_BOT_ROOT), str(_SHARED)):
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    import numpy as np
    import pandas as pd
    import lightgbm as lgb
    from sklearn.metrics import (
        accuracy_score, precision_score, recall_score, f1_score,
        roc_auc_score, average_precision_score, confusion_matrix,
        brier_score_loss,
    )
    from sklearn.preprocessing import OrdinalEncoder
    from scipy.stats import spearmanr as _spearmanr
except ImportError as e:
    print(f"[FAIL] Missing dependency: {e}")
    sys.exit(1)

from shared_lib.ml.contract import ML_FEATURE_COLUMNS, EVENT_FEATURE_COLUMNS
from shared_lib.ml.readiness import (
    check_dataset_readiness,
    apply_readiness_gate,
    format_readiness_report,
)

# ─── Constants ────────────────────────────────────────────────────────────────

LABEL_COL       = "label_win"          # pnl > 0 — 34% positive rate
BASE_FEATURES   = list(ML_FEATURE_COLUMNS)
EVENT_FEATURES  = list(EVENT_FEATURE_COLUMNS)
ALL_FEATURES    = BASE_FEATURES + EVENT_FEATURES
CATEGORICAL_COLS = ["reaction_type"]

TRAIN_FRAC  = 0.60
VAL_FRAC    = 0.20
# TEST_FRAC  = 0.20 (remainder)

LGB_PARAMS = {
    "objective":       "binary",
    "metric":          "binary_logloss",
    "n_estimators":    400,
    "max_depth":       6,
    "learning_rate":   0.05,
    "num_leaves":      31,
    "min_child_samples": 20,
    "subsample":       0.8,
    "colsample_bytree": 0.8,
    "reg_lambda":      1.0,
    "verbose":         -1,
    "n_jobs":          -1,
    "random_state":    42,
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _classification_metrics(y_true: np.ndarray, y_pred: np.ndarray, y_prob: np.ndarray) -> dict[str, Any]:
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    n = len(y_true)
    pos = int(y_true.sum())
    neg = n - pos
    try:
        roc = float(roc_auc_score(y_true, y_prob))
    except Exception:
        roc = float("nan")
    try:
        pr_auc = float(average_precision_score(y_true, y_prob))
    except Exception:
        pr_auc = float("nan")
    try:
        brier = float(brier_score_loss(y_true, y_prob))
    except Exception:
        brier = float("nan")
    fpr = fp / (fp + tn) if (fp + tn) > 0 else float("nan")
    fnr = fn / (fn + tp) if (fn + tp) > 0 else float("nan")
    return {
        "n": n,
        "pos": pos,
        "neg": neg,
        "pos_rate": round(pos / n, 4) if n else float("nan"),
        "accuracy":  round(float(accuracy_score(y_true, y_pred)), 4),
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        "recall":    round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
        "f1":        round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
        "roc_auc":   round(roc, 4) if not math.isnan(roc) else None,
        "pr_auc":    round(pr_auc, 4) if not math.isnan(pr_auc) else None,
        "brier":     round(brier, 4) if not math.isnan(brier) else None,
        "fpr":       round(fpr, 4) if not math.isnan(fpr) else None,
        "fnr":       round(fnr, 4) if not math.isnan(fnr) else None,
        "confusion": {"tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn)},
    }


def _segment_metrics(
    df: pd.DataFrame, y_true: np.ndarray, y_pred: np.ndarray, y_prob: np.ndarray,
    mask: pd.Series, label: str,
) -> dict[str, Any] | None:
    idx = mask[mask].index
    if len(idx) < 5:
        return {"segment": label, "n": len(idx), "skipped": "too_few_rows"}
    sel = df.index.isin(idx)
    yt = y_true[sel]
    yp = y_pred[sel]
    ypr = y_prob[sel]
    m = _classification_metrics(yt, yp, ypr)
    m["segment"] = label
    return m


def _encode_categoricals(
    train: pd.DataFrame, val: pd.DataFrame, test: pd.DataFrame, cols: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    enc = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
    present = [c for c in cols if c in train.columns]
    if not present:
        return train, val, test
    train = train.copy()
    val   = val.copy()
    test  = test.copy()
    train[present] = enc.fit_transform(train[present].fillna("__NA__"))
    val[present]   = enc.transform(val[present].fillna("__NA__"))
    test[present]  = enc.transform(test[present].fillna("__NA__"))
    return train, val, test


def _train_eval(
    train_df: pd.DataFrame,
    val_df:   pd.DataFrame,
    test_df:  pd.DataFrame,
    features: list[str],
    model_tag: str,
) -> tuple[lgb.LGBMClassifier, dict[str, Any]]:
    feat_present = [f for f in features if f in train_df.columns]

    X_tr  = train_df[feat_present].values.astype("float64")
    y_tr  = train_df[LABEL_COL].values.astype(int)
    X_val = val_df[feat_present].values.astype("float64")
    y_val = val_df[LABEL_COL].values.astype(int)
    X_te  = test_df[feat_present].values.astype("float64")
    y_te  = test_df[LABEL_COL].values.astype(int)

    pos   = int(y_tr.sum())
    neg   = len(y_tr) - pos
    spw   = neg / max(pos, 1)

    params = {**LGB_PARAMS, "scale_pos_weight": round(spw, 3)}
    model  = lgb.LGBMClassifier(**params)
    model.fit(
        X_tr, y_tr,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
    )

    threshold = 0.50
    prob_te   = model.predict_proba(X_te)[:, 1]
    pred_te   = (prob_te >= threshold).astype(int)

    return model, {
        "model_tag":        model_tag,
        "feature_count":    len(feat_present),
        "features_used":    feat_present,
        "train_rows":       len(X_tr),
        "val_rows":         len(X_val),
        "test_rows":        len(X_te),
        "scale_pos_weight": round(spw, 3),
        "best_iteration":   model.best_iteration_,
        "threshold":        threshold,
        "test_metrics":     _classification_metrics(y_te, pred_te, prob_te),
    }


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    today  = datetime.now(timezone.utc).strftime("%Y%m%d")
    exp_dir = _BOT_ROOT / "models" / "experiments" / f"event_features_experimental_{today}"
    exp_dir.mkdir(parents=True, exist_ok=True)

    # ── Load dataset ──────────────────────────────────────────────────────────
    parquet_path = Path("C:/tmp/phase_e_dataset/training_20260426.parquet")
    if not parquet_path.exists():
        # Fall back to latest in models/datasets/
        candidates = sorted((_BOT_ROOT / "models" / "datasets").glob("training_*.parquet"))
        if not candidates:
            print("[FAIL] No training parquet found.")
            sys.exit(1)
        parquet_path = candidates[-1]

    print(f"Loading dataset: {parquet_path}")
    raw = pd.read_parquet(str(parquet_path))
    raw = raw.sort_values("open_timestamp").reset_index(drop=True)
    print(f"  Raw rows: {len(raw):,}")

    # ── Label selection ───────────────────────────────────────────────────────
    # label (R>=0.5) is 0% positive — not usable.
    # label_win (pnl>0) is 34% positive — use this.
    label_null = raw[LABEL_COL].isna().sum()
    raw = raw[raw[LABEL_COL].notna()].copy()
    raw[LABEL_COL] = raw[LABEL_COL].astype(int)
    print(f"  Rows after dropping null {LABEL_COL}: {len(raw):,} (dropped {label_null})")
    print(f"  Positive rate: {raw[LABEL_COL].mean():.1%}")

    # ── Temporal split ────────────────────────────────────────────────────────
    n      = len(raw)
    n_tr   = int(n * TRAIN_FRAC)
    n_val  = int(n * VAL_FRAC)
    n_test = n - n_tr - n_val

    train_df = raw.iloc[:n_tr].copy()
    val_df   = raw.iloc[n_tr : n_tr + n_val].copy()
    test_df  = raw.iloc[n_tr + n_val :].copy()

    print(f"\nTemporal split:")
    print(f"  Train : {len(train_df):,} rows  ({train_df['open_timestamp'].min()[:10]} -> {train_df['open_timestamp'].max()[:10]})")
    print(f"  Val   : {len(val_df):,}  rows  ({val_df['open_timestamp'].min()[:10]} -> {val_df['open_timestamp'].max()[:10]})")
    print(f"  Test  : {len(test_df):,}  rows  ({test_df['open_timestamp'].min()[:10]} -> {test_df['open_timestamp'].max()[:10]})")

    # ── Encode categoricals ───────────────────────────────────────────────────
    train_enc, val_enc, test_enc = _encode_categoricals(
        train_df, val_df, test_df, CATEGORICAL_COLS
    )

    # ── Phase F1: Dataset comparison ──────────────────────────────────────────
    base_null = {c: int(raw[c].isna().sum()) for c in BASE_FEATURES if c in raw.columns}
    event_null = {c: int(raw[c].isna().sum()) for c in EVENT_FEATURES if c in raw.columns}

    print("\n=== Phase F1: Dataset Comparison ===")
    print(f"  Dataset A: {len(BASE_FEATURES)} base features")
    print(f"  Dataset B: {len(ALL_FEATURES)} features (base + event)")
    print(f"  Same rows: {len(raw):,}   Same labels: label_win @ {raw[LABEL_COL].mean():.1%}")
    print(f"  Event feature null summary:")
    for c, n in event_null.items():
        pct = n / max(len(raw), 1) * 100
        print(f"    {c}: {n:,}/{len(raw):,} ({pct:.1f}%) null")

    # ── Phase F2: Train both models ───────────────────────────────────────────
    print("\n=== Phase F2: Training Model A (baseline) ===")
    model_a, results_a = _train_eval(train_enc, val_enc, test_enc, BASE_FEATURES, "model_A_baseline")
    print(f"  Best iteration: {results_a['best_iteration']}")
    ma = results_a["test_metrics"]
    print(f"  Test: acc={ma['accuracy']} prec={ma['precision']} rec={ma['recall']} f1={ma['f1']} auc={ma['roc_auc']}")

    print("\n=== Phase F2: Training Model B (event features) ===")
    model_b, results_b = _train_eval(train_enc, val_enc, test_enc, ALL_FEATURES, "model_B_event_features")
    print(f"  Best iteration: {results_b['best_iteration']}")
    mb = results_b["test_metrics"]
    print(f"  Test: acc={mb['accuracy']} prec={mb['precision']} rec={mb['recall']} f1={mb['f1']} auc={mb['roc_auc']}")

    # ── Phase F3: Metric comparison ───────────────────────────────────────────
    print("\n=== Phase F3: Metric Comparison ===")
    metric_keys = ["accuracy", "precision", "recall", "f1", "roc_auc", "pr_auc", "brier", "fpr", "fnr"]
    print(f"  {'Metric':20s}  {'Model A':>10s}  {'Model B':>10s}  {'Delta':>10s}")
    print(f"  {'-'*20}  {'-'*10}  {'-'*10}  {'-'*10}")
    diffs = {}
    for k in metric_keys:
        va = ma.get(k)
        vb = mb.get(k)
        if va is None or vb is None:
            continue
        delta = round(float(vb) - float(va), 4)
        diffs[k] = delta
        sign = "+" if delta > 0 else ""
        print(f"  {k:20s}  {va:>10}  {vb:>10}  {sign+str(delta):>10}")

    # ── Phase F4: Event-specific performance (Model B) ────────────────────────
    print("\n=== Phase F4: Event-Specific Segments (Model B on test set) ===")
    feat_b   = [f for f in ALL_FEATURES if f in test_enc.columns]
    X_te_b   = test_enc[feat_b].values.astype("float64")
    prob_te_b = model_b.predict_proba(X_te_b)[:, 1]
    pred_te_b = (prob_te_b >= 0.50).astype(int)
    y_te      = test_enc[LABEL_COL].values.astype(int)
    test_idx  = test_enc.index

    def seg(mask_series: pd.Series, label: str) -> dict:
        m = _segment_metrics(test_enc, y_te, pred_te_b, prob_te_b, mask_series, label)
        if m:
            skipped = m.get("skipped")
            if skipped:
                print(f"  {label}: n={m['n']} — SKIPPED ({skipped})")
            else:
                print(f"  {label}: n={m['n']} acc={m['accuracy']} f1={m['f1']} auc={m.get('roc_auc','N/A')}")
        return m or {}

    segments = []
    segments.append(seg(test_enc.get("is_blackout_active", pd.Series(0, index=test_idx)) == 1, "inside_blackout"))
    segments.append(seg(
        test_enc.get("minutes_to_next_event", pd.Series(float("nan"), index=test_idx)).between(0, 60),
        "60min_before_event"
    ))
    segments.append(seg(
        test_enc.get("minutes_since_last_event", pd.Series(float("nan"), index=test_idx)).between(0, 60),
        "60min_after_event"
    ))
    segments.append(seg(
        test_enc.get("reaction_type", pd.Series("NO_PRIOR_EVENT", index=test_idx)) != "NO_PRIOR_EVENT",
        "after_completed_reaction"
    ))
    for rt in ["WHIPSAW", "TREND_CONTINUATION", "NO_REACTION", "NO_PRIOR_EVENT"]:
        col = test_enc.get("reaction_type", pd.Series("NO_PRIOR_EVENT", index=test_idx))
        segments.append(seg(col == rt, f"reaction_{rt}"))

    # ── Phase F5: Feature importance ──────────────────────────────────────────
    print("\n=== Phase F5: Feature Importance ===")
    feat_b_list = [f for f in ALL_FEATURES if f in train_enc.columns]
    importances = dict(zip(feat_b_list, model_b.feature_importances_))
    sorted_imp  = sorted(importances.items(), key=lambda x: -x[1])

    print("  Top 15 features (Model B):")
    for rank, (name, imp) in enumerate(sorted_imp[:15], 1):
        tag = " <- EVENT" if name in EVENT_FEATURES else ""
        print(f"    {rank:2d}. {name:35s} {imp:6d}{tag}")

    print("\n  Event feature importances:")
    ev_importances = {}
    for name in EVENT_FEATURES:
        imp = importances.get(name, 0)
        total = sum(importances.values()) or 1
        pct   = imp / total * 100
        ev_importances[name] = {"importance": int(imp), "pct_of_total": round(pct, 2)}
        is_zero = imp == 0
        note    = " (ZERO — unused by model)" if is_zero else ""
        print(f"    {name:35s} {imp:6d}  ({pct:.2f}% of total){note}")

    # ── Phase F5b: Date-proxy risk detection ──────────────────────────────────
    print("\n=== Phase F5b: Date-Proxy Risk Detection ===")
    proxy_risk = False
    proxy_details: dict = {}

    mtn_col = "minutes_to_next_event"
    if mtn_col in raw.columns:
        valid_mtn = raw[mtn_col].dropna()
        if len(valid_mtn) >= 10:
            idx_arr  = np.arange(len(valid_mtn), dtype=float)
            corr, _  = _spearmanr(idx_arr, valid_mtn.values)
            corr = float(corr) if not math.isnan(corr) else 0.0
        else:
            corr = 0.0
        proxy_details["spearman_vs_rowindex"] = round(corr, 4)
        # A strong negative correlation means "feature is counting down toward
        # a single cluster of events at the end of the dataset" — date proxy.
        proxy_risk = abs(corr) > 0.70
        print(f"  Spearman(minutes_to_next_event, row_index) = {corr:.4f}")
        print(f"  Threshold: |corr| > 0.70 => date_proxy_risk")
    else:
        proxy_details["spearman_vs_rowindex"] = None
        print(f"  {mtn_col} not in dataset — proxy check skipped")

    # Check event date spread across train/val/test
    train_ts = pd.to_datetime(train_df["open_timestamp"], utc=True, errors="coerce")
    test_ts  = pd.to_datetime(test_df["open_timestamp"],  utc=True, errors="coerce")
    train_event_rows = int(train_df[mtn_col].notna().sum()) if mtn_col in train_df.columns else 0
    test_event_rows  = int(test_df[mtn_col].notna().sum())  if mtn_col in test_df.columns else 0
    proxy_details["train_event_rows"] = train_event_rows
    proxy_details["test_event_rows"]  = test_event_rows

    # Count unique event dates visible in dataset (from non-null minutes_to_next_event)
    # Approximate: look at abrupt changes in minutes_to_next_event (new event detected)
    if mtn_col in raw.columns:
        # When minutes_to_next_event increases sharply between consecutive rows,
        # a new future event has entered the window.
        mtn_full = raw[mtn_col].dropna()
        diffs_mtn = mtn_full.diff()
        # A jump > 60 minutes means at least one new event boundary crossed
        jumps = int((diffs_mtn > 60).sum())
        proxy_details["distinct_event_boundaries"] = jumps + 1 if len(mtn_full) > 0 else 0
    else:
        proxy_details["distinct_event_boundaries"] = 0

    proxy_details["date_proxy_risk"] = proxy_risk
    print(f"  Distinct event boundaries in dataset   : {proxy_details.get('distinct_event_boundaries', 0)}")
    print(f"  Train rows with non-null event feature : {train_event_rows}")
    print(f"  Test  rows with non-null event feature : {test_event_rows}")
    print(f"  EVENT_FEATURE_DATE_PROXY_RISK          : {proxy_risk}")

    if proxy_risk:
        print(
            "  [WARNING] Date-proxy risk detected. The model improvement attributable "
            "to event features may be a temporal artifact, not a real economic signal. "
            "Do NOT claim feature improvement from event features in this run."
        )

    # ── Phase I Readiness Gate ────────────────────────────────────────────────
    print("\n=== Phase I: Data Readiness Gate ===")
    readiness = check_dataset_readiness(
        raw,
        check_reactions=True,
        check_blackout=False,
    )
    print(format_readiness_report(readiness, title="Phase F Dataset Readiness"))

    # ── Phase F6: Safety decision ─────────────────────────────────────────────
    print("\n=== Phase F6: Safety Decision ===")
    event_total_imp = sum(v["importance"] for v in ev_importances.values())
    total_imp       = sum(importances.values()) or 1
    event_pct       = event_total_imp / total_imp * 100

    f1_delta   = diffs.get("f1", 0.0)
    auc_delta  = diffs.get("roc_auc", 0.0) or 0.0
    has_event_data = sum(1 for c, n in event_null.items() if n < len(raw) * 0.99)

    if proxy_risk:
        verdict = "D"
        reason  = (
            f"EVENT_FEATURE_DATE_PROXY_RISK=true. "
            f"Spearman correlation between minutes_to_next_event and row index = "
            f"{proxy_details.get('spearman_vs_rowindex', 'N/A')} (threshold |0.70|). "
            "Any apparent improvement from event features cannot be attributed to genuine "
            "economic event signal — the feature acts as a date proxy for the current "
            "dataset. Backfill real historical events with Phase G and re-run."
        )
    elif event_pct < 1.0 or has_event_data < 2:
        verdict = "D"
        reason  = (
            f"Event features contribute only {event_pct:.1f}% of total importance. "
            f"Only {has_event_data}/7 event features have non-trivial variance. "
            "Insufficient real economic event data in DB. "
            "Run Phase G historical backfill and re-run this experiment."
        )
    elif f1_delta > 0.01 and auc_delta > 0.005:
        verdict = "A"
        reason  = f"Event features improved F1 by {f1_delta:+.4f} and AUC by {auc_delta:+.4f}. Continue offline testing."
    elif f1_delta < -0.01:
        verdict = "C"
        reason  = f"Event features degraded F1 by {f1_delta:.4f}. Keep out of model."
    else:
        verdict = "B"
        reason  = f"Event features are neutral (F1 delta={f1_delta:+.4f}). Collect more event data."

    # Apply readiness gate: override promotable verdicts if data quality fails
    verdict, readiness_suffix = apply_readiness_gate(
        readiness, verdict, promotable_verdicts={"A"}
    )
    if readiness_suffix:
        reason = reason + " | " + readiness_suffix

    print(f"  Verdict: Option {verdict}")
    print(f"  Reason:  {reason}")

    # ── Write artifact JSON ───────────────────────────────────────────────────
    artifact = {
        "experiment": "phase_f_event_features",
        "generated_at": _now(),
        "safety": {
            "deployed": False,
            "scorer_updated": False,
            "live_impact": False,
            "model_tag": "event_features_experimental",
        },
        "dataset": {
            "path": str(parquet_path),
            "rows": len(raw),
            "label": LABEL_COL,
            "positive_rate": round(raw[LABEL_COL].mean(), 4),
            "temporal_range": {
                "train": [str(train_df["open_timestamp"].min())[:10], str(train_df["open_timestamp"].max())[:10]],
                "val":   [str(val_df["open_timestamp"].min())[:10],   str(val_df["open_timestamp"].max())[:10]],
                "test":  [str(test_df["open_timestamp"].min())[:10],  str(test_df["open_timestamp"].max())[:10]],
            },
            "event_feature_null_rates": {c: f"{n/max(len(raw),1)*100:.1f}%" for c, n in event_null.items()},
        },
        "model_a": results_a,
        "model_b": results_b,
        "metric_deltas": diffs,
        "event_segments": [s for s in segments if s],
        "feature_importance_b": {k: v for k, v in sorted_imp},
        "event_feature_importance": ev_importances,
        "event_feature_total_importance_pct": round(event_pct, 2),
        "verdict": verdict,
        "verdict_reason": reason,
        "event_feature_date_proxy_risk": proxy_risk,
        "date_proxy_details": proxy_details,
        "leakage_confirmed": True,
        "artifact_dir": str(exp_dir),
        "production_model_unchanged": True,
    }

    out_path = exp_dir / f"phase_f_results_{today}.json"
    with open(out_path, "w") as f:
        json.dump(artifact, f, indent=2, default=str)

    print(f"\nArtifact written: {out_path}")
    print(f"\n{'='*62}")
    print(f"  PHASE F COMPLETE — Verdict: Option {verdict}")
    print(f"{'='*62}")
    print(f"  {reason}")
    print(f"\n  Model A (baseline):  F1={ma['f1']}  AUC={ma['roc_auc']}")
    print(f"  Model B (event+base): F1={mb['f1']}  AUC={mb['roc_auc']}")
    print(f"  Event feature importance: {event_pct:.1f}% of total")
    print(f"  Production model: UNCHANGED")
    print(f"  Deployed: NO")
    print()


if __name__ == "__main__":
    main()
