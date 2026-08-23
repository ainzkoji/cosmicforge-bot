#!/usr/bin/env python3
"""Compare temporal-holdout logistic, LightGBM, and optional IOFS-score baselines."""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_score, roc_auc_score
from sklearn.preprocessing import StandardScaler

from shared_lib.ml.contract import ML_FEATURE_COLUMNS


def compare_baselines(dataset_path: str | Path) -> dict[str, Any]:
    dataset = Path(dataset_path)
    frame = pd.read_parquet(dataset)
    if "open_timestamp" in frame.columns:
        frame = frame.sort_values("open_timestamp").reset_index(drop=True)
    missing = [column for column in (*ML_FEATURE_COLUMNS, "label_win") if column not in frame]
    if missing:
        raise ValueError(f"Dataset missing required columns: {missing}")
    split = len(frame) - int(len(frame) * 0.15)
    train, test = frame.iloc[:split], frame.iloc[split:]
    X_train, X_test = _prepare_features(train, test)
    y_train = train["label_win"].astype(int).to_numpy()
    y_test = test["label_win"].astype(int).to_numpy()
    y_r = test["label_r_multiple"].astype(float).to_numpy() if "label_r_multiple" in test else None
    tp_hit = (
        test["label_exit_reason"].isin(["TP1", "TP2"]).astype(int).to_numpy()
        if "label_exit_reason" in test else None
    )

    logistic_score = _model_scores("logistic_regression", X_train, y_train, X_test)
    lightgbm_score = _model_scores("lightgbm_fixed_baseline", X_train, y_train, X_test)
    logistic_walk_forward = _walk_forward_aucs(frame, "logistic_regression")
    lightgbm_walk_forward = _walk_forward_aucs(frame, "lightgbm_fixed_baseline")

    models = {
        "logistic_regression": score_metrics(
            y_test,
            logistic_score,
            y_r=y_r,
            tp_hit=tp_hit,
            walk_forward_aucs=logistic_walk_forward,
        ),
        "lightgbm_fixed_baseline": score_metrics(
            y_test,
            lightgbm_score,
            y_r=y_r,
            tp_hit=tp_hit,
            walk_forward_aucs=lightgbm_walk_forward,
        ),
    }
    if "iofs_score" in frame.columns and frame["iofs_score"].notna().any():
        iofs_test = pd.to_numeric(test["iofs_score"], errors="coerce").fillna(0).to_numpy() / 100.0
        models["iofs_score_rule"] = score_metrics(
            y_test,
            iofs_test,
            y_r=y_r,
            tp_hit=tp_hit,
            walk_forward_aucs=_iofs_walk_forward_aucs(frame),
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset.resolve()),
        "row_count": int(len(frame)),
        "train_rows": int(len(train)),
        "holdout_rows": int(len(test)),
        "models": models,
        "best_holdout_auc_model": max(
            models, key=lambda name: models[name]["holdout_auc"] or float("-inf")
        ),
        "lightgbm_beats_logistic_by": round(
            models["lightgbm_fixed_baseline"]["holdout_auc"]
            - models["logistic_regression"]["holdout_auc"],
            6,
        ),
        "lightgbm_beats_logistic_walk_forward_by": round(
            models["lightgbm_fixed_baseline"]["walk_forward_auc_mean"]
            - models["logistic_regression"]["walk_forward_auc_mean"],
            6,
        ),
        "recommendation": (
            "Do not continue with LightGBM; logistic baseline performs better."
            if (
                models["lightgbm_fixed_baseline"]["holdout_auc"]
                <= models["logistic_regression"]["holdout_auc"]
                or models["lightgbm_fixed_baseline"]["walk_forward_auc_mean"]
                <= models["logistic_regression"]["walk_forward_auc_mean"]
            )
            else "LightGBM shows improvement; still require full validation gates."
        ),
    }


def _prepare_features(
    train: pd.DataFrame,
    test: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    X_train = train[list(ML_FEATURE_COLUMNS)].copy()
    X_test = test[list(ML_FEATURE_COLUMNS)].copy()
    medians = X_train.median()
    return X_train.fillna(medians).fillna(0.0), X_test.fillna(medians).fillna(0.0)


def _model_scores(
    model_name: str,
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    X_test: pd.DataFrame,
) -> np.ndarray:
    if model_name == "logistic_regression":
        scaler = StandardScaler()
        model = LogisticRegression(C=1.0, max_iter=1000, random_state=42)
        model.fit(scaler.fit_transform(X_train), y_train)
        return model.predict_proba(scaler.transform(X_test))[:, 1]
    positive = max(int(y_train.sum()), 1)
    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        scale_pos_weight=(len(y_train) - positive) / positive,
        random_state=42,
        verbose=-1,
    )
    model.fit(X_train, y_train)
    return model.predict_proba(X_test)[:, 1]


def _walk_forward_slices(frame: pd.DataFrame) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    fold_size = max(10, int(len(frame) * 0.15))
    first_train_end = max(30, len(frame) - (3 * fold_size))
    train_ends = list(range(first_train_end, len(frame) - fold_size + 1, fold_size))[-3:]
    return [
        (frame.iloc[:train_end], frame.iloc[train_end:train_end + fold_size])
        for train_end in train_ends
    ]


def _walk_forward_aucs(frame: pd.DataFrame, model_name: str) -> list[float]:
    aucs: list[float] = []
    for train, test in _walk_forward_slices(frame):
        y_train = train["label_win"].astype(int).to_numpy()
        y_test = test["label_win"].astype(int).to_numpy()
        if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
            continue
        X_train, X_test = _prepare_features(train, test)
        scores = _model_scores(model_name, X_train, y_train, X_test)
        aucs.append(float(roc_auc_score(y_test, scores)))
    return aucs


def _iofs_walk_forward_aucs(frame: pd.DataFrame) -> list[float]:
    aucs: list[float] = []
    for _, test in _walk_forward_slices(frame):
        y_test = test["label_win"].astype(int).to_numpy()
        if len(np.unique(y_test)) < 2:
            continue
        scores = pd.to_numeric(test["iofs_score"], errors="coerce").fillna(0).to_numpy() / 100.0
        aucs.append(float(roc_auc_score(y_test, scores)))
    return aucs


def score_metrics(
    y_true: np.ndarray,
    score: np.ndarray,
    *,
    y_r: np.ndarray | None = None,
    tp_hit: np.ndarray | None = None,
    walk_forward_aucs: list[float] | None = None,
) -> dict[str, Any]:
    order = np.argsort(score)
    quartile = max(1, int(len(score) * 0.25))
    bottom, top = order[:quartile], order[-quartile:]
    predicted_top = np.zeros(len(score), dtype=int)
    predicted_top[top] = 1
    positives = y_r[top][y_r[top] > 0] if y_r is not None else np.array([])
    negatives = y_r[top][y_r[top] < 0] if y_r is not None else np.array([])
    walk_forward = walk_forward_aucs or [float(roc_auc_score(y_true, score))]
    return {
        "holdout_auc": round(float(roc_auc_score(y_true, score)), 6),
        "walk_forward_auc_mean": round(float(np.mean(walk_forward)), 6),
        "walk_forward_auc_std": round(float(np.std(walk_forward)), 6),
        "walk_forward_fold_count": len(walk_forward),
        "quartile_gap": round(float(y_true[top].mean() - y_true[bottom].mean()), 6),
        "precision_top_quartile": round(float(precision_score(y_true, predicted_top)), 6),
        "tp_hit_rate_top_quartile": (
            round(float(tp_hit[top].mean()), 6) if tp_hit is not None else None
        ),
        "win_rate_gap": round(float(y_true[top].mean() - y_true[bottom].mean()), 6),
        "profit_factor_proxy_top_quartile": (
            round(float(positives.sum() / abs(negatives.sum())), 6)
            if len(negatives) and abs(negatives.sum()) > 0 else None
        ),
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Entry Model Baseline Comparison",
        "",
        f"- Dataset rows: {report['row_count']}",
        f"- Train rows: {report['train_rows']}",
        f"- Temporal holdout rows: {report['holdout_rows']}",
        f"- Best holdout AUC model: {report['best_holdout_auc_model']}",
        f"- LightGBM minus logistic AUC: {report['lightgbm_beats_logistic_by']:.6f}",
        "- LightGBM minus logistic walk-forward mean AUC: "
        f"{report['lightgbm_beats_logistic_walk_forward_by']:.6f}",
        f"- Recommendation: {report['recommendation']}",
        "- Holdout caution: this comparison uses one 48-row temporal holdout and is not an acceptance result.",
        "",
        "| Model | Holdout AUC | WF AUC Mean | WF AUC Std | Quartile Gap | Top-Q Precision | TP-Hit Rate | PF Proxy |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, metrics in report["models"].items():
        lines.append(
            f"| {name} | {_fmt(metrics['holdout_auc'])} | {_fmt(metrics['walk_forward_auc_mean'])} | "
            f"{_fmt(metrics['walk_forward_auc_std'])} | {_fmt(metrics['quartile_gap'])} | "
            f"{_fmt(metrics['precision_top_quartile'])} | {_fmt(metrics['tp_hit_rate_top_quartile'])} | "
            f"{_fmt(metrics['profit_factor_proxy_top_quartile'])} |"
        )
    lines.extend([
        "",
        "This is an offline comparison baseline only. No baseline model is approved for deployment.",
        "The existing v2.0 candidate remains rejected; this fixed-parameter result does not create an artifact.",
        "",
    ])
    return "\n".join(lines)


def write_reports(report: dict[str, Any], output_json: Path, output_md: Path) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(render_markdown(report), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-path", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = compare_baselines(args.dataset_path)
    write_reports(report, Path(args.output_json), Path(args.output_md))
    print(json.dumps(report, indent=2))
    return 0


def _fmt(value: Any) -> str:
    return "N/A" if value is None else f"{float(value):.4f}"


if __name__ == "__main__":
    raise SystemExit(main())
