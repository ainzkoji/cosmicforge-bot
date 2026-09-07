# ML Retraining Plan — Entry Quality Scorer v1
**Status:** PENDING — do not retrain until pre-conditions are met
**Written:** 2026-03-29
**Scope:** `backends/bot-backend/app/ml/scorer.py` (entry quality scorer only)

---

## 1. WHY THE CURRENT MODEL CANNOT BE RETRAINED YET

The current model (`entry_quality_v1`) was trained on data where:

- `regime_state` = `"UNKNOWN"` for 100% of traces (Stage 1A bug — now fixed)
- `adx`, `atr_pct`, `buy_score`, `sell_score`, `threshold` = NULL for 100% of traces (same root cause)
- `fill_price` = 0.0 for all executed trades (Stage 1B bug — now fixed)

**A model retrained on this data would inherit all these defects.**
The ML model must only be retrained after a clean data accumulation period.

---

## 2. PRE-CONDITIONS THAT MUST PASS BEFORE RETRAINING

All of the following must be satisfied simultaneously:

| # | Pre-condition | How to verify |
|---|---|---|
| P1 | Stage 1A deployed ≥ 30 days | Git tag date + 30d |
| P2 | `decision_traces.regime_state != 'UNKNOWN'` for ≥ 95% of rows in the 30-day window | `SELECT COUNT(*) WHERE regime_state='UNKNOWN' / COUNT(*) < 0.05` |
| P3 | `decision_traces.adx IS NOT NULL` for ≥ 90% of rows | `SELECT AVG(adx IS NULL) < 0.10` |
| P4 | `decision_traces.buy_score IS NOT NULL` for ≥ 90% of rows | `SELECT AVG(buy_score IS NULL) < 0.10` |
| P5 | `decision_traces.confidence > 0` for all ALLOW-scored rows | `SELECT COUNT(*) WHERE ml_score > 0.5 AND confidence = 0` should be 0 |
| P6 | ≥ 500 closed trades (`trade_fills` WHERE `action='CLOSE'`) with `realized_pnl IS NOT NULL` | `SELECT COUNT(*) FROM trade_fills WHERE action='CLOSE' AND realized_pnl IS NOT NULL` ≥ 500 |
| P7 | ≥ 50 closed trades per regime class (STRONG_TREND, WEAK_TREND, RANGE, HIGH_VOLATILITY) | Per-regime count query below |
| P8 | `slippage_pct` populated for ≥ 80% of fills | `SELECT AVG(slippage_pct IS NULL) < 0.20 FROM trade_fills` |
| P9 | Dynamic threshold at P55 stable for ≥ 14 days (no manual overrides) | Check `DYNAMIC_THRESHOLD_PERCENTILE` env var history |

**Estimated earliest retraining date:** 2026-04-29 (30 days after Stage 1A deploy)

---

## 3. DATASET CONSTRUCTION

### 3A. Primary join

```sql
-- Build training dataset: one row per CLOSED trade, features from decision_traces
SELECT
    -- Labels
    tf.realized_pnl,
    tf.r_multiple,
    CASE WHEN tf.realized_pnl > 0 THEN 1 ELSE 0 END AS is_win,
    tf.exit_reason,

    -- Entry-time features from decision_traces (NO post-entry leakage)
    dt.regime_state,
    dt.regime_confidence,
    dt.adx,
    dt.atr_pct,
    dt.buy_score,
    dt.sell_score,
    dt.threshold,
    dt.confidence,            -- ensemble output confidence
    dt.htf_opposed,
    dt.active_strategy_count,

    -- Temporal features
    strftime('%H', dt.ts) AS hour_utc,
    strftime('%w', dt.ts) AS day_of_week,

    -- Context
    dt.drawdown_pct,
    dt.portfolio_risk_used,
    dt.open_positions_count,
    dt.kill_switch_state,
    dt.exposure_freeze,

    -- Adaptive engine state at entry
    dt.aggressiveness_score,
    dt.size_multiplier,

    -- Symbol / timeframe
    tf.symbol,
    tf.timeframe,
    tf.strategy,
    tf.broker_id

FROM trade_fills tf
JOIN decision_traces dt ON dt.order_id = tf.order_id
WHERE
    tf.action = 'CLOSE'
    AND tf.realized_pnl IS NOT NULL
    AND dt.regime_state != 'UNKNOWN'   -- exclude pre-Stage-1A data
    AND dt.ts >= '2026-04-01'          -- only post-fix data
ORDER BY dt.ts ASC;
```

### 3B. Train / validation / test split

**Rule: chronological split only — never random.**

| Split | Period | Purpose |
|---|---|---|
| Train | First 70% of clean rows (by `dt.ts`) | Gradient boosting fit |
| Validation | Next 15% | Hyperparameter tuning, early stopping |
| Test | Final 15% (most recent) | Final reported metrics |

Do not stratify by symbol or regime — time ordering must be preserved.

### 3C. Feature engineering at dataset-build time (no leakage)

| Feature | Derivation | Leakage risk |
|---|---|---|
| `consensus_gap` | `buy_score - sell_score` | None (pre-entry) |
| `threshold_margin` | `confidence - threshold` | None (pre-entry) |
| `regime_*` one-hot | From `regime_state` column | None |
| `hour_utc` sin/cos | From `dt.ts` | None |
| `day_sin`, `day_cos` | From `dt.ts` | None |

**FORBIDDEN features (post-entry leakage):**
- `fill_price` (execution result)
- `fill_qty` (execution result)
- `execution_status` (execution result)
- `final_state_change` (post-trade outcome)
- Trade duration
- Any P&L metric from the same trade

---

## 4. MODEL SELECTION

### V1 model: LightGBM binary classifier

**Target:** `is_win` (1 = profitable close, 0 = loss)
**Primary metric:** Calibration error (Brier score) — we care about probability estimates, not raw accuracy
**Secondary metric:** Precision @ P(win) > 0.60 (what matters for live gating)

**Rationale for LightGBM:**
- Handles missing values natively (no imputation needed during transition period)
- Fast training on small datasets (<10K rows)
- SHAP values available for explainability
- No need for feature scaling
- Already used in `scorer.py` — no new dependencies

**Hyperparameters to tune:**
```python
{
    "n_estimators": [100, 200, 500],
    "max_depth": [3, 4, 5],       # shallow = less overfit on small data
    "learning_rate": [0.05, 0.1],
    "min_child_samples": [20, 50], # forces leaf generalization
    "colsample_bytree": [0.7, 0.9],
    "subsample": [0.8],
    "class_weight": "balanced",    # handles win/loss imbalance
}
```

**Walk-forward validation:** Use existing `scripts/validation/walk_forward_validation.py`
Minimum 3 folds, each fold ≥ 60 days.

---

## 5. ACCEPTANCE CRITERIA

A challenger model is accepted if ALL of the following pass on the **test split**:

| Metric | Minimum | Reasoning |
|---|---|---|
| Brier score | < 0.22 | Better than the no-skill baseline (0.25 for balanced) |
| Precision @ score > 0.6 | > 0.55 | Must beat random at high-confidence predictions |
| Recall @ score > 0.6 | > 0.20 | Must not be vacuous (too few high-confidence predictions) |
| Win-rate lift | > 3% vs full dataset baseline | Must improve at the threshold |
| BLOCK rate change | < ±20% vs current live BLOCK rate | Must not destabilize signal flow |
| Regime balance | Representation in all 4 live regimes | Fail if model never scores RANGE or HIGH_VOL |

**If any criterion fails:** do not promote. Fix data quality issues and retrain.

---

## 6. DEPLOYMENT PROCEDURE

### Step 1 — Shadow mode (mandatory, minimum 14 days)

```python
# config.py
ML_ENABLED = True
ML_SHADOW_MODE = True   # log scores but DO NOT gate entries
```

Collect `ml_shadow_predictions.jsonl` logs:
```json
{"trace_id": "...", "model_version": "v2.0", "score": 0.73, "would_block": false, "features_hash": "abc123", "ts": "2026-05-01T12:00:00Z"}
```

After 14 days of shadow, compute:
- Actual win rate of trades the model would have blocked vs allowed
- If would-have-blocked trades have win rate < 40%: promote
- If would-have-blocked trades have win rate ≥ 45%: do NOT promote (model is wrong)

### Step 2 — Staged activation

```python
# config.py
ML_ENABLED = True
ML_SHADOW_MODE = False
ML_THRESHOLD = 0.35      # conservative first threshold — blocks low-confidence only
```

Monitor for 7 days:
- Check ALLOW/BLOCK distribution hasn't shifted > 25%
- Check win rate on ML-allowed trades vs baseline

### Step 3 — Threshold calibration

After 30 days live: run `scripts/validation/calibrate_ml_threshold.py` to find the threshold that maximises precision @ recall ≥ 0.30.

### Step 4 — Rollback triggers

Automatically revert `ML_ENABLED = False` if ANY of:
- Live win rate drops > 5% vs 30-day pre-ML baseline
- BLOCK rate increases > 50% (over-filtering)
- Model inference latency > 50ms per cycle
- Model file missing or corrupted at startup

---

## 7. RETRAINING CADENCE (AFTER FIRST SUCCESSFUL DEPLOY)

| Cadence | Trigger | Procedure |
|---|---|---|
| Weekly | Automatic (scheduled) | Train challenger on last 90 days of data; compare vs champion on last 14 days |
| Monthly | Manual review | Full walk-forward + regime stratification analysis |
| Ad-hoc | Win rate drops > 5% | Immediate retraining with extended lookback (120 days) |

**Retrain data window:** Rolling 90-day window (drop data older than 90 days).
**Exception:** If total closed trades < 1000, use all available clean data regardless of age.

---

## 8. WHAT MUST NOT CHANGE WITHOUT EXPLICIT REVIEW

- Feature column list (`FEATURE_COLUMNS` in `scorer.py`) — any change requires retraining
- Score threshold applied in live gating — document all threshold changes in git
- Model file naming convention (`entry_quality_vX.Y_YYYYMMDD.pkl`) — used for audit trail
- Shadow mode bypass — ML must always respect `ML_SHADOW_MODE` flag

---

## 9. FILES INVOLVED

| File | Role |
|---|---|
| `app/ml/scorer.py` | Live inference: loads model artifact, scores traces |
| `app/ml/feature_builder.py` | Feature extraction from trace + symbol state |
| `scripts/ml/build_training_dataset.py` | Offline dataset construction (to be created) |
| `scripts/ml/train_entry_quality.py` | Training script (to be created) |
| `scripts/validation/walk_forward_validation.py` | Existing — use for CV |
| `scripts/validation/regime_performance.py` | Existing — use for regime stratification |
| `models/artifacts/` | Versioned model artifacts |
| `models/logs/predictions_*.jsonl` | Shadow mode and live prediction logs |

---

## 10. AUDIT TRAIL REQUIREMENTS

Every model artifact must be accompanied by a `_meta.json` file:

```json
{
  "model_version": "v2.0",
  "trained_at": "2026-04-30T09:15:00Z",
  "training_rows": 847,
  "date_range": ["2026-04-01", "2026-04-29"],
  "dataset_hash": "sha256:abc...",
  "brier_score": 0.198,
  "precision_at_60": 0.61,
  "recall_at_60": 0.27,
  "win_rate_lift_pct": 4.2,
  "regimes_represented": ["STRONG_TREND", "WEAK_TREND", "RANGE", "HIGH_VOLATILITY"],
  "feature_columns": ["regime_STRONG_TREND", "adx", "..."],
  "promoted_to_live": false,
  "shadow_start": null
}
```
