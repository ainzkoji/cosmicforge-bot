#!/usr/bin/env python3
"""
Walk-Forward Validation (Leakage-Free)
Phase 3 Fast-Track: Temporal split on backfill simulation data.
Train on earliest 60%, score on latest 40% — strictly no overlap.
"""
import sqlite3
import sys
import warnings
import time

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import LabelEncoder

sys.stdout.reconfigure(encoding='utf-8')
warnings.filterwarnings('ignore')

DB = 'c:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/shared/shared_lib/persistence/cosmicforge.db'

FEATURE_SQL = """
SELECT
    dt.ts,
    dt.symbol,
    tf_o.side,
    dt.adx,
    dt.atr_pct,
    dt.ma_slope,
    dt.compression_ratio,
    dt.breakout_pressure,
    dt.buy_score,
    dt.sell_score,
    dt.last_price,
    dt.mark_price,
    dt.confidence,
    dt.threshold,
    COALESCE(dt.confidence - dt.threshold, 0.0) AS threshold_gap,
    COALESCE(dt.buy_score - dt.sell_score, 0.0) AS consensus_gap,
    COALESCE(dt.regime_state, 'UNKNOWN') AS regime_state,
    tf_c.realized_pnl
FROM trade_fills tf_o
JOIN trade_fills tf_c ON tf_o.position_id = tf_c.position_id AND tf_c.action = 'CLOSE'
JOIN decision_traces dt ON tf_o.cycle_id = dt.cycle_id
WHERE tf_o.action = 'OPEN'
  AND tf_o.account_id = 'backfill'
  AND tf_o.position_id IS NOT NULL
  AND dt.adx IS NOT NULL
  AND dt.breakout_pressure IS NOT NULL
  AND tf_c.realized_pnl IS NOT NULL
ORDER BY dt.ts ASC
"""

NUM_FEATURES = [
    'adx', 'atr_pct', 'ma_slope', 'compression_ratio', 'breakout_pressure',
    'buy_score', 'sell_score', 'last_price', 'mark_price', 'confidence',
    'threshold_gap', 'consensus_gap', 'hour_utc', 'day_of_week', 'is_weekend'
]
CAT_FEATURES = ['side_enc', 'symbol_enc', 'regime_enc']
ALL_FEATURES = NUM_FEATURES + CAT_FEATURES

SEP = "=" * 70


def main():
    # ── SECTION 1: DATA EXTRACTION ──────────────────────────────────────────
    print(SEP)
    print("PHASE 3 FAST-TRACK — WALK-FORWARD VALIDATION (LEAKAGE-FREE)")
    print(SEP)
    print("\n[SECTION 1 — DATA EXTRACTION]")

    conn = sqlite3.connect(DB)
    df = pd.read_sql_query(FEATURE_SQL, conn)
    conn.close()

    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['label_win'] = (df['realized_pnl'] > 0).astype(int)
    df['hour_utc'] = df['ts'].dt.hour
    df['day_of_week'] = df['ts'].dt.dayofweek
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)

    print(f"  Total rows extracted:    {len(df):,}")
    print(f"  Date range:              {df['ts'].min().date()} to {df['ts'].max().date()}")
    print(f"  Win rate (label=1):      {df['label_win'].mean()*100:.1f}%  (n wins={int(df['label_win'].sum()):,})")
    print(f"  Avg realized PnL:        {df['realized_pnl'].mean():.4f} USDT")
    print(f"  Source:                  account_id='backfill' (synthetic simulation)")

    # ── SECTION 2: TEMPORAL SPLIT ────────────────────────────────────────────
    print("\n[SECTION 2 — TEMPORAL SPLIT (60/40, STRICT CHRONOLOGICAL)]")

    n_train = int(len(df) * 0.60)
    df_train = df.iloc[:n_train].copy()
    df_test  = df.iloc[n_train:].copy()
    n_test   = len(df_test)

    print(f"  TRAIN window:  {df_train['ts'].min().date()} -> {df_train['ts'].max().date()}  (n={n_train:,})")
    print(f"  TEST  window:  {df_test['ts'].min().date()} -> {df_test['ts'].max().date()}  (n={n_test:,})")
    print(f"  Split date:    {df_test['ts'].min().date()}")
    print(f"  TRAIN win rate: {df_train['label_win'].mean()*100:.1f}%")
    print(f"  TEST  win rate: {df_test['label_win'].mean()*100:.1f}%")

    # ── SECTION 3: FEATURE ENGINEERING ──────────────────────────────────────
    print("\n[SECTION 3 — FEATURE ENGINEERING]")

    le_side   = LabelEncoder().fit(df['side'].fillna('UNKNOWN'))
    le_symbol = LabelEncoder().fit(df['symbol'].fillna('UNKNOWN'))
    le_regime = LabelEncoder().fit(df['regime_state'].fillna('UNKNOWN'))

    for split_df in [df_train, df_test]:
        split_df['side_enc']   = le_side.transform(split_df['side'].fillna('UNKNOWN'))
        split_df['symbol_enc'] = le_symbol.transform(split_df['symbol'].fillna('UNKNOWN'))
        split_df['regime_enc'] = le_regime.transform(split_df['regime_state'].fillna('UNKNOWN'))

    X_train = df_train[ALL_FEATURES].values
    y_train = df_train['label_win'].values
    X_test  = df_test[ALL_FEATURES].values
    y_test  = df_test['label_win'].values

    print(f"  Features used: {len(ALL_FEATURES)}  ({len(NUM_FEATURES)} numeric + {len(CAT_FEATURES)} categorical)")
    print(f"  Dropped (all-NaN in data): aggressiveness_score, confidence_gate_modifier,")
    print(f"    size_multiplier, rolling_win_rate, rolling_expectancy, loss_streak")
    print(f"  X_train: {X_train.shape}  pos_rate={y_train.mean()*100:.1f}%")
    print(f"  X_test:  {X_test.shape}  pos_rate={y_test.mean()*100:.1f}%")

    # ── SECTION 4: MODEL TRAINING (TRAIN WINDOW ONLY) ────────────────────────
    print("\n[SECTION 4 — MODEL TRAINING (TRAIN WINDOW ONLY)]")

    scale_pos = float((y_train == 0).sum()) / max(float((y_train == 1).sum()), 1.0)

    model = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        min_child_samples=20,
        scale_pos_weight=scale_pos,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbose=-1
    )

    t0 = time.time()
    model.fit(X_train, y_train)
    elapsed = time.time() - t0

    train_scores = model.predict_proba(X_train)[:, 1]
    train_auc = roc_auc_score(y_train, train_scores)

    print(f"  Training time:        {elapsed:.1f}s")
    print(f"  Train AUC (in-sample): {train_auc:.5f}")
    print(f"  scale_pos_weight:     {scale_pos:.2f}")

    # ── SECTION 5: OUT-OF-SAMPLE SCORING ─────────────────────────────────────
    print("\n[SECTION 5 — OUT-OF-SAMPLE RESULTS]")

    test_scores = model.predict_proba(X_test)[:, 1]
    test_auc    = roc_auc_score(y_test, test_scores)

    print(f"  Walk-Forward AUC (test, out-of-sample): {test_auc:.5f}")
    print(f"  Original v1.1 AUC (train=test, same day): 0.99977")
    print(f"  Logistic baseline AUC:                  0.52653")
    print(f"  Overfit delta (v1.1 - walk-forward):    {0.99977 - test_auc:+.5f}")

    df_test = df_test.copy()
    df_test['score'] = test_scores
    bins   = [0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0]
    labels = ['0.0-0.1','0.1-0.2','0.2-0.3','0.3-0.4','0.4-0.5',
              '0.5-0.6','0.6-0.7','0.7-0.8','0.8-0.9','0.9-1.0']
    df_test['score_band'] = pd.cut(df_test['score'], bins=bins, labels=labels)

    print(f"\n  Score Band Analysis (test set — actual outcomes):")
    print(f"  {'Band':<12} {'n':>6}  {'actual_wr':>9}  {'avg_pnl':>9}")
    for band in labels:
        grp = df_test[df_test['score_band'] == band]
        if len(grp) == 0:
            continue
        wr   = grp['label_win'].mean() * 100
        apnl = grp['realized_pnl'].mean()
        n    = len(grp)
        print(f"  {band:<12} {n:>6,}   {wr:>7.1f}%   {apnl:>+8.3f}")

    # ── SECTION 6: COUNTERFACTUAL PnL ────────────────────────────────────────
    print("\n[SECTION 6 — COUNTERFACTUAL PnL ANALYSIS (TEST WINDOW)]")

    baseline_pnl = df_test['realized_pnl'].sum()
    baseline_wr  = df_test['label_win'].mean() * 100

    print(f"  Baseline (no filter):  n={len(df_test):,}, total_pnl={baseline_pnl:+.2f}, win_rate={baseline_wr:.1f}%")
    print()
    header = f"  {'Thresh':<8} {'Passed':>7}  {'Pass WR':>8}  {'Blocked':>8}  {'Block WR':>9}  {'Pass PnL':>10}  {'Delta vs base':>13}"
    print(header)
    print("  " + "-" * 68)
    for thresh in [0.30, 0.40, 0.45, 0.50, 0.55, 0.60]:
        passed  = df_test[df_test['score'] >= thresh]
        blocked = df_test[df_test['score'] <  thresh]
        pass_wr  = passed['label_win'].mean() * 100  if len(passed)  else 0.0
        block_wr = blocked['label_win'].mean() * 100 if len(blocked) else 0.0
        pass_pnl = passed['realized_pnl'].sum()
        delta    = pass_pnl - baseline_pnl
        print(f"  {thresh:<8.2f} {len(passed):>7,}  {pass_wr:>7.1f}%  {len(blocked):>8,}  {block_wr:>8.1f}%  {pass_pnl:>+9.2f}  {delta:>+12.2f}")

    # Feature importance
    print(f"\n  Top 12 Feature Importances (walk-forward model):")
    fi = sorted(zip(ALL_FEATURES, model.feature_importances_), key=lambda x: -x[1])
    max_imp = max(v for _, v in fi)
    for feat, imp in fi[:12]:
        bar = '#' * int(imp / max_imp * 30)
        print(f"    {feat:<25} {imp:>6}  {bar}")

    # ── VERDICT ──────────────────────────────────────────────────────────────
    print(f"\n[VERDICT]")
    print(f"  Walk-Forward AUC = {test_auc:.5f}")
    if test_auc < 0.55:
        print("  STATUS: OVERFIT CONFIRMED")
        print("  AUC < 0.55 indicates the model learned price-level patterns specific")
        print("  to the synthetic backtest data. Features do not reliably predict outcome.")
        print("  Implication: v1.1 AUC=0.99977 is entirely price-level memorization.")
    elif test_auc < 0.62:
        print("  STATUS: WEAK / MARGINAL SIGNAL")
        print("  AUC 0.55-0.62: some features carry generalizable signal, but too weak")
        print("  for reliable live deployment. Counterfactual PnL improvement (if any)")
        print("  is likely within noise on real production data.")
    elif test_auc < 0.70:
        print("  STATUS: MODERATE SIGNAL")
        print("  AUC 0.62-0.70: features show meaningful generalization within simulation.")
        print("  Warrants controlled live testing. Not sufficient for automated blocking.")
    else:
        print("  STATUS: STRONG SIGNAL (validate on real data before deploying)")
        print("  AUC > 0.70: features generalize well within simulation. Proceed to")
        print("  real production data evaluation (n>=200 live linked trades).")

    print()
    print("  Caveat: All source data is synthetic backtest simulation (account_id='backfill').")
    print("  Real-world generalization cannot be confirmed until n>=200 live linked trades")
    print("  with full feature set are available (currently: n=7 all-loss live trades).")
    print(SEP)


if __name__ == "__main__":
    main()
