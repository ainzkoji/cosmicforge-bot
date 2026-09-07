# ML Runtime vs Experiments

CosmicForge has one runtime ML path and several offline research/data paths. The
names can look similar in admin screens and model folders, so this note defines
the boundary future contributors must preserve.

## Runtime ML Scorer/Gate

The runtime scorer lives in `app/ml/scorer.py`.

- It is the only ML path that can affect trading.
- It is disabled when `ML_ENABLED=false`.
- It can only block entries when `ML_ENABLED=true` and `ML_SHADOW_MODE=false`.
- It is additive only: policy, risk, exposure, event blackout, and execution
  safety remain authoritative.
- It uses the canonical `entry_quality_v2` runtime contract from
  `shared_lib/ml/contract.py`.
- Runtime artifacts must be placed under `models/production/`.
- Legacy artifacts, datasets, and experiment outputs must not be referenced as
  runtime model paths.

## Shadow ML

Shadow ML is the same runtime scorer with `ML_ENABLED=true` and
`ML_SHADOW_MODE=true`.

- It may write `ml_score` and `ml_action` to `decision_traces`.
- It must not block entries except if a separately reviewed hard-block floor is
  explicitly configured.
- It must not deploy or train models.

## Offline Experiments

Offline experiments live under `scripts/ml/` and `models/experiments/`.

- `phase_f_experiment.py` compares the 21 base runtime features against
  21 base + 7 event/reaction features.
- `phase_h_experiment.py` compares the 21 base runtime features against
  21 base + 3 event-timing features.
- Experiment outputs are research evidence only.
- Experiment artifacts must be marked `deployed=false` and
  `allowed_runtime=false`.
- Experiments must not update `.env`, scorer configuration, production model
  paths, or live trading behavior.

## Dataset-Only Event/News Features

Event and news feature flags control dataset construction, not live scoring.

- `ML_EVENT_FEATURES_ENABLED` controls event timing feature generation.
- `ML_MARKET_REACTION_FEATURES_ENABLED` controls market reaction feature
  generation for datasets.
- `ML_NEWS_VALIDATION_FEATURES_ENABLED` and `ML_RAW_NEWS_FEATURES_ENABLED` are
  dataset/research flags only.
- These columns are not part of runtime `FEATURE_COLUMNS` unless a future
  production contract explicitly changes and passes scorer compatibility tests.

## Folder Intent

- `models/production/`: approved runtime artifacts only.
- `models/legacy/`: historical artifacts retained for audit/backtesting.
- `models/experiments/`: offline research outputs, never runtime.
- `models/datasets/`: parquet datasets and dataset metadata.

## Current State

As of the cleanup that introduced this document:

- `ML_ENABLED=false`.
- Runtime scoring is not active.
- The old `entry_quality_v1.1_20260322` artifact is legacy/incompatible with the
  current 21-feature `entry_quality_v2` runtime contract.
- Phase F/H event-feature outputs are offline-only and isolated from live
  execution.
