# Entry Model Baseline Comparison

- Dataset rows: 326
- Train rows: 278
- Temporal holdout rows: 48
- Best holdout AUC model: lightgbm_fixed_baseline
- LightGBM minus logistic AUC: 0.100737
- LightGBM minus logistic walk-forward mean AUC: -0.001793
- Recommendation: Do not continue with LightGBM; logistic baseline performs better.
- Holdout caution: this comparison uses one 48-row temporal holdout and is not an acceptance result.

| Model | Holdout AUC | WF AUC Mean | WF AUC Std | Quartile Gap | Top-Q Precision | TP-Hit Rate | PF Proxy |
|---|---:|---:|---:|---:|---:|---:|---:|
| logistic_regression | 0.5749 | 0.6071 | 0.0367 | 0.1667 | 0.3333 | 0.2500 | 0.9402 |
| lightgbm_fixed_baseline | 0.6757 | 0.6053 | 0.0627 | 0.4167 | 0.4167 | 0.2500 | 1.1024 |

This is an offline comparison baseline only. No baseline model is approved for deployment.
The existing v2.0 candidate remains rejected; this fixed-parameter result does not create an artifact.
