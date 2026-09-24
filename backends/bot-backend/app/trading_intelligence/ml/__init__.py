"""CATI Section 23 -- AI/ML as interchangeable ESTIMATORS behind CATI interfaces.

ML is never an end-to-end trading authority. Each role replaces (after
shadow + promotion) only one estimator; every deterministic gate -- data
quality, economic admission, veto, portfolio selection, hard risk, executor
validation, mechanical protection -- stays outside and above the model.

* ``contracts``  -- roles + hard boundaries, statuses, schemas, model identity
* ``features``   -- tenant-neutral, point-in-time feature schemas per role
* ``datasets``   -- training sets with MARKET / EXECUTION / ACCOUNT separation
* ``artifacts``  -- immutable artifacts; legacy V2 artifacts are rejected
* ``registry``   -- append-only model registry + status history
* ``training``   -- per-role training gates; calibrated, chronological trainers
* ``boundaries`` -- the hard limits each role's output is clamped to
* ``shadow``     -- shadow / champion-challenger evidence (never affects orders)
* ``monitoring`` -- drift (PSI) and live calibration
* ``promotion``  -- the explicit model-evidence gate (+ Section 25 phase)
* ``legacy``     -- classification of the pre-existing V2 ML system
* ``config``     -- CATI_ML_* flags (OFF by default; never authority alone)

Legacy ``app/ml`` (the V2 LightGBM entry scorer) stays supported as V2 until
Section 25 retires it; it is not a CATI estimator.
"""
