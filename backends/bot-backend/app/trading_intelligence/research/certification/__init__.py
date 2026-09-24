"""CATI Section 22 -- Research & Certification Standard.

Measures the FROZEN deterministic CATI system; it never tunes it. The
framework working and CATI passing certification are separate outcomes:

* ``contracts``    -- stages, statuses, scope, run / stage-result records
* ``policy``       -- the one versioned CertificationPolicy (fail-closed on
                      every threshold the source specification does not give)
* ``dataset``      -- immutable certification dataset manifest
* ``splits``       -- chronological partitions, purge/embargo, holdout guard
* ``registry``     -- append-only experiment + holdout registries
* ``freeze``       -- PolicyFreezeManifest (hash of every decision policy)
* ``stats``        -- expectancy, block bootstrap, calibration, drawdown/tail,
                      stratification, concentration, PBO/CSCV, deflated Sharpe
* ``replay_venue`` -- modeled historical venue economics (explicit provenance)
* ``replay``       -- FAST / MEDIUM / STRESS / FULL / HOLDOUT on the canonical
                      CATI controller (no parallel "research CATI")
* ``gates``        -- promotion gates A-I and the overall status (no averaging)
* ``forward_demo`` -- ForwardDemoCertificationTracker (cannot fabricate days)
* ``report``       -- deterministic CertificationReport artifact
* ``cli``          -- ``python -m app.trading_intelligence.research.certification``

CATI active execution stays OFF; Section 25 owns promotion. No ML here.
"""
