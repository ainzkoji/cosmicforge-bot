"""CATI research governance (Section H, Step 2.1-2.2 and 2.5).

One authoritative, committed, append-only research register replaces the per-database counters that
could not see earlier work:

* ``register``   -- the hash-chained register file and its typed views: hypotheses (every attempt, failures
                    included, numbered once), mandates (hash-pinned before any run), amendments, runs and
                    holdout events
* ``statistics`` -- the registered portfolio-level test: serial dependence, multiple testing over the WHOLE
                    register, explicit power; six possible outcomes, never a pass by default
* ``admission``  -- the rule-based admission route (defined, DISABLED until an owner decision is recorded)
* ``holdout``    -- pre-holdout readiness, the recorded authorization, and the open-once access protocol

Nothing here trades, promotes a family, moves a governance phase or reads a credential.
"""
