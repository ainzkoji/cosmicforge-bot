"""The CATI research evaluator for a registered, rule-based mandate (Section H, Step 2.4 - 2.6).

One evaluator, inside CATI's research package; not a second strategy engine. The strategy rules live in
``app.trading_intelligence.families`` and are imported, never re-implemented here.

* ``simulator`` -- daily portfolio accounting: next-open fills, conservative stops, signed funding, four ledgers
* ``metrics``   -- performance figures, benchmarks, the frozen pass rule
* ``official``  -- the one controlled run: pinned inputs, run log, embargoed holdout, artifacts, verdict
* ``report``    -- the certification report (Markdown) generated from a run's machine-readable result
* ``__main__``  -- operator commands

Research only: nothing here places an order, promotes a family or changes a governance phase.
"""
