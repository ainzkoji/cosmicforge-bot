"""CosmicForge Autonomous Trading Intelligence (CATI) -- shadow market intelligence.

This package computes deterministic, causal, tenant-free market analysis
(``MarketState``) and a probabilistic regime distribution on top of it. It is
strictly SHADOW-ONLY:

- It never places, cancels or amends an order.
- It never reads user/bot/account state (capital, positions, risk, credentials).
- It never changes signal, threshold, sizing, risk or execution behavior in
  the existing V2 pipeline.
- Its failures never stop V2 execution.

See ``app/trading_intelligence/versions.py`` for the schema/engine version
identity that every produced record carries, and
``app/trading_intelligence/integration/snapshot_adapter.py`` for the single
entry point that turns an existing, immutable ``MarketSnapshot`` into CATI
inputs.
"""
