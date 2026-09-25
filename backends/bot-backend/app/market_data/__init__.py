"""Multi-asset, venue-aware market-data platform (Phase 4).

Raw provenance is never merged: every candle / feature observation / FX
quote carries its venue (or provider), venue symbol, product and source.
Missing data is UNAVAILABLE with a reason, never zero, never filled.
Resampling reuses ``app.research.dataset.derive`` (complete, aligned,
gapless windows only).
"""
