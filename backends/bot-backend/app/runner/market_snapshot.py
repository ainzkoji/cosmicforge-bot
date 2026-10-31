"""Re-export shim: the immutable MarketSnapshot now lives in ``app.market_data.market_snapshot``.

Moved in Section I Batch 7 (legacy runner consolidation): CATI reads the snapshot contract and it is
market data, not runner state. This module IS the moved module (``sys.modules`` alias), so an import or
a monkeypatch through either path reaches the same objects. New code imports the new path.
"""
import sys

from app.market_data import market_snapshot as _moved

sys.modules[__name__] = _moved
