"""Re-export shim: the per-bot policy resolver now lives in ``app.policy.effective_policy``.

Moved in Section I Batch 7 (legacy runner consolidation): CATI production execution resolves bot policy
through it, so it belongs with the policy layer, not the legacy runner. This module IS the moved module
(``sys.modules`` alias), so an import or a monkeypatch through either path reaches the same objects.
New code imports the new path.
"""
import sys

from app.policy import effective_policy as _moved

sys.modules[__name__] = _moved
