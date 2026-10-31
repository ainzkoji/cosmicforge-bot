"""The strategy loader may drop params only when the constructor's signature rejects them.

The old loader caught *any* TypeError from ``cls(client, interval, **params)``
and rebuilt the strategy with no params. A TypeError raised inside ``__init__``
-- a bug -- was silently converted into a strategy running on defaults: the same
failure class as the get_signal retry that relabelled NO_OPPORTUNITY as
SESSION_BLOCKED.

``build_strategy`` no longer constructs any strategy class: every stored name
(with or without params) binds the CATI runtime, so there is no constructor
TypeError to swallow. That is covered by
``tests/trading_intelligence/test_cati_sole_runtime.py::
test_historical_configuration_never_constructs_an_old_strategy``. The
signature-binding rule itself is still pinned here.
"""
from __future__ import annotations

import app.strategy.loader as loader


def test_rejection_is_decided_by_signature_binding_only():
    class Sig:
        def __init__(self, client, interval, fast=10):
            pass

    assert loader._signature_rejects(Sig, client=None, interval="15m", slow=3) is True
    assert loader._signature_rejects(Sig, client=None, interval="15m", fast=3) is False
