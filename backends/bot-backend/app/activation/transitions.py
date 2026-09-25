"""Observed activation state changes (structured log + bounded metric).

``observe(decision)`` is called wherever a derived state is read at runtime.
The first observation and every change of state for a (capability, scope)
are logged as ``[ACTIVATION] ...`` and counted in
``activation_state_change_total{component,status,reason_family}``; repeats
are silent. Scopes (which may contain account ids) go to the log line only,
never to a metric label.
"""
from __future__ import annotations

import logging
import threading
from typing import Dict, Tuple

from .model import ActivationDecision

logger = logging.getLogger(__name__)
_lock = threading.Lock()
_last: Dict[Tuple[str, str], str] = {}


def observe(decision: ActivationDecision) -> ActivationDecision:
    key = (decision.capability, decision.scope)
    state = decision.state.value
    with _lock:
        prev = _last.get(key)
        if prev == state:
            return decision
        _last[key] = state
    logger.info("[ACTIVATION] capability=%s scope=%s %s -> %s reason=%s override=%s", decision.capability,
                decision.scope, prev or "UNOBSERVED", state, decision.reason or "-", decision.override)
    try:
        from app.trading_intelligence.observability.metrics import METRICS, reason_family

        METRICS.inc("activation_state_change_total", component=decision.capability.split(":")[0][:48],
                    status=state, reason_family=reason_family(decision.reason))
    except Exception:  # metrics never break the caller
        pass
    return decision


def last_observed() -> Dict[str, str]:
    with _lock:
        return {f"{c}@{s}": v for (c, s), v in _last.items()}


def reset_for_tests() -> None:
    with _lock:
        _last.clear()


__all__ = ["last_observed", "observe", "reset_for_tests"]
