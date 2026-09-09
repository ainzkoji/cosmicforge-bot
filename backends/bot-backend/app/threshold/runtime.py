"""Process-wide wiring for the threshold engine.

Deliberately thin. The only shared objects are the *state store* (which must
survive across strategy instances so a restart does not lose the smoothing
anchor) and the *performance source* (which is a database handle). The engine
itself is cheap and is constructed per strategy.

Policies are cached per resolution key rather than globally, so a per-symbol or
per-bot override cannot leak into another symbol's policy. That is the same
partitioning rule the state store follows, applied to configuration.
"""
from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Any, Mapping

from app.threshold.calibration import PerformanceCalibrator, SqlitePerformanceSource
from app.threshold.policy import EffectiveThresholdPolicy, policy_from_settings
from app.threshold.state import SqliteThresholdStateStore, ThresholdStateStore

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_state_store: ThresholdStateStore | None = None
_performance: PerformanceCalibrator | None = None
_policies: dict[tuple, EffectiveThresholdPolicy] = {}
_research_policy: EffectiveThresholdPolicy | None = None
_startup_reported = False


def _db() -> Any | None:
    """The canonical database, or ``None`` under test.

    Under ``COSMICFORGE_TEST_MODE`` this returns ``None`` so the state store
    stays in memory. Threshold state is written on every evaluated candle, and
    the replay and parity suites drive the real strategy -- without this, a test
    run deposits adaptive state into the production database under test bot ids
    and pollutes the live distribution calibration.
    """
    import os

    if os.environ.get("COSMICFORGE_TEST_MODE") == "1":
        return None
    try:
        from shared_lib.persistence.db import DB

        return DB()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("[THRESHOLD] database unavailable, using in-memory state: %s", exc)
        return None


def get_threshold_state_store() -> ThresholdStateStore:
    """The shared, restart-safe state store."""
    global _state_store
    with _lock:
        if _state_store is None:
            db = _db()
            _state_store = SqliteThresholdStateStore(db) if db is not None else ThresholdStateStore()
        return _state_store


def get_performance_calibrator() -> PerformanceCalibrator:
    global _performance
    with _lock:
        if _performance is None:
            db = _db()
            _performance = PerformanceCalibrator(
                SqlitePerformanceSource(db) if db is not None else None
            )
        return _performance


@contextmanager
def research_policy(policy: EffectiveThresholdPolicy):
    """Run a block against an alternate policy without touching production config.

    This is how RESEARCH mode -- replay and threshold sensitivity work -- varies
    the band or the bounds. It does not create a second engine, a second
    calculation or a second set of settings; it swaps the policy that the one
    engine resolves, for the duration of the block only.
    """
    global _research_policy
    with _lock:
        previous = _research_policy
        _research_policy = policy
    try:
        yield policy
    finally:
        with _lock:
            _research_policy = previous


def get_threshold_policy(
    *,
    symbol: str | None = None,
    venue: str | None = None,
    market_type: str | None = None,
    bot_overrides: Mapping[str, Any] | None = None,
    settings: Any = None,
) -> EffectiveThresholdPolicy:
    """Resolve (and cache) the effective policy for one scope combination.

    A :class:`~app.threshold.policy.ThresholdPolicyError` from here is not
    caught. Contradictory threshold configuration must stop the process, because
    the alternative -- falling back to a default -- is how the previous stack
    ran inert for months without anyone being told.
    """
    with _lock:
        if _research_policy is not None:
            return _research_policy

    if settings is None:
        from app.core.config import settings as _settings

        settings = _settings

    key = (
        id(settings),
        str(symbol or "").upper(),
        str(venue or "").upper(),
        str(market_type or "").upper(),
        tuple(sorted((bot_overrides or {}).items())),
    )
    with _lock:
        cached = _policies.get(key)
        if cached is not None:
            return cached

    policy = policy_from_settings(
        settings,
        symbol=symbol,
        venue=venue,
        market_type=market_type,
        bot_overrides=bot_overrides,
    )
    with _lock:
        _policies[key] = policy
    _report_once(policy, settings)
    return policy


def _report_once(policy: EffectiveThresholdPolicy, settings: Any) -> None:
    """Print one resolved threshold configuration, the first time we resolve one."""
    global _startup_reported
    with _lock:
        if _startup_reported:
            return
        _startup_reported = True
    from app.threshold.migration import log_startup_report

    log_startup_report(policy, settings)


def reset_for_tests() -> None:
    """Clear the process-wide wiring. Used by tests to guarantee isolation."""
    global _state_store, _performance, _startup_reported, _research_policy
    with _lock:
        _state_store = None
        _performance = None
        _research_policy = None
        _policies.clear()
        _startup_reported = False


__all__ = [
    "get_performance_calibrator",
    "research_policy",
    "get_threshold_policy",
    "get_threshold_state_store",
    "reset_for_tests",
]
