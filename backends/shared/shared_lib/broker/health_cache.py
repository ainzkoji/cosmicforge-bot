"""
In-process broker health cache.

Prevents N bots sharing one broken broker account from each independently
hammering the exchange with failing requests. When a credential version is
known-bad, subsequent resolution attempts get a fast-path error from the
cache instead of a live network call.

Design:
  - Keyed by (account_id, credential_version) so rotating credentials
    automatically busts the cached failure state.
  - TTL is short enough that a repaired broker reconnects within minutes.
  - Cache holds both healthy and unhealthy records; healthy records are
    refreshed on each successful resolution.
  - Thread-safe: uses a threading.Lock around the dict.
  - Process-scoped singleton; safe to instantiate per-process.

Usage:
    cache = get_broker_health_cache()
    entry = cache.get(account_id, credential_version)
    if entry and not entry.is_healthy:
        raise BrokerResolverError(entry.reason_code, entry.error_message)
    ...
    cache.mark_healthy(account_id, credential_version)
    cache.mark_unhealthy(account_id, credential_version, reason_code, msg)
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Default TTLs (seconds)
_HEALTHY_TTL_SECONDS   = 300   # 5 minutes — re-validate periodically
_UNHEALTHY_TTL_SECONDS = 120   # 2 minutes — allow quick recovery after reconnect

_CacheKey = Tuple[str, int]  # (account_id, credential_version)


@dataclass
class BrokerHealthEntry:
    """
    Cached health state for one (account_id, credential_version) pair.
    """
    account_id:         str
    credential_version: int
    is_healthy:         bool
    reason_code:        Optional[str]      # set when is_healthy=False
    error_message:      Optional[str]      # human-readable failure detail
    recorded_at:        float = field(default_factory=time.monotonic)
    ttl_seconds:        float = _UNHEALTHY_TTL_SECONDS

    @property
    def is_expired(self) -> bool:
        return (time.monotonic() - self.recorded_at) > self.ttl_seconds

    def safe_log_dict(self) -> dict:
        return {
            "account_id":         self.account_id,
            "credential_version": self.credential_version,
            "is_healthy":         self.is_healthy,
            "reason_code":        self.reason_code,
            "age_seconds":        round(time.monotonic() - self.recorded_at, 1),
        }


class BrokerHealthCache:
    """
    Thread-safe, process-scoped broker health cache.

    Instantiate once per process via get_broker_health_cache().
    """

    def __init__(
        self,
        healthy_ttl:   float = _HEALTHY_TTL_SECONDS,
        unhealthy_ttl: float = _UNHEALTHY_TTL_SECONDS,
    ) -> None:
        self._healthy_ttl   = healthy_ttl
        self._unhealthy_ttl = unhealthy_ttl
        self._entries: Dict[_CacheKey, BrokerHealthEntry] = {}
        self._lock = threading.Lock()

    # ── Public interface ───────────────────────────────────────────────────────

    def get(
        self,
        account_id:         str,
        credential_version: int,
    ) -> Optional[BrokerHealthEntry]:
        """
        Return a non-expired cache entry, or None if not cached / expired.
        Expired entries are evicted lazily on access.
        """
        key = (account_id, credential_version)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.is_expired:
                del self._entries[key]
                logger.debug(
                    "broker_health_cache_evict account_id=%s version=%s",
                    account_id, credential_version,
                )
                return None
            return entry

    def mark_healthy(
        self,
        account_id:         str,
        credential_version: int,
    ) -> None:
        """Record a successful auth resolution for this (account, version)."""
        key = (account_id, credential_version)
        entry = BrokerHealthEntry(
            account_id=account_id,
            credential_version=credential_version,
            is_healthy=True,
            reason_code=None,
            error_message=None,
            ttl_seconds=self._healthy_ttl,
        )
        with self._lock:
            prev = self._entries.get(key)
            self._entries[key] = entry
            if prev and not prev.is_healthy:
                logger.info(
                    "broker_health_cache_recovered account_id=%s version=%s "
                    "previous_reason=%s",
                    account_id, credential_version, prev.reason_code,
                )

    def mark_unhealthy(
        self,
        account_id:         str,
        credential_version: int,
        reason_code:        str,
        error_message:      str,
    ) -> None:
        """
        Record a failed auth resolution.

        Subsequent calls to get() within TTL will return this entry, allowing
        callers to skip the DB lookup + decrypt + network call.
        """
        key = (account_id, credential_version)
        entry = BrokerHealthEntry(
            account_id=account_id,
            credential_version=credential_version,
            is_healthy=False,
            reason_code=reason_code,
            error_message=error_message,
            ttl_seconds=self._unhealthy_ttl,
        )
        with self._lock:
            prev = self._entries.get(key)
            self._entries[key] = entry
            if prev is None or prev.is_healthy:
                logger.warning(
                    "broker_health_cache_degraded account_id=%s version=%s "
                    "reason=%s message=%r",
                    account_id, credential_version, reason_code, error_message,
                )
            else:
                logger.debug(
                    "broker_health_cache_still_degraded account_id=%s version=%s "
                    "reason=%s",
                    account_id, credential_version, reason_code,
                )

    def invalidate(self, account_id: str, credential_version: Optional[int] = None) -> int:
        """
        Forcibly evict cache entries for account_id.

        If credential_version is given, evicts only that version.
        If None, evicts ALL versions for the account (use after reconnect).

        Returns number of entries evicted.
        """
        evicted = 0
        with self._lock:
            if credential_version is not None:
                key = (account_id, credential_version)
                if key in self._entries:
                    del self._entries[key]
                    evicted = 1
            else:
                keys_to_del = [
                    k for k in self._entries if k[0] == account_id
                ]
                for k in keys_to_del:
                    del self._entries[k]
                evicted = len(keys_to_del)

        if evicted:
            logger.info(
                "broker_health_cache_invalidated account_id=%s version=%s "
                "entries_evicted=%d",
                account_id, credential_version, evicted,
            )
        return evicted

    def stats(self) -> dict:
        """Return a summary suitable for health-check endpoints."""
        with self._lock:
            total    = len(self._entries)
            healthy  = sum(1 for e in self._entries.values() if e.is_healthy)
            degraded = total - healthy
        return {
            "total_cached":   total,
            "healthy":        healthy,
            "degraded":       degraded,
            "healthy_ttl_s":  self._healthy_ttl,
            "unhealthy_ttl_s": self._unhealthy_ttl,
        }


# ── Process singleton ─────────────────────────────────────────────────────────
_instance: Optional[BrokerHealthCache] = None
_instance_lock = threading.Lock()


def get_broker_health_cache() -> BrokerHealthCache:
    """Return the process-scoped singleton BrokerHealthCache."""
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                _instance = BrokerHealthCache()
    return _instance
