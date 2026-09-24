"""The one canonical stable-hash helper for every CATI analytical record.

Mirrors ``app/replay/identity.py``'s ``ReplayIdentity.replay_hash`` pattern:
sha256 over ``json.dumps(..., sort_keys=True, separators=(",", ":"))`` so
dict ordering never moves the hash. Every contract module that needs a
deterministic id/hash should import ``stable_hash`` from here rather than
redefining it -- Sections 9-10 originally each had their own private copy;
this module is the single source of truth going forward.
"""
from __future__ import annotations

import hashlib
import json


def stable_hash(payload: object) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def short_id(prefix: str, payload: object, length: int = 24) -> str:
    """Deterministic ``prefix_<hexhash>`` id from analytical content -- never
    a random UUID (P2 determinism: analytical identity must be a function of
    analytical content, not of when/where it was computed)."""
    return f"{prefix}_{stable_hash(payload)[:length]}"


__all__ = ["stable_hash", "short_id"]
