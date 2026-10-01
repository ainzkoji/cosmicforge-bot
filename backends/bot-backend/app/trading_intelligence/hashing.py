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
from typing import Iterable


def stable_hash(payload: object) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def stable_hash_of_json_list(item_json: Iterable[str]) -> str:
    """``stable_hash(items)`` computed incrementally: each element is given already serialized exactly as
    ``stable_hash`` serializes it (``json.dumps(x, sort_keys=True, separators=(",", ":"), default=str)``).
    Identical digest, without materializing the whole list or its JSON text."""
    h = hashlib.sha256(b"[")
    for i, s in enumerate(item_json):
        if i:
            h.update(b",")
        h.update(s.encode("utf-8"))
    h.update(b"]")
    return h.hexdigest()


class TextHasher:
    """``stable_hash(text)`` for a text fed in chunks (JSON string escaping is per character, so escaping
    each chunk and concatenating equals escaping the whole). Identical digest, constant memory."""

    def __init__(self) -> None:
        self._h = hashlib.sha256(b'"')

    def update(self, chunk: str) -> None:
        self._h.update(json.dumps(chunk)[1:-1].encode("utf-8"))

    def hexdigest(self) -> str:
        h = self._h.copy()
        h.update(b'"')
        return h.hexdigest()


def short_id(prefix: str, payload: object, length: int = 24) -> str:
    """Deterministic ``prefix_<hexhash>`` id from analytical content -- never
    a random UUID (P2 determinism: analytical identity must be a function of
    analytical content, not of when/where it was computed)."""
    return f"{prefix}_{stable_hash(payload)[:length]}"


__all__ = ["stable_hash", "short_id", "stable_hash_of_json_list", "TextHasher"]
