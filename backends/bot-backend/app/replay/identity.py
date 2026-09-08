"""Replay identity and manifest (§13.9), and the determinism contract (§13.12).

A replay result is only worth something if someone can reproduce it. That means
the run has to record everything that could change its output — the data, the
code, the policy, the costs, the fill semantics, the seed — and it has to
record them in a way that makes an accidental change visible rather than
silent.

Hence a manifest with a ``replay_hash``: the same dataset, revision, policy,
cost model, fill model and seed produce the same hash, and any difference
produces a different one. Two runs that claim to be comparable and are not will
say so.

Provenance is fixed at ``REPLAY`` and cannot be overridden. §13.10 requires
replay evidence to be impossible to confuse with organic paper or live
evidence, and a settable provenance field is exactly how that guarantee gets
lost six months later.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from shared_lib.persistence.evidence_schema import REPLAY


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def dataset_hash(series: Mapping[str, Mapping[str, Sequence[Any]]]) -> str:
    """A fingerprint of the exact candles a replay was given.

    Built from the data itself rather than from a filename, because a file can
    be edited in place and a path proves nothing about its contents.
    """
    digest = hashlib.sha256()
    for symbol in sorted(series):
        for timeframe in sorted(series[symbol]):
            rows = series[symbol][timeframe]
            digest.update(f"{symbol}|{timeframe}|{len(rows)}|".encode())
            for row in rows:
                digest.update(repr(row).encode())
    return digest.hexdigest()[:32]


def code_revision() -> tuple[str | None, str | None, bool | None]:
    """``(revision, branch, working_tree_dirty)``, best effort.

    A dirty tree is recorded rather than refused: a research run against
    uncommitted work is legitimate, but a result that cannot say the tree was
    dirty is not.
    """
    def _git(*args: str) -> str | None:
        try:
            out = subprocess.run(
                ["git", *args], capture_output=True, text=True, timeout=10,
            )
            return (out.stdout or "").strip() or None
        except Exception:
            return None

    revision = _git("rev-parse", "HEAD")
    branch = _git("rev-parse", "--abbrev-ref", "HEAD")
    status = _git("status", "--porcelain")
    dirty = None if status is None else bool(status)
    return revision, branch, dirty


@dataclass(frozen=True)
class ReplayIdentity:
    """Everything needed to reproduce one replay, and nothing that varies."""

    dataset_id: str
    dataset_hash: str
    symbols: tuple[str, ...]
    timeframes: tuple[str, ...]
    start_ms: int
    end_ms: int
    policy_hash: str
    strategy_id: str
    strategy_version: str
    cost_model_hash: str
    fill_model: str
    intrabar_policy: str
    seed: int | None = None
    code_revision: str | None = None
    branch: str | None = None
    working_tree_dirty: bool | None = None

    replay_id: str = field(default_factory=lambda: f"rpl_{uuid.uuid4().hex[:20]}")
    created_at: str = field(default_factory=_now)

    #: Not a parameter. §13.10 — replay evidence must never be mistakable for
    #: organic evidence, so this is not settable.
    provenance: str = field(default=REPLAY, init=False)

    @property
    def replay_hash(self) -> str:
        """Fingerprint of everything that determines the result.

        Deliberately excludes ``replay_id`` and ``created_at``: two runs of the
        same thing at different times must hash the same, or the hash cannot be
        used to check reproducibility.
        """
        payload = {
            k: v for k, v in asdict(self).items()
            if k not in {"replay_id", "created_at"}
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:32]

    def manifest(self) -> dict[str, Any]:
        """The §13.9 record, ready to persist alongside the results."""
        return {**asdict(self), "provenance": self.provenance,
                "replay_hash": self.replay_hash}

    def reproduces(self, other: "ReplayIdentity") -> bool:
        """True when two runs are genuinely comparable."""
        return self.replay_hash == other.replay_hash

    def differences(self, other: "ReplayIdentity") -> dict[str, tuple[Any, Any]]:
        """Exactly which inputs differ. Empty when the runs are comparable."""
        mine, theirs = asdict(self), asdict(other)
        return {
            key: (mine[key], theirs[key])
            for key in sorted(mine)
            if key not in {"replay_id", "created_at"} and mine[key] != theirs[key]
        }
