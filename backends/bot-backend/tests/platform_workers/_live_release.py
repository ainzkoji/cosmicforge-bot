"""Test-only: a live-capable build through the REAL section-30 mechanism (``shared_lib.core.live_build``).

Both release keys are injected into the test process only -- ``BUILD_PERMITS_LIVE`` and the allowlist through
``monkeypatch``, the manifest as a file under the test's ``tmp_path``, the build commit through ``BUILD_VERSION``.
Nothing here writes the repository's allowlist or a manifest anywhere a production path reads.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional

from shared_lib.core import live_build as LB
from shared_lib.core import live_release_allowlist as ALLOW
from shared_lib.core import production as P


def commit_id(seed: str = "release") -> str:
    """A 40-hex commit id built at run time (never a literal)."""
    return hashlib.sha1(seed.encode()).hexdigest()


def manifest_doc(**overrides: Any) -> Dict[str, Any]:
    doc = {"schema": LB.MANIFEST_SCHEMA, "release_id": "test-live-0001", "git_commit": commit_id(),
           "source_tree_sha256": LB.running_source_sha256(), "reviewers": ["reviewer-one", "reviewer-two"],
           "approval_refs": ["TEST-APPROVAL-1"], "created_at": "2026-10-31T00:00:00Z"}
    doc.update(overrides)
    return doc


def install_capable_release(monkeypatch, tmp_path: Path, *, constant: bool = True, pin: bool = True,
                            manifest: Optional[bytes] = None, build_version: Optional[str] = "default",
                            **overrides: Any) -> Dict[str, Any]:
    raw = manifest if manifest is not None else json.dumps(manifest_doc(**overrides), sort_keys=True).encode()
    path = Path(tmp_path) / "live-release" / "manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    digest = LB.manifest_sha256(raw)
    monkeypatch.setattr(P, "BUILD_PERMITS_LIVE", constant)
    monkeypatch.setattr(ALLOW, "APPROVED_LIVE_RELEASES", frozenset({digest}) if pin else frozenset())
    monkeypatch.setattr(LB, "LIVE_RELEASE_MANIFEST_PATH", str(path))
    if build_version is None:
        monkeypatch.delenv("BUILD_VERSION", raising=False)
    else:
        monkeypatch.setenv("BUILD_VERSION", commit_id() if build_version == "default" else build_version)
    return {"path": path, "sha256": digest, "raw": raw}
