"""Whether THIS build artifact is capable of live (real-money) orders -- master prompt section 30.

A live-capable build needs two independent keys, held by different parties, and both must match:

* **Key A -- reviewed source.** ``shared_lib.core.production.BUILD_PERMITS_LIVE`` is True *and* the SHA-256 of the
  approved release manifest is pinned in ``shared_lib.core.live_release_allowlist.APPROVED_LIVE_RELEASES``. Both are
  reviewed code; in this repository the constant is False and the allowlist is empty.
* **Key B -- the release artifact.** A release manifest baked into the image at ``LIVE_RELEASE_MANIFEST_PATH`` by
  the release overlay (``deploy/containers/live-release.Dockerfile``), never by the default Dockerfile or compose.
  Its exact bytes hash to a pinned entry, it names at least two distinct reviewers and the approval references, its
  ``git_commit`` equals the running build's commit (``BUILD_VERSION``, baked from ``--build-arg BUILD_REVISION``),
  and its ``source_tree_sha256`` equals the code this process actually runs (computed here from the installed
  sources, so a manifest cannot be carried onto different code).

Anything missing, malformed, unpinned or mismatched answers ``capable=False`` with one stable reason code (below).
A capable build authorizes nothing: it only lets go-live condition 15 pass. Every other go-live condition, the
operator's ``LIVE_ORDER_SUBMISSION_ENABLED``, the per-venue live switch, consent, certifications, governance, risk
and the transport gates stay in force and are evaluated independently.

Why the source identity excludes the allowlist module: the manifest hash is pinned in source, so the commit that
pins it cannot be the commit the manifest names (the hash would have to contain itself). The manifest names the
reviewed code commit ``C0``; the pin commit ``C1`` changes ``live_release_allowlist.py`` only, which the source
identity excludes, so ``C0`` and ``C1`` have the same ``source_tree_sha256``. ``scripts/release/live_release_manifest.py``
checks that relation before it prints the build command.

Reason codes (stable):
  LIVE_NOT_PERMITTED_IN_THIS_BUILD       BUILD_PERMITS_LIVE is False (this repository)
  LIVE_RELEASE_ALLOWLIST_INVALID         the allowlist is not a frozenset of 64-character lowercase hex digests
  LIVE_RELEASE_NOT_APPROVED_IN_SOURCE    the allowlist is empty (this repository)
  LIVE_RELEASE_MANIFEST_ABSENT           no manifest baked into this artifact (every default image)
  LIVE_RELEASE_MANIFEST_UNREADABLE       the manifest exists but cannot be read
  LIVE_RELEASE_MANIFEST_MALFORMED        not JSON, wrong schema, unknown/missing field, bad value (detail names it)
  LIVE_RELEASE_REVIEWERS_INSUFFICIENT    fewer than two distinct reviewers
  LIVE_RELEASE_APPROVAL_REFS_MISSING     no approval reference
  LIVE_RELEASE_MANIFEST_NOT_APPROVED     the manifest's SHA-256 is not pinned in the allowlist
  LIVE_RELEASE_BUILD_COMMIT_UNKNOWN      the running build does not know its commit (BUILD_VERSION unset/unknown)
  LIVE_RELEASE_COMMIT_MISMATCH           the manifest names a different commit than the running build
  LIVE_RELEASE_SOURCE_UNREADABLE         the installed sources cannot be hashed
  LIVE_RELEASE_SOURCE_MISMATCH           the installed sources are not the reviewed release's sources
  LIVE_BUILD_CAPABLE                     both keys present and matching (never in this repository)
"""
from __future__ import annotations

import functools
import hashlib
import importlib.util
import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

#: Where the release overlay bakes the manifest. A constant on purpose: no setting can point the check elsewhere.
LIVE_RELEASE_MANIFEST_PATH = "/etc/cosmicforge/live-release/manifest.json"
MANIFEST_SCHEMA = "cosmicforge.live-release/1"
MANIFEST_MAX_BYTES = 65_536
#: The source identity: every ``*.py`` file under these package roots, except EXCLUDED_FROM_SOURCE_IDENTITY.
SOURCE_PACKAGES = ("shared_lib", "app")
EXCLUDED_FROM_SOURCE_IDENTITY = frozenset({"shared_lib/core/live_release_allowlist.py"})

NOT_PERMITTED = "LIVE_NOT_PERMITTED_IN_THIS_BUILD"
ALLOWLIST_INVALID = "LIVE_RELEASE_ALLOWLIST_INVALID"
NOT_APPROVED_IN_SOURCE = "LIVE_RELEASE_NOT_APPROVED_IN_SOURCE"
MANIFEST_ABSENT = "LIVE_RELEASE_MANIFEST_ABSENT"
MANIFEST_UNREADABLE = "LIVE_RELEASE_MANIFEST_UNREADABLE"
MANIFEST_MALFORMED = "LIVE_RELEASE_MANIFEST_MALFORMED"
REVIEWERS_INSUFFICIENT = "LIVE_RELEASE_REVIEWERS_INSUFFICIENT"
APPROVAL_REFS_MISSING = "LIVE_RELEASE_APPROVAL_REFS_MISSING"
MANIFEST_NOT_APPROVED = "LIVE_RELEASE_MANIFEST_NOT_APPROVED"
BUILD_COMMIT_UNKNOWN = "LIVE_RELEASE_BUILD_COMMIT_UNKNOWN"
COMMIT_MISMATCH = "LIVE_RELEASE_COMMIT_MISMATCH"
SOURCE_UNREADABLE = "LIVE_RELEASE_SOURCE_UNREADABLE"
SOURCE_MISMATCH = "LIVE_RELEASE_SOURCE_MISMATCH"
CAPABLE = "LIVE_BUILD_CAPABLE"
REASON_CODES = (NOT_PERMITTED, ALLOWLIST_INVALID, NOT_APPROVED_IN_SOURCE, MANIFEST_ABSENT, MANIFEST_UNREADABLE,
                MANIFEST_MALFORMED, REVIEWERS_INSUFFICIENT, APPROVAL_REFS_MISSING, MANIFEST_NOT_APPROVED,
                BUILD_COMMIT_UNKNOWN, COMMIT_MISMATCH, SOURCE_UNREADABLE, SOURCE_MISMATCH, CAPABLE)

_HEX64 = re.compile(r"[0-9a-f]{64}")
_HEX40 = re.compile(r"[0-9a-f]{40}")
_RELEASE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{2,63}")
_CREATED_AT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
MANIFEST_FIELDS = ("schema", "release_id", "git_commit", "source_tree_sha256", "reviewers", "approval_refs",
                   "created_at")


@dataclass(frozen=True)
class LiveBuildStatus:
    capable: bool
    reason: str
    detail: str = ""
    release_id: Optional[str] = None
    manifest_sha256: Optional[str] = None
    git_commit: Optional[str] = None
    build_commit: Optional[str] = None
    keys: Dict[str, bool] = field(default_factory=dict)

    def to_plain(self) -> Dict[str, Any]:
        return asdict(self)


class ManifestInvalid(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code, self.detail = code, detail


# ── manifest ─────────────────────────────────────────────────────────────────────────────────────────────────
def manifest_sha256(raw: bytes) -> str:
    """The pin of a manifest: SHA-256 of its exact bytes (no re-serialisation)."""
    return hashlib.sha256(raw).hexdigest()


def parse_manifest(raw: bytes) -> Dict[str, Any]:
    """Parse and validate a manifest; raises ``ManifestInvalid`` with a stable code. Shared by the runtime check and
    the release script, so the script can never approve a manifest the runtime would refuse."""
    if len(raw) > MANIFEST_MAX_BYTES:
        raise ManifestInvalid(MANIFEST_MALFORMED, "too large")
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise ManifestInvalid(MANIFEST_MALFORMED, "not UTF-8 JSON")
    if not isinstance(doc, dict):
        raise ManifestInvalid(MANIFEST_MALFORMED, "not an object")
    unknown = sorted(set(doc) - set(MANIFEST_FIELDS))
    missing = [k for k in MANIFEST_FIELDS if k not in doc]
    if unknown or missing:
        raise ManifestInvalid(MANIFEST_MALFORMED, ("unknown:" + ",".join(unknown)) if unknown
                              else ("missing:" + ",".join(missing)))
    if doc["schema"] != MANIFEST_SCHEMA:
        raise ManifestInvalid(MANIFEST_MALFORMED, "schema")
    if not isinstance(doc["release_id"], str) or not _RELEASE_ID.fullmatch(doc["release_id"]):
        raise ManifestInvalid(MANIFEST_MALFORMED, "release_id")
    if not isinstance(doc["git_commit"], str) or not _HEX40.fullmatch(doc["git_commit"]):
        raise ManifestInvalid(MANIFEST_MALFORMED, "git_commit")
    if not isinstance(doc["source_tree_sha256"], str) or not _HEX64.fullmatch(doc["source_tree_sha256"]):
        raise ManifestInvalid(MANIFEST_MALFORMED, "source_tree_sha256")
    created = doc["created_at"]
    if not isinstance(created, str) or not _CREATED_AT.fullmatch(created):
        raise ManifestInvalid(MANIFEST_MALFORMED, "created_at")
    try:
        datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ManifestInvalid(MANIFEST_MALFORMED, "created_at")
    reviewers = doc["reviewers"]
    if not isinstance(reviewers, list) or not all(isinstance(r, str) for r in reviewers):
        raise ManifestInvalid(MANIFEST_MALFORMED, "reviewers")
    if len({r.strip().casefold() for r in reviewers if r.strip()}) < 2:
        raise ManifestInvalid(REVIEWERS_INSUFFICIENT, f"{len(reviewers)} listed")
    refs = doc["approval_refs"]
    if not isinstance(refs, list) or not all(isinstance(r, str) for r in refs):
        raise ManifestInvalid(MANIFEST_MALFORMED, "approval_refs")
    if not [r for r in refs if len(r.strip()) >= 3]:
        raise ManifestInvalid(APPROVAL_REFS_MISSING)
    return doc


# ── source identity ──────────────────────────────────────────────────────────────────────────────────────────
def source_files(roots: Mapping[str, Path]) -> Iterable[Tuple[str, Path]]:
    for package, root in sorted(roots.items()):
        root = Path(root)
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            rel = package + "/" + path.relative_to(root).as_posix()
            if rel not in EXCLUDED_FROM_SOURCE_IDENTITY:
                yield rel, path


def source_tree_sha256(roots: Mapping[str, Path]) -> str:
    """SHA-256 over ``<relpath> NUL <sha256(content)> LF`` for every source file, sorted by path. CRLF is read as
    LF (Python reads source with universal newlines either way), so a Linux image, a Windows checkout and the Git
    blobs of one commit share one identity."""
    outer = hashlib.sha256()
    count = 0
    for rel, path in sorted(source_files(roots)):
        data = path.read_bytes().replace(b"\r\n", b"\n")
        outer.update(rel.encode("utf-8") + b"\0" + hashlib.sha256(data).hexdigest().encode("ascii") + b"\n")
        count += 1
    if count == 0:
        raise FileNotFoundError("no source files under the release roots")
    return outer.hexdigest()


def installed_source_roots() -> Dict[str, Path]:
    """The package roots this process imports from (the bot backend's ``app`` next to ``shared_lib``)."""
    roots = {}
    for package in SOURCE_PACKAGES:
        spec = importlib.util.find_spec(package)
        if spec is None or not spec.submodule_search_locations:
            raise FileNotFoundError(f"package {package} not importable")
        roots[package] = Path(list(spec.submodule_search_locations)[0])
    return roots


@functools.lru_cache(maxsize=4)
def _installed_source_sha256(key: Tuple[Tuple[str, str], ...]) -> str:
    return source_tree_sha256({k: Path(v) for k, v in key})


def running_source_sha256() -> str:
    """Hashed once per process (only ever reached when a manifest is pinned and present)."""
    roots = installed_source_roots()
    return _installed_source_sha256(tuple(sorted((k, str(v)) for k, v in roots.items())))


def build_commit() -> Optional[str]:
    value = str(os.environ.get("BUILD_VERSION") or "").strip().lower()
    return value if _HEX40.fullmatch(value) else None


# ── the status ───────────────────────────────────────────────────────────────────────────────────────────────
def _allowlist() -> Any:
    from shared_lib.core import live_release_allowlist as allow
    return allow.APPROVED_LIVE_RELEASES


def live_build_status() -> LiveBuildStatus:
    """Read-only. Evaluated in key order; the first failing check is the reason. Never raises."""
    from shared_lib.core import production as P
    keys = {"source_constant": bool(P.BUILD_PERMITS_LIVE is True), "source_allowlist": False,
            "artifact_manifest": False, "manifest_pinned": False, "commit_match": False, "source_match": False}

    def refuse(reason, detail="", **extra):
        return LiveBuildStatus(False, reason, detail, keys=dict(keys), **extra)

    if not keys["source_constant"]:
        return refuse(NOT_PERMITTED)
    allow = _allowlist()
    if not isinstance(allow, frozenset) or not all(isinstance(h, str) and _HEX64.fullmatch(h) for h in allow):
        return refuse(ALLOWLIST_INVALID)
    if not allow:
        return refuse(NOT_APPROVED_IN_SOURCE)
    keys["source_allowlist"] = True
    path = Path(LIVE_RELEASE_MANIFEST_PATH)
    try:
        if not path.is_file():
            return refuse(MANIFEST_ABSENT)
        raw = path.read_bytes()
    except OSError as exc:
        return refuse(MANIFEST_UNREADABLE, type(exc).__name__)
    digest = manifest_sha256(raw)
    try:
        doc = parse_manifest(raw)
    except ManifestInvalid as exc:
        return refuse(exc.code, exc.detail, manifest_sha256=digest)
    keys["artifact_manifest"] = True
    info = dict(manifest_sha256=digest, release_id=doc["release_id"], git_commit=doc["git_commit"])
    if digest not in allow:
        return refuse(MANIFEST_NOT_APPROVED, **info)
    keys["manifest_pinned"] = True
    commit = build_commit()
    if commit is None:
        return refuse(BUILD_COMMIT_UNKNOWN, **info)
    if commit != doc["git_commit"]:
        return refuse(COMMIT_MISMATCH, build_commit=commit, **info)
    keys["commit_match"] = True
    try:
        running = running_source_sha256()
    except Exception as exc:  # unreadable sources are a refusal, never a pass
        return refuse(SOURCE_UNREADABLE, type(exc).__name__, build_commit=commit, **info)
    if running != doc["source_tree_sha256"]:
        return refuse(SOURCE_MISMATCH, build_commit=commit, **info)
    keys["source_match"] = True
    return LiveBuildStatus(True, CAPABLE, "", build_commit=commit, keys=dict(keys), **info)


def live_build_capable() -> bool:
    return live_build_status().capable
