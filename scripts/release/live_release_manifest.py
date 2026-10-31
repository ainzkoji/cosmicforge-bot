#!/usr/bin/env python3
"""Validate a live-release manifest and print its SHA-256 for review (master prompt section 30).

    python scripts/release/live_release_manifest.py <manifest.json>

This tool only READS. It never writes the allowlist, a manifest, an image or any other file, and it refuses to run in
CI (``CI`` or ``GITHUB_ACTIONS`` set): an approved live release is a human act, not a pipeline step.

A live-capable build needs two independent keys (``backends/shared/shared_lib/core/live_build.py``):

  Key A (reviewed source): ``BUILD_PERMITS_LIVE = True`` in ``shared_lib/core/production.py`` at the reviewed code
         commit C0, and, in a separate reviewed commit C1 that changes ``shared_lib/core/live_release_allowlist.py``
         ONLY, the manifest's SHA-256 added to ``APPROVED_LIVE_RELEASES``.
  Key B (artifact): the manifest baked into the image by ``deploy/containers/live-release.Dockerfile``.

Manifest (UTF-8 JSON object, exactly these fields; the pin is the hash of the exact file bytes):

  schema              "cosmicforge.live-release/1"
  release_id          3-64 characters [A-Za-z0-9._-]
  git_commit          C0, 40 lowercase hex (the reviewed code commit; the image's BUILD_REVISION)
  source_tree_sha256  the source identity of C0 (printed by this tool as ``source_tree_sha256``)
  reviewers           at least two distinct reviewers
  approval_refs       at least one approval reference (change ticket, review URL, owner decision id)
  created_at          "YYYY-MM-DDTHH:MM:SSZ"

Checks: the manifest is valid by the SAME parser the runtime uses; its ``source_tree_sha256`` equals the source
identity of this checkout; the checkout has no modified or untracked source under the release roots; HEAD is
``git_commit`` or differs from it only in the allowlist module. The output is one JSON object for the reviewers. A
capable build still authorizes nothing: every go-live condition and the operator's ``LIVE_ORDER_SUBMISSION_ENABLED``
stay separate acts.

Exit status: 0 valid; 2 usage; 3 refused (CI); 4 manifest invalid; 5 checkout does not match the manifest.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "backends" / "shared"))

from shared_lib.core import live_build as LB  # noqa: E402

ROOTS = {"shared_lib": REPO / "backends" / "shared" / "shared_lib", "app": REPO / "backends" / "bot-backend" / "app"}
ALLOWLIST_PATH = "backends/shared/shared_lib/core/live_release_allowlist.py"
EXIT_OK, EXIT_USAGE, EXIT_CI, EXIT_INVALID, EXIT_MISMATCH = 0, 2, 3, 4, 5


def _git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), *args], check=True, capture_output=True, text=True).stdout


def in_ci(environ=os.environ) -> bool:
    return any(str(environ.get(name, "")).strip() for name in ("CI", "GITHUB_ACTIONS"))


def check(manifest_path: Path, *, roots=ROOTS, git=_git) -> tuple:
    """(exit_code, report). Pure reads."""
    raw = Path(manifest_path).read_bytes()
    report = {"manifest_sha256": LB.manifest_sha256(raw), "valid": False}
    try:
        doc = LB.parse_manifest(raw)
    except LB.ManifestInvalid as exc:
        report.update(reason=exc.code, detail=exc.detail)
        return EXIT_INVALID, report
    report.update(release_id=doc["release_id"], git_commit=doc["git_commit"],
                  reviewers=doc["reviewers"], approval_refs=doc["approval_refs"])
    problems = []
    try:
        report["source_tree_sha256"] = LB.source_tree_sha256(roots)
    except OSError as exc:
        problems.append(f"{LB.SOURCE_UNREADABLE}:{type(exc).__name__}")
    else:
        if report["source_tree_sha256"] != doc["source_tree_sha256"]:
            problems.append(LB.SOURCE_MISMATCH)
    rel_roots = [str(Path(p).relative_to(REPO).as_posix()) for p in roots.values()] if roots is ROOTS else []
    try:
        dirty = [line for line in git("status", "--porcelain", "--ignored", "--", *rel_roots).splitlines()
                 if line.strip().endswith(".py")] if rel_roots else []
        if dirty:
            problems.append("CHECKOUT_HAS_MODIFIED_OR_UNTRACKED_SOURCE")
        head = git("rev-parse", "HEAD").strip()
        report["head"] = head
        if head != doc["git_commit"]:
            changed = [p for p in git("diff", "--name-only", doc["git_commit"], head).splitlines() if p.strip()]
            if changed != [ALLOWLIST_PATH] and changed:
                problems.append("HEAD_DIFFERS_FROM_RELEASE_COMMIT_BEYOND_THE_ALLOWLIST")
    except (OSError, subprocess.CalledProcessError) as exc:
        problems.append(f"GIT_UNAVAILABLE:{type(exc).__name__}")
    if problems:
        report.update(reason=problems[0], problems=problems)
        return EXIT_MISMATCH, report
    report.update(valid=True, reason="MANIFEST_VALID_FOR_REVIEW", next_steps=[
        f"reviewed change to {ALLOWLIST_PATH} ONLY: add \"{report['manifest_sha256']}\" to APPROVED_LIVE_RELEASES",
        f"docker build -f deploy/containers/backend.Dockerfile --build-arg SERVICE=bot-backend "
        f"--build-arg BUILD_REVISION={doc['git_commit']} -t cosmicforge/bot-backend:{doc['release_id']}-base .",
        f"docker build -f deploy/containers/live-release.Dockerfile --build-arg "
        f"BASE_IMAGE=cosmicforge/bot-backend:{doc['release_id']}-base -t cosmicforge/bot-backend:{doc['release_id']} "
        f"<directory holding only live-release/manifest.json>",
        "the image is then live-CAPABLE only; live orders still need every go-live condition and the operator's "
        "LIVE_ORDER_SUBMISSION_ENABLED"])
    return EXIT_OK, report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args(argv)
    if in_ci():
        print(json.dumps({"valid": False, "reason": "LIVE_RELEASE_TOOL_REFUSED_IN_CI"}))
        return EXIT_CI
    if not args.manifest.is_file():
        print(json.dumps({"valid": False, "reason": LB.MANIFEST_ABSENT}))
        return EXIT_USAGE
    code, report = check(args.manifest)
    print(json.dumps(report, indent=2, sort_keys=True))
    return code


if __name__ == "__main__":
    sys.exit(main())
