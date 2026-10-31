"""Key A of the live-capable build (``shared_lib.core.live_build``): the reviewed allowlist of approved live releases.

Each entry is the lowercase hex SHA-256 of the exact bytes of one approved release manifest. It is EMPTY in this
repository and must stay empty in every development, CI and staging build. Adding an entry is a reviewed source change
made by people, after ``scripts/release/live_release_manifest.py`` has validated the manifest and printed its hash;
no script, setting or deployment step writes this file.

This module is the only file excluded from the release's source identity (``live_build.source_tree_sha256``), so
the commit that pins a manifest may differ from the manifest's reviewed code commit in this file alone. Keep anything
else out of it.
"""
from __future__ import annotations

APPROVED_LIVE_RELEASES: frozenset = frozenset()
