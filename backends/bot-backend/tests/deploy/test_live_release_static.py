"""Static checks (section 30): no default image, compose stack, deploy script, systemd unit or CI job bakes or mounts a
live-release manifest, and the overlay that can is used by none of them. Nothing here builds an image.
"""
from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

import pytest

from shared_lib.core import live_build as LB

REPO = Path(__file__).resolve().parents[4]
DEPLOY = REPO / "deploy"
OVERLAY = DEPLOY / "containers" / "live-release.Dockerfile"
MANIFEST_DIR = str(Path(LB.LIVE_RELEASE_MANIFEST_PATH).parent.as_posix())       # /etc/cosmicforge/live-release
MARKERS = (MANIFEST_DIR, "live-release.Dockerfile", "live-release/manifest", "live_release_manifest")


def _default_artifacts():
    files = [p for p in (DEPLOY / "containers").glob("*Dockerfile*") if p != OVERLAY]
    files += sorted((DEPLOY / "staging").rglob("*.yml")) + sorted((DEPLOY / "staging").rglob("*.yaml"))
    files += sorted((DEPLOY / "staging").rglob("*.sh")) + sorted((DEPLOY / "staging").rglob("*.example"))
    files += sorted((DEPLOY / "systemd").iterdir()) + sorted(DEPLOY.glob("*.example"))
    files += sorted((REPO / ".github" / "workflows").glob("*.y*ml"))
    return [p for p in files if p.is_file()]


def test_the_default_images_compose_scripts_units_and_ci_carry_no_manifest():
    artifacts = _default_artifacts()
    assert any(p.name == "backend.Dockerfile" for p in artifacts)
    assert any(p.name == "docker-compose.staging.yml" for p in artifacts)
    assert any(p.name == "ci.yml" for p in artifacts)
    for path in artifacts:
        text = path.read_text(encoding="utf-8", errors="replace")
        for marker in MARKERS:
            assert marker not in text, f"{path.relative_to(REPO)} mentions {marker}"


def test_the_default_backend_image_copies_nothing_into_etc():
    text = (DEPLOY / "containers" / "backend.Dockerfile").read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.strip().upper().startswith(("COPY", "ADD")):
            assert "/etc" not in line, line


def test_the_overlay_is_the_only_place_a_manifest_enters_and_it_targets_the_checked_path():
    text = OVERLAY.read_text(encoding="utf-8")
    copies = [line for line in text.splitlines() if line.strip().upper().startswith(("COPY", "ADD"))]
    assert copies == [f"COPY live-release/manifest.json {LB.LIVE_RELEASE_MANIFEST_PATH}"]
    assert re.search(r"^FROM \$\{BASE_IMAGE\}$", text, re.M) and re.search(r"^USER cosmicforge$", text, re.M)


def test_no_manifest_is_tracked_and_the_allowlist_is_empty():
    try:
        tracked = subprocess.run(["git", "-C", str(REPO), "ls-files"], check=True, capture_output=True,
                                 text=True).stdout.splitlines()
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("git not available")
    assert not [p for p in tracked if "live-release/" in p and p.endswith(".json")]
    source = (REPO / "backends" / "shared" / "shared_lib" / "core" / "live_release_allowlist.py").read_text(encoding="utf-8")
    values = [n.value for n in ast.parse(source).body if isinstance(n, ast.AnnAssign)]
    assert [ast.unparse(v) for v in values] == ["frozenset()"]
    assert not re.search(r"[0-9a-f]{64}", source)
