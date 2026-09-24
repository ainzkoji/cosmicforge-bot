"""Tests that assert on files which are deliberately NOT in the repository.

* DEPLOYMENT_CONFIG -- ``backends/bot-backend/.env`` is the operator's private
  deployment configuration (gitignored; it holds credentials). Tests that
  assert what the ACTIVE deployment is set to (paper / ML off / IOFS shadow)
  run wherever that file exists and skip, with this reason, in a fresh clone.
* ARTIFACT_REQUIRED -- ``models/{artifacts,experiments,reports}/`` are
  gitignored, locally generated artifacts.

A skip is never a pass: each one names the missing file, so a report can list
exactly which checks did not run and why.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pytest

BOT_ROOT = Path(__file__).resolve().parents[1]
ACTIVE_ENV = BOT_ROOT / ".env"


def _rel(path: Path) -> str:
    try:
        return path.resolve().relative_to(BOT_ROOT).as_posix()
    except ValueError:
        return str(path)


def active_env_or_skip() -> Path:
    if not ACTIVE_ENV.exists():
        pytest.skip("DEPLOYMENT_CONFIG: the private operator .env is absent (gitignored; never "
                    "required for unit tests) -- this asserts the ACTIVE deployment's settings")
    return ACTIVE_ENV


def artifact_or_skip(*paths: Path) -> None:
    missing = [p for p in paths if not p.exists()]
    if missing:
        pytest.skip("ARTIFACT_REQUIRED: untracked local artifact(s) missing: "
                    + ", ".join(_rel(p) for p in missing))


def read_bytes_or_none(path: Path) -> Optional[bytes]:
    """For "must not modify .env" checks: an absent file must stay absent."""
    return path.read_bytes() if path.exists() else None
