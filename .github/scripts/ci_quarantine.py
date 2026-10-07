"""pytest plugin: run known pre-existing failures as non-strict xfail.

Loaded in CI with ``-p ci_quarantine``. The list lives in
``.github/known_test_failures.txt`` (junit ``classname::name`` per line).
A listed test that fails is reported as xfailed; one that now passes is
reported as xpassed (remove its line). Anything not listed fails CI as usual.
"""
from __future__ import annotations

from pathlib import Path

import pytest

_LIST = Path(__file__).resolve().parents[1] / "known_test_failures.txt"


def _known() -> set[str]:
    try:
        lines = _LIST.read_text(encoding="utf-8").splitlines()
    except OSError:
        return set()
    return {ln.strip() for ln in lines if ln.strip() and not ln.startswith("#")}


def _junit_id(nodeid: str) -> str:
    parts = nodeid.split("::")
    parts[0] = parts[0][:-3].replace("/", ".") if parts[0].endswith(".py") else parts[0]
    return ".".join(parts[:-1]) + "::" + parts[-1]


def pytest_collection_modifyitems(config, items):
    known = _known()
    if not known:
        return
    for item in items:
        if _junit_id(item.nodeid) in known:
            item.add_marker(pytest.mark.xfail(reason="known pre-existing failure (see .github/known_test_failures.txt)", strict=False))
