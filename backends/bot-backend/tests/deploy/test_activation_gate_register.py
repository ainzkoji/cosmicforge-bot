"""Section 29: the activation-gate register and its machine-readable twin stay consistent and point at real tests."""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
DOCS = REPO / "docs" / "implementation"
TEST_REF = re.compile(r"((?:backends|tests)/[\w./-]+\.py)?::([A-Za-z_][\w]*)")


def register():
    return json.loads((DOCS / "activation_gates.json").read_text(encoding="utf-8"))


def test_the_json_counts_types_and_ids_are_consistent():
    doc = register()
    gates = doc["gates"]
    assert set(doc["types"]) == {"A", "B", "C", "D", "E"}
    assert {t: n for t, n in doc["counts"].items()} == {t: Counter(g["type"] for g in gates).get(t, 0) for t in doc["types"]}
    assert len({g["id"] for g in gates}) == len(gates)
    required = {"id", "gate", "type", "location", "satisfied_by", "owner", "activation", "revocation", "refusal_test"}
    assert all(required <= set(g) and all(str(g[k]).strip() for k in required) for g in gates)
    # nothing was converted from Type A, and nothing is Type A
    assert doc["converted_from_type_a"] == [] and doc["counts"]["A"] == 0


def test_the_markdown_lists_every_gate_of_the_json():
    text = (DOCS / "ACTIVATION_GATE_REGISTER.md").read_text(encoding="utf-8")
    for g in register()["gates"]:
        assert f"| {g['id']} |" in text, g["id"]


def test_every_named_refusal_test_exists():
    missing = []
    for g in register()["gates"]:
        current = None
        for path, name in TEST_REF.findall(g["refusal_test"]):
            current = path or current
            source = (REPO / current).read_text(encoding="utf-8") if current and (REPO / current).is_file() else ""
            if not re.search(rf"^\s*(async )?def {re.escape(name)}\(", source, re.M):
                missing.append(f"{g['id']}: {current}::{name}")
        for path in re.findall(r"((?:backends|tests)/[\w./-]+\.py)", g["refusal_test"]):
            if not (REPO / path).is_file():
                missing.append(f"{g['id']}: {path}")
    assert not missing, missing


def test_the_live_build_gate_names_the_two_keys():
    [build] = [g for g in register()["gates"] if g["id"] == "GATE-BUILD-LIVE"]
    assert build["type"] == "E"
    for part in ("live_release_allowlist.py", "live_build.py", "live-release.Dockerfile", "live_release_manifest.py"):
        assert part in build["location"], part
