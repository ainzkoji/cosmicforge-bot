"""A function-local ``from X import name`` must never shadow an earlier use.

Python binds every name assigned anywhere in a function -- including by an
``import`` statement -- as a local for the WHOLE function body.  So this:

    def f(self):
        get_trace_recorder().record_gate(...)          # line 3555
        ...
        from shared_lib... import get_trace_recorder   # line 3993

raises ``UnboundLocalError`` at line 3555 even though the module also imports
``get_trace_recorder`` at module level.

That is exactly what happened in ``_step_symbol_orchestrated``.  The failing
call sat inside ``try: ... except Exception: pass``, so every same-candle
heartbeat silently lost its gate reason and persisted a bare ``SKIP`` decision
instead of ``NO_NEW_CANDLE`` -- 8,640 unattributable rows per symbol per day in
``decision_traces`` and ``canonical_trade_decisions``.

The other local imports in that function were already aliased (``_gtr``,
``_gtr_mkt``, ...); one was missed.  This test makes the whole class impossible.
"""
from __future__ import annotations

import ast
import io
from pathlib import Path

import pytest

_RUNNER_MODULES = [
    Path(__file__).resolve().parents[1] / "app" / "runner" / "runner.py",
    Path(__file__).resolve().parents[1] / "app" / "runner" / "multi_runner.py",
]


def _hazards(path: Path) -> list[str]:
    tree = ast.parse(io.open(path, encoding="utf-8").read())
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        first_import: dict[str, int] = {}
        for child in ast.walk(node):
            if isinstance(child, (ast.Import, ast.ImportFrom)):
                for alias in child.names:
                    name = (alias.asname or alias.name).split(".")[0]
                    first_import[name] = min(first_import.get(name, child.lineno), child.lineno)
        for name, import_line in first_import.items():
            uses = [
                u.lineno for u in ast.walk(node)
                if isinstance(u, ast.Name) and isinstance(u.ctx, ast.Load) and u.id == name
            ]
            if uses and min(uses) < import_line:
                found.append(
                    f"{path.name}::{node.name} uses '{name}' at line {min(uses)} "
                    f"but only imports it locally at line {import_line} "
                    f"(UnboundLocalError at runtime)"
                )
    return found


@pytest.mark.parametrize("module_path", _RUNNER_MODULES, ids=lambda p: p.name)
def test_no_function_local_import_shadows_an_earlier_use(module_path: Path):
    hazards = _hazards(module_path)
    assert not hazards, "\n".join(hazards)
