"""Repository-wide invariants that keep the old threshold architecture dead.

These tests do not exercise behaviour. They scan production source and fail if
a deleted threshold authority reappears anywhere it could execute.

That is the point. The previous stack did not break loudly -- it was *added to*,
one component at a time, until five of them resolved a threshold in series and
the last one won. Every check here exists so that reintroducing any of those
five fails CI on the commit that does it, rather than months later in a forensic
audit.

Historical documentation, this file's own assertions, and the removal record in
``app.threshold.migration`` are excluded explicitly, by path.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

# Production trees. Tests, docs, scripts and reports are deliberately excluded:
# a string in a report is a record, not an authority.
_BOT_BACKEND = Path(__file__).resolve().parents[1]
_APP = _BOT_BACKEND / "app"
_SHARED = _BOT_BACKEND.parent / "shared" / "shared_lib"

#: Files allowed to mention any deleted name, and why.
_ALLOWED_FILES = {
    # The permanent record of what was deleted and what happened to it.
    _APP / "threshold" / "migration.py",
    # The configuration rejection list -- it must name the keys to reject them.
    _APP / "core" / "config.py",
}

#: Narrow, per-identifier exemptions. Deliberately not file-wide: the recorder
#: is allowed to write NULL into one legacy column, and is still forbidden from
#: mentioning any other deleted control.
_ALLOWED_IDENTIFIERS: dict[str, set[Path]] = {
    # LEGACY_READ_ONLY_EVIDENCE_COLUMN. The column preserves what the old stack
    # recorded and cannot be dropped without destroying historical evidence.
    # Nothing computes a consensus requirement; the recorder writes NULL.
    "consensus_required": {
        _APP / "evidence" / "decision_recorder.py",
        _SHARED / "persistence" / "evidence_schema.py",
    },
}

#: Identifiers that must not appear in executable production code.
FORBIDDEN_IDENTIFIERS = (
    "MIN_CONFIDENCE_THRESHOLD",
    "ENSEMBLE_MIN_THRESHOLD_FLOOR",
    "DYNAMIC_THRESHOLD_MIN",
    "DYNAMIC_THRESHOLD_MAX",
    "DYNAMIC_THRESHOLD_FALLBACK",
    "DYNAMIC_THRESHOLD_MIN_SAMPLES",
    "DYNAMIC_THRESHOLD_ENABLED",
    "DYNAMIC_THRESHOLD_WINDOW_SIZE",
    "DYNAMIC_THRESHOLD_PERCENTILE",
    "consensus_threshold",
    "consensus_required",
    "confidence_absolute_floor",
    "min_confidence_gate",
    "min_confidence_hard",
    "min_confidence_soft",
    "min_strategy_confidence",
    "DynamicThresholdCalculator",
    "get_dynamic_threshold_calculator",
    "log_threshold_event",
)


def _production_files() -> list[Path]:
    files: list[Path] = []
    for root in (_APP, _SHARED):
        if not root.is_dir():
            continue
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            if path in _ALLOWED_FILES:
                continue
            files.append(path)
    return sorted(files)


def _code_only(path: Path) -> str:
    """Source with docstrings removed, so prose cannot trip or satisfy a check.

    Comments are already absent from an unparsed AST. Without this, deleting a
    control but explaining the deletion in a comment would fail the very test
    that is supposed to confirm the deletion.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except SyntaxError:  # pragma: no cover - a syntax error fails elsewhere
        return ""
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", None)
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                body.pop(0)
    return ast.unparse(tree)


@pytest.fixture(scope="module")
def production_sources() -> dict[Path, str]:
    return {path: _code_only(path) for path in _production_files()}


# -- The deleted names must not execute anywhere ------------------------------


@pytest.mark.parametrize("identifier", FORBIDDEN_IDENTIFIERS)
def test_no_production_reference_to_a_deleted_threshold_control(
    identifier, production_sources
):
    exempt = _ALLOWED_IDENTIFIERS.get(identifier, set())
    offenders = [
        str(path.relative_to(_BOT_BACKEND.parent))
        for path, source in production_sources.items()
        if identifier in source and path not in exempt
    ]
    assert not offenders, (
        f"{identifier} was deleted with the old threshold architecture but is "
        f"still referenced in executable production code: {offenders}"
    )


def test_the_legacy_evidence_column_is_never_written_with_a_value():
    """The one exemption above must stay an exemption, not a loophole.

    ``consensus_required`` survives as a column so historical rows keep what the
    old stack recorded. The new engine writes NULL into it, and this asserts
    that it cannot start writing a number again.
    """
    from app.evidence.decision_recorder import TradingDecision

    row = TradingDecision(
        decision_id="d1", bot_instance_id="b", symbol="BTCUSDT", evaluated_at="t"
    ).to_row()
    assert "consensus_required" in row
    assert row["consensus_required"] is None


def test_the_old_calculator_module_no_longer_exists():
    assert not (_APP / "risk" / "dynamic_threshold.py").exists()


def test_nothing_imports_the_old_calculator(production_sources):
    offenders = [
        str(path)
        for path, source in production_sources.items()
        if "app.risk.dynamic_threshold" in source
    ]
    assert not offenders, offenders


# -- Exactly one component may produce a final entry threshold ----------------

#: The only module permitted to produce a final entry threshold.
THRESHOLD_AUTHORITY = _APP / "threshold" / "engine.py"

#: Assignments whose name would make the target a threshold authority.
_AUTHORITY_TARGETS = ("final_threshold", "effective_entry_threshold")


#: Calls that merely convert or re-read a threshold someone else decided.
_PASSTHROUGH_CALLS = {"float", "getattr"}


def _is_passthrough_call(call: ast.Call) -> bool:
    return isinstance(call.func, ast.Name) and call.func.id in _PASSTHROUGH_CALLS


def _assigns_a_final_threshold(path: Path) -> bool:
    """True when this module *computes* a final threshold, rather than reading one.

    Passing a threshold through -- ``final_threshold=decision.final_threshold``
    -- is consumption. Computing one from parts, or clamping/flooring an
    existing one, is authority.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except SyntaxError:  # pragma: no cover
        return False

    for node in ast.walk(tree):
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = [node.target]
        elif isinstance(node, ast.keyword) and node.arg in _AUTHORITY_TARGETS:
            value = node.value
            # A computed expression here is a threshold being decided. A bare
            # name or an attribute read is the decision being passed along,
            # which every consumer is allowed to do.
            if isinstance(value, (ast.BinOp, ast.IfExp)):
                return True
            if isinstance(value, ast.Call) and not _is_passthrough_call(value):
                return True
            continue
        else:
            continue

        for target in targets:
            name = (
                target.id
                if isinstance(target, ast.Name)
                else target.attr
                if isinstance(target, ast.Attribute)
                else None
            )
            if name not in _AUTHORITY_TARGETS:
                continue
            value = getattr(node, "value", None)
            # An arithmetic expression, a conditional, or a max()/min() call is
            # a threshold being *decided* here.
            if isinstance(value, (ast.BinOp, ast.IfExp)):
                return True
            if (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id in {"max", "min", "_clamp"}
            ):
                return True
    return False


def test_exactly_one_module_can_produce_a_final_entry_threshold():
    """The architectural invariant, enforced against source rather than trusted.

    A second authority is how ``max(dynamic <= 0.65, 0.70)`` came to exist: no
    single commit introduced a bug, each one merely added another opinion.
    """
    authorities = [p for p in _production_files() if _assigns_a_final_threshold(p)]
    authorities.append(THRESHOLD_AUTHORITY) if _assigns_a_final_threshold(
        THRESHOLD_AUTHORITY
    ) else None

    unexpected = [
        str(p.relative_to(_BOT_BACKEND.parent)) for p in authorities if p != THRESHOLD_AUTHORITY
    ]
    assert not unexpected, (
        "only AdaptiveEntryThresholdEngine may produce a final entry threshold; "
        f"also computed in: {unexpected}"
    )


def test_the_authority_actually_is_the_authority():
    """Guards the test above from passing because nothing computes anything."""
    assert _assigns_a_final_threshold(THRESHOLD_AUTHORITY), (
        "app/threshold/engine.py no longer computes a final threshold, so the "
        "single-authority test above would pass vacuously"
    )


# -- Downstream consumers must not modify the decision ------------------------


def test_the_decision_engine_only_reads_the_threshold():
    from app.decision import decision_engine

    source = _code_only(Path(decision_engine.__file__))
    for forbidden in ("max(", "min(", "_clamp"):
        assert forbidden not in source, (
            f"TradingDecisionEngine contains {forbidden!r}; it compares against "
            "the engine's threshold and must not clamp, floor or adjust it"
        )


def test_the_threshold_decision_is_immutable():
    """Downstream code cannot override final_threshold even if it tries."""
    from app.threshold.contracts import AdaptiveThresholdDecision, ThresholdMode, ThresholdStatus

    decision = AdaptiveThresholdDecision(
        threshold_decision_id="t1",
        bot_instance_id="b",
        symbol="BTCUSDT",
        timeframe="15m",
        status=ThresholdStatus.EVALUATED,
        threshold_engine_version="1.0.0",
        threshold_mode=ThresholdMode.ADAPTIVE,
        base_threshold=0.6,
        raw_unclamped_threshold=0.6,
        final_threshold=0.6,
        min_threshold=0.5,
        max_threshold=0.9,
    )
    with pytest.raises(Exception):
        decision.final_threshold = 0.1  # type: ignore[misc]


# -- Configuration surface ----------------------------------------------------


def test_no_env_file_carries_a_deleted_threshold_key():
    from app.core.config import detect_legacy_threshold_keys

    for name in (".env", ".env.example", ".env.data_collection"):
        path = _BOT_BACKEND / name
        if not path.is_file():
            continue
        found = detect_legacy_threshold_keys(environ={}, env_file=str(path))
        assert not found, f"{name} still carries deleted threshold keys: {found}"


def test_the_only_threshold_namespace_is_threshold():
    """Every threshold setting an operator can see lives under THRESHOLD_*."""
    from app.core.config import Settings
    from app.threshold.policy import SETTING_MAP

    for name in SETTING_MAP:
        assert name.startswith("THRESHOLD_"), name
        assert name in Settings.model_fields, f"{name} is mapped but not a setting"
