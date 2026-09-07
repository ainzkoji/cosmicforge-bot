"""Auto Pilot runner initialization must be atomic and its context contract enforced.

Why this file exists
--------------------
``PaperRunner._load_orchestrator`` read ``self.context.max_daily_loss``.
``BotRunContext`` has no such field -- it exposes ``daily_max_loss_usdt``.
The resulting ``AttributeError`` was swallowed by a broad
``except Exception: print(...)``, so:

  * ``self.orchestrator`` stayed ``None``,
  * the runner was still cached and scheduled by ``MultiBotRunner``,
  * every symbol on every 10-second tick emitted ERROR_STRATEGY_UNAVAILABLE,
  * and nothing durable recorded the real cause.

The existing orchestrator tests all built ``TradingOrchestrator.__new__(...)``
directly, so no test ever exercised the runner->context attribute contract.
These tests close both gaps.
"""
from __future__ import annotations

import inspect

import pytest

from app.core.bot_instance_service import BotInstanceService
from app.models.bot_instance_models import BotInstance
from app.runner.bot_context import BotRunContext
from app.runner.effective_policy import resolve_effective_bot_policy
from app.runner.errors import RunnerInitializationError
from app.runner.runner import PaperRunner


def _instance(**overrides) -> BotInstance:
    defaults = dict(
        id="bot-init-1",
        user_id="user-1",
        broker_account_id="acct-1",
        market_type="CRYPTO",
        strategy_id="master_ensemble",
        strategy_version="1.0.0",
        risk_level="balanced",
        symbols=["BTCUSDT", "ETHUSDT"],
        timeframes=["15m"],
        allocation_type="fixed_amount",
        allocation_value=120.0,
        mode="paper",
        capital_allocation=806.54580081,
        capital_allocation_type="fixed_amount",
    )
    defaults.update(overrides)
    return BotInstance(**defaults)


def _context() -> BotRunContext:
    instance = _instance()
    policy = resolve_effective_bot_policy(
        instance=instance,
        broker_environment="demo",
        risk_params=BotInstanceService.get_risk_profile_preset(instance.risk_level),
    )
    return BotRunContext.from_effective_policy(policy, {"api_key": "k", "api_secret": "s"})


# ── The exact defect: context attribute contract ─────────────────────────────


def test_load_orchestrator_only_reads_attributes_bot_run_context_defines():
    """Static guard: every ``self.context.<attr>`` read in the runner must exist.

    This is what actually failed in production. It is asserted statically so the
    regression is caught without a broker, a DB, or a live strategy.
    """
    import re

    source = inspect.getsource(PaperRunner)
    referenced = set(re.findall(r"self\.context\.([a-zA-Z_][a-zA-Z0-9_]*)", source))
    available = set(dir(BotRunContext)) | set(getattr(BotRunContext, "__annotations__", {}))
    missing = sorted(referenced - available)
    assert not missing, (
        "PaperRunner reads BotRunContext attributes that do not exist: "
        f"{missing}. This silently nulls the orchestrator and produces "
        "ERROR_STRATEGY_UNAVAILABLE on every cycle."
    )


def test_bot_run_context_exposes_daily_loss_under_its_real_name():
    ctx = _context()
    assert hasattr(ctx, "daily_max_loss_usdt")
    assert not hasattr(ctx, "max_daily_loss"), (
        "If this field is ever added, update _load_orchestrator deliberately "
        "rather than relying on a fallback."
    )


# ── Atomic initialization ────────────────────────────────────────────────────


def test_orchestrator_failure_raises_instead_of_leaving_runner_half_built(monkeypatch):
    """A context-bound runner must never survive an orchestrator failure."""
    runner = PaperRunner.__new__(PaperRunner)
    runner.context = _context()
    runner.effective_policy = runner.context.effective_policy
    runner.strategy = object()

    def _boom(*_a, **_kw):
        raise AttributeError("simulated orchestrator construction failure")

    monkeypatch.setattr("app.runner.runner.TradingOrchestrator", _boom)

    with pytest.raises(RunnerInitializationError) as excinfo:
        runner._load_orchestrator()

    err = excinfo.value
    assert err.reason_code == RunnerInitializationError.ORCHESTRATOR_INITIALIZATION_FAILED
    assert err.cause_type == "AttributeError"
    assert runner.orchestrator is None
    # The cause is preserved for persistence, without leaking configuration.
    detail = err.structured_detail()
    assert detail["reason_code"] == err.reason_code
    assert "simulated orchestrator construction failure" in detail["cause_message"]


def test_assert_initialized_rejects_missing_collaborator():
    runner = PaperRunner.__new__(PaperRunner)
    runner.context = _context()
    runner.effective_policy = runner.context.effective_policy
    runner.strategy = object()
    runner.orchestrator = object()
    runner.executor = object()
    runner.position_manager = None  # the missing piece

    with pytest.raises(RunnerInitializationError) as excinfo:
        runner._assert_initialized()

    assert excinfo.value.reason_code == RunnerInitializationError.POSITION_REHYDRATION_FAILED
    assert runner.initialization_status == "FAILED_INITIALIZATION"


def test_assert_initialized_marks_ready_when_every_collaborator_exists():
    runner = PaperRunner.__new__(PaperRunner)
    runner.context = _context()
    runner.effective_policy = runner.context.effective_policy
    runner.strategy = object()
    runner.orchestrator = object()
    runner.executor = object()
    runner.position_manager = object()

    runner._assert_initialized()

    assert runner.initialization_status == "READY"
    report = runner.initialization_report()
    assert report["status"] == "READY"
    assert all(report["components"].values())


def test_contextless_legacy_runner_is_exempt_and_never_claims_ready():
    """The global diagnostic runner has no context; it must not raise, and must
    not be mistaken for an operational Auto Pilot runner."""
    runner = PaperRunner.__new__(PaperRunner)
    runner.context = None
    runner._assert_initialized()
    assert runner.initialization_status == "LEGACY_NO_CONTEXT"


# ── Atomic policy delivery ───────────────────────────────────────────────────


def test_constructor_rejects_context_without_resolved_policy():
    ctx = _context()
    ctx.effective_policy = None
    with pytest.raises(RunnerInitializationError) as excinfo:
        PaperRunner(client=object(), context=ctx)
    assert excinfo.value.reason_code == RunnerInitializationError.EFFECTIVE_POLICY_INVALID


def test_constructor_accepts_policy_atomically():
    """The policy must be attached before collaborators are built, not patched on
    afterwards by the caller."""
    signature = inspect.signature(PaperRunner.__init__)
    assert "effective_policy" in signature.parameters
