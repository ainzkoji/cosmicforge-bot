"""Characterization of the ownership / CATI-bot / capability / policy prelude of the two production
``boundary_for`` factories (Section I Batch 8).

``production_execution.boundary_for`` (residual production family, ``CATIExecutionBoundary``) and
``portfolio_runtime.boundary_for`` (broker portfolio cycle, ``PortfolioExecutionBoundary``) open with similar
checks. Before any extraction of a shared prelude these tests pin what each one does TODAY -- including where they
differ -- so an extraction that changes a gate fails here. Nothing below builds a boundary: every case stops in
the prelude (the capability gate and the policy resolver are replaced by recorders where a case must pass them).
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.trading_intelligence.integration import portfolio_runtime as PR
from app.trading_intelligence.integration import production_execution as PE
from app.trading_intelligence.integration.residual_prospective import FAMILY

USER, ACCOUNT, BOT = "user_p", "acct_p", "bot_p"


class _Stop(Exception):
    """Raised by the policy recorder: the prelude passed every check before policy resolution."""


@pytest.fixture
def seen(monkeypatch):
    calls = {"capability": [], "policy": [], "instance": []}
    state = {"instance": None, "capability_error": None, "policy": None}

    def get_bot_instance(self, bot_id):
        calls["instance"].append(bot_id)
        return state["instance"]

    def capability(db, *, user_id, broker_account_id):
        calls["capability"].append((user_id, broker_account_id))
        if state["capability_error"] is not None:
            raise state["capability_error"]

    def policy(*, instance, broker_environment, risk_params):
        calls["policy"].append(broker_environment)
        if state["policy"] is None:
            raise _Stop()
        return state["policy"]

    monkeypatch.setattr("app.core.bot_instance_service.BotInstanceService.get_bot_instance", get_bot_instance)
    monkeypatch.setattr("app.core.broker_capability_gate.assert_broker_execution_capability", capability)
    monkeypatch.setattr("app.policy.effective_policy.resolve_effective_bot_policy", policy)
    return calls, state


def _instance(**over):
    base = dict(id=BOT, user_id=USER, broker_account_id=ACCOUNT, strategy_id="cati", allocation_type="fixed_amount",
                risk_level="balanced")
    base.update(over)
    return SimpleNamespace(**base)


def _account(environment="demo"):
    return {"id": ACCOUNT, "user_id": USER, "broker_id": "binance", "environment": environment}


def _db():
    return SimpleNamespace(path=":memory:")


def _production(account, bot=None, client_environment="demo"):
    return PE.boundary_for(_db(), account, bot or {"id": BOT, "risk_level": "balanced"},
                           SimpleNamespace(broker_environment=client_environment))


def _portfolio(account, bot=None):
    return PR.boundary_for(_db(), account, SimpleNamespace(), bot or {"id": BOT, "risk_level": "balanced"},
                           lambda: 0)


FACTORIES = {"production_execution": _production, "portfolio_runtime": _portfolio}


@pytest.mark.parametrize("factory", FACTORIES)
@pytest.mark.parametrize("instance", [None, _instance(user_id="someone_else"),
                                      _instance(broker_account_id="another_account")],
                         ids=["missing", "other_user", "other_account"])
def test_a_bot_of_another_owner_or_account_is_refused_before_any_gate(seen, factory, instance):
    calls, state = seen
    state["instance"] = instance
    with pytest.raises(ValueError, match="^BROKER_ACCOUNT_OWNERSHIP_MISMATCH$"):
        FACTORIES[factory](_account())
    assert calls["capability"] == [] and calls["policy"] == []


@pytest.mark.parametrize("factory", FACTORIES)
@pytest.mark.parametrize("strategy", ["master_ensemble", "sma_cross", "", None])
def test_a_non_cati_bot_is_refused_before_the_capability_gate(seen, factory, strategy):
    calls, state = seen
    state["instance"] = _instance(strategy_id=strategy)
    with pytest.raises(ValueError, match="^PRODUCTION_REQUIRES_CATI_BOT$"):
        FACTORIES[factory](_account())
    assert calls["capability"] == []


def test_the_family_strategy_id_is_accepted_only_by_the_production_family_path(seen):
    """A real difference between the two preludes: the residual production path also accepts a bot whose
    strategy id is the frozen family's own id; the portfolio path accepts only ``cati``."""
    calls, state = seen
    state["instance"] = _instance(strategy_id=FAMILY)
    with pytest.raises(_Stop):
        _production(_account())
    assert calls["capability"] == [(USER, ACCOUNT)]
    with pytest.raises(ValueError, match="^PRODUCTION_REQUIRES_CATI_BOT$"):
        _portfolio(_account())
    assert calls["capability"] == [(USER, ACCOUNT)]           # the portfolio path never reached the gate


@pytest.mark.parametrize("factory", FACTORIES)
def test_the_capability_gate_runs_for_the_account_and_its_refusal_propagates(seen, factory):
    from app.core.broker_capability_gate import BrokerCapabilityGateError

    calls, state = seen
    state["instance"] = _instance()
    state["capability_error"] = BrokerCapabilityGateError("BROKER_EXECUTION_CAPABILITY_INCOMPLETE", "no trade scope")
    with pytest.raises(BrokerCapabilityGateError):
        FACTORIES[factory](_account())
    assert calls["capability"] == [(USER, ACCOUNT)] and calls["policy"] == []


@pytest.mark.parametrize("factory", FACTORIES)
def test_a_paper_policy_is_refused(seen, factory):
    calls, state = seen
    state["instance"] = _instance()
    state["policy"] = SimpleNamespace(execution_mode="paper")
    with pytest.raises(ValueError, match="^PRODUCTION_REJECTS_PAPER_BOT$"):
        FACTORIES[factory](_account())
    assert calls["capability"] == [(USER, ACCOUNT)] and len(calls["policy"]) == 1


def test_the_environment_given_to_the_policy_resolver_differs_between_the_paths(seen):
    """Second real difference: the production path normalizes the account environment (``testnet`` -> ``demo``)
    and first refuses a client of another environment; the portfolio path passes the stored value lower-cased."""
    calls, state = seen
    state["instance"] = _instance()
    with pytest.raises(_Stop):
        _production(_account("TESTNET"), client_environment="demo")
    with pytest.raises(_Stop):
        _portfolio(_account("TESTNET"))
    assert calls["policy"] == ["demo", "testnet"]


def test_the_production_path_refuses_a_client_of_another_environment_before_reading_the_bot(seen):
    calls, state = seen
    state["instance"] = _instance()
    with pytest.raises(ValueError, match="^BROKER_ENVIRONMENT_MISMATCH$"):
        _production(_account("demo"), client_environment="live")
    assert calls["instance"] == [] and calls["capability"] == []
