"""E-03: the user-capital readiness gate and the operations alerts read the DURABLE CATI sizing refusals.

Since ``6e0b42c`` nothing writes the legacy ``SIZING_FAILURE`` audit event; a sizing refusal is a RiskDecision
(REJECTED, family SIZING) appended to ``cati_risk_decisions`` by the execution boundary. These tests produce genuine
refusals through the REAL path -- a real Section 18 TradePlan, the real ``CATIExecutionBoundary``, the real
``TradingOrchestrator`` hard-risk sizing and the real ``BinanceExecutor`` sizing gate over a MagicMock exchange client
(no network, no order) -- and check that:

* a refusal blocks readiness (``SIZING_FAILURES_DETECTED``) and is attributed to its own bot, account and family;
* a re-appended decision is counted once; a new process (new ``DB`` / store objects over the same file) sees the same;
* another account's refusals, other rejection families and approvals are never counted for this bot;
* the readiness window starts at the paper period and has no upper bound;
* a database without the evidence table fails closed (``SIZING_EVIDENCE_UNAVAILABLE``);
* repeated refusals raise the ``REPEATED_SIZING_REFUSALS`` alert for that account only, and it resolves.

Nothing here changes a risk ceiling, a trading flag or research state, and nothing certifies anything.
"""
from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone

import pytest
from _exec import Harness

from app.product_safety.readiness_gate import RejectionReason, evaluate_user_capital_readiness
from app.trading_intelligence.contracts.execution import RiskRejectionFamily as F
from app.trading_intelligence.evidence.stores import RiskDecisionStore
from app.trading_intelligence.execution.boundary import BoundaryStatus as B
from app.trading_intelligence.execution.risk_evidence import build_risk_decision

HOUR, DAY = 3_600_000, 86_400_000


@pytest.fixture(autouse=True)
def _sections_confirmed(monkeypatch):
    # the readiness verdict itself is not under test; only the sizing evidence feeding it
    monkeypatch.setattr("app.core.config.settings.SECTIONS_A_TO_E_CONFIRMED", True, raising=False)


def _executor_sizing_refusal(tmp_path):
    """The executor's own sizing authority refuses at submission (stage EXECUTOR_GATES)."""
    h = Harness(tmp_path, fixed=0.5)
    h.seed_catalog(instruments=[h.instrument("BTCUSDT", min_notional=None), h.instrument("ETHUSDT", min_notional=None)])
    res = h.run(h.boundary(preflight=h.preflight(seed=False)))
    assert res.status == B.EXECUTION_REJECTED and res.risk_decision.rejection_family == F.SIZING.value
    return h, res.risk_decision


def _report(db, bot):
    return evaluate_user_capital_readiness(db=db, bot_instance_id=bot)


def _record(db, plan, *, family="SIZING", approved=False, at, codes=("SIZE_ZERO",), **plan_changes):
    """A decision written by the REAL recorder the boundary calls (``RiskDecisionStore.append``)."""
    p = dataclasses.replace(plan, **plan_changes) if plan_changes else plan
    risk = build_risk_decision(p, approved=approved, stage="PRE_EXECUTION", reason_codes=codes, decision_time=at,
                               rejection_family=None if approved else family)
    assert RiskDecisionStore(db).append(risk) is True
    return risk


# ════════════════════════════════════════════════════════════════════════════════════ the readiness gate
def test_a_real_executor_sizing_refusal_blocks_readiness_and_is_attributed(tmp_path):
    h, risk = _executor_sizing_refusal(tmp_path)
    r = _report(h.db, h.plan.bot_instance_id)
    assert RejectionReason.SIZING_FAILURES_DETECTED.value in r.blocking_reasons
    assert RejectionReason.SIZING_EVIDENCE_UNAVAILABLE.value not in r.blocking_reasons
    assert (r.sizing_failure_count, r.sizing_refusal_count_durable, r.sizing_failure_legacy_event_count) == (1, 1, 0)
    assert r.sizing_refusal_ids == [risk.risk_decision_id]
    assert r.sizing_refusals_by_account == {h.plan.broker_account_id: 1}
    assert r.sizing_refusals_by_stage == {"EXECUTOR_GATES": 1}
    assert r.last_sizing_refusal_at_ms == h.now
    assert r.to_dict()["sizing_refusal_ids"] == [risk.risk_decision_id]
    # the strategy family comes from the plan's own record in cati_trade_plans, where it was recorded
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    TradePlanEvidenceStore(h.db).append(h.plan)                  # idempotent when the pipeline already recorded it
    assert _report(h.db, h.plan.bot_instance_id).sizing_refusals_by_family == {h.plan.setup_family: 1}


def test_a_real_pre_execution_sizing_refusal_from_the_orchestrator_is_counted(tmp_path):
    h = Harness(tmp_path)
    assert h.plan.setup_family != "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"   # the structural-stop path
    h.orch.system_limits.max_stop_loss_pct = 1e-6     # the plan's structural stop now exceeds the system maximum
    res = h.run()
    assert res.status == B.RISK_REJECTED and res.risk_decision.rejection_family == F.SIZING.value
    assert res.reason_codes == ("CATI_STRUCTURAL_STOP_EXCEEDS_SYSTEM_MAX",)
    r = _report(h.db, h.plan.bot_instance_id)
    assert RejectionReason.SIZING_FAILURES_DETECTED.value in r.blocking_reasons
    assert r.sizing_refusal_ids == [res.risk_decision.risk_decision_id]
    assert r.sizing_refusals_by_stage == {"PRE_EXECUTION": 1}
    assert r.sizing_refusal_reasons == {"CATI_STRUCTURAL_STOP_EXCEEDS_SYSTEM_MAX": 1}


def test_a_duplicate_is_counted_once_and_a_restart_sees_the_same_refusals(tmp_path):
    h, risk = _executor_sizing_refusal(tmp_path)
    assert RiskDecisionStore(h.db).append(risk) is False            # the same decision again: idempotent no-op
    first = _report(h.db, h.plan.bot_instance_id)
    assert first.sizing_failure_count == 1
    # "restart": fresh process objects over the same database file, nothing carried in memory
    from shared_lib.persistence.db import DB
    reopened = DB(path=h.db.path)
    assert RiskDecisionStore(reopened).append(risk) is False
    again = _report(reopened, h.plan.bot_instance_id)
    assert (again.sizing_failure_count, again.sizing_refusal_ids) == (1, [risk.risk_decision_id])
    assert RejectionReason.SIZING_FAILURES_DETECTED.value in again.blocking_reasons


def test_other_accounts_other_families_and_approvals_are_not_attributed(tmp_path):
    h, risk = _executor_sizing_refusal(tmp_path)
    bot, account = h.plan.bot_instance_id, h.plan.broker_account_id
    other = _record(h.db, h.plan, at=h.now + 1, bot_instance_id="botC", broker_account_id="acct2")
    _record(h.db, h.plan, at=h.now + 2, family="MARGIN", codes=("INSUFFICIENT_MARGIN",))   # this bot, not sizing
    _record(h.db, h.plan, at=h.now + 3, approved=True, codes=("APPROVED_FOR_EXECUTION",))  # this bot, approved
    mine = _report(h.db, bot)
    assert mine.sizing_refusal_ids == [risk.risk_decision_id] and mine.sizing_refusals_by_account == {account: 1}
    theirs = _report(h.db, "botC")
    assert theirs.sizing_refusal_ids == [other.risk_decision_id] and theirs.sizing_refusals_by_account == {"acct2": 1}
    # another account's plan id never lends this tenant's recorded family
    assert set(theirs.sizing_refusals_by_family) == {"UNRECORDED"}
    clean = _report(h.db, "botB")                       # same account as ours, other bot, no refusal
    assert clean.sizing_failure_count == 0 and RejectionReason.SIZING_FAILURES_DETECTED.value not in clean.blocking_reasons


def _close_fill(db, bot, at: datetime):
    with db.connect() as c:
        c.execute("INSERT INTO trade_fills (run_id, cycle_id, trace_id, symbol, side, action, qty, price, fee, "
                  "realized_pnl, total_fees, net_pnl, timestamp_utc, bot_instance_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  ("run-e03", "c1", None, "BTCUSDT", "LONG", "CLOSE", 1.0, 100.0, 0.1, 1.0, 0.1, 0.9,
                   at.astimezone(timezone.utc).isoformat(), bot))


def test_the_window_starts_at_the_paper_period_and_has_no_upper_bound(tmp_path):
    h = Harness(tmp_path)
    bot = h.plan.bot_instance_id
    start = datetime.fromtimestamp(h.now / 1000, tz=timezone.utc)
    _close_fill(h.db, bot, start)
    _close_fill(h.db, bot, start + timedelta(days=1))
    before = _record(h.db, h.plan, at=h.now - DAY)                 # before the first paper close: not counted
    after = _record(h.db, h.plan, at=h.now + 5 * DAY)              # after the last close: still a sizing failure
    r = _report(h.db, bot)
    assert r.closed_trade_count == 2
    assert r.sizing_refusal_ids == [after.risk_decision_id] and before.risk_decision_id not in r.sizing_refusal_ids


def test_legacy_sizing_failure_events_are_read_for_compatibility_and_reported_separately(tmp_path):
    import json
    h, risk = _executor_sizing_refusal(tmp_path)
    with h.db.connect() as c:
        c.execute("INSERT INTO events (timestamp_utc, ts, run_id, cycle_id, symbol, event_type, action, details_json) "
                  "VALUES (?,?,?,?,?,?,?,?)", ("2026-01-05T00:00:00+00:00", "2026-01-05T00:00:00+00:00", "run-old",
                                               "c", "BTCUSDT", "SIZING_FAILURE", None,
                                               json.dumps({"bot_instance_id": h.plan.bot_instance_id})))
    r = _report(h.db, h.plan.bot_instance_id)
    assert (r.sizing_failure_count, r.sizing_refusal_count_durable, r.sizing_failure_legacy_event_count) == (2, 1, 1)


def test_a_database_without_the_evidence_table_fails_closed(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    bare = DB(path=str(tmp_path / "no_cati.db"))        # a throwaway file: the real schema minus the evidence table
    migrate(bare)
    with bare.connect() as c:
        c.execute("DROP TABLE cati_risk_decisions")
    r = _report(bare, "botA")
    assert r.sizing_evidence_available is False
    assert RejectionReason.SIZING_EVIDENCE_UNAVAILABLE.value in r.blocking_reasons


def test_the_activation_refusal_and_the_execution_gate_carry_the_sizing_reason(tmp_path):
    """The readers of the gate: real-capital activation (``assert_user_capital_activation_allowed``) and the
    connected-account execution gate (``evaluate_execution_readiness``) both surface the sizing refusals."""
    from app.product_safety.execution_safety import evaluate_execution_readiness
    from app.product_safety.readiness_gate import UserCapitalReadinessError, assert_user_capital_activation_allowed
    h, risk = _executor_sizing_refusal(tmp_path)
    with pytest.raises(UserCapitalReadinessError) as err:
        assert_user_capital_activation_allowed(db=h.db, bot_instance_id=h.plan.bot_instance_id)
    payload = err.value.payload
    assert RejectionReason.SIZING_FAILURES_DETECTED.value in payload["missing_requirements"]
    assert payload["readiness_report"]["sizing_refusal_ids"] == [risk.risk_decision_id]
    decision = evaluate_execution_readiness(db=h.db, bot_instance_id=h.plan.bot_instance_id, broker_environment="live")
    assert decision.allowed is False and RejectionReason.SIZING_FAILURES_DETECTED.value in decision.reason


def test_the_admin_readiness_status_route_reports_the_sizing_refusals(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api import readiness_admin
    from app.core.auth import require_admin
    h, risk = _executor_sizing_refusal(tmp_path)
    monkeypatch.setattr(readiness_admin, "DB", lambda: h.db)
    app = FastAPI()
    app.include_router(readiness_admin.router)
    client = TestClient(app)
    path = f"/api/v1/admin/bots/{h.plan.bot_instance_id}/readiness"
    assert client.get(path).status_code == 401                        # admin only
    app.dependency_overrides[require_admin] = lambda: {"id": "admin-e03", "role": "admin"}
    body = client.get(path).json()
    assert RejectionReason.SIZING_FAILURES_DETECTED.value in body["blocking_reasons"]
    assert body["sizing_refusal_ids"] == [risk.risk_decision_id]
    assert body["sizing_refusals_by_account"] == {h.plan.broker_account_id: 1}
    assert client.get("/api/v1/admin/bots/no-such-bot/readiness").status_code == 404


# ════════════════════════════════════════════════════════════════════════════════════ the operations alert
@pytest.fixture
def alerts(tmp_path):
    from app.ops.alerting import AlertEngine
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    platform = Database(f"sqlite:///{(tmp_path / 'platform.db').as_posix()}")
    migrate(platform, all_migrations())
    yield AlertEngine(platform, sinks=[])
    platform.close()


def test_repeated_real_sizing_refusals_raise_one_account_alert_that_resolves(tmp_path, alerts):
    from app.ops import alert_producers as P
    from shared_lib.operations.alerting import fingerprint, pseudonym
    h, risk = _executor_sizing_refusal(tmp_path)
    account = h.plan.broker_account_id
    _record(h.db, h.plan, at=h.now + 1)
    _record(h.db, h.plan, at=h.now + 2, codes=("SIZING_FAILURE_MIN_NOTIONAL",))
    _record(h.db, h.plan, at=h.now + 3, bot_instance_id="botC", broker_account_id="acct2")   # one refusal: quiet
    assert RiskDecisionStore(h.db).append(risk) is False                                      # never double counted

    def tick(now):
        return P.evaluate_sizing_refusals(P.ProducerContext(alerts, runtime_db=h.db, now_ms=now, state={}))

    out = tick(h.now + HOUR)
    assert out["firing"] == 1
    [alert] = [a for a in alerts.open_alerts() if a["category"] == "REPEATED_SIZING_REFUSALS"]
    assert alert["fingerprint"] == fingerprint("REPEATED_SIZING_REFUSALS", {"account": account})
    assert alert["severity"] == "MEDIUM" and alert["scope"] == "ACCOUNT" and alert["account_ref"] == account
    assert alert["detail"]["refusals"] == 3 and alert["detail"]["account"] == pseudonym(account)
    assert account not in alert["title"]
    # a new process object over the same file: the same durable rows, the same (deduplicated) alert
    from shared_lib.persistence.db import DB
    again = P.evaluate_sizing_refusals(P.ProducerContext(alerts, runtime_db=DB(path=h.db.path), now_ms=h.now + 2 * HOUR,
                                                         state={}))
    assert again["firing"] == 1 and len([a for a in alerts.open_alerts()
                                          if a["category"] == "REPEATED_SIZING_REFUSALS"]) == 1
    # a day later the window holds none: the alert resolves itself
    assert tick(h.now + 2 * DAY)["resolved"] == 1
    assert not [a for a in alerts.open_alerts() if a["category"] == "REPEATED_SIZING_REFUSALS"]
