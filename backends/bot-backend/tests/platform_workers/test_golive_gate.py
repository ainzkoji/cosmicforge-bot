"""Step 6.1-6.3: the go-live gate -- every condition independent, visible and fail-closed; two-person rules;
revocations that survive a database restore; live-beta stages that never enable anything by themselves."""
from __future__ import annotations

import shutil
import sqlite3
import time
from pathlib import Path

import pytest

from app.golive import gate as G
from app.golive.authorization import GoLiveAuthority
from app.golive.beta import BetaReviews, LiveBeta, export_evidence
from app.golive.ledger import GoLiveError, GoLiveLedger, Operator
from shared_lib.persistence.sql import Database

ALICE, BOB = Operator("alice", "test"), Operator("bob", "test")
DAY = 86_400_000
REQ = G.Request(account_id="acct-1", user_id="alice", family_id="FAM", family_version="1.0.0", venue="binance")


@pytest.fixture
def anchor(tmp_path):
    return str(tmp_path / "anchor" / "revocations.jsonl")


@pytest.fixture
def ledger(platform_db, anchor):
    return GoLiveLedger(platform_db, anchor_path=anchor)


def green_sources(**override):
    base = dict(
        adapter_certified=lambda venue, env, now: (True, ""),
        family_entry=lambda fid: {"family_version": "1.0.0", "certification_status": "CERTIFIED",
                                  "allowed_modes": ["SHADOW", "DEMO", "LIVE"], "governance_stage": "M7",
                                  "kill_switch_state": "OFF", "disabled": False},
        identity_permits=lambda user, action: (True, ""),
        account=lambda aid: {"id": aid, "user_id": "alice", "broker_id": "binance", "status": "connected",
                             "environment": "LIVE"},
        bot_for_account=lambda aid: {"id": "bot-1", "user_id": "alice", "risk_level": "conservative",
                                     "capital_allocation": 500},
        live_submission_enabled=lambda: True,
        kill_switch=lambda scope: False,
        open_uncertainty=lambda aid: [],
    )
    base.update(override)
    return G.Sources(**base)


def fully_authorized(ledger, *, until_days=30):
    auth, beta = GoLiveAuthority(ledger), LiveBeta(ledger)
    auth.set_policy("MIN_DEMO_RUN_DAYS", 28, ALICE, reason="owner decision (test)")
    auth.set_policy("LIVE_CONSENT_VERSION", "live-v1", ALICE, reason="test")
    auth.set_policy("STAGE_A_MAX_NOTIONAL", "1000", ALICE, reason="test")
    now = int(time.time() * 1000)
    auth.record_demo_run(family_id="FAM", family_version="1.0.0", venue="binance", run_ref="demo-1",
                         started_at_ms=now - 30 * DAY, ended_at_ms=now - DAY, outcome="PASS", operator=BOB)
    auth.give_consent(account_id="acct-1", user_id="alice", consent_version="live-v1", text_sha256="ab" * 32)
    rid = auth.request_switch_on("binance", ALICE, reason="stage A")
    auth.approve_switch_on("binance", rid, BOB, reason="second operator")
    aid = auth.request_approval(account_id="acct-1", family_id="FAM", venue="binance", operator=ALICE, reason="A")
    auth.grant_approval(aid, BOB, reason="reviewed")
    beta.enroll_stage_a(account_id="acct-1", user_id="alice", operator=ALICE, authorized_until_ms=now + until_days * DAY,
                        budget="500", risk_level="conservative", reason="operator account")
    return auth, aid


def test_this_build_refuses_live_even_when_everything_else_passes(ledger):
    assert G.BUILD_PERMITS_LIVE is False
    fully_authorized(ledger)
    decision = G.GoLiveGate(ledger, green_sources()).evaluate(REQ)
    assert not decision.authorized
    assert [c.id for c in decision.blocking] == ["BUILD_PERMITS_LIVE"]
    assert len(decision.conditions) == len(G.CONDITION_IDS) == 17


def test_conditions_are_satisfiable_together_when_the_build_lock_is_lifted_in_a_test(ledger, monkeypatch):
    monkeypatch.setattr(G, "BUILD_PERMITS_LIVE", True)       # test process only
    fully_authorized(ledger)
    assert G.GoLiveGate(ledger, green_sources()).evaluate(REQ).authorized


BREAKERS = {
    "VENUE_ADAPTER_CERTIFIED": dict(adapter_certified=lambda v, e, n: (False, "ADAPTER_NOT_CERTIFIED")),
    "STRATEGY_RESEARCH_PASSED": dict(family_entry=lambda f: {"family_version": "1.0.0", "certification_status": "NOT_CERTIFIED",
                                                             "allowed_modes": ["LIVE"], "kill_switch_state": "OFF"}),
    "FAMILY_STAGE_PERMITS_LIVE": dict(family_entry=lambda f: {"family_version": "1.0.0", "certification_status": "CERTIFIED",
                                                              "allowed_modes": ["SHADOW", "DEMO"], "governance_stage": "M6",
                                                              "kill_switch_state": "OFF"}),
    "IDENTITY_REQUIREMENT_SATISFIED": dict(identity_permits=lambda u, a: (False, "VERIFICATION_REQUIRED")),
    "ACCOUNT_CONNECTION_VALID": dict(account=lambda a: {"id": a, "user_id": "alice", "broker_id": "binance",
                                                        "status": "disconnected", "environment": "LIVE"}),
    "RISK_PROFILE_VALID": dict(bot_for_account=lambda a: {"risk_level": "yolo", "capital_allocation": 500}),
    "ACCOUNT_CAPITAL_RULES": dict(bot_for_account=lambda a: {"risk_level": "conservative", "capital_allocation": 900}),
    "LIVE_ORDER_SUBMISSION_ENABLED": dict(live_submission_enabled=lambda: False),
    "KILL_SWITCHES_CLEAR": dict(kill_switch=lambda scope: scope == "acct-1"),
    "NO_OPEN_EXECUTION_UNCERTAINTY": dict(open_uncertainty=lambda a: ["PROTECTION_UNSETTLED"]),
}


@pytest.mark.parametrize("condition", sorted(BREAKERS))
def test_each_source_condition_fails_on_its_own(ledger, monkeypatch, condition):
    monkeypatch.setattr(G, "BUILD_PERMITS_LIVE", True)
    fully_authorized(ledger)
    decision = G.GoLiveGate(ledger, green_sources(**BREAKERS[condition])).evaluate(REQ)
    assert [c.id for c in decision.blocking] == [condition]
    assert decision.blocking[0].reason, "a blocked condition always says why"


def test_ledger_conditions_fail_on_their_own(ledger, monkeypatch):
    monkeypatch.setattr(G, "BUILD_PERMITS_LIVE", True)
    auth, approval = fully_authorized(ledger)
    gate = G.GoLiveGate(ledger, green_sources())
    auth.revoke_consent(account_id="acct-1", user_id="alice")
    d = gate.evaluate(REQ)
    assert [c.id for c in d.blocking] == ["CUSTOMER_LIVE_RISK_CONSENT"] and d.blocking[0].reason == "CONSENT_REVOKED"
    auth.give_consent(account_id="acct-1", user_id="alice", consent_version="live-v1", text_sha256="cd" * 32)
    auth.switch_off("binance", BOB, reason="incident")          # one operator closes, immediately
    assert [c.id for c in gate.evaluate(REQ).blocking] == ["OPERATOR_LIVE_SWITCH_ON"]
    rid = auth.request_switch_on("binance", BOB, reason="reopen")
    auth.approve_switch_on("binance", rid, ALICE, reason="ok")
    auth.revoke_approval(approval, ALICE, reason="re-review")
    d = gate.evaluate(REQ)
    assert [c.id for c in d.blocking] == ["OPERATOR_APPROVAL_AUDITED"] and d.blocking[0].reason == "APPROVAL_REVOKED"


def test_unset_owner_policy_blocks_instead_of_defaulting(ledger):
    auth = GoLiveAuthority(ledger)
    d = G.GoLiveGate(ledger, green_sources()).evaluate(REQ)
    by = {c.id: c for c in d.conditions}
    assert by["STRATEGY_DEMO_RUN_COMPLETED"].status == G.BLOCKED_PENDING_POLICY
    auth.give_consent(account_id="acct-1", user_id="alice", consent_version="live-v1", text_sha256="ab" * 32)
    d = G.GoLiveGate(ledger, green_sources()).evaluate(REQ)
    assert {c.id: c for c in d.conditions}["CUSTOMER_LIVE_RISK_CONSENT"].status == G.BLOCKED_PENDING_POLICY


def test_short_or_failed_or_invalidated_demo_runs_do_not_count(ledger):
    auth = GoLiveAuthority(ledger)
    auth.set_policy("MIN_DEMO_RUN_DAYS", 28, ALICE, reason="t")
    now = int(time.time() * 1000)
    short = auth.record_demo_run(family_id="FAM", family_version="1.0.0", venue="binance", run_ref="short",
                                 started_at_ms=now - 5 * DAY, ended_at_ms=now, outcome="PASS", operator=BOB)
    auth.record_demo_run(family_id="FAM", family_version="1.0.0", venue="binance", run_ref="failed",
                         started_at_ms=now - 40 * DAY, ended_at_ms=now, outcome="FAIL", operator=BOB)
    good = auth.record_demo_run(family_id="FAM", family_version="1.0.0", venue="binance", run_ref="good",
                                started_at_ms=now - 40 * DAY, ended_at_ms=now, outcome="PASS", operator=BOB)
    gate = G.GoLiveGate(ledger, green_sources())
    cond = lambda: {c.id: c for c in gate.evaluate(REQ).conditions}["STRATEGY_DEMO_RUN_COMPLETED"]  # noqa: E731
    assert cond().status == G.PASS
    auth.invalidate_demo_run(family_id="FAM", run_event_id=good, operator=ALICE, reason="data defect")
    assert cond().reason == "DEMO_RUN_SHORTER_THAN_POLICY"
    assert short


def test_self_approval_is_refused_for_switch_and_account_approval(ledger):
    auth = GoLiveAuthority(ledger)
    rid = auth.request_switch_on("bybit", ALICE, reason="r")
    with pytest.raises(GoLiveError, match="SELF_APPROVAL_REFUSED"):
        auth.approve_switch_on("bybit", rid, ALICE, reason="me again")
    aid = auth.request_approval(account_id="a", family_id="F", venue="bybit", operator=ALICE, reason="r")
    with pytest.raises(GoLiveError, match="SELF_APPROVAL_REFUSED"):
        auth.grant_approval(aid, ALICE, reason="me again")


def test_live_switch_alone_cannot_authorize(ledger, monkeypatch):
    monkeypatch.setattr(G, "BUILD_PERMITS_LIVE", True)
    auth = GoLiveAuthority(ledger)
    rid = auth.request_switch_on("binance", ALICE, reason="r")
    auth.approve_switch_on("binance", rid, BOB, reason="r")
    d = G.GoLiveGate(ledger, green_sources()).evaluate(REQ)
    assert not d.authorized and "OPERATOR_LIVE_SWITCH_ON" not in [c.id for c in d.blocking]
    assert len(d.blocking) >= 4


def test_expired_approval_and_beta_window_refuse_automatically(ledger, monkeypatch):
    monkeypatch.setattr(G, "BUILD_PERMITS_LIVE", True)
    fully_authorized(ledger, until_days=1)
    gate = G.GoLiveGate(ledger, green_sources())
    later = int(time.time() * 1000) + 2 * DAY
    d = gate.evaluate(REQ, now_ms=later)
    assert "LIVE_BETA_ENROLLMENT_CURRENT" in [c.id for c in d.blocking]
    assert {c.id: c for c in d.conditions}["LIVE_BETA_ENROLLMENT_CURRENT"].reason == "BETA_AUTHORIZATION_EXPIRED"


def test_a_failing_check_is_a_refusal_not_a_pass(ledger, monkeypatch):
    monkeypatch.setattr(G, "BUILD_PERMITS_LIVE", True)
    fully_authorized(ledger)

    def boom(*a):
        raise ConnectionError("registry unreachable")

    d = G.GoLiveGate(ledger, green_sources(family_entry=boom)).evaluate(REQ)
    assert not d.authorized
    assert {c.status for c in d.blocking} == {G.CHECK_ERROR}


def test_ledger_is_append_only_on_both_dialects(ledger):
    GoLiveAuthority(ledger).set_policy("MIN_DEMO_RUN_DAYS", 28, ALICE, reason="t")
    for sql in ("UPDATE golive_events SET actor_ref='mallory'", "DELETE FROM golive_events"):
        with pytest.raises(Exception, match="APPEND_ONLY"):
            with ledger.db.transaction() as conn:
                conn.execute(sql)


def test_revocation_requires_an_off_database_anchor(platform_db):
    with pytest.raises(GoLiveError, match="REVOCATION_ANCHOR_NOT_CONFIGURED"):
        GoLiveAuthority(GoLiveLedger(platform_db, anchor_path="")).switch_off("binance", ALICE, reason="r")


def test_database_restore_cannot_resurrect_a_revoked_authorization(tmp_path, anchor, monkeypatch):
    monkeypatch.setattr(G, "BUILD_PERMITS_LIVE", True)
    path = tmp_path / "golive.db"
    from shared_lib.persistence.sql import migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    db = Database(f"sqlite:///{path.as_posix()}")
    migrate(db, all_migrations())
    ledger = GoLiveLedger(db, anchor_path=anchor)
    auth, approval = fully_authorized(ledger)
    gate = G.GoLiveGate(ledger, green_sources())
    assert gate.evaluate(REQ).authorized
    db.close()
    backup = tmp_path / "backup.db"
    src, dst = sqlite3.connect(str(path)), sqlite3.connect(str(backup))
    src.backup(dst)
    src.close(), dst.close()                                   # the nightly backup, taken while authorized
    db = Database(f"sqlite:///{path.as_posix()}")
    ledger = GoLiveLedger(db, anchor_path=anchor)
    GoLiveAuthority(ledger).revoke_approval(approval, BOB, reason="customer complaint")
    assert not G.GoLiveGate(ledger, green_sources()).evaluate(REQ).authorized
    db.close()
    for suffix in ("", "-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)
    shutil.copy(backup, path)                                   # an operator restores the old backup
    db = Database(f"sqlite:///{path.as_posix()}")
    restored = G.GoLiveGate(GoLiveLedger(db, anchor_path=anchor), green_sources()).evaluate(REQ)
    assert not restored.authorized
    blocking = {c.id: c.reason for c in restored.blocking}
    assert blocking["AUTHORIZATION_LEDGER_INTACT"].startswith("LEDGER_BEHIND_REVOCATION_ANCHOR")
    db.close()


def test_stage_a_rules(ledger):
    beta, auth = LiveBeta(ledger), GoLiveAuthority(ledger)
    now = int(time.time() * 1000)
    kw = dict(account_id="acct-1", user_id="alice", operator=ALICE, authorized_until_ms=now + DAY, budget="100",
              risk_level="conservative", reason="r")
    with pytest.raises(GoLiveError, match="STAGE_A_MAX_NOTIONAL_NOT_SET"):
        beta.enroll_stage_a(**kw)
    auth.set_policy("STAGE_A_MAX_NOTIONAL", "250", ALICE, reason="owner")
    with pytest.raises(GoLiveError, match="OWN_ACCOUNT"):
        beta.enroll_stage_a(**{**kw, "user_id": "carol"})
    with pytest.raises(GoLiveError, match="CONSERVATIVE"):
        beta.enroll_stage_a(**{**kw, "risk_level": "balanced"})
    with pytest.raises(GoLiveError, match="CEILING"):
        beta.enroll_stage_a(**{**kw, "budget": "251"})
    beta.enroll_stage_a(**kw)
    assert beta.status("acct-1")["current"] is True
    assert beta.status("acct-1", now_ms=now + 2 * DAY)["reason"] == "BETA_AUTHORIZATION_EXPIRED"


def test_stage_b_invitation_enrolment_withdrawal_and_disable(ledger, monkeypatch):
    monkeypatch.setattr(G, "BUILD_PERMITS_LIVE", False)
    beta, auth = LiveBeta(ledger), GoLiveAuthority(ledger)
    now = int(time.time() * 1000)
    with pytest.raises(GoLiveError, match="STAGE_B_MAX_BUDGET_NOT_SET"):
        beta.invite(account_id="c-1", user_id="carol", operator=ALICE, budget_ceiling="300", expires_at_ms=now + 9 * DAY,
                    reason="r")
    auth.set_policy("STAGE_B_MAX_BUDGET", "1000", ALICE, reason="owner")
    with pytest.raises(GoLiveError, match="ABOVE_POLICY"):
        beta.invite(account_id="c-1", user_id="carol", operator=ALICE, budget_ceiling="5000", expires_at_ms=now + DAY,
                    reason="r")
    beta.invite(account_id="c-1", user_id="carol", operator=ALICE, budget_ceiling="300", expires_at_ms=now + 9 * DAY,
                reason="invited")
    req = G.Request(account_id="c-1", user_id="carol", family_id="FAM", family_version="1.0.0", venue="binance")
    gate = G.GoLiveGate(ledger, green_sources(account=lambda a: {"id": a, "user_id": "carol", "broker_id": "binance",
                                                                 "status": "connected", "environment": "LIVE"}))
    assert "LIVE_BETA_ENROLLMENT_CURRENT" in [c.id for c in gate.evaluate(req).blocking]   # invited is not enrolled
    with pytest.raises(GoLiveError, match="ANOTHER_USER"):
        beta.accept(account_id="c-1", user_id="mallory", budget="100", risk_level="balanced", consent_ref="x",
                    authorized_until_ms=now + 30 * DAY)
    with pytest.raises(GoLiveError, match="CEILING"):
        beta.accept(account_id="c-1", user_id="carol", budget="301", risk_level="balanced", consent_ref="x",
                    authorized_until_ms=now + 30 * DAY)
    beta.accept(account_id="c-1", user_id="carol", budget="200", risk_level="balanced", consent_ref="consent-ev",
                authorized_until_ms=now + 30 * DAY)
    status = beta.status("c-1")
    assert status["current"] and int(status["authorized_until_ms"]) <= now + 9 * DAY   # capped at the invitation
    decision = gate.evaluate(req)
    assert not decision.authorized and "LIVE_BETA_ENROLLMENT_CURRENT" not in [c.id for c in decision.blocking]
    beta.withdraw(account_id="c-1", user_id="carol")
    assert beta.status("c-1")["reason"] == "BETA_WITHDRAWN"


def test_reviews_and_evidence_export(ledger):
    reviews = BetaReviews(ledger.db)
    fully_authorized(ledger)
    shadow = [{"symbol": "BTCUSDT", "quantity": "0.010"}]
    fills = [{"symbol": "BTCUSDT", "quantity": "0.010", "side": "BUY", "reference_price": "50000", "price": "50010"}]
    day = reviews.daily(stage="A", account_id="acct-1", day="2026-10-16", reviewer_ref="operator:bob", shadow=shadow,
                        live_fills=fills, equity=[1000, 990, 1001], incidents=[],
                        limits={"max_drawdown": 0.02, "max_worst_slippage_bps": 25})
    assert day["verdict"] == "WITHIN_EXPECTATION" and day["slippage"]["worst_bps"] == 2.0
    unset = reviews.daily(stage="A", account_id="acct-1", day="2026-10-17", reviewer_ref="operator:bob", shadow=shadow,
                          live_fills=fills, equity=[1000], incidents=[], limits={})
    assert unset["verdict"] == "INCOMPLETE"
    week = reviews.weekly_summary(stage="A", scope_id="acct-1", week_start="2026-10-12", week_end="2026-10-18",
                                  reviewer_ref="operator:bob", expectations=None)
    assert week["verdict"] == "INCOMPLETE" and week["versus_backtest"]["status"] == "NO_REGISTERED_EXPECTATION"
    bundle = export_evidence(ledger, reviews, "acct-1")
    assert bundle["ledger_verified"] and len(bundle["reviews"]) == 3 and len(bundle["bundle_sha256"]) == 64
    with pytest.raises(Exception, match="APPEND_ONLY"):
        with ledger.db.transaction() as conn:
            conn.execute("UPDATE beta_reviews SET verdict='WITHIN_EXPECTATION'")


def test_real_families_are_blocked_on_the_real_runtime_database(platform_db, anchor, tmp_path, monkeypatch):
    """Production wiring against a migrated runtime database: DAILY_TREND is registered but not certified, at M0,
    and no venue adapter holds LIVE certification. The gate refuses for those real reasons."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate as legacy_migrate
    from app.trading_intelligence.pipeline.family_registry import FamilyRegistry
    runtime = DB(path=str(tmp_path / "runtime.db"))
    legacy_migrate(runtime)
    FamilyRegistry(runtime).register_all(actor_ref="system:test")
    gate = G.GoLiveGate(GoLiveLedger(platform_db, anchor_path=anchor), G.production_sources(runtime))
    d = gate.evaluate(G.Request(account_id="none", user_id="u", family_id="DAILY_TREND", family_version="1.0.0",
                                venue="binance"))
    blocking = {c.id: c for c in d.blocking}
    assert not d.authorized
    for required in ("VENUE_ADAPTER_CERTIFIED", "STRATEGY_RESEARCH_PASSED", "FAMILY_STAGE_PERMITS_LIVE",
                     "OPERATOR_LIVE_SWITCH_ON", "OPERATOR_APPROVAL_AUDITED", "LIVE_ORDER_SUBMISSION_ENABLED",
                     "BUILD_PERMITS_LIVE", "ACCOUNT_CONNECTION_VALID"):
        assert required in blocking, required
    assert blocking["STRATEGY_RESEARCH_PASSED"].reason.startswith("RESEARCH_")
    assert all(c.status != G.PASS or c.id in ("KILL_SWITCHES_CLEAR", "NO_OPEN_EXECUTION_UNCERTAINTY",
                                                "AUTHORIZATION_LEDGER_INTACT")
               for c in d.conditions)


def test_execution_boundary_refuses_live_entries_through_the_gate(platform_db, database_url, anchor, tmp_path,
                                                                  monkeypatch):
    from types import SimpleNamespace
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate as legacy_migrate
    from app.trading_intelligence.execution.boundary import CATIExecutionBoundary
    runtime = DB(path=str(tmp_path / "runtime.db"))
    legacy_migrate(runtime)
    plan = SimpleNamespace(broker_account_id="acct", user_id="u", setup_family="DAILY_TREND", setup_version="1.0.0",
                           venue="binance", environment="LIVE", versions={})
    boundary = SimpleNamespace(_db=runtime)
    monkeypatch.delenv("PLATFORM_DATABASE_URL", raising=False)
    assert CATIExecutionBoundary._go_live_reason(boundary, plan, 0) == "GO_LIVE_GATE_UNAVAILABLE:RuntimeError"
    monkeypatch.setenv("PLATFORM_DATABASE_URL", database_url)
    monkeypatch.setenv("GOLIVE_REVOCATION_ANCHOR", anchor)
    why = CATIExecutionBoundary._go_live_reason(boundary, plan, int(time.time() * 1000))
    assert why.startswith("GO_LIVE_GATE:") and why != "GO_LIVE_GATE:"


def test_withdrawn_legal_acceptance_blocks_consent(ledger, monkeypatch):
    monkeypatch.setattr(G, "BUILD_PERMITS_LIVE", True)
    fully_authorized(ledger)
    d = G.GoLiveGate(ledger, green_sources(legal_acceptance=lambda u: ["AUTOMATED_EXECUTION_CONSENT_NOT_ACCEPTED"])
                     ).evaluate(REQ)
    assert [c.id for c in d.blocking] == ["CUSTOMER_LIVE_RISK_CONSENT"]
    assert d.blocking[0].reason == "AUTOMATED_EXECUTION_CONSENT_NOT_ACCEPTED"
