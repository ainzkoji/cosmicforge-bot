"""Step 7.4 -- staged market activation: PAPER -> DEMO -> LIVE (+ REVOKED), owner authorization on every
transition, policy durations that block until the owner sets them, DEMO only with a CERTIFIED research
status, LIVE refused in this build, revocation always possible, and an append-only hash-chained record."""
from __future__ import annotations

from decimal import Decimal

import pytest

import _step7 as F
from app.markets import activation as A
from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
from shared_lib.persistence.markets_schema import ACTIVATION_EVENT_TABLE

OPERATOR = OperatorAuthorization("owner-1", "admin_api", "admin.markets")
FAMILY, MARKET = "TEST_TRADFI_FAMILY", "FIX_FUTURES"
DAY = 86_400_000
T0 = 1_800_000_000_000
PAPER_EVIDENCE = {"research_specification": "research/trend_v2_tradfi/SPEC.md"}
DEMO_EVIDENCE = {"paper_record": "evidence/paper/1", "research_certification": "register:H-TEST"}


def ledger(db, *, certified=True, adapter=True, supports=True, policy=None):
    return A.MarketActivationLedger(
        db, registry=F.registry(), policy=policy,
        certification_lookup=lambda f, m: {"status": "CERTIFIED" if certified else "NOT_CERTIFIED"},
        adapter_available=lambda venue: adapter,
        supports=(lambda f, m: True) if supports else (lambda f, m: False))


@pytest.fixture
def db(tmp_path):
    return F.fresh_db(tmp_path)


def to_paper(led, now=T0):
    out = led.request_transition(MARKET, FAMILY, A.PAPER, operator=OPERATOR, reason="start paper",
                                 evidence=PAPER_EVIDENCE, now_ms=now)
    assert out["status"] == A.APPROVED, out
    return out


# ── authorization ──────────────────────────────────────────────────────────

def test_every_transition_needs_an_operator_authorization_and_a_reason(db):
    led = ledger(db)
    with pytest.raises(A.ActivationError) as e:
        led.request_transition(MARKET, FAMILY, A.PAPER, operator="owner-1", reason="x", evidence=PAPER_EVIDENCE)
    assert e.value.code == "OPERATOR_AUTHORIZATION_REQUIRED"
    with pytest.raises(A.ActivationError) as e:
        led.request_transition(MARKET, FAMILY, A.PAPER, operator=OPERATOR, reason=" ", evidence=PAPER_EVIDENCE)
    assert e.value.code == A.REASON_REQUIRED
    with pytest.raises(A.ActivationError):
        led.revoke(MARKET, FAMILY, operator=None, reason="x")
    assert led.history() == []


def test_nothing_is_active_by_default_and_nothing_promotes_itself(db):
    led = ledger(db)
    assert led.state(MARKET, FAMILY).stage == A.NONE
    assert led.highest_stage(MARKET) == A.NONE
    to_paper(led)
    # time passing changes nothing: the stage only moves on an authorized request
    assert led.state(MARKET, FAMILY).stage == A.PAPER


# ── PAPER ──────────────────────────────────────────────────────────────────

def test_paper_needs_its_evidence_and_a_family_that_declares_the_market(db):
    out = ledger(db).request_transition(MARKET, FAMILY, A.PAPER, operator=OPERATOR, reason="x", evidence={}, now_ms=T0)
    assert out["status"] == A.REFUSED and "EVIDENCE_MISSING:research_specification" in out["blockers"]
    out = ledger(db, supports=False).request_transition(MARKET, FAMILY, A.PAPER, operator=OPERATOR, reason="x",
                                                        evidence=PAPER_EVIDENCE, now_ms=T0)
    assert out["status"] == A.REFUSED and A.FAMILY_DOES_NOT_DECLARE_MARKET in out["blockers"]
    assert ledger(db).state(MARKET, FAMILY).stage == A.NONE
    assert [r["event_type"] for r in ledger(db).history()] == [A.TRANSITION_REFUSED, A.TRANSITION_REFUSED]


def test_paper_is_approved_with_risk_restrictions_and_exit_criteria_recorded(db):
    out = to_paper(ledger(db))
    assert out["event"]["to_stage"] == A.PAPER and out["event"]["from_stage"] == A.NONE
    payload = out["event"]["payload"]
    assert payload["risk_restrictions"] == {"max_risk_per_trade_pct": "0.40", "max_entry_stop_pct": "15"}
    assert payload["exit_criteria"] and payload["operator"] == "owner-1"
    assert payload["evidence"] == PAPER_EVIDENCE


def test_crypto_markets_and_unknown_markets_are_not_activated_here(db):
    led = ledger(db)
    crypto = led.request_transition("CRYPTO_BINANCE_USDT_PERPETUAL", FAMILY, A.PAPER, operator=OPERATOR, reason="x",
                                    evidence=PAPER_EVIDENCE, now_ms=T0)
    assert crypto["status"] == A.REFUSED and A.MARKET_GOVERNED_BY_FAMILY_GOVERNANCE in crypto["blockers"]
    unknown = led.request_transition("NOWHERE", FAMILY, A.PAPER, operator=OPERATOR, reason="x",
                                     evidence=PAPER_EVIDENCE, now_ms=T0)
    assert unknown["status"] == A.REFUSED and A.MARKET_UNKNOWN in unknown["blockers"]


def test_stages_cannot_be_skipped(db):
    led = ledger(db)
    out = led.request_transition(MARKET, FAMILY, A.DEMO, operator=OPERATOR, reason="x",
                                 evidence={**PAPER_EVIDENCE, **DEMO_EVIDENCE}, now_ms=T0)
    assert out["status"] == A.REFUSED and "STAGE_SKIPPED:NONE->DEMO" in out["blockers"]
    with pytest.raises(A.ActivationError):
        led.request_transition(MARKET, FAMILY, "LIMITED_PRODUCTION", operator=OPERATOR, reason="x")


# ── DEMO ───────────────────────────────────────────────────────────────────

def test_demo_with_no_duration_policy_is_blocked_pending_policy(db):
    led = ledger(db)
    to_paper(led)
    out = led.request_transition(MARKET, FAMILY, A.DEMO, operator=OPERATOR, reason="x", evidence=DEMO_EVIDENCE,
                                 now_ms=T0 + 400 * DAY)
    assert out["status"] == A.BLOCKED_PENDING_POLICY
    assert out["pending_policy"] == ["POLICY_DURATION_UNSET:paper_min_days"] and out["blockers"] == []
    assert led.state(MARKET, FAMILY).stage == A.PAPER
    assert led.history()[-1]["event_type"] == A.TRANSITION_BLOCKED_PENDING_POLICY


def test_demo_needs_a_certified_research_status(db):
    led = ledger(db, certified=False, policy=A.ActivationPolicy(policy_id="t", paper_min_days=30))
    to_paper(led)
    out = led.request_transition(MARKET, FAMILY, A.DEMO, operator=OPERATOR, reason="x", evidence=DEMO_EVIDENCE,
                                 now_ms=T0 + 31 * DAY)
    assert out["status"] == A.REFUSED and "RESEARCH_CERTIFICATION_NOT_CERTIFIED" in out["blockers"]


def test_demo_needs_the_paper_duration_a_verified_calendar_and_an_adapter(db):
    policy = A.ActivationPolicy(policy_id="t", paper_min_days=30)
    led = ledger(db, policy=policy)
    to_paper(led)
    early = led.request_transition(MARKET, FAMILY, A.DEMO, operator=OPERATOR, reason="x", evidence=DEMO_EVIDENCE,
                                   now_ms=T0 + 29 * DAY)
    assert early["status"] == A.REFUSED and early["blockers"] == ["STAGE_DURATION_NOT_MET:paper_min_days"]
    no_adapter = ledger(db, policy=policy, adapter=False).request_transition(
        MARKET, FAMILY, A.DEMO, operator=OPERATOR, reason="x", evidence=DEMO_EVIDENCE, now_ms=T0 + 31 * DAY)
    assert no_adapter["status"] == A.REFUSED and A.NO_EXECUTION_ADAPTER in no_adapter["blockers"]
    # a synthetic, fully satisfied case: every criterion met, the stage moves to DEMO
    ok = led.request_transition(MARKET, FAMILY, A.DEMO, operator=OPERATOR, reason="x", evidence=DEMO_EVIDENCE,
                                now_ms=T0 + 31 * DAY)
    assert ok["status"] == A.APPROVED and led.state(MARKET, FAMILY).stage == A.DEMO


def test_demo_on_an_unverified_calendar_is_refused(db):
    led = ledger(db, policy=A.ActivationPolicy(policy_id="t", paper_min_days=1))
    led.request_transition("FIX_UNVERIFIED", FAMILY, A.PAPER, operator=OPERATOR, reason="x", evidence=PAPER_EVIDENCE,
                           now_ms=T0)
    out = led.request_transition("FIX_UNVERIFIED", FAMILY, A.DEMO, operator=OPERATOR, reason="x",
                                 evidence=DEMO_EVIDENCE, now_ms=T0 + 2 * DAY)
    assert out["status"] == A.REFUSED and A.CALENDAR_UNVERIFIED in out["blockers"]


def test_the_default_lookups_refuse_demo_for_every_shipped_tradfi_market(db):
    """No fixture lookups: the real research register, the real adapters, the real declarations."""
    led = A.MarketActivationLedger(db, policy=A.ActivationPolicy(policy_id="t", paper_min_days=1))
    from app.markets.registry import default_registry

    for market in default_registry().tradfi_markets():
        paper = led.request_transition(market.market_id, "DAILY_TREND", A.PAPER, operator=OPERATOR, reason="x",
                                       evidence=PAPER_EVIDENCE, now_ms=T0)
        assert paper["status"] == A.REFUSED and A.FAMILY_DOES_NOT_DECLARE_MARKET in paper["blockers"]
    cert = A.default_certification_lookup(db)("DAILY_TREND", "TRADFI_EXAMPLE_INDEX_FUTURES")
    assert cert["status"] != "CERTIFIED" and cert["reason"] == "MARKET_NOT_IN_FAMILY_DECLARATION"
    assert not A.default_adapter_available("example_futures_venue") and A.default_adapter_available("binance")


# ── LIVE ───────────────────────────────────────────────────────────────────

def test_live_is_refused_in_this_build_even_with_everything_else_met(db):
    policy = A.ActivationPolicy(policy_id="t", paper_min_days=1, demo_min_days=1)
    led = ledger(db, policy=policy)
    to_paper(led)
    assert led.request_transition(MARKET, FAMILY, A.DEMO, operator=OPERATOR, reason="x", evidence=DEMO_EVIDENCE,
                                  now_ms=T0 + 2 * DAY)["status"] == A.APPROVED
    live = led.request_transition(MARKET, FAMILY, A.LIVE, operator=OPERATOR, reason="x",
                                  evidence={"demo_record": "evidence/demo/1"}, now_ms=T0 + 400 * DAY)
    assert live["status"] == A.REFUSED and live["blockers"] == [A.TRADFI_LIVE_NOT_PERMITTED_IN_THIS_BUILD]
    assert led.state(MARKET, FAMILY).stage == A.DEMO


def test_no_account_is_eligible_for_live_and_the_policy_cannot_say_otherwise():
    with pytest.raises(A.ActivationError):
        A.ActivationPolicy(account_environments={A.PAPER: (), A.DEMO: ("DEMO",), A.LIVE: ("LIVE",)})


# ── revocation, risk, eligibility ──────────────────────────────────────────

def test_revocation_is_always_possible_and_restart_begins_at_paper(db):
    led = ledger(db)
    with pytest.raises(A.ActivationError):
        led.revoke(MARKET, FAMILY, operator=OPERATOR, reason="nothing active")
    to_paper(led)
    event = led.revoke(MARKET, FAMILY, operator=OPERATOR, reason="calendar data withdrawn", now_ms=T0 + DAY)
    assert event["to_stage"] == A.REVOKED and led.state(MARKET, FAMILY).stage == A.REVOKED
    assert led.risk_restrictions(MARKET, FAMILY) is None
    skip = led.request_transition(MARKET, FAMILY, A.DEMO, operator=OPERATOR, reason="x", evidence=DEMO_EVIDENCE,
                                  now_ms=T0 + 2 * DAY)
    assert "STAGE_SKIPPED:REVOKED->DEMO" in skip["blockers"]
    assert to_paper(led, now=T0 + 3 * DAY)["event"]["from_stage"] == A.REVOKED


def test_risk_restrictions_never_exceed_the_platform_caps(db):
    with pytest.raises(A.ActivationError):
        A.RiskRestriction(max_risk_per_trade_pct=Decimal("0.41"))
    with pytest.raises(A.ActivationError):
        A.RiskRestriction(max_entry_stop_pct=Decimal("15.01"))
    tighter = A.RiskRestriction(max_risk_per_trade_pct=Decimal("0.10"), max_entry_stop_pct=Decimal("8"))
    policy = A.ActivationPolicy(policy_id="t", risk_restrictions={A.PAPER: tighter, A.DEMO: tighter, A.LIVE: tighter})
    led = ledger(db, policy=policy)
    to_paper(led)
    assert led.risk_restrictions(MARKET, FAMILY) == tighter


def test_account_eligibility_follows_the_stage(db):
    policy = A.ActivationPolicy(policy_id="t", paper_min_days=1)
    led = ledger(db, policy=policy)
    demo_account = {"broker_id": "fixture_futures", "environment": "DEMO"}
    assert led.account_eligible(MARKET, FAMILY, demo_account) == (False, ["ACTIVATION_STAGE_NONE"])
    to_paper(led)
    assert led.account_eligible(MARKET, FAMILY, demo_account) == (False, ["NO_ACCOUNT_ELIGIBLE_AT_PAPER"])
    led.request_transition(MARKET, FAMILY, A.DEMO, operator=OPERATOR, reason="x", evidence=DEMO_EVIDENCE,
                           now_ms=T0 + 2 * DAY)
    assert led.account_eligible(MARKET, FAMILY, demo_account) == (True, [])
    ok, reasons = led.account_eligible(MARKET, FAMILY, {"broker_id": "fixture_futures", "environment": "LIVE"})
    assert not ok and reasons == ["ACCOUNT_ENVIRONMENT_LIVE_NOT_ELIGIBLE_AT_DEMO"]
    ok, reasons = led.account_eligible(MARKET, FAMILY, {"broker_id": "binance", "environment": "DEMO"})
    assert not ok and reasons == ["ACCOUNT_VENUE_MISMATCH"]
    assert led.highest_stage(MARKET) == A.DEMO


# ── the record ─────────────────────────────────────────────────────────────

def test_the_ledger_is_append_only_and_hash_chained(db):
    led = ledger(db)
    to_paper(led)
    led.revoke(MARKET, FAMILY, operator=OPERATOR, reason="x", now_ms=T0 + 1)
    assert led.verify_chain() == {"ok": True, "records": 2, "head_hash": led.history()[-1]["event_hash"]}
    import sqlite3

    with pytest.raises(sqlite3.DatabaseError):
        with db.connect() as c:
            c.execute(f"UPDATE {ACTIVATION_EVENT_TABLE} SET to_stage='LIVE' WHERE seq=1")
    with pytest.raises(sqlite3.DatabaseError):
        with db.connect() as c:
            c.execute(f"DELETE FROM {ACTIVATION_EVENT_TABLE}")
    assert led.verify_chain()["ok"] is True


def test_a_tampered_record_breaks_the_chain(db):
    led = ledger(db)
    to_paper(led)
    led.revoke(MARKET, FAMILY, operator=OPERATOR, reason="x", now_ms=T0 + 1)
    with db.connect() as c:
        c.execute("DROP TRIGGER trg_market_activation_events_no_update")
        c.execute(f"UPDATE {ACTIVATION_EVENT_TABLE} SET actor_ref='someone-else' WHERE seq=1")
    assert led.verify_chain() == {"ok": False, "records": 1, "broken_at_seq": 1}


def test_the_schema_is_created_by_migrate_and_is_idempotent(db):
    from shared_lib.persistence.markets_schema import ensure_markets_schema
    from shared_lib.persistence.migrations import migrate

    ensure_markets_schema(db)
    migrate(db)
    with db.connect() as c:
        assert c.execute("SELECT name FROM sqlite_master WHERE name=?", (ACTIVATION_EVENT_TABLE,)).fetchone()
