"""Experimental DEMO authorization: an uncertified family on ONE verified DEMO account, bounded and recorded.

What must hold (owner's activation brief, section 3): DEMO only and verified; never LIVE; never certification,
promotion or a holdout; every gate other than the family's certification and stage still applies; expiring and
revocable; hash-chained; every claimed order labelled.
"""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.trading_intelligence.governance import experimental_demo as X
from app.trading_intelligence.integration.residual_prospective import FAMILY, REGISTRY_HASH
from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
from app.trading_intelligence.pipeline.family_registry import FamilyRegistry
from app.trading_intelligence.pipeline.provenance import FamilyExecutionGate
from shared_lib.persistence.db import DB

NOW = 1_800_000_000_000
HOUR = 3_600_000
DEMO_URL = "https://demo-fapi.binance.com"
OP = OperatorAuthorization(operator_id="admin-1", channel="admin_api:test", permission="admin-emergency")
REGISTER = Path(__file__).resolve().parents[4] / "docs" / "research" / "registry" / "research_register.jsonl"


@pytest.fixture
def db(tmp_path, monkeypatch):
    database = DB(str(tmp_path / "xdemo.db"))
    with database.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS broker_accounts (id TEXT PRIMARY KEY, user_id TEXT, broker_id TEXT, "
                  "environment TEXT, status TEXT)")
        c.executemany("INSERT INTO broker_accounts (id,user_id,broker_id,environment,status,market_type,label,"
                      "created_at,updated_at) VALUES (?,?,?,?,?,'crypto','test','2026-11-01','2026-11-01')", [
            ("brk_demo", "u1", "binance", "demo", "connected"),
            ("brk_live", "u1", "binance", "live", "connected"),
            ("brk_other", "u2", "binance", "demo", "connected"),
        ])
    from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
    ensure_cati_platform_schema(database)
    FamilyRegistry(database).register_all(actor_ref="system:test", now_ms=NOW - HOUR)

    def resolve(account_id, user_id, _db):
        env = "live" if account_id == "brk_live" else "demo"
        from shared_lib.broker.environment import normalize_environment
        return SimpleNamespace(environment=normalize_environment(env),
                               base_url="https://fapi.binance.com" if env == "live" else DEMO_URL)

    monkeypatch.setattr("shared_lib.broker.resolver.resolve_broker_auth", resolve)
    return database


def grant(db, **over):
    args = dict(account_id="brk_demo", family_id=FAMILY, environment="DEMO", acknowledged_research_status="FAILED",
                approval_ref="owner brief 2026-11-01", reason="watch the strategy on demo", duration_hours=24,
                max_entry_notional_usdt=200, now_ms=NOW)
    args.update(over)
    return X.ExperimentalDemoLedger(db).authorize(OP, **args)


def plan(account="brk_demo", user="u1", environment="DEMO", mode=None, family=FAMILY):
    return SimpleNamespace(setup_family=family, source_candidate_id="d", decision_time=NOW, mode=mode or environment,
                           user_id=user, broker_account_id=account, environment=environment, venue="binance_usdm",
                           plan_expiry_time=NOW + HOUR, versions=(("residual_registry", REGISTRY_HASH),),
                           setup_version=REGISTRY_HASH, trade_plan_id="tp_1")


def test_the_residual_family_is_refused_without_a_grant(db):
    entry, why, experimental = FamilyExecutionGate(db).family_authorization_scope(plan(), NOW)
    assert why == "FAMILY_FAILED_RESEARCH" and experimental is None


def test_a_grant_waives_only_certification_and_stage_for_its_demo_account(db):
    g = grant(db)
    gate = FamilyExecutionGate(db)
    entry, why, experimental = gate.family_authorization_scope(plan(), NOW + 1)
    assert why is None and experimental.authorization_id == g.authorization_id
    # every other family check still runs: an unsupported market is refused under the grant
    wrong = plan(); wrong.venue = "bybit_linear"
    assert gate.family_authorization_scope(wrong, NOW + 1)[1] == "FAMILY_MARKET_NOT_SUPPORTED"
    # mode / environment mismatch is refused, not waived
    assert gate.family_authorization_scope(plan(mode="LIVE"), NOW + 1)[1] in (
        "FAMILY_ENVIRONMENT_NOT_AUTHORIZED", "FAMILY_FAILED_RESEARCH")
    # another user's account, another account, a LIVE plan: the grant does not apply
    assert gate.family_authorization_scope(plan(account="brk_other", user="u2"), NOW + 1)[1] == "FAMILY_FAILED_RESEARCH"
    assert gate.family_authorization_scope(plan(account="brk_live", environment="LIVE"), NOW + 1)[1] == \
        "FAMILY_FAILED_RESEARCH"
    # the family's kill switch still stops it
    FamilyRegistry(db).governance.set_kill_switch(FAMILY, True, operator=OP, reason="test kill switch")
    assert gate.family_authorization_scope(plan(), NOW + 2)[1] == "FAMILY_KILL_SWITCH"


def test_authorize_reports_the_grant_and_still_requires_decision_and_venue_certification(db, monkeypatch):
    g = grant(db)
    certified = FamilyExecutionGate(db, venue_certified=lambda v, e, n: (True, ""))
    monkeypatch.setattr(certified, "decision_verification", lambda entry, plan, now: None)
    assert certified.authorize(plan(), now_ms=NOW + 1) == (True, f"{X.AUTHORIZED_REASON}:{g.authorization_id}")
    # the immutable decision is still verified under the grant
    monkeypatch.setattr(certified, "decision_verification", lambda entry, plan, now: "CURRENT_CATI_DECISION_CHANGED")
    assert certified.authorize(plan(), now_ms=NOW + 1) == (False, "CURRENT_CATI_DECISION_CHANGED")
    # and so is venue (adapter) certification
    uncertified = FamilyExecutionGate(db, venue_certified=lambda v, e, n: (False, "NO_CERTIFICATION_EVIDENCE"))
    monkeypatch.setattr(uncertified, "decision_verification", lambda entry, plan, now: None)
    ok, why = uncertified.authorize(plan(), now_ms=NOW + 1)
    assert not ok and why.startswith("VENUE_ADAPTER_NOT_CERTIFIED")


def test_the_grant_is_demo_only_and_cannot_be_turned_into_live_authority(db):
    with pytest.raises(X.ExperimentalDemoError) as e:
        grant(db, environment="LIVE")
    assert e.value.code == X.LIVE_NOT_PERMITTED
    with pytest.raises(X.ExperimentalDemoError) as e:
        grant(db, account_id="brk_live")
    assert e.value.code == X.ACCOUNT_NOT_DEMO
    g = grant(db)
    # the account is later switched to LIVE: the grant stops applying at once
    with db.connect() as c:
        c.execute("UPDATE broker_accounts SET environment='live' WHERE id='brk_demo'")
    assert X.plan_scope(db, plan(), FamilyRegistry(db).entry(FAMILY), NOW + 1) is None
    assert X.plan_scope(db, plan(environment="LIVE"), FamilyRegistry(db).entry(FAMILY), NOW + 1) is None
    assert g.to_plain(NOW)["grants_live_authority"] is False


def test_credentials_that_do_not_resolve_to_the_demo_host_are_refused(db, monkeypatch):
    from shared_lib.broker.environment import normalize_environment
    monkeypatch.setattr("shared_lib.broker.resolver.resolve_broker_auth",
                        lambda a, u, d: SimpleNamespace(environment=normalize_environment("demo"),
                                                        base_url="https://fapi.binance.com"))
    with pytest.raises(X.ExperimentalDemoError) as e:
        grant(db)
    assert e.value.code == X.CREDENTIALS_NOT_DEMO


@pytest.mark.parametrize("over,code", [
    ({"acknowledged_research_status": "NOT_CERTIFIED"}, X.RESEARCH_STATUS_NOT_ACKNOWLEDGED),
    ({"approval_ref": ""}, X.APPROVAL_REF_REQUIRED),
    ({"reason": "x"}, X.REASON_REQUIRED),
    ({"duration_hours": 0}, X.DURATION_INVALID),
    ({"duration_hours": X.MAX_DURATION_HOURS + 1}, X.DURATION_INVALID),
    ({"max_entry_notional_usdt": 0}, X.NOTIONAL_INVALID),
    ({"max_entry_notional_usdt": float(X.MAX_ENTRY_NOTIONAL_USDT) + 1}, X.NOTIONAL_INVALID),
    ({"family_id": "NOT_A_FAMILY"}, X.FAMILY_UNKNOWN),
    ({"account_id": "brk_missing"}, X.ACCOUNT_NOT_FOUND),
])
def test_grant_terms_are_validated(db, over, code):
    with pytest.raises(X.ExperimentalDemoError) as e:
        grant(db, **over)
    assert e.value.code == code


def test_only_an_authenticated_operator_grants_and_one_grant_per_scope(db):
    with pytest.raises(X.ExperimentalDemoError) as e:
        X.ExperimentalDemoLedger(db).authorize("admin-1", account_id="brk_demo", family_id=FAMILY,
                                               environment="DEMO", acknowledged_research_status="FAILED",
                                               approval_ref="ref", reason="reason", duration_hours=1,
                                               max_entry_notional_usdt=10)
    assert e.value.code == X.OPERATOR_REQUIRED
    grant(db)
    with pytest.raises(X.ExperimentalDemoError) as e:
        grant(db, now_ms=NOW + 5)
    assert e.value.code == X.ALREADY_ACTIVE


def test_grants_expire_on_their_own_and_revocation_is_immediate(db):
    g = grant(db, duration_hours=2)
    entry = FamilyRegistry(db).entry(FAMILY)
    assert X.plan_scope(db, plan(), entry, NOW + HOUR) is not None
    assert X.plan_scope(db, plan(), entry, NOW + 2 * HOUR) is None          # lapsed
    assert X.entry_refusal(db, g.authorization_id, 10, NOW + 2 * HOUR) == X.GRANT_LAPSED
    g2 = grant(db, now_ms=NOW + 3 * HOUR)
    X.ExperimentalDemoLedger(db).revoke(OP, g2.authorization_id, reason="stop the test", now_ms=NOW + 3 * HOUR + 1)
    assert X.plan_scope(db, plan(), entry, NOW + 3 * HOUR + 2) is None
    assert X.ExperimentalDemoLedger(db).grant(g2.authorization_id).state(NOW + 3 * HOUR + 2) == "REVOKED"
    with pytest.raises(X.ExperimentalDemoError) as e:
        X.ExperimentalDemoLedger(db).revoke(OP, g2.authorization_id, reason="again", now_ms=NOW + 3 * HOUR + 3)
    assert e.value.code == X.ALREADY_REVOKED


def test_the_notional_cap_refuses_and_never_resizes(db):
    g = grant(db, max_entry_notional_usdt=50)
    assert X.entry_refusal(db, g.authorization_id, 49.9, NOW + 1) is None
    assert X.entry_refusal(db, g.authorization_id, 50.01, NOW + 1).startswith(X.NOTIONAL_EXCEEDED)
    assert X.entry_refusal(db, g.authorization_id, 0, NOW + 1).startswith(X.NOTIONAL_EXCEEDED)


def test_ledger_is_append_only_hash_chained_and_a_broken_chain_authorizes_nothing(db):
    g = grant(db)
    X.ExperimentalDemoLedger(db).record_entry(g, trade_plan_id="tp_1", attempt_id="att_1", symbol="1INCHUSDT",
                                              side="LONG", notional=40.0, now_ms=NOW + 10)
    ledger = X.ExperimentalDemoLedger(db)
    assert ledger.verify()[0]
    assert X.experimental_plans(db, "brk_demo") == {"tp_1": g.authorization_id}
    assert ledger.grant(g.authorization_id).to_plain(NOW + 11)["entries_claimed"] == 1
    with db.connect() as c:
        with pytest.raises(Exception):
            c.execute(f"UPDATE {X.TABLE} SET payload='{{}}' WHERE seq=1")
    with db.connect() as c:
        with pytest.raises(Exception):
            c.execute(f"DELETE FROM {X.TABLE}")
    # tamper below the triggers: the chain breaks and nothing is authorized any more
    with db.connect() as c:
        c.execute(f"DROP TRIGGER trg_cati_xdemo_no_update")
        c.execute(f"UPDATE {X.TABLE} SET actor_ref='operator:someone-else' WHERE seq=1")
    assert not X.ExperimentalDemoLedger(db).verify()[0]
    assert X.plan_scope(db, plan(), FamilyRegistry(db).entry(FAMILY), NOW + 12) is None
    assert X.entry_refusal(db, g.authorization_id, 10, NOW + 12) == X.LEDGER_INTEGRITY_FAILED


def test_a_grant_is_never_certification_promotion_or_a_holdout(db):
    register_before = hashlib.sha256(REGISTER.read_bytes()).hexdigest()
    registry = FamilyRegistry(db)
    before = registry.entry(FAMILY)
    history_before = registry.governance.history(FAMILY)
    g = grant(db)
    X.ExperimentalDemoLedger(db).record_entry(g, trade_plan_id="tp_1", attempt_id="att_1", symbol="1INCHUSDT",
                                              side="LONG", notional=40.0, now_ms=NOW + 10)
    after = FamilyRegistry(db).entry(FAMILY)
    assert (after.certification_status, after.governance_stage, after.allowed_modes) == \
        (before.certification_status, before.governance_stage, before.allowed_modes) == \
        ("FAILED", "M0", before.allowed_modes)
    assert "DEMO" not in after.allowed_modes and "LIVE" not in after.allowed_modes
    assert FamilyRegistry(db).governance.history(FAMILY) == history_before
    prereq = FamilyRegistry(db).promotion_prerequisites(FAMILY)
    assert prereq.get("certification_status") == "FAILED"
    assert hashlib.sha256(REGISTER.read_bytes()).hexdigest() == register_before
    with db.connect() as c:
        holdouts = c.execute("SELECT name FROM sqlite_master WHERE name='cati_holdout_registry'").fetchone()
        if holdouts:
            assert c.execute("SELECT COUNT(*) FROM cati_holdout_registry").fetchone()[0] == 0
    import app.trading_intelligence.governance.experimental_demo as module
    source = Path(module.__file__).read_text(encoding="utf-8")
    for forbidden in ("cati_family_events", "research_register", "cati_holdout_registry", "approve_promotion",
                      "holdouts.reserve"):
        assert forbidden not in source.split('"""', 2)[2], forbidden


def test_every_reason_the_grant_can_produce_is_registered():
    from app.trading_intelligence.integration import reason_registry as R
    for code in (X.AUTHORIZED_REASON, X.NOTIONAL_EXCEEDED, X.GRANT_LAPSED, X.LEDGER_INTEGRITY_FAILED,
                 "EXPERIMENTAL_DEMO_AUDIT_UNAVAILABLE"):
        assert R.known(code), code
