"""Pre-Section-22 final closure: submit-unknown ownership, canonical broker
environment, ONE canonical economics path, upstream OOD lineage, CATI
safety. REAL components throughout (see _exec / _position / _plan)."""
from __future__ import annotations

import dataclasses
import inspect
import sqlite3
import time

import pytest
from _exec import Harness, _entry, _order
from _plan import build_kwargs, venue_evaluated
from test_section17_18_shadow_flow import _Client, _Ctx, _Runner, _trend_rows

from app.execution.position_slots import occupied_slots
from app.risk.capital_ledger import ACCOUNT_RESERVATIONS
from app.runner.market_snapshot import MarketSnapshot
from app.trading_intelligence.contracts.portfolio_intel import ACCOUNT_RESERVATION_CONFLICT
from app.trading_intelligence.contracts.ranking import SymbolEvalKind
from app.trading_intelligence.contracts.system_health import BrokerHealthContext
from app.trading_intelligence.contracts.trade_plan import SubmissionValidity, TradePlanBuildStatus
from app.trading_intelligence.controller.cati_controller import CATIController
from app.trading_intelligence.economics.canonical import (
    CANONICAL_VENUE, REFERENCE_DIAGNOSTIC, canonical_economics,
)
from app.trading_intelligence.evidence.stores import DecisionEvidenceStore
from app.trading_intelligence.execution.boundary import BoundaryStatus as B, SUBMIT_OUTCOME_UNRESOLVED
from app.trading_intelligence.execution.config import CATIExecutionConfig
from app.trading_intelligence.integration import cycle_shadow, shadow_hook
from app.trading_intelligence.integration.context_adapters import (
    broker_health_from_runner, canonical_broker_identity,
)
from app.trading_intelligence.integration.errors import clear_component_errors, recent_component_errors
from app.trading_intelligence.integration.venue_context import venue_context_from_runner
from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
from app.trading_intelligence.research.export import export_research_rows
from app.trading_intelligence.trade_plan.builder import TradePlanBuilder
from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
from app.trading_intelligence.trade_plan.validation import MarketReference, validate_trade_plan_for_submission

UNKNOWN_KW = dict(entry=_entry(), order_error=RuntimeError("timeout"))


def _unknown(tmp_path):
    h = Harness(tmp_path, **UNKNOWN_KW)
    TradePlanEvidenceStore(h.db).append(h.plan)
    b = h.boundary()
    res = h.run(b)
    assert res.status == B.SUBMIT_UNKNOWN
    return h, b


# ============================== SUBMIT_UNKNOWN OWNERSHIP ==============================
def test_submit_unknown_enters_resolution_pending(tmp_path):
    h, _b = _unknown(tmp_path)
    row = h.reservations.pending_resolutions(h.plan.broker_account_id)[0]
    assert row["status"] == "RESOLUTION_PENDING" and row["trade_plan_id"] == h.plan.trade_plan_id
    assert row["execution_attempt_id"] and row["pending_since"] and row["resolution_deadline"] > row["pending_since"]


def test_pending_cannot_expire_or_be_released_normally(tmp_path):
    h, _b = _unknown(tmp_path)
    far = h.now + 10 * 86_400_000
    rid = h.plan.portfolio_reservation_id
    h.reservations.expire_stale(h.plan.broker_account_id, far)
    h.reservations.cleanup_expired(far)
    assert not h.reservations.release(rid, far) and not h.reservations.consume(rid, far)
    assert h.reservation_status() == "RESOLUTION_PENDING"


def test_duplicate_submission_blocked_while_pending(tmp_path):
    h, b = _unknown(tmp_path)
    assert h.run(b).status == B.DUPLICATE_PLAN
    assert len(h.seen["orders"]) == 1


def test_restart_preserves_pending_and_recovery_resolves_from_broker(tmp_path):
    h, _b = _unknown(tmp_path)
    # "restart": brand-new store + boundary over the same persisted database
    assert CATIReservationStore(h.db).pending_resolutions()[0]["status"] == "RESOLUTION_PENDING"
    fresh = h.boundary()
    h.client.get_order.side_effect = lambda s, o: _order("FILLED", "6.0", "100.0")
    h.client.get_position_info.return_value = {"positionAmt": "6.0", "entryPrice": "100.0"}
    out = fresh.recover_pending(now_ms=h.now + 60_000)
    assert [r.status for r in out] == [B.RECONCILED]
    assert h.reservation_status() == "CONSUMED" and len(h.seen["orders"]) == 1  # never re-submitted


def test_broker_confirms_no_entry_releases(tmp_path):
    h, b = _unknown(tmp_path)
    h.client.get_order.side_effect = lambda s, o: _order("CANCELED", "0")
    assert b.reconcile_submit_unknown(h.plan, now_ms=h.now + 1).status == B.RECONCILED
    assert h.reservation_status() == "RELEASED"


def test_reconciliation_error_stays_pending_fail_closed(tmp_path):
    clear_component_errors()
    h, b = _unknown(tmp_path)
    b.adapter.query_order = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("broker 503"))
    res = b.reconcile_submit_unknown(h.plan, now_ms=h.now + 1)
    assert res.status == B.STILL_UNKNOWN and "RECONCILIATION_ERROR" in res.reason_codes
    assert h.reservation_status() == "RESOLUTION_PENDING"
    assert any(r.component == "boundary.reconcile_submit_unknown" for r in recent_component_errors())


def test_unresolved_past_deadline_is_escalated_not_released(tmp_path):
    clear_component_errors()
    h, _b = _unknown(tmp_path)
    fresh = h.boundary()
    out = fresh.recover_pending(now_ms=h.now + 3_600_000)  # broker still cannot answer
    assert SUBMIT_OUTCOME_UNRESOLVED in out[0].reason_codes
    row = h.reservations.pending_resolutions()[0]
    assert row["status"] == "RESOLUTION_PENDING" and row["resolution_note"] == SUBMIT_OUTCOME_UNRESOLVED
    assert any(SUBMIT_OUTCOME_UNRESOLVED in r.message for r in recent_component_errors())


def test_concurrent_cycle_cannot_steal_and_capacity_counts_pending(tmp_path):
    h, _b = _unknown(tmp_path)
    acct, far = h.plan.broker_account_id, h.now + 10 * 86_400_000
    active = h.reservations.active_reservations(acct, far)
    assert [r.reservation_id for r in active] == [h.plan.portfolio_reservation_id]
    key = h.plan.instrument_key
    other = h.reservations.reserve(broker_account_id=acct, bot_instance_id="botB", cycle_id="c9",
                                   selected=[("cand_x", key.canonical_symbol, key.venue, key.venue_symbol, "LONG")],
                                   now_ms=far, ttl_seconds=60)
    # the pending reservation keeps the instrument: a second cycle never reserves it
    assert not other.reserved and other.conflict_reason in (ACCOUNT_RESERVATION_CONFLICT, "DUPLICATE_EXPOSURE")
    assert h.reservation_status() == "RESOLUTION_PENDING"


def test_repeat_reserve_never_deletes_pending_ownership(tmp_path):
    h, _b = _unknown(tmp_path)
    with h.db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM cati_portfolio_reservations").fetchone())
    import json

    selected = [(cid, *inst) for cid, inst in zip(json.loads(row["selected_candidate_ids"]),
                                                  json.loads(row["selected_instruments"]))]
    again = h.reservations.reserve(broker_account_id=row["broker_account_id"], bot_instance_id=row["bot_instance_id"],
                                   cycle_id=row["cycle_id"], selected=selected, now_ms=h.now + 1, ttl_seconds=60)
    assert not again.reserved and h.reservation_status() == "RESOLUTION_PENDING"


def test_production_slot_and_margin_remain_separate_authorities(tmp_path):
    h, _b = _unknown(tmp_path)
    assert "BTCUSDT" in occupied_slots(h.db, h.plan.bot_instance_id)["pending"]  # executor's slot authority
    assert ACCOUNT_RESERVATIONS.pending_margin(h.executor._broker_account_id) > 0  # executor's margin authority
    with h.db.connect() as conn:  # neither authority reads or writes the CATI reservation table
        entry = conn.execute("SELECT submit_state FROM pending_entries WHERE bot_id=?",
                             (h.plan.bot_instance_id,)).fetchone()
    assert entry["submit_state"] == "SUBMIT_UNKNOWN"


def test_old_four_state_reservation_table_is_upgraded_in_place(tmp_path):
    from shared_lib.persistence.cati_schema import ensure_cati_schema_on_connection

    conn = sqlite3.connect(str(tmp_path / "old.db"))
    conn.execute("""CREATE TABLE cati_portfolio_reservations (reservation_id TEXT PRIMARY KEY,
        broker_account_id TEXT NOT NULL, bot_instance_id TEXT NOT NULL, cycle_id TEXT NOT NULL,
        selected_candidate_ids TEXT NOT NULL, selected_instruments TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('RESERVED', 'CONSUMED', 'RELEASED', 'EXPIRED')),
        mode TEXT NOT NULL DEFAULT 'SHADOW', created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL, reservation_version TEXT NOT NULL)""")
    conn.execute("INSERT INTO cati_portfolio_reservations VALUES ('r1','a','b','c','[]','[]','RESERVED','SHADOW',1,2,1,'2.0.0')")
    ensure_cati_schema_on_connection(conn)
    ensure_cati_schema_on_connection(conn)  # idempotent
    assert conn.execute("SELECT status, expires_at FROM cati_portfolio_reservations").fetchall() == [("RESERVED", 2)]
    conn.execute("UPDATE cati_portfolio_reservations SET status='RESOLUTION_PENDING'")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE cati_portfolio_reservations SET status='BOGUS'")


# ============================== CANONICAL BROKER ENVIRONMENT ==============================
class _Live(_Ctx):
    broker_account_id = "acct_real"
    broker_environment = "live"   # broker_accounts.environment of a REAL account
    execution_mode = "paper"      # how the BOT trades -- never the broker environment


class _Demo(_Ctx):
    broker_account_id = "acct_demo"
    broker_environment = "demo"
    execution_mode = "live"


def _runner(ctx_cls):
    r = _Runner()
    r.context = ctx_cls()
    return r


def test_environment_comes_from_the_broker_account_not_execution_mode():
    assert canonical_broker_identity(_Demo()) == ("BINANCE_USDM", "DEMO")
    assert canonical_broker_identity(_Live())[1] == "REAL"
    h = broker_health_from_runner(_runner(_Demo), now_ms=1)
    assert (h.environment, h.venue, h.broker_account_id) == ("DEMO", "BINANCE_USDM", "acct_demo")


def test_two_accounts_same_venue_different_environments_are_separated():
    t = int(time.time() * 1000)
    demo, real = _runner(_Demo), _runner(_Live)
    hd, hr = broker_health_from_runner(demo, now_ms=t), broker_health_from_runner(real, now_ms=t)
    assert (hd.environment, hr.environment) == ("DEMO", "REAL") and hd.broker_account_id != hr.broker_account_id
    vd, vr = venue_context_from_runner(demo, "BTCUSDT", now_ms=t), venue_context_from_runner(real, "BTCUSDT", now_ms=t)
    assert (vd.environment, vr.environment) == ("DEMO", "REAL")
    from app.trading_intelligence.contracts.instrument import instrument_key_for

    key = instrument_key_for(venue="binance", venue_symbol="BTCUSDT")
    od, orr = vd.observe(key, t), vr.observe(key, t)
    assert od.environment == "DEMO" and orr.environment == "REAL" and od.observation_id != orr.observation_id


def test_prevalidation_rejects_health_from_another_environment(tmp_path):
    from _position import plan_setup

    _db, pipe, kw, plan = plan_setup(tmp_path)
    now = pipe["now_ms"]
    caps = kw["evaluated"].venue_observation.execution_capabilities
    res = CATIReservationStore(_db).get(plan.portfolio_reservation_id)
    ok = validate_trade_plan_for_submission(
        plan, now, MarketReference(plan.entry_reference, now - 10, 1.0),
        BrokerHealthContext(plan.broker_account_id, plan.venue, plan.environment, "HEALTHY", now, "t"), res, caps)
    bad = validate_trade_plan_for_submission(
        plan, now, MarketReference(plan.entry_reference, now - 10, 1.0),
        BrokerHealthContext(plan.broker_account_id, plan.venue, "REAL", "HEALTHY", now, "t"), res, caps)
    assert ok.valid and plan.environment == "DEMO"
    assert bad.status == SubmissionValidity.BROKER_DEGRADED.value and "BROKER_HEALTH_WRONG_ENVIRONMENT" in bad.reason_codes


# ============================== ONE CANONICAL ECONOMICS PATH ==============================
def _snapshot():
    start = int(time.time() * 1000) // 900_000 * 900_000 - 140 * 900_000
    return MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=_trend_rows(start), source="Test")


def test_whole_universe_and_replay_paths_share_the_canonical_contracts():
    runner = _runner(_Demo)
    vctx = venue_context_from_runner(runner, "BTCUSDT")
    tenant = dict(user_id="u1", broker_account_id="acct_demo", bot_instance_id="botA", run_id="r1", cycle_id="c1")
    ev = CATIController().evaluate_symbol(snapshot=_snapshot(), venue="binance", source="Test", venue_context=vctx,
                                          require_venue_economics=True, **tenant)
    assert ev.kind == SymbolEvalKind.EVALUATED.value and ev.opportunities
    for o in ev.opportunities:
        assert o.economics_basis == CANONICAL_VENUE
        # a certification / replay harness replaying the SAME recorded observation reaches identical economics
        cost, opp = canonical_economics(o.candidate, o.market_state, o.forecast, o.venue_observation,
                                        venue_policy=vctx.cost_policy, reference_notional=vctx.reference_notional,
                                        **tenant)
        assert cost.cost_estimate_id == o.cost_estimate.cost_estimate_id and cost == o.cost_estimate
        assert opp.economic_opportunity_id == o.opportunity.economic_opportunity_id and opp == o.opportunity
        if o.forecast.is_usable:
            assert opp.ev_net_r == pytest.approx(opp.ev_gross_r - cost.total_cost_R)  # costs subtracted once
        else:  # no usable library: the fail-closed unavailable opportunity is never admitted
            assert opp.admission_status == "INSUFFICIENT_EVIDENCE" and opp.ev_net_r == 0.0


def test_certifiable_path_never_falls_back_to_reference_costs():
    ev = CATIController().evaluate_symbol(snapshot=_snapshot(), venue="binance", source="Test",
                                          require_venue_economics=True)
    assert ev.kind == SymbolEvalKind.CATI_COMPONENT_ERROR.value and ev.error == "VENUE_ECONOMICS_REQUIRED"
    assert "require_venue_economics=True" in inspect.getsource(cycle_shadow.record_symbol)


def test_reference_diagnostic_economics_cannot_become_a_plan(tmp_path):
    from _plan import fresh_db, run_pipeline

    db = fresh_db(tmp_path)
    ev = venue_evaluated("BTCUSDT", seed=999)
    p = run_pipeline(db, [ev, venue_evaluated("ETHUSDT", seed=998)])
    kw = build_kwargs(p)
    ref = dataclasses.replace(kw["evaluated"], venue_observation=None)
    assert ref.economics_basis == REFERENCE_DIAGNOSTIC and kw["evaluated"].economics_basis == CANONICAL_VENUE
    res = TradePlanBuilder().build_from_evaluated(**{**kw, "evaluated": ref})
    assert res.status == TradePlanBuildStatus.INVALID_INPUT.value and "VENUE_OBSERVATION_MISSING" in res.reason_codes


def test_per_symbol_hook_is_labelled_diagnostic_only(monkeypatch, caplog):
    import logging

    monkeypatch.setenv(shadow_hook.FULL_PIPELINE_ENV_FLAG, "1")
    shadow_hook._controller = None
    with caplog.at_level(logging.INFO):
        shadow_hook.run_full_shadow_pipeline(_snapshot(), venue="binance", source="Test", symbol="BTCUSDT")
    lines = [r.message for r in caplog.records if "CATI_ECONOMIC" in r.message]
    assert lines and all(m.startswith("[CATI_ECONOMIC_DIAGNOSTIC] economics_basis=REFERENCE_DIAGNOSTIC "
                                      "certification_parity=false") for m in lines)
    assert shadow_hook.ECONOMICS_BASIS == REFERENCE_DIAGNOSTIC


# ============================== UPSTREAM OOD LINEAGE ==============================
def test_research_export_carries_the_exact_upstream_ood(tmp_path, monkeypatch):
    h = Harness(tmp_path)
    ev = h.kw["evaluated"]
    TradePlanEvidenceStore(h.db).append(h.plan)
    DecisionEvidenceStore(h.db).append(ev, broker_account_id=h.plan.broker_account_id, user_id=h.plan.user_id,
                                       bot_instance_id=h.plan.bot_instance_id)
    # the export must not recompute OOD: any recomputation attempt would explode here
    import app.trading_intelligence.forecast.ood as ood_mod

    monkeypatch.setattr(ood_mod, "assess_distribution_shift", lambda **_k: (_ for _ in ()).throw(AssertionError()))
    f = export_research_rows(h.db, h.plan.broker_account_id)[0]["forecast"]
    shift = ev.forecast.distribution_shift_assessment
    assert f["ood_evidence_status"] == "UPSTREAM_EVIDENCE"
    assert f["ood_score"] == shift.ood_score == ev.opportunity.ood_score == f["admission_ood_score"]
    assert f["ood_reason_codes"] == list(shift.reason_codes) and f["ood_severity"] == shift.severity
    assert f["forecast_uncertainty"] == ev.forecast.forecast_uncertainty
    veto_ood = next(c for c in ev.veto.checks if c.check == "distribution_shift")
    assert next(c for c in f["veto_ood_checks"] if c["check"] == "distribution_shift")["observed_value"] == \
        veto_ood.observed_value == shift.ood_score
    assert f["forecast_id"] == h.plan.forecast_id


def test_missing_upstream_evidence_is_explicit_never_zero(tmp_path):
    h = Harness(tmp_path)
    TradePlanEvidenceStore(h.db).append(h.plan)
    f = export_research_rows(h.db, h.plan.broker_account_id)[0]["forecast"]
    assert f["ood_score"] is None and f["ood_evidence_status"] == "UPSTREAM_EVIDENCE_UNAVAILABLE"


def test_decision_evidence_is_tenant_scoped_and_append_only(tmp_path):
    h = Harness(tmp_path)
    store = DecisionEvidenceStore(h.db)
    store.append(h.kw["evaluated"], broker_account_id=h.plan.broker_account_id)
    assert store.append(h.kw["evaluated"], broker_account_id=h.plan.broker_account_id) is False  # idempotent
    assert store.for_opportunity("acct_other", h.plan.economic_opportunity_id) is None
    with h.db.connect() as conn, pytest.raises(sqlite3.DatabaseError, match="append-only"):
        conn.execute("UPDATE cati_decision_evidence SET schema_version='x'")


# ============================== CATI SAFETY ==============================
@pytest.mark.parametrize("outcome", ["WATCH", "REJECT"])
def test_watch_and_reject_cannot_plan(tmp_path, outcome):
    h = Harness(tmp_path)
    kw = build_kwargs(h.pipe)
    ev = kw["evaluated"]
    blocked = dataclasses.replace(ev, veto=dataclasses.replace(ev.veto, outcome=outcome))
    res = TradePlanBuilder().build_from_evaluated(**{**kw, "evaluated": blocked})
    assert res.plan is None and res.status == TradePlanBuildStatus.VETO_NOT_APPROVED.value


def test_active_execution_and_exit_routing_default_off(monkeypatch):
    for name in ("CATI_ACTIVE_EXECUTION_ENABLED", "CATI_EXIT_INTENT_ROUTING_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    # AUTO: unset switches are not "off", they defer to governance (M0 here -> no authority)
    cfg = CATIExecutionConfig.from_env()
    assert cfg.active_execution_enabled is True and cfg.exit_intent_routing_enabled is True
    from app.activation import cati as act

    assert not act.exit_intent_routing(None).active and not act.active_execution(None).active
    for name in ("CATI_ACTIVE_EXECUTION_ENABLED", "CATI_EXIT_INTENT_ROUTING_ENABLED"):
        monkeypatch.setenv(name, "0")
    cfg = CATIExecutionConfig.from_env()
    assert cfg.active_execution_enabled is False and cfg.exit_intent_routing_enabled is False
    assert _Client  # the shadow-flow client stand-in (order paths explode) is the one used above


# ============================== CATI PATH: CANONICAL SIDE ==============================
def test_cati_trade_plan_reaches_policy_engine_with_canonical_side(tmp_path, monkeypatch):
    from app.trading_intelligence.execution.boundary import AccountState

    h = Harness(tmp_path)
    seen = []
    real = h.orch.policy_engine.evaluate
    monkeypatch.setattr(h.orch.policy_engine, "evaluate", lambda ctx: (seen.append(ctx.signal), real(ctx))[1])
    h.run(account=AccountState(50_000.0, 0.0, 50_000.0, 0))
    assert seen == [{"LONG": "BUY", "SHORT": "SELL"}[h.plan.side]]


def test_cati_trade_plan_is_subject_to_the_max_open_position_gate(tmp_path):
    from app.trading_intelligence.execution.boundary import AccountState

    h = Harness(tmp_path)
    res = h.run(account=AccountState(50_000.0, 0.0, 50_000.0, 5), max_open_positions=5)
    assert res.status == B.RISK_REJECTED and "MAX_POSITIONS_REACHED" in res.reason_codes and not h.seen["orders"]
    ok = Harness(tmp_path)  # a fresh plan: 0 of 5 open reaches the broker
    assert ok.run(account=AccountState(50_000.0, 0.0, 50_000.0, 0), max_open_positions=5).status == B.EXECUTED


# ============================== SECURITY: RESOLUTION EVIDENCE ==============================
def test_broker_text_in_resolution_note_is_redacted(tmp_path):
    h, _b = _unknown(tmp_path)
    rid = h.plan.portfolio_reservation_id
    leak = "NEW signature=deadbeefcafe apiKey=AKIAXXXXXXXX Bearer abc.def.ghi"
    assert h.reservations.note_unresolved(rid, h.now + 1, leak)
    note = h.reservations.pending_resolutions()[0]["resolution_note"]
    assert "deadbeefcafe" not in note and "AKIAXXXXXXXX" not in note and "abc.def.ghi" not in note
    assert "[REDACTED]" in note and note.startswith("NEW")


# ============================== LINEAGE: CYCLE WRITES DECISION EVIDENCE ==============================
def test_cycle_stage_persists_decision_evidence_for_each_plan(tmp_path):
    from _plan import fresh_db, run_pipeline

    db = fresh_db(tmp_path)
    p = run_pipeline(db, [venue_evaluated("BTCUSDT")])
    [res] = cycle_shadow.trade_plan_stage(db, p["result"], p["outcome"], p["evaluated"], p["now_ms"])
    plan = res.plan
    up = DecisionEvidenceStore(db).for_opportunity(plan.broker_account_id, plan.economic_opportunity_id)
    assert up is not None and up["forecast_id"] == plan.forecast_id and up["veto_decision_id"] == plan.veto_decision_id


def test_decision_evidence_write_failure_is_recorded_not_silent(tmp_path, monkeypatch):
    from _plan import fresh_db, run_pipeline

    clear_component_errors()
    db = fresh_db(tmp_path)
    p = run_pipeline(db, [venue_evaluated("BTCUSDT")])
    monkeypatch.setattr(DecisionEvidenceStore, "append", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("disk")))
    [res] = cycle_shadow.trade_plan_stage(db, p["result"], p["outcome"], p["evaluated"], p["now_ms"])
    assert TradePlanEvidenceStore(db).load_plan(res.plan.broker_account_id, res.plan.trade_plan_id) is not None  # the plan itself is kept
    assert any(r.component == "cycle_shadow.decision_evidence" for r in recent_component_errors())
    [row] = export_research_rows(db, res.plan.broker_account_id)
    assert row["forecast"]["ood_evidence_status"] == "UPSTREAM_EVIDENCE_UNAVAILABLE"
