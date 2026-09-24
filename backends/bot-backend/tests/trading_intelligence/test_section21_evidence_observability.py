"""Section 21 -- Evidence, observability and research data: the 21.24 matrix.

The chain below is REAL end to end: Section 12-18 pipeline -> TradePlan
evidence -> CATI boundary (real orchestrator hard risk + real executor over
a mock client) -> CATI-linked position -> Section 19 shadow intelligence."""
from __future__ import annotations

import ast
import dataclasses
import json
import logging
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from _exec import Harness, _entry, _hard_cap_context, _order
from _position import BAR, bars, clean_events, dims_for, good_state, healthy, policy, position, position_library, \
    regime_with, winners

from app.runner.market_snapshot import MarketSnapshot
from app.trading_intelligence.contracts.position import ExitDecision
from app.trading_intelligence.controller.cati_controller import CATIController
from app.trading_intelligence.evidence.lineage import CHAIN, reconstruct_from_exit_decision, reconstruct_from_trade_plan
from app.trading_intelligence.evidence.stores import (
    CATIEvidenceSchemaMissing, EvidenceConflict, ExecutionAttemptStore, ExitDecisionStore, PositionForecastStore,
    RiskDecisionStore,
)
from app.trading_intelligence.integration.errors import clear_component_errors, recent_component_errors
from app.trading_intelligence.observability.emitters import required_metric_families
from app.trading_intelligence.observability.logging import STAGES, log_stage, timed_stage
from app.trading_intelligence.observability.metrics import (
    ALLOWED_LABELS, MAX_SERIES_PER_METRIC, METRICS, OVERFLOW, MetricLabelError, MetricsRegistry,
)
from app.trading_intelligence.observability.sanitize import REDACTED, sanitize_payload
from app.trading_intelligence.position.service import PositionIntelligenceService, resolve_cati_plan
from app.trading_intelligence.research.export import export_research_rows
from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore

BACKEND = Path(__file__).resolve().parents[2]
CATI = BACKEND / "app" / "trading_intelligence"
NEW_MODULES = [CATI / "position", CATI / "execution", CATI / "evidence", CATI / "observability", CATI / "research"]


def _evaluate_position(h, res, *, closes=(101.0, 101.5, 102.0), mode="SHADOW"):
    acct = h.plan.broker_account_id
    plan = resolve_cati_plan(h.db, acct, res.attempt.position_id)
    assert plan is not None and plan.trade_plan_id == h.plan.trade_plan_id  # reloaded + hash-verified
    ev = h.kw["evaluated"]
    ms, regime = good_state(ev.market_state), regime_with(ev.regime)
    service = PositionIntelligenceService(library=position_library(dims_for(plan, ms, regime), winners(20)),
                                          policy=policy(), db=h.db, evaluation_mode=mode)
    t = plan.decision_time + len(closes) * BAR + 1_000
    return service.evaluate(
        plan=plan, position=position(plan, position_id=res.attempt.position_id, qty=6.0), candle_rows=bars(
            plan.decision_time, closes), current_time=t, current_price=closes[-1], market_state=ms, regime=regime,
        venue_observation=ev.venue_observation, event_context=clean_events(t), system_context=healthy(plan, t),
        runtime_session_id="rts_test")


@pytest.fixture
def chain(tmp_path):
    h = Harness(tmp_path)
    TradePlanEvidenceStore(h.db).append(h.plan)
    res = h.run()
    assert res.status == "EXECUTED", res
    out = _evaluate_position(h, res)
    return SimpleNamespace(h=h, res=res, out=out, acct=h.plan.broker_account_id)


# ============================== LINEAGE ==============================
def test_complete_lineage_chain(chain):
    rep = reconstruct_from_exit_decision(chain.h.db, chain.acct, chain.out.decision.exit_decision_id)
    assert rep.complete, (rep.missing, rep.tenant_violations, rep.chain())
    ids = rep.ids
    assert list(CHAIN)[0] == "runtime_session" and list(CHAIN)[-1] == "exit_decision"
    assert ids["runtime_session"] == "rts_test" and ids["bot_run"] == "r1" and ids["cycle"] == "c1"
    assert ids["trade_plan"] == chain.h.plan.trade_plan_id
    assert ids["execution_attempt"] == chain.res.attempt.execution_attempt_id
    assert ids["position"] == chain.res.attempt.position_id
    assert ids["position_forecast"] == chain.out.forecast.position_forecast_id
    assert reconstruct_from_trade_plan(chain.h.db, chain.acct, chain.h.plan.trade_plan_id).complete


def test_missing_parent_detected(chain):
    d = chain.out.decision
    orphan = ExitDecision.build(**{**{f: getattr(d, f) for f in ExitDecision.__dataclass_fields__
                                      if f not in ("exit_decision_id", "decision_hash")},
                                   "position_forecast_id": "pfc_missing_parent", "decision_time": d.decision_time + 1})
    ExitDecisionStore(chain.h.db).append(orphan)
    rep = reconstruct_from_exit_decision(chain.h.db, chain.acct, orphan.exit_decision_id)
    assert "position_forecast" in rep.missing and not rep.complete


def test_wrong_tenant_lineage_rejected(chain):
    stores = RiskDecisionStore(chain.h.db)
    own = stores.for_plan(chain.acct, chain.h.plan.trade_plan_id)[0]["payload"]
    from app.trading_intelligence.contracts.execution import RiskDecision

    foreign = RiskDecision.build(**{**{k: v for k, v in own.items() if k != "risk_decision_id"},
                                    "broker_account_id": "acct_OTHER", "reason_codes": tuple(own["reason_codes"]),
                                    "allocation_basis": tuple(map(tuple, own["allocation_basis"])),
                                    "policy_versions": tuple(map(tuple, own["policy_versions"]))})
    stores.append(foreign)
    rep = reconstruct_from_trade_plan(chain.h.db, chain.acct, chain.h.plan.trade_plan_id)
    assert rep.tenant_violations and not rep.complete
    other = reconstruct_from_exit_decision(chain.h.db, "acct_OTHER", chain.out.decision.exit_decision_id)
    assert other.tenant_violations and not other.ids.get("trade_plan")


# ============================== PERSISTENCE ==============================
_TABLES = ("cati_position_forecasts", "cati_exit_decisions", "cati_risk_decisions", "cati_execution_attempts",
           "cati_component_errors", "cati_trade_plans")


def test_analytical_rows_are_append_only(chain):
    from app.trading_intelligence.observability.logging import record_stage_error

    record_stage_error("test.component", "RISK", RuntimeError("boom"), db=chain.h.db)
    with chain.h.db.connect() as conn:
        for table in _TABLES:
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] >= 1, table
            with pytest.raises(sqlite3.DatabaseError, match="append-only"):
                conn.execute(f"UPDATE {table} SET schema_version='x'")
            with pytest.raises(sqlite3.DatabaseError, match="append-only"):
                conn.execute(f"DELETE FROM {table}")


def test_new_knowledge_is_a_new_row(chain):
    before = len(PositionForecastStore(chain.h.db).for_position(chain.acct, chain.res.attempt.position_id))
    _evaluate_position(chain.h, chain.res, closes=(101.0, 102.0, 103.0, 104.0))
    rows = PositionForecastStore(chain.h.db).for_position(chain.acct, chain.res.attempt.position_id)
    assert len(rows) == before + 1 and rows[0]["position_forecast_id"] != rows[-1]["position_forecast_id"]
    history = ExecutionAttemptStore(chain.h.db).history(chain.acct, chain.res.attempt.execution_attempt_id)
    assert [r["status"] for r in history] == ["PENDING_SUBMIT", "FILLED"]  # states are rows, never rewrites


def test_duplicate_deterministic_id_is_idempotent_and_conflicts_are_refused(chain):
    store = ExitDecisionStore(chain.h.db)
    assert store.append(chain.out.decision) is False
    tampered = dataclasses.replace(chain.out.decision, reason_codes=("TAMPERED",))
    with pytest.raises(EvidenceConflict):
        store.append(tampered)


def test_migration_idempotent_and_no_runtime_ddl(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    db = DB(str(tmp_path / "m.db"))
    migrate(db)
    with db.connect() as conn:
        first = sorted(tuple(r) for r in conn.execute("SELECT type, name FROM sqlite_master WHERE name LIKE '%cati%'"))
    migrate(db)
    with db.connect() as conn:
        second = sorted(tuple(r) for r in conn.execute("SELECT type, name FROM sqlite_master WHERE name LIKE '%cati%'"))
    assert first == second and ("table", "cati_exit_decisions") in first
    for pkg in NEW_MODULES:
        for path in pkg.glob("*.py"):
            assert "CREATE TABLE" not in path.read_text().upper(), path


def test_missing_schema_fails_closed(tmp_path):
    from shared_lib.persistence.db import DB

    with pytest.raises(CATIEvidenceSchemaMissing):
        PositionForecastStore(DB(str(tmp_path / "empty.db")))


# ============================== METRICS ==============================
def _controller_run():
    import math

    closes = [100 + 0.5 * i + 6 * math.sin((i + 3) / (16 / (2 * math.pi))) for i in range(140)]
    rows = []
    for i, c in enumerate(closes):
        o = closes[i - 1] if i else c
        t = 1_700_000_000_000 + i * 900_000
        rows.append([t, o, max(o, c) + 0.6, min(o, c) - 0.6, c, 1000, t + 899_999, 0, 0, 0, 0, 0])
    snap = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=rows, source="Test")
    return CATIController().evaluate_symbol(snapshot=snap, venue="binance", source="Test")


def test_every_required_metric_family_and_stage_latency_is_emitted(chain, tmp_path):
    _controller_run()
    Harness(tmp_path).run(adaptive_daily_risk=_hard_cap_context(125.0))  # a CATI-approved, hard-risk-rejected plan
    snap = METRICS.snapshot()
    names = set(snap["counters"]) | set(snap["summaries"])
    missing = [m for m in required_metric_families() if m not in names]
    assert not missing, missing
    stages = {s["labels"]["stage"] for s in snap["summaries"]["cati_stage_latency_ms"]}
    assert set(STAGES) <= stages, set(STAGES) - stages


def test_metric_labels_are_bounded_and_never_ids(chain):
    snap = json.dumps(METRICS.snapshot())
    assert set(METRICS.label_keys()) <= ALLOWED_LABELS
    for leaked in (chain.h.plan.trade_plan_id, chain.res.attempt.position_id, chain.res.attempt.execution_attempt_id,
                   chain.out.decision.exit_decision_id, chain.acct):
        assert leaked not in snap
    reg = MetricsRegistry()
    with pytest.raises(MetricLabelError):
        reg.inc("x", user_id="u1")
    with pytest.raises(MetricLabelError):
        reg.inc("x", setup_family=chain.h.plan.trade_plan_id)
    with pytest.raises(MetricLabelError):
        reg.inc("x", status="3f2b1c9a-1111-2222-3333-444455556666")
    for i in range(MAX_SERIES_PER_METRIC + 5):
        reg.inc("y", bucket=f"b{chr(65 + i % 26)}{chr(65 + (i // 26) % 26)}")
    assert any(dict(s["labels"]).get("bucket") == OVERFLOW for s in reg.snapshot()["counters"]["y"])


# ============================== LOGS / SECURITY ==============================
_SECRETS = {
    "authorization": "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJl",
    "bearer": "bearer abcDEF123token",
    "binance_key": "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
    "api_key_kv": "request failed apiKey=AKIAIOSFODNN7EXAMPLE",
    "secret_kv": "secret=hunter2hunter2",
    "password_kv": "password: s3cr3tP@ss",
    "private_key": "-----BEGIN PRIVATE KEY-----MIIEvQIBADANBgkqhkiG9w0BAQEFAASC-----END PRIVATE KEY-----",
    "token_kv": "token=ghp_AbCdEfGhIjKlMnOpQrStUvWxYz0123456789",
    "query_signature": "GET /fapi/v1/order?symbol=BTCUSDT&signature=9f8e7d6c5b4a&timestamp=1",
}


def test_sanitizer_redacts_injected_secrets_but_keeps_hashes():
    digest = "a" * 64
    payload = {"note": list(_SECRETS.values()), "api_key": "plain-looking-value", "Password": "x", "jwt": "y",
               "nested": {"secret_token": "z", "trade_plan_hash": digest, "trade_plan_id": "tplan_" + "b" * 24},
               "reason": _SECRETS["binance_key"]}
    clean = sanitize_payload(payload)
    blob = json.dumps(clean)
    for raw in ("eyJhbGciOiJIUzI1NiJ9", "abcDEF123token", _SECRETS["binance_key"], "AKIAIOSFODNN7EXAMPLE", "hunter2",
                "s3cr3tP@ss", "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC", "ghp_AbCdEf", "9f8e7d6c5b4a", "plain-looking-value"):
        assert raw not in blob, raw
    assert clean["api_key"] == clean["Password"] == clean["jwt"] == REDACTED
    assert clean["nested"]["trade_plan_hash"] == digest and clean["nested"]["trade_plan_id"].startswith("tplan_")


def test_structured_stage_logs_are_sanitized(caplog):
    with caplog.at_level(logging.INFO, logger="app.trading_intelligence.stage"):
        log_stage(component="boundary.risk", status="REJECTED", duration_ms=1.5, reason_codes=("DAILY_LOSS_LIMIT",),
                  runtime_session_id="rts_1", bot_run_id="run_1", cycle_id="c1", user_id="u1",
                  broker_account_id="acct1", bot_instance_id="botA",
                  extra={"api_key": "AKIAIOSFODNN7EXAMPLE", "note": _SECRETS["authorization"],
                         "headers": {"Authorization": "Bearer zzz"}})
    line = next(r.message for r in caplog.records if r.message.startswith("[CATI_STAGE]"))
    payload = json.loads(line.split(" ", 1)[1])
    for key in ("runtime_session_id", "bot_run_id", "cycle_id", "component", "status", "duration_ms", "reason_codes"):
        assert key in payload
    assert "AKIAIOSFODNN7EXAMPLE" not in line and "eyJhbGci" not in line and "Bearer zzz" not in line


def test_evidence_and_exports_never_contain_secrets(chain):
    from app.trading_intelligence.observability.logging import record_stage_error

    record_stage_error("security.probe", "EXECUTION", RuntimeError(" ".join(_SECRETS.values())), db=chain.h.db,
                       broker_account_id=chain.acct)
    rows = export_research_rows(chain.h.db, chain.acct)
    with chain.h.db.connect() as conn:
        dump = json.dumps([[dict(r) for r in conn.execute(f"SELECT * FROM {t}").fetchall()] for t in _TABLES])
    blob = dump + json.dumps(rows)
    for raw in ("eyJhbGciOiJIUzI1NiJ9", _SECRETS["binance_key"], "AKIAIOSFODNN7EXAMPLE", "hunter2", "s3cr3tP@ss",
                "ghp_AbCdEf"):
        assert raw not in blob, raw


# ============================== RESEARCH ==============================
def test_risk_reject_remains_a_valid_market_observation(tmp_path):
    h = Harness(tmp_path)
    TradePlanEvidenceStore(h.db).append(h.plan)
    assert h.run(adaptive_daily_risk=_hard_cap_context(125.0)).status == "RISK_REJECTED"
    pid = h.plan.trade_plan_id
    row = export_research_rows(h.db, h.plan.broker_account_id,
                               market_outcomes={pid: {"terminal_outcome": "TARGET_BEFORE_STOP", "net_R": 1.8}})[0]
    assert row["execution_outcome"]["status"] == "NOT_SUBMITTED_RISK_REJECTED"
    assert row["execution_outcome"]["hard_risk_rejected"] and row["execution_outcome"]["rejection_family"] == "DAILY_LOSS"
    assert row["market_outcome"]["terminal_outcome"] == "TARGET_BEFORE_STOP"
    assert row["market_outcome"]["market_observation_valid"] is True
    assert row["labels"]["setup_failure"] is False  # a risk rejection is NOT a setup failure
    assert row["account_outcome"]["position_existed"] is False


def test_execution_failure_separated_from_market_outcome(tmp_path):
    h = Harness(tmp_path, entry=_entry(), orders=[_order("CANCELED", "0")])
    TradePlanEvidenceStore(h.db).append(h.plan)
    h.run()
    pid = h.plan.trade_plan_id
    row = export_research_rows(h.db, h.plan.broker_account_id,
                               market_outcomes={pid: {"terminal_outcome": "TARGET_BEFORE_STOP"}})[0]
    assert row["execution_outcome"]["status"] == "NOT_FILLED"
    assert row["market_outcome"]["terminal_outcome"] == "TARGET_BEFORE_STOP" and row["labels"]["setup_failure"] is False
    unlabeled = export_research_rows(h.db, h.plan.broker_account_id)[0]
    assert unlabeled["market_outcome"]["terminal_outcome"] == "UNLABELED" and unlabeled["labels"]["setup_failure"] is None


def test_realized_execution_cost_linked(chain):
    row = export_research_rows(chain.h.db, chain.acct, account_outcomes={
        chain.h.plan.trade_plan_id: {"realized_pnl": 12.5}})[0]
    assert row["lineage"]["execution_attempt_id"] == chain.res.attempt.execution_attempt_id
    assert row["lineage"]["exit_decision_id"] == chain.out.decision.exit_decision_id
    assert row["execution"]["planned_costs_R"]["total_cost_R"] == pytest.approx(chain.h.plan.expected_costs.total_cost_R)
    assert "slippage_bps" in row["execution"]["realized_costs"]
    assert row["execution"]["filled_price"] == pytest.approx(100.0)
    assert row["position"]["exit_intent"] == chain.out.decision.action and row["position"]["mfe_R"] is not None
    assert row["account_outcome"]["realized_pnl"] == 12.5
    assert row["versions"]["trade_plan_hash"] == chain.h.plan.trade_plan_hash
    assert row["schema_version"] and row["risk"]["status"] == "APPROVED"


# ============================== REPLAY ==============================
def test_replay_live_parity_and_deterministic_evidence(chain):
    replay = _evaluate_position(chain.h, chain.res, mode="REPLAY")
    assert replay.forecast.position_forecast_id == chain.out.forecast.position_forecast_id
    assert replay.decision.exit_decision_id == chain.out.decision.exit_decision_id
    live_row = PositionForecastStore(chain.h.db).get(chain.out.forecast.position_forecast_id)
    assert live_row["payload"]["forecast_hash"] == replay.forecast.forecast_hash
    # the same contract, the same engine: a REPLAY evaluation is never a separate simplified path
    assert type(replay.forecast) is type(chain.out.forecast) and replay.forecast.engine_version == \
        chain.out.forecast.engine_version


# ============================== FAILURE ==============================
def test_component_error_recorded_and_reraised(chain):
    clear_component_errors()
    with pytest.raises(ZeroDivisionError):
        with timed_stage("RISK", "test.stage", db=chain.h.db, cycle_id="c9", broker_account_id=chain.acct):
            1 / 0
    rec = recent_component_errors()[-1]
    assert rec.reason_code == "CATI_COMPONENT_ERROR" and rec.stage == "RISK" and rec.exception_class == "ZeroDivisionError"
    with chain.h.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM cati_component_errors WHERE stage='RISK'").fetchone()[0] >= 1


def test_no_silent_exception_on_submit(tmp_path):
    clear_component_errors()
    h = Harness(tmp_path)

    class Exploding:
        venue, adapter_id = h.plan.venue, "exploding"
        execution_support_status, protocol_version = "CONTRACT_VALIDATED", "1.0.0"
        executor = h.executor

        def submit_entry(self, request):
            raise RuntimeError("socket closed after send")

    res = h.run(h.boundary(adapter=Exploding()))
    assert res.status == "SUBMIT_UNKNOWN_PENDING_RECONCILIATION"  # never read as a failure, never re-submitted
    assert any(r.component == "boundary.submit_entry" for r in recent_component_errors())
    assert h.reservation_status() == "RESERVED"


# ============================== MULTI-TENANT ==============================
def test_account_a_evidence_never_appears_for_account_b(chain):
    db, other = chain.h.db, "acct2"
    assert export_research_rows(db, other) == []
    assert PositionForecastStore(db).for_account(other) == []
    assert ExitDecisionStore(db).for_account(other) == []
    assert RiskDecisionStore(db).for_account(other) == []
    assert ExecutionAttemptStore(db).for_account(other) == []
    assert resolve_cati_plan(db, other, chain.res.attempt.position_id) is None
    assert TradePlanEvidenceStore(db).load_plan(other, chain.h.plan.trade_plan_id) is None
    with pytest.raises(ValueError):
        export_research_rows(db, "")
    assert len(export_research_rows(db, chain.acct)) == 1


def test_evidence_modules_store_only_opaque_ids():
    """No personal / credential field is part of any CATI evidence contract."""
    from app.trading_intelligence.contracts.execution import ExecutionAttempt, RiskDecision
    from app.trading_intelligence.contracts.position import PositionForecast, PositionPathSnapshot

    banned = ("email", "name", "phone", "address", "api_key", "secret", "password", "token")
    for cls in (ExecutionAttempt, RiskDecision, PositionForecast, PositionPathSnapshot, ExitDecision):
        for f in cls.__dataclass_fields__:
            assert not any(b == f or f.endswith("_" + b) for b in banned), (cls.__name__, f)
    for pkg in NEW_MODULES:
        for path in pkg.glob("*.py"):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute):
                    assert node.attr not in ("api_key", "api_secret", "secret_key"), (path.name, node.attr)
