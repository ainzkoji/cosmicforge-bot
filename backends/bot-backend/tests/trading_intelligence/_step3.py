"""Shared fixtures of the Step 3 pipeline tests.

Synthetic families, a synthetic research register and a synthetic market drive the real registry, the real
governance log, the real gate and the real cycle driver with controlled input. None of it is evidence about a
strategy, and none of it exists outside a temporary directory.
"""
from __future__ import annotations

import dataclasses
import uuid

import _step2
from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.governance.phases import PHASE_BY_ID, PHASE_IDS
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
from app.trading_intelligence.pipeline.provenance import DecisionStore
from app.trading_intelligence.research.governance.register import HYPOTHESIS_STATUS

OPERATOR = OperatorAuthorization("admin-7", "admin_api", "admin.emergency")
TEST_FAMILY = "TEST_CERTIFIED_FAMILY"
TEST_RULE_HASH = stable_hash({"rules": "synthetic"})


def fresh_db(tmp_path, name=None):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    db = DB(str(tmp_path / (name or f"step3_{uuid.uuid4().hex[:8]}.db")))
    migrate(db)
    return db


def research_register(tmp_path, *, status=None, name="register.jsonl"):
    """History imported and Mandate 004 registered. ``status`` appends a status record to hypothesis 18, so a
    test can stand a family on a hypothesis that passed, failed or is still open."""
    reg = _step2.new_register(tmp_path / name)
    if status:
        reg.append(HYPOTHESIS_STATUS, {"hypothesis_id": spec.HYPOTHESIS_ID, "status": status, "reason": "SYNTHETIC_TEST"})
    return reg


def definition(family_id=TEST_FAMILY, **over):
    """A synthetic family standing on hypothesis 18 of the test register. No pipeline adapter by default."""
    base = dict(
        family_id=family_id, family_version="t1", mandate_id=spec.MANDATE_ID, hypothesis_id=spec.HYPOTHESIS_ID,
        rule_hash=TEST_RULE_HASH, supported_markets=(("binance", "USDT_PERPETUAL"),), timeframe="15m",
        trading_direction=FR.LONG_SHORT, position_model=FR.SINGLE_POSITION, entry_contract="TradePlan",
        exit_contract="stop and target", data_requirements=("synthetic",),
        activation_policy={"kill_switch_position_policy": FR.MANAGE_TO_EXIT}, evidence_reference=("synthetic",),
        decision_ttl_ms=900_000, implementation="tests.synthetic", pipeline_adapter=None,
        admission_route=FR.FORECAST_BASED)
    base.update(over)
    return FR.FamilyDefinition(**base)


def registry(db, reg, *definitions):
    return FR.FamilyRegistry(db, research_register=reg, definitions_source={d.family_id: d for d in definitions})


def prerequisites(d, *, certification="CERTIFIED", **over):
    out = {"rule_hash": d.rule_hash, "certification_status": certification, "mandate_verified": True,
           "valid_run": True, "admission": None}
    out.update(over)
    return out


def promote(registry_, family_id, to_stage="M6", *, prereq=None, operator=OPERATOR):
    """Walk a registered family stage by stage through the real governance operations."""
    gov = registry_.governance
    d = registry_.definitions()[family_id]
    for stage in PHASE_IDS[1:PHASE_IDS.index(to_stage) + 1]:
        for key in PHASE_BY_ID[stage].entry_requirements:
            gov.record_evidence(family_id, key, reference=f"test://{key}", actor_ref="operator:tester")
        request = gov.request_promotion(family_id, stage, actor_ref="operator:tester", reason=f"test promotion to {stage}")
        gov.approve_promotion(request["request_id"], operator=operator, reason="test approval",
                              prerequisites=prereq or prerequisites(d))


def rebuilt(plan, **changes):
    """A TradePlan with some fields changed and a correct id and hash, as its builder would have made it."""
    fields = {f.name: getattr(plan, f.name) for f in dataclasses.fields(plan)
              if f.name not in ("trade_plan_id", "trade_plan_hash")}
    fields.update(changes)
    return type(plan).build(**fields)


def decided_plan(db, plan, d, *, mode="DEMO", environment="DEMO", now, ttl=900_000, intent_over=None, **record_over):
    """Record an immutable family decision for ``plan`` and return the plan carrying its provenance."""
    cycle_id = record_over.pop("cycle_id", f"cyc_test_{uuid.uuid4().hex[:8]}")
    intent = C.EntryIntent(symbol=plan.instrument_key.venue_symbol, direction=plan.side, quantity=1.0,
                           reference_price=plan.entry_reference, stop_price=plan.structural_invalidation_price,
                           risk_fraction=0.004)
    document = {"intents": [dict(C.to_plain(intent), **(intent_over or {}))]}
    fields = dict(cycle_id=cycle_id, family_id=d.family_id, family_version=d.family_version, rule_hash=d.rule_hash,
                  mandate_id=d.mandate_id, account_id=plan.broker_account_id, user_id=plan.user_id,
                  environment=environment, mode=mode, decision_time=now - 1_000, data_cutoff="test",
                  expires_at=now - 1_000 + ttl, document=document)
    fields.update(record_over)
    recorded = DecisionStore(db).record(now_ms=now, **fields)
    versions = tuple(sorted({**dict(plan.versions), "family_version": d.family_version, "family_rule_hash": d.rule_hash,
                             "mandate_id": d.mandate_id or "NONE", "hypothesis_id": d.hypothesis_id or "NONE",
                             "decision_hash": recorded["decision_hash"], "data_snapshot_id": "test",
                             "timeframe": d.timeframe}.items()))
    return rebuilt(plan, setup_family=d.family_id, setup_version=d.family_version,
                   source_candidate_id=recorded["decision_id"], decision_time=fields["decision_time"], mode=mode,
                   environment=environment, versions=versions), recorded
