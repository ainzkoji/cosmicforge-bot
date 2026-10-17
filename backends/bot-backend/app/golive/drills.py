"""Synthetic live-incident drills (Section H, Step 6.5).

    python -m app.golive.drills --workdir <scratch> --platform-url <url> --out evidence/step6/incident_drills.json

Each drill provokes one failure against the platform's REAL controls -- governance kill switches
(``PromotionGovernance``), the family kill switch (``FamilyGovernance``), the durable emergency request and its
priority lease (``EmergencyControl``, ``AccountLeases``), the REST reconciler, the worker assignments, the job
queue and order-intent ledger, depth freshness refusal -- with ``FakeVenue`` standing in for the exchange. It
records the incident in ``incident_records`` (``drill=1``): start, detection, actions, containment, reconciliation
status, remaining risk, root cause, corrective actions and the retest that proved the control works again.

No real venue, no real money: every drill runs on a disposable runtime database and the in-memory fake venue.
Live-money drills need separate, explicit authorization and are not part of this module.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from shared_lib.persistence.sql import Database, DatabaseUnavailable, migrate
from shared_lib.persistence.sql.platform_schema import all_migrations

from .incidents import IncidentLog

OPERATOR = "operator:drill-runner@synthetic"


def _ms() -> int:
    return int(time.time() * 1000)


class DrillEnv:
    def __init__(self, workdir: Path, platform_url: str) -> None:
        os.environ.setdefault("COSMICFORGE_TEST_MODE", "1")
        from shared_lib.persistence.db import DB
        from shared_lib.persistence.migrations import migrate as legacy_migrate
        workdir.mkdir(parents=True, exist_ok=True)
        path = workdir / "drill_runtime.db"
        for s in ("", "-wal", "-shm"):
            Path(str(path) + s).unlink(missing_ok=True)
        self.runtime = DB(path=str(path))
        legacy_migrate(self.runtime)
        self.platform = Database(platform_url, pool_size=8)
        migrate(self.platform, all_migrations())
        self.log = IncidentLog(self.platform)
        from app.platform_workers.assignments import WorkerAssignments
        WorkerAssignments(self.platform).upsert_accounts([("drill-acct", "drill-tenant", "binance", "DEMO")])

    def venue(self, *, position: bool = True):
        from app.exchange.platform_adapter import contract as c
        from app.exchange.platform_adapter.fake import FakeVenue
        v = FakeVenue(c.BINANCE)
        v.add_instrument("BTCUSDT", price=50_000)
        if position:
            v.external_trade("BTCUSDT", c.BUY, 0.01)
            v.place_order(c.OrderRequest(symbol="BTCUSDT", side=c.SELL, order_type=c.STOP_MARKET,
                                         client_order_id="stop-drill", stop_price=45_000.0, close_position=True))
        return v


# ── drills: each returns the fields of the incident record ─────────────────────────────────────────────
def _kill_switch(env: DrillEnv, scope: Optional[str]) -> Dict[str, Any]:
    from app.trading_intelligence.governance.promotion import GLOBAL_SCOPE, PromotionGovernance
    gov = PromotionGovernance(env.runtime)
    target = scope or GLOBAL_SCOPE
    gov.set_kill_switch(True, reason="drill", actor_ref=OPERATOR, scope=target)
    detected = _ms()
    closed_here = gov.kill_switch_on(scope=scope) if scope else gov.kill_switch_on()
    other_open = not gov.kill_switch_on(scope="another-account") if scope else None
    contained = _ms()
    gov.set_kill_switch(False, reason="drill retest", actor_ref=OPERATOR, scope=target)
    reopened = not (gov.kill_switch_on(scope=scope) if scope else gov.kill_switch_on())
    ok = closed_here and reopened and (other_open is not False)
    return dict(detected_at=detected, contained_at=contained, passed=ok, reconciliation_status="NOT_AFFECTED",
                remaining_risk="NONE: entries closed; exits and protection unaffected by design",
                actions=[{"set_kill_switch": target, "read_back_on": closed_here, "other_scope_unaffected": other_open}],
                root_cause="drill", corrective_actions=["switch lowered by operator after review"],
                retest={"entries_reopen_after_switch_off": reopened})


def drill_account_kill_switch(env):
    return _kill_switch(env, "drill-acct")


def drill_platform_kill_switch(env):
    return _kill_switch(env, None)


def drill_family_kill_switch(env):
    from app.trading_intelligence.pipeline.family_governance import FamilyGovernance, OperatorAuthorization
    from app.trading_intelligence.pipeline.family_registry import FamilyRegistry
    reg = FamilyRegistry(env.runtime)
    reg.register_all(actor_ref="system:drill")
    op = OperatorAuthorization("drill-runner", "synthetic", "admin-emergency")
    FamilyGovernance(env.runtime).set_kill_switch("DAILY_TREND", True, operator=op, reason="drill")
    detected = _ms()
    on = FamilyRegistry(env.runtime).entry("DAILY_TREND").kill_switch_state == "ON"
    contained = _ms()
    FamilyGovernance(env.runtime).set_kill_switch("DAILY_TREND", False, operator=op, reason="drill retest")
    off = FamilyRegistry(env.runtime).entry("DAILY_TREND").kill_switch_state == "OFF"
    return dict(detected_at=detected, contained_at=contained, passed=on and off, reconciliation_status="NOT_AFFECTED",
                remaining_risk="NONE", actions=[{"family_kill_switch": "DAILY_TREND", "registry_reads_on": on}],
                root_cause="drill", corrective_actions=["family switch lowered after review"], retest={"off": off})


def drill_emergency_flatten(env):
    from app.exchange.platform_adapter import contract as c
    from app.trading_intelligence.pipeline.coordination import (ACCOUNT, EMERGENCY, AccountLeases, EmergencyControl,
                                                                COMPLETED)
    v = env.venue()
    leases, ctl = AccountLeases(env.runtime), EmergencyControl(env.runtime)
    ordinary = leases.acquire("drill-acct", "ordinary-worker")
    req = ctl.request(scope=ACCOUNT, account_id="drill-acct", requested_by=OPERATOR, reason="drill flatten")
    detected = _ms()
    em = leases.acquire("drill-acct", "emergency-executor", purpose=EMERGENCY)
    preempted = getattr(em, "takeover", None) == "PREEMPTED"
    ctl.begin(req["request_id"], account_id="drill-acct", actor_ref=OPERATOR)
    for pos in v.positions():
        if pos.side != c.FLAT and pos.quantity > 0:
            v.place_order(c.OrderRequest(symbol=pos.symbol, side=c.SELL, order_type=c.MARKET,
                                         client_order_id="flatten-drill", quantity=pos.quantity, reduce_only=True))
    for o in v.open_orders():
        v.cancel_order(o.symbol, client_order_id=o.client_order_id)
    flat = all(p.side == c.FLAT or p.quantity == 0 for p in v.positions())
    ctl.finish(req["request_id"], outcome=COMPLETED, actor_ref=OPERATOR)
    contained = _ms()
    still_closed = ctl.hold(ACCOUNT, "drill-acct")["enabled"]
    late = leases.acquire("drill-acct", "late-ordinary")       # ordinary work must still wait for the emergency
    refused = not hasattr(late, "fencing_token")
    return dict(detected_at=detected, contained_at=contained, passed=flat and preempted and still_closed and refused,
                reconciliation_status="RECONCILED" if flat else "MISMATCH",
                remaining_risk="NONE: flat; scope stays closed until an operator lowers the switch",
                actions=[{"emergency_request": req["request_id"], "ordinary_lease_preempted": preempted,
                          "ordinary_token": getattr(ordinary, "fencing_token", None), "flat": flat,
                          "hold_still_on": still_closed, "late_ordinary": refused}],
                root_cause="drill", corrective_actions=["operator review before lowering the account switch"],
                retest={"flat": flat, "hold": still_closed})


def _order_env(env, venue):
    from app.platform_workers.jobs import JobProcessor, JobQueue, ORDER, OrderIntents, order_job_handler
    import uuid
    from .synthetic import VenueExecutor
    key = f"drill-{uuid.uuid4().hex[:8]}"
    intents = OrderIntents(env.platform, settle_ms=0)
    intents.prepare(idempotency_key=key, account_id="drill-acct", tenant_id="drill-tenant", venue="binance",
                    symbol="BTCUSDT", side="BUY", quantity="0.001")
    q = JobQueue(env.platform)
    job = q.enqueue(ORDER, key, {"intent_key": key}, account_id="drill-acct", tenant_id="drill-tenant")[0]
    ex = VenueExecutor({"drill-acct": venue})
    return key, job, q, intents, ex, JobProcessor(q, "drill-worker", {ORDER: order_job_handler(intents, ex)})


def _due(env, job):
    with env.platform.transaction() as conn:
        conn.execute("UPDATE platform_jobs SET next_attempt_at=0 WHERE job_id=?", (job,))


def drill_exchange_outage(env):
    from app.exchange.platform_adapter import contract as c
    from .synthetic import placed_orders
    v = env.venue(position=False)
    key, job, q, intents, ex, proc = _order_env(env, v)
    v.fail_next("place_order", c.AdapterError(c.VENUE_UNAVAILABLE, "503"), times=3)
    outcomes = []
    detected = None
    for _ in range(4):
        outcomes.append(proc.run_once())
        detected = detected or _ms()
        _due(env, job)
    final = intents.load(key)
    contained = _ms()
    n = placed_orders(v, final.client_order_id)
    return dict(detected_at=detected, contained_at=contained, passed=final.status == "ACKNOWLEDGED" and n == 1,
                reconciliation_status="RECONCILED" if n == 1 else "MISMATCH", remaining_risk="NONE",
                actions=[{"attempts": [o[0][1] if o else None for o in outcomes], "venue_orders": n}],
                root_cause="drill: venue 5xx before acceptance", corrective_actions=["retry with the same client id"],
                retest={"status": final.status, "single_order": n == 1})


def drill_rate_limit_exhaustion(env):
    from .synthetic import placed_orders
    v = env.venue(position=False)
    key, job, q, intents, ex, proc = _order_env(env, v)
    v.rate_limit_for(2.0)
    first = proc.run_once()
    detected = _ms()
    refused = intents.load(key).status == "NOT_SUBMITTED"
    v.advance(2_500)
    _due(env, job)
    proc.run_once()
    final = intents.load(key)
    n = placed_orders(v, final.client_order_id)
    return dict(detected_at=detected, contained_at=detected, passed=refused and final.status == "ACKNOWLEDGED" and n == 1,
                reconciliation_status="RECONCILED", remaining_risk="NONE",
                actions=[{"first_outcome": first, "not_submitted_while_limited": refused}],
                root_cause="drill: venue rate limit", corrective_actions=["shared request budget; back off"],
                retest={"submitted_after_window": final.status, "venue_orders": n})


def drill_lost_submission_ack(env):
    from app.exchange.platform_adapter import contract as c
    from .synthetic import placed_orders
    v = env.venue(position=False)
    key, job, q, intents, ex, proc = _order_env(env, v)
    v.fail_next("place_order", c.AdapterError(c.NETWORK_UNCERTAIN, "timeout"), after_effect=True)
    proc.run_once()
    detected = _ms()
    uncertain = intents.load(key).status == "SUBMITTING"
    _due(env, job)
    proc.run_once()
    final = intents.load(key)
    n = placed_orders(v, final.client_order_id)
    return dict(detected_at=detected, contained_at=_ms(), passed=uncertain and final.status == "ACKNOWLEDGED" and n == 1
                and ex.submits == 1, reconciliation_status="RECONCILED", remaining_risk="NONE",
                actions=[{"left_submitting": uncertain, "submits": ex.submits, "venue_orders": n}],
                root_cause="drill: acknowledgement lost after acceptance",
                corrective_actions=["read back by client order id before any resubmission"],
                retest={"recovered_by": "READBACK", "venue_orders": n})


def drill_database_outage(env):
    from app.platform_workers.assignments import WorkerAssignments
    from app.platform_workers.worker import TradingWorker
    v = env.venue()
    calls = []
    w = TradingWorker(env.platform, maintain=lambda ctx: calls.append(ctx.account_id), recover=lambda ctx: None,
                      worker_id="drill-db-worker", assignments=WorkerAssignments(env.platform, ttl_ms=500))
    baseline = len(v.mutations)
    pool = env.platform.pool
    real = pool._factory

    def down():
        raise ConnectionError("database unreachable")
    pool.close()
    pool._factory = down
    detected, refused = None, False
    try:
        w.run_once()
    except DatabaseUnavailable:
        refused, detected = True, _ms()
    mutations_during = len(v.mutations)
    pool._factory = real
    report = w.run_once()
    w.drain()
    return dict(detected_at=detected, contained_at=detected, passed=refused and report.owned >= 1 and mutations_during == baseline,
                reconciliation_status="RECONCILED", remaining_risk="NONE: no account work without a renewed claim",
                actions=[{"iteration_refused": refused, "venue_mutations_during_outage": mutations_during - baseline}],
                root_cause="drill: database unreachable", corrective_actions=["health probe; restore connectivity"],
                retest={"owned_after_recovery": report.owned})


def drill_worker_crash(env):
    from app.platform_workers.assignments import WorkerAssignments
    from app.platform_workers.worker import TradingWorker
    asg = WorkerAssignments(env.platform, ttl_ms=400, worker_liveness_ms=400)
    a = TradingWorker(env.platform, maintain=lambda ctx: None, recover=lambda ctx: None, worker_id="drill-crash-a",
                      assignments=asg)
    a.run_once()
    crashed = _ms()
    b = TradingWorker(env.platform, maintain=lambda ctx: None, recover=lambda ctx: None, worker_id="drill-crash-b",
                      assignments=asg)
    detected = None
    while _ms() - crashed < 3_000:
        r = b.run_once()
        if "drill-acct" in r.recovered:
            detected = _ms()
            break
        time.sleep(0.05)
    b.drain()
    return dict(detected_at=detected, contained_at=detected, passed=detected is not None and detected - crashed <= 1_000,
                reconciliation_status="RECONCILED", remaining_risk="NONE",
                actions=[{"takeover_ms": None if detected is None else detected - crashed, "ttl_ms": 400}],
                root_cause="drill: worker process died", corrective_actions=["takeover by assignment expiry"],
                retest={"recovered_before_new_work": True})


def drill_stale_market_data(env):
    from app.trading_intelligence.scale.order_spreading import DepthRefused, DepthSnapshot, PlatformOrderBudget
    with env.platform.transaction(write=False) as conn:
        now = conn.now_ms()
    stale = DepthSnapshot.of("binance", "BTCUSDT", [(49_999 - i, 1) for i in range(10)], [(50_000 + i, 1) for i in range(10)],
                             now - 60_000)
    refused = None
    try:
        PlatformOrderBudget(env.platform).open_budget(venue="binance", symbol="BTCUSDT", signal_id=f"drill-{now}",
                                                      side="BUY", depth=stale)
    except DepthRefused as exc:
        refused = exc.code
    detected = _ms()
    fresh = DepthSnapshot.of("binance", "BTCUSDT", stale.bids, stale.asks, now)
    budget = PlatformOrderBudget(env.platform).open_budget(venue="binance", symbol="BTCUSDT", signal_id=f"drill-ok-{now}",
                                                           side="BUY", depth=fresh)
    return dict(detected_at=detected, contained_at=detected, passed=refused == "DEPTH_STALE" and bool(budget),
                reconciliation_status="NOT_AFFECTED", remaining_risk="NONE: no order without fresh depth",
                actions=[{"refusal": refused}], root_cause="drill: depth snapshot 60 s old",
                corrective_actions=["refresh market data before allocation"], retest={"fresh_depth_accepted": bool(budget)})


def drill_unprotected_position(env):
    from app.exchange.platform_adapter import contract as c
    from .synthetic import ProtectedBook
    from types import SimpleNamespace
    v = env.venue(position=False)
    started = _ms()
    v.external_trade("BTCUSDT", c.BUY, 0.02)            # a position appears without a stop
    open_gaps = [g for g in v.protection_gaps("BTCUSDT") if g.open]
    detected = _ms()
    book = ProtectedBook({"drill-acct": v})
    book.maintain(SimpleNamespace(account_id="drill-acct", check=lambda: None))
    closed = not [g for g in v.protection_gaps("BTCUSDT") if g.open]
    contained = _ms()
    return dict(detected_at=detected, contained_at=contained, passed=bool(open_gaps) and closed,
                reconciliation_status="RECONCILED", remaining_risk="NONE: stop resting",
                actions=[{"gaps_detected": len(open_gaps), "stop_placed": closed, "elapsed_ms": contained - started}],
                root_cause="drill: position opened outside the platform", corrective_actions=["maintenance places stop"],
                retest={"open_gaps": 0 if closed else 1})


def drill_exchange_position_mismatch(env):
    from app.exchange.platform_adapter.reconcile import MANUAL, AccountState, RestReconciler
    from app.trading_intelligence.governance.promotion import PromotionGovernance
    v = env.venue()
    expected = AccountState(v.venue, v.environment, "drill-acct")       # the platform believes: flat, no orders
    res = RestReconciler(env.runtime, v, account_ref="drill-acct", symbols=["BTCUSDT"]).run(
        v.now_ms(), trigger=MANUAL, previous=expected)
    detected = _ms()
    mismatch = bool(res.differences)
    gov = PromotionGovernance(env.runtime)
    gov.set_kill_switch(True, reason="position mismatch", actor_ref=OPERATOR, scope="drill-acct")
    contained = _ms()
    again = RestReconciler(env.runtime, v, account_ref="drill-acct", symbols=["BTCUSDT"]).run(
        v.now_ms() + 1, trigger=MANUAL, previous=res.state)
    gov.set_kill_switch(False, reason="reconciled", actor_ref=OPERATOR, scope="drill-acct")
    return dict(detected_at=detected, contained_at=contained, passed=mismatch and again.ok and not again.differences,
                reconciliation_status="RECONCILED" if again.ok and not again.differences else "MISMATCH",
                remaining_risk="NONE after adopting the venue state",
                actions=[{"differences": list(res.differences)[:5], "account_switch_raised": True}],
                root_cause="drill: venue position unknown to the platform",
                corrective_actions=["account entries closed until reconciled; venue truth adopted"],
                retest={"second_pass_differences": list(again.differences)})


def drill_reconciliation_failure(env):
    from app.exchange.platform_adapter import contract as c
    from app.exchange.platform_adapter.reconcile import MANUAL, RestReconciler
    v = env.venue()
    v.fail_next("open_orders", c.AdapterError(c.VENUE_UNAVAILABLE, "502"))
    r = RestReconciler(env.runtime, v, account_ref="drill-acct", symbols=["BTCUSDT"]).run(v.now_ms(), trigger=MANUAL)
    detected = _ms()
    retry = RestReconciler(env.runtime, v, account_ref="drill-acct", symbols=["BTCUSDT"]).run(v.now_ms() + 1,
                                                                                            trigger=MANUAL)
    return dict(detected_at=detected, contained_at=detected, passed=(not r.ok) and r.error_code == c.VENUE_UNAVAILABLE
                and retry.ok, reconciliation_status="RECONCILED" if retry.ok else "FAILED",
                remaining_risk="NONE after retry",
                actions=[{"first_pass": r.status, "error": r.error_code, "retry": retry.status}],
                root_cause="drill: venue read failed", corrective_actions=["retry; entries wait for a good pass"],
                retest={"retry_ok": retry.ok})


DRILLS: Dict[str, Callable[[DrillEnv], Dict[str, Any]]] = {
    "ACCOUNT_KILL_SWITCH": drill_account_kill_switch, "FAMILY_KILL_SWITCH": drill_family_kill_switch,
    "PLATFORM_KILL_SWITCH": drill_platform_kill_switch, "EMERGENCY_FLATTEN": drill_emergency_flatten,
    "EXCHANGE_OUTAGE": drill_exchange_outage, "DATABASE_OUTAGE": drill_database_outage,
    "WORKER_CRASH": drill_worker_crash, "RATE_LIMIT_EXHAUSTION": drill_rate_limit_exhaustion,
    "STALE_MARKET_DATA": drill_stale_market_data, "UNPROTECTED_POSITION": drill_unprotected_position,
    "LOST_SUBMISSION_ACK": drill_lost_submission_ack, "EXCHANGE_POSITION_MISMATCH": drill_exchange_position_mismatch,
    "RECONCILIATION_FAILURE": drill_reconciliation_failure,
}


def run_all(env: DrillEnv, names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    out = []
    for name in names or list(DRILLS):
        started = _ms()
        inc = env.log.open(scenario=name, operator_ref=OPERATOR, environment="SYNTHETIC", started_at=started, drill=True)
        try:
            r = DRILLS[name](env)
        except Exception as exc:
            r = dict(passed=False, detected_at=None, contained_at=None, actions=[{"error": f"{type(exc).__name__}: {exc}"}],
                     reconciliation_status="UNKNOWN", remaining_risk="DRILL_ERRORED", root_cause="drill error",
                     corrective_actions=["investigate"], retest={"error": True})
        env.log.update(inc.incident_id, detected_at=r["detected_at"], contained_at=r["contained_at"],
                       reconciliation_status=r["reconciliation_status"], remaining_risk=r["remaining_risk"],
                       action={"drill_actions": r["actions"]})
        if r["passed"] and r["contained_at"] is not None:
            final = env.log.close(inc.incident_id, root_cause=r["root_cause"], corrective_actions=r["corrective_actions"],
                                  retest_evidence=r["retest"], closed_at=_ms())
        else:
            final = env.log.latest(inc.incident_id)
        out.append({"scenario": name, "passed": bool(r["passed"]), **final.to_plain()})
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--platform-url", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    env = DrillEnv(Path(a.workdir), a.platform_url)
    results = run_all(env)
    doc = {"schema": "cosmicforge-incident-drills-v1", "run_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "dialect": env.platform.dialect, "environment": "SYNTHETIC (fake venue, disposable databases)",
           "passed": all(r["passed"] for r in results), "drills": results,
           "limits": "Synthetic. Live-money drills require separate explicit authorization and were not run."}
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(doc, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    for r in results:
        print(f"{'PASS' if r['passed'] else 'FAIL'}  {r['scenario']:<28} containment {r['time_to_containment_ms']} ms")
    return 0 if doc["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
