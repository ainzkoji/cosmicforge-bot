"""Step 5.6: a deployment that kills a worker mid-cycle, while several protected positions exist on a fake venue."""
from __future__ import annotations

import time

from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.fake import FakeVenue
from app.exchange.platform_adapter.reconcile import MANUAL, RestReconciler
from app.platform_workers.assignments import WorkerAssignments
from app.platform_workers.jobs import OrderIntents, RetryLater
from app.platform_workers.worker import TradingWorker
from app.trading_intelligence.governance.promotion import PromotionGovernance
from app.trading_intelligence.pipeline.coordination import ACCOUNT, EmergencyControl
from venue_bridge import ProtectedBook, VenueExecutor, placed_orders

N = 5


class Killed(BaseException):
    """The process dies: nothing after this point of the cycle runs (not an Exception, so nothing catches it)."""


def test_deployment_kill_mid_cycle_preserves_protection_intents_and_risk_state(platform_db, tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate as legacy_migrate
    runtime = DB(path=str(tmp_path / "runtime.db"))
    legacy_migrate(runtime)
    venues = {}
    for i in range(N):
        v = FakeVenue(c.BINANCE)
        v.add_instrument("BTCUSDT", price=50_000)
        v.external_trade("BTCUSDT", c.BUY, 0.01)
        venues[f"acct{i:04d}"] = v
    asg = WorkerAssignments(platform_db, ttl_ms=400, worker_liveness_ms=400)
    asg.upsert_accounts([(a, "tenant", "binance", "DEMO") for a in venues])
    executor = VenueExecutor(venues)
    intents = OrderIntents(platform_db, asg, settle_ms=0)
    book = ProtectedBook(venues, intents, executor)

    # risk state that must survive the deployment: an account latch raised before it
    PromotionGovernance(runtime).set_kill_switch(True, reason="loss-streak latch", actor_ref="engine", scope="acct0002")

    old = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="old-build",
                        assignments=asg, max_concurrency=1)
    assert old.run_once().owned == N                      # every position protected by the old build
    keys = {}

    def mid_cycle(ctx):
        key = f"rebalance:{ctx.account_id}"
        intents.prepare(idempotency_key=key, account_id=ctx.account_id, tenant_id="tenant", venue="binance",
                        symbol="BTCUSDT", side="BUY", quantity="0.001", fencing_token=ctx.assignment.fencing_token)
        keys.setdefault(ctx.account_id, []).append(key)
        if ctx.account_id == "acct0001":
            venues[ctx.account_id].fail_next("place_order", c.AdapterError(c.NETWORK_UNCERTAIN, "deploy cut"),
                                             after_effect=True)
            try:
                intents.execute(key, executor, fence=ctx.fence)
            except RetryLater:
                pass
            raise Killed()                               # SIGKILL in the middle of the cycle

    book.hook = mid_cycle
    try:
        old.run_once()
    except Killed:
        pass
    book.hook = None
    book.pending_intents = keys
    assert intents.load("rebalance:acct0001").status == "SUBMITTING"   # outcome unknown at the kill

    time.sleep(0.45)                                      # the old build's assignments expire
    new = TradingWorker(platform_db, maintain=book.maintain, recover=book.recover, worker_id="new-build",
                        assignments=WorkerAssignments(platform_db, ttl_ms=400, worker_liveness_ms=400))
    report = new.run_once()
    assert report.owned == N and set(report.recovered) == set(venues) and not report.failed

    final = intents.load("rebalance:acct0001")
    assert final.status == "ACKNOWLEDGED" and placed_orders(venues["acct0001"], final.client_order_id) == 1
    for account_id, v in venues.items():
        assert not [g for g in v.protection_gaps("BTCUSDT") if g.open], account_id          # never unprotected
        assert len([o for o in v.open_orders("BTCUSDT") if o.order_type == c.STOP_MARKET]) == 1
        assert asg.owner_of(account_id)[0] == "new-build"
    assert PromotionGovernance(runtime).kill_switch_on(scope="acct0002")                    # risk state persisted
    result = RestReconciler(runtime, venues["acct0001"], account_ref="acct0001", symbols=["BTCUSDT"]).run(
        venues["acct0001"].now_ms(), trigger=MANUAL)
    assert result.ok                                                                       # reconciliation resumes
    req = EmergencyControl(runtime).request(scope=ACCOUNT, account_id="acct0003", requested_by="operator:on-call",
                                            reason="deploy drill")
    assert req["state"] == "REQUESTED" and PromotionGovernance(runtime).kill_switch_on(scope="acct0003")
