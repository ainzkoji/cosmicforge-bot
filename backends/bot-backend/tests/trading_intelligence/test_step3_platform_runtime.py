"""Lifecycle and ownership of the independent shadow scheduler."""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.trading_intelligence.integration.platform_runtime import ShadowScheduler
from app.trading_intelligence.pipeline.driver import PlatformCycleDriver
from test_step3_cycle_driver import Setup, START
from app.trading_intelligence.pipeline.daily_trend import close_ms


def test_scheduler_runs_real_pipeline_only_with_runtime_ownership(tmp_path):
    setup = Setup(tmp_path)
    owns = [False]
    runtime = ShadowScheduler(setup.db, owns_runtime=lambda db: owns[0], driver=setup.driver,
                              clock=lambda: close_ms(START) + 60_000)
    assert runtime.tick() == []
    assert setup.count("cati_platform_cycles") == 0
    owns[0] = True
    results = runtime.tick()
    assert results and all(r.mode == "SHADOW" for r in results)
    count = setup.count("cati_family_decisions")
    assert count > 0
    runtime.tick()
    assert setup.count("cati_family_decisions") == count
    runtime.request_stop()
    assert runtime.tick() == []


def test_unsupported_handoff_cannot_report_orders_dispatched():
    with pytest.raises(ValueError, match="SHADOW_DRIVER_EXECUTION_HANDOFF_NOT_SUPPORTED"):
        PlatformCycleDriver(None, accounts=None, data={}, handoff=Mock())


def test_worker_that_loses_cycle_claim_cannot_overwrite_successor(tmp_path, monkeypatch):
    setup = Setup(tmp_path)
    original = setup.driver._update
    def takeover(cycle_id, **fields):
        with setup.db.connect() as conn:
            conn.execute("UPDATE cati_platform_cycles SET owner='successor' WHERE cycle_id=?", (cycle_id,))
        return original(cycle_id, **fields)
    monkeypatch.setattr(setup.driver, "_update", takeover)
    with pytest.raises(RuntimeError, match="CYCLE_OWNERSHIP_LOST"):
        setup.run(START)
    assert setup.count("cati_family_decisions") == 0
    assert setup.rows("SELECT owner, status FROM cati_platform_cycles") == [("successor", "STARTED")]


def test_shutdown_joins_inflight_worker():
    started, release = threading.Event(), threading.Event()
    driver = Mock()
    def run_due(now):
        started.set()
        assert release.wait(5)
        return []
    driver.run_due.side_effect = run_due
    runtime = ShadowScheduler(SimpleNamespace(), owns_runtime=lambda db: True, driver=driver)
    async def scenario():
        task = asyncio.create_task(runtime.run(interval=0.01))
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0.02)
        assert runtime.stop.is_set() and not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert driver.run_due.call_count == 1
    asyncio.run(scenario())
