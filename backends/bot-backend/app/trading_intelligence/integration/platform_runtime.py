"""Independent, lease-owned SHADOW_ONLY scheduler. No order submission capability."""
from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


class ShadowScheduler:
    def __init__(self, db, *, owns_runtime, driver=None, runner=None, clock=None):
        self.db = db
        self.owns_runtime = owns_runtime
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.stop = threading.Event()
        self.driver = driver
        self.runner = runner
        self.last_retention = None

    def _initialize(self):
        from app.market_data.daily_feed import default_daily_bars
        from app.trading_intelligence.families.daily_trend import spec
        from app.trading_intelligence.pipeline.accounts import CombinedAccounts, ConnectedAccounts, ModelAccounts
        from app.trading_intelligence.pipeline.coordination import AccountLeases, BoundedAccountRunner, EmergencyControl
        from app.trading_intelligence.pipeline.driver import PlatformCycleDriver
        from app.trading_intelligence.pipeline.family_registry import FamilyRegistry

        registry = FamilyRegistry(self.db)
        registry.register_all(actor_ref="system:shadow-scheduler")
        self.runner = BoundedAccountRunner(db=self.db, stop=self.stop.is_set,
                                          leases=AccountLeases(self.db), emergency=EmergencyControl(self.db),
                                          admit=lambda *args: self.owns_runtime(self.db))
        self.driver = PlatformCycleDriver(
            self.db, registry=registry,
            accounts=CombinedAccounts(ModelAccounts(initial_equity=spec.INTERPRETATION["initial_equity_usdt"]),
                                      ConnectedAccounts(self.db)),
            data={"1d": default_daily_bars(self.db)}, account_runner=self.runner,
            owner="shadow-runtime:" + uuid.uuid4().hex)

    def tick(self):
        if self.stop.is_set() or not self.owns_runtime(self.db):
            return []
        if self.driver is None:
            self._initialize()
        now = self.clock()
        results = self.driver.run_due(now)
        # Bounded batches, never VACUUM. Each archive is verified before its rows are removed.
        if not self.stop.is_set() and self.owns_runtime(self.db) and (
                self.last_retention is None or now - self.last_retention >= 86_400_000):
            from app.trading_intelligence.pipeline.retention import run_all
            run_all(self.db, archive_dir=Path(self.db.path).parent / "cati_archives",
                    now_ms=now, max_rows=500, max_batches=1)
            self.last_retention = now
        return results

    def request_stop(self):
        self.stop.set()
        if self.runner is not None:
            self.runner.request_stop()

    async def run(self, interval=30):
        try:
            while not self.stop.is_set():
                # Shield the worker so cancellation cannot leave it detached from runtime shutdown.
                worker = asyncio.create_task(asyncio.to_thread(self.tick))
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError:
                    self.request_stop()
                    await worker
                    raise
                except Exception:
                    logger.exception("[CATI_SHADOW] cycle failed; retrying durable unfinished work")
                await asyncio.sleep(interval)
        finally:
            self.request_stop()
