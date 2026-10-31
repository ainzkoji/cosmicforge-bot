"""Independent, lease-owned SHADOW_ONLY scheduler. No order submission capability."""
from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


def budgeted_archive_get(db):
    """Inject read-only HTTP into the feed without coupling it to execution."""
    from urllib.parse import urlsplit
    from app.exchange.platform_adapter.http_budget import public_get
    from app.market_data.binance_archive import http_get
    def get(url):
        if urlsplit(url).hostname == "fapi.binance.com":
            response = public_get(db, url, timeout=(5, 30))
            response.raise_for_status()
            return response.content
        return http_get(url)  # archive CDN has a separate allowance
    return get


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
            data={"1d": default_daily_bars(self.db, http_get=budgeted_archive_get(self.db))}, account_runner=self.runner,
            owner="shadow-runtime:" + uuid.uuid4().hex, compare_models=True)

    def tick(self):
        if self.stop.is_set() or not self.owns_runtime(self.db):
            return []
        if self.driver is None:
            self._initialize()
        now = self.clock()
        try:
            return self.driver.run_due(now)
        finally:
            # Retention does not depend on the cycles: a failing cycle must not let the tables grow unbounded.
            self._retain(now)

    def _retain(self, now):
        """Bounded batches, never VACUUM. Each archive is verified before its rows are removed. At most once a day;
        a failure is logged and retried on the next tick, and never hides the cycle's own result or error."""
        if self.stop.is_set() or not self.owns_runtime(self.db):
            return
        if self.last_retention is not None and now - self.last_retention < 86_400_000:
            return
        try:
            from app.trading_intelligence.pipeline.retention import run_all
            run_all(self.db, archive_dir=Path(self.db.path).parent / "cati_archives",
                    now_ms=now, max_rows=500, max_batches=1)
        except Exception:
            logger.exception("[CATI_SHADOW] retention failed; retried on the next tick")
            return
        self.last_retention = now

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
