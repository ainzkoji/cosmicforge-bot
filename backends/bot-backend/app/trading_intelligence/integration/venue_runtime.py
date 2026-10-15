"""Account-scoped read-only stream workers, independent of strategy execution."""
import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class VenueRuntime:
    def __init__(self, db, *, owns_runtime, server_address, accounts, resolve, build, session_factory=None):
        from app.exchange.platform_adapter.session import AccountSession
        self.db, self.owns_runtime, self.server_address = db, owns_runtime, server_address
        self.accounts, self.resolve, self.build = accounts, resolve, build
        self.session_factory = session_factory or AccountSession
        self.stop = threading.Event()
        self.sessions = {}
        self.last_discovery = 0.0

    def _close(self, account_id):
        existing = self.sessions.pop(account_id, None)
        if existing:
            try:
                existing[1].close()
            except Exception as exc:
                logger.warning("[VENUE_STREAM] close failed (%s)", type(exc).__name__)

    def _discover(self):
        seen = set()
        for account in self.accounts(self.db):
            if self.stop.is_set() or not self.owns_runtime(self.db):
                break
            key = account["id"]
            seen.add(key)
            try:
                auth = self.resolve(key, account["user_id"], self.db)
                identity = (auth.user_id, str(auth.broker_type), str(auth.environment),
                            getattr(auth, "credential_version", None), getattr(auth, "key_fingerprint", None))
                old = self.sessions.get(key)
                if old and old[0] == identity:
                    continue
                self._close(key)
                env = getattr(auth.environment, "value", str(auth.environment)).upper()
                session = self.session_factory(self.db, self.build(auth), auth.broker_type.lower(), env,
                                               account_ref=key, server_address=self.server_address)
                self.sessions[key] = (identity, session)
            except Exception as exc:
                self._close(key)
                logger.warning("[VENUE_STREAM] account discovery failed (%s)", type(exc).__name__)
        for key in set(self.sessions) - seen:
            self._close(key)

    def tick(self):
        if self.stop.is_set() or not self.owns_runtime(self.db):
            for key in list(self.sessions):
                self._close(key)
            return
        if time.monotonic() - self.last_discovery >= 30:
            self._discover()
            self.last_discovery = time.monotonic()
        def step(item):
            if self.stop.is_set() or not self.owns_runtime(self.db):
                return
            try:
                item[1].step()
            except Exception as exc:
                logger.warning("[VENUE_STREAM] reconciliation failed (%s)", type(exc).__name__)
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="venue-read") as pool:
            list(pool.map(step, tuple(self.sessions.values())))

    def request_stop(self):
        self.stop.set()

    async def run(self):
        try:
            while not self.stop.is_set():
                worker = asyncio.create_task(asyncio.to_thread(self.tick))
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError:
                    self.request_stop()
                    await worker
                    raise
                except Exception as exc:
                    logger.warning("[VENUE_STREAM] iteration failed (%s)", type(exc).__name__)
                await asyncio.sleep(1)
        finally:
            self.request_stop()
            for key in list(self.sessions):
                self._close(key)
