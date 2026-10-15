"""One account's operational adapter, sharing venue/IP budgets and instrument rules.

The owner supplies the actual egress identity; account IDs must never stand in for
an IP budget. Construction sends nothing. Call step at startup and periodically,
then close on account removal, credential rotation, or runtime shutdown.
"""
import time
import uuid

from .certification import adapter_certified
from .factory import adapter_for
from .rate_budget import BudgetedVenueAdapter, RateBudget
from .rules_cache import RulesCache
from .stream_transport import WebSocketTransport
from .user_stream import UserStreamManager, stream_allows_entries


class AccountSession:
    def __init__(self, db, client, venue, environment, *, account_ref, server_address,
                 symbols=(), clock=None):
        if not str(account_ref).strip() or not str(server_address).strip():
            raise ValueError("ACCOUNT_AND_EGRESS_IDENTITY_REQUIRED")
        self.db, self.account_ref = db, account_ref
        self.clock = clock or (lambda: int(time.time() * 1000))
        owner = "venue-session:" + uuid.uuid4().hex
        raw = adapter_for(client, venue, environment, clock=self.clock)
        self.adapter = BudgetedVenueAdapter(raw, RateBudget(db, worker=owner),
                                           account_ref=account_ref, server_address=server_address,
                                           clock=self.clock)
        self.rules = RulesCache(db, worker=owner)
        self.stream = UserStreamManager(db, self.adapter, WebSocketTransport(),
                                        account_ref=account_ref, owner=owner, symbols=symbols)

    def step(self):
        now = self.clock()
        refresh = None
        if self.rules.refresh_due(self.adapter.venue, self.adapter.environment, now):
            refresh = self.rules.refresh(self.adapter, now)
        return {"rules_refresh": refresh, "stream": self.stream.step(now)}

    def certified(self, venue, environment):
        if (venue, environment) != (self.adapter.venue, self.adapter.environment):
            return False, "SESSION_SCOPE_MISMATCH"
        return adapter_certified(self.db, venue, environment, self.clock(),
                                 adapter_version=self.adapter.adapter_version)

    def entries_allowed(self):
        return stream_allows_entries(self.db, self.adapter.venue, self.adapter.environment,
                                     self.account_ref, self.clock())

    def close(self):
        self.stream.close(self.clock())
