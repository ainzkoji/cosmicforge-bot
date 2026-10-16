"""Concrete, owner-scoped DEMO certification wiring. Construction/run are explicit.

No runtime service invokes this automatically. Order flags, the canonical lease,
the runner's permit and its flat-account checks all remain mandatory.
"""
import math
import os
import time

import requests

from shared_lib.broker.client_factory import build_client_from_auth
from shared_lib.broker.environment import normalize_environment
from shared_lib.broker.http_budget import BudgetedHTTP
from shared_lib.broker.rate_budget import RateBudget

from . import contract as C
from .certification import CertificationRefused, CertificationRunner, OperatorPermit
from .factory import adapter_for
from .http_faults import HTTPFaultInjector


class CertificationHarness:
    def __init__(self, db, auth, permit, *, source_commit):
        now = int(time.time() * 1000)
        if not isinstance(permit, OperatorPermit):
            raise CertificationRefused("OPERATOR_PERMIT_REQUIRED")
        if permit.environment != C.DEMO:
            raise CertificationRefused("LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD")
        if (permit.account_ref, permit.venue, permit.environment) != (
                auth.account_id, auth.broker_type.lower(), normalize_environment(auth.environment).value.upper()):
            raise CertificationRefused("PERMIT_ACCOUNT_VENUE_ENVIRONMENT_MISMATCH")
        if not permit.issued_at_ms <= now < permit.expires_at_ms:
            raise CertificationRefused("PERMIT_NOT_CURRENT")
        self.db, self.auth, self.permit = db, auth, permit
        self.clock = lambda: int(time.time() * 1000)
        self._http, self._external_done, self._ran = [], False, False
        self.faults = HTTPFaultInjector(requests.Session(), permit.venue, signing_secret=auth.api_secret,
                                       external_reduce=self._reduce_externally)
        try:
            self.adapter = self._adapter(self.faults)
            self.runner = CertificationRunner(
                db, self.adapter, account_ref=auth.account_id, permit=permit, source_commit=source_commit,
                clock=self.clock, faults=self.faults, adapter_factory=self._adapter)
        except BaseException:
            self.close()
            raise

    def _adapter(self, transport=None):
        http = BudgetedHTTP(RateBudget(self.db), self.permit.venue, account_ref=self.auth.account_id,
                            server_address=os.environ.get("CATI_EGRESS_SCOPE", "shared-runtime-egress"),
                            transport=transport, clock=self.clock)
        self._http.append(http)
        client = build_client_from_auth(self.auth, http=http)
        client._production_db, client._production_account_id = self.db, self.auth.account_id
        return adapter_for(client, self.permit.venue, self.permit.environment, clock=self.clock)

    def _reduce_externally(self, symbol, quantity):
        if (not self.permit.allow_orders or not self.runner._flat_at_start or not self.runner._run_id
                or self._external_done or symbol != self.permit.symbol or quantity <= 0
                or quantity > self.runner._unit * (1 + 1e-9)):
            raise CertificationRefused("EXTERNAL_REDUCTION_OUTSIDE_CERTIFICATION_SCOPE")
        separate = self._adapter()
        held = [p for p in separate.positions(symbol) if p.side != C.FLAT]
        if (len(held) != 1 or held[0].side != C.LONG or held[0].mark_price is None
                or not math.isclose(held[0].quantity, self.runner._position_quantity, rel_tol=1e-8)
                or quantity >= held[0].quantity
                or held[0].quantity * held[0].mark_price > self.permit.max_notional * (1 + 1e-9)):
            raise CertificationRefused("CERTIFICATION_POSITION_SCOPE_CHANGED")
        self._external_done = True  # An unanswered request is never replayed.
        request = C.OrderRequest(symbol, C.SELL, C.MARKET, self.runner._next_id(), quantity=quantity, reduce_only=True)
        return self.runner._mutate(
            "EXTERNAL_REDUCE", request.client_order_id, {"quantity": quantity, "reduce_only": True},
            lambda: separate.place_order(request))

    def run(self):
        from app.trading_intelligence.integration.production_runtime import _cycle_lock, owner_current
        if self._ran:
            raise CertificationRefused("CERTIFICATION_HARNESS_ALREADY_USED")
        self._ran = True
        try:
            with _cycle_lock:
                if self.permit.allow_orders and not owner_current(self.db):
                    raise CertificationRefused("CANONICAL_RUNTIME_LEASE_REQUIRED")
                return self.runner.run()
        finally:
            self.close()

    def close(self):
        for http in self._http:
            http.close()
        self._http.clear()
