"""Concrete, owner-scoped DEMO certification wiring. Construction/run are explicit.

No runtime service invokes this automatically. Order flags, the canonical lease,
the runner's permit and its flat-account checks all remain mandatory.

Two ways to run it, and only two:

* a REAL run against a venue's DEMO host. It needs an ``OperatorPermit`` for DEMO (LIVE is refused before a
  client is built), credentials resolved from the connected demo account by the canonical resolver, and the
  canonical runtime lease held by the running process. ``scripts/run_venue_certification.py --execute`` is
  the operator entry point.
* a MOCKED-TRANSPORT run (``transport_factory``): every HTTP request of every client the harness builds goes
  to the transport that factory returns, never to ``requests``. The test suite and
  ``scripts/run_venue_certification.py --dry-run`` use it to drive the complete catalogue end to end through
  the REAL raw clients, adapters, HTTP budget and fault injector over a simulated venue. A mocked run proves
  the harness, never a venue: its register is a scratch database and nothing it records is evidence about
  Binance, Bybit or BingX.

``VENUE_REQUIREMENTS`` states, per venue, what a real run needs (credentials by the names that hold them,
hosts, account settings, test sizes, stop capability, what is still unverified). ``preflight`` checks a
planned run against it without any network access.
"""
import json
import math
import os
import time

import requests

from shared_lib.broker.client_factory import build_client_from_auth
from shared_lib.broker.environment import BrokerEnvironment, normalize_environment, resolve_base_url
from shared_lib.broker.http_budget import BudgetedHTTP
from shared_lib.broker.rate_budget import RateBudget

from . import contract as C
from .certification import (REHEARSAL_ACCOUNT_PREFIX, CertificationRefused, CertificationRegister, CertificationRunner,
                            OperatorPermit)
from .factory import ADAPTER_VERSIONS, adapter_for
from .http_faults import HTTPFaultInjector

#: what an operator must have before a REAL DEMO run of each venue. Credentials are never configured here: they
#: are the connected demo account's, stored encrypted in the platform database (``broker_credentials``) and
#: decrypted by ``shared_lib.broker.resolver`` with the key named by ``credential_key_env``.
_COMMON = {
    "environment": C.DEMO,
    "credential_source": "the connected DEMO broker account (broker_accounts / broker_credentials), resolved by "
                         "shared_lib.broker.resolver.resolve_broker_auth(account_id, user_id, db)",
    "credential_key_env": "BROKER_SECRET_KEY",
    "database_env": "DATABASE_URL",
    "order_flag": "DEMO_ORDER_SUBMISSION_ENABLED (settings; must be True for allow_orders, never changed by the harness)",
    "account_settings": ("one-way position mode", "no open position and no open order on the test symbol",
                         "an API key created without withdrawal permission"),
    "test_size": "the venue's minimum order at a price 5% under the mark; two units with the fault injector; "
                 "the permit's max_notional must hold twice the entry",
    "default_symbol": "BTCUSDT",
    "leverage": 1,
}
VENUE_REQUIREMENTS = {
    C.BINANCE: {**_COMMON,
                "rest_host": resolve_base_url(C.BINANCE, BrokerEnvironment.DEMO),
                "private_stream": "wss://demo-fstream.binance.com/private/ws/<listenKey> (the /private route on the "
                                  "demo host is NOT documented: USER_STREAM is a certification item)",
                "margin_mode": "cross or isolated (reported per position as marginType)",
                "position_mode_check": "GET /fapi/v1/positionSide/dual -> dualSidePosition false",
                "stop_form": "conditional (Algo Service) STOP_MARKET: closePosition, or reduceOnly + quantity",
                "replacement": "CREATE_CONFIRM_RETIRE (multiple_stops_per_position; doc only until certified)",
                "unverified": ("Algo Service endpoints on demo-fapi.binance.com",
                               "-4130 for a second closePosition stop (no longer in the published error list)",
                               "the conditional-order limit (FAQ: 10 per symbol; change log: 200 in all)")},
    C.BYBIT: {**_COMMON,
              "rest_host": resolve_base_url(C.BYBIT, BrokerEnvironment.DEMO),
              "private_stream": "wss://stream-demo.bybit.com/v5/private (doc: demo trading page)",
              "margin_mode": "an account setting (UTA 2.0); the position's tradeMode is deprecated and not read",
              "position_mode_check": "position list positionIdx 0 (one-way, 'Merged Single')",
              "stop_form": "the position's own stop: POST /v5/position/trading-stop, tpslMode=Full",
              "replacement": "AMEND_IN_PLACE (Full mode modifies the existing TP/SL, doc)",
              "unverified": ("that a Full-mode stopLoss change is atomic (not documented)",
                             "Partial tpslMode on demo (not mentioned on the demo page; not used)")},
    C.BINGX: {**_COMMON,
              "rest_host": resolve_base_url(C.BINGX, BrokerEnvironment.DEMO),
              "private_stream": "none: the VST private-stream host is not documented (only a VST market-data host)",
              "margin_mode": "CROSSED or ISOLATED (POST /openApi/swap/v2/trade/marginType)",
              "position_mode_check": "GET /openApi/swap/v1/positionSide/dual -> dualSidePosition \"false\"",
              "stop_form": "STOP_MARKET with reduceOnly and its own quantity, no clientOrderId (LIMIT/MARKET only)",
              "replacement": "REPLACEMENT_NOT_POSSIBLE_SAFELY: PROTECTION_REPLACE proves the refusal and that the "
                             "first stop survives; STOP_REPLACEMENT is never certified",
              "unverified": ("every response shape and error code on the VST host",
                             "whether a second reduce-only stop is refused, and with which code (doc: the pending "
                             "stop-loss quantity may not exceed the position)",
                             "what happens to a resting stop when its position is closed")},
}


def preflight(venue, *, symbol, max_notional, authorization_ref, operator_ref, environment=C.DEMO,
              allow_orders=False, leverage=1, env=None):
    """What stands in the way of a REAL run with these parameters. No network, no database, no credential is
    read: only the NAMES of the settings the run needs are checked for presence. Returns
    ``{"venue", "ready", "problems", "requirements"}``."""
    env = os.environ if env is None else env
    problems = []
    if venue not in VENUE_REQUIREMENTS:
        return {"venue": venue, "ready": False, "problems": [f"UNKNOWN_VENUE:{venue}"], "requirements": {}}
    if str(environment).upper() != C.DEMO:
        problems.append("ENVIRONMENT_MUST_BE_DEMO")
    if not str(authorization_ref or "").strip():
        problems.append("OWNER_AUTHORIZATION_REFERENCE_REQUIRED")
    if not str(operator_ref or "").strip():
        problems.append("OPERATOR_REFERENCE_REQUIRED")
    if C.canonical_symbol(symbol) != symbol:
        problems.append("SYMBOL_NOT_CANONICAL")
    if not (isinstance(max_notional, (int, float)) and 0 < float(max_notional) < math.inf):
        problems.append("MAX_NOTIONAL_REQUIRED")
    if int(leverage) != 1:
        problems.append("LEVERAGE_OTHER_THAN_1_NEEDS_A_SEPARATE_OWNER_DECISION")
    requirements = VENUE_REQUIREMENTS[venue]
    for name in (requirements["credential_key_env"], requirements["database_env"]):
        if not str(env.get(name) or "").strip():
            problems.append(f"SETTING_NOT_PRESENT:{name}")
    if allow_orders and str(env.get("DEMO_ORDER_SUBMISSION_ENABLED", "")).strip().lower() not in ("1", "true", "yes"):
        problems.append("DEMO_ORDER_SUBMISSION_ENABLED_IS_NOT_SET")
    return {"venue": venue, "ready": not problems, "problems": problems, "adapter_version": ADAPTER_VERSIONS[venue],
            "requirements": requirements}


#: the message of a refusal by the platform's OWN HTTP budget (shared_lib.broker.http_budget): nothing was sent
_LOCAL_BUDGET_REFUSAL = "LOCAL_HTTP_BUDGET_REFUSED"
#: the longest a run waits, in total, for one call the local budget refused
MAX_LOCAL_BUDGET_WAIT_S = 300.0


class _PacedAdapter:
    """The adapter, waiting out the platform's own HTTP budget.

    The budget refuses a request BEFORE it is sent when its class has used its share of the window (with the
    conservative 100 units per Binance request, an entry may find its share used up by the run's reads). Such a
    refusal proves nothing was sent, so the same call is made again once the budget says it may be, within
    ``MAX_LOCAL_BUDGET_WAIT_S``. A refusal by the VENUE (an HTTP 429 or 418, Bybit 10006), a ban and every
    other error pass through unchanged and are never retried here."""

    def __init__(self, adapter, sleep):
        self._adapter, self._sleep = adapter, sleep
        self.venue, self.environment, self.adapter_version = adapter.venue, adapter.environment, adapter.adapter_version
        self.waited_s = 0.0

    def __getattr__(self, name):            # anything outside the contract (e.g. the raw client) is the adapter's
        return getattr(self._adapter, name)

    def _paced(self, name, *args, **kwargs):
        waited = 0.0
        while True:
            try:
                return getattr(self._adapter, name)(*args, **kwargs)
            except C.AdapterError as error:
                local = (error.code == C.RATE_LIMITED and error.message == _LOCAL_BUDGET_REFUSAL
                         and error.http_status is None and not error.uncertain)
                if not local or waited >= MAX_LOCAL_BUDGET_WAIT_S:
                    raise
                wait = min(max(float(error.retry_after_s or 1.0), 0.05), MAX_LOCAL_BUDGET_WAIT_S - waited)
                self._sleep(wait)
                waited += wait
                self.waited_s += wait


def _contract_method(name):
    def method(self, *args, **kwargs):
        return self._paced(name, *args, **kwargs)
    method.__name__ = name
    return method


for _name in C.MUTATING_METHODS + C.READ_METHODS:   # explicit members: the contract's protocol check sees them
    setattr(_PacedAdapter, _name, _contract_method(_name))


class CertificationHarness:
    def __init__(self, db, auth, permit, *, source_commit, transport_factory=None, rate_budget=None, sleep=time.sleep):
        now = int(time.time() * 1000)
        if not isinstance(permit, OperatorPermit):
            raise CertificationRefused("OPERATOR_PERMIT_REQUIRED")
        if permit.environment != C.DEMO:
            raise CertificationRefused("LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD")
        if (permit.account_ref, permit.venue, permit.environment) != (
                auth.account_id, auth.broker_type.lower(), normalize_environment(auth.environment).value.upper()):
            raise CertificationRefused("PERMIT_ACCOUNT_VENUE_ENVIRONMENT_MISMATCH")
        if str(getattr(auth, "base_url", "") or "").rstrip("/") != resolve_base_url(
                permit.venue, BrokerEnvironment.DEMO).rstrip("/"):
            raise CertificationRefused("ACCOUNT_IS_NOT_ON_THE_DEMO_HOST")
        if not permit.issued_at_ms <= now < permit.expires_at_ms:
            raise CertificationRefused("PERMIT_NOT_CURRENT")
        # a mocked transport only ever serves a rehearsal account, and a rehearsal account is never a real run:
        # a rehearsal records neither CERTIFIED nor FAILED (CertificationRegister.complete_run)
        if (transport_factory is not None) != str(permit.account_ref).startswith(REHEARSAL_ACCOUNT_PREFIX):
            raise CertificationRefused("MOCKED_TRANSPORT_AND_REHEARSAL_ACCOUNT_GO_TOGETHER")
        self.db, self.auth, self.permit = db, auth, permit
        #: requests.Session for a real run; a mocked transport for a rehearsal (every client the harness builds)
        self._transport_factory = transport_factory or requests.Session
        self.mocked = transport_factory is not None
        self.clock = lambda: int(time.time() * 1000)
        #: the shared persistent budget for a real run; a mocked run may bring its own (a simulated venue has no
        #: limit to protect, and the default envelope would make a rehearsal last many minutes)
        self._rate_budget = rate_budget if rate_budget is not None else RateBudget(db)
        self._sleep = sleep
        self._http, self._external_done, self._ran = [], False, False
        self.faults = HTTPFaultInjector(self._transport_factory(), permit.venue, signing_secret=auth.api_secret,
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
        http = BudgetedHTTP(self._rate_budget, self.permit.venue, account_ref=self.auth.account_id,
                            server_address=os.environ.get("CATI_EGRESS_SCOPE", "shared-runtime-egress"),
                            transport=transport if transport is not None else self._transport_factory(),
                            clock=self.clock)
        self._http.append(http)
        client = build_client_from_auth(self.auth, http=http)
        client._production_db, client._production_account_id = self.db, self.auth.account_id
        return _PacedAdapter(adapter_for(client, self.permit.venue, self.permit.environment, clock=self.clock),
                             self._sleep)

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
        # Journalled, but NOT as one of the run's own orders: it stands for a change made outside the run, which
        # the runner accounts for separately (reconciliation subtracts it; flatten closes only what the run's own
        # orders left). Recording it as an own order counted it twice (found by the mocked end-to-end run).
        return self.runner._mutate(
            "EXTERNAL_REDUCE", None, {"quantity": quantity, "reduce_only": True,
                                      "external_client_order_id": request.client_order_id},
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


def evidence_document(db, result, *, mocked, permit=None, source_commit=None):
    """The evidence of one run, as one JSON-ready document: the outcome the register derived, every check with
    its evidence, every INTENT/OUTCOME action, the hash-chained events, and the permit's references (never a
    credential, a signature or a stream address). ``mocked`` says whether a venue was contacted at all."""
    document = {
        "mocked_transport": bool(mocked), "venue_contacted": not mocked, "venue": result.venue,
        "environment": result.environment, "run_id": result.run_id, "outcome": result.outcome,
        "recorded_status": result.recorded_status, "operations": list(result.operations),
        "failure_reasons": list(result.failure_reasons), "cleanup_complete": result.cleanup_complete,
        "checks": {k: dict(v) for k, v in result.checks.items()}, "source_commit": source_commit,
        "register": json.loads(json.dumps(CertificationRegister(db).run_evidence(result.run_id), default=str))}
    if permit is not None:
        document["permit"] = {k: getattr(permit, k) for k in (
            "operator_ref", "owner_authorization_ref", "venue", "environment", "account_ref", "authorized_scope",
            "symbol", "max_notional", "issued_at_ms", "expires_at_ms", "allow_orders", "leverage")}
    return document
