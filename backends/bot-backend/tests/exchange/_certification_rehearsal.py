"""A complete certification run over a mocked transport (test support and ``--dry-run`` of
``scripts/run_venue_certification.py``; no network, ever).

``rehearse(venue, db)`` runs ``CertificationHarness`` exactly as an operator's DEMO run would -- the real raw
client of the venue, the real adapter, ``BudgetedHTTP``, ``HTTPFaultInjector``, the runner, the register,
the entry permit and the canonical runtime lease -- with one difference: every HTTP request goes to a
``WireTransport`` over the venue's test wire and an in-memory ``FakeVenue``. The behaviour behind the wire is
the fake's, so a green rehearsal proves the harness and the adapter's translation, never a venue.

The transport also does what every one of the three venues documents and the wires do not: it refuses a
signed request whose timestamp is outside the receive window (Binance -1021, Bybit 10002, BingX 100421), so
the CLOCK_DRIFT check is exercised as it would be against a venue.
"""
from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import parse_qsl, urlsplit

from _platform_adapter_wire import BTC, ETH, SECRET, SECRET_KEY, WIRES, Reply, Wire

from app.exchange.platform_adapter import contract as C
from app.exchange.platform_adapter.certification import OperatorPermit
from app.exchange.platform_adapter.certification_harness import CertificationHarness, evidence_document
from app.exchange.platform_adapter.fake import FakeVenue
from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url

#: the receive window each venue documents by default (ms)
RECV_WINDOW_MS = 5_000
_TIMESTAMP_REFUSAL = {
    C.BINANCE: lambda: Reply(400, {"code": -1021, "msg": "Timestamp for this request is outside of the recvWindow."}),
    C.BYBIT: lambda: Reply(200, {"retCode": 10002, "retMsg": "The request time exceeds the time window range.",
                                 "result": {}}),
    C.BINGX: lambda: Reply(200, {"code": 100421, "msg": "Null timestamp or timestamp mismatch with server time"}),
}


class WireTransport:
    """``requests.Session``-shaped: ``request(method, url, **kwargs)`` and ``close()``, served by a test wire."""

    def __init__(self, wire: Wire, clock: Callable[[], int]) -> None:
        self.wire, self.clock = wire, clock
        self.requests = 0

    def _stamp(self, url: str, kwargs: Dict[str, Any]) -> Optional[int]:
        headers = kwargs.get("headers") or {}
        if "X-BAPI-TIMESTAMP" in headers:
            return int(headers["X-BAPI-TIMESTAMP"])
        fields = dict(parse_qsl(urlsplit(url).query))
        if isinstance(kwargs.get("data"), str):
            fields.update(parse_qsl(kwargs["data"]))
        return int(fields["timestamp"]) if "timestamp" in fields else None

    def request(self, method: str, url: str, **kwargs: Any) -> Reply:
        self.requests += 1
        assert urlsplit(url).scheme == "https" and urlsplit(url).hostname == urlsplit(
            resolve_base_url(self.wire.venue, BrokerEnvironment.DEMO)).hostname, url   # the DEMO host, nothing else
        stamp = self._stamp(url, kwargs)
        if stamp is not None and abs(stamp - self.clock()) > RECV_WINDOW_MS:
            return _TIMESTAMP_REFUSAL[self.wire.venue]()
        method, headers = method.upper(), kwargs.get("headers")
        if self.wire.venue == C.BINANCE:
            return self.wire.request(method, url, kwargs.get("params"), headers)
        if method == "GET":
            return self.wire.get(url, headers=headers)
        if method == "POST":
            return self.wire.post(url, data=kwargs.get("data"), headers=headers)
        if method == "DELETE":
            return self.wire.delete(url, headers=headers)
        raise AssertionError(f"unexpected method {method}")

    def close(self) -> None:
        pass


def rehearsal_budget(db: Any, venue: str) -> Any:
    """The persistent budget with a capacity no rehearsal reaches: a simulated venue has no limit to protect.
    The admission, the 429 closure and the pacing are the real code; only the envelope is wider."""
    from shared_lib.broker.rate_budget import REQUESTS, UNVERIFIED, WEIGHT, RateBudget, VenueLimit
    unit = WEIGHT if venue == C.BINANCE else REQUESTS
    return RateBudget(db, limits={venue: VenueLimit(venue, 1_000_000.0, 60_000, unit, UNVERIFIED,
                                                    "mocked-transport rehearsal: not a venue limit")})


def rehearsal_fake(venue: str, clock: Callable[[], int]) -> FakeVenue:
    fake = FakeVenue(venue, environment=C.DEMO, default_leverage=5, clock=clock)
    fake.add_instrument(BTC, price=50_000.0, price_tick=0.1, quantity_step=0.001, min_notional=5.0)
    fake.add_instrument(ETH, price=2_500.0, price_tick=0.01, quantity_step=0.01, min_notional=5.0)
    return fake


def synthetic_auth(venue: str, account_ref: str) -> SimpleNamespace:
    """A resolved-credential stand-in for the mocked run (the wire never checks a signature)."""
    return SimpleNamespace(account_id=account_ref, broker_type=venue, environment=BrokerEnvironment.DEMO,
                           user_id="rehearsal-user", api_key=SECRET_KEY, api_secret=SECRET, key_fingerprint="synthetic",
                           base_url=resolve_base_url(venue, BrokerEnvironment.DEMO), database=None)


def rehearsal_permit(venue: str, account_ref: str, *, symbol: str = BTC, max_notional: float = 500.0,
                     allow_orders: bool = True) -> OperatorPermit:
    now = int(time.time() * 1000)
    return OperatorPermit("rehearsal-operator", "REHEARSAL-NO-OWNER-AUTHORIZATION", venue, C.DEMO, account_ref,
                          "mocked-transport rehearsal; no venue contacted", symbol, max_notional, now - 1_000,
                          now + 600_000, allow_orders=allow_orders)


def rehearse(venue: str, db: Any, *, source_commit: str = "rehearsal", allow_orders: bool = True,
             fake_setup: Optional[Callable[[FakeVenue], None]] = None) -> Tuple[Any, Dict[str, Any], FakeVenue, Wire]:
    """Run the full catalogue for ``venue`` over a mocked transport. Holds the canonical runtime lease of ``db``
    for the run, as a real run must. Returns ``(result, evidence, fake, wire)``."""
    from app.ops.runtime_ownership import RuntimeOwnership

    clock = lambda: int(time.time() * 1000)                              # noqa: E731
    fake = rehearsal_fake(venue, clock)
    if fake_setup is not None:
        fake_setup(fake)
    wire = WIRES[venue](fake)
    account_ref = f"rehearsal-{venue}-account"
    lease = RuntimeOwnership(db, database_path=db.path, database_role="test")
    acquired = lease.acquire()
    assert acquired.acquired, acquired.reason
    try:
        harness = CertificationHarness(db, synthetic_auth(venue, account_ref),
                                       rehearsal_permit(venue, account_ref, allow_orders=allow_orders),
                                       source_commit=source_commit,
                                       transport_factory=lambda: WireTransport(wire, clock),
                                       rate_budget=rehearsal_budget(db, venue))
        result = harness.run()
    finally:
        lease.release(reason="REHEARSAL_DONE")
    return result, evidence_document(db, result, mocked=True), fake, wire
