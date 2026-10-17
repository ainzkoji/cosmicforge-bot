"""Synthetic bridges between the platform worker layer and ``FakeVenue`` (tests and incident drills only).

``VenueExecutor`` maps the order-intent executor protocol onto the venue adapter contract the real adapters
implement; ``ProtectedBook`` is a minimal account maintenance/recovery pair: keep every open position covered by
exactly one resting whole-position stop, and on takeover reconcile durable intents before anything else.
Production order flow stays on the CATI execution boundary; only tests and ``app.golive.drills`` import this.
"""
from __future__ import annotations

import threading
from decimal import Decimal
from typing import Dict, List, Optional

from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.fake import FakeVenue
from app.platform_workers.jobs import SubmitNotReached, UnknownSubmitOutcome


class VenueExecutor:
    def __init__(self, venues: Dict[str, FakeVenue]) -> None:
        self.venues = venues
        self.submits = 0
        self.lock = threading.Lock()
        self.on_submit = None  # test hook: called inside submit, with no transaction open

    def submit(self, *, account_id, symbol, side, quantity, limit_price, client_order_id) -> str:
        with self.lock:
            self.submits += 1
        if self.on_submit is not None:
            self.on_submit()
        venue = self.venues[account_id]
        req = c.OrderRequest(symbol=symbol, side=side, order_type=c.LIMIT if limit_price else c.MARKET,
                             client_order_id=client_order_id, quantity=float(quantity),
                             price=float(limit_price) if limit_price else None,
                             time_in_force="GTC" if limit_price else None)
        try:
            return venue.place_order(req).venue_order_id
        except c.AdapterError as exc:
            if exc.uncertain:
                raise UnknownSubmitOutcome(exc.code) from exc
            if exc.code in c.RETRYABLE_CODES:
                raise SubmitNotReached(exc.code) from exc
            raise

    def lookup(self, *, account_id, symbol, client_order_id) -> Optional[str]:
        try:
            return self.venues[account_id].order(symbol, client_order_id=client_order_id).venue_order_id
        except c.AdapterError as exc:
            if exc.code == c.ORDER_NOT_FOUND:
                return None
            raise


def placed_orders(venue: FakeVenue, client_order_id: str) -> int:
    """How many place_order calls with this client id changed venue state. An idempotent repeat of an accepted
    id changes nothing, so a value above 1 is a duplicated order."""
    return sum(1 for e in venue.mutations if e.method == "place_order" and e.effect
               and e.arguments["request"].client_order_id == client_order_id)


class ProtectedBook:
    """maintain(): every open position has one resting whole-position stop 10% away.
    recover(): read the venue's positions and open orders and reconcile durable intents first."""

    def __init__(self, venues: Dict[str, FakeVenue], intents=None, executor=None) -> None:
        self.venues = venues
        self.intents = intents
        self.executor = executor
        self.recovered: List[str] = []
        self.maintained: List[str] = []
        self.pending_intents: Dict[str, List[str]] = {}
        self.hook = None
        self._lock = threading.Lock()

    def recover(self, ctx) -> None:
        venue = self.venues[ctx.account_id]
        venue.positions()
        venue.open_orders()
        for key in self.pending_intents.get(ctx.account_id, []):
            intent = self.intents.load(key)
            if intent.status == "SUBMITTING":
                self.intents.execute(key, self.executor, fence=ctx.fence)
        with self._lock:
            self.recovered.append(ctx.account_id)

    def maintain(self, ctx) -> None:
        if self.hook is not None:
            self.hook(ctx)
        venue = self.venues[ctx.account_id]
        for pos in venue.positions():
            if pos.side == c.FLAT or pos.quantity <= 0:
                continue
            stops = [o for o in venue.open_orders(pos.symbol) if o.order_type == c.STOP_MARKET]
            if stops:
                continue
            ctx.check()  # fencing: never place protection for an account this worker no longer owns
            price = Decimal(str(pos.mark_price or pos.entry_price))
            stop = float((price * Decimal("0.9")).quantize(Decimal("0.1")))
            venue.place_order(c.OrderRequest(symbol=pos.symbol, side=c.SELL, order_type=c.STOP_MARKET,
                                             client_order_id=f"stop-{ctx.account_id}-{pos.symbol}", stop_price=stop,
                                             close_position=True))
        with self._lock:
            self.maintained.append(ctx.account_id)
