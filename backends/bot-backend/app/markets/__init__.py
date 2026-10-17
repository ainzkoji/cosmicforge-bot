"""Broker-neutral market calendars and contract economics (Section H, Step 7.2).

Foundations only. Nothing in this package places, sizes or authorizes an order on its own, and nothing in it
reads a family's rules. It answers four questions from versioned data files:

* ``calendar``: is a market open at an instant, which session is that, when does it next open, and which bars
  are expected over a period (never a bar for a closed period);
* ``contracts`` / ``roll``: what a contract is (multiplier, tick, quantity rules, margin, expiry, carry) and
  which contract of a dated series is the one that may be traded on a date;
* ``economics``: notional, PnL in the settlement currency, financing / carry and dividend adjustments, in
  ``Decimal``;
* ``guard``: why an order for a market may not be submitted now (closed, contract expired or not listed,
  calendar unverified, data stale).

``registry`` loads the definitions from ``app/markets/data/``. Every definition carries a ``source`` and a
``verification_status``; the traditional-market definitions shipped here are ``UNVERIFIED`` examples, and the
guard refuses an unverified calendar for submission. The 24/7 crypto calendar preserves the existing
behaviour: crypto perpetual markets are always open.

``activation`` is the staged (PAPER -> DEMO -> LIVE) activation ledger per (market, family); ``availability``
is what the portal is told about each market.
"""
