"""REST reconciliation of one connected account: the venue's own answer about orders, positions and fills
(Section H, Step 4.4).

A push stream is fast and can be wrong in every way a connection can be: it drops a frame, repeats one, stops
without saying so, or starts after the thing it should have reported. So the stream is only ever a hint. What
the account's state IS comes from here: one pass reads the open orders, the positions and the fills since the
last pass through the ``VenueAdapter``, and the result replaces whatever a stream suggested.

* **No transaction spans a venue call.** Everything is read first; one transaction then writes the fills,
  the cursors and the record of the pass.
* **All or nothing.** If any read fails the pass fails: no cursor moves, no state is returned, and a refusal
  by the venue or by the local budget carries the instant before which nothing is tried again.
* **A fill is counted once.** The ledger is keyed by venue, account, symbol and trade id. The symbol is part
  of the key because Binance numbers trades per symbol: the same trade id on two symbols of one account is
  two fills. A fill seen on the stream and then read from the venue is the same fill; where the two disagree
  the venue's record is the one that is read, and the disagreement is reported.
* **Cursors are per symbol and durable**, and every pass reads again from a little before its cursor, so a
  fill the venue published late is not lost and a restart neither loses nor repeats one.
* **The log records what a pass found.** Every pass that failed, found a new fill or a difference, or was
  started by something other than a timer or a hint is written to ``cati_user_stream_reconciliations``. A
  routine pass that found nothing is not: the table is append-only and would otherwise grow with the clock.

Scope. The contract reads fills per symbol, so a pass covers the symbols the caller trades for the account,
every symbol with an open order or a position now or in the state the caller held before, and every symbol a
stream frame mentioned. A fill on a symbol outside all of these is not seen by this pass.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.exchange.platform_adapter import contract as C
from app.trading_intelligence.hashing import short_id

from shared_lib.persistence.cati_exchange_schema import STREAM_CURSOR_TABLE, STREAM_FILL_TABLE, STREAM_RECONCILE_TABLE

STREAM, REST = "STREAM", "REST"
NEW_FILL, DUPLICATE_FILL, CONFIRMED_FILL, CORRECTED_FILL = "NEW", "DUPLICATE", "CONFIRMED", "CORRECTED"

CONNECT, RECONNECT, RESTART, GAP, UNKNOWN_ORDER, STREAM_DOWN, HINT, PERIODIC, MANUAL = (
    "CONNECT", "RECONNECT", "RESTART", "GAP", "UNKNOWN_ORDER", "STREAM_DOWN", "HINT", "PERIODIC", "MANUAL")
TRIGGERS: Tuple[str, ...] = (CONNECT, RECONNECT, RESTART, GAP, UNKNOWN_ORDER, STREAM_DOWN, HINT, PERIODIC, MANUAL)
#: triggers that fire on a timer or on any frame. A pass they start is written to the log only when it found
#: something (a fill, a difference, a failure): the log is append-only and must not grow with the clock.
ROUTINE_TRIGGERS: Tuple[str, ...] = (STREAM_DOWN, HINT, PERIODIC)
OK, FAILED = "OK", "FAILED"

#: every pass reads fills again from this long before its cursor (late publication, clock difference)
DEFAULT_CURSOR_OVERLAP_MS = 60_000
#: how many differences one pass writes into its record (all of them are returned to the caller)
RECORDED_DIFFERENCES = 50

#: the row that is read for a fill: the venue's own record, else the stream's while it is the only one
_EFFECTIVE =(f"(f.source='{REST}' OR NOT EXISTS (SELECT 1 FROM {STREAM_FILL_TABLE} r WHERE r.venue=f.venue AND "
              f"r.account_ref=f.account_ref AND r.symbol=f.symbol AND r.trade_id=f.trade_id AND r.source='{REST}'))")


def order_key(venue_order_id: Optional[str], client_order_id: Optional[str]) -> str:
    """One identity for an order whichever side reported it: the venue's id, else the client's."""
    if venue_order_id:
        return str(venue_order_id)
    if client_order_id:
        return f"client:{client_order_id}"
    raise C.ContractViolation("an order without any identity cannot be reconciled")


@dataclass(frozen=True)
class OrderView:
    symbol: str
    client_order_id: Optional[str]
    venue_order_id: Optional[str]
    status: str
    filled_quantity: Optional[float]
    updated_at_ms: int
    source: str                         # REST = the venue's answer; STREAM = a hint not yet confirmed

    @property
    def key(self) -> str:
        return order_key(self.venue_order_id, self.client_order_id)


@dataclass(frozen=True)
class PositionView:
    symbol: str
    side: str
    quantity: float
    entry_price: Optional[float]
    updated_at_ms: int
    source: str


@dataclass(frozen=True)
class AccountState:
    """Open orders by order key and non-flat positions by symbol of one account."""
    venue: str
    environment: str
    account_ref: str
    open_orders: Mapping[str, OrderView] = field(default_factory=dict)
    positions: Mapping[str, PositionView] = field(default_factory=dict)
    observed_at_ms: int = 0

    def symbols(self) -> Tuple[str, ...]:
        return tuple(sorted({o.symbol for o in self.open_orders.values()} | set(self.positions)))


@dataclass(frozen=True)
class ReconcileResult:
    reconcile_id: str
    status: str                         # OK | FAILED
    trigger: str
    state: Optional[AccountState]       # the venue's answer; None when the pass failed
    new_fills: Tuple[C.FillInfo, ...] = ()
    duplicate_fills: int = 0
    confirmed_fills: int = 0
    differences: Tuple[str, ...] = ()   # where the venue's answer differed from what the caller held
    error_code: Optional[str] = None
    retry_at_ms: Optional[int] = None   # set when the venue or the budget said to wait
    started_at_ms: int = 0

    @property
    def ok(self) -> bool:
        return self.status == OK


class FillLedger:
    """The fills of one account, each counted once across stream, REST and restart."""

    def __init__(self, db: Any, *, venue: str, environment: str, account_ref: str) -> None:
        if not str(account_ref or "").strip():
            raise ValueError("a ledger belongs to one connected account")
        self.db, self.venue, self.environment, self.account_ref = db, venue, environment, str(account_ref)

    def record(self, conn: Any, fill: C.FillInfo, source: str, now_ms: int) -> str:
        """Enter one fill inside the caller's transaction. ``NEW`` exactly once per fill; ``DUPLICATE`` for a
        repeat; ``CONFIRMED`` / ``CORRECTED`` when the venue's record joins one first seen on the stream."""
        if source not in (STREAM, REST):
            raise ValueError(f"unknown fill source {source!r}")
        if fill.venue != self.venue:
            raise ValueError(f"a {fill.venue} fill does not belong in the {self.venue} ledger")
        key = (self.venue, self.account_ref, fill.symbol, str(fill.trade_id))
        seen = {row["source"]: row for row in conn.execute(
            f"SELECT source, side, quantity, price FROM {STREAM_FILL_TABLE} WHERE venue=? AND account_ref=? AND "
            "symbol=? AND trade_id=?", key).fetchall()}
        if source in seen or (source == STREAM and seen):
            return DUPLICATE_FILL
        conn.execute(
            f"INSERT INTO {STREAM_FILL_TABLE} (venue, account_ref, symbol, trade_id, source, environment, "
            "venue_order_id, client_order_id, side, quantity, price, fee, fee_asset, realized_pnl, is_maker, time_ms, "
            "recorded_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (*key, source, self.environment, fill.venue_order_id, fill.client_order_id, fill.side,
             float(fill.quantity), float(fill.price), fill.fee, fill.fee_asset, fill.realized_pnl,
             None if fill.is_maker is None else int(fill.is_maker), int(fill.time_ms), int(now_ms)))
        if not seen:
            return NEW_FILL
        hinted = seen[STREAM]
        same = (hinted["side"] == fill.side and float(hinted["quantity"]) == float(fill.quantity)
                and float(hinted["price"]) == float(fill.price))
        return CONFIRMED_FILL if same else CORRECTED_FILL

    def fills(self, *, symbol: Optional[str] = None, since_ms: Optional[int] = None) -> Tuple[C.FillInfo, ...]:
        """Every fill once, oldest first; the venue's own record where both exist."""
        sql = f"SELECT f.* FROM {STREAM_FILL_TABLE} f WHERE f.venue=? AND f.account_ref=? AND {_EFFECTIVE}"
        args: List[Any] = [self.venue, self.account_ref]
        if symbol is not None:
            sql += " AND f.symbol=?"
            args.append(C.canonical_symbol(symbol))
        if since_ms is not None:
            sql += " AND f.time_ms>=?"
            args.append(int(since_ms))
        with self.db.connect() as conn:
            rows = conn.execute(sql + " ORDER BY f.time_ms, f.symbol, f.trade_id", args).fetchall()
        return tuple(C.FillInfo(
            venue=row["venue"], symbol=row["symbol"], trade_id=row["trade_id"], venue_order_id=row["venue_order_id"],
            client_order_id=row["client_order_id"], side=row["side"], quantity=row["quantity"], price=row["price"],
            fee=row["fee"], fee_asset=row["fee_asset"], realized_pnl=row["realized_pnl"],
            is_maker=None if row["is_maker"] is None else bool(row["is_maker"]), time_ms=row["time_ms"])
            for row in rows)

    def filled_quantity(self, symbol: str, venue_order_id: str) -> float:
        """What the ledger holds for one order: the sum a cumulative figure from the venue is compared with."""
        with self.db.connect() as conn:
            row = conn.execute(
                f"SELECT COALESCE(SUM(f.quantity), 0) FROM {STREAM_FILL_TABLE} f WHERE f.venue=? AND f.account_ref=? "
                f"AND f.symbol=? AND f.venue_order_id=? AND {_EFFECTIVE}",
                (self.venue, self.account_ref, symbol, str(venue_order_id))).fetchone()
        return float(row[0] or 0.0)

    def cursors(self) -> Dict[str, int]:
        with self.db.connect() as conn:
            rows = conn.execute(f"SELECT symbol, cursor_ms FROM {STREAM_CURSOR_TABLE} WHERE venue=? AND account_ref=?",
                                (self.venue, self.account_ref)).fetchall()
        return {row[0]: int(row[1]) for row in rows}


def _differences(previous: AccountState, current: AccountState) -> List[str]:
    out = [f"ORDER_NOT_OPEN_AT_VENUE:{key}" for key in sorted(set(previous.open_orders) - set(current.open_orders))]
    out += [f"OPEN_ORDER_NOT_HELD_LOCALLY:{key}" for key in sorted(set(current.open_orders) - set(previous.open_orders))]
    for symbol in sorted(set(previous.positions) | set(current.positions)):
        before, after = previous.positions.get(symbol), current.positions.get(symbol)
        if (before is None) != (after is None) or (before is not None and after is not None and (
                before.side != after.side or float(before.quantity) != float(after.quantity))):
            out.append(f"POSITION_DIFFERS:{symbol}")
    return out


class RestReconciler:
    def __init__(self, db: Any, adapter: Any, *, account_ref: str, symbols: Sequence[str] = (),
                 cursor_overlap_ms: int = DEFAULT_CURSOR_OVERLAP_MS) -> None:
        self.db, self.adapter = db, adapter
        self.ledger = FillLedger(db, venue=adapter.venue, environment=adapter.environment, account_ref=account_ref)
        self.symbols = tuple(sorted({C.canonical_symbol(s) for s in symbols}))
        self.cursor_overlap_ms = int(cursor_overlap_ms)

    def run(self, now_ms: int, *, trigger: str, extra_symbols: Sequence[str] = (),
            previous: Optional[AccountState] = None) -> ReconcileResult:
        """One pass. ``previous`` is what the caller believed (a stream's hints included); the result says
        where the venue differs and the returned state is the venue's."""
        if trigger not in TRIGGERS:
            raise ValueError(f"unknown reconciliation trigger {trigger!r}")
        now_ms = int(now_ms)
        venue, environment, account_ref = self.ledger.venue, self.ledger.environment, self.ledger.account_ref
        cursors = self.ledger.cursors()
        try:  # every venue read happens here, with no transaction open
            orders = tuple(self.adapter.open_orders())
            positions = tuple(self.adapter.positions())
            if not all(isinstance(o, C.OrderInfo) for o in orders) or not all(
                    isinstance(p, C.PositionInfo) for p in positions):
                raise C.AdapterError(C.MALFORMED_RESPONSE, "not contract values", venue=venue)
            scope = sorted(set(self.symbols) | {C.canonical_symbol(s) for s in extra_symbols}
                           | {o.symbol for o in orders} | {p.symbol for p in positions if p.side != C.FLAT}
                           | set(() if previous is None else previous.symbols()))
            fetched: Dict[str, Tuple[C.FillInfo, ...]] = {}
            for symbol in scope:
                cursor = cursors.get(symbol)
                since_ms = None if cursor is None else max(0, cursor - self.cursor_overlap_ms)
                fetched[symbol] = tuple(self.adapter.fills(symbol, since_ms=since_ms))
                if not all(isinstance(f, C.FillInfo) and f.symbol == symbol for f in fetched[symbol]):
                    raise C.AdapterError(C.MALFORMED_RESPONSE, "not the fills of the symbol asked for", venue=venue)
        except C.AdapterError as error:
            retry_at_ms = error.banned_until_ms
            if error.retry_after_s is not None:
                retry_at_ms = max(retry_at_ms or 0, now_ms + int(float(error.retry_after_s) * 1000) + 1)
            return self._record(ReconcileResult("", FAILED, trigger, None, error_code=error.code,
                                                retry_at_ms=retry_at_ms, started_at_ms=now_ms), {}, now_ms)
        state = AccountState(
            venue, environment, account_ref,
            open_orders={order_key(o.venue_order_id, o.client_order_id): OrderView(
                o.symbol, o.client_order_id, o.venue_order_id, o.status, float(o.filled_quantity),
                int(o.updated_at_ms), REST) for o in orders},
            positions={p.symbol: PositionView(p.symbol, p.side, float(p.quantity), p.entry_price, now_ms, REST)
                       for p in positions if p.side != C.FLAT},
            observed_at_ms=now_ms)
        differences = [] if previous is None else _differences(previous, state)
        new_fills: List[C.FillInfo] = []
        counts = {DUPLICATE_FILL: 0, CONFIRMED_FILL: 0}
        new_cursors: Dict[str, int] = {}
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for symbol in scope:
                for fill in sorted(fetched[symbol], key=lambda f: (int(f.time_ms), str(f.trade_id))):
                    outcome = self.ledger.record(conn, fill, REST, now_ms)
                    if outcome == NEW_FILL:
                        new_fills.append(fill)
                        if previous is not None:
                            differences.append(f"FILL_NOT_SEEN_ON_STREAM:{symbol}:{fill.trade_id}")
                    elif outcome == CORRECTED_FILL:
                        differences.append(f"FILL_CORRECTED_BY_VENUE:{symbol}:{fill.trade_id}")
                    else:
                        counts[outcome] += 1
                latest = max((int(f.time_ms) for f in fetched[symbol]), default=0)
                new_cursors[symbol] = max(cursors.get(symbol, 0), latest, now_ms - self.cursor_overlap_ms)
                conn.execute(
                    f"INSERT INTO {STREAM_CURSOR_TABLE} (venue, account_ref, symbol, cursor_ms, updated_at_ms) "
                    "VALUES (?,?,?,?,?) ON CONFLICT(venue, account_ref, symbol) DO UPDATE SET "
                    "cursor_ms=excluded.cursor_ms, updated_at_ms=excluded.updated_at_ms",
                    (venue, account_ref, symbol, new_cursors[symbol], now_ms))
            result = ReconcileResult(
                "", OK, trigger, state, tuple(new_fills), counts[DUPLICATE_FILL], counts[CONFIRMED_FILL],
                tuple(differences), started_at_ms=now_ms)
            return self._record(result, {"symbols": scope, "cursors": new_cursors}, now_ms, conn)

    def _record(self, result: ReconcileResult, extra: Mapping[str, Any], now_ms: int,
                conn: Optional[Any] = None) -> ReconcileResult:
        """Name the pass and, unless it was a routine pass that found nothing, write its record (in the
        caller's transaction when there is one)."""
        if conn is None:
            with self.db.connect() as own:
                own.execute("BEGIN IMMEDIATE")
                return self._record(result, extra, now_ms, own)
        venue, environment, account_ref = self.ledger.venue, self.ledger.environment, self.ledger.account_ref
        ordinal = conn.execute(f"SELECT COUNT(*) FROM {STREAM_RECONCILE_TABLE} WHERE venue=? AND account_ref=?",
                               (venue, account_ref)).fetchone()[0]
        reconcile_id = short_id("recon", [venue, environment, account_ref, int(ordinal), now_ms, result.trigger])
        named = ReconcileResult(reconcile_id, result.status, result.trigger, result.state, result.new_fills,
                                result.duplicate_fills, result.confirmed_fills, result.differences, result.error_code,
                                result.retry_at_ms, result.started_at_ms)
        if result.ok and result.trigger in ROUTINE_TRIGGERS and not result.new_fills and not result.differences:
            return named
        document = {
            "open_orders": 0 if result.state is None else len(result.state.open_orders),
            "positions": 0 if result.state is None else len(result.state.positions),
            "new_fills": len(result.new_fills), "duplicate_fills": result.duplicate_fills,
            "confirmed_fills": result.confirmed_fills, "differences": list(result.differences[:RECORDED_DIFFERENCES]),
            "difference_count": len(result.differences), "retry_at_ms": result.retry_at_ms, **dict(extra)}
        conn.execute(
            f"INSERT INTO {STREAM_RECONCILE_TABLE} (reconcile_id, venue, environment, account_ref, trigger_reason, "
            "status, error_code, started_at_ms, completed_at_ms, document) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (reconcile_id, venue, environment, account_ref, result.trigger, result.status, result.error_code,
             result.started_at_ms, now_ms, json.dumps(document, sort_keys=True)))
        return named


__all__ = ["STREAM", "REST", "NEW_FILL", "DUPLICATE_FILL", "CONFIRMED_FILL", "CORRECTED_FILL", "CONNECT", "RECONNECT",
           "RESTART", "GAP", "UNKNOWN_ORDER", "STREAM_DOWN", "HINT", "PERIODIC", "MANUAL", "TRIGGERS",
           "ROUTINE_TRIGGERS", "OK", "FAILED",
           "DEFAULT_CURSOR_OVERLAP_MS", "order_key", "OrderView", "PositionView", "AccountState", "ReconcileResult",
           "FillLedger", "RestReconciler"]
