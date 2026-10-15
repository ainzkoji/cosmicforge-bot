"""Persistence of the shadow book (Section H, Steps 3.1 and 3.6).

A shadow book is what a family would hold for an account if its decisions were filled as its frozen rules
say. It is a record of intent, never of an exchange position: nothing in these tables is read by any code
that sends an order, and no table of the execution path is written here.

Every function takes the caller's connection. The cycle driver moves one account through one day inside one
``BEGIN IMMEDIATE`` transaction (book, fills, the day's figures, the immutable decision and the account's
cycle row together), so a process that dies in the middle leaves the account exactly where it was and the
replayed cycle produces the same rows once.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional

from app.trading_intelligence.hashing import short_id
from shared_lib.persistence.cati_platform_schema import (
    SHADOW_ACCOUNT_TABLE,
    SHADOW_DAY_TABLE,
    SHADOW_FILL_TABLE,
    SHADOW_POSITION_TABLE,
)

from .daily_trend import Book, Position


def load(conn: Any, account_id: str, family_id: str) -> Optional[Book]:
    row = conn.execute(f"SELECT * FROM {SHADOW_ACCOUNT_TABLE} WHERE account_id=? AND family_id=?",
                       (account_id, family_id)).fetchone()
    if row is None:
        return None
    row = dict(row)
    book = Book(risk_level=row["risk_level"], policy=row["policy"], initial_equity=float(row["initial_equity"]),
                equity=float(row["equity"]), peak=float(row["peak_equity"]), first_day=int(row["first_day"]),
                last_day=None if row["last_day"] is None else int(row["last_day"]), paused=bool(row["paused"]),
                halved=bool(row["halved"]), halted=bool(row["halted"]), pending=json.loads(row["pending_orders"]))
    # rowid order is the order the positions were written in, which is the order they were opened in
    for p in conn.execute(f"SELECT * FROM {SHADOW_POSITION_TABLE} WHERE account_id=? AND family_id=? ORDER BY rowid",
                          (account_id, family_id)):
        p = dict(p)
        book.positions[p["symbol"]] = Position(
            quantity=float(p["quantity"]), stop=float(p["stop_price"]), mark=float(p["mark_price"]),
            entry_day=int(p["entry_day"]), entry_price=float(p["entry_price"]),
            entry_stop_distance=float(p["entry_stop_distance"]))
    return book


def save(conn: Any, account_id: str, family_id: str, book: Book, *, cycle_id: str, now_ms: int) -> None:
    conn.execute(
        f"INSERT INTO {SHADOW_ACCOUNT_TABLE} (account_id, family_id, risk_level, policy, initial_equity, equity, "
        "peak_equity, paused, halved, halted, first_day, last_day, pending_orders, updated_cycle, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id, family_id) DO UPDATE SET "
        "equity=excluded.equity, peak_equity=excluded.peak_equity, paused=excluded.paused, halved=excluded.halved, "
        "halted=excluded.halted, last_day=excluded.last_day, pending_orders=excluded.pending_orders, "
        "updated_cycle=excluded.updated_cycle, updated_at=excluded.updated_at",
        (account_id, family_id, book.risk_level, book.policy, book.initial_equity, book.equity, book.peak,
         int(book.paused), int(book.halved), int(book.halted), book.first_day, book.last_day,
         json.dumps(book.pending, sort_keys=True), cycle_id, int(now_ms)))
    conn.execute(f"DELETE FROM {SHADOW_POSITION_TABLE} WHERE account_id=? AND family_id=?", (account_id, family_id))
    conn.executemany(
        f"INSERT INTO {SHADOW_POSITION_TABLE} (account_id, family_id, symbol, quantity, stop_price, mark_price, "
        "entry_day, entry_price, entry_stop_distance, updated_cycle) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [(account_id, family_id, s, p.quantity, p.stop, p.mark, p.entry_day, p.entry_price, p.entry_stop_distance,
          cycle_id) for s, p in book.positions.items()])


def record_day(conn: Any, account_id: str, family_id: str, *, cycle_id: str, result: Mapping[str, Any],
               now_ms: int) -> None:
    """The fills and the figures of one accounted day (both append-only)."""
    day = int(result["day"])
    conn.executemany(
        f"INSERT INTO {SHADOW_FILL_TABLE} (fill_id, cycle_id, account_id, family_id, day, symbol, kind, quantity, "
        "price, recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [(short_id("sfil", {"a": account_id, "f": family_id, "d": day, "n": n}), cycle_id, account_id, family_id, day,
          f["symbol"], f["kind"], float(f["quantity"]), float(f["price"]), int(now_ms))
         for n, f in enumerate(result["fills"])])
    document = {"events": result["events"], "rejections": result["rejections"], "sums": result["sums"]}
    conn.execute(
        f"INSERT INTO {SHADOW_DAY_TABLE} (account_id, family_id, day, cycle_id, equity, daily_return, drawdown, "
        "positions, gross_notional, paused, halved, halted, document, recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (account_id, family_id, day, cycle_id, float(result["equity"]), float(result["return"]),
         float(result["drawdown"]), int(result["positions"]), float(result["gross_notional"]), int(result["paused"]),
         int(result["halved"]), int(result["halted"]), json.dumps(document, sort_keys=True), int(now_ms)))


def days(conn: Any, account_id: str, family_id: str) -> List[Dict[str, Any]]:
    return [dict(r) for r in conn.execute(
        f"SELECT * FROM {SHADOW_DAY_TABLE} WHERE account_id=? AND family_id=? ORDER BY day", (account_id, family_id))]


def fills(conn: Any, account_id: str, family_id: str) -> List[Dict[str, Any]]:
    return [dict(r) for r in conn.execute(
        f"SELECT * FROM {SHADOW_FILL_TABLE} WHERE account_id=? AND family_id=? ORDER BY day, rowid",
        (account_id, family_id))]


__all__ = ["load", "save", "record_day", "days", "fills"]
