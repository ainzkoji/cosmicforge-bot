"""Position-slot occupancy and atomic slot reservation.

``max_open_positions`` limits how many economic positions a bot holds. A slot
is taken by anything that is, or can become, exposure at the broker:

    OPEN ledger positions        (placed by the bot or adopted by reconciliation)
    pending_entries rows          (in-flight intents, including orders whose
                                   broker state is still unresolved)

deduplicated by economic identity: the symbol in ONE_WAY mode, symbol+side in
HEDGE mode. CLOSED rows never count.

The check and the reservation must be one step. Two symbols evaluated at the
same time would otherwise both see the last free slot and both submit. So
``reserve_entry_slot`` runs the check and the durable reservation -- the
EntryProtection intent that writes the pending_entries row -- under one lock
per bot. The row is released by EntryProtection (mark_failed / mark_closed) and
is kept while a submitted order is unresolved, so an order of unknown outcome
never frees its slot early.

Slot reservation is not capital reservation: this is about how many positions
may exist; AccountMarginReservations is about whether the account can fund one.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable

from app.decision.reasons import RiskReason

_GUARD = threading.Lock()
_LOCKS: dict[str, threading.Lock] = {}


def slot_lock(bot_id: str) -> threading.Lock:
    with _GUARD:
        return _LOCKS.setdefault(str(bot_id), threading.Lock())


def slot_key(symbol: str, side: str | None, hedge_mode: bool) -> str:
    sym = str(symbol or "").upper()
    return f"{sym}:{str(side or '').upper()}" if hedge_mode else sym


def _rows(conn: Any, sql: str, params: tuple) -> list:
    try:
        return conn.execute(sql, params).fetchall()
    except Exception as exc:  # a missing table is "nothing there", anything else is not
        if "no such table" in str(exc).lower():
            return []
        raise


def occupied_slots(db: Any, bot_id: str, *, hedge_mode: bool = False) -> dict[str, set[str]]:
    """Slot keys held by open positions and by in-flight entries."""
    with db.connect() as conn:
        positions = _rows(
            conn,
            "SELECT symbol, side FROM positions WHERE bot_instance_id=? AND status='OPEN'",
            (bot_id,),
        )
        pending = _rows(conn, "SELECT symbol, side FROM pending_entries WHERE bot_id=?", (bot_id,))
    return {
        "positions": {slot_key(r[0], r[1], hedge_mode) for r in positions},
        "pending": {slot_key(r[0], r[1], hedge_mode) for r in pending},
    }


@dataclass(frozen=True)
class SlotVerdict:
    allowed: bool
    reason: str
    slot_key: str
    occupied: tuple[str, ...]
    max_slots: int

    @property
    def available(self) -> int:
        return max(0, self.max_slots - len(self.occupied))

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "slot_key": self.slot_key,
            "occupied": list(self.occupied),
            "occupied_count": len(self.occupied),
            "max_position_slots": self.max_slots,
            "available_position_slots": self.available,
        }


def evaluate_slot(db: Any, bot_id: str, symbol: str, side: str, max_slots: int, *,
                  hedge_mode: bool = False) -> SlotVerdict:
    key = slot_key(symbol, side, hedge_mode)
    held = occupied_slots(db, bot_id, hedge_mode=hedge_mode)
    occupied = tuple(sorted(held["positions"] | held["pending"]))
    if key in occupied:
        # The symbol already holds its slot (a flip, or an add guarded elsewhere).
        return SlotVerdict(True, "SLOT_ALREADY_HELD", key, occupied, int(max_slots))
    if len(occupied) >= int(max_slots):
        return SlotVerdict(False, RiskReason.MAX_OPEN_POSITIONS, key, occupied, int(max_slots))
    return SlotVerdict(True, RiskReason.APPROVED, key, occupied, int(max_slots))


def reserve_entry_slot(
    db: Any,
    bot_id: str,
    symbol: str,
    side: str,
    max_slots: int,
    *,
    hedge_mode: bool = False,
    acquire: Callable[[], Any] | None = None,
) -> tuple[SlotVerdict, Any]:
    """Check the slot and create the durable reservation under one lock.

    ``acquire`` writes the reservation (EntryProtection.acquire_intent). It runs
    inside the lock only when the slot is available, so the last free slot can
    be taken by exactly one entry.
    """
    with slot_lock(bot_id):
        verdict = evaluate_slot(db, bot_id, symbol, side, max_slots, hedge_mode=hedge_mode)
        if not verdict.allowed:
            return verdict, None
        return verdict, (acquire() if acquire is not None else None)


def slot_diagnostics(db: Any, bot_id: str, max_slots: int, *, hedge_mode: bool = False) -> dict[str, Any]:
    held = occupied_slots(db, bot_id, hedge_mode=hedge_mode)
    occupied = held["positions"] | held["pending"]
    with db.connect() as conn:
        unresolved = _rows(
            conn,
            "SELECT COUNT(*) FROM pending_entries WHERE bot_id=? AND submit_state='SUBMIT_UNKNOWN'",
            (bot_id,),
        )
        columns = {r[1] for r in _rows(conn, "PRAGMA table_info(execution_attempts)", ())}
        last = None
        if "fill_resolution_status" in columns:
            last = conn.execute(
                "SELECT fill_resolution_status, completed_at FROM execution_attempts "
                "WHERE bot_instance_id=? AND fill_resolution_status IS NOT NULL "
                "ORDER BY completed_at DESC LIMIT 1",
                (bot_id,),
            ).fetchone()
    return {
        "max_position_slots": int(max_slots),
        "economic_open_position_count": len(held["positions"]),
        "pending_entry_count": len(held["pending"] - held["positions"]),
        "occupied_position_slots": len(occupied),
        "available_position_slots": max(0, int(max_slots) - len(occupied)),
        "unresolved_broker_orders": int(unresolved[0][0]) if unresolved else 0,
        "last_fill_resolution_status": last[0] if last else None,
        "last_fill_resolution_at": last[1] if last else None,
        "slot_identity": "SYMBOL_SIDE" if hedge_mode else "SYMBOL",
    }
