"""Persistent, ACCOUNT-SCOPED CATI portfolio reservations (Section 16.23-16.28).

Three separate authorities -- never confused:

* CATI portfolio reservation (this module): portfolio-SELECTION consistency
  across bots sharing a broker account. SHADOW mode: production execution
  and risk never read it.
* production position-slot reservation (``app.execution.position_slots``):
  a bot's open-position capacity. Only READ here (to revalidate capacity),
  never written.
* account margin reservation (``app.risk.capital_ledger.
  AccountMarginReservations``): account funding. Never touched here.

Schema comes from the canonical migration
(``shared_lib.persistence.cati_schema``, run by ``migrate()``). This module
issues no DDL; a missing table fails closed with ``ReservationSchemaMissing``.

Reserve-time revalidation: inside one ``BEGIN IMMEDIATE`` transaction the
store re-reads open positions, pending entries and unexpired reservations
for the whole account plus the bot's current slot usage, and refuses the
reservation when the selection's assumptions are stale:

* ``ACCOUNT_RESERVATION_CONFLICT`` -- another active reservation claims an
  instrument, or the reservation set changed since selection
* ``DUPLICATE_EXPOSURE`` / ``EXPOSURE_CHANGED`` -- positions/pending entries
  changed since selection
* ``CAPACITY_CHANGED`` -- the bot's available slot count differs

Nothing is ever substituted: the caller must explicitly re-run selection.

Lifecycle: RESERVED -> CONSUMED | RELEASED | EXPIRED. Transitions are
compare-and-set from RESERVED only, so repeats are idempotent no-ops, and an
expired reservation can no longer be consumed.
"""
from __future__ import annotations

import contextlib
import json
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.execution.position_slots import occupied_slots, slot_key
from app.trading_intelligence.contracts.portfolio import AccountPortfolioReservation, ReservationStatus
from app.trading_intelligence.contracts.portfolio_intel import (
    ACCOUNT_RESERVATION_CONFLICT, CAPACITY_CHANGED, DUPLICATE_EXPOSURE, EXPOSURE_CHANGED,
)
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.portfolio.exposure_builder import (
    account_state_fingerprint, dedupe_reservations, load_open_and_pending, reservation_records,
)

TABLE = "cati_portfolio_reservations"
MODE_SHADOW = "SHADOW"

Selected = Tuple[str, str, str, str, str]  # (candidate_id, canonical, venue, venue_symbol, side)


class ReservationSchemaMissing(RuntimeError):
    """The canonical migration has not created the reservation table."""


@dataclass(frozen=True)
class ReservationOutcome:
    reservation: Optional[AccountPortfolioReservation]
    conflict_reason: Optional[str] = None
    conflicting: Tuple[str, ...] = ()

    @property
    def reserved(self) -> bool:
        return self.reservation is not None and self.conflict_reason is None


_LOCKS: Dict[str, threading.Lock] = {}
_GUARD = threading.Lock()


def _account_lock(account: str) -> threading.Lock:
    with _GUARD:
        return _LOCKS.setdefault(str(account), threading.Lock())


class _TxnDB:
    """Lets the existing slot authority's read helper run on the open
    transaction's connection (same snapshot, no commit/close)."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def connect(self):
        return contextlib.nullcontext(self._conn)


def _row_to_reservation(row: Any) -> AccountPortfolioReservation:
    return AccountPortfolioReservation(
        reservation_id=row["reservation_id"], broker_account_id=row["broker_account_id"], cycle_id=row["cycle_id"],
        selected_candidate_ids=tuple(json.loads(row["selected_candidate_ids"])),
        created_at=int(row["created_at"]), expires_at=int(row["expires_at"]),
        reservation_version=row["reservation_version"], status=row["status"], bot_instance_id=row["bot_instance_id"],
        selected_instruments=tuple(tuple(x) for x in json.loads(row["selected_instruments"])), mode=row["mode"],
    )


def available_slots_from(occupied: Dict[str, set], own_active: Sequence[AccountPortfolioReservation],
                         max_open_positions: int) -> int:
    """max_open - production-occupied slots - this bot's still-unrealised
    CATI reservations (a reservation already realised as a position or
    pending entry is not counted twice)."""
    used = set(occupied["positions"]) | set(occupied["pending"])
    reserved = {slot_key(vsym, side, False) for r in own_active for (_c, _v, vsym, side) in r.selected_instruments}
    return max(0, int(max_open_positions) - len(used) - len(reserved - used))


class CATIReservationStore:
    def __init__(self, db: Any) -> None:
        self._db = db
        self.assert_schema()

    def assert_schema(self) -> None:
        """Fail closed when the canonical migration has not run. No DDL here."""
        with self._db.connect() as conn:
            row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone()
        if row is None:
            raise ReservationSchemaMissing(f"{TABLE} is missing: run shared_lib.persistence.migrations.migrate()")

    # -- lifecycle -----------------------------------------------------------------
    @staticmethod
    def _expire(conn: Any, now_ms: int, account: Optional[str] = None) -> int:
        sql = f"UPDATE {TABLE} SET status='EXPIRED', updated_at=? WHERE status='RESERVED' AND expires_at<=?"
        params: list = [now_ms, now_ms]
        if account is not None:
            sql += " AND broker_account_id=?"
            params.append(account)
        return conn.execute(sql, params).rowcount

    def expire_stale(self, broker_account_id: str, now_ms: int) -> int:
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self._expire(conn, now_ms, broker_account_id)

    def cleanup_expired(self, now_ms: int) -> int:
        """Global, idempotent expiry sweep (marks; never deletes)."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return self._expire(conn, now_ms)

    @staticmethod
    def _active(conn: Any, account: str, now_ms: int) -> List[AccountPortfolioReservation]:
        rows = conn.execute(
            f"SELECT * FROM {TABLE} WHERE broker_account_id=? AND status='RESERVED' AND expires_at>? ORDER BY reservation_id",
            (account, now_ms),
        ).fetchall()
        return [_row_to_reservation(r) for r in rows]

    def reserve(
        self, *, broker_account_id: str, bot_instance_id: str, cycle_id: str, selected: Sequence[Selected],
        now_ms: int, ttl_seconds: int, allow_hedge_duplicates: bool = False, mode: str = MODE_SHADOW,
        expected_exposure_fingerprint: Optional[str] = None, expected_reservation_fingerprint: Optional[str] = None,
        expected_available_slots: Optional[int] = None,
        max_open_positions: Optional[int] = None, policy_version: Optional[str] = None,
        policy_hash: Optional[str] = None,
    ) -> ReservationOutcome:
        """Atomic revalidate-and-insert under the account lock. On any
        conflict NOTHING is substituted or mutated."""
        if mode != MODE_SHADOW:
            raise ValueError("only SHADOW reservations are permitted in this phase")
        candidate_ids = tuple(sorted(s[0] for s in selected))
        rid = short_id("resv", {"acct": broker_account_id, "bot": bot_instance_id, "cycle": cycle_id, "cands": list(candidate_ids)})
        instruments = tuple(sorted((s[1], s[2], s[3], s[4]) for s in selected))
        with _account_lock(broker_account_id):
            with self._db.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")  # account-scoped serialization point (DB-wide writer lock)
                self._expire(conn, now_ms, broker_account_id)
                existing = conn.execute(f"SELECT * FROM {TABLE} WHERE reservation_id=?", (rid,)).fetchone()
                if existing is not None and existing["status"] == ReservationStatus.RESERVED.value:
                    return ReservationOutcome(_row_to_reservation(existing))  # idempotent retry
                opens, pending = load_open_and_pending(conn, broker_account_id)
                active = self._active(conn, broker_account_id, now_ms)

                # 1) direct instrument clashes
                taken: Dict[str, Tuple[str, str]] = {}
                for rec in opens + pending:
                    taken.setdefault(rec.instrument_key.canonical_symbol, (DUPLICATE_EXPOSURE, rec.side))
                for r in active:
                    for canonical, _v, _s, side in r.selected_instruments:
                        taken.setdefault(canonical, (ACCOUNT_RESERVATION_CONFLICT, side))
                clashes = sorted({canonical for _cid, canonical, _v, _vs, side in selected
                                  if canonical in taken and not (allow_hedge_duplicates and taken[canonical][1] != side)})
                if clashes:
                    kinds = {taken[c][0] for c in clashes}
                    reason = ACCOUNT_RESERVATION_CONFLICT if ACCOUNT_RESERVATION_CONFLICT in kinds else DUPLICATE_EXPOSURE
                    return ReservationOutcome(None, reason, tuple(clashes))

                # 2) the account state the selection was computed on is still current:
                #    positions/pending (EXPOSURE_CHANGED) and other reservations (CONFLICT)
                reserved_recs = dedupe_reservations(reservation_records(active), opens, pending)
                exposure_fp = account_state_fingerprint(broker_account_id, opens + pending)
                reservation_fp = account_state_fingerprint(broker_account_id, reserved_recs)
                if expected_exposure_fingerprint is not None and exposure_fp != expected_exposure_fingerprint:
                    return ReservationOutcome(None, EXPOSURE_CHANGED, ())
                if expected_reservation_fingerprint is not None and reservation_fp != expected_reservation_fingerprint:
                    return ReservationOutcome(None, ACCOUNT_RESERVATION_CONFLICT, ())

                # 3) the bot's capacity the selection assumed is still current
                capacity = None
                if max_open_positions is not None:
                    occ = occupied_slots(_TxnDB(conn), bot_instance_id)
                    own = [r for r in active if r.bot_instance_id == bot_instance_id]
                    capacity = available_slots_from(occ, own, max_open_positions)
                    if (expected_available_slots is not None and capacity != expected_available_slots) \
                            or capacity < len(selected):
                        return ReservationOutcome(None, CAPACITY_CHANGED, ())

                if existing is not None:  # a previous RELEASED/EXPIRED/CONSUMED row under the same id
                    conn.execute(f"DELETE FROM {TABLE} WHERE reservation_id=?", (rid,))
                payload = {"candidate_ids": list(candidate_ids), "instruments": [list(i) for i in instruments]}
                conn.execute(
                    f"INSERT INTO {TABLE} (reservation_id, broker_account_id, bot_instance_id, cycle_id, selected_candidate_ids,"
                    " selected_instruments, status, mode, created_at, expires_at, updated_at, reservation_version,"
                    " policy_version, policy_hash, payload_hash, capacity_snapshot)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (rid, broker_account_id, bot_instance_id, cycle_id, json.dumps(list(candidate_ids)),
                     json.dumps([list(i) for i in instruments]), ReservationStatus.RESERVED.value, mode, now_ms,
                     now_ms + ttl_seconds * 1000, now_ms,
                     AccountPortfolioReservation.__dataclass_fields__["reservation_version"].default,
                     policy_version, policy_hash, stable_hash(payload),
                     json.dumps({"available_slots": capacity, "exposure_fingerprint": exposure_fp,
                                 "reservation_fingerprint": reservation_fp})),
                )
                row = conn.execute(f"SELECT * FROM {TABLE} WHERE reservation_id=?", (rid,)).fetchone()
                return ReservationOutcome(_row_to_reservation(row))

    def _transition(self, reservation_id: str, to_status: str, now_ms: int) -> bool:
        """Compare-and-set from RESERVED. An expired reservation is marked
        EXPIRED first, so it can never be consumed or released late."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                f"UPDATE {TABLE} SET status='EXPIRED', updated_at=? WHERE reservation_id=? AND status='RESERVED' AND expires_at<=?",
                (now_ms, reservation_id, now_ms),
            )
            cur = conn.execute(
                f"UPDATE {TABLE} SET status=?, updated_at=? WHERE reservation_id=? AND status='RESERVED'",
                (to_status, now_ms, reservation_id),
            )
            return cur.rowcount == 1

    def release(self, reservation_id: str, now_ms: int) -> bool:
        """E.g. hard risk rejected the selected candidate. Never substitutes rank #2."""
        return self._transition(reservation_id, ReservationStatus.RELEASED.value, now_ms)

    def consume(self, reservation_id: str, now_ms: int) -> bool:
        return self._transition(reservation_id, ReservationStatus.CONSUMED.value, now_ms)

    # -- reads -----------------------------------------------------------------------
    def get(self, reservation_id: str) -> Optional[AccountPortfolioReservation]:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT * FROM {TABLE} WHERE reservation_id=?", (reservation_id,)).fetchone()
        return _row_to_reservation(row) if row is not None else None

    def active_reservations(self, broker_account_id: str, now_ms: int) -> List[AccountPortfolioReservation]:
        with self._db.connect() as conn:
            return self._active(conn, broker_account_id, now_ms)


__all__ = ["TABLE", "MODE_SHADOW", "Selected", "ReservationOutcome", "ReservationSchemaMissing",
           "available_slots_from", "CATIReservationStore"]
