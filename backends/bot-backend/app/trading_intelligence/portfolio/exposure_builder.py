"""AccountExposureSnapshot builder (Section 16.1-16.3).

Aggregates, from the canonical persisted tables, EVERYTHING one broker
account holds or intends across ALL bots that share it:

* open ``positions`` (status='OPEN')
* in-flight ``pending_entries``
* active CATI portfolio reservations (SHADOW)

joined through ``bot_instances.broker_account_id`` -- so a bot can never act
as if another bot's positions on the same account do not exist, and one
account never sees another account's rows. Read-only: this module never
writes and never reserves anything.

Canonical economic-position identity
------------------------------------
``(broker_account_id, bot_instance_id, canonical_symbol, side)``.

* Several OPEN rows with the same identity (e.g. scale-in fills recorded as
  separate rows) are ONE economic position: quantities/notionals are summed,
  never counted twice.
* A PENDING entry whose identity is already OPEN is the same position (a
  confirmed entry) and is dropped.
* An active SHADOW reservation whose identity is already OPEN/PENDING has
  been realised and is dropped.
* The same canonical instrument held by DIFFERENT bots on one account stays
  as separate attributable exposures (they are genuinely separate
  positions); it is surfaced via ``duplicate_instruments`` instead.
* Expired / released / consumed reservations are never exposure.
"""
from __future__ import annotations

import dataclasses
from typing import Any, List, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.exposure import (
    AccountExposureSnapshot, ExposureRecord, ExposureSide, ExposureStatus,
)
from app.trading_intelligence.contracts.instrument import instrument_key_for
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.portfolio.returns import side_sign


class PortfolioDataError(ValueError):
    """Persisted account state could not be interpreted -- fail closed."""


_OPEN_SQL = """
    SELECT p.bot_instance_id, p.symbol, p.side, p.remaining_qty, p.entry_price
    FROM positions p JOIN bot_instances b ON b.id = p.bot_instance_id
    WHERE b.broker_account_id = ? AND p.status = 'OPEN'
    ORDER BY p.bot_instance_id, p.symbol, p.side
"""
_ASSET_CLASS_SQL = "SELECT id, market_type FROM bot_instances WHERE broker_account_id = ?"
_MARKET_TYPE_TO_ASSET_CLASS = {"CRYPTO": "CRYPTO", "FOREX": "FX", "FX": "FX", "FUTURES": "FUTURES", "EQUITY": "EQUITY"}
_PENDING_SQL = """
    SELECT e.bot_id, e.symbol, e.side, e.sized_qty, e.intended_notional, e.reference_price, e.state
    FROM pending_entries e JOIN bot_instances b ON b.id = e.bot_id
    WHERE b.broker_account_id = ?
    ORDER BY e.bot_id, e.symbol, e.side
"""


def _side(value: Any) -> str:
    try:
        return ExposureSide.LONG.value if side_sign(str(value)) > 0 else ExposureSide.SHORT.value
    except ValueError as exc:
        raise PortfolioDataError(f"uninterpretable position side: {value!r}") from exc


def _rows(conn: Any, sql: str, params: tuple) -> list:
    try:
        return conn.execute(sql, params).fetchall()
    except Exception as exc:  # a missing table means "nothing there"; anything else is a fault
        if "no such table" in str(exc).lower():
            return []
        raise


def _key(venue: Any, symbol: Any, asset_class: str = "CRYPTO"):
    return instrument_key_for(venue=str(venue or "unknown").lower(), venue_symbol=str(symbol), asset_class=asset_class)


def _asset_classes(conn: Any, broker_account_id: str) -> dict:
    """bot_id -> CATI asset class from ``bot_instances.market_type``."""
    try:
        rows = conn.execute(_ASSET_CLASS_SQL, (broker_account_id,)).fetchall()
    except Exception as exc:
        if "no such column" in str(exc).lower() or "no such table" in str(exc).lower():
            return {}
        raise
    return {str(r[0]): _MARKET_TYPE_TO_ASSET_CLASS.get(str(r[1] or "CRYPTO").upper(), "CRYPTO") for r in rows}


def _venue(conn: Any, broker_account_id: str) -> str:
    """All bots on one broker account share its venue (broker_accounts.broker_id)."""
    rows = _rows(conn, "SELECT broker_id FROM broker_accounts WHERE id = ?", (broker_account_id,))
    return str(rows[0][0]) if rows and rows[0][0] else "unknown"


def economic_identity(rec: ExposureRecord) -> Tuple[str, str, str]:
    return (rec.bot_instance_id, rec.instrument_key.canonical_symbol, rec.side)


def load_open_and_pending(conn: Any, broker_account_id: str) -> Tuple[List[ExposureRecord], List[ExposureRecord]]:
    """Shared by the builder and by the reservation transaction (which must
    re-read state INSIDE its lock). Deduplicated by economic identity."""
    venue = _venue(conn, broker_account_id)
    classes = _asset_classes(conn, broker_account_id)
    merged: dict = {}
    for bot, symbol, side, qty, entry in _rows(conn, _OPEN_SQL, (broker_account_id,)):
        s = _side(side)
        q, px = float(qty or 0.0), float(entry or 0.0)
        rec = ExposureRecord(str(bot), _key(venue, symbol, classes.get(str(bot), "CRYPTO")), s, q, abs(q * px), px,
                             ExposureStatus.OPEN.value)
        ident = economic_identity(rec)
        prev = merged.get(ident)
        if prev is None:
            merged[ident] = rec
        else:  # same bot/instrument/side: one economic position, summed -- never double counted
            qty_sum = prev.quantity + rec.quantity
            notional = prev.notional + rec.notional
            merged[ident] = dataclasses.replace(prev, quantity=qty_sum, notional=notional,
                                                entry_reference=(notional / qty_sum) if qty_sum else prev.entry_reference)
    opens = [merged[k] for k in sorted(merged)]
    pending: List[ExposureRecord] = []
    for bot, symbol, side, qty, notional, ref, state in _rows(conn, _PENDING_SQL, (broker_account_id,)):
        if str(state or "PENDING_OPEN") == "OPEN_FAILED":
            continue  # a failed entry is not exposure
        rec = ExposureRecord(str(bot), _key(venue, symbol, classes.get(str(bot), "CRYPTO")), _side(side),
                             float(qty or 0.0), float(notional or 0.0), float(ref or 0.0), ExposureStatus.PENDING_ENTRY.value)
        if economic_identity(rec) in merged or any(economic_identity(p) == economic_identity(rec) for p in pending):
            continue  # already an open position (confirmed entry) / duplicate pending row
        pending.append(rec)
    return opens, pending


def dedupe_reservations(reserved: Sequence[ExposureRecord], opens: Sequence[ExposureRecord],
                        pending: Sequence[ExposureRecord]) -> List[ExposureRecord]:
    """Drop reservation records already realised as an open/pending position."""
    realised = {economic_identity(r) for r in list(opens) + list(pending)}
    out, seen = [], set()
    for r in reserved:
        ident = economic_identity(r)
        if ident in realised or ident in seen:
            continue
        seen.add(ident)
        out.append(r)
    return out


def account_state_fingerprint(broker_account_id: str, records: Sequence[ExposureRecord]) -> str:
    """Content hash of an account's exposure state. Computed identically by
    the selection snapshot and by the reservation transaction, so a stale
    selection is detected as EXPOSURE_CHANGED."""
    recs = sorted(
        (r.bot_instance_id, r.instrument_key.canonical_symbol, r.instrument_key.venue_symbol, r.side,
         r.exposure_status, round(r.quantity, 12), round(r.notional, 12))
        for r in records
    )
    return stable_hash({"broker_account_id": broker_account_id, "records": recs})


def reservation_records(reservations: Sequence[Any]) -> List[ExposureRecord]:
    out: List[ExposureRecord] = []
    for r in reservations:
        for canonical, venue, venue_symbol, side in r.selected_instruments:
            key = dataclasses.replace(_key(venue, venue_symbol), canonical_symbol=canonical)
            out.append(ExposureRecord(r.bot_instance_id or "unknown", key, _side(side), 0.0, 0.0, 0.0,
                                      ExposureStatus.SHADOW_RESERVED.value))
    return out


def build_account_exposure_snapshot(
    db: Any, broker_account_id: str, as_of_time: int, *, reservation_store: Optional[Any] = None,
    now_ms: Optional[int] = None,
) -> AccountExposureSnapshot:
    if not broker_account_id:
        raise PortfolioDataError("broker_account_id is required")
    with db.connect() as conn:
        opens, pending = load_open_and_pending(conn, broker_account_id)
    reserved: List[ExposureRecord] = []
    if reservation_store is not None:
        reserved = dedupe_reservations(reservation_records(reservation_store.active_reservations(
            broker_account_id, now_ms if now_ms is not None else as_of_time)), opens, pending)
    return AccountExposureSnapshot.build(
        broker_account_id=broker_account_id, as_of_time=as_of_time, open_exposures=tuple(opens),
        pending_exposures=tuple(pending), reservation_exposures=tuple(reserved), source_version="persisted_tables_v1",
    )


__all__ = ["PortfolioDataError", "economic_identity", "load_open_and_pending", "dedupe_reservations",
           "account_state_fingerprint", "reservation_records", "build_account_exposure_snapshot"]
