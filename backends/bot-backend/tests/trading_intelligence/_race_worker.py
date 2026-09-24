"""Spawned-process worker for the reservation race test (must be importable
by name in a fresh interpreter, hence a separate module)."""
from __future__ import annotations


def reserve_worker(db_path, account, bot, cycle, selected, now_ms, max_open, expected_slots, start_event, results):
    from shared_lib.persistence.db import DB

    from app.trading_intelligence.portfolio.exposure_builder import account_state_fingerprint
    from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore

    store = CATIReservationStore(DB(db_path))
    empty = account_state_fingerprint(account, [])
    start_event.wait(60)
    try:
        out = store.reserve(broker_account_id=account, bot_instance_id=bot, cycle_id=cycle, selected=selected,
                            now_ms=now_ms, ttl_seconds=900, expected_exposure_fingerprint=empty,
                            expected_reservation_fingerprint=empty, max_open_positions=max_open,
                            expected_available_slots=expected_slots)
        results.put((bot, cycle, out.reserved, out.conflict_reason))
    except Exception as exc:  # surfaced to the parent, never swallowed
        results.put((bot, cycle, None, f"{type(exc).__name__}: {exc}"))
