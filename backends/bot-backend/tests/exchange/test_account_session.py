"""Real adapter factory composition with scripted HTTP; never venue certification."""
import pytest

from _platform_adapter_wire import build_rig
from app.exchange.platform_adapter.session import AccountSession
from app.exchange.platform_adapter.rate_budget import BudgetedVenueAdapter
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB


@pytest.mark.parametrize("venue", ["binance", "bybit", "bingx"])
def test_session_wraps_real_clients_with_shared_budget_and_persisted_gates(tmp_path, monkeypatch, venue):
    rig = build_rig(venue, monkeypatch)
    db = DB(str(tmp_path / "sessions.db"))
    ensure_cati_platform_schema(db)
    now = 1_800_000_000_000
    session = AccountSession(db, rig.client, venue, "DEMO", account_ref="customer-1",
                             server_address="203.0.113.1", clock=lambda: now)
    assert isinstance(session.adapter, BudgetedVenueAdapter)
    assert not session.certified(venue, "DEMO")[0]
    assert session.certified(venue, "LIVE") == (False, "SESSION_SCOPE_MISMATCH")
    assert not session.entries_allowed()[0]
    # Reads enter the durable venue/IP budget, even before private-stream certification.
    session.adapter.balance()
    snapshot = session.adapter._budget.snapshot(venue, "203.0.113.1", now)
    assert snapshot["used"] > 0
    session.close()
