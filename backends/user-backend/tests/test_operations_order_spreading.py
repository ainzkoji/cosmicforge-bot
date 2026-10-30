"""Step 5.5 / EXT-LIQ-01: the admin operations overview reports the order-spreading blocker, read-only."""
from __future__ import annotations

import uuid

from tests.test_operations_api import api  # noqa: F401  (fixture: throwaway platform database, admin client)


def _refusal(db, code, at=1):
    with db.transaction() as conn:
        conn.execute("INSERT INTO platform_spread_decisions (decision_id, venue, symbol, side, signal_id, account_id, "
                     "outcome, code, calibrated, policy_version, created_at) VALUES (?,?,?,?,?,?,'REFUSED',?,0,?,?)",
                     (uuid.uuid4().hex, "BINANCE_USDM", "BTCUSDT", "BUY", "sig", "acct", code,
                      "conservative-uncalibrated-1", at))


def test_overview_reports_uncalibrated_spreading_and_refusals_by_code(api):  # noqa: F811
    _refusal(api.db, "ORDER_SPREADING_CALIBRATION_REQUIRED")
    _refusal(api.db, "ORDER_SPREADING_CALIBRATION_REQUIRED")
    _refusal(api.db, "MARKET_FALLBACK_DISABLED_BY_POLICY")
    with api.db.transaction(write=False) as conn:
        before = conn.scalar("SELECT COUNT(*) FROM platform_spread_decisions")
    view = api.client.get("/api/admin/operations/overview").json()["order_spreading"]
    assert view["available"] is True and view["external_gate"] == "EXT-LIQ-01"
    assert view["calibrated"] is False and view["status"] == "BLOCKED_PENDING_CALIBRATION"
    assert view["budgets"]["calibrated"] == 0
    assert view["refusals_by_code"] == {"MARKET_FALLBACK_DISABLED_BY_POLICY": 1,
                                        "ORDER_SPREADING_CALIBRATION_REQUIRED": 2}
    with api.db.transaction(write=False) as conn:          # read-only: nothing was written by the view
        assert conn.scalar("SELECT COUNT(*) FROM platform_spread_decisions") == before
