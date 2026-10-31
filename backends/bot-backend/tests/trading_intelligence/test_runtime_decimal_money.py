"""Runtime money arithmetic is exact (``shared_lib.core.money_math``): account risk, fills, metrics, reports, capital.

Each test names the float artifact the old arithmetic produced and shows the runtime now computes the exact
decimal result from the venue's own strings, converts it to the caller's float once, and persists and reloads it
unchanged. Storage on SQLite and PostgreSQL is covered by ``test_legacy_money_postgres`` and, for this module's own
persisted values, by running it under ``legacy_pg_plugin`` (listed in ``legacy_postgres_modules.txt``).
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from _exec import Harness, cati_bot
from app.core import config
from app.core.config import Settings
from app.trading_intelligence.integration import production_execution as production


def profile():
    state = Settings(_env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production", ENVIRONMENT_NAME="production",
                     EXECUTION_MODE="live", LIVE_ORDER_SUBMISSION_ENABLED=False)
    return state


@pytest.fixture
def harness(tmp_path, monkeypatch):
    h = Harness(tmp_path)
    settings = profile()
    monkeypatch.setattr(config, "settings", settings)
    monkeypatch.setattr(production, "settings", settings)
    production.initialize(h.db)
    # A fixed mid-day instant (Wednesday 12:00 UTC), so every event below falls inside one risk day and week.
    h.now = int(datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
    return h


def income_client(account, events):
    """A venue whose income endpoint answers each requested window with the events inside it."""
    client = Mock()
    client.account.return_value = account
    client.income_history.side_effect = lambda start_time_ms, end_time_ms, limit: [
        e for e in events if start_time_ms <= int(e["time"]) <= end_time_ms]
    return client


# ════════════════════════════════════════════════════════════════════════════════════ daily loss latch
def test_daily_loss_latch_fires_at_the_exact_hard_cap(harness):
    """Seven realized losses of 8.2 and 67.6 unrealized are a loss of exactly 125 = the 2.5 % cap on 5000.

    The float arithmetic computed 124.99999999999999 (sum of seven -8.2 is -57.39999999999999) and did NOT latch:
    the account could keep opening positions past its hard daily cap."""
    h = harness
    events = [{"incomeType": "REALIZED_PNL", "income": "-8.2", "time": h.now - 1000 + i, "tranId": f"t{i}",
               "asset": "USDT"} for i in range(7)]
    client = income_client(dict(totalMarginBalance="4875", totalWalletBalance="4942.6", availableBalance="4875",
                                totalInitialMargin="0", totalUnrealizedProfit="-67.6"), events)
    cati_bot(h.db, "a", "exact")
    risk = production.account_risk(h.db, {"id": "exact"}, client, [], [], [{"id": "a"}], h.now)
    assert sum(float(e["income"]) for e in events) == -57.39999999999999          # the float artifact
    assert risk["opening_equity"] == 5000.0
    assert risk["realized_pnl"] == -57.4 and risk["daily_loss_usage"] == 125.0
    assert risk["loss_latched"] is True and risk["remaining_daily_risk"] == 0.0
    assert risk["reason"] == "USER_DAILY_LOSS_LIMIT_REACHED"


def test_fees_funding_and_transfers_are_summed_exactly(harness):
    h = harness
    events = ([{"incomeType": "COMMISSION", "income": "-0.1", "time": h.now - 5000 + i, "tranId": f"c{i}",
                "asset": "USDT"} for i in range(900)]
              + [{"incomeType": "FUNDING_FEE", "income": v, "time": h.now - 4000 + i, "tranId": f"f{i}", "asset": "USDT"}
                 for i, v in enumerate(("-0.01234", "0.005", "-0.00266"))]
              + [{"incomeType": "TRANSFER", "income": "0.3", "time": h.now - 3000, "tranId": "x1", "asset": "USDT"}])
    # Wallet after the events: 5000 - 90 (fees) - 0.01 (funding) + 0.3 (transfer)
    client = income_client(dict(totalMarginBalance="4910.29", totalWalletBalance="4910.29", availableBalance="4910.29",
                                totalInitialMargin="0", totalUnrealizedProfit="0"), events)
    cati_bot(h.db, "a", "fees")
    risk = production.account_risk(h.db, {"id": "fees"}, client, [], [], [{"id": "a"}], h.now)
    assert risk["fees"] == 90.0 and risk["funding"] == -0.01
    assert risk["opening_equity"] == 5000.0
    assert risk["realized_pnl"] == -90.01 and risk["account_realized_pnl"] == -90.01
    assert risk["daily_loss_usage"] == 90.01 and risk["loss_latched"] is False
    assert risk["remaining_daily_risk"] == 34.99                        # 125 - 90.01, exactly


def test_risk_state_is_identical_after_restart(harness):
    """Persist -> reload: a second evaluation (a restart re-reads the stored ledger and basis) gives identical
    money, and the durable period peaks are unchanged by the repeat."""
    h = harness
    events = [{"incomeType": "REALIZED_PNL", "income": v, "time": h.now - 900 + i, "tranId": f"r{i}", "asset": "USDT"}
              for i, v in enumerate(("12.5", "-13.75", "1.25", "-0.00000001"))]
    client = income_client(dict(totalMarginBalance="4999.99999999", totalWalletBalance="4999.99999999",
                                availableBalance="4999.99999999", totalInitialMargin="0", totalUnrealizedProfit="0"),
                           events)
    cati_bot(h.db, "a", "restart")
    first = production.account_risk(h.db, {"id": "restart"}, client, [], [], [{"id": "a"}], h.now)
    second = production.account_risk(h.db, {"id": "restart"}, client, [], [], [{"id": "a"}], h.now)
    keys = ("opening_equity", "realized_pnl", "fees", "funding", "account_realized_pnl", "daily_loss_usage",
            "remaining_daily_risk")
    assert [first[k] for k in keys] == [second[k] for k in keys]
    assert first["opening_equity"] == 5000.0 and first["realized_pnl"] == -0.00000001
    assert first["periods"]["weekly"]["peak_equity"] == second["periods"]["weekly"]["peak_equity"]
    with h.db.connect() as c:
        stored = c.execute("SELECT opening_wallet FROM cati_production_daily_risk WHERE account_id='restart'").fetchone()[0]
    assert float(stored) == 5000.0


def test_non_finite_income_still_fails_closed(harness):
    h = harness
    client = income_client(dict(totalMarginBalance="5000", totalWalletBalance="5000", availableBalance="5000",
                                totalInitialMargin="0", totalUnrealizedProfit="0"), [])
    cati_bot(h.db, "a", "nan")
    production.account_risk(h.db, {"id": "nan"}, client, [], [], [{"id": "a"}], h.now)      # creates the ledger
    with h.db.connect() as c:                                   # a corrupt stored venue document
        c.execute("INSERT INTO cati_account_income VALUES(?,?,?,?,?,?,?,?)",
                  ("nan", "bad", "REALIZED_PNL", 0.0, h.now - 10, None, None,
                   '{"incomeType":"REALIZED_PNL","income":"NaN","time":%d}' % (h.now - 10)))
    with pytest.raises(ValueError, match="DAILY_RISK_BASIS_UNAVAILABLE"):
        production.account_risk(h.db, {"id": "nan"}, client, [], [], [{"id": "a"}], h.now)


# ════════════════════════════════════════════════════════════════════════════════════════════ fills
def test_fill_vwap_and_quantity_are_exact():
    fills = [{"price": "0.1", "qty": "0.7"}, {"price": "0.2", "qty": "0.1"}]
    assert sum(float(f["qty"]) for f in fills) == 0.7999999999999999            # float: under the 0.8 executed
    assert production._fill_vwap(fills) == 0.1125                               # (0.07 + 0.02) / 0.8 exactly
    assert production._fill_vwap([]) is None
    many = [{"price": "65000.1", "qty": "0.001"} for _ in range(1000)]
    assert production._fill_vwap(many) == 65000.1


def test_certification_pnl_is_exact(harness):
    h = harness
    import json
    with h.db.connect() as c:
        for i in range(1000):
            c.execute("INSERT INTO cati_production_fills VALUES(?,?,?,?,?)",
                      ("cert", f"fill{i}", f"o{i}", "ADAUSDT", json.dumps({
                          "id": f"fill{i}", "time": h.now - 100, "realizedPnl": "0.3", "commission": "0.1",
                          "_execution": {"purpose": "DEMO_CERTIFICATION"}})))
    ids, pnl = production.certification_pnl(h.db, "cert", h.now - 1000, h.now)
    assert len(ids) == 1000 and pnl == 200.0
    _, exact_pnl = production.certification_pnl(h.db, "cert", h.now - 1000, h.now, exact_decimal=True)
    assert str(exact_pnl) == "200.0"


# ════════════════════════════════════════════════════════════════════════ strategy performance (running total)
def test_strategy_performance_fee_total_does_not_drift(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    from app.metrics.performance import update_strategy_performance
    db = DB(str(tmp_path / "perf.db"))
    migrate(db)
    key = dict(strategy="cati", strategy_version="1", symbol="BTCUSDT", asset_class="CRYPTO", broker_id="binance",
               account_id="acct", timeframe="15m")
    for _ in range(2000):
        update_strategy_performance(db, **key, net_pnl_delta=-0.1, gross_pnl_delta=0.0, fees_delta=0.1)
    with db.connect() as c:
        row = c.execute("SELECT trades, net_pnl, gross_pnl, fees FROM strategy_performance").fetchone()
    assert (row[0], float(row[1]), float(row[2]), float(row[3])) == (2000, -200.0, 0.0, 200.0)
    running = 0.0
    for _ in range(2000):
        running += 0.1
    assert running != 200.0                                     # the float artifact the stored total no longer has


# ════════════════════════════════════════════════════════════════════════════════════════════ reports
def _fills_db(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    db = DB(str(tmp_path / "reports.db"))
    migrate(db)
    with db.connect() as c:
        for i in range(10):
            ts = datetime(2026, 3, 1, 12, i, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            c.execute("INSERT INTO trade_fills (symbol, side, action, qty, price, fee, realized_pnl, timestamp_utc, "
                      "user_id, broker_id, broker_account_id) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                      ("BTCUSDT" if i % 2 else "ETHUSDT", "LONG", "CLOSE", 0.1, 0.7, 0.1,
                       0.1 if i < 7 else -0.2, ts, "u1", "binance", "acct"))
    return db


def test_tax_report_totals_are_exact(tmp_path):
    from app.analytics.tax_report_service import TaxReportService
    report = TaxReportService(_fills_db(tmp_path)).get_execution_based_tax_report("u1", 2026)
    s = report["summary"]
    assert (s["total_trades"], s["gains_count"], s["losses_count"]) == (10, 7, 3)
    assert s["total_gains"] == 0.7 and s["total_losses"] == -0.6 and s["total_fees"] == 1.0
    assert s["net_pnl"] == 0.1                                  # float: 0.7 + -0.6 = 0.09999999999999998
    assert report["trades"][0]["proceeds"] == 0.07              # float: 0.1 * 0.7 = 0.06999999999999999
    by = {r["symbol"]: r for r in report["by_symbol"]}
    assert by["ETHUSDT"]["total_fees"] == 0.5 and by["BTCUSDT"]["total_fees"] == 0.5
    assert by["ETHUSDT"]["total_pnl"] + by["BTCUSDT"]["total_pnl"] == pytest.approx(0.1)


def test_pnl_breakdown_net_is_exact(tmp_path):
    from app.analytics.pnl_service import PnLService, _exact_difference, _exact_total
    assert _exact_difference(0.3, 0.1) == 0.2 and 0.3 - 0.1 != 0.2
    assert _exact_total([0.1, 0.7, None]) == 0.8
    out = PnLService(_fills_db(tmp_path)).get_pnl_breakdown("u1", group_by="symbol")
    assert {r["symbol"]: r["trade_count"] for r in out["breakdown"]} == {"BTCUSDT": 5, "ETHUSDT": 5}
    assert out["total_pnl"] == pytest.approx(0.1)


# ════════════════════════════════════════════════════════════════════════════════════ capital and margin
def test_reservations_and_margin_are_exact():
    from app.risk.capital_ledger import AccountMarginReservations, margin_for, per_trade_allocation_margin
    clock = [0.0]
    book = AccountMarginReservations(clock=lambda: clock[0])
    for _ in range(7):
        assert book.reserve("acct", available_balance=1000.0, requested_margin=0.1).approved
    assert book.pending_margin("acct") == 0.7                    # float: 0.7000000000000001 after 7 additions
    verdict = book.reserve("acct", available_balance=0.8, requested_margin=0.1)
    assert verdict.effective_available == 0.1                     # 0.8 - 0.7, not 0.09999999999999987
    assert margin_for(0.1, 0.7, 1) == 0.07 and margin_for("3", "0.1", 3) == 0.1
    assert per_trade_allocation_margin("percent", 0.7, 10.0) == 0.07
    assert margin_for(float("nan"), 1.0, 1) != margin_for(float("nan"), 1.0, 1)   # non-finite: float semantics kept


def test_capital_ledger_chain_is_exact(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    from app.risk.capital_ledger import CapitalLedger
    db = DB(str(tmp_path / "ledger.db"))
    migrate(db)
    ledger = CapitalLedger(db, bot_instance_id="b", per_trade_allocation=0.1, portfolio_margin_limit=0.0)
    verdict = ledger.authorize(risk_notional=1.0, leverage=3.0)
    assert verdict.approved and verdict.approved_notional == pytest.approx(0.3)
    assert verdict.approved_notional == 0.3                       # float: 0.1 * 3 = 0.30000000000000004


# ════════════════════════════════════════════════════════════════════════════════════ equity snapshots
@pytest.mark.parametrize("broker_id, account, equity, margin", [
    ("binance", {"totalWalletBalance": "100.1", "totalUnrealizedProfit": "0.2", "availableBalance": "0.1",
                 "totalMarginBalance": "100.3"}, 100.3, 100.2),
    ("bingx", {"balance": {"balance": "0.3", "unrealizedProfit": "-0.1", "availableMargin": "0.1"}}, 0.2, 0.2),
    ("bybit", {"result": {"walletBalance": "0.7", "unrealisedPnl": "0.1", "availableBalance": "0.6"}}, 0.8, 0.1),
])
def test_equity_snapshot_derived_values_are_exact(tmp_path, broker_id, account, equity, margin):
    """Float: 0.3 - 0.1 = 0.19999999999999998, 0.7 + 0.1 = 0.7999999999999999, 0.7 - 0.6 = 0.09999999999999998
    (the binance row has no artifact: it pins that exact and float agree when the float is already right)."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    from app.analytics.equity_snapshot_service import EquitySnapshotService
    db = DB(str(tmp_path / "equity.db"))
    migrate(db)
    client = Mock()
    client.account.return_value = account
    EquitySnapshotService(db).record_snapshot("u1", "acct", broker_id, client, "cycle_end")
    with db.connect() as c:
        row = c.execute("SELECT equity, margin_used FROM equity_snapshots WHERE broker_account_id='acct'").fetchone()
    assert (float(row[0]), float(row[1])) == (equity, margin)


# ════════════════════════════════════════════════════════════════════════════════════════ risk budget
def test_symbol_concentration_at_the_exact_limit_is_not_refused():
    """Equity 1000 x 20 % = 200 per symbol; 0.3 held plus 0.1 x 1997 = 199.7 new is exactly 200, inside the limit.

    Float: 0.1 * 1997 = 199.70000000000002 and 0.3 + that = 200.00000000000003 > 200 -> a spurious
    SYMBOL_CONCENTRATION_EXCEEDED."""
    from app.risk.risk_budget import PositionRisk, RiskBudgetConfig, RiskBudgetEngine
    engine = RiskBudgetEngine(RiskBudgetConfig(max_exposure_per_side_pct=1.0))
    engine.update_account_state(equity=1000.0, margin_used=0.0, margin_available=1000.0)
    engine.update_positions([PositionRisk(symbol="BTCUSDT", side="LONG", qty=0.0003, entry_price=1000.0,
                                          stop_price=990.0, notional=0.3)])
    assert 0.3 + 0.1 * 1997 > 200.0                                          # the float artifact
    decision = engine.can_add_position("BTCUSDT", "LONG", 0.1, 1997.0, 1990.0)
    assert decision.reason_code != "SYMBOL_CONCENTRATION_EXCEEDED", decision
    assert decision.allowed, decision
    over = engine.can_add_position("BTCUSDT", "LONG", 0.1, 1997.1, 1990.0)    # 199.71 + 0.3 > 200
    assert not over.allowed and over.reason_code == "SYMBOL_CONCENTRATION_EXCEEDED"
