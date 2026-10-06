"""Natural-signal replay through the REAL production factory.

Every other end-to-end test drives a harness orchestrator at 5,000 USDT with a
synthetic 2% stop. This one answers the operational question directly: given
the deployed account as it actually is -- Binance DEMO, ~418 USDT equity, a
Balanced bot with a 120 USDT fixed margin -- does a prospective decision with
the geometry CATI has really produced reach the broker CREATE?

``production.process_account`` runs with its default ``boundary_for`` factory:
the orchestrator, executor, sizing, preflight and account authority are the
ones production builds. Only the exchange transport is a mock, loaded with the
DEMO venue's real instrument filters and a DEMO-shaped (wide-spread) book.
"""
from types import SimpleNamespace

import pytest

from _exec import _entry, _order, cati_bot
from test_production_demo_execution import demo  # noqa: F401  (fixture)
from test_production_execution import live  # noqa: F401  (fixture)
from app.exchange.instruments import InstrumentCatalog
from app.trading_intelligence.evidence.stores import RiskDecisionStore
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration.residual_prospective import Tracker, H

#: The deployed account's broker truth during the audited window.
EQUITY = 418.6091673
MARGIN = 120.0

#: Venue filters as Binance DEMO publishes them (tick, step, min notional).
VENUE = {
    "ADAUSDT": ("0.00010", "1", "5"),
    "FILUSDT": ("0.000100", "0.1", "5"),
    "BIGTIMEUSDT": ("0.0000100", "1", "5"),
    "FETUSDT": ("0.0001000", "1", "5"),
    "MINAUSDT": ("0.0001000", "1", "5"),
}

#: Candidates exactly as the prospective tracker recorded them, with the next
#: native open that followed: (symbol, side, score, close, stop, target, risk,
#: atr14, next_open).
RECORDED = {
    "ADA_7": ("ADAUSDT", "LONG", 2.0598095740256848, 0.2642, 0.24546964285714287, 0.31102589285714277,
              0.018730357142857118, 0.0045214285714285695, 0.2642),
    "ADA_12": ("ADAUSDT", "LONG", 3.3838852687326018, 0.275, 0.2425625, 0.3560937500000001,
               0.03243750000000003, 0.005750000000000001, 0.2751),
    "FIL_12": ("FILUSDT", "LONG", 2.0620623127693323, 1.1789, 1.0364017857142858, 1.535145535714286,
               0.1424982142857144, 0.02679285714285711, 1.1789),
    "BIGTIME_14": ("BIGTIMEUSDT", "SHORT", -2.287005883502138, 0.00901, 0.010308321428571429, 0.00576419642857143,
                   0.0012983214285714282, 8.128571428571417e-05, 0.009009),
    "FET_15": ("FETUSDT", "LONG", 2.1527185399530055, 0.2581, 0.21920535714285716, 0.3553366071428571,
               0.03889464285714285, 0.007178571428571431, 0.2581),
    "MINA_39": ("MINAUSDT", "SHORT", -4.0153372701235055, 0.11721, 0.1632319642857143, 0.002155089285714229,
                0.046021964285714305, 0.004007857142857144, 0.11721),
}


def book(price, tick, spread_ticks=4, size="500000"):
    """A DEMO-shaped book: deep levels, a spread several ticks wide."""
    tick = float(tick)
    half = spread_ticks * tick / 2
    return {"bids": [[f"{price - half - i * tick:.8f}", size] for i in range(5)],
            "asks": [[f"{price + half + i * tick:.8f}", size] for i in range(5)]}


@pytest.fixture
def deployed(demo, monkeypatch):
    from app.core import broker_capability_gate, config
    from app.execution import executor
    from app.product_safety import execution_safety

    monkeypatch.setattr(broker_capability_gate, "assert_broker_execution_capability", lambda *a, **kw: None)
    monkeypatch.setattr(execution_safety, "evaluate_execution_readiness",
                        lambda **kw: SimpleNamespace(allowed=True, state="APPROVED"))
    # The executor reads venue metadata from the connected account in production.
    monkeypatch.setattr(executor, "settings", config.settings)
    # In-flight margin reservations are process-wide and linger after a fill.
    from app.risk import capital_ledger
    monkeypatch.setattr(capital_ledger, "ACCOUNT_RESERVATIONS", capital_ledger.AccountMarginReservations())
    production._boundaries.clear()
    with demo.db.connect() as c:
        c.execute("UPDATE bot_instances SET status='stopped' WHERE broker_account_id=? AND id<>?",
                  (demo.account["id"], demo.plan.bot_instance_id))
        c.execute("UPDATE broker_accounts SET environment='demo', status='connected' WHERE id=?",
                  (demo.account["id"],))
    # The deployment as the user made it: 120 USDT fixed margin per position.
    cati_bot(demo.db, demo.plan.bot_instance_id, demo.account["id"], user_id=demo.plan.user_id,
             capital=MARGIN, allocation=MARGIN)
    with demo.db.connect() as c:
        c.execute("UPDATE bot_instances SET mode='live' WHERE id=?", (demo.plan.bot_instance_id,))
    demo.reservations.release(demo.plan.portfolio_reservation_id, demo.now)
    demo.account = {**demo.account, "environment": "demo"}

    def signal(name, *, price=None, spread_ticks=4, minutes_late=2):
        symbol, side, score, close, stop, target, risk, atr, next_open = RECORDED[name]
        tick, step, min_notional = VENUE[symbol]
        tracker = Tracker(demo.db, now_ms=demo.now)
        decision = production.latest_decision(demo.db)["decision_time"] + H
        candidate = dict(symbol=symbol, side=side, score=score, entry_reference=close, stop=stop, target=target,
                         risk=risk, atr14=atr)
        did, _ = tracker.commit_decision(decision, {
            "candidate": candidate, "reason": "SELECTED_TOP1",
            "eligible_universe": production.frozen_definition()[0]["universe"][:30] + [symbol]}, decision + 100_000)
        tracker.record_entry(did, decision + 1, next_open, decision + 101_000)
        demo.now = decision + minutes_late * 60_000
        live_price = next_open if price is None else price
        instrument = demo.instrument(symbol, tick_size=float(tick), qty_step=float(step), min_qty=float(step),
                                     min_notional=float(min_notional), max_qty=10_000_000.0)
        InstrumentCatalog(demo.db).upsert("binance_usdm", "DEMO", [instrument], decision - 600_000)
        client = demo.client
        client.broker_environment = "demo"
        client.account.return_value = dict(totalWalletBalance=EQUITY, totalMarginBalance=EQUITY,
            availableBalance=EQUITY, totalInitialMargin=0, totalUnrealizedProfit=0, totalMaintMargin=0)
        client.income_history.return_value = []
        client.last_price.return_value = live_price
        client.get_prices.return_value = {symbol: live_price}
        client.klines.return_value = []
        client.get_algo_orders.return_value = []
        client.get_instrument.return_value = instrument
        client.depth.return_value = book(live_price, tick, spread_ticks)
        client.exchange_info_cached.return_value = {"symbols": [{
            "symbol": symbol, "baseAsset": symbol[:-4], "quoteAsset": "USDT", "marginAsset": "USDT",
            "contractType": "PERPETUAL", "orderTypes": ["LIMIT", "MARKET", "STOP_MARKET", "TAKE_PROFIT_MARKET"],
            "timeInForce": ["GTC"], "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": tick},
                {"filterType": "LOT_SIZE", "stepSize": step, "minQty": step},
                {"filterType": "MIN_NOTIONAL", "notional": min_notional}]}]}

        def place(req):
            demo.seen["orders"].append(req)
            fill = float(client.depth.return_value["asks" if side == "LONG" else "bids"][0][0])
            return _entry(str(req.qty), f"{fill:.8f}", "FILLED")(req)

        client.place_order.side_effect = place
        client.get_order.side_effect = lambda symbol, order_id: _order(
            "FILLED", str(demo.seen["orders"][-1].qty), "0")
        return did

    def process(positions=()):
        return production.process_account(demo.db, demo.account, demo.client,
                                          {"positions": list(positions), "orders": []}, now_ms=demo.now)

    def approved(result):
        """The hard-risk decision the boundary persisted for this evaluation."""
        rows = RiskDecisionStore(demo.db).for_plan(demo.account["id"], result["trade_plan_id"])
        return rows[-1]["payload"]

    demo.signal, demo.process, demo.approved = signal, process, approved
    return demo


@pytest.mark.parametrize("name,leverage", [("ADA_7", 3), ("ADA_12", 1), ("FIL_12", 1), ("BIGTIME_14", 1)])
def test_recorded_signal_inside_the_stop_ceiling_reaches_the_broker(deployed, name, leverage):
    _symbol, side, _score, _close, stop, target, *_ = RECORDED[name]
    deployed.signal(name)
    result = deployed.process()
    assert result["boundary"]["status"] == "EXECUTED", result["boundary"]
    assert result["orders_submitted"] and result["execution_permission"] == "ORDER_ACTIVE"
    risk = deployed.approved(result)
    assert risk["status"] == "APPROVED" and risk["resolved_leverage"] == leverage
    # The user's fixed margin is neither shrunk nor converted to risk sizing,
    # and the structural stop is the one CATI recorded.
    assert risk["resolved_notional"] / risk["resolved_leverage"] == pytest.approx(MARGIN)
    assert risk["resolved_stop_price"] == pytest.approx(stop)
    [order] = deployed.seen["orders"]
    # Submitted margin: the full allocation less only step rounding and the
    # executor's pre-entry headroom -- never above the user's 120 USDT.
    submitted_margin = float(order.qty) * deployed.client.last_price.return_value / leverage
    assert 0.995 * MARGIN <= submitted_margin <= MARGIN
    assert order.side.value.upper() == ("BUY" if side == "LONG" else "SELL")
    [protection] = deployed.seen["protection"]
    assert float(protection.sl_price) == pytest.approx(stop, rel=2e-3)
    assert float(protection.tp_price) == pytest.approx(target, rel=2e-3)


@pytest.mark.parametrize("name", ["FET_15", "MINA_39"])
def test_recorded_signal_beyond_the_stop_ceiling_is_rejected_before_any_create(deployed, name):
    deployed.signal(name)
    result = deployed.process()
    assert tuple(result["boundary"]["reason_codes"]) == ("CATI_STRUCTURAL_STOP_EXCEEDS_SYSTEM_MAX",)
    assert result["reservation_status"] == "RELEASED"
    deployed.client.place_order.assert_not_called()


def test_entry_minutes_after_the_open_across_a_wide_demo_spread_still_executes(deployed):
    # Ten minutes into the window, 0.4% above the next-open reference, through a
    # 45 bps spread: outside the retired +/-2 bps band, inside the trade's economics.
    deployed.signal("ADA_7", price=0.2642 * 1.004, spread_ticks=12, minutes_late=10)
    result = deployed.process()
    assert result["boundary"]["status"] == "EXECUTED", result["boundary"]
    assert deployed.approved(result)["resolved_leverage"] == 3


def test_a_later_cycle_in_the_same_window_never_creates_twice(deployed):
    deployed.signal("ADA_7")
    assert deployed.process()["boundary"]["status"] == "EXECUTED"
    deployed.now += 30_000
    deployed.process()
    # A restarted process rebuilds the boundary from durable state alone.
    production._boundaries.clear()
    deployed.now += 30_000
    deployed.process()
    assert deployed.client.place_order.call_count == 1


def test_chase_that_destroys_reward_to_risk_is_rejected(deployed):
    # 0.6R above the reference: the stop is intact but the remaining reward is not.
    deployed.signal("ADA_7", price=0.2642 + 0.6 * 0.018730357142857118)
    result = deployed.process()
    assert tuple(result["boundary"]["reason_codes"]) == ("CATI_ENTRY_ECONOMICS_DEGRADED",)
    deployed.client.place_order.assert_not_called()


# ── Restart and crash recovery, through the same real factory ────────────────


def native_protection(deployed):
    """Protection through the real durable close-position envelope, over a
    mock algo-order book that behaves like the venue's."""
    from app.execution.production_protection import place_native_protection

    book = []

    def create_leg(path, *, params):
        leg = {**params, "algoId": f"algo-{len(book) + 1}"}
        book.append(leg)
        return leg

    client = deployed.client
    client._signed_post.side_effect = create_leg
    client.get_algo_orders.side_effect = lambda *a, **k: list(book)
    client.place_protection.side_effect = lambda request: place_native_protection(client, request)
    return book


def broker_position(deployed, qty, price):
    """The venue's own record of the filled entry, as a restart would read it."""
    position = {"symbol": "ADAUSDT", "positionAmt": str(qty), "entryPrice": f"{price:.8f}", "positionSide": "BOTH"}
    deployed.client.get_position_info.return_value = position
    deployed.client.user_trades.side_effect = lambda *a, **k: [{
        "id": "fill-1", "orderId": "555", "qty": str(qty), "price": f"{price:.8f}", "side": "BUY",
        "time": deployed.now, "realizedPnl": "0", "commission": "0.1"}]
    return position


def test_restart_with_an_open_protected_position_sends_no_second_entry_and_no_duplicate_protection(deployed):
    deployed.signal("ADA_7")
    book = native_protection(deployed)
    assert deployed.process()["boundary"]["status"] == "EXECUTED"
    [order] = deployed.seen["orders"]
    assert [leg["type"] for leg in book] == ["STOP_MARKET", "TAKE_PROFIT_MARKET"]
    fill = float(deployed.client.depth.return_value["asks"][0][0])
    position = broker_position(deployed, float(order.qty), fill)

    # The process dies. Its successor has the database and the broker, nothing else.
    production._boundaries.clear()
    for _ in range(3):
        deployed.now += 30_000
        restarted = deployed.process(positions=[position])
        assert restarted["reason"] == "ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE"
        assert restarted["execution_portfolio"]["state"] == "POSITION_OPEN"
        [history] = restarted["execution_history"]
        assert history["position"]["quantity"] == float(order.qty) and history["protection"]["status"] == "success"
    assert deployed.client.place_order.call_count == 1
    assert deployed.client._signed_post.call_count == 2 and len(book) == 2


def test_crash_with_an_unconfirmed_create_reads_back_and_never_resubmits(deployed):
    deployed.signal("ADA_7")
    book = native_protection(deployed)
    deployed.client.place_order.side_effect = TimeoutError("connection lost after the request was sent")
    unknown = deployed.process()
    assert unknown["boundary"]["status"] == "SUBMIT_UNKNOWN_PENDING_RECONCILIATION"

    # Restart while the venue still cannot say what happened to that order.
    production._boundaries.clear()
    deployed.client.get_order_by_client_order_id.side_effect = RuntimeError("order service unavailable")
    deployed.now += 30_000
    blocked = deployed.process()
    assert blocked["reason"] == "ACCOUNT_SUBMIT_OUTCOME_UNRESOLVED" and not blocked["orders_submitted"]
    assert deployed.client.place_order.call_count == 1 and book == []

    # The venue answers: the order had filled. It is adopted and protected once.
    client_order_id = unknown["boundary"]["attempt"]["client_order_id"]
    deployed.client.get_order_by_client_order_id.side_effect = None
    deployed.client.get_order_by_client_order_id.return_value = {
        "orderId": 555, "clientOrderId": client_order_id, "status": "FILLED", "executedQty": "1360",
        "avgPrice": "0.26440000", "origQty": "1360"}
    position = broker_position(deployed, 1360.0, 0.2644)
    production._boundaries.clear()
    deployed.now += 30_000
    adopted = deployed.process(positions=[position])
    assert [r["reason_codes"] for r in adopted["recovery"]] == [("RECONCILED_POSITION_EXISTS",)]
    assert [leg["type"] for leg in book] == ["STOP_MARKET", "TAKE_PROFIT_MARKET"]
    assert deployed.client.place_order.call_count == 1


def test_a_stopping_runtime_opens_no_new_position(deployed):
    from app.ops import runtime_shutdown

    deployed.signal("ADA_7")
    runtime_shutdown.request_stop()
    try:
        with pytest.raises(ValueError, match="RUNTIME_SHUTDOWN_IN_PROGRESS") as stopped:
            deployed.process()
    finally:
        runtime_shutdown.reset_for_tests()
    evaluation = stopped.value.production_evaluation
    assert evaluation["reason"] == "RUNTIME_SHUTDOWN_IN_PROGRESS" and evaluation["reservation_status"] == "RELEASED"
    deployed.client.place_order.assert_not_called()
