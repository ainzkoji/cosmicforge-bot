"""E-02: exact Decimal money arithmetic in the legacy executor and fill-evidence modules.

Follows ``shared_lib.core.money_math`` (commits 26a46cb, 7e3447c): each value is computed exactly from the
decimals of its operands and converted to the caller's float once. Every test names the float artifact the
previous arithmetic produced at that boundary; stored historical values and column types are untouched.

Modules: ``execution/executor.py`` (sizing, leverage cap, TP1 quantity, trailing and break-even stops, exposure),
``execution/paper_executor.py`` (fills, fees, PnL, remaining quantity), ``evidence/fill_bridge.py`` (position row
quantities, PnL, fees, margin release), ``shared_lib/persistence/trade_fills.calculate_close_pnl``, and the SQL
``SUM`` aggregates that feed a limit or a reconciliation total (entry-protection exposure, the capital ledger's
committed margin, the user-stream fill ledger). SQLite only: no PostgreSQL server is configured for this module.
"""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate


# ══════════════════════════════════════════════════════════════════════════════════════ paper executor
def _paper(**kw):
    from app.execution.paper_executor import PaperExecutor
    return PaperExecutor(None, slippage_bps=kw.pop("slippage_bps", 0.0), fee_bps=kw.pop("fee_bps", 4.0))


def test_paper_fees_of_900_partial_closes_accumulate_exactly():
    """900 partial closes of 0.1 at 2500 with 4 bps cost 0.1 each. The float running totals came to
    189.99999999999488 fees (100 opening fee + 900 x 0.1) and 10.000000000001386 remaining; exact: 190 and 10."""
    paper = _paper()
    opened = paper.open_position(symbol="ETHUSDT", side="BUY", notional_usdt=0, quantity=100.0, fallback_price=2500.0)
    assert opened.success and opened.fee == 100.0
    for _ in range(900):
        result = paper.partial_close(symbol="ETHUSDT", position_side="LONG", quantity=0.1, fallback_price=2500.0)
        assert result.success and result.fee == 0.1
    saved = paper.get_position("ETHUSDT")
    assert saved["fees"] == 190.0
    assert saved["remaining_qty"] == 10.0
    assert saved["realized_qty"] == 90.0
    # The authoritative remainder closes without a quantity mismatch.
    assert paper.close_position(symbol="ETHUSDT", quantity=10.0, fallback_price=2500.0).success


def test_paper_partial_close_pnl_and_remaining_quantity_are_exact():
    """Entry 100.1, exit 100.3 on 1 of 3: the float PnL was 0.20000000000000284. A SHORT of 0.3 at 50 closing 0.1
    at 49.9: the float remainder was 0.19999999999999998 and the PnL 0.010000000000000142. Exact: 0.2, 0.2, 0.01."""
    paper = _paper(fee_bps=0.0)
    paper.open_position(symbol="X", side="BUY", notional_usdt=0, quantity=3.0, fallback_price=100.1)
    result = paper.partial_close(symbol="X", position_side="BUY", quantity=1.0, fallback_price=100.3)
    assert result.details["realized_pnl"] == 0.2
    assert result.details["remaining_qty"] == 2.0

    paper = _paper(fee_bps=0.0)
    paper.open_position(symbol="Y", side="SELL", notional_usdt=0, quantity=0.3, fallback_price=50.0)
    result = paper.partial_close(symbol="Y", position_side="SHORT", quantity=0.1, fallback_price=49.9)
    assert result.details["remaining_qty"] == 0.2
    assert result.details["realized_pnl"] == 0.01
    # A second leg of exactly 0.2 is the whole remainder, so it is not a partial close.
    assert not paper.partial_close(symbol="Y", position_side="SHORT", quantity=0.2, fallback_price=49.9).success


def test_paper_fill_price_fee_and_quantity_are_one_rounding_of_the_exact_value():
    paper = _paper(slippage_bps=2.0, fee_bps=4.0)
    # 2.3 x 1.0002 = 2.30046; the float product was 2.3004599999999997.
    assert paper._apply_slippage("BUY", 2.3) == 2.30046
    assert paper._apply_slippage("SELL", 2.3) == 2.29954
    # 3 x 0.7 x 4 bps = 0.00084; the float product chain was 0.0008399999999999999.
    assert paper._fee(3.0, 0.7) == 0.00084
    # A notional order: 0.3 USDT at 0.1 is 3 units; the float quotient was 2.9999999999999996.
    flat = _paper(slippage_bps=0.0, fee_bps=0.0)
    assert flat.open_position(symbol="Z", side="BUY", notional_usdt=0.3, fallback_price=0.1).filled_qty == 3.0


def test_paper_non_finite_price_still_fails_closed():
    paper = _paper()
    result = paper.open_position(symbol="X", side="BUY", notional_usdt=100.0, fallback_price=float("nan"))
    # NaN is not a usable reference price (and there is no client to ask): an error result, not an exception.
    assert not result.success and result.status == "PAPER_ERROR"


# ═════════════════════════════════════════════════════════════════════════════════════════ executor
def _executor(client=None, mode="live"):
    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    ex.client = client or MagicMock()
    ex.circuit = MagicMock(is_tripped=lambda: False)
    ex.audit = None
    ex.run_id = None
    ex.execution_mode = mode
    ex._live_symbols_override = {"BTCUSDT"}
    return ex


def _spec(step="0.001", min_qty="0.001", min_notional="5", contract_size="1"):
    return SimpleNamespace(step_size=Decimal(step), min_qty=Decimal(min_qty), min_notional=Decimal(min_notional),
                           contract_size=Decimal(contract_size), tick_size=Decimal("0.01"))


@pytest.fixture
def sizing(monkeypatch):
    """``_size_qty`` with a registry spec (not production) and a fixed venue price."""
    from app.execution import executor as module
    monkeypatch.setattr(module.settings, "MIN_NOTIONAL_USDT", 0.0, raising=False)
    registry = MagicMock()
    registry.get_spec.return_value = _spec()
    monkeypatch.setattr(module, "get_instrument_registry", lambda: registry)

    def run(usdt, price, *, leverage, sl_price=0.0):
        client = MagicMock()
        client.get_prices.return_value = {"BTCUSDT": price}
        return _executor(client)._size_qty("BTCUSDT", usdt, 1.0, sl_price=sl_price, leverage_override=leverage)
    return run


def test_size_qty_keeps_the_last_lot_when_margin_round_trips_through_leverage(sizing):
    """1000 notional at 3x and 100: the margin 1000/3 was the float 333.3333333333333, whose decimal x 3 is
    999.9999999999999, so the quantity floored to 9.999 instead of 10.000 (one lot lost at an exact boundary)."""
    qty, details = sizing(1000.0, 100.0, leverage=3)
    assert qty == 10.0
    assert details["leverage"] == 3


def test_size_qty_still_rounds_down_to_the_step(sizing):
    qty, _ = sizing(1000.0, 99.97, leverage=3)       # 10.003000900... -> 10.003
    assert qty == 10.003
    qty, _ = sizing(123.4567, 1.0, leverage=7)
    assert qty == 123.456


def test_liquidation_leverage_cap_is_inclusive_at_the_exact_bound(sizing):
    """Stop 95.335 under 100 is a 4.665 % stop; with 2 x 1.5 bps slippage the bound 1/(2 x 4.695 % + 0.61 %) is
    exactly 10x. The float denominator was 0.10000000000000012, which truncated the cap to 9x."""
    _, details = sizing(1000.0, 100.0, leverage=20, sl_price=95.335)
    assert details["leverage"] == 10
    # A hair wider stop is below the bound: the cap stays strictly at 9x.
    _, details = sizing(1000.0, 100.0, leverage=20, sl_price=95.334)
    assert details["leverage"] == 9


def _tp1_client(pos_amt, post_amt, price=50_000.0):
    client = MagicMock()
    client.get_position_amt.side_effect = [pos_amt, post_amt]
    client.get_prices.return_value = {"BTCUSDT": price}
    client.get_symbol_filters.return_value = SimpleNamespace(step_size=0.001, min_qty=0.001, min_notional=5.0)
    client.capabilities = SimpleNamespace(supports_reduce_only=True)
    client.place_order.return_value = MagicMock(model_dump=lambda: {"orderId": "1", "status": "FILLED"})
    client.update_protection.return_value = {"status": "OK", "sl_order_id": "S", "tp_order_id": "T"}
    return client


def test_tp1_quantity_is_not_floored_a_lot_below_the_exact_fraction(monkeypatch):
    """70 % of 0.01 is 0.007. The float product was 0.006999999999999999, floored to 0.006 at a 0.001 step, so
    TP1 closed one lot less than configured."""
    monkeypatch.setattr("time.sleep", lambda s: None)
    client = _tp1_client(0.01, 0.003)
    result = _executor(client).execute_tp1_partial_close(
        symbol="BTCUSDT", live_qty=0.01, position_side="LONG", sl_price=49_000.0, tp_price=53_000.0,
        tp1_fraction=0.7)
    assert result["requested_tp1_qty"] == 0.007
    assert result["normalized_tp1_qty"] == 0.007
    assert client.place_order.call_args[0][0].qty == Decimal("0.007")
    assert result["fill_qty"] == 0.007
    assert result["runner_qty"] == 0.003


def test_tp1_quantity_still_rounds_down_to_the_step(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    client = _tp1_client(0.013, 0.007)
    result = _executor(client).execute_tp1_partial_close(
        symbol="BTCUSDT", live_qty=0.013, position_side="LONG", sl_price=49_000.0, tp_price=53_000.0,
        tp1_fraction=0.5)
    assert result["requested_tp1_qty"] == 0.0065
    assert result["normalized_tp1_qty"] == 0.006


def _runner_pm(entry, stop, qty, highest, lowest):
    from app.execution.position_manager import PositionManager, PositionPhase, PositionSide
    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.LONG, entry, qty * 2, stop, entry * 1.02, entry * 1.05)
    pos = pm.get_position("BTCUSDT")
    pos.current_qty = qty
    pos.tp.tp1_hit = True
    pos.sl.be_exchange_confirmed = True
    pos.sl.current_stop = stop
    pos.highest_since_entry, pos.lowest_since_entry = highest, lowest
    pos.phase = PositionPhase.RUNNER_TRAILING
    return pm


def test_trailing_stop_is_not_a_tick_below_the_exact_anchor(monkeypatch):
    """Highest 0.3 less 1 x ATR 0.1 is 0.2. The float difference 0.19999999999999998 was rounded DOWN (a LONG
    stop) to 0.1999 at a 0.0001 tick: a stop one tick looser than the rule."""
    import app.exchange.binance.filters as filters
    monkeypatch.setattr(filters, "_tick", lambda symbol: 0.0001)
    pm = _runner_pm(0.25, 0.21, 10.0, 0.3, 0.2)
    result = _executor(_tp1_client(10.0, 10.0)).execute_trailing_stop_update(
        symbol="BTCUSDT", position_side="LONG", runner_qty=10.0, entry_price=0.25, current_stop=0.21,
        highest_since_entry=0.3, lowest_since_entry=0.2, atr=0.1, sl_order_id="S", tp_order_id="T",
        tp2_price=None, position_manager=pm, trail_atr_mult=1.0, min_update_interval_s=0)
    assert result["raw_trailing_stop"] == 0.2
    assert result["normalized_trailing_stop"] == 0.2
    assert result["skip_reason"] == "TRAILING_WOULD_LOOSEN"   # 0.2 is below the confirmed 0.21: never loosened


def test_break_even_stop_is_not_a_tick_below_the_exact_price(monkeypatch):
    """LONG entered at 100.02 with its initial stop at 90.07: 20 % of the 9.95 stop distance is a 1.99 buffer, so
    break-even is 102.01. The float chain (distance 9.950000000000003, buffer 1.9900000000000007) gave
    102.00999999999999, rounded DOWN (a LONG stop) to 102.00 at a 0.01 tick. Exact: 102.01."""
    import app.exchange.binance.filters as filters
    from app.execution.position_manager import PositionManager, PositionPhase, PositionSide
    monkeypatch.setattr(filters, "_tick", lambda symbol: 0.01)
    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.LONG, 100.02, 2.0, 90.07, 102.0, 105.0)
    pos = pm.get_position("BTCUSDT")
    pos.current_qty = 1.0
    pos.tp.tp1_hit = True
    pos.sl.initial_stop = 90.07
    pos.phase = PositionPhase.RUNNER_TRAILING
    result = _executor(_tp1_client(1.0, 1.0), mode="paper").execute_break_even_update(
        symbol="BTCUSDT", position_side="LONG", runner_qty=1.0, entry_price=100.02, current_stop=90.07,
        position_manager=pm)
    assert result["sl_distance"] == 9.95
    assert result["buffer_amount"] == 1.99
    assert result["buffered_break_even_price"] == 102.01
    assert result["normalized_break_even_price"] == 102.01
    assert pm.get_position("BTCUSDT").sl.current_stop == 102.01


def test_configured_percent_exposure_is_exact():
    ex = _executor()
    ex._allocation_type, ex._allocation_value, ex._max_notional_per_symbol = "percent_equity", 3.0, 0.0
    assert ex._configured_max_exposure(3333.3) == 99.999       # 3 % of 3333.3; float: 99.99900000000001


# ═══════════════════════════════════════════════════════════════════════════ SQL SUM -> limit / total
@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


def test_entry_exposure_sum_meets_an_exact_limit(db):
    """Two in-flight entries of 100.03 and 200.3 notional against a 300.33 per-symbol limit. SQL SUM over REAL gave
    300.33000000000004 > 300.33, so the executor's post-sizing exposure guard refused an entry that is exactly at
    its limit. The sum is now exact."""
    from app.execution.entry_protection import EntryProtection
    ep = EntryProtection(db)
    for side, notional in (("LONG", 100.03), ("SHORT", 200.3)):
        assert ep.acquire_intent(bot_id="b", symbol="BTCUSDT", side=side, intended_notional=notional,
                                 client_order_id=f"c-{side}", intent_key=f"k-{side}", allow_hedge=True)
        ep.mark_sized("b", "BTCUSDT", side, sized_notional=notional, sized_qty=0.01, reference_price=notional * 100)
    exposure = ep.get_effective_exposure("b", "BTCUSDT")
    assert exposure == 300.33
    assert not exposure > 300.33


def _open_position(db, pid, margin, *, bot="bot-e02", status="OPEN"):
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO positions (position_id, bot_instance_id, symbol, side, original_qty, remaining_qty, "
            "status, opened_at, committed_margin) VALUES (?,?,?,?,?,?,?,?,?)",
            (pid, bot, "BTCUSDT", "LONG", 1.0, 1.0, status, "2026-10-01T00:00:00+00:00", margin))


def test_committed_margin_of_many_positions_meets_the_portfolio_limit_exactly(db):
    """Ten open positions of 0.1 margin plus one of 0.4: SQLite's REAL SUM (compensated) gives 1.0 for the ten
    but 1.4000000000000001 with the eleventh: more margin than the positions hold, compared with the portfolio
    margin limit. Exact: 1.0 and 1.4."""
    from app.risk.capital_ledger import CapitalLedger
    for i in range(10):
        _open_position(db, f"p{i}", 0.1)
    _open_position(db, "closed", 5.0, status="CLOSED")
    ledger = CapitalLedger(db, bot_instance_id="bot-e02", per_trade_allocation=1.0, portfolio_margin_limit=1.0)
    assert ledger.committed_margin() == 1.0
    _open_position(db, "big", 0.4)
    assert ledger.committed_margin() == 1.4


def test_user_stream_fill_ledger_total_is_exact(tmp_path):
    """Ten partial fills of 0.1 and one of 0.4 on one order are the venue's cumulative 1.4; SQLite's REAL SUM
    gave 1.4000000000000001 (a plain float SUM, as on older SQLite, gives 0.9999999999999999 for the ten)."""
    from app.exchange.platform_adapter import contract as C
    from app.exchange.platform_adapter import reconcile as R
    from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
    database = DB(str(tmp_path / "stream.db"))
    ensure_cati_platform_schema(database)
    ledger = R.FillLedger(database, venue=C.BINANCE, environment="demo", account_ref="acct")
    with database.connect() as conn:
        for i, quantity in enumerate([0.1] * 10 + [0.4]):
            fill = C.FillInfo(C.BINANCE, "BTCUSDT", f"t{i}", "o1", "c1", C.BUY, quantity, 60_000.0, 0.01, "USDT",
                              0.0, False, 1_000 + i)
            assert ledger.record(conn, fill, R.REST, 2_000) == R.NEW_FILL
    assert ledger.filled_quantity("BTCUSDT", "o1") == 1.4
    assert ledger.filled_quantity("BTCUSDT", "missing") == 0.0


# ════════════════════════════════════════════════════════════════════════════════ fill evidence bridge
BOT = "bot_e02_evidence"
RUN_ID = "run_e02_evidence"


class _Context:
    bot_instance_id = BOT
    user_id = "user_e02"
    broker_account_id = "brk_e02"
    broker_environment = "demo"
    market_type = "CRYPTO"
    effective_policy_hash = "policy_e02"


class _Runner:
    def __init__(self, db):
        self.db, self.run_id, self.cycle_id = db, RUN_ID, "cyc_e02"
        self.context = _Context()
        self.position_manager = None
        self._active_decision_ids = {"BTCUSDT": "dec_e02"}

    def _effective_execution_mode(self):
        return "paper"


@pytest.fixture
def runner(db):
    from app.evidence.writers import open_bot_run, open_runtime_session
    from shared_lib.persistence.evidence_schema import PAPER_FORWARD_VALIDATION
    session = open_runtime_session(db, database_role="development", database_path=":memory:",
                                   process_execution_mode="paper", environment_name="e02-tests")
    open_bot_run(db, run_id=RUN_ID, bot_instance_id=BOT, runtime_session_id=session, user_id="user_e02",
                 policy_hash="policy_e02", provenance=PAPER_FORWARD_VALIDATION, execution_mode="paper",
                 broker_environment="demo")
    return _Runner(db)


def _position(db, pid="pos_e02"):
    with db.connect() as conn:
        return dict(conn.execute("SELECT * FROM positions WHERE position_id=?", (pid,)).fetchone())


def _fill(**kw):
    base = {"symbol": "BTCUSDT", "side": "LONG", "action": "OPEN", "qty": 0.3, "price": 100.0,
            "position_id": "pos_e02", "leverage": 1.0}
    base.update(kw)
    return base


def test_fill_bridge_partial_close_leaves_exact_remainder_and_releases_exact_margin(db, runner):
    """0.3 at 100 (30 margin), then 0.1 closed: the float remainder was 0.19999999999999998; exact: 0.2, with 20
    of the 30 margin still committed. The final close of 0.2 leaves the row exactly flat."""
    from app.evidence.fill_bridge import project_fill
    project_fill(runner, db, _fill())
    project_fill(runner, db, _fill(action="PARTIAL_CLOSE", qty=0.1, price=101.0, realized_pnl=0.1, fee=0.0101))
    row = _position(db)
    assert row["remaining_qty"] == 0.2
    assert row["realized_qty"] == 0.1
    assert row["committed_margin"] == 20.0
    assert row["status"] == "OPEN"
    project_fill(runner, db, _fill(action="CLOSE", qty=0.2, price=102.0, realized_pnl=0.4, fee=0.0204))
    row = _position(db)
    assert row["status"] == "CLOSED" and row["remaining_qty"] == 0.0
    assert row["realized_qty"] == 0.3
    assert row["realized_pnl"] == 0.5
    assert row["fees"] == 0.0305


def test_fill_bridge_second_partial_close_releases_only_its_own_share(db, runner):
    """0.3 at 100 (30 margin) closed in legs of 0.1: 20 then 10 stay committed. The bridge scaled the row's
    CURRENT margin by remaining / ORIGINAL quantity, so the second leg left 20 x 0.1 / 0.3 = 6.67
    (6.666666666666665 in floats) committed for the 0.1 still open: the first leg's share released twice."""
    from app.evidence.fill_bridge import project_fill
    project_fill(runner, db, _fill())
    project_fill(runner, db, _fill(action="PARTIAL_CLOSE", qty=0.1, price=100.0, realized_pnl=0.0, fee=0.0))
    assert _position(db)["committed_margin"] == 20.0
    project_fill(runner, db, _fill(action="PARTIAL_CLOSE", qty=0.1, price=100.0, realized_pnl=0.0, fee=0.0))
    row = _position(db)
    assert row["remaining_qty"] == 0.1 and row["committed_margin"] == 10.0
    project_fill(runner, db, _fill(action="CLOSE", qty=0.1, price=100.0, realized_pnl=0.0, fee=0.0))
    row = _position(db)
    assert row["status"] == "CLOSED" and row["committed_margin"] == 0.0


def test_fill_bridge_adds_and_signed_pnl_accumulate_exactly(db, runner):
    """0.1 then an ADD of 0.2 is 0.3 held (float: 0.30000000000000004, whose closes of 0.1 and 0.2 left 2.8e-17
    of dust); a loss of -0.1 and a gain of 0.3 net to 0.2 (float: 0.19999999999999998), signed like funding."""
    from app.evidence.fill_bridge import project_fill
    project_fill(runner, db, _fill(qty=0.1, price=100.0))
    project_fill(runner, db, _fill(qty=0.2, price=100.0))
    row = _position(db)
    assert row["original_qty"] == 0.3 and row["remaining_qty"] == 0.3
    assert row["committed_margin"] == 30.0
    project_fill(runner, db, _fill(action="PARTIAL_CLOSE", qty=0.1, price=99.0, realized_pnl=-0.1, fee=0.0))
    project_fill(runner, db, _fill(action="CLOSE", qty=0.2, price=101.5, realized_pnl=0.3, fee=0.0))
    row = _position(db)
    assert row["status"] == "CLOSED"
    assert row["realized_qty"] == 0.3
    assert row["realized_pnl"] == 0.2                    # float: 0.19999999999999998


def test_fill_bridge_900_partial_fees_sum_to_the_exact_total(db, runner):
    """900 partial closes each paying a 0.1 fee: a REAL running total read back and added once per fill came to
    89.99999999999916. The position row now holds 90 exactly, and the remainder is exact too."""
    from app.evidence.fill_bridge import project_fill
    project_fill(runner, db, _fill(qty=100.0, price=10.0))
    for _ in range(900):
        project_fill(runner, db, _fill(action="PARTIAL_CLOSE", qty=0.1, price=10.0, realized_pnl=0.0, fee=0.1))
    row = _position(db)
    assert row["fees"] == 90.0
    assert row["remaining_qty"] == 10.0
    assert row["realized_qty"] == 90.0
    assert row["committed_margin"] == 100.0             # 1000 x (10 / 100)


# ══════════════════════════════════════════════════════════════════════════════════ close PnL helper
def test_calculate_close_pnl_large_notional_is_exact():
    """10,000 BTC from 60000.1 to 60000.3: the float gross was 2000.0000000436557, which rounded to 8 places is
    2000.00000004; exact: 2000."""
    from shared_lib.persistence.trade_fills import calculate_close_pnl
    r = calculate_close_pnl(side="LONG", entry_price=60000.1, exit_price=60000.3, quantity=10_000.0,
                            entry_fee=0.1, exit_fee=0.2, funding_fees=-0.7, initial_risk=400.0)
    assert r["gross_pnl"] == 2000.0
    assert r["total_fees"] == -0.4                       # signed funding: a received payment lowers the costs
    assert r["funding_fees"] == -0.7
    assert r["net_pnl"] == 2000.4
    assert r["r_multiple"] == 5.001


def test_calculate_close_pnl_short_and_fractional_quantities():
    from shared_lib.persistence.trade_fills import calculate_close_pnl
    r = calculate_close_pnl(side="SHORT", entry_price=0.3, exit_price=0.1, quantity=0.7, entry_fee=0.1,
                            exit_fee=0.2, slippage_cost=0.0)
    assert r["gross_pnl"] == 0.14
    assert r["total_fees"] == 0.3
    assert r["net_pnl"] == -0.16
    assert r["r_multiple"] is None


def test_calculate_close_pnl_non_finite_input_keeps_float_semantics():
    import math
    from shared_lib.persistence.trade_fills import calculate_close_pnl
    r = calculate_close_pnl(side="LONG", entry_price=1.0, exit_price=float("nan"), quantity=1.0)
    assert math.isnan(r["gross_pnl"]) and math.isnan(r["net_pnl"])
