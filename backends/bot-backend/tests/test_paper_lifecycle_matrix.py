"""Phase 4 — the complete paper position lifecycle, end to end.

This is the blueprint's Test A–L matrix.  The invariant it exists to protect:

    OPEN 1.0 -> TP1 0.5 -> remaining 0.5 -> FINAL CLOSE 0.5 -> FLAT

Any path that closes 1.0 after the TP1 is a defect, and so is any path that
reports FLAT without persisted close evidence.
"""
from __future__ import annotations

import pytest

from app.execution.executor import BinanceExecutor
from app.execution.paper_executor import (
    PAPER_CLOSE_QUANTITY_MISMATCH,
    PAPER_CLOSE_QUANTITY_UNKNOWN,
    PAPER_CLOSE_SIDE_UNKNOWN,
    PaperExecutor,
)

QTY = 1.0
TP1_QTY = 0.5
TOLERANCE = 1e-9


class PriceOnlyClient:
    """A market-data-only client. Any order API call is a test failure."""

    def __init__(self, price: float = 100.0) -> None:
        self.price = price
        self.live_order_calls: list[str] = []

    def get_prices(self, symbols):
        return {symbol: float(self.price) for symbol in symbols}

    def get_ticker(self, symbol):
        return {"symbol": symbol, "lastPrice": str(self.price)}

    def klines(self, **_kwargs):
        return []

    def _forbidden(self, name):
        self.live_order_calls.append(name)
        raise AssertionError(f"paper execution must not call {name}")

    def place_order(self, *a, **k):
        self._forbidden("place_order")

    def close_position_market(self, *a, **k):
        self._forbidden("close_position_market")

    def update_protection(self, *a, **k):
        self._forbidden("update_protection")

    def get_position_amt(self, *a, **k):
        self._forbidden("get_position_amt")

    def get_position_info(self, *a, **k):
        self._forbidden("get_position_info")


@pytest.fixture
def client():
    return PriceOnlyClient(price=100.0)


@pytest.fixture
def paper(client):
    return PaperExecutor(client=client)


def open_position(paper, side="LONG", qty=QTY):
    return paper.open_position(
        symbol="BTCUSDT", side=side, notional_usdt=0.0, quantity=qty, fallback_price=100.0
    )


# ── Test A — Open ────────────────────────────────────────────────────────────


def test_a_open_records_original_and_remaining_quantity(paper, client):
    result = open_position(paper)

    assert result.success is True
    assert result.filled_qty == pytest.approx(QTY)
    assert result.details["position_id"]
    assert result.details["fill_id"]

    position = paper.get_position("BTCUSDT")
    assert position["status"] == "OPEN"
    assert position["original_qty"] == pytest.approx(QTY)
    assert position["remaining_qty"] == pytest.approx(QTY)
    assert position["realized_qty"] == pytest.approx(0.0)
    assert client.live_order_calls == []


# ── Test B — TP1 partial close ───────────────────────────────────────────────


def test_b_tp1_reduces_remaining_and_records_realized(paper, client):
    open_position(paper)

    tp1 = paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)

    assert tp1.success is True
    assert tp1.filled_qty == pytest.approx(TP1_QTY)
    assert tp1.details["fill_type"] == "TP1"
    assert tp1.details["remaining_qty"] == pytest.approx(QTY - TP1_QTY)
    assert tp1.details["fill_id"]

    position = paper.get_position("BTCUSDT")
    assert position["remaining_qty"] == pytest.approx(TP1_QTY)
    assert position["realized_qty"] == pytest.approx(TP1_QTY)
    assert position["status"] == "PARTIALLY_CLOSED"
    assert client.live_order_calls == []


def test_b_authoritative_remaining_quantity_agrees_after_tp1(paper):
    open_position(paper)
    tp1 = paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)

    # One authoritative remaining quantity, reported consistently everywhere.
    assert paper.remaining_quantity("BTCUSDT") == pytest.approx(TP1_QTY)
    assert paper.get_position("BTCUSDT")["remaining_qty"] == pytest.approx(TP1_QTY)
    assert tp1.details["remaining_qty"] == pytest.approx(TP1_QTY)


def test_b_tp1_cannot_exceed_the_remaining_quantity(paper):
    open_position(paper)
    result = paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=QTY)
    assert result.success is False


def test_b_tp1_side_must_match_the_open_position(paper):
    open_position(paper, side="LONG")
    result = paper.partial_close(symbol="BTCUSDT", position_side="SHORT", quantity=TP1_QTY)
    assert result.success is False


# ── Test C — Final close closes the remainder, not the original ──────────────


def test_c_final_close_executes_the_remainder_only(paper, client):
    open_position(paper)
    paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)

    remaining = paper.remaining_quantity("BTCUSDT")
    close = paper.close_position(symbol="BTCUSDT", position_side="LONG", quantity=remaining)

    assert close.success is True
    assert close.filled_qty == pytest.approx(TP1_QTY), "must close 0.5, never the original 1.0"
    assert close.details["remaining_qty"] == pytest.approx(0.0)
    assert paper.get_position("BTCUSDT") is None
    assert client.live_order_calls == []


def test_c_closing_the_original_quantity_after_tp1_is_rejected(paper):
    open_position(paper)
    paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)

    stale = paper.close_position(symbol="BTCUSDT", position_side="LONG", quantity=QTY)

    assert stale.success is False
    assert stale.details["reason_code"] == PAPER_CLOSE_QUANTITY_MISMATCH
    assert paper.remaining_quantity("BTCUSDT") == pytest.approx(TP1_QTY), "position survives"


# ── Test D — Short position ──────────────────────────────────────────────────


def test_d_short_lifecycle_uses_correct_execution_sides(paper, client):
    opened = open_position(paper, side="SHORT")
    assert opened.success is True

    tp1 = paper.partial_close(symbol="BTCUSDT", position_side="SHORT", quantity=TP1_QTY)
    assert tp1.success is True
    assert tp1.details["position_side"] == "SHORT"
    assert tp1.details["side"] == "BUY", "closing a short buys back"
    assert tp1.details["remaining_qty"] == pytest.approx(TP1_QTY)

    close = paper.close_position(symbol="BTCUSDT", position_side="SHORT", quantity=TP1_QTY)
    assert close.success is True
    assert close.details["position_before"] == "SHORT"
    assert close.filled_qty == pytest.approx(TP1_QTY)
    assert paper.get_position("BTCUSDT") is None
    assert client.live_order_calls == []


def test_d_short_profits_when_price_falls(client):
    paper = PaperExecutor(client=client, slippage_bps=0.0, fee_bps=0.0)
    open_position(paper, side="SHORT")
    client.price = 90.0

    tp1 = paper.partial_close(symbol="BTCUSDT", position_side="SHORT", quantity=TP1_QTY)

    assert tp1.details["realized_pnl"] > 0


def test_d_long_profits_when_price_rises(client):
    paper = PaperExecutor(client=client, slippage_bps=0.0, fee_bps=0.0)
    open_position(paper, side="LONG")
    client.price = 110.0

    tp1 = paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)

    assert tp1.details["realized_pnl"] > 0


# ── Tests E/F/G — Restart restores the authoritative quantity ────────────────


def test_e_restart_after_entry_restores_the_full_position(client):
    before = PaperExecutor(client=client)
    opened = open_position(before)
    snapshot = before.get_position("BTCUSDT")

    after = PaperExecutor(client=client)  # simulates a process restart
    after.seed_position(
        "BTCUSDT",
        snapshot["side"],
        snapshot["remaining_qty"],
        snapshot["entry_price"],
        position_id=snapshot["position_id"],
        original_quantity=snapshot["original_qty"],
    )

    restored = after.get_position("BTCUSDT")
    assert restored["side"] == "LONG"
    assert restored["original_qty"] == pytest.approx(QTY)
    assert restored["remaining_qty"] == pytest.approx(QTY)
    assert restored["position_id"] == opened.details["position_id"]


def test_f_restart_after_tp1_keeps_the_partial_remainder(client):
    before = PaperExecutor(client=client)
    open_position(before)
    before.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)
    snapshot = before.get_position("BTCUSDT")

    after = PaperExecutor(client=client)
    after.seed_position(
        "BTCUSDT",
        snapshot["side"],
        snapshot["remaining_qty"],
        snapshot["entry_price"],
        position_id=snapshot["position_id"],
        original_quantity=snapshot["original_qty"],
        realized_quantity=snapshot["realized_qty"],
    )

    assert after.remaining_quantity("BTCUSDT") == pytest.approx(TP1_QTY)
    assert after.remaining_quantity("BTCUSDT") != pytest.approx(QTY), "must not revert to 1.0"
    assert after.get_position("BTCUSDT")["original_qty"] == pytest.approx(QTY)
    assert after.get_position("BTCUSDT")["realized_qty"] == pytest.approx(TP1_QTY)


def test_f_restarted_runner_closes_only_the_remainder(client):
    before = PaperExecutor(client=client)
    open_position(before)
    before.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)
    snapshot = before.get_position("BTCUSDT")

    after = PaperExecutor(client=client)
    after.seed_position(
        "BTCUSDT", snapshot["side"], snapshot["remaining_qty"], snapshot["entry_price"]
    )

    close = after.close_position(
        symbol="BTCUSDT", position_side="LONG", quantity=after.remaining_quantity("BTCUSDT")
    )
    assert close.success is True
    assert close.filled_qty == pytest.approx(TP1_QTY)


def test_g_restart_after_full_close_stays_flat(client):
    before = PaperExecutor(client=client)
    open_position(before)
    before.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)
    before.close_position(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)
    assert before.get_position("BTCUSDT") is None

    after = PaperExecutor(client=client)  # nothing persisted to restore

    assert after.get_position("BTCUSDT") is None
    assert after.remaining_quantity("BTCUSDT") is None, "unknown, not a phantom zero"

    duplicate = after.close_position(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)
    assert duplicate.success is False, "no duplicate close"


def test_g_seeding_a_flat_position_is_a_no_op(client):
    paper = PaperExecutor(client=client)
    paper.seed_position("BTCUSDT", "LONG", 0.0, 100.0)
    assert paper.get_position("BTCUSDT") is None


# ── Tests H/I — CLOSE fails closed on unknown side or quantity ───────────────


def test_h_close_without_a_determinable_side_fails_closed(paper):
    result = paper.close_position(symbol="BTCUSDT", position_side=None, quantity=QTY)

    assert result.success is False
    assert result.details["reason_code"] == PAPER_CLOSE_SIDE_UNKNOWN
    assert result.filled_qty == 0.0


def test_h_close_never_silently_assumes_long(paper):
    result = paper.close_position(symbol="ETHUSDT", position_side="", quantity=QTY)
    assert result.success is False
    assert result.details["reason_code"] == PAPER_CLOSE_SIDE_UNKNOWN


def test_i_close_with_zero_quantity_fails_closed(paper):
    open_position(paper)
    result = paper.close_position(symbol="BTCUSDT", position_side="LONG", quantity=0.0)

    assert result.success is False
    assert result.details["reason_code"] == PAPER_CLOSE_QUANTITY_UNKNOWN
    assert paper.remaining_quantity("BTCUSDT") == pytest.approx(QTY), "position survives"


def test_i_close_with_negative_quantity_fails_closed(paper):
    open_position(paper)
    result = paper.close_position(symbol="BTCUSDT", position_side="LONG", quantity=-1.0)
    assert result.success is False
    assert result.details["reason_code"] == PAPER_CLOSE_QUANTITY_UNKNOWN


def test_i_close_of_an_unknown_position_fails_closed(paper):
    result = paper.close_position(symbol="DOGEUSDT", position_side="LONG", quantity=QTY)
    assert result.success is False
    assert result.filled_qty == 0.0


# ── Test J — Attribution ─────────────────────────────────────────────────────


def test_j_every_paper_event_carries_a_position_id_and_a_fill_id(paper):
    opened = open_position(paper)
    position_id = opened.details["position_id"]

    tp1 = paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)
    close = paper.close_position(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)

    for event in (opened, tp1, close):
        assert event.details["fill_id"], "every fill needs its own id"
        assert event.details["position_id"] == position_id, "one position, one id"

    # Order ids are per-execution and must not be reused as fill or position ids.
    order_ids = {opened.order_id, tp1.order_id, close.order_id}
    assert len(order_ids) == 3
    assert position_id not in order_ids


def test_j_runner_persists_tp1_with_distinct_bot_and_run_identifiers():
    """bot_instance_id and run_id are different columns and must stay distinct."""
    import inspect

    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner._persist_tp1_outcome)
    assert "bot_instance_id=self.context.bot_instance_id" in source
    assert "run_id=self.run_id" in source
    assert "position_id=st.position_id" in source
    assert "bot_instance_id=self.run_id" not in source, "run_id must never stand in for bot id"


def test_j_both_tp1_call_sites_persist_through_the_shared_helper():
    """The step_symbol site used to update memory only, losing the fill."""
    import inspect

    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner)
    assert source.count("self._persist_tp1_outcome(") == 2
    assert source.count("execute_tp1_partial_close(") == 2


# ── Test K — Quantity accounting invariant ───────────────────────────────────


def test_k_open_minus_partials_minus_final_is_zero(paper):
    opened = open_position(paper)
    tp1 = paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)
    close = paper.close_position(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)

    residual = opened.filled_qty - tp1.filled_qty - close.filled_qty
    assert abs(residual) <= TOLERANCE, f"unaccounted quantity: {residual}"
    assert paper.get_position("BTCUSDT") is None


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("fractions", [(0.5,), (0.25, 0.25), (0.1, 0.2, 0.3)])
def test_k_invariant_holds_across_multiple_partials(client, side, fractions):
    paper = PaperExecutor(client=client)
    opened = open_position(paper, side=side)

    partial_total = 0.0
    for fraction in fractions:
        result = paper.partial_close(symbol="BTCUSDT", position_side=side, quantity=fraction)
        assert result.success is True
        partial_total += result.filled_qty

    remaining = paper.remaining_quantity("BTCUSDT")
    close = paper.close_position(symbol="BTCUSDT", position_side=side, quantity=remaining)

    residual = opened.filled_qty - partial_total - close.filled_qty
    assert abs(residual) <= TOLERANCE
    assert paper.get_position("BTCUSDT") is None


def test_k_realized_plus_remaining_always_equals_original(paper):
    open_position(paper)
    paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=0.3)
    paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=0.2)

    position = paper.get_position("BTCUSDT")
    total = position["realized_qty"] + position["remaining_qty"]
    assert total == pytest.approx(position["original_qty"], abs=TOLERANCE)


# ── Test L — No exchange order submission, ever ──────────────────────────────


def test_l_full_lifecycle_never_calls_a_live_order_api(client):
    paper = PaperExecutor(client=client)
    open_position(paper)
    paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)
    paper.close_position(symbol="BTCUSDT", position_side="LONG", quantity=TP1_QTY)

    assert client.live_order_calls == []


def test_l_executor_in_paper_mode_never_reaches_the_exchange(client):
    executor = BinanceExecutor(client=client, execution_mode="paper")

    opened = executor.execute_signal("BTCUSDT", "BUY", 100.0)
    closed = executor.execute_signal(
        "BTCUSDT", "CLOSE", 0.0, position_side="BUY", remaining_quantity=opened.details["filled_qty"]
    )

    assert opened.success is True
    assert closed.success is True
    assert client.live_order_calls == []


def test_l_failed_paper_close_still_makes_no_exchange_call(client):
    executor = BinanceExecutor(client=client, execution_mode="paper")
    result = executor.execute_signal("BTCUSDT", "CLOSE", 0.0)

    assert result.success is False
    assert client.live_order_calls == []


# ── Paper truth is not broker truth ──────────────────────────────────────────


def test_paper_state_is_authoritative_and_not_reconciled_from_the_exchange(client):
    """A demo exchange is flat while a paper position is open. That is expected."""
    import inspect

    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner._step_symbol_orchestrated)
    assert 'if self._effective_execution_mode() == "broker":' in source
    assert "get_position_info" in source

    paper = PaperExecutor(client=client)
    open_position(paper)
    # The client would raise if the runner tried to read exchange positions here.
    assert paper.remaining_quantity("BTCUSDT") == pytest.approx(QTY)
    assert client.live_order_calls == []
