"""Performance calibration reads realised R from canonical lineage -- and says so when it cannot.

The audited defect: ``SqlitePerformanceSource`` selected ``positions.risk_amount``,
a column that never existed, on every evaluated candle. The SQL error was
swallowed, turned into ``[]``, and reported as ``INSUFFICIENT_SAMPLE`` -- a broken
subsystem dressed up as a data shortage.

The source now joins ``positions`` to ``trading_decisions`` on ``decision_id``,
computes net R = (gross realised P&L - all fees) / approved risk, and raises
when it cannot be read, which the calibrator reports as ``UNAVAILABLE``.
"""
from __future__ import annotations

import pytest

from app.threshold.calibration import (
    REALIZED_R_QUERY,
    PerformanceCalibrator,
    PerformanceSourceUnavailable,
    SqlitePerformanceSource,
)
from app.threshold.contracts import CalibrationStatus
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

BOT = "bot_calibration"
SYMBOL = "BTCUSDT"


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


def _closed_trade(
    db, n: int, *, gross: float, risk: float | None, entry_fee: float | None = 0.1,
    exit_fees: tuple[float, ...] = (0.1,), provenance: str = "PAPER_FORWARD",
    status: str = "CLOSED", remaining: float = 0.0, added: bool = False,
) -> None:
    """One position with its originating decision and its fills."""
    decision_id = f"dec_{n}"
    position_id = f"pos_{n}"
    closed_at = f"2026-01-01T00:{n % 60:02d}:00+00:00"
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO trading_decisions (decision_id, bot_instance_id, symbol, "
            "evaluated_at, risk_amount, provenance, complete) VALUES (?,?,?,?,?,?,1)",
            (decision_id, BOT, SYMBOL, closed_at, risk, provenance),
        )
        conn.execute(
            "INSERT INTO positions (position_id, bot_instance_id, symbol, side, "
            "original_qty, remaining_qty, realized_qty, entry_price, realized_pnl, fees, "
            "status, opened_at, closed_at, decision_id, provenance) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (position_id, BOT, SYMBOL, "LONG", 1.0, remaining, 1.0 - remaining, 100.0,
             gross, sum(exit_fees), status, closed_at,
             closed_at if status == "CLOSED" else None, decision_id, provenance),
        )
        conn.execute(
            "INSERT INTO trade_fills (symbol, side, action, qty, price, fee, "
            "timestamp_utc, position_id) VALUES (?,?,?,?,?,?,?,?)",
            (SYMBOL, "LONG", "OPEN", 1.0, 100.0, entry_fee, closed_at, position_id),
        )
        for leg, fee in enumerate(exit_fees):
            conn.execute(
                "INSERT INTO trade_fills (symbol, side, action, qty, price, fee, "
                "realized_pnl, timestamp_utc, position_id) VALUES (?,?,?,?,?,?,?,?,?)",
                (SYMBOL, "LONG", "CLOSE", 1.0 / len(exit_fees), 101.0, fee,
                 gross / len(exit_fees), closed_at, position_id),
            )
        if added:
            conn.execute(
                "INSERT INTO position_events (event_id, position_id, bot_instance_id, "
                "symbol, event_type, occurred_at, provenance) VALUES (?,?,?,?,?,?,?)",
                (f"pev_{n}", position_id, BOT, SYMBOL, "ADDED", closed_at, provenance),
            )


def _r(db):
    return list(SqlitePerformanceSource(db).recent_r_multiples(
        bot_instance_id=BOT, symbol=SYMBOL, timeframe="15m", limit=100,
    ))


# ── Schema contract ─────────────────────────────────────────────────────────


def test_the_query_runs_against_the_real_migrated_schema(db):
    """The regression: the old query referenced a column the schema never had."""
    with db.connect() as conn:
        conn.execute(REALIZED_R_QUERY, (BOT, SYMBOL, 10)).fetchall()


def test_positions_still_has_no_risk_amount_and_the_query_does_not_need_one(db):
    with db.connect() as conn:
        columns = {r[1] for r in conn.execute("PRAGMA table_info(positions)")}
    assert "risk_amount" not in columns
    assert "positions.risk_amount" not in REALIZED_R_QUERY
    assert "JOIN trading_decisions" in REALIZED_R_QUERY


# ── Economics ───────────────────────────────────────────────────────────────


def test_r_is_net_of_every_fee_over_the_approved_risk(db):
    _closed_trade(db, 1, gross=2.0, risk=1.0, entry_fee=0.1, exit_fees=(0.1,))
    assert _r(db) == [pytest.approx((2.0 - 0.2) / 1.0)]


def test_partial_closes_aggregate_into_one_complete_result(db):
    _closed_trade(db, 1, gross=3.0, risk=1.5, entry_fee=0.2, exit_fees=(0.1, 0.1, 0.1))
    assert _r(db) == [pytest.approx((3.0 - 0.5) / 1.5)]


def test_a_losing_trade_is_negative_r(db):
    _closed_trade(db, 1, gross=-1.0, risk=1.0, entry_fee=0.1, exit_fees=(0.1,))
    assert _r(db) == [pytest.approx(-1.2)]


# ── Only complete, organic results count ────────────────────────────────────


@pytest.mark.parametrize("kwargs", [
    {"risk": None},                                  # no approved risk
    {"risk": 0.0},                                   # no approved risk
    {"entry_fee": None},                             # net result unknowable
    {"provenance": "REPLAY"},                        # never calibrates production
    {"provenance": "TEST_FIXTURE"},
    {"status": "OPEN", "remaining": 1.0},            # not a result yet
    {"remaining": 0.5},                              # partially open
    {"added": True},                                 # risk differs from the decision's
])
def test_incomplete_or_non_organic_results_are_excluded(db, kwargs):
    params = {"gross": 1.0, "risk": 1.0}
    params.update(kwargs)
    _closed_trade(db, 1, **params)
    assert _r(db) == []


# ── Failure semantics ───────────────────────────────────────────────────────


def test_an_unreadable_source_raises_instead_of_returning_empty():
    unmigrated = DB(":memory:")  # base tables only: no canonical positions
    with pytest.raises(PerformanceSourceUnavailable):
        _r(unmigrated)


def test_a_query_failure_is_unavailable_not_insufficient_sample():
    result = PerformanceCalibrator(SqlitePerformanceSource(DB(":memory:"))).evaluate(
        bot_instance_id=BOT, symbol=SYMBOL, timeframe="15m",
        min_samples=30, lookback=100, bound=0.05,
    )
    assert result.status == CalibrationStatus.UNAVAILABLE
    assert result.status != CalibrationStatus.INSUFFICIENT_SAMPLE
    assert result.adjustment == 0.0
    assert result.score is None


def test_a_scoring_failure_is_error_and_neutral(db, monkeypatch):
    _closed_trade(db, 1, gross=1.0, risk=1.0)

    def boom(*_args, **_kwargs):
        raise ArithmeticError("scoring broke")

    monkeypatch.setattr(PerformanceCalibrator, "score", staticmethod(boom))
    result = PerformanceCalibrator(SqlitePerformanceSource(db)).evaluate(
        bot_instance_id=BOT, symbol=SYMBOL, timeframe="15m",
        min_samples=1, lookback=100, bound=0.05,
    )
    assert result.status == CalibrationStatus.ERROR
    assert result.adjustment == 0.0


def test_fewer_than_thirty_samples_is_insufficient_sample(db):
    for n in range(29):
        _closed_trade(db, n, gross=1.0, risk=1.0)
    result = PerformanceCalibrator(SqlitePerformanceSource(db)).evaluate(
        bot_instance_id=BOT, symbol=SYMBOL, timeframe="15m",
        min_samples=30, lookback=100, bound=0.05,
    )
    assert result.status == CalibrationStatus.INSUFFICIENT_SAMPLE
    assert result.sample_size == 29
    assert result.adjustment == 0.0


def test_thirty_samples_produce_a_deterministic_adjustment(db):
    grosses = [1.5, -0.5, 2.0, -1.0, 0.8] * 6
    for n, gross in enumerate(grosses):
        _closed_trade(db, n, gross=gross, risk=1.0, entry_fee=0.05, exit_fees=(0.05,))
    calibrator = PerformanceCalibrator(SqlitePerformanceSource(db))
    kwargs = dict(bot_instance_id=BOT, symbol=SYMBOL, timeframe="15m",
                  min_samples=30, lookback=100, bound=0.05)
    first = calibrator.evaluate(**kwargs)
    second = calibrator.evaluate(**kwargs)
    expected = PerformanceCalibrator.score(
        [(g - 0.1) / 1.0 for g in grosses], min_samples=30, bound=0.05,
    )
    assert first.status == CalibrationStatus.OK
    assert first.sample_size == 30
    assert first == second
    assert first.adjustment == pytest.approx(expected.adjustment)
    assert abs(first.adjustment) <= 0.05
