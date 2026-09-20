from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest


def _store(tmp_path, bot_id: str = "bot-a"):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    from shared_lib.persistence.state_store import StateStore

    db_path = str(tmp_path / "weekly-drawdown.db")
    migrate(db_path=db_path)
    return StateStore(db=DB(db_path), bot_instance_id=bot_id)


def _equity(
    store,
    *,
    timestamp: str,
    equity: float,
    wallet: float | None = None,
    bot_id: str = "bot-a",
    account_id: str = "account-a",
    source: str = "cycle_end",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    wallet = equity if wallet is None else wallet
    with store.db.connect() as conn:
        conn.execute(
            """
            INSERT INTO equity_snapshots (
                user_id, bot_instance_id, broker_account_id, broker_id,
                timestamp_utc, wallet_balance, equity, available_balance,
                unrealized_pnl, margin_used, currency, source, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "user-a", bot_id, account_id, "BINANCE", timestamp,
                wallet, equity, equity, equity - wallet, 0.0, "USDT", source, now, now,
            ),
        )


def _startup_settings(max_trades):
    return SimpleNamespace(
        DEFAULT_INTERVAL="15m",
        MAX_ADDS_PER_POSITION=0,
        MAX_WEEKLY_DRAWDOWN_PCT=5.0,
        MAX_MONTHLY_DRAWDOWN_PCT=10.0,
        DAILY_MAX_LOSS_USDT=50.0,
        MAX_TRADES_DAILY=max_trades,
        MAX_OPEN_POSITIONS=3,
        EXECUTION_MODE="paper",
        KILL_SWITCH_CLOSE_POSITIONS=True,
        EVENT_FILTER_ENABLED=True,
        NEWS_TRADING_ENABLED=False,
        HIGH_IMPACT_BLACKOUT_MINUTES_BEFORE=30,
        HIGH_IMPACT_BLACKOUT_MINUTES_AFTER=15,
        TRADE_USDT_PER_ORDER=50.0,
    )


def test_reconstructs_local_week_open_peak_and_low(tmp_path):
    store = _store(tmp_path)
    _equity(store, timestamp="2026-09-13T21:59:59+00:00", equity=786.0)
    _equity(store, timestamp="2026-09-13T22:01:00+00:00", equity=784.0)
    _equity(store, timestamp="2026-09-16T17:47:15+00:00", equity=802.0)
    _equity(store, timestamp="2026-09-20T09:20:00+00:00", equity=705.0)
    _equity(store, timestamp="2026-09-20T10:00:00+00:00", equity=900.0, bot_id="bot-b")
    _equity(store, timestamp="2026-09-20T10:01:00+00:00", equity=901.0, account_id="account-b")

    snapshot = store.reconstruct_weekly_snapshot(
        date(2026, 9, 14), broker_account_id="account-a"
    )

    assert snapshot is not None
    assert snapshot.start_equity == pytest.approx(786.0)
    assert snapshot.peak_equity == pytest.approx(802.0)
    assert snapshot.low_equity == pytest.approx(705.0)


def test_weekly_drawdown_is_peak_to_current(tmp_path):
    from app.risk.drawdown import DrawdownMonitor

    store = _store(tmp_path)
    _equity(store, timestamp="2026-09-13T22:01:00+00:00", equity=784.0)
    _equity(store, timestamp="2026-09-16T17:47:15+00:00", equity=802.0)
    snapshot = store.reconstruct_weekly_snapshot(
        date(2026, 9, 14), broker_account_id="account-a"
    )

    expected_pct = (802.0 - 705.0) / 802.0 * 100.0
    assert expected_pct == pytest.approx(12.0947630923)
    assert DrawdownMonitor(store).check_weekly_drawdown(5.0, 705.0, snapshot)


def test_first_in_week_sample_is_opening_when_it_is_closer_to_boundary(tmp_path):
    store = _store(tmp_path)
    _equity(store, timestamp="2026-09-13T21:00:00+00:00", equity=900.0)
    _equity(store, timestamp="2026-09-13T22:00:10+00:00", equity=800.0)

    snapshot = store.reconstruct_weekly_snapshot(
        date(2026, 9, 14), broker_account_id="account-a"
    )

    assert snapshot is not None
    assert snapshot.start_equity == pytest.approx(800.0)


def test_broker_equity_preserves_fees_and_unrealized_pnl_effects(tmp_path):
    store = _store(tmp_path)
    _equity(
        store,
        timestamp="2026-09-13T22:01:00+00:00",
        wallet=1000.0,
        equity=990.0,
    )
    _equity(
        store,
        timestamp="2026-09-13T22:02:00+00:00",
        wallet=995.0,
        equity=1010.0,
    )

    snapshot = store.reconstruct_weekly_snapshot(
        date(2026, 9, 14), broker_account_id="account-a"
    )

    assert snapshot.start_equity == pytest.approx(990.0)
    assert snapshot.peak_equity == pytest.approx(1010.0)


def test_replay_synthetic_and_legacy_snapshots_cannot_set_baseline(tmp_path):
    store = _store(tmp_path)
    for index, source in enumerate(("replay", "synthetic", "LEGACY_BACKFILL")):
        _equity(
            store,
            timestamp=f"2026-09-13T22:0{index}:00+00:00",
            equity=5000.0 + index,
            source=source,
        )
    _equity(store, timestamp="2026-09-13T22:04:00+00:00", equity=800.0)

    snapshot = store.reconstruct_weekly_snapshot(
        date(2026, 9, 14), broker_account_id="account-a"
    )

    assert snapshot is not None
    assert snapshot.start_equity == pytest.approx(800.0)
    assert snapshot.peak_equity == pytest.approx(800.0)


def test_week_reset_is_local_date_based_and_preserves_prior_week(tmp_path):
    from app.risk.drawdown import DrawdownMonitor

    store = _store(tmp_path)
    monitor = DrawdownMonitor(store)
    monitor.update_snapshots(date(2026, 9, 20), 800.0)
    monitor.update_snapshots(date(2026, 9, 21), 750.0)

    prior = store.load_weekly_snapshot(date(2026, 9, 14))
    current = store.load_weekly_snapshot(date(2026, 9, 21))
    assert prior is not None and prior.start_equity == pytest.approx(800.0)
    assert current is not None and current.start_equity == pytest.approx(750.0)


def test_snapshot_survives_restart_and_continues_peak_low_tracking(tmp_path):
    from app.risk.drawdown import DrawdownMonitor
    from shared_lib.persistence.state_store import StateStore

    store = _store(tmp_path)
    monitor = DrawdownMonitor(store)
    monitor.update_snapshots(date(2026, 9, 14), 780.0)
    monitor.update_snapshots(date(2026, 9, 15), 810.0)

    restarted = StateStore(db=store.db, bot_instance_id="bot-a")
    DrawdownMonitor(restarted).update_snapshots(date(2026, 9, 16), 700.0)
    snapshot = restarted.load_weekly_snapshot(date(2026, 9, 14))

    assert snapshot is not None
    assert snapshot.start_equity == pytest.approx(780.0)
    assert snapshot.peak_equity == pytest.approx(810.0)
    assert snapshot.low_equity == pytest.approx(700.0)


@pytest.mark.parametrize("disabled_value", [None, 0])
def test_startup_validator_accepts_disabled_daily_trade_cap(disabled_value):
    from app.core.safety_startup import run_startup_safety_check

    report = run_startup_safety_check(_startup_settings(disabled_value), mode="paper")
    check = next(item for item in report.checks if item.name == "MAX_TRADES_DAILY")
    assert check.passed
    assert check.message == "Daily trade cap disabled"


@pytest.mark.parametrize("invalid_value", [-1, 21, 1.5, True])
def test_startup_validator_rejects_invalid_daily_trade_cap(invalid_value):
    from app.core.safety_startup import run_startup_safety_check

    report = run_startup_safety_check(_startup_settings(invalid_value), mode="paper")
    check = next(item for item in report.checks if item.name == "MAX_TRADES_DAILY")
    assert not check.passed
