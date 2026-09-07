from __future__ import annotations

import inspect
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "bot-backend"))
sys.path.insert(0, str(ROOT / "backends" / "bot-backend" / "scripts"))

from app.signals.signal_expiry import SignalExpiryUpdater  # noqa: E402
from app.signals.signal_performance import SignalPerformanceUpdater  # noqa: E402
import update_signal_statuses as update_script  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_PENDING_ENTRY,
    SIGNAL_STATUS_SL_HIT,
    SIGNAL_STATUS_TP1_HIT,
    SIGNAL_STATUS_TP2_HIT,
    SIGNAL_STATUS_TP3_HIT,
    acquire_signal_operation_lock,
    create_trading_signal,
    is_signal_operation_locked,
    publish_trading_signal,
    release_signal_operation_lock,
    set_signal_setting,
)
from app.signals.signal_risk import calculate_dynamic_validity_minutes  # noqa: E402


class StaticMarketData:
    def __init__(self, candles):
        self.candles = candles

    def fetch_candles(self, symbol: str, timeframe: str, limit: int):
        return self.candles[-limit:]


def _db(tmp_path: Path) -> DB:
    db_path = tmp_path / "signal_lifecycle.db"
    migrate(str(db_path))
    return DB(path=str(db_path))


def _iso(delta: timedelta) -> str:
    return (datetime.now(timezone.utc) + delta).isoformat()


def _candle(low: float, high: float, close: float | None = None):
    now = datetime.now(timezone.utc)
    close = close if close is not None else (low + high) / 2
    return [[int(now.timestamp() * 1000), str(close), str(high), str(low), str(close), "1000"]]


def _seed_signal(
    db: DB,
    *,
    signal_id: str,
    side: str = "BUY",
    status: str = SIGNAL_STATUS_PENDING_ENTRY,
    expires_delta: timedelta = timedelta(hours=2),
) -> str:
    if side == "BUY":
        entry, stop, tp1, tp2, tp3 = 100.0, 95.0, 107.5, 110.0, 115.0
    else:
        entry, stop, tp1, tp2, tp3 = 100.0, 105.0, 92.5, 90.0, 85.0
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": side,
            "timeframe": "1h",
            "strategy_name": "Lifecycle Test",
            "entry_price": entry,
            "entry_zone_low": entry - 0.5,
            "entry_zone_high": entry + 0.5,
            "stop_loss": stop,
            "take_profit_1": tp1,
            "take_profit_2": tp2,
            "take_profit_3": tp3,
            "risk_reward": 2.0,
            "confidence_score": 80.0,
            "signal_reason": "Lifecycle test signal",
            "status": status,
            "expires_at": _iso(expires_delta),
        },
        db=db,
    )
    publish_trading_signal(signal_id, published_at=_iso(timedelta(hours=-1)), db=db)
    return signal_id


def test_scheduled_status_updater_respects_pause_setting(tmp_path):
    db_path = str(tmp_path / "paused_status_update.db")
    migrate(db_path)
    db = DB(path=db_path)
    set_signal_setting("status_updater_paused", "true", db=db)

    summary = update_script.run_update(db_path=db_path, scheduled=True)

    assert summary["paused"] is True
    assert summary["checked"] == 0


def test_scheduled_status_updater_lock_prevents_overlap_and_releases(tmp_path):
    db_path = str(tmp_path / "locked_status_update.db")
    migrate(db_path)
    db = DB(path=db_path)
    assert acquire_signal_operation_lock("STATUS_UPDATE", 60, locked_by="existing", db=db)

    locked_summary = update_script.run_update(db_path=db_path, scheduled=True, ignore_pause=True)

    assert locked_summary["lock_not_acquired"] is True
    release_signal_operation_lock("STATUS_UPDATE", db=db)

    open_summary = update_script.run_update(db_path=db_path, scheduled=True, ignore_pause=True)
    assert open_summary["errors"] == []
    assert is_signal_operation_locked("STATUS_UPDATE", db=db) is False


def _signal(db: DB, signal_id: str):
    with db.connect() as conn:
        return dict(conn.execute("SELECT * FROM trading_signals WHERE id = ?", (signal_id,)).fetchone())


def _performance(db: DB, signal_id: str):
    with db.connect() as conn:
        return dict(conn.execute("SELECT * FROM signal_performance WHERE signal_id = ?", (signal_id,)).fetchone())


def test_expired_pending_signal_becomes_expired_and_updates_performance(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-expire-pending", expires_delta=timedelta(minutes=-1))

    summary = SignalExpiryUpdater(db=db).expire_due_signals()

    assert summary["expired"] == 1
    assert _signal(db, "sig-expire-pending")["status"] == SIGNAL_STATUS_EXPIRED
    perf = _performance(db, "sig-expire-pending")
    assert perf["expired"] == 1
    assert perf["result"] == "EXPIRED"


def test_expired_active_signal_becomes_expired_when_no_outcome(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-expire-active", status=SIGNAL_STATUS_ACTIVE, expires_delta=timedelta(minutes=-1))

    summary = SignalExpiryUpdater(db=db).expire_due_signals()

    assert summary["expired"] == 1
    assert _signal(db, "sig-expire-active")["status"] == SIGNAL_STATUS_EXPIRED


def test_pending_entry_becomes_active_when_entry_touched_and_perf_record_created(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-entry")

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(99, 101))).update_signal_performance()

    assert summary["entry_triggered"] == 1
    assert _signal(db, "sig-entry")["status"] == SIGNAL_STATUS_ACTIVE
    assert _performance(db, "sig-entry")["entry_triggered"] == 1


def test_dynamic_validity_windows_are_bounded_and_adjusted():
    low_vol = calculate_dynamic_validity_minutes(
        timeframe="1h",
        atr=0.2,
        entry_price=100.0,
        stop_loss=99.0,
    )
    normal_vol = calculate_dynamic_validity_minutes(
        timeframe="1h",
        atr=1.0,
        entry_price=100.0,
        stop_loss=99.0,
    )
    high_vol = calculate_dynamic_validity_minutes(
        timeframe="1h",
        atr=2.0,
        entry_price=100.0,
        stop_loss=99.0,
    )
    extreme_vol = calculate_dynamic_validity_minutes(
        timeframe="1h",
        atr=4.0,
        entry_price=100.0,
        stop_loss=99.0,
    )
    tight_risk = calculate_dynamic_validity_minutes(
        timeframe="1h",
        atr=1.0,
        entry_price=100.0,
        stop_loss=99.8,
    )
    fifteen = calculate_dynamic_validity_minutes(
        timeframe="15m",
        atr=0.2,
        entry_price=100.0,
        stop_loss=99.0,
    )
    thirty = calculate_dynamic_validity_minutes(
        timeframe="30m",
        atr=1.0,
        entry_price=100.0,
        stop_loss=99.0,
    )
    one_hour = calculate_dynamic_validity_minutes(
        timeframe="1h",
        atr=1.0,
        entry_price=100.0,
        stop_loss=99.0,
    )
    four_hour = calculate_dynamic_validity_minutes(
        timeframe="4h",
        atr=1.0,
        entry_price=100.0,
        stop_loss=99.0,
    )
    unknown = calculate_dynamic_validity_minutes(
        timeframe="2h",
        atr=1.0,
        entry_price=100.0,
        stop_loss=99.0,
    )

    assert low_vol["validity_minutes"] == 120
    assert normal_vol["validity_minutes"] == 120
    assert high_vol["validity_minutes"] == 90
    assert extreme_vol["validity_minutes"] == 60
    assert tight_risk["validity_minutes"] == 90
    assert fifteen["validity_minutes"] == 45
    assert thirty["validity_minutes"] == 90
    assert one_hour["validity_minutes"] == 120
    assert four_hour["validity_minutes"] == 360
    assert unknown["validity_minutes"] == 120


def test_buy_tp_progression_and_sl_detection(tmp_path):
    db = _db(tmp_path / "tp")
    _seed_signal(db, signal_id="sig-buy-tp", status=SIGNAL_STATUS_ACTIVE)

    tp_summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(99, 116))).update_signal_performance()
    sl_db = _db(tmp_path / "sl")
    _seed_signal(sl_db, signal_id="sig-buy-sl", status=SIGNAL_STATUS_ACTIVE)
    sl_summary = SignalPerformanceUpdater(db=sl_db, market_data=StaticMarketData(_candle(94, 101))).update_signal_performance()

    assert tp_summary["tp1_hit"] == 1
    assert tp_summary["tp2_hit"] == 1
    assert tp_summary["tp3_hit"] == 1
    assert _signal(db, "sig-buy-tp")["status"] == SIGNAL_STATUS_TP3_HIT
    assert sl_summary["sl_hit"] >= 1
    assert _signal(sl_db, "sig-buy-sl")["status"] == SIGNAL_STATUS_SL_HIT


def test_sell_tp_progression_and_sl_detection(tmp_path):
    db = _db(tmp_path / "tp")
    _seed_signal(db, signal_id="sig-sell-tp", side="SELL", status=SIGNAL_STATUS_ACTIVE)

    tp_summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(84, 101))).update_signal_performance()
    sl_db = _db(tmp_path / "sl")
    _seed_signal(sl_db, signal_id="sig-sell-sl", side="SELL", status=SIGNAL_STATUS_ACTIVE)
    sl_summary = SignalPerformanceUpdater(db=sl_db, market_data=StaticMarketData(_candle(99, 106))).update_signal_performance()

    assert tp_summary["tp1_hit"] == 1
    assert tp_summary["tp2_hit"] == 1
    assert tp_summary["tp3_hit"] == 1
    assert _signal(db, "sig-sell-tp")["status"] == SIGNAL_STATUS_TP3_HIT
    assert sl_summary["sl_hit"] >= 1
    assert _signal(sl_db, "sig-sell-sl")["status"] == SIGNAL_STATUS_SL_HIT


def test_same_candle_tp_and_sl_is_ambiguous_not_clean_win(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-ambiguous", status=SIGNAL_STATUS_ACTIVE)

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(94, 116))).update_signal_performance()

    assert summary["ambiguous"] == 1
    perf = _performance(db, "sig-ambiguous")
    assert perf["result"] == "AMBIGUOUS"
    assert perf["result"] != "WIN"


def test_tp1_only_by_expiry_is_not_win(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-tp1-only", status=SIGNAL_STATUS_ACTIVE)

    SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(99, 108))).update_signal_performance()
    expired_summary = SignalPerformanceUpdater(
        db=db,
        market_data=StaticMarketData(_candle(99, 108)),
    ).update_signal_performance(now=datetime.now(timezone.utc) + timedelta(hours=5))

    assert expired_summary["expired"] == 1
    assert _signal(db, "sig-tp1-only")["status"] == SIGNAL_STATUS_TP1_HIT
    perf = _performance(db, "sig-tp1-only")
    assert perf["tp1_hit"] == 1
    assert perf["tp2_hit"] == 0
    assert perf["result"] == "EXPIRED"
    assert perf["result"] != "WIN"


def test_tp2_by_expiry_is_win_without_tp3(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-tp2-expiry", status=SIGNAL_STATUS_ACTIVE)

    SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(99, 111))).update_signal_performance()
    expired_summary = SignalPerformanceUpdater(
        db=db,
        market_data=StaticMarketData(_candle(99, 111)),
    ).update_signal_performance(now=datetime.now(timezone.utc) + timedelta(hours=5))

    assert expired_summary["expired"] == 1
    assert _signal(db, "sig-tp2-expiry")["status"] == SIGNAL_STATUS_TP2_HIT
    perf = _performance(db, "sig-tp2-expiry")
    assert perf["tp2_hit"] == 1
    assert perf["tp3_hit"] == 0
    assert perf["result"] == "WIN"


def test_tp3_hit_is_win_and_closed_immediately(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-tp3-win", status=SIGNAL_STATUS_ACTIVE)

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(99, 116))).update_signal_performance()

    assert summary["tp3_hit"] == 1
    assert _signal(db, "sig-tp3-win")["status"] == SIGNAL_STATUS_TP3_HIT
    perf = _performance(db, "sig-tp3-win")
    assert perf["result"] == "WIN"
    assert perf["closed_at"] is not None


def test_tp1_then_sl_before_tp2_is_not_win(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-tp1-then-sl", status=SIGNAL_STATUS_ACTIVE)

    SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(99, 108))).update_signal_performance()
    sl_summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(94, 101))).update_signal_performance()

    assert sl_summary["sl_hit"] == 1
    assert _signal(db, "sig-tp1-then-sl")["status"] == SIGNAL_STATUS_SL_HIT
    perf = _performance(db, "sig-tp1-then-sl")
    assert perf["tp1_hit"] == 1
    assert perf["tp2_hit"] == 0
    assert perf["result"] == "LOSS"
    assert perf["result"] != "WIN"


def test_updater_is_idempotent_and_terminal_signals_are_not_modified(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-idempotent")
    _seed_signal(db, signal_id="sig-terminal", status=SIGNAL_STATUS_CANCELLED)

    updater = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(99, 108)))
    updater.update_signal_performance()
    first_perf_count = _count_performance_rows(db, "sig-idempotent")
    first_tp1_at = _signal(db, "sig-idempotent")["tp1_hit_at"]
    updater.update_signal_performance()

    assert _count_performance_rows(db, "sig-idempotent") == first_perf_count
    assert _signal(db, "sig-idempotent")["tp1_hit_at"] == first_tp1_at
    assert _signal(db, "sig-terminal")["status"] == SIGNAL_STATUS_CANCELLED


def test_pending_buy_sl_before_entry_closes_signal(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-pre-buy-sl")

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(94, 97))).update_signal_performance()

    assert summary["sl_hit"] == 1
    assert _signal(db, "sig-pre-buy-sl")["status"] == SIGNAL_STATUS_SL_HIT
    assert _performance(db, "sig-pre-buy-sl")["result"] == "LOSS"


def test_pending_sell_sl_before_entry_closes_signal(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-pre-sell-sl", side="SELL")

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(106, 108))).update_signal_performance()

    assert summary["sl_hit"] == 1
    assert _signal(db, "sig-pre-sell-sl")["status"] == SIGNAL_STATUS_SL_HIT
    assert _performance(db, "sig-pre-sell-sl")["result"] == "LOSS"


def test_pending_buy_tp_before_entry_invalidates_signal(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-pre-buy-tp")

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(103, 108))).update_signal_performance()

    assert summary["invalidated"] == 1
    assert _signal(db, "sig-pre-buy-tp")["status"] == "INVALIDATED"
    assert _performance(db, "sig-pre-buy-tp")["result"] == "INVALIDATED"


def test_pending_sell_tp_before_entry_invalidates_signal(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-pre-sell-tp", side="SELL")

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(92, 97))).update_signal_performance()

    assert summary["invalidated"] == 1
    assert _signal(db, "sig-pre-sell-tp")["status"] == "INVALIDATED"
    assert _performance(db, "sig-pre-sell-tp")["result"] == "INVALIDATED"


def test_pending_entry_drift_too_far_invalidates_signal(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-drift")

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(101, 103, close=103))).update_signal_performance()

    assert summary["invalidated"] == 1
    assert _signal(db, "sig-drift")["status"] == "INVALIDATED"


def test_pending_volatility_spike_invalidates_signal(tmp_path):
    db = _db(tmp_path)
    _seed_signal(db, signal_id="sig-vol-spike")

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarketData(_candle(101, 107, close=104))).update_signal_performance()

    assert summary["invalidated"] == 1
    assert _signal(db, "sig-vol-spike")["status"] == "INVALIDATED"


def test_script_runs_and_prints_summary_on_empty_database(tmp_path):
    db_path = tmp_path / "script_empty.db"
    script = ROOT / "backends" / "bot-backend" / "scripts" / "update_signal_statuses.py"

    result = subprocess.run(
        [sys.executable, str(script), "--db-path", str(db_path)],
        cwd=str(ROOT / "backends" / "bot-backend"),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["checked"] == 0
    assert payload["errors"] == []


def test_lifecycle_source_does_not_reference_execution_or_tradingview_queue():
    import app.signals.signal_expiry as expiry_module
    import app.signals.signal_performance as performance_module

    source = inspect.getsource(expiry_module) + inspect.getsource(performance_module)

    assert "BinanceExecutor" not in source
    assert "execute_signal" not in source
    assert "create_external_signal_queue_row" not in source
    assert "auto_pilot" not in source.lower()
    assert "place_order" not in source.lower()


def _count_performance_rows(db: DB, signal_id: str) -> int:
    with db.connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM signal_performance WHERE signal_id = ?", (signal_id,)).fetchone()
    return int(row["count"])
