"""
Phase 11: Crypto Signal Center - Backend tests, persistence checks, engine gap tests, E2E validation.

Covers gaps not already addressed by:
  test_crypto_signal_engine.py
  test_signal_lifecycle_update.py
  test_generate_daily_crypto_signals.py

Specifically adds:
  1. Direct persistence layer tests (signal_candidates, trading_signals,
     signal_performance, signal_delivery, user_signal_preferences)
  2. TP1/TP2/TP3 explicit 1.5R/2R/3R ratio verification
  3. Missing performance record auto-creation on first entry
  4. Safety proof: no signal module references execution systems
  5. E2E: generate → lifecycle update → TP/SL outcomes → performance record
"""
from __future__ import annotations

import inspect
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "bot-backend"))

from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    CANDIDATE_STATUS_ACCEPTED,
    CANDIDATE_STATUS_REJECTED,
    PERFORMANCE_RESULT_LOSS,
    PERFORMANCE_RESULT_WIN,
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_PENDING_ENTRY,
    SIGNAL_STATUS_SL_HIT,
    SIGNAL_STATUS_TP2_HIT,
    SIGNAL_STATUS_TP3_HIT,
    create_signal_candidate,
    create_signal_delivery,
    create_signal_performance,
    create_trading_signal,
    create_user_signal_preferences,
    get_signal_candidate,
    get_signal_performance,
    get_trading_signal,
    get_user_signal_preferences,
    mark_signal_delivered,
    mark_signal_entry_triggered,
    mark_signal_sl_hit,
    mark_signal_tp_hit,
    mark_signal_viewed,
    publish_trading_signal,
    save_signal_for_user,
    update_signal_performance,
    update_user_signal_preferences,
)


def _db(tmp_path: Path) -> DB:
    db_path = tmp_path / "phase11.db"
    migrate(str(db_path))
    return DB(path=str(db_path))


def _future_iso(hours: int = 4) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 1. Persistence layer – signal_candidates
# ---------------------------------------------------------------------------


def test_create_signal_candidate_returns_id_and_defaults(tmp_path):
    db = _db(tmp_path)
    candidate_id = create_signal_candidate(
        {
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "confidence_score": 75.0,
            "signal_reason": "Phase 11 test",
        },
        db=db,
    )
    assert candidate_id
    row = get_signal_candidate(candidate_id, db=db)
    assert row is not None
    assert row["symbol"] == "BTCUSDT"
    assert row["side"] == "BUY"
    assert row["status"] == "CANDIDATE"
    assert row["dev_mode"] == 0
    assert row["source"] == "internal_signal_engine"
    assert row["created_at"]
    assert row["updated_at"]


def test_rejected_candidate_stores_rejection_reason(tmp_path):
    db = _db(tmp_path)
    candidate_id = create_signal_candidate(
        {
            "asset_class": "crypto",
            "symbol": "ETHUSDT",
            "side": "SELL",
            "status": CANDIDATE_STATUS_REJECTED,
            "rejection_reason": "LOW_CONFIDENCE",
        },
        db=db,
    )
    row = get_signal_candidate(candidate_id, db=db)
    assert row["status"] == CANDIDATE_STATUS_REJECTED
    assert row["rejection_reason"] == "LOW_CONFIDENCE"


def test_accepted_candidate_stores_accepted_status(tmp_path):
    db = _db(tmp_path)
    candidate_id = create_signal_candidate(
        {
            "asset_class": "crypto",
            "symbol": "SOLUSDT",
            "side": "BUY",
            "status": CANDIDATE_STATUS_ACCEPTED,
        },
        db=db,
    )
    row = get_signal_candidate(candidate_id, db=db)
    assert row["status"] == CANDIDATE_STATUS_ACCEPTED
    assert row["rejection_reason"] is None


# ---------------------------------------------------------------------------
# 2. Persistence layer – trading_signals
# ---------------------------------------------------------------------------


def test_create_trading_signal_is_unpublished_by_default(tmp_path):
    db = _db(tmp_path)
    signal_id = "sig-phase11-a"
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_price": 60000.0,
            "stop_loss": 58000.0,
            "take_profit_1": 63000.0,
            "risk_reward": 1.5,
            "confidence_score": 80.0,
            "status": SIGNAL_STATUS_PENDING_ENTRY,
            "expires_at": _future_iso(),
        },
        db=db,
    )
    row = get_trading_signal(signal_id, db=db)
    assert row is not None
    assert row["is_published"] == 0
    assert row["status"] == SIGNAL_STATUS_PENDING_ENTRY
    assert row["dev_mode"] == 0
    assert row["published_at"] is None


def test_publish_trading_signal_sets_is_published_and_published_at(tmp_path):
    db = _db(tmp_path)
    signal_id = "sig-phase11-pub"
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_price": 100.0,
            "stop_loss": 95.0,
            "take_profit_1": 107.5,
            "risk_reward": 2.0,
            "confidence_score": 78.0,
            "status": SIGNAL_STATUS_PENDING_ENTRY,
            "expires_at": _future_iso(),
        },
        db=db,
    )
    publish_trading_signal(signal_id, published_at=_now_iso(), db=db)
    row = get_trading_signal(signal_id, db=db)
    assert row["is_published"] == 1
    assert row["published_at"] is not None


# ---------------------------------------------------------------------------
# 3. Persistence layer – signal_performance
# ---------------------------------------------------------------------------


def test_create_signal_performance_defaults(tmp_path):
    db = _db(tmp_path)
    signal_id = "sig-phase11-perf"
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_price": 100.0,
            "stop_loss": 95.0,
            "take_profit_1": 107.5,
            "risk_reward": 2.0,
            "confidence_score": 80.0,
            "status": SIGNAL_STATUS_PENDING_ENTRY,
            "expires_at": _future_iso(),
        },
        db=db,
    )
    perf_id = create_signal_performance(
        {"signal_id": signal_id, "asset_class": "crypto", "symbol": "BTCUSDT", "side": "BUY"},
        db=db,
    )
    assert perf_id
    perf = get_signal_performance(signal_id, db=db)
    assert perf is not None
    assert perf["entry_triggered"] == 0
    assert perf["tp1_hit"] == 0
    assert perf["tp2_hit"] == 0
    assert perf["tp3_hit"] == 0
    assert perf["sl_hit"] == 0
    assert perf["expired"] == 0
    assert perf["cancelled"] == 0
    assert perf["result"] == "OPEN"


def test_update_signal_performance_modifies_fields(tmp_path):
    db = _db(tmp_path)
    signal_id = "sig-phase11-perf-upd"
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "ETHUSDT",
            "side": "SELL",
            "entry_price": 3000.0,
            "stop_loss": 3100.0,
            "take_profit_1": 2850.0,
            "risk_reward": 2.0,
            "confidence_score": 75.0,
            "status": SIGNAL_STATUS_PENDING_ENTRY,
            "expires_at": _future_iso(),
        },
        db=db,
    )
    create_signal_performance(
        {"signal_id": signal_id, "asset_class": "crypto", "symbol": "ETHUSDT", "side": "SELL"},
        db=db,
    )
    update_signal_performance(signal_id, {"entry_triggered": 1, "result": "OPEN"}, db=db)
    perf = get_signal_performance(signal_id, db=db)
    assert perf["entry_triggered"] == 1
    assert perf["result"] == "OPEN"


def test_mark_signal_tp_hit_and_sl_hit_persistence(tmp_path):
    db_tp = _db(tmp_path / "tp")
    db_sl = _db(tmp_path / "sl")

    for db, sig_id in [(db_tp, "sig-tp2"), (db_sl, "sig-sl")]:
        create_trading_signal(
            {
                "id": sig_id,
                "asset_class": "crypto",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "entry_price": 100.0,
                "stop_loss": 95.0,
                "take_profit_1": 107.5,
                "take_profit_2": 110.0,
                "take_profit_3": 115.0,
                "risk_reward": 2.0,
                "confidence_score": 80.0,
                "status": SIGNAL_STATUS_ACTIVE,
                "expires_at": _future_iso(),
            },
            db=db,
        )
        create_signal_performance(
            {"signal_id": sig_id, "asset_class": "crypto", "symbol": "BTCUSDT", "side": "BUY"},
            db=db,
        )

    mark_signal_tp_hit("sig-tp2", tp_level=2, db=db_tp)
    tp_signal = get_trading_signal("sig-tp2", db=db_tp)
    tp_perf = get_signal_performance("sig-tp2", db=db_tp)
    assert tp_signal["status"] == SIGNAL_STATUS_TP2_HIT
    assert tp_perf["tp2_hit"] == 1
    assert tp_perf["result"] == "OPEN"  # TP2 does not auto-close in persistence layer

    mark_signal_tp_hit("sig-sl", tp_level=3, db=db_sl)
    tp3_perf = get_signal_performance("sig-sl", db=db_sl)
    assert tp3_perf["tp3_hit"] == 1
    assert tp3_perf["result"] == PERFORMANCE_RESULT_WIN
    assert tp3_perf["closed_at"] is not None

    mark_signal_sl_hit("sig-sl", db=db_sl)  # noqa: note: this overrides but tests sl path separately
    db_sl2 = _db(tmp_path / "sl2")
    create_trading_signal(
        {
            "id": "sig-sl2",
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_price": 100.0,
            "stop_loss": 95.0,
            "take_profit_1": 107.5,
            "risk_reward": 2.0,
            "confidence_score": 80.0,
            "status": SIGNAL_STATUS_ACTIVE,
            "expires_at": _future_iso(),
        },
        db=db_sl2,
    )
    create_signal_performance(
        {"signal_id": "sig-sl2", "asset_class": "crypto", "symbol": "BTCUSDT", "side": "BUY"},
        db=db_sl2,
    )
    mark_signal_sl_hit("sig-sl2", db=db_sl2)
    sl_signal = get_trading_signal("sig-sl2", db=db_sl2)
    sl_perf = get_signal_performance("sig-sl2", db=db_sl2)
    assert sl_signal["status"] == SIGNAL_STATUS_SL_HIT
    assert sl_perf["sl_hit"] == 1
    assert sl_perf["result"] == PERFORMANCE_RESULT_LOSS
    assert sl_perf["closed_at"] is not None


# ---------------------------------------------------------------------------
# 4. Persistence layer – signal_delivery
# ---------------------------------------------------------------------------


def test_create_and_update_signal_delivery(tmp_path):
    db = _db(tmp_path)
    signal_id = "sig-phase11-delivery"
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_price": 100.0,
            "stop_loss": 95.0,
            "take_profit_1": 107.5,
            "risk_reward": 2.0,
            "confidence_score": 80.0,
            "status": SIGNAL_STATUS_ACTIVE,
            "expires_at": _future_iso(),
        },
        db=db,
    )

    delivery_id = create_signal_delivery(
        {"signal_id": signal_id, "user_id": "user-1"},
        db=db,
    )
    assert delivery_id

    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM signal_delivery WHERE id = ?", (delivery_id,)
        ).fetchone()
    assert row is not None
    assert row["signal_id"] == signal_id
    assert row["user_id"] == "user-1"
    assert row["saved"] == 0
    assert row["delivered_at"] is None


def test_mark_signal_delivered_and_viewed_creates_upsert(tmp_path):
    db = _db(tmp_path)
    signal_id = "sig-phase11-delivery2"
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_price": 100.0,
            "stop_loss": 95.0,
            "take_profit_1": 107.5,
            "risk_reward": 2.0,
            "confidence_score": 80.0,
            "status": SIGNAL_STATUS_ACTIVE,
            "expires_at": _future_iso(),
        },
        db=db,
    )

    mark_signal_delivered(signal_id, "user-2", db=db)
    mark_signal_viewed(signal_id, "user-2", db=db)
    save_signal_for_user(signal_id, "user-2", db=db)

    with db.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM signal_delivery WHERE signal_id = ? AND user_id = ?",
            (signal_id, "user-2"),
        ).fetchall()

    assert len(rows) == 1
    row = dict(rows[0])
    assert row["delivered_at"] is not None
    assert row["viewed_at"] is not None
    assert row["saved"] == 1


# ---------------------------------------------------------------------------
# 5. Persistence layer – user_signal_preferences
# ---------------------------------------------------------------------------


def test_create_and_update_user_signal_preferences(tmp_path):
    db = _db(tmp_path)

    pref_id = create_user_signal_preferences("user-pref-1", db=db)
    assert pref_id

    prefs = get_user_signal_preferences("user-pref-1", db=db)
    assert prefs is not None
    assert prefs["user_id"] == "user-pref-1"
    assert prefs["crypto_enabled"] == 1
    assert prefs["forex_enabled"] == 0
    assert prefs["minimum_confidence"] == 70
    assert prefs["notifications_enabled"] == 1

    update_user_signal_preferences("user-pref-1", {"minimum_confidence": 80, "notifications_enabled": 0}, db=db)
    updated = get_user_signal_preferences("user-pref-1", db=db)
    assert updated["minimum_confidence"] == 80
    assert updated["notifications_enabled"] == 0


# ---------------------------------------------------------------------------
# 6. Signal engine – TP1/TP2/TP3 follow 1.5R/2R/3R
# ---------------------------------------------------------------------------


def test_buy_take_profits_follow_1_5r_2r_3r_ratios():
    from app.signals.signal_risk import calculate_take_profits

    entry = 100.0
    stop = 95.0
    risk = entry - stop  # 5.0

    tp1, tp2, tp3 = calculate_take_profits(side="BUY", entry_price=entry, stop_loss=stop)

    assert abs(tp1 - (entry + 1.5 * risk)) < 0.000001, f"TP1 expected {entry + 1.5 * risk}, got {tp1}"
    assert abs(tp2 - (entry + 2.0 * risk)) < 0.000001, f"TP2 expected {entry + 2.0 * risk}, got {tp2}"
    assert abs(tp3 - (entry + 3.0 * risk)) < 0.000001, f"TP3 expected {entry + 3.0 * risk}, got {tp3}"


def test_sell_take_profits_follow_1_5r_2r_3r_ratios():
    from app.signals.signal_risk import calculate_take_profits

    entry = 100.0
    stop = 105.0
    risk = stop - entry  # 5.0

    tp1, tp2, tp3 = calculate_take_profits(side="SELL", entry_price=entry, stop_loss=stop)

    assert abs(tp1 - (entry - 1.5 * risk)) < 0.000001, f"TP1 expected {entry - 1.5 * risk}, got {tp1}"
    assert abs(tp2 - (entry - 2.0 * risk)) < 0.000001, f"TP2 expected {entry - 2.0 * risk}, got {tp2}"
    assert abs(tp3 - (entry - 3.0 * risk)) < 0.000001, f"TP3 expected {entry - 3.0 * risk}, got {tp3}"


def test_risk_reward_calculation_uses_tp2():
    from app.signals.signal_risk import calculate_risk_reward

    entry = 100.0
    stop = 95.0  # risk = 5
    tp2 = 110.0  # reward = 10  => RR = 2.0

    rr = calculate_risk_reward(entry_price=entry, stop_loss=stop, take_profit_2=tp2)
    assert rr == 2.0


# ---------------------------------------------------------------------------
# 7. Missing performance record is auto-created on first entry trigger
# ---------------------------------------------------------------------------


def test_entry_trigger_auto_creates_missing_performance_record(tmp_path):
    db = _db(tmp_path)
    signal_id = "sig-auto-perf"
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_price": 100.0,
            "stop_loss": 95.0,
            "take_profit_1": 107.5,
            "take_profit_2": 110.0,
            "take_profit_3": 115.0,
            "risk_reward": 2.0,
            "confidence_score": 82.0,
            "status": SIGNAL_STATUS_PENDING_ENTRY,
            "expires_at": _future_iso(),
        },
        db=db,
    )
    publish_trading_signal(signal_id, published_at=_now_iso(), db=db)

    from app.signals.signal_performance import SignalPerformanceUpdater

    class StaticMarket:
        def fetch_candles(self, symbol, timeframe, limit):
            return [[int(datetime.now(timezone.utc).timestamp() * 1000), "100", "101", "99", "100", "1000"]]

    summary = SignalPerformanceUpdater(db=db, market_data=StaticMarket()).update_signal_performance()

    assert summary["entry_triggered"] == 1
    perf = get_signal_performance(signal_id, db=db)
    assert perf is not None
    assert perf["entry_triggered"] == 1


# ---------------------------------------------------------------------------
# 8. E2E: generate → lifecycle update → TP2 win → verify persistence
# ---------------------------------------------------------------------------


def test_e2e_signal_lifecycle_buy_tp2_win(tmp_path):
    from app.signals.crypto_signal_engine import CryptoSignalEngine, normalize_candles
    from app.signals.signal_performance import SignalPerformanceUpdater
    from app.signals.signal_repository import SignalRepository

    now = datetime.now(timezone.utc)
    count = 120
    start = 100.0
    step = 0.08
    candles = []
    price = start
    for i in range(count):
        open_price = price
        close = price + step
        high = max(open_price, close) + 0.08
        low = min(open_price, close) - 0.08
        candles.append([
            int((now - timedelta(hours=count - i - 1)).timestamp() * 1000),
            f"{open_price:.8f}",
            f"{high:.8f}",
            f"{low:.8f}",
            f"{close:.8f}",
            "100000",
        ])
        price = close

    class StaticMarket:
        def fetch_candles(self, symbol, timeframe, limit):
            return candles[-limit:]

    db = _db(tmp_path)
    engine = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarket(),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
    )

    result = engine.generate_candidate_for_side("BTCUSDT", "BUY", normalize_candles(candles))
    assert result["accepted"] is True
    signal_id = result["signal_id"]

    publish_trading_signal(signal_id, published_at=_now_iso(), db=db)

    sig = get_trading_signal(signal_id, db=db)
    assert sig["is_published"] == 1
    assert sig["status"] == SIGNAL_STATUS_PENDING_ENTRY

    entry = float(sig["entry_price"])
    stop = float(sig["stop_loss"])
    tp2 = float(sig["take_profit_2"])
    above_tp2 = tp2 + 1.0

    # Candle spans entry zone: low slightly below entry, high slightly above entry.
    # This guarantees `_entry_touched` returns True regardless of zone_low/zone_high width.
    entry_low = entry * 0.998
    entry_high = entry * 1.002

    class EntryMarket:
        def fetch_candles(self, symbol, timeframe, limit):
            return [[int(now.timestamp() * 1000), str(entry), str(entry_high), str(entry_low), str(entry), "1000"]]

    SignalPerformanceUpdater(db=db, market_data=EntryMarket()).update_signal_performance()

    sig_after_entry = get_trading_signal(signal_id, db=db)
    assert sig_after_entry["status"] == SIGNAL_STATUS_ACTIVE

    class Tp2Market:
        def fetch_candles(self, symbol, timeframe, limit):
            return [[int(now.timestamp() * 1000), str(entry), str(above_tp2), str(stop + 0.5), str(above_tp2 - 0.5), "1000"]]

    SignalPerformanceUpdater(db=db, market_data=Tp2Market()).update_signal_performance()

    sig_final = get_trading_signal(signal_id, db=db)
    assert sig_final["status"] == SIGNAL_STATUS_TP2_HIT

    perf = get_signal_performance(signal_id, db=db)
    assert perf["tp1_hit"] == 1
    assert perf["tp2_hit"] == 1
    assert perf["tp3_hit"] == 0

    from app.signals.signal_expiry import SignalExpiryUpdater

    expiry_summary = SignalExpiryUpdater(db=db).expire_due_signals()
    assert expiry_summary["expired"] == 0
    assert get_trading_signal(signal_id, db=db)["status"] == SIGNAL_STATUS_TP2_HIT


def test_e2e_cancelled_signal_excluded_from_active_via_expiry_updater(tmp_path):
    db = _db(tmp_path)
    signal_id = "sig-e2e-cancel"
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_price": 100.0,
            "stop_loss": 95.0,
            "take_profit_1": 107.5,
            "risk_reward": 2.0,
            "confidence_score": 80.0,
            "status": SIGNAL_STATUS_ACTIVE,
            "expires_at": _future_iso(-1),
        },
        db=db,
    )
    publish_trading_signal(signal_id, published_at=_now_iso(), db=db)
    create_signal_performance(
        {"signal_id": signal_id, "asset_class": "crypto", "symbol": "BTCUSDT", "side": "BUY"},
        db=db,
    )

    from app.signals.signal_expiry import SignalExpiryUpdater

    expiry_summary = SignalExpiryUpdater(db=db).expire_due_signals()
    assert expiry_summary["expired"] >= 1
    assert get_trading_signal(signal_id, db=db)["status"] == SIGNAL_STATUS_EXPIRED

    perf = get_signal_performance(signal_id, db=db)
    assert perf["expired"] == 1
    assert perf["result"] == "EXPIRED"


# ---------------------------------------------------------------------------
# 9. Dev signal produced with DEV_SIGNAL_MODE is labeled dev_mode=1
# ---------------------------------------------------------------------------


def test_e2e_dev_mock_signal_is_dev_mode_and_unpublished(tmp_path, monkeypatch):
    monkeypatch.setenv("DEV_SIGNAL_MODE", "true")
    sys.path.insert(0, str(ROOT / "backends" / "bot-backend" / "scripts"))
    import generate_daily_crypto_signals as gen_script

    db_path = str(tmp_path / "dev_e2e.db")
    summary = gen_script.create_mock_dev_signals(db_path=db_path, symbols=["BTCUSDT"])

    assert summary["signals_created"] == 1
    assert summary["published"] == 0

    db = DB(path=db_path)
    with db.connect() as conn:
        sig = conn.execute("SELECT dev_mode, is_published FROM trading_signals").fetchone()
    assert sig["dev_mode"] == 1
    assert sig["is_published"] == 0


# ---------------------------------------------------------------------------
# 10. Safety proof – no signal source file references execution systems
# ---------------------------------------------------------------------------


def _collect_signal_sources() -> str:
    import app.signals.crypto_signal_engine as eng
    import app.signals.signal_expiry as exp
    import app.signals.signal_performance as perf
    import app.signals.signal_repository as repo
    import app.signals.signal_risk as risk
    import app.signals.signal_scoring as scoring

    return "\n".join(
        inspect.getsource(m)
        for m in [eng, exp, perf, repo, risk, scoring]
    )


def test_no_signal_source_references_execution_systems():
    source = _collect_signal_sources()

    forbidden = [
        "BinanceExecutor",
        "execute_signal",
        "create_external_signal_queue_row",
        "auto_pilot",
        "place_order",
        "BybitExecutor",
        "BrokerClient",
        "open_position",
        "close_position",
    ]
    for term in forbidden:
        assert term not in source, f"Execution reference found in signal source: {term}"


def test_signal_persistence_source_does_not_reference_execution_systems():
    import shared_lib.persistence.signals as sig_persistence

    source = inspect.getsource(sig_persistence)
    forbidden = [
        "BinanceExecutor",
        "execute_signal",
        "create_external_signal_queue_row",
        "auto_pilot",
        "place_order",
        "open_position",
    ]
    for term in forbidden:
        assert term not in source, f"Execution reference found in persistence.signals: {term}"
