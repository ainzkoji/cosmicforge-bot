from __future__ import annotations

import inspect
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "bot-backend"))

from app.signals.crypto_signal_engine import ALLOWED_CRYPTO_SYMBOLS, CryptoSignalEngine, MarketDataRunCache, normalize_candles  # noqa: E402
from app.signals.signal_repository import SignalRepository  # noqa: E402
from app.signals.signal_risk import (  # noqa: E402
    calculate_dynamic_validity_minutes,
    estimate_expected_signal_duration_minutes,
    get_signal_expiry_minutes,
    calculate_risk_reward,
    calculate_stop_loss,
    calculate_take_profits,
    explain_risk_setup,
    validate_signal_prices,
    validate_signal_quality,
    validate_stop_distance,
)
from app.signals.signal_scoring import SCORE_WEIGHTS, calculate_market_features, score_signal  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import SIGNAL_STATUS_PENDING_ENTRY  # noqa: E402


class StaticMarketData:
    def __init__(self, candles):
        self.candles = candles

    def fetch_candles(self, symbol: str, timeframe: str, limit: int):
        return self.candles[-limit:]


class CountingMarketData:
    def __init__(self, candles):
        self.candles = candles
        self.calls = 0

    def fetch_candles(self, symbol: str, timeframe: str, limit: int):
        self.calls += 1
        return self.candles[-limit:]


def _db(tmp_path: Path) -> DB:
    db_path = tmp_path / "crypto_signal_engine.db"
    migrate(str(db_path))
    return DB(path=str(db_path))


def _candles(*, direction: str = "up", count: int = 120, start: float = 100.0):
    now = datetime.now(timezone.utc)
    rows = []
    price = start
    step = 0.08 if direction == "up" else -0.08
    for i in range(count):
        open_price = price
        close = price + step
        high = max(open_price, close) + 0.08
        low = min(open_price, close) - 0.08
        rows.append(
            [
                int((now - timedelta(hours=count - i - 1)).timestamp() * 1000),
                f"{open_price:.8f}",
                f"{high:.8f}",
                f"{low:.8f}",
                f"{close:.8f}",
                "100000",
            ]
        )
        price = close
    return rows


def test_allowed_crypto_symbol_list_is_v1_list():
    assert ALLOWED_CRYPTO_SYMBOLS == (
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "ADAUSDT",
        "DOGEUSDT",
        "LINKUSDT",
        "AVAXUSDT",
        "LTCUSDT",
    )


def test_engine_can_accept_injected_dynamic_symbol_list(tmp_path):
    db = _db(tmp_path)
    engine = CryptoSignalEngine(
        repository=SignalRepository(db=db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=("DOTUSDT",),
        timeframe="1h",
    )

    dot_result = engine.scan_symbol("DOTUSDT")
    fil_result = engine.scan_symbol("FILUSDT")

    assert {item["rejection_reason"] for item in dot_result if not item["accepted"]} != {"UNSUPPORTED_SYMBOL"}
    assert fil_result[0]["rejection_reason"] == "UNSUPPORTED_SYMBOL"


def test_market_data_run_cache_reuses_same_symbol_timeframe_limit():
    base = CountingMarketData(_candles())
    cache = MarketDataRunCache(base)

    first = cache.fetch_candles("BTCUSDT", "1h", 100)
    second = cache.fetch_candles("btcusdt", "1h", 100)
    third = cache.fetch_candles("BTCUSDT", "1h", 50)

    assert first == second
    assert third
    assert base.calls == 2


def test_buy_and_sell_risk_calculations_create_directional_prices():
    entry = 100.0
    buy_stop = calculate_stop_loss(
        side="BUY",
        entry_price=entry,
        recent_swing_low=98.0,
        recent_swing_high=104.0,
        atr=1.0,
    )
    buy_tp1, buy_tp2, buy_tp3 = calculate_take_profits(side="BUY", entry_price=entry, stop_loss=buy_stop)
    assert buy_stop < entry < buy_tp1 < buy_tp2 < buy_tp3
    assert calculate_risk_reward(entry_price=entry, stop_loss=buy_stop, take_profit_2=buy_tp2) == 2.0

    sell_stop = calculate_stop_loss(
        side="SELL",
        entry_price=entry,
        recent_swing_low=96.0,
        recent_swing_high=102.0,
        atr=1.0,
    )
    sell_tp1, sell_tp2, sell_tp3 = calculate_take_profits(side="SELL", entry_price=entry, stop_loss=sell_stop)
    assert sell_tp3 < sell_tp2 < sell_tp1 < entry < sell_stop
    assert calculate_risk_reward(entry_price=entry, stop_loss=sell_stop, take_profit_2=sell_tp2) == 2.0


def test_dynamic_validity_formula_for_generated_expiry_is_not_fixed_four_hours():
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
    validity_15m = calculate_dynamic_validity_minutes(timeframe="15m", atr=1.0, entry_price=100.0, stop_loss=99.0)
    validity_30m = calculate_dynamic_validity_minutes(timeframe="30m", atr=1.0, entry_price=100.0, stop_loss=99.0)
    validity_4h = calculate_dynamic_validity_minutes(timeframe="4h", atr=1.0, entry_price=100.0, stop_loss=99.0)
    validity_unknown = calculate_dynamic_validity_minutes(timeframe="2h", atr=1.0, entry_price=100.0, stop_loss=99.0)

    assert low_vol["validity_minutes"] == 120
    assert normal_vol["validity_minutes"] == 120
    assert high_vol["validity_minutes"] == 90
    assert extreme_vol["validity_minutes"] == 60
    assert tight_risk["validity_minutes"] == 90
    assert validity_15m["validity_minutes"] == 45
    assert validity_30m["validity_minutes"] == 90
    assert validity_4h["validity_minutes"] == 360
    assert validity_unknown["validity_minutes"] == 120
    assert get_signal_expiry_minutes("1h") == 120
    assert "latest recommended entry time" in normal_vol["reason"]


def test_expected_signal_duration_under_24h_is_valid():
    result = estimate_expected_signal_duration_minutes(
        timeframe="1h",
        atr=2.0,
        entry_price=100.0,
        take_profit_2=104.0,
    )

    assert result["valid"] is True
    assert result["estimated_minutes"] == 120.0


def test_expected_signal_duration_over_24h_is_rejected():
    result = estimate_expected_signal_duration_minutes(
        timeframe="1h",
        atr=0.1,
        entry_price=100.0,
        take_profit_2=104.0,
    )

    assert result["valid"] is False
    assert result["reason"] == "EXPECTED_DURATION_TOO_LONG"


def test_expected_signal_duration_missing_atr_rejects_as_insufficient_data():
    result = estimate_expected_signal_duration_minutes(
        timeframe="1h",
        atr=None,
        entry_price=100.0,
        take_profit_2=104.0,
    )

    assert result["valid"] is False
    assert result["reason"] == "INSUFFICIENT_MARKET_DATA"


def test_4h_expected_duration_rejects_when_tp2_exceeds_24h():
    result = estimate_expected_signal_duration_minutes(
        timeframe="4h",
        atr=0.5,
        entry_price=100.0,
        take_profit_2=104.0,
    )

    assert result["valid"] is False
    assert result["reason"] == "EXPECTED_DURATION_TOO_LONG"


def test_15m_30m_and_1h_can_pass_intraday_duration_rule():
    for timeframe in ("15m", "30m", "1h"):
        result = estimate_expected_signal_duration_minutes(
            timeframe=timeframe,
            atr=1.0,
            entry_price=100.0,
            take_profit_2=104.0,
        )
        assert result["valid"] is True
        assert result["estimated_minutes"] <= 1440


def test_confidence_scoring_is_deterministic_numeric():
    features = calculate_market_features(normalize_candles(_candles(direction="up")))
    score_a, details_a = score_signal(side="BUY", features=features, risk_reward=2.0)
    score_b, details_b = score_signal(side="BUY", features=features, risk_reward=2.0)

    assert isinstance(score_a, float)
    assert score_a == score_b
    assert details_a == details_b
    assert 0 <= score_a <= 100
    assert sum(details_a["component_weights"].values()) == 100
    assert sum(SCORE_WEIGHTS.values()) == 100
    assert details_a["confidence_score"] == score_a
    assert details_a["notes"]


def test_invalid_buy_and_sell_prices_are_rejected():
    invalid_buy = {
        "side": "BUY",
        "entry_price": 100.0,
        "stop_loss": 101.0,
        "take_profit_1": 110.0,
        "take_profit_2": 115.0,
        "take_profit_3": 120.0,
    }
    invalid_sell = {
        "side": "SELL",
        "entry_price": 100.0,
        "stop_loss": 99.0,
        "take_profit_1": 90.0,
        "take_profit_2": 85.0,
        "take_profit_3": 80.0,
    }

    assert validate_signal_prices(invalid_buy) == (False, "INVALID_SL")
    assert validate_signal_prices(invalid_sell) == (False, "INVALID_SL")


def test_stop_too_tight_and_too_wide_are_rejected():
    tight = validate_stop_distance(entry_price=100.0, stop_loss=99.95)
    wide = validate_stop_distance(entry_price=100.0, stop_loss=90.0)

    assert tight == (False, "STOP_TOO_TIGHT")
    assert wide == (False, "STOP_TOO_WIDE")


def test_validate_signal_quality_rejects_missing_required_fields_and_extreme_volatility():
    base = {
        "side": "BUY",
        "entry_price": 100.0,
        "stop_loss": 98.0,
        "take_profit_1": 103.0,
        "take_profit_2": 104.0,
        "take_profit_3": 106.0,
        "expires_at": _future_iso(),
        "confidence_score": 80.0,
        "risk_reward": 2.0,
    }

    missing_entry = dict(base, entry_price=None)
    missing_sl = dict(base, stop_loss=None)
    missing_tp1 = dict(base, take_profit_1=None)
    missing_expiry = dict(base, expires_at=None)
    volatile = dict(base, stop_loss=95.0, take_profit_1=107.5, take_profit_2=110.0, take_profit_3=115.0)

    assert validate_signal_quality(missing_entry, atr=0.5, atr_pct=0.005)["reason"] == "INVALID_ENTRY"
    assert validate_signal_quality(missing_sl, atr=0.5, atr_pct=0.005)["reason"] == "INVALID_SL"
    assert validate_signal_quality(missing_tp1, atr=0.5, atr_pct=0.005)["reason"] == "INVALID_TP"
    assert validate_signal_quality(missing_expiry, atr=0.5, atr_pct=0.005)["reason"] == "MISSING_EXPIRY"
    assert validate_signal_quality(volatile, atr=12.0, atr_pct=0.12)["reason"] == "VOLATILITY_TOO_HIGH"
    assert validate_signal_quality(
        dict(base, timeframe="1h", take_profit_2=140.0, take_profit_3=160.0),
        atr=0.5,
        atr_pct=0.005,
    )["reason"] == "EXPECTED_DURATION_TOO_LONG"
    assert validate_signal_quality(dict(base, timeframe="1h"), atr=None, atr_pct=None)["reason"] == "INSUFFICIENT_MARKET_DATA"
    assert "risk setup" in explain_risk_setup(base).lower()


def _future_iso() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat()


def test_low_confidence_candidate_is_rejected(tmp_path):
    db = _db(tmp_path)
    engine = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
    )

    result = engine.generate_candidate_for_side("BTCUSDT", "SELL", normalize_candles(_candles(direction="up")))

    assert result["accepted"] is False
    assert result["rejection_reason"] in {"NO_CLEAR_TREND", "LOW_CONFIDENCE"}
    with db.connect() as conn:
        row = conn.execute("SELECT status, rejection_reason FROM signal_candidates WHERE id = ?", (result["candidate_id"],)).fetchone()
    assert row["status"] == "REJECTED"
    assert row["rejection_reason"] == result["rejection_reason"]


def test_low_risk_reward_candidate_is_rejected(tmp_path, monkeypatch):
    db = _db(tmp_path)
    engine = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
    )
    monkeypatch.setattr("app.signals.crypto_signal_engine.calculate_risk_reward", lambda **kwargs: 1.0)

    result = engine.generate_candidate_for_side("BTCUSDT", "BUY", normalize_candles(_candles(direction="up")))

    assert result["accepted"] is False
    assert result["rejection_reason"] == "RISK_REWARD_TOO_LOW"


def test_invalid_symbol_is_rejected(tmp_path):
    db = _db(tmp_path)
    engine = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
    )

    result = engine.scan_symbol("NOTREALUSDT")

    assert result[0]["accepted"] is False
    assert result[0]["rejection_reason"] == "UNSUPPORTED_SYMBOL"


def test_unsupported_timeframe_and_invalid_side_are_rejected(tmp_path):
    db = _db(tmp_path)
    bad_timeframe = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="2h",
    )
    good_engine = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
    )

    timeframe_result = bad_timeframe.scan_symbol("BTCUSDT")
    side_result = good_engine.generate_candidate_for_side("BTCUSDT", "HOLD", normalize_candles(_candles(direction="up")))

    assert timeframe_result[0]["rejection_reason"] == "UNSUPPORTED_TIMEFRAME"
    assert side_result["rejection_reason"] == "INVALID_SIDE"


def test_stale_and_insufficient_market_data_are_rejected(tmp_path):
    db = _db(tmp_path)
    engine = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
    )
    stale = _candles(direction="up")
    for row in stale:
        row[0] = int((datetime.now(timezone.utc) - timedelta(days=20)).timestamp() * 1000)

    stale_result = engine.generate_candidate_for_side("BTCUSDT", "BUY", normalize_candles(stale))
    insufficient_result = engine.generate_candidate_for_side("BTCUSDT", "BUY", normalize_candles(_candles(count=10)))

    assert stale_result["rejection_reason"] == "STALE_MARKET_DATA"
    assert insufficient_result["rejection_reason"] == "INSUFFICIENT_MARKET_DATA"


def test_valid_real_signal_creates_accepted_candidate_and_auto_published_signal(tmp_path):
    db = _db(tmp_path)
    engine = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
    )

    result = engine.generate_candidate_for_side("BTCUSDT", "BUY", normalize_candles(_candles(direction="up")))

    assert result["accepted"] is True
    assert result["signal_id"]
    with db.connect() as conn:
        candidate = conn.execute("SELECT status FROM signal_candidates WHERE id = ?", (result["candidate_id"],)).fetchone()
        signal = conn.execute("SELECT is_published, published_at, status, expires_at, signal_reason FROM trading_signals WHERE id = ?", (result["signal_id"],)).fetchone()
    assert candidate["status"] == "ACCEPTED"
    assert signal["is_published"] == 1
    assert signal["published_at"] is not None
    assert signal["status"] == SIGNAL_STATUS_PENDING_ENTRY
    published_at = datetime.fromisoformat(signal["published_at"].replace("Z", "+00:00"))
    expires_at = datetime.fromisoformat(signal["expires_at"].replace("Z", "+00:00"))
    assert timedelta(minutes=119) <= expires_at - published_at <= timedelta(minutes=121)
    assert "Entry validity:" in signal["signal_reason"]
    assert result["auto_published"] is True


def test_dev_signal_remains_unpublished_even_when_quality_passes(tmp_path):
    db = _db(tmp_path)
    engine = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
        dev_mode=True,
    )

    result = engine.generate_candidate_for_side("BTCUSDT", "BUY", normalize_candles(_candles(direction="up")))

    assert result["accepted"] is True
    assert result["auto_published"] is False
    with db.connect() as conn:
        signal = conn.execute("SELECT is_published, published_at, dev_mode, source FROM trading_signals WHERE id = ?", (result["signal_id"],)).fetchone()
    assert signal["is_published"] == 0
    assert signal["published_at"] is None
    assert signal["dev_mode"] == 1
    assert signal["source"] == "dev_mock_signal_engine"


def test_duplicate_signal_is_rejected(tmp_path):
    db = _db(tmp_path)
    repository = SignalRepository(db)
    engine = CryptoSignalEngine(
        repository=repository,
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
    )
    candles = normalize_candles(_candles(direction="up"))

    first = engine.generate_candidate_for_side("BTCUSDT", "BUY", candles)
    second = engine.generate_candidate_for_side("BTCUSDT", "BUY", candles)

    assert first["accepted"] is True
    assert second["accepted"] is False
    assert second["rejection_reason"] == "DUPLICATE_SIGNAL"


def test_generate_crypto_signals_summary_counts_candidates(tmp_path):
    db = _db(tmp_path)
    engine = CryptoSignalEngine(
        repository=SignalRepository(db),
        market_data=StaticMarketData(_candles(direction="up")),
        allowed_symbols=["BTCUSDT"],
        timeframe="1h",
    )

    summary = engine.generate_crypto_signals()

    assert summary["scanned_symbols"] == 1
    assert summary["candidates_created"] == 2
    assert summary["accepted"] >= 1
    assert summary["signals_created"] >= 1
    assert summary["errors"] == []


def test_engine_source_does_not_reference_execution_or_tradingview_queue():
    import app.signals.crypto_signal_engine as engine_module

    source = inspect.getsource(engine_module)

    assert "BinanceExecutor" not in source
    assert "execute_signal" not in source
    assert "create_external_signal_queue_row" not in source
    assert "auto_pilot" not in source.lower()
