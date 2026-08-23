from __future__ import annotations

import inspect
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "bot-backend"))
sys.path.insert(0, str(ROOT / "backends" / "bot-backend" / "scripts"))

import generate_daily_crypto_signals as script  # noqa: E402
from app.signals.crypto_signal_engine import ALLOWED_CRYPTO_SYMBOLS, CryptoSignalEngine  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    acquire_signal_operation_lock,
    blacklist_signal_pair,
    get_signal_scan_run,
    get_signal_setting,
    is_signal_operation_locked,
    list_signal_scan_results,
    release_signal_operation_lock,
    set_signal_setting,
    upsert_signal_pair,
    upsert_signal_pair_metrics,
)


class StaticMarketData:
    def __init__(self, candles):
        self.candles = candles

    def fetch_candles(self, symbol: str, timeframe: str, limit: int):
        return self.candles[-limit:]


class FakeEngine:
    calls: list[dict] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        FakeEngine.calls.append(kwargs)

    def generate_crypto_signals(self, symbols=None):
        return {
            "scanned_symbols": len(symbols or ALLOWED_CRYPTO_SYMBOLS),
            "candidates_created": 2,
            "accepted": 1,
            "rejected": 1,
            "signals_created": 1,
            "published": 1,
            "errors": [],
        }


class NoSetupEngine:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def generate_crypto_signals(self, symbols=None):
        return {
            "scanned_symbols": len(symbols or []),
            "candidates_created": 2,
            "accepted": 0,
            "rejected": 2,
            "signals_created": 0,
            "published": 0,
            "errors": [],
        }


class ErrorSummaryEngine:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def generate_crypto_signals(self, symbols=None):
        return {
            "scanned_symbols": 2,
            "candidates_created": 2,
            "accepted": 0,
            "rejected": 1,
            "signals_created": 0,
            "published": 0,
            "errors": [{"symbol": "ETHUSDT", "error": "market data unavailable"}],
        }


class PartialScanEngine:
    calls: list[str] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def scan_symbol(self, symbol):
        PartialScanEngine.calls.append(symbol)
        if symbol == "ETHUSDT":
            raise RuntimeError("market data unavailable")
        return [
            {"accepted": True, "signal_id": "sig1", "auto_published": True},
            {"accepted": False, "rejection_reason": "LOW_CONFIDENCE"},
        ]


class FailingInitEngine:
    def __init__(self, **kwargs):
        raise RuntimeError("engine boot failed")


class TemporaryFailureEngine:
    attempts: dict[str, int] = {}

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def scan_symbol(self, symbol):
        attempts = TemporaryFailureEngine.attempts.get(symbol, 0)
        TemporaryFailureEngine.attempts[symbol] = attempts + 1
        if attempts == 0:
            raise RuntimeError("temporary rate limit 429")
        return [{"accepted": True, "signal_id": f"sig_{symbol}", "auto_published": False}]


def _candles(*, direction: str = "up", count: int = 120, start: float = 100.0):
    now = datetime.now(timezone.utc)
    rows = []
    price = start
    step = 0.08 if direction == "up" else -0.08
    for index in range(count):
        open_price = price
        close = price + step
        high = max(open_price, close) + 0.08
        low = min(open_price, close) - 0.08
        rows.append(
            [
                int((now - timedelta(hours=count - index - 1)).timestamp() * 1000),
                f"{open_price:.8f}",
                f"{high:.8f}",
                f"{low:.8f}",
                f"{close:.8f}",
                "100000",
            ]
        )
        price = close
    return rows


def test_generation_script_imports_without_side_effects():
    assert hasattr(script, "run_generation")
    assert hasattr(script, "main")
    assert script.SUMMARY_FIELDS == (
        "scan_run_id",
        "scanned_symbols",
        "eligible_symbols",
        "skipped_symbols",
        "candidates_created",
        "accepted",
        "rejected",
        "ranked",
        "signals_created",
        "published",
        "not_published_due_to_limits",
        "errors",
    )
    assert script.is_dev_signal_mode_enabled() is False


def test_run_generation_calls_engine_and_returns_required_summary_fields():
    FakeEngine.calls = []

    summary = script.run_generation(
        symbols=["BTCUSDT", "ETHUSDT"],
        dry_run=True,
        engine_factory=FakeEngine,
    )

    assert set(script.SUMMARY_FIELDS).issubset(summary.keys())
    assert summary["scanned_symbols"] == 2
    assert summary["signals_created"] == 1
    assert summary["published"] == 0
    assert summary["dry_run"] is True
    assert summary["database_writes"] == 0
    assert FakeEngine.calls[0]["allowed_symbols"] == ("BTCUSDT", "ETHUSDT")


def test_no_valid_setup_does_not_force_signals():
    summary = script.run_generation(
        symbols=["BTCUSDT"],
        dry_run=True,
        engine_factory=NoSetupEngine,
    )

    assert summary["accepted"] == 0
    assert summary["signals_created"] == 0
    assert summary["published"] == 0
    assert summary["errors"] == []


def test_symbol_errors_are_reported_in_summary():
    summary = script.run_generation(
        symbols=["BTCUSDT", "ETHUSDT"],
        dry_run=True,
        engine_factory=ErrorSummaryEngine,
    )

    assert summary["published"] == 0
    assert summary["errors"] == [{"symbol": "ETHUSDT", "error": "market data unavailable"}]


def test_dev_mode_requires_dev_signal_mode_enabled(monkeypatch):
    monkeypatch.delenv("DEV_SIGNAL_MODE", raising=False)

    try:
        script.run_generation(symbols=["BTCUSDT"], dry_run=True, dev_mode=True, engine_factory=FakeEngine)
    except RuntimeError as exc:
        assert "DEV_SIGNAL_MODE" in str(exc)
    else:
        raise AssertionError("dev mode should be blocked when DEV_SIGNAL_MODE is not enabled")


def test_mock_dev_signals_require_flag_and_are_labeled(tmp_path, monkeypatch):
    monkeypatch.setenv("DEV_SIGNAL_MODE", "true")
    db_path = str(tmp_path / "mock_dev_signals.db")

    summary = script.create_mock_dev_signals(db_path=db_path, symbols=["BTCUSDT"])

    assert summary["signals_created"] == 1
    assert summary["published"] == 0
    with DB(path=db_path).connect() as conn:
        signal = conn.execute("SELECT is_published, dev_mode, source, signal_reason FROM trading_signals").fetchone()
        candidate = conn.execute("SELECT dev_mode, source, signal_reason FROM signal_candidates").fetchone()
    assert signal["is_published"] == 0
    assert signal["dev_mode"] == 1
    assert signal["source"] == "dev_mock_signal_engine"
    assert "DEV/TEST" in signal["signal_reason"]
    assert candidate["dev_mode"] == 1
    assert candidate["source"] == "dev_mock_signal_engine"
    assert "DEV/TEST" in candidate["signal_reason"]


def test_mock_dev_signals_are_not_created_automatically(monkeypatch):
    monkeypatch.setenv("DEV_SIGNAL_MODE", "true")
    FakeEngine.calls = []

    summary = script.run_generation(symbols=["BTCUSDT"], dry_run=True, engine_factory=FakeEngine)

    assert summary["signals_created"] == 1
    assert FakeEngine.calls


def test_real_engine_auto_publishes_signals_and_duplicate_prevention_holds(tmp_path):
    db_path = str(tmp_path / "daily_crypto_signals.db")

    def engine_factory(**kwargs):
        return CryptoSignalEngine(
            **kwargs,
            market_data=StaticMarketData(_candles(direction="up")),
        )

    first = script.run_generation(
        symbols=["BTCUSDT"],
        db_path=db_path,
        engine_factory=engine_factory,
    )
    second = script.run_generation(
        symbols=["BTCUSDT"],
        db_path=db_path,
        engine_factory=engine_factory,
    )

    assert first["published"] == first["signals_created"]
    assert first["signals_created"] >= 1
    assert second["published"] == 0
    with DB(path=db_path).connect() as conn:
        signals = conn.execute("SELECT id, is_published, published_at, expires_at FROM trading_signals").fetchall()
        rejected_duplicates = conn.execute(
            "SELECT COUNT(*) AS c FROM signal_candidates WHERE rejection_reason = 'DUPLICATE_SIGNAL'"
        ).fetchone()
    assert len(signals) == first["signals_created"]
    assert all(row["is_published"] == 1 for row in signals)
    assert all(row["published_at"] is not None for row in signals)
    for row in signals:
        published_at = datetime.fromisoformat(row["published_at"].replace("Z", "+00:00"))
        expires_at = datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00"))
        assert timedelta(minutes=119) <= expires_at - published_at <= timedelta(minutes=121)
    assert rejected_duplicates["c"] >= 1


def _seed_pair(db_path: str, symbol: str, *, tier: str = "TIER_2", safe: bool = True, blacklisted: bool = False):
    migrate(db_path)
    db = DB(path=db_path)
    upsert_signal_pair(
        {
            "symbol": symbol,
            "exchange": "binance_futures",
            "asset_class": "crypto",
            "quote_asset": "USDT",
            "contract_type": "PERPETUAL",
            "tier": tier,
            "enabled": 1,
        },
        db=db,
    )
    upsert_signal_pair_metrics(
        {
            "symbol": symbol,
            "exchange": "binance_futures",
            "quote_volume_24h": 80_000_000,
            "spread_percent": 0.05,
            "is_safe": 1 if safe else 0,
            "unsafe_reason": None if safe else "LOW_VOLUME",
            "reliability_score": 80,
        },
        db=db,
    )
    if blacklisted:
        blacklist_signal_pair(symbol, "test blacklist", db=db)


def test_eligible_universe_mode_respects_tiers_and_max_symbols(tmp_path):
    db_path = str(tmp_path / "eligible_universe.db")
    _seed_pair(db_path, "DOTUSDT", tier="TIER_2")
    _seed_pair(db_path, "AAVEUSDT", tier="TIER_2")
    _seed_pair(db_path, "XMRUSDT", tier="DISCOVERED")

    summary = script.run_generation(
        db_path=db_path,
        use_eligible_universe=True,
        tiers=("TIER_2",),
        max_symbols=1,
        engine_factory=PartialScanEngine,
    )

    assert summary["scan_run_id"]
    assert summary["eligible_symbols"] == 1
    assert summary["scanned_symbols"] == 1
    run = get_signal_scan_run(summary["scan_run_id"], db=DB(path=db_path))
    assert run["status"] == "COMPLETED"
    assert run["duration_seconds"] is not None
    rows = list_signal_scan_results(scan_run_id=summary["scan_run_id"], db=DB(path=db_path))
    assert len(rows) == 1
    assert rows[0]["symbol"] in {"DOTUSDT", "AAVEUSDT"}


def test_chunking_sleeps_between_chunks(tmp_path):
    db_path = str(tmp_path / "chunking.db")
    sleeps = []
    PartialScanEngine.calls = []

    summary = script.run_generation(
        symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        db_path=db_path,
        engine_factory=PartialScanEngine,
        chunk_size=1,
        sleep_between_chunks=0.5,
        sleep_fn=lambda seconds: sleeps.append(seconds),
    )

    assert summary["scanned_symbols"] == 2
    assert sleeps == [0.5, 0.5]
    assert PartialScanEngine.calls == ["BTCUSDT", "ETHUSDT", "BNBUSDT"]


def test_retry_backoff_retries_temporary_errors(tmp_path):
    db_path = str(tmp_path / "retry.db")
    sleeps = []
    TemporaryFailureEngine.attempts = {}

    summary = script.run_generation(
        symbols=["BTCUSDT"],
        db_path=db_path,
        engine_factory=TemporaryFailureEngine,
        retry_base_delay=0.25,
        sleep_between_chunks=0,
        sleep_fn=lambda seconds: sleeps.append(seconds),
    )

    assert summary["errors"] == []
    assert TemporaryFailureEngine.attempts["BTCUSDT"] == 2
    assert sleeps == [0.25]


def test_publishing_limits_rank_and_cap_outputs(tmp_path):
    db_path = str(tmp_path / "ranking_limits.db")

    summary = script.run_generation(
        symbols=["BTCUSDT", "BNBUSDT"],
        db_path=db_path,
        engine_factory=PartialScanEngine,
        max_published_per_scan=1,
        sleep_between_chunks=0,
    )

    assert summary["ranked"] == 2
    assert summary["published"] == 1
    assert summary["not_published_due_to_limits"] == 1
    assert summary["ranked_candidates"][0]["published"] is True
    assert summary["ranked_candidates"][1]["limit_reason"] == "MAX_PUBLISHED_PER_SCAN_REACHED"


def test_refresh_universe_calls_pair_discovery(tmp_path, monkeypatch):
    db_path = str(tmp_path / "refresh_universe.db")
    calls = []

    class FakeDiscovery:
        def __init__(self, db=None):
            self.db = db

        def discover_binance_futures_pairs(self, **kwargs):
            calls.append(kwargs)
            return {"errors": []}

    monkeypatch.setattr(script, "PairDiscoveryService", FakeDiscovery)
    script.run_generation(
        symbols=["BTCUSDT"],
        db_path=db_path,
        refresh_universe=True,
        engine_factory=PartialScanEngine,
        sleep_between_chunks=0,
    )

    assert calls
    assert calls[0]["min_quote_volume_24h"] == 50_000_000


def test_cli_override_skips_blacklisted_unsafe_and_unknown_symbols(tmp_path):
    db_path = str(tmp_path / "symbol_skip.db")
    _seed_pair(db_path, "DOTUSDT", safe=True)
    _seed_pair(db_path, "LOWUSDT", safe=False)
    _seed_pair(db_path, "FILUSDT", safe=True, blacklisted=True)

    summary = script.run_generation(
        symbols=["DOTUSDT", "LOWUSDT", "FILUSDT", "UNKNOWNUSDT"],
        db_path=db_path,
        engine_factory=PartialScanEngine,
    )

    assert summary["scanned_symbols"] == 1
    assert summary["skipped_symbols"] == 3
    rows = list_signal_scan_results(scan_run_id=summary["scan_run_id"], db=DB(path=db_path))
    skipped = {row["symbol"]: row["skip_reason"] for row in rows if row["was_skipped"]}
    assert skipped["LOWUSDT"] == "LOW_VOLUME"
    assert skipped["FILUSDT"] == "BLACKLISTED_SYMBOL"
    assert skipped["UNKNOWNUSDT"] == "SYMBOL_NOT_ELIGIBLE"


def test_scan_run_is_partial_when_symbol_errors(tmp_path):
    db_path = str(tmp_path / "partial_scan.db")
    summary = script.run_generation(
        symbols=["BTCUSDT", "ETHUSDT"],
        db_path=db_path,
        engine_factory=PartialScanEngine,
    )

    assert summary["errors"] == [{"symbol": "ETHUSDT", "error": "market data unavailable"}]
    run = get_signal_scan_run(summary["scan_run_id"], db=DB(path=db_path))
    assert run["status"] == "PARTIAL"
    rows = list_signal_scan_results(scan_run_id=summary["scan_run_id"], db=DB(path=db_path))
    assert len(rows) == 2
    assert any(row["symbol"] == "ETHUSDT" and row["skip_reason"] == "API_ERROR" for row in rows)


def test_scan_run_is_failed_when_whole_generation_fails(tmp_path):
    db_path = str(tmp_path / "failed_scan.db")
    try:
        script.run_generation(
            symbols=["BTCUSDT"],
            db_path=db_path,
            engine_factory=FailingInitEngine,
        )
    except RuntimeError as exc:
        assert "engine boot failed" in str(exc)
    else:
        raise AssertionError("run_generation should raise whole-run engine failures")

    with DB(path=db_path).connect() as conn:
        run = conn.execute("SELECT status, errors FROM signal_scan_runs").fetchone()
    assert run["status"] == "FAILED"
    assert "engine boot failed" in run["errors"]


def test_dry_run_creates_no_scan_run(tmp_path):
    db_path = str(tmp_path / "dry_run_no_writes.db")
    migrate(db_path)
    summary = script.run_generation(
        symbols=["BTCUSDT"],
        db_path=db_path,
        dry_run=True,
        engine_factory=FakeEngine,
    )

    assert summary["scan_run_id"] is None
    with DB(path=db_path).connect() as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM signal_scan_runs").fetchone()["c"]
    assert count == 0


def test_operation_lock_prevents_overlap_and_expired_lock_can_be_taken_over(tmp_path):
    db_path = str(tmp_path / "operation_locks.db")
    migrate(db_path)
    db = DB(path=db_path)

    assert acquire_signal_operation_lock("SIGNAL_GENERATION", 60, locked_by="worker-a", db=db) is True
    assert is_signal_operation_locked("SIGNAL_GENERATION", db=db) is True
    assert acquire_signal_operation_lock("SIGNAL_GENERATION", 60, locked_by="worker-b", db=db) is False
    assert release_signal_operation_lock("SIGNAL_GENERATION", locked_by="worker-a", db=db) is True
    assert is_signal_operation_locked("SIGNAL_GENERATION", db=db) is False

    assert acquire_signal_operation_lock("SIGNAL_GENERATION", -1, locked_by="stale-worker", db=db) is True
    assert acquire_signal_operation_lock("SIGNAL_GENERATION", 60, locked_by="worker-c", db=db) is True


def test_scheduled_generation_respects_pause_setting(tmp_path):
    db_path = str(tmp_path / "paused_generation.db")
    migrate(db_path)
    db = DB(path=db_path)
    set_signal_setting("signal_generation_paused", "true", db=db)

    summary = script.run_generation(
        symbols=["BTCUSDT"],
        db_path=db_path,
        scheduled=True,
        engine_factory=FakeEngine,
    )

    assert summary["paused"] is True
    assert summary["scanned_symbols"] == 0
    assert get_signal_setting("signal_generation_paused", db=db) == "true"
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) AS c FROM signal_scan_runs").fetchone()["c"] == 0


def test_scheduled_generation_ignore_pause_bypasses_pause_setting(tmp_path):
    db_path = str(tmp_path / "ignore_pause_generation.db")
    migrate(db_path)
    db = DB(path=db_path)
    set_signal_setting("signal_generation_paused", "true", db=db)

    summary = script.run_generation(
        symbols=["BTCUSDT"],
        db_path=db_path,
        scheduled=True,
        ignore_pause=True,
        engine_factory=PartialScanEngine,
        sleep_between_chunks=0,
    )

    assert summary["paused"] is not True if "paused" in summary else True
    assert summary["scan_run_id"]
    assert is_signal_operation_locked("SIGNAL_GENERATION", db=db) is False


def test_scheduled_generation_exits_safely_when_lock_is_held(tmp_path):
    db_path = str(tmp_path / "locked_generation.db")
    migrate(db_path)
    db = DB(path=db_path)
    assert acquire_signal_operation_lock("SIGNAL_GENERATION", 60, locked_by="already-running", db=db)

    summary = script.run_generation(
        symbols=["BTCUSDT"],
        db_path=db_path,
        scheduled=True,
        ignore_pause=True,
        engine_factory=FakeEngine,
    )

    assert summary["lock_not_acquired"] is True
    assert summary["scanned_symbols"] == 0
    release_signal_operation_lock("SIGNAL_GENERATION", db=db)


def test_scheduled_rollout_mode_uses_tier_1_tier_2_and_excludes_tier_3_by_default(tmp_path):
    db_path = str(tmp_path / "scheduled_rollout.db")
    _seed_pair(db_path, "ETHUSDT", tier="TIER_1")
    _seed_pair(db_path, "DOTUSDT", tier="TIER_2")
    _seed_pair(db_path, "XMRUSDT", tier="TIER_3")
    PartialScanEngine.calls = []

    summary = script.run_generation(
        db_path=db_path,
        scheduled=True,
        ignore_pause=True,
        rollout_mode="TIER_1_TIER_2",
        max_symbols=50,
        engine_factory=PartialScanEngine,
        sleep_between_chunks=0,
    )

    assert summary["rollout_mode"] == "TIER_1_TIER_2"
    assert set(PartialScanEngine.calls) == {"ETHUSDT", "DOTUSDT"}
    assert "XMRUSDT" not in PartialScanEngine.calls


def test_tier_3_rollout_requires_explicit_enable(tmp_path):
    db_path = str(tmp_path / "tier3_blocked.db")
    _seed_pair(db_path, "XMRUSDT", tier="TIER_3")

    try:
        script.run_generation(
            db_path=db_path,
            scheduled=True,
            ignore_pause=True,
            rollout_mode="TIER_1_TIER_2_TIER_3",
            engine_factory=PartialScanEngine,
        )
    except RuntimeError as exc:
        assert "TIER_3" in str(exc)
    else:
        raise AssertionError("Tier 3 rollout should require explicit enablement")


def test_generation_script_source_does_not_reference_execution_paths():
    source = inspect.getsource(script)

    assert "BinanceExecutor" not in source
    assert "execute_signal" not in source
    assert "create_external_signal_queue_row" not in source
    assert "auto_pilot" not in source.lower()
    assert "place_order" not in source
