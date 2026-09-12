"""Connected-broker market universe.

A bot's markets come from the broker account it is connected to -- not from
``.env``, not from a list copied into the bot row at deploy time. The pipeline:

    connected broker -> instruments (cached discovery)
                     -> hard eligibility -> market quality -> deterministic rank
                     -> active shortlist -> Master Ensemble -> threshold -> risk -> execution

The universe only decides which symbols may produce NEW entries. Positions the
bot holds are managed first, every cycle, whether or not they still rank.

Everything here runs against fakes or a per-test temporary database; nothing
touches a broker or the canonical runtime database.
"""
from __future__ import annotations

import ast
import dataclasses
import inspect
import json
import re
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import get_args
from unittest.mock import Mock, patch

import pytest

from app.universe.adapters import (
    BinanceUsdmUniverseAdapter,
    RequestBudget,
    UniverseAdapterUnavailable,
    adapter_for,
)
from app.universe.contracts import Exclusion, InstrumentMeta, MarketStats, Product, UniverseMode
from app.universe.engine import UniverseConfig, UniverseEngine, hard_exclusion, quality_exclusion
from app.universe.evidence import record_universe_snapshot
from app.universe.identity import (
    PERP,
    canonical_instrument,
    parse_venue_symbol,
    split_multiplier,
    underlying_from_symbol,
)
from app.universe.runtime import UniverseRuntime, resolve_managed

BACKEND = Path(__file__).resolve().parents[1]
NOW = 1_800_000_000.0
NOW_MS = int(NOW * 1000)
DAY_MS = 86_400_000
BOT = "bot_universe_test"


# ── fixtures and fakes ───────────────────────────────────────────────────────


class Clock:
    def __init__(self, t: float = NOW) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def meta(
    symbol: str,
    base: str | None = None,
    *,
    quote: str = "USDT",
    margin: str | None = None,
    product: str = Product.PERPETUAL,
    status: str = "TRADING",
    underlying_type: str | None = "COIN",
    tick: float | None = 0.01,
    step: float | None = 0.001,
    min_qty: float | None = 0.001,
    min_notional: float | None = 5.0,
    listed_days: float | None = 400.0,
    venue: str = "fake_venue",
    kind: str = PERP,
) -> InstrumentMeta:
    base = base if base is not None else symbol[: -len(quote)]
    return InstrumentMeta(
        venue=venue,
        venue_symbol=symbol,
        canonical=canonical_instrument(base, quote, kind),
        status=status,
        tradable=status == "TRADING",
        product=product,
        underlying_type=underlying_type,
        quote_asset=quote,
        margin_asset=margin or quote,
        tick_size=tick,
        step_size=step,
        min_qty=min_qty,
        min_notional=min_notional,
        listed_at_ms=int(NOW_MS - listed_days * DAY_MS) if listed_days is not None else None,
        expires_at_ms=None,
    )


def st(
    qv: float | None = 1e9,
    spread: float | None = 1.0,
    price: float | None = 100.0,
    age_s: float = 5.0,
    count: int | None = 500_000,
) -> MarketStats:
    return MarketStats(
        quote_volume_24h=qv,
        trade_count_24h=count,
        last_price=price,
        spread_bps=spread,
        stats_time_ms=int(NOW_MS - age_s * 1000),
        open_interest=None,
    )


class FakeAdapter:
    venue = "fake_venue"

    def __init__(self, metas, stats, *, venue: str | None = None) -> None:
        self.metas = list(metas)
        self.stats = dict(stats)
        self.fail: Exception | None = None
        self.instrument_calls = 0
        self.stats_calls = 0
        self.budget = RequestBudget(used=None, limit=None)
        if venue:
            self.venue = venue

    def capabilities(self):
        return {"quote_volume_24h": True, "spread_bps": True, "open_interest": False}

    def instruments(self):
        self.instrument_calls += 1
        if self.fail:
            raise self.fail
        return list(self.metas)

    def market_stats(self):
        self.stats_calls += 1
        if self.fail:
            raise self.fail
        return dict(self.stats)

    def request_budget(self):
        return self.budget


def venue_of(n: int, *, first=("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK")):
    """n liquid perpetuals, most traded first."""
    bases = list(first) + [f"ALT{i:03d}" for i in range(max(0, n - len(first)))]
    metas, stats = [], {}
    for i, base in enumerate(bases[:n]):
        m = meta(f"{base}USDT", base)
        metas.append(m)
        stats[m.venue_symbol] = st(qv=1e10 / (i + 1), spread=1.0 + i * 0.01)
    return metas, stats


def engine_for(metas, stats, clock=None, **config) -> tuple[UniverseEngine, FakeAdapter, Clock]:
    clock = clock or Clock()
    adapter = FakeAdapter(metas, stats)
    return UniverseEngine(adapter, UniverseConfig(**config), clock=clock), adapter, clock


def temp_db(tmp_path, name="universe.db"):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    db = DB(str(tmp_path / name))
    migrate(db)
    return db


def _insert(conn, table: str, payload: dict) -> None:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    usable = {k: v for k, v in payload.items() if k in cols}
    conn.execute(
        f"INSERT INTO {table} ({','.join(usable)}) VALUES ({','.join('?' * len(usable))})",
        tuple(usable.values()),
    )


def bot_row(
    bot_id: str,
    *,
    strategy: str = "master_ensemble",
    market: str = "CRYPTO",
    config: str = "__auto_pilot__",
    symbols=("BTCUSDT", "ETHUSDT"),
    status: str = "active",
    mode: str = "live",
    capital: float = 120.0,
    universe_mode: str | None = None,
) -> dict:
    now = "2026-09-07T21:49:58+00:00"
    row = {
        "id": bot_id, "user_id": "user_universe", "broker_account_id": "brk_universe",
        "market_type": market, "strategy_id": strategy, "strategy_version": "1.0.0",
        "config_id": config, "risk_profile_id": config,
        "symbols_json": json.dumps(list(symbols)), "timeframes_json": json.dumps(["15m"]),
        "allocation_type": "fixed_amount", "allocation_value": capital,
        "capital_allocation": capital, "capital_allocation_type": "fixed_amount",
        "mode": mode, "status": status, "risk_level": "balanced",
        "created_at": now, "updated_at": now, "started_at": now,
    }
    if universe_mode is not None:
        row["universe_mode"] = universe_mode
    return row


def seed_owner(conn) -> None:
    now = "2026-09-07T21:49:58+00:00"
    _insert(conn, "users", {"id": "user_universe", "email": "universe@test.local",
                            "hashed_password": "!no-login", "is_active": 1, "status": "active",
                            "created_at": now, "updated_at": now})
    _insert(conn, "broker_accounts", {"id": "brk_universe", "user_id": "user_universe",
                                      "broker_id": "binance", "market_type": "crypto",
                                      "status": "connected", "environment": "demo", "label": "test",
                                      "active_credential_version": 1,
                                      "created_at": now, "updated_at": now})


class FakeLedgerDB:
    """``positions`` rows the runner reads for OPEN ledger positions."""

    def __init__(self, open_symbols=()) -> None:
        self.open_symbols = list(open_symbols)

    @contextmanager
    def connect(self):
        rows = [(s,) for s in self.open_symbols]
        yield SimpleNamespace(execute=lambda sql, params=(): SimpleNamespace(fetchall=lambda: rows))


def bare_runner(*, db=None, runtime=None, interval="15m", bot_id=BOT):
    """A PaperRunner shell carrying only what the universe hooks read."""
    from app.runner.runner import PaperRunner

    r = PaperRunner.__new__(PaperRunner)
    r.context = SimpleNamespace(bot_instance_id=bot_id, broker_account_id="brk_universe",
                                max_leverage=10.0, capital_budget=120.0, broker_type="binance",
                                universe_mode="BROKER")
    r.universe_mode = UniverseMode.BROKER
    r._universe_runtime = runtime
    r._universe_open_symbols = set()
    r._next_candle_due_ms = {}
    r._last_closed_candle_ms = {}
    r._universe_deferred = 0
    r._last_quiet_feed_check = 0.0
    r.state = {}
    r.store = SimpleNamespace(load_symbols=lambda: {})
    r.db = db or FakeLedgerDB()
    r.run_id = "run_universe"
    r.runtime_session_id = None
    r.interval = interval
    r.trade_symbols = []
    r.orchestrator = None
    return r


def held_state(position="SHORT", pending="NONE"):
    from app.runner.models import SymbolState

    s = SymbolState()
    s.position = position
    s.pending_open = pending
    return s


# ══════════════════════════════════════════════════════════════════════════
# 1. Universe mode: BROKER by default, ALLOWLIST only when explicit
# ══════════════════════════════════════════════════════════════════════════


def test_the_default_universe_mode_is_the_connected_broker():
    from app.core.config import settings

    assert settings.UNIVERSE_DEFAULT_MODE == "BROKER"
    assert int(settings.UNIVERSE_ACTIVE_LIMIT) > 2


@pytest.mark.parametrize("raw, expected", [
    ("BROKER", "BROKER"), ("auto", "BROKER"), ("dynamic", "BROKER"),
    ("ALLOWLIST", "ALLOWLIST"), ("custom", "ALLOWLIST"), ("static", "ALLOWLIST"),
    (None, None), ("", None), ("  ", None),
])
def test_universe_mode_aliases_normalize(raw, expected):
    assert UniverseMode.normalize(raw) == expected


def test_an_unknown_universe_mode_is_an_error_not_a_default():
    with pytest.raises(ValueError):
        UniverseMode.normalize("everything")


def test_the_required_exclusion_reason_codes_exist():
    for code in ("NOT_FUTURES", "NOT_TRADING", "INSUFFICIENT_HISTORY", "LOW_LIQUIDITY",
                 "SPREAD_TOO_WIDE", "MIN_NOTIONAL_UNSUPPORTED", "STALE_DATA",
                 "UNSUPPORTED_CONTRACT", "RATE_LIMIT_DEFERRED", "USER_ALLOWLIST_EXCLUDED"):
        assert getattr(Exclusion, code) == code


# ══════════════════════════════════════════════════════════════════════════
# 2. Broker mode is not limited to BTC/ETH
# ══════════════════════════════════════════════════════════════════════════


def test_broker_mode_trades_the_venue_not_two_hardcoded_symbols():
    metas, stats = venue_of(30)
    engine, _, _ = engine_for(metas, stats, active_limit=25)
    snap = engine.refresh(broker_account_id="brk_universe")

    assert snap.mode == UniverseMode.BROKER
    assert snap.discovered_count == 30 and snap.eligible_count == 30
    assert snap.active_count == 25
    assert set(snap.active_symbols) - {"BTCUSDT", "ETHUSDT"}, "only BTC/ETH selected"
    assert {m.selection_reason for m in snap.active} == {"RANKED_BY_QUOTE_VOLUME"}


def test_the_active_limit_caps_the_shortlist_and_records_why():
    metas, stats = venue_of(30)
    engine, _, _ = engine_for(metas, stats, active_limit=25)
    snap = engine.refresh(broker_account_id="brk_universe")

    below = [s for s, r in snap.excluded.items() if r == Exclusion.RANK_BELOW_ACTIVE_LIMIT]
    assert len(below) == 5
    assert snap.ranked_count == 30
    assert [m.rank for m in snap.active] == list(range(1, 26))


def test_an_active_limit_of_zero_selects_nothing():
    metas, stats = venue_of(5)
    engine, _, _ = engine_for(metas, stats, active_limit=0)
    assert engine.refresh(broker_account_id="b").active == ()


# ══════════════════════════════════════════════════════════════════════════
# 3. Hard eligibility
# ══════════════════════════════════════════════════════════════════════════


def _hard_cases():
    cap = UniverseConfig(max_position_notional=1200.0)
    base = UniverseConfig()
    return [
        ("spot", meta("BTCUSDT", "BTC", product=Product.SPOT), st(), base, Exclusion.NOT_FUTURES),
        ("delivery", meta("BTCUSDT_261225", "BTC", product=Product.DELIVERY), st(), base,
         Exclusion.UNSUPPORTED_CONTRACT),
        ("other contract", meta("XUSDT", "X", product=Product.OTHER), st(), base, Exclusion.UNSUPPORTED_CONTRACT),
        ("settling", meta("LUNAUSDT", "LUNA", status="SETTLING"), st(), base, Exclusion.NOT_TRADING),
        ("pending", meta("NEWUSDT", "NEW", status="PENDING_TRADING"), st(), base, Exclusion.NOT_TRADING),
        ("usdc", meta("BTCUSDC", "BTC", quote="USDC"), st(), base, Exclusion.QUOTE_UNSUPPORTED),
        ("coin-margined", meta("BTCUSDT", "BTC", margin="BTC"), st(), base, Exclusion.QUOTE_UNSUPPORTED),
        ("equity", meta("TSLAUSDT", "TSLA", underlying_type="EQUITY"), st(), base, Exclusion.UNSUPPORTED_CONTRACT),
        ("no tick", meta("AUSDT", "A", tick=None), st(), base, Exclusion.INVALID_INSTRUMENT_FILTERS),
        ("no step", meta("AUSDT", "A", step=None), st(), base, Exclusion.INVALID_INSTRUMENT_FILTERS),
        ("new listing", meta("FRESHUSDT", "FRESH", listed_days=3), st(), base, Exclusion.INSUFFICIENT_HISTORY),
        ("no price", meta("AUSDT", "A"), st(price=None), base, Exclusion.INVALID_PRICE),
        ("stale ticker", meta("AUSDT", "A"), st(age_s=7200), base, Exclusion.STALE_DATA),
        ("min notional", meta("AUSDT", "A", min_notional=2000.0), st(), cap, Exclusion.MIN_NOTIONAL_UNSUPPORTED),
        ("min qty x price", meta("AUSDT", "A", min_qty=1.0), st(price=5000.0), cap,
         Exclusion.MIN_NOTIONAL_UNSUPPORTED),
        ("eligible", meta("SOLUSDT", "SOL"), st(), cap, None),
    ]


@pytest.mark.parametrize("label, m, s, cfg, expected", _hard_cases(), ids=[c[0] for c in _hard_cases()])
def test_hard_eligibility(label, m, s, cfg, expected):
    assert hard_exclusion(m, s, NOW_MS, cfg) == expected


def test_hard_eligibility_without_statistics_checks_metadata_only():
    assert hard_exclusion(meta("SOLUSDT", "SOL"), None, NOW_MS, UniverseConfig(max_position_notional=10)) is None


# ══════════════════════════════════════════════════════════════════════════
# 4. Market quality -- UNKNOWN is never treated as good
# ══════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("label, s, expected", [
    ("no stats", None, Exclusion.MARKET_STATS_UNKNOWN),
    ("no volume", st(qv=None), Exclusion.MARKET_STATS_UNKNOWN),
    ("thin", st(qv=1e6), Exclusion.LOW_LIQUIDITY),
    ("wide", st(spread=25.0), Exclusion.SPREAD_TOO_WIDE),
    ("no spread", st(spread=None), Exclusion.MARKET_STATS_UNKNOWN),
    ("good", st(), None),
])
def test_market_quality(label, s, expected):
    assert quality_exclusion(s, UniverseConfig()) == expected


def test_the_snapshot_explains_every_exclusion():
    metas = [
        meta("BTCUSDT", "BTC"), meta("THINUSDT", "THIN"), meta("WIDEUSDT", "WIDE"),
        meta("SPOTUSDT", "SPOT", product=Product.SPOT), meta("OLDUSDT", "OLD", status="SETTLING"),
        meta("FRESHUSDT", "FRESH", listed_days=2), meta("STALEUSDT", "STALE"),
        meta("BIGUSDT", "BIG", min_notional=5000.0), meta("QTRUSDT", "QTR", product=Product.DELIVERY),
    ]
    stats = {
        "BTCUSDT": st(), "THINUSDT": st(qv=1e5), "WIDEUSDT": st(spread=40.0), "SPOTUSDT": st(),
        "OLDUSDT": st(), "FRESHUSDT": st(), "STALEUSDT": st(age_s=10_000), "BIGUSDT": st(), "QTRUSDT": st(),
    }
    engine, _, _ = engine_for(metas, stats, max_position_notional=1200.0)
    snap = engine.refresh(broker_account_id="b")

    assert snap.active_symbols == ("BTCUSDT",)
    assert snap.excluded == {
        "BIGUSDT": Exclusion.MIN_NOTIONAL_UNSUPPORTED,
        "FRESHUSDT": Exclusion.INSUFFICIENT_HISTORY,
        "OLDUSDT": Exclusion.NOT_TRADING,
        "QTRUSDT": Exclusion.UNSUPPORTED_CONTRACT,
        "SPOTUSDT": Exclusion.NOT_FUTURES,
        "STALEUSDT": Exclusion.STALE_DATA,
        "THINUSDT": Exclusion.LOW_LIQUIDITY,
        "WIDEUSDT": Exclusion.SPREAD_TOO_WIDE,
    }
    member = snap.active[0]
    assert (member.rank, member.canonical_id, member.underlying) == (1, "BTC/USDT:PERP", "BTC")
    assert member.quote_volume_24h == 1e9 and member.spread_bps == 1.0


# ══════════════════════════════════════════════════════════════════════════
# 5. Deterministic ranking
# ══════════════════════════════════════════════════════════════════════════


def test_ranking_is_volume_then_spread_then_symbol_whatever_the_input_order():
    metas = [meta(f"{b}USDT", b) for b in ("ZED", "AAA", "MID", "TOP", "TIE")]
    stats = {
        "TOPUSDT": st(qv=9e9), "MIDUSDT": st(qv=5e9, spread=2.0),
        "TIEUSDT": st(qv=5e9, spread=2.0), "AAAUSDT": st(qv=5e9, spread=1.0), "ZEDUSDT": st(qv=1e9),
    }
    expected = ("TOPUSDT", "AAAUSDT", "MIDUSDT", "TIEUSDT", "ZEDUSDT")
    for order in (metas, list(reversed(metas))):
        engine, _, _ = engine_for(order, stats)
        assert engine.refresh(broker_account_id="b").active_symbols == expected


# ══════════════════════════════════════════════════════════════════════════
# 6. Explicit allowlist
# ══════════════════════════════════════════════════════════════════════════


def test_an_allowlist_restricts_to_the_users_symbols_in_the_users_order():
    metas, stats = venue_of(10)
    engine, _, _ = engine_for(metas, stats)
    snap = engine.refresh(broker_account_id="b", mode="custom", allowlist=["solusdt", "BTCUSDT"])

    assert snap.mode == UniverseMode.ALLOWLIST
    assert snap.active_symbols == ("SOLUSDT", "BTCUSDT")
    assert {m.selection_reason for m in snap.active} == {"USER_ALLOWLIST"}
    assert snap.excluded["ETHUSDT"] == Exclusion.USER_ALLOWLIST_EXCLUDED


def test_an_allowlist_still_obeys_hard_eligibility_and_reports_unknown_symbols():
    metas = [meta("BTCUSDT", "BTC"), meta("DEADUSDT", "DEAD", status="SETTLING")]
    engine, _, _ = engine_for(metas, {"BTCUSDT": st(), "DEADUSDT": st()})
    snap = engine.refresh(broker_account_id="b", mode="ALLOWLIST",
                          allowlist=["DEADUSDT", "BTCUSDT", "NOSUCHUSDT"])

    assert snap.active_symbols == ("BTCUSDT",)
    assert snap.excluded["DEADUSDT"] == Exclusion.NOT_TRADING
    assert snap.excluded["NOSUCHUSDT"] == Exclusion.NOT_TRADING


# ══════════════════════════════════════════════════════════════════════════
# 7. Two-stage efficiency: cached discovery, batched statistics
# ══════════════════════════════════════════════════════════════════════════


def test_discovery_and_statistics_are_cached_not_fetched_per_cycle():
    metas, stats = venue_of(20)
    engine, adapter, clock = engine_for(metas, stats, metadata_ttl_seconds=3600, stats_ttl_seconds=900)

    for _ in range(5):
        engine.refresh(broker_account_id="b")
    assert (adapter.instrument_calls, adapter.stats_calls) == (1, 1)

    clock.t += 900
    engine.refresh(broker_account_id="b")
    assert (adapter.instrument_calls, adapter.stats_calls) == (1, 2)

    clock.t += 2700
    engine.refresh(broker_account_id="b")
    assert (adapter.instrument_calls, adapter.stats_calls) == (2, 3)
    # instruments = 1 request, market stats = 2 batched requests -- for the whole venue.
    assert engine.request_count == 2 * 1 + 3 * 2


def test_the_runtime_refreshes_on_its_cadence_not_every_cycle():
    metas, stats = venue_of(10)
    engine, adapter, clock = engine_for(metas, stats)
    runtime = UniverseRuntime(engine=engine, broker_account_id="b", bot_instance_id=BOT,
                              refresh_seconds=900, clock=clock)

    assert runtime.resolve(open_symbols=[]).refreshed is True
    clock.t += 60
    assert runtime.resolve(open_symbols=[]).refreshed is False
    assert runtime.resolve(open_symbols=[], force=True).refreshed is True
    clock.t += 900
    assert runtime.resolve(open_symbols=[]).refreshed is True
    assert adapter.instrument_calls == 1  # metadata TTL (1h) not reached


# ══════════════════════════════════════════════════════════════════════════
# 8. Rate limits: backoff, stale fallback, deferral -- never a blocking loop
# ══════════════════════════════════════════════════════════════════════════


def test_a_rate_limited_refresh_serves_the_last_good_data_flagged_stale():
    metas, stats = venue_of(10)
    engine, adapter, clock = engine_for(metas, stats, stats_ttl_seconds=900)
    good = engine.refresh(broker_account_id="b")

    clock.t += 901
    adapter.fail = RuntimeError("HTTP 429: Too many requests")
    degraded = engine.refresh(broker_account_id="b")

    assert degraded.stale is True and "429" in (degraded.error or "")
    assert degraded.active_symbols == good.active_symbols

    calls = adapter.stats_calls
    clock.t += 10  # inside the 30 s backoff
    engine.refresh(broker_account_id="b")
    assert adapter.stats_calls == calls, "retried inside the backoff window"


def test_once_cached_statistics_are_too_old_candidates_are_deferred_not_guessed():
    metas, stats = venue_of(10)
    engine, adapter, clock = engine_for(metas, stats, stats_ttl_seconds=900, stale_stats_multiplier=4.0)
    engine.refresh(broker_account_id="b")

    adapter.fail = RuntimeError("APIError -1003: Too much request weight used")
    clock.t += 901
    engine.refresh(broker_account_id="b")
    clock.t += 3000  # > 4 x TTL since the last good statistics
    snap = engine.refresh(broker_account_id="b")

    assert snap.active == ()
    assert set(snap.excluded.values()) == {Exclusion.RATE_LIMIT_DEFERRED}
    assert snap.stale is True


def test_an_ordinary_failure_marks_statistics_unknown_not_rate_limited():
    metas, stats = venue_of(3)
    engine, adapter, clock = engine_for(metas, stats, stats_ttl_seconds=60)
    engine.refresh(broker_account_id="b")
    adapter.fail = ConnectionError("connection reset")
    clock.t += 10_000
    snap = engine.refresh(broker_account_id="b")
    assert set(snap.excluded.values()) == {Exclusion.MARKET_STATS_UNKNOWN}


def test_backoff_is_exponential_and_capped():
    metas, stats = venue_of(3)
    engine, adapter, clock = engine_for(metas, stats, backoff_initial_seconds=30, backoff_max_seconds=900)
    adapter.fail = RuntimeError("HTTP 429")
    delays = []
    for _ in range(7):
        engine.refresh(broker_account_id="b")
        delays.append(round(engine._next_attempt_at - clock.t))
        clock.t = engine._next_attempt_at
    assert delays == [30, 60, 120, 240, 480, 900, 900]


def test_with_no_instrument_data_at_all_there_are_no_candidates():
    engine, adapter, _ = engine_for([], {})
    adapter.fail = RuntimeError("HTTP 418: IP banned")
    snap = engine.refresh(broker_account_id="b")
    assert snap.active == () and snap.discovered_count == 0
    assert snap.stale is True and "418" in snap.error


def test_the_binance_adapter_never_holds_the_runner_in_a_retry_loop():
    client = FakeBinanceClient()
    BinanceUsdmUniverseAdapter(client).market_stats()
    assert [c for c in client.calls if c[0] == "GET"] == [
        ("GET", "/fapi/v1/ticker/24hr", 1),
        ("GET", "/fapi/v1/ticker/bookTicker", 1),
    ]


# ══════════════════════════════════════════════════════════════════════════
# 9. Binance USD-M adapter (through the connected account's own client)
# ══════════════════════════════════════════════════════════════════════════


def _filters(tick="0.10", step="0.001", min_qty="0.001", notional="100"):
    return [
        {"filterType": "PRICE_FILTER", "tickSize": tick},
        {"filterType": "LOT_SIZE", "stepSize": step, "minQty": min_qty},
        {"filterType": "MIN_NOTIONAL", "notional": notional},
    ]


class FakeBinanceClient:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.last_used_weight_1m = 120
        onboard = NOW_MS - 400 * DAY_MS
        self.info = {
            "rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE",
                            "intervalNum": 1, "limit": 2400}],
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL",
                 "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT",
                 "underlyingType": "COIN", "onboardDate": onboard, "filters": _filters()},
                {"symbol": "1000PEPEUSDT", "status": "TRADING", "contractType": "PERPETUAL",
                 "baseAsset": "1000PEPE", "quoteAsset": "USDT", "marginAsset": "USDT",
                 "underlyingType": "COIN", "onboardDate": onboard,
                 "filters": _filters("0.0000001", "1", "1", "5")},
                {"symbol": "XAUUSDT", "status": "TRADING", "contractType": "TRADIFI_PERPETUAL",
                 "baseAsset": "XAU", "quoteAsset": "USDT", "marginAsset": "USDT",
                 "underlyingType": "COMMODITY", "onboardDate": onboard, "filters": _filters()},
                {"symbol": "BTCUSDT_261225", "status": "TRADING", "contractType": "CURRENT_QUARTER",
                 "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT",
                 "underlyingType": "COIN", "onboardDate": onboard, "deliveryDate": NOW_MS + 90 * DAY_MS,
                 "filters": _filters()},
                {"symbol": "NEWUSDT", "status": "PENDING_TRADING", "contractType": "PERPETUAL",
                 "baseAsset": "NEW", "quoteAsset": "USDT", "marginAsset": "USDT",
                 "underlyingType": "COIN", "onboardDate": NOW_MS, "filters": _filters()},
            ],
        }
        close = NOW_MS - 1000
        self.tickers = [
            {"symbol": "BTCUSDT", "quoteVolume": "9000000000", "count": 3000000, "lastPrice": "100000",
             "closeTime": close},
            {"symbol": "1000PEPEUSDT", "quoteVolume": "400000000", "count": 900000, "lastPrice": "0.0105",
             "closeTime": close},
            {"symbol": "XAUUSDT", "quoteVolume": "800000000", "count": 100000, "lastPrice": "2500",
             "closeTime": close},
            {"symbol": "BTCUSDT_261225", "quoteVolume": "90000000", "count": 1000, "lastPrice": "101000",
             "closeTime": close},
        ]
        self.books = [
            {"symbol": "BTCUSDT", "bidPrice": "100000.0", "askPrice": "100000.1"},
            {"symbol": "1000PEPEUSDT", "bidPrice": "0.0105", "askPrice": "0.0105001"},
        ]

    def exchange_info(self):
        self.calls.append(("exchange_info",))
        return self.info

    def _request(self, method, path, params=None, headers=None, max_retries=6):
        self.calls.append((method, path, max_retries))
        return {"/fapi/v1/ticker/24hr": self.tickers, "/fapi/v1/ticker/bookTicker": self.books}[path]


def test_the_binance_adapter_maps_instruments_to_canonical_identity():
    adapter = BinanceUsdmUniverseAdapter(FakeBinanceClient())
    by = {m.venue_symbol: m for m in adapter.instruments()}

    btc = by["BTCUSDT"]
    assert (btc.product, btc.tradable, btc.canonical.canonical_id) == (Product.PERPETUAL, True, "BTC/USDT:PERP")
    assert (btc.tick_size, btc.step_size, btc.min_qty, btc.min_notional) == (0.1, 0.001, 0.001, 100.0)
    assert btc.listed_at_ms == NOW_MS - 400 * DAY_MS

    pepe = by["1000PEPEUSDT"].canonical
    assert (pepe.underlying, pepe.multiplier) == ("PEPE", 1000)
    assert by["XAUUSDT"].product == Product.PERPETUAL and by["XAUUSDT"].underlying_type == "COMMODITY"
    assert by["BTCUSDT_261225"].product == Product.DELIVERY and by["BTCUSDT_261225"].expires_at_ms
    assert by["NEWUSDT"].tradable is False


def test_the_binance_adapter_batches_statistics_and_reads_the_weight_budget():
    client = FakeBinanceClient()
    adapter = BinanceUsdmUniverseAdapter(client)
    adapter.instruments()
    stats = adapter.market_stats()

    assert stats["BTCUSDT"].quote_volume_24h == 9e9
    assert stats["BTCUSDT"].spread_bps == pytest.approx(0.1 / 100000.05 * 10_000)
    assert stats["XAUUSDT"].spread_bps is None, "no book -> unknown, never zero"
    assert stats["XAUUSDT"].open_interest is None
    budget = adapter.request_budget()
    assert (budget.used, budget.limit) == (120, 2400)
    assert budget.fraction_used == pytest.approx(0.05)
    assert not adapter.capabilities()["open_interest"]


def test_the_binance_venue_end_to_end():
    engine = UniverseEngine(BinanceUsdmUniverseAdapter(FakeBinanceClient()),
                            UniverseConfig(max_position_notional=1200.0), clock=Clock())
    snap = engine.refresh(broker_account_id="brk_c729454e6c98")

    assert snap.venue == "binance_usdm"
    assert snap.active_symbols == ("BTCUSDT", "1000PEPEUSDT")
    assert snap.excluded == {
        "BTCUSDT_261225": Exclusion.UNSUPPORTED_CONTRACT,
        "NEWUSDT": Exclusion.NOT_TRADING,
        "XAUUSDT": Exclusion.UNSUPPORTED_CONTRACT,
    }


def test_the_binance_client_records_the_used_request_weight():
    from app.exchange.binance.client import BinanceFuturesClient

    # Constructor startup probes are unrelated to header parsing and must not
    # touch a venue in this unit test.
    with patch.object(BinanceFuturesClient, "exchange_info", return_value={"symbols": []}), \
         patch.object(BinanceFuturesClient, "sync_time", return_value=0):
        client = BinanceFuturesClient("k", "s", "https://demo-fapi.binance.com")
    assert client.last_used_weight_1m is None
    client._note_weight(SimpleNamespace(headers={"X-MBX-USED-WEIGHT-1M": "321"}))
    assert client.last_used_weight_1m == 321
    client._note_weight(SimpleNamespace(headers={}))
    assert client.last_used_weight_1m == 321


def test_the_binance_client_preserves_rate_limit_status_after_bounded_retries():
    from app.exchange.binance.client import BinanceFuturesClient

    with patch.object(BinanceFuturesClient, "exchange_info", return_value={"symbols": []}), \
         patch.object(BinanceFuturesClient, "sync_time", return_value=0):
        client = BinanceFuturesClient("k", "s", "https://demo-fapi.binance.com")
    response = SimpleNamespace(
        status_code=429,
        headers={"Retry-After": "0", "X-MBX-USED-WEIGHT-1M": "6000"},
        text="Too many requests",
        content=b"",
    )
    client.session.request = Mock(return_value=response)
    with patch("app.exchange.binance.client.time.sleep"), \
         patch("app.exchange.binance.client.random.uniform", return_value=0):
        with pytest.raises(RuntimeError, match="HTTP 429: Too many requests"):
            client._request("GET", "/fapi/v1/ticker/24hr", max_retries=1)
    assert client.session.request.call_count == 2
    assert client.last_used_weight_1m == 6000


# ══════════════════════════════════════════════════════════════════════════
# 10. Broker neutrality
# ══════════════════════════════════════════════════════════════════════════


def test_adapters_are_chosen_by_broker_type_and_unknown_brokers_fail_closed():
    assert isinstance(adapter_for("BINANCE", FakeBinanceClient()), BinanceUsdmUniverseAdapter)
    with pytest.raises(UniverseAdapterUnavailable):
        adapter_for("kraken", object())


def test_a_new_broker_is_an_adapter_registration_not_an_engine_change(monkeypatch):
    import app.universe.adapters as adapters

    monkeypatch.setattr(adapters, "_ADAPTERS", dict(adapters._ADAPTERS))
    okx_metas = [
        meta(vs.symbol, vs.instrument.underlying, venue="okx_swap")
        if False else InstrumentMeta(
            venue="okx_swap", venue_symbol=vs.symbol, canonical=vs.instrument, status="live",
            tradable=True, product=Product.PERPETUAL, underlying_type="COIN", quote_asset="USDT",
            margin_asset="USDT", tick_size=0.1, step_size=0.01, min_qty=0.01, min_notional=None,
            listed_at_ms=NOW_MS - 900 * DAY_MS, expires_at_ms=None,
        )
        for vs in (parse_venue_symbol("okx", "BTC-USDT-SWAP"), parse_venue_symbol("okx", "SOL-USDT-SWAP"))
    ]
    okx = FakeAdapter(okx_metas, {"BTC-USDT-SWAP": st(qv=5e9), "SOL-USDT-SWAP": st(qv=1e9)}, venue="okx_swap")
    adapters.register_adapter("OKX", lambda client: okx)

    engine = UniverseEngine(adapter_for("okx", None), UniverseConfig(), clock=Clock())
    snap = engine.refresh(broker_account_id="brk_okx")
    assert snap.venue == "okx_swap"
    assert snap.active_symbols == ("BTC-USDT-SWAP", "SOL-USDT-SWAP")
    assert snap.active[0].canonical_id == "BTC/USDT:PERP"


# ══════════════════════════════════════════════════════════════════════════
# 11. Canonical instrument identity
# ══════════════════════════════════════════════════════════════════════════


def test_the_same_exposure_has_one_canonical_identity_across_venues():
    binance = parse_venue_symbol("binance_usdm", "BTCUSDT", base_asset="BTC", quote_asset="USDT")
    okx = parse_venue_symbol("okx", "BTC-USDT-SWAP")
    bybit = parse_venue_symbol("bybit", "BTCUSDT", base_asset="BTC", quote_asset="USDT")

    assert binance.instrument.canonical_id == okx.instrument.canonical_id == bybit.instrument.canonical_id
    assert binance.instrument.same_exposure(canonical_instrument("BTC", "USDC"))


@pytest.mark.parametrize("base, expected", [
    ("1000PEPE", ("PEPE", 1000)), ("1000000MOG", ("MOG", 1_000_000)),
    ("1MBABYDOGE", ("BABYDOGE", 1_000_000)), ("BTC", ("BTC", 1)), ("1INCH", ("1INCH", 1)),
])
def test_multiplier_contracts_map_to_their_underlying(base, expected):
    assert split_multiplier(base) == expected


def test_concatenated_symbols_are_not_guessed_without_metadata():
    with pytest.raises(ValueError):
        parse_venue_symbol("binance_usdm", "BTCUSDT")
    assert underlying_from_symbol("1000SHIBUSDT") == "SHIB"
    assert underlying_from_symbol("ETHUSDC") == "ETH"


# ══════════════════════════════════════════════════════════════════════════
# 12. Open positions are managed first, always
# ══════════════════════════════════════════════════════════════════════════


def test_held_symbols_are_managed_first_even_when_they_no_longer_rank():
    metas, stats = venue_of(5)
    engine, _, _ = engine_for(metas, stats)
    snap = engine.refresh(broker_account_id="b")

    candidates, managed, _ = resolve_managed(snap, ["DELISTEDUSDT"])
    assert managed[0] == "DELISTEDUSDT"
    assert "DELISTEDUSDT" not in candidates
    assert set(snap.active_symbols) <= set(managed)


def test_a_second_contract_on_an_underlying_already_held_is_not_a_candidate():
    metas = [meta("BTCUSDT", "BTC"), meta("BTCUSDC", "BTC", quote="USDC"),
             meta("1000PEPEUSDT", "1000PEPE"), meta("SOLUSDT", "SOL")]
    stats = {m.venue_symbol: st(qv=1e9 - i) for i, m in enumerate(metas)}
    engine, _, _ = engine_for(metas, stats, settlement_assets=("USDT", "USDC"))
    runtime = UniverseRuntime(engine=engine, broker_account_id="b", bot_instance_id=BOT, clock=Clock())

    res = runtime.resolve(open_symbols=["BTCUSDT", "PEPEUSDT"])
    assert res.managed[:2] == ("BTCUSDT", "PEPEUSDT")
    assert res.candidates == ("SOLUSDT",)
    assert res.exposure_excluded == {"BTCUSDC": Exclusion.UNDERLYING_ALREADY_OPEN,
                                     "1000PEPEUSDT": Exclusion.UNDERLYING_ALREADY_OPEN}


def test_the_runner_manages_held_positions_first_and_adds_ranked_candidates():
    metas, stats = venue_of(6)
    engine, _, clock = engine_for(metas, stats)
    runtime = UniverseRuntime(engine=engine, broker_account_id="b", bot_instance_id=BOT, clock=clock)
    runner = bare_runner(db=FakeLedgerDB(["OLDUSDT"]), runtime=runtime)
    runner.state["BTCUSDT"] = held_state("SHORT")
    seen = {}
    runner.orchestrator = SimpleNamespace(update_allowed_symbols=lambda s, leverage=None: seen.update(s=list(s), lev=leverage))

    runner._apply_universe()

    assert runner.trade_symbols[:2] == ["BTCUSDT", "OLDUSDT"]
    assert {"ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT"} <= set(runner.trade_symbols)
    assert runner._universe_open_symbols == {"BTCUSDT", "OLDUSDT"}
    assert set(runner.trade_symbols) <= set(runner.state)
    assert seen == {"s": runner.trade_symbols, "lev": 10.0}


def test_a_pending_entry_is_held_too():
    runner = bare_runner()
    runner.state["SOLUSDT"] = held_state("NONE", pending="LONG")
    runner.state["XRPUSDT"] = held_state("NONE")
    assert runner._held_symbols() == ["SOLUSDT"]


def test_when_the_universe_fails_the_runner_still_manages_what_it_holds():
    class Broken:
        def resolve(self, **_):
            raise RuntimeError("venue unreachable")

    runner = bare_runner(runtime=Broken())
    runner.state["BTCUSDT"] = held_state("SHORT")
    runner._apply_universe()
    assert runner.trade_symbols == ["BTCUSDT"]


def test_the_env_symbol_list_is_not_the_broker_universe(monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "TRADE_SYMBOLS", "ETHUSDT,DOGEUSDT", raising=False)
    metas, stats = venue_of(3, first=("BTC", "SOL", "XRP"))
    engine, _, clock = engine_for(metas, stats)
    runtime = UniverseRuntime(engine=engine, broker_account_id="b", bot_instance_id=BOT, clock=clock)
    runner = bare_runner(runtime=runtime)
    runner._apply_universe()
    assert runner.trade_symbols == ["BTCUSDT", "SOLUSDT", "XRPUSDT"]


def test_an_allowlist_bot_is_untouched_by_the_universe_hooks():
    runner = bare_runner()
    runner.universe_mode = UniverseMode.ALLOWLIST
    runner.trade_symbols = ["BTCUSDT", "ETHUSDT"]
    runner._apply_universe()
    assert runner.trade_symbols == ["BTCUSDT", "ETHUSDT"]


def test_a_runner_built_without_init_skips_the_universe_hooks():
    from app.runner.runner import PaperRunner

    r = PaperRunner.__new__(PaperRunner)
    r.context = SimpleNamespace(bot_instance_id="bot-1")
    r._apply_universe()  # no universe attributes at all
    assert r._candle_pregate("BTCUSDT") is None


# ══════════════════════════════════════════════════════════════════════════
# 13. Evaluate only on a genuinely new closed candle; bounded cycle cost
# ══════════════════════════════════════════════════════════════════════════


def test_a_flat_candidate_between_closes_costs_no_fetch_and_no_trace():
    from app.decision.reasons import CycleReason

    runner = bare_runner(runtime=object())
    runner.state["SOLUSDT"] = held_state("NONE")
    runner._note_candle_close("SOLUSDT", int(time.time() * 1000) - 1000)

    result = runner._candle_pregate("SOLUSDT")
    assert result["reason_code"] == CycleReason.NO_NEW_CANDLE
    assert result["pregate"] == "CANDLE_NOT_DUE"
    assert result["evaluated"] is False


def test_the_pregate_never_skips_a_due_unknown_or_held_symbol():
    runner = bare_runner(runtime=object())
    now_ms = int(time.time() * 1000)
    runner.state["SOLUSDT"] = held_state("NONE")

    assert runner._candle_pregate("SOLUSDT") is None, "no known close yet"
    runner._next_candle_due_ms["SOLUSDT"] = now_ms - 1
    assert runner._candle_pregate("SOLUSDT") is None, "candle is due"

    runner._next_candle_due_ms["SOLUSDT"] = now_ms + 600_000
    runner._universe_open_symbols = {"SOLUSDT"}
    assert runner._candle_pregate("SOLUSDT") is None, "held symbols take the full path"

    runner._universe_open_symbols = set()
    runner.state["SOLUSDT"] = held_state("LONG")
    assert runner._candle_pregate("SOLUSDT") is None


def test_the_next_close_is_one_interval_after_the_last_plus_grace():
    runner = bare_runner(interval="15m")
    runner._note_candle_close("solusdt", 1_000_000)
    assert runner._next_candle_due_ms["SOLUSDT"] == 1_000_000 + 900_000 + 2_000
    runner._note_candle_close("SOLUSDT", "garbage")
    assert runner._last_closed_candle_ms["SOLUSDT"] == 1_000_000


def test_the_pregate_runs_before_any_fetch_or_candle_claim():
    from app.runner.runner import PaperRunner

    src = inspect.getsource(PaperRunner._step_symbol_evaluate)
    gate = src.index("self._candle_pregate(")
    assert gate < src.index("claim_candle(")
    if "klines(" in src:
        assert gate < src.index("klines(")


def test_the_cycle_budget_defers_candidates_never_held_symbols():
    from app.runner.runner import PaperRunner

    src = inspect.getsource(PaperRunner.run_cycle)
    block = src[src.index("_candidate_budget_exhausted") - 400: src.index("_candidate_budget_exhausted")]
    assert "_universe_open_symbols" in block
    assert "_candidate_started" in block
    assert src.index("_candidate_started = time.monotonic()") > src.index("for symbol in list(self.trade_symbols)")


def test_the_candidate_budget_is_time_and_request_weight():
    engine, adapter, _ = engine_for([], {})
    runner = bare_runner(runtime=SimpleNamespace(engine=engine))

    assert runner._candidate_budget_exhausted(time.monotonic() - 30, 20.0) is True
    assert runner._candidate_budget_exhausted(time.monotonic(), 20.0) is False
    adapter.budget = RequestBudget(used=3500, limit=6000)
    assert runner._candidate_budget_exhausted(time.monotonic(), 20.0) is True
    adapter.budget = RequestBudget(used=1000, limit=6000)
    assert runner._candidate_budget_exhausted(time.monotonic(), 20.0) is False


def test_quiet_candidates_prove_the_feed_with_one_batched_price_read():
    from app.ops.runtime_watchdog import get_watchdog

    bot = "bot_universe_quiet_feed"
    client = Mock()
    client.get_prices.return_value = {"SOLUSDT": 150.0, "XRPUSDT": 0.5}
    runner = bare_runner(bot_id=bot)
    runner._universe_runtime = object()
    runner.client = client
    runner.trade_symbols = ["BTCUSDT", "SOLUSDT", "XRPUSDT"]
    runner._universe_open_symbols = {"BTCUSDT"}
    runner._last_closed_candle_ms = {"SOLUSDT": 111, "XRPUSDT": 222}

    runner._after_universe_cycle(["ADAUSDT", "LINKUSDT"])
    runner._after_universe_cycle([])  # within 60 s: no second request

    assert client.get_prices.call_count == 1
    assert client.get_prices.call_args[0][0] == ["SOLUSDT", "XRPUSDT"]
    clocks = get_watchdog()._bots[bot]["clocks"]
    assert clocks["SOLUSDT:15m"].latest_available_closed_candle == 111
    assert runner._universe_deferred == 0


def test_the_watchdog_forgets_symbols_the_bot_no_longer_manages():
    from app.ops.runtime_watchdog import RuntimeWatchdog

    wd = RuntimeWatchdog()
    for s in ("BTCUSDT", "SOLUSDT", "XRPUSDT"):
        wd.market_data("b", s, "15m", latest_closed_candle=1)
    wd.retain_symbols("b", ["btcusdt", "SOLUSDT"])
    assert set(wd._bots["b"]["clocks"]) == {"BTCUSDT:15m", "SOLUSDT:15m"}
    wd.retain_symbols("unknown-bot", [])


# ══════════════════════════════════════════════════════════════════════════
# 14. Per-symbol leverage ceilings in a broad universe
# ══════════════════════════════════════════════════════════════════════════


def test_every_managed_symbol_gets_its_own_asset_class_leverage_ceiling():
    from app.core.trading_orchestrator import TradingOrchestrator
    from app.risk.system_limits import ConfigValidator, SystemLimits, UserConfigurableLimits

    stub = SimpleNamespace(
        user_config=UserConfigurableLimits(requested_leverage={"BTCUSDT": 10}, allowed_symbols=["BTCUSDT"]),
        config_validator=ConfigValidator(SystemLimits()),
    )
    calls = []

    def validate():
        calls.append(1)
        return TradingOrchestrator._validate_config(stub)

    stub._validate_config = validate
    TradingOrchestrator.update_allowed_symbols(stub, ["BTCUSDT", "SOLUSDT", "1000PEPEUSDT", "ETHUSDC"], leverage=10)

    assert stub.validated_config.requested_leverage == {
        "BTCUSDT": 10, "SOLUSDT": 7.0, "1000PEPEUSDT": 5.0, "ETHUSDC": 10,
    }
    assert stub.validated_config.allowed_symbols == ["BTCUSDT", "SOLUSDT", "1000PEPEUSDT", "ETHUSDC"]

    TradingOrchestrator.update_allowed_symbols(stub, ["btcusdt", "SOLUSDT", "1000PEPEUSDT", "ETHUSDC"], leverage=10)
    assert len(calls) == 1, "an unchanged universe must not re-validate"


def test_asset_classes_are_by_underlying_not_by_spelling():
    from app.risk.system_limits import AssetClass, ConfigValidator

    classify = ConfigValidator()._classify_asset
    assert classify("BTCUSDT") == AssetClass.MAJOR_CRYPTO
    assert classify("1000SHIBUSDT") == AssetClass.MEME_CRYPTO
    assert classify("1000PEPEUSDT") == AssetClass.MEME_CRYPTO
    assert classify("SOLUSDT") == AssetClass.ALT_CRYPTO
    assert classify("USDCUSDT") == AssetClass.STABLE_PAIRS


def test_the_runner_sizes_the_min_notional_check_from_capital_and_leverage():
    runner = bare_runner()
    runner.client = FakeBinanceClient()
    runtime = runner._build_universe_runtime()
    assert isinstance(runtime.engine.adapter, BinanceUsdmUniverseAdapter)
    assert runtime.engine.config.max_position_notional == 1200.0
    assert runtime.mode == UniverseMode.BROKER

    runner.context.broker_type = "kraken"
    assert runner._build_universe_runtime() is None


@pytest.mark.parametrize("raw, expected", [
    (None, "ALLOWLIST"), ("", "ALLOWLIST"), ("auto", "BROKER"), ("BROKER", "BROKER"),
    ("custom", "ALLOWLIST"), ("nonsense", "ALLOWLIST"),
])
def test_the_runner_resolves_its_universe_mode_without_widening(raw, expected):
    runner = bare_runner()
    runner.context.universe_mode = raw
    assert runner._resolve_universe_mode() == expected


# ══════════════════════════════════════════════════════════════════════════
# 15. Deploy contract: AUTO (default) and CUSTOM
# ══════════════════════════════════════════════════════════════════════════


def _deploy(**kwargs):
    from app.core.bot_instance_service import BotInstanceService
    from app.models.bot_instance_models import BotInstance

    db = Mock()
    service = BotInstanceService(db=db)
    with patch.object(service, "create_bot_instance") as create:
        create.return_value = BotInstance(
            id="bot_x", user_id="u", broker_account_id="brk_abc", market_type="CRYPTO",
            strategy_id="master_ensemble", strategy_version="1.0.0", risk_level="balanced",
            symbols=[], timeframes=["15m"], allocation_type="fixed_amount", allocation_value=100.0,
            mode="paper", status="active", created_at="t", updated_at="t",
        )
        service.deploy_auto_pilot(
            user_id="u", risk_level="balanced", allocation_type="fixed_amount", allocation_value=100.0,
            broker_account_ids=["brk_abc"], mode="paper", market_type="CRYPTO", **kwargs,
        )
        return create.call_args[0][0]


def test_an_auto_deploy_takes_the_connected_brokers_markets(monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "TRADE_SYMBOLS", "BTCUSDT,ETHUSDT", raising=False)
    request = _deploy()
    assert request.universe_mode == "BROKER"
    assert request.symbols == []
    assert request.validate() == [] or "At least one symbol is required" not in request.validate()


def test_a_custom_deploy_is_an_explicit_allowlist():
    request = _deploy(symbol_universe_mode="custom", symbols=["SOLUSDT", "XRPUSDT"])
    assert request.universe_mode == "ALLOWLIST"
    assert request.symbols == ["SOLUSDT", "XRPUSDT"]


def test_a_custom_deploy_without_symbols_is_refused():
    with pytest.raises(ValueError):
        _deploy(symbol_universe_mode="custom", symbols=[])


def test_the_api_accepts_auto_and_custom_and_defaults_to_auto():
    import app.api.auto_pilot as api

    model = api.DeployAutoPilotRequest
    fields = getattr(model, "model_fields", None) or model.__fields__
    field = fields["symbol_universe_mode"]
    assert set(get_args(field.annotation)) == {"auto", "custom"}
    assert field.default == "auto"
    assert "symbols" in fields

    proxy = (BACKEND.parent / "user-backend" / "app" / "api" / "auto_pilot_proxy.py").read_text(encoding="utf-8")
    assert re.search(r"Literal\[\s*[\"']auto[\"']\s*,\s*[\"']custom[\"']\s*\]", proxy)


def test_a_bot_request_needs_symbols_only_for_an_allowlist():
    from app.models.bot_instance_models import CreateBotInstanceRequest

    common = dict(user_id="u", broker_account_id="b", market_type="CRYPTO", strategy_id="s",
                  strategy_version="1", risk_level="balanced", timeframes=["15m"],
                  allocation_type="fixed_amount", allocation_value=10.0, mode="paper")
    fields = {f.name for f in dataclasses.fields(CreateBotInstanceRequest)} \
        if dataclasses.is_dataclass(CreateBotInstanceRequest) else None
    if fields is not None:
        common = {k: v for k, v in common.items() if k in fields}
    broker = CreateBotInstanceRequest(symbols=[], universe_mode="BROKER", **common)
    allow = CreateBotInstanceRequest(symbols=[], universe_mode="ALLOWLIST", **common)
    bogus = CreateBotInstanceRequest(symbols=["BTCUSDT"], universe_mode="EVERYTHING", **common)

    assert "At least one symbol is required" not in broker.validate()
    assert "At least one symbol is required" in allow.validate()
    assert "universe_mode must be BROKER or ALLOWLIST" in bogus.validate()


# ══════════════════════════════════════════════════════════════════════════
# 16. Migration of existing bots -- proven, recorded, never inferred
# ══════════════════════════════════════════════════════════════════════════


def test_the_migration_resolves_each_legacy_row_by_evidence(tmp_path):
    from shared_lib.persistence.universe_schema import AUTO_PILOT_REASON, migrate_bot_universe_modes

    db = temp_db(tmp_path)
    with db.connect() as conn:
        seed_owner(conn)
        _insert(conn, "bot_instances", bot_row("bot_autopilot"))
        _insert(conn, "bot_instances", bot_row("bot_custom", strategy="user_breakout", config="cfg_7",
                                               symbols=("SOLUSDT",)))
        _insert(conn, "bot_instances", bot_row("bot_empty", strategy="user_breakout", config="cfg_8", symbols=()))
        _insert(conn, "bot_instances", bot_row("bot_deleted", status="deleted"))
        _insert(conn, "bot_instances", bot_row("bot_archived", status="archived"))
        _insert(conn, "bot_instances", bot_row("bot_decided", universe_mode="ALLOWLIST", symbols=("BTCUSDT",)))
        _insert(conn, "bot_instances", bot_row("bot_forex", market="FOREX", symbols=("EURUSD",)))
        changed = migrate_bot_universe_modes(conn)
        rows = {r[0]: (r[1], json.loads(r[2])) for r in conn.execute(
            "SELECT id, universe_mode, symbols_json FROM bot_instances").fetchall()}
        records = {r[0]: (r[1], json.loads(r[2]), r[3]) for r in conn.execute(
            "SELECT bot_instance_id, new_mode, previous_symbols_json, reason FROM universe_mode_migrations"
        ).fetchall()}

    assert rows["bot_autopilot"] == ("BROKER", [])
    assert records["bot_autopilot"] == ("BROKER", ["BTCUSDT", "ETHUSDT"], AUTO_PILOT_REASON)
    assert rows["bot_custom"] == ("ALLOWLIST", ["SOLUSDT"]), "an explicit list must never be dropped"
    assert records["bot_custom"][2] == "EXPLICIT_SYMBOL_LIST"
    assert rows["bot_empty"] == ("BROKER", [])
    assert rows["bot_forex"] == ("ALLOWLIST", ["EURUSD"])
    assert rows["bot_deleted"] == (None, ["BTCUSDT", "ETHUSDT"])
    assert rows["bot_archived"] == (None, ["BTCUSDT", "ETHUSDT"])
    assert rows["bot_decided"] == ("ALLOWLIST", ["BTCUSDT"])
    assert {"bot_deleted", "bot_archived", "bot_decided"}.isdisjoint(records)
    assert {c["bot_instance_id"] for c in changed} == {"bot_autopilot", "bot_custom", "bot_empty", "bot_forex"}


def test_the_migration_changes_nothing_else_about_a_live_bot_and_is_idempotent(tmp_path):
    from shared_lib.persistence.migrations import migrate
    from shared_lib.persistence.universe_schema import migrate_bot_universe_modes

    db = temp_db(tmp_path)
    cols = ("mode", "broker_account_id", "capital_allocation", "strategy_id", "timeframes_json",
            "allocation_value", "status", "risk_level")
    with db.connect() as conn:
        seed_owner(conn)
        _insert(conn, "bot_instances", bot_row("bot_live_shape"))
        before = conn.execute(f"SELECT {','.join(cols)} FROM bot_instances WHERE id='bot_live_shape'").fetchone()
        migrate_bot_universe_modes(conn)
        assert migrate_bot_universe_modes(conn) == []
    migrate(db)  # the full migration chain again
    with db.connect() as conn:
        after = conn.execute(f"SELECT {','.join(cols)} FROM bot_instances WHERE id='bot_live_shape'").fetchone()
        mode = conn.execute("SELECT universe_mode FROM bot_instances WHERE id='bot_live_shape'").fetchone()[0]
        n = conn.execute("SELECT COUNT(*) FROM universe_mode_migrations").fetchone()[0]
    assert tuple(before) == tuple(after)
    assert tuple(after)[:3] == ("live", "brk_universe", 120.0)
    assert mode == "BROKER" and n == 1


# ══════════════════════════════════════════════════════════════════════════
# 17. Effective policy and run context accept a broker universe
# ══════════════════════════════════════════════════════════════════════════


def _policy_for(tmp_path, **row):
    from app.core.bot_instance_service import BotInstanceService
    from app.runner.effective_policy import resolve_effective_bot_policy

    db = temp_db(tmp_path)
    with db.connect() as conn:
        seed_owner(conn)
        _insert(conn, "bot_instances", bot_row("bot_policy", mode="paper", **row))
    service = BotInstanceService(db)
    instance = service.get_bot_instance("bot_policy")
    return resolve_effective_bot_policy(
        instance=instance, broker_environment="demo",
        risk_params=BotInstanceService.get_risk_profile_preset(instance.risk_level),
        monitor_interval_seconds=10,
    )


def test_a_broker_universe_bot_resolves_with_no_symbol_list(tmp_path):
    from app.runner.bot_context import BotRunContext

    policy = _policy_for(tmp_path, symbols=(), universe_mode="BROKER")
    assert policy.universe_mode == "BROKER" and tuple(policy.symbols) == ()
    assert policy.capital_budget == 120.0

    ctx = BotRunContext.from_effective_policy(policy, {"api_key": "x", "api_secret": "y",
                                                       "broker_type": "binance", "environment": "demo"})
    assert ctx.universe_mode == "BROKER" and list(ctx.symbols) == []
    with pytest.raises(ValueError):
        dataclasses.replace(ctx, universe_mode="ALLOWLIST")


def test_an_allowlist_bot_still_requires_its_symbols(tmp_path):
    from app.runner.effective_policy import EffectivePolicyError

    with pytest.raises(EffectivePolicyError) as exc:
        _policy_for(tmp_path, symbols=(), universe_mode="ALLOWLIST")
    assert exc.value.reason_code == "MISSING_SYMBOLS"


def test_a_pre_universe_row_is_treated_as_its_explicit_list(tmp_path):
    policy = _policy_for(tmp_path, symbols=("BTCUSDT",))
    assert policy.universe_mode == "ALLOWLIST" and tuple(policy.symbols) == ("BTCUSDT",)


# ══════════════════════════════════════════════════════════════════════════
# 18. Evidence
# ══════════════════════════════════════════════════════════════════════════


def test_every_refresh_is_recorded_with_ranks_inputs_and_reasons(tmp_path):
    db = temp_db(tmp_path)
    metas, stats = venue_of(8)
    metas.append(meta("THINUSDT", "THIN"))
    stats["THINUSDT"] = st(qv=10.0)
    engine, adapter, clock = engine_for(metas, stats, active_limit=5)
    adapter.budget = RequestBudget(used=42, limit=6000)
    runtime = UniverseRuntime(engine=engine, broker_account_id="brk_universe", bot_instance_id=BOT,
                              db=db, clock=clock)

    res = runtime.resolve(open_symbols=["BTCUSDT"], run_id="run_1", runtime_session_id="rts_1")
    runtime.resolve(open_symbols=["BTCUSDT"])  # not due: no second row

    assert res.snapshot_id and res.snapshot_id.startswith("unv_")
    with db.connect() as conn:
        rows = conn.execute("SELECT * FROM universe_snapshots").fetchall()
        assert len(rows) == 1
        cols = [d[0] for d in conn.execute("SELECT * FROM universe_snapshots").description]
        row = dict(zip(cols, rows[0]))
        members = conn.execute(
            "SELECT symbol, rank, canonical_id, quote_volume_24h, spread_bps, selection_reason "
            "FROM universe_members WHERE snapshot_id=? ORDER BY rank", (res.snapshot_id,)).fetchall()

    assert (row["discovered_count"], row["eligible_count"], row["active_count"]) == (9, 9, 5)
    assert row["universe_mode"] == "BROKER" and row["run_id"] == "run_1"
    assert (row["request_weight_used"], row["request_weight_limit"]) == (42, 6000)
    assert json.loads(row["excluded_by_reason_json"]) == {"LOW_LIQUIDITY": 1, "RANK_BELOW_ACTIVE_LIMIT": 3}
    assert json.loads(row["open_symbols_json"]) == ["BTCUSDT"]
    assert json.loads(row["managed_symbols_json"])[0] == "BTCUSDT"
    assert [m[0] for m in members] == ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]
    assert members[0][1:3] == (1, "BTC/USDT:PERP") and members[0][3] == 1e10
    blob = json.dumps(row).lower()
    assert "api_key" not in blob and "secret" not in blob


def test_losing_an_evidence_row_never_stops_a_cycle():
    class BrokenDB:
        def connect(self):
            raise RuntimeError("disk full")

    metas, stats = venue_of(2)
    engine, _, _ = engine_for(metas, stats)
    snap = engine.refresh(broker_account_id="b")
    assert record_universe_snapshot(BrokenDB(), snap, bot_instance_id=BOT) is None


# ══════════════════════════════════════════════════════════════════════════
# 19. The universe decides candidates only -- never threshold, risk or capital
# ══════════════════════════════════════════════════════════════════════════


def test_the_universe_package_cannot_reach_threshold_risk_execution_or_orders():
    forbidden_modules = ("app.threshold", "app.risk", "app.execution", "app.core.trading_orchestrator",
                         "app.capital", "app.runner")
    forbidden_calls = {"place_order", "new_order", "create_order", "cancel_order", "_signed_request",
                       "close_position", "set_leverage"}
    for path in sorted((BACKEND / "app" / "universe").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith(forbidden_modules), f"{path.name} imports {node.module}"
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith(forbidden_modules), f"{path.name} imports {alias.name}"
            if isinstance(node, ast.Attribute):
                assert node.attr not in forbidden_calls, f"{path.name} touches {node.attr}"


def test_the_universe_uses_only_public_read_requests():
    src = (BACKEND / "app" / "universe" / "adapters.py").read_text(encoding="utf-8")
    assert '"POST"' not in src and '"DELETE"' not in src and "_signed_request" not in src


def test_breadth_is_not_capacity():
    """Candidates can far outnumber position slots; slots are decided downstream."""
    metas, stats = venue_of(40)
    engine, _, _ = engine_for(metas, stats, active_limit=40)
    candidates, managed, _ = resolve_managed(engine.refresh(broker_account_id="b"), ["BTCUSDT"])
    assert len(candidates) == 39 and len(managed) == 40
    src = (BACKEND / "app" / "universe" / "runtime.py").read_text(encoding="utf-8")
    assert "max_open_positions" not in src and "capital_budget" not in src
