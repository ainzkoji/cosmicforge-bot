"""Phase 13 §13.1–§13.4 — the historical clock, and no future leakage.

Look-ahead is the bug that makes a backtest confidently wrong rather than
merely inaccurate. These tests exist to make it impossible to introduce
quietly: they assert the cut at candle boundaries, at the exact millisecond of
a close, and across timeframes, including the higher-timeframe rule that a 15m
decision may never consult an hourly candle that has not closed yet.
"""
from __future__ import annotations

import pytest

from app.replay.historical_provider import (
    TIMEFRAME_MS,
    HistoricalClock,
    HistoricalMarketDataProvider,
    ReplayDataError,
    ReplayMarketClient,
)

M15 = TIMEFRAME_MS["15m"]
H1 = TIMEFRAME_MS["1h"]

#: A round anchor so boundaries are easy to reason about: 2023-11-14T22:13:20Z
#: is not, so the series starts on an exact hour instead.
ANCHOR = 1_700_000_000_000 - (1_700_000_000_000 % H1)


def candles(count: int, step_ms: int, *, start_ms: int = ANCHOR, base: float = 100.0):
    """`count` consecutive candles, close price rising by 1 each bar."""
    rows = []
    for i in range(count):
        open_time = start_ms + i * step_ms
        close_time = open_time + step_ms - 1
        close = base + i
        rows.append([
            open_time, f"{close - 1:.2f}", f"{close + 0.5:.2f}", f"{close - 1.5:.2f}",
            f"{close:.2f}", "1000", close_time, "0", 0, "0", "0", "0",
        ])
    return rows


@pytest.fixture
def provider():
    clock = HistoricalClock(now_ms=ANCHOR)
    return HistoricalMarketDataProvider(
        {"BTCUSDT": {"15m": candles(240, M15), "1h": candles(60, H1)}},
        clock,
    )


# ── The clock ───────────────────────────────────────────────────────────────


def test_the_clock_only_moves_forward():
    clock = HistoricalClock(now_ms=1_000)
    clock.advance_to(2_000)
    assert clock.now_ms == 2_000

    with pytest.raises(ReplayDataError, match="cannot go backwards"):
        clock.advance_to(1_999)


def test_advancing_to_the_same_moment_is_allowed():
    clock = HistoricalClock(now_ms=1_000)
    clock.advance_to(1_000)
    assert clock.now_ms == 1_000


# ── The cut ─────────────────────────────────────────────────────────────────


def test_nothing_is_visible_before_the_first_candle_closes(provider):
    assert provider.closed_candles("BTCUSDT", "15m") == ()
    assert provider.build_snapshot("BTCUSDT", "15m") is None


def test_a_candle_becomes_visible_at_its_close_and_not_a_millisecond_before(provider):
    first_close = ANCHOR + M15 - 1

    provider.clock.advance_to(first_close - 1)
    assert provider.closed_candles("BTCUSDT", "15m") == ()

    provider.clock.advance_to(first_close)
    visible = provider.closed_candles("BTCUSDT", "15m")
    assert len(visible) == 1
    assert visible[-1][6] == first_close


def test_the_forming_candle_is_never_visible(provider):
    """Mid-bar, the bar being formed must not be readable."""
    provider.clock.advance_to(ANCHOR + M15 - 1)          # bar 0 closed
    provider.clock.advance_to(ANCHOR + M15 + M15 // 2)   # halfway through bar 1

    visible = provider.closed_candles("BTCUSDT", "15m")
    assert len(visible) == 1, "only the closed bar may be seen"
    assert visible[-1][6] == ANCHOR + M15 - 1


@pytest.mark.parametrize("bars", [1, 2, 17, 96])
def test_exactly_the_closed_bars_are_visible(provider, bars):
    provider.clock.advance_to(ANCHOR + bars * M15 - 1)
    assert len(provider.closed_candles("BTCUSDT", "15m")) == bars


def test_the_reference_price_is_the_last_closed_close_not_a_mid_bar_price(provider):
    provider.clock.advance_to(ANCHOR + 3 * M15 - 1)
    assert provider.reference_price("BTCUSDT", "15m") == pytest.approx(102.0)

    # Move most of the way through the next bar: the price must not budge.
    provider.clock.advance_to(ANCHOR + 4 * M15 - 2)
    assert provider.reference_price("BTCUSDT", "15m") == pytest.approx(102.0)


def test_a_limit_takes_the_most_recent_bars_not_the_first(provider):
    provider.clock.advance_to(ANCHOR + 50 * M15 - 1)
    visible = provider.closed_candles("BTCUSDT", "15m", limit=5)
    assert len(visible) == 5
    assert visible[-1][6] == ANCHOR + 50 * M15 - 1


# ── §13.4 higher-timeframe alignment ────────────────────────────────────────


def test_an_hourly_candle_that_has_not_closed_is_not_in_the_snapshot(provider):
    """A 15m decision at :45 may not see the 1h candle that closes at the hour."""
    # Third 15m bar of the first hour closes at ANCHOR + 45m - 1; the hour
    # closes at ANCHOR + 60m - 1, fifteen minutes later.
    provider.clock.advance_to(ANCHOR + 3 * M15 - 1)

    snapshot = provider.build_snapshot("BTCUSDT", "15m", higher_timeframe="1h")
    assert snapshot is not None
    assert snapshot.higher_timeframe_candles == ()
    assert snapshot.htf_is_timestamp_aligned()


def test_the_hourly_candle_appears_once_the_hour_has_closed(provider):
    provider.clock.advance_to(ANCHOR + 4 * M15 - 1)  # exactly the hour close

    snapshot = provider.build_snapshot("BTCUSDT", "15m", higher_timeframe="1h")
    assert len(snapshot.higher_timeframe_candles) == 1
    assert snapshot.higher_timeframe_closed_candle_time == ANCHOR + H1 - 1
    assert snapshot.htf_is_timestamp_aligned()


@pytest.mark.parametrize("bar", range(1, 13))
def test_htf_never_leads_the_strategy_candle_at_any_boundary(provider, bar):
    """Swept across three hours of 15m bars, including every hour boundary."""
    provider.clock.advance_to(ANCHOR + bar * M15 - 1)
    snapshot = provider.build_snapshot("BTCUSDT", "15m", higher_timeframe="1h")

    assert snapshot.htf_is_timestamp_aligned()
    if snapshot.higher_timeframe_candles:
        assert (
            snapshot.higher_timeframe_closed_candle_time
            <= snapshot.latest_closed_candle_time
        )
        # And the count is exactly the number of whole hours that have closed.
        assert len(snapshot.higher_timeframe_candles) == (bar * M15) // H1


def test_the_htf_cut_is_the_strategy_candle_not_the_wall_clock():
    """A clock ahead of the strategy candle must not leak an extra HTF bar.

    The provider is asked for a limited 15m window, so "now" can legitimately
    be later than the newest bar the snapshot carries. The HTF cut must follow
    the *snapshot*, not the clock.
    """
    clock = HistoricalClock(now_ms=ANCHOR)
    provider = HistoricalMarketDataProvider(
        {"BTCUSDT": {"15m": candles(8, M15), "1h": candles(2, H1)}}, clock,
    )
    # Clock at the end of hour two; snapshot restricted to one 15m bar, which
    # is the last bar of hour two.
    clock.advance_to(ANCHOR + 2 * H1 - 1)
    snapshot = provider.build_snapshot(
        "BTCUSDT", "15m", limit=1, higher_timeframe="1h",
    )
    assert snapshot.latest_closed_candle_time == ANCHOR + 8 * M15 - 1
    assert snapshot.higher_timeframe_closed_candle_time <= snapshot.latest_closed_candle_time


# ── Data quality: refuse rather than silently repair ────────────────────────


def test_duplicate_candles_are_refused():
    rows = candles(3, M15)
    with pytest.raises(ReplayDataError, match="duplicate candle"):
        HistoricalMarketDataProvider(
            {"BTCUSDT": {"15m": rows + [rows[-1]]}}, HistoricalClock(now_ms=ANCHOR),
        )


def test_out_of_order_candles_are_refused():
    rows = candles(4, M15)
    scrambled = [rows[0], rows[2], rows[1], rows[3]]
    with pytest.raises(ReplayDataError, match="not ascending"):
        HistoricalMarketDataProvider(
            {"BTCUSDT": {"15m": scrambled}}, HistoricalClock(now_ms=ANCHOR),
        )


def test_an_empty_series_is_refused():
    with pytest.raises(ReplayDataError, match="no candles"):
        HistoricalMarketDataProvider(
            {"BTCUSDT": {"15m": []}}, HistoricalClock(now_ms=ANCHOR),
        )


def test_an_unknown_timeframe_names_what_is_available(provider):
    with pytest.raises(ReplayDataError, match="have \\['15m', '1h'\\]"):
        provider.closed_candles("BTCUSDT", "4h")


# ── Stepping ────────────────────────────────────────────────────────────────


def test_stepping_visits_every_closed_candle_once(provider):
    seen = list(provider.step("BTCUSDT", "15m", end_ms=ANCHOR + 10 * M15))
    assert len(seen) == 10
    assert seen == sorted(seen)
    assert provider.clock.now_ms == seen[-1]


def test_at_each_step_the_snapshot_ends_on_that_candle(provider):
    for close_time in provider.step("BTCUSDT", "15m", end_ms=ANCHOR + 6 * M15):
        snapshot = provider.build_snapshot("BTCUSDT", "15m", higher_timeframe="1h")
        assert snapshot.latest_closed_candle_time == close_time
        assert snapshot.htf_is_timestamp_aligned()


# ── The client the strategy stack sees ──────────────────────────────────────


def test_the_replay_client_serves_only_the_visible_window(provider):
    provider.clock.advance_to(ANCHOR + 5 * M15 - 1)
    client = ReplayMarketClient(provider, default_timeframe="15m")

    assert len(client.klines("BTCUSDT", "15m", limit=100)) == 5
    assert client.last_price("BTCUSDT") == pytest.approx(104.0)
    assert client.server_time() == provider.clock.now_ms


def test_the_replay_client_refuses_to_reach_an_exchange(provider):
    client = ReplayMarketClient(provider, default_timeframe="15m")
    with pytest.raises(AttributeError, match="may not reach a live exchange"):
        client.place_order


def test_the_snapshot_is_labelled_as_replay(provider):
    provider.clock.advance_to(ANCHOR + 2 * M15 - 1)
    snapshot = provider.build_snapshot("BTCUSDT", "15m")
    assert snapshot.source == "REPLAY"
    assert snapshot.source_environment == "REPLAY"


def test_the_snapshot_is_the_production_contract(provider):
    """§13.1 — the strategy must not be able to tell this is a replay."""
    from app.runner.market_snapshot import MarketSnapshot

    provider.clock.advance_to(ANCHOR + 30 * M15 - 1)
    snapshot = provider.build_snapshot("BTCUSDT", "15m", higher_timeframe="1h")

    assert isinstance(snapshot, MarketSnapshot)
    assert snapshot.market_snapshot_id.startswith("ms_")
    assert snapshot.data_hash  # deterministic fingerprint of the pinned data
    assert snapshot.reference_price == pytest.approx(129.0)
