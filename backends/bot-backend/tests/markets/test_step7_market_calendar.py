"""Step 7.2 -- venue-local calendars: sessions (overnight and across weekends), holidays, early closes,
maintenance, daylight-saving transitions, expected bars that never cover a closed period, and the 24/7
crypto calendar that keeps the existing behaviour."""
from __future__ import annotations

import random

import pytest

import _step7 as F
from app.markets.calendar import (CalendarCoverageError, CalendarDefinitionError, MarketCalendar, local_ms, utc_ms)
from app.markets.registry import default_registry

CHI, NY = "America/Chicago", "America/New_York"
H = 3_600_000


@pytest.fixture
def fut():
    return F.calendar(F.FUTURES_FIXTURE)


@pytest.fixture
def cash():
    return F.calendar(F.CASH_FIXTURE)


# ── sessions ───────────────────────────────────────────────────────────────

def test_an_overnight_session_spans_midnight_and_belongs_to_the_closing_date(fut):
    ts = local_ms(CHI, 2026, 10, 19, 20, 0)                 # Monday evening
    s = fut.session_for(ts)
    assert fut.is_open(ts) and s is not None
    assert s.trading_date.isoformat() == "2026-10-20"       # the date it closes
    assert s.open_ms == local_ms(CHI, 2026, 10, 19, 17, 0) and s.close_ms == local_ms(CHI, 2026, 10, 20, 16, 0)
    assert fut.is_open(local_ms(CHI, 2026, 10, 20, 0, 30))  # after midnight, same session
    assert fut.session_for(local_ms(CHI, 2026, 10, 20, 0, 30)) == s


def test_the_daily_break_and_the_weekend_are_closed(fut):
    assert not fut.is_open(local_ms(CHI, 2026, 10, 19, 16, 30))     # daily break
    assert fut.session_for(local_ms(CHI, 2026, 10, 19, 16, 30)) is None
    assert not fut.is_open(local_ms(CHI, 2026, 10, 17, 12, 0))      # Saturday
    assert not fut.is_open(local_ms(CHI, 2026, 10, 18, 16, 59))     # Sunday before the open
    assert fut.is_open(local_ms(CHI, 2026, 10, 18, 17, 0))          # the open instant is open
    assert not fut.is_open(local_ms(CHI, 2026, 10, 23, 16, 0))      # the close instant is closed (half-open)


def test_next_open_and_next_close(fut):
    assert fut.next_open(local_ms(CHI, 2026, 10, 17, 12, 0)) == local_ms(CHI, 2026, 10, 18, 17, 0)
    assert fut.next_open(local_ms(CHI, 2026, 10, 19, 16, 30)) == local_ms(CHI, 2026, 10, 19, 17, 0)
    inside = local_ms(CHI, 2026, 10, 19, 20, 0)
    assert fut.next_open(inside) == inside
    # the next close inside a Monday-night session is the Tuesday maintenance break
    assert fut.next_close(inside) == local_ms(CHI, 2026, 10, 20, 3, 0)
    assert fut.next_close(local_ms(CHI, 2026, 10, 17, 12, 0)) is None


def test_a_holiday_removes_the_session_with_that_trading_date(fut):
    # the session that would close on Friday 2026-12-25 does not exist
    assert not fut.is_open(local_ms(CHI, 2026, 12, 24, 20, 0))
    assert fut.is_open(local_ms(CHI, 2026, 12, 24, 10, 0))          # Wed -> Thu session is normal
    assert fut.next_open(local_ms(CHI, 2026, 12, 24, 20, 0)) == local_ms(CHI, 2026, 12, 27, 17, 0)
    dates = [s.trading_date.isoformat() for s in fut.sessions_between(local_ms(CHI, 2026, 12, 20), local_ms(CHI, 2026, 12, 27))]
    assert "2026-12-25" not in dates and dates == ["2026-12-21", "2026-12-22", "2026-12-23", "2026-12-24"]


def test_an_early_close_shortens_only_that_session(fut):
    s = fut.session_for(local_ms(CHI, 2026, 11, 27, 9, 0))
    assert s.early_close and s.close_ms == local_ms(CHI, 2026, 11, 27, 12, 15)
    assert fut.is_open(local_ms(CHI, 2026, 11, 27, 12, 14))
    assert not fut.is_open(local_ms(CHI, 2026, 11, 27, 12, 15))
    assert fut.next_close(local_ms(CHI, 2026, 11, 26, 20, 0)) == local_ms(CHI, 2026, 11, 27, 12, 15)
    normal = fut.session_for(local_ms(CHI, 2026, 11, 20, 9, 0))
    assert not normal.early_close and normal.close_ms == local_ms(CHI, 2026, 11, 20, 16, 0)


def test_a_maintenance_window_closes_the_market_inside_a_session(fut):
    assert fut.is_open(local_ms(CHI, 2026, 10, 20, 2, 59))
    assert not fut.is_open(local_ms(CHI, 2026, 10, 20, 3, 5))
    assert fut.session_for(local_ms(CHI, 2026, 10, 20, 3, 5)) is not None     # still the same session
    assert fut.is_open(local_ms(CHI, 2026, 10, 20, 3, 15))
    assert fut.next_open(local_ms(CHI, 2026, 10, 20, 3, 5)) == local_ms(CHI, 2026, 10, 20, 3, 15)
    assert fut.is_open(local_ms(CHI, 2026, 10, 21, 3, 5))                     # Wednesday has no window


# ── daylight saving ────────────────────────────────────────────────────────

def test_session_opens_move_in_utc_across_dst_transitions(fut):
    # 17:00 Chicago is 23:00 UTC in winter and 22:00 UTC in summer
    assert fut.session_for(utc_ms(2026, 3, 1, 23, 30)).open_ms == utc_ms(2026, 3, 1, 23, 0)
    assert not fut.is_open(utc_ms(2026, 3, 1, 22, 30))
    assert fut.session_for(utc_ms(2026, 3, 8, 22, 30)).open_ms == utc_ms(2026, 3, 8, 22, 0)   # after spring forward
    assert fut.session_for(utc_ms(2026, 10, 25, 22, 30)).open_ms == utc_ms(2026, 10, 25, 22, 0)
    assert not fut.is_open(utc_ms(2026, 11, 1, 22, 30))                                         # after fall back
    assert fut.session_for(utc_ms(2026, 11, 1, 23, 30)).open_ms == utc_ms(2026, 11, 1, 23, 0)


def test_a_session_spanning_a_transition_has_its_real_length():
    cal = MarketCalendar.from_plain({
        "calendar_id": "DST_SPAN", "kind": "SESSIONS", "timezone": CHI, "verification_status": "VERIFIED",
        "source": "test", "weekly_sessions": [{"open_weekday": 5, "open": "22:00", "close_weekday": 6, "close": "06:00"}]})
    spring = cal.session_for(local_ms(CHI, 2026, 3, 8, 1, 0))
    fall = cal.session_for(local_ms(CHI, 2026, 10, 31, 23, 0))
    normal = cal.session_for(local_ms(CHI, 2026, 6, 6, 23, 0))
    assert (spring.close_ms - spring.open_ms) == 7 * H
    assert (fall.close_ms - fall.open_ms) == 9 * H
    assert (normal.close_ms - normal.open_ms) == 8 * H


def test_a_local_time_inside_the_dst_gap_is_a_definition_error():
    cal = MarketCalendar.from_plain({
        "calendar_id": "GAP", "kind": "SESSIONS", "timezone": CHI, "verification_status": "VERIFIED", "source": "test",
        "weekly_sessions": [{"open_weekday": 6, "open": "02:30", "close_weekday": 6, "close": "10:00"}]})
    assert cal.is_open(local_ms(CHI, 2026, 3, 1, 3, 0))          # an ordinary Sunday
    with pytest.raises(CalendarDefinitionError):
        cal.is_open(local_ms(CHI, 2026, 3, 8, 4, 0))             # 02:30 does not exist on 2026-03-08


# ── coverage and definitions ───────────────────────────────────────────────

def test_a_question_outside_the_holiday_data_is_an_error_not_an_assumption(fut):
    with pytest.raises(CalendarCoverageError):
        fut.is_open(local_ms(CHI, 2028, 1, 4, 10, 0))
    with pytest.raises(CalendarCoverageError):
        fut.expected_bars(local_ms(CHI, 2027, 12, 20), local_ms(CHI, 2028, 1, 10), "1d")


@pytest.mark.parametrize("over", [
    {"kind": "CONTINUOUS"},                                     # continuous with sessions
    {"timezone": "Mars/Olympus"},
    {"source": ""},
    {"verification_status": "PROBABLY"},
    {"weekly_sessions": []},
    {"maintenance_windows": [{"weekdays": [1], "start": "04:00", "end": "03:00"}]},
    {"weekly_sessions": [{"open_weekday": 7, "open": "17:00", "close_weekday": 0, "close": "16:00"}]},
    {"weekly_sessions": [{"open_weekday": 1, "open": "17:00", "close_weekday": 1, "close": "16:00"}]},
])
def test_invalid_calendar_definitions_are_refused(over):
    with pytest.raises(CalendarDefinitionError):
        F.calendar(F.FUTURES_FIXTURE, **over)


# ── expected bars: never a bar for a closed period ─────────────────────────

def test_daily_bars_are_one_per_session_and_none_for_a_holiday(fut):
    week = fut.expected_bars(local_ms(CHI, 2026, 10, 18), local_ms(CHI, 2026, 10, 25), "1d")
    assert week == [local_ms(CHI, 2026, 10, d, 17, 0) for d in (18, 19, 20, 21, 22)]
    christmas = fut.expected_bars(local_ms(CHI, 2026, 12, 20), local_ms(CHI, 2026, 12, 27), "1d")
    assert len(christmas) == 4 and local_ms(CHI, 2026, 12, 24, 17, 0) not in christmas


def test_intraday_bars_cover_open_time_only(cash):
    monday = cash.expected_bars(utc_ms(2026, 10, 19, 0), utc_ms(2026, 10, 20, 0), "1h")
    # 09:30-16:00 New York (UTC-4): hourly bars 13:00 (partially open) to 19:00 UTC
    assert monday == [utc_ms(2026, 10, 19, h) for h in range(13, 20)]
    assert cash.expected_bars(utc_ms(2026, 10, 17), utc_ms(2026, 10, 19), "1h") == []        # weekend
    assert cash.expected_bars(utc_ms(2026, 7, 3), utc_ms(2026, 7, 4), "15m") == []          # holiday
    winter = cash.expected_bars(utc_ms(2026, 12, 7, 0), utc_ms(2026, 12, 8, 0), "1h")       # UTC-5
    assert winter[0] == utc_ms(2026, 12, 7, 14) and winter[-1] == utc_ms(2026, 12, 7, 20)


def test_no_bar_is_expected_inside_maintenance_or_the_daily_break(fut):
    bars = fut.expected_bars(local_ms(CHI, 2026, 10, 20, 2, 0), local_ms(CHI, 2026, 10, 20, 4, 0), "15m")
    assert local_ms(CHI, 2026, 10, 20, 3, 0) not in bars
    assert local_ms(CHI, 2026, 10, 20, 2, 45) in bars and local_ms(CHI, 2026, 10, 20, 3, 15) in bars
    brk = fut.expected_bars(local_ms(CHI, 2026, 10, 19, 16, 0), local_ms(CHI, 2026, 10, 19, 17, 0), "15m")
    assert brk == []


def test_forward_filled_closure_bars_are_flagged_never_accepted(fut):
    start, end = local_ms(CHI, 2026, 10, 16, 0, 0), local_ms(CHI, 2026, 10, 20, 0, 0)
    expected = fut.expected_bars(start, end, "1h")
    # a feed that forward-filled the weekend: one bar every hour, open or not
    filled = list(range(start, end, H))
    report = fut.classify_bars(filled, start, end, "1h")
    weekend = [t for t in filled if not fut.open_intervals(t, t + H)]
    assert weekend and report["unexpected"] == weekend and report["missing"] == []
    assert set(report["unexpected"]).isdisjoint(expected)
    # and a feed that missed one real bar is told which
    gappy = [t for t in expected if t != expected[3]]
    assert fut.classify_bars(gappy, start, end, "1h") == {"missing": [expected[3]], "unexpected": []}


def test_unknown_timeframes_are_refused(fut):
    with pytest.raises(ValueError):
        fut.expected_bars(0, 10, "2d")


# ── crypto regression: always open ─────────────────────────────────────────

def test_the_shipped_crypto_calendar_is_always_open_and_daily_bars_are_utc_days():
    crypto = default_registry().calendar("CRYPTO_24_7")
    rng = random.Random(7)
    for _ in range(500):
        ts = rng.randrange(utc_ms(2020, 1, 1), utc_ms(2030, 1, 1))
        assert crypto.is_open(ts) and crypto.next_open(ts) == ts and crypto.next_close(ts) is None
    for ts in (utc_ms(2026, 12, 25, 12), utc_ms(2026, 10, 17, 12), utc_ms(2026, 3, 8, 8), utc_ms(2026, 11, 1, 7)):
        assert crypto.is_open(ts)
    days = crypto.expected_bars(utc_ms(2026, 10, 1), utc_ms(2026, 11, 1), "1d")
    assert days == [utc_ms(2026, 10, d) for d in range(1, 32)]
    assert len(crypto.expected_bars(utc_ms(2026, 10, 17), utc_ms(2026, 10, 18), "1h")) == 24
    assert crypto.session_for(utc_ms(2026, 10, 17, 13)).open_ms == utc_ms(2026, 10, 17)


def test_the_crypto_calendar_agrees_with_the_existing_session_guard():
    from app.symbols.market_hours import ForexSessionGuard
    from app.models.unified_trading import AssetClass

    crypto = default_registry().calendar("CRYPTO_24_7")
    for ts in (utc_ms(2026, 10, 17, 12), utc_ms(2026, 12, 25), utc_ms(2026, 1, 1)):
        assert crypto.is_open(ts) is ForexSessionGuard.is_market_open(AssetClass.CRYPTO_PERP, ts) is True
