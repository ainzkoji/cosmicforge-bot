from app.execution.executor import _interval_to_ms, _kline_staleness


def _kline(open_ms: int, close_ms: int | None = None):
    if close_ms is None:
        return [open_ms, "1", "1", "1", "1", "1"]
    return [open_ms, "1", "1", "1", "1", "1", close_ms]


def test_fresh_latest_candle_does_not_block_entries():
    now_ms = 1_000_000
    verdict = _kline_staleness(
        [_kline(now_ms - 60_000, now_ms - 20_000)],
        interval="1m",
        now_ms=now_ms,
        buffer_ms=180_000,
    )

    assert verdict["ok"] is True
    assert verdict["age_ms"] == 20_000


def test_stale_latest_candle_blocks_entries():
    now_ms = 1_000_000
    verdict = _kline_staleness(
        [_kline(now_ms - 600_000, now_ms - 500_000)],
        interval="1m",
        now_ms=now_ms,
        buffer_ms=180_000,
    )

    assert verdict["ok"] is False
    assert verdict["reason"] == "stale_data"


def test_stale_threshold_is_interval_aware():
    now_ms = 10_000_000
    ten_minutes_old = now_ms - (10 * 60_000)

    one_minute = _kline_staleness(
        [_kline(now_ms - 11 * 60_000, ten_minutes_old)],
        interval="1m",
        now_ms=now_ms,
        buffer_ms=180_000,
    )
    fifteen_minutes = _kline_staleness(
        [_kline(now_ms - 15 * 60_000, ten_minutes_old)],
        interval="15m",
        now_ms=now_ms,
        buffer_ms=180_000,
    )

    assert one_minute["ok"] is False
    assert fifteen_minutes["ok"] is True
    assert fifteen_minutes["threshold_ms"] == _interval_to_ms("15m") + 180_000


def test_open_time_falls_back_to_close_time_when_close_missing():
    now_ms = 1_000_000
    open_ms = now_ms - 60_000
    verdict = _kline_staleness(
        [_kline(open_ms)],
        interval="1m",
        now_ms=now_ms,
        buffer_ms=180_000,
    )

    assert verdict["ok"] is True
    assert verdict["open_time_ms"] == open_ms
    assert verdict["close_time_ms"] == open_ms + 60_000 - 1


def test_freshness_check_is_stateless_and_does_not_reuse_stale_data():
    now_ms = 1_000_000
    stale = _kline_staleness(
        [_kline(now_ms - 600_000, now_ms - 500_000)],
        interval="1m",
        now_ms=now_ms,
        buffer_ms=180_000,
    )
    fresh = _kline_staleness(
        [_kline(now_ms - 60_000, now_ms - 10_000)],
        interval="1m",
        now_ms=now_ms,
        buffer_ms=180_000,
    )

    assert stale["ok"] is False
    assert fresh["ok"] is True
