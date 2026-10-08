"""Step 1.0b -- the incremental income ledger behind the daily-loss latch and
the period drawdown baselines.

The venue is asked only for what the local ledger does not cover (plus the
late-record overlap); replays are idempotent; an incomplete window leaves the
cursor where it was so the caller fails closed exactly as before.
"""
import pytest
from shared_lib.persistence.db import DB

from app.execution import production_schema
from app.trading_intelligence.integration import income_ledger as ledger

H = 3_600_000
DAY = 86_400_000
ACCOUNT = "acct-ledger"


class Venue:
    """Income history fake: records are visible once ``published_at`` has passed."""

    def __init__(self, records=(), *, now=0):
        self.records = [dict(r) for r in records]
        self.now = now
        self.pages = 0
        self.windows = []
        self.fail_windows = set()

    def income_history(self, *, start_time_ms, end_time_ms, limit=1000):
        self.pages += 1
        self.windows.append((start_time_ms, end_time_ms))
        if any(a <= start_time_ms <= b for a, b in self.fail_windows):
            raise TimeoutError("income page timed out")
        visible = [r for r in self.records if start_time_ms <= r["time"] <= end_time_ms
                   and r.get("published_at", r["time"]) <= self.now]
        return [{k: v for k, v in r.items() if k != "published_at"} for r in visible][:limit]


def rec(tran, t, income, kind="REALIZED_PNL", **extra):
    return {"tranId": tran, "time": t, "income": str(income), "incomeType": kind, "asset": "USDT",
            "symbol": "ADAUSDT", "tradeId": extra.pop("tradeId", ""), **extra}


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "ledger.db"))
    production_schema.forget()
    yield database
    production_schema.forget()


def total(rows):
    return round(sum(float(r["income"]) for r in rows), 8)


def test_first_use_backfills_the_whole_period_once_then_reads_only_the_overlap(db):
    now = 30 * DAY
    venue = Venue([rec(1, 2 * DAY, "-5"), rec(2, 20 * DAY, "3"), rec(3, now - H, "1.5", "FUNDING_FEE")], now=now)
    first = ledger.ensure_covered(db, venue, ACCOUNT, 0, now)
    assert first["pages"] == 5 and first["refreshed"]           # 30 days = five seven-day windows
    assert total(ledger.rows(db, ACCOUNT, 0, now)) == -0.5
    assert total(ledger.rows(db, ACCOUNT, now - DAY, now)) == 1.5  # today's funding fee only
    venue.pages = 0
    later = now + 30_000
    again = ledger.ensure_covered(db, venue, ACCOUNT, 0, later, force=True)
    assert again["pages"] == 1                                 # one overlap page, not the month
    assert venue.windows[-1] == (now - ledger.OVERLAP_MS + 1, later)
    assert again["covered_from"] == 0 and again["covered_to"] == later


def test_unforced_refreshes_wait_for_the_interval(db):
    now = 10 * DAY
    venue = Venue([rec(1, now - H, "-2")], now=now)
    ledger.ensure_covered(db, venue, ACCOUNT, 0, now)
    venue.pages = 0
    assert ledger.ensure_covered(db, venue, ACCOUNT, 0, now + 60_000)["refreshed"] is False
    assert venue.pages == 0
    assert ledger.ensure_covered(db, venue, ACCOUNT, 0, now + ledger.REFRESH_INTERVAL_MS)["refreshed"] is True
    assert venue.pages == 1


def test_a_wallet_change_forces_a_refresh_inside_the_interval(db):
    now = 10 * DAY
    venue = Venue([rec(1, now - H, "-2")], now=now)
    ledger.ensure_covered(db, venue, ACCOUNT, 0, now, wallet="1000.0")
    venue.records.append(rec(2, now + 10_000, "-3"))
    venue.now = now + 30_000
    assert ledger.ensure_covered(db, venue, ACCOUNT, 0, now + 30_000, wallet="1000.0")["refreshed"] is False
    out = ledger.ensure_covered(db, venue, ACCOUNT, 0, now + 30_000, wallet="995.0")   # the wallet moved
    assert out["refreshed"] is True and total(ledger.rows(db, ACCOUNT, 0, now + 30_000)) == -5
    assert ledger.cursor(db, ACCOUNT)["wallet"] == "995.0"


def test_a_late_record_with_an_old_timestamp_is_picked_up_by_the_overlap(db):
    now = 10 * DAY
    venue = Venue([rec(1, now - 2 * H, "-4", published_at=now + 60_000)], now=now)   # not yet visible
    ledger.ensure_covered(db, venue, ACCOUNT, 0, now)
    assert ledger.rows(db, ACCOUNT, 0, now) == []
    venue.now = now + 60_000
    ledger.ensure_covered(db, venue, ACCOUNT, 0, now + 60_000, force=True)
    assert total(ledger.rows(db, ACCOUNT, 0, now + 60_000)) == -4


def test_replaying_a_window_is_idempotent_and_duplicates_are_ignored(db):
    now = 5 * DAY
    venue = Venue([rec(7, now - H, "-1"), rec(7, now - H, "-1")], now=now)    # the venue repeats itself
    ledger.ensure_covered(db, venue, ACCOUNT, 0, now)
    for _ in range(3):
        ledger.ensure_covered(db, venue, ACCOUNT, 0, now + 1, force=True)
    assert total(ledger.rows(db, ACCOUNT, 0, now + 1)) == -1
    assert len(ledger.rows(db, ACCOUNT, 0, now + 1)) == 1


def test_a_restart_resumes_from_the_persisted_cursor(db):
    now = 5 * DAY
    venue = Venue([rec(1, now - H, "-1")], now=now)
    ledger.ensure_covered(db, venue, ACCOUNT, 0, now)
    restarted = DB(db.path)
    production_schema.forget()                                   # a new process knows nothing
    venue.pages = 0
    cursor = ledger.cursor(restarted, ACCOUNT)
    assert cursor and cursor["covered_to"] == now
    out = ledger.ensure_covered(restarted, venue, ACCOUNT, 0, now + 30_000, force=True)
    assert out["pages"] == 1 and venue.windows[-1][0] == now - ledger.OVERLAP_MS + 1
    assert total(ledger.rows(restarted, ACCOUNT, 0, now + 30_000)) == -1


def test_an_earlier_period_is_backfilled_without_re_reading_what_is_covered(db):
    now = 40 * DAY
    venue = Venue([rec(1, 5 * DAY, "-9"), rec(2, 39 * DAY, "2")], now=now)
    ledger.ensure_covered(db, venue, ACCOUNT, 35 * DAY, now)       # the day's ledger
    venue.pages, seen = 0, len(venue.windows)
    out = ledger.ensure_covered(db, venue, ACCOUNT, 0, now)        # the month is needed too
    backfill = venue.windows[seen:]
    assert out["covered_from"] == 0 and backfill
    assert max(end for _, end in backfill) == 35 * DAY - 1        # only the uncovered part was read
    assert all(start >= 0 and end < 35 * DAY for start, end in backfill)
    assert total(ledger.rows(db, ACCOUNT, 0, now)) == -7


def test_a_missing_page_moves_nothing_and_fails_closed(db):
    now = 20 * DAY
    venue = Venue([rec(1, 2 * DAY, "-5")], now=now)
    ledger.ensure_covered(db, venue, ACCOUNT, 0, now)
    before = ledger.cursor(db, ACCOUNT)
    venue.fail_windows.add((now - ledger.OVERLAP_MS + 1, now + 60_000))
    with pytest.raises(TimeoutError):
        ledger.ensure_covered(db, venue, ACCOUNT, 0, now + 60_000, force=True)
    assert ledger.cursor(db, ACCOUNT) == before                    # the next cycle asks again
    venue.fail_windows.clear()
    assert ledger.ensure_covered(db, venue, ACCOUNT, 0, now + 60_000, force=True)["covered_to"] == now + 60_000


def test_a_full_page_is_bisected_never_trusted_as_complete(db):
    now = 2 * DAY
    venue = Venue([rec(i, now - 10 * H + i * 10, "-0.001") for i in range(1000)], now=now)
    ledger.ensure_covered(db, venue, ACCOUNT, 0, now)
    assert len(ledger.rows(db, ACCOUNT, 0, now)) == 1000 and venue.pages > 1


def test_malformed_pages_are_rejected_as_before(db):
    now = 2 * DAY

    class Bad(Venue):
        def income_history(self, **k):
            return {"not": "a list"}
    with pytest.raises(ValueError, match="BROKER_INCOME_HISTORY_UNAVAILABLE"):
        ledger.ensure_covered(db, Bad(now=now), ACCOUNT, 0, now)
    venue = Venue([{**rec(1, now - H, "1"), "asset": "BNB"}], now=now)
    with pytest.raises(ValueError, match="BROKER_INCOME_HISTORY_INVALID"):
        ledger.ensure_covered(db, venue, ACCOUNT, 0, now)
    assert ledger.cursor(db, ACCOUNT) is None


def test_records_without_a_transaction_id_get_a_deterministic_key():
    a = {"incomeType": "FUNDING_FEE", "time": 5, "tradeId": "", "symbol": "ADAUSDT", "income": "0.1"}
    assert ledger.record_id(a) == ledger.record_id(dict(a)) and ledger.record_id({**a, "tranId": 9}) == "9"
