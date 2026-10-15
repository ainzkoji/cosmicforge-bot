"""Steps 3.1 and 3.12: the daily bar feed, the market-data provider that follows the exchange day by day.

No test here touches the network. The feed is driven by a source that answers from a synthetic panel, and the
real archive fetcher is driven by canned responses in the shape the research dataset's own parser reads.
"""
from __future__ import annotations

import ast
import hashlib
import io
import json
import urllib.parse
import zipfile
from pathlib import Path

import numpy as np
import pytest

import _step2
import _step3 as S
from app.market_data import binance_archive as BA
from app.market_data import daily_dataset as DD
from app.market_data import daily_feed as F
from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend import targets as T
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import driver as D
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline import performance as P
from app.trading_intelligence.pipeline.accounts import ModelAccounts
from app.trading_intelligence.pipeline.market_data import FrozenPanelBars
from app.trading_intelligence.research.evaluator import simulator as SIM

DAY_MS = 86_400_000
EQUITY = spec.INTERPRETATION["initial_equity_usdt"]
START = DT.day_of("2020-09-01")
FEED_TABLES = ("cati_feed_daily_bars", "cati_feed_funding", "cati_feed_metadata", "cati_feed_rejections",
               "cati_feed_revisions", "cati_feed_symbols", "cati_feed_updates", "cati_feed_sealed_days", "cati_feed_state")


class Clock:
    def __init__(self, now: int = 0) -> None:
        self.now = int(now)

    def __call__(self) -> int:
        return self.now

    def after_close(self, day: int, ms: int = 60_000) -> "Clock":
        self.now = F.close_ms(day) + ms
        return self


class PanelSource:
    """A source that answers from a panel: its bars and funding as raw rows, exactly as an archive file would
    hold them. ``fail`` (symbols whose request raises), ``edit`` (a function that changes the raw kline rows of
    a symbol) and ``no_funding`` (symbols without funding records) make it misbehave on purpose."""

    def __init__(self, panel, metadata, *, fail=(), edit=None, no_funding=(), last_day=None):
        self.panel, self._metadata = panel, metadata
        self.fail, self.edit, self.no_funding, self.hidden = set(fail), edit, set(no_funding), set()
        self.last_day = int(panel.days[-1]) if last_day is None else int(last_day)
        self.lo = int(panel.days[0])
        self.column = {s: j for j, s in enumerate(panel.symbols)}
        self.requests = 0
        self.asked = []                                # (kind, symbol, days)

    def symbols(self):
        self.requests += 1
        self.asked.append(("symbols", None, ()))
        return [s for s in self.panel.symbols if s not in self.hidden]

    def metadata(self, fetched_at):
        self.requests += 1
        self.asked.append(("metadata", None, ()))
        if self._metadata is None:
            raise RuntimeError("exchangeInfo is down")
        return {**self._metadata, "content_hash": stable_hash(self._metadata), "fetched_at": fetched_at}

    def _held(self, days):
        return [d for d in days if self.lo <= d <= self.last_day]

    def klines(self, symbol, days):
        self.requests += 1
        self.asked.append(("klines", symbol, tuple(days)))
        if symbol in self.fail:
            raise BA.ArchiveError(f"HTTP 503 for {symbol}")
        j, p, rows = self.column[symbol], self.panel, []
        for d in self._held(days):
            i = d - self.lo
            if not np.isnan(p.close[i, j]):
                rows.append((d * DAY_MS, float(p.open[i, j]), float(p.high[i, j]), float(p.low[i, j]), float(p.close[i, j]),
                             0.0, float(p.quote_volume[i, j]), 1))
        if self.edit:
            rows = self.edit(symbol, rows)
        return F.Fetched(tuple(rows), frozenset(self._held(days)))

    def funding(self, symbol, days):
        self.requests += 1
        self.asked.append(("funding", symbol, tuple(days)))
        if symbol in self.no_funding:
            return F.Fetched((), frozenset(self._held(days)))
        j, p, rows = self.column[symbol], self.panel, []
        for d in self._held(days):
            i = d - self.lo
            rows += [(d * DAY_MS, 8, float(p.funding_midnight[i, j])),
                     (d * DAY_MS + 8 * 3_600_000, 8, float(p.funding_later_positive[i, j])),
                     (d * DAY_MS + 16 * 3_600_000, 8, float(p.funding_later_negative[i, j]))]
        return F.Fetched(tuple(rows), frozenset(self._held(days)))

    def kline_days(self, since=0):
        return [(s, d) for kind, s, days in self.asked[since:] if kind == "klines" for d in days]


def small_market(days=160, symbols=12):
    return P.synthetic_market(days=days, symbols=symbols)


def make_feed(tmp_path, panel, metadata, *, day=None, policy=None, db=None, clock=None, **source_kw):
    """A feed over a fresh database, its source answering from ``panel``, the clock a minute after ``day``."""
    db = db or S.fresh_db(tmp_path)
    clock = clock or Clock()
    source = PanelSource(panel, metadata, **source_kw)
    if day is not None:
        clock.after_close(day)
    feed = F.DailyBarFeed(db, fetcher=source, clock=clock, first_day=int(panel.days[0]), policy=policy)
    return feed, source, clock, db


def count(db, table, where="1=1", args=()):
    with db.connect() as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", args).fetchone()[0]


def same_panel(a, b):
    assert list(a.symbols) == list(b.symbols) and np.array_equal(a.days, b.days)
    assert set(a.fields) == set(b.fields)
    for name in a.fields:
        assert np.array_equal(a.fields[name], b.fields[name], equal_nan=True), name


# ----------------------------------------------------------------------- the feed against the frozen dataset
def test_a_feed_filled_day_by_day_from_the_dataset_gives_the_same_market_stage_and_the_same_decisions(tmp_path):
    """The synthetic dataset's own bars, fetched and kept in the database (the history at first contact, then
    one closed day at a time), against the same panel held in memory: the same panel bit for bit, the same
    features, the same market stage, the same recorded decisions and the same books."""
    dataset = _step2.SyntheticDataset()
    panel = dataset._load("2020-01-01", "2021-06-30")
    lo = int(panel.days[0])
    first_cycle, last_cycle = lo + 392, lo + 404       # 2021-01-27 to 2021-02-08: one contract ends on 2021-02-04
    accounts = ModelAccounts(initial_equity=EQUITY, levels=("balanced", "aggressive"), policies=(T.MANDATE, T.EXECUTABLE))
    clock = Clock()

    def platform(name):
        directory = tmp_path / name
        directory.mkdir()
        db = S.fresh_db(directory)
        registry = FR.FamilyRegistry(db, research_register=_step2.new_register(directory / "register.jsonl"))
        registry.register_all(actor_ref="system:test")
        return db, registry

    frozen = FrozenPanelBars(panel, dataset.metadata)
    db_frozen, registry = platform("frozen")
    with_frozen = D.PlatformCycleDriver(db_frozen, accounts=accounts, data={"1d": frozen}, registry=registry)
    db_feed, registry = platform("feed")
    source = PanelSource(panel, dataset.metadata)
    feed = F.DailyBarFeed(db_feed, fetcher=source, clock=clock, first_day=lo, auto_update=True)
    with_feed = D.PlatformCycleDriver(db_feed, accounts=accounts, data={"1d": feed}, registry=registry)
    clock.after_close(lo + 300)
    assert feed.update()["status"] == F.COMPLETED      # first contact: the history
    for day in range(lo + 301, first_cycle):           # then one closed day at a time
        clock.after_close(day)
        daily = feed.update()
        assert daily["status"] == F.COMPLETED and daily["symbol_days_requested"] == 26
    for day in range(first_cycle, last_cycle + 1):
        clock.after_close(day)
        a = with_frozen.run_cycle("DAILY_TREND", DT.day_text(day), now_ms=clock.now)
        b = with_feed.run_cycle("DAILY_TREND", DT.day_text(day), now_ms=clock.now)   # the feed fetches the day on demand
        assert (a.status, b.status) == (D.COMPLETED, D.COMPLETED), (DT.day_text(day), b.reason)
        from_feed, from_frozen = feed.panel_until(day), frozen.panel_until(day)
        same_panel(from_feed[0], from_frozen[0])
        assert DT.market_stage(*from_feed, day).summary() == DT.market_stage(*from_frozen, day).summary()
        features_a, features_b = SIM.prepare(*from_feed), SIM.prepare(*from_frozen)
        for name in ("strength", "stop_distance", "member", "volume", "step", "min_notional", "in_scope", "funding_hours"):
            assert np.array_equal(getattr(features_a, name), getattr(features_b, name), equal_nan=True), name
    assert np.isnan(feed.panel_until(last_cycle)[0].close[-1, 3]) and count(db_feed, "cati_feed_sealed_days") == 13
    sql = "SELECT account_id, data_cutoff, decision_hash FROM cati_family_decisions ORDER BY 1, 2"
    books = "SELECT account_id, equity, peak_equity, last_day, pending_orders FROM cati_shadow_accounts ORDER BY 1"
    with db_frozen.connect() as x, db_feed.connect() as y:
        decided = [tuple(r) for r in x.execute(sql)]
        assert decided == [tuple(r) for r in y.execute(sql)] and len(decided) == 4 * 13
        assert [tuple(r) for r in x.execute(books)] == [tuple(r) for r in y.execute(books)]
    assert count(db_feed, "cati_shadow_fills") > 0     # the comparison is not vacuous


# ------------------------------------------------------------------------------------ incremental, bounded
def test_only_missing_days_are_requested_and_the_request_volume_is_counted(tmp_path):
    panel, metadata = small_market()
    lo = int(panel.days[0])
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=lo + 99)
    first = feed.update()
    assert first["status"] == F.COMPLETED and first["symbols_requested"] == 12 and first["symbols_failed"] == 0
    assert first["bars_stored"] == count(db, "cati_feed_daily_bars") == int(np.isfinite(panel.close[:100]).sum())
    assert first["http_requests"] == source.requests == 2 + 2 * 12      # the listing, the metadata, bars and funding per symbol
    assert first["source_calls"] == 26
    assert first["symbol_days_requested"] == 12 * 100
    again = feed.update()                              # nothing new to learn: nothing is asked for
    assert (again["status"], again["http_requests"], again["symbols_requested"], again["bars_stored"]) == (F.COMPLETED, 0, 0, 0)
    for day in range(lo + 100, lo + 110):              # the daily case: exactly the day that closed, once per symbol
        mark = len(source.asked)
        clock.after_close(day)
        summary = feed.update()
        assert sorted(source.kline_days(mark)) == sorted((s, day) for s in panel.symbols)
        assert summary["bars_stored"] == 12 and summary["http_requests"] == 26 and summary["symbol_days_requested"] == 12
    with db.connect() as conn:
        rows = [tuple(r) for r in conn.execute("SELECT status, http_requests, symbol_days_requested FROM cati_feed_updates ORDER BY update_id")]
    assert rows[0] == (F.COMPLETED, 26, 1200) and rows[1] == (F.COMPLETED, 0, 0) and len(rows) == 12
    assert feed.status()["http_requests_total"] == source.requests == 26 + 10 * 26
    # a day's volume does not grow with the history: 26 requests on the first day after the history and on the tenth
    # serving a day reads the database; serving it again reads nothing and asks the source for nothing
    before = source.requests
    one, two = feed.panel_until(lo + 109), feed.panel_until(lo + 109)
    assert one[0] is two[0] and (feed.calls, feed.builds) == (2, 1) and source.requests == before
    same_panel(one[0], panel.truncated(lo + 109))


def test_an_update_is_bounded_and_the_history_is_not_served_before_it_is_complete(tmp_path):
    panel, metadata = small_market()
    lo = int(panel.days[0])
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=lo + 99, policy=F.FeedPolicy(max_requests_per_update=10))
    partial = feed.update()
    assert partial["status"] == F.PARTIAL and 0 < partial["symbols_requested"] < 12 and partial["detail"]["deferred"] > 0
    assert partial["http_requests"] <= 10 + 2          # the bound, overshot by at most one symbol
    fresh = feed.freshness(lo + 99)
    assert (fresh["usable"], fresh["reason"]) == (False, "DAILY_FEED_HISTORY_INCOMPLETE") and fresh["symbols_never_fetched"]
    with pytest.raises(F.FeedUnavailable) as refused:
        feed.panel_until(lo + 99)
    assert refused.value.code == "DAILY_FEED_HISTORY_INCOMPLETE" and count(db, "cati_feed_sealed_days") == 0
    for _ in range(4):
        last = feed.update()
    assert last["status"] == F.COMPLETED and feed.freshness(lo + 99)["usable"] is True
    same_panel(feed.panel_until(lo + 99)[0], panel.truncated(lo + 99))


def test_a_source_that_is_down_is_not_hammered_and_counts_against_no_symbol(tmp_path):
    panel, metadata = small_market()
    lo = int(panel.days[0])
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=lo + 99, fail=panel.symbols,
                                        policy=F.FeedPolicy(circuit_breaker_failures=4))
    summary = feed.update()
    assert summary["status"] == F.FAILED and summary["symbols_requested"] == 4 and "circuit_breaker" in summary["detail"]
    assert count(db, "cati_feed_symbols", "failures>0") == 0 and count(db, "cati_feed_daily_bars") == 0
    auto = F.DailyBarFeed(db, fetcher=source, clock=clock, auto_update=True)
    with pytest.raises(F.FeedUnavailable) as refused:
        auto.panel_until(lo + 99)
    assert refused.value.code == F.UNAVAILABLE == "DAILY_FEED_UNAVAILABLE"


def test_a_contract_that_ended_is_asked_again_only_now_and_then(tmp_path):
    panel, metadata = small_market()                   # the fourth contract has no bar from day 140 on
    lo, ended = int(panel.days[0]), panel.symbols[3]
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=lo + 139)
    feed.update()
    asked = []
    for day in range(lo + 140, lo + 160):
        mark = len(source.asked)
        clock.after_close(day)
        feed.update()
        asked.append(ended in {s for s, _ in source.kline_days(mark)})
    assert asked[:14] == [True] * 14                   # for two weeks its bar may only be late
    assert asked[14:] == [False] * 6                   # after that it is dormant: not asked every day
    clock.now += feed.policy.dormant_probe_ms
    mark = len(source.asked)
    assert feed.update()["detail"]["dormant_skipped"] == 0 and ended in {s for s, _ in source.kline_days(mark)}
    assert feed.freshness(lo + 159)["usable"] is True  # a contract without bars holds nothing up


def test_the_next_day_is_read_incrementally_and_is_the_same_panel_as_a_full_read(tmp_path):
    """In a running process the panel of a day is yesterday's panel plus the rows of the new day. It must be
    the panel a process that reads the sealed day whole gets, bit for bit: also when a contract is listed, and
    not at all when a bar for an older day has arrived in between."""
    panel, metadata = small_market()                   # the sixth contract has its first bar on day 60
    lo, late_listed = int(panel.days[0]), panel.symbols[5]
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=lo + 50)
    source.hidden = {late_listed}                      # the source does not list it before it trades
    feed.update()
    first, _ = feed.panel_until(lo + 50)
    assert late_listed not in first.symbols and first.shape == (51, 11) and (feed.builds, feed.extensions) == (1, 0)
    for day in range(lo + 51, lo + 70):
        if day == lo + 60:
            source.hidden = set()
        clock.after_close(day)
        mark = source.requests
        feed.update()
        assert source.requests - mark <= 2 + 2 * 12 + 1
        served, _ = feed.panel_until(day)
        whole, _ = F.DailyBarFeed(db, clock=clock).panel_until(day)    # another process: the sealed day, read whole
        same_panel(served, whole)
        assert DT.snapshot_id(served) == DT.snapshot_id(whole)
        expected = panel.truncated(day)
        if day < lo + 60:
            expected = DD.DailyPanel(expected.days, [s for s in expected.symbols if s != late_listed],
                                     {k: np.delete(v, 5, axis=1) for k, v in expected.fields.items()})
        same_panel(served, expected)
    assert (feed.builds, feed.extensions) == (1, 19) and late_listed in served.symbols
    assert (served.funding_hours[:60, 5] == 24).all()  # the funding it had before it traded came along with the column
    with db.connect() as conn:                         # each day read the rows of one day, not the history
        assert conn.execute("SELECT COUNT(*) FROM cati_feed_sealed_days").fetchone()[0] == 20
    # a bar that arrives late for an older day: the next panel is read whole, never patched
    source.fail = {panel.symbols[2]}
    clock.after_close(lo + 70, feed.policy.settle_ms)
    feed.update()
    assert np.isnan(feed.panel_until(lo + 70)[0].close[-1, 2]) and (feed.builds, feed.extensions) == (1, 20)
    source.fail = set()
    clock.after_close(lo + 71)
    feed.update()
    after, _ = feed.panel_until(lo + 71)
    assert (feed.builds, feed.extensions) == (2, 20)
    same_panel(after, panel.truncated(lo + 71))
    same_panel(feed.panel_until(lo + 70)[0], F.DailyBarFeed(db, clock=clock).panel_until(lo + 70)[0])
    assert np.isnan(feed.panel_until(lo + 70)[0].close[-1, 2])         # the day already served still lacks it


# --------------------------------------------------------------------------------------- what is a bar
ADVERSE_ROWS = [
    (5 * DAY_MS, 10.0, 11.0, 9.0, 10.5, 3.0, 30.0, 7),                 # a bar
    (5 * DAY_MS, 10.0, 11.0, 9.0, 10.5, 3.0, 30.0, 0),                 # no trades
    (5 * DAY_MS, 10.0, 11.0, 9.0, float("nan"), 3.0, 30.0, 7),
    (5 * DAY_MS, 10.0, float("inf"), 9.0, 10.5, 3.0, 30.0, 7),
    (5 * DAY_MS, 10.0, 10.4, 9.0, 10.5, 3.0, 30.0, 7),                 # high below the close
    (5 * DAY_MS, 10.0, 11.0, 10.2, 10.5, 3.0, 30.0, 7),                # low above the open
    (5 * DAY_MS, 0.0, 11.0, 0.0, 10.5, 3.0, 30.0, 7),
    (5 * DAY_MS, -1.0, 11.0, -2.0, 10.5, 3.0, 30.0, 7),
    (5 * DAY_MS, 10.0, 11.0, 9.0, 10.5, -3.0, 30.0, 7),
    (5 * DAY_MS, 10.0, 11.0, 9.0, 10.5, 3.0, -30.0, 7),
    (5 * DAY_MS + 1, 10.0, 11.0, 9.0, 10.5, 3.0, 30.0, 7),             # not the open of a day
    (5 * DAY_MS, 10.0, 10.0, 10.0, 10.0, 0.0, 0.0, 1),                 # flat and tiny, but a bar
]


@pytest.mark.parametrize("row", ADVERSE_ROWS)
def test_the_feed_and_the_research_dataset_agree_on_what_a_bar_is(row):
    kept = DD.clean_klines([row])
    is_bar_in_dataset = len(kept["days"]) == 1 and int(kept["trades"][0]) > 0      # "a zero-trade bar is not a bar"
    assert (F.bar_rejection(row) is None) == is_bar_in_dataset
    if len(kept["days"]) == 1 and not is_bar_in_dataset:
        assert F.bar_rejection(row) == F.NO_TRADES


def test_a_corrupt_or_untraded_row_is_rejected_with_a_reason_and_the_other_symbols_are_unaffected(tmp_path):
    panel, metadata = small_market()
    day = int(panel.days[0]) + 130
    bad = {panel.symbols[0]: lambda r: (r[0], r[1], r[2], r[3], float("nan"), r[5], r[6], r[7]),
           panel.symbols[1]: lambda r: (r[0], r[1], r[4] * 0.5, r[3], r[4], r[5], r[6], r[7]),
           panel.symbols[2]: lambda r: (*r[:7], 0),
           panel.symbols[4]: lambda r: (r[0], -r[1], r[2], -r[3], r[4], r[5], r[6], r[7])}

    def edit(symbol, rows):
        return [bad[symbol](r) if symbol in bad and r[0] == day * DAY_MS else r for r in rows]

    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=day, edit=edit)
    summary = feed.update()
    assert summary["status"] == F.COMPLETED and summary["days_rejected"] == 4 and summary["symbols_failed"] == 0
    with db.connect() as conn:
        reasons = {r[0]: r[1] for r in conn.execute("SELECT symbol, reason FROM cati_feed_rejections WHERE day=?", (day,))}
    assert reasons == {panel.symbols[0]: "NOT_FINITE", panel.symbols[1]: "HIGH_BELOW_OPEN_OR_CLOSE",
                       panel.symbols[2]: "NO_TRADES", panel.symbols[4]: "PRICE_NOT_POSITIVE"}
    fresh = feed.freshness(day)
    assert fresh["symbols_rejected"] == reasons and fresh["usable"] is False and fresh["reason"] == "DAILY_FEED_DAY_INCOMPLETE"
    assert fresh["coverage"] == pytest.approx(8 / 12)                  # twelve traded yesterday, four of them have no bar today
    lenient = F.DailyBarFeed(db, clock=clock, policy=F.FeedPolicy(min_coverage=0.5))
    served, _ = lenient.panel_until(day)
    expected = panel.truncated(day)
    expected.fields = {k: v.copy() for k, v in expected.fields.items()}
    for j in (0, 1, 2, 4):                             # rejected: no price that day, nothing repaired or carried forward
        for name in ("open", "high", "low", "close", "quote_volume"):
            expected.fields[name][-1, j] = np.nan
    same_panel(served, expected)
    # a rejected day stays rejected: the source offering a clean row later is recorded, not used
    clock.after_close(day + 1)
    clean = F.DailyBarFeed(db, fetcher=PanelSource(panel, metadata), clock=clock)
    recheck = clean.recheck(day, day)
    assert recheck["bars_stored"] == 0 and recheck["revisions"] == 4 and count(db, "cati_feed_daily_bars", "day=?", (day,)) == 8
    assert {r["symbol"] for r in clean.revisions()} == set(bad) and all(r["applied"] == 0 for r in clean.revisions())
    with db.connect() as conn:
        with pytest.raises(Exception, match="append-only"):
            conn.execute("DELETE FROM cati_feed_rejections")


def test_a_bar_is_stored_only_when_its_day_has_closed(tmp_path):
    panel, metadata = small_market()
    lo, day = int(panel.days[0]), int(panel.days[0]) + 120

    def eager(symbol, rows):                           # the source already prints the bar of the day that is still open
        j = panel.symbols.index(symbol)
        return rows + [((day + 1) * DAY_MS, *(float(panel.fields[n][day + 1 - lo, j]) for n in ("open", "high", "low", "close")),
                        0.0, float(panel.quote_volume[day + 1 - lo, j]), 1)]

    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=day, edit=eager)
    clock.now = F.close_ms(day + 1) - 1                # one millisecond before the next day closes
    summary = feed.update()
    assert summary["detail"]["unclosed_rows_ignored"] > 0 and count(db, "cati_feed_daily_bars", "day>?", (day,)) == 0
    assert feed.freshness(day + 1)["reason"] == "DAILY_FEED_DAY_NOT_CLOSED"
    with pytest.raises(F.FeedUnavailable, match="DAILY_FEED_DAY_NOT_CLOSED"):
        feed.panel_until(day + 1)
    clock.now += 1
    source.edit = None
    assert feed.update()["bars_stored"] == 12 and feed.freshness(day + 1)["usable"] is True


# ------------------------------------------------------------------------------------ failed-symbol isolation
def test_a_symbol_whose_request_fails_is_isolated_reported_and_retried_and_a_served_day_never_changes(tmp_path):
    panel, metadata = small_market()
    lo, day, victim = int(panel.days[0]), int(panel.days[0]) + 130, panel.symbols[7]
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=day - 1)
    assert feed.update()["status"] == F.COMPLETED
    source.fail = {victim}
    clock.after_close(day)
    summary = feed.update()
    assert summary["status"] == F.COMPLETED and summary["symbols_failed"] == 1 and summary["bars_stored"] == 11
    assert summary["symbol_failure_rate"] == pytest.approx(1 / 12) and victim in summary["detail"]["failed"]
    fresh = feed.freshness(day)
    assert fresh["symbols_failed"] == [victim] and fresh["coverage"] == pytest.approx(11 / 12)
    assert (fresh["usable"], fresh["reason"]) == (False, "DAILY_FEED_DAY_SETTLING")     # it may only be late: wait, within a limit
    clock.after_close(day, feed.policy.settle_ms)
    assert feed.update()["symbols_requested"] == 1     # only the one that is missing is asked again
    fresh = feed.freshness(day)
    assert fresh["usable"] is True and fresh["symbols_failed"] == [victim] and fresh["failures"] == {victim: "ArchiveError: HTTP 503 for " + victim}
    served, _ = feed.panel_until(day)                  # the day is used without it: one symbol does not stop the others
    j = panel.symbols.index(victim)
    assert np.isnan(served.close[-1, j]) and np.isfinite(np.delete(served.close[-1], j)).all()
    identity = DT.snapshot_id(served)
    with db.connect() as conn:
        sealed = json.loads(conn.execute("SELECT freshness FROM cati_feed_sealed_days WHERE day=?", (day,)).fetchone()[0])
    assert sealed["symbols_failed"] == [victim]        # what the decision did not have is on record with the day
    # the bar arrives late: it is stored, later days see it, the day already served does not change
    source.fail = set()
    clock.after_close(day + 1)
    late = feed.update()
    assert late["bars_stored"] == 12 + 1 and count(db, "cati_feed_daily_bars", "symbol=? AND day=?", (victim, day)) == 1
    again = F.DailyBarFeed(db, clock=clock)            # another process, no source: it serves what was sealed
    assert DT.snapshot_id(again.panel_until(day)[0]) == identity and np.isnan(again.panel_until(day)[0].close[-1, j])
    assert again.panel_until(day + 1)[0].close[-2, j] == panel.close[day - lo, j]
    assert count(db, "cati_feed_symbols", "failures>0") == 0


def test_the_driver_completes_a_cycle_while_one_symbol_fails_and_refuses_an_incomplete_day(tmp_path):
    dataset = _step2.SyntheticDataset()
    panel = dataset._load("2020-01-01", "2020-09-10")
    db = S.fresh_db(tmp_path)
    registry = FR.FamilyRegistry(db, research_register=_step2.new_register(tmp_path / "register.jsonl"))
    registry.register_all(actor_ref="system:test")
    clock = Clock().after_close(START - 1)
    source = PanelSource(panel, dataset.metadata)
    feed = F.DailyBarFeed(db, fetcher=source, clock=clock, first_day=int(panel.days[0]), auto_update=True,
                          policy=F.FeedPolicy(settle_ms=0))
    driver = D.PlatformCycleDriver(db, accounts=ModelAccounts(initial_equity=EQUITY, levels=("balanced",), policies=(T.MANDATE,)),
                                   data={"1d": feed}, registry=registry)
    assert driver.run_cycle("DAILY_TREND", DT.day_text(START - 1), now_ms=clock.now).status == D.COMPLETED
    source.fail = {panel.symbols[9]}
    clock.after_close(START)
    one_down = driver.run_cycle("DAILY_TREND", DT.day_text(START), now_ms=clock.now)
    assert one_down.status == D.COMPLETED and feed.freshness(START)["symbols_failed"] == [panel.symbols[9]]
    source.fail = set(panel.symbols[6:])               # most of the market is missing: the day is not used
    clock.after_close(START + 1)
    incomplete = driver.run_cycle("DAILY_TREND", DT.day_text(START + 1), now_ms=clock.now)
    assert (incomplete.status, incomplete.reason) == (D.WAITING_FOR_DATA, "DAILY_FEED_DAY_INCOMPLETE")
    assert count(db, "cati_family_decisions", "data_cutoff=?", (DT.day_text(START + 1),)) == 0
    assert count(db, "cati_feed_sealed_days", "day=?", (START + 1,)) == 0
    source.fail = set()                                # the source recovers: the same cycle is retried and completes
    assert driver.run_cycle("DAILY_TREND", DT.day_text(START + 1), now_ms=clock.now).status == D.COMPLETED
    assert F.FeedPolicy().min_coverage == DT.MIN_BAR_COVERAGE          # the feed's bar is not lower than the driver's


# ----------------------------------------------------------------------------------- stored data never changes
def test_a_changed_bar_is_detected_recorded_and_not_applied(tmp_path):
    panel, metadata = small_market()
    lo, day, symbol = int(panel.days[0]), int(panel.days[0]) + 120, panel.symbols[2]
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=day)
    feed.update()
    served, _ = feed.panel_until(day)
    identity, stored_close = DT.snapshot_id(served), float(panel.close[day - 10 - lo, 2])

    def revised(name, rows):                           # the source now prints another close for a day already stored
        return [(r[0], r[1], r[2] * 1.5, r[3], r[4] * 1.01, r[5], r[6], r[7]) if name == symbol and r[0] == (day - 10) * DAY_MS else r
                for r in rows]

    source.edit = revised
    clock.after_close(day + 1)
    recheck = feed.recheck(day - 20, day)
    assert recheck["revisions"] == 1 and recheck["bars_stored"] == 0
    revisions = feed.revisions()
    assert [(r["kind"], r["symbol"], r["record_key"], r["applied"]) for r in revisions] == [("BAR", symbol, day - 10, 0)]
    assert json.loads(revisions[0]["stored"])[3] == repr(stored_close) and json.loads(revisions[0]["offered"])[3] == repr(stored_close * 1.01)
    with db.connect() as conn:
        assert conn.execute("SELECT close FROM cati_feed_daily_bars WHERE symbol=? AND day=?", (symbol, day - 10)).fetchone()[0] == stored_close
    assert feed.recheck(day - 20, day)["revisions"] == 0               # the same offer again is the same record
    assert feed.status()["revisions_not_applied"] == 1
    fresh = F.DailyBarFeed(db, clock=clock)
    assert DT.snapshot_id(fresh.panel_until(day)[0]) == identity
    for table in ("cati_feed_daily_bars", "cati_feed_funding", "cati_feed_metadata", "cati_feed_revisions", "cati_feed_sealed_days"):
        assert count(db, table) > 0
        with db.connect() as conn:
            with pytest.raises(Exception, match="append-only"):
                conn.execute(f"DELETE FROM {table}")
        if table in ("cati_feed_daily_bars", "cati_feed_funding"):
            with db.connect() as conn:
                with pytest.raises(Exception, match="append-only"):
                    conn.execute(f"UPDATE {table} SET stored_at=0")


def test_a_sealed_day_whose_rows_were_removed_is_refused(tmp_path):
    panel, metadata = small_market()
    day = int(panel.days[0]) + 120
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=day)
    feed.update()
    feed.panel_until(day)
    with db.connect() as conn:                         # someone gets around the triggers
        conn.execute("DROP TRIGGER trg_cati_feed_bar_no_delete")
        conn.execute("DELETE FROM cati_feed_daily_bars WHERE day=? AND symbol=?", (day - 3, panel.symbols[0]))
    with pytest.raises(F.FeedUnavailable) as refused:
        F.DailyBarFeed(db, clock=clock).panel_until(day)
    assert refused.value.code == "DAILY_FEED_SEALED_DAY_CHANGED"


def test_the_first_day_of_the_history_cannot_be_moved(tmp_path):
    panel, metadata = small_market()
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=int(panel.days[0]) + 5)
    assert F.DailyBarFeed(db, clock=clock).first_day == feed.first_day == int(panel.days[0])
    with pytest.raises(ValueError, match="history starts on"):
        F.DailyBarFeed(db, clock=clock, first_day=feed.first_day + 30)
    assert F.DEFAULT_FIRST_DAY == DD.day_index("2020-01-01") == DT.day_of(spec.DEVELOPMENT_START)


# ------------------------------------------------------------------------------- unavailable stays unavailable
def test_missing_funding_stays_missing_and_is_charged_by_the_registered_rule_not_as_zero(tmp_path):
    panel, metadata = small_market()
    lo, day, bare = int(panel.days[0]), int(panel.days[0]) + 130, panel.symbols[1]
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=day, no_funding=[bare])
    feed.update()
    served, served_metadata = feed.panel_until(day)
    j = panel.symbols.index(bare)
    assert count(db, "cati_feed_funding", "symbol=?", (bare,)) == 0
    assert (served.funding_hours[:, j] == 0).all()     # zero hours covered is the evaluator's "unknown"
    assert (served.funding_hours[:, 0] == 24).all() and np.array_equal(served.funding_midnight[:, 0], panel.funding_midnight[:day - lo + 1, 0])
    market = DT.market_stage(served, served_metadata, day)
    costs = {}
    for symbol in (bare, panel.symbols[0]):
        price = float(market.features.close[market.row - 1, market.index(symbol)])
        book = DT.new_book("balanced", T.MANDATE, EQUITY, day)
        book.positions[symbol] = DT.Position(quantity=1.0, stop=price * 0.01, mark=price, entry_day=day - 1, entry_price=price,
                                             entry_stop_distance=0.5)
        costs[symbol] = (DT.advance_day(book, market)["sums"]["funding"], float(market.features.open[market.row, market.index(symbol)]))
    unknown, opened = costs[bare]
    assert unknown == pytest.approx(3 * spec.INTERPRETATION["missing_funding_rate_per_8h"] * opened)   # three imputed events
    assert unknown != 0.0 and costs[panel.symbols[0]][0] != pytest.approx(unknown / opened * costs[panel.symbols[0]][1])


def test_missing_exchange_metadata_is_not_invented(tmp_path):
    panel, _ = small_market()
    day = int(panel.days[0]) + 130
    listed = {"symbols": {panel.symbols[0]: {"contract_type": "PERPETUAL", "market_step_size": "0.001", "min_notional": "20"}}}
    feed, source, clock, db = make_feed(tmp_path, panel, listed, day=day)
    feed.update()
    served, metadata = feed.panel_until(day)
    assert metadata["symbols"] == listed["symbols"]    # exactly what the exchange said: nothing added for the others
    features = SIM.prepare(served, metadata)
    assert (features.step[0], features.min_notional[0]) == (0.001, 20.0)
    # the others are handled as the research dataset handles a symbol without metadata: no step, the registered minimum
    assert (features.step[1:] == 0.0).all() and (features.min_notional[1:] == spec.INTERPRETATION["min_notional_without_metadata_usdt"]).all()
    # no snapshot at all is not "no metadata for a symbol": the feed does not serve without one
    other = tmp_path / "none"
    other.mkdir()
    feed, source, clock, db = make_feed(other, panel, None, day=day)
    summary = feed.update()
    assert summary["status"] == F.COMPLETED and "metadata" in summary["detail"]["listing_errors"]
    assert feed.freshness(day)["reason"] == "DAILY_FEED_NO_EXCHANGE_METADATA"
    with pytest.raises(F.FeedUnavailable, match="DAILY_FEED_NO_EXCHANGE_METADATA"):
        feed.panel_until(day)
    source._metadata = {"symbols": {}}                 # it comes back; later it goes away again and the snapshot ages
    clock.now += feed.policy.listing_refresh_ms
    feed.update()
    assert feed.freshness(day)["usable"] is True
    source._metadata = None
    clock.after_close(day + 4)
    feed.update()
    stale = feed.freshness(day + 4)
    assert stale["reason"] == "DAILY_FEED_EXCHANGE_METADATA_STALE" and stale["exchange_metadata"]["age_ms"] > feed.policy.metadata_max_age_ms


# -------------------------------------------------------------------------------------------- freshness
def test_freshness_says_what_a_day_rests_on(tmp_path):
    panel, metadata = small_market()
    lo, day = int(panel.days[0]), int(panel.days[0]) + 130
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=day)
    assert feed.freshness(day)["reason"] == "DAILY_FEED_NOT_UPDATED" and feed.freshness(day)["newest_bar_day"] is None
    feed.update()
    fresh = feed.freshness(day)
    assert fresh["usable"] is True and fresh["reason"] is None and fresh["sealed"] is False and fresh["closed"] is True
    assert (fresh["bars"], fresh["expected_symbols"], fresh["present_of_expected"], fresh["coverage"]) == (12, 12, 12, 1.0)
    assert fresh["newest_bar_day"] == DT.day_text(day) and fresh["newest_bar_age_ms"] == fresh["decision_bar_age_ms"] == 60_000
    assert fresh["exchange_metadata"]["age_ms"] == 0 and fresh["last_update"]["status"] == F.COMPLETED
    assert fresh["symbols_failed"] == fresh["symbols_pending"] == [] and fresh["min_coverage"] == 0.90
    clock.now += 5 * 3_600_000
    assert feed.freshness(day)["newest_bar_age_ms"] == 60_000 + 5 * 3_600_000
    assert feed.freshness(day + 1)["reason"] == "DAILY_FEED_DAY_NOT_CLOSED" and feed.freshness(day + 1)["decision_bar_age_ms"] is None
    assert feed.freshness(lo - 1)["reason"] == "DAILY_FEED_DAY_BEFORE_HISTORY"
    feed.panel_until(day)
    assert feed.freshness(day)["sealed"] is True and feed.freshness(day)["usable"] is True
    clock.after_close(day + 3)                         # nobody updated: the newest bar is days old and says so
    behind = feed.freshness(day + 3)
    assert behind["reason"] == "DAILY_FEED_NOT_UPDATED" and behind["newest_bar_age_ms"] == 3 * DAY_MS + 60_000


# ------------------------------------------------------------------------------------------- governance
def test_the_reserved_research_holdout_is_never_served_as_a_decision_day(tmp_path):
    assert F.research_holdout_window() == (DT.day_of("2025-01-01"), DT.day_of("2026-09-30"))
    first = DT.day_of("2024-10-01")
    panel, metadata = P.synthetic_market(days=140, symbols=8, first_day="2024-10-01")
    feed, source, clock, db = make_feed(tmp_path, panel, metadata, day=first + 91)      # 2024-12-31
    feed.update()
    assert feed.panel_until(first + 91)[0].shape == (92, 8)            # the last day before the holdout is served
    for text in ("2025-01-01", "2025-02-10", "2026-09-30"):
        day = DT.day_of(text)
        clock.after_close(day)
        feed.update(through=min(day, first + 139))                     # its bars are history for the indicators ...
        with pytest.raises(F.FeedUnavailable) as refused:              # ... and never a decision day
            feed.panel_until(day)
        assert refused.value.code == "DAILY_FEED_DAY_IN_RESERVED_HOLDOUT"
        assert feed.freshness(day)["reason"] == "DAILY_FEED_DAY_IN_RESERVED_HOLDOUT"
    assert count(db, "cati_feed_sealed_days") == 1 and count(db, "cati_feed_daily_bars", "day>?", (first + 91,)) > 0
    assert feed.status()["reserved_research_holdout"] == ["2025-01-01", "2026-09-30"]


def test_the_feed_cannot_reach_the_research_dataset_an_order_or_a_strategy_evaluation():
    source = Path(F.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.add("." * node.level + (node.module or ""))
            imported |= {f"{'.' * node.level}{node.module or ''}:{a.name}" for a in node.names}
        elif isinstance(node, ast.Import):
            imported |= {a.name for a in node.names}
    forbidden = ("app.runner", "app.exchange", "app.execution", "app.trading_intelligence.execution", "app.trading_intelligence.research",
                 "app.trading_intelligence.integration", "app.trading_intelligence.pipeline", "app.core", "requests", "pandas")
    assert not [m for m in imported if m.startswith(forbidden)], imported
    used = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    # nothing that opens, loads, builds or writes the frozen dataset store, and nothing that scores a strategy
    assert not used & {"load_panel", "load_tables", "normalize", "mirror", "discover", "build_manifest", "verify_manifest",
                       "ArchiveMirror", "complete_from_daily_files", "simulate", "prepare", "decide_targets", "open", "write_bytes",
                       "write_text", "read_parquet", "to_parquet"}
    for text in ("data/research", "docs/research", ".parquet", "manifest.json"):
        assert text not in source.replace("``data/research``", "")
    public = {name for name in F.__all__ if callable(getattr(F, name))} | {m for m in dir(F.DailyBarFeed) if not m.startswith("_")}
    assert not [n for n in public if any(word in n.lower() for word in ("evaluat", "backtest", "simulat", "performance", "pnl", "return"))]
    assert {m for m in dir(F.DailyBarFeed) if not m.startswith("_")} == {
        "first_day", "policy", "update", "recheck", "freshness", "panel_until", "status", "revisions"}


def test_the_feed_tables_are_part_of_the_platform_schema(tmp_path):
    db = S.fresh_db(tmp_path)
    with db.connect() as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert set(FEED_TABLES) <= tables
        columns = {t: {r[1] for r in conn.execute(f"PRAGMA table_info({t})")} for t in FEED_TABLES}
    # no table holds a credential, an account or an order: the feed is public market data
    assert not [c for cols in columns.values() for c in cols if any(w in c.lower() for w in ("key", "secret", "token", "account", "user", "order"))
                and c not in ("record_key", "key")]
    from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
    ensure_cati_platform_schema(db)                    # idempotent


# ------------------------------------------------------------------------------------- the archive fetcher
class CannedArchive:
    """An ``http_get`` over a dictionary of archive objects, in the shapes ``binance_archive`` parses: zipped
    CSV files with their ``.CHECKSUM``, the bucket listing as XML, ``exchangeInfo`` as JSON. No network."""

    def __init__(self) -> None:
        self.objects, self.requests, self.down, self.exchange_info = {}, [], False, {"symbols": []}

    @staticmethod
    def _zip(name: str, text: str) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0)), text)
        return buffer.getvalue()

    def put(self, key: str, text: str, *, bad_checksum: bool = False) -> None:
        body = self._zip(key.rsplit("/", 1)[-1][:-4] + ".csv", text)
        digest = hashlib.sha256(body + (b"x" if bad_checksum else b"")).hexdigest()
        self.objects[key] = body
        self.objects[key + ".CHECKSUM"] = f"{digest}  {key.rsplit('/', 1)[-1]}\n".encode()

    def klines(self, symbol: str, days, *, monthly: str = "", header: bool = True, trades: int = 12, close=None, bad_checksum=False):
        lines = ["open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore"] if header else []
        for d in days:
            c = 100.0 + d % 7 if close is None else close
            lines.append(f"{d * DAY_MS},{c - 1},{c + 2},{c - 3},{c},10.5,{(d + 1) * DAY_MS - 1},{1000.0 + d},{trades},5.0,500.0,0")
        key = (f"{BA.KLINES_ROOT}{symbol}/1d/{symbol}-1d-{monthly}.zip" if monthly else
               f"{DD.DAILY_KLINES_ROOT}{symbol}/1d/{symbol}-1d-{DD.day_text(days[0])}.zip")
        self.put(key, "\n".join(lines) + "\n", bad_checksum=bad_checksum)

    def funding(self, symbol: str, month: str, days, rate: float = 0.0001) -> None:
        lines = ["calc_time,funding_interval_hours,last_funding_rate"]
        lines += [f"{d * DAY_MS + h * 3_600_000},8,{rate}" for d in days for h in (0, 8, 16)]
        self.put(f"{BA.FUNDING_ROOT}{symbol}/{symbol}-fundingRate-{month}.zip", "\n".join(lines) + "\n")

    def __call__(self, url: str) -> bytes:
        self.requests.append(url)
        if self.down:
            raise BA.ArchiveError(f"could not fetch {url}: connection refused")
        if url == DD.EXCHANGE_INFO_URL:
            return json.dumps(self.exchange_info).encode()
        if url.startswith(BA.LISTING_URL + "?"):
            prefix = urllib.parse.parse_qs(url.split("?", 1)[1])["prefix"][0]
            below = sorted(k for k in self.objects if k.startswith(prefix))
            folders = sorted({prefix + k[len(prefix):].split("/", 1)[0] + "/" for k in below if "/" in k[len(prefix):]})
            files = [k for k in below if "/" not in k[len(prefix):]]
            xml = ['<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
                   "<IsTruncated>false</IsTruncated>"]
            xml += [f"<Contents><Key>{k}</Key><LastModified>2026-10-01T00:00:00.000Z</LastModified><Size>{len(self.objects[k])}</Size></Contents>" for k in files]
            xml += [f"<CommonPrefixes><Prefix>{f}</Prefix></CommonPrefixes>" for f in folders]
            return ("".join(xml) + "</ListBucketResult>").encode()
        key = url[len(BA.ARCHIVE_URL) + 1:]
        if key not in self.objects:
            raise BA.ArchiveError(f"not in the archive: {url}")
        return self.objects[key]


def test_the_archive_fetcher_reads_daily_files_verifies_them_and_counts_its_requests():
    archive = CannedArchive()
    day = DD.day_index("2026-10-05")
    archive.klines("BTCUSDT", [day])
    archive.klines("ETHUSDT", [day + 1], header=False)
    fetcher = F.BinanceArchiveFetcher(archive)
    got = fetcher.klines("BTCUSDT", [day, day + 1])
    c = 100.0 + day % 7
    assert got.rows == ((day * DAY_MS, c - 1, c + 2, c - 3, c, 10.5, 1000.0 + day, 12),) and got.answered == {day}
    assert fetcher.requests == len(archive.requests) == 3             # checksum and file of one day, the checksum of the missing one
    assert all(u.startswith("https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-10-0") for u in archive.requests)
    assert fetcher.klines("ETHUSDT", [day + 1]).rows[0][0] == (day + 1) * DAY_MS       # an older file without a header row
    archive.klines("XRPUSDT", [day], bad_checksum=True)
    with pytest.raises(BA.ChecksumMismatch):           # bytes that do not match the archive's checksum are never parsed
        fetcher.klines("XRPUSDT", [day])
    archive.down = True
    with pytest.raises(BA.ArchiveError, match="could not fetch"):     # an outage is an error, never "no bar"
        fetcher.klines("BTCUSDT", [day])


def test_the_archive_fetcher_takes_a_long_range_from_monthly_files_and_the_gaps_from_daily_files():
    archive = CannedArchive()
    august = [DD.day_index("2026-08-01") + n for n in range(31)]
    september = [DD.day_index("2026-09-01") + n for n in range(30)]
    october = [DD.day_index("2026-10-01") + n for n in range(6)]
    archive.klines("BTCUSDT", [d for d in august if d != august[9]], monthly="2026-08")   # the monthly file skips a day
    archive.klines("BTCUSDT", [august[9]])                                               # ... which a daily file has
    archive.klines("BTCUSDT", september, monthly="2026-09")
    for d in october[:5]:                              # the running month: daily files only, the last day not published yet
        archive.klines("BTCUSDT", [d])
    fetcher = F.BinanceArchiveFetcher(archive)
    wanted = [DD.day_index("2026-07-20") + n for n in range(12)] + august + september + october
    got = fetcher.klines("BTCUSDT", wanted)
    days = sorted(r[0] // DAY_MS for r in got.rows)
    assert days == august + september + october[:5]
    assert got.answered == set(august) | set(september) | set(october[:5])           # July and the unpublished day: no answer
    listings = [u for u in archive.requests if u.startswith(BA.LISTING_URL)]
    assert len(listings) == 2 and fetcher.requests == 2 + 2 * 2 + 2 * 6               # two listings, two months, six daily files
    cleaned = DD.clean_klines(list(got.rows))          # the dataset's own cleaning accepts every row
    assert len(cleaned["days"]) == len(days) and cleaned["invalid"] == [] and cleaned["duplicates"] == 0


def test_the_archive_fetcher_asks_for_funding_only_when_the_month_has_ended():
    archive = CannedArchive()
    september = [DD.day_index("2026-09-01") + n for n in range(30)]
    archive.funding("BTCUSDT", "2026-09", september)
    fetcher = F.BinanceArchiveFetcher(archive)
    running = fetcher.funding("BTCUSDT", september[:20])               # the month is not over: nothing to ask, nothing known
    assert running.rows == () and running.answered == frozenset() and fetcher.requests == 0
    ended = fetcher.funding("BTCUSDT", september[10:] + [september[-1] + 1])
    assert len(ended.rows) == 90 and ended.answered == set(september[10:]) and fetcher.requests == 2
    assert ended.rows[0] == (september[0] * DAY_MS, 8, 0.0001)
    unpublished = fetcher.funding("ETHUSDT", september)                # ended, but the archive has not published it yet
    assert unpublished.rows == () and unpublished.answered == frozenset()


def test_the_archive_fetcher_lists_contracts_and_reads_the_exchange_metadata():
    archive = CannedArchive()
    day = DD.day_index("2026-10-05")
    archive.klines("BTCUSDT", [day - 40], monthly="2026-08")
    archive.klines("OLDUSDTSETTLED", [day - 400], monthly="2025-08")   # an ended contract is found like a trading one
    archive.klines("NEWUSDT", [day])                   # listed this month: daily files only
    archive.klines("BTCUSD_PERP", [day])               # not a USDT-margined perpetual
    archive.klines("ETHBTC", [day - 40], monthly="2026-08")
    archive.exchange_info = {"symbols": [{"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING", "baseAsset": "BTC",
                                          "quoteAsset": "USDT", "marginAsset": "USDT", "onboardDate": 1, "deliveryDate": 2,
                                          "filters": [{"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                                                      {"filterType": "MIN_NOTIONAL", "notional": "100"}]}]}
    fetcher = F.BinanceArchiveFetcher(archive)
    assert fetcher.symbols() == ["BTCUSDT", "NEWUSDT", "OLDUSDTSETTLED"]
    snapshot = fetcher.metadata("2026-10-06T00:01:00Z")
    assert snapshot["symbols"]["BTCUSDT"]["market_step_size"] == "0.001" and snapshot["symbols"]["BTCUSDT"]["min_notional"] == "100"
    assert snapshot["point_in_time"] is False and snapshot["fetched_at"] == "2026-10-06T00:01:00Z" and "NEWUSDT" not in snapshot["symbols"]
    assert archive.requests[-1] == "https://fapi.binance.com/fapi/v1/exchangeInfo"
    hosts = {urllib.parse.urlparse(u).netloc for u in archive.requests}
    assert hosts == {"s3-ap-northeast-1.amazonaws.com", "fapi.binance.com"}       # public market data, nothing else


def _october_archive(symbols, first, last, *, absent=()):
    archive = CannedArchive()
    for n, symbol in enumerate(symbols):
        for d in range(first, last + 1):
            if (symbol, d) not in absent:
                archive.klines(symbol, [d], close=50.0 + 10 * n + (d - first))
    archive.exchange_info = {"symbols": [{"symbol": s, "contractType": "PERPETUAL", "status": "TRADING",
                                          "filters": [{"filterType": "MARKET_LOT_SIZE", "stepSize": "0.01", "minQty": "0.01"},
                                                      {"filterType": "MIN_NOTIONAL", "notional": "5"}]} for s in symbols[:2]]}
    return archive


def test_default_daily_bars_updates_on_demand_from_the_public_archive(tmp_path):
    """The provider of the operator command line (``pipeline.__main__``, ``run-due``), with a canned archive."""
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]
    first, day = DD.day_index("2026-10-01"), DD.day_index("2026-10-06")
    archive = _october_archive(symbols, first, day + 1)
    db = S.fresh_db(tmp_path)
    clock = Clock().after_close(day)
    provider = F.default_daily_bars(db, http_get=archive, clock=clock, first_day=first, workers=2)
    assert archive.requests == []                      # building the provider asks nothing
    panel, metadata = provider.panel_until(day)        # the one call the driver makes
    assert panel.symbols == symbols and panel.shape == (6, 3) and list(panel.days) == list(range(first, day + 1))
    assert set(panel.fields) == set(F.PRICE_FIELDS + F.FUNDING_FIELDS)
    assert panel.close[-1].tolist() == [55.0, 65.0, 75.0] and panel.open[0, 2] == 69.0 and panel.quote_volume[0, 0] == 1000.0 + first
    assert (panel.funding_hours == 0).all()            # the archive publishes funding after the month: unknown, not zero
    assert sorted(metadata["symbols"]) == symbols[:2] and metadata["symbols"]["AAAUSDT"]["market_step_size"] == "0.01"
    used = len(archive.requests)
    assert provider.panel_until(day)[0] is panel and len(archive.requests) == used   # a served day costs no request
    status = provider.status()
    assert status["bars"] == 18 and status["http_requests_total"] == used and status["sealed_days"] == 1
    clock.after_close(day + 1)                         # the next day: one daily file per contract and nothing else
    provider.panel_until(day + 1)
    assert sorted(u for u in archive.requests[used:] if u.startswith(BA.ARCHIVE_URL) and "CHECKSUM" not in u) == [
        f"{BA.ARCHIVE_URL}/{DD.DAILY_KLINES_ROOT}{s}/1d/{s}-1d-2026-10-07.zip" for s in symbols]
    # the operator command line calls it with the database alone: everything else has a default
    import inspect

    parameters = list(inspect.signature(F.default_daily_bars).parameters.values())
    assert parameters[0].name == "db" and all(p.kind is p.KEYWORD_ONLY and p.default is not p.empty for p in parameters[1:])
    cli = Path(D.__file__).with_name("__main__.py")
    assert not cli.exists() or "from app.market_data.daily_feed import default_daily_bars" in cli.read_text(encoding="utf-8")


def test_default_daily_bars_refuses_with_a_code_when_the_source_fails_and_never_serves_a_partial_panel(tmp_path):
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]
    first, day = DD.day_index("2026-10-01"), DD.day_index("2026-10-06")
    db = S.fresh_db(tmp_path)
    clock = Clock().after_close(day)
    dead = CannedArchive()
    dead.down = True
    with pytest.raises(F.FeedUnavailable) as refused:  # nothing can be fetched at all
        F.default_daily_bars(db, http_get=dead, clock=clock, first_day=first).panel_until(day)
    assert refused.value.code == "DAILY_FEED_UNAVAILABLE" and getattr(refused.value, "code", None) and count(db, "cati_feed_daily_bars") == 0
    archive = _october_archive(symbols, first, day, absent={("BBBUSDT", day), ("CCCUSDT", day)})
    provider = F.default_daily_bars(db, http_get=archive, clock=clock, first_day=first, workers=1)
    with pytest.raises(F.FeedUnavailable) as refused:  # most of the day is not published yet: no partial panel
        provider.panel_until(day)
    assert refused.value.code == "DAILY_FEED_DAY_INCOMPLETE" and count(db, "cati_feed_sealed_days") == 0
    assert count(db, "cati_feed_daily_bars", "day=?", (day,)) == 1
    for symbol in symbols[1:]:                         # the archive publishes the rest: only those are fetched
        archive.klines(symbol, [day], close=77.0)
    mark = len(archive.requests)
    panel, _ = provider.panel_until(day)
    assert panel.close[-1].tolist()[1:] == [77.0, 77.0]
    assert not [u for u in archive.requests[mark:] if "AAAUSDT" in u and "klines" in u]
    # one contract whose file is corrupt: the day is served without it once the settling time has passed
    clock.after_close(day + 1)
    archive.klines("AAAUSDT", [day + 1], close=60.0, bad_checksum=True)
    for symbol in symbols[1:]:
        archive.klines(symbol, [day + 1], close=78.0)
    lenient = F.default_daily_bars(db, http_get=archive, clock=clock, policy=F.FeedPolicy(min_coverage=0.6, settle_ms=0))
    panel, _ = lenient.panel_until(day + 1)
    assert np.isnan(panel.close[-1, 0]) and panel.close[-1].tolist()[1:] == [78.0, 78.0]
    assert "ChecksumMismatch" in lenient.freshness(day + 1)["failures"]["AAAUSDT"]
