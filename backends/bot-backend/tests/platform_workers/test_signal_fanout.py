"""Step 5.4: one shared signal per cycle, isolated per-account sizing, bounded concurrency."""
from __future__ import annotations

import pytest

from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import performance as P
from app.trading_intelligence.scale.fanout import (AccountSnapshot, SharedSignal, SharedSignalMutated, size_accounts)
from app.trading_intelligence.scale.measure_fanout import DECISION_DAY, _books


@pytest.fixture(scope="module")
def signal():
    panel, metadata = P.synthetic_market()
    calls = []

    def build():
        calls.append(1)
        return DT.market_stage(panel, metadata, DT.day_of(DECISION_DAY))

    s = SharedSignal.compute(DT.FAMILY_ID, DECISION_DAY, build)
    assert calls == [1]
    return s


def test_one_signal_sizes_every_account_identically_in_parallel(signal):
    snaps = _books(60, signal.market)
    seq = size_accounts(signal, snaps, max_concurrency=1)
    par = size_accounts(signal, snaps, max_concurrency=8)
    assert not seq.errors and not par.errors and len(par.sizings) == 60
    for a in seq.sizings:
        assert seq.sizings[a].result.document == par.sizings[a].result.document


def test_account_results_depend_only_on_their_own_snapshot(signal):
    snaps = _books(20, signal.market)
    alone = {s.account_id: size_accounts(signal, [s]).sizings[s.account_id].result.document for s in snaps[:5]}
    together = size_accounts(signal, snaps, max_concurrency=8)
    for account_id, doc in alone.items():
        assert together.sizings[account_id].result.document == doc


def test_sizer_cannot_modify_the_callers_or_another_accounts_book(signal):
    snaps = _books(4, signal.market)
    before = [len(s.book.positions) for s in snaps]

    def vandal(sig, book):
        book.positions.clear()
        book.equity = -1
        return DT.decide(book, sig.market)

    size_accounts(signal, snaps, sizer=vandal, max_concurrency=4)
    assert [len(s.book.positions) for s in snaps] == before


def test_mutating_the_shared_signal_is_detected(signal):
    panel, metadata = P.synthetic_market()
    own = SharedSignal.compute(DT.FAMILY_ID, DECISION_DAY, lambda: DT.market_stage(panel, metadata,
                                                                                 DT.day_of(DECISION_DAY)))

    def corrupt(sig, book):
        sig.market.features.close[sig.market.row, 0] *= 2
        return None

    with pytest.raises(SharedSignalMutated):
        size_accounts(own, _books(1, own.market), sizer=corrupt)


def test_one_failing_account_is_isolated_and_budget_is_reported(signal):
    snaps = _books(10, signal.market)

    def sizer(sig, book):
        if book.initial_equity == snaps[3].book.initial_equity:
            raise ValueError("snapshot incomplete")
        return DT.decide(book, sig.market)

    report = size_accounts(signal, snaps, sizer=sizer, max_concurrency=4, account_budget_ms=10_000,
                           cycle_budget_ms=60_000)
    assert set(report.errors) == {snaps[3].account_id}
    assert report.within_budget and len(report.sizings) == 10


def test_duplicate_account_in_one_fanout_is_refused(signal):
    s = _books(1, signal.market)[0]
    with pytest.raises(ValueError):
        size_accounts(signal, [s, AccountSnapshot(s.account_id, s.tenant_id, 2, s.book)])
