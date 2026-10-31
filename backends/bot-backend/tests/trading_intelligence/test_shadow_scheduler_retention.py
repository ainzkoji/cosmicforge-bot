"""The shadow scheduler runs table retention independently of the family cycles (Section K4 maintenance).

Before, retention ran after ``run_due`` returned, so a cycle that raised every tick also stopped retention and the
CATI tables grew without bound. Now retention runs in a ``finally`` at most once a day, a retention failure is
logged and retried on the next tick, and it never replaces the cycle's own result or error.
"""
from types import SimpleNamespace

import pytest

from app.trading_intelligence.integration.platform_runtime import ShadowScheduler
from app.trading_intelligence.pipeline import retention

DAY = 86_400_000


class Driver:
    def __init__(self, fail=False):
        self.fail, self.calls = fail, 0

    def run_due(self, now):
        self.calls += 1
        if self.fail:
            raise RuntimeError("cycle failed")
        return ["result"]


@pytest.fixture
def runs(monkeypatch):
    seen = []
    monkeypatch.setattr(retention, "run_all", lambda db, **kw: seen.append(kw["now_ms"]))
    return seen


def scheduler(tmp_path, driver, clock, owns=lambda db: True):
    db = SimpleNamespace(path=str(tmp_path / "bot.db"))
    return ShadowScheduler(db, owns_runtime=owns, driver=driver, clock=clock)


def test_retention_runs_when_the_cycle_raises_and_the_error_still_propagates(tmp_path, runs):
    now = [10 * DAY]
    s = scheduler(tmp_path, Driver(fail=True), lambda: now[0])
    with pytest.raises(RuntimeError, match="cycle failed"):
        s.tick()
    assert runs == [10 * DAY]
    now[0] += DAY // 2
    with pytest.raises(RuntimeError):
        s.tick()
    assert runs == [10 * DAY]                       # at most once a day
    now[0] += DAY
    with pytest.raises(RuntimeError):
        s.tick()
    assert runs == [10 * DAY, 10 * DAY + DAY // 2 + DAY]


def test_a_retention_failure_never_hides_the_cycle_result_and_is_retried(tmp_path, monkeypatch, caplog):
    attempts = []

    def failing(db, **kw):
        attempts.append(kw["now_ms"])
        if len(attempts) == 1:
            raise OSError("archive disk full")

    monkeypatch.setattr(retention, "run_all", failing)
    now = [5 * DAY]
    s = scheduler(tmp_path, Driver(), lambda: now[0])
    assert s.tick() == ["result"]
    assert s.last_retention is None and "retention failed" in caplog.text
    now[0] += 30_000
    assert s.tick() == ["result"]                    # retried on the next tick, not a day later
    assert attempts == [5 * DAY, 5 * DAY + 30_000] and s.last_retention == 5 * DAY + 30_000


def test_no_retention_and_no_cycle_without_runtime_ownership_or_after_stop(tmp_path, runs):
    driver = Driver()
    s = scheduler(tmp_path, driver, lambda: DAY, owns=lambda db: False)
    assert s.tick() == [] and driver.calls == 0 and runs == []
    owner = scheduler(tmp_path, driver, lambda: DAY)
    owner.request_stop()
    assert owner.tick() == [] and runs == []
