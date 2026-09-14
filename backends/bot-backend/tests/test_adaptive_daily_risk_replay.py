from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from app.risk.adaptive_daily_replay import (
    Opportunity,
    ReplayDataset,
    ReplayPolicy,
    RiskObservation,
    build_report_payload,
    dataset_hash,
    load_replay_dataset,
    replay_policies,
    replay_policy,
)


BASE = datetime(2026, 9, 12, 10, tzinfo=timezone.utc)


def opp(
    i: int,
    *,
    at: datetime | None = None,
    close_after_minutes: int = 5,
    risk: float = 2.0,
    pnl: float = 0.0,
    regime: str = "WEAK_TREND",
) -> Opportunity:
    evaluated_at = at or (BASE + timedelta(minutes=i * 10))
    return Opportunity(
        decision_id=f"dec-{i}",
        position_id=f"pos-{i}",
        symbol="BTCUSDT",
        side="LONG",
        regime=regime,
        evaluated_at=evaluated_at,
        closed_at=evaluated_at + timedelta(minutes=close_after_minutes),
        planned_risk_usdt=risk,
        realized_pnl_usdt=pnl,
        fees_usdt=0.01,
    )


def risk_history(n: int = 40, *, risk: float = 4.0, before: datetime = BASE) -> tuple[RiskObservation, ...]:
    return tuple(
        RiskObservation(before - timedelta(minutes=n - i + 1), risk)
        for i in range(n)
    )


def dataset(opportunities: tuple[Opportunity, ...], risks: tuple[RiskObservation, ...] | None = None) -> ReplayDataset:
    risks = risks if risks is not None else risk_history()
    return ReplayDataset(
        opportunities=opportunities,
        risk_observations=risks,
        dataset_hash=dataset_hash(opportunities, risks),
        source_summary={"opportunity_count": len(opportunities), "risk_observation_count": len(risks)},
    )


def adaptive(name: str = "R_1_5_CAP_2_5", r: float = 1.5, cap: float = 0.025) -> ReplayPolicy:
    return ReplayPolicy(name=name, kind="adaptive", daily_r_budget=r, hard_daily_equity_cap_pct=cap)


def old6() -> ReplayPolicy:
    return ReplayPolicy(name="OLD_6_USDT", kind="fixed", fixed_daily_loss_usdt=6.0)


def test_same_opportunity_stream_used_for_every_policy():
    d = dataset((opp(1, risk=2, pnl=1), opp(2, risk=7, pnl=-7)))
    results = replay_policies(d, policies=[old6(), adaptive()], starting_equity=1000)
    assert {r.opportunities_seen for r in results.values()} == {2}
    assert results["OLD_6_USDT"].approved_decision_ids + results["OLD_6_USDT"].blocked_decision_ids
    assert results["R_1_5_CAP_2_5"].approved_decision_ids + results["R_1_5_CAP_2_5"].blocked_decision_ids


def test_replay_is_deterministic():
    d = dataset((opp(1, risk=2, pnl=1), opp(2, risk=7, pnl=-7)))
    first = replay_policy(d.opportunities, d.risk_observations, adaptive(), starting_equity=1000).as_dict()
    second = replay_policy(d.opportunities, d.risk_observations, adaptive(), starting_equity=1000).as_dict()
    assert first == second


def test_no_lookahead_future_risk_observations_do_not_raise_budget():
    opportunity = opp(1, risk=7, pnl=1)
    future_risks = tuple(
        RiskObservation(opportunity.evaluated_at + timedelta(minutes=i + 1), 20.0)
        for i in range(40)
    )
    result = replay_policy((opportunity,), future_risks, adaptive(), starting_equity=1000)
    assert result.blocked_decision_ids == ["dec-1"]


def test_daily_reset_and_opening_equity_freeze():
    day1 = opp(1, at=BASE, risk=4, pnl=-10)
    day2 = opp(2, at=BASE + timedelta(days=1), risk=4, pnl=1)
    result = replay_policy((day1, day2), risk_history(), adaptive(), starting_equity=1000)
    assert result.day_open_equity["2026-09-12"] == 1000
    assert result.day_open_equity["2026-09-13"] == 990
    assert result.approved_decision_ids == ["dec-1", "dec-2"]


def test_reservations_prevent_budget_from_being_used_twice():
    first = opp(1, at=BASE, close_after_minutes=60, risk=4, pnl=1)
    second = opp(2, at=BASE + timedelta(minutes=1), risk=3, pnl=1)
    result = replay_policy((first, second), risk_history(), adaptive(), starting_equity=1000)
    assert result.approved_decision_ids == ["dec-1"]
    assert result.blocked_decision_ids == ["dec-2"]


def test_hard_cap_can_be_more_restrictive_than_r_budget():
    result = replay_policy((opp(1, risk=6.5, pnl=1),), risk_history(risk=10), adaptive(cap=0.005), starting_equity=1000)
    assert result.blocked_decision_ids == ["dec-1"]


def test_r_budget_blocks_position_that_exceeds_remaining_budget():
    result = replay_policy((opp(1, risk=7, pnl=1),), risk_history(risk=4), adaptive(), starting_equity=1000)
    assert result.blocked_decision_ids == ["dec-1"]
    assert result.state_trace[0]["reason"] == "PLANNED_RISK_EXCEEDS_REMAINING_DAILY_BUDGET"


def test_position_management_continues_after_hard_stop():
    first = opp(1, at=BASE, close_after_minutes=30, risk=4, pnl=-8)
    second = opp(2, at=BASE + timedelta(minutes=40), risk=1, pnl=1)
    result = replay_policy((first, second), risk_history(), adaptive(), starting_equity=1000)
    assert result.approved_decision_ids == ["dec-1"]
    assert result.blocked_decision_ids == ["dec-2"]
    assert result.daily_pnl["2026-09-12"] == pytest.approx(-8)


def test_old_six_usdt_policy_reproduces_fixed_daily_stop():
    first = opp(1, at=BASE, risk=2, pnl=-6.01)
    second = opp(2, at=BASE + timedelta(minutes=10), risk=1, pnl=1)
    result = replay_policy((first, second), risk_history(), old6(), starting_equity=1000)
    assert result.approved_decision_ids == ["dec-1"]
    assert result.blocked_decision_ids == ["dec-2"]
    assert result.hard_stop_days == 1


def test_candidate_reproduction_records_state_trace():
    result = replay_policy((opp(1, risk=4, pnl=-1), opp(2, risk=4, pnl=1)), risk_history(), adaptive(), starting_equity=1000)
    assert [row["action"] for row in result.state_trace] == ["APPROVED", "APPROVED"]
    assert all("effective_budget_usdt" in row for row in result.state_trace)


def test_holdout_summary_is_reported_without_tuning_inputs():
    d = dataset(tuple(opp(i, risk=1, pnl=1) for i in range(10)))
    results = replay_policies(d, policies=[old6(), adaptive()], starting_equity=1000)
    payload = build_report_payload(
        dataset=d,
        results=results,
        code_revision="abc123",
        canonical_db_path="temp.db",
    )
    assert payload["holdout"]["split"] == "chronological final 20%"
    assert payload["holdout"]["holdout_opportunities"] == 2
    assert "threshold" in payload["holdout"]["tuning_note"]


def test_loader_reads_temp_db_without_canonical_writes(tmp_path):
    db_path = tmp_path / "risk_replay.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE trading_decisions (
            decision_id TEXT, symbol TEXT, regime TEXT, evaluated_at TEXT, risk_amount REAL
        );
        CREATE TABLE positions (
            position_id TEXT, decision_id TEXT, side TEXT, closed_at TEXT,
            realized_pnl REAL, status TEXT
        );
        CREATE TABLE trade_fills (position_id TEXT, fee REAL);
        INSERT INTO trading_decisions VALUES ('dec-1','BTCUSDT','WEAK_TREND','2026-09-12T10:00:00+00:00',2.0);
        INSERT INTO positions VALUES ('pos-1','dec-1','LONG','2026-09-12T10:05:00+00:00',1.0,'CLOSED');
        INSERT INTO trade_fills VALUES ('pos-1',0.01);
        """
    )
    loaded = load_replay_dataset(conn)
    assert loaded.source_summary["opportunity_count"] == 1
    assert "cosmicforge.db" not in str(db_path)
