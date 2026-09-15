"""Phase 13 §13.2, §13.8, §13.12 — the production brain, driven by a clock.

These run the *real* ``PaperRunner`` over historical data. There is no replay
strategy, no replay sizing and no replay exit engine: if any of those existed,
the results would describe them instead of the system.
"""
from __future__ import annotations

import math
import os
import shutil
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from app.replay.engine import ReplaySession, restore_position_state_from_replay_evidence
from app.replay.historical_provider import TIMEFRAME_MS
from app.runner.models import SymbolState
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import ORGANIC_PROVENANCE, REPLAY
from shared_lib.persistence.migrations import migrate

M1 = TIMEFRAME_MS["1m"]
ANCHOR = 1_700_000_000_000 - (1_700_000_000_000 % TIMEFRAME_MS["15m"])


def candle(open_time, o, c):
    return [open_time, f"{o:.4f}", f"{max(o, c) * 1.0015:.4f}",
            f"{min(o, c) * 0.9985:.4f}", f"{c:.4f}", "1000",
            open_time + M1 - 1, "0", 0, "0", "0", "0"]


def market(*, warmup: int = 150, rise: int = 39, fall: int = 25) -> dict:
    """A deterministic market that opens, runs to TP1, trails, then stops out.

    The warm-up length is not arbitrary: the ensemble's regime classifier needs
    100 candles before it will classify at all, and the rise is deliberately
    gentle so the regime gate does not (correctly) refuse a STRONG_TREND.
    """
    rows = []
    price = 100.0
    for i in range(warmup):
        nxt = 100.0 + 100.0 * 0.0001 * i + 100.0 * 0.004 * math.sin(i / 7.0)
        rows.append(candle(ANCHOR + i * M1, price, nxt))
        price = nxt
    base = price
    for i in range(1, rise + 1):
        nxt = base * (1 + 0.0009 * i)
        rows.append(candle(rows[-1][0] + M1, price, nxt))
        price = nxt
    peak = price
    for i in range(1, fall + 1):
        nxt = peak * (1 - 0.0022 * i)
        rows.append(candle(rows[-1][0] + M1, price, nxt))
        price = nxt

    def aggregate(src, factor):
        out = []
        for i in range(0, len(src) - factor + 1, factor):
            chunk = src[i:i + factor]
            out.append([
                chunk[0][0], chunk[0][1],
                f"{max(float(x[2]) for x in chunk):.4f}",
                f"{min(float(x[3]) for x in chunk):.4f}",
                chunk[-1][4], "1000", chunk[-1][6], "0", 0, "0", "0", "0",
            ])
        return out

    return {"BTCUSDT": {"1m": rows, "15m": aggregate(rows, 15)}}


def two_symbol_market() -> dict:
    btc = market(warmup=120, rise=8, fall=4)["BTCUSDT"]
    eth_1m = []
    for row in btc["1m"]:
        scaled = list(row)
        for idx in (1, 2, 3, 4):
            scaled[idx] = f"{float(row[idx]) * 0.04:.4f}"
        eth_1m.append(scaled)
    eth_15m = []
    for row in btc["15m"]:
        scaled = list(row)
        for idx in (1, 2, 3, 4):
            scaled[idx] = f"{float(row[idx]) * 0.04:.4f}"
        eth_15m.append(scaled)
    return {"BTCUSDT": btc, "ETHUSDT": {"1m": eth_1m, "15m": eth_15m}}


def controlled(strategy):
    """Pin the ensemble's *component votes*, nothing else.

    §13.2 forbids a replay-only strategy, so this is the real
    MasterEnsembleStrategy with deterministic experts underneath it. Every gate
    above the votes -- regime, threshold, TradingDecisionEngine, HTF, risk,
    feasibility -- is untouched and still runs.
    """
    import sys
    from pathlib import Path

    # The controlled-component installer lives with the Phase 12 harness at the
    # repository root, deliberately outside `app` so nothing in the production
    # import graph can reach it. Importing it here beats duplicating it.
    repo_root = str(Path(__file__).resolve().parents[3])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from scripts.phase12.controlled import install_controlled_opportunity

    return install_controlled_opportunity(strategy, confidence=0.95)


@contextmanager
def replay_database(name: str):
    """One coherent database for the whole run.

    Several production components construct a bare ``DB()``, which resolves
    through ``DATABASE_URL``. An in-memory session database would leave those
    components writing somewhere else entirely, so the file is real and the
    environment points at it -- the same single-database arrangement production
    has.
    """
    directory = tempfile.mkdtemp(prefix=f"replay_{name}_")
    path = os.path.join(directory, "replay.db")
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = "sqlite:///" + path.replace("\\", "/")
    try:
        db = DB(path)
        migrate(db)
        yield db
    finally:
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous
        shutil.rmtree(directory, ignore_errors=True)


def run_replay(*, bot: str, hook=None, series=None, warmup=150):
    with replay_database(bot) as db:
        session = ReplaySession(
            db, series or market(warmup=warmup), bot_instance_id=bot,
            symbol="BTCUSDT", timeframe="1m", higher_timeframe="15m",
            capital_budget=10_000.0, position_allocation=1_000.0,
            strategy_hook=hook,
        )
        return session, session.run()


# ══════════════════════════════════════════════════════════════════════════
# §13.2 — the production brain
# ══════════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module")
def unmodified():
    """The real ensemble, unassisted, over the same market.

    The kline observations are collected here rather than in a second run: the
    leakage proof is about this run, and replaying 214 bars through the real
    ensemble twice to assert it would be waste.
    """
    seen: list[tuple[int, int]] = []
    with replay_database("unmodified") as db:
        session = ReplaySession(
            db, market(), bot_instance_id="bot_replay_unmodified",
            symbol="BTCUSDT", timeframe="1m", higher_timeframe="15m",
            capital_budget=10_000.0, position_allocation=1_000.0,
        )
        session.build()
        original = session.client.klines

        def watched(symbol, interval=None, limit=250, **kw):
            rows = original(symbol, interval, limit, **kw)
            if rows:
                seen.append((int(rows[-1][6]), session.clock.now_ms))
            return rows

        session.client.klines = watched
        result = session.run()
    return session, result, seen


def test_replay_drives_the_production_runner(unmodified):
    session, result, _ = unmodified
    from app.runner.runner import PaperRunner
    from app.strategy.master_ensemble import MasterEnsembleStrategy

    assert isinstance(session.runner, PaperRunner)
    assert isinstance(session.runner.strategy, MasterEnsembleStrategy)
    assert result.evaluations > 0


def test_every_closed_candle_becomes_a_canonical_decision(unmodified):
    _, result, _ = unmodified
    assert len(result.decisions) == result.evaluations


def test_replay_evidence_can_never_be_mistaken_for_organic(unmodified):
    _, result, _ = unmodified
    provenances = {str(d["provenance"]) for d in result.decisions}
    assert provenances == {REPLAY}
    assert REPLAY not in ORGANIC_PROVENANCE


def test_replay_reaches_no_broker(unmodified):
    _, result, _ = unmodified
    assert result.blocked_broker_calls == []


def test_replay_produces_production_reason_codes(unmodified):
    """The reasons are the ones production emits, not replay inventions."""
    _, result, _ = unmodified
    reasons = {d["primary_reason"] for d in result.decisions}
    assert reasons, "no decisions recorded"
    # The ensemble's own warm-up and gate vocabulary, unchanged.
    assert reasons & {"NO_OPPORTUNITY", "EXECUTION_DATA_STALE",
                      "ENTRY_CONFIDENCE_BELOW_THRESHOLD", "REGIME_BLOCKED"}


def test_the_unassisted_ensemble_is_not_forced_to_trade(unmodified):
    """No thresholds were lowered to manufacture a result."""
    _, result, _ = unmodified
    assert all(d["final_action"] != "APPROVED" or d["primary_reason"]
               == "APPROVED_FOR_EXECUTION" for d in result.decisions)


# ══════════════════════════════════════════════════════════════════════════
# §13.8 — position lifecycle parity
# ══════════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module")
def lifecycle():
    return run_replay(bot="bot_replay_lifecycle", hook=controlled)


def test_the_full_lifecycle_runs_in_replay(lifecycle):
    _, result = lifecycle
    events = [e["event_type"] for e in result.position_events]

    assert events[0] == "OPENED"
    assert "TP1" in events
    assert "BREAK_EVEN_ACTIVATED" in events
    assert "TRAILING_ACTIVATED" in events
    assert "STOP_UPDATED" in events
    assert events[-1] == "FINAL_CLOSE"


def test_the_accounting_invariant_holds(lifecycle):
    """open qty - partial closes - final close = 0."""
    _, result = lifecycle
    assert result.accounting_residual() == pytest.approx(0.0, abs=1e-9)

    (position,) = result.positions
    assert position["remaining_qty"] == pytest.approx(0.0, abs=1e-9)
    assert position["realized_qty"] == pytest.approx(position["original_qty"])
    assert position["status"] == "CLOSED"


def test_tp1_leaves_exactly_the_remainder(lifecycle):
    _, result = lifecycle
    (position,) = result.positions
    tp1 = next(e for e in result.position_events if e["event_type"] == "TP1")

    assert tp1["quantity"] == pytest.approx(position["original_qty"] / 2, rel=1e-6)
    assert tp1["remaining_qty"] == pytest.approx(
        position["original_qty"] - tp1["quantity"], rel=1e-9
    )


def test_break_even_and_trailing_act_on_the_post_tp1_remainder(lifecycle):
    _, result = lifecycle
    tp1 = next(e for e in result.position_events if e["event_type"] == "TP1")
    for event_type in ("BREAK_EVEN_ACTIVATED", "TRAILING_ACTIVATED"):
        event = next(e for e in result.position_events if e["event_type"] == event_type)
        assert event["remaining_qty"] == pytest.approx(tp1["remaining_qty"])


def test_replay_restart_after_tp1_restores_the_remainder_not_original_qty(lifecycle):
    _, result = lifecycle
    (position,) = result.positions
    through_tp1 = []
    for event in result.position_events:
        through_tp1.append(event)
        if event["event_type"] == "TP1":
            break

    restored = restore_position_state_from_replay_evidence(position, through_tp1)
    tp1 = through_tp1[-1]

    assert restored.phase == "TP1_TAKEN"
    assert restored.remaining_qty == pytest.approx(tp1["remaining_qty"])
    assert restored.remaining_qty < restored.original_qty
    assert restored.realized_qty == pytest.approx(restored.original_qty - restored.remaining_qty)


def test_the_trailing_stop_only_moves_in_one_direction(lifecycle):
    _, result = lifecycle
    stops = [
        float(e["stop_price"]) for e in result.position_events
        if e["event_type"] in {"BREAK_EVEN_ACTIVATED", "STOP_UPDATED"}
        and e["stop_price"] is not None
    ]
    assert len(stops) >= 2
    assert stops == sorted(stops), "a long's trailing stop must never fall"


def test_the_fills_match_the_lifecycle(lifecycle):
    _, result = lifecycle
    actions = [f["action"] for f in result.fills]
    assert actions == ["OPEN", "PARTIAL_CLOSE", "CLOSE"]

    opened, partial, closed = result.fills
    assert partial["qty"] + closed["qty"] == pytest.approx(opened["qty"], rel=1e-9)


def test_costs_are_actually_charged(lifecycle):
    _, result = lifecycle
    (position,) = result.positions
    assert float(position["fees"] or 0.0) > 0.0, "a replay with fees must charge them"


# ══════════════════════════════════════════════════════════════════════════
# §13.12 — determinism
# ══════════════════════════════════════════════════════════════════════════


#: Determinism does not need a full lifecycle, only identical inputs. The
#: warm-up still has to clear the ensemble's 100-candle regime minimum.
SHORT_MARKET = dict(warmup=110, rise=18, fall=10)


@pytest.fixture(scope="module")
def determinism_pair():
    first = run_replay(bot="bot_determinism_a", hook=controlled,
                       series=market(**SHORT_MARKET))
    second = run_replay(bot="bot_determinism_b", hook=controlled,
                        series=market(**SHORT_MARKET))
    return first[1], second[1]


def test_the_same_replay_twice_produces_identical_results(determinism_pair):
    """Same dataset, revision, policy, costs, fill model and seed."""
    first, second = determinism_pair

    assert first.outcome_fingerprint() == second.outcome_fingerprint()
    assert first.evaluations == second.evaluations
    assert len(first.decisions) == len(second.decisions)
    assert len(first.position_events) == len(second.position_events)
    assert first.realized_pnl == pytest.approx(second.realized_pnl)
    assert first.total_fees == pytest.approx(second.total_fees)


def test_a_different_market_produces_a_different_result(determinism_pair):
    """The fingerprint must actually be sensitive to the inputs."""
    baseline, _ = determinism_pair
    _, altered = run_replay(
        bot="bot_sensitivity", hook=controlled,
        series=market(warmup=110, rise=25, fall=10),
    )
    assert baseline.outcome_fingerprint() != altered.outcome_fingerprint()


def test_the_manifest_pins_what_the_run_depended_on(lifecycle):
    session, result = lifecycle
    manifest = result.manifest

    assert manifest["provenance"] == REPLAY
    assert manifest["dataset_hash"]
    assert manifest["cost_model_hash"]
    assert manifest["fill_model"]
    assert manifest["intrabar_policy"]
    assert manifest["policy_hash"] not in (None, "", "unresolved")
    assert manifest["replay_hash"]


# ══════════════════════════════════════════════════════════════════════════
# No future leakage, through the whole production stack
# ══════════════════════════════════════════════════════════════════════════


def test_the_production_strategy_cannot_see_past_the_replay_clock(unmodified):
    """Integration-level: every kline the runner received was already closed."""
    _, _, seen = unmodified

    assert seen, "the strategy never asked for candles"
    for newest_close, clock in seen:
        assert newest_close <= clock, (
            f"a candle closing at {newest_close} was served at clock {clock}"
        )


def test_the_higher_timeframe_never_leads_the_strategy_candle():
    with replay_database("htf") as db:
        session = ReplaySession(
            db, market(), bot_instance_id="bot_htf_check",
            symbol="BTCUSDT", timeframe="1m", higher_timeframe="15m",
        )
        # The provider alone is enough here: no runner, no cycles, just the
        # cut. Stepping and checking must happen together, because the check
        # is about what was visible *at that step*.
        checked = 0
        for close_time in session.provider.step("BTCUSDT", "1m"):
            snapshot = session.provider.build_snapshot(
                "BTCUSDT", "1m", higher_timeframe="15m",
            )
            assert snapshot.htf_is_timestamp_aligned()
            if snapshot.higher_timeframe_closed_candle_time is not None:
                assert snapshot.higher_timeframe_closed_candle_time <= close_time
            checked += 1
    assert checked > 100


# ══════════════════════════════════════════════════════════════════════════
# Final closure blockers — multi-symbol and calendar parity
# ══════════════════════════════════════════════════════════════════════════


def test_multi_symbol_replay_uses_one_session_identity_and_deterministic_schedule():
    with replay_database("multi_symbol_identity") as db:
        session = ReplaySession(
            db, two_symbol_market(), bot_instance_id="bot_multi_identity",
            symbols=("ETHUSDT", "BTCUSDT"), timeframe="1m", higher_timeframe="15m",
        )
        times = session._evaluation_times()
        assert times == tuple(sorted(times))
        assert len(times) == len(set(times)), "same-timestamp BTC/ETH bars share one cycle"

        session.build()
        assert tuple(session.runner.trade_symbols) == ("BTCUSDT", "ETHUSDT")
        manifest = session.identity().manifest()
        assert tuple(manifest["symbols"]) == ("BTCUSDT", "ETHUSDT")


def test_production_position_capacity_counts_positions_across_symbols():
    from app.decision.reasons import RiskReason
    from app.execution.position_slots import evaluate_slot

    with replay_database("slot_capacity") as db:
        bot = "bot_slot_capacity"
        with db.connect() as conn:
            conn.execute(
                "INSERT INTO positions (position_id, bot_instance_id, symbol, side, "
                "provenance, original_qty, remaining_qty, realized_qty, opened_at, status) "
                "VALUES ('pos_btc', ?, 'BTCUSDT', 'LONG', 'REPLAY', 1, 1, 0, "
                "'2026-01-01T00:00:00+00:00', 'OPEN')",
                (bot,),
            )
            conn.execute(
                "INSERT INTO positions (position_id, bot_instance_id, symbol, side, "
                "provenance, original_qty, remaining_qty, realized_qty, opened_at, status) "
                "VALUES ('pos_eth', ?, 'ETHUSDT', 'LONG', 'REPLAY', 1, 1, 0, "
                "'2026-01-01T00:00:00+00:00', 'OPEN')",
                (bot,),
            )

        verdict = evaluate_slot(db, bot, "SOLUSDT", "LONG", max_slots=2)

    assert not verdict.allowed
    assert verdict.reason == RiskReason.MAX_OPEN_POSITIONS
    assert verdict.occupied == ("BTCUSDT", "ETHUSDT")


def test_production_correlation_filter_blocks_second_same_direction_correlated_symbol():
    from app.risk.correlation_filter import CorrelationFilter

    blocked, reason = CorrelationFilter().should_block(
        "ETHUSDT", "LONG", {"BTCUSDT": "LONG"},
    )

    assert blocked
    assert "correlation_block" in reason
    assert "BTCUSDT" in reason


def test_historical_daily_close_uses_replay_clock_and_is_idempotent(monkeypatch):
    from app.execution.position_manager import PositionSide

    series = two_symbol_market()
    with replay_database("daily_close_clock") as db:
        session = ReplaySession(
            db, series, bot_instance_id="bot_daily_close_clock",
            symbols=("BTCUSDT", "ETHUSDT"), timeframe="1m", higher_timeframe="15m",
        )
        runner = session.build()
        close_ts = int(series["BTCUSDT"]["1m"][110][6])
        session.clock.advance_to(close_ts)
        local = datetime.fromtimestamp(close_ts / 1000.0, timezone.utc)
        monkeypatch.setattr("app.runner.runner.settings.DAILY_CLOSE_ENABLED", True)
        monkeypatch.setattr("app.runner.runner.settings.DAILY_CLOSE_TIMEZONE", "UTC")
        monkeypatch.setattr(
            "app.runner.runner.settings.DAILY_CLOSE_WINDOW_START",
            f"{local.hour:02d}:{local.minute:02d}",
        )
        monkeypatch.setattr(
            "app.runner.runner.settings.DAILY_CLOSE_WINDOW_END",
            f"{local.hour:02d}:{(local.minute + 1) % 60:02d}",
        )
        monkeypatch.setattr("app.runner.runner.settings.DAILY_CLOSE_MIN_PROFIT_USDT", 0.0)
        monkeypatch.setattr("app.runner.runner.settings.DAILY_CLOSE_MIN_PROFIT_PCT", 0.0)

        runner.state["BTCUSDT"] = SymbolState(
            position="LONG", entry_price=90.0, entry_qty=2.0,
            position_id="pos_daily_close",
        )
        runner.position_manager.open_position(
            "BTCUSDT", PositionSide.LONG, position_id="pos_daily_close",
            entry_price=90.0, qty=2.0, stop_price=80.0,
            tp1_price=100.0, tp2_price=110.0,
        )
        closed_qty = []

        def fake_close(symbol: str, reason: str):
            pos = runner.position_manager.get_position(symbol)
            closed_qty.append(float(pos.current_qty))
            runner.position_manager.close_position(symbol, reason)
            runner.state[symbol].position = "NONE"
            runner.state[symbol].entry_qty = 0.0
            return {"success": True}

        runner._close_managed_position = fake_close

        assert runner._run_daily_close_from_cycle() == 1
        assert runner._run_daily_close_from_cycle() == 0
        assert closed_qty == [2.0]


def test_historical_day_and_week_boundaries_use_replay_clock():
    from app.risk.state import get_week_start

    with replay_database("calendar_boundaries") as db:
        session = ReplaySession(
            db, two_symbol_market(), bot_instance_id="bot_calendar_boundaries",
            symbols=("BTCUSDT", "ETHUSDT"), timeframe="1m", higher_timeframe="15m",
        )
        runner = session.build()
        sunday = int(datetime(2026, 9, 13, 21, 59, tzinfo=timezone.utc).timestamp() * 1000)
        monday_rome = int(datetime(2026, 9, 13, 22, 1, tzinfo=timezone.utc).timestamp() * 1000)
        monday_utc = int(datetime(2026, 9, 14, 0, 1, tzinfo=timezone.utc).timestamp() * 1000)

        session.clock.advance_to(sunday)
        before = runner.daily_budget_engine.risk_date_for(runner._now_utc())
        before_week = get_week_start(runner._today())

        session.clock.advance_to(monday_rome)
        after = runner.daily_budget_engine.risk_date_for(runner._now_utc())
        session.clock.advance_to(monday_utc)
        after_week = get_week_start(runner._today())

        assert str(before) == "2026-09-13"
        assert str(after) == "2026-09-14"
        assert before_week != after_week
