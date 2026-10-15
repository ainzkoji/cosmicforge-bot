"""Step 3.10: per-account risk brakes (``pipeline.brakes``): exact thresholds, the frozen evaluator's semantics,
persistence across a restart of the process, and the explicit restart of a stopped account.

The reference arithmetic in this file is written out again from ``research/evaluator/simulator.py`` ("mark to
the close; equity and brakes"); nothing private of the simulator is imported.
"""
from __future__ import annotations

import contextlib
import inspect
import json
import math
import random
import sqlite3
import subprocess
import sys
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.trading_intelligence.families.daily_trend import targets as frozen
from app.trading_intelligence.pipeline import brakes as B
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.cati_portfolio_schema import BRAKE_EVENT_TABLE, BRAKE_RESTART_TABLE, BRAKE_STATE_TABLE
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate
from shared_lib.risk_levels import LEVELS, get_profile

INITIAL = 10_000.0
FIRST_DAY = date(2026, 1, 1)
NOW = 1_780_000_000_000


@pytest.fixture(scope="module")
def template(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("step3_brakes") / "template.db")
    database = DB(path)
    migrate(database)
    ensure_cati_platform_schema(database)
    return path


@pytest.fixture
def db(template, tmp_path):
    target = str(tmp_path / "brakes.db")
    with contextlib.closing(sqlite3.connect(template)) as source, contextlib.closing(sqlite3.connect(target)) as copy:
        source.backup(copy)
    return DB(target)


def day(index: int) -> str:
    return (FIRST_DAY + timedelta(days=index)).isoformat()


def close(db, index, equity, *, level="balanced", account="acct-1", scope=B.ACCOUNT_SCOPE, transfers=0.0, initial=INITIAL):
    return B.evaluate(db, account_id=account, scope=scope, risk_day=day(index), day_definition=B.UTC_DAY,
                      closing_equity=equity, thresholds=B.BrakeThresholds.from_profile(level), now_ms=NOW + index,
                      initial_equity=initial, net_transfers=transfers)


def evaluator_flags(path, level_name, initial=INITIAL):
    """simulator.py, step 6, on a given series of closing equities: ``(paused, halve, halted)`` per day."""
    level = frozen.risk_level(level_name)
    equity = peak = initial
    halted = False
    out = []
    for closing in path:
        start_equity = equity
        equity = closing
        ret = equity / start_equity - 1.0 if start_equity > 0 else 0.0
        peak = max(peak, equity)
        drawdown = equity / peak - 1.0 if peak > 0 else 0.0
        paused = ret <= -level.daily_pause
        halve = drawdown <= -level.halve_drawdown
        if not halted and drawdown <= -level.stop_drawdown:
            halted = True
        if equity <= 0 and not halted:
            halted = True
        out.append((paused, halve, halted))
    return out


def run(db, path, level, account="acct-1"):
    with db.scope():                                                 # one connection for the whole path
        return [close(db, i, equity, level=level, account=account) for i, equity in enumerate(path)]


def rows(db, sql, *params):
    with db.connect() as conn:
        return [tuple(r) for r in conn.execute(sql, params).fetchall()]


# ------------------------------------------------------------------------------------------------- thresholds
@pytest.mark.parametrize("level", LEVELS)
def test_thresholds_are_the_shared_profile_and_equal_the_frozen_mandate(level):
    got, profile, family = B.BrakeThresholds.from_profile(level), get_profile(level), frozen.risk_level(level)
    assert (got.daily_pause, got.halve_drawdown, got.stop_drawdown) == (
        float(profile.daily_loss_pause_pct) / 100, float(profile.drawdown_reduce_pct) / 100, float(profile.drawdown_stop_pct) / 100)
    assert (got.daily_pause, got.halve_drawdown, got.stop_drawdown) == (family.daily_pause, family.halve_drawdown, family.stop_drawdown)
    assert (got.risk_level, got.profile_version) == (profile.level, profile.version)


def test_thresholds_cannot_be_disordered_or_non_positive():
    good = B.BrakeThresholds.from_profile("balanced")
    for change in ({"daily_pause": 0.0}, {"halve_drawdown": -0.1}, {"stop_drawdown": good.halve_drawdown}, {"daily_pause": math.nan}):
        with pytest.raises(B.BrakeError):
            B.BrakeThresholds(**{**good.__dict__, **change})


def test_the_production_module_has_no_switch_that_turns_a_brake_off():
    for function in (B.evaluate, B.brake_flags, B.close_measures, B.authorize_restart):
        names = set(inspect.signature(function).parameters)
        assert not {n for n in names if "brake" in n or "enable" in n or "disable" in n or "skip" in n}
    assert "drawdown_brakes" not in "".join(inspect.signature(f).__str__() for f in (B.evaluate, B.brake_flags))


@pytest.mark.parametrize("level", LEVELS)
@pytest.mark.parametrize("brake", ["paused", "halve", "halted"])
def test_each_brake_fires_at_its_exact_threshold_and_not_one_ulp_inside(level, brake):
    t = B.BrakeThresholds.from_profile(level)
    threshold = {"paused": t.daily_pause, "halve": t.halve_drawdown, "halted": t.stop_drawdown}[brake]

    def flag(value):
        measures = {"daily_return": 0.0, "drawdown": 0.0}
        measures["daily_return" if brake == "paused" else "drawdown"] = value
        return getattr(B.brake_flags(closing_equity=1.0, thresholds=t, halted_before=False, **measures), brake)

    assert flag(-threshold) is True                                   # at the threshold: <=
    assert flag(math.nextafter(-threshold, -math.inf)) is True        # one ulp beyond it
    assert flag(math.nextafter(-threshold, 0.0)) is False             # one ulp inside it
    assert flag(0.0) is False and flag(-1.0) is True


def boundary_equity(threshold, basis=INITIAL):
    """The largest closing equity at which the evaluator's measure is at or below minus the threshold."""
    equity = basis * (1.0 - threshold)
    while equity / basis - 1.0 <= -threshold:
        equity = math.nextafter(equity, math.inf)
    while equity / basis - 1.0 > -threshold:
        equity = math.nextafter(equity, -math.inf)
    return equity


@pytest.mark.parametrize("level", LEVELS)
@pytest.mark.parametrize("brake", ["paused", "halve", "halted"])
def test_evaluate_crosses_each_threshold_between_two_adjacent_equities(db, level, brake):
    t = B.BrakeThresholds.from_profile(level)
    threshold = {"paused": t.daily_pause, "halve": t.halve_drawdown, "halted": t.stop_drawdown}[brake]
    at = boundary_equity(threshold)
    assert abs(at - INITIAL * (1.0 - threshold)) <= 4 * math.ulp(at)
    for name, equity, expected in (("at", at, True), ("beyond", math.nextafter(at, -math.inf), True),
                                   ("inside", math.nextafter(at, math.inf), False)):
        decision = close(db, 0, equity, level=level, account=f"{level}-{brake}-{name}")
        assert getattr(decision, brake) is expected
        assert decision.flags == B.BrakeFlags(*evaluator_flags([equity], level)[0])
        assert decision.stop_fired is decision.halted


def test_the_stop_fires_on_exact_equality_where_the_threshold_is_representable(db):
    stop = B.BrakeThresholds.from_profile("aggressive").stop_drawdown
    assert stop == 0.25                                              # a power of two: equality is reachable in binary
    exact = close(db, 0, 75.0, level="aggressive", account="equal", initial=100.0)
    assert exact.state.drawdown == -stop and exact.halted and exact.stop_fired
    inside = close(db, 0, math.nextafter(75.0, math.inf), level="aggressive", account="inside", initial=100.0)
    assert inside.state.drawdown > -stop and not inside.halted and inside.halve


# ----------------------------------------------------------------------------------------------------- parity
def crafted_path(level_name):
    """Rise, a dip past the halve threshold, recovery, one bad day, then a slide through the stop and beyond."""
    level = frozen.risk_level(level_name)
    path, equity = [], INITIAL
    for change in ([0.004] * 5 + [-(level.halve_drawdown + 0.01)] + [0.003] * 3 + [level.halve_drawdown + 0.03] + [0.002] * 4
                   + [-(level.daily_pause + 0.001)] + [0.001] * 2 + [-level.daily_pause] * 3 + [-0.5 * level.stop_drawdown] * 3
                   + [0.02, -0.01, 0.15, 0.3, -0.02, 0.5]):
        equity *= 1.0 + change
        path.append(equity)
    return path


def random_path(seed, days=250):
    rng, equity, path = random.Random(seed), INITIAL, []
    for _ in range(days):
        equity *= 1.0 + rng.gauss(-0.0004, 0.012) + (rng.choice([-0.04, 0.05]) if rng.random() < 0.02 else 0.0)
        path.append(equity)
    return path


@pytest.mark.parametrize("level", LEVELS)
def test_a_crafted_equity_path_gives_the_frozen_evaluators_flags_day_by_day(db, level):
    path = crafted_path(level)
    expected = evaluator_flags(path, level)
    decisions = run(db, path, level)
    assert [(d.paused, d.halve, d.halted) for d in decisions] == expected
    paused, halve, halted = zip(*expected)
    assert any(paused) and not all(paused)
    assert (False, True) in zip(halve, halve[1:]) and (True, False) in zip(halve, halve[1:])     # the halve brake lifts again
    assert halted[-1] and not halted[0] and sum(d.stop_fired for d in decisions) == 1
    stop_index = halted.index(True)
    assert all(halted[stop_index:]) and max(path[stop_index:]) > path[stop_index] * 1.5        # terminal through a recovery


@pytest.mark.parametrize("level", LEVELS)
@pytest.mark.parametrize("seed", [3, 17, 2026])
def test_random_equity_paths_give_the_frozen_evaluators_flags(db, level, seed):
    path = random_path(seed)
    decisions = run(db, path, level)
    assert [(d.paused, d.halve, d.halted) for d in decisions] == evaluator_flags(path, level)
    peak = INITIAL
    for d, equity in zip(decisions, path):
        peak = max(peak, equity)
        assert d.state.highest_closing_equity == peak and d.state.drawdown == equity / peak - 1.0


# -------------------------------------------------------------------------------------------- frozen semantics
def test_the_daily_pause_holds_for_the_affected_decision_only(db):
    t = B.BrakeThresholds.from_profile("balanced")
    bad = close(db, 0, INITIAL * (1.0 - t.daily_pause - 0.001))
    assert bad.paused and not bad.halve and not bad.halted and not bad.new_entries_allowed and not bad.close_all_positions
    assert (bad.state.active_brake, bad.state.triggered_day) == (B.DAILY_LOSS_PAUSE, day(0))
    assert bad.state.daily_loss == pytest.approx(INITIAL * (t.daily_pause + 0.001))
    flat = close(db, 1, bad.state.closing_equity)
    assert not flat.paused and flat.new_entries_allowed and flat.state.active_brake == B.NONE
    assert (flat.state.reason, flat.state.triggered_at_ms, flat.state.daily_loss) == (None, None, 0.0)


def test_the_halve_brake_holds_while_the_drawdown_does_and_lifts_when_it_recovers(db):
    t = B.BrakeThresholds.from_profile("balanced")
    down = close(db, 0, INITIAL * (1.0 - t.halve_drawdown - 0.01))
    still = close(db, 1, down.state.closing_equity * 1.001)
    assert down.halve and still.halve and not still.paused and not still.halted
    assert (still.state.active_brake, still.state.triggered_day, still.state.triggered_at_ms) == (B.DRAWDOWN_HALVE, day(0), NOW)
    back = close(db, 2, INITIAL * (1.0 - t.halve_drawdown + 0.01))
    assert not back.halve and back.state.active_brake == B.NONE and back.state.highest_closing_equity == INITIAL
    types = [e["event_type"] for e in B.events(db, "acct-1")]
    assert types.count(B.EVENT_HALVE_ON) == 1 and types.count(B.EVENT_HALVE_OFF) == 1 and types.count(B.EVENT_EVALUATED) == 3


def test_the_terminal_stop_closes_everything_and_is_not_lifted_by_any_later_close(db):
    t = B.BrakeThresholds.from_profile("balanced")
    stop = close(db, 0, INITIAL * (1.0 - t.stop_drawdown - 0.001))
    assert stop.halted and stop.stop_fired and stop.close_all_positions and not stop.new_entries_allowed
    assert stop.state.active_brake == B.DRAWDOWN_STOP and stop.state.reason.startswith("DRAWDOWN_STOP|drawdown=")
    for index, equity in enumerate((INITIAL, INITIAL * 2, INITIAL * 3), start=1):
        later = close(db, index, equity)
        assert later.halted and not later.stop_fired and later.close_all_positions
        assert (later.state.active_brake, later.state.triggered_day, later.state.stop_seq) == (
            B.DRAWDOWN_STOP, day(0), stop.state.stop_seq)
    assert [e["event_type"] for e in B.events(db, "acct-1")].count(B.EVENT_STOP) == 1


def test_exhausted_equity_stops_the_account(db):
    wiped = close(db, 0, 0.0)
    assert wiped.halted and wiped.stop_fired
    gone = close(db, 0, -5.0, account="acct-2", level="aggressive")
    assert gone.halted and B.EVENT_STOP in [e["event_type"] for e in B.events(db, "acct-2")]
    crafted = B.brake_flags(daily_return=0.0, drawdown=0.0, closing_equity=0.0,
                            thresholds=B.BrakeThresholds.from_profile("balanced"), halted_before=False)
    assert crafted.halted and not crafted.halve


# ------------------------------------------------------------------------------------------------ persistence
def test_every_required_field_is_persisted(db):
    t = B.BrakeThresholds.from_profile("conservative")
    close(db, 0, INITIAL * 1.01, level="conservative")
    decision = close(db, 1, INITIAL * 1.01 * (1.0 - t.halve_drawdown - 0.001), level="conservative")
    state = B.current(db, "acct-1", B.ACCOUNT_SCOPE)
    assert state == decision.state
    assert state.risk_day == day(1) and state.first_day == day(0) and state.day_definition == B.UTC_DAY
    assert state.highest_closing_equity == INITIAL * 1.01 and state.opening_equity == INITIAL * 1.01
    assert state.drawdown == state.closing_equity / state.highest_closing_equity - 1.0 <= -t.halve_drawdown
    assert state.daily_loss == state.opening_equity - state.closing_equity > 0
    assert state.daily_return == state.closing_equity / state.opening_equity - 1.0
    assert (state.active_brake, state.triggered_at_ms, state.triggered_day) == (B.DRAWDOWN_HALVE, NOW + 1, day(1))
    assert state.reason.startswith("DRAWDOWN_HALVE|") and state.paused and state.halve and not state.halted
    assert (state.risk_level, state.profile_version, state.stop_drawdown) == (t.risk_level, t.profile_version, t.stop_drawdown)
    assert (state.restart_id, state.restart_by, state.restart_reference, state.restart_pending) == (None, None, None, False)
    assert state.evaluations == 2 and state.updated_at_ms == NOW + 1


def test_a_stopped_account_stays_stopped_after_the_process_restarts(db):
    t = B.BrakeThresholds.from_profile("balanced")
    close(db, 0, INITIAL * (1.0 - t.stop_drawdown - 0.001))
    reopened = DB(db.path)                                            # a new process: nothing but the database
    state = B.current(reopened, "acct-1", B.ACCOUNT_SCOPE)
    assert state.halted and state.flags.close_all_positions and not state.flags.new_entries_allowed
    assert B.stopped(reopened) == [("acct-1", B.ACCOUNT_SCOPE)]
    with reopened.connect() as conn:
        assert B.stopped_scopes(conn, "acct-1") == (B.ACCOUNT_SCOPE,)
        assert B.stopped_scopes(conn, "acct-1", ["family:daily_trend"]) == () and B.stopped_scopes(conn, "acct-2") == ()
    assert close(reopened, 1, INITIAL * 5).halted


#: another operating-system process: it knows the database file and nothing else
OTHER_PROCESS = """
import json, sys
sys.path[:0] = sys.argv[1:4]
import test_step3_brakes as T
db = T.DB(sys.argv[4])
state = T.B.current(db, "acct-1", T.B.ACCOUNT_SCOPE)
later = T.close(db, 1, T.INITIAL * 4)
print(json.dumps({"halted": state.halted, "day": state.triggered_day, "stopped": T.B.stopped(db),
                  "later_halted": later.halted, "later_fired": later.stop_fired}))
"""


def test_a_different_process_finds_the_account_stopped_and_cannot_trade_it_out_of_the_stop(db):
    t = B.BrakeThresholds.from_profile("balanced")
    close(db, 0, INITIAL * (1.0 - t.stop_drawdown - 0.001))
    tests_dir = Path(__file__).resolve().parent
    bot_backend = tests_dir.parents[1]
    done = subprocess.run([sys.executable, "-c", OTHER_PROCESS, str(bot_backend), str(bot_backend.parent / "shared"),
                           str(tests_dir), db.path], capture_output=True, text=True, timeout=120, cwd=str(bot_backend))
    assert done.returncode == 0, done.stderr
    seen = json.loads(done.stdout.strip().splitlines()[-1])
    assert seen == {"halted": True, "day": day(0), "stopped": [["acct-1", B.ACCOUNT_SCOPE]], "later_halted": True,
                    "later_fired": False}
    assert B.current(db, "acct-1", B.ACCOUNT_SCOPE).risk_day == day(1)       # what that process wrote is here too


def test_the_database_refuses_to_clear_or_delete_a_stop_without_a_restart_record(db):
    close(db, 0, INITIAL * 0.5)
    with pytest.raises(sqlite3.IntegrityError, match="restart record"):
        with db.connect() as conn:
            conn.execute(f"UPDATE {BRAKE_STATE_TABLE} SET halted=0 WHERE account_id='acct-1'")
    with pytest.raises(sqlite3.IntegrityError, match="restart record"):
        with db.connect() as conn:
            conn.execute(f"UPDATE {BRAKE_STATE_TABLE} SET halted=0, restart_id='rst_forged' WHERE account_id='acct-1'")
    with pytest.raises(sqlite3.IntegrityError, match="never deleted"):
        with db.connect() as conn:
            conn.execute(f"DELETE FROM {BRAKE_STATE_TABLE} WHERE account_id='acct-1'")
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with db.connect() as conn:
            conn.execute(f"UPDATE {BRAKE_STATE_TABLE} SET account_id='acct-9' WHERE account_id='acct-1'")
    assert B.current(db, "acct-1", B.ACCOUNT_SCOPE).halted


def test_the_brake_log_and_the_restart_records_are_append_only(db):
    close(db, 0, INITIAL * 0.5)
    B.authorize_restart(db, account_id="acct-1", scope=B.ACCOUNT_SCOPE, authorized_by="user-7", role=B.USER,
                        reference="ticket-1", now_ms=NOW + 10)
    for table in (BRAKE_EVENT_TABLE, BRAKE_RESTART_TABLE):
        for statement in (f"UPDATE {table} SET account_id='x'", f"DELETE FROM {table}"):
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                with db.connect() as conn:
                    conn.execute(statement)
    logged = B.events(db, "acct-1")
    assert [e["seq"] for e in logged] == sorted(e["seq"] for e in logged)
    assert logged[-1]["event_type"] == B.EVENT_RESTART_AUTHORIZED and logged[-1]["actor"] == "user-7"


# ----------------------------------------------------------------------------------------------- evaluation rules
def test_a_close_is_evaluated_once_and_days_only_move_forward(db):
    first = close(db, 3, INITIAL * 0.99)
    again = close(db, 3, INITIAL * 0.99)
    assert again.replayed and again.state == first.state and not first.replayed
    assert len(B.events(db, "acct-1")) == 2                           # opened + evaluated; the replay wrote nothing
    with pytest.raises(B.BrakeError) as conflict:
        close(db, 3, INITIAL * 0.98)
    assert conflict.value.code == "RISK_DAY_ALREADY_EVALUATED"
    with pytest.raises(B.BrakeError) as backwards:
        close(db, 2, INITIAL)
    assert backwards.value.code == "RISK_DAY_OUT_OF_ORDER"
    assert B.current(db, "acct-1", B.ACCOUNT_SCOPE) == first.state


def test_the_first_evaluation_needs_a_stated_basis_and_inputs_are_validated(db):
    thresholds = B.BrakeThresholds.from_profile("balanced")

    def evaluate(**changes):
        arguments = dict(account_id="acct-1", scope=B.ACCOUNT_SCOPE, risk_day=day(0), day_definition=B.UTC_DAY,
                         closing_equity=INITIAL, thresholds=thresholds, now_ms=NOW, initial_equity=INITIAL)
        arguments.update(changes)
        return B.evaluate(db, **arguments)

    for changes, code in (({"initial_equity": None}, "BRAKE_BASIS_REQUIRED"), ({"initial_equity": 0.0}, "BRAKE_BASIS_REQUIRED"),
                          ({"risk_day": "2026-13-40"}, "INVALID_ARGUMENT"), ({"closing_equity": math.inf}, "INVALID_ARGUMENT"),
                          ({"account_id": " "}, "INVALID_ARGUMENT"), ({"thresholds": None}, "INVALID_ARGUMENT"),
                          ({"day_definition": ""}, "DAY_DEFINITION_REQUIRED"), ({"day_definition": "Mars/Olympus"}, "DAY_DEFINITION_UNKNOWN")):
        with pytest.raises(B.BrakeError) as error:
            evaluate(**changes)
        assert error.value.code == code
    assert B.current(db, "acct-1", B.ACCOUNT_SCOPE) is None           # nothing was written by a refused call
    evaluate()
    with pytest.raises(B.BrakeError) as changed:
        evaluate(risk_day=day(1), day_definition="Europe/Rome")
    assert changed.value.code == "DAY_DEFINITION_CHANGED"


def ms(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


def test_the_risk_day_is_explicit_for_both_day_definitions():
    midnight = ms("2026-10-09T00:00:00")
    assert B.risk_day_closed_at(midnight, B.UTC_DAY) == "2026-10-08"          # the daily bar that just closed
    assert B.risk_day_containing(midnight, B.UTC_DAY) == "2026-10-09"
    assert B.risk_day_containing(midnight - 1, B.UTC_DAY) == "2026-10-08"
    late_evening_utc = ms("2026-10-08T22:30:00")                              # 00:30 on the 9th in Rome (UTC+2)
    assert B.risk_day_containing(late_evening_utc, B.UTC_DAY) == "2026-10-08"
    assert B.risk_day_containing(late_evening_utc, "Europe/Rome") == "2026-10-09"
    assert B.risk_day_closed_at(ms("2026-10-08T22:00:00"), "Europe/Rome") == "2026-10-08"
    assert B.risk_day_containing(ms("2026-01-15T23:30:00"), "Europe/Rome") == "2026-01-16"      # UTC+1 in winter
    with pytest.raises(B.BrakeError):
        B.risk_day_containing(midnight, "Not/AZone")


def test_external_transfers_are_neither_a_gain_nor_a_loss(db):
    t = B.BrakeThresholds.from_profile("balanced")
    close(db, 0, INITIAL)
    withdrawn = close(db, 1, INITIAL - 4_000.0, transfers=-4_000.0)           # 40 % taken out, nothing lost
    assert (withdrawn.state.daily_return, withdrawn.state.drawdown, withdrawn.state.daily_loss) == (0.0, 0.0, 0.0)
    assert withdrawn.flags == B.NO_BRAKE and withdrawn.state.highest_closing_equity == 6_000.0
    deposited = close(db, 2, 6_000.0 + 3_000.0, transfers=3_000.0)            # a deposit is not a new high from trading
    assert (deposited.state.daily_return, deposited.state.highest_closing_equity) == (0.0, 9_000.0)
    lost = close(db, 3, 9_000.0 * (1.0 - t.halve_drawdown - 0.001))
    assert lost.halve and lost.state.drawdown == lost.state.closing_equity / 9_000.0 - 1.0
    unflagged = close(db, 1, INITIAL - 4_000.0, account="acct-2")
    assert unflagged.halted                                                   # the same move as a trading loss stops


def test_scopes_and_accounts_do_not_share_state(db):
    t = B.BrakeThresholds.from_profile("balanced")
    close(db, 0, INITIAL * (1.0 - t.stop_drawdown - 0.001), scope="family:daily_trend")
    close(db, 0, INITIAL * 1.01, scope="bot:42")
    close(db, 0, INITIAL * (1.0 - t.halve_drawdown - 0.001), account="acct-2", scope="family:daily_trend")
    assert B.current(db, "acct-1", "family:daily_trend").halted and not B.current(db, "acct-1", "bot:42").halted
    assert B.current(db, "acct-2", "family:daily_trend").flags == B.BrakeFlags(paused=True, halve=True, halted=False)
    assert B.stopped(db) == [("acct-1", "family:daily_trend")]
    assert {e["scope"] for e in B.events(db, "acct-1")} == {"family:daily_trend", "bot:42"}
    assert {e["scope"] for e in B.events(db, "acct-1", "bot:42")} == {"bot:42"} and len(B.events(db, "acct-2")) == 4


# ------------------------------------------------------------------------------------------------- concurrency
def test_many_accounts_evaluated_concurrently_each_get_their_own_path(db):
    accounts = [f"acct-{i:02d}" for i in range(12)]
    paths = {a: random_path(seed=100 + i, days=6) for i, a in enumerate(accounts)}
    paths[accounts[0]][3] = INITIAL * 0.5                                     # one of them stops on the way
    barrier, failures = threading.Barrier(len(accounts)), []

    def worker(account):
        own = DB(db.path)                                                     # its own connections, as a process has
        try:
            barrier.wait(timeout=30)
            for index, equity in enumerate(paths[account]):
                close(own, index, equity, account=account)
        except Exception as error:                                           # surfaced below, never swallowed
            failures.append((account, repr(error)))

    threads = [threading.Thread(target=worker, args=(a,)) for a in accounts]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    assert failures == [] and not any(thread.is_alive() for thread in threads)
    for account in accounts:
        state = B.current(db, account, B.ACCOUNT_SCOPE)
        assert (state.paused, state.halve, state.halted) == evaluator_flags(paths[account], "balanced")[-1]
        assert state.evaluations == 6 and state.closing_equity == paths[account][-1]
    assert B.stopped(db) == [(accounts[0], B.ACCOUNT_SCOPE)]


def test_one_close_evaluated_from_several_threads_is_recorded_once(db):
    workers = 8
    barrier, results, failures = threading.Barrier(workers), [], []

    def worker():
        own = DB(db.path)
        try:
            barrier.wait(timeout=30)
            results.append(close(own, 0, INITIAL * 0.5))
        except Exception as error:
            failures.append(repr(error))

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    assert failures == [] and len(results) == workers
    assert sum(not r.replayed for r in results) == 1 and sum(r.stop_fired for r in results) == 1
    assert len({r.state for r in results}) == 1
    types = [e["event_type"] for e in B.events(db, "acct-1")]
    assert types.count(B.EVENT_EVALUATED) == 1 and types.count(B.EVENT_STOP) == 1


# ---------------------------------------------------------------------------------------------------- restart
def restart(db, **changes):
    arguments = dict(account_id="acct-1", scope=B.ACCOUNT_SCOPE, authorized_by="user-7", role=B.USER,
                     reference="portal-request-881", now_ms=NOW + 500)
    arguments.update(changes)
    return B.authorize_restart(db, **arguments)


def test_a_restart_needs_a_stopped_account_an_identity_and_a_reference(db):
    with pytest.raises(B.BrakeError) as missing:
        restart(db)
    assert missing.value.code == "BRAKE_STATE_NOT_FOUND"
    close(db, 0, INITIAL)
    with pytest.raises(B.BrakeError) as running:
        restart(db)
    assert running.value.code == "NOT_STOPPED"
    close(db, 1, INITIAL * 0.5)
    for changes in ({"authorized_by": ""}, {"authorized_by": "   "}, {"reference": ""}, {"role": "SYSTEM"}, {"role": ""}):
        with pytest.raises(B.BrakeError) as invalid:
            restart(db, **changes)
        assert invalid.value.code == "INVALID_ARGUMENT"
    with pytest.raises(B.BrakeError) as other_account:
        restart(db, account_id="acct-2")
    assert other_account.value.code == "BRAKE_STATE_NOT_FOUND"
    assert rows(db, f"SELECT COUNT(*) FROM {BRAKE_RESTART_TABLE}") == [(0,)]
    assert B.current(db, "acct-1", B.ACCOUNT_SCOPE).halted and not B.current(db, "acct-1", B.ACCOUNT_SCOPE).restart_pending


def test_a_restart_is_recorded_with_who_when_and_why_and_does_not_lift_the_stop_between_closes(db):
    stop = close(db, 0, INITIAL * 0.5)
    record = restart(db, role=B.OPERATOR, authorized_by="ops:maria")
    assert (record.authorized_by, record.role, record.reference, record.authorized_at_ms) == (
        "ops:maria", B.OPERATOR, "portal-request-881", NOW + 500)
    assert record.stop_seq == stop.state.stop_seq and record.high_water_mark == B.HIGH_WATER_MARK_PRESERVED and not record.replayed
    state = B.current(db, "acct-1", B.ACCOUNT_SCOPE)
    assert state.halted and state.restart_pending                             # still stopped until the next close
    assert (state.restart_id, state.restart_by, state.restart_role, state.restart_reference, state.restart_at_ms) == (
        record.restart_id, "ops:maria", B.OPERATOR, "portal-request-881", NOW + 500)
    stored = rows(db, f"SELECT restart_id, account_id, scope, stop_seq, authorized_by, authorized_role, reference, authorized_at"
                      f" FROM {BRAKE_RESTART_TABLE}")
    assert stored == [(record.restart_id, "acct-1", B.ACCOUNT_SCOPE, record.stop_seq, "ops:maria", B.OPERATOR,
                       "portal-request-881", NOW + 500)]
    assert restart(db, role=B.OPERATOR, authorized_by="ops:maria").replayed
    with pytest.raises(B.BrakeError) as second:
        restart(db, authorized_by="user-8")
    assert second.value.code == "RESTART_ALREADY_PENDING"
    assert len(rows(db, f"SELECT 1 FROM {BRAKE_RESTART_TABLE}")) == 1


def test_a_restart_keeps_the_high_water_mark_so_an_unrecovered_account_stays_stopped(db):
    t = B.BrakeThresholds.from_profile("balanced")
    stopped_at = INITIAL * (1.0 - t.stop_drawdown - 0.01)
    close(db, 0, stopped_at)
    restart(db)
    retested = close(db, 1, stopped_at)
    assert retested.halted and retested.restart_applied is False and not retested.stop_fired
    assert retested.state.highest_closing_equity == INITIAL and not retested.state.restart_pending
    assert (retested.state.triggered_day, retested.state.active_brake) == (day(0), B.DRAWDOWN_STOP)
    assert B.events(db, "acct-1")[-1]["event_type"] == B.EVENT_RESTART_NOT_APPLIED
    with pytest.raises(sqlite3.IntegrityError, match="restart record"):
        with db.connect() as conn:                                            # a used authorization clears nothing
            conn.execute(f"UPDATE {BRAKE_STATE_TABLE} SET halted=0 WHERE account_id='acct-1'")
    assert close(db, 2, stopped_at).restart_applied is None                   # the authorization was used up
    again = restart(db, reference="portal-request-882", now_ms=NOW + 900)     # a new one is a new record
    assert not again.replayed and len(rows(db, f"SELECT 1 FROM {BRAKE_RESTART_TABLE}")) == 2


def test_a_restart_applies_at_the_next_close_once_the_drawdown_is_inside_the_stop(db):
    t = B.BrakeThresholds.from_profile("balanced")
    stopped_at = INITIAL * (1.0 - t.stop_drawdown - 0.01)
    close(db, 0, stopped_at)
    record = restart(db)
    deposit = 3_000.0
    resumed = close(db, 1, stopped_at + deposit, transfers=deposit)           # new capital; no trading result
    assert not resumed.halted and resumed.restart_applied is True and resumed.new_entries_allowed
    assert resumed.state.highest_closing_equity == INITIAL + deposit          # kept, and moved only by the transfer
    assert resumed.state.drawdown == (stopped_at + deposit) / (INITIAL + deposit) - 1.0 > -t.stop_drawdown
    assert resumed.halve and resumed.state.active_brake == B.DRAWDOWN_HALVE   # the preserved drawdown still halves
    assert (resumed.state.restart_id, resumed.state.restart_applied_day, resumed.state.restart_pending) == (
        record.restart_id, day(1), False)
    assert B.stopped(db) == [] and B.events(db, "acct-1")[-1]["event_type"] == B.EVENT_RESTART_APPLIED
    fallen = close(db, 2, (INITIAL + deposit) * (1.0 - t.stop_drawdown - 0.001))
    assert fallen.halted and fallen.stop_fired and fallen.state.stop_seq > record.stop_seq       # a new stop, a new record
    with pytest.raises(sqlite3.IntegrityError, match="restart record"):
        with db.connect() as conn:                                            # the used restart does not clear the new stop
            conn.execute(f"UPDATE {BRAKE_STATE_TABLE} SET halted=0 WHERE account_id='acct-1'")
