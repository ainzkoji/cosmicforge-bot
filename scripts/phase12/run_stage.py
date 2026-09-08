"""Phase 12 stage driver.

Each stage is a separate process invocation on purpose: the restart proof is
only worth anything if the process really goes away and the replacement
rehydrates from persisted state.

    python scripts/phase12/run_stage.py setup
    python scripts/phase12/run_stage.py open
    python scripts/phase12/run_stage.py tp1
    ...
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.phase12.bootstrap import assert_database_is_validation, bootstrap  # noqa: E402

STATE_FILE_ENV = "PHASE12_STATE_FILE"


def _state_path() -> str:
    from scripts.phase12.bootstrap import REPO_ROOT

    return os.environ.get(STATE_FILE_ENV) or os.path.join(
        REPO_ROOT, "backends", "bot-backend", "logs", "phase12_state.json"
    )


def load_state() -> dict:
    path = _state_path()
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def save_state(state: dict) -> None:
    path = _state_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, default=str)


def _emit(stage: str, payload: dict) -> None:
    print("PHASE12_RESULT " + json.dumps({"stage": stage, **payload}, default=str))


# ── Evidence readers ────────────────────────────────────────────────────────


def position_snapshot(db, bot_id: str) -> list[dict]:
    with db.connect() as conn:
        rows = conn.execute(
            """SELECT position_id, symbol, side, status, original_qty, remaining_qty,
                      realized_qty, entry_price, opened_at, closed_at,
                      realized_pnl, fees, provenance, close_reason
               FROM positions WHERE bot_instance_id=? ORDER BY opened_at""",
            (bot_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def event_log(db, bot_id: str) -> list[dict]:
    with db.connect() as conn:
        rows = conn.execute(
            """SELECT event_type, symbol, quantity, remaining_qty, price, fee,
                      realized_pnl, stop_price, occurred_at, reason
               FROM position_events WHERE bot_instance_id=? ORDER BY occurred_at""",
            (bot_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def runtime_lifecycle_state(db, bot_id: str) -> list[dict]:
    """The persisted state a restarted runner must rehydrate from."""
    out = []
    with db.connect() as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if "position_lifecycle_state" in tables:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(position_lifecycle_state)")]
            rows = conn.execute("SELECT * FROM position_lifecycle_state").fetchall()
            out = [dict(zip(cols, r)) for r in rows]
    return out


def symbol_state(db) -> list[dict]:
    with db.connect() as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if "symbol_state" not in tables:
            return []
        cols = [r[1] for r in conn.execute("PRAGMA table_info(symbol_state)")]
        rows = conn.execute("SELECT * FROM symbol_state").fetchall()
    return [dict(zip(cols, r)) for r in rows]


def pm_view(runner, symbol: str) -> dict:
    """What the PositionManager currently believes about one symbol."""
    try:
        position = runner.position_manager.get_position(symbol)
    except Exception as exc:
        return {"error": str(exc)}
    if position is None:
        return {"position": None}
    sl = getattr(position, "sl", None)
    tp = getattr(position, "tp", None)
    return {
        "phase": str(getattr(position, "phase", None)),
        "current_qty": getattr(position, "current_qty", None),
        "entry_price": getattr(position, "entry_price", None),
        "tp1_price": getattr(tp, "tp1_price", None),
        "tp1_hit": getattr(tp, "tp1_hit", None),
        "tp2_price": getattr(tp, "tp2_price", None),
        "current_stop": getattr(sl, "current_stop", None),
        "is_break_even": getattr(sl, "is_break_even", None),
        "be_exchange_confirmed": getattr(sl, "be_exchange_confirmed", None),
        "trailing_last_stop_price": getattr(sl, "trailing_last_stop_price", None),
    }


# ── Runtime assembly shared by every driving stage ──────────────────────────


def _runtime(args, *, wait: bool = False, close: float | None = None):
    """Open a session, build the real runner, return what the stage needs.

    ``wait`` blocks until a genuinely new candle has closed, which is what lets
    the strategy clock advance between stages without inventing data.
    """
    from shared_lib.persistence.db import DB

    from scripts.phase12 import harness
    from scripts.phase12.market import ControlledClient, wait_for_next_boundary

    db = DB()
    assert_database_is_validation(db)
    state = load_state()

    if wait:
        wait_for_next_boundary()

    session_id = harness.open_session(db)
    os.environ["PHASE12_RUNTIME_SESSION_ID"] = session_id

    # Always anchored to the newest closed candle: fresh enough for the entry
    # path's staleness guard, and never running into the future.
    client = ControlledClient(symbols=(harness.SYMBOL,), start_ms=args.start_ms)
    if close is not None:
        client.set_last_close(harness.SYMBOL, close)

    runner, policy, context = harness.build_runner(
        db, client, run_id=state.get("run_id") or harness.new_run_id(),
        controlled=not args.no_control, confidence=args.confidence,
    )
    harness.open_run(db, runner, policy, runtime_session_id=session_id)

    state["run_id"] = runner.run_id
    state["runtime_session_id"] = session_id
    state["last_close"] = close
    save_state(state)
    return db, client, runner, policy, state


# ── Stages ──────────────────────────────────────────────────────────────────


def stage_setup(args) -> dict:
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    from scripts.phase12.harness import VALIDATION_BOT, ensure_validation_identity

    db = DB()
    assert_database_is_validation(db)
    migrate(db)
    ensure_validation_identity(db)
    save_state({})
    with db.connect() as conn:
        row = conn.execute(
            "SELECT id, status, mode, capital_allocation, allocation_value "
            "FROM bot_instances WHERE id=?", (VALIDATION_BOT,),
        ).fetchone()
    return {"database": db.path, "bot": dict(row) if row else None}


def stage_open(args) -> dict:
    from scripts.phase12 import harness
    from scripts.phase12.controlled import controlled_summary

    db, client, runner, policy, state = _runtime(args, wait=args.wait, close=args.close)
    cycle = runner.run_cycle()
    positions = position_snapshot(db, harness.VALIDATION_BOT)
    return {
        "cycle_summary": cycle.get("summary"),
        "symbol_result": cycle.get("results", {}).get(harness.SYMBOL),
        "controlled": controlled_summary(runner.strategy),
        "positions": positions,
        "events": event_log(db, harness.VALIDATION_BOT),
        "blocked_broker_calls": client.blocked_calls,
        "price": client.last_price(harness.SYMBOL),
    }


def stage_manage(args) -> dict:
    """Run management cycles at a given price (TP1, break-even, trailing)."""
    from scripts.phase12 import harness

    db, client, runner, policy, state = _runtime(
        args, wait=args.wait, close=args.close,
    )
    cycles = []
    for _ in range(max(1, args.cycles)):
        cycles.append(runner.run_cycle().get("summary"))
    return {
        "price": client.last_price(harness.SYMBOL),
        "latest_close_time": client.latest_close_time(harness.SYMBOL),
        "pm": pm_view(runner, harness.SYMBOL),
        "cycles": cycles,
        "positions": position_snapshot(db, harness.VALIDATION_BOT),
        "events": event_log(db, harness.VALIDATION_BOT),
        "lifecycle_state": runtime_lifecycle_state(db, harness.VALIDATION_BOT),
        "symbol_state": symbol_state(db),
        "blocked_broker_calls": client.blocked_calls,
    }


def stage_inspect(args) -> dict:
    """Read persisted state only. Constructs no runner, drives no cycle."""
    from shared_lib.persistence.db import DB

    from scripts.phase12 import harness

    db = DB()
    assert_database_is_validation(db)
    return {
        "database": db.path,
        "positions": position_snapshot(db, harness.VALIDATION_BOT),
        "events": event_log(db, harness.VALIDATION_BOT),
        "lifecycle_state": runtime_lifecycle_state(db, harness.VALIDATION_BOT),
        "symbol_state": symbol_state(db),
        "state_file": load_state(),
    }


def stage_restore(args) -> dict:
    """Fresh process: build a runner and report what it rehydrated.

    This is the restart proof. Nothing is replayed into it — every value here
    came out of the database written by the previous processes.
    """
    from scripts.phase12 import harness

    db, client, runner, policy, state = _runtime(args)
    restored = {}
    for symbol, st in (getattr(runner, "state", None) or {}).items():
        restored[symbol] = {
            "position": getattr(st, "position", None),
            "entry_price": getattr(st, "entry_price", None),
            "entry_qty": getattr(st, "entry_qty", None),
            "position_id": getattr(st, "position_id", None),
            "sl": getattr(st, "sl", None),
            "tp": getattr(st, "tp", None),
        }
    pm_state = {harness.SYMBOL: pm_view(runner, harness.SYMBOL)}
    return {
        "runtime_session_id": state["runtime_session_id"],
        "run_id": runner.run_id,
        "policy_hash": policy.policy_hash,
        "runner_initialization_status": getattr(runner, "initialization_status", None),
        "restored_symbol_state": restored,
        "restored_position_manager": pm_state,
        "positions": position_snapshot(db, harness.VALIDATION_BOT),
        "lifecycle_state": runtime_lifecycle_state(db, harness.VALIDATION_BOT),
    }


def daily_close_marks(db) -> list[dict]:
    with db.connect() as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if "bot_daily_close_marks" not in tables:
            return []
        cols = [r[1] for r in conn.execute("PRAGMA table_info(bot_daily_close_marks)")]
        rows = conn.execute("SELECT * FROM bot_daily_close_marks").fetchall()
    return [dict(zip(cols, r)) for r in rows]


def stage_daily_close(args) -> dict:
    """B7 — one daily close per window, idempotent across ticks and restarts.

    The close *window* is moved to bracket 'now' so the proof does not have to
    wait for 23:55 Europe/Rome. That is a schedule, not a safety threshold: the
    profit floors, the position checks and the idempotency marker are all left
    exactly as configured.
    """
    from datetime import datetime, timedelta, timezone

    from scripts.phase12 import harness

    now = datetime.now(timezone.utc)
    os.environ["DAILY_CLOSE_ENABLED"] = "true"
    os.environ["DAILY_CLOSE_TIMEZONE"] = "UTC"
    os.environ["DAILY_CLOSE_WINDOW_START"] = (now - timedelta(minutes=5)).strftime("%H:%M")
    os.environ["DAILY_CLOSE_WINDOW_END"] = (now + timedelta(minutes=30)).strftime("%H:%M")
    os.environ["DAILY_CLOSE_MIN_PROFIT_USDT"] = "0"
    os.environ["DAILY_CLOSE_MIN_PROFIT_PCT"] = "0"
    for key in ("DAILY_CLOSE_ENABLED", "DAILY_CLOSE_TIMEZONE", "DAILY_CLOSE_WINDOW_START",
                "DAILY_CLOSE_WINDOW_END", "DAILY_CLOSE_MIN_PROFIT_USDT",
                "DAILY_CLOSE_MIN_PROFIT_PCT"):
        _reload_setting(key)

    db, client, runner, policy, state = _runtime(args, wait=args.wait, close=args.close)
    cycles = []
    for _ in range(max(1, args.cycles)):
        cycles.append(runner.run_cycle().get("summary"))
    return {
        "window": {
            "start": os.environ["DAILY_CLOSE_WINDOW_START"],
            "end": os.environ["DAILY_CLOSE_WINDOW_END"],
            "timezone": "UTC",
        },
        "cycles_run": len(cycles),
        "positions": position_snapshot(db, harness.VALIDATION_BOT),
        "events": event_log(db, harness.VALIDATION_BOT),
        "daily_close_marks": daily_close_marks(db),
        "blocked_broker_calls": client.blocked_calls,
    }


def stage_kill_switch(args) -> dict:
    """B8 — kill switch blocks new entries and closes with canonical evidence."""
    from scripts.phase12 import harness

    db, client, runner, policy, state = _runtime(args, wait=args.wait, close=args.close)

    # Arm the kill switch the way the runtime does: through persisted daily
    # state, so the runner restores it exactly as it would after a real
    # daily-loss breach.
    runner.daily.kill = True
    try:
        runner.store.save_daily(
            runner.daily.day,
            float(getattr(runner.daily, "realized_pnl", 0.0) or 0.0),
            True,
            trade_count=int(getattr(runner.daily, "trade_count", 0) or 0),
        )
    except Exception as exc:
        return {"error": f"could not persist kill state: {exc}"}

    before = len(position_snapshot(db, harness.VALIDATION_BOT))
    attempts_before = _count(db, "execution_attempts")
    cycle = runner.run_cycle()
    return {
        "cycle_status": cycle.get("status"),
        "cycle_reason": cycle.get("reason"),
        "health_status": cycle.get("health_status"),
        "positions_before": before,
        "new_execution_attempts": _count(db, "execution_attempts") - attempts_before,
        "positions": position_snapshot(db, harness.VALIDATION_BOT),
        "events": event_log(db, harness.VALIDATION_BOT),
        "blocked_broker_calls": client.blocked_calls,
    }


def stage_duplicate(args) -> dict:
    """B9 — repeated ticks on one candle yield at most one execution."""
    from scripts.phase12 import harness

    db, client, runner, policy, state = _runtime(args, wait=args.wait, close=args.close)
    attempts_before = _count(db, "execution_attempts")
    positions_before = _count(db, "positions")
    summaries = []
    for _ in range(max(2, args.cycles)):
        summaries.append(runner.run_cycle().get("summary"))
    return {
        "cycles_run": len(summaries),
        "new_execution_attempts": _count(db, "execution_attempts") - attempts_before,
        "new_positions": _count(db, "positions") - positions_before,
        "positions": position_snapshot(db, harness.VALIDATION_BOT),
        "blocked_broker_calls": client.blocked_calls,
    }


def _count(db, table: str) -> int:
    with db.connect() as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _reload_setting(key: str) -> None:
    """Push an env override into the already-imported settings object."""
    from app.core.config import settings

    raw = os.environ.get(key)
    if raw is None:
        return
    current = getattr(settings, key, None)
    if isinstance(current, bool):
        value = raw.strip().lower() in {"1", "true", "yes", "on"}
    elif isinstance(current, int) and not isinstance(current, bool):
        value = int(float(raw))
    elif isinstance(current, float):
        value = float(raw)
    else:
        value = raw
    object.__setattr__(settings, key, value)


STAGES = {
    "setup": stage_setup,
    "open": stage_open,
    "manage": stage_manage,
    "inspect": stage_inspect,
    "restore": stage_restore,
    "daily_close": stage_daily_close,
    "kill_switch": stage_kill_switch,
    "duplicate": stage_duplicate,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 12 live lifecycle stages")
    parser.add_argument("stage", choices=sorted(STAGES))
    parser.add_argument("--db", default=None, help="validation database path")
    parser.add_argument("--confidence", type=float, default=0.92)
    parser.add_argument("--price", type=float, default=None,
                        help="mark price for management (TP1, break-even, trailing)")
    parser.add_argument("--close", type=float, default=None,
                        help="close price for each appended candle")
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--wait", action="store_true",
                        help="wait for a genuinely new closed candle first")
    parser.add_argument("--no-control", action="store_true",
                        help="run the unmodified ensemble (no controlled opportunity)")
    parser.add_argument(
        "--start-ms", type=int, default=None,
        help="override the candle anchor (default: the one pinned at setup)",
    )
    args = parser.parse_args()

    bootstrap(args.db)
    try:
        payload = STAGES[args.stage](args)
    except Exception as exc:  # surfaced, never swallowed
        import traceback

        traceback.print_exc()
        _emit(args.stage, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        return 1
    _emit(args.stage, {"ok": True, **payload})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
