#!/usr/bin/env python3
"""Kill a trading worker process mid-cycle and measure the takeover (Section H, Steps 5.2 / 5.6).

    python scripts/worker_kill_drill.py --url <sqlite:///... | postgresql://...> --out evidence/step5/worker_kill_drill.json

1. register N accounts; start worker A and worker B as separate OS processes (``app.platform_workers.drill``);
2. wait until both own their fair share and have processed every account at least once;
3. hard-kill A (SIGKILL / ``taskkill /F``) -- no drain, no release, whatever it was doing is cut;
4. measure the time until B owns every account (one TTL is the bound), keep it running a few epochs;
5. stop B gracefully (SIGTERM / CTRL_BREAK -> drain) and start C: a rolling deploy must hand over without waiting;
6. verify: no client order id accepted twice, every intent resolved (none left SUBMITTING after recovery), every
   account kept its protective orders in each epoch it was owned, assignment history shows the takeovers.

``--url`` must name a throwaway database. The drill refuses a URL that does not contain ``drill``.

``--broker-cycle`` runs the same kill / takeover / drain sequence against the REAL production broker cycle
(``production_runtime.sync`` under ``BrokerCycleAssignments``, ``assigned`` mode; ``app.platform_workers.drill
broker``) over flat fake DEMO accounts in a throwaway legacy SQLite database, and checks from the per-step logs that
no two workers ever processed one account at the same time, that the killed worker did nothing after the kill and
that every account's first step under its new owner was recovery only. Its result goes under ``broker_cycle``.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT = ROOT / "backends" / "bot-backend"
sys.path[:0] = [str(ROOT / "backends" / "shared"), str(BOT)]

from shared_lib.persistence.sql import Database  # noqa: E402
from app.platform_workers import drill  # noqa: E402
from app.platform_workers.assignments import WorkerAssignments  # noqa: E402


def spawn(url: str, wid: str, ttl_ms: int, log: Path) -> subprocess.Popen:
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(ROOT / "backends" / "shared"), str(BOT)]),
               COSMICFORGE_TEST_MODE="1")
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    return subprocess.Popen([sys.executable, "-m", "app.platform_workers.drill", "worker", "--url", url, "--id", wid,
                             "--ttl-ms", str(ttl_ms)], cwd=str(BOT), env=env, stdout=open(log, "w"),
                            stderr=subprocess.STDOUT, creationflags=flags)


def hard_kill(p: subprocess.Popen) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], capture_output=True, check=False)
    else:
        os.kill(p.pid, signal.SIGKILL)
    p.wait(10)


def graceful(p: subprocess.Popen) -> None:
    if os.name == "nt":
        p.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        p.send_signal(signal.SIGTERM)
    p.wait(30)


def owners(db: Database):
    with db.transaction(write=False) as conn:
        now = conn.now_ms()
        return dict(conn.all("SELECT account_id, worker_id FROM platform_account_assignments WHERE released_at IS NULL "
                             "AND expires_at > ?", (now,)))


def wait_until(pred, timeout_s, step=0.05):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        if pred():
            return time.monotonic() - t0
        time.sleep(step)
    return None


def run(url: str, *, accounts: int, ttl_ms: int, workdir: Path) -> dict:
    if "drill" not in url:
        raise SystemExit("refusing: --url must name a throwaway drill database")
    workdir.mkdir(parents=True, exist_ok=True)
    db = Database(url)
    drill.ensure(db)
    asg = WorkerAssignments(db)
    asg.upsert_accounts([(f"d{i:04d}", f"tenant{i % 11}", "drill", "DEMO") for i in range(accounts)])
    out = {"schema": "cosmicforge-worker-kill-drill-v1", "dialect": db.dialect, "accounts": accounts, "ttl_ms": ttl_ms,
           "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    a = spawn(url, "A", ttl_ms, workdir / "A.log")
    b = spawn(url, "B", ttl_ms, workdir / "B.log")
    try:
        both = wait_until(lambda: set(owners(db).values()) == {"A", "B"} and len(owners(db)) == accounts, 60)
        out["fair_split_s"] = both
        split = owners(db)
        out["split"] = {w: list(split.values()).count(w) for w in ("A", "B")}
        time.sleep(2 * drill.EPOCH_MS / 1000)              # A is mid-cycle somewhere in here
        killed = time.monotonic()
        hard_kill(a)
        took = wait_until(lambda: len(owners(db)) == accounts and set(owners(db).values()) == {"B"}, 4 * ttl_ms / 1000)
        out["takeover_ms_after_kill"] = round(took * 1000) if took is not None else None
        out["takeover_within_one_lease_plus_poll"] = took is not None and took * 1000 <= ttl_ms + 1_000
        out["kill_to_full_ownership_wall_ms"] = round((time.monotonic() - killed) * 1000)
        time.sleep(2 * drill.EPOCH_MS / 1000)
        c = spawn(url, "C", ttl_ms, workdir / "C.log")
        time.sleep(1.0)
        drained = time.monotonic()
        graceful(b)                                        # rolling deploy: B drains and releases
        handover = wait_until(lambda: len(owners(db)) == accounts and set(owners(db).values()) == {"C"}, 4 * ttl_ms / 1000)
        out["drain_handover_ms"] = round(handover * 1000) if handover is not None else None
        out["drain_faster_than_ttl"] = handover is not None and handover * 1000 < ttl_ms
        time.sleep(2 * drill.EPOCH_MS / 1000)
        graceful(c)
    finally:
        for p in (a, b):
            if p.poll() is None:
                hard_kill(p)
    # Restart recovery: a fresh process resolves every intent left SUBMITTING by reading the venue -- never by
    # resubmitting. The venue order count must not move.
    from app.platform_workers.jobs import OrderIntents
    with db.transaction(write=False) as conn:
        before_orders = int(conn.scalar("SELECT COUNT(*) FROM drill_venue_orders"))
        open_keys = [r[0] for r in conn.all("SELECT idempotency_key FROM platform_order_intents WHERE status='SUBMITTING'")]
        never_reached = int(conn.scalar(
            "SELECT COUNT(*) FROM platform_order_intents i WHERE i.status='SUBMITTING' AND NOT EXISTS "
            "(SELECT 1 FROM drill_venue_orders v WHERE v.client_order_id=i.client_order_id)"))
    out["submitting_never_reached_venue"] = never_reached
    venue = drill.DrillVenue(db, "recovery", lose_ack_every=0)
    resolved = [OrderIntents(db, settle_ms=0).execute(k, venue).status for k in open_keys]
    out["left_submitting_at_stop"] = len(open_keys)
    out["recovery_outcomes"] = {s: resolved.count(s) for s in set(resolved)}
    with db.transaction(write=False) as conn:
        out["recovery_added_orders"] = int(conn.scalar("SELECT COUNT(*) FROM drill_venue_orders")) - before_orders
    with db.transaction(write=False) as conn:
        out["venue_orders"] = int(conn.scalar("SELECT COUNT(*) FROM drill_venue_orders"))
        out["intents"] = dict(conn.all("SELECT status, COUNT(*) FROM platform_order_intents GROUP BY status"))
        out["duplicate_client_ids"] = int(conn.scalar(
            "SELECT COUNT(*) FROM (SELECT client_order_id FROM drill_venue_orders GROUP BY client_order_id "
            "HAVING COUNT(*) > 1) d"))
        out["acknowledged_without_venue_order"] = int(conn.scalar(
            "SELECT COUNT(*) FROM platform_order_intents i WHERE i.status='ACKNOWLEDGED' AND NOT EXISTS "
            "(SELECT 1 FROM drill_venue_orders v WHERE v.client_order_id=i.client_order_id)"))
        out["takeover_events"] = int(conn.scalar("SELECT COUNT(*) FROM platform_assignment_history "
                                                 "WHERE reason='EXPIRED_TAKEOVER'"))
        out["drain_releases"] = int(conn.scalar("SELECT COUNT(*) FROM platform_assignment_history WHERE reason='DRAIN'"))
        out["accounts_with_orders"] = int(conn.scalar("SELECT COUNT(DISTINCT account_id) FROM drill_venue_orders"))
    # An intent may legitimately remain SUBMITTING only if its account's final owner stopped inside the settle window.
    out["checks"] = {
        "takeover_within_one_lease_plus_poll": out["takeover_within_one_lease_plus_poll"],
        "drain_faster_than_ttl": out["drain_faster_than_ttl"],
        "no_duplicate_orders": out["duplicate_client_ids"] == 0,
        "no_ack_without_order": out["acknowledged_without_venue_order"] == 0,
        "every_account_protected": out["accounts_with_orders"] == accounts,
        "takeovers_recorded": out["takeover_events"] >= out["split"].get("A", 0),
        "every_intent_resolved_after_restart": out["intents"].get("SUBMITTING", 0) == 0,
        # an order is added only for an intent the venue never received (crash between commit and send)
        "recovery_adds_only_never_reached_orders": out["recovery_added_orders"] == out["submitting_never_reached_venue"],
    }
    out["passed"] = all(out["checks"].values())
    out["limits"] = ("Separate OS processes on one workstation against a database-backed fake venue. Not a hosted "
                     "staging run, not venue latency, not a Linux certification.")
    return out


def spawn_broker(url: str, legacy: Path, wid: str, ttl_ms: int, workdir: Path, *, slow_ms: int) -> subprocess.Popen:
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(ROOT / "backends" / "shared"), str(BOT)]))
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    # cwd: the drill's work directory -- no operator .env is ever read by a drill process.
    return subprocess.Popen([sys.executable, "-m", "app.platform_workers.drill", "broker", "--url", url, "--legacy",
                             str(legacy), "--id", wid, "--steps", str(workdir / f"steps_{wid}.jsonl"),
                             "--ttl-ms", str(ttl_ms), "--slow-ms", str(slow_ms)], cwd=str(workdir), env=env,
                            stdout=open(workdir / f"{wid}.log", "w"), stderr=subprocess.STDOUT, creationflags=flags)


def run_broker_cycle(url: str, *, accounts: int, ttl_ms: int, workdir: Path, slow_ms: int = 60) -> dict:
    if "drill" not in url:
        raise SystemExit("refusing: --url must name a throwaway drill database")
    workdir.mkdir(parents=True, exist_ok=True)
    db = Database(url)
    drill.ensure(db)
    legacy = workdir / f"broker_legacy_{db.dialect}.db"
    for stale in [legacy, *workdir.glob("steps_*.jsonl")]:
        stale.unlink(missing_ok=True)
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(ROOT / "backends" / "shared"), str(BOT)]))
    subprocess.run([sys.executable, "-m", "app.platform_workers.drill", "broker-setup", "--legacy", str(legacy),
                    "--accounts", str(accounts)], cwd=str(workdir), env=env, check=True)
    ids = [f"{drill.BROKER_DRILL_PREFIX}{i:04d}" for i in range(accounts)]

    def recovered():
        with db.transaction(write=False) as conn:
            now = conn.now_ms()
            return dict(conn.all("SELECT account_id, worker_id FROM platform_account_assignments WHERE released_at IS "
                                 "NULL AND expires_at > ? AND recovered_token = fencing_token", (now,)))

    def db_now():
        with db.transaction(write=False) as conn:
            return conn.now_ms()

    out = {"schema": "cosmicforge-broker-cycle-kill-drill-v1", "dialect": db.dialect, "accounts": accounts,
           "ttl_ms": ttl_ms, "loop_interval_ms": 200, "step_read_pause_ms": slow_ms,
           "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    a = spawn_broker(url, legacy, "A", ttl_ms, workdir, slow_ms=slow_ms)
    b = spawn_broker(url, legacy, "B", ttl_ms, workdir, slow_ms=slow_ms)
    c = None
    try:
        split = wait_until(lambda: set(recovered().values()) == {"A", "B"} and set(recovered()) == set(ids), 120)
        out["fair_split_and_recovered_s"] = split
        owned = recovered()
        out["split"] = {w: list(owned.values()).count(w) for w in ("A", "B")}
        a_accounts = {k for k, v in owned.items() if v == "A"}
        time.sleep(1.5)                                   # A is inside an account step (each first read pauses)
        hard_kill(a)
        killed_ms = db_now()
        took = wait_until(lambda: set(owners(db)) == set(ids) and set(owners(db).values()) == {"B"}, 6 * ttl_ms / 1000)
        out["kill_to_full_ownership_ms"] = round(took * 1000) if took is not None else None
        rec = wait_until(lambda: set(recovered()) == set(ids) and set(recovered().values()) == {"B"}, 60)
        out["kill_to_all_recovered_by_successor_ms"] = round((took + rec) * 1000) if None not in (took, rec) else None
        with db.transaction(write=False) as conn:
            claims = conn.all("SELECT account_id, at FROM platform_assignment_history WHERE reason='EXPIRED_TAKEOVER' "
                              "AND from_worker='A' AND to_worker='B'")
        out["takeover_claim_ms_after_kill"] = {"max": max((int(t) - killed_ms for _, t in claims), default=None),
                                               "min": min((int(t) - killed_ms for _, t in claims), default=None)}
        out["takeovers"] = len(claims)
        c = spawn_broker(url, legacy, "C", ttl_ms, workdir, slow_ms=slow_ms)
        c_log = workdir / "steps_C.jsonl"
        out["successor_started_s"] = wait_until(      # C is up and looping before B is asked to drain
            lambda: c_log.exists() and '"round"' in c_log.read_text(encoding="utf-8"), 120)
        drain_started = db_now()
        graceful(b)                                       # rolling deploy: B drains, C takes over at once
        handover = wait_until(lambda: set(owners(db)) == set(ids) and set(owners(db).values()) == {"C"}, 6 * ttl_ms / 1000)
        with db.transaction(write=False) as conn:
            out["drain_releases"] = int(conn.scalar("SELECT COUNT(*) FROM platform_assignment_history WHERE "
                                                    "reason='DRAIN' AND from_worker='B' AND at >= ?", (drain_started,)))
            last_claim = conn.scalar("SELECT MAX(at) FROM platform_assignment_history WHERE to_worker='C' AND at >= ?",
                                     (drain_started,))
        # database clock: from the stop signal to C holding the last of B's accounts (B's in-flight step included)
        out["drain_to_full_ownership_ms"] = (int(last_claim) - drain_started) if handover is not None and last_claim else None
        wait_until(lambda: set(recovered()) == set(ids) and set(recovered().values()) == {"C"}, 60)
        graceful(c)
    finally:
        for p in (a, b, c):
            if p is not None and p.poll() is None:
                hard_kill(p)
    steps, rounds = {}, {}
    for wid in ("A", "B", "C"):
        path = workdir / f"steps_{wid}.jsonl"
        for line in (path.read_text(encoding="utf-8").splitlines() if path.exists() else ()):
            row = json.loads(line)
            if row["event"] == "round":
                rounds.setdefault(wid, []).append(row["at"])
                continue
            key = (row["worker"], row["account"])
            if row["event"] == "start":
                steps.setdefault(key, []).append({"start": row["at"], "end": None, "recovery": row["recovery"]})
            elif steps.get(key):
                steps[key][-1].update(end=row["at"], outcome=row.get("outcome"))
    by_account, overlaps = {}, []
    for (wid, account), spans in steps.items():
        for span in spans:
            by_account.setdefault(account, []).append((span["start"], span["end"], wid, span["recovery"]))
    for account, spans in by_account.items():
        spans.sort()
        for s1, s2 in zip(spans, spans[1:]):
            # A step cut by the kill has no end: it ended when the process did.
            end1 = s1[1] if s1[1] is not None else (killed_ms if s1[2] == "A" else None)
            if s1[2] != s2[2] and (end1 is None or end1 > s2[0]):
                overlaps.append({"account": account, "first": s1[2], "second": s2[2]})
    a_after_kill = sum(1 for (wid, _), spans in steps.items() if wid == "A" for s in spans if s["start"] > killed_ms)
    first_after_takeover = []
    for account in a_accounts:
        b_spans = sorted(s for s in by_account.get(account, []) if s[2] == "B" and s[0] > killed_ms)
        first_after_takeover.append(bool(b_spans) and b_spans[0][3] is True)
    out.update(account_steps=sum(len(v) for v in steps.values()), overlapping_steps=overlaps[:20],
               killed_worker_steps_after_kill=a_after_kill, cut_steps_of_killed_worker=sum(
                   1 for (wid, _), spans in steps.items() if wid == "A" for s in spans if s["end"] is None))
    # A worker claims at the start of a round, so the bound is one TTL after the kill plus ONE CYCLE of the
    # successor (its round period around the takeover -- in production the 30 s broker-loop interval) + 1 s slack.
    b_rounds = sorted(rounds.get("B", []))
    takeover_at = killed_ms + (out["takeover_claim_ms_after_kill"]["max"] or 0)
    periods = [b - a for a, b in zip(b_rounds, b_rounds[1:]) if a <= takeover_at and b >= killed_ms - 30_000]
    out["successor_cycle_ms_around_takeover"] = max(periods) if periods else None
    bound = ttl_ms + (out["successor_cycle_ms_around_takeover"] or 200) + 1_000
    out["checks"] = {
        "takeover_within_ttl_plus_loop_interval": out["takeover_claim_ms_after_kill"]["max"] is not None
        and out["takeover_claim_ms_after_kill"]["max"] <= bound,
        "every_killed_account_taken_over": out["takeovers"] == len(a_accounts) and out["kill_to_full_ownership_ms"] is not None,
        "successor_recovers_first": all(first_after_takeover) and len(first_after_takeover) == len(a_accounts),
        "never_two_workers_on_one_account": not overlaps,
        "killed_worker_silent_after_kill": a_after_kill == 0,
        "drain_hands_over_faster_than_ttl": out["drain_to_full_ownership_ms"] is not None
        and out["drain_to_full_ownership_ms"] < ttl_ms and out["drain_releases"] >= 1,
    }
    out["passed"] = all(out["checks"].values())
    out["bound_ms"] = bound
    out["limits"] = ("Separate OS processes on one workstation running the real production broker-cycle code over flat "
                     "fake DEMO accounts (no order surface; venue-mutation fencing under kill is proven in-process on "
                     "the real execution path by tests/trading_intelligence/test_step5_broker_cycle_assignment.py). "
                     "Not a hosted staging run, not venue latency, not a Linux certification.")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--accounts", type=int, default=200)
    ap.add_argument("--ttl-ms", type=int, default=3_000)
    ap.add_argument("--workdir", default=str(ROOT / "tmp_drill"))
    ap.add_argument("--broker-cycle", action="store_true", help="drill the real production broker cycle")
    a = ap.parse_args(argv)
    if a.broker_cycle:
        result = run_broker_cycle(a.url, accounts=a.accounts, ttl_ms=a.ttl_ms, workdir=Path(a.workdir))
        out = Path(a.out)
        existing = json.loads(out.read_text()) if out.exists() else {}
        existing.setdefault("process_drill", {})[result["dialect"]] = result
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(existing, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps({k: result.get(k) for k in ("dialect", "takeover_claim_ms_after_kill",
                                                     "kill_to_all_recovered_by_successor_ms",
                                                     "drain_to_full_ownership_ms", "checks", "passed")}, indent=1))
        return 0 if result["passed"] else 1
    result = run(a.url, accounts=a.accounts, ttl_ms=a.ttl_ms, workdir=Path(a.workdir))
    out = Path(a.out)
    existing = json.loads(out.read_text()) if out.exists() else {}
    existing[result["dialect"]] = result
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(existing, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("dialect", "takeover_ms_after_kill", "drain_handover_ms", "checks",
                                             "passed", "venue_orders", "intents")}, indent=1))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
