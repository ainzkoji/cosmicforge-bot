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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--accounts", type=int, default=200)
    ap.add_argument("--ttl-ms", type=int, default=3_000)
    ap.add_argument("--workdir", default=str(ROOT / "tmp_drill"))
    a = ap.parse_args(argv)
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
