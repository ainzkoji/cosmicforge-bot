"""Keep strict FX finalization supervised without repeating unchanged failed QA.

Only acquisition-log/manifest changes trigger another existing finalization.
The singleton lock prevents duplicate finalizers; it never changes gap policy.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data/research/fx_reference_dukascopy.db"
MANIFEST = ROOT / "docs/research/cati_fx_universe_dukascopy_v1.json"
OUTPUT = ROOT / "data/research/fx_finalization"


def fingerprint():
    h = hashlib.sha256(MANIFEST.read_bytes())
    with sqlite3.connect(DB.resolve().as_uri() + "?mode=ro", uri=True) as c:
        for row in c.execute("SELECT * FROM fx_reference_ingest_log ORDER BY provider,pair,timeframe,period,side"):
            h.update(json.dumps(row, separators=(",", ":")).encode())
    return h.hexdigest()


def remaining():
    """Same completed-day accounting as the acquisition planner; read only."""
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8-sig"))
    start = datetime.fromtimestamp(manifest["window_start_ms"] / 1000, timezone.utc).date()
    end = datetime.fromtimestamp(manifest["window_end_ms"] / 1000, timezone.utc).date()
    days = []
    while start < end:
        if start.weekday() != 5:
            days.append(start.isoformat())
        start += timedelta(days=1)
    total = 0
    with sqlite3.connect(DB.resolve().as_uri() + "?mode=ro", uri=True) as c:
        for member in manifest["members"]:
            done = dict(c.execute("SELECT period,MIN(status IN ('FETCHED','NO_FILE','EMPTY')) FROM fx_reference_ingest_log WHERE provider=? AND pair=? AND timeframe='1m' GROUP BY period", (manifest["provider"], member["pair"])))
            total += sum(done.get(day) != 1 for day in days)
    return total


def main(recheck=False):
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with (OUTPUT / "watcher.lock").open("a+b") as lock:
        lock.seek(0)
        if not lock.read(1):
            lock.write(b"0")
            lock.flush()
        lock.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise SystemExit("FX finalization watcher already running")
        status_file = OUTPUT / "watcher_status.json"
        saved = json.loads(status_file.read_text()) if status_file.exists() else {}
        previous = None if recheck or saved.get("status") == "FINALIZING" else saved.get("source_fingerprint", fingerprint())
        while True:
            current = fingerprint()
            pending = remaining()
            state = {"pid": os.getpid(), "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                     "source_fingerprint": current, "status": "WATCHING_INPUT_CHANGES",
                     "strict_finalization": "UNCHANGED", "acquisition_completed": pending == 0,
                     "remaining_periods": pending}
            if current != previous and pending == 0:
                state["status"] = "FINALIZING"
                (OUTPUT / "watcher_status.json").write_text(json.dumps(state, indent=2))
                with (OUTPUT / "finalization.log").open("a", encoding="utf-8") as log:
                    result = subprocess.run([str(ROOT / "backends/venv/Scripts/python.exe"),
                        str(ROOT / "scripts/finalize_fx_reference.py"), "--db", str(DB),
                        "--manifest", str(MANIFEST), "--output", str(OUTPUT)], cwd=ROOT,
                        stdout=log, stderr=subprocess.STDOUT)
                state["last_finalization_exit_code"] = result.returncode
                state["status"] = "WATCHING_INPUT_CHANGES"
                previous = fingerprint()
            final = OUTPUT / "status.json"
            state["finalization"] = json.loads(final.read_text()) if final.exists() else {"status": "NOT_RUN"}
            (OUTPUT / "watcher_status.json").write_text(json.dumps(state, indent=2))
            time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--recheck", action="store_true")
    main(parser.parse_args().recheck)
