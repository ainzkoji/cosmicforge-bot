"""Read-only local runtime/FX proof; never reads prices, keys or holdout data."""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psutil

ROOT = Path(__file__).resolve().parents[1]


def capture():
    evidence = {"observed_at_utc": datetime.now(timezone.utc).isoformat()}
    evidence["code_revision"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    evidence["working_tree_clean"] = not subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
    with urllib.request.urlopen("http://127.0.0.1:9000/health", timeout=15) as response:
        evidence["runtime_components"] = json.load(response)["components"]
    database = ROOT / "backends/shared/shared_lib/persistence/cosmicforge.db"
    with sqlite3.connect("file:" + database.as_posix() + "?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        sessions = conn.execute("SELECT runtime_session_id, pid, started_at, code_revision, "
                                "working_tree_dirty, status FROM runtime_sessions "
                                "WHERE status='RUNNING'").fetchall()
        evidence["running_sessions"] = [dict(row) for row in sessions]
        evidence["latest_cycles"] = [dict(row) for row in conn.execute(
            "SELECT started_at, completed_at, symbols_seen, symbols_managed, new_candle_evaluations, "
            "execution_attempt_count, fill_count, error_count, reason_counts_json "
            "FROM trading_cycles ORDER BY rowid DESC LIMIT 3")]
        evidence["latest_snapshot"] = dict(conn.execute(
            "SELECT fetched_at, closed_candle_close_time, higher_timeframe_aligned, source_environment "
            "FROM market_snapshots ORDER BY rowid DESC LIMIT 1").fetchone())
        evidence["broker_connections"] = [dict(row) for row in conn.execute(
            "SELECT broker_id, environment, status FROM broker_accounts")]
        table = "cati_global_market_states"
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone():
            evidence["global_market_state_rows"] = conn.execute(
                "SELECT COUNT(*) FROM " + table).fetchone()[0]
    processes = []
    for proc in psutil.process_iter(["pid", "ppid", "name", "cmdline", "create_time"]):
        try:
            command = " ".join(proc.info["cmdline"] or [])
            if "acquire_fx_reference_dataset.py" in command and "--plan" not in command:
                role = "FX_WRITER"
            elif "-File" in command and "fx_minute_supervisor.ps1" in command:
                role = "FX_SUPERVISOR"
            elif "-File" in command and "fx_completion.ps1" in command:
                role = "FX_COMPLETION"
            elif "uvicorn" in command and "app.main:app" in command:
                role = "RUNTIME"
            else:
                continue
            processes.append({key: proc.info[key] for key in ("pid", "ppid", "name", "create_time")}
                             | {"role": role})
        except (psutil.Error, TypeError):
            continue
    evidence["process_tree"] = processes
    writers = [row for row in processes if row["role"] == "FX_WRITER"]
    evidence["fx_logical_writer_count"] = sum(
        not any(child["ppid"] == row["pid"] for child in writers) for row in writers)
    command = [str(ROOT / "backends/venv/Scripts/python.exe"),
               "scripts/acquire_fx_reference_dataset.py", "--db", "data/research/fx_reference_dukascopy.db",
               "minute", "--manifest", "docs/research/cati_fx_universe_dukascopy_v1.json", "--plan"]
    evidence["fx_acquisition_plan"] = json.loads(subprocess.check_output(command, cwd=ROOT, text=True))
    latest = max((ROOT / "backends/bot-backend/logs/runtime").glob("*.err"),
                 key=lambda path: path.stat().st_mtime)
    lines = latest.read_text(encoding="utf-8", errors="replace").splitlines()
    evidence["cati_runtime_log_evidence"] = [line for line in lines if "[CATI_" in line][-12:]
    evidence["holdout_queries_by_this_capture"] = 0
    return evidence


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = capture()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"running_sessions": len(result["running_sessions"]),
                      "fx_logical_writers": result["fx_logical_writer_count"],
                      "fx_remaining": result["fx_acquisition_plan"]["remaining_periods"],
                      "cati_log_records": len(result["cati_runtime_log_evidence"])}))
