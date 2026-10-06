#!/usr/bin/env python3
"""Is the trading runtime actually trading-capable right now?

``systemctl is-active`` says the process exists. This asks the runtime itself,
through its local ``/health`` endpoint, and turns the answer into an exit
status a cron job, a systemd timer or an external monitor can act on.

    python scripts/vps_health_check.py
    python scripts/vps_health_check.py --url http://127.0.0.1:9000 --json

Checked: the API answers; the supervisor reports the scheduler HEALTHY; this
process holds a fresh lease; the CATI production task runs; the database is the
production one and its disk is not filling up; the broker account is synced
and reconciled recently; market data is being collected; an hourly CATI
decision was enrolled recently; the real-money LIVE order gate is off (unless
``--allow-live-gate``).

Exit status: 0 healthy (warnings allowed); 1 degraded -- running, but something
needs attention; 2 unhealthy -- the runtime cannot trade, or cannot be reached.
Standard library only; prints no credentials because it is given none.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

OK, WARN, FAIL = "OK", "WARN", "FAIL"
EXIT = {OK: 0, WARN: 1, FAIL: 2}


def fetch(url: str, timeout: float) -> dict:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def evaluate(health: dict, *, max_broker_sync_age: float, max_decision_age: float,
             allow_live_gate: bool, expect_revision: str | None,
             min_free_gb: float = 5.0) -> list[tuple[str, str, str]]:
    """Every check as (level, name, detail). Missing evidence is never a pass."""
    checks: list[tuple[str, str, str]] = []

    def add(level: str, name: str, detail) -> None:
        checks.append((level, name, str(detail)))

    runtime = health.get("runtime") or {}
    lease = runtime.get("lease") or {}
    scheduler = runtime.get("scheduler") or {}
    trading = health.get("trading") or {}
    config = health.get("production_configuration") or {}
    components = health.get("components") or {}

    add(OK, "api", "answering")
    state = runtime.get("state")
    add(OK if state == "HEALTHY" else WARN if state in ("STARTING", "STOPPING") else FAIL,
        "runtime_supervisor", f"{state} faults={runtime.get('faults')} warnings={runtime.get('warnings')}")
    owns = bool(components.get("runtime_owns_lease")) and bool(lease.get("held_by_this_process"))
    age = lease.get("heartbeat_age_seconds")
    add(OK if owns and age is not None and age <= 90 else FAIL, "runtime_lease",
        f"owned={owns} heartbeat_age_s={age}")
    add(OK if health.get("cati_production_running") and scheduler.get("runner_loop_alive") else FAIL,
        "trading_scheduler", f"cati_production_running={health.get('cati_production_running')} "
        f"runner_loop_alive={scheduler.get('runner_loop_alive')} "
        f"last_cycle_age_s={scheduler.get('last_cycle_completed_age_seconds')}")

    revision = health.get("runtime_revision")
    if expect_revision:
        add(OK if revision and revision.startswith(expect_revision) else FAIL, "runtime_revision",
            f"{revision} (expected {expect_revision})")
    else:
        add(OK if revision else WARN, "runtime_revision", revision)
    role = config.get("DATABASE ROLE")
    add(OK if role == "PRODUCTION" else FAIL, "database_role", role)
    disk = runtime.get("database") or {}
    free = disk.get("disk_free_bytes")
    if free is not None:
        gib = free / 1024 ** 3
        add(FAIL if gib < min_free_gb / 5 else WARN if gib < min_free_gb else OK, "database_disk",
            f"free_gib={gib:.1f} database_gib={(disk.get('size_bytes') or 0) / 1024 ** 3:.1f}")

    synced, accounts = health.get("synced_accounts"), health.get("discovered_accounts")
    sync_age = trading.get("broker_sync_max_age_seconds")
    broker_ok = bool(accounts) and synced == accounts and health.get("reconciliation_health") == "SYNCED" \
        and sync_age is not None and sync_age <= max_broker_sync_age
    add(OK if broker_ok else FAIL, "broker_sync",
        f"synced={synced}/{accounts} reconciliation={health.get('reconciliation_health')} max_age_s={sync_age}")
    add(OK if health.get("risk_engine_health") == "HEALTHY" else FAIL, "risk_engine", health.get("risk_engine_health"))

    market = health.get("market_data_status")
    add(OK if market == "COLLECTING" else WARN if market == "RATE_LIMIT_BACKOFF" else FAIL, "market_data", market)
    decision = trading.get("latest_decision") or {}
    decision_age = decision.get("age_seconds")
    add(OK if decision_age is not None and decision_age <= max_decision_age else FAIL, "cati_decision",
        f"age_s={decision_age} reason={decision.get('reason')} symbol={decision.get('symbol')}")

    enabled = trading.get("auto_trading_enabled_accounts")
    add(OK if enabled else WARN, "auto_trading", f"enabled_accounts={enabled}")
    add(OK if not trading.get("kill_switch_engaged_accounts") else WARN, "kill_switch",
        f"engaged_accounts={trading.get('kill_switch_engaged_accounts')}")
    add(OK, "execution_portfolio", f"states={trading.get('execution_portfolio_states')} "
        f"permissions={trading.get('execution_permissions')} reasons={trading.get('execution_reasons')}")

    demo, live = health.get("demo_system_gate"), health.get("live_system_gate")
    add(OK if demo else WARN, "demo_order_gate", demo)
    add(OK if (not live or allow_live_gate) else FAIL, "live_order_gate",
        f"{live}" + ("" if not live else " (real-money submission is ENABLED)"))
    if health.get("status") != "ok":
        add(WARN, "overall_status", health.get("status"))
    return checks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://127.0.0.1:9000", help="base URL of the local backend")
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--max-broker-sync-age", type=float, default=180.0, help="seconds")
    parser.add_argument("--max-decision-age", type=float, default=7800.0,
                        help="seconds since the last hourly CATI decision (default 2h10m)")
    parser.add_argument("--expect-revision", help="git revision the runtime must be running (prefix accepted)")
    parser.add_argument("--min-free-gb", type=float, default=5.0,
                        help="warn below this much free space on the database volume; fail below a fifth of it")
    parser.add_argument("--allow-live-gate", action="store_true",
                        help="do not fail when real-money LIVE order submission is enabled")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)

    try:
        health = fetch(args.url.rstrip("/") + "/health", args.timeout)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        checks = [(FAIL, "api", f"unreachable: {type(exc).__name__}: {getattr(exc, 'reason', exc)}")]
    else:
        checks = evaluate(health, max_broker_sync_age=args.max_broker_sync_age,
                          max_decision_age=args.max_decision_age, allow_live_gate=args.allow_live_gate,
                          expect_revision=args.expect_revision, min_free_gb=args.min_free_gb)
    worst = max((level for level, _, _ in checks), key=lambda level: EXIT[level])
    verdict = {OK: "HEALTHY", WARN: "DEGRADED", FAIL: "UNHEALTHY"}[worst]
    if args.json:
        print(json.dumps({"verdict": verdict, "checks": [
            {"level": level, "check": name, "detail": detail} for level, name, detail in checks]}, indent=2))
    else:
        for level, name, detail in checks:
            print(f"{level:4s} {name:20s} {detail}")
        print(f"VERDICT {verdict}")
    return EXIT[worst]


if __name__ == "__main__":
    sys.exit(main())
