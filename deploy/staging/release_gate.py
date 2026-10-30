#!/usr/bin/env python3
"""Release gate of the staging deploy (Section H, Step 5.6). Standard library only; runs on the deploy host.

    curl ... /health?ready=1 | python3 release_gate.py --http-status 200 [--emergency-status 401]
                                                       [--require-supervised]

``deploy.sh``, ``rollback.sh`` and ``restore.sh`` ask the TRADING process (never an API replica) for
``/health?ready=1`` and pass the answer here. The verdict decides whether a release is accepted:

  READY               supervised runtime (production profile): supervisor HEALTHY, this process holds the runtime
                      lease, every discovered account synced and reconciled (post-start reconciliation done)
  READY_UNSUPERVISED  non-production profile (staging today runs APP_ENV=DEVELOPMENT): the process answers as the
                      trading role and its readiness probe is 200, but no supervisor judges the trading loop and
                      no broker reconciliation is reported. Accepted unless ``--require-supervised``; the verdict
                      names it so nobody reads it as more than it is
  NOT_READY           no answer, non-200, wrong process role, supervisor not HEALTHY, lease not held, emergency
                      routes not served by this process
  DEGRADED            the process is healthy but broker truth is not: an account is not synced and reconciled
                      (exchange unreachable, credentials refused). Never accepted: a release is not accepted
                      before it has reconciled with the venue
  UNSAFE              real-money LIVE order submission is enabled on a staging stack. Never accepted

Exit status: 0 READY / READY_UNSUPERVISED, 10 NOT_READY, 11 DEGRADED, 12 UNSAFE, 2 bad arguments.
Output: one JSON line. The gate only reads what it is given; it never calls a route itself.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Optional

READY, READY_UNSUPERVISED, NOT_READY, DEGRADED, UNSAFE = (
    "READY", "READY_UNSUPERVISED", "NOT_READY", "DEGRADED", "UNSAFE")
EXIT = {READY: 0, READY_UNSUPERVISED: 0, NOT_READY: 10, DEGRADED: 11, UNSAFE: 12}
#: An emergency route that exists answers an unauthenticated read with 401/403 (or 200 behind a dev bypass);
#: 404 means the router is not mounted, 409 that this is an API replica, 5xx that it is broken.
EMERGENCY_SERVED = (200, 401, 403)


def _verdict(verdict: str, reason: str, **detail: Any) -> Dict[str, Any]:
    return {"verdict": verdict, "reason": reason, **detail}


def evaluate(http_status: Optional[int], body: Any, *, emergency_status: Optional[int] = None,
             require_supervised: bool = False, expect_role: str = "trading",
             lease_optional: bool = False) -> Dict[str, Any]:
    """The verdict for one readiness answer. ``http_status`` None/0 means the process did not answer at all."""
    if not http_status:
        return _verdict(NOT_READY, "NO_ANSWER")
    if not isinstance(body, dict):
        return _verdict(NOT_READY, f"HTTP_{http_status}" if http_status != 200 else "BODY_UNREADABLE",
                        http_status=http_status)
    if body.get("live_system_gate") is True:
        return _verdict(UNSAFE, "LIVE_ORDER_GATE_OPEN_ON_STAGING", http_status=http_status)
    role = body.get("process_role")
    if role != expect_role:
        return _verdict(NOT_READY, f"PROCESS_ROLE_{str(role).upper()}", http_status=http_status)
    runtime = body.get("runtime")
    state = runtime.get("state") if isinstance(runtime, dict) else None
    if http_status != 200:
        return _verdict(NOT_READY, f"HTTP_{http_status}", http_status=http_status, runtime_state=state)
    if emergency_status is not None and emergency_status not in EMERGENCY_SERVED:
        return _verdict(NOT_READY, f"EMERGENCY_ROUTES_NOT_SERVED_HTTP_{emergency_status}", http_status=http_status)
    if not isinstance(runtime, dict):
        if require_supervised:
            return _verdict(NOT_READY, "RUNTIME_UNSUPERVISED", http_status=http_status)
        return _verdict(READY_UNSUPERVISED, "NON_PRODUCTION_PROFILE_NO_SUPERVISOR_NO_BROKER_RECONCILIATION",
                        http_status=http_status)
    if state != "HEALTHY":
        return _verdict(NOT_READY, f"RUNTIME_{state}", http_status=http_status, faults=runtime.get("faults"))
    if not lease_optional and not (body.get("components") or {}).get("runtime_owns_lease"):
        return _verdict(NOT_READY, "RUNTIME_LEASE_NOT_HELD", http_status=http_status)
    accounts = int(body.get("discovered_accounts") or 0)
    synced = int(body.get("synced_accounts") or 0)
    if accounts and (body.get("reconciliation_health") != "SYNCED" or synced != accounts):
        return _verdict(DEGRADED, "BROKER_RECONCILIATION_NOT_SYNCED", http_status=http_status,
                        synced_accounts=synced, discovered_accounts=accounts,
                        reconciliation_health=body.get("reconciliation_health"))
    return _verdict(READY, "SUPERVISED_RUNTIME_HEALTHY_AND_RECONCILED" if accounts else
                    "SUPERVISED_RUNTIME_HEALTHY_NO_ACCOUNTS", http_status=http_status, discovered_accounts=accounts)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--http-status", type=int, default=0, help="HTTP status of /health?ready=1 (0: no answer)")
    ap.add_argument("--emergency-status", type=int, default=None,
                    help="HTTP status of an unauthenticated GET of the emergency status route (omit: not probed)")
    ap.add_argument("--require-supervised", action="store_true",
                    help="refuse a release whose trading runtime is not supervised (production-profile staging)")
    ap.add_argument("--expect-role", default="trading")
    ap.add_argument("--lease-optional", action="store_true",
                    help="multi-worker topology: the runtime-owner lease is held by ONE of the trading processes")
    a = ap.parse_args(argv)
    raw = sys.stdin.read()
    try:
        body = json.loads(raw) if raw.strip() else None
    except ValueError:
        body = None
    result = evaluate(a.http_status, body, emergency_status=a.emergency_status,
                      require_supervised=a.require_supervised, expect_role=a.expect_role,
                      lease_optional=a.lease_optional)
    print(json.dumps(result, sort_keys=True))
    return EXIT[result["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
