#!/usr/bin/env python3
"""Venue adapter certification on an explicitly authorised DEMO account (Section H Step 4.2, Section 28).

Two modes. The default is the dry run; a real run needs ``--execute`` and everything listed below.

    # dry run: preflight + the full catalogue over a MOCKED transport (no network, scratch register)
    python scripts/run_venue_certification.py --venue bybit --dry-run

    # real run on the venue's DEMO host (reads only unless --allow-orders)
    python scripts/run_venue_certification.py --venue bybit --execute --environment DEMO \\
        --account-id brk_... --user-id <uuid> --operator <name> --authorization-ref <owner's written authorisation> \\
        --scope "<what was authorised>" --symbol BTCUSDT --max-notional 200 --allow-orders --out-dir <dir>

What a real run refuses, before any client is built:
  * an environment other than DEMO (LIVE cannot be named; the harness and the register refuse it again);
  * a missing owner authorisation reference, operator, scope or notional bound;
  * an account whose stored environment is not DEMO, or whose host is not the venue's canonical DEMO host;
  * --allow-orders while DEMO_ORDER_SUBMISSION_ENABLED is off, or while another process holds the canonical
    runtime lease (the run takes the lease itself and gives it back; it never takes it over from a live holder).

Credentials are never passed to this script. They are the connected demo account's, resolved by
``shared_lib.broker.resolver`` from the platform database (``DATABASE_URL``) with the key named by
``BROKER_SECRET_KEY``; this script prints neither. The evidence (``--out-dir``) is the run's events, checks and
actions as the hash-chained register holds them. Write it outside ``docs/research`` (hash-pinned).

A dry run proves the harness, never a venue: its register is a scratch database and the run records neither
CERTIFIED nor FAILED. Genuine venue certification stays PENDING until a real run is green.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BOT = os.path.join(REPO, "backends", "bot-backend")
sys.path[:0] = [BOT, os.path.join(REPO, "backends", "shared")]
VENUES = ("binance", "bybit", "bingx")
MAX_PERMIT_MINUTES = 120


def _commit() -> str:
    try:
        return subprocess.run(["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True,
                              timeout=10).stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _write(out_dir: str | None, document: dict, venue: str, kind: str) -> str | None:
    if not out_dir:
        return None
    if os.path.abspath(out_dir).startswith(os.path.join(REPO, "docs", "research")):
        raise SystemExit("REFUSED: evidence must not be written under docs/research (hash-pinned)")
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(out_dir, f"{venue}_{kind}_certification_{stamp}.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(document, handle, indent=2, sort_keys=True, default=str)
    return path


def dry_run(args: argparse.Namespace) -> int:
    """Preflight, then the whole catalogue over a mocked transport in a scratch database."""
    from dotenv import dotenv_values

    # what a real run would find: the operator's .env and environment, read by NAME only, before the overrides
    configured = {**dotenv_values(os.path.join(BOT, ".env")), **os.environ}
    scratch = tempfile.mkdtemp(prefix="cert_rehearsal_")
    # the dry run must not depend on (or touch) the operator's runtime configuration or database
    os.environ["APP_ENV"] = "DEVELOPMENT"
    os.environ["DATABASE_URL"] = "sqlite:///" + os.path.join(scratch, "rehearsal.db").replace("\\", "/")
    os.chdir(BOT)
    sys.path.insert(0, os.path.join(BOT, "tests", "exchange"))         # the venue wires: test support, no network
    from app.core import config
    from app.exchange.platform_adapter.certification_harness import preflight
    from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
    from shared_lib.persistence.db import DB
    from _certification_rehearsal import rehearse

    planned = preflight(args.venue, symbol=args.symbol, max_notional=args.max_notional or 0.0,
                        authorization_ref=args.authorization_ref, operator_ref=args.operator,
                        environment=args.environment, allow_orders=args.allow_orders, leverage=args.leverage,
                        env=configured)

    class _DemoOrderGate:                   # the order gate of a real DEMO run (production profile, demo orders on)
        production, DEMO_ORDER_SUBMISSION_ENABLED, LIVE_ORDER_SUBMISSION_ENABLED = True, True, False

        def __init__(self, base):
            self._base = base

        def __getattr__(self, name):
            return getattr(self._base, name)

    config.settings = _DemoOrderGate(config.settings)
    db = DB(os.path.join(scratch, "rehearsal.db"))
    ensure_cati_platform_schema(db)
    result, evidence, _fake, _wire = rehearse(args.venue, db, source_commit=_commit())
    summary = {"mode": "DRY_RUN_MOCKED_TRANSPORT", "venue_contacted": False, "venue": args.venue,
               "rehearsal_outcome": result.outcome, "recorded_status": result.recorded_status,
               "not_passed": {k: v for k, v in result.checks.items() if v["status"] != "PASS"},
               "operations_exercised": list(result.operations), "cleanup_complete": result.cleanup_complete,
               "preflight_for_a_real_run": {k: planned[k] for k in ("ready", "problems")},
               "requirements": planned.get("requirements"),
               "venue_certification": "PENDING: a dry run is not evidence about the venue"}
    summary["evidence"] = _write(args.out_dir, {**evidence, "summary": summary}, args.venue, "dry_run")
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0 if result.outcome == "PASSED" else 1


def execute(args: argparse.Namespace) -> int:
    """A real run on the DEMO host of one connected, owner-authorised demo account."""
    os.chdir(BOT)
    from dotenv import load_dotenv

    load_dotenv(".env")
    from app.exchange.platform_adapter import contract as C
    from app.exchange.platform_adapter.certification import CertificationRefused, OperatorPermit
    from app.exchange.platform_adapter.certification_harness import CertificationHarness, evidence_document, preflight
    from app.ops.runtime_ownership import RuntimeOwnership
    from shared_lib.broker.environment import BrokerEnvironment, normalize_environment
    from shared_lib.broker.resolver import resolve_broker_auth
    from shared_lib.persistence.db import DB

    missing = [name for name in ("account_id", "user_id", "operator", "authorization_ref", "scope", "out_dir")
               if not str(getattr(args, name) or "").strip()]
    if missing or not args.max_notional:
        raise SystemExit("REFUSED: a real run names " + ", ".join(missing + ([] if args.max_notional else ["max_notional"])))
    planned = preflight(args.venue, symbol=args.symbol, max_notional=args.max_notional,
                        authorization_ref=args.authorization_ref, operator_ref=args.operator,
                        environment=args.environment, allow_orders=args.allow_orders, leverage=args.leverage)
    if not planned["ready"]:
        print(json.dumps({"status": "REFUSED", "problems": planned["problems"]}, indent=2))
        return 2
    db = DB()
    auth = resolve_broker_auth(args.account_id, args.user_id, db)
    if normalize_environment(auth.environment) != BrokerEnvironment.DEMO or auth.broker_type.lower() != args.venue:
        print(json.dumps({"status": "REFUSED", "reason": "ACCOUNT_IS_NOT_A_DEMO_ACCOUNT_OF_THIS_VENUE"}, indent=2))
        return 2
    now = int(time.time() * 1000)
    permit = OperatorPermit(args.operator, args.authorization_ref, args.venue, C.DEMO, auth.account_id, args.scope,
                            args.symbol, float(args.max_notional), now,
                            now + min(args.valid_minutes, MAX_PERMIT_MINUTES) * 60_000,
                            allow_orders=args.allow_orders, leverage=args.leverage)
    lease = None
    if args.allow_orders:
        lease = RuntimeOwnership(db, database_path=db.path, database_role="certification")
        taken = lease.acquire()
        if not taken.acquired:
            print(json.dumps({"status": "REFUSED", "reason": taken.reason}, indent=2))
            return 2
    try:
        harness = CertificationHarness(db, auth, permit, source_commit=_commit())
        result = harness.run()
    except CertificationRefused as refused:
        print(json.dumps({"status": "REFUSED", "reason": str(refused)}, indent=2))
        return 2
    finally:
        if lease is not None:
            lease.release(reason="CERTIFICATION_RUN_DONE")
    document = evidence_document(db, result, mocked=False, permit=permit, source_commit=_commit())
    path = _write(args.out_dir, document, args.venue, "demo")
    print(json.dumps({"outcome": result.outcome, "recorded_status": result.recorded_status,
                      "failure_reasons": list(result.failure_reasons), "operations": list(result.operations),
                      "cleanup_complete": result.cleanup_complete, "run_id": result.run_id, "evidence": path},
                     indent=2))
    return 0 if result.outcome == "PASSED" else 1


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--venue", required=True, choices=VENUES)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="the default: preflight and a mocked-transport run")
    mode.add_argument("--execute", action="store_true", help="a real run on the venue's DEMO host")
    ap.add_argument("--environment", default="DEMO", choices=["DEMO"], help="DEMO only; LIVE cannot be named")
    ap.add_argument("--account-id")
    ap.add_argument("--user-id")
    ap.add_argument("--operator", default="")
    ap.add_argument("--authorization-ref", default="")
    ap.add_argument("--scope", default="")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--max-notional", type=float, default=None)
    ap.add_argument("--allow-orders", action="store_true")
    ap.add_argument("--leverage", type=int, default=1)
    ap.add_argument("--valid-minutes", type=int, default=60)
    ap.add_argument("--out-dir", default=None)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    return execute(args) if args.execute else dry_run(args)


if __name__ == "__main__":
    sys.exit(main())
