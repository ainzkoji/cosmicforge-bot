#!/usr/bin/env python3
"""Authenticated Bybit Demo / BingX VST validation for ONE user-connected broker account.

The account must have been connected by its user through the normal app flow (Broker Connection -> Demo). Its
credentials are resolved by the canonical resolver; this script never reads, prints, or stores a secret.

    python scripts/validate_demo_venue.py --account-id brk_... --user-id <uuid> --symbol BTCUSDT
    python scripts/validate_demo_venue.py --account-id brk_... --user-id <uuid> --symbol BTCUSDT --submit-demo-orders

Without ``--submit-demo-orders`` only the authenticated reads (A-H) run. With it, one minimum-size demo MARKET
entry is placed, looked up, reconciled, protected, re-read by a fresh client, then closed (I-O + CLEANUP).
Refused for any non-DEMO account. Evidence: docs/research/venue_validation/<venue>_demo_<utc>.json.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(REPO, "backends", "bot-backend"), os.path.join(REPO, "backends", "shared")]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--account-id", required=True)
    ap.add_argument("--user-id", required=True)
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--submit-demo-orders", action="store_true")
    ap.add_argument("--out-dir", default=os.path.join(REPO, "docs", "research", "venue_validation"))
    args = ap.parse_args()

    os.chdir(os.path.join(REPO, "backends", "bot-backend"))
    from dotenv import load_dotenv

    load_dotenv(".env")
    from shared_lib.broker.client_factory import build_client_from_auth
    from shared_lib.broker.resolver import resolve_broker_auth
    from shared_lib.persistence.db import DB

    from app.venue_validation.demo_validation import ValidationRefused, run_validation

    auth = resolve_broker_auth(args.account_id, args.user_id, DB(), allow_unvalidated=True)
    commit = subprocess.run(["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    try:
        evidence = run_validation(auth=auth, client_factory=build_client_from_auth, symbol=args.symbol,
                                  submit_orders=args.submit_demo_orders, code_version=commit or None)
    except ValidationRefused as exc:
        print(json.dumps({"status": "REFUSED", "reason": str(exc)}, indent=2))
        return 2
    os.makedirs(args.out_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(args.out_dir, f"{evidence['venue']}_demo_{stamp}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2, sort_keys=True)
    print(json.dumps({"verdict": evidence["verdict"], "results": evidence["results"], "blocking": evidence["blocking"],
                      "evidence": os.path.relpath(path, REPO).replace(os.sep, "/")}, indent=2))
    return 0 if evidence["verdict"] == "VALIDATED" else 1


if __name__ == "__main__":
    sys.exit(main())
