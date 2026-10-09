"""Operator interface to the research register::

    python -m app.trading_intelligence.research.governance verify
    python -m app.trading_intelligence.research.governance status
    python -m app.trading_intelligence.research.governance import-history
    python -m app.trading_intelligence.research.governance record-decision --id ... --state APPROVED \\
        --decided-by "<name>" --reference "<where the decision was given>" --reason "..."
    python -m app.trading_intelligence.research.governance authorize-holdout --readiness <readiness.json> \\
        --holdout-id ... --authorized-by "<name>" --reference "<where the approval was given>" --reason "..."

``record-decision`` and ``authorize-holdout`` write the project owner's own decisions. They are run by, or on
the explicit recorded instruction of, the person named in them -- never by a script, a test or an agent on its
own initiative.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from .history import HISTORICAL_ANCHOR, import_historical_hypotheses
from .holdout import record_owner_holdout_authorization
from .register import GOVERNANCE_DECISION, ResearchRegister


def _register(args) -> ResearchRegister:
    return ResearchRegister(Path(args.register) if args.register else None)


def cmd_verify(args) -> int:
    reg = _register(args)
    anchor = tuple(HISTORICAL_ANCHOR) if (HISTORICAL_ANCHOR and not args.register) else None
    print(json.dumps({**reg.verify(anchor=anchor), "anchor_checked": anchor is not None}, indent=2))
    return 0


def cmd_status(args) -> int:
    reg = _register(args)
    recs = reg.records()
    holdouts = sorted({r["body"]["holdout_id"] for r in recs if r["type"].startswith("HOLDOUT_")})
    mandates = [r["body"]["mandate_id"] for r in recs if r["type"] == "MANDATE_REGISTERED"]
    print(json.dumps({
        "hypotheses": [{k: h[k] for k in ("hypothesis_number", "hypothesis_id", "family_id", "mandate_id", "status")}
                       for h in reg.hypotheses()],
        "multiple_testing_n": reg.hypothesis_count(),
        "mandates": [{k: v for k, v in reg.mandate(m).items() if k != "rule_artifact"} for m in mandates],
        "runs": [{k: r[k] for k in ("run_id", "run_type", "mandate_id", "code_commit", "state")} for r in reg.runs()],
        "holdouts": [{k: v for k, v in reg.holdout(h).items() if k != "events"} for h in holdouts],
        "head_hash": recs[-1]["record_hash"] if recs else None}, indent=2))
    return 0


def cmd_import_history(args) -> int:
    print(json.dumps(import_historical_hypotheses(_register(args), Path(args.source) if args.source else None), indent=2))
    return 0


def cmd_record_decision(args) -> int:
    rec = _register(args).append(GOVERNANCE_DECISION, {
        "decision_id": args.id, "state": args.state, "decided_by": args.decided_by,
        "authorization_reference": args.reference, "reason": args.reason})
    print(json.dumps({"seq": rec["seq"], "record_hash": rec["record_hash"], **rec["body"]}, indent=2))
    return 0


def cmd_holdout_authorization(args) -> int:
    readiness = json.loads(Path(args.readiness).read_text(encoding="utf-8"))
    acks = json.loads(args.acknowledgements) if args.acknowledgements else {}
    rec = record_owner_holdout_authorization(
        _register(args), holdout_id=args.holdout_id, readiness=readiness, authorized_by=args.authorized_by,
        authorization_reference=args.reference, reason=args.reason, acknowledgements=acks)
    print(json.dumps({"seq": rec["seq"], "record_hash": rec["record_hash"], **rec["body"]}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="governance", description="CATI research register")
    p.add_argument("--register", default=None, help="register file (default: the committed authoritative one)")
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("verify").set_defaults(func=cmd_verify)
    sub.add_parser("status").set_defaults(func=cmd_status)
    imp = sub.add_parser("import-history")
    imp.add_argument("--source", default=None)
    imp.set_defaults(func=cmd_import_history)
    dec = sub.add_parser("record-decision")
    for name in ("--id", "--state", "--decided-by", "--reference", "--reason"):
        dec.add_argument(name, required=True)
    dec.set_defaults(func=cmd_record_decision)
    auth = sub.add_parser("authorize-holdout")
    for name in ("--readiness", "--holdout-id", "--authorized-by", "--reference", "--reason"):
        auth.add_argument(name, required=True)
    auth.add_argument("--acknowledgements", default=None, help="JSON object of points the approver acknowledges")
    auth.set_defaults(func=cmd_holdout_authorization)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
