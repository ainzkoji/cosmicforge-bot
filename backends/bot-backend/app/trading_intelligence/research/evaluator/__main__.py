"""Operator interface to the official Mandate 004 evaluation::

    python -m app.trading_intelligence.research.evaluator freeze-dataset      # dataset manifest -> register
    python -m app.trading_intelligence.research.evaluator register-costs      # frozen cost model -> register
    python -m app.trading_intelligence.research.evaluator development --researcher "<name>"
    python -m app.trading_intelligence.research.evaluator readiness --junit <step2-tests.xml> \\
        --no-known-critical-defect --attested-by "<name>"
    python -m app.trading_intelligence.research.evaluator holdout --researcher "<name>"
    python -m app.trading_intelligence.research.evaluator record-verdict --run-id run_... --recorded-by "<name>"
    python -m app.trading_intelligence.research.evaluator report --run-id run_...

``holdout`` evaluates the held-back period. It runs only after the project owner's authorization is in the
research register (``python -m app.trading_intelligence.research.governance authorize-holdout``); without it
the attempt is refused and logged. Nothing here trades, promotes a family or changes a governance phase.
"""
from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.research.governance import holdout as H
from app.trading_intelligence.research.governance.mandates import verify_mandate
from app.trading_intelligence.research.governance.register import (
    RUN_COMPLETED, ResearchRegister, canonical_text_sha256, repository_root,
)

from . import official as O

MANDATE_DOCS = "docs/research/mandate_004"
REPRODUCTION_DOC = f"{MANDATE_DOCS}/REPRODUCTION.md"
READINESS_FILE = f"{MANDATE_DOCS}/pre_holdout_readiness.json"
REQUIRED_TEST_MODULES = ("test_step2_trend_evaluator", "test_step2_official_evaluation", "test_step2_daily_dataset",
                         "test_step2_research_governance", "test_step2_mandate_004", "test_step2_report")


def junit_facts(path: Path) -> Dict[str, Any]:
    """What a junit file PROVES: which Step 2 test modules ran, and that nothing failed or errored."""
    root = ET.fromstring(Path(path).read_bytes())
    cases = list(root.iter("testcase"))
    bad = [c for c in cases if any(c.find(tag) is not None for tag in ("failure", "error"))]
    skipped = [c for c in cases if c.find("skipped") is not None]
    modules = {m: sum(1 for c in cases if m in (c.get("classname") or "")) for m in REQUIRED_TEST_MODULES}
    lookahead = [c for c in cases if "look_ahead" in (c.get("name") or "") or "looks_ahead" in (c.get("name") or "")]
    return {"junit_sha256": canonical_text_sha256(path), "tests": len(cases), "failed_or_errored": len(bad),
            "skipped": len(skipped), "modules": modules, "lookahead_tests": len(lookahead),
            "green": bool(cases) and not bad and all(modules.values()),
            "lookahead_green": bool(lookahead) and not any(c in bad or c in skipped for c in lookahead),
            "report_green": modules["test_step2_report"] > 0 and not bad}


def build_readiness(register: ResearchRegister, dataset: O.DatasetHandle, *, junit: Path, attested_by: str,
                    no_known_critical_defect: bool, periods: Optional[O.Periods] = None,
                    source: Optional[Dict[str, Any]] = None, repo_root: Optional[Path] = None) -> Dict[str, Any]:
    periods = periods or O.Periods()
    root = Path(repo_root) if repo_root else repository_root()
    src = source if source is not None else O.source_state()
    tests = junit_facts(junit)
    try:
        pin = verify_mandate(register, spec.MANDATE_ID, rule_artifact=spec.RULE_ARTIFACT)
    except Exception:
        pin = None
    cost = O.frozen_cost_model()
    runs = [r for r in register.runs(mandate_id=spec.MANDATE_ID) if r["run_type"] == O.DEVELOPMENT
            and r["state"] == RUN_COMPLETED and r["dataset_hash"] == dataset.dataset_hash
            and r.get("source_fingerprint") == src["fingerprint"] and pin and r["strategy_hash"] == pin["strategy_hash"]]
    dev = runs[-1] if runs else None
    result = None
    if dev:
        path = root / dev["end"]["artifact_dir"] / "result.json"
        result = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
    facts = {
        "mandate_frozen": pin is not None,
        "hypothesis_registered": any(h["hypothesis_id"] == spec.HYPOTHESIS_ID for h in register.hypotheses()),
        "dataset_frozen": any(r["body"]["manifest_hash"] == dataset.manifest_hash for r in register.of_type("DATASET_FROZEN")),
        "costs_registered": any(r["body"]["cost_model_hash"] == cost["cost_model_hash"]
                                for r in register.of_type("COST_MODEL_REGISTERED")),
        "statistical_gate_frozen": bool(result) and result["statistical_policy_hash"] == O.statistical_policy(register).policy_hash,
        "simulator_tests_green": tests["green"],
        "causality_audit_green": tests["lookahead_green"] and bool(result) and result["integrity"]["causality_verified"] is True,
        "development_run_valid": bool(dev) and dev["end"]["status"] == "VALID" and dev["end"]["verdict"] == "PENDING_HOLDOUT",
        "no_known_critical_defect": bool(no_known_critical_defect and str(attested_by or "").strip()),
        "reporting_ready": tests["report_green"],
        "reproduction_documented": (root / REPRODUCTION_DOC).is_file(),
        "source_committed_clean": src.get("evaluation_source_dirty") is False,
    }
    return H.pre_holdout_readiness(
        facts=facts, mandate_id=spec.MANDATE_ID, specification_sha256=pin["specification_sha256"] if pin else None,
        dataset_hash=dataset.dataset_hash, source_fingerprint=src["fingerprint"],
        development_run_id=dev["run_id"] if dev else None,
        evidence={"tests": tests, "attested_by": attested_by, "code_commit": src.get("commit"),
                  "development_verdict": dev["end"]["verdict"] if dev else None,
                  "development_result_hash": dev["end"]["result_hash"] if dev else None,
                  "statistical_thresholds_approved": O.statistical_policy(register).approved,
                  "holdout_id": H.holdout_id_for(spec.MANDATE_ID, dataset.dataset_hash, periods.holdout_start, periods.holdout_end),
                  "holdout_window": [periods.holdout_start, periods.holdout_end]})


def _dataset(args) -> O.DatasetHandle:
    root = repository_root()
    return O.DatasetHandle(Path(args.store) if args.store else root / "data" / "research" / "binance_usdm_daily_v1",
                           root / "docs" / "research")


def cmd_freeze_dataset(args) -> int:
    rec = O.freeze_dataset_in_register(ResearchRegister(), _dataset(args))
    print(json.dumps({"seq": rec["seq"], **rec["body"]}, indent=1))
    return 0


def cmd_register_costs(args) -> int:
    rec = O.register_cost_model(ResearchRegister())
    print(json.dumps({"seq": rec["seq"], **rec["body"]}, indent=1))
    return 0


def _summary(res: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: res[k] for k in ("run_id", "run_type", "verdict", "mandate_pass_rule", "statistical_gate", "result_hash",
                               "hypotheses_in_register", "holdout_opened_by_this_run")}
    out["integrity"] = res["integrity"]
    out["pass_rule"] = {k: {"status": v["status"], "observed": v["observed"]} for k, v in res["pass_rule"]["criteria"].items()}
    for name, block in res["blocks"].items():
        s = block["scenarios"][res["primary_scenario"]]
        out[name] = {k: s[k] for k in ("first_day", "last_day", "net_return", "annual_return", "annual_volatility", "sharpe",
                                       "max_drawdown", "gross_return")}
        out[name]["trades_closed"] = s["trades"]["trades_closed"]
    return out


def cmd_run(args) -> int:
    res = O.run_evaluation(args.run_type, dataset=_dataset(args), researcher=args.researcher,
                           parent_run_id=args.rerun_of, reason_for_rerun=args.reason)
    print(json.dumps(_summary(res), indent=1))
    return 0


def cmd_readiness(args) -> int:
    readiness = build_readiness(ResearchRegister(), _dataset(args), junit=Path(args.junit), attested_by=args.attested_by,
                                no_known_critical_defect=args.no_known_critical_defect)
    out = repository_root() / READINESS_FILE
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes((json.dumps(readiness, indent=1, sort_keys=True) + "\n").encode("utf-8"))
    print(json.dumps({k: readiness[k] for k in ("status", "reason_codes", "facts", "readiness_hash")}, indent=1))
    return 0 if readiness["status"] == H.READY else 3


def cmd_record_verdict(args) -> int:
    rec = O.record_verdict(ResearchRegister(), args.run_id, recorded_by=args.recorded_by)
    print(json.dumps({"seq": rec["seq"], **rec["body"]}, indent=1))
    return 0


def cmd_report(args) -> int:
    from .report import write_report

    path = write_report(args.run_id)
    print(json.dumps({"report": str(path)}))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="evaluator", description="CATI Mandate 004 official evaluation")
    p.add_argument("--store", default=None, help="local dataset store (default: data/research/binance_usdm_daily_v1)")
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("freeze-dataset").set_defaults(func=cmd_freeze_dataset)
    sub.add_parser("register-costs").set_defaults(func=cmd_register_costs)
    for name, run_type in (("development", O.DEVELOPMENT), ("holdout", O.HOLDOUT)):
        sp = sub.add_parser(name)
        sp.add_argument("--researcher", required=True)
        sp.add_argument("--rerun-of", default=None, help="run id this run repeats (a failed or defective run)")
        sp.add_argument("--reason", default=None, help="why the rerun is needed")
        sp.set_defaults(func=cmd_run, run_type=run_type)
    rd = sub.add_parser("readiness")
    rd.add_argument("--junit", required=True, help="junit XML of the Step 2 test modules")
    rd.add_argument("--attested-by", required=True)
    rd.add_argument("--no-known-critical-defect", action="store_true")
    rd.set_defaults(func=cmd_readiness)
    rv = sub.add_parser("record-verdict")
    rv.add_argument("--run-id", required=True)
    rv.add_argument("--recorded-by", required=True)
    rv.set_defaults(func=cmd_record_verdict)
    rp = sub.add_parser("report")
    rp.add_argument("--run-id", default=None, help="default: the latest completed run")
    rp.set_defaults(func=cmd_report)
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
