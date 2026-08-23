from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.run_tradingview_phase6d_broader_controlled_rollout_proof as phase6d


VERDICT_PASSED_EXECUTED = "PHASE 6E CONTROLLED NATURAL EXECUTION PASSED"
VERDICT_PASSED_SAFE_REJECTIONS = "PHASE 6E PASSED WITH SAFE REJECTIONS ONLY"
VERDICT_NEEDS_FIX = "PHASE 6E NEEDS FIX"
VERDICT_UNSAFE = "UNSAFE — DISABLE PHASE 6 LIMITED MODE"


def render_phase6e_markdown(report: phase6d.Phase6DReport) -> str:
    sections = [
        "# Phase 6E — Controlled Natural Execution Proof",
        f"Final verdict: `{report.final_verdict}`",
        "## Runtime Process Verification\n```json\n" + json.dumps(report.runtime_fingerprint, indent=2, default=str) + "\n```",
        "## Runtime Fingerprint\n```json\n" + json.dumps(report.runtime_fingerprint, indent=2, default=str) + "\n```",
        "## Phase 6E Config Applied\n```json\n" + json.dumps(report.config_applied, indent=2, default=str) + "\n```",
        f"## Validation Result\n`{report.validation_result}`",
        "## Candidate Precheck Summary\n```json\n" + json.dumps(report.candidate_precheck_summary, indent=2, default=str) + "\n```",
        "## Natural Execution Candidate Result\n```json\n" + json.dumps(report.natural_execution_candidate_result, indent=2, default=str) + "\n```",
        "## Positive Execution Proof\n```json\n" + json.dumps(report.natural_execution_candidate_result, indent=2, default=str) + "\n```",
        "## Negative Safety Test Results\n```json\n" + json.dumps(report.negative_safety_tests, indent=2, default=str) + "\n```",
        "## Cap and Rate-Limit Evidence\n```json\n" + json.dumps(report.rate_limit_result, indent=2, default=str) + "\n```",
        "## Queue/Decision Audit\n```json\n" + json.dumps(report.queue_decision_audit, indent=2, default=str) + "\n```",
        "## Execution/Protection Evidence\n```json\n" + json.dumps(report.execution_protection_evidence, indent=2, default=str) + "\n```",
        "## Admin Visibility Evidence\n```json\n" + json.dumps(report.admin_visibility, indent=2, default=str) + "\n```",
        "## Safety Invariant Results\n```json\n" + json.dumps(report.safety_invariants, indent=2, default=str) + "\n```",
        f"## Whether Phase 6F / Wider Rollout Is Allowed\n`{report.final_verdict == VERDICT_PASSED_EXECUTED}`",
    ]
    return "\n\n".join(sections)


def write_phase6e_report(report: phase6d.Phase6DReport, output_dir: Path) -> phase6d.Phase6DReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = output_dir / f"phase6e_controlled_natural_execution_proof_{stamp}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")
    report.json_report_path = str(json_path)
    report.markdown_report_path = str(md_path)
    json_path.write_text(json.dumps(asdict(report), indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_phase6e_markdown(report), encoding="utf-8")
    return report


def main() -> int:
    phase6d.VERDICT_PASSED_EXECUTED = VERDICT_PASSED_EXECUTED
    phase6d.VERDICT_PASSED_SAFE_REJECTIONS = VERDICT_PASSED_SAFE_REJECTIONS
    phase6d.VERDICT_NEEDS_FIX = VERDICT_NEEDS_FIX
    phase6d.VERDICT_UNSAFE = VERDICT_UNSAFE
    phase6d.write_report = write_phase6e_report
    return phase6d.main()


if __name__ == "__main__":
    raise SystemExit(main())
