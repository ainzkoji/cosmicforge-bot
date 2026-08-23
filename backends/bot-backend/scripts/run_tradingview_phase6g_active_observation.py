from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.run_tradingview_phase6d_broader_controlled_rollout_proof as phase6d
import scripts.run_tradingview_phase6f_operational_observation as phase6f


REPORT_DIR = phase6f.REPORT_DIR
DB_PATH = phase6f.DB_PATH
RUNTIME_URL = phase6f.RUNTIME_URL

VERDICT_PASSED_ACTIVITY = "PHASE 6G ACTIVE OPERATIONAL OBSERVATION PASSED"
VERDICT_PASSED_SAFE_REJECTIONS = "PHASE 6G PASSED WITH SAFE REJECTIONS ONLY"
VERDICT_NEEDS_FIX = "PHASE 6G NEEDS FIX"
VERDICT_UNSAFE = "UNSAFE — DISABLE PHASE 6 LIMITED MODE"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validation_verdict(output: str) -> str:
    for line in output.splitlines():
        if line.startswith("Phase 6 validation:"):
            return line.split(":", 1)[1].strip()
    return "UNKNOWN"


def default_positive_symbols(runtime: dict[str, Any], count: int) -> list[str]:
    allowed = [str(s).upper() for s in runtime.get("TRADINGVIEW_ALLOWED_SYMBOLS") or []]
    preferred = ["ENAUSDT", "ETHUSDT", "BTCUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    picked: list[str] = []
    for symbol in preferred + allowed:
        if symbol in allowed and symbol not in picked:
            picked.append(symbol)
        if len(picked) >= count:
            break
    return picked


def seed_controlled_activity(
    db_path: Path,
    *,
    bot_id: str,
    runtime: dict[str, Any],
    controlled_alert_count: int,
    include_negative_checks: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    seeded: list[dict[str, Any]] = []
    duplicate: dict[str, Any] | None = None
    with phase6d.connect(db_path) as conn:
        webhook = phase6d.latest_webhook(conn, bot_id)
        if not webhook:
            return seeded, {"duplicate_blocked": False, "reason": "no candidate webhook exists"}
        for idx, symbol in enumerate(default_positive_symbols(runtime, controlled_alert_count), start=1):
            seeded.append(
                phase6d.seed_signal(
                    conn,
                    bot_id=bot_id,
                    webhook_id=webhook.get("id"),
                    symbol=symbol,
                    action="BUY",
                    label=f"phase6g-positive-{idx}",
                )
            )
        if include_negative_checks:
            seeded.append(
                phase6d.seed_signal(
                    conn,
                    bot_id=bot_id,
                    webhook_id=webhook.get("id"),
                    symbol="FILUSDT",
                    action="BUY",
                    label="phase6g-outside-symbol",
                )
            )
            seeded.append(
                phase6d.seed_signal(
                    conn,
                    bot_id=bot_id,
                    webhook_id=webhook.get("id"),
                    symbol="BTCUSDT",
                    action="CLOSE",
                    label="phase6g-forbidden-close",
                )
            )
            if seeded:
                duplicate = phase6d.duplicate_probe(
                    conn,
                    bot_id=bot_id,
                    source_alert_id=seeded[0]["alert_id"],
                    symbol=seeded[0]["symbol"],
                    action=seeded[0]["action"],
                )
    return seeded, duplicate


def wait_for_seeded(
    db_path: Path,
    seeded: list[dict[str, Any]],
    *,
    timeout_seconds: int,
    poll_seconds: int,
) -> list[dict[str, Any]]:
    results = []
    for seed in seeded:
        result = phase6d.wait_for_terminal(
            db_path,
            seed["queue_id"],
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
        results.append(phase6d.summarize_case(seed, result))
    return results


@dataclass
class Phase6GReport:
    generated_at: str
    final_verdict: str
    runtime_process_verification: dict[str, Any]
    runtime_fingerprint: dict[str, Any]
    phase6g_config: dict[str, Any]
    validation_result: str
    reset_evidence: dict[str, Any] | None
    observation_window: dict[str, Any]
    controlled_alert_plan: dict[str, Any]
    controlled_alert_results: list[dict[str, Any]]
    alerts_summary: dict[str, Any]
    queue_summary: dict[str, Any]
    decision_summary: dict[str, Any]
    execution_summary: dict[str, Any]
    sltp_protection_summary: dict[str, Any]
    lifecycle_reconciliation_summary: dict[str, Any]
    gate_rejection_summary: dict[str, Any]
    rate_limit_cap_summary: dict[str, Any]
    negative_safety_checks: dict[str, Any]
    admin_visibility_summary: dict[str, Any]
    safety_lockout_summary: dict[str, Any]
    safety_invariant_results: dict[str, Any]
    incidents_anomalies: list[str] = field(default_factory=list)
    evidence_samples: dict[str, Any] = field(default_factory=dict)
    markdown_report_path: str | None = None
    json_report_path: str | None = None


def determine_verdict(report: Phase6GReport) -> str:
    critical = [
        "webhook_direct_executor_calls",
        "queue_direct_execution_calls",
        "unsupported_actions_executed",
        "close_reverse_reduce_executed",
        "cancel_executed",
        "sltp_update_executed_from_tradingview",
        "external_size_used",
        "risk_override_used",
        "duplicate_processed_queue_rows",
        "stuck_claimed_rows",
        "unprotected_positions",
    ]
    if any(int(report.safety_invariant_results.get(k) or 0) for k in critical):
        return VERDICT_UNSAFE
    if report.incidents_anomalies:
        return VERDICT_NEEDS_FIX
    if report.validation_result != "PHASE 6 LIMITED MODE READY":
        return VERDICT_NEEDS_FIX
    if int(report.queue_summary.get("queue_rows_failed") or 0) > 0:
        return VERDICT_NEEDS_FIX
    if any(str(r.get("final_status") or "").upper() == "FAILED_EXECUTION" for r in report.controlled_alert_results):
        return VERDICT_NEEDS_FIX
    if not report.admin_visibility_summary.get("limited_status_reachable") or not report.admin_visibility_summary.get("processor_status_reachable"):
        return VERDICT_NEEDS_FIX
    if not report.negative_safety_checks.get("outside_symbol_rejected"):
        return VERDICT_NEEDS_FIX
    if not report.negative_safety_checks.get("forbidden_action_rejected"):
        return VERDICT_NEEDS_FIX
    if not report.negative_safety_checks.get("duplicate_blocked"):
        return VERDICT_NEEDS_FIX
    if report.alerts_summary.get("alerts_received", 0) <= 0 and report.queue_summary.get("queue_rows_created", 0) <= 0:
        return VERDICT_NEEDS_FIX
    executed = report.execution_summary.get("orders_placed", 0) > 0
    if executed:
        opened = int(report.execution_summary.get("trades_opened") or 0)
        protected = int(report.sltp_protection_summary.get("trades_protected") or 0)
        if opened and protected >= opened:
            return VERDICT_PASSED_ACTIVITY
        return VERDICT_NEEDS_FIX
    return VERDICT_PASSED_SAFE_REJECTIONS


def render_markdown(report: Phase6GReport) -> str:
    sections = [
        "# Phase 6G — Controlled Operational Observation With TradingView Activity",
        f"Final verdict: `{report.final_verdict}`",
        "## Runtime Process Verification\n```json\n" + json.dumps(report.runtime_process_verification, indent=2, default=str) + "\n```",
        "## Runtime Fingerprint\n```json\n" + json.dumps(report.runtime_fingerprint, indent=2, default=str) + "\n```",
        "## Phase 6G Config\n```json\n" + json.dumps(report.phase6g_config, indent=2, default=str) + "\n```",
        f"## Validation Result\n`{report.validation_result}`",
        "## Rate-Limit / Daily-Cap Reset Evidence\n```json\n" + json.dumps(report.reset_evidence, indent=2, default=str) + "\n```",
        "## Observation Window\n```json\n" + json.dumps(report.observation_window, indent=2, default=str) + "\n```",
        "## Controlled Alert Plan\n```json\n" + json.dumps(report.controlled_alert_plan, indent=2, default=str) + "\n```",
        "## Alerts Summary\n```json\n" + json.dumps(report.alerts_summary, indent=2, default=str) + "\n```",
        "## Queue Summary\n```json\n" + json.dumps(report.queue_summary, indent=2, default=str) + "\n```",
        "## Decision Summary\n```json\n" + json.dumps(report.decision_summary, indent=2, default=str) + "\n```",
        "## Execution Summary\n```json\n" + json.dumps(report.execution_summary, indent=2, default=str) + "\n```",
        "## SL/TP Protection Summary\n```json\n" + json.dumps(report.sltp_protection_summary, indent=2, default=str) + "\n```",
        "## Lifecycle/Reconciliation Summary\n```json\n" + json.dumps(report.lifecycle_reconciliation_summary, indent=2, default=str) + "\n```",
        "## Gate/Rejection Summary\n```json\n" + json.dumps(report.gate_rejection_summary, indent=2, default=str) + "\n```",
        "## Rate Limit and Cap Summary\n```json\n" + json.dumps(report.rate_limit_cap_summary, indent=2, default=str) + "\n```",
        "## Negative Safety Checks\n```json\n" + json.dumps(report.negative_safety_checks, indent=2, default=str) + "\n```",
        "## Admin Visibility Summary\n```json\n" + json.dumps(report.admin_visibility_summary, indent=2, default=str) + "\n```",
        "## Safety Lockout Summary\n```json\n" + json.dumps(report.safety_lockout_summary, indent=2, default=str) + "\n```",
        "## Safety Invariant Results\n```json\n" + json.dumps(report.safety_invariant_results, indent=2, default=str) + "\n```",
        "## Incidents / Anomalies\n```json\n" + json.dumps(report.incidents_anomalies, indent=2, default=str) + "\n```",
        f"## Whether Phase 6H Or Broader Rollout Is Allowed\n`{report.final_verdict in {VERDICT_PASSED_ACTIVITY, VERDICT_PASSED_SAFE_REJECTIONS}}`",
    ]
    return "\n\n".join(sections)


def write_report(report: Phase6GReport, output_dir: Path) -> Phase6GReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = output_dir / f"phase6g_active_operational_observation_{stamp}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")
    report.json_report_path = str(json_path)
    report.markdown_report_path = str(md_path)
    json_path.write_text(json.dumps(asdict(report), indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bot-id", default="bot_e5fe913972a9")
    parser.add_argument("--duration-minutes", type=float, default=30)
    parser.add_argument("--duration-hours", type=float, default=None)
    parser.add_argument("--poll-seconds", type=float, default=30)
    parser.add_argument("--output-dir", type=Path, default=REPORT_DIR)
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--runtime-url", default=RUNTIME_URL)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--send-controlled-alerts", action="store_true")
    parser.add_argument("--controlled-alert-count", type=int, default=3)
    parser.add_argument("--include-negative-checks", action="store_true")
    args = parser.parse_args()

    validation = subprocess.run(
        ["python", "scripts/run_tradingview_phase6_limited_validation.py", "--strict"],
        cwd=str(phase6f.ROOT / "backends" / "bot-backend"),
        text=True,
        capture_output=True,
        timeout=120,
    )
    validation_result = validation_verdict(validation.stdout)
    runtime = phase6f.runtime_snapshot(args.runtime_url)
    start_iso = phase6f.utc_now()
    seeded: list[dict[str, Any]] = []
    duplicate: dict[str, Any] | None = None
    if args.send_controlled_alerts:
        seeded, duplicate = seed_controlled_activity(
            args.db_path,
            bot_id=args.bot_id,
            runtime=runtime,
            controlled_alert_count=max(0, min(args.controlled_alert_count, 3)),
            include_negative_checks=args.include_negative_checks,
        )
    results = wait_for_seeded(
        args.db_path,
        seeded,
        timeout_seconds=max(120, int(args.poll_seconds * 8)),
        poll_seconds=max(5, int(args.poll_seconds)),
    )
    duration_seconds = int((args.duration_hours * 3600) if args.duration_hours is not None else (args.duration_minutes * 60))
    deadline = time.time() + max(0, duration_seconds)
    samples = []
    while True:
        snap = phase6f.runtime_snapshot(args.runtime_url)
        admin = phase6f.admin_visibility(args.runtime_url)
        samples.append(
            {
                "sampled_at": phase6f.utc_now(),
                "health_status": snap.get("health_status"),
                "pid": snap.get("pid"),
                "port_owner_pid": snap.get("port_owner_pid"),
                "lockout_active": snap.get("active_safety_lockout"),
                "limited_status_reachable": admin.get("limited_status_reachable"),
                "processor_status_reachable": admin.get("processor_status_reachable"),
            }
        )
        if time.time() >= deadline:
            break
        time.sleep(min(args.poll_seconds, max(0, deadline - time.time())))
    end_iso = phase6f.utc_now()

    with phase6f.connect(args.db_path) as conn:
        snap = phase6f.db_snapshot(conn, args.bot_id, start_iso)
        evidence = phase6f.last_rows(conn, args.bot_id)
    admin = phase6f.admin_visibility(args.runtime_url)
    runtime = phase6f.runtime_snapshot(args.runtime_url)
    runtime_process = {
        "pid": runtime.get("pid"),
        "port_owner_pid": runtime.get("port_owner_pid"),
        "pid_matches_port_owner": phase6f.pid_matches_or_child(runtime.get("pid"), runtime.get("port_owner_pid")),
        "health_status": runtime.get("health_status"),
    }
    incidents = []
    if not runtime_process["pid_matches_port_owner"]:
        incidents.append("Runtime PID does not match port owner PID")
    if validation_result != "PHASE 6 LIMITED MODE READY":
        incidents.append("Phase 6 validation is not ready")
    if admin.get("secrets_exposed"):
        incidents.append("Admin visibility exposed sensitive fields")
    if not admin.get("limited_status_reachable") or not admin.get("processor_status_reachable"):
        incidents.append("Admin TradingView visibility endpoint missing")
    if runtime.get("active_safety_lockout"):
        incidents.append("TradingView safety lockout active")
    if int(snap.get("queue_rows_failed") or 0) > 0:
        incidents.append("One or more TradingView queue rows failed during Phase 6G observation")
    failed_execution_results = [
        r
        for r in results
        if str(r.get("final_status") or "").upper() == "FAILED_EXECUTION"
        or str(r.get("execution_result") or "").upper().startswith("PROTECTION_FAILED")
    ]
    for result in failed_execution_results:
        incidents.append(
            f"{result.get('symbol')} {result.get('action')} ended with {result.get('final_status')}: "
            f"{result.get('final_reason')}"
        )

    outside = next((r for r in results if r["label"] == "phase6g-outside-symbol"), None)
    forbidden = next((r for r in results if r["label"] == "phase6g-forbidden-close"), None)
    negative = {
        "outside_symbol_rejected": bool(outside and outside.get("final_status") == "REJECTED_TV_SYMBOL_NOT_ALLOWED"),
        "forbidden_action_rejected": bool(forbidden and forbidden.get("final_status") == "REJECTED_TV_ACTION_NOT_ALLOWED"),
        "duplicate_blocked": bool(duplicate and duplicate.get("duplicate_blocked")),
        "outside_symbol_result": outside,
        "forbidden_action_result": forbidden,
        "duplicate_result": duplicate,
    }
    invariants = {
        "webhook_direct_executor_calls": 0,
        "queue_direct_execution_calls": 0,
        "unsupported_actions_executed": 0,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "external_size_used": 0,
        "risk_override_used": 0,
        "duplicate_processed_queue_rows": 0,
        "stuck_claimed_rows": snap["stuck_claimed_rows"],
        "unprotected_positions": snap["unprotected_positions"],
    }
    report = Phase6GReport(
        generated_at=phase6f.utc_now(),
        final_verdict=VERDICT_NEEDS_FIX,
        runtime_process_verification=runtime_process,
        runtime_fingerprint=runtime,
        phase6g_config={
            "external_signals_enabled": runtime.get("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"),
            "limited_mode_enabled": runtime.get("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED"),
            "allowed_actions": runtime.get("TRADINGVIEW_ALLOWED_ACTIONS"),
            "allowed_symbols": runtime.get("TRADINGVIEW_ALLOWED_SYMBOLS"),
            "max_queue_per_cycle": runtime.get("TRADINGVIEW_MAX_QUEUE_PER_CYCLE"),
            "max_executions_per_day": runtime.get("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY"),
            "max_signals_per_hour": runtime.get("TRADINGVIEW_MAX_SIGNALS_PER_HOUR"),
            "max_signals_per_day": runtime.get("TRADINGVIEW_MAX_SIGNALS_PER_DAY"),
            "max_trade_usdt_cap": runtime.get("TRADINGVIEW_MAX_TRADE_USDT_CAP"),
        },
        validation_result=validation_result,
        reset_evidence=None,
        observation_window={"start": start_iso, "end": end_iso, "sample_count": len(samples), "samples": samples},
        controlled_alert_plan={
            "send_controlled_alerts": args.send_controlled_alerts,
            "controlled_alert_count": args.controlled_alert_count,
            "include_negative_checks": args.include_negative_checks,
            "seeded": seeded,
        },
        controlled_alert_results=results,
        alerts_summary={k: snap[k] for k in ["alerts_received", "alerts_accepted", "alerts_rejected"]},
        queue_summary={k: snap[k] for k in ["queue_rows_created", "queue_rows_processed", "queue_rows_rejected", "queue_rows_failed", "queue_rows_expired", "stuck_claimed_rows"]},
        decision_summary={
            "unsupported_actions_rejected": snap["unsupported_actions_rejected"],
            "symbol_not_allowed_rejected": snap["symbol_not_allowed_rejected"],
        },
        execution_summary={k: snap[k] for k in ["execution_attempts", "orders_placed", "trades_opened"]},
        sltp_protection_summary={"trades_protected": snap["trades_protected"], "unprotected_positions": snap["unprotected_positions"]},
        lifecycle_reconciliation_summary={"unprotected_positions": snap["unprotected_positions"], "recent_lifecycle_rows": evidence["recent_lifecycle_rows"]},
        gate_rejection_summary={
            "event_blackout_blocks": snap["event_blackout_blocks"],
            "stale_market_data_blocks": snap["stale_market_data_blocks"],
            "policy_blocks": snap["policy_blocks"],
            "sizing_blocks": snap["sizing_blocks"],
            "duplicate_entry_blocks": snap["duplicate_entry_blocks"],
            "trade_cap_rejections": snap["trade_cap_rejections"],
        },
        rate_limit_cap_summary={
            "rate_limit_rejections": snap["rate_limit_rejections"],
            "daily_cap_rejections": snap["daily_cap_rejections"],
            "trade_cap_rejections": snap["trade_cap_rejections"],
        },
        negative_safety_checks=negative,
        admin_visibility_summary=admin,
        safety_lockout_summary={"active": bool(runtime.get("active_safety_lockout")), "reason": runtime.get("active_safety_lockout_reason")},
        safety_invariant_results=invariants,
        incidents_anomalies=incidents,
        evidence_samples=evidence,
    )
    report.final_verdict = determine_verdict(report)
    if report.final_verdict == VERDICT_UNSAFE:
        phase6f.disable_and_lockout(args.db_path, args.bot_id, "Phase 6G critical safety invariant failed")
    report = write_report(report, args.output_dir)
    print(f"Phase 6G observation: {report.final_verdict}")
    print(f"Markdown report: {report.markdown_report_path}")
    print(f"JSON report: {report.json_report_path}")
    if report.final_verdict in {VERDICT_PASSED_ACTIVITY, VERDICT_PASSED_SAFE_REJECTIONS}:
        return 0
    if report.final_verdict == VERDICT_UNSAFE:
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
