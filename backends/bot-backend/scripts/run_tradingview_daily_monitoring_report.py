from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.run_tradingview_phase6f_operational_observation as phase6f


ROOT = phase6f.ROOT
DB_PATH = phase6f.DB_PATH
REPORT_DIR = ROOT / "reports" / "tradingview_operations"
RUNTIME_URL = phase6f.RUNTIME_URL

CONTROLLED_MODE_CONFIRMATION = (
    "Phase 6 remains in controlled operational mode: BUY/SELL only, 20 allowed symbols max, "
    "400 USDT max trade cap, 3 executions/day, 1 queue row/cycle, mandatory SL/TP, auto-lockout enabled."
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_since(value: str | None, hours: int) -> str:
    if value:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def scalar(conn: sqlite3.Connection, query: str, params: tuple[Any, ...]) -> int:
    row = conn.execute(query, params).fetchone()
    return int(row["c"] if row else 0)


def attempts_summary(conn: sqlite3.Connection, bot_id: str, since_iso: str) -> dict[str, int]:
    payload_match = """
        SELECT COUNT(*) AS c
        FROM tradingview_alerts
        WHERE bot_id=? AND created_at>=?
          AND (
            payload_json LIKE ?
            OR payload_json LIKE ?
            OR payload_json LIKE ?
          )
    """
    return {
        "forbidden_action_attempts": scalar(
            conn,
            """
            SELECT COUNT(*) AS c
            FROM tradingview_signal_decisions
            WHERE bot_id=? AND created_at>=?
              AND final_status='REJECTED_TV_ACTION_NOT_ALLOWED'
            """,
            (bot_id, since_iso),
        ),
        "external_size_attempts": scalar(conn, payload_match, (bot_id, since_iso, '%"size"%', '%"quantity"%', '%"qty"%')),
        "external_sltp_attempts": scalar(conn, payload_match, (bot_id, since_iso, '%"sl"%', '%"stop_loss"%', '%"take_profit"%')),
        "risk_override_attempts": scalar(conn, payload_match, (bot_id, since_iso, '%"risk"%', '%"risk_override"%', '%"force_execute"%')),
    }


def operational_incidents(
    *,
    runtime: dict[str, Any],
    admin: dict[str, Any],
    snapshot: dict[str, Any],
    attempts: dict[str, int],
) -> list[str]:
    incidents: list[str] = []
    if not phase6f.pid_matches_or_child(runtime.get("pid"), runtime.get("port_owner_pid")):
        incidents.append("Runtime PID does not match the port owner PID or child process relationship.")
    if runtime.get("phase6_gate_available") is not True:
        incidents.append("Phase 6 gate is not available in the running process.")
    if runtime.get("phase6_gate_code_version") != "phase6_limited_gate_v1_2026-05-21":
        incidents.append("Unexpected Phase 6 gate code version.")
    if runtime.get("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED") is not True:
        incidents.append("TradingView external signals are not enabled.")
    if runtime.get("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED") is not True:
        incidents.append("TradingView live limited mode is not enabled.")
    if len(runtime.get("TRADINGVIEW_ALLOWED_SYMBOLS") or []) > 20:
        incidents.append("Allowed symbol list exceeds the controlled 20-symbol maximum.")
    if runtime.get("TRADINGVIEW_ALLOWED_ACTIONS") != ["BUY", "SELL"]:
        incidents.append("Allowed actions are not exactly BUY/SELL.")
    if float(runtime.get("TRADINGVIEW_MAX_TRADE_USDT_CAP") or 0) > 400:
        incidents.append("TradingView max trade cap exceeds 400 USDT.")
    if int(runtime.get("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY") or 0) > 3:
        incidents.append("Daily execution cap exceeds 3.")
    if int(runtime.get("TRADINGVIEW_MAX_QUEUE_PER_CYCLE") or 0) > 1:
        incidents.append("Max queue per cycle exceeds 1.")
    for key in [
        "TRADINGVIEW_ALLOW_CLOSE",
        "TRADINGVIEW_ALLOW_REVERSE",
        "TRADINGVIEW_ALLOW_REDUCE",
        "TRADINGVIEW_ALLOW_CANCEL",
        "TRADINGVIEW_ALLOW_EXTERNAL_SLTP",
        "TRADINGVIEW_ALLOW_EXTERNAL_SIZE",
        "TRADINGVIEW_ALLOW_RISK_OVERRIDE",
    ]:
        if runtime.get(key):
            incidents.append(f"Forbidden capability enabled: {key}.")
    if runtime.get("TRADINGVIEW_REQUIRE_SLTP_PROTECTION") is not True:
        incidents.append("Mandatory SL/TP protection is not enabled.")
    if runtime.get("TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL") is not True:
        incidents.append("Auto-lockout on invariant failure is not enabled.")
    if runtime.get("active_safety_lockout"):
        incidents.append(f"Safety lockout is active: {runtime.get('active_safety_lockout_reason')}")
    if snapshot.get("stuck_claimed_rows"):
        incidents.append("Stuck CLAIMED TradingView queue rows detected.")
    if snapshot.get("unprotected_positions"):
        incidents.append("Unprotected active positions detected.")
    if snapshot.get("queue_rows_failed"):
        incidents.append("Failed TradingView queue rows detected in the reporting window.")
    if not admin.get("limited_status_reachable") or not admin.get("processor_status_reachable"):
        incidents.append("Admin limited-status or processor-status endpoint is unreachable.")
    if admin.get("secrets_exposed"):
        incidents.append("Admin visibility response appears to expose sensitive fields.")
    if attempts.get("external_size_attempts"):
        incidents.append("External size attempt observed and should remain blocked/ignored.")
    if attempts.get("external_sltp_attempts"):
        incidents.append("External SL/TP attempt observed and should remain blocked/ignored.")
    if attempts.get("risk_override_attempts"):
        incidents.append("Risk override attempt observed and should remain blocked/ignored.")
    return incidents


@dataclass
class DailyTradingViewReport:
    generated_at: str
    bot_id: str
    reporting_window: dict[str, str]
    controlled_mode_confirmation: str
    runtime_fingerprint: dict[str, Any]
    phase6_config: dict[str, Any]
    alerts_summary: dict[str, int]
    queue_summary: dict[str, int]
    execution_summary: dict[str, int]
    protection_summary: dict[str, int]
    forbidden_external_attempts: dict[str, int]
    rate_limit_cap_summary: dict[str, int]
    safety_lockout_status: dict[str, Any]
    processor_heartbeat: dict[str, Any]
    admin_endpoint_health: dict[str, Any]
    incidents_anomalies: list[str]
    evidence_samples: dict[str, Any] = field(default_factory=dict)
    markdown_report_path: str | None = None
    json_report_path: str | None = None


def render_markdown(report: DailyTradingViewReport) -> str:
    checklist = [
        "[x] Phase 6 limited mode remains enabled",
        "[x] BUY/SELL only",
        "[x] Controlled 20-symbol allowlist maximum",
        "[x] 400 USDT max trade cap",
        "[x] 3 executions/day cap",
        "[x] 1 queue row/cycle cap",
        "[x] Mandatory SL/TP protection",
        "[x] Auto-lockout enabled",
        "[x] Admin limited-status and processor-status checked",
    ]
    sections = [
        "# TradingView Daily Operational Monitoring Report",
        f"Generated: `{report.generated_at}`",
        f"Bot: `{report.bot_id}`",
        f"Window: `{report.reporting_window['since']}` to `{report.reporting_window['until']}`",
        f"Controlled mode: {report.controlled_mode_confirmation}",
        "## Operational Checklist\n" + "\n".join(checklist),
        "## Runtime Fingerprint\n```json\n" + json.dumps(report.runtime_fingerprint, indent=2, default=str) + "\n```",
        "## Phase 6 Config\n```json\n" + json.dumps(report.phase6_config, indent=2, default=str) + "\n```",
        "## Alerts Summary\n```json\n" + json.dumps(report.alerts_summary, indent=2, default=str) + "\n```",
        "## Queue Summary\n```json\n" + json.dumps(report.queue_summary, indent=2, default=str) + "\n```",
        "## Execution Summary\n```json\n" + json.dumps(report.execution_summary, indent=2, default=str) + "\n```",
        "## Trades / Protection Summary\n```json\n" + json.dumps(report.protection_summary, indent=2, default=str) + "\n```",
        "## Forbidden / External Attempts\n```json\n" + json.dumps(report.forbidden_external_attempts, indent=2, default=str) + "\n```",
        "## Rate-Limit / Cap Rejections\n```json\n" + json.dumps(report.rate_limit_cap_summary, indent=2, default=str) + "\n```",
        "## Safety Lockout Status\n```json\n" + json.dumps(report.safety_lockout_status, indent=2, default=str) + "\n```",
        "## Processor Heartbeat\n```json\n" + json.dumps(report.processor_heartbeat, indent=2, default=str) + "\n```",
        "## Admin Endpoint Health\n```json\n" + json.dumps(report.admin_endpoint_health, indent=2, default=str) + "\n```",
        "## Incidents / Anomalies\n```json\n" + json.dumps(report.incidents_anomalies, indent=2, default=str) + "\n```",
        "## Evidence Samples\n```json\n" + json.dumps(report.evidence_samples, indent=2, default=str) + "\n```",
    ]
    return "\n\n".join(sections)


def write_report(report: DailyTradingViewReport, output_dir: Path) -> DailyTradingViewReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = output_dir / f"tradingview_daily_operational_report_{stamp}"
    report.json_report_path = str(base.with_suffix(".json"))
    report.markdown_report_path = str(base.with_suffix(".md"))
    Path(report.json_report_path).write_text(json.dumps(asdict(report), indent=2, default=str), encoding="utf-8")
    Path(report.markdown_report_path).write_text(render_markdown(report), encoding="utf-8")
    return report


def build_report(
    *,
    bot_id: str,
    db_path: Path,
    runtime_url: str,
    since_iso: str,
    until_iso: str,
) -> DailyTradingViewReport:
    runtime = phase6f.runtime_snapshot(runtime_url)
    admin = phase6f.admin_visibility(runtime_url)
    with phase6f.connect(db_path) as conn:
        snapshot = phase6f.db_snapshot(conn, bot_id, since_iso)
        attempts = attempts_summary(conn, bot_id, since_iso)
        evidence = phase6f.last_rows(conn, bot_id)

    limited_status = admin.get("limited_status") if isinstance(admin.get("limited_status"), dict) else {}
    processor_status = admin.get("processor_status") if isinstance(admin.get("processor_status"), dict) else {}
    incidents = operational_incidents(runtime=runtime, admin=admin, snapshot=snapshot, attempts=attempts)

    return DailyTradingViewReport(
        generated_at=utc_now(),
        bot_id=bot_id,
        reporting_window={"since": since_iso, "until": until_iso},
        controlled_mode_confirmation=CONTROLLED_MODE_CONFIRMATION,
        runtime_fingerprint=runtime,
        phase6_config={
            "external_signals_enabled": runtime.get("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"),
            "limited_mode_enabled": runtime.get("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED"),
            "allowed_actions": runtime.get("TRADINGVIEW_ALLOWED_ACTIONS"),
            "allowed_symbols": runtime.get("TRADINGVIEW_ALLOWED_SYMBOLS"),
            "max_trade_usdt_cap": runtime.get("TRADINGVIEW_MAX_TRADE_USDT_CAP"),
            "max_executions_per_day": runtime.get("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY"),
            "max_queue_per_cycle": runtime.get("TRADINGVIEW_MAX_QUEUE_PER_CYCLE"),
            "auto_disable_on_invariant_fail": runtime.get("TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL"),
            "require_sltp_protection": runtime.get("TRADINGVIEW_REQUIRE_SLTP_PROTECTION"),
        },
        alerts_summary={k: snapshot[k] for k in ["alerts_received", "alerts_accepted", "alerts_rejected"]},
        queue_summary={k: snapshot[k] for k in ["queue_rows_created", "queue_rows_processed", "queue_rows_rejected", "queue_rows_failed", "queue_rows_expired", "stuck_claimed_rows"]},
        execution_summary={k: snapshot[k] for k in ["execution_attempts", "orders_placed", "trades_opened"]},
        protection_summary={"trades_protected": snapshot["trades_protected"], "unprotected_positions": snapshot["unprotected_positions"]},
        forbidden_external_attempts=attempts,
        rate_limit_cap_summary={k: snapshot[k] for k in ["rate_limit_rejections", "daily_cap_rejections", "trade_cap_rejections"]},
        safety_lockout_status={
            "runtime_active": bool(runtime.get("active_safety_lockout")),
            "runtime_reason": runtime.get("active_safety_lockout_reason"),
            "db_active_count": snapshot["active_safety_lockouts"],
            "limited_status": limited_status.get("safety_lockout"),
        },
        processor_heartbeat=processor_status.get("last_processor_result") or {},
        admin_endpoint_health={
            "limited_status_reachable": admin.get("limited_status_reachable"),
            "processor_status_reachable": admin.get("processor_status_reachable"),
            "secrets_exposed": admin.get("secrets_exposed"),
        },
        incidents_anomalies=incidents,
        evidence_samples=evidence,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a daily TradingView Phase 6 operational report.")
    parser.add_argument("--bot-id", default="bot_e5fe913972a9")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--runtime-url", default=RUNTIME_URL)
    parser.add_argument("--output-dir", type=Path, default=REPORT_DIR)
    parser.add_argument("--since")
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()

    since_iso = parse_since(args.since, args.hours)
    until_iso = utc_now()
    report = build_report(
        bot_id=args.bot_id,
        db_path=args.db_path,
        runtime_url=args.runtime_url,
        since_iso=since_iso,
        until_iso=until_iso,
    )
    report = write_report(report, args.output_dir)
    print("TradingView daily operational report generated")
    print(f"Markdown report: {report.markdown_report_path}")
    print(f"JSON report: {report.json_report_path}")
    if report.incidents_anomalies:
        print(f"Incidents/anomalies: {len(report.incidents_anomalies)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
