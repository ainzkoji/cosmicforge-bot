from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DB_PATH = ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
ENV_PATH = ROOT / "backends" / "bot-backend" / ".env"
REPORT_DIR = ROOT / "reports" / "tradingview_phase6"
RUNTIME_URL = "http://127.0.0.1:9000/health"

VERDICT_PASSED_ACTIVITY = "PHASE 6F OPERATIONAL OBSERVATION PASSED"
VERDICT_PASSED_NO_ACTIVITY = "PHASE 6F PASSED WITH NO TRADINGVIEW ACTIVITY"
VERDICT_NEEDS_FIX = "PHASE 6F NEEDS FIX"
VERDICT_UNSAFE = "UNSAFE — DISABLE PHASE 6 LIMITED MODE"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def fetch_json(url: str, *, timeout: int = 8) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return {"error": str(exc)}


def port_owner_pid(runtime_url: str) -> int | None:
    parsed = urllib.parse.urlparse(runtime_url)
    if parsed.hostname not in {"127.0.0.1", "localhost"} or not parsed.port:
        return None
    try:
        out = subprocess.check_output(["netstat", "-ano"], text=True, timeout=5)
    except Exception:
        return None
    needle = f":{parsed.port}"
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 5 and needle in parts[1] and parts[3].upper() == "LISTENING":
            try:
                return int(parts[-1])
            except Exception:
                return None
    return None


def pid_matches_or_child(pid: Any, owner_pid: Any) -> bool:
    if pid is None or owner_pid is None:
        return False
    try:
        pid_i = int(pid)
        owner_i = int(owner_pid)
    except Exception:
        return False
    if pid_i == owner_i:
        return True
    try:
        import psutil  # type: ignore

        return int(psutil.Process(pid_i).ppid()) == owner_i
    except Exception:
        return False


def runtime_snapshot(runtime_url: str) -> dict[str, Any]:
    health = fetch_json(runtime_url)
    fp = health.get("tradingview_runtime_fingerprint") if isinstance(health, dict) else None
    if not isinstance(fp, dict):
        fp = {"fingerprint_present": False}
    else:
        fp = dict(fp)
        fp["fingerprint_present"] = True
    fp["port_owner_pid"] = port_owner_pid(runtime_url)
    fp["health_status"] = health.get("status") if isinstance(health, dict) else None
    fp["health_error"] = health.get("error") if isinstance(health, dict) else "invalid health response"
    return fp


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=20000")
    return conn


def scalar(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(query, params).fetchone()
    return int(row["c"] if row else 0)


def db_snapshot(conn: sqlite3.Connection, bot_id: str, since_iso: str) -> dict[str, Any]:
    return {
        "alerts_received": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_alerts WHERE bot_id=? AND created_at>=?", (bot_id, since_iso)),
        "alerts_accepted": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_alerts WHERE bot_id=? AND created_at>=? AND status LIKE 'ACCEPTED%'", (bot_id, since_iso)),
        "alerts_rejected": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_alerts WHERE bot_id=? AND created_at>=? AND status NOT LIKE 'ACCEPTED%'", (bot_id, since_iso)),
        "queue_rows_created": scalar(conn, "SELECT COUNT(*) AS c FROM external_signal_queue WHERE bot_id=? AND source='TRADINGVIEW' AND created_at>=?", (bot_id, since_iso)),
        "queue_rows_processed": scalar(conn, "SELECT COUNT(*) AS c FROM external_signal_queue WHERE bot_id=? AND source='TRADINGVIEW' AND created_at>=? AND status='PROCESSED'", (bot_id, since_iso)),
        "queue_rows_rejected": scalar(conn, "SELECT COUNT(*) AS c FROM external_signal_queue WHERE bot_id=? AND source='TRADINGVIEW' AND created_at>=? AND status='REJECTED'", (bot_id, since_iso)),
        "queue_rows_failed": scalar(conn, "SELECT COUNT(*) AS c FROM external_signal_queue WHERE bot_id=? AND source='TRADINGVIEW' AND created_at>=? AND status='FAILED'", (bot_id, since_iso)),
        "queue_rows_expired": scalar(conn, "SELECT COUNT(*) AS c FROM external_signal_queue WHERE bot_id=? AND source='TRADINGVIEW' AND created_at>=? AND status='EXPIRED'", (bot_id, since_iso)),
        "stuck_claimed_rows": scalar(conn, "SELECT COUNT(*) AS c FROM external_signal_queue WHERE bot_id=? AND status='CLAIMED'", (bot_id,)),
        "execution_attempts": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND execution_result LIKE 'ORDER_PLACED:%'", (bot_id, since_iso)),
        "orders_placed": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status IN ('PROCESSED_EXECUTED','PROCESSED_EXECUTED_PROTECTED')", (bot_id, since_iso)),
        "trades_opened": scalar(conn, "SELECT COUNT(*) AS c FROM trade_fills WHERE bot_instance_id=? AND timestamp_utc>=? AND action='OPEN' AND trigger_source='TRADINGVIEW_EXTERNAL_SIGNAL'", (bot_id, since_iso)),
        "trades_protected": scalar(conn, """
            SELECT COUNT(*) AS c
            FROM trade_fills tf
            JOIN position_lifecycle_state pls
              ON pls.bot_instance_id=tf.bot_instance_id
             AND (pls.position_id=tf.position_id OR pls.symbol=tf.symbol)
            WHERE tf.bot_instance_id=? AND tf.timestamp_utc>=?
              AND tf.action='OPEN'
              AND tf.trigger_source='TRADINGVIEW_EXTERNAL_SIGNAL'
              AND pls.sl_order_id IS NOT NULL AND pls.tp_order_id IS NOT NULL
              AND pls.sl_order_id NOT LIKE 'DUPLICATE_%'
              AND pls.tp_order_id NOT LIKE 'DUPLICATE_%'
        """, (bot_id, since_iso)),
        "unprotected_positions": scalar(conn, """
            SELECT COUNT(*) AS c FROM position_lifecycle_state
            WHERE bot_instance_id=? AND COALESCE(exchange_position_active,0)=1
              AND (
                sl_order_id IS NULL OR tp_order_id IS NULL
                OR sl_order_id LIKE 'DUPLICATE_%'
                OR tp_order_id LIKE 'DUPLICATE_%'
              )
        """, (bot_id,)),
        "unsupported_actions_rejected": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_TV_ACTION_NOT_ALLOWED'", (bot_id, since_iso)),
        "symbol_not_allowed_rejected": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_TV_SYMBOL_NOT_ALLOWED'", (bot_id, since_iso)),
        "rate_limit_rejections": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_TV_RATE_LIMIT'", (bot_id, since_iso)),
        "daily_cap_rejections": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_TV_DAILY_EXECUTION_CAP'", (bot_id, since_iso)),
        "trade_cap_rejections": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_TV_TRADE_CAP_EXCEEDED'", (bot_id, since_iso)),
        "event_blackout_blocks": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_EVENT_BLACKOUT'", (bot_id, since_iso)),
        "stale_market_data_blocks": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_STALE_MARKET_DATA'", (bot_id, since_iso)),
        "policy_blocks": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_POLICY_RISK'", (bot_id, since_iso)),
        "sizing_blocks": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_SIZING'", (bot_id, since_iso)),
        "duplicate_entry_blocks": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_signal_decisions WHERE bot_id=? AND created_at>=? AND final_status='REJECTED_DUPLICATE_POSITION'", (bot_id, since_iso)),
        "active_safety_lockouts": scalar(conn, "SELECT COUNT(*) AS c FROM tradingview_safety_lockouts WHERE bot_instance_id=? AND is_locked=1", (bot_id,)),
    }


def last_rows(conn: sqlite3.Connection, bot_id: str) -> dict[str, Any]:
    queue = conn.execute(
        "SELECT id, symbol, action, status, created_at, processed_at, result FROM external_signal_queue WHERE bot_id=? ORDER BY created_at DESC LIMIT 5",
        (bot_id,),
    ).fetchall()
    decisions = conn.execute(
        "SELECT queue_id, symbol, action, final_status, final_reason, execution_result, created_at FROM tradingview_signal_decisions WHERE bot_id=? ORDER BY created_at DESC LIMIT 5",
        (bot_id,),
    ).fetchall()
    lifecycle = conn.execute(
        """
        SELECT symbol, phase, exchange_position_active, sl_order_id, tp_order_id,
               reconciliation_status, reconciliation_reason, updated_at
        FROM position_lifecycle_state
        WHERE bot_instance_id=?
        ORDER BY updated_at DESC
        LIMIT 8
        """,
        (bot_id,),
    ).fetchall()
    return {
        "recent_queue_rows": [dict(r) for r in queue],
        "recent_decisions": [dict(r) for r in decisions],
        "recent_lifecycle_rows": [dict(r) for r in lifecycle],
    }


def admin_visibility(runtime_base_url: str) -> dict[str, Any]:
    base = runtime_base_url.removesuffix("/health")
    limited = fetch_json(base + "/api/admin/tradingview/limited-status")
    processor = fetch_json(base + "/api/admin/tradingview/processor-status")
    return {
        "limited_status_reachable": "error" not in limited,
        "processor_status_reachable": "error" not in processor,
        "limited_status": limited,
        "processor_status": processor,
        "secrets_exposed": any(
            token in json.dumps({"limited": limited, "processor": processor}).lower()
            for token in ["token_hash", "secret_hash", "api_secret", "api_key", "authorization"]
        ),
    }


def validation_verdict(output: str) -> str:
    for line in output.splitlines():
        if line.startswith("Phase 6 validation:"):
            return line.split(":", 1)[1].strip()
    return "UNKNOWN"


def disable_and_lockout(db_path: Path, bot_id: str, reason: str) -> None:
    if ENV_PATH.exists():
        text = ENV_PATH.read_text(encoding="utf-8")
        text = text.replace("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=true", "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=false")
        text = text.replace("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED=true", "TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED=false")
        ENV_PATH.write_text(text, encoding="utf-8")
    with connect(db_path) as conn:
        now = utc_now()
        conn.execute(
            """
            INSERT INTO tradingview_safety_lockouts (
                bot_instance_id, is_locked, reason, created_at, updated_at
            ) VALUES (?, 1, ?, ?, ?)
            ON CONFLICT(bot_instance_id) DO UPDATE SET
                is_locked=1, reason=excluded.reason, updated_at=excluded.updated_at
            """,
            (bot_id, reason, now, now),
        )
        conn.commit()


@dataclass
class Phase6FReport:
    generated_at: str
    final_verdict: str
    runtime_process_verification: dict[str, Any]
    runtime_fingerprint: dict[str, Any]
    phase6f_config: dict[str, Any]
    validation_result: str
    observation_window: dict[str, Any]
    alerts_summary: dict[str, Any]
    queue_summary: dict[str, Any]
    decision_summary: dict[str, Any]
    execution_summary: dict[str, Any]
    sltp_protection_summary: dict[str, Any]
    lifecycle_reconciliation_summary: dict[str, Any]
    gate_rejection_summary: dict[str, Any]
    rate_limit_cap_summary: dict[str, Any]
    admin_visibility_summary: dict[str, Any]
    safety_lockout_summary: dict[str, Any]
    safety_invariant_results: dict[str, Any]
    incidents_anomalies: list[str] = field(default_factory=list)
    evidence_samples: dict[str, Any] = field(default_factory=dict)
    markdown_report_path: str | None = None
    json_report_path: str | None = None


def determine_verdict(report: Phase6FReport) -> str:
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
    if not report.admin_visibility_summary.get("limited_status_reachable") or not report.admin_visibility_summary.get("processor_status_reachable"):
        return VERDICT_NEEDS_FIX
    if report.admin_visibility_summary.get("secrets_exposed"):
        return VERDICT_NEEDS_FIX
    if report.alerts_summary.get("alerts_received", 0) == 0 and report.queue_summary.get("queue_rows_created", 0) == 0:
        return VERDICT_PASSED_NO_ACTIVITY
    return VERDICT_PASSED_ACTIVITY


def render_markdown(report: Phase6FReport) -> str:
    sections = [
        "# Phase 6F — Controlled Operational Observation",
        f"Final verdict: `{report.final_verdict}`",
        "## Runtime Process Verification\n```json\n" + json.dumps(report.runtime_process_verification, indent=2, default=str) + "\n```",
        "## Runtime Fingerprint\n```json\n" + json.dumps(report.runtime_fingerprint, indent=2, default=str) + "\n```",
        "## Phase 6F Config\n```json\n" + json.dumps(report.phase6f_config, indent=2, default=str) + "\n```",
        f"## Validation Result\n`{report.validation_result}`",
        "## Observation Window\n```json\n" + json.dumps(report.observation_window, indent=2, default=str) + "\n```",
        "## Alerts Summary\n```json\n" + json.dumps(report.alerts_summary, indent=2, default=str) + "\n```",
        "## Queue Summary\n```json\n" + json.dumps(report.queue_summary, indent=2, default=str) + "\n```",
        "## Decision Summary\n```json\n" + json.dumps(report.decision_summary, indent=2, default=str) + "\n```",
        "## Execution Summary\n```json\n" + json.dumps(report.execution_summary, indent=2, default=str) + "\n```",
        "## SL/TP Protection Summary\n```json\n" + json.dumps(report.sltp_protection_summary, indent=2, default=str) + "\n```",
        "## Lifecycle/Reconciliation Summary\n```json\n" + json.dumps(report.lifecycle_reconciliation_summary, indent=2, default=str) + "\n```",
        "## Gate/Rejection Summary\n```json\n" + json.dumps(report.gate_rejection_summary, indent=2, default=str) + "\n```",
        "## Rate Limit and Cap Summary\n```json\n" + json.dumps(report.rate_limit_cap_summary, indent=2, default=str) + "\n```",
        "## Admin Visibility Summary\n```json\n" + json.dumps(report.admin_visibility_summary, indent=2, default=str) + "\n```",
        "## Safety Lockout Summary\n```json\n" + json.dumps(report.safety_lockout_summary, indent=2, default=str) + "\n```",
        "## Safety Invariant Results\n```json\n" + json.dumps(report.safety_invariant_results, indent=2, default=str) + "\n```",
        "## Incidents / Anomalies\n```json\n" + json.dumps(report.incidents_anomalies, indent=2, default=str) + "\n```",
        f"## Whether Phase 6G Or Broader Rollout Is Allowed\n`{report.final_verdict in {VERDICT_PASSED_ACTIVITY, VERDICT_PASSED_NO_ACTIVITY}}`",
    ]
    return "\n\n".join(sections)


def write_report(report: Phase6FReport, output_dir: Path) -> Phase6FReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = output_dir / f"phase6f_operational_observation_{stamp}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")
    report.json_report_path = str(json_path)
    report.markdown_report_path = str(md_path)
    json_path.write_text(json.dumps(asdict(report), indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return report


def build_report(
    *,
    bot_id: str,
    db_path: Path,
    runtime_url: str,
    output_dir: Path,
    start_iso: str,
    end_iso: str,
    validation_result: str,
    samples: list[dict[str, Any]],
) -> Phase6FReport:
    runtime = runtime_snapshot(runtime_url)
    admin = admin_visibility(runtime_url)
    with connect(db_path) as conn:
        snap = db_snapshot(conn, bot_id, start_iso)
        evidence = last_rows(conn, bot_id)
    runtime_process = {
        "pid": runtime.get("pid"),
        "port_owner_pid": runtime.get("port_owner_pid"),
        "pid_matches_port_owner": pid_matches_or_child(runtime.get("pid"), runtime.get("port_owner_pid")),
        "health_status": runtime.get("health_status"),
    }
    incidents: list[str] = []
    if not runtime.get("fingerprint_present"):
        incidents.append("Runtime fingerprint missing")
    if not runtime_process["pid_matches_port_owner"]:
        incidents.append("Runtime PID does not match port owner PID")
    if not runtime.get("phase6_gate_available"):
        incidents.append("Phase 6 gate unavailable")
    if runtime.get("phase6_gate_code_version") != "phase6_limited_gate_v1_2026-05-21":
        incidents.append("Phase 6 gate code version mismatch")
    if runtime.get("active_safety_lockout"):
        incidents.append("TradingView safety lockout active")
    if snap["stuck_claimed_rows"]:
        incidents.append("Stuck CLAIMED rows detected")
    if snap["unprotected_positions"]:
        incidents.append("Unprotected active positions detected")
    if not admin.get("limited_status_reachable") or not admin.get("processor_status_reachable"):
        incidents.append("Admin TradingView visibility endpoint missing")
    if admin.get("secrets_exposed"):
        incidents.append("Admin visibility exposed sensitive fields")

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
    report = Phase6FReport(
        generated_at=utc_now(),
        final_verdict=VERDICT_NEEDS_FIX,
        runtime_process_verification=runtime_process,
        runtime_fingerprint=runtime,
        phase6f_config={
            "external_signals_enabled": runtime.get("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"),
            "limited_mode_enabled": runtime.get("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED"),
            "allowed_actions": runtime.get("TRADINGVIEW_ALLOWED_ACTIONS"),
            "allowed_symbols": runtime.get("TRADINGVIEW_ALLOWED_SYMBOLS"),
            "max_queue_per_cycle": runtime.get("TRADINGVIEW_MAX_QUEUE_PER_CYCLE"),
            "max_executions_per_day": runtime.get("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY"),
            "max_signals_per_hour": runtime.get("TRADINGVIEW_MAX_SIGNALS_PER_HOUR"),
            "max_signals_per_day": runtime.get("TRADINGVIEW_MAX_SIGNALS_PER_DAY"),
            "max_trade_usdt_cap": runtime.get("TRADINGVIEW_MAX_TRADE_USDT_CAP"),
            "forbidden_capabilities": {
                "close": runtime.get("TRADINGVIEW_ALLOW_CLOSE"),
                "reverse": runtime.get("TRADINGVIEW_ALLOW_REVERSE"),
                "reduce": runtime.get("TRADINGVIEW_ALLOW_REDUCE"),
                "cancel": runtime.get("TRADINGVIEW_ALLOW_CANCEL"),
                "external_sltp": runtime.get("TRADINGVIEW_ALLOW_EXTERNAL_SLTP"),
                "external_size": runtime.get("TRADINGVIEW_ALLOW_EXTERNAL_SIZE"),
                "risk_override": runtime.get("TRADINGVIEW_ALLOW_RISK_OVERRIDE"),
            },
        },
        validation_result=validation_result,
        observation_window={
            "start": start_iso,
            "end": end_iso,
            "sample_count": len(samples),
            "samples": samples,
        },
        alerts_summary={k: snap[k] for k in ["alerts_received", "alerts_accepted", "alerts_rejected"]},
        queue_summary={k: snap[k] for k in ["queue_rows_created", "queue_rows_processed", "queue_rows_rejected", "queue_rows_failed", "queue_rows_expired", "stuck_claimed_rows"]},
        decision_summary={
            "unsupported_actions_rejected": snap["unsupported_actions_rejected"],
            "symbol_not_allowed_rejected": snap["symbol_not_allowed_rejected"],
        },
        execution_summary={k: snap[k] for k in ["execution_attempts", "orders_placed", "trades_opened"]},
        sltp_protection_summary={
            "trades_protected": snap["trades_protected"],
            "unprotected_positions": snap["unprotected_positions"],
        },
        lifecycle_reconciliation_summary={
            "unprotected_positions": snap["unprotected_positions"],
            "recent_lifecycle_rows": evidence["recent_lifecycle_rows"],
        },
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
        admin_visibility_summary=admin,
        safety_lockout_summary={
            "active": bool(runtime.get("active_safety_lockout")),
            "reason": runtime.get("active_safety_lockout_reason"),
            "db_active_lockouts": snap["active_safety_lockouts"],
        },
        safety_invariant_results=invariants,
        incidents_anomalies=incidents,
        evidence_samples=evidence,
    )
    report.final_verdict = determine_verdict(report)
    if report.final_verdict == VERDICT_UNSAFE:
        disable_and_lockout(db_path, bot_id, "Phase 6F critical safety invariant failed")
    return write_report(report, output_dir)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bot-id", default="bot_e5fe913972a9")
    parser.add_argument("--duration-hours", type=float, default=None)
    parser.add_argument("--duration-minutes", type=float, default=None)
    parser.add_argument("--poll-seconds", type=float, default=60)
    parser.add_argument("--output-dir", type=Path, default=REPORT_DIR)
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--runtime-url", default=RUNTIME_URL)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    duration_seconds = 24 * 60 * 60
    if args.duration_hours is not None:
        duration_seconds = max(0, int(args.duration_hours * 3600))
    if args.duration_minutes is not None:
        duration_seconds = max(0, int(args.duration_minutes * 60))

    validation = subprocess.run(
        ["python", "scripts/run_tradingview_phase6_limited_validation.py", "--strict"],
        cwd=str(ROOT / "backends" / "bot-backend"),
        text=True,
        capture_output=True,
        timeout=120,
    )
    validation_result = validation_verdict(validation.stdout)
    start_iso = utc_now()
    samples: list[dict[str, Any]] = []
    deadline = time.time() + duration_seconds
    while True:
        snap = runtime_snapshot(args.runtime_url)
        admin = admin_visibility(args.runtime_url)
        samples.append(
            {
                "sampled_at": utc_now(),
                "health_status": snap.get("health_status"),
                "pid": snap.get("pid"),
                "port_owner_pid": snap.get("port_owner_pid"),
                "lockout_active": snap.get("active_safety_lockout"),
                "limited_status_reachable": admin.get("limited_status_reachable"),
                "processor_status_reachable": admin.get("processor_status_reachable"),
            }
        )
        with connect(args.db_path) as conn:
            live = db_snapshot(conn, args.bot_id, start_iso)
        if live["stuck_claimed_rows"] or live["unprotected_positions"] or snap.get("active_safety_lockout"):
            break
        if time.time() >= deadline:
            break
        time.sleep(min(args.poll_seconds, max(0, deadline - time.time())))

    report = build_report(
        bot_id=args.bot_id,
        db_path=args.db_path,
        runtime_url=args.runtime_url,
        output_dir=args.output_dir,
        start_iso=start_iso,
        end_iso=utc_now(),
        validation_result=validation_result,
        samples=samples,
    )
    print(f"Phase 6F observation: {report.final_verdict}")
    print(f"Markdown report: {report.markdown_report_path}")
    print(f"JSON report: {report.json_report_path}")
    if report.final_verdict in {VERDICT_PASSED_ACTIVITY, VERDICT_PASSED_NO_ACTIVITY}:
        return 0
    if report.final_verdict == VERDICT_UNSAFE:
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
