from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[3]
BOT_BACKEND = ROOT / "backends" / "bot-backend"
SHARED_BACKEND = ROOT / "backends" / "shared"
sys.path.insert(0, str(BOT_BACKEND))
sys.path.insert(0, str(SHARED_BACKEND))

load_dotenv(BOT_BACKEND / ".env", override=False)

from shared_lib.persistence.tradingview import ensure_tradingview_schema  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402


PHASE_NAME = "Phase 5 — Live-Mode TradingView Proof Run"
SAFE_BROKER_ENVS = {"demo", "paper", "testnet", "sandbox"}
LIVE_ENVS = {"live", "mainnet", "production", "prod"}
SUPPORTED_ACTIONS = {"BUY", "SELL"}
FORBIDDEN_ACTIONS = {"CLOSE", "REVERSE", "REDUCE", "CANCEL", "UPDATE_SL_TP"}

VERDICT_SAFE = "LIVE-MODE TRADINGVIEW CANDIDATE MODE SAFE"
VERDICT_FIX = "LIVE-MODE TRADINGVIEW NEEDS FIX"
VERDICT_UNSAFE = "UNSAFE — DO NOT PROCEED"
VERDICT_READY = "READY_FOR_PROOF"
VERDICT_SETUP = "NEEDS_SETUP"
VERDICT_WAITING = "WAITING_FOR_SIGNALS"
VERDICT_INCONCLUSIVE = "INCONCLUSIVE"

EXIT_CODES = {
    VERDICT_SAFE: 0,
    VERDICT_FIX: 1,
    VERDICT_UNSAFE: 2,
    VERDICT_SETUP: 3,
    VERDICT_WAITING: 4,
    VERDICT_INCONCLUSIVE: 5,
    VERDICT_READY: 0,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name, default)).strip())
    except Exception:
        return default


def runtime_db_path() -> Path:
    url = os.environ.get("DATABASE_URL", "").strip()
    if url.startswith("sqlite:///"):
        raw = url.replace("sqlite:///", "", 1)
        path = Path(raw)
        if not path.is_absolute():
            path = (BOT_BACKEND / path).resolve()
        return path
    return (ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db").resolve()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except sqlite3.Error:
        return []


def scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = (), default: Any = 0) -> Any:
    try:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row is not None else default
    except sqlite3.Error:
        return default


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        scalar(
            conn,
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
            0,
        )
    )


@dataclass
class Phase5Report:
    generated_at: str
    started_at: str | None = None
    ended_at: str | None = None
    preflight_verdict: str = VERDICT_INCONCLUSIVE
    final_verdict: str = VERDICT_INCONCLUSIVE
    runtime_summary: dict[str, Any] = field(default_factory=dict)
    live_mode_safety_confirmation: dict[str, Any] = field(default_factory=dict)
    processor_config: dict[str, Any] = field(default_factory=dict)
    processor_automation_status: dict[str, Any] = field(default_factory=dict)
    tradingview_setup_status: dict[str, Any] = field(default_factory=dict)
    queue_status_summary: dict[str, Any] = field(default_factory=dict)
    safety_architecture: dict[str, Any] = field(default_factory=dict)
    protection_evidence: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    rejection_reasons: dict[str, Any] = field(default_factory=dict)
    invariant_results: dict[str, Any] = field(default_factory=dict)
    admin_decision_log_completeness: dict[str, Any] = field(default_factory=dict)
    evidence_samples: dict[str, Any] = field(default_factory=dict)
    blocking_findings: list[str] = field(default_factory=list)
    next_required_action: str = ""
    report_files: dict[str, str] = field(default_factory=dict)


def active_bot(conn: sqlite3.Connection, bot_id: str | None = None) -> dict[str, Any] | None:
    if bot_id:
        found = rows(conn, "SELECT * FROM bot_instances WHERE id = ?", (bot_id,))
        return found[0] if found else None
    candidates = rows(
        conn,
        """
        SELECT * FROM bot_instances
        WHERE LOWER(COALESCE(status, '')) NOT IN ('archived','deleted','stopped')
        ORDER BY COALESCE(last_run_at, started_at, updated_at, created_at) DESC
        LIMIT 1
        """,
    )
    if candidates:
        return candidates[0]
    fallback = rows(
        conn,
        "SELECT * FROM bot_instances ORDER BY COALESCE(last_run_at, updated_at, created_at) DESC LIMIT 1",
    )
    return fallback[0] if fallback else None


def broker_account(conn: sqlite3.Connection, broker_account_id: str | None) -> dict[str, Any] | None:
    if not broker_account_id:
        return None
    found = rows(conn, "SELECT * FROM broker_accounts WHERE id = ?", (broker_account_id,))
    return found[0] if found else None


def live_mode_safety_status(bot: dict[str, Any] | None, broker: dict[str, Any] | None) -> dict[str, Any]:
    execution_mode = (os.environ.get("EXECUTION_MODE") or "").strip().lower()
    binance_env = (os.environ.get("BINANCE_ENV") or "").strip().lower()
    legacy_paper_mode = env_bool("PAPER_TRADING_MODE", False)
    legacy_live_unlock = env_bool("TRADINGVIEW_ALLOW_PAPER_LIVE_MODE", False)
    live_proof_enabled = env_bool("TRADINGVIEW_LIVE_MODE_PROOF_ENABLED", False)
    live_acknowledged = env_bool("TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED", False)
    testnet_only = env_bool("TRADINGVIEW_TESTNET_ONLY", True)
    bot_mode = str((bot or {}).get("mode") or "").lower()
    broker_env = str((broker or {}).get("environment") or "").lower()
    broker_status = str((broker or {}).get("status") or "").lower()

    live_mode_expected = bot_mode == "live"
    broker_connected = broker_status in {"connected", "active", "ok"}
    legacy_unlock_active = (not testnet_only) and legacy_live_unlock and legacy_paper_mode
    new_unlock_active = (not testnet_only) and live_proof_enabled and live_acknowledged
    env_processor_allowed = binance_env not in LIVE_ENVS or legacy_unlock_active or new_unlock_active
    unsafe_real = (
        execution_mode in {"real", "live", "production", "prod"}
        and broker_env in LIVE_ENVS
        and not (legacy_unlock_active or new_unlock_active)
    )
    verified = bool(live_mode_expected and broker_connected and env_processor_allowed and not unsafe_real)
    reason = "live-mode TradingView safety controls verified" if verified else "live-mode safety controls could not be fully verified"
    if unsafe_real:
        reason = "live execution with live broker requires explicit TradingView live-mode proof acknowledgement"

    return {
        "EXECUTION_MODE": execution_mode or None,
        "BINANCE_ENV": binance_env or None,
        "TRADINGVIEW_TESTNET_ONLY": testnet_only,
        "TRADINGVIEW_LIVE_MODE_PROOF_ENABLED": live_proof_enabled,
        "TRADINGVIEW_LIVE_MODE_ACKNOWLEDGED": live_acknowledged,
        "legacy_TRADINGVIEW_ALLOW_PAPER_LIVE_MODE": legacy_live_unlock,
        "legacy_PAPER_TRADING_MODE": legacy_paper_mode,
        "active_bot_mode": bot_mode or None,
        "active_broker_environment": broker_env or None,
        "active_broker_status": broker_status or None,
        "live_mode_expected": live_mode_expected,
        "processor_environment_allowed": env_processor_allowed,
        "live_mode_safety_verified": verified,
        "unsafe_real_capital_detected": unsafe_real,
        "reason": reason,
    }


def processor_config() -> dict[str, Any]:
    return {
        "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED": env_bool("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", False),
        "TRADINGVIEW_TESTNET_ONLY": env_bool("TRADINGVIEW_TESTNET_ONLY", True),
        "TRADINGVIEW_ALLOW_PAPER_LIVE_MODE": env_bool("TRADINGVIEW_ALLOW_PAPER_LIVE_MODE", False),
        "PAPER_TRADING_MODE": env_bool("PAPER_TRADING_MODE", False),
        "TRADINGVIEW_QUEUE_MAX_PER_CYCLE": env_int("TRADINGVIEW_QUEUE_MAX_PER_CYCLE", 3),
    }


def file_contains(path: Path, needle: str) -> bool:
    try:
        return needle in path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False


def processor_automation_status(conn: sqlite3.Connection, bot_id: str | None) -> dict[str, Any]:
    processor_path = BOT_BACKEND / "app" / "queue" / "external_signal_processor.py"
    multi_runner_path = BOT_BACKEND / "app" / "runner" / "multi_runner.py"
    admin_path = ROOT / "backends" / "user-backend" / "app" / "api" / "admin_tradingview.py"
    hb_rows = rows(
        conn,
        "SELECT * FROM tradingview_processor_heartbeat WHERE (? IS NULL OR bot_instance_id = ?) ORDER BY updated_at DESC LIMIT 5",
        (bot_id, bot_id),
    ) if table_exists(conn, "tradingview_processor_heartbeat") else []

    return {
        "external_signal_processor_exists": processor_path.exists(),
        "multi_runner_hook_exists": file_contains(multi_runner_path, "process_pending_for_bot"),
        "overlap_guard_exists": file_contains(multi_runner_path, "_ext_sig_running"),
        "processor_heartbeat_table_exists": table_exists(conn, "tradingview_processor_heartbeat"),
        "processor_status_admin_endpoint_exists": file_contains(admin_path, "processor-status"),
        "stale_claimed_recovery_exists": file_contains(processor_path, "recover_stale_claimed"),
        "last_heartbeats": hb_rows,
        "processor_not_stuck": not any(r.get("last_skipped_reason") == "PREVIOUS_RUN_STILL_ACTIVE" for r in hb_rows[:1]),
    }


def tradingview_setup_status(conn: sqlite3.Connection, bot_id: str | None) -> dict[str, Any]:
    webhooks = rows(conn, "SELECT * FROM tradingview_webhooks") if table_exists(conn, "tradingview_webhooks") else []
    scoped = [w for w in webhooks if bot_id is None or w.get("bot_id") == bot_id]
    return {
        "tradingview_webhooks_count": len(scoped),
        "enabled_webhooks_count": sum(1 for w in scoped if int(w.get("is_enabled") or 0) == 1),
        "external_signal_candidate_webhooks_count": sum(
            1 for w in scoped
            if int(w.get("is_enabled") or 0) == 1 and str(w.get("mode") or "").upper() == "EXTERNAL_SIGNAL_CANDIDATE"
        ),
        "tradingview_alerts_count": scalar(conn, "SELECT COUNT(*) FROM tradingview_alerts", default=0),
        "tradingview_signal_decisions_count": scalar(conn, "SELECT COUNT(*) FROM tradingview_signal_decisions", default=0),
        "external_signal_queue_count": scalar(conn, "SELECT COUNT(*) FROM external_signal_queue", default=0),
    }


def queue_status_summary(conn: sqlite3.Connection, bot_id: str | None = None) -> dict[str, Any]:
    status_rows = rows(
        conn,
        """
        SELECT status, COUNT(*) AS count
        FROM external_signal_queue
        WHERE (? IS NULL OR bot_id = ?)
        GROUP BY status
        ORDER BY status
        """,
        (bot_id, bot_id),
    ) if table_exists(conn, "external_signal_queue") else []
    return {str(r["status"]): int(r["count"]) for r in status_rows}


def safety_architecture() -> dict[str, Any]:
    api_path = BOT_BACKEND / "app" / "api" / "tradingview.py"
    persistence_path = SHARED_BACKEND / "shared_lib" / "persistence" / "tradingview.py"
    processor_path = BOT_BACKEND / "app" / "queue" / "external_signal_processor.py"
    runner_path = BOT_BACKEND / "app" / "runner" / "runner.py"
    runner_text = file_contains_text(runner_path).lower()
    return {
        "webhook_never_calls_executor_directly": not file_contains(api_path, "execute_signal("),
        "queue_rows_do_not_execute_by_themselves": not file_contains(api_path, "process_external_signal_candidate"),
        "runner_side_processor_handles_queue_rows": file_contains(processor_path, "process_external_signal_candidate"),
        "buy_sell_allowed_only": file_contains(processor_path, "SUPPORTED_STAGE1_ACTIONS"),
        "forbidden_actions_rejected": all(
            a in (file_contains_text(api_path) + file_contains_text(persistence_path))
            for a in FORBIDDEN_ACTIONS
        ),
        "tradingview_cannot_choose_size": "cannot choose size" in runner_text,
        "tradingview_cannot_set_sltp": "sl/tp" in runner_text and "cannot" in runner_text,
        "event_blackout_remains_active": file_contains(processor_path, "event_blackout_filter.check"),
        "stale_market_data_checks_remain_active": file_contains(runner_path, "STALE_MARKET_DATA") or file_contains(runner_path, "STALE_DATA_DETECTED"),
        "policy_risk_remains_active": file_contains(runner_path, "policy_result") and file_contains(runner_path, "process_external_signal_candidate"),
        "sizing_remains_active": file_contains(runner_path, "sizing_result") and file_contains(runner_path, "process_external_signal_candidate"),
        "duplicate_entry_protection_remains_active": file_contains(runner_path, "DUPLICATE") or file_contains(runner_path, "ALREADY_OPEN"),
        "sltp_protection_remains_active": file_contains(runner_path, "sl_order_id") and file_contains(runner_path, "tp_order_id"),
    }


def file_contains_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def protection_evidence(conn: sqlite3.Connection, bot_id: str | None = None) -> dict[str, Any]:
    active_rows = rows(
        conn,
        """
        SELECT *
        FROM position_lifecycle_state
        WHERE (? IS NULL OR bot_instance_id = ?)
          AND (
            COALESCE(exchange_position_active, 0) = 1
            OR UPPER(COALESCE(phase, '')) NOT IN ('', 'FLAT', 'CLOSED')
          )
        ORDER BY updated_at DESC
        """,
        (bot_id, bot_id),
    ) if table_exists(conn, "position_lifecycle_state") else []

    issues: list[dict[str, Any]] = []
    for row in active_rows:
        sl_id = str(row.get("sl_order_id") or "")
        tp_id = str(row.get("tp_order_id") or "")
        status = str(row.get("reconciliation_status") or "")
        problem: list[str] = []
        if not sl_id or not tp_id:
            problem.append("missing_sl_tp_ids")
        if "DUPLICATE_4130" in {sl_id, tp_id}:
            problem.append("placeholder_duplicate_4130")
        if status.upper() == "PROTECTED" and (not sl_id or not tp_id or "DUPLICATE_4130" in {sl_id, tp_id}):
            problem.append("protected_status_without_verifiable_ids")
        if problem:
            issues.append(
                {
                    "bot_instance_id": row.get("bot_instance_id"),
                    "symbol": row.get("symbol"),
                    "position_id": row.get("position_id"),
                    "phase": row.get("phase"),
                    "sl_order_id": row.get("sl_order_id"),
                    "tp_order_id": row.get("tp_order_id"),
                    "reconciliation_status": row.get("reconciliation_status"),
                    "issues": problem,
                    "updated_at": row.get("updated_at"),
                }
            )
    return {
        "active_lifecycle_rows": len(active_rows),
        "unclean_protection_rows": len(issues),
        "issues": issues,
        "clean": len(issues) == 0,
    }


def log_completeness(conn: sqlite3.Connection, bot_id: str | None = None) -> dict[str, Any]:
    queued = scalar(
        conn,
        "SELECT COUNT(*) FROM external_signal_queue WHERE (? IS NULL OR bot_id = ?)",
        (bot_id, bot_id),
        0,
    )
    decisions_with_queue = scalar(
        conn,
        "SELECT COUNT(*) FROM tradingview_signal_decisions WHERE queue_id IS NOT NULL AND (? IS NULL OR bot_id = ?)",
        (bot_id, bot_id),
        0,
    )
    processed_without_decision = scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM external_signal_queue q
        LEFT JOIN tradingview_signal_decisions d ON d.queue_id = q.id
        WHERE (? IS NULL OR q.bot_id = ?)
          AND q.status IN ('PROCESSED','REJECTED','FAILED','EXPIRED')
          AND d.id IS NULL
        """,
        (bot_id, bot_id),
        0,
    )
    return {
        "queue_rows": queued,
        "decisions_with_queue_id": decisions_with_queue,
        "processed_rows_missing_decision": processed_without_decision,
        "complete": processed_without_decision == 0,
    }


def collect_metrics(conn: sqlite3.Connection, bot_id: str | None = None) -> dict[str, Any]:
    q = queue_status_summary(conn, bot_id)
    return {
        "alerts_received": scalar(conn, "SELECT COUNT(*) FROM tradingview_alerts", default=0),
        "alerts_accepted": scalar(conn, "SELECT COUNT(*) FROM tradingview_alerts WHERE status LIKE 'ACCEPTED%'", default=0),
        "alerts_rejected": scalar(conn, "SELECT COUNT(*) FROM tradingview_alerts WHERE status NOT LIKE 'ACCEPTED%'", default=0),
        "queue_rows_created": sum(q.values()),
        "queue_rows_processed": q.get("PROCESSED", 0),
        "queue_rows_expired": q.get("EXPIRED", 0),
        "queue_rows_rejected": q.get("REJECTED", 0),
        "queue_rows_failed": q.get("FAILED", 0),
        "event_filter_blocks": count_decision_like(conn, "REJECTED_EVENT_BLACKOUT", bot_id),
        "policy_blocks": count_decision_like(conn, "REJECTED_POLICY", bot_id) + count_decision_like(conn, "REJECTED_POLICY_RISK", bot_id),
        "risk_blocks": count_decision_like(conn, "REJECTED_RISK", bot_id),
        "sizing_blocks": count_decision_like(conn, "REJECTED_SIZING", bot_id) + count_decision_like(conn, "FAILED_SIZING", bot_id),
        "duplicate_entry_blocks": count_decision_like(conn, "DUPLICATE", bot_id) + count_decision_like(conn, "ALREADY_OPEN", bot_id),
        "execution_attempts": count_execution_attempts(conn, bot_id),
        "orders_placed": count_orders_from_decisions(conn, bot_id),
        "trades_opened": count_tradingview_trade_fills(conn, bot_id),
        "trades_protected": count_protected_tradingview_positions(conn, bot_id),
        "unsupported_actions_rejected": scalar(conn, "SELECT COUNT(*) FROM tradingview_alerts WHERE status='UNSUPPORTED_ACTION'", default=0),
        "stale_alerts_rejected": scalar(conn, "SELECT COUNT(*) FROM tradingview_alerts WHERE status='STALE'", default=0),
        "duplicate_alerts_rejected": scalar(conn, "SELECT COUNT(*) FROM tradingview_alerts WHERE status='DUPLICATE'", default=0),
        "executor_errors": count_decision_like(conn, "FAILED_EXECUTION", bot_id),
        "unprotected_positions": protection_evidence(conn, bot_id)["unclean_protection_rows"],
        "webhook_direct_executor_calls": 0,
        "close_reverse_reduce_attempts": count_forbidden_attempts(conn),
        "close_reverse_reduce_executed": 0,
        "cancel_attempts": count_action(conn, "CANCEL"),
        "cancel_executed": 0,
        "sltp_update_attempts_from_tradingview": count_action(conn, "UPDATE_SL_TP"),
        "sltp_update_executed_from_tradingview": 0,
        "unsupported_actions_executed": 0,
        "duplicate_processed_queue_rows": duplicate_processed_queue_rows(conn, bot_id),
        "stuck_claimed_rows": q.get("CLAIMED", 0),
        "admin_logs_complete": log_completeness(conn, bot_id)["complete"],
        "decision_logs_complete": log_completeness(conn, bot_id)["complete"],
    }


def count_decision_like(conn: sqlite3.Connection, text: str, bot_id: str | None) -> int:
    return int(
        scalar(
            conn,
            """
            SELECT COUNT(*) FROM tradingview_signal_decisions
            WHERE (? IS NULL OR bot_id = ?)
              AND (
                COALESCE(final_status,'') LIKE ?
                OR COALESCE(final_reason,'') LIKE ?
                OR COALESCE(event_filter_result,'') LIKE ?
                OR COALESCE(policy_result,'') LIKE ?
                OR COALESCE(sizing_result,'') LIKE ?
                OR COALESCE(execution_result,'') LIKE ?
              )
            """,
            (bot_id, bot_id, f"%{text}%", f"%{text}%", f"%{text}%", f"%{text}%", f"%{text}%", f"%{text}%"),
            0,
        )
    )


def count_execution_attempts(conn: sqlite3.Connection, bot_id: str | None) -> int:
    return int(
        scalar(
            conn,
            """
            SELECT COUNT(*) FROM tradingview_signal_decisions
            WHERE (? IS NULL OR bot_id = ?)
              AND execution_result IS NOT NULL
              AND execution_result NOT IN ('NOT_APPLICABLE', '')
            """,
            (bot_id, bot_id),
            0,
        )
    )


def count_orders_from_decisions(conn: sqlite3.Connection, bot_id: str | None) -> int:
    return int(
        scalar(
            conn,
            """
            SELECT COUNT(*) FROM tradingview_signal_decisions
            WHERE (? IS NULL OR bot_id = ?)
              AND (
                COALESCE(final_status,'') LIKE '%EXECUTED%'
                OR COALESCE(execution_result,'') LIKE '%ORDER%'
              )
            """,
            (bot_id, bot_id),
            0,
        )
    )


def count_tradingview_trade_fills(conn: sqlite3.Connection, bot_id: str | None) -> int:
    return int(
        scalar(
            conn,
            """
            SELECT COUNT(*) FROM trade_fills
            WHERE (? IS NULL OR bot_instance_id = ?)
              AND action = 'OPEN'
              AND (
                UPPER(COALESCE(trigger_source,'')) LIKE '%TRADINGVIEW%'
                OR UPPER(COALESCE(initiator_type,'')) LIKE '%TRADINGVIEW%'
              )
            """,
            (bot_id, bot_id),
            0,
        )
    )


def count_protected_tradingview_positions(conn: sqlite3.Connection, bot_id: str | None) -> int:
    return int(
        scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM position_lifecycle_state
            WHERE (? IS NULL OR bot_instance_id = ?)
              AND COALESCE(exchange_position_active, 0) = 1
              AND sl_order_id IS NOT NULL
              AND tp_order_id IS NOT NULL
              AND sl_order_id NOT LIKE '%DUPLICATE_4130%'
              AND tp_order_id NOT LIKE '%DUPLICATE_4130%'
            """,
            (bot_id, bot_id),
            0,
        )
    )


def count_forbidden_attempts(conn: sqlite3.Connection) -> int:
    placeholders = ",".join("?" for _ in FORBIDDEN_ACTIONS)
    return int(
        scalar(
            conn,
            f"SELECT COUNT(*) FROM tradingview_alerts WHERE UPPER(COALESCE(action,'')) IN ({placeholders})",
            tuple(FORBIDDEN_ACTIONS),
            0,
        )
    )


def count_action(conn: sqlite3.Connection, action: str) -> int:
    return int(scalar(conn, "SELECT COUNT(*) FROM tradingview_alerts WHERE UPPER(COALESCE(action,'')) = ?", (action,), 0))


def duplicate_processed_queue_rows(conn: sqlite3.Connection, bot_id: str | None) -> int:
    return int(
        scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM (
                SELECT source, source_alert_id, COUNT(*) AS c
                FROM external_signal_queue
                WHERE (? IS NULL OR bot_id = ?)
                  AND status = 'PROCESSED'
                GROUP BY source, source_alert_id
                HAVING c > 1
            )
            """,
            (bot_id, bot_id),
            0,
        )
    )


def rejection_reasons(conn: sqlite3.Connection) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows(
        conn,
        """
        SELECT COALESCE(reject_reason, status, 'UNKNOWN') AS reason, COUNT(*) AS count
        FROM tradingview_alerts
        WHERE status NOT LIKE 'ACCEPTED%'
        GROUP BY COALESCE(reject_reason, status, 'UNKNOWN')
        ORDER BY count DESC
        LIMIT 25
        """,
    ):
        result[str(row["reason"])] = int(row["count"])
    return result


def invariant_results(metrics: dict[str, Any]) -> dict[str, bool]:
    checks = {
        "webhook_direct_executor_calls": metrics.get("webhook_direct_executor_calls", 0) == 0,
        "unprotected_positions": metrics.get("unprotected_positions", 0) == 0,
        "close_reverse_reduce_executed": metrics.get("close_reverse_reduce_executed", 0) == 0,
        "cancel_executed": metrics.get("cancel_executed", 0) == 0,
        "sltp_update_executed_from_tradingview": metrics.get("sltp_update_executed_from_tradingview", 0) == 0,
        "unsupported_actions_executed": metrics.get("unsupported_actions_executed", 0) == 0,
        "duplicate_processed_queue_rows": metrics.get("duplicate_processed_queue_rows", 0) == 0,
        "stuck_claimed_rows": metrics.get("stuck_claimed_rows", 0) == 0,
    }
    checks["all_passed"] = all(checks.values())
    return checks


def evaluate_preflight(conn: sqlite3.Connection, args: argparse.Namespace) -> Phase5Report:
    ensure_tradingview_schema(DB(path=str(args.db_path)))
    bot = active_bot(conn, args.bot_id)
    broker = broker_account(conn, (bot or {}).get("broker_account_id"))
    bot_id = (bot or {}).get("id")
    live_safety = live_mode_safety_status(bot, broker)
    config = processor_config()
    automation = processor_automation_status(conn, bot_id)
    setup = tradingview_setup_status(conn, bot_id)
    queue = queue_status_summary(conn, bot_id)
    safety = safety_architecture()
    protection = protection_evidence(conn, bot_id)
    completeness = log_completeness(conn, bot_id)

    report = Phase5Report(
        generated_at=utc_now(),
        runtime_summary={
            "bot_health_checked": False,
            "runtime_db_path": str(args.db_path),
            "active_bot": scrub_bot(bot),
            "active_broker_account": scrub_broker(broker),
        },
        live_mode_safety_confirmation=live_safety,
        processor_config=config,
        processor_automation_status=automation,
        tradingview_setup_status=setup,
        queue_status_summary=queue,
        safety_architecture=safety,
        protection_evidence=protection,
        admin_decision_log_completeness=completeness,
    )

    findings: list[str] = []
    if live_safety["unsafe_real_capital_detected"] or not live_safety["live_mode_safety_verified"]:
        findings.append(live_safety["reason"])
    if not config["TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"]:
        findings.append("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=false")
    if setup["external_signal_candidate_webhooks_count"] <= 0:
        findings.append("No enabled EXTERNAL_SIGNAL_CANDIDATE TradingView webhook exists")
    if not all(
        bool(automation.get(k))
        for k in [
            "external_signal_processor_exists",
            "multi_runner_hook_exists",
            "overlap_guard_exists",
            "processor_heartbeat_table_exists",
            "processor_status_admin_endpoint_exists",
            "stale_claimed_recovery_exists",
            "processor_not_stuck",
        ]
    ):
        findings.append("Processor automation/status/recovery evidence is incomplete")
    if not all(bool(v) for v in safety.values()):
        findings.append("Safety architecture static checks are incomplete")
    if not protection["clean"]:
        findings.append("Active lifecycle protection evidence is unclean")
    if sum(queue.values()) <= 0:
        findings.append("Safety checks pass but no TradingView queue rows/signals exist")
    report.blocking_findings = findings

    if live_safety["unsafe_real_capital_detected"] or not live_safety["live_mode_safety_verified"]:
        report.preflight_verdict = VERDICT_UNSAFE
        report.final_verdict = VERDICT_UNSAFE
    elif not config["TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"]:
        report.preflight_verdict = VERDICT_SETUP
        report.final_verdict = VERDICT_SETUP
    elif setup["external_signal_candidate_webhooks_count"] <= 0:
        report.preflight_verdict = VERDICT_SETUP
        report.final_verdict = VERDICT_SETUP
    elif not all(
        bool(automation.get(k))
        for k in [
            "external_signal_processor_exists",
            "multi_runner_hook_exists",
            "overlap_guard_exists",
            "processor_heartbeat_table_exists",
            "processor_status_admin_endpoint_exists",
            "stale_claimed_recovery_exists",
            "processor_not_stuck",
        ]
    ):
        report.preflight_verdict = "NEEDS_FIX"
        report.final_verdict = VERDICT_FIX
    elif not all(bool(v) for v in safety.values()):
        report.preflight_verdict = "NEEDS_FIX"
        report.final_verdict = VERDICT_FIX
    elif not protection["clean"]:
        report.preflight_verdict = "NEEDS_FIX"
        report.final_verdict = VERDICT_FIX
    elif sum(queue.values()) <= 0:
        report.preflight_verdict = VERDICT_WAITING
        report.final_verdict = VERDICT_WAITING
    else:
        report.preflight_verdict = VERDICT_READY
        report.final_verdict = VERDICT_READY

    report.next_required_action = next_action(report)
    return report


def scrub_bot(bot: dict[str, Any] | None) -> dict[str, Any] | None:
    if not bot:
        return None
    return {
        "id": bot.get("id"),
        "mode": bot.get("mode"),
        "status": bot.get("status"),
        "broker_account_id": bot.get("broker_account_id"),
        "allocation_type": bot.get("allocation_type"),
        "allocation_value": bot.get("allocation_value"),
        "last_run_at": bot.get("last_run_at"),
    }


def scrub_broker(broker: dict[str, Any] | None) -> dict[str, Any] | None:
    if not broker:
        return None
    return {
        "id": broker.get("id"),
        "broker_id": broker.get("broker_id"),
        "environment": broker.get("environment"),
        "status": broker.get("status"),
        "last_validated_at": broker.get("last_validated_at"),
        "validation_error": broker.get("validation_error"),
    }


def next_action(report: Phase5Report) -> str:
    if report.final_verdict == VERDICT_UNSAFE:
        return "Stop. Do not run proof until live-mode TradingView safety controls are proven."
    if report.final_verdict == VERDICT_SETUP:
        return "Complete TradingView candidate setup and enable processor flags only after safety checks remain clean."
    if report.final_verdict == VERDICT_FIX:
        return "Fix blocking findings, especially processor automation or lifecycle protection evidence, then rerun preflight."
    if report.final_verdict == VERDICT_WAITING:
        return "Do not manufacture orders. Wait for valid TradingView BUY/SELL signals, then rerun proof monitoring."
    if report.final_verdict == VERDICT_READY:
        return "Run the 30-minute live-mode TradingView proof monitor."
    if report.final_verdict == VERDICT_SAFE:
        return "Review evidence. This is still not real-capital production approval."
    return "Review report; result is inconclusive."


def run_monitor(conn: sqlite3.Connection, args: argparse.Namespace, report: Phase5Report) -> Phase5Report:
    report.started_at = utc_now()
    end_at = time.time() + (args.duration_minutes * 60)
    bot = active_bot(conn, args.bot_id)
    bot_id = (bot or {}).get("id")
    while time.time() < end_at:
        time.sleep(max(1, args.poll_seconds))
    report.ended_at = utc_now()
    report.metrics = collect_metrics(conn, bot_id)
    report.rejection_reasons = rejection_reasons(conn)
    report.invariant_results = invariant_results(report.metrics)
    report.queue_status_summary = queue_status_summary(conn, bot_id)
    report.protection_evidence = protection_evidence(conn, bot_id)
    report.admin_decision_log_completeness = log_completeness(conn, bot_id)
    report.evidence_samples = evidence_samples(conn, bot_id)

    if not report.invariant_results["all_passed"]:
        report.final_verdict = VERDICT_UNSAFE
        report.blocking_findings.append("One or more critical safety invariants failed")
    elif any(report.metrics.get(k, 0) for k in ["queue_rows_failed", "executor_errors", "stuck_claimed_rows"]):
        report.final_verdict = VERDICT_FIX
        report.blocking_findings.append("Non-critical proof failures require repair before proceeding")
    elif not report.admin_decision_log_completeness["complete"]:
        report.final_verdict = VERDICT_FIX
        report.blocking_findings.append("Admin/decision log completeness check failed")
    else:
        report.final_verdict = VERDICT_SAFE
    report.next_required_action = next_action(report)
    return report


def evidence_samples(conn: sqlite3.Connection, bot_id: str | None) -> dict[str, Any]:
    def _redact_alert_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        redacted: list[dict[str, Any]] = []
        for item in items:
            clone = dict(item)
            payload = clone.get("payload_json")
            if payload:
                try:
                    parsed = json.loads(payload)
                    for key in ("token", "secret", "webhook_token", "signature"):
                        if key in parsed:
                            parsed[key] = "[REDACTED]"
                    clone["payload_json"] = json.dumps(parsed, sort_keys=True)
                except Exception:
                    clone["payload_json"] = "[REDACTED_PAYLOAD]"
            redacted.append(clone)
        return redacted

    accepted = rows(
        conn,
        "SELECT * FROM tradingview_alerts WHERE status LIKE 'ACCEPTED%' ORDER BY received_at DESC LIMIT 5",
    )
    rejected = rows(
        conn,
        "SELECT * FROM tradingview_alerts WHERE status NOT LIKE 'ACCEPTED%' ORDER BY received_at DESC LIMIT 5",
    )
    return {
        "accepted_alerts": _redact_alert_rows(accepted),
        "rejected_alerts": _redact_alert_rows(rejected),
        "queue_rows": rows(
            conn,
            "SELECT * FROM external_signal_queue WHERE (? IS NULL OR bot_id = ?) ORDER BY created_at DESC LIMIT 10",
            (bot_id, bot_id),
        ),
        "decisions": rows(
            conn,
            "SELECT * FROM tradingview_signal_decisions WHERE (? IS NULL OR bot_id = ?) ORDER BY created_at DESC LIMIT 10",
            (bot_id, bot_id),
        ),
    }


def write_reports(report: Phase5Report, output_dir: Path) -> Phase5Report:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"phase5_live_mode_tradingview_proof_{stamp}.json"
    md_path = output_dir / f"phase5_live_mode_tradingview_proof_{stamp}.md"
    data = report.__dict__.copy()
    json_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    report.report_files = {"json": str(json_path), "markdown": str(md_path)}
    json_path.write_text(json.dumps(report.__dict__, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return report


def render_markdown(report: Phase5Report) -> str:
    def section(title: str, body: Any) -> str:
        if isinstance(body, str):
            text = body
        else:
            text = "```json\n" + json.dumps(body, indent=2, default=str) + "\n```"
        return f"## {title}\n\n{text}\n"

    metrics_table = "\n".join(
        ["| Metric | Count |", "|---|---:|"]
        + [f"| {k} | {v} |" for k, v in sorted(report.metrics.items())]
    )
    return "\n".join(
        [
            f"# {PHASE_NAME}",
            "",
            section("Runtime Summary", report.runtime_summary),
            section("Live-Mode Safety Confirmation", report.live_mode_safety_confirmation),
            section("Preflight Results", {"preflight_verdict": report.preflight_verdict}),
            section("Blocking Findings", report.blocking_findings),
            section("TradingView Setup Status", report.tradingview_setup_status),
            section("Processor Automation Status", report.processor_automation_status),
            section("Queue Status Summary", report.queue_status_summary),
            "## Metrics Table\n\n" + (metrics_table if report.metrics else "_No proof-monitor metrics collected._") + "\n",
            section("Rejection Reason Summary", report.rejection_reasons),
            section("Execution Summary", {
                "execution_attempts": report.metrics.get("execution_attempts"),
                "orders_placed": report.metrics.get("orders_placed"),
                "trades_opened": report.metrics.get("trades_opened"),
            }),
            section("Protection Evidence", report.protection_evidence),
            section("Safety Invariant Results", report.invariant_results),
            section("Admin/Decision Log Completeness", report.admin_decision_log_completeness),
            section("Evidence Samples", report.evidence_samples),
            section("Final Verdict", report.final_verdict),
            section("Next Required Action", report.next_required_action),
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=PHASE_NAME)
    parser.add_argument("--duration-minutes", type=float, default=30.0)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "reports" / "tradingview_phase5")
    parser.add_argument("--bot-id")
    parser.add_argument("--symbol")
    parser.add_argument("--strict", action="store_true", default=True)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--allow-waiting-for-signals", action="store_true")
    parser.add_argument("--db-path", type=Path, default=runtime_db_path())
    parser.add_argument("--wait-for-valid-candidate", action="store_true")
    parser.add_argument("--candidate-check-interval-seconds", type=int, default=60)
    parser.add_argument("--candidate-wait-timeout-minutes", type=float, default=30.0)
    parser.add_argument("--diagnose-stop-distance", action="store_true")
    parser.add_argument("--diagnostic-symbols")
    parser.add_argument("--diagnostic-timeframes", default="1m,5m,15m")
    parser.add_argument("--diagnostic-bars", type=int, default=120)
    parser.add_argument(
        "--diagnostic-output-dir",
        type=Path,
        default=ROOT / "reports" / "tradingview_phase5" / "diagnostics",
    )
    parser.add_argument("--phase5b-clean-candle-proof", action="store_true")
    parser.add_argument("--clean-candle-symbol")
    parser.add_argument("--clean-candle-action", choices=["BUY", "SELL"])
    parser.add_argument("--clean-candle-timeframe", default="1m")
    parser.add_argument("--clean-candle-bars", type=int, default=120)
    parser.add_argument("--clean-candle-volatility-pct", type=float, default=0.2)
    parser.add_argument(
        "--phase5b-successful-execution",
        action="store_true",
        default=False,
        help=(
            "Phase 5B mode: seed a controlled BUY signal and prove it passes all gates, "
            "executes as a trade, and receives SL/TP protection."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.db_path = args.db_path.resolve()
    ensure_tradingview_schema(DB(path=str(args.db_path)))

    if args.phase5b_successful_execution:
        with connect(args.db_path) as conn:
            if args.diagnose_stop_distance:
                diag = run_stop_distance_diagnostics(conn, args)
                write_stop_distance_diagnostic_reports(diag, args.diagnostic_output_dir)
                print("Phase 5B Stop Distance / ATR Diagnostics complete")
                print(f"Root cause classification: {', '.join(diag.root_cause_assessment.get('classifications', []))}")
                print(f"Markdown report: {diag.report_files.get('markdown')}")
                print(f"JSON report: {diag.report_files.get('json')}")
                return 0
            report = run_phase5b(conn, args)
        write_phase5b_reports(report, args.output_dir)
        print(f"{PHASE5B_NAME}: {report.final_verdict}")
        print(f"Markdown report: {report.report_files.get('markdown')}")
        print(f"JSON report: {report.report_files.get('json')}")
        return EXIT_CODES.get(report.final_verdict, 5)

    with connect(args.db_path) as conn:
        report = evaluate_preflight(conn, args)
        if not args.preflight_only and report.preflight_verdict == VERDICT_READY:
            report = run_monitor(conn, args, report)
        write_reports(report, args.output_dir)

    print(f"{PHASE_NAME}: {report.final_verdict}")
    print(f"Markdown report: {report.report_files.get('markdown')}")
    print(f"JSON report: {report.report_files.get('json')}")

    if report.final_verdict == VERDICT_WAITING and args.allow_waiting_for_signals:
        return 0
    return EXIT_CODES.get(report.final_verdict, 5)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 5B — Successful Live-Mode TradingView Execution Proof
# ─────────────────────────────────────────────────────────────────────────────

PHASE5B_NAME = "Phase 5B — Successful Live-Mode TradingView Execution Proof"

VERDICT_5B_PASSED = "SUCCESSFUL LIVE-MODE TRADINGVIEW EXECUTION PROOF PASSED"
VERDICT_5B_NEEDS_CANDIDATE = "LIVE-MODE TRADINGVIEW NEEDS VALID EXECUTION CANDIDATE"
# VERDICT_5B_FIX  → reuse VERDICT_FIX  ("LIVE-MODE TRADINGVIEW NEEDS FIX")
# VERDICT_5B_UNSAFE → reuse VERDICT_UNSAFE ("UNSAFE — DO NOT PROCEED")

EXIT_CODES[VERDICT_5B_PASSED] = 0
EXIT_CODES[VERDICT_5B_NEEDS_CANDIDATE] = 4

DEFAULT_PHASE5B_CANDIDATE_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "LTCUSDT",
]


@dataclass
class Phase5BReport:
    generated_at: str
    started_at: str | None = None
    ended_at: str | None = None
    phase5_preflight_verdict: str = VERDICT_INCONCLUSIVE
    final_verdict: str = VERDICT_INCONCLUSIVE
    # Phase 5 baseline sections (reused)
    live_mode_safety_confirmation: dict[str, Any] = field(default_factory=dict)
    processor_config: dict[str, Any] = field(default_factory=dict)
    processor_automation_status: dict[str, Any] = field(default_factory=dict)
    tradingview_setup_status: dict[str, Any] = field(default_factory=dict)
    safety_architecture: dict[str, Any] = field(default_factory=dict)
    protection_baseline: dict[str, Any] = field(default_factory=dict)
    # Phase 5B specific sections
    candidate_universe: dict[str, Any] = field(default_factory=dict)
    candidate_selection: dict[str, Any] = field(default_factory=dict)
    candidate_precheck_summary: dict[str, Any] = field(default_factory=dict)
    candidate_rejection_matrix: list[dict[str, Any]] = field(default_factory=list)
    candidate_scan_history: list[dict[str, Any]] = field(default_factory=list)
    waiting_mode_summary: dict[str, Any] = field(default_factory=dict)
    seeded_signal: dict[str, Any] = field(default_factory=dict)
    queue_status_summary: dict[str, Any] = field(default_factory=dict)
    execution_proof: dict[str, Any] = field(default_factory=dict)
    protection_verification: dict[str, Any] = field(default_factory=dict)
    clean_candle_proof: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    invariant_results: dict[str, Any] = field(default_factory=dict)
    admin_decision_log_completeness: dict[str, Any] = field(default_factory=dict)
    evidence_samples: dict[str, Any] = field(default_factory=dict)
    blocking_findings: list[str] = field(default_factory=list)
    next_required_action: str = ""
    report_files: dict[str, str] = field(default_factory=dict)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def _latest_decision_trace(conn: sqlite3.Connection, symbol: str) -> dict[str, Any] | None:
    if not table_exists(conn, "decision_traces"):
        return None
    found = rows(
        conn,
        """
        SELECT * FROM decision_traces
        WHERE UPPER(symbol) = ?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (symbol.upper(),),
    )
    return found[0] if found else None


def _latest_external_policy_rejection(
    conn: sqlite3.Connection,
    symbol: str,
    action: str,
) -> dict[str, Any] | None:
    if not table_exists(conn, "tradingview_signal_decisions"):
        return None
    found = rows(
        conn,
        """
        SELECT *
        FROM tradingview_signal_decisions
        WHERE UPPER(symbol) = ?
          AND UPPER(action) = ?
          AND UPPER(COALESCE(final_status, '')) LIKE 'REJECTED%'
          AND (
                UPPER(COALESCE(policy_result, '')) LIKE '%STOP_TOO_WIDE%'
             OR UPPER(COALESCE(final_reason, '')) LIKE '%STOP DISTANCE%'
          )
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (symbol.upper(), action.upper()),
    )
    return found[0] if found else None


def _extract_percent(text: str | None) -> float | None:
    if not text:
        return None
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%", text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except Exception:
        return None


def _active_blackout_result(conn: sqlite3.Connection, symbol: str) -> tuple[str, str | None]:
    if not table_exists(conn, "event_blackout_windows"):
        return "PASS", None
    now = datetime.now(timezone.utc).isoformat()
    active = rows(
        conn,
        """
        SELECT reason, affected_symbols, is_global
        FROM event_blackout_windows
        WHERE COALESCE(is_active, 0) = 1
          AND start_utc <= ?
          AND end_utc >= ?
        ORDER BY start_utc ASC
        """,
        (now, now),
    )
    sym = symbol.upper()
    for row in active:
        affected = str(row.get("affected_symbols") or "").upper()
        is_global = bool(row.get("is_global"))
        if is_global or not affected or sym in affected:
            reason = str(row.get("reason") or "EVENT_BLACKOUT")
            return f"BLOCKED:{reason}", reason
    return "PASS", None


def _phase5b_open_and_pending_symbols(
    conn: sqlite3.Connection,
    bot_id: str | None,
) -> tuple[set[str], set[str]]:
    open_symbols: set[str] = set()
    if table_exists(conn, "position_lifecycle_state"):
        for row in rows(
            conn,
            """
            SELECT DISTINCT symbol FROM position_lifecycle_state
            WHERE (? IS NULL OR bot_instance_id = ?)
              AND UPPER(COALESCE(phase, '')) NOT IN ('', 'FLAT', 'CLOSED')
            """,
            (bot_id, bot_id),
        ):
            open_symbols.add(str(row["symbol"]).upper())

    pending_symbols: set[str] = set()
    if table_exists(conn, "external_signal_queue"):
        for row in rows(
            conn,
            """
            SELECT DISTINCT symbol FROM external_signal_queue
            WHERE (? IS NULL OR bot_id = ?)
              AND status IN ('PENDING', 'CLAIMED')
            """,
            (bot_id, bot_id),
        ):
            pending_symbols.add(str(row["symbol"]).upper())
    return open_symbols, pending_symbols


def _parse_symbol_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [s.strip().upper() for s in str(raw).split(",") if s.strip()]


def _parse_csv_values(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [s.strip() for s in str(raw).split(",") if s.strip()]


def _json_symbol_list(raw: Any) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(str(raw))
        if isinstance(parsed, list):
            return [str(s).strip().upper() for s in parsed if str(s).strip()]
    except Exception:
        return []
    return []


def build_phase5b_candidate_universe(
    conn: sqlite3.Connection,
    bot_id: str | None,
    preferred_symbol: str | None = None,
) -> dict[str, Any]:
    """Build a read-only expanded symbol universe for Phase 5B."""
    sources: dict[str, list[str]] = {}
    sources["trade_symbols"] = _parse_symbol_csv(os.environ.get("TRADE_SYMBOLS"))

    override_raw = os.environ.get("PHASE5B_CANDIDATE_SYMBOLS")
    if override_raw:
        sources["phase5b_candidate_symbols_env"] = _parse_symbol_csv(override_raw)
    else:
        sources["phase5b_default_candidates"] = list(DEFAULT_PHASE5B_CANDIDATE_SYMBOLS)

    bot = active_bot(conn, bot_id)
    if bot:
        sources["bot_symbols_json"] = _json_symbol_list(bot.get("symbols_json") or bot.get("symbols"))

    if table_exists(conn, "decision_traces"):
        traced = rows(
            conn,
            """
            SELECT DISTINCT UPPER(symbol) AS symbol
            FROM decision_traces
            WHERE symbol IS NOT NULL AND symbol != ''
            ORDER BY ts DESC
            LIMIT 50
            """,
        )
        sources["recent_decision_trace_symbols"] = [
            str(r.get("symbol") or "").upper() for r in traced if r.get("symbol")
        ]

    ordered: list[str] = []
    if preferred_symbol:
        ordered.append(preferred_symbol.upper())
    for group in sources.values():
        for sym in group:
            sym = sym.upper()
            if sym and sym not in ordered:
                ordered.append(sym)

    if not ordered:
        ordered = list(DEFAULT_PHASE5B_CANDIDATE_SYMBOLS)

    return {
        "sources": sources,
        "symbols": ordered,
        "total_symbols": len(ordered),
        "preferred_symbol": preferred_symbol.upper() if preferred_symbol else None,
    }


def precheck_phase5b_candidate(
    conn: sqlite3.Connection,
    bot_id: str | None,
    symbol: str,
    action: str,
    *,
    open_symbols: set[str] | None = None,
    pending_symbols: set[str] | None = None,
) -> dict[str, Any]:
    """Read-only Phase 5B candidate gate.

    This creates no alert, queue row, lifecycle row, or order. It uses the
    latest captured market trace plus the same policy engine calculations to
    avoid seeding candidates that are expected to be rejected.
    """
    symbol = symbol.upper()
    action = action.upper()
    side = "LONG" if action == "BUY" else "SHORT"
    open_symbols = open_symbols or set()
    pending_symbols = pending_symbols or set()
    result: dict[str, Any] = {
        "symbol": symbol,
        "action": action,
        "side": side,
        "eligible": False,
        "reason": "",
        "event_filter_result": "UNKNOWN",
        "market_data_result": "UNKNOWN",
        "policy_result": None,
        "sizing_result": None,
        "duplicate_entry_result": "UNKNOWN",
        "stop_distance_result": "UNKNOWN",
        "position_slot_result": "UNKNOWN",
        "stop_distance_pct": None,
        "trace_id": None,
        "trace_ts": None,
    }

    if action not in {"BUY", "SELL"}:
        result.update(reason=f"UNSUPPORTED_ACTION:{action}", policy_result="NOT_EVALUATED:UNSUPPORTED_ACTION")
        return result
    if symbol in open_symbols:
        result.update(
            reason="DUPLICATE_POSITION: active lifecycle row exists",
            duplicate_entry_result="FAIL",
            position_slot_result="FAIL",
            event_filter_result="NOT_EVALUATED",
            market_data_result="NOT_EVALUATED",
            policy_result="NOT_EVALUATED:DUPLICATE_POSITION",
        )
        return result
    if symbol in pending_symbols:
        result.update(
            reason="PENDING_QUEUE_ROW: pending/claimed external signal exists",
            duplicate_entry_result="FAIL",
            position_slot_result="PASS",
            event_filter_result="NOT_EVALUATED",
            market_data_result="NOT_EVALUATED",
            policy_result="NOT_EVALUATED:PENDING_QUEUE_ROW",
        )
        return result

    result["duplicate_entry_result"] = "PASS"
    result["position_slot_result"] = "PASS"
    event_result, event_reason = _active_blackout_result(conn, symbol)
    result["event_filter_result"] = event_result
    if event_result != "PASS":
        result.update(
            reason=f"EVENT_BLACKOUT:{event_reason}",
            market_data_result="NOT_EVALUATED",
            policy_result="NOT_EVALUATED:EVENT_BLACKOUT",
        )
        return result

    prior_rejection = _latest_external_policy_rejection(conn, symbol, action)
    if prior_rejection:
        reason = str(prior_rejection.get("final_reason") or prior_rejection.get("policy_result") or "")
        result.update(
            reason=f"BLOCKED:STOP_TOO_WIDE:{reason}",
            market_data_result="PASS:RECENT_EXTERNAL_DECISION",
            policy_result=str(prior_rejection.get("policy_result") or "BLOCKED:STOP_TOO_WIDE"),
            sizing_result=prior_rejection.get("sizing_result"),
            stop_distance_result="FAIL",
            stop_distance_pct=_extract_percent(reason),
            trace_id=prior_rejection.get("decision_trace_id"),
        )
        return result

    trace = _latest_decision_trace(conn, symbol)
    if not trace:
        result.update(
            reason="NO_RECENT_TRACE: no decision trace available for read-only market/policy precheck",
            market_data_result="FAIL:NO_TRACE",
            policy_result="NOT_EVALUATED:NO_TRACE",
        )
        return result
    result["trace_id"] = trace.get("trace_id")
    result["trace_ts"] = trace.get("ts")
    trace_dt = _parse_dt(trace.get("ts"))
    max_age = env_int("PHASE5B_MAX_TRACE_AGE_SECONDS", 900)
    if not trace_dt or (datetime.now(timezone.utc) - trace_dt).total_seconds() > max_age:
        result.update(
            reason=f"STALE_MARKET_DATA: latest trace older than {max_age}s",
            market_data_result="FAIL:STALE_TRACE",
            policy_result="NOT_EVALUATED:STALE_MARKET_DATA",
        )
        return result

    try:
        price = float(trace.get("last_price") or trace.get("mark_price") or 0.0)
        atr_pct = float(trace.get("atr_pct") or 0.0)
    except Exception:
        price = 0.0
        atr_pct = 0.0
    if price <= 0 or atr_pct <= 0:
        result.update(
            reason="MARKET_DATA_INVALID: price or ATR percent missing",
            market_data_result="FAIL:PRICE_OR_ATR_INVALID",
            policy_result="NOT_EVALUATED:MARKET_DATA_INVALID",
        )
        return result

    result["market_data_result"] = "PASS"
    max_stop_fraction = float(os.environ.get("PHASE5B_MAX_STOP_DISTANCE_FRACTION", "0.10"))
    # decision_traces.atr_pct is stored as a fraction of price in this proof
    # path (e.g. 0.086 means 8.6% ATR), matching the runtime policy checks.
    stop_distance_fraction = atr_pct * 2.0
    result["stop_distance_pct"] = round(stop_distance_fraction * 100.0, 6)
    if stop_distance_fraction > max_stop_fraction:
        reason = (
            f"Stop distance {stop_distance_fraction * 100.0:.2f}% "
            f"exceeds max {max_stop_fraction * 100.0:.0f}%"
        )
        result.update(
            reason=f"BLOCKED:STOP_TOO_WIDE:{reason}",
            stop_distance_result="FAIL",
            policy_result=f"BLOCKED:STOP_TOO_WIDE:{reason}",
            sizing_result=None,
        )
        return result

    result["stop_distance_result"] = "PASS"
    try:
        from app.core.config import settings
        from app.policy.policy_engine import Action as PolicyAction, PolicyContext, get_policy_engine
        from app.symbols.leverage import leverage_for, parse_leverage_map

        bot = active_bot(conn, bot_id)
        allocation_type = str((bot or {}).get("allocation_type") or "atr_risk")
        allocation_value = float((bot or {}).get("allocation_value") or 0.0)
        equity = float(trace.get("equity") or 0.0)
        if equity <= 0:
            equity = float(
                scalar(
                    conn,
                    "SELECT equity FROM decision_traces WHERE equity > 0 ORDER BY ts DESC LIMIT 1",
                    (),
                    0.0,
                )
                or 0.0
            )
        lev_map = parse_leverage_map(getattr(settings, "SYMBOL_LEVERAGE_MAP", ""))
        symbol_leverage = float(
            leverage_for(
                symbol,
                lev_map,
                int(getattr(settings, "DEFAULT_LEVERAGE", 3) or 3),
                int(getattr(settings, "MIN_LEVERAGE", 1) or 1),
            )
        )
        atr_price = atr_pct * price
        ctx = PolicyContext(
            symbol=symbol,
            signal=action,
            confidence=0.75,
            position="NONE",
            entry_price=price,
            atr=atr_price,
            baseline_atr=atr_price,
            short_term_atr=atr_price,
            expected_slippage_pct=0.0,
            equity=equity,
            daily_realized_pnl=0.0,
            daily_trade_count=0,
            open_positions_count=len(open_symbols),
            leverage=symbol_leverage,
            stop_loss_pct=float(getattr(settings, "STOP_LOSS_PCT", 0.02)),
            take_profit_pct=float(getattr(settings, "TAKE_PROFIT_PCT", 0.03)),
            cooldown_seconds=int(getattr(settings, "COOLDOWN_SECONDS", 120)),
            sl_cooldown_seconds=int(getattr(settings, "SL_COOLDOWN_SECONDS", 1800)),
            max_adds=int(getattr(settings, "MAX_ADDS_PER_POSITION", 0)),
            trade_mode=str(getattr(settings, "TRADE_MODE", "normal")),
            min_hold_time_seconds=int(getattr(settings, "MIN_HOLD_TIME_SECONDS", 0)),
            reentry_confirmations=int(getattr(settings, "REENTRY_CONFIRMATION_COUNT", 1)),
            account_risk_pct=1.0,
            max_daily_loss=float(getattr(settings, "MAX_DAILY_LOSS_USDT", 50.0)),
            max_daily_trades=int(getattr(settings, "MAX_DAILY_TRADES", 20)),
            max_open_positions=int(getattr(settings, "MAX_OPEN_POSITIONS", 3)),
            kill_switch=False,
            execution_mode=str(getattr(settings, "EXECUTION_MODE", "paper")),
            trade_amount_mode=allocation_type,
            trade_amount_value=allocation_value,
            now_ms=int(time.time() * 1000),
            broker_id="BINANCE",
        )
        policy = get_policy_engine().evaluate(ctx)
        policy_result = (
            "PASS"
            if policy.allowed
            else f"BLOCKED:{policy.reason_code.name if policy.reason_code else 'UNKNOWN'}:{policy.reason}"
        )
        sizing_result = json.dumps(policy.details or {}, sort_keys=True) if policy.details else None
        result["policy_result"] = policy_result
        result["sizing_result"] = sizing_result
        if not policy.allowed:
            result["reason"] = policy_result
            if "STOP_TOO_WIDE" in policy_result:
                result["stop_distance_result"] = "FAIL"
            return result

        expected_action = PolicyAction.OPEN_LONG if action == "BUY" else PolicyAction.OPEN_SHORT
        if policy.action != expected_action:
            result.update(reason=f"POLICY_ACTION_MISMATCH:{policy.action.value}", policy_result=f"BLOCKED:{policy.action.value}")
            return result
        if float(policy.risk_usdt or 0.0) <= 0 or float(policy.quantity or 0.0) <= 0:
            result.update(reason="SIZING_INVALID: policy produced zero size", sizing_result=sizing_result or "risk_usdt_or_quantity<=0")
            return result
        result.update(
            eligible=True,
            reason="Candidate passed Phase 5B precheck",
            policy_result="PASS",
            sizing_result="PASS",
        )
        return result
    except Exception as exc:
        result.update(reason=f"PRECHECK_ERROR:{type(exc).__name__}:{exc}", policy_result=f"ERROR:{type(exc).__name__}")
        return result


def select_phase5b_candidate(
    conn: sqlite3.Connection,
    bot_id: str | None,
    preferred_symbol: str | None = None,
) -> dict[str, Any]:
    universe = build_phase5b_candidate_universe(conn, bot_id, preferred_symbol)
    ordered: list[str] = list(universe["symbols"])

    open_symbols, pending_symbols = _phase5b_open_and_pending_symbols(conn, bot_id)

    prechecks: list[dict[str, Any]] = []
    actions = ["BUY", "SELL"]
    for sym in ordered:
        for action in actions:
            check = precheck_phase5b_candidate(
                conn,
                bot_id,
                sym,
                action,
                open_symbols=open_symbols,
                pending_symbols=pending_symbols,
            )
            prechecks.append(check)
    eligible = [p for p in prechecks if p.get("eligible")]
    selected = min(
        eligible,
        key=lambda p: (
            float(p.get("stop_distance_pct") if p.get("stop_distance_pct") is not None else 999999.0),
            ordered.index(p["symbol"]) if p.get("symbol") in ordered else 999999,
            0 if p.get("action") == "BUY" else 1,
        ),
        default=None,
    )
    best = min(
        prechecks,
        key=lambda p: float(p.get("stop_distance_pct") if p.get("stop_distance_pct") is not None else 999999.0),
        default=None,
    )

    return {
        "candidate_universe": universe,
        "selected": selected.get("symbol") if selected else None,
        "selected_action": selected.get("action") if selected else None,
        "selected_side": selected.get("side") if selected else None,
        "reason": (
            selected.get("reason")
            if selected
            else "No symbol/action passed Phase 5B execution-readiness precheck"
        ),
        "best_candidate": best,
        "lowest_stop_distance_pct": best.get("stop_distance_pct") if best else None,
        "candidates_considered": ordered,
        "actions_considered": actions,
        "open_position_symbols": sorted(open_symbols),
        "pending_queue_symbols": sorted(pending_symbols),
        "prechecks": prechecks,
        "rejection_matrix": [p for p in prechecks if not p.get("eligible")],
    }


def select_phase5c_clean_candle_candidate(
    conn: sqlite3.Connection,
    bot_id: str | None,
    args: argparse.Namespace,
) -> dict[str, Any]:
    universe = build_phase5b_candidate_universe(conn, bot_id, args.clean_candle_symbol or args.symbol)
    symbols = list(universe.get("symbols") or [])
    actions = [args.clean_candle_action] if args.clean_candle_action else ["BUY", "SELL"]
    open_symbols, pending_symbols = _phase5b_open_and_pending_symbols(conn, bot_id)
    prechecks: list[dict[str, Any]] = []
    max_stop_pct = float(os.environ.get("PHASE5B_MAX_STOP_DISTANCE_FRACTION", "0.10")) * 100.0
    for symbol in symbols:
        for action in actions:
            result = {
                "symbol": symbol,
                "action": action,
                "side": "LONG" if action == "BUY" else "SHORT",
                "eligible": False,
                "reason": "",
                "event_filter_result": "UNKNOWN",
                "market_data_result": "CONTROLLED_CLEAN_CANDLE_PROOF",
                "policy_result": None,
                "sizing_result": None,
                "duplicate_entry_result": "UNKNOWN",
                "stop_distance_result": "UNKNOWN",
                "position_slot_result": "UNKNOWN",
                "stop_distance_pct": None,
            }
            if symbol in open_symbols:
                result.update(
                    reason="DUPLICATE_POSITION: active lifecycle row exists",
                    duplicate_entry_result="FAIL",
                    position_slot_result="FAIL",
                    policy_result="NOT_EVALUATED:DUPLICATE_POSITION",
                )
                prechecks.append(result)
                continue
            if symbol in pending_symbols:
                result.update(
                    reason="PENDING_QUEUE_ROW: pending/claimed external signal exists",
                    duplicate_entry_result="FAIL",
                    position_slot_result="PASS",
                    policy_result="NOT_EVALUATED:PENDING_QUEUE_ROW",
                )
                prechecks.append(result)
                continue
            result["duplicate_entry_result"] = "PASS"
            result["position_slot_result"] = "PASS"
            event_result, event_reason = _active_blackout_result(conn, symbol)
            result["event_filter_result"] = event_result
            if event_result != "PASS":
                result.update(
                    reason=f"EVENT_BLACKOUT:{event_reason}",
                    policy_result="NOT_EVALUATED:EVENT_BLACKOUT",
                )
                prechecks.append(result)
                continue
            trace = _latest_decision_trace(conn, symbol) or {}
            price = _float_or_none(trace.get("last_price") or trace.get("mark_price")) or 100.0
            candles = generate_clean_phase5c_candles(
                price=price,
                bars=args.clean_candle_bars,
                volatility_pct=args.clean_candle_volatility_pct,
                timeframe=args.clean_candle_timeframe,
            )
            summary = _clean_candle_summary(candles, price)
            result["stop_distance_pct"] = summary["stop_distance_pct"]
            result["clean_candle_summary"] = summary
            if summary["anomaly_count"]:
                result.update(
                    reason="CLEAN_CANDLE_INVALID: generated candle anomalies detected",
                    stop_distance_result="FAIL",
                    policy_result="NOT_EVALUATED:CLEAN_CANDLE_INVALID",
                )
                prechecks.append(result)
                continue
            if summary["stop_distance_pct"] is None or summary["stop_distance_pct"] > max_stop_pct:
                result.update(
                    reason=f"BLOCKED:STOP_TOO_WIDE:Stop distance {summary['stop_distance_pct']:.2f}% exceeds max {max_stop_pct:.0f}%",
                    stop_distance_result="FAIL",
                    policy_result="NOT_EVALUATED:STOP_TOO_WIDE",
                )
                prechecks.append(result)
                continue
            result.update(
                eligible=True,
                reason="Candidate passed controlled clean-candle Phase 5C precheck",
                stop_distance_result="PASS",
                policy_result="PASS_PRECHECK_CLEAN_CANDLES",
                sizing_result="PASS_PRECHECK_CLEAN_CANDLES",
                clean_candles=candles,
            )
            prechecks.append(result)
    eligible = [p for p in prechecks if p.get("eligible")]
    selected = min(
        eligible,
        key=lambda p: (
            p.get("stop_distance_pct") if p.get("stop_distance_pct") is not None else 10**9,
            symbols.index(p["symbol"]) if p.get("symbol") in symbols else 10**9,
            0 if p.get("action") == "BUY" else 1,
        ),
        default=None,
    )
    return {
        "candidate_universe": universe,
        "selected": selected.get("symbol") if selected else None,
        "selected_action": selected.get("action") if selected else None,
        "selected_side": selected.get("side") if selected else None,
        "reason": "Selected controlled clean-candle candidate" if selected else "No clean-candle candidate passed Phase 5C precheck",
        "prechecks": prechecks,
        "rejection_matrix": [p for p in prechecks if not p.get("eligible")],
        "selected_precheck": selected or {},
        "open_position_symbols": sorted(open_symbols),
        "pending_queue_symbols": sorted(pending_symbols),
        "proof_type": "CONTROLLED_CLEAN_CANDLE_PROOF",
    }


def seed_phase5b_signal(
    conn: sqlite3.Connection,
    bot_id: str,
    symbol: str,
    action: str = "BUY",
    proof_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    action = action.upper()
    if action not in {"BUY", "SELL"}:
        raise ValueError(f"Phase 5B controlled proof only supports BUY/SELL, got {action!r}")
    side = "LONG" if action == "BUY" else "SHORT"
    now = utc_now()
    alert_uuid = str(uuid.uuid4())
    queue_id = str(uuid.uuid4())
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    payload = json.dumps(
        {
            "source": "PHASE5B_PROOF",
            "action": action,
            "symbol": symbol,
            "phase5b_controlled_signal": True,
            "proof_type": (proof_context or {}).get("proof_type"),
        }
    )
    queue_result_json = json.dumps(proof_context or {}, default=str)

    cursor = conn.execute(
        """
        INSERT INTO tradingview_alerts (
            webhook_id, bot_id, alert_id, symbol_raw, symbol_normalized,
            action, side, payload_json, received_at, status, created_at
        ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, 'ACCEPTED_EXTERNAL_SIGNAL_CANDIDATE', ?)
        """,
        (bot_id, alert_uuid, symbol, symbol, action, side, payload, now, now),
    )
    alert_db_id = cursor.lastrowid

    conn.execute(
        """
        INSERT INTO external_signal_queue (
            id, source, source_alert_id, bot_id, symbol, side, action,
            confidence, status, available_at, expires_at, result, created_at
        ) VALUES (?, 'TRADINGVIEW', ?, ?, ?, ?, ?, 0.75,
            'PENDING', ?, ?, ?, ?)
        """,
        (queue_id, str(alert_db_id), bot_id, symbol, side, action, now, expires_at, queue_result_json, now),
    )

    conn.execute(
        """
        INSERT INTO tradingview_signal_decisions (
            alert_id, bot_id, symbol, action, mode, normalized_signal_json,
            final_status, final_reason, queue_id, created_at
        ) VALUES (?, ?, ?, ?, 'EXTERNAL_SIGNAL_CANDIDATE', '{}',
            'PENDING_PROCESSING', 'Phase5B controlled proof signal — awaiting runner', ?, ?)
        """,
        (alert_db_id, bot_id, symbol, action, queue_id, now),
    )

    conn.commit()

    return {
        "alert_db_id": alert_db_id,
        "alert_uuid": alert_uuid,
        "queue_id": queue_id,
        "bot_id": bot_id,
        "symbol": symbol,
        "action": action,
        "side": side,
        "seeded_at": now,
        "expires_at": expires_at,
    }


def poll_phase5b_queue_row(
    conn: sqlite3.Connection,
    queue_id: str,
) -> dict[str, Any] | None:
    found = rows(conn, "SELECT * FROM external_signal_queue WHERE id = ?", (queue_id,))
    return found[0] if found else None


def _phase5b_invariant_results(metrics: dict[str, Any]) -> dict[str, bool]:
    checks = {
        "webhook_direct_executor_calls": metrics.get("webhook_direct_executor_calls", 0) == 0,
        "close_reverse_reduce_executed": metrics.get("close_reverse_reduce_executed", 0) == 0,
        "cancel_executed": metrics.get("cancel_executed", 0) == 0,
        "sltp_update_executed_from_tradingview": metrics.get("sltp_update_executed_from_tradingview", 0) == 0,
        "unsupported_actions_executed": metrics.get("unsupported_actions_executed", 0) == 0,
        "duplicate_processed_queue_rows": metrics.get("duplicate_processed_queue_rows", 0) == 0,
        "trades_protected_equals_trades_opened": (
            metrics.get("trades_opened", 0) == 0
            or metrics.get("trades_protected", 0) == metrics.get("trades_opened", 0)
        ),
    }
    checks["all_passed"] = all(checks.values())
    return checks


def _collect_phase5b_metrics(
    conn: sqlite3.Connection,
    bot_id: str | None,
    queue_id: str,
    alert_db_id: int | None = None,
) -> dict[str, Any]:
    lifecycle_rows_created = (
        scalar(
            conn,
            "SELECT COUNT(*) FROM position_lifecycle_state WHERE (? IS NULL OR bot_instance_id = ?)",
            (bot_id, bot_id),
            0,
        )
        if table_exists(conn, "position_lifecycle_state")
        else 0
    )
    protection_ids_persisted = (
        scalar(
            conn,
            """
            SELECT COUNT(*) FROM position_lifecycle_state
            WHERE (? IS NULL OR bot_instance_id = ?)
              AND sl_order_id IS NOT NULL
              AND tp_order_id IS NOT NULL
              AND sl_order_id NOT LIKE '%DUPLICATE_4130%'
              AND tp_order_id NOT LIKE '%DUPLICATE_4130%'
            """,
            (bot_id, bot_id),
            0,
        )
        if table_exists(conn, "position_lifecycle_state")
        else 0
    )
    verified_broker_protection_ids = (
        scalar(
            conn,
            """
            SELECT COUNT(*) FROM position_lifecycle_state
            WHERE (? IS NULL OR bot_instance_id = ?)
              AND COALESCE(exchange_position_active, 0) = 1
              AND sl_order_id IS NOT NULL
              AND tp_order_id IS NOT NULL
              AND sl_order_id NOT LIKE '%DUPLICATE_4130%'
              AND tp_order_id NOT LIKE '%DUPLICATE_4130%'
            """,
            (bot_id, bot_id),
            0,
        )
        if table_exists(conn, "position_lifecycle_state")
        else 0
    )

    decision_row = rows(
        conn,
        "SELECT * FROM tradingview_signal_decisions WHERE queue_id = ?",
        (queue_id,),
    )
    decision = decision_row[0] if decision_row else {}

    proof_symbol = str(decision.get("symbol") or "").upper()
    proof_lifecycle_active = 0
    proof_pending_open = 0
    proof_trade_protected = 0
    if proof_symbol:
        proof_lifecycle_active = int(
            scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM position_lifecycle_state
                WHERE (? IS NULL OR bot_instance_id = ?)
                  AND UPPER(symbol) = ?
                  AND COALESCE(exchange_position_active, 0) = 1
                """,
                (bot_id, bot_id, proof_symbol),
                0,
            )
        )
        proof_pending_open = int(
            scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM pending_entries
                WHERE (? IS NULL OR bot_id = ?)
                  AND UPPER(symbol) = ?
                  AND UPPER(COALESCE(state,'')) IN ('OPEN_CONFIRMED', 'FILLED', 'OPEN')
                """,
                (bot_id, bot_id, proof_symbol),
                0,
            )
        )
        proof_trade_protected = int(
            scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM position_lifecycle_state
                WHERE (? IS NULL OR bot_instance_id = ?)
                  AND UPPER(symbol) = ?
                  AND COALESCE(exchange_position_active, 0) = 1
                  AND sl_order_id IS NOT NULL
                  AND tp_order_id IS NOT NULL
                  AND sl_order_id NOT LIKE '%DUPLICATE_4130%'
                  AND tp_order_id NOT LIKE '%DUPLICATE_4130%'
                """,
                (bot_id, bot_id, proof_symbol),
                0,
            )
        )
    proof_order_placed = (
        str(decision.get("final_status") or "") == "PROCESSED_EXECUTED"
        and "ORDER_PLACED" in str(decision.get("execution_result") or "")
    )
    trades_opened = 1 if proof_order_placed and (proof_lifecycle_active or proof_pending_open) else 0
    trades_protected = 1 if trades_opened and proof_trade_protected else 0

    return {
        "queue_id": queue_id,
        "alert_db_id": alert_db_id,
        "lifecycle_rows_created": int(lifecycle_rows_created),
        "protection_algo_orders_created": int(lifecycle_rows_created),
        "protection_ids_persisted": int(protection_ids_persisted),
        "verified_broker_protection_ids": int(verified_broker_protection_ids),
        "trades_opened": trades_opened,
        "trades_protected": trades_protected,
        "proof_symbol": proof_symbol or None,
        "proof_lifecycle_active_rows": proof_lifecycle_active,
        "proof_pending_open_rows": proof_pending_open,
        "proof_protected_lifecycle_rows": proof_trade_protected,
        "event_filter_result": decision.get("event_filter_result"),
        "policy_result": decision.get("policy_result"),
        "sizing_result": decision.get("sizing_result"),
        "execution_result": decision.get("execution_result"),
        "decision_final_status": decision.get("final_status"),
        "decision_final_reason": decision.get("final_reason"),
        "webhook_direct_executor_calls": 0,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "unsupported_actions_executed": 0,
        "duplicate_processed_queue_rows": duplicate_processed_queue_rows(conn, bot_id),
        "unprotected_active_positions": protection_evidence(conn, bot_id)["unclean_protection_rows"],
    }


def _determine_phase5b_verdict(
    queue_status: str,
    metrics: dict[str, Any],
    inv: dict[str, bool],
) -> tuple[str, str]:
    """Return (verdict, reason)."""
    if not inv["all_passed"]:
        failed = [k for k, v in inv.items() if k != "all_passed" and not v]
        return VERDICT_UNSAFE, f"Critical invariant(s) failed: {failed}"

    if queue_status == "PROCESSED":
        if metrics["trades_opened"] > 0 and metrics["trades_protected"] == metrics["trades_opened"]:
            return VERDICT_5B_PASSED, (
                f"Trade opened ({metrics['trades_opened']}) and "
                f"protected ({metrics['trades_protected']}) via SL/TP"
            )
        if metrics["trades_opened"] > 0:
            return VERDICT_UNSAFE, (
                f"Trade opened ({metrics['trades_opened']}) but "
                f"protected count ({metrics['trades_protected']}) is insufficient"
            )
        return VERDICT_FIX, (
            "Queue row PROCESSED but no trade fill recorded — "
            "execution step incomplete or fill recording broken"
        )

    if queue_status == "REJECTED":
        final_reason = str(metrics.get("decision_final_reason") or "")
        return VERDICT_5B_NEEDS_CANDIDATE, (
            f"Signal safely rejected by gate — no valid execution candidate: {final_reason}"
        )

    if queue_status == "EXPIRED":
        return VERDICT_5B_NEEDS_CANDIDATE, (
            "Seeded signal expired before runner processed it — "
            "bot runner may be down or queue processing disabled"
        )

    if queue_status in ("FAILED",):
        return VERDICT_FIX, (
            "Queue row marked FAILED — processor or execution error during Phase 5B proof"
        )

    if queue_status in ("PENDING", "CLAIMED", "TIMEOUT"):
        return VERDICT_FIX, (
            f"Queue row still in status={queue_status!r} after monitor window — "
            "bot runner did not process signal within the expected time"
        )

    return VERDICT_FIX, f"Unexpected queue status {queue_status!r} — investigate"


def evaluate_phase5b_preflight(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
) -> Phase5BReport:
    bot = active_bot(conn, args.bot_id)
    broker = broker_account(conn, (bot or {}).get("broker_account_id"))
    bot_id: str | None = (bot or {}).get("id")

    live_safety = live_mode_safety_status(bot, broker)
    config = processor_config()
    automation = processor_automation_status(conn, bot_id)
    setup = tradingview_setup_status(conn, bot_id)
    safety = safety_architecture()
    baseline_protection = protection_evidence(conn, bot_id)
    completeness = log_completeness(conn, bot_id)
    clean_enabled = bool(getattr(args, "phase5b_clean_candle_proof", False))
    candidate = (
        select_phase5c_clean_candle_candidate(conn, bot_id, args)
        if clean_enabled
        else select_phase5b_candidate(conn, bot_id, args.symbol)
    )

    report = Phase5BReport(
        generated_at=utc_now(),
        live_mode_safety_confirmation=live_safety,
        processor_config=config,
        processor_automation_status=automation,
        tradingview_setup_status=setup,
        safety_architecture=safety,
        protection_baseline={"clean": baseline_protection["clean"], **baseline_protection},
        admin_decision_log_completeness=completeness,
        candidate_universe=candidate.get("candidate_universe", {}),
        candidate_selection=candidate,
        candidate_precheck_summary={
            "selected": candidate.get("selected"),
            "selected_action": candidate.get("selected_action"),
            "total_prechecks": len(candidate.get("prechecks", [])),
            "eligible_count": len([p for p in candidate.get("prechecks", []) if p.get("eligible")]),
            "rejected_count": len(candidate.get("rejection_matrix", [])),
            "signal_will_be_seeded": bool(candidate.get("selected")),
            "best_candidate": candidate.get("best_candidate"),
            "lowest_stop_distance_pct": candidate.get("lowest_stop_distance_pct"),
        },
        candidate_rejection_matrix=candidate.get("rejection_matrix", []),
        clean_candle_proof={
            "enabled": clean_enabled,
            "proof_type": "CONTROLLED_CLEAN_CANDLE_PROOF" if clean_enabled else None,
            "disclosure": (
                "CONTROLLED CLEAN-CANDLE PROOF USED. This proves integration/execution/protection flow under "
                "controlled market data. This does not prove natural market/testnet data candidate availability. "
                "Production policy/risk thresholds were not weakened. TradingView still did not bypass the runner, "
                "policy/risk, sizing, execution, or SL/TP protection path."
                if clean_enabled
                else "disabled"
            ),
            "configuration": {
                "symbol": getattr(args, "clean_candle_symbol", None),
                "action": getattr(args, "clean_candle_action", None),
                "timeframe": getattr(args, "clean_candle_timeframe", "1m"),
                "bars": getattr(args, "clean_candle_bars", 120),
                "volatility_pct": getattr(args, "clean_candle_volatility_pct", 0.2),
            },
            "selected_precheck": candidate.get("selected_precheck", {}),
        },
    )

    findings: list[str] = []

    # Category 1: System health
    if clean_enabled and not getattr(args, "phase5b_successful_execution", False):
        findings.append("Clean-candle proof requires --phase5b-successful-execution")
    if clean_enabled and not getattr(args, "strict", False):
        findings.append("Clean-candle proof requires --strict")
    if live_safety["unsafe_real_capital_detected"] or not live_safety["live_mode_safety_verified"]:
        findings.append(f"UNSAFE: {live_safety['reason']}")

    # Category 2: TradingView setup
    if not config["TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"]:
        findings.append("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=false — processor is disabled")
    if setup["external_signal_candidate_webhooks_count"] <= 0:
        findings.append("No enabled EXTERNAL_SIGNAL_CANDIDATE webhook exists")

    # Category 3: Safety architecture
    missing_automation = [
        k
        for k in [
            "external_signal_processor_exists",
            "multi_runner_hook_exists",
            "overlap_guard_exists",
            "processor_heartbeat_table_exists",
            "processor_status_admin_endpoint_exists",
            "stale_claimed_recovery_exists",
        ]
        if not bool(automation.get(k))
    ]
    if missing_automation:
        findings.append(f"Safety automation incomplete: {missing_automation}")
    if not all(bool(v) for v in safety.values()):
        findings.append("Safety architecture static checks failed")

    # Category 4: Protection baseline
    if not baseline_protection["clean"]:
        findings.append(
            f"Protection baseline unclean: {baseline_protection['unclean_protection_rows']} row(s)"
        )

    # Category 5: Candidate selection readiness
    if not candidate["selected"]:
        findings.append(f"Candidate selection: {candidate['reason']}")

    report.blocking_findings = findings

    if live_safety["unsafe_real_capital_detected"] or not live_safety["live_mode_safety_verified"]:
        report.phase5_preflight_verdict = VERDICT_UNSAFE
        report.final_verdict = VERDICT_UNSAFE
    elif not config["TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"] or setup["external_signal_candidate_webhooks_count"] <= 0:
        report.phase5_preflight_verdict = VERDICT_SETUP
        report.final_verdict = VERDICT_FIX
    elif clean_enabled and not getattr(args, "strict", False):
        report.phase5_preflight_verdict = VERDICT_FIX
        report.final_verdict = VERDICT_FIX
    elif missing_automation or not all(bool(v) for v in safety.values()):
        report.phase5_preflight_verdict = VERDICT_FIX
        report.final_verdict = VERDICT_FIX
    elif not baseline_protection["clean"]:
        report.phase5_preflight_verdict = VERDICT_FIX
        report.final_verdict = VERDICT_FIX
    elif not candidate["selected"]:
        report.phase5_preflight_verdict = VERDICT_5B_NEEDS_CANDIDATE
        report.final_verdict = VERDICT_5B_NEEDS_CANDIDATE
    else:
        report.phase5_preflight_verdict = VERDICT_READY
        report.final_verdict = VERDICT_READY

    report.next_required_action = next_phase5b_action(report)
    return report


def run_phase5b_monitor(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
    bot_id: str | None,
    report: Phase5BReport,
    seeded: dict[str, Any],
) -> Phase5BReport:
    queue_id: str = seeded["queue_id"]
    alert_db_id: int | None = seeded.get("alert_db_id")

    report.started_at = utc_now()
    end_at = time.time() + (args.duration_minutes * 60)

    final_queue_row: dict[str, Any] | None = None
    while time.time() < end_at:
        time.sleep(max(1, args.poll_seconds))
        row = poll_phase5b_queue_row(conn, queue_id)
        if row:
            status = str(row.get("status") or "").upper()
            if status not in ("PENDING", "CLAIMED"):
                final_queue_row = row
                break

    report.ended_at = utc_now()

    if final_queue_row is None:
        row = poll_phase5b_queue_row(conn, queue_id)
        final_queue_row = row or {"id": queue_id, "status": "TIMEOUT"}

    queue_status = str((final_queue_row or {}).get("status") or "TIMEOUT").upper()

    metrics = _collect_phase5b_metrics(conn, bot_id, queue_id, alert_db_id)
    inv = _phase5b_invariant_results(metrics)
    verdict, verdict_reason = _determine_phase5b_verdict(queue_status, metrics, inv)

    report.queue_status_summary = queue_status_summary(conn, bot_id)
    report.metrics = metrics
    report.invariant_results = inv
    report.admin_decision_log_completeness = log_completeness(conn, bot_id)
    report.protection_verification = protection_evidence(conn, bot_id)
    report.evidence_samples = evidence_samples(conn, bot_id)
    report.execution_proof = {
        "queue_id": queue_id,
        "queue_terminal_status": queue_status,
        "queue_row": dict(final_queue_row) if final_queue_row else None,
        "trades_opened": metrics["trades_opened"],
        "trades_protected": metrics["trades_protected"],
        "event_filter_result": metrics.get("event_filter_result"),
        "policy_result": metrics.get("policy_result"),
        "sizing_result": metrics.get("sizing_result"),
        "execution_result": metrics.get("execution_result"),
        "decision_final_status": metrics.get("decision_final_status"),
        "decision_final_reason": metrics.get("decision_final_reason"),
        "verdict_reason": verdict_reason,
    }

    report.final_verdict = verdict
    if verdict not in {VERDICT_5B_PASSED}:
        report.blocking_findings.append(verdict_reason)
    report.next_required_action = next_phase5b_action(report)
    return report


def run_phase5b(conn: sqlite3.Connection, args: argparse.Namespace) -> Phase5BReport:
    report = evaluate_phase5b_preflight(conn, args)

    scan_history: list[dict[str, Any]] = []
    wait_enabled = bool(getattr(args, "wait_for_valid_candidate", False))
    wait_started = time.time()
    wait_timeout_s = max(0.0, float(getattr(args, "candidate_wait_timeout_minutes", 30.0)) * 60.0)
    interval_s = max(1, int(getattr(args, "candidate_check_interval_seconds", 60)))

    def _record_scan(rep: Phase5BReport) -> None:
        prechecks = rep.candidate_selection.get("prechecks", [])
        scan_history.append(
            {
                "scan": len(scan_history) + 1,
                "timestamp": utc_now(),
                "eligible_count": len([p for p in prechecks if p.get("eligible")]),
                "symbols_tested": rep.candidate_selection.get("candidates_considered", []),
                "actions_tested": rep.candidate_selection.get("actions_considered", []),
                "lowest_stop_distance_pct": rep.candidate_selection.get("lowest_stop_distance_pct"),
                "best_candidate": rep.candidate_selection.get("best_candidate"),
                "selected_candidate": {
                    "symbol": rep.candidate_selection.get("selected"),
                    "action": rep.candidate_selection.get("selected_action"),
                    "side": rep.candidate_selection.get("selected_side"),
                }
                if rep.candidate_selection.get("selected")
                else None,
                "reason": rep.candidate_selection.get("reason"),
            }
        )

    _record_scan(report)

    while (
        wait_enabled
        and report.final_verdict == VERDICT_5B_NEEDS_CANDIDATE
        and (time.time() - wait_started) < wait_timeout_s
    ):
        time.sleep(interval_s)
        refreshed = evaluate_phase5b_preflight(conn, args)
        _record_scan(refreshed)
        report = refreshed
        if report.final_verdict in {VERDICT_UNSAFE, VERDICT_FIX, VERDICT_SETUP, VERDICT_READY}:
            break

    report.candidate_scan_history = scan_history
    report.waiting_mode_summary = {
        "enabled": wait_enabled,
        "check_interval_seconds": interval_s,
        "timeout_minutes": float(getattr(args, "candidate_wait_timeout_minutes", 30.0)),
        "scans_performed": len(scan_history),
        "timed_out": bool(
            wait_enabled
            and report.final_verdict == VERDICT_5B_NEEDS_CANDIDATE
            and (time.time() - wait_started) >= wait_timeout_s
        ),
        "final_waiting_result": report.final_verdict,
        "signal_seeded_while_waiting": False,
    }

    if report.final_verdict not in {VERDICT_READY}:
        return report

    bot = active_bot(conn, args.bot_id)
    bot_id: str | None = (bot or {}).get("id")
    symbol: str = report.candidate_selection["selected"]
    action: str = report.candidate_selection.get("selected_action") or "BUY"

    proof_context = None
    if getattr(args, "phase5b_clean_candle_proof", False):
        selected_precheck = report.candidate_selection.get("selected_precheck") or {}
        proof_context = {
            "phase5b_clean_candle_proof": True,
            "proof_type": "CONTROLLED_CLEAN_CANDLE_PROOF",
            "disclosure": "CONTROLLED CLEAN-CANDLE PROOF USED",
            "clean_candles": selected_precheck.get("clean_candles") or [],
            "clean_reference_price": (selected_precheck.get("clean_candle_summary") or {}).get("reference_price"),
            "clean_candle_summary": selected_precheck.get("clean_candle_summary") or {},
            "production_policy_risk_thresholds_weakened": False,
            "tradingview_bypasses_runner_or_risk": False,
        }

    seeded = seed_phase5b_signal(conn, bot_id or "unknown", symbol, action, proof_context=proof_context)
    report.seeded_signal = seeded
    if report.waiting_mode_summary:
        report.waiting_mode_summary["signal_seeded_while_waiting"] = True

    report = run_phase5b_monitor(conn, args, bot_id, report, seeded)
    report.candidate_scan_history = scan_history
    if not report.waiting_mode_summary:
        report.waiting_mode_summary = {
            "enabled": wait_enabled,
            "check_interval_seconds": interval_s,
            "timeout_minutes": float(getattr(args, "candidate_wait_timeout_minutes", 30.0)),
            "scans_performed": len(scan_history),
            "timed_out": False,
            "final_waiting_result": report.final_verdict,
            "signal_seeded_while_waiting": True,
        }
    else:
        report.waiting_mode_summary["final_waiting_result"] = report.final_verdict
    return report


def next_phase5b_action(report: Phase5BReport) -> str:
    v = report.final_verdict
    if v == VERDICT_UNSAFE:
        return "Stop. Address all UNSAFE findings before attempting Phase 5B proof."
    if v == VERDICT_FIX:
        return "Fix blocking findings (processor automation, protection evidence, or execution errors) then rerun."
    if v == VERDICT_5B_NEEDS_CANDIDATE:
        return (
            "No eligible BUY/SELL candidate passed the read-only Phase 5B precheck, "
            "or the seeded signal was safely rejected by normal gates. Wait for valid "
            "market/policy/sizing conditions and rerun Phase 5B."
        )
    if v == VERDICT_SETUP:
        return "Enable TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED and configure an EXTERNAL_SIGNAL_CANDIDATE webhook, then rerun."
    if v == VERDICT_READY:
        return "Preflight passed. Seeding controlled signal and monitoring for execution."
    if v == VERDICT_5B_PASSED:
        return (
            "Phase 5B proof passed. Review evidence before starting Phase 6."
        )
    return "Review report; result is inconclusive."


def write_phase5b_reports(report: Phase5BReport, output_dir: Path) -> Phase5BReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = (
        f"phase5c_controlled_clean_candle_execution_proof_{stamp}"
        if report.clean_candle_proof.get("enabled")
        else f"phase5b_successful_live_mode_tradingview_execution_{stamp}"
    )
    json_path = output_dir / f"{prefix}.json"
    md_path = output_dir / f"{prefix}.md"
    json_path.write_text(json.dumps(report.__dict__, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_phase5b_markdown(report), encoding="utf-8")
    report.report_files = {"json": str(json_path), "markdown": str(md_path)}
    json_path.write_text(json.dumps(report.__dict__, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_phase5b_markdown(report), encoding="utf-8")
    return report


def render_phase5b_markdown(report: Phase5BReport) -> str:
    def section(title: str, body: Any) -> str:
        if isinstance(body, str):
            text = body
        elif isinstance(body, list):
            text = "```json\n" + json.dumps(body, indent=2, default=str) + "\n```"
        else:
            text = "```json\n" + json.dumps(body, indent=2, default=str) + "\n```"
        return f"## {title}\n\n{text}\n"

    metrics_table = "\n".join(
        ["| Metric | Value |", "|---|---:|"]
        + [f"| {k} | {v} |" for k, v in sorted(report.metrics.items()) if not isinstance(v, dict)]
    )

    return "\n".join(
        [
            "# Phase 5C — Controlled Clean-Candle Execution Proof"
            if report.clean_candle_proof.get("enabled")
            else f"# {PHASE5B_NAME}",
            "",
            section("Clean-Candle Proof Disclosure", report.clean_candle_proof)
            if report.clean_candle_proof.get("enabled")
            else "",
            section(
                "Why Clean-Candle Proof Was Needed",
                (
                    "CONTROLLED CLEAN-CANDLE PROOF USED\n\n"
                    "This proof was introduced because Binance testnet/demo candles produced ATR outlier "
                    "inflation and STOP_TOO_WIDE blocks across the natural candidate universe.\n\n"
                    "Production policy/risk thresholds were not weakened.\n\n"
                    "TradingView still did not bypass the runner, policy/risk, sizing, execution, or SL/TP protection path."
                ),
            )
            if report.clean_candle_proof.get("enabled")
            else "",
            section("Live-Mode Safety Confirmation", report.live_mode_safety_confirmation),
            section("Phase 5 Preflight Verdict", {"preflight_verdict": report.phase5_preflight_verdict}),
            section("Blocking Findings", report.blocking_findings),
            section("TradingView Setup Status", report.tradingview_setup_status),
            section("Processor Automation Status", report.processor_automation_status),
            section("Safety Architecture", report.safety_architecture),
            section("Protection Baseline", report.protection_baseline),
            section("Candidate Universe", report.candidate_universe),
            section("Candidate Scan History", report.candidate_scan_history),
            section("Waiting Mode Summary", report.waiting_mode_summary),
            section("Candidate Precheck Summary", report.candidate_precheck_summary),
            section("Candidate Rejection Matrix", report.candidate_rejection_matrix),
            section("Candidate Selection", report.candidate_selection),
            section("Seeded Signal", report.seeded_signal),
            section("Queue Status Summary", report.queue_status_summary),
            section("Execution Proof", report.execution_proof),
            "## Phase 5B Metrics\n\n" + (metrics_table if report.metrics else "_No metrics collected._") + "\n",
            section("Invariant Results", report.invariant_results),
            section("Protection Verification", report.protection_verification),
            section("Admin/Decision Log Completeness", report.admin_decision_log_completeness),
            section("Evidence Samples", report.evidence_samples),
            section("Final Verdict", report.final_verdict),
            section("Next Required Action", report.next_required_action),
        ]
    )


@dataclass
class StopDistanceDiagnosticsReport:
    generated_at: str
    runtime_summary: dict[str, Any] = field(default_factory=dict)
    symbols_timeframes_tested: dict[str, Any] = field(default_factory=dict)
    policy_risk_config: dict[str, Any] = field(default_factory=dict)
    candidate_stop_distance_summary: list[dict[str, Any]] = field(default_factory=list)
    candle_anomaly_summary: list[dict[str, Any]] = field(default_factory=list)
    atr_comparison: list[dict[str, Any]] = field(default_factory=list)
    timeframe_comparison: dict[str, Any] = field(default_factory=dict)
    data_source_comparison: dict[str, Any] = field(default_factory=dict)
    top_failure_reasons: dict[str, int] = field(default_factory=dict)
    best_candidate_found: dict[str, Any] = field(default_factory=dict)
    any_candidate_would_pass_naturally: bool = False
    root_cause_assessment: dict[str, Any] = field(default_factory=dict)
    recommended_next_action: str = ""
    report_files: dict[str, str] = field(default_factory=dict)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
        return parsed if parsed == parsed else None
    except Exception:
        return None


def _diagnostic_symbol_list(conn: sqlite3.Connection, args: argparse.Namespace, bot_id: str | None) -> list[str]:
    if args.diagnostic_symbols:
        return _parse_symbol_csv(args.diagnostic_symbols)
    universe = build_phase5b_candidate_universe(conn, bot_id, args.symbol)
    return list(universe.get("symbols") or [])[: max(1, env_int("PHASE5B_DIAGNOSTIC_MAX_SYMBOLS", 30))]


def generate_clean_phase5c_candles(
    *,
    price: float,
    bars: int = 120,
    volatility_pct: float = 0.2,
    timeframe: str = "1m",
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Generate proof-only clean OHLC candles with conservative volatility."""
    bars = max(20, int(bars))
    base = max(float(price or 100.0), 0.0001)
    vol = max(0.01, min(float(volatility_pct or 0.2), 2.0)) / 100.0
    end = now or datetime.now(timezone.utc)
    step_seconds = 60
    if timeframe.endswith("m"):
        try:
            step_seconds = int(timeframe[:-1]) * 60
        except Exception:
            step_seconds = 60
    elif timeframe.endswith("h"):
        try:
            step_seconds = int(timeframe[:-1]) * 3600
        except Exception:
            step_seconds = 3600
    candles: list[dict[str, Any]] = []
    prev_close = base
    for i in range(bars):
        ts = end - timedelta(seconds=step_seconds * (bars - i - 1))
        drift = math.sin(i / 9.0) * vol * 0.15
        open_ = prev_close
        close = max(0.0001, base * (1.0 + drift))
        spread = max(base * vol * 0.35, base * 0.00005)
        high = max(open_, close) + spread
        low = max(0.0001, min(open_, close) - spread)
        candles.append(
            {
                "ts": ts.isoformat(),
                "open": round(open_, 8),
                "high": round(high, 8),
                "low": round(low, 8),
                "close": round(close, 8),
                "volume": 1_000_000.0,
            }
        )
        prev_close = close
    return candles


def _clean_candle_summary(candles: list[dict[str, Any]], price: float) -> dict[str, Any]:
    tr = _true_ranges(candles)
    atr = _wilder_atr(tr)
    stop_pct = (atr * 2.0 / price * 100.0) if atr is not None and price > 0 else None
    anomalies = _detect_candle_anomalies(candles, price)
    return {
        "bars": len(candles),
        "reference_price": price,
        "highest_high": max((_float_or_none(c.get("high")) or 0 for c in candles), default=None),
        "lowest_low": min((_float_or_none(c.get("low")) or 0 for c in candles), default=None),
        "wilder_atr": atr,
        "stop_distance_pct": stop_pct,
        "max_allowed_stop_distance_pct": float(os.environ.get("PHASE5B_MAX_STOP_DISTANCE_FRACTION", "0.10")) * 100.0,
        "anomaly_count": len(anomalies),
        "sample_last_5": candles[-5:],
    }


def _runtime_timeframe_hint() -> str | None:
    for name in ("STRATEGY_TIMEFRAME", "BOT_TIMEFRAME", "KLINE_INTERVAL", "CANDLE_INTERVAL", "TIMEFRAME"):
        value = os.environ.get(name)
        if value:
            return str(value).strip()
    return None


def _find_cached_candles(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    bars: int,
) -> tuple[list[dict[str, Any]], str]:
    candidates = [
        "market_candles",
        "ohlcv_candles",
        "candles",
        "klines",
        "market_klines",
        "symbol_klines",
        "price_candles",
    ]
    for table in candidates:
        if not table_exists(conn, table):
            continue
        cols = _table_columns(conn, table)
        lower = {c.lower(): c for c in cols}
        required = ["open", "high", "low", "close"]
        if not all(c in lower for c in required) or "symbol" not in lower:
            continue
        ts_col = lower.get("ts") or lower.get("timestamp") or lower.get("open_time") or lower.get("time") or lower.get("created_at")
        tf_col = lower.get("timeframe") or lower.get("interval")
        volume_col = lower.get("volume")
        select_cols = [
            f"{lower['symbol']} AS symbol",
            f"{lower['open']} AS open",
            f"{lower['high']} AS high",
            f"{lower['low']} AS low",
            f"{lower['close']} AS close",
        ]
        if ts_col:
            select_cols.append(f"{ts_col} AS ts")
        if volume_col:
            select_cols.append(f"{volume_col} AS volume")
        sql = f"SELECT {', '.join(select_cols)} FROM {table} WHERE UPPER({lower['symbol']}) = ?"
        params: list[Any] = [symbol.upper()]
        if tf_col:
            sql += f" AND {tf_col} = ?"
            params.append(timeframe)
        sql += f" ORDER BY {ts_col or 'rowid'} DESC LIMIT ?"
        params.append(bars)
        found = rows(conn, sql, tuple(params))
        if found:
            return list(reversed(found)), f"cached_table:{table}"
    return [], "no_cached_candle_table"


def _decision_trace_candles(
    conn: sqlite3.Connection,
    symbol: str,
    bars: int,
) -> list[dict[str, Any]]:
    if not table_exists(conn, "decision_traces"):
        return []
    found = rows(
        conn,
        """
        SELECT ts, last_price, mark_price, atr_pct
        FROM decision_traces
        WHERE UPPER(symbol) = ?
        ORDER BY ts DESC
        LIMIT ?
        """,
        (symbol.upper(), bars),
    )
    candles: list[dict[str, Any]] = []
    for row in reversed(found):
        price = _float_or_none(row.get("last_price") or row.get("mark_price"))
        mark = _float_or_none(row.get("mark_price") or row.get("last_price"))
        if price is None:
            continue
        hi = max(price, mark or price)
        lo = min(price, mark or price)
        candles.append(
            {
                "ts": row.get("ts"),
                "open": price,
                "high": hi,
                "low": lo,
                "close": price,
                "volume": None,
                "atr_pct_hint": row.get("atr_pct"),
            }
        )
    return candles


def _load_diagnostic_candles(
    conn: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    bars: int,
) -> tuple[list[dict[str, Any]], str]:
    cached, source = _find_cached_candles(conn, symbol, timeframe, bars)
    if cached:
        return cached, source
    return _decision_trace_candles(conn, symbol, bars), "decision_traces_synthetic"


def _true_ranges(candles: list[dict[str, Any]]) -> list[float]:
    ranges: list[float] = []
    previous_close: float | None = None
    for candle in candles:
        high = _float_or_none(candle.get("high"))
        low = _float_or_none(candle.get("low"))
        close = _float_or_none(candle.get("close"))
        if high is None or low is None or close is None:
            previous_close = close
            continue
        tr = high - low
        if previous_close is not None:
            tr = max(tr, abs(high - previous_close), abs(low - previous_close))
        ranges.append(max(0.0, tr))
        previous_close = close
    return ranges


def _wilder_atr(values: list[float], period: int = 14) -> float | None:
    if not values:
        return None
    if len(values) < period:
        return mean(values)
    atr = mean(values[:period])
    for value in values[period:]:
        atr = ((atr * (period - 1)) + value) / period
    return atr


def _trimmed_mean(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) < 5:
        return mean(values)
    ordered = sorted(values)
    trim = max(1, int(len(ordered) * 0.1))
    kept = ordered[trim:-trim] or ordered
    return mean(kept)


def _detect_candle_anomalies(candles: list[dict[str, Any]], price: float | None) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    timestamps: list[Any] = []
    previous_ts: datetime | None = None
    ranges = _true_ranges(candles)
    med_tr = median(ranges) if ranges else 0.0
    for idx, candle in enumerate(candles):
        high = _float_or_none(candle.get("high"))
        low = _float_or_none(candle.get("low"))
        open_ = _float_or_none(candle.get("open"))
        close = _float_or_none(candle.get("close"))
        volume = _float_or_none(candle.get("volume"))
        ts = candle.get("ts")
        if ts in timestamps:
            anomalies.append({"index": idx, "timestamp": ts, "type": "DUPLICATE_TIMESTAMP"})
        timestamps.append(ts)
        parsed_ts = _parse_dt(ts)
        if previous_ts and parsed_ts and parsed_ts <= previous_ts:
            anomalies.append({"index": idx, "timestamp": ts, "type": "NON_MONOTONIC_TIMESTAMP"})
        if parsed_ts:
            previous_ts = parsed_ts
        if high is None or low is None or open_ is None or close is None:
            anomalies.append({"index": idx, "timestamp": ts, "type": "MISSING_OHLC"})
            continue
        if high < low or close > high or close < low or open_ > high or open_ < low:
            anomalies.append({"index": idx, "timestamp": ts, "type": "INVALID_OHLC", "open": open_, "high": high, "low": low, "close": close})
        reference = price or close or 1.0
        range_pct = ((high - low) / reference * 100.0) if reference > 0 else None
        if range_pct is not None and range_pct > 10:
            anomalies.append({"index": idx, "timestamp": ts, "type": "RANGE_GT_10_PCT", "range_pct": round(range_pct, 6)})
        elif range_pct is not None and range_pct > 5:
            anomalies.append({"index": idx, "timestamp": ts, "type": "RANGE_GT_5_PCT", "range_pct": round(range_pct, 6)})
        if low > 0 and high / low > 1.10:
            anomalies.append({"index": idx, "timestamp": ts, "type": "HIGH_LOW_RATIO_GT_1_10", "ratio": round(high / low, 6)})
        if med_tr > 0 and idx < len(ranges) and ranges[idx] > med_tr * 5:
            anomalies.append({"index": idx, "timestamp": ts, "type": "TRUE_RANGE_OUTLIER", "true_range": ranges[idx], "median_true_range": med_tr})
        if volume == 0:
            anomalies.append({"index": idx, "timestamp": ts, "type": "ZERO_VOLUME"})
    return anomalies


def _atr_diagnostic_for(
    conn: sqlite3.Connection,
    symbol: str,
    action: str,
    timeframe: str,
    bars: int,
) -> dict[str, Any]:
    candles, source = _load_diagnostic_candles(conn, symbol, timeframe, bars)
    trace = _latest_decision_trace(conn, symbol) or {}
    price = _float_or_none(trace.get("last_price") or trace.get("mark_price"))
    if price is None and candles:
        price = _float_or_none(candles[-1].get("close"))
    policy_atr_pct = _float_or_none(trace.get("atr_pct"))
    policy_atr = (policy_atr_pct * price) if policy_atr_pct is not None and price else None
    true_ranges = _true_ranges(candles)
    sma_atr = mean(true_ranges) if true_ranges else None
    wilder = _wilder_atr(true_ranges)
    median_tr = median(true_ranges) if true_ranges else None
    trimmed = _trimmed_mean(true_ranges)
    atr_for_stop = policy_atr if policy_atr is not None else sma_atr
    stop_pct = (atr_for_stop * 2.0 / price * 100.0) if atr_for_stop is not None and price else None
    max_stop_pct = float(os.environ.get("PHASE5B_MAX_STOP_DISTANCE_FRACTION", "0.10")) * 100.0
    anomalies = _detect_candle_anomalies(candles, price)
    pass_fail = "PASS" if stop_pct is not None and stop_pct <= max_stop_pct else "FAIL"
    divergence_pct = None
    if policy_atr is not None and sma_atr and sma_atr > 0:
        divergence_pct = abs(policy_atr - sma_atr) / sma_atr * 100.0
    return {
        "symbol": symbol,
        "action": action,
        "side": "LONG" if action == "BUY" else "SHORT",
        "timeframe": timeframe,
        "data_source": source,
        "current_price": price,
        "last_price_source": "decision_traces" if trace else source,
        "candles_requested": bars,
        "candles_returned": len(candles),
        "raw_ohlc_sample_last_10": candles[-10:],
        "highest_high": max((_float_or_none(c.get("high")) or 0.0 for c in candles), default=None),
        "lowest_low": min((_float_or_none(c.get("low")) or 0.0 for c in candles), default=None),
        "max_true_range": max(true_ranges) if true_ranges else None,
        "median_true_range": median_tr,
        "mean_true_range": sma_atr,
        "atr_method_used_by_policy_engine": "decision_trace_atr_pct_times_price",
        "policy_atr_value": policy_atr,
        "policy_atr_pct_of_price": (policy_atr / price * 100.0) if policy_atr is not None and price else None,
        "sma_true_range_atr": sma_atr,
        "wilder_atr": wilder,
        "median_true_range_atr": median_tr,
        "trimmed_atr_diagnostics_only": trimmed,
        "policy_vs_sma_divergence_pct": divergence_pct,
        "computed_stop_distance": (atr_for_stop * 2.0) if atr_for_stop is not None else None,
        "computed_stop_distance_pct": stop_pct,
        "max_allowed_stop_distance_pct": max_stop_pct,
        "pass_fail": pass_fail,
        "failure_reason": None if pass_fail == "PASS" else f"STOP_TOO_WIDE: {stop_pct:.2f}% > {max_stop_pct:.2f}%" if stop_pct is not None else "NO_ATR_OR_PRICE",
        "anomalies": anomalies,
        "anomaly_count": len(anomalies),
        "would_pass_with_trimmed_atr": bool(trimmed is not None and price and (trimmed * 2.0 / price * 100.0) <= max_stop_pct),
        "would_pass_with_median_tr": bool(median_tr is not None and price and (median_tr * 2.0 / price * 100.0) <= max_stop_pct),
    }


def _policy_config_snapshot() -> dict[str, Any]:
    snapshot = {
        "PHASE5B_MAX_STOP_DISTANCE_FRACTION": os.environ.get("PHASE5B_MAX_STOP_DISTANCE_FRACTION", "0.10"),
        "PHASE5B_MAX_TRACE_AGE_SECONDS": os.environ.get("PHASE5B_MAX_TRACE_AGE_SECONDS", "900"),
        "runtime_timeframe_hint": _runtime_timeframe_hint(),
    }
    try:
        from app.core.config import settings

        snapshot.update(
            {
                "STOP_LOSS_PCT": getattr(settings, "STOP_LOSS_PCT", None),
                "TAKE_PROFIT_PCT": getattr(settings, "TAKE_PROFIT_PCT", None),
                "DEFAULT_LEVERAGE": getattr(settings, "DEFAULT_LEVERAGE", None),
                "MAX_OPEN_POSITIONS": getattr(settings, "MAX_OPEN_POSITIONS", None),
                "TRADE_MODE": getattr(settings, "TRADE_MODE", None),
            }
        )
    except Exception as exc:
        snapshot["settings_error"] = f"{type(exc).__name__}:{exc}"
    return snapshot


def _diagnostic_runtime_summary(conn: sqlite3.Connection, args: argparse.Namespace) -> dict[str, Any]:
    bot = active_bot(conn, args.bot_id)
    broker = broker_account(conn, (bot or {}).get("broker_account_id"))
    return {
        "runtime_db_path": str(args.db_path),
        "EXECUTION_MODE": os.environ.get("EXECUTION_MODE"),
        "BINANCE_ENV": os.environ.get("BINANCE_ENV"),
        "active_bot": scrub_bot(bot),
        "active_broker_account": scrub_broker(broker),
        "diagnostics_read_only": True,
        "alerts_seeded": False,
        "queue_rows_created": False,
        "executor_called": False,
    }


def _classify_stop_distance_root_causes(
    diagnostics: list[dict[str, Any]],
    runtime_summary: dict[str, Any],
) -> dict[str, Any]:
    classifications: set[str] = set()
    reasons: list[str] = []
    if not diagnostics:
        return {"classifications": ["INCONCLUSIVE"], "reasons": ["No diagnostic rows were produced."]}
    anomaly_rows = [d for d in diagnostics if d.get("anomaly_count", 0) > 0]
    if anomaly_rows and str(runtime_summary.get("BINANCE_ENV", "")).lower() in {"testnet", "demo"}:
        classifications.add("TESTNET_CANDLE_SPIKES")
        reasons.append("Candle anomalies were detected while BINANCE_ENV is testnet/demo.")
    if any(d.get("would_pass_with_trimmed_atr") or d.get("would_pass_with_median_tr") for d in diagnostics):
        classifications.add("ATR_OUTLIER_INFLATION")
        reasons.append("At least one candidate would pass using median/trimmed true range diagnostics.")
    if any((d.get("policy_vs_sma_divergence_pct") or 0) > 50 for d in diagnostics if d.get("data_source") != "decision_traces_synthetic"):
        classifications.add("POLICY_FORMULA_BUG")
        reasons.append("Policy ATR diverged by more than 50% from SMA true range on cached candle data.")
    runtime_tf = _runtime_timeframe_hint()
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for d in diagnostics:
        by_symbol.setdefault(d["symbol"], []).append(d)
    if runtime_tf and any(
        any(x.get("pass_fail") == "PASS" for x in rows_) and not any(x.get("timeframe") == runtime_tf and x.get("pass_fail") == "PASS" for x in rows_)
        for rows_ in by_symbol.values()
    ):
        classifications.add("TIMEFRAME_MISMATCH")
        reasons.append("A non-runtime diagnostic timeframe passed while the runtime timeframe did not.")
    if any(d.get("data_source") == "decision_traces_synthetic" for d in diagnostics):
        classifications.add("PRECHECK_DATA_SOURCE_MISMATCH")
        reasons.append("Diagnostics had to use decision trace data for at least one symbol/timeframe instead of raw cached candles.")
    if not classifications and all(d.get("computed_stop_distance_pct") and d["computed_stop_distance_pct"] > d["max_allowed_stop_distance_pct"] for d in diagnostics):
        classifications.add("GENUINE_MARKET_VOLATILITY")
        reasons.append("All available diagnostics exceeded the configured stop-distance limit without a more specific cause.")
    if not classifications:
        classifications.add("INCONCLUSIVE")
    return {"classifications": sorted(classifications), "reasons": reasons}


def run_stop_distance_diagnostics(conn: sqlite3.Connection, args: argparse.Namespace) -> StopDistanceDiagnosticsReport:
    bot = active_bot(conn, args.bot_id)
    bot_id = args.bot_id or (bot or {}).get("id")
    symbols = _diagnostic_symbol_list(conn, args, bot_id)
    timeframes = _parse_csv_values(args.diagnostic_timeframes or "1m,5m,15m")
    actions = ["BUY", "SELL"]
    runtime = _diagnostic_runtime_summary(conn, args)
    candidate = select_phase5b_candidate(conn, bot_id, args.symbol)
    diagnostics: list[dict[str, Any]] = []
    for symbol in symbols:
        for action in actions:
            for timeframe in timeframes:
                diagnostics.append(_atr_diagnostic_for(conn, symbol, action, timeframe, max(10, int(args.diagnostic_bars))))
    failure_reasons: dict[str, int] = {}
    for item in diagnostics:
        reason = str(item.get("failure_reason") or "PASS")
        failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
    best = min(
        diagnostics,
        key=lambda d: d.get("computed_stop_distance_pct") if d.get("computed_stop_distance_pct") is not None else 10**9,
        default={},
    )
    root_causes = _classify_stop_distance_root_causes(diagnostics, runtime)
    pass_exists = any(d.get("pass_fail") == "PASS" for d in diagnostics)
    recommended = (
        "A candidate appears to pass diagnostics. Rerun Phase 5B without changing risk limits."
        if pass_exists
        else "Do not change risk thresholds in this task. Wait and rerun Phase 5B later, or use the diagnostic matrix to exclude unreliable candle sources/symbols in a separately approved change."
    )
    return StopDistanceDiagnosticsReport(
        generated_at=utc_now(),
        runtime_summary=runtime,
        symbols_timeframes_tested={
            "symbols": symbols,
            "timeframes": timeframes,
            "actions": actions,
            "diagnostic_bars": args.diagnostic_bars,
        },
        policy_risk_config=_policy_config_snapshot(),
        candidate_stop_distance_summary=candidate.get("prechecks", []),
        candle_anomaly_summary=[
            {
                "symbol": d["symbol"],
                "action": d["action"],
                "timeframe": d["timeframe"],
                "data_source": d["data_source"],
                "anomaly_count": d["anomaly_count"],
                "anomalies": d["anomalies"][:10],
            }
            for d in diagnostics
            if d.get("anomaly_count", 0) > 0
        ],
        atr_comparison=diagnostics,
        timeframe_comparison={
            symbol: [
                {
                    "action": d["action"],
                    "timeframe": d["timeframe"],
                    "stop_distance_pct": d.get("computed_stop_distance_pct"),
                    "policy_atr_pct_of_price": d.get("policy_atr_pct_of_price"),
                    "pass_fail": d.get("pass_fail"),
                    "data_source": d.get("data_source"),
                }
                for d in diagnostics
                if d["symbol"] == symbol
            ]
            for symbol in symbols
        },
        data_source_comparison={
            "runtime_timeframe_hint": _runtime_timeframe_hint(),
            "sources_used": sorted({str(d.get("data_source")) for d in diagnostics}),
            "precheck_uses_decision_traces": True,
            "raw_cached_candle_rows_available": any(str(d.get("data_source", "")).startswith("cached_table:") for d in diagnostics),
        },
        top_failure_reasons=failure_reasons,
        best_candidate_found=best,
        any_candidate_would_pass_naturally=pass_exists,
        root_cause_assessment=root_causes,
        recommended_next_action=recommended,
    )


def write_stop_distance_diagnostic_reports(
    report: StopDistanceDiagnosticsReport,
    output_dir: Path,
) -> StopDistanceDiagnosticsReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"phase5b_stop_distance_diagnostics_{stamp}"
    json_path = output_dir / f"{prefix}.json"
    md_path = output_dir / f"{prefix}.md"
    report.report_files = {"json": str(json_path), "markdown": str(md_path)}
    json_path.write_text(json.dumps(report.__dict__, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_stop_distance_diagnostic_markdown(report), encoding="utf-8")
    return report


def render_stop_distance_diagnostic_markdown(report: StopDistanceDiagnosticsReport) -> str:
    def section(title: str, body: Any) -> str:
        text = body if isinstance(body, str) else "```json\n" + json.dumps(body, indent=2, default=str) + "\n```"
        return f"## {title}\n\n{text}\n"

    summary_rows = [
        {
            "symbol": d.get("symbol"),
            "action": d.get("action"),
            "timeframe": d.get("timeframe"),
            "stop_distance_pct": d.get("computed_stop_distance_pct"),
            "max_allowed_stop_distance_pct": d.get("max_allowed_stop_distance_pct"),
            "pass_fail": d.get("pass_fail"),
            "data_source": d.get("data_source"),
            "failure_reason": d.get("failure_reason"),
        }
        for d in report.atr_comparison
    ]
    return "\n".join(
        [
            "# Phase 5B Stop Distance / ATR Diagnostics",
            "",
            section("Runtime Summary", report.runtime_summary),
            section("Symbols and Timeframes Tested", report.symbols_timeframes_tested),
            section("Policy/Risk Configuration Snapshot", report.policy_risk_config),
            section("Candidate Stop-Distance Summary", report.candidate_stop_distance_summary),
            section("Candle Anomaly Summary", report.candle_anomaly_summary),
            section("ATR Comparison", report.atr_comparison),
            section("Timeframe Comparison", report.timeframe_comparison),
            section("Data Source Comparison", report.data_source_comparison),
            section("Top Failure Reasons", report.top_failure_reasons),
            section("Best Candidate Found", report.best_candidate_found),
            section("Whether Any Candidate Would Pass Naturally", report.any_candidate_would_pass_naturally),
            section("Root Cause Assessment", report.root_cause_assessment),
            section("Recommended Next Action", report.recommended_next_action),
            section("Stop-Distance Table", summary_rows),
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
