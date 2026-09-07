#!/usr/bin/env python3
"""Generate the daily Section 4 paper-validation monitoring closure pack."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.request import urlopen

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from app.core.config import settings
from scripts.ml.watch_training_readiness import evaluate_training_readiness
from scripts.validation.monitor_strong_trend_experiment import build_report as build_strong_report
from scripts.validation.monitor_strong_trend_experiment import parse_env
from scripts.validation.replay_strategy_components import resolve_db_path
from scripts.validation.run_paper_cycle_diagnostic import classify_block_reason


logger = logging.getLogger(__name__)

DEFAULT_HEALTH_URL = "http://127.0.0.1:9000/health"
DEFAULT_ACTIVE_ENV = _BOT_ROOT / ".env"
DEFAULT_EXPERIMENT_CONFIG = _BOT_ROOT / ".env.paper_strong_trend_experiment"
DEFAULT_SECTION4_STATUS = _BOT_ROOT / "models/reports/iofs_paper_validation_status.md"
DEFAULT_COMPONENT_REPORT = _BOT_ROOT / "models/reports/post_restart_component_diagnostic.json"
DEFAULT_OUTPUT_JSON = _BOT_ROOT / "models/reports/daily_paper_validation_status.json"
DEFAULT_OUTPUT_MD = _BOT_ROOT / "models/reports/daily_paper_validation_status.md"
DEFAULT_PRODUCTION_DIR = _BOT_ROOT / "models/production"

SYMBOLS = ("BTCUSDT", "ETHUSDT")
LATEST_TRACE_LIMIT = 100
SMOKE_BOT_INSTANCE_IDS = {"paper_smoke"}
SNAPSHOT_HEADER = "## Latest Daily Monitor Snapshot"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest().upper()
    except OSError:
        return None


def _list_files(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        return sorted(item.name for item in path.iterdir() if item.is_file())
    except OSError:
        return []


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _clean_start_from_status(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Section 4 status not found: {path}")
    text = path.read_text(encoding="utf-8")
    match = re.search(
        r"^-\s+(?:start_timestamp_utc|post_repair_restart_time):\s*(\S+)",
        text,
        flags=re.MULTILINE,
    )
    if not match:
        raise ValueError(f"No clean validation start timestamp found in {path}")
    return match.group(1)


def _fetch_health(url: str, timeout: float = 10.0) -> dict[str, Any]:
    with urlopen(url, timeout=timeout) as response:  # nosec B310 - configured local health URL
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("health endpoint did not return an object")
    return payload


def _rows(connection: sqlite3.Connection, query: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(query, parameters).fetchall()]


def _one(connection: sqlite3.Connection, query: str, parameters: tuple[Any, ...] = ()) -> dict[str, Any]:
    row = connection.execute(query, parameters).fetchone()
    return dict(row) if row else {}


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return bool(
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
    )


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()


def _top_counts(values: list[str], limit: int = 8) -> dict[str, int]:
    return dict(Counter(value for value in values if value).most_common(limit))


def _exclude_smoke_clause(column_names: set[str]) -> tuple[str, tuple[Any, ...]]:
    if "bot_instance_id" not in column_names:
        return "", ()
    placeholders = ",".join("?" for _ in SMOKE_BOT_INSTANCE_IDS)
    return (
        f" AND COALESCE(bot_instance_id, '') NOT IN ({placeholders})",
        tuple(sorted(SMOKE_BOT_INSTANCE_IDS)),
    )


def _latest_component_failures(path: Path) -> dict[str, int]:
    report = _load_json(path)
    summary = report.get("summary") or {}
    failures = summary.get("top_failed_conditions")
    if isinstance(failures, dict):
        return {str(key): int(value) for key, value in list(failures.items())[:15]}
    aggregate: Counter[str] = Counter()
    for symbol in report.get("symbols") or []:
        aggregate.update(symbol.get("top_failed_conditions") or {})
    return dict(aggregate.most_common(15))


def _collect_database_status(
    db_path: Path,
    clean_start: str,
    *,
    generated: datetime,
) -> dict[str, Any]:
    today = generated.date().isoformat()
    placeholders = ",".join("?" for _ in SYMBOLS)
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        decision_columns = _table_columns(connection, "decision_traces")
        fill_columns = _table_columns(connection, "trade_fills")

        active_bot = _one(
            connection,
            """
            SELECT id, status, mode, last_run_at, last_error, active_positions
            FROM bot_instances
            WHERE status='active'
            ORDER BY last_run_at DESC
            LIMIT 1
            """,
        )
        active_bot_id = active_bot.get("id")
        decision_bot_clause, decision_bot_params = _exclude_smoke_clause(decision_columns)
        if active_bot_id and "bot_instance_id" in decision_columns:
            decision_bot_clause += " AND (bot_instance_id=? OR bot_instance_id IS NULL OR bot_instance_id='')"
            decision_bot_params = (*decision_bot_params, active_bot_id)
        fill_bot_clause, fill_bot_params = _exclude_smoke_clause(fill_columns)
        if active_bot_id and "bot_instance_id" in fill_columns:
            fill_bot_clause += " AND (bot_instance_id=? OR bot_instance_id IS NULL OR bot_instance_id='')"
            fill_bot_params = (*fill_bot_params, active_bot_id)

        traces = _rows(
            connection,
            f"""
            SELECT trace_id, cycle_id, symbol, ts, regime_state, signal, intended_action,
                   reason_codes, gate_reason, execution_status, execution_error,
                   submit_attempted, rejection_reason, event_block_reason,
                   open_positions_count
            FROM decision_traces
            WHERE symbol IN ({placeholders})
              {decision_bot_clause}
            ORDER BY ts DESC
            LIMIT ?
            """,
            (*SYMBOLS, *decision_bot_params, LATEST_TRACE_LIMIT),
        )
        fill_summary = _one(
            connection,
            f"""
            SELECT
              SUM(CASE WHEN date(timestamp_utc)=? THEN 1 ELSE 0 END) AS fills_today,
              SUM(CASE WHEN UPPER(action)='CLOSE' THEN 1 ELSE 0 END) AS closed_total,
              SUM(CASE WHEN UPPER(action)='CLOSE' AND timestamp_utc>=? THEN 1 ELSE 0 END)
                AS closed_since_start,
              SUM(CASE WHEN UPPER(action)='CLOSE' AND date(timestamp_utc)=? THEN 1 ELSE 0 END)
                AS closed_today,
              MAX(timestamp_utc) AS last_fill_time,
              MAX(CASE WHEN UPPER(action)='CLOSE' THEN timestamp_utc END) AS last_closed_trade_time
            FROM trade_fills
            WHERE symbol IN ({placeholders})
              {fill_bot_clause}
            """,
            (today, clean_start, today, *SYMBOLS, *fill_bot_params),
        )
        order_summary = _one(
            connection,
            f"""
            SELECT
              SUM(CASE WHEN submit_attempted=1 AND date(ts)=? THEN 1 ELSE 0 END)
                AS paper_orders_today,
              MAX(CASE WHEN submit_attempted=1 THEN ts END) AS last_order_time
            FROM decision_traces
            WHERE symbol IN ({placeholders})
              {decision_bot_clause}
            """,
            (today, *SYMBOLS, *decision_bot_params),
        )
        active_positions = 0
        if _table_exists(connection, "position_lifecycle_state"):
            position_columns = _table_columns(connection, "position_lifecycle_state")
            position_bot_clause, position_params = _exclude_smoke_clause(position_columns)
            if active_bot_id and "bot_instance_id" in position_columns:
                position_bot_clause += " AND (bot_instance_id=? OR bot_instance_id IS NULL OR bot_instance_id='')"
                position_params = (*position_params, active_bot_id)
            position_row = _one(
                connection,
                f"""
                SELECT COUNT(*) AS count
                FROM position_lifecycle_state
                WHERE (
                    UPPER(COALESCE(phase, 'FLAT')) != 'FLAT'
                    OR COALESCE(exchange_position_active, 0) = 1
                )
                  {position_bot_clause}
                """,
                position_params,
            )
            active_positions = int(position_row.get("count") or 0)

        daily_columns = _table_columns(connection, "bot_daily_state")
        daily_bot_clause = ""
        daily_params: tuple[Any, ...] = (today,)
        if active_bot_id and "bot_instance_id" in daily_columns:
            daily_bot_clause = " AND (bot_instance_id=? OR bot_instance_id IS NULL OR bot_instance_id='')"
            daily_params = (today, active_bot_id)
        daily = _one(
            connection,
            f"""
            SELECT *
            FROM bot_daily_state
            WHERE day=?
              {daily_bot_clause}
            ORDER BY last_updated_at DESC
            LIMIT 1
            """,
            daily_params,
        )
        crash_row = _one(
            connection,
            """
            SELECT COUNT(*) AS count
            FROM events
            WHERE timestamp_utc>=?
              AND (
                UPPER(event_type) IN ('ERROR', 'CRITICAL')
                OR UPPER(action) LIKE '%CRASH%'
                OR UPPER(action) LIKE '%FATAL%'
                OR UPPER(action) LIKE '%CYCLE_STEP_ERROR%'
              )
            """,
            (clean_start,),
        )
        circuit_row = _one(
            connection,
            f"""
            SELECT COUNT(*) AS count
            FROM decision_traces
            WHERE symbol IN ({placeholders})
              AND ts>=?
              {decision_bot_clause}
              AND (
                UPPER(COALESCE(reason_codes, '')) LIKE '%CIRCUIT%'
                OR UPPER(COALESCE(gate_reason, '')) LIKE '%CIRCUIT%'
              )
            """,
            (*SYMBOLS, clean_start, *decision_bot_params),
        )

    block_labels = [classify_block_reason(trace) for trace in traces]
    hold_labels = [
        classify_block_reason(trace)
        for trace in traces
        if str(trace.get("intended_action") or trace.get("signal") or "").upper()
        in {"", "HOLD", "NONE", "SKIP"}
    ]
    regimes = [str(trace.get("regime_state") or "UNKNOWN") for trace in traces]
    return {
        "paper_orders_today": int(order_summary.get("paper_orders_today") or 0),
        "paper_fills_today": int(fill_summary.get("fills_today") or 0),
        "active_positions": active_positions,
        "closed_paper_trades_total": int(fill_summary.get("closed_total") or 0),
        "closed_paper_trades_since_clean_start": int(
            fill_summary.get("closed_since_start") or 0
        ),
        "closed_paper_trades_today": int(fill_summary.get("closed_today") or 0),
        "last_decision_time": traces[0].get("ts") if traces else None,
        "last_order_time": order_summary.get("last_order_time"),
        "last_fill_time": fill_summary.get("last_fill_time"),
        "last_closed_trade_time": fill_summary.get("last_closed_trade_time"),
        "latest_block_reasons": _top_counts(block_labels),
        "latest_hold_reasons": _top_counts(hold_labels),
        "latest_regime_distribution": _top_counts(regimes),
        "latest_trace_sample_size": len(traces),
        "active_bot": active_bot,
        "crash_loop_count": int(crash_row.get("count") or 0),
        "circuit_breaker_count": int(circuit_row.get("count") or 0),
        "daily_loss_status": {
            "realized_pnl": float(daily.get("realized_pnl") or 0.0),
            "kill_switch_active": bool(daily.get("kill") or 0),
            "daily_loss_limit_usdt": float(getattr(settings, "DAILY_MAX_LOSS_USDT", 0.0)),
            "status": "BLOCKED" if bool(daily.get("kill") or 0) else "OK",
        },
        "max_daily_trades_status": {
            "trade_count": int(daily.get("trade_count") or 0),
            "limit": int(getattr(settings, "MAX_TRADES_DAILY", 0) or 0),
            "status": (
                "LIMIT_REACHED"
                if int(getattr(settings, "MAX_TRADES_DAILY", 0) or 0) > 0
                and int(daily.get("trade_count") or 0)
                >= int(getattr(settings, "MAX_TRADES_DAILY", 0) or 0)
                else "OK"
            ),
        },
    }


def _alert(name: str, severity: str, evidence: str, action: str) -> dict[str, str]:
    return {
        "alert_name": name,
        "severity": severity,
        "evidence": evidence,
        "recommended_action": action,
    }


def _build_alerts(report: dict[str, Any], clean_start_dt: datetime, generated: datetime) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []
    no_closes = report["closed_paper_trades_since_clean_start"] == 0
    elapsed_hours = (generated - clean_start_dt).total_seconds() / 3600
    for hours, name, severity in (
        (24, "NO_TRADES_AFTER_24H", "warning"),
        (72, "NO_TRADES_AFTER_72H", "warning"),
        (168, "NO_TRADES_AFTER_7D", "critical"),
    ):
        if no_closes and elapsed_hours >= hours:
            alerts.append(
                _alert(
                    name,
                    severity,
                    f"Zero closed paper trades after {elapsed_hours:.1f} hours from clean start.",
                    "continue_monitoring" if hours == 24 else "run_signal_audit",
                )
            )

    last_decision = _parse_timestamp(report.get("last_decision_time"))
    decision_age = (
        (generated - last_decision).total_seconds() if last_decision else None
    )
    if decision_age is None or decision_age >= 3600:
        alerts.append(
            _alert(
                "NO_DECISIONS_AFTER_1H",
                "critical" if decision_age is None else "warning",
                "No persisted BTCUSDT/ETHUSDT decision within the last hour.",
                "run_signal_audit",
            )
        )
        alerts.append(
            _alert(
                "MARKET_DATA_STALE",
                "critical" if decision_age is None else "warning",
                "Latest persisted strategy decision is absent or older than one hour.",
                "run_signal_audit",
            )
        )
    if report["health_ok"] is False:
        alerts.append(
            _alert(
                "BOT_NOT_RUNNING",
                "critical",
                f"Health endpoint unavailable: {report.get('health_error')}",
                "do_not_enable_live",
            )
        )

    counts = report.get("latest_block_reasons") or {}
    sample_size = int(report.get("latest_trace_sample_size") or 0)
    if sample_size and int(counts.get("regime_blocked") or 0) / sample_size >= 0.5:
        alerts.append(
            _alert(
                "REGIME_BLOCKING_DOMINATES",
                "info",
                f"Regime blocks are {counts.get('regime_blocked', 0)}/{sample_size} latest traces.",
                "review_strong_trend_experiment",
            )
        )
    if sample_size and int(counts.get("strategy_no_signal") or 0) / sample_size >= 0.5:
        alerts.append(
            _alert(
                "NO_PATTERN_DOMINATES",
                "info",
                f"No-pattern/no-signal holds are {counts.get('strategy_no_signal', 0)}/{sample_size} latest traces.",
                "run_component_replay",
            )
        )
    if (
        report.get("strong_trend_experiment_active")
        and int(report.get("strong_trend_cycles") or 0) > 0
        and int(report.get("strong_trend_signals") or 0) == 0
    ):
        attempts = int(report.get("strong_trend_order_attempts") or 0)
        created = int(report.get("strong_trend_paper_orders_created") or 0)
        note = str(report.get("strong_trend_order_consistency_note") or "").strip()
        extra = f" {note}" if note else ""
        if attempts or created:
            extra = (
                f" order_attempts={attempts}; paper_orders_created={created}."
                f"{extra}"
            )
        alerts.append(
            _alert(
                "STRONG_TREND_EXPERIMENT_NO_SIGNALS",
                "info",
                f"{report.get('strong_trend_cycles')} STRONG_TREND cycles produced zero signals.{extra}",
                "review_strong_trend_experiment",
            )
        )
    if report.get("stop_recommended"):
        alerts.append(
            _alert(
                "STRONG_TREND_STOP_RECOMMENDED",
                "critical",
                str(report.get("stop_reason") or "STRONG_TREND stop rules triggered."),
                "review_strong_trend_experiment",
            )
        )
    return alerts


def _update_section4_snapshot(path: Path, report: dict[str, Any]) -> None:
    text = path.read_text(encoding="utf-8") if path.exists() else "# IOFS Paper Validation Status\n"
    snapshot = "\n".join(
        [
            SNAPSHOT_HEADER,
            "",
            f"- generated_at_utc: {report['generated_at_utc']}",
            f"- bot_health: {'healthy' if report['health_ok'] else 'unavailable'}",
            f"- paper_orders_today: {report['paper_orders_today']}",
            f"- closed_paper_trades_since_clean_start: {report['closed_paper_trades_since_clean_start']}",
            f"- strong_trend_experiment_status: {'ACTIVE' if report['strong_trend_experiment_active'] else 'UNAVAILABLE'}",
            f"- section5_retry_ready: {str(report['ready_to_retry_5a']).lower()}",
            f"- active_alerts: {json.dumps([item['alert_name'] for item in report['alerts']])}",
            "- section4_status: In Progress",
            "",
            "Section 4 remains in progress. This snapshot does not approve live trading or capital deployment.",
            "",
        ]
    )
    pattern = re.compile(
        rf"\n?{re.escape(SNAPSHOT_HEADER)}.*?(?=\n## |\Z)",
        flags=re.DOTALL,
    )
    updated = pattern.sub("", text).rstrip() + "\n\n" + snapshot
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(updated, encoding="utf-8")


def render_markdown(report: dict[str, Any]) -> str:
    alerts = report["alerts"] or [
        {
            "alert_name": "NONE",
            "severity": "info",
            "evidence": "No configured daily monitor alerts are active.",
            "recommended_action": "continue_monitoring",
        }
    ]
    return "\n".join(
        [
            "# Daily Paper Validation Status",
            "",
            f"- generated_at_utc: {report['generated_at_utc']}",
            f"- bot_process_running: {report['bot_process_running']}",
            f"- health_ok: {str(report['health_ok']).lower()}",
            f"- execution_mode: {report['execution_mode']}",
            f"- binance_env: {report['binance_env']}",
            f"- ML_ENABLED: {report['ML_ENABLED']}",
            f"- IOFS_GATE_MODE: {report['IOFS_GATE_MODE']}",
            f"- trade_symbols: {report['trade_symbols']}",
            f"- live_symbols_count: {report['live_symbols_count']}",
            "",
            "## Paper Activity",
            "",
            f"- paper_orders_today: {report['paper_orders_today']}",
            f"- paper_fills_today: {report['paper_fills_today']}",
            f"- active_positions: {report['active_positions']}",
            f"- closed_paper_trades_total: {report['closed_paper_trades_total']}",
            f"- closed_paper_trades_since_clean_start: {report['closed_paper_trades_since_clean_start']}",
            f"- closed_paper_trades_today: {report['closed_paper_trades_today']}",
            f"- last_decision_time: {report['last_decision_time']}",
            f"- last_order_time: {report['last_order_time']}",
            f"- last_fill_time: {report['last_fill_time']}",
            f"- latest_block_reasons: `{report['latest_block_reasons']}`",
            f"- latest_hold_reasons: `{report['latest_hold_reasons']}`",
            f"- latest_regime_distribution: `{report['latest_regime_distribution']}`",
            f"- latest_component_failures: `{report['latest_component_failures']}`",
            "",
            "## STRONG_TREND Experiment",
            "",
            f"- active: {str(report['strong_trend_experiment_active']).lower()}",
            f"- cycles: {report['strong_trend_cycles']}",
            f"- signals: {report['strong_trend_signals']}",
            f"- order_attempts: {report['strong_trend_order_attempts']}",
            f"- paper_orders_created: {report['strong_trend_paper_orders_created']}",
            f"- order_errors: {report['strong_trend_order_errors']}",
            f"- fills: {report['strong_trend_fills']}",
            f"- closed_trades: {report['strong_trend_closed_trades']}",
            f"- stop_recommended: {str(report['stop_recommended']).lower()}",
            f"- stop_reason: {report['stop_reason'] or 'none'}",
            f"- order_consistency_note: {report['strong_trend_order_consistency_note'] or 'none'}",
            "",
            "## Section 5 Readiness",
            "",
            f"- organic_rows: {report['organic_rows']}",
            f"- iofs_organic_rows: {report['iofs_organic_rows']}",
            f"- closed_iofs_paper_trades: {report['closed_iofs_paper_trades']}",
            f"- ready_to_retry_5a: {str(report['ready_to_retry_5a']).lower()}",
            f"- ready_for_5b: {str(report['ready_for_5b']).lower()}",
            f"- section5b_status: {report['section5b_status']}",
            f"- t25_status: {report['t25_status']}",
            "",
            "## Active Alerts",
            "",
            "| Alert | Severity | Evidence | Recommended action |",
            "|---|---|---|---|",
            *[
                f"| {item['alert_name']} | {item['severity']} | {item['evidence']} | {item['recommended_action']} |"
                for item in alerts
            ],
            "",
            "Section 4 remains in progress. This monitor does not approve live trading.",
            "",
        ]
    )


def _write_reports(report: dict[str, Any], output_json: Path, output_md: Path) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    output_md.write_text(render_markdown(report), encoding="utf-8")


def run_daily_paper_validation_monitor(
    *,
    health_url: str = DEFAULT_HEALTH_URL,
    clean_start: str | None = None,
    db_path: str | Path | None = None,
    output_json: str | Path = DEFAULT_OUTPUT_JSON,
    output_md: str | Path = DEFAULT_OUTPUT_MD,
    section4_status: str | Path = DEFAULT_SECTION4_STATUS,
    active_env: str | Path = DEFAULT_ACTIVE_ENV,
    experiment_config: str | Path = DEFAULT_EXPERIMENT_CONFIG,
    component_report: str | Path = DEFAULT_COMPONENT_REPORT,
    production_dir: str | Path = DEFAULT_PRODUCTION_DIR,
    now: datetime | None = None,
    health_fetcher: Callable[[str], dict[str, Any]] = _fetch_health,
    strong_report_builder: Callable[[Path, str], dict[str, Any]] = build_strong_report,
    readiness_evaluator: Callable[..., dict[str, Any]] = evaluate_training_readiness,
) -> dict[str, Any]:
    """Run the monitor without raising into APScheduler."""
    generated = (now or _utc_now()).astimezone(timezone.utc)
    status_path = Path(section4_status)
    env_path = Path(active_env)
    production = Path(production_dir)
    experiment_path = Path(experiment_config)
    json_path = Path(output_json)
    md_path = Path(output_md)
    initialization_error: str | None = None
    if clean_start:
        clean_start_value = clean_start
    else:
        try:
            clean_start_value = _clean_start_from_status(status_path)
        except Exception as exc:
            clean_start_value = generated.isoformat()
            initialization_error = str(exc)
    clean_start_dt = _parse_timestamp(clean_start_value)
    if clean_start_dt is None:
        clean_start_dt = generated

    env_hash_before = _sha256(env_path)
    production_before = _list_files(production)
    experiment_hash_before = _sha256(experiment_path)
    report: dict[str, Any] = {
        "generated_at_utc": generated.isoformat(),
        "clean_start_utc": clean_start_value,
        "bot_process_running": "unknown",
        "health_ok": False,
        "health_error": None,
        "execution_mode": str(getattr(settings, "EXECUTION_MODE", "unknown")),
        "binance_env": str(getattr(settings, "BINANCE_ENV", "unknown")),
        "ML_ENABLED": bool(getattr(settings, "ML_ENABLED", False)),
        "IOFS_GATE_MODE": str(getattr(settings, "IOFS_GATE_MODE", "unknown")),
        "trade_symbols": str(getattr(settings, "TRADE_SYMBOLS", "")),
        "live_symbols_count": 0,
        "paper_orders_today": 0,
        "paper_fills_today": 0,
        "active_positions": 0,
        "closed_paper_trades_total": 0,
        "closed_paper_trades_since_clean_start": 0,
        "closed_paper_trades_today": 0,
        "last_decision_time": None,
        "last_order_time": None,
        "last_fill_time": None,
        "last_closed_trade_time": None,
        "latest_block_reasons": {},
        "latest_hold_reasons": {},
        "latest_regime_distribution": {},
        "latest_component_failures": {},
        "latest_trace_sample_size": 0,
        "crash_loop_count": 0,
        "circuit_breaker_count": 0,
        "daily_loss_status": {},
        "max_daily_trades_status": {},
        "strong_trend_experiment_active": False,
        "experiment_start_time": None,
        "strong_trend_cycles": 0,
        "strong_trend_signals": 0,
        "strong_trend_order_attempts": 0,
        "strong_trend_paper_orders_created": 0,
        "strong_trend_paper_orders": 0,
        "strong_trend_order_errors": 0,
        "strong_trend_fills": 0,
        "strong_trend_closed_trades": 0,
        "strong_trend_win_rate": None,
        "strong_trend_profit_factor": None,
        "strong_trend_expectancy_R": None,
        "strong_trend_max_drawdown_R": 0.0,
        "strong_trend_order_consistency_note": "",
        "strong_trend_order_count_diagnosis_summary": {},
        "stop_recommended": False,
        "stop_reason": "",
        "organic_rows": 0,
        "iofs_organic_rows": 0,
        "closed_iofs_paper_trades": 0,
        "ready_to_retry_5a": False,
        "ready_for_5b": False,
        "section5b_status": "BLOCKED",
        "t25_status": "NOT_STARTED",
        "blocking_reasons": [],
        "alerts": [],
        "section4_status": "In Progress",
        "pipeline_error": initialization_error,
        "env_changed": False,
        "production_changed": False,
        "strong_trend_experiment_changed": False,
    }
    try:
        try:
            health = health_fetcher(health_url)
            report["health_ok"] = str(health.get("status") or "").lower() in {
                "ok",
                "healthy",
            }
            report["bot_process_running"] = True if report["health_ok"] else "unknown"
            for target, source in (
                ("execution_mode", "execution_mode"),
                ("binance_env", "binance_env"),
                ("ML_ENABLED", "ml_enabled"),
                ("IOFS_GATE_MODE", "iofs_gate_mode"),
                ("trade_symbols", "trade_symbols"),
                ("live_symbols_count", "live_symbols_count"),
            ):
                if source in health:
                    report[target] = health[source]
        except Exception as exc:
            report["health_error"] = str(exc)
            report["health_ok"] = False
            report["bot_process_running"] = "unknown"

        resolved_db = resolve_db_path(str(db_path) if db_path else None)
        report.update(
            _collect_database_status(resolved_db, clean_start_value, generated=generated)
        )
        report["latest_component_failures"] = _latest_component_failures(
            Path(component_report)
        )

        experiment_values = parse_env(experiment_path)
        experiment_start = experiment_values.get("STRONG_TREND_EXPERIMENT_START_TIME")
        if experiment_start:
            strong = strong_report_builder(resolved_db, experiment_start)
            metrics = strong.get("strong_trend_metrics") or {}
            strong_attempts = int(strong.get("strong_trend_order_attempts") or 0)
            strong_created = int(
                strong.get("strong_trend_paper_orders_created")
                or strong.get("strong_trend_paper_orders")
                or 0
            )
            strong_errors = int(
                strong.get("strong_trend_order_errors")
                or strong.get("paper_order_errors")
                or 0
            )
            diagnosis_summary = dict(strong.get("order_count_diagnosis_summary") or {})
            if not diagnosis_summary:
                consistency_note = str(strong.get("order_consistency_note") or "").lower()
                diagnosis_summary = {
                    "total_reported_paper_orders": strong_attempts,
                    "valid_post_experiment_strong_trend_orders": strong_created,
                    "failed_attempts": strong_errors,
                    "historical_paper_only_skipped_attempts": strong_errors
                    if "failed attempt" in consistency_note
                    else 0,
                }
            report.update(
                {
                    "strong_trend_experiment_active": True,
                    "experiment_start_time": experiment_start,
                    "strong_trend_cycles": int(strong.get("strong_trend_cycles") or 0),
                    "strong_trend_signals": int(strong.get("strong_trend_signals") or 0),
                    "strong_trend_order_attempts": strong_attempts,
                    "strong_trend_paper_orders_created": strong_created,
                    "strong_trend_paper_orders": strong_created,
                    "strong_trend_order_errors": strong_errors,
                    "strong_trend_fills": int(
                        strong.get("strong_trend_fills") or 0
                    ),
                    "strong_trend_closed_trades": int(
                        strong.get("strong_trend_closed_trades") or 0
                    ),
                    "strong_trend_win_rate": metrics.get("win_rate"),
                    "strong_trend_profit_factor": metrics.get("profit_factor"),
                    "strong_trend_expectancy_R": metrics.get("expectancy_R"),
                    "strong_trend_max_drawdown_R": float(
                        metrics.get("max_drawdown_R") or 0.0
                    ),
                    "strong_trend_order_consistency_note": str(
                        strong.get("order_consistency_note") or ""
                    ),
                    "strong_trend_order_count_diagnosis_summary": diagnosis_summary,
                    "stop_recommended": bool(strong.get("stop_recommended")),
                    "stop_reason": str(strong.get("stop_reason") or ""),
                }
            )

        readiness = readiness_evaluator(
            paper_status_path=status_path,
            active_env_path=env_path,
        )
        report.update(
            {
                "organic_rows": int(readiness.get("organic_rows") or 0),
                "iofs_organic_rows": int(readiness.get("iofs_organic_rows") or 0),
                "closed_iofs_paper_trades": int(
                    readiness.get("closed_iofs_paper_trades") or 0
                ),
                "ready_to_retry_5a": bool(readiness.get("ready_to_retry_5a")),
                "ready_for_5b": bool(readiness.get("ready_for_5b")),
                "section5b_status": (
                    "READY" if readiness.get("ready_for_5b") else "BLOCKED"
                ),
                "blocking_reasons": list(readiness.get("blocking_reasons") or []),
            }
        )
        report["alerts"] = _build_alerts(report, clean_start_dt, generated)
        if initialization_error:
            report["alerts"].append(
                _alert(
                    "DAILY_MONITOR_FAILED",
                    "critical",
                    initialization_error,
                    "do_not_enable_live",
                )
            )
        _update_section4_snapshot(status_path, report)
    except Exception as exc:
        report["pipeline_error"] = str(exc)
        logger.exception(
            "[DAILY_PAPER_VALIDATION_MONITOR] failure=%s; trading runner continues",
            exc,
        )
        report["alerts"].append(
            _alert(
                "DAILY_MONITOR_FAILED",
                "critical",
                str(exc),
                "do_not_enable_live",
            )
        )
    finally:
        report["env_changed"] = env_hash_before != _sha256(env_path)
        report["production_changed"] = production_before != _list_files(production)
        report["strong_trend_experiment_changed"] = experiment_hash_before != _sha256(
            experiment_path
        )
        try:
            _write_reports(report, json_path, md_path)
        except Exception as exc:
            logger.exception(
                "[DAILY_PAPER_VALIDATION_MONITOR] report write failed=%s; runner continues",
                exc,
            )
    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--health-url", default=DEFAULT_HEALTH_URL)
    parser.add_argument("--clean-start")
    parser.add_argument("--db-path")
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON))
    parser.add_argument("--output-md", default=str(DEFAULT_OUTPUT_MD))
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = run_daily_paper_validation_monitor(
        health_url=args.health_url,
        clean_start=args.clean_start,
        db_path=args.db_path,
        output_json=args.output_json,
        output_md=args.output_md,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
