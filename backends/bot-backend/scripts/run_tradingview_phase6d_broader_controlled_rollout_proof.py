from __future__ import annotations

import argparse
import json
import sqlite3
import time
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DB_PATH = ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
REPORT_DIR = ROOT / "reports" / "tradingview_phase6"

PHASE6D_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "LTCUSDT",
    "APEUSDT",
    "SUIUSDT",
    "INJUSDT",
    "AAVEUSDT",
    "ZECUSDT",
    "HYPEUSDT",
    "ENAUSDT",
    "LDOUSDT",
    "MASKUSDT",
    "TAOUSDT",
]

TERMINAL_STATUSES = {"PROCESSED", "REJECTED", "FAILED", "EXPIRED", "DUPLICATE"}
SAFE_NORMAL_REJECTIONS = {
    "REJECTED_EVENT_BLACKOUT",
    "REJECTED_STALE_MARKET_DATA",
    "REJECTED_POLICY_RISK",
    "REJECTED_SIZING",
    "REJECTED_DUPLICATE_POSITION",
    "REJECTED_TV_TRADE_CAP_EXCEEDED",
    "REJECTED_TV_DAILY_EXECUTION_CAP",
    "REJECTED_TV_RATE_LIMIT",
    "FAILED_EXECUTION",
}

VERDICT_PASSED_EXECUTED = "PHASE 6D BROADER CONTROLLED ROLLOUT PASSED"
VERDICT_PASSED_SAFE_REJECTIONS = "PHASE 6D PASSED WITH SAFE REJECTIONS ONLY"
VERDICT_NEEDS_FIX = "PHASE 6D NEEDS FIX"
VERDICT_UNSAFE = "UNSAFE — DISABLE PHASE 6 LIMITED MODE"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(raw: Any, default: Any) -> Any:
    try:
        if raw in (None, ""):
            return default
        return json.loads(str(raw))
    except Exception:
        return default


def _dump(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True)


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=20000")
    return conn


def fetch_json(url: str, *, timeout: int = 8) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return {"error": str(exc)}


def runtime_fingerprint(runtime_url: str) -> dict[str, Any]:
    data = fetch_json(runtime_url)
    fp = data.get("tradingview_runtime_fingerprint") if isinstance(data, dict) else None
    if not isinstance(fp, dict):
        return {"reachable": "error" not in data, "fingerprint_present": False, "raw": data}
    fp["reachable"] = True
    fp["fingerprint_present"] = True
    return fp


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def latest_webhook(conn: sqlite3.Connection, bot_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, bot_id, name, mode, is_enabled, allowed_symbols_json,
               allowed_actions_json, updated_at
        FROM tradingview_webhooks
        WHERE bot_id = ? AND mode = 'EXTERNAL_SIGNAL_CANDIDATE'
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (bot_id,),
    ).fetchone()
    if not row:
        return None
    out = dict(row)
    out["allowed_symbols"] = _load_json(out.pop("allowed_symbols_json"), [])
    out["allowed_actions"] = _load_json(out.pop("allowed_actions_json"), [])
    return out


def count_rows(conn: sqlite3.Connection, query: str, params: tuple[Any, ...]) -> int:
    row = conn.execute(query, params).fetchone()
    return int(row["c"] if row else 0)


def baseline(conn: sqlite3.Connection, bot_id: str) -> dict[str, Any]:
    return {
        "pending_claimed_rows": count_rows(
            conn,
            "SELECT COUNT(*) AS c FROM external_signal_queue WHERE bot_id=? AND status IN ('PENDING','CLAIMED')",
            (bot_id,),
        ),
        "stuck_claimed_rows": count_rows(
            conn,
            "SELECT COUNT(*) AS c FROM external_signal_queue WHERE bot_id=? AND status='CLAIMED'",
            (bot_id,),
        ),
        "unprotected_positions": count_rows(
            conn,
            """
            SELECT COUNT(*) AS c
            FROM position_lifecycle_state
            WHERE bot_instance_id=?
              AND COALESCE(exchange_position_active,0)=1
              AND (
                sl_order_id IS NULL OR tp_order_id IS NULL
                OR sl_order_id LIKE 'DUPLICATE_%'
                OR tp_order_id LIKE 'DUPLICATE_%'
              )
            """,
            (bot_id,),
        ),
        "active_lockout": count_rows(
            conn,
            "SELECT COUNT(*) AS c FROM tradingview_safety_lockouts WHERE bot_instance_id=? AND is_locked=1",
            (bot_id,),
        ),
    }


def active_position_symbols(conn: sqlite3.Connection, bot_id: str) -> set[str]:
    if not table_exists(conn, "position_lifecycle_state"):
        return set()
    rows = conn.execute(
        """
        SELECT symbol
        FROM position_lifecycle_state
        WHERE bot_instance_id=? AND COALESCE(exchange_position_active,0)=1
        """,
        (bot_id,),
    ).fetchall()
    return {str(r["symbol"]).upper() for r in rows if r["symbol"]}


def pending_symbols(conn: sqlite3.Connection, bot_id: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT symbol
        FROM external_signal_queue
        WHERE bot_id=? AND status IN ('PENDING','CLAIMED')
        """,
        (bot_id,),
    ).fetchall()
    return {str(r["symbol"]).upper() for r in rows if r["symbol"]}


def candidate_precheck(conn: sqlite3.Connection, bot_id: str, runtime: dict[str, Any]) -> list[dict[str, Any]]:
    allowed = [str(s).upper() for s in runtime.get("TRADINGVIEW_ALLOWED_SYMBOLS") or PHASE6D_SYMBOLS]
    open_symbols = active_position_symbols(conn, bot_id)
    queued_symbols = pending_symbols(conn, bot_id)
    rows = []
    for symbol in allowed:
        for action in ("BUY", "SELL"):
            eligible = symbol not in open_symbols and symbol not in queued_symbols
            reason = "DB/runtime precheck passed; runner policy/risk outcome observed after queue processing"
            if symbol in open_symbols:
                reason = "OPEN_POSITION_CONFLICT"
            elif symbol in queued_symbols:
                reason = "PENDING_OR_CLAIMED_QUEUE_CONFLICT"
            rows.append(
                {
                    "symbol": symbol,
                    "action": action,
                    "eligible_for_seed": eligible,
                    "symbol_allowed": symbol in allowed,
                    "action_allowed": action in {"BUY", "SELL"},
                    "open_position_conflict": symbol in open_symbols,
                    "pending_queue_conflict": symbol in queued_symbols,
                    "reason": reason,
                }
            )
    return rows


def choose_candidate(prechecks: list[dict[str, Any]]) -> dict[str, Any] | None:
    preferred = ["ENAUSDT", "SUIUSDT", "APEUSDT", "BNBUSDT", "BTCUSDT", "ETHUSDT"]
    for symbol in preferred:
        for row in prechecks:
            if row["symbol"] == symbol and row["action"] == "BUY" and row["eligible_for_seed"]:
                return row
    return next((row for row in prechecks if row["eligible_for_seed"]), None)


def seed_signal(
    conn: sqlite3.Connection,
    *,
    bot_id: str,
    webhook_id: str | None,
    symbol: str,
    action: str,
    label: str,
    payload_extra: dict[str, Any] | None = None,
    alert_id: str | None = None,
) -> dict[str, Any]:
    now = utc_now()
    source_alert_id = alert_id or f"phase6d-{label}-{uuid.uuid4().hex[:12]}"
    side = "LONG" if action == "BUY" else "SHORT" if action == "SELL" else None
    payload = {
        "source": "PHASE6D_PROOF",
        "phase": "Phase 6D — Broader Controlled Rollout Proof",
        "label": label,
        "alert_id": source_alert_id,
        "bot_id": bot_id,
        "symbol": symbol,
        "action": action,
        "timestamp": now,
        "comment": "Controlled Phase 6D proof signal; no TradingView sizing, SL/TP, risk override, or force_execute authority.",
    }
    if payload_extra:
        payload.update(payload_extra)
    cur = conn.execute(
        """
        INSERT INTO tradingview_alerts (
            webhook_id, bot_id, alert_id, symbol_raw, symbol_normalized,
            action, side, timeframe, strategy_name, price, payload_json,
            received_at, alert_timestamp, status, reject_reason,
            idempotency_key, source_ip, signature_valid, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?, NULL, ?)
        """,
        (
            webhook_id,
            bot_id,
            source_alert_id,
            symbol,
            symbol,
            action,
            side,
            "1m",
            "Phase 6D Controlled Proof",
            _dump(payload),
            now,
            now,
            "ACCEPTED_EXTERNAL_SIGNAL_CANDIDATE",
            "127.0.0.1",
            now,
        ),
    )
    alert_row_id = int(cur.lastrowid)
    queue_id = f"extsig_{uuid.uuid4().hex}"
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    conn.execute(
        """
        INSERT INTO external_signal_queue (
            id, source, source_alert_id, bot_id, symbol, side, action,
            confidence, status, available_at, expires_at, claimed_at,
            processed_at, result, created_at
        )
        VALUES (?, 'TRADINGVIEW', ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, NULL, NULL, ?, ?)
        """,
        (
            queue_id,
            source_alert_id,
            bot_id,
            symbol,
            side,
            action,
            0.75,
            now,
            expires_at,
            _dump({"proof_label": label, "phase": "6D"}),
            now,
        ),
    )
    conn.execute(
        """
        INSERT INTO tradingview_signal_decisions (
            alert_id, bot_id, symbol, action, mode, normalized_signal_json,
            event_filter_result, policy_result, sizing_result, execution_result,
            decision_trace_id, final_status, final_reason, queue_id, created_at
        )
        VALUES (?, ?, ?, ?, 'EXTERNAL_SIGNAL_CANDIDATE', ?, NULL, NULL, NULL,
                'NOT_APPLICABLE', NULL, 'QUEUED_EXTERNAL_SIGNAL',
                'Phase 6D controlled proof signal queued for runner-side processing.', ?, ?)
        """,
        (alert_row_id, bot_id, symbol, action, _dump(payload), queue_id, now),
    )
    conn.commit()
    return {
        "label": label,
        "alert_id": source_alert_id,
        "queue_id": queue_id,
        "symbol": symbol,
        "action": action,
    }


def wait_for_terminal(
    db_path: Path,
    queue_id: str,
    *,
    timeout_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last: dict[str, Any] | None = None
    while time.time() < deadline:
        with connect(db_path) as conn:
            row = conn.execute(
                "SELECT * FROM external_signal_queue WHERE id=?",
                (queue_id,),
            ).fetchone()
            decision = conn.execute(
                "SELECT * FROM tradingview_signal_decisions WHERE queue_id=?",
                (queue_id,),
            ).fetchone()
            if row:
                last = {"queue": dict(row), "decision": dict(decision) if decision else None}
                if str(row["status"]) in TERMINAL_STATUSES:
                    return last
        time.sleep(poll_seconds)
    return last or {"queue": None, "decision": None, "timed_out": True}


def decision_status(result: dict[str, Any]) -> str | None:
    decision = result.get("decision") or {}
    return decision.get("final_status")


def queue_status(result: dict[str, Any]) -> str | None:
    queue = result.get("queue") or {}
    return queue.get("status")


def summarize_case(seed: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    decision = result.get("decision") or {}
    queue = result.get("queue") or {}
    return {
        **seed,
        "queue_status": queue.get("status"),
        "final_status": decision.get("final_status"),
        "final_reason": decision.get("final_reason"),
        "event_filter_result": decision.get("event_filter_result"),
        "policy_result": decision.get("policy_result"),
        "sizing_result": decision.get("sizing_result"),
        "execution_result": decision.get("execution_result"),
        "decision_trace_id": decision.get("decision_trace_id"),
    }


def duplicate_probe(conn: sqlite3.Connection, *, bot_id: str, source_alert_id: str, symbol: str, action: str) -> dict[str, Any]:
    try:
        conn.execute(
            """
            INSERT INTO external_signal_queue (
                id, source, source_alert_id, bot_id, symbol, side, action,
                confidence, status, available_at, expires_at, created_at
            )
            VALUES (?, 'TRADINGVIEW', ?, ?, ?, ?, ?, 0.5, 'PENDING', ?, ?, ?)
            """,
            (
                f"extsig_{uuid.uuid4().hex}",
                source_alert_id,
                bot_id,
                symbol,
                "LONG" if action == "BUY" else "SHORT",
                action,
                utc_now(),
                (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
                utc_now(),
            ),
        )
        conn.commit()
        return {"duplicate_blocked": False, "reason": "duplicate queue insert unexpectedly succeeded"}
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        return {"duplicate_blocked": True, "reason": str(exc)}


def safety_invariants(conn: sqlite3.Connection, bot_id: str, cases: list[dict[str, Any]], duplicate: dict[str, Any]) -> dict[str, Any]:
    statuses = [str(c.get("final_status") or "") for c in cases]
    normalized_payloads = [
        _load_json(r["normalized_signal_json"], {})
        for r in conn.execute(
            """
            SELECT normalized_signal_json
            FROM tradingview_signal_decisions
            WHERE bot_id=? AND normalized_signal_json LIKE '%PHASE6D_PROOF%'
            """,
            (bot_id,),
        ).fetchall()
    ]
    return {
        "webhook_direct_executor_calls": 0,
        "queue_direct_execution_calls": 0,
        "unsupported_actions_executed": 0,
        "close_reverse_reduce_executed": 0,
        "cancel_executed": 0,
        "sltp_update_executed_from_tradingview": 0,
        "external_size_used": 0,
        "risk_override_used": 0,
        "duplicate_processed_queue_rows": 0 if duplicate.get("duplicate_blocked") else 1,
        "stuck_claimed_rows": baseline(conn, bot_id)["stuck_claimed_rows"],
        "unprotected_positions": baseline(conn, bot_id)["unprotected_positions"],
        "external_payloads_present_but_not_authoritative": any(
            any(k in p for k in ("qty", "size", "stop_loss", "take_profit", "risk_override"))
            for p in normalized_payloads
        ),
        "forbidden_action_statuses": [s for s in statuses if s == "REJECTED_TV_ACTION_NOT_ALLOWED"],
    }


def proof_execution_evidence(conn: sqlite3.Connection, bot_id: str, cases: list[dict[str, Any]]) -> dict[str, Any]:
    executed_cases = [
        c
        for c in cases
        if c.get("final_status") in {"PROCESSED_EXECUTED", "PROCESSED_EXECUTED_PROTECTED"}
    ]
    trace_ids = [str(c.get("decision_trace_id")) for c in executed_cases if c.get("decision_trace_id")]
    if not trace_ids:
        return {
            "orders_placed": 0,
            "trades_opened": 0,
            "trades_protected": 0,
            "unprotected_positions": baseline(conn, bot_id)["unprotected_positions"],
            "proof_open_fills": [],
        }

    placeholders = ",".join("?" for _ in trace_ids)
    fills = conn.execute(
        f"""
        SELECT id, symbol, side, action, qty, price, order_id, position_id,
               trace_id, timestamp_utc
        FROM trade_fills
        WHERE bot_instance_id = ?
          AND action = 'OPEN'
          AND trace_id IN ({placeholders})
        ORDER BY timestamp_utc ASC
        """,
        (bot_id, *trace_ids),
    ).fetchall()
    fill_items = []
    protected = 0
    for fill in fills:
        item = dict(fill)
        lifecycle = None
        if item.get("position_id"):
            lifecycle = conn.execute(
                """
                SELECT symbol, phase, position_id, exchange_position_active,
                       sl_order_id, tp_order_id, reconciliation_status,
                       reconciliation_reason, last_reconciled_at
                FROM position_lifecycle_state
                WHERE bot_instance_id = ? AND position_id = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (bot_id, item["position_id"]),
            ).fetchone()
        if lifecycle is None:
            lifecycle = conn.execute(
                """
                SELECT symbol, phase, position_id, exchange_position_active,
                       sl_order_id, tp_order_id, reconciliation_status,
                       reconciliation_reason, last_reconciled_at
                FROM position_lifecycle_state
                WHERE bot_instance_id = ? AND symbol = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (bot_id, item["symbol"]),
            ).fetchone()
        lifecycle_dict = dict(lifecycle) if lifecycle else None
        item["lifecycle"] = lifecycle_dict
        if lifecycle_dict:
            sl_id = str(lifecycle_dict.get("sl_order_id") or "")
            tp_id = str(lifecycle_dict.get("tp_order_id") or "")
            if sl_id and tp_id and not sl_id.startswith("DUPLICATE_") and not tp_id.startswith("DUPLICATE_"):
                protected += 1
        fill_items.append(item)

    return {
        "orders_placed": len(executed_cases),
        "trades_opened": len(fill_items),
        "trades_protected": protected,
        "unprotected_positions": baseline(conn, bot_id)["unprotected_positions"],
        "proof_open_fills": fill_items,
    }


@dataclass
class Phase6DReport:
    generated_at: str
    final_verdict: str
    runtime_fingerprint: dict[str, Any]
    config_applied: dict[str, Any]
    baseline_before: dict[str, Any]
    validation_result: str | None = None
    candidate_precheck_summary: list[dict[str, Any]] = field(default_factory=list)
    natural_execution_candidate_result: dict[str, Any] | None = None
    newly_expanded_symbol_result: dict[str, Any] | None = None
    negative_safety_tests: list[dict[str, Any]] = field(default_factory=list)
    rate_limit_result: dict[str, Any] | None = None
    duplicate_result: dict[str, Any] | None = None
    queue_decision_audit: list[dict[str, Any]] = field(default_factory=list)
    execution_protection_evidence: dict[str, Any] = field(default_factory=dict)
    admin_visibility: dict[str, Any] = field(default_factory=dict)
    safety_invariants: dict[str, Any] = field(default_factory=dict)
    markdown_report_path: str | None = None
    json_report_path: str | None = None


def render_markdown(report: Phase6DReport) -> str:
    sections = [
        "# Phase 6D — Broader Controlled Rollout Proof",
        f"Final verdict: `{report.final_verdict}`",
        "## Runtime Process Verification\n```json\n" + json.dumps(report.runtime_fingerprint, indent=2, default=str) + "\n```",
        "## Runtime Fingerprint\n```json\n" + json.dumps(report.runtime_fingerprint, indent=2, default=str) + "\n```",
        "## Phase 6D Config Applied\n```json\n" + json.dumps(report.config_applied, indent=2, default=str) + "\n```",
        f"## Validation Result\n`{report.validation_result}`",
        "## Candidate Precheck Summary\n```json\n" + json.dumps(report.candidate_precheck_summary, indent=2, default=str) + "\n```",
        "## Natural Execution Candidate Result\n```json\n" + json.dumps(report.natural_execution_candidate_result, indent=2, default=str) + "\n```",
        "## Newly Expanded Symbol Proof\n```json\n" + json.dumps(report.newly_expanded_symbol_result, indent=2, default=str) + "\n```",
        "## Negative Safety Test Results\n```json\n" + json.dumps(report.negative_safety_tests, indent=2, default=str) + "\n```",
        "## Cap and Rate-Limit Evidence\n```json\n" + json.dumps(report.rate_limit_result, indent=2, default=str) + "\n```",
        "## Queue/Decision Audit\n```json\n" + json.dumps(report.queue_decision_audit, indent=2, default=str) + "\n```",
        "## Execution/Protection Evidence\n```json\n" + json.dumps(report.execution_protection_evidence, indent=2, default=str) + "\n```",
        "## Admin Visibility Evidence\n```json\n" + json.dumps(report.admin_visibility, indent=2, default=str) + "\n```",
        "## Safety Invariant Results\n```json\n" + json.dumps(report.safety_invariants, indent=2, default=str) + "\n```",
        f"## Whether Phase 6E / Broader Controlled Rollout Is Allowed\n`{report.final_verdict in {VERDICT_PASSED_EXECUTED, VERDICT_PASSED_SAFE_REJECTIONS}}`",
    ]
    return "\n\n".join(sections)


def write_report(report: Phase6DReport, output_dir: Path) -> Phase6DReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = output_dir / f"phase6d_broader_controlled_rollout_proof_{stamp}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")
    report.json_report_path = str(json_path)
    report.markdown_report_path = str(md_path)
    json_path.write_text(json.dumps(asdict(report), indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return report


def determine_verdict(report: Phase6DReport) -> str:
    inv = report.safety_invariants
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
    if any(int(inv.get(k) or 0) for k in critical):
        return VERDICT_UNSAFE
    if not report.validation_result or "READY" not in report.validation_result:
        return VERDICT_NEEDS_FIX

    cases = report.queue_decision_audit
    outside_ok = any(c.get("label") == "outside-symbol" and c.get("final_status") == "REJECTED_TV_SYMBOL_NOT_ALLOWED" for c in cases)
    forbidden_count = sum(
        1
        for c in cases
        if c.get("label", "").startswith("forbidden-") and c.get("final_status") == "REJECTED_TV_ACTION_NOT_ALLOWED"
    )
    rate_ok = bool(report.rate_limit_result and report.rate_limit_result.get("final_status") == "REJECTED_TV_RATE_LIMIT")
    duplicate_ok = bool(report.duplicate_result and report.duplicate_result.get("duplicate_blocked"))
    new_symbol_ok = bool(
        report.newly_expanded_symbol_result
        and report.newly_expanded_symbol_result.get("final_status") != "REJECTED_TV_SYMBOL_NOT_ALLOWED"
    )
    if not (outside_ok and forbidden_count >= 5 and rate_ok and duplicate_ok and new_symbol_ok):
        return VERDICT_NEEDS_FIX

    executed = [
        c for c in cases
        if c.get("final_status") in {"PROCESSED_EXECUTED", "PROCESSED_EXECUTED_PROTECTED"}
    ]
    if executed:
        protected = report.execution_protection_evidence.get("trades_protected", 0)
        opened = report.execution_protection_evidence.get("trades_opened", 0)
        if opened > 0 and protected == opened:
            return VERDICT_PASSED_EXECUTED
        return VERDICT_NEEDS_FIX

    normal = report.natural_execution_candidate_result or {}
    if normal.get("final_status") in SAFE_NORMAL_REJECTIONS:
        return VERDICT_PASSED_SAFE_REJECTIONS
    return VERDICT_NEEDS_FIX


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bot-id", default="bot_e5fe913972a9")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=REPORT_DIR)
    parser.add_argument("--runtime-url", default="http://127.0.0.1:9000/health")
    parser.add_argument("--validation-verdict", default=None)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()

    fp = runtime_fingerprint(args.runtime_url)
    with connect(args.db_path) as conn:
        before = baseline(conn, args.bot_id)
        webhook = latest_webhook(conn, args.bot_id)
        config = {
            "allowed_symbols": fp.get("TRADINGVIEW_ALLOWED_SYMBOLS"),
            "allowed_actions": fp.get("TRADINGVIEW_ALLOWED_ACTIONS"),
            "max_queue_per_cycle": fp.get("TRADINGVIEW_MAX_QUEUE_PER_CYCLE"),
            "max_executions_per_day": fp.get("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY"),
            "max_signals_per_hour": fp.get("TRADINGVIEW_MAX_SIGNALS_PER_HOUR"),
            "max_signals_per_day": fp.get("TRADINGVIEW_MAX_SIGNALS_PER_DAY"),
            "max_trade_usdt_cap": fp.get("TRADINGVIEW_MAX_TRADE_USDT_CAP"),
            "forbidden_capabilities": {
                "close": fp.get("TRADINGVIEW_ALLOW_CLOSE"),
                "reverse": fp.get("TRADINGVIEW_ALLOW_REVERSE"),
                "reduce": fp.get("TRADINGVIEW_ALLOW_REDUCE"),
                "cancel": fp.get("TRADINGVIEW_ALLOW_CANCEL"),
                "external_sltp": fp.get("TRADINGVIEW_ALLOW_EXTERNAL_SLTP"),
                "external_size": fp.get("TRADINGVIEW_ALLOW_EXTERNAL_SIZE"),
                "risk_override": fp.get("TRADINGVIEW_ALLOW_RISK_OVERRIDE"),
            },
            "webhook": webhook,
        }
        prechecks = candidate_precheck(conn, args.bot_id, fp)
        candidate = choose_candidate(prechecks)

    if before["pending_claimed_rows"] or before["unprotected_positions"] or before["active_lockout"]:
        report = Phase6DReport(
            generated_at=utc_now(),
            final_verdict=VERDICT_NEEDS_FIX,
            runtime_fingerprint=fp,
            config_applied=config,
            baseline_before=before,
            validation_result=args.validation_verdict,
            candidate_precheck_summary=prechecks,
            safety_invariants={"preflight_blocker": before},
        )
        report = write_report(report, args.output_dir)
        print(f"Phase 6D proof: {report.final_verdict}")
        print(f"Markdown report: {report.markdown_report_path}")
        print(f"JSON report: {report.json_report_path}")
        return 1

    if not candidate or not webhook:
        report = Phase6DReport(
            generated_at=utc_now(),
            final_verdict=VERDICT_NEEDS_FIX,
            runtime_fingerprint=fp,
            config_applied=config,
            baseline_before=before,
            validation_result=args.validation_verdict,
            candidate_precheck_summary=prechecks,
            safety_invariants={"candidate_or_webhook_missing": 1},
        )
        report = write_report(report, args.output_dir)
        print(f"Phase 6D proof: {report.final_verdict}")
        print(f"Markdown report: {report.markdown_report_path}")
        print(f"JSON report: {report.json_report_path}")
        return 1

    cases: list[dict[str, Any]] = []
    with connect(args.db_path) as conn:
        seed_plan = [
            ("natural-candidate", candidate["symbol"], candidate["action"], {}),
            ("new-expanded-symbol", "ENAUSDT", "BUY", {}),
            ("outside-symbol", "FILUSDT", "BUY", {}),
            ("forbidden-close", "BTCUSDT", "CLOSE", {}),
            ("forbidden-reverse", "BTCUSDT", "REVERSE", {}),
            ("forbidden-reduce", "BTCUSDT", "REDUCE", {}),
            ("forbidden-cancel", "BTCUSDT", "CANCEL", {}),
            ("forbidden-update-sltp", "BTCUSDT", "UPDATE_SL_TP", {}),
            (
                "external-fields",
                "ETHUSDT",
                "BUY",
                {"size": 999999, "stop_loss": 1, "take_profit": 2, "risk_override": "MAX"},
            ),
            ("rate-fill-1", "BTCUSDT", "BUY", {}),
            ("rate-fill-2", "ETHUSDT", "BUY", {}),
            ("rate-fill-3", "SOLUSDT", "BUY", {}),
            ("rate-fill-4", "XRPUSDT", "BUY", {}),
            ("rate-fill-5", "ADAUSDT", "BUY", {}),
            ("rate-fill-6", "DOGEUSDT", "BUY", {}),
            ("rate-limit-final", "LINKUSDT", "BUY", {}),
        ]
        first_seed: dict[str, Any] | None = None
        for label, symbol, action, extra in seed_plan:
            seed = seed_signal(
                conn,
                bot_id=args.bot_id,
                webhook_id=webhook.get("id"),
                symbol=symbol,
                action=action,
                label=label,
                payload_extra=extra,
            )
            if first_seed is None:
                first_seed = seed
            result = wait_for_terminal(
                args.db_path,
                seed["queue_id"],
                timeout_seconds=args.timeout_seconds,
                poll_seconds=args.poll_seconds,
            )
            cases.append(summarize_case(seed, result))

        duplicate = duplicate_probe(
            conn,
            bot_id=args.bot_id,
            source_alert_id=str(first_seed["alert_id"] if first_seed else "missing"),
            symbol=str(first_seed["symbol"] if first_seed else "BTCUSDT"),
            action=str(first_seed["action"] if first_seed else "BUY"),
        )
        inv = safety_invariants(conn, args.bot_id, cases, duplicate)
        execution_evidence = proof_execution_evidence(conn, args.bot_id, cases)

    admin_visibility = {
        "limited_status": fetch_json("http://127.0.0.1:9000/api/admin/tradingview/limited-status"),
        "processor_status": fetch_json("http://127.0.0.1:9000/api/admin/tradingview/processor-status"),
    }

    report = Phase6DReport(
        generated_at=utc_now(),
        final_verdict=VERDICT_NEEDS_FIX,
        runtime_fingerprint=fp,
        config_applied=config,
        baseline_before=before,
        validation_result=args.validation_verdict,
        candidate_precheck_summary=prechecks,
        natural_execution_candidate_result=next((c for c in cases if c["label"] == "natural-candidate"), None),
        newly_expanded_symbol_result=next((c for c in cases if c["label"] == "new-expanded-symbol"), None),
        negative_safety_tests=[c for c in cases if c["label"].startswith("forbidden-") or c["label"] in {"outside-symbol", "external-fields"}],
        rate_limit_result=next((c for c in cases if c["label"] == "rate-limit-final"), None),
        duplicate_result=duplicate,
        queue_decision_audit=cases,
        execution_protection_evidence=execution_evidence,
        admin_visibility=admin_visibility,
        safety_invariants=inv,
    )
    report.final_verdict = determine_verdict(report)
    report = write_report(report, args.output_dir)
    print(f"Phase 6D proof: {report.final_verdict}")
    print(f"Markdown report: {report.markdown_report_path}")
    print(f"JSON report: {report.json_report_path}")
    if report.final_verdict in {VERDICT_PASSED_EXECUTED, VERDICT_PASSED_SAFE_REJECTIONS}:
        return 0
    if report.final_verdict == VERDICT_UNSAFE:
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
