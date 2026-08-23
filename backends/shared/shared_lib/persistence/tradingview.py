from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from shared_lib.persistence.db import DB


MODE_ADVISORY_ONLY = "ADVISORY_ONLY"
MODE_EXTERNAL_SIGNAL_CANDIDATE = "EXTERNAL_SIGNAL_CANDIDATE"
MODE_CONFIRMATION_ONLY = "CONFIRMATION_ONLY"
MODE_DISABLED = "DISABLED"

STATUS_ACCEPTED_ADVISORY = "ACCEPTED_ADVISORY"
STATUS_REJECTED = "REJECTED"
STATUS_DUPLICATE = "DUPLICATE"
STATUS_STALE = "STALE"
STATUS_DISABLED_WEBHOOK = "DISABLED_WEBHOOK"
STATUS_INVALID_SIGNATURE = "INVALID_SIGNATURE"
STATUS_INVALID_TOKEN = "INVALID_TOKEN"
STATUS_INVALID_SCHEMA = "INVALID_SCHEMA"
STATUS_UNSUPPORTED_ACTION = "UNSUPPORTED_ACTION"
STATUS_UNSUPPORTED_SYMBOL = "UNSUPPORTED_SYMBOL"
STATUS_RATE_LIMITED = "RATE_LIMITED"

SUPPORTED_STAGE1_ACTIONS = {"BUY", "SELL"}
REJECTED_STAGE1_ACTIONS = {"CLOSE", "REDUCE", "REVERSE", "CANCEL", "UPDATE_SL_TP"}

QUEUE_STATUS_PENDING = "PENDING"
QUEUE_STATUS_CLAIMED = "CLAIMED"
QUEUE_STATUS_PROCESSED = "PROCESSED"
QUEUE_STATUS_REJECTED = "REJECTED"
QUEUE_STATUS_EXPIRED = "EXPIRED"
QUEUE_STATUS_DUPLICATE = "DUPLICATE"
QUEUE_STATUS_FAILED = "FAILED"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dump(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True)


def _load(raw: Any, default: Any) -> Any:
    try:
        if raw is None or raw == "":
            return default
        return json.loads(str(raw))
    except Exception:
        return default


def ensure_tradingview_schema(db: DB | None = None) -> None:
    db = db or DB()
    with db.connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tradingview_webhooks (
                id TEXT PRIMARY KEY,
                bot_id TEXT NOT NULL,
                name TEXT NOT NULL,
                token_hash TEXT NOT NULL,
                secret_hash TEXT,
                mode TEXT NOT NULL DEFAULT 'ADVISORY_ONLY',
                is_enabled INTEGER NOT NULL DEFAULT 1,
                allowed_symbols_json TEXT,
                allowed_actions_json TEXT NOT NULL DEFAULT '["BUY","SELL"]',
                max_alert_age_seconds INTEGER NOT NULL DEFAULT 300,
                rate_limit_per_minute INTEGER NOT NULL DEFAULT 30,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_used_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tradingview_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                webhook_id TEXT,
                bot_id TEXT,
                alert_id TEXT,
                symbol_raw TEXT,
                symbol_normalized TEXT,
                action TEXT,
                side TEXT,
                timeframe TEXT,
                strategy_name TEXT,
                price REAL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                received_at TEXT NOT NULL,
                alert_timestamp TEXT,
                status TEXT NOT NULL,
                reject_reason TEXT,
                idempotency_key TEXT,
                source_ip TEXT,
                signature_valid INTEGER,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tradingview_signal_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER NOT NULL,
                bot_id TEXT,
                symbol TEXT,
                action TEXT,
                mode TEXT NOT NULL,
                normalized_signal_json TEXT NOT NULL DEFAULT '{}',
                event_filter_result TEXT,
                policy_result TEXT,
                sizing_result TEXT,
                execution_result TEXT,
                decision_trace_id TEXT,
                final_status TEXT NOT NULL,
                final_reason TEXT NOT NULL,
                queue_id TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        rows = conn.execute("PRAGMA table_info(tradingview_signal_decisions)").fetchall()
        existing = {r["name"] for r in rows}
        if "queue_id" not in existing:
            conn.execute("ALTER TABLE tradingview_signal_decisions ADD COLUMN queue_id TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_signal_queue (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                source_alert_id TEXT NOT NULL,
                bot_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT,
                action TEXT NOT NULL,
                confidence REAL,
                status TEXT NOT NULL,
                available_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                claimed_at TEXT,
                processed_at TEXT,
                result TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_tv_alert_webhook_alert "
            "ON tradingview_alerts(webhook_id, alert_id)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_tv_alert_idempotency "
            "ON tradingview_alerts(idempotency_key) WHERE idempotency_key IS NOT NULL"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tv_alert_bot_received "
            "ON tradingview_alerts(bot_id, received_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tv_alert_symbol_received "
            "ON tradingview_alerts(symbol_normalized, received_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tv_alert_status "
            "ON tradingview_alerts(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tv_decisions_created "
            "ON tradingview_signal_decisions(created_at)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_external_signal_source_alert "
            "ON external_signal_queue(source, source_alert_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_external_signal_status "
            "ON external_signal_queue(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_external_signal_bot_created "
            "ON external_signal_queue(bot_id, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_external_signal_symbol_created "
            "ON external_signal_queue(symbol, created_at)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tradingview_processor_heartbeat (
                bot_instance_id TEXT PRIMARY KEY,
                processor_enabled INTEGER NOT NULL DEFAULT 0,
                env_gate_reason TEXT,
                last_started_at TEXT,
                last_finished_at TEXT,
                last_processed_count INTEGER NOT NULL DEFAULT 0,
                last_rejected_count INTEGER NOT NULL DEFAULT 0,
                last_failed_count INTEGER NOT NULL DEFAULT 0,
                last_skipped_count INTEGER NOT NULL DEFAULT 0,
                last_skipped_reason TEXT,
                last_result_json TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tradingview_safety_lockouts (
                bot_instance_id TEXT PRIMARY KEY,
                is_locked INTEGER NOT NULL DEFAULT 0,
                reason TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def get_tradingview_safety_lockout(db: DB, bot_instance_id: str) -> dict[str, Any] | None:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM tradingview_safety_lockouts WHERE bot_instance_id = ?",
            (bot_instance_id,),
        ).fetchone()
    return dict(row) if row else None


def set_tradingview_safety_lockout(db: DB, *, bot_instance_id: str, reason: str) -> None:
    ensure_tradingview_schema(db)
    now = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO tradingview_safety_lockouts (
                bot_instance_id, is_locked, reason, created_at, updated_at
            ) VALUES (?, 1, ?, ?, ?)
            ON CONFLICT(bot_instance_id) DO UPDATE SET
                is_locked = 1,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (bot_instance_id, reason, now, now),
        )


def upsert_processor_heartbeat(
    db: DB,
    *,
    bot_instance_id: str,
    processor_enabled: bool,
    env_gate_reason: str | None = None,
    last_started_at: str | None = None,
    last_finished_at: str | None = None,
    last_processed_count: int = 0,
    last_rejected_count: int = 0,
    last_failed_count: int = 0,
    last_skipped_count: int = 0,
    last_skipped_reason: str | None = None,
    last_result_json: str | None = None,
    last_error: str | None = None,
) -> None:
    ensure_tradingview_schema(db)
    now = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO tradingview_processor_heartbeat (
                bot_instance_id, processor_enabled, env_gate_reason,
                last_started_at, last_finished_at,
                last_processed_count, last_rejected_count,
                last_failed_count, last_skipped_count,
                last_skipped_reason, last_result_json, last_error, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bot_instance_id) DO UPDATE SET
                processor_enabled   = excluded.processor_enabled,
                env_gate_reason     = excluded.env_gate_reason,
                last_started_at     = COALESCE(excluded.last_started_at, last_started_at),
                last_finished_at    = COALESCE(excluded.last_finished_at, last_finished_at),
                last_processed_count= excluded.last_processed_count,
                last_rejected_count = excluded.last_rejected_count,
                last_failed_count   = excluded.last_failed_count,
                last_skipped_count  = excluded.last_skipped_count,
                last_skipped_reason = excluded.last_skipped_reason,
                last_result_json    = COALESCE(excluded.last_result_json, last_result_json),
                last_error          = excluded.last_error,
                updated_at          = excluded.updated_at
            """,
            (
                bot_instance_id,
                int(processor_enabled),
                env_gate_reason,
                last_started_at,
                last_finished_at,
                last_processed_count,
                last_rejected_count,
                last_failed_count,
                last_skipped_count,
                last_skipped_reason,
                last_result_json,
                last_error,
                now,
            ),
        )


def get_processor_heartbeat(db: DB, bot_instance_id: str) -> dict[str, Any] | None:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM tradingview_processor_heartbeat WHERE bot_instance_id = ?",
            (bot_instance_id,),
        ).fetchone()
    return dict(row) if row else None


def list_processor_heartbeats(db: DB) -> list[dict[str, Any]]:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM tradingview_processor_heartbeat ORDER BY updated_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def hash_secret(raw: str) -> str:
    salt = secrets.token_bytes(16)
    iterations = 120_000
    digest = hashlib.pbkdf2_hmac("sha256", raw.encode("utf-8"), salt, iterations)
    return "pbkdf2_sha256${}${}${}".format(
        iterations,
        base64.urlsafe_b64encode(salt).decode("ascii"),
        base64.urlsafe_b64encode(digest).decode("ascii"),
    )


def verify_secret(raw: str, encoded: str | None) -> bool:
    if not raw or not encoded:
        return False
    try:
        algo, iterations_s, salt_s, digest_s = encoded.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        salt = base64.urlsafe_b64decode(salt_s.encode("ascii"))
        expected = base64.urlsafe_b64decode(digest_s.encode("ascii"))
        actual = hashlib.pbkdf2_hmac(
            "sha256", raw.encode("utf-8"), salt, int(iterations_s)
        )
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def generate_webhook_token() -> str:
    return "cf_tv_" + secrets.token_urlsafe(32)


def verify_hmac_signature(*, token: str, body: bytes, timestamp: str, nonce: str, signature: str) -> bool:
    if not token or not signature:
        return False
    signed = timestamp.encode("utf-8") + b"." + nonce.encode("utf-8") + b"." + body
    expected = hmac.new(token.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    provided = signature.removeprefix("sha256=").strip()
    return hmac.compare_digest(expected, provided)


def create_webhook(
    db: DB,
    *,
    bot_id: str,
    name: str = "TradingView Advisory Webhook",
    allowed_symbols: list[str] | None = None,
    allowed_actions: list[str] | None = None,
    mode: str = MODE_ADVISORY_ONLY,
    is_enabled: bool = True,
    max_alert_age_seconds: int = 300,
    rate_limit_per_minute: int = 30,
) -> dict[str, Any]:
    ensure_tradingview_schema(db)
    raw_token = generate_webhook_token()
    now = utc_now_iso()
    webhook_id = f"tvwh_{uuid.uuid4().hex}"
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO tradingview_webhooks (
                id, bot_id, name, token_hash, secret_hash, mode, is_enabled,
                allowed_symbols_json, allowed_actions_json, max_alert_age_seconds,
                rate_limit_per_minute, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                webhook_id,
                bot_id,
                name,
                hash_secret(raw_token),
                None,
                mode,
                1 if is_enabled else 0,
                _dump(allowed_symbols) if allowed_symbols is not None else None,
                _dump(allowed_actions or ["BUY", "SELL"]),
                max_alert_age_seconds,
                rate_limit_per_minute,
                now,
                now,
            ),
        )
    return {"id": webhook_id, "token": raw_token}


def find_webhook_by_id_or_token(db: DB, token_or_id: str, payload_token: str | None = None) -> tuple[dict[str, Any] | None, str | None]:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        by_id = conn.execute(
            "SELECT * FROM tradingview_webhooks WHERE id = ?",
            (token_or_id,),
        ).fetchone()
        if by_id:
            token = payload_token
            if token and verify_secret(token, by_id["token_hash"]):
                return dict(by_id), token
            return dict(by_id), None

        token = payload_token or token_or_id
        rows = conn.execute("SELECT * FROM tradingview_webhooks").fetchall()
    for row in rows:
        if verify_secret(token, row["token_hash"]):
            return dict(row), token
    return None, None


def compute_idempotency_key(
    *, webhook_id: str, alert_id: str, symbol: str, action: str, alert_timestamp: str
) -> str:
    raw = "|".join([webhook_id, alert_id, symbol, action, alert_timestamp])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def insert_alert(
    db: DB,
    *,
    webhook_id: str | None,
    bot_id: str | None,
    alert_id: str | None,
    symbol_raw: str | None,
    symbol_normalized: str | None,
    action: str | None,
    side: str | None,
    timeframe: str | None,
    strategy_name: str | None,
    price: float | None,
    payload: dict[str, Any],
    received_at: str,
    alert_timestamp: str | None,
    status: str,
    reject_reason: str | None,
    idempotency_key: str | None,
    source_ip: str | None,
    signature_valid: bool | None,
) -> int:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO tradingview_alerts (
                webhook_id, bot_id, alert_id, symbol_raw, symbol_normalized,
                action, side, timeframe, strategy_name, price, payload_json,
                received_at, alert_timestamp, status, reject_reason,
                idempotency_key, source_ip, signature_valid, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                webhook_id,
                bot_id,
                alert_id,
                symbol_raw,
                symbol_normalized,
                action,
                side,
                timeframe,
                strategy_name,
                price,
                _dump(payload),
                received_at,
                alert_timestamp,
                status,
                reject_reason,
                idempotency_key,
                source_ip,
                None if signature_valid is None else int(signature_valid),
                received_at,
            ),
        )
        return int(cur.lastrowid)


def insert_advisory_decision(
    db: DB,
    *,
    alert_row_id: int,
    bot_id: str,
    symbol: str,
    action: str,
    normalized_signal: dict[str, Any],
) -> int:
    ensure_tradingview_schema(db)
    now = utc_now_iso()
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO tradingview_signal_decisions (
                alert_id, bot_id, symbol, action, mode, normalized_signal_json,
                event_filter_result, policy_result, sizing_result, execution_result,
                decision_trace_id, final_status, final_reason, queue_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, ?, NULL, ?)
            """,
            (
                alert_row_id,
                bot_id,
                symbol,
                action,
                MODE_ADVISORY_ONLY,
                _dump(normalized_signal),
                "ACCEPTED_ADVISORY_ONLY",
                "Alert accepted; advisory-only mode; no queue row created.",
                now,
            ),
        )
        return int(cur.lastrowid)


def create_external_signal_queue_row(
    db: DB,
    *,
    source_alert_id: str,
    bot_id: str,
    symbol: str,
    side: str | None,
    action: str,
    confidence: float | None,
    available_at: str,
    expires_at: str,
    result: dict[str, Any] | None = None,
) -> str:
    ensure_tradingview_schema(db)
    queue_id = f"extsig_{uuid.uuid4().hex}"
    now = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO external_signal_queue (
                id, source, source_alert_id, bot_id, symbol, side, action,
                confidence, status, available_at, expires_at, claimed_at,
                processed_at, result, created_at
            )
            VALUES (?, 'TRADINGVIEW', ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
            """,
            (
                queue_id,
                str(source_alert_id),
                bot_id,
                symbol,
                side,
                action,
                confidence,
                QUEUE_STATUS_PENDING,
                available_at,
                expires_at,
                _dump(result) if result is not None else None,
                now,
            ),
        )
    return queue_id


def find_queue_row_by_source_alert(
    db: DB,
    *,
    source: str,
    source_alert_id: str,
) -> dict[str, Any] | None:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM external_signal_queue
            WHERE source = ? AND source_alert_id = ?
            """,
            (source, str(source_alert_id)),
        ).fetchone()
    return dict(row) if row else None


def mark_queue_status(
    db: DB,
    *,
    queue_id: str,
    status: str,
    result: dict[str, Any] | None = None,
) -> None:
    ensure_tradingview_schema(db)
    now = utc_now_iso()
    processed_at = now if status in {QUEUE_STATUS_PROCESSED, QUEUE_STATUS_REJECTED, QUEUE_STATUS_EXPIRED, QUEUE_STATUS_FAILED} else None
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE external_signal_queue
            SET status = ?,
                processed_at = COALESCE(?, processed_at),
                result = COALESCE(?, result)
            WHERE id = ?
            """,
            (status, processed_at, _dump(result) if result is not None else None, queue_id),
        )


def insert_candidate_decision(
    db: DB,
    *,
    alert_row_id: int,
    bot_id: str,
    symbol: str,
    action: str,
    normalized_signal: dict[str, Any],
    queue_id: str,
) -> int:
    ensure_tradingview_schema(db)
    now = utc_now_iso()
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO tradingview_signal_decisions (
                alert_id, bot_id, symbol, action, mode, normalized_signal_json,
                event_filter_result, policy_result, sizing_result, execution_result,
                decision_trace_id, final_status, final_reason, queue_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, ?, ?, ?)
            """,
            (
                alert_row_id,
                bot_id,
                symbol,
                action,
                MODE_EXTERNAL_SIGNAL_CANDIDATE,
                _dump(normalized_signal),
                "NOT_APPLICABLE",
                "QUEUED_EXTERNAL_SIGNAL",
                "Alert accepted and queued as external signal candidate; runner processing disabled in this phase.",
                queue_id,
                now,
            ),
        )
        return int(cur.lastrowid)


def insert_rejected_decision(
    db: DB,
    *,
    alert_row_id: int,
    bot_id: str | None,
    symbol: str | None,
    action: str | None,
    mode: str | None,
    normalized_signal: dict[str, Any],
    final_status: str,
    final_reason: str,
) -> int:
    ensure_tradingview_schema(db)
    now = utc_now_iso()
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO tradingview_signal_decisions (
                alert_id, bot_id, symbol, action, mode, normalized_signal_json,
                event_filter_result, policy_result, sizing_result, execution_result,
                decision_trace_id, final_status, final_reason, queue_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, ?, NULL, ?)
            """,
            (
                alert_row_id,
                bot_id,
                symbol,
                action,
                mode or "REJECTED",
                _dump(normalized_signal),
                final_status,
                final_reason,
                now,
            ),
        )
        return int(cur.lastrowid)


def update_signal_decision_processing_result(
    db: DB,
    *,
    queue_id: str,
    event_filter_result: str | None,
    policy_result: str | None,
    sizing_result: str | None,
    execution_result: str | None,
    final_status: str,
    final_reason: str,
    decision_trace_id: str | None = None,
) -> bool:
    """
    Update the tradingview_signal_decisions row linked to queue_id with Phase 4 results.
    Returns True if a row was updated, False if no matching row was found.
    """
    ensure_tradingview_schema(db)
    now = utc_now_iso()
    with db.connect() as conn:
        changed = conn.execute(
            """
            UPDATE tradingview_signal_decisions
            SET event_filter_result  = COALESCE(?, event_filter_result),
                policy_result        = COALESCE(?, policy_result),
                sizing_result        = COALESCE(?, sizing_result),
                execution_result     = ?,
                decision_trace_id    = COALESCE(?, decision_trace_id),
                final_status         = ?,
                final_reason         = ?
            WHERE queue_id = ?
            """,
            (
                event_filter_result,
                policy_result,
                sizing_result,
                execution_result,
                decision_trace_id,
                final_status,
                final_reason,
                queue_id,
            ),
        ).rowcount
    return changed > 0


def update_webhook_last_used(db: DB, webhook_id: str, when: str) -> None:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        conn.execute(
            "UPDATE tradingview_webhooks SET last_used_at = ?, updated_at = ? WHERE id = ?",
            (when, when, webhook_id),
        )


def recent_request_count(db: DB, webhook_id: str, since_iso: str) -> int:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM tradingview_alerts
            WHERE webhook_id = ? AND received_at >= ?
            """,
            (webhook_id, since_iso),
        ).fetchone()
    return int(row["c"] if row else 0)


def list_webhooks(db: DB, *, limit: int = 100) -> list[dict[str, Any]]:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT id, bot_id, name, mode, is_enabled, allowed_symbols_json,
                   allowed_actions_json, max_alert_age_seconds,
                   rate_limit_per_minute, created_at, updated_at, last_used_at
            FROM tradingview_webhooks
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["allowed_symbols"] = _load(item.pop("allowed_symbols_json"), None)
        item["allowed_actions"] = _load(item.pop("allowed_actions_json"), [])
        out.append(item)
    return out


def list_alerts(db: DB, *, limit: int = 100) -> list[dict[str, Any]]:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM tradingview_alerts
            ORDER BY received_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def list_decisions(db: DB, *, limit: int = 100) -> list[dict[str, Any]]:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM tradingview_signal_decisions
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["normalized_signal"] = _load(item.get("normalized_signal_json"), {})
        out.append(item)
    return out


def list_external_signal_queue(db: DB, *, limit: int = 100) -> list[dict[str, Any]]:
    ensure_tradingview_schema(db)
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM external_signal_queue
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["result_json"] = _load(item.get("result"), None)
        out.append(item)
    return out


@dataclass(frozen=True)
class TradingViewWebhookSeed:
    webhook_id: str
    token: str
