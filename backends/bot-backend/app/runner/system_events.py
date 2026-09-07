"""
Append-only bot/runtime system events.

``bot_instances`` health columns hold DERIVED CURRENT STATE and are overwritten.
This module writes the durable history alongside them so an operator can answer
"what happened to this bot, and when" without reading console output or the
76 MB live_audit.jsonl.

Writes here are best-effort: evidence recording must never take down a runner.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

logger = logging.getLogger(__name__)

_SENSITIVE_KEYS = {
    "api_key", "api_secret", "secret", "password", "token",
    "bridge_token", "authorization", "private_key",
}


def _redact(details: Mapping[str, Any] | None) -> dict[str, Any]:
    if not details:
        return {}
    out: dict[str, Any] = {}
    for key, value in details.items():
        if str(key).lower() in _SENSITIVE_KEYS:
            out[key] = "***redacted***"
        elif isinstance(value, Mapping):
            out[key] = _redact(value)
        else:
            out[key] = value
    return out


def record_bot_system_event(
    db: Any,
    *,
    bot_instance_id: Optional[str],
    event_type: str,
    message: str = "",
    user_id: Optional[str] = None,
    run_id: Optional[str] = None,
    severity: str = "INFO",
    reason_code: Optional[str] = None,
    details: Mapping[str, Any] | None = None,
    provenance: str = "RUNTIME",
) -> Optional[str]:
    """Persist one append-only system event. Returns the event id, or None on failure."""
    event_id = uuid.uuid4().hex
    try:
        payload = json.dumps(_redact(details), sort_keys=True, default=str)
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO bot_system_events (
                    event_id, bot_instance_id, user_id, run_id, event_type,
                    severity, reason_code, message, details_json, provenance, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_id, bot_instance_id, user_id, run_id, event_type,
                    severity, reason_code, message, payload, provenance,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        return event_id
    except Exception as exc:  # pragma: no cover - evidence must never break trading
        logger.error("bot_system_event_write_failed type=%s bot=%s err=%s",
                     event_type, bot_instance_id, exc)
        return None
