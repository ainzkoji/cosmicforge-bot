"""Structured CATI component-error evidence (pre-Section-17 closure, item 14).

CATI hooks stay fail-safe relative to V2 (they never raise into the runner),
but a swallowed failure must still leave evidence: one structured
``[CATI_COMPONENT_ERROR]`` JSON log line per failure with the component,
exception class, a SANITIZED message and the tenant/cycle identifiers.

Sanitization drops anything that looks like a credential (key=..., token,
secret, signature, bearer headers, long hex/base64 blobs) and truncates. The
exception's repr/args are never logged raw.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import Deque, List, Optional

logger = logging.getLogger("app.trading_intelligence.errors")

MAX_MESSAGE_CHARS = 300
_RECENT: Deque["ComponentErrorRecord"] = deque(maxlen=200)
_LOCK = threading.Lock()

_SECRET_PATTERNS = (  # order matters: bearer/JWT before the generic key=value rule
    re.compile(r"(?i)\bbearer\s+\S+"),
    re.compile(r"\beyJ[\w-]+\.[\w-]+\.[\w-]+"),
    re.compile(r"(?i)\b(api[_-]?key|api[_-]?secret|secret|token|password|passwd|signature|sig|auth|authorization|private[_-]?key|listenkey)\b\s*[:=]\s*\S+"),
    re.compile(r"(?i)(signature|apikey|api_key|timestamp|recvwindow)=[^&\s]+"),
    re.compile(r"\b[A-Fa-f0-9]{32,}\b"),
    re.compile(r"\b[A-Za-z0-9+/_-]{40,}={0,2}"),
)


def sanitize_message(message: object) -> str:
    text = str(message if message is not None else "")
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    text = " ".join(text.split())
    return text[:MAX_MESSAGE_CHARS] + ("..." if len(text) > MAX_MESSAGE_CHARS else "")


@dataclass(frozen=True)
class ComponentErrorRecord:
    reason_code: str
    component: str
    exception_class: str
    message: str
    cycle_id: Optional[str]
    bot_instance_id: Optional[str]
    broker_account_id: Optional[str]
    symbol: Optional[str]
    observed_at: int


def record_component_error(
    component: str, exc: BaseException, *, cycle_id: Optional[str] = None, bot_instance_id: Optional[str] = None,
    broker_account_id: Optional[str] = None, symbol: Optional[str] = None,
) -> ComponentErrorRecord:
    """Log + retain one structured record. Never raises."""
    try:
        rec = ComponentErrorRecord(
            reason_code="CATI_COMPONENT_ERROR", component=str(component), exception_class=type(exc).__name__,
            message=sanitize_message(exc), cycle_id=None if cycle_id is None else str(cycle_id),
            bot_instance_id=bot_instance_id, broker_account_id=broker_account_id, symbol=symbol,
            observed_at=int(time.time() * 1000),
        )
        with _LOCK:
            _RECENT.append(rec)
        logger.error("[CATI_COMPONENT_ERROR] %s", json.dumps(asdict(rec), sort_keys=True))
        return rec
    except Exception:  # pragma: no cover - evidence must never break the hook
        return ComponentErrorRecord("CATI_COMPONENT_ERROR", str(component), "Unknown", "", None, None, None, None, 0)


def recent_component_errors() -> List[ComponentErrorRecord]:
    with _LOCK:
        return list(_RECENT)


def clear_component_errors() -> None:
    with _LOCK:
        _RECENT.clear()


__all__ = ["sanitize_message", "ComponentErrorRecord", "record_component_error", "recent_component_errors",
           "clear_component_errors", "MAX_MESSAGE_CHARS"]
