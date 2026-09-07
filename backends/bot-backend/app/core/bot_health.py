"""
Canonical bot health-state semantics.

Separation of concerns:

  * ``bot_health_status`` / ``bot_health_reason_code`` / ``bot_health_message``
    are DERIVED CURRENT STATE.  They describe the bot right now.
  * ``last_error`` is the CURRENT error.  It must agree with the current health
    state -- it is NOT a historical archive.  Keeping it after the bot moved on
    produced the observed contradiction where health said
    ERROR_STRATEGY_UNAVAILABLE while last_error still said
    "Auto Pilot capital budget is missing".
  * ``bot_system_events`` is the append-only history.  Nothing is ever deleted
    there, so clearing ``last_error`` never destroys evidence.
"""
from __future__ import annotations

#: Health statuses that mean "the bot is currently in an error condition".
ERROR_HEALTH_STATUSES = frozenset({
    "ERROR",
    "ERROR_CONFIGURATION",
    "ERROR_INITIALIZATION",
    "BROKER_AUTH_FAILED",
    "BROKER_BLOCKED",
    "QUARANTINED",
    "FAILED_INITIALIZATION",
})


def is_error_health_status(status: str | None) -> bool:
    """True when ``status`` represents a current error condition."""
    if not status:
        return False
    normalized = str(status).strip().upper()
    return normalized in ERROR_HEALTH_STATUSES or normalized.startswith("ERROR")


def resolve_last_error(
    *,
    status: str | None,
    message: str | None = None,
    reason_code: str | None = None,
    explicit_last_error: str | None = None,
) -> str | None:
    """Return the value ``last_error`` must hold for this health state.

    ``None`` means "clear the column" -- the bot is not currently in error.
    History is preserved separately in ``bot_system_events``.
    """
    if not is_error_health_status(status):
        return None
    if explicit_last_error:
        return explicit_last_error
    if message:
        return message
    if reason_code:
        return str(reason_code)
    return str(status)
