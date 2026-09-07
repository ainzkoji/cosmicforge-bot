"""
Structured runner initialization diagnostics.

An Auto Pilot runner must be atomic: either every collaborator (context,
policy, strategy, orchestrator, executor, position manager) is constructed
successfully, or construction fails loudly with a machine-readable reason.

The pre-existing failure mode this module removes was a broad
``except Exception: print(...)`` inside ``PaperRunner._load_orchestrator``.
A single ``AttributeError`` there left ``self.orchestrator = None`` while the
runner stayed registered and "operational", so every symbol on every 10-second
tick produced ``ERROR_STRATEGY_UNAVAILABLE`` forever with no persisted cause.
"""
from __future__ import annotations

import traceback
from typing import Any, Optional


class RunnerInitializationError(RuntimeError):
    """Raised when a per-bot runner cannot be fully constructed.

    Carries a stable ``reason_code`` so bot health, decision evidence and the
    operator-facing API all describe the same failure, plus a redacted cause
    summary (type + message) that is safe to persist.
    """

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        cause: Optional[BaseException] = None,
        bot_instance_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.message = message
        self.cause = cause
        self.bot_instance_id = bot_instance_id
        self.cause_type = type(cause).__name__ if cause is not None else None
        self.cause_message = str(cause) if cause is not None else None
        self.cause_traceback = (
            "".join(traceback.format_exception(type(cause), cause, cause.__traceback__))
            if cause is not None
            else None
        )

    # Reason codes -------------------------------------------------------
    RUNNER_INITIALIZATION_FAILED = "RUNNER_INITIALIZATION_FAILED"
    EFFECTIVE_POLICY_INVALID = "EFFECTIVE_POLICY_INVALID"
    STRATEGY_INITIALIZATION_FAILED = "STRATEGY_INITIALIZATION_FAILED"
    ORCHESTRATOR_INITIALIZATION_FAILED = "ORCHESTRATOR_INITIALIZATION_FAILED"
    BROKER_INITIALIZATION_FAILED = "BROKER_INITIALIZATION_FAILED"
    POSITION_REHYDRATION_FAILED = "POSITION_REHYDRATION_FAILED"
    RUNTIME_CONTEXT_INVALID = "RUNTIME_CONTEXT_INVALID"

    def structured_detail(self) -> dict[str, Any]:
        """Operator-facing payload. Contains no credentials or raw config values."""
        return {
            "reason_code": self.reason_code,
            "message": self.message,
            "bot_instance_id": self.bot_instance_id,
            "cause_type": self.cause_type,
            "cause_message": self.cause_message,
        }

    def __str__(self) -> str:  # pragma: no cover - formatting only
        if self.cause_type:
            return f"{self.reason_code}: {self.message} ({self.cause_type}: {self.cause_message})"
        return f"{self.reason_code}: {self.message}"
