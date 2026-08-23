from __future__ import annotations
from contextvars import ContextVar
from typing import Optional

# Context-local (safe for async & threads)
_current_run_id: ContextVar[Optional[str]] = ContextVar("current_run_id", default=None)
_current_cycle_id: ContextVar[Optional[str]] = ContextVar(
    "current_cycle_id", default=None
)


def set_run_id(run_id: str) -> None:
    _current_run_id.set(run_id)


def get_run_id() -> Optional[str]:
    return _current_run_id.get()


def clear_run_id() -> None:
    _current_run_id.set(None)


def set_cycle_id(cycle_id: str) -> None:
    _current_cycle_id.set(cycle_id)


def get_cycle_id() -> Optional[str]:
    return _current_cycle_id.get()


def clear_cycle_id() -> None:
    _current_cycle_id.set(None)
