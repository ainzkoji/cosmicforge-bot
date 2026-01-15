"""
ID Generators - D Persistence System

Standardized IDs for:
- run_id: unique per bot session
- trade_id: unique per position lifecycle
- cycle_id: monotonic per evaluation tick
- request_id: correlation for exchange calls
"""
from __future__ import annotations
import uuid
from datetime import datetime
from typing import Optional
import threading


class IDGenerator:
    """Thread-safe ID generator singleton."""
    
    _instance: Optional['IDGenerator'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._cycle_counter = 0
                    cls._instance._current_run_id: Optional[str] = None
        return cls._instance
    
    def generate_run_id(self) -> str:
        """Generate unique run ID (UUIDv4)."""
        run_id = str(uuid.uuid4())
        self._current_run_id = run_id
        self._cycle_counter = 0  # Reset cycle counter for new run
        return run_id
    
    def get_current_run_id(self) -> Optional[str]:
        """Get the current run ID."""
        return self._current_run_id
    
    def generate_trade_id(self) -> str:
        """Generate unique trade ID (UUIDv4)."""
        return str(uuid.uuid4())
    
    def generate_cycle_id(self) -> int:
        """Generate monotonic cycle ID for ordering events within a run."""
        with self._lock:
            self._cycle_counter += 1
            return self._cycle_counter
    
    def generate_request_id(self) -> str:
        """Generate correlation ID for exchange requests."""
        return f"req_{uuid.uuid4().hex[:12]}"
    
    def generate_event_id(self) -> str:
        """Generate unique event ID."""
        return f"evt_{uuid.uuid4().hex[:16]}"


# Singleton access
_generator = IDGenerator()


def generate_run_id() -> str:
    """Generate a new run ID and set it as current."""
    return _generator.generate_run_id()


def get_current_run_id() -> Optional[str]:
    """Get the current run ID."""
    return _generator.get_current_run_id()


def generate_trade_id() -> str:
    """Generate a new trade ID."""
    return _generator.generate_trade_id()


def generate_cycle_id() -> int:
    """Generate next cycle ID (monotonic counter)."""
    return _generator.generate_cycle_id()


def generate_request_id() -> str:
    """Generate a correlation ID for exchange requests."""
    return _generator.generate_request_id()


def generate_event_id() -> str:
    """Generate a unique event ID."""
    return _generator.generate_event_id()


def set_run_id(run_id: str):
    """Manually set the current run ID (for resuming runs)."""
    _generator._current_run_id = run_id
