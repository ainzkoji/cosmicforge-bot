from __future__ import annotations
from enum import Enum
import time
from collections import deque
from typing import List

class CircuitState(Enum):
    NORMAL = "NORMAL"
    DEGRADED = "DEGRADED" # Warning level
    HALTED = "HALTED"     # Blocking level
    RECOVERY = "RECOVERY" # Cooldown/Probing

class ExchangeCircuitBreaker:
    def __init__(self, error_limit: int = 5, window_seconds: int = 60, timeout_seconds: int = 300):
        self.error_limit = error_limit
        self.window_seconds = window_seconds
        self.timeout_seconds = timeout_seconds
        self.errors = deque()
        self.last_trip_time = 0
        self.state = CircuitState.NORMAL

    def record_error(self):
        now = time.time()
        self.errors.append(now)
        self._cleanup(now)
        self._evaluate_state(now)

    def _cleanup(self, now):
        while self.errors and (now - self.errors[0] > self.window_seconds):
            self.errors.popleft()

    def _evaluate_state(self, now):
        count = len(self.errors)
        if count >= self.error_limit:
            if self.state != CircuitState.HALTED:
                self.state = CircuitState.HALTED
                self.last_trip_time = now
        elif count >= (self.error_limit / 2):
            if self.state == CircuitState.NORMAL:
                self.state = CircuitState.DEGRADED
        else:
             if self.state == CircuitState.DEGRADED:
                 self.state = CircuitState.NORMAL
             # If HALTED, we wait for timeout (handled in is_tripped or similar)

    def get_state(self) -> CircuitState:
        now = time.time()
        # Auto-recover from HALTED if timeout passed
        if self.state == CircuitState.HALTED:
            if (now - self.last_trip_time) > self.timeout_seconds:
                self.state = CircuitState.RECOVERY
                # In recovery, we might transition to NORMAL immediately or wait for success?
                # For simplicity, go back to NORMAL (or DEGRADED if errors persist in window? 
                # errors should be cleared or window moved. errors are timestamps.)
                # If window < timeout, errors deque might be empty by now.
                self._cleanup(now)
                if not self.errors:
                    self.state = CircuitState.NORMAL
                else:
                    self.state = CircuitState.DEGRADED
        return self.state

    def is_tripped(self) -> bool:
        return self.get_state() == CircuitState.HALTED

    def reset(self):
        self.errors.clear()
        self.state = CircuitState.NORMAL


class CircuitBreakerRegistry:
    """
    Universal registry for per-broker circuit breakers.
    
    Works for any market (crypto, forex, stocks) and any broker.
    Broker IDs are dynamic - no hardcoded names.
    
    Usage:
        registry = get_circuit_registry()
        registry.record_error("BINANCE")  # Record error for specific broker
        if registry.is_tripped("BINANCE"):
            # Block trades for this broker only
    """
    
    _instance: 'CircuitBreakerRegistry | None' = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._breakers = {}
            cls._instance._lock = None
        return cls._instance
    
    def __init__(self):
        import threading
        if self._lock is None:
            self._lock = threading.RLock()
    
    def get_breaker(
        self,
        broker_id: str,
        error_limit: int = 5,
        window_seconds: int = 60,
        timeout_seconds: int = 300,
    ) -> ExchangeCircuitBreaker:
        """Get or create circuit breaker for a broker."""
        with self._lock:
            if broker_id not in self._breakers:
                self._breakers[broker_id] = ExchangeCircuitBreaker(
                    error_limit=error_limit,
                    window_seconds=window_seconds,
                    timeout_seconds=timeout_seconds,
                )
            return self._breakers[broker_id]
    
    def record_error(self, broker_id: str):
        """Record an error for a specific broker."""
        self.get_breaker(broker_id).record_error()
    
    def is_tripped(self, broker_id: str) -> bool:
        """Check if a specific broker's circuit breaker is tripped."""
        if broker_id not in self._breakers:
            return False  # Unknown broker = not tripped
        return self._breakers[broker_id].is_tripped()
    
    def get_state(self, broker_id: str) -> CircuitState:
        """Get state for a specific broker."""
        return self.get_breaker(broker_id).get_state()
    
    def reset(self, broker_id: str):
        """Reset a specific broker's circuit breaker."""
        if broker_id in self._breakers:
            self._breakers[broker_id].reset()
    
    def reset_all(self):
        """Reset all circuit breakers."""
        with self._lock:
            for breaker in self._breakers.values():
                breaker.reset()
    
    def get_all_states(self) -> dict:
        """Get states for all brokers."""
        with self._lock:
            return {
                broker_id: breaker.get_state().value
                for broker_id, breaker in self._breakers.items()
            }
    
    def list_brokers(self) -> list:
        """List all registered broker IDs."""
        with self._lock:
            return list(self._breakers.keys())


# Singleton accessor
_circuit_registry: CircuitBreakerRegistry | None = None


def get_circuit_registry() -> CircuitBreakerRegistry:
    """Get the global circuit breaker registry singleton."""
    global _circuit_registry
    if _circuit_registry is None:
        _circuit_registry = CircuitBreakerRegistry()
    return _circuit_registry

