from typing import Callable, Any, Dict, List
import logging

logger = logging.getLogger(__name__)

class EventHooks:
    """
    Lightweight internal hook system to decouple Audit from Notification Dispatcher.
    """
    _listeners: List[Callable[[Dict[str, Any]], None]] = []

    @classmethod
    def register(cls, listener: Callable[[Dict[str, Any]], None]):
        """Register a new listener function."""
        if listener not in cls._listeners:
            cls._listeners.append(listener)
            logger.info(f"EventHooks: Registered listener {listener.__name__}")

    @classmethod
    def dispatch(cls, event_payload: Dict[str, Any]):
        """
        Dispatch event to all listeners. 
        Swallows exceptions to prevent impacting the caller (Trading Engine).
        """
        for listener in cls._listeners:
            try:
                listener(event_payload)
            except Exception as e:
                logger.error(f"EventHooks: Listener {listener.__name__} failed: {e}", exc_info=True)
