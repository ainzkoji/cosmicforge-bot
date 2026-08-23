"""
Event Broadcaster - Real-time event distribution

Provides in-memory pub/sub for streaming events to SSE clients.
Integrates with existing EventStore to broadcast trade events.
"""
from __future__ import annotations
import asyncio
import uuid
from typing import Dict, Optional
from dataclasses import dataclass
import threading

from shared_lib.persistence.events import Event, EventType


@dataclass
class EventListener:
    """Represents a single SSE client listener."""
    listener_id: str
    queue: asyncio.Queue
    event_filter: Optional[set] = None  # None = all events
    
    def should_receive(self, event: Event) -> bool:
        """Check if this listener should receive the event."""
        if self.event_filter is None:
            return True
        return event.event_type in self.event_filter


class EventBroadcaster:
    """
    In-memory event broadcaster for real-time updates.
    
    Thread-safe singleton that distributes events to SSE clients.
    Events are sent to asyncio.Queue instances that SSE endpoints read from.
    """
    
    _instance: Optional['EventBroadcaster'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._listeners: Dict[str, EventListener] = {}
                    cls._instance._listeners_lock = threading.Lock()
        return cls._instance
    
    def subscribe(
        self,
        listener_id: Optional[str] = None,
        event_filter: Optional[set] = None
    ) -> tuple[str, asyncio.Queue]:
        """
        Subscribe to events.
        
        Args:
            listener_id: Optional ID (auto-generated if None)
            event_filter: Set of EventTypes to receive (None = all events)
            
        Returns:
            Tuple of (listener_id, queue) where queue yields events
        """
        if listener_id is None:
            listener_id = f"listener_{uuid.uuid4().hex[:8]}"
        
        # Create queue in a thread-safe manner
        queue = asyncio.Queue(maxsize=100)  # Prevent memory bloat
        
        listener = EventListener(
            listener_id=listener_id,
            queue=queue,
            event_filter=event_filter
        )
        
        with self._listeners_lock:
            self._listeners[listener_id] = listener
        
        return listener_id, queue
    
    def unsubscribe(self, listener_id: str):
        """Remove a listener."""
        with self._listeners_lock:
            if listener_id in self._listeners:
                listener = self._listeners.pop(listener_id)
                # Clear queue to free memory
                while not listener.queue.empty():
                    try:
                        listener.queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
    
    def broadcast(self, event: Event):
        """
        Send event to all subscribed listeners.
        
        This is called from sync code (EventStore.emit) so we need
        to handle async queues carefully.
        
        Args:
            event: Event to broadcast
        """
        with self._listeners_lock:
            listeners = list(self._listeners.values())
        
        for listener in listeners:
            if listener.should_receive(event):
                # Put in queue (non-blocking)
                try:
                    listener.queue.put_nowait(event)
                except asyncio.QueueFull:
                    # Queue full - drop event to prevent memory issues
                    # In production, consider logging this
                    pass
    
    def get_listener_count(self) -> int:
        """Get current number of active listeners."""
        with self._listeners_lock:
            return len(self._listeners)


# Global instance
_broadcaster: Optional[EventBroadcaster] = None


def get_event_broadcaster() -> EventBroadcaster:
    """Get or create the event broadcaster singleton."""
    global _broadcaster
    if _broadcaster is None:
        _broadcaster = EventBroadcaster()
    return _broadcaster
