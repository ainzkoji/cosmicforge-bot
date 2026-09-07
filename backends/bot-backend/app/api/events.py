"""
Events API - Real-time event streaming via Server-Sent Events (SSE)

Provides SSE endpoint for real-time analytics updates.
"""
from fastapi import APIRouter, Depends
from sse_starlette.sse import EventSourceResponse
import json
import asyncio
from typing import AsyncGenerator

from app.core.auth import get_current_user_id
from shared_lib.persistence.event_broadcaster import get_event_broadcaster
from shared_lib.persistence.events import EventType

router = APIRouter(tags=["Events"])

# Analytics-relevant events that trigger UI updates
ANALYTICS_EVENTS = {
    EventType.POSITION_OPENED,
    EventType.POSITION_CLOSED,
    EventType.TP1_HIT,
    EventType.ADD_FILLED,
}


@router.get("/stream")
async def stream_events(user_id: str = Depends(get_current_user_id)):
    """
    Server-Sent Events stream for real-time analytics updates.
    
    **Usage**:
    ```javascript
    const eventSource = new EventSource('/api/v1/events/stream', {
        headers: { 'Authorization': 'Bearer token' }
    });
    
    eventSource.addEventListener('POSITION_CLOSED', (e) => {
        const event = JSON.parse(e.data);
        console.log('Trade closed:', event.payload.realized_pnl);
        // Trigger analytics refetch
    });
    ```
    
    **Events Sent**:
    - `POSITION_OPENED`: New trade started
    - `POSITION_CLOSED`: Trade closed (includes P&L)
    - `TP1_HIT`: Take profit 1 hit
    - `ADD_FILLED`: Add to position filled
    
    **Auto-reconnect**: Browser EventSource API handles reconnection automatically.
    """
    
    async def event_generator() -> AsyncGenerator:
        """Generate SSE events from broadcaster."""
        # user_id is passed directly now
        listener_id = f"sse_{user_id}"
        
        # Subscribe to analytics-relevant events only
        _, queue = get_event_broadcaster().subscribe(
            listener_id=listener_id,
            event_filter=ANALYTICS_EVENTS
        )
        
        try:
            # Send initial connection confirmation
            yield {
                "event": "connected",
                "data": json.dumps({"status": "connected", "user_id": user_id})
            }
            
            # Stream events as they arrive
            while True:
                # Wait for event from queue (with timeout for keepalive)
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=10.0)
                    
                    # TODO: Filter by user_id once user isolation is implemented
                    # For now, send all events (single-tenant assumption)
                    
                    # Send SSE formatted message
                    yield {
                        "event": event.event_type.value,
                        "data": json.dumps({
                            "event_id": event.event_id,
                            "trade_id": event.trade_id,
                            "symbol": event.symbol,
                            "ts": event.ts,
                            "payload": event.payload
                        })
                    }
                    
                except asyncio.TimeoutError:
                    # Send keepalive comment to prevent connection timeout
                    yield {
                        "comment": "keepalive"
                    }
                    
        except asyncio.CancelledError:
            # Client disconnected
            pass
        finally:
            # Clean up listener
            get_event_broadcaster().unsubscribe(listener_id)
    
    return EventSourceResponse(event_generator())


@router.get("/stream/health")
async def stream_health():
    """
    Health check for SSE streaming infrastructure.
    
    Returns number of active listeners.
    """
    broadcaster = get_event_broadcaster()
    return {
        "status": "ok",
        "active_listeners": broadcaster.get_listener_count()
    }
