"""
Events API - Real-time event streaming via Server-Sent Events (SSE)

Provides SSE endpoint for real-time analytics updates.

Tenant isolation: the broadcaster is process-global and carries every user's
trade events, so each event is filtered per subscriber before it is sent:

* a subscriber only receives events whose owner is their own user id;
* admins receive every event;
* an event with no resolvable owner is delivered to admins only (fail closed).

The owner is resolved from the event itself (``user_id``), or from the bot
(``bot_instance_id`` / ``bot_id``) or broker account (``broker_account_id``)
it names, looked up in the database through a small cache. The payload sent
to the client is unchanged.
"""
from fastapi import APIRouter, Depends
from sse_starlette.sse import EventSourceResponse
import json
import asyncio
import logging
import threading
import time
import uuid
from typing import Any, AsyncGenerator, Dict, Optional, Tuple

from app.core.auth import get_current_user_id, oauth2_scheme
from app.core.security import decode_token
from shared_lib.persistence.db import DB
from shared_lib.persistence.event_broadcaster import get_event_broadcaster
from shared_lib.persistence.events import EventType

logger = logging.getLogger(__name__)

# Authentication is declared on the router so a route added later cannot be
# public by omission.
router = APIRouter(tags=["Events"], dependencies=[Depends(get_current_user_id)])

# Analytics-relevant events that trigger UI updates, plus the customer-facing
# production events of Step 1.8 (app.observability.user_events). Each is
# owner-filtered below like every other event.
ANALYTICS_EVENTS = {
    EventType.POSITION_OPENED,
    EventType.POSITION_CLOSED,
    EventType.TP1_HIT,
    EventType.ADD_FILLED,
    EventType.ENTRY_FILLED,
    EventType.EXIT_FILLED,
    EventType.POSITION_UNPROTECTED,
    EventType.BOT_PAUSED,
    EventType.BOT_RESUMED,
    EventType.BOT_STOPPED,
    EventType.DAILY_LOSS_PAUSE,
    EventType.ENTRY_BLOCKED,
}


# --- Event ownership -------------------------------------------------------

_OWNER_KEYS = ("user_id", "owner_user_id")
_BOT_KEYS = ("bot_instance_id", "bot_id")
_ACCOUNT_KEYS = ("broker_account_id",)

# (kind, id) -> (owner user id or None, expires_at monotonic)
_OWNER_CACHE: Dict[Tuple[str, str], Tuple[Optional[str], float]] = {}
_OWNER_CACHE_LOCK = threading.Lock()
_OWNER_CACHE_TTL_SECONDS = 300.0       # a bot/account never changes owner
_OWNER_CACHE_MISS_TTL_SECONDS = 15.0   # an unknown id may be created shortly after
_OWNER_CACHE_MAX_ENTRIES = 2048

_OWNER_QUERIES = {
    "bot": "SELECT user_id FROM bot_instances WHERE id = ?",
    "account": "SELECT user_id FROM broker_accounts WHERE id = ?",
}


def _lookup_owner(kind: str, entity_id: str) -> Optional[str]:
    """Owner user id of a bot instance / broker account (cached); None if unknown."""
    key = (kind, str(entity_id))
    now = time.monotonic()
    with _OWNER_CACHE_LOCK:
        cached = _OWNER_CACHE.get(key)
        if cached is not None and cached[1] > now:
            return cached[0]
    owner: Optional[str] = None
    try:
        with DB().connect() as conn:
            row = conn.execute(_OWNER_QUERIES[kind], (str(entity_id),)).fetchone()
        if row and row[0] is not None:
            owner = str(row[0])
    except Exception as exc:
        # Fail closed: an owner that cannot be verified is "no owner".
        logger.warning("[SSE] owner lookup failed kind=%s: %s", kind, exc)
        return None
    ttl = _OWNER_CACHE_TTL_SECONDS if owner is not None else _OWNER_CACHE_MISS_TTL_SECONDS
    with _OWNER_CACHE_LOCK:
        if len(_OWNER_CACHE) >= _OWNER_CACHE_MAX_ENTRIES:
            _OWNER_CACHE.clear()
        _OWNER_CACHE[key] = (owner, now + ttl)
    return owner


def _first_value(event: Any, payload: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[str]:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            value = getattr(event, key, None)
        if value not in (None, ""):
            return str(value)
    return None


def resolve_event_owner(event: Any) -> Optional[str]:
    """User id that owns ``event``, or None when it cannot be resolved."""
    payload = getattr(event, "payload", None)
    if not isinstance(payload, dict):
        payload = {}
    owner = _first_value(event, payload, _OWNER_KEYS)
    if owner is not None:
        return owner
    bot_id = _first_value(event, payload, _BOT_KEYS)
    if bot_id is not None:
        owner = _lookup_owner("bot", bot_id)
        if owner is not None:
            return owner
    account_id = _first_value(event, payload, _ACCOUNT_KEYS)
    if account_id is not None:
        return _lookup_owner("account", account_id)
    return None


def event_visible_to(event: Any, user_id: str, is_admin: bool) -> bool:
    """Whether a subscriber may receive ``event``.

    Admins receive everything. Everyone else receives only events they own;
    an event with no resolvable owner goes to admins only.
    """
    if is_admin:
        return True
    owner = resolve_event_owner(event)
    return owner is not None and owner == str(user_id)


def _caller_is_admin(token: str = Depends(oauth2_scheme)) -> bool:
    """True when the (already validated) access token carries the admin role."""
    payload = decode_token(token) or {}
    return payload.get("type") == "access" and payload.get("role") == "admin"


async def _event_stream(user_id: str, is_admin: bool) -> AsyncGenerator:
    """Generate SSE events from the broadcaster for ONE subscriber (tenant-filtered)."""
    # Unique per connection: a second tab from the same user must not
    # replace (and later unsubscribe) the first one's listener.
    listener_id = f"sse_{user_id}_{uuid.uuid4().hex[:12]}"
    
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
        last_sent = time.monotonic()
        while True:
            # Wait for event from queue (with timeout for keepalive)
            try:
                event = await asyncio.wait_for(queue.get(), timeout=10.0)

                # Tenant isolation: only the owner (or an admin) receives
                # the event; ownerless events go to admins only.
                if not event_visible_to(event, user_id, is_admin):
                    # Other tenants' events must not starve this
                    # subscriber's keepalive.
                    if time.monotonic() - last_sent >= 10.0:
                        last_sent = time.monotonic()
                        yield {
                            "comment": "keepalive"
                        }
                    continue

                last_sent = time.monotonic()
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
                last_sent = time.monotonic()
                yield {
                    "comment": "keepalive"
                }
                
    except asyncio.CancelledError:
        # Client disconnected
        pass
    finally:
        # Clean up listener
        get_event_broadcaster().unsubscribe(listener_id)


@router.get("/stream")
async def stream_events(
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(_caller_is_admin),
):
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
    
    return EventSourceResponse(_event_stream(user_id, is_admin))


@router.get("/stream/health")
async def stream_health(user_id: str = Depends(get_current_user_id)):
    """
    Health check for SSE streaming infrastructure (authenticated).
    
    Returns number of active listeners.
    """
    broadcaster = get_event_broadcaster()
    return {
        "status": "ok",
        "active_listeners": broadcaster.get_listener_count()
    }
