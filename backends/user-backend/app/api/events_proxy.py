"""
Events Proxy API

Proxies SSE (Server-Sent Events) stream from frontend to bot-backend service.

Note: EventSource API doesn't support custom headers, so JWT token is sent as query param.
"""
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import StreamingResponse
import httpx
import logging

from app.core.security import decode_token
from app.core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

# Bot-backend service URL (internal service-to-service)
BOT_BACKEND_URL = getattr(settings, 'BOT_BACKEND_URL', 'http://127.0.0.1:9000')


@router.get("/stream")
async def stream_events(
    request: Request,
    token: str = Query(..., description="JWT access token (EventSource doesn't support headers)")
):
    """
    Proxy SSE stream from bot-backend.
    
    **Authentication**: Token must be passed as query parameter because 
    EventSource API doesn't support custom headers.
    
    This endpoint:
    1. Validates JWT token from query param
    2. Converts to Authorization header
    3. Proxies SSE stream from bot-backend
    """
    # Validate token (EventSource can't send headers, so token comes from query param)
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        logger.warning(f"SSE auth failed: Invalid token type or payload")
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
    try:
        # Convert query param token to Authorization header for bot-backend
        headers = {"Authorization": f"Bearer {token}"}
        
        # Create streaming connection to bot-backend
        async def event_stream():
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream(
                    "GET",
                    f"{BOT_BACKEND_URL}/api/v1/events/stream",
                    headers=headers
                ) as response:
                    # Check if bot-backend rejected auth
                    if response.status_code == 401:
                        yield f"event: error\ndata: {{\"message\": \"Authentication failed\"}}\n\n"
                        return
                    
                    try:
                        async for chunk in response.aiter_bytes():
                            yield chunk
                    except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ReadError) as exc:
                        # Log as warning for transient network issues, don't crash stream
                        logger.warning(f"SSE stream interrupted: {exc}")
                    except Exception as exc:
                        logger.error(f"SSE stream error: {exc}")
        
        # Return as streaming response with SSE headers
        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering
            }
        )
        
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend SSE stream: {e}")
        # Return a simple error event
        async def error_stream():
            yield f"event: error\ndata: {{\"message\": \"Bot service unavailable\"}}\n\n"
        
        return StreamingResponse(
            error_stream(),
            media_type="text/event-stream"
        )
