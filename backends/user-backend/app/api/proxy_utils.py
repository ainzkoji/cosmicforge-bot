"""
Proxy Utilities

Reusable utilities for proxying requests from user-backend to bot-backend.
Provides a standardized way to forward HTTP requests with proper error handling.
"""
from fastapi import HTTPException, Request, Response
from typing import Any, Dict, Optional
import httpx
import logging
import os

logger = logging.getLogger(__name__)

# Bot-backend service URL from environment or default
BOT_BACKEND_BASE_URL = os.getenv("BOT_BACKEND_URL", "http://127.0.0.1:9000")


# Global HTTP client for connection pooling and avoiding ephemeral port exhaustion
# Set trust_env=False to ensure local system proxies do not interfere with 127.0.0.1
_proxy_client = httpx.AsyncClient(trust_env=False)

async def proxy_request(
    request: Request,
    target_path: str,
    method: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0
) -> Response:
    """
    Forward an HTTP request to the bot-backend service.
    
    Args:
        request: The incoming FastAPI Request object
        target_path: The path to forward to on bot-backend (e.g., "/api/v1/bot-instances")
        method: HTTP method to use. If None, uses the request's method
        params: Query parameters to include. If None, uses request's query params
        json_body: JSON body to send. If None, attempts to read from request
        timeout: Request timeout in seconds (default: 10.0)
    
    Returns:
        FastAPI Response object with status code, headers, and body from bot-backend
        
    Raises:
        HTTPException: 502 Bad Gateway if connection fails, or the status code from bot-backend
    """
    try:
        # Determine HTTP method
        http_method = (method or request.method).upper()
        
        # Build target URL
        target_url = f"{BOT_BACKEND_BASE_URL}{target_path}"
        
        # Forward Authorization header
        headers = {}
        auth_header = request.headers.get("authorization")
        if auth_header:
            headers["Authorization"] = auth_header
        
        # Use provided params or extract from request
        query_params = params if params is not None else dict(request.query_params)
        
        # Prepare request body for methods that support it
        request_body = None
        if http_method in ["POST", "PUT", "PATCH"]:
            if json_body is not None:
                request_body = json_body
            else:
                # Try to read from request
                try:
                    request_body = await request.json()
                except Exception:
                    # No body or invalid JSON - that's okay
                    pass
        
        # Make the proxied request using the global client
        response = await _proxy_client.request(
            method=http_method,
            url=target_url,
            params=query_params,
            json=request_body,
            headers=headers,
            timeout=timeout
        )
        
        # Return response with same status code and body
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.headers.get("content-type")
        )
    
    except httpx.TimeoutException as e:
        logger.error(f"Timeout connecting to bot-backend at {target_url}: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Bot service timeout: request to {target_path} took too long"
        )
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend at {target_url}: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Bot service unavailable: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected proxy error for {target_url}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Proxy error: {str(e)}"
        )
