"""Public track-record proxy (Section H, Steps 6.4 / 8.6). No authentication: the data is public and aggregated.

    GET /api/v1/public/track-record                      ->  bot-backend /api/v1/public/track-record
    GET /api/v1/public/track-record?series_id=<id>       ->  bot-backend /api/v1/public/track-record?series_id=<id>

No credentials are forwarded and no query parameter other than a validated ``series_id``.
"""
from __future__ import annotations

import re
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Response

from app.api.proxy_utils import BOT_BACKEND_BASE_URL

router = APIRouter(prefix="/api/v1/public/track-record", tags=["Public Track Record"])

_SERIES_ID = re.compile(r"^[a-z0-9._-]{1,200}$")
_TIMEOUT_S = 10.0


async def _forward(path: str, params: Optional[dict] = None) -> Response:
    try:
        async with httpx.AsyncClient(trust_env=False, timeout=_TIMEOUT_S) as client:
            upstream = await client.get(f"{BOT_BACKEND_BASE_URL}{path}", params=params)
    except httpx.HTTPError:
        raise HTTPException(status_code=502, detail="TRACK_RECORD_UPSTREAM_UNAVAILABLE")
    return Response(content=upstream.content, status_code=upstream.status_code,
                    media_type=upstream.headers.get("content-type", "application/json"))


@router.get("")
async def track_record(series_id: Optional[str] = Query(default=None, max_length=200)) -> Response:
    if series_id is None:
        return await _forward("/api/v1/public/track-record")
    if not _SERIES_ID.match(series_id):
        raise HTTPException(status_code=404, detail="TRACK_RECORD_SERIES_NOT_PUBLISHED")
    return await _forward("/api/v1/public/track-record", {"series_id": series_id})
