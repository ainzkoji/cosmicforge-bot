"""Public track record (Section H, Steps 6.4 / 8.6). Read-only, no authentication.

    GET /api/v1/public/track-record                       every source kind's published series (summaries)
    GET /api/v1/public/track-record?series_id=<series_id>  one published series document

One path (with a query parameter for the detail) so the public allow-list in
``tests/test_api_auth_hardening.py`` stays a single entry.

Serves only documents stored by ``app.golive.track_record.publish_series`` (aggregated, minimum-count
suppressed, scanned for identifying fields). It never reads the daily ledger rows into a response. With no
published live series the live answer is ``{state: "NOT_YET_AVAILABLE", reason: "No authorized live trading has
occurred"}``. No live trading has happened, so that is the answer today.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.golive import track_record as tr

router = APIRouter(prefix="/api/v1/public/track-record", tags=["Public Track Record"])

_lock = threading.Lock()
_db: Any = None
_db_resolved = False


def get_track_record_db() -> Optional[Any]:
    """The platform database (``PLATFORM_DATABASE_URL``), or ``None`` when none is configured."""
    global _db, _db_resolved
    with _lock:
        if not _db_resolved:
            try:
                from shared_lib.persistence.sql import Database

                _db = Database.from_env(pool_size=2)
            except Exception:
                _db = None
            _db_resolved = True
        return _db


@router.get("")
def track_record(series_id: Optional[str] = Query(default=None, max_length=200),
                 db: Optional[Any] = Depends(get_track_record_db)) -> Dict[str, Any]:
    if series_id is None:
        return tr.public_index(db)
    doc = tr.public_series(db, series_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="TRACK_RECORD_SERIES_NOT_PUBLISHED")
    return doc
