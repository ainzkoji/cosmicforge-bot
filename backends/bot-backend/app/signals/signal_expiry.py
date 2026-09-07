from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from shared_lib.persistence.db import DB, utc_now_iso

logger = logging.getLogger(__name__)
from shared_lib.persistence.signals import (
    PERFORMANCE_RESULT_EXPIRED,
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_PENDING_ENTRY,
    create_signal_performance,
    ensure_signals_schema,
    get_signal_performance,
    update_signal_performance,
    update_trading_signal,
)


def parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def ensure_signal_performance(signal: dict[str, Any], db: DB) -> dict[str, Any]:
    existing = get_signal_performance(signal["id"], db=db)
    if existing:
        return existing
    create_signal_performance(
        {
            "signal_id": signal["id"],
            "asset_class": signal["asset_class"],
            "symbol": signal["symbol"],
            "side": signal["side"],
        },
        db=db,
    )
    return get_signal_performance(signal["id"], db=db) or {}


class SignalExpiryUpdater:
    def __init__(self, db: DB | None = None):
        self.db = db or DB()
        ensure_signals_schema(self.db)

    def expire_due_signals(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        summary = {"checked": 0, "expired": 0, "skipped_missing_expiry": 0, "errors": []}
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM trading_signals
                WHERE is_published = 1
                  AND status IN (?, ?)
                ORDER BY expires_at ASC
                """,
                (SIGNAL_STATUS_PENDING_ENTRY, SIGNAL_STATUS_ACTIVE),
            ).fetchall()
        for row in rows:
            signal = dict(row)
            summary["checked"] += 1
            expires_at = parse_utc(signal.get("expires_at"))
            if not expires_at:
                summary["skipped_missing_expiry"] += 1
                summary["errors"].append({"signal_id": signal["id"], "reason": "MISSING_EXPIRY"})
                continue
            if expires_at > now:
                continue
            try:
                self._mark_expired(signal)
                summary["expired"] += 1
            except Exception as exc:
                summary["errors"].append({"signal_id": signal["id"], "reason": str(exc)})
        return summary

    def _mark_expired(self, signal: dict[str, Any]) -> None:
        now = utc_now_iso()
        ensure_signal_performance(signal, self.db)
        update_signal_performance(
            signal["id"],
            {"expired": 1, "result": PERFORMANCE_RESULT_EXPIRED, "closed_at": now},
            db=self.db,
        )
        update_trading_signal(signal["id"], {"status": SIGNAL_STATUS_EXPIRED}, db=self.db)


def expire_due_signals(db: DB | None = None, now: datetime | None = None) -> dict[str, Any]:
    return SignalExpiryUpdater(db=db).expire_due_signals(now=now)


def expire_stale_signals(db: DB | None = None) -> None:
    """Scheduler-facing wrapper: expire PENDING_ENTRY/ACTIVE signals past their expires_at."""
    logger.info("[SIGNAL_EXPIRY_STARTED] Running stale signal expiry")
    try:
        summary = expire_due_signals(db=db)
        logger.info(
            "[SIGNAL_EXPIRY_COMPLETED] checked=%d expired=%d skipped_missing_expiry=%d errors=%d",
            summary.get("checked", 0),
            summary.get("expired", 0),
            summary.get("skipped_missing_expiry", 0),
            len(summary.get("errors", [])),
        )
        for err in summary.get("errors", []):
            logger.warning("[SIGNAL_EXPIRED] Error expiring signal_id=%s reason=%s", err.get("signal_id"), err.get("reason"))
    except Exception:
        logger.exception("[SIGNAL_EXPIRY_FAILED] Unhandled error during stale signal expiry")
