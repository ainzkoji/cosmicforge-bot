import asyncio
import logging
import uuid
from datetime import datetime, timezone, timedelta
import json
import traceback

from shared_lib.persistence.db import DB, utc_now_iso
from shared_lib.notifications.channels.email import EmailChannel
from shared_lib.notifications.channels.telegram import TelegramChannel
from shared_lib.notifications.channels.push import PushChannel

logger = logging.getLogger(__name__)

class NotificationWorker:
    """
    Background worker that polls for pending notification jobs and executes them.
    safe for SQLite (uses token claiming).
    """

    def __init__(self, db: DB):
        self.db = db
        self.running = False
        self._stop_event = asyncio.Event()

    async def start(self):
        self.running = True
        logger.info("NotificationWorker: Started.")
        while self.running:
            try:
                processed = await self.process_batch()
                if processed == 0:
                    # No jobs, sleep a bit
                    await asyncio.sleep(5)
                else:
                    # Had jobs, sleep less to churn through backlog
                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"NotificationWorker: Loop failed: {e}")
                traceback.print_exc()
                await asyncio.sleep(5)

    def stop(self):
        self.running = False
        logger.info("NotificationWorker: Stopping...")

    async def process_batch(self) -> int:
        # Runs in thread pool to avoid blocking async loop with DB/Network calls
        return await asyncio.to_thread(self._process_batch_sync)

    def _process_batch_sync(self) -> int:
        claim_token = str(uuid.uuid4())
        now = utc_now_iso()
        
        # 1. Claim Jobs (Atomic Update)
        with self.db.connect() as conn:
            # Mark up to 10 pending jobs as processing
            # We use a subquery to select IDs.
            # SQLite safe update with limit via subquery
            conn.execute(
                """
                UPDATE notification_jobs
                SET status = 'processing',
                    processing_token = ?,
                    processing_started_at = ?
                WHERE id IN (
                    SELECT id FROM notification_jobs
                    WHERE status = 'pending' 
                      AND (next_retry_at IS NULL OR next_retry_at <= ?)
                    LIMIT 10
                )
                """,
                (claim_token, now, now)
            )
            
            # 2. Fetch Claimed Jobs
            rows = conn.execute(
                "SELECT * FROM notification_jobs WHERE processing_token = ?",
                (claim_token,)
            ).fetchall()
        
        if not rows:
            return 0
            
        logger.info(f"NotificationWorker: Claimed {len(rows)} jobs.")
        
        for row in rows:
            self._execute_job(row)
            
        return len(rows)

    def _execute_job(self, row):
        job_id = row["id"]
        channel = row["channel"]
        recipient = row["recipient"]
        template = row["template_id"]
        status = "failed"
        error = None
        
        # Resolve Recipient if missing (join logic in future, for now mostly pre-filled)
        if not recipient:
             # Try to resolve from endpoints table if user_id present
             recipient = self._resolve_recipient(row["user_id"], channel)
        
        success = False
        if not recipient:
            error = "No recipient found"
        else:
            try:
                if channel == "email":
                    success = EmailChannel.send(
                        recipient, 
                        row["subject"] or "Notification", 
                        row["body_html"] or row["body_text"] or "No content",
                        row["body_text"]
                    )
                elif channel == "telegram":
                    success = TelegramChannel.send(
                        recipient,
                        row["body_text"] or row["body_html"] or "Notification"
                    )
                elif channel == "push":
                    metadata = json.loads(row["metadata_json"] or "{}")
                    success = PushChannel.send(
                        recipient,
                        row["subject"] or "Notification",
                        row["body_text"] or row["body_html"] or "New Alert",
                        metadata
                    )
                    
                    # Mark token as invalid if Firebase reports it's unregistered
                    if not success and error:
                        error_lower = error.lower()
                        if any(keyword in error_lower for keyword in ['unregistered', 'invalid', 'notregistered']):
                            self._mark_token_invalid(row["user_id"], recipient)
                else:
                    error = f"Unknown channel {channel}"
            except Exception as e:
                error = str(e)

        # Update Job Status
        with self.db.connect() as conn:
            now = utc_now_iso()
            if success:
                conn.execute(
                    "UPDATE notification_jobs SET status='sent', updated_at=? WHERE id=?",
                    (now, job_id)
                )
            else:
                # Retry logic
                attempts = row["attempts"] + 1
                if attempts >= 5:
                     status = 'dead'
                     retry_at = None
                else:
                     status = 'pending' # Release to pending for retry
                     # Exponential backoff: 30s, 60s, 120s, etc
                     backoff_seconds = 30 * (2 ** (attempts - 1))
                     # We need to calc next_retry_at using python datetime math
                     # Since we are in sync method, this is fine
                     retry_dt = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
                     retry_at = retry_dt.isoformat()

                conn.execute(
                    """
                    UPDATE notification_jobs 
                    SET status=?, attempts=?, last_error=?, next_retry_at=?, processing_token=NULL, updated_at=?
                    WHERE id=?
                    """,
                    (status, attempts, error or "Send Failed", retry_at, now, job_id)
                )
    
    def _mark_token_invalid(self, user_id: str, token: str):
        """Mark a push token as invalid in the database."""
        try:
            with self.db.connect() as conn:
                conn.execute(
                    """
                    UPDATE notification_endpoints
                    SET status = 'invalid', updated_at = ?
                    WHERE user_id = ? AND channel = 'push' AND recipient = ?
                    """,
                    (utc_now_iso(), user_id, token)
                )
            logger.info(f"Marked invalid push token for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to mark token as invalid: {e}")


    def _resolve_recipient(self, user_id, channel):
        if not user_id: return None
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT recipient FROM notification_endpoints WHERE user_id=? AND channel=? AND status='active'",
                (user_id, channel)
            ).fetchone()
            return row["recipient"] if row else None
