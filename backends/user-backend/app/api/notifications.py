import hmac
import logging
import secrets
import string
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from app.api.auth import get_current_active_user
from shared_lib.persistence.db import DB, utc_now_iso
import os
import requests
import json

router = APIRouter()
db = DB()
logger = logging.getLogger(__name__)

# ============================================================================
# Pydantic Models
# ============================================================================

class NotificationPreference(BaseModel):
    channel: str
    category: str
    is_enabled: bool
    min_severity: str = "INFO"

class TelegramLinkResponse(BaseModel):
    code: str
    bot_username: str
    instructions: str
    deep_link: str

class NotificationEndpoint(BaseModel):
    channel: str
    recipient: Optional[str] = None
    status: str
    verified_at: Optional[str] = None

class PushTokenRequest(BaseModel):
    token: str = Field(..., min_length=16, max_length=4096)

# ============================================================================
# Preferences
# ============================================================================

@router.get("/preferences")
def get_preferences(user: dict = Depends(get_current_active_user)):
    """Get user's notification preferences."""
    user_id = user["id"]
    
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT channel, category, is_enabled, min_severity FROM notification_preferences WHERE user_id=?",
            (user_id,)
        ).fetchall()
    
    return {"preferences": [dict(r) for r in rows]}

@router.put("/preferences")
def update_preferences(
    prefs: List[NotificationPreference],
    user: dict = Depends(get_current_active_user)
):
    """Update user's notification preferences."""
    user_id = user["id"]
    
    with db.connect() as conn:
        for pref in prefs:
            conn.execute(
                """
                INSERT OR REPLACE INTO notification_preferences 
                (user_id, channel, category, is_enabled, min_severity, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (user_id, pref.channel, pref.category, int(pref.is_enabled), pref.min_severity, utc_now_iso())
            )
    
    return {"status": "updated"}

# ============================================================================
# Endpoints
# ============================================================================

@router.get("/endpoints")
def get_endpoints(
    channel: Optional[str] = None,
    user: dict = Depends(get_current_active_user)
):
    """Get user's notification endpoints (email, telegram, etc)."""
    user_id = user["id"]
    
    with db.connect() as conn:
        if channel:
            rows = conn.execute(
                "SELECT channel, recipient, status, verified_at FROM notification_endpoints WHERE user_id=? AND channel=?",
                (user_id, channel)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT channel, recipient, status, verified_at FROM notification_endpoints WHERE user_id=?",
                (user_id,)
            ).fetchall()
    
    return {"endpoints": [dict(r) for r in rows]}

# ============================================================================
# Alerts (In-App)
# ============================================================================

@router.get("/alerts")
def get_alerts(
    limit: int = 50,
    unread_only: bool = False,
    user: dict = Depends(get_current_active_user)
):
    """Get user's in-app alerts."""
    user_id = user["id"]
    
    with db.connect() as conn:
        if unread_only:
            rows = conn.execute(
                """
                SELECT id, ts, alert_type, severity, symbol, message, acknowledged 
                FROM alerts 
                WHERE user_id=? AND acknowledged=0 
                ORDER BY ts DESC LIMIT ?
                """,
                (user_id, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, ts, alert_type, severity, symbol, message, acknowledged 
                FROM alerts 
                WHERE user_id=? 
                ORDER BY ts DESC LIMIT ?
                """,
                (user_id, limit)
            ).fetchall()
    
    return {"alerts": [dict(r) for r in rows], "count": len(rows)}

@router.post("/alerts/{alert_id}/acknowledge")
def acknowledge_alert(
    alert_id: int,
    user: dict = Depends(get_current_active_user)
):
    """Mark an alert as acknowledged."""
    user_id = user["id"]
    
    with db.connect() as conn:
        # Verify ownership
        row = conn.execute(
            "SELECT user_id FROM alerts WHERE id=?", (alert_id,)
        ).fetchone()
        
        if not row or row["user_id"] != user_id:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        conn.execute(
            "UPDATE alerts SET acknowledged=1, acknowledged_at=?, acknowledged_by=? WHERE id=?",
            (utc_now_iso(), user_id, alert_id)
        )
    
    return {"status": "acknowledged"}

@router.post("/alerts/acknowledge-all")
def acknowledge_all_alerts(user: dict = Depends(get_current_active_user)):
    """Mark all alerts as acknowledged."""
    user_id = user["id"]
    
    with db.connect() as conn:
        conn.execute(
            "UPDATE alerts SET acknowledged=1, acknowledged_at=?, acknowledged_by=? WHERE user_id=? AND acknowledged=0",
            (utc_now_iso(), user_id, user_id)
        )
    
    return {"status": "acknowledged_all"}

# ============================================================================
# Telegram Linking
# ============================================================================

@router.post("/telegram/link/start", response_model=TelegramLinkResponse)
def telegram_link_start(user: dict = Depends(get_current_active_user)):
    """Generate a one-time code for Telegram linking."""
    user_id = user["id"]
    
    # Generate 6-character code
    code = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(6))
    
    # Store with 10-minute expiry
    created = datetime.now(timezone.utc)
    expires = created + timedelta(minutes=10)
    
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO telegram_link_codes (code, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (code, user_id, created.isoformat(), expires.isoformat())
        )
    
    bot_username = os.getenv("TELEGRAM_BOT_USERNAME", "CosmicForgeBot")
    
    return TelegramLinkResponse(
        code=code,
        bot_username=f"@{bot_username}",
        instructions=f"1. Open Telegram\n2. Click the link below or search for @{bot_username}\n3. Send: /start {code}",
        deep_link=f"https://t.me/{bot_username}?start={code}"
    )

def _config_value(name: str) -> str:
    """Environment variable, falling back to the app settings object."""
    value = os.getenv(name)
    if value:
        return value.strip()
    try:
        from app.core.config import settings
        return str(getattr(settings, name, "") or "").strip()
    except Exception:
        return ""


def _is_production() -> bool:
    try:
        from shared_lib.core.security.broker_security import is_production
        return bool(is_production())
    except Exception:
        # If the environment cannot be determined, behave as production.
        return True


def _verify_telegram_webhook_secret(provided: Optional[str]) -> None:
    """Authenticate a Telegram webhook call.

    Telegram echoes the ``secret_token`` given to ``setWebhook`` in the
    ``X-Telegram-Bot-Api-Secret-Token`` header of every update. Without it,
    anyone who can reach this URL can forge updates (and link their own chat
    to another user's account by guessing a link code).

    * ``TELEGRAM_WEBHOOK_SECRET`` configured: the header must match.
    * Not configured, production: rejected (fail closed).
    * Not configured, non-production: accepted, for local development.
    """
    expected = _config_value("TELEGRAM_WEBHOOK_SECRET")
    if not expected:
        if _is_production():
            logger.error(
                "Telegram webhook rejected: TELEGRAM_WEBHOOK_SECRET is not configured "
                "(register the webhook with setWebhook secret_token=<the same value>)"
            )
            raise HTTPException(status_code=503, detail="Telegram webhook is not configured")
        return
    if not provided or not hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=403, detail="Invalid webhook secret")


@router.post("/telegram/webhook")
async def telegram_webhook(
    update: Dict[str, Any],
    x_telegram_bot_api_secret_token: Optional[str] = Header(None),
):
    """Handle Telegram bot updates (webhook)."""
    _verify_telegram_webhook_secret(x_telegram_bot_api_secret_token)

    # Extract message
    message = update.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    text = message.get("text") or ""
    
    if not chat_id or not text.startswith("/start"):
        return {"ok": True}
    
    # Extract code
    parts = text.split()
    if len(parts) < 2:
        return {"ok": True}
    
    code = parts[1].strip()
    
    # Validate code
    with db.connect() as conn:
        row = conn.execute(
            "SELECT user_id, expires_at FROM telegram_link_codes WHERE code=?",
            (code,)
        ).fetchone()
        
        if not row:
            _send_telegram_message(chat_id, "❌ Invalid or expired code. Please generate a new one.")
            return {"ok": True}
        
        # Check expiry
        expires_at = datetime.fromisoformat(row["expires_at"])
        if datetime.now(timezone.utc) > expires_at:
            _send_telegram_message(chat_id, "❌ This code has expired. Please generate a new one.")
            conn.execute("DELETE FROM telegram_link_codes WHERE code=?", (code,))
            return {"ok": True}
        
        user_id = row["user_id"]
        
        # Store endpoint
        conn.execute(
            """
            INSERT OR REPLACE INTO notification_endpoints 
            (user_id, channel, recipient, status, verified_at, created_at)
            VALUES (?, 'telegram', ?, 'active', ?, ?)
            """,
            (user_id, str(chat_id), utc_now_iso(), utc_now_iso())
        )
        
        # Delete code
        conn.execute("DELETE FROM telegram_link_codes WHERE code=?", (code,))
    
    _send_telegram_message(chat_id, "✅ Successfully linked! You will now receive notifications here.")
    return {"ok": True}

def _send_telegram_message(chat_id, text):
    """Helper to send Telegram message."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        return
    
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=5
        )
    except:
        pass

# ============================================================================
# Push Notifications
# ============================================================================

class FCMTokenRequest(BaseModel):
    """Request model for registering FCM token.

    The owner of the token is ALWAYS the authenticated caller. ``userId`` is
    accepted only so older clients keep working; its value is ignored.
    """
    userId: Optional[str] = None  # ignored (legacy clients)
    fcmToken: str = Field(..., min_length=16, max_length=4096)
    deviceId: Optional[str] = None  # Optional device identifier
    deviceName: Optional[str] = None  # e.g., "iPhone 13", "Android Pixel"

class TestNotificationRequest(BaseModel):
    """Request model for sending test notification."""
    userId: Optional[str] = None  # ignored: a test notification only goes to the caller
    title: str = Field(..., max_length=200)
    body: str = Field(..., max_length=1000)
    data: Optional[Dict[str, str]] = None


def _register_fcm_token_for_user(user_id: str, req: FCMTokenRequest) -> dict:
    fcm_token = req.fcmToken
    device_id = req.deviceId or fcm_token[:16]  # Use token prefix as device ID if not provided
    
    now = utc_now_iso()
    
    with db.connect() as conn:
        # A device token identifies one browser/device. If it is currently
        # attached to a different account (e.g. another user logged in on this
        # device before), it moves to the authenticated caller -- it is never
        # left delivering this device's notifications for someone else, and a
        # caller can never attach a token to an account that is not theirs.
        # Possessing a token is how a device registers itself, so "whoever
        # presents the token owns it" is inherent; the previous owner's row is
        # removed here (replaced, never duplicated) before the caller's is written.
        conn.execute(
            "DELETE FROM notification_endpoints WHERE channel = 'push' AND recipient = ? AND user_id != ?",
            (fcm_token, user_id)
        )

        # Check if this exact token already exists for this user
        # Note: Schema uses composite PRIMARY KEY (user_id, channel), not id
        existing = conn.execute(
            """
            SELECT recipient FROM notification_endpoints 
            WHERE user_id = ? AND channel = 'push' AND recipient = ?
            """,
            (user_id, fcm_token)
        ).fetchone()
        
        if existing:
            # Update existing token - just mark as active and verified
            conn.execute(
                """
                UPDATE notification_endpoints
                SET status = 'active', verified_at = ?
                WHERE user_id = ? AND channel = 'push' AND recipient = ?
                """,
                (now, user_id, fcm_token)
            )
            return {
                "status": "updated",
                "message": "FCM token updated successfully",
                "userId": user_id,
                "deviceId": device_id
            }
        else:
            # Insert new token, replacing only the CALLER'S OWN previous push
            # endpoint (PRIMARY KEY is (user_id, channel)).
            conn.execute(
                "DELETE FROM notification_endpoints WHERE user_id = ? AND channel = 'push'",
                (user_id,)
            )
            conn.execute(
                """
                INSERT INTO notification_endpoints 
                (user_id, channel, recipient, status, verified_at, created_at)
                VALUES (?, 'push', ?, 'active', ?, ?)
                """,
                (user_id, fcm_token, now, now)
            )
            
            return {
                "status": "registered",
                "message": "FCM token registered successfully",
                "userId": user_id,
                "deviceId": device_id
            }


@router.post("/token")
def register_fcm_token(
    req: FCMTokenRequest,
    user: dict = Depends(get_current_active_user)
):
    """
    Register or update the FCM token of the authenticated user.

    Requires login. The token is always registered to the caller; any
    ``userId`` in the body is ignored.
    
    Body: { "fcmToken": "...", "deviceId": "...", "deviceName": "..." }
    """
    return _register_fcm_token_for_user(user["id"], req)


@router.post("/test")
def send_test_notification(
    req: TestNotificationRequest,
    user: dict = Depends(get_current_active_user)
):
    """
    Send a test push notification to the caller's own devices.
    
    Body: { "title": "...", "body": "...", "data": {...} }
    
    The target is always the authenticated user: any ``userId`` in the body
    is ignored, so this cannot be used to push arbitrary text to other users.
    """
    target_user_id = user["id"]
    
    # Get all active push tokens for the user
    with db.connect() as conn:
        tokens = conn.execute(
            """
            SELECT recipient as token
            FROM notification_endpoints
            WHERE user_id = ? AND channel = 'push' AND status = 'active'
            """,
            (target_user_id,)
        ).fetchall()
    
    if not tokens:
        raise HTTPException(
            status_code=404,
            detail="No active push tokens found for your account"
        )
    
    # Send to all tokens
    try:
        from shared_lib.notifications.push_notifications import send_push_to_tokens
        
        token_list = [row["token"] for row in tokens]
        
        result = send_push_to_tokens(
            tokens=token_list,
            title=req.title,
            body=req.body,
            data=req.data or {}
        )
        
        # Clean up invalid tokens
        if result.failure_count > 0:
            _cleanup_invalid_tokens(target_user_id, result.responses, token_list)
        
        return {
            "status": "sent",
            "userId": target_user_id,
            "sent_to_devices": result.success_count,
            "failed_devices": result.failure_count,
            "total_devices": len(token_list)
        }
        
    except Exception as e:
        logger.error("Failed to send test notification: %s", type(e).__name__)
        raise HTTPException(
            status_code=500,
            detail="Failed to send test notification"
        )


@router.delete("/token/{device_id}")
def remove_fcm_token(
    device_id: str,
    user: dict = Depends(get_current_active_user)
):
    """Remove a specific FCM token by device ID."""
    user_id = user["id"]
    
    with db.connect() as conn:
        # Find and delete the token
        result = conn.execute(
            """
            DELETE FROM notification_endpoints
            WHERE user_id = ? AND channel = 'push' 
            AND (
                recipient LIKE ? OR 
                json_extract(metadata_json, '$.deviceId') = ?
            )
            """,
            (user_id, f"{device_id}%", device_id)
        )
        
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Device token not found")
        
        return {"status": "deleted", "deviceId": device_id}


@router.get("/tokens")
def get_user_tokens(user: dict = Depends(get_current_active_user)):
    """Get all registered FCM tokens for the current user."""
    user_id = user["id"]
    
    with db.connect() as conn:
        tokens = conn.execute(
            """
            SELECT 
                recipient as token,
                metadata_json,
                status,
                created_at,
                verified_at
            FROM notification_endpoints
            WHERE user_id = ? AND channel = 'push'
            ORDER BY created_at DESC
            """,
            (user_id,)
        ).fetchall()
    
    devices = []
    for row in tokens:
        metadata = json.loads(row["metadata_json"] or "{}")
        devices.append({
            "deviceId": metadata.get("deviceId", row["token"][:16]),
            "deviceName": metadata.get("deviceName", "Unknown Device"),
            "token": row["token"][:20] + "...",  # Truncate for security
            "status": row["status"],
            "registeredAt": row["created_at"]
        })
    
    return {"devices": devices, "total": len(devices)}


def _cleanup_invalid_tokens(user_id: str, responses: list, tokens: list):
    """
    Remove invalid tokens from database based on Firebase response.
    Called automatically when Firebase reports unregistered/invalid tokens.
    """
    invalid_tokens = []
    
    for idx, response in enumerate(responses):
        if not response.success and response.error:
            error_lower = response.error.lower()
            if any(keyword in error_lower for keyword in ['unregistered', 'invalid', 'notregistered']):
                invalid_tokens.append(tokens[idx])
    
    if invalid_tokens:
        with db.connect() as conn:
            placeholders = ','.join(['?' for _ in invalid_tokens])
            conn.execute(
                f"""
                UPDATE notification_endpoints
                SET status = 'invalid', updated_at = ?
                WHERE user_id = ? AND channel = 'push' AND recipient IN ({placeholders})
                """,
                [utc_now_iso(), user_id] + invalid_tokens
            )
        
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Marked {len(invalid_tokens)} invalid tokens for user {user_id}")


# Legacy endpoint for backward compatibility
@router.post("/push/register")
def register_push_token(
    req: PushTokenRequest,
    user: dict = Depends(get_current_active_user)
):
    """Legacy endpoint - use POST /notifications/token instead."""
    fcm_req = FCMTokenRequest(fcmToken=req.token)
    return _register_fcm_token_for_user(user["id"], fcm_req)

