import secrets
import string
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.api.auth import get_current_active_user
from shared_lib.persistence.db import DB, utc_now_iso
import os
import requests
import json

router = APIRouter()
db = DB()

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
    token: str

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

@router.post("/telegram/webhook")
async def telegram_webhook(update: Dict[str, Any]):
    """Handle Telegram bot updates (webhook)."""
    # Extract message
    message = update.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "")
    
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
    """Request model for registering FCM token."""
    userId: str
    fcmToken: str
    deviceId: Optional[str] = None  # Optional device identifier
    deviceName: Optional[str] = None  # e.g., "iPhone 13", "Android Pixel"

class TestNotificationRequest(BaseModel):
    """Request model for sending test notification."""
    userId: str
    title: str
    body: str
    data: Optional[Dict[str, str]] = None


@router.post("/token")
def register_fcm_token(req: FCMTokenRequest):
    """
    Register or update FCM token for a user.
    Supports multiple devices per user.
    
    Body: { "userId": "...", "fcmToken": "...", "deviceId": "...", "deviceName": "..." }
    """
    user_id = req.userId
    fcm_token = req.fcmToken
    device_id = req.deviceId or fcm_token[:16]  # Use token prefix as device ID if not provided
    device_name = req.deviceName or "Unknown Device"
    
    now = utc_now_iso()
    
    with db.connect() as conn:
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
            # Insert new token
            # Note: Will replace any existing push endpoint for this user (PRIMARY KEY constraint)
            conn.execute(
                """
                INSERT OR REPLACE INTO notification_endpoints 
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


@router.post("/test")
def send_test_notification(
    req: TestNotificationRequest,
    user: dict = Depends(get_current_active_user)
):
    """
    Send a test push notification (admin/debug endpoint).
    
    Body: { "userId": "...", "title": "...", "body": "...", "data": {...} }
    
    Note: In production, add admin role check here.
    """
    # TODO: Add admin role check
    # if not user.get("is_admin"):
    #     raise HTTPException(status_code=403, detail="Admin access required")
    
    target_user_id = req.userId
    
    # Get all active push tokens for the user
    with db.connect() as conn:
        tokens = conn.execute(
            """
            SELECT recipient as token, metadata_json 
            FROM notification_endpoints
            WHERE user_id = ? AND channel = 'push' AND status = 'active'
            """,
            (target_user_id,)
        ).fetchall()
    
    if not tokens:
        raise HTTPException(
            status_code=404,
            detail=f"No active push tokens found for user {target_user_id}"
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
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send test notification: {str(e)}"
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
    fcm_req = FCMTokenRequest(
        userId=user["id"],
        fcmToken=req.token
    )
    return register_fcm_token(fcm_req)

