"""
Push notification channel using Firebase Admin SDK.
"""
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class PushChannel:
    """
    Push notification channel using Firebase Admin SDK.
    This is a thin wrapper around the shared push_notifications service.
    """
    
    @staticmethod
    def send(recipient_token: str, title: str, body: str, data: Optional[Dict[str, str]] = None) -> bool:
        """
        Sends push notification via Firebase Admin SDK.
        
        Args:
            recipient_token: The device FCM token.
            title: Notification title.
            body: Notification body text.
            data: Optional data payload (all values must be strings).
            
        Returns:
            bool: True if sent successfully, False otherwise.
        """
        try:
            # Import here to avoid circular dependencies
            from shared_lib.notifications.push_notifications import send_push_to_token
            
            result = send_push_to_token(
                token=recipient_token,
                title=title,
                body=body,
                data=data
            )
            
            return result.success
            
        except Exception as e:
            logger.error(f"PushChannel: Failed to send notification: {e}")
            return False
