"""
Push Notification Service using Firebase Admin SDK.
Provides functions to send push notifications to single or multiple FCM tokens.
"""
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from firebase_admin import messaging, exceptions
from shared_lib.notifications.firebase_client import get_firebase_app

logger = logging.getLogger(__name__)


@dataclass
class PushResult:
    """Result of a push notification send operation."""
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None


@dataclass
class BatchPushResult:
    """Result of a batch push notification send operation."""
    success_count: int
    failure_count: int
    responses: List[PushResult]


def send_push_to_token(
    token: str,
    title: str,
    body: str,
    data: Optional[Dict[str, str]] = None
) -> PushResult:
    """
    Send a push notification to a single FCM token.
    
    Args:
        token: The FCM device token.
        title: Notification title.
        body: Notification body text.
        data: Optional data payload (all values must be strings).
    
    Returns:
        PushResult: Result of the send operation.
    """
    try:
        # Ensure Firebase is initialized
        get_firebase_app()
        
        # Build the message
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
            ),
            data=data or {},
            token=token,
            android=messaging.AndroidConfig(
                priority='high',
            ),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        sound='default',
                    ),
                ),
            ),
        )
        
        # Send the message
        response = messaging.send(message)
        
        logger.info(f"Successfully sent push to token {token[:10]}... (message_id: {response})")
        return PushResult(success=True, message_id=response)
        
    except exceptions.FirebaseError as e:
        # Handle all Firebase errors (InvalidArgumentError, NotFoundError, etc.)
        error_msg = f"Firebase error: {str(e)}"
        logger.warning(error_msg)
        return PushResult(success=False, error=error_msg)
        
    except Exception as e:
        error_msg = f"Failed to send push notification: {str(e)}"
        logger.error(error_msg)
        return PushResult(success=False, error=error_msg)


def send_push_to_tokens(
    tokens: List[str],
    title: str,
    body: str,
    data: Optional[Dict[str, str]] = None
) -> BatchPushResult:
    """
    Send a push notification to multiple FCM tokens (batch/multicast).
    
    Args:
        tokens: List of FCM device tokens.
        title: Notification title.
        body: Notification body text.
        data: Optional data payload (all values must be strings).
    
    Returns:
        BatchPushResult: Result of the batch send operation.
    """
    if not tokens:
        logger.warning("No tokens provided for batch push")
        return BatchPushResult(success_count=0, failure_count=0, responses=[])
    
    try:
        # Ensure Firebase is initialized
        get_firebase_app()
        
        # Build the multicast message
        message = messaging.MulticastMessage(
            notification=messaging.Notification(
                title=title,
                body=body,
            ),
            data=data or {},
            tokens=tokens,
            android=messaging.AndroidConfig(
                priority='high',
            ),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        sound='default',
                    ),
                ),
            ),
        )
        
        # Send to all tokens
        batch_response = messaging.send_multicast(message)
        
        # Parse results
        responses = []
        for idx, response in enumerate(batch_response.responses):
            token_preview = tokens[idx][:10] + "..." if len(tokens[idx]) > 10 else tokens[idx]
            
            if response.success:
                responses.append(PushResult(
                    success=True,
                    message_id=response.message_id
                ))
                logger.debug(f"Push sent to {token_preview}: {response.message_id}")
            else:
                error_msg = str(response.exception) if response.exception else "Unknown error"
                responses.append(PushResult(
                    success=False,
                    error=error_msg
                ))
                logger.warning(f"Push failed for {token_preview}: {error_msg}")
        
        logger.info(
            f"Batch push completed: {batch_response.success_count}/{len(tokens)} successful"
        )
        
        return BatchPushResult(
            success_count=batch_response.success_count,
            failure_count=batch_response.failure_count,
            responses=responses
        )
        
    except Exception as e:
        error_msg = f"Failed to send batch push notification: {str(e)}"
        logger.error(error_msg)
        
        # Return all failures
        return BatchPushResult(
            success_count=0,
            failure_count=len(tokens),
            responses=[PushResult(success=False, error=error_msg) for _ in tokens]
        )
