"""
Bot Backend Notification Service
Sends push notifications for trading events.
"""
import logging
from typing import Dict, Any, Optional, List
from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)


def notify_user_trade_event(user_id: str, event_type: str, payload: Dict[str, Any]):
    """
    Send push notification to user for a trading event.
    
    Args:
        user_id: The user ID to notify
        event_type: Type of event (e.g., 'trade_executed', 'stoploss_hit', 'signal_generated')
        payload: Event data containing trade details
    
    Supported event types:
        - trade_executed: When a trade is filled
        - stoploss_hit: When stop loss is triggered
        - takeprofit_hit: When take profit is triggered  
        - signal_generated: When a new trading signal is generated
        - order_failed: When an order fails
        - risk_alert: Risk management alerts
    """
    try:
        # Get all active push tokens for the user
        db = DB()
        with db.connect() as conn:
            tokens_result = conn.execute(
                """
                SELECT recipient as token 
                FROM notification_endpoints
                WHERE user_id = ? AND channel = 'push' AND status = 'active'
                """,
                (user_id,)
            ).fetchall()
        
        if not tokens_result:
            logger.debug(f"No push tokens found for user {user_id}")
            return False
        
        tokens = [row["token"] for row in tokens_result]
        
        # Build notification based on event type
        title, body, data = _build_notification_content(event_type, payload)
        
        # Send push notification
        from shared_lib.notifications.push_notifications import send_push_to_tokens
        
        result = send_push_to_tokens(
            tokens=tokens,
            title=title,
            body=body,
            data=data
        )
        
        # Clean up invalid tokens
        if result.failure_count > 0:
            _cleanup_invalid_tokens(user_id, result.responses, tokens)
        
        logger.info(
            f"Sent {event_type} notification to user {user_id}: "
            f"{result.success_count}/{len(tokens)} devices"
        )
        
        return result.success_count > 0
        
    except Exception as e:
        logger.error(f"Failed to send trade event notification: {e}", exc_info=True)
        return False


def _build_notification_content(event_type: str, payload: Dict[str, Any]) -> tuple:
    """
    Build notification title, body, and data based on event type.
    
    Returns:
        tuple: (title, body, data_dict)
    """
    symbol = payload.get("symbol", "N/A")
    
    # Convert data to strings for FCM
    data = {
        "event_type": event_type,
        "symbol": str(symbol),
        "timestamp": str(payload.get("timestamp", "")),
    }
    
    if event_type == "trade_executed":
        side = payload.get("side", "UNKNOWN")
        qty = payload.get("qty", 0)
        price = payload.get("price", 0)
        
        title = f"✅ Trade Executed: {symbol}"
        body = f"{side} {qty} @ ${price}"
        
        data.update({
            "side": str(side),
            "qty": str(qty),
            "price": str(price)
        })
        
    elif event_type == "stoploss_hit":
        loss = payload.get("loss", 0)
        
        title = f"🛑 Stop Loss Hit: {symbol}"
        body = f"Position closed. Loss: ${abs(loss):.2f}"
        
        data.update({
            "loss": str(loss)
        })
        
    elif event_type == "takeprofit_hit":
        profit = payload.get("profit", 0)
        
        title = f"🎯 Take Profit Hit: {symbol}"
        body = f"Position closed. Profit: ${profit:.2f}"
        
        data.update({
            "profit": str(profit)
        })
        
    elif event_type == "signal_generated":
        signal = payload.get("signal", "UNKNOWN")
        confidence = payload.get("confidence", 0)
        
        title = f"📊 Trading Signal: {symbol}"
        body = f"{signal} signal generated (confidence: {confidence:.1%})"
        
        data.update({
            "signal": str(signal),
            "confidence": str(confidence)
        })
        
    elif event_type == "order_failed":
        reason = payload.get("reason", "Unknown error")
        
        title = f"❌ Order Failed: {symbol}"
        body = f"Reason: {reason}"
        
        data.update({
            "reason": str(reason)
        })
        
    elif event_type == "risk_alert":
        alert_type = payload.get("alert_type", "RISK")
        message = payload.get("message", "Risk threshold reached")
        
        title = f"⚠️ {alert_type} Alert"
        body = message
        
        data.update({
            "alert_type": str(alert_type),
            "message": str(message)
        })
        
    else:
        # Generic fallback
        title = f"CosmicForge: {event_type.replace('_', ' ').title()}"
        body = f"Event for {symbol}"
        
    return title, body, data


def _cleanup_invalid_tokens(user_id: str, responses: List, tokens: List[str]):
    """
    Mark invalid tokens in the database.
    """
    invalid_tokens = []
    
    for idx, response in enumerate(responses):
        if not response.success and response.error:
            error_lower = response.error.lower()
            if any(keyword in error_lower for keyword in ['unregistered', 'invalid', 'notregistered']):
                invalid_tokens.append(tokens[idx])
    
    if invalid_tokens:
        from shared_lib.persistence.db import DB, utc_now_iso
        
        db = DB()
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
        
        logger.info(f"Marked {len(invalid_tokens)} invalid tokens for user {user_id}")


# Convenience functions for common events
def notify_trade_executed(user_id: str, symbol: str, side: str, qty: float, price: float, **kwargs):
    """Send notification when a trade is executed."""
    payload = {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
        **kwargs
    }
    return notify_user_trade_event(user_id, "trade_executed", payload)


def notify_stoploss_hit(user_id: str, symbol: str, loss: float, **kwargs):
    """Send notification when stop loss is hit."""
    payload = {
        "symbol": symbol,
        "loss": loss,
        **kwargs
    }
    return notify_user_trade_event(user_id, "stoploss_hit", payload)


def notify_signal_generated(user_id: str, symbol: str, signal: str, confidence: float, **kwargs):
    """Send notification when a trading signal is generated."""
    payload = {
        "symbol": symbol,
        "signal": signal,
        "confidence": confidence,
        **kwargs
    }
    return notify_user_trade_event(user_id, "signal_generated", payload)


def notify_risk_alert(user_id: str, alert_type: str, message: str, **kwargs):
    """Send notification for risk alerts."""
    payload = {
        "alert_type": alert_type,
        "message": message,
        **kwargs
    }
    return notify_user_trade_event(user_id, "risk_alert", payload)
