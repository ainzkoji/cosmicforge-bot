from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from shared_lib.persistence.db import DB
from shared_lib.persistence.tradingview import (
    MODE_ADVISORY_ONLY,
    MODE_DISABLED,
    MODE_EXTERNAL_SIGNAL_CANDIDATE,
    REJECTED_STAGE1_ACTIONS,
    STATUS_ACCEPTED_ADVISORY,
    STATUS_DISABLED_WEBHOOK,
    STATUS_DUPLICATE,
    STATUS_INVALID_SCHEMA,
    STATUS_INVALID_SIGNATURE,
    STATUS_INVALID_TOKEN,
    STATUS_RATE_LIMITED,
    STATUS_STALE,
    STATUS_UNSUPPORTED_ACTION,
    STATUS_UNSUPPORTED_SYMBOL,
    compute_idempotency_key,
    create_external_signal_queue_row,
    find_webhook_by_id_or_token,
    insert_advisory_decision,
    insert_alert,
    insert_candidate_decision,
    insert_rejected_decision,
    recent_request_count,
    update_webhook_last_used,
    utc_now_iso,
    verify_hmac_signature,
)
from shared_lib.tradingview_symbol_mapper import validate_tradingview_symbol


logger = logging.getLogger(__name__)
router = APIRouter()


class TradingViewWebhookResponse(BaseModel):
    status: str
    reason: str | None = None
    mode: str | None = None
    execution_enabled: bool = False
    alert_id: str | None = None
    symbol: str | None = None
    action: str | None = None
    queue_id: str | None = None


def _get_db() -> DB:
    return DB()


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw = raw / 1000.0
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    text = str(value).strip()
    if text.isdigit():
        return _parse_timestamp(int(text))
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _side_for_action(action: str) -> str:
    return "LONG" if action == "BUY" else "SHORT"


def _safe_price(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _safe_confidence(value: Any) -> float | None:
    """
    Parse and validate confidence from a webhook payload.

    B-5 Fix: Missing, null, non-numeric, or out-of-range confidence returns None
    (reject-signal). Values are NOT clamped — an out-of-range value is treated
    as invalid, not silently normalised.
    """
    try:
        if value in (None, ""):
            return None
        parsed = float(value)
        # Reject out-of-range — do not silently clamp
        if parsed < 0 or parsed > 1:
            return None
        return parsed
    except Exception:
        return None


def _reject(
    db: DB,
    *,
    status: str,
    reason: str,
    payload: dict[str, Any],
    webhook_id: str | None = None,
    bot_id: str | None = None,
    source_ip: str | None = None,
    signature_valid: bool | None = None,
    symbol_normalized: str | None = None,
    mode: str | None = None,
) -> TradingViewWebhookResponse:
    received_at = utc_now_iso()
    alert_row_id = insert_alert(
        db,
        webhook_id=webhook_id,
        bot_id=bot_id or payload.get("bot_id"),
        alert_id=payload.get("alert_id"),
        symbol_raw=payload.get("symbol"),
        symbol_normalized=symbol_normalized,
        action=str(payload.get("action")).upper() if payload.get("action") else None,
        side=payload.get("side"),
        timeframe=payload.get("timeframe"),
        strategy_name=payload.get("strategy_name"),
        price=_safe_price(payload.get("price")),
        payload=payload,
        received_at=received_at,
        alert_timestamp=str(payload.get("timestamp")) if payload.get("timestamp") is not None else None,
        status=status,
        reject_reason=reason,
        idempotency_key=None,
        source_ip=source_ip,
        signature_valid=signature_valid,
    )
    if bot_id or payload.get("bot_id"):
        insert_rejected_decision(
            db,
            alert_row_id=alert_row_id,
            bot_id=bot_id or payload.get("bot_id"),
            symbol=symbol_normalized or payload.get("symbol"),
            action=str(payload.get("action")).upper() if payload.get("action") else None,
            mode=mode,
            normalized_signal={
                "source": "TRADINGVIEW",
                "reject_reason": reason,
                "execution_enabled": False,
                "queue_created": False,
            },
            final_status=status,
            final_reason=reason,
        )
    logger.info(
        "[TradingView] rejected webhook_id=%s bot_id=%s alert_id=%s symbol=%s action=%s reason=%s",
        webhook_id,
        bot_id or payload.get("bot_id"),
        payload.get("alert_id"),
        symbol_normalized or payload.get("symbol"),
        payload.get("action"),
        reason,
    )
    response_status = "duplicate" if status == STATUS_DUPLICATE else "rejected"
    return TradingViewWebhookResponse(status=response_status, reason=reason, execution_enabled=False)


@router.post("/webhook/{token_or_id}", response_model=TradingViewWebhookResponse)
async def receive_tradingview_webhook(token_or_id: str, request: Request) -> TradingViewWebhookResponse:
    db = _get_db()
    raw_body = await request.body()
    source_ip = request.client.host if request.client else None
    try:
        payload = json.loads(raw_body.decode("utf-8") or "{}")
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}

    payload_token = payload.get("token")
    webhook, verified_token = find_webhook_by_id_or_token(db, token_or_id, payload_token)
    if not webhook or not verified_token:
        return _reject(
            db,
            status=STATUS_INVALID_TOKEN,
            reason="INVALID_TOKEN",
            payload=payload,
            webhook_id=webhook.get("id") if webhook else None,
            bot_id=webhook.get("bot_id") if webhook else payload.get("bot_id"),
            source_ip=source_ip,
            mode=webhook.get("mode") if webhook else None,
        )

    webhook_id = webhook["id"]
    bot_id = webhook["bot_id"]
    if int(webhook.get("is_enabled", 0)) != 1 or webhook.get("mode") == MODE_DISABLED:
        return _reject(
            db,
            status=STATUS_DISABLED_WEBHOOK,
            reason="DISABLED_WEBHOOK",
            payload=payload,
            webhook_id=webhook_id,
            bot_id=bot_id,
            source_ip=source_ip,
            mode=webhook.get("mode"),
        )

    signature = request.headers.get("X-CF-TV-Signature")
    signature_valid = None
    if signature:
        timestamp_h = request.headers.get("X-CF-TV-Timestamp", "")
        nonce_h = request.headers.get("X-CF-TV-Nonce", "")
        signature_valid = verify_hmac_signature(
            token=verified_token,
            body=raw_body,
            timestamp=timestamp_h,
            nonce=nonce_h,
            signature=signature,
        )
        if not signature_valid:
            return _reject(
                db,
                status=STATUS_INVALID_SIGNATURE,
                reason="INVALID_SIGNATURE",
                payload=payload,
                webhook_id=webhook_id,
                bot_id=bot_id,
                source_ip=source_ip,
                signature_valid=False,
                mode=webhook.get("mode"),
            )

    required = ["bot_id", "alert_id", "symbol", "action", "timestamp"]
    if any(not payload.get(field) for field in required):
        return _reject(
            db,
            status=STATUS_INVALID_SCHEMA,
            reason="INVALID_SCHEMA",
            payload=payload,
            webhook_id=webhook_id,
            bot_id=bot_id,
            source_ip=source_ip,
            signature_valid=signature_valid,
            mode=webhook.get("mode"),
        )
    if str(payload.get("bot_id")) != str(bot_id):
        return _reject(
            db,
            status=STATUS_INVALID_SCHEMA,
            reason="BOT_ID_MISMATCH",
            payload=payload,
            webhook_id=webhook_id,
            bot_id=bot_id,
            source_ip=source_ip,
            signature_valid=signature_valid,
            mode=webhook.get("mode"),
        )

    action = str(payload.get("action")).strip().upper()
    if action in REJECTED_STAGE1_ACTIONS or action not in {"BUY", "SELL"}:
        return _reject(
            db,
            status=STATUS_UNSUPPORTED_ACTION,
            reason="UNSUPPORTED_ACTION",
            payload=payload,
            webhook_id=webhook_id,
            bot_id=bot_id,
            source_ip=source_ip,
            signature_valid=signature_valid,
            mode=webhook.get("mode"),
        )

    expected_side = _side_for_action(action)
    side = str(payload.get("side") or expected_side).strip().upper()
    if side != expected_side:
        return _reject(
            db,
            status=STATUS_INVALID_SCHEMA,
            reason="SIDE_ACTION_MISMATCH",
            payload=payload,
            webhook_id=webhook_id,
            bot_id=bot_id,
            source_ip=source_ip,
            signature_valid=signature_valid,
            mode=webhook.get("mode"),
        )

    alert_dt = _parse_timestamp(payload.get("timestamp"))
    if not alert_dt:
        return _reject(
            db,
            status=STATUS_INVALID_SCHEMA,
            reason="INVALID_TIMESTAMP",
            payload=payload,
            webhook_id=webhook_id,
            bot_id=bot_id,
            source_ip=source_ip,
            signature_valid=signature_valid,
            mode=webhook.get("mode"),
        )
    now = datetime.now(timezone.utc)
    max_age = int(webhook.get("max_alert_age_seconds") or 300)
    if alert_dt < now - timedelta(seconds=max_age) or alert_dt > now + timedelta(seconds=60):
        return _reject(
            db,
            status=STATUS_STALE,
            reason="STALE_ALERT",
            payload=payload,
            webhook_id=webhook_id,
            bot_id=bot_id,
            source_ip=source_ip,
            signature_valid=signature_valid,
            mode=webhook.get("mode"),
        )

    try:
        allowed_symbols = json.loads(webhook["allowed_symbols_json"]) if webhook.get("allowed_symbols_json") else None
    except Exception:
        allowed_symbols = None
    ok_symbol, normalized_symbol, symbol_reason = validate_tradingview_symbol(
        db=db,
        bot_id=bot_id,
        symbol_raw=str(payload.get("symbol")),
        exchange_raw=payload.get("exchange"),
        allowed_symbols=allowed_symbols,
        expected_exchange="BINANCE",
    )
    if not ok_symbol or not normalized_symbol:
        return _reject(
            db,
            status=STATUS_UNSUPPORTED_SYMBOL,
            reason=symbol_reason or "UNSUPPORTED_SYMBOL",
            payload=payload,
            webhook_id=webhook_id,
            bot_id=bot_id,
            source_ip=source_ip,
            signature_valid=signature_valid,
            symbol_normalized=normalized_symbol,
            mode=webhook.get("mode"),
        )

    since = (now - timedelta(minutes=1)).isoformat()
    if recent_request_count(db, webhook_id, since) >= int(webhook.get("rate_limit_per_minute") or 30):
        return _reject(
            db,
            status=STATUS_RATE_LIMITED,
            reason="RATE_LIMITED",
            payload=payload,
            webhook_id=webhook_id,
            bot_id=bot_id,
            source_ip=source_ip,
            signature_valid=signature_valid,
            symbol_normalized=normalized_symbol,
            mode=webhook.get("mode"),
        )

    alert_timestamp = alert_dt.isoformat()
    idempotency_key = compute_idempotency_key(
        webhook_id=webhook_id,
        alert_id=str(payload.get("alert_id")),
        symbol=normalized_symbol,
        action=action,
        alert_timestamp=alert_timestamp,
    )
    received_at = utc_now_iso()
    try:
        alert_row_id = insert_alert(
            db,
            webhook_id=webhook_id,
            bot_id=bot_id,
            alert_id=str(payload.get("alert_id")),
            symbol_raw=str(payload.get("symbol")),
            symbol_normalized=normalized_symbol,
            action=action,
            side=side,
            timeframe=payload.get("timeframe"),
            strategy_name=payload.get("strategy_name"),
            price=_safe_price(payload.get("price")),
            payload=payload,
            received_at=received_at,
            alert_timestamp=alert_timestamp,
            status=STATUS_ACCEPTED_ADVISORY,
            reject_reason=None,
            idempotency_key=idempotency_key,
            source_ip=source_ip,
            signature_valid=signature_valid,
        )
    except Exception as exc:
        text = str(exc).lower()
        if "unique" in text:
            logger.info("[TradingView] duplicate webhook_id=%s alert_id=%s", webhook_id, payload.get("alert_id"))
            return TradingViewWebhookResponse(
                status="duplicate",
                reason="DUPLICATE_ALERT",
                execution_enabled=False,
            )
        logger.exception("[TradingView] failed to store alert safely")
        return TradingViewWebhookResponse(
            status="rejected",
            reason="INTERNAL_STORAGE_ERROR",
            execution_enabled=False,
        )

    normalized_signal = {
        "source": "TRADINGVIEW",
        "alert_id": payload.get("alert_id"),
        "symbol": normalized_symbol,
        "action": action,
        "side": side,
        "timeframe": payload.get("timeframe"),
        "strategy_name": payload.get("strategy_name"),
        "confidence": payload.get("confidence"),
        "external_sl_tp_ignored": bool(payload.get("stop_loss") or payload.get("take_profit")),
        "execution_enabled": False,
    }
    queue_id = None
    mode = webhook.get("mode") or MODE_ADVISORY_ONLY
    # B-5 Fix: Validate confidence before enqueuing as execution candidate.
    # Missing, null, non-numeric, or out-of-range confidence is rejected.
    # A missing confidence must NEVER default to 1.0 (perfect reliability).
    if mode == MODE_EXTERNAL_SIGNAL_CANDIDATE:
        raw_confidence = payload.get("confidence")
        parsed_confidence = _safe_confidence(raw_confidence)

        if raw_confidence is None or raw_confidence == "":
            logger.warning(
                "[TV_SIGNAL] EXTERNAL_SIGNAL_REJECTED_MISSING_CONFIDENCE "
                "webhook_id=%s bot_id=%s symbol=%s action=%s",
                webhook_id, bot_id, normalized_symbol, action,
            )
            return _reject(
                db,
                status=STATUS_INVALID_SCHEMA,
                reason="EXTERNAL_SIGNAL_REJECTED_MISSING_CONFIDENCE",
                payload=payload,
                webhook_id=webhook_id,
                bot_id=bot_id,
                source_ip=source_ip,
                signature_valid=signature_valid,
                symbol_normalized=normalized_symbol,
                mode=mode,
            )

        if parsed_confidence is None:
            logger.warning(
                "[TV_SIGNAL] EXTERNAL_SIGNAL_REJECTED_INVALID_CONFIDENCE "
                "webhook_id=%s bot_id=%s symbol=%s raw_confidence=%r",
                webhook_id, bot_id, normalized_symbol, raw_confidence,
            )
            return _reject(
                db,
                status=STATUS_INVALID_SCHEMA,
                reason="EXTERNAL_SIGNAL_REJECTED_INVALID_CONFIDENCE",
                payload=payload,
                webhook_id=webhook_id,
                bot_id=bot_id,
                source_ip=source_ip,
                signature_valid=signature_valid,
                symbol_normalized=normalized_symbol,
                mode=mode,
            )

        expires_at = (alert_dt + timedelta(seconds=max_age)).isoformat()
        queue_id = create_external_signal_queue_row(
            db,
            source_alert_id=str(alert_row_id),
            bot_id=bot_id,
            symbol=normalized_symbol,
            side=side,
            action=action,
            confidence=_safe_confidence(payload.get("confidence")),
            available_at=received_at,
            expires_at=expires_at,
            result={
                "stage": "TRADINGVIEW_PHASE_3_QUEUE_ONLY",
                "runner_processing_enabled": False,
                "execution_enabled": False,
                "alert_id": payload.get("alert_id"),
            },
        )
        normalized_signal["queue_id"] = queue_id
        insert_candidate_decision(
            db,
            alert_row_id=alert_row_id,
            bot_id=bot_id,
            symbol=normalized_symbol,
            action=action,
            normalized_signal=normalized_signal,
            queue_id=queue_id,
        )
    else:
        insert_advisory_decision(
            db,
            alert_row_id=alert_row_id,
            bot_id=bot_id,
            symbol=normalized_symbol,
            action=action,
            normalized_signal=normalized_signal,
        )
    update_webhook_last_used(db, webhook_id, received_at)
    logger.info(
        "[TradingView] accepted webhook_id=%s bot_id=%s alert_id=%s symbol=%s action=%s mode=%s queue_id=%s",
        webhook_id,
        bot_id,
        payload.get("alert_id"),
        normalized_symbol,
        action,
        mode,
        queue_id,
    )
    reason = (
        "QUEUED_EXTERNAL_SIGNAL_RUNNER_DISABLED_STAGE_3"
        if queue_id
        else "ACCEPTED_ADVISORY_ONLY_NO_QUEUE"
    )
    return TradingViewWebhookResponse(
        status="accepted",
        mode=mode,
        execution_enabled=False,
        alert_id=str(payload.get("alert_id")),
        symbol=normalized_symbol,
        action=action,
        reason=reason,
        queue_id=queue_id,
    )
