"""Sanitized classification of a failed account read (Binance / Bybit / BingX).

The production cycle used to record only the exception class (``AdapterError``,
``RuntimeError``), which told neither the customer nor the operator what to do.
``classify`` turns the exception into a stable reason code with a short
diagnostic. Nothing free-form from the exception is kept: only upper-case
tokens, HTTP statuses and numeric venue error codes, so a signed URL, an API
key or a response body can never reach the state document or the log.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

#: Binance error codes that say the key itself was refused (not a transient fault).
_BINANCE_CREDENTIAL_CODES = {-2014, -2015, -2008}
_BINANCE_SIGNATURE_CODES = {-1022}
_BINANCE_TIMESTAMP_CODES = {-1021}
_BINANCE_RATE_CODES = {-1003, -1015}

_HTTP = re.compile(r"HTTP\s+(\d{3})")
_VENUE_CODE = re.compile(r'"code"\s*:\s*(-?\d+)')
_TOKEN = re.compile(r"^[A-Z][A-Z0-9_]{2,80}$")

#: reason code -> (customer message, action). Registered in reason_registry.
MESSAGES: Dict[str, tuple] = {
    "BROKER_RATE_LIMITED_LOCAL": (
        "This server's own exchange request budget was used up, so the read was not sent.",
        "No action needed; it is retried on the next cycle. The operator is alerted if it persists."),
    "BROKER_RATE_LIMITED": (
        "The exchange asked this server to slow down (HTTP 429).",
        "No action needed; reads resume after the exchange's wait time."),
    "BROKER_IP_BANNED": (
        "The exchange temporarily banned this server's IP address for too many requests (HTTP 418).",
        "No action needed; reads resume when the ban expires. The operator is alerted."),
    "BROKER_CREDENTIALS_REJECTED": (
        "The exchange rejected the API key (invalid key, wrong IP whitelist, or missing Futures permission).",
        "Reconnect the exchange: create a new API key with Futures reading and trading enabled, "
        "no withdrawal permission, and this server's IP whitelisted, then save it in Exchange settings."),
    "BROKER_PERMISSION_DENIED": (
        "The API key does not have the permission this read needs.",
        "Edit the key at the exchange to enable Futures, or reconnect with a new key."),
    "BROKER_SIGNATURE_REJECTED": (
        "The exchange rejected the request signature; the stored secret does not match the key.",
        "Reconnect the exchange with the key's correct secret."),
    "BROKER_TIMESTAMP_REJECTED": (
        "The exchange rejected the request time; this server's clock is out of step.",
        "No action needed; the clock is re-synchronised automatically. The operator is alerted if it persists."),
    "BROKER_CREDENTIAL_DECRYPT_FAILED": (
        "The saved exchange key could not be decrypted on this server (its encryption key changed).",
        "Operator: restore the original credential encryption key; otherwise reconnect the exchange."),
    "BROKER_CREDENTIALS_MISSING": (
        "No usable exchange credentials are stored for this account.",
        "Reconnect the exchange in Exchange settings."),
    "BROKER_CONNECTION_INVALID": (
        "This exchange connection is not valid for trading (revoked, superseded, or not validated).",
        "Reconnect the exchange in Exchange settings."),
    "BROKER_NETWORK_UNAVAILABLE": (
        "The exchange could not be reached from this server.",
        "No action needed; it is retried on the next cycle."),
    "BROKER_VENUE_ERROR": (
        "The exchange returned a server error.",
        "No action needed; it is retried on the next cycle."),
    "BROKER_RESPONSE_MALFORMED": (
        "The exchange answered in an unexpected format.",
        "It is retried on the next cycle. If it persists, contact support with this code."),
    "BROKER_ENDPOINT_UNSUPPORTED": (
        "This server tried an exchange endpoint it has no request budget entry for.",
        "Operator review: the endpoint must be registered before it can be used."),
    "BROKER_VENUE_REQUEST_REFUSED": (
        "The exchange refused a read request.",
        "It is retried on the next cycle. If it persists, re-validate the exchange connection."),
}


def _token(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text if _TOKEN.match(text) else None


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def classify(exc: BaseException) -> Dict[str, Any]:
    """``{"reason": CODE, "diagnostic": {...}}`` for a failed read. The
    diagnostic carries only the exception class and sanitized tokens."""
    from shared_lib.broker import venue_contract as C
    diagnostic: Dict[str, Any] = {"exception": type(exc).__name__}
    reason: Optional[str] = None

    if isinstance(exc, C.AdapterError):
        detail = _token(exc.message)
        diagnostic.update(adapter_code=exc.code, detail=detail, http_status=exc.http_status,
                          venue_code=_int(exc.venue_code), retry_after_s=exc.retry_after_s)
        if exc.code == C.RATE_LIMITED:
            reason = "BROKER_RATE_LIMITED_LOCAL" if detail == "LOCAL_HTTP_BUDGET_REFUSED" else "BROKER_RATE_LIMITED"
        elif exc.code == C.BANNED:
            reason = "BROKER_IP_BANNED"
        elif exc.code == C.AUTH_FAILED:
            reason = "BROKER_CREDENTIALS_REJECTED"
        elif exc.code == C.PERMISSION_DENIED:
            reason = "BROKER_PERMISSION_DENIED"
        elif exc.code == C.TIMESTAMP_OUT_OF_SYNC:
            reason = "BROKER_TIMESTAMP_REJECTED"
        elif exc.code in (C.NETWORK_UNCERTAIN, C.VENUE_UNAVAILABLE):
            reason = "BROKER_NETWORK_UNAVAILABLE"
        elif exc.code == C.MALFORMED_RESPONSE:
            reason = "BROKER_RESPONSE_MALFORMED"
        elif exc.code == C.UNSUPPORTED:
            reason = "BROKER_ENDPOINT_UNSUPPORTED"
        else:
            reason = "BROKER_VENUE_REQUEST_REFUSED"
        return {"reason": reason, "diagnostic": diagnostic}

    code = getattr(exc, "reason_code", None)
    if type(exc).__name__ == "BrokerResolverError":
        diagnostic["resolver_reason"] = _token(str(code).upper())
        reason = {"broker_decrypt_failed": "BROKER_CREDENTIAL_DECRYPT_FAILED",
                  "broker_no_credentials": "BROKER_CREDENTIALS_MISSING",
                  "broker_auth_failed": "BROKER_CREDENTIALS_REJECTED"}.get(code, "BROKER_CONNECTION_INVALID")
        return {"reason": reason, "diagnostic": diagnostic}
    if type(exc).__name__ == "InvalidToken":  # cryptography.fernet: the key changed
        return {"reason": "BROKER_CREDENTIAL_DECRYPT_FAILED", "diagnostic": diagnostic}

    try:
        import requests
        if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
            return {"reason": "BROKER_NETWORK_UNAVAILABLE", "diagnostic": diagnostic}
    except ImportError:  # pragma: no cover
        pass

    text = str(exc)
    status = _HTTP.search(text)
    if isinstance(exc, RuntimeError) and status:
        http_status = int(status.group(1))
        venue = _VENUE_CODE.search(text)
        venue_code = int(venue.group(1)) if venue else None
        diagnostic.update(http_status=http_status, venue_code=venue_code)
        if venue_code in _BINANCE_CREDENTIAL_CODES or http_status == 401:
            reason = "BROKER_CREDENTIALS_REJECTED"
        elif venue_code in _BINANCE_SIGNATURE_CODES:
            reason = "BROKER_SIGNATURE_REJECTED"
        elif venue_code in _BINANCE_TIMESTAMP_CODES:
            reason = "BROKER_TIMESTAMP_REJECTED"
        elif venue_code in _BINANCE_RATE_CODES or http_status == 429:
            reason = "BROKER_RATE_LIMITED"
        elif http_status == 418:
            reason = "BROKER_IP_BANNED"
        elif http_status == 403:
            reason = "BROKER_PERMISSION_DENIED"
        elif http_status >= 500:
            reason = "BROKER_VENUE_ERROR"
        else:
            reason = "BROKER_VENUE_REQUEST_REFUSED"
        return {"reason": reason, "diagnostic": diagnostic}
    if isinstance(exc, RuntimeError) and "request failed after retries" in text:
        return {"reason": "BROKER_NETWORK_UNAVAILABLE", "diagnostic": diagnostic}

    # A gate or shape code raised by the engine itself (ValueError("CODE")).
    own = _token(code) or (_token(text) if isinstance(exc, ValueError) else None)
    if own:
        return {"reason": own, "diagnostic": diagnostic}
    return {"reason": type(exc).__name__, "diagnostic": diagnostic}


def is_local_budget_refusal(exc: BaseException) -> bool:
    """A refusal by this server's own request budget: nothing reached the venue,
    so it says nothing about the client and is no reason to rebuild it."""
    from shared_lib.broker import venue_contract as C
    return (isinstance(exc, C.AdapterError) and exc.code in (C.RATE_LIMITED, C.BANNED)
            and exc.message == "LOCAL_HTTP_BUDGET_REFUSED")


__all__ = ["MESSAGES", "classify", "is_local_budget_refusal"]
