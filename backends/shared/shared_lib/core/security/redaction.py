"""Secret redaction for logs, exceptions, API responses and audit records.

Two layers:

* ``redact_text`` scrubs free text: signed query strings
  (``signature=...``), auth headers (``X-MBX-APIKEY``, ``X-BAPI-*``,
  ``X-BX-APIKEY``, ``Authorization: Bearer``), and ``key=value`` /
  ``"key": "value"`` pairs whose key names a secret.
* ``redact_mapping`` walks dicts/lists and masks values under secret-named
  keys, recursively.

``SecretRedactionFilter`` applies ``redact_text`` to every log record's
formatted message and exception text; ``install_log_redaction()`` attaches
it to the root logger's handlers (idempotent).
"""
from __future__ import annotations

import logging
import re
from typing import Any, Iterable

MASK = "***REDACTED***"

SECRET_KEY_NAMES = frozenset({
    "api_key", "apikey", "api-key", "api_secret", "apisecret", "secret", "secret_key", "private_key",
    "passphrase", "password", "api_token", "token", "access_token", "refresh_token", "bridge_token",
    "signature", "sign", "x-mbx-apikey", "x-bapi-api-key", "x-bapi-sign", "x-bx-apikey", "authorization",
    "encrypted_blob", "credentials",
})

_KEY_ALT = r"(?:api[_-]?key|api[_-]?secret|secret(?:_key)?|private_key|passphrase|password|api_token|" \
           r"access_token|refresh_token|bridge_token|signature|sign|x-mbx-apikey|x-bapi-api-key|x-bapi-sign|" \
           r"x-bx-apikey|encrypted_blob)"

_PATTERNS = (
    # key=value in query strings / form bodies / log text
    re.compile(rf"(?i)\b({_KEY_ALT})=([^&\s'\",}}]+)"),
    # "key": "value"  /  'key': 'value'
    re.compile(rf"(?i)([\"']{_KEY_ALT}[\"']\s*:\s*[\"'])([^\"']+)([\"'])"),
    # Header: value
    re.compile(rf"(?i)\b({_KEY_ALT})\s*:\s*([A-Za-z0-9_\-\.=+/]{{8,}})"),
    # Authorization: Bearer <token>
    re.compile(r"(?i)(bearer\s+)([A-Za-z0-9_\-\.=+/]{8,})"),
)


def redact_text(text: Any) -> str:
    if text is None:
        return ""
    out = str(text)
    out = _PATTERNS[0].sub(lambda m: f"{m.group(1)}={MASK}", out)
    out = _PATTERNS[1].sub(lambda m: f"{m.group(1)}{MASK}{m.group(3)}", out)
    out = _PATTERNS[2].sub(lambda m: f"{m.group(1)}: {MASK}", out)
    out = _PATTERNS[3].sub(lambda m: f"{m.group(1)}{MASK}", out)
    return out


def _is_secret_key(name: Any) -> bool:
    return str(name).strip().lower() in SECRET_KEY_NAMES


def redact_mapping(value: Any, extra_keys: Iterable[str] = ()) -> Any:
    extra = {k.lower() for k in extra_keys}
    if isinstance(value, dict):
        return {k: (MASK if (_is_secret_key(k) or str(k).lower() in extra) and v not in (None, "")
                    else redact_mapping(v, extra)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_mapping(v, extra) for v in value]
    if isinstance(value, tuple):
        return tuple(redact_mapping(v, extra) for v in value)
    if isinstance(value, str):
        return redact_text(value)
    return value


def redact_exception(exc: BaseException) -> str:
    return redact_text(f"{type(exc).__name__}: {exc}")


class SecretRedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        scrubbed = redact_text(message)
        if scrubbed != message:
            record.msg, record.args = scrubbed, ()
        if record.exc_info and record.exc_info[1] is not None:
            exc = record.exc_info[1]
            text = str(exc)
            if redact_text(text) != text:
                # Keep the traceback shape, drop the secret-bearing message.
                record.exc_text = redact_text(logging.Formatter().formatException(record.exc_info))
        return True


_FILTER = SecretRedactionFilter()


def install_log_redaction(logger: logging.Logger | None = None) -> None:
    target = logger or logging.getLogger()
    if _FILTER not in target.filters:
        target.addFilter(_FILTER)
    for handler in target.handlers:
        if _FILTER not in handler.filters:
            handler.addFilter(_FILTER)


__all__ = ["MASK", "SECRET_KEY_NAMES", "SecretRedactionFilter", "install_log_redaction", "redact_exception",
           "redact_mapping", "redact_text"]
