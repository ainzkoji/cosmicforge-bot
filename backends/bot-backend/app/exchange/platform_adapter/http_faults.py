"""Scoped certification faults around a real requests-compatible transport.

Install underneath BudgetedHTTP on a dedicated certification client, never on
the shared runtime client. The runner still requires its OperatorPermit. This
transport grants no mutation authority and does not retry. External reductions
must be supplied by the operator harness using a separately authorised client.
"""
import hashlib
import hmac
import math
import re
from urllib.parse import urlsplit, urlunsplit

import requests

from . import contract as C


class HTTPFaultInjector:
    def __init__(self, transport, venue, *, signing_secret, external_reduce):
        if venue not in C.VENUES or not callable(external_reduce):
            raise ValueError("VENUE_AND_SCOPED_EXTERNAL_REDUCER_REQUIRED")
        self.transport, self.venue = transport, venue
        self._secret, self._external_reduce = signing_secret, external_reduce
        self._next, self._skew = None, 0

    def _arm(self, kind, value=None):
        if self._next is not None:
            raise RuntimeError("FAULT_ALREADY_ARMED")
        self._next = (kind, value)

    def lose_next_acknowledgment(self):
        self._arm("lost_ack")

    def interrupt_next_request(self):
        self._arm("interrupt")

    def rate_limit_next_request(self, retry_after_s):
        if not math.isfinite(retry_after_s) or retry_after_s < 0:
            raise ValueError("INVALID_RETRY_AFTER")
        self._arm("rate", retry_after_s)

    def skew_clock(self, offset_ms):
        self._skew = int(offset_ms)

    def reduce_position_externally(self, symbol, quantity):
        return self._external_reduce(symbol, quantity)

    def _signature(self, value):
        return hmac.new(self._secret.encode(), value.encode(), hashlib.sha256).hexdigest()

    def _skew_query(self, value):
        if "signature=" not in value or not re.search(r"(?:^|&)timestamp=\d+", value):
            return value
        query = re.sub(r"(?:^|&)signature=[^&]*", "", value).strip("&")
        query = re.sub(r"(?P<prefix>^|&)timestamp=(?P<ts>\d+)",
                       lambda m: m["prefix"] + "timestamp=" + str(int(m["ts"]) + self._skew), query)
        return query + "&signature=" + self._signature(query)

    def request(self, method, url, **kwargs):
        fault = self._next
        # Lost acknowledgements concern mutations, and must survive preparatory reads.
        if fault and (fault[0] != "lost_ack" or method.upper() in ("POST", "PUT", "DELETE")):
            self._next = None
        else:
            fault = None
        if fault and fault[0] == "interrupt":
            raise requests.ConnectionError("CERTIFICATION_INTERRUPTED_BEFORE_SEND")
        if fault and fault[0] == "rate":
            response = requests.Response()
            response.status_code = 429
            response.headers["Retry-After"] = str(fault[1])
            response._content = b'{"code":429,"msg":"certification injected rate refusal"}'
            return response
        if self._skew:
            parts = urlsplit(url)
            if self.venue == C.BYBIT:
                headers = dict(kwargs.get("headers", {}))
                if "X-BAPI-TIMESTAMP" in headers:
                    stamp = str(int(headers["X-BAPI-TIMESTAMP"]) + self._skew)
                    payload = parts.query if method.upper() == "GET" else kwargs.get("data", "")
                    headers["X-BAPI-TIMESTAMP"] = stamp
                    headers["X-BAPI-SIGN"] = self._signature(
                        stamp + headers["X-BAPI-API-KEY"] + str(headers["X-BAPI-RECV-WINDOW"]) + payload)
                    kwargs["headers"] = headers
            else:
                url = urlunsplit(parts._replace(query=self._skew_query(parts.query)))
                if isinstance(kwargs.get("data"), str):
                    kwargs["data"] = self._skew_query(kwargs["data"])
        response = self.transport.request(method, url, **kwargs)
        if fault and fault[0] == "lost_ack":
            response.close()
            raise requests.ConnectionError("CERTIFICATION_ACKNOWLEDGMENT_LOST_AFTER_SEND")
        return response

    def close(self):
        self._next, self._skew = None, 0
        self.transport.close()
