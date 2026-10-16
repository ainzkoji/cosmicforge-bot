"""Bounded WebSocket transport for UserStreamManager (websockets 15 sync API).

Authentication and subscriptions belong to the manager. The connection logger is
private and disabled because request paths and frames can contain credentials.
"""
import json
import logging
from collections.abc import Mapping
from urllib.parse import urlsplit

from websockets.exceptions import WebSocketException
from websockets.sync.client import connect

from .user_stream import HEARTBEAT, MAX_FRAME_BYTES, StreamClosed


class WebSocketTransport:
    def __init__(self, *, admit=None):
        self._admit = admit
        self._connection = None
        self._pong = None
        self._logger = logging.Logger("cati.private_stream", level=logging.CRITICAL + 1)
        self._logger.disabled = True

    def connect(self, spec):
        self.close()
        if urlsplit(spec.url).scheme != "wss":
            raise StreamClosed("STREAM_TLS_REQUIRED")
        if self._admit is not None and not self._admit():
            raise StreamClosed("STREAM_CONNECTION_BUDGET_REFUSED")
        try:
            self._connection = connect(spec.url, open_timeout=10, close_timeout=2,
                                       ping_interval=None, max_size=MAX_FRAME_BYTES,
                                       max_queue=16, logger=self._logger)
        except (OSError, WebSocketException, ValueError) as exc:
            raise StreamClosed("STREAM_CONNECT_FAILED:" + type(exc).__name__) from None

    def _socket(self):
        if self._connection is None:
            raise StreamClosed("STREAM_NOT_CONNECTED")
        return self._connection

    def send(self, frame):
        try:
            self._socket().send(json.dumps(dict(frame)) if isinstance(frame, Mapping) else frame)
        except (OSError, WebSocketException) as exc:
            raise StreamClosed("STREAM_SEND_FAILED:" + type(exc).__name__) from None

    def receive(self):
        try:
            if self._pong is not None and self._pong.is_set():
                self._pong = None
                return HEARTBEAT
            return self._socket().recv(timeout=0)
        except TimeoutError:
            return None
        except (OSError, WebSocketException) as exc:
            raise StreamClosed("STREAM_RECEIVE_FAILED:" + type(exc).__name__) from None

    def ping(self):
        try:
            if self._pong is None:
                self._pong = self._socket().ping()
        except (OSError, WebSocketException) as exc:
            raise StreamClosed("STREAM_PING_FAILED:" + type(exc).__name__) from None

    def close(self):
        connection, self._connection = self._connection, None
        self._pong = None
        if connection is not None:
            try:
                connection.close()
            except (OSError, WebSocketException):
                pass


def connection_admission(db, venue, server_address, account_ref, clock):
    """A shared handshake window, separate from the venue's REST request window.

    Bybit documents 500 handshakes per IP per five minutes. Other venues retain
    a local conservative envelope of 50 until authenticated certification.
    """
    from shared_lib.broker.rate_budget import RateBudget, VenueLimit, REQUESTS, VERIFIED_BY_DOC, UNVERIFIED, RECONCILIATION
    capacity = 500 if venue == "bybit" else 50
    limits = {venue: VenueLimit(venue, capacity, 300_000, REQUESTS,
                               VERIFIED_BY_DOC if venue == "bybit" else UNVERIFIED,
                               "Bybit: https://bybit-exchange.github.io/docs/v5/rate-limit (2026-10-16); "
                               "other venues: conservative configured handshake envelope")}
    budget = RateBudget(db, limits=limits)
    def admit():
        return budget.reserve(venue, server_address + ":ws-connections", clock(), priority=RECONCILIATION,
                              account_ref=account_ref, category="WEBSOCKET_HANDSHAKE").granted
    return admit
