"""Canonical pytest bootstrap for the bot-backend component.

Two jobs:

1. Make ``app`` and ``shared_lib`` importable no matter which directory pytest
   was launched from, so the test interpreter sees exactly the tree the server
   imports.
2. Fail closed on real order submission.  No test in this suite may reach a
   broker order endpoint; paper execution runs entirely inside PaperExecutor.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_BOT_BACKEND = Path(__file__).resolve().parent
_BACKENDS = _BOT_BACKEND.parent
_SHARED = _BACKENDS / "shared"

for _p in (_BOT_BACKEND, _SHARED):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

# Tests never operate against real capital.
os.environ.setdefault("EXECUTION_MODE", "paper")
os.environ.setdefault("COSMICFORGE_TEST_MODE", "1")

import pytest  # noqa: E402


class LiveOrderSubmissionBlocked(AssertionError):
    """Raised when a test attempts to submit an order to a real broker."""


# Path fragments that create, amend or cancel real orders across every adapter
# the project ships (Binance, Bybit, BingX, OANDA, IBKR, MT bridge).
_ORDER_PATH_FRAGMENTS = (
    "/order",
    "/orders",
    "/batchorders",
    "/allopenorders",
    "/v5/order",
    "/openapi/swap",
    "/iserver/account",
    "/trade/order",
)


def _is_order_endpoint(url: str) -> bool:
    lowered = str(url or "").lower().split("?", 1)[0]
    return any(fragment in lowered for fragment in _ORDER_PATH_FRAGMENTS)


@pytest.fixture(autouse=True)
def block_live_order_submission(monkeypatch, request):
    """Fail any test that tries to POST/PUT/DELETE a broker order endpoint.

    Read-only market-data calls are untouched so connectivity tests still work.
    A test that genuinely needs to exercise the transport can opt out with
    ``@pytest.mark.allow_live_orders``.
    """
    if request.node.get_closest_marker("allow_live_orders"):
        yield
        return

    import requests

    original_request = requests.Session.request

    def guarded_request(self, method, url, *args, **kwargs):
        if str(method).upper() in {"POST", "PUT", "DELETE"} and _is_order_endpoint(url):
            raise LiveOrderSubmissionBlocked(
                f"Test attempted a live order call: {method} {url}. "
                "Paper execution must stay inside PaperExecutor."
            )
        return original_request(self, method, url, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "request", guarded_request)

    for verb in ("post", "put", "delete"):
        original_verb = getattr(requests.Session, verb)

        def make_guard(_verb, _original):
            def guarded(self, url, *args, **kwargs):
                if _is_order_endpoint(url):
                    raise LiveOrderSubmissionBlocked(
                        f"Test attempted a live order call: {_verb.upper()} {url}."
                    )
                return _original(self, url, *args, **kwargs)

            return guarded

        monkeypatch.setattr(requests.Session, verb, make_guard(verb, original_verb))

    yield


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "allow_live_orders: opt out of the live-order transport guard (transport tests only)",
    )
