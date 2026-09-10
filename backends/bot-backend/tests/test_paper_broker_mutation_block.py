"""In paper mode no broker-side mutation can reach the broker, even by accident.

Routing already keeps paper execution local (the executor's paper branch
returns before any order call). ``PaperBookClient`` is the second line of
defence: it used to forward every unknown method to the real client through
``__getattr__``, so one future call site that forgot to check the mode would
have placed a real order. Mutations now raise ``PAPER_BROKER_MUTATION_FORBIDDEN``
and are never forwarded; public market data still passes through.
"""
from __future__ import annotations

import pytest

from app.execution.paper_book_client import (
    PAPER_BROKER_MUTATION_FORBIDDEN,
    PaperBookClient,
    PaperBrokerMutationForbidden,
    is_broker_mutation,
    wrap_for_paper,
)

MUTATIONS = [
    "place_order",
    "place_protection",
    "place_algo_order",
    "close_position_market",
    "cancel_all_orders",
    "cancel_order",
    "cancel_algo_order",
    "cancel_open_orders",
    "new_order",
    "create_order",
    "submit_order",
    "amend_order",
    "modify_order",
    "reduce_only_close",
    "set_leverage",
    "change_margin_type",
    "set_position_mode",
    "_signed_post",
    "_signed_delete",
]

PUBLIC_DATA = ["klines", "last_price", "get_ticker", "exchange_info", "server_time", "account"]


class RecordingBroker:
    """A broker that records every call. A paper test must leave it untouched."""

    def __init__(self):
        self.calls: list[str] = []

    def __getattr__(self, name):
        # A real client does not claim to be a paper book or expose private
        # attributes; answering those would make the fake unrealistic.
        if name.startswith("_") or name == "is_paper_book":
            raise AttributeError(name)

        def method(*args, **kwargs):
            self.calls.append(name)
            return {"called": name}

        return method


class EmptyBook:
    _positions: dict = {}

    def get_position(self, symbol):
        return None


@pytest.fixture
def broker():
    return RecordingBroker()


@pytest.fixture
def paper_client(broker):
    return PaperBookClient(broker, EmptyBook())


@pytest.mark.parametrize("method", MUTATIONS)
def test_every_broker_mutation_is_refused_and_never_forwarded(paper_client, broker, method):
    with pytest.raises(PaperBrokerMutationForbidden) as exc:
        getattr(paper_client, method)("BTCUSDT")
    assert exc.value.code == PAPER_BROKER_MUTATION_FORBIDDEN
    assert PAPER_BROKER_MUTATION_FORBIDDEN in str(exc.value)
    assert broker.calls == [], f"{method} reached the broker"


@pytest.mark.parametrize("method", PUBLIC_DATA)
def test_public_market_data_still_passes_through(paper_client, broker, method):
    assert getattr(paper_client, method)("BTCUSDT") == {"called": method}
    assert broker.calls == [method]


def test_position_and_order_reads_come_from_the_paper_book(paper_client, broker):
    assert paper_client.get_position_amt("BTCUSDT") == 0.0
    assert paper_client.open_orders("BTCUSDT") == []
    assert paper_client.get_algo_orders("BTCUSDT") == []
    assert broker.calls == []


def test_mutation_classification_is_by_name_family():
    assert is_broker_mutation("place_anything_new")
    assert is_broker_mutation("cancel_everything")
    assert not is_broker_mutation("klines")
    assert not is_broker_mutation("get_position_info")


def test_only_paper_mode_is_wrapped(broker):
    assert isinstance(wrap_for_paper(broker, EmptyBook(), "paper"), PaperBookClient)
    assert wrap_for_paper(broker, EmptyBook(), "live") is broker
